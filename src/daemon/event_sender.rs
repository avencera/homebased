//! Ordered executor outbox delivery and recovery

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use ractor::ActorRef;
use serde::Deserialize;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::daemon::cluster::{EventBody, ReceiveEvent};
use crate::domain::{API_VERSION, TaskId};
use crate::error::AppError;
use crate::events::{EventAcceptance, TaskEvent};
use crate::fleet::FleetState;
use crate::fleet::http::{ClusterClient, ClusterResponse};
use crate::fleet::probe::verify_destination;
use crate::fleet::runtime::{FleetHandle, LocalIdentityStatus};
use crate::machine::MachineId;

const MAX_TASKS: usize = 8;
const MAX_EVENTS_PER_CYCLE: usize = 32;
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const EVENT_CLEANUP_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
const EVENT_CLEANUP_CATCHUP_PAUSE: Duration = Duration::from_secs(2);
const EVENT_CLEANUP_MAX_BACKOFF: Duration = Duration::from_secs(15 * 60);

#[derive(Clone)]
struct Sender {
    store: ActorRef<StoreMsg>,
    supervisor: ActorRef<SupervisorMsg>,
    local: MachineId,
    fleet: FleetState,
}

#[derive(Clone, Copy)]
struct Retry {
    failures: u32,
    next: Instant,
}

impl Retry {
    fn after_failure(previous: Option<Self>, now: Instant) -> Self {
        let failures = previous.map_or(0, |retry| retry.failures).saturating_add(1);
        Self {
            failures,
            next: now + backoff(failures),
        }
    }

    fn ready(self, now: Instant) -> bool {
        self.next <= now
    }
}

#[derive(Debug)]
enum DeliveryResult {
    Progress,
    Retry(String),
    Orphaned,
}

#[derive(Deserialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Deserialize)]
struct ErrorDetail {
    code: String,
    input: serde_json::Value,
}

/// Resume pending events at startup and scan for later committed events
pub async fn run(
    store: ActorRef<StoreMsg>,
    supervisor: ActorRef<SupervisorMsg>,
    local: MachineId,
    fleet: FleetState,
) {
    let sender = Sender {
        store,
        supervisor,
        local,
        fleet,
    };
    let mut active = HashSet::new();
    let mut retries: HashMap<TaskId, Retry> = HashMap::new();
    let mut jobs = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut cleanup_failures = 0;
    let mut cleanup_at = Instant::now();
    schedule_event_cleanup(&sender.store, &mut cleanup_at, &mut cleanup_failures).await;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                schedule(&sender, &mut active, &mut retries, &mut jobs).await;
            }
            _ = tokio::time::sleep(cleanup_at.saturating_duration_since(Instant::now())) => {
                schedule_event_cleanup(
                    &sender.store,
                    &mut cleanup_at,
                    &mut cleanup_failures,
                ).await;
            }
            result = jobs.join_next(), if !jobs.is_empty() => {
                if let Some(Ok((task, outcome))) = result {
                    active.remove(&task);
                    match outcome {
                        DeliveryResult::Progress | DeliveryResult::Orphaned => {
                            retries.remove(&task);
                        }
                        DeliveryResult::Retry(message) => {
                            let retry = Retry::after_failure(retries.get(&task).copied(), Instant::now());
                            let delay = backoff(retry.failures);
                            warn!(%task, %message, ?delay, "outbound event delivery deferred");
                            retries.insert(task, retry);
                        }
                    }
                }
            }
        }
    }
}

async fn schedule_event_cleanup(
    store: &ActorRef<StoreMsg>,
    cleanup_at: &mut Instant,
    failures: &mut u32,
) {
    match call(store, |reply| StoreMsg::CompactOldEventPayloads { reply }).await {
        Ok(batch) => {
            *failures = 0;
            if batch.compacted > 0 {
                info!(
                    compacted = batch.compacted,
                    "compacted old settled task event payloads"
                );
            }
            let pause = if batch.has_more {
                EVENT_CLEANUP_CATCHUP_PAUSE
            } else {
                EVENT_CLEANUP_INTERVAL
            };
            *cleanup_at = Instant::now() + pause;
        }
        Err(error) => {
            *failures = failures.saturating_add(1);
            let delay = event_cleanup_backoff(*failures);
            warn!(%error, ?delay, "event payload cleanup deferred");
            *cleanup_at = Instant::now() + delay;
        }
    }
}

fn event_cleanup_backoff(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(8);
    Duration::from_secs(5_u64 << shift).min(EVENT_CLEANUP_MAX_BACKOFF)
}

fn backoff(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(6);
    Duration::from_secs(1_u64 << shift).min(MAX_BACKOFF)
}

async fn schedule(
    sender: &Sender,
    active: &mut HashSet<TaskId>,
    retries: &mut HashMap<TaskId, Retry>,
    jobs: &mut JoinSet<(TaskId, DeliveryResult)>,
) {
    let tasks = match call(&sender.store, |reply| StoreMsg::PendingOutboundTasks {
        reply,
    })
    .await
    {
        Ok(tasks) => tasks,
        Err(err) => {
            warn!("read pending outbound tasks: {err}");
            return;
        }
    };
    let pending: HashSet<TaskId> = tasks.iter().copied().collect();
    retries.retain(|task, _| pending.contains(task));
    for task in tasks {
        if active.len() >= MAX_TASKS {
            break;
        }
        if active.contains(&task)
            || retries
                .get(&task)
                .is_some_and(|retry| !retry.ready(Instant::now()))
        {
            continue;
        }
        active.insert(task);
        let sender = sender.clone();
        jobs.spawn(async move { (task, sender.deliver(task).await) });
    }
}

impl Sender {
    async fn deliver(&self, task: TaskId) -> DeliveryResult {
        match self.deliver_inner(task).await {
            Ok(outcome) => outcome,
            Err(err) => DeliveryResult::Retry(err.to_string()),
        }
    }

    async fn deliver_inner(&self, task: TaskId) -> Result<DeliveryResult, AppError> {
        let Some(first_pending) = call(&self.store, |reply| StoreMsg::FirstPendingOutbound {
            id: task,
            reply,
        })
        .await?
        else {
            return Ok(DeliveryResult::Progress);
        };
        let origin = first_pending.event.origin_machine;
        let destination = if origin == self.local {
            None
        } else {
            // the authority observes its own task row, so a remote callback outage
            // cannot hide a confirmed start from the resource owner
            self.supervisor
                .cast(SupervisorMsg::RemoteOriginEvent { id: task })?;
            let FleetState::Enabled(fleet) = &self.fleet else {
                return Ok(DeliveryResult::Retry(
                    "fleet is disabled for remote origin".into(),
                ));
            };
            if !matches!(
                fleet.local_identity_status().await,
                LocalIdentityStatus::Consistent
            ) {
                return Ok(DeliveryResult::Retry(
                    "local machine identity is duplicated".into(),
                ));
            }
            let verified = match fleet.connect(origin).await {
                Ok(verified) => verified,
                Err(_) => {
                    fleet.discover_now().await;
                    fleet.connect(origin).await?
                }
            };
            Some((fleet.clone(), verified))
        };
        let client = ClusterClient::default();
        let mut seq = first_pending.event.seq;
        let mut requested = HashSet::new();
        let mut sent = 0;
        while sent < MAX_EVENTS_PER_CYCLE {
            let row = call(&self.store, |reply| StoreMsg::OutboundEventAtOrAfter {
                id: task,
                seq,
                reply,
            })
            .await?;
            let Some(row) = row else {
                return Ok(DeliveryResult::Progress);
            };
            if row.event.seq != seq {
                return Ok(DeliveryResult::Retry(format!(
                    "outbox sequence {seq} is missing"
                )));
            }
            let response = match &destination {
                None => {
                    let result = call(&self.store, |reply| StoreMsg::AcceptInboundEvent {
                        event: Box::new(row.event.clone()),
                        reply,
                    })
                    .await?;
                    if matches!(result, EventAcceptance::Acknowledged { .. }) {
                        self.supervisor
                            .cast(SupervisorMsg::DispatchInbox { id: task })?;
                    }
                    result
                }
                Some((fleet, verified)) => {
                    match send_remote(&client, fleet, verified, &row.event).await? {
                        RemoteResult::Accepted(result) => result,
                        RemoteResult::Orphaned => {
                            call(&self.store, |reply| StoreMsg::OrphanOutboundRoute {
                                id: task,
                                reply,
                            })
                            .await?;
                            return Ok(DeliveryResult::Orphaned);
                        }
                        RemoteResult::Retry(message) => return Ok(DeliveryResult::Retry(message)),
                    }
                }
            };
            match response {
                EventAcceptance::Acknowledged { seq: ack } if ack == seq.get() => {
                    call(&self.store, |reply| StoreMsg::AcknowledgeOutbound {
                        id: task,
                        seq,
                        reply,
                    })
                    .await?;
                    sent += 1;
                    let Some(next) = seq.get().checked_add(1).and_then(NonZeroU64::new) else {
                        return Ok(DeliveryResult::Progress);
                    };
                    seq = next;
                }
                EventAcceptance::Expected { seq: expected }
                    if expected > 0 && expected <= seq.get() =>
                {
                    if !requested.insert(expected) {
                        return Ok(DeliveryResult::Retry(
                            "origin repeated the same sequence gap".into(),
                        ));
                    }
                    let Some(next) = NonZeroU64::new(expected) else {
                        return Ok(DeliveryResult::Retry(
                            "origin requested zero sequence".into(),
                        ));
                    };
                    seq = next;
                }
                _ => {
                    return Ok(DeliveryResult::Retry(
                        "origin response did not match event sequence".into(),
                    ));
                }
            }
        }
        Ok(DeliveryResult::Progress)
    }
}

enum RemoteResult {
    Accepted(EventAcceptance),
    Orphaned,
    Retry(String),
}

async fn send_remote(
    client: &ClusterClient,
    fleet: &FleetHandle,
    verified: &crate::fleet::probe::VerifiedDestination,
    event: &TaskEvent,
) -> Result<RemoteResult, AppError> {
    let request = ReceiveEvent {
        api_version: API_VERSION,
        protocol_version: verified.protocol.0,
        destination_machine: event.origin_machine,
        event: event.clone(),
    };
    let response = match client
        .post_json(&verified.address, "/v1/cluster/events", &request)
        .await
    {
        Ok(response) => response,
        Err(err) => return Ok(RemoteResult::Retry(err.to_string())),
    };
    if response.status == StatusCode::OK {
        let body: EventBody = match serde_json::from_slice(&response.body) {
            Ok(body) => body,
            Err(err) => {
                return Ok(RemoteResult::Retry(format!(
                    "invalid origin acknowledgement: {err}"
                )));
            }
        };
        if body.api_version != API_VERSION {
            return Ok(RemoteResult::Retry("origin API response differs".into()));
        }
        if body.protocol_version != verified.protocol.0 {
            return Ok(RemoteResult::Retry(
                "origin protocol response differs".into(),
            ));
        }
        return Ok(RemoteResult::Accepted(body.result));
    }
    classify_error(client, fleet, verified, event, response).await
}

async fn classify_error(
    client: &ClusterClient,
    fleet: &FleetHandle,
    verified: &crate::fleet::probe::VerifiedDestination,
    event: &TaskEvent,
    response: ClusterResponse,
) -> Result<RemoteResult, AppError> {
    let error: ErrorBody = match serde_json::from_slice(&response.body) {
        Ok(error) => error,
        Err(_) => {
            return Ok(RemoteResult::Retry(format!(
                "origin returned HTTP {}",
                response.status
            )));
        }
    };
    if error.error.code == "machine_identity_mismatch" {
        let _ = fleet.probe_address(&verified.address).await;
        return Ok(RemoteResult::Retry(
            "origin address changed identity".into(),
        ));
    }
    if response.status == StatusCode::NOT_FOUND
        && error.error.code == "route_not_found"
        && error.error.input.get("task") == Some(&serde_json::json!(event.task))
    {
        let current = verify_destination(
            client,
            &verified.address,
            event.origin_machine,
            fleet.local().protocol,
        )
        .await;
        if let Ok(current) = current
            && current.boot == verified.boot
        {
            return Ok(RemoteResult::Orphaned);
        }
        let _ = fleet.probe_address(&verified.address).await;
        return Ok(RemoteResult::Retry(
            "origin identity changed during route check".into(),
        ));
    }
    Ok(RemoteResult::Retry(format!(
        "origin returned {}: {}",
        response.status, error.error.code
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_is_bounded() {
        assert_eq!(backoff(1), Duration::from_secs(1));
        assert_eq!(backoff(4), Duration::from_secs(8));
        assert_eq!(backoff(u32::MAX), MAX_BACKOFF);
        let now = Instant::now();
        let first = Retry::after_failure(None, now);
        assert!(!first.ready(now));
        assert!(first.ready(now + Duration::from_secs(1)));
        let second = Retry::after_failure(Some(first), now);
        assert!(!second.ready(now + Duration::from_secs(1)));
        assert!(second.ready(now + Duration::from_secs(2)));
    }
}
