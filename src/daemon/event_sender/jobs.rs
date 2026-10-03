//! Ordered job outbox and callback delivery using the existing Fleet and origin senders

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::warn;

use super::Retry;
use crate::callback::send_check::{SendCheck, SendFailure};
use crate::daemon::AppState;
use crate::daemon::actors::callback::deliver_job_event;
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::daemon::queue_api::ClusterJobEvent;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::events::{DeliveryOutcome, EventAcceptance};
use crate::fleet::http::ClusterClient;
use crate::queue::JobId;
use crate::queue::delivery::{JobRoute, RoutedJobEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Stage {
    Authority,
    Origin,
}

type Key = (Stage, JobId);

/// Scan committed events at startup and after worker commits, with one sender per job and stage
pub(crate) async fn run(state: AppState) {
    let mut active = HashSet::new();
    let mut retries: HashMap<Key, Retry> = HashMap::new();
    let mut workers = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(error) = schedule(&state, &mut active, &retries, &mut workers).await {
                    warn!("job event scan: {error}");
                }
            }
            result = workers.join_next(), if !workers.is_empty() => {
                if let Some(Ok((key, result))) = result {
                    active.remove(&key);
                    match result {
                        Ok(()) => { retries.remove(&key); }
                        Err(error) => {
                            warn!(job = %key.1, stage = ?key.0, "job event delivery deferred: {error}");
                            retries.insert(key, Retry::after_failure(retries.get(&key).copied(), Instant::now()));
                        }
                    }
                }
            }
        }
    }
}

async fn schedule(
    state: &AppState,
    active: &mut HashSet<Key>,
    retries: &HashMap<Key, Retry>,
    workers: &mut JoinSet<(Key, Result<(), AppError>)>,
) -> Result<(), AppError> {
    let outbox = call(&state.store, |reply| StoreMsg::PendingJobOutbox { reply }).await?;
    let inbox = call(&state.store, |reply| StoreMsg::PendingJobInbox { reply }).await?;
    let requests = outbox
        .into_iter()
        .map(|event| (Stage::Authority, None, event))
        .chain(
            inbox
                .into_iter()
                .map(|(route, event)| (Stage::Origin, Some(route), event)),
        );
    for (stage, route, event) in requests {
        let key = (stage, event.event.job);
        if active.len() >= 8 {
            break;
        }
        if active.contains(&key)
            || retries
                .get(&key)
                .is_some_and(|retry| !retry.ready(Instant::now()))
        {
            continue;
        }
        active.insert(key);
        let state = state.clone();
        workers.spawn(async move {
            let result = match route {
                Some(route) => deliver_origin(&state, route, event).await,
                None => deliver_authority(&state, event).await,
            };
            (key, result)
        });
    }
    Ok(())
}

async fn deliver_authority(state: &AppState, mut event: RoutedJobEvent) -> Result<(), AppError> {
    let original = event.event.seq;
    let job = event.event.job;
    let mut requested = HashSet::new();
    for _ in 0..32 {
        let result = if event.origin == state.machine.identity.machine {
            call(&state.store, |reply| StoreMsg::AcceptJobEvent {
                event: Box::new(event.clone()),
                reply,
            })
            .await?
        } else {
            let fleet = state
                .fleet
                .handle()
                .ok_or_else(|| AppError::MachineNotFound {
                    machine: event.origin.to_string(),
                })?;
            let destination = match fleet.connect(event.origin).await {
                Ok(destination) => destination,
                Err(_) => {
                    fleet.discover_now().await;
                    fleet.connect(event.origin).await?
                }
            };
            let body = ClusterJobEvent {
                api_version: API_VERSION,
                protocol_version: destination.protocol.0,
                destination_machine: event.origin,
                event: event.clone(),
            };
            let response = ClusterClient::default()
                .post_json(&destination.address, "/v1/cluster/job-events", &body)
                .await
                .map_err(|error| AppError::MachineUnavailable {
                    machine: event.origin,
                    message: error.to_string(),
                })?;
            if !response.status.is_success() {
                return Err(crate::client::map_error(response.status, &response.body));
            }
            #[derive(serde::Deserialize)]
            struct Ack {
                api_version: u32,
                protocol_version: u32,
                result: EventAcceptance,
            }
            let ack: Ack = serde_json::from_slice(&response.body)?;
            if ack.api_version != API_VERSION || ack.protocol_version != destination.protocol.0 {
                return Err(AppError::Usage {
                    message: "job event acknowledgement version differs".into(),
                });
            }
            ack.result
        };
        match result {
            EventAcceptance::Acknowledged { seq } if seq == event.event.seq => {
                call(&state.store, |reply| StoreMsg::AckJobEvent {
                    job,
                    seq,
                    reply,
                })
                .await?;
                if seq == original {
                    return Ok(());
                }
                event.event = stored_event(state, job, seq + 1).await?;
            }
            EventAcceptance::Expected { seq }
                if seq > 0 && seq <= event.event.seq && requested.insert(seq) =>
            {
                event.event = stored_event(state, job, seq).await?;
            }
            _ => {
                return Err(AppError::Usage {
                    message: "job event acknowledgement sequence differs".into(),
                });
            }
        }
    }
    Err(AppError::DaemonBusy)
}

async fn stored_event(
    state: &AppState,
    job: JobId,
    seq: u64,
) -> Result<crate::queue::JobEvent, AppError> {
    call(&state.store, |reply| StoreMsg::QueueEvents { job, reply })
        .await?
        .into_iter()
        .find(|event| event.seq == seq)
        .ok_or_else(|| AppError::Internal {
            message: format!("job {job} has no event {seq}"),
        })
}

async fn deliver_origin(
    state: &AppState,
    route: JobRoute,
    event: RoutedJobEvent,
) -> Result<(), AppError> {
    let before_send = if event.event.event == crate::queue::JobEventKind::JobBlocked {
        // the authority owns the episode, including when the origin was offline
        if !notice_current(state, &route, event.event.seq).await? {
            return call(&state.store, |reply| StoreMsg::SuppressJobNotice {
                job: route.job,
                seq: event.event.seq,
                reply,
            })
            .await;
        }
        let state = state.clone();
        let route = route.clone();
        let seq = event.event.seq;
        let runtime = tokio::runtime::Handle::current();
        let check: SendCheck = std::sync::Arc::new(move || {
            match runtime.block_on(notice_current(&state, &route, seq)) {
                Ok(true) => Ok(()),
                Ok(false) => Err(SendFailure::Suppressed),
                Err(error) => Err(SendFailure::Failed(error.to_string())),
            }
        });
        Some(check)
    } else {
        None
    };
    let callback = call(&state.supervisor, |reply| SupervisorMsg::GetCallback {
        reply,
    })
    .await?;
    let outcome =
        deliver_job_event(&state.home, &route, &event.event, &callback, before_send).await?;
    settle_callback(&state.store, route.job, event.event.seq, outcome).await
}

/// Settle delivered or suppressed callbacks without removing numbered payloads
pub(crate) async fn settle_callback(
    store: &ractor::ActorRef<StoreMsg>,
    job: JobId,
    seq: u64,
    outcome: Option<DeliveryOutcome>,
) -> Result<(), AppError> {
    match outcome {
        Some(DeliveryOutcome::Delivered) => {
            call(store, |reply| StoreMsg::SettleJobEvent { job, seq, reply }).await
        }
        None => {
            call(store, |reply| StoreMsg::SuppressJobNotice {
                job,
                seq,
                reply,
            })
            .await
        }
        Some(
            DeliveryOutcome::Retryable(message)
            | DeliveryOutcome::Deferred(message)
            | DeliveryOutcome::Permanent(message),
        ) => Err(AppError::Internal { message }),
    }
}

async fn notice_current(state: &AppState, route: &JobRoute, seq: u64) -> Result<bool, AppError> {
    let value = crate::daemon::queue_api::forward(
        state,
        route.authority,
        crate::store::queue::interface::QueueRequest::NoticeCurrent {
            job: route.job,
            seq,
        },
    )
    .await?;
    value
        .get("current")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| AppError::Internal {
            message: "notice eligibility response has no boolean current field".into(),
        })
}
