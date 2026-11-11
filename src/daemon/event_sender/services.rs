//! `SERVICE_EXITED` delivery: one event per service that ended on its own
//!
//! The event goes to the owner like a task callback. It settles once
//! delivered, and a refused delivery settles as failed. A route that can no
//! longer run settles at once, and a retryable failure settles after
//! [`MAX_ATTEMPTS`]. An owner whose T3 thread is archived or deleted is not
//! woken: its event settles as suppressed. No `homebased wait` takes a
//! service id, so no connected wait prints it

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::warn;

use super::Retry;
use crate::daemon::AppState;
use crate::daemon::actors::callback::deliver_service_event;
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::error::AppError;
use crate::events::{DeliveryOutcome, THREAD_WAIT_LIMIT};
use crate::service::{ServiceId, ServiceRecord, TAIL_LINES, exited_payload};

const MAX_SENDERS: usize = 8;
/// Retryable failures an event may have before it settles as failed, as for
/// a watch event
const MAX_ATTEMPTS: u32 = 3;

/// How an attempt that did not settle its event ended
enum Unsettled {
    /// A failure that spends one of the event's attempts
    Retryable(String),
    /// The owner cannot take the event yet, or the daemon could not try
    Later(String),
}

impl From<AppError> for Unsettled {
    fn from(error: AppError) -> Self {
        Self::Later(error.to_string())
    }
}

/// What the sender remembers of each pending event, in memory only, so a
/// restart grants a fresh budget
#[derive(Default)]
struct Pending {
    retry: Option<Retry>,
    attempts: u32,
    deferred_since: Option<DateTime<Utc>>,
}

/// Send due service events for as long as the daemon runs
pub(crate) async fn run(state: AppState) {
    let mut active: HashSet<ServiceId> = HashSet::new();
    let mut pending: HashMap<ServiceId, Pending> = HashMap::new();
    let mut workers: JoinSet<(ServiceId, Result<(), Unsettled>)> = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(error) = schedule(&state, &mut active, &pending, &mut workers).await {
                    warn!("service event scan: {error}");
                }
            }
            result = workers.join_next(), if !workers.is_empty() => {
                let Some(Ok((id, result))) = result else {
                    continue;
                };
                active.remove(&id);
                let Err(unsettled) = result else {
                    pending.remove(&id);
                    continue;
                };
                let entry = pending.entry(id).or_default();
                let error = match unsettled {
                    Unsettled::Retryable(error) => {
                        entry.attempts += 1;
                        error
                    }
                    Unsettled::Later(error) => {
                        entry.deferred_since.get_or_insert_with(Utc::now);
                        error
                    }
                };
                warn!(service = %id, "service event delivery deferred: {error}");
                entry.retry = Some(Retry::after_failure(entry.retry, Instant::now()));
            }
        }
    }
}

async fn schedule(
    state: &AppState,
    active: &mut HashSet<ServiceId>,
    pending: &HashMap<ServiceId, Pending>,
    workers: &mut JoinSet<(ServiceId, Result<(), Unsettled>)>,
) -> Result<(), AppError> {
    let due = call(&state.store, |reply| StoreMsg::PendingServiceEvents {
        reply,
    })
    .await?;
    for record in due {
        if active.len() >= MAX_SENDERS {
            break;
        }
        let id = record.service.id;
        let memory = pending.get(&id);
        if active.contains(&id)
            || memory
                .and_then(|memory| memory.retry)
                .is_some_and(|retry| !retry.ready(Instant::now()))
        {
            continue;
        }
        active.insert(id);
        let last = memory.map_or(0, |memory| memory.attempts) + 1 >= MAX_ATTEMPTS;
        // an owner that never comes back would keep the event forever, so a
        // wait ends at the limit task callbacks have
        let waited_out = memory
            .and_then(|memory| memory.deferred_since)
            .is_some_and(|since| Utc::now() - since >= THREAD_WAIT_LIMIT);
        let state = state.clone();
        workers.spawn(async move {
            let result = std::panic::AssertUnwindSafe(deliver(&state, &record, last, waited_out))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(Unsettled::Later("service delivery worker panicked".into()))
                });
            (id, result)
        });
    }
    Ok(())
}

/// Try the event once; with `last`, a retryable failure settles it as
/// failed, and with `waited_out`, so does a wait for the owner
async fn deliver(
    state: &AppState,
    record: &ServiceRecord,
    last: bool,
    waited_out: bool,
) -> Result<(), Unsettled> {
    let service = &record.service;
    let id = service.id;
    let settle = |delivery: &'static str| {
        call(&state.store, move |reply| StoreMsg::SettleServiceEvent {
            id,
            delivery: delivery.into(),
            reply,
        })
    };
    if owner_put_away(service.owner_t3.clone()).await? {
        return Ok(settle("suppressed").await?);
    }
    let Some(ending) = &record.ending else {
        return Ok(settle("failed").await?);
    };

    let paths = state.home.service_paths(id);
    let tail = tokio::task::spawn_blocking(move || paths.read_output(Some(TAIL_LINES)))
        .await
        .map_err(|error| Unsettled::Later(format!("read service log join: {error}")))??;
    let tail = tail.map_or_else(Vec::new, |tail| crate::service::tail_lines(&tail.text));
    let payload = exited_payload(state.machine.identity.machine, record, ending, &tail);
    let line = format!("HOMEBASED_EVENT {payload}");

    let callback = call(&state.supervisor, |reply| SupervisorMsg::GetCallback {
        reply,
    })
    .await?;
    match deliver_service_event(&state.home, service, line, &callback).await? {
        Some(DeliveryOutcome::Delivered) => Ok(settle("delivered").await?),
        None => Ok(settle("suppressed").await?),
        Some(DeliveryOutcome::Permanent(message)) => {
            warn!(service = %id, "service event cannot be delivered: {message}");
            Ok(settle("failed").await?)
        }
        Some(DeliveryOutcome::Retryable(message)) if last => {
            warn!(service = %id, "service event not delivered after {MAX_ATTEMPTS} attempts: {message}");
            Ok(settle("failed").await?)
        }
        Some(DeliveryOutcome::Retryable(message)) => Err(Unsettled::Retryable(message)),
        Some(DeliveryOutcome::Deferred(message)) if waited_out => {
            warn!(service = %id, "service event not delivered after waiting {} days for the owner: {message}", THREAD_WAIT_LIMIT.num_days());
            Ok(settle("failed").await?)
        }
        Some(DeliveryOutcome::Deferred(message)) => Err(Unsettled::Later(message)),
    }
}

/// Whether the owner's T3 thread is archived or deleted, so waking it would
/// cost a turn nobody reads, and unarchive it
async fn owner_put_away(owner_t3: Option<String>) -> Result<bool, AppError> {
    let Some(thread) = owner_t3 else {
        return Ok(false);
    };
    let put_away = tokio::task::spawn_blocking(move || {
        crate::t3::thread_put_away(&crate::t3::T3Env::from_env(), &thread)
    })
    .await
    .map_err(|error| AppError::Internal {
        message: format!("T3 owner lookup join: {error}"),
    })?;
    Ok(put_away == Some(true))
}
