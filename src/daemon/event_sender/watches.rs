//! Thread watch event delivery, in sequence per watch
//!
//! A connected `homebased wait` on the watch takes each event first; otherwise
//! it goes to the caller like a task callback. An event settles once it is
//! printed or delivered, and a refused delivery settles as failed so the
//! events after it still go out

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::warn;

use super::Retry;
use crate::daemon::AppState;
use crate::daemon::actors::callback::{CallbackMsg, deliver_watch_event};
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::error::AppError;
use crate::events::DeliveryOutcome;
use crate::thread_watch::{ThreadWatch, WatchEvent, WatchId};

const MAX_SENDERS: usize = 8;

/// Send committed watch events for as long as the daemon runs
pub(crate) async fn run(state: AppState) {
    let mut active: HashSet<WatchId> = HashSet::new();
    let mut retries: HashMap<WatchId, Retry> = HashMap::new();
    let mut workers = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(error) = schedule(&state, &mut active, &retries, &mut workers).await {
                    warn!("watch event scan: {error}");
                }
            }
            result = workers.join_next(), if !workers.is_empty() => {
                if let Some(Ok((watch, result))) = result {
                    active.remove(&watch);
                    match result {
                        Ok(()) => {
                            retries.remove(&watch);
                        }
                        Err(error) => {
                            warn!(%watch, "watch event delivery deferred: {error}");
                            let retry = Retry::after_failure(retries.get(&watch).copied(), Instant::now());
                            retries.insert(watch, retry);
                        }
                    }
                }
            }
        }
    }
}

async fn schedule(
    state: &AppState,
    active: &mut HashSet<WatchId>,
    retries: &HashMap<WatchId, Retry>,
    workers: &mut JoinSet<(WatchId, Result<(), AppError>)>,
) -> Result<(), AppError> {
    let pending = call(&state.store, |reply| StoreMsg::PendingWatchEvents { reply }).await?;
    for (watch, event) in pending {
        if active.len() >= MAX_SENDERS {
            break;
        }
        let id = watch.id;
        if active.contains(&id)
            || retries
                .get(&id)
                .is_some_and(|retry| !retry.ready(Instant::now()))
        {
            continue;
        }
        active.insert(id);
        let state = state.clone();
        workers.spawn(async move {
            let result = std::panic::AssertUnwindSafe(deliver(&state, &watch, &event))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(AppError::Internal {
                        message: "watch delivery worker panicked".into(),
                    })
                });
            (id, result)
        });
    }
    Ok(())
}

async fn deliver(
    state: &AppState,
    watch: &ThreadWatch,
    event: &WatchEvent,
) -> Result<(), AppError> {
    let callback = call(&state.supervisor, |reply| SupervisorMsg::GetCallback {
        reply,
    })
    .await?;
    let settle = |delivery: &'static str| {
        call(&state.store, move |reply| StoreMsg::SettleWatchEvent {
            id: watch.id,
            seq: event.seq,
            delivery: delivery.into(),
            reply,
        })
    };
    // a connected wait prints the line for the agent; waking the thread too would wake it twice
    if state.waiters.hand_off_watch(watch.caller, event).await {
        let _ = callback.cast(CallbackMsg::Delivered {
            thread: watch.caller,
        });
        return settle("printed").await;
    }
    match deliver_watch_event(&state.home, watch, event, &callback).await? {
        Some(DeliveryOutcome::Delivered) => settle("delivered").await,
        None => settle("suppressed").await,
        Some(DeliveryOutcome::Permanent(message)) => {
            warn!(watch = %watch.id, seq = event.seq, "watch event cannot be delivered: {message}");
            settle("failed").await
        }
        Some(DeliveryOutcome::Retryable(message) | DeliveryOutcome::Deferred(message)) => {
            Err(AppError::Internal { message })
        }
    }
}
