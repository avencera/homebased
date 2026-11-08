//! Thread watch event delivery, in sequence per watch
//!
//! A connected `homebased wait` on the watch takes each event first; otherwise
//! it goes to the caller like a task callback. An event settles once it is
//! printed or delivered, and a refused delivery settles as failed so the
//! events after it still go out. A route that can no longer run, such as a
//! `codex queue` whose binary is gone, settles at once, and a retryable
//! failure settles after [`MAX_ATTEMPTS`]. An event that asks nothing of the
//! caller (see [`WatchEvent::wakes_caller`]), or whose caller's T3 thread is
//! archived or deleted, or whose watch ended as `caller_gone`, reaches only a
//! connected wait, and settles as suppressed otherwise

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::warn;

use super::Retry;
use crate::daemon::AppState;
use crate::daemon::actors::callback::{CallbackMsg, deliver_watch_event};
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::daemon::thread_api::caller_t3_here;
use crate::error::AppError;
use crate::events::{DeliveryOutcome, THREAD_WAIT_LIMIT};
use crate::thread_watch::{ThreadWatch, WatchEndReason, WatchEvent, WatchId};

const MAX_SENDERS: usize = 8;
/// Retryable failures one event may have before it settles as failed, as in
/// a task's inbox
const MAX_ATTEMPTS: u32 = 3;

/// How an attempt that did not settle its event ended
enum Unsettled {
    /// A failure that spends one of the event's attempts
    Retryable(String),
    /// The caller cannot take the event yet, or the daemon could not try
    Later(String),
}

impl From<AppError> for Unsettled {
    fn from(error: AppError) -> Self {
        Self::Later(error.to_string())
    }
}

/// Send committed watch events for as long as the daemon runs
pub(crate) async fn run(state: AppState) {
    let mut active: HashSet<WatchId> = HashSet::new();
    let mut retries: HashMap<WatchId, Retry> = HashMap::new();
    // retryable failures of the event each watch is on, by its seq; kept in
    // memory, so a restart grants a fresh budget
    let mut attempts: HashMap<WatchId, (u64, u32)> = HashMap::new();
    // when the event each watch is on was first deferred, by its seq; like a
    // task's wait, the limit counts from there, and a restart starts it over
    let mut waiting: HashMap<WatchId, (u64, DateTime<Utc>)> = HashMap::new();
    let mut workers = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(error) = schedule(&state, &mut active, &retries, &attempts, &waiting, &mut workers).await {
                    warn!("watch event scan: {error}");
                }
            }
            result = workers.join_next(), if !workers.is_empty() => {
                if let Some(Ok((watch, seq, result))) = result {
                    active.remove(&watch);
                    match result {
                        Ok(()) => {
                            retries.remove(&watch);
                            attempts.remove(&watch);
                            waiting.remove(&watch);
                        }
                        Err(unsettled) => {
                            let error = match unsettled {
                                Unsettled::Retryable(error) => {
                                    let spent = spent(&attempts, watch, seq);
                                    attempts.insert(watch, (seq, spent + 1));
                                    error
                                }
                                Unsettled::Later(error) => {
                                    if waiting.get(&watch).is_none_or(|(on, _)| *on != seq) {
                                        waiting.insert(watch, (seq, Utc::now()));
                                    }
                                    error
                                }
                            };
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

/// Whether delivering an event of `watch` would reach a caller that is put away
///
/// Waking such a caller costs a turn for news nobody reads, and unarchives an
/// archived T3 thread. Its T3 thread lives on this machine, so this reads it as
/// it is now, which covers events that were pending when it was archived or
/// deleted, whatever ended the watch. A watch the scan ended as `caller_gone`
/// counts too, since a caller done and quiet for an hour is still neither. The
/// events stay in the store, where `thread watches` reads them. A watch from
/// before `caller_t3` finds its caller by the session, since an ended watch is
/// never scanned again to learn it
async fn caller_put_away(state: &AppState, watch: &ThreadWatch) -> Result<bool, AppError> {
    let caller_t3 = match watch.caller_t3.clone() {
        Some(thread) => Some(thread),
        None => caller_t3_here(state, watch.caller).await,
    };
    if let Some(thread) = caller_t3 {
        let put_away = tokio::task::spawn_blocking(move || {
            crate::t3::thread_put_away(&crate::t3::T3Env::from_env(), &thread)
        })
        .await
        .map_err(|error| AppError::Internal {
            message: format!("T3 caller lookup join: {error}"),
        })?;
        if put_away == Some(true) {
            return Ok(true);
        }
    }

    let id = watch.id;
    let record = call(&state.store, |reply| StoreMsg::GetWatch { id, reply }).await?;
    Ok(record.is_some_and(|record| record.end_reason == Some(WatchEndReason::CallerGone)))
}

/// Retryable failures the event `seq` of `watch` has had
fn spent(attempts: &HashMap<WatchId, (u64, u32)>, watch: WatchId, seq: u64) -> u32 {
    attempts
        .get(&watch)
        .filter(|(on, _)| *on == seq)
        .map_or(0, |(_, spent)| *spent)
}

type Attempt = (WatchId, u64, Result<(), Unsettled>);

async fn schedule(
    state: &AppState,
    active: &mut HashSet<WatchId>,
    retries: &HashMap<WatchId, Retry>,
    attempts: &HashMap<WatchId, (u64, u32)>,
    waiting: &HashMap<WatchId, (u64, DateTime<Utc>)>,
    workers: &mut JoinSet<Attempt>,
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
        let last = spent(attempts, id, event.seq) + 1 >= MAX_ATTEMPTS;
        // a caller that never comes back would hold its watch's later events
        // forever, so a wait ends at the limit task callbacks have
        let waited_out = waiting
            .get(&id)
            .filter(|(on, _)| *on == event.seq)
            .is_some_and(|(_, since)| Utc::now() - *since >= THREAD_WAIT_LIMIT);
        let state = state.clone();
        workers.spawn(async move {
            let result =
                std::panic::AssertUnwindSafe(deliver(&state, &watch, &event, last, waited_out))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        Err(Unsettled::Later("watch delivery worker panicked".into()))
                    });
            (id, event.seq, result)
        });
    }
    Ok(())
}

/// Try one event; with `last`, a retryable failure settles it as failed, and
/// with `waited_out`, so does a wait for the caller
async fn deliver(
    state: &AppState,
    watch: &ThreadWatch,
    event: &WatchEvent,
    last: bool,
    waited_out: bool,
) -> Result<(), Unsettled> {
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
        return Ok(settle("printed").await?);
    }
    if !event.wakes_caller(watch.caller_t3.as_deref()) || caller_put_away(state, watch).await? {
        return Ok(settle("suppressed").await?);
    }
    // the routes that run `codex queue` check the saved cwd and Codex binary
    // themselves and settle a missing one as permanent; a Claude or T3 route
    // needs neither, so no check runs before the route is chosen
    match deliver_watch_event(&state.home, watch, event, &callback).await? {
        Some(DeliveryOutcome::Delivered) => Ok(settle("delivered").await?),
        None => Ok(settle("suppressed").await?),
        Some(DeliveryOutcome::Permanent(message)) => {
            warn!(watch = %watch.id, seq = event.seq, "watch event cannot be delivered: {message}");
            Ok(settle("failed").await?)
        }
        Some(DeliveryOutcome::Retryable(message)) if last => {
            warn!(watch = %watch.id, seq = event.seq, "watch event not delivered after {MAX_ATTEMPTS} attempts: {message}");
            Ok(settle("failed").await?)
        }
        Some(DeliveryOutcome::Retryable(message)) => Err(Unsettled::Retryable(message)),
        Some(DeliveryOutcome::Deferred(message)) if waited_out => {
            warn!(watch = %watch.id, seq = event.seq, "watch event not delivered after waiting {} days for the caller: {message}", THREAD_WAIT_LIMIT.num_days());
            Ok(settle("failed").await?)
        }
        Some(DeliveryOutcome::Deferred(message)) => Err(Unsettled::Later(message)),
    }
}
