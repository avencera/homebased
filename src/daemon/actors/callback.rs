//! `CallbackActor` owns ordered origin inbox delivery

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use tracing::warn;

use crate::callback::{
    HomebasedEvent, OriginSession, ReachableOrigin, append_fallback, check_saved_callback,
    find_saved_inbox_origin, send_codex_queue_attempt, wake_codex_thread, wake_stopped_session,
};
use crate::daemon::actors::{StoreMsg, call, send_reply};
use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::events::{DeliveryOutcome, DeliveryState, EventPayload, TaskEvent};
use crate::home::{Home, TaskPaths};
use crate::notify::{Notice, NoticePriority, Notifier};
use crate::submission::CallbackContext;
use crate::thread_title::TitleSources;

const WAITING_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const T3_WAKE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Delivery messages; workers run concurrently across tasks
pub enum CallbackMsg {
    /// Start or join the one ordered origin inbox worker for this task
    DispatchInbox { id: TaskId },
    /// Retry the earliest waiting event for each task
    RetryWaiting,
    /// One inbox worker ended; check for a receive that raced with its exit
    InboxFinished { id: TaskId, completed: bool },
    /// Claim one T3 wake before a worker starts the blocking wake
    ClaimWake {
        /// Origin thread to wake
        thread: ThreadId,
        /// Whether the wake is allowed now
        reply: RpcReplyPort<Result<bool, AppError>>,
    },
    /// A settled callback had a failed T3 wake and needs a push notice
    WakeFailed {
        /// Origin thread that could not be woken
        thread: ThreadId,
        /// Callback event for this origin thread
        event: TaskEvent,
        /// Whether delivery waits for Claude or Codex queue accepted the event
        failure: WakeFailure,
    },
    /// Clear the alert after the event reaches its origin thread
    Delivered {
        /// Origin thread that received an event
        thread: ThreadId,
    },
    #[cfg(test)]
    /// Read the actor-owned alert state in tests
    InspectAlert {
        /// Origin thread to inspect
        thread: ThreadId,
        /// Whether the thread is alerted
        reply: RpcReplyPort<Result<bool, AppError>>,
    },
}

/// Wake failure that needs a user notice
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeFailure {
    /// The Claude event still waits for its session
    Waiting {
        /// Why T3 did not wake the session
        reason: String,
    },
    /// Codex queue accepted the event after T3 could not wake its thread
    CodexQueued {
        /// Why T3 did not wake the thread
        reason: String,
    },
}

/// Startup args
pub struct CallbackArgs {
    /// Store actor
    pub(crate) store: ActorRef<StoreMsg>,
    /// State dir for fallback log
    pub home: Home,
    /// Optional push sender
    pub notifier: Option<Arc<Notifier>>,
    /// Name shown in waiting-thread notices
    pub machine_name: String,
}

/// Holds store ref, home, and retry state
pub struct CallbackState {
    store: ActorRef<StoreMsg>,
    home: Home,
    notifier: Option<Arc<Notifier>>,
    machine_name: String,
    wake_times: HashMap<ThreadId, Instant>,
    alerted_threads: HashSet<ThreadId>,
    active_inbox: HashSet<TaskId>,
}

impl CallbackState {
    fn claim_wake(&mut self, thread: ThreadId) -> bool {
        let now = Instant::now();
        if self
            .wake_times
            .get(&thread)
            .is_some_and(|last| now.duration_since(*last) < T3_WAKE_INTERVAL)
        {
            return false;
        }
        self.wake_times.insert(thread, now);
        true
    }

    fn alert_after_failed_wake(
        &mut self,
        thread: ThreadId,
        event: TaskEvent,
        failure: WakeFailure,
    ) {
        if !self.alerted_threads.insert(thread) {
            return;
        }
        let Some(notifier) = self.notifier.clone() else {
            return;
        };
        let EventPayload::Callback { event, .. } = event.payload else {
            return;
        };
        let machine_name = self.machine_name.clone();
        // thread titles read transcripts and ntfy runs curl; both block
        tokio::task::spawn_blocking(move || {
            let title = TitleSources::from_env()
                .and_then(|sources| sources.titles(&[thread]).into_iter().next().flatten())
                .unwrap_or_else(|| thread.to_string());
            let notice = wake_failure_notice(&title, &event, &machine_name, &failure);
            if let Err(error) = notifier.send(&notice) {
                warn!(%thread, "origin-thread push failed: {error}");
            }
        });
    }
}

fn wake_failure_notice(
    title: &str,
    event: &HomebasedEvent,
    machine_name: &str,
    failure: &WakeFailure,
) -> Notice {
    let event_kind = event.event;
    let event_name = serde_json::to_value(event_kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{event_kind:?}"));
    let display_name = &event.display_name;
    let message = match failure {
        WakeFailure::Waiting { reason } => format!(
            "{display_name} ({event_name}) is waiting on {machine_name}: {reason}. Open the thread to receive it."
        ),
        WakeFailure::CodexQueued { reason } => format!(
            "{display_name} ({event_name}) is queued in Codex on {machine_name}: T3 could not wake the thread ({reason}). Open it to run the event."
        ),
    };
    Notice {
        title: format!("Thread needs you: {title}"),
        message,
        tags: vec!["hourglass".into()],
        priority: NoticePriority::High,
    }
}

/// Owns callback transport for the daemon
pub struct CallbackActor;

impl Actor for CallbackActor {
    type Msg = CallbackMsg;
    type State = CallbackState;
    type Arguments = CallbackArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        // the timer task ends on its own once this actor stops
        myself.send_interval(WAITING_RETRY_INTERVAL, || CallbackMsg::RetryWaiting);
        Ok(CallbackState {
            store: args.store,
            home: args.home,
            notifier: args.notifier,
            machine_name: args.machine_name,
            wake_times: HashMap::new(),
            alerted_threads: HashSet::new(),
            active_inbox: HashSet::new(),
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CallbackMsg::DispatchInbox { id } => {
                start_inbox_worker(&myself, state, id).await?;
            }
            CallbackMsg::RetryWaiting => {
                for id in call(&state.store, |reply| StoreMsg::WaitingInboxTasks { reply }).await? {
                    start_inbox_worker(&myself, state, id).await?;
                }
            }
            CallbackMsg::InboxFinished { id, completed } => {
                state.active_inbox.remove(&id);
                if completed {
                    start_inbox_worker(&myself, state, id).await?;
                }
            }
            CallbackMsg::ClaimWake { thread, reply } => {
                send_reply(reply, Ok(state.claim_wake(thread)));
            }
            CallbackMsg::WakeFailed {
                thread,
                event,
                failure,
            } => state.alert_after_failed_wake(thread, event, failure),
            CallbackMsg::Delivered { thread } => {
                state.alerted_threads.remove(&thread);
            }
            #[cfg(test)]
            CallbackMsg::InspectAlert { thread, reply } => {
                send_reply(reply, Ok(state.alerted_threads.contains(&thread)));
            }
        }
        Ok(())
    }
}

async fn start_inbox_worker(
    myself: &ActorRef<CallbackMsg>,
    state: &mut CallbackState,
    id: TaskId,
) -> Result<(), AppError> {
    if state.active_inbox.contains(&id) {
        return Ok(());
    }
    let first = call(&state.store, |reply| StoreMsg::EarliestInbox { id, reply }).await?;
    if !first.is_some_and(|entry| entry.delivery.is_unsettled()) {
        return Ok(());
    }
    state.active_inbox.insert(id);
    let store = state.store.clone();
    let home = state.home.clone();
    let myself = myself.clone();
    tokio::spawn(async move {
        let completed = match dispatch_inbox(store, home, id, myself.clone()).await {
            Ok(completed) => completed,
            Err(error) => {
                warn!(%id, "origin inbox dispatch: {error}");
                false
            }
        };
        let _ = myself.cast(CallbackMsg::InboxFinished { id, completed });
    });
    Ok(())
}

/// Result of one reserved attempt, before settlement
enum AttemptOutcome {
    /// Settle the event with this outcome
    Settle(DeliveryOutcome),
    /// A claimed T3 wake failed, so the event waits and the thread may need a push
    WakeFailed(String),
    /// The T3 wake failed, but `codex queue` accepted the event
    DeliveredWakeFailed(String),
}

impl AttemptOutcome {
    fn into_settlement(self) -> (DeliveryOutcome, Option<WakeFailure>) {
        match self {
            Self::Settle(outcome) => (outcome, None),
            Self::WakeFailed(reason) => (
                DeliveryOutcome::Deferred(reason.clone()),
                Some(WakeFailure::Waiting { reason }),
            ),
            Self::DeliveredWakeFailed(reason) => (
                DeliveryOutcome::Delivered,
                Some(WakeFailure::CodexQueued { reason }),
            ),
        }
    }
}

/// Drain one task's inbox in sequence using only its saved origin route
///
/// Returns `false` when the worker stops before the inbox is drained, for
/// example because the earliest event waits for its origin thread
pub(crate) async fn dispatch_inbox(
    store: ActorRef<StoreMsg>,
    home: Home,
    id: TaskId,
    callback: ActorRef<CallbackMsg>,
) -> Result<bool, AppError> {
    loop {
        let Some(entry) = call(&store, |reply| StoreMsg::EarliestInbox { id, reply }).await? else {
            return Ok(true);
        };
        let (attempts, last_error) = match &entry.delivery {
            DeliveryState::PendingDelivery {
                attempts,
                last_error,
            } => (*attempts, last_error.clone()),
            DeliveryState::AwaitingThread {
                attempts, reason, ..
            } => (*attempts, Some(reason.clone())),
            _ => return Ok(true),
        };
        let seq = entry.event.seq;
        let route = call(&store, |reply| StoreMsg::OriginRoute { id, reply })
            .await?
            .ok_or(AppError::RouteNotFound { task: id })?;
        let outcome = if attempts >= 3 {
            AttemptOutcome::Settle(DeliveryOutcome::Permanent(last_error.unwrap_or_else(
                || "queue attempt budget exhausted after daemon restart".into(),
            )))
        } else if let Err(error) = check_saved_callback(&route.callback) {
            AttemptOutcome::Settle(DeliveryOutcome::Permanent(error))
        } else if let Err(error) = home.prepare_task(id) {
            AttemptOutcome::Settle(DeliveryOutcome::Permanent(format!(
                "prepare callback delivery lock: {error}"
            )))
        } else {
            let Some(_reserved) = call(&store, |reply| StoreMsg::ReserveInboxAttempt {
                id,
                seq,
                reply,
            })
            .await?
            else {
                return Ok(false);
            };
            let line = entry
                .event
                .callback_message_line()?
                .ok_or(AppError::Internal {
                    message: "pending inbox event has no callback payload".into(),
                })?;
            let attempt = OriginAttempt {
                context: route.callback.clone(),
                thread: route.thread,
                line,
                paths: home.task_paths(id),
            };
            attempt.run(&callback).await?
        };
        let (outcome, failed_wake) = outcome.into_settlement();
        let settled = call(&store, |reply| StoreMsg::SettleInboxAttempt {
            id,
            seq,
            outcome,
            reply,
        })
        .await?;
        match &settled.delivery {
            DeliveryState::DeliveryFailed { last_error, .. } => {
                let line = settled
                    .event
                    .callback_message_line()?
                    .ok_or(AppError::Internal {
                        message: "failed inbox event has no callback payload".into(),
                    })?;
                if let Err(error) = append_fallback(&home.fallback_log_path(), &line, last_error) {
                    warn!(%id, seq = seq.get(), "origin callback fallback log: {error}");
                }
            }
            DeliveryState::Delivered { .. } => {
                if let Some(failure @ WakeFailure::CodexQueued { .. }) = failed_wake {
                    let _ = callback.cast(CallbackMsg::WakeFailed {
                        thread: route.thread,
                        event: settled.event,
                        failure,
                    });
                } else {
                    let _ = callback.cast(CallbackMsg::Delivered {
                        thread: route.thread,
                    });
                }
            }
            DeliveryState::AwaitingThread { .. } => {
                if let Some(failure @ WakeFailure::Waiting { .. }) = failed_wake {
                    let _ = callback.cast(CallbackMsg::WakeFailed {
                        thread: route.thread,
                        event: settled.event,
                        failure,
                    });
                }
                // the retry timer picks this task up again
                return Ok(false);
            }
            DeliveryState::PendingDelivery { .. } => {
                tokio::time::sleep(Duration::from_millis(200 * u64::from(attempts + 1))).await;
            }
            DeliveryState::NotRequired => {}
        }
    }
}

/// One reserved attempt on a saved origin route
struct OriginAttempt {
    context: CallbackContext,
    thread: ThreadId,
    line: String,
    paths: TaskPaths,
}

impl OriginAttempt {
    async fn run(self, callback: &ActorRef<CallbackMsg>) -> Result<AttemptOutcome, AppError> {
        let lookup = self.context.clone();
        let thread = self.thread;
        let origin = run_blocking("origin callback lookup", move || {
            find_saved_inbox_origin(&lookup, thread)
        })
        .await?;
        match origin {
            Ok(OriginSession::Reachable(origin)) => self.send(origin).await,
            Ok(OriginSession::Stopped) => self.wake(callback).await,
            Ok(OriginSession::T3Codex) => self.wake_codex().await,
            Ok(OriginSession::Codex) => self.send_codex(None).await,
            Err(error) => Ok(AttemptOutcome::Settle(DeliveryOutcome::Retryable(error))),
        }
    }

    async fn send(self, origin: ReachableOrigin) -> Result<AttemptOutcome, AppError> {
        let sent = run_blocking("origin callback", move || {
            origin.send(
                self.thread,
                &self.line,
                &self.paths.callback_log,
                &self.paths.delivery_lock,
            )
        })
        .await?;
        Ok(AttemptOutcome::Settle(match sent {
            Ok(()) => DeliveryOutcome::Delivered,
            Err(error) => DeliveryOutcome::Retryable(error),
        }))
    }

    async fn send_codex(self, wake_failure: Option<String>) -> Result<AttemptOutcome, AppError> {
        let sent = run_blocking("origin callback", move || {
            send_codex_queue_attempt(
                &self.context,
                self.thread,
                &self.line,
                &self.paths.callback_log,
                &self.paths.delivery_lock,
            )
        })
        .await?;
        Ok(match sent {
            Ok(()) => match wake_failure {
                Some(reason) => AttemptOutcome::DeliveredWakeFailed(reason),
                None => AttemptOutcome::Settle(DeliveryOutcome::Delivered),
            },
            Err(error) => AttemptOutcome::Settle(DeliveryOutcome::Retryable(error)),
        })
    }

    async fn wake_codex(self) -> Result<AttemptOutcome, AppError> {
        let context = self.context.clone();
        let thread = self.thread;
        let line = self.line.clone();
        let log_path = self.paths.callback_log.clone();
        let woken = run_blocking("T3 Codex wake", move || {
            wake_codex_thread(&context, thread, &line, &log_path)
        })
        .await?;
        match woken {
            Ok(()) => Ok(AttemptOutcome::Settle(DeliveryOutcome::Delivered)),
            Err(reason) => self.send_codex(Some(reason)).await,
        }
    }

    /// Wake a stopped Claude session through T3 unless another attempt woke it recently
    async fn wake(self, callback: &ActorRef<CallbackMsg>) -> Result<AttemptOutcome, AppError> {
        let thread = self.thread;
        if !call(callback, |reply| CallbackMsg::ClaimWake { thread, reply }).await? {
            return Ok(AttemptOutcome::Settle(DeliveryOutcome::Deferred(format!(
                "Claude session {thread} is not running; T3 wake retried later"
            ))));
        }
        let woken = run_blocking("T3 wake", move || {
            wake_stopped_session(&self.context, thread, &self.line, &self.paths.callback_log)
        })
        .await?;
        Ok(match woken {
            Ok(()) => AttemptOutcome::Settle(DeliveryOutcome::Delivered),
            Err(reason) => AttemptOutcome::WakeFailed(reason),
        })
    }
}

async fn run_blocking<T: Send + 'static>(
    label: &str,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, AppError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| AppError::Internal {
            message: format!("{label} join: {error}"),
        })
}

#[cfg(test)]
mod tests {
    use super::{AttemptOutcome, WakeFailure};
    use crate::events::DeliveryOutcome;

    #[test]
    fn codex_queue_fallback_settles_as_delivered_with_a_wake_failure() {
        let result =
            AttemptOutcome::DeliveredWakeFailed("T3 is unavailable".into()).into_settlement();

        assert!(matches!(result.0, DeliveryOutcome::Delivered));
        assert!(matches!(
            result.1,
            Some(WakeFailure::CodexQueued { reason }) if reason == "T3 is unavailable"
        ));
    }
}
