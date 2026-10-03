//! `CallbackActor` owns ordered origin inbox delivery

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use tracing::warn;

use crate::callback::send_check::{SendCheck, SendFailure, SendGate};
use crate::callback::{
    CodexWakeError, HomebasedEvent, OriginSession, PendingRetry, PendingT3Send, ReachableOrigin,
    append_fallback, check_saved_callback, find_saved_inbox_origin, retry_pending_t3,
    send_codex_queue_attempt, send_t3_claude, wake_codex_thread, wake_stopped_session,
};
use crate::daemon::actors::{StoreMsg, call, send_reply};
use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::events::{DeliveryOutcome, DeliveryState, EventPayload, TaskEvent};
use crate::home::Home;
use crate::notify::{Notice, NoticePriority, Notifier};
use crate::submission::CallbackContext;
use crate::thread_title::TitleSources;

// a store call can time out on a loaded machine; the scan also restarts a
// worker that failed to start, so no unsettled event is stranded
const INBOX_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const T3_WAKE_INTERVAL: Duration = Duration::from_secs(5 * 60);

// a host such as T3 Code can run one short Claude Code process per turn, so a
// waiting event needs a live registry entry noticed well inside one turn
const CLAUDE_REGISTRY_POLL: Duration = Duration::from_secs(2);

// the registry file can appear before its peer key file or socket; an early
// send spends the three-attempt budget, so let the new process settle first
const CLAUDE_REGISTRY_SETTLE: Duration = Duration::from_secs(1);

/// Delivery messages; workers run concurrently across tasks
pub enum CallbackMsg {
    /// Start or join the one ordered origin inbox worker for this task
    DispatchInbox { id: TaskId },
    /// Start a worker for each task with an unsettled event
    RetryInbox,
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
    /// Delivery gave up and the event went only to the fallback log
    Undelivered {
        /// Final delivery error
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
    /// Claude Code session registry to watch, usually `~/.claude/sessions`
    ///
    /// A new or changed entry retries waiting events at once instead of at the
    /// next periodic scan
    pub claude_sessions: Option<PathBuf>,
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
    registry_watch: Option<tokio::task::JoinHandle<()>>,
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
        // a final failure always alerts, even after an earlier waiting alert
        let first_alert = self.alerted_threads.insert(thread);
        if !first_alert && !matches!(failure, WakeFailure::Undelivered { .. }) {
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
    let display_name = &event.name;
    let message = match failure {
        WakeFailure::Waiting { reason } => format!(
            "{display_name} ({event_name}) is waiting on {machine_name}: {reason}. Open the thread to receive it."
        ),
        WakeFailure::CodexQueued { reason } => format!(
            "{display_name} ({event_name}) is queued in Codex on {machine_name}: T3 could not wake the thread ({reason}). Open it to run the event."
        ),
        WakeFailure::Undelivered { reason } => format!(
            "{display_name} ({event_name}) was not delivered on {machine_name}: {reason}. It is in callback-fallback.log."
        ),
    };
    let title = match failure {
        WakeFailure::Undelivered { .. } => format!("Event not delivered: {title}"),
        WakeFailure::Waiting { .. } | WakeFailure::CodexQueued { .. } => {
            format!("Thread needs you: {title}")
        }
    };
    Notice {
        title,
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
        myself.send_interval(INBOX_RETRY_INTERVAL, || CallbackMsg::RetryInbox);
        let registry_watch = args
            .claude_sessions
            .map(|sessions| tokio::spawn(watch_claude_sessions(sessions, myself.clone())));
        Ok(CallbackState {
            store: args.store,
            home: args.home,
            notifier: args.notifier,
            machine_name: args.machine_name,
            wake_times: HashMap::new(),
            alerted_threads: HashSet::new(),
            active_inbox: HashSet::new(),
            registry_watch,
        })
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if let Some(watch) = state.registry_watch.take() {
            watch.abort();
        }
        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CallbackMsg::DispatchInbox { id } => start_inbox_worker(&myself, state, id).await,
            CallbackMsg::RetryInbox => {
                // a store timeout must not stop this actor; the next tick retries
                match call(&state.store, |reply| StoreMsg::PendingInboxTasks { reply }).await {
                    Ok(ids) => {
                        for id in ids {
                            start_inbox_worker(&myself, state, id).await;
                        }
                    }
                    Err(error) => warn!("origin inbox retry scan: {error}"),
                }
            }
            CallbackMsg::InboxFinished { id, completed } => {
                state.active_inbox.remove(&id);
                if completed {
                    start_inbox_worker(&myself, state, id).await;
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

/// Snapshot of `*.json` registry entries by file name and modification time
type RegistrySnapshot = HashMap<OsString, Option<SystemTime>>;

fn registry_snapshot(sessions: &Path) -> RegistrySnapshot {
    let Ok(entries) = std::fs::read_dir(sessions) else {
        return RegistrySnapshot::new();
    };
    entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "json")
        })
        .map(|entry| {
            let modified = entry.metadata().and_then(|meta| meta.modified()).ok();
            (entry.file_name(), modified)
        })
        .collect()
}

/// Retry waiting events when a Claude Code session registry entry appears or changes
///
/// The retry only starts workers; each worker still resolves the live entry
/// for its exact session id and live PID
async fn watch_claude_sessions(sessions: PathBuf, callback: ActorRef<CallbackMsg>) {
    let mut last: Option<RegistrySnapshot> = None;
    let mut interval = tokio::time::interval(CLAUDE_REGISTRY_POLL);
    loop {
        interval.tick().await;
        let dir = sessions.clone();
        let Ok(snapshot) = tokio::task::spawn_blocking(move || registry_snapshot(&dir)).await
        else {
            continue;
        };
        let changed = last.as_ref().is_some_and(|last| {
            snapshot
                .iter()
                .any(|(name, modified)| last.get(name) != Some(modified))
        });
        last = Some(snapshot);
        if !changed {
            continue;
        }

        tokio::time::sleep(CLAUDE_REGISTRY_SETTLE).await;
        if callback.cast(CallbackMsg::RetryInbox).is_err() {
            return;
        }
    }
}

/// Start the task's inbox worker unless one runs or nothing is unsettled
///
/// A failed store read is logged; the periodic retry scan tries again
async fn start_inbox_worker(myself: &ActorRef<CallbackMsg>, state: &mut CallbackState, id: TaskId) {
    if state.active_inbox.contains(&id) {
        return;
    }
    let first = match call(&state.store, |reply| StoreMsg::EarliestInbox { id, reply }).await {
        Ok(first) => first,
        Err(error) => {
            warn!(%id, "origin inbox read: {error}");
            return;
        }
    };
    if !first.is_some_and(|entry| entry.delivery.is_unsettled()) {
        return;
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
}

/// Result of one reserved attempt, before settlement
enum AttemptOutcome {
    /// Settle the event with this outcome
    Settle(DeliveryOutcome),
    /// The blocked episode ended before any callback bytes were written
    Suppressed,
    /// A claimed T3 wake failed, so the event waits and the thread may need a push
    WakeFailed(String),
    /// The T3 wake failed, but `codex queue` accepted the event
    DeliveredWakeFailed(String),
}

impl AttemptOutcome {
    fn into_settlement(self) -> Option<(DeliveryOutcome, Option<WakeFailure>)> {
        Some(match self {
            Self::Suppressed => return None,
            Self::Settle(outcome) => (outcome, None),
            Self::WakeFailed(reason) => (
                DeliveryOutcome::Deferred(reason.clone()),
                Some(WakeFailure::Waiting { reason }),
            ),
            Self::DeliveredWakeFailed(reason) => (
                DeliveryOutcome::Delivered,
                Some(WakeFailure::CodexQueued { reason }),
            ),
        })
    }

    fn from_send(sent: Result<(), SendFailure>) -> Self {
        match sent {
            Ok(()) => Self::Settle(DeliveryOutcome::Delivered),
            Err(SendFailure::Suppressed) => Self::Suppressed,
            Err(SendFailure::Failed(message)) => Self::Settle(DeliveryOutcome::Retryable(message)),
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
            let paths = home.task_paths(id);
            let pending = PendingT3Send::new(paths.dir.join(format!("t3-pending-{}", seq.get())));
            let attempt = OriginAttempt {
                context: route.callback.clone(),
                thread: route.thread,
                line,
                paths: CallbackPaths {
                    callback_log: paths.callback_log,
                    delivery_lock: paths.delivery_lock,
                },
                pending,
                before_send: None,
            };
            attempt.run(&callback).await?
        };
        let (outcome, failed_wake) =
            outcome
                .into_settlement()
                .ok_or_else(|| AppError::Internal {
                    message: "an ordinary task callback cannot be suppressed".into(),
                })?;
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
                warn!(%id, seq = seq.get(), "origin callback not delivered: {last_error}");
                let _ = callback.cast(CallbackMsg::WakeFailed {
                    thread: route.thread,
                    event: settled.event.clone(),
                    failure: WakeFailure::Undelivered {
                        reason: last_error.clone(),
                    },
                });
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

/// Callback evidence independent of task or job identity
struct CallbackPaths {
    callback_log: PathBuf,
    delivery_lock: PathBuf,
}

/// Deliver one job event through the same Claude, Codex, and T3 routing as task events
///
/// Returns `None` only when a blocked notice ended before the callback write
pub(crate) async fn deliver_job_event(
    home: &Home,
    route: &crate::queue::delivery::JobRoute,
    event: &crate::queue::JobEvent,
    callback: &ActorRef<CallbackMsg>,
    before_send: Option<SendCheck>,
) -> Result<Option<DeliveryOutcome>, AppError> {
    let dir = home
        .root()
        .join("jobs")
        .join(route.job.to_string())
        .join("delivery");
    std::fs::create_dir_all(&dir)?;
    let mut payload = serde_json::to_value(event)?;
    payload["api_version"] = serde_json::json!(crate::domain::API_VERSION);
    payload["thread"] = serde_json::json!(route.thread);
    payload["name"] = serde_json::json!(route.spec.name);
    payload["machine"] = serde_json::json!(route.authority);
    // blocked and never-started jobs have no run; explicit nulls keep the envelope stable
    payload["resource"] = serde_json::json!(event.run.map(|run| run.resource));
    payload["task"] = serde_json::json!(event.run.map(|run| run.task));
    payload["run_number"] = serde_json::json!(event.run.map(|run| run.run_number));
    payload["step"] = serde_json::json!(event.run.map(|run| run.step));
    let attempt = OriginAttempt {
        context: route.callback.clone(),
        thread: route.thread,
        line: format!("HOMEBASED_EVENT {}", serde_json::to_string(&payload)?),
        paths: CallbackPaths {
            callback_log: dir.join("callback.log"),
            delivery_lock: dir.join("delivery.lock"),
        },
        pending: PendingT3Send::new(dir.join(format!("t3-pending-{}", event.seq))),
        before_send: before_send.filter(|_| event.event == crate::queue::JobEventKind::JobBlocked),
    };
    let Some((outcome, _)) = attempt.run(callback).await?.into_settlement() else {
        return Ok(None);
    };
    if matches!(outcome, DeliveryOutcome::Delivered) {
        let _ = callback.cast(CallbackMsg::Delivered {
            thread: route.thread,
        });
    }
    Ok(Some(outcome))
}

/// One reserved attempt on a saved origin route
struct OriginAttempt {
    context: CallbackContext,
    thread: ThreadId,
    line: String,
    paths: CallbackPaths,
    pending: PendingT3Send,
    before_send: Option<SendCheck>,
}

impl OriginAttempt {
    fn gate(&self) -> SendGate<'_> {
        SendGate {
            path: &self.paths.delivery_lock,
            check: self.before_send.as_ref(),
        }
    }

    async fn run(self, callback: &ActorRef<CallbackMsg>) -> Result<AttemptOutcome, AppError> {
        let (attempt, retry) = run_blocking("pending T3 retry", move || {
            let retry = match self.before_send.as_ref() {
                Some(_) => crate::callback::retry_pending_t3_checked(
                    &self.context,
                    self.thread,
                    &self.line,
                    &self.paths.callback_log,
                    &self.pending,
                    self.gate(),
                ),
                None => Ok(retry_pending_t3(
                    &self.context,
                    self.thread,
                    &self.line,
                    &self.paths.callback_log,
                    &self.pending,
                )),
            };
            (self, retry)
        })
        .await?;
        let retry = match retry {
            Ok(retry) => retry,
            Err(error) => return Ok(AttemptOutcome::from_send(Err(error))),
        };
        match retry {
            Some(PendingRetry::Delivered) => Ok(AttemptOutcome::Settle(DeliveryOutcome::Delivered)),
            Some(PendingRetry::Uncertain(reason)) => {
                Ok(AttemptOutcome::Settle(DeliveryOutcome::Retryable(reason)))
            }
            Some(PendingRetry::Blocked(reason)) => Ok(AttemptOutcome::WakeFailed(reason)),
            None => attempt.route(callback).await,
        }
    }

    async fn route(self, callback: &ActorRef<CallbackMsg>) -> Result<AttemptOutcome, AppError> {
        let lookup = self.context.clone();
        let thread = self.thread;
        let origin = run_blocking("origin callback lookup", move || {
            find_saved_inbox_origin(&lookup, thread)
        })
        .await?;
        match origin {
            Ok(OriginSession::Reachable(origin)) => self.send(origin).await,
            Ok(OriginSession::T3Claude(origin)) => self.send_t3_claude(origin).await,
            Ok(OriginSession::Stopped) => self.wake(callback).await,
            Ok(OriginSession::T3Codex) => self.wake_codex().await,
            Ok(OriginSession::Codex) => self.send_codex(None).await,
            Err(error) => Ok(AttemptOutcome::Settle(DeliveryOutcome::Retryable(error))),
        }
    }

    async fn send(self, origin: ReachableOrigin) -> Result<AttemptOutcome, AppError> {
        let sent = run_blocking("origin callback", move || match self.before_send.as_ref() {
            Some(_) => origin.send_checked(
                self.thread,
                &self.line,
                &self.paths.callback_log,
                self.gate(),
            ),
            None => origin
                .send(
                    self.thread,
                    &self.line,
                    &self.paths.callback_log,
                    &self.paths.delivery_lock,
                )
                .map_err(Into::into),
        })
        .await?;
        Ok(AttemptOutcome::from_send(sent))
    }

    async fn send_t3_claude(self, origin: ReachableOrigin) -> Result<AttemptOutcome, AppError> {
        let sent = run_blocking("T3 Claude send", move || match self.before_send.as_ref() {
            Some(_) => crate::callback::send_t3_claude_checked(
                &self.context,
                self.thread,
                origin,
                &self.line,
                &self.paths.callback_log,
                self.gate(),
                &self.pending,
            ),
            None => send_t3_claude(
                &self.context,
                self.thread,
                origin,
                &self.line,
                &self.paths.callback_log,
                &self.paths.delivery_lock,
                &self.pending,
            )
            .map_err(Into::into),
        })
        .await?;
        Ok(AttemptOutcome::from_send(sent))
    }

    async fn send_codex(self, wake_failure: Option<String>) -> Result<AttemptOutcome, AppError> {
        let sent = run_blocking("origin callback", move || match self.before_send.as_ref() {
            Some(_) => crate::callback::send_codex_queue_attempt_checked(
                &self.context,
                self.thread,
                &self.line,
                &self.paths.callback_log,
                self.gate(),
            ),
            None => send_codex_queue_attempt(
                &self.context,
                self.thread,
                &self.line,
                &self.paths.callback_log,
                &self.paths.delivery_lock,
            )
            .map_err(Into::into),
        })
        .await?;
        Ok(match sent {
            Ok(()) => match wake_failure {
                Some(reason) => AttemptOutcome::DeliveredWakeFailed(reason),
                None => AttemptOutcome::Settle(DeliveryOutcome::Delivered),
            },
            Err(error) => AttemptOutcome::from_send(Err(error)),
        })
    }

    async fn wake_codex(self) -> Result<AttemptOutcome, AppError> {
        let (attempt, woken) = run_blocking("T3 Codex wake", move || {
            let woken = match self.before_send.as_ref() {
                Some(_) => crate::callback::wake_codex_thread_checked(
                    &self.context,
                    self.thread,
                    &self.line,
                    &self.paths.callback_log,
                    &self.pending,
                    self.gate(),
                ),
                None => Ok(wake_codex_thread(
                    &self.context,
                    self.thread,
                    &self.line,
                    &self.paths.callback_log,
                    &self.pending,
                )),
            };
            (self, woken)
        })
        .await?;
        let woken = match woken {
            Ok(woken) => woken,
            Err(error) => return Ok(AttemptOutcome::from_send(Err(error))),
        };
        match woken {
            Ok(()) => Ok(AttemptOutcome::Settle(DeliveryOutcome::Delivered)),
            // `codex queue` could deliver a second copy of a turn T3 already started
            Err(CodexWakeError::Uncertain(reason)) => {
                Ok(AttemptOutcome::Settle(DeliveryOutcome::Retryable(reason)))
            }
            Err(CodexWakeError::NotStarted(reason)) => attempt.send_codex(Some(reason)).await,
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
        let woken = run_blocking("T3 wake", move || match self.before_send.as_ref() {
            Some(_) => crate::callback::wake_stopped_session_checked(
                &self.context,
                thread,
                &self.line,
                &self.paths.callback_log,
                &self.pending,
                self.gate(),
            ),
            None => wake_stopped_session(
                &self.context,
                thread,
                &self.line,
                &self.paths.callback_log,
                &self.pending,
            )
            .map_err(Into::into),
        })
        .await?;
        Ok(match woken {
            Ok(()) => AttemptOutcome::Settle(DeliveryOutcome::Delivered),
            Err(SendFailure::Suppressed) => AttemptOutcome::Suppressed,
            Err(SendFailure::Failed(reason)) => AttemptOutcome::WakeFailed(reason),
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
        let result = AttemptOutcome::DeliveredWakeFailed("T3 is unavailable".into())
            .into_settlement()
            .unwrap();

        assert!(matches!(result.0, DeliveryOutcome::Delivered));
        assert!(matches!(
            result.1,
            Some(WakeFailure::CodexQueued { reason }) if reason == "T3 is unavailable"
        ));
    }
}
