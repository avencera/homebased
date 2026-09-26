//! `CallbackActor` owns ordered origin inbox delivery

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use tokio::sync::watch;
use tracing::warn;

use crate::callback::{
    AttemptResult, QueueDeliveryMode, append_fallback, check_saved_callback,
    prepare_saved_queue_attempt, send_saved_queue_attempt,
};
use crate::daemon::actors::{StoreMsg, call, send_reply};
use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::events::{DeliveryOutcome, DeliveryState, EventPayload, TaskEvent};
use crate::home::Home;
use crate::notify::{Notice, NoticePriority, Notifier};
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
    /// A settled waiting event followed a real failed T3 wake
    WakeFailed {
        /// Origin thread that could not be woken
        thread: ThreadId,
        /// Event that remains waiting
        event: TaskEvent,
        /// Failure shown in the push notice
        reason: String,
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
    _timer_stop: watch::Sender<bool>,
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

    fn alert_after_failed_wake(&mut self, thread: ThreadId, event: TaskEvent, reason: String) {
        if !self.alerted_threads.insert(thread) {
            return;
        }
        let Some(notifier) = self.notifier.clone() else {
            return;
        };
        let machine_name = self.machine_name.clone();
        tokio::task::spawn_blocking(move || {
            let EventPayload::Callback { event, .. } = event.payload else {
                return;
            };
            let title = TitleSources::from_env()
                .and_then(|sources| sources.titles(&[thread]).into_iter().next().flatten())
                .unwrap_or_else(|| thread.to_string());
            let event_kind = event.event;
            let event_name = serde_json::to_value(event.event)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| format!("{event_kind:?}"));
            let display_name = &event.display_name;
            let notice = Notice {
                title: format!("Thread needs you: {title}"),
                message: format!(
                    "{display_name} ({event_name}) is waiting on {machine_name}: {reason}. Open the thread to receive it."
                ),
                tags: vec!["hourglass".into()],
                priority: NoticePriority::High,
            };
            if let Err(error) = notifier.send(&notice) {
                warn!(%thread, "waiting-thread push failed: {error}");
            }
        });
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
        let (timer_stop, mut stopped) = watch::channel(false);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(WAITING_RETRY_INTERVAL);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if myself.cast(CallbackMsg::RetryWaiting).is_err() {
                            break;
                        }
                    }
                    result = stopped.changed() => {
                        if result.is_err() || *stopped.borrow() {
                            break;
                        }
                    }
                }
            }
        });
        Ok(CallbackState {
            store: args.store,
            home: args.home,
            notifier: args.notifier,
            machine_name: args.machine_name,
            wake_times: HashMap::new(),
            alerted_threads: HashSet::new(),
            active_inbox: HashSet::new(),
            _timer_stop: timer_stop,
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
                reason,
            } => state.alert_after_failed_wake(thread, event, reason),
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
    if !first.is_some_and(|entry| unsettled(&entry.delivery)) {
        return Ok(());
    }
    state.active_inbox.insert(id);
    let store = state.store.clone();
    let home = state.home.clone();
    let myself = myself.clone();
    tokio::spawn(async move {
        let completed = match dispatch_inbox_with(store, home, id, myself.clone()).await {
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

fn unsettled(delivery: &DeliveryState) -> bool {
    matches!(
        delivery,
        DeliveryState::PendingDelivery { .. } | DeliveryState::AwaitingThread { .. }
    )
}

/// Drain one task's inbox in sequence using only its saved origin route
#[cfg(test)]
pub(crate) async fn dispatch_inbox(
    store: ActorRef<StoreMsg>,
    home: Home,
    id: TaskId,
) -> Result<(), AppError> {
    let (actor, handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home: home.clone(),
            notifier: None,
            machine_name: "test".into(),
        },
    )
    .await
    .map_err(|error| AppError::Internal {
        message: format!("callback actor spawn: {error}"),
    })?;
    let result = dispatch_inbox_with(store, home, id, actor.clone()).await;
    actor.stop(None);
    handle.await.map_err(|error| AppError::Internal {
        message: format!("callback actor join: {error}"),
    })?;
    result.map(|_| ())
}

async fn dispatch_inbox_with(
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
        let mut failed_wake = None;
        let outcome = if attempts >= 3 {
            DeliveryOutcome::Permanent(
                last_error.unwrap_or_else(|| {
                    "queue attempt budget exhausted after daemon restart".into()
                }),
            )
        } else if let Err(error) = check_saved_callback(&route.callback) {
            DeliveryOutcome::Permanent(error)
        } else if let Err(error) = home.prepare_task(id) {
            DeliveryOutcome::Permanent(format!("prepare callback delivery lock: {error}"))
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
            let context = route.callback;
            let thread = route.thread;
            let prepared = tokio::task::spawn_blocking({
                let context = context.clone();
                move || prepare_saved_queue_attempt(&context, thread)
            })
            .await
            .map_err(|error| AppError::Internal {
                message: format!("origin callback lookup join: {error}"),
            })?;
            let result = match prepared {
                Ok(prepared) => {
                    let wake_claimed = if prepared.needs_wake() {
                        call(&callback, |reply| CallbackMsg::ClaimWake { thread, reply }).await?
                    } else {
                        false
                    };
                    tokio::task::spawn_blocking(move || {
                        send_saved_queue_attempt(
                            &context,
                            thread,
                            &line,
                            &paths.callback_log,
                            &paths.delivery_lock,
                            prepared,
                            QueueDeliveryMode::OriginInbox { wake_claimed },
                        )
                    })
                    .await
                    .map_err(|error| AppError::Internal {
                        message: format!("origin callback join: {error}"),
                    })?
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(AttemptResult::Delivered) => DeliveryOutcome::Delivered,
                Ok(AttemptResult::Deferred {
                    reason,
                    wake_failed,
                }) => {
                    if wake_failed {
                        failed_wake = Some(reason.clone());
                    }
                    DeliveryOutcome::Deferred(reason)
                }
                Err(error) => DeliveryOutcome::Retryable(error),
            }
        };
        let settled = call(&store, |reply| StoreMsg::SettleInboxAttempt {
            id,
            seq,
            outcome,
            reply,
        })
        .await?;
        if let DeliveryState::DeliveryFailed { last_error, .. } = &settled.delivery {
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
        if matches!(settled.delivery, DeliveryState::Delivered { .. }) {
            let _ = callback.cast(CallbackMsg::Delivered {
                thread: route.thread,
            });
        }
        if matches!(settled.delivery, DeliveryState::AwaitingThread { .. }) {
            if let Some(reason) = failed_wake {
                let _ = callback.cast(CallbackMsg::WakeFailed {
                    thread: route.thread,
                    event: settled.event,
                    reason,
                });
            }
            return Ok(false);
        }
        if matches!(settled.delivery, DeliveryState::PendingDelivery { .. }) {
            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempts + 1))).await;
        }
    }
}
