//! `CallbackActor` owns `codex queue` delivery

use std::collections::HashSet;

use ractor::{Actor, ActorProcessingErr, ActorRef};

use crate::callback::{append_fallback, check_saved_callback, send_saved_queue_attempt};
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::TaskId;
use crate::error::AppError;
use crate::events::{DeliveryOutcome, DeliveryState};
use crate::home::Home;

/// One-way deliver; concurrent across tasks via `spawn_blocking` inside `tokio::spawn`
pub enum CallbackMsg {
    /// Start or join the one ordered origin-inbox worker for this task
    DispatchInbox { id: TaskId },
    /// One inbox worker ended; check for a receive that raced with its exit
    InboxFinished { id: TaskId, completed: bool },
}

/// Startup args
pub struct CallbackArgs {
    /// Store actor
    pub(crate) store: ActorRef<StoreMsg>,
    /// State dir for fallback log
    pub home: Home,
}

/// Holds store ref and home
pub struct CallbackState {
    store: ActorRef<StoreMsg>,
    home: Home,
    active_inbox: HashSet<TaskId>,
}

/// Owns callback transport for the daemon
pub struct CallbackActor;

impl Actor for CallbackActor {
    type Msg = CallbackMsg;
    type State = CallbackState;
    type Arguments = CallbackArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(CallbackState {
            store: args.store,
            home: args.home,
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
            CallbackMsg::InboxFinished { id, completed } => {
                state.active_inbox.remove(&id);
                if completed {
                    start_inbox_worker(&myself, state, id).await?;
                }
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
    if !first.is_some_and(|entry| matches!(entry.delivery, DeliveryState::PendingDelivery { .. })) {
        return Ok(());
    }
    state.active_inbox.insert(id);
    let store = state.store.clone();
    let home = state.home.clone();
    let myself = myself.clone();
    tokio::spawn(async move {
        let completed = match dispatch_inbox(store, home, id).await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%id, "origin inbox dispatch: {error}");
                false
            }
        };
        let _ = myself.cast(CallbackMsg::InboxFinished { id, completed });
    });
    Ok(())
}

/// Drain one task's inbox in sequence using only its saved origin route
pub(crate) async fn dispatch_inbox(
    store: ActorRef<StoreMsg>,
    home: Home,
    id: TaskId,
) -> Result<(), AppError> {
    loop {
        let Some(entry) = call(&store, |reply| StoreMsg::EarliestInbox { id, reply }).await? else {
            return Ok(());
        };
        let DeliveryState::PendingDelivery {
            attempts,
            last_error,
        } = &entry.delivery
        else {
            return Ok(());
        };
        let seq = entry.event.seq;
        let route = call(&store, |reply| StoreMsg::OriginRoute { id, reply })
            .await?
            .ok_or(AppError::RouteNotFound { task: id })?;
        let outcome =
            if *attempts >= 3 {
                DeliveryOutcome::Permanent(last_error.clone().unwrap_or_else(|| {
                    "queue attempt budget exhausted after daemon restart".into()
                }))
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
                    return Ok(());
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
                let result = tokio::task::spawn_blocking(move || {
                    send_saved_queue_attempt(
                        &context,
                        thread,
                        &line,
                        &paths.callback_log,
                        &paths.delivery_lock,
                    )
                })
                .await
                .map_err(|error| AppError::Internal {
                    message: format!("origin callback join: {error}"),
                })?;
                match result {
                    Ok(()) => DeliveryOutcome::Delivered,
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
                tracing::warn!(%id, seq = seq.get(), "origin callback fallback log: {error}");
            }
        }
        if matches!(settled.delivery, DeliveryState::PendingDelivery { .. }) {
            tokio::time::sleep(std::time::Duration::from_millis(
                200 * u64::from(attempts + 1),
            ))
            .await;
        }
    }
}
