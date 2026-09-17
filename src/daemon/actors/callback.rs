//! `CallbackActor` owns `codex queue` delivery.

use ractor::{Actor, ActorProcessingErr, ActorRef};

use crate::callback::{
    ATTENTION_POLL, ATTENTION_SETTLE, HomebasedEvent, append_fallback, send_queue,
};
use crate::daemon::actors::{StoreMsg, call_store};
use crate::domain::{CallbackStatus, TaskId, TaskRow};
use crate::error::AppError;
use crate::home::Home;
use crate::store::CallbackClaim;

/// One-way deliver; concurrent across tasks via `spawn_blocking` inside `tokio::spawn`.
pub enum CallbackMsg {
    /// Claim, send, finish. Best-effort if already claimed.
    Deliver { row: TaskRow, event: HomebasedEvent },
}

/// Startup args.
pub struct CallbackArgs {
    /// Store actor.
    pub store: ActorRef<StoreMsg>,
    /// State dir for fallback log.
    pub home: Home,
}

/// Holds store ref and home.
pub struct CallbackState {
    store: ActorRef<StoreMsg>,
    home: Home,
}

/// Owns callback transport for the daemon.
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
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CallbackMsg::Deliver { row, event } => {
                let store = state.store.clone();
                let home = state.home.clone();
                tokio::spawn(async move {
                    if let Err(err) = deliver(store, home, row, event).await {
                        tracing::warn!("callback deliver: {err}");
                    }
                });
            }
        }
        Ok(())
    }
}

/// Claim the terminal callback, waiting out an in-flight `TASK_CHECK_DUE` so
/// the reminder can never land after the terminal event. The wait is async and
/// off the store actor, so a slow reminder never blocks the daemon.
async fn claim_exit_callback(store: &ActorRef<StoreMsg>, id: TaskId) -> Result<bool, AppError> {
    let deadline = tokio::time::Instant::now() + ATTENTION_SETTLE;
    loop {
        match call_store(store, |reply| StoreMsg::ClaimCallback { id, reply }).await? {
            CallbackClaim::Claimed => return Ok(true),
            CallbackClaim::NotOurs => return Ok(false),
            CallbackClaim::WaitForAttention if tokio::time::Instant::now() >= deadline => {
                tracing::warn!(%id, "attention claim stranded; releasing it to deliver the terminal event");
                call_store(store, |reply| StoreMsg::ReleaseAttention { id, reply }).await?;
            }
            CallbackClaim::WaitForAttention => tokio::time::sleep(ATTENTION_POLL).await,
        }
    }
}

async fn deliver(
    store: ActorRef<StoreMsg>,
    home: Home,
    row: TaskRow,
    event: HomebasedEvent,
) -> Result<(), AppError> {
    if !claim_exit_callback(&store, row.id).await? {
        return Ok(());
    }
    let line = event.to_message_line()?;
    let log_path = home.task_paths(row.id).callback_log;
    let row_clone = row.clone();
    let line_clone = line.clone();
    let result =
        tokio::task::spawn_blocking(move || send_queue(&row_clone, &line_clone, &log_path))
            .await
            .map_err(|err| AppError::Internal {
                message: format!("callback join: {err}"),
            })?;
    match result {
        Ok(()) => {
            call_store(&store, |reply| StoreMsg::FinishCallback {
                id: row.id,
                status: CallbackStatus::Sent,
                reply,
            })
            .await?;
        }
        Err(err) => {
            call_store(&store, |reply| StoreMsg::FinishCallback {
                id: row.id,
                status: CallbackStatus::Failed,
                reply,
            })
            .await?;
            append_fallback(&home.fallback_log_path(), &line, &err.to_string())?;
        }
    }
    Ok(())
}
