//! `CallbackActor` owns `codex queue` delivery.

use ractor::{Actor, ActorProcessingErr, ActorRef};

use crate::callback::{
    ATTENTION_POLL, ATTENTION_SETTLE, append_fallback, send_queue, terminal_event,
};
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{CallbackStatus, TaskId, TaskRow};
use crate::error::AppError;
use crate::home::Home;
use crate::store::CallbackClaim;

/// One-way deliver; concurrent across tasks via `spawn_blocking` inside `tokio::spawn`.
pub enum CallbackMsg {
    /// Claim, build the terminal event for `row`, send, finish. Best-effort if
    /// already claimed.
    Deliver { row: TaskRow },
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
            CallbackMsg::Deliver { row } => {
                let store = state.store.clone();
                let home = state.home.clone();
                // detached on purpose: delivery is best-effort and its outcome
                // is persisted by `FinishCallback`, so a failure here is a log
                // line, not a supervision event
                tokio::spawn(async move {
                    if let Err(err) = deliver(store, home, row).await {
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
        match call(store, |reply| StoreMsg::ClaimCallback { id, reply }).await? {
            CallbackClaim::Claimed => return Ok(true),
            CallbackClaim::NotOurs => return Ok(false),
            CallbackClaim::WaitForAttention if tokio::time::Instant::now() >= deadline => {
                tracing::warn!(%id, "attention claim stranded; releasing it to deliver the terminal event");
                call(store, |reply| StoreMsg::ReleaseAttention { id, reply }).await?;
            }
            CallbackClaim::WaitForAttention => tokio::time::sleep(ATTENTION_POLL).await,
        }
    }
}

async fn deliver(store: ActorRef<StoreMsg>, home: Home, row: TaskRow) -> Result<(), AppError> {
    let id = row.id;
    if !claim_exit_callback(&store, id).await? {
        return Ok(());
    }
    // `task report` refuses terminal rows, so once the exit CAS has landed the
    // set read here is the one the caller would have seen, or a superset if a
    // report slipped in during the CAS itself
    let reports = call(&store, |reply| StoreMsg::Reports { id, reply }).await?;
    let line = terminal_event(&row, &reports, home.task_dir(id)).to_message_line()?;
    let paths = home.task_paths(id);
    let line_for_send = line.clone();
    let result = tokio::task::spawn_blocking(move || {
        send_queue(
            &row,
            &line_for_send,
            &paths.callback_log,
            &paths.delivery_lock,
        )
    })
    .await
    .map_err(|err| AppError::Internal {
        message: format!("callback join: {err}"),
    })?;
    let status = match &result {
        Ok(()) => CallbackStatus::Sent,
        Err(_) => CallbackStatus::Failed,
    };
    call(&store, |reply| StoreMsg::FinishCallback {
        id,
        status,
        reply,
    })
    .await?;
    if let Err(err) = result {
        append_fallback(&home.fallback_log_path(), &line, &err.to_string())?;
    }
    Ok(())
}
