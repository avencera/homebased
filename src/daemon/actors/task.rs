//! One `TaskActor` per non-terminal task: flock watch, apply, cancel.

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::callback::{exit_event, lost_event};
use crate::daemon::actors::callback::CallbackMsg;
use crate::daemon::actors::{call_store, send_reply, StoreMsg};
use crate::domain::{status_from_exit, CallbackStatus, ProcessStatus, TaskId};
use crate::error::AppError;
use crate::home::{self, Home};
use crate::store::{self, CancelResult};

/// Per-task messages.
pub enum TaskMsg {
    /// `runner.lock` is free; apply `exit.json` or Lost.
    LockReleased,
    /// Cancel this task.
    Cancel {
        reply: RpcReplyPort<Result<CancelResult, AppError>>,
    },
}

/// Holds refs; `Arguments` is the `TaskId`.
pub struct TaskActor {
    /// State dir.
    pub home: Home,
    /// Store actor.
    pub store: ActorRef<StoreMsg>,
    /// Callback actor.
    pub callback: ActorRef<CallbackMsg>,
}

/// Runtime state.
pub struct TaskState {
    id: TaskId,
}

impl Actor for TaskActor {
    type Msg = TaskMsg;
    type State = TaskState;
    type Arguments = TaskId;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        id: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let lock_path = self.home.task_paths(id).runner_lock;
        let actor = myself.clone();
        tokio::task::spawn_blocking(move || {
            match home::flock_exclusive_blocking(&lock_path) {
                Ok(_held) => {}
                Err(err) => tracing::warn!(%id, "flock watch: {err}"),
            }
            let _ = actor.cast(TaskMsg::LockReleased);
        });
        Ok(TaskState { id })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            TaskMsg::LockReleased => {
                if let Err(err) = apply_after_lock(self, state.id).await {
                    tracing::warn!(id = %state.id, "apply after lock: {err}");
                }
                myself.stop(None);
            }
            TaskMsg::Cancel { reply } => {
                send_reply(reply, cancel_task(self, state.id).await);
            }
        }
        Ok(())
    }
}

async fn apply_after_lock(actor: &TaskActor, id: TaskId) -> Result<(), AppError> {
    let row = match call_store(&actor.store, |reply| StoreMsg::GetTask { id, reply }).await? {
        Some(row) => row,
        None => return Ok(()),
    };
    let pending_callback = matches!(
        row.callback_status,
        CallbackStatus::Pending | CallbackStatus::Sending
    );
    if row.status.is_terminal() && !pending_callback {
        return Ok(());
    }
    let paths = actor.home.task_paths(id);
    match store::read_exit_json(&paths.exit_json)? {
        Some(exit) => apply_exit(actor, id, &exit.reason).await,
        None => {
            if !row.status.is_terminal() {
                let from = row.status;
                let _ = call_store(&actor.store, |reply| StoreMsg::CasStatus {
                    id,
                    from,
                    to: ProcessStatus::Lost,
                    reply,
                })
                .await?;
            }
            let row = require_task(&actor.store, id).await?;
            if matches!(
                row.callback_status,
                CallbackStatus::Sent | CallbackStatus::Failed
            ) {
                return Ok(());
            }
            let reports = call_store(&actor.store, |reply| StoreMsg::Reports { id, reply }).await?;
            let event = if row.status == ProcessStatus::Cancelled {
                exit_event(&row, &reports, paths.dir)
            } else if row.status == ProcessStatus::Lost || !row.status.is_terminal() {
                tracing::info!(%id, "runner lost");
                lost_event(&row, &reports, paths.dir)
            } else {
                exit_event(&row, &reports, paths.dir)
            };
            actor.callback.cast(CallbackMsg::Deliver { row, event })?;
            Ok(())
        }
    }
}

async fn apply_exit(
    actor: &TaskActor,
    id: TaskId,
    reason: &crate::domain::ExitReason,
) -> Result<(), AppError> {
    let row = require_task(&actor.store, id).await?;
    if !row.status.is_terminal() {
        let to = status_from_exit(reason);
        let _ = call_store(&actor.store, |reply| StoreMsg::CasExit {
            id,
            from: row.status,
            to,
            reason: reason.clone(),
            reply,
        })
        .await?;
    }
    let row = require_task(&actor.store, id).await?;
    if matches!(
        row.callback_status,
        CallbackStatus::Pending | CallbackStatus::Sending
    ) {
        let reports = call_store(&actor.store, |reply| StoreMsg::Reports { id, reply }).await?;
        let event = exit_event(&row, &reports, actor.home.task_dir(id));
        actor.callback.cast(CallbackMsg::Deliver { row, event })?;
    }
    Ok(())
}

async fn cancel_task(actor: &TaskActor, id: TaskId) -> Result<CancelResult, AppError> {
    let result = call_store(&actor.store, |reply| StoreMsg::RequestCancel { id, reply }).await?;
    match &result {
        CancelResult::AlreadyTerminal(_) => {}
        CancelResult::CancelledQueued(row) => {
            let reports = call_store(&actor.store, |reply| StoreMsg::Reports { id, reply }).await?;
            let event = exit_event(row, &reports, actor.home.task_dir(id));
            actor.callback.cast(CallbackMsg::Deliver {
                row: row.clone(),
                event,
            })?;
        }
        CancelResult::SignalWorker(row) => {
            if let Some(pid) = row.pid {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid),
                    nix::sys::signal::Signal::SIGTERM,
                );
            }
        }
    }
    Ok(result)
}

async fn require_task(
    store: &ActorRef<StoreMsg>,
    id: TaskId,
) -> Result<crate::domain::TaskRow, AppError> {
    call_store(store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .ok_or(AppError::TaskNotFound { id })
}

impl From<ractor::MessagingErr<CallbackMsg>> for AppError {
    fn from(err: ractor::MessagingErr<CallbackMsg>) -> Self {
        Self::Internal {
            message: format!("callback actor: {err}"),
        }
    }
}
