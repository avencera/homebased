//! One `TaskActor` per non-terminal task: flock watch, apply, cancel.

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::callback::{exit_event, lost_event};
use crate::daemon::actors::callback::CallbackMsg;
use crate::daemon::actors::{StoreMsg, call_store, send_reply};
use crate::domain::{ExitReason, ProcessStatus, TaskId, TaskRow, TaskState};
use crate::error::AppError;
use crate::home::{self, Home, LockMode};
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

/// Runtime state of one watch actor.
pub struct TaskWatch {
    id: TaskId,
}

impl Actor for TaskActor {
    type Msg = TaskMsg;
    type State = TaskWatch;
    type Arguments = TaskId;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        id: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let lock_path = self.home.task_paths(id).runner_lock;
        // a dedicated OS thread, not `spawn_blocking`: the `flock` is
        // uninterruptible and the tokio blocking pool waits for every thread on
        // runtime drop, which would hold `serve` open past SIGTERM until the
        // worker exits (REQ-23, DEC-21). This thread may outlive the runtime;
        // the cast simply fails once the actor is gone.
        std::thread::Builder::new()
            .name(format!("hb-lock-{id}"))
            .spawn(move || {
                if let Err(err) = home::flock_exclusive(&lock_path, LockMode::Blocking) {
                    tracing::warn!(%id, "flock watch: {err}");
                }
                // the actor is already stopping if the cast fails; nothing to apply
                if let Err(err) = myself.cast(TaskMsg::LockReleased) {
                    tracing::debug!(%id, "lock watch cast: {err}");
                }
            })
            .map_err(|err| AppError::Internal {
                message: format!("spawn lock watch thread: {err}"),
            })?;
        Ok(TaskWatch { id })
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
                send_reply(
                    reply,
                    cancel_task(&self.store, &self.callback, &self.home, state.id).await,
                );
            }
        }
        Ok(())
    }
}

async fn apply_after_lock(actor: &TaskActor, id: TaskId) -> Result<(), AppError> {
    let Some(row) = call_store(&actor.store, |reply| StoreMsg::GetTask { id, reply }).await? else {
        return Ok(());
    };
    if row.state.is_terminal() && !row.callback_outstanding() {
        return Ok(());
    }
    let paths = actor.home.task_paths(id);
    match store::read_exit_json(&paths.exit_json)? {
        Some(exit) => apply_exit(actor, row, &exit.reason).await,
        None => apply_lost(actor, row).await,
    }
}

/// The worker released the lock without writing `exit.json`.
async fn apply_lost(actor: &TaskActor, row: TaskRow) -> Result<(), AppError> {
    let id = row.id;
    let row = match row.state {
        TaskState::Queued | TaskState::Running { .. } => {
            let from = row.status();
            match call_store(&actor.store, |reply| StoreMsg::CasStatus {
                id,
                from,
                to: ProcessStatus::Lost,
                reply,
            })
            .await?
            {
                // the worker finished the row between the watch and this CAS
                None => require_task(&actor.store, id).await?,
                Some(row) => row,
            }
        }
        TaskState::Finished { .. } | TaskState::Lost => row,
    };
    if !row.callback_outstanding() {
        return Ok(());
    }
    let reports = call_store(&actor.store, |reply| StoreMsg::Reports { id, reply }).await?;
    let event = match &row.state {
        TaskState::Lost | TaskState::Queued | TaskState::Running { .. } => {
            tracing::info!(%id, "runner lost");
            lost_event(&row, &reports, actor.home.task_dir(id))
        }
        TaskState::Finished { .. } => exit_event(&row, &reports, actor.home.task_dir(id)),
    };
    actor.callback.cast(CallbackMsg::Deliver { row, event })?;
    Ok(())
}

/// The worker wrote `exit.json`; adopt its reason if the row is still live.
async fn apply_exit(actor: &TaskActor, row: TaskRow, reason: &ExitReason) -> Result<(), AppError> {
    let id = row.id;
    let row = if row.state.is_terminal() {
        row
    } else {
        let from = row.status();
        match call_store(&actor.store, |reply| StoreMsg::CasExit {
            id,
            from,
            reason: reason.clone(),
            reply,
        })
        .await?
        {
            // the worker's own CAS won the race; its row is authoritative
            None => require_task(&actor.store, id).await?,
            Some(row) => row,
        }
    };
    if !row.callback_outstanding() {
        return Ok(());
    }
    let reports = call_store(&actor.store, |reply| StoreMsg::Reports { id, reply }).await?;
    let event = exit_event(&row, &reports, actor.home.task_dir(id));
    actor.callback.cast(CallbackMsg::Deliver { row, event })?;
    Ok(())
}

/// Request cancel in the store, then act on the outcome: deliver the exit
/// callback for a queued task, or SIGTERM a live worker. Shared by the task
/// actor and the supervisor fallback when no task actor exists.
pub(crate) async fn cancel_task(
    store: &ActorRef<StoreMsg>,
    callback: &ActorRef<CallbackMsg>,
    home: &Home,
    id: TaskId,
) -> Result<CancelResult, AppError> {
    let result = call_store(store, |reply| StoreMsg::RequestCancel { id, reply }).await?;
    match &result {
        CancelResult::AlreadyTerminal(_) => {}
        CancelResult::CancelledQueued(row) => {
            let reports = call_store(store, |reply| StoreMsg::Reports { id, reply }).await?;
            let event = exit_event(row, &reports, home.task_dir(id));
            callback.cast(CallbackMsg::Deliver {
                row: row.clone(),
                event,
            })?;
        }
        // signal the worker pid, not `-pid`: `task-run` forwards to the agent's own process group
        CancelResult::SignalWorker(row) => {
            if let Some(pid) = row.pid()
                && let Err(err) = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid),
                    nix::sys::signal::Signal::SIGTERM,
                )
            {
                tracing::warn!(%id, pid, "SIGTERM worker: {err}");
            }
        }
    }
    Ok(result)
}

async fn require_task(store: &ActorRef<StoreMsg>, id: TaskId) -> Result<TaskRow, AppError> {
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

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::str::FromStr;
    use std::time::Duration;

    use ractor::Actor;
    use tempfile::tempdir;

    use super::cancel_task;
    use crate::daemon::actors::callback::{CallbackActor, CallbackArgs};
    use crate::daemon::actors::{StoreActor, StoreMsg, call_store};
    use crate::domain::{
        Agent, AgentKind, CallbackStatus, ProcessStatus, TaskEnv, TaskId, TaskRow, ThreadId,
    };
    use crate::home::Home;
    use crate::store::{CancelResult, NewTask, new_queued_task};

    #[tokio::test]
    async fn cancel_task_without_actor_cancels_queued_row_and_delivers_callback() {
        let dir = tempdir().unwrap();
        let home = Home::resolve(Some(dir.path().to_path_buf())).unwrap();
        home.ensure().unwrap();

        let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
            .await
            .unwrap();
        let (callback, callback_handle) = CallbackActor::spawn(
            None,
            CallbackActor,
            CallbackArgs {
                store: store.clone(),
                home: home.clone(),
            },
        )
        .await
        .unwrap();

        let id = TaskId::new();
        let row = new_queued_task(NewTask {
            id,
            thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
            agent: Agent::new(AgentKind::Claude, None),
            cwd: dir.path().to_path_buf(),
            timeout: Duration::from_secs(60),
            extra_args: vec![],
            report_trailer: false,
            env: TaskEnv {
                // empty PATH so no `codex` resolves: the callback must still finish, as Failed
                path: dir.path().join("empty-bin").display().to_string(),
                home: dir.path().display().to_string(),
            },
            binary: Path::new("/bin/true").to_path_buf(),
        });
        call_store(&store, |reply| StoreMsg::InsertTask {
            row: Box::new(row),
            reply,
        })
        .await
        .unwrap();

        let result = cancel_task(&store, &callback, &home, id).await.unwrap();
        assert!(matches!(result, CancelResult::CancelledQueued(_)));

        let final_row = wait_for_callback(&store, id).await;
        assert_eq!(final_row.status(), ProcessStatus::Cancelled);
        assert_ne!(final_row.callback_status, CallbackStatus::Pending);

        store.stop(None);
        callback.stop(None);
        let _ = store_handle.await;
        let _ = callback_handle.await;
    }

    /// Poll until the callback actor leaves `pending`, since `Deliver` is fire-and-forget.
    async fn wait_for_callback(store: &ractor::ActorRef<StoreMsg>, id: TaskId) -> TaskRow {
        for _ in 0..100 {
            let row = call_store(store, |reply| StoreMsg::GetTask { id, reply })
                .await
                .unwrap()
                .unwrap();
            if row.callback_status != CallbackStatus::Pending {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("callback stayed pending");
    }
}
