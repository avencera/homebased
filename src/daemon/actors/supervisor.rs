//! Root supervisor: store, callback, and per-task actors.

use std::collections::HashMap;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};

use crate::daemon::actors::callback::{CallbackActor, CallbackArgs, CallbackMsg};
use crate::daemon::actors::task::{TaskActor, TaskMsg, cancel_task};
use crate::daemon::actors::{StoreActor, StoreMsg, call, send_reply};
use crate::domain::{ExitReason, ProcessStatus, TaskId, TaskRow};
use crate::error::AppError;
use crate::home::Home;
use crate::runner;
use crate::store::CancelResult;

const STORE_NAME: &str = "homebased.store";
const CALLBACK_NAME: &str = "homebased.callback";

/// Messages the HTTP layer sends here.
pub enum SupervisorMsg {
    /// Store ref for `AppState` reads.
    GetStore {
        reply: RpcReplyPort<Result<ActorRef<StoreMsg>, AppError>>,
    },
    /// Persist a queued row, spawn its worker, and watch it.
    Launch {
        row: Box<TaskRow>,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Cancel a task.
    Cancel {
        id: TaskId,
        reply: RpcReplyPort<Result<CancelResult, AppError>>,
    },
}

/// Supervisor state.
pub struct SupervisorState {
    home: Home,
    store: ActorRef<StoreMsg>,
    callback: ActorRef<CallbackMsg>,
    tasks: HashMap<TaskId, ActorRef<TaskMsg>>,
}

/// Root actor.
pub struct SupervisorActor;

impl Actor for SupervisorActor {
    type Msg = SupervisorMsg;
    type State = SupervisorState;
    type Arguments = Home;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        home: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let (store, _store_handle) = StoreActor::spawn_linked(
            Some(STORE_NAME.into()),
            StoreActor,
            home.db_path(),
            myself.get_cell(),
        )
        .await?;
        let (callback, _callback_handle) = CallbackActor::spawn_linked(
            Some(CALLBACK_NAME.into()),
            CallbackActor,
            CallbackArgs {
                store: store.clone(),
                home: home.clone(),
            },
            myself.get_cell(),
        )
        .await?;
        let mut state = SupervisorState {
            home,
            store,
            callback,
            tasks: HashMap::new(),
        };
        let rows = call(&state.store, |reply| StoreMsg::NonTerminal { reply }).await?;
        for row in rows {
            spawn_task_actor(&myself, &mut state, row.id).await?;
        }
        deliver_pending_callbacks(&state).await?;
        Ok(state)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisorMsg::GetStore { reply } => send_reply(reply, Ok(state.store.clone())),
            SupervisorMsg::Launch { row, reply } => {
                send_reply(reply, launch(&myself, state, *row).await);
            }
            SupervisorMsg::Cancel { id, reply } => {
                send_reply(reply, cancel_task(&state.store, &state.callback, id).await);
            }
        }
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisionEvent::ActorFailed(who, err) => {
                let name = who.get_name();
                // store and callback are singletons the whole daemon depends on
                // and every live task actor holds their refs; there is no
                // correct in-process recovery, so fail and let the host unit
                // restart `serve` (DEC-20)
                if name.as_deref() == Some(STORE_NAME) || name.as_deref() == Some(CALLBACK_NAME) {
                    tracing::error!(actor = ?name, "daemon actor failed; stopping serve: {err}");
                    return Err(err);
                }
                tracing::error!(actor = ?name, "actor failed: {err}");
                if let Some(id) = task_id_from_name(name) {
                    state.tasks.remove(&id);
                    spawn_task_actor(&myself, state, id).await?;
                }
            }
            SupervisionEvent::ActorTerminated(who, _, _) => {
                if let Some(id) = task_id_from_name(who.get_name()) {
                    state.tasks.remove(&id);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn task_name(id: TaskId) -> String {
    format!("homebased.task.{id}")
}

fn task_id_from_name(name: Option<String>) -> Option<TaskId> {
    let name = name?;
    let rest = name.strip_prefix("homebased.task.")?;
    rest.parse().ok()
}

/// Insert the row, spawn the worker with the runner lock held, then watch it.
/// Runs inside the supervisor so a task always has an in-process owner from
/// the moment its worker exists, and so a client that disconnects mid-submit
/// cannot strand a Queued row with no worker. The cost is that launches are
/// serialized under one `CALL_TIMEOUT`; each is a few store calls and a
/// fork, so that only bites when SQLite itself stalls. A failed spawn
/// finishes the row as `SpawnFailed`, queues its exit callback, and returns
/// the spawn error.
async fn launch(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    row: TaskRow,
) -> Result<(), AppError> {
    let id = row.id;
    call(&state.store, |reply| StoreMsg::InsertTask {
        row: Box::new(row),
        reply,
    })
    .await?;
    let paths = state.home.task_paths(id);
    // uncontended: the lock file is new for this id, so the blocking flock
    // never stalls the mailbox
    let lock = runner::lock_before_spawn(&paths)?;
    let pid = match runner::spawn_task_run(&state.home, id, lock) {
        Ok(pid) => pid,
        Err(err) => {
            finish_spawn_failed(state, id, &err).await?;
            return Err(err);
        }
    };
    call(&state.store, |reply| StoreMsg::SetPid {
        id,
        pid: pid as i32,
        reply,
    })
    .await?;
    spawn_task_actor(supervisor, state, id).await
}

async fn finish_spawn_failed(
    state: &SupervisorState,
    id: TaskId,
    err: &AppError,
) -> Result<(), AppError> {
    let reason = ExitReason::SpawnFailed {
        message: err.to_string(),
    };
    let cas = call(&state.store, |reply| StoreMsg::CasExit {
        id,
        from: ProcessStatus::Queued,
        reason,
        reply,
    })
    .await?;
    let row = match cas {
        Some(row) => row,
        // another path already finished the row; report on what it stored
        None => call(&state.store, |reply| StoreMsg::GetTask { id, reply })
            .await?
            .ok_or(AppError::TaskNotFound { id })?,
    };
    // the spawn error is what the caller must see, so a failed cast is only logged
    if let Err(cast_err) = state.callback.cast(CallbackMsg::Deliver { row }) {
        tracing::warn!(%id, "queue spawn-failure callback: {cast_err}");
    }
    Ok(())
}

async fn spawn_task_actor(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    id: TaskId,
) -> Result<(), AppError> {
    if state.tasks.contains_key(&id) {
        return Ok(());
    }
    let actor = TaskActor {
        home: state.home.clone(),
        store: state.store.clone(),
        callback: state.callback.clone(),
    };
    let (task_ref, _handle) =
        TaskActor::spawn_linked(Some(task_name(id)), actor, id, supervisor.get_cell())
            .await
            .map_err(|err| AppError::Internal {
                message: format!("spawn task actor: {err}"),
            })?;
    state.tasks.insert(id, task_ref);
    Ok(())
}

/// Deliver exit callbacks stranded on terminal rows. A daemon that died between
/// the exit CAS and `FinishCallback` leaves `callback_status` at
/// `pending|sending` with no worker and no task actor left to retry.
async fn deliver_pending_callbacks(state: &SupervisorState) -> Result<(), AppError> {
    let rows = call(&state.store, |reply| StoreMsg::PendingCallbacks { reply }).await?;
    for row in rows {
        tracing::info!(id = %row.id, "redelivering pending callback");
        state.callback.cast(CallbackMsg::Deliver { row })?;
    }
    Ok(())
}
