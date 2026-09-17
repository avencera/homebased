//! Root supervisor: store, callback, and per-task actors.

use std::collections::HashMap;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};

use crate::callback::{exit_event, lost_event};
use crate::daemon::actors::callback::{CallbackActor, CallbackArgs, CallbackMsg};
use crate::daemon::actors::task::{TaskActor, TaskMsg, cancel_task};
use crate::daemon::actors::{CALL_TIMEOUT, StoreActor, StoreMsg, call_store, send_reply};
use crate::domain::{TaskId, TaskState};
use crate::error::AppError;
use crate::home::Home;
use crate::store::CancelResult;

const STORE_NAME: &str = "homebased.store";
const CALLBACK_NAME: &str = "homebased.callback";

/// Messages the HTTP layer and spawn path send here.
pub enum SupervisorMsg {
    /// Actor refs for `AppState`.
    GetRefs {
        reply: RpcReplyPort<Result<DaemonRefs, AppError>>,
    },
    /// Start a watch actor for a newly spawned worker.
    SpawnTask {
        id: TaskId,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Forward cancel to the task actor, or run it here if none.
    Cancel {
        id: TaskId,
        reply: RpcReplyPort<Result<CancelResult, AppError>>,
    },
}

/// Refs handed to axum after startup.
#[derive(Clone)]
pub struct DaemonRefs {
    /// Store actor.
    pub store: ActorRef<StoreMsg>,
    /// Callback actor.
    pub callback: ActorRef<CallbackMsg>,
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
            home: home.clone(),
            store: store.clone(),
            callback: callback.clone(),
            tasks: HashMap::new(),
        };
        let rows = call_store(&store, |reply| StoreMsg::NonTerminal { reply }).await?;
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
            SupervisorMsg::GetRefs { reply } => {
                send_reply(
                    reply,
                    Ok(DaemonRefs {
                        store: state.store.clone(),
                        callback: state.callback.clone(),
                    }),
                );
            }
            SupervisorMsg::SpawnTask { id, reply } => {
                send_reply(reply, spawn_task_actor(&myself, state, id).await);
            }
            SupervisorMsg::Cancel { id, reply } => {
                send_reply(reply, cancel(state, id).await);
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

async fn cancel(state: &SupervisorState, id: TaskId) -> Result<CancelResult, AppError> {
    if let Some(task) = state.tasks.get(&id) {
        return crate::daemon::actors::flatten_call(
            task.call(|reply| TaskMsg::Cancel { reply }, Some(CALL_TIMEOUT))
                .await,
        );
    }
    cancel_task(&state.store, &state.callback, &state.home, id).await
}

/// Deliver exit callbacks stranded on terminal rows. A daemon that died between
/// the exit CAS and `FinishCallback` leaves `callback_status` at
/// `pending|sending` with no worker and no task actor left to retry.
async fn deliver_pending_callbacks(state: &SupervisorState) -> Result<(), AppError> {
    let rows = call_store(&state.store, |reply| StoreMsg::PendingCallbacks { reply }).await?;
    for row in rows {
        let id = row.id;
        let reports = call_store(&state.store, |reply| StoreMsg::Reports { id, reply }).await?;
        let dir = state.home.task_dir(id);
        let event = match &row.state {
            TaskState::Lost => lost_event(&row, &reports, dir),
            _ => exit_event(&row, &reports, dir),
        };
        tracing::info!(%id, "redelivering pending callback");
        state.callback.cast(CallbackMsg::Deliver { row, event })?;
    }
    Ok(())
}
