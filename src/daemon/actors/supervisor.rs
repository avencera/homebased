//! Root supervisor: store, callback, and per-task actors.

use std::collections::HashMap;

use ractor::concurrency::JoinHandle;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};

use crate::daemon::actors::callback::{CallbackActor, CallbackArgs, CallbackMsg};
use crate::daemon::actors::task::{TaskActor, TaskMsg};
use crate::daemon::actors::{call_store, send_reply, StoreActor, StoreMsg, CALL_TIMEOUT};
use crate::domain::TaskId;
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
    /// Join handles so `ActorTerminated` can drop them.
    handles: HashMap<TaskId, JoinHandle<()>>,
    store_handle: Option<JoinHandle<()>>,
    callback_handle: Option<JoinHandle<()>>,
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
        let (store, store_handle) = StoreActor::spawn_linked(
            Some(STORE_NAME.into()),
            StoreActor,
            home.db_path(),
            myself.get_cell(),
        )
        .await?;
        let (callback, callback_handle) = CallbackActor::spawn_linked(
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
            handles: HashMap::new(),
            store_handle: Some(store_handle),
            callback_handle: Some(callback_handle),
        };
        let rows = call_store(&store, |reply| StoreMsg::NonTerminal { reply }).await?;
        for row in rows {
            spawn_task_actor(&myself, &mut state, row.id).await?;
        }
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
                send_reply(reply, cancel(&myself, state, id).await);
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
                tracing::error!(actor = ?who.get_name(), "actor failed: {err}");
                if who.get_name().as_deref() == Some(STORE_NAME) {
                    respawn_store(&myself, state).await?;
                } else if who.get_name().as_deref() == Some(CALLBACK_NAME) {
                    respawn_callback(&myself, state).await?;
                } else if let Some(id) = task_id_from_name(who.get_name()) {
                    state.tasks.remove(&id);
                    state.handles.remove(&id);
                    spawn_task_actor(&myself, state, id).await?;
                }
            }
            SupervisionEvent::ActorTerminated(who, _, _) => {
                if let Some(id) = task_id_from_name(who.get_name()) {
                    state.tasks.remove(&id);
                    state.handles.remove(&id);
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
    let (task_ref, handle) =
        TaskActor::spawn_linked(Some(task_name(id)), actor, id, supervisor.get_cell())
            .await
            .map_err(|err| AppError::Internal {
                message: format!("spawn task actor: {err}"),
            })?;
    state.tasks.insert(id, task_ref);
    state.handles.insert(id, handle);
    Ok(())
}

async fn cancel(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    id: TaskId,
) -> Result<CancelResult, AppError> {
    if let Some(task) = state.tasks.get(&id) {
        return crate::daemon::actors::flatten_call(
            task.call(|reply| TaskMsg::Cancel { reply }, Some(CALL_TIMEOUT))
                .await,
        );
    }
    let _ = supervisor;
    call_store(&state.store, |reply| StoreMsg::RequestCancel { id, reply }).await
}

async fn respawn_store(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
) -> Result<(), ActorProcessingErr> {
    let (store, handle) = StoreActor::spawn_linked(
        Some(STORE_NAME.into()),
        StoreActor,
        state.home.db_path(),
        supervisor.get_cell(),
    )
    .await?;
    state.store = store.clone();
    state.store_handle = Some(handle);
    respawn_callback(supervisor, state).await
}

async fn respawn_callback(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
) -> Result<(), ActorProcessingErr> {
    let (callback, handle) = CallbackActor::spawn_linked(
        Some(CALLBACK_NAME.into()),
        CallbackActor,
        CallbackArgs {
            store: state.store.clone(),
            home: state.home.clone(),
        },
        supervisor.get_cell(),
    )
    .await?;
    state.callback = callback;
    state.callback_handle = Some(handle);
    Ok(())
}
