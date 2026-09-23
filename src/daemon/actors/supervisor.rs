//! Root supervisor: store, callback, and per-task and per-resource actors.

use std::collections::HashMap;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};

use crate::daemon::actors::callback::{CallbackActor, CallbackArgs, CallbackMsg};
use crate::daemon::actors::resource::{
    ResourceActor, ResourceActorInspection, ResourceMsg, resource_actor_name,
    resource_id_from_actor_name,
};
use crate::daemon::actors::task::{TaskActor, TaskMsg, cancel_task};
use crate::daemon::actors::{StoreActor, StoreMsg, call, send_reply};
use crate::domain::AgentKind;
use crate::domain::TaskEnv;
use crate::domain::{ExitReason, ProcessStatus, TaskId, TaskRow};
use crate::error::AppError;
use crate::home::{Home, LockMode};
use crate::invocation::{persist_workload, resolve_agent_binary};
use crate::machine::MachineId;
use crate::machine::load_or_create_machine_id;
use crate::resource::store::ResourceSnapshot;
use crate::resource::{Resource, ResourceId};
use crate::runner;
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::{CancelResult, NewTask, new_queued_task};
use crate::submission::{ExecutionRecord, ExecutorIdentity};
use std::path::PathBuf;

const STORE_NAME: &str = "homebased.store";
const CALLBACK_NAME: &str = "homebased.callback";

/// Messages sent to the daemon root supervisor.
pub enum SupervisorMsg {
    /// Store ref for `AppState` reads.
    GetStore {
        reply: RpcReplyPort<Result<ActorRef<StoreMsg>, AppError>>,
    },
    /// Register or reuse one resource on this machine and ensure its actor exists.
    RegisterResource {
        resource: Box<Resource>,
        reply: RpcReplyPort<Result<Resource, AppError>>,
    },
    /// Inspect the restored snapshot and identity of one resource actor.
    InspectResource {
        id: ResourceId,
        reply: RpcReplyPort<Result<Option<ResourceActorInspection>, AppError>>,
    },
    /// Persist a queued row, spawn its worker, and watch it.
    Launch {
        row: Box<TaskRow>,
        spec: Box<NormalizedSpec>,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Accept a remote task and launch only if this request won acceptance
    LaunchRemote {
        request: Box<RemoteLaunch>,
        reply: RpcReplyPort<Result<ExecutorIdentity, AppError>>,
    },
    /// Cancel a task.
    Cancel {
        id: TaskId,
        reply: RpcReplyPort<Result<CancelResult, AppError>>,
    },
    /// Wake the ordered origin inbox worker after a receive commits
    DispatchInbox { id: TaskId },
}

/// Validated executor-local inputs with the original normalized retry content
pub struct RemoteLaunch {
    /// Global task UUID
    pub task: TaskId,
    /// Fixed callback owner
    pub origin: MachineId,
    /// Fixed execution owner
    pub execution: MachineId,
    /// Original normalized request
    pub spec: NormalizedSpec,
    /// Executor-expanded directory
    pub cwd: PathBuf,
    /// Executor environment
    pub env: TaskEnv,
    /// Executor-resolved workload binary
    pub binary: PathBuf,
}

/// Supervisor state.
pub struct SupervisorState {
    home: Home,
    machine: MachineId,
    store: ActorRef<StoreMsg>,
    callback: ActorRef<CallbackMsg>,
    tasks: HashMap<TaskId, ActorRef<TaskMsg>>,
    resources: HashMap<ResourceId, ActorRef<ResourceMsg>>,
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
        let machine = load_or_create_machine_id(&home)?;
        call(&store, |reply| StoreMsg::MigrateLegacyLocal {
            machine,
            reply,
        })
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
            machine,
            store,
            callback,
            tasks: HashMap::new(),
            resources: HashMap::new(),
        };
        restore_resource_actors(&myself, &mut state).await?;
        let rows = call(&state.store, |reply| StoreMsg::NonTerminal { reply }).await?;
        for row in rows {
            if row.status() == ProcessStatus::Queued {
                let identity = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
                    id: row.id,
                    reply,
                })
                .await?;
                if let Some(ExecutorIdentity::Accepted(record)) = identity
                    && record.origin_machine != record.execution_machine
                {
                    launch_accepted(&myself, &mut state, row.id).await?;
                    continue;
                }
            }
            spawn_task_actor(&myself, &mut state, row.id).await?;
        }
        for id in call(&state.store, |reply| StoreMsg::PendingInboxTasks { reply }).await? {
            state.callback.cast(CallbackMsg::DispatchInbox { id })?;
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
            SupervisorMsg::GetStore { reply } => send_reply(reply, Ok(state.store.clone())),
            SupervisorMsg::RegisterResource { resource, reply } => {
                send_reply(
                    reply,
                    register_resource_actor(&myself, state, *resource).await,
                );
            }
            SupervisorMsg::InspectResource { id, reply } => {
                let inspection = match state.resources.get(&id) {
                    Some(actor) => call(actor, |reply| ResourceMsg::Inspect { reply })
                        .await
                        .map(Some),
                    None => Ok(None),
                };
                send_reply(reply, inspection);
            }
            SupervisorMsg::Launch { row, spec, reply } => {
                send_reply(reply, launch(&myself, state, *row, *spec).await);
            }
            SupervisorMsg::LaunchRemote { request, reply } => {
                send_reply(reply, launch_remote(&myself, state, *request).await);
            }
            SupervisorMsg::Cancel { id, reply } => {
                send_reply(reply, cancel_task(&state.store, id).await);
            }
            SupervisorMsg::DispatchInbox { id } => {
                state.callback.cast(CallbackMsg::DispatchInbox { id })?;
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
                if let Some(id) = resource_id_from_actor_name(name.clone()) {
                    tracing::error!(actor = ?name, resource = ?id, "resource actor failed; stopping serve: {err}");
                    return Err(err);
                }
                tracing::error!(actor = ?name, "actor failed: {err}");
                if let Some(id) = task_id_from_name(name) {
                    state.tasks.remove(&id);
                    spawn_task_actor(&myself, state, id).await?;
                }
            }
            SupervisionEvent::ActorTerminated(who, _, reason) => {
                let name = who.get_name();
                if let Some(id) = resource_id_from_actor_name(name.clone()) {
                    let reason = reason.unwrap_or_else(|| "without an exit reason".into());
                    tracing::error!(actor = ?name, resource = ?id, %reason, "resource actor terminated; stopping serve");
                    return Err(Box::new(AppError::Internal {
                        message: format!(
                            "resource actor {} terminated unexpectedly: {reason}",
                            id.as_uuid()
                        ),
                    }));
                }
                if let Some(id) = task_id_from_name(who.get_name()) {
                    state.tasks.remove(&id);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

async fn restore_resource_actors(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
) -> Result<(), AppError> {
    let snapshots = call(&state.store, |reply| {
        StoreMsg::ResourceSnapshotsForAuthority {
            authority_machine: state.machine,
            reply,
        }
    })
    .await?;
    for snapshot in snapshots {
        spawn_resource_actor(supervisor, &mut state.resources, snapshot).await?;
    }

    Ok(())
}

async fn register_resource_actor(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    resource: Resource,
) -> Result<Resource, AppError> {
    let resource = call(&state.store, |reply| StoreMsg::RegisterResource {
        authority_machine: state.machine,
        resource: Box::new(resource),
        reply,
    })
    .await?;
    ensure_resource_actor(supervisor, state, resource.id).await?;

    Ok(resource)
}

async fn ensure_resource_actor(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    id: ResourceId,
) -> Result<(), AppError> {
    if state.resources.contains_key(&id) {
        return Ok(());
    }

    let snapshots = call(&state.store, |reply| {
        StoreMsg::ResourceSnapshotsForAuthority {
            authority_machine: state.machine,
            reply,
        }
    })
    .await?;
    let snapshot = snapshots
        .into_iter()
        .find(|snapshot| snapshot.resource.id == id)
        .ok_or_else(|| AppError::Internal {
            message: format!(
                "registered resource {} is missing from the authority store",
                id.as_uuid()
            ),
        })?;
    spawn_resource_actor(supervisor, &mut state.resources, snapshot).await
}

async fn spawn_resource_actor(
    supervisor: &ActorRef<SupervisorMsg>,
    resources: &mut HashMap<ResourceId, ActorRef<ResourceMsg>>,
    snapshot: ResourceSnapshot,
) -> Result<(), AppError> {
    let id = snapshot.resource.id;
    if resources.contains_key(&id) {
        return Ok(());
    }

    let (actor, _handle) = ResourceActor::spawn_linked(
        Some(resource_actor_name(id)),
        ResourceActor,
        (snapshot.resource, snapshot.loan),
        supervisor.get_cell(),
    )
    .await
    .map_err(|err| AppError::Internal {
        message: format!("spawn resource actor {}: {err}", id.as_uuid()),
    })?;
    resources.insert(id, actor);

    Ok(())
}

async fn launch_remote(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    request: RemoteLaunch,
) -> Result<ExecutorIdentity, AppError> {
    let RemoteLaunch {
        task,
        origin,
        execution,
        spec,
        cwd,
        env,
        binary,
    } = request;
    let existing = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
        id: task,
        reply,
    })
    .await?;
    if let Some(identity) = existing {
        return matching_identity(identity, &spec, origin, execution, task);
    }
    let paths = state.home.prepare_task(task)?;
    if let NormalizedWorkload::Agent(agent) = &spec.workload {
        runner::write_task_files(&paths, &agent.prompt, agent.report_trailer)?;
    }
    let row = new_queued_task(NewTask {
        id: task,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: persist_workload(&spec.workload),
        cwd,
        timeout: spec.timeout,
        env,
        binary,
    });
    let identity = call(&state.store, |reply| StoreMsg::InsertRemoteTask {
        row: Box::new(row),
        spec: Box::new(spec.clone()),
        origin,
        execution,
        reply,
    })
    .await?;
    let identity = matching_identity(identity, &spec, origin, execution, task)?;
    if matches!(identity, ExecutorIdentity::Accepted(_)) {
        if let Err(err) = launch_accepted(supervisor, state, task).await {
            tracing::error!(%task, "launch accepted remote task: {err}");
        }
        return call(&state.store, |reply| StoreMsg::ExecutorIdentity {
            id: task,
            reply,
        })
        .await?
        .ok_or(AppError::TaskNotFound { id: task });
    }
    Ok(identity)
}

fn matching_identity(
    identity: ExecutorIdentity,
    spec: &NormalizedSpec,
    origin: MachineId,
    execution: MachineId,
    task: TaskId,
) -> Result<ExecutorIdentity, AppError> {
    let spec_value = serde_json::to_value(spec)?;
    let same = match &identity {
        ExecutorIdentity::Accepted(ExecutionRecord {
            origin_machine,
            execution_machine,
            spec: saved,
            ..
        }) => {
            *origin_machine == origin
                && *execution_machine == execution
                && origin != execution
                && saved.current().is_some_and(|current| {
                    serde_json::to_value(current).is_ok_and(|stored| stored == spec_value)
                })
        }
        ExecutorIdentity::Rejected(record) => {
            record.origin_machine == origin && record.execution_machine == execution
        }
    };
    if same {
        Ok(identity)
    } else {
        Err(AppError::ClusterTaskConflict { task })
    }
}

async fn launch_accepted(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    id: TaskId,
) -> Result<(), AppError> {
    let paths = state.home.task_paths(id);
    let lock = match crate::home::flock_exclusive(&paths.runner_lock, LockMode::NonBlocking) {
        Ok(lock) => lock,
        Err(AppError::LockHeld { .. }) => return spawn_task_actor(supervisor, state, id).await,
        Err(err) => {
            finish_spawn_failed(state, id, &err).await?;
            return Ok(());
        }
    };
    let pid = match runner::spawn_task_run(&state.home, id, lock) {
        Ok(pid) => pid,
        Err(err) => {
            finish_spawn_failed(state, id, &err).await?;
            return Ok(());
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
/// finishes the row as `SpawnFailed` and returns the spawn error.
async fn launch(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    row: TaskRow,
    spec: NormalizedSpec,
) -> Result<(), AppError> {
    let id = row.id;
    let machine = load_or_create_machine_id(&state.home)?;
    let codex = resolve_agent_binary(AgentKind::Codex, &row.env.path, &row.cwd)
        .unwrap_or_else(|_| state.home.root().join("unavailable-codex"));
    call(&state.store, |reply| StoreMsg::InsertLocalTask {
        row: Box::new(row),
        spec: Box::new(spec),
        machine,
        codex,
        reply,
    })
    .await?;
    let paths = state.home.task_paths(id);
    // uncontended: the lock file is new for this id, so the blocking flock
    // never stalls the mailbox
    let lock = match runner::lock_before_spawn(&paths) {
        Ok(lock) => lock,
        Err(err) => {
            finish_spawn_failed(state, id, &err).await?;
            return Err(err);
        }
    };
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
    call(&state.store, |reply| StoreMsg::CasExit {
        id,
        from: ProcessStatus::Queued,
        reason,
        reply,
    })
    .await?;
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::daemon::actors::call;
    use crate::domain::{ExitReason, TaskEnv, TaskWorkload, ThreadId, Workload};
    use crate::resource::store::OpenReleaseLoanResult;
    use crate::resource::{AssignmentRevision, Loan, ResourceRevision, SupervisorAddress};
    use crate::spec::{NormalizedSpec, NormalizedWorkload};
    use crate::store::{NewTask, Store, new_queued_task};
    use crate::submission::RequestId;

    fn resource(authority: MachineId, background_task: Option<TaskId>) -> Resource {
        Resource::new(
            ResourceId::new(),
            "gpu-0".into(),
            authority,
            SupervisorAddress {
                machine: authority,
                thread: ThreadId(Uuid::now_v7()),
            },
            AssignmentRevision::new(0),
            ResourceRevision::new(0),
            background_task,
        )
    }

    fn resource_command() -> NormalizedSpec {
        serde_json::from_value(json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "resource startup test",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["/bin/echo", "hello"] }
        }))
        .unwrap()
    }

    fn seed_active_loan(home: &Home, authority: MachineId) -> (Resource, Loan) {
        let mut store = Store::open(&home.db_path()).unwrap();
        let background_task = TaskId::new();
        let resource = resource(authority, Some(background_task));
        store.register_resource(authority, &resource).unwrap();

        let spec = resource_command();
        let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
            panic!("resource startup seed must use a command workload");
        };
        let row = new_queued_task(NewTask {
            id: background_task,
            name: Some(spec.name.clone()),
            thread: spec.thread,
            workload: Workload::Task(TaskWorkload {
                command: workload.command,
            }),
            cwd: spec.cwd.clone(),
            timeout: spec.timeout,
            env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
            binary: "/bin/echo".into(),
        });
        store.insert_task(&row).unwrap();
        store
            .cas_status(
                background_task,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                authority,
                spec,
            )
            .unwrap();
        let OpenReleaseLoanResult::Opened { loan, .. } = store
            .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
            .unwrap()
        else {
            panic!("startup seed must open a release loan");
        };
        store
            .cas_exit(
                background_task,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
            )
            .unwrap()
            .unwrap();

        let saved_resource = store
            .resource_snapshots_for_authority(authority)
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.resource.id == resource.id)
            .unwrap()
            .resource;
        (saved_resource, loan)
    }

    async fn inspect_resource(
        supervisor: &ActorRef<SupervisorMsg>,
        id: ResourceId,
    ) -> Option<ResourceActorInspection> {
        call(supervisor, |reply| SupervisorMsg::InspectResource {
            id,
            reply,
        })
        .await
        .unwrap()
    }

    async fn stop_supervisor(
        supervisor: ActorRef<SupervisorMsg>,
        handle: ractor::concurrency::JoinHandle<()>,
    ) {
        supervisor.stop(None);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn startup_restores_resource_and_active_loan_snapshot() {
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (saved_resource, saved_loan) = seed_active_loan(&home, authority);

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home)
            .await
            .unwrap();
        let inspection = inspect_resource(&supervisor, saved_resource.id)
            .await
            .unwrap();

        assert_eq!(inspection.resource, saved_resource);
        assert_eq!(inspection.loan, Some(saved_loan));
        assert!(inspection.actor_id.is_local());

        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn resource_registration_retry_keeps_one_actor_identity() {
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let resource = resource(authority, None);

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home)
            .await
            .unwrap();
        let registered = call(&supervisor, |reply| SupervisorMsg::RegisterResource {
            resource: Box::new(resource.clone()),
            reply,
        })
        .await
        .unwrap();
        assert_eq!(registered, resource);
        let before_retry = inspect_resource(&supervisor, resource.id).await.unwrap();

        let retried = call(&supervisor, |reply| SupervisorMsg::RegisterResource {
            resource: Box::new(resource.clone()),
            reply,
        })
        .await
        .unwrap();
        let after_retry = inspect_resource(&supervisor, resource.id).await.unwrap();

        assert_eq!(retried, resource);
        assert_eq!(before_retry.actor_id, after_retry.actor_id);
        assert_eq!(before_retry.resource, after_retry.resource);
        assert_eq!(before_retry.loan, None);

        stop_supervisor(supervisor, handle).await;
    }
}
