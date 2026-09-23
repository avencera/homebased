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
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskId, TaskRow, TaskState,
};
use crate::error::AppError;
use crate::home::{Home, LockMode};
use crate::invocation::{persist_workload, resolve_agent_binary};
use crate::machine::MachineId;
use crate::machine::load_or_create_machine_id;
use crate::resource::release_watcher::ReleaseWatcherCommand;
use crate::resource::store::{
    AcceptedResourceTask, ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError,
    ReleaseWatcherAcceptanceInput, ResourceSnapshot, ResourceStoreError, ResourceTaskAcceptance,
    ResourceTaskAcceptanceInput,
};
use crate::resource::{ReleaseWatcherIntent, Resource, ResourceId, SupervisorAddress};
use crate::runner;
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::{CancelResult, NewTask, new_queued_task};
use crate::submission::{CallbackContext, CallbackExecutable, ExecutionRecord, ExecutorIdentity};
use std::path::PathBuf;

const STORE_NAME: &str = "homebased.store";
const CALLBACK_NAME: &str = "homebased.callback";

#[cfg(test)]
pub(crate) static SUPERVISOR_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    /// Wake one resource actor after queue acceptance or an authority-side retry.
    ReconcileResource {
        /// Resource whose authority-owned queue must be reconciled.
        id: ResourceId,
        /// Reply after the resource actor has reconciled and refreshed its snapshot.
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Accept one assigned request and launch only when this call inserts its task.
    // keep the resource storage types private to the actor protocol
    #[allow(private_interfaces)]
    LaunchAssignedResourceTask {
        /// Exact Serving assignment and fixed task identity selected by the resource owner.
        input: Box<ResourceTaskAcceptanceInput>,
        /// Typed task acceptance, nested inside actor and storage errors.
        reply: RpcReplyPort<Result<Result<ResourceTaskAcceptance, ResourceStoreError>, AppError>>,
    },
    /// Accept one authority-bound release watcher and spawn only when this call inserts it
    // keep the resource storage types private to the actor protocol
    #[allow(private_interfaces)]
    LaunchBoundReleaseWatcher {
        /// Saved watcher intent and the executable for its canonical command
        launch: Box<ReleaseWatcherLaunch>,
        /// Typed watcher acceptance, nested inside actor and storage errors
        reply: RpcReplyPort<
            Result<Result<ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError>, AppError>,
        >,
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

/// Authority-owned inputs for one co-located release-watcher launch
///
/// The supervisor builds the canonical command from these identities; the store
/// rejects any row or spec that differs from the saved intent's digest
pub(crate) struct ReleaseWatcherLaunch {
    /// Machine that owns the resource and executes the watcher
    pub(crate) authority_machine: MachineId,
    /// Resource whose release action owns the watcher
    pub(crate) resource_id: ResourceId,
    /// Supervisor address saved on the resource
    pub(crate) supervisor: SupervisorAddress,
    /// Saved watcher task, request, action, and digest identities
    pub(crate) intent: ReleaseWatcherIntent,
    /// Executable placed in the canonical watcher command
    pub(crate) executable: PathBuf,
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
        let accepted_resource_tasks = call(&state.store, |reply| {
            StoreMsg::AcceptedResourceTasksForAuthority {
                authority_machine: state.machine,
                reply,
            }
        })
        .await?;
        let accepted_resource_tasks: HashMap<_, _> = accepted_resource_tasks
            .into_iter()
            .map(|task| (task.request.task_id, task))
            .collect();
        let rows = call(&state.store, |reply| StoreMsg::NonTerminal { reply }).await?;
        for row in rows {
            let identity = if row.status() == ProcessStatus::Queued
                && !accepted_resource_tasks.contains_key(&row.id)
            {
                call(&state.store, |reply| StoreMsg::ExecutorIdentity {
                    id: row.id,
                    reply,
                })
                .await?
            } else {
                None
            };
            match startup_recovery_action(
                &row,
                accepted_resource_tasks.get(&row.id),
                identity.as_ref(),
            ) {
                StartupRecoveryAction::LaunchAccepted => {
                    launch_accepted(&myself, &mut state, row.id).await?;
                }
                StartupRecoveryAction::Observe => {
                    spawn_task_actor(&myself, &mut state, row.id).await?;
                }
                StartupRecoveryAction::DeferResource => {}
            }
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
            SupervisorMsg::ReconcileResource { id, reply } => {
                let result = reconcile_resource_actor(&myself, state, id).await;
                send_reply(reply, result);
            }
            SupervisorMsg::LaunchAssignedResourceTask { input, reply } => {
                let result = launch_assigned_resource_task(&myself, state, *input).await;
                send_reply(reply, result);
            }
            SupervisorMsg::LaunchBoundReleaseWatcher { launch, reply } => {
                let result = launch_bound_release_watcher(&myself, state, *launch).await;
                send_reply(reply, result);
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
                    reconcile_terminal_task(&myself, state, id).await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

async fn reconcile_terminal_task(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    task_id: TaskId,
) -> Result<(), AppError> {
    let Some(task) = call(&state.store, |reply| StoreMsg::GetTask {
        id: task_id,
        reply,
    })
    .await?
    else {
        return Ok(());
    };
    if !task.state.is_terminal() {
        return Ok(());
    }

    for resource in state.resources.values() {
        resource.cast(ResourceMsg::TaskTerminal { task_id })?;
    }

    let snapshots = call(&state.store, |reply| {
        StoreMsg::ResourceSnapshotsForAuthority {
            authority_machine: state.machine,
            reply,
        }
    })
    .await?;
    for snapshot in snapshots {
        let Some(loan) = snapshot.loan else {
            continue;
        };
        let is_exact_release = snapshot.resource.registered_background_task == Some(task_id)
            && matches!(
                loan.state,
                crate::resource::LoanState::Active {
                    phase: crate::resource::LoanPhase::AwaitingRelease {
                        observed_background_task,
                        ..
                    }
                } if observed_background_task == task_id
            );
        if is_exact_release {
            reconcile_resource_actor(supervisor, state, snapshot.resource.id).await?;
        }
    }

    Ok(())
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
        spawn_resource_actor(
            supervisor,
            &mut state.resources,
            state.store.clone(),
            state.machine,
            snapshot,
        )
        .await?;
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
    spawn_resource_actor(
        supervisor,
        &mut state.resources,
        state.store.clone(),
        state.machine,
        snapshot,
    )
    .await
}

async fn reconcile_resource_actor(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    id: ResourceId,
) -> Result<(), AppError> {
    ensure_resource_actor(supervisor, state, id).await?;
    let actor = state.resources.get(&id).ok_or_else(|| AppError::Internal {
        message: format!(
            "resource actor {} disappeared during reconciliation",
            id.as_uuid()
        ),
    })?;
    call(actor, |reply| ResourceMsg::Reconcile { reply })
        .await
        .map(|_| ())
}

async fn spawn_resource_actor(
    supervisor: &ActorRef<SupervisorMsg>,
    resources: &mut HashMap<ResourceId, ActorRef<ResourceMsg>>,
    store: ActorRef<StoreMsg>,
    authority_machine: MachineId,
    snapshot: ResourceSnapshot,
) -> Result<(), AppError> {
    let id = snapshot.resource.id;
    if resources.contains_key(&id) {
        return Ok(());
    }

    let (actor, _handle) = ResourceActor::spawn_linked(
        Some(resource_actor_name(id)),
        ResourceActor,
        (
            store,
            Some(supervisor.clone()),
            authority_machine,
            snapshot.resource,
            snapshot.loan,
        ),
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

async fn launch_assigned_resource_task(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    input: ResourceTaskAcceptanceInput,
) -> Result<Result<ResourceTaskAcceptance, ResourceStoreError>, AppError> {
    let task_id = input.task_id;
    let resource_id = input.resource_id;
    let acceptance = call(&state.store, |reply| StoreMsg::AcceptAssignedResourceTask {
        input: Box::new(input),
        reply,
    })
    .await?;
    let acceptance = match acceptance {
        Ok(acceptance) => acceptance,
        Err(error) => return Ok(Err(error)),
    };

    match &acceptance {
        ResourceTaskAcceptance::Inserted { task } if *task == task_id => {
            if let Err(err) = state.home.prepare_task(task_id) {
                finish_spawn_failed(state, task_id, &err).await?;
                return Ok(Ok(acceptance));
            }
            launch_accepted(supervisor, state, task_id).await?;
        }
        ResourceTaskAcceptance::Existing {
            task,
            state: status,
        } if *task == task_id => match status {
            ProcessStatus::Queued => {
                reconcile_resource_actor(supervisor, state, resource_id).await?;
            }
            ProcessStatus::Running => spawn_task_actor(supervisor, state, task_id).await?,
            ProcessStatus::Succeeded
            | ProcessStatus::Failed
            | ProcessStatus::Cancelled
            | ProcessStatus::Lost => {}
        },
        _ => {
            return Err(AppError::Internal {
                message: format!(
                    "resource task acceptance returned an unexpected identity for {task_id}"
                ),
            });
        }
    }

    Ok(Ok(acceptance))
}

async fn launch_bound_release_watcher(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    launch: ReleaseWatcherLaunch,
) -> Result<Result<ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError>, AppError> {
    let ReleaseWatcherLaunch {
        authority_machine,
        resource_id,
        supervisor: owner,
        intent,
        executable,
    } = launch;
    let task_id = intent.watcher_task_id.as_task_id();
    let spec = ReleaseWatcherCommand::from_intent(resource_id, &intent)
        .normalized_spec(&executable, owner.thread)?;
    let (env, callback) = watcher_launch_context(state, &intent, &spec.cwd).await?;
    let row = new_queued_task(NewTask {
        id: task_id,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: persist_workload(&spec.workload),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env,
        binary: executable,
    });
    let acceptance = call(&state.store, |reply| {
        StoreMsg::AcceptReleaseWatcherForAuthority {
            input: Box::new(ReleaseWatcherAcceptanceInput {
                authority_machine,
                resource_id,
                supervisor: owner,
                intent,
                row,
                spec,
                callback,
            }),
            reply,
        }
    })
    .await?;
    let acceptance = match acceptance {
        Ok(acceptance) => acceptance,
        Err(error) => return Ok(Err(error)),
    };

    match &acceptance {
        // only the transaction that inserted the row may spawn its worker
        ReleaseWatcherAcceptance::Inserted { task } if *task == task_id => {
            if let Err(err) = state.home.prepare_task(task_id) {
                finish_spawn_failed(state, task_id, &err).await?;
                return Ok(Ok(acceptance));
            }
            launch_accepted(supervisor, state, task_id).await?;
        }
        // a queued row found again may already have a worker or a lost spawn, and a
        // free runner lock does not tell them apart, so it is only observed
        ReleaseWatcherAcceptance::Existing {
            task,
            state: task_state,
        } if *task == task_id => {
            if matches!(task_state, TaskState::Running { .. }) {
                spawn_task_actor(supervisor, state, task_id).await?;
            }
        }
        ReleaseWatcherAcceptance::UnsupportedRemoteSupervisor { .. } => {}
        ReleaseWatcherAcceptance::Inserted { .. } | ReleaseWatcherAcceptance::Existing { .. } => {
            return Err(AppError::Internal {
                message: format!(
                    "release watcher acceptance returned an unexpected identity for {task_id}"
                ),
            });
        }
    }

    Ok(Ok(acceptance))
}

/// Reuse the environment and callback saved by an earlier acceptance of this watcher
///
/// The canonical command and spec are rebuilt and compared by the store, but the
/// daemon environment can differ after a restart and must not turn an exact retry
/// into a conflict
async fn watcher_launch_context(
    state: &SupervisorState,
    intent: &ReleaseWatcherIntent,
    cwd: &std::path::Path,
) -> Result<(TaskEnv, CallbackContext), AppError> {
    let task_id = intent.watcher_task_id.as_task_id();
    let saved_row = call(&state.store, |reply| StoreMsg::GetTask {
        id: task_id,
        reply,
    })
    .await?;
    let saved_route = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request: intent.request_id,
        reply,
    })
    .await?;
    if let (Some(row), Some(route)) = (saved_row, saved_route)
        && route.task == task_id
    {
        return Ok((row.env, route.callback));
    }

    let env = TaskEnv::capture();
    let codex = resolve_agent_binary(AgentKind::Codex, &env.path, cwd)
        .unwrap_or_else(|_| state.home.root().join("unavailable-codex"));
    let callback = CallbackContext {
        env: env.clone(),
        cwd: cwd.to_path_buf(),
        codex: CallbackExecutable::available(codex),
    };
    Ok((env, callback))
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
        process_group_exit_evidence: ProcessGroupExitEvidence::NoChildSpawned,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupRecoveryAction {
    LaunchAccepted,
    Observe,
    DeferResource,
}

fn startup_recovery_action(
    row: &TaskRow,
    resource_task: Option<&AcceptedResourceTask>,
    identity: Option<&ExecutorIdentity>,
) -> StartupRecoveryAction {
    if let Some(resource_task) = resource_task {
        return match (row.status(), resource_task.state) {
            (ProcessStatus::Queued, ProcessStatus::Queued) => StartupRecoveryAction::DeferResource,
            _ => StartupRecoveryAction::Observe,
        };
    }

    if row.status() == ProcessStatus::Queued
        && matches!(
            identity,
            Some(ExecutorIdentity::Accepted(record))
                if record.origin_machine != record.execution_machine
        )
    {
        return StartupRecoveryAction::LaunchAccepted;
    }

    StartupRecoveryAction::Observe
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::io::Write;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::daemon::actors::call;
    use crate::domain::{ExitReason, TaskEnv, TaskWorkload, ThreadId, Workload};
    use crate::home::flock_exclusive;
    use crate::resource::store::{
        OpenReleaseLoanResult, ResourceTaskAcceptance, ResourceTaskAcceptanceInput,
    };
    use crate::resource::{
        AssignmentRevision, CommandSpec, Loan, LoanPhase, LoanState, ResourceQueueAttentionReason,
        ResourceQueueReconcileOutcome, ResourceRequestState, ResourceRevision, ReturnContext,
        SupervisorAddress,
    };
    use crate::spec::{NormalizedSpec, NormalizedWorkload};
    use crate::store::{NewTask, Store, new_queued_task};
    use crate::submission::{
        CallbackContext, CallbackExecutable, ExecutionRecord, NewResourceRoute, OriginRoute,
        RequestId,
    };

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

    fn resource_command_waiting_on_fifo(fifo: &std::path::Path) -> NormalizedSpec {
        serde_json::from_value(json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "resource one-shot launch test",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": {
                "type": "task",
                "command": [
                    "/bin/sh",
                    "-c",
                    format!(
                        "printf 'launch-marker\\n'; exec /bin/cat '{}'",
                        fifo.display()
                    )
                ]
            }
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

    fn seed_queued_resource_request(home: &Home, authority: MachineId) -> (Resource, TaskId) {
        let mut store = Store::open(&home.db_path()).unwrap();
        let background_task = TaskId::new();
        let resource = resource(authority, Some(background_task));
        store.register_resource(authority, &resource).unwrap();

        let spec = resource_command();
        let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
            panic!("startup seed must use a command workload");
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
        home.prepare_task(background_task).unwrap();

        (resource, background_task)
    }

    fn seed_serving_assigned_resource_task(
        home: &Home,
        authority: MachineId,
        origin: MachineId,
    ) -> (
        Resource,
        Loan,
        RequestId,
        TaskId,
        ResourceTaskAcceptanceInput,
    ) {
        seed_serving_assigned_resource_task_with_spec(home, authority, origin, resource_command())
    }

    fn seed_serving_assigned_resource_task_with_spec(
        home: &Home,
        authority: MachineId,
        origin: MachineId,
        spec: NormalizedSpec,
    ) -> (
        Resource,
        Loan,
        RequestId,
        TaskId,
        ResourceTaskAcceptanceInput,
    ) {
        let mut store = Store::open(&home.db_path()).unwrap();
        let background_task = TaskId::new();
        let request_id = RequestId::new();
        let task_id = TaskId::new();
        let mut resource = resource(authority, Some(background_task));
        resource.supervisor.thread = spec.thread;
        store.register_resource(authority, &resource).unwrap();

        let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
            panic!("resource acceptance seed must use a command workload");
        };
        let background = new_queued_task(NewTask {
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
        store.insert_task(&background).unwrap();
        store
            .cas_status(
                background_task,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();

        if origin == authority {
            store
                .insert_origin_route(
                    &OriginRoute::new_resource_waiting(NewResourceRoute {
                        request: request_id,
                        task: task_id,
                        origin_machine: origin,
                        authority_machine: authority,
                        thread: spec.thread,
                        callback: CallbackContext {
                            env: TaskEnv {
                                path: "/bin".into(),
                                home: "/tmp".into(),
                            },
                            cwd: "/tmp".into(),
                            codex: CallbackExecutable::available("/bin/echo".into()),
                        },
                        spec: spec.clone(),
                        resource: resource.id,
                    })
                    .unwrap(),
                )
                .unwrap();
        }
        let request = store
            .accept_resource_request(
                authority,
                request_id,
                task_id,
                resource.id,
                origin,
                spec.clone(),
            )
            .unwrap();
        let OpenReleaseLoanResult::Opened { .. } = store
            .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
            .unwrap()
        else {
            panic!("resource acceptance seed must open one release loan");
        };
        store
            .cas_exit(
                background_task,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
            )
            .unwrap()
            .unwrap();
        let (loan, state_revision) = store
            .seed_verified_serving_loan_for_test(
                authority,
                resource.id,
                request.request_id,
                ReturnContext::AlreadyCompleted {
                    task_id: background_task,
                    result_ref: "result-1".into(),
                },
            )
            .unwrap();
        let input = ResourceTaskAcceptanceInput {
            authority_machine: authority,
            resource_id: resource.id,
            request_id,
            task_id,
            acceptance_sequence: request.acceptance_sequence,
            loan_id: loan.id,
            expected_state_revision: state_revision,
            command_spec: CommandSpec::try_from(spec).unwrap(),
            executor_env: TaskEnv::capture(),
        };
        home.prepare_task(task_id).unwrap();

        (resource, loan, request_id, task_id, input)
    }

    fn seed_accepted_resource_task(
        home: &Home,
        authority: MachineId,
        origin: MachineId,
    ) -> (Resource, Loan, RequestId, TaskId) {
        let (resource, loan, request_id, task_id, input) =
            seed_serving_assigned_resource_task(home, authority, origin);
        assert!(matches!(
            Store::open(&home.db_path())
                .unwrap()
                .accept_assigned_resource_task(input)
                .unwrap(),
            ResourceTaskAcceptance::Inserted { task } if task == task_id
        ));

        (resource, loan, request_id, task_id)
    }

    fn configure_task_runner() {
        crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
    }

    async fn acquire_runner_lock_after_task_exit(home: &Home, task_id: TaskId) -> std::fs::File {
        let lock_path = home.task_paths(task_id).runner_lock;
        tokio::task::spawn_blocking(move || flock_exclusive(&lock_path, LockMode::Blocking))
            .await
            .unwrap()
            .unwrap()
    }

    async fn call_assigned_resource_launch(
        supervisor: &ActorRef<SupervisorMsg>,
        input: ResourceTaskAcceptanceInput,
    ) -> Result<ResourceTaskAcceptance, ResourceStoreError> {
        call(supervisor, |reply| {
            SupervisorMsg::LaunchAssignedResourceTask {
                input: Box::new(input),
                reply,
            }
        })
        .await
        .unwrap()
    }

    async fn wait_for_running_task(
        store: &ActorRef<StoreMsg>,
        home: &Home,
        task_id: TaskId,
    ) -> TaskRow {
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let row = call(store, |reply| StoreMsg::GetTask { id: task_id, reply })
                    .await
                    .unwrap()
                    .unwrap();
                if row.status() == ProcessStatus::Running && row.pid().is_some() {
                    return row;
                }

                tokio::task::yield_now().await;
            }
        })
        .await;
        match result {
            Ok(row) => row,
            Err(_) => {
                let row = call(store, |reply| StoreMsg::GetTask { id: task_id, reply })
                    .await
                    .unwrap()
                    .unwrap();
                panic!(
                    "task {task_id} did not run: state={:?}, reason={:?}, output={:?}",
                    row.state,
                    row.exit_reason(),
                    std::fs::read_to_string(home.task_paths(task_id).output)
                );
            }
        }
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
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
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
    async fn daemon_startup_reconciles_accepted_work_without_relaunching_background_task() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, background_task) = seed_queued_resource_request(&home, authority);
        let runner_lock = flock_exclusive(
            &home.task_paths(background_task).runner_lock,
            LockMode::NonBlocking,
        )
        .unwrap();

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
        assert!(matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::ReleaseProofUnavailable { .. })
        ));
        assert!(matches!(
            inspection.loan.map(|loan| loan.state),
            Some(crate::resource::LoanState::Active {
                phase: crate::resource::LoanPhase::AwaitingRelease {
                    observed_background_task: saved_task,
                    ..
                }
            }) if saved_task == background_task
        ));
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        assert_eq!(
            call(&store, |reply| StoreMsg::PendingSupervisorNotices { reply })
                .await
                .unwrap()
                .unwrap()
                .len(),
            1
        );

        stop_supervisor(supervisor, handle).await;
        drop(runner_lock);
    }

    #[tokio::test]
    async fn restart_defers_accepted_queued_resource_tasks_from_local_and_remote_origins() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        for local_origin in [false, true] {
            let directory = tempdir().unwrap();
            let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
            home.ensure().unwrap();
            let authority = load_or_create_machine_id(&home).unwrap();
            let origin = if local_origin {
                authority
            } else {
                MachineId::new()
            };
            let (resource, loan, request_id, task_id) =
                seed_accepted_resource_task(&home, authority, origin);

            let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
                .await
                .unwrap();
            let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
            assert!(matches!(
                &inspection.reconcile_outcome,
                Some(ResourceQueueReconcileOutcome::AttentionRequired {
                    request,
                    reason: ResourceQueueAttentionReason::AssignedTaskLaunchUncertain {
                        task_id: attention_task,
                    },
                }) if request.request_id == request_id
                    && request.task_id == task_id
                    && *attention_task == task_id
            ));
            assert!(matches!(
                &inspection.loan,
                Some(Loan {
                    id,
                    state: LoanState::Active {
                        phase: LoanPhase::Serving {
                            current_request_id, ..
                        },
                    },
                    ..
                }) if *id == loan.id && *current_request_id == request_id
            ));

            let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
                .await
                .unwrap();
            let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.status(), ProcessStatus::Queued);
            assert_eq!(row.pid(), None);
            assert!(!home.task_paths(task_id).output.exists());
            assert!(matches!(
                call(&store, |reply| StoreMsg::ResourceRequests {
                    authority_machine: authority,
                    resource_id: resource.id,
                    reply,
                })
                .await
                .unwrap()[0]
                    .state,
                ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
            ));

            let runner_lock =
                flock_exclusive(&home.task_paths(task_id).runner_lock, LockMode::NonBlocking)
                    .unwrap();
            drop(runner_lock);
            stop_supervisor(supervisor, handle).await;
        }
    }

    #[tokio::test]
    async fn restart_observes_accepted_running_resource_task_without_relaunching_it() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, loan, request_id, task_id) =
            seed_accepted_resource_task(&home, authority, MachineId::new());
        let saved = Store::open(&home.db_path()).unwrap();
        saved
            .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();
        drop(saved);
        let runner_lock =
            flock_exclusive(&home.task_paths(task_id).runner_lock, LockMode::NonBlocking).unwrap();

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
        assert!(matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::LoanAlreadyActive { loan: ref active })
                if active.id == loan.id
        ));
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status(), ProcessStatus::Running);
        assert_eq!(row.pid(), None);
        assert!(matches!(
            inspection.loan.map(|active| active.state),
            Some(LoanState::Active {
                phase: LoanPhase::Serving {
                    current_request_id: current,
                    ..
                },
            }) if current == request_id
        ));

        stop_supervisor(supervisor, handle).await;
        drop(runner_lock);
    }

    #[tokio::test]
    async fn fresh_serving_assignment_starts_one_command_and_retry_only_observes_it() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let fifo = directory.path().join("command-gate");
        nix::unistd::mkfifo(
            &fifo,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )
        .unwrap();
        let spec = resource_command_waiting_on_fifo(&fifo);
        let (resource, loan, request_id, task_id, input) =
            seed_serving_assigned_resource_task_with_spec(&home, authority, authority, spec);

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let running = wait_for_running_task(&store, &home, task_id).await;
        assert_eq!(running.id, task_id);
        assert!(running.pid().is_some_and(|pid| pid > 0));
        assert!(matches!(
            call_assigned_resource_launch(&supervisor, input.clone()).await,
            Ok(ResourceTaskAcceptance::Existing {
                task,
                state: ProcessStatus::Running,
            }) if task == task_id
        ));

        tokio::task::spawn_blocking(move || {
            let mut writer = std::fs::OpenOptions::new().write(true).open(fifo)?;
            writer.write_all(b"released\n")
        })
        .await
        .unwrap()
        .unwrap();
        let runner_lock = acquire_runner_lock_after_task_exit(&home, task_id).await;
        let output = std::fs::read_to_string(home.task_paths(task_id).output).unwrap();
        assert_eq!(output, "launch-marker\nreleased\n");
        let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status(), ProcessStatus::Succeeded);
        assert!(matches!(
            row.exit_reason(),
            Some(ExitReason::Exit { code: 0 })
        ));
        assert_eq!(
            call(&store, |reply| StoreMsg::GetProcessGroupExitEvidence {
                id: task_id,
                reply,
            })
            .await
            .unwrap(),
            Some(ProcessGroupExitEvidence::ConfirmedExited)
        );
        assert_eq!(resource.id, input.resource_id);
        assert_eq!(loan.id, input.loan_id);
        assert_eq!(request_id, input.request_id);
        drop(runner_lock);
        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn existing_queued_assignment_retries_without_launch_even_with_free_lock() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, _loan, request_id, task_id, input) =
            seed_serving_assigned_resource_task(&home, authority, authority);
        assert!(matches!(
            Store::open(&home.db_path())
                .unwrap()
                .accept_assigned_resource_task(input.clone())
                .unwrap(),
            ResourceTaskAcceptance::Inserted { task } if task == task_id
        ));

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
        assert!(matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::AttentionRequired {
                request,
                reason: ResourceQueueAttentionReason::AcceptedTaskLaunchUncertain {
                    task_id: attention_task,
                },
            }) if request.request_id == request_id
                && request.task_id == task_id
                && attention_task == task_id
        ));
        let free_lock =
            flock_exclusive(&home.task_paths(task_id).runner_lock, LockMode::NonBlocking).unwrap();
        drop(free_lock);

        for _ in 0..2 {
            assert!(matches!(
                call_assigned_resource_launch(&supervisor, input.clone()).await,
                Ok(ResourceTaskAcceptance::Existing {
                    task,
                    state: ProcessStatus::Queued,
                }) if task == task_id
            ));
        }

        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status(), ProcessStatus::Queued);
        assert_eq!(row.pid(), None);
        assert!(!home.task_paths(task_id).output.exists());
        let free_lock =
            flock_exclusive(&home.task_paths(task_id).runner_lock, LockMode::NonBlocking).unwrap();
        drop(free_lock);
        let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
        assert!(matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::AttentionRequired {
                reason: ResourceQueueAttentionReason::AcceptedTaskLaunchUncertain {
                    task_id: attention_task,
                },
                ..
            }) if attention_task == task_id
        ));
        assert!(matches!(
            inspection.loan.map(|saved| saved.state),
            Some(LoanState::Active {
                phase: LoanPhase::Serving {
                    current_request_id: current,
                    ..
                },
            }) if current == request_id
        ));

        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn cancellation_before_acceptance_prevents_resource_task_spawn() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, _loan, _request_id, task_id, input) =
            seed_serving_assigned_resource_task(&home, authority, authority);
        Store::open(&home.db_path())
            .unwrap()
            .cancel_resource_request_before_activation(
                authority,
                input.request_id,
                task_id,
                resource.id,
                authority,
            )
            .unwrap();

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let result = call(&supervisor, |reply| {
            SupervisorMsg::LaunchAssignedResourceTask {
                input: Box::new(input),
                reply,
            }
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(ResourceStoreError::Prevented)));
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        assert!(
            call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                .is_none()
        );
        assert!(!home.task_paths(task_id).output.exists());
        assert!(!home.task_paths(task_id).runner_lock.exists());

        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn failed_first_resource_spawn_records_no_child_proof_and_reserves_return() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, loan, _request_id, task_id, _input) =
            seed_serving_assigned_resource_task(&home, authority, authority);
        crate::runner::set_task_run_executable_for_tests(
            directory.path().join("missing-homebased-binary"),
        );

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let row = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(row) = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
                    .await
                    .unwrap()
                    && row.status() == ProcessStatus::Failed
                {
                    return row;
                }

                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed resource spawn must reach a durable failed state");
        assert_eq!(row.status(), ProcessStatus::Failed);
        assert!(matches!(
            row.exit_reason(),
            Some(ExitReason::SpawnFailed { .. })
        ));
        assert_eq!(
            call(&store, |reply| StoreMsg::GetProcessGroupExitEvidence {
                id: task_id,
                reply,
            })
            .await
            .unwrap(),
            Some(ProcessGroupExitEvidence::NoChildSpawned)
        );
        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource.id,
            reply,
        })
        .await
        .unwrap();
        let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
        assert!(matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::LoanAlreadyActive {
                loan: Loan {
                    id,
                    state: LoanState::Active {
                        phase: LoanPhase::AwaitingReturn { .. },
                    },
                    ..
                }
            }) if id == loan.id
        ));
        let snapshot = call(&store, |reply| StoreMsg::ResourceSnapshotsForAuthority {
            authority_machine: authority,
            reply,
        })
        .await
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource.id)
        .unwrap();
        assert!(matches!(
            snapshot.loan,
            Some(Loan {
                id,
                state: LoanState::Active {
                    phase: LoanPhase::AwaitingReturn { .. },
                },
                ..
            }) if id == loan.id
        ));
        assert!(matches!(
            call(&store, |reply| StoreMsg::ResourceRequests {
                authority_machine: authority,
                resource_id: resource.id,
                reply,
            })
            .await
            .unwrap()[0]
                .state,
            ResourceRequestState::Finished {
                outcome: ExitReason::SpawnFailed { .. }
            }
        ));
        let notices = call(&store, |reply| StoreMsg::PendingSupervisorNotices { reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].loan_id, loan.id);

        let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.id, task_id);

        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn direct_launch_still_starts_its_command() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let spec = resource_command();
        let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
            panic!("direct launch test must use a command workload");
        };
        let id = TaskId::new();
        let row = new_queued_task(NewTask {
            id,
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
        home.prepare_task(id).unwrap();

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        call(&supervisor, |reply| SupervisorMsg::Launch {
            row: Box::new(row),
            spec: Box::new(spec),
            reply,
        })
        .await
        .unwrap();
        let runner_lock = acquire_runner_lock_after_task_exit(&home, id).await;
        assert_eq!(
            std::fs::read_to_string(home.task_paths(id).output).unwrap(),
            "hello\n"
        );
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let row = call(&store, |reply| StoreMsg::GetTask { id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status(), ProcessStatus::Succeeded);

        drop(runner_lock);
        stop_supervisor(supervisor, handle).await;
    }

    #[test]
    fn direct_queued_tasks_keep_their_existing_startup_actions() {
        let spec = resource_command();
        let id = TaskId::new();
        let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
            panic!("startup test must use a command workload");
        };
        let row = new_queued_task(NewTask {
            id,
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
        assert_eq!(
            startup_recovery_action(&row, None, None),
            StartupRecoveryAction::Observe
        );

        let origin_machine = MachineId::new();
        let mut execution_machine = MachineId::new();
        while execution_machine == origin_machine {
            execution_machine = MachineId::new();
        }
        let identity = ExecutorIdentity::Accepted(ExecutionRecord {
            task: id,
            origin_machine,
            execution_machine,
            spec: spec.into(),
            state: ProcessStatus::Queued,
        });
        assert_eq!(
            startup_recovery_action(&row, None, Some(&identity)),
            StartupRecoveryAction::LaunchAccepted
        );
    }

    #[tokio::test]
    async fn resource_registration_retry_keeps_one_actor_identity() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
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
