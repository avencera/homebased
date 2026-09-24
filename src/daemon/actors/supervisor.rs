//! Root supervisor: store, callback, and per-task and per-resource actors.

use std::collections::HashMap;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};

use crate::daemon::actors::callback::{CallbackActor, CallbackArgs, CallbackMsg};
use crate::daemon::actors::resource::{
    BackgroundLaunchResult, ReleaseWatcherLaunchResult, ResourceActor, ResourceActorInspection,
    ResourceMsg, RestoreLaunchResult, resource_actor_name, resource_id_from_actor_name,
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
use crate::resource::background_launch::RemoteBackgroundLaunchReceipt;
use crate::resource::bound_action::{
    ActionTaskAcceptance, ActionTaskIdentity, ActionTaskReceipt, PreparedActionTask,
    ResourceActionKind, ResourceActionOperation, ResourceActionOutcome, ResourceActionRejection,
    ResourceActionRequest,
};
use crate::resource::release_watcher::ReleaseWatcherCommand;
use crate::resource::store::{
    AcceptedResourceTask, ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError,
    ReleaseWatcherAcceptanceInput, ResourceSnapshot, ResourceStoreError, ResourceTaskAcceptance,
    ResourceTaskAcceptanceInput,
};
use crate::resource::{
    ReleaseWatcherIntent, Resource, ResourceId, ReturnDecision, ReturnLaunch,
    SupervisorActionAuthority, SupervisorAddress,
};
use crate::runner;
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::{
    AcceptedActionTask, BackgroundLaunchAcceptance, BackgroundLaunchError, BackgroundLaunchInput,
    CancelResult, EndedRestoreResolution, NewTask, RemoteBackgroundLaunchInput,
    RemoteReleaseWatcherAcceptanceInput, ResourceActionError, ReturnClosure, ReturnDecisionError,
    ReturnTaskAcceptance, ReturnTaskAcceptanceInput, ReturnTaskOrigin, new_queued_task,
};
use crate::submission::{
    CallbackContext, CallbackExecutable, ExecutionRecord, ExecutorIdentity, RequestId,
};
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
    /// Apply one supervisor return decision and spawn only a newly inserted return task
    DecideReturn {
        /// Exact supervisor authority for the pending return action
        authority: SupervisorActionAuthority,
        /// Typed no-resume or launch decision
        decision: Box<ReturnDecision>,
        /// Typed decision result, nested inside actor and storage errors
        // keep the resource storage types private to the actor protocol
        #[allow(private_interfaces)]
        reply: RpcReplyPort<Result<Result<ReturnDecisionOutcome, ReturnDecisionError>, AppError>>,
    },
    /// Apply one validated operation from a remote supervisor's machine
    ///
    /// The cluster route has already checked the destination, source machine, and
    /// saved callback-route evidence. Only an insertion by this call spawns a task
    ResourceAction {
        /// Validated request from the supervisor machine
        request: Box<ResourceActionRequest>,
        /// Typed authority outcome inside actor and storage errors
        reply: RpcReplyPort<Result<ResourceActionOutcome, AppError>>,
    },
    /// Close a Restoring loan after the supervisor resolves a return task that ended early
    // keep the resource storage types private to the actor protocol
    #[allow(private_interfaces)]
    ResolveEndedRestore {
        /// Exact authority, bound task, and supervisor reason
        resolution: Box<EndedRestoreResolution>,
        /// Typed closure result, nested inside actor and storage errors
        reply: RpcReplyPort<Result<Result<ReturnClosure, ReturnDecisionError>, AppError>>,
    },
    /// Bind one first background launch and spawn only a newly inserted task
    // keep the resource storage types private to the actor protocol
    #[allow(private_interfaces)]
    LaunchBackground {
        /// Stable request, full spec, and co-located executor context
        launch: Box<BackgroundLaunch>,
        /// Typed launch result, nested inside actor and storage errors
        reply: RpcReplyPort<
            Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError>,
        >,
    },
    /// Bind one remote supervisor's first background launch and spawn only a new insertion
    ///
    /// The cluster route has already checked the destination, source machine, and
    /// saved callback-route evidence
    // keep the resource storage types private to the actor protocol
    #[allow(private_interfaces)]
    LaunchRemoteBackground {
        /// Exact receipt that the supervisor machine saved, and the saved spec
        launch: Box<RemoteBackgroundLaunch>,
        /// Typed launch result, nested inside actor and storage errors
        reply: RpcReplyPort<
            Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError>,
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
    /// Wake resource owners after an executor event for a remote origin is ready to send
    ///
    /// The origin inbox is on another machine, so this is the authority-side hint
    /// that an action-bound task may have reached its running boundary
    RemoteOriginEvent { id: TaskId },
}

/// Authority-owned inputs for one release-watcher launch with a local callback route
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

/// Caller inputs for one first background launch on this authority
///
/// The supervisor preallocates the task identity; the store keeps the identity
/// saved by an earlier exact launch with the same request
pub(crate) struct BackgroundLaunch {
    /// Resource whose background slot receives the task
    pub(crate) resource_id: ResourceId,
    /// Stable caller retry identity
    pub(crate) request_id: RequestId,
    /// Full normalized command spec
    pub(crate) spec: NormalizedSpec,
    /// Executor environment captured by the co-located supervisor
    pub(crate) env: TaskEnv,
    /// Directory used to find the callback Codex executable
    pub(crate) callback_cwd: PathBuf,
}

/// Remote supervisor inputs for one first background launch on this authority
///
/// The supervisor machine fixed every identity before it sent the launch; the
/// authority supplies only its own executor environment
pub(crate) struct RemoteBackgroundLaunch {
    /// Fixed identities, digest, supervisor assignment, and observed revision
    pub(crate) receipt: RemoteBackgroundLaunchReceipt,
    /// Full normalized spec saved in the supervisor's route
    pub(crate) spec: NormalizedSpec,
}

/// Durable result of one supervisor return decision
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReturnDecisionOutcome {
    /// The no-resume decision closed the loan
    Closed(Box<ReturnClosure>),
    /// The launch decision bound a task, or found the exact earlier binding
    Launch(ReturnTaskAcceptance),
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
        let restoring_tasks = call(&state.store, |reply| StoreMsg::RestoringTasksForAuthority {
            authority_machine: state.machine,
            reply,
        })
        .await?;
        let background_launches = call(&state.store, |reply| {
            StoreMsg::QueuedBackgroundLaunchTasksForAuthority {
                authority_machine: state.machine,
                reply,
            }
        })
        .await?;
        let watcher_tasks = call(&state.store, |reply| {
            StoreMsg::ReleaseWatcherTasksForAuthority {
                authority_machine: state.machine,
                reply,
            }
        })
        .await?;
        let rows = call(&state.store, |reply| StoreMsg::NonTerminal { reply }).await?;
        for row in rows {
            // a queued return or first background task may have lost its spawn, and a
            // free runner lock cannot prove otherwise, so it stays queued for resource attention
            if row.status() == ProcessStatus::Queued
                && (restoring_tasks.contains(&row.id) || background_launches.contains(&row.id))
            {
                continue;
            }
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
                watcher_tasks.contains(&row.id),
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
            SupervisorMsg::DecideReturn {
                authority,
                decision,
                reply,
            } => {
                let result = decide_return(&myself, state, authority, *decision).await;
                send_reply(reply, result);
            }
            SupervisorMsg::ResourceAction { request, reply } => {
                let result = resource_action(&myself, state, *request).await;
                send_reply(reply, result);
            }
            SupervisorMsg::RemoteOriginEvent { id } => {
                for resource in state.resources.values() {
                    resource.cast(ResourceMsg::TaskProgress { task_id: id })?;
                }
            }
            SupervisorMsg::ResolveEndedRestore { resolution, reply } => {
                let resource_id = resolution.authority.resource_id;
                let result = call(&state.store, |reply| {
                    StoreMsg::ResolveEndedRestoreForAuthority { resolution, reply }
                })
                .await;
                if matches!(result, Ok(Ok(_))) {
                    wake_resource(state, resource_id);
                }
                send_reply(reply, result);
            }
            SupervisorMsg::LaunchBackground { launch, reply } => {
                let result = launch_background(&myself, state, *launch).await;
                send_reply(reply, result);
            }
            SupervisorMsg::LaunchRemoteBackground { launch, reply } => {
                let result = launch_remote_background(&myself, state, *launch).await;
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
                // a delivered state event may confirm the start of a bound return task
                for resource in state.resources.values() {
                    resource.cast(ResourceMsg::TaskProgress { task_id: id })?;
                }
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

async fn decide_return(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    authority: SupervisorActionAuthority,
    decision: ReturnDecision,
) -> Result<Result<ReturnDecisionOutcome, ReturnDecisionError>, AppError> {
    let launch = match decision {
        ReturnDecision::NoResume { reason } => {
            let result = call(&state.store, |reply| StoreMsg::RecordNoResumeForAuthority {
                authority,
                reason,
                reply,
            })
            .await?;
            if result.is_ok() {
                wake_resource(state, authority.resource_id);
            }
            return Ok(result.map(|closure| ReturnDecisionOutcome::Closed(Box::new(closure))));
        }
        ReturnDecision::Launch(launch) => *launch,
    };

    let executor_env = TaskEnv::capture();
    let callback_cwd = match launch.work.supervisor_spec() {
        Some(spec) => spec.as_normalized().cwd.clone(),
        None => state.home.root().to_path_buf(),
    };
    let codex = resolve_agent_binary(AgentKind::Codex, &executor_env.path, &callback_cwd)
        .unwrap_or_else(|_| state.home.root().join("unavailable-codex"));
    launch_return_task(
        supervisor,
        state,
        authority,
        launch,
        executor_env,
        ReturnTaskOrigin::Local {
            callback_codex: CallbackExecutable::available(codex),
        },
    )
    .await
    .map(|result| result.map(ReturnDecisionOutcome::Launch))
}

/// Bind one fixed return task, and spawn it only when this call inserted it
async fn launch_return_task(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    authority: SupervisorActionAuthority,
    launch: ReturnLaunch,
    executor_env: TaskEnv,
    origin: ReturnTaskOrigin,
) -> Result<Result<ReturnTaskAcceptance, ReturnDecisionError>, AppError> {
    let task_id = launch.task_id;
    let resource = state.resources.get(&authority.resource_id).cloned();
    let report = |result| {
        if let Some(resource) = &resource
            && let Err(error) = resource.cast(ResourceMsg::RestoreLaunchFinished {
                action_id: authority.action_id,
                task_id,
                result,
            })
        {
            tracing::debug!(%task_id, "return launch result cast: {error}");
        }
    };
    if let Some(resource) = &resource {
        resource.cast(ResourceMsg::RestoreLaunchStarted {
            action_id: authority.action_id,
            task_id,
        })?;
    }

    let acceptance = call(&state.store, |reply| {
        StoreMsg::AcceptReturnTaskForAuthority {
            input: Box::new(ReturnTaskAcceptanceInput {
                authority,
                launch,
                executor_env,
                origin,
            }),
            reply,
        }
    })
    .await;
    let acceptance = match acceptance {
        Ok(Ok(acceptance)) => acceptance,
        Ok(Err(error)) => {
            report(RestoreLaunchResult::NotInserted);
            return Ok(Err(error));
        }
        Err(error) => {
            report(RestoreLaunchResult::NotInserted);
            return Err(error);
        }
    };

    let result = match &acceptance {
        // only the transaction that inserted the row may spawn its worker
        ReturnTaskAcceptance::Inserted { task, .. } if *task == task_id => {
            let started = match state.home.prepare_task(task_id) {
                Ok(_) => launch_accepted(supervisor, state, task_id).await,
                Err(error) => finish_spawn_failed(state, task_id, &error).await,
            };
            if let Err(error) = started {
                // the committed row may have no worker, so it must show as attention
                report(RestoreLaunchResult::NotInserted);
                return Err(error);
            }
            RestoreLaunchResult::Inserted
        }
        // an existing binding is observed from durable state and never respawned
        ReturnTaskAcceptance::Existing {
            task,
            state: status,
        } if *task == task_id => {
            if *status == ProcessStatus::Running {
                spawn_task_actor(supervisor, state, task_id).await?;
            }
            RestoreLaunchResult::Existing { state: *status }
        }
        ReturnTaskAcceptance::UnsupportedRemoteSupervisor { .. } => {
            RestoreLaunchResult::NotInserted
        }
        ReturnTaskAcceptance::Inserted { .. } | ReturnTaskAcceptance::Existing { .. } => {
            report(RestoreLaunchResult::NotInserted);
            return Err(AppError::Internal {
                message: format!(
                    "return task acceptance returned an unexpected identity for {task_id}"
                ),
            });
        }
    };
    report(result);

    Ok(Ok(acceptance))
}

/// Bind one co-located first background launch, and spawn it only when this call inserted it
async fn launch_background(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    launch: BackgroundLaunch,
) -> Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError> {
    let BackgroundLaunch {
        resource_id,
        request_id,
        spec,
        env,
        callback_cwd,
    } = launch;
    let codex = resolve_agent_binary(AgentKind::Codex, &env.path, &callback_cwd)
        .unwrap_or_else(|_| state.home.root().join("unavailable-codex"));
    let input = BackgroundLaunchInput {
        authority_machine: state.machine,
        resource_id,
        request_id,
        task_id: TaskId::new(),
        spec,
        env,
        callback_codex: CallbackExecutable::available(codex),
    };
    bind_background_launch(supervisor, state, resource_id, request_id, |reply| {
        StoreMsg::AcceptBackgroundLaunchForAuthority {
            input: Box::new(input),
            reply,
        }
    })
    .await
}

/// Bind one remote supervisor's first background launch, and spawn it only on insertion
///
/// The task runs with this authority's executor environment. The callback
/// context stays in the route that the supervisor machine saved
async fn launch_remote_background(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    launch: RemoteBackgroundLaunch,
) -> Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError> {
    let RemoteBackgroundLaunch { receipt, spec } = launch;
    let resource_id = receipt.binding.assignment.resource_id;
    ensure_resource_actor(supervisor, state, resource_id).await?;
    let input = RemoteBackgroundLaunchInput {
        receipt,
        spec,
        env: TaskEnv::capture(),
    };
    bind_background_launch(
        supervisor,
        state,
        resource_id,
        receipt.request_id,
        |reply| StoreMsg::AcceptRemoteBackgroundLaunchForAuthority {
            input: Box::new(input),
            reply,
        },
    )
    .await
}

/// Apply one store launch binding, and spawn the task only when the store inserted it
///
/// An existing launch is observed from durable state. A queued row found again
/// may already have a worker or may have lost its spawn, and a free runner lock
/// cannot tell them apart, so it is never respawned here
async fn bind_background_launch(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    resource_id: ResourceId,
    request_id: RequestId,
    accept: impl FnOnce(
        RpcReplyPort<Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError>>,
    ) -> StoreMsg,
) -> Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError> {
    let resource = state.resources.get(&resource_id).cloned();
    let report = |result| {
        if let Some(resource) = &resource
            && let Err(error) =
                resource.cast(ResourceMsg::BackgroundLaunchFinished { request_id, result })
        {
            tracing::debug!(request = %request_id.0, "background launch result cast: {error}");
        }
    };
    if let Some(resource) = &resource {
        resource.cast(ResourceMsg::BackgroundLaunchStarted { request_id })?;
    }

    let acceptance = match call(&state.store, accept).await {
        Ok(Ok(acceptance)) => acceptance,
        Ok(Err(error)) => {
            report(BackgroundLaunchResult::NotInserted);
            return Ok(Err(error));
        }
        Err(error) => {
            // the store may have committed; the resource owner treats a queued row as uncertain
            report(BackgroundLaunchResult::NotInserted);
            return Err(error);
        }
    };

    let result = match &acceptance {
        // only the transaction that inserted the row may spawn its worker
        BackgroundLaunchAcceptance::Inserted { task, .. } => {
            let task_id = *task;
            let started = match state.home.prepare_task(task_id) {
                Ok(_) => launch_accepted(supervisor, state, task_id).await,
                Err(error) => finish_spawn_failed(state, task_id, &error).await,
            };
            if let Err(error) = started {
                // the committed row may have no worker, so it must show as uncertain
                report(BackgroundLaunchResult::NotInserted);
                return Err(error);
            }
            BackgroundLaunchResult::Inserted
        }
        BackgroundLaunchAcceptance::Existing {
            task,
            state: status,
        } => {
            if *status == ProcessStatus::Running {
                spawn_task_actor(supervisor, state, *task).await?;
            }
            BackgroundLaunchResult::Existing
        }
        BackgroundLaunchAcceptance::UnsupportedRemoteSupervisor { .. } => {
            BackgroundLaunchResult::NotInserted
        }
    };
    report(result);

    Ok(Ok(acceptance))
}

/// Apply one validated remote-supervisor operation on this authority
///
/// Prepare operations only derive or bind identities. Launch operations commit
/// the task records and exact receipt before any spawn, and only an insertion
/// by this call starts a worker
async fn resource_action(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    request: ResourceActionRequest,
) -> Result<ResourceActionOutcome, AppError> {
    let authority = request.authority;
    match request.operation {
        ResourceActionOperation::PrepareReleaseWatcher {
            observed_background_task,
        } => {
            ensure_resource_actor(supervisor, state, authority.resource_id).await?;
            let actor =
                state
                    .resources
                    .get(&authority.resource_id)
                    .ok_or_else(|| AppError::Internal {
                        message: format!(
                            "resource actor {} disappeared",
                            authority.resource_id.as_uuid()
                        ),
                    })?;
            let prepared = call(actor, |reply| ResourceMsg::PrepareRemoteWatcher {
                authority,
                observed_background_task,
                reply,
            })
            .await?;
            Ok(match prepared {
                Ok(task) => ResourceActionOutcome::Prepared { task },
                Err(reason) => ResourceActionOutcome::Rejected { reason },
            })
        }
        ResourceActionOperation::LaunchReleaseWatcher {
            observed_background_task,
            task,
        } => {
            launch_remote_release_watcher(
                supervisor,
                state,
                authority,
                observed_background_task,
                task,
            )
            .await
        }
        ResourceActionOperation::PrepareReturn { launch } => {
            let (request_id, task_id) = (launch.request_id, launch.task_id);
            let prepared = call(&state.store, |reply| {
                StoreMsg::PrepareReturnTaskForAuthority {
                    authority,
                    launch: Box::new(launch),
                    executor_env: TaskEnv::capture(),
                    reply,
                }
            })
            .await?;
            match prepared {
                Ok(prepared) => Ok(ResourceActionOutcome::Prepared {
                    task: PreparedActionTask {
                        request_id,
                        task_id,
                        spec: prepared.spec,
                        normalized_spec_sha256: prepared.normalized_spec_sha256,
                    },
                }),
                Err(error) => return_rejection(error),
            }
        }
        ResourceActionOperation::LaunchReturn {
            launch,
            normalized_spec_sha256,
        } => {
            let receipt = ActionTaskReceipt {
                kind: ResourceActionKind::Return,
                authority,
                request_id: launch.request_id,
                task_id: launch.task_id,
                normalized_spec_sha256,
            };
            let accepted = launch_return_task(
                supervisor,
                state,
                authority,
                launch,
                TaskEnv::capture(),
                ReturnTaskOrigin::Remote {
                    normalized_spec_sha256,
                },
            )
            .await?;
            let acceptance = match accepted {
                Ok(ReturnTaskAcceptance::Inserted { .. }) => ActionTaskAcceptance::Inserted,
                Ok(ReturnTaskAcceptance::Existing { state, .. }) => {
                    ActionTaskAcceptance::Existing { state }
                }
                Ok(ReturnTaskAcceptance::UnsupportedRemoteSupervisor { .. }) => {
                    return Ok(ResourceActionOutcome::Rejected {
                        reason: ResourceActionRejection::NotCurrentSupervisor,
                    });
                }
                Err(error) => return return_rejection(error),
            };
            Ok(ResourceActionOutcome::Accepted {
                receipt,
                acceptance,
            })
        }
        ResourceActionOperation::NoResume { reason } => {
            let closed = call(&state.store, |reply| StoreMsg::RecordNoResumeForAuthority {
                authority,
                reason,
                reply,
            })
            .await?;
            closed_outcome(state, authority.resource_id, closed)
        }
        ResourceActionOperation::ResolveEndedRestore { task_id, reason } => {
            let closed = call(&state.store, |reply| {
                StoreMsg::ResolveEndedRestoreForAuthority {
                    resolution: Box::new(EndedRestoreResolution {
                        authority,
                        task_id,
                        reason,
                    }),
                    reply,
                }
            })
            .await?;
            closed_outcome(state, authority.resource_id, closed)
        }
    }
}

/// Accept a remote supervisor's bound watcher and spawn only a fresh insertion
async fn launch_remote_release_watcher(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    authority: SupervisorActionAuthority,
    observed_background_task: TaskId,
    task: ActionTaskIdentity,
) -> Result<ResourceActionOutcome, AppError> {
    let task_id = task.task_id;
    let executable = crate::resource::release_watcher::release_watcher_executable()?;
    // the authority rebuilds its own command; the store rejects any other identity
    let spec = ReleaseWatcherCommand {
        resource_id: authority.resource_id,
        action_id: authority.action_id,
        state_revision: authority.expected_state_revision,
        trainer_task_id: observed_background_task,
        watcher_task_id: crate::resource::ReleaseWatcherTaskId::new(task_id),
    }
    .normalized_spec(&executable, authority.supervisor.thread)?;
    let row = new_queued_task(NewTask {
        id: task_id,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: persist_workload(&spec.workload),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: TaskEnv::capture(),
        binary: executable,
    });
    let resource = state.resources.get(&authority.resource_id).cloned();
    let report = |result| {
        if let Some(resource) = &resource
            && let Err(error) = resource.cast(ResourceMsg::WatcherLaunchFinished {
                action_id: authority.action_id,
                watcher_task_id: task_id,
                result,
            })
        {
            tracing::debug!(%task_id, "remote watcher launch result cast: {error}");
        }
    };
    if let Some(resource) = &resource {
        resource.cast(ResourceMsg::RemoteWatcherLaunchStarted {
            action_id: authority.action_id,
            watcher_task_id: task_id,
        })?;
    }

    let accepted = call(&state.store, |reply| {
        StoreMsg::AcceptRemoteReleaseWatcherForAuthority {
            input: Box::new(RemoteReleaseWatcherAcceptanceInput {
                authority,
                observed_background_task,
                task,
                row,
                spec,
            }),
            reply,
        }
    })
    .await;
    let AcceptedActionTask {
        receipt,
        acceptance,
    } = match accepted {
        Ok(Ok(accepted)) => accepted,
        Ok(Err(ResourceActionError::Rejected(reason))) => {
            report(ReleaseWatcherLaunchResult::Rejected);
            return Ok(ResourceActionOutcome::Rejected { reason });
        }
        Ok(Err(ResourceActionError::Storage(error))) | Err(error) => {
            report(ReleaseWatcherLaunchResult::Uncertain);
            return Err(error);
        }
    };

    let result = match acceptance {
        // only the transaction that inserted the row may spawn its worker
        ActionTaskAcceptance::Inserted => {
            let started = match state.home.prepare_task(task_id) {
                Ok(_) => launch_accepted(supervisor, state, task_id).await,
                Err(error) => finish_spawn_failed(state, task_id, &error).await,
            };
            if let Err(error) = started {
                // the committed row may have no worker, so it must show as attention
                report(ReleaseWatcherLaunchResult::Uncertain);
                return Err(error);
            }
            ReleaseWatcherLaunchResult::Inserted
        }
        // an existing acceptance is observed from durable state and never respawned
        ActionTaskAcceptance::Existing { state: status } => {
            if status == ProcessStatus::Running {
                spawn_task_actor(supervisor, state, task_id).await?;
            }
            ReleaseWatcherLaunchResult::Existing { state: status }
        }
    };
    report(result);

    Ok(ResourceActionOutcome::Accepted {
        receipt,
        acceptance,
    })
}

fn closed_outcome(
    state: &SupervisorState,
    resource_id: ResourceId,
    closed: Result<ReturnClosure, ReturnDecisionError>,
) -> Result<ResourceActionOutcome, AppError> {
    match closed {
        Ok(closure) => {
            wake_resource(state, resource_id);
            Ok(ResourceActionOutcome::Closed {
                loan: closure.loan,
                state_revision: closure.state_revision,
            })
        }
        Err(error) => return_rejection(error),
    }
}

/// Keep storage failures retryable and turn every domain refusal into a typed rejection
fn return_rejection(error: ReturnDecisionError) -> Result<ResourceActionOutcome, AppError> {
    return_decision_rejection(error).map(|reason| ResourceActionOutcome::Rejected { reason })
}

/// Split one return-decision error into a definitive rejection or an unknown outcome
///
/// Storage, encoding, and internal task-record failures leave the result unknown,
/// so they stay errors and are never reported as a refusal
pub(crate) fn return_decision_rejection(
    error: ReturnDecisionError,
) -> Result<ResourceActionRejection, AppError> {
    let reason = match error {
        ReturnDecisionError::Storage(error) => return Err(error.into()),
        ReturnDecisionError::Encoding(error) => return Err(error.into()),
        ReturnDecisionError::Identity(crate::store::IdentityError::Storage(error))
        | ReturnDecisionError::TaskRecords(error @ AppError::Internal { .. }) => return Err(error),
        ReturnDecisionError::Resource(ResourceStoreError::Storage(error)) => {
            return Err(error.into());
        }
        ReturnDecisionError::RevisionExhausted { revision } => {
            return Err(AppError::Internal {
                message: format!("resource revision {revision:?} cannot be incremented"),
            });
        }
        ReturnDecisionError::NotCurrentSupervisor | ReturnDecisionError::OriginMismatch => {
            ResourceActionRejection::NotCurrentSupervisor
        }
        ReturnDecisionError::ActionNotPending { .. }
        | ReturnDecisionError::InvalidReturnNotice { .. }
        | ReturnDecisionError::Resource(_) => ResourceActionRejection::ActionNotPending,
        ReturnDecisionError::StaleRevision { expected, actual } => {
            ResourceActionRejection::StaleRevision { expected, actual }
        }
        ReturnDecisionError::SpecMismatch => ResourceActionRejection::SpecMismatch,
        ReturnDecisionError::ConflictingRetry { .. } => ResourceActionRejection::ConflictingRetry,
        ReturnDecisionError::IdentityConflict { .. } | ReturnDecisionError::Identity(_) => {
            ResourceActionRejection::IdentityConflict
        }
        error @ (ReturnDecisionError::RestoreNotEnded { .. }
        | ReturnDecisionError::RestoreReleaseUnproven { .. }
        | ReturnDecisionError::RestoreOwnershipUnproven { .. }
        | ReturnDecisionError::ExecutionModeUnproven { .. }) => {
            ResourceActionRejection::RestoreNotResolvable {
                reason: error.to_string(),
            }
        }
        error @ (ReturnDecisionError::Rejected(_)
        | ReturnDecisionError::BackgroundTaskMismatch
        | ReturnDecisionError::TaskRecords(_)) => ResourceActionRejection::DecisionRejected {
            reason: error.to_string(),
        },
    };
    Ok(reason)
}

fn wake_resource(state: &SupervisorState, id: ResourceId) {
    if let Some(resource) = state.resources.get(&id)
        && let Err(error) = resource.cast(ResourceMsg::Wake)
    {
        tracing::debug!(resource = %id.as_uuid(), "resource wake cast: {error}");
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
    release_watcher: bool,
    identity: Option<&ExecutorIdentity>,
) -> StartupRecoveryAction {
    if let Some(resource_task) = resource_task {
        return match (row.status(), resource_task.state) {
            (ProcessStatus::Queued, ProcessStatus::Queued) => StartupRecoveryAction::DeferResource,
            _ => StartupRecoveryAction::Observe,
        };
    }
    // a bound watcher, even one accepted for a remote supervisor, is never
    // relaunched from a queued row whose first spawn may already have happened
    if release_watcher {
        return StartupRecoveryAction::Observe;
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
    use crate::resource::command_shape::test_support::FakeTrainer;
    use crate::resource::store::{
        OpenReleaseLoanResult, ResourceTaskAcceptance, ResourceTaskAcceptanceInput,
    };
    use crate::resource::{
        AssignmentRevision, CommandSpec, Loan, LoanPhase, LoanState, ResourceQueueAttentionReason,
        ResourceQueueReconcileOutcome, ResourceRequestState, ResourceRevision, ReturnContext,
        ReturnWork, SupervisorAddress,
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

    // a native foreground command prints the marker, then blocks on the FIFO
    fn resource_command_waiting_on_fifo(fifo: &std::path::Path) -> NormalizedSpec {
        let marker = fifo.with_extension("marker");
        std::fs::write(&marker, "launch-marker\n").unwrap();
        serde_json::from_value(json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "resource one-shot launch test",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["/bin/cat", marker, fifo] }
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
                    reason: ResourceQueueAttentionReason::AcceptedTaskLaunchUncertain {
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
        let return_notices = notices
            .iter()
            .filter(|notice| {
                matches!(
                    notice.payload,
                    crate::resource::SupervisorNoticePayload::ReturnRequired { .. }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(return_notices.len(), 1);
        assert_eq!(return_notices[0].loan_id, loan.id);

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
            startup_recovery_action(&row, None, false, None),
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
            startup_recovery_action(&row, None, false, Some(&identity)),
            StartupRecoveryAction::LaunchAccepted
        );
        // a remote supervisor's bound watcher may already have spawned, so it is only observed
        assert_eq!(
            startup_recovery_action(&row, None, true, Some(&identity)),
            StartupRecoveryAction::Observe
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

    /// Drain one accepted request so its loan awaits the supervisor return decision
    fn seed_awaiting_return(
        home: &Home,
        authority: MachineId,
    ) -> (Resource, SupervisorActionAuthority) {
        let (resource, _, request_id, task_id, input) =
            seed_serving_assigned_resource_task_with_spec(
                home,
                authority,
                MachineId::new(),
                resource_command(),
            );
        let (loan_id, expected_state_revision) = (input.loan_id, input.expected_state_revision);
        let mut store = Store::open(&home.db_path()).unwrap();
        assert!(matches!(
            store.accept_assigned_resource_task(input).unwrap(),
            ResourceTaskAcceptance::Inserted { .. }
        ));
        store
            .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();
        store
            .cas_exit_with_evidence(
                task_id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
                ProcessGroupExitEvidence::ConfirmedExited,
            )
            .unwrap()
            .unwrap();
        let crate::resource::store::AssignedResourceTaskReconcileOutcome::Completed(result) = store
            .reconcile_assigned_resource_task_for_authority(
                crate::resource::store::AssignedResourceTaskReconcileInput {
                    authority_machine: authority,
                    resource_id: resource.id,
                    loan_id,
                    request_id,
                    task_id,
                    expected_state_revision,
                },
            )
            .unwrap()
        else {
            panic!("the drained request must complete");
        };
        let crate::resource::store::ResourceTaskCompletionResult::ReturnRequired {
            loan,
            notice,
            ..
        } = *result
        else {
            panic!("the drained queue must reserve the return");
        };
        let authority = SupervisorActionAuthority {
            authority_machine: authority,
            resource_id: resource.id,
            loan_id: loan.id,
            action_id: notice.action_id,
            expected_state_revision: notice.state_revision,
            supervisor: resource.supervisor,
            assignment_revision: resource.assignment_revision,
        };
        (resource, authority)
    }

    /// Evaluation command that stays in its foreground process group until the gate exists
    fn gated_return_launch(
        root: &std::path::Path,
        resource: &Resource,
        marker: &std::path::Path,
        gate: &std::path::Path,
    ) -> crate::resource::ReturnLaunch {
        let command = crate::resource::foreground::test_support::native_fake_command();
        let spec: NormalizedSpec = serde_json::from_value(json!({
            "api_version": 1,
            "thread": resource.supervisor.thread,
            "name": "gated evaluation",
            "cwd": root,
            "timeout": "4h",
            "workload": { "type": "task", "command": [command, marker, gate] }
        }))
        .unwrap();
        crate::resource::ReturnLaunch {
            request_id: RequestId::new(),
            task_id: TaskId::new(),
            work: ReturnWork::EvaluationOrNextEpoch {
                completed_task: resource.registered_background_task.unwrap(),
                spec: CommandSpec::try_from(spec).unwrap(),
            },
        }
    }

    async fn decide_return_launch(
        supervisor: &ActorRef<SupervisorMsg>,
        authority: SupervisorActionAuthority,
        launch: crate::resource::ReturnLaunch,
    ) -> ReturnTaskAcceptance {
        let outcome = call(supervisor, |reply| SupervisorMsg::DecideReturn {
            authority,
            decision: Box::new(ReturnDecision::Launch(Box::new(launch))),
            reply,
        })
        .await
        .unwrap()
        .unwrap();
        let ReturnDecisionOutcome::Launch(acceptance) = outcome else {
            panic!("a launch decision must return its task acceptance");
        };
        acceptance
    }

    /// Native foreground return that keeps its Restoring loan, with no registration
    fn foreground_restoring(inspection: &ResourceActorInspection, task_id: TaskId) -> bool {
        matches!(
            inspection.loan.as_ref().map(|loan| &loan.state),
            Some(LoanState::Active {
                phase: LoanPhase::Restoring { resume_task_id, .. }
            }) if *resume_task_id == task_id
        ) && inspection.resource.registered_background_task != Some(task_id)
            && matches!(
                inspection.reconcile_outcome,
                Some(ResourceQueueReconcileOutcome::LoanAlreadyActive { .. })
            )
    }

    #[tokio::test]
    async fn native_return_keeps_its_loan_while_running_and_its_end_serves_the_next_request() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let home = Home::resolve(Some(root.join("home"))).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, return_authority) = seed_awaiting_return(&home, authority);
        // a request accepted after the return reservation waits for the next loan;
        // its own gate keeps it serving until the test inspects that loan
        let (later_marker, later_gate) = (root.join("later-marker"), root.join("later-gate"));
        let mut later_spec = resource_command();
        later_spec.workload = NormalizedWorkload::Task(crate::spec::NormalizedTaskWorkload {
            command: crate::invocation::CommandLine::try_from_argv(vec![
                crate::resource::foreground::test_support::native_fake_command()
                    .display()
                    .to_string(),
                later_marker.display().to_string(),
                later_gate.display().to_string(),
            ])
            .unwrap(),
        });
        let later = Store::open(&home.db_path())
            .unwrap()
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                MachineId::new(),
                later_spec,
            )
            .unwrap();
        let (marker, gate) = (root.join("marker"), root.join("gate"));
        let launch = gated_return_launch(&root, &resource, &marker, &gate);
        let task_id = launch.task_id;

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        assert!(matches!(
            decide_return_launch(&supervisor, return_authority, launch.clone()).await,
            ReturnTaskAcceptance::Inserted { task, .. } if task == task_id
        ));
        let running = wait_for_running_task(&store, &home, task_id).await;
        assert_eq!(running.thread, resource.supervisor.thread);

        // the running foreground task never opens a release for the queued request
        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource.id,
            reply,
        })
        .await
        .unwrap();
        let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
        assert!(
            foreground_restoring(&inspection, task_id),
            "{:?}",
            (&inspection.loan, &inspection.reconcile_outcome)
        );
        let requests = call(&store, |reply| StoreMsg::ResourceRequests {
            authority_machine: authority,
            resource_id: resource.id,
            reply,
        })
        .await
        .unwrap();
        assert!(
            requests
                .iter()
                .any(|request| request.request_id == later.request_id
                    && request.state == ResourceRequestState::Queued)
        );

        // an exact retry observes the running task and never respawns it
        assert_eq!(
            decide_return_launch(&supervisor, return_authority, launch).await,
            ReturnTaskAcceptance::Existing {
                task: task_id,
                state: ProcessStatus::Running,
            }
        );
        std::fs::write(&gate, b"").unwrap();
        let ended = wait_for_terminal_task(&store, task_id).await;
        assert_eq!(ended.status(), ProcessStatus::Succeeded);
        assert_eq!(std::fs::read(&marker).unwrap(), b"x");

        // the confirmed end closes the loan and FIFO reconciliation serves the request
        let inspection = wait_for_inspection(&supervisor, resource.id, |inspection| {
            idle_serving(inspection).is_some()
        })
        .await;
        assert_eq!(
            idle_serving(&inspection),
            Some(&crate::resource::IdleBoundaryProof::ForegroundReturnEnded {
                loan_id: return_authority.loan_id,
                task_id,
            })
        );
        assert!(matches!(
            inspection.loan.as_ref().map(|loan| &loan.state),
            Some(LoanState::Active {
                phase: LoanPhase::Serving { current_request_id, .. }
            }) if *current_request_id == later.request_id
        ));
        assert_eq!(inspection.resource.registered_background_task, None);
        std::fs::write(&later_gate, b"").unwrap();
        wait_for_terminal_task(&store, later.task_id).await;
        assert_eq!(std::fs::read(&later_marker).unwrap(), b"x");

        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn startup_keeps_an_unspawned_return_task_reserved_until_explicit_resolution() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let home = Home::resolve(Some(root.join("home"))).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, return_authority) = seed_awaiting_return(&home, authority);
        let (marker, gate) = (root.join("marker"), root.join("gate"));
        let launch = gated_return_launch(&root, &resource, &marker, &gate);
        let task_id = launch.task_id;
        // the binding commits, then the daemon stops before it spawns a worker
        let ReturnTaskAcceptance::Inserted { state_revision, .. } = Store::open(&home.db_path())
            .unwrap()
            .accept_return_task_for_authority(ReturnTaskAcceptanceInput {
                authority: return_authority,
                launch: launch.clone(),
                executor_env: TaskEnv::capture(),
                origin: ReturnTaskOrigin::Local {
                    callback_codex: CallbackExecutable::available("/bin/echo".into()),
                },
            })
            .unwrap()
        else {
            panic!("the first exact launch must insert its task");
        };

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let attention = |inspection: &ResourceActorInspection| {
            matches!(
                &inspection.reconcile_outcome,
                Some(ResourceQueueReconcileOutcome::RestoreAttentionRequired {
                    task_id: attention_task,
                    reason: crate::resource::RestoreAttentionReason::LaunchUncertain,
                    ..
                }) if *attention_task == task_id
            )
        };
        assert!(attention(
            &inspect_resource(&supervisor, resource.id).await.unwrap()
        ));
        assert_eq!(
            decide_return_launch(&supervisor, return_authority, launch).await,
            ReturnTaskAcceptance::Existing {
                task: task_id,
                state: ProcessStatus::Queued,
            }
        );
        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource.id,
            reply,
        })
        .await
        .unwrap();
        assert!(attention(
            &inspect_resource(&supervisor, resource.id).await.unwrap()
        ));
        let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status(), ProcessStatus::Queued);
        assert!(!marker.exists());

        // cancelling the queued row proves no child spawned, so the supervisor can resolve it
        assert!(matches!(
            call(&supervisor, |reply| SupervisorMsg::Cancel {
                id: task_id,
                reply
            })
            .await
            .unwrap(),
            CancelResult::CancelledQueued(_)
        ));
        let mut current = return_authority;
        current.expected_state_revision = state_revision;
        let closure = call(&supervisor, |reply| SupervisorMsg::ResolveEndedRestore {
            resolution: Box::new(EndedRestoreResolution {
                authority: current,
                task_id,
                reason: "return launch never started".into(),
            }),
            reply,
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            closure.loan.state,
            LoanState::Closed {
                result: crate::resource::LoanClosure::RestoreEnded {
                    outcome: ExitReason::Cancelled,
                    ..
                }
            }
        ));
        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource.id,
            reply,
        })
        .await
        .unwrap();
        let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
        assert_eq!(inspection.loan, None);
        assert_eq!(inspection.resource.registered_background_task, None);
        assert!(!marker.exists());

        stop_supervisor(supervisor, handle).await;
    }

    /// Reassign the seeded supervisor thread to another machine
    fn move_supervisor_to_another_machine(
        home: &Home,
        resource: &Resource,
        action: &mut SupervisorActionAuthority,
    ) -> MachineId {
        let mut remote = MachineId::new();
        while remote == action.authority_machine {
            remote = MachineId::new();
        }
        rusqlite::Connection::open(home.db_path())
            .unwrap()
            .execute(
                "UPDATE resources SET supervisor_machine = ?1 WHERE id = ?2",
                rusqlite::params![remote.to_string(), resource.id.as_uuid().to_string()],
            )
            .unwrap();
        action.supervisor.machine = remote;
        remote
    }

    async fn remote_action(
        supervisor: &ActorRef<SupervisorMsg>,
        action: SupervisorActionAuthority,
        operation: ResourceActionOperation,
    ) -> ResourceActionOutcome {
        call(supervisor, |reply| SupervisorMsg::ResourceAction {
            request: Box::new(ResourceActionRequest::new(1, action, operation)),
            reply,
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn remote_native_return_spawns_once_and_closes_after_its_confirmed_end() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let home = Home::resolve(Some(root.join("home"))).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, mut action) = seed_awaiting_return(&home, authority);
        let remote = move_supervisor_to_another_machine(&home, &resource, &mut action);
        let (marker, gate) = (root.join("marker"), root.join("gate"));
        let launch = gated_return_launch(&root, &resource, &marker, &gate);
        let task_id = launch.task_id;

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let ResourceActionOutcome::Prepared { task } = remote_action(
            &supervisor,
            action,
            ResourceActionOperation::PrepareReturn {
                launch: launch.clone(),
            },
        )
        .await
        else {
            panic!("the authority must prepare the return task");
        };
        assert_eq!(task.task_id, task_id);
        assert!(task.digest_matches());
        assert!(
            call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                .is_none()
        );

        let operation = ResourceActionOperation::LaunchReturn {
            launch,
            normalized_spec_sha256: task.normalized_spec_sha256,
        };
        let ResourceActionOutcome::Accepted {
            receipt,
            acceptance: ActionTaskAcceptance::Inserted,
        } = remote_action(&supervisor, action, operation.clone()).await
        else {
            panic!("the first remote launch must insert its task");
        };
        assert_eq!(receipt.origin_machine(), remote);
        wait_for_running_task(&store, &home, task_id).await;
        // the authority keeps no callback route for the remote supervisor's task
        assert!(
            call(&store, |reply| StoreMsg::OriginRoute { id: task_id, reply })
                .await
                .unwrap()
                .is_none()
        );

        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource.id,
            reply,
        })
        .await
        .unwrap();
        let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
        assert!(
            foreground_restoring(&inspection, task_id),
            "{:?}",
            (&inspection.loan, &inspection.reconcile_outcome)
        );
        assert!(matches!(
            remote_action(&supervisor, action, operation).await,
            ResourceActionOutcome::Accepted {
                acceptance: ActionTaskAcceptance::Existing {
                    state: ProcessStatus::Running
                },
                ..
            }
        ));

        std::fs::write(&gate, b"").unwrap();
        wait_for_terminal_task(&store, task_id).await;
        // the retry observed the running worker, so the command ran exactly once
        assert_eq!(std::fs::read(&marker).unwrap(), b"x");
        // the Restoring loan was the only non-closed loan, so its closure leaves none
        let inspection = wait_for_inspection(&supervisor, resource.id, |inspection| {
            inspection.loan.is_none()
        })
        .await;
        assert_eq!(inspection.resource.registered_background_task, None);
        // the remote supervisor keeps its route; the authority adds none
        assert!(
            call(&store, |reply| StoreMsg::OriginRoute { id: task_id, reply })
                .await
                .unwrap()
                .is_none()
        );
        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn remote_return_accepted_before_spawn_is_never_relaunched_after_restart() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let home = Home::resolve(Some(root.join("home"))).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, mut action) = seed_awaiting_return(&home, authority);
        move_supervisor_to_another_machine(&home, &resource, &mut action);
        let (marker, gate) = (root.join("marker"), root.join("gate"));
        let launch = gated_return_launch(&root, &resource, &marker, &gate);
        let task_id = launch.task_id;
        // the remote acceptance commits, then the daemon stops before it spawns a worker
        let mut store = Store::open(&home.db_path()).unwrap();
        let digest = store
            .prepare_return_task_for_authority(action, launch.clone(), TaskEnv::capture())
            .unwrap()
            .normalized_spec_sha256;
        assert!(matches!(
            store
                .accept_return_task_for_authority(ReturnTaskAcceptanceInput {
                    authority: action,
                    launch: launch.clone(),
                    executor_env: TaskEnv::capture(),
                    origin: ReturnTaskOrigin::Remote {
                        normalized_spec_sha256: digest,
                    },
                })
                .unwrap(),
            ReturnTaskAcceptance::Inserted { .. }
        ));
        drop(store);

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let uncertain = |inspection: &ResourceActorInspection| {
            matches!(
                &inspection.reconcile_outcome,
                Some(ResourceQueueReconcileOutcome::RestoreAttentionRequired {
                    task_id: attention_task,
                    reason: crate::resource::RestoreAttentionReason::LaunchUncertain,
                    ..
                }) if *attention_task == task_id
            )
        };
        assert!(uncertain(
            &inspect_resource(&supervisor, resource.id).await.unwrap()
        ));
        // the lost-reply retry observes the queued row and starts nothing
        assert!(matches!(
            remote_action(
                &supervisor,
                action,
                ResourceActionOperation::LaunchReturn {
                    launch,
                    normalized_spec_sha256: digest,
                },
            )
            .await,
            ResourceActionOutcome::Accepted {
                acceptance: ActionTaskAcceptance::Existing {
                    state: ProcessStatus::Queued
                },
                ..
            }
        ));
        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource.id,
            reply,
        })
        .await
        .unwrap();
        assert!(uncertain(
            &inspect_resource(&supervisor, resource.id).await.unwrap()
        ));
        let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status(), ProcessStatus::Queued);
        assert!(row.pid().is_none());
        assert!(!marker.exists());
        stop_supervisor(supervisor, handle).await;
    }

    async fn launch_background_task(
        supervisor: &ActorRef<SupervisorMsg>,
        launch: BackgroundLaunch,
    ) -> Result<BackgroundLaunchAcceptance, BackgroundLaunchError> {
        call(supervisor, |reply| SupervisorMsg::LaunchBackground {
            launch: Box::new(launch),
            reply,
        })
        .await
        .unwrap()
    }

    fn trainer_launch(
        trainer: &FakeTrainer,
        resource: &Resource,
        request_id: RequestId,
    ) -> BackgroundLaunch {
        BackgroundLaunch {
            resource_id: resource.id,
            request_id,
            spec: trainer.spec(resource.supervisor.thread, "direct segment trainer"),
            env: trainer.env.clone(),
            callback_cwd: trainer.cwd.clone(),
        }
    }

    async fn wait_for_inspection(
        supervisor: &ActorRef<SupervisorMsg>,
        id: ResourceId,
        predicate: impl Fn(&ResourceActorInspection) -> bool,
    ) -> ResourceActorInspection {
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let inspection = inspect_resource(supervisor, id).await.unwrap();
                if predicate(&inspection) {
                    return inspection;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await;
        match result {
            Ok(inspection) => inspection,
            Err(_) => panic!(
                "resource never reached the expected state: {:?}",
                inspect_resource(supervisor, id)
                    .await
                    .map(|inspection| (inspection.loan, inspection.reconcile_outcome))
            ),
        }
    }

    async fn wait_for_terminal_task(store: &ActorRef<StoreMsg>, task_id: TaskId) -> TaskRow {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let row = call(store, |reply| StoreMsg::GetTask { id: task_id, reply })
                    .await
                    .unwrap();
                if let Some(row) = row.filter(|row| row.state.is_terminal()) {
                    return row;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap()
    }

    fn idle_serving(
        inspection: &ResourceActorInspection,
    ) -> Option<&crate::resource::IdleBoundaryProof> {
        match inspection.loan.as_ref().map(|loan| &loan.state) {
            Some(LoanState::Active {
                phase:
                    LoanPhase::Serving {
                        return_context: ReturnContext::Idle,
                        release_provenance:
                            crate::resource::ServingReleaseProvenance::IdleBoundary { proof },
                        ..
                    },
            }) => Some(proof),
            _ => None,
        }
    }

    #[tokio::test]
    async fn first_background_launch_spawns_once_and_registers_after_its_start_event() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let home = Home::resolve(Some(root.join("home"))).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let trainer = FakeTrainer::new(&root);
        let resource = resource(authority, None);
        Store::open(&home.db_path())
            .unwrap()
            .register_resource(authority, &resource)
            .unwrap();

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let request_id = RequestId::new();
        let BackgroundLaunchAcceptance::Inserted { task, .. } =
            launch_background_task(&supervisor, trainer_launch(&trainer, &resource, request_id))
                .await
                .unwrap()
        else {
            panic!("the first exact launch must insert and spawn its task");
        };
        wait_for_running_task(&store, &home, task).await;

        // the delivered start event wakes the owner; no caller reconcile is needed.
        // The event sender is not running here, so send the same inbox hint it sends
        supervisor
            .cast(SupervisorMsg::DispatchInbox { id: task })
            .unwrap();
        wait_for_inspection(&supervisor, resource.id, |inspection| {
            inspection.resource.registered_background_task == Some(task)
        })
        .await;

        // an exact retry observes the running task and never spawns a second one
        assert_eq!(
            launch_background_task(&supervisor, trainer_launch(&trainer, &resource, request_id))
                .await
                .unwrap(),
            BackgroundLaunchAcceptance::Existing {
                task,
                state: ProcessStatus::Running,
            }
        );
        // another request cannot start a competing background task
        assert!(matches!(
            launch_background_task(
                &supervisor,
                trainer_launch(&trainer, &resource, RequestId::new())
            )
            .await,
            Err(BackgroundLaunchError::BackgroundTaskActive { task_id, .. }) if task_id == task
        ));

        std::fs::write(&trainer.gate, b"").unwrap();
        wait_for_terminal_task(&store, task).await;
        assert_eq!(trainer.starts(), 1);

        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn committed_unspawned_background_launch_is_never_respawned_after_restart() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let home = Home::resolve(Some(root.join("home"))).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let trainer = FakeTrainer::new(&root);
        let resource = resource(authority, None);
        let request_id = RequestId::new();
        // the binding commits, then the daemon stops before it spawns a worker
        let (task, later) = {
            let mut store = Store::open(&home.db_path()).unwrap();
            store.register_resource(authority, &resource).unwrap();
            let launch = trainer_launch(&trainer, &resource, request_id);
            let BackgroundLaunchAcceptance::Inserted { task, .. } = store
                .accept_background_launch_for_authority(BackgroundLaunchInput {
                    authority_machine: authority,
                    resource_id: resource.id,
                    request_id,
                    task_id: TaskId::new(),
                    spec: launch.spec,
                    env: launch.env,
                    callback_codex: CallbackExecutable::available("/bin/echo".into()),
                })
                .unwrap()
            else {
                panic!("the first exact launch must insert its task");
            };
            let later = store
                .accept_resource_request(
                    authority,
                    RequestId::new(),
                    TaskId::new(),
                    resource.id,
                    MachineId::new(),
                    resource_command(),
                )
                .unwrap();
            (task, later)
        };

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let uncertain = |inspection: &ResourceActorInspection| {
            matches!(
                inspection.reconcile_outcome,
                Some(ResourceQueueReconcileOutcome::BackgroundLaunchUncertain { task_id })
                    if task_id == task
            )
        };
        assert!(uncertain(
            &inspect_resource(&supervisor, resource.id).await.unwrap()
        ));
        assert_eq!(
            launch_background_task(&supervisor, trainer_launch(&trainer, &resource, request_id))
                .await
                .unwrap(),
            BackgroundLaunchAcceptance::Existing {
                task,
                state: ProcessStatus::Queued,
            }
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
        assert!(uncertain(&inspection));
        assert_eq!(inspection.resource.registered_background_task, None);
        assert_eq!(inspection.loan, None);
        let row = call(&store, |reply| StoreMsg::GetTask { id: task, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status(), ProcessStatus::Queued);
        assert_eq!(trainer.starts(), 0);

        // cancelling the queued row proves no child started, which is the idle proof
        assert!(matches!(
            call(&supervisor, |reply| SupervisorMsg::Cancel {
                id: task,
                reply
            })
            .await
            .unwrap(),
            CancelResult::CancelledQueued(_)
        ));
        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource.id,
            reply,
        })
        .await
        .unwrap();
        let inspection = wait_for_inspection(&supervisor, resource.id, |inspection| {
            idle_serving(inspection).is_some()
        })
        .await;
        assert_eq!(
            idle_serving(&inspection),
            Some(
                &crate::resource::IdleBoundaryProof::BackgroundLaunchNeverSpawned {
                    request_id,
                    task_id: task,
                }
            )
        );
        wait_for_terminal_task(&store, later.task_id).await;
        assert_eq!(trainer.starts(), 0);

        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn no_resume_closure_runs_a_later_request_from_the_idle_boundary() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let home = Home::resolve(Some(root.join("home"))).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, return_authority) = seed_awaiting_return(&home, authority);
        let later = Store::open(&home.db_path())
            .unwrap()
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                MachineId::new(),
                resource_command(),
            )
            .unwrap();

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        // the reservation stays in place until the supervisor decides
        assert!(matches!(
            inspect_resource(&supervisor, resource.id)
                .await
                .unwrap()
                .loan
                .map(|loan| loan.state),
            Some(LoanState::Active {
                phase: LoanPhase::AwaitingReturn { .. }
            })
        ));
        let outcome = call(&supervisor, |reply| SupervisorMsg::DecideReturn {
            authority: return_authority,
            decision: Box::new(ReturnDecision::NoResume {
                reason: "training is complete".into(),
            }),
            reply,
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(outcome, ReturnDecisionOutcome::Closed(_)));

        let inspection = wait_for_inspection(&supervisor, resource.id, |inspection| {
            idle_serving(inspection).is_some()
        })
        .await;
        assert_eq!(
            idle_serving(&inspection),
            Some(&crate::resource::IdleBoundaryProof::SupervisorNoResume {
                loan_id: return_authority.loan_id,
            })
        );
        wait_for_terminal_task(&store, later.task_id).await;

        stop_supervisor(supervisor, handle).await;
    }

    /// Socket-route `submit` on a supervisor that is also the resource authority
    mod co_located_action {
        use super::*;
        use crate::daemon::AppState;
        use crate::daemon::resource_action::submit;
        use crate::files::StreamSlots;
        use crate::fleet::FleetState;
        use crate::fleet::directory::LocalMachine;
        use crate::fleet::protocol::SUPPORTED_PROTOCOLS;
        use crate::machine::{LocalIdentity, MachineName};
        use crate::resource::bound_action::{
            LocalReturnAcceptance, LocalReturnReceipt, ResourceActionChoice,
            ResourceActionSubmitOutcome,
        };

        fn app_state(
            home: &Home,
            supervisor: &ActorRef<SupervisorMsg>,
            store: ActorRef<StoreMsg>,
        ) -> AppState {
            AppState {
                home: home.clone(),
                store,
                supervisor: supervisor.clone(),
                web: None,
                content: None,
                stream_slots: StreamSlots::new(),
                machine: LocalMachine {
                    identity: LocalIdentity::start(home).unwrap(),
                    name: MachineName::fallback(),
                    protocol: SUPPORTED_PROTOCOLS,
                },
                fleet: FleetState::Disabled,
                message_receiver: crate::daemon::message_receiver::MessageReceiver::default(),
            }
        }

        fn launch_choice(launch: crate::resource::ReturnLaunch) -> ResourceActionChoice {
            ResourceActionChoice::Return {
                decision: ReturnDecision::Launch(Box::new(launch)),
            }
        }

        fn no_resume(reason: &str) -> ResourceActionChoice {
            ResourceActionChoice::Return {
                decision: ReturnDecision::NoResume {
                    reason: reason.into(),
                },
            }
        }

        fn conflicting_retry(outcome: &ResourceActionSubmitOutcome) -> bool {
            matches!(
                outcome,
                ResourceActionSubmitOutcome::Rejected {
                    reason: ResourceActionRejection::ConflictingRetry
                }
            )
        }

        #[tokio::test]
        async fn return_launch_spawns_once_replays_and_refuses_other_content() {
            let _guard = SUPERVISOR_TEST_LOCK.lock().await;
            configure_task_runner();
            let directory = tempdir().unwrap();
            let root = directory.path().canonicalize().unwrap();
            let home = Home::resolve(Some(root.join("home"))).unwrap();
            home.ensure().unwrap();
            let authority = load_or_create_machine_id(&home).unwrap();
            let (resource, return_authority) = seed_awaiting_return(&home, authority);
            let (marker, gate) = (root.join("marker"), root.join("gate"));
            let launch = gated_return_launch(&root, &resource, &marker, &gate);
            let (request_id, task_id) = (launch.request_id, launch.task_id);

            let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
                .await
                .unwrap();
            let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
                .await
                .unwrap();
            let state = app_state(&home, &supervisor, store.clone());
            let receipt = LocalReturnReceipt {
                authority: return_authority,
                request_id,
                task_id,
            };

            let first = submit(&state, return_authority, launch_choice(launch.clone()))
                .await
                .unwrap();
            let ResourceActionSubmitOutcome::LocalReturnAccepted {
                receipt: first_receipt,
                acceptance: LocalReturnAcceptance::Inserted { loan, .. },
            } = first
            else {
                panic!("the first co-located launch must insert its task: {first:?}");
            };
            assert_eq!(first_receipt, receipt);
            assert_eq!(loan.id, return_authority.loan_id);
            wait_for_running_task(&store, &home, task_id).await;

            // an exact retry observes the bound task and never spawns it again
            let retry = submit(&state, return_authority, launch_choice(launch.clone()))
                .await
                .unwrap();
            assert!(matches!(
                retry,
                ResourceActionSubmitOutcome::LocalReturnAccepted {
                    receipt: retry_receipt,
                    acceptance: LocalReturnAcceptance::Existing {
                        state: ProcessStatus::Running
                    },
                } if retry_receipt == receipt
            ));

            let mut other = launch;
            other.request_id = RequestId::new();
            other.task_id = TaskId::new();
            let other_task = other.task_id;
            let conflict = submit(&state, return_authority, launch_choice(other))
                .await
                .unwrap();
            assert!(conflicting_retry(&conflict), "{conflict:?}");
            let absent = call(&store, |reply| StoreMsg::GetTask {
                id: other_task,
                reply,
            })
            .await
            .unwrap();
            assert!(absent.is_none());

            // the resource actor owns a co-located watcher, so the socket path refuses one
            let watcher = submit(
                &state,
                return_authority,
                ResourceActionChoice::ReleaseWatcher {
                    observed_background_task: task_id,
                },
            )
            .await;
            assert!(matches!(
                watcher,
                Err(AppError::ResourceActionNotAllowed { .. })
            ));

            std::fs::write(&gate, b"").unwrap();
            wait_for_terminal_task(&store, task_id).await;
            assert_eq!(std::fs::read(&marker).unwrap(), b"x");

            stop_supervisor(supervisor, handle).await;
        }

        #[tokio::test]
        async fn no_resume_closes_once_and_replays_the_saved_closure() {
            let _guard = SUPERVISOR_TEST_LOCK.lock().await;
            let directory = tempdir().unwrap();
            let root = directory.path().canonicalize().unwrap();
            let home = Home::resolve(Some(root.join("home"))).unwrap();
            home.ensure().unwrap();
            let authority = load_or_create_machine_id(&home).unwrap();
            let (_, return_authority) = seed_awaiting_return(&home, authority);

            let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
                .await
                .unwrap();
            let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
                .await
                .unwrap();
            let state = app_state(&home, &supervisor, store);

            let first = submit(&state, return_authority, no_resume("training is complete"))
                .await
                .unwrap();
            let ResourceActionSubmitOutcome::Closed {
                loan,
                state_revision,
            } = first
            else {
                panic!("no-resume must close the loan: {first:?}");
            };
            assert_eq!(loan.id, return_authority.loan_id);
            assert!(matches!(loan.state, LoanState::Closed { .. }));

            let retry = submit(&state, return_authority, no_resume("training is complete"))
                .await
                .unwrap();
            assert!(matches!(
                retry,
                ResourceActionSubmitOutcome::Closed {
                    loan: ref replayed,
                    state_revision: replayed_revision,
                } if *replayed == loan && replayed_revision == state_revision
            ));

            let conflict = submit(&state, return_authority, no_resume("another reason"))
                .await
                .unwrap();
            assert!(conflicting_retry(&conflict), "{conflict:?}");

            stop_supervisor(supervisor, handle).await;
        }

        #[tokio::test]
        async fn queued_return_after_restart_is_observed_then_resolved_without_spawn() {
            let _guard = SUPERVISOR_TEST_LOCK.lock().await;
            configure_task_runner();
            let directory = tempdir().unwrap();
            let root = directory.path().canonicalize().unwrap();
            let home = Home::resolve(Some(root.join("home"))).unwrap();
            home.ensure().unwrap();
            let authority = load_or_create_machine_id(&home).unwrap();
            let (resource, return_authority) = seed_awaiting_return(&home, authority);
            let (marker, gate) = (root.join("marker"), root.join("gate"));
            let launch = gated_return_launch(&root, &resource, &marker, &gate);
            let task_id = launch.task_id;
            // the binding commits, then the daemon stops before it spawns a worker
            let ReturnTaskAcceptance::Inserted { state_revision, .. } =
                Store::open(&home.db_path())
                    .unwrap()
                    .accept_return_task_for_authority(ReturnTaskAcceptanceInput {
                        authority: return_authority,
                        launch: launch.clone(),
                        executor_env: TaskEnv::capture(),
                        origin: ReturnTaskOrigin::Local {
                            callback_codex: CallbackExecutable::available("/bin/echo".into()),
                        },
                    })
                    .unwrap()
            else {
                panic!("the first exact launch must insert its task");
            };

            let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
                .await
                .unwrap();
            let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
                .await
                .unwrap();
            let state = app_state(&home, &supervisor, store.clone());

            // the queued return found after restart is only observed
            let observed = submit(&state, return_authority, launch_choice(launch))
                .await
                .unwrap();
            assert!(matches!(
                observed,
                ResourceActionSubmitOutcome::LocalReturnAccepted {
                    receipt: LocalReturnReceipt { task_id: observed_task, .. },
                    acceptance: LocalReturnAcceptance::Existing {
                        state: ProcessStatus::Queued
                    },
                } if observed_task == task_id
            ));
            let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.status(), ProcessStatus::Queued);

            let mut current = return_authority;
            current.expected_state_revision = state_revision;
            let resolve = |reason: &str| ResourceActionChoice::ResolveEndedRestore {
                task_id,
                reason: reason.into(),
            };
            // a queued task has not ended, so resolution is refused without a write
            let early = submit(&state, current, resolve("launch never started"))
                .await
                .unwrap();
            assert!(matches!(
                early,
                ResourceActionSubmitOutcome::Rejected {
                    reason: ResourceActionRejection::RestoreNotResolvable { .. }
                }
            ));

            // cancelling the queued row proves no child spawned
            assert!(matches!(
                call(&supervisor, |reply| SupervisorMsg::Cancel {
                    id: task_id,
                    reply
                })
                .await
                .unwrap(),
                CancelResult::CancelledQueued(_)
            ));
            let closed = submit(&state, current, resolve("launch never started"))
                .await
                .unwrap();
            let ResourceActionSubmitOutcome::Closed { loan, .. } = &closed else {
                panic!("resolution must close the loan: {closed:?}");
            };
            assert_eq!(loan.id, return_authority.loan_id);
            assert!(matches!(
                loan.state,
                LoanState::Closed {
                    result: crate::resource::LoanClosure::RestoreEnded {
                        outcome: ExitReason::Cancelled,
                        ..
                    }
                }
            ));
            let replay = submit(&state, current, resolve("launch never started"))
                .await
                .unwrap();
            assert!(matches!(
                &replay,
                ResourceActionSubmitOutcome::Closed { loan: replayed, .. } if replayed == loan
            ));
            let conflict = submit(&state, current, resolve("another reason"))
                .await
                .unwrap();
            assert!(conflicting_retry(&conflict), "{conflict:?}");
            assert!(!marker.exists());

            stop_supervisor(supervisor, handle).await;
        }

        #[tokio::test]
        async fn remote_authority_keeps_the_route_path_and_foreign_supervisor_is_refused() {
            let _guard = SUPERVISOR_TEST_LOCK.lock().await;
            let directory = tempdir().unwrap();
            let home = Home::resolve(Some(directory.path().join("home"))).unwrap();
            home.ensure().unwrap();
            let local = load_or_create_machine_id(&home).unwrap();
            let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
                .await
                .unwrap();
            let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
                .await
                .unwrap();
            let state = app_state(&home, &supervisor, store);
            let remote = SupervisorActionAuthority {
                authority_machine: MachineId::new(),
                resource_id: ResourceId::new(),
                loan_id: crate::resource::LoanId::new(),
                action_id: crate::resource::ActionId::new(),
                expected_state_revision: ResourceRevision::new(1),
                supervisor: SupervisorAddress {
                    machine: local,
                    thread: ThreadId(Uuid::now_v7()),
                },
                assignment_revision: AssignmentRevision::new(1),
            };

            // a remote watcher still goes through the authority, which is unreachable here
            let watcher = submit(
                &state,
                remote,
                ResourceActionChoice::ReleaseWatcher {
                    observed_background_task: TaskId::new(),
                },
            )
            .await;
            assert!(
                matches!(watcher, Err(AppError::MachineUnavailable { machine, .. }) if machine == remote.authority_machine)
            );
            let closure = submit(&state, remote, no_resume("done")).await;
            assert!(matches!(closure, Err(AppError::MachineUnavailable { .. })));

            let mut foreign = remote;
            foreign.supervisor.machine = MachineId::new();
            foreign.authority_machine = local;
            assert!(matches!(
                submit(&state, foreign, no_resume("done")).await,
                Err(AppError::Usage { .. })
            ));

            stop_supervisor(supervisor, handle).await;
        }
    }
}
