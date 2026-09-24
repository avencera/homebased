//! Root supervisor: store, callback, and per-task and per-resource actors

use std::collections::HashMap;
use std::path::PathBuf;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};

use crate::daemon::actors::callback::{CallbackActor, CallbackArgs, CallbackMsg};
use crate::daemon::actors::resource::{
    ResourceActor, ResourceActorInspection, ResourceMsg, resource_actor_name,
    resource_id_from_actor_name,
};
use crate::daemon::actors::task::{TaskActor, TaskMsg, cancel_task};
use crate::daemon::actors::{StoreActor, StoreMsg, call, send_reply};
use crate::domain::{
    AgentKind, ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId, TaskRow,
};
use crate::error::AppError;
use crate::home::{Home, LockMode};
use crate::invocation::{persist_workload, resolve_agent_binary_with};
use crate::machine::{MachineId, load_or_create_machine_id};
use crate::resource::background_launch::RemoteBackgroundLaunchReceipt;
use crate::resource::bound_action::{ResourceActionOutcome, ResourceActionRequest};
use crate::resource::store::{
    ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError, ResourceSnapshot, ResourceStoreError,
    ResourceTaskAcceptance, ResourceTaskAcceptanceInput,
};
use crate::resource::{
    ReleaseWatcherIntent, Resource, ResourceId, ReturnDecision, SupervisorActionAuthority,
    SupervisorAddress,
};
use crate::runner;
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::{
    BackgroundLaunchAcceptance, BackgroundLaunchError, CancelResult, EndedRestoreResolution,
    NewTask, ReturnClosure, ReturnDecisionError, ReturnTaskAcceptance, new_queued_task,
};
use crate::submission::{CallbackExecutable, ExecutionRecord, ExecutorIdentity, RequestId};

mod recovery;
mod resource_launch;

pub(crate) use resource_launch::return_decision_rejection;
use resource_launch::{
    decide_return, launch_assigned_resource_task, launch_background, launch_bound_release_watcher,
    launch_remote_background, resource_action,
};

const STORE_NAME: &str = "homebased.store";
const CALLBACK_NAME: &str = "homebased.callback";

#[cfg(test)]
pub(crate) static SUPERVISOR_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Messages sent to the daemon root supervisor
pub(crate) enum SupervisorMsg {
    /// Store ref for `AppState` reads
    GetStore {
        reply: RpcReplyPort<Result<ActorRef<StoreMsg>, AppError>>,
    },
    /// Register or reuse one resource on this machine and ensure its actor exists
    RegisterResource {
        resource: Box<Resource>,
        reply: RpcReplyPort<Result<Resource, AppError>>,
    },
    /// Inspect the restored snapshot and identity of one resource actor
    InspectResource {
        id: ResourceId,
        reply: RpcReplyPort<Result<Option<ResourceActorInspection>, AppError>>,
    },
    /// Wake one resource actor after queue acceptance or an authority-side retry
    ReconcileResource {
        /// Resource whose authority-owned queue must be reconciled
        id: ResourceId,
        /// Reply after the resource actor has reconciled and refreshed its snapshot
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Accept one assigned request and launch only when this call inserts its task
    LaunchAssignedResourceTask {
        /// Exact Serving assignment and fixed task identity selected by the resource owner
        input: Box<ResourceTaskAcceptanceInput>,
        /// Typed task acceptance, nested inside actor and storage errors
        reply: RpcReplyPort<Result<Result<ResourceTaskAcceptance, ResourceStoreError>, AppError>>,
    },
    /// Accept one authority-bound release watcher and spawn only when this call inserts it
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
    ResolveEndedRestore {
        /// Exact authority, bound task, and supervisor reason
        resolution: Box<EndedRestoreResolution>,
        /// Typed closure result, nested inside actor and storage errors
        reply: RpcReplyPort<Result<Result<ReturnClosure, ReturnDecisionError>, AppError>>,
    },
    /// Bind one first background launch and spawn only a newly inserted task
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
    LaunchRemoteBackground {
        /// Exact receipt that the supervisor machine saved, and the saved spec
        launch: Box<RemoteBackgroundLaunch>,
        /// Typed launch result, nested inside actor and storage errors
        reply: RpcReplyPort<
            Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError>,
        >,
    },
    /// Persist a queued row, spawn its worker, and watch it
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
    /// Cancel a task
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
pub(crate) struct RemoteLaunch {
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

/// Supervisor state
pub(crate) struct SupervisorState {
    home: Home,
    callback_codex_pin: Option<String>,
    machine: MachineId,
    store: ActorRef<StoreMsg>,
    callback: ActorRef<CallbackMsg>,
    tasks: HashMap<TaskId, ActorRef<TaskMsg>>,
    resources: HashMap<ResourceId, ActorRef<ResourceMsg>>,
}

/// Root actor
pub(crate) struct SupervisorActor;

/// Startup inputs for the root actor
pub(crate) struct SupervisorArgs {
    home: Home,
    callback_codex_pin: Option<String>,
}

impl SupervisorArgs {
    /// Build startup inputs for the daemon home
    ///
    /// `callback_codex_pin` names the Codex executable that callbacks use instead
    /// of a `PATH` lookup. The daemon captures it once so the actor never reads
    /// process environment while it runs
    pub(crate) fn new(home: Home, callback_codex_pin: Option<String>) -> Self {
        Self {
            home,
            callback_codex_pin,
        }
    }
}

impl Actor for SupervisorActor {
    type Msg = SupervisorMsg;
    type State = SupervisorState;
    type Arguments = SupervisorArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        SupervisorArgs {
            home,
            callback_codex_pin,
        }: Self::Arguments,
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
            callback_codex_pin,
            machine,
            store,
            callback,
            tasks: HashMap::new(),
            resources: HashMap::new(),
        };
        restore_resource_actors(&myself, &mut state).await?;
        recovery::recover_tasks(&myself, &mut state).await?;
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
            supervisor.clone(),
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
                && saved.current() == Some(spec)
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
    record_pid(state, id, pid).await?;
    spawn_task_actor(supervisor, state, id).await
}

/// Resolve the Codex executable that callbacks for a supervisor-launched task use
///
/// A missing executable must not block the task itself, so the failure is kept
/// as the callback's durable reason instead of a path that cannot run
fn resolve_callback_codex(
    state: &SupervisorState,
    path: &str,
    cwd: &std::path::Path,
) -> CallbackExecutable {
    callback_executable(resolve_agent_binary_with(
        AgentKind::Codex,
        state.callback_codex_pin.as_deref(),
        path,
        cwd,
    ))
}

fn callback_executable(resolved: Result<PathBuf, AppError>) -> CallbackExecutable {
    match resolved {
        Ok(path) => CallbackExecutable::available(path),
        Err(error) => CallbackExecutable::Unavailable {
            reason: format!("callback Codex executable is unavailable: {error}"),
        },
    }
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

/// Insert the row, spawn the worker with the runner lock held, then watch it
/// Runs inside the supervisor so a task always has an in-process owner from
/// the moment its worker exists, and so a client that disconnects mid-submit
/// cannot strand a Queued row with no worker. The cost is that launches are
/// serialized under one `CALL_TIMEOUT`; each is a few store calls and a
/// fork, so that only bites when SQLite itself stalls. A failed spawn
/// finishes the row as `SpawnFailed` and returns the spawn error
async fn launch(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    row: TaskRow,
    spec: NormalizedSpec,
) -> Result<(), AppError> {
    let id = row.id;
    let codex = resolve_callback_codex(state, &row.env.path, &row.cwd);
    call(&state.store, |reply| StoreMsg::InsertLocalTask {
        row: Box::new(row),
        spec: Box::new(spec),
        machine: state.machine,
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
    record_pid(state, id, pid).await?;
    spawn_task_actor(supervisor, state, id).await
}

/// Save the spawned runner's pid, refusing one outside the signed range the store keeps
async fn record_pid(state: &SupervisorState, id: TaskId, pid: u32) -> Result<(), AppError> {
    let pid = i32::try_from(pid).map_err(|_| AppError::Internal {
        message: format!("runner pid {pid} for task {id} does not fit a signed process id"),
    })?;
    call(&state.store, |reply| StoreMsg::SetPid { id, pid, reply }).await
}

/// Prepare the files of a task that this call inserted, then spawn its worker
///
/// A preparation failure finishes the committed row as `SpawnFailed`, so the
/// row never waits for a worker that cannot start
async fn spawn_inserted(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    id: TaskId,
) -> Result<(), AppError> {
    match state.home.prepare_task(id) {
        Ok(_) => launch_accepted(supervisor, state, id).await,
        Err(error) => finish_spawn_failed(state, id, &error).await,
    }
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
        evidence: ProcessGroupExitEvidence::NoChildSpawned.into(),
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
mod tests;
