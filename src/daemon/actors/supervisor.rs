//! Root supervisor: store, callback, and per-task actors

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};

use crate::daemon::actors::callback::{CallbackActor, CallbackArgs, CallbackMsg};
use crate::daemon::actors::queue::{QUEUE_NAME, QueueActor, QueueArgs, QueueMsg};
use crate::daemon::actors::task::{TaskActor, TaskMsg, cancel_task};
use crate::daemon::actors::{StoreActor, StoreMsg, call, send_reply};
use crate::domain::{
    AgentKind, ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId, TaskRow,
};
use crate::error::AppError;
use crate::home::{Home, LockMode, TaskPaths};
use crate::invocation::{persist_workload, resolve_agent_binary_with};
use crate::machine::{MachineId, load_or_create_machine_id};
use crate::notify::Notifier;
use crate::runner;
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::{CancelResult, LocalAdmission, NewTask, new_queued_task};
use crate::submission::{CallbackExecutable, ExecutionRecord, ExecutorIdentity};

mod recovery;

const STORE_NAME: &str = "homebased.store";
const CALLBACK_NAME: &str = "homebased.callback";

#[cfg(test)]
pub(crate) static SUPERVISOR_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Messages sent to the daemon root supervisor
pub(crate) enum SupervisorMsg {
    /// Callback actor for job events sharing the normal origin delivery path
    GetCallback {
        reply: RpcReplyPort<Result<ActorRef<CallbackMsg>, AppError>>,
    },
    /// Finish the queue actor's newly reserved launch through the normal worker path
    LaunchQueueRun {
        task: TaskId,
        prepared: Result<(), AppError>,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Observe a recovered queue worker without ever spawning queued work
    ObserveQueueRun {
        task: TaskId,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Store ref for `AppState` reads
    GetStore {
        reply: RpcReplyPort<Result<ActorRef<StoreMsg>, AppError>>,
    },
    /// Persist a queued row under its admission, spawn its worker, and watch it
    Launch {
        row: Box<TaskRow>,
        spec: Box<NormalizedSpec>,
        admission: LocalAdmission,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Resolve the Codex executable that callbacks for a local task would use
    ///
    /// A held local task saves it at submit, like the route of a launched one
    CallbackCodex {
        path: String,
        cwd: PathBuf,
        reply: RpcReplyPort<Result<CallbackExecutable, AppError>>,
    },
    /// Finish the launch of a saved local task
    ResumeLocal {
        id: TaskId,
        reply: RpcReplyPort<Result<ProcessStatus, AppError>>,
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
    queue: Option<ActorRef<QueueMsg>>,
}

/// Root actor
pub(crate) struct SupervisorActor;

/// Startup inputs for the root actor
pub(crate) struct SupervisorArgs {
    home: Home,
    callback_codex_pin: Option<String>,
    notifier: Option<Arc<Notifier>>,
    machine_name: String,
    thresholds: crate::queue::schedule::NoticeThresholds,
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
            notifier: None,
            machine_name: "this machine".into(),
            thresholds: crate::queue::schedule::NoticeThresholds::default(),
        }
    }

    /// Set the blocked notice thresholds from the daemon configuration
    pub(crate) fn with_queue_thresholds(
        mut self,
        thresholds: crate::queue::schedule::NoticeThresholds,
    ) -> Self {
        self.thresholds = thresholds;
        self
    }

    /// Supply daemon notification settings to the callback actor
    pub(crate) fn with_notifications(
        mut self,
        notifier: Option<Arc<Notifier>>,
        machine_name: String,
    ) -> Self {
        self.notifier = notifier;
        self.machine_name = machine_name;
        self
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
            notifier,
            machine_name,
            thresholds,
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
        let (callback, _callback_handle) = CallbackActor::spawn_linked(
            Some(CALLBACK_NAME.into()),
            CallbackActor,
            CallbackArgs {
                store: store.clone(),
                home: home.clone(),
                notifier,
                machine_name,
                claude_sessions: std::env::home_dir().map(|home| home.join(".claude/sessions")),
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
            queue: None,
        };
        let detected = tokio::task::spawn_blocking(crate::queue::gpu::detect).await?;
        call(&state.store, |reply| StoreMsg::QueueDetect {
            machine,
            detected,
            reply,
        })
        .await?;
        recovery::recover_tasks(&myself, &mut state).await?;
        let (queue, _) = QueueActor::spawn_linked(
            Some(QUEUE_NAME.into()),
            QueueActor,
            QueueArgs {
                home: state.home.clone(),
                machine,
                store: state.store.clone(),
                supervisor: myself.clone(),
                thresholds,
            },
            myself.get_cell(),
        )
        .await?;
        state.queue = Some(queue);
        // a slow store must not fail startup; the callback actor's retry scan
        // picks these tasks up later
        match call(&state.store, |reply| StoreMsg::PendingInboxTasks { reply }).await {
            Ok(ids) => {
                for id in ids {
                    state.callback.cast(CallbackMsg::DispatchInbox { id })?;
                }
            }
            Err(error) => tracing::warn!("startup origin inbox scan: {error}"),
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
            SupervisorMsg::LaunchQueueRun {
                task,
                prepared,
                reply,
            } => {
                let result = match prepared {
                    Ok(()) => launch_queue_run(&myself, state, task).await,
                    Err(error) => finish_spawn_failed(state, task, &error).await,
                };
                send_reply(reply, result);
            }
            SupervisorMsg::ObserveQueueRun { task, reply } => {
                let result = match task_status(state, task).await {
                    Ok(ProcessStatus::Running) => spawn_task_actor(&myself, state, task).await,
                    Ok(_) => Ok(()),
                    Err(error) => Err(error),
                };
                send_reply(reply, result);
            }
            SupervisorMsg::GetCallback { reply } => send_reply(reply, Ok(state.callback.clone())),
            SupervisorMsg::GetStore { reply } => send_reply(reply, Ok(state.store.clone())),
            SupervisorMsg::Launch {
                row,
                spec,
                admission,
                reply,
            } => {
                send_reply(reply, launch(&myself, state, *row, *spec, admission).await);
            }
            SupervisorMsg::CallbackCodex { path, cwd, reply } => {
                send_reply(reply, Ok(resolve_callback_codex(state, &path, &cwd)));
            }
            SupervisorMsg::ResumeLocal { id, reply } => {
                send_reply(reply, resume_local(&myself, state, id).await);
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
                if matches!(
                    name.as_deref(),
                    Some(STORE_NAME | CALLBACK_NAME | QUEUE_NAME)
                ) {
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

async fn launch_queue_run(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    task: TaskId,
) -> Result<(), AppError> {
    let checkpoint = call(&state.store, |reply| StoreMsg::QueueCheckpoint {
        task,
        reply,
    })
    .await?;
    let Some(checkpoint) = checkpoint else {
        return Ok(());
    };
    if !matches!(
        checkpoint.run.phase,
        crate::queue::RunPhase::Launching { .. }
    ) || task_status(state, task).await? != ProcessStatus::Queued
    {
        return Ok(());
    }
    launch_accepted(supervisor, state, task).await
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
        name: spec.name.clone(),
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
                && saved == spec
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
    let recorded = record_pid(state, id, pid).await;
    // the worker runs whether or not its pid was saved, so it is always watched
    spawn_task_actor(supervisor, state, id).await?;
    recorded
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

fn task_name(id: TaskId) -> String {
    format!("homebased.task.{id}")
}

fn task_id_from_name(name: Option<String>) -> Option<TaskId> {
    let name = name?;
    let rest = name.strip_prefix("homebased.task.")?;
    rest.parse().ok()
}

/// Write the task files, insert the row, spawn the worker with the runner lock held, then watch it
/// Runs inside the supervisor so a task always has an in-process owner from
/// the moment its worker exists, and so a client that disconnects mid-submit
/// cannot strand a Queued row with no worker. The cost is that launches are
/// serialized under one `CALL_TIMEOUT`; each is a few store calls and a
/// fork, so that only bites when SQLite itself stalls. A failed spawn
/// finishes the row as `SpawnFailed` and returns the spawn error
///
/// A release keeps its pre-assigned task UUID, so the release loop resends it
/// after a timed-out attempt that may still be queued here. Launches run one
/// at a time, so the resend sees that attempt's row and only finishes its
/// launch, leaving the files of its worker alone
async fn launch(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    row: TaskRow,
    spec: NormalizedSpec,
    admission: LocalAdmission,
) -> Result<(), AppError> {
    let id = row.id;
    if matches!(admission, LocalAdmission::Released { .. }) && task_exists(state, id).await? {
        resume_local(supervisor, state, id).await?;
        return Ok(());
    }
    let paths = state.home.prepare_task(id)?;
    if let NormalizedWorkload::Agent(agent) = &spec.workload {
        runner::write_task_files(&paths, &agent.prompt, agent.report_trailer)?;
    }
    let codex = resolve_callback_codex(state, &row.env.path, &row.cwd);
    let inserted = call(&state.store, |reply| StoreMsg::InsertLocalTask {
        row: Box::new(row),
        spec: Box::new(spec),
        machine: state.machine,
        admission,
        codex,
        reply,
    })
    .await;
    if let Err(error) = inserted {
        remove_refused_task_files(state, id, &paths, &error).await;
        return Err(error);
    }
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
    let recorded = record_pid(state, id, pid).await;
    // the worker runs whether or not its pid was saved, so it is always watched
    spawn_task_actor(supervisor, state, id).await?;
    recorded
}

/// Remove the files of a launch whose row the store refused
///
/// A timed-out insert may still commit, and a committed row belongs to a
/// worker that reads these files, so only a refusal with no row for this task
/// gives them up. A failed check keeps them
async fn remove_refused_task_files(
    state: &SupervisorState,
    id: TaskId,
    paths: &TaskPaths,
    error: &AppError,
) {
    if matches!(error, AppError::DaemonBusy) || task_exists(state, id).await.unwrap_or(true) {
        return;
    }
    if let Err(cleanup) = std::fs::remove_dir_all(&paths.dir) {
        tracing::warn!(%id, "remove refused task directory: {cleanup}");
    }
}

async fn task_exists(state: &SupervisorState, id: TaskId) -> Result<bool, AppError> {
    Ok(call(&state.store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .is_some())
}

/// Give a saved local task the worker and watch that its launch may have missed
///
/// A submit whose caller timed out can leave its row committed without a
/// worker, or with a worker that nothing watches
async fn resume_local(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    id: TaskId,
) -> Result<ProcessStatus, AppError> {
    if call(&state.store, |reply| StoreMsg::QueueTaskJob {
        task: id,
        reply,
    })
    .await?
    .is_some()
    {
        return task_status(state, id).await;
    }
    if !state.tasks.contains_key(&id) {
        match task_status(state, id).await? {
            ProcessStatus::Queued => launch_accepted(supervisor, state, id).await?,
            status if !status.is_terminal() => spawn_task_actor(supervisor, state, id).await?,
            _ => {}
        }
    }
    // a failed spawn finishes the row, so report the status after the launch
    task_status(state, id).await
}

async fn task_status(state: &SupervisorState, id: TaskId) -> Result<ProcessStatus, AppError> {
    call(&state.store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .map(|row| row.status())
        .ok_or(AppError::TaskNotFound { id })
}

/// Save the spawned runner's pid, refusing one outside the signed range the store keeps
async fn record_pid(state: &SupervisorState, id: TaskId, pid: u32) -> Result<(), AppError> {
    let pid = i32::try_from(pid).map_err(|_| AppError::Internal {
        message: format!("runner pid {pid} for task {id} does not fit a signed process id"),
    })?;
    call(&state.store, |reply| StoreMsg::SetPid { id, pid, reply }).await
}

async fn finish_spawn_failed(
    state: &SupervisorState,
    id: TaskId,
    err: &AppError,
) -> Result<(), AppError> {
    fail_queued_task(state, id, err.to_string()).await?;
    Ok(())
}

/// End a queued task with a spawn failure; no worker can start it afterwards
///
/// Returns whether this call won the transition; a worker that already moved
/// the task to running keeps it
async fn fail_queued_task(
    state: &SupervisorState,
    id: TaskId,
    message: String,
) -> Result<bool, AppError> {
    let reason = ExitReason::SpawnFailed { message };
    let failed = call(&state.store, |reply| StoreMsg::CasExit {
        id,
        from: ProcessStatus::Queued,
        reason,
        evidence: ProcessGroupExitEvidence::NoChildSpawned.into(),
        worker_thread: None,
        reply,
    })
    .await?;
    Ok(failed.is_some())
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
