//! One machine queue: reconcile stored runs, schedule work, and drive cleanup

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use chrono::Utc;
use nix::unistd::Pid;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::task::AbortHandle;

use crate::cleanup::{self, CleanupTiming, GroupOutcome, SweepOutcome};
use crate::daemon::actors::supervisor::SupervisorMsg;
use crate::daemon::actors::task::stop_run_task;
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{
    ProcessGroupExitEvidence, ProcessStatus, TaskId, TaskRow, WorkExitEvidence, Workload,
};
use crate::error::AppError;
use crate::home::Home;
use crate::invocation::{resolve_docker_binary, resolve_executable};
use crate::machine::MachineId;
use crate::queue::schedule::{NoticeThresholds, decide};
use crate::queue::{
    ActiveRun, CleanupFailure, JobId, ResourceId, RunPhase, StepWorkload, StopCause,
};

/// A reserved worker has this long to claim its task before the launch is abandoned
pub(crate) const LAUNCH_CONFIRMATION_BOUND: Duration = Duration::from_secs(10);
const RECONCILE_INTERVAL: Duration = Duration::from_millis(250);

pub(crate) const QUEUE_NAME: &str = "homebased.queue";

/// Wakeups are hints; each reconciliation rereads durable state
pub(crate) enum QueueMsg {
    /// Store mutation, task progress, startup, or recovery scan
    Reconcile,
    /// Completion belongs to an exact task and cleanup attempt
    CleanupFinished {
        resource: ResourceId,
        task: TaskId,
        attempt: u32,
        result: Result<(), CleanupFailure>,
    },
}

/// The machine-local queue actor
pub(crate) struct QueueActor;

/// Fixed actor dependencies
pub(crate) struct QueueArgs {
    /// State directory of this daemon
    pub home: Home,
    /// Authority that owns the queue
    pub machine: MachineId,
    /// Owner of the daemon store connection
    pub store: ActorRef<StoreMsg>,
    /// Worker launch and watch owner
    pub supervisor: ActorRef<SupervisorMsg>,
    /// Blocked notice settings from config.toml
    pub thresholds: NoticeThresholds,
}

/// Only timers and cleanup handles are transient; all domain state is in the store
pub(crate) struct QueueState {
    args: QueueArgs,
    timer: AbortHandle,
    notice: Option<AbortHandle>,
    cleanups: HashMap<TaskId, AbortHandle>,
}

impl Drop for QueueState {
    fn drop(&mut self) {
        self.timer.abort();
        if let Some(timer) = self.notice.take() {
            timer.abort();
        }
        for (_, cleanup) in self.cleanups.drain() {
            cleanup.abort();
        }
    }
}

impl Actor for QueueActor {
    type Msg = QueueMsg;
    type State = QueueState;
    type Arguments = QueueArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<QueueMsg>,
        args: QueueArgs,
    ) -> Result<QueueState, ActorProcessingErr> {
        args.store.cast(StoreMsg::WatchQueue {
            queue: myself.clone(),
        })?;
        let timer = myself
            .send_after(Duration::ZERO, || QueueMsg::Reconcile)
            .abort_handle();
        Ok(QueueState {
            args,
            timer,
            notice: None,
            cleanups: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<QueueMsg>,
        message: QueueMsg,
        state: &mut QueueState,
    ) -> Result<(), ActorProcessingErr> {
        state.timer.abort();
        if let QueueMsg::CleanupFinished {
            resource,
            task,
            attempt,
            result,
        } = message
        {
            // the sweep is done before this message; a failed commit can safely
            // start a new idempotent attempt on the next stored-state scan
            let committed = call(&state.args.store, |reply| StoreMsg::QueueCleanupResult {
                resource,
                task,
                attempt,
                result,
                reply,
            })
            .await;
            state.cleanups.remove(&task);
            if let Err(error) = committed {
                tracing::warn!(%task, "queue cleanup commit: {error}");
            }
        }
        if let Err(error) = reconcile(&myself, state).await {
            tracing::warn!("queue reconcile: {error}");
        }
        state.timer = myself
            .send_after(RECONCILE_INTERVAL, || QueueMsg::Reconcile)
            .abort_handle();
        Ok(())
    }
}

async fn reconcile(myself: &ActorRef<QueueMsg>, state: &mut QueueState) -> Result<(), AppError> {
    let args = &state.args;
    let resources = call(&args.store, |reply| StoreMsg::QueueResources {
        machine: args.machine,
        reply,
    })
    .await?;
    for run in resources.into_iter().filter_map(|resource| resource.run) {
        reconcile_run(myself, state, run).await?;
    }

    let args = &state.args;
    let snapshot = call(&args.store, |reply| StoreMsg::QueueSnapshot {
        machine: args.machine,
        now: Utc::now(),
        thresholds: args.thresholds,
        reply,
    })
    .await?;
    let decisions = decide(&snapshot);
    for launch in &decisions.launches {
        launch_job(args, launch.job, launch.resource).await?;
    }
    if let Some(stop) = decisions.preemption {
        let committed = call(&args.store, |reply| StoreMsg::QueueStop {
            stop,
            now: Utc::now(),
            reply,
        })
        .await?;
        if let Some(run) = committed
            && let RunPhase::Stopping { cause, .. } = run.phase
        {
            enact_stop(args, run.task, cause).await?;
        }
    }

    let head = decisions
        .blocked
        .as_ref()
        .map(|head| (head.job, head.blocked_since));
    let unchanged = snapshot.episode.map(|episode| episode.job) == head.map(|(job, _)| job);
    let episode = if unchanged {
        snapshot.episode
    } else {
        call(&args.store, |reply| StoreMsg::QueueRecordHead {
            machine: args.machine,
            head,
            reply,
        })
        .await?
    };
    if let Some(timer) = state.notice.take() {
        timer.abort();
    }
    if let (Some(head), Some(episode)) = (decisions.blocked, episode)
        && !episode.notified
    {
        if head.send_now {
            call(&args.store, |reply| StoreMsg::QueueBlocked {
                machine: args.machine,
                episode,
                thresholds: args.thresholds,
                now: Utc::now(),
                reply,
            })
            .await?;
        } else {
            let wait = (head.due_at - Utc::now())
                .to_std()
                .unwrap_or(Duration::ZERO);
            state.notice = Some(
                myself
                    .send_after(wait.min(Duration::from_secs(3600)), || QueueMsg::Reconcile)
                    .abort_handle(),
            );
        }
    }
    Ok(())
}

async fn reconcile_run(
    myself: &ActorRef<QueueMsg>,
    state: &mut QueueState,
    run: ActiveRun,
) -> Result<(), AppError> {
    let args = &state.args;
    match run.phase {
        RunPhase::Launching { reserved_at } => {
            let row = call(&args.store, |reply| StoreMsg::GetTask {
                id: run.task,
                reply,
            })
            .await?
            .ok_or(AppError::TaskNotFound { id: run.task })?;
            if row.status() == ProcessStatus::Running {
                observe_run(args, run.task).await?;
            } else if (Utc::now() - reserved_at)
                .to_std()
                .unwrap_or(Duration::ZERO)
                >= LAUNCH_CONFIRMATION_BOUND
            {
                call(&args.store, |reply| StoreMsg::QueueAbandon {
                    resource: run.resource,
                    task: run.task,
                    reply,
                })
                .await?;
            }
        }
        RunPhase::Executing { .. } => observe_run(args, run.task).await?,
        RunPhase::Stopping { cause, .. } => {
            enact_stop(args, run.task, cause).await?;
            observe_run(args, run.task).await?;
        }
        RunPhase::Cleaning { .. } if !state.cleanups.contains_key(&run.task) => {
            start_cleanup(myself, state, run).await?;
        }
        RunPhase::Cleaning { .. } | RunPhase::Attention { .. } => {}
    }
    Ok(())
}

async fn observe_run(args: &QueueArgs, task: TaskId) -> Result<(), AppError> {
    call(&args.supervisor, |reply| SupervisorMsg::ObserveQueueRun {
        task,
        reply,
    })
    .await
}

async fn enact_stop(args: &QueueArgs, task: TaskId, cause: StopCause) -> Result<(), AppError> {
    if cause != StopCause::Yield {
        let row = call(&args.store, |reply| StoreMsg::GetTask { id: task, reply })
            .await?
            .ok_or(AppError::TaskNotFound { id: task })?;
        // the worker polls its durable marker; repeated recovery scans must not
        // send another signal or reset the stop time after the first request
        if row.cancel_requested_at.is_none() && !row.state.is_terminal() {
            stop_run_task(&args.store, task).await?;
        }
        return Ok(());
    }
    if let Some(checkpoint) = call(&args.store, |reply| StoreMsg::QueueCheckpoint {
        task,
        reply,
    })
    .await?
    {
        checkpoint.request_yield(&args.home)?;
    }
    Ok(())
}

async fn launch_job(args: &QueueArgs, job: JobId, resource: ResourceId) -> Result<(), AppError> {
    let record = call(&args.store, |reply| StoreMsg::QueueJob { id: job, reply })
        .await?
        .ok_or_else(|| AppError::Internal {
            message: format!("scheduled job {job} is missing"),
        })?;
    let step = record
        .spec
        .steps
        .get(record.next_step)
        .ok_or_else(|| AppError::Internal {
            message: format!("scheduled job {job} has no next step"),
        })?;
    let binary = match step {
        StepWorkload::Task(workload) => resolve_executable(
            workload.command.program(),
            &record.env.path,
            &record.spec.cwd,
        ),
        StepWorkload::Container(_) => resolve_docker_binary(&record.env.path, &record.spec.cwd),
    };
    let task = TaskId::new();
    let binary = match binary {
        Ok(binary) => binary,
        Err(error) => {
            return call(&args.store, |reply| StoreMsg::QueueLaunchFailed {
                machine: args.machine,
                job,
                resource,
                task,
                message: error.to_string(),
                now: Utc::now(),
                reply,
            })
            .await;
        }
    };
    call(&args.store, |reply| StoreMsg::QueueReserve {
        machine: args.machine,
        job,
        resource,
        task,
        binary,
        now: Utc::now(),
        reply,
    })
    .await?;
    let prepared = call(&args.store, |reply| StoreMsg::QueueCheckpoint {
        task,
        reply,
    })
    .await?
    .ok_or_else(|| AppError::Internal {
        message: "new reservation is missing".into(),
    })?
    .prepare(&args.home);
    call(&args.supervisor, |reply| SupervisorMsg::LaunchQueueRun {
        task,
        prepared,
        reply,
    })
    .await
}

async fn start_cleanup(
    myself: &ActorRef<QueueMsg>,
    state: &mut QueueState,
    run: ActiveRun,
) -> Result<(), AppError> {
    let args = &state.args;
    let row = call(&args.store, |reply| StoreMsg::GetTask {
        id: run.task,
        reply,
    })
    .await?
    .ok_or(AppError::TaskNotFound { id: run.task })?;
    let mut protected: BTreeSet<_> = call(&args.store, |reply| StoreMsg::QueueProtected { reply })
        .await?
        .into_iter()
        .map(Pid::from_raw)
        .collect();
    protected.insert(Pid::this());
    let attempt = call(&args.store, |reply| StoreMsg::QueueBeginCleanup {
        resource: run.resource,
        task: run.task,
        reply,
    })
    .await?;
    let myself = myself.clone();
    let task = run.task;
    let handle = tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || cleanup_run(&row, &protected))
            .await
            .unwrap_or_else(|error| {
                Err(cleanup::CleanupFailure::EnumerationFailed {
                    message: format!("cleanup thread: {error}"),
                }
                .into())
            });
        let _ = myself.cast(QueueMsg::CleanupFinished {
            resource: run.resource,
            task,
            attempt,
            result,
        });
    })
    .abort_handle();
    state.cleanups.insert(task, handle);
    Ok(())
}

fn cleanup_run(row: &TaskRow, protected: &BTreeSet<Pid>) -> Result<(), CleanupFailure> {
    if matches!(row.workload, Workload::Container(_)) {
        return match row.work_exit_evidence() {
            WorkExitEvidence::Unconfirmed => Err(CleanupFailure::ContainerUnconfirmed),
            _ => Ok(()),
        };
    }
    if row.status() == ProcessStatus::Lost {
        let Some(child) = row.child else {
            return Err(CleanupFailure::ProcessGroupUnconfirmed);
        };
        if let GroupOutcome::Incomplete(failure) =
            cleanup::cleanup_lost_group(child, row.id, protected, CleanupTiming::STANDARD)
        {
            return Err(failure.into());
        }
    } else if row.process_group_exit_evidence == ProcessGroupExitEvidence::Unconfirmed {
        return Err(CleanupFailure::ProcessGroupUnconfirmed);
    }
    match cleanup::sweep_marker(
        row.id,
        row.child.map(|child| child.start),
        protected,
        CleanupTiming::STANDARD,
    ) {
        SweepOutcome::Completed { .. } => Ok(()),
        SweepOutcome::Incomplete(failure) => Err(failure.into()),
    }
}

#[cfg(test)]
mod tests;
