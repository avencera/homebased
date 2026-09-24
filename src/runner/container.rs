//! Task-run worker for a container task: launch or adopt, then record the witness

use tokio::signal::unix::Signal as SignalStream;
use tokio::time;
use tracing::warn;

use super::{CANCELLATION_POLL, record_exit};
use crate::container::docker::{CreateContext, DockerCli, append_note, create_args};
use crate::container::lifecycle::{
    CONTAINER_TIMING, ContainerLedger, ContainerRun, ContainerRunEnd, Interrupt, Interrupts,
};
use crate::container::{ContainerUser, ContainerWorkload, check_container_host};
use crate::domain::{
    ContainerId, ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskExitEvidence, TaskId,
    TaskRow,
};
use crate::error::AppError;
use crate::home::TaskPaths;
use crate::store::Store;

/// How this worker came to the container task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Entry {
    /// This worker won the queued-to-running transition and creates the container
    Launch,
    /// An earlier worker stopped while the task ran; adopt its container
    Adopt,
}

/// Supervise one container task and record how it ended
pub(super) async fn run(
    store: &Store,
    row: &TaskRow,
    workload: &ContainerWorkload,
    paths: &TaskPaths,
    entry: Entry,
    sigterm: &mut SignalStream,
) -> Result<(), AppError> {
    let id = row.id;
    let engine = DockerCli::new(row.binary.clone(), row.env.clone());
    let ledger = StoreLedger { store, task: id };
    let mut interrupts = WorkerInterrupts::new(store, id, sigterm);
    let mut run = ContainerRun::new(
        &engine,
        &ledger,
        &mut interrupts,
        id,
        paths.output.clone(),
        CONTAINER_TIMING,
    );
    let end = match entry {
        Entry::Launch => {
            // a host input that disappeared since acceptance fails before any Docker call
            if let Err(error) = check_container_host(workload) {
                let message = error.to_string();
                append_note(&paths.output, &message);
                return record_exit(
                    store,
                    id,
                    paths,
                    &ExitReason::SpawnFailed { message },
                    ProcessGroupExitEvidence::NoChildSpawned.into(),
                );
            }
            let resource = store.resource_for_task(id).unwrap_or_else(|error| {
                warn!(%id, "read resource of container task: {error}");
                None
            });
            let args = create_args(
                workload,
                &CreateContext {
                    task: id,
                    resource,
                    cidfile: &paths.container_cid,
                    default_user: ContainerUser::current(),
                },
            );
            run.launch(&args, &paths.container_cid).await
        }
        Entry::Adopt => run.adopt(&paths.container_cid).await,
    };
    match end {
        ContainerRunEnd::Finished { reason, evidence } => record_exit(
            store,
            id,
            paths,
            &reason,
            TaskExitEvidence {
                // the worker's Docker clients prove nothing about the container
                process_group: ProcessGroupExitEvidence::Unconfirmed,
                container: evidence,
            },
        ),
        ContainerRunEnd::Lost { note } => {
            append_note(&paths.output, &note);
            if store
                .cas_status(id, ProcessStatus::Running, ProcessStatus::Lost)?
                .is_none()
            {
                warn!(%id, "container task changed before it could be marked lost");
            }
            Ok(())
        }
        // the container keeps running; the daemon starts a worker that adopts it
        ContainerRunEnd::Detached => Ok(()),
    }
}

/// Task records that the container witness writes through the worker's store
struct StoreLedger<'a> {
    store: &'a Store,
    task: TaskId,
}

impl ContainerLedger for StoreLedger<'_> {
    fn saved_container(&self) -> Result<Option<ContainerId>, AppError> {
        Ok(self
            .store
            .task_container(self.task)?
            .and_then(|record| record.container_id))
    }

    fn save_container(&self, id: &ContainerId) -> Result<(), AppError> {
        self.store.save_task_container_id(self.task, id)
    }

    fn record_started(&self) -> Result<(), AppError> {
        self.store.record_task_container_started(self.task)
    }

    fn record_observed(&self) -> Result<(), AppError> {
        self.store.reset_task_container_adoptions(self.task)
    }
}

/// Cancellation marker and SIGTERM as seen by the worker
///
/// SIGTERM with a saved cancellation marker cancels the task. SIGTERM without
/// one only stops this worker; the container keeps running for a later worker
struct WorkerInterrupts<'a> {
    store: &'a Store,
    task: TaskId,
    sigterm: &'a mut SignalStream,
    poll: time::Interval,
}

impl<'a> WorkerInterrupts<'a> {
    fn new(store: &'a Store, task: TaskId, sigterm: &'a mut SignalStream) -> Self {
        let mut poll = time::interval(CANCELLATION_POLL);
        poll.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        Self {
            store,
            task,
            sigterm,
            poll,
        }
    }
}

impl Interrupts for WorkerInterrupts<'_> {
    fn cancel_requested(&mut self) -> bool {
        match self.store.require_task(self.task) {
            Ok(row) => row.cancel_requested_at.is_some(),
            Err(error) => {
                warn!(task = %self.task, "read cancellation state: {error}");
                false
            }
        }
    }

    async fn next(&mut self) -> Interrupt {
        loop {
            tokio::select! {
                _ = self.sigterm.recv() => {
                    return if self.cancel_requested() {
                        Interrupt::Cancel
                    } else {
                        Interrupt::Detach
                    };
                }
                _ = self.poll.tick() => {
                    if self.cancel_requested() {
                        return Interrupt::Cancel;
                    }
                }
            }
        }
    }
}
