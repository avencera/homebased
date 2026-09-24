//! Container witness state machine for one container task
//!
//! The task-run worker creates the container, saves its ID, starts it, follows
//! its logs, and waits for it. When the container stops, the worker reads the
//! exit code, removes the container, and confirms that the same ID is absent.
//! Only then is the evidence [`ContainerExitEvidence::Confirmed`]
//!
//! The container runs under `dockerd`, so it outlives the worker. A worker that
//! stops without a cancel leaves the container running; a later worker adopts
//! it by the saved ID, the ID file, or the fixed name and task label. Every
//! state that Homebased cannot prove keeps the evidence unconfirmed

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::time::{self, Instant};

use super::docker::{append_note, container_name};
use crate::domain::{ContainerExitEvidence, ContainerId, ExitReason, TaskId};
use crate::error::AppError;

/// Why one Docker Engine call did not produce an answer
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// `dockerd` or the CLI failed, so the container state is unknown
    #[error("{0}")]
    Unavailable(String),
}

/// Container state as Docker Engine reports it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerStatus {
    /// Created and never started
    Created,
    /// Running
    Running,
    /// Paused while running
    Paused,
    /// Restarting
    Restarting,
    /// Being removed
    Removing,
    /// Stopped with an exit code
    Exited {
        /// Container exit code
        exit_code: i32,
    },
    /// Stopped and partly removed, with an exit code
    Dead {
        /// Container exit code
        exit_code: i32,
    },
}

impl ContainerStatus {
    fn exit_code(self) -> Option<i32> {
        match self {
            Self::Exited { exit_code } | Self::Dead { exit_code } => Some(exit_code),
            Self::Created | Self::Running | Self::Paused | Self::Restarting | Self::Removing => {
                None
            }
        }
    }

    fn is_live(self) -> bool {
        matches!(self, Self::Running | Self::Paused | Self::Restarting)
    }
}

/// One inspected container
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerObservation {
    /// Full container ID
    pub id: ContainerId,
    /// Current state
    pub status: ContainerStatus,
    /// Value of the Homebased task label, if the container has one
    pub task_label: Option<String>,
}

/// Answer to one lookup by ID or name
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainerProbe {
    /// Docker answered and lists no such container
    Absent,
    /// The container exists in this state
    Present(ContainerObservation),
}

/// Docker Engine operations that the witness needs
pub(crate) trait ContainerEngine {
    /// Running log follower
    type Logs: LogFollower;

    /// Create a container and return its full ID
    fn create(&self, args: &[String]) -> impl Future<Output = Result<ContainerId, EngineError>>;
    /// Start a created container
    fn start(&self, id: &ContainerId) -> impl Future<Output = Result<(), EngineError>>;
    /// Look up one container by full ID
    fn probe_id(
        &self,
        id: &ContainerId,
    ) -> impl Future<Output = Result<ContainerProbe, EngineError>>;
    /// Look up one container by exact name
    fn probe_name(&self, name: &str) -> impl Future<Output = Result<ContainerProbe, EngineError>>;
    /// Block until the container is not running
    fn wait(&self, id: &ContainerId) -> impl Future<Output = Result<(), EngineError>>;
    /// Ask the container to stop, then kill it after `grace`
    fn stop(
        &self,
        id: &ContainerId,
        grace: Duration,
    ) -> impl Future<Output = Result<(), EngineError>>;
    /// Kill the container now
    fn kill(&self, id: &ContainerId) -> impl Future<Output = Result<(), EngineError>>;
    /// Remove a stopped container without force
    fn remove(&self, id: &ContainerId) -> impl Future<Output = Result<(), EngineError>>;
    /// Append the container's logs to `output`, from `since` when set
    fn follow_logs(
        &self,
        id: &ContainerId,
        since: Option<DateTime<Utc>>,
        output: &Path,
    ) -> Result<Self::Logs, EngineError>;
}

/// Running log follower
pub(crate) trait LogFollower {
    /// Let the follower drain for up to `drain`, then stop it
    fn finish(self, drain: Duration) -> impl Future<Output = ()>;
}

/// Durable records that the witness writes as it proceeds
pub(crate) trait ContainerLedger {
    /// Container ID saved for this task, if any
    fn saved_container(&self) -> Result<Option<ContainerId>, AppError>;
    /// Save the container ID before the container starts
    fn save_container(&self, id: &ContainerId) -> Result<(), AppError>;
    /// Record that the container started
    fn record_started(&self) -> Result<(), AppError>;
    /// Record that a worker reached the container, which ends an adoption streak
    fn record_observed(&self) -> Result<(), AppError>;
}

/// Why the worker must stop supervising
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interrupt {
    /// Cancellation was requested: stop the container
    Cancel,
    /// The worker must stop without a cancel; the container keeps running
    Detach,
}

/// Source of cancellation and worker-stop requests
pub(crate) trait Interrupts {
    /// Whether cancellation is already requested
    fn cancel_requested(&mut self) -> bool;
    /// Resolve at the next interrupt. Must be safe to drop before it resolves
    fn next(&mut self) -> impl Future<Output = Interrupt>;
}

/// End of one worker's supervision of a container task
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ContainerRunEnd {
    /// The task is terminal with this reason and container evidence
    Finished {
        /// Task exit reason
        reason: ExitReason,
        /// Container witness
        evidence: ContainerExitEvidence,
    },
    /// The container's exit code cannot be known, so the task is lost
    Lost {
        /// Explanation written to the task output
        note: String,
    },
    /// The worker stops and leaves the task running for a later worker
    Detached,
}

/// Waits and bounds of one container supervision
#[derive(Debug, Clone, Copy)]
pub(crate) struct ContainerTiming {
    /// Grace that `docker stop` gives the container on cancel
    pub(crate) stop_grace: Duration,
    /// Longest wait for a killed container to show as stopped
    pub(crate) kill_wait: Duration,
    /// Interval between repeated probes
    pub(crate) poll: Duration,
    /// Longest time Docker may stay unreachable before the witness gives up
    pub(crate) unavailable_budget: Duration,
    /// Wait before a missing container proves that a launch never happened
    pub(crate) settle: Duration,
    /// Longest time the log follower may drain after the container stops
    pub(crate) log_drain: Duration,
}

/// Production timing
pub(crate) const CONTAINER_TIMING: ContainerTiming = ContainerTiming {
    stop_grace: Duration::from_secs(10),
    kill_wait: Duration::from_secs(10),
    poll: Duration::from_secs(1),
    unavailable_budget: Duration::from_secs(300),
    settle: Duration::from_secs(2),
    log_drain: Duration::from_secs(5),
};

/// How the worker came to this task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Entry {
    /// This worker created or started the container
    Launch,
    /// This worker adopted a container that an earlier worker created
    Adopt {
        /// Last write to `output.log` before this worker wrote anything; an
        /// earlier worker copied the container's logs up to about this time
        copied_until: Option<DateTime<Utc>>,
    },
}

/// Supervision of one container task by one worker
pub(crate) struct ContainerRun<'a, E, L, I> {
    engine: &'a E,
    ledger: &'a L,
    interrupts: &'a mut I,
    task: TaskId,
    output: PathBuf,
    timing: ContainerTiming,
}

impl<'a, E, L, I> ContainerRun<'a, E, L, I>
where
    E: ContainerEngine,
    L: ContainerLedger,
    I: Interrupts,
{
    /// Supervise one task with its engine, records, and interrupts
    pub(crate) fn new(
        engine: &'a E,
        ledger: &'a L,
        interrupts: &'a mut I,
        task: TaskId,
        output: PathBuf,
        timing: ContainerTiming,
    ) -> Self {
        Self {
            engine,
            ledger,
            interrupts,
            task,
            output,
            timing,
        }
    }

    fn note(&self, note: &str) {
        append_note(&self.output, note);
    }

    fn name(&self) -> String {
        container_name(self.task)
    }

    fn never_started(&self, reason: ExitReason) -> ContainerRunEnd {
        ContainerRunEnd::Finished {
            reason,
            evidence: ContainerExitEvidence::NeverStarted,
        }
    }

    fn spawn_failed(&self, message: String) -> ContainerRunEnd {
        self.note(&message);
        self.never_started(ExitReason::SpawnFailed { message })
    }

    fn lost(&self, note: String) -> ContainerRunEnd {
        ContainerRunEnd::Lost { note }
    }

    /// Create, save, start, and supervise a new container for a queued task
    ///
    /// An ID file from an earlier create means this is not a new launch, so the
    /// worker adopts whatever that create left
    pub(crate) async fn launch(
        &mut self,
        create_args: &[String],
        cidfile: &Path,
    ) -> ContainerRunEnd {
        if self.interrupts.cancel_requested() {
            return self.never_started(ExitReason::Cancelled);
        }
        if cidfile.exists() {
            return self.adopt(cidfile).await;
        }
        let id = match self.engine.create(create_args).await {
            Ok(id) => id,
            Err(EngineError::Unavailable(message)) => {
                return self.after_failed_create(message).await;
            }
        };
        if let Err(error) = self.ledger.save_container(&id) {
            // the witness needs a saved ID before start, so this container never runs
            let message = format!("could not save container id {id}: {error}");
            return self
                .remove_unstarted(&id, ExitReason::SpawnFailed { message })
                .await;
        }
        if self.interrupts.cancel_requested() {
            return self.remove_unstarted(&id, ExitReason::Cancelled).await;
        }
        let start_error = self.engine.start(&id).await.err();
        if let Some(EngineError::Unavailable(message)) = &start_error {
            self.note(&format!("docker start {id} failed: {message}"));
        }
        let failure = start_error.map(|EngineError::Unavailable(message)| message);
        self.continue_with(&id, Entry::Launch, failure).await
    }

    /// Adopt the container of a task whose earlier worker stopped
    ///
    /// The saved ID comes first, then the ID file, then the fixed name with this
    /// task's label. With none of them, the launch never created a container
    pub(crate) async fn adopt(&mut self, cidfile: &Path) -> ContainerRunEnd {
        let entry = self.adoption();
        let saved = match self.ledger.saved_container() {
            Ok(saved) => saved,
            Err(error) => return self.lost(format!("read saved container id: {error}")),
        };
        let id = match saved.or_else(|| read_cidfile(cidfile)) {
            Some(id) => id,
            None => match self.find_own_container().await {
                Ok(Some(id)) => id,
                Ok(None) => {
                    return self.spawn_failed(format!(
                        "the container launch did not complete: no saved id, no id file, and no container named {}",
                        self.name()
                    ));
                }
                Err(end) => return end,
            },
        };
        if let Err(error) = self.ledger.save_container(&id) {
            return self.lost(format!("save adopted container id {id}: {error}"));
        }
        self.note(&format!("adopting container {id}"));
        self.continue_with(&id, entry, None).await
    }

    /// Find this task's container by its fixed name after a settle wait
    ///
    /// A create that a stopped worker sent may still land, so absence is read
    /// twice. A container that lands later stays created and never starts
    async fn find_own_container(&mut self) -> Result<Option<ContainerId>, ContainerRunEnd> {
        for attempt in 0..2 {
            if attempt > 0 {
                time::sleep(self.timing.settle).await;
            }
            match self.probe_name_answered().await? {
                ContainerProbe::Present(found)
                    if found.task_label.as_deref() == Some(&self.task.to_string()) =>
                {
                    return Ok(Some(found.id));
                }
                // a container with this name but another task label is not this task's
                ContainerProbe::Present(_) | ContainerProbe::Absent => {}
            }
        }
        Ok(None)
    }

    /// Decide what a failed create left behind
    async fn after_failed_create(&mut self, message: String) -> ContainerRunEnd {
        self.note(&format!("docker create failed: {message}"));
        match self.probe_name_answered().await {
            Ok(ContainerProbe::Absent) => self.never_started(ExitReason::SpawnFailed { message }),
            Ok(ContainerProbe::Present(found))
                if found.task_label.as_deref() == Some(&self.task.to_string()) =>
            {
                if let Err(error) = self.ledger.save_container(&found.id) {
                    return self.lost(format!("save container id {}: {error}", found.id));
                }
                let entry = self.adoption();
                self.continue_with(&found.id, entry, None).await
            }
            Ok(ContainerProbe::Present(found)) => self.spawn_failed(format!(
                "container name {} is used by container {}, which this task did not create",
                self.name(),
                found.id
            )),
            Err(end) => end,
        }
    }

    /// Act on the current state of a container that this task owns
    async fn continue_with(
        &mut self,
        id: &ContainerId,
        entry: Entry,
        start_failure: Option<String>,
    ) -> ContainerRunEnd {
        loop {
            let found = match self.observe(id).await {
                Ok(ContainerProbe::Present(found)) => found,
                Ok(ContainerProbe::Absent) => {
                    return self.lost(format!(
                        "container {id} no longer exists and its exit code is unknown"
                    ));
                }
                Err(end) => return end,
            };
            if matches!(entry, Entry::Adopt { .. })
                && let Err(error) = self.ledger.record_observed()
            {
                tracing::warn!(task = %self.task, "record container observation: {error}");
            }
            match found.status {
                ContainerStatus::Created => {
                    let reason = if self.interrupts.cancel_requested() {
                        ExitReason::Cancelled
                    } else {
                        ExitReason::SpawnFailed {
                            message: start_failure.clone().unwrap_or_else(|| {
                                format!("container {id} was created but never started")
                            }),
                        }
                    };
                    match self.engine.remove(id).await {
                        Ok(()) => return self.confirm_unstarted_removed(id, reason).await,
                        // a start that an earlier worker sent may have won; look again
                        Err(EngineError::Unavailable(message)) => {
                            self.note(&format!("docker rm {id} failed: {message}"));
                            time::sleep(self.timing.poll).await;
                        }
                    }
                }
                ContainerStatus::Removing => time::sleep(self.timing.poll).await,
                status if status.is_live() => {
                    self.record_started();
                    let since = self.logs_since(entry);
                    if self.interrupts.cancel_requested() {
                        return self.cancel(id, None).await;
                    }
                    return self.supervise(id, since).await;
                }
                status => {
                    let Some(exit_code) = status.exit_code() else {
                        continue;
                    };
                    self.record_started();
                    // a container that exited before supervision still has its logs
                    let logs = self.follow_logs(id, self.logs_since(entry));
                    finish_logs(logs, self.timing.log_drain).await;
                    return self
                        .finish(id, exit_code, ExitReason::Exit { code: exit_code })
                        .await;
                }
            }
        }
    }

    /// Where log copying starts: all logs for a launch, or after the last
    /// output an earlier worker copied for an adoption
    ///
    /// The last write time of `output.log` bounds what the earlier worker
    /// copied, so an adoption neither repeats nor drops more than lines written
    /// in that same instant
    fn logs_since(&self, entry: Entry) -> Option<DateTime<Utc>> {
        match entry {
            Entry::Launch => None,
            Entry::Adopt { copied_until } => copied_until,
        }
    }

    /// Adoption entry, read before this worker writes to `output.log`
    fn adoption(&self) -> Entry {
        Entry::Adopt {
            copied_until: std::fs::metadata(&self.output)
                .and_then(|metadata| metadata.modified())
                .map(DateTime::<Utc>::from)
                .ok(),
        }
    }

    fn follow_logs(&self, id: &ContainerId, since: Option<DateTime<Utc>>) -> Option<E::Logs> {
        match self.engine.follow_logs(id, since, &self.output) {
            Ok(logs) => Some(logs),
            Err(EngineError::Unavailable(message)) => {
                self.note(&format!("container logs are unavailable: {message}"));
                None
            }
        }
    }

    fn record_started(&self) {
        if let Err(error) = self.ledger.record_started() {
            tracing::warn!(task = %self.task, "record container start: {error}");
        }
    }

    /// Follow logs and wait until the container stops, is cancelled, or the worker detaches
    async fn supervise(
        &mut self,
        id: &ContainerId,
        since: Option<DateTime<Utc>>,
    ) -> ContainerRunEnd {
        let logs = self.follow_logs(id, since);
        loop {
            let interrupt = tokio::select! {
                // an error only means the wait ended early; the probe decides
                _ = self.engine.wait(id) => None,
                interrupt = self.interrupts.next() => Some(interrupt),
            };
            match interrupt {
                Some(Interrupt::Cancel) => return self.cancel(id, logs).await,
                Some(Interrupt::Detach) => {
                    finish_logs(logs, Duration::ZERO).await;
                    return ContainerRunEnd::Detached;
                }
                None => {}
            }
            let found = match self.observe(id).await {
                Ok(ContainerProbe::Present(found)) => found,
                Ok(ContainerProbe::Absent) => {
                    finish_logs(logs, self.timing.log_drain).await;
                    return self.lost(format!(
                        "container {id} disappeared before its exit code was read"
                    ));
                }
                Err(end) => {
                    finish_logs(logs, Duration::ZERO).await;
                    return end;
                }
            };
            if let Some(exit_code) = found.status.exit_code() {
                finish_logs(logs, self.timing.log_drain).await;
                return self
                    .finish(id, exit_code, ExitReason::Exit { code: exit_code })
                    .await;
            }
            time::sleep(self.timing.poll).await;
        }
    }

    /// Stop, then kill if needed, and finish with a cancelled reason
    async fn cancel(&mut self, id: &ContainerId, logs: Option<E::Logs>) -> ContainerRunEnd {
        self.note(&format!("cancel: stopping container {id}"));
        if let Err(EngineError::Unavailable(message)) =
            self.engine.stop(id, self.timing.stop_grace).await
        {
            self.note(&format!("docker stop {id} failed: {message}"));
        }
        let deadline = Instant::now() + self.timing.kill_wait;
        let mut killed = false;
        let status = loop {
            let status = match self.observe(id).await {
                Ok(ContainerProbe::Present(found)) => Some(found.status),
                Ok(ContainerProbe::Absent) => None,
                Err(_) => break None,
            };
            match status {
                Some(status) if status.is_live() && Instant::now() < deadline => {
                    if !killed {
                        killed = true;
                        if let Err(EngineError::Unavailable(message)) = self.engine.kill(id).await {
                            self.note(&format!("docker kill {id} failed: {message}"));
                        }
                    }
                    time::sleep(self.timing.poll).await;
                }
                Some(ContainerStatus::Removing) if Instant::now() < deadline => {
                    time::sleep(self.timing.poll).await;
                }
                status => break status,
            }
        };
        finish_logs(logs, self.timing.log_drain).await;
        match status.and_then(ContainerStatus::exit_code) {
            Some(exit_code) => self.finish(id, exit_code, ExitReason::Cancelled).await,
            None if status == Some(ContainerStatus::Created) => {
                self.remove_unstarted(id, ExitReason::Cancelled).await
            }
            None => {
                self.note(&format!(
                    "container {id} did not report an exit code after cancel"
                ));
                ContainerRunEnd::Finished {
                    reason: ExitReason::Cancelled,
                    evidence: ContainerExitEvidence::Unconfirmed,
                }
            }
        }
    }

    /// Remove an exited container and confirm that its ID is absent
    async fn finish(
        &mut self,
        id: &ContainerId,
        exit_code: i32,
        reason: ExitReason,
    ) -> ContainerRunEnd {
        let deadline = Instant::now() + self.timing.unavailable_budget;
        let gap = loop {
            if let Err(EngineError::Unavailable(message)) = self.engine.remove(id).await {
                self.note(&format!("docker rm {id} failed: {message}"));
            }
            match self.engine.probe_id(id).await {
                Ok(ContainerProbe::Absent) => {
                    return ContainerRunEnd::Finished {
                        reason,
                        evidence: ContainerExitEvidence::Confirmed {
                            container_id: id.clone(),
                            exit_code,
                        },
                    };
                }
                // something started the exact container again after it exited
                Ok(ContainerProbe::Present(found)) if found.status.is_live() => {
                    break format!("container {id} started again after it exited with {exit_code}");
                }
                Ok(ContainerProbe::Present(_)) | Err(_) if Instant::now() >= deadline => {
                    break format!(
                        "container {id} exited with {exit_code}, but its removal is not confirmed"
                    );
                }
                Ok(ContainerProbe::Present(_)) | Err(_) => time::sleep(self.timing.poll).await,
            }
        };
        self.note(&gap);
        ContainerRunEnd::Finished {
            reason,
            evidence: ContainerExitEvidence::Unconfirmed,
        }
    }

    /// Remove a container that never started and confirm that it is absent
    async fn remove_unstarted(&mut self, id: &ContainerId, reason: ExitReason) -> ContainerRunEnd {
        if let Err(EngineError::Unavailable(message)) = self.engine.remove(id).await {
            self.note(&format!("docker rm {id} failed: {message}"));
        }
        self.confirm_unstarted_removed(id, reason).await
    }

    async fn confirm_unstarted_removed(
        &mut self,
        id: &ContainerId,
        reason: ExitReason,
    ) -> ContainerRunEnd {
        match self.observe(id).await {
            Ok(ContainerProbe::Absent) => {
                if let ExitReason::SpawnFailed { message } = &reason {
                    self.note(message);
                }
                self.never_started(reason)
            }
            Ok(ContainerProbe::Present(found)) => {
                self.note(&format!(
                    "container {id} is still present ({:?}) after removal of the unstarted container",
                    found.status
                ));
                ContainerRunEnd::Finished {
                    reason,
                    evidence: ContainerExitEvidence::Unconfirmed,
                }
            }
            Err(ContainerRunEnd::Lost { note }) => {
                self.note(&note);
                ContainerRunEnd::Finished {
                    reason,
                    evidence: ContainerExitEvidence::Unconfirmed,
                }
            }
            Err(end) => end,
        }
    }

    /// Probe one ID until Docker answers or the unavailable budget passes
    async fn observe(&mut self, id: &ContainerId) -> Result<ContainerProbe, ContainerRunEnd> {
        let deadline = Instant::now() + self.timing.unavailable_budget;
        loop {
            match self.engine.probe_id(id).await {
                Ok(probe) => return Ok(probe),
                Err(EngineError::Unavailable(message)) if Instant::now() >= deadline => {
                    return Err(self.lost(format!(
                        "docker did not answer for container {id} within {}s: {message}",
                        self.timing.unavailable_budget.as_secs()
                    )));
                }
                Err(_) => time::sleep(self.timing.poll).await,
            }
        }
    }

    /// Probe the fixed name until Docker answers or the unavailable budget passes
    async fn probe_name_answered(&mut self) -> Result<ContainerProbe, ContainerRunEnd> {
        let name = self.name();
        let deadline = Instant::now() + self.timing.unavailable_budget;
        loop {
            match self.engine.probe_name(&name).await {
                Ok(probe) => return Ok(probe),
                Err(EngineError::Unavailable(message)) if Instant::now() >= deadline => {
                    return Err(self.lost(format!(
                        "docker did not answer for container {name} within {}s: {message}",
                        self.timing.unavailable_budget.as_secs()
                    )));
                }
                Err(_) => time::sleep(self.timing.poll).await,
            }
        }
    }
}

async fn finish_logs<F: LogFollower>(logs: Option<F>, drain: Duration) {
    if let Some(logs) = logs {
        logs.finish(drain).await;
    }
}

/// Read the container ID that Docker wrote to the ID file, if it holds one
fn read_cidfile(cidfile: &Path) -> Option<ContainerId> {
    let text = std::fs::read_to_string(cidfile).ok()?;
    ContainerId::parse(&text).ok()
}

#[cfg(test)]
mod tests;
