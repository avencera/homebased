//! Witness state machine against a scripted Docker engine

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::{Notify, mpsc};

use super::{
    ContainerEngine, ContainerLedger, ContainerObservation, ContainerProbe, ContainerRun,
    ContainerRunEnd, ContainerStatus, ContainerTiming, EngineError, Interrupt, Interrupts,
    LogFollower,
};
use crate::container::docker::container_name;
use crate::domain::{ContainerExitEvidence, ContainerId, ExitReason, TaskId};
use crate::error::AppError;

const TIMING: ContainerTiming = ContainerTiming {
    stop_grace: Duration::from_secs(1),
    kill_wait: Duration::from_millis(100),
    poll: Duration::from_millis(5),
    unavailable_budget: Duration::from_millis(80),
    settle: Duration::from_millis(5),
    log_drain: Duration::ZERO,
};

/// Shared record of engine and ledger calls, in order
type Calls = Arc<Mutex<Vec<String>>>;

#[derive(Debug, Clone)]
struct FakeContainer {
    id: ContainerId,
    name: String,
    status: ContainerStatus,
    task_label: Option<String>,
}

#[derive(Debug, Default)]
struct FakeState {
    containers: Vec<FakeContainer>,
    next_id: u8,
    /// every call fails while set
    down: bool,
    /// the next this many calls fail, then Docker answers again
    failing_calls: usize,
    /// the next this many waits fail while the container keeps running
    failing_waits: usize,
    /// create fails with this message and creates nothing
    create_refused: Option<String>,
    /// create creates the container but its client reports this failure
    create_lost_reply: Option<String>,
    /// start fails with this message and leaves the container created
    start_refused: Option<String>,
    /// stop leaves the container running
    ignore_stop: bool,
    /// removal fails while the engine stays up
    remove_refused: bool,
}

#[derive(Clone, Default)]
struct FakeDocker {
    state: Arc<Mutex<FakeState>>,
    changed: Arc<Notify>,
    calls: Calls,
}

impl FakeDocker {
    fn with_calls(calls: Calls) -> Self {
        Self {
            calls,
            ..Self::default()
        }
    }

    fn record(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().unwrap()
    }

    /// Fail when Docker is down or a transient failure is scripted
    fn answer(&self) -> Result<(), EngineError> {
        let mut state = self.state();
        if state.down {
            return Err(EngineError::Unavailable("dockerd is down".into()));
        }
        if state.failing_calls > 0 {
            state.failing_calls -= 1;
            return Err(EngineError::Unavailable("transient failure".into()));
        }
        Ok(())
    }

    fn add(&self, name: &str, status: ContainerStatus, task_label: Option<String>) -> ContainerId {
        let mut state = self.state();
        state.next_id += 1;
        let id = ContainerId::parse(&format!("{:02x}", state.next_id).repeat(32)).unwrap();
        state.containers.push(FakeContainer {
            id: id.clone(),
            name: name.to_owned(),
            status,
            task_label,
        });
        id
    }

    fn set_status(&self, id: &ContainerId, status: ContainerStatus) {
        let mut state = self.state();
        if let Some(container) = state.containers.iter_mut().find(|c| c.id == *id) {
            container.status = status;
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn status(&self, id: &ContainerId) -> Option<ContainerStatus> {
        self.state()
            .containers
            .iter()
            .find(|c| c.id == *id)
            .map(|c| c.status)
    }

    fn observe(&self, container: &FakeContainer) -> ContainerProbe {
        ContainerProbe::Present(ContainerObservation {
            id: container.id.clone(),
            status: container.status,
            task_label: container.task_label.clone(),
        })
    }
}

struct FakeLogs;

impl LogFollower for FakeLogs {
    async fn finish(self, _drain: Duration) {}
}

impl ContainerEngine for FakeDocker {
    type Logs = FakeLogs;

    async fn create(&self, args: &[String]) -> Result<ContainerId, EngineError> {
        self.record("create".into());
        self.answer()?;
        let name = args[args.iter().position(|arg| arg == "--name").unwrap() + 1].clone();
        let label = args
            .iter()
            .find_map(|arg| arg.strip_prefix("homebased.task="))
            .map(str::to_owned);
        if let Some(message) = self.state().create_refused.clone() {
            return Err(EngineError::Unavailable(message));
        }
        if self.state().containers.iter().any(|c| c.name == name) {
            return Err(EngineError::Unavailable(format!(
                "Conflict. The container name \"/{name}\" is already in use"
            )));
        }
        let id = self.add(&name, ContainerStatus::Created, label);
        if let Some(message) = self.state().create_lost_reply.clone() {
            return Err(EngineError::Unavailable(message));
        }
        Ok(id)
    }

    async fn start(&self, id: &ContainerId) -> Result<(), EngineError> {
        self.record(format!("start {}", &id.as_str()[..2]));
        self.answer()?;
        if let Some(message) = self.state().start_refused.clone() {
            return Err(EngineError::Unavailable(message));
        }
        self.set_status(id, ContainerStatus::Running);
        Ok(())
    }

    async fn probe_id(&self, id: &ContainerId) -> Result<ContainerProbe, EngineError> {
        self.answer()?;
        let state = self.state();
        Ok(state
            .containers
            .iter()
            .find(|c| c.id == *id)
            .map_or(ContainerProbe::Absent, |c| self.observe(c)))
    }

    async fn probe_name(&self, name: &str) -> Result<ContainerProbe, EngineError> {
        self.answer()?;
        let state = self.state();
        Ok(state
            .containers
            .iter()
            .find(|c| c.name == name)
            .map_or(ContainerProbe::Absent, |c| self.observe(c)))
    }

    async fn wait(&self, id: &ContainerId) -> Result<(), EngineError> {
        loop {
            let notified = self.changed.notified();
            self.answer()?;
            {
                let mut state = self.state();
                if state.failing_waits > 0 {
                    state.failing_waits -= 1;
                    return Err(EngineError::Unavailable("docker wait client died".into()));
                }
            }
            match self.status(id) {
                Some(status) if status.is_live() => notified.await,
                _ => return Ok(()),
            }
        }
    }

    async fn stop(&self, id: &ContainerId, _grace: Duration) -> Result<(), EngineError> {
        self.record(format!("stop {}", &id.as_str()[..2]));
        self.answer()?;
        if !self.state().ignore_stop && self.status(id).is_some_and(ContainerStatus::is_live) {
            self.set_status(id, ContainerStatus::Exited { exit_code: 143 });
        }
        Ok(())
    }

    async fn kill(&self, id: &ContainerId) -> Result<(), EngineError> {
        self.record(format!("kill {}", &id.as_str()[..2]));
        self.answer()?;
        if self.status(id).is_some_and(ContainerStatus::is_live) {
            self.set_status(id, ContainerStatus::Exited { exit_code: 137 });
        }
        Ok(())
    }

    async fn remove(&self, id: &ContainerId) -> Result<(), EngineError> {
        self.record(format!("rm {}", &id.as_str()[..2]));
        self.answer()?;
        if self.state().remove_refused {
            return Err(EngineError::Unavailable("removal refused".into()));
        }
        let mut state = self.state();
        match state.containers.iter().position(|c| c.id == *id) {
            Some(index) if state.containers[index].status.is_live() => Err(
                EngineError::Unavailable("cannot remove a running container".into()),
            ),
            Some(index) => {
                state.containers.remove(index);
                Ok(())
            }
            None => Err(EngineError::Unavailable("No such container".into())),
        }
    }

    fn follow_logs(
        &self,
        id: &ContainerId,
        since: Option<DateTime<Utc>>,
        _output: &Path,
    ) -> Result<Self::Logs, EngineError> {
        let since = if since.is_some() { " since" } else { "" };
        self.record(format!("logs {}{since}", &id.as_str()[..2]));
        Ok(FakeLogs)
    }
}

#[derive(Default)]
struct FakeLedger {
    saved: Mutex<Option<ContainerId>>,
    started: AtomicBool,
    observed: AtomicBool,
    refuse_save: bool,
    calls: Calls,
}

impl ContainerLedger for FakeLedger {
    fn saved_container(&self) -> Result<Option<ContainerId>, AppError> {
        Ok(self.saved.lock().unwrap().clone())
    }

    fn save_container(&self, id: &ContainerId) -> Result<(), AppError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("save {}", &id.as_str()[..2]));
        if self.refuse_save {
            return Err(AppError::Internal {
                message: "database is read-only".into(),
            });
        }
        let mut saved = self.saved.lock().unwrap();
        match saved.as_ref() {
            Some(existing) if existing != id => Err(AppError::Internal {
                message: "a different container is saved".into(),
            }),
            _ => {
                *saved = Some(id.clone());
                Ok(())
            }
        }
    }

    fn record_started(&self) -> Result<(), AppError> {
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn record_observed(&self) -> Result<(), AppError> {
        self.observed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct FakeInterrupts {
    cancelled: Arc<AtomicBool>,
    receiver: mpsc::UnboundedReceiver<Interrupt>,
}

impl Interrupts for FakeInterrupts {
    fn cancel_requested(&mut self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    async fn next(&mut self) -> Interrupt {
        match self.receiver.recv().await {
            Some(interrupt) => interrupt,
            None => std::future::pending().await,
        }
    }
}

/// Sender side of the fake interrupts
#[derive(Clone)]
struct Interrupter {
    cancelled: Arc<AtomicBool>,
    sender: mpsc::UnboundedSender<Interrupt>,
}

impl Interrupter {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.sender.send(Interrupt::Cancel).unwrap();
    }

    fn detach(&self) {
        self.sender.send(Interrupt::Detach).unwrap();
    }
}

struct Harness {
    task: TaskId,
    docker: FakeDocker,
    ledger: FakeLedger,
    interrupts: FakeInterrupts,
    interrupter: Interrupter,
    calls: Calls,
    _directory: tempfile::TempDir,
    output: PathBuf,
    cidfile: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let calls: Calls = Arc::default();
        let directory = tempfile::tempdir().unwrap();
        let (sender, receiver) = mpsc::unbounded_channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        Self {
            task: TaskId::new(),
            docker: FakeDocker::with_calls(calls.clone()),
            ledger: FakeLedger {
                calls: calls.clone(),
                ..FakeLedger::default()
            },
            interrupts: FakeInterrupts {
                cancelled: cancelled.clone(),
                receiver,
            },
            interrupter: Interrupter { cancelled, sender },
            calls,
            output: directory.path().join("output.log"),
            cidfile: directory.path().join("container.cid"),
            _directory: directory,
        }
    }

    fn name(&self) -> String {
        container_name(self.task)
    }

    fn label(&self) -> Option<String> {
        Some(self.task.to_string())
    }

    fn create_args(&self) -> Vec<String> {
        vec![
            "container".into(),
            "create".into(),
            "--name".into(),
            self.name(),
            "--label".into(),
            format!("homebased.task={}", self.task),
        ]
    }

    async fn launch(&mut self) -> ContainerRunEnd {
        let args = self.create_args();
        let cidfile = self.cidfile.clone();
        let mut run = ContainerRun::new(
            &self.docker,
            &self.ledger,
            &mut self.interrupts,
            self.task,
            self.output.clone(),
            TIMING,
        );
        tokio::time::timeout(Duration::from_secs(5), run.launch(&args, &cidfile))
            .await
            .expect("the witness must end")
    }

    async fn adopt(&mut self) -> ContainerRunEnd {
        let cidfile = self.cidfile.clone();
        let mut run = ContainerRun::new(
            &self.docker,
            &self.ledger,
            &mut self.interrupts,
            self.task,
            self.output.clone(),
            TIMING,
        );
        tokio::time::timeout(Duration::from_secs(5), run.adopt(&cidfile))
            .await
            .expect("the witness must end")
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn output(&self) -> String {
        std::fs::read_to_string(&self.output).unwrap_or_default()
    }

    /// Stop the only container with `status` once supervision has started
    fn exit_later(&self, status: ContainerStatus) {
        let docker = self.docker.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let running = docker
                    .state()
                    .containers
                    .iter()
                    .find(|c| c.status == ContainerStatus::Running)
                    .map(|c| c.id.clone());
                if let Some(id) = running {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    docker.set_status(&id, status);
                    return;
                }
            }
        });
    }
}

fn confirmed(reason: ExitReason, id: &ContainerId, exit_code: i32) -> ContainerRunEnd {
    ContainerRunEnd::Finished {
        reason,
        evidence: ContainerExitEvidence::Confirmed {
            container_id: id.clone(),
            exit_code,
        },
    }
}

fn saved(harness: &Harness) -> ContainerId {
    harness.ledger.saved.lock().unwrap().clone().unwrap()
}

#[tokio::test]
async fn normal_exit_saves_the_id_before_start_and_confirms_removal() {
    let mut harness = Harness::new();
    harness.exit_later(ContainerStatus::Exited { exit_code: 0 });

    let end = harness.launch().await;

    let id = saved(&harness);
    assert_eq!(end, confirmed(ExitReason::Exit { code: 0 }, &id, 0));
    assert_eq!(harness.docker.status(&id), None, "the container is removed");
    assert!(harness.ledger.started.load(Ordering::SeqCst));
    let short = &id.as_str()[..2];
    assert_eq!(
        harness.calls(),
        [
            "create".to_owned(),
            format!("save {short}"),
            format!("start {short}"),
            format!("logs {short}"),
            format!("rm {short}"),
        ]
    );
}

#[tokio::test]
async fn non_zero_exit_is_the_task_exit_code() {
    let mut harness = Harness::new();
    harness.exit_later(ContainerStatus::Exited { exit_code: 3 });

    let end = harness.launch().await;

    assert_eq!(
        end,
        confirmed(ExitReason::Exit { code: 3 }, &saved(&harness), 3)
    );
}

#[tokio::test]
async fn cancel_stops_the_container_then_confirms_its_removal() {
    let mut harness = Harness::new();
    let interrupter = harness.interrupter.clone();
    let docker = harness.docker.clone();
    tokio::spawn(async move {
        while !docker
            .state()
            .containers
            .iter()
            .any(|c| c.status == ContainerStatus::Running)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        interrupter.cancel();
    });

    let end = harness.launch().await;

    let id = saved(&harness);
    assert_eq!(end, confirmed(ExitReason::Cancelled, &id, 143));
    assert!(
        harness
            .calls()
            .contains(&format!("stop {}", &id.as_str()[..2]))
    );
    assert!(!harness.calls().iter().any(|call| call.starts_with("kill")));
}

#[tokio::test]
async fn cancel_kills_a_container_that_ignores_stop() {
    let mut harness = Harness::new();
    harness.docker.state().ignore_stop = true;
    let interrupter = harness.interrupter.clone();
    let docker = harness.docker.clone();
    tokio::spawn(async move {
        while !docker
            .state()
            .containers
            .iter()
            .any(|c| c.status == ContainerStatus::Running)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        interrupter.cancel();
    });

    let end = harness.launch().await;

    let id = saved(&harness);
    assert_eq!(end, confirmed(ExitReason::Cancelled, &id, 137));
    assert!(
        harness
            .calls()
            .contains(&format!("kill {}", &id.as_str()[..2]))
    );
}

#[tokio::test]
async fn cancel_before_create_starts_nothing() {
    let mut harness = Harness::new();
    harness.interrupter.cancel();

    let end = harness.launch().await;

    assert_eq!(
        end,
        ContainerRunEnd::Finished {
            reason: ExitReason::Cancelled,
            evidence: ContainerExitEvidence::NeverStarted,
        }
    );
    assert!(harness.calls().is_empty());
}

#[tokio::test]
async fn client_loss_keeps_the_task_running_while_the_container_runs() {
    let mut harness = Harness::new();
    // the wait client dies twice while the container keeps running
    harness.docker.state().failing_waits = 2;
    harness.exit_later(ContainerStatus::Exited { exit_code: 0 });

    let end = harness.launch().await;

    assert_eq!(
        end,
        confirmed(ExitReason::Exit { code: 0 }, &saved(&harness), 0)
    );
}

#[tokio::test]
async fn worker_loss_detaches_and_leaves_the_container_running() {
    let mut harness = Harness::new();
    let interrupter = harness.interrupter.clone();
    let docker = harness.docker.clone();
    tokio::spawn(async move {
        while !docker
            .state()
            .containers
            .iter()
            .any(|c| c.status == ContainerStatus::Running)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        interrupter.detach();
    });

    let end = harness.launch().await;

    assert_eq!(end, ContainerRunEnd::Detached);
    assert_eq!(
        harness.docker.status(&saved(&harness)),
        Some(ContainerStatus::Running)
    );
    assert!(!harness.calls().iter().any(|call| call.starts_with("rm")));
}

#[tokio::test]
async fn daemon_restart_adopts_a_running_container_by_its_saved_id() {
    let mut harness = Harness::new();
    let id = harness
        .docker
        .add(&harness.name(), ContainerStatus::Running, harness.label());
    *harness.ledger.saved.lock().unwrap() = Some(id.clone());
    std::fs::write(&harness.output, "copied by the earlier worker\n").unwrap();
    harness.exit_later(ContainerStatus::Exited { exit_code: 0 });

    let end = harness.adopt().await;

    assert_eq!(end, confirmed(ExitReason::Exit { code: 0 }, &id, 0));
    assert!(harness.ledger.observed.load(Ordering::SeqCst));
    // adopted logs start after the earlier worker's last copy, so it is not repeated
    assert!(
        harness
            .calls()
            .contains(&format!("logs {} since", &id.as_str()[..2]))
    );
    assert!(!harness.calls().contains(&"create".to_owned()));
    assert!(harness.output().contains("adopting container"));
}

#[tokio::test]
async fn daemon_restart_after_exit_finishes_the_exit_path() {
    let mut harness = Harness::new();
    let id = harness.docker.add(
        &harness.name(),
        ContainerStatus::Exited { exit_code: 7 },
        harness.label(),
    );
    *harness.ledger.saved.lock().unwrap() = Some(id.clone());

    let end = harness.adopt().await;

    assert_eq!(end, confirmed(ExitReason::Exit { code: 7 }, &id, 7));
    // the exited container's logs are copied before it is removed
    let short = &id.as_str()[..2];
    let calls = harness.calls();
    let logs = calls
        .iter()
        .position(|call| call.starts_with(&format!("logs {short}")));
    let removal = calls.iter().position(|call| *call == format!("rm {short}"));
    assert!(logs.is_some() && logs < removal, "{calls:?}");
}

#[tokio::test]
async fn adoption_reads_the_id_file_when_no_id_was_saved() {
    let mut harness = Harness::new();
    let id = harness.docker.add(
        &harness.name(),
        ContainerStatus::Exited { exit_code: 0 },
        harness.label(),
    );
    std::fs::write(&harness.cidfile, id.as_str()).unwrap();

    let end = harness.adopt().await;

    assert_eq!(end, confirmed(ExitReason::Exit { code: 0 }, &id, 0));
    assert_eq!(saved(&harness), id);
}

#[tokio::test]
async fn a_missing_id_file_falls_back_to_the_name_and_task_label() {
    let mut harness = Harness::new();
    let id = harness
        .docker
        .add(&harness.name(), ContainerStatus::Running, harness.label());
    harness.exit_later(ContainerStatus::Exited { exit_code: 0 });

    let end = harness.adopt().await;

    assert_eq!(end, confirmed(ExitReason::Exit { code: 0 }, &id, 0));
    assert_eq!(saved(&harness), id, "the adopted ID is saved");
}

#[tokio::test]
async fn adoption_with_no_id_and_no_container_proves_the_launch_never_happened() {
    let mut harness = Harness::new();

    let end = harness.adopt().await;

    assert!(matches!(
        end,
        ContainerRunEnd::Finished {
            reason: ExitReason::SpawnFailed { .. },
            evidence: ContainerExitEvidence::NeverStarted,
        }
    ));
    assert!(harness.ledger.saved.lock().unwrap().is_none());
}

#[tokio::test]
async fn adoption_removes_a_container_that_never_started() {
    let mut harness = Harness::new();
    let id = harness
        .docker
        .add(&harness.name(), ContainerStatus::Created, harness.label());
    *harness.ledger.saved.lock().unwrap() = Some(id.clone());

    let end = harness.adopt().await;

    assert!(matches!(
        end,
        ContainerRunEnd::Finished {
            reason: ExitReason::SpawnFailed { .. },
            evidence: ContainerExitEvidence::NeverStarted,
        }
    ));
    assert_eq!(harness.docker.status(&id), None);
    assert!(!harness.ledger.started.load(Ordering::SeqCst));
}

#[tokio::test]
async fn a_saved_container_that_disappeared_is_lost() {
    let mut harness = Harness::new();
    *harness.ledger.saved.lock().unwrap() = Some(ContainerId::parse(&"ab".repeat(32)).unwrap());

    let end = harness.adopt().await;

    assert!(matches!(end, ContainerRunEnd::Lost { .. }), "{end:?}");
}

#[tokio::test]
async fn a_name_used_by_another_task_is_a_conflict_that_starts_nothing() {
    let mut harness = Harness::new();
    let foreign = harness.docker.add(
        &harness.name(),
        ContainerStatus::Running,
        Some("other".into()),
    );

    let end = harness.launch().await;

    let ContainerRunEnd::Finished {
        reason: ExitReason::SpawnFailed { message },
        evidence: ContainerExitEvidence::NeverStarted,
    } = end
    else {
        panic!("a conflict must end the launch unstarted: {end:?}");
    };
    assert!(message.contains("did not create"), "{message}");
    assert_eq!(
        harness.docker.status(&foreign),
        Some(ContainerStatus::Running),
        "the other container is untouched"
    );
    assert!(harness.ledger.saved.lock().unwrap().is_none());
    assert!(!harness.calls().iter().any(|call| call.starts_with("start")));
}

#[tokio::test]
async fn a_create_whose_reply_was_lost_adopts_the_container_it_made() {
    let mut harness = Harness::new();
    harness.docker.state().create_lost_reply = Some("client timed out".into());

    let end = harness.launch().await;

    let ContainerRunEnd::Finished {
        reason: ExitReason::SpawnFailed { .. },
        evidence: ContainerExitEvidence::NeverStarted,
    } = end
    else {
        panic!("the unstarted container must be removed: {end:?}");
    };
    assert!(harness.docker.state().containers.is_empty());
}

#[tokio::test]
async fn a_refused_create_leaves_no_container() {
    let mut harness = Harness::new();
    harness.docker.state().create_refused = Some("No such image".into());

    let end = harness.launch().await;

    assert_eq!(
        end,
        ContainerRunEnd::Finished {
            reason: ExitReason::SpawnFailed {
                message: "No such image".into()
            },
            evidence: ContainerExitEvidence::NeverStarted,
        }
    );
}

#[tokio::test]
async fn a_failed_start_removes_the_created_container() {
    let mut harness = Harness::new();
    harness.docker.state().start_refused = Some("could not select device driver".into());

    let end = harness.launch().await;

    assert_eq!(
        end,
        ContainerRunEnd::Finished {
            reason: ExitReason::SpawnFailed {
                message: "could not select device driver".into()
            },
            evidence: ContainerExitEvidence::NeverStarted,
        }
    );
    assert!(harness.docker.state().containers.is_empty());
}

#[tokio::test]
async fn an_unsaved_id_never_starts_its_container() {
    let mut harness = Harness::new();
    harness.ledger.refuse_save = true;

    let end = harness.launch().await;

    assert!(matches!(
        end,
        ContainerRunEnd::Finished {
            reason: ExitReason::SpawnFailed { .. },
            evidence: ContainerExitEvidence::NeverStarted,
        }
    ));
    assert!(!harness.calls().iter().any(|call| call.starts_with("start")));
    assert!(harness.docker.state().containers.is_empty());
}

#[tokio::test]
async fn dockerd_unavailable_while_running_keeps_the_evidence_unconfirmed() {
    let mut harness = Harness::new();
    let docker = harness.docker.clone();
    tokio::spawn(async move {
        while !docker
            .state()
            .containers
            .iter()
            .any(|c| c.status == ContainerStatus::Running)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        docker.state().down = true;
        docker.changed.notify_waiters();
    });

    let end = harness.launch().await;

    assert!(matches!(end, ContainerRunEnd::Lost { .. }), "{end:?}");
    assert!(harness.ledger.saved.lock().unwrap().is_some());
}

#[tokio::test]
async fn a_transient_dockerd_failure_does_not_end_supervision() {
    let mut harness = Harness::new();
    let docker = harness.docker.clone();
    tokio::spawn(async move {
        while !docker
            .state()
            .containers
            .iter()
            .any(|c| c.status == ContainerStatus::Running)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        docker.state().failing_calls = 3;
        docker.changed.notify_waiters();
    });
    harness.exit_later(ContainerStatus::Exited { exit_code: 0 });

    let end = harness.launch().await;

    assert_eq!(
        end,
        confirmed(ExitReason::Exit { code: 0 }, &saved(&harness), 0)
    );
}

#[tokio::test]
async fn an_unconfirmed_removal_keeps_the_exit_code_but_not_the_witness() {
    let mut harness = Harness::new();
    harness.docker.state().remove_refused = true;
    harness.exit_later(ContainerStatus::Exited { exit_code: 0 });

    let end = harness.launch().await;

    assert_eq!(
        end,
        ContainerRunEnd::Finished {
            reason: ExitReason::Exit { code: 0 },
            evidence: ContainerExitEvidence::Unconfirmed,
        }
    );
    assert!(harness.output().contains("removal is not confirmed"));
}
