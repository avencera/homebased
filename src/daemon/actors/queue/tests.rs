//! Phase 4 integration tests: real daemon and task workers, with direct store inputs

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::cleanup::{self, CleanupTiming, ProcessIdentity};
use crate::domain::{ExitReason, ProcessStatus, TaskEnv, TaskId, Workload};
use crate::home::Home;
use crate::machine::{MachineId, load_or_create_machine_id};
use crate::queue::spec::JobSpec;
use crate::queue::{
    JobEventKind, JobId, JobState, OperationId, ResourceId, ResourceName, RunPhase, StopCause,
};
use crate::store::Store;
use crate::store::queue::{JobRecord, NewJob};

static PHASE4_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

const WAIT: Duration = Duration::from_secs(25);

fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !condition() {
        assert!(Instant::now() < deadline, "queue condition did not settle");
        thread::sleep(Duration::from_millis(20));
    }
}

fn quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

struct Harness {
    _guard: std::sync::MutexGuard<'static, ()>,
    directory: TempDir,
    home: Home,
    machine: MachineId,
    daemon: Option<Child>,
    helpers: Vec<ProcessIdentity>,
}

impl Harness {
    fn new(threshold: &str) -> Self {
        crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
        let directory = tempfile::tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().join("state"))).unwrap();
        home.ensure().unwrap();
        let machine = load_or_create_machine_id(&home).unwrap();
        Store::open(&home.db_path()).unwrap();
        fs::write(
            directory.path().join("config.toml"),
            format!(
                "[resource.notify_blocked_after]\nyield = \"{threshold}\"\nwait = \"{threshold}\"\n"
            ),
        )
        .unwrap();
        let mut harness = Self {
            _guard: PHASE4_TEST_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            directory,
            home,
            machine,
            daemon: None,
            helpers: vec![],
        };
        harness.start();
        harness
    }

    fn store(&self) -> Store {
        Store::open(&self.home.db_path()).unwrap()
    }

    fn start(&mut self) {
        let log = fs::File::create(self.directory.path().join("daemon.log")).unwrap();
        let mut command = Command::new(assert_cmd::cargo::cargo_bin("homebased"));
        crate::run_env::scrub(&mut command);
        let child = command
            .args(["daemon", "serve", "--home"])
            .arg(self.home.root())
            .env(
                "HOMEBASED_CONFIG",
                self.directory.path().join("config.toml"),
            )
            .env("HOMEBASED_WEB_LISTEN", "off")
            .env(
                "HOMEBASED_CODEX",
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-codex"),
            )
            .env("FAKE_RECORD_DIR", self.directory.path())
            .env("HOME", self.directory.path())
            .env_remove("HOMEBASED_TASK_ID")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        self.daemon = Some(child);
        wait(|| {
            if !self.home.sock_path().exists() {
                return false;
            }
            Command::new(assert_cmd::cargo::cargo_bin("homebased"))
                .args(["--json", "daemon", "status", "--home"])
                .arg(self.home.root())
                .env(
                    "HOMEBASED_CONFIG",
                    self.directory.path().join("config.toml"),
                )
                .stdin(Stdio::null())
                .output()
                .is_ok_and(|output| output.status.success())
        });
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
        let _ = fs::remove_file(self.home.sock_path());
    }

    fn restart(&mut self) {
        self.stop();
        self.start();
    }

    fn resource(&self, name: &str) -> ResourceId {
        self.store()
            .resources_on(self.machine)
            .unwrap()
            .into_iter()
            .find(|record| record.resource.name.as_str() == name)
            .unwrap()
            .resource
            .id
    }

    fn submit(
        &self,
        priority: &str,
        preempt: Value,
        scripts: &[String],
        pin: Option<&str>,
    ) -> JobId {
        let steps: Vec<_> = scripts
            .iter()
            .map(|script| {
                let command = ["/bin/sh", "-c", &format!("set -eu; {script}")].map(String::from);
                json!({ "type": "task", "command": command })
            })
            .collect();
        let mut value = json!({
            "api_version": 1, "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "phase 4 command", "cwd": self.directory.path(), "timeout": "30m",
            "priority": priority, "preempt": preempt, "steps": steps,
        });
        if let Some(pin) = pin {
            value["resource"] = json!(pin);
        }
        let spec = JobSpec::parse_value(&value).unwrap();
        spec.check_on_authority(&std::env::var("PATH").unwrap())
            .unwrap();
        let id = JobId::new();
        self.store()
            .submit_job(&NewJob {
                id,
                machine: self.machine,
                origin: self.machine,
                spec,
                env: TaskEnv::capture(),
            })
            .unwrap();
        id
    }

    fn job(&self, id: JobId) -> JobRecord {
        self.store().job(id).unwrap().unwrap()
    }

    fn events(&self, id: JobId) -> Vec<JobEventKind> {
        self.store()
            .job_events(id)
            .unwrap()
            .iter()
            .map(|event| event.event)
            .collect()
    }

    fn task(&self, id: JobId) -> TaskId {
        self.store()
            .resources_on(self.machine)
            .unwrap()
            .iter()
            .filter_map(|resource| resource.run.as_ref())
            .find(|run| run.job == id)
            .unwrap()
            .task
    }

    fn executing(&self, id: JobId) {
        wait(|| {
            self.store()
                .resources_on(self.machine)
                .unwrap()
                .iter()
                .filter_map(|resource| resource.run.as_ref())
                .any(|run| run.job == id && matches!(run.phase, RunPhase::Executing { .. }))
        });
    }

    fn ended(&self, id: JobId, expected: &str) {
        wait(|| {
            self.job(id).state.as_str() == expected
                && !self
                    .store()
                    .resources_on(self.machine)
                    .unwrap()
                    .iter()
                    .filter_map(|resource| resource.run.as_ref())
                    .any(|run| run.job == id)
        });
    }

    fn cancel(&self, id: JobId) {
        self.store()
            .cancel_job(self.machine, OperationId::new(), id, Utc::now())
            .unwrap();
    }

    fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }

    fn helper(&mut self, task: TaskId) -> ProcessIdentity {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "cleanup::tests::helper_process",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("HOMEBASED_CLEANUP_TEST_HELPER", "sleep")
            .env(crate::run_env::TASK_ID, task.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let identity = cleanup::process_identity(Pid::from_raw(child.id() as i32)).unwrap();
        self.helpers.push(identity);
        thread::spawn(move || {
            let _ = child.wait();
        });
        identity
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if thread::panicking() {
            let evidence = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("_scratch/gpu-priority-queue/phase4");
            let _ = fs::create_dir_all(&evidence);
            let _ = fs::copy(
                self.path("daemon.log"),
                evidence.join(format!("failed-{}.log", self.machine)),
            );
            if let Ok(store) = Store::open(&self.home.db_path()) {
                let _ = fs::write(
                    evidence.join(format!("failed-{}-state.txt", self.machine)),
                    format!(
                        "resources={:?}\nqueue={:?}\ntasks={:?}\n",
                        store.resources_on(self.machine),
                        store.machine_queue(self.machine),
                        store.non_terminal()
                    ),
                );
            }
        }
        if let Ok(store) = Store::open(&self.home.db_path()) {
            if let Ok(jobs) = store.machine_queue(self.machine) {
                for job in jobs {
                    let _ = store.cancel_job(self.machine, OperationId::new(), job.id, Utc::now());
                    if let Ok(resources) = store.resources_on(self.machine) {
                        for run in resources
                            .iter()
                            .filter_map(|resource| resource.run.as_ref())
                            .filter(|run| run.job == job.id)
                        {
                            let _ = store.request_cancel(run.task);
                        }
                    }
                }
            }
            let deadline = Instant::now() + Duration::from_secs(12);
            while store
                .non_terminal()
                .map(|rows| !rows.is_empty())
                .unwrap_or(false)
                && Instant::now() < deadline
            {
                thread::sleep(Duration::from_millis(25));
            }
            if let Ok(rows) = store.non_terminal() {
                for row in rows {
                    if let Some(pid) = row.pid() {
                        let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
                    }
                    if let Some(child) = row.child {
                        let _ = cleanup::cleanup_lost_group(
                            child,
                            row.id,
                            &Default::default(),
                            CleanupTiming::STANDARD,
                        );
                    }
                }
            }
        }
        for identity in &self.helpers {
            if cleanup::process_identity(identity.pid).ok() == Some(*identity) {
                let _ = kill(identity.pid, Signal::SIGKILL);
            }
        }
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn script(value: &str) -> Vec<String> {
    vec![value.into()]
}
fn wait_mode() -> Value {
    json!({"mode": "wait"})
}

#[test]
fn launch_steps_failure_and_checkpoint_environment() {
    let h = Harness::new("30m");
    let trace = h.path("trace");
    let command = format!(
        "test \"$HOMEBASED_RESUME\" = 0; test -d \"$HOMEBASED_JOB_DIR\"; test \"$HOMEBASED_RESOURCE\" = gpu0; echo \"$HOMEBASED_STEP_INDEX:$HOMEBASED_RUN_NUMBER\" >> {}",
        quote(&trace)
    );
    let job = h.submit("low", wait_mode(), &[command.clone(), command], None);
    h.ended(job, "succeeded");
    assert_eq!(fs::read_to_string(trace).unwrap(), "0:1\n1:2\n");
    assert_eq!(h.events(job), vec![JobEventKind::JobSucceeded]);
    let fail = h.submit(
        "low",
        wait_mode(),
        &[
            "exit 9".into(),
            format!("touch {}", quote(&h.path("wrong"))),
        ],
        None,
    );
    h.ended(fail, "failed");
    assert_eq!(h.job(fail).runs, 1);
    assert!(!h.path("wrong").exists());
    assert_eq!(h.events(fail), vec![JobEventKind::JobFailed]);
}

#[test]
fn yield_resumes_same_directory_without_overlap() {
    let h = Harness::new("30m");
    let order = h.path("order");
    let directory = h.path("job-dir");
    let command = format!(
        r#"
        mkdir {lock} || exit 90
        trap 'rmdir {lock}' EXIT
        echo "low:$HOMEBASED_RESUME:$HOMEBASED_RUN_NUMBER" >> {order}
        if [ "$HOMEBASED_RESUME" = 1 ]; then
            test "$(cat {directory})" = "$HOMEBASED_JOB_DIR" || exit 91
            test ! -e "$HOMEBASED_YIELD_FILE" || exit 92
            exit 0
        fi
        echo "$HOMEBASED_JOB_DIR" > {directory}
        while [ ! -f "$HOMEBASED_YIELD_FILE" ]; do sleep 0.02; done
        exit 75
    "#,
        lock = quote(&h.path("exclusive")),
        order = quote(&order),
        directory = quote(&directory)
    );
    let low = h.submit("low", json!({"mode": "yield"}), &script(&command), None);
    h.executing(low);
    wait(|| directory.exists());
    let first = h.task(low);
    let high = h.submit(
        "high",
        wait_mode(),
        &script(&format!(
            "mkdir {lock} || exit 93; echo high >> {order}; rmdir {lock}",
            lock = quote(&h.path("exclusive")),
            order = quote(&order)
        )),
        None,
    );
    h.ended(high, "succeeded");
    h.ended(low, "succeeded");
    assert_eq!(
        h.events(low),
        vec![JobEventKind::JobPreempted, JobEventKind::JobSucceeded]
    );
    assert_eq!(
        h.store().require_task(first).unwrap().status(),
        ProcessStatus::Preempted
    );
    assert_eq!(
        fs::read_to_string(order).unwrap(),
        "low:0:1\nhigh\nlow:1:2\n"
    );
    assert_eq!(h.job(low).runs, 2);
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        fs::metadata(
            h.home
                .root()
                .join("jobs")
                .join(low.to_string())
                .join("state")
        )
        .unwrap()
        .permissions()
        .mode()
            & 0o777,
        0o700,
        "a host step's job directory is owner-only"
    );
}

#[test]
fn restart_and_wait_window_preserve_run_age() {
    let mut h = Harness::new("30m");
    let ready = h.path("ready");
    let body = format!(
        "echo \"$HOMEBASED_RESUME:$HOMEBASED_RUN_NUMBER\" >> {ready}; if [ \"$HOMEBASED_RUN_NUMBER\" = 1 ]; then while :; do sleep 0.02; done; fi",
        ready = quote(&ready)
    );
    let low = h.submit("low", json!({"mode":"restart"}), &script(&body), None);
    h.executing(low);
    wait(|| ready.exists());
    let first = h.task(low);
    let high = h.submit("high", wait_mode(), &script("exit 0"), None);
    h.ended(high, "succeeded");
    h.ended(low, "succeeded");
    assert_eq!(
        h.store().require_task(first).unwrap().status(),
        ProcessStatus::Preempted
    );
    assert_eq!(fs::read_to_string(&ready).unwrap(), "0:1\n0:2\n");

    let early = h.submit(
        "low",
        json!({"mode":"wait", "restart_within":"1m"}),
        &script("if [ \"$HOMEBASED_RUN_NUMBER\" = 1 ]; then while :; do sleep 0.02; done; fi"),
        None,
    );
    h.executing(early);
    let high = h.submit("high", wait_mode(), &script("exit 0"), None);
    h.ended(high, "succeeded");
    h.ended(early, "succeeded");
    assert!(h.events(early).contains(&JobEventKind::JobPreempted));

    // reserve with a historical start to exercise the bound without a minute's sleep
    h.stop();
    let old = h.submit(
        "low",
        json!({"mode":"wait", "restart_within":"1m"}),
        &script("while [ ! -e done ]; do sleep 0.02; done"),
        None,
    );
    let task = TaskId::new();
    let resource = h.resource("gpu0");
    h.store()
        .reserve_run(h.machine, old, resource, task, "/bin/sh".into(), Utc::now())
        .unwrap();
    let cp = h.store().run_checkpoint(task).unwrap().unwrap();
    cp.prepare(&h.home).unwrap();
    let lock = crate::runner::lock_before_spawn(&h.home.task_paths(task)).unwrap();
    let pid = crate::runner::spawn_task_run(&h.home, task, lock).unwrap();
    wait(|| h.store().require_task(task).unwrap().child.is_some());
    // keep this fixture's historical clock in the store, not in actor memory
    let connection = rusqlite::Connection::open(h.home.db_path()).unwrap();
    connection
        .execute(
            "UPDATE resources SET run_started_at = ?1 WHERE id = ?2",
            rusqlite::params![
                (Utc::now() - chrono::Duration::minutes(2)).to_rfc3339(),
                resource.to_string()
            ],
        )
        .unwrap();
    h.store().set_pid(task, pid as i32).unwrap();
    h.start();
    let high = h.submit("high", wait_mode(), &script("exit 0"), None);
    thread::sleep(Duration::from_millis(600));
    assert!(matches!(h.job(high).state, JobState::Queued { .. }));
    assert_eq!(
        h.store().require_task(task).unwrap().status(),
        ProcessStatus::Running
    );
    fs::write(h.path("done"), "").unwrap();
    h.ended(old, "succeeded");
    h.ended(high, "succeeded");
    assert_eq!(h.job(old).runs, 1);
    h.restart();
}

#[test]
fn cancel_active_and_queued() {
    let h = Harness::new("30m");
    let active = h.submit(
        "low",
        wait_mode(),
        &script("while :; do sleep 0.02; done"),
        None,
    );
    h.executing(active);
    let queued = h.submit("low", wait_mode(), &script("exit 0"), None);
    h.cancel(queued);
    h.cancel(active);
    h.ended(queued, "cancelled");
    h.ended(active, "cancelled");
    assert_eq!(h.job(queued).runs, 0);
    assert_eq!(h.events(queued), vec![JobEventKind::JobCancelled]);
    assert_eq!(h.events(active), vec![JobEventKind::JobCancelled]);
}

#[test]
fn two_resources_pinning_and_device_environment() {
    let h = Harness::new("30m");
    h.store()
        .register_resource(h.machine, ResourceName::parse("gpu1").unwrap(), Some(7))
        .unwrap();
    let low = h.submit(
        "low",
        wait_mode(),
        &script("while [ ! -e done ]; do sleep 0.02; done"),
        Some("gpu0"),
    );
    h.executing(low);
    let pin = h.submit("high", wait_mode(), &script("exit 0"), Some("gpu0"));
    let body = "test \"$HOMEBASED_RESOURCE\" = gpu1 && test \"$CUDA_VISIBLE_DEVICES\" = 7; while [ ! -e done ]; do sleep 0.02; done";
    let any = h.submit("medium", wait_mode(), &script(body), None);
    h.executing(any);
    assert!(matches!(h.job(pin).state, JobState::Queued { .. }));
    assert_eq!(
        h.store()
            .resources_on(h.machine)
            .unwrap()
            .iter()
            .filter(|resource| matches!(
                resource.run.as_ref().map(|run| &run.phase),
                Some(RunPhase::Executing { .. })
            ))
            .count(),
        2
    );
    fs::write(h.path("done"), "").unwrap();
    h.ended(low, "succeeded");
    h.ended(any, "succeeded");
    h.ended(pin, "succeeded");
}

#[test]
fn cleanup_kills_detached_helper_before_next_launch() {
    let mut h = Harness::new("30m");
    let pidfile = h.path("detached");
    let command = format!(
        "env HOMEBASED_CLEANUP_TEST_HELPER=escape HOMEBASED_CLEANUP_TEST_PIDFILE={} {} cleanup::tests::helper_process --exact --nocapture --test-threads=1",
        quote(&pidfile),
        quote(&std::env::current_exe().unwrap())
    );
    let job = h.submit("low", wait_mode(), &script(&command), None);
    wait(|| pidfile.exists());
    let pid: i32 = fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    if let Ok(identity) = cleanup::process_identity(Pid::from_raw(pid)) {
        h.helpers.push(identity);
    }
    let next = h.submit(
        "low",
        wait_mode(),
        &script(&format!("kill -0 {pid} 2>/dev/null && exit 95; exit 0")),
        None,
    );
    h.ended(job, "succeeded");
    h.ended(next, "succeeded");
    assert!(cleanup::process_identity(Pid::from_raw(pid)).is_err());
}

#[test]
fn attention_holds_lane_and_release_is_exact() {
    let mut h = Harness::new("30m");
    h.stop();
    h.store()
        .register_resource(h.machine, ResourceName::parse("gpu1").unwrap(), None)
        .unwrap();
    let job = h.submit("low", wait_mode(), &script("exit 0"), Some("gpu0"));
    let task = TaskId::new();
    let resource = h.resource("gpu0");
    h.store()
        .reserve_run(h.machine, job, resource, task, "/bin/sh".into(), Utc::now())
        .unwrap();
    h.store()
        .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    h.store()
        .cas_exit_with_evidence(
            task,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
            crate::domain::ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap();
    let marked = h.helper(task);
    // model another live task worker carrying the ended run's marker
    let protected_id = TaskId::new();
    let mut row = h.store().require_task(task).unwrap();
    row.id = protected_id;
    row.state = crate::domain::TaskState::Queued;
    row.workload = Workload::Task(crate::domain::TaskWorkload {
        command: crate::invocation::CommandLine::try_from_argv(vec!["/bin/true".into()]).unwrap(),
    });
    h.store().insert_task(&row).unwrap();
    h.store()
        .cas_status(protected_id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    h.store()
        .set_pid(protected_id, marked.pid.as_raw())
        .unwrap();
    let held_lock =
        crate::runner::lock_before_spawn(&h.home.prepare_task(protected_id).unwrap()).unwrap();
    // a worker remains protected between its terminal commit and lock release
    h.store()
        .cas_exit(protected_id, ProcessStatus::Running, &ExitReason::Cancelled)
        .unwrap();
    let pin = h.submit("high", wait_mode(), &script("exit 0"), Some("gpu0"));
    h.start();
    wait(|| {
        matches!(
            h.store()
                .resource(resource)
                .unwrap()
                .unwrap()
                .run
                .unwrap()
                .phase,
            RunPhase::Attention { .. }
        )
    });
    let RunPhase::Attention { id, .. } = h
        .store()
        .resource(resource)
        .unwrap()
        .unwrap()
        .run
        .unwrap()
        .phase
    else {
        panic!("attention missing")
    };
    assert_eq!(
        h.events(job),
        vec![JobEventKind::JobSucceeded, JobEventKind::JobAttention]
    );
    assert!(cleanup::process_identity(marked.pid).is_ok());
    let other = h.submit("medium", wait_mode(), &script("exit 0"), Some("gpu1"));
    h.ended(other, "succeeded");
    assert!(matches!(h.job(pin).state, JobState::Queued { .. }));
    kill(marked.pid, Signal::SIGKILL).unwrap();
    h.store()
        .cas_exit(protected_id, ProcessStatus::Running, &ExitReason::Cancelled)
        .unwrap();
    drop(held_lock);
    let operation = OperationId::new();
    h.store()
        .release_resource_attention(h.machine, operation, id)
        .unwrap();
    h.store()
        .release_resource_attention(h.machine, operation, id)
        .unwrap();
    h.ended(pin, "succeeded");
    assert!(
        h.store()
            .release_resource_attention(h.machine, OperationId::new(), id)
            .is_err()
    );
}

#[test]
fn notices_fire_once_per_episode_and_survive_restart() {
    let mut h = Harness::new("500ms");
    let low = h.submit(
        "low",
        wait_mode(),
        &script("while :; do sleep 0.02; done"),
        None,
    );
    h.executing(low);
    let high = h.submit("high", wait_mode(), &script("exit 0"), None);
    wait(|| h.store().blocked_episode(h.machine).unwrap().is_some());
    h.stop();
    thread::sleep(Duration::from_millis(600));
    h.start();
    wait(|| h.events(high).contains(&JobEventKind::JobBlocked));
    let events = h.store().job_events(high).unwrap();
    let notice = events[0].blocked.as_ref().unwrap();
    assert_eq!(notice.blockers.len(), 1);
    assert_eq!(
        notice.episode,
        h.store().blocked_episode(h.machine).unwrap().unwrap().id
    );
    h.restart();
    thread::sleep(Duration::from_millis(700));
    assert_eq!(h.events(high), vec![JobEventKind::JobBlocked]);
    assert_eq!(
        h.store().require_task(h.task(low)).unwrap().status(),
        ProcessStatus::Running
    );
    h.cancel(high);
    let another = h.submit("high", wait_mode(), &script("exit 0"), None);
    wait(|| h.events(another).contains(&JobEventKind::JobBlocked));
    h.cancel(low);
    h.ended(low, "cancelled");
    h.ended(another, "succeeded");
}

#[test]
fn recovery_executing_and_committed_stops() {
    let mut h = Harness::new("30m");
    let body = "if [ \"$HOMEBASED_RESUME\" = 1 ]; then exit 0; fi; while [ ! -f \"$HOMEBASED_YIELD_FILE\" ]; do sleep 0.02; done; exit 75";
    let low = h.submit("low", json!({"mode":"yield"}), &script(body), None);
    h.executing(low);
    let task = h.task(low);
    h.restart();
    h.executing(low);
    assert_eq!(h.task(low), task);
    h.stop();
    h.store()
        .commit_stop(h.resource("gpu0"), task, StopCause::Yield, Utc::now())
        .unwrap();
    assert!(!h.home.task_dir(task).join("control/yield").exists());
    let high = h.submit("high", wait_mode(), &script("exit 0"), None);
    h.start();
    h.ended(high, "succeeded");
    h.ended(low, "succeeded");
    assert_eq!(
        h.events(low),
        vec![JobEventKind::JobPreempted, JobEventKind::JobSucceeded]
    );

    let low = h.submit(
        "low",
        json!({"mode":"restart"}),
        &script("if [ \"$HOMEBASED_RUN_NUMBER\" = 1 ]; then while :; do sleep 0.02; done; fi"),
        None,
    );
    h.executing(low);
    let task = h.task(low);
    h.stop();
    h.store()
        .commit_stop(h.resource("gpu0"), task, StopCause::Restart, Utc::now())
        .unwrap();
    assert!(
        h.store()
            .require_task(task)
            .unwrap()
            .cancel_requested_at
            .is_none()
    );
    h.start();
    h.ended(low, "succeeded");
    assert_eq!(h.job(low).runs, 2);
}

#[test]
fn recovery_terminal_cleanup_and_abandoned_reservation() {
    let mut h = Harness::new("30m");
    h.stop();
    let job = h.submit("low", wait_mode(), &script("exit 0"), None);
    let task = TaskId::new();
    let resource = h.resource("gpu0");
    h.store()
        .reserve_run(h.machine, job, resource, task, "/bin/sh".into(), Utc::now())
        .unwrap();
    h.store()
        .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    h.store()
        .cas_exit_with_evidence(
            task,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
            crate::domain::ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap();
    assert!(matches!(
        h.store()
            .resource(resource)
            .unwrap()
            .unwrap()
            .run
            .unwrap()
            .phase,
        RunPhase::Cleaning { .. }
    ));
    h.store().begin_cleanup_attempt(resource, task).unwrap();
    h.start();
    h.ended(job, "succeeded");

    h.stop();
    let job = h.submit("low", wait_mode(), &script("exit 0"), None);
    let task = TaskId::new();
    h.store()
        .reserve_run(
            h.machine,
            job,
            resource,
            task,
            "/bin/sh".into(),
            Utc::now() - chrono::Duration::seconds(11),
        )
        .unwrap();
    h.start();
    h.ended(job, "succeeded");
    assert_eq!(
        h.store().require_task(task).unwrap().status(),
        ProcessStatus::Failed
    );
    assert_eq!(h.job(job).runs, 2);
    assert_eq!(h.events(job), vec![JobEventKind::JobSucceeded]);
}

#[test]
fn check_due_is_a_job_event_and_never_stops_the_run() {
    let mut h = Harness::new("30m");
    let job = h.submit(
        "low",
        wait_mode(),
        &script("while :; do sleep 0.02; done"),
        None,
    );
    h.executing(job);
    let task = h.task(job);
    h.stop();
    let connection = rusqlite::Connection::open(h.home.db_path()).unwrap();
    connection
        .execute(
            "UPDATE tasks SET created_at = ?1 WHERE id = ?2",
            rusqlite::params![
                (Utc::now() - chrono::Duration::minutes(31)).to_rfc3339(),
                task.to_string()
            ],
        )
        .unwrap();
    h.start();
    wait(|| h.events(job).contains(&JobEventKind::JobCheckDue));
    assert_eq!(
        h.store().require_task(task).unwrap().status(),
        ProcessStatus::Running
    );
    assert!(!h.store().is_event_task(task).unwrap());
    assert!(!h.store().produce_job_check_due(task).unwrap());
    h.restart();
    thread::sleep(Duration::from_millis(350));
    assert_eq!(h.events(job), vec![JobEventKind::JobCheckDue]);
    h.cancel(job);
    h.ended(job, "cancelled");
    assert_eq!(
        h.events(job),
        vec![JobEventKind::JobCheckDue, JobEventKind::JobCancelled]
    );
    assert!(!h.store().produce_job_check_due(task).unwrap());
}

#[test]
fn recovery_user_cancel_before_marker_and_lost_worker_children() {
    let mut h = Harness::new("30m");
    let job = h.submit(
        "low",
        wait_mode(),
        &script("while :; do sleep 0.02; done"),
        None,
    );
    h.executing(job);
    let task = h.task(job);
    h.stop();
    h.store()
        .cancel_job(h.machine, OperationId::new(), job, Utc::now())
        .unwrap();
    assert!(
        h.store()
            .require_task(task)
            .unwrap()
            .cancel_requested_at
            .is_none()
    );
    h.start();
    h.ended(job, "cancelled");
    assert_eq!(h.events(job), vec![JobEventKind::JobCancelled]);

    let pidfile = h.path("lost-children");
    let command = format!(
        "exec env HOMEBASED_CLEANUP_TEST_HELPER=group-leader HOMEBASED_CLEANUP_TEST_PIDFILE={} {} cleanup::tests::helper_process --exact --nocapture --test-threads=1",
        quote(&pidfile),
        quote(&std::env::current_exe().unwrap())
    );
    let job = h.submit("low", wait_mode(), &script(&command), None);
    h.executing(job);
    wait(|| pidfile.exists());
    let task = h.task(job);
    let row = h.store().require_task(task).unwrap();
    if let Some(identity) = row.child {
        h.helpers.push(identity);
    }
    let pid: i32 = fs::read_to_string(pidfile).unwrap().trim().parse().unwrap();
    if let Ok(identity) = cleanup::process_identity(Pid::from_raw(pid)) {
        h.helpers.push(identity);
    }
    kill(Pid::from_raw(row.pid().unwrap()), Signal::SIGKILL).unwrap();
    h.ended(job, "failed");
    assert_eq!(
        h.store().require_task(task).unwrap().status(),
        ProcessStatus::Lost
    );
    assert_eq!(h.events(job), vec![JobEventKind::JobFailed]);
    assert!(cleanup::process_identity(Pid::from_raw(pid)).is_err());
    let next = h.submit("low", wait_mode(), &script("exit 0"), None);
    h.ended(next, "succeeded");
}

#[test]
fn spawn_failure_serves_the_next_job() {
    let mut h = Harness::new("30m");
    h.stop();
    let job = h.submit("low", wait_mode(), &script("exit 0"), None);
    let task = TaskId::new();
    let resource = h.resource("gpu0");
    h.store()
        .reserve_run(h.machine, job, resource, task, "/bin/sh".into(), Utc::now())
        .unwrap();
    let checkpoint = h.store().run_checkpoint(task).unwrap().unwrap();
    checkpoint.prepare(&h.home).unwrap();
    // the accepted working directory can disappear before the worker spawns
    let missing = h.path("missing-cwd");
    let connection = rusqlite::Connection::open(h.home.db_path()).unwrap();
    connection
        .execute(
            "UPDATE tasks SET cwd = ?1 WHERE id = ?2",
            rusqlite::params![missing.display().to_string(), task.to_string()],
        )
        .unwrap();
    let lock = crate::runner::lock_before_spawn(&h.home.task_paths(task)).unwrap();
    crate::runner::spawn_task_run(&h.home, task, lock).unwrap();
    wait(|| h.store().require_task(task).unwrap().status().is_terminal());
    h.start();
    h.ended(job, "failed");
    assert_eq!(h.events(job), vec![JobEventKind::JobFailed]);
    let next = h.submit("low", wait_mode(), &script("exit 0"), None);
    h.ended(next, "succeeded");
}

#[test]
fn container_contract_uses_resource_and_fixed_mounts() {
    use crate::container::docker::{CreateContext, create_args};
    use crate::container::{ContainerUser, ContainerWorkload};
    let mut h = Harness::new("30m");
    h.stop();
    let resource = h.resource("gpu0");
    let job = h.submit("low", wait_mode(), &script("exit 0"), None);
    let task = TaskId::new();
    h.store()
        .reserve_run(h.machine, job, resource, task, "/bin/sh".into(), Utc::now())
        .unwrap();
    let mut checkpoint = h.store().run_checkpoint(task).unwrap().unwrap();
    checkpoint.resource.device = Some(3);
    checkpoint.step = crate::queue::checkpoint::StepKind::Container;
    checkpoint.prepare(&h.home).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(checkpoint.job_dir(&h.home))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o777, "a container user may have another UID");
    }
    checkpoint.request_yield(&h.home).unwrap();
    let workload = ContainerWorkload::from_value(
        &json!({"image":format!("sha256:{}", "0".repeat(64)), "memory":"1g"}),
    )
    .unwrap();
    let workload = checkpoint.container_workload(&h.home, &workload).unwrap();
    assert_eq!(workload.mounts[0].target, Path::new("/homebased/job"));
    assert!(!workload.mounts[0].read_only);
    assert_eq!(workload.mounts[1].target, Path::new("/homebased/run"));
    assert!(workload.mounts[1].read_only);
    let env: std::collections::BTreeMap<_, _> = workload
        .env
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    assert_eq!(env["HOMEBASED_JOB_DIR"], "/homebased/job");
    assert_eq!(env["HOMEBASED_YIELD_FILE"], "/homebased/run/yield");
    assert_eq!(env["CUDA_VISIBLE_DEVICES"], "0");
    assert!(
        checkpoint
            .host_environment(&h.home)
            .contains(&("CUDA_VISIBLE_DEVICES", "3".into()))
    );
    let paths = h.home.task_paths(task);
    let args = create_args(
        &workload,
        &CreateContext {
            task,
            cidfile: &paths.container_cid,
            default_user: ContainerUser::current(),
        },
    );
    assert!(
        args.windows(2)
            .any(|pair| pair == ["--gpus", "\"device=3\""])
    );
    checkpoint.resource.device = None;
    let workload = checkpoint
        .container_workload(
            &h.home,
            &ContainerWorkload::from_value(&json!({
                "image": format!("sha256:{}", "0".repeat(64)),
                "memory": "1g",
                "env": { "CUDA_VISIBLE_DEVICES": "99" },
            }))
            .unwrap(),
        )
        .unwrap();
    let args = create_args(
        &workload,
        &CreateContext {
            task,
            cidfile: &paths.container_cid,
            default_user: ContainerUser::current(),
        },
    );
    assert!(!args.iter().any(|arg| arg == "--gpus"));
    assert!(
        !workload
            .env
            .keys()
            .any(|key| key.as_str() == "CUDA_VISIBLE_DEVICES")
    );
    assert!(
        !checkpoint
            .host_environment(&h.home)
            .iter()
            .any(|(key, _)| *key == "CUDA_VISIBLE_DEVICES")
    );
    h.cancel(job);
    h.store().request_cancel(task).unwrap();
}

#[test]
fn two_yields_keep_distinct_tasks_and_control_files() {
    let h = Harness::new("30m");
    let command = "echo \"$HOMEBASED_TASK_ID\" >> attempts; if [ \"$HOMEBASED_RUN_NUMBER\" = 3 ]; then test \"$HOMEBASED_RESUME\" = 1; test ! -e \"$HOMEBASED_YIELD_FILE\"; exit 0; fi; while [ ! -f \"$HOMEBASED_YIELD_FILE\" ]; do sleep 0.02; done; exit 75";
    let low = h.submit("low", json!({"mode":"yield"}), &script(command), None);
    h.executing(low);
    let first = h.task(low);
    let high = h.submit("high", wait_mode(), &script("exit 0"), None);
    h.ended(high, "succeeded");
    wait(|| h.job(low).runs == 2);
    h.executing(low);
    let second = h.task(low);
    assert_ne!(first, second);
    assert!(h.home.task_dir(first).join("control/yield").exists());
    assert!(!h.home.task_dir(second).join("control/yield").exists());
    let high = h.submit("high", wait_mode(), &script("exit 0"), None);
    h.ended(high, "succeeded");
    h.ended(low, "succeeded");
    let ids = fs::read_to_string(h.path("attempts")).unwrap();
    assert_eq!(ids.lines().count(), 3);
    assert_eq!(
        ids.lines().collect::<std::collections::BTreeSet<_>>().len(),
        3
    );
    assert_eq!(
        h.events(low),
        vec![
            JobEventKind::JobPreempted,
            JobEventKind::JobPreempted,
            JobEventKind::JobSucceeded
        ]
    );
}

#[test]
fn moves_and_cancel_do_not_withdraw_a_committed_yield() {
    use crate::queue::{LevelEnd, Placement, Priority};
    let h = Harness::new("30m");
    let body = "if [ \"$HOMEBASED_RESUME\" = 1 ]; then exit 0; fi; while [ ! -f \"$HOMEBASED_YIELD_FILE\" ]; do sleep 0.02; done; touch saw-yield; while [ ! -e finish-yield ]; do sleep 0.02; done; exit 75";
    let low = h.submit("low", json!({"mode":"yield"}), &script(body), None);
    h.executing(low);
    // an equal-priority job cannot preempt until the move raises its level
    let head = h.submit("low", wait_mode(), &script("exit 0"), None);
    thread::sleep(Duration::from_millis(300));
    assert!(!h.path("saw-yield").exists());
    h.store()
        .move_job(
            h.machine,
            OperationId::new(),
            head,
            Placement::Edge {
                priority: Some(Priority::High),
                end: LevelEnd::Front,
            },
        )
        .unwrap();
    wait(|| h.path("saw-yield").exists());
    h.cancel(head);
    h.store()
        .move_job(
            h.machine,
            OperationId::new(),
            low,
            Placement::Edge {
                priority: Some(Priority::High),
                end: LevelEnd::Front,
            },
        )
        .unwrap();
    fs::write(h.path("finish-yield"), "").unwrap();
    h.ended(low, "succeeded");
    h.ended(head, "cancelled");
    assert_eq!(
        h.events(low),
        vec![JobEventKind::JobPreempted, JobEventKind::JobSucceeded]
    );
    assert_eq!(h.job(head).runs, 0);
}

#[test]
fn step_boundary_is_success_and_restart_only_repeats_current_step() {
    let h = Harness::new("30m");
    let low = h.submit("low", json!({"mode":"yield"}), &[
        "echo step0 >> order; while [ ! -e \"$HOMEBASED_YIELD_FILE\" ]; do sleep 0.02; done; exit 0".into(),
        "test \"$HOMEBASED_STEP_INDEX\" = 1; test \"$HOMEBASED_RESUME\" = 0; echo step1 >> order".into(),
    ], None);
    h.executing(low);
    let high = h.submit("high", wait_mode(), &script("echo high >> order"), None);
    h.ended(high, "succeeded");
    h.ended(low, "succeeded");
    assert_eq!(
        fs::read_to_string(h.path("order")).unwrap(),
        "step0\nhigh\nstep1\n"
    );
    assert_eq!(h.events(low), vec![JobEventKind::JobSucceeded]);

    let low = h.submit("low", json!({"mode":"restart"}), &[
        "echo completed >> later-steps".into(),
        "echo \"$HOMEBASED_STEP_INDEX:$HOMEBASED_RUN_NUMBER:$HOMEBASED_RESUME\" >> later-steps; if [ \"$HOMEBASED_RUN_NUMBER\" = 2 ]; then while :; do sleep 0.02; done; fi".into(),
    ], None);
    wait(|| h.job(low).runs == 2);
    h.executing(low);
    wait(|| fs::read_to_string(h.path("later-steps")).is_ok_and(|text| text.lines().count() == 2));
    let high = h.submit("high", wait_mode(), &script("exit 0"), None);
    h.ended(high, "succeeded");
    h.ended(low, "succeeded");
    assert_eq!(
        fs::read_to_string(h.path("later-steps")).unwrap(),
        "completed\n1:2:0\n1:3:0\n"
    );
    assert_eq!(
        h.events(low),
        vec![JobEventKind::JobPreempted, JobEventKind::JobSucceeded]
    );
}

#[test]
fn missing_executable_fails_once_and_does_not_block_the_queue() {
    use std::os::unix::fs::PermissionsExt;
    let mut h = Harness::new("30m");
    h.stop();
    let executable = h.path("entry");
    fs::write(&executable, "#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
    let spec = JobSpec::parse_value(&json!({
        "api_version": 1, "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431", "name": "removed executable",
        "cwd": h.directory.path(), "timeout": "30m", "priority":"low", "preempt":{"mode":"wait"},
        "workload":{"type":"task", "command":[executable]},
    })).unwrap();
    spec.check_on_authority(&TaskEnv::capture().path).unwrap();
    let id = JobId::new();
    h.store()
        .submit_job(&NewJob {
            id,
            machine: h.machine,
            origin: h.machine,
            spec,
            env: TaskEnv::capture(),
        })
        .unwrap();
    fs::remove_file(executable).unwrap();
    let next = h.submit("low", wait_mode(), &script("exit 0"), None);
    h.start();
    h.ended(id, "failed");
    h.ended(next, "succeeded");
    assert_eq!(h.job(id).runs, 1);
    assert_eq!(h.events(id), vec![JobEventKind::JobFailed]);
}

#[test]
fn unconfirmed_cleanup_waits_for_release_and_fresh_reservation_waits_for_bound() {
    let mut h = Harness::new("30m");
    h.stop();
    let resource = h.resource("gpu0");
    let job = h.submit("low", wait_mode(), &script("exit 0"), None);
    let task = TaskId::new();
    h.store()
        .reserve_run(h.machine, job, resource, task, "/bin/sh".into(), Utc::now())
        .unwrap();
    h.store()
        .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    h.store()
        .cas_exit(task, ProcessStatus::Running, &ExitReason::Exit { code: 0 })
        .unwrap();
    h.start();
    wait(|| {
        matches!(
            h.store()
                .resource(resource)
                .unwrap()
                .unwrap()
                .run
                .unwrap()
                .phase,
            RunPhase::Attention { .. }
        )
    });
    let RunPhase::Attention { id, failure } = h
        .store()
        .resource(resource)
        .unwrap()
        .unwrap()
        .run
        .unwrap()
        .phase
    else {
        panic!("attention missing")
    };
    assert_eq!(
        failure,
        crate::queue::CleanupFailure::ProcessGroupUnconfirmed
    );
    h.restart();
    thread::sleep(Duration::from_millis(350));
    assert_eq!(
        h.events(job),
        vec![JobEventKind::JobSucceeded, JobEventKind::JobAttention]
    );
    h.store()
        .release_resource_attention(h.machine, OperationId::new(), id)
        .unwrap();
    h.ended(job, "succeeded");

    h.stop();
    let job = h.submit("low", wait_mode(), &script("exit 0"), None);
    let task = TaskId::new();
    h.store()
        .reserve_run(h.machine, job, resource, task, "/bin/sh".into(), Utc::now())
        .unwrap();
    h.start();
    thread::sleep(Duration::from_millis(350));
    assert_eq!(
        h.store().require_task(task).unwrap().status(),
        ProcessStatus::Queued
    );
    assert_eq!(h.job(job).runs, 1);
    assert!(!h.home.task_paths(task).output.exists());
    let connection = rusqlite::Connection::open(h.home.db_path()).unwrap();
    connection
        .execute(
            "UPDATE resources SET run_reserved_at = ?1 WHERE id = ?2",
            rusqlite::params![
                (Utc::now() - chrono::Duration::seconds(11)).to_rfc3339(),
                resource.to_string()
            ],
        )
        .unwrap();
    h.ended(job, "succeeded");
    assert_eq!(h.job(job).runs, 2);
    assert_eq!(h.events(job), vec![JobEventKind::JobSucceeded]);
}
