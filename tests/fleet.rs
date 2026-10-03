//! Multi-daemon fleet foundation tests: real `daemon serve` processes with
//! separate state directories, probed by an in-process fleet runtime

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use homebased::domain::{ProcessStatus, TaskEnv, TaskId, ThreadId};
use homebased::events::EventPayload;
use homebased::fleet::address::MachineAddress;
use homebased::fleet::http::ClusterClient;
use homebased::fleet::probe::probe;
use homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
use homebased::fleet::protocol::SUPPORTED_PROTOCOLS;
use homebased::machine::MachineId;
use homebased::spec::NormalizedSpec;
use homebased::store::{NewTask, Store, new_queued_task};
use homebased::submission::{
    CallbackContext, ExecutionRecord, OriginRoute, RequestId, SubmissionState,
};
use serde_json::Value;
use tempfile::TempDir;

/// One `homebased daemon serve` process on a fixed loopback port
struct Daemon {
    _dir: TempDir,
    home: PathBuf,
    user_home: PathBuf,
    config: PathBuf,
    port: u16,
    listen_host: String,
    mdns: bool,
    codex_override: Option<PathBuf>,
    child: Option<Child>,
}

impl Daemon {
    fn start(name: &str, fleet: bool) -> Self {
        Self::start_on(name, fleet, false, "127.0.0.1")
    }

    fn start_on(name: &str, fleet: bool, mdns: bool, host: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let home = dir.path().join("state");
        let user_home = dir.path().join("user-home");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&user_home).unwrap();
        let config = dir.path().join("config.toml");
        let mut daemon = Self {
            _dir: dir,
            home,
            user_home,
            config,
            // the daemon binds port 0 and reports the port it got, so a concurrent
            // test cannot take the port between choosing it and binding it
            port: 0,
            listen_host: host.to_string(),
            mdns,
            // submit resolves `codex` for the callback even when no test reads
            // the callback, and CI hosts have no `codex`, so default to a no-op
            codex_override: Some(PathBuf::from("/usr/bin/true")),
            child: None,
        };
        daemon.write_config(name, fleet);
        daemon.spawn();
        daemon
    }

    fn write_config(&self, name: &str, fleet: bool) {
        let text = format!(
            "[fleet]\nenabled = {fleet}\nmachine_name = \"{name}\"\n\n[fleet.discovery]\nmdns = {}\n",
            self.mdns
        );
        fs::write(&self.config, text).unwrap();
    }

    fn cmd(&self) -> Command {
        let mut cmd = Command::new(assert_cmd::cargo::cargo_bin("homebased"));
        cmd.env("HOMEBASED_HOME", &self.home)
            .env("HOMEBASED_CONFIG", &self.config)
            .env("HOME", &self.user_home)
            .env_remove("CODEX_HOME")
            .env_remove("HOMEBASED_TASK_ID")
            .env_remove("HOMEBASED_WEB_LISTEN");
        if let Some(codex) = &self.codex_override {
            cmd.env("HOMEBASED_CODEX", codex);
        }
        cmd
    }

    fn spawn(&mut self) {
        let child = self
            .cmd()
            .args(["daemon", "serve", "--web-listen"])
            .arg(format!("{}:{}", self.listen_host, self.port))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        self.child = Some(child);
        let sock = self.home.join("homebased.sock");
        assert!(
            wait_until(Duration::from_secs(10), || sock.exists()),
            "socket did not appear"
        );
        let port = self.bound_port();
        assert!(
            self.port == 0 || self.port == port,
            "daemon bound port {port}, not the requested port {}",
            self.port
        );
        self.port = port;
    }

    /// Port of the dashboard listener, which the daemon omits when its bind failed
    fn bound_port(&self) -> u16 {
        let mut status = None;
        let answered = wait_until(Duration::from_secs(10), || {
            status = self
                .cmd()
                .args(["--json", "daemon", "status"])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| serde_json::from_slice::<Value>(&output.stdout).ok());
            // the socket file appears before the daemon serves it
            status
                .as_ref()
                .is_some_and(|status| status["socket"] == "up")
        });
        assert!(answered, "daemon did not start serving: {status:?}");
        let status = status.unwrap();
        let web = status["web"]
            .as_str()
            .unwrap_or_else(|| panic!("daemon has no dashboard listener: {status}"));
        web.rsplit(':')
            .next()
            .and_then(|port| port.trim_end_matches('/').parse().ok())
            .unwrap_or_else(|| panic!("dashboard URL has no port: {web}"))
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_file(self.home.join("homebased.sock"));
    }

    fn restart(&mut self) {
        self.stop();
        self.spawn();
    }

    fn address(&self) -> MachineAddress {
        format!("http://127.0.0.1:{}", self.port).parse().unwrap()
    }

    fn machine_id(&self) -> MachineId {
        fs::read_to_string(self.home.join("machine-id"))
            .unwrap()
            .parse()
            .unwrap()
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.stop();
    }
}

fn event_spec() -> NormalizedSpec {
    homebased::spec::parse_normalized_value(&serde_json::json!({
        "api_version": 1,
        "thread": ThreadId(uuid::Uuid::now_v7()),
        "name": "event test",
        "cwd": "/tmp",
        "timeout": "30m",
        "workload": {"type": "task", "command": ["echo", "hello"]}
    }))
    .unwrap()
}

fn seed_event(executor: &Daemon, origin: &Daemon, task: TaskId, route: bool, count: usize) {
    let spec = event_spec();
    if route {
        let mut store = Store::open(&origin.home.join(homebased::home::DB_NAME)).unwrap();
        store
            .insert_origin_route(&OriginRoute {
                request: RequestId::new(),
                task,
                origin_machine: origin.machine_id(),
                execution_machine: executor.machine_id(),
                thread: spec.thread,
                callback: CallbackContext {
                    env: TaskEnv {
                        path: "/bin".into(),
                        home: "/tmp".into(),
                    },
                    cwd: PathBuf::from("/tmp"),
                    codex: PathBuf::from("/bin/echo").into(),
                },
                spec: spec.clone(),
                submission: SubmissionState::AcceptanceUnknown,
                last_execution_state: None,
                last_updated_at: chrono::Utc::now(),
                last_accepted_seq: 0,
                last_settled_seq: 0,
            })
            .unwrap();
    }
    let mut store = Store::open(&executor.home.join(homebased::home::DB_NAME)).unwrap();
    store
        .accept_execution(&ExecutionRecord {
            task,
            origin_machine: origin.machine_id(),
            execution_machine: executor.machine_id(),
            spec,
            state: ProcessStatus::Queued,
        })
        .unwrap();
    for _ in 0..count {
        store
            .append_outbound_event(
                task,
                origin.machine_id(),
                executor.machine_id(),
                EventPayload::State {
                    status: ProcessStatus::Running,
                },
            )
            .unwrap();
    }
}

fn add_peer(executor: &Daemon, origin: &Daemon) {
    let result = executor
        .cmd()
        .args(["--json", "fleet", "add", &origin.address().to_string()])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let result = executor
        .cmd()
        .args(["--json", "fleet", "discover"])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

fn route_state(executor: &Daemon, task: TaskId) -> homebased::events::EventRouteStatus {
    Store::open(&executor.home.join(homebased::home::DB_NAME))
        .unwrap()
        .outbound_route_status(task)
        .unwrap()
        .unwrap()
}

fn remote_spec(executor: &Daemon, command: Vec<&str>) -> NormalizedSpec {
    homebased::spec::parse_normalized_value(&serde_json::json!({
        "api_version": 1,
        "thread": ThreadId(uuid::Uuid::now_v7()),
        "name": "remote executor test",
        "cwd": executor.user_home,
        "timeout": "30m",
        "workload": { "type": "task", "command": command }
    }))
    .unwrap()
}

async fn post_execution(
    executor: &Daemon,
    origin: &Daemon,
    task: TaskId,
    spec: &Value,
) -> (u16, Value) {
    let response = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions",
            &serde_json::json!({
                "api_version": 1,
                "protocol_version": CLUSTER_PROTOCOL_VERSION,
                "destination_machine": executor.machine_id(),
                "origin_machine": origin.machine_id(),
                "task": task,
                "spec": spec
            }),
        )
        .await
        .unwrap();
    let status = response.status.as_u16();
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    if status == 200 {
        assert_eq!(body["api_version"], 1);
        assert_eq!(body["protocol_version"], CLUSTER_PROTOCOL_VERSION.0);
    }
    (status, body)
}

async fn post_message(executor: &Daemon, request: &Value) -> (u16, Value) {
    let response = ClusterClient::default()
        .post_json(&executor.address(), "/v1/cluster/messages", request)
        .await
        .unwrap();
    (
        response.status.as_u16(),
        serde_json::from_slice(&response.body).unwrap(),
    )
}

fn seed_origin_route(
    origin: &Daemon,
    executor: &Daemon,
    task: TaskId,
    spec: &NormalizedSpec,
) -> RequestId {
    let request = RequestId::new();
    Store::open(&origin.home.join(homebased::home::DB_NAME))
        .unwrap()
        .insert_origin_route(&OriginRoute {
            request,
            task,
            origin_machine: origin.machine_id(),
            execution_machine: executor.machine_id(),
            thread: spec.thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin:/usr/bin".into(),
                    home: origin.user_home.to_string_lossy().into_owned(),
                },
                cwd: origin.user_home.clone(),
                codex: PathBuf::from("/bin/true").into(),
            },
            spec: spec.clone(),
            submission: SubmissionState::AcceptanceUnknown,
            last_execution_state: None,
            last_updated_at: chrono::Utc::now(),
            last_accepted_seq: 0,
            last_settled_seq: 0,
        })
        .unwrap();
    request
}

/// Give `thread` a Codex session file under `user_home` so the submitting CLI
/// accepts it as a callback thread
fn register_thread(user_home: &Path, thread: &str) {
    let day = user_home.join(".codex/sessions/2026/01/01");
    fs::create_dir_all(&day).unwrap();
    fs::write(
        day.join(format!("rollout-2026-01-01T00-00-00-{thread}.jsonl")),
        "",
    )
    .unwrap();
}

fn submit_file(daemon: &Daemon, spec: &NormalizedSpec, request: RequestId) -> std::process::Output {
    let path = daemon
        ._dir
        .path()
        .join(format!("submit-{}.json", request.0));
    fs::write(&path, serde_json::to_vec(spec).unwrap()).unwrap();
    submit_command(daemon, &path, request).output().unwrap()
}

fn submit_command(daemon: &Daemon, path: &Path, request: RequestId) -> Command {
    let spec: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    register_thread(&daemon.user_home, spec["thread"].as_str().unwrap());
    let mut command = daemon.cmd();
    command
        .current_dir(&daemon.user_home)
        .args(["--json", "task", "submit", "--spec"])
        .arg(path)
        .args(["--request-id", &request.0.to_string()]);
    command
}

fn cli_remote_spec(executor: &Daemon, command: Vec<&str>) -> NormalizedSpec {
    let mut spec = remote_spec(executor, command);
    spec.machine = Some("remote-executor".parse().unwrap());
    spec
}

fn wait_until(budget: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < budget {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(40));
    }
    pred()
}

async fn wait_for_probe(address: &MachineAddress) {
    let client = ClusterClient::default();
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if probe(&client, address, SUPPORTED_PROTOCOLS).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{address} never answered the machine probe");
}

fn inspect_cli(daemon: &Daemon, command: &str, task: TaskId) -> (bool, Value) {
    let output = daemon
        .cmd()
        .args(["--json", "task", command, &task.to_string()])
        .output()
        .unwrap();
    let body = if output.status.success() {
        &output.stdout
    } else {
        &output.stderr
    };
    (
        output.status.success(),
        serde_json::from_slice(body).unwrap(),
    )
}

fn seed_inspection_row(daemon: &Daemon, task: TaskId) {
    let spec = remote_spec(daemon, vec!["/bin/echo", "saved-output"]);
    let row = new_queued_task(NewTask {
        id: task,
        name: spec.name.clone(),
        thread: spec.thread,
        workload: homebased::invocation::persist_workload(&spec.workload),
        cwd: spec.cwd,
        timeout: spec.timeout,
        env: TaskEnv {
            path: "/bin".into(),
            home: daemon.user_home.to_string_lossy().into_owned(),
        },
        binary: PathBuf::from("/bin/echo"),
    });
    Store::open(&daemon.home.join(homebased::home::DB_NAME))
        .unwrap()
        .insert_task(&row)
        .unwrap();
    let evidence = daemon.home.join("tasks").join(task.to_string());
    fs::create_dir_all(&evidence).unwrap();
    fs::write(evidence.join("output.log"), "saved-output\n").unwrap();
}

/// Origin and executor daemons that know each other, with callbacks recorded on the origin
fn dependency_fleet(name: &str) -> (Daemon, Daemon) {
    use std::os::unix::fs::PermissionsExt;

    let mut origin = Daemon::start(&format!("{name}-origin"), true);
    let executor = Daemon::start("remote-executor", true);
    let codex = origin.user_home.join("codex");
    fs::write(
        &codex,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/callbacks\"\n",
    )
    .unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    origin.codex_override = Some(codex);
    origin.restart();
    add_peer(&origin, &executor);
    add_peer(&executor, &origin);
    (origin, executor)
}

/// Submit a spec with `after` through the origin CLI and return the response
fn submit_after(daemon: &Daemon, spec: &NormalizedSpec, after: &[TaskId]) -> Value {
    let mut value = serde_json::to_value(spec).unwrap();
    value["after"] = serde_json::json!(after);
    let request = RequestId::new();
    let path = daemon
        ._dir
        .path()
        .join(format!("submit-{}.json", request.0));
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    let output = submit_command(daemon, &path, request).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn gate_command(gate: &Path) -> String {
    format!("while [ ! -f '{}' ]; do sleep 0.1; done", gate.display())
}

#[path = "fleet/cancellation.rs"]
mod cancellation;
#[path = "fleet/dependencies.rs"]
mod dependencies;
#[path = "fleet/events.rs"]
mod events;
#[path = "fleet/inspection.rs"]
mod inspection;
#[path = "fleet/messages.rs"]
mod messages;
#[path = "fleet/peers.rs"]
mod peers;
#[path = "fleet/resource_jobs.rs"]
mod resource_jobs;
#[path = "fleet/submission.rs"]
mod submission;
