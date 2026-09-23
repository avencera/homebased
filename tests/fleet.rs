//! Multi-daemon fleet foundation tests: real `daemon serve` processes with
//! separate state directories, probed by an in-process fleet runtime.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use homebased::cancellation::{CancellationRequestIdentity, ExecutorCancelState};
use homebased::config::{Discovery, FleetSettings};
use homebased::domain::{ProcessStatus, TaskEnv, TaskId, ThreadId};
use homebased::events::{EventPayload, EventRouteState};
use homebased::fleet::address::MachineAddress;
use homebased::fleet::directory::LocalMachine;
use homebased::fleet::http::ClusterClient;
use homebased::fleet::identity::IdentityStatus;
use homebased::fleet::probe::{ProbeError, probe};
use homebased::fleet::protocol::SUPPORTED_PROTOCOLS;
use homebased::fleet::runtime::{FleetHandle, FleetRuntime, FleetStart, RuntimeTimings};
use homebased::machine::{BootId, LocalIdentity, MachineId, MachineName};
use homebased::message::MessageId;
use homebased::resource::{CommandSpec, ResourceId, ResourceQueueRequest};
use homebased::spec::NormalizedSpec;
use homebased::store::{NewTask, Store, new_queued_task};
use homebased::submission::{
    CallbackContext, ExecutionRecord, NewResourceRoute, OriginRoute, RequestId,
    ResourceQueueReceipt, ResourceRoutePhase, SubmissionState,
};
use serde_json::Value;
use tempfile::TempDir;

/// One `homebased daemon serve` process on a fixed loopback port.
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
            port: free_port(),
            listen_host: host.to_string(),
            mdns,
            codex_override: None,
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

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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
        let mut store = Store::open(&origin.home.join("homebased.sqlite")).unwrap();
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
                spec: spec.clone().into(),
                submission: SubmissionState::AcceptanceUnknown,
                last_execution_state: None,
                last_updated_at: Some(chrono::Utc::now()),
                last_accepted_seq: 0,
                last_settled_seq: 0,
            })
            .unwrap();
    }
    let mut store = Store::open(&executor.home.join("homebased.sqlite")).unwrap();
    store
        .accept_execution(&ExecutionRecord {
            task,
            origin_machine: origin.machine_id(),
            execution_machine: executor.machine_id(),
            spec: spec.into(),
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
    Store::open(&executor.home.join("homebased.sqlite"))
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
                "protocol_version": 1,
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
        assert_eq!(body["protocol_version"], 1);
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
    Store::open(&origin.home.join("homebased.sqlite"))
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
            spec: spec.clone().into(),
            submission: SubmissionState::AcceptanceUnknown,
            last_execution_state: None,
            last_updated_at: Some(chrono::Utc::now()),
            last_accepted_seq: 0,
            last_settled_seq: 0,
        })
        .unwrap();
    request
}

fn seed_resource_route(
    origin: &Daemon,
    authority: &Daemon,
    resource: ResourceId,
    request: RequestId,
    task: TaskId,
    spec: &NormalizedSpec,
) -> OriginRoute {
    let route = OriginRoute::new_resource_waiting(NewResourceRoute {
        request,
        task,
        origin_machine: origin.machine_id(),
        authority_machine: authority.machine_id(),
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
        resource,
    })
    .unwrap();
    Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .insert_origin_route(&route)
        .unwrap();
    route
}

fn seed_authority_resource(authority: &Daemon, resource: ResourceId) {
    let supervisor_thread = ThreadId(uuid::Uuid::now_v7());
    let connection = rusqlite::Connection::open(authority.home.join("homebased.sqlite")).unwrap();
    connection
        .execute(
            "INSERT INTO resources (
                id, display_name, authority_machine, supervisor_machine, supervisor_thread,
                assignment_revision, state_revision, registered_background_task
             ) VALUES (?1, 'test-gpu', ?2, ?2, ?3, 0, 0, NULL)",
            rusqlite::params![
                resource.as_uuid().to_string(),
                authority.machine_id().as_uuid().to_string(),
                supervisor_thread.to_string(),
            ],
        )
        .unwrap();
}

async fn post_resource_queue(
    authority: &Daemon,
    origin: &Daemon,
    route: &OriginRoute,
) -> (u16, Value) {
    let spec = CommandSpec::try_from(route.current_spec().unwrap().clone()).unwrap();
    let request = ResourceQueueRequest::new(
        homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION.0,
        authority.machine_id(),
        origin.machine_id(),
        route.request,
        route.task,
        match &route.submission {
            SubmissionState::Resource { resource, .. } => *resource,
            _ => panic!("resource fixture must retain resource identity"),
        },
        spec,
    );
    let response = ClusterClient::default()
        .post_json(
            &authority.address(),
            "/v1/cluster/resource-requests",
            &request,
        )
        .await
        .unwrap();
    (
        response.status.as_u16(),
        serde_json::from_slice(&response.body).unwrap(),
    )
}

async fn post_resource_cancel_wire(
    authority: &Daemon,
    identity: &homebased::cancellation::ResourceCancellationRequestIdentity,
) -> (u16, Value) {
    let response = ClusterClient::default()
        .post_json(
            &authority.address(),
            "/v1/cluster/resource-requests/cancel",
            &serde_json::json!({
                "api_version": 1,
                "protocol_version": homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION.0,
                "destination_machine": authority.machine_id(),
                "request": identity,
            }),
        )
        .await
        .unwrap();
    (
        response.status.as_u16(),
        serde_json::from_slice(&response.body).unwrap(),
    )
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

#[test]
fn origin_socket_submission_retries_and_keeps_callbacks_local() {
    use std::os::unix::fs::PermissionsExt;

    let mut origin = Daemon::start("remote-origin", true);
    let executor = Daemon::start("remote-executor", true);
    let codex = origin.user_home.join("codex");
    fs::write(
        &codex,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/callbacks\"\n",
    )
    .unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    origin.codex_override = Some(codex.clone());
    origin.restart();
    add_peer(&origin, &executor);
    add_peer(&executor, &origin);

    let spec = cli_remote_spec(
        &executor,
        vec!["/bin/sh", "-c", "echo remote-run >> \"$HOME/remote-runs\""],
    );
    let request = RequestId::new();
    let first = submit_file(&origin, &spec, request);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let body: Value = serde_json::from_slice(&first.stdout).unwrap();
    let task: TaskId = body["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(body["task_id"], body["id"]);
    assert_eq!(body["request_id"], request.0.to_string());
    let second = submit_file(&origin, &spec, request);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let retry: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(retry["id"], body["id"]);
    let route = Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .origin_route_by_request(request)
        .unwrap()
        .unwrap();
    assert_eq!(route.task, task);
    assert_eq!(route.callback.cwd, origin.user_home.canonicalize().unwrap());
    assert_eq!(route.callback.codex.path(), Some(codex.as_path()));
    assert!(matches!(route.submission, SubmissionState::Accepted));
    assert!(
        Store::open(&origin.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_none()
    );
    assert!(wait_until(Duration::from_secs(10), || origin
        .user_home
        .join("callbacks")
        .exists()));
    assert!(executor.user_home.join("remote-runs").exists());
    assert!(!executor.user_home.join("callbacks").exists());

    let mut changed = spec.clone();
    changed.cwd = executor.user_home.join("different-content");
    let conflict = submit_file(&origin, &changed, request);
    assert!(!conflict.status.success());
    let error: Value = serde_json::from_slice(&conflict.stderr).unwrap();
    assert_eq!(error["error"]["code"], "submission_conflict");
    assert_eq!(error["error"]["input"]["task_id"], task.to_string());
}

#[test]
fn concurrent_remote_submissions_with_one_request_uuid_share_the_first_task() {
    let origin = Daemon::start("remote-origin", true);
    let executor = Daemon::start("remote-executor", true);
    add_peer(&origin, &executor);
    add_peer(&executor, &origin);

    let spec = cli_remote_spec(&executor, vec!["/bin/echo", "one-task"]);
    let request = RequestId::new();
    let first_path = origin._dir.path().join("submit-concurrent-first.json");
    let second_path = origin._dir.path().join("submit-concurrent-second.json");
    let contents = serde_json::to_vec(&spec).unwrap();
    fs::write(&first_path, &contents).unwrap();
    fs::write(&second_path, contents).unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let first_barrier = barrier.clone();
    let mut first_command = submit_command(&origin, &first_path, request);
    let first = std::thread::spawn(move || {
        first_barrier.wait();
        first_command.output().unwrap()
    });
    let second_barrier = barrier.clone();
    let mut second_command = submit_command(&origin, &second_path, request);
    let second = std::thread::spawn(move || {
        second_barrier.wait();
        second_command.output().unwrap()
    });
    barrier.wait();

    let first = first.join().unwrap();
    let second = second.join().unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let first: Value = serde_json::from_slice(&first.stdout).unwrap();
    let second: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(first["id"], second["id"]);
    assert_eq!(first["request_id"], request.0.to_string());
    assert_eq!(second["request_id"], request.0.to_string());

    let task: TaskId = first["id"].as_str().unwrap().parse().unwrap();
    let origin_store = Store::open(&origin.home.join("homebased.sqlite")).unwrap();
    let route = origin_store
        .origin_route_by_request(request)
        .unwrap()
        .unwrap();
    assert_eq!(route.task, task);
    assert!(matches!(route.submission, SubmissionState::Accepted));
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .executor_identity(task)
            .unwrap()
            .is_some()
    );
}

#[test]
fn explicit_local_machine_name_is_rejected_as_a_remote_selector() {
    let daemon = Daemon::start("solo", false);
    let mut spec = remote_spec(&daemon, vec!["/bin/echo", "local"]);
    spec.machine = Some("solo".parse().unwrap());
    let request = RequestId::new();

    let output = submit_file(&daemon, &spec, request);
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "invalid_spec");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("omit machine")
    );
    assert!(
        Store::open(&daemon.home.join("homebased.sqlite"))
            .unwrap()
            .list_tasks(&[], None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn unknown_origin_route_resolves_by_identity_or_abandon() {
    let mut origin = Daemon::start("remote-origin", true);
    let mut executor = Daemon::start("remote-executor", true);
    add_peer(&origin, &executor);
    let mut spec = cli_remote_spec(&executor, vec!["/bin/echo", "hello"]);
    let accepted_task = TaskId::new();
    let accepted_request = seed_origin_route(&origin, &executor, accepted_task, &spec);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (status, body) = post_execution(
            &executor,
            &origin,
            accepted_task,
            &serde_json::to_value(&spec).unwrap(),
        )
        .await;
        assert_eq!(status, 200, "{body}");
    });
    let retry = submit_file(&origin, &spec, accepted_request);
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&retry.stdout).unwrap()["id"],
        accepted_task.to_string()
    );

    let absent_task = TaskId::new();
    let absent_request = seed_origin_route(&origin, &executor, absent_task, &spec);
    executor.stop();
    origin.restart();
    let offline = submit_file(&origin, &spec, absent_request);
    assert!(!offline.status.success());
    let error: Value = serde_json::from_slice(&offline.stderr).unwrap();
    assert_eq!(error["error"]["code"], "submission_outcome_unknown");
    assert_eq!(error["error"]["input"]["task_id"], absent_task.to_string());
    assert!(matches!(
        Store::open(&origin.home.join("homebased.sqlite"))
            .unwrap()
            .origin_route_by_task(absent_task)
            .unwrap()
            .unwrap()
            .submission,
        SubmissionState::AcceptanceUnknown
    ));
    executor.restart();
    let abandoned = submit_file(&origin, &spec, absent_request);
    assert!(!abandoned.status.success());
    let error: Value = serde_json::from_slice(&abandoned.stderr).unwrap();
    assert_eq!(error["error"]["code"], "submission_rejected");
    assert_eq!(
        error["error"]["input"]["reason"],
        "abandoned_before_acceptance"
    );
    runtime.block_on(async {
        let (status, body) = post_execution(
            &executor,
            &origin,
            absent_task,
            &serde_json::to_value(&spec).unwrap(),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["identity"]["reason"], "abandoned_before_acceptance");
    });

    spec.cwd = executor.user_home.join("not-present");
    let before = RequestId::new();
    executor.stop();
    let unavailable = submit_file(&origin, &spec, before);
    assert!(!unavailable.status.success());
    let error: Value = serde_json::from_slice(&unavailable.stderr).unwrap();
    assert_eq!(error["error"]["code"], "machine_unavailable");
    assert_eq!(error["error"]["retryable"], true);
    assert!(
        Store::open(&origin.home.join("homebased.sqlite"))
            .unwrap()
            .origin_route_by_request(before)
            .unwrap()
            .is_none()
    );
}

#[test]
fn remote_dry_run_expands_executor_home_without_identity() {
    let origin = Daemon::start("remote-origin", true);
    let executor = Daemon::start("remote-executor", true);
    add_peer(&origin, &executor);
    let mut spec = cli_remote_spec(&executor, vec!["/bin/echo", "hello"]);
    spec.cwd = PathBuf::from("~/");
    let path = origin._dir.path().join("dry-run.json");
    fs::write(&path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let output = origin
        .cmd()
        .current_dir(&origin.user_home)
        .args(["--json", "task", "submit", "--spec"])
        .arg(&path)
        .arg("--dry-run")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        body["execution_cwd"]
            .as_str()
            .unwrap()
            .trim_end_matches('/'),
        executor.user_home.to_string_lossy().as_ref()
    );
    assert_eq!(body["argv"], serde_json::json!(["/bin/echo", "hello"]));
    spec.cwd = PathBuf::from("~/not-present");
    fs::write(&path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let missing_cwd = origin
        .cmd()
        .current_dir(&origin.user_home)
        .args(["--json", "task", "submit", "--spec"])
        .arg(&path)
        .arg("--dry-run")
        .output()
        .unwrap();
    assert!(!missing_cwd.status.success());
    let error: Value = serde_json::from_slice(&missing_cwd.stderr).unwrap();
    assert_eq!(error["error"]["code"], "cwd_not_found");
    spec.cwd = PathBuf::from("~/");
    spec.workload = remote_spec(&executor, vec!["/missing-executor-command"]).workload;
    fs::write(&path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let missing_program = origin
        .cmd()
        .current_dir(&origin.user_home)
        .args(["--json", "task", "submit", "--spec"])
        .arg(&path)
        .arg("--dry-run")
        .output()
        .unwrap();
    assert!(!missing_program.status.success());
    let error: Value = serde_json::from_slice(&missing_program.stderr).unwrap();
    assert_eq!(error["error"]["code"], "executable_missing");
    assert!(
        Store::open(&origin.home.join("homebased.sqlite"))
            .unwrap()
            .list_tasks(&[], None)
            .unwrap()
            .is_empty()
    );
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .list_tasks(&[], None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn dropped_local_socket_response_reuses_the_allocated_task() {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    let mut origin = Daemon::start("remote-origin", true);
    let executor = Daemon::start("remote-executor", true);
    origin.codex_override = Some(PathBuf::from("/usr/bin/true"));
    origin.restart();
    add_peer(&origin, &executor);
    add_peer(&executor, &origin);
    let spec = cli_remote_spec(&executor, vec!["/bin/echo", "one-run"]);
    let request = RequestId::new();
    let body = serde_json::to_vec(&serde_json::json!({
        "spec": spec,
        "env": TaskEnv::capture(),
        "callback_cwd": origin.user_home,
        "request_id": request,
    }))
    .unwrap();
    let mut socket = UnixStream::connect(origin.home.join("homebased.sock")).unwrap();
    write!(socket, "POST /v1/tasks HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", body.len()).unwrap();
    socket.write_all(&body).unwrap();
    let routed = wait_until(Duration::from_secs(5), || {
        Store::open(&origin.home.join("homebased.sqlite"))
            .unwrap()
            .origin_route_by_request(request)
            .unwrap()
            .is_some()
    });
    if !routed {
        socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut response = String::new();
        let _ = socket.read_to_string(&mut response);
        panic!("socket request made no route: {response}");
    }
    let task = Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .origin_route_by_request(request)
        .unwrap()
        .unwrap()
        .task;
    assert!(wait_until(Duration::from_secs(5), || Store::open(
        &executor.home.join("homebased.sqlite")
    )
    .unwrap()
    .executor_identity(task)
    .unwrap()
    .is_some()));
    drop(socket);
    let retry = submit_file(&origin, &spec, request);
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let body: Value = serde_json::from_slice(&retry.stdout).unwrap();
    let returned: TaskId = body["id"].as_str().unwrap().parse().unwrap();
    let route = Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .origin_route_by_request(request)
        .unwrap()
        .unwrap();
    assert_eq!(route.task, returned);
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .executor_identity(task)
            .unwrap()
            .is_some()
    );
}

#[test]
fn restart_resolves_a_route_saved_before_send() {
    let mut origin = Daemon::start("remote-origin", true);
    let executor = Daemon::start("remote-executor", true);
    add_peer(&origin, &executor);
    let spec = cli_remote_spec(&executor, vec!["/bin/echo", "never-start"]);
    let task = TaskId::new();
    let request = seed_origin_route(&origin, &executor, task, &spec);
    origin.restart();
    assert!(wait_until(Duration::from_secs(10), || {
        matches!(
            Store::open(&origin.home.join("homebased.sqlite"))
                .unwrap()
                .origin_route_by_request(request)
                .unwrap()
                .unwrap()
                .submission,
            SubmissionState::Rejected { .. }
        )
    }));
    let identity = Store::open(&executor.home.join("homebased.sqlite"))
        .unwrap()
        .executor_identity(task)
        .unwrap()
        .unwrap();
    assert!(
        matches!(identity, homebased::submission::ExecutorIdentity::Rejected(tombstone)
        if tombstone.reason == "abandoned_before_acceptance")
    );
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_none()
    );
}

#[test]
fn cli_rejects_an_invalid_request_uuid() {
    let daemon = Daemon::start("remote-origin", true);
    let output = daemon
        .cmd()
        .args([
            "task",
            "submit",
            "--spec",
            "missing.json",
            "--request-id",
            "not-a-uuid",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid value"));
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_accept_retry_conflict_and_events_use_executor_only() {
    let origin = Daemon::start("remote-origin", true);
    let mut executor = Daemon::start("remote-executor", true);
    let fake_codex = executor.user_home.join("codex");
    fs::write(
        &fake_codex,
        "#!/bin/sh\necho called >> \"$HOME/local-callback\"\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    executor.codex_override = Some(fake_codex);
    executor.restart();
    wait_for_probe(&executor.address()).await;
    add_peer(&executor, &origin);
    let task = TaskId::new();
    let spec = remote_spec(
        &executor,
        vec![
            "/bin/sh",
            "-c",
            "echo run >> \"$HOME/count\"; echo remote-output",
        ],
    );
    seed_origin_route(&origin, &executor, task, &spec);
    let value = serde_json::to_value(&spec).unwrap();

    let (status, first) = post_execution(&executor, &origin, task, &value).await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(first["identity"]["type"], "accepted");
    let (status, duplicate) = post_execution(&executor, &origin, task, &value).await;
    assert_eq!(status, 200, "{duplicate}");
    assert_eq!(duplicate["identity"]["type"], "accepted");
    assert!(wait_until(Duration::from_secs(10), || {
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_some_and(|row| row.status() == ProcessStatus::Succeeded)
    }));
    assert!(wait_until(Duration::from_secs(12), || route_state(
        &executor, task
    )
    .acknowledged
        >= 3));
    assert_eq!(
        fs::read_to_string(executor.user_home.join("count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(
        fs::read_to_string(
            executor
                .home
                .join("tasks")
                .join(task.to_string())
                .join("output.log")
        )
        .unwrap()
        .contains("remote-output")
    );
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .origin_route_by_task(task)
            .unwrap()
            .is_none()
    );
    assert!(!executor.user_home.join("local-callback").exists());
    assert_eq!(
        Store::open(&origin.home.join("homebased.sqlite"))
            .unwrap()
            .inbound_events(task)
            .unwrap()
            .len(),
        3
    );

    let mut changed = value.clone();
    changed["workload"]["command"][2] = serde_json::json!("echo changed");
    let (status, _) = post_execution(&executor, &origin, task, &changed).await;
    assert_eq!(status, 409);
    let (status, _) = post_execution(&executor, &executor, task, &value).await;
    assert_eq!(status, 409);
    assert_eq!(
        fs::read_to_string(executor.user_home.join("count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_destination_abandon_and_invalid_requests_do_not_launch() {
    let origin = Daemon::start("reject-origin", true);
    let executor = Daemon::start("reject-executor", true);
    wait_for_probe(&executor.address()).await;
    let legacy_probe = ClusterClient::default()
        .get(&executor.address(), "/v1/cluster/machine")
        .await
        .unwrap();
    assert_eq!(legacy_probe.status.as_u16(), 200);
    let advertisement: Value = serde_json::from_slice(&legacy_probe.body).unwrap();
    assert_eq!(advertisement["api_version"], 1);

    let task = TaskId::new();
    let spec = serde_json::to_value(remote_spec(&executor, vec!["/bin/echo", "never"])).unwrap();
    let wrong = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions",
            &serde_json::json!({
                "api_version": 1, "protocol_version": 1, "destination_machine": origin.machine_id(),
                "origin_machine": origin.machine_id(), "task": task, "spec": spec
            }),
        )
        .await
        .unwrap();
    assert_eq!(wrong.status.as_u16(), 409);
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .executor_identity(task)
            .unwrap()
            .is_none()
    );

    let incompatible = TaskId::new();
    let response = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions",
            &serde_json::json!({
                "api_version": 1, "protocol_version": 99, "destination_machine": executor.machine_id(),
                "origin_machine": origin.machine_id(), "task": incompatible, "spec": spec
            }),
        )
        .await
        .unwrap();
    assert_ne!(response.status.as_u16(), 200);
    let error: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(error["api_version"], 1);
    assert_eq!(error["error"]["code"], "cluster_protocol_incompatible");
    assert_eq!(error["error"]["input"]["remote"]["min"], 99);
    assert_eq!(error["error"]["input"]["remote"]["max"], 99);
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .executor_identity(incompatible)
            .unwrap()
            .is_none()
    );

    for api_version in [Some(99), None] {
        let task = TaskId::new();
        let mut request = serde_json::json!({
            "protocol_version": 1,
            "destination_machine": executor.machine_id(),
            "origin_machine": origin.machine_id(),
            "task": task,
            "spec": spec
        });
        if let Some(api_version) = api_version {
            request["api_version"] = serde_json::json!(api_version);
        }
        let response = ClusterClient::default()
            .post_json(&executor.address(), "/v1/cluster/executions", &request)
            .await
            .unwrap();
        assert_eq!(response.status.as_u16(), 400);
        let error: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error["api_version"], 1);
        assert_eq!(error["error"]["code"], "usage");
        assert!(
            Store::open(&executor.home.join("homebased.sqlite"))
                .unwrap()
                .executor_identity(task)
                .unwrap()
                .is_none()
        );
    }

    let same_owner = TaskId::new();
    let (status, body) = post_execution(&executor, &executor, same_owner, &spec).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["identity"]["reason"], "origin_equals_executor");

    let abandon = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions/abandon",
            &serde_json::json!({
                "api_version": 1, "protocol_version": 1, "destination_machine": executor.machine_id(),
                "origin_machine": origin.machine_id(), "task": task
            }),
        )
        .await
        .unwrap();
    assert_eq!(abandon.status.as_u16(), 200);
    let abandon_body: Value = serde_json::from_slice(&abandon.body).unwrap();
    assert_eq!(abandon_body["api_version"], 1);
    assert_eq!(abandon_body["protocol_version"], 1);
    let (status, body) = post_execution(&executor, &origin, task, &spec).await;
    assert_eq!(status, 200);
    assert_eq!(body["identity"]["type"], "rejected");
    assert_eq!(body["identity"]["reason"], "abandoned_before_acceptance");
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_none()
    );

    let invalid = TaskId::new();
    let mut bad = spec;
    bad["workload"]["prompt_file"] = serde_json::json!("/origin/only");
    let (status, body) = post_execution(&executor, &origin, invalid, &bad).await;
    assert_eq!(status, 200);
    assert_eq!(body["identity"]["reason"], "invalid_spec");
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(invalid)
            .unwrap()
            .is_none()
    );

    let leaked_context = TaskId::new();
    let response = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions",
            &serde_json::json!({
                "api_version": 1, "protocol_version": 1, "destination_machine": executor.machine_id(),
                "origin_machine": origin.machine_id(), "task": leaked_context,
                "spec": remote_spec(&executor, vec!["/bin/echo", "never"]),
                "callback_context": { "cwd": "/origin-only" }
            }),
        )
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["identity"]["reason"], "invalid_request_fields");

    let missing_binary = TaskId::new();
    let unavailable = serde_json::to_value(remote_spec(
        &executor,
        vec!["missing-homebased-test-binary"],
    ))
    .unwrap();
    let (status, body) = post_execution(&executor, &origin, missing_binary, &unavailable).await;
    assert_eq!(status, 503, "{body}");
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .executor_identity(missing_binary)
            .unwrap()
            .is_none()
    );

    let home_task = TaskId::new();
    let mut home_spec =
        serde_json::to_value(remote_spec(&executor, vec!["/bin/echo", "home"])).unwrap();
    home_spec["cwd"] = serde_json::json!("~/");
    let (status, accepted) = post_execution(&executor, &origin, home_task, &home_spec).await;
    assert_eq!(status, 200, "{accepted}");
    assert_eq!(accepted["identity"]["spec"]["cwd"], "~/", "{accepted}");
    assert_eq!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .require_task(home_task)
            .unwrap()
            .cwd,
        executor.user_home
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn accepted_queued_remote_task_launches_after_restart() {
    let origin = Daemon::start("restart-origin", true);
    let mut executor = Daemon::start("restart-executor", true);
    wait_for_probe(&executor.address()).await;
    let task = TaskId::new();
    let spec = remote_spec(
        &executor,
        vec!["/bin/sh", "-c", "echo once >> \"$HOME/restarted\""],
    );
    executor.stop();
    fs::create_dir_all(executor.home.join("tasks").join(task.to_string())).unwrap();
    let row = new_queued_task(NewTask {
        id: task,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: homebased::invocation::persist_workload(&spec.workload),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: TaskEnv {
            path: "/bin:/usr/bin".into(),
            home: executor.user_home.to_string_lossy().into_owned(),
        },
        binary: PathBuf::from("/bin/sh"),
    });
    Store::open(&executor.home.join("homebased.sqlite"))
        .unwrap()
        .insert_remote_task(&row, &spec, origin.machine_id(), executor.machine_id())
        .unwrap();
    executor.restart();
    assert!(wait_until(Duration::from_secs(10), || {
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_some_and(|row| row.status() == ProcessStatus::Succeeded)
    }));
    let (status, body) = post_execution(
        &executor,
        &origin,
        task,
        &serde_json::to_value(spec).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        fs::read_to_string(executor.user_home.join("restarted"))
            .unwrap()
            .lines()
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_acceptance_and_abandon_have_one_winner() {
    let origin = Daemon::start("race-origin", true);
    let executor = Daemon::start("race-executor", true);
    wait_for_probe(&executor.address()).await;
    let task = TaskId::new();
    let spec = serde_json::to_value(remote_spec(
        &executor,
        vec!["/bin/sh", "-c", "echo run >> \"$HOME/race-count\""],
    ))
    .unwrap();
    let client = ClusterClient::default();
    let address = executor.address();
    let abandon_body = serde_json::json!({
        "api_version": 1, "protocol_version": 1, "destination_machine": executor.machine_id(),
        "origin_machine": origin.machine_id(), "task": task
    });
    let abandon = client.post_json(&address, "/v1/cluster/executions/abandon", &abandon_body);
    let submit = post_execution(&executor, &origin, task, &spec);
    let (abandon, submit) = tokio::join!(abandon, submit);
    let abandoned: Value = serde_json::from_slice(&abandon.unwrap().body).unwrap();
    let (status, submitted) = submit;
    assert_eq!(status, 200, "{submitted}");
    assert_eq!(abandoned["identity"]["type"], submitted["identity"]["type"]);
    let saved = Store::open(&executor.home.join("homebased.sqlite")).unwrap();
    if submitted["identity"]["type"] == "rejected" {
        assert!(saved.get_task(task).unwrap().is_none());
        assert!(!executor.user_home.join("race-count").exists());
    } else {
        assert!(saved.get_task(task).unwrap().is_some());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lost_remote_response_retries_without_second_child() {
    let origin = Daemon::start("lost-origin", true);
    let executor = Daemon::start("lost-executor", true);
    wait_for_probe(&executor.address()).await;
    let task = TaskId::new();
    let spec = serde_json::to_value(remote_spec(
        &executor,
        vec!["/bin/sh", "-c", "echo run >> \"$HOME/lost-count\""],
    ))
    .unwrap();
    let response = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions",
            &serde_json::json!({
                "api_version": 1, "protocol_version": 1, "destination_machine": executor.machine_id(),
                "origin_machine": origin.machine_id(), "task": task, "spec": spec
            }),
        )
        .await
        .unwrap();
    drop(response);
    let (status, retry) = post_execution(&executor, &origin, task, &spec).await;
    assert_eq!(status, 200, "{retry}");
    assert_eq!(retry["identity"]["type"], "accepted");
    assert!(wait_until(Duration::from_secs(10), || executor
        .user_home
        .join("lost-count")
        .exists()));
    assert_eq!(
        fs::read_to_string(executor.user_home.join("lost-count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sender_uses_real_receiver_and_recovers_a_sequence_gap() {
    let origin = Daemon::start("origin", true);
    let executor = Daemon::start("executor", true);
    wait_for_probe(&origin.address()).await;
    let task = TaskId::new();
    seed_event(&executor, &origin, task, true, 2);
    let store = Store::open(&executor.home.join("homebased.sqlite")).unwrap();
    store
        .mark_outbound_acknowledged(task, std::num::NonZeroU64::new(1).unwrap())
        .unwrap();
    add_peer(&executor, &origin);
    assert!(wait_until(Duration::from_secs(12), || {
        route_state(&executor, task).acknowledged == 2
    }));
    let inbox = Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .inbound_events(task)
        .unwrap();
    assert_eq!(
        inbox
            .iter()
            .map(|row| row.event.seq.get())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        route_state(&executor, task).state,
        EventRouteState::Acknowledged
    );
    let mut store = Store::open(&executor.home.join("homebased.sqlite")).unwrap();
    store
        .append_outbound_event(
            task,
            origin.machine_id(),
            executor.machine_id(),
            EventPayload::State {
                status: ProcessStatus::Succeeded,
            },
        )
        .unwrap();
    assert!(wait_until(Duration::from_secs(12), || route_state(
        &executor, task
    )
    .acknowledged
        == 3));
}

#[tokio::test(flavor = "multi_thread")]
async fn sender_retries_after_lost_response_and_origin_restart() {
    let mut origin = Daemon::start("origin", true);
    let executor = Daemon::start("executor", true);
    wait_for_probe(&origin.address()).await;
    let task = TaskId::new();
    seed_event(&executor, &origin, task, true, 1);
    let event = Store::open(&executor.home.join("homebased.sqlite"))
        .unwrap()
        .pending_outbound_events(task)
        .unwrap()
        .remove(0)
        .event;
    let client = ClusterClient::default();
    let response = client
        .post_json(
            &origin.address(),
            "/v1/cluster/events",
            &serde_json::json!({
                "api_version": 1, "protocol_version": 1,
                "destination_machine": origin.machine_id(), "event": event
            }),
        )
        .await
        .unwrap();
    assert_eq!(response.status.as_u16(), 200);
    let acknowledgement: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(acknowledgement["api_version"], 1);
    assert_eq!(acknowledgement["protocol_version"], 1);
    origin.stop();
    add_peer(&executor, &origin);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(route_state(&executor, task).state, EventRouteState::Pending);
    origin.restart();
    wait_for_probe(&origin.address()).await;
    assert!(wait_until(Duration::from_secs(12), || route_state(
        &executor, task
    )
    .acknowledged
        == 1));
    assert_eq!(
        Store::open(&origin.home.join("homebased.sqlite"))
            .unwrap()
            .inbound_events(task)
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn verified_route_not_found_orphans_without_discarding_events() {
    let origin = Daemon::start("origin", true);
    let executor = Daemon::start("executor", true);
    wait_for_probe(&origin.address()).await;
    let task = TaskId::new();
    seed_event(&executor, &origin, task, false, 1);
    add_peer(&executor, &origin);
    assert!(wait_until(Duration::from_secs(12), || route_state(
        &executor, task
    )
    .state
        == EventRouteState::Orphaned));
    let mut store = Store::open(&executor.home.join("homebased.sqlite")).unwrap();
    store
        .append_outbound_event(
            task,
            origin.machine_id(),
            executor.machine_id(),
            EventPayload::State {
                status: ProcessStatus::Succeeded,
            },
        )
        .unwrap();
    std::thread::sleep(Duration::from_secs(2));
    let state = route_state(&executor, task);
    assert_eq!(state.pending, 2);
    assert_eq!(state.reason.as_deref(), Some("route_not_found"));
    assert_eq!(state.state, EventRouteState::Orphaned);
    let socket = homebased::client::Client::new(executor.home.join("homebased.sock"));
    let detail = socket
        .get(&format!("/v1/fleet/executor/tasks/{task}/events"))
        .await
        .unwrap();
    assert_eq!(detail["state"], "orphaned");
    assert_eq!(detail["pending"], 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_origin_address_does_not_orphan_or_redirect_events() {
    let mut origin = Daemon::start("origin", true);
    let executor = Daemon::start("executor", true);
    wait_for_probe(&origin.address()).await;
    add_peer(&executor, &origin);
    let task = TaskId::new();
    origin.stop();
    let mut replacement = Daemon::start("replacement", true);
    replacement.stop();
    replacement.port = origin.port;
    replacement.spawn();
    wait_for_probe(&replacement.address()).await;
    seed_event(&executor, &origin, task, true, 1);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(route_state(&executor, task).state, EventRouteState::Pending);
    assert_eq!(
        Store::open(&replacement.home.join("homebased.sqlite"))
            .unwrap()
            .inbound_events(task)
            .unwrap()
            .len(),
        0
    );
    replacement.stop();
    origin.restart();
    wait_for_probe(&origin.address()).await;
    assert!(wait_until(Duration::from_secs(12), || route_state(
        &executor, task
    )
    .acknowledged
        == 1));
    assert_eq!(
        route_state(&executor, task).state,
        EventRouteState::Acknowledged
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn local_origin_uses_the_durable_inbox() {
    let daemon = Daemon::start("local", false);
    let task = TaskId::new();
    seed_event(&daemon, &daemon, task, true, 1);
    assert!(wait_until(Duration::from_secs(5), || route_state(
        &daemon, task
    )
    .acknowledged
        == 1));
    let store = Store::open(&daemon.home.join("homebased.sqlite")).unwrap();
    assert_eq!(store.inbound_events(task).unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn new_local_reports_and_exit_use_sequenced_callbacks_across_restart() {
    use std::os::unix::fs::PermissionsExt;

    let mut daemon = Daemon::start("local-producer", false);
    let fake_bin = daemon._dir.path().join("fake-bin");
    fs::create_dir(&fake_bin).unwrap();
    let codex = fake_bin.join("codex");
    fs::write(
        &codex,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/callbacks\"\n",
    )
    .unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    daemon.codex_override = Some(codex);
    daemon.restart();
    let spec_path = daemon._dir.path().join("submit.json");
    fs::write(
        &spec_path,
        serde_json::to_vec(&serde_json::json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "local producer",
            "cwd": daemon.user_home,
            "timeout": "30m",
        "workload": { "type": "task", "command": ["/bin/sh", "-c", "while [ ! -f go ]; do sleep 0.05; done"] }
        }))
        .unwrap(),
    )
    .unwrap();
    assert!(wait_until(Duration::from_secs(5), || daemon
        .cmd()
        .args(["--json", "daemon", "status"])
        .output()
        .is_ok_and(|output| output.status.success())));
    let output = daemon
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .env("PATH", format!("{}:/bin:/usr/bin", fake_bin.display()))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    let task: TaskId = response["id"].as_str().unwrap().parse().unwrap();
    let callbacks = daemon.user_home.join("callbacks");
    assert!(wait_until(Duration::from_secs(5), || Store::open(
        &daemon.home.join("homebased.sqlite")
    )
    .is_ok_and(|store| store
        .get_task(task)
        .unwrap()
        .is_some_and(|row| row.status() == ProcessStatus::Running))));
    for (summary, notify) in [("silent", false), ("notified", true)] {
        let mut command = daemon.cmd();
        command.args([
            "--json",
            "task",
            "report",
            "--id",
            &task.to_string(),
            "--outcome",
            "succeeded",
            "--summary",
            summary,
        ]);
        if notify {
            command.arg("--notify");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(wait_until(Duration::from_secs(5), || fs::read_to_string(
        &callbacks
    )
    .is_ok_and(|text| text.lines().count() == 1)));
    fs::write(daemon.user_home.join("go"), "").unwrap();
    let settled = wait_until(Duration::from_secs(10), || {
        let Ok(store) = Store::open(&daemon.home.join("homebased.sqlite")) else {
            return false;
        };
        store
            .origin_route_by_task(task)
            .unwrap()
            .is_some_and(|route| route.last_settled_seq == 5)
            && fs::read_to_string(&callbacks).is_ok_and(|text| text.lines().count() == 2)
    });
    if !settled {
        let store = Store::open(&daemon.home.join("homebased.sqlite")).unwrap();
        panic!(
            "task={:?} route={:?} outbox={:?} inbox={:?} callbacks={:?}",
            store.get_task(task).unwrap(),
            store
                .origin_route_by_task(task)
                .unwrap()
                .map(|route| (route.last_accepted_seq, route.last_settled_seq)),
            store.pending_outbound_events(task).unwrap(),
            store.inbound_events(task).unwrap(),
            fs::read_to_string(&callbacks)
        );
    }
    let before = fs::read_to_string(&callbacks).unwrap();
    assert!(before.contains("\"seq\":4"));
    assert!(before.contains("\"seq\":5"));
    assert!(!before.contains("\"seq\":3"));
    daemon.restart();
    assert!(wait_until(Duration::from_secs(5), || daemon
        .cmd()
        .args(["--json", "daemon", "status"])
        .output()
        .is_ok_and(|output| output.status.success())));
    std::thread::sleep(Duration::from_millis(1200));
    assert_eq!(fs::read_to_string(&callbacks).unwrap(), before);
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

/// In-process observer with its own identity and peer file. Its background
/// tasks end with the test's tokio runtime.
struct Observer {
    _dir: TempDir,
    _runtime: FleetRuntime,
    handle: FleetHandle,
}

impl Observer {
    fn start(configured: Vec<MachineAddress>) -> Self {
        Self::start_with(configured, false)
    }

    fn start_with(configured: Vec<MachineAddress>, mdns: bool) -> Self {
        let dir = TempDir::new().unwrap();
        let local = LocalMachine {
            identity: LocalIdentity {
                machine: MachineId::new(),
                boot: BootId::new(),
            },
            name: MachineName::parse("observer").unwrap(),
            protocol: SUPPORTED_PROTOCOLS,
        };
        let runtime = FleetRuntime::start(FleetStart {
            local,
            settings: FleetSettings {
                discovery: Discovery {
                    mdns,
                    tailscale: None,
                },
                machines: configured,
            },
            listener: None,
            peers_path: dir.path().join("fleet-peers.json"),
            timings: RuntimeTimings {
                round_interval: Duration::from_secs(3600),
                recheck_delay: Duration::from_millis(300),
                ..RuntimeTimings::default()
            },
        })
        .unwrap();
        let handle = runtime.handle();
        Self {
            _dir: dir,
            _runtime: runtime,
            handle,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_peers_survive_restart_and_rename_without_conflict() {
    let mut alpha = Daemon::start("alpha", true);
    let beta = Daemon::start("beta", true);
    wait_for_probe(&alpha.address()).await;
    wait_for_probe(&beta.address()).await;
    let observer = Observer::start(vec![alpha.address(), beta.address()]);

    let report = observer.handle.discover_now().await;
    assert_eq!(report.answered, 2, "{report:?}");
    assert!(report.duplicates.is_empty(), "{report:?}");
    let peers = observer.handle.peers().await;
    let names: Vec<&str> = peers.iter().map(|peer| peer.name.as_str()).collect();
    assert_eq!(names, vec!["alpha", "beta"]);
    let alpha_id = alpha.machine_id();
    let verified = observer.handle.connect(alpha_id).await.unwrap();
    assert_eq!(verified.machine, alpha_id);
    assert_eq!(verified.address, alpha.address());
    let first_boot = verified.boot;

    // ordinary restart: same machine UUID, new boot UUID, no conflict
    alpha.restart();
    wait_for_probe(&alpha.address()).await;
    let report = observer.handle.discover_now().await;
    assert!(report.duplicates.is_empty(), "{report:?}");
    let verified = observer.handle.connect(alpha_id).await.unwrap();
    assert_ne!(verified.boot, first_boot);
    assert_eq!(alpha.machine_id(), alpha_id);

    // rename across a restart updates the same machine record
    alpha.write_config("gamma", true);
    alpha.restart();
    wait_for_probe(&alpha.address()).await;
    let report = observer.handle.discover_now().await;
    assert!(report.duplicates.is_empty(), "{report:?}");
    let peer = observer
        .handle
        .peers()
        .await
        .into_iter()
        .find(|peer| peer.machine == alpha_id)
        .unwrap();
    assert_eq!(peer.name.as_str(), "gamma");
    assert_eq!(peer.identity, IdentityStatus::Consistent);
    assert_eq!(observer.handle.peers().await.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn cloned_state_directory_is_a_duplicate_identity() {
    let alpha = Daemon::start("alpha", true);
    wait_for_probe(&alpha.address()).await;
    let alpha_id = alpha.machine_id();

    // a clone copies the source installation's machine UUID
    let mut clone = Daemon::start("alpha-copy", true);
    clone.stop();
    fs::copy(alpha.home.join("machine-id"), clone.home.join("machine-id")).unwrap();
    clone.spawn();
    wait_for_probe(&clone.address()).await;
    assert_eq!(clone.machine_id(), alpha_id);

    let observer = Observer::start(vec![alpha.address(), clone.address()]);
    let report = observer.handle.discover_now().await;
    assert_eq!(report.duplicates, vec![alpha_id], "{report:?}");
    let err = observer.handle.connect(alpha_id).await.unwrap_err();
    assert_eq!(err.code(), "duplicate_machine_identity");

    // once the clone is gone, a later round clears the conflict
    clone.stop();
    let report = observer.handle.discover_now().await;
    assert!(report.duplicates.is_empty(), "{report:?}");
    assert!(observer.handle.connect(alpha_id).await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_address_is_never_used_for_another_installation() {
    let mut alpha = Daemon::start("alpha", true);
    wait_for_probe(&alpha.address()).await;
    let alpha_id = alpha.machine_id();
    let observer = Observer::start(Vec::new());
    observer.handle.add_explicit(alpha.address()).await;
    observer.handle.discover_now().await;
    observer.handle.connect(alpha_id).await.unwrap();

    // another installation takes over the port
    alpha.stop();
    let mut other = Daemon::start("other", true);
    other.stop();
    other.port = alpha.port;
    other.spawn();
    wait_for_probe(&other.address()).await;

    let err = observer.handle.connect(alpha_id).await.unwrap_err();
    assert_eq!(err.code(), "machine_unavailable", "{err}");
    let other_id = other.machine_id();
    assert_ne!(other_id, alpha_id);
    // the address moved to the installation that answered; the alpha record stays
    let verified = observer.handle.connect(other_id).await.unwrap();
    assert_eq!(verified.address, alpha.address());
    assert!(
        observer
            .handle
            .peers()
            .await
            .iter()
            .any(|peer| peer.machine == alpha_id && peer.addresses.is_empty())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fleet_disabled_daemon_has_no_cluster_routes() {
    let daemon = Daemon::start("solo", false);
    let client = ClusterClient::default();
    let address = daemon.address();
    let start = Instant::now();
    let result = loop {
        let result = probe(&client, &address, SUPPORTED_PROTOCOLS).await;
        if !matches!(result, Err(ProbeError::Transport(_))) || start.elapsed().as_secs() > 10 {
            break result;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        matches!(result, Err(ProbeError::Status { status, .. }) if status == 404),
        "{result:?}"
    );
    // machine identity still exists for local task ownership
    assert!(daemon.home.join("machine-id").is_file());
}

fn validate(config: &Path) -> (i32, Value) {
    let output = Command::new(assert_cmd::cargo::cargo_bin("homebased"))
        .args(["--json", "config", "validate"])
        .env("HOMEBASED_CONFIG", config)
        .output()
        .unwrap();
    let stream = if output.status.success() {
        &output.stdout
    } else {
        &output.stderr
    };
    (
        output.status.code().unwrap(),
        serde_json::from_slice(stream).unwrap(),
    )
}

#[test]
fn config_validate_reports_fleet_and_rejects_bad_files() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(
        &path,
        "[fleet]\nenabled = true\nmachine_name = \"code\"\n[[fleet.machines]]\naddress = \"http://main:7677\"\n",
    )
    .unwrap();
    let (code, body) = validate(&path);
    assert_eq!(code, 0, "{body}");
    assert_eq!(body["valid"], true);
    assert_eq!(body["source"], "explicit");
    assert_eq!(body["machine_name"], "code");
    assert_eq!(body["fleet"]["state"], "enabled");
    assert_eq!(body["fleet"]["machines"][0], "http://main:7677");

    fs::write(&path, "[fleet]\nenabled = yes\n").unwrap();
    let (code, body) = validate(&path);
    assert_eq!(code, 2, "{body}");
    assert_eq!(body["error"]["code"], "config_invalid");

    let (code, body) = validate(&dir.path().join("missing.toml"));
    assert_eq!(code, 2, "{body}");
    assert_eq!(body["error"]["code"], "config_invalid");
}

/// Uses real multicast on the host network, so it is opt-in:
/// `cargo test --test fleet -- --ignored mdns`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "uses host multicast networking"]
async fn mdns_discovers_a_lan_peer() {
    let daemon = Daemon::start_on("lanpeer", true, true, "0.0.0.0");
    let local: MachineAddress = format!("http://127.0.0.1:{}", daemon.port).parse().unwrap();
    wait_for_probe(&local).await;
    let machine = daemon.machine_id();
    let observer = Observer::start_with(Vec::new(), true);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(20) {
        observer.handle.discover_now().await;
        if let Some(peer) = observer
            .handle
            .peers()
            .await
            .into_iter()
            .find(|peer| peer.machine == machine)
        {
            assert!(
                peer.addresses.iter().any(|ranked| ranked.source
                    == homebased::fleet::address::AddressSource::Lan
                    || ranked.source == homebased::fleet::address::AddressSource::Tailscale),
                "{peer:?}"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("mDNS never reported the peer");
}

#[tokio::test(flavor = "multi_thread")]
async fn fleet_cli_and_cluster_reads_keep_machine_boundaries() {
    let alpha = Daemon::start("alpha", true);
    let mut beta = Daemon::start("beta", true);
    wait_for_probe(&alpha.address()).await;
    wait_for_probe(&beta.address()).await;

    let output = alpha
        .cmd()
        .args(["--json", "fleet", "machines"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let inventory: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        inventory["local"]["machine"],
        alpha.machine_id().to_string()
    );
    assert!(inventory["machines"].as_array().unwrap().is_empty());

    let output = alpha
        .cmd()
        .args(["--json", "fleet", "add", &beta.address().to_string()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = alpha
        .cmd()
        .args(["--json", "fleet", "discover"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let discovered: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        discovered["machines"][0]["machine"],
        beta.machine_id().to_string()
    );
    let output = alpha
        .cmd()
        .args(["--json", "fleet", "probe", "beta"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let probed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(probed["machine"]["machine"], beta.machine_id().to_string());

    let client = ClusterClient::default();
    let task = homebased::domain::TaskId::new();
    let wrong = format!(
        "/v1/cluster/tasks/{task}?api_version=1&destination_machine={}",
        alpha.machine_id()
    );
    let response = client.get(&beta.address(), &wrong).await.unwrap();
    assert_eq!(response.status.as_u16(), 409);
    let right = format!(
        "/v1/cluster/tasks/{task}?api_version=1&destination_machine={}",
        beta.machine_id()
    );
    let response = client.get(&beta.address(), &right).await.unwrap();
    assert_eq!(response.status.as_u16(), 404);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert!(body["execution"].is_null());

    let socket = homebased::client::Client::new(alpha.home.join("homebased.sock"));
    let lookup = socket
        .get(&format!("/v1/fleet/tasks/{task}"))
        .await
        .unwrap();
    assert_eq!(lookup["result"], "not_found");
    let beta_id = beta.machine_id();
    beta.stop();
    let lookup = socket
        .get(&format!("/v1/fleet/tasks/{task}"))
        .await
        .unwrap();
    assert_eq!(lookup["result"], "incomplete");
    assert_eq!(lookup["unchecked"][0]["machine"], beta_id.to_string());
    let output = alpha
        .cmd()
        .args(["--json", "fleet", "remove", "beta"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let removed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(removed["changed"], true);
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

async fn cancel_socket(daemon: &Daemon, task: TaskId) -> Value {
    homebased::client::Client::new(daemon.home.join("homebased.sock"))
        .post(&format!("/v1/tasks/{task}/cancel"), &serde_json::json!({}))
        .await
        .unwrap()
}

async fn await_cancel_delivery(daemon: &Daemon, task: TaskId) -> Value {
    let start = Instant::now();
    loop {
        let body = cancel_socket(daemon, task).await;
        if body["delivery"]["state"] == "delivered" {
            return body;
        }
        assert!(start.elapsed() < Duration::from_secs(12), "{body}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn await_resource_cancel_delivery(daemon: &Daemon, task: TaskId) -> Value {
    let start = Instant::now();
    loop {
        let body = cancel_socket(daemon, task).await;
        if body["delivery"]["state"] == "resource_delivered" {
            return body;
        }
        assert!(start.elapsed() < Duration::from_secs(12), "{body}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn resource_cancel_identity(
    route: &OriginRoute,
    cancellation: uuid::Uuid,
    target_phase: ResourceRoutePhase,
) -> homebased::cancellation::ResourceCancellationRequestIdentity {
    let SubmissionState::Resource { resource, .. } = &route.submission else {
        panic!("resource fixture must retain resource identity");
    };
    homebased::cancellation::ResourceCancellationRequestIdentity {
        requester_machine: route.origin_machine,
        cancellation,
        request: route.request,
        task: route.task,
        origin_machine: route.origin_machine,
        authority_machine: route.execution_machine,
        resource: *resource,
        target_phase,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn resource_cancellation_before_acceptance_fences_delayed_queue_acceptance() {
    let origin = Daemon::start("resource-cancel-origin", true);
    let authority = Daemon::start("resource-cancel-authority", true);
    add_peer(&origin, &authority);
    add_peer(&authority, &origin);
    let resource = ResourceId::new();
    seed_authority_resource(&authority, resource);
    let request = RequestId::new();
    let task = TaskId::new();
    let spec = remote_spec(&authority, vec!["/bin/echo", "must-not-start"]);
    let route = seed_resource_route(&origin, &authority, resource, request, task, &spec);

    let delivered = await_resource_cancel_delivery(&origin, task).await;
    assert_eq!(
        delivered["delivery"]["resource"]["outcome"]["type"],
        "prevented_before_acceptance"
    );
    let saved = Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .origin_route_by_task(task)
        .unwrap()
        .unwrap();
    assert!(matches!(
        saved.submission,
        SubmissionState::Resource {
            phase: ResourceRoutePhase::CancelledBeforeLaunch,
            ..
        }
    ));

    let (status, delayed) = post_resource_queue(&authority, &origin, &route).await;
    assert_eq!(status, 200, "{delayed}");
    assert_eq!(delayed["receipt"]["outcome"]["type"], "rejected");
    assert_eq!(
        delayed["receipt"]["outcome"]["reason"],
        "cancelled_before_launch"
    );
    let connection = rusqlite::Connection::open(authority.home.join("homebased.sqlite")).unwrap();
    let prevented: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM resource_request_preventions",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let queued: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM resource_requests WHERE task_id=?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(prevented, 1);
    assert_eq!(queued, 0);
    assert_eq!(cancel_socket(&origin, task).await, delivered);
}

#[tokio::test(flavor = "multi_thread")]
async fn resource_cancellation_of_queued_request_uses_the_authority_route() {
    let origin = Daemon::start("resource-queued-cancel-origin", true);
    let authority = Daemon::start("resource-queued-cancel-authority", true);
    add_peer(&origin, &authority);
    add_peer(&authority, &origin);
    let resource = ResourceId::new();
    seed_authority_resource(&authority, resource);
    let request = RequestId::new();
    let task = TaskId::new();
    let spec = remote_spec(&authority, vec!["/bin/echo", "queued-not-started"]);
    let route = seed_resource_route(&origin, &authority, resource, request, task, &spec);
    let (status, accepted) = post_resource_queue(&authority, &origin, &route).await;
    assert_eq!(status, 200, "{accepted}");
    let receipt: ResourceQueueReceipt =
        serde_json::from_value(accepted["receipt"].clone()).unwrap();
    assert_eq!(
        Store::open(&origin.home.join("homebased.sqlite"))
            .unwrap()
            .resolve_resource_route(&receipt)
            .unwrap()
            .submission,
        SubmissionState::Resource {
            resource,
            phase: ResourceRoutePhase::Waiting,
        }
    );

    let delivered = await_resource_cancel_delivery(&origin, task).await;
    assert_eq!(
        delivered["delivery"]["resource"]["outcome"]["type"],
        "cancelled_before_launch"
    );
    let route = Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .origin_route_by_task(task)
        .unwrap()
        .unwrap();
    assert!(matches!(
        route.submission,
        SubmissionState::Resource {
            phase: ResourceRoutePhase::CancelledBeforeLaunch,
            ..
        }
    ));
    let connection = rusqlite::Connection::open(authority.home.join("homebased.sqlite")).unwrap();
    let state: String = connection
        .query_row(
            "SELECT json_extract(state_json, '$.type') FROM resource_requests WHERE request_id=?1",
            [request.0.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "cancelled_before_launch");
    let receipts: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM resource_cancellation_receipts",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipts, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_resource_cancellation_persists_intent_at_the_origin() {
    let viewer = Daemon::start("resource-remote-cancel-viewer", true);
    let origin = Daemon::start("resource-remote-cancel-origin", true);
    let authority = Daemon::start("resource-remote-cancel-authority", true);
    add_peer(&viewer, &origin);
    add_peer(&viewer, &authority);
    add_peer(&origin, &authority);
    add_peer(&authority, &origin);

    let resource = ResourceId::new();
    seed_authority_resource(&authority, resource);
    let request = RequestId::new();
    let task = TaskId::new();
    let spec = remote_spec(&authority, vec!["/bin/echo", "remote-resource-cancel"]);
    seed_resource_route(&origin, &authority, resource, request, task, &spec);

    let delivered = await_resource_cancel_delivery(&viewer, task).await;
    assert_eq!(
        delivered["delivery"]["resource"]["outcome"]["type"],
        "prevented_before_acceptance"
    );

    let origin_store = Store::open(&origin.home.join("homebased.sqlite")).unwrap();
    let saved = origin_store
        .cancellation_request(task)
        .unwrap()
        .expect("origin must own the durable cancellation intent");
    assert_eq!(saved.requester_machine, origin.machine_id());
    assert!(matches!(
        saved.target,
        homebased::cancellation::CancellationTarget::Resource(_)
    ));
    assert!(
        Store::open(&viewer.home.join("homebased.sqlite"))
            .unwrap()
            .cancellation_request(task)
            .unwrap()
            .is_none(),
        "viewer must not persist a second cancellation identity"
    );
    assert_eq!(cancel_socket(&viewer, task).await, delivered);
}

#[tokio::test(flavor = "multi_thread")]
async fn resource_cancellation_receipt_replays_after_restart_and_conflicts_on_reuse() {
    let origin = Daemon::start("resource-receipt-origin", true);
    let mut authority = Daemon::start("resource-receipt-authority", true);
    add_peer(&origin, &authority);
    add_peer(&authority, &origin);
    let resource = ResourceId::new();
    seed_authority_resource(&authority, resource);
    let request = RequestId::new();
    let task = TaskId::new();
    let spec = remote_spec(&authority, vec!["/bin/echo", "receipt-replay"]);
    let route = seed_resource_route(&origin, &authority, resource, request, task, &spec);
    let (status, accepted) = post_resource_queue(&authority, &origin, &route).await;
    assert_eq!(status, 200, "{accepted}");
    let queue_receipt: ResourceQueueReceipt =
        serde_json::from_value(accepted["receipt"].clone()).unwrap();
    Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .resolve_resource_route(&queue_receipt)
        .unwrap();
    let identity =
        resource_cancel_identity(&route, uuid::Uuid::now_v7(), ResourceRoutePhase::Waiting);

    let (status, first) = post_resource_cancel_wire(&authority, &identity).await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(
        first["receipt"]["outcome"]["type"],
        "cancelled_before_launch"
    );
    authority.restart();
    let (status, replay) = post_resource_cancel_wire(&authority, &identity).await;
    assert_eq!(status, 200, "{replay}");
    assert_eq!(first, replay);

    let mut changed = identity.clone();
    changed.resource = ResourceId::new();
    let (status, conflict) = post_resource_cancel_wire(&authority, &changed).await;
    assert_eq!(status, 409, "{conflict}");
}

#[tokio::test(flavor = "multi_thread")]
async fn resource_cancellation_origin_restart_replays_a_lost_authority_reply() {
    let mut origin = Daemon::start("resource-origin-restart-cancel-origin", true);
    let authority = Daemon::start("resource-origin-restart-cancel-authority", true);
    add_peer(&authority, &origin);
    let resource = ResourceId::new();
    seed_authority_resource(&authority, resource);
    let request = RequestId::new();
    let task = TaskId::new();
    let spec = remote_spec(&authority, vec!["/bin/echo", "restart-resource-cancel"]);
    let route = seed_resource_route(&origin, &authority, resource, request, task, &spec);
    let (status, queued) = post_resource_queue(&authority, &origin, &route).await;
    assert_eq!(status, 200, "{queued}");
    assert_eq!(queued["receipt"]["outcome"]["type"], "waiting");

    let pending = cancel_socket(&origin, task).await;
    assert_eq!(pending["delivery"]["state"], "pending");
    let saved = Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .cancellation_request(task)
        .unwrap()
        .expect("origin must save cancellation before delivery");
    let identity = saved
        .resource_identity()
        .expect("resource route must retain its typed identity");

    let (status, authority_reply) = post_resource_cancel_wire(&authority, &identity).await;
    assert_eq!(status, 200, "{authority_reply}");
    assert_eq!(
        authority_reply["receipt"]["outcome"]["type"],
        "cancelled_before_launch"
    );

    origin.restart();
    add_peer(&origin, &authority);
    let delivered = await_resource_cancel_delivery(&origin, task).await;
    assert_eq!(delivered["cancellation"], pending["cancellation"]);
    assert_eq!(
        delivered["delivery"]["resource"],
        authority_reply["receipt"]
    );
    let route = Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .origin_route_by_task(task)
        .unwrap()
        .unwrap();
    assert!(matches!(
        route.submission,
        SubmissionState::Resource {
            phase: ResourceRoutePhase::CancelledBeforeLaunch,
            ..
        }
    ));
    let connection = rusqlite::Connection::open(authority.home.join("homebased.sqlite")).unwrap();
    let receipts: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM resource_cancellation_receipts",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipts, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn resource_cancellation_rejects_wrong_owner_and_origin_proof() {
    let origin = Daemon::start("resource-proof-origin", true);
    let authority = Daemon::start("resource-proof-authority", true);
    add_peer(&origin, &authority);
    add_peer(&authority, &origin);
    let resource = ResourceId::new();
    seed_authority_resource(&authority, resource);
    let request = RequestId::new();
    let task = TaskId::new();
    let spec = remote_spec(&authority, vec!["/bin/echo", "proof-check"]);
    let route = seed_resource_route(&origin, &authority, resource, request, task, &spec);
    let identity = resource_cancel_identity(
        &route,
        uuid::Uuid::now_v7(),
        ResourceRoutePhase::AcceptanceUnknown,
    );

    let wrong_destination = ClusterClient::default()
        .post_json(
            &authority.address(),
            "/v1/cluster/resource-requests/cancel",
            &serde_json::json!({
                "api_version": 1,
                "protocol_version": homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION.0,
                "destination_machine": origin.machine_id(),
                "request": identity,
            }),
        )
        .await
        .unwrap();
    assert_eq!(wrong_destination.status.as_u16(), 409);

    let mut wrong_owner = identity.clone();
    wrong_owner.authority_machine = origin.machine_id();
    let (status, owner_conflict) = post_resource_cancel_wire(&authority, &wrong_owner).await;
    assert_eq!(status, 409, "{owner_conflict}");

    let mut wrong_proof = identity.clone();
    wrong_proof.request = RequestId::new();
    let (status, proof_conflict) = post_resource_cancel_wire(&authority, &wrong_proof).await;
    assert_eq!(status, 409, "{proof_conflict}");
    let connection = rusqlite::Connection::open(authority.home.join("homebased.sqlite")).unwrap();
    let prevented: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM resource_request_preventions",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let receipts: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM resource_cancellation_receipts",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(prevented, 0);
    assert_eq!(receipts, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_before_acceptance_prevents_delayed_submit() {
    let origin = Daemon::start("cancel-origin", true);
    let executor = Daemon::start("cancel-executor", true);
    let second = Daemon::start("cancel-second-requester", true);
    add_peer(&origin, &executor);
    add_peer(&second, &origin);
    add_peer(&second, &executor);
    let task = TaskId::new();
    let spec = remote_spec(&executor, vec!["/bin/echo", "must-not-run"]);
    seed_origin_route(&origin, &executor, task, &spec);

    let pending = cancel_socket(&origin, task).await;
    assert_eq!(pending["delivery"]["state"], "pending");
    assert_eq!(
        pending["requester_machine"],
        origin.machine_id().to_string()
    );
    let delivered = await_cancel_delivery(&origin, task).await;
    assert_eq!(
        delivered["delivery"]["executor"]["state"],
        "prevented_before_start"
    );
    assert_eq!(pending["cancellation"], delivered["cancellation"]);
    let other = await_cancel_delivery(&second, task).await;
    assert_eq!(
        other["delivery"]["executor"]["state"],
        "prevented_before_start"
    );
    assert_ne!(other["cancellation"], delivered["cancellation"]);

    let (status, submission) = post_execution(
        &executor,
        &origin,
        task,
        &serde_json::to_value(spec).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "{submission}");
    assert_eq!(
        submission["identity"]["reason"],
        "cancelled_before_acceptance"
    );
    assert!(!executor.home.join("tasks").join(task.to_string()).exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_keeps_a_terminal_execution_state() {
    let origin = Daemon::start("cancel-terminal-origin", true);
    let executor = Daemon::start("cancel-terminal-executor", true);
    let task = TaskId::new();
    let spec = remote_spec(&executor, vec!["/bin/echo", "complete"]);
    let (status, body) = post_execution(
        &executor,
        &origin,
        task,
        &serde_json::to_value(spec).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(wait_until(Duration::from_secs(8), || {
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_some_and(|row| row.status() == ProcessStatus::Succeeded)
    }));
    let (status, response) = post_cancel_wire(
        &executor,
        cancel_identity(&origin, &origin, &executor, task),
    )
    .await;
    assert_eq!(status, 200, "{response}");
    assert_eq!(response["receipt"]["state"]["state"], "already_terminal");
    assert_eq!(response["receipt"]["state"]["status"], "succeeded");
    assert_eq!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(task)
            .unwrap()
            .unwrap()
            .status(),
        ProcessStatus::Succeeded
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn third_machine_refuses_to_cancel_without_the_origin_route() {
    let viewer = Daemon::start("cancel-viewer", true);
    let mut origin = Daemon::start("cancel-offline-origin", true);
    let executor = Daemon::start("cancel-running-executor", true);
    add_peer(&viewer, &origin);
    add_peer(&viewer, &executor);
    add_peer(&executor, &origin);
    let task = TaskId::new();
    let spec = remote_spec(&executor, vec!["/bin/sh", "-c", "sleep 30"]);
    seed_origin_route(&origin, &executor, task, &spec);
    let (status, body) = post_execution(
        &executor,
        &origin,
        task,
        &serde_json::to_value(spec).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(wait_until(Duration::from_secs(5), || {
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_some_and(|row| row.status() == ProcessStatus::Running)
    }));
    origin.stop();

    let (status, legacy) = post_legacy_cancel_wire(
        &executor,
        cancel_identity(&viewer, &origin, &executor, task),
    )
    .await;
    assert_ne!(status, 200, "{legacy}");
    assert_eq!(legacy["error"]["code"], "cluster_lookup_incomplete");

    let (ok, local_body) = inspect_cli(&executor, "cancel", task);
    assert!(!ok, "{local_body}");
    assert_eq!(local_body["error"]["code"], "cluster_lookup_incomplete");

    let (ok, body) = inspect_cli(&viewer, "cancel", task);
    assert!(!ok, "{body}");
    assert_eq!(body["error"]["code"], "cluster_lookup_incomplete");
    assert!(
        Store::open(&viewer.home.join("homebased.sqlite"))
            .unwrap()
            .pending_cancellation_requests()
            .unwrap()
            .is_empty()
    );
    assert!(wait_until(Duration::from_secs(2), || {
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_some_and(|row| row.status() == ProcessStatus::Running)
    }));
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_lookup_offline_gap_does_not_claim_acceptance() {
    let viewer = Daemon::start("cancel-lookup", true);
    let mut peer = Daemon::start("cancel-offline", true);
    add_peer(&viewer, &peer);
    peer.stop();
    let task = TaskId::new();
    let (ok, body) = inspect_cli(&viewer, "cancel", task);
    assert!(!ok, "{body}");
    assert_eq!(body["error"]["code"], "cluster_lookup_incomplete");
    assert!(
        Store::open(&viewer.home.join("homebased.sqlite"))
            .unwrap()
            .pending_cancellation_requests()
            .unwrap()
            .is_empty()
    );
}

async fn post_cancel_wire(executor: &Daemon, request: CancellationRequestIdentity) -> (u16, Value) {
    let response = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions/cancel",
            &serde_json::json!({
                "api_version": 1,
                "protocol_version": 2,
                "request": request,
                "target": {"type": "execution", "request_id": null},
            }),
        )
        .await
        .unwrap();
    let status = response.status.as_u16();
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    if status == 200 {
        assert_eq!(body["api_version"], 1);
        assert_eq!(body["protocol_version"], 2);
    }
    (status, body)
}

async fn post_legacy_cancel_wire(
    executor: &Daemon,
    request: CancellationRequestIdentity,
) -> (u16, Value) {
    let response = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions/cancel",
            &serde_json::json!({
                "api_version": 1,
                "protocol_version": 1,
                "request": request,
            }),
        )
        .await
        .unwrap();
    let status = response.status.as_u16();
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    if status == 200 {
        assert_eq!(body["api_version"], 1);
        assert_eq!(body["protocol_version"], 1);
    }
    (status, body)
}

fn cancel_identity(
    requester: &Daemon,
    origin: &Daemon,
    executor: &Daemon,
    task: TaskId,
) -> CancellationRequestIdentity {
    CancellationRequestIdentity {
        requester_machine: requester.machine_id(),
        cancellation: uuid::Uuid::now_v7(),
        task,
        origin_machine: origin.machine_id(),
        execution_machine: executor.machine_id(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_requester_restart_and_lost_reply_reuse_one_receipt() {
    let mut origin = Daemon::start("cancel-restart-origin", true);
    let mut executor = Daemon::start("cancel-restart-executor", true);
    add_peer(&origin, &executor);
    let task = TaskId::new();
    let spec = remote_spec(&executor, vec!["/bin/echo", "never"]);
    seed_origin_route(&origin, &executor, task, &spec);
    executor.stop();
    let first = cancel_socket(&origin, task).await;
    assert_eq!(first["delivery"]["state"], "pending");
    origin.stop();
    let saved = Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .pending_cancellation_requests()
        .unwrap();
    assert_eq!(saved.len(), 1);
    let receipt = Store::open(&executor.home.join("homebased.sqlite"))
        .unwrap()
        .receive_cancellation(saved[0].identity())
        .unwrap();
    assert!(matches!(
        receipt.state,
        ExecutorCancelState::PreventedBeforeStart { .. }
    ));
    executor.restart();
    origin.restart();
    let delivered = await_cancel_delivery(&origin, task).await;
    assert_eq!(delivered["cancellation"], first["cancellation"]);
    assert_eq!(
        delivered["delivery"]["executor"]["state"],
        "prevented_before_start"
    );
    assert_eq!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .pending_executor_cancellations()
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn executor_restart_resumes_received_cancellation() {
    let origin = Daemon::start("cancel-executor-restart-origin", true);
    let mut executor = Daemon::start("cancel-executor-restart-owner", true);
    let task = TaskId::new();
    let spec = remote_spec(&executor, vec!["/bin/sh", "-c", "sleep 30"]);
    let (status, body) = post_execution(
        &executor,
        &origin,
        task,
        &serde_json::to_value(spec).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    executor.stop();
    let request = cancel_identity(&origin, &origin, &executor, task);
    let receipt = Store::open(&executor.home.join("homebased.sqlite"))
        .unwrap()
        .receive_cancellation(request)
        .unwrap();
    assert!(matches!(
        receipt.state,
        ExecutorCancelState::PendingApplication
    ));
    executor.restart();
    assert!(wait_until(Duration::from_secs(10), || {
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .pending_executor_cancellations()
            .unwrap()
            .is_empty()
    }));
    let (status, duplicate) = post_cancel_wire(&executor, request).await;
    assert_eq!(status, 200, "{duplicate}");
    assert_ne!(
        duplicate["receipt"]["state"]["state"],
        "pending_application"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_requesters_and_terminal_identity_keep_original_outcome() {
    let first = Daemon::start("cancel-first", true);
    let second = Daemon::start("cancel-second", true);
    let executor = Daemon::start("cancel-compact-owner", true);
    let task = TaskId::new();
    Store::open(&executor.home.join("homebased.sqlite"))
        .unwrap()
        .accept_execution(&ExecutionRecord {
            task,
            origin_machine: first.machine_id(),
            execution_machine: executor.machine_id(),
            spec: remote_spec(&executor, vec!["/bin/true"]).into(),
            state: ProcessStatus::Succeeded,
        })
        .unwrap();
    let one = cancel_identity(&first, &first, &executor, task);
    let two = cancel_identity(&second, &first, &executor, task);
    let (status, first_reply) = post_cancel_wire(&executor, one).await;
    assert_eq!(status, 200, "{first_reply}");
    assert_eq!(first_reply["receipt"]["state"]["state"], "already_terminal");
    assert_eq!(first_reply["receipt"]["state"]["status"], "succeeded");
    let (status, duplicate) = post_cancel_wire(&executor, one).await;
    assert_eq!(status, 200, "{duplicate}");
    assert_eq!(first_reply, duplicate);
    let (status, second_reply) = post_cancel_wire(&executor, two).await;
    assert_eq!(status, 200, "{second_reply}");
    assert_eq!(second_reply["receipt"]["state"]["status"], "succeeded");
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_conflicting_executor_claims_return_error() {
    let viewer = Daemon::start("cancel-conflict-viewer", true);
    let first = Daemon::start("cancel-conflict-first", true);
    let second = Daemon::start("cancel-conflict-second", true);
    add_peer(&viewer, &first);
    add_peer(&viewer, &second);
    let task = TaskId::new();
    seed_inspection_row(&first, task);
    seed_inspection_row(&second, task);
    let (ok, body) = inspect_cli(&viewer, "cancel", task);
    assert!(!ok, "{body}");
    assert_eq!(body["error"]["code"], "cluster_task_conflict");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_rejects_wrong_destination_and_version_without_tombstone() {
    let requester = Daemon::start("cancel-protocol-requester", true);
    let executor = Daemon::start("cancel-protocol-executor", true);
    let task = TaskId::new();
    let mut request = cancel_identity(&requester, &requester, &executor, task);
    request.execution_machine = requester.machine_id();
    let (status, _) = post_cancel_wire(&executor, request).await;
    assert_eq!(status, 409);
    let request = cancel_identity(&requester, &requester, &executor, task);
    let response = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions/cancel",
            &serde_json::json!({ "api_version": 1, "protocol_version": 99, "request": request }),
        )
        .await
        .unwrap();
    assert_ne!(response.status.as_u16(), 200);
    let error: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(error["api_version"], 1);
    assert_eq!(error["error"]["code"], "cluster_protocol_incompatible");
    assert!(
        Store::open(&executor.home.join("homebased.sqlite"))
            .unwrap()
            .executor_identity(task)
            .unwrap()
            .is_none()
    );
}

fn seed_inspection_row(daemon: &Daemon, task: TaskId) {
    let spec = remote_spec(daemon, vec!["/bin/echo", "saved-output"]);
    let row = new_queued_task(NewTask {
        id: task,
        name: Some(spec.name.clone()),
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
    Store::open(&daemon.home.join("homebased.sqlite"))
        .unwrap()
        .insert_task(&row)
        .unwrap();
    let evidence = daemon.home.join("tasks").join(task.to_string());
    fs::create_dir_all(&evidence).unwrap();
    fs::write(evidence.join("output.log"), "saved-output\n").unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn inspection_uses_local_row_before_unavailable_peers() {
    let local = Daemon::start("inspect-local", true);
    let mut peer = Daemon::start("inspect-offline", true);
    wait_for_probe(&peer.address()).await;
    add_peer(&local, &peer);
    peer.stop();
    let task = TaskId::new();
    seed_inspection_row(&local, task);
    let (ok, show) = inspect_cli(&local, "show", task);
    assert!(ok, "{show}");
    assert_eq!(show["status"], "queued");
    assert_eq!(show["found_on"], local.machine_id().to_string());
    let (ok, log) = inspect_cli(&local, "log", task);
    assert!(ok, "{log}");
    assert_eq!(log["log"], "saved-output\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn third_machine_proxies_executor_detail_and_log() {
    let viewer = Daemon::start("inspect-viewer", true);
    let origin = Daemon::start("inspect-origin", true);
    let executor = Daemon::start("inspect-executor", true);
    wait_for_probe(&origin.address()).await;
    wait_for_probe(&executor.address()).await;
    add_peer(&viewer, &origin);
    add_peer(&viewer, &executor);
    let task = TaskId::new();
    let spec = remote_spec(&executor, vec!["/bin/echo", "third-machine-log"]);
    seed_origin_route(&origin, &executor, task, &spec);
    let (status, body) = post_execution(
        &executor,
        &origin,
        task,
        &serde_json::to_value(spec).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(wait_until(Duration::from_secs(10), || executor
        .home
        .join("tasks")
        .join(task.to_string())
        .join("output.log")
        .exists()));
    let (ok, show) = inspect_cli(&viewer, "show", task);
    assert!(ok, "{show}");
    assert_eq!(show["execution_machine"], executor.machine_id().to_string());
    assert_eq!(show["origin_machine"], origin.machine_id().to_string());
    assert_eq!(show["found_on"], executor.machine_id().to_string());
    assert_eq!(show["availability"], "available");
    let (ok, log) = inspect_cli(&viewer, "log", task);
    assert!(ok, "{log}");
    assert!(log["log"].as_str().unwrap().contains("third-machine-log"));
    let output = viewer
        .cmd()
        .args(["--json", "task", "log", &task.to_string(), "--tail", "1"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tail: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(tail["log"], "third-machine-log");
}

#[tokio::test(flavor = "multi_thread")]
async fn route_cache_survives_offline_executor_without_invented_unknown_status() {
    use std::num::NonZeroU64;

    let origin = Daemon::start("cache-origin", true);
    let mut executor = Daemon::start("cache-executor", true);
    wait_for_probe(&executor.address()).await;
    add_peer(&origin, &executor);
    let task = TaskId::new();
    let spec = remote_spec(&executor, vec!["/bin/echo", "no-run"]);
    seed_origin_route(&origin, &executor, task, &spec);
    let event = homebased::events::TaskEvent {
        task,
        seq: NonZeroU64::new(1).unwrap(),
        origin_machine: origin.machine_id(),
        execution_machine: executor.machine_id(),
        payload: EventPayload::State {
            status: ProcessStatus::Running,
        },
    };
    Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .accept_inbound_event(&event)
        .unwrap();
    executor.stop();
    let (ok, unknown) = inspect_cli(&origin, "show", task);
    assert!(ok, "{unknown}");
    assert!(unknown["status"].is_null());
    assert_eq!(unknown["submission"]["type"], "acceptance_unknown");
    assert_eq!(unknown["availability"], "executor_unavailable");
    assert!(unknown["last_update"].as_str().is_some());
    Store::open(&origin.home.join("homebased.sqlite"))
        .unwrap()
        .resolve_origin_route(task, SubmissionState::Accepted)
        .unwrap();
    let (ok, cached) = inspect_cli(&origin, "show", task);
    assert!(ok, "{cached}");
    assert_eq!(cached["status"], "running");
    let (ok, log) = inspect_cli(&origin, "log", task);
    assert!(!ok);
    assert_eq!(log["error"]["code"], "task_unavailable");
}

#[tokio::test(flavor = "multi_thread")]
async fn inspection_distinguishes_complete_negative_from_offline_gap() {
    let mut viewer = Daemon::start("negative-viewer", true);
    let mut peer = Daemon::start("negative-peer", true);
    wait_for_probe(&peer.address()).await;
    viewer.stop();
    let mut config = fs::OpenOptions::new()
        .append(true)
        .open(&viewer.config)
        .unwrap();
    use std::io::Write;
    writeln!(
        config,
        "[[fleet.machines]]\naddress = \"{}\"",
        peer.address()
    )
    .unwrap();
    viewer.restart();
    add_peer(&viewer, &peer);
    let task = TaskId::new();
    let (ok, absent) = inspect_cli(&viewer, "show", task);
    assert!(!ok);
    assert_eq!(absent["error"]["code"], "task_not_found");
    let peer_id = peer.machine_id();
    peer.stop();
    let (ok, incomplete) = inspect_cli(&viewer, "show", task);
    assert!(!ok);
    assert_eq!(incomplete["error"]["code"], "cluster_lookup_incomplete");
    assert_eq!(incomplete["error"]["retryable"], true);
    assert_eq!(
        incomplete["error"]["input"]["unchecked"][0],
        peer_id.to_string()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn inspection_retains_rejection_and_compact_accepted_state() {
    let viewer = Daemon::start("identity-viewer", true);
    let executor = Daemon::start("identity-executor", true);
    wait_for_probe(&executor.address()).await;
    add_peer(&viewer, &executor);
    let rejected = TaskId::new();
    Store::open(&executor.home.join("homebased.sqlite"))
        .unwrap()
        .reject_execution(&homebased::submission::RejectionTombstone {
            task: rejected,
            origin_machine: viewer.machine_id(),
            execution_machine: executor.machine_id(),
            reason: "abandoned_before_acceptance".into(),
        })
        .unwrap();
    let (ok, show) = inspect_cli(&viewer, "show", rejected);
    assert!(ok, "{show}");
    assert_eq!(show["reason"], "abandoned_before_acceptance");
    let (ok, log) = inspect_cli(&viewer, "log", rejected);
    assert!(!ok);
    assert_eq!(log["error"]["code"], "task_not_started");
    let accepted = TaskId::new();
    let spec = remote_spec(&executor, vec!["/bin/echo", "done"]);
    Store::open(&executor.home.join("homebased.sqlite"))
        .unwrap()
        .accept_execution(&ExecutionRecord {
            task: accepted,
            origin_machine: viewer.machine_id(),
            execution_machine: executor.machine_id(),
            spec: spec.into(),
            state: ProcessStatus::Succeeded,
        })
        .unwrap();
    let (ok, show) = inspect_cli(&viewer, "show", accepted);
    assert!(ok, "{show}");
    assert_eq!(show["status"], "succeeded");
    assert_eq!(show["detail_available"], false);
    let (ok, log) = inspect_cli(&viewer, "log", accepted);
    assert!(!ok);
    assert_eq!(log["error"]["code"], "task_unavailable");
}

#[tokio::test(flavor = "multi_thread")]
async fn inspection_detects_conflicting_executor_claims() {
    let viewer = Daemon::start("conflict-viewer", true);
    let first = Daemon::start("conflict-first", true);
    let second = Daemon::start("conflict-second", true);
    wait_for_probe(&first.address()).await;
    wait_for_probe(&second.address()).await;
    add_peer(&viewer, &first);
    add_peer(&viewer, &second);
    let task = TaskId::new();
    seed_inspection_row(&first, task);
    seed_inspection_row(&second, task);
    let (ok, show) = inspect_cli(&viewer, "show", task);
    assert!(!ok);
    assert_eq!(show["error"]["code"], "cluster_task_conflict");
    let (ok, log) = inspect_cli(&viewer, "log", task);
    assert!(!ok);
    assert_eq!(log["error"]["code"], "cluster_task_conflict");
}

#[tokio::test(flavor = "multi_thread")]
async fn inspection_follows_route_to_executor_outside_peer_snapshot() {
    let viewer = Daemon::start("route-viewer", true);
    let origin = Daemon::start("route-origin", true);
    let executor = Daemon::start("route-executor", true);
    wait_for_probe(&origin.address()).await;
    wait_for_probe(&executor.address()).await;
    add_peer(&origin, &executor);
    add_peer(&viewer, &origin);
    let task = TaskId::new();
    let spec = remote_spec(&executor, vec!["/bin/echo", "saved-output"]);
    seed_origin_route(&origin, &executor, task, &spec);
    seed_inspection_row(&executor, task);
    let route_path = format!(
        "/v1/cluster/origin/tasks/{task}?api_version=1&destination_machine={}",
        origin.machine_id()
    );
    let response = ClusterClient::default()
        .get(&origin.address(), &route_path)
        .await
        .unwrap();
    let route: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(response.status.as_u16(), 200);
    assert!(route["origin"].get("callback").is_none());
    assert!(route["origin"].get("spec").is_none());
    let wrong = route_path.replace(
        &origin.machine_id().to_string(),
        &viewer.machine_id().to_string(),
    );
    let response = ClusterClient::default()
        .get(&origin.address(), &wrong)
        .await
        .unwrap();
    assert_eq!(response.status.as_u16(), 409);
    let (ok, show) = inspect_cli(&viewer, "show", task);
    assert!(ok, "{show}");
    assert_eq!(show["found_on"], executor.machine_id().to_string());
    let (ok, log) = inspect_cli(&viewer, "log", task);
    assert!(ok, "{log}");
    assert_eq!(log["log"], "saved-output\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn inspection_detects_disagreeing_origin_routes() {
    let viewer = Daemon::start("route-conflict-viewer", true);
    let first = Daemon::start("route-conflict-first", true);
    let second = Daemon::start("route-conflict-second", true);
    wait_for_probe(&first.address()).await;
    wait_for_probe(&second.address()).await;
    add_peer(&viewer, &first);
    add_peer(&viewer, &second);
    let task = TaskId::new();
    let first_spec = remote_spec(&second, vec!["/bin/echo", "first"]);
    let second_spec = remote_spec(&first, vec!["/bin/echo", "second"]);
    seed_origin_route(&first, &second, task, &first_spec);
    seed_origin_route(&second, &first, task, &second_spec);
    let (ok, show) = inspect_cli(&viewer, "show", task);
    assert!(!ok);
    assert_eq!(show["error"]["code"], "cluster_task_conflict");
}

#[tokio::test(flavor = "multi_thread")]
async fn task_log_never_reads_a_guessed_directory_without_the_socket() {
    let mut daemon = Daemon::start("socket-log", false);
    let task = TaskId::new();
    seed_inspection_row(&daemon, task);
    daemon.stop();
    let (ok, response) = inspect_cli(&daemon, "log", task);
    assert!(!ok);
    assert_eq!(response["error"]["code"], "daemon_unavailable");
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_message_retry_accepts_protocol_change_and_reuses_receipt() {
    use std::os::unix::fs::PermissionsExt;

    let mut receiver = Daemon::start("message-protocol-receiver", true);
    let codex = receiver._dir.path().join("fake-codex");
    fs::write(
        &codex,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOMEBASED_HOME/message-queue.log\"\nif [ -e \"$HOMEBASED_HOME/message-queue.fail\" ]; then exit 9; fi\nexit 0\n",
    )
    .unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    receiver.codex_override = Some(codex);
    receiver.restart();

    let cwd = receiver.user_home.join("message-workspace");
    fs::create_dir_all(&cwd).unwrap();
    let thread = ThreadId(uuid::Uuid::now_v7());
    let session = receiver
        .user_home
        .join(".codex/sessions/2026/09/22/rollout-message-protocol.jsonl");
    fs::create_dir_all(session.parent().unwrap()).unwrap();
    fs::write(
        &session,
        format!(
            "{}\n",
            serde_json::json!({
                "type": "session_meta",
                "payload": {"id": thread.to_string(), "cwd": cwd},
            })
        ),
    )
    .unwrap();

    let message_id = MessageId::new();
    let mut request = serde_json::json!({
        "api_version": 1,
        "protocol_version": 1,
        "message_id": message_id,
        "destination_machine": receiver.machine_id(),
        "source": {
            "kind": "thread",
            "machine": MachineId::new(),
            "thread": uuid::Uuid::now_v7(),
        },
        "recipient": {"kind": "thread", "thread": thread},
        "body": "Review the protocol retry",
        "reply_to": null,
        "conversation_id": message_id.as_uuid(),
    });
    let fail_marker = receiver.home.join("message-queue.fail");
    fs::write(&fail_marker, "fail").unwrap();

    let (status, failed) = post_message(&receiver, &request).await;
    assert_eq!(status, 503, "{failed}");
    assert_eq!(failed["error"]["code"], "message_delivery_failed");
    let first_attempt = Store::open(&receiver.home.join("homebased.sqlite"))
        .unwrap()
        .message_delivery(message_id)
        .unwrap();
    assert_eq!(first_attempt.attempt.unwrap().request.protocol_version, 1);
    assert!(first_attempt.receipt.is_none());

    fs::remove_file(fail_marker).unwrap();
    request["protocol_version"] = serde_json::json!(2);
    let (status, delivered) = post_message(&receiver, &request).await;
    assert_eq!(status, 200, "{delivered}");
    assert_eq!(delivered["protocol_version"], 2);
    assert_eq!(delivered["receipt"]["protocol_version"], 2);
    let delivery = Store::open(&receiver.home.join("homebased.sqlite"))
        .unwrap()
        .message_delivery(message_id)
        .unwrap();
    assert_eq!(delivery.attempt.unwrap().request.protocol_version, 2);
    assert_eq!(delivery.receipt.unwrap().protocol_version, 2);

    request["protocol_version"] = serde_json::json!(1);
    let (status, retried) = post_message(&receiver, &request).await;
    assert_eq!(status, 200, "{retried}");
    assert_eq!(retried["protocol_version"], 1);
    assert_eq!(retried["receipt"]["protocol_version"], 1);
    let saved = Store::open(&receiver.home.join("homebased.sqlite"))
        .unwrap()
        .message_delivery(message_id)
        .unwrap();
    assert_eq!(saved.receipt.unwrap().protocol_version, 2);
    assert_eq!(
        fs::read_to_string(receiver.home.join("message-queue.log"))
            .unwrap()
            .lines()
            .count(),
        2
    );

    let unsupported_id = MessageId::new();
    let mut unsupported = request;
    unsupported["protocol_version"] = serde_json::json!(99);
    unsupported["message_id"] = serde_json::json!(unsupported_id);
    unsupported["conversation_id"] = serde_json::json!(unsupported_id.as_uuid());
    let (status, incompatible) = post_message(&receiver, &unsupported).await;
    assert_eq!(status, 409, "{incompatible}");
    assert_eq!(
        incompatible["error"]["code"],
        "cluster_protocol_incompatible"
    );
    assert!(
        Store::open(&receiver.home.join("homebased.sqlite"))
            .unwrap()
            .message_delivery(unsupported_id)
            .unwrap()
            .attempt
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn local_duplicate_identity_recovers_after_the_clone_goes_offline() {
    use std::io::Write;

    let original = Daemon::start("identity-original", true);
    let mut clone = Daemon::start("identity-clone", true);
    clone.stop();
    fs::copy(
        original.home.join("machine-id"),
        clone.home.join("machine-id"),
    )
    .unwrap();
    let mut config = fs::OpenOptions::new()
        .append(true)
        .open(&clone.config)
        .unwrap();
    writeln!(
        config,
        "[[fleet.machines]]\naddress = \"{}\"",
        original.address()
    )
    .unwrap();
    clone.spawn();
    assert_eq!(clone.machine_id(), original.machine_id());

    assert!(wait_until(Duration::from_secs(10), || {
        clone
            .cmd()
            .args(["--json", "fleet", "machines"])
            .output()
            .is_ok_and(|output| {
                output.status.success()
                    && serde_json::from_slice::<Value>(&output.stdout).is_ok_and(|inventory| {
                        inventory["local"]["identity"]["state"] == "duplicate_machine_identity"
                    })
            })
    }));

    let mut original = original;
    original.stop();
    for expected in ["duplicate_machine_identity", "consistent"] {
        let output = clone
            .cmd()
            .args(["--json", "fleet", "discover"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let inventory: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(inventory["local"]["identity"]["state"], expected);
    }
}
