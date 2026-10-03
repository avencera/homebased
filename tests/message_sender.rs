//! Direct message sender routing through real Fleet-enabled daemons.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use homebased::domain::{
    API_VERSION, Agent, AgentKind, AgentWorkload, TaskEnv, TaskId, ThreadId, Workload,
};
use homebased::fleet::address::MachineAddress;
use homebased::machine::MachineId;
use homebased::message::{MessageId, MessageSource};
use homebased::spec;
use homebased::store::{NewTask, Store, new_queued_task};
use homebased::submission::{
    CallbackContext, CallbackExecutable, OriginRoute, RequestId, SubmissionState,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use uuid::Uuid;

struct Daemon {
    _dir: TempDir,
    state_home: PathBuf,
    user_home: PathBuf,
    config: PathBuf,
    queue_log: PathBuf,
    codex: PathBuf,
    claude: PathBuf,
    /// A fake Claude worker runs until this file exists
    claude_release: PathBuf,
    address: String,
    child: Option<Child>,
}

struct ForwardProxy {
    address: String,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ForwardProxy {
    fn start(destination: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let drop_next_message_response = Arc::new(AtomicBool::new(true));
        let thread_stop = stop.clone();
        let thread_drop = drop_next_message_response.clone();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((client, _)) => {
                        let destination = destination.clone();
                        let drop_response = thread_drop.clone();
                        thread::spawn(move || forward(client, &destination, &drop_response));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            address,
            stop,
            thread: Some(thread),
        }
    }

    fn address(&self) -> MachineAddress {
        format!("http://{}", self.address).parse().unwrap()
    }
}

impl Drop for ForwardProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn forward(mut client: TcpStream, destination: &str, drop_response: &AtomicBool) {
    let _ = client.set_read_timeout(Some(Duration::from_secs(5)));
    let Some((request, path, header_end)) = read_request(&mut client) else {
        return;
    };
    let request_header = String::from_utf8_lossy(&request[..header_end]);
    let request_body = &request[header_end + 4..];
    let rewritten_header = request_header
        .lines()
        .filter(|line| !line.to_ascii_lowercase().starts_with("host:"))
        .collect::<Vec<_>>()
        .join("\r\n");
    let outgoing =
        format!("{rewritten_header}\r\nHost: {destination}\r\nConnection: close\r\n\r\n");
    let Ok(mut upstream) = TcpStream::connect(destination) else {
        return;
    };
    if upstream
        .write_all(outgoing.as_bytes())
        .and_then(|()| upstream.write_all(request_body))
        .is_err()
    {
        return;
    }
    let mut response = Vec::new();
    let _ = upstream.read_to_end(&mut response);
    if path == "/v1/cluster/messages" && drop_response.swap(false, Ordering::Relaxed) {
        return;
    }
    let _ = client.write_all(&response);
}

fn read_request(stream: &mut TcpStream) -> Option<(Vec<u8>, String, usize)> {
    let mut request = Vec::new();
    let mut chunk = [0; 4096];
    let header_end = loop {
        let count = stream.read(&mut chunk).ok()?;
        if count == 0 {
            return None;
        }
        request.extend_from_slice(&chunk[..count]);
        if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break index;
        }
    };
    let header = std::str::from_utf8(&request[..header_end]).ok()?;
    let path = header
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .to_string();
    let content_length = header
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    let request_end = header_end + 4 + content_length;
    while request.len() < request_end {
        let count = stream.read(&mut chunk).ok()?;
        if count == 0 {
            return None;
        }
        request.extend_from_slice(&chunk[..count]);
    }
    request.truncate(request_end);
    Some((request, path, header_end))
}

/// Why a daemon start did not become ready
enum Launch {
    /// Another process bound the chosen port first
    PortTaken,
    /// The daemon exited for another reason or never answered
    NotReady(String),
}

impl Daemon {
    fn start(name: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let state_home = dir.path().join("state");
        let user_home = dir.path().join("user-home");
        fs::create_dir_all(&state_home).unwrap();
        fs::create_dir_all(&user_home).unwrap();
        let config = dir.path().join("config.toml");
        fs::write(
            &config,
            format!(
                "[fleet]\nenabled = true\nmachine_name = \"{name}\"\n\n[fleet.discovery]\nmdns = false\ntailscale = false\n"
            ),
        )
        .unwrap();
        let queue_log = dir.path().join("queue.log");
        let codex = dir.path().join("fake-codex");
        fs::write(
            &codex,
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOMEBASED_QUEUE_LOG\"\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
        let claude_release = dir.path().join("release-claude");
        let claude = dir.path().join("fake-claude");
        fs::write(
            &claude,
            format!(
                "#!/bin/sh\ncat > /dev/null\nwhile [ ! -f '{}' ]; do sleep 0.05; done\nexit 0\n",
                claude_release.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&claude, fs::Permissions::from_mode(0o700)).unwrap();
        let address = format!("127.0.0.1:{}", free_port());
        let mut daemon = Self {
            _dir: dir,
            state_home,
            user_home,
            config,
            queue_log,
            codex,
            claude,
            claude_release,
            address,
            child: None,
        };
        daemon.start_on_free_port();
        daemon
    }

    fn command(&self) -> Command {
        let mut command = Command::new(assert_cmd::cargo::cargo_bin("homebased"));
        command
            .env("HOMEBASED_HOME", &self.state_home)
            .env("HOMEBASED_CONFIG", &self.config)
            .env("HOME", &self.user_home)
            .env("HOMEBASED_CODEX", &self.codex)
            .env("HOMEBASED_CLAUDE", &self.claude)
            .env("HOMEBASED_QUEUE_LOG", &self.queue_log)
            .env_remove("HOMEBASED_TASK_ID")
            .env_remove("CODEX_THREAD_ID")
            .env_remove("CODEX_SESSION_ID")
            .env_remove("CLAUDE_CODE_SESSION_ID")
            .env_remove("CODEX_HOME")
            .env_remove("HOMEBASED_WEB_LISTEN")
            .stdin(Stdio::null());
        command
    }

    /// First start of a daemon on a port from `free_port`
    ///
    /// Another test's process can bind that port between `free_port` releasing
    /// it and this daemon binding it, so a start that fails with the address in
    /// use retries on a fresh port. Restarts go through `spawn` and keep their
    /// address, because peers already point at it
    fn start_on_free_port(&mut self) {
        for _ in 0..3 {
            match self.launch() {
                Ok(()) => return,
                Err(Launch::PortTaken) => {
                    self.address = format!("127.0.0.1:{}", free_port());
                }
                Err(Launch::NotReady(message)) => panic!("{message}"),
            }
        }
        panic!("daemon found no free port after 3 attempts");
    }

    fn spawn(&mut self) {
        match self.launch() {
            Ok(()) => {}
            Err(Launch::PortTaken) => panic!("daemon port {} is taken", self.address),
            Err(Launch::NotReady(message)) => panic!("{message}"),
        }
    }

    fn launch(&mut self) -> Result<(), Launch> {
        let log = self.state_home.join("daemon-stderr.log");
        let child = self
            .command()
            .args(["daemon", "serve", "--web-listen"])
            .arg(&self.address)
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log).unwrap())
            .spawn()
            .unwrap();
        self.child = Some(child);
        let mut exited = None;
        let ready = wait_until(Duration::from_secs(10), || {
            exited = self
                .child
                .as_mut()
                .and_then(|child| child.try_wait().ok().flatten());
            exited.is_none() && self.http_ready()
        });
        if ready {
            return Ok(());
        }

        let stderr = fs::read_to_string(&log).unwrap_or_default();
        if exited.is_some() && stderr.contains("Address already in use") {
            self.child = None;
            return Err(Launch::PortTaken);
        }
        Err(Launch::NotReady(format!(
            "daemon on {} was not ready after 10s; exit status: {exited:?}\n{stderr}",
            self.address
        )))
    }

    fn http_ready(&self) -> bool {
        let Ok(mut stream) = TcpStream::connect(&self.address) else {
            return false;
        };
        use std::io::{Read, Write};
        let request = format!(
            "GET /v1/status HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.address
        );
        if stream.write_all(request.as_bytes()).is_err() {
            return false;
        }
        let mut response = String::new();
        stream.read_to_string(&mut response).is_ok() && response.starts_with("HTTP/1.1 200")
    }

    fn cli(&self, args: &[&str]) -> Output {
        self.command().args(args).output().unwrap()
    }

    fn machine_id(&self) -> MachineId {
        fs::read_to_string(self.state_home.join("machine-id"))
            .unwrap()
            .parse()
            .unwrap()
    }

    fn address(&self) -> MachineAddress {
        format!("http://{}", self.address).parse().unwrap()
    }

    fn queue_calls(&self) -> Vec<String> {
        fs::read_to_string(&self.queue_log)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn delivery(&self, id: MessageId) -> homebased::message::MessageDelivery {
        Store::open(&self.state_home.join(homebased::home::DB_NAME))
            .unwrap()
            .message_delivery(id)
            .unwrap()
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
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

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

fn write_session(user_home: &Path, name: &str, thread: ThreadId, cwd: &Path) {
    let path = user_home
        .join(".codex/sessions/2026/09/22")
        .join(format!("rollout-{name}.jsonl"));
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        path,
        format!(
            "{}\n{{\"type\":\"event_msg\"}}\n",
            json!({
                "type": "session_meta",
                "payload": { "id": thread.to_string(), "cwd": cwd },
            })
        ),
    )
    .unwrap();
}

fn add_peer(sender: &Daemon, address: &MachineAddress) {
    let address = address.to_string();
    let added = sender.cli(&["--json", "fleet", "add", &address]);
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    let discovered = sender.cli(&["--json", "fleet", "discover"]);
    assert!(
        discovered.status.success(),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&discovered.stderr),
        String::from_utf8_lossy(&discovered.stdout)
    );
    let inventory = read_json(&discovered);
    assert!(
        inventory["machines"]
            .as_array()
            .is_some_and(|machines| machines.iter().any(|machine| machine["name"] == "receiver")),
        "{}",
        String::from_utf8_lossy(&discovered.stdout)
    );
}

fn send_thread(
    sender: &Daemon,
    machine: &str,
    thread: ThreadId,
    message: &str,
    id: MessageId,
) -> Output {
    let thread = thread.to_string();
    let id = id.to_string();
    sender.cli(&[
        "--json",
        "message",
        "send",
        "--machine",
        machine,
        "--thread",
        &thread,
        "--message",
        message,
        "--message-id",
        &id,
        "--source-thread",
        "018f0a48-f0ef-7d12-8f01-000000000001",
    ])
}

fn read_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn sender_resolves_remote_local_and_task_routes_and_retries_same_binding() {
    let mut sender = Daemon::start("sender");
    let mut receiver = Daemon::start("receiver");
    let proxy = ForwardProxy::start(receiver.address.clone());
    add_peer(&sender, &proxy.address());

    let remote_cwd = receiver.user_home.join("workspace");
    fs::create_dir_all(&remote_cwd).unwrap();
    let exact_thread = ThreadId(Uuid::now_v7());
    write_session(&receiver.user_home, "exact", exact_thread, &remote_cwd);

    let remote_id = MessageId::new();
    let first = send_thread(
        &sender,
        "receiver",
        exact_thread,
        "Review the remote change",
        remote_id,
    );
    assert!(!first.status.success());
    let lost_response: Value = serde_json::from_slice(&first.stderr).unwrap();
    assert_eq!(
        lost_response["error"]["code"],
        "message_outcome_unknown",
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(
        lost_response["error"]["input"]["machine"],
        receiver.machine_id().to_string()
    );
    assert_eq!(receiver.queue_calls().len(), 1);

    sender.stop();
    sender.spawn();
    assert_eq!(receiver.queue_calls().len(), 1);
    let original_machine = receiver.machine_id();
    receiver.stop();
    let replacement = Daemon::start("receiver");
    add_peer(&sender, &replacement.address());
    let while_rebound = send_thread(
        &sender,
        "receiver",
        exact_thread,
        "Review the remote change",
        remote_id,
    );
    assert!(!while_rebound.status.success());
    let rebound_error: Value = serde_json::from_slice(&while_rebound.stderr).unwrap();
    assert_eq!(rebound_error["error"]["code"], "machine_unavailable");
    assert_eq!(
        rebound_error["error"]["input"]["machine"],
        original_machine.to_string()
    );
    assert!(replacement.queue_calls().is_empty());

    receiver.spawn();
    let retry = send_thread(
        &sender,
        "receiver",
        exact_thread,
        "Review the remote change",
        remote_id,
    );
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert_eq!(receiver.queue_calls().len(), 1);
    let removed = sender.cli(&[
        "--json",
        "fleet",
        "remove",
        &replacement.machine_id().to_string(),
    ]);
    assert!(
        removed.status.success(),
        "{}",
        String::from_utf8_lossy(&removed.stderr)
    );
    let saved = receiver.delivery(remote_id).attempt.unwrap();
    assert_eq!(saved.destination_thread, exact_thread);
    assert_eq!(saved.request.conversation_id, remote_id.as_uuid());
    assert_eq!(
        saved.request.source,
        MessageSource::Thread {
            machine: sender.machine_id(),
            thread: ThreadId(
                "018f0a48-f0ef-7d12-8f01-000000000001"
                    .parse::<Uuid>()
                    .unwrap()
            ),
        }
    );

    let changed = send_thread(
        &sender,
        "missing-machine-name",
        exact_thread,
        "Changed content must conflict",
        remote_id,
    );
    assert!(!changed.status.success());
    let changed_error: Value = serde_json::from_slice(&changed.stderr).unwrap();
    assert_eq!(changed_error["error"]["code"], "message_conflict");
    assert_eq!(receiver.queue_calls().len(), 1);

    let unresolved_id = MessageId::new();
    let unresolved = send_thread(
        &sender,
        "missing-machine-name",
        exact_thread,
        "Original request whose target cannot be resolved",
        unresolved_id,
    );
    assert!(!unresolved.status.success());
    let unresolved_error: Value = serde_json::from_slice(&unresolved.stderr).unwrap();
    assert_eq!(unresolved_error["error"]["code"], "machine_not_found");
    let changed_unresolved = send_thread(
        &sender,
        "missing-machine-name",
        exact_thread,
        "Changed request after failed resolution",
        unresolved_id,
    );
    assert!(!changed_unresolved.status.success());
    let changed_unresolved_error: Value =
        serde_json::from_slice(&changed_unresolved.stderr).unwrap();
    assert_eq!(
        changed_unresolved_error["error"]["code"],
        "message_conflict"
    );

    let cwd_id = MessageId::new();
    let cwd_output = sender.cli(&[
        "--json",
        "message",
        "send",
        "--machine",
        "receiver",
        "--cwd",
        "~/workspace",
        "--message",
        "Use the receiver home directory",
        "--message-id",
        &cwd_id.to_string(),
        "--source-thread",
        "018f0a48-f0ef-7d12-8f01-000000000001",
    ]);
    assert!(
        cwd_output.status.success(),
        "{}",
        String::from_utf8_lossy(&cwd_output.stderr)
    );
    let cwd_response = read_json(&cwd_output);
    assert_eq!(
        cwd_response["destination_cwd"],
        remote_cwd.to_string_lossy().to_string()
    );
    assert_eq!(receiver.queue_calls().len(), 2);

    let task_thread = ThreadId(Uuid::now_v7());
    let task_cwd = receiver.user_home.join("task-workspace");
    fs::create_dir_all(&task_cwd).unwrap();
    write_session(&receiver.user_home, "task-origin", task_thread, &task_cwd);
    let task = TaskId::new();
    let spec = spec::parse_normalized_value(&json!({
        "api_version": API_VERSION,
        "thread": task_thread,
        "name": "message origin route",
        "cwd": task_cwd,
        "timeout": "30m",
        "workload": { "type": "task", "command": ["echo", "route"] },
    }))
    .unwrap();
    Store::open(&receiver.state_home.join(homebased::home::DB_NAME))
        .unwrap()
        .insert_origin_route(&OriginRoute {
            request: RequestId::new(),
            task,
            origin_machine: receiver.machine_id(),
            execution_machine: receiver.machine_id(),
            thread: task_thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: receiver.user_home.to_string_lossy().into_owned(),
                },
                cwd: task_cwd,
                codex: CallbackExecutable::available(receiver.codex.clone()),
            },
            spec,
            submission: SubmissionState::AcceptanceUnknown,
            last_execution_state: None,
            last_updated_at: chrono::Utc::now(),
            last_accepted_seq: 0,
            last_settled_seq: 0,
        })
        .unwrap();
    let task_id = task.to_string();
    let task_message_id = MessageId::new();
    let task_output = sender.cli(&[
        "--json",
        "message",
        "send",
        "--task",
        &task_id,
        "--message",
        "Send this to the task origin",
        "--message-id",
        &task_message_id.to_string(),
        "--source-thread",
        "018f0a48-f0ef-7d12-8f01-000000000001",
    ]);
    assert!(
        task_output.status.success(),
        "{}",
        String::from_utf8_lossy(&task_output.stderr)
    );
    let task_response = read_json(&task_output);
    assert_eq!(
        task_response["destination_machine"],
        receiver.machine_id().to_string()
    );
    assert_eq!(task_response["destination_thread"], task_thread.to_string());
    assert_eq!(receiver.queue_calls().len(), 3);

    let local_cwd = sender.user_home.join("local-workspace");
    fs::create_dir_all(&local_cwd).unwrap();
    let local_thread = ThreadId(Uuid::now_v7());
    write_session(&sender.user_home, "local", local_thread, &local_cwd);
    let local_output = send_thread(
        &sender,
        "sender",
        local_thread,
        "Deliver to the local thread",
        MessageId::new(),
    );
    assert!(
        local_output.status.success(),
        "{}",
        String::from_utf8_lossy(&local_output.stderr)
    );
    let local_response = read_json(&local_output);
    assert_eq!(
        local_response["destination_machine"],
        sender.machine_id().to_string()
    );
    assert_eq!(
        local_response["destination_thread"],
        local_thread.to_string()
    );
    assert_eq!(sender.queue_calls().len(), 1);
}
#[test]
fn sender_reports_offline_machine_before_message_delivery() {
    let sender = Daemon::start("sender");
    let mut receiver = Daemon::start("receiver");
    add_peer(&sender, &receiver.address());
    receiver.stop();

    let output = sender.cli(&[
        "--json",
        "message",
        "send",
        "--machine",
        "receiver",
        "--thread",
        "018f0a48-f0ef-7d12-8f01-000000000002",
        "--message",
        "Do not redirect this message",
        "--message-id",
        "0196a33a-35b0-7b12-8f01-000000000003",
        "--source-thread",
        "018f0a48-f0ef-7d12-8f01-000000000001",
    ]);
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "machine_unavailable");
    assert_eq!(
        error["error"]["input"]["machine"],
        receiver.machine_id().to_string()
    );
}

#[test]
fn task_message_from_its_origin_thread_is_rejected_but_task_source_is_allowed() {
    let sender = Daemon::start("sender");
    let thread = ThreadId(Uuid::now_v7());
    let cwd = sender.user_home.join("self-message-workspace");
    fs::create_dir_all(&cwd).unwrap();
    write_session(&sender.user_home, "self-message", thread, &cwd);
    let task = TaskId::new();
    let spec = spec::parse_normalized_value(&json!({
        "api_version": API_VERSION,
        "thread": thread,
        "name": "self message source",
        "cwd": cwd,
        "timeout": "30m",
        "workload": { "type": "task", "command": ["/bin/true"] },
    }))
    .unwrap();
    Store::open(&sender.state_home.join(homebased::home::DB_NAME))
        .unwrap()
        .insert_origin_route(&OriginRoute {
            request: RequestId::new(),
            task,
            origin_machine: sender.machine_id(),
            execution_machine: sender.machine_id(),
            thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: sender.user_home.to_string_lossy().into_owned(),
                },
                cwd,
                codex: CallbackExecutable::available(sender.codex.clone()),
            },
            spec,
            submission: SubmissionState::Accepted,
            last_execution_state: None,
            last_updated_at: chrono::Utc::now(),
            last_accepted_seq: 0,
            last_settled_seq: 0,
        })
        .unwrap();

    let task_id = task.to_string();
    let self_message = MessageId::new();
    let self_output = sender.cli(&[
        "--json",
        "message",
        "send",
        "--task",
        &task_id,
        "--message",
        "Do not deliver this to myself",
        "--message-id",
        &self_message.to_string(),
        "--source-thread",
        &thread.to_string(),
    ]);
    assert!(!self_output.status.success());
    let self_error: Value = serde_json::from_slice(&self_output.stderr).unwrap();
    assert_eq!(self_error["error"]["code"], "message_to_self");
    assert_eq!(self_error["error"]["input"]["thread"], thread.to_string());
    assert!(
        self_error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("homebased task followup")
    );
    assert!(sender.queue_calls().is_empty());
    let repeated_self_message = sender.cli(&[
        "--json",
        "message",
        "send",
        "--task",
        &task_id,
        "--message",
        "Do not deliver this to myself",
        "--message-id",
        &self_message.to_string(),
        "--source-thread",
        &thread.to_string(),
    ]);
    assert!(!repeated_self_message.status.success());
    let repeated_self_error: Value = serde_json::from_slice(&repeated_self_message.stderr).unwrap();
    assert_eq!(repeated_self_error["error"]["code"], "message_to_self");
    assert!(sender.queue_calls().is_empty());
    let binding_count: i64 =
        rusqlite::Connection::open(sender.state_home.join(homebased::home::DB_NAME))
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM outbound_message_bindings WHERE message_id=?1",
                [self_message.to_string()],
                |row| row.get(0),
            )
            .unwrap();
    assert_eq!(binding_count, 1);

    let task_source_id = MessageId::new();
    let task_source_output = sender.cli(&[
        "--json",
        "message",
        "send",
        "--task",
        &task_id,
        "--message",
        "A worker task may message its origin",
        "--message-id",
        &task_source_id.to_string(),
        "--source-task",
        &task_id,
    ]);
    assert!(
        task_source_output.status.success(),
        "{}",
        String::from_utf8_lossy(&task_source_output.stderr)
    );
    assert_eq!(sender.queue_calls().len(), 1);
}

fn send_worker(sender: &Daemon, task: &str, source: &[&str]) -> Output {
    let id = MessageId::new().to_string();
    let mut args = vec![
        "--json",
        "message",
        "send",
        "--worker",
        task,
        "--message",
        "Also update the changelog",
        "--message-id",
        &id,
    ];
    args.extend_from_slice(source);
    sender.cli(&args)
}

fn error_json(output: &Output) -> Value {
    assert!(!output.status.success());
    serde_json::from_slice(&output.stderr).unwrap()
}

#[test]
fn worker_message_reaches_a_running_claude_worker_on_its_execution_machine() {
    let sender = Daemon::start("sender");
    let receiver = Daemon::start("receiver");
    add_peer(&sender, &receiver.address());
    let source = ["--source-thread", "018f0a48-f0ef-7d12-8f01-000000000001"];

    let origin = ThreadId(Uuid::now_v7());
    let cwd = receiver.user_home.join("worker-workspace");
    fs::create_dir_all(&cwd).unwrap();
    // submit finds the origin thread by the id in its rollout file name
    write_session(
        &receiver.user_home,
        &format!("2026-09-22T00-00-00-{origin}"),
        origin,
        &cwd,
    );
    let spec_path = receiver.state_home.join("worker-spec.json");
    fs::write(
        &spec_path,
        serde_json::to_vec(&json!({
            "api_version": API_VERSION,
            "thread": origin,
            "name": "live worker",
            "cwd": cwd,
            "timeout": "30m",
            "workload": { "type": "agent", "agent": "claude", "prompt": "work" },
        }))
        .unwrap(),
    )
    .unwrap();
    let submitted = receiver.cli(&[
        "--json",
        "task",
        "submit",
        "--spec",
        spec_path.to_str().unwrap(),
    ]);
    assert!(
        submitted.status.success(),
        "{}",
        String::from_utf8_lossy(&submitted.stderr)
    );
    let task = read_json(&submitted)["id"].as_str().unwrap().to_string();
    let status = |daemon: &Daemon| read_json(&daemon.cli(&["--json", "task", "show", &task]));
    assert!(wait_until(Duration::from_secs(10), || {
        status(&receiver)["status"] == "running"
    }));

    // the sender holds no row for this task, so it reads the executor's detail
    let remote_view = status(&sender);
    assert_eq!(remote_view["worker_thread"], task.as_str());
    assert_eq!(
        remote_view["execution_machine"],
        receiver.machine_id().to_string()
    );

    // the fake worker publishes no Claude registry entry, so a Codex session with
    // the same id makes the receiver's delivery visible in its queue log
    let worker_thread: ThreadId = task.parse().unwrap();
    write_session(&receiver.user_home, "worker", worker_thread, &cwd);
    let sent = send_worker(&sender, &task, &source);
    assert!(
        sent.status.success(),
        "{}",
        String::from_utf8_lossy(&sent.stderr)
    );
    let receipt = read_json(&sent);
    assert_eq!(
        receipt["destination_machine"],
        receiver.machine_id().to_string()
    );
    assert_eq!(receipt["destination_thread"], task.as_str());
    let queued_lines = receiver.queue_calls();
    assert_eq!(queued_lines.len(), 1);
    assert!(queued_lines[0].contains(&task), "{queued_lines:?}");

    let self_send = error_json(&send_worker(&receiver, &task, &["--source-task", &task]));
    assert_eq!(self_send["error"]["code"], "message_to_self");
    assert_eq!(self_send["error"]["input"]["thread"], task.as_str());

    let unknown = error_json(&send_worker(&sender, &TaskId::new().to_string(), &source));
    assert_eq!(unknown["error"]["code"], "task_not_found");

    // the daemon starts queued rows only at startup, so this one stays queued
    let queued = TaskId::new();
    fs::create_dir_all(receiver.state_home.join("tasks").join(queued.to_string())).unwrap();
    Store::open(&receiver.state_home.join(homebased::home::DB_NAME))
        .unwrap()
        .insert_task(&new_queued_task(NewTask {
            id: queued,
            name: homebased::domain::TaskName::parse("test task").unwrap(),
            thread: origin,
            workload: Workload::Agent(AgentWorkload {
                agent: Agent::new(AgentKind::Claude, None),
                extra_args: vec![],
                report_trailer: false,
                resume_thread: None,
            }),
            cwd: cwd.clone(),
            timeout: Duration::from_secs(1800),
            env: TaskEnv {
                path: "/bin".into(),
                home: receiver.user_home.to_string_lossy().into_owned(),
            },
            binary: receiver.claude.clone(),
        }))
        .unwrap();
    let not_started = error_json(&send_worker(&sender, &queued.to_string(), &source));
    assert_eq!(not_started["error"]["code"], "worker_message_unavailable");
    assert_eq!(not_started["error"]["input"]["reason"], "no_worker_thread");

    fs::write(&receiver.claude_release, "").unwrap();
    assert!(wait_until(Duration::from_secs(10), || {
        status(&receiver)["status"]
            .as_str()
            .is_some_and(|status| status != "running")
    }));
    let finished = error_json(&send_worker(&sender, &task, &source));
    assert_eq!(finished["error"]["code"], "worker_message_unavailable");
    assert_eq!(finished["error"]["input"]["reason"], "terminal");
    assert!(
        finished["error"]["message"]
            .as_str()
            .unwrap()
            .contains("homebased task followup")
    );
    // the finished task's own callback also goes through `codex queue`, at a time
    // the test does not control, so count only the worker message
    let messages = receiver
        .queue_calls()
        .into_iter()
        .filter(|call| call.contains("HOMEBASED_MESSAGE"))
        .count();
    assert_eq!(messages, 1);
}
