//! Message receiver behavior through a Fleet-enabled daemon

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use homebased::domain::{API_VERSION, ThreadId};
use homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
use homebased::machine::MachineId;
use homebased::message::{MessageId, MessageRequest};
use homebased::store::Store;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

struct Daemon {
    _dir: TempDir,
    state_home: PathBuf,
    user_home: PathBuf,
    config: PathBuf,
    queue_log: PathBuf,
    queue_fail_marker: PathBuf,
    codex: PathBuf,
    address: String,
    child: Option<Child>,
}

impl Daemon {
    fn start() -> Self {
        let dir = TempDir::new().unwrap();
        let state_home = dir.path().join("state");
        let user_home = dir.path().join("user-home");
        fs::create_dir_all(&state_home).unwrap();
        fs::create_dir_all(&user_home).unwrap();
        let config = dir.path().join("config.toml");
        fs::write(
            &config,
            "[fleet]\nenabled = true\nmachine_name = \"receiver\"\n\n[fleet.discovery]\nmdns = false\n",
        )
        .unwrap();
        let queue_log = dir.path().join("queue.log");
        let queue_fail_marker = dir.path().join("queue.fail");
        let codex = dir.path().join("fake-codex");
        fs::write(
            &codex,
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOMEBASED_QUEUE_LOG\"\nif [ -e \"$HOMEBASED_QUEUE_FAIL\" ]; then exit 9; fi\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
        let port = free_port();
        let mut daemon = Self {
            _dir: dir,
            state_home,
            user_home,
            config,
            queue_log,
            queue_fail_marker,
            codex,
            address: format!("127.0.0.1:{port}"),
            child: None,
        };
        daemon.spawn();
        daemon
    }

    fn spawn(&mut self) {
        let child = Command::new(assert_cmd::cargo::cargo_bin("homebased"))
            .args(["daemon", "serve", "--web-listen", &self.address])
            .env("HOMEBASED_HOME", &self.state_home)
            .env("HOMEBASED_CONFIG", &self.config)
            .env("HOME", &self.user_home)
            .env("HOMEBASED_CODEX", &self.codex)
            .env("HOMEBASED_QUEUE_LOG", &self.queue_log)
            .env("HOMEBASED_QUEUE_FAIL", &self.queue_fail_marker)
            .env_remove("CODEX_HOME")
            .env_remove("HOMEBASED_WEB_LISTEN")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        self.child = Some(child);
        assert!(wait_until(Duration::from_secs(10), || self.http_ready()));
    }

    fn http_ready(&self) -> bool {
        let Ok(mut stream) = TcpStream::connect(&self.address) else {
            return false;
        };
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

    fn machine_id(&self) -> MachineId {
        fs::read_to_string(self.state_home.join("machine-id"))
            .unwrap()
            .parse()
            .unwrap()
    }

    fn send(&self, body: &Value) -> HttpResponse {
        post_json(&self.address, "/v1/cluster/messages", body)
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

    fn restart(&mut self) {
        self.stop();
        self.spawn();
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

struct HttpResponse {
    status: u16,
    body: Value,
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
    let first_line = json!({
        "type": "session_meta",
        "payload": {
            "id": thread.to_string(),
            "cwd": cwd,
        }
    });
    fs::write(path, format!("{first_line}\n{{\"type\":\"event_msg\"}}\n")).unwrap();
}

/// Register a live Claude Code session inbox, the way Claude Code 2.1 does
fn write_claude_session(user_home: &Path, thread: ThreadId, cwd: &Path) -> UnixListener {
    let sessions = user_home.join(".claude/sessions");
    fs::create_dir_all(&sessions).unwrap();
    let socket = user_home.join("claude.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    // this test process stands in for the live session process
    let pid = std::process::id();
    let record = json!({
        "pid": pid,
        "sessionId": thread.to_string(),
        "cwd": cwd,
        "messagingSocketPath": socket,
        "peerProtocol": 1,
        "updatedAt": 1,
    });
    fs::write(sessions.join(format!("{pid}.json")), record.to_string()).unwrap();
    let digest = Sha256::digest(socket.as_os_str().as_encoded_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    fs::write(
        sessions.join(format!("{pid}.{hex}.key")),
        json!({ "peerToken": "test-token" }).to_string(),
    )
    .unwrap();
    listener
}

fn request(id: MessageId, destination: MachineId, recipient: Value, body: &str) -> Value {
    json!({
        "api_version": API_VERSION,
        "protocol_version": CLUSTER_PROTOCOL_VERSION.0,
        "message_id": id,
        "destination_machine": destination,
        "source": {
            "kind": "thread",
            "machine": MachineId::new(),
            "thread": Uuid::now_v7(),
        },
        "recipient": recipient,
        "body": body,
        "reply_to": Uuid::now_v7(),
        "conversation_id": Uuid::now_v7(),
    })
}

fn post_json(address: &str, path: &str, body: &Value) -> HttpResponse {
    let bytes = serde_json::to_vec(body).unwrap();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        bytes.len()
    );
    let mut stream = TcpStream::connect(address).unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    stream.write_all(&bytes).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let response = String::from_utf8_lossy(&response);
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap();
    HttpResponse {
        status,
        body: serde_json::from_str(body).unwrap_or_else(|_| json!({"raw": body})),
    }
}

#[test]
fn receiver_binds_sessions_retries_once_and_never_replays_after_restart() {
    let mut daemon = Daemon::start();
    let workspace = daemon.user_home.join("workspace");
    let exact_cwd = daemon.user_home.join("exact-project");
    fs::create_dir_all(&workspace).unwrap();
    fs::create_dir_all(&exact_cwd).unwrap();

    let exact_thread = ThreadId(Uuid::now_v7());
    write_session(&daemon.user_home, "exact", exact_thread, &exact_cwd);
    let exact_id = MessageId::new();
    let exact_request = request(
        exact_id,
        daemon.machine_id(),
        json!({"kind": "thread", "thread": exact_thread}),
        "Review the local change",
    );
    let exact_response = daemon.send(&exact_request);
    assert_eq!(exact_response.status, 200, "{:?}", exact_response.body);
    assert_eq!(exact_response.body["api_version"], API_VERSION);
    assert_eq!(
        exact_response.body["protocol_version"],
        CLUSTER_PROTOCOL_VERSION.0
    );
    assert_eq!(exact_response.body["receipt"]["api_version"], API_VERSION);
    assert_eq!(
        exact_response.body["receipt"]["protocol_version"],
        CLUSTER_PROTOCOL_VERSION.0
    );
    assert_eq!(
        exact_response.body["receipt"]["destination_thread"],
        exact_thread.to_string()
    );
    assert_eq!(
        exact_response.body["receipt"]["destination_cwd"],
        exact_cwd.to_string_lossy().to_string()
    );
    let exact_queue_line = &daemon.queue_calls()[0];
    assert!(exact_queue_line.contains(&format!(
        "--thread {exact_thread} --message HOMEBASED_MESSAGE "
    )));
    assert!(exact_queue_line.contains(&format!("\"message_id\":\"{exact_id}\"")));
    assert!(exact_queue_line.contains(&format!(
        "\"source\":{{\"kind\":\"thread\",\"machine\":\"{}\",\"thread\":\"{}\"}}",
        exact_request["source"]["machine"].as_str().unwrap(),
        exact_request["source"]["thread"].as_str().unwrap(),
    )));
    assert!(exact_queue_line.contains(&format!("\"destination_thread\":\"{exact_thread}\"")));
    assert!(exact_queue_line.contains("\"body\":\"Review the local change\""));
    assert!(exact_queue_line.contains("\"reply_to\":"));
    assert!(exact_queue_line.contains("\"conversation_id\":"));

    let older_thread = ThreadId(Uuid::now_v7());
    write_session(&daemon.user_home, "cwd-older", older_thread, &workspace);
    thread::sleep(Duration::from_millis(1100));
    let newer_thread = ThreadId(Uuid::now_v7());
    write_session(&daemon.user_home, "cwd-newer", newer_thread, &workspace);
    let cwd_id = MessageId::new();
    let cwd_request = request(
        cwd_id,
        daemon.machine_id(),
        json!({"kind": "cwd", "cwd": "~/workspace"}),
        "Use the newest matching thread",
    );
    let cwd_response = daemon.send(&cwd_request);
    assert_eq!(cwd_response.status, 200, "{:?}", cwd_response.body);
    assert_eq!(
        cwd_response.body["receipt"]["destination_thread"],
        newer_thread.to_string()
    );
    assert_eq!(daemon.queue_calls().len(), 2);

    thread::sleep(Duration::from_millis(1100));
    let newest_after_delivery = ThreadId(Uuid::now_v7());
    write_session(
        &daemon.user_home,
        "cwd-later",
        newest_after_delivery,
        &workspace,
    );
    let cwd_retry = daemon.send(&cwd_request);
    assert_eq!(cwd_retry.status, 200, "{:?}", cwd_retry.body);
    assert_eq!(
        cwd_retry.body["receipt"]["destination_thread"],
        newer_thread.to_string()
    );
    assert_eq!(daemon.queue_calls().len(), 2);

    let mut changed_request = cwd_request.clone();
    changed_request["body"] = json!("different message content");
    let changed_response = daemon.send(&changed_request);
    assert_eq!(changed_response.status, 409);
    assert_eq!(changed_response.body["error"]["code"], "message_conflict");
    assert_eq!(daemon.queue_calls().len(), 2);

    let mismatched_id = MessageId::new();
    let mismatched_request = request(
        mismatched_id,
        MachineId::new(),
        json!({"kind": "thread", "thread": exact_thread}),
        "Wrong destination",
    );
    let mismatched_response = daemon.send(&mismatched_request);
    assert_eq!(mismatched_response.status, 409);
    assert_eq!(
        mismatched_response.body["error"]["code"],
        "machine_identity_mismatch"
    );
    assert!(daemon.delivery(mismatched_id).attempt.is_none());
    assert_eq!(daemon.queue_calls().len(), 2);

    let failed_id = MessageId::new();
    let failed_request = request(
        failed_id,
        daemon.machine_id(),
        json!({"kind": "cwd", "cwd": "~/workspace"}),
        "Retry this failed delivery",
    );
    fs::write(&daemon.queue_fail_marker, "fail").unwrap();
    let failed_response = daemon.send(&failed_request);
    assert_eq!(failed_response.status, 503, "{:?}", failed_response.body);
    assert_eq!(
        failed_response.body["error"]["code"],
        "message_delivery_failed"
    );
    let failed_delivery = daemon.delivery(failed_id);
    assert!(failed_delivery.attempt.is_some());
    assert!(failed_delivery.receipt.is_none());
    assert_eq!(daemon.queue_calls().len(), 3);

    thread::sleep(Duration::from_millis(1100));
    write_session(
        &daemon.user_home,
        "cwd-newer-after-failure",
        ThreadId(Uuid::now_v7()),
        &workspace,
    );

    daemon.restart();
    assert_eq!(daemon.queue_calls().len(), 3);
    fs::remove_file(&daemon.queue_fail_marker).unwrap();
    let retried_response = daemon.send(&failed_request);
    assert_eq!(retried_response.status, 200, "{:?}", retried_response.body);
    assert_eq!(
        retried_response.body["receipt"]["destination_thread"],
        newest_after_delivery.to_string()
    );
    assert_eq!(daemon.queue_calls().len(), 4);
    assert!(daemon.queue_calls()[3].contains(&format!(
        "--thread {newest_after_delivery} --message HOMEBASED_MESSAGE "
    )));
    assert!(daemon.delivery(failed_id).receipt.is_some());

    let id: MessageId = failed_request["message_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let typed: MessageRequest = serde_json::from_value(failed_request).unwrap();
    assert_eq!(typed.message_id, id);
}

#[test]
fn receiver_delivers_to_claude_sessions_without_codex_queue() {
    let daemon = Daemon::start();
    let live_cwd = daemon.user_home.join("live-project");
    fs::create_dir_all(&live_cwd).unwrap();
    let live_thread = ThreadId(Uuid::now_v7());
    let listener = write_claude_session(&daemon.user_home, live_thread, &live_cwd);
    let inbox = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut body = String::new();
        stream.read_to_string(&mut body).unwrap();
        body
    });

    let live_id = MessageId::new();
    let live = daemon.send(&request(
        live_id,
        daemon.machine_id(),
        json!({"kind": "thread", "thread": live_thread}),
        "Review the Claude change",
    ));
    assert_eq!(live.status, 200, "{:?}", live.body);
    assert_eq!(
        live.body["receipt"]["destination_thread"],
        live_thread.to_string()
    );
    assert_eq!(
        live.body["receipt"]["destination_cwd"],
        live_cwd.to_string_lossy().to_string()
    );
    let frame = inbox.join().unwrap();
    let lines: Vec<Value> = frame
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines[1]["session_id"], live_thread.to_string());
    let content = lines[1]["message"]["content"].as_str().unwrap();
    assert!(content.starts_with("HOMEBASED_MESSAGE "), "{content}");
    assert!(content.contains(&format!("\"message_id\":\"{live_id}\"")));
    assert!(content.contains("\"body\":\"Review the Claude change\""));
    assert!(daemon.queue_calls().is_empty());

    // a stopped session with no T3 owner fails with the reason, not as unknown
    let stopped_thread = ThreadId(Uuid::now_v7());
    let project = daemon.user_home.join(".claude/projects/-stopped-project");
    fs::create_dir_all(&project).unwrap();
    fs::write(
        project.join(format!("{stopped_thread}.jsonl")),
        format!("{}\n", json!({ "type": "user", "cwd": live_cwd })),
    )
    .unwrap();
    let stopped = daemon.send(&request(
        MessageId::new(),
        daemon.machine_id(),
        json!({"kind": "thread", "thread": stopped_thread}),
        "Are you there?",
    ));
    assert_eq!(stopped.body["error"]["code"], "message_delivery_failed");
    let message = stopped.body["error"]["input"]["message"].as_str().unwrap();
    assert!(message.contains("no T3 thread owns it"), "{message}");
    assert!(daemon.queue_calls().is_empty());
}
