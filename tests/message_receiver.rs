//! Message and supervisor-notice receiver behavior through a Fleet-enabled daemon

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use homebased::domain::{API_VERSION, ThreadId};
use homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
use homebased::machine::MachineId;
use homebased::message::{MessageId, MessageRequest};
use homebased::resource::{
    ActionId, AssignmentRevision, DeliveryAttemptId, LoanId, NoticeId, ResourceRevision,
    SupervisorAddress, SupervisorNoticePayload, SupervisorNoticeRequest,
};
use homebased::store::Store;
use serde_json::{Value, json};
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

    fn send_notice(&self, body: &Value) -> HttpResponse {
        post_json(&self.address, "/v1/cluster/resource-notices", body)
    }

    fn queue_calls(&self) -> Vec<String> {
        fs::read_to_string(&self.queue_log)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn delivery(&self, id: MessageId) -> homebased::message::MessageDelivery {
        Store::open(&self.state_home.join("homebased.sqlite"))
            .unwrap()
            .message_delivery(id)
            .unwrap()
    }

    fn notice_delivery(
        &self,
        attempt_id: DeliveryAttemptId,
    ) -> homebased::message::MessageDelivery {
        self.delivery(MessageId::from_uuid(attempt_id.as_uuid()).unwrap())
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
    fs::write(
        path,
        format!("{}\n{{\"type\":\"event_msg\"}}\n", first_line),
    )
    .unwrap();
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

fn notice_request(
    destination_machine: MachineId,
    destination_thread: ThreadId,
    attempt_id: DeliveryAttemptId,
) -> Value {
    serde_json::to_value(SupervisorNoticeRequest {
        api_version: API_VERSION,
        protocol_version: CLUSTER_PROTOCOL_VERSION.0,
        source_machine: MachineId::new(),
        destination: SupervisorAddress {
            machine: destination_machine,
            thread: destination_thread,
        },
        notice_id: NoticeId::new(),
        loan_id: LoanId::new(),
        action_id: ActionId::new(),
        state_revision: ResourceRevision::new(9),
        assignment_revision: AssignmentRevision::new(3),
        attempt_id,
        payload: SupervisorNoticePayload::ReleaseRequired {
            task_id: homebased::domain::TaskId::new(),
        },
    })
    .unwrap()
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
fn receiver_queues_resource_notice_by_attempt_on_the_exact_thread() {
    let daemon = Daemon::start();
    let exact_cwd = daemon.user_home.join("exact-project");
    let newer_cwd = daemon.user_home.join("newer-project");
    fs::create_dir_all(&exact_cwd).unwrap();
    fs::create_dir_all(&newer_cwd).unwrap();

    let exact_thread = ThreadId(Uuid::now_v7());
    let newer_thread = ThreadId(Uuid::now_v7());
    write_session(&daemon.user_home, "notice-exact", exact_thread, &exact_cwd);
    write_session(&daemon.user_home, "notice-newer", newer_thread, &newer_cwd);

    let wrong_machine_attempt = DeliveryAttemptId::new();
    let wrong_machine = notice_request(MachineId::new(), exact_thread, wrong_machine_attempt);
    let mismatch = daemon.send_notice(&wrong_machine);
    assert_eq!(mismatch.status, 409, "{:?}", mismatch.body);
    assert_eq!(mismatch.body["error"]["code"], "machine_identity_mismatch");
    assert!(
        daemon
            .notice_delivery(wrong_machine_attempt)
            .attempt
            .is_none()
    );

    let missing_thread_attempt = DeliveryAttemptId::new();
    let missing_thread = notice_request(
        daemon.machine_id(),
        ThreadId(Uuid::now_v7()),
        missing_thread_attempt,
    );
    let missing = daemon.send_notice(&missing_thread);
    assert_eq!(missing.status, 404, "{:?}", missing.body);
    assert_eq!(missing.body["error"]["code"], "agent_thread_not_found");
    assert!(
        daemon
            .notice_delivery(missing_thread_attempt)
            .attempt
            .is_none()
    );
    assert!(daemon.queue_calls().is_empty());

    let attempt_id = DeliveryAttemptId::new();
    let notice = notice_request(daemon.machine_id(), exact_thread, attempt_id);
    let response = daemon.send_notice(&notice);
    assert_eq!(response.status, 200, "{:?}", response.body);
    assert_eq!(response.body["receipt"]["attempt_id"], json!(attempt_id));
    assert_eq!(
        response.body["receipt"]["destination_thread"],
        exact_thread.to_string()
    );
    let first_queue_line = daemon.queue_calls().remove(0);
    assert!(first_queue_line.contains(&format!(
        "--thread {exact_thread} --message HOMEBASED_RESOURCE_NOTICE "
    )));
    assert!(!first_queue_line.contains("HOMEBASED_MESSAGE "));
    assert!(!first_queue_line.contains("HOMEBASED_EVENT "));
    let queued: Value = serde_json::from_str(
        first_queue_line
            .split_once("--message HOMEBASED_RESOURCE_NOTICE ")
            .unwrap()
            .1,
    )
    .unwrap();
    assert_eq!(queued["attempt_id"], json!(attempt_id));
    assert!(queued.get("notice_id").is_some());
    assert!(queued.get("loan_id").is_some());
    assert!(queued.get("action_id").is_some());
    assert!(queued.get("state_revision").is_some());
    assert!(queued.get("assignment_revision").is_some());
    assert_eq!(queued["destination"]["thread"], json!(exact_thread));
    assert!(queued.get("delivery").is_none());

    let saved_receipt = response.body["receipt"].clone();
    let duplicate = daemon.send_notice(&notice);
    assert_eq!(duplicate.status, 200, "{:?}", duplicate.body);
    assert_eq!(duplicate.body["receipt"], saved_receipt);
    assert_eq!(daemon.queue_calls().len(), 1);

    let mut conflicting = notice.clone();
    conflicting["payload"] = json!({
        "type": "attention_required",
        "reason": "different content for the same attempt",
    });
    let conflict = daemon.send_notice(&conflicting);
    assert_eq!(conflict.status, 409, "{:?}", conflict.body);
    assert_eq!(conflict.body["error"]["code"], "message_conflict");
    assert_eq!(daemon.queue_calls().len(), 1);

    let retargeted_attempt = DeliveryAttemptId::new();
    let mut retargeted = notice.clone();
    retargeted["attempt_id"] = json!(retargeted_attempt);
    retargeted["assignment_revision"] = json!(4);
    retargeted["destination"]["thread"] = json!(newer_thread);
    let retargeted_response = daemon.send_notice(&retargeted);
    assert_eq!(
        retargeted_response.status, 200,
        "{:?}",
        retargeted_response.body
    );
    assert_eq!(
        retargeted_response.body["receipt"]["notice_id"],
        notice["notice_id"]
    );
    assert_eq!(
        retargeted_response.body["receipt"]["attempt_id"],
        json!(retargeted_attempt)
    );
    assert_eq!(
        retargeted_response.body["receipt"]["destination_thread"],
        newer_thread.to_string()
    );
    assert!(daemon.queue_calls()[1].contains(&format!(
        "--thread {newer_thread} --message HOMEBASED_RESOURCE_NOTICE "
    )));

    let direct = request(
        MessageId::new(),
        daemon.machine_id(),
        json!({"kind": "thread", "thread": exact_thread}),
        "Keep direct message delivery unchanged",
    );
    let direct_response = daemon.send(&direct);
    assert_eq!(direct_response.status, 200, "{:?}", direct_response.body);
    let queue_calls = daemon.queue_calls();
    assert!(queue_calls[2].contains("HOMEBASED_MESSAGE "));
    assert!(!queue_calls[2].contains("HOMEBASED_RESOURCE_NOTICE "));

    let mut notice_source = request(
        MessageId::new(),
        daemon.machine_id(),
        json!({"kind": "thread", "thread": exact_thread}),
        "A notice source cannot use the direct-message route",
    );
    notice_source["source"] = json!({
        "kind": "resource_notice",
        "machine": daemon.machine_id(),
        "notice_id": NoticeId::new(),
    });
    let rejected_source = daemon.send(&notice_source);
    assert_eq!(rejected_source.status, 400, "{:?}", rejected_source.body);
    assert_eq!(rejected_source.body["error"]["code"], "message_invalid");
    assert_eq!(daemon.queue_calls().len(), 3);

    let mut unknown = notice_request(daemon.machine_id(), exact_thread, DeliveryAttemptId::new());
    unknown["unknown"] = json!(true);
    assert!((400..500).contains(&daemon.send_notice(&unknown).status));

    let invalid = notice_request(
        daemon.machine_id(),
        exact_thread,
        DeliveryAttemptId::from_uuid(Uuid::nil()),
    );
    assert_eq!(daemon.send_notice(&invalid).status, 400);
    assert_eq!(daemon.queue_calls().len(), 3);
}

#[test]
fn failed_notice_attempt_is_retained_and_same_attempt_can_retry() {
    let daemon = Daemon::start();
    let exact_cwd = daemon.user_home.join("exact-project");
    let other_cwd = daemon.user_home.join("other-project");
    fs::create_dir_all(&exact_cwd).unwrap();
    fs::create_dir_all(&other_cwd).unwrap();
    let exact_thread = ThreadId(Uuid::now_v7());
    let other_thread = ThreadId(Uuid::now_v7());
    write_session(
        &daemon.user_home,
        "failed-notice-exact",
        exact_thread,
        &exact_cwd,
    );
    write_session(
        &daemon.user_home,
        "failed-notice-other",
        other_thread,
        &other_cwd,
    );

    let attempt_id = DeliveryAttemptId::new();
    let request = notice_request(daemon.machine_id(), exact_thread, attempt_id);
    fs::write(&daemon.queue_fail_marker, "fail").unwrap();
    let failed = daemon.send_notice(&request);
    assert_eq!(failed.status, 503, "{:?}", failed.body);
    assert_eq!(failed.body["error"]["code"], "message_delivery_failed");
    let delivery = daemon.notice_delivery(attempt_id);
    assert!(delivery.attempt.is_some());
    assert!(delivery.receipt.is_none());
    assert_eq!(daemon.queue_calls().len(), 1);

    fs::remove_file(&daemon.queue_fail_marker).unwrap();
    let retried = daemon.send_notice(&request);
    assert_eq!(retried.status, 200, "{:?}", retried.body);
    assert_eq!(
        retried.body["receipt"]["destination_thread"],
        exact_thread.to_string()
    );
    assert_eq!(daemon.queue_calls().len(), 2);
    assert!(daemon.queue_calls()[1].contains(&format!(
        "--thread {exact_thread} --message HOMEBASED_RESOURCE_NOTICE "
    )));
    assert!(daemon.notice_delivery(attempt_id).receipt.is_some());
}
