//! Direct and worker messages between machines

use super::{
    Daemon, dependency_fleet, gate_command, post_message, remote_spec, submit_after, submit_file,
};
use homebased::domain::{TaskId, ThreadId};
use homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
use homebased::machine::MachineId;
use homebased::message::MessageId;
use homebased::store::Store;
use homebased::submission::RequestId;
use serde_json::Value;
use std::fs;

#[tokio::test(flavor = "multi_thread")]
async fn direct_message_retry_reuses_receipt() {
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
    let request = serde_json::json!({
        "api_version": 1,
        "protocol_version": CLUSTER_PROTOCOL_VERSION,
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
    let first_attempt = Store::open(&receiver.home.join(homebased::home::DB_NAME))
        .unwrap()
        .message_delivery(message_id)
        .unwrap();
    assert_eq!(
        first_attempt.attempt.unwrap().request.protocol_version,
        CLUSTER_PROTOCOL_VERSION.0
    );
    assert!(first_attempt.receipt.is_none());

    fs::remove_file(fail_marker).unwrap();
    let (status, delivered) = post_message(&receiver, &request).await;
    assert_eq!(status, 200, "{delivered}");
    assert_eq!(delivered["protocol_version"], CLUSTER_PROTOCOL_VERSION.0);
    assert_eq!(
        delivered["receipt"]["protocol_version"],
        CLUSTER_PROTOCOL_VERSION.0
    );
    let delivery = Store::open(&receiver.home.join(homebased::home::DB_NAME))
        .unwrap()
        .message_delivery(message_id)
        .unwrap();
    assert_eq!(
        delivery.attempt.unwrap().request.protocol_version,
        CLUSTER_PROTOCOL_VERSION.0
    );
    assert_eq!(
        delivery.receipt.unwrap().protocol_version,
        CLUSTER_PROTOCOL_VERSION.0
    );

    let (status, retried) = post_message(&receiver, &request).await;
    assert_eq!(status, 200, "{retried}");
    assert_eq!(retried["receipt"], delivered["receipt"]);
    let saved = Store::open(&receiver.home.join(homebased::home::DB_NAME))
        .unwrap()
        .message_delivery(message_id)
        .unwrap();
    assert_eq!(
        saved.receipt.unwrap().protocol_version,
        CLUSTER_PROTOCOL_VERSION.0
    );
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
        Store::open(&receiver.home.join(homebased::home::DB_NAME))
            .unwrap()
            .message_delivery(unsupported_id)
            .unwrap()
            .attempt
            .is_none()
    );
}

/// Send a worker message from `sender` and return its error body
fn worker_message_error(sender: &Daemon, task: TaskId) -> Value {
    let task = task.to_string();
    let id = MessageId::new().to_string();
    let output = sender
        .cmd()
        .args([
            "--json",
            "message",
            "send",
            "--worker",
            &task,
            "--message",
            "Also update the changelog",
            "--message-id",
            &id,
            "--source-thread",
            "018f0a48-f0ef-7d12-8f01-000000000001",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    serde_json::from_slice(&output.stderr).unwrap()
}

#[test]
fn worker_message_from_another_machine_explains_a_held_task() {
    let (origin, executor) = dependency_fleet("held-message");
    let gate = origin.user_home.join("gate");
    let mut dependency_spec = remote_spec(&executor, vec!["/bin/sh", "-c", &gate_command(&gate)]);
    dependency_spec.cwd = origin.user_home.clone();
    let dependency = submit_file(&origin, &dependency_spec, RequestId::new());
    assert!(
        dependency.status.success(),
        "{}",
        String::from_utf8_lossy(&dependency.stderr)
    );
    let dependency: TaskId = serde_json::from_slice::<Value>(&dependency.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let mut held_spec = remote_spec(&executor, vec!["/bin/sh", "-c", "true"]);
    held_spec.cwd = origin.user_home.clone();
    let held: TaskId = submit_after(&origin, &held_spec, &[dependency])["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    // only the origin's route knows the task, and no worker exists while it is held
    let waiting = worker_message_error(&executor, held);
    assert_eq!(waiting["error"]["code"], "worker_message_unavailable");
    assert_eq!(waiting["error"]["input"]["reason"], "no_worker_thread");

    let cancelled = origin
        .cmd()
        .args(["--json", "task", "cancel", &held.to_string()])
        .output()
        .unwrap();
    assert!(
        cancelled.status.success(),
        "{}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    let ended = worker_message_error(&executor, held);
    assert_eq!(ended["error"]["code"], "worker_message_unavailable");
    assert_eq!(ended["error"]["input"]["reason"], "terminal");

    fs::write(&gate, "").unwrap();
}
