//! Cross-machine cancellation and its durable receipts

use super::{
    Daemon, add_peer, inspect_cli, post_execution, remote_spec, seed_inspection_row,
    seed_origin_route, wait_until,
};
use homebased::cancellation::{CancellationRequestIdentity, ExecutorCancelState};
use homebased::domain::{ProcessStatus, TaskId};
use homebased::fleet::http::ClusterClient;
use homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
use homebased::store::Store;
use homebased::submission::{ExecutionRecord, RequestId};
use serde_json::Value;
use std::time::{Duration, Instant};

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
        Store::open(&executor.home.join(homebased::home::DB_NAME))
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
        Store::open(&executor.home.join(homebased::home::DB_NAME))
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
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_some_and(|row| row.status() == ProcessStatus::Running)
    }));
    origin.stop();

    let (ok, local_body) = inspect_cli(&executor, "cancel", task);
    assert!(!ok, "{local_body}");
    assert_eq!(local_body["error"]["code"], "cluster_lookup_incomplete");

    let (ok, body) = inspect_cli(&viewer, "cancel", task);
    assert!(!ok, "{body}");
    assert_eq!(body["error"]["code"], "cluster_lookup_incomplete");
    assert!(
        Store::open(&viewer.home.join(homebased::home::DB_NAME))
            .unwrap()
            .pending_cancellation_requests()
            .unwrap()
            .is_empty()
    );
    assert!(wait_until(Duration::from_secs(2), || {
        Store::open(&executor.home.join(homebased::home::DB_NAME))
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
        Store::open(&viewer.home.join(homebased::home::DB_NAME))
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
                "protocol_version": CLUSTER_PROTOCOL_VERSION,
                "request": request,
                "target": {"type": "execution", "request_id": RequestId::new()},
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
    let saved = Store::open(&origin.home.join(homebased::home::DB_NAME))
        .unwrap()
        .pending_cancellation_requests()
        .unwrap();
    assert_eq!(saved.len(), 1);
    let receipt = Store::open(&executor.home.join(homebased::home::DB_NAME))
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
        Store::open(&executor.home.join(homebased::home::DB_NAME))
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
    let receipt = Store::open(&executor.home.join(homebased::home::DB_NAME))
        .unwrap()
        .receive_cancellation(request)
        .unwrap();
    assert!(matches!(
        receipt.state,
        ExecutorCancelState::PendingApplication
    ));
    executor.restart();
    assert!(wait_until(Duration::from_secs(10), || {
        Store::open(&executor.home.join(homebased::home::DB_NAME))
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
    Store::open(&executor.home.join(homebased::home::DB_NAME))
        .unwrap()
        .accept_execution(&ExecutionRecord {
            task,
            origin_machine: first.machine_id(),
            execution_machine: executor.machine_id(),
            spec: remote_spec(&executor, vec!["/bin/true"]),
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
        Store::open(&executor.home.join(homebased::home::DB_NAME))
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
            &serde_json::json!({
                "api_version": 1,
                "protocol_version": 99,
                "request": request,
                "target": {"type": "execution", "request_id": RequestId::new()},
            }),
        )
        .await
        .unwrap();
    assert_ne!(response.status.as_u16(), 200);
    let error: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(error["api_version"], 1);
    assert_eq!(error["error"]["code"], "cluster_protocol_incompatible");
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .executor_identity(task)
            .unwrap()
            .is_none()
    );
}
