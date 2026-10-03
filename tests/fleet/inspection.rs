//! Task inspection across machines: local rows, routes, and proxies

use super::{
    Daemon, add_peer, inspect_cli, post_execution, remote_spec, seed_inspection_row,
    seed_origin_route, wait_for_probe, wait_until,
};
use homebased::domain::{ProcessStatus, TaskId};
use homebased::events::EventPayload;
use homebased::fleet::http::ClusterClient;
use homebased::store::Store;
use homebased::submission::{ExecutionRecord, SubmissionState};
use serde_json::Value;
use std::fs;
use std::time::Duration;

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
    Store::open(&origin.home.join(homebased::home::DB_NAME))
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
    Store::open(&origin.home.join(homebased::home::DB_NAME))
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
    Store::open(&executor.home.join(homebased::home::DB_NAME))
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
    Store::open(&executor.home.join(homebased::home::DB_NAME))
        .unwrap()
        .accept_execution(&ExecutionRecord {
            task: accepted,
            origin_machine: viewer.machine_id(),
            execution_machine: executor.machine_id(),
            spec,
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
