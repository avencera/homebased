//! Sequenced task events between executor and origin

use super::{
    Daemon, add_peer, register_thread, route_state, seed_event, wait_for_probe, wait_until,
};
use homebased::domain::{ProcessStatus, TaskId};
use homebased::events::{EventPayload, EventRouteState};
use homebased::fleet::http::ClusterClient;
use homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
use homebased::store::Store;
use serde_json::Value;
use std::fs;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn sender_uses_real_receiver_and_recovers_a_sequence_gap() {
    let origin = Daemon::start("origin", true);
    let executor = Daemon::start("executor", true);
    wait_for_probe(&origin.address()).await;
    let task = TaskId::new();
    seed_event(&executor, &origin, task, true, 2);
    let store = Store::open(&executor.home.join(homebased::home::DB_NAME)).unwrap();
    store
        .mark_outbound_acknowledged(task, std::num::NonZeroU64::new(1).unwrap())
        .unwrap();
    add_peer(&executor, &origin);
    assert!(wait_until(Duration::from_secs(12), || {
        route_state(&executor, task).acknowledged == 2
    }));
    let inbox = Store::open(&origin.home.join(homebased::home::DB_NAME))
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
    let mut store = Store::open(&executor.home.join(homebased::home::DB_NAME)).unwrap();
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
    let event = Store::open(&executor.home.join(homebased::home::DB_NAME))
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
                "api_version": 1, "protocol_version": CLUSTER_PROTOCOL_VERSION,
                "destination_machine": origin.machine_id(), "event": event
            }),
        )
        .await
        .unwrap();
    assert_eq!(response.status.as_u16(), 200);
    let acknowledgement: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(acknowledgement["api_version"], 1);
    assert_eq!(
        acknowledgement["protocol_version"],
        CLUSTER_PROTOCOL_VERSION.0
    );
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
        Store::open(&origin.home.join(homebased::home::DB_NAME))
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
    let mut store = Store::open(&executor.home.join(homebased::home::DB_NAME)).unwrap();
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
        Store::open(&replacement.home.join(homebased::home::DB_NAME))
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
    let store = Store::open(&daemon.home.join(homebased::home::DB_NAME)).unwrap();
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
    register_thread(&daemon.user_home, "01a0ab97-a7aa-7463-a5b0-8d500e40e431");
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
        &daemon.home.join(homebased::home::DB_NAME)
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
        let Ok(store) = Store::open(&daemon.home.join(homebased::home::DB_NAME)) else {
            return false;
        };
        store
            .origin_route_by_task(task)
            .unwrap()
            .is_some_and(|route| route.last_settled_seq == 5)
            && fs::read_to_string(&callbacks).is_ok_and(|text| text.lines().count() == 2)
    });
    if !settled {
        let store = Store::open(&daemon.home.join(homebased::home::DB_NAME)).unwrap();
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
