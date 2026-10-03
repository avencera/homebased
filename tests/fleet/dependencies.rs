//! Held remote tasks that start after their dependencies succeed

use super::{
    Daemon, cli_remote_spec, dependency_fleet, gate_command, remote_spec, submit_after,
    submit_file, wait_until,
};
use homebased::domain::TaskId;
use homebased::store::Store;
use homebased::submission::{ExecutionRecord, OriginRoute, RequestId, SubmissionState};
use serde_json::Value;
use std::fs;
use std::time::Duration;

fn origin_route(origin: &Daemon, task: TaskId) -> OriginRoute {
    Store::open(&origin.home.join(homebased::home::DB_NAME))
        .unwrap()
        .origin_route_by_task(task)
        .unwrap()
        .unwrap()
}

fn accepted_on_executor(executor: &Daemon, task: TaskId) -> Option<ExecutionRecord> {
    match Store::open(&executor.home.join(homebased::home::DB_NAME))
        .unwrap()
        .executor_identity(task)
        .unwrap()
    {
        Some(homebased::submission::ExecutorIdentity::Accepted(record)) => Some(record),
        _ => None,
    }
}

#[test]
fn held_remote_task_launches_after_its_remote_dependency_succeeds() {
    let (origin, executor) = dependency_fleet("dependency");
    let gate = executor.user_home.join("gate");
    let dependency_spec = cli_remote_spec(&executor, vec!["/bin/sh", "-c", &gate_command(&gate)]);
    let dependency = submit_file(&origin, &dependency_spec, RequestId::new());
    assert!(dependency.status.success());
    let dependency: TaskId = serde_json::from_slice::<Value>(&dependency.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let held_spec = cli_remote_spec(
        &executor,
        vec!["/bin/sh", "-c", "echo released >> \"$HOME/released\""],
    );
    let held = submit_after(&origin, &held_spec, &[dependency]);
    assert_eq!(held["status"], "held");
    let held: TaskId = held["id"].as_str().unwrap().parse().unwrap();
    assert!(accepted_on_executor(&executor, held).is_none());

    fs::write(&gate, "").unwrap();
    assert!(
        wait_until(Duration::from_secs(30), || executor
            .user_home
            .join("released")
            .exists()),
        "held task never ran on its executor"
    );
    // the executor got the saved normalized spec under the pre-assigned task UUID
    let record = accepted_on_executor(&executor, held).unwrap();
    let route = origin_route(&origin, held);
    assert_eq!(record.spec, route.spec);
    let wire = serde_json::to_value(&record.spec).unwrap();
    assert!(wire.get("after").is_none());
    assert!(wait_until(Duration::from_secs(10), || matches!(
        origin_route(&origin, held).submission,
        SubmissionState::Accepted
    )));
}

#[test]
fn held_remote_task_waits_for_an_unreachable_executor_then_launches() {
    let (origin, mut executor) = dependency_fleet("unreachable");
    let gate = origin.user_home.join("gate");
    let mut local_spec = remote_spec(&executor, vec!["/bin/sh", "-c", &gate_command(&gate)]);
    local_spec.cwd = origin.user_home.clone();
    let dependency = submit_file(&origin, &local_spec, RequestId::new());
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
    let held_spec = cli_remote_spec(
        &executor,
        vec!["/bin/sh", "-c", "echo released >> \"$HOME/released\""],
    );
    let held: TaskId = submit_after(&origin, &held_spec, &[dependency])["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    executor.stop();
    fs::write(&gate, "").unwrap();
    assert!(wait_until(Duration::from_secs(10), || {
        Store::open(&origin.home.join(homebased::home::DB_NAME))
            .unwrap()
            .dependency_states(&[dependency])
            .unwrap()
            == vec![(
                dependency,
                Some(homebased::dependency::DependencyState::Ended(
                    homebased::dependency::TaskOutcome::Succeeded.into(),
                )),
            )]
    }));
    std::thread::sleep(Duration::from_secs(2));
    // the release cannot reach the executor, so the task stays held and cancellable here
    assert_eq!(
        serde_json::to_value(origin_route(&origin, held).submission).unwrap(),
        serde_json::json!({ "type": "held", "phase": { "type": "waiting" } })
    );

    executor.spawn();
    assert!(
        wait_until(Duration::from_secs(40), || executor
            .user_home
            .join("released")
            .exists()),
        "held task never ran after its executor came back"
    );
    assert!(accepted_on_executor(&executor, held).is_some());
}
