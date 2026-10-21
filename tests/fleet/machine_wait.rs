//! Remote tasks that wait on their origin for an offline machine, without a thread

use super::{Daemon, cli_remote_spec, dependency_fleet, inspect_cli, wait_until};
use homebased::dependency::HeldCancellation;
use homebased::domain::TaskId;
use homebased::store::Store;
use homebased::submission::{HeldPhase, OriginRoute, RequestId, SubmissionState};
use serde_json::{Value, json};
use std::fs;
use std::time::Duration;

fn origin_route(origin: &Daemon, task: TaskId) -> OriginRoute {
    Store::open(&origin.home.join(homebased::home::DB_NAME))
        .unwrap()
        .origin_route_by_task(task)
        .unwrap()
        .unwrap()
}

/// Submit a thread-less command that waits for the executor under one coalesce key
fn submit_waiting(origin: &Daemon, executor: &Daemon, label: &str) -> Value {
    let command = format!("echo {label} >> \"$HOME/ran\"");
    let spec = cli_remote_spec(executor, vec!["/bin/sh", "-c", &command]);
    let mut value = serde_json::to_value(spec).unwrap();
    let object = value.as_object_mut().unwrap();
    object.remove("thread");
    object.insert("wait_for_machine".into(), json!(true));
    object.insert("coalesce".into(), json!("fleet-update"));
    let path = origin._dir.path().join(format!("wait-{label}.json"));
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    let output = origin
        .cmd()
        .current_dir(&origin.user_home)
        .args(["--json", "task", "submit", "--spec"])
        .arg(&path)
        .args(["--request-id", &RequestId::new().0.to_string()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn task_id(response: &Value) -> TaskId {
    response["id"].as_str().unwrap().parse().unwrap()
}

#[test]
fn a_coalesced_task_waits_for_its_machine_and_replaces_the_older_wait() {
    let (origin, mut executor) = dependency_fleet("machine-wait");
    executor.stop();

    let first = submit_waiting(&origin, &executor, "first");
    assert_eq!(first["status"], "held");
    let first = task_id(&first);
    let second = submit_waiting(&origin, &executor, "second");
    assert_eq!(second["status"], "held");
    let second = task_id(&second);

    // the newer submit replaced the older one before either launched
    assert_eq!(
        origin_route(&origin, first).submission,
        SubmissionState::Held {
            phase: HeldPhase::Cancelled {
                cause: HeldCancellation::Superseded { by: second }
            }
        }
    );
    let (found, shown) = inspect_cli(&origin, "show", second);
    assert!(found, "{shown}");
    assert_eq!(shown["status"], "held");
    assert_eq!(shown["wait_for_machine"], true);
    assert_eq!(shown["coalesce"], "fleet-update");

    executor.spawn();
    assert!(
        wait_until(Duration::from_secs(60), || {
            // the origin settled every event of the finished task without a session
            let route = origin_route(&origin, second);
            route
                .last_execution_state
                .is_some_and(|state| state.is_terminal())
                && route.last_settled_seq == route.last_accepted_seq
        }),
        "waiting task never finished after its machine came back: {} {:?}",
        inspect_cli(&origin, "show", second).1,
        fs::read_to_string(executor.user_home.join("ran"))
    );
    assert_eq!(
        inspect_cli(&origin, "show", second).1["status"],
        "succeeded"
    );
    let ran = fs::read_to_string(executor.user_home.join("ran")).unwrap();
    assert_eq!(ran, "second\n");

    // with its machine up, the same submit starts at once instead of holding
    let third = submit_waiting(&origin, &executor, "third");
    assert_ne!(third["status"], "held", "{third}");
    assert!(
        wait_until(Duration::from_secs(30), || fs::read_to_string(
            executor.user_home.join("ran")
        )
        .is_ok_and(|ran| ran == "second\nthird\n")),
        "a task for a reachable machine did not run"
    );
    // no task had a thread, so no agent was woken for any ending
    assert!(!origin.user_home.join("callbacks").exists());
}
