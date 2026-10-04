//! Held tasks that start after their dependencies succeed

use super::{Harness, event_json, wait_until};
use homebased::domain::TaskId;
use serde_json::{Value, json};
use std::fs;
use std::time::Duration;

impl Harness {
    /// Submit a spec file through the CLI and return the raw output
    fn submit_output(&self, spec: &Value, extra: &[&str]) -> std::process::Output {
        let spec_path = self.home.join(format!("spec-{}.json", TaskId::new()));
        fs::write(&spec_path, serde_json::to_vec(spec).unwrap()).unwrap();
        self.cmd()
            .args(["--json", "task", "submit"])
            .args(extra)
            .arg("--spec")
            .arg(&spec_path)
            .output()
            .unwrap()
    }

    /// Submit and return the response object, which must be a success
    fn submit_json(&self, spec: &Value, extra: &[&str]) -> Value {
        let out = self.submit_output(spec, extra);
        assert!(
            out.status.success(),
            "submit failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).unwrap()
    }

    /// Submit a spec that must fail and return its error object and exit code
    fn submit_error(&self, spec: &Value, extra: &[&str]) -> (Value, i32) {
        let out = self.submit_output(spec, extra);
        assert!(!out.status.success(), "submit unexpectedly succeeded");
        let error: Value = serde_json::from_slice(&out.stderr).unwrap();
        (error["error"].clone(), out.status.code().unwrap())
    }

    /// A task that waits until `gate` exists under the record directory, then exits with `code`
    fn gated_spec(&self, gate: &str, code: i32) -> Value {
        let gate = self.record.join(gate);
        Self::task_spec(&[
            "/bin/sh",
            "-c",
            &format!(
                "while [ ! -f '{}' ]; do sleep 0.1; done; exit {code}",
                gate.display()
            ),
        ])
    }

    fn open_gate(&self, gate: &str) {
        fs::write(self.record.join(gate), "").unwrap();
    }

    fn list(&self, extra: &[&str]) -> Vec<Value> {
        let out = self
            .cmd()
            .args(["--json", "task", "list"])
            .args(extra)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "list failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let value: Value = serde_json::from_slice(&out.stdout).unwrap();
        value["tasks"].as_array().unwrap().clone()
    }

    /// The delivered event of one kind for one task
    fn event(&self, id: &str, event: &str) -> Value {
        self.wait_for_event(id, event)
            .iter()
            .map(|line| event_json(line))
            .find(|payload| payload["task"] == id && payload["event"] == event)
            .unwrap()
    }
}

fn with_after(mut spec: Value, after: &[&str]) -> Value {
    spec["after"] = json!(after);
    spec
}

#[test]
fn held_task_and_its_chain_start_after_each_dependency_succeeds() {
    let h = Harness::new();
    let first = h.submit(&h.gated_spec("first", 0));

    // a dry run checks the dependencies and saves nothing
    let preview = h.submit_json(
        &with_after(Harness::task_spec(&["/bin/echo", "second"]), &[&first]),
        &["--dry-run"],
    );
    assert_eq!(
        preview["after"],
        json!([{ "task": first, "state": "pending" }])
    );

    let request = uuid::Uuid::now_v7().to_string();
    let second_spec = with_after(Harness::task_spec(&["/bin/echo", "second"]), &[&first]);
    let submitted = h.submit_json(&second_spec, &["--request-id", &request]);
    assert_eq!(submitted["status"], "held");
    let second = submitted["id"].as_str().unwrap().to_string();
    assert!(
        h.store()
            .get_task(second.parse().unwrap())
            .unwrap()
            .is_none()
    );

    // a held task may itself be a dependency
    let third = h.submit_json(
        &with_after(Harness::task_spec(&["/bin/echo", "third"]), &[&second]),
        &[],
    );
    assert_eq!(third["status"], "held");
    let third = third["id"].as_str().unwrap().to_string();

    let shown = h.show(&second);
    assert_eq!(shown["status"], "held");
    assert_eq!(shown["availability"], "held");
    assert_eq!(
        shown["after"],
        json!([{ "task": first, "state": "pending" }])
    );
    assert_eq!(shown["submission"]["phase"]["type"], "waiting");
    let held: Vec<_> = h
        .list(&["--status", "held"])
        .iter()
        .map(|task| task["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(held, vec![second.clone(), third.clone()]);
    assert!(
        h.list(&["--status", "running,queued"])
            .iter()
            .all(|task| task["status"] != "held")
    );
    let log = h
        .cmd()
        .args(["--json", "task", "log", &second])
        .output()
        .unwrap();
    let error: Value = serde_json::from_slice(&log.stderr).unwrap();
    assert_eq!(error["error"]["code"], "task_not_started");

    // a retry returns the held task; another dependency list is another request
    let retry = h.submit_json(&second_spec, &["--request-id", &request]);
    assert_eq!(retry["id"], second.as_str());
    assert_eq!(retry["status"], "held");
    let other = h.submit(&Harness::task_spec(&["/bin/echo", "done"]));
    let (error, code) = h.submit_error(
        &with_after(
            Harness::task_spec(&["/bin/echo", "second"]),
            &[&first, &other],
        ),
        &["--request-id", &request],
    );
    assert_eq!(error["code"], "submission_conflict", "{error}");
    assert_eq!(code, 5);

    h.open_gate("first");
    let second_done = h.wait_status(&second, "succeeded");
    assert_eq!(
        second_done["after"],
        json!([{ "task": first, "state": "ended", "outcome": "succeeded" }])
    );
    h.wait_status(&third, "succeeded");
    h.event(&second, "TASK_SUCCEEDED");
    h.event(&third, "TASK_SUCCEEDED");
}

#[test]
fn admission_refuses_unknown_and_failed_dependencies_and_runs_when_all_succeeded() {
    let h = Harness::new();
    let unknown = TaskId::new().to_string();
    let (error, code) = h.submit_error(
        &with_after(Harness::task_spec(&["/bin/echo", "done"]), &[&unknown]),
        &[],
    );
    assert_eq!(error["code"], "unknown_dependency", "{error}");
    assert_eq!(error["input"]["task"], unknown.as_str());
    assert_eq!(code, 3);
    let (error, _) = h.submit_error(
        &with_after(Harness::task_spec(&["/bin/echo", "done"]), &[]),
        &[],
    );
    assert_eq!(error["code"], "invalid_spec");
    assert_eq!(error["input"]["pointer"], "/after");

    let failed = h.submit(&Harness::task_spec(&["/bin/sh", "-c", "exit 3"]));
    h.event(&failed, "TASK_FAILED");
    let (error, code) = h.submit_error(
        &with_after(Harness::task_spec(&["/bin/echo", "done"]), &[&failed]),
        &["--dry-run"],
    );
    assert_eq!(error["code"], "dependency_failed", "{error}");
    assert_eq!(error["input"]["task"], failed.as_str());
    assert_eq!(error["input"]["outcome"], "failed");
    assert_eq!(code, 5);

    let succeeded = h.submit(&Harness::task_spec(&["/bin/echo", "done"]));
    h.event(&succeeded, "TASK_SUCCEEDED");
    let ready = h.submit_json(
        &with_after(Harness::task_spec(&["/bin/echo", "ready"]), &[&succeeded]),
        &[],
    );
    assert_eq!(ready["status"], "queued");
    let ready = ready["id"].as_str().unwrap();
    let shown = h.wait_status(ready, "succeeded");
    assert_eq!(
        shown["after"],
        json!([{ "task": succeeded, "state": "ended", "outcome": "succeeded" }])
    );
}

#[test]
fn failed_dependency_and_explicit_cancel_cascade_through_held_chains() {
    let h = Harness::new();
    let failing = h.submit(&h.gated_spec("failing", 1));
    let held = h.submit(&with_after(
        Harness::task_spec(&["/bin/echo", "never"]),
        &[&failing],
    ));
    let chained = h.submit(&with_after(
        Harness::task_spec(&["/bin/echo", "never"]),
        &[&held],
    ));
    h.open_gate("failing");

    let event = h.event(&held, "TASK_CANCELLED");
    assert_eq!(
        event["cancel_reason"],
        json!({ "type": "dependency_ended", "dependency": failing, "outcome": "failed" })
    );
    assert_eq!(event["process"], json!({ "kind": "cancelled" }));
    assert_eq!(event["reports"], json!([]));
    let event = h.event(&chained, "TASK_CANCELLED");
    assert_eq!(
        event["cancel_reason"],
        json!({ "type": "dependency_ended", "dependency": held, "outcome": "cancelled" })
    );
    let shown = h.show(&held);
    assert_eq!(shown["status"], "cancelled");
    assert_eq!(shown["availability"], "not_started");
    assert_eq!(shown["last_event"]["event"], "TASK_CANCELLED");
    for id in [&held, &chained] {
        assert!(h.store().get_task(id.parse().unwrap()).unwrap().is_none());
    }

    let running = h.submit(&h.gated_spec("running", 0));
    let cancelled = h.submit(&with_after(
        Harness::task_spec(&["/bin/echo", "never"]),
        &[&running],
    ));
    let downstream = h.submit(&with_after(
        Harness::task_spec(&["/bin/echo", "never"]),
        &[&cancelled],
    ));
    let out = h
        .cmd()
        .args(["--json", "task", "cancel", &cancelled])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let response: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(response["status"], "cancelled");
    let event = h.event(&cancelled, "TASK_CANCELLED");
    assert_eq!(event["cancel_reason"], json!({ "type": "requested" }));
    let event = h.event(&downstream, "TASK_CANCELLED");
    assert_eq!(event["cancel_reason"]["dependency"], cancelled.as_str());
    // cancelling again changes nothing and the dependency keeps running
    let again = h
        .cmd()
        .args(["--json", "task", "cancel", &cancelled])
        .output()
        .unwrap();
    assert!(again.status.success());
    assert_eq!(h.show(&running)["status"], "running");
    h.open_gate("running");
    h.wait_status(&running, "succeeded");
}

#[test]
fn restart_applies_dependency_endings_that_arrived_while_the_daemon_was_down() {
    let mut h = Harness::new();
    let succeeding = h.submit(&h.gated_spec("succeeding", 0));
    let failing = h.submit(&h.gated_spec("failing", 1));
    let released = h.submit(&with_after(
        Harness::task_spec(&["/bin/echo", "released"]),
        &[&succeeding],
    ));
    let cancelled = h.submit(&with_after(
        Harness::task_spec(&["/bin/echo", "never"]),
        &[&failing],
    ));
    h.wait_status(&succeeding, "running");
    h.wait_status(&failing, "running");

    h.stop_daemon();
    h.open_gate("succeeding");
    h.open_gate("failing");
    // workers outlive the daemon and finish while it is down
    let store = h.store();
    assert!(wait_until(Duration::from_secs(20), || {
        [&succeeding, &failing].iter().all(|id| {
            store
                .get_task(id.parse().unwrap())
                .unwrap()
                .is_some_and(|row| row.status().is_terminal())
        })
    }));
    let saved = store
        .unlaunched_task(released.parse().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&saved.held.route.submission).unwrap(),
        json!({ "type": "held", "phase": { "type": "waiting" } })
    );
    drop(store);

    h.start_daemon();
    h.wait_status(&released, "succeeded");
    let event = h.event(&cancelled, "TASK_CANCELLED");
    assert_eq!(event["cancel_reason"]["dependency"], failing.as_str());
}
