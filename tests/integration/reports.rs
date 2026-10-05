//! Task reports and their callbacks

use super::{Harness, event_json, wait_until};
use serde_json::{Value, json};
use std::fs;
use std::process::Stdio;
use std::time::Duration;

#[test]
fn report_variants() {
    let h = Harness::new();

    // an agent told to report that exits 0 without one did not finish its work
    let id = h.submit(&Harness::spec("claude", "no report"));
    h.wait_status(&id, "succeeded");
    let msgs = h.wait_for_event(&id, "TASK_FAILED");
    let ev = event_json(&msgs[0]);
    assert_eq!(ev["reports"], json!([]));
    assert_eq!(ev["process"], json!({"kind": "exit", "code": 0}));
    assert_eq!(ev["reason"], "no_report");
    assert_eq!(ev["next_action"], "inspect_log");
    let show = h.show(&id);
    assert_eq!(show["last_event"]["event"], "TASK_FAILED");
    assert_eq!(show["last_event"]["reason"], "no_report");

    // without the trailer, and for a command, exit 0 alone is success
    let mut silent = Harness::spec("claude", "no trailer");
    silent["workload"]["report_trailer"] = json!(false);
    let id = h.submit(&silent);
    let msgs = h.wait_for_event(&id, "TASK_SUCCEEDED");
    let ev = event_json(
        msgs.iter()
            .rev()
            .find(|m| event_json(m)["task"] == id)
            .unwrap(),
    );
    assert!(ev.get("reason").is_none(), "{ev}");
    let id = h.submit(&Harness::task_spec(&["true"]));
    h.wait_for_event(&id, "TASK_SUCCEEDED");

    let spec_path = h.home.join("spec-r.json");
    let spec = Harness::spec("claude", "reports");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("report", "blocked\nNeed the DB name.");
    h.set_control("report2", "succeeded\nFound it.");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(show["reports"].as_array().unwrap().len(), 2);
    let msgs = h.wait_for_event(&id, "TASK_SUCCEEDED");
    let ev = event_json(msgs.last().unwrap());
    assert_eq!(ev["event"], "TASK_SUCCEEDED");
    assert_eq!(ev["reports"].as_array().unwrap().len(), 2);

    let spec_path = h.home.join("spec-n.json");
    fs::write(
        &spec_path,
        serde_json::to_vec(&Harness::spec("claude", "notify")).unwrap(),
    )
    .unwrap();
    h.clear_controls();
    h.set_control("report", "blocked\nblocked now");
    h.set_control("notify", "");
    let before = h.queue_messages().len();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    h.wait_status(&id, "succeeded");
    let msgs = h.wait_for_event(&id, "TASK_BLOCKED");
    let new: Vec<_> = msgs.iter().skip(before).cloned().collect();
    assert_eq!(new.len(), 2, "{new:?}");
    let first = event_json(&new[0]);
    let second = event_json(&new[1]);
    assert_eq!(first["event"], "TASK_REPORTED");
    assert!(first["process"].is_null());
    assert_eq!(second["event"], "TASK_BLOCKED");

    let out = h
        .cmd()
        .args([
            "--json",
            "task",
            "report",
            "--id",
            &id,
            "--outcome",
            "succeeded",
            "--summary",
            "late",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(5));

    let stdin = h.agent_stdin(&id);
    assert!(stdin.starts_with("notify"), "{stdin}");
    assert!(stdin.contains("--- homebased ---"), "{stdin}");
}

#[test]
fn report_with_daemon_stopped_and_trailer_off() {
    let mut h = Harness::new();
    let mut spec = Harness::spec("claude", "keep running");
    spec["workload"]["report_trailer"] = json!(false);
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "8");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    h.stop_daemon();
    let out = h
        .cmd()
        .args([
            "--json",
            "task",
            "report",
            "--id",
            &id,
            "--outcome",
            "succeeded",
            "--summary",
            "from worker while daemon down",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let feed = h.home.join("tasks").join(&id).join("prompt.txt");
    let prompt = fs::read_to_string(feed).unwrap();
    assert_eq!(prompt, "keep running");
}

#[test]
fn callback_failure_fallback() {
    let h = Harness::new();
    let spec_path = h.home.join("spec.json");
    fs::write(
        &spec_path,
        serde_json::to_vec(&Harness::spec("claude", "fail queue")).unwrap(),
    )
    .unwrap();
    h.set_control("queue-fails", "3");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    h.wait_status(&id, "succeeded");
    assert!(
        wait_until(Duration::from_secs(10), || {
            h.show(&id)["callback"] == "failed"
        }),
        "callback={}",
        h.show(&id)["callback"]
    );
    let show = h.show(&id);
    assert_eq!(show["callback"], "failed");
    let fallback = fs::read_to_string(h.home.join("callback-fallback.log")).unwrap();
    assert!(fallback.contains("HOMEBASED_EVENT"), "{fallback}");
}

#[test]
fn summary_too_long() {
    let h = Harness::new();
    let id = h.submit(&Harness::spec("claude", "x"));
    h.wait_status(&id, "succeeded");
    // submit a running task to report against
    let spec = Harness::spec("claude", "running");
    let spec_path = h.home.join("spec2.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "15");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let long = "x".repeat(4097);
    let out = h
        .cmd()
        .args([
            "--json",
            "task",
            "report",
            "--id",
            &id,
            "--outcome",
            "succeeded",
            "--summary",
            &long,
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn report_reads_summary_from_stdin() {
    let h = Harness::new();
    let spec = Harness::spec("claude", "running");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "20");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));

    let mut child = h
        .cmd()
        .args([
            "--json",
            "task",
            "report",
            "--id",
            &id,
            "--outcome",
            "blocked",
            "--summary-file",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write;
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"summary read from stdin")
            .unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["seq"], 1);
    assert_eq!(value["reports"][0]["summary"], "summary read from stdin");
    assert_eq!(value["reports"][0]["outcome"], "blocked");
}

#[test]
fn report_without_id_or_env_is_usage_error() {
    let h = Harness::new();
    let out = h
        .cmd()
        .env_remove("HOMEBASED_TASK_ID")
        .args([
            "task",
            "report",
            "--outcome",
            "succeeded",
            "--summary",
            "orphan",
        ])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("HOMEBASED_TASK_ID"), "{stderr}");
}
