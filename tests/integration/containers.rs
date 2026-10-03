//! Container tasks against a real Docker Engine

use super::{Harness, THREAD, process_is_live, wait_until};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

/// Real Docker CLI and the ID of a pulled busybox image
fn docker_image() -> (PathBuf, String) {
    let docker = which::which("docker").expect("these tests need the docker CLI on PATH");
    let run = |args: &[&str]| {
        let output = Command::new(&docker).args(args).output().unwrap();
        assert!(
            output.status.success(),
            "docker {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    run(&["pull", "--quiet", "busybox:1.37"]);
    let id = run(&["image", "inspect", "--format", "{{.Id}}", "busybox:1.37"]);
    (docker, id.trim().to_owned())
}

fn container_spec(image: &str, script: &str) -> Value {
    json!({
        "api_version": 1,
        "thread": THREAD,
        "name": "docker container test",
        "cwd": std::env::temp_dir(),
        "timeout": "30m",
        "workload": {
            "type": "container",
            "image": image,
            "entrypoint": ["sh", "-c", script],
            "memory": "64m"
        }
    })
}

fn container_status(docker: &Path, id: &str) -> Option<String> {
    let output = Command::new(docker)
        .args(["container", "inspect", "--format", "{{.State.Status}}", id])
        .output()
        .unwrap();
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn confirmed_container(show: &Value, exit_code: i64) -> String {
    let evidence = &show["container"]["exit_evidence"];
    assert_eq!(evidence["type"], "confirmed", "{show}");
    assert_eq!(evidence["exit_code"], exit_code, "{show}");
    evidence["container_id"].as_str().unwrap().to_owned()
}

#[test]
#[ignore = "requires Docker Engine; run with just test-docker"]
fn docker_container_task_reports_its_exit_code_and_removes_its_container() {
    let (docker, image) = docker_image();
    let h = Harness::new();
    let id = h.submit(&container_spec(
        &image,
        "echo hello from the container; exit 5",
    ));
    let show = h.wait_status(&id, "failed");
    assert_eq!(show["exit_reason"], json!({ "kind": "exit", "code": 5 }));
    let container = confirmed_container(&show, 5);
    assert_eq!(container_status(&docker, &container), None);
    let log = fs::read_to_string(h.output_log(&id)).unwrap();
    assert!(log.contains("hello from the container"), "{log}");
}

#[test]
#[ignore = "requires Docker Engine; run with just test-docker"]
fn docker_cancel_stops_and_removes_the_container() {
    let (docker, image) = docker_image();
    let h = Harness::new();
    let id = h.submit(&container_spec(
        &image,
        "trap 'exit 7' TERM; sleep 60 & wait",
    ));
    assert!(wait_until(Duration::from_secs(30), || {
        h.show(&id)["container"]["started_at"].is_string()
    }));
    let out = h
        .cmd()
        .args(["--json", "task", "cancel", &id])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let show = h.wait_status(&id, "cancelled");
    let container = confirmed_container(&show, 7);
    assert_eq!(container_status(&docker, &container), None);
}

#[test]
#[ignore = "requires Docker Engine; run with just test-docker"]
fn docker_killed_daemon_and_worker_adopt_the_running_container() {
    let (docker, image) = docker_image();
    let mut h = Harness::new();
    let id = h.submit(&container_spec(
        &image,
        "echo before the restart; sleep 8; echo after the restart",
    ));
    assert!(wait_until(Duration::from_secs(30), || {
        h.show(&id)["container"]["started_at"].is_string()
    }));
    let show = h.show(&id);
    let container = show["container"]["container_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let worker = show["pid"].as_i64().unwrap() as i32;

    // a daemon restart under a service manager can take the worker with it
    h.stop_daemon();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(worker),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    assert!(wait_until(Duration::from_secs(5), || !process_is_live(
        worker
    )));
    assert_eq!(
        container_status(&docker, &container).as_deref(),
        Some("running"),
        "the container outlives its client and worker"
    );

    h.restart_daemon();
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(confirmed_container(&show, 0), container);
    assert_eq!(container_status(&docker, &container), None);
    let log = fs::read_to_string(h.output_log(&id)).unwrap();
    assert!(log.contains("adopting container"), "{log}");
    assert!(log.contains("after the restart"), "{log}");
    let msgs = h.wait_for_event(&id, "TASK_SUCCEEDED");
    assert_eq!(msgs.len(), 1, "{msgs:?}");
}
