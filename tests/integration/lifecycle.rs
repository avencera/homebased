//! Task lifecycle across daemon restarts, signals, cancels, inactivity checks, and cleanup

use super::{Harness, THREAD, event_json, fixture, wait_until};
use homebased::domain::{
    Agent, AgentKind, AgentWorkload, ExitReason, ProcessStatus, TaskEnv, TaskId, Workload,
};
use homebased::store::{NewTask, new_queued_task};
use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn daemon_restart_keeps_worker() {
    let mut h = Harness::new();
    let spec = Harness::spec("claude", "sleeping");
    h.set_control("sleep", "8");
    h.set_control("report", "succeeded\nSlept.");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || {
        h.show(&id)["status"] == "running"
    }));
    let pid = h.show(&id)["pid"].as_i64().unwrap() as u32;
    assert!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok(),
        "worker pid {pid} should be alive"
    );
    h.restart_daemon();
    assert!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok(),
        "worker should survive serve restart"
    );
    h.wait_status(&id, "succeeded");
    let msgs = h.wait_for_event(&id, "TASK_SUCCEEDED");
    assert_eq!(msgs.len(), 1, "{msgs:?}");
}

/// Environment of another process, as `KEY=VALUE` entries
fn process_environment(pid: i32) -> Vec<String> {
    #[cfg(target_os = "linux")]
    {
        fs::read(format!("/proc/{pid}/environ"))
            .unwrap()
            .split(|byte| *byte == 0)
            .map(|entry| String::from_utf8_lossy(entry).into_owned())
            .collect()
    }
    #[cfg(target_os = "macos")]
    {
        // `ps -E` appends the environment to the command; a value with spaces
        // splits, which these checks tolerate
        let out = std::process::Command::new("ps")
            .args(["-E", "-ww", "-o", "command=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .map(str::to_owned)
            .collect()
    }
}

#[test]
fn inherited_run_markers_reach_neither_the_worker_nor_the_task_child() {
    const INHERITED: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e431";
    let mut h = Harness::new();
    h.restart_daemon_with_env(&[
        ("HOMEBASED_TASK_ID", INHERITED),
        ("HOMEBASED_JOB_ID", INHERITED),
        ("HOMEBASED_YIELD_FILE", "/tmp/inherited-yield"),
    ]);
    let id = h.submit(&Harness::task_spec(&["/bin/sh", "-c", "env; sleep 5"]));
    let running = h.wait_status(&id, "running");
    let worker = running["pid"].as_i64().unwrap() as i32;
    assert!(wait_until(Duration::from_secs(10), || {
        fs::read_to_string(h.output_log(&id)).is_ok_and(|log| log.contains("HOMEBASED_TASK_ID="))
    }));

    // only Homebased entries are kept, so a failure does not print the
    // environment's other values into test logs
    let worker_env: Vec<String> = process_environment(worker)
        .into_iter()
        .filter(|entry| entry.starts_with("HOMEBASED_"))
        .collect();
    assert!(
        worker_env
            .iter()
            .any(|entry| entry.starts_with("HOMEBASED_HOME=")),
        "configuration must still reach the worker: {worker_env:?}"
    );
    assert!(
        !worker_env
            .iter()
            .any(|entry| entry.starts_with("HOMEBASED_TASK_ID=")
                || entry.starts_with("HOMEBASED_JOB_ID=")
                || entry.starts_with("HOMEBASED_YIELD_FILE=")),
        "inherited markers reached the worker: {worker_env:?}"
    );

    let log = fs::read_to_string(h.output_log(&id)).unwrap();
    let child_env: Vec<&str> = log.lines().collect();
    assert!(child_env.contains(&format!("HOMEBASED_TASK_ID={id}").as_str()));
    assert!(
        child_env
            .iter()
            .any(|line| line.starts_with("HOMEBASED_HOME="))
    );
    assert!(
        !child_env
            .iter()
            .any(|line| line.starts_with("HOMEBASED_JOB_ID=")
                || line.starts_with("HOMEBASED_YIELD_FILE=")),
        "inherited markers reached the child"
    );

    let task: homebased::domain::TaskId = id.parse().unwrap();
    let child = h
        .store()
        .require_task(task)
        .unwrap()
        .child
        .expect("the worker records its child at spawn");
    assert_ne!(child.pid.as_raw(), worker);
    assert_eq!(
        homebased::cleanup::process_identity(child.pid).unwrap(),
        child,
        "the recorded identity is the live child's"
    );

    let cancel = h.cmd().args(["task", "cancel", &id]).output().unwrap();
    assert!(cancel.status.success());
    h.wait_status(&id, "cancelled");
}

#[cfg(target_os = "linux")]
#[test]
fn kill9_worker_marks_lost() {
    let h = Harness::new();
    let worker_thread = "01a0e487-b877-76e2-9dc2-806bff0bf685";
    h.set_control("stdout", &format!("session id: {worker_thread}\n"));
    let spec = Harness::spec("codex", "hold");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "20");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let agent_pid = h.agent_pid(&id);
    let pid = h.show(&id)["pid"].as_i64().unwrap() as i32;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    let lost = h.wait_status(&id, "lost");
    assert_eq!(lost["worker_thread"], worker_thread);
    let msgs = h.wait_for_event(&id, "TASK_LOST");
    assert_eq!(msgs.len(), 1, "{msgs:?}");
    let ev = event_json(&msgs[0]);
    assert_eq!(ev["event"], "TASK_LOST");
    assert_eq!(ev["process"]["kind"], "runner_lost");
    let gone = wait_until(Duration::from_secs(5), || {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(agent_pid), None).is_err()
    });
    assert!(gone, "fake agent {agent_pid} outlived the killed worker");
}

#[test]
fn attention_reminder_and_cancel() {
    let mut h = Harness::new();
    h.set_control("sleep", "12");
    h.set_control("report", "succeeded\nPaid attention.");
    // empty output.log is not activity, so a backdated created_at is overdue
    let id = h.submit(&Harness::spec("claude", "attention me"));
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let before = h.queue_messages().len();
    h.backdate_created_at(&id, 3);
    // re-arm from persisted created_at; overdue deadline fires immediately
    h.restart_daemon();
    assert!(
        wait_until(Duration::from_secs(10), || {
            h.queue_messages()
                .iter()
                .skip(before)
                .any(|m| event_json(m)["event"] == "TASK_CHECK_DUE")
        }),
        "no TASK_CHECK_DUE after backdate+restart: {:?}",
        h.queue_messages()
    );
    let check = h
        .queue_messages()
        .iter()
        .skip(before)
        .map(|m| event_json(m))
        .find(|ev| ev["event"] == "TASK_CHECK_DUE")
        .unwrap();
    assert!(check["process"].is_null(), "{check}");
    assert_eq!(check["next_action"], "inspect_task");
    assert_eq!(check["timeout_secs"], 2 * 3600);
    assert_eq!(
        check["workload"],
        json!({"type": "agent", "agent": "claude", "model": "fable"})
    );
    assert_eq!(h.show(&id)["status"], "running", "child must keep running");
    assert!(
        wait_until(Duration::from_secs(5), || h.show(&id)["check_timeout"]
            == "sent"),
        "check timeout stayed pending after TASK_CHECK_DUE: {}",
        h.show(&id)
    );
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(show["status"], "succeeded");
    let msgs = h.wait_for_event(&id, "TASK_SUCCEEDED");
    let terminal = msgs
        .iter()
        .skip(before)
        .map(|m| event_json(m))
        .find(|ev| ev["event"] == "TASK_SUCCEEDED")
        .expect("terminal event after check due");
    assert_eq!(terminal["task"], id);

    h.clear_controls();
    h.set_control("sleep", "20");
    let id = h.submit(&Harness::spec("claude", "cancel me"));
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let agent_pid = h.agent_pid(&id);
    let out = h.cmd().args(["task", "cancel", &id]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    h.wait_status(&id, "cancelled");
    let msgs = h.wait_for_event(&id, "TASK_CANCELLED");
    let last = event_json(msgs.last().unwrap());
    assert_eq!(last["event"], "TASK_CANCELLED");
    let gone = wait_until(Duration::from_secs(5), || {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(agent_pid), None).is_err()
    });
    assert!(gone, "cancelled agent {agent_pid} still alive");
}

#[test]
fn recent_output_defers_an_overdue_inactivity_check() {
    let mut h = Harness::new();
    h.set_control("sleep", "60");
    h.set_control("stdout", "still working\n");
    let id = h.submit(&Harness::spec("claude", "keep writing"));
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let output = h.output_log(&id);
    assert!(
        wait_until(Duration::from_secs(5), || {
            fs::metadata(&output).map(|m| m.len() != 0).unwrap_or(false)
        }),
        "agent never wrote output.log"
    );

    // created_at is already overdue, but the initial output starts a fresh
    // inactivity window when the daemon comes back
    h.stop_daemon();
    h.backdate_created_at(&id, 5);
    h.set_timeout_secs(&id, 6);
    h.start_daemon();
    thread::sleep(Duration::from_secs(2));

    // write while the timer is armed; this must move the deadline
    {
        let mut log = fs::OpenOptions::new().append(true).open(&output).unwrap();
        log.write_all(b"later\n").unwrap();
    }

    let fired_while_fresh = wait_until(Duration::from_secs(5), || {
        h.queue_messages()
            .iter()
            .any(|m| event_json(m)["event"] == "TASK_CHECK_DUE")
    });
    assert!(
        !fired_while_fresh,
        "recent non-empty output must defer TASK_CHECK_DUE: {:?}",
        h.queue_messages()
    );
    assert_eq!(h.show(&id)["status"], "running");
    assert_eq!(h.show(&id)["check_timeout"], "pending");

    assert!(
        wait_until(Duration::from_secs(15), || {
            h.queue_messages()
                .iter()
                .any(|m| event_json(m)["event"] == "TASK_CHECK_DUE")
        }),
        "no TASK_CHECK_DUE after output went stale: {:?}",
        h.queue_messages()
    );
    let check = h
        .queue_messages()
        .iter()
        .map(|m| event_json(m))
        .find(|ev| ev["event"] == "TASK_CHECK_DUE")
        .unwrap();
    assert!(check["process"].is_null(), "{check}");
    assert_eq!(check["next_action"], "inspect_task");
    assert_eq!(check["timeout_secs"], 6);
    assert_eq!(h.show(&id)["status"], "running", "child must keep running");
    assert!(
        wait_until(Duration::from_secs(5), || h.show(&id)["check_timeout"]
            == "sent"),
        "check timeout stayed pending after TASK_CHECK_DUE: {}",
        h.show(&id)
    );

    let out = h.cmd().args(["task", "cancel", &id]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    h.wait_status(&id, "cancelled");
}

#[test]
fn status_responds_while_callback_hangs() {
    let h = Harness::new();
    h.set_control("queue-sleep", "5");
    let id = h.submit(&Harness::spec("claude", "fast"));
    thread::sleep(Duration::from_millis(100));
    let start = Instant::now();
    let list = h.cmd().args(["--json", "task", "list"]).output().unwrap();
    let list_elapsed = start.elapsed();
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(
        list_elapsed < Duration::from_millis(500),
        "task list took {list_elapsed:?}"
    );
    let start = Instant::now();
    let status = h
        .cmd()
        .args(["--json", "daemon", "status"])
        .output()
        .unwrap();
    let status_elapsed = start.elapsed();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(
        status_elapsed < Duration::from_millis(500),
        "daemon status took {status_elapsed:?}"
    );
    let _ = id;
}

#[test]
fn sigterm_without_cancel_is_failed() {
    let h = Harness::new();
    let spec = Harness::spec("claude", "hold");
    h.set_control("sleep", "20");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
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
    let pid = h.show(&id)["pid"].as_i64().unwrap() as i32;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    let show = h.wait_status(&id, "failed");
    assert_eq!(show["exit_reason"]["kind"], "signal");
    assert_eq!(show["exit_reason"]["signal"], 15);
    assert!(show["cancel_requested_at"].is_null(), "{show}");
    let msgs = h.wait_for_event(&id, "TASK_FAILED");
    let ev = event_json(msgs.last().unwrap());
    assert_eq!(ev["event"], "TASK_FAILED");
}

#[test]
fn cancel_immediately_after_submit() {
    let h = Harness::new();
    let spec = Harness::spec("claude", "hold");
    h.set_control("sleep", "20");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
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
    for _ in 0..20 {
        let _ = h.cmd().args(["task", "cancel", &id]).output();
    }
    let show = h.wait_status(&id, "cancelled");
    assert_eq!(show["status"], "cancelled");
    let msgs = h.wait_for_event(&id, "TASK_CANCELLED");
    assert!(
        msgs.iter()
            .any(|m| event_json(m)["event"] == "TASK_CANCELLED"),
        "{msgs:?}"
    );
    assert!(
        msgs.iter().all(|m| event_json(m)["event"] != "TASK_LOST"),
        "{msgs:?}"
    );
}

#[test]
fn process_group_cleaned_when_child_leaves_descendant() {
    let h = Harness::new();
    let pid_file = h.home.join("descendant.pid");
    let script = h.home.join("leave-descendant.sh");
    fs::write(
        &script,
        format!(
            "#!/bin/sh\n# nested sh so $$ is the descendant, not this script\nsh -c 'trap \"\" TERM; printf \"%s\\n\" \"$$\" > \"{pid}\"; sleep 60' &\nwhile [ ! -f '{pid}' ]; do sleep 0.01; done\nexit 0\n",
            pid = pid_file.display()
        ),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).unwrap();
    }
    let script_s = script.to_string_lossy().into_owned();
    let start = Instant::now();
    let id = h.submit(&Harness::task_spec(&[script_s.as_str()]));
    assert!(
        wait_until(Duration::from_secs(5), || pid_file.exists()),
        "descendant never recorded its pid"
    );
    let descendant: i32 = fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(descendant), None).is_ok(),
        "descendant {descendant} should be alive before group cleanup"
    );
    // direct child exited 0; runner must still reap the ignore-TERM descendant
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(show["exit_reason"]["kind"], "exit");
    assert_eq!(show["exit_reason"]["code"], 0);
    assert!(
        start.elapsed() < Duration::from_secs(25),
        "group cleanup took {:?}",
        start.elapsed()
    );
    let gone = wait_until(Duration::from_secs(5), || {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(descendant), None).is_err()
    });
    assert!(
        gone,
        "descendant {descendant} survived process-group cleanup"
    );
}

#[test]
fn cancel_on_terminal_task_is_idempotent() {
    let h = Harness::new();
    h.set_control("report", "succeeded\nQuick.");
    let id = h.submit(&Harness::spec("claude", "quick"));
    h.wait_status(&id, "succeeded");
    // the success callback can land after the status flips, so count from it
    let before = h.wait_for_event(&id, "TASK_SUCCEEDED").len();
    let out = h
        .cmd()
        .args(["--json", "task", "cancel", &id])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "cancel on a terminal task must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["status"], "succeeded");
    assert_eq!(
        h.queue_messages().len(),
        before,
        "cancel re-sent a callback"
    );
}

#[test]
fn daemon_exits_on_sigterm_with_a_live_worker() {
    let mut h = Harness::new();
    let spec = Harness::spec("claude", "hold");
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

    let mut daemon = h.daemon.take().unwrap();
    let daemon_pid = daemon.id() as i32;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(daemon_pid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    let exited = wait_until(Duration::from_secs(2), || {
        matches!(daemon.try_wait(), Ok(Some(_)))
    });
    let _ = daemon.kill();
    let _ = daemon.wait();
    assert!(
        exited,
        "daemon {daemon_pid} did not exit within 2s of SIGTERM"
    );
}

#[test]
fn sigterm_right_after_cas_is_cancelled_not_lost() {
    let h = Harness::new();
    // the window runs from the Queued->Running CAS to `signal()` inside
    // `run_agent`, and the only work in it is the feed read. Swap a very large
    // feed in behind the daemon so that read takes long enough to signal into
    let big = h.home.join("big-feed.txt");
    fs::write(&big, vec![b'x'; 512 * 1024 * 1024]).unwrap();

    let mut spec = Harness::spec("claude", "widen the window");
    spec["workload"]["report_trailer"] = json!(false);
    h.set_control("no-stdin", "");
    h.set_control("sleep", "20");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id: TaskId = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    // rename is atomic and instant, unlike writing the bytes here
    fs::rename(
        &big,
        h.home
            .join("tasks")
            .join(id.to_string())
            .join("prompt.feed.txt"),
    )
    .unwrap();

    // poll SQLite directly with no back-off and signal from this process:
    // going through the socket would cost more than the window is wide
    let store = h.store();
    let deadline = Instant::now() + Duration::from_secs(10);
    let pid = loop {
        assert!(Instant::now() < deadline, "task never reached running");
        let row = store.require_task(id).unwrap();
        if row.status() == ProcessStatus::Running {
            break row.pid().expect("running row records the worker pid");
        }
    };
    store.request_cancel(id).unwrap();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    drop(store);

    let show = h.wait_status(&id.to_string(), "cancelled");
    assert_eq!(show["exit_reason"]["kind"], "cancelled");
    // the worker delivers the exit event just after the CAS `wait_status` saw
    let delivered = wait_until(Duration::from_secs(10), || {
        h.queue_messages()
            .iter()
            .any(|m| event_json(m)["task"] == id.to_string())
    });
    assert!(delivered, "worker never delivered an exit event for {id}");
    let last = event_json(h.queue_messages().last().unwrap());
    assert_eq!(last["event"], "TASK_CANCELLED");
}

#[test]
fn reconcile_delivers_a_pending_callback_on_a_terminal_row() {
    let mut h = Harness::new();
    h.stop_daemon();

    let id = TaskId::new();
    let store = h.store();
    fs::create_dir_all(h.home.join("tasks").join(id.to_string())).unwrap();
    let cwd = std::env::temp_dir();
    let spec: homebased::spec::NormalizedSpec = serde_json::from_value(json!({
        "api_version": 1,
        "thread": THREAD,
        "name": "reconcile test",
        "cwd": cwd,
        "timeout": "2h",
        "workload": {
            "type": "agent",
            "agent": "claude",
            "prompt": "reconcile",
            "report_trailer": false
        }
    }))
    .unwrap();
    let row = new_queued_task(NewTask {
        id,
        name: spec.name.clone(),
        thread: spec.thread,
        workload: Workload::Agent(AgentWorkload {
            agent: Agent::new(AgentKind::Claude, None),
            extra_args: vec![],
            report_trailer: false,
            resume_thread: None,
        }),
        cwd: cwd.clone(),
        timeout: spec.timeout,
        env: TaskEnv {
            path: h.path.clone(),
            home: h.user_home.display().to_string(),
        },
        binary: fixture("fake-claude"),
    });
    let home = homebased::home::Home::resolve(Some(h.home.clone())).unwrap();
    let machine = homebased::machine::load_or_create_machine_id(&home).unwrap();
    store
        .insert_local_task(
            &row,
            &spec,
            machine,
            homebased::submission::RequestId::new(),
            homebased::submission::CallbackExecutable::available(fixture("fake-codex")),
        )
        .unwrap();
    // the daemon died after the exit CAS committed its terminal event
    store
        .cas_exit(id, ProcessStatus::Queued, &ExitReason::Cancelled)
        .unwrap()
        .expect("queued row cancels");
    drop(store);

    let before = h.queue_messages().len();
    h.start_daemon();
    let delivered = wait_until(Duration::from_secs(10), || {
        h.queue_messages()
            .iter()
            .skip(before)
            .any(|m| event_json(m)["task"] == id.to_string())
    });
    assert!(delivered, "reconcile never delivered the pending callback");
    // the queued message lands before the settle commits
    let sent = wait_until(Duration::from_secs(10), || {
        h.show(&id.to_string())["callback"] == "sent"
    });
    assert!(sent, "delivered callback never settled as sent");
}

/// A terminal event must never overtake a `TASK_CHECK_DUE` that is already on
/// the wire. The gated fake queue makes `queue-messages.txt` record completion
/// order, so the assertion is about real delivery order, not about timing
///
/// Also covers a settlement boundary without waiting the full 90s settle
/// budget: the gate stays closed past 15s from queue entry, still under the
/// 20s attempt deadline, and the terminal event must not leak
#[test]
fn terminal_event_waits_for_an_in_flight_check_due() {
    let mut h = Harness::new();
    h.set_control("sleep", "40");
    let id = h.submit(&Harness::spec("claude", "race the terminal event"));
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let before = h.queue_messages().len();

    // hold the check-due send open, then make the reminder overdue
    h.set_control("queue-gate", "TASK_CHECK_DUE");
    h.backdate_created_at(&id, 3);
    h.restart_daemon();
    let entered = h.record.join("queue-gate-entered");
    assert!(
        wait_until(Duration::from_secs(15), || entered.exists()),
        "the check-due send never reached the queue: {:?}",
        h.queue_messages()
    );
    let entered_at = Instant::now();
    assert_eq!(h.show(&id)["check_timeout"], "pending", "not delivered yet");

    // the child ends while the reminder is still in flight
    let out = h.cmd().args(["task", "cancel", &id]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    h.wait_status(&id, "cancelled");

    // hold past 15s, still under the 20s attempt deadline, and
    // prove the terminal event cannot overtake the live claim
    let hold_until = entered_at + Duration::from_secs(16);
    while Instant::now() < hold_until {
        assert_eq!(
            h.queue_messages().len(),
            before,
            "a terminal event was delivered while TASK_CHECK_DUE was still sending: {:?}",
            h.queue_messages()
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    h.clear_control("queue-gate");
    assert!(
        wait_until(Duration::from_secs(20), || {
            let events: Vec<String> = h
                .queue_messages()
                .iter()
                .skip(before)
                .map(|m| {
                    event_json(m)["event"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string()
                })
                .collect();
            events.iter().any(|e| e == "TASK_CANCELLED")
        }),
        "terminal event never arrived: {:?}",
        h.queue_messages()
    );
    let events: Vec<String> = h
        .queue_messages()
        .iter()
        .skip(before)
        .map(|m| {
            event_json(m)["event"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    let check = events.iter().position(|e| e == "TASK_CHECK_DUE");
    let terminal = events.iter().position(|e| e == "TASK_CANCELLED");
    assert_eq!(check, Some(0), "check-due must land first: {events:?}");
    assert!(
        terminal > check,
        "terminal event landed before the reminder: {events:?}"
    );
    assert_eq!(h.show(&id)["check_timeout"], "sent");
}
