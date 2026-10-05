//! Agent workers that park on other tasks and continue once those tasks end

use super::{Harness, THREAD, event_json, wait_until};
use homebased::domain::{TaskEnv, TaskId};
use homebased::machine::MachineId;
use homebased::store::{NewTask, new_queued_task};
use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::process::{Output, Stdio};
use std::time::Duration;

impl Harness {
    /// A task that waits until `gate` exists under the record directory, then exits with `code`
    fn gated_target(&self, gate: &str, code: i32) -> Value {
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

    fn release_gate(&self, gate: &str) {
        fs::write(self.record.join(gate), "").unwrap();
    }

    /// The callback of `kind` for `id`, waiting for it to be delivered
    fn delivered(&self, id: &str, kind: &str) -> Value {
        self.wait_for_event(id, kind)
            .iter()
            .map(|line| event_json(line))
            .find(|payload| payload["task"] == id && payload["event"] == kind)
            .unwrap()
    }

    /// Delivered callbacks of `kind` for `id`
    fn delivered_count(&self, id: &str, kind: &str) -> usize {
        self.queue_messages()
            .iter()
            .map(|line| event_json(line))
            .filter(|payload| payload["task"] == id && payload["event"] == kind)
            .count()
    }

    /// Run `task report --outcome waiting` for `worker` as the CLI would
    fn report_waiting(&self, worker: &str, on: &[&str], notes: Option<&str>) -> Output {
        let mut cmd = self.cmd();
        cmd.args([
            "--json",
            "task",
            "report",
            "--id",
            worker,
            "--outcome",
            "waiting",
            "--summary",
            "waiting",
        ]);
        for target in on {
            cmd.args(["--on", target]);
        }
        if notes.is_some() {
            cmd.args(["--notes-file", "-"]);
        }
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(notes.unwrap_or_default().as_bytes())
            .unwrap();
        drop(stdin);
        child.wait_with_output().unwrap()
    }

    /// How many times a fake agent started for `id`
    fn agent_runs(&self, id: &str) -> usize {
        fs::read_to_string(self.record.join("agent-runs.txt"))
            .unwrap_or_default()
            .lines()
            .filter(|line| *line == id)
            .count()
    }

    /// Wait until the terminal callback of `id` settled, so a restart cannot resend it
    fn wait_callback_sent(&self, id: &str) {
        assert!(
            wait_until(Duration::from_secs(10), || self.show(id)["callback"]
                == "sent"),
            "callback of {id} did not settle: {}",
            self.show(id)
        );
    }

    fn cancel(&self, id: &str) -> Value {
        let out = self
            .cmd()
            .args(["--json", "task", "cancel", id])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "cancel failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).unwrap()
    }

    fn wait_running(&self, id: &str) {
        assert!(
            wait_until(Duration::from_secs(10), || self.show(id)["status"]
                == "running"),
            "task {id} did not start: {}",
            self.show(id)
        );
    }

    fn wait_chain(&self, id: &str, state: &str) -> Value {
        assert!(
            wait_until(Duration::from_secs(20), || self.show(id)["chain"]["state"]
                == state),
            "chain of {id} did not reach {state}: {}",
            self.show(id)
        );
        self.show(id)["chain"].clone()
    }
}

fn after(mut spec: Value, tasks: &[&str]) -> Value {
    spec["after"] = json!(tasks);
    spec
}

/// Error code and exit code of a refused CLI call
fn refusal(out: &Output) -> (String, i32) {
    assert!(!out.status.success(), "the call unexpectedly succeeded");
    let error: Value = serde_json::from_slice(&out.stderr).unwrap_or_else(|_| {
        panic!(
            "stderr is not an error object: {}",
            String::from_utf8_lossy(&out.stderr)
        )
    });
    (
        error["error"]["code"].as_str().unwrap().to_string(),
        out.status.code().unwrap(),
    )
}

#[test]
fn codex_worker_resumes_its_thread_after_a_failed_target_and_releases_its_dependent() {
    let h = Harness::new();
    let worker_thread = "01a0e487-b877-76e2-9dc2-806bff0bf685";
    h.set_control("stdout", &format!("session id: {worker_thread}\n"));
    let target = h.submit(&h.gated_target("gpu-run", 3));
    h.set_control(
        "waiting",
        &format!("{target}\nIf {target} failed, rerun it with the small batch.\n"),
    );
    h.set_control("report", "succeeded\nFinished after the GPU run.");
    h.set_control("sleep", "1");
    let worker = h.submit(&Harness::spec("codex", "train the model"));
    let dependent = h.submit(&after(Harness::task_spec(&["true"]), &[&worker]));

    let parked = h.delivered(&worker, "TASK_WAITING");
    assert_eq!(parked["next_action"], "none");
    assert_eq!(parked["waiting_on"], json!([target]));
    assert_eq!(parked["process"], json!({"kind": "exit", "code": 0}));
    assert_eq!(parked["reports"][0]["outcome"], "waiting");
    let continuation = parked["continuation"].as_str().unwrap().to_string();
    let show = h.show(&worker);
    assert_eq!(show["chain"]["state"], "waiting");
    assert_eq!(show["chain"]["continuation"], continuation);
    assert_eq!(show["chain"]["run"], 1);
    assert_eq!(show["last_event"]["event"], "TASK_WAITING");
    assert_eq!(
        show["reports"][0]["notes"],
        format!("If {target} failed, rerun it with the small batch.\n")
    );
    let held = h.show(&continuation);
    assert_eq!(held["status"], "held");
    assert_eq!(held["name"], "test agent (continued)");
    assert_eq!(held["chain"]["run"], 2);
    // parking is not an outcome, so the task held after the worker stays held
    assert_eq!(h.show(&dependent)["status"], "held");

    h.release_gate("gpu-run");
    h.delivered(&target, "TASK_FAILED");
    // the continuation starts although its target failed
    let finished = h.delivered(&continuation, "TASK_SUCCEEDED");
    assert_eq!(finished["thread"], THREAD);
    let meta = h.agent_meta(&continuation);
    assert!(
        meta.contains(&format!(" resume {worker_thread} -")),
        "the continuation must resume the worker's thread: {meta}"
    );
    let feed = h.agent_stdin(&continuation);
    assert!(
        feed.starts_with("--- homebased continuation ---\n"),
        "{feed}"
    );
    assert!(feed.contains(&format!("You are continuing task {worker}.")));
    assert!(feed.contains(&format!("homebased --json task show {target}")));
    assert!(feed.contains("rerun it with the small batch"));
    assert!(
        !feed.contains("train the model"),
        "a resumed thread already has the prompt: {feed}"
    );

    h.wait_status(&dependent, "succeeded");
    let chain = h.wait_chain(&worker, "ended");
    assert_eq!(chain["outcome"], "succeeded");
    assert_eq!(chain["runs"], 2);
}

#[test]
fn codex_continuation_without_a_session_header_resumes_the_exiting_runs_thread() {
    let h = Harness::new();
    let thread = "01a0e487-b877-76e2-9dc2-806bff0bf687";
    let first_target = h.submit(&h.gated_target("first-park", 0));
    let second_target = h.submit(&h.gated_target("second-park", 0));
    h.set_control("stdout", &format!("session id: {thread}\n"));
    h.set_control("waiting", &format!("{first_target}\nFirst notes.\n"));
    let worker = h.submit(&Harness::spec("codex", "original work"));
    let first = h.delivered(&worker, "TASK_WAITING")["continuation"]
        .as_str()
        .unwrap()
        .to_string();

    h.clear_controls();
    h.set_control("waiting", &format!("{second_target}\nSecond notes.\n"));
    h.release_gate("first-park");
    let second = h.delivered(&first, "TASK_WAITING")["continuation"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(h.show(&first).get("worker_thread").is_none());
    h.set_control("report", "succeeded\nDone.");
    h.release_gate("second-park");
    h.delivered(&second, "TASK_SUCCEEDED");

    let meta = h.agent_meta(&second);
    assert!(meta.contains(&format!(" resume {thread} -")), "{meta}");
    let feed = h.agent_stdin(&second);
    assert!(
        feed.starts_with("--- homebased continuation ---\n"),
        "{feed}"
    );
    assert!(
        feed.contains(&format!("You are continuing task {first}.")),
        "{feed}"
    );
    assert!(feed.contains("Second notes."), "{feed}");
    assert!(!feed.contains("original work"), "{feed}");
}

#[test]
fn cancel_requested_before_exit_zero_with_waiting_ends_the_chain_without_parking() {
    let h = Harness::new();
    let target = h.submit(&h.gated_target("cancel-exit-target", 0));
    h.set_control("waiting", &format!("{target}\nFirst park.\n"));
    let worker = h.submit(&Harness::spec("codex", "cancel during continuation"));
    let continuation = h.delivered(&worker, "TASK_WAITING")["continuation"]
        .as_str()
        .unwrap()
        .to_string();
    let dependent = h.submit(&after(Harness::task_spec(&["true"]), &[&worker]));
    h.set_control("sleep", "3");
    h.release_gate("cancel-exit-target");
    h.wait_running(&continuation);
    let out = h.report_waiting(&continuation, &[&target], Some("Wait again."));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // pause the runner so the store can commit the exit-zero race without
    // the normal cancel monitor replacing it with a cancelled process result
    let pid = nix::unistd::Pid::from_raw(h.show(&continuation)["pid"].as_i64().unwrap() as i32);
    nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGSTOP).unwrap();
    let store = h.store();
    let id = continuation.parse().unwrap();
    store.request_cancel(id).unwrap();
    let committed = store.cas_exit(
        id,
        homebased::domain::ProcessStatus::Running,
        &homebased::domain::ExitReason::Exit { code: 0 },
    );
    nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGCONT).unwrap();
    assert!(committed.unwrap().is_some());
    let event = h.delivered(&continuation, "TASK_CANCELLED");
    assert_eq!(event["process"], json!({"kind": "exit", "code": 0}));
    assert!(event.get("continuation").is_none(), "{event}");
    assert_eq!(h.delivered_count(&continuation, "TASK_WAITING"), 0);
    let chain = h.wait_chain(&worker, "ended");
    assert_eq!(chain["outcome"], "cancelled");
    assert_eq!(chain["runs"], 2);
    let cancelled = h.delivered(&dependent, "TASK_CANCELLED");
    assert_eq!(cancelled["cancel_reason"]["outcome"], "cancelled");
}

#[test]
fn claude_worker_continues_in_a_fresh_session_and_its_failure_cancels_dependents() {
    let h = Harness::new();
    let target = h.submit(&Harness::task_spec(&["true"]));
    h.delivered(&target, "TASK_SUCCEEDED");
    h.set_control("waiting", &format!("{target}\nCheck the result.\n"));
    h.set_control("report", "failed\nThe fix did not hold.");
    h.set_control("sleep", "1");
    let worker = h.submit(&Harness::spec("claude", "fix the flaky test"));
    let dependent = h.submit(&after(Harness::task_spec(&["true"]), &[&worker]));

    let parked = h.delivered(&worker, "TASK_WAITING");
    let continuation = parked["continuation"].as_str().unwrap().to_string();
    // the target ended before the worker exited, so the continuation starts at once
    let failed = h.delivered(&continuation, "TASK_FAILED");
    assert_eq!(failed["reports"][0]["outcome"], "failed");

    let feed = h.agent_stdin(&continuation);
    assert!(
        feed.starts_with("fix the flaky test\n\n--- homebased continuation ---\n"),
        "{feed}"
    );
    assert!(feed.contains("Check the result."));
    assert!(
        feed.contains("--- homebased ---"),
        "the trailer follows: {feed}"
    );
    let meta = h.agent_meta(&continuation);
    assert!(
        meta.contains(&format!("--session-id {continuation} ")),
        "a continuation is a fresh session: {meta}"
    );
    // Claude-managed background work would die with the turn, so every worker
    // disables it. Monitor stays allowed: checked on Claude Code 2.1.288 with
    // `claude -p --model claude-haiku-4-5-20251001`, a monitored 25 s script
    // kept the process alive until the script finished, with and without the
    // variable, so its work never outlives the turn
    assert!(
        meta.contains("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1\n"),
        "{meta}"
    );
    assert!(
        h.agent_meta(&worker)
            .contains("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1\n")
    );

    let cancelled = h.delivered(&dependent, "TASK_CANCELLED");
    assert_eq!(
        cancelled["cancel_reason"],
        json!({"type": "dependency_ended", "dependency": worker, "outcome": "failed"})
    );
    let chain = h.wait_chain(&continuation, "ended");
    assert_eq!(chain["outcome"], "failed");
}

#[test]
fn other_agents_do_not_get_the_claude_background_variable() {
    let h = Harness::new();
    h.set_control("report", "succeeded\nDone.");
    let id = h.submit(&Harness::spec("codex", "plain codex"));
    h.delivered(&id, "TASK_SUCCEEDED");
    assert!(
        h.agent_meta(&id)
            .contains("CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=\n")
    );
}

#[test]
fn a_continuation_launches_once_across_daemon_restarts() {
    let mut h = Harness::new();

    // the worker parks while the daemon is down, between the commit of its
    // continuation and any launch
    let target = h.submit(&Harness::task_spec(&["true"]));
    h.delivered(&target, "TASK_SUCCEEDED");
    h.set_control("waiting", &format!("{target}\nCarry on.\n"));
    h.set_control("report", "succeeded\nCarried on.");
    h.set_control("sleep", "3");
    let worker = h.submit(&Harness::spec("claude", "first chain"));
    h.wait_running(&worker);
    h.stop_daemon();
    assert!(
        wait_until(Duration::from_secs(20), || h
            .store()
            .chain_view(worker.parse().unwrap())
            .unwrap()
            .is_some()),
        "the worker did not park while the daemon was down"
    );
    // a second exit for the same run, as recovery from exit.json applies it, changes nothing
    let reapplied = h
        .store()
        .cas_exit(
            worker.parse().unwrap(),
            homebased::domain::ProcessStatus::Running,
            &homebased::domain::ExitReason::Exit { code: 0 },
        )
        .unwrap();
    assert!(reapplied.is_none());
    h.clear_control("sleep");
    h.start_daemon();
    let continuation = h.delivered(&worker, "TASK_WAITING")["continuation"]
        .as_str()
        .unwrap()
        .to_string();
    h.delivered(&continuation, "TASK_SUCCEEDED");
    h.wait_callback_sent(&continuation);
    h.restart_daemon();
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(h.agent_runs(&continuation), 1);
    assert_eq!(h.delivered_count(&continuation, "TASK_SUCCEEDED"), 1);
    assert_eq!(h.delivered_count(&worker, "TASK_WAITING"), 1);
    assert_eq!(h.show(&worker)["chain"]["runs"], 2);

    // the daemon restarts while the chain is parked on a running target
    let gated = h.submit(&h.gated_target("restart-gate", 0));
    h.set_control("waiting", &format!("{gated}\nCarry on again.\n"));
    let second = h.submit(&Harness::spec("claude", "second chain"));
    let continuation = h.delivered(&second, "TASK_WAITING")["continuation"]
        .as_str()
        .unwrap()
        .to_string();
    h.restart_daemon();
    assert_eq!(h.show(&continuation)["status"], "held");
    h.release_gate("restart-gate");
    h.delivered(&continuation, "TASK_SUCCEEDED");
    h.wait_callback_sent(&continuation);
    h.restart_daemon();
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(h.agent_runs(&continuation), 1);
    assert_eq!(h.delivered_count(&continuation, "TASK_SUCCEEDED"), 1);
    assert_eq!(h.show(&second)["chain"]["runs"], 2);
}

#[test]
fn a_waiting_report_needs_a_local_agent_known_targets_and_no_cycle() {
    let h = Harness::new();
    h.set_control("sleep", "60");
    let first = h.submit(&Harness::spec("claude", "first worker"));
    let second = h.submit(&Harness::spec("claude", "second worker"));
    let command = h.submit(&Harness::task_spec(&["sleep", "60"]));
    let finished = h.submit(&Harness::task_spec(&["true"]));
    h.wait_running(&first);
    h.wait_running(&second);
    h.delivered(&finished, "TASK_SUCCEEDED");

    let unknown = TaskId::new().to_string();
    let out = h.report_waiting(&first, &[&unknown], Some("notes"));
    assert_eq!(refusal(&out), ("unknown_dependency".into(), 3));
    let out = h.report_waiting(&first, &[&first], Some("notes"));
    assert_eq!(refusal(&out), ("waiting_cycle".into(), 5));
    let out = h.report_waiting(&first, &[&finished], None);
    assert_eq!(refusal(&out).1, 2, "notes are required");
    let out = h.report_waiting(&first, &[], Some("notes"));
    assert_eq!(refusal(&out).1, 2, "a target is required");
    let out = h.report_waiting(&command, &[&finished], Some("notes"));
    assert_eq!(refusal(&out), ("waiting_unsupported_workload".into(), 5));

    // two running workers cannot wait on each other
    let out = h.report_waiting(&first, &[&second], Some("wait for the second"));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = h.report_waiting(&second, &[&first], Some("wait for the first"));
    assert_eq!(refusal(&out), ("waiting_cycle".into(), 5));
    // nor through a task held after the worker
    let held = h.submit(&after(Harness::task_spec(&["true"]), &[&first]));
    let out = h.report_waiting(&first, &[&held], Some("wait for the held task"));
    assert_eq!(refusal(&out), ("waiting_cycle".into(), 5));
    let out = h.report_waiting(&second, &[&held], Some("transitive"));
    assert_eq!(refusal(&out), ("waiting_cycle".into(), 5));

    // a task that runs for another origin
    let remote = TaskId::new();
    let spec: homebased::spec::NormalizedSpec = serde_json::from_value(json!({
        "api_version": 1,
        "thread": THREAD,
        "name": "remote",
        "cwd": "/tmp",
        "machine": "executor",
        "timeout": "4h",
        "workload": {"type": "task", "command": ["/bin/echo", "hello"]}
    }))
    .unwrap();
    let row = new_queued_task(NewTask {
        id: remote,
        name: spec.name.clone(),
        thread: spec.thread,
        workload: homebased::invocation::persist_workload(&spec.workload),
        cwd: "/tmp".into(),
        timeout: spec.timeout,
        env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
        binary: "/bin/echo".into(),
    });
    h.store()
        .insert_remote_task(&row, &spec, MachineId::new(), MachineId::new())
        .unwrap();
    let out = h.report_waiting(&second, &[&remote.to_string()], Some("remote"));
    assert_eq!(refusal(&out), ("waiting_remote_unsupported".into(), 5));

    // a GPU queue run can end while its job continues
    let conn = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME)).unwrap();
    conn.execute(
        "UPDATE tasks SET resource_job_id=?1, run_number=1, step_index=0 WHERE id=?2",
        rusqlite::params![TaskId::new().to_string(), finished],
    )
    .unwrap();
    let out = h.report_waiting(&second, &[&finished], Some("queue run"));
    assert_eq!(refusal(&out), ("waiting_target_unsupported".into(), 5));

    // a chain that already has the most runs it may have
    let chain = TaskId::new().to_string();
    conn.execute(
        "INSERT INTO task_chains (id, state_json, updated_at) VALUES (?1, ?2, 'x')",
        rusqlite::params![
            chain,
            json!({"state": "running", "current": second}).to_string()
        ],
    )
    .unwrap();
    for run in 1..20 {
        conn.execute(
            "INSERT INTO chain_runs (task_id, chain_id, run_number) VALUES (?1, ?2, ?3)",
            rusqlite::params![TaskId::new().to_string(), chain, run],
        )
        .unwrap();
    }
    conn.execute(
        "INSERT INTO chain_runs (task_id, chain_id, run_number) VALUES (?1, ?2, 20)",
        rusqlite::params![second, chain],
    )
    .unwrap();
    let target = h.submit(&Harness::task_spec(&["true"]));
    let out = h.report_waiting(&second, &[&target], Some("one more run"));
    assert_eq!(refusal(&out), ("too_many_continuations".into(), 5));

    for id in [&first, &second, &command] {
        h.cancel(id);
    }
}

#[test]
fn the_last_report_decides_whether_a_worker_parks() {
    let h = Harness::new();
    let target = h.submit(&Harness::task_spec(&["true"]));
    h.delivered(&target, "TASK_SUCCEEDED");

    // a waiting report followed by a non-zero exit fails and parks nothing
    h.set_control("waiting", &format!("{target}\nnotes\n"));
    h.set_control("exit", "4");
    let failed = h.submit(&Harness::spec("claude", "crashes after reporting"));
    let event = h.delivered(&failed, "TASK_FAILED");
    assert_eq!(event["reports"][0]["outcome"], "waiting");
    assert!(event.get("continuation").is_none(), "{event}");
    assert!(h.show(&failed).get("chain").is_none());
    h.clear_controls();

    // a later succeeded report replaces the waiting one
    h.set_control("sleep", "2");
    h.set_control("report", "succeeded\nDid it myself.");
    let worker = h.submit(&Harness::spec("claude", "changes its mind"));
    h.wait_running(&worker);
    let out = h.report_waiting(&worker, &[&target], Some("maybe later"));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let event = h.delivered(&worker, "TASK_SUCCEEDED");
    assert_eq!(event["reports"].as_array().unwrap().len(), 2);
    assert!(h.show(&worker).get("chain").is_none());
}

#[test]
fn cancel_on_any_run_cancels_the_chain_wherever_its_work_is() {
    let h = Harness::new();
    let gated = h.submit(&h.gated_target("cancel-gate", 0));

    // a parked chain: cancelling the parked run cancels its held continuation
    h.set_control("waiting", &format!("{gated}\nnotes\n"));
    let worker = h.submit(&Harness::spec("claude", "parked"));
    let continuation = h.delivered(&worker, "TASK_WAITING")["continuation"]
        .as_str()
        .unwrap()
        .to_string();
    let dependent = h.submit(&after(Harness::task_spec(&["true"]), &[&worker]));
    let response = h.cancel(&worker);
    assert_eq!(response["id"], continuation);
    assert_eq!(response["status"], "cancelled");
    let cancelled = h.delivered(&continuation, "TASK_CANCELLED");
    assert_eq!(cancelled["cancel_reason"], json!({"type": "requested"}));
    assert_eq!(h.wait_chain(&worker, "ended")["outcome"], "cancelled");
    let dependent_event = h.delivered(&dependent, "TASK_CANCELLED");
    assert_eq!(dependent_event["cancel_reason"]["outcome"], "cancelled");
    // the parked run itself stays as it ended
    assert_eq!(h.cancel(&worker)["status"], "succeeded");

    // a held continuation, named directly
    h.set_control("waiting", &format!("{gated}\nnotes\n"));
    let worker = h.submit(&Harness::spec("claude", "parked again"));
    let continuation = h.delivered(&worker, "TASK_WAITING")["continuation"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(h.cancel(&continuation)["status"], "cancelled");
    h.delivered(&continuation, "TASK_CANCELLED");
    assert_eq!(h.wait_chain(&worker, "ended")["outcome"], "cancelled");

    // a running continuation, by the parked run's id
    let running_gate = h.submit(&h.gated_target("running-gate", 0));
    h.set_control("waiting", &format!("{running_gate}\nnotes\n"));
    let worker = h.submit(&Harness::spec("claude", "runs on"));
    let continuation = h.delivered(&worker, "TASK_WAITING")["continuation"]
        .as_str()
        .unwrap()
        .to_string();
    h.set_control("sleep", "30");
    h.release_gate("running-gate");
    h.wait_running(&continuation);
    assert_eq!(h.cancel(&worker)["id"], continuation);
    h.delivered(&continuation, "TASK_CANCELLED");
    assert_eq!(h.wait_chain(&worker, "ended")["outcome"], "cancelled");
    h.release_gate("cancel-gate");
}

#[test]
fn a_parked_codex_chain_reserves_its_thread() {
    let h = Harness::new();
    let worker_thread = "01a0e487-b877-76e2-9dc2-806bff0bf686";
    h.set_control("stdout", &format!("session id: {worker_thread}\n"));
    let gated = h.submit(&h.gated_target("reserve-gate", 0));
    h.set_control("waiting", &format!("{gated}\nnotes\n"));
    let worker = h.submit(&Harness::spec("codex", "reserve"));
    let continuation = h.delivered(&worker, "TASK_WAITING")["continuation"]
        .as_str()
        .unwrap()
        .to_string();
    let followup = h
        .cmd()
        .args([
            "--json",
            "task",
            "followup",
            &worker,
            "--message",
            "do something else",
            "--thread",
            THREAD,
        ])
        .output()
        .unwrap();
    assert_eq!(refusal(&followup).0, "resume_thread_busy");
    let error: Value = serde_json::from_slice(&followup.stderr).unwrap();
    assert_eq!(error["error"]["input"]["task"], continuation);
    h.cancel(&continuation);
}
