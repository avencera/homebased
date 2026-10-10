//! Agent workers: worker threads, follow-ups, prompt feeds, and agent CLIs

use super::{
    Harness, THREAD, event_json, process_is_live, register_thread, submit_error, wait_until,
};
use homebased::domain::ExitReason;
use serde_json::{Value, json};
use std::fs;
use std::time::Duration;

#[test]
fn submit_then_fake_codex_receives_event() {
    let h = Harness::new();
    h.set_control("report", "succeeded\nDid the work.");
    let id = h.submit(&Harness::spec("claude", "do the work"));
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(show["thread"], THREAD);
    let msgs = h.wait_for_event(&id, "TASK_SUCCEEDED");
    assert_eq!(msgs.len(), 1, "{msgs:?}");
    let ev = event_json(&msgs[0]);
    assert_eq!(ev["event"], "TASK_SUCCEEDED");
    assert_eq!(ev["task"], id);
    assert_eq!(ev["thread"], THREAD);
    assert_eq!(
        ev["workload"],
        json!({"type": "agent", "agent": "claude", "model": "fable"})
    );
    assert_eq!(ev["reports"].as_array().unwrap().len(), 1);
    assert_eq!(ev["process"]["kind"], "exit");
    assert_eq!(ev["process"]["code"], 0);
    let meta = fs::read_to_string(h.record.join("agent-meta.txt")).unwrap();
    assert!(meta.contains("HOMEBASED_TASK_ID="), "{meta}");
    assert!(meta.contains(&format!("HOMEBASED_TASK_ID={id}")), "{meta}");
    assert!(meta.contains("PATH="), "{meta}");
    assert!(meta.contains("fake"), "{meta}");
}

#[test]
fn running_claude_worker_exposes_its_session_as_worker_thread() {
    let h = Harness::new();
    h.set_control("sleep", "30");
    let id = h.submit(&Harness::spec("claude", "long work"));
    let meta_path = h.record.join(format!("agent-meta-{id}.txt"));
    assert!(wait_until(Duration::from_secs(10), || {
        meta_path.exists() && h.show(&id)["status"] == "running"
    }));

    let running = h.show(&id);
    assert_eq!(running["worker_thread"], id);
    let meta = fs::read_to_string(&meta_path).unwrap();
    let argv = meta
        .lines()
        .find_map(|line| line.strip_prefix("argv="))
        .unwrap();
    assert!(
        argv.contains(&format!("--no-session-persistence --session-id {id} ")),
        "{argv}"
    );
    assert_eq!(argv.matches("--session-id").count(), 1, "{argv}");

    let list = h.cmd().args(["--json", "task", "list"]).output().unwrap();
    assert!(list.status.success());
    let listed: Value = serde_json::from_slice(&list.stdout).unwrap();
    assert!(
        listed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["id"] == id.as_str() && task["worker_thread"] == id.as_str())
    );

    h.clear_controls();
    let cancel = h.cmd().args(["task", "cancel", &id]).output().unwrap();
    assert!(cancel.status.success());
    let cancelled = h.wait_status(&id, "cancelled");
    assert_eq!(cancelled["worker_thread"], id);
}

#[test]
fn claude_extra_args_cannot_replace_the_worker_session() {
    let h = Harness::new();
    for extra in ["--session-id", "--resume", "--continue"] {
        let mut spec = Harness::spec("claude", "use another session");
        spec["workload"]["extra_args"] = json!(["--verbose", extra]);
        let (code, error) = submit_error(&h, &spec, &["task", "submit"], None);
        assert_eq!(code, 2, "{error}");
        assert_eq!(error["error"]["code"], "invalid_spec", "{error}");
        assert_eq!(error["error"]["input"]["pointer"], "/workload/extra_args/1");
    }
}

#[test]
fn codex_worker_thread_is_recorded_and_followup_resumes_the_worker() {
    let h = Harness::new();
    let cwd = h.dir.path().join("codex-workspace");
    fs::create_dir_all(&cwd).unwrap();
    let worker_thread = "01a0e487-b877-76e2-9dc2-806bff0bf685";
    h.set_control("stdout", &format!("session id: {worker_thread}\n"));
    let mut spec = Harness::spec_with_cwd("codex", "original prompt", &cwd);
    spec["workload"]["model"] = json!("gpt-6-luna");
    spec["workload"]["extra_args"] = json!(["-c", "model_reasoning_effort=\"high\""]);
    spec["workload"]["report_trailer"] = json!(false);
    let source = h.submit(&spec);
    let source_show = h.wait_status(&source, "succeeded");
    assert_eq!(source_show["worker_thread"], worker_thread);

    let list = h.cmd().args(["--json", "task", "list"]).output().unwrap();
    assert!(list.status.success());
    let listed: Value = serde_json::from_slice(&list.stdout).unwrap();
    assert!(
        listed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| { task["id"] == source && task["worker_thread"] == worker_thread })
    );

    let human = h.cmd().args(["task", "show", &source]).output().unwrap();
    assert!(human.status.success());
    assert!(
        String::from_utf8_lossy(&human.stdout).contains(&format!("worker thread {worker_thread}"))
    );

    let followup = h
        .cmd()
        .args([
            "--json",
            "task",
            "followup",
            &source,
            "--message",
            "Review the new information",
            "--thread",
            THREAD,
        ])
        .output()
        .unwrap();
    assert!(
        followup.status.success(),
        "{}",
        String::from_utf8_lossy(&followup.stderr)
    );
    let followup_json: Value = serde_json::from_slice(&followup.stdout).unwrap();
    let followup_id = followup_json["id"].as_str().unwrap();
    let followup_show = h.wait_status(followup_id, "succeeded");
    assert_eq!(followup_show["worker_thread"], worker_thread);
    let argv = h.agent_meta(followup_id);
    assert!(
        argv.contains(&format!(
            "argv=exec -C {} -s danger-full-access --dangerously-bypass-approvals-and-sandbox -m gpt-6-luna -c model_reasoning_effort=\"high\" resume {worker_thread} -",
            cwd.display()
        )),
        "{argv}"
    );
    assert_eq!(h.agent_stdin(followup_id), "Review the new information");

    let conn = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME)).unwrap();
    let workload_json: String = conn
        .query_row(
            "SELECT workload_json FROM tasks WHERE id=?1",
            [followup_id],
            |row| row.get(0),
        )
        .unwrap();
    let workload: Value = serde_json::from_str(&workload_json).unwrap();
    assert_eq!(workload["resume_thread"], worker_thread);
    assert_eq!(workload["agent"], "codex");
    assert_eq!(workload["model"], "gpt-6-luna");
    assert_eq!(workload["extra_args"], spec["workload"]["extra_args"]);
    assert_eq!(workload["report_trailer"], false);
    let stored_cwd: String = conn
        .query_row("SELECT cwd FROM tasks WHERE id=?1", [followup_id], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(stored_cwd, cwd.to_string_lossy().to_string());
}

#[test]
fn task_followup_reports_each_blocker() {
    let h = Harness::new();
    h.set_control("sleep", "30");
    let running = h.submit(&Harness::spec("codex", "keep running"));
    assert!(wait_until(Duration::from_secs(5), || {
        h.show(&running)["status"] == "running"
    }));
    let running_error = followup_error(&h, &running);
    assert_eq!(running_error["error"]["code"], "followup_unavailable");
    assert_eq!(running_error["error"]["input"]["reason"], "not_terminal");
    let cancel = h.cmd().args(["task", "cancel", &running]).output().unwrap();
    assert!(cancel.status.success());
    h.wait_status(&running, "cancelled");
    // a slow fake agent reads its sleep control after it is marked running, so
    // clearing the control before the cancel can let it exit 0 first
    h.clear_controls();

    let claude = h.submit(&Harness::spec("claude", "not codex"));
    h.wait_status(&claude, "succeeded");
    let claude_error = followup_error(&h, &claude);
    assert_eq!(claude_error["error"]["code"], "followup_unavailable");
    assert_eq!(claude_error["error"]["input"]["reason"], "not_codex");

    let codex = h.submit(&Harness::spec("codex", "no session header"));
    let codex_show = h.wait_status(&codex, "succeeded");
    assert!(codex_show.get("worker_thread").is_none());
    let no_thread_error = followup_error(&h, &codex);
    assert_eq!(no_thread_error["error"]["code"], "followup_unavailable");
    assert_eq!(
        no_thread_error["error"]["input"]["reason"],
        "no_worker_thread"
    );
}

#[test]
fn task_followup_refuses_a_second_active_resume_then_allows_a_later_one() {
    let h = Harness::new();
    let worker_thread = "01a0e487-b877-76e2-9dc2-806bff0bf685";
    h.set_control("stdout", &format!("session id: {worker_thread}\n"));
    let source = h.submit(&Harness::spec("codex", "original"));
    h.wait_status(&source, "succeeded");

    h.set_control("sleep", "3");
    let first = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args([
            "--json",
            "task",
            "followup",
            &source,
            "--message",
            "first follow-up",
        ])
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_id = serde_json::from_slice::<Value>(&first.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let second = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args([
            "--json",
            "task",
            "followup",
            &source,
            "--message",
            "second follow-up",
        ])
        .output()
        .unwrap();
    assert_eq!(second.status.code(), Some(5));
    let error: Value = serde_json::from_slice(&second.stderr).unwrap();
    assert_eq!(error["error"]["code"], "resume_thread_busy");
    assert_eq!(error["error"]["input"]["thread"], worker_thread);
    assert_eq!(error["error"]["input"]["task"], first_id);

    h.wait_status(&first_id, "succeeded");
    h.clear_controls();
    let next = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args([
            "--json",
            "task",
            "followup",
            &source,
            "--message",
            "after the first event",
        ])
        .output()
        .unwrap();
    assert!(
        next.status.success(),
        "{}",
        String::from_utf8_lossy(&next.stderr)
    );
    let next_id = serde_json::from_slice::<Value>(&next.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    h.wait_status(&next_id, "succeeded");
}

#[test]
fn followup_allows_a_worker_to_select_another_event_thread_when_requested() {
    let h = Harness::new();
    let worker_thread = "01a0e487-b877-76e2-9dc2-806bff0bf685";
    h.set_control("stdout", &format!("session id: {worker_thread}\n"));
    let source = h.submit(&Harness::spec("codex", "original"));
    h.wait_status(&source, "succeeded");

    let other_thread = "01a0e487-b877-76e2-9dc2-806bff0bf686";
    register_thread(&h.user_home, other_thread);
    let mismatch = h
        .cmd()
        .env("HOMEBASED_TASK_ID", &source)
        .env("CODEX_THREAD_ID", other_thread)
        .args([
            "--json",
            "task",
            "followup",
            &source,
            "--message",
            "wrong thread by default",
            "--thread",
            other_thread,
        ])
        .output()
        .unwrap();
    assert_eq!(mismatch.status.code(), Some(2));
    let mismatch_error: Value = serde_json::from_slice(&mismatch.stderr).unwrap();
    assert_eq!(mismatch_error["error"]["code"], "thread_mismatch");

    let accepted = h
        .cmd()
        .env("HOMEBASED_TASK_ID", &source)
        .env("CODEX_THREAD_ID", other_thread)
        .args([
            "--json",
            "task",
            "followup",
            &source,
            "--message",
            "allow the other thread",
            "--thread",
            other_thread,
            "--allow-other-thread",
        ])
        .output()
        .unwrap();
    assert!(
        accepted.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    let id = serde_json::from_slice::<Value>(&accepted.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    h.wait_status(&id, "succeeded");
}

#[test]
fn followup_dry_run_reads_message_file_without_spawning() {
    let h = Harness::new();
    let worker_thread = "01a0e487-b877-76e2-9dc2-806bff0bf685";
    h.set_control("stdout", &format!("session id: {worker_thread}\n"));
    let source = h.submit(&Harness::spec("codex", "original"));
    h.wait_status(&source, "succeeded");
    let message_path = h.home.join("followup-message.txt");
    fs::write(&message_path, "message from file").unwrap();

    let before: i64 = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME))
        .unwrap()
        .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
        .unwrap();
    let dry_run = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args(["--json", "task", "followup", &source, "--thread", THREAD])
        .arg("--message-file")
        .arg(&message_path)
        .arg("--dry-run")
        .output()
        .unwrap();
    assert!(
        dry_run.status.success(),
        "{}",
        String::from_utf8_lossy(&dry_run.stderr)
    );
    let preview: Value = serde_json::from_slice(&dry_run.stdout).unwrap();
    assert_eq!(preview["spec"]["workload"]["prompt"], "message from file");
    assert_eq!(preview["stdin"], "prompt_feed");
    let argv: Vec<String> = serde_json::from_value(preview["argv"].clone()).unwrap();
    assert!(
        argv.windows(3)
            .any(|args| { args == ["resume", worker_thread, "-"] })
    );
    let after: i64 = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME))
        .unwrap()
        .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after, before);

    let local_request_id = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args([
            "--json",
            "task",
            "followup",
            &source,
            "--thread",
            THREAD,
            "--message",
            "local retry id",
            "--request-id",
            "01a0e487-b877-76e2-9dc2-806bff0bf687",
            "--dry-run",
        ])
        .output()
        .unwrap();
    assert!(
        local_request_id.status.success(),
        "{}",
        String::from_utf8_lossy(&local_request_id.stderr)
    );
}

#[test]
fn daemon_applies_exit_json_and_records_codex_worker_thread() {
    let h = Harness::new();
    let worker_thread = "01a0e487-b877-76e2-9dc2-806bff0bf685";
    h.set_control("stdout", &format!("session id: {worker_thread}\n"));
    h.set_control("sleep", "30");
    let task = h.submit(&Harness::spec("codex", "hold"));
    assert!(wait_until(Duration::from_secs(5), || {
        h.show(&task)["status"] == "running"
    }));
    let agent_pid = h.agent_pid(&task);
    let output = h.home.join("tasks").join(&task).join("output.log");
    assert!(wait_until(Duration::from_secs(5), || {
        fs::read_to_string(&output)
            .ok()
            .is_some_and(|text| text.contains(&format!("session id: {worker_thread}")))
    }));

    let exit_path = h.home.join("tasks").join(&task).join("exit.json");
    homebased::store::write_exit_json(
        &exit_path,
        &ExitReason::Exit { code: 0 },
        homebased::domain::TaskExitEvidence::default(),
    )
    .unwrap();
    let worker_pid = h.show(&task)["pid"].as_i64().unwrap() as i32;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(worker_pid),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    let _ = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(-agent_pid),
        nix::sys::signal::Signal::SIGKILL,
    );

    let finished = h.wait_status(&task, "succeeded");
    assert_eq!(finished["worker_thread"], worker_thread);
}

fn followup_error(h: &Harness, task: &str) -> Value {
    let output = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args(["--json", "task", "followup", task, "--message", "more work"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    serde_json::from_slice(&output.stderr).unwrap()
}

#[test]
fn grok_gets_feed_file() {
    let h = Harness::new();
    let id = h.submit(&Harness::spec("grok", "grok prompt"));
    h.wait_status(&id, "succeeded");
    let feed = fs::read_to_string(h.record.join("prompt-file.txt")).unwrap();
    assert!(feed.contains("grok prompt"), "{feed}");
    assert!(feed.contains("--- homebased ---"), "{feed}");
}

#[test]
fn opencode_success_receives_feed_environment_and_report() {
    let h = Harness::new();
    let cwd = h.dir.path().join("opencode-work");
    fs::create_dir_all(&cwd).unwrap();
    let mut spec = Harness::spec_with_cwd("opencode", "opencode prompt", &cwd);
    spec["workload"]["model"] = json!("zai-coding-plan/glm-5.3-flash");
    h.set_control("report", "succeeded\nOpenCode finished.");
    let id = h.submit(&spec);
    h.wait_status(&id, "succeeded");
    let _ = h.wait_for_event(&id, "TASK_SUCCEEDED");
    assert!(wait_until(Duration::from_secs(10), || {
        h.show(&id)["callback"] == "sent"
    }));
    let show = h.show(&id);
    assert_eq!(show["workload"]["agent"], "opencode");
    assert_eq!(show["workload"]["model"], "zai-coding-plan/glm-5.3-flash");
    assert_eq!(show["callback"], "sent");

    let meta = h.opencode_meta(&id);
    assert!(
        meta.contains(&format!(
            "argv=run --standalone --agent homebased-{id} --format json --auto --model zai-coding-plan/glm-5.3-flash"
        )),
        "{meta}"
    );
    assert!(
        meta.contains("--format json --auto --model zai-coding-plan/glm-5.3-flash"),
        "{meta}"
    );
    assert!(meta.contains(&format!("cwd={}", cwd.display())), "{meta}");
    assert!(meta.contains(&format!("PWD={}", cwd.display())), "{meta}");
    assert!(meta.contains("permission=wildcard-allow"), "{meta}");
    assert!(meta.contains("generated-agent=present"), "{meta}");
    assert!(meta.contains("provider-content=absent"), "{meta}");
    assert!(
        !meta.contains("prompt"),
        "prompt must not be in argv metadata: {meta}"
    );

    let stdin = h.opencode_stdin(&id);
    assert!(stdin.starts_with("opencode prompt"), "{stdin}");
    assert!(stdin.contains("--- homebased ---"), "{stdin}");
    let output = fs::read_to_string(h.output_log(&id)).unwrap();
    assert!(output.contains("fake opencode progress"), "{output}");
    let terminal = event_json(h.queue_messages().last().unwrap());
    assert_eq!(terminal["event"], "TASK_SUCCEEDED");
    assert_eq!(terminal["task"], id);
}

#[test]
fn opencode_nonzero_exit_is_reported() {
    let h = Harness::new();
    h.set_control("opencode-exit", "7");
    let id = h.submit(&Harness::spec("opencode", "fail opencode"));
    let show = h.wait_status(&id, "failed");
    assert_eq!(show["exit_reason"]["kind"], "exit");
    assert_eq!(show["exit_reason"]["code"], 7);
    let msgs = h.wait_for_event(&id, "TASK_FAILED");
    assert_eq!(event_json(msgs.last().unwrap())["event"], "TASK_FAILED");
    assert!(
        fs::read_to_string(h.output_log(&id))
            .unwrap()
            .contains("fake opencode progress")
    );
}

#[test]
fn opencode_cancellation_cleans_cli_server_and_tool() {
    let h = Harness::new();
    h.set_control("opencode-hold", "");
    let graceful = h.submit(&Harness::spec("opencode", "graceful cancellation"));
    assert!(wait_until(
        Duration::from_secs(5),
        || h.show(&graceful)["status"] == "running"
    ));
    let graceful_pids = [
        h.opencode_pid(&graceful, "cli"),
        h.opencode_pid(&graceful, "server"),
        h.opencode_pid(&graceful, "tool"),
    ];
    let out = h
        .cmd()
        .args(["task", "cancel", &graceful])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    h.wait_status(&graceful, "cancelled");
    assert!(wait_until(Duration::from_secs(10), || {
        graceful_pids.iter().all(|pid| !process_is_live(*pid))
    }));
    h.wait_for_event(&graceful, "TASK_CANCELLED");

    h.set_control("opencode-ignore-term", "");
    h.set_control("opencode-tool-ignore-term", "");
    let forced = h.submit(&Harness::spec("opencode", "forced cancellation"));
    assert!(wait_until(
        Duration::from_secs(5),
        || h.show(&forced)["status"] == "running"
    ));
    let forced_pids = [
        h.opencode_pid(&forced, "cli"),
        h.opencode_pid(&forced, "server"),
        h.opencode_pid(&forced, "tool"),
    ];
    let out = h.cmd().args(["task", "cancel", &forced]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    h.wait_status(&forced, "cancelled");
    assert!(wait_until(Duration::from_secs(15), || {
        forced_pids.iter().all(|pid| !process_is_live(*pid))
    }));
    h.wait_for_event(&forced, "TASK_CANCELLED");
    assert!(
        h.queue_messages()
            .iter()
            .filter_map(|line| event_json(line)["event"].as_str().map(str::to_string))
            .filter(|event| event == "TASK_CANCELLED")
            .count()
            >= 2
    );
}

#[test]
fn large_prompt_early_exit_keeps_code() {
    let h = Harness::new();
    h.set_control("no-stdin", "");
    h.set_control("exit", "3");
    let spec = Harness::spec("claude", &"x".repeat(1024 * 1024));
    let spec_path = h.home.join("spec-big.json");
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
    let show = h.wait_status(&id, "failed");
    assert_eq!(show["exit_reason"]["kind"], "exit");
    assert_eq!(show["exit_reason"]["code"], 3);
    let msgs = h.wait_for_event(&id, "TASK_FAILED");
    let ev = event_json(msgs.last().unwrap());
    assert_eq!(ev["event"], "TASK_FAILED");
    assert_ne!(ev["process"]["kind"], "spawn_failed");
}

/// A worker's stream: one run that ended with Claude Code's accounting, then a
/// run whose message `m2` repeats with growing output and that ended in an
/// error result reporting no usage
const CLAUDE_STREAM: &str = r#"{"type":"system","subtype":"init"}
{"type":"assistant","message":{"id":"m1","model":"claude-opus-5-5","usage":{"input_tokens":2,"output_tokens":5,"cache_read_input_tokens":100,"cache_creation_input_tokens":50}}}
{"type":"result","subtype":"success","num_turns":3,"total_cost_usd":1.5,"modelUsage":{"claude-opus-5-5":{"inputTokens":10,"outputTokens":200,"cacheReadInputTokens":1000,"cacheCreationInputTokens":300,"costUSD":1.25},"claude-haiku-5-5":{"inputTokens":4,"outputTokens":20,"cacheReadInputTokens":0,"cacheCreationInputTokens":40,"costUSD":0.25}}}
{"type":"assistant","message":{"id":"m2","model":"claude-opus-5-5","usage":{"input_tokens":3,"output_tokens":1,"cache_read_input_tokens":2000,"cache_creation_input_tokens":60}}}
{"type":"assistant","message":{"id":"m2","model":"claude-opus-5-5","usage":{"input_tokens":3,"output_tokens":7,"cache_read_input_tokens":2000,"cache_creation_input_tokens":60}}}
{"type":"result","subtype":"error_during_execution","num_turns":1,"modelUsage":{}}
"#;

#[test]
fn claude_worker_usage_reaches_its_event_and_the_usage_report() {
    let mut h = Harness::new();
    h.set_control("stdout", CLAUDE_STREAM);
    h.set_control("report", "succeeded\nDone.");
    let id = h.submit(&Harness::spec("claude", "spend tokens"));
    h.wait_status(&id, "succeeded");

    let msgs = h.wait_for_event(&id, "TASK_SUCCEEDED");
    let usage = &event_json(&msgs[0])["usage"];
    assert_eq!(usage["complete"], false, "{usage}");
    assert_eq!(usage["turns"], 4, "{usage}");
    assert_eq!(usage["cost_usd"], 1.5, "{usage}");
    assert_eq!(usage["input_tokens"], 17, "{usage}");
    assert_eq!(usage["output_tokens"], 227, "{usage}");
    assert_eq!(usage["cache_read_tokens"], 3000, "{usage}");
    assert_eq!(usage["cache_write_tokens"], 400, "{usage}");
    let models = usage["models"].as_array().unwrap();
    assert_eq!(models.len(), 2, "{usage}");
    assert_eq!(models[0]["model"], "claude-opus-5-5");
    assert_eq!(models[0]["output_tokens"], 207);
    assert_eq!(models[1]["model"], "claude-haiku-5-5");
    assert_eq!(h.show(&id)["last_event"]["usage"], *usage);

    let report = h.usage_report();
    assert_eq!(report["totals"]["tasks"], 1, "{report}");
    assert_eq!(report["totals"]["partial_tasks"], 1, "{report}");
    assert_eq!(report["totals"]["cost_usd"], 1.5, "{report}");
    assert_eq!(report["by_model"][0]["key"], "claude-opus-5-5", "{report}");
    assert_eq!(report["by_model"][0]["cost_usd"], 1.25, "{report}");
    assert_eq!(report["by_thread"][0]["key"], THREAD, "{report}");
    assert_eq!(report["tasks"][0]["task"], id, "{report}");
    assert_eq!(report["tasks"][0]["usage"], *usage, "{report}");
    let evidence = report["tasks"][0]["evidence"].as_str().unwrap();
    assert!(evidence.ends_with(&id), "{evidence}");

    // a task that ended before usage was recorded gets it when the daemon starts
    let conn = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME)).unwrap();
    conn.execute("DELETE FROM task_usage", []).unwrap();
    drop(conn);
    assert_eq!(h.usage_report()["totals"]["tasks"], 0);
    h.restart_daemon();
    assert!(wait_until(Duration::from_secs(10), || {
        h.usage_report()["tasks"][0]["usage"] == *usage
    }));
}
