//! Task submission, validation, dry runs, listing, and command workloads

use super::{Harness, THREAD, event_json, register_thread, submit_error, wait_until};
use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;

#[test]
fn local_submit_retry_with_the_same_request_returns_the_saved_task() {
    let h = Harness::new();
    let request = "01a0e487-b877-76e2-9dc2-806bff0bf687";
    let submit = |prompt: &str| {
        let spec_path = h.home.join("retry-spec.json");
        fs::write(
            &spec_path,
            serde_json::to_vec(&Harness::spec("claude", prompt)).unwrap(),
        )
        .unwrap();
        h.cmd()
            .args(["--json", "task", "submit", "--spec"])
            .arg(&spec_path)
            .args(["--request-id", request])
            .output()
            .unwrap()
    };

    let first = submit("do the work");
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(first["request_id"], request);
    let id = first["id"].as_str().unwrap().to_string();

    // a caller that lost the first response retries with the same request
    let retry = submit("do the work");
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let retry: Value = serde_json::from_slice(&retry.stdout).unwrap();
    assert_eq!(retry["id"], id);
    let tasks: i64 = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME))
        .unwrap()
        .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
        .unwrap();
    assert_eq!(tasks, 1);

    let changed = submit("different work");
    assert!(!changed.status.success());
    let error: Value = serde_json::from_slice(&changed.stderr).unwrap();
    assert_eq!(error["error"]["code"], "submission_conflict");
    h.wait_status(&id, "succeeded");
}

#[test]
fn submit_dry_run_and_schema() {
    let h = Harness::new();
    let spec = Harness::spec("claude", "preview");
    let mut child = h
        .cmd()
        .args(["--json", "task", "submit", "--dry-run", "--spec", "-"])
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
            .write_all(serde_json::to_vec(&spec).unwrap().as_slice())
            .unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(v.get("argv").is_some(), "{v}");
    assert_eq!(v["stdin"], "prompt_feed");
    let argv = v["argv"].as_array().unwrap();
    assert!(argv.iter().any(|a| a.as_str() == Some("-p")));
    assert!(
        !argv
            .iter()
            .any(|a| a.as_str().is_some_and(|s| s.contains("prompt.feed.txt"))),
        "claude dry-run must not put the feed path in argv: {v}"
    );

    let grok = Harness::spec("grok", "preview");
    let mut child = h
        .cmd()
        .args(["--json", "task", "submit", "--dry-run", "--spec", "-"])
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
            .write_all(serde_json::to_vec(&grok).unwrap().as_slice())
            .unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["stdin"], "null");
    let argv = v["argv"].as_array().unwrap();
    assert!(
        argv.iter().any(|a| a
            .as_str()
            .is_some_and(|s| s.contains("tasks/<task-id>/prompt.feed.txt"))),
        "grok dry-run must include the deterministic feed placeholder: {v}"
    );

    let mut opencode = Harness::spec("opencode", "preview opencode");
    opencode["workload"]["model"] = json!("zai-coding-plan/glm-5.3-flash");
    let mut child = h
        .cmd()
        .args(["--json", "task", "submit", "--dry-run", "--spec", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(serde_json::to_vec(&opencode).unwrap().as_slice())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["stdin"], "prompt_feed");
    assert_eq!(
        v["managed_environment"]["policy"],
        "open_code_full_work_permissions"
    );
    assert_eq!(v["managed_environment"]["wildcard_permission"], "allow");
    assert_eq!(
        v["managed_environment"]["generated_agent"]["name"],
        "homebased-<task-id>"
    );
    let argv = v["argv"].as_array().unwrap();
    assert!(argv.iter().any(|arg| arg == "run"));
    assert!(argv.iter().any(|arg| arg == "homebased-<task-id>"));
    assert!(
        argv.iter()
            .any(|arg| arg == "zai-coding-plan/glm-5.3-flash")
    );
    assert!(!v.to_string().contains("OPENCODE_CONFIG_CONTENT"));
    assert!(h.store().list_tasks(&[], None).unwrap().is_empty());

    let out = h.cmd().args(["task", "schema"]).output().unwrap();
    assert!(out.status.success());
    let schema: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(schema.is_object());
}

#[test]
fn help_lists_tree_and_exit_codes() {
    let hb = assert_cmd::cargo::cargo_bin("homebased");
    for args in [
        vec!["--help"],
        vec!["task", "--help"],
        vec!["daemon", "--help"],
        vec!["update", "--help"],
    ] {
        let out = Command::new(&hb).args(&args).output().unwrap();
        assert!(out.status.success());
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(text.contains("Exit codes"), "{text}");
        if args.len() == 1 {
            assert!(text.contains("daemon"), "{text}");
            assert!(text.contains("task"), "{text}");
            assert!(text.contains("update"), "{text}");
        }
        if args[0] == "task" {
            assert!(text.contains("submit"), "{text}");
        }
        if args[0] == "daemon" {
            assert!(text.contains("install"), "{text}");
            assert!(text.contains("stop"), "{text}");
        }
    }
    let report_help = Command::new(&hb)
        .args(["task", "report", "--help"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&report_help.stdout);
    assert!(text.contains("succeeded"), "{text}");
    assert!(text.contains("failed"), "{text}");
    assert!(text.contains("blocked"), "{text}");
    let list_help = Command::new(&hb)
        .args(["task", "list", "--help"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&list_help.stdout);
    assert!(text.contains("queued"), "{text}");
    assert!(text.contains("running"), "{text}");
    let update_help = Command::new(&hb)
        .args(["update", "--help"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&update_help.stdout);
    assert!(text.contains("--tag"), "{text}");
    assert!(text.contains("--dry-run"), "{text}");
    assert!(text.contains("restart"), "{text}");
}

#[test]
fn list_filters_by_status_and_thread() {
    let h = Harness::new();
    let other_thread = "01a0ab97-a7aa-7463-a5b0-8d500e40e999";
    let done = h.submit(&Harness::spec("claude", "done"));
    h.wait_status(&done, "succeeded");

    let mut spec = Harness::spec("claude", "still running");
    register_thread(&h.user_home, other_thread);
    spec["thread"] = json!(other_thread);
    let spec_path = h.home.join("spec-running.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "20");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let running = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(
        Duration::from_secs(5),
        || h.show(&running)["status"] == "running"
    ));

    assert_eq!(list_ids(&h, &["--status", "succeeded"]), vec![done.clone()]);
    assert_eq!(
        list_ids(&h, &["--status", "running"]),
        vec![running.clone()]
    );
    let mut both = list_ids(&h, &["--status", "succeeded,running"]);
    both.sort();
    let mut want = vec![done.clone(), running.clone()];
    want.sort();
    assert_eq!(both, want);
    assert_eq!(list_ids(&h, &["--thread", THREAD]), vec![done]);
    assert_eq!(list_ids(&h, &["--thread", other_thread]), vec![running]);
    assert!(list_ids(&h, &["--status", "lost"]).is_empty());
}

fn list_ids(h: &Harness, args: &[&str]) -> Vec<String> {
    let out = h
        .cmd()
        .args(["--quiet", "task", "list"])
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

#[test]
fn log_tail_returns_the_last_lines() {
    let h = Harness::new();
    h.set_control("stdout", "line one\nline two\nline three\n");
    let id = h.submit(&Harness::spec("claude", "logging"));
    h.wait_status(&id, "succeeded");

    let out = h.cmd().args(["task", "log", &id]).output().unwrap();
    let full = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(full.contains("line one"), "{full}");

    let out = h
        .cmd()
        .args(["task", "log", &id, "--tail", "1"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let tail = String::from_utf8_lossy(&out.stdout).into_owned();
    assert_eq!(tail, "line three\n");

    let out = h
        .cmd()
        .args(["task", "log", &id, "--tail", "2"])
        .output()
        .unwrap();
    let tail = String::from_utf8_lossy(&out.stdout).into_owned();
    assert_eq!(tail, "line two\nline three\n");
}

#[test]
fn named_task_exposes_its_name_and_missing_name_is_rejected() {
    let h = Harness::new();
    let mut named = Harness::task_spec(&["true"]);
    named["name"] = json!("named job");
    let named_id = h.submit(&named);

    let named_show: Value = serde_json::from_slice(
        &h.cmd()
            .args(["--json", "task", "show", &named_id])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    assert_eq!(named_show["name"], "named job");

    let mut nameless = Harness::task_spec(&["echo", "hi", "there", "x"]);
    nameless.as_object_mut().unwrap().remove("name");
    let spec_path = h.home.join("spec-nameless.json");
    fs::write(&spec_path, serde_json::to_vec(&nameless).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["code"], "invalid_spec");
    assert_eq!(err["error"]["input"]["pointer"], "/name");
}

#[test]
fn task_workload_success() {
    let h = Harness::new();
    let id = h.submit(&Harness::task_spec(&["/bin/echo", "hello"]));
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(
        show["workload"],
        json!({"type": "task", "command": ["/bin/echo", "hello"]})
    );
    assert_eq!(show["exit_reason"]["kind"], "exit");
    assert_eq!(show["exit_reason"]["code"], 0);
    let msgs = h.wait_for_event(&id, "TASK_SUCCEEDED");
    let ev = event_json(msgs.last().unwrap());
    assert_eq!(ev["event"], "TASK_SUCCEEDED");
    assert_eq!(
        ev["workload"],
        json!({"type": "task", "command": ["/bin/echo", "hello"]})
    );
    let log = fs::read_to_string(h.home.join("tasks").join(&id).join("output.log")).unwrap();
    assert!(log.contains("hello"), "{log}");
    assert!(
        !h.home.join("tasks").join(&id).join("prompt.txt").exists(),
        "task workloads must not write prompt evidence"
    );
}

#[test]
fn task_workload_nonzero_exit() {
    let h = Harness::new();
    let id = h.submit(&Harness::task_spec(&["false"]));
    let show = h.wait_status(&id, "failed");
    assert_eq!(show["exit_reason"]["kind"], "exit");
    assert_ne!(show["exit_reason"]["code"], 0);
    let msgs = h.wait_for_event(&id, "TASK_FAILED");
    let ev = event_json(msgs.last().unwrap());
    assert_eq!(ev["event"], "TASK_FAILED");
    assert_eq!(ev["process"]["kind"], "exit");
}

#[test]
fn task_argv_fidelity_empty_and_space_args() {
    let h = Harness::new();
    let script = h.home.join("argv-dump.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'argc=%s\\n' \"$#\"\ni=1\nfor a in \"$@\"; do\n  printf 'arg%s=%s\\n' \"$i\" \"$a\"\n  i=$((i+1))\ndone\n",
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).unwrap();
    }
    let script_s = script.to_string_lossy().into_owned();
    let id = h.submit(&Harness::task_spec(&[script_s.as_str(), "", " ", "ok"]));
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(
        show["workload"]["command"],
        json!([script_s, "", " ", "ok"])
    );
    let log = fs::read_to_string(h.home.join("tasks").join(&id).join("output.log")).unwrap();
    assert!(log.contains("argc=3"), "{log}");
    assert!(log.contains("arg1=\n"), "{log}");
    assert!(log.contains("arg2= \n") || log.contains("arg2= "), "{log}");
    assert!(log.contains("arg3=ok"), "{log}");
}

#[test]
fn timeout_minimum_is_thirty_minutes() {
    let h = Harness::new();
    let mut spec = Harness::spec("claude", "too short");
    spec["timeout"] = json!("29m");
    let spec_path = h.home.join("spec-short.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["code"], "invalid_spec");
    assert_eq!(err["error"]["input"]["pointer"], "/timeout");
    assert!(h.store().list_tasks(&[], None).unwrap().is_empty());

    spec["timeout"] = json!("30m");
    let id = h.submit(&spec);
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(show["timeout_secs"], 30 * 60);
}

#[test]
fn missing_executable_returns_executable_missing() {
    let h = Harness::new();
    let missing = h.home.join("no-such-bin");
    let spec = Harness::task_spec(&[missing.to_str().unwrap(), "x"]);
    let spec_path = h.home.join("spec-missing.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(3),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["code"], "executable_missing");
    assert!(h.store().list_tasks(&[], None).unwrap().is_empty());
}

#[test]
fn task_dry_run_returns_exact_argv_and_creates_no_row() {
    let h = Harness::new();
    let spec = Harness::task_spec(&["/bin/echo", "", " spaced ", "hi"]);
    let mut child = h
        .cmd()
        .args(["--json", "task", "submit", "--dry-run", "--spec", "-"])
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
            .write_all(serde_json::to_vec(&spec).unwrap().as_slice())
            .unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["argv"], json!(["/bin/echo", "", " spaced ", "hi"]), "{v}");
    assert_eq!(v["stdin"], "null");
    assert!(h.store().list_tasks(&[], None).unwrap().is_empty());
    let tasks_dir = h.home.join("tasks");
    assert!(!tasks_dir.exists() || fs::read_dir(&tasks_dir).unwrap().next().is_none());
}

#[test]
fn mistyped_thread_is_rejected_before_any_work_starts() {
    let h = Harness::new();
    let mistyped = "01a0ab97-a7aa-7463-a5b0-8d500e40e4ff";
    let mut spec = Harness::task_spec(&["/bin/echo", "hello"]);
    spec["thread"] = json!(mistyped);

    for args in [vec!["task", "submit"], vec!["task", "submit", "--dry-run"]] {
        let (code, error) = submit_error(&h, &spec, &args, None);
        assert_eq!(code, 2, "{args:?} {error}");
        assert_eq!(error["api_version"], 1);
        assert_eq!(error["error"]["code"], "unknown_thread", "{args:?} {error}");
        assert_eq!(error["error"]["input"]["value"], mistyped);
        let message = error["error"]["message"].as_str().unwrap();
        assert!(
            message.contains(&format!("did you mean {THREAD}")),
            "{message}"
        );
    }
    let listed = h.cmd().args(["--json", "task", "list"]).output().unwrap();
    let listed: Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(listed["tasks"], json!([]), "{listed}");
}

#[test]
fn worker_submit_to_another_thread_needs_the_override_flag() {
    let h = Harness::new();
    let parent = h.submit(&Harness::task_spec(&["/bin/echo", "parent"]));
    let other = "01a0ab97-a7aa-7463-a5b0-8d500e40e777";
    register_thread(&h.user_home, other);
    let mut spec = Harness::task_spec(&["/bin/echo", "child"]);
    spec["thread"] = json!(other);

    let (code, error) = submit_error(&h, &spec, &["task", "submit", "--dry-run"], Some(&parent));
    assert_eq!(code, 2, "{error}");
    assert_eq!(error["error"]["code"], "thread_mismatch", "{error}");
    assert_eq!(error["error"]["input"]["parent_task"], parent.as_str());
    assert_eq!(error["error"]["input"]["parent_thread"], THREAD);
    let message = error["error"]["message"].as_str().unwrap();
    assert!(
        message.contains(other) && message.contains(THREAD),
        "{message}"
    );

    let allowed = h
        .cmd()
        .env("HOMEBASED_TASK_ID", &parent)
        .args([
            "--json",
            "task",
            "submit",
            "--dry-run",
            "--allow-other-thread",
        ])
        .arg("--spec")
        .arg(h.home.join("rejected-spec.json"))
        .output()
        .unwrap();
    assert!(
        allowed.status.success(),
        "{}",
        String::from_utf8_lossy(&allowed.stderr)
    );

    let same_path = h.home.join("same-thread.json");
    let same_spec = Harness::task_spec(&["/bin/echo", "same"]);
    fs::write(&same_path, serde_json::to_vec(&same_spec).unwrap()).unwrap();
    let same = h
        .cmd()
        .env("HOMEBASED_TASK_ID", &parent)
        .args(["--json", "task", "submit", "--dry-run", "--spec"])
        .arg(&same_path)
        .output()
        .unwrap();
    assert!(
        same.status.success(),
        "{}",
        String::from_utf8_lossy(&same.stderr)
    );
}
