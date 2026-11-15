//! T3 thread launch on another machine, through two real daemons and a fake T3 server

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use homebased::domain::ThreadId;
use serde_json::{Value, json};

use super::fake_t3::{FakeT3, install_t3, open_state, set_run, set_session};
use super::{Daemon, add_peer};

fn run_json(daemon: &Daemon, args: &[&str]) -> (i32, Value) {
    run_json_in_thread(daemon, None, args)
}

/// Runs the command the way an agent in `thread` would, with no thread when `None`
fn run_json_in_thread(daemon: &Daemon, thread: Option<&str>, args: &[&str]) -> (i32, Value) {
    let mut cmd = daemon.cmd();
    for var in [
        "CODEX_THREAD_ID",
        "CODEX_SESSION_ID",
        "CLAUDE_CODE_SESSION_ID",
    ] {
        cmd.env_remove(var);
    }
    if let Some(thread) = thread {
        cmd.env("CLAUDE_CODE_SESSION_ID", thread);
    }
    let output = cmd.arg("--json").args(args).output().unwrap();
    let stream = if output.status.success() {
        &output.stdout
    } else {
        &output.stderr
    };
    let value = serde_json::from_slice(stream).unwrap_or_else(|_| {
        panic!(
            "not JSON: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.code().unwrap_or(-1), value)
}

#[test]
fn launch_starts_a_t3_thread_on_the_named_machine_with_a_reply_route_only_when_asked() {
    if which::which("curl").is_err() {
        return;
    }
    let origin = Daemon::start("t3-origin", true);
    let executor = Daemon::start("t3-executor", true);
    add_peer(&origin, &executor);
    let t3 = FakeT3::start(None);
    install_t3(&executor.user_home, &t3.origin);
    let source = ThreadId(uuid::Uuid::now_v7()).to_string();
    let request = uuid::Uuid::now_v7().to_string();
    // what the shell passes for an unquoted `~/code/app` on the origin, whose
    // home differs from the executor's
    let project = format!("{}/code/app", origin.user_home.display());
    let launch = [
        "t3",
        "launch",
        "--machine",
        "t3-executor",
        "--project",
        &project,
        "--message",
        "Fix the flaky upload test",
        "--reply-route",
        "--request-id",
        &request,
    ];

    let (code, launched) = run_json_in_thread(&origin, Some(&source), &launch);
    assert_eq!(code, 0, "{launched}");
    let (code, retried) = run_json_in_thread(&origin, Some(&source), &launch);
    assert_eq!(code, 0, "{retried}");

    let thread = launched["thread"]["thread_id"].as_str().unwrap();
    assert_eq!(launched["machine"], executor.machine_id().to_string());
    assert_eq!(launched["machine_name"], "t3-executor");
    assert_eq!(launched["thread"]["project_id"], "project-app");
    assert_eq!(
        launched["thread"]["link"],
        format!("t3code://threads/executor-env/{thread}")
    );
    assert_eq!(retried["thread"]["thread_id"], thread);

    let launches = t3.launches();
    assert_eq!(launches.len(), 2);
    assert_eq!(launches[0]["payload"], launches[1]["payload"]);
    let payload = &launches[0]["payload"];
    assert_eq!(payload["threadId"], thread);
    assert_eq!(payload["commandId"], format!("homebased:{request}"));
    assert_eq!(
        payload["modelSelection"],
        json!({ "instanceId": "codex", "model": "gpt-6.1-sol" })
    );
    let text = payload["initialMessage"]["text"].as_str().unwrap();
    assert!(text.starts_with("Fix the flaky upload test\n"), "{text}");
    assert!(
        text.contains(&format!(
            "homebased message send --machine t3-origin --thread {source}"
        )),
        "{text}"
    );

    let (code, handed_off) = run_json_in_thread(
        &origin,
        Some(&source),
        &[
            "t3",
            "launch",
            "--machine",
            "t3-executor",
            "--project",
            &project,
            "--message",
            "Take over the upload fix",
        ],
    );
    assert_eq!(code, 0, "{handed_off}");
    assert_eq!(
        t3.launches()[2]["payload"]["initialMessage"]["text"],
        "Take over the upload fix"
    );

    let (code, listed) = run_json(&origin, &["t3", "projects", "--machine", "t3-executor"]);
    assert_eq!(code, 0, "{listed}");
    assert_eq!(listed["projects"][0]["id"], "project-app");
    assert_eq!(
        listed["projects"][0]["default_model"]["model"],
        "gpt-6.1-sol"
    );
    let providers: Vec<&str> = listed["providers"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|provider| provider["id"].as_str())
        .collect();
    assert_eq!(providers, ["codex"]);

    let (code, missing) = run_json(
        &origin,
        &[
            "t3",
            "launch",
            "--machine",
            "t3-executor",
            "--project",
            "~/code/missing",
            "--title",
            "Nothing",
        ],
    );
    assert_eq!(code, 3, "{missing}");
    assert_eq!(missing["error"]["code"], "t3_project_not_found");
    assert_eq!(t3.launches().len(), 3);
}

#[test]
fn a_lost_t3_reply_reaches_the_caller_with_the_request_id_to_retry() {
    if which::which("curl").is_err() {
        return;
    }
    let origin = Daemon::start("t3-lost-origin", true);
    let executor = Daemon::start("t3-lost-executor", true);
    add_peer(&origin, &executor);
    let t3 = FakeT3::start(None);
    t3.behavior
        .drop_launch_replies
        .store(true, Ordering::Relaxed);
    install_t3(&executor.user_home, &t3.origin);
    let request = uuid::Uuid::now_v7().to_string();

    let (code, lost) = run_json(
        &origin,
        &[
            "t3",
            "launch",
            "--machine",
            "t3-lost-executor",
            "--project",
            "app",
            "--title",
            "Lost reply",
            "--request-id",
            &request,
        ],
    );

    assert_eq!(code, 1, "{lost}");
    assert_eq!(lost["error"]["code"], "t3_launch_outcome_unknown");
    assert_eq!(lost["error"]["retryable"], true);
    assert_eq!(lost["error"]["input"]["request_id"], request);
    assert_eq!(
        lost["error"]["input"]["machine"],
        executor.machine_id().to_string()
    );
    assert_eq!(t3.launches().len(), 1);
}

const PARENT: &str = "0199aaaa-0000-7000-8000-00000000000a";
const PARENT_SESSION: &str = "0199aaaa-1111-7000-8000-00000000000a";
const CHILD_SESSION: &str = "0199aaaa-1111-7000-8000-00000000000b";

fn state_path(daemon: &Daemon) -> PathBuf {
    daemon.user_home.join(".t3/userdata/statev2.sqlite")
}

/// A Codex rollout for `session` under `home`, so messages can reach it
fn codex_rollout(home: &Path, session: &str) {
    let day = home.join(".codex/sessions/2026/10/02");
    fs::create_dir_all(&day).unwrap();
    let meta = json!({ "type": "session_meta", "payload": { "id": session, "cwd": home } });
    fs::write(
        day.join(format!("rollout-2026-10-02T20-00-00-{session}.jsonl")),
        format!("{meta}\n"),
    )
    .unwrap();
}

/// Runs the command as the agent of a T3 thread, given as T3's variables
fn run_as(daemon: &Daemon, agent: &[(&str, &str)], args: &[&str]) -> (i32, Value) {
    let mut cmd = daemon.cmd();
    cmd.env_remove("T3CODE_THREAD_ID")
        .env_remove("CLAUDE_CODE_SESSION_ID")
        .envs(agent.iter().copied());
    let output = cmd.arg("--json").args(args).output().unwrap();
    let stream = if output.status.success() {
        &output.stdout
    } else {
        &output.stderr
    };
    let value = serde_json::from_slice(stream).unwrap_or_else(|_| {
        panic!(
            "not JSON: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.code().unwrap_or(-1), value)
}

#[test]
fn a_child_launched_on_another_machine_keeps_its_parent_waiting_until_it_reports() {
    if which::which("curl").is_err() {
        return;
    }
    let origin = Daemon::start("child-origin", true);
    let executor = Daemon::start("child-executor", true);
    add_peer(&origin, &executor);
    add_peer(&executor, &origin);
    let origin_t3 = FakeT3::start(Some(state_path(&origin)));
    install_t3(&origin.user_home, &origin_t3.origin);
    let executor_t3 = FakeT3::start(Some(state_path(&executor)));
    install_t3(&executor.user_home, &executor_t3.origin);
    // the tables the executor's T3 adds the launched thread to
    let child_state = open_state(&state_path(&executor));
    let parent_state = open_state(&state_path(&origin));
    parent_state
        .execute(
            "INSERT INTO orchestration_v2_projection_threads (thread_id, title) VALUES (?1, 'parent')",
            [PARENT],
        )
        .unwrap();
    set_session(&parent_state, PARENT, PARENT_SESSION);
    codex_rollout(&origin.user_home, PARENT_SESSION);
    let parent = [
        ("T3CODE_THREAD_ID", PARENT),
        ("CODEX_THREAD_ID", PARENT_SESSION),
    ];

    let (code, launched) = run_as(
        &origin,
        &parent,
        &[
            "t3",
            "launch",
            "--machine",
            "child-executor",
            "--project",
            "app",
            "--message",
            "Fix the flaky upload test",
            "--child",
        ],
    );
    assert_eq!(code, 0, "{launched}");
    let child = launched["thread"]["thread_id"].as_str().unwrap().to_owned();
    let lineage = &launched["lineage"];
    assert_eq!(
        lineage["parent"]["machine"],
        origin.machine_id().to_string()
    );
    assert_eq!(lineage["parent"]["t3_thread"], PARENT);
    assert_eq!(
        lineage["child"]["machine"],
        executor.machine_id().to_string()
    );
    assert_eq!(lineage["child"]["t3_thread"], child.as_str());
    let text = executor_t3.launches()[0]["payload"]["initialMessage"]["text"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(text.contains("homebased thread report"), "{text}");
    assert!(
        text.contains(&format!(
            "homebased message send --machine child-origin --thread thread_{PARENT}"
        )),
        "{text}"
    );
    set_session(&child_state, &child, CHILD_SESSION);
    set_run(&child_state, &child, "run-c-1", "running");
    codex_rollout(&executor.user_home, CHILD_SESSION);

    let children = || {
        let (code, listed) = run_as(&origin, &parent, &["thread", "context", "--children"]);
        assert_eq!(code, 0, "{listed}");
        listed["children"].as_array().unwrap().clone()
    };
    let parent_state = || {
        let (code, found) = run_as(&origin, &parent, &["thread", "context"]);
        assert_eq!(code, 0, "{found}");
        found["threads"][0]["state"].clone()
    };
    let listed = children();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0]["t3_thread"], child.as_str());
    assert_eq!(listed[0]["machine"], executor.machine_id().to_string());
    assert_eq!(listed[0]["child_thread"]["reported"], false);
    assert_eq!(parent_state(), "waiting");

    let body = "Fixed the race in the upload retry";
    let (code, reported) = run_as(
        &executor,
        &[
            ("T3CODE_THREAD_ID", &child),
            ("CODEX_THREAD_ID", CHILD_SESSION),
        ],
        &["thread", "report", "--summary", body],
    );
    assert_eq!(code, 0, "{reported}");
    assert_eq!(reported["report"]["stage"], "settle_pending", "{reported}");
    let delivered: Vec<Value> = origin_t3
        .dispatched("message.dispatch")
        .into_iter()
        .filter(|payload| payload["threadId"] == PARENT)
        .collect();
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    assert!(delivered[0]["text"].as_str().unwrap().contains(body));
    assert_eq!(parent_state(), "idle");
    assert_eq!(children()[0]["child_thread"]["reported"], true);
}
