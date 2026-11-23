//! T3 thread launch on another machine, through two real daemons and a fake T3 server

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use homebased::domain::ThreadId;
use serde_json::{Value, json};

use super::fake_t3::{FakeT3, ask, install_t3, open_state, set_run, set_session};
use super::{Daemon, add_peer, wait_until};

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
const SECOND_SESSION: &str = "0199aaaa-1111-7000-8000-00000000000c";

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

#[test]
fn a_child_on_another_machine_detaches_from_either_end_on_both_machines() {
    if which::which("curl").is_err() {
        return;
    }
    let origin = Daemon::start("detach-origin", true);
    let executor = Daemon::start("detach-executor", true);
    add_peer(&origin, &executor);
    add_peer(&executor, &origin);
    let origin_t3 = FakeT3::start(Some(state_path(&origin)));
    install_t3(&origin.user_home, &origin_t3.origin);
    let executor_t3 = FakeT3::start(Some(state_path(&executor)));
    install_t3(&executor.user_home, &executor_t3.origin);
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
    let launch = |message: &str, session: &str| {
        let (code, launched) = run_as(
            &origin,
            &parent,
            &[
                "t3",
                "launch",
                "--machine",
                "detach-executor",
                "--project",
                "app",
                "--message",
                message,
                "--child",
            ],
        );
        assert_eq!(code, 0, "{launched}");
        let child = launched["thread"]["thread_id"].as_str().unwrap().to_owned();
        set_session(&child_state, &child, session);
        set_run(&child_state, &child, "run-1", "running");
        codex_rollout(&executor.user_home, session);
        child
    };
    let children = || {
        let (code, listed) = run_as(&origin, &parent, &["thread", "context", "--children"]);
        assert_eq!(code, 0, "{listed}");
        listed["children"].as_array().unwrap().len()
    };
    let parent_thread_state = || {
        let (code, found) = run_as(&origin, &parent, &["thread", "context"]);
        assert_eq!(code, 0, "{found}");
        found["threads"][0]["state"].clone()
    };
    let notices = || {
        origin_t3
            .dispatched("message.dispatch")
            .into_iter()
            .filter(|payload| payload["threadId"] == PARENT)
            .count()
    };

    // the child detaches itself, and its machine ends the parent's copy too
    let child = launch("Coordinate the migration", CHILD_SESSION);
    assert_eq!(parent_thread_state(), "waiting");
    let as_child = [
        ("T3CODE_THREAD_ID", child.as_str()),
        ("CODEX_THREAD_ID", CHILD_SESSION),
    ];
    let (code, detached) = run_as(&executor, &as_child, &["thread", "detach"]);
    assert_eq!(code, 0, "{detached}");
    assert_eq!(detached["notice"]["outcome"], "sent", "{detached}");
    assert_eq!(notices(), 1);
    assert_eq!(children(), 0);
    assert_eq!(parent_thread_state(), "idle");
    let (code, refused) = run_as(
        &executor,
        &as_child,
        &["thread", "report", "--summary", "x"],
    );
    assert_eq!(code, 5, "{refused}");
    assert_eq!(refused["error"]["code"], "not_child_thread");

    // the parent detaches its other remote child through that child's machine
    let second = launch("Check the metrics", SECOND_SESSION);
    assert_eq!(children(), 1);
    let reference = format!("thread_{second}");
    let (code, detached) = run_as(&origin, &parent, &["thread", "detach", &reference]);
    assert_eq!(code, 0, "{detached}");
    assert_eq!(detached["notice"]["outcome"], "skipped", "{detached}");
    assert_eq!(
        detached["child"]["machine"],
        executor.machine_id().to_string()
    );
    assert_eq!(children(), 0);
    assert_eq!(notices(), 1);
    let (code, refused) = run_as(
        &executor,
        &[
            ("T3CODE_THREAD_ID", second.as_str()),
            ("CODEX_THREAD_ID", SECOND_SESSION),
        ],
        &["thread", "report", "--summary", "x"],
    );
    assert_eq!(code, 5, "{refused}");
    assert_eq!(refused["error"]["code"], "not_child_thread");
}

/// Run git in `dir` as a test user, panicking on failure
fn git(dir: &Path, args: &[&str]) -> String {
    // the developer's own config, such as commit signing, stays out; the
    // daemons read their own homes' config
    let output = std::process::Command::new("git")
        .args(["-c", "user.name=test", "-c", "user.email=test@example.com"])
        .args(["-c", "commit.gpgsign=false"])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// A repository at `dir` with one commit on `main`
fn repository(dir: &Path) {
    fs::create_dir_all(dir).unwrap();
    git(dir, &["init", "--quiet", "--initial-branch=main"]);
    git(dir, &["commit", "--quiet", "--allow-empty", "-m", "first"]);
}

/// Runs the command from `cwd`, as an agent in a checkout there would
fn run_json_in(daemon: &Daemon, cwd: &Path, args: &[&str]) -> (i32, Value) {
    let output = daemon
        .cmd()
        .current_dir(cwd)
        .arg("--json")
        .args(args)
        .output()
        .unwrap();
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

/// Make the fake `t3` CLI of `home` add projects to the state database too,
/// as `t3 project add <root>` does
fn fake_project_add(home: &Path, sqlite: &Path) {
    let state = home.join(".t3/userdata/statev2.sqlite");
    let script = format!(
        "#!/bin/sh\ncase \"$1 $2 $3\" in\n  'auth session issue') printf '%s\\n' '{{\"sessionId\":\"fake-session\",\"token\":\"fake-token\",\"method\":\"bearer-access-token\",\"scopes\":[]}}' ;;\n  'auth session revoke') exit 0 ;;\n  'project add --') '{}' '{}' \"INSERT INTO projection_projects (project_id, title, workspace_root) VALUES ('project-cloned', 'cloned', '$4')\" ;;\n  *) exit 3 ;;\nesac\n",
        sqlite.display(),
        state.display()
    );
    fs::write(home.join(".t3/bin/t3"), script).unwrap();
}

#[test]
fn a_project_resolves_on_another_machine_by_its_repository_or_is_cloned_there() {
    let (Ok(_), Ok(_), Ok(sqlite)) = (
        which::which("curl"),
        which::which("git"),
        which::which("sqlite3"),
    ) else {
        return;
    };
    let origin = Daemon::start("repo-origin", true);
    let executor = Daemon::start("repo-executor", true);
    add_peer(&origin, &executor);
    let t3 = FakeT3::start(None);
    install_t3(&executor.user_home, &t3.origin);
    fake_project_add(&executor.user_home, &sqlite);
    // the executor's checkout of the project, whose origin is an SSH remote
    let app = executor.user_home.join("code/app");
    repository(&app);
    git(
        &app,
        &["remote", "add", "origin", "git@github.com:Acme/App.git"],
    );
    // a repository no project checks out yet, reachable offline through a
    // rewrite of its HTTPS URL
    let remotes = executor.user_home.join("remotes");
    let source = remotes.join("acme/tool.git");
    repository(&source);
    let gitconfig = format!(
        "[url \"{}/\"]\n\tinsteadOf = https://git.example.test/\n",
        remotes.display()
    );
    fs::write(executor.user_home.join(".gitconfig"), gitconfig).unwrap();
    let launch = |project: &str, extra: &[&str]| {
        let mut args = vec![
            "t3",
            "launch",
            "--machine",
            "repo-executor",
            "--project",
            project,
            "--title",
            "By repository",
        ];
        args.extend(extra);
        run_json(&origin, &args)
    };

    let (code, by_url) = launch("https://github.com/acme/app", &[]);
    assert_eq!(code, 0, "{by_url}");
    assert_eq!(by_url["thread"]["project_id"], "project-app");
    let (code, by_name) = launch("acme/app.git", &[]);
    assert_eq!(code, 0, "{by_name}");
    assert_eq!(by_name["thread"]["project_id"], "project-app");

    let (code, missing) = launch("https://git.example.test/acme/tool.git", &[]);
    assert_eq!(code, 3, "{missing}");
    assert_eq!(missing["error"]["code"], "t3_project_not_found");
    assert_eq!(missing["error"]["input"]["repository"], true);
    assert_eq!(t3.launches().len(), 2);

    let request = uuid::Uuid::now_v7().to_string();
    let clone = [
        "--clone-if-missing",
        "--clone-into",
        "~/code/tool",
        "--request-id",
        &request,
    ];
    let (code, cloned) = launch("https://git.example.test/acme/tool.git", &clone);
    assert_eq!(code, 0, "{cloned}");
    let checkout = executor.user_home.join("code/tool");
    assert_eq!(cloned["thread"]["project_id"], "project-cloned");
    assert_eq!(
        cloned["thread"]["workspace_root"],
        checkout.to_string_lossy().as_ref()
    );
    assert_eq!(
        git(&checkout, &["config", "--get", "remote.origin.url"]),
        "https://git.example.test/acme/tool.git"
    );
    // the clone moved into place; no staging directory is left beside it
    let leftovers: Vec<String> = fs::read_dir(executor.user_home.join("code"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("homebased-clone"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");

    // a retry after a lost reply finds the clone and its project, adding nothing
    let (code, retried) = launch("https://git.example.test/acme/tool.git", &clone);
    assert_eq!(code, 0, "{retried}");
    assert_eq!(
        retried["thread"]["thread_id"],
        cloned["thread"]["thread_id"]
    );
    let (code, listed) = run_json(&origin, &["t3", "projects", "--machine", "repo-executor"]);
    assert_eq!(code, 0, "{listed}");
    assert_eq!(listed["projects"].as_array().unwrap().len(), 2, "{listed}");
}

#[test]
fn a_missing_worktree_base_fails_the_command_and_names_a_branch_only_the_caller_has() {
    let (Ok(_), Ok(_)) = (which::which("curl"), which::which("git")) else {
        return;
    };
    let origin = Daemon::start("base-origin", true);
    let executor = Daemon::start("base-executor", true);
    add_peer(&origin, &executor);
    // T3 records each launched thread, which a retried launch looks for
    let t3 = FakeT3::start(Some(state_path(&executor)));
    install_t3(&executor.user_home, &t3.origin);
    open_state(&state_path(&executor));
    // the shared remote has `pushed`, which the executor has not fetched yet
    let remote = executor.user_home.join("remote.git");
    let seed = origin.user_home.join("seed");
    repository(&seed);
    git(&seed, &["branch", "pushed"]);
    git(
        &origin.user_home,
        &[
            "clone",
            "--quiet",
            "--bare",
            &seed.to_string_lossy(),
            &remote.to_string_lossy(),
        ],
    );
    // install_t3 made the project directory; the executor's checkout replaces it
    let app = executor.user_home.join("code/app");
    fs::remove_dir_all(&app).unwrap();
    git(
        &executor.user_home,
        &[
            "clone",
            "--quiet",
            "--single-branch",
            "--branch",
            "main",
            &remote.to_string_lossy(),
            &app.to_string_lossy(),
        ],
    );
    // both checkouts name one origin URL, which the executor's git config
    // rewrites to the shared remote, so the error carries its repository key
    let url = "https://git.example.test/acme/app.git";
    git(&app, &["remote", "set-url", "origin", url]);
    let gitconfig = format!("[url \"{}\"]\n\tinsteadOf = {url}\n", remote.display());
    fs::write(executor.user_home.join(".gitconfig"), gitconfig).unwrap();
    git(&seed, &["remote", "add", "origin", url]);
    git(
        &seed,
        &[
            "fetch",
            "--quiet",
            &remote.to_string_lossy(),
            "+refs/heads/*:refs/remotes/origin/*",
        ],
    );
    // the caller's checkout has a branch it never pushed
    git(&seed, &["branch", "local-only"]);
    let launch = |base: &str, extra: &[&str]| {
        let mut args = vec![
            "t3",
            "launch",
            "--machine",
            "base-executor",
            "--project",
            "app",
            "--title",
            "Worktree",
            "--worktree",
            base,
        ];
        args.extend(extra);
        run_json_in(&origin, &seed, &args)
    };

    let (code, local_only) = launch("local-only", &[]);
    assert_eq!(code, 3, "{local_only}");
    let error = &local_only["error"];
    assert_eq!(error["code"], "t3_worktree_base_missing");
    assert_eq!(error["input"]["base_ref"], "local-only");
    assert_eq!(error["input"]["repository"], "git.example.test/acme/app");
    assert_eq!(error["input"]["local"]["kind"], "unpushed_branch");
    let message = error["message"].as_str().unwrap();
    assert!(
        message.contains("git push -u origin local-only"),
        "{message}"
    );

    let (code, unfetched) = launch("pushed", &[]);
    assert_eq!(code, 3, "{unfetched}");
    assert_eq!(unfetched["error"]["input"]["fetched"], false);
    assert_eq!(unfetched["error"]["input"]["local"]["kind"], "pushed");
    assert!(
        unfetched["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--from-origin"),
        "{unfetched}"
    );
    assert!(t3.launches().is_empty());

    let request = uuid::Uuid::now_v7().to_string();
    let (code, fetched) = launch("pushed", &["--from-origin", "--request-id", &request]);
    assert_eq!(code, 0, "{fetched}");
    // a retry returns the launched thread even once its base has gone
    git(&app, &["update-ref", "-d", "refs/remotes/origin/pushed"]);
    git(&remote, &["branch", "-D", "pushed"]);
    let (code, retried) = launch("pushed", &["--from-origin", "--request-id", &request]);
    assert_eq!(code, 0, "{retried}");
    assert_eq!(
        retried["thread"]["thread_id"],
        fetched["thread"]["thread_id"]
    );
    let (code, present) = launch("main", &[]);
    assert_eq!(code, 0, "{present}");
    // the retry asked T3 again, which answers with the thread it launched
    assert_eq!(t3.launches().len(), 3);
}

#[test]
fn a_parent_hears_of_a_question_its_child_on_another_machine_waits_on_and_stops_it() {
    if which::which("curl").is_err() {
        return;
    }
    let scan = [("HOMEBASED_TEST_SCAN_SECS", "1")];
    let origin = Daemon::start_with_env("ask-origin", true, &scan);
    let executor = Daemon::start_with_env("ask-executor", true, &scan);
    add_peer(&origin, &executor);
    add_peer(&executor, &origin);
    let origin_t3 = FakeT3::start(Some(state_path(&origin)));
    install_t3(&origin.user_home, &origin_t3.origin);
    let executor_t3 = FakeT3::start(Some(state_path(&executor)));
    install_t3(&executor.user_home, &executor_t3.origin);
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
            "ask-executor",
            "--project",
            "app",
            "--message",
            "Pick a database",
            "--child",
        ],
    );
    assert_eq!(code, 0, "{launched}");
    let child = launched["thread"]["thread_id"].as_str().unwrap().to_owned();
    set_session(&child_state, &child, CHILD_SESSION);
    set_run(&child_state, &child, "run-c-1", "waiting");
    codex_rollout(&executor.user_home, CHILD_SESSION);
    let questions = json!([{ "id": "Which database?", "question": "Which database?" }]);
    ask(&child_state, &child, "request-1", &questions);

    let alerts = || -> Vec<Value> {
        origin_t3
            .dispatched("message.dispatch")
            .into_iter()
            .filter(|payload| {
                payload["threadId"] == PARENT
                    && payload["text"]
                        .as_str()
                        .is_some_and(|text| text.contains("is waiting on a question"))
            })
            .collect()
    };
    assert!(
        wait_until(std::time::Duration::from_secs(30), || !alerts().is_empty()),
        "the parent heard nothing"
    );
    let text = alerts()[0]["text"].as_str().unwrap().to_owned();
    assert!(text.contains("Which database?"), "{text}");

    let reference = format!("thread_{child}");
    let (code, answered) = run_as(
        &origin,
        &parent,
        &[
            "thread",
            "answer",
            &reference,
            "--request",
            "request-1",
            "--answer",
            "SQLite",
        ],
    );
    assert_eq!(code, 0, "{answered}");
    assert_eq!(
        executor_t3.dispatched("runtime-request.respond")[0]["answers"],
        json!({ "Which database?": "SQLite" })
    );

    // the stop runs on the child's machine and closes the link on the parent's
    let (code, stopped) = run_as(
        &origin,
        &parent,
        &["thread", "interrupt", "--stop", &reference],
    );
    assert_eq!(code, 0, "{stopped}");
    assert_eq!(stopped["outcome"], "stopped");
    assert!(stopped["closed_link"].is_string(), "{stopped}");
    assert_eq!(executor_t3.dispatched("thread.stop").len(), 1);
    assert!(origin_t3.dispatched("thread.stop").is_empty());
    let (code, found) = run_as(&origin, &parent, &["thread", "context"]);
    assert_eq!(code, 0, "{found}");
    assert_eq!(found["threads"][0]["state"], "idle", "{found}");
}
