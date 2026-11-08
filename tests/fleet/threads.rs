//! `homebased thread` across machines: watch scans and child lookups that a
//! peer answers

use super::{Daemon, add_peer, remote_spec, submit_file, wait_until};
use chrono::{Duration as ChronoDuration, Utc};
use homebased::submission::RequestId;
use serde_json::{Value, json};
use std::fs;
use std::process::Output;
use std::time::Duration;

const CALLER: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e432";

/// Two daemons that know each other
fn pair(name: &str) -> (Daemon, Daemon) {
    let origin = Daemon::start(&format!("{name}-origin"), true);
    let peer = Daemon::start(&format!("{name}-peer"), true);
    add_peer(&origin, &peer);
    add_peer(&peer, &origin);
    (origin, peer)
}

/// A Claude Code session outside T3 on `daemon`, idle for ten minutes
fn claude_session(daemon: &Daemon, session: &str, tokens: u64) {
    let project = daemon.user_home.join(".claude/projects/-work");
    fs::create_dir_all(&project).unwrap();
    let line = json!({
        "type": "assistant",
        "timestamp": (Utc::now() - ChronoDuration::minutes(10)).to_rfc3339(),
        "message": { "usage": { "input_tokens": 2, "cache_read_input_tokens": tokens } }
    });
    fs::write(project.join(format!("{session}.jsonl")), line.to_string()).unwrap();
}

fn thread_cmd(daemon: &Daemon, env: &[(&str, &str)], args: &[&str]) -> Output {
    daemon
        .cmd()
        .envs(env.iter().copied())
        .current_dir(&daemon.user_home)
        .args(["--json", "thread"])
        .args(args)
        .output()
        .unwrap()
}

fn ok(out: &Output) -> Value {
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

#[test]
fn a_watch_scan_asks_a_peer_for_more_threads_than_one_lookup_takes() {
    let (mut origin, peer) = pair("watch-batches");
    // one more than a peer takes in one lookup
    let sessions: Vec<String> = (0..65)
        .map(|index| format!("0199eeee-2222-7000-8000-{index:012}"))
        .collect();
    for session in &sessions {
        claude_session(&peer, session, 300_000);
    }
    let peer_name = "watch-batches-peer";
    for batch in sessions.chunks(40) {
        let mut args = vec!["watch", "--machine", peer_name, "--context-above", "100k"];
        args.extend(batch.iter().map(String::as_str));
        let body = ok(&thread_cmd(&origin, &[("CODEX_THREAD_ID", CALLER)], &args));
        assert_eq!(body["watches"].as_array().unwrap().len(), batch.len());
    }

    // the scan runs as the daemon starts, then every minute
    origin.restart();
    let mut fired = 0;
    let all_fired = wait_until(Duration::from_secs(150), || {
        let listed = ok(&thread_cmd(
            &origin,
            &[("CODEX_THREAD_ID", CALLER)],
            &["watches"],
        ));
        fired = listed["watches"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|watch| watch["seq"].as_u64() >= Some(1))
            .count();
        fired == sessions.len()
    });
    assert!(
        all_fired,
        "only {fired} of {} watches fired",
        sessions.len()
    );
}

#[test]
fn a_children_lookup_names_the_machines_that_did_not_answer() {
    let (origin, peer) = pair("children-unanswered");
    // a T3 state the peer cannot read makes its lookups fail rather than come back empty
    let userdata = peer.user_home.join(".t3/userdata");
    fs::create_dir_all(&userdata).unwrap();
    rusqlite::Connection::open(userdata.join("statev2.sqlite"))
        .unwrap()
        .execute_batch(
            "CREATE TABLE orchestration_v2_projection_threads (
                thread_id TEXT PRIMARY KEY, title TEXT, archived_at TEXT, deleted_at TEXT,
                payload_json TEXT
             );
             CREATE TABLE orchestration_v2_projection_provider_threads (
                thread_id TEXT, provider TEXT, driver TEXT, payload_json TEXT, updated_at TEXT
             );",
        )
        .unwrap();
    let caller = [
        ("CODEX_THREAD_ID", CALLER),
        ("T3CODE_THREAD_ID", "mcp:launch:orchestrator"),
    ];

    let body = ok(&thread_cmd(&origin, &caller, &["context", "--children"]));
    assert_eq!(body["children_complete"], false, "{body}");
    let unanswered = body["unanswered"].as_array().unwrap();
    assert_eq!(unanswered.len(), 1, "{body}");
    assert!(
        unanswered[0]
            .as_str()
            .unwrap()
            .starts_with("children-unanswered-peer"),
        "{body}"
    );

    // a watch of only the children that answered would miss the rest silently
    let out = thread_cmd(
        &origin,
        &caller,
        &["watch", "--children", "--context-above", "100k"],
    );
    let error: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(1), "{error}");
    assert_eq!(error["error"]["code"], "thread_lookup_incomplete");
    assert_eq!(error["error"]["retryable"], true);
    let listed = ok(&thread_cmd(&origin, &caller, &["watches"]));
    assert!(listed["watches"].as_array().unwrap().is_empty(), "{listed}");
}

/// T3's state on `daemon` with the tables homebased reads, empty
fn t3_state(daemon: &Daemon) -> rusqlite::Connection {
    let userdata = daemon.user_home.join(".t3/userdata");
    fs::create_dir_all(&userdata).unwrap();
    let db = rusqlite::Connection::open(userdata.join("statev2.sqlite")).unwrap();
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS orchestration_v2_projection_threads (
            thread_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL DEFAULT 'project',
            title TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT '2026-10-02T19:00:00Z',
            default_provider TEXT NOT NULL DEFAULT 'codex',
            active_provider_thread_id TEXT,
            provider_instance_id TEXT,
            archived_at TEXT,
            deleted_at TEXT,
            payload_json TEXT NOT NULL DEFAULT '{}'
         );
         CREATE TABLE IF NOT EXISTS projection_projects (
            project_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            workspace_root TEXT NOT NULL,
            deleted_at TEXT,
            default_model_selection_json TEXT
         );
         CREATE TABLE IF NOT EXISTS orchestration_v2_projection_provider_threads (
            provider_thread_id TEXT PRIMARY KEY,
            thread_id TEXT,
            provider TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            driver TEXT,
            provider_instance_id TEXT
         );
         CREATE TABLE IF NOT EXISTS orchestration_v2_projection_runs (
            run_id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            provider TEXT NOT NULL DEFAULT 'codex',
            status TEXT NOT NULL,
            requested_at TEXT NOT NULL DEFAULT '2026-10-02T20:00:00Z',
            payload_json TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS orchestration_v2_projection_messages (
            message_id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            created_at TEXT NOT NULL DEFAULT '2026-10-02T20:00:00Z',
            updated_at TEXT NOT NULL DEFAULT '2026-10-02T20:00:00Z',
            payload_json TEXT NOT NULL
         );",
    )
    .unwrap();
    db
}

#[test]
fn children_on_a_named_peer_are_found_for_a_caller_known_only_by_its_session() {
    const PARENT: &str = "mcp:launch:orchestrator";
    let (origin, peer) = pair("children-named-peer");
    // the caller's T3 thread is on the origin, where T3 runs it in CALLER
    let here = t3_state(&origin);
    here.execute(
        "INSERT INTO orchestration_v2_projection_threads
         (thread_id, title, active_provider_thread_id, provider_instance_id)
         VALUES (?1, 'orchestrator', 'provider-thread:caller', 'codex')",
        [PARENT],
    )
    .unwrap();
    here.execute(
        "INSERT INTO orchestration_v2_projection_provider_threads
         (provider_thread_id, thread_id, provider, driver, provider_instance_id, updated_at, payload_json)
         VALUES ('provider-thread:caller', ?1, 'codex', 'codex', 'codex', '2026-10-02T20:00:00Z', ?2)",
        rusqlite::params![
            PARENT,
            json!({ "nativeThreadRef": { "driver": "codex", "nativeId": CALLER } }).to_string()
        ],
    )
    .unwrap();
    // its subagent runs on the peer
    t3_state(&peer)
        .execute(
            "INSERT INTO orchestration_v2_projection_threads (thread_id, title, payload_json)
             VALUES ('mcp:launch:delegate', 'delegate', ?1)",
            [json!({
                "lineage": { "parentThreadId": PARENT, "relationshipToParent": "subagent" }
            })
            .to_string()],
        )
        .unwrap();

    // no T3CODE_THREAD_ID, which an empty value stands for even when the test
    // itself runs in T3: the parent comes from the caller's session here
    let body = ok(&thread_cmd(
        &origin,
        &[("CODEX_THREAD_ID", CALLER), ("T3CODE_THREAD_ID", "")],
        &[
            "context",
            "--children",
            "--machine",
            "children-named-peer-peer",
        ],
    ));
    let children = body["children"].as_array().unwrap();
    assert_eq!(children.len(), 1, "{body}");
    assert_eq!(children[0]["t3_thread"], "mcp:launch:delegate", "{body}");
    assert_eq!(body["children_complete"], true, "{body}");
}

#[test]
fn a_task_thread_on_another_machine_than_machine_names_is_refused() {
    let (origin, _peer) = pair("task-machine");
    // the task's thread is a Codex session on the origin, which submit registers
    let submitted = submit_file(
        &origin,
        &remote_spec(&origin, vec!["true"]),
        RequestId::new(),
    );
    let task = ok(&submitted)["id"].as_str().unwrap().to_owned();
    let lookup = |machine: &str| {
        thread_cmd(
            &origin,
            &[("T3CODE_THREAD_ID", "")],
            &["context", "--task", &task, "--machine", machine],
        )
    };

    let body = ok(&lookup("task-machine-origin"));
    assert_eq!(body["threads"][0]["provider"], "codex", "{body}");
    let out = lookup("task-machine-peer");
    let error: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(2), "{error}");
    assert_eq!(error["error"]["code"], "usage", "{error}");
}

/// One raw HTTP/1.1 POST, as a browser page on the LAN would send it
fn raw_post(daemon: &Daemon, path: &str, origin: Option<&str>, body: &str) -> String {
    use std::io::{Read, Write};

    let address = daemon.address().to_string();
    let host = address.trim_start_matches("http://").trim_end_matches('/');
    let mut stream = std::net::TcpStream::connect(host).unwrap();
    let origin = origin.map_or_else(String::new, |origin| format!("Origin: {origin}\r\n"));
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {host}\r\n{origin}Content-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

#[test]
fn a_browser_page_cannot_compact_a_thread_through_the_fleet_listener() {
    let daemon = Daemon::start("compact-guard", true);
    let body = json!({
        "api_version": 1,
        "protocol_version": 1,
        "destination_machine": daemon.machine_id(),
        "thread": "mcp:launch:anything",
    })
    .to_string();

    let browser = raw_post(
        &daemon,
        "/v1/cluster/threads/compact",
        Some("http://page.example"),
        &body,
    );
    assert!(browser.starts_with("HTTP/1.1 403"), "{browser}");
    assert!(browser.contains("\"permission\""), "{browser}");

    // a Fleet peer sends no Origin and still reaches the route
    let peer = raw_post(&daemon, "/v1/cluster/threads/compact", None, &body);
    assert!(!peer.starts_with("HTTP/1.1 403"), "{peer}");
    // the context lookup only reads, and keeps answering whoever asks
    let lookup = raw_post(
        &daemon,
        "/v1/cluster/threads/context",
        Some("http://page.example"),
        &body,
    );
    assert!(!lookup.starts_with("HTTP/1.1 403"), "{lookup}");
}
