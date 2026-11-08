//! `homebased thread`: lookups, compaction refusals, and watches end to end

use super::{Harness, THREAD, event_json, wait_until};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::{Value, json};
use std::fs;
use std::io::{BufRead, BufReader};
use std::process::{Output, Stdio};
use std::sync::mpsc::channel;
use std::time::Duration;

const TERMINAL: &str = "0199eeee-2222-7000-8000-000000000006";
const OTHER_THREAD: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e432";

impl Harness {
    /// A Claude Code session outside T3 whose last request carried `tokens`
    fn terminal_session(&self, session: &str, tokens: u64) {
        let project = self.user_home.join(".claude/projects/-work");
        fs::create_dir_all(&project).unwrap();
        let line = json!({
            "type": "assistant",
            "timestamp": (Utc::now() - ChronoDuration::minutes(10)).to_rfc3339(),
            "message": { "usage": { "input_tokens": 2, "cache_read_input_tokens": tokens } }
        });
        fs::write(project.join(format!("{session}.jsonl")), line.to_string()).unwrap();
    }

    /// T3's state database with the tables homebased reads, empty
    fn t3_state(&self) -> rusqlite::Connection {
        let userdata = self.user_home.join(".t3/userdata");
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

    /// A T3 thread whose active provider thread is the Codex thread `session`
    fn t3_thread(&self, thread: &str, title: &str, session: Option<&str>) {
        let db = self.t3_state();
        db.execute(
            "INSERT INTO orchestration_v2_projection_threads (thread_id, title) VALUES (?1, ?2)",
            rusqlite::params![thread, title],
        )
        .unwrap();
        if let Some(session) = session {
            self.t3_session(thread, session);
        }
    }

    /// Make the Codex thread `session` the one T3 runs `thread` in next
    fn t3_session(&self, thread: &str, session: &str) {
        let id = format!("provider-thread:{session}");
        let payload = json!({ "nativeThreadRef": { "driver": "codex", "nativeId": session } });
        let db = self.t3_state();
        db.execute(
            "INSERT INTO orchestration_v2_projection_provider_threads
             (provider_thread_id, thread_id, provider, driver, provider_instance_id, updated_at, payload_json)
             VALUES (?1, ?2, 'codex', 'codex', 'codex', '2026-10-02T20:00:00Z', ?3)",
            rusqlite::params![id, thread, payload.to_string()],
        )
        .unwrap();
        db.execute(
            "UPDATE orchestration_v2_projection_threads
             SET active_provider_thread_id = ?1, provider_instance_id = 'codex' WHERE thread_id = ?2",
            rusqlite::params![id, thread],
        )
        .unwrap();
    }

    fn delete_t3_thread(&self, thread: &str) {
        self.t3_state()
            .execute(
                "UPDATE orchestration_v2_projection_threads SET deleted_at = '2026-10-03T00:00:00Z'
                 WHERE thread_id = ?1",
                [thread],
            )
            .unwrap();
    }

    /// A Codex rollout for `session` in `cwd`, whose last request carried `tokens`
    fn codex_session(&self, session: &str, cwd: &std::path::Path, tokens: u64) {
        let day = self.user_home.join(".codex/sessions/2026/10/02");
        fs::create_dir_all(&day).unwrap();
        let at = (Utc::now() - ChronoDuration::minutes(10)).to_rfc3339();
        let lines = [
            json!({ "type": "session_meta", "payload": { "id": session, "cwd": cwd } }),
            json!({ "timestamp": at, "type": "event_msg", "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": tokens,
                        "cached_input_tokens": 0,
                        "output_tokens": 0,
                        "total_tokens": tokens
                    },
                    "model_context_window": 258_400
                }
            }}),
        ];
        let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
        fs::write(
            day.join(format!("rollout-2026-10-02T20-00-00-{session}.jsonl")),
            text,
        )
        .unwrap();
    }

    fn machine_id(&self) -> String {
        fs::read_to_string(self.home.join("machine-id"))
            .unwrap()
            .trim()
            .to_owned()
    }

    fn thread_cmd(&self, caller: Option<&str>, args: &[&str]) -> Output {
        let mut cmd = self.cmd();
        cmd.env_remove("T3CODE_THREAD_ID");
        if let Some(caller) = caller {
            cmd.env("CODEX_THREAD_ID", caller);
        }
        cmd.args(["--json", "thread"]).args(args).output().unwrap()
    }
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

fn refused(out: &Output, exit: i32, code: &str) {
    let error: Value = serde_json::from_slice(&out.stderr).unwrap_or(Value::Null);
    assert_eq!(out.status.code(), Some(exit), "{error}");
    if !code.is_empty() {
        assert_eq!(error["error"]["code"], code, "{error}");
    }
}

#[test]
fn a_terminal_session_is_found_by_id_or_prefix_but_never_compacted() {
    let h = Harness::new();
    h.terminal_session(TERMINAL, 300_000);

    for reference in [TERMINAL, &TERMINAL[..8]] {
        let body = ok(&h.thread_cmd(None, &["context", reference]));
        let thread = &body["threads"][0];
        assert_eq!(thread["reference"], reference, "{body}");
        assert_eq!(thread["session"], TERMINAL);
        assert_eq!(thread["host"], "terminal");
        assert_eq!(thread["provider"], "claude");
        assert_eq!(thread["state"], "idle");
        assert_eq!(thread["tokens"], 300_002);
        assert_eq!(thread["cache_warm"], true);
    }

    refused(
        &h.thread_cmd(None, &["context", "0199eeee-9999-7000-8000-000000000009"]),
        3,
        "thread_not_found",
    );
    // a prefix is a read convenience; a write needs the exact reference
    refused(
        &h.thread_cmd(None, &["compact", &TERMINAL[..8]]),
        3,
        "thread_not_found",
    );
    refused(
        &h.thread_cmd(None, &["compact", TERMINAL]),
        5,
        "compact_unsupported_host",
    );
    // no reference and no session environment names nothing
    refused(&h.thread_cmd(None, &["context"]), 2, "");
}

#[test]
fn a_watch_is_listed_reused_and_ends_through_wait() {
    let h = Harness::new();
    h.terminal_session(TERMINAL, 300_000);
    let watch = |args: &[&str]| {
        let mut all = vec!["watch", TERMINAL];
        all.extend_from_slice(args);
        h.thread_cmd(Some(THREAD), &all)
    };

    refused(&watch(&["--context-above", "80%"]), 2, "usage");
    refused(
        &watch(&["--context-above", "100k", "--action", "compact"]),
        5,
        "compact_unsupported_host",
    );
    let first = ok(&watch(&["--context-above", "100k"]));
    let id = first["watches"][0]["watch"].as_str().unwrap().to_owned();
    assert_eq!(first["watches"][0]["created"], true);
    assert_eq!(first["watches"][0]["target"]["session"], TERMINAL);
    // a retry is the same watch, and another limit on the thread is a second one
    let again = ok(&watch(&["--context-above", "100k"]));
    assert_eq!(again["watches"][0]["watch"], id.as_str());
    assert_eq!(again["watches"][0]["created"], false);
    let higher = ok(&watch(&["--context-above", "200k"]));
    assert_eq!(higher["watches"][0]["created"], true);
    assert_eq!(
        higher["watches"][0]["threshold"],
        json!({ "unit": "tokens", "value": 200_000 })
    );
    let higher = higher["watches"][0]["watch"].as_str().unwrap().to_owned();
    assert_ne!(higher, id);

    let listed = ok(&h.thread_cmd(Some(THREAD), &["watches"]));
    assert_eq!(listed["watches"].as_array().unwrap().len(), 2, "{listed}");
    ok(&h.thread_cmd(Some(THREAD), &["unwatch", &higher]));
    assert!(
        ok(&h.thread_cmd(Some(OTHER_THREAD), &["watches"]))["watches"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    // another thread cannot take this watch's events
    let out = h
        .cmd()
        .env("CODEX_THREAD_ID", OTHER_THREAD)
        .args(["--json", "wait", &id])
        .output()
        .unwrap();
    refused(&out, 5, "wait_unavailable");

    let mut waiter = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args(["wait", "--next", "--final-only", &id])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = waiter.stdout.take().unwrap();
    let (sender, lines) = channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = sender.send(line);
        }
    });
    // the daemon registers a waiter only while the client is connected
    std::thread::sleep(Duration::from_millis(800));

    let ended = ok(&h.thread_cmd(Some(THREAD), &["unwatch", &id]));
    assert_eq!(ended["watches"][0]["end_reason"], "unwatched");
    let event = event_json(&lines.recv_timeout(Duration::from_secs(20)).unwrap());
    assert_eq!(event["event"], "THREAD_WATCH_ENDED", "{event}");
    assert_eq!(event["watch"], id.as_str());
    assert_eq!(event["seq"], 1);
    assert_eq!(event["reason"], "unwatched");
    assert_eq!(event["thread"], THREAD);
    assert_eq!(event["target"]["reference"], TERMINAL);
    let mut code = None;
    assert!(
        wait_until(Duration::from_secs(20), || {
            code = waiter.try_wait().unwrap().map(|status| status.code());
            code.is_some()
        }),
        "the waiter did not exit"
    );
    assert_eq!(code, Some(Some(0)));

    // the ended watch no longer counts as live, and a second unwatch changes nothing
    assert!(
        ok(&h.thread_cmd(Some(THREAD), &["watches"]))["watches"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let again = ok(&h.thread_cmd(Some(THREAD), &["unwatch", &id]));
    assert_eq!(again["watches"][0]["seq"], 1);
}

#[test]
fn a_final_only_wait_prints_interim_events_but_exits_on_the_final_one() {
    let h = Harness::new();
    let task = h.submit(&h.gated_target("final-only", 0));
    let mut waiter = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args(["wait", "--next", "--final-only", &task])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = waiter.stdout.take().unwrap();
    let (sender, lines) = channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = sender.send(line);
        }
    });
    std::thread::sleep(Duration::from_millis(800));

    let report = h
        .cmd()
        .args([
            "--json",
            "task",
            "report",
            "--id",
            &task,
            "--outcome",
            "blocked",
            "--summary",
            "halfway",
            "--notify",
        ])
        .output()
        .unwrap();
    assert_eq!(
        report.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&report.stderr)
    );
    let interim = event_json(&lines.recv_timeout(Duration::from_secs(20)).unwrap());
    assert_eq!(interim["event"], "TASK_REPORTED", "{interim}");
    // well past the one-second gather a plain `--next` would exit after
    std::thread::sleep(Duration::from_secs(2));
    assert!(
        waiter.try_wait().unwrap().is_none(),
        "the waiter exited on an interim event"
    );

    h.release_gate("final-only");
    let last = event_json(&lines.recv_timeout(Duration::from_secs(20)).unwrap());
    assert!(
        last["event"] == "TASK_SUCCEEDED" || last["event"] == "TASK_BLOCKED",
        "{last}"
    );
    let mut code = None;
    assert!(
        wait_until(Duration::from_secs(20), || {
            code = waiter.try_wait().unwrap().map(|status| status.code());
            code.is_some()
        }),
        "the waiter did not exit"
    );
    assert_eq!(code, Some(Some(0)));
}

#[test]
fn a_message_retry_reaches_the_thread_its_first_attempt_bound() {
    const T3_THREAD: &str = "mcp:launch:message-retry";
    const FIRST: &str = "0199abcd-2222-7000-8000-000000000101";
    const REPLACED: &str = "0199abcd-2222-7000-8000-000000000102";
    let h = Harness::new();
    let cwd = h.user_home.join("work");
    fs::create_dir_all(&cwd).unwrap();
    h.codex_session(FIRST, &cwd, 10_000);
    h.codex_session(REPLACED, &cwd, 10_000);
    h.t3_thread(T3_THREAD, "retry target", Some(FIRST));
    let machine = h.machine_id();
    let message_id = uuid::Uuid::now_v7().to_string();
    let send = || {
        h.cmd()
            .env("CODEX_THREAD_ID", THREAD)
            .args([
                "--json",
                "message",
                "send",
                "--machine",
                &machine,
                "--thread",
                T3_THREAD,
                "--message",
                "Please review",
                "--message-id",
                &message_id,
            ])
            .output()
            .unwrap()
    };

    let first = ok(&send());
    assert_eq!(first["destination_thread"], FIRST, "{first}");

    // T3 moved the thread to a new session: the retry still names the first one
    h.t3_session(T3_THREAD, REPLACED);
    let retried = ok(&send());
    assert_eq!(retried["destination_thread"], FIRST, "{retried}");
    assert_eq!(retried["receipt"], first["receipt"]);

    // and once the thread is gone, the saved receipt still answers
    h.delete_t3_thread(T3_THREAD);
    let after_delete = ok(&send());
    assert_eq!(after_delete["receipt"], first["receipt"]);
}

#[test]
fn compacting_a_t3_thread_with_no_session_is_not_found() {
    const FRESH: &str = "mcp:launch:fresh";
    let h = Harness::new();
    h.t3_thread(FRESH, "fresh", None);
    let context = ok(&h.thread_cmd(None, &["context", FRESH]));
    assert_eq!(context["threads"][0]["host"], "t3", "{context}");
    assert_eq!(context["threads"][0]["session"], Value::Null);

    // an agent tells a missing session from a refusal by the exit code
    refused(
        &h.thread_cmd(None, &["compact", FRESH]),
        3,
        "thread_not_found",
    );
}

#[test]
fn a_chip_ref_reads_back_through_the_message_that_carried_it() {
    const LAUNCHED: &str = "mcp:launch:a9edc0e1-chip";
    const PASTED: &str = "910af7b5-c1a3-4b38-8ad3-95f52dca76f5";
    let h = Harness::new();
    h.t3_thread(LAUNCHED, "Decide the plan", None);
    // T3 keeps the chip's record in the message, and its `ref` is the record's id
    let payload = json!({
        "text": "see [Decide the plan](t3-context://v1/thread/910af7b5)",
        "context": { "records": [
            { "contextId": "unrelated", "kind": "file", "path": "/tmp/x" },
            {
                "contextId": PASTED,
                "kind": "thread",
                "threadId": LAUNCHED,
                "environmentId": "env-chip",
                "title": "Decide the plan"
            }
        ] }
    });
    h.t3_state()
        .execute(
            "INSERT INTO orchestration_v2_projection_messages (message_id, thread_id, payload_json)
             VALUES ('message-chip', 'mcp:launch:orchestrator', ?1)",
            [payload.to_string()],
        )
        .unwrap();

    let chip = format!("[Thread: Decide the plan; ref={PASTED}]");
    let body = ok(&h.thread_cmd(None, &["context", &chip]));
    assert_eq!(body["threads"][0]["t3_thread"], LAUNCHED, "{body}");
    // the `threadId:` line of the t3_context block works as given
    let body = ok(&h.thread_cmd(None, &["context", &format!("threadId: {LAUNCHED}")]));
    assert_eq!(body["threads"][0]["t3_thread"], LAUNCHED, "{body}");

    // a chip with no record here names no thread, and says what to pass instead
    let out = h.thread_cmd(
        None,
        &[
            "context",
            "[Thread: Elsewhere; ref=94791fe2-0000-4000-8000-000000000000]",
        ],
    );
    refused(&out, 3, "thread_not_found");
    let error: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("chip ref is not a thread id; pass the threadId from the t3_context block"),
        "{error}"
    );
}

#[test]
fn a_watch_scan_that_cannot_read_t3_leaves_the_watch_as_it_was() {
    const WATCHED: &str = "mcp:launch:watched-through-outage";
    const SESSION: &str = "0199eeee-3333-7000-8000-000000000001";
    let mut h = Harness::new();
    h.t3_thread(WATCHED, "watched", Some(SESSION));
    h.codex_session(SESSION, &h.user_home.clone(), 50_000);
    let watched = ok(&h.thread_cmd(Some(THREAD), &["watch", WATCHED, "--context-above", "100k"]));
    assert_eq!(watched["watches"][0]["created"], true, "{watched}");
    let live = |h: &Harness| {
        ok(&h.thread_cmd(Some(THREAD), &["watches"]))["watches"]
            .as_array()
            .unwrap()
            .len()
    };

    // a state T3 is migrating reads as an error, never as a thread that is gone
    h.t3_state()
        .execute_batch("DROP TABLE orchestration_v2_projection_runs")
        .unwrap();
    // the scan runs as the daemon starts
    h.restart_daemon();
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(live(&h), 1, "the watch ended while T3 could not be read");

    // once T3 reads again, a deleted thread does end the watch
    h.delete_t3_thread(WATCHED);
    h.restart_daemon();
    assert!(
        wait_until(Duration::from_secs(20), || live(&h) == 0),
        "the scan did not end the watch of a deleted thread"
    );
}

#[test]
fn a_chip_ref_two_messages_reuse_reads_through_the_message_of_the_caller() {
    const CARRIER: &str = "mcp:launch:chip-carrier";
    const ALPHA: &str = "mcp:launch:chip-alpha";
    const BETA: &str = "mcp:launch:chip-beta";
    // a mobile paste mints a ref per message, so two messages can reuse one for different threads
    const REUSED: &str = "5d1e0c3a-8b7f-4e21-9a6d-2f4c8e1b7a90";
    let h = Harness::new();
    h.t3_thread(ALPHA, "Alpha", None);
    h.t3_thread(BETA, "Beta", None);
    let message = |id: &str, thread: &str, at: &str, target: &str| {
        let payload = json!({ "context": { "records": [
            { "contextId": REUSED, "kind": "thread", "threadId": target, "title": target }
        ] } });
        h.t3_state()
            .execute(
                "INSERT INTO orchestration_v2_projection_messages
                 (message_id, thread_id, created_at, updated_at, payload_json)
                 VALUES (?1, ?2, ?3, ?3, ?4)",
                rusqlite::params![id, thread, at, payload.to_string()],
            )
            .unwrap();
    };
    // the caller's chip is the older one, so the newest record names the wrong thread
    message("message-alpha", CARRIER, "2026-10-02T20:00:00Z", ALPHA);
    message(
        "message-beta",
        "mcp:launch:elsewhere",
        "2026-10-02T21:00:00Z",
        BETA,
    );
    let chip = format!("[Thread: Alpha; ref={REUSED}]");

    let out = h
        .cmd()
        .env("T3CODE_THREAD_ID", CARRIER)
        .args(["--json", "thread", "context", &chip])
        .output()
        .unwrap();
    let body = ok(&out);
    assert_eq!(body["threads"][0]["t3_thread"], ALPHA, "{body}");

    // with no carrying message to go by, two targets are a question, not a guess
    let out = h.thread_cmd(None, &["context", &chip]);
    refused(&out, 5, "thread_ref_ambiguous");
    let error: Value = serde_json::from_slice(&out.stderr).unwrap();
    let matches = error["error"]["input"]["matches"].as_array().unwrap();
    let mut named: Vec<&str> = matches
        .iter()
        .map(|found| found["reference"].as_str().unwrap())
        .collect();
    named.sort_unstable();
    assert_eq!(
        named,
        [format!("thread_{ALPHA}"), format!("thread_{BETA}")],
        "{error}"
    );
}
