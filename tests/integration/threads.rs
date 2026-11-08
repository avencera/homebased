//! `homebased thread`: lookups, compaction refusals, and watches end to end

use super::{Harness, THREAD, event_json, wait_until};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::{Value, json};
use std::collections::HashSet;
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

    /// A T3 thread `parent` started as a subagent
    fn t3_child(&self, child: &str, parent: &str, session: Option<&str>) {
        let payload = json!({
            "lineage": { "parentThreadId": parent, "relationshipToParent": "subagent" }
        });
        self.t3_state()
            .execute(
                "INSERT INTO orchestration_v2_projection_threads (thread_id, title, payload_json)
                 VALUES (?1, ?1, ?2)",
                rusqlite::params![child, payload.to_string()],
            )
            .unwrap();
        if let Some(session) = session {
            self.t3_session(child, session);
        }
    }

    /// A delegated task `node` that `parent` gave `child`, as T3 records it in
    /// a subagent node and the subagent row of the same id
    fn t3_task(&self, parent: &str, child: &str, node: &str, status: &str, started_at: &str) {
        let db = self.t3_state();
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS orchestration_v2_projection_nodes (
                node_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                root_node_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                payload_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS orchestration_v2_projection_subagents (
                subagent_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                child_thread_id TEXT,
                origin TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
             );",
        )
        .unwrap();
        db.execute(
            "INSERT INTO orchestration_v2_projection_nodes
             (node_id, thread_id, root_node_id, kind, status, started_at, payload_json)
             VALUES (?1, ?2, ?1, 'subagent', ?3, ?4, '{}')",
            rusqlite::params![node, parent, status, started_at],
        )
        .unwrap();
        db.execute(
            "INSERT INTO orchestration_v2_projection_subagents
             (subagent_id, thread_id, child_thread_id, origin, status, started_at, updated_at,
              payload_json)
             VALUES (?1, ?2, ?3, 'app_owned', ?4, ?5, ?5, ?6)",
            rusqlite::params![
                node,
                parent,
                child,
                status,
                started_at,
                json!({ "childThreadId": child }).to_string()
            ],
        )
        .unwrap();
    }

    /// Settle `thread` in T3's sidebar, or unsettle it
    fn settle_t3_thread(&self, thread: &str, settled: bool) {
        let state = if settled { "settled" } else { "unsettled" };
        self.t3_state()
            .execute(
                "UPDATE orchestration_v2_projection_threads
                 SET payload_json = json_set(payload_json, '$.settledOverride', ?2)
                 WHERE thread_id = ?1",
                rusqlite::params![thread, state],
            )
            .unwrap();
    }

    fn archive_t3_thread(&self, thread: &str) {
        self.t3_state()
            .execute(
                "UPDATE orchestration_v2_projection_threads SET archived_at = '2026-10-03T00:00:00Z'
                 WHERE thread_id = ?1",
                [thread],
            )
            .unwrap();
    }

    /// Stored payloads of `watch`'s events, in order, with how each settled
    fn watch_events(&self, watch: &str) -> Vec<(Value, Option<String>)> {
        let db = rusqlite::Connection::open(self.home.join(homebased::home::DB_NAME)).unwrap();
        let mut statement = db
            .prepare(
                "SELECT event_json, delivery FROM thread_watch_events WHERE watch_id = ?1
                 ORDER BY seq",
            )
            .unwrap();
        statement
            .query_map([watch], |row| {
                let event: String = row.get(0)?;
                Ok((
                    serde_json::from_str::<Value>(&event).unwrap()["payload"].clone(),
                    row.get(1)?,
                ))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    /// Why `watch` ended, or `None` while it is live
    fn watch_end(&self, watch: &str) -> Option<String> {
        let all = ok(&self.thread_cmd(None, &["watches", "--all"]));
        let found = all["watches"]
            .as_array()
            .unwrap()
            .iter()
            .find(|listed| listed["watch"] == watch)
            .unwrap_or_else(|| panic!("{watch} is not listed: {all}"))
            .clone();
        found["end_reason"].as_str().map(str::to_owned)
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

    // a prefix matches a session id in either case, as a full id does
    let upper = TERMINAL[..8].replace('e', "E");
    for reference in [TERMINAL, &TERMINAL[..8], &upper] {
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
            // with an explicit source, a send by thread reference reads no
            // session variable, so a malformed one must not stop it
            .env("CODEX_THREAD_ID", "not-a-thread")
            .args([
                "--json",
                "message",
                "send",
                "--source-thread",
                THREAD,
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

    // once T3 reads again, one scan that misses the thread ends nothing
    h.delete_t3_thread(WATCHED);
    h.restart_daemon();
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(live(&h), 1, "one missing read ended the watch");

    // and the second scan in a row does
    h.restart_daemon_with_env(&[("HOMEBASED_TEST_SCAN_SECS", "1")]);
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

    // newer repeats of one target never hide the older one that makes the ref ambiguous
    for repeat in 0..70 {
        message(
            &format!("message-beta-{repeat}"),
            "mcp:launch:elsewhere",
            "2026-10-02T22:00:00Z",
            BETA,
        );
    }

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

#[test]
fn a_watch_whose_callback_can_no_longer_run_settles_each_event_as_failed() {
    let mut h = Harness::new();
    h.terminal_session(TERMINAL, 300_000);
    let gone = h.user_home.join("gone");
    fs::create_dir_all(&gone).unwrap();
    let out = h
        .cmd()
        .current_dir(&gone)
        .env("CODEX_THREAD_ID", THREAD)
        .args([
            "--json",
            "thread",
            "watch",
            TERMINAL,
            "--context-above",
            "100k",
            "--once",
        ])
        .output()
        .unwrap();
    let watch = ok(&out)["watches"][0]["watch"].as_str().unwrap().to_owned();
    // the agent's directory is removed before any event reaches it
    fs::remove_dir_all(&gone).unwrap();
    // a fresh daemon scans its watches at once instead of a minute on
    h.restart_daemon();

    // the fired event settles as failed rather than retrying forever; it
    // ends the watch itself, so no end event is queued behind it
    let db = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME)).unwrap();
    let settled = || -> Vec<(i64, Option<String>)> {
        let mut statement = db
            .prepare(
                "SELECT seq, delivery FROM thread_watch_events
                 WHERE watch_id = ?1 AND settled_at IS NOT NULL ORDER BY seq",
            )
            .unwrap();
        statement
            .query_map([&watch], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert!(
        wait_until(Duration::from_secs(20), || !settled().is_empty()),
        "{:?}",
        settled()
    );
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(settled(), vec![(1, Some("failed".into()))]);
    assert!(h.queue_messages().is_empty(), "{:?}", h.queue_messages());
}

#[test]
fn a_codex_follow_up_reads_as_running_while_it_resumes_the_thread() {
    const WORKER: &str = "01a0e487-b877-76e2-9dc2-806bff0bf6a1";
    let h = Harness::new();
    let cwd = h.user_home.join("work");
    fs::create_dir_all(&cwd).unwrap();
    h.set_control("stdout", &format!("session id: {WORKER}\n"));
    let first = h.submit(&Harness::spec_with_cwd("codex", "start", &cwd));
    h.wait_status(&first, "succeeded");
    // its rollout's last request was long enough ago to read as idle
    h.codex_session(WORKER, &cwd, 50_000);
    assert_eq!(
        ok(&h.thread_cmd(None, &["context", WORKER]))["threads"][0]["state"],
        "idle"
    );

    // the follow-up records the thread only when it exits, so until then
    // only its resume target ties it to the thread
    h.set_control("sleep", "30");
    let out = h
        .cmd()
        .args(["--json", "task", "followup", &first, "--message", "more"])
        .args(["--thread", THREAD])
        .output()
        .unwrap();
    let followup = ok(&out)["id"].as_str().unwrap().to_owned();
    h.wait_status(&followup, "running");
    let thread = &ok(&h.thread_cmd(None, &["context", WORKER]))["threads"][0];
    assert_eq!(thread["host"], "worker", "{thread}");
    assert_eq!(thread["state"], "running", "{thread}");
}

#[test]
fn a_running_claude_worker_without_a_transcript_is_found_and_live() {
    let h = Harness::new();
    // a worker saves no transcript unless the spec asks it to
    h.set_control("sleep", "30");
    let task = h.submit(&Harness::spec("claude", "work"));
    h.wait_status(&task, "running");

    // the worker's session id is its task id, whole or as a prefix
    for selector in [
        vec!["--worker", task.as_str()],
        vec![task.as_str()],
        vec![&task[..8]],
    ] {
        let mut args = vec!["context"];
        args.extend(selector);
        let thread = &ok(&h.thread_cmd(None, &args))["threads"][0];
        assert_eq!(thread["host"], "worker", "{thread}");
        assert_eq!(thread["provider"], "claude", "{thread}");
        assert_eq!(thread["state"], "running", "{thread}");
        // a watch on it must not end at once as session_gone
        assert_eq!(thread["session_exists"], true, "{thread}");
    }
}

#[test]
fn a_once_watch_fires_one_event_that_ends_it() {
    let mut h = Harness::new();
    h.terminal_session(TERMINAL, 300_000);
    let watched = ok(&h.thread_cmd(
        Some(THREAD),
        &["watch", TERMINAL, "--context-above", "100k", "--once"],
    ));
    let watch = watched["watches"][0]["watch"].as_str().unwrap().to_owned();
    // a fresh daemon scans its watches at once
    h.restart_daemon();

    assert!(
        wait_until(Duration::from_secs(20), || !h.queue_messages().is_empty()),
        "the watch did not fire"
    );
    // time for an end event to follow, if one were sent
    std::thread::sleep(Duration::from_secs(2));
    let delivered = h.queue_messages();
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    let event = event_json(&delivered[0]);
    assert_eq!(event["event"], "THREAD_CONTEXT_OVER", "{event}");
    assert_eq!(event["watch_ended"], true, "{event}");
    assert_eq!(event["end_reason"], "once", "{event}");
    assert_eq!(h.watch_events(&watch).len(), 1);
    assert_eq!(h.watch_end(&watch).as_deref(), Some("once"));
}

#[test]
fn children_watches_and_live_context_leave_out_delegates_that_are_done() {
    const ROOT: &str = "mcp:launch:children-root";
    const SETTLED: &str = "mcp:launch:child-settled";
    const ARCHIVED: &str = "mcp:launch:child-archived";
    const DONE: &str = "mcp:launch:child-done";
    const RESENT: &str = "mcp:launch:child-resent";
    const WORKING: &str = "mcp:launch:child-working";
    const UNTRACKED: &str = "mcp:launch:child-untracked";
    let h = Harness::new();
    h.t3_thread(ROOT, "root", Some(THREAD));
    for child in [SETTLED, ARCHIVED, DONE, RESENT, WORKING, UNTRACKED] {
        h.t3_child(child, ROOT, None);
    }
    h.settle_t3_thread(SETTLED, true);
    h.archive_t3_thread(ARCHIVED);
    h.t3_task(ROOT, DONE, "node-done", "completed", "2026-10-02T20:00:00Z");
    // a message to a finished child starts a new task, so it works again
    h.t3_task(
        ROOT,
        RESENT,
        "node-resent-1",
        "completed",
        "2026-10-02T20:00:00Z",
    );
    h.t3_task(
        ROOT,
        RESENT,
        "node-resent-2",
        "running",
        "2026-10-02T21:00:00Z",
    );
    h.t3_task(
        ROOT,
        WORKING,
        "node-working",
        "running",
        "2026-10-02T20:00:00Z",
    );
    // a child with no task record, as an older T3 made it, reads by its state
    let names = |threads: &Value| -> HashSet<String> {
        threads
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|thread| thread["t3_thread"].as_str())
            .filter(|thread| *thread != ROOT)
            .map(str::to_owned)
            .collect()
    };
    let live = HashSet::from([RESENT, WORKING, UNTRACKED].map(str::to_owned));

    // the ledger keeps every child and marks the finished one
    let ledger = ok(&h.thread_cmd(Some(THREAD), &["context", "--children"]));
    assert_eq!(names(&ledger["children"]).len(), 6, "{ledger}");
    for child in ledger["children"].as_array().unwrap() {
        let finished = child["t3_thread"] == DONE;
        assert_eq!(child["task_finished"], finished, "{child}");
    }
    let working = ok(&h.thread_cmd(Some(THREAD), &["context", "--children", "--live"]));
    assert_eq!(names(&working["children"]), live, "{working}");

    let watched = ok(&h.thread_cmd(
        Some(THREAD),
        &["watch", "--children", "--context-above", "100k"],
    ));
    let targets: Vec<Value> = watched["watches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|watch| watch["target"].clone())
        .collect();
    assert_eq!(names(&Value::Array(targets)), live, "{watched}");
}

#[test]
fn a_done_t3_target_never_fires_and_its_watch_ends_after_an_hour_of_quiet() {
    const PARENT: &str = "mcp:launch:quiet-parent";
    const CHILD: &str = "mcp:launch:quiet-child";
    const SESSION: &str = "0199eeee-4444-7000-8000-000000000001";
    let mut h = Harness::new();
    let cwd = h.user_home.clone();
    h.t3_thread(PARENT, "parent", None);
    h.t3_child(CHILD, PARENT, Some(SESSION));
    h.codex_session(SESSION, &cwd, 150_000);
    h.settle_t3_thread(CHILD, true);
    let watched = ok(&h.thread_cmd(Some(THREAD), &["watch", CHILD, "--context-above", "100k"]));
    let watch = watched["watches"][0]["watch"].as_str().unwrap().to_owned();
    h.restart_daemon_with_env(&[("HOMEBASED_TEST_SCAN_SECS", "1")]);

    // over the limit and idle, but settled: several scans fire nothing
    std::thread::sleep(Duration::from_secs(4));
    assert!(
        h.watch_events(&watch).is_empty(),
        "{:?}",
        h.watch_events(&watch)
    );

    // unsettled, it is a working thread again
    h.settle_t3_thread(CHILD, false);
    assert!(
        wait_until(Duration::from_secs(20), || !h
            .watch_events(&watch)
            .is_empty()),
        "the unsettled thread did not fire"
    );
    let fired = &h.watch_events(&watch)[0].0;
    assert_eq!(fired["event"], "THREAD_CONTEXT_OVER", "{fired}");
    assert_eq!(fired["watch_ended"], false, "{fired}");

    // its task ends; activity keeps the watch while the quiet hour, made 4
    // seconds here, has not passed
    h.t3_task(
        PARENT,
        CHILD,
        "node-quiet",
        "completed",
        "2026-10-02T20:00:00Z",
    );
    h.restart_daemon_with_env(&[
        ("HOMEBASED_TEST_SCAN_SECS", "1"),
        ("HOMEBASED_TEST_WATCH_QUIET_SECS", "4"),
    ]);
    for step in 0..14 {
        h.codex_session(SESSION, &cwd, 150_000 + step * 1_000);
        std::thread::sleep(Duration::from_millis(500));
    }
    assert_eq!(h.watch_end(&watch), None, "activity did not keep the watch");

    assert!(
        wait_until(Duration::from_secs(20), || h.watch_end(&watch).is_some()),
        "the quiet finished thread kept its watch"
    );
    assert_eq!(h.watch_end(&watch).as_deref(), Some("finished"));
    let events = h.watch_events(&watch);
    let (ended, _) = events.last().unwrap();
    assert_eq!(ended["event"], "THREAD_WATCH_ENDED", "{ended}");
    assert_eq!(ended["reason"], "finished", "{ended}");
    assert_eq!(ended["watch_ended"], true, "{ended}");
    assert_eq!(events.len(), 2, "{events:?}");
}

#[test]
fn a_watch_ends_without_waking_its_caller_once_the_caller_is_archived() {
    const ROOT: &str = "mcp:launch:gone-caller";
    let mut h = Harness::new();
    let cwd = h.user_home.clone();
    h.t3_thread(ROOT, "root", Some(THREAD));
    h.codex_session(THREAD, &cwd, 20_000);
    h.terminal_session(TERMINAL, 50_000);
    let watched = ok(&h.thread_cmd(
        Some(THREAD),
        &["watch", TERMINAL, "--context-above", "100k"],
    ));
    let watch = watched["watches"][0]["watch"].as_str().unwrap().to_owned();
    h.restart_daemon_with_env(&[("HOMEBASED_TEST_SCAN_SECS", "1")]);
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(h.watch_end(&watch), None);

    h.archive_t3_thread(ROOT);
    assert!(
        wait_until(Duration::from_secs(20), || h.watch_end(&watch).is_some()),
        "the watch of an archived caller kept running"
    );
    assert_eq!(h.watch_end(&watch).as_deref(), Some("caller_gone"));
    // the end is kept, but delivering it would unarchive the caller
    assert!(
        wait_until(Duration::from_secs(10), || h
            .watch_events(&watch)
            .iter()
            .all(|(_, delivery)| delivery.is_some())),
        "{:?}",
        h.watch_events(&watch)
    );
    let events = h.watch_events(&watch);
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0].0["reason"], "caller_gone");
    assert_eq!(events[0].1.as_deref(), Some("suppressed"));
    assert!(h.queue_messages().is_empty(), "{:?}", h.queue_messages());
}

#[test]
fn a_watch_made_before_caller_threads_were_saved_learns_its_caller_and_follows_it() {
    const ROOT: &str = "mcp:launch:learned-caller";
    const MOVED: &str = "0199eeee-5555-7000-8000-000000000001";
    let mut h = Harness::new();
    let cwd = h.user_home.clone();
    h.t3_thread(ROOT, "root", Some(THREAD));
    h.codex_session(THREAD, &cwd, 20_000);
    h.terminal_session(TERMINAL, 50_000);
    let watched = ok(&h.thread_cmd(
        Some(THREAD),
        &["watch", TERMINAL, "--context-above", "100k"],
    ));
    let watch = watched["watches"][0]["watch"].as_str().unwrap().to_owned();

    // v0.25.0 saved no caller T3 thread
    h.stop_daemon();
    let db = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME)).unwrap();
    let saved = || -> Value {
        let json: String = db
            .query_row(
                "SELECT watch_json FROM thread_watches WHERE id = ?1",
                [&watch],
                |row| row.get(0),
            )
            .unwrap();
        serde_json::from_str::<Value>(&json).unwrap()["caller_t3"].clone()
    };
    assert_eq!(saved(), ROOT);
    db.execute(
        "UPDATE thread_watches SET watch_json = json_remove(watch_json, '$.caller_t3')
         WHERE id = ?1",
        [&watch],
    )
    .unwrap();
    assert_eq!(saved(), Value::Null);

    // the first scan finds the caller by its session and saves its thread
    h.start_daemon_with_env(&[("HOMEBASED_TEST_SCAN_SECS", "1")]);
    assert!(
        wait_until(Duration::from_secs(20), || saved() == ROOT),
        "the scan did not save the caller's T3 thread"
    );

    // once T3 moves the caller to another session, only the saved thread
    // still finds it, and its deletion ends the watch
    h.codex_session(MOVED, &cwd, 20_000);
    h.t3_session(ROOT, MOVED);
    h.delete_t3_thread(ROOT);
    assert!(
        wait_until(Duration::from_secs(20), || h.watch_end(&watch).is_some()),
        "the watch of a deleted caller kept running"
    );
    assert_eq!(h.watch_end(&watch).as_deref(), Some("caller_gone"));
}

#[test]
fn a_pending_event_never_reaches_a_caller_archived_after_its_watch_fired() {
    const ROOT: &str = "mcp:launch:archived-after-fire";
    let mut h = Harness::new();
    let cwd = h.user_home.clone();
    h.t3_thread(ROOT, "root", Some(THREAD));
    h.codex_session(THREAD, &cwd, 20_000);
    h.terminal_session(TERMINAL, 50_000);
    let watch_at = |limit: &str| {
        let out = h.thread_cmd(
            Some(THREAD),
            &["watch", TERMINAL, "--context-above", limit, "--once"],
        );
        ok(&out)["watches"][0]["watch"].as_str().unwrap().to_owned()
    };
    let control = watch_at("100k");
    let late = watch_at("200k");
    let legacy = watch_at("300k");
    // a `--once` watch that fired and ended, while its event waits to go out
    let fired = |h: &Harness, watch: &str| {
        let db = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME)).unwrap();
        let payload = json!({
            "api_version": 1, "event": "THREAD_CONTEXT_OVER", "watch": watch, "seq": 1,
            "thread": THREAD, "watch_ended": true, "end_reason": "once"
        });
        let event = json!({
            "watch": watch, "seq": 1, "kind": "THREAD_CONTEXT_OVER", "payload": payload
        });
        db.execute(
            "UPDATE thread_watches SET armed = 0, event_seq = 1,
             ended_at = '2026-10-02T20:00:00Z', end_reason = 'once' WHERE id = ?1",
            [watch],
        )
        .unwrap();
        db.execute(
            "INSERT INTO thread_watch_events (watch_id, seq, event_json, created_at)
             VALUES (?1, 1, ?2, '2026-10-02T20:00:00Z')",
            rusqlite::params![watch, event.to_string()],
        )
        .unwrap();
    };
    let settled = |h: &Harness, watch: &str| h.watch_events(watch)[0].1.clone();

    // while the caller is live, the event goes out as usual
    h.stop_daemon();
    fired(&h, &control);
    h.start_daemon();
    assert!(
        wait_until(Duration::from_secs(20), || settled(&h, &control).is_some()),
        "the control event never settled"
    );
    assert_ne!(settled(&h, &control).as_deref(), Some("suppressed"));

    // archived before its event goes out, the caller is never woken, and
    // the watch keeps the end it had. A watch from before `caller_t3` was
    // saved finds its caller by the session, so its event is held back too
    h.stop_daemon();
    fired(&h, &late);
    fired(&h, &legacy);
    rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME))
        .unwrap()
        .execute(
            "UPDATE thread_watches SET watch_json = json_remove(watch_json, '$.caller_t3')
             WHERE id = ?1",
            [&legacy],
        )
        .unwrap();
    h.archive_t3_thread(ROOT);
    let before = h.queue_messages();
    h.start_daemon();
    for watch in [&late, &legacy] {
        assert!(
            wait_until(Duration::from_secs(20), || settled(&h, watch).is_some()),
            "the event of an archived caller never settled"
        );
        assert_eq!(settled(&h, watch).as_deref(), Some("suppressed"));
        assert_eq!(h.watch_end(watch).as_deref(), Some("once"));
        assert_eq!(h.watch_events(watch).len(), 1);
    }
    assert_eq!(h.queue_messages(), before);
}
