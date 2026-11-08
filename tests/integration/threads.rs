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
    let again = ok(&watch(&["--context-above", "200k"]));
    assert_eq!(again["watches"][0]["watch"], id.as_str());
    assert_eq!(again["watches"][0]["created"], false);
    assert_eq!(
        again["watches"][0]["threshold"],
        json!({ "unit": "tokens", "value": 200_000 })
    );

    let listed = ok(&h.thread_cmd(Some(THREAD), &["watches"]));
    assert_eq!(listed["watches"].as_array().unwrap().len(), 1, "{listed}");
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
