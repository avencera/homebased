//! `homebased gc` through the daemon: dry run, prune, and the tombstone that
//! `task log` and `task show` report afterwards

use std::time::Duration;

use super::{Harness, wait_until};
use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use serde_json::Value;

fn gc(h: &Harness, args: &[&str]) -> Value {
    let out = h.cmd().args(["--json", "gc"]).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "gc failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

/// Whether every event of `ids` is delivered and settled; until then
/// retention counts a task as live, and settling may still touch its row
fn settled(h: &Harness, ids: &[String]) -> bool {
    let conn = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME)).unwrap();
    ids.iter().all(|id| {
        conn.query_row(
            "SELECT NOT EXISTS (SELECT 1 FROM executor_outbox WHERE task_id = ?1 AND state = 'pending')
                AND NOT EXISTS (SELECT 1 FROM origin_inbox WHERE task_id = ?1 AND settled_at IS NULL)",
            [id],
            |row| row.get::<_, bool>(0),
        )
        .unwrap()
    })
}

fn backdate_end(h: &Harness, id: &str, days_ago: i64) {
    let at =
        (Utc::now() - ChronoDuration::days(days_ago)).to_rfc3339_opts(SecondsFormat::Millis, true);
    let conn = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME)).unwrap();
    conn.execute(
        "UPDATE tasks SET updated_at = ?1 WHERE id = ?2",
        rusqlite::params![at, id],
    )
    .unwrap();
}

#[test]
fn gc_prunes_an_old_finished_task_beyond_keep_last_and_its_log_says_so() {
    let h = Harness::new();
    // keep_last is 3 per thread, so the oldest of four is the one past it
    let ids: Vec<String> = (0..4)
        .map(|_| {
            let id = h.submit(&Harness::task_spec(&["echo", "hello"]));
            h.wait_for_event(&id, "TASK_SUCCEEDED");
            id
        })
        .collect();
    assert!(
        wait_until(Duration::from_secs(30), || settled(&h, &ids)),
        "events never settled"
    );
    for (offset, id) in ids.iter().enumerate() {
        backdate_end(&h, id, 30 - i64::try_from(offset).unwrap());
    }
    let oldest = h.home.join("tasks").join(&ids[0]);

    let dry = gc(&h, &["--dry-run"]);
    assert_eq!(dry["dry_run"], true);
    let planned: Vec<&str> = dry["pruned"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["path"].as_str().unwrap())
        .collect();
    assert_eq!(planned, [oldest.to_str().unwrap()]);
    assert!(oldest.exists(), "a dry run removes nothing");
    assert_eq!(dry["retention"]["tasks"]["keep_last"], 3);

    let pruned = gc(&h, &[]);
    assert_eq!(pruned["dry_run"], false);
    assert_eq!(pruned["pruned"].as_array().unwrap().len(), 1);
    assert!(!oldest.exists());
    for id in &ids[1..] {
        assert!(h.home.join("tasks").join(id).exists());
    }

    let log = h
        .cmd()
        .args(["--json", "task", "log", &ids[0]])
        .output()
        .unwrap();
    assert_eq!(log.status.code(), Some(3));
    let error: Value = serde_json::from_slice(&log.stderr).unwrap();
    assert_eq!(error["error"]["code"], "output_pruned", "{error}");
    let show = h.show(&ids[0]);
    assert!(show["output_pruned_at"].is_string(), "{show}");
    assert_eq!(show["status"], "succeeded");
}
