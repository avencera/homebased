//! Placeholder runs that `--hold` left in T3
//!
//! Releases up to 0.23.0 kept an idle Codex thread in T3's Working shelf with a
//! `message.dispatch` in `defer_start` mode: a run in `preparing` that T3 never
//! starts, whose text starts with [`HOLD_MARKER`] and whose message id
//! [`is_hold_message`] accepts. T3 replays a run that ended
//! before its provider started into the thread's next Codex turn, so every hold
//! cost input tokens, and holds were removed
//!
//! A placeholder still in place when the daemon updated keeps its thread
//! working, and an event or a message the user types queues behind it until
//! someone presses Stop, so each daemon start ends the ones that remain
//!
//! Runs are read from `statev2.sqlite`; the tables are those that
//! `055_OrchestrationV2.ts` creates: `orchestration_v2_projection_runs`, whose
//! `payload_json` holds the run, and `orchestration_v2_projection_messages`

use std::path::Path;

use rusqlite::params;
use serde_json::json;

use super::v2::dispatch;
use super::{ApiFailure, deterministic_id, state_connection};

/// Start of the placeholder message that marks a run as a homebased hold
const HOLD_MARKER: &str = "Waiting on homebased";

// placeholders that T3 has not ended; only `preparing` runs are ones it never started
const LEFTOVER_SQL: &str = "
    SELECT r.thread_id, r.run_id, r.ordinal, m.message_id
    FROM orchestration_v2_projection_runs r
    JOIN orchestration_v2_projection_messages m
      ON m.message_id = json_extract(r.payload_json, '$.userMessageId')
    WHERE r.status = 'preparing'
      AND substr(json_extract(m.payload_json, '$.text'), 1, length(?1)) = ?1
    ORDER BY r.thread_id, r.ordinal";

/// A placeholder run that a hold left in a T3 thread
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LeftoverHold {
    thread: String,
    run: String,
}

/// Id that a hold keyed by the ordinal `key` gave its placeholder message
fn hold_message_id(thread: &str, key: i64) -> String {
    deterministic_id("homebased-t3-hold-message", thread, &key.to_string())
}

/// Whether `message` is the id a hold gave the placeholder that became run `ordinal`
///
/// A hold was keyed by the ordinal of the thread's latest run when it looked,
/// usually the run just before. A run that started between that look and the
/// dispatch pushes the placeholder further, so any earlier key counts. The
/// match keeps the sweep off a real message that starts with [`HOLD_MARKER`]
fn is_hold_message(thread: &str, ordinal: i64, message: &str) -> bool {
    (0..ordinal)
        .rev()
        .any(|key| hold_message_id(thread, key) == message)
}

/// Placeholder runs still waiting in T3's state database
pub(super) fn leftover(state: &Path) -> Result<Vec<LeftoverHold>, String> {
    let db = state_connection(state).map_err(|_| "T3 state cannot be read".to_string())?;
    let mut statement = db
        .prepare(LEFTOVER_SQL)
        .map_err(|_| "T3 run query failed".to_string())?;
    let candidates = statement
        .query_map(params![HOLD_MARKER], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|_| "T3 run query failed".to_string())?;
    Ok(candidates
        .into_iter()
        .filter(|(thread, _, ordinal, message)| is_hold_message(thread, *ordinal, message))
        .map(|(thread, run, ..)| LeftoverHold { thread, run })
        .collect())
}

/// End a placeholder run, so whatever queued behind it runs
pub(super) fn end(origin: &str, token: &str, hold: &LeftoverHold) -> Result<(), ApiFailure> {
    // no `holdQueue`: that is the Stop button, and it would keep the queue from running
    let payload = json!({
        "type": "run.interrupt",
        "commandId": deterministic_id("homebased-t3-hold-interrupt", &hold.thread, &hold.run),
        "threadId": hold.thread,
        "runId": hold.run,
        "reason": "homebased hold ended",
    });
    dispatch(origin, token, &payload, None)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{HOLD_MARKER, hold_message_id};
    use crate::t3::end_leftover_holds;
    use crate::t3::test_support::{
        FakeT3Server, Fixture, SESSION_ID, T3_THREAD_ID, dispatched, live_pid, ticket,
    };

    #[test]
    fn a_start_ends_only_the_placeholders_t3_never_started() {
        let hold = format!("{HOLD_MARKER}: build");
        let responses = (0..4).flat_map(|_| [ticket(), dispatched()]).collect();
        let server = FakeT3Server::start(Some(2), responses);
        let fixture = Fixture::empty(Some(&server), live_pid());
        fixture
            .v2_state()
            .thread(T3_THREAD_ID, "Thread", false)
            .native("codex", SESSION_ID, T3_THREAD_ID)
            // a Stop already ended this one
            .run(T3_THREAD_ID, 1, "interrupted", &hold)
            .run(T3_THREAD_ID, 2, "completed", "work")
            .run_with_message(
                T3_THREAD_ID,
                3,
                "preparing",
                &hold,
                &hold_message_id(T3_THREAD_ID, 2),
            )
            // a first request in a new thread is `preparing` while its workspace is set up
            .run(T3_THREAD_ID, 4, "preparing", &hold)
            // run 5 started after the hold read key 4, so its placeholder became run 6
            .run(T3_THREAD_ID, 5, "failed", "work")
            .run_with_message(
                T3_THREAD_ID,
                6,
                "preparing",
                &hold,
                &hold_message_id(T3_THREAD_ID, 4),
            );

        assert_eq!(end_leftover_holds(&fixture.env), Ok(2));

        let sent: Vec<Value> = server
            .requests()
            .iter()
            .filter(|request| request.method == "WS")
            .map(|request| serde_json::from_str::<Value>(&request.body).unwrap()["payload"].clone())
            .collect();
        let ended: Vec<&Value> = sent.iter().map(|command| &command["runId"]).collect();
        assert_eq!(
            ended,
            [
                &Value::from(format!("run-{T3_THREAD_ID}-3")),
                &Value::from(format!("run-{T3_THREAD_ID}-6")),
            ]
        );
        assert!(sent.iter().all(
            |command| command["type"] == "run.interrupt" && command.get("holdQueue").is_none()
        ));
    }
}
