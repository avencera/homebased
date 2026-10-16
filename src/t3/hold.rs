//! Keep a T3 Codex thread in T3's Working shelf with a placeholder run
//!
//! T3 counts a thread with an active run as working, and Codex reports no
//! background work that would park an idle one. A `message.dispatch` with
//! `defer_start` on an idle thread creates a run in `preparing` that T3 never
//! starts by itself, so the thread stays working until something ends it. The
//! placeholder is that run's user message, and its text starts with
//! [`HOLD_MARKER`], which is how a later look at T3's state recognises it
//! without anything remembered in the daemon
//!
//! - Sending a `defer_start` message while a run is active would queue it, and
//!   it would later run as a real turn, so a hold is only ever sent to an idle thread
//! - An event for a held thread is dispatched with `queue_after_active` and
//!   the hold is then interrupted without `holdQueue`; the interrupt drains the
//!   queue, so the event runs as an ordinary turn
//! - A message the user types meanwhile queues behind the hold. Releasing the
//!   hold the same way runs it
//! - The Stop button interrupts the run and holds the queue. The thread's
//!   latest run is then an interrupted placeholder, which [`ThreadActivity::Stopped`]
//!   reports, so a Stop is not undone
//! - A T3 restart cancels `preparing` runs, which leaves the thread idle
//!
//! Runs are read from `statev2.sqlite` rather than through
//! `orchestration.getThreadProjection`, which returns the whole transcript and
//! would be polled every few seconds. The tables are those that
//! `055_OrchestrationV2.ts` creates: `orchestration_v2_projection_runs`, whose
//! `payload_json` holds the run, and `orchestration_v2_projection_messages`

use std::path::Path;

use rusqlite::params;
use serde_json::{Value, json};

use super::v2::{dispatch, dispatch_payload, is_archived};
use super::{ApiFailure, TurnStart, deterministic_id, state_connection};

/// Start of the placeholder message that marks a run as a homebased hold
pub(crate) const HOLD_MARKER: &str = "Waiting on homebased";

// runs T3 treats as active; a thread with one is working, and a message sent
// to it starts nothing now
const ACTIVE_RUNS_SQL: &str = "
    SELECT r.run_id, r.status,
           COALESCE(substr(json_extract(m.payload_json, '$.text'), 1, length(?2)) = ?2, 0)
    FROM orchestration_v2_projection_runs r
    LEFT JOIN orchestration_v2_projection_messages m
      ON m.message_id = json_extract(r.payload_json, '$.userMessageId')
    WHERE r.thread_id = ?1
      AND r.status IN ('preparing', 'queued', 'starting', 'running', 'waiting')
    ORDER BY r.ordinal";
const LATEST_RUN_SQL: &str = "
    SELECT r.ordinal, r.status,
           COALESCE(substr(json_extract(m.payload_json, '$.text'), 1, length(?2)) = ?2, 0)
    FROM orchestration_v2_projection_runs r
    LEFT JOIN orchestration_v2_projection_messages m
      ON m.message_id = json_extract(r.payload_json, '$.userMessageId')
    WHERE r.thread_id = ?1
    ORDER BY r.ordinal DESC
    LIMIT 1";

/// What a T3 thread is doing, as far as holding it matters
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ThreadActivity {
    /// No run is active, so a hold may start
    Idle,
    /// A homebased placeholder is the active run
    Held {
        /// The placeholder run
        run: String,
        /// Another run waits behind it, for example a message the user typed
        queued_behind: bool,
    },
    /// Another run is active
    Busy,
    /// The user pressed Stop on the placeholder, and nothing has run since
    Stopped,
    /// Nothing may run in an archived thread where the user would see it
    Archived,
}

/// A T3 thread's activity and the number of its latest run
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ThreadView {
    /// What the thread is doing now
    pub(crate) activity: ThreadActivity,
    /// Names this idle period, so a retried hold repeats its ids and a later one differs
    pub(crate) key: String,
}

/// Read a thread's runs from the state database
pub(super) fn view(state: &Path, thread_id: &str) -> Result<ThreadView, String> {
    let db = state_connection(state).map_err(|_| "T3 state cannot be read".to_string())?;
    let read = |_| "T3 run query failed".to_string();
    let latest = db
        .query_row(LATEST_RUN_SQL, params![thread_id, HOLD_MARKER], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })
        .ok();
    let mut statement = db.prepare(ACTIVE_RUNS_SQL).map_err(read)?;
    let active = statement
        .query_map(params![thread_id, HOLD_MARKER], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(read)?;

    let key = latest
        .as_ref()
        .map_or_else(|| "0".to_string(), |(ordinal, ..)| ordinal.to_string());
    let activity = if is_archived(state, thread_id) {
        ThreadActivity::Archived
    } else if let Some((run, _, _)) = active
        .iter()
        .find(|(_, status, is_hold)| *is_hold && status == "preparing")
    {
        ThreadActivity::Held {
            run: run.clone(),
            queued_behind: active.iter().any(|(other, ..)| other != run),
        }
    } else if !active.is_empty() {
        ThreadActivity::Busy
    } else if latest
        .as_ref()
        .is_some_and(|(_, status, is_hold)| *is_hold && status == "interrupted")
    {
        ThreadActivity::Stopped
    } else {
        ThreadActivity::Idle
    };
    Ok(ThreadView { activity, key })
}

/// The placeholder run holding `thread_id`, if one does
///
/// Any doubt reads as no hold, so an unreadable state never changes how an
/// event is delivered
pub(super) fn active_hold(state: &Path, thread_id: &str) -> Option<String> {
    match view(state, thread_id).ok()?.activity {
        ThreadActivity::Held { run, .. } => Some(run),
        _ => None,
    }
}

/// Create the placeholder run that holds an idle thread
///
/// `key` names the idle period, so a repeated request creates no second hold
pub(super) fn start(
    origin: &str,
    token: &str,
    thread_id: &str,
    text: &str,
    key: &str,
) -> Result<(), ApiFailure> {
    let command_id = deterministic_id("homebased-t3-hold-command", thread_id, key);
    let message_id = deterministic_id("homebased-t3-hold-message", thread_id, key);
    let payload = dispatch_payload(thread_id, &command_id, &message_id, text, TurnStart::Defer);
    dispatch(origin, token, &payload, None)
}

/// End the placeholder run `run`, so whatever waits behind it runs
pub(super) fn interrupt(
    origin: &str,
    token: &str,
    thread_id: &str,
    run: &str,
    reason: &str,
) -> Result<(), ApiFailure> {
    // no `holdQueue`: that is the Stop button, and it would keep the queue from running
    let payload: Value = json!({
        "type": "run.interrupt",
        "commandId": deterministic_id("homebased-t3-hold-interrupt", thread_id, run),
        "threadId": thread_id,
        "runId": run,
        "reason": reason,
    });
    dispatch(origin, token, &payload, None)
}

/// Deliver `text` to a held thread: queue it behind the placeholder, then end the placeholder
///
/// Only the dispatch decides the outcome. If the interrupt fails the event
/// stays queued and the daemon's next look at the thread sees it waiting
/// behind the hold and ends the hold
pub(super) fn deliver_through(
    origin: &str,
    token: &str,
    thread_id: &str,
    run: &str,
    text: &str,
    before_send: Option<&crate::callback::send_check::SendCheck>,
) -> Result<(), ApiFailure> {
    let command_id = deterministic_id("homebased-t3-command", thread_id, text);
    let message_id = deterministic_id("homebased-t3-message", thread_id, text);
    let payload = dispatch_payload(
        thread_id,
        &command_id,
        &message_id,
        text,
        TurnStart::AfterActive,
    );
    dispatch(origin, token, &payload, before_send)?;
    let _ = interrupt(origin, token, thread_id, run, "homebased event delivered");
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{HOLD_MARKER, ThreadActivity, view};
    use crate::domain::ThreadId;
    use crate::t3::test_support::{
        FakeT3Server, Fixture, SESSION_ID, T3_THREAD_ID, V2State, dispatched, live_pid, ticket,
    };
    use crate::t3::{ProviderThread, TurnStart, WakeOutcome, wake_thread_checked};

    fn hold_text() -> String {
        format!("{HOLD_MARKER}: build")
    }

    fn thread() -> ThreadId {
        ThreadId(SESSION_ID.parse().unwrap())
    }

    fn activity(runs: impl FnOnce(V2State) -> V2State, archived: bool) -> ThreadActivity {
        let fixture = Fixture::empty(None, live_pid());
        runs(
            fixture
                .v2_state()
                .thread(T3_THREAD_ID, "Thread", archived)
                .native("codex", SESSION_ID, T3_THREAD_ID),
        );
        view(&fixture.userdata().join("statev2.sqlite"), T3_THREAD_ID)
            .unwrap()
            .activity
    }

    #[test]
    fn thread_activity_follows_its_runs() {
        assert_eq!(activity(|state| state, false), ThreadActivity::Idle);
        assert_eq!(
            activity(
                |state| state.run(T3_THREAD_ID, 1, "completed", "work"),
                false
            ),
            ThreadActivity::Idle
        );
        assert_eq!(
            activity(|state| state.run(T3_THREAD_ID, 1, "running", "work"), false),
            ThreadActivity::Busy
        );
        // only a run that never started is a placeholder
        assert_eq!(
            activity(
                |state| state.run(T3_THREAD_ID, 1, "running", &hold_text()),
                false
            ),
            ThreadActivity::Busy
        );
        assert_eq!(
            activity(
                |state| state.run(T3_THREAD_ID, 1, "preparing", &hold_text()).run(
                    T3_THREAD_ID,
                    2,
                    "queued",
                    "hello"
                ),
                false
            ),
            ThreadActivity::Held {
                run: format!("run-{T3_THREAD_ID}-1"),
                queued_behind: true
            }
        );
        assert_eq!(
            activity(
                |state| state.run(T3_THREAD_ID, 1, "interrupted", &hold_text()),
                false
            ),
            ThreadActivity::Stopped
        );
        // an interrupted ordinary run is only a stopped turn, not a Stop on a hold
        assert_eq!(
            activity(
                |state| state.run(T3_THREAD_ID, 1, "interrupted", "work"),
                false
            ),
            ThreadActivity::Idle
        );
        assert_eq!(activity(|state| state, true), ThreadActivity::Archived);
    }

    fn deliver(
        runs: impl FnOnce(V2State) -> V2State,
    ) -> (WakeOutcome, Vec<Value>, FakeT3Server, Fixture) {
        let responses = (0..4).flat_map(|_| [ticket(), dispatched()]).collect();
        let server = FakeT3Server::start(Some(2), responses);
        let fixture = Fixture::empty(Some(&server), live_pid());
        runs(
            fixture
                .v2_state()
                .thread(T3_THREAD_ID, "Thread", false)
                .native("codex", SESSION_ID, T3_THREAD_ID),
        );
        let outcome = wake_thread_checked(
            &fixture.env,
            ProviderThread::Codex(thread()),
            "task finished",
            None,
            TurnStart::Auto,
        )
        .unwrap();
        let sent = server
            .requests()
            .iter()
            .filter(|request| request.method == "WS")
            .map(|request| serde_json::from_str::<Value>(&request.body).unwrap()["payload"].clone())
            .collect();
        (outcome, sent, server, fixture)
    }

    #[test]
    fn an_event_for_a_held_thread_is_queued_and_the_hold_interrupted() {
        let (outcome, sent, ..) =
            deliver(|state| state.run(T3_THREAD_ID, 1, "preparing", &hold_text()));

        assert!(matches!(outcome, WakeOutcome::Woken { .. }), "{outcome:?}");
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0]["type"], "message.dispatch");
        assert_eq!(sent[0]["text"], "task finished");
        assert_eq!(
            sent[0]["dispatchMode"],
            json!({ "type": "queue_after_active" })
        );
        assert_eq!(sent[1]["type"], "run.interrupt");
        assert_eq!(sent[1]["runId"], format!("run-{T3_THREAD_ID}-1"));
        assert!(sent[1].get("holdQueue").is_none());
    }

    #[test]
    fn an_event_for_an_unheld_thread_is_delivered_as_before() {
        let (outcome, sent, ..) = deliver(|state| state.run(T3_THREAD_ID, 1, "completed", "work"));

        assert!(matches!(outcome, WakeOutcome::Woken { .. }), "{outcome:?}");
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0]["dispatchMode"],
            json!({ "type": "start_immediately" })
        );
        assert_eq!(sent[0]["deliveryIntent"], "auto");
    }
}
