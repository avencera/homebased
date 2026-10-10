//! T3 orchestration protocol 1, the contract of T3 Code stable `0.0.45` and earlier
//!
//! Pinned at T3 Code commit `95030dc67` on 2026-09-26 and verified with stable
//! `0.0.42` through `0.0.45`. Delete this module once every machine runs
//! protocol 2
//!
//! - `state.sqlite` has `provider_session_runtime(provider_name,thread_id,
//!   resume_cursor_json,last_seen_at)` and `projection_threads(thread_id,
//!   deleted_at)`. Claude sessions use `provider_name = 'claudeAgent'` and
//!   `json_extract(resume_cursor_json, '$.resume')`; Codex threads use
//!   `provider_name = 'codex'` and `json_extract(resume_cursor_json,
//!   '$.threadId')`. `thread_id` is T3's thread id. A join on `thread_id`
//!   excludes deleted threads, and the newest `last_seen_at` wins
//! - `GET /api/orchestration/threads/{threadId}?turnLimit=1` returns a `thread`
//!   with `id`, `runtimeMode`, `interactionMode`, `archivedAt`, and `deletedAt`
//!   on HTTP 200. HTTP 401 or 403 with a fresh token indicates a changed API;
//!   HTTP 404 has a typed error object with `_tag` and optional `reason`
//! - `POST /api/orchestration/dispatch` accepts `thread.turn.start` with
//!   `commandId`, `threadId`, `message`, `runtimeMode`, `interactionMode`, and
//!   `createdAt`; its `message` has `messageId`, `role`, `text`, and
//!   `attachments`. `modelSelection` is omitted. HTTP 200 returns integer
//!   `sequence`; HTTP 400 indicates a changed command shape. Unknown thread ids
//!   return HTTP 500 with `reason: orchestration_dispatch_failed`. HTTP 401 or
//!   403 indicates a changed API, and HTTP 404 can return a typed error object
//! - The same route accepts `thread.unarchive` with `commandId` and `threadId`.
//!   A turn in an archived thread is recorded but never starts, so homebased
//!   unarchives the thread first
//! - A `thread.turn.start` whose text is exactly `/compact` runs as a
//!   compaction (`ProviderCommandReactor.ts`) when no turn is running. Turns
//!   that arrive during it wait in memory and start once it finishes. A
//!   repeated `commandId` replays its first result, so each compaction needs
//!   its own id

use std::path::Path;

use chrono::{SecondsFormat, Utc};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    ApiFailure, COMPACT_COMMAND, ClaudeThreadRow, ProbeCheck, ProbeStatus, ProviderThread,
    claude_thread_rows, deterministic_id, failed, non_empty_string, passed, percent_encode,
    query_thread, send_empty, send_json, state_connection,
};
use crate::curl::{CurlMethod, CurlResponse};

const PROVIDER_THREAD_SQL: &str = "
    SELECT r.thread_id
    FROM provider_session_runtime r
    JOIN projection_threads t ON t.thread_id = r.thread_id
    WHERE t.deleted_at IS NULL
      AND r.provider_name = ?1
      AND CASE WHEN json_valid(r.resume_cursor_json)
          THEN json_extract(r.resume_cursor_json, ?2) = ?3
          ELSE 0 END
    ORDER BY r.last_seen_at DESC
    LIMIT 1";
const REQUIRED_STATE_SQL: &str = "
    SELECT r.thread_id, r.provider_name, r.resume_cursor_json, r.last_seen_at,
           t.thread_id, t.deleted_at
    FROM provider_session_runtime r
    JOIN projection_threads t ON t.thread_id = r.thread_id
    LIMIT 0";
const LATEST_THREAD_SQL: &str = "
    SELECT r.thread_id
    FROM provider_session_runtime r
    JOIN projection_threads t ON t.thread_id = r.thread_id
    WHERE t.deleted_at IS NULL
    ORDER BY r.last_seen_at DESC
    LIMIT 1";

const OPEN_CLAUDE_THREADS_SQL: &str = "
    SELECT json_extract(r.resume_cursor_json, '$.resume'), t.thread_id, t.title,
           coalesce(t.settled_override = 'settled', 0)
    FROM provider_session_runtime r
    JOIN projection_threads t ON t.thread_id = r.thread_id
    WHERE r.provider_name = 'claudeAgent'
      AND t.deleted_at IS NULL AND t.archived_at IS NULL
      AND json_valid(r.resume_cursor_json)
      AND json_type(r.resume_cursor_json, '$.resume') = 'text'
    ORDER BY r.last_seen_at DESC";

/// Claude sessions whose thread is neither archived nor deleted, newest first
pub(super) fn open_claude_threads(path: &Path) -> Result<Vec<ClaudeThreadRow>, String> {
    claude_thread_rows(path, OPEN_CLAUDE_THREADS_SQL)
}

/// T3 thread that owns `provider_thread`, if any
pub(super) fn find_thread(
    path: &Path,
    provider_thread: ProviderThread,
) -> Result<Option<String>, String> {
    let (provider, cursor, session) = provider_thread.mapping();
    query_thread(
        path,
        PROVIDER_THREAD_SQL,
        rusqlite::params![provider, cursor, session.to_string()],
    )
}

pub(super) fn schema_matches(path: &Path) -> bool {
    let Ok(db) = state_connection(path) else {
        return false;
    };
    db.prepare(REQUIRED_STATE_SQL).is_ok()
}

/// Start a turn in `thread_id` with the thread's own runtime and interaction modes
///
/// An archived thread is unarchived first, because V1 records a turn there but
/// never starts it
pub(super) fn start_turn(
    origin: &str,
    thread_id: &str,
    text: &str,
    token: &str,
    before_send: Option<&crate::callback::send_check::SendCheck>,
) -> Result<(), ApiFailure> {
    let snapshot = fetch_snapshot(origin, thread_id, token)?;
    if snapshot.deleted {
        return Err(ApiFailure::Refused("T3 thread is deleted".into()));
    }
    if snapshot.archived {
        unarchive(origin, thread_id, text, token)?;
    }

    let turn = Turn {
        command_id: deterministic_id("homebased-t3-command", thread_id, text),
        message_id: deterministic_id("homebased-t3-message", thread_id, text),
        text,
    };
    dispatch_turn(origin, thread_id, &snapshot, &turn, token, before_send)
}

/// Start a `/compact` turn in `thread_id`, keyed by `key`
///
/// The ids derive from `key`, not the text, so each idle period compacts once
/// while a later one can compact again. T3 holds turns that arrive during the
/// compaction and starts them once it finishes. An archived thread is left
/// alone, because V1 records a turn there but never starts it
pub(super) fn compact(
    origin: &str,
    thread_id: &str,
    key: &str,
    token: &str,
) -> Result<(), ApiFailure> {
    let snapshot = fetch_snapshot(origin, thread_id, token)?;
    if snapshot.deleted || snapshot.archived {
        return Err(ApiFailure::Refused(
            "T3 thread is deleted or archived".into(),
        ));
    }

    let turn = Turn {
        command_id: deterministic_id("homebased-t3-compact-command", thread_id, key),
        message_id: deterministic_id("homebased-t3-compact-message", thread_id, key),
        text: COMPACT_COMMAND,
    };
    dispatch_turn(origin, thread_id, &snapshot, &turn, token, None)
}

/// One user message to start as a turn, with ids T3 deduplicates by
struct Turn<'a> {
    command_id: String,
    message_id: String,
    text: &'a str,
}

/// Check the snapshot and dispatch contracts without starting a turn
pub(super) fn probe_api(
    origin: &str,
    state: &Path,
    token: &str,
    checks: &mut Vec<ProbeCheck>,
) -> ProbeStatus {
    let latest_thread = match query_thread(state, LATEST_THREAD_SQL, []) {
        Ok(thread) => thread,
        Err(detail) => {
            checks.push(failed("thread_snapshot", detail));
            return ProbeStatus::Changed;
        }
    };
    if let Some(thread_id) = latest_thread {
        match fetch_snapshot(origin, &thread_id, token) {
            Ok(_) => checks.push(passed(
                "thread_snapshot",
                "thread snapshot has the required fields",
            )),
            Err(error) => {
                checks.push(failed("thread_snapshot", error.detail()));
                return ProbeStatus::Changed;
            }
        }
    } else {
        checks.push(passed(
            "thread_snapshot",
            "skipped because no non-deleted T3 thread is available",
        ));
    }

    let unknown_thread = Uuid::now_v7().to_string();
    let probe_body = dispatch_body(
        &unknown_thread,
        &Uuid::now_v7().to_string(),
        &Uuid::now_v7().to_string(),
        "homebased compatibility probe",
        "full-access",
        "default",
    );
    let response = match send_json(
        CurlMethod::Post,
        &format!("{origin}/api/orchestration/dispatch"),
        token,
        &probe_body,
    ) {
        Ok(response) => response,
        Err(detail) => {
            checks.push(failed("dispatch", detail));
            return ProbeStatus::Changed;
        }
    };
    if !is_unknown_thread_response(&response) {
        checks.push(failed("dispatch", dispatch_probe_failure(&response)));
        return ProbeStatus::Changed;
    }
    checks.push(passed(
        "dispatch",
        "unknown thread returned orchestration_dispatch_failed",
    ));
    ProbeStatus::Compatible
}

struct ThreadSnapshot {
    runtime_mode: String,
    interaction_mode: String,
    archived: bool,
    deleted: bool,
}

fn fetch_snapshot(
    origin: &str,
    thread_id: &str,
    token: &str,
) -> Result<ThreadSnapshot, ApiFailure> {
    let url = format!(
        "{origin}/api/orchestration/threads/{}?turnLimit=1",
        percent_encode(thread_id)
    );
    let response = send_empty(CurlMethod::Get, &url, token).map_err(ApiFailure::Unavailable)?;
    if response.status == 401 || response.status == 403 {
        return Err(ApiFailure::Changed(format!(
            "thread snapshot returned HTTP {} with a fresh session token",
            response.status
        )));
    }
    if response.status == 404 {
        return Err(classify_typed_404(&response.body, "thread snapshot"));
    }
    if response.status != 200 {
        return Err(ApiFailure::Unavailable(format!(
            "thread snapshot returned HTTP {}",
            response.status
        )));
    }

    let value: Value = serde_json::from_str(&response.body)
        .map_err(|_| ApiFailure::Changed("thread snapshot is not valid JSON".into()))?;
    let Some(thread) = value.get("thread").and_then(Value::as_object) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no thread object".into(),
        ));
    };
    let Some(id) = thread.get("id").and_then(Value::as_str) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no thread id".into(),
        ));
    };
    if id != thread_id {
        return Err(ApiFailure::Changed(
            "thread snapshot id does not match the requested thread".into(),
        ));
    }
    let Some(runtime_mode) = non_empty_string(thread.get("runtimeMode")) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no valid runtimeMode".into(),
        ));
    };
    let Some(interaction_mode) = non_empty_string(thread.get("interactionMode")) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no valid interactionMode".into(),
        ));
    };
    let Some(archived_at) = timestamp_field(thread.get("archivedAt")) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no valid archivedAt".into(),
        ));
    };
    let Some(deleted_at) = timestamp_field(thread.get("deletedAt")) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no valid deletedAt".into(),
        ));
    };

    Ok(ThreadSnapshot {
        runtime_mode,
        interaction_mode,
        archived: archived_at,
        deleted: deleted_at,
    })
}

/// Unarchive `thread_id`; an error means the turn must not be sent
fn unarchive(origin: &str, thread_id: &str, text: &str, token: &str) -> Result<(), ApiFailure> {
    let body = json!({
        "type": "thread.unarchive",
        "commandId": deterministic_id("homebased-t3-unarchive", thread_id, text),
        "threadId": thread_id
    });
    let response = send_json(
        CurlMethod::Post,
        &format!("{origin}/api/orchestration/dispatch"),
        token,
        &body,
    )
    .map_err(ApiFailure::Unavailable)?;
    if response.status == 200 {
        return Ok(());
    }
    Err(ApiFailure::Unavailable(format!(
        "unarchiving the T3 thread returned HTTP {}",
        response.status
    )))
}

fn timestamp_field(value: Option<&Value>) -> Option<bool> {
    match value? {
        Value::Null => Some(false),
        Value::String(timestamp) if !timestamp.trim().is_empty() => Some(true),
        _ => None,
    }
}

fn dispatch_turn(
    origin: &str,
    thread_id: &str,
    snapshot: &ThreadSnapshot,
    turn: &Turn<'_>,
    token: &str,
    before_send: Option<&crate::callback::send_check::SendCheck>,
) -> Result<(), ApiFailure> {
    let body = dispatch_body(
        thread_id,
        &turn.command_id,
        &turn.message_id,
        turn.text,
        &snapshot.runtime_mode,
        &snapshot.interaction_mode,
    );
    if let Some(check) = before_send {
        check().map_err(ApiFailure::Delivery)?;
    }
    // a transport error can follow a request that T3 already took
    let response = send_json(
        CurlMethod::Post,
        &format!("{origin}/api/orchestration/dispatch"),
        token,
        &body,
    )
    .map_err(ApiFailure::Uncertain)?;
    if response.status == 401 || response.status == 403 {
        return Err(ApiFailure::Changed(format!(
            "turn dispatch returned HTTP {} with a fresh session token",
            response.status
        )));
    }
    if response.status == 404 {
        return Err(classify_typed_404(&response.body, "turn dispatch"));
    }
    if response.status == 400 {
        return Err(ApiFailure::Changed(
            "turn dispatch returned HTTP 400".into(),
        ));
    }
    if response.status == 500 && has_dispatch_failed_reason(&response.body) {
        return Err(ApiFailure::Refused(
            "T3 could not dispatch the turn for this thread".into(),
        ));
    }
    if response.status != 200 {
        return Err(ApiFailure::Unavailable(format!(
            "turn dispatch returned HTTP {}",
            response.status
        )));
    }

    let value: Value = serde_json::from_str(&response.body)
        .map_err(|_| ApiFailure::Changed("turn dispatch response is not valid JSON".into()))?;
    if value.get("sequence").and_then(Value::as_u64).is_none() {
        return Err(ApiFailure::Changed(
            "turn dispatch response has no integer sequence".into(),
        ));
    }
    Ok(())
}

fn dispatch_body(
    thread_id: &str,
    command_id: &str,
    message_id: &str,
    text: &str,
    runtime_mode: &str,
    interaction_mode: &str,
) -> Value {
    json!({
        "type": "thread.turn.start",
        "commandId": command_id,
        "threadId": thread_id,
        "message": {
            "messageId": message_id,
            "role": "user",
            "text": text,
            "attachments": []
        },
        "runtimeMode": runtime_mode,
        "interactionMode": interaction_mode,
        "createdAt": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
    })
}

fn classify_typed_404(body: &str, operation: &str) -> ApiFailure {
    let typed_error = serde_json::from_str::<Value>(body)
        .ok()
        .filter(Value::is_object);
    if let Some(tag) = typed_error
        .as_ref()
        .and_then(|value| value.get("_tag"))
        .and_then(Value::as_str)
        .filter(|tag| !tag.trim().is_empty())
    {
        let reason = typed_error
            .as_ref()
            .and_then(|value| value.get("reason"))
            .and_then(Value::as_str)
            .filter(|reason| !reason.trim().is_empty());
        let detail = reason.map_or_else(
            || format!("{operation} returned typed T3 error {tag}"),
            |reason| format!("{operation} returned typed T3 error {tag}: {reason}"),
        );
        ApiFailure::Refused(detail)
    } else {
        ApiFailure::Changed(format!(
            "{operation} returned HTTP 404 without a typed T3 error"
        ))
    }
}

fn has_dispatch_failed_reason(body: &str) -> bool {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|reason| reason == "orchestration_dispatch_failed")
}

fn is_unknown_thread_response(response: &CurlResponse) -> bool {
    response.status == 500 && has_dispatch_failed_reason(&response.body)
}

fn dispatch_probe_failure(response: &CurlResponse) -> String {
    if response.status == 400 {
        return "dispatch returned HTTP 400; command shape changed".into();
    }
    if response.status == 500 {
        return "dispatch did not return orchestration_dispatch_failed".into();
    }
    format!("dispatch contract probe returned HTTP {}", response.status)
}

#[cfg(test)]
mod tests {
    use super::{ApiFailure, classify_typed_404};

    #[test]
    fn typed_404_drops_trace_id_from_refusal_detail() {
        let error = classify_typed_404(
            r#"{"_tag":"SomeT3Error","reason":"thread_closed","traceId":"secret-trace"}"#,
            "thread snapshot",
        );
        assert!(
            matches!(error, ApiFailure::Refused(detail) if detail == "thread snapshot returned typed T3 error SomeT3Error: thread_closed")
        );
    }
}
