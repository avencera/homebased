//! T3 orchestration protocol 2, the orchestrator from pingdotgg/t3code#2829
//!
//! Pinned at T3 Code `main` commit `e9298af6` on 2026-10-02 and verified with
//! preview `0.0.46-preview.20261002.2598`; no nightly or stable release had it
//! yet. When V2 reaches stable and `homebased t3 check` fails, compare these
//! files in pingdotgg/t3code: `packages/contracts/src/environmentHttp.ts` (HTTP
//! routes), `packages/contracts/src/orchestrationV2.ts` (`message.dispatch` and
//! `ORCHESTRATION_V2_WS_METHODS`), and
//! `apps/server/src/persistence/Migrations/055_OrchestrationV2.ts` (tables).
//! V2 removed `POST /api/orchestration/dispatch`, which is why a turn goes over
//! the client WebSocket in [`super::rpc`]. If `environmentHttp.ts` gains an
//! authenticated HTTP route that dispatches commands or wakes a thread, use it
//! and delete the WebSocket client
//!
//! - `statev2.sqlite` replaces `state.sqlite`, which V2 copies once at its first
//!   start and never writes again. `orchestration_v2_projection_provider_threads`
//!   maps `provider` (`claudeAgent` or `codex`) and
//!   `payload_json.$.nativeThreadRef.nativeId` (the Claude session or Codex
//!   thread id) to `thread_id`; `orchestration_v2_projection_threads` has
//!   `archived_at` and `deleted_at`
//! - Threads imported from V1 keep their ids but get no provider thread, so
//!   their sessions resolve only through the copied V1 `provider_session_runtime`
//!   table. A turn there starts a new provider session with a context handoff
//! - `orchestration.dispatchCommand` takes `message.dispatch` and returns integer
//!   `sequence`; a repeated `commandId` returns the first result without a second
//!   turn. An unknown thread fails with `OrchestrationV2DispatchCommandError`
//! - V2 runs a turn in an archived thread and leaves it archived, where nobody
//!   sees it, so homebased sends `thread.unarchive` first
//! - A `message.dispatch` whose text is exactly `/compact` runs as a
//!   compaction turn (`RunExecutionService.ts`). T3 rejects a message that
//!   would steer into it, so the message after it uses `queue_after_active`
//!   without `deliveryIntent`, like the web client's "compact first" send

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use uuid::Uuid;

use super::rpc::{self, Exit};
use super::{
    ApiFailure, ProbeCheck, ProbeStatus, ProviderThread, TurnStart, deterministic_id, failed,
    passed, state_connection,
};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const DISPATCH_METHOD: &str = "orchestration.dispatchCommand";
const DISPATCH_ERROR: &str = "OrchestrationV2DispatchCommandError";
/// Message text that T3 runs as a compaction turn instead of a prompt
const COMPACT_COMMAND: &str = "/compact";
const NATIVE_THREAD_SQL: &str = "
    SELECT t.thread_id
    FROM orchestration_v2_projection_provider_threads p
    JOIN orchestration_v2_projection_threads t ON t.thread_id = p.thread_id
    WHERE t.deleted_at IS NULL
      AND p.provider = ?1
      AND CASE WHEN json_valid(p.payload_json)
          THEN json_extract(p.payload_json, '$.nativeThreadRef.nativeId') = ?2
          ELSE 0 END
    ORDER BY p.updated_at DESC
    LIMIT 1";
const LEGACY_THREAD_SQL: &str = "
    SELECT t.thread_id
    FROM provider_session_runtime r
    JOIN orchestration_v2_projection_threads t ON t.thread_id = r.thread_id
    WHERE t.deleted_at IS NULL
      AND r.provider_name = ?1
      AND CASE WHEN json_valid(r.resume_cursor_json)
          THEN json_extract(r.resume_cursor_json, ?2) = ?3
          ELSE 0 END
    ORDER BY r.last_seen_at DESC
    LIMIT 1";
const LEGACY_TABLE_SQL: &str =
    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'provider_session_runtime'";
const ARCHIVED_SQL: &str =
    "SELECT archived_at IS NOT NULL FROM orchestration_v2_projection_threads WHERE thread_id = ?1";
const REQUIRED_STATE_SQL: &str = "
    SELECT p.thread_id, p.provider, p.payload_json, p.updated_at,
           t.thread_id, t.title, t.archived_at, t.deleted_at
    FROM orchestration_v2_projection_provider_threads p
    JOIN orchestration_v2_projection_threads t ON t.thread_id = p.thread_id
    LIMIT 0";

/// T3 thread that owns `provider_thread`, if any
pub(super) fn find_thread(
    path: &Path,
    provider_thread: ProviderThread,
) -> Result<Option<String>, String> {
    let db = state_connection(path).map_err(|_| "statev2.sqlite cannot be read".to_string())?;
    let (provider, cursor, session) = provider_thread.mapping();
    let session = session.to_string();
    let native = db
        .query_row(NATIVE_THREAD_SQL, [provider, &session], |row| row.get(0))
        .optional()
        .map_err(|_| "statev2.sqlite thread lookup failed".to_string())?;
    if native.is_some() || !has_legacy_table(&db)? {
        return Ok(native);
    }

    db.query_row(
        LEGACY_THREAD_SQL,
        rusqlite::params![provider, cursor, session],
        |row| row.get(0),
    )
    .optional()
    .map_err(|_| "statev2.sqlite legacy thread lookup failed".to_string())
}

fn has_legacy_table(db: &Connection) -> Result<bool, String> {
    db.query_row(LEGACY_TABLE_SQL, [], |_| Ok(()))
        .optional()
        .map(|table| table.is_some())
        .map_err(|_| "statev2.sqlite table lookup failed".to_string())
}

pub(super) fn schema_matches(path: &Path) -> bool {
    let Ok(db) = state_connection(path) else {
        return false;
    };
    db.prepare(REQUIRED_STATE_SQL).is_ok()
}

/// Send `text` to `thread_id` as a message that T3 starts, steers, or queues
///
/// An archived thread is unarchived first. V2 would run the turn while the
/// thread stays archived, where nobody sees it. With
/// [`TurnStart::CompactFirst`], a `/compact` turn runs first and `text` queues
/// behind it
pub(super) fn start_turn(
    origin: &str,
    state: &Path,
    thread_id: &str,
    text: &str,
    token: &str,
    before_send: Option<&crate::callback::send_check::SendCheck>,
    start: TurnStart,
) -> Result<(), ApiFailure> {
    // the turn reaches the agent either way, so a failed unarchive only costs visibility
    if is_archived(state, thread_id) {
        let command_id = deterministic_id("homebased-t3-unarchive", thread_id, text);
        let unarchive = json!({
            "type": "thread.unarchive",
            "commandId": command_id,
            "threadId": thread_id
        });
        let _ = rpc::call(origin, token, DISPATCH_METHOD, &unarchive, RPC_TIMEOUT);
    }

    let command_id = deterministic_id("homebased-t3-command", thread_id, text);
    let message_id = deterministic_id("homebased-t3-message", thread_id, text);
    let delivery = match start {
        TurnStart::Auto => Delivery::Auto,
        TurnStart::AfterActive | TurnStart::CompactFirst => Delivery::AfterActive,
    };
    let payload = dispatch_payload(thread_id, &command_id, &message_id, text, delivery);
    if start == TurnStart::CompactFirst {
        return compact_then_send(origin, thread_id, text, token, &payload, before_send);
    }
    dispatch(origin, token, &payload, before_send)
}

/// Compact the thread, keyed by the message `text`, then send `payload`
///
/// T3 rejects steering into a compaction, so `payload` waits for the active
/// run. A refused compaction most likely met one an earlier message started,
/// so the message waits either way
fn compact_then_send(
    origin: &str,
    thread_id: &str,
    text: &str,
    token: &str,
    payload: &Value,
    before_send: Option<&crate::callback::send_check::SendCheck>,
) -> Result<(), ApiFailure> {
    let compaction_may_run = match compact(origin, thread_id, text, token, before_send) {
        Ok(()) | Err(ApiFailure::Uncertain(_)) => true,
        Err(failure @ ApiFailure::Delivery(_)) => return Err(failure),
        // compaction is best effort, so the message still goes
        Err(_) => false,
    };
    match dispatch(origin, token, payload, before_send) {
        // a socket fallback would reach the session mid-compaction, so a
        // failure keeps retries on T3, which drops the repeated compaction
        Err(failure) if compaction_may_run && !matches!(failure, ApiFailure::Delivery(_)) => {
            Err(ApiFailure::Uncertain(failure.detail()))
        }
        sent => sent,
    }
}

/// Start a `/compact` turn, keyed by `key`
///
/// The ids derive from `key`, so a retry for the same message or idle period
/// does not compact twice, while a later one can compact again. T3 refuses it
/// while a run is active
pub(super) fn compact(
    origin: &str,
    thread_id: &str,
    key: &str,
    token: &str,
    before_send: Option<&crate::callback::send_check::SendCheck>,
) -> Result<(), ApiFailure> {
    let command_id = deterministic_id("homebased-t3-compact-command", thread_id, key);
    let message_id = deterministic_id("homebased-t3-compact-message", thread_id, key);
    let payload = dispatch_payload(
        thread_id,
        &command_id,
        &message_id,
        COMPACT_COMMAND,
        Delivery::Auto,
    );
    dispatch(origin, token, &payload, before_send)
}

fn dispatch(
    origin: &str,
    token: &str,
    payload: &Value,
    before_send: Option<&crate::callback::send_check::SendCheck>,
) -> Result<(), ApiFailure> {
    match rpc::call_checked(
        origin,
        token,
        DISPATCH_METHOD,
        payload,
        RPC_TIMEOUT,
        before_send,
    ) {
        Ok(Exit::Success) => Ok(()),
        // only T3's typed refusal proves it did not take the message
        Ok(Exit::Failure(cause)) => match dispatch_failure(&cause) {
            ApiFailure::Refused(detail) => Err(ApiFailure::Refused(detail)),
            failure => Err(ApiFailure::Uncertain(failure.detail())),
        },
        Err(error) if error.sent => Err(ApiFailure::Uncertain(error.failure.detail())),
        Err(error) => Err(error.failure),
    }
}

fn is_archived(state: &Path, thread_id: &str) -> bool {
    state_connection(state)
        .ok()
        .and_then(|db| {
            db.query_row(ARCHIVED_SQL, [thread_id], |row| row.get::<_, bool>(0))
                .optional()
                .ok()
                .flatten()
        })
        .unwrap_or(false)
}

/// Check the dispatch contract with an unknown thread, which starts no turn
pub(super) fn probe_api(origin: &str, token: &str, checks: &mut Vec<ProbeCheck>) -> ProbeStatus {
    let payload = dispatch_payload(
        &Uuid::now_v7().to_string(),
        &Uuid::now_v7().to_string(),
        &Uuid::now_v7().to_string(),
        "homebased compatibility probe",
        Delivery::Auto,
    );
    let failure = match rpc::call(origin, token, DISPATCH_METHOD, &payload, RPC_TIMEOUT) {
        Ok(Exit::Failure(cause)) => dispatch_failure(&cause),
        Ok(Exit::Success) => {
            ApiFailure::Changed("message dispatch accepted an unknown thread".into())
        }
        Err(error) => error.failure,
    };
    match failure {
        ApiFailure::Delivery(_) | ApiFailure::Refused(_) => {
            checks.push(passed(
                "dispatch",
                "unknown thread returned OrchestrationV2DispatchCommandError",
            ));
            ProbeStatus::Compatible
        }
        ApiFailure::Unavailable(detail) | ApiFailure::Uncertain(detail) => {
            checks.push(failed("dispatch", detail));
            ProbeStatus::Unavailable
        }
        ApiFailure::Changed(detail) => {
            checks.push(failed("dispatch", detail));
            ProbeStatus::Changed
        }
    }
}

/// How T3 places a dispatched message relative to the thread's active run
#[derive(Clone, Copy)]
enum Delivery {
    /// T3 decides whether the message starts a turn, steers the active run, or
    /// waits for it, so a busy thread may fold it into the current turn
    Auto,
    /// The message waits for the active run, as the T3 web client queues a
    /// message behind its own "compact first" turn
    AfterActive,
}

// matches what the T3 web client sends for a user message
fn dispatch_payload(
    thread_id: &str,
    command_id: &str,
    message_id: &str,
    text: &str,
    delivery: Delivery,
) -> Value {
    let mut payload = json!({
        "type": "message.dispatch",
        "commandId": command_id,
        "createdBy": "user",
        "creationSource": "server",
        "threadId": thread_id,
        "messageId": message_id,
        "text": text,
        "attachments": [],
    });
    match delivery {
        Delivery::Auto => {
            payload["deliveryIntent"] = json!("auto");
            payload["dispatchMode"] = json!({ "type": "start_immediately" });
        }
        Delivery::AfterActive => {
            payload["dispatchMode"] = json!({ "type": "queue_after_active" });
        }
    }
    payload
}

/// A typed dispatch refusal, or a changed contract for any other failure
///
/// Details stay free of ids so probe fingerprints are stable
fn dispatch_failure(cause: &[Value]) -> ApiFailure {
    let Some(first) = cause.first() else {
        return ApiFailure::Changed("message dispatch failed with an empty cause".into());
    };
    match first.get("_tag").and_then(Value::as_str) {
        Some("Fail") => {
            let tag = first
                .get("error")
                .and_then(|error| error.get("_tag"))
                .and_then(Value::as_str)
                .unwrap_or("an untagged error");
            if tag == DISPATCH_ERROR {
                ApiFailure::Refused("T3 could not dispatch the message to this thread".into())
            } else {
                ApiFailure::Changed(format!("message dispatch failed with {tag}"))
            }
        }
        Some("Interrupt") => ApiFailure::Unavailable("message dispatch was interrupted".into()),
        _ => ApiFailure::Changed("message dispatch failed with a defect".into()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ApiFailure, dispatch_failure};

    #[test]
    fn only_the_typed_dispatch_error_is_a_refusal() {
        let refused = dispatch_failure(&[json!({
            "_tag": "Fail",
            "error": { "_tag": "OrchestrationV2DispatchCommandError", "message": "No projection" }
        })]);
        let decode = dispatch_failure(&[json!({
            "_tag": "Fail",
            "error": { "_tag": "SchemaError" }
        })]);
        let defect = dispatch_failure(&[json!({ "_tag": "Die", "defect": "boom" })]);

        assert!(matches!(refused, ApiFailure::Refused(_)));
        assert!(
            matches!(decode, ApiFailure::Changed(detail) if detail == "message dispatch failed with SchemaError")
        );
        assert!(matches!(defect, ApiFailure::Changed(_)));
    }
}
