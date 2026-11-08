//! T3 orchestration protocol 2, the orchestrator from pingdotgg/t3code#2829
//!
//! Pinned at T3 Code `v0.1.0`, the first stable release with V2; every
//! contract below is unchanged from `main` commit `e9298af6`, where it was
//! first verified. When `homebased t3 check` fails after a T3 update, compare
//! these files in pingdotgg/t3code: `packages/contracts/src/environmentHttp.ts`
//! (HTTP routes), `packages/contracts/src/orchestrationV2.ts`
//! (`message.dispatch` and `ORCHESTRATION_V2_WS_METHODS`),
//! `apps/server/src/persistence/Migrations/055_OrchestrationV2.ts`, and
//! `apps/server/src/persistence/Migrations/OrchestrationV2/Foundation.ts`
//! (tables).
//! V2 removed `POST /api/orchestration/dispatch`, which is why a turn goes over
//! the client WebSocket in [`super::rpc`]. If `environmentHttp.ts` gains an
//! authenticated HTTP route that dispatches commands or wakes a thread, use it
//! and delete the WebSocket client
//!
//! - `statev2.sqlite` replaces the previous orchestrator's `state.sqlite`, which
//!   V2 copies once at its first start and never writes again.
//!   `orchestration_v2_projection_provider_threads`
//!   maps `driver` (`claudeAgent` or `codex`) and
//!   `payload_json.$.nativeThreadRef.nativeId` (the Claude session or Codex
//!   thread id) to `thread_id`; `orchestration_v2_projection_threads` has
//!   `archived_at` and `deleted_at`. `provider` holds the provider instance id,
//!   which equals the driver only for the default instance. `driver` is
//!   nullable, so a lookup falls back to `provider`
//! - Threads imported from the previous orchestrator keep their ids but get no
//!   provider thread, so their sessions resolve only through the copied
//!   `provider_session_runtime` table. A turn there starts a new provider session with a context handoff
//! - `orchestration.dispatchCommand` takes `message.dispatch` and returns integer
//!   `sequence`; a repeated `commandId` returns the first result without a second
//!   turn. An unknown thread fails with `OrchestrationV2DispatchCommandError`
//! - V2 runs a turn in an archived thread and leaves it archived, where nobody
//!   sees it, so homebased sends `thread.unarchive` first
//! - A `message.dispatch` whose text is exactly `/compact` runs as a
//!   compaction turn (`RunExecutionService.ts`). T3 rejects a message that
//!   would steer into it, so the message after it uses `queue_after_active`
//!   without `deliveryIntent`, like the web client's "compact first" send

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, Row};
use serde_json::{Value, json};
use tracing::debug;
use uuid::Uuid;

use super::rpc::{self, Exit};
use super::{
    ApiFailure, COMPACT_COMMAND, OpenThreadRow, ProbeCheck, ProbeStatus, ProviderThread,
    PullRequest, ThreadDetails, TurnStart, deterministic_id, failed, open_thread_rows, passed,
    state_connection,
};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const DISPATCH_METHOD: &str = "orchestration.dispatchCommand";
const DISPATCH_ERROR: &str = "OrchestrationV2DispatchCommandError";
const NATIVE_THREAD_SQL: &str = "
    SELECT t.thread_id
    FROM orchestration_v2_projection_provider_threads p
    JOIN orchestration_v2_projection_threads t ON t.thread_id = p.thread_id
    WHERE t.deleted_at IS NULL
      AND COALESCE(p.driver, p.provider) = ?1
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
// T3 runs the next turn, `/compact` included, in the active provider thread
// while it is of the instance the thread selects; a switch picks its thread
// only when the next message is dispatched
// a row from before instances keeps only `provider` and `default_provider`
macro_rules! active_thread_sql {
    ($filter:literal) => {
        concat!(
            "SELECT COALESCE(p.driver, p.provider),
                    CASE WHEN json_valid(p.payload_json)
                          AND json_type(p.payload_json, '$.nativeThreadRef.nativeId') = 'text'
                        THEN json_extract(p.payload_json, '$.nativeThreadRef.nativeId') END,
                    COALESCE(p.provider_instance_id, p.provider)
                        IS COALESCE(t.provider_instance_id, t.default_provider),
                    t.thread_id
             FROM orchestration_v2_projection_threads t
             JOIN orchestration_v2_projection_provider_threads p
               ON p.provider_thread_id = t.active_provider_thread_id
             WHERE ",
            $filter
        )
    };
}
const ACTIVE_THREAD_SQL: &str = active_thread_sql!("t.thread_id = ?1");
// the same rule for every thread at once, for thread lookups
const ACTIVE_THREADS_SQL: &str = active_thread_sql!("t.deleted_at IS NULL");
// T3's composer context records: `contextId` is the chip's `ref`, unique
// only within the message that carried it
macro_rules! chip_records_sql {
    ($messages:literal, $tail:literal) => {
        concat!(
            "SELECT json_extract(r.value, '$.threadId'), json_extract(r.value, '$.environmentId'),
                    json_extract(r.value, '$.title')
             FROM (", $messages, ") m,
                  json_each(CASE WHEN json_valid(m.payload_json) THEN m.payload_json ELSE '{}' END,
                            '$.context.records') r
             WHERE instr(m.payload_json, ?1) > 0
               AND json_extract(r.value, '$.kind') = 'thread'
               AND json_extract(r.value, '$.contextId') = ?1
               AND json_type(r.value, '$.threadId') = 'text' ",
            $tail
        )
    };
}
// the carrying thread's newest user message with the record carried the chip;
// T3's latest-user index serves the inner query
const CARRIED_CHIP_SQL: &str = chip_records_sql!(
    "SELECT payload_json, updated_at, message_id FROM orchestration_v2_projection_messages
     WHERE thread_id = ?2 AND role = 'user'
     ORDER BY updated_at DESC, message_id DESC LIMIT 200",
    "ORDER BY m.updated_at DESC, m.message_id DESC LIMIT 1"
);
// with no carrier, every distinct target the ref names in any user message;
// the scan is complete, since a capped one could miss the older target that
// makes the ref ambiguous, and grouping comes before the limit so repeats of
// one target never crowd out another
// a full scan reads 7183 user messages (12 MB) in 30 ms
const ANY_CHIP_SQL: &str = chip_records_sql!(
    "SELECT payload_json FROM orchestration_v2_projection_messages WHERE role = 'user'",
    "GROUP BY 1, 2 LIMIT 16"
);
const LEGACY_TABLE_SQL: &str =
    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'provider_session_runtime'";
const ARCHIVED_SQL: &str =
    "SELECT archived_at IS NOT NULL FROM orchestration_v2_projection_threads WHERE thread_id = ?1";
const PUT_AWAY_SQL: &str = "SELECT archived_at IS NOT NULL OR deleted_at IS NOT NULL
     FROM orchestration_v2_projection_threads WHERE thread_id = ?1";
const REQUIRED_STATE_SQL: &str = "
    SELECT p.thread_id, p.provider, p.driver, p.payload_json, p.updated_at,
           t.thread_id, t.title, t.archived_at, t.deleted_at
    FROM orchestration_v2_projection_provider_threads p
    JOIN orchestration_v2_projection_threads t ON t.thread_id = p.thread_id
    LIMIT 0";

// imported threads have no provider thread and resolve only through the copied
// session table, as in `find_thread`
// V2's settled state is unverified, so its threads all count as unsettled
// a Codex thread that never started has no native id yet, and is left out
const OPEN_THREADS_SQL: &str = "
    SELECT COALESCE(p.driver, p.provider),
           json_extract(p.payload_json, '$.nativeThreadRef.nativeId'), t.thread_id, t.title, 0
    FROM orchestration_v2_projection_provider_threads p
    JOIN orchestration_v2_projection_threads t ON t.thread_id = p.thread_id
    WHERE COALESCE(p.driver, p.provider) IN ('claudeAgent', 'codex')
      AND t.deleted_at IS NULL AND t.archived_at IS NULL
      AND json_valid(p.payload_json)
      AND json_type(p.payload_json, '$.nativeThreadRef.nativeId') = 'text'
    ORDER BY p.updated_at DESC";
// the cursor paths are those of `ProviderThread::mapping`
const OPEN_LEGACY_THREADS_SQL: &str = "
    SELECT r.provider_name, json_extract(r.resume_cursor_json, c.path), t.thread_id, t.title, 0
    FROM provider_session_runtime r
    JOIN (SELECT 'claudeAgent' AS provider, '$.resume' AS path
          UNION ALL SELECT 'codex', '$.threadId') c ON c.provider = r.provider_name
    JOIN orchestration_v2_projection_threads t ON t.thread_id = r.thread_id
    WHERE t.deleted_at IS NULL AND t.archived_at IS NULL
      AND json_valid(r.resume_cursor_json)
      AND json_type(r.resume_cursor_json, c.path) = 'text'
    ORDER BY r.last_seen_at DESC";
// effort is one entry of the model options list, and the last linked pull
// request is the newest
const THREAD_DETAILS_SELECT: &str = "
    SELECT t.thread_id, pr.title,
           COALESCE(json_extract(t.payload_json, '$.worktreePath'), pr.workspace_root),
           json_extract(t.payload_json, '$.branch'),
           json_extract(t.payload_json, '$.modelSelection.model'),
           (SELECT json_extract(o.value, '$.value')
            FROM json_each(t.payload_json, '$.modelSelection.options') o
            WHERE json_extract(o.value, '$.id') = 'effort'),
           parent.title,
           json_extract(t.payload_json, '$.pullRequests[#-1].number'),
           json_extract(t.payload_json, '$.pullRequests[#-1].url'),
           json_extract(t.payload_json, '$.pullRequests[#-1].snapshot.state'),
           t.created_at
    FROM orchestration_v2_projection_threads t
    LEFT JOIN projection_projects pr ON pr.project_id = t.project_id
    LEFT JOIN orchestration_v2_projection_threads parent
      ON parent.thread_id = json_extract(t.payload_json, '$.lineage.parentThreadId')
    WHERE t.deleted_at IS NULL AND json_valid(t.payload_json)";
const OPEN_THREAD_DETAILS_FILTER: &str = " AND t.archived_at IS NULL";
// statuses of a run that has not finished, as T3's own run recovery index
// lists them; any such run counts, so a newer follow-up that was cancelled
// does not hide a turn still running. Settled is the sidebar state T3
// records with `settledAt`
const THREAD_ROWS_SQL: &str = "
    SELECT t.thread_id, t.title, t.archived_at IS NOT NULL,
           CASE WHEN json_valid(t.payload_json)
               THEN COALESCE(json_extract(t.payload_json, '$.settledOverride') = 'settled', 0)
               ELSE 0 END,
           CASE WHEN json_valid(t.payload_json)
               THEN json_extract(t.payload_json, '$.lineage.parentThreadId') END,
           CASE WHEN json_valid(t.payload_json)
               THEN COALESCE(json_extract(t.payload_json, '$.lineage.relationshipToParent')
                             = 'subagent', 0)
               ELSE 0 END,
           EXISTS (SELECT 1 FROM orchestration_v2_projection_runs r
                   WHERE r.thread_id = t.thread_id
                     AND r.status IN ('queued', 'preparing', 'starting', 'running', 'waiting'))
    FROM orchestration_v2_projection_threads t
    WHERE t.deleted_at IS NULL
    ORDER BY t.created_at DESC";

// T3 records each delegated task as a `subagent` node on the parent, and the
// subagent row of the same id names the child; the node payload does not. A
// message to a finished child starts a new node, so the newest one per child
// counts. Node statuses T3 treats as ended, `rolled_back` included
// a follow-up node can still wait to start, so its time falls back to the row's
const TASK_STATES_SQL: &str = "
    SELECT s.child_thread_id,
           n.status IN ('completed', 'failed', 'cancelled', 'interrupted', 'rolled_back')
    FROM orchestration_v2_projection_subagents s
    JOIN orchestration_v2_projection_nodes n ON n.node_id = s.subagent_id
    WHERE n.kind = 'subagent' AND s.child_thread_id IS NOT NULL
    ORDER BY COALESCE(n.started_at, s.started_at, s.updated_at), n.node_id";

/// One row of [`THREAD_ROWS_SQL`]
pub(super) struct ThreadRow {
    pub(super) id: String,
    pub(super) title: String,
    pub(super) archived: bool,
    pub(super) settled: bool,
    pub(super) parent: Option<String>,
    pub(super) delegate: bool,
    pub(super) running: bool,
}

/// Every thread that is not deleted, newest first
pub(super) fn thread_rows(path: &Path) -> Result<Vec<ThreadRow>, String> {
    let db = state_connection(path).map_err(|_| "statev2.sqlite cannot be read".to_string())?;
    let mut statement = db
        .prepare(THREAD_ROWS_SQL)
        .map_err(|error| format!("T3 thread query failed: {error}"))?;
    statement
        .query_map([], |row| {
            Ok(ThreadRow {
                id: row.get(0)?,
                title: row.get(1)?,
                archived: row.get(2)?,
                settled: row.get(3)?,
                parent: row.get(4)?,
                delegate: row.get(5)?,
                running: row.get(6)?,
            })
        })
        .and_then(Iterator::collect)
        .map_err(|error| format!("T3 thread query failed: {error}"))
}

/// Child threads whose newest delegated task ended, as T3's subagent nodes
/// record it
///
/// Empty when the query does not fit the schema, so an older T3 without the
/// node tables, or with changed ones, reads every child by its run and
/// settled state alone, the way [`check_schema`] tells a schema apart
///
/// # Errors
///
/// Any other failed read, such as a locked state, so the index read fails as
/// a failed thread query does
pub(super) fn finished_tasks(path: &Path) -> Result<HashSet<String>, String> {
    let failed = |error: rusqlite::Error| format!("T3 delegated task query failed: {error}");
    let db = state_connection(path).map_err(|_| "statev2.sqlite cannot be read".to_string())?;
    let mut statement = match db.prepare(TASK_STATES_SQL) {
        Ok(statement) => statement,
        Err(rusqlite::Error::SqliteFailure(failure, _))
            if matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            ) =>
        {
            return Err("statev2.sqlite is locked".into());
        }
        Err(error) => {
            debug!(%error, "T3 has no delegated task tables homebased reads");
            return Ok(HashSet::new());
        }
    };
    let mut newest: HashMap<String, bool> = HashMap::new();
    // oldest first, so each child keeps its newest node
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(failed)?;
    for row in rows {
        let (child, ended): (String, bool) = row.map_err(failed)?;
        newest.insert(child, ended);
    }
    Ok(newest
        .into_iter()
        .filter_map(|(child, ended)| ended.then_some(child))
        .collect())
}

/// Claude sessions and Codex threads whose thread is neither archived nor
/// deleted, newest first
pub(super) fn open_threads(path: &Path) -> Result<Vec<OpenThreadRow>, String> {
    let mut rows = open_thread_rows(path, OPEN_THREADS_SQL)?;
    let db = state_connection(path).map_err(|_| "statev2.sqlite cannot be read".to_string())?;
    if has_legacy_table(&db)? {
        rows.extend(open_thread_rows(path, OPEN_LEGACY_THREADS_SQL)?);
    }
    Ok(rows)
}

/// Details of every open thread, by T3 thread id
///
/// Empty when the query fails, so a T3 change to these display-only fields
/// hides them instead of breaking the thread list
pub(super) fn thread_details(path: &Path) -> HashMap<String, ThreadDetails> {
    details_where(path, OPEN_THREAD_DETAILS_FILTER)
}

/// Details of every thread that is not deleted, archived ones too
pub(super) fn all_thread_details(path: &Path) -> HashMap<String, ThreadDetails> {
    details_where(path, "")
}

fn details_where(path: &Path, filter: &str) -> HashMap<String, ThreadDetails> {
    let read = || -> rusqlite::Result<HashMap<String, ThreadDetails>> {
        let db = state_connection(path)?;
        let mut statement = db.prepare(&format!("{THREAD_DETAILS_SELECT}{filter}"))?;
        statement
            .query_map([], |row| Ok((row.get(0)?, details_from_row(row)?)))?
            .collect()
    };
    read().unwrap_or_else(|error| {
        debug!(%error, "T3 thread details cannot be read");
        HashMap::new()
    })
}

fn details_from_row(row: &Row<'_>) -> rusqlite::Result<ThreadDetails> {
    let number: Option<i64> = row.get(7)?;
    let url: Option<String> = row.get(8)?;
    let pull_request = number.zip(url).map(|(number, url)| PullRequest {
        number,
        url,
        state: row.get(9).ok().flatten(),
    });

    Ok(ThreadDetails {
        project: row.get(1)?,
        workspace: row.get(2)?,
        branch: row.get(3)?,
        model: row.get(4)?,
        effort: row.get(5)?,
        parent_title: row.get(6)?,
        pull_request,
        created_at: row.get(10)?,
    })
}

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

/// Active provider thread of T3 thread `t3_thread`: its driver, its native id
/// once it has one, and whether it is of the instance the thread selects
pub(super) fn active_thread(path: &Path, t3_thread: &str) -> Result<Option<ActiveThread>, String> {
    let db = state_connection(path).map_err(|_| "statev2.sqlite cannot be read".to_string())?;
    db.query_row(ACTIVE_THREAD_SQL, [t3_thread], ActiveThread::from_row)
        .optional()
        .map_err(|_| "statev2.sqlite active provider thread lookup failed".to_string())
}

/// Active provider thread of every thread that is not deleted, as in [`active_thread`]
pub(super) fn active_threads(path: &Path) -> Result<Vec<ActiveThread>, String> {
    let db = state_connection(path).map_err(|_| "statev2.sqlite cannot be read".to_string())?;
    let mut statement = db
        .prepare(ACTIVE_THREADS_SQL)
        .map_err(|_| "statev2.sqlite active provider thread query failed".to_string())?;
    statement
        .query_map([], ActiveThread::from_row)
        .and_then(Iterator::collect)
        .map_err(|_| "statev2.sqlite active provider thread query failed".to_string())
}

/// One row of [`ACTIVE_THREAD_SQL`]: the provider thread a T3 thread runs next
pub(super) struct ActiveThread {
    /// T3 thread id
    pub(super) thread: String,
    /// Driver of the provider thread
    pub(super) driver: String,
    /// Its native id, once it has one
    pub(super) native: Option<String>,
    /// Whether it is of the instance the thread selects
    pub(super) selected: bool,
}

impl ActiveThread {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            driver: row.get(0)?,
            native: row.get(1)?,
            selected: row.get(2)?,
            thread: row.get(3)?,
        })
    }
}

fn has_legacy_table(db: &Connection) -> Result<bool, String> {
    db.query_row(LEGACY_TABLE_SQL, [], |_| Ok(()))
        .optional()
        .map(|table| table.is_some())
        .map_err(|_| "statev2.sqlite table lookup failed".to_string())
}

/// The threads the thread context record `context`, the `ref` of a T3 chip,
/// names
///
/// T3 keeps the chip's record in the message that carried it, at
/// `$.context.records[]`, and the id is unique only in that message. With
/// `carrier`, the T3 thread whose message carried the chip, the result is the
/// target of that thread's newest user message with the record. Otherwise,
/// or when none of its recent messages has it, it is every distinct target
/// the id names in any user message, for the caller to accept only when there
/// is one
pub(super) fn chip_targets(
    path: &Path,
    context: &str,
    carrier: Option<&str>,
) -> Result<Vec<super::ChipTarget>, String> {
    let db = state_connection(path).map_err(|_| "statev2.sqlite cannot be read".to_string())?;
    let failed = |_| "statev2.sqlite chip lookup failed".to_string();
    let target = |row: &Row<'_>| {
        Ok(super::ChipTarget {
            thread: row.get(0)?,
            environment: row.get(1)?,
            title: row.get(2)?,
        })
    };
    if let Some(carrier) = carrier
        && let Some(carried) = db
            .query_row(CARRIED_CHIP_SQL, [context, carrier], target)
            .optional()
            .map_err(failed)?
    {
        return Ok(vec![carried]);
    }
    let mut statement = db.prepare(ANY_CHIP_SQL).map_err(failed)?;
    statement
        .query_map([context], target)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(failed)
}

/// Check that an existing state has the tables homebased reads
///
/// Every failure is an error, whether the database is locked now or its
/// schema is one homebased does not read, so a lookup that cannot read T3 is
/// never taken for one that found no threads
pub(super) fn check_schema(path: &Path) -> Result<(), String> {
    let db = state_connection(path).map_err(|_| "statev2.sqlite cannot be read".to_string())?;
    match db.prepare(REQUIRED_STATE_SQL) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(failure, _))
            if matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            ) =>
        {
            Err("statev2.sqlite is locked".into())
        }
        Err(_) => Err("statev2.sqlite lacks the thread tables homebased reads".into()),
    }
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
/// thread stays archived, where nobody sees it
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
    let payload = dispatch_payload(thread_id, &command_id, &message_id, text, start);
    dispatch(origin, token, &payload, before_send)
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
        TurnStart::Auto,
    );
    dispatch(origin, token, &payload, before_send)
}

pub(super) fn dispatch(
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
        Ok(Exit::Success(_)) => Ok(()),
        // only T3's typed refusal proves it did not take the message
        Ok(Exit::Failure(cause)) => match dispatch_failure(&cause) {
            ApiFailure::Refused(detail) => Err(ApiFailure::Refused(detail)),
            failure => Err(ApiFailure::Uncertain(failure.detail())),
        },
        Err(error) if error.sent => Err(ApiFailure::Uncertain(error.failure.detail())),
        Err(error) => Err(error.failure),
    }
}

/// Whether `thread_id` is archived or deleted; a thread with no row counts as
/// deleted
pub(super) fn is_put_away(state: &Path, thread_id: &str) -> rusqlite::Result<bool> {
    let db = state_connection(state)?;
    let put_away = db
        .query_row(PUT_AWAY_SQL, [thread_id], |row| row.get::<_, bool>(0))
        .optional()?;
    Ok(put_away.unwrap_or(true))
}

pub(super) fn is_archived(state: &Path, thread_id: &str) -> bool {
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
        TurnStart::Auto,
    );
    let failure = match rpc::call(origin, token, DISPATCH_METHOD, &payload, RPC_TIMEOUT) {
        Ok(Exit::Failure(cause)) => dispatch_failure(&cause),
        Ok(Exit::Success(_)) => {
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

// matches what the T3 web client sends for a user message
pub(super) fn dispatch_payload(
    thread_id: &str,
    command_id: &str,
    message_id: &str,
    text: &str,
    start: TurnStart,
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
    // with `deliveryIntent` set, T3 decides whether the message starts a turn,
    // steers the active run, or waits for it; T3's web client queues a message
    // behind its own compaction with `queue_after_active` and no intent
    match start {
        TurnStart::Auto => {
            payload["deliveryIntent"] = json!("auto");
            payload["dispatchMode"] = json!({ "type": "start_immediately" });
        }
        TurnStart::AfterActive => {
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
