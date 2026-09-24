//! SQLite source of truth: task state, sequenced events, and callback results

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, params_from_iter};
use serde_json::Value;

use crate::callback::{
    EventKind, ReportView, check_due_event, exit_event, lost_event, notify_event, terminal_event,
};
use crate::domain::{
    AttentionState, CallbackStatus, ContainerExitEvidence, ExitReason, ProcessGroupExitEvidence,
    ProcessStatus, REPORTS_MAX, ReportOutcome, SCHEMA_VERSION, SUMMARY_MAX_BYTES, TaskEnv,
    TaskExitEvidence, TaskId, TaskName, TaskReport, TaskRow, TaskState, TerminalCallbackProjection,
    ThreadId, Workload, check_report_allowed, check_status_transition,
};
use crate::error::AppError;
use crate::events::EventPayload;
use crate::events::{DeliveryState, EventError, TaskEvent};
use crate::machine::MachineId;
use crate::resource::store::RESOURCE_SCHEMA;
use crate::spec::NormalizedSpec;
use crate::submission::{
    CallbackContext, CallbackExecutable, ExecutionRecord, ExecutorIdentity, OriginRoute,
    PersistedSpec, RequestId, ResourceRoutePhase, SubmissionState,
};

mod cancellation;
mod container;
mod events;
mod identity;
mod message;
mod resource;
pub use container::TaskContainerRecord;
pub(crate) use events::EventRetentionBatch;
pub use identity::{IdentityError, ResourceActionRouteResult, ResourceBackgroundRouteResult};
pub(crate) use resource::VerifiedReleaseProof;
#[cfg(test)]
pub(crate) use resource::test_support::{
    mark_request_assigned_for_race, unreceipted_release_provenance,
};
pub(crate) use resource::{
    AcceptedActionTask, EndedRestoreResolution, PreparedReturnTask,
    RemoteReleaseWatcherAcceptanceInput, ResourceActionError, RestoreReconcileOutcome,
    ReturnClosure, ReturnDecisionError, ReturnTaskAcceptance, ReturnTaskAcceptanceInput,
    ReturnTaskOrigin,
};
pub(crate) use resource::{
    BackgroundLaunchAcceptance, BackgroundLaunchError, BackgroundLaunchInput,
    BackgroundLaunchPhase, BackgroundLaunchView, RemoteBackgroundLaunchInput,
    idle_boundary_decision_on, idle_opening_matches_on, open_idle_serving_loan_on,
    pending_background_launch_on, promote_started_background_launch_on,
};
pub(crate) use resource::{OperatorGpuFreeError, operator_serving_release_matches_on};
pub(crate) use resource::{
    ResourceControlEffect, ResourceControlError, ResourceControlRequest, ResourceControlStart,
    ResourceReadModel, SupervisorReplacement, open_action_id,
};

/// Released version 2 schema
///
/// Fresh databases apply this schema and then the version 2 migration, so new
/// and upgraded databases share one path to the current schema
///
/// `timeout_secs` is decimal TEXT, not INTEGER: the inactivity timer has no
/// product maximum, and a `Duration` above `i64::MAX` seconds cannot be stored
/// in SQLite's signed INTEGER without a lossy cast
const BASE_SCHEMA: &str = r"
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    name TEXT,
    workload_json TEXT NOT NULL,
    cwd TEXT NOT NULL,
    timeout_secs TEXT NOT NULL,
    env_path TEXT NOT NULL,
    env_home TEXT NOT NULL,
    binary TEXT NOT NULL,
    status TEXT NOT NULL,
    exit_reason TEXT,
    callback_status TEXT NOT NULL,
    attention_state TEXT NOT NULL,
    timeout_notified_at TEXT,
    pid INTEGER,
    cancel_requested_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX tasks_status ON tasks(status);
CREATE INDEX tasks_thread ON tasks(thread_id);

CREATE TABLE reports (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    summary TEXT NOT NULL,
    reported_at TEXT NOT NULL,
    notified_at TEXT,
    PRIMARY KEY (task_id, seq),
    FOREIGN KEY (task_id) REFERENCES tasks(id)
);
";

/// Schema version 1 had no `name` column
const MIGRATE_1_TO_2: &str = r"
ALTER TABLE tasks ADD COLUMN name TEXT;
";

/// Schema version of the v0.4.0 release
const RELEASED_V0_4_SCHEMA_VERSION: i64 = 27;

/// Everything added after the released version 2 schema, except the resource
/// tables that `RESOURCE_SCHEMA` owns
///
/// Versions 3 through 26 were never released, so a version 2 database moves to
/// the current schema in one step
const MIGRATE_2_TO_CURRENT: &str = r"
ALTER TABLE tasks ADD COLUMN project_root TEXT;
ALTER TABLE tasks ADD COLUMN process_group_exit_evidence TEXT;

CREATE TABLE report_notification_intents (
    task_id TEXT NOT NULL,
    report_seq INTEGER NOT NULL,
    requested_at TEXT NOT NULL,
    PRIMARY KEY (task_id, report_seq),
    FOREIGN KEY (task_id, report_seq) REFERENCES reports(task_id, seq)
);

CREATE TABLE origin_routes (
    request_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    execution_machine TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    route_json TEXT NOT NULL
);

CREATE TABLE executor_identities (
    task_id TEXT PRIMARY KEY,
    origin_machine TEXT NOT NULL,
    identity_json TEXT NOT NULL
);

CREATE TABLE executor_outbox (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    origin_machine TEXT NOT NULL,
    execution_machine TEXT NOT NULL,
    event_json TEXT NOT NULL,
    notification_required INTEGER NOT NULL CHECK (notification_required IN (0, 1)),
    state TEXT NOT NULL CHECK (state IN ('pending', 'acknowledged')),
    acknowledged_at TEXT,
    PRIMARY KEY (task_id, seq)
);

CREATE TABLE executor_event_cursors (
    task_id TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL CHECK (last_seq >= 0)
);

CREATE TABLE executor_event_routes (
    task_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state = 'orphaned'),
    reason TEXT NOT NULL
);

CREATE TABLE origin_inbox (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    origin_machine TEXT NOT NULL,
    execution_machine TEXT NOT NULL,
    event_json TEXT NOT NULL,
    notification_required INTEGER NOT NULL CHECK (notification_required IN (0, 1)),
    delivery_json TEXT NOT NULL,
    settled_at TEXT,
    PRIMARY KEY (task_id, seq)
);

CREATE INDEX executor_outbox_pending ON executor_outbox(state, task_id, seq);
CREATE INDEX executor_outbox_retention ON executor_outbox(acknowledged_at, task_id, seq);
CREATE INDEX origin_inbox_order ON origin_inbox(task_id, seq);
CREATE INDEX origin_inbox_retention ON origin_inbox(settled_at, task_id, seq);

CREATE TABLE executor_event_receipts (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_digest TEXT NOT NULL,
    result_json TEXT NOT NULL,
    terminal_callback INTEGER NOT NULL CHECK (terminal_callback IN (0, 1)),
    PRIMARY KEY (task_id, seq)
);

CREATE TABLE origin_event_receipts (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_digest TEXT NOT NULL,
    delivery_json TEXT NOT NULL,
    terminal_callback INTEGER NOT NULL CHECK (terminal_callback IN (0, 1)),
    PRIMARY KEY (task_id, seq)
);

CREATE TABLE cancellation_requests (
    task_id TEXT PRIMARY KEY,
    request_json TEXT NOT NULL
);

CREATE TABLE executor_cancellations (
    cancellation_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    receipt_json TEXT NOT NULL
);
CREATE INDEX executor_cancellations_task ON executor_cancellations(task_id);

CREATE TABLE message_attempts (
    message_id TEXT PRIMARY KEY,
    attempt_json TEXT NOT NULL
);

CREATE TABLE message_receipts (
    message_id TEXT PRIMARY KEY REFERENCES message_attempts(message_id),
    receipt_json TEXT NOT NULL
);

CREATE TABLE outbound_message_bindings (
    message_id TEXT PRIMARY KEY,
    binding_json TEXT NOT NULL
);
";

const TASK_SELECT: &str = "SELECT id, thread_id, name, workload_json, cwd, timeout_secs,
    env_path, env_home, binary, status, exit_reason, callback_status,
    attention_state, timeout_notified_at, pid, cancel_requested_at, created_at, updated_at,
    process_group_exit_evidence, container_exit_evidence
 FROM tasks";

/// Operator attestation receipts gained the Restoring return outcomes after v0.4.0
///
/// SQLite cannot change a CHECK constraint in place, so the table is rebuilt
/// Existing receipts keep their bytes, and `RESOURCE_SCHEMA` recreates the
/// resource index that the dropped table owned
const MIGRATE_27_TO_28: &str = r"
CREATE TABLE resource_operator_attestations_v28 (
    operation_id TEXT PRIMARY KEY NOT NULL,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    task_id TEXT NOT NULL UNIQUE,
    preceding_loan TEXT,
    preceding_launch TEXT,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.operation_id') = operation_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.task_id') = task_id, 0)
        AND COALESCE(
            json_extract(receipt_json, '$.attestation.confirmation') = 'operator_confirmed_gpu_free',
            0
        )
        AND COALESCE(length(trim(json_extract(receipt_json, '$.attestation.observation'))) > 0, 0)
        AND COALESCE(json_type(receipt_json, '$.evidence') = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.outcome.type') IN (
            'release_resolved_serving', 'release_resolved_return_required',
            'idle_serving', 'idle_boundary',
            'restore_closed_serving', 'restore_closed_idle_boundary'
        ), 0)
    )
);
INSERT INTO resource_operator_attestations_v28
    (operation_id, resource_id, task_id, preceding_loan, preceding_launch, receipt_json)
SELECT operation_id, resource_id, task_id, preceding_loan, preceding_launch, receipt_json
FROM resource_operator_attestations ORDER BY rowid;
DROP TABLE resource_operator_attestations;
ALTER TABLE resource_operator_attestations_v28 RENAME TO resource_operator_attestations;
";

/// Schema version of the v0.5.0 release
const RELEASED_V0_5_SCHEMA_VERSION: i64 = 28;

/// Container tasks gained their witness after v0.5.0
///
/// `container_exit_evidence` sits beside `process_group_exit_evidence` so the
/// terminal transition writes both in one update. `task_containers` keeps the
/// saved container ID, its start, and the adoption streak of later workers
const MIGRATE_28_TO_29_TASKS: &str = r"
ALTER TABLE tasks ADD COLUMN container_exit_evidence TEXT;

CREATE TABLE task_containers (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES tasks(id),
    container_id TEXT CHECK (
        container_id IS NULL
        OR (length(container_id) = 64 AND container_id NOT GLOB '*[^0-9a-f]*')
    ),
    started_at TEXT,
    adoptions INTEGER NOT NULL DEFAULT 0 CHECK (adoptions >= 0)
);
";

/// Resource requests and restore closures gained container work after v0.5.0
///
/// SQLite cannot change a CHECK constraint in place, so both tables are
/// rebuilt with the constraints that `RESOURCE_SCHEMA` now declares. Existing
/// rows keep their bytes, and `RESOURCE_SCHEMA` recreates the dropped indexes
const MIGRATE_28_TO_29_RESOURCES: &str = r"
CREATE TABLE resource_requests_v29 (
    acceptance_sequence INTEGER PRIMARY KEY AUTOINCREMENT CHECK (acceptance_sequence > 0),
    request_id TEXT NOT NULL UNIQUE,
    task_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    origin_machine TEXT NOT NULL,
    spec_json TEXT NOT NULL CHECK (
        json_valid(spec_json)
        AND COALESCE(json_type(spec_json) = 'object', 0)
        AND COALESCE(json_type(spec_json, '$.api_version') = 'integer', 0)
        AND COALESCE(json_type(spec_json, '$.thread') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.name') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.cwd') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.timeout') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.workload') = 'object', 0)
        AND COALESCE(
            (
                json_extract(spec_json, '$.workload.type') = 'task'
                AND json_type(spec_json, '$.workload.command') = 'array'
            ) OR (
                json_extract(spec_json, '$.workload.type') = 'container'
                AND json_type(spec_json, '$.workload.image') = 'text'
                AND json_type(spec_json, '$.workload.gpus') IS NOT NULL
            ),
            0
        )
    ),
    state_json TEXT NOT NULL CHECK (
        json_valid(state_json)
        AND COALESCE(json_type(state_json) = 'object', 0)
        AND COALESCE(json_type(state_json, '$.type') = 'text', 0)
        AND COALESCE(json_extract(state_json, '$.type') IN (
            'queued', 'assigned', 'finished', 'cancelled_before_launch', 'rejected'
        ), 0)
    )
);
INSERT INTO resource_requests_v29
    (acceptance_sequence, request_id, task_id, resource_id, origin_machine, spec_json, state_json)
SELECT acceptance_sequence, request_id, task_id, resource_id, origin_machine, spec_json, state_json
FROM resource_requests ORDER BY acceptance_sequence;
DROP TABLE resource_requests;
ALTER TABLE resource_requests_v29 RENAME TO resource_requests;

CREATE TABLE resource_restore_closures_v29 (
    action_id TEXT PRIMARY KEY REFERENCES resource_return_decisions(action_id),
    task_id TEXT NOT NULL UNIQUE,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.action_id') = action_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.basis.type') IN (
            'confirmed_running', 'foreground_ended', 'container_ended', 'supervisor_resolved_end'
        ), 0)
    )
);
INSERT INTO resource_restore_closures_v29 (action_id, task_id, receipt_json)
SELECT action_id, task_id, receipt_json FROM resource_restore_closures ORDER BY rowid;
DROP TABLE resource_restore_closures;
ALTER TABLE resource_restore_closures_v29 RENAME TO resource_restore_closures;
";

/// Move a released version 2 database to the current schema
///
/// The version 2 schema has no resource tables, so `RESOURCE_SCHEMA` creates
/// them with the current constraints
fn migrate_2_to_current(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(MIGRATE_2_TO_CURRENT)?;
    conn.execute_batch(MIGRATE_28_TO_29_TASKS)?;
    conn.execute_batch(RESOURCE_SCHEMA)
}

/// Move a v0.4.0 database to the current schema
fn migrate_27_to_current(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(MIGRATE_27_TO_28)?;
    migrate_28_to_current(conn)
}

/// Move a v0.5.0 database to the current schema
fn migrate_28_to_current(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(MIGRATE_28_TO_29_TASKS)?;
    conn.execute_batch(MIGRATE_28_TO_29_RESOURCES)?;
    conn.execute_batch(RESOURCE_SCHEMA)
}

/// Why `Store::open` refuses a database version
fn unsupported_schema_version(version: i64) -> AppError {
    let message = if (3..RELEASED_V0_4_SCHEMA_VERSION).contains(&version) {
        format!(
            "schema user_version={version} came from an unreleased development build and has no \
             migration; move the database aside to start over"
        )
    } else {
        format!("unsupported schema user_version={version}")
    };
    AppError::Internal { message }
}

/// Open or create the database
pub struct Store {
    conn: Connection,
    tasks_dir: std::path::PathBuf,
}

fn resource_task_id_is_reserved(conn: &Connection, task: TaskId) -> Result<bool, rusqlite::Error> {
    if resource_request_task_id_is_reserved(conn, task)? {
        return Ok(true);
    }
    release_watcher_task_id_is_reserved(conn, task)
}

fn resource_request_task_id_is_reserved(
    conn: &Connection,
    task: TaskId,
) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM resource_requests WHERE task_id = ?1
            UNION ALL
            SELECT 1 FROM resource_request_preventions WHERE task_id = ?1
        )",
        [task.to_string()],
        |row| row.get(0),
    )
}

fn release_watcher_task_id_is_reserved(
    conn: &Connection,
    task: TaskId,
) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM loans
            WHERE json_extract(state_json, '$.phase.watcher_intent.watcher_task_id') = ?1
               OR json_extract(state_json, '$.last_safe_phase.watcher_intent.watcher_task_id') = ?1
        )",
        [task.to_string()],
        |row| row.get(0),
    )
}

fn insert_task_with_project_root_on(
    conn: &Connection,
    row: &TaskRow,
    project_root: Option<&Path>,
) -> Result<(), AppError> {
    conn.execute(
        "INSERT INTO tasks (
            id, thread_id, name, workload_json, cwd, timeout_secs,
            env_path, env_home, binary, status, exit_reason,
            callback_status, attention_state, timeout_notified_at,
            pid, cancel_requested_at, created_at, updated_at, project_root,
            process_group_exit_evidence, container_exit_evidence
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
        params![
            row.id.to_string(),
            row.thread.to_string(),
            row.name.as_ref().map(TaskName::as_str),
            serde_json::to_string(&row.workload)?,
            row.cwd.to_string_lossy(),
            fmt_timeout(row.timeout),
            row.env.path,
            row.env.home,
            row.binary.to_string_lossy(),
            row.status().as_str(),
            row.exit_reason().map(serde_json::to_string).transpose()?,
            row.callback_status.as_str(),
            row.attention.as_str(),
            row.attention.delivered_at().map(fmt_time),
            row.pid(),
            row.cancel_requested_at.map(fmt_time),
            fmt_time(row.created_at),
            fmt_time(row.updated_at),
            project_root.map(|root| root.to_string_lossy().into_owned()),
            row.process_group_exit_evidence.as_str(),
            row.container_exit_evidence.to_storage()?,
        ],
    )?;
    Ok(())
}

pub(crate) fn task_by_id_on(conn: &Connection, id: TaskId) -> Result<Option<TaskRow>, AppError> {
    let mut statement = conn.prepare(&format!("{TASK_SELECT} WHERE id = ?1"))?;
    Ok(statement
        .query_row(params![id.to_string()], parse_task_row)
        .optional()?)
}

pub(crate) fn executor_identity_for_resource_task_on(
    conn: &Connection,
    task: TaskId,
) -> Result<Option<ExecutorIdentity>, IdentityError> {
    identity::executor_identity_on(conn, task)
}

pub(crate) fn initial_queued_event_matches_on(
    conn: &Connection,
    task: TaskId,
    origin_machine: MachineId,
    execution_machine: MachineId,
) -> Result<bool, EventError> {
    events::initial_queued_event_matches_on(conn, task, origin_machine, execution_machine)
}

pub(super) fn validate_local_task_acceptance(
    row: &TaskRow,
    spec: &NormalizedSpec,
    callback: &CallbackContext,
) -> Result<(), AppError> {
    if spec.machine.is_some()
        || row.status() != ProcessStatus::Queued
        || row.name.as_ref() != Some(&spec.name)
        || row.thread != spec.thread
        || row.cwd != spec.cwd
        || row.timeout != spec.timeout
        || callback.env != row.env
        || callback.cwd != row.cwd
        || !row.cwd.is_absolute()
        || callback
            .codex
            .path()
            .is_some_and(|path| !path.is_absolute())
    {
        return Err(AppError::Internal {
            message: "local task and accepted origin spec do not match".into(),
        });
    }
    Ok(())
}

fn insert_local_task_records_on(
    conn: &Connection,
    row: &TaskRow,
    spec: &NormalizedSpec,
    machine: MachineId,
    request: RequestId,
    callback: &CallbackContext,
    allowed_watcher_task: Option<TaskId>,
) -> Result<(), AppError> {
    validate_local_task_acceptance(row, spec, callback)?;
    if resource_request_task_id_is_reserved(conn, row.id)?
        || (allowed_watcher_task != Some(row.id)
            && release_watcher_task_id_is_reserved(conn, row.id)?)
    {
        return Err(AppError::ClusterTaskConflict { task: row.id });
    }

    let project_root = find_project_root(&row.cwd);
    insert_task_with_project_root_on(conn, row, project_root.as_deref())?;
    let route = OriginRoute {
        request,
        task: row.id,
        origin_machine: machine,
        execution_machine: machine,
        thread: row.thread,
        callback: callback.clone(),
        spec: spec.clone().into(),
        submission: SubmissionState::Accepted,
        last_execution_state: Some(ProcessStatus::Queued),
        last_updated_at: Some(chrono::Utc::now()),
        last_accepted_seq: 0,
        last_settled_seq: 0,
    };
    let identity = ExecutorIdentity::Accepted(ExecutionRecord {
        task: row.id,
        origin_machine: machine,
        execution_machine: machine,
        spec: spec.clone().into(),
        state: ProcessStatus::Queued,
    });
    conn.execute(
        "INSERT INTO origin_routes (request_id,task_id,execution_machine,spec_json,route_json) VALUES (?1,?2,?3,?4,?5)",
        params![request.0.to_string(), row.id.to_string(), machine.to_string(), serde_json::to_string(&route.spec)?, serde_json::to_string(&route)?],
    )?;
    conn.execute(
        "INSERT INTO executor_identities (task_id,origin_machine,identity_json) VALUES (?1,?2,?3)",
        params![
            row.id.to_string(),
            machine.to_string(),
            serde_json::to_string(&identity)?
        ],
    )?;
    events::append_produced_event_on(
        conn,
        row.id,
        EventPayload::State {
            status: ProcessStatus::Queued,
        },
    )?;
    Ok(())
}

/// Fixed owners of one authority task whose callback route lives on another machine
pub(crate) struct RemoteOriginTask {
    /// Stable retry identity saved in the remote route
    pub(crate) request_id: RequestId,
    /// Supervisor machine that owns the callback route
    pub(crate) origin_machine: MachineId,
    /// Authority machine that executes the task
    pub(crate) execution_machine: MachineId,
    /// Supervisor thread named by the spec
    pub(crate) thread: ThreadId,
    /// Whether the task is the release watcher that its own action reserved
    pub(crate) reserved_watcher: bool,
}

/// Insert one action-bound task whose callback route lives on the supervisor machine
///
/// The task row, accepted remote executor identity, first queued event, and exact
/// action receipt commit in the caller's transaction. No origin route is written
/// here because the supervisor machine saved it before sending the launch
pub(crate) fn insert_remote_action_task_records_on(
    conn: &Connection,
    row: &TaskRow,
    spec: &NormalizedSpec,
    receipt: &crate::resource::bound_action::ActionTaskReceipt,
) -> Result<(), AppError> {
    if row.id != receipt.task_id {
        return Err(AppError::ClusterTaskConflict { task: row.id });
    }
    insert_remote_origin_task_records_on(
        conn,
        row,
        spec,
        &RemoteOriginTask {
            request_id: receipt.request_id,
            origin_machine: receipt.origin_machine(),
            execution_machine: receipt.execution_machine(),
            thread: receipt.authority.supervisor.thread,
            // only the watcher bound by this exact action may use its reserved identity
            reserved_watcher: receipt.kind
                == crate::resource::bound_action::ResourceActionKind::ReleaseWatcher,
        },
    )
}

/// Insert one authority task whose callback route lives on a remote supervisor machine
///
/// The task row, accepted remote executor identity, and first queued event commit
/// in the caller's transaction. The caller saves its own receipt in the same one
pub(crate) fn insert_remote_origin_task_records_on(
    conn: &Connection,
    row: &TaskRow,
    spec: &NormalizedSpec,
    owners: &RemoteOriginTask,
) -> Result<(), AppError> {
    let task = row.id;
    let origin = owners.origin_machine;
    let execution = owners.execution_machine;
    if origin == execution
        || spec.machine.is_some()
        || spec.thread != owners.thread
        || matches!(&spec.workload, crate::spec::NormalizedWorkload::Agent(_))
        || row.status() != ProcessStatus::Queued
        || row.name.as_ref() != Some(&spec.name)
        || row.thread != spec.thread
        || row.workload != crate::invocation::persist_workload(&spec.workload)
        || row.cwd != spec.cwd
        || row.timeout != spec.timeout
        || !row.cwd.is_absolute()
        || !row.binary.is_absolute()
    {
        return Err(AppError::ClusterTaskConflict { task });
    }
    let watcher_reserved = release_watcher_task_id_is_reserved(conn, task)?;
    if resource_request_task_id_is_reserved(conn, task)?
        || (watcher_reserved && !owners.reserved_watcher)
    {
        return Err(AppError::ClusterTaskConflict { task });
    }
    let occupied: bool = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM tasks WHERE id=?1
            UNION ALL SELECT 1 FROM executor_identities WHERE task_id=?1
            UNION ALL SELECT 1 FROM origin_routes WHERE task_id=?1 OR request_id=?2
            UNION ALL SELECT 1 FROM executor_outbox WHERE task_id=?1
            UNION ALL SELECT 1 FROM executor_event_receipts WHERE task_id=?1
        )",
        params![task.to_string(), owners.request_id.0.to_string()],
        |entry| entry.get(0),
    )?;
    if occupied {
        return Err(AppError::ClusterTaskConflict { task });
    }

    let project_root = find_project_root(&row.cwd);
    insert_task_with_project_root_on(conn, row, project_root.as_deref())?;
    let identity = ExecutorIdentity::Accepted(ExecutionRecord {
        task,
        origin_machine: origin,
        execution_machine: execution,
        spec: spec.clone().into(),
        state: ProcessStatus::Queued,
    });
    conn.execute(
        "INSERT INTO executor_identities (task_id,origin_machine,identity_json) VALUES (?1,?2,?3)",
        params![
            task.to_string(),
            origin.to_string(),
            serde_json::to_string(&identity)?
        ],
    )?;
    events::append_produced_event_on(
        conn,
        task,
        EventPayload::State {
            status: ProcessStatus::Queued,
        },
    )?;
    Ok(())
}

/// Both machine owners of one accepted execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskOwners {
    /// Machine that owns callbacks for the task
    pub origin_machine: MachineId,
    /// Machine that runs the task
    pub execution_machine: MachineId,
}

/// Dashboard metadata read with a task row
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPresentation {
    /// Task identity
    pub id: TaskId,
    /// Nearest Git worktree root, captured when the executor accepted the task
    pub project_root: Option<PathBuf>,
    /// Owners from an accepted executor identity. Rejected and legacy tasks have none
    pub owners: Option<TaskOwners>,
    /// Typed origin-inbox ownership or legacy status for this task row
    pub terminal_callback: TerminalCallbackProjection,
    /// Whether this task's inactivity reminder reached the origin queue
    pub attention_delivered: bool,
}

impl Store {
    /// Open the SQLite file at `path`, applying the initial schema when empty
    pub fn open(path: &Path) -> Result<Self, AppError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let transaction = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version: i64 =
            transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version != SCHEMA_VERSION {
            match version {
                0 => {
                    transaction.execute_batch(BASE_SCHEMA)?;
                    migrate_2_to_current(&transaction)?;
                }
                1 => {
                    transaction.execute_batch(MIGRATE_1_TO_2)?;
                    migrate_2_to_current(&transaction)?;
                }
                2 => migrate_2_to_current(&transaction)?,
                RELEASED_V0_4_SCHEMA_VERSION => migrate_27_to_current(&transaction)?,
                RELEASED_V0_5_SCHEMA_VERSION => migrate_28_to_current(&transaction)?,
                other => return Err(unsupported_schema_version(other)),
            }
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        }
        transaction.commit()?;
        let tasks_dir = path.parent().unwrap_or(Path::new(".")).join("tasks");
        Ok(Self { conn, tasks_dir })
    }

    /// Insert a queued task
    pub fn insert_task(&self, row: &TaskRow) -> Result<(), AppError> {
        self.insert_task_with_project_root(row, None)
    }

    fn insert_task_with_project_root(
        &self,
        row: &TaskRow,
        project_root: Option<&Path>,
    ) -> Result<(), AppError> {
        insert_task_with_project_root_on(&self.conn, row, project_root)
    }

    /// Insert a new local task with both owners and its initial state event
    pub fn insert_local_task(
        &self,
        row: &TaskRow,
        spec: &NormalizedSpec,
        machine: MachineId,
        codex: CallbackExecutable,
    ) -> Result<(), AppError> {
        let callback = CallbackContext {
            env: row.env.clone(),
            cwd: row.cwd.clone(),
            codex,
        };
        self.immediate(|| {
            insert_local_task_records_on(
                &self.conn,
                row,
                spec,
                machine,
                RequestId::new(),
                &callback,
                None,
            )
        })
    }

    /// Accept one remote execution with its queued row and first outbound event atomically
    pub fn insert_remote_task(
        &self,
        row: &TaskRow,
        spec: &NormalizedSpec,
        origin: MachineId,
        execution: MachineId,
    ) -> Result<ExecutorIdentity, AppError> {
        if origin == execution
            || row.status() != ProcessStatus::Queued
            || row.name.as_ref() != Some(&spec.name)
            || row.thread != spec.thread
            || row.timeout != spec.timeout
            || !row.cwd.is_absolute()
            || !row.binary.is_absolute()
        {
            return Err(AppError::ClusterTaskConflict { task: row.id });
        }
        self.immediate(|| {
            let saved: Option<String> = self.conn.query_row(
                "SELECT identity_json FROM executor_identities WHERE task_id=?1",
                [row.id.to_string()],
                |entry| entry.get(0),
            ).optional()?;
            if let Some(saved) = saved {
                let identity: ExecutorIdentity = serde_json::from_str(&saved)?;
                let same = match &identity {
                    ExecutorIdentity::Accepted(record) => {
                        record.has_valid_spec_owners()
                            && record.origin_machine == origin
                            && record.execution_machine == execution
                            && record.current_spec() == Some(spec)
                    }
                    ExecutorIdentity::Rejected(record) => record.origin_machine == origin
                        && record.execution_machine == execution,
                };
                return if !same
                    || (matches!(&identity, ExecutorIdentity::Accepted(_))
                        && resource_task_id_is_reserved(&self.conn, row.id)?)
                {
                    Err(AppError::ClusterTaskConflict { task: row.id })
                } else {
                    Ok(identity)
                };
            }
            if resource_task_id_is_reserved(&self.conn, row.id)? {
                return Err(AppError::ClusterTaskConflict { task: row.id });
            }
            if self.get_task(row.id)?.is_some() {
                return Err(AppError::ClusterTaskConflict { task: row.id });
            }
            let project_root = find_project_root(&row.cwd);
            self.insert_task_with_project_root(row, project_root.as_deref())?;
            let identity = ExecutorIdentity::Accepted(ExecutionRecord {
                task: row.id,
                origin_machine: origin,
                execution_machine: execution,
                spec: spec.clone().into(),
                state: ProcessStatus::Queued,
            });
            self.conn.execute(
                "INSERT INTO executor_identities (task_id,origin_machine,identity_json) VALUES (?1,?2,?3)",
                params![row.id.to_string(), origin.to_string(), serde_json::to_string(&identity)?],
            )?;
            self.append_produced_event(row.id, EventPayload::State { status: ProcessStatus::Queued })?;
            Ok(identity)
        })
    }

    /// Migrate pre-Fleet task rows to local origin and executor identities
    ///
    /// The immediate transaction serializes with runner completion so either
    /// completion emits the first typed event, or migration retains its terminal
    /// callback as that first event
    pub fn migrate_legacy_local(&mut self, machine: MachineId) -> Result<(), AppError> {
        self.migrate_legacy_local_with(machine, |path, cwd| {
            crate::invocation::resolve_agent_binary(crate::domain::AgentKind::Codex, path, cwd)
        })
    }

    fn migrate_legacy_local_with(
        &mut self,
        machine: MachineId,
        resolve_codex: impl Fn(&str, &Path) -> Result<PathBuf, AppError>,
    ) -> Result<(), AppError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let rows = {
            let mut statement = tx.prepare(&format!("{TASK_SELECT} ORDER BY id"))?;
            statement
                .query_map([], parse_task_row)?
                .collect::<Result<Vec<_>, _>>()?
        };

        for row in rows {
            let identity: Option<String> = tx
                .query_row(
                    "SELECT identity_json FROM executor_identities WHERE task_id=?1",
                    [row.id.to_string()],
                    |entry| entry.get(0),
                )
                .optional()?;
            if identity.is_some() {
                continue;
            }

            let route_json: Option<String> = tx
                .query_row(
                    "SELECT route_json FROM origin_routes WHERE task_id=?1",
                    [row.id.to_string()],
                    |entry| entry.get(0),
                )
                .optional()?;
            if let Some(route_json) = route_json {
                let route: OriginRoute = serde_json::from_str(&route_json)?;
                if route.origin_machine == machine && route.execution_machine == machine {
                    return Err(AppError::Internal {
                        message: format!(
                            "local origin route for task {} has no executor identity",
                            row.id
                        ),
                    });
                }
                continue;
            }

            let pending_terminal_callback = row.state.is_terminal()
                && matches!(
                    row.callback_status,
                    CallbackStatus::Pending | CallbackStatus::Sending
                );
            let saved_notification_intents = {
                let mut statement = tx.prepare(
                    "SELECT report_seq FROM report_notification_intents
                     WHERE task_id=?1 ORDER BY report_seq",
                )?;
                statement
                    .query_map([row.id.to_string()], |entry| entry.get::<_, i64>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            if saved_notification_intents
                .windows(2)
                .any(|pair| pair[0] == pair[1])
            {
                return Err(AppError::Internal {
                    message: format!("duplicate notification intent for task {}", row.id),
                });
            }
            let reports = if saved_notification_intents.is_empty() && !pending_terminal_callback {
                Vec::new()
            } else {
                reports_from(&tx, row.id)?
            };
            let mut notification_intents = Vec::new();
            for report_seq in saved_notification_intents {
                let report = reports
                    .iter()
                    .find(|report| report.seq == report_seq)
                    .ok_or_else(|| AppError::Internal {
                        message: format!(
                            "notification intent for missing report {report_seq} on task {}",
                            row.id
                        ),
                    })?;
                if report.notified_at.is_none() {
                    notification_intents.push(report_seq);
                }
            }
            let mut next_event_seq =
                u64::try_from(notification_intents.len()).map_err(|_| AppError::Internal {
                    message: format!("too many retained notification intents for task {}", row.id),
                })?;
            if pending_terminal_callback {
                next_event_seq =
                    next_event_seq
                        .checked_add(1)
                        .ok_or_else(|| AppError::Internal {
                            message: format!("event sequence exhausted for task {}", row.id),
                        })?;
            }
            let callback = CallbackContext {
                env: row.env.clone(),
                cwd: row.cwd.clone(),
                codex: match resolve_codex(&row.env.path, &row.cwd) {
                    Ok(path) if path.is_absolute() => CallbackExecutable::available(path),
                    Ok(_) => CallbackExecutable::Unavailable {
                        reason: "resolved saved Codex executable path is not absolute".into(),
                    },
                    Err(error) => CallbackExecutable::Unavailable {
                        reason: format!("saved Codex executable could not be resolved: {error}"),
                    },
                },
            };
            let route = OriginRoute {
                request: RequestId::new(),
                task: row.id,
                origin_machine: machine,
                execution_machine: machine,
                thread: row.thread,
                callback,
                spec: PersistedSpec::MigratedLocal,
                submission: SubmissionState::Accepted,
                last_execution_state: Some(row.status()),
                last_updated_at: Some(Utc::now()),
                last_accepted_seq: next_event_seq,
                last_settled_seq: 0,
            };
            route.validate().map_err(|error| AppError::Internal {
                message: format!("invalid migrated local route for task {}: {error}", row.id),
            })?;

            let identity = ExecutorIdentity::Accepted(ExecutionRecord {
                task: row.id,
                origin_machine: machine,
                execution_machine: machine,
                spec: PersistedSpec::MigratedLocal,
                state: row.status(),
            });
            tx.execute(
                "INSERT INTO origin_routes (request_id,task_id,execution_machine,spec_json,route_json)
                 VALUES (?1,?2,?3,?4,?5)",
                params![
                    route.request.0.to_string(),
                    row.id.to_string(),
                    machine.to_string(),
                    serde_json::to_string(&route.spec)?,
                    serde_json::to_string(&route)?,
                ],
            )?;
            tx.execute(
                "INSERT INTO executor_identities (task_id,origin_machine,identity_json)
                 VALUES (?1,?2,?3)",
                params![
                    row.id.to_string(),
                    machine.to_string(),
                    serde_json::to_string(&identity)?,
                ],
            )?;

            let mut seq = 0_u64;
            for report_seq in &notification_intents {
                let report = reports
                    .iter()
                    .find(|report| report.seq == *report_seq)
                    .ok_or_else(|| AppError::Internal {
                        message: format!(
                            "notification intent for missing report {report_seq} on task {}",
                            row.id
                        ),
                    })?;
                seq = seq.checked_add(1).ok_or_else(|| AppError::Internal {
                    message: format!("event sequence exhausted for task {}", row.id),
                })?;
                let event = TaskEvent {
                    task: row.id,
                    seq: NonZeroU64::new(seq).ok_or_else(|| AppError::Internal {
                        message: "invalid migrated event sequence".into(),
                    })?,
                    origin_machine: machine,
                    execution_machine: machine,
                    payload: EventPayload::Callback {
                        event: Box::new(notify_event(
                            &row,
                            report,
                            self.tasks_dir.join(row.id.to_string()),
                        )),
                        state: None,
                    },
                };
                insert_migrated_callback_event(&tx, &event)?;
            }
            if pending_terminal_callback {
                let callback =
                    terminal_event(&row, &reports, self.tasks_dir.join(row.id.to_string()));
                seq = seq.checked_add(1).ok_or_else(|| AppError::Internal {
                    message: format!("event sequence exhausted for task {}", row.id),
                })?;
                let event = TaskEvent {
                    task: row.id,
                    seq: NonZeroU64::new(seq).ok_or_else(|| AppError::Internal {
                        message: "invalid migrated event sequence".into(),
                    })?,
                    origin_machine: machine,
                    execution_machine: machine,
                    payload: EventPayload::Callback {
                        event: Box::new(callback),
                        state: Some(row.status()),
                    },
                };
                insert_migrated_callback_event(&tx, &event)?;
            }
            if seq > 0 {
                let last_seq = i64::try_from(seq).map_err(|_| AppError::Internal {
                    message: "migrated event sequence exceeds SQLite range".into(),
                })?;
                tx.execute(
                    "INSERT INTO executor_event_cursors (task_id,last_seq) VALUES (?1,?2)",
                    params![row.id.to_string(), last_seq],
                )?;
            }
            tx.execute(
                "DELETE FROM report_notification_intents WHERE task_id=?1",
                [row.id.to_string()],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    /// Whether this task has a typed executor identity and uses sequenced events
    pub fn is_event_task(&self, id: TaskId) -> Result<bool, AppError> {
        let pair: Option<(String, Option<String>)> = self
            .conn
            .query_row(
                "SELECT e.identity_json,r.route_json FROM executor_identities e
             LEFT JOIN origin_routes r ON r.task_id=e.task_id WHERE e.task_id=?1",
                [id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((identity, route)) = pair else {
            let has_route: bool = self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM origin_routes WHERE task_id=?1)",
                [id.to_string()],
                |row| row.get(0),
            )?;
            if has_route {
                return Err(AppError::ClusterTaskConflict { task: id });
            }
            return Ok(false);
        };
        let identity: ExecutorIdentity = serde_json::from_str(&identity)?;
        let valid = match (identity, route) {
            (ExecutorIdentity::Accepted(record), Some(route)) => {
                let route: OriginRoute = serde_json::from_str(&route)?;
                route.validate().map_err(|error| AppError::Internal {
                    message: format!("invalid saved origin route: {error}"),
                })?;
                let accepted_submission = match &route.submission {
                    SubmissionState::Accepted => true,
                    SubmissionState::Resource { phase, .. } => matches!(
                        phase,
                        ResourceRoutePhase::AcceptanceUnknown
                            | ResourceRoutePhase::Waiting
                            | ResourceRoutePhase::Activated
                    ),
                    _ => false,
                };
                record.task == id
                    && route.task == id
                    && route.origin_machine == route.execution_machine
                    && record.origin_machine == route.origin_machine
                    && record.execution_machine == route.execution_machine
                    && record.has_valid_spec_owners()
                    && record.spec == route.spec
                    && accepted_submission
            }
            (ExecutorIdentity::Accepted(record), None) => {
                record.task == id
                    && record.origin_machine != record.execution_machine
                    && record.current_spec().is_some()
                    && record.has_valid_spec_owners()
            }
            _ => false,
        };
        if !valid {
            return Err(AppError::ClusterTaskConflict { task: id });
        }
        Ok(true)
    }

    /// Fetch one task
    pub fn get_task(&self, id: TaskId) -> Result<Option<TaskRow>, AppError> {
        let mut stmt = self.conn.prepare(&format!("{TASK_SELECT} WHERE id = ?1"))?;
        let row = stmt
            .query_row(params![id.to_string()], parse_task_row)
            .optional()?;
        Ok(row)
    }

    /// Require a task row
    pub fn require_task(&self, id: TaskId) -> Result<TaskRow, AppError> {
        self.get_task(id)?.ok_or(AppError::TaskNotFound { id })
    }

    /// List tasks, optionally filtered
    pub fn list_tasks(
        &self,
        statuses: &[ProcessStatus],
        thread: Option<ThreadId>,
    ) -> Result<Vec<TaskRow>, AppError> {
        let mut sql = String::from(TASK_SELECT);
        sql.push_str(" WHERE 1=1");
        if !statuses.is_empty() {
            sql.push_str(" AND status IN (");
            for (i, status) in statuses.iter().enumerate() {
                if i > 0 {
                    sql.push(',');
                }
                sql.push('\'');
                sql.push_str(status.as_str());
                sql.push('\'');
            }
            sql.push(')');
        }
        if thread.is_some() {
            sql.push_str(" AND thread_id = ?1");
        }
        sql.push_str(" ORDER BY id");
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = if let Some(thread) = thread {
            stmt.query_map(params![thread.to_string()], parse_task_row)?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map([], parse_task_row)?
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(rows)
    }

    /// Read project roots and complete accepted machine-owner pairs for task IDs
    pub fn task_presentations(
        &self,
        ids: &[TaskId],
    ) -> Result<HashMap<TaskId, TaskPresentation>, AppError> {
        let mut presentations = HashMap::with_capacity(ids.len());

        for chunk in ids.chunks(500) {
            let id_values: Vec<String> = chunk.iter().map(ToString::to_string).collect();
            let placeholders = (1..=id_values.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT t.id, t.project_root, e.identity_json
                 FROM tasks t LEFT JOIN executor_identities e ON e.task_id=t.id
                 WHERE t.id IN ({placeholders})"
            );
            let mut statement = self.conn.prepare(&sql)?;
            let rows = statement.query_map(params_from_iter(id_values.iter()), |row| {
                let raw_id: String = row.get(0)?;
                let project_root: Option<String> = row.get(1)?;
                let identity_json: Option<String> = row.get(2)?;
                let conversion_error = |error: AppError| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                };
                let id = raw_id.parse().map_err(conversion_error)?;
                let owners = match identity_json {
                    Some(json) => {
                        let identity: ExecutorIdentity =
                            serde_json::from_str(&json).map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    2,
                                    rusqlite::types::Type::Text,
                                    Box::new(error),
                                )
                            })?;
                        match identity {
                            ExecutorIdentity::Accepted(record) if record.task == id => {
                                Some(TaskOwners {
                                    origin_machine: record.origin_machine,
                                    execution_machine: record.execution_machine,
                                })
                            }
                            ExecutorIdentity::Accepted(_) => {
                                return Err(conversion_error(AppError::Internal {
                                    message: format!(
                                        "executor identity task does not match task row {id}"
                                    ),
                                }));
                            }
                            ExecutorIdentity::Rejected(_) => None,
                        }
                    }
                    None => None,
                };
                Ok(TaskPresentation {
                    id,
                    project_root: project_root.map(PathBuf::from),
                    owners,
                    terminal_callback: TerminalCallbackProjection::Legacy(CallbackStatus::Pending),
                    attention_delivered: false,
                })
            })?;
            let rows = rows.collect::<Result<Vec<_>, _>>()?;
            drop(statement);

            for mut presentation in rows {
                let task = self.require_task(presentation.id)?;
                presentation.terminal_callback = self.terminal_callback_projection(&task)?;
                presentation.attention_delivered = self.attention_callback_delivered(&task)?;
                presentations.insert(presentation.id, presentation);
            }
        }

        Ok(presentations)
    }

    /// Whether a terminal callback still waits for its inbox result to settle
    pub fn has_pending_terminal_callbacks(&self) -> Result<bool, AppError> {
        let rows = self.list_tasks(
            &[
                ProcessStatus::Succeeded,
                ProcessStatus::Failed,
                ProcessStatus::Cancelled,
                ProcessStatus::Lost,
            ],
            None,
        )?;
        for row in rows {
            match self.terminal_callback_projection(&row)? {
                TerminalCallbackProjection::Legacy(status)
                | TerminalCallbackProjection::OriginInbox(status)
                    if matches!(status, CallbackStatus::Pending | CallbackStatus::Sending) =>
                {
                    return Ok(true);
                }
                TerminalCallbackProjection::Legacy(_)
                | TerminalCallbackProjection::OriginInbox(_)
                | TerminalCallbackProjection::NotOwned => {}
            }
        }
        Ok(false)
    }

    fn terminal_callback_projection(
        &self,
        row: &TaskRow,
    ) -> Result<TerminalCallbackProjection, AppError> {
        let route_json: Option<String> = self
            .conn
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [row.id.to_string()],
                |entry| entry.get(0),
            )
            .optional()?;
        let Some(route_json) = route_json else {
            let identity = self
                .executor_identity(row.id)
                .map_err(|error| match error {
                    IdentityError::Conflict | IdentityError::RouteNotFound => {
                        AppError::ClusterTaskConflict { task: row.id }
                    }
                    IdentityError::Storage(error) => error,
                })?;
            return match identity {
                Some(ExecutorIdentity::Accepted(record))
                    if record.task == row.id
                        && record.origin_machine != record.execution_machine =>
                {
                    Ok(TerminalCallbackProjection::NotOwned)
                }
                Some(_) => Err(AppError::ClusterTaskConflict { task: row.id }),
                None => Ok(TerminalCallbackProjection::Legacy(row.callback_status)),
            };
        };
        if !self.is_event_task(row.id)? {
            return Err(AppError::ClusterTaskConflict { task: row.id });
        }
        let route: OriginRoute = serde_json::from_str(&route_json)?;
        if route.task != row.id {
            return Err(AppError::Internal {
                message: format!("origin route task does not match task row {}", row.id),
            });
        }
        if route.origin_machine != route.execution_machine {
            return Ok(TerminalCallbackProjection::NotOwned);
        }
        if !row.state.is_terminal() {
            return Ok(TerminalCallbackProjection::OriginInbox(
                CallbackStatus::Pending,
            ));
        }
        if let Some(delivery) = self.terminal_callback_delivery(row.id)? {
            return match delivery {
                DeliveryState::PendingDelivery { attempts, .. } => {
                    Ok(TerminalCallbackProjection::OriginInbox(if attempts == 0 {
                        CallbackStatus::Pending
                    } else {
                        CallbackStatus::Sending
                    }))
                }
                DeliveryState::Delivered { .. } => Ok(TerminalCallbackProjection::OriginInbox(
                    CallbackStatus::Sent,
                )),
                DeliveryState::DeliveryFailed { .. } => Ok(
                    TerminalCallbackProjection::OriginInbox(CallbackStatus::Failed),
                ),
                DeliveryState::NotRequired => Err(AppError::Internal {
                    message: format!("terminal callback event {} is marked not required", row.id),
                }),
            };
        }
        if self.has_terminal_callback_outbox(row.id)? {
            return Ok(TerminalCallbackProjection::OriginInbox(
                CallbackStatus::Pending,
            ));
        }
        if matches!(
            row.callback_status,
            CallbackStatus::Sent | CallbackStatus::Failed
        ) {
            return Ok(TerminalCallbackProjection::Legacy(row.callback_status));
        }
        Ok(TerminalCallbackProjection::OriginInbox(
            CallbackStatus::Pending,
        ))
    }

    fn terminal_callback_delivery(&self, id: TaskId) -> Result<Option<DeliveryState>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT seq,event_json,delivery_json FROM origin_inbox
             WHERE task_id=?1 ORDER BY seq",
        )?;
        let rows = statement
            .query_map([id.to_string()], |entry| {
                Ok((
                    entry.get::<_, i64>(0)?,
                    entry.get::<_, String>(1)?,
                    entry.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut latest: Option<(i64, DeliveryState)> = None;
        for (seq, event_json, delivery_json) in rows {
            let event: TaskEvent = serde_json::from_str(&event_json)?;
            if event.task != id {
                return Err(AppError::Internal {
                    message: format!("inbox event task does not match task row {id}"),
                });
            }
            if is_terminal_callback_event(&event) {
                latest = Some((seq, serde_json::from_str(&delivery_json)?));
            }
        }
        let mut statement = self.conn.prepare(
            "SELECT seq,delivery_json FROM origin_event_receipts
             WHERE task_id=?1 AND terminal_callback=1 ORDER BY seq",
        )?;
        let receipts = statement
            .query_map([id.to_string()], |entry| {
                Ok((entry.get::<_, i64>(0)?, entry.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (seq, delivery_json) in receipts {
            if latest
                .as_ref()
                .is_none_or(|(latest_seq, _)| seq > *latest_seq)
            {
                latest = Some((seq, serde_json::from_str(&delivery_json)?));
            }
        }
        Ok(latest.map(|(_, delivery)| delivery))
    }

    fn attention_callback_delivered(&self, row: &TaskRow) -> Result<bool, AppError> {
        let route_json: Option<String> = self
            .conn
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [row.id.to_string()],
                |entry| entry.get(0),
            )
            .optional()?;
        let Some(route_json) = route_json else {
            let identity = self
                .executor_identity(row.id)
                .map_err(|error| match error {
                    IdentityError::Conflict | IdentityError::RouteNotFound => {
                        AppError::ClusterTaskConflict { task: row.id }
                    }
                    IdentityError::Storage(error) => error,
                })?;
            return match identity {
                Some(ExecutorIdentity::Accepted(record))
                    if record.task == row.id
                        && record.origin_machine != record.execution_machine =>
                {
                    Ok(false)
                }
                Some(_) => Err(AppError::ClusterTaskConflict { task: row.id }),
                None => Ok(row.attention.is_delivered()),
            };
        };
        let route: OriginRoute = serde_json::from_str(&route_json)?;
        if route.task != row.id {
            return Err(AppError::Internal {
                message: format!("origin route task does not match task row {}", row.id),
            });
        }
        if route.origin_machine != route.execution_machine {
            return Ok(false);
        }

        let mut statement = self.conn.prepare(
            "SELECT event_json,delivery_json FROM origin_inbox
             WHERE task_id=?1 ORDER BY seq",
        )?;
        let rows = statement
            .query_map([row.id.to_string()], |entry| {
                Ok((entry.get::<_, String>(0)?, entry.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut latest = None;
        for (event_json, delivery_json) in rows {
            let event: TaskEvent = serde_json::from_str(&event_json)?;
            if let EventPayload::Callback { event, .. } = event.payload
                && event.event == EventKind::TaskCheckDue
            {
                latest = Some(serde_json::from_str::<DeliveryState>(&delivery_json)?);
            }
        }
        if let Some(delivery) = latest {
            return Ok(matches!(delivery, DeliveryState::Delivered { .. }));
        }

        let mut statement = self.conn.prepare(
            "SELECT event_json FROM executor_outbox
             WHERE task_id=?1 AND state='pending' ORDER BY seq",
        )?;
        let events = statement
            .query_map([row.id.to_string()], |entry| entry.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for event_json in events {
            let event: TaskEvent = serde_json::from_str(&event_json)?;
            if let EventPayload::Callback { event, .. } = event.payload
                && event.event == EventKind::TaskCheckDue
            {
                return Ok(false);
            }
        }

        // legacy delivered rows keep their marker after event payload compaction
        Ok(row.attention.is_delivered())
    }

    fn has_terminal_callback_outbox(&self, id: TaskId) -> Result<bool, AppError> {
        let mut statement = self
            .conn
            .prepare("SELECT event_json FROM executor_outbox WHERE task_id=?1 ORDER BY seq")?;
        let rows = statement
            .query_map([id.to_string()], |entry| entry.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for event_json in rows {
            let event: TaskEvent = serde_json::from_str(&event_json)?;
            if event.task != id {
                return Err(AppError::Internal {
                    message: format!("outbox event task does not match task row {id}"),
                });
            }
            if is_terminal_callback_event(&event) {
                return Ok(true);
            }
        }
        Ok(self.conn.query_row(
            "SELECT EXISTS(
                     SELECT 1 FROM executor_event_receipts
                     WHERE task_id=?1 AND terminal_callback=1
                 )",
            [id.to_string()],
            |row| row.get(0),
        )?)
    }

    /// Non-terminal tasks
    pub fn non_terminal(&self) -> Result<Vec<TaskRow>, AppError> {
        self.list_tasks(&[ProcessStatus::Queued, ProcessStatus::Running], None)
    }

    /// Count of queued or running tasks
    pub fn in_flight_count(&self) -> Result<usize, AppError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM tasks WHERE status IN ('queued', 'running')",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Compare-and-swap process status. `None` means the CAS did not match
    pub fn cas_status(
        &self,
        id: TaskId,
        from: ProcessStatus,
        to: ProcessStatus,
    ) -> Result<Option<TaskRow>, AppError> {
        check_status_transition(from, to)?;
        self.immediate(|| {
            let n = self.conn.execute(
                "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE id = ?3 AND status = ?4",
                params![
                    to.as_str(),
                    fmt_time(Utc::now()),
                    id.to_string(),
                    from.as_str()
                ],
            )?;
            let row = self.row_after_cas(id, n)?;
            if let Some(row) = &row {
                self.produce_state_event(row)?;
            }
            Ok(row)
        })
    }

    /// CAS status and store an exit reason
    pub fn cas_exit(
        &self,
        id: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
    ) -> Result<Option<TaskRow>, AppError> {
        let evidence = if from == ProcessStatus::Queued {
            ProcessGroupExitEvidence::NoChildSpawned
        } else {
            ProcessGroupExitEvidence::Unconfirmed
        };
        self.cas_exit_with_evidence(id, from, reason, evidence)
    }

    /// CAS terminal state and persist evidence from the task-run worker or its exit-file recovery
    pub(crate) fn cas_exit_with_evidence(
        &self,
        id: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
        evidence: impl Into<TaskExitEvidence>,
    ) -> Result<Option<TaskRow>, AppError> {
        let evidence = &evidence.into();
        check_exit_evidence(from, reason, evidence)?;
        let to = ProcessStatus::from(reason);
        check_status_transition(from, to)?;
        self.immediate(|| {
            if evidence.container != ContainerExitEvidence::Unconfirmed
                && !matches!(
                    self.get_task(id)?.map(|row| row.workload),
                    Some(Workload::Container(_))
                )
            {
                return Err(AppError::Internal {
                    message: "container evidence requires a container task".into(),
                });
            }
            self.cas_exit_inner(id, from, reason, evidence)
        })
    }

    fn cas_exit_inner(
        &self,
        id: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
        evidence: &TaskExitEvidence,
    ) -> Result<Option<TaskRow>, AppError> {
        let to = ProcessStatus::from(reason);
        let now = fmt_time(Utc::now());
        let reason_json = serde_json::to_string(reason)?;
        let n = self.conn.execute(
            "UPDATE tasks SET status = ?1, exit_reason = ?2,
                process_group_exit_evidence = ?3, container_exit_evidence = ?4, updated_at = ?5
             WHERE id = ?6 AND status = ?7",
            params![
                to.as_str(),
                reason_json,
                evidence.process_group.as_str(),
                evidence.container.to_storage()?,
                now,
                id.to_string(),
                from.as_str()
            ],
        )?;
        let row = self.row_after_cas(id, n)?;
        if let Some(row) = &row {
            self.produce_state_event(row)?;
        }
        Ok(row)
    }

    fn produce_state_event(&self, row: &TaskRow) -> Result<(), AppError> {
        if !self.is_event_task(row.id)? {
            return Ok(());
        }
        let identity: String = self.conn.query_row(
            "SELECT identity_json FROM executor_identities WHERE task_id=?1",
            [row.id.to_string()],
            |entry| entry.get(0),
        )?;
        let mut identity: ExecutorIdentity = serde_json::from_str(&identity)?;
        let ExecutorIdentity::Accepted(record) = &mut identity else {
            return Err(AppError::ClusterTaskConflict { task: row.id });
        };
        record.state = row.status();
        self.conn.execute(
            "UPDATE executor_identities SET identity_json=?1 WHERE task_id=?2",
            params![serde_json::to_string(&identity)?, row.id.to_string()],
        )?;
        let payload = if row.state.is_terminal() {
            let reports = self.reports(row.id)?;
            let evidence = self.tasks_dir.join(row.id.to_string());
            let callback = if row.status() == ProcessStatus::Lost {
                lost_event(row, &reports, evidence)
            } else {
                exit_event(row, &reports, evidence)
            };
            EventPayload::Callback {
                event: Box::new(callback),
                state: Some(row.status()),
            }
        } else {
            EventPayload::State {
                status: row.status(),
            }
        };
        self.append_produced_event(row.id, payload)
    }

    fn row_after_cas(&self, id: TaskId, updated: usize) -> Result<Option<TaskRow>, AppError> {
        if updated == 1 {
            Ok(Some(self.require_task(id)?))
        } else {
            Ok(None)
        }
    }

    /// Record the worker pid
    pub fn set_pid(&self, id: TaskId, pid: i32) -> Result<(), AppError> {
        let now = fmt_time(Utc::now());
        self.conn.execute(
            "UPDATE tasks SET pid = ?1, updated_at = ?2 WHERE id = ?3",
            params![pid, now, id.to_string()],
        )?;
        Ok(())
    }

    /// Mark cancel requested. Terminal tasks are unchanged (idempotent)
    pub fn request_cancel(&self, id: TaskId) -> Result<CancelResult, AppError> {
        self.immediate(|| {
            self.require_task(id)?;
            self.conn.execute(
                "UPDATE tasks SET cancel_requested_at = ?1, updated_at = ?1
                 WHERE id = ?2 AND status NOT IN ('succeeded', 'failed', 'cancelled', 'lost')",
                params![fmt_time(Utc::now()), id.to_string()],
            )?;
            if let Some(row) = self.cas_exit_inner(
                id,
                ProcessStatus::Queued,
                &ExitReason::Cancelled,
                &ProcessGroupExitEvidence::NoChildSpawned.into(),
            )? {
                return Ok(CancelResult::CancelledQueued(row));
            }
            let row = self.require_task(id)?;
            if row.state.is_terminal() {
                Ok(CancelResult::AlreadyTerminal(row))
            } else {
                Ok(CancelResult::SignalWorker(row))
            }
        })
    }

    /// Run `body` inside `BEGIN IMMEDIATE`, rolling back on error
    fn immediate<T>(&self, body: impl FnOnce() -> Result<T, AppError>) -> Result<T, AppError> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        match body() {
            Ok(value) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(value)
            }
            Err(err) => {
                if let Err(rollback) = self.conn.execute_batch("ROLLBACK") {
                    tracing::warn!("rollback after {err}: {rollback}");
                }
                Err(err)
            }
        }
    }

    /// Produce one inactivity callback while the task is running
    pub fn produce_attention_event(&self, id: TaskId) -> Result<bool, AppError> {
        self.immediate(|| {
            if !self.is_event_task(id)? {
                return Ok(false);
            }
            let now = fmt_time(Utc::now());
            let changed = self.conn.execute(
                "UPDATE tasks SET attention_state='delivered', timeout_notified_at=?1, updated_at=?1
                 WHERE id=?2 AND attention_state='pending' AND status='running'",
                params![now, id.to_string()],
            )?;
            if changed == 0 {
                return Ok(false);
            }
            let row = self.require_task(id)?;
            let reports = self.reports(id)?;
            let event = check_due_event(&row, &reports, self.tasks_dir.join(id.to_string()));
            self.append_produced_event(
                id,
                EventPayload::Callback {
                    event: Box::new(event),
                    state: None,
                },
            )?;
            Ok(true)
        })
    }

    /// Drop a claim that did not deliver, so a later attempt can take it
    /// Also the release valve for a claim stranded by a dead daemon
    pub fn release_attention(&self, id: TaskId) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE tasks SET attention_state = 'pending', updated_at = ?1
             WHERE id = ?2 AND attention_state = 'sending'",
            params![fmt_time(Utc::now()), id.to_string()],
        )?;
        Ok(())
    }

    /// Append a report. Enforces cap, summary length, and terminal rejection
    pub fn append_report(
        &self,
        id: TaskId,
        outcome: ReportOutcome,
        summary: &str,
    ) -> Result<Vec<TaskReport>, AppError> {
        self.append_report_with_notification(id, outcome, summary, false)
    }

    /// Commit a report and its silent or notifying event in one transaction
    pub fn append_report_with_notification(
        &self,
        id: TaskId,
        outcome: ReportOutcome,
        summary: &str,
        notify: bool,
    ) -> Result<Vec<TaskReport>, AppError> {
        if summary.len() > SUMMARY_MAX_BYTES {
            return Err(AppError::SummaryTooLong { len: summary.len() });
        }
        self.immediate(|| {
            let row = self.require_task(id)?;
            check_report_allowed(row.status()).map_err(|_| AppError::TaskTerminal {
                id,
                status: row.status(),
            })?;
            let existing = self.reports(id)?;
            if existing.len() >= REPORTS_MAX {
                return Err(AppError::TooManyReports {
                    count: existing.len(),
                });
            }
            let seq = existing.len() as i64 + 1;
            self.conn.execute(
                "INSERT INTO reports (task_id, seq, outcome, summary, reported_at, notified_at)
                 VALUES (?1,?2,?3,?4,?5,NULL)",
                params![
                    id.to_string(),
                    seq,
                    outcome.as_str(),
                    summary,
                    fmt_time(Utc::now())
                ],
            )?;
            let reports = self.reports(id)?;
            if self.is_event_task(id)? {
                let report = reports.last().ok_or(AppError::Internal {
                    message: "inserted report missing".into(),
                })?;
                let payload = if notify {
                    EventPayload::Callback {
                        event: Box::new(notify_event(
                            &row,
                            report,
                            self.tasks_dir.join(id.to_string()),
                        )),
                        state: None,
                    }
                } else {
                    EventPayload::Report {
                        report: ReportView::from(report),
                    }
                };
                self.append_produced_event(id, payload)?;
            } else if notify {
                self.conn.execute(
                    "INSERT INTO report_notification_intents (task_id,report_seq,requested_at)
                     VALUES (?1,?2,?3)",
                    params![id.to_string(), seq, fmt_time(Utc::now())],
                )?;
            }
            Ok(reports)
        })
    }

    /// Reports in seq order
    pub fn reports(&self, id: TaskId) -> Result<Vec<TaskReport>, AppError> {
        reports_from(&self.conn, id)
    }
}

fn insert_migrated_callback_event(conn: &Connection, event: &TaskEvent) -> Result<(), AppError> {
    let seq = i64::try_from(event.seq.get()).map_err(|_| AppError::Internal {
        message: "migrated event sequence exceeds SQLite range".into(),
    })?;
    let event_json = serde_json::to_string(event)?;
    let delivery = DeliveryState::PendingDelivery {
        attempts: 0,
        last_error: None,
    };
    conn.execute(
        "INSERT INTO executor_outbox
         (task_id,seq,origin_machine,execution_machine,event_json,notification_required,state,acknowledged_at)
         VALUES (?1,?2,?3,?4,?5,1,'acknowledged',?6)",
        params![
            event.task.to_string(),
            seq,
            event.origin_machine.to_string(),
            event.execution_machine.to_string(),
            event_json,
            fmt_time(Utc::now()),
        ],
    )?;
    conn.execute(
        "INSERT INTO origin_inbox
         (task_id,seq,origin_machine,execution_machine,event_json,notification_required,delivery_json)
         VALUES (?1,?2,?3,?4,?5,1,?6)",
        params![
            event.task.to_string(),
            seq,
            event.origin_machine.to_string(),
            event.execution_machine.to_string(),
            event_json,
            serde_json::to_string(&delivery)?,
        ],
    )?;
    Ok(())
}

fn is_terminal_callback_event(event: &TaskEvent) -> bool {
    matches!(
        &event.payload,
        EventPayload::Callback {
            state: Some(status),
            ..
        } if status.is_terminal()
    )
}

fn reports_from(conn: &Connection, id: TaskId) -> Result<Vec<TaskReport>, AppError> {
    let mut statement = conn.prepare(
        "SELECT seq, outcome, summary, reported_at, notified_at
         FROM reports WHERE task_id = ?1 ORDER BY seq",
    )?;
    let rows = statement
        .query_map(params![id.to_string()], |row| {
            let seq: i64 = row.get(0)?;
            let outcome: String = row.get(1)?;
            let summary: String = row.get(2)?;
            let reported_at: String = row.get(3)?;
            let notified_at: Option<String> = row.get(4)?;
            Ok((seq, outcome, summary, reported_at, notified_at))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(seq, outcome, summary, reported_at, notified_at)| {
            Ok(TaskReport {
                seq,
                outcome: ReportOutcome::from_storage(&outcome)?,
                summary,
                reported_at: parse_time(&reported_at)?,
                notified_at: notified_at.as_deref().map(parse_time).transpose()?,
            })
        })
        .collect()
}

/// Result of `request_cancel`
#[derive(Debug)]
pub enum CancelResult {
    /// Already terminal: no change
    AlreadyTerminal(TaskRow),
    /// Queued task flipped to Cancelled
    CancelledQueued(TaskRow),
    /// Running task: caller must SIGTERM the worker group
    SignalWorker(TaskRow),
}

/// Refuse terminal evidence that no worker path records with this transition
fn check_exit_evidence(
    from: ProcessStatus,
    reason: &ExitReason,
    evidence: &TaskExitEvidence,
) -> Result<(), AppError> {
    let refuse = |message: &str| {
        Err(AppError::Internal {
            message: message.into(),
        })
    };
    if evidence.process_group == ProcessGroupExitEvidence::ConfirmedExited
        && from != ProcessStatus::Running
    {
        return refuse("confirmed process-group exit requires a running task worker");
    }
    // a worker that won Queued->Running may already have spawned, so only its
    // own pre-spawn failure can claim that no child exists
    if evidence.process_group == ProcessGroupExitEvidence::NoChildSpawned
        && from != ProcessStatus::Queued
        && !matches!(reason, ExitReason::SpawnFailed { .. })
    {
        return refuse("no-child evidence after start requires a spawn failure");
    }
    match &evidence.container {
        ContainerExitEvidence::Unconfirmed => Ok(()),
        _ if from != ProcessStatus::Running => {
            refuse("container evidence requires a running task worker")
        }
        ContainerExitEvidence::NeverStarted
            if !matches!(
                reason,
                ExitReason::SpawnFailed { .. } | ExitReason::Cancelled
            ) =>
        {
            refuse("never-started container evidence requires a spawn failure or a cancel")
        }
        ContainerExitEvidence::Confirmed { exit_code, .. }
            if !matches!(reason, ExitReason::Cancelled)
                && *reason != (ExitReason::Exit { code: *exit_code }) =>
        {
            refuse("confirmed container evidence must match the task exit code")
        }
        ContainerExitEvidence::NeverStarted | ContainerExitEvidence::Confirmed { .. } => Ok(()),
    }
}

fn fmt_time(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Whole seconds as decimal text. `u64` is wider than SQLite's INTEGER, and
/// the inactivity timer has no product maximum
fn fmt_timeout(timeout: Duration) -> String {
    timeout.as_secs().to_string()
}

fn parse_timeout(value: &str) -> Result<Duration, AppError> {
    value
        .parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|err| AppError::Internal {
            message: format!("bad timeout_secs {value}: {err}"),
        })
}

// failed marker checks only omit display metadata; they never reject task acceptance
fn find_project_root(cwd: &Path) -> Option<PathBuf> {
    cwd.ancestors().find_map(|ancestor| {
        let marker = ancestor.join(".git");
        let metadata = std::fs::metadata(marker).ok()?;
        (metadata.is_dir() || metadata.is_file()).then(|| ancestor.to_path_buf())
    })
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, AppError> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| AppError::Internal {
            message: format!("bad timestamp {value}: {err}"),
        })
}

fn parse_task_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRow> {
    let id: String = row.get(0)?;
    let thread: String = row.get(1)?;
    let name: Option<String> = row.get(2)?;
    let workload_json: String = row.get(3)?;
    let cwd: String = row.get(4)?;
    let timeout_secs: String = row.get(5)?;
    let env_path: String = row.get(6)?;
    let env_home: String = row.get(7)?;
    let binary: String = row.get(8)?;
    let status: String = row.get(9)?;
    let exit_reason: Option<String> = row.get(10)?;
    let callback_status: String = row.get(11)?;
    let attention_state: String = row.get(12)?;
    let timeout_notified_at: Option<String> = row.get(13)?;
    let pid: Option<i32> = row.get(14)?;
    let cancel_requested_at: Option<String> = row.get(15)?;
    let created_at: String = row.get(16)?;
    let updated_at: String = row.get(17)?;
    let process_group_exit_evidence: Option<String> = row.get(18)?;
    let container_exit_evidence: Option<String> = row.get(19)?;

    let parse_err = |err: AppError| rusqlite::Error::ToSqlConversionFailure(Box::new(err));

    let id: TaskId = id.parse().map_err(parse_err)?;
    let thread: ThreadId = thread.parse().map_err(parse_err)?;
    let name = match name {
        Some(raw) => Some(TaskName::parse(&raw).map_err(|err| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(AppError::Internal {
                message: format!("stored task name: {err}"),
            }))
        })?),
        None => None,
    };
    let workload: Workload = serde_json::from_str(&workload_json).map_err(|err| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(AppError::Internal {
            message: err.to_string(),
        }))
    })?;
    let exit_reason = match exit_reason {
        Some(raw) => Some(serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(AppError::Internal {
                message: err.to_string(),
            }))
        })?),
        None => None,
    };
    let status = ProcessStatus::from_storage(&status).map_err(parse_err)?;
    let (process_group_exit_evidence, container_exit_evidence) = match status {
        ProcessStatus::Succeeded | ProcessStatus::Failed | ProcessStatus::Cancelled => (
            ProcessGroupExitEvidence::from_storage(process_group_exit_evidence.as_deref()),
            ContainerExitEvidence::from_storage(container_exit_evidence.as_deref()),
        ),
        ProcessStatus::Queued | ProcessStatus::Running | ProcessStatus::Lost => (
            ProcessGroupExitEvidence::Unconfirmed,
            ContainerExitEvidence::Unconfirmed,
        ),
    };
    let callback_status = CallbackStatus::from_storage(&callback_status).map_err(parse_err)?;
    let timeout_notified_at = match timeout_notified_at {
        Some(raw) => Some(parse_time(&raw).map_err(parse_err)?),
        None => None,
    };
    let attention =
        AttentionState::from_storage(&attention_state, timeout_notified_at).map_err(parse_err)?;
    let cancel_requested_at = match cancel_requested_at {
        Some(raw) => Some(parse_time(&raw).map_err(parse_err)?),
        None => None,
    };
    Ok(TaskRow {
        id,
        name,
        thread,
        workload,
        cwd: Path::new(&cwd).to_path_buf(),
        timeout: parse_timeout(&timeout_secs).map_err(parse_err)?,
        env: TaskEnv {
            path: env_path,
            home: env_home,
        },
        binary: Path::new(&binary).to_path_buf(),
        state: TaskState::from_storage(status, exit_reason, pid).map_err(parse_err)?,
        process_group_exit_evidence,
        container_exit_evidence,
        callback_status,
        attention,
        cancel_requested_at,
        created_at: parse_time(&created_at).map_err(parse_err)?,
        updated_at: parse_time(&updated_at).map_err(parse_err)?,
    })
}

/// Inputs for a newly queued task
pub struct NewTask {
    /// Task id
    pub id: TaskId,
    /// Submitted name. `None` only for rows stored before name was required
    pub name: Option<TaskName>,
    /// Submitting thread
    pub thread: ThreadId,
    /// Workload configuration
    pub workload: Workload,
    /// Working directory
    pub cwd: std::path::PathBuf,
    /// Output-inactivity timeout
    pub timeout: Duration,
    /// Captured env
    pub env: TaskEnv,
    /// Resolved binary
    pub binary: std::path::PathBuf,
}

/// Build a queued row for insert
#[must_use]
pub fn new_queued_task(new: NewTask) -> TaskRow {
    let now = Utc::now();
    TaskRow {
        id: new.id,
        name: new.name,
        thread: new.thread,
        workload: new.workload,
        cwd: new.cwd,
        timeout: new.timeout,
        env: new.env,
        binary: new.binary,
        state: TaskState::Queued,
        process_group_exit_evidence: ProcessGroupExitEvidence::Unconfirmed,
        container_exit_evidence: ContainerExitEvidence::Unconfirmed,
        callback_status: CallbackStatus::Pending,
        attention: AttentionState::Pending,
        cancel_requested_at: None,
        created_at: now,
        updated_at: now,
    }
}

/// JSON view of `exit.json`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExitJson {
    /// Exit reason
    pub reason: ExitReason,
    /// Evidence for the task-run worker's child process group
    #[serde(default)]
    pub process_group_exit_evidence: ProcessGroupExitEvidence,
    /// Evidence for a container task's container
    #[serde(default, skip_serializing_if = "is_unconfirmed_container")]
    pub container_exit_evidence: ContainerExitEvidence,
}

impl ExitJson {
    /// Evidence recorded with this exit
    #[must_use]
    pub fn evidence(&self) -> TaskExitEvidence {
        TaskExitEvidence {
            process_group: self.process_group_exit_evidence,
            container: self.container_exit_evidence.clone(),
        }
    }
}

fn is_unconfirmed_container(evidence: &ContainerExitEvidence) -> bool {
    *evidence == ContainerExitEvidence::Unconfirmed
}

/// Parse `exit.json` if present
pub fn read_exit_json(path: &Path) -> Result<Option<ExitJson>, AppError> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let value: Value = serde_json::from_str(&text)?;
            let parsed: ExitJson = serde_json::from_value(value)?;
            Ok(Some(parsed))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Write `exit.json` via temp + rename
pub fn write_exit_json(path: &Path, reason: &ExitReason) -> Result<(), AppError> {
    write_exit_json_with_evidence(path, reason, TaskExitEvidence::default())
}

pub(crate) fn write_exit_json_with_evidence(
    path: &Path,
    reason: &ExitReason,
    evidence: impl Into<TaskExitEvidence>,
) -> Result<(), AppError> {
    let evidence = evidence.into();
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(&ExitJson {
        reason: reason.clone(),
        process_group_exit_evidence: evidence.process_group,
        container_exit_evidence: evidence.container.clone(),
    })?;
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BASE_SCHEMA, CancelResult, NewTask, RELEASED_V0_4_SCHEMA_VERSION,
        RELEASED_V0_5_SCHEMA_VERSION, Store, new_queued_task, read_exit_json,
        write_exit_json_with_evidence,
    };
    use crate::callback::EventKind;
    use crate::daemon::api::views::TaskSummary;
    use crate::domain::{
        Agent, AgentKind, AgentWorkload, AttentionState, CallbackStatus, ContainerExitEvidence,
        ContainerId, ExitReason, ProcessGroupExitEvidence, ProcessStatus, ReportOutcome,
        SCHEMA_VERSION, SUMMARY_MAX_BYTES, TaskEnv, TaskExitEvidence, TaskId, TaskName, TaskRow,
        TaskState, TaskWorkload, TerminalCallbackProjection, ThreadId, TransitionError, Workload,
    };
    use crate::error::AppError;
    use crate::events::{DeliveryOutcome, DeliveryState, EventPayload, OutboxState};
    use crate::invocation::CommandLine;
    use crate::machine::MachineId;
    use crate::spec::NormalizedSpec;
    use crate::submission::{
        CallbackContext, CallbackExecutable, ExecutorIdentity, OriginRoute, PersistedSpec,
        RequestId, SubmissionState,
    };
    use chrono::Utc;
    use rusqlite::{Connection, OptionalExtension, params};
    use serde_json::json;
    use std::num::NonZeroU64;
    use std::path::Path;
    use std::str::FromStr;
    use std::time::Duration;
    use tempfile::tempdir;

    fn agent_row(id: TaskId) -> TaskRow {
        new_queued_task(NewTask {
            id,
            name: Some(TaskName::parse("agent job").unwrap()),
            thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
            workload: Workload::Agent(AgentWorkload {
                agent: Agent::new(AgentKind::Claude, Some("fable".into())),
                extra_args: vec!["--verbose".into()],
                report_trailer: true,
            }),
            cwd: Path::new("/tmp").to_path_buf(),
            timeout: Duration::from_secs(4 * 3600),
            env: TaskEnv {
                path: "/bin".into(),
                home: "/home/u".into(),
            },
            binary: Path::new("/bin/true").to_path_buf(),
        })
    }

    fn task_row(id: TaskId) -> TaskRow {
        new_queued_task(NewTask {
            id,
            name: None,
            thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
            workload: Workload::Task(TaskWorkload {
                command: CommandLine::try_from_argv(vec![
                    "cargo".into(),
                    "build".into(),
                    "--release".into(),
                ])
                .unwrap(),
            }),
            cwd: Path::new("/tmp").to_path_buf(),
            timeout: Duration::from_secs(4 * 3600),
            env: TaskEnv {
                path: "/bin".into(),
                home: "/home/u".into(),
            },
            binary: Path::new("/bin/cargo").to_path_buf(),
        })
    }

    fn local_spec(row: &TaskRow) -> NormalizedSpec {
        serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "thread": row.thread,
            "name": "local task",
            "cwd": row.cwd,
            "timeout": "4h",
            "workload": { "type": "task", "command": ["true"] }
        }))
        .unwrap()
    }

    fn insert_local(store: &Store, id: TaskId) {
        let mut row = task_row(id);
        row.name = Some(TaskName::parse("local task").unwrap());
        let spec = local_spec(&row);
        store
            .insert_local_task(
                &row,
                &spec,
                MachineId::new(),
                CallbackExecutable::available("/bin/true".into()),
            )
            .unwrap();
    }

    #[test]
    fn terminal_process_group_evidence_survives_restart_with_its_event() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("db");
        let id = TaskId::new();
        {
            let store = Store::open(&path).unwrap();
            insert_local(&store, id);
            store
                .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
                .unwrap();
            store
                .cas_exit_with_evidence(
                    id,
                    ProcessStatus::Running,
                    &ExitReason::Exit { code: 0 },
                    ProcessGroupExitEvidence::ConfirmedExited,
                )
                .unwrap();
        }

        let store = Store::open(&path).unwrap();
        let row = store.require_task(id).unwrap();
        assert_eq!(row.status(), ProcessStatus::Succeeded);
        assert_eq!(
            row.process_group_exit_evidence(),
            ProcessGroupExitEvidence::ConfirmedExited
        );
        assert_eq!(
            store.process_group_exit_evidence(id).unwrap(),
            Some(ProcessGroupExitEvidence::ConfirmedExited)
        );
        let terminal_events = store
            .pending_outbound_events(id)
            .unwrap()
            .into_iter()
            .filter(|event| {
                matches!(
                    &event.event.payload,
                    EventPayload::Callback {
                        state: Some(ProcessStatus::Succeeded),
                        ..
                    }
                )
            })
            .count();
        assert_eq!(terminal_events, 1);
    }

    #[test]
    fn terminal_evidence_rolls_back_when_its_event_cannot_commit() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let id = TaskId::new();
        insert_local(&store, id);
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER reject_terminal_event BEFORE INSERT ON executor_outbox
                 WHEN NEW.seq = 3
                 BEGIN SELECT RAISE(ABORT, 'terminal event unavailable'); END;",
            )
            .unwrap();

        assert!(
            store
                .cas_exit_with_evidence(
                    id,
                    ProcessStatus::Running,
                    &ExitReason::Exit { code: 0 },
                    ProcessGroupExitEvidence::ConfirmedExited,
                )
                .is_err()
        );

        let row = store.require_task(id).unwrap();
        assert_eq!(row.status(), ProcessStatus::Running);
        assert_eq!(row.exit_reason(), None);
        assert_eq!(
            row.process_group_exit_evidence(),
            ProcessGroupExitEvidence::Unconfirmed
        );
        assert_eq!(store.pending_outbound_events(id).unwrap().len(), 2);
    }

    #[test]
    fn queued_cancel_records_that_no_child_was_spawned() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("db");
        let id = TaskId::new();
        {
            let store = Store::open(&path).unwrap();
            store.insert_task(&agent_row(id)).unwrap();
            let CancelResult::CancelledQueued(row) = store.request_cancel(id).unwrap() else {
                panic!("queued task should cancel before a worker can spawn its child");
            };
            assert_eq!(
                row.process_group_exit_evidence(),
                ProcessGroupExitEvidence::NoChildSpawned
            );
        }

        let store = Store::open(&path).unwrap();
        assert_eq!(
            store.process_group_exit_evidence(id).unwrap(),
            Some(ProcessGroupExitEvidence::NoChildSpawned)
        );
    }

    #[test]
    fn released_exit_json_and_database_rows_remain_unconfirmed() {
        let directory = tempdir().unwrap();
        let exit_path = directory.path().join("exit.json");
        std::fs::write(&exit_path, r#"{"reason":{"kind":"exit","code":0}}"#).unwrap();
        let old_exit = read_exit_json(&exit_path).unwrap().unwrap();
        assert_eq!(old_exit.reason, ExitReason::Exit { code: 0 });
        assert_eq!(
            old_exit.process_group_exit_evidence,
            ProcessGroupExitEvidence::Unconfirmed
        );

        let path = directory.path().join("v2-db");
        let id = TaskId::new();
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(BASE_SCHEMA).unwrap();
            conn.pragma_update(None, "user_version", 2i64).unwrap();
            conn.execute(
                "INSERT INTO tasks (
                    id, thread_id, name, workload_json, cwd, timeout_secs,
                    env_path, env_home, binary, status, exit_reason,
                    callback_status, attention_state, timeout_notified_at,
                    pid, cancel_requested_at, created_at, updated_at
                ) VALUES (?1,?2,NULL,?3,'/tmp','14400','/bin','/home/u','/bin/echo',
                    'cancelled',?4,'pending','pending',NULL,NULL,NULL,?5,?5)",
                params![
                    id.to_string(),
                    "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
                    serde_json::to_string(&Workload::Task(TaskWorkload {
                        command: CommandLine::try_from_argv(vec!["echo".into(), "hi".into()])
                            .unwrap(),
                    }))
                    .unwrap(),
                    serde_json::to_string(&ExitReason::Cancelled).unwrap(),
                    "2024-01-01T00:00:00Z",
                ],
            )
            .unwrap();
        }

        let store = Store::open(&path).unwrap();
        let version: i64 = store
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        assert_resource_tables_installed(&store);
        let row = store.require_task(id).unwrap();
        assert_eq!(row.status(), ProcessStatus::Cancelled);
        assert_eq!(row.exit_reason(), Some(&ExitReason::Cancelled));
        assert_eq!(
            row.process_group_exit_evidence(),
            ProcessGroupExitEvidence::Unconfirmed
        );
        assert_eq!(
            store.process_group_exit_evidence(id).unwrap(),
            Some(ProcessGroupExitEvidence::Unconfirmed)
        );
    }

    #[test]
    fn exit_json_round_trips_typed_process_group_evidence() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("exit.json");
        write_exit_json_with_evidence(
            &path,
            &ExitReason::Cancelled,
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap();

        assert_eq!(
            read_exit_json(&path)
                .unwrap()
                .unwrap()
                .process_group_exit_evidence,
            ProcessGroupExitEvidence::ConfirmedExited
        );
    }

    fn deliver_outbound_events(
        store: &mut Store,
        id: TaskId,
        mut outcome_for_seq: impl FnMut(u64) -> DeliveryOutcome,
    ) {
        let mut seq = 1_u64;
        while let Some(outbox) = store
            .outbound_event_at_or_after(id, NonZeroU64::new(seq).unwrap())
            .unwrap()
        {
            assert_eq!(outbox.event.seq.get(), seq);
            store.accept_inbound_event(&outbox.event).unwrap();
            if outbox.notification_required {
                store
                    .reserve_inbox_attempt(id, outbox.event.seq)
                    .unwrap()
                    .expect("callback attempt reservation");
                store
                    .settle_inbox_attempt(id, outbox.event.seq, outcome_for_seq(seq))
                    .unwrap();
            }
            seq += 1;
        }
    }

    fn row_at(id: TaskId, cwd: &Path) -> (TaskRow, NormalizedSpec) {
        let mut row = task_row(id);
        row.name = Some(TaskName::parse("local task").unwrap());
        row.cwd = cwd.to_path_buf();
        let spec = local_spec(&row);
        (row, spec)
    }

    fn insert_local_at(store: &Store, id: TaskId, cwd: &Path, machine: MachineId) {
        let (row, spec) = row_at(id, cwd);
        store
            .insert_local_task(
                &row,
                &spec,
                machine,
                CallbackExecutable::available("/bin/true".into()),
            )
            .unwrap();
    }

    #[test]
    fn queued_resource_task_cannot_be_accepted_as_local_or_remote_work() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let task = TaskId::new();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let (row, spec) = row_at(task, Path::new("/tmp"));
        let resource = crate::resource::Resource::new(
            crate::resource::ResourceId::new(),
            "gpu-0".into(),
            authority,
            crate::resource::SupervisorAddress {
                machine: authority,
                thread: row.thread,
            },
            crate::resource::AssignmentRevision::new(0),
            crate::resource::ResourceRevision::new(0),
            None,
        );
        store.register_resource(authority, &resource).unwrap();
        store
            .accept_resource_request(
                authority,
                RequestId::new(),
                task,
                resource.id,
                origin,
                spec.clone(),
            )
            .unwrap();

        assert!(matches!(
            store.insert_local_task(
                &row,
                &spec,
                MachineId::new(),
                CallbackExecutable::available("/bin/true".into()),
            ),
            Err(AppError::ClusterTaskConflict { task: conflict }) if conflict == task
        ));
        assert!(matches!(
            store.insert_remote_task(&row, &spec, origin, authority),
            Err(AppError::ClusterTaskConflict { task: conflict }) if conflict == task
        ));
        assert!(store.get_task(task).unwrap().is_none());
        assert!(store.executor_identity(task).unwrap().is_none());
        assert_eq!(
            store
                .oldest_queued_resource_request(authority, resource.id)
                .unwrap()
                .unwrap()
                .task_id,
            task
        );

        let ordinary_task = TaskId::new();
        let (ordinary_row, ordinary_spec) = row_at(ordinary_task, Path::new("/tmp"));
        assert!(matches!(
            store.insert_remote_task(&ordinary_row, &ordinary_spec, origin, authority),
            Ok(ExecutorIdentity::Accepted(_))
        ));
        assert!(matches!(
            store.insert_remote_task(&ordinary_row, &ordinary_spec, origin, authority),
            Ok(ExecutorIdentity::Accepted(_))
        ));
    }

    fn insert_legacy_terminal(store: &Store, id: TaskId, callback: CallbackStatus) {
        let mut row = task_row(id);
        row.state = TaskState::Finished {
            reason: ExitReason::Exit { code: 0 },
        };
        row.callback_status = callback;
        store.insert_task(&row).unwrap();
    }

    #[test]
    fn legacy_pending_and_sending_callbacks_migrate_once_and_survive_restart() {
        let directory = tempdir().unwrap();
        let machine = MachineId::new();

        for callback_status in [CallbackStatus::Pending, CallbackStatus::Sending] {
            let id = TaskId::new();
            let db = directory
                .path()
                .join(format!("{}.sqlite", callback_status.as_str()));
            let mut store = Store::open(&db).unwrap();
            store.insert_task(&task_row(id)).unwrap();
            store
                .append_report_with_notification(id, ReportOutcome::Succeeded, "old report", false)
                .unwrap();
            store
                .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
                .unwrap();
            store
                .cas_exit(id, ProcessStatus::Running, &ExitReason::Exit { code: 0 })
                .unwrap();
            store
                .conn
                .execute(
                    "UPDATE tasks SET callback_status=?1 WHERE id=?2",
                    params![callback_status.as_str(), id.to_string()],
                )
                .unwrap();
            store.migrate_legacy_local(machine).unwrap();

            let route = store.origin_route_by_task(id).unwrap().unwrap();
            let request = route.request;
            assert_eq!(route.origin_machine, machine);
            assert_eq!(route.execution_machine, machine);
            assert!(matches!(route.spec, PersistedSpec::MigratedLocal));
            assert_eq!(route.submission, SubmissionState::Accepted);
            assert_eq!(route.last_accepted_seq, 1);
            assert_eq!(route.last_settled_seq, 0);
            assert_eq!(
                store.require_task(id).unwrap().callback_status,
                callback_status
            );
            assert_eq!(
                store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
                TerminalCallbackProjection::OriginInbox(CallbackStatus::Pending)
            );
            assert!(store.has_pending_terminal_callbacks().unwrap());

            let identity = store.executor_identity(id).unwrap().unwrap();
            assert!(matches!(
                identity,
                ExecutorIdentity::Accepted(record)
                    if record.spec.current().is_none()
                        && record.origin_machine == machine
                        && record.execution_machine == machine
            ));
            let outbox = store
                .outbound_event_at_or_after(id, NonZeroU64::MIN)
                .unwrap()
                .unwrap();
            assert_eq!(outbox.state, OutboxState::Acknowledged);
            assert!(outbox.notification_required);
            assert_eq!(outbox.event.seq.get(), 1);
            let EventPayload::Callback { event, state } = &outbox.event.payload else {
                panic!("legacy terminal callback payload")
            };
            assert_eq!(*state, Some(ProcessStatus::Succeeded));
            assert_eq!(event.reports.len(), 1);
            assert_eq!(event.reports[0].summary, "old report");

            let inbox = store.inbound_events(id).unwrap();
            assert_eq!(inbox.len(), 1);
            assert_eq!(inbox[0].event, outbox.event);
            assert_eq!(
                inbox[0].delivery,
                DeliveryState::PendingDelivery {
                    attempts: 0,
                    last_error: None,
                }
            );
            assert_eq!(store.pending_inbox_tasks().unwrap(), vec![id]);
            let cursor: i64 = store
                .conn
                .query_row(
                    "SELECT last_seq FROM executor_event_cursors WHERE task_id=?1",
                    [id.to_string()],
                    |entry| entry.get(0),
                )
                .unwrap();
            assert_eq!(cursor, 1);

            drop(store);
            let mut reopened = Store::open(&db).unwrap();
            reopened.migrate_legacy_local(machine).unwrap();
            let restarted = reopened.origin_route_by_task(id).unwrap().unwrap();
            assert_eq!(restarted.request, request);
            assert_eq!(restarted.last_accepted_seq, 1);
            assert_eq!(reopened.inbound_events(id).unwrap().len(), 1);
            assert_eq!(
                reopened.task_presentations(&[id]).unwrap()[&id].terminal_callback,
                TerminalCallbackProjection::OriginInbox(CallbackStatus::Pending)
            );
            assert!(reopened.has_pending_terminal_callbacks().unwrap());
            assert_eq!(
                reopened
                    .outbound_event_at_or_after(id, NonZeroU64::MIN)
                    .unwrap()
                    .unwrap()
                    .state,
                OutboxState::Acknowledged
            );
        }
    }

    #[test]
    fn legacy_sent_and_failed_callbacks_do_not_create_events() {
        let directory = tempdir().unwrap();
        let machine = MachineId::new();

        for status in [CallbackStatus::Sent, CallbackStatus::Failed] {
            let mut store =
                Store::open(&directory.path().join(format!("{}.sqlite", status.as_str()))).unwrap();
            let id = TaskId::new();
            insert_legacy_terminal(&store, id, status);
            store.migrate_legacy_local(machine).unwrap();

            let route = store.origin_route_by_task(id).unwrap().unwrap();
            assert_eq!(route.last_accepted_seq, 0);
            assert_eq!(route.last_settled_seq, 0);
            assert!(store.inbound_events(id).unwrap().is_empty());
            assert!(
                store
                    .outbound_event_at_or_after(id, NonZeroU64::MIN)
                    .unwrap()
                    .is_none()
            );
            let cursor: Option<i64> = store
                .conn
                .query_row(
                    "SELECT last_seq FROM executor_event_cursors WHERE task_id=?1",
                    [id.to_string()],
                    |entry| entry.get(0),
                )
                .optional()
                .unwrap();
            assert_eq!(cursor, None);
            assert_eq!(
                store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
                TerminalCallbackProjection::Legacy(status)
            );
            let summary = TaskSummary::from_row(
                &store.require_task(id).unwrap(),
                Some(&store.task_presentations(&[id]).unwrap()[&id]),
            );
            assert_eq!(
                serde_json::to_value(summary).unwrap()["callback"],
                status.as_str()
            );
        }
    }

    #[test]
    fn legacy_nonterminal_migration_leaves_sequence_for_first_real_transition() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let machine = MachineId::new();
        let id = TaskId::new();
        store.insert_task(&task_row(id)).unwrap();

        store.migrate_legacy_local(machine).unwrap();
        let migrated = store.origin_route_by_task(id).unwrap().unwrap();
        assert_eq!(migrated.last_accepted_seq, 0);
        assert_eq!(migrated.last_settled_seq, 0);
        assert!(store.inbound_events(id).unwrap().is_empty());
        assert!(
            store
                .outbound_event_at_or_after(id, NonZeroU64::MIN)
                .unwrap()
                .is_none()
        );

        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        let first = store
            .outbound_event_at_or_after(id, NonZeroU64::MIN)
            .unwrap()
            .unwrap();
        assert_eq!(first.event.seq.get(), 1);
        assert_eq!(
            first.event.payload,
            EventPayload::State {
                status: ProcessStatus::Running
            }
        );
    }

    #[test]
    fn legacy_migration_skips_remote_executors_and_rejects_a_partial_local_route() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let machine = MachineId::new();

        let remote_id = TaskId::new();
        let (remote_row, remote_spec) = row_at(remote_id, Path::new("/tmp"));
        let origin = MachineId::new();
        store
            .insert_remote_task(&remote_row, &remote_spec, origin, machine)
            .unwrap();
        store.migrate_legacy_local(machine).unwrap();
        assert!(store.origin_route_by_task(remote_id).unwrap().is_none());
        assert!(matches!(
            store.executor_identity(remote_id).unwrap(),
            Some(ExecutorIdentity::Accepted(record))
                if record.origin_machine == origin
                    && record.execution_machine == machine
                    && record.current_spec().is_some()
        ));

        let legacy_id = TaskId(uuid::Uuid::from_u128(1));
        let partial_id = TaskId(uuid::Uuid::from_u128(2));
        store.insert_task(&task_row(legacy_id)).unwrap();
        let row = task_row(partial_id);
        store.insert_task(&row).unwrap();
        let partial_route = OriginRoute {
            request: RequestId::new(),
            task: partial_id,
            origin_machine: machine,
            execution_machine: machine,
            thread: row.thread,
            callback: CallbackContext {
                env: row.env.clone(),
                cwd: row.cwd.clone(),
                codex: CallbackExecutable::available(Path::new("/bin/true").into()),
            },
            spec: local_spec(&row).into(),
            submission: SubmissionState::Accepted,
            last_execution_state: Some(ProcessStatus::Queued),
            last_updated_at: Some(Utc::now()),
            last_accepted_seq: 0,
            last_settled_seq: 0,
        };
        store.insert_origin_route(&partial_route).unwrap();
        assert!(store.migrate_legacy_local(machine).is_err());
        assert!(store.executor_identity(legacy_id).unwrap().is_none());
        assert!(store.origin_route_by_task(legacy_id).unwrap().is_none());
        assert!(store.executor_identity(partial_id).unwrap().is_none());
        assert_eq!(
            store
                .origin_route_by_task(partial_id)
                .unwrap()
                .unwrap()
                .request,
            partial_route.request
        );
        assert!(store.inbound_events(partial_id).unwrap().is_empty());
    }

    #[test]
    fn missing_legacy_callback_directory_does_not_abort_migration() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let machine = MachineId::new();
        let id = TaskId::new();
        let missing_cwd = directory.path().join("removed-cwd");
        let mut row = task_row(id);
        row.cwd = missing_cwd.clone();
        row.state = TaskState::Finished {
            reason: ExitReason::Exit { code: 0 },
        };
        store.insert_task(&row).unwrap();

        store.migrate_legacy_local(machine).unwrap();
        let route = store.origin_route_by_task(id).unwrap().unwrap();
        assert_eq!(route.callback.cwd, missing_cwd);
        let error = crate::callback::check_saved_callback(&route.callback).unwrap_err();
        assert!(error.contains("saved callback directory is unavailable"));

        let reserved = store
            .reserve_inbox_attempt(id, NonZeroU64::MIN)
            .unwrap()
            .unwrap();
        assert!(matches!(
            reserved.delivery,
            DeliveryState::PendingDelivery { attempts: 1, .. }
        ));
        let failed = store
            .settle_inbox_attempt(id, NonZeroU64::MIN, DeliveryOutcome::Permanent(error))
            .unwrap();
        assert!(matches!(
            failed.delivery,
            DeliveryState::DeliveryFailed { attempts: 1, .. }
        ));
        assert_eq!(
            store
                .origin_route_by_task(id)
                .unwrap()
                .unwrap()
                .last_settled_seq,
            1
        );
    }

    #[test]
    fn unavailable_legacy_codex_is_durable_and_fails_inbox_delivery() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let machine = MachineId::new();
        let id = TaskId::new();
        let empty_path = directory.path().join("empty-bin");
        std::fs::create_dir(&empty_path).unwrap();
        let mut row = task_row(id);
        row.cwd = directory.path().to_path_buf();
        row.env.path = empty_path.to_string_lossy().into_owned();
        row.state = TaskState::Finished {
            reason: ExitReason::Exit { code: 0 },
        };
        store.insert_task(&row).unwrap();

        store
            .migrate_legacy_local_with(machine, |path, cwd| {
                assert_eq!(path, empty_path.to_string_lossy());
                assert!(cwd.is_dir());
                Err(AppError::ExecutableMissing {
                    program: "codex".into(),
                })
            })
            .unwrap();

        let route = store.origin_route_by_task(id).unwrap().unwrap();
        let CallbackExecutable::Unavailable { reason } = &route.callback.codex else {
            panic!("unresolved Codex binary must remain explicitly unavailable")
        };
        assert!(reason.contains("saved Codex executable could not be resolved"));
        let error = crate::callback::check_saved_callback(&route.callback).unwrap_err();
        assert!(error.contains("codex"));

        let reserved = store
            .reserve_inbox_attempt(id, NonZeroU64::MIN)
            .unwrap()
            .unwrap();
        assert!(matches!(
            reserved.delivery,
            DeliveryState::PendingDelivery { attempts: 1, .. }
        ));
        let failed = store
            .settle_inbox_attempt(id, NonZeroU64::MIN, DeliveryOutcome::Permanent(error))
            .unwrap();
        assert!(matches!(
            failed.delivery,
            DeliveryState::DeliveryFailed { attempts: 1, .. }
        ));
        assert_eq!(
            store
                .origin_route_by_task(id)
                .unwrap()
                .unwrap()
                .last_settled_seq,
            1
        );
    }

    #[test]
    fn legacy_migration_serializes_with_runner_completion_and_produces_one_first_event() {
        use std::sync::{Arc, Barrier};

        let directory = tempdir().unwrap();
        let db = directory.path().join("db");
        let id = TaskId::new();
        let machine = MachineId::new();
        let store = Store::open(&db).unwrap();
        store.insert_task(&task_row(id)).unwrap();
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        drop(store);

        let barrier = Arc::new(Barrier::new(3));
        let migration_db = db.clone();
        let migration_barrier = barrier.clone();
        let migration = std::thread::spawn(move || {
            let mut store = Store::open(&migration_db).unwrap();
            migration_barrier.wait();
            store.migrate_legacy_local(machine).unwrap();
        });
        let completion_db = db.clone();
        let completion_barrier = barrier.clone();
        let completion = std::thread::spawn(move || {
            let store = Store::open(&completion_db).unwrap();
            completion_barrier.wait();
            store
                .cas_exit(id, ProcessStatus::Running, &ExitReason::Exit { code: 0 })
                .unwrap();
        });
        barrier.wait();
        migration.join().unwrap();
        completion.join().unwrap();

        let store = Store::open(&db).unwrap();
        let outbox: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                [id.to_string()],
                |entry| entry.get(0),
            )
            .unwrap();
        assert_eq!(outbox, 1);
        let event = store
            .outbound_event_at_or_after(id, NonZeroU64::MIN)
            .unwrap()
            .unwrap();
        assert_eq!(event.event.seq.get(), 1);
        assert!(matches!(
            event.event.payload,
            EventPayload::Callback {
                state: Some(ProcessStatus::Succeeded),
                ..
            }
        ));
        assert_eq!(
            store.require_task(id).unwrap().status(),
            ProcessStatus::Succeeded
        );
        assert!(matches!(
            store.executor_identity(id).unwrap(),
            Some(ExecutorIdentity::Accepted(record))
                if record.state == ProcessStatus::Succeeded
                    && record.has_valid_spec_owners()
        ));
    }

    #[test]
    fn project_metadata_uses_the_nearest_nested_git_root() {
        let dir = tempdir().unwrap();
        let repository = dir.path().join("project");
        let cwd = repository.join("packages/app/src");
        std::fs::create_dir_all(cwd.as_path()).unwrap();
        std::fs::create_dir(repository.join(".git")).unwrap();

        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        insert_local_at(&store, id, &cwd, MachineId::new());

        let presentation = store
            .task_presentations(&[id])
            .unwrap()
            .remove(&id)
            .unwrap();
        assert_eq!(
            presentation.project_root.as_deref(),
            Some(repository.as_path())
        );
    }

    #[test]
    fn project_metadata_recognizes_linked_worktree_git_files() {
        let dir = tempdir().unwrap();
        let worktree = dir.path().join("worktree");
        let cwd = worktree.join("nested");
        std::fs::create_dir_all(&cwd).unwrap();
        std::fs::write(
            worktree.join(".git"),
            "gitdir: /some/repository/worktrees/topic\n",
        )
        .unwrap();

        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        insert_local_at(&store, id, &cwd, MachineId::new());

        let presentation = store
            .task_presentations(&[id])
            .unwrap()
            .remove(&id)
            .unwrap();
        assert_eq!(
            presentation.project_root.as_deref(),
            Some(worktree.as_path())
        );
    }

    #[test]
    fn project_metadata_keeps_cwd_fallback_and_serializes_accepted_owners() {
        let dir = tempdir().unwrap();
        let local_cwd = dir.path().join("standalone");
        let remote_cwd = dir.path().join("remote-project/nested");
        std::fs::create_dir_all(&local_cwd).unwrap();
        std::fs::create_dir_all(&remote_cwd).unwrap();
        std::fs::create_dir(remote_cwd.parent().unwrap().join(".git")).unwrap();

        let store = Store::open(&dir.path().join("db")).unwrap();
        let local_id = TaskId::new();
        let local_machine = MachineId::new();
        insert_local_at(&store, local_id, &local_cwd, local_machine);

        let remote_id = TaskId::new();
        let origin_machine = MachineId::new();
        let execution_machine = MachineId::new();
        let (mut remote_row, remote_spec) = row_at(remote_id, &remote_cwd);
        remote_row.callback_status = CallbackStatus::Sent;
        store
            .insert_remote_task(&remote_row, &remote_spec, origin_machine, execution_machine)
            .unwrap();

        let legacy_id = TaskId::new();
        let legacy_row = task_row(legacy_id);
        store.insert_task(&legacy_row).unwrap();

        let presentations = store
            .task_presentations(&[local_id, remote_id, legacy_id])
            .unwrap();
        let local = presentations.get(&local_id).unwrap();
        assert_eq!(local.project_root, None);
        assert_eq!(local.owners.unwrap().origin_machine, local_machine);
        assert_eq!(local.owners.unwrap().execution_machine, local_machine);
        let local_json = serde_json::to_value(TaskSummary::from_row(
            &store.require_task(local_id).unwrap(),
            Some(local),
        ))
        .unwrap();
        assert!(local_json.get("project_root").is_none());
        assert_eq!(local_json["cwd"], json!(local_cwd));
        assert_eq!(local_json["origin_machine"], json!(local_machine));
        assert_eq!(local_json["execution_machine"], json!(local_machine));

        let remote = presentations.get(&remote_id).unwrap();
        assert_eq!(remote.project_root.as_deref(), remote_cwd.parent());
        assert_eq!(remote.owners.unwrap().origin_machine, origin_machine);
        assert_eq!(remote.owners.unwrap().execution_machine, execution_machine);
        let remote_json = serde_json::to_value(TaskSummary::from_row(
            &store.require_task(remote_id).unwrap(),
            Some(remote),
        ))
        .unwrap();
        assert_eq!(remote_json["project_root"], json!(remote_cwd.parent()));
        assert_eq!(remote_json["origin_machine"], json!(origin_machine));
        assert_eq!(remote_json["execution_machine"], json!(execution_machine));
        assert_eq!(remote_json["callback"], "pending");

        let legacy = presentations.get(&legacy_id).unwrap();
        assert_eq!(legacy.owners, None);
        let legacy_json =
            serde_json::to_value(TaskSummary::from_row(&legacy_row, Some(legacy))).unwrap();
        assert!(legacy_json.get("origin_machine").is_none());
        assert!(legacy_json.get("execution_machine").is_none());
    }

    #[test]
    fn project_metadata_is_not_recomputed_after_acceptance() {
        let dir = tempdir().unwrap();
        let repository = dir.path().join("project");
        let cwd = repository.join("nested");
        std::fs::create_dir_all(&cwd).unwrap();
        std::fs::create_dir(repository.join(".git")).unwrap();

        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        insert_local_at(&store, id, &cwd, MachineId::new());
        let moved_repository = dir.path().join("moved-project");
        std::fs::rename(&repository, &moved_repository).unwrap();

        let presentation = store
            .task_presentations(&[id])
            .unwrap()
            .remove(&id)
            .unwrap();
        assert_eq!(
            presentation.project_root.as_deref(),
            Some(repository.as_path())
        );
        assert!(!repository.exists());
        assert!(moved_repository.exists());
    }

    #[test]
    fn remote_acceptance_rolls_back_with_event_and_spawn_failure_retains_state() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        let origin = MachineId::new();
        let execution = MachineId::new();
        let mut row = task_row(id);
        row.name = Some(TaskName::parse("local task").unwrap());
        let spec = local_spec(&row);
        row.workload = crate::invocation::persist_workload(&spec.workload);
        row.binary = Path::new("/bin/true").into();
        store.conn.execute_batch("CREATE TRIGGER reject_outbox BEFORE INSERT ON executor_outbox BEGIN SELECT RAISE(ABORT, 'event insert failed'); END;").unwrap();
        assert!(
            store
                .insert_remote_task(&row, &spec, origin, execution)
                .is_err()
        );
        assert!(store.get_task(id).unwrap().is_none());
        assert!(store.executor_identity(id).unwrap().is_none());
        store
            .conn
            .execute_batch("DROP TRIGGER reject_outbox")
            .unwrap();

        let accepted = store
            .insert_remote_task(&row, &spec, origin, execution)
            .unwrap();
        assert!(matches!(accepted, ExecutorIdentity::Accepted(_)));
        assert!(store.is_event_task(id).unwrap());
        store
            .cas_exit(
                id,
                ProcessStatus::Queued,
                &ExitReason::SpawnFailed {
                    message: "failed to fork".into(),
                },
            )
            .unwrap();
        assert!(
            matches!(store.executor_identity(id).unwrap(), Some(ExecutorIdentity::Accepted(record))
            if record.state == ProcessStatus::Failed)
        );
        let outbox_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(outbox_count, 2);
        assert!(!store.has_pending_terminal_callbacks().unwrap());
    }

    #[test]
    fn local_producer_sequences_reports_and_terminal_state() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db");
        let store = Store::open(&db).unwrap();
        let id = TaskId::new();
        insert_local(&store, id);
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        store
            .append_report_with_notification(id, ReportOutcome::Blocked, "silent", false)
            .unwrap();
        store
            .append_report_with_notification(id, ReportOutcome::Succeeded, "notify", true)
            .unwrap();
        store
            .cas_exit(id, ProcessStatus::Running, &ExitReason::Exit { code: 0 })
            .unwrap();
        let events = store.pending_outbound_events(id).unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.event.seq.get())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(
            events
                .iter()
                .map(|event| event.notification_required)
                .collect::<Vec<_>>(),
            vec![false, false, false, true, true]
        );
        let EventPayload::Report { report } = &events[2].event.payload else {
            panic!("silent report payload")
        };
        assert_eq!(report.summary, "silent");
        let EventPayload::Callback { event, .. } = &events[3].event.payload else {
            panic!("notify payload")
        };
        assert_eq!(event.reports.len(), 1);
        assert_eq!(event.reports[0].summary, "notify");
        let EventPayload::Callback { event, state } = &events[4].event.payload else {
            panic!("terminal payload")
        };
        assert_eq!(*state, Some(ProcessStatus::Succeeded));
        assert_eq!(event.reports.len(), 2);
        assert_eq!(event.reports[0].summary, "silent");
        assert_eq!(event.reports[1].summary, "notify");
        drop(store);
        let reopened = Store::open(&db).unwrap();
        assert_eq!(reopened.pending_outbound_events(id).unwrap().len(), 5);
        assert!(reopened.has_pending_terminal_callbacks().unwrap());
    }

    #[test]
    fn local_producer_rolls_back_state_and_report_when_event_insert_fails() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        insert_local(&store, id);
        store.conn.execute_batch("CREATE TRIGGER reject_outbox BEFORE INSERT ON executor_outbox BEGIN SELECT RAISE(ABORT, 'event insert failed'); END;").unwrap();
        assert!(
            store
                .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
                .is_err()
        );
        assert_eq!(
            store.require_task(id).unwrap().status(),
            ProcessStatus::Queued
        );
        assert!(
            store
                .append_report_with_notification(id, ReportOutcome::Succeeded, "body", true)
                .is_err()
        );
        assert!(store.reports(id).unwrap().is_empty());
        assert_eq!(store.pending_outbound_events(id).unwrap().len(), 1);
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                    [id.to_string()],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .executor_identity(id)
                .unwrap()
                .and_then(|identity| match identity {
                    ExecutorIdentity::Accepted(row) => Some(row.state),
                    ExecutorIdentity::Rejected(_) => None,
                }),
            Some(ProcessStatus::Queued)
        );
    }

    #[test]
    fn local_acceptance_rejects_mismatched_task_without_partial_insert() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        let row = task_row(id);
        let spec = local_spec(&row);
        assert!(
            store
                .insert_local_task(
                    &row,
                    &spec,
                    MachineId::new(),
                    CallbackExecutable::available("/bin/true".into())
                )
                .is_err()
        );
        assert!(store.get_task(id).unwrap().is_none());
        assert!(store.origin_route_by_task(id).unwrap().is_none());
        assert!(store.executor_identity(id).unwrap().is_none());
    }

    #[test]
    fn queued_cancel_and_runner_loss_each_keep_one_terminal_event() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let cancelled = TaskId::new();
        insert_local(&store, cancelled);
        assert!(matches!(
            store.request_cancel(cancelled).unwrap(),
            CancelResult::CancelledQueued(_)
        ));
        assert_eq!(store.pending_outbound_events(cancelled).unwrap().len(), 2);
        assert!(matches!(
            store.request_cancel(cancelled).unwrap(),
            CancelResult::AlreadyTerminal(_)
        ));
        assert_eq!(store.pending_outbound_events(cancelled).unwrap().len(), 2);
        let lost = TaskId::new();
        insert_local(&store, lost);
        store
            .cas_status(lost, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        store
            .cas_status(lost, ProcessStatus::Running, ProcessStatus::Lost)
            .unwrap();
        let events = store.pending_outbound_events(lost).unwrap();
        assert_eq!(events.len(), 3);
        let EventPayload::Callback { event, .. } = &events[2].event.payload else {
            panic!("loss payload")
        };
        assert_eq!(event.event, EventKind::TaskLost);
        let spawn_failed = TaskId::new();
        insert_local(&store, spawn_failed);
        store
            .cas_exit(
                spawn_failed,
                ProcessStatus::Queued,
                &ExitReason::SpawnFailed {
                    message: "no runner".into(),
                },
            )
            .unwrap();
        let events = store.pending_outbound_events(spawn_failed).unwrap();
        assert_eq!(events.len(), 2);
        let EventPayload::Callback { event, state } = &events[1].event.payload else {
            panic!("spawn failure payload")
        };
        assert_eq!(*state, Some(ProcessStatus::Failed));
        assert_eq!(event.event, EventKind::TaskFailed);
    }

    #[test]
    fn inactivity_reminder_is_one_event_with_current_reports() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        insert_local(&store, id);
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        store
            .append_report_with_notification(id, ReportOutcome::Blocked, "waiting", false)
            .unwrap();
        assert!(store.produce_attention_event(id).unwrap());
        assert!(!store.produce_attention_event(id).unwrap());
        let events = store.pending_outbound_events(id).unwrap();
        assert_eq!(events.len(), 4);
        let EventPayload::Callback { event, state } = &events[3].event.payload else {
            panic!("reminder payload")
        };
        assert_eq!(event.event, EventKind::TaskCheckDue);
        assert_eq!(event.reports[0].summary, "waiting");
        assert_eq!(*state, None);
    }

    #[test]
    fn legacy_task_keeps_its_callback_after_reopen() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db");
        let id = TaskId::new();
        let store = Store::open(&db).unwrap();
        store.insert_task(&agent_row(id)).unwrap();
        drop(store);
        let store = Store::open(&db).unwrap();
        assert!(!store.is_event_task(id).unwrap());
        store
            .cas_exit(id, ProcessStatus::Queued, &ExitReason::Cancelled)
            .unwrap();
        let mut store = store;
        store.migrate_legacy_local(MachineId::new()).unwrap();
        assert!(store.has_pending_terminal_callbacks().unwrap());
        assert_eq!(store.inbound_events(id).unwrap().len(), 1);
        assert!(store.pending_outbound_events(id).unwrap().is_empty());
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                    [id.to_string()],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    /// Schema version 1 layout used only to prove the 1→2 migration
    const SCHEMA_V1: &str = r"
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    workload_json TEXT NOT NULL,
    cwd TEXT NOT NULL,
    timeout_secs TEXT NOT NULL,
    env_path TEXT NOT NULL,
    env_home TEXT NOT NULL,
    binary TEXT NOT NULL,
    status TEXT NOT NULL,
    exit_reason TEXT,
    callback_status TEXT NOT NULL,
    attention_state TEXT NOT NULL,
    timeout_notified_at TEXT,
    pid INTEGER,
    cancel_requested_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX tasks_status ON tasks(status);
CREATE INDEX tasks_thread ON tasks(thread_id);
CREATE TABLE reports (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    summary TEXT NOT NULL,
    reported_at TEXT NOT NULL,
    notified_at TEXT,
    PRIMARY KEY (task_id, seq),
    FOREIGN KEY (task_id) REFERENCES tasks(id)
);
";

    #[test]
    fn unreleased_development_schema_is_refused() {
        for version in [3, RELEASED_V0_4_SCHEMA_VERSION - 1, SCHEMA_VERSION + 1] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("db");
            Connection::open(&path)
                .unwrap()
                .pragma_update(None, "user_version", version)
                .unwrap();
            let Err(AppError::Internal { message }) = Store::open(&path) else {
                panic!("schema user_version={version} must be refused");
            };
            assert!(
                message.contains(&format!("user_version={version}")),
                "{message}"
            );
            let unreleased = version < RELEASED_V0_4_SCHEMA_VERSION;
            assert_eq!(message.contains("unreleased"), unreleased, "{message}");
        }
    }

    #[test]
    fn migrate_from_empty() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let version: i64 = store
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        assert_resource_tables_installed(&store);
        assert_foreign_keys_enabled(&store);
    }

    fn assert_resource_tables_installed(store: &Store) {
        for table in [
            "resources",
            "trainer_attempt_associations",
            "resource_requests",
            "resource_request_preventions",
            "resource_cancellation_receipts",
            "loans",
            "resource_supervisor_notices",
            "resource_release_completions",
            "resource_release_checkpoint_states",
            "resource_return_decisions",
            "resource_restore_closures",
            "resource_action_task_receipts",
            "resource_background_launches",
            "resource_idle_openings",
            "resource_registration_receipts",
        ] {
            let exists: bool = store
                .conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "resource table {table} was not installed");
        }

        let resource_primary_key: i64 = store
            .conn
            .query_row(
                "SELECT pk FROM pragma_table_info('trainer_attempt_associations')
                 WHERE name='resource_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let task_primary_key: i64 = store
            .conn
            .query_row(
                "SELECT pk FROM pragma_table_info('trainer_attempt_associations')
                 WHERE name='task_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(resource_primary_key, 0);
        assert_eq!(task_primary_key, 1);
    }

    fn assert_foreign_keys_enabled(store: &Store) {
        let enabled: bool = store
            .conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .unwrap();
        assert!(enabled);
    }

    /// Schema text of one table or index
    fn schema_sql(store: &Store, name: &str) -> String {
        store
            .conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name = ?1",
                [name],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn released_v0_5_database_gains_the_container_witness_and_keeps_its_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let id = TaskId::new();
        {
            let store = Store::open(&path).unwrap();
            insert_local(&store, id);
            // restore the v0.5.0 shape: no container column or table, and a
            // request CHECK that accepted only command work
            store
                .conn
                .execute_batch(&format!(
                    "ALTER TABLE tasks DROP COLUMN container_exit_evidence;
                     DROP TABLE task_containers;
                     DROP TABLE resource_requests;
                     CREATE TABLE resource_requests (
                         acceptance_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                         request_id TEXT NOT NULL UNIQUE,
                         task_id TEXT NOT NULL UNIQUE,
                         resource_id TEXT NOT NULL REFERENCES resources(id),
                         origin_machine TEXT NOT NULL,
                         spec_json TEXT NOT NULL CHECK (
                             COALESCE(json_extract(spec_json, '$.workload.type') = 'task', 0)
                         ),
                         state_json TEXT NOT NULL
                     );
                     PRAGMA user_version = {RELEASED_V0_5_SCHEMA_VERSION};"
                ))
                .unwrap();
        }

        let store = Store::open(&path).unwrap();
        let version: i64 = store
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let row = store.require_task(id).unwrap();
        assert_eq!(
            row.container_exit_evidence,
            ContainerExitEvidence::Unconfirmed
        );
        assert_eq!(store.task_container(id).unwrap(), None);
        assert!(schema_sql(&store, "resource_requests").contains("'container'"));
        assert!(!schema_sql(&store, "resource_requests").contains("'task', 0) AND"));
        assert!(schema_sql(&store, "resource_restore_closures").contains("'container_ended'"));
        schema_sql(&store, "resource_requests_fifo");
        schema_sql(&store, "resource_requests_queued_fifo");
        assert_resource_tables_installed(&store);
        assert_foreign_keys_enabled(&store);
    }

    #[test]
    fn container_evidence_is_refused_where_no_container_path_records_it() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        insert_local(&store, id);
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();
        let confirmed = |exit_code| TaskExitEvidence {
            process_group: ProcessGroupExitEvidence::Unconfirmed,
            container: ContainerExitEvidence::Confirmed {
                container_id: ContainerId::parse(&"a".repeat(64)).unwrap(),
                exit_code,
            },
        };
        // a command task has no container, so container evidence cannot be its witness
        assert!(
            store
                .cas_exit_with_evidence(
                    id,
                    ProcessStatus::Running,
                    &ExitReason::Exit { code: 0 },
                    confirmed(0),
                )
                .is_err()
        );
        // the confirmed exit code must be the task's exit code
        assert!(
            store
                .cas_exit_with_evidence(
                    id,
                    ProcessStatus::Running,
                    &ExitReason::Exit { code: 0 },
                    confirmed(1),
                )
                .is_err()
        );
        let never_started = TaskExitEvidence {
            process_group: ProcessGroupExitEvidence::Unconfirmed,
            container: ContainerExitEvidence::NeverStarted,
        };
        assert!(
            store
                .cas_exit_with_evidence(
                    id,
                    ProcessStatus::Running,
                    &ExitReason::Exit { code: 0 },
                    never_started,
                )
                .is_err(),
            "a container that never started cannot exit with a code"
        );
        assert_eq!(
            store.require_task(id).unwrap().status(),
            ProcessStatus::Running
        );
    }

    #[test]
    fn migrate_version_1_preserves_tasks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(SCHEMA_V1).unwrap();
            conn.pragma_update(None, "user_version", 1i64).unwrap();
            let id = TaskId::new();
            conn.execute(
                "INSERT INTO tasks (
                    id, thread_id, workload_json, cwd, timeout_secs,
                    env_path, env_home, binary, status, exit_reason,
                    callback_status, attention_state, timeout_notified_at,
                    pid, cancel_requested_at, created_at, updated_at
                ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'queued',NULL,'pending','pending',NULL,NULL,NULL,?9,?9)",
                params![
                    id.to_string(),
                    "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
                    serde_json::to_string(&Workload::Task(TaskWorkload {
                        command: CommandLine::try_from_argv(vec!["echo".into(), "hi".into()])
                            .unwrap(),
                    }))
                    .unwrap(),
                    "/tmp",
                    "14400",
                    "/bin",
                    "/home/u",
                    "/bin/echo",
                    "2024-01-01T00:00:00Z",
                ],
            )
            .unwrap();
        }
        let store = Store::open(&path).unwrap();
        let version: i64 = store
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let listed = store.list_tasks(&[], None).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, None);
        assert_eq!(listed[0].display_name(), "echo hi");
        let id = listed[0].id;
        assert_resource_tables_installed(&store);
        assert!(!store.is_event_task(id).unwrap());
        store
            .cas_exit(id, ProcessStatus::Queued, &ExitReason::Cancelled)
            .unwrap();
        let mut store = store;
        store.migrate_legacy_local(MachineId::new()).unwrap();
        assert!(store.has_pending_terminal_callbacks().unwrap());
        assert_eq!(store.inbound_events(id).unwrap().len(), 1);
        assert!(store.pending_outbound_events(id).unwrap().is_empty());
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                    [id.to_string()],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn insert_list_get_agent_and_task() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let agent_id = TaskId::new();
        let task_id = TaskId::new();
        store.insert_task(&agent_row(agent_id)).unwrap();
        store.insert_task(&task_row(task_id)).unwrap();
        let got = store.require_task(agent_id).unwrap();
        assert!(matches!(got.workload, Workload::Agent(_)));
        assert_eq!(got.name.as_ref().map(TaskName::as_str), Some("agent job"));
        assert_eq!(got.display_name(), "agent job");
        let got = store.require_task(task_id).unwrap();
        assert!(matches!(got.workload, Workload::Task(_)));
        assert_eq!(got.name, None);
        assert_eq!(got.display_name(), "cargo build --release");
        let listed = store.list_tasks(&[ProcessStatus::Queued], None).unwrap();
        assert_eq!(listed.len(), 2);
    }

    #[test]
    fn cas_rejects_lost_to_running() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        assert!(
            store
                .cas_status(id, ProcessStatus::Queued, ProcessStatus::Lost)
                .unwrap()
                .is_some()
        );
        let err = store
            .cas_status(id, ProcessStatus::Lost, ProcessStatus::Running)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            TransitionError::LostToRunning.to_string(),
            "CAS must surface the transition rule, not a generic failure"
        );
    }

    #[test]
    fn fleet_disabled_local_notify_and_terminal_callback_use_the_durable_inbox() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let mut store = Store::open(&path).unwrap();
        let id = TaskId::new();
        insert_local(&store, id);
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        store
            .append_report_with_notification(id, ReportOutcome::Succeeded, "interim", true)
            .unwrap();
        store
            .cas_exit(id, ProcessStatus::Running, &ExitReason::Exit { code: 0 })
            .unwrap();

        let outbox = store.pending_outbound_events(id).unwrap();
        assert_eq!(outbox.len(), 4);
        assert!(outbox[2].notification_required);
        assert!(outbox[3].notification_required);
        assert_eq!(
            store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
            TerminalCallbackProjection::OriginInbox(CallbackStatus::Pending)
        );

        deliver_outbound_events(&mut store, id, |_| DeliveryOutcome::Delivered);

        assert_eq!(
            store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
            TerminalCallbackProjection::OriginInbox(CallbackStatus::Sent)
        );
        assert!(store.reports(id).unwrap()[0].notified_at.is_some());
        assert!(!store.has_pending_terminal_callbacks().unwrap());
        assert_eq!(
            serde_json::to_value(TaskSummary::from_row(
                &store.require_task(id).unwrap(),
                Some(&store.task_presentations(&[id]).unwrap()[&id]),
            ))
            .unwrap()["callback"],
            "sent"
        );

        for seq in 1..=4 {
            store
                .mark_outbound_acknowledged(id, NonZeroU64::new(seq).unwrap())
                .unwrap();
        }
        store
            .conn
            .execute_batch(
                "UPDATE executor_outbox
                 SET acknowledged_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 days');
                 UPDATE origin_inbox
                 SET settled_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 days');",
            )
            .unwrap();
        assert_eq!(store.compact_old_event_payloads().unwrap().compacted, 8);
        let route = store.origin_route_by_task(id).unwrap().unwrap();
        assert_eq!(route.last_accepted_seq, 4);
        assert_eq!(route.last_settled_seq, 4);
        assert_eq!(
            store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
            TerminalCallbackProjection::OriginInbox(CallbackStatus::Sent)
        );
        drop(store);

        let reopened = Store::open(&path).unwrap();
        assert_eq!(
            reopened.task_presentations(&[id]).unwrap()[&id].terminal_callback,
            TerminalCallbackProjection::OriginInbox(CallbackStatus::Sent)
        );
        assert!(!reopened.has_pending_terminal_callbacks().unwrap());
    }

    #[test]
    fn interim_failure_stays_visible_after_terminal_success_and_terminal_failure_is_projected() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let interim_failure_id = TaskId::new();
        insert_local(&store, interim_failure_id);
        store
            .cas_status(
                interim_failure_id,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap();
        store
            .append_report_with_notification(
                interim_failure_id,
                ReportOutcome::Blocked,
                "interim failure",
                true,
            )
            .unwrap();
        store
            .cas_exit(
                interim_failure_id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
            )
            .unwrap();
        deliver_outbound_events(&mut store, interim_failure_id, |seq| {
            if seq == 3 {
                DeliveryOutcome::Permanent("interim callback failed".into())
            } else {
                DeliveryOutcome::Delivered
            }
        });

        assert_eq!(
            store.task_presentations(&[interim_failure_id]).unwrap()[&interim_failure_id]
                .terminal_callback,
            TerminalCallbackProjection::OriginInbox(CallbackStatus::Sent)
        );
        let failed = store.failed_inbox_events(interim_failure_id).unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].seq, 3);
        assert_eq!(failed[0].error, "interim callback failed");

        let terminal_failure_id = TaskId::new();
        insert_local(&store, terminal_failure_id);
        store
            .cas_exit(
                terminal_failure_id,
                ProcessStatus::Queued,
                &ExitReason::Cancelled,
            )
            .unwrap();
        deliver_outbound_events(&mut store, terminal_failure_id, |_| {
            DeliveryOutcome::Permanent("terminal callback failed".into())
        });
        assert_eq!(
            store.task_presentations(&[terminal_failure_id]).unwrap()[&terminal_failure_id]
                .terminal_callback,
            TerminalCallbackProjection::OriginInbox(CallbackStatus::Failed)
        );
        assert_eq!(
            serde_json::to_value(TaskSummary::from_row(
                &store.require_task(terminal_failure_id).unwrap(),
                Some(
                    &store.task_presentations(&[terminal_failure_id]).unwrap()
                        [&terminal_failure_id]
                ),
            ))
            .unwrap()["callback"],
            "failed"
        );
    }

    #[test]
    fn terminal_event_follows_one_durable_inactivity_event() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        insert_local(&store, id);
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        assert!(store.produce_attention_event(id).unwrap());
        assert!(!store.produce_attention_event(id).unwrap());

        store
            .cas_exit(id, ProcessStatus::Running, &ExitReason::Exit { code: 0 })
            .unwrap();
        let events = store.pending_outbound_events(id).unwrap();
        assert_eq!(events.len(), 4);
        let EventPayload::Callback {
            event: attention, ..
        } = &events[2].event.payload
        else {
            panic!("inactivity callback payload")
        };
        assert_eq!(attention.event, EventKind::TaskCheckDue);
        let EventPayload::Callback {
            event: terminal,
            state,
        } = &events[3].event.payload
        else {
            panic!("terminal callback payload")
        };
        assert_eq!(*state, Some(ProcessStatus::Succeeded));
        assert_eq!(terminal.event, EventKind::TaskSucceeded);
        assert!(store.require_task(id).unwrap().attention.is_delivered());
    }

    #[test]
    fn old_attention_claim_can_be_released_into_a_durable_event() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        insert_local(&store, id);
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE tasks SET attention_state='sending' WHERE id=?1",
                [id.to_string()],
            )
            .unwrap();
        store.release_attention(id).unwrap();
        assert_eq!(
            store.require_task(id).unwrap().attention,
            AttentionState::Pending
        );
        assert!(store.produce_attention_event(id).unwrap());
        assert_eq!(store.pending_outbound_events(id).unwrap().len(), 3);
    }

    #[test]
    fn inactivity_event_cannot_be_produced_while_queued_or_after_terminal() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let queued_id = TaskId::new();
        insert_local(&store, queued_id);
        assert!(!store.produce_attention_event(queued_id).unwrap());
        assert_eq!(
            store.require_task(queued_id).unwrap().attention,
            AttentionState::Pending
        );

        let terminal_id = TaskId::new();
        insert_local(&store, terminal_id);
        store
            .cas_exit(terminal_id, ProcessStatus::Queued, &ExitReason::Cancelled)
            .unwrap();
        assert!(!store.produce_attention_event(terminal_id).unwrap());
        assert_eq!(
            store.require_task(terminal_id).unwrap().attention,
            AttentionState::Pending
        );
    }

    #[test]
    fn timeout_seconds_above_i64_max_round_trip() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        let mut row = agent_row(id);
        row.timeout = Duration::from_secs(u64::MAX);
        store.insert_task(&row).unwrap();
        assert_eq!(
            store.require_task(id).unwrap().timeout,
            Duration::from_secs(u64::MAX),
            "a timeout wider than SQLite INTEGER must survive persistence"
        );
    }

    #[test]
    fn report_append_order_and_cap() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        let reports = store
            .append_report(id, ReportOutcome::Blocked, "need x")
            .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].seq, 1);
        let reports = store
            .append_report(id, ReportOutcome::Succeeded, "got x")
            .unwrap();
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[1].seq, 2);
        for i in 0..18 {
            store
                .append_report(id, ReportOutcome::Succeeded, &format!("n{i}"))
                .unwrap();
        }
        let err = store
            .append_report(id, ReportOutcome::Succeeded, "overflow")
            .unwrap_err();
        assert!(matches!(err, AppError::TooManyReports { count: 20 }));
    }

    #[test]
    fn report_summary_too_long() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        let long = "x".repeat(SUMMARY_MAX_BYTES + 1);
        let err = store
            .append_report(id, ReportOutcome::Succeeded, &long)
            .unwrap_err();
        assert!(matches!(err, AppError::SummaryTooLong { .. }));
    }

    #[test]
    fn report_on_terminal_rejected() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        store
            .cas_exit(id, ProcessStatus::Queued, &ExitReason::Cancelled)
            .unwrap()
            .expect("queued task cancels");
        let err = store
            .append_report(id, ReportOutcome::Succeeded, "late")
            .unwrap_err();
        assert!(matches!(err, AppError::TaskTerminal { .. }));
    }

    #[test]
    fn cancel_race_reports_cancelled_queued() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db");
        let store = Store::open(&db).unwrap();
        let worker = Store::open(&db).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();

        worker.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        worker
            .conn
            .execute(
                "UPDATE tasks SET status = 'running' WHERE id = ?1 AND status = 'queued'",
                params![id.to_string()],
            )
            .unwrap();

        let cancel = std::thread::spawn(move || {
            let result = store.request_cancel(id).unwrap();
            (store, result)
        });
        std::thread::sleep(Duration::from_millis(200));
        worker.conn.execute_batch("COMMIT").unwrap();

        let (store, result) = cancel.join().unwrap();
        let row = store.require_task(id).unwrap();
        assert_eq!(row.status(), ProcessStatus::Running);
        assert!(
            matches!(result, CancelResult::SignalWorker(_)),
            "a row that reached Running must be signalled, not reported cancelled: {result:?}"
        );
    }

    #[test]
    fn recover_lost() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        assert!(
            store
                .cas_status(id, ProcessStatus::Running, ProcessStatus::Lost)
                .unwrap()
                .is_some()
        );
        assert_eq!(store.require_task(id).unwrap().state, TaskState::Lost);
    }
}
