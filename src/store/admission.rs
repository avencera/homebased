//! Accepting a task row with its executor identity, origin route, and first event

use rusqlite::{Connection, OptionalExtension, params};

use super::Store;
use super::events::append_produced_event_on;
use super::identity::insert_identity_on;
use super::task::insert_accepted_task_on;
use crate::dependency::TaskDependencies;
use crate::domain::{ProcessStatus, TaskId, TaskRow};
use crate::error::AppError;
use crate::events::EventPayload;
use crate::machine::MachineId;
use crate::spec::NormalizedSpec;
use crate::submission::{
    CallbackContext, CallbackExecutable, ExecutionRecord, ExecutorIdentity, HeldPhase, OriginRoute,
    RequestId, SubmissionState,
};

/// How a local launch commits its origin route with the task row
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalAdmission {
    /// A new submission: insert an accepted route for this request
    Submitted {
        /// Caller retry identity
        request: RequestId,
        /// Dependencies that had all succeeded when the request was admitted
        after: Option<TaskDependencies>,
    },
    /// A held route whose dependencies succeeded: accept the route saved at submit
    Released {
        /// Retry identity saved with the held route
        request: RequestId,
    },
}

fn validate_local_task_acceptance(
    row: &TaskRow,
    spec: &NormalizedSpec,
    callback: &CallbackContext,
) -> Result<(), AppError> {
    if spec.machine.is_some()
        || row.status() != ProcessStatus::Queued
        || row.name != spec.name
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

/// Save the accepted executor identity of a new row and its queued event
fn accept_execution_on(
    conn: &Connection,
    row: &TaskRow,
    spec: &NormalizedSpec,
    origin: MachineId,
    execution: MachineId,
) -> Result<ExecutorIdentity, AppError> {
    let identity = ExecutorIdentity::Accepted(ExecutionRecord {
        task: row.id,
        origin_machine: origin,
        execution_machine: execution,
        spec: spec.clone(),
        state: ProcessStatus::Queued,
    });
    insert_identity_on(conn, row.id, origin, &identity)?;
    append_produced_event_on(
        conn,
        row.id,
        EventPayload::State {
            status: ProcessStatus::Queued,
        },
    )?;
    Ok(identity)
}

fn insert_local_task_records_on(
    conn: &Connection,
    row: &TaskRow,
    spec: &NormalizedSpec,
    machine: MachineId,
    request: RequestId,
    callback: &CallbackContext,
) -> Result<(), AppError> {
    validate_local_task_acceptance(row, spec, callback)?;
    insert_accepted_task_on(conn, row)?;
    let route = OriginRoute {
        request,
        task: row.id,
        origin_machine: machine,
        execution_machine: machine,
        thread: row.thread,
        callback: callback.clone(),
        spec: spec.clone(),
        submission: SubmissionState::Accepted,
        last_execution_state: Some(ProcessStatus::Queued),
        last_updated_at: chrono::Utc::now(),
        last_accepted_seq: 0,
        last_settled_seq: 0,
    };
    conn.execute(
        "INSERT INTO origin_routes (request_id,task_id,route_json) VALUES (?1,?2,?3)",
        params![
            request.0.to_string(),
            row.id.to_string(),
            serde_json::to_string(&route)?
        ],
    )?;
    accept_execution_on(conn, row, spec, machine, machine)?;
    Ok(())
}

/// Accept the waiting held route of a local task with its first row and event
///
/// The row must repeat the saved spec and callback environment, so the task
/// runs as submitted even though the submitting shell is gone
fn release_held_local_task_records_on(
    conn: &Connection,
    row: &TaskRow,
    spec: &NormalizedSpec,
    machine: MachineId,
    request: RequestId,
) -> Result<(), AppError> {
    let route_json: String = conn
        .query_row(
            "SELECT route_json FROM origin_routes WHERE task_id=?1",
            [row.id.to_string()],
            |entry| entry.get(0),
        )
        .optional()?
        .ok_or(AppError::RouteNotFound { task: row.id })?;
    let mut route: OriginRoute = serde_json::from_str(&route_json)?;
    let waiting = matches!(
        route.submission,
        SubmissionState::Held {
            phase: HeldPhase::Waiting
        }
    );
    if !waiting
        || route.request != request
        || route.task != row.id
        || route.origin_machine != machine
        || route.execution_machine != machine
        || route.spec != *spec
    {
        return Err(AppError::ClusterTaskConflict { task: row.id });
    }
    validate_local_task_acceptance(row, spec, &route.callback)?;

    insert_accepted_task_on(conn, row)?;
    route.submission = SubmissionState::Accepted;
    route.last_execution_state = Some(ProcessStatus::Queued);
    route.last_updated_at = chrono::Utc::now();
    conn.execute(
        "UPDATE origin_routes SET route_json=?1 WHERE task_id=?2",
        params![serde_json::to_string(&route)?, row.id.to_string()],
    )?;
    accept_execution_on(conn, row, spec, machine, machine)?;
    Ok(())
}

impl Store {
    /// Insert a new local task with both owners and its initial state event
    pub fn insert_local_task(
        &self,
        row: &TaskRow,
        spec: &NormalizedSpec,
        machine: MachineId,
        request: RequestId,
        codex: CallbackExecutable,
    ) -> Result<(), AppError> {
        let admission = LocalAdmission::Submitted {
            request,
            after: None,
        };
        self.admit_local_task(row, spec, machine, &admission, codex)
    }

    /// Commit a local task row with the origin route its admission names
    ///
    /// A submission inserts a new accepted route, saving its dependencies. A
    /// release accepts the held route saved at submit, with the callback
    /// context saved then, so the row and the route change together
    pub fn admit_local_task(
        &self,
        row: &TaskRow,
        spec: &NormalizedSpec,
        machine: MachineId,
        admission: &LocalAdmission,
        codex: CallbackExecutable,
    ) -> Result<(), AppError> {
        match admission {
            LocalAdmission::Submitted { request, after } => {
                let callback = CallbackContext {
                    env: row.env.clone(),
                    cwd: row.cwd.clone(),
                    codex,
                };
                self.immediate(|| {
                    insert_local_task_records_on(
                        &self.conn, row, spec, machine, *request, &callback,
                    )?;
                    if let Some(after) = after {
                        self.conn.execute(
                            "UPDATE origin_routes SET after_json=?1 WHERE task_id=?2",
                            params![serde_json::to_string(after)?, row.id.to_string()],
                        )?;
                    }
                    Ok(())
                })
            }
            LocalAdmission::Released { request } => self.immediate(|| {
                release_held_local_task_records_on(&self.conn, row, spec, machine, *request)
            }),
        }
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
            || row.name != spec.name
            || row.thread != spec.thread
            || row.timeout != spec.timeout
            || !row.cwd.is_absolute()
            || !row.binary.is_absolute()
        {
            return Err(AppError::ClusterTaskConflict { task: row.id });
        }
        self.immediate(|| {
            let saved: Option<String> = self
                .conn
                .query_row(
                    "SELECT identity_json FROM executor_identities WHERE task_id=?1",
                    [row.id.to_string()],
                    |entry| entry.get(0),
                )
                .optional()?;
            if let Some(saved) = saved {
                let identity: ExecutorIdentity = serde_json::from_str(&saved)?;
                let same = match &identity {
                    ExecutorIdentity::Accepted(record) => {
                        record.origin_machine == origin
                            && record.execution_machine == execution
                            && record.spec == *spec
                    }
                    ExecutorIdentity::Rejected(record) => {
                        record.origin_machine == origin && record.execution_machine == execution
                    }
                };
                return if same {
                    Ok(identity)
                } else {
                    Err(AppError::ClusterTaskConflict { task: row.id })
                };
            }
            if self.get_task(row.id)?.is_some() {
                return Err(AppError::ClusterTaskConflict { task: row.id });
            }
            insert_accepted_task_on(&self.conn, row)?;
            accept_execution_on(&self.conn, row, spec, origin, execution)
        })
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
                let accepted_submission = matches!(route.submission, SubmissionState::Accepted);
                record.task == id
                    && route.task == id
                    && route.origin_machine == route.execution_machine
                    && record.origin_machine == route.origin_machine
                    && record.execution_machine == route.execution_machine
                    && record.spec == route.spec
                    && accepted_submission
            }
            (ExecutorIdentity::Accepted(record), None) => {
                record.task == id && record.origin_machine != record.execution_machine
            }
            _ => false,
        };
        if !valid {
            return Err(AppError::ClusterTaskConflict { task: id });
        }
        Ok(true)
    }
}
