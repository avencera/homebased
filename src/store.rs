//! SQLite source of truth: task state, sequenced events, and callback results

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use nix::unistd::Pid;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, params_from_iter};

use crate::callback::{EventKind, ReportView, check_due_event, notify_event, terminal_event};
use crate::cleanup::{ProcessIdentity, ProcessStartTime};
use crate::dependency::TaskDependencies;
use crate::domain::{
    CallbackStatus, ContainerExitEvidence, ExitReason, ProcessGroupExitEvidence, ProcessStatus,
    REPORTS_MAX, ReportOutcome, SUMMARY_MAX_BYTES, TaskEnv, TaskExitEvidence, TaskId, TaskName,
    TaskReport, TaskRow, TaskState, ThreadId, Workload, check_report_allowed,
    check_status_transition,
};
use crate::error::AppError;
use crate::events::EventPayload;
use crate::events::{DeliveryState, TaskEvent};
use crate::machine::MachineId;
use crate::spec::NormalizedSpec;
use crate::submission::{
    CallbackContext, CallbackExecutable, ExecutionRecord, ExecutorIdentity, OriginRoute, RequestId,
    SubmissionState,
};

mod cancellation;
mod container;
mod dependency;
mod events;
mod identity;
mod message;
pub mod queue;
mod schema;
pub use container::TaskContainerRecord;
pub use dependency::{HeldCancel, UnlaunchedTask};
pub(crate) use events::EventRetentionBatch;
pub use identity::IdentityError;

const TASK_SELECT: &str = "SELECT id, thread_id, name, workload_json, cwd, timeout_secs,
    env_path, env_home, binary, status, exit_reason, check_due_at, pid, cancel_requested_at,
    created_at, updated_at, process_group_exit_evidence, container_exit_evidence, child_pid,
    child_start_time
 FROM tasks";

/// Open or create the database
pub struct Store {
    conn: Connection,
    tasks_dir: std::path::PathBuf,
}

fn insert_task_with_project_root_on(
    conn: &Connection,
    row: &TaskRow,
    project_root: Option<&Path>,
) -> Result<(), AppError> {
    reject_busy_resume_thread_on(conn, row)?;
    let (child_pid, child_start_time) = child_to_storage(row.child)?;
    conn.execute(
        "INSERT INTO tasks (
            id, thread_id, name, workload_json, cwd, timeout_secs,
            env_path, env_home, binary, status, exit_reason, check_due_at,
            pid, cancel_requested_at, created_at, updated_at, project_root,
            process_group_exit_evidence, container_exit_evidence, child_pid, child_start_time
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
        params![
            row.id.to_string(),
            row.thread.to_string(),
            row.name.as_str(),
            serde_json::to_string(&row.workload)?,
            row.cwd.to_string_lossy(),
            fmt_timeout(row.timeout),
            row.env.path,
            row.env.home,
            row.binary.to_string_lossy(),
            row.status().as_str(),
            row.exit_reason().map(serde_json::to_string).transpose()?,
            row.check_due_at.map(fmt_time),
            row.pid(),
            row.cancel_requested_at.map(fmt_time),
            fmt_time(row.created_at),
            fmt_time(row.updated_at),
            project_root.map(|root| root.to_string_lossy().into_owned()),
            row.process_group_exit_evidence.as_str(),
            row.container_exit_evidence.to_storage()?,
            child_pid,
            child_start_time,
        ],
    )?;
    Ok(())
}

fn reject_busy_resume_thread_on(conn: &Connection, row: &TaskRow) -> Result<(), AppError> {
    let Workload::Agent(agent) = &row.workload else {
        return Ok(());
    };
    if agent.agent.kind != crate::domain::AgentKind::Codex {
        return Ok(());
    }
    let Some(thread) = agent.resume_thread else {
        return Ok(());
    };
    let task: Option<String> = conn
        .query_row(
            "SELECT id FROM tasks
             WHERE status IN ('queued', 'running')
               AND json_extract(workload_json, '$.type') = 'agent'
               AND json_extract(workload_json, '$.agent') = 'codex'
               AND json_extract(workload_json, '$.resume_thread') = ?1
             ORDER BY created_at, id LIMIT 1",
            [thread.to_string()],
            |entry| entry.get(0),
        )
        .optional()?;
    if let Some(task) = task {
        return Err(AppError::ResumeThreadBusy {
            thread,
            task: task.parse()?,
        });
    }
    Ok(())
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

fn insert_local_task_records_on(
    conn: &Connection,
    row: &TaskRow,
    spec: &NormalizedSpec,
    machine: MachineId,
    request: RequestId,
    callback: &CallbackContext,
) -> Result<(), AppError> {
    validate_local_task_acceptance(row, spec, callback)?;
    let project_root = find_project_root(&row.cwd);
    insert_task_with_project_root_on(conn, row, project_root.as_deref())?;
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
    let identity = ExecutorIdentity::Accepted(ExecutionRecord {
        task: row.id,
        origin_machine: machine,
        execution_machine: machine,
        spec: spec.clone(),
        state: ProcessStatus::Queued,
    });
    conn.execute(
        "INSERT INTO origin_routes (request_id,task_id,route_json) VALUES (?1,?2,?3)",
        params![
            request.0.to_string(),
            row.id.to_string(),
            serde_json::to_string(&route)?
        ],
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
            phase: crate::submission::HeldPhase::Waiting
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

    let project_root = find_project_root(&row.cwd);
    insert_task_with_project_root_on(conn, row, project_root.as_deref())?;
    route.submission = SubmissionState::Accepted;
    route.last_execution_state = Some(ProcessStatus::Queued);
    route.last_updated_at = chrono::Utc::now();
    let identity = ExecutorIdentity::Accepted(ExecutionRecord {
        task: row.id,
        origin_machine: machine,
        execution_machine: machine,
        spec: spec.clone(),
        state: ProcessStatus::Queued,
    });
    conn.execute(
        "UPDATE origin_routes SET route_json=?1 WHERE task_id=?2",
        params![serde_json::to_string(&route)?, row.id.to_string()],
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
    /// Codex thread created by this task's worker, when the log included one
    pub worker_thread: Option<ThreadId>,
    /// Owners from an accepted executor identity. Queue runs have none
    pub owners: Option<TaskOwners>,
    /// Delivery of the terminal event, or `None` when this machine owns no
    /// callback for the task: the origin of a remote task delivers it, and a
    /// queue run reports through its job
    pub terminal_callback: Option<CallbackStatus>,
    /// Whether this task's inactivity reminder reached the origin queue
    pub attention_delivered: bool,
}

/// Wait for another connection's write lock. It matches the daemon's actor call
/// timeout, since the daemon's callers stop waiting then anyway
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

impl Store {
    /// Open the SQLite file at `path`, applying the initial schema when empty
    pub fn open(path: &Path) -> Result<Self, AppError> {
        Self::open_with_busy_timeout(path, BUSY_TIMEOUT)
    }

    /// Open with a chosen wait for another connection's write lock
    ///
    /// A process that no caller waits on can outlast a slow daemon commit
    /// instead of failing its work
    pub fn open_with_busy_timeout(path: &Path, busy_timeout: Duration) -> Result<Self, AppError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut conn = Connection::open(path)?;
        conn.busy_timeout(busy_timeout)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let transaction = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        schema::migrate(&transaction)?;
        transaction.commit()?;
        let tasks_dir = path.parent().unwrap_or(Path::new(".")).join("tasks");
        Ok(Self { conn, tasks_dir })
    }

    /// Insert a queued task
    pub fn insert_task(&self, row: &TaskRow) -> Result<(), AppError> {
        self.immediate(|| self.insert_task_with_project_root(row, None))
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
        request: RequestId,
        codex: CallbackExecutable,
    ) -> Result<(), AppError> {
        let callback = CallbackContext {
            env: row.env.clone(),
            cwd: row.cwd.clone(),
            codex,
        };
        self.immediate(|| {
            insert_local_task_records_on(&self.conn, row, spec, machine, request, &callback)
        })
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
            let saved: Option<String> = self.conn.query_row(
                "SELECT identity_json FROM executor_identities WHERE task_id=?1",
                [row.id.to_string()],
                |entry| entry.get(0),
            ).optional()?;
            if let Some(saved) = saved {
                let identity: ExecutorIdentity = serde_json::from_str(&saved)?;
                let same = match &identity {
                    ExecutorIdentity::Accepted(record) => {
                        record.origin_machine == origin
                            && record.execution_machine == execution
                            && record.spec == *spec
                    }
                    ExecutorIdentity::Rejected(record) => record.origin_machine == origin
                        && record.execution_machine == execution,
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
            let project_root = find_project_root(&row.cwd);
            self.insert_task_with_project_root(row, project_root.as_deref())?;
            let identity = ExecutorIdentity::Accepted(ExecutionRecord {
                task: row.id,
                origin_machine: origin,
                execution_machine: execution,
                spec: spec.clone(),
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
                "SELECT t.id, t.project_root, e.identity_json, t.worker_thread
                 FROM tasks t LEFT JOIN executor_identities e ON e.task_id=t.id
                 WHERE t.id IN ({placeholders})"
            );
            let mut statement = self.conn.prepare(&sql)?;
            let rows = statement.query_map(params_from_iter(id_values.iter()), |row| {
                let raw_id: String = row.get(0)?;
                let project_root: Option<String> = row.get(1)?;
                let identity_json: Option<String> = row.get(2)?;
                let raw_worker_thread: Option<String> = row.get(3)?;
                let conversion_error = |error: AppError| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                };
                let id = raw_id.parse().map_err(conversion_error)?;
                let worker_thread = raw_worker_thread
                    .map(|raw| raw.parse::<ThreadId>().map_err(conversion_error))
                    .transpose()?;
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
                    worker_thread,
                    owners,
                    terminal_callback: None,
                    attention_delivered: false,
                })
            })?;
            let rows = rows.collect::<Result<Vec<_>, _>>()?;
            drop(statement);

            for mut presentation in rows {
                let task = self.require_task(presentation.id)?;
                presentation.terminal_callback = self.terminal_callback(&task)?;
                presentation.attention_delivered = self.attention_callback_delivered(&task)?;
                presentations.insert(presentation.id, presentation);
            }
        }

        Ok(presentations)
    }

    /// Whether a terminal callback still waits for its inbox result to settle
    pub fn has_pending_terminal_callbacks(&self) -> Result<bool, AppError> {
        // waiting threads can sleep for hours; their events remain durable across restarts
        let rows = self.list_tasks(
            &[
                ProcessStatus::Succeeded,
                ProcessStatus::Failed,
                ProcessStatus::Cancelled,
                ProcessStatus::Lost,
                ProcessStatus::Preempted,
            ],
            None,
        )?;
        for row in rows {
            if matches!(
                self.terminal_callback(&row)?,
                Some(CallbackStatus::Pending | CallbackStatus::Sending)
            ) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Delivery of a task's terminal event from this machine's origin inbox
    ///
    /// `None` when this machine owns no callback for the task: an executor
    /// whose origin is another machine, or a queue run, whose job reports
    fn terminal_callback(&self, row: &TaskRow) -> Result<Option<CallbackStatus>, AppError> {
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
                    Ok(None)
                }
                Some(_) => Err(AppError::ClusterTaskConflict { task: row.id }),
                None => Ok(None),
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
            return Ok(None);
        }
        if !row.state.is_terminal() {
            return Ok(Some(CallbackStatus::Pending));
        }
        let Some(delivery) = self.terminal_callback_delivery(row.id)? else {
            // the terminal event has not reached the origin inbox yet
            return Ok(Some(CallbackStatus::Pending));
        };
        delivery
            .callback_status()
            .map(Some)
            .ok_or_else(|| AppError::Internal {
                message: format!("terminal callback event {} is marked not required", row.id),
            })
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
                None => Ok(row.check_due_at.is_some()),
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

        // the reminder event may be compacted away; the row keeps when it was produced
        Ok(row.check_due_at.is_some())
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
        self.cas_status_with_worker_thread(id, from, to, None)
    }

    /// CAS process status and save a worker thread in the same update
    pub(crate) fn cas_status_with_worker_thread(
        &self,
        id: TaskId,
        from: ProcessStatus,
        to: ProcessStatus,
        worker_thread: Option<ThreadId>,
    ) -> Result<Option<TaskRow>, AppError> {
        check_status_transition(from, to)?;
        self.immediate(|| {
            // a run of a queued job ends through its job's classification
            if to.is_terminal() && self.job_run_link(id)?.is_some() {
                return self.commit_job_run_end(id, from, None, None, worker_thread);
            }
            let n = self.conn.execute(
                "UPDATE tasks SET status = ?1, updated_at = ?2,
                    worker_thread = COALESCE(?3, worker_thread)
                 WHERE id = ?4 AND status = ?5",
                params![
                    to.as_str(),
                    fmt_time(Utc::now()),
                    worker_thread.map(|thread| thread.to_string()),
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
        self.cas_exit_with_evidence_and_worker_thread(id, from, reason, evidence, None)
    }

    /// Commit terminal state and the Codex worker thread in one transaction
    pub(crate) fn cas_exit_with_evidence_and_worker_thread(
        &self,
        id: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
        evidence: impl Into<TaskExitEvidence>,
        worker_thread: Option<ThreadId>,
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
            self.cas_exit_inner_with_worker_thread(id, from, reason, evidence, worker_thread)
        })
    }

    fn cas_exit_inner(
        &self,
        id: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
        evidence: &TaskExitEvidence,
    ) -> Result<Option<TaskRow>, AppError> {
        self.cas_exit_inner_with_worker_thread(id, from, reason, evidence, None)
    }

    fn cas_exit_inner_with_worker_thread(
        &self,
        id: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
        evidence: &TaskExitEvidence,
        worker_thread: Option<ThreadId>,
    ) -> Result<Option<TaskRow>, AppError> {
        // a run of a queued job ends through its job's classification, which
        // may store another outcome than the raw exit and produces no task event
        if self.job_run_link(id)?.is_some() {
            return self.commit_job_run_end(id, from, Some(reason), Some(evidence), worker_thread);
        }
        let to = ProcessStatus::from(reason);
        let now = fmt_time(Utc::now());
        let reason_json = serde_json::to_string(reason)?;
        let n = self.conn.execute(
            "UPDATE tasks SET status = ?1, exit_reason = ?2,
                process_group_exit_evidence = ?3, container_exit_evidence = ?4,
                updated_at = ?5, worker_thread = COALESCE(?6, worker_thread)
             WHERE id = ?7 AND status = ?8",
            params![
                to.as_str(),
                reason_json,
                evidence.process_group.as_str(),
                evidence.container.to_storage()?,
                now,
                worker_thread.map(|thread| thread.to_string()),
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
        if self.job_run_link(row.id)?.is_some() {
            return Ok(());
        }
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
            let callback = terminal_event(row, &reports, evidence);
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

    /// Record the worker's child once, while the task runs
    ///
    /// Returns whether the identity was saved. A task that already has a child,
    /// or that is no longer running, keeps its row unchanged
    pub fn set_child_identity(&self, id: TaskId, child: ProcessIdentity) -> Result<bool, AppError> {
        let (pid, start) = child_to_storage(Some(child))?;
        let updated = self.conn.execute(
            "UPDATE tasks SET child_pid = ?1, child_start_time = ?2, updated_at = ?3
             WHERE id = ?4 AND status = 'running' AND child_pid IS NULL",
            params![pid, start, fmt_time(Utc::now()), id.to_string()],
        )?;
        Ok(updated == 1)
    }

    /// Mark cancel requested. Terminal tasks are unchanged (idempotent)
    pub fn request_cancel(&self, id: TaskId) -> Result<CancelResult, AppError> {
        self.immediate(|| {
            let task = self.require_task(id)?;
            if !task.state.is_terminal()
                && let Some(job) = self.job_run_link(id)?
                && let Some(run) = self.run_of_job(job)?.filter(|run| run.task == id)
            {
                self.request_stop(
                    run.resource,
                    Some(id),
                    crate::queue::StopCause::UserCancel,
                    Utc::now(),
                )?;
            }
            self.request_cancel_inner(id)
        })
    }

    fn request_cancel_inner(&self, id: TaskId) -> Result<CancelResult, AppError> {
        self.conn.execute(
            "UPDATE tasks SET cancel_requested_at = ?1, updated_at = ?1
                 WHERE id = ?2 AND cancel_requested_at IS NULL
                   AND status NOT IN ('succeeded', 'failed', 'cancelled', 'lost', 'preempted')",
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
        if self.job_run_link(id)?.is_some() {
            return self.produce_job_check_due(id);
        }
        self.immediate(|| {
            if !self.is_event_task(id)? {
                return Ok(false);
            }
            let now = fmt_time(Utc::now());
            let changed = self.conn.execute(
                "UPDATE tasks SET check_due_at=?1, updated_at=?1
                 WHERE id=?2 AND check_due_at IS NULL AND status='running'",
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
                "INSERT INTO reports (task_id, seq, outcome, summary, reported_at)
                 VALUES (?1,?2,?3,?4,?5)",
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
            }
            Ok(reports)
        })
    }

    /// Reports in seq order
    pub fn reports(&self, id: TaskId) -> Result<Vec<TaskReport>, AppError> {
        reports_from(&self.conn, id)
    }
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

/// `child_pid` and `child_start_time` columns for a recorded child
fn child_to_storage(
    child: Option<ProcessIdentity>,
) -> Result<(Option<i32>, Option<i64>), AppError> {
    let Some(child) = child else {
        return Ok((None, None));
    };
    let start = i64::try_from(child.start.as_raw()).map_err(|_| AppError::Internal {
        message: format!("child start time exceeds SQLite range: {child}"),
    })?;
    Ok((Some(child.pid.as_raw()), Some(start)))
}

fn child_from_storage(
    pid: Option<i32>,
    start: Option<i64>,
) -> Result<Option<ProcessIdentity>, AppError> {
    match (pid, start) {
        (None, None) => Ok(None),
        (Some(pid), Some(start)) if pid > 0 => {
            let start = u64::try_from(start).map_err(|_| AppError::Internal {
                message: format!("negative child start time {start}"),
            })?;
            Ok(Some(ProcessIdentity {
                pid: Pid::from_raw(pid),
                start: ProcessStartTime::from_raw(start),
            }))
        }
        (pid, start) => Err(AppError::Internal {
            message: format!("invalid child identity: pid={pid:?} start={start:?}"),
        }),
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
    let name: String = row.get(2)?;
    let workload_json: String = row.get(3)?;
    let cwd: String = row.get(4)?;
    let timeout_secs: String = row.get(5)?;
    let env_path: String = row.get(6)?;
    let env_home: String = row.get(7)?;
    let binary: String = row.get(8)?;
    let status: String = row.get(9)?;
    let exit_reason: Option<String> = row.get(10)?;
    let check_due_at: Option<String> = row.get(11)?;
    let pid: Option<i32> = row.get(12)?;
    let cancel_requested_at: Option<String> = row.get(13)?;
    let created_at: String = row.get(14)?;
    let updated_at: String = row.get(15)?;
    let process_group_exit_evidence: String = row.get(16)?;
    let container_exit_evidence: Option<String> = row.get(17)?;
    let child_pid: Option<i32> = row.get(18)?;
    let child_start_time: Option<i64> = row.get(19)?;

    let parse_err = |err: AppError| rusqlite::Error::ToSqlConversionFailure(Box::new(err));

    let id: TaskId = id.parse().map_err(parse_err)?;
    let thread: ThreadId = thread.parse().map_err(parse_err)?;
    let name = TaskName::parse(&name).map_err(|err| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(AppError::Internal {
            message: format!("stored task name: {err}"),
        }))
    })?;
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
        ProcessStatus::Succeeded
        | ProcessStatus::Failed
        | ProcessStatus::Cancelled
        | ProcessStatus::Preempted => (
            ProcessGroupExitEvidence::from_storage(&process_group_exit_evidence)
                .map_err(parse_err)?,
            ContainerExitEvidence::from_storage(container_exit_evidence.as_deref())
                .map_err(parse_err)?,
        ),
        ProcessStatus::Queued | ProcessStatus::Running | ProcessStatus::Lost => (
            ProcessGroupExitEvidence::Unconfirmed,
            ContainerExitEvidence::Unconfirmed,
        ),
    };
    let check_due_at = match check_due_at {
        Some(raw) => Some(parse_time(&raw).map_err(parse_err)?),
        None => None,
    };
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
        child: child_from_storage(child_pid, child_start_time).map_err(parse_err)?,
        check_due_at,
        cancel_requested_at,
        created_at: parse_time(&created_at).map_err(parse_err)?,
        updated_at: parse_time(&updated_at).map_err(parse_err)?,
    })
}

/// Inputs for a newly queued task
pub struct NewTask {
    /// Task id
    pub id: TaskId,
    /// Submitted name
    pub name: TaskName,
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
        child: None,
        check_due_at: None,
        cancel_requested_at: None,
        created_at: now,
        updated_at: now,
    }
}

/// JSON view of `exit.json`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExitJson {
    /// Exit reason
    pub reason: ExitReason,
    /// Evidence for the task-run worker's child process group
    pub process_group_exit_evidence: ProcessGroupExitEvidence,
    /// Evidence for a container task's container
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

/// Parse `exit.json` if present
pub fn read_exit_json(path: &Path) -> Result<Option<ExitJson>, AppError> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(serde_json::from_str(&text)?)),
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
        CancelResult, NewTask, Store, new_queued_task, read_exit_json,
        write_exit_json_with_evidence,
    };
    use crate::callback::EventKind;
    use crate::daemon::api::views::TaskSummary;
    use crate::domain::{
        Agent, AgentKind, AgentWorkload, CallbackStatus, ContainerExitEvidence, ContainerId,
        ExitReason, ProcessGroupExitEvidence, ProcessStatus, ReportOutcome, SUMMARY_MAX_BYTES,
        TaskEnv, TaskExitEvidence, TaskId, TaskName, TaskRow, TaskState, TaskWorkload, ThreadId,
        TransitionError, Workload,
    };
    use crate::error::AppError;
    use crate::events::{DeliveryOutcome, EventPayload};
    use crate::invocation::CommandLine;
    use crate::machine::MachineId;
    use crate::spec::NormalizedSpec;
    use crate::submission::{CallbackExecutable, ExecutorIdentity};
    use rusqlite::params;
    use serde_json::json;
    use std::num::NonZeroU64;
    use std::path::Path;
    use std::str::FromStr;
    use std::time::Duration;
    use tempfile::tempdir;

    fn agent_row(id: TaskId) -> TaskRow {
        new_queued_task(NewTask {
            id,
            name: TaskName::parse("agent job").unwrap(),
            thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
            workload: Workload::Agent(AgentWorkload {
                agent: Agent::new(AgentKind::Claude, Some("fable".into())),
                extra_args: vec!["--verbose".into()],
                report_trailer: true,
                resume_thread: None,
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
            name: TaskName::parse("command job").unwrap(),
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
        row.name = TaskName::parse("local task").unwrap();
        let spec = local_spec(&row);
        store
            .insert_local_task(
                &row,
                &spec,
                MachineId::new(),
                crate::submission::RequestId::new(),
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
            store
                .get_task(id)
                .unwrap()
                .map(|row| row.process_group_exit_evidence()),
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
            store
                .get_task(id)
                .unwrap()
                .map(|row| row.process_group_exit_evidence()),
            Some(ProcessGroupExitEvidence::NoChildSpawned)
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
            if outbox.event.payload.notification_required() {
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
        row.name = TaskName::parse("local task").unwrap();
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
                crate::submission::RequestId::new(),
                CallbackExecutable::available("/bin/true".into()),
            )
            .unwrap();
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
        let (remote_row, remote_spec) = row_at(remote_id, &remote_cwd);
        store
            .insert_remote_task(&remote_row, &remote_spec, origin_machine, execution_machine)
            .unwrap();

        // a row with neither identity nor route, the shape of a queue run
        let run_id = TaskId::new();
        let run_row = task_row(run_id);
        store.insert_task(&run_row).unwrap();

        let presentations = store
            .task_presentations(&[local_id, remote_id, run_id])
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
        // the origin, not this executor, delivers the callback
        assert_eq!(remote_json["callback"], json!(null));

        let run = presentations.get(&run_id).unwrap();
        assert_eq!(run.owners, None);
        let run_json = serde_json::to_value(TaskSummary::from_row(&run_row, Some(run))).unwrap();
        assert!(run_json.get("origin_machine").is_none());
        assert!(run_json.get("execution_machine").is_none());
        assert_eq!(run_json["callback"], json!(null));
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
        row.name = TaskName::parse("local task").unwrap();
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
                .map(|event| event.event.payload.notification_required())
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
                    crate::submission::RequestId::new(),
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
    fn child_identity_is_recorded_once_while_the_task_runs() {
        use crate::cleanup::{ProcessIdentity, ProcessStartTime};
        use nix::unistd::Pid;

        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let id = TaskId::new();
        let child = ProcessIdentity {
            pid: Pid::from_raw(4242),
            start: ProcessStartTime::from_raw(1_790_000_000_123_456),
        };
        {
            let store = Store::open(&path).unwrap();
            insert_local(&store, id);
            assert!(!store.set_child_identity(id, child).unwrap(), "queued");
            store
                .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
                .unwrap();
            assert!(store.set_child_identity(id, child).unwrap());
            let later = ProcessIdentity {
                pid: Pid::from_raw(4343),
                ..child
            };
            assert!(!store.set_child_identity(id, later).unwrap(), "already set");
        }

        let store = Store::open(&path).unwrap();
        assert_eq!(store.require_task(id).unwrap().child, Some(child));
    }

    #[test]
    fn preempted_run_keeps_its_state_event_and_outcome_and_is_never_success() {
        use crate::callback::{EventKind, NextAction};
        use crate::dependency::{DependencyState, TaskOutcome};

        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let id = TaskId::new();
        {
            let mut store = Store::open(&path).unwrap();
            insert_local(&store, id);
            store
                .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
                .unwrap();
            // nothing produces a preemption before the queue's terminal commit
            // exists, so the row takes the state that commit will write
            store
                .conn
                .execute(
                    "UPDATE tasks SET status = 'preempted', exit_reason = ?1 WHERE id = ?2",
                    params![
                        serde_json::to_string(&ExitReason::Exit { code: 75 }).unwrap(),
                        id.to_string()
                    ],
                )
                .unwrap();
            let row = store.require_task(id).unwrap();
            store.produce_state_event(&row).unwrap();
            deliver_outbound_events(&mut store, id, |_| DeliveryOutcome::Delivered);
        }

        let store = Store::open(&path).unwrap();
        let row = store.require_task(id).unwrap();
        assert_eq!(
            row.state,
            TaskState::Preempted {
                reason: ExitReason::Exit { code: 75 }
            }
        );
        assert_eq!(row.status(), ProcessStatus::Preempted);

        let last = store.inbound_events(id).unwrap().pop().unwrap();
        let EventPayload::Callback { event, state } = &last.event.payload else {
            panic!("terminal event is a callback: {last:?}");
        };
        assert_eq!(*state, Some(ProcessStatus::Preempted));
        assert_eq!(event.event, EventKind::TaskPreempted);
        assert_eq!(event.next_action, NextAction::None);
        let wire = serde_json::to_value(event).unwrap();
        assert_eq!(wire["event"], "TASK_PREEMPTED");
        assert_eq!(
            wire["process"],
            serde_json::json!({"kind": "exit", "code": 75})
        );

        let outcome: String = store
            .conn
            .query_row(
                "SELECT outcome FROM origin_routes WHERE task_id = ?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(outcome, "preempted");
        let states = store.dependency_states(&[id]).unwrap();
        assert_eq!(
            states,
            vec![(
                id,
                Some(DependencyState::Ended(TaskOutcome::Preempted.into()))
            )]
        );
        assert!(!TaskOutcome::Preempted.is_success());
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
    fn insert_list_get_agent_and_task() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let agent_id = TaskId::new();
        let task_id = TaskId::new();
        store.insert_task(&agent_row(agent_id)).unwrap();
        store.insert_task(&task_row(task_id)).unwrap();
        let got = store.require_task(agent_id).unwrap();
        assert!(matches!(got.workload, Workload::Agent(_)));
        assert_eq!(got.name.as_str(), "agent job");
        let got = store.require_task(task_id).unwrap();
        assert!(matches!(got.workload, Workload::Task(_)));
        assert_eq!(got.name.as_str(), "command job");
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
        assert!(outbox[2].event.payload.notification_required());
        assert!(outbox[3].event.payload.notification_required());
        assert_eq!(
            store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
            Some(CallbackStatus::Pending)
        );

        deliver_outbound_events(&mut store, id, |_| DeliveryOutcome::Delivered);

        assert_eq!(
            store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
            Some(CallbackStatus::Sent)
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
            Some(CallbackStatus::Sent)
        );
        drop(store);

        let reopened = Store::open(&path).unwrap();
        assert_eq!(
            reopened.task_presentations(&[id]).unwrap()[&id].terminal_callback,
            Some(CallbackStatus::Sent)
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
            Some(CallbackStatus::Sent)
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
            Some(CallbackStatus::Failed)
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
    fn terminal_callback_waiting_projects_to_callback_status() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        insert_local(&store, id);
        store
            .cas_exit(id, ProcessStatus::Queued, &ExitReason::Cancelled)
            .unwrap();

        deliver_outbound_events(&mut store, id, |_| {
            DeliveryOutcome::Deferred("origin thread is asleep".into())
        });

        assert_eq!(
            store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
            Some(CallbackStatus::Waiting)
        );
        assert_eq!(
            serde_json::to_value(TaskSummary::from_row(
                &store.require_task(id).unwrap(),
                Some(&store.task_presentations(&[id]).unwrap()[&id]),
            ))
            .unwrap()["callback"],
            "waiting"
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
        assert!(store.require_task(id).unwrap().check_due_at.is_some());
    }

    #[test]
    fn inactivity_event_cannot_be_produced_while_queued_or_after_terminal() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let queued_id = TaskId::new();
        insert_local(&store, queued_id);
        assert!(!store.produce_attention_event(queued_id).unwrap());
        assert_eq!(store.require_task(queued_id).unwrap().check_due_at, None);

        let terminal_id = TaskId::new();
        insert_local(&store, terminal_id);
        store
            .cas_exit(terminal_id, ProcessStatus::Queued, &ExitReason::Cancelled)
            .unwrap();
        assert!(!store.produce_attention_event(terminal_id).unwrap());
        assert_eq!(store.require_task(terminal_id).unwrap().check_due_at, None);
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
