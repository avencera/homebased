//! Task rows: insert, read, and the column codecs they share

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use nix::unistd::Pid;
use rusqlite::{Connection, OptionalExtension, params};

use super::chain::thread_reserved_by_on;
use super::{Store, fmt_time, parse_time};
use crate::cleanup::{ProcessIdentity, ProcessStartTime};
use crate::domain::{
    AgentKind, ContainerExitEvidence, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId,
    TaskName, TaskRow, TaskState, ThreadId, Workload,
};
use crate::error::AppError;

pub(super) const TASK_SELECT: &str = "SELECT id, thread_id, name, workload_json, cwd, timeout_secs,
    env_path, env_home, binary, status, exit_reason, check_due_at, pid, cancel_requested_at,
    created_at, updated_at, process_group_exit_evidence, container_exit_evidence, child_pid,
    child_start_time
 FROM tasks";

/// Insert an accepted task row with the Git root of its working directory
pub(super) fn insert_accepted_task_on(conn: &Connection, row: &TaskRow) -> Result<(), AppError> {
    let project_root = find_project_root(&row.cwd);
    insert_task_on(conn, row, project_root.as_deref())
}

fn insert_task_on(
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
    if agent.agent.kind != AgentKind::Codex {
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
    // a parked chain resumes this thread later, so it owns it until then
    if let Some(task) = thread_reserved_by_on(conn, thread, row.id)? {
        return Err(AppError::ResumeThreadBusy { thread, task });
    }
    Ok(())
}

impl Store {
    /// Insert a queued task without an origin route or executor identity
    pub fn insert_task(&self, row: &TaskRow) -> Result<(), AppError> {
        self.immediate(|| insert_task_on(&self.conn, row, None))
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
        let mut sql = format!("{TASK_SELECT} WHERE 1=1");
        if !statuses.is_empty() {
            let statuses = statuses
                .iter()
                .map(|status| format!("'{}'", status.as_str()))
                .collect::<Vec<_>>()
                .join(",");
            sql.push_str(&format!(" AND status IN ({statuses})"));
        }
        if thread.is_some() {
            sql.push_str(" AND thread_id = ?1");
        }
        sql.push_str(" ORDER BY id");
        let mut stmt = self.conn.prepare(&sql)?;
        let thread = thread.map(|thread| thread.to_string());
        let rows = stmt
            .query_map(rusqlite::params_from_iter(thread), parse_task_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
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

pub(super) fn parse_task_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRow> {
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
    let json_err = |err: serde_json::Error| {
        parse_err(AppError::Internal {
            message: err.to_string(),
        })
    };

    let id: TaskId = id.parse().map_err(parse_err)?;
    let thread: ThreadId = thread.parse().map_err(parse_err)?;
    let name = TaskName::parse(&name).map_err(|err| {
        parse_err(AppError::Internal {
            message: format!("stored task name: {err}"),
        })
    })?;
    let workload: Workload = serde_json::from_str(&workload_json).map_err(json_err)?;
    let exit_reason = exit_reason
        .map(|raw| serde_json::from_str(&raw).map_err(json_err))
        .transpose()?;
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
    let parse_optional_time = |raw: Option<String>| {
        raw.as_deref()
            .map(parse_time)
            .transpose()
            .map_err(parse_err)
    };
    Ok(TaskRow {
        id,
        name,
        thread,
        workload,
        cwd: PathBuf::from(cwd),
        timeout: parse_timeout(&timeout_secs).map_err(parse_err)?,
        env: TaskEnv {
            path: env_path,
            home: env_home,
        },
        binary: PathBuf::from(binary),
        state: TaskState::from_storage(status, exit_reason, pid).map_err(parse_err)?,
        process_group_exit_evidence,
        container_exit_evidence,
        child: child_from_storage(child_pid, child_start_time).map_err(parse_err)?,
        check_due_at: parse_optional_time(check_due_at)?,
        cancel_requested_at: parse_optional_time(cancel_requested_at)?,
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
    pub cwd: PathBuf,
    /// Output-inactivity timeout
    pub timeout: Duration,
    /// Captured env
    pub env: TaskEnv,
    /// Resolved binary
    pub binary: PathBuf,
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
