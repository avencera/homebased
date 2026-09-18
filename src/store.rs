//! SQLite source of truth: schema, CAS transitions, callback claim.

use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;

use crate::domain::{
    AttentionState, CallbackStatus, ExitReason, ProcessStatus, REPORTS_MAX, ReportOutcome,
    SCHEMA_VERSION, SUMMARY_MAX_BYTES, TaskEnv, TaskId, TaskName, TaskReport, TaskRow, TaskState,
    ThreadId, Workload, check_callback_sent, check_report_allowed, check_status_transition,
};
use crate::error::AppError;

/// `timeout_secs` is decimal TEXT, not INTEGER: the attention timer has no
/// product maximum, and a `Duration` above `i64::MAX` seconds cannot be stored
/// in SQLite's signed INTEGER without a lossy cast.
const SCHEMA: &str = r"
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

const TASK_SELECT: &str = "SELECT id, thread_id, name, workload_json, cwd, timeout_secs,
    env_path, env_home, binary, status, exit_reason, callback_status,
    attention_state, timeout_notified_at, pid, cancel_requested_at, created_at, updated_at
 FROM tasks";

/// Schema version 1 had no `name` column.
const MIGRATE_1_TO_2: &str = r"
ALTER TABLE tasks ADD COLUMN name TEXT;
";

/// Open or create the database.
pub struct Store {
    conn: Connection,
}

impl Store {
    /// Open the SQLite file at `path`, applying the initial schema when empty.
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
        match version {
            0 => {
                transaction.execute_batch(SCHEMA)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            }
            1 => {
                transaction.execute_batch(MIGRATE_1_TO_2)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            }
            v if v == SCHEMA_VERSION => {}
            other => {
                return Err(AppError::Internal {
                    message: format!("unsupported schema user_version={other}"),
                });
            }
        }
        transaction.commit()?;
        Ok(Self { conn })
    }

    /// Insert a queued task.
    pub fn insert_task(&self, row: &TaskRow) -> Result<(), AppError> {
        self.conn.execute(
            "INSERT INTO tasks (
                id, thread_id, name, workload_json, cwd, timeout_secs,
                env_path, env_home, binary, status, exit_reason,
                callback_status, attention_state, timeout_notified_at,
                pid, cancel_requested_at, created_at, updated_at
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
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
            ],
        )?;
        Ok(())
    }

    /// Fetch one task.
    pub fn get_task(&self, id: TaskId) -> Result<Option<TaskRow>, AppError> {
        let mut stmt = self.conn.prepare(&format!("{TASK_SELECT} WHERE id = ?1"))?;
        let row = stmt
            .query_row(params![id.to_string()], parse_task_row)
            .optional()?;
        Ok(row)
    }

    /// Require a task row.
    pub fn require_task(&self, id: TaskId) -> Result<TaskRow, AppError> {
        self.get_task(id)?.ok_or(AppError::TaskNotFound { id })
    }

    /// List tasks, optionally filtered.
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

    /// Non-terminal tasks.
    pub fn non_terminal(&self) -> Result<Vec<TaskRow>, AppError> {
        self.list_tasks(&[ProcessStatus::Queued, ProcessStatus::Running], None)
    }

    /// Terminal tasks whose exit callback was never finished.
    pub fn pending_callbacks(&self) -> Result<Vec<TaskRow>, AppError> {
        let mut stmt = self.conn.prepare(&format!(
            "{TASK_SELECT}
             WHERE status IN ('succeeded', 'failed', 'cancelled', 'lost')
               AND callback_status IN ('pending', 'sending')
             ORDER BY id"
        ))?;
        let rows = stmt
            .query_map([], parse_task_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Count of queued or running tasks.
    pub fn in_flight_count(&self) -> Result<usize, AppError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM tasks WHERE status IN ('queued', 'running')",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Compare-and-swap process status. `None` means the CAS did not match.
    pub fn cas_status(
        &self,
        id: TaskId,
        from: ProcessStatus,
        to: ProcessStatus,
    ) -> Result<Option<TaskRow>, AppError> {
        check_status_transition(from, to)?;
        let now = fmt_time(Utc::now());
        let n = self.conn.execute(
            "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE id = ?3 AND status = ?4",
            params![to.as_str(), now, id.to_string(), from.as_str()],
        )?;
        self.row_after_cas(id, n)
    }

    /// CAS status and store an exit reason.
    pub fn cas_exit(
        &self,
        id: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
    ) -> Result<Option<TaskRow>, AppError> {
        let to = ProcessStatus::from(reason);
        check_status_transition(from, to)?;
        let now = fmt_time(Utc::now());
        let reason_json = serde_json::to_string(reason)?;
        let n = self.conn.execute(
            "UPDATE tasks SET status = ?1, exit_reason = ?2, updated_at = ?3
             WHERE id = ?4 AND status = ?5",
            params![to.as_str(), reason_json, now, id.to_string(), from.as_str()],
        )?;
        self.row_after_cas(id, n)
    }

    fn row_after_cas(&self, id: TaskId, updated: usize) -> Result<Option<TaskRow>, AppError> {
        if updated == 1 {
            Ok(Some(self.require_task(id)?))
        } else {
            Ok(None)
        }
    }

    /// Record the worker pid.
    pub fn set_pid(&self, id: TaskId, pid: i32) -> Result<(), AppError> {
        let now = fmt_time(Utc::now());
        self.conn.execute(
            "UPDATE tasks SET pid = ?1, updated_at = ?2 WHERE id = ?3",
            params![pid, now, id.to_string()],
        )?;
        Ok(())
    }

    /// Mark cancel requested. Terminal tasks are unchanged (idempotent).
    pub fn request_cancel(&self, id: TaskId) -> Result<CancelResult, AppError> {
        self.immediate(|| {
            self.require_task(id)?;
            self.conn.execute(
                "UPDATE tasks SET cancel_requested_at = ?1, updated_at = ?1
                 WHERE id = ?2 AND status NOT IN ('succeeded', 'failed', 'cancelled', 'lost')",
                params![fmt_time(Utc::now()), id.to_string()],
            )?;
            if let Some(row) = self.cas_exit(id, ProcessStatus::Queued, &ExitReason::Cancelled)? {
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

    /// Run `body` inside `BEGIN IMMEDIATE`, rolling back on error.
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

    /// Claim the exit callback: `pending|sending` → `sending`.
    ///
    /// A live attention claim blocks the terminal callback. Every caller CASes
    /// the row terminal before it claims, so a reminder that has not started
    /// can no longer start, and one that is in flight finishes first. That
    /// orders `TASK_CHECK_DUE` strictly before the terminal event.
    pub fn claim_callback(&self, id: TaskId) -> Result<CallbackClaim, AppError> {
        self.immediate(|| {
            if self.require_task(id)?.attention == AttentionState::Sending {
                Ok(CallbackClaim::WaitForAttention)
            } else if self.conn.execute(
                "UPDATE tasks SET callback_status = 'sending', updated_at = ?1
                 WHERE id = ?2 AND callback_status IN ('pending', 'sending')",
                params![fmt_time(Utc::now()), id.to_string()],
            )? == 1
            {
                Ok(CallbackClaim::Claimed)
            } else {
                Ok(CallbackClaim::NotOurs)
            }
        })
    }

    /// Finish a claimed callback as `sent` or `failed`.
    pub fn finish_callback(&self, id: TaskId, status: CallbackStatus) -> Result<(), AppError> {
        if !matches!(status, CallbackStatus::Sent | CallbackStatus::Failed) {
            return Err(AppError::Internal {
                message: format!("invalid callback finish {status}"),
            });
        }
        let row = self.require_task(id)?;
        if status == CallbackStatus::Sent {
            check_callback_sent(row.status())?;
        }
        let now = fmt_time(Utc::now());
        self.conn.execute(
            "UPDATE tasks SET callback_status = ?1, updated_at = ?2 WHERE id = ?3",
            params![status.as_str(), now, id.to_string()],
        )?;
        Ok(())
    }

    /// Claim the attention reminder: `pending|sending` → `sending`, and only
    /// while the task is non-terminal. A persisted `sending` owner must be
    /// released only after its bounded delivery process can no longer exist.
    /// `false` means the reminder must not be sent.
    pub fn claim_attention(&self, id: TaskId) -> Result<bool, AppError> {
        let n = self.conn.execute(
            "UPDATE tasks SET attention_state = 'sending', updated_at = ?1
             WHERE id = ?2
               AND attention_state = 'pending'
               AND status IN ('queued', 'running')",
            params![fmt_time(Utc::now()), id.to_string()],
        )?;
        Ok(n == 1)
    }

    /// Record a delivered reminder. Only a live claim can finish, and the
    /// timestamp is written only after the queue send succeeded.
    pub fn mark_attention_delivered(&self, id: TaskId) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE tasks SET attention_state = 'delivered',
                 timeout_notified_at = ?1, updated_at = ?1
             WHERE id = ?2 AND attention_state = 'sending'",
            params![fmt_time(Utc::now()), id.to_string()],
        )?;
        Ok(())
    }

    /// Drop a claim that did not deliver, so a later attempt can take it.
    /// Also the release valve for a claim stranded by a dead daemon.
    pub fn release_attention(&self, id: TaskId) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE tasks SET attention_state = 'pending', updated_at = ?1
             WHERE id = ?2 AND attention_state = 'sending'",
            params![fmt_time(Utc::now()), id.to_string()],
        )?;
        Ok(())
    }

    /// Append a report. Enforces cap, summary length, and terminal rejection.
    pub fn append_report(
        &self,
        id: TaskId,
        outcome: ReportOutcome,
        summary: &str,
    ) -> Result<Vec<TaskReport>, AppError> {
        if summary.len() > SUMMARY_MAX_BYTES {
            return Err(AppError::SummaryTooLong { len: summary.len() });
        }
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
        let now = Utc::now();
        self.conn.execute(
            "INSERT INTO reports (task_id, seq, outcome, summary, reported_at, notified_at)
             VALUES (?1,?2,?3,?4,?5,NULL)",
            params![
                id.to_string(),
                seq,
                outcome.as_str(),
                summary,
                fmt_time(now)
            ],
        )?;
        self.reports(id)
    }

    /// Record a successful `--notify` send.
    pub fn mark_notified(&self, id: TaskId, seq: i64) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE reports SET notified_at = ?1 WHERE task_id = ?2 AND seq = ?3",
            params![fmt_time(Utc::now()), id.to_string(), seq],
        )?;
        Ok(())
    }

    /// Reports in seq order.
    pub fn reports(&self, id: TaskId) -> Result<Vec<TaskReport>, AppError> {
        let mut stmt = self.conn.prepare(
            "SELECT seq, outcome, summary, reported_at, notified_at
             FROM reports WHERE task_id = ?1 ORDER BY seq",
        )?;
        let rows = stmt
            .query_map(params![id.to_string()], |row| {
                let seq: i64 = row.get(0)?;
                let outcome: String = row.get(1)?;
                let summary: String = row.get(2)?;
                let reported_at: String = row.get(3)?;
                let notified_at: Option<String> = row.get(4)?;
                Ok((seq, outcome, summary, reported_at, notified_at))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Vec::new();
        for (seq, outcome, summary, reported_at, notified_at) in rows {
            out.push(TaskReport {
                seq,
                outcome: ReportOutcome::from_storage(&outcome)?,
                summary,
                reported_at: parse_time(&reported_at)?,
                notified_at: notified_at.as_deref().map(parse_time).transpose()?,
            });
        }
        Ok(out)
    }
}

/// Outcome of claiming the terminal callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackClaim {
    /// The caller owns delivery and must finish the claim.
    Claimed,
    /// Another sender already owns or finished delivery.
    NotOurs,
    /// An attention reminder is in flight. Delivering now could put
    /// `TASK_CHECK_DUE` after the terminal event, so the caller waits.
    WaitForAttention,
}

/// Result of `request_cancel`.
#[derive(Debug)]
pub enum CancelResult {
    /// Already terminal: no change.
    AlreadyTerminal(TaskRow),
    /// Queued task flipped to Cancelled.
    CancelledQueued(TaskRow),
    /// Running task: caller must SIGTERM the worker group.
    SignalWorker(TaskRow),
}

fn fmt_time(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Whole seconds as decimal text. `u64` is wider than SQLite's INTEGER, and
/// the attention timer has no product maximum.
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
        callback_status,
        attention,
        cancel_requested_at,
        created_at: parse_time(&created_at).map_err(parse_err)?,
        updated_at: parse_time(&updated_at).map_err(parse_err)?,
    })
}

/// Inputs for a newly queued task.
pub struct NewTask {
    /// Task id.
    pub id: TaskId,
    /// Optional submitted name.
    pub name: Option<TaskName>,
    /// Submitting thread.
    pub thread: ThreadId,
    /// Workload configuration.
    pub workload: Workload,
    /// Working directory.
    pub cwd: std::path::PathBuf,
    /// Attention timeout.
    pub timeout: Duration,
    /// Captured env.
    pub env: TaskEnv,
    /// Resolved binary.
    pub binary: std::path::PathBuf,
}

/// Build a queued row for insert.
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
        callback_status: CallbackStatus::Pending,
        attention: AttentionState::Pending,
        cancel_requested_at: None,
        created_at: now,
        updated_at: now,
    }
}

/// JSON view of `exit.json`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExitJson {
    /// Exit reason.
    pub reason: ExitReason,
}

/// Parse `exit.json` if present.
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

/// Write `exit.json` via temp + rename.
pub fn write_exit_json(path: &Path, reason: &ExitReason) -> Result<(), AppError> {
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(&ExitJson {
        reason: reason.clone(),
    })?;
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        Agent, AgentKind, AgentWorkload, AttentionState, TaskWorkload, TransitionError, Workload,
    };
    use crate::invocation::CommandLine;
    use std::str::FromStr;
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

    /// Schema version 1 layout used only to prove the 1→2 migration.
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
    fn migrate_from_empty() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let version: i64 = store
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
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
    fn callback_claim_and_sent() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        assert!(
            store
                .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
                .unwrap()
                .is_some()
        );
        assert_eq!(store.claim_callback(id).unwrap(), CallbackClaim::Claimed);
        store.finish_callback(id, CallbackStatus::Sent).unwrap();
        let row = store.require_task(id).unwrap();
        assert_eq!(row.callback_status, CallbackStatus::Sent);
    }

    #[test]
    fn callback_sent_while_queued_rejected() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        store.claim_callback(id).unwrap();
        let err = store.finish_callback(id, CallbackStatus::Sent).unwrap_err();
        assert_eq!(
            err.to_string(),
            TransitionError::CallbackSentWhileQueued.to_string()
        );
    }

    #[test]
    fn attention_claim_records_the_time_only_after_delivery() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        assert_eq!(
            store.require_task(id).unwrap().attention,
            AttentionState::Pending
        );

        assert!(store.claim_attention(id).unwrap());
        let claimed = store.require_task(id).unwrap();
        assert_eq!(claimed.attention, AttentionState::Sending);
        assert_eq!(claimed.attention.delivered_at(), None);

        store.mark_attention_delivered(id).unwrap();
        let delivered = store.require_task(id).unwrap();
        assert!(delivered.attention.is_delivered());
        assert!(delivered.attention.delivered_at().is_some());

        // one logical reminder: a delivered row can never be claimed again
        assert!(!store.claim_attention(id).unwrap());
    }

    #[test]
    fn attention_release_allows_a_later_retry() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        assert!(store.claim_attention(id).unwrap());
        store.release_attention(id).unwrap();
        assert_eq!(
            store.require_task(id).unwrap().attention,
            AttentionState::Pending
        );
        assert!(store.claim_attention(id).unwrap());
        // one live claim has one owner until delivery or explicit release
        assert!(!store.claim_attention(id).unwrap());
    }

    #[test]
    fn attention_cannot_be_claimed_once_terminal() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        store
            .cas_exit(id, ProcessStatus::Queued, &ExitReason::Cancelled)
            .unwrap();
        assert!(!store.claim_attention(id).unwrap());
        assert_eq!(
            store.require_task(id).unwrap().attention,
            AttentionState::Pending
        );
    }

    #[test]
    fn terminal_callback_waits_for_an_in_flight_attention_send() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&agent_row(id)).unwrap();
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        assert!(store.claim_attention(id).unwrap());

        // the child exits while the reminder is still on the wire
        store
            .cas_exit(id, ProcessStatus::Running, &ExitReason::Exit { code: 0 })
            .unwrap();
        assert_eq!(
            store.claim_callback(id).unwrap(),
            CallbackClaim::WaitForAttention,
            "the terminal event must not overtake an in-flight TASK_CHECK_DUE"
        );

        store.mark_attention_delivered(id).unwrap();
        assert_eq!(store.claim_callback(id).unwrap(), CallbackClaim::Claimed);
        assert_eq!(store.claim_callback(id).unwrap(), CallbackClaim::Claimed);
        store.finish_callback(id, CallbackStatus::Sent).unwrap();
        assert_eq!(store.claim_callback(id).unwrap(), CallbackClaim::NotOurs);
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
