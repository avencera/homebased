//! SQLite source of truth: migrations, CAS transitions, callback claim.

use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::domain::{
    check_callback_sent, check_report_allowed, check_status_transition, Agent, AgentKind,
    AgentReport, CallbackStatus, ExitReason, ProcessStatus, ReportOutcome, TaskEnv, TaskId,
    TaskRow, ThreadId, REPORTS_MAX, SUMMARY_MAX_BYTES,
};
use crate::error::AppError;

const SCHEMA: &str = r#"
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    agent_kind TEXT NOT NULL,
    model TEXT,
    cwd TEXT NOT NULL,
    timeout_secs INTEGER NOT NULL,
    extra_args TEXT NOT NULL,
    report_trailer INTEGER NOT NULL,
    env_path TEXT NOT NULL,
    env_home TEXT NOT NULL,
    binary TEXT NOT NULL,
    status TEXT NOT NULL,
    exit_reason TEXT,
    callback_status TEXT NOT NULL,
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
"#;

/// Open or create the database.
pub struct Store {
    conn: Connection,
}

impl Store {
    /// Open the SQLite file at `path`, applying migrations.
    pub fn open(path: &Path) -> Result<Self, AppError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        match version {
            0 => {
                conn.execute_batch(SCHEMA)?;
                conn.pragma_update(None, "user_version", 1)?;
            }
            1 => {}
            other => {
                return Err(AppError::Internal {
                    message: format!("unsupported schema user_version={other}"),
                });
            }
        }
        Ok(Self { conn })
    }

    /// Insert a queued task.
    pub fn insert_task(&self, row: &TaskRow) -> Result<(), AppError> {
        self.conn.execute(
            "INSERT INTO tasks (
                id, thread_id, agent_kind, model, cwd, timeout_secs, extra_args,
                report_trailer, env_path, env_home, binary, status, exit_reason,
                callback_status, pid, cancel_requested_at, created_at, updated_at
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
            params![
                row.id.to_string(),
                row.thread.to_string(),
                row.agent.kind.binary_name(),
                row.agent.model,
                row.cwd.to_string_lossy(),
                row.timeout.as_secs() as i64,
                serde_json::to_string(&row.extra_args)?,
                i64::from(row.report_trailer),
                row.env.path,
                row.env.home,
                row.binary.to_string_lossy(),
                row.status.as_str(),
                row.exit_reason
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                row.callback_status.as_str(),
                row.pid,
                row.cancel_requested_at.map(fmt_time),
                fmt_time(row.created_at),
                fmt_time(row.updated_at),
            ],
        )?;
        Ok(())
    }

    /// Fetch one task.
    pub fn get_task(&self, id: TaskId) -> Result<Option<TaskRow>, AppError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, thread_id, agent_kind, model, cwd, timeout_secs, extra_args,
                    report_trailer, env_path, env_home, binary, status, exit_reason,
                    callback_status, pid, cancel_requested_at, created_at, updated_at
             FROM tasks WHERE id = ?1",
        )?;
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
        let mut sql = String::from(
            "SELECT id, thread_id, agent_kind, model, cwd, timeout_secs, extra_args,
                    report_trailer, env_path, env_home, binary, status, exit_reason,
                    callback_status, pid, cancel_requested_at, created_at, updated_at
             FROM tasks WHERE 1=1",
        );
        if !statuses.is_empty() {
            sql.push_str(" AND status IN (");
            for (i, status) in statuses.iter().enumerate() {
                if i > 0 {
                    sql.push(',');
                }
                // status.as_str() is a closed enum, not user text
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

    /// Count of queued or running tasks.
    pub fn in_flight_count(&self) -> Result<usize, AppError> {
        Ok(self.non_terminal()?.len())
    }

    /// Compare-and-swap process status.
    pub fn cas_status(
        &self,
        id: TaskId,
        from: ProcessStatus,
        to: ProcessStatus,
    ) -> Result<bool, AppError> {
        check_status_transition(from, to)?;
        let now = fmt_time(Utc::now());
        let n = self.conn.execute(
            "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE id = ?3 AND status = ?4",
            params![to.as_str(), now, id.to_string(), from.as_str()],
        )?;
        Ok(n == 1)
    }

    /// CAS status and store an exit reason.
    pub fn cas_exit(
        &self,
        id: TaskId,
        from: ProcessStatus,
        to: ProcessStatus,
        reason: &ExitReason,
    ) -> Result<bool, AppError> {
        check_status_transition(from, to)?;
        let now = fmt_time(Utc::now());
        let reason_json = serde_json::to_string(reason)?;
        let n = self.conn.execute(
            "UPDATE tasks SET status = ?1, exit_reason = ?2, updated_at = ?3
             WHERE id = ?4 AND status = ?5",
            params![to.as_str(), reason_json, now, id.to_string(), from.as_str()],
        )?;
        Ok(n == 1)
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
        let row = self.require_task(id)?;
        if row.status.is_terminal() {
            return Ok(CancelResult::AlreadyTerminal(row));
        }
        let now = Utc::now();
        if row.status == ProcessStatus::Queued {
            let _ = self.cas_exit(
                id,
                ProcessStatus::Queued,
                ProcessStatus::Cancelled,
                &ExitReason::Cancelled,
            )?;
            let row = self.require_task(id)?;
            return Ok(CancelResult::CancelledQueued(row));
        }
        self.conn.execute(
            "UPDATE tasks SET cancel_requested_at = ?1, updated_at = ?1 WHERE id = ?2",
            params![fmt_time(now), id.to_string()],
        )?;
        let row = self.require_task(id)?;
        Ok(CancelResult::SignalWorker(row))
    }

    /// Claim the exit callback: `pending|sending` → `sending`.
    pub fn claim_callback(&self, id: TaskId) -> Result<bool, AppError> {
        let now = fmt_time(Utc::now());
        let n = self.conn.execute(
            "UPDATE tasks SET callback_status = 'sending', updated_at = ?1
             WHERE id = ?2 AND callback_status IN ('pending', 'sending')",
            params![now, id.to_string()],
        )?;
        Ok(n == 1)
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
            check_callback_sent(row.status)?;
        }
        let now = fmt_time(Utc::now());
        self.conn.execute(
            "UPDATE tasks SET callback_status = ?1, updated_at = ?2 WHERE id = ?3",
            params![status.as_str(), now, id.to_string()],
        )?;
        Ok(())
    }

    /// Append a report. Enforces cap, summary length, and terminal rejection.
    pub fn append_report(
        &self,
        id: TaskId,
        outcome: ReportOutcome,
        summary: &str,
    ) -> Result<Vec<AgentReport>, AppError> {
        if summary.len() > SUMMARY_MAX_BYTES {
            return Err(AppError::SummaryTooLong { len: summary.len() });
        }
        let row = self.require_task(id)?;
        check_report_allowed(row.status).map_err(|_| AppError::TaskTerminal {
            id,
            status: row.status,
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
    pub fn reports(&self, id: TaskId) -> Result<Vec<AgentReport>, AppError> {
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
            out.push(AgentReport {
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
    let kind: String = row.get(2)?;
    let model: Option<String> = row.get(3)?;
    let cwd: String = row.get(4)?;
    let timeout_secs: i64 = row.get(5)?;
    let extra_args: String = row.get(6)?;
    let report_trailer: i64 = row.get(7)?;
    let env_path: String = row.get(8)?;
    let env_home: String = row.get(9)?;
    let binary: String = row.get(10)?;
    let status: String = row.get(11)?;
    let exit_reason: Option<String> = row.get(12)?;
    let callback_status: String = row.get(13)?;
    let pid: Option<i32> = row.get(14)?;
    let cancel_requested_at: Option<String> = row.get(15)?;
    let created_at: String = row.get(16)?;
    let updated_at: String = row.get(17)?;

    let parse_err = |err: AppError| rusqlite::Error::ToSqlConversionFailure(Box::new(err));

    let id: TaskId = id.parse().map_err(parse_err)?;
    let thread: ThreadId = thread.parse().map_err(parse_err)?;
    let kind = match kind.as_str() {
        "codex" => AgentKind::Codex,
        "claude" => AgentKind::Claude,
        "grok" => AgentKind::Grok,
        other => {
            return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
                AppError::Internal {
                    message: format!("unknown agent {other}"),
                },
            )));
        }
    };
    let extra_args: Vec<String> = serde_json::from_str(&extra_args).map_err(|err| {
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
    let cancel_requested_at = match cancel_requested_at {
        Some(raw) => Some(parse_time(&raw).map_err(parse_err)?),
        None => None,
    };
    Ok(TaskRow {
        id,
        thread,
        agent: Agent::new(kind, model),
        cwd: Path::new(&cwd).to_path_buf(),
        timeout: Duration::from_secs(timeout_secs as u64),
        extra_args,
        report_trailer: report_trailer != 0,
        env: TaskEnv {
            path: env_path,
            home: env_home,
        },
        binary: Path::new(&binary).to_path_buf(),
        status,
        exit_reason,
        callback_status,
        pid,
        cancel_requested_at,
        created_at: parse_time(&created_at).map_err(parse_err)?,
        updated_at: parse_time(&updated_at).map_err(parse_err)?,
    })
}

/// Inputs for a newly queued task.
pub struct NewTask {
    /// Task id.
    pub id: TaskId,
    /// Submitting thread.
    pub thread: ThreadId,
    /// Agent.
    pub agent: Agent,
    /// Working directory.
    pub cwd: std::path::PathBuf,
    /// Timeout.
    pub timeout: Duration,
    /// Extra argv.
    pub extra_args: Vec<String>,
    /// Trailer flag.
    pub report_trailer: bool,
    /// Captured env.
    pub env: TaskEnv,
    /// Resolved binary.
    pub binary: std::path::PathBuf,
}

/// Build a queued row for insert.
pub fn new_queued_task(new: NewTask) -> TaskRow {
    let now = Utc::now();
    TaskRow {
        id: new.id,
        thread: new.thread,
        agent: new.agent,
        cwd: new.cwd,
        timeout: new.timeout,
        extra_args: new.extra_args,
        report_trailer: new.report_trailer,
        env: new.env,
        binary: new.binary,
        status: ProcessStatus::Queued,
        exit_reason: None,
        callback_status: CallbackStatus::Pending,
        pid: None,
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
    use crate::domain::TransitionError;
    use std::str::FromStr;
    use tempfile::tempdir;

    fn sample_row(id: TaskId) -> TaskRow {
        new_queued_task(NewTask {
            id,
            thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
            agent: Agent::new(AgentKind::Claude, Some("fable".into())),
            cwd: Path::new("/tmp").to_path_buf(),
            timeout: Duration::from_secs(3600),
            extra_args: vec!["--verbose".into()],
            report_trailer: true,
            env: TaskEnv {
                path: "/bin".into(),
                home: "/home/u".into(),
            },
            binary: Path::new("/bin/true").to_path_buf(),
        })
    }

    #[test]
    fn migrate_from_empty() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let version: i64 = store
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 1);
    }

    #[test]
    fn insert_list_get() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&sample_row(id)).unwrap();
        let got = store.require_task(id).unwrap();
        assert_eq!(got.agent.kind, AgentKind::Claude);
        assert_eq!(got.status, ProcessStatus::Queued);
        let listed = store.list_tasks(&[ProcessStatus::Queued], None).unwrap();
        assert_eq!(listed.len(), 1);
        let none = store.list_tasks(&[ProcessStatus::Running], None).unwrap();
        assert!(none.is_empty());
        let by_thread = store
            .list_tasks(
                &[],
                Some(ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap()),
            )
            .unwrap();
        assert_eq!(by_thread.len(), 1);
    }

    #[test]
    fn cas_rejects_lost_to_running() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&sample_row(id)).unwrap();
        assert!(store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Lost)
            .unwrap());
        let err = store
            .cas_status(id, ProcessStatus::Lost, ProcessStatus::Running)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Lost") || matches!(err, AppError::Internal { .. }));
        let _ = TransitionError::LostToRunning;
    }

    #[test]
    fn callback_claim_and_sent() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&sample_row(id)).unwrap();
        assert!(store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap());
        assert!(store.claim_callback(id).unwrap());
        store.finish_callback(id, CallbackStatus::Sent).unwrap();
        let row = store.require_task(id).unwrap();
        assert_eq!(row.callback_status, CallbackStatus::Sent);
    }

    #[test]
    fn callback_sent_while_queued_rejected() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&sample_row(id)).unwrap();
        store.claim_callback(id).unwrap();
        let err = store.finish_callback(id, CallbackStatus::Sent).unwrap_err();
        assert!(err.to_string().contains("Queued") || err.to_string().contains("callback"));
    }

    #[test]
    fn report_append_order_and_cap() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&sample_row(id)).unwrap();
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
        store.insert_task(&sample_row(id)).unwrap();
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
        store.insert_task(&sample_row(id)).unwrap();
        store
            .cas_exit(
                id,
                ProcessStatus::Queued,
                ProcessStatus::Cancelled,
                &ExitReason::Cancelled,
            )
            .unwrap();
        let err = store
            .append_report(id, ReportOutcome::Succeeded, "late")
            .unwrap_err();
        assert!(matches!(err, AppError::TaskTerminal { .. }));
    }

    #[test]
    fn recover_lost() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let id = TaskId::new();
        store.insert_task(&sample_row(id)).unwrap();
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        assert!(store
            .cas_status(id, ProcessStatus::Running, ProcessStatus::Lost)
            .unwrap());
        assert_eq!(store.require_task(id).unwrap().status, ProcessStatus::Lost);
    }
}
