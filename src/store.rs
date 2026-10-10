//! SQLite source of truth: task state, sequenced events, and callback results

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, TransactionBehavior};

use crate::domain::{
    ContainerExitEvidence, ExitReason, ProcessGroupExitEvidence, TaskExitEvidence,
};
use crate::error::AppError;

mod admission;
mod cancellation;
mod chain;
mod container;
mod dependency;
mod events;
mod identity;
mod lifecycle;
mod message;
mod presentation;
pub mod queue;
mod schema;
mod task;
#[cfg(test)]
mod tests;
mod usage;

pub use admission::LocalAdmission;
pub use container::TaskContainerRecord;
pub use dependency::{HeldCancel, UnlaunchedTask};
pub(crate) use events::EventRetentionBatch;
pub use identity::IdentityError;
pub use lifecycle::CancelResult;
pub use presentation::{TaskOwners, TaskPresentation};
pub use task::{NewTask, new_queued_task};

/// Open or create the database
pub struct Store {
    conn: Connection,
    tasks_dir: PathBuf,
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
pub fn write_exit_json(
    path: &Path,
    reason: &ExitReason,
    evidence: impl Into<TaskExitEvidence>,
) -> Result<(), AppError> {
    let evidence = evidence.into();
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(&ExitJson {
        reason: reason.clone(),
        process_group_exit_evidence: evidence.process_group,
        container_exit_evidence: evidence.container,
    })?;
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}
