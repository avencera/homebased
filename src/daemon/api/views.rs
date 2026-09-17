//! Typed JSON bodies shared by the socket API, the CLI, and the dashboard.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::callback::{HomebasedEvent, WorkloadView};
use crate::domain::{
    API_VERSION, CallbackStatus, ExitReason, ProcessStatus, TaskId, TaskReport, TaskRow, ThreadId,
};

/// `GET /v1/status`.
#[derive(Debug, Clone, Serialize)]
pub struct StatusBody {
    /// Schema version.
    pub api_version: u32,
    /// Crate version of the running daemon.
    pub version: &'static str,
    /// Daemon pid.
    pub pid: u32,
    /// Unix socket path.
    pub socket: String,
    /// Dashboard base URL, or `None` when the TCP listener is off or failed to bind.
    pub web: Option<String>,
    /// Queued plus running tasks.
    pub in_flight: usize,
}

/// Attention-reminder state for the check timeout. Public and two-valued: a
/// send that is still in flight has not been delivered, so it reads `pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckTimeoutStatus {
    /// Reminder not yet delivered.
    Pending,
    /// Reminder delivered successfully.
    Sent,
}

/// One task in `GET /v1/tasks` and the head of `GET /v1/tasks/{id}`.
#[derive(Debug, Clone, Serialize)]
pub struct TaskSummary {
    /// Task id.
    pub id: TaskId,
    /// Process status.
    pub status: ProcessStatus,
    /// Workload view.
    pub workload: WorkloadView,
    /// Submitting Codex thread.
    pub thread: ThreadId,
    /// Working directory.
    pub cwd: PathBuf,
    /// Worker pid while running. Display only.
    pub pid: Option<i32>,
    /// Callback delivery state.
    pub callback: CallbackStatus,
    /// Attention timer in seconds.
    pub timeout_secs: u64,
    /// Whether the attention reminder is pending or sent.
    pub check_timeout: CheckTimeoutStatus,
    /// Why the process ended, once known.
    pub exit_reason: Option<ExitReason>,
    /// When cancel was requested.
    pub cancel_requested_at: Option<DateTime<Utc>>,
    /// Insert time.
    pub created_at: DateTime<Utc>,
    /// Last row update. For a terminal task this is the finish time.
    pub updated_at: DateTime<Utc>,
}

impl From<&TaskRow> for TaskSummary {
    fn from(row: &TaskRow) -> Self {
        Self {
            id: row.id,
            status: row.status(),
            workload: WorkloadView::from(&row.workload),
            thread: row.thread,
            cwd: row.cwd.clone(),
            pid: row.pid(),
            callback: row.callback_status,
            timeout_secs: row.timeout.as_secs(),
            check_timeout: if row.attention.is_delivered() {
                CheckTimeoutStatus::Sent
            } else {
                CheckTimeoutStatus::Pending
            },
            exit_reason: row.exit_reason().cloned(),
            cancel_requested_at: row.cancel_requested_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

/// `GET /v1/tasks`.
#[derive(Debug, Clone, Serialize)]
pub struct TaskList {
    /// Schema version.
    pub api_version: u32,
    /// Tasks in id order, which is creation order for UUID v7.
    pub tasks: Vec<TaskSummary>,
}

impl TaskList {
    /// Build from store rows.
    #[must_use]
    pub fn from_rows(rows: &[TaskRow]) -> Self {
        Self {
            api_version: API_VERSION,
            tasks: rows.iter().map(TaskSummary::from).collect(),
        }
    }
}

/// `GET /v1/tasks/{id}`.
#[derive(Debug, Clone, Serialize)]
pub struct TaskDetail {
    /// Schema version.
    pub api_version: u32,
    /// Fields shared with the list.
    #[serde(flatten)]
    pub summary: TaskSummary,
    /// Worker reports in seq order.
    pub reports: Vec<TaskReport>,
    /// Combined stdout and stderr of the child.
    pub output_log: PathBuf,
    /// Task directory.
    pub evidence: PathBuf,
    /// Event already sent, or the one that will be sent. `None` while running
    /// with no interim event.
    pub last_event: Option<HomebasedEvent>,
}

/// `GET /v1/tasks/{id}/log`.
#[derive(Debug, Clone, Serialize)]
pub struct LogTail {
    /// Schema version.
    pub api_version: u32,
    /// Task id.
    pub id: TaskId,
    /// Log text. Empty while the child has written nothing.
    pub log: String,
    /// Whether earlier lines were dropped by `tail`.
    pub truncated: bool,
}
