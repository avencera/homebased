//! Domain types and legal state transitions.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::AppError;

/// Public JSON schema version.
pub const API_VERSION: u32 = 1;

/// Maximum length of one report summary.
pub const SUMMARY_MAX_BYTES: usize = 4096;

/// Maximum number of reports on one task.
pub const REPORTS_MAX: usize = 20;

/// Codex thread that submitted the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ThreadId(pub Uuid);

impl fmt::Display for ThreadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ThreadId {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::parse_str(s).map_err(|_| AppError::Usage {
            message: format!("invalid thread id (full UUID required): {s}"),
        })?;
        Ok(Self(uuid))
    }
}

impl schemars::JsonSchema for ThreadId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ThreadId".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "format": "uuid"
        })
    }
}

/// Task identifier (UUID v7 at creation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub Uuid);

impl TaskId {
    /// Allocate a new v7 id.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TaskId {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::parse_str(s).map_err(|_| AppError::Usage {
            message: format!("invalid task id (full UUID required): {s}"),
        })?;
        Ok(Self(uuid))
    }
}

impl schemars::JsonSchema for TaskId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "TaskId".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "format": "uuid"
        })
    }
}

/// Supported agent CLIs.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    clap::ValueEnum,
    schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum AgentKind {
    /// `codex exec`.
    Codex,
    /// `claude -p`.
    Claude,
    /// `grok` full agent.
    Grok,
}

impl AgentKind {
    /// Environment override that pins this agent's binary.
    #[must_use]
    pub fn binary_env(self) -> &'static str {
        match self {
            Self::Codex => "HOMEBASED_CODEX",
            Self::Claude => "HOMEBASED_CLAUDE",
            Self::Grok => "HOMEBASED_GROK",
        }
    }

    /// Default binary name on PATH.
    #[must_use]
    pub fn binary_name(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Grok => "grok",
        }
    }
}

impl fmt::Display for AgentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.binary_name())
    }
}

/// Agent kind plus optional model alias.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Agent {
    /// CLI kind.
    pub kind: AgentKind,
    /// Model alias such as `fable` or `grok-4.6`. Empty becomes `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

impl Agent {
    /// Build an agent, treating a blank model as unset.
    #[must_use]
    pub fn new(kind: AgentKind, model: Option<String>) -> Self {
        let model = model.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });
        Self { kind, model }
    }
}

/// Process lifecycle.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    clap::ValueEnum,
    schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum ProcessStatus {
    /// Inserted, worker not yet CAS-marked running.
    Queued,
    /// Worker holds `runner.lock` and the agent may be alive.
    Running,
    /// Agent exited 0.
    Succeeded,
    /// Agent exited non-zero, timed out, or failed to spawn.
    Failed,
    /// Cancelled by `task cancel` or `stop --yes`.
    Cancelled,
    /// Lock free and no `exit.json`.
    Lost,
}

impl ProcessStatus {
    /// Whether this status is terminal.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Lost
        )
    }

    /// SQLite storage tag.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Lost => "lost",
        }
    }

    /// Parse a storage tag.
    pub fn from_storage(value: &str) -> Result<Self, AppError> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "lost" => Ok(Self::Lost),
            other => Err(AppError::Internal {
                message: format!("unknown process status: {other}"),
            }),
        }
    }
}

impl fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why the process ended. `runner_lost` is a process payload, not a stored reason
/// on a live worker: Lost tasks have no `exit.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExitReason {
    /// Process exited.
    Exit { code: i32 },
    /// Process received a signal.
    Signal { signal: i32 },
    /// Wall-clock timeout.
    Timeout { secs: u64 },
    /// Cancelled.
    Cancelled,
    /// `task-run` or the agent binary could not start.
    SpawnFailed { message: String },
}

/// Callback delivery state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CallbackStatus {
    /// Not yet claimed.
    Pending,
    /// A sender has claimed the row.
    Sending,
    /// `codex queue` succeeded.
    Sent,
    /// All attempts failed.
    Failed,
}

impl CallbackStatus {
    /// SQLite storage tag.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Sending => "sending",
            Self::Sent => "sent",
            Self::Failed => "failed",
        }
    }

    /// Parse a storage tag.
    pub fn from_storage(value: &str) -> Result<Self, AppError> {
        match value {
            "pending" => Ok(Self::Pending),
            "sending" => Ok(Self::Sending),
            "sent" => Ok(Self::Sent),
            "failed" => Ok(Self::Failed),
            other => Err(AppError::Internal {
                message: format!("unknown callback status: {other}"),
            }),
        }
    }
}

impl fmt::Display for CallbackStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Caller environment captured at submit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEnv {
    /// Caller's `PATH`.
    pub path: String,
    /// Caller's `HOME`.
    pub home: String,
}

impl TaskEnv {
    /// Capture from this process.
    #[must_use]
    pub fn capture() -> Self {
        Self {
            path: std::env::var("PATH").unwrap_or_default(),
            home: std::env::var("HOME").unwrap_or_default(),
        }
    }
}

/// Worker-authored outcome.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum ReportOutcome {
    /// Work completed.
    Succeeded,
    /// Work failed.
    Failed,
    /// Worker needs a decision.
    Blocked,
}

impl ReportOutcome {
    /// SQLite storage tag.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
        }
    }

    /// Parse a storage tag.
    pub fn from_storage(value: &str) -> Result<Self, AppError> {
        match value {
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "blocked" => Ok(Self::Blocked),
            other => Err(AppError::Internal {
                message: format!("unknown report outcome: {other}"),
            }),
        }
    }
}

impl fmt::Display for ReportOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One append-only worker report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentReport {
    /// 1-based sequence.
    pub seq: i64,
    /// Worker outcome.
    pub outcome: ReportOutcome,
    /// Summary text, at most 4 KiB.
    pub summary: String,
    /// When the row was appended.
    pub reported_at: DateTime<Utc>,
    /// When an interim `--notify` send succeeded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notified_at: Option<DateTime<Utc>>,
}

/// Persisted task row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRow {
    /// Task id.
    pub id: TaskId,
    /// Submitting Codex thread.
    pub thread: ThreadId,
    /// Agent identity.
    pub agent: Agent,
    /// Working directory.
    pub cwd: PathBuf,
    /// Wall-clock timeout.
    pub timeout: Duration,
    /// Extra argv appended to the agent.
    pub extra_args: Vec<String>,
    /// Whether the reporting trailer is fed to the child.
    pub report_trailer: bool,
    /// Captured caller environment.
    pub env: TaskEnv,
    /// Resolved agent binary.
    pub binary: PathBuf,
    /// Process status.
    pub status: ProcessStatus,
    /// Exit reason once known.
    pub exit_reason: Option<ExitReason>,
    /// Callback delivery.
    pub callback_status: CallbackStatus,
    /// Worker pid (also pgid). Display and signalling only.
    pub pid: Option<i32>,
    /// When cancel was requested.
    pub cancel_requested_at: Option<DateTime<Utc>>,
    /// Insert time.
    pub created_at: DateTime<Utc>,
    /// Last row update.
    pub updated_at: DateTime<Utc>,
}

/// Illegal transition.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TransitionError {
    /// `Lost → Running` is forbidden.
    #[error("cannot move Lost to Running")]
    LostToRunning,
    /// Callback cannot be `Sent` while the task is still `Queued`.
    #[error("cannot mark callback Sent while Queued")]
    CallbackSentWhileQueued,
    /// Reports cannot be appended after the process is terminal.
    #[error("cannot report on terminal task ({status})")]
    ReportOnTerminal { status: ProcessStatus },
    /// Any other illegal process-status pair.
    #[error("illegal status transition {from} -> {to}")]
    IllegalStatus {
        from: ProcessStatus,
        to: ProcessStatus,
    },
}

/// Check a process-status compare-and-swap.
pub fn check_status_transition(
    from: ProcessStatus,
    to: ProcessStatus,
) -> Result<(), TransitionError> {
    if from == ProcessStatus::Lost && to == ProcessStatus::Running {
        return Err(TransitionError::LostToRunning);
    }
    let allowed = matches!(
        (from, to),
        (
            ProcessStatus::Queued,
            ProcessStatus::Running
                | ProcessStatus::Failed
                | ProcessStatus::Cancelled
                | ProcessStatus::Lost
        ) | (
            ProcessStatus::Running,
            ProcessStatus::Succeeded
                | ProcessStatus::Failed
                | ProcessStatus::Cancelled
                | ProcessStatus::Lost
        )
    );
    if allowed {
        Ok(())
    } else {
        Err(TransitionError::IllegalStatus { from, to })
    }
}

/// Reject callback `Sent` while still `Queued`.
pub fn check_callback_sent(status: ProcessStatus) -> Result<(), TransitionError> {
    if status == ProcessStatus::Queued {
        Err(TransitionError::CallbackSentWhileQueued)
    } else {
        Ok(())
    }
}

/// Reject a report on a terminal task.
pub fn check_report_allowed(status: ProcessStatus) -> Result<(), TransitionError> {
    if status.is_terminal() {
        Err(TransitionError::ReportOnTerminal { status })
    } else {
        Ok(())
    }
}

impl From<TransitionError> for AppError {
    fn from(err: TransitionError) -> Self {
        match err {
            TransitionError::ReportOnTerminal { status } => Self::Internal {
                message: format!("report on terminal task ({status})"),
            },
            other => Self::Internal {
                message: other.to_string(),
            },
        }
    }
}

/// Map an agent wait result to process status and reason.
#[must_use]
pub fn status_from_exit(reason: &ExitReason) -> ProcessStatus {
    match reason {
        ExitReason::Exit { code: 0 } => ProcessStatus::Succeeded,
        ExitReason::Cancelled => ProcessStatus::Cancelled,
        ExitReason::Exit { .. }
        | ExitReason::Signal { .. }
        | ExitReason::Timeout { .. }
        | ExitReason::SpawnFailed { .. } => ProcessStatus::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_id_parse_rejects_prefix() {
        let err = ThreadId::from_str("01a0ab97").unwrap_err();
        assert!(matches!(err, AppError::Usage { .. }));
    }

    #[test]
    fn thread_id_parse_full_uuid() {
        let id = ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap();
        assert_eq!(id.to_string(), "01a0ab97-a7aa-7463-a5b0-8d500e40e431");
    }

    #[test]
    fn task_id_is_uuid_v7() {
        let id = TaskId::new();
        assert_eq!(id.0.get_version(), Some(uuid::Version::SortRand));
    }

    #[test]
    fn empty_model_becomes_none() {
        let agent = Agent::new(AgentKind::Claude, Some("  ".into()));
        assert_eq!(agent.model, None);
        let agent = Agent::new(AgentKind::Claude, Some("fable".into()));
        assert_eq!(agent.model.as_deref(), Some("fable"));
    }

    #[test]
    fn lost_to_running_rejected() {
        let err = check_status_transition(ProcessStatus::Lost, ProcessStatus::Running).unwrap_err();
        assert_eq!(err, TransitionError::LostToRunning);
    }

    #[test]
    fn queued_to_running_ok() {
        check_status_transition(ProcessStatus::Queued, ProcessStatus::Running).unwrap();
    }

    #[test]
    fn succeeded_to_running_rejected() {
        assert!(check_status_transition(ProcessStatus::Succeeded, ProcessStatus::Running).is_err());
    }

    #[test]
    fn callback_sent_while_queued_rejected() {
        let err = check_callback_sent(ProcessStatus::Queued).unwrap_err();
        assert_eq!(err, TransitionError::CallbackSentWhileQueued);
        check_callback_sent(ProcessStatus::Running).unwrap();
    }

    #[test]
    fn report_on_terminal_rejected() {
        let err = check_report_allowed(ProcessStatus::Succeeded).unwrap_err();
        assert!(matches!(
            err,
            TransitionError::ReportOnTerminal {
                status: ProcessStatus::Succeeded
            }
        ));
        check_report_allowed(ProcessStatus::Running).unwrap();
    }

    #[test]
    fn terminal_statuses() {
        assert!(!ProcessStatus::Queued.is_terminal());
        assert!(!ProcessStatus::Running.is_terminal());
        assert!(ProcessStatus::Succeeded.is_terminal());
        assert!(ProcessStatus::Failed.is_terminal());
        assert!(ProcessStatus::Cancelled.is_terminal());
        assert!(ProcessStatus::Lost.is_terminal());
    }
}
