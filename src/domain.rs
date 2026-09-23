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

/// SQLite `user_version`. Earlier databases migrate in place to this version.
pub const SCHEMA_VERSION: i64 = 21;

/// Maximum Unicode scalar values in a submitted task name.
pub const TASK_NAME_MAX_CHARS: usize = 120;

/// Minimum output-inactivity timeout. Values below this are rejected at submit.
pub const MIN_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Default output-inactivity timeout when the submitter omits `timeout`.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3600);

/// Maximum length of one report summary.
pub const SUMMARY_MAX_BYTES: usize = 4096;

/// Maximum number of reports on one task.
pub const REPORTS_MAX: usize = 20;

/// Human-readable task name from submit. Non-unique; `TaskId` is identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct TaskName(String);

impl TaskName {
    /// Trim and validate a submitted name.
    pub fn parse(raw: &str) -> Result<Self, TaskNameError> {
        for ch in raw.chars() {
            if ch == '\n' || ch == '\r' {
                return Err(TaskNameError::LineBreak);
            }
            if ch.is_control() {
                return Err(TaskNameError::Control { ch });
            }
        }

        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(TaskNameError::Blank);
        }
        if trimmed.chars().count() > TASK_NAME_MAX_CHARS {
            return Err(TaskNameError::TooLong {
                len: trimmed.chars().count(),
            });
        }
        Ok(Self(trimmed.to_string()))
    }

    /// Borrow the validated name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for TaskName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for TaskName {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "TaskName".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": TASK_NAME_MAX_CHARS,
            "allOf": [
                { "pattern": "\\S" },
                { "pattern": "^[^\\u0000-\\u001F\\u007F-\\u009F]*$" }
            ],
            "description": "Human-readable task name. Trimmed. Rejects blank names, line breaks, control characters, and names longer than 120 Unicode scalar values. Non-unique."
        })
    }
}

/// Why a submitted task name was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TaskNameError {
    /// Empty after trim.
    #[error("name must not be blank")]
    Blank,
    /// Contains a newline or carriage return.
    #[error("name must not contain line breaks")]
    LineBreak,
    /// Contains a Unicode control character.
    #[error("name must not contain control character U+{:04X}", *.ch as u32)]
    Control {
        /// Offending scalar value.
        ch: char,
    },
    /// Longer than [`TASK_NAME_MAX_CHARS`] scalars after trim.
    #[error("name must be at most {max} characters (got {len})", max = TASK_NAME_MAX_CHARS)]
    TooLong {
        /// Scalar count after trim.
        len: usize,
    },
}

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

/// Identity available while constructing one child invocation.
///
/// Dry runs use [`Self::Preview`] because no task row exists. Workers use
/// [`Self::Actual`] so agent-specific names cannot collide across tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskIdentity {
    /// Deterministic placeholder used by dry-run output.
    Preview,
    /// Persisted task identity used by a worker.
    Actual(TaskId),
}

impl TaskIdentity {
    /// Build the OpenCode primary-agent name for this identity.
    #[must_use]
    pub fn opencode_agent_name(self) -> String {
        format!("homebased-{self}")
    }
}

impl fmt::Display for TaskIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Preview => f.write_str("<task-id>"),
            Self::Actual(id) => id.fmt(f),
        }
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
    /// `opencode run`.
    OpenCode,
}

impl AgentKind {
    /// Every supported agent, in declaration order.
    pub const ALL: [Self; 4] = [Self::Codex, Self::Claude, Self::Grok, Self::OpenCode];

    /// Environment override that pins this agent's binary.
    #[must_use]
    pub fn binary_env(self) -> &'static str {
        match self {
            Self::Codex => "HOMEBASED_CODEX",
            Self::Claude => "HOMEBASED_CLAUDE",
            Self::Grok => "HOMEBASED_GROK",
            Self::OpenCode => "HOMEBASED_OPENCODE",
        }
    }

    /// Default binary name on PATH.
    #[must_use]
    pub fn binary_name(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Grok => "grok",
            Self::OpenCode => "opencode",
        }
    }
}

impl fmt::Display for AgentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.binary_name())
    }
}

/// Agent kind plus an optional model id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Agent {
    /// CLI kind. Serialized as `agent` to match the public workload shape.
    #[serde(rename = "agent")]
    pub kind: AgentKind,
    /// Model id such as `fable`, `grok-4.6`, or `provider/model#variant`. Empty becomes `None`.
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
    /// Worker holds `runner.lock` and the child may be alive.
    Running,
    /// Child exited 0.
    Succeeded,
    /// Child exited non-zero, failed to spawn, or was signalled without cancel.
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
    Exit {
        /// Exit code the process returned.
        code: i32,
    },
    /// Process received a signal.
    Signal {
        /// Signal number that ended the process.
        signal: i32,
    },
    /// Cancelled.
    Cancelled,
    /// `task-run` or the child binary could not start.
    SpawnFailed {
        /// Why the spawn failed.
        message: String,
    },
}

/// Evidence about the child process group owned by one live task-run worker.
/// This is separate from `ExitReason`: a terminal task can still have an
/// unconfirmed process group. It does not cover detached containers or
/// processes in another session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessGroupExitEvidence {
    /// The worker did not confirm that its child process group exited.
    #[default]
    Unconfirmed,
    /// The worker's post-cleanup probe confirmed that its owned process group was gone.
    ConfirmedExited,
    /// The task-run worker did not spawn a child process.
    NoChildSpawned,
}

impl ProcessGroupExitEvidence {
    /// SQLite storage tag.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unconfirmed => "unconfirmed",
            Self::ConfirmedExited => "confirmed_exited",
            Self::NoChildSpawned => "no_child_spawned",
        }
    }

    /// Parse a storage tag. Unknown values remain conservative.
    #[must_use]
    pub fn from_storage(value: Option<&str>) -> Self {
        match value {
            Some("confirmed_exited") => Self::ConfirmedExited,
            Some("no_child_spawned") => Self::NoChildSpawned,
            Some("unconfirmed") | None => Self::Unconfirmed,
            Some(_) => Self::Unconfirmed,
        }
    }
}

/// Compatibility projection of the terminal event's origin-inbox result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CallbackStatus {
    /// No queue attempt is in flight and the terminal event is unsettled.
    Pending,
    /// A queue attempt has been reserved for the terminal event.
    Sending,
    /// `codex queue` succeeded.
    Sent,
    /// The terminal event settled without queue success.
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

/// Origin-inbox status exposed beside a task row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalCallbackProjection {
    /// Status retained by a row that predates typed event ownership.
    Legacy(CallbackStatus),
    /// Status read from this machine's origin inbox.
    OriginInbox(CallbackStatus),
    /// The origin machine owns delivery, so this executor cannot report it.
    NotOwned,
}

/// Attention-reminder delivery state. A third axis, independent of the process
/// status and of the terminal callback: a reminder never changes either one.
/// `Delivered` carries its timestamp, so "delivered with no time" cannot exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttentionState {
    /// No reminder claimed yet.
    Pending,
    /// A sender holds the claim and the queue send is in flight.
    Sending,
    /// `TASK_CHECK_DUE` was delivered.
    Delivered {
        /// When the queue send succeeded.
        at: DateTime<Utc>,
    },
}

impl AttentionState {
    /// SQLite storage tag for the `attention_state` column.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Sending => "sending",
            Self::Delivered { .. } => "delivered",
        }
    }

    /// Rebuild from the `attention_state` and `timeout_notified_at` columns.
    pub fn from_storage(tag: &str, at: Option<DateTime<Utc>>) -> Result<Self, AppError> {
        match (tag, at) {
            ("pending", None) => Ok(Self::Pending),
            ("sending", None) => Ok(Self::Sending),
            ("delivered", Some(at)) => Ok(Self::Delivered { at }),
            (state, timestamp) => Err(AppError::Internal {
                message: format!(
                    "invalid attention state: state={state} notified={}",
                    timestamp.is_some()
                ),
            }),
        }
    }

    /// When the reminder was delivered, if it was.
    #[must_use]
    pub fn delivered_at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Delivered { at } => Some(*at),
            Self::Pending | Self::Sending => None,
        }
    }

    /// Whether `TASK_CHECK_DUE` has already been delivered.
    #[must_use]
    pub fn is_delivered(&self) -> bool {
        matches!(self, Self::Delivered { .. })
    }
}

impl fmt::Display for AttentionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Caller environment captured at submit. Closed like every other socket
/// shape: an unrecognised key is a caller mistake, not data to ignore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// One append-only worker report. Belongs to the supervised task, not only to an agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskReport {
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

/// Persisted agent workload. Prompt bytes live in task evidence files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWorkload {
    /// Agent identity.
    #[serde(flatten)]
    pub agent: Agent,
    /// Extra argv appended after the unattended flags.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
    /// Whether the reporting trailer is fed to the child.
    #[serde(default = "default_true")]
    pub report_trailer: bool,
}

fn default_true() -> bool {
    true
}

/// Persisted task workload: a validated argv.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskWorkload {
    /// Validated command line.
    pub command: crate::invocation::CommandLine,
}

/// Persisted workload discriminant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Workload {
    /// Agent CLI with prompt evidence on disk.
    Agent(AgentWorkload),
    /// Arbitrary non-interactive command.
    Task(TaskWorkload),
}

/// Lifecycle of one task, derived from the `status`, `exit_reason` and `pid`
/// columns. Every terminal state except `Lost` carries the reason it ended, so
/// `status` is never read without the data that explains it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    /// Inserted, worker not yet CAS-marked running.
    Queued,
    /// Worker holds `runner.lock`. `pid` is recorded just after the CAS.
    Running {
        /// Worker pid (also pgid). Display and signalling only.
        pid: Option<i32>,
    },
    /// Worker wrote an exit reason.
    Finished {
        /// Why the process ended.
        reason: ExitReason,
    },
    /// Lock free and no `exit.json`.
    Lost,
}

impl TaskState {
    /// Rebuild the state from the stored columns.
    pub fn from_storage(
        status: ProcessStatus,
        exit_reason: Option<ExitReason>,
        pid: Option<i32>,
    ) -> Result<Self, AppError> {
        match (status, exit_reason) {
            (ProcessStatus::Queued, _) => Ok(Self::Queued),
            (ProcessStatus::Running, _) => Ok(Self::Running { pid }),
            (ProcessStatus::Lost, _) => Ok(Self::Lost),
            (
                ProcessStatus::Succeeded | ProcessStatus::Failed | ProcessStatus::Cancelled,
                Some(reason),
            ) => Ok(Self::Finished { reason }),
            (status, None) => Err(AppError::Internal {
                message: format!("task row is {status} with no exit_reason"),
            }),
        }
    }

    /// Storage tag for the `status` column.
    #[must_use]
    pub fn status(&self) -> ProcessStatus {
        match self {
            Self::Queued => ProcessStatus::Queued,
            Self::Running { .. } => ProcessStatus::Running,
            Self::Finished { reason } => ProcessStatus::from(reason),
            Self::Lost => ProcessStatus::Lost,
        }
    }

    /// Stored exit reason, if the task ended with one.
    #[must_use]
    pub fn exit_reason(&self) -> Option<&ExitReason> {
        match self {
            Self::Finished { reason } => Some(reason),
            Self::Queued | Self::Running { .. } | Self::Lost => None,
        }
    }

    /// Worker pid while running.
    #[must_use]
    pub fn pid(&self) -> Option<i32> {
        match self {
            Self::Running { pid } => *pid,
            Self::Queued | Self::Finished { .. } | Self::Lost => None,
        }
    }

    /// Whether the task can no longer change process status.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Finished { .. } | Self::Lost)
    }
}

/// Persisted task row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRow {
    /// Task id.
    pub id: TaskId,
    /// Submitted name. `None` only for rows stored before name was required.
    pub name: Option<TaskName>,
    /// Submitting Codex thread.
    pub thread: ThreadId,
    /// Workload configuration.
    pub workload: Workload,
    /// Working directory.
    pub cwd: PathBuf,
    /// Output-inactivity timeout. Reminds the submitter; does not kill the child.
    pub timeout: Duration,
    /// Captured caller environment.
    pub env: TaskEnv,
    /// Resolved executable path.
    pub binary: PathBuf,
    /// Process lifecycle.
    pub state: TaskState,
    /// Durable evidence for the task-run worker's child process group.
    pub process_group_exit_evidence: ProcessGroupExitEvidence,
    /// Terminal callback delivery. A second axis: it outlives the process state.
    pub callback_status: CallbackStatus,
    /// Attention-reminder delivery state.
    pub attention: AttentionState,
    /// When cancel was requested.
    pub cancel_requested_at: Option<DateTime<Utc>>,
    /// Insert time.
    pub created_at: DateTime<Utc>,
    /// Last row update.
    pub updated_at: DateTime<Utc>,
}

impl TaskRow {
    /// Storage status of the process.
    #[must_use]
    pub fn status(&self) -> ProcessStatus {
        self.state.status()
    }

    /// Exit reason once known.
    #[must_use]
    pub fn exit_reason(&self) -> Option<&ExitReason> {
        self.state.exit_reason()
    }

    /// Child process-group evidence, unknown until the task reaches a terminal state.
    #[must_use]
    pub fn process_group_exit_evidence(&self) -> ProcessGroupExitEvidence {
        match &self.state {
            TaskState::Finished { .. } => self.process_group_exit_evidence,
            TaskState::Queued | TaskState::Running { .. } | TaskState::Lost => {
                ProcessGroupExitEvidence::Unconfirmed
            }
        }
    }

    /// Worker pid while running.
    #[must_use]
    pub fn pid(&self) -> Option<i32> {
        self.state.pid()
    }

    /// Non-empty label for UI and events: submitted name, else workload fallback.
    #[must_use]
    pub fn display_name(&self) -> String {
        display_name(self.name.as_ref(), &self.workload)
    }
}

/// Non-empty display label from an optional name and workload.
#[must_use]
pub fn display_name(name: Option<&TaskName>, workload: &Workload) -> String {
    if let Some(name) = name {
        return name.as_str().to_string();
    }
    workload_display_name(workload)
}

/// Workload-only fallback used when no submitted name is present.
#[must_use]
pub fn workload_display_name(workload: &Workload) -> String {
    match workload {
        Workload::Agent(agent) => match &agent.agent.model {
            Some(model) => format!("{}/{}", agent.agent.kind, model),
            None => agent.agent.kind.to_string(),
        },
        Workload::Task(task) => {
            let argv = task.command.to_vec();
            let parts: Vec<&str> = argv.iter().take(3).map(String::as_str).collect();
            if argv.len() > 3 {
                format!("{}…", parts.join(" "))
            } else if parts.is_empty() {
                "task".into()
            } else {
                parts.join(" ")
            }
        }
    }
}

/// Illegal transition.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TransitionError {
    /// `Lost → Running` is forbidden.
    #[error("cannot move Lost to Running")]
    LostToRunning,
    /// Reports cannot be appended after the process is terminal.
    #[error("cannot report on terminal task ({status})")]
    ReportOnTerminal {
        /// Terminal status that rejected the report.
        status: ProcessStatus,
    },
    /// Any other illegal process-status pair.
    #[error("illegal status transition {from} -> {to}")]
    IllegalStatus {
        /// Status the task holds.
        from: ProcessStatus,
        /// Status the caller asked for.
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
        Self::Internal {
            message: err.to_string(),
        }
    }
}

impl From<&ExitReason> for ProcessStatus {
    fn from(reason: &ExitReason) -> Self {
        match reason {
            ExitReason::Exit { code: 0 } => Self::Succeeded,
            ExitReason::Cancelled => Self::Cancelled,
            ExitReason::Exit { .. }
            | ExitReason::Signal { .. }
            | ExitReason::SpawnFailed { .. } => Self::Failed,
        }
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
    fn opencode_identity_and_binary_contract() {
        let id: TaskId = "01a0ab97-a7aa-7463-a5b0-8d500e40e431".parse().unwrap();
        assert_eq!(TaskIdentity::Preview.to_string(), "<task-id>");
        assert_eq!(
            TaskIdentity::Preview.opencode_agent_name(),
            "homebased-<task-id>"
        );
        assert_eq!(
            TaskIdentity::Actual(id).opencode_agent_name(),
            "homebased-01a0ab97-a7aa-7463-a5b0-8d500e40e431"
        );
        assert_eq!(AgentKind::OpenCode.binary_env(), "HOMEBASED_OPENCODE");
        assert_eq!(AgentKind::OpenCode.binary_name(), "opencode");
        assert_eq!(AgentKind::OpenCode.to_string(), "opencode");
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
    fn attention_state_rejects_impossible_storage_pairs() {
        let at = Utc::now();
        assert_eq!(
            AttentionState::from_storage("pending", None).unwrap(),
            AttentionState::Pending
        );
        assert_eq!(
            AttentionState::from_storage("sending", None).unwrap(),
            AttentionState::Sending
        );
        assert_eq!(
            AttentionState::from_storage("delivered", Some(at)).unwrap(),
            AttentionState::Delivered { at }
        );
        assert!(AttentionState::from_storage("pending", Some(at)).is_err());
        assert!(AttentionState::from_storage("sending", Some(at)).is_err());
        assert!(AttentionState::from_storage("delivered", None).is_err());
        assert!(AttentionState::from_storage("unknown", None).is_err());
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
    fn state_derives_status_and_rejects_a_terminal_row_without_a_reason() {
        let running = TaskState::from_storage(ProcessStatus::Running, None, Some(42)).unwrap();
        assert_eq!(running.status(), ProcessStatus::Running);
        assert_eq!(running.pid(), Some(42));
        assert_eq!(running.exit_reason(), None);

        let finished = TaskState::from_storage(
            ProcessStatus::Failed,
            Some(ExitReason::Exit { code: 1 }),
            None,
        )
        .unwrap();
        assert_eq!(finished.status(), ProcessStatus::Failed);
        assert_eq!(finished.exit_reason(), Some(&ExitReason::Exit { code: 1 }));

        // Lost is the one terminal state with no exit.json to explain it
        assert_eq!(
            TaskState::from_storage(ProcessStatus::Lost, None, Some(7)).unwrap(),
            TaskState::Lost
        );
        let err = TaskState::from_storage(ProcessStatus::Succeeded, None, None).unwrap_err();
        assert!(matches!(err, AppError::Internal { .. }), "{err:?}");
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

    #[test]
    fn task_name_trims_and_rejects_invalid() {
        assert_eq!(TaskName::parse("  hello  ").unwrap().as_str(), "hello");
        assert_eq!(
            TaskName::parse(&"a".repeat(120))
                .unwrap()
                .as_str()
                .chars()
                .count(),
            120
        );
        assert!(matches!(TaskName::parse("   "), Err(TaskNameError::Blank)));
        assert!(matches!(
            TaskName::parse("a\nb"),
            Err(TaskNameError::LineBreak)
        ));
        assert!(matches!(
            TaskName::parse("a\rb"),
            Err(TaskNameError::LineBreak)
        ));
        assert!(matches!(
            TaskName::parse("a\u{0007}b"),
            Err(TaskNameError::Control { .. })
        ));
        assert!(matches!(
            TaskName::parse("name\n"),
            Err(TaskNameError::LineBreak)
        ));
        assert!(matches!(
            TaskName::parse("\tname"),
            Err(TaskNameError::Control { .. })
        ));
        assert!(matches!(
            TaskName::parse(&"a".repeat(121)),
            Err(TaskNameError::TooLong { len: 121 })
        ));
    }

    #[test]
    fn task_name_serde_round_trips() {
        let name = TaskName::parse("build release").unwrap();
        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, "\"build release\"");
        let back: TaskName = serde_json::from_str(&json).unwrap();
        assert_eq!(back, name);
        assert!(serde_json::from_str::<TaskName>("\"  \"").is_err());
    }

    #[test]
    fn display_name_prefers_submitted_then_workload() {
        let name = TaskName::parse("my job").unwrap();
        let agent = Workload::Agent(AgentWorkload {
            agent: Agent::new(AgentKind::Claude, Some("fable".into())),
            extra_args: vec![],
            report_trailer: true,
        });
        assert_eq!(display_name(Some(&name), &agent), "my job");
        assert_eq!(workload_display_name(&agent), "claude/fable");
        let agent_bare = Workload::Agent(AgentWorkload {
            agent: Agent::new(AgentKind::Grok, None),
            extra_args: vec![],
            report_trailer: true,
        });
        assert_eq!(workload_display_name(&agent_bare), "grok");
        let task = Workload::Task(TaskWorkload {
            command: crate::invocation::CommandLine::try_from_argv(vec![
                "cargo".into(),
                "build".into(),
                "--release".into(),
                "--locked".into(),
            ])
            .unwrap(),
        });
        assert_eq!(workload_display_name(&task), "cargo build --release…");
    }
}
