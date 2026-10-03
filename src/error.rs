//! CLI and HTTP error codes

use crate::message::MessageId;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

use serde_json::{Value, json};

use crate::callback::destination::ThreadSuggestion;
use crate::dependency::DependencyOutcome;
use crate::domain::{AgentKind, ProcessStatus, TaskId, ThreadId};
use crate::fleet::protocol::ProtocolRange;
use crate::machine::{MachineId, MachineName};
use crate::queue::QueueError;
use crate::spec::CwdProblem;
use crate::submission::RequestId;

/// Hint for a `cwd` that names a path inside the container instead of a host path
fn cwd_suggestion(path: &std::path::Path, suggested: Option<&std::path::Path>) -> String {
    suggested.map_or_else(String::new, |source| {
        format!(
            "; cwd is a host path, and {} is inside a container mount target, so use the \
             mount source path {}",
            path.display(),
            source.display()
        )
    })
}

/// Application error with a stable machine-readable code
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// Daemon socket is missing or not accepting connections
    #[error("{message}")]
    DaemonUnavailable {
        /// Why the socket could not be reached
        message: String,
    },
    /// No task row for this id
    #[error("task not found: {id}")]
    TaskNotFound {
        /// Id that had no row
        id: TaskId,
    },
    /// Known peers could not all be checked for this task
    #[error("cluster lookup incomplete for {task}")]
    ClusterLookupIncomplete {
        /// Task being inspected
        task: TaskId,
        /// Machine UUIDs without a definitive local-record answer
        unchecked: Vec<MachineId>,
    },
    /// Task evidence is not available from its execution owner
    #[error("task {task} detail or log unavailable on {machine}")]
    TaskUnavailable {
        /// Task being inspected
        task: TaskId,
        /// Execution owner
        machine: MachineId,
    },
    /// This task UUID was rejected before a child started
    #[error("task not started: {task}")]
    TaskNotStarted {
        /// Task that did not start
        task: TaskId,
    },
    /// A task cannot be resumed as a Codex worker
    #[error("task {task} cannot be followed up: {reason}")]
    FollowupUnavailable {
        /// Source task
        task: TaskId,
        /// Why this task cannot be resumed
        reason: FollowupBlocker,
    },
    /// A task's worker cannot take a direct message
    #[error("the worker for task {task} cannot take a message: {reason}")]
    WorkerMessageUnavailable {
        /// Task named by `message send --worker`
        task: TaskId,
        /// Why its worker cannot be addressed
        reason: WorkerMessageBlocker,
    },
    /// A Codex thread already has an active resume task
    #[error("thread {thread} is already being resumed by task {task}; wait for its event")]
    ResumeThreadBusy {
        /// Codex thread requested by the new task
        thread: ThreadId,
        /// Active task already resuming the thread
        task: TaskId,
    },
    /// Follow-up was requested on a machine that does not own the task
    #[error(
        "task {task} must be followed up on its origin machine {origin_machine} or execution machine {execution_machine}; run `homebased task followup` on either machine"
    )]
    FollowupWrongMachine {
        /// Source task
        task: TaskId,
        /// Machine that owns callbacks for the source task
        origin_machine: MachineId,
        /// Machine that ran the source task
        execution_machine: MachineId,
    },
    /// An `after` entry names no task that this daemon is the origin for
    #[error(
        "dependency {task} is not a task submitted through this machine; after may name only tasks this daemon is the origin for"
    )]
    UnknownDependency {
        /// Dependency without an origin route here
        task: TaskId,
    },
    /// An `after` entry already ended without success, so the task could never start
    #[error(
        "dependency {task} already ended without success (outcome {outcome}); the task would never start"
    )]
    DependencyFailed {
        /// Dependency that ended
        task: TaskId,
        /// How it ended, `unknown` when no record says
        outcome: DependencyOutcome,
    },
    /// `cwd` is not an existing, accessible host directory on the machine that runs the task
    #[error(
        "invalid cwd {}: it {} on the machine that runs the task{}",
        path.display(),
        problem.describe(),
        cwd_suggestion(path, suggested_cwd.as_deref())
    )]
    InvalidCwd {
        /// Directory the spec asked for
        path: PathBuf,
        /// Why the directory cannot be used
        problem: CwdProblem,
        /// Host path of the container mount whose target contains `path`
        suggested_cwd: Option<PathBuf>,
    },
    /// Requested program is missing, not a file, or not executable
    #[error("executable missing: {program}")]
    ExecutableMissing {
        /// Program name or path the caller requested
        program: String,
    },
    /// Report summary exceeds 4 KiB
    #[error("summary too long: {len} bytes")]
    SummaryTooLong {
        /// Summary length in bytes
        len: usize,
    },
    /// Task already has 20 reports
    #[error("too many reports: {count}")]
    TooManyReports {
        /// Reports already stored for the task
        count: usize,
    },
    /// Report or mutate attempted on a terminal task
    #[error("task {id} is terminal ({status})")]
    TaskTerminal {
        /// Task that is already finished
        id: TaskId,
        /// Terminal status the task holds
        status: ProcessStatus,
    },
    /// Submit spec failed validation
    #[error("{message}")]
    InvalidSpec {
        /// JSON pointer to the offending key
        pointer: String,
        /// Value found at `pointer`
        value: Value,
        /// Why the value was rejected
        message: String,
    },
    /// Spec `thread` names no session that can receive events on this machine
    #[error("{message}")]
    UnknownThread {
        /// Thread the spec named
        thread: ThreadId,
        /// Known threads that differ from `thread` by a likely typo, best first
        suggestions: Vec<ThreadSuggestion>,
        /// Why the thread was rejected and what was likely intended
        message: String,
    },
    /// A worker's spec names a thread other than its parent task's thread
    #[error(
        "spec field thread {thread} differs from thread {parent_thread} of parent task {parent_task}; use {parent_thread}, or pass --allow-other-thread when the events must go to another session"
    )]
    ThreadMismatch {
        /// Thread the spec named
        thread: ThreadId,
        /// Worker task from `HOMEBASED_TASK_ID`
        parent_task: TaskId,
        /// Thread that receives the parent task's events
        parent_thread: ThreadId,
    },
    /// Agent-specific child configuration could not be built safely
    #[error("{agent} child configuration: {message}")]
    AgentConfiguration {
        /// Agent whose child environment or command was invalid
        agent: AgentKind,
        /// Safe configuration failure detail
        message: String,
    },
    /// Another serve process holds `daemon.lock`
    #[error("daemon already running")]
    DaemonAlreadyRunning,
    /// A non-blocking `flock` found the file locked by another process
    #[error("lock held: {}", path.display())]
    LockHeld {
        /// Lock file another process holds
        path: PathBuf,
    },
    /// Stop or uninstall without `--yes` while tasks are queued or running
    #[error("{count} task(s) in flight")]
    TasksInFlight {
        /// Queued or running tasks that block the operation
        count: usize,
    },
    /// Generated host unit failed `systemd-analyze` or `plutil`
    #[error("unit invalid: {message}")]
    UnitInvalid {
        /// Validator output
        message: String,
    },
    /// Uninstall refused because the host unit belongs to another home
    #[error(
        "host unit belongs to another home (selected {}, configured {})",
        selected.display(),
        configured.display()
    )]
    HostUnitHomeMismatch {
        /// Home selected by this command
        selected: PathBuf,
        /// Home recorded in the installed unit
        configured: PathBuf,
    },
    /// Usage error (conflicting flags, bad UUID)
    #[error("{message}")]
    Usage {
        /// What the caller got wrong
        message: String,
    },
    /// Permission denied
    #[error("{message}")]
    Permission {
        /// Operation that was denied
        message: String,
    },
    /// Filesystem path is missing
    #[error("{message}")]
    FileNotFound {
        /// Human-readable failure
        message: String,
    },
    /// Path exists but is not a directory when one was required
    #[error("{message}")]
    NotDirectory {
        /// Human-readable failure
        message: String,
    },
    /// Directory or file changed while it was being read
    #[error("{message}")]
    ChangedDuringRead {
        /// Human-readable failure
        message: String,
    },
    /// Special file (device, socket, FIFO) is not browsable
    #[error("{message}")]
    UnsupportedFile {
        /// Human-readable failure
        message: String,
    },
    /// Too many concurrent content streams
    #[error("{message}")]
    StreamLimit {
        /// Human-readable failure
        message: String,
    },
    /// `config.toml` is missing, unreadable, or fails validation
    #[error("invalid config {}: {message}", path.display())]
    ConfigInvalid {
        /// Config file path
        path: PathBuf,
        /// Why the file was rejected
        message: String,
    },
    /// Push notification support is not configured
    #[error("ntfy is not configured; add a [notify.ntfy] section to config.toml")]
    NotifyNotConfigured,
    /// An ntfy push could not be sent
    #[error("ntfy notification failed: {message}")]
    NotifyFailed {
        /// Safe summary of the send failure
        message: String,
    },
    /// No known machine matches this name or UUID
    #[error("machine not found: {machine}")]
    MachineNotFound {
        /// Name or UUID the caller gave
        machine: String,
    },
    /// An address answered for another installation than the request named
    #[error(
        "machine identity mismatch: expected {expected}, found {}",
        found.map_or_else(|| "no machine".to_string(), |id| id.to_string())
    )]
    MachineIdentityMismatch {
        /// Destination machine UUID the request carried
        expected: MachineId,
        /// Machine UUID that answered, when known
        found: Option<MachineId>,
    },
    /// Distinct live daemons claim one machine UUID
    #[error("duplicate machine identity: {machine}")]
    DuplicateMachineIdentity {
        /// Conflicting machine UUID
        machine: MachineId,
    },
    /// More than one machine uses one name
    #[error("duplicate machine name: {name}")]
    DuplicateMachineName {
        /// Conflicting name
        name: MachineName,
        /// Machines that claim it
        machines: Vec<MachineId>,
    },
    /// Known machine that did not answer
    #[error("machine {machine} unavailable: {message}")]
    MachineUnavailable {
        /// Machine that did not answer
        machine: MachineId,
        /// Last failure
        message: String,
    },
    /// Remote execution cannot start until its event route is available
    #[error("remote submission is unavailable: {message}")]
    RemoteSubmissionUnavailable {
        /// Why this daemon cannot start the task now
        message: String,
    },
    /// The executor may have accepted this task; retry the same request UUID
    #[error("submission outcome unknown for request {} task {task}: {message}", request.0)]
    SubmissionOutcomeUnknown {
        /// Caller retry identity
        request: RequestId,
        /// Allocated global task identity
        task: TaskId,
        /// Why a definitive identity could not be obtained
        message: String,
    },
    /// The executor durably refused this identity
    #[error("submission rejected for request {} task {task}: {reason}", request.0)]
    SubmissionRejected {
        /// Caller retry identity
        request: RequestId,
        /// Allocated global task identity
        task: TaskId,
        /// Retained tombstone reason
        reason: String,
    },
    /// A caller reused one request UUID for changed content or ownership
    #[error("submission conflict for request {} task {task}: {message}", request.0)]
    SubmissionConflict {
        /// Caller retry identity
        request: RequestId,
        /// Original global task identity
        task: TaskId,
        /// Conflict detail
        message: String,
    },
    /// A verified origin machine has no saved route for this task
    #[error("origin route not found: {task}")]
    RouteNotFound {
        /// Task with no origin route
        task: TaskId,
    },
    /// An event names the wrong task owner
    #[error("cluster task conflict: {task}")]
    ClusterTaskConflict {
        /// Task with conflicting ownership
        task: TaskId,
    },
    /// A previously accepted event sequence has different content
    #[error("event content conflict: {task} sequence {seq}")]
    EventContentConflict {
        /// Task with conflicting event content
        task: TaskId,
        /// Conflicting sequence
        seq: u64,
    },
    /// Direct-message request failed input validation
    #[error("invalid message request: {message}")]
    MessageInvalid {
        /// Why the message request is invalid
        message: String,
    },
    /// A message destination is the sender's own thread
    #[error(
        "destination thread {thread} is the sender's own thread; --task targets the task's origin thread, not its worker; use --worker for a running worker or `homebased task followup` for a finished Codex worker"
    )]
    MessageToSelf {
        /// Thread that is both sender and destination
        thread: ThreadId,
    },
    /// No local Codex session matches the requested destination
    #[error("agent thread not found for {selector}")]
    AgentThreadNotFound {
        /// Exact thread UUID or resolved cwd that was requested
        selector: String,
    },
    /// A message UUID was reused for different request content
    #[error("message conflict: {id}")]
    MessageConflict {
        /// Message UUID that already has a different saved request
        id: MessageId,
    },
    /// The receiver could not complete one explicit queue attempt
    #[error("message delivery failed: {id}: {message}")]
    MessageDeliveryFailed {
        /// Message UUID that remains available for an explicit retry
        id: MessageId,
        /// Why the attempt failed and how to retry it
        message: String,
    },
    /// The receiver may have queued the message but did not return a valid acknowledgement
    #[error("message outcome unknown for {id} on machine {machine}: {message}")]
    MessageOutcomeUnknown {
        /// Message UUID to reuse for an explicit retry
        id: MessageId,
        /// Fixed receiver identity for the retry
        machine: MachineId,
        /// Why the acknowledgement is unknown
        message: String,
    },
    /// The receiver could not inspect local Codex session metadata
    #[error("message receiver unavailable: {message}")]
    MessageUnavailable {
        /// Safe failure summary
        message: String,
    },
    /// Machines share no cluster protocol version
    #[error("machine {machine} speaks cluster protocol {remote}, this daemon accepts {local}")]
    ClusterProtocolIncompatible {
        /// Remote machine
        machine: MachineId,
        /// Local accepted range
        local: ProtocolRange,
        /// Remote accepted range
        remote: ProtocolRange,
    },
    /// A daemon actor did not answer within the call timeout. The operation may
    /// still complete after the caller stops waiting
    #[error("daemon busy: an internal call timed out; the operation may still complete")]
    DaemonBusy,
    /// The GPU priority queue refused a request
    #[error(transparent)]
    Queue(#[from] QueueError),
    /// The database was written by a newer build that this binary cannot read
    #[error(
        "database schema version {found} is newer than this build supports ({supported}); \
         update homebased"
    )]
    SchemaTooNew {
        /// `user_version` found in the database file
        found: i64,
        /// Newest schema version this binary reads
        supported: i64,
    },
    /// Unexpected internal failure
    #[error("{message}")]
    Internal {
        /// Underlying failure text
        message: String,
    },
}

/// Why a task cannot be resumed as a Codex worker
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, thiserror::Error,
)]
#[serde(rename_all = "snake_case")]
pub enum FollowupBlocker {
    /// The task workload is not a Codex agent
    #[error("the workload is not a Codex agent")]
    NotCodex,
    /// The task has no known terminal status
    #[error("the task is not terminal")]
    NotTerminal,
    /// The task has no recorded Codex worker thread
    #[error("the task has no recorded worker thread")]
    NoWorkerThread,
}

/// Why a task's worker cannot take a direct message
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, thiserror::Error,
)]
#[serde(rename_all = "snake_case")]
pub enum WorkerMessageBlocker {
    /// The task is queued, or its agent records a worker thread only at exit
    #[error(
        "the task has no recorded worker thread yet; a Claude worker records one when it starts running, a Codex worker only when it finishes"
    )]
    NoWorkerThread,
    /// The worker has exited and reads no further messages
    #[error(
        "the task is terminal; use `homebased task followup` to resume a finished Codex worker"
    )]
    Terminal,
}

impl AppError {
    /// Machine-readable code from `plan §CLI`
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::DaemonUnavailable { .. } => "daemon_unavailable",
            Self::TaskNotFound { .. } => "task_not_found",
            Self::ClusterLookupIncomplete { .. } => "cluster_lookup_incomplete",
            Self::TaskUnavailable { .. } => "task_unavailable",
            Self::TaskNotStarted { .. } => "task_not_started",
            Self::FollowupUnavailable { .. } => "followup_unavailable",
            Self::WorkerMessageUnavailable { .. } => "worker_message_unavailable",
            Self::ResumeThreadBusy { .. } => "resume_thread_busy",
            Self::FollowupWrongMachine { .. } => "followup_wrong_machine",
            Self::InvalidCwd { .. } => "invalid_cwd",
            Self::UnknownDependency { .. } => "unknown_dependency",
            Self::DependencyFailed { .. } => "dependency_failed",
            Self::ExecutableMissing { .. } => "executable_missing",
            Self::SummaryTooLong { .. } => "summary_too_long",
            Self::TooManyReports { .. } => "too_many_reports",
            Self::TaskTerminal { .. } => "task_terminal",
            Self::InvalidSpec { .. } => "invalid_spec",
            Self::UnknownThread { .. } => "unknown_thread",
            Self::ThreadMismatch { .. } => "thread_mismatch",
            Self::AgentConfiguration { .. } => "agent_configuration",
            Self::DaemonAlreadyRunning => "daemon_already_running",
            Self::LockHeld { .. } => "lock_held",
            Self::TasksInFlight { .. } => "tasks_in_flight",
            Self::UnitInvalid { .. } => "unit_invalid",
            Self::HostUnitHomeMismatch { .. } => "host_unit_home_mismatch",
            Self::Usage { .. } => "usage",
            Self::Permission { .. } => "permission",
            Self::FileNotFound { .. } => "file_not_found",
            Self::NotDirectory { .. } => "not_directory",
            Self::ChangedDuringRead { .. } => "changed_during_read",
            Self::UnsupportedFile { .. } => "unsupported_file",
            Self::StreamLimit { .. } => "stream_limit",
            Self::ConfigInvalid { .. } => "config_invalid",
            Self::NotifyNotConfigured => "notify_not_configured",
            Self::NotifyFailed { .. } => "notify_failed",
            Self::MachineNotFound { .. } => "machine_not_found",
            Self::MachineIdentityMismatch { .. } => "machine_identity_mismatch",
            Self::DuplicateMachineIdentity { .. } => "duplicate_machine_identity",
            Self::DuplicateMachineName { .. } => "duplicate_machine_name",
            Self::MachineUnavailable { .. } => "machine_unavailable",
            Self::RemoteSubmissionUnavailable { .. } => "remote_submission_unavailable",
            Self::SubmissionOutcomeUnknown { .. } => "submission_outcome_unknown",
            Self::SubmissionRejected { .. } => "submission_rejected",
            Self::SubmissionConflict { .. } => "submission_conflict",
            Self::RouteNotFound { .. } => "route_not_found",
            Self::ClusterTaskConflict { .. } => "cluster_task_conflict",
            Self::EventContentConflict { .. } => "event_content_conflict",
            Self::MessageInvalid { .. } => "message_invalid",
            Self::MessageToSelf { .. } => "message_to_self",
            Self::AgentThreadNotFound { .. } => "agent_thread_not_found",
            Self::MessageConflict { .. } => "message_conflict",
            Self::MessageDeliveryFailed { .. } => "message_delivery_failed",
            Self::MessageOutcomeUnknown { .. } => "message_outcome_unknown",
            Self::MessageUnavailable { .. } => "message_receiver_unavailable",
            Self::ClusterProtocolIncompatible { .. } => "cluster_protocol_incompatible",
            Self::DaemonBusy => "daemon_busy",
            Self::Queue(error) => error.code(),
            Self::SchemaTooNew { .. } => "schema_too_new",
            Self::Internal { .. } => "internal",
        }
    }

    /// Process exit code
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::DaemonUnavailable { .. }
            | Self::UnitInvalid { .. }
            | Self::LockHeld { .. }
            | Self::MachineUnavailable { .. }
            | Self::RemoteSubmissionUnavailable { .. }
            | Self::SubmissionOutcomeUnknown { .. }
            | Self::ClusterLookupIncomplete { .. }
            | Self::TaskUnavailable { .. }
            | Self::NotifyFailed { .. }
            | Self::DaemonBusy
            | Self::SchemaTooNew { .. }
            | Self::Internal { .. } => 1,
            Self::InvalidSpec { .. }
            | Self::InvalidCwd { .. }
            | Self::UnknownThread { .. }
            | Self::ThreadMismatch { .. }
            | Self::SummaryTooLong { .. }
            | Self::MessageInvalid { .. }
            | Self::Usage { .. }
            | Self::MessageToSelf { .. }
            | Self::FollowupWrongMachine { .. }
            | Self::ConfigInvalid { .. }
            | Self::NotifyNotConfigured => 2,
            Self::AgentConfiguration { .. } => 1,
            Self::TaskNotFound { .. }
            | Self::TaskNotStarted { .. }
            | Self::RouteNotFound { .. }
            | Self::UnknownDependency { .. }
            | Self::ExecutableMissing { .. }
            | Self::FileNotFound { .. }
            | Self::NotDirectory { .. }
            | Self::UnsupportedFile { .. }
            | Self::MachineNotFound { .. } => 3,
            Self::AgentThreadNotFound { .. } => 3,
            Self::Permission { .. } => 4,
            Self::TooManyReports { .. }
            | Self::TaskTerminal { .. }
            | Self::DependencyFailed { .. }
            | Self::FollowupUnavailable { .. }
            | Self::WorkerMessageUnavailable { .. }
            | Self::ResumeThreadBusy { .. }
            | Self::DaemonAlreadyRunning
            | Self::TasksInFlight { .. }
            | Self::HostUnitHomeMismatch { .. }
            | Self::ChangedDuringRead { .. }
            | Self::StreamLimit { .. }
            | Self::MachineIdentityMismatch { .. }
            | Self::ClusterTaskConflict { .. }
            | Self::EventContentConflict { .. }
            | Self::DuplicateMachineIdentity { .. }
            | Self::DuplicateMachineName { .. }
            | Self::ClusterProtocolIncompatible { .. } => 5,
            Self::SubmissionRejected { .. }
            | Self::SubmissionConflict { .. }
            | Self::MessageConflict { .. } => 5,
            Self::MessageDeliveryFailed { .. }
            | Self::MessageOutcomeUnknown { .. }
            | Self::MessageUnavailable { .. } => 1,
            Self::Queue(error) => error.exit_code(),
        }
    }

    /// HTTP status for the Unix-socket API
    #[must_use]
    pub fn http_status(&self) -> http::StatusCode {
        match self {
            Self::InvalidSpec { .. }
            | Self::InvalidCwd { .. }
            | Self::UnknownThread { .. }
            | Self::ThreadMismatch { .. }
            | Self::SummaryTooLong { .. }
            | Self::MessageInvalid { .. }
            | Self::Usage { .. }
            | Self::NotDirectory { .. }
            | Self::UnsupportedFile { .. } => http::StatusCode::BAD_REQUEST,
            Self::MessageToSelf { .. } => http::StatusCode::BAD_REQUEST,
            Self::FollowupWrongMachine { .. } => http::StatusCode::BAD_REQUEST,
            Self::NotifyNotConfigured => http::StatusCode::BAD_REQUEST,
            Self::AgentConfiguration { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            Self::TaskNotFound { .. }
            | Self::TaskNotStarted { .. }
            | Self::RouteNotFound { .. }
            | Self::UnknownDependency { .. }
            | Self::ExecutableMissing { .. }
            | Self::FileNotFound { .. }
            | Self::MachineNotFound { .. } => http::StatusCode::NOT_FOUND,
            Self::AgentThreadNotFound { .. } => http::StatusCode::NOT_FOUND,
            Self::Permission { .. } => http::StatusCode::FORBIDDEN,
            Self::TooManyReports { .. }
            | Self::TaskTerminal { .. }
            | Self::DependencyFailed { .. }
            | Self::FollowupUnavailable { .. }
            | Self::WorkerMessageUnavailable { .. }
            | Self::ResumeThreadBusy { .. }
            | Self::DaemonAlreadyRunning
            | Self::TasksInFlight { .. }
            | Self::HostUnitHomeMismatch { .. }
            | Self::ChangedDuringRead { .. }
            | Self::StreamLimit { .. }
            | Self::MachineIdentityMismatch { .. }
            | Self::ClusterTaskConflict { .. }
            | Self::EventContentConflict { .. }
            | Self::DuplicateMachineIdentity { .. }
            | Self::DuplicateMachineName { .. }
            | Self::ClusterProtocolIncompatible { .. } => http::StatusCode::CONFLICT,
            Self::SubmissionRejected { .. }
            | Self::SubmissionConflict { .. }
            | Self::MessageConflict { .. } => http::StatusCode::CONFLICT,
            Self::MachineUnavailable { .. }
            | Self::RemoteSubmissionUnavailable { .. }
            | Self::ClusterLookupIncomplete { .. }
            | Self::TaskUnavailable { .. }
            | Self::MessageDeliveryFailed { .. }
            | Self::MessageOutcomeUnknown { .. }
            | Self::MessageUnavailable { .. }
            | Self::SubmissionOutcomeUnknown { .. }
            | Self::DaemonBusy => http::StatusCode::SERVICE_UNAVAILABLE,
            Self::DaemonUnavailable { .. }
            | Self::ConfigInvalid { .. }
            | Self::UnitInvalid { .. }
            | Self::LockHeld { .. }
            | Self::NotifyFailed { .. }
            | Self::SchemaTooNew { .. }
            | Self::Internal { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            Self::Queue(error) => error.http_status(),
        }
    }

    /// Whether a caller should retry the same request
    #[must_use]
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::DaemonUnavailable { .. }
                | Self::MachineUnavailable { .. }
                | Self::RemoteSubmissionUnavailable { .. }
                | Self::SubmissionOutcomeUnknown { .. }
                | Self::ClusterLookupIncomplete { .. }
                | Self::TaskUnavailable { .. }
                | Self::MessageDeliveryFailed { .. }
                | Self::MessageOutcomeUnknown { .. }
                | Self::MessageUnavailable { .. }
        )
    }

    /// Structured `input` object for the error envelope
    #[must_use]
    pub fn input(&self) -> Value {
        match self {
            Self::TaskNotFound { id } => json!({ "id": id }),
            Self::FollowupUnavailable { task, reason } => {
                json!({ "task": task, "reason": reason })
            }
            Self::WorkerMessageUnavailable { task, reason } => {
                json!({ "task": task, "reason": reason })
            }
            Self::ClusterLookupIncomplete { task, unchecked } => {
                json!({ "task": task, "unchecked": unchecked })
            }
            Self::TaskUnavailable { task, machine } => {
                json!({ "task": task, "machine": machine })
            }
            Self::ResumeThreadBusy { thread, task } => json!({ "thread": thread, "task": task }),
            Self::FollowupWrongMachine {
                task,
                origin_machine,
                execution_machine,
            } => json!({
                "task": task,
                "origin_machine": origin_machine,
                "execution_machine": execution_machine
            }),
            Self::TaskNotStarted { task } => json!({ "task": task }),
            Self::UnknownDependency { task } => json!({ "pointer": "/after", "task": task }),
            Self::DependencyFailed { task, outcome } => {
                json!({ "pointer": "/after", "task": task, "outcome": outcome })
            }
            Self::RouteNotFound { task } | Self::ClusterTaskConflict { task } => {
                json!({ "task": task })
            }
            Self::EventContentConflict { task, seq } => json!({ "task": task, "seq": seq }),
            Self::MessageInvalid { message } => json!({ "message": message }),
            Self::MessageToSelf { thread } => json!({ "thread": thread }),
            Self::AgentThreadNotFound { selector } => json!({ "selector": selector }),
            Self::MessageConflict { id } => json!({ "message_id": id }),
            Self::MessageDeliveryFailed { id, message } => {
                json!({ "message_id": id, "message": message })
            }
            Self::MessageOutcomeUnknown {
                id,
                machine,
                message,
            } => {
                json!({ "message_id": id, "machine": machine, "message": message })
            }
            Self::MessageUnavailable { message } => json!({ "message": message }),
            Self::InvalidCwd {
                path,
                problem,
                suggested_cwd,
            } => json!({
                "pointer": "/cwd",
                "value": path,
                "problem": problem,
                "suggested_cwd": suggested_cwd,
            }),
            Self::ExecutableMissing { program } => json!({ "program": program }),
            Self::SummaryTooLong { len } => json!({ "len": len }),
            Self::TooManyReports { count } => json!({ "count": count }),
            Self::TaskTerminal { id, status } => json!({ "id": id, "status": status }),
            Self::InvalidSpec { pointer, value, .. } => {
                json!({ "pointer": pointer, "value": value })
            }
            Self::UnknownThread {
                thread,
                suggestions,
                ..
            } => json!({ "pointer": "/thread", "value": thread, "suggestions": suggestions }),
            Self::ThreadMismatch {
                thread,
                parent_task,
                parent_thread,
            } => json!({
                "pointer": "/thread",
                "value": thread,
                "parent_task": parent_task,
                "parent_thread": parent_thread,
            }),
            Self::AgentConfiguration { agent, message } => {
                json!({ "agent": agent, "message": message })
            }
            Self::TasksInFlight { count } => json!({ "count": count }),
            Self::Usage { message } => json!({ "message": message }),
            Self::Permission { message } => json!({ "message": message }),
            Self::FileNotFound { message }
            | Self::NotDirectory { message }
            | Self::ChangedDuringRead { message }
            | Self::UnsupportedFile { message }
            | Self::StreamLimit { message } => json!({ "message": message }),
            Self::UnitInvalid { message } => json!({ "message": message }),
            Self::HostUnitHomeMismatch {
                selected,
                configured,
            } => json!({ "selected": selected, "configured": configured }),
            Self::NotifyNotConfigured => json!({}),
            Self::NotifyFailed { message } => json!({ "message": message }),
            Self::DaemonBusy => json!({}),
            Self::SchemaTooNew { found, supported } => {
                json!({ "found": found, "supported": supported })
            }
            Self::Internal { message } => json!({ "message": message }),
            Self::ConfigInvalid { path, .. } => json!({ "path": path }),
            Self::MachineNotFound { machine } => json!({ "machine": machine }),
            Self::MachineIdentityMismatch { expected, found } => {
                json!({ "expected": expected, "found": found })
            }
            Self::DuplicateMachineIdentity { machine } => json!({ "machine": machine }),
            Self::DuplicateMachineName { name, machines } => {
                json!({ "name": name, "machines": machines })
            }
            Self::MachineUnavailable { machine, message } => {
                json!({ "machine": machine, "message": message })
            }
            Self::RemoteSubmissionUnavailable { message } => json!({ "message": message }),
            Self::SubmissionOutcomeUnknown {
                request,
                task,
                message,
            } => json!({ "request_id": request, "task_id": task, "message": message }),
            Self::SubmissionRejected {
                request,
                task,
                reason,
            } => json!({ "request_id": request, "task_id": task, "reason": reason }),
            Self::SubmissionConflict {
                request,
                task,
                message,
            } => json!({ "request_id": request, "task_id": task, "message": message }),
            Self::ClusterProtocolIncompatible {
                machine,
                local,
                remote,
            } => json!({ "machine": machine, "local": local, "remote": remote }),
            Self::DaemonUnavailable { message } => json!({ "message": message }),
            Self::DaemonAlreadyRunning => json!({}),
            Self::LockHeld { path } => json!({ "path": path }),
            Self::Queue(error) => error.input(),
        }
    }

    /// JSON envelope for `--json` and HTTP errors
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "api_version": crate::API_VERSION,
            "error": {
                "code": self.code(),
                "message": self.to_string(),
                "retryable": self.retryable(),
                "input": self.input(),
            }
        })
    }

    /// Convert to a process exit code
    #[must_use]
    pub fn to_exit_code(&self) -> ExitCode {
        ExitCode::from(self.exit_code())
    }
}

impl From<io::Error> for AppError {
    fn from(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::PermissionDenied {
            Self::Permission {
                message: err.to_string(),
            }
        } else {
            Self::Internal {
                message: err.to_string(),
            }
        }
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(err: rusqlite::Error) -> Self {
        Self::Internal {
            message: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> Self {
        Self::Internal {
            message: err.to_string(),
        }
    }
}
