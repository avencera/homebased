//! CLI and HTTP error codes.

use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

use serde_json::{Value, json};

use crate::domain::{AgentKind, ProcessStatus, TaskId};
use crate::fleet::protocol::ProtocolRange;
use crate::machine::{MachineId, MachineName};
use crate::submission::RequestId;

/// Application error with a stable machine-readable code.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// Daemon socket is missing or not accepting connections.
    #[error("{message}")]
    DaemonUnavailable {
        /// Why the socket could not be reached.
        message: String,
    },
    /// No task row for this id.
    #[error("task not found: {id}")]
    TaskNotFound {
        /// Id that had no row.
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
    /// Spec `cwd` is missing or not a directory
    #[error("cwd not found: {}", path.display())]
    CwdNotFound {
        /// Directory the spec asked for.
        path: PathBuf,
    },
    /// Requested program is missing, not a file, or not executable.
    #[error("executable missing: {program}")]
    ExecutableMissing {
        /// Program name or path the caller requested.
        program: String,
    },
    /// Report summary exceeds 4 KiB.
    #[error("summary too long: {len} bytes")]
    SummaryTooLong {
        /// Summary length in bytes.
        len: usize,
    },
    /// Task already has 20 reports.
    #[error("too many reports: {count}")]
    TooManyReports {
        /// Reports already stored for the task.
        count: usize,
    },
    /// Report or mutate attempted on a terminal task.
    #[error("task {id} is terminal ({status})")]
    TaskTerminal {
        /// Task that is already finished.
        id: TaskId,
        /// Terminal status the task holds.
        status: ProcessStatus,
    },
    /// Submit spec failed validation.
    #[error("{message}")]
    InvalidSpec {
        /// JSON pointer to the offending key.
        pointer: String,
        /// Value found at `pointer`.
        value: Value,
        /// Why the value was rejected.
        message: String,
    },
    /// Agent-specific child configuration could not be built safely.
    #[error("{agent} child configuration: {message}")]
    AgentConfiguration {
        /// Agent whose child environment or command was invalid.
        agent: AgentKind,
        /// Safe configuration failure detail.
        message: String,
    },
    /// Another serve process holds `daemon.lock`.
    #[error("daemon already running")]
    DaemonAlreadyRunning,
    /// A non-blocking `flock` found the file locked by another process.
    #[error("lock held: {}", path.display())]
    LockHeld {
        /// Lock file another process holds.
        path: PathBuf,
    },
    /// Stop or uninstall without `--yes` while tasks are queued or running.
    #[error("{count} task(s) in flight")]
    TasksInFlight {
        /// Queued or running tasks that block the operation.
        count: usize,
    },
    /// Generated host unit failed `systemd-analyze` or `plutil`.
    #[error("unit invalid: {message}")]
    UnitInvalid {
        /// Validator output.
        message: String,
    },
    /// Uninstall refused because the host unit belongs to another home.
    #[error(
        "host unit belongs to another home (selected {}, configured {})",
        selected.display(),
        configured.display()
    )]
    HostUnitHomeMismatch {
        /// Home selected by this command.
        selected: PathBuf,
        /// Home recorded in the installed unit.
        configured: PathBuf,
    },
    /// Usage error (conflicting flags, bad UUID).
    #[error("{message}")]
    Usage {
        /// What the caller got wrong.
        message: String,
    },
    /// Permission denied.
    #[error("{message}")]
    Permission {
        /// Operation that was denied.
        message: String,
    },
    /// Filesystem path is missing.
    #[error("{message}")]
    FileNotFound {
        /// Human-readable failure.
        message: String,
    },
    /// Path exists but is not a directory when one was required.
    #[error("{message}")]
    NotDirectory {
        /// Human-readable failure.
        message: String,
    },
    /// Directory or file changed while it was being read.
    #[error("{message}")]
    ChangedDuringRead {
        /// Human-readable failure.
        message: String,
    },
    /// Special file (device, socket, FIFO) is not browsable.
    #[error("{message}")]
    UnsupportedFile {
        /// Human-readable failure.
        message: String,
    },
    /// Too many concurrent content streams.
    #[error("{message}")]
    StreamLimit {
        /// Human-readable failure.
        message: String,
    },
    /// `config.toml` is missing, unreadable, or fails validation.
    #[error("invalid config {}: {message}", path.display())]
    ConfigInvalid {
        /// Config file path.
        path: PathBuf,
        /// Why the file was rejected.
        message: String,
    },
    /// No known machine matches this name or UUID.
    #[error("machine not found: {machine}")]
    MachineNotFound {
        /// Name or UUID the caller gave.
        machine: String,
    },
    /// An address answered for another installation than the request named.
    #[error(
        "machine identity mismatch: expected {expected}, found {}",
        found.map_or_else(|| "no machine".to_string(), |id| id.to_string())
    )]
    MachineIdentityMismatch {
        /// Destination machine UUID the request carried.
        expected: MachineId,
        /// Machine UUID that answered, when known.
        found: Option<MachineId>,
    },
    /// Distinct live daemons claim one machine UUID.
    #[error("duplicate machine identity: {machine}")]
    DuplicateMachineIdentity {
        /// Conflicting machine UUID.
        machine: MachineId,
    },
    /// More than one machine uses one name.
    #[error("duplicate machine name: {name}")]
    DuplicateMachineName {
        /// Conflicting name.
        name: MachineName,
        /// Machines that claim it.
        machines: Vec<MachineId>,
    },
    /// Known machine that did not answer.
    #[error("machine {machine} unavailable: {message}")]
    MachineUnavailable {
        /// Machine that did not answer.
        machine: MachineId,
        /// Last failure.
        message: String,
    },
    /// Remote execution cannot start until its event route is available.
    #[error("remote submission is unavailable: {message}")]
    RemoteSubmissionUnavailable {
        /// Why this daemon cannot start the task now.
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
        id: crate::message::MessageId,
    },
    /// The receiver could not complete one explicit queue attempt
    #[error("message delivery failed: {id}")]
    MessageDeliveryFailed {
        /// Message UUID that remains available for an explicit retry
        id: crate::message::MessageId,
        /// Safe summary of the queue failure
        message: String,
    },
    /// The receiver may have queued the message but did not return a valid acknowledgement
    #[error("message outcome unknown for {id} on machine {machine}: {message}")]
    MessageOutcomeUnknown {
        /// Message UUID to reuse for an explicit retry
        id: crate::message::MessageId,
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
    /// Machines share no cluster protocol version.
    #[error("machine {machine} speaks cluster protocol {remote}, this daemon accepts {local}")]
    ClusterProtocolIncompatible {
        /// Remote machine.
        machine: MachineId,
        /// Local accepted range.
        local: ProtocolRange,
        /// Remote accepted range.
        remote: ProtocolRange,
    },
    /// Unexpected internal failure.
    #[error("{message}")]
    Internal {
        /// Underlying failure text.
        message: String,
    },
}

impl AppError {
    /// Machine-readable code from `plan §CLI`.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::DaemonUnavailable { .. } => "daemon_unavailable",
            Self::TaskNotFound { .. } => "task_not_found",
            Self::ClusterLookupIncomplete { .. } => "cluster_lookup_incomplete",
            Self::TaskUnavailable { .. } => "task_unavailable",
            Self::TaskNotStarted { .. } => "task_not_started",
            Self::CwdNotFound { .. } => "cwd_not_found",
            Self::ExecutableMissing { .. } => "executable_missing",
            Self::SummaryTooLong { .. } => "summary_too_long",
            Self::TooManyReports { .. } => "too_many_reports",
            Self::TaskTerminal { .. } => "task_terminal",
            Self::InvalidSpec { .. } => "invalid_spec",
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
            Self::AgentThreadNotFound { .. } => "agent_thread_not_found",
            Self::MessageConflict { .. } => "message_conflict",
            Self::MessageDeliveryFailed { .. } => "message_delivery_failed",
            Self::MessageOutcomeUnknown { .. } => "message_outcome_unknown",
            Self::MessageUnavailable { .. } => "message_receiver_unavailable",
            Self::ClusterProtocolIncompatible { .. } => "cluster_protocol_incompatible",
            Self::Internal { .. } => "internal",
        }
    }

    /// Process exit code.
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
            | Self::Internal { .. } => 1,
            Self::InvalidSpec { .. }
            | Self::SummaryTooLong { .. }
            | Self::MessageInvalid { .. }
            | Self::Usage { .. }
            | Self::ConfigInvalid { .. } => 2,
            Self::AgentConfiguration { .. } => 1,
            Self::TaskNotFound { .. }
            | Self::TaskNotStarted { .. }
            | Self::RouteNotFound { .. }
            | Self::CwdNotFound { .. }
            | Self::ExecutableMissing { .. }
            | Self::FileNotFound { .. }
            | Self::NotDirectory { .. }
            | Self::UnsupportedFile { .. }
            | Self::MachineNotFound { .. } => 3,
            Self::AgentThreadNotFound { .. } => 3,
            Self::Permission { .. } => 4,
            Self::TooManyReports { .. }
            | Self::TaskTerminal { .. }
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
        }
    }

    /// HTTP status for the Unix-socket API.
    #[must_use]
    pub fn http_status(&self) -> http::StatusCode {
        match self {
            Self::InvalidSpec { .. }
            | Self::SummaryTooLong { .. }
            | Self::MessageInvalid { .. }
            | Self::Usage { .. }
            | Self::NotDirectory { .. }
            | Self::UnsupportedFile { .. } => http::StatusCode::BAD_REQUEST,
            Self::AgentConfiguration { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            Self::TaskNotFound { .. }
            | Self::TaskNotStarted { .. }
            | Self::RouteNotFound { .. }
            | Self::CwdNotFound { .. }
            | Self::ExecutableMissing { .. }
            | Self::FileNotFound { .. }
            | Self::MachineNotFound { .. } => http::StatusCode::NOT_FOUND,
            Self::AgentThreadNotFound { .. } => http::StatusCode::NOT_FOUND,
            Self::Permission { .. } => http::StatusCode::FORBIDDEN,
            Self::TooManyReports { .. }
            | Self::TaskTerminal { .. }
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
            | Self::SubmissionOutcomeUnknown { .. } => http::StatusCode::SERVICE_UNAVAILABLE,
            Self::DaemonUnavailable { .. }
            | Self::ConfigInvalid { .. }
            | Self::UnitInvalid { .. }
            | Self::LockHeld { .. }
            | Self::Internal { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Whether a caller should retry the same request.
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

    /// Structured `input` object for the error envelope.
    #[must_use]
    pub fn input(&self) -> Value {
        match self {
            Self::TaskNotFound { id } => json!({ "id": id }),
            Self::ClusterLookupIncomplete { task, unchecked } => {
                json!({ "task": task, "unchecked": unchecked })
            }
            Self::TaskUnavailable { task, machine } => {
                json!({ "task": task, "machine": machine })
            }
            Self::TaskNotStarted { task } => json!({ "task": task }),
            Self::RouteNotFound { task } | Self::ClusterTaskConflict { task } => {
                json!({ "task": task })
            }
            Self::EventContentConflict { task, seq } => json!({ "task": task, "seq": seq }),
            Self::MessageInvalid { message } => json!({ "message": message }),
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
            Self::CwdNotFound { path } => json!({ "cwd": path }),
            Self::ExecutableMissing { program } => json!({ "program": program }),
            Self::SummaryTooLong { len } => json!({ "len": len }),
            Self::TooManyReports { count } => json!({ "count": count }),
            Self::TaskTerminal { id, status } => json!({ "id": id, "status": status }),
            Self::InvalidSpec { pointer, value, .. } => {
                json!({ "pointer": pointer, "value": value })
            }
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
        }
    }

    /// JSON envelope for `--json` and HTTP errors.
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

    /// Convert to a process exit code.
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
