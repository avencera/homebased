//! CLI and HTTP error codes.

use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

use serde_json::{json, Value};

use crate::domain::{AgentKind, ProcessStatus, TaskId};

/// Application error with a stable machine-readable code.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// Daemon socket is missing or not accepting connections.
    #[error("{message}")]
    DaemonUnavailable { message: String },
    /// No task row for this id.
    #[error("task not found: {id}")]
    TaskNotFound { id: TaskId },
    /// Spec `cwd` is missing or not a directory.
    #[error("cwd not found: {}", path.display())]
    CwdNotFound { path: PathBuf },
    /// Agent binary is not on PATH and no HOMEBASED_* override exists.
    #[error("agent binary missing: {agent}")]
    AgentBinaryMissing { agent: AgentKind },
    /// Report summary exceeds 4 KiB.
    #[error("summary too long: {len} bytes")]
    SummaryTooLong { len: usize },
    /// Task already has 20 reports.
    #[error("too many reports: {count}")]
    TooManyReports { count: usize },
    /// Report or mutate attempted on a terminal task.
    #[error("task {id} is terminal ({status})")]
    TaskTerminal { id: TaskId, status: ProcessStatus },
    /// Submit spec failed validation.
    #[error("{message}")]
    InvalidSpec {
        pointer: String,
        value: Value,
        message: String,
    },
    /// Another serve process holds `daemon.lock`.
    #[error("daemon already running")]
    DaemonAlreadyRunning,
    /// Stop or uninstall without `--yes` while tasks are queued or running.
    #[error("{count} task(s) in flight")]
    TasksInFlight { count: usize },
    /// Generated host unit failed `systemd-analyze` or `plutil`.
    #[error("unit invalid: {message}")]
    UnitInvalid { message: String },
    /// Usage error (conflicting flags, bad UUID).
    #[error("{message}")]
    Usage { message: String },
    /// Permission denied.
    #[error("{message}")]
    Permission { message: String },
    /// Unexpected internal failure.
    #[error("{message}")]
    Internal { message: String },
}

impl AppError {
    /// Machine-readable code from `plan §CLI`.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::DaemonUnavailable { .. } => "daemon_unavailable",
            Self::TaskNotFound { .. } => "task_not_found",
            Self::CwdNotFound { .. } => "cwd_not_found",
            Self::AgentBinaryMissing { .. } => "agent_binary_missing",
            Self::SummaryTooLong { .. } => "summary_too_long",
            Self::TooManyReports { .. } => "too_many_reports",
            Self::TaskTerminal { .. } => "task_terminal",
            Self::InvalidSpec { .. } => "invalid_spec",
            Self::DaemonAlreadyRunning => "daemon_already_running",
            Self::TasksInFlight { .. } => "tasks_in_flight",
            Self::UnitInvalid { .. } => "unit_invalid",
            Self::Usage { .. } => "usage",
            Self::Permission { .. } => "permission",
            Self::Internal { .. } => "internal",
        }
    }

    /// Process exit code.
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::DaemonUnavailable { .. } | Self::UnitInvalid { .. } | Self::Internal { .. } => 1,
            Self::InvalidSpec { .. } | Self::SummaryTooLong { .. } | Self::Usage { .. } => 2,
            Self::TaskNotFound { .. }
            | Self::CwdNotFound { .. }
            | Self::AgentBinaryMissing { .. } => 3,
            Self::Permission { .. } => 4,
            Self::TooManyReports { .. }
            | Self::TaskTerminal { .. }
            | Self::DaemonAlreadyRunning
            | Self::TasksInFlight { .. } => 5,
        }
    }

    /// HTTP status for the Unix-socket API.
    #[must_use]
    pub fn http_status(&self) -> http::StatusCode {
        match self {
            Self::InvalidSpec { .. } | Self::SummaryTooLong { .. } | Self::Usage { .. } => {
                http::StatusCode::BAD_REQUEST
            }
            Self::TaskNotFound { .. }
            | Self::CwdNotFound { .. }
            | Self::AgentBinaryMissing { .. } => http::StatusCode::NOT_FOUND,
            Self::Permission { .. } => http::StatusCode::FORBIDDEN,
            Self::TooManyReports { .. }
            | Self::TaskTerminal { .. }
            | Self::DaemonAlreadyRunning
            | Self::TasksInFlight { .. } => http::StatusCode::CONFLICT,
            Self::DaemonUnavailable { .. } | Self::UnitInvalid { .. } | Self::Internal { .. } => {
                http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    /// Whether a caller should retry the same request.
    #[must_use]
    pub fn retryable(&self) -> bool {
        matches!(self, Self::DaemonUnavailable { .. })
    }

    /// Structured `input` object for the error envelope.
    #[must_use]
    pub fn input(&self) -> Value {
        match self {
            Self::TaskNotFound { id } => json!({ "id": id }),
            Self::CwdNotFound { path } => json!({ "cwd": path }),
            Self::AgentBinaryMissing { agent } => json!({ "agent": agent }),
            Self::SummaryTooLong { len } => json!({ "len": len }),
            Self::TooManyReports { count } => json!({ "count": count }),
            Self::TaskTerminal { id, status } => json!({ "id": id, "status": status }),
            Self::InvalidSpec { pointer, value, .. } => {
                json!({ "pointer": pointer, "value": value })
            }
            Self::TasksInFlight { count } => json!({ "count": count }),
            Self::Usage { message } => json!({ "message": message }),
            Self::Permission { message } => json!({ "message": message }),
            Self::UnitInvalid { message } => json!({ "message": message }),
            Self::Internal { message } => json!({ "message": message }),
            Self::DaemonUnavailable { message } => json!({ "message": message }),
            Self::DaemonAlreadyRunning => json!({}),
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

    /// Map an HTTP status from the daemon to a CLI exit code.
    #[must_use]
    pub fn exit_from_http(status: http::StatusCode) -> u8 {
        match status.as_u16() {
            400 => 2,
            404 => 3,
            403 => 4,
            409 => 5,
            _ => 1,
        }
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
