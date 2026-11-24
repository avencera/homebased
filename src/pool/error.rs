//! Refusals of pool requests

use serde::{Deserialize, Serialize};

/// Stable error code of a pool refusal
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolCode {
    /// This daemon has no `[pools]` config, so it cannot reach cloudlease
    PoolsNotConfigured,
    /// A pool of that name is open or releasing
    PoolExists,
    /// No pool of that name
    PoolNotFound,
    /// The pool takes no more jobs or machines
    PoolEnded,
    /// The job's kill limit runs past the pool's deadline
    PoolDeadlineTooClose,
    /// A machine's join did not match a machine this coordinator asked for
    JoinRefused,
}

impl PoolCode {
    /// Wire text
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PoolsNotConfigured => "pools_not_configured",
            Self::PoolExists => "pool_exists",
            Self::PoolNotFound => "pool_not_found",
            Self::PoolEnded => "pool_ended",
            Self::PoolDeadlineTooClose => "pool_deadline_too_close",
            Self::JoinRefused => "join_refused",
        }
    }

    /// Process exit code, grouped like the other errors: 2 input, 3 not found, 5 conflict
    #[must_use]
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::PoolsNotConfigured | Self::PoolDeadlineTooClose => 2,
            Self::PoolNotFound => 3,
            Self::PoolExists | Self::PoolEnded | Self::JoinRefused => 5,
        }
    }
}

/// A refused pool request
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct PoolError {
    /// Why
    pub code: PoolCode,
    /// What to tell the caller
    pub message: String,
}

impl PoolError {
    /// A refusal with `code`
    #[must_use]
    pub fn new(code: PoolCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// HTTP status for the same grouping as [`PoolCode::exit_code`]
    #[must_use]
    pub fn http_status(&self) -> http::StatusCode {
        match self.code.exit_code() {
            2 => http::StatusCode::BAD_REQUEST,
            3 => http::StatusCode::NOT_FOUND,
            _ => http::StatusCode::CONFLICT,
        }
    }
}
