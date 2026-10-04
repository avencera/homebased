//! Refusals and conflicts of the GPU priority queue

use std::time::Duration;

use serde_json::{Value, json};

use super::{AttentionId, JobId, OperationId, Priority, ResourceId, ResourceName};

/// Why a queue request was refused, or stored queue data could not be read
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueueError {
    /// A typed refusal decoded from an authority's response, preserving its input and code
    #[error("{message}")]
    Remote {
        /// Known queue response code
        code: QueueCode,
        /// Authority diagnostic
        message: String,
        /// Structured refused input
        input: Value,
    },
    /// A UUID argument did not parse
    #[error("invalid {what} (full UUID required): {value}")]
    InvalidId {
        /// Kind of identity
        what: &'static str,
        /// Text that did not parse
        value: String,
    },
    /// A priority level is not one of the known names
    #[error("invalid priority {value:?}; use low, medium, or high")]
    InvalidPriority {
        /// Text that did not parse
        value: String,
    },
    /// A restart window outside 1m through 24h
    #[error(
        "restart_within must be from 1m through 24h, got {}",
        humantime::format_duration(*window)
    )]
    InvalidRestartWindow {
        /// Refused window
        window: Duration,
    },
    /// A resource name breaks the naming rules
    #[error("invalid resource name {value:?}: it {reason}")]
    InvalidResourceName {
        /// Refused name
        value: String,
        /// Rule it breaks
        reason: &'static str,
    },
    /// A job with no steps or more than 32
    #[error("a job has 1 to 32 steps, got {count}")]
    InvalidStepCount {
        /// Number of steps given
        count: usize,
    },
    /// No job with this id in the machine queue
    #[error("job not found: {job}")]
    JobNotFound {
        /// Job that had no record
        job: JobId,
    },
    /// The job already left the queue
    #[error("job {job} is already {state}")]
    JobTerminal {
        /// Terminal job
        job: JobId,
        /// Its terminal state name
        state: &'static str,
    },
    /// The same job id was submitted with a different spec
    #[error("job {job} was already submitted with a different spec")]
    JobConflict {
        /// Reused job id
        job: JobId,
    },
    /// The same operation id was used for different content
    #[error("operation {operation} was already used for a different request")]
    OperationConflict {
        /// Reused operation id
        operation: OperationId,
    },
    /// A move's flags or target do not name one placement
    #[error("cannot move job {job}: {reason}")]
    MoveRefused {
        /// Job being moved
        job: JobId,
        /// Why the placement was refused
        reason: MoveRefusal,
    },
    /// No resource with this name or id on the machine
    #[error("resource not found: {resource}")]
    ResourceNotFound {
        /// Name or id that had no record
        resource: String,
    },
    /// A resource with this name already exists on the machine
    #[error("resource name {name} is already used on this machine")]
    ResourceNameTaken {
        /// Name in use
        name: ResourceName,
    },
    /// A resource for this GPU index already exists on the machine
    #[error("GPU device {device} already has resource {resource} on this machine")]
    DeviceTaken {
        /// Device index in use
        device: u32,
        /// Resource that has it
        resource: ResourceName,
    },
    /// No resource is in `Attention` with this id; it was released or never existed
    #[error("no resource is waiting for release with attention id {attention}")]
    AttentionNotFound {
        /// Stale or unknown attention id
        attention: AttentionId,
    },
    /// A request named a run that is no longer the resource's active run, or a
    /// cleanup attempt other than the stored one
    #[error("resource {resource} has no matching active run: {message}")]
    StaleRun {
        /// Resource named by the request
        resource: ResourceId,
        /// What did not match
        message: String,
    },
    /// The request would break a queue invariant
    #[error("queue invariant: {message}")]
    Invariant {
        /// The invariant and how the request breaks it
        message: String,
    },
    /// Stored queue data could not be read
    #[error("corrupt queue data: {message}")]
    Corrupt {
        /// What could not be read
        message: String,
    },
}

/// Why a move was refused
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, thiserror::Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MoveRefusal {
    /// No placement flag was given
    #[error("give one placement: --front, --back, --priority, --before, or --after")]
    NoPlacement,
    /// The flags name more than one placement
    #[error("the placement flags conflict; give one placement")]
    ConflictingFlags,
    /// The relative target is the job itself
    #[error("a job cannot move relative to itself")]
    SelfTarget,
    /// The relative target does not exist in this machine queue
    #[error("target job {target} is not in this machine queue")]
    TargetNotFound {
        /// Missing or foreign target
        target: JobId,
    },
    /// The relative target already left the queue
    #[error("target job {target} is already terminal")]
    TargetTerminal {
        /// Terminal target
        target: JobId,
    },
    /// `--priority` beside `--before` or `--after` names another level than the target's
    #[error("target job {target} is {actual}, not {requested}")]
    LevelMismatch {
        /// Relative target
        target: JobId,
        /// Level the flags asked for
        requested: Priority,
        /// Target's level
        actual: Priority,
    },
}

impl QueueError {
    /// The public code of a refusal; `None` for an internal error
    #[must_use]
    pub const fn queue_code(&self) -> Option<QueueCode> {
        match self {
            Self::Remote { code, .. } => Some(*code),
            Self::InvalidId { .. }
            | Self::InvalidPriority { .. }
            | Self::InvalidRestartWindow { .. }
            | Self::InvalidResourceName { .. }
            | Self::InvalidStepCount { .. } => Some(QueueCode::InvalidQueueInput),
            Self::JobNotFound { .. } => Some(QueueCode::JobNotFound),
            Self::JobTerminal { .. } => Some(QueueCode::JobTerminal),
            Self::JobConflict { .. } => Some(QueueCode::JobConflict),
            Self::OperationConflict { .. } => Some(QueueCode::OperationConflict),
            Self::MoveRefused { .. } => Some(QueueCode::MoveRefused),
            Self::ResourceNotFound { .. } => Some(QueueCode::ResourceNotFound),
            Self::ResourceNameTaken { .. } | Self::DeviceTaken { .. } => {
                Some(QueueCode::ResourceConflict)
            }
            Self::AttentionNotFound { .. } => Some(QueueCode::AttentionNotFound),
            Self::StaleRun { .. } => Some(QueueCode::StaleRun),
            Self::Invariant { .. } | Self::Corrupt { .. } => None,
        }
    }

    /// Machine-readable code
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self.queue_code() {
            Some(code) => code.as_str(),
            None => "internal",
        }
    }

    /// Process exit code, grouped like the other errors: 2 input, 3 not
    /// found, 5 conflict, 1 internal
    #[must_use]
    pub const fn exit_code(&self) -> u8 {
        match self.queue_code() {
            Some(code) => code.exit_code(),
            None => 1,
        }
    }

    /// HTTP status for the same grouping as [`Self::exit_code`]
    #[must_use]
    pub fn http_status(&self) -> http::StatusCode {
        match self.exit_code() {
            2 => http::StatusCode::BAD_REQUEST,
            3 => http::StatusCode::NOT_FOUND,
            5 => http::StatusCode::CONFLICT,
            _ => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Structured input for the JSON error envelope
    #[must_use]
    pub fn input(&self) -> Value {
        match self {
            Self::Remote { input, .. } => input.clone(),
            Self::InvalidId { what, value } => json!({ "what": what, "value": value }),
            Self::InvalidPriority { value } => json!({ "value": value }),
            Self::InvalidRestartWindow { window } => {
                json!({ "value": humantime::format_duration(*window).to_string() })
            }
            Self::InvalidResourceName { value, .. } => json!({ "value": value }),
            Self::InvalidStepCount { count } => json!({ "count": count }),
            Self::JobNotFound { job } | Self::JobConflict { job } => json!({ "job": job }),
            Self::JobTerminal { job, state } => json!({ "job": job, "state": state }),
            Self::OperationConflict { operation } => json!({ "operation": operation }),
            Self::MoveRefused { job, reason } => json!({ "job": job, "reason": reason }),
            Self::ResourceNotFound { resource } => json!({ "resource": resource }),
            Self::ResourceNameTaken { name } => json!({ "name": name }),
            Self::DeviceTaken { device, resource } => {
                json!({ "device": device, "resource": resource })
            }
            Self::AttentionNotFound { attention } => json!({ "attention": attention }),
            Self::StaleRun { resource, message } => {
                json!({ "resource": resource, "message": message })
            }
            Self::Invariant { message } | Self::Corrupt { message } => {
                json!({ "message": message })
            }
        }
    }
}

/// Known queue refusal codes carried by the public API
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueCode {
    /// Invalid queue argument
    InvalidQueueInput,
    /// Missing job
    JobNotFound,
    /// Job already ended
    JobTerminal,
    /// Job identity reused with different content
    JobConflict,
    /// Operation identity reused with different content
    OperationConflict,
    /// Invalid placement
    MoveRefused,
    /// Missing resource
    ResourceNotFound,
    /// Resource name or device already registered
    ResourceConflict,
    /// Stale or unknown Attention
    AttentionNotFound,
    /// Run or cleanup identity changed
    StaleRun,
}

impl QueueCode {
    /// Stable wire name
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidQueueInput => "invalid_queue_input",
            Self::JobNotFound => "job_not_found",
            Self::JobTerminal => "job_terminal",
            Self::JobConflict => "job_conflict",
            Self::OperationConflict => "operation_conflict",
            Self::MoveRefused => "move_refused",
            Self::ResourceNotFound => "resource_not_found",
            Self::ResourceConflict => "resource_conflict",
            Self::AttentionNotFound => "attention_not_found",
            Self::StaleRun => "stale_run",
        }
    }

    /// Process exit code: 2 input, 3 not found, 5 conflict
    #[must_use]
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::InvalidQueueInput | Self::MoveRefused => 2,
            Self::JobNotFound | Self::ResourceNotFound | Self::AttentionNotFound => 3,
            Self::JobTerminal
            | Self::JobConflict
            | Self::OperationConflict
            | Self::ResourceConflict
            | Self::StaleRun => 5,
        }
    }
}
