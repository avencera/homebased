//! Durable fleet submission identities, separate from process and callback delivery

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::dependency::{HeldCancellation, TaskDependencies};
use crate::domain::{ProcessStatus, TaskEnv, TaskId, TaskStatus, ThreadId};
use crate::machine::MachineId;
use crate::spec::NormalizedSpec;

/// The executable saved for callbacks owned by the origin
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CallbackExecutable {
    /// Resolved executable path captured when the route was accepted
    Available {
        /// Absolute path to the Codex executable
        path: PathBuf,
    },
    /// Resolution failed when the route was accepted
    Unavailable {
        /// Durable reason shown when callback delivery settles as failed
        reason: String,
    },
}

impl CallbackExecutable {
    /// Wrap a resolved executable path
    #[must_use]
    pub fn available(path: PathBuf) -> Self {
        Self::Available { path }
    }

    /// Return the executable path when it was resolved
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Available { path } => Some(path),
            Self::Unavailable { .. } => None,
        }
    }

    /// Return the resolution failure when no executable could be resolved
    #[must_use]
    pub fn unavailable_reason(&self) -> Option<&str> {
        match self {
            Self::Available { .. } => None,
            Self::Unavailable { reason } => Some(reason),
        }
    }
}

impl From<PathBuf> for CallbackExecutable {
    fn from(path: PathBuf) -> Self {
        Self::available(path)
    }
}

/// Stable caller retry identity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(pub Uuid);

impl RequestId {
    /// Allocate a new request identity
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

/// Local context owned by the origin, never sent to an executor
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallbackContext {
    /// The submitter's environment
    pub env: TaskEnv,
    /// Absolute local callback directory
    pub cwd: PathBuf,
    /// Resolved local Codex executable
    pub codex: CallbackExecutable,
}

/// Durable phase of one origin route held until its dependencies succeed
///
/// A held route has no task row and no executor identity. Its dependencies
/// are saved beside it, and only the origin's dependency release moves it on:
/// a local task goes straight to [`SubmissionState::Accepted`] with its row,
/// and a remote one goes through [`Self::Launching`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HeldPhase {
    /// At least one dependency has not finished
    Waiting,
    /// Every dependency succeeded and the launch may have reached the remote executor
    ///
    /// The launch is resent with the same task UUID until the executor answers,
    /// so cancellation from here must reach the executor
    Launching,
    /// The origin cancelled the task before any launch
    Cancelled {
        /// Dependency ending or request that cancelled it
        cause: HeldCancellation,
    },
}

impl HeldPhase {
    /// Public status of a task in this phase
    #[must_use]
    pub fn status(&self) -> TaskStatus {
        match self {
            Self::Waiting | Self::Launching => TaskStatus::Held,
            Self::Cancelled { .. } => TaskStatus::Process(ProcessStatus::Cancelled),
        }
    }
}

/// Why a saved origin route breaks its invariants
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    /// The route and normalized spec must retain one exact thread identity
    #[error("route thread does not match its normalized spec")]
    ThreadMismatch,
    /// A held route cannot have accepted executor events
    #[error("route phase does not agree with its event cursor")]
    InvalidEventCursor,
}

/// Definitive result or unresolved sent request
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubmissionState {
    /// The send may have reached the executor
    AcceptanceUnknown,
    /// The executor durably accepted this identity
    Accepted,
    /// The executor durably rejected this identity
    Rejected { reason: String },
    /// This task identity waits on its origin for its dependencies to succeed
    Held {
        /// Hold, launch, or cancellation phase
        phase: HeldPhase,
    },
}

/// Origin-owned request mapping and callback route
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginRoute {
    /// Caller retry UUID
    pub request: RequestId,
    /// Global task UUID
    pub task: TaskId,
    /// Machine that owns callbacks
    pub origin_machine: MachineId,
    /// Fixed execution owner
    pub execution_machine: MachineId,
    /// Original Codex thread
    pub thread: ThreadId,
    /// Origin-only callback environment and directory
    pub callback: CallbackContext,
    /// Normalized request used for conflict checks
    pub spec: NormalizedSpec,
    /// Submission result, independent of process state
    pub submission: SubmissionState,
    /// Last execution status learned from events
    pub last_execution_state: Option<ProcessStatus>,
    /// Time of the last route state update
    pub last_updated_at: DateTime<Utc>,
    /// Last accepted event sequence
    pub last_accepted_seq: u64,
    /// Last settled callback sequence
    pub last_settled_seq: u64,
}

/// One origin route with the dependencies it was submitted with
///
/// The store saves the dependencies beside the route, like its spec, so they
/// stay on the origin and never enter the route summary that peers read
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependentRoute {
    /// Saved origin route
    pub route: OriginRoute,
    /// Tasks that must succeed before it launches
    pub after: TaskDependencies,
}

/// Exact identity and origin-owned context used to create a held route
#[derive(Debug, Clone)]
pub struct NewHeldRoute {
    /// Caller retry UUID
    pub request: RequestId,
    /// Global task UUID assigned before any launch
    pub task: TaskId,
    /// Machine that accepted the submit and owns callbacks
    pub origin_machine: MachineId,
    /// Machine that will run the task: the origin, or the spec's Fleet machine
    pub execution_machine: MachineId,
    /// Callback context captured from the submitting shell
    pub callback: CallbackContext,
    /// Normalized request content
    pub spec: NormalizedSpec,
}

impl OriginRoute {
    /// Create a route held on its origin until its dependencies succeed
    ///
    /// The task UUID is assigned now, so the eventual launch, its retries, and
    /// every later event use the identity that the submit returned
    #[must_use]
    pub fn new_held(input: NewHeldRoute) -> Self {
        let NewHeldRoute {
            request,
            task,
            origin_machine,
            execution_machine,
            callback,
            spec,
        } = input;
        Self {
            request,
            task,
            origin_machine,
            execution_machine,
            thread: spec.thread,
            callback,
            spec,
            submission: SubmissionState::Held {
                phase: HeldPhase::Waiting,
            },
            last_execution_state: None,
            last_updated_at: Utc::now(),
            last_accepted_seq: 0,
            last_settled_seq: 0,
        }
    }

    /// Validate held route invariants; other submission states carry none
    pub fn validate(&self) -> Result<(), RouteError> {
        match &self.submission {
            SubmissionState::Held { phase } => self.validate_held_route(phase),
            SubmissionState::AcceptanceUnknown
            | SubmissionState::Accepted
            | SubmissionState::Rejected { .. } => Ok(()),
        }
    }

    fn validate_held_route(&self, phase: &HeldPhase) -> Result<(), RouteError> {
        if self.thread != self.spec.thread {
            return Err(RouteError::ThreadMismatch);
        }
        // no executor event can reach a route that never launched; a
        // cancelled one carries only the origin's own terminal event
        let max_seq = match phase {
            HeldPhase::Waiting | HeldPhase::Launching => 0,
            HeldPhase::Cancelled { .. } => 1,
        };
        if self.last_accepted_seq > max_seq
            || self.last_settled_seq > self.last_accepted_seq
            || self.last_execution_state.is_some()
        {
            return Err(RouteError::InvalidEventCursor);
        }
        Ok(())
    }
}

/// Executor-owned accepted task identity, retained after detail cleanup
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRecord {
    /// Global task UUID
    pub task: TaskId,
    /// Machine that owns callbacks
    pub origin_machine: MachineId,
    /// Execution owner
    pub execution_machine: MachineId,
    /// Normalized request used for conflict checks
    pub spec: NormalizedSpec,
    /// Retained process state
    pub state: ProcessStatus,
}

/// A definitive rejection that prevents delayed submission
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectionTombstone {
    /// Global task UUID
    pub task: TaskId,
    /// Origin bound to the rejected UUID
    pub origin_machine: MachineId,
    /// Execution owner bound to the rejected UUID
    pub execution_machine: MachineId,
    /// Durable rejection reason
    pub reason: String,
}

/// One durable executor identity for a task UUID
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutorIdentity {
    /// The executor accepted the request and retained its process state
    Accepted(ExecutionRecord),
    /// The UUID can never start a child
    Rejected(RejectionTombstone),
}

/// Definitive rejection reasons created by pre-acceptance transitions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreAcceptanceRejection {
    /// Unknown acceptance was resolved by abandonment
    Abandoned,
    /// Cancellation won the acceptance race
    Cancelled,
}

impl PreAcceptanceRejection {
    /// Durable reason understood by submission recovery
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Abandoned => "abandoned_before_acceptance",
            Self::Cancelled => "cancelled_before_acceptance",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::CallbackExecutable;

    #[test]
    fn callback_executable_round_trips_both_typed_states() {
        let path = PathBuf::from("/bin/codex");
        let available = CallbackExecutable::available(path.clone());
        assert_eq!(
            serde_json::from_value::<CallbackExecutable>(serde_json::to_value(&available).unwrap())
                .unwrap(),
            available
        );
        let unavailable = CallbackExecutable::Unavailable {
            reason: "codex was not found".into(),
        };
        assert_eq!(
            serde_json::from_value::<CallbackExecutable>(
                serde_json::to_value(&unavailable).unwrap()
            )
            .unwrap(),
            unavailable
        );
    }
}
