//! Shared failures for authority-local resource storage

use crate::error::AppError;
use std::fmt;

use super::codec::StoredReadError;
use crate::domain::TaskId;
use crate::machine::MachineId;
use crate::resource::{CommandSpecError, ResourceId, ResourceTaskOwnershipRisk};
use crate::store::IdentityError;

/// Failure to accept or read authority-local resource data
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResourceStoreError {
    /// Saved authority state contradicts the requested identity or transition
    #[error("resource identity conflict: {0}")]
    Conflict(ConflictReason),
    /// The resource identity was first registered with different content
    #[error("resource {resource:?} is already registered with different content")]
    RegistrationConflict {
        /// Resource whose saved registration differs
        resource: ResourceId,
    },
    /// The request was cancelled before it entered the resource queue
    #[error("resource request was prevented before acceptance")]
    Prevented,
    /// The referenced resource does not exist
    #[error("resource not found")]
    ResourceNotFound,
    /// The local daemon does not own the resource authority
    #[error("resource authority mismatch: expected {expected}, found {found}")]
    WrongAuthority {
        /// Fixed authority recorded for this resource
        expected: MachineId,
        /// Machine identity supplied by the daemon
        found: MachineId,
    },
    /// The local origin route required for callback delivery is missing
    #[error("resource callback route for task {task} is missing")]
    OriginRouteNotFound {
        /// Preallocated task identity whose saved origin route is missing
        task: TaskId,
    },
    /// A prior ordinary execution acceptance won before queue cancellation
    #[error("task identity {task} was accepted before resource cancellation")]
    ExecutorAlreadyAccepted {
        /// Task identity that was already accepted
        task: TaskId,
    },
    /// The executor identity table rejected this atomic resource transition
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// An agent workload cannot enter the finite command queue
    #[error(transparent)]
    InvalidCommandSpec(#[from] CommandSpecError),
    /// The command falls outside the foreground ownership contract, so its end cannot release the resource
    #[error("resource command can outlive or hide from its task process group ({risk:?})")]
    UnsupportedCommandOwnership {
        /// First contract violation found in the command or its entry point
        risk: ResourceTaskOwnershipRisk,
    },
    /// The saved command cannot be prepared on the executor machine
    #[error("resource command cannot be prepared: {0}")]
    TaskPreparation(#[from] AppError),
    /// The accepted resource task is inconsistent with its durable task row
    #[error("accepted resource task storage is inconsistent: {0}")]
    TaskRow(AppError),
    /// A saved return decision could not be read for a resource view
    #[error("saved return decision could not be read: {0}")]
    ReturnDecisionRead(String),
    /// The first durable task event is missing or conflicts with its identity
    #[error(transparent)]
    Event(#[from] crate::events::EventError),
    /// A saved record cannot be decoded or fails its integrity check, so a retry cannot succeed
    #[error("corrupt stored {what}: {reason}")]
    CorruptRecord {
        /// Kind of record that failed to decode
        what: &'static str,
        /// Decoder or integrity failure
        reason: String,
    },
    /// SQLite failed
    #[error("resource storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

impl ResourceStoreError {
    /// Classify one saved record that cannot be decoded or verified
    pub(crate) fn corrupt(what: &'static str, reason: impl fmt::Display) -> Self {
        Self::CorruptRecord {
            what,
            reason: reason.to_string(),
        }
    }
}

impl From<StoredReadError> for ResourceStoreError {
    fn from(error: StoredReadError) -> Self {
        match error {
            StoredReadError::Storage(error) => Self::Storage(error),
            StoredReadError::Corrupt { what, reason } => Self::CorruptRecord { what, reason },
        }
    }
}

/// Failure to bind or read durable trainer-attempt evidence on its resource authority
#[derive(Debug, thiserror::Error)]
pub(crate) enum TrainerAttemptAssociationStoreError {
    /// Resource existence or authority ownership failed
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// The task does not match the resource's registered background task
    #[error("task {task_id} is not the registered background task")]
    TaskNotRegistered {
        /// Task supplied for the association
        task_id: TaskId,
    },
    /// The registered task row is missing on this authority
    #[error("registered background task {task_id} is missing locally")]
    TaskMissing {
        /// Exact task registered on the resource
        task_id: TaskId,
    },
    /// The registered task row is not running
    #[error("registered background task {task_id} is not running (state {state})")]
    TaskNotRunning {
        /// Exact task registered on the resource
        task_id: TaskId,
        /// Process state saved on this authority
        state: String,
    },
    /// No accepted executor identity exists for the registered task
    #[error("accepted executor identity for task {task_id} is missing")]
    IdentityMissing {
        /// Exact task registered on the resource
        task_id: TaskId,
    },
    /// The executor identity is a rejection, not an accepted task
    #[error("executor identity for task {task_id} is not accepted")]
    IdentityNotAccepted {
        /// Exact task registered on the resource
        task_id: TaskId,
    },
    /// The accepted identity does not belong to this task or authority
    #[error("accepted executor identity for task {task_id} has a different owner")]
    IdentityMismatch {
        /// Exact task registered on the resource
        task_id: TaskId,
    },
    /// The accepted executor identity is not running
    #[error("accepted executor identity for task {task_id} is not running")]
    IdentityNotRunning {
        /// Exact task registered on the resource
        task_id: TaskId,
    },
    /// The accepted identity has no current normalized request spec
    #[error("accepted executor identity for task {task_id} has no normalized spec")]
    NormalizedSpecMissing {
        /// Exact task registered on the resource
        task_id: TaskId,
    },
    /// The accepted normalized spec does not match the local task row
    #[error("accepted normalized spec does not match task row {task_id}")]
    NormalizedSpecMismatch {
        /// Exact task registered on the resource
        task_id: TaskId,
    },
    /// The accepted task binding changed after its command shape was checked
    #[error("accepted task binding for task {task_id} changed during command-shape validation")]
    BindingChanged {
        /// Exact task whose accepted binding changed
        task_id: TaskId,
    },
    /// The accepted command does not match the maintained direct-segment contract
    #[error(transparent)]
    DirectSegmentCommandShape(
        #[from] crate::resource::command_shape::DirectSegmentCommandShapeError,
    ),
    /// A non-closed loan does not await release of this exact task
    #[error("resource {resource_id:?} has an incompatible active loan")]
    ActiveLoanConflict {
        /// Resource whose active loan blocks this association
        resource_id: ResourceId,
    },
    /// The resource already has a different immutable association
    #[error(
        "trainer attempt association for resource {resource_id:?} conflicts with saved evidence"
    )]
    Conflict {
        /// Resource whose immutable association conflicts with this request
        resource_id: ResourceId,
    },
    /// The task is already associated with another resource
    #[error("trainer task {task_id} is already associated with another resource")]
    TaskAlreadyAssociated {
        /// Task already owned by a trainer association
        task_id: TaskId,
    },
    /// Persisted association data is malformed or disagrees with its row identity
    #[error("invalid saved trainer attempt association: {0}")]
    InvalidStoredAssociation(String),
    /// A validated task row or JSON conversion failed
    #[error(transparent)]
    App(#[from] AppError),
    /// Executor identity storage failed
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// SQLite or stored data failed
    #[error("trainer attempt association storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Saved authority state that a resource transition contradicts
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConflictReason {
    /// A request identity was reused with a different task, resource, or origin
    RequestIdentityMismatch,
    /// An exact request retry carries a different command
    RequestSpecMismatch,
    /// The selected request is missing from the authority queue
    RequestMissing,
    /// The request left the state that this transition expects
    RequestStateChanged,
    /// An earlier request in the authority FIFO is still queued or assigned
    EarlierRequestActive,
    /// A prevention record holds a different identity for this request or task
    PreventionIdentityMismatch,
    /// The task identity already belongs to a request, task row, or executor identity
    TaskIdentityInUse,
    /// The task row or its first event disagrees with the accepted request
    TaskRowMismatch,
    /// The executor identity is a rejection or disagrees with the request
    ExecutorIdentityMismatch,
    /// The saved origin route disagrees with the request
    OriginRouteMismatch,
    /// The resource revision changed before the transition committed
    ResourceRevisionChanged,
    /// The resource revision cannot advance past its maximum value
    RevisionExhausted,
    /// The resource supervisor or registered background task changed
    ResourceAssignmentChanged,
    /// The loan changed or no longer owns this request
    LoanChanged,
    /// The loan is not in the release action that this transition expects
    ReleaseActionChanged,
    /// The release notice is missing or disagrees with its action
    ReleaseNoticeMismatch,
    /// The release watcher identity is nil or reuses the observed task or request
    WatcherIdentityInvalid,
    /// Another loan, request, or task already claims the release watcher identity
    WatcherIdentityClaimed,
    /// A different release watcher is already bound to this action
    WatcherIntentMismatch,
    /// The Serving loan has no verified release provenance
    ServingReleaseUnverified,
    /// The supervisor notice for this transition conflicts with a saved notice
    SupervisorNoticeRejected,
    /// The saved registration receipt names a different resource
    RegistrationReceiptMismatch,
    /// A cancellation disagrees with its saved receipt or route proof
    CancellationReceiptMismatch,
}

impl ConflictReason {
    /// Stable description for logs and API error messages
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RequestIdentityMismatch => "request identity belongs to different content",
            Self::RequestSpecMismatch => "request retry carries a different command",
            Self::RequestMissing => "request is missing from the authority queue",
            Self::RequestStateChanged => "request state changed",
            Self::EarlierRequestActive => "an earlier queued request is still active",
            Self::PreventionIdentityMismatch => "prevention record holds a different identity",
            Self::TaskIdentityInUse => "task identity is already in use",
            Self::TaskRowMismatch => "task row disagrees with the accepted request",
            Self::ExecutorIdentityMismatch => "executor identity disagrees with the request",
            Self::OriginRouteMismatch => "origin route disagrees with the request",
            Self::ResourceRevisionChanged => "resource revision changed",
            Self::RevisionExhausted => "resource revision cannot advance",
            Self::ResourceAssignmentChanged => "resource assignment changed",
            Self::LoanChanged => "loan changed",
            Self::ReleaseActionChanged => "release action changed",
            Self::ReleaseNoticeMismatch => "release notice is missing or differs",
            Self::WatcherIdentityInvalid => "release watcher identity is invalid",
            Self::WatcherIdentityClaimed => "release watcher identity is already claimed",
            Self::WatcherIntentMismatch => "a different release watcher is bound",
            Self::ServingReleaseUnverified => "serving release provenance is unverified",
            Self::SupervisorNoticeRejected => "supervisor notice conflicts with saved state",
            Self::RegistrationReceiptMismatch => "registration receipt names another resource",
            Self::CancellationReceiptMismatch => "cancellation disagrees with its saved receipt",
        }
    }
}

impl fmt::Display for ConflictReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
