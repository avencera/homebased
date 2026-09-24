//! Operator attestation that an unprovable ended trainer no longer holds its GPU
//!
//! A direct-segment trainer can end before the supervisor binds its trainer
//! attempt, so no saved lock can prove that its detached worker exited. The
//! automatic release proof stays fail-closed for that trainer. An operator who
//! inspected the authority machine can instead record one explicit, auditable
//! attestation. The authority saves it with an evidence snapshot and the
//! resulting queue or loan transition in one transaction
//!
//! An attestation is a human trust decision. It is never a confirmed
//! process-group exit, a trainer-lock release, or a resumable checkpoint, and
//! every record that it produces says so through its own typed variant

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{ActionId, Loan, LoanId, ResourceId, ResourceRequest, ResourceRevision};
use super::{SupervisorNotice, TaskId};
use crate::domain::{ExitReason, ProcessGroupExitEvidence, ProcessStatus};
use crate::machine::MachineId;
use crate::submission::{NormalizedSpecSha256, RequestId};

/// Stable caller identity of one operator attestation
///
/// The caller allocates it before the first send. An exact retry returns the
/// saved receipt, and different content under the same identity conflicts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperatorAttestationId(Uuid);

impl OperatorAttestationId {
    /// Allocate a new attestation identity
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wrap an existing UUID
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Borrow the underlying UUID value
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for OperatorAttestationId {
    fn default() -> Self {
        Self::new()
    }
}

/// Non-empty operator account of what was inspected and why the GPU is free
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperatorObservation(String);

impl OperatorObservation {
    /// Borrow the saved observation text
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for OperatorObservation {
    type Error = OperatorGpuFreeRefusal;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.trim().is_empty() {
            return Err(OperatorGpuFreeRefusal::EmptyObservation);
        }
        Ok(Self(value))
    }
}

impl From<OperatorObservation> for String {
    fn from(value: OperatorObservation) -> Self {
        value.0
    }
}

/// Explicit human confirmation that the operator checked the GPU on the authority
///
/// The field has one value and no default, so a request that omits it cannot
/// decode or be built
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorGpuFreeConfirmation {
    /// The operator confirmed that no GPU work of the named trainer remains
    OperatorConfirmedGpuFree,
}

/// Resource state that the operator observed and attests against
///
/// The authority compares it with the current state in the deciding
/// transaction, so an attestation never applies to a state it did not name
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorStateBinding {
    /// No non-closed loan reserves the resource
    NoLoan,
    /// One exact release action waits for the registered trainer
    AwaitingRelease {
        /// Loan that owns the release action
        loan_id: LoanId,
        /// Release action that the attestation resolves
        action_id: ActionId,
    },
}

/// Immutable operator request that one ended trainer no longer holds its GPU
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorGpuFreeAttestation {
    /// Stable caller retry identity
    pub operation_id: OperatorAttestationId,
    /// Resource whose registered trainer ended
    pub resource_id: ResourceId,
    /// Authority machine that the operator inspected
    pub authority_machine: MachineId,
    /// Exact registered trainer task that the operator attests is gone
    pub task_id: TaskId,
    /// Resource revision that the operator observed
    pub expected_state_revision: ResourceRevision,
    /// Loan state that the operator observed
    pub state_binding: OperatorStateBinding,
    /// What the operator inspected and why the GPU work is gone
    pub observation: OperatorObservation,
    /// Explicit human confirmation
    pub confirmation: OperatorGpuFreeConfirmation,
}

impl OperatorGpuFreeAttestation {
    /// Check the identities that a well-formed attestation needs
    pub fn validate(&self) -> Result<(), OperatorGpuFreeRefusal> {
        let binding_nil = match self.state_binding {
            OperatorStateBinding::NoLoan => false,
            OperatorStateBinding::AwaitingRelease { loan_id, action_id } => {
                loan_id.as_uuid().is_nil() || action_id.as_uuid().is_nil()
            }
        };
        if self.operation_id.as_uuid().is_nil()
            || self.resource_id.as_uuid().is_nil()
            || self.authority_machine.as_uuid().is_nil()
            || self.task_id.0.is_nil()
            || binding_nil
        {
            return Err(OperatorGpuFreeRefusal::InvalidIdentity);
        }
        if self.observation.as_str().trim().is_empty() {
            return Err(OperatorGpuFreeRefusal::EmptyObservation);
        }
        Ok(())
    }
}

/// Task-layer end of the attested trainer when the attestation committed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AttestedTrainerEnd {
    /// The wrapper wrote an exit reason
    Finished {
        /// Task-layer outcome
        outcome: ExitReason,
        /// Wrapper process-group evidence as saved, which never covers the detached worker
        process_group_exit: ProcessGroupExitEvidence,
    },
    /// The wrapper was lost with no exit reason
    Lost,
}

/// Resource launch that made the attested task the registered trainer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AttestedTrainerLaunch {
    /// First background launch whose confirmed start registered the task
    FirstBackgroundLaunch {
        /// Stable launch request identity
        request_id: RequestId,
    },
    /// Direct-segment return task whose confirmed start registered the task
    DirectSegmentReturn {
        /// Return action that bound the task
        action_id: ActionId,
        /// Stable return request identity
        request_id: RequestId,
    },
}

/// Trainer-attempt association of the attested task when the attestation committed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AttestedTrainerAssociation {
    /// No attempt was bound, so no saved lock names the worker
    Missing,
    /// An attempt was bound, but its release proof was not used
    Saved {
        /// Request digest of the saved attempt
        attempt_request_sha256: String,
    },
}

/// Authority evidence snapshot saved with the attestation
///
/// These facts show which run the operator resolved. None of them proves that
/// the trainer worker exited
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorGpuFreeEvidence {
    /// Task-layer end of the trainer
    pub trainer_end: AttestedTrainerEnd,
    /// Resource launch that registered the trainer
    pub trainer_launch: AttestedTrainerLaunch,
    /// Digest of the accepted executor identity's normalized spec
    pub normalized_spec_sha256: NormalizedSpecSha256,
    /// Trainer-attempt association state
    pub trainer_association: AttestedTrainerAssociation,
}

/// Queue or loan transition committed with the attestation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorGpuFreeOutcome {
    /// The release action closed and the oldest queued request now serves
    ReleaseResolvedServing {
        /// Loan moved from AwaitingRelease to Serving
        loan: Loan,
        /// Request selected in the same transaction
        request: ResourceRequest,
    },
    /// The release action closed with an empty queue; the supervisor owns the return
    ReleaseResolvedReturnRequired {
        /// Loan moved from AwaitingRelease to AwaitingReturn
        loan: Loan,
        /// Return notice saved in the same transaction
        notice: SupervisorNotice,
    },
    /// The registration cleared and an idle loan serves the oldest queued request
    IdleServing {
        /// New Serving loan that names this attestation as its idle boundary
        loan: Loan,
        /// Request selected in the same transaction
        request: ResourceRequest,
    },
    /// The registration cleared and this attestation is the saved idle boundary
    ///
    /// A later queue reconciliation or first background launch reads it
    IdleBoundary,
}

/// Durable receipt of one committed operator attestation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorGpuFreeReceipt {
    /// Exact attestation content
    pub attestation: OperatorGpuFreeAttestation,
    /// Authority evidence snapshot
    pub evidence: OperatorGpuFreeEvidence,
    /// Resource revision committed with the transition
    pub state_revision: ResourceRevision,
    /// Committed transition
    pub outcome: OperatorGpuFreeOutcome,
}

/// Result of one attestation call
#[derive(Debug, Clone)]
pub struct OperatorGpuFreeResolution {
    /// Saved receipt
    pub receipt: OperatorGpuFreeReceipt,
    /// Whether an earlier call committed this receipt
    pub replayed: bool,
}

/// Why the authority refused an attestation without writing any record
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorGpuFreeRefusal {
    /// An identity is nil
    #[error("operator attestation identities must not be nil")]
    InvalidIdentity,
    /// The observation has no text
    #[error("operator observation must not be empty")]
    EmptyObservation,
    /// The resource does not exist on this authority
    #[error("resource not found")]
    ResourceNotFound,
    /// The attestation or daemon names another authority
    #[error("resource authority is {expected}, not {found}")]
    WrongAuthority {
        /// Authority saved on the resource
        expected: MachineId,
        /// Authority named by the attestation or the daemon
        found: MachineId,
    },
    /// The operation identity already names different content
    #[error("operator attestation {operation_id:?} was retried with different content")]
    ConflictingRetry {
        /// Reused operation identity
        operation_id: OperatorAttestationId,
    },
    /// The resource changed after the operator read it
    #[error("stale resource revision: expected {expected:?}, found {actual:?}")]
    StaleRevision {
        /// Revision that the operator observed
        expected: ResourceRevision,
        /// Current revision
        actual: ResourceRevision,
    },
    /// The task is not the current registered trainer
    #[error("task {task_id} is not the registered background task")]
    NotRegisteredTrainer {
        /// Task named by the attestation
        task_id: TaskId,
        /// Current registration
        registered: Option<TaskId>,
    },
    /// The current loan state differs from the named binding
    #[error("the current loan state differs from the attested binding")]
    LoanStateChanged {
        /// Current non-closed loan, if any
        current_loan: Option<LoanId>,
    },
    /// The loan is past its release phase, so its work may already use the GPU
    #[error("loan {loan_id:?} is not awaiting release")]
    LoanNotAwaitingRelease {
        /// Current non-closed loan
        loan_id: LoanId,
    },
    /// The release action notice is missing or names another task or assignment
    #[error("release action {action_id:?} has an invalid durable notice")]
    InvalidReleaseNotice {
        /// Release action named by the attestation
        action_id: ActionId,
    },
    /// The task row is missing on this authority
    #[error("task {task_id} is missing")]
    TaskMissing {
        /// Task named by the attestation
        task_id: TaskId,
    },
    /// The task has not ended, so its wrapper still owns the GPU
    #[error("task {task_id} has not ended (state {state})")]
    TaskNotEnded {
        /// Task named by the attestation
        task_id: TaskId,
        /// Current task-layer state
        state: ProcessStatus,
    },
    /// No saved resource launch and matching accepted identity name the task
    #[error("task {task_id} has no matching resource launch and accepted identity")]
    TrainerLaunchUnproven {
        /// Task named by the attestation
        task_id: TaskId,
    },
    /// Saved launch history does not match the registration
    #[error("resource launch history is inconsistent with task {task_id}")]
    InconsistentHistory {
        /// Task named by the attestation
        task_id: TaskId,
    },
    /// The resource revision cannot be incremented
    #[error("resource revision {revision:?} cannot be incremented")]
    RevisionExhausted {
        /// Current revision
        revision: ResourceRevision,
    },
}
