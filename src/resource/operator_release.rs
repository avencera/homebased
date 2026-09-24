//! Operator attestation that unprovable ended GPU work no longer holds its GPU
//!
//! A direct-segment trainer can end before the supervisor binds its trainer
//! attempt, so no saved lock can prove that its detached worker exited. This
//! includes a first background launch or a return task that ends before its
//! confirmed start registers it, because only a registered trainer can bind an
//! attempt. A native foreground return task can be lost, or end without a
//! confirmed process-group exit, so the task layer cannot prove that its process
//! released the GPU. The automatic release proof stays fail-closed for all of
//! them. An operator who inspected the authority machine can instead record one
//! explicit, auditable attestation. The authority saves it with an evidence
//! snapshot and the resulting queue or loan transition in one transaction
//!
//! An attestation is a human trust decision. It is never a confirmed
//! process-group exit, a trainer-lock release, or a resumable checkpoint, and
//! every record that it produces says so through its own typed variant

use serde::{Deserialize, Serialize};

pub use super::id::OperatorAttestationId;
use super::ownership_lock::TrainerRequestDigest;
use super::{ActionId, Loan, LoanId, ResourceId, ResourceRequest, ResourceRevision};
use super::{SupervisorNotice, TaskId};
use crate::domain::{ContainerExitEvidence, ExitReason, ProcessGroupExitEvidence, ProcessStatus};
use crate::machine::MachineId;
use crate::submission::{NormalizedSpecSha256, RequestId};

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
/// `NoLoan` and `AwaitingRelease` name the registered trainer
/// `FirstBackgroundLaunch` and `RestoringReturn` name a trainer that ended
/// before its confirmed start registered it. `RestoringForegroundReturn` names
/// a native foreground return task, which is never registered
///
/// Decoding refuses unknown fields on every variant, including `NoLoan`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    deny_unknown_fields,
    from = "StrictStateBinding"
)]
pub enum OperatorStateBinding {
    /// No non-closed loan reserves the resource, and the task is the registered trainer
    NoLoan,
    /// One exact release action waits for the registered trainer
    AwaitingRelease {
        /// Loan that owns the release action
        loan_id: LoanId,
        /// Release action that the attestation resolves
        action_id: ActionId,
    },
    /// The latest first background launch ended before its confirmed start, with no loan
    ///
    /// The task is the launch task, which was never registered
    FirstBackgroundLaunch {
        /// Stable launch request identity
        request_id: RequestId,
    },
    /// The direct-segment return task of one Restoring loan ended before its confirmed start
    ///
    /// The task is the loan's bound return task, which was never registered
    RestoringReturn {
        /// Restoring loan that keeps the resource reserved
        loan_id: LoanId,
        /// Return action that bound the task
        action_id: ActionId,
    },
    /// The native foreground return task of one Restoring loan ended or was lost
    /// without proof that its process group released the GPU
    ///
    /// The task is the loan's bound return task. It is terminal or lost, and
    /// its saved decision accepted it as native foreground work
    RestoringForegroundReturn {
        /// Restoring loan that keeps the resource reserved
        loan_id: LoanId,
        /// Return action that bound the task
        action_id: ActionId,
    },
}

/// Decoding shape of [`OperatorStateBinding`]
///
/// Serde ignores extra fields on a unit variant of an internally tagged enum,
/// so `no_loan` decodes through an empty struct variant that refuses them. The
/// JSON shape is the same as the public enum
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum StrictStateBinding {
    NoLoan {},
    AwaitingRelease {
        loan_id: LoanId,
        action_id: ActionId,
    },
    FirstBackgroundLaunch {
        request_id: RequestId,
    },
    RestoringReturn {
        loan_id: LoanId,
        action_id: ActionId,
    },
    RestoringForegroundReturn {
        loan_id: LoanId,
        action_id: ActionId,
    },
}

impl From<StrictStateBinding> for OperatorStateBinding {
    fn from(value: StrictStateBinding) -> Self {
        match value {
            StrictStateBinding::NoLoan {} => Self::NoLoan,
            StrictStateBinding::AwaitingRelease { loan_id, action_id } => {
                Self::AwaitingRelease { loan_id, action_id }
            }
            StrictStateBinding::FirstBackgroundLaunch { request_id } => {
                Self::FirstBackgroundLaunch { request_id }
            }
            StrictStateBinding::RestoringReturn { loan_id, action_id } => {
                Self::RestoringReturn { loan_id, action_id }
            }
            StrictStateBinding::RestoringForegroundReturn { loan_id, action_id } => {
                Self::RestoringForegroundReturn { loan_id, action_id }
            }
        }
    }
}

impl OperatorStateBinding {
    /// Whether the attested task must be the registered trainer
    #[must_use]
    pub const fn names_registered_trainer(&self) -> bool {
        match self {
            Self::NoLoan | Self::AwaitingRelease { .. } => true,
            Self::FirstBackgroundLaunch { .. }
            | Self::RestoringReturn { .. }
            | Self::RestoringForegroundReturn { .. } => false,
        }
    }

    /// Restoring loan and return action that the binding names, if any
    #[must_use]
    pub const fn restoring_action(&self) -> Option<(LoanId, ActionId)> {
        match *self {
            Self::RestoringReturn { loan_id, action_id }
            | Self::RestoringForegroundReturn { loan_id, action_id } => Some((loan_id, action_id)),
            Self::NoLoan | Self::AwaitingRelease { .. } | Self::FirstBackgroundLaunch { .. } => {
                None
            }
        }
    }

    fn has_nil_identity(&self) -> bool {
        matches!(self, Self::FirstBackgroundLaunch { request_id } if request_id.0.is_nil())
    }
}

/// Immutable operator request that one ended trainer no longer holds its GPU
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorGpuFreeAttestation {
    /// Stable caller retry identity
    pub operation_id: OperatorAttestationId,
    /// Resource whose trainer ended
    pub resource_id: ResourceId,
    /// Authority machine that the operator inspected
    pub authority_machine: MachineId,
    /// Exact task whose GPU work the operator attests is gone
    ///
    /// It is the registered trainer, the launch task, or the bound return task,
    /// as `state_binding` requires
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
        if self.authority_machine.as_uuid().is_nil()
            || self.task_id.0.is_nil()
            || self.state_binding.has_nil_identity()
        {
            return Err(OperatorGpuFreeRefusal::InvalidIdentity);
        }
        if self.observation.as_str().trim().is_empty() {
            return Err(OperatorGpuFreeRefusal::EmptyObservation);
        }
        Ok(())
    }
}

/// Task-layer end of the attested task when the attestation committed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AttestedTrainerEnd {
    /// The wrapper wrote an exit reason
    Finished {
        /// Task-layer outcome
        outcome: ExitReason,
        /// Wrapper process-group evidence as saved
        ///
        /// It never covers a detached trainer worker. For a native foreground
        /// return it is the saved task-layer fact, often `unconfirmed`; the
        /// attestation does not upgrade it to a confirmed exit
        process_group_exit: ProcessGroupExitEvidence,
        /// Container evidence as saved, for a container task only
        ///
        /// The attestation does not upgrade it to confirmed evidence
        #[serde(default, skip_serializing_if = "Option::is_none")]
        container_exit: Option<ContainerExitEvidence>,
    },
    /// The wrapper was lost with no exit reason
    Lost,
}

/// Resource launch that bound the attested task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AttestedTrainerLaunch {
    /// First background launch that bound the task
    FirstBackgroundLaunch {
        /// Stable launch request identity
        request_id: RequestId,
    },
    /// Direct-segment return decision that bound the task
    DirectSegmentReturn {
        /// Return action that bound the task
        action_id: ActionId,
        /// Stable return request identity
        request_id: RequestId,
    },
    /// Native foreground return decision that bound the task
    ///
    /// The saved decision accepted the task as native foreground work, which
    /// holds the GPU only through its own process group
    NativeForegroundReturn {
        /// Return action that bound the task
        action_id: ActionId,
        /// Stable return request identity
        request_id: RequestId,
    },
    /// Container return decision that bound the task
    ///
    /// The saved decision accepted the task as a container, which holds the
    /// GPU through the container that Homebased started
    ContainerReturn {
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
        attempt_request_sha256: TrainerRequestDigest,
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
    /// Resource launch that bound the trainer
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
    /// The Restoring loan closed and an idle loan serves the oldest queued request
    RestoreClosedServing {
        /// Restoring loan closed with the operator-attested end
        closed: Box<Loan>,
        /// New Serving loan that names this attestation as its idle boundary
        loan: Loan,
        /// Request selected in the same transaction
        request: ResourceRequest,
    },
    /// The Restoring loan closed with an empty queue, and its closure is the idle boundary
    RestoreClosedIdleBoundary {
        /// Restoring loan closed with the operator-attested end
        closed: Loan,
    },
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
    /// The task is not the task that the named launch or return action bound
    #[error("task {task_id} is not the task bound by the attested launch or action")]
    NotBoundTask {
        /// Task named by the attestation
        task_id: TaskId,
        /// Task bound by the launch or return action
        bound: TaskId,
    },
    /// The latest first background launch is not the named launch awaiting release
    ///
    /// It is another launch, it was registered or superseded, or it already has
    /// automatic proof that no child started
    #[error("first background launch {request_id:?} does not await an operator release")]
    LaunchNotAwaitingRelease {
        /// Launch named by the attestation
        request_id: RequestId,
        /// Latest first background launch, if any
        current_launch: Option<RequestId>,
    },
    /// The named loan is not in its Restoring phase
    #[error("loan {loan_id:?} is not restoring")]
    LoanNotRestoring {
        /// Current non-closed loan
        loan_id: LoanId,
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
    /// No saved resource launch of the binding's execution mode and matching accepted identity name the task
    ///
    /// A direct-segment binding needs direct-segment work, and a foreground
    /// binding needs native foreground work
    #[error(
        "task {task_id} has no matching resource launch, execution mode, and accepted identity"
    )]
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
