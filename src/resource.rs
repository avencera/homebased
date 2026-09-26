//! Typed resource, request, loan, and supervisor-notice domain models

use crate::error::AppError;
use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize};

use crate::digest::Sha256Digest;
use crate::domain::{API_VERSION, ExitReason, ProcessStatus, TaskId, ThreadId};
use crate::machine::MachineId;
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::submission::{NormalizedSpecSha256, RequestId, ResourceQueueReceipt};

pub mod api;
pub mod background_launch;
pub mod bound_action;
pub mod command_shape;
pub mod foreground;
mod id;
pub mod initial_idle;
pub mod operator_release;
pub mod ownership_lock;
mod release_checkpoint;
pub mod release_watcher;
pub mod return_window;
pub(crate) mod store;
pub mod trainer_publication;

pub use id::{
    ActionId, DeliveryAttemptId, IdentityParseError, LoanId, NilIdentity, NoticeId,
    ReleaseStopReservationId, ResourceId,
};
pub(crate) use release_checkpoint::{
    ReleaseCheckpointAction, ReleaseCheckpointBaseline, ReleaseCheckpointBinding,
    ReleaseCheckpointCancellation, ReleaseCheckpointPhase, ReleaseCheckpointState,
    ReleaseCheckpointStopDecision, ReleaseCheckpointStopOutcome,
};
pub use return_window::{
    RETURN_DECISION_GRACE, RETURN_DECISION_LIMIT, ReturnDecisionWindow, ReturnHoldRejection,
};

/// Immutable authority-assigned acceptance identity for a resource request
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AcceptanceSequence(u64);

impl AcceptanceSequence {
    /// Wrap an authority-assigned sequence value
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the underlying sequence value
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Revision of resource-owned state used to reject stale actions
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResourceRevision(u64);

impl ResourceRevision {
    /// Wrap a resource state revision
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the underlying revision value
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Return the revision that follows this one, or `None` at the maximum value
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Revision of the resource supervisor assignment
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AssignmentRevision(u64);

impl AssignmentRevision {
    /// Wrap a supervisor assignment revision
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the underlying revision value
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Exact fleet address of the supervisor assigned to a resource
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorAddress {
    /// Machine that owns the supervisor thread
    pub machine: MachineId,
    /// Exact Codex thread that receives supervisor notices
    pub thread: ThreadId,
}

/// One exclusive physical GPU and its fixed authority machine
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    /// Stable resource identity
    pub id: ResourceId,
    /// Human-readable resource name
    pub display_name: String,
    authority_machine: MachineId,
    /// Exact assigned supervisor
    pub supervisor: SupervisorAddress,
    /// Revision of the supervisor assignment
    pub assignment_revision: AssignmentRevision,
    /// Revision of resource-owned state
    pub state_revision: ResourceRevision,
    /// Registered background task identity, if one exists
    pub registered_background_task: Option<TaskId>,
}

impl Resource {
    /// Create a resource with an authority that cannot be changed through this model
    #[must_use]
    pub fn new(
        id: ResourceId,
        display_name: String,
        authority_machine: MachineId,
        supervisor: SupervisorAddress,
        assignment_revision: AssignmentRevision,
        state_revision: ResourceRevision,
        registered_background_task: Option<TaskId>,
    ) -> Self {
        Self {
            id,
            display_name,
            authority_machine,
            supervisor,
            assignment_revision,
            state_revision,
            registered_background_task,
        }
    }

    /// Return the fixed authority machine for this resource
    #[must_use]
    pub const fn authority_machine(&self) -> MachineId {
        self.authority_machine
    }
}

/// Immutable content of the first registration of one resource
///
/// A later replacement changes the current supervisor, so a registration retry
/// compares this saved content and never the mutable resource row
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResourceRegistrationReceipt {
    /// Registered resource identity
    pub(crate) resource_id: ResourceId,
    /// Display name named by the first registration
    pub(crate) display_name: String,
    /// Fixed authority that accepted the first registration
    pub(crate) authority_machine: MachineId,
    /// Supervisor named by the first registration
    pub(crate) initial_supervisor: SupervisorAddress,
}

impl ResourceRegistrationReceipt {
    /// Registration content named by a resource before its first insert
    pub(crate) fn initial(resource: &Resource) -> Self {
        Self {
            resource_id: resource.id,
            display_name: resource.display_name.clone(),
            authority_machine: resource.authority_machine,
            initial_supervisor: resource.supervisor,
        }
    }
}

/// Durable association between one authority-owned resource and one trainer task
///
/// This immutable record keeps exact point-in-time trainer request and lock evidence
/// with the normalized spec digest from the accepted Homebased task identity
/// The Store checks the direct-segment command shape before its first insert
/// This record does not prove that the live process used the lock or that the GPU worker exited
/// It cannot permit release completion or `Serving`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrainerAttemptAssociation {
    resource_id: ResourceId,
    authority_machine: MachineId,
    task_id: TaskId,
    verified_attempt: ownership_lock::VerifiedTrainerAttempt,
    normalized_spec_sha256: NormalizedSpecSha256,
}

impl TrainerAttemptAssociation {
    /// Return the associated authority-owned resource
    #[must_use]
    pub const fn resource_id(&self) -> ResourceId {
        self.resource_id
    }

    /// Return the machine that owns the associated resource
    #[must_use]
    pub const fn authority_machine(&self) -> MachineId {
        self.authority_machine
    }

    /// Return the exact registered Homebased background task
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Return the exact trainer attempt evidence captured at registration time
    #[must_use]
    pub const fn verified_attempt(&self) -> &ownership_lock::VerifiedTrainerAttempt {
        &self.verified_attempt
    }

    /// Return the digest derived from the accepted executor identity's normalized spec
    #[must_use]
    pub const fn normalized_spec_sha256(&self) -> NormalizedSpecSha256 {
        self.normalized_spec_sha256
    }

    pub(crate) fn from_components(
        resource_id: ResourceId,
        authority_machine: MachineId,
        task_id: TaskId,
        verified_attempt: ownership_lock::VerifiedTrainerAttempt,
        normalized_spec_sha256: NormalizedSpecSha256,
    ) -> Result<Self, &'static str> {
        if authority_machine.as_uuid().is_nil() || task_id.0.is_nil() {
            return Err("trainer attempt association identities must not be nil");
        }

        Ok(Self {
            resource_id,
            authority_machine,
            task_id,
            verified_attempt,
            normalized_spec_sha256,
        })
    }
}

/// A normalized specification restricted to finite command or container workloads
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct CommandSpec(NormalizedSpec);

impl CommandSpec {
    /// Borrow the immutable normalized specification
    #[must_use]
    pub const fn as_normalized(&self) -> &NormalizedSpec {
        &self.0
    }
}

impl TryFrom<NormalizedSpec> for CommandSpec {
    type Error = CommandSpecError;

    fn try_from(spec: NormalizedSpec) -> Result<Self, Self::Error> {
        if spec.machine.is_some() {
            return Err(CommandSpecError::ExplicitMachine);
        }

        match &spec.workload {
            NormalizedWorkload::Task(_) => Ok(Self(spec)),
            // resource work holds one exact GPU, so the container must name it
            NormalizedWorkload::Container(container) if container.gpus.is_none() => {
                Err(CommandSpecError::ContainerWithoutGpus)
            }
            NormalizedWorkload::Container(_) => Ok(Self(spec)),
            NormalizedWorkload::Agent(_) => Err(CommandSpecError::AgentWorkload),
        }
    }
}

impl<'de> Deserialize<'de> for CommandSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let spec = NormalizedSpec::deserialize(deserializer)?;
        Self::try_from(spec).map_err(serde::de::Error::custom)
    }
}

/// Why a normalized specification cannot enter the resource queue
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CommandSpecError {
    /// Resource command execution is routed by the resource authority
    #[error("resource command specifications cannot specify a machine")]
    ExplicitMachine,
    /// Agent workloads are open-ended and cannot enter the finite command queue
    #[error("resource requests require a command or container workload")]
    AgentWorkload,
    /// A container on a resource must request its GPUs
    #[error("resource container workloads require gpus")]
    ContainerWithoutGpus,
}

/// Queue and task-result state for a resource request
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceRequestState {
    /// Ready for selection in the authority's serving order
    Queued,
    /// Reserved by one loan and awaiting task-layer execution
    Assigned {
        /// Loan that owns this request
        loan_id: LoanId,
    },
    /// The task ended and its process outcome is known
    Finished {
        /// Existing task-layer process outcome
        outcome: ExitReason,
    },
    /// Cancelled before the command task was activated
    CancelledBeforeLaunch,
    /// Rejected before command activation
    Rejected {
        /// Durable rejection reason
        reason: String,
    },
}

/// Accepted command request waiting for one exclusive resource
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRequest {
    /// Caller-provided retry identity
    pub request_id: RequestId,
    /// Preallocated task identity retained when the command is selected
    pub task_id: TaskId,
    /// Resource authority that accepted the request
    pub resource_id: ResourceId,
    /// Immutable authority-assigned acceptance identity
    pub acceptance_sequence: AcceptanceSequence,
    /// Machine that owns the requesting thread and callback route
    pub origin_machine: MachineId,
    /// Immutable normalized command specification
    spec: CommandSpec,
    /// Resource queue state, separate from task process state
    pub state: ResourceRequestState,
}

impl ResourceRequest {
    /// Create a queued request, rejecting agent workloads before acceptance
    pub fn new(
        request_id: RequestId,
        task_id: TaskId,
        resource_id: ResourceId,
        acceptance_sequence: AcceptanceSequence,
        origin_machine: MachineId,
        normalized_spec: NormalizedSpec,
    ) -> Result<Self, CommandSpecError> {
        Ok(Self {
            request_id,
            task_id,
            resource_id,
            acceptance_sequence,
            origin_machine,
            spec: CommandSpec::try_from(normalized_spec)?,
            state: ResourceRequestState::Queued,
        })
    }

    /// Borrow the immutable command specification
    #[must_use]
    pub const fn spec(&self) -> &CommandSpec {
        &self.spec
    }
}

/// Strict versioned request to queue one command on a resource authority
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceQueueRequest {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Intended resource authority
    pub destination_machine: MachineId,
    /// Machine that owns the requesting thread and callback route
    pub origin_machine: MachineId,
    /// Stable caller retry identity
    pub request_id: RequestId,
    /// Preallocated global task identity
    pub task_id: TaskId,
    /// Resource whose serving queue will receive the command
    pub resource_id: ResourceId,
    /// Command workload with no explicit execution machine
    pub spec: CommandSpec,
}

impl ResourceQueueRequest {
    /// Build a queue request with the current public API version
    #[must_use]
    pub fn new(
        protocol_version: u32,
        destination_machine: MachineId,
        origin_machine: MachineId,
        request_id: RequestId,
        task_id: TaskId,
        resource_id: ResourceId,
        spec: CommandSpec,
    ) -> Self {
        Self {
            api_version: API_VERSION,
            protocol_version,
            destination_machine,
            origin_machine,
            request_id,
            task_id,
            resource_id,
            spec,
        }
    }
}

/// Strict versioned response from a resource queue authority
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceQueueResponse {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Machine that handled the queue request
    pub destination_machine: MachineId,
    /// Definitive receipt bound to the resource route identity
    pub receipt: ResourceQueueReceipt,
}

impl ResourceQueueResponse {
    /// Build a response whose destination matches the receipt authority
    #[must_use]
    pub fn new(protocol_version: u32, receipt: ResourceQueueReceipt) -> Self {
        Self {
            api_version: API_VERSION,
            protocol_version,
            destination_machine: receipt.authority_machine,
            receipt,
        }
    }
}

/// Opaque training and result references retained for the return decision
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReturnContext {
    /// A training task stopped after publishing a newer checkpoint
    Stopped {
        /// Exact background task that was stopped
        task_id: TaskId,
        /// Opaque identity of the complete checkpoint publication
        checkpoint_ref: String,
        /// Opaque immutable recovery instructions or reference
        recovery_ref: String,
    },
    /// A training task finished before it was stopped
    AlreadyCompleted {
        /// Exact background task that completed
        task_id: TaskId,
        /// Opaque final result reference
        result_ref: String,
    },
    /// A training task ended with no usable result and no saved checkpoint stop
    ///
    /// No evidence names a checkpoint that the run can resume from. The release
    /// basis is saved beside this context: either the authority proved that the
    /// exact trainer worker released its saved ownership lock, or an operator
    /// attested that its GPU work is gone. The supervisor can start new work or
    /// record no-resume; a same-run resume is never valid for this context
    EndedWithoutResult {
        /// Exact background task that ended
        task_id: TaskId,
        /// Task-layer outcome of the ended run
        outcome: ExitReason,
    },
    /// A training task was lost with no exit reason, no result, and no checkpoint stop
    ///
    /// Only an operator attestation releases a lost trainer, and its receipt is
    /// the release basis. The supervisor can start new work or record
    /// no-resume; a same-run resume is never valid for this context
    LostWithoutResult {
        /// Exact background task that was lost
        task_id: TaskId,
    },
    /// No registered live background task occupied the resource
    Idle,
}

impl ReturnContext {
    /// Return the released background task named by this context, if any
    #[must_use]
    pub const fn released_task(&self) -> Option<TaskId> {
        match self {
            Self::Stopped { task_id, .. }
            | Self::AlreadyCompleted { task_id, .. }
            | Self::EndedWithoutResult { task_id, .. }
            | Self::LostWithoutResult { task_id } => Some(*task_id),
            Self::Idle => None,
        }
    }
}

/// Foreground ownership contract accepted for one resource background command
///
/// Homebased proves release only for a command whose GPU ownership has a
/// witness that the authority can check after the process exits. Command names
/// alone cannot detect a shebang wrapper or a self-daemonizing executable, so
/// every other shape is refused before a task record exists
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BackgroundCommandContract {
    /// The maintained `python -m ops.run_segment run` trainer
    ///
    /// Its witness is the segment ownership lock under the runtime root. A
    /// release needs a trainer-attempt association that the supervisor binds
    /// after the running trainer holds that lock
    DirectSegmentTrainer {
        /// Runtime root named by `--runtime-root`
        runtime_root: PathBuf,
    },
}

/// Saved evidence that no background work holds an unregistered resource
///
/// Each variant names the latest resource history record that cleared or never
/// created the background registration. A missing task row is never evidence
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum IdleBoundaryProof {
    /// The latest loan closed with the supervisor's explicit no-resume decision
    ///
    /// That decision required the prior background task to be terminal with a
    /// verified or operator-attested release, or no background task at all
    SupervisorNoResume {
        /// Closed loan that holds the decision
        loan_id: LoanId,
    },
    /// The latest loan closed after the supervisor resolved a return task that
    /// ended with proven process-group release
    RestoreEndedWithProvenRelease {
        /// Closed loan that holds the resolution
        loan_id: LoanId,
        /// Return task that ended
        task_id: TaskId,
    },
    /// The latest loan closed after a native foreground or container return
    /// task ended successfully with its confirmed exit witness
    ForegroundReturnEnded {
        /// Closed loan that holds the foreground-end evidence
        loan_id: LoanId,
        /// Return task that ended
        task_id: TaskId,
    },
    /// The latest first background launch ended before a child process started
    BackgroundLaunchNeverSpawned {
        /// Stable launch request identity
        request_id: RequestId,
        /// Launch task that recorded no child spawn
        task_id: TaskId,
    },
    /// An operator attested that a resource with no history started with a free GPU
    ///
    /// The resource had no registered task, loan, or first background launch
    /// when the attestation committed, and still has none. This is a human
    /// trust decision saved with its receipt, not a process or lock proof
    OperatorAttestedInitialIdle {
        /// Attestation whose receipt holds the observation
        operation_id: operator_release::OperatorAttestationId,
    },
    /// An operator attested that an ended trainer no longer holds the GPU
    ///
    /// The trainer was the registered trainer, a first background launch task,
    /// or a Restoring loan's return task. This is a human trust decision saved
    /// with its receipt, not a process or lock proof. The attestation cleared
    /// the registration
    OperatorAttestedGpuFree {
        /// Attestation whose receipt holds the observation and evidence
        operation_id: operator_release::OperatorAttestationId,
        /// Trainer task that the operator resolved
        task_id: TaskId,
    },
}

/// Missing evidence that keeps an unregistered resource from serving queued work
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleProofGap {
    /// No loan closure or background launch records why the GPU is free
    NoIdleEvidence,
    /// The latest first background launch ended, but its process release is not proven
    BackgroundLaunchReleaseUnproven {
        /// Launch task that ended without a no-child-spawned record
        task_id: TaskId,
    },
    /// The latest closure or launch record does not match the resource registration
    InconsistentHistory,
}

/// Authority decision at the idle boundary of an unregistered resource
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdleBoundaryDecision {
    /// Saved history proves that no background work holds the resource
    Proven(IdleBoundaryProof),
    /// The exact proof that is missing, so the resource stays reserved
    Unproven(IdleProofGap),
}

/// Stable Homebased task identity reserved for one release watcher
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReleaseWatcherTaskId(TaskId);

impl ReleaseWatcherTaskId {
    /// Wrap a preallocated Homebased task identity
    #[must_use]
    pub const fn new(task_id: TaskId) -> Self {
        Self(task_id)
    }

    /// Return the underlying Homebased task identity
    #[must_use]
    pub const fn as_task_id(self) -> TaskId {
        self.0
    }
}

/// Durable identity intent linking one watcher task to one release action
///
/// This record reserves the exact task identity before launch. It does not
/// establish that the Homebased task is running or that the resource is free
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseWatcherIntent {
    /// Stable release decision identity
    pub action_id: ActionId,
    /// Resource revision associated with the release action
    pub state_revision: ResourceRevision,
    /// Exact background task observed when release was requested
    pub observed_background_task: TaskId,
    /// Preallocated identity of the Homebased release watcher task
    pub watcher_task_id: ReleaseWatcherTaskId,
    /// Stable request identity distinct from the preallocated task identity
    pub request_id: RequestId,
    /// Digest of the watcher's immutable normalized command specification
    pub normalized_spec_sha256: NormalizedSpecSha256,
}

impl ReleaseWatcherIntent {
    /// Check that the watcher identities are usable for the observed trainer
    pub fn validate(&self) -> Result<(), InvalidReleaseWatcherIdentity> {
        validate_release_watcher_identity(
            self.observed_background_task,
            self.request_id,
            self.watcher_task_id.as_task_id(),
        )
    }
}

/// Release watcher identities that are nil or that share one UUID between two roles
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("release watcher identities must be non-nil and distinct from each other and the trainer")]
pub struct InvalidReleaseWatcherIdentity;

/// Check the identities of one release watcher task before it is saved or launched
///
/// The retry identity, the watcher task, and the observed trainer are three
/// different records, so no two of them may share a UUID
pub fn validate_release_watcher_identity(
    observed_background_task: TaskId,
    request_id: RequestId,
    watcher_task_id: TaskId,
) -> Result<(), InvalidReleaseWatcherIdentity> {
    if request_id.0.is_nil()
        || watcher_task_id.0.is_nil()
        || request_id.0 == watcher_task_id.0
        || watcher_task_id == observed_background_task
    {
        return Err(InvalidReleaseWatcherIdentity);
    }
    Ok(())
}

/// Saved form of one trainer-attempt association
///
/// The association row and every checkpoint binding store this exact shape
/// Decoding checks only the shape; [`TrainerAttemptAssociation::try_from`]
/// checks the identities and the attempt evidence
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrainerAttemptAssociationProof {
    pub(crate) resource_id: ResourceId,
    pub(crate) authority_machine: MachineId,
    pub(crate) task_id: TaskId,
    pub(crate) canonical_runtime_root: PathBuf,
    pub(crate) attempt_binding: trainer_publication::AttemptBinding,
    pub(crate) request_sha256: ownership_lock::TrainerRequestDigest,
    pub(crate) ownership_lock_identity: OwnershipLockProof,
    pub(crate) normalized_spec_sha256: NormalizedSpecSha256,
}

impl From<&TrainerAttemptAssociation> for TrainerAttemptAssociationProof {
    fn from(association: &TrainerAttemptAssociation) -> Self {
        let evidence = association.verified_attempt();
        let lock = evidence.ownership_lock_identity();
        Self {
            resource_id: association.resource_id(),
            authority_machine: association.authority_machine(),
            task_id: association.task_id(),
            canonical_runtime_root: evidence.canonical_runtime_root().to_path_buf(),
            attempt_binding: evidence.binding().clone(),
            request_sha256: evidence.request_digest(),
            ownership_lock_identity: OwnershipLockProof {
                device: lock.device(),
                inode: lock.inode(),
            },
            normalized_spec_sha256: association.normalized_spec_sha256(),
        }
    }
}

impl TryFrom<TrainerAttemptAssociationProof> for TrainerAttemptAssociation {
    type Error = &'static str;

    fn try_from(proof: TrainerAttemptAssociationProof) -> Result<Self, Self::Error> {
        let lock = proof.ownership_lock_identity;
        let verified_attempt = ownership_lock::VerifiedTrainerAttempt::from_persisted(
            proof.canonical_runtime_root,
            proof.attempt_binding,
            proof.request_sha256,
            ownership_lock::OwnershipLockIdentity::new(lock.device, lock.inode),
        )?;
        Self::from_components(
            proof.resource_id,
            proof.authority_machine,
            proof.task_id,
            verified_attempt,
            proof.normalized_spec_sha256,
        )
    }
}

/// Saved device and inode of the trainer ownership lock
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnershipLockProof {
    pub(crate) device: u64,
    pub(crate) inode: u64,
}

/// Proof provenance for the trainer release that opened a serving loan
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServingReleaseProvenance {
    /// The authority verified the exact trainer's completed-result publication
    CompletedTrainerResult {
        /// Release action whose authority-built proof was accepted
        action_id: ActionId,
        /// Exact registered trainer task in the proof
        task_id: TaskId,
        /// SHA-256 digest of the published completed result
        publication_sha256: Sha256Digest,
    },
    /// The authority derived a saved idle boundary before opening the loan
    ///
    /// No background task was registered, and the latest resource history
    /// records why no background work holds the GPU
    IdleBoundary {
        /// Evidence named by the loan-opening receipt
        proof: IdleBoundaryProof,
    },
    /// The authority verified a stopped trainer's exact checkpoint publication
    StoppedTrainerCheckpoint {
        /// Release action whose committed stop decision was proved
        action_id: ActionId,
        /// Exact registered trainer task in the proof
        task_id: TaskId,
        /// Generation accepted by the trainer's `--resume` contract
        generation_id: String,
        /// SHA-256 digest of the selected checkpoint record
        record_sha256: Sha256Digest,
        /// SHA-256 digest of the selected checkpoint inventory
        inventory_sha256: Sha256Digest,
    },
    /// The authority proved that an ended trainer released its exact saved lock
    ///
    /// The trainer had no usable completed result and no committed checkpoint
    /// stop, so this release carries no resume evidence
    EndedTrainerLockReleased {
        /// Release action whose authority-built proof was accepted
        action_id: ActionId,
        /// Exact registered trainer task in the proof
        task_id: TaskId,
        /// Task-layer outcome of the ended trainer
        outcome: ExitReason,
        /// Request digest of the trainer-attempt association that named the lock
        attempt_request_sha256: ownership_lock::TrainerRequestDigest,
    },
    /// The supervisor left a return action undecided past its decision window
    ///
    /// The loan entered AwaitingReturn only after its own release basis was
    /// proven, and nothing ran on the resource while it waited. The deadline
    /// receipt of the expired action holds the loan, return context, and
    /// registration that the authority saw when it served the queue
    ReturnDeadlinePassed {
        /// Expired return action whose deadline receipt permits activation
        action_id: ActionId,
    },
    /// An operator attested that the ended trainer of this release action no longer holds the GPU
    ///
    /// No process-group exit or trainer lock proves the release. The attestation
    /// receipt holds the operator observation and the authority evidence snapshot
    OperatorAttestedGpuFree {
        /// Attestation whose receipt permits activation
        operation_id: operator_release::OperatorAttestationId,
        /// Release action that the attestation resolved
        action_id: ActionId,
        /// Exact registered trainer task that the operator resolved
        task_id: TaskId,
    },
}

/// Phase-specific identities and return data for every active loan phase
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LoanPhase {
    /// A watcher must release the observed background task
    AwaitingRelease {
        /// Stable release decision identity
        action_id: ActionId,
        /// Exact background task observed when release was requested
        observed_background_task: TaskId,
        /// Optional durable watcher task identity reserved for this action
        #[serde(default, skip_serializing_if = "Option::is_none")]
        watcher_intent: Option<ReleaseWatcherIntent>,
    },
    /// One selected request is using the resource
    Serving {
        /// Return obligation retained for the whole interruption
        return_context: ReturnContext,
        /// Selected request identity
        current_request_id: RequestId,
        /// Durable proof that permits activation once its receipt matches
        release_provenance: ServingReleaseProvenance,
    },
    /// The queue is drained and the supervisor must decide what runs next
    AwaitingReturn {
        /// Stable return decision identity
        action_id: ActionId,
        /// Return obligation retained for the whole interruption
        return_context: ReturnContext,
    },
    /// A supervisor-bound task submission is unresolved or starting
    Restoring {
        /// Stable return decision identity
        action_id: ActionId,
        /// Return obligation retained until the task is reconciled
        return_context: ReturnContext,
        /// Preallocated task identity bound to the return action
        resume_task_id: TaskId,
    },
}

/// Result retained after a loan is closed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LoanClosure {
    /// Background work resumed or a new background task started
    Resumed {
        /// Return evidence retained after closure
        return_context: ReturnContext,
        /// Task identity registered for background work
        task_id: TaskId,
    },
    /// Supervisor explicitly chose not to resume background work
    NoResume {
        /// Return evidence retained after closure
        return_context: ReturnContext,
        /// Supervisor's durable decision reason
        reason: String,
    },
    /// The release watcher confirmed that the observed task was not stopped
    NotStopped {
        /// Exact background task that remained active
        background_task: TaskId,
        /// Evidence that the watcher cannot stop the task later
        reason: String,
    },
    /// A return task that held the loan while it ran ended successfully with its
    /// confirmed exit witness
    ///
    /// A native foreground task needs a confirmed process-group exit. A container
    /// task needs its exited container removed and confirmed absent. The task was
    /// never registered as background training, so the closure leaves the
    /// resource unregistered and names the terminal evidence
    ForegroundReturnEnded {
        /// Return evidence retained after closure
        return_context: ReturnContext,
        /// Bound return task that ended
        task_id: TaskId,
        /// Successful task-layer outcome
        outcome: ExitReason,
    },
    /// The bound return task ended without a successful closure, and the supervisor accepted that end
    ///
    /// A direct-segment task ended before a confirmed start. A native foreground
    /// task ended without success. Both needed proven process release
    RestoreEnded {
        /// Return evidence retained after closure
        return_context: ReturnContext,
        /// Bound return task that ended
        task_id: TaskId,
        /// Task-layer outcome with proven process release
        outcome: ExitReason,
        /// Supervisor's durable resolution reason
        reason: String,
    },
    /// The bound return task ended without release proof, and an operator
    /// attested that its GPU work is gone
    ///
    /// A direct-segment task ended before a confirmed start, or a native
    /// foreground task ended or was lost while its loan stayed reserved. The
    /// receipt's binding names which. This is a human trust decision saved
    /// with its receipt, not a process or lock proof. The closure cleared the
    /// background registration
    OperatorAttestedRestoreEnded {
        /// Return evidence retained after closure
        return_context: ReturnContext,
        /// Bound return task that ended
        task_id: TaskId,
        /// Attestation whose receipt holds the observation and evidence
        operation_id: operator_release::OperatorAttestationId,
    },
}

/// Exact supervisor authority presented with one release or return action
///
/// The deciding transaction compares every field with the saved resource, loan,
/// action, and supervisor assignment. A stale or different value cannot decide
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorActionAuthority {
    /// Resource authority that owns the loan
    pub authority_machine: MachineId,
    /// Resource whose loan awaits the decision
    pub resource_id: ResourceId,
    /// Loan that owns the return action
    pub loan_id: LoanId,
    /// Stable return action identity
    pub action_id: ActionId,
    /// Resource revision that the supervisor observed with the action
    pub expected_state_revision: ResourceRevision,
    /// Supervisor that makes the decision
    pub supervisor: SupervisorAddress,
    /// Supervisor assignment revision that the decision uses
    pub assignment_revision: AssignmentRevision,
}

/// Supervisor's explicit choice after the ready queue drained
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReturnDecision {
    /// Close the interruption without starting background work
    NoResume {
        /// Supervisor's durable decision reason
        reason: String,
    },
    /// Bind one fixed task identity as the returning background work
    Launch(Box<ReturnLaunch>),
}

/// Fixed task identity and typed work for one return launch
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReturnLaunch {
    /// Stable caller retry identity for the return task
    pub request_id: RequestId,
    /// Preallocated task identity for the return task
    pub task_id: TaskId,
    /// Kind of background work, checked against the saved return context
    pub work: ReturnWork,
}

/// Kind of background work that the supervisor chose
///
/// Each kind is valid for exactly one return context. Homebased does not infer
/// the kind from a checkpoint or an optimization result
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReturnWork {
    /// Resume the stopped run from its selected checkpoint
    ///
    /// The authority derives the command from the saved run records, so the
    /// caller cannot supply command text for this choice
    SameRunResume {
        /// Stopped task named by the return context
        stopped_task: TaskId,
        /// Recovery reference named by the return context
        recovery_ref: String,
    },
    /// Run evaluation or the next epoch after the previous run completed
    EvaluationOrNextEpoch {
        /// Completed task named by the return context
        completed_task: TaskId,
        /// Command chosen by the supervisor
        spec: CommandSpec,
    },
    /// Start new background work when no background task was registered
    NewBackgroundWork {
        /// Command chosen by the supervisor
        spec: CommandSpec,
    },
    /// Start new background work after the previous run ended or was lost without a usable result
    ///
    /// The ended run has no resume evidence, so the command is a new choice by
    /// the supervisor and never a continuation of that run
    AfterEndedRun {
        /// Ended task named by the return context
        ended_task: TaskId,
        /// Command chosen by the supervisor
        spec: CommandSpec,
    },
}

impl ReturnWork {
    /// Return the command chosen by the supervisor, or `None` for a same-run resume
    ///
    /// The authority derives a same-run resume command from saved run records
    #[must_use]
    pub const fn supervisor_spec(&self) -> Option<&CommandSpec> {
        match self {
            Self::SameRunResume { .. } => None,
            Self::EvaluationOrNextEpoch { spec, .. }
            | Self::NewBackgroundWork { spec }
            | Self::AfterEndedRun { spec, .. } => Some(spec),
        }
    }
}

/// How one accepted return task holds the resource, fixed when the task is accepted
///
/// The authority derives the mode from the validated ownership contract in the
/// accepting transaction and saves it with the decision receipt. Later
/// reconciliation, restart, and retry read the saved mode; they never classify
/// the command again
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReturnExecutionMode {
    /// Maintained direct-segment trainer
    ///
    /// A confirmed start closes the Restoring loan and registers the task as the
    /// background trainer. Its segment ownership lock is the release witness
    DirectSegmentTrainer,
    /// Native foreground executable
    ///
    /// The task never becomes registered background training. The Restoring
    /// loan keeps the resource reserved while the task runs and closes only
    /// after a successful end with a confirmed process-group exit
    NativeForeground,
    /// Typed container workload
    ///
    /// Like a native foreground task, it never becomes registered background
    /// training and keeps the Restoring loan while it runs. The loan closes only
    /// after exit code 0 with confirmed container evidence: the exact container
    /// exited, Homebased removed it, and its ID is absent
    Container,
}

impl ReturnExecutionMode {
    /// Whether the return task holds the Restoring loan until it ends
    #[must_use]
    pub const fn holds_loan_while_running(self) -> bool {
        match self {
            Self::NativeForeground | Self::Container => true,
            Self::DirectSegmentTrainer => false,
        }
    }
}

impl From<foreground::CommandOwnershipContract> for ReturnExecutionMode {
    fn from(contract: foreground::CommandOwnershipContract) -> Self {
        match contract {
            foreground::CommandOwnershipContract::ForegroundExecutable => Self::NativeForeground,
            foreground::CommandOwnershipContract::DirectSegmentTrainer => {
                Self::DirectSegmentTrainer
            }
            foreground::CommandOwnershipContract::Container => Self::Container,
        }
    }
}

/// Why a return decision cannot apply to the saved action
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReturnDecisionRejection {
    /// A decision identity is nil or reuses one UUID for two roles
    #[error("return decision identities are invalid")]
    InvalidIdentity,
    /// A no-resume decision has no reason
    #[error("no-resume reason must not be empty")]
    EmptyReason,
    /// Same-run resume needs the stopped context with the same task and recovery reference
    #[error("same-run resume requires the matching stopped return context")]
    ResumeRequiresStoppedContext,
    /// Evaluation or next epoch needs the completed context with the same task
    #[error("evaluation or next epoch requires the matching completed return context")]
    EvaluationRequiresCompletedContext,
    /// New background work needs the idle context
    #[error("new background work requires the idle return context")]
    NewWorkRequiresIdleContext,
    /// Work after an ended run needs the ended or lost context with the same task
    #[error("work after an ended run requires the matching ended return context")]
    AfterEndedRunRequiresEndedContext,
    /// The command callback thread is not the assigned supervisor thread
    #[error("return command thread must be the assigned supervisor thread")]
    ThreadMismatch,
    /// The command shape can keep GPU work alive outside its task process group
    #[error("return command can outlive its task process group ({risk:?})")]
    UnsupportedCommandOwnership {
        /// Recognized command shape that can outlive the task-run process group
        risk: ResourceTaskOwnershipRisk,
    },
    /// The saved records cannot prove the stopped run's immutable resume inputs
    #[error("saved records cannot prove same-run resume ({gap:?})")]
    ResumeUnproven {
        /// First missing or changed proof element
        gap: SameRunResumeGap,
    },
}

/// Missing or changed evidence that prevents a same-run resume
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SameRunResumeGap {
    /// No verified release receipt produced this loan's stopped context
    ReleaseReceiptMissing,
    /// The committed checkpoint stop decision is missing or names another checkpoint
    CheckpointDecisionMismatch,
    /// The selected checkpoint publication is missing or changed on disk
    CheckpointUnavailable,
    /// The stopped run has no matching trainer-attempt association
    AssociationMissing,
    /// The accepted identity, task row, or spec digest of the stopped run changed
    RunRecordsChanged,
    /// The saved command does not have the maintained direct-segment shape
    CommandShapeInvalid,
    /// The saved interpreter no longer resolves from the saved environment
    InterpreterChanged,
}

impl ReturnDecision {
    /// Check the typed choice against the saved return context and supervisor thread
    ///
    /// Storage and command-derivation checks run later in the deciding transaction
    pub fn validate_for(
        &self,
        context: &ReturnContext,
        supervisor_thread: ThreadId,
    ) -> Result<(), ReturnDecisionRejection> {
        let launch = match self {
            Self::NoResume { reason } if reason.trim().is_empty() => {
                return Err(ReturnDecisionRejection::EmptyReason);
            }
            Self::NoResume { .. } => return Ok(()),
            Self::Launch(launch) => launch,
        };
        if launch.request_id.0.is_nil()
            || launch.task_id.0.is_nil()
            || launch.request_id.0 == launch.task_id.0
        {
            return Err(ReturnDecisionRejection::InvalidIdentity);
        }

        let spec = match (&launch.work, context) {
            (
                ReturnWork::SameRunResume {
                    stopped_task,
                    recovery_ref,
                },
                ReturnContext::Stopped {
                    task_id,
                    recovery_ref: saved_recovery,
                    ..
                },
            ) if stopped_task == task_id
                && recovery_ref == saved_recovery
                && *task_id != launch.task_id =>
            {
                return Ok(());
            }
            (ReturnWork::SameRunResume { .. }, _) => {
                return Err(ReturnDecisionRejection::ResumeRequiresStoppedContext);
            }
            (
                ReturnWork::EvaluationOrNextEpoch {
                    completed_task,
                    spec,
                },
                ReturnContext::AlreadyCompleted { task_id, .. },
            ) if completed_task == task_id && *task_id != launch.task_id => spec,
            (ReturnWork::EvaluationOrNextEpoch { .. }, _) => {
                return Err(ReturnDecisionRejection::EvaluationRequiresCompletedContext);
            }
            (ReturnWork::NewBackgroundWork { spec }, ReturnContext::Idle) => spec,
            (ReturnWork::NewBackgroundWork { .. }, _) => {
                return Err(ReturnDecisionRejection::NewWorkRequiresIdleContext);
            }
            (
                ReturnWork::AfterEndedRun { ended_task, spec },
                ReturnContext::EndedWithoutResult { task_id, .. }
                | ReturnContext::LostWithoutResult { task_id },
            ) if ended_task == task_id && *task_id != launch.task_id => spec,
            (ReturnWork::AfterEndedRun { .. }, _) => {
                return Err(ReturnDecisionRejection::AfterEndedRunRequiresEndedContext);
            }
        };

        if spec.as_normalized().thread != supervisor_thread {
            return Err(ReturnDecisionRejection::ThreadMismatch);
        }
        foreground::CommandOwnershipContract::for_return_work(&spec.as_normalized().workload)
            .map_err(|risk| ReturnDecisionRejection::UnsupportedCommandOwnership { risk })?;

        Ok(())
    }
}

/// Lifecycle of one resource interruption and its return obligation
///
/// Active phases are carried by [`LoanState::Active`] and shared with the
/// typed `last_safe_phase` in [`LoanState::NeedsAttention`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LoanState {
    /// One active phase with its required identities and return data
    Active {
        /// Shared phase definition for active state and attention fallback
        phase: LoanPhase,
    },
    /// An unresolved resource condition requires a supervisor decision
    NeedsAttention {
        /// Stable identity of the required attention decision
        action_id: ActionId,
        /// Last safe phase with all of its required identity and return data
        last_safe_phase: LoanPhase,
        /// Durable explanation of the unresolved condition
        reason: String,
    },
    /// The interruption and its return decision are resolved
    Closed {
        /// Resume, no-resume, or confirmed non-interruption result
        result: LoanClosure,
    },
}

impl LoanState {
    /// Whether this state still waits for the exact decision that `notice` asks for
    ///
    /// Notice delivery state is independent of action completion, so a notice
    /// with attempts left must stop once the loan moves past its action.
    /// `Restoring` keeps the return action identity but already has its decision
    #[must_use]
    pub fn awaits_notice(&self, notice: &SupervisorNotice) -> bool {
        let awaited = match (self, &notice.payload) {
            (
                Self::Active {
                    phase: LoanPhase::AwaitingRelease { action_id, .. },
                },
                SupervisorNoticePayload::ReleaseRequired { .. },
            )
            | (
                Self::Active {
                    phase: LoanPhase::AwaitingReturn { action_id, .. },
                },
                SupervisorNoticePayload::ReturnRequired { .. },
            )
            | (
                Self::NeedsAttention { action_id, .. },
                SupervisorNoticePayload::AttentionRequired { .. },
            ) => action_id,
            _ => return false,
        };
        *awaited == notice.action_id
    }
}

/// One interruption spanning all optimization requests before training returns
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Loan {
    /// Stable loan identity
    pub id: LoanId,
    /// Exclusive resource held for this interruption
    pub resource_id: ResourceId,
    /// Active phase, attention fallback, or closed result
    pub state: LoanState,
}

/// Action presented to the exact assigned supervisor
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SupervisorNoticePayload {
    /// Release the exact background task observed by the resource authority
    ReleaseRequired {
        /// Background task that must release the resource
        task_id: TaskId,
    },
    /// Decide whether and how to return to background work
    ReturnRequired {
        /// Context that the supervisor must use for its decision
        return_context: ReturnContext,
    },
    /// Resolve an unsafe or uncertain loan condition
    AttentionRequired {
        /// Durable explanation of the condition
        reason: String,
    },
}

/// Independent delivery state for one supervisor notice
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SupervisorNoticeDelivery {
    /// Notice has not had a delivery attempt
    Pending {
        /// Number of attempts already reserved
        attempts: u8,
    },
    /// A prior attempt failed and another attempt may be reserved
    RetryPending {
        /// Number of attempts already reserved
        attempts: u8,
        /// Error from the most recent failed attempt
        last_error: String,
    },
    /// Delivery attempt is reserved and may be in flight
    Sending {
        /// Stable identity of this attempt
        attempt_id: DeliveryAttemptId,
        /// One-based attempt number
        attempt: u8,
    },
    /// Notice was delivered
    Delivered {
        /// Number of attempts made
        attempts: u8,
    },
    /// Bounded delivery attempts were exhausted
    Failed {
        /// Number of attempts made
        attempts: u8,
        /// Error from the final failed attempt
        last_error: String,
    },
}

/// Durable notice to one exact supervisor destination
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorNotice {
    /// Stable deduplication identity for this notice
    pub id: NoticeId,
    /// Loan that owns the required decision
    pub loan_id: LoanId,
    /// Stable action identity shared with loan state
    pub action_id: ActionId,
    /// Resource state revision associated with this notice
    pub state_revision: ResourceRevision,
    /// Exact machine and thread destination
    pub destination: SupervisorAddress,
    /// Supervisor assignment revision used for routing
    pub assignment_revision: AssignmentRevision,
    /// Typed action content
    pub payload: SupervisorNoticePayload,
    /// Delivery status, independent of action completion
    pub delivery: SupervisorNoticeDelivery,
}

/// Authority-owned result of reconciling queued work for one resource
#[derive(Debug, Clone)]
pub enum ResourceQueueReconcileOutcome {
    /// No request is ready for selection
    NoQueuedRequest,
    /// A non-closed loan already reserves the resource
    LoanAlreadyActive {
        /// Existing loan that prevents a second loan from opening
        loan: Loan,
    },
    /// Saved idle evidence opened a loan that serves the next request in serving order
    IdleServing {
        /// Serving loan with an idle return context
        loan: Loan,
        /// Request selected by the opening
        request: ResourceRequest,
        /// Evidence named by the loan-opening receipt
        proof: IdleBoundaryProof,
    },
    /// A queued first background launch row may have lost its worker spawn
    BackgroundLaunchUncertain {
        /// Launch task that keeps the resource reserved
        task_id: TaskId,
    },
    /// A first background launch ended before registration with no release proof
    ///
    /// No request is queued, but the resource stays reserved until an operator
    /// attests that the launch task's GPU work is gone
    BackgroundLaunchReleaseUnproven {
        /// Stable launch request identity
        request_id: RequestId,
        /// Launch task that keeps the resource reserved
        task_id: TaskId,
    },
    /// A running registered task now has one durable release action
    ReleaseRequired {
        /// Loan created for the resource interruption
        loan: Loan,
        /// Notice saved in the same transaction as the loan
        notice: SupervisorNotice,
    },
    /// Work needs owner attention because assignment or resource safety is uncertain
    AttentionRequired {
        /// Request that remains reserved or queued
        request: ResourceRequest,
        /// Authoritative reason why the resource cannot be assigned
        reason: ResourceQueueAttentionReason,
    },
    /// The bound return task keeps the loan reserved because its start or release is not proven
    RestoreAttentionRequired {
        /// Restoring loan that keeps the resource reserved
        loan: Loan,
        /// Return action bound to the task
        action_id: ActionId,
        /// Bound return task
        task_id: TaskId,
        /// Authority-classified reason
        reason: RestoreAttentionReason,
    },
    /// The exact registered trainer remains reserved because its release proof failed
    ReleaseProofUnavailable {
        /// Loan that remains in its existing AwaitingRelease phase
        loan: Loan,
        /// Exact release action that still needs proof
        action_id: ActionId,
        /// Exact registered trainer task that still owns the release obligation
        task_id: TaskId,
        /// Authority-classified reason why the proof did not pass
        reason: ReleaseProofAttentionReason,
    },
}

/// Why an authority cannot safely select a queued request
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceQueueAttentionReason {
    /// No registered task exists, and the store has no proof that the GPU is idle
    IdleNotProven {
        /// Exact proof that is missing
        gap: IdleProofGap,
    },
    /// A first background launch has not reached a confirmed start
    BackgroundLaunchPending {
        /// Launch task that keeps the resource reserved
        task_id: TaskId,
    },
    /// A registered background task has no authority-owned task row
    BackgroundTaskMissing {
        /// Exact task registered on the resource
        task_id: TaskId,
    },
    /// A registered background task is not verifiably running
    BackgroundTaskNotRunning {
        /// Exact task registered on the resource
        task_id: TaskId,
        /// Durable process state observed by the authority
        state: String,
    },
    /// A resource task was accepted, but startup cannot prove its worker started
    AcceptedTaskLaunchUncertain {
        /// Exact accepted command task that needs an owner decision
        task_id: TaskId,
    },
    /// The saved Serving loan's release provenance does not match its saved receipts
    UnverifiedServingRelease,
    /// The exact trainer release proof is missing or did not pass validation
    ReleaseProofUnavailable {
        /// Exact release action retained by the active loan
        action_id: ActionId,
        /// Exact registered trainer task retained by the active loan
        task_id: TaskId,
        /// Authority-classified reason why the proof is not sufficient
        reason: ReleaseProofAttentionReason,
    },
    /// Task acceptance may have committed, but its launch result is uncertain
    AssignedTaskLaunchUncertain {
        /// Exact assigned command task that needs an owner decision
        task_id: TaskId,
    },
    /// The assigned command task is lost and cannot prove that its work stopped
    AssignedTaskLost {
        /// Exact assigned command task that needs an owner decision
        task_id: TaskId,
    },
    /// The assigned task is terminal but its exit witness is unconfirmed: a
    /// process-group exit for a command, or a removed container for a container
    AssignedTaskExitUnconfirmed {
        /// Exact assigned command task that needs an owner decision
        task_id: TaskId,
    },
    /// The assigned task, request, executor, loan, or route identity does not match
    AssignedTaskIdentityMismatch {
        /// Exact assigned command task that needs an owner decision
        task_id: TaskId,
    },
    /// The assigned task could not prove that no child was spawned for its terminal result
    AssignedTaskNoChildSpawnProofInvalid {
        /// Exact assigned command task that needs an owner decision
        task_id: TaskId,
    },
    /// The assigned command may outlive its task-run process group
    AssignedTaskOwnershipUncertain {
        /// Exact assigned command task that needs an owner decision
        task_id: TaskId,
        /// Recognized command shape that can outlive its local process group
        risk: ResourceTaskOwnershipRisk,
    },
    /// The resource revision changed before the task completion transition committed
    AssignedTaskStaleRevision {
        /// Exact assigned command task that needs an owner decision
        task_id: TaskId,
    },
    /// The authority store could not evaluate the assigned task, so the loan stays reserved
    AssignedTaskReconcileFailed {
        /// Exact assigned command task that needs an owner decision
        task_id: TaskId,
    },
}

/// Why a Restoring loan cannot close or advance
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreAttentionReason {
    /// The task row is queued, and this actor did not insert it, so its worker may never start
    LaunchUncertain,
    /// The task ended before a confirmed start; the supervisor must resolve it explicitly
    EndedBeforeConfirmedStart {
        /// Terminal task state
        state: ProcessStatus,
    },
    /// The native foreground task ended without success; the supervisor must resolve it explicitly
    ForegroundEnded {
        /// Terminal task state
        state: ProcessStatus,
    },
    /// The native foreground task ended, but its process-group exit is not confirmed
    ForegroundExitUnconfirmed {
        /// Terminal task state
        state: ProcessStatus,
    },
    /// The container task ended without success; the supervisor must resolve it explicitly
    ContainerEnded {
        /// Terminal task state
        state: ProcessStatus,
    },
    /// The container task ended, but its container evidence is not confirmed
    ContainerExitUnconfirmed {
        /// Terminal task state
        state: ProcessStatus,
    },
    /// The task is lost, so its process release is not proven
    Lost,
    /// The task, route, identity, or receipt does not match the bound return action
    IdentityMismatch,
    /// The authority store could not evaluate the restore
    ReconcileFailed,
}

/// Why a command falls outside the foreground ownership contract
///
/// See [`foreground`] for the contract. Each variant names a shape whose GPU work
/// can outlive, or hide from, the task-run process group that release proof observes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceTaskOwnershipRisk {
    /// A remote execution client can exit while its remote command continues
    RemoteShell,
    /// A container client can exit while a container process continues
    ContainerClient,
    /// A shell wrapper can start work outside its foreground process group
    ShellWrapper,
    /// A command explicitly starts or manages detached work
    DetachedLauncher,
    /// An interpreter runs code that Homebased does not inspect
    Interpreter,
    /// A launcher runs another program named in its arguments
    ProgramLauncher,
    /// The entry point is a script whose interpreter and code Homebased does not inspect
    ScriptEntryPoint,
    /// The entry point is missing, unreadable, or not a recognized native executable
    UninspectableEntryPoint,
}

/// Safe, inspectable classification for a failed release proof
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseProofAttentionReason {
    /// The registered trainer has not reached a successful terminal state
    TrainerNotCompleted,
    /// The registered trainer is lost
    TrainerLost,
    /// Homebased has not confirmed that the trainer worker exited
    WorkerExitUnconfirmed,
    /// No exact completed-result publication is available
    CompletedResultUnavailable,
    /// No exact stopped-checkpoint publication is available
    StoppedCheckpointUnavailable,
    /// The saved ownership lock is held or cannot be verified
    OwnershipLockUnverified,
    /// The registered trainer has no trainer-attempt association, so no saved
    /// lock can prove its release; only a separate operator resolution applies
    TrainerAssociationMissing,
    /// Saved task, authority, route, or proof identities do not match
    SavedEvidenceMismatch,
}

/// Strict Fleet request for one exact supervisor-notice delivery attempt
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorNoticeRequest {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Machine that sent this delivery attempt
    pub source_machine: MachineId,
    /// Exact destination machine and Codex thread
    pub destination: SupervisorAddress,
    /// Stable identity of the notice
    pub notice_id: NoticeId,
    /// Loan that owns the required decision
    pub loan_id: LoanId,
    /// Stable identity of the required supervisor action
    pub action_id: ActionId,
    /// Resource state revision associated with this notice
    pub state_revision: ResourceRevision,
    /// Supervisor assignment revision used for routing
    pub assignment_revision: AssignmentRevision,
    /// Stable identity of this delivery attempt
    pub attempt_id: DeliveryAttemptId,
    /// Typed action content without mutable delivery state
    pub payload: SupervisorNoticePayload,
}

impl SupervisorNoticeRequest {
    /// Validate the strict versioned route and all stable identities
    pub fn validate(&self) -> Result<(), AppError> {
        if self.api_version != API_VERSION {
            return Err(AppError::Usage {
                message: "unsupported API version".into(),
            });
        }

        if self.source_machine.as_uuid().is_nil()
            || self.destination.machine.as_uuid().is_nil()
            || self.destination.thread.0.is_nil()
        {
            return Err(AppError::MessageInvalid {
                message: "supervisor notice identities must not be nil".into(),
            });
        }

        let payload_task = match &self.payload {
            SupervisorNoticePayload::ReleaseRequired { task_id } => Some(*task_id),
            SupervisorNoticePayload::ReturnRequired { return_context } => {
                return_context.released_task()
            }
            SupervisorNoticePayload::AttentionRequired { .. } => None,
        };
        if payload_task.is_some_and(|task_id| task_id.0.is_nil()) {
            return Err(AppError::MessageInvalid {
                message: "supervisor notice task identity must not be nil".into(),
            });
        }

        Ok(())
    }
}

/// Durable receipt returned after the exact notice attempt reaches Codex
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorNoticeReceipt {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Stable identity of the delivered notice
    pub notice_id: NoticeId,
    /// Stable identity of the delivered attempt
    pub attempt_id: DeliveryAttemptId,
    /// Machine that accepted the queue command
    pub destination_machine: MachineId,
    /// Exact thread that received the queue command
    pub destination_thread: ThreadId,
    /// Time when the receiver committed this receipt
    pub delivered_at: chrono::DateTime<chrono::Utc>,
}

/// Versioned response from a remote supervisor-notice receiver
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorNoticeResponse {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Receiver machine identity
    pub destination_machine: MachineId,
    /// Receiver's durable success receipt
    pub receipt: SupervisorNoticeReceipt,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use serde_json::json;
    use uuid::Uuid;

    use crate::domain::{API_VERSION, AgentKind, TaskId, TaskName, ThreadId};
    use crate::invocation::CommandLine;
    use crate::machine::{MachineId, MachineName};
    use crate::spec::{
        NormalizedAgentWorkload, NormalizedSpec, NormalizedTaskWorkload, NormalizedWorkload,
    };
    use crate::submission::{RequestId, ResourceQueueOutcome, ResourceQueueReceipt};

    use super::{
        AcceptanceSequence, ActionId, AssignmentRevision, CommandSpec, CommandSpecError,
        DeliveryAttemptId, LoanClosure, LoanId, LoanPhase, LoanState, NoticeId, ResourceId,
        ResourceQueueRequest, ResourceQueueResponse, ResourceRequest, ResourceRequestState,
        ResourceRevision, ReturnContext, ReturnDecision, ReturnDecisionRejection, ReturnLaunch,
        ReturnWork, SupervisorAddress, SupervisorNoticePayload, SupervisorNoticeRequest,
    };
    use crate::domain::ExitReason;

    fn task_spec() -> NormalizedSpec {
        NormalizedSpec {
            api_version: API_VERSION,
            thread: ThreadId(Uuid::now_v7()),
            name: TaskName::parse("resource command").unwrap(),
            cwd: PathBuf::from("/tmp"),
            machine: None,
            timeout: Duration::from_secs(1800),
            workload: NormalizedWorkload::Task(NormalizedTaskWorkload {
                command: CommandLine::try_from_argv(vec!["echo".into(), "gpu".into()]).unwrap(),
            }),
        }
    }

    fn agent_spec() -> NormalizedSpec {
        NormalizedSpec {
            api_version: API_VERSION,
            thread: ThreadId(Uuid::now_v7()),
            name: TaskName::parse("resource agent").unwrap(),
            cwd: PathBuf::from("/tmp"),
            machine: None,
            timeout: Duration::from_secs(1800),
            workload: NormalizedWorkload::Agent(NormalizedAgentWorkload {
                agent: AgentKind::Codex,
                model: None,
                prompt: "do work".into(),
                extra_args: Vec::new(),
                report_trailer: true,
            }),
        }
    }

    fn request(spec: NormalizedSpec) -> Result<ResourceRequest, CommandSpecError> {
        ResourceRequest::new(
            RequestId::new(),
            TaskId::new(),
            ResourceId::new(),
            AcceptanceSequence::new(1),
            MachineId::new(),
            spec,
        )
    }

    fn queue_request(spec: NormalizedSpec) -> Result<ResourceQueueRequest, CommandSpecError> {
        Ok(ResourceQueueRequest::new(
            1,
            MachineId::new(),
            MachineId::new(),
            RequestId::new(),
            TaskId::new(),
            ResourceId::new(),
            CommandSpec::try_from(spec)?,
        ))
    }

    fn queue_receipt() -> ResourceQueueReceipt {
        ResourceQueueReceipt {
            request: RequestId::new(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            authority_machine: MachineId::new(),
            resource: ResourceId::new(),
            outcome: ResourceQueueOutcome::Waiting,
        }
    }

    #[test]
    fn request_construction_only_accepts_command_workloads() {
        let queued = request(task_spec()).unwrap();
        assert!(matches!(queued.state, ResourceRequestState::Queued));
        assert!(matches!(
            request(agent_spec()),
            Err(CommandSpecError::AgentWorkload)
        ));
    }

    #[test]
    fn command_spec_rejects_an_explicit_machine() {
        let mut spec = task_spec();
        spec.machine = Some(MachineName::parse("other").unwrap());

        assert!(matches!(
            CommandSpec::try_from(spec),
            Err(CommandSpecError::ExplicitMachine)
        ));
    }

    #[test]
    fn request_deserialization_rejects_agent_workloads() {
        let mut value = serde_json::to_value(request(task_spec()).unwrap()).unwrap();
        value["spec"] = serde_json::to_value(agent_spec()).unwrap();

        assert!(serde_json::from_value::<ResourceRequest>(value).is_err());
    }

    #[test]
    fn request_deserialization_rejects_an_explicit_machine() {
        let mut value = serde_json::to_value(request(task_spec()).unwrap()).unwrap();
        value["spec"]["machine"] = json!("other");

        assert!(serde_json::from_value::<ResourceRequest>(value).is_err());
    }

    #[test]
    fn resource_queue_request_round_trips_command_without_explicit_machine() {
        let request = queue_request(task_spec()).unwrap();
        let encoded = serde_json::to_value(&request).unwrap();

        assert_eq!(encoded["api_version"], API_VERSION);
        assert_eq!(encoded["spec"]["workload"]["type"], "task");
        assert!(encoded["spec"].get("machine").is_none());

        let decoded = serde_json::from_value::<ResourceQueueRequest>(encoded.clone()).unwrap();
        assert_eq!(decoded.request_id, request.request_id);
        assert_eq!(decoded.task_id, request.task_id);
        assert_eq!(decoded.resource_id, request.resource_id);
        assert!(decoded.spec.as_normalized().machine.is_none());
        assert!(matches!(
            &decoded.spec.as_normalized().workload,
            NormalizedWorkload::Task(_)
        ));
        assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
    }

    #[test]
    fn resource_queue_request_rejects_agents_and_explicit_machines() {
        assert!(matches!(
            queue_request(agent_spec()),
            Err(CommandSpecError::AgentWorkload)
        ));

        let mut explicit_machine = task_spec();
        explicit_machine.machine = Some(MachineName::parse("other").unwrap());
        assert!(matches!(
            queue_request(explicit_machine),
            Err(CommandSpecError::ExplicitMachine)
        ));

        let valid = serde_json::to_value(queue_request(task_spec()).unwrap()).unwrap();
        let mut agent = valid.clone();
        agent["spec"] = serde_json::to_value(agent_spec()).unwrap();
        assert!(serde_json::from_value::<ResourceQueueRequest>(agent).is_err());

        let mut explicit_machine = valid;
        explicit_machine["spec"]["machine"] = json!("other");
        assert!(serde_json::from_value::<ResourceQueueRequest>(explicit_machine).is_err());
    }

    #[test]
    fn resource_queue_response_binds_destination_to_receipt_authority() {
        let receipt = queue_receipt();
        let response = ResourceQueueResponse::new(1, receipt.clone());
        let encoded = serde_json::to_value(&response).unwrap();

        assert_eq!(response.destination_machine, receipt.authority_machine);
        assert_eq!(encoded["api_version"], API_VERSION);
        assert_eq!(
            serde_json::from_value::<ResourceQueueResponse>(encoded.clone()).unwrap(),
            response
        );
    }

    #[test]
    fn resource_queue_request_and_response_reject_unknown_fields() {
        let mut request = serde_json::to_value(queue_request(task_spec()).unwrap()).unwrap();
        request["unexpected"] = json!(true);
        assert!(serde_json::from_value::<ResourceQueueRequest>(request).is_err());

        let mut response =
            serde_json::to_value(ResourceQueueResponse::new(1, queue_receipt())).unwrap();
        response["unexpected"] = json!(true);
        assert!(serde_json::from_value::<ResourceQueueResponse>(response).is_err());
    }

    #[test]
    fn stopped_return_context_preserves_task_and_opaque_references() {
        let context = ReturnContext::Stopped {
            task_id: TaskId::new(),
            checkpoint_ref: "trainer://generation/204".into(),
            recovery_ref: "run-config:immutable-77".into(),
        };
        let encoded = serde_json::to_value(&context).unwrap();

        assert_eq!(encoded["type"], "stopped");
        assert_eq!(
            serde_json::from_value::<ReturnContext>(encoded).unwrap(),
            context
        );
    }

    #[test]
    fn loan_attention_retains_the_last_safe_phase_data() {
        let state = LoanState::NeedsAttention {
            action_id: ActionId::new(),
            last_safe_phase: LoanPhase::AwaitingRelease {
                action_id: ActionId::new(),
                observed_background_task: TaskId::new(),
                watcher_intent: None,
            },
            reason: "watcher could not establish process exit".into(),
        };
        let encoded = serde_json::to_value(&state).unwrap();

        assert_eq!(encoded["type"], "needs_attention");
        assert_eq!(encoded["last_safe_phase"]["type"], "awaiting_release");
        assert_eq!(serde_json::from_value::<LoanState>(encoded).unwrap(), state);
    }

    #[test]
    fn awaiting_release_without_watcher_intent_remains_compatible() {
        let action_id = ActionId::new();
        let background_task = TaskId::new();
        let legacy = json!({
            "type": "active",
            "phase": {
                "type": "awaiting_release",
                "action_id": action_id,
                "observed_background_task": background_task,
            },
        });

        assert!(matches!(
            serde_json::from_value::<LoanState>(legacy).unwrap(),
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease {
                    action_id: saved_action,
                    observed_background_task: saved_task,
                    watcher_intent: None,
                }
            } if saved_action == action_id && saved_task == background_task
        ));
    }

    #[test]
    fn loan_state_deserialization_requires_active_phase_data() {
        let incomplete = json!({
            "type": "active",
            "phase": {
                "type": "awaiting_release",
                "action_id": ActionId::new(),
            },
        });

        assert!(serde_json::from_value::<LoanState>(incomplete).is_err());
    }

    #[test]
    fn loan_attention_rejects_attention_or_closed_last_safe_phase() {
        for phase_type in ["needs_attention", "closed"] {
            let invalid = json!({
                "type": "needs_attention",
                "action_id": ActionId::new(),
                "last_safe_phase": { "type": phase_type },
                "reason": "unresolved condition",
            });

            assert!(serde_json::from_value::<LoanState>(invalid).is_err());
        }
    }

    #[test]
    fn return_decisions_are_strict_and_older_closures_still_decode() {
        let launch = ReturnDecision::Launch(Box::new(ReturnLaunch {
            request_id: RequestId::new(),
            task_id: TaskId::new(),
            work: ReturnWork::NewBackgroundWork {
                spec: CommandSpec::try_from(task_spec()).unwrap(),
            },
        }));
        let mut encoded = serde_json::to_value(&launch).unwrap();
        assert_eq!(encoded["type"], "launch");
        assert_eq!(encoded["work"]["type"], "new_background_work");
        serde_json::from_value::<ReturnDecision>(encoded.clone()).unwrap();
        encoded["work"]["command"] = json!(["/bin/sh", "-c", "resume"]);
        assert!(serde_json::from_value::<ReturnDecision>(encoded).is_err());

        let legacy = json!({
            "type": "closed",
            "result": {
                "type": "no_resume",
                "return_context": { "type": "idle" },
                "reason": "legacy",
            },
        });
        assert!(matches!(
            serde_json::from_value::<LoanState>(legacy).unwrap(),
            LoanState::Closed {
                result: LoanClosure::NoResume { .. }
            }
        ));
        let ended = LoanState::Closed {
            result: LoanClosure::RestoreEnded {
                return_context: ReturnContext::Idle,
                task_id: TaskId::new(),
                outcome: ExitReason::Cancelled,
                reason: "never started".into(),
            },
        };
        assert_eq!(
            serde_json::from_value::<LoanState>(serde_json::to_value(&ended).unwrap()).unwrap(),
            ended
        );
    }

    #[test]
    fn return_decision_choice_must_match_its_context_and_supervisor_thread() {
        let spec = task_spec();
        let thread = spec.thread;
        let new_work = |spec: NormalizedSpec| {
            ReturnDecision::Launch(Box::new(ReturnLaunch {
                request_id: RequestId::new(),
                task_id: TaskId::new(),
                work: ReturnWork::NewBackgroundWork {
                    spec: CommandSpec::try_from(spec).unwrap(),
                },
            }))
        };

        assert_eq!(
            new_work(spec.clone()).validate_for(&ReturnContext::Idle, thread),
            Ok(())
        );
        assert_eq!(
            new_work(spec.clone()).validate_for(
                &ReturnContext::AlreadyCompleted {
                    task_id: TaskId::new(),
                    result_ref: "result".into(),
                },
                thread
            ),
            Err(ReturnDecisionRejection::NewWorkRequiresIdleContext)
        );
        assert_eq!(
            new_work(spec.clone()).validate_for(&ReturnContext::Idle, ThreadId(Uuid::now_v7())),
            Err(ReturnDecisionRejection::ThreadMismatch)
        );
        let mut detached = spec;
        detached.workload = NormalizedWorkload::Task(NormalizedTaskWorkload {
            command: CommandLine::try_from_argv(vec!["setsid".into(), "trainer".into()]).unwrap(),
        });
        assert_eq!(
            new_work(detached).validate_for(&ReturnContext::Idle, thread),
            Err(ReturnDecisionRejection::UnsupportedCommandOwnership {
                risk: super::ResourceTaskOwnershipRisk::DetachedLauncher,
            })
        );
        assert_eq!(
            ReturnDecision::NoResume { reason: " ".into() }
                .validate_for(&ReturnContext::Idle, thread),
            Err(ReturnDecisionRejection::EmptyReason)
        );
    }

    #[test]
    fn ended_context_permits_new_work_or_no_resume_but_never_same_run_resume() {
        let spec = task_spec();
        let thread = spec.thread;
        let ended_task = TaskId::new();
        let ended = ReturnContext::EndedWithoutResult {
            task_id: ended_task,
            outcome: ExitReason::Exit { code: 3 },
        };
        let launch = |work| {
            ReturnDecision::Launch(Box::new(ReturnLaunch {
                request_id: RequestId::new(),
                task_id: TaskId::new(),
                work,
            }))
        };
        let after_ended = |ended_task| ReturnWork::AfterEndedRun {
            ended_task,
            spec: CommandSpec::try_from(spec.clone()).unwrap(),
        };

        assert_eq!(
            launch(after_ended(ended_task)).validate_for(&ended, thread),
            Ok(())
        );
        assert_eq!(
            ReturnDecision::NoResume {
                reason: "failed run".into()
            }
            .validate_for(&ended, thread),
            Ok(())
        );
        assert_eq!(
            launch(ReturnWork::SameRunResume {
                stopped_task: ended_task,
                recovery_ref: "generation-after-failure".into(),
            })
            .validate_for(&ended, thread),
            Err(ReturnDecisionRejection::ResumeRequiresStoppedContext)
        );
        assert_eq!(
            launch(after_ended(TaskId::new())).validate_for(&ended, thread),
            Err(ReturnDecisionRejection::AfterEndedRunRequiresEndedContext)
        );
        assert_eq!(
            launch(after_ended(ended_task)).validate_for(&ReturnContext::Idle, thread),
            Err(ReturnDecisionRejection::AfterEndedRunRequiresEndedContext)
        );
        // a lost run released by an operator attestation has the same choices
        let lost = ReturnContext::LostWithoutResult {
            task_id: ended_task,
        };
        assert_eq!(
            launch(after_ended(ended_task)).validate_for(&lost, thread),
            Ok(())
        );
        assert_eq!(
            launch(after_ended(TaskId::new())).validate_for(&lost, thread),
            Err(ReturnDecisionRejection::AfterEndedRunRequiresEndedContext)
        );
        assert_eq!(
            launch(ReturnWork::SameRunResume {
                stopped_task: ended_task,
                recovery_ref: "generation-before-loss".into(),
            })
            .validate_for(&lost, thread),
            Err(ReturnDecisionRejection::ResumeRequiresStoppedContext)
        );
        assert_eq!(
            launch(ReturnWork::NewBackgroundWork {
                spec: CommandSpec::try_from(spec.clone()).unwrap(),
            })
            .validate_for(&ended, thread),
            Err(ReturnDecisionRejection::NewWorkRequiresIdleContext)
        );
    }

    fn supervisor_notice_request() -> SupervisorNoticeRequest {
        SupervisorNoticeRequest {
            api_version: API_VERSION,
            protocol_version: 1,
            source_machine: MachineId::new(),
            destination: SupervisorAddress {
                machine: MachineId::new(),
                thread: ThreadId(Uuid::now_v7()),
            },
            notice_id: NoticeId::new(),
            loan_id: LoanId::new(),
            action_id: ActionId::new(),
            state_revision: ResourceRevision::new(4),
            assignment_revision: AssignmentRevision::new(2),
            attempt_id: DeliveryAttemptId::new(),
            payload: SupervisorNoticePayload::ReleaseRequired {
                task_id: TaskId::new(),
            },
        }
    }

    #[test]
    fn supervisor_notice_request_is_strict_and_excludes_delivery_state() {
        let request = supervisor_notice_request();
        request.validate().unwrap();
        let value = serde_json::to_value(&request).unwrap();

        assert!(value.get("delivery").is_none());
        assert!(value.get("attempt_id").is_some());
        let mut unknown = value;
        unknown["unexpected"] = json!(true);
        assert!(serde_json::from_value::<SupervisorNoticeRequest>(unknown).is_err());
    }

    #[test]
    fn supervisor_notice_request_rejects_nil_identity_and_unsupported_version() {
        let mut wire = serde_json::to_value(supervisor_notice_request()).unwrap();
        wire["attempt_id"] = json!(Uuid::nil());
        assert!(serde_json::from_value::<SupervisorNoticeRequest>(wire).is_err());

        let mut request = supervisor_notice_request();
        request.destination.thread = ThreadId(Uuid::nil());
        assert!(request.validate().is_err());

        let mut request = supervisor_notice_request();
        request.api_version += 1;
        assert!(request.validate().is_err());
    }
}
