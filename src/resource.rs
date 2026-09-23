//! Typed resource, request, loan, and supervisor-notice domain models

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use crate::domain::{API_VERSION, ExitReason, ProcessStatus, TaskId, ThreadId};
use crate::machine::MachineId;
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::submission::{NormalizedSpecSha256, RequestId, ResourceQueueReceipt};

pub mod command_shape;
pub mod ownership_lock;
pub mod release_watcher;
#[expect(
    dead_code,
    reason = "StoreActor migration integration is outside this module's scope"
)]
pub(crate) mod store;
pub mod watcher;

/// Stable identity of one physical GPU resource
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResourceId(Uuid);

impl ResourceId {
    /// Allocate a new resource identity
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

impl Default for ResourceId {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable identity of one interruption loan
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LoanId(Uuid);

impl LoanId {
    /// Allocate a new loan identity
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

impl Default for LoanId {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable identity of one required supervisor decision
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActionId(Uuid);

impl ActionId {
    /// Allocate a new action identity
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

impl Default for ActionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable identity of one durable supervisor notice
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NoticeId(Uuid);

impl NoticeId {
    /// Allocate a new notice identity
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

impl Default for NoticeId {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable identity of one notice delivery attempt
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeliveryAttemptId(Uuid);

impl DeliveryAttemptId {
    /// Allocate a new delivery attempt identity
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

impl Default for DeliveryAttemptId {
    fn default() -> Self {
        Self::new()
    }
}

/// Authority-assigned FIFO position for a resource request
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
        if resource_id.as_uuid().is_nil()
            || authority_machine.as_uuid().is_nil()
            || task_id.0.is_nil()
        {
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

/// A normalized specification restricted to finite command workloads
#[derive(Debug, Clone, Serialize)]
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

        if matches!(&spec.workload, NormalizedWorkload::Task(_)) {
            return Ok(Self(spec));
        }

        Err(CommandSpecError::AgentWorkload)
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
    #[error("resource requests require a command workload")]
    AgentWorkload,
}

/// Queue and task-result state for a resource request
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceRequestState {
    /// Ready for FIFO selection by the authority
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
    /// Authority-assigned FIFO position
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
    /// Resource whose FIFO queue will receive the command
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
    /// No registered live background task occupied the resource
    Idle,
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
/// Every launch identity field is required so older partial bindings fail as stored-data errors
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

/// Persisted watcher identity that keeps older partial records readable
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SavedReleaseWatcherIntent {
    /// Complete identity that can bind a new watcher action
    Complete(ReleaseWatcherIntent),
    /// Legacy identity retained for inspection but not trusted as release evidence
    LegacyUnproven(serde_json::Value),
}

impl SavedReleaseWatcherIntent {
    pub(crate) fn complete(&self) -> Option<&ReleaseWatcherIntent> {
        match self {
            Self::Complete(intent) => Some(intent),
            Self::LegacyUnproven(_) => None,
        }
    }
}

/// Durable release identity captured before the watcher task can act
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointAction {
    pub(crate) resource_id: ResourceId,
    pub(crate) action_id: ActionId,
    pub(crate) state_revision: ResourceRevision,
    pub(crate) observed_background_task: TaskId,
}

/// Immutable identity of the saved trainer-attempt association
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrainerAttemptAssociationProof {
    pub(crate) resource_id: ResourceId,
    pub(crate) authority_machine: MachineId,
    pub(crate) task_id: TaskId,
    pub(crate) canonical_runtime_root: PathBuf,
    pub(crate) attempt_binding: watcher::AttemptBinding,
    pub(crate) request_sha256: String,
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
            request_sha256: evidence.request_digest().to_hex(),
            ownership_lock_identity: OwnershipLockProof {
                device: lock.device(),
                inode: lock.inode(),
            },
            normalized_spec_sha256: association.normalized_spec_sha256(),
        }
    }
}

/// Saved device and inode of the trainer ownership lock
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnershipLockProof {
    pub(crate) device: u64,
    pub(crate) inode: u64,
}

/// Full authority-owned identity that scopes one checkpoint baseline and stop decision
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointBinding {
    pub(crate) action: ReleaseCheckpointAction,
    pub(crate) association: TrainerAttemptAssociationProof,
    pub(crate) attempt_binding: watcher::AttemptBinding,
    pub(crate) watcher_intent: ReleaseWatcherIntent,
}

impl ReleaseCheckpointBinding {
    pub(crate) fn validate_for(
        &self,
        action: &ReleaseCheckpointAction,
    ) -> Result<(), &'static str> {
        validate_release_checkpoint_binding(action, self)
    }
}

/// Checkpoint baseline captured from the saved trainer association
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointBaseline {
    pub(crate) binding: ReleaseCheckpointBinding,
    pub(crate) snapshot: watcher::RecoverySnapshot,
}

/// Stable identity of one reserved exact-task stop request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ReleaseStopReservationId(Uuid);

impl ReleaseStopReservationId {
    pub(crate) fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

/// Stop decision reserved after the authority verified its exact checkpoint
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointStopDecision {
    pub(crate) binding: ReleaseCheckpointBinding,
    pub(crate) reservation_id: ReleaseStopReservationId,
    pub(crate) selected_checkpoint: watcher::VerifiedCheckpointPublication,
}

/// Exact trainer task cancellation committed with its saved stop decision
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointCancellation {
    pub(crate) task_id: TaskId,
    pub(crate) cancel_requested_at: DateTime<Utc>,
}

/// Durable checkpoint-evidence phase stored beside the resource loan
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ReleaseCheckpointPhase {
    /// New release action with no watcher identity yet
    WatcherBindingPending,
    /// Exact watcher identity is bound to a persisted pre-action baseline
    BaselineCaptured {
        /// Saved baseline and all immutable owner identities
        baseline: ReleaseCheckpointBaseline,
    },
    /// Exact-task stop request is durably reserved after checkpoint verification
    StopReserved {
        /// Baseline used to reject pre-existing publications
        baseline: ReleaseCheckpointBaseline,
        /// Fixed checkpoint selected by the authority
        decision: Box<ReleaseCheckpointStopDecision>,
    },
    /// Exact trainer cancellation committed with the saved stop decision
    CancellationCommitted {
        /// Baseline used to reject pre-existing publications
        baseline: ReleaseCheckpointBaseline,
        /// Fixed checkpoint selected by the authority
        decision: Box<ReleaseCheckpointStopDecision>,
        /// Exact task cancellation marker committed in the same transaction
        cancellation: ReleaseCheckpointCancellation,
    },
}

/// Durable evidence state for one exact AwaitingRelease action
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointState {
    pub(crate) action: ReleaseCheckpointAction,
    pub(crate) phase: ReleaseCheckpointPhase,
}

impl ReleaseCheckpointState {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.action.resource_id.as_uuid().is_nil()
            || self.action.action_id.as_uuid().is_nil()
            || self.action.observed_background_task.0.is_nil()
            || self.action.state_revision.get() == 0
        {
            return Err("release checkpoint action identity is invalid");
        }

        let baseline = match &self.phase {
            ReleaseCheckpointPhase::WatcherBindingPending => return Ok(()),
            ReleaseCheckpointPhase::BaselineCaptured { baseline }
            | ReleaseCheckpointPhase::StopReserved { baseline, .. }
            | ReleaseCheckpointPhase::CancellationCommitted { baseline, .. } => baseline,
        };
        validate_release_checkpoint_binding(&self.action, &baseline.binding)?;
        if !baseline.snapshot.is_for(&baseline.binding.attempt_binding) {
            return Err("checkpoint baseline belongs to a different trainer attempt");
        }

        let decision = match &self.phase {
            ReleaseCheckpointPhase::StopReserved { decision, .. }
            | ReleaseCheckpointPhase::CancellationCommitted { decision, .. } => decision,
            ReleaseCheckpointPhase::WatcherBindingPending
            | ReleaseCheckpointPhase::BaselineCaptured { .. } => return Ok(()),
        };
        if let ReleaseCheckpointPhase::CancellationCommitted { cancellation, .. } = &self.phase
            && cancellation.task_id != self.action.observed_background_task
        {
            return Err("checkpoint cancellation differs from its observed trainer task");
        }
        {
            validate_release_checkpoint_binding(&self.action, &decision.binding)?;
            if decision.binding != baseline.binding
                || decision.reservation_id.0.is_nil()
                || decision.selected_checkpoint.binding != baseline.binding.attempt_binding
                || baseline
                    .snapshot
                    .contains_generation(&decision.selected_checkpoint.generation_id)
                || !is_sha256(&decision.selected_checkpoint.record_sha256)
                || !is_sha256(&decision.selected_checkpoint.inventory_sha256)
                || decision.selected_checkpoint.path
                    != baseline
                        .binding
                        .association
                        .canonical_runtime_root
                        .join("published")
                        .join(&decision.selected_checkpoint.generation_id)
            {
                return Err("checkpoint stop decision differs from its verified baseline");
            }
        }

        Ok(())
    }
}

fn validate_release_checkpoint_binding(
    action: &ReleaseCheckpointAction,
    binding: &ReleaseCheckpointBinding,
) -> Result<(), &'static str> {
    let intent = &binding.watcher_intent;
    let association = &binding.association;
    if binding.action != *action
        || association.resource_id != action.resource_id
        || association.task_id != action.observed_background_task
        || association.authority_machine.as_uuid().is_nil()
        || association.attempt_binding != binding.attempt_binding
        || !association.canonical_runtime_root.is_absolute()
        || !is_sha256(&association.request_sha256)
        || intent.action_id != action.action_id
        || intent.state_revision != action.state_revision
        || intent.observed_background_task != action.observed_background_task
        || intent.watcher_task_id.as_task_id().0.is_nil()
        || intent.watcher_task_id.as_task_id() == action.observed_background_task
        || intent.request_id.0.is_nil()
        || intent.request_id.0 == intent.watcher_task_id.as_task_id().0
    {
        return Err("release checkpoint binding does not match the saved action");
    }

    binding
        .attempt_binding
        .validate()
        .map_err(|_| "release checkpoint attempt binding is invalid")
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Typed result of checking whether an exact-task stop can be reserved
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReleaseCheckpointStopOutcome {
    /// A new stop decision and checkpoint identity were persisted
    Reserved(ReleaseCheckpointStopDecision),
    /// An exact retry returned the previously persisted decision
    AlreadyReserved(ReleaseCheckpointStopDecision),
    /// The exact task has not started yet
    WaitingForTaskStart,
    /// No complete new checkpoint is available yet
    WaitingForCheckpoint,
    /// A complete final result takes precedence over a checkpoint stop
    CompletedResultAwaitingTaskExit,
    /// The task already published its successful final result
    AlreadyCompleted,
    /// The saved task or publication state needs attention
    Attention(watcher::WatcherAttention),
}

/// Proof provenance for the trainer release that opened a serving loan
///
/// The unverified value is the safe default for serving rows written before
/// release-proof provenance was stored
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServingReleaseProvenance {
    /// No verified release proof is recorded
    #[default]
    Unverified,
    /// The authority verified the exact trainer's completed-result publication
    CompletedTrainerResult {
        /// Release action whose authority-built proof was accepted
        action_id: ActionId,
        /// Exact registered trainer task in the proof
        task_id: TaskId,
        /// SHA-256 digest of the published completed result
        publication_sha256: String,
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
        record_sha256: String,
        /// SHA-256 digest of the selected checkpoint inventory
        inventory_sha256: String,
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
        watcher_intent: Option<SavedReleaseWatcherIntent>,
    },
    /// One selected request is using the resource
    Serving {
        /// Return obligation retained for the whole interruption
        return_context: ReturnContext,
        /// Selected request identity
        current_request_id: RequestId,
        /// Durable proof that permits activation, if release was verified
        #[serde(default)]
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
    IdleNotProven,
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
    /// The assigned command task terminated with a failure outcome
    AssignedTaskFailed {
        /// Exact assigned command task that needs an owner decision
        task_id: TaskId,
        /// Durable terminal state observed by the authority
        state: ProcessStatus,
    },
    /// The saved Serving loan came from a legacy or otherwise unverified release
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
    /// The assigned command task is terminal but its process-group exit is unconfirmed
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
}

/// Recognized command shapes whose ownership can outlive the local task-run process group
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceTaskOwnershipRisk {
    /// An SSH client can exit while a remote command continues
    RemoteShell,
    /// A container client can exit while a container process continues
    ContainerClient,
    /// A shell wrapper can start work outside its foreground process group
    ShellWrapper,
    /// A command explicitly starts or manages detached work
    DetachedLauncher,
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
    pub fn validate(&self) -> Result<(), crate::error::AppError> {
        if self.api_version != crate::domain::API_VERSION {
            return Err(crate::error::AppError::Usage {
                message: "unsupported API version".into(),
            });
        }

        if self.source_machine.as_uuid().is_nil()
            || self.destination.machine.as_uuid().is_nil()
            || self.destination.thread.0.is_nil()
            || self.notice_id.as_uuid().is_nil()
            || self.loan_id.as_uuid().is_nil()
            || self.action_id.as_uuid().is_nil()
            || self.attempt_id.as_uuid().is_nil()
        {
            return Err(crate::error::AppError::MessageInvalid {
                message: "supervisor notice identities must not be nil".into(),
            });
        }

        let payload_task = match &self.payload {
            SupervisorNoticePayload::ReleaseRequired { task_id } => Some(task_id),
            SupervisorNoticePayload::ReturnRequired {
                return_context:
                    ReturnContext::Stopped { task_id, .. }
                    | ReturnContext::AlreadyCompleted { task_id, .. },
            } => Some(task_id),
            SupervisorNoticePayload::ReturnRequired {
                return_context: ReturnContext::Idle,
            }
            | SupervisorNoticePayload::AttentionRequired { .. } => None,
        };
        if payload_task.is_some_and(|task_id| task_id.0.is_nil()) {
            return Err(crate::error::AppError::MessageInvalid {
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
        DeliveryAttemptId, LoanId, LoanPhase, LoanState, NoticeId, ResourceId,
        ResourceQueueRequest, ResourceQueueResponse, ResourceRequest, ResourceRequestState,
        ResourceRevision, ReturnContext, ServingReleaseProvenance, SupervisorAddress,
        SupervisorNoticePayload, SupervisorNoticeRequest,
    };

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

    #[test]
    fn serving_phase_without_release_provenance_decodes_as_unverified() {
        let mut saved = serde_json::to_value(LoanPhase::Serving {
            return_context: ReturnContext::Stopped {
                task_id: TaskId::new(),
                checkpoint_ref: "legacy-checkpoint".into(),
                recovery_ref: "legacy-recovery".into(),
            },
            current_request_id: RequestId::new(),
            release_provenance: ServingReleaseProvenance::Unverified,
        })
        .unwrap();
        saved.as_object_mut().unwrap().remove("release_provenance");

        let decoded: LoanPhase = serde_json::from_value(saved).unwrap();
        assert!(matches!(
            decoded,
            LoanPhase::Serving {
                release_provenance: ServingReleaseProvenance::Unverified,
                ..
            }
        ));
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

        assert_eq!(encoded["api_version"], crate::domain::API_VERSION);
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
        assert_eq!(encoded["api_version"], crate::domain::API_VERSION);
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
        let mut request = supervisor_notice_request();
        request.attempt_id = DeliveryAttemptId::from_uuid(Uuid::nil());
        assert!(request.validate().is_err());

        let mut request = supervisor_notice_request();
        request.api_version += 1;
        assert!(request.validate().is_err());
    }
}
