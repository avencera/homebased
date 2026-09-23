//! Typed resource, request, loan, and supervisor-notice domain models

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use crate::domain::{API_VERSION, ExitReason, TaskId, ThreadId};
use crate::machine::MachineId;
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::submission::{RequestId, ResourceQueueReceipt};

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
    },
    /// One selected request is using the resource
    Serving {
        /// Return obligation retained for the whole interruption
        return_context: ReturnContext,
        /// Selected request identity
        current_request_id: RequestId,
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
        ResourceRevision, ReturnContext, SupervisorAddress, SupervisorNoticePayload,
        SupervisorNoticeRequest,
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
            },
            reason: "watcher could not establish process exit".into(),
        };
        let encoded = serde_json::to_value(&state).unwrap();

        assert_eq!(encoded["type"], "needs_attention");
        assert_eq!(encoded["last_safe_phase"]["type"], "awaiting_release");
        assert_eq!(serde_json::from_value::<LoanState>(encoded).unwrap(), state);
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
