//! Versioned read and control bodies for resource routes on the socket, dashboard, and Fleet
//!
//! The resource authority builds every view from durable resource, loan,
//! request, notice, and task state. No view stores a second occupancy state

use crate::resource::trainer_publication::AttemptBinding;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{CallbackStatus, ExitReason, ProcessStatus, TaskId, ThreadId};
use crate::machine::MachineId;
use crate::resource::operator_release::{OperatorGpuFreeAttestation, OperatorGpuFreeReceipt};
use crate::resource::{
    AcceptanceSequence, ActionId, AssignmentRevision, Loan, LoanId, LoanPhase, NoticeId, Resource,
    ResourceId, ResourceRequestState, ResourceRevision, ReturnContext, ReturnDecisionWindow,
    ReturnExecutionMode, SupervisorAddress, SupervisorNotice,
};
use crate::submission::RequestId;

/// Socket route that registers a resource on this daemon as its fixed authority
pub const RESOURCE_REGISTER_PATH: &str = "/v1/resources/register";
/// Socket and dashboard route that lists pending supervisor actions for one thread
pub const RESOURCE_PENDING_PATH: &str = "/v1/resources/pending";
/// Fleet route that lists resources owned by the receiving authority
pub const CLUSTER_RESOURCES_PATH: &str = "/v1/cluster/resources";
/// Fleet route that lists pending actions owned by the receiving authority
pub const CLUSTER_RESOURCE_PENDING_PATH: &str = "/v1/cluster/resources/pending";

/// `GET /v1/resources`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceList {
    /// Public API version
    pub api_version: u32,
    /// Resources read from reachable authorities
    pub resources: Vec<ResourceOverview>,
    /// Authorities whose resources could not be read; their GPUs are unknown, not idle
    pub unavailable_authorities: Vec<UnavailableAuthority>,
}

/// Summary of one resource derived from its durable state
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceOverview {
    /// Resource with its fixed authority, supervisor, and revisions
    pub resource: Resource,
    /// Non-closed loan that reserves the resource, if one exists
    pub loan: Option<Loan>,
    /// Requests still waiting for selection in serving order
    pub queued_count: u64,
    /// Task that holds the resource for the current loan phase
    pub current_task: Option<ResourceTaskSummary>,
    /// Most important condition that needs an operator or supervisor
    pub attention: Option<AttentionView>,
    /// First background launch that reserves the unregistered resource, if one does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_launch: Option<BackgroundLaunchReservation>,
    /// Decision window of the pending return action, when the loan awaits one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_window: Option<ReturnDecisionWindow>,
}

/// First background launch that reserves a resource before its task is registered
///
/// The authority derives it from the launch receipt and the task row. It clears
/// only when the task registers on its confirmed start, when saved evidence or
/// an operator attestation releases it, or when a later loan supersedes it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackgroundLaunchReservation {
    /// Stable launch request identity, used by an operator attestation binding
    pub request_id: RequestId,
    /// Launch task that keeps the resource reserved
    pub task_id: TaskId,
    /// Why the launch still reserves the resource
    pub status: BackgroundLaunchReservationStatus,
}

/// Why a first background launch still reserves its resource
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackgroundLaunchReservationStatus {
    /// The task row is queued, and its worker may still be starting
    Queued,
    /// The task started and waits for the resource owner to register it
    StartedUnregistered,
    /// The task ended before registration, and no proof shows that its GPU work stopped
    ///
    /// Only an operator attestation after inspecting the authority GPU releases it
    ReleaseUnproven,
    /// The launch records do not match the launch receipt
    IdentityMismatch,
}

/// One authority that could not answer a resource read
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnavailableAuthority {
    /// Machine that did not answer
    pub machine: MachineId,
    /// Last known machine name, when discovery has one
    pub name: Option<String>,
    /// Why the read failed
    pub message: String,
}

/// `GET /v1/resources/{id}` and the result of a settled resource mutation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDetail {
    /// Public API version
    pub api_version: u32,
    /// Resource with its fixed authority, supervisor, and revisions
    pub resource: Resource,
    /// Non-closed loan that reserves the resource, if one exists
    pub loan: Option<Loan>,
    /// Every request for this resource in serving order
    pub requests: Vec<ResourceRequestView>,
    /// Supervisor notices for the non-closed loan with their delivery state
    pub notices: Vec<SupervisorNotice>,
    /// Task that holds the resource for the current loan phase
    pub current_task: Option<ResourceTaskSummary>,
    /// Registered background task
    pub background_task: Option<ResourceTaskSummary>,
    /// Most important condition that needs an operator or supervisor
    pub attention: Option<AttentionView>,
    /// Identity of `current_task`, present even when its task row is unavailable
    pub current_task_id: Option<TaskId>,
    /// Identity of `background_task`, present even when its task row is unavailable
    pub background_task_id: Option<TaskId>,
    /// First background launch that reserves the unregistered resource, if one does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_launch: Option<BackgroundLaunchReservation>,
    /// Accepted execution mode of the exact current Restoring loan, when proven
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_execution_mode: Option<ReturnExecutionMode>,
    /// Decision window of the pending return action, when the loan awaits one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_window: Option<ReturnDecisionWindow>,
}

/// One queued, assigned, or retained request without its private command content
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRequestView {
    /// Caller retry identity
    pub request_id: RequestId,
    /// Preallocated task identity
    pub task_id: TaskId,
    /// Immutable authority-assigned acceptance identity
    pub acceptance_sequence: AcceptanceSequence,
    /// Machine that owns the requesting thread and callback route
    pub origin_machine: MachineId,
    /// Requesting thread, which runs on `origin_machine`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<ThreadId>,
    /// Submitted task name
    pub display_name: String,
    /// Queue state, separate from task process state
    pub state: ResourceRequestState,
}

/// Task fields a resource page needs, with the same names as the task list view
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceTaskSummary {
    /// Task identity
    pub id: TaskId,
    /// Non-empty server-derived label
    pub display_name: String,
    /// Process status
    pub status: ProcessStatus,
    /// Submitting thread, which runs on `origin_machine`, or on the executor
    /// when `origin_machine` is unknown
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<ThreadId>,
    /// Machine that owns callbacks, when known
    pub origin_machine: Option<MachineId>,
    /// Machine that runs the task, when known
    pub execution_machine: Option<MachineId>,
    /// Worker pid while running
    pub pid: Option<i32>,
    /// Terminal callback delivery state
    pub callback: CallbackStatus,
    /// Why the process ended, once known
    pub exit_reason: Option<ExitReason>,
    /// When cancellation was requested
    pub cancel_requested_at: Option<DateTime<Utc>>,
    /// Insert time
    pub created_at: DateTime<Utc>,
    /// Last row update
    pub updated_at: DateTime<Utc>,
}

/// Typed reason a resource needs attention
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionCode {
    /// The loan is in its durable attention state
    LoanNeedsAttention,
    /// The authority cannot safely select or advance a queued request
    QueueBlocked,
    /// The bound return task cannot close or advance the loan
    RestoreBlocked,
    /// The exact trainer release proof is missing or failed validation
    ReleaseProofUnavailable,
    /// A first background launch ended before registration with no release proof
    ///
    /// The resource stays reserved until an operator attests that its GPU work is gone
    BackgroundLaunchReleaseUnproven,
    /// The bound release watcher cannot proceed
    ReleaseWatcherBlocked,
    /// A supervisor notice used its automatic delivery attempts
    NoticeDeliveryFailed,
}

/// Condition that needs an operator or supervisor
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttentionView {
    /// Typed reason
    pub code: AttentionCode,
    /// Human-readable explanation
    pub message: String,
    /// Action that owns the condition, when one exists
    pub action_id: Option<ActionId>,
    /// Task that the condition names, when one exists
    pub task_id: Option<TaskId>,
    /// Notice that the condition names, when one exists
    pub notice_id: Option<NoticeId>,
}

/// `GET /v1/resources/pending?machine=<uuid>&thread=<uuid>`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingActionList {
    /// Public API version
    pub api_version: u32,
    /// Open supervisor actions for the exact thread
    pub actions: Vec<PendingActionView>,
    /// Authorities that could not answer; their actions are unknown, not absent
    pub unavailable_authorities: Vec<UnavailableAuthority>,
}

/// One open supervisor action read from durable loan and notice state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingActionView {
    /// Resource that the loan reserves
    pub resource_id: ResourceId,
    /// Fixed resource authority
    pub authority_machine: MachineId,
    /// Loan that owns the action
    pub loan_id: LoanId,
    /// Stable action identity
    pub action_id: ActionId,
    /// Resource revision a decision must present
    pub state_revision: ResourceRevision,
    /// Exact assigned supervisor
    pub supervisor: SupervisorAddress,
    /// Supervisor assignment revision a decision must present
    pub assignment_revision: AssignmentRevision,
    /// Required decision
    pub phase: PendingActionPhase,
    /// Return evidence retained by the loan, when the phase has one
    pub return_context: Option<ReturnContext>,
    /// Durable notice for the action, when one exists
    pub notice: Option<SupervisorNotice>,
    /// Decision window of the pending return action, when the loan awaits one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_window: Option<ReturnDecisionWindow>,
}

/// Decision that one open action requires
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PendingActionPhase {
    /// Release the exact observed background task
    ReleaseRequired {
        /// Background task that must release the resource
        observed_background_task: TaskId,
    },
    /// Choose same-task resume, new background work, or no resume
    ReturnRequired,
    /// A bound return task has not closed the loan yet
    Restoring {
        /// Fixed return task identity
        resume_task_id: TaskId,
    },
    /// Resolve an unsafe or uncertain loan condition
    AttentionRequired {
        /// Durable explanation of the condition
        reason: String,
        /// Last safe phase with its identities and return data
        last_safe_phase: Box<LoanPhase>,
    },
}

/// Fields that register one resource on the receiving daemon
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRegistration {
    /// Caller-allocated stable resource identity
    pub id: ResourceId,
    /// Human-readable resource name
    pub display_name: String,
    /// Exact supervisor thread for release and return notices
    pub supervisor: SupervisorAddress,
}

/// `POST /v1/resources/register`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRegisterBody {
    /// Public API version
    pub api_version: u32,
    /// Registration fields
    pub spec: ResourceRegistration,
}

/// `POST /v1/resources/{id}/supervisor`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorReplacementBody {
    /// Public API version
    pub api_version: u32,
    /// Resource revision the caller observed
    pub expected_revision: ResourceRevision,
    /// Exact new supervisor thread
    pub supervisor: SupervisorAddress,
}

/// `POST /v1/resources/{id}/requests/{request_id}/cancel`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestCancelBody {
    /// Public API version
    pub api_version: u32,
    /// Stable retry identity for this cancellation
    pub operation_id: Uuid,
    /// Resource revision the caller observed
    pub expected_revision: ResourceRevision,
}

/// `POST /v1/resources/{id}/actions`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceActionBody {
    /// Public API version
    pub api_version: u32,
    /// Resource revision the caller observed
    pub expected_revision: ResourceRevision,
    /// Stable retry identity; a changed body for the same identity is a conflict
    pub operation_id: Uuid,
    /// Requested control
    pub action: BrowserResourceAction,
}

/// One place for a moved queued request, relative to other queued requests
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueuePlacement {
    /// Place the request first in the queue
    Front,
    /// Place the request last in the queue
    Back,
    /// Place the request directly before another queued request
    Before {
        /// Anchor request identity
        request_id: RequestId,
    },
    /// Place the request directly after another queued request
    After {
        /// Anchor request identity
        request_id: RequestId,
    },
}

/// The resource controls exposed to operators and the dashboard
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrowserResourceAction {
    /// Remove one queued request through its origin-owned cancellation
    CancelQueued {
        /// Queued request identity
        request_id: RequestId,
    },
    /// Move one queued request to a new place in the serving order
    MoveQueued {
        /// Queued request identity
        request_id: RequestId,
        /// New place in the queue
        placement: QueuePlacement,
    },
    /// Cancel the active command task through its origin-owned cancellation
    StopActive {
        /// Exact active task identity
        task_id: TaskId,
    },
    /// Start one explicit delivery attempt for a notice whose automatic attempts failed
    Renotify {
        /// Notice identity
        notice_id: NoticeId,
    },
}

/// Response to `POST /v1/resources/{id}/requests`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRequestSubmitResponse {
    /// Public API version
    pub api_version: u32,
    /// Caller retry identity
    pub request_id: RequestId,
    /// Preallocated task identity
    pub task_id: TaskId,
    /// Resource queue that owns the request
    pub resource_id: ResourceId,
    /// Fixed resource authority
    pub authority_machine: MachineId,
    /// Saved authority result
    pub outcome: ResourceRequestSubmitOutcome,
}

/// Response to `POST /v1/resources/{id}/background`
///
/// The launch binds a task but does not register it; the resource owner
/// registers the task only after the task layer records a confirmed start
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceBackgroundSubmitResponse {
    /// Public API version
    pub api_version: u32,
    /// Caller retry identity
    pub request_id: RequestId,
    /// Task identity bound to the request
    pub task_id: TaskId,
    /// Authoritative resource after the launch binding
    pub resource: Resource,
    /// Saved authority result
    pub outcome: ResourceBackgroundSubmitOutcome,
}

/// Definitive authority result for one first background launch
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceBackgroundSubmitOutcome {
    /// This request bound the task and started its one worker spawn
    Inserted,
    /// An exact earlier launch exists; its task was observed and not respawned
    Existing {
        /// State retained by the task layer
        state: ProcessStatus,
    },
}

/// `POST /v1/resources/{id}/background/{task_id}/trainer-attempt`
///
/// The supervisor names the trainer attempt after the registered trainer holds
/// its ownership lock. The authority reads the attempt request and probes the
/// lock itself; the body carries no evidence
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrainerAttemptBody {
    /// Public API version
    pub api_version: u32,
    /// Trainer identity of the running attempt
    pub attempt_binding: AttemptBinding,
}

/// Response to `POST /v1/resources/{id}/background/{task_id}/trainer-attempt`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrainerAttemptResponse {
    /// Public API version
    pub api_version: u32,
    /// Authoritative resource after the association
    pub resource: Resource,
    /// Registered task bound to the attempt
    pub task_id: TaskId,
    /// Canonical runtime root whose lock the trainer held
    pub runtime_root: std::path::PathBuf,
    /// Trainer identity of the associated attempt
    pub attempt_binding: AttemptBinding,
}

/// `POST /v1/resources/{id}/initial-idle`
///
/// The attestation is a human confirmation after inspecting the authority GPU
/// It is not an automatic proof that GPU work stopped
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialIdleBody {
    /// Public API version
    pub api_version: u32,
    /// Complete, immutable initial idle attestation
    pub attestation: crate::resource::initial_idle::InitialIdleAttestation,
}

/// Saved receipt from `POST /v1/resources/{id}/initial-idle`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialIdleResponse {
    /// Public API version
    pub api_version: u32,
    /// Durable receipt saved by the authority
    pub receipt: crate::resource::initial_idle::InitialIdleReceipt,
    /// Whether the authority returned an exact earlier receipt
    pub replayed: bool,
}

/// `POST /v1/resources/{id}/operator-release`
///
/// The attestation is a human confirmation after inspecting the authority GPU
/// It is not an automatic proof that GPU work stopped
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorReleaseBody {
    /// Public API version
    pub api_version: u32,
    /// Complete, immutable operator attestation
    pub attestation: OperatorGpuFreeAttestation,
}

/// Saved receipt from `POST /v1/resources/{id}/operator-release`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorReleaseResponse {
    /// Public API version
    pub api_version: u32,
    /// Durable receipt saved by the authority
    pub receipt: OperatorGpuFreeReceipt,
    /// Whether the authority returned an exact earlier receipt
    pub replayed: bool,
}

/// Definitive authority result for one request submission
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceRequestSubmitOutcome {
    /// The request waits in serving order or runs under a loan
    Waiting,
    /// The authority activated the command task
    Activated,
    /// The authority rejected the request before activation
    Rejected {
        /// Durable rejection reason
        reason: String,
    },
    /// The request was cancelled before activation
    CancelledBeforeLaunch,
}

/// Fleet read of one authority's resources
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterResourceList {
    /// Public API version
    pub api_version: u32,
    /// Authority that answered
    pub machine: MachineId,
    /// Resources owned by that authority
    pub resources: Vec<ResourceOverview>,
}

/// Fleet read of one resource detail, absent when the receiver is not its authority
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterResourceDetail {
    /// Public API version
    pub api_version: u32,
    /// Authority that answered
    pub machine: MachineId,
    /// Detail, when the receiver owns the resource
    pub detail: Option<ResourceDetail>,
}

/// Fleet read of pending actions owned by one authority
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterPendingActions {
    /// Public API version
    pub api_version: u32,
    /// Authority that answered
    pub machine: MachineId,
    /// Open actions for the requested thread
    pub actions: Vec<PendingActionView>,
}

/// Destination-checked control forwarded to the resource authority
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterResourceControl {
    /// Public API version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Intended resource authority
    pub destination_machine: MachineId,
    /// Daemon that forwarded the control
    pub source_machine: MachineId,
    /// Resource that the control targets
    pub resource_id: ResourceId,
    /// Requested control
    pub control: ResourceControl,
}

/// One authority-owned resource mutation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceControl {
    /// A dashboard or CLI action with a stable operation identity
    Action {
        /// Resource revision the caller observed
        expected_revision: ResourceRevision,
        /// Stable retry identity
        operation_id: Uuid,
        /// Requested action
        action: BrowserResourceAction,
    },
    /// Explicit supervisor replacement
    ReplaceSupervisor {
        /// Resource revision the caller observed
        expected_revision: ResourceRevision,
        /// Exact new supervisor thread
        supervisor: SupervisorAddress,
    },
}
