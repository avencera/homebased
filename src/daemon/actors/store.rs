//! `StoreActor` owns the daemon's single SQLite connection.

use std::collections::HashMap;
use std::path::PathBuf;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::cancellation::{
    CancellationReceipt, CancellationRequest, CancellationRequestIdentity, ExecutorCancelState,
    ResourceCancellationRequestIdentity,
};
use crate::daemon::actors::send_reply;
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskId, TaskReport, TaskRow, ThreadId,
};
use crate::error::AppError;
use std::num::NonZeroU64;

use crate::events::{
    DeliveryOutcome, EventAcceptance, EventError, EventPayload, EventRouteStatus, FailedInboxEvent,
    InboxEvent, OutboxEvent, TaskEvent,
};
use crate::machine::MachineId;
use crate::message::{
    MessageAttempt, MessageDelivery, MessageId, MessageReceipt, MessageSendRequest,
    OutboundMessageBinding, Recipient,
};
use crate::resource::ownership_lock::VerifiedTrainerAttempt;
use crate::resource::release_watcher::{ReleaseWatcherPollOutcome, ReleaseWatcherPollRequest};
use crate::resource::store::{
    AcceptedResourceTask, AssignedResourceTaskReconcileInput, AssignedResourceTaskReconcileOutcome,
    CompleteReleaseError, OpenReleaseLoanError, OpenReleaseLoanResult, QueueCancellationResult,
    ReleaseCheckpointCancellationOutcome, ReleaseCheckpointError, ReleaseCompletionResult,
    ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError, ReleaseWatcherAcceptanceInput,
    ResourceQueueReconcileError, ResourceSnapshot, ResourceStoreError, ResourceTaskAcceptance,
    ResourceTaskAcceptanceInput, SupervisorNoticeStoreError, TrainerAttemptAssociationStoreError,
};
use crate::resource::{
    ActionId, AssignmentRevision, DeliveryAttemptId, NoticeId, ReleaseCheckpointBaseline,
    ReleaseCheckpointStopDecision, ReleaseCheckpointStopOutcome, ReleaseWatcherIntent, Resource,
    ResourceId, ResourceQueueReconcileOutcome, ResourceRequest, ResourceRevision,
    SupervisorAddress, SupervisorNotice, TrainerAttemptAssociation,
};
use crate::spec::NormalizedSpec;
use crate::store::IdentityError;
use crate::store::{CancelResult, Store, TaskPresentation};
use crate::submission::{
    ExecutorIdentity, OriginRoute, RejectionTombstone, RequestId, ResourceCancellationReceipt,
    ResourceQueueReceipt, SubmissionState,
};

fn identity_error(error: IdentityError) -> AppError {
    match error {
        IdentityError::Conflict => AppError::Usage {
            message: "executor identity conflict".into(),
        },
        IdentityError::RouteNotFound => AppError::Internal {
            message: "executor identity disappeared".into(),
        },
        IdentityError::Storage(error) => error,
    }
}

fn resource_error(
    error: ResourceStoreError,
    task: Option<TaskId>,
    request: Option<RequestId>,
) -> AppError {
    match error {
        ResourceStoreError::Conflict => task.map_or_else(
            || AppError::Usage {
                message: "resource identity conflict".into(),
            },
            |task| AppError::ClusterTaskConflict { task },
        ),
        ResourceStoreError::LegacyWatcherIntentUnproven => AppError::Usage {
            message: "legacy release watcher identity is unproven".into(),
        },
        ResourceStoreError::Prevented => match (request, task) {
            (Some(request), Some(task)) => AppError::SubmissionRejected {
                request,
                task,
                reason: "cancelled_before_acceptance".into(),
            },
            _ => AppError::Usage {
                message: "resource request was prevented before acceptance".into(),
            },
        },
        ResourceStoreError::ResourceNotFound => AppError::Usage {
            message: "resource not found".into(),
        },
        ResourceStoreError::WrongAuthority { expected, found } => {
            AppError::MachineIdentityMismatch {
                expected,
                found: Some(found),
            }
        }
        ResourceStoreError::OriginRouteNotFound { task } => AppError::RouteNotFound { task },
        ResourceStoreError::ExecutorAlreadyAccepted { task } => {
            AppError::ClusterTaskConflict { task }
        }
        ResourceStoreError::Identity(IdentityError::Conflict) => task.map_or_else(
            || AppError::Usage {
                message: "resource executor identity conflict".into(),
            },
            |task| AppError::ClusterTaskConflict { task },
        ),
        ResourceStoreError::Identity(IdentityError::RouteNotFound) => AppError::Internal {
            message: "executor identity disappeared during resource cancellation".into(),
        },
        ResourceStoreError::Identity(IdentityError::Storage(error)) => error,
        ResourceStoreError::InvalidCommandSpec(error) => AppError::Usage {
            message: error.to_string(),
        },
        ResourceStoreError::TaskPreparation(error) => error,
        ResourceStoreError::TaskRow(error) => error,
        ResourceStoreError::Event(error) => event_error(error),
        ResourceStoreError::Storage(error) => error.into(),
    }
}

fn resource_queue_reconcile_error(error: ResourceQueueReconcileError) -> AppError {
    match error {
        ResourceQueueReconcileError::Resource(error)
        | ResourceQueueReconcileError::Release(OpenReleaseLoanError::Resource(error)) => {
            resource_error(error, None, None)
        }
        ResourceQueueReconcileError::Storage(error) => error.into(),
        ResourceQueueReconcileError::Release(error) => AppError::Internal {
            message: format!("resource queue reconciliation failed: {error}"),
        },
    }
}

fn event_error(error: EventError) -> AppError {
    match error {
        EventError::RouteNotFound { task } => AppError::RouteNotFound { task },
        EventError::OwnerConflict { task } => AppError::ClusterTaskConflict { task },
        EventError::ContentConflict { task, seq } => AppError::EventContentConflict { task, seq },
        EventError::Invalid { message } => AppError::Usage { message },
        EventError::Storage(error) => error,
    }
}

/// Messages for daemon SQLite operations.
#[expect(
    private_interfaces,
    reason = "typed storage outcomes stay crate-private across this internal actor boundary"
)]
pub enum StoreMsg {
    /// Migrate historical local rows before supervisor recovery begins.
    MigrateLegacyLocal {
        machine: MachineId,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Register or reuse one resource on its fixed authority
    RegisterResource {
        authority_machine: MachineId,
        resource: Box<Resource>,
        reply: RpcReplyPort<Result<Resource, AppError>>,
    },
    /// Load the local authority's resources and non-closed loans for startup
    ResourceSnapshotsForAuthority {
        authority_machine: MachineId,
        reply: RpcReplyPort<Result<Vec<ResourceSnapshot>, AppError>>,
    },
    /// Bind verified trainer attempt evidence to an authority-owned registered task.
    BindTrainerAttemptAssociationForAuthority {
        /// Fixed authority recorded on the resource.
        authority_machine: MachineId,
        /// Resource that registered the trainer task.
        resource_id: ResourceId,
        /// Exact registered Homebased background task.
        task_id: TaskId,
        /// Point-in-time trainer request and held-lock evidence.
        verified_attempt: Box<VerifiedTrainerAttempt>,
        /// Typed durable association result.
        reply: RpcReplyPort<
            Result<
                Result<TrainerAttemptAssociation, TrainerAttemptAssociationStoreError>,
                AppError,
            >,
        >,
    },
    /// Read the saved trainer attempt association for one authority-owned resource.
    TrainerAttemptAssociationForAuthority {
        /// Fixed authority recorded on the resource.
        authority_machine: MachineId,
        /// Resource whose association is read.
        resource_id: ResourceId,
        /// Typed durable association result, if one exists.
        reply: RpcReplyPort<
            Result<
                Result<Option<TrainerAttemptAssociation>, TrainerAttemptAssociationStoreError>,
                AppError,
            >,
        >,
    },
    /// Read a historical trainer association by its exact task identity.
    TrainerAttemptAssociationForTaskForAuthority {
        /// Fixed authority recorded on the associated resource.
        authority_machine: MachineId,
        /// Exact historical task identity.
        task_id: TaskId,
        /// Typed durable association result, if one exists.
        reply: RpcReplyPort<
            Result<
                Result<Option<TrainerAttemptAssociation>, TrainerAttemptAssociationStoreError>,
                AppError,
            >,
        >,
    },
    /// Read exact accepted task identities from authority-owned resource assignments.
    AcceptedResourceTasksForAuthority {
        authority_machine: MachineId,
        reply: RpcReplyPort<Result<Vec<AcceptedResourceTask>, AppError>>,
    },
    /// Accept a command request and allocate its authority FIFO position
    AcceptResourceRequest {
        authority_machine: MachineId,
        request_id: RequestId,
        task_id: TaskId,
        resource_id: ResourceId,
        origin_machine: MachineId,
        normalized_spec: Box<NormalizedSpec>,
        reply: RpcReplyPort<Result<ResourceRequest, AppError>>,
    },
    /// Atomically accept the exact request selected by a Serving loan as a queued task.
    AcceptAssignedResourceTask {
        /// Selection identity, immutable spec, and executor runtime environment.
        input: Box<ResourceTaskAcceptanceInput>,
        /// Typed task-layer result inside actor and transport errors.
        reply: RpcReplyPort<Result<Result<ResourceTaskAcceptance, ResourceStoreError>, AppError>>,
    },
    /// Reconcile one exact assigned task and atomically advance only after its exit proof
    AssignedResourceTaskReconcile {
        /// Exact resource, loan, request, task, and expected revision to reconcile
        input: AssignedResourceTaskReconcileInput,
        /// Typed evidence result inside actor and transport errors
        reply: RpcReplyPort<
            Result<Result<AssignedResourceTaskReconcileOutcome, ResourceStoreError>, AppError>,
        >,
    },
    /// Read all requests for one resource in authority acceptance order
    ResourceRequests {
        authority_machine: MachineId,
        resource_id: ResourceId,
        reply: RpcReplyPort<Result<Vec<ResourceRequest>, AppError>>,
    },
    /// Read the oldest queued request for one resource
    OldestQueuedResourceRequest {
        authority_machine: MachineId,
        resource_id: ResourceId,
        reply: RpcReplyPort<Result<Option<ResourceRequest>, AppError>>,
    },
    /// Reconcile one resource queue from current authority-owned state.
    ReconcileResourceQueue {
        /// Fixed authority machine recorded on the resource.
        authority_machine: MachineId,
        /// Resource queue to reconcile.
        resource_id: ResourceId,
        /// Typed durable-state result, including fail-closed attention reasons.
        reply: RpcReplyPort<Result<ResourceQueueReconcileOutcome, AppError>>,
    },
    /// Fence cancellation before executor activation
    CancelResourceRequestBeforeActivation {
        authority_machine: MachineId,
        request_id: RequestId,
        task_id: TaskId,
        resource_id: ResourceId,
        origin_machine: MachineId,
        reply: RpcReplyPort<Result<QueueCancellationResult, AppError>>,
    },
    /// Read a durable resource cancellation receipt, checking its full identity.
    ResourceCancellationReceipt {
        /// Stable resource cancellation identity.
        identity: ResourceCancellationRequestIdentity,
        /// Saved authority result, if this identity was already handled.
        reply:
            RpcReplyPort<Result<Option<crate::submission::ResourceCancellationReceipt>, AppError>>,
    },
    /// Cancel a queued or assigned resource request and retain its exact authority result.
    CancelResourceRequestWithReceipt {
        /// Fixed authority that owns the resource queue.
        authority_machine: MachineId,
        /// Stable resource cancellation identity.
        identity: ResourceCancellationRequestIdentity,
        /// Exact validated origin-route proof.
        proof: crate::submission::ResourceRouteProof,
        /// Durable resource cancellation result.
        reply: RpcReplyPort<Result<crate::submission::ResourceCancellationReceipt, AppError>>,
    },
    /// Open or reuse a release loan and keep the storage result typed
    OpenReleaseLoanForAuthority {
        /// Authority machine recorded on the resource
        authority_machine: MachineId,
        /// Resource that will be loaned
        resource_id: ResourceId,
        /// Resource revision observed by the caller
        expected_state_revision: ResourceRevision,
        /// Storage result inside actor and transport errors
        reply: RpcReplyPort<Result<Result<OpenReleaseLoanResult, OpenReleaseLoanError>, AppError>>,
    },
    /// Bind a preallocated watcher launch identity to the saved release action
    BindReleaseWatcherForAuthority {
        /// Authority machine recorded on the resource
        authority_machine: MachineId,
        /// Resource whose release action owns this watcher
        resource_id: ResourceId,
        /// Exact action, revision, observed task, watcher task, request, and spec digest
        intent: ReleaseWatcherIntent,
        /// Storage result inside actor and transport errors
        reply: RpcReplyPort<Result<Result<ReleaseWatcherIntent, ResourceStoreError>, AppError>>,
    },
    /// Capture the exact trainer checkpoint baseline before the watcher is accepted.
    CaptureReleaseCheckpointBaselineForAuthority {
        /// Authority machine recorded on the resource.
        authority_machine: MachineId,
        /// Resource whose release action owns the baseline.
        resource_id: ResourceId,
        /// Stable release action identity.
        action_id: ActionId,
        /// Resource revision observed with the release notice.
        expected_state_revision: ResourceRevision,
        /// Typed checkpoint baseline or attention result.
        reply: RpcReplyPort<
            Result<Result<ReleaseCheckpointBaseline, ReleaseCheckpointError>, AppError>,
        >,
    },
    /// Reserve a stop decision only after the exact trainer checkpoint verifies.
    ReserveReleaseCheckpointStopForAuthority {
        /// Authority machine recorded on the resource.
        authority_machine: MachineId,
        /// Resource whose release action owns the stop decision.
        resource_id: ResourceId,
        /// Stable release action identity.
        action_id: ActionId,
        /// Resource revision observed with the release notice.
        expected_state_revision: ResourceRevision,
        /// Typed reservation result or attention result.
        reply: RpcReplyPort<
            Result<Result<ReleaseCheckpointStopOutcome, ReleaseCheckpointError>, AppError>,
        >,
    },
    /// Revalidate the one saved checkpoint without selecting a replacement.
    RevalidateReleaseCheckpointStopForAuthority {
        /// Authority machine recorded on the resource.
        authority_machine: MachineId,
        /// Resource whose release action owns the stop decision.
        resource_id: ResourceId,
        /// Stable release action identity.
        action_id: ActionId,
        /// Resource revision observed with the release notice.
        expected_state_revision: ResourceRevision,
        /// The exact saved decision, or a changed-publication attention result.
        reply: RpcReplyPort<
            Result<Result<ReleaseCheckpointStopDecision, ReleaseCheckpointError>, AppError>,
        >,
    },
    /// Commit cancellation for the exact reserved trainer task and saved checkpoint
    CommitReleaseCheckpointCancellationForAuthority {
        /// Authority machine recorded on the resource
        authority_machine: MachineId,
        /// Resource whose release action owns the stop decision
        resource_id: ResourceId,
        /// Stable release action identity
        action_id: ActionId,
        /// Resource revision observed with the release notice
        expected_state_revision: ResourceRevision,
        /// Exact decision returned by the authority reservation operation
        decision: Box<ReleaseCheckpointStopDecision>,
        /// Typed commit, retry, or watcher-not-ready outcome
        reply: RpcReplyPort<
            Result<Result<ReleaseCheckpointCancellationOutcome, ReleaseCheckpointError>, AppError>,
        >,
    },
    /// Atomically bind and persist the one co-located fixed-ID watcher acceptance
    AcceptReleaseWatcherForAuthority {
        /// Fixed task, callback, action, and owner data to accept
        input: Box<ReleaseWatcherAcceptanceInput>,
        /// Typed storage outcome inside actor and transport errors
        reply: RpcReplyPort<
            Result<Result<ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError>, AppError>,
        >,
    },
    /// Validate one co-located watcher poll and advance its saved stop decision
    PollReleaseWatcherForAuthority {
        /// Machine that must own the polled resource
        authority_machine: MachineId,
        /// Exact identities sent by the watcher command
        request: ReleaseWatcherPollRequest,
        /// Typed poll decision; errors are storage failures the watcher may retry
        reply: RpcReplyPort<
            Result<Result<ReleaseWatcherPollOutcome, ReleaseCheckpointError>, AppError>,
        >,
    },
    /// Complete a saved release action and keep the storage result typed
    CompleteReleaseForAuthority {
        /// Authority machine recorded on the resource
        authority_machine: MachineId,
        /// Resource whose saved release action is completed
        resource_id: ResourceId,
        /// Stable identity of the release action
        action_id: ActionId,
        /// Resource revision observed with the release notice
        expected_state_revision: ResourceRevision,
        /// Storage result inside actor and transport errors
        reply:
            RpcReplyPort<Result<Result<ReleaseCompletionResult, CompleteReleaseError>, AppError>>,
    },
    /// Read one durable supervisor notice
    SupervisorNotice {
        /// Stable notice identity
        notice_id: NoticeId,
        /// Storage result inside actor and transport errors
        reply: RpcReplyPort<
            Result<Result<Option<SupervisorNotice>, SupervisorNoticeStoreError>, AppError>,
        >,
    },
    /// List notices that can receive another delivery attempt
    PendingSupervisorNotices {
        /// Storage result inside actor and transport errors
        reply: RpcReplyPort<
            Result<Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError>, AppError>,
        >,
    },
    /// Reserve one exact supervisor notice delivery attempt
    ReserveSupervisorNoticeAttempt {
        /// Stable notice identity
        notice_id: NoticeId,
        /// Stable identity for this delivery attempt
        attempt_id: DeliveryAttemptId,
        /// Storage result inside actor and transport errors
        reply: RpcReplyPort<Result<Result<SupervisorNotice, SupervisorNoticeStoreError>, AppError>>,
    },
    /// Settle one exact supervisor notice delivery attempt
    SettleSupervisorNoticeAttempt {
        /// Stable notice identity
        notice_id: NoticeId,
        /// Stable identity for the in-flight delivery attempt
        attempt_id: DeliveryAttemptId,
        /// Result of the exact delivery attempt
        result: Result<(), String>,
        /// Storage result inside actor and transport errors
        reply: RpcReplyPort<Result<Result<SupervisorNotice, SupervisorNoticeStoreError>, AppError>>,
    },
    /// Recover in-flight supervisor notices after restart
    RecoverSendingSupervisorNotices {
        /// Storage result inside actor and transport errors
        reply: RpcReplyPort<
            Result<Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError>, AppError>,
        >,
    },
    /// Retarget an undelivered notice with assignment-revision compare-and-set
    RetargetSupervisorNotice {
        /// Stable notice identity
        notice_id: NoticeId,
        /// Assignment revision read before selecting the new supervisor
        expected_assignment_revision: AssignmentRevision,
        /// Exact new machine and thread destination
        destination: SupervisorAddress,
        /// New assignment revision that must exceed the expected revision
        new_assignment_revision: AssignmentRevision,
        /// Storage result inside actor and transport errors
        reply: RpcReplyPort<Result<Result<SupervisorNotice, SupervisorNoticeStoreError>, AppError>>,
    },
    /// Save or reuse a caller-owned cancellation before network delivery
    InsertCancellationRequest {
        request: CancellationRequest,
        reply: RpcReplyPort<Result<(CancellationRequest, bool), AppError>>,
    },
    /// Read the retained caller-owned request for one task
    GetCancellationRequest {
        task: TaskId,
        reply: RpcReplyPort<Result<Option<CancellationRequest>, AppError>>,
    },
    /// Resume caller-owned requests after restart
    PendingCancellationRequests {
        reply: RpcReplyPort<Result<Vec<CancellationRequest>, AppError>>,
    },
    /// Set delivery complete after a validated executor receipt
    AcknowledgeCancellation {
        receipt: CancellationReceipt,
        reply: RpcReplyPort<Result<CancellationRequest, AppError>>,
    },
    /// Settle one resource cancellation intent with its authority receipt.
    AcknowledgeResourceCancellation {
        receipt: crate::submission::ResourceCancellationReceipt,
        reply: RpcReplyPort<Result<CancellationRequest, AppError>>,
    },
    /// Store executor receipt and a possible pre-acceptance tombstone
    ReceiveCancellation {
        request: CancellationRequestIdentity,
        reply: RpcReplyPort<Result<CancellationReceipt, AppError>>,
    },
    /// Resume accepted cancellation after executor restart
    PendingExecutorCancellations {
        reply: RpcReplyPort<Result<Vec<CancellationReceipt>, AppError>>,
    },
    /// Retain executor application result
    FinishExecutorCancellation {
        cancellation: uuid::Uuid,
        state: ExecutorCancelState,
        reply: RpcReplyPort<Result<CancellationReceipt, AppError>>,
    },
    /// Reserve a message UUID and compare its complete caller request
    BeginOutboundMessage {
        request: MessageSendRequest,
        reply: RpcReplyPort<Result<OutboundMessageBinding, AppError>>,
    },
    /// Commit the fixed receiver and recipient for a reserved message
    BindOutboundMessage {
        request: MessageSendRequest,
        destination_machine: MachineId,
        recipient: Recipient,
        reply: RpcReplyPort<Result<OutboundMessageBinding, AppError>>,
    },
    /// Read one message's saved attempt and optional success receipt
    MessageDelivery {
        id: MessageId,
        reply: RpcReplyPort<Result<MessageDelivery, AppError>>,
    },
    /// Bind a message UUID to immutable content and its resolved destination
    BindMessageAttempt {
        attempt: MessageAttempt,
        reply: RpcReplyPort<Result<MessageAttempt, AppError>>,
    },
    /// Commit a message receipt after the queue command succeeds
    CommitMessageReceipt {
        receipt: MessageReceipt,
        reply: RpcReplyPort<Result<MessageReceipt, AppError>>,
    },
    /// Find all deliverable executor outbox tasks
    PendingOutboundTasks {
        reply: RpcReplyPort<Result<Vec<TaskId>, AppError>>,
    },
    /// Compact one bounded batch of old, settled event payloads
    CompactOldEventPayloads {
        reply: RpcReplyPort<Result<crate::store::EventRetentionBatch, AppError>>,
    },
    /// Read the earliest pending row for one task
    FirstPendingOutbound {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<OutboxEvent>, AppError>>,
    },
    /// Read one retained row at or after one sequence
    OutboundEventAtOrAfter {
        id: TaskId,
        seq: NonZeroU64,
        reply: RpcReplyPort<Result<Option<OutboxEvent>, AppError>>,
    },
    /// Append an event for a retained accepted execution identity
    AppendOutboundEvent {
        id: TaskId,
        origin: MachineId,
        execution: MachineId,
        payload: EventPayload,
        reply: RpcReplyPort<Result<OutboxEvent, AppError>>,
    },
    /// Record durable origin receipt for one row
    AcknowledgeOutbound {
        id: TaskId,
        seq: NonZeroU64,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Stop sends after a verified route-not-found response
    OrphanOutboundRoute {
        id: TaskId,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Read the executor transport state without mutation
    OutboundRouteStatus {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<EventRouteStatus>, AppError>>,
    },
    /// Find tasks with callback work left from this or a previous daemon
    PendingInboxTasks {
        reply: RpcReplyPort<Result<Vec<TaskId>, AppError>>,
    },
    /// Read only the first event after the settled cursor
    EarliestInbox {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<InboxEvent>, AppError>>,
    },
    /// Reserve exactly one queue attempt for the earliest pending event
    ReserveInboxAttempt {
        id: TaskId,
        seq: NonZeroU64,
        reply: RpcReplyPort<Result<Option<InboxEvent>, AppError>>,
    },
    /// Record one command result and advance the contiguous settled cursor
    SettleInboxAttempt {
        id: TaskId,
        seq: NonZeroU64,
        outcome: DeliveryOutcome,
        reply: RpcReplyPort<Result<InboxEvent, AppError>>,
    },
    /// Read retained origin callback failures
    FailedInboxEvents {
        id: TaskId,
        reply: RpcReplyPort<Result<Vec<FailedInboxEvent>, AppError>>,
    },
    /// Fetch the immutable origin callback route
    OriginRoute {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<OriginRoute>, AppError>>,
    },
    /// Fetch the original route for a caller retry UUID
    OriginRouteByRequest {
        request: RequestId,
        reply: RpcReplyPort<Result<Option<OriginRoute>, AppError>>,
    },
    /// Find unresolved origin routes after a daemon restart
    UnknownOriginRoutes {
        reply: RpcReplyPort<Result<Vec<OriginRoute>, AppError>>,
    },
    /// Find unresolved resource origin routes after a daemon restart
    UnknownResourceOriginRoutes {
        reply: RpcReplyPort<Result<Vec<OriginRoute>, AppError>>,
    },
    /// Durably allocate a request and its origin route before network send
    InsertOriginRoute {
        route: Box<OriginRoute>,
        reply: RpcReplyPort<Result<OriginRoute, AppError>>,
    },
    /// Persist a definitive executor result
    ResolveOriginRoute {
        id: TaskId,
        outcome: SubmissionState,
        reply: RpcReplyPort<Result<OriginRoute, AppError>>,
    },
    /// Apply a definitive resource queue receipt to an origin route
    ResolveResourceRoute {
        receipt: ResourceQueueReceipt,
        reply: RpcReplyPort<Result<OriginRoute, AppError>>,
    },
    /// Cancel an origin resource route after pre-activation authority confirmation
    CancelResourceRouteBeforeLaunch {
        receipt: ResourceCancellationReceipt,
        reply: RpcReplyPort<Result<OriginRoute, AppError>>,
    },
    /// Accept a sequenced origin event and return only after commit
    AcceptInboundEvent {
        event: Box<TaskEvent>,
        reply: RpcReplyPort<Result<EventAcceptance, AppError>>,
    },
    /// Read a retained executor identity.
    ExecutorIdentity {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<ExecutorIdentity>, AppError>>,
    },
    /// Store a definitive pre-acceptance rejection.
    RejectExecution {
        tombstone: RejectionTombstone,
        reply: RpcReplyPort<Result<ExecutorIdentity, AppError>>,
    },
    /// Abandon an identity before acceptance.
    AbandonExecution {
        id: TaskId,
        origin: MachineId,
        execution: MachineId,
        reply: RpcReplyPort<Result<ExecutorIdentity, AppError>>,
    },
    /// Insert a queued task.
    InsertTask {
        row: Box<TaskRow>,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Insert a new local task with retained ownership and its queued event
    InsertLocalTask {
        row: Box<TaskRow>,
        spec: Box<NormalizedSpec>,
        machine: MachineId,
        codex: PathBuf,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Atomically retain a remote identity, queued detail, and first event
    InsertRemoteTask {
        row: Box<TaskRow>,
        spec: Box<NormalizedSpec>,
        origin: MachineId,
        execution: MachineId,
        reply: RpcReplyPort<Result<ExecutorIdentity, AppError>>,
    },
    /// Check the typed compatibility boundary for a task
    IsEventTask {
        id: TaskId,
        reply: RpcReplyPort<Result<bool, AppError>>,
    },
    /// Fetch one task.
    GetTask {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<TaskRow>, AppError>>,
    },
    /// Read child process-group evidence for one exact task identity.
    GetProcessGroupExitEvidence {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<ProcessGroupExitEvidence>, AppError>>,
    },
    /// List with optional filters.
    ListTasks {
        statuses: Vec<ProcessStatus>,
        thread: Option<ThreadId>,
        reply: RpcReplyPort<Result<Vec<TaskRow>, AppError>>,
    },
    /// Read dashboard metadata for a set of task rows.
    TaskPresentations {
        ids: Vec<TaskId>,
        reply: RpcReplyPort<Result<HashMap<TaskId, TaskPresentation>, AppError>>,
    },
    /// Queued and running tasks.
    NonTerminal {
        reply: RpcReplyPort<Result<Vec<TaskRow>, AppError>>,
    },
    /// Count of queued or running tasks.
    InFlightCount {
        reply: RpcReplyPort<Result<usize, AppError>>,
    },
    /// Compare-and-swap process status. Replies with the post-update row, or
    /// `None` when the CAS did not match.
    CasStatus {
        id: TaskId,
        from: ProcessStatus,
        to: ProcessStatus,
        reply: RpcReplyPort<Result<Option<TaskRow>, AppError>>,
    },
    /// Compare-and-swap to the status the reason implies, storing the reason.
    CasExit {
        id: TaskId,
        from: ProcessStatus,
        reason: ExitReason,
        process_group_exit_evidence: ProcessGroupExitEvidence,
        reply: RpcReplyPort<Result<Option<TaskRow>, AppError>>,
    },
    /// Record the worker pid.
    SetPid {
        id: TaskId,
        pid: i32,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Request cancel.
    RequestCancel {
        id: TaskId,
        reply: RpcReplyPort<Result<CancelResult, AppError>>,
    },
    /// Commit a typed task's inactivity event and claim in one transaction
    ProduceAttentionEvent {
        id: TaskId,
        reply: RpcReplyPort<Result<bool, AppError>>,
    },
    /// Release a legacy direct-send claim after its owner bound passes.
    ReleaseAttention {
        id: TaskId,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Reports in seq order.
    Reports {
        id: TaskId,
        reply: RpcReplyPort<Result<Vec<TaskReport>, AppError>>,
    },
}

/// Owns `rusqlite::Connection` via `Store`.
pub struct StoreActor;

impl Actor for StoreActor {
    type Msg = StoreMsg;
    type State = Store;
    type Arguments = PathBuf;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        db_path: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(Store::open(&db_path)?)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            StoreMsg::MigrateLegacyLocal { machine, reply } => {
                send_reply(reply, state.migrate_legacy_local(machine));
            }
            StoreMsg::RegisterResource {
                authority_machine,
                resource,
                reply,
            } => send_reply(
                reply,
                state
                    .register_resource(authority_machine, &resource)
                    .map_err(|error| resource_error(error, None, None)),
            ),
            StoreMsg::ResourceSnapshotsForAuthority {
                authority_machine,
                reply,
            } => send_reply(
                reply,
                state
                    .resource_snapshots_for_authority(authority_machine)
                    .map_err(|error| resource_error(error, None, None)),
            ),
            StoreMsg::BindTrainerAttemptAssociationForAuthority {
                authority_machine,
                resource_id,
                task_id,
                verified_attempt,
                reply,
            } => send_reply(
                reply,
                Ok(state.bind_trainer_attempt_association(
                    authority_machine,
                    resource_id,
                    task_id,
                    *verified_attempt,
                )),
            ),
            StoreMsg::TrainerAttemptAssociationForAuthority {
                authority_machine,
                resource_id,
                reply,
            } => send_reply(
                reply,
                Ok(state.trainer_attempt_association_for_authority(authority_machine, resource_id)),
            ),
            StoreMsg::TrainerAttemptAssociationForTaskForAuthority {
                authority_machine,
                task_id,
                reply,
            } => send_reply(
                reply,
                Ok(state.trainer_attempt_association_for_task_for_authority(
                    authority_machine,
                    task_id,
                )),
            ),
            StoreMsg::AcceptedResourceTasksForAuthority {
                authority_machine,
                reply,
            } => send_reply(
                reply,
                state
                    .accepted_resource_tasks_for_authority(authority_machine)
                    .map_err(|error| resource_error(error, None, None)),
            ),
            StoreMsg::AcceptResourceRequest {
                authority_machine,
                request_id,
                task_id,
                resource_id,
                origin_machine,
                normalized_spec,
                reply,
            } => send_reply(
                reply,
                state
                    .accept_resource_request(
                        authority_machine,
                        request_id,
                        task_id,
                        resource_id,
                        origin_machine,
                        *normalized_spec,
                    )
                    .map_err(|error| resource_error(error, Some(task_id), Some(request_id))),
            ),
            StoreMsg::AcceptAssignedResourceTask { input, reply } => {
                send_reply(reply, Ok(state.accept_assigned_resource_task(*input)));
            }
            StoreMsg::AssignedResourceTaskReconcile { input, reply } => {
                send_reply(
                    reply,
                    Ok(state.reconcile_assigned_resource_task_for_authority(input)),
                );
            }
            StoreMsg::ResourceRequests {
                authority_machine,
                resource_id,
                reply,
            } => send_reply(
                reply,
                state
                    .resource_requests(authority_machine, resource_id)
                    .map_err(|error| resource_error(error, None, None)),
            ),
            StoreMsg::OldestQueuedResourceRequest {
                authority_machine,
                resource_id,
                reply,
            } => send_reply(
                reply,
                state
                    .oldest_queued_resource_request(authority_machine, resource_id)
                    .map_err(|error| resource_error(error, None, None)),
            ),
            StoreMsg::ReconcileResourceQueue {
                authority_machine,
                resource_id,
                reply,
            } => send_reply(
                reply,
                state
                    .reconcile_resource_queue_for_authority(authority_machine, resource_id)
                    .map_err(resource_queue_reconcile_error),
            ),
            StoreMsg::CancelResourceRequestBeforeActivation {
                authority_machine,
                request_id,
                task_id,
                resource_id,
                origin_machine,
                reply,
            } => send_reply(
                reply,
                state
                    .cancel_resource_request_before_activation(
                        authority_machine,
                        request_id,
                        task_id,
                        resource_id,
                        origin_machine,
                    )
                    .map_err(|error| resource_error(error, Some(task_id), Some(request_id))),
            ),
            StoreMsg::ResourceCancellationReceipt { identity, reply } => {
                let task = identity.task;
                let request = identity.request;
                send_reply(
                    reply,
                    state
                        .resource_cancellation_receipt(&identity)
                        .map_err(|error| resource_error(error, Some(task), Some(request))),
                );
            }
            StoreMsg::CancelResourceRequestWithReceipt {
                authority_machine,
                identity,
                proof,
                reply,
            } => {
                let task = identity.task;
                let request = identity.request;
                send_reply(
                    reply,
                    state
                        .cancel_resource_request_with_receipt(authority_machine, identity, proof)
                        .map_err(|error| resource_error(error, Some(task), Some(request))),
                );
            }
            StoreMsg::OpenReleaseLoanForAuthority {
                authority_machine,
                resource_id,
                expected_state_revision,
                reply,
            } => send_reply(
                reply,
                Ok(state.open_release_loan_for_authority(
                    authority_machine,
                    resource_id,
                    expected_state_revision,
                )),
            ),
            StoreMsg::BindReleaseWatcherForAuthority {
                authority_machine,
                resource_id,
                intent,
                reply,
            } => send_reply(
                reply,
                Ok(state.bind_release_watcher_for_authority(
                    authority_machine,
                    resource_id,
                    intent,
                )),
            ),
            StoreMsg::CaptureReleaseCheckpointBaselineForAuthority {
                authority_machine,
                resource_id,
                action_id,
                expected_state_revision,
                reply,
            } => send_reply(
                reply,
                Ok(state.capture_release_checkpoint_baseline_for_authority(
                    authority_machine,
                    resource_id,
                    action_id,
                    expected_state_revision,
                )),
            ),
            StoreMsg::ReserveReleaseCheckpointStopForAuthority {
                authority_machine,
                resource_id,
                action_id,
                expected_state_revision,
                reply,
            } => send_reply(
                reply,
                Ok(state.reserve_release_checkpoint_stop_for_authority(
                    authority_machine,
                    resource_id,
                    action_id,
                    expected_state_revision,
                )),
            ),
            StoreMsg::RevalidateReleaseCheckpointStopForAuthority {
                authority_machine,
                resource_id,
                action_id,
                expected_state_revision,
                reply,
            } => send_reply(
                reply,
                Ok(state.revalidate_release_checkpoint_stop_for_authority(
                    authority_machine,
                    resource_id,
                    action_id,
                    expected_state_revision,
                )),
            ),
            StoreMsg::CommitReleaseCheckpointCancellationForAuthority {
                authority_machine,
                resource_id,
                action_id,
                expected_state_revision,
                decision,
                reply,
            } => send_reply(
                reply,
                Ok(state.commit_release_checkpoint_cancellation_for_authority(
                    authority_machine,
                    resource_id,
                    action_id,
                    expected_state_revision,
                    &decision,
                )),
            ),
            StoreMsg::AcceptReleaseWatcherForAuthority { input, reply } => send_reply(
                reply,
                Ok(state.accept_release_watcher_for_authority(*input)),
            ),
            StoreMsg::PollReleaseWatcherForAuthority {
                authority_machine,
                request,
                reply,
            } => send_reply(
                reply,
                Ok(state.poll_release_watcher_for_authority(authority_machine, request)),
            ),
            StoreMsg::CompleteReleaseForAuthority {
                authority_machine,
                resource_id,
                action_id,
                expected_state_revision,
                reply,
            } => send_reply(
                reply,
                Ok(state.complete_release_for_authority(
                    authority_machine,
                    resource_id,
                    action_id,
                    expected_state_revision,
                )),
            ),
            StoreMsg::SupervisorNotice { notice_id, reply } => {
                send_reply(reply, Ok(state.supervisor_notice(notice_id)));
            }
            StoreMsg::PendingSupervisorNotices { reply } => {
                send_reply(reply, Ok(state.pending_supervisor_notices()));
            }
            StoreMsg::ReserveSupervisorNoticeAttempt {
                notice_id,
                attempt_id,
                reply,
            } => send_reply(
                reply,
                Ok(state.reserve_supervisor_notice_attempt(notice_id, attempt_id)),
            ),
            StoreMsg::SettleSupervisorNoticeAttempt {
                notice_id,
                attempt_id,
                result,
                reply,
            } => send_reply(
                reply,
                Ok(state.settle_supervisor_notice_attempt(notice_id, attempt_id, result)),
            ),
            StoreMsg::RecoverSendingSupervisorNotices { reply } => {
                send_reply(reply, Ok(state.recover_sending_supervisor_notices()));
            }
            StoreMsg::RetargetSupervisorNotice {
                notice_id,
                expected_assignment_revision,
                destination,
                new_assignment_revision,
                reply,
            } => send_reply(
                reply,
                Ok(state.retarget_supervisor_notice(
                    notice_id,
                    expected_assignment_revision,
                    destination,
                    new_assignment_revision,
                )),
            ),
            StoreMsg::InsertCancellationRequest { request, reply } => {
                send_reply(reply, state.insert_cancellation_request(request));
            }
            StoreMsg::GetCancellationRequest { task, reply } => {
                send_reply(reply, state.cancellation_request(task));
            }
            StoreMsg::PendingCancellationRequests { reply } => {
                send_reply(reply, state.pending_cancellation_requests());
            }
            StoreMsg::AcknowledgeCancellation { receipt, reply } => {
                send_reply(reply, state.acknowledge_cancellation(&receipt));
            }
            StoreMsg::AcknowledgeResourceCancellation { receipt, reply } => {
                send_reply(reply, state.acknowledge_resource_cancellation(&receipt));
            }
            StoreMsg::ReceiveCancellation { request, reply } => {
                send_reply(reply, state.receive_cancellation(request));
            }
            StoreMsg::PendingExecutorCancellations { reply } => {
                send_reply(reply, state.pending_executor_cancellations());
            }
            StoreMsg::FinishExecutorCancellation {
                cancellation,
                state: result,
                reply,
            } => {
                send_reply(
                    reply,
                    state.finish_executor_cancellation(cancellation, result),
                );
            }
            StoreMsg::BeginOutboundMessage { request, reply } => {
                send_reply(reply, state.begin_outbound_message(&request));
            }
            StoreMsg::BindOutboundMessage {
                request,
                destination_machine,
                recipient,
                reply,
            } => send_reply(
                reply,
                state.bind_outbound_message(&request, destination_machine, &recipient),
            ),
            StoreMsg::MessageDelivery { id, reply } => {
                send_reply(reply, state.message_delivery(id));
            }
            StoreMsg::BindMessageAttempt { attempt, reply } => {
                send_reply(reply, state.bind_message_attempt(&attempt));
            }
            StoreMsg::CommitMessageReceipt { receipt, reply } => {
                send_reply(reply, state.commit_message_receipt(&receipt));
            }
            StoreMsg::PendingOutboundTasks { reply } => {
                send_reply(reply, state.pending_outbound_tasks().map_err(event_error))
            }
            StoreMsg::CompactOldEventPayloads { reply } => send_reply(
                reply,
                state.compact_old_event_payloads().map_err(event_error),
            ),
            StoreMsg::FirstPendingOutbound { id, reply } => {
                send_reply(reply, state.first_pending_outbound(id).map_err(event_error))
            }
            StoreMsg::OutboundEventAtOrAfter { id, seq, reply } => send_reply(
                reply,
                state
                    .outbound_event_at_or_after(id, seq)
                    .map_err(event_error),
            ),
            StoreMsg::AppendOutboundEvent {
                id,
                origin,
                execution,
                payload,
                reply,
            } => send_reply(
                reply,
                state
                    .append_outbound_event(id, origin, execution, payload)
                    .map_err(event_error),
            ),
            StoreMsg::AcknowledgeOutbound { id, seq, reply } => send_reply(
                reply,
                state
                    .mark_outbound_acknowledged(id, seq)
                    .map_err(event_error),
            ),
            StoreMsg::OrphanOutboundRoute { id, reply } => {
                send_reply(reply, state.orphan_outbound_route(id).map_err(event_error))
            }
            StoreMsg::OutboundRouteStatus { id, reply } => {
                send_reply(reply, state.outbound_route_status(id).map_err(event_error))
            }
            StoreMsg::PendingInboxTasks { reply } => {
                send_reply(reply, state.pending_inbox_tasks().map_err(event_error))
            }
            StoreMsg::EarliestInbox { id, reply } => send_reply(
                reply,
                state.earliest_unsettled_inbox(id).map_err(event_error),
            ),
            StoreMsg::ReserveInboxAttempt { id, seq, reply } => send_reply(
                reply,
                state.reserve_inbox_attempt(id, seq).map_err(event_error),
            ),
            StoreMsg::SettleInboxAttempt {
                id,
                seq,
                outcome,
                reply,
            } => send_reply(
                reply,
                state
                    .settle_inbox_attempt(id, seq, outcome)
                    .map_err(event_error),
            ),
            StoreMsg::FailedInboxEvents { id, reply } => {
                send_reply(reply, state.failed_inbox_events(id).map_err(event_error))
            }
            StoreMsg::OriginRoute { id, reply } => send_reply(
                reply,
                state.origin_route_by_task(id).map_err(identity_error),
            ),
            StoreMsg::OriginRouteByRequest { request, reply } => send_reply(
                reply,
                state
                    .origin_route_by_request(request)
                    .map_err(identity_error),
            ),
            StoreMsg::UnknownOriginRoutes { reply } => {
                send_reply(reply, state.unknown_origin_routes().map_err(identity_error))
            }
            StoreMsg::UnknownResourceOriginRoutes { reply } => send_reply(
                reply,
                state
                    .unknown_resource_origin_routes()
                    .map_err(identity_error),
            ),
            StoreMsg::InsertOriginRoute { route, reply } => send_reply(
                reply,
                state.insert_origin_route(&route).map_err(identity_error),
            ),
            StoreMsg::ResolveOriginRoute { id, outcome, reply } => send_reply(
                reply,
                state
                    .resolve_origin_route(id, outcome)
                    .map_err(identity_error),
            ),
            StoreMsg::ResolveResourceRoute { receipt, reply } => send_reply(
                reply,
                state
                    .resolve_resource_route(&receipt)
                    .map_err(|error| match error {
                        IdentityError::Conflict => {
                            AppError::ClusterTaskConflict { task: receipt.task }
                        }
                        other => identity_error(other),
                    }),
            ),
            StoreMsg::CancelResourceRouteBeforeLaunch { receipt, reply } => send_reply(
                reply,
                state
                    .cancel_resource_route_before_launch(&receipt)
                    .map_err(|error| match error {
                        IdentityError::Conflict => {
                            AppError::ClusterTaskConflict { task: receipt.task }
                        }
                        other => identity_error(other),
                    }),
            ),
            StoreMsg::AcceptInboundEvent { event, reply } => {
                send_reply(
                    reply,
                    state.accept_inbound_event(&event).map_err(event_error),
                );
            }
            StoreMsg::ExecutorIdentity { id, reply } => {
                send_reply(reply, state.executor_identity(id).map_err(identity_error))
            }
            StoreMsg::RejectExecution { tombstone, reply } => send_reply(
                reply,
                state
                    .reject_execution(&tombstone)
                    .map_err(|error| match error {
                        IdentityError::Conflict => AppError::ClusterTaskConflict {
                            task: tombstone.task,
                        },
                        other => identity_error(other),
                    }),
            ),
            StoreMsg::AbandonExecution {
                id,
                origin,
                execution,
                reply,
            } => send_reply(
                reply,
                state
                    .abandon_before_acceptance(id, origin, execution)
                    .map_err(|error| match error {
                        IdentityError::Conflict => AppError::ClusterTaskConflict { task: id },
                        other => identity_error(other),
                    }),
            ),
            StoreMsg::InsertTask { row, reply } => send_reply(reply, state.insert_task(&row)),
            StoreMsg::InsertLocalTask {
                row,
                spec,
                machine,
                codex,
                reply,
            } => {
                send_reply(reply, state.insert_local_task(&row, &spec, machine, codex));
            }
            StoreMsg::InsertRemoteTask {
                row,
                spec,
                origin,
                execution,
                reply,
            } => {
                send_reply(
                    reply,
                    state.insert_remote_task(&row, &spec, origin, execution),
                );
            }
            StoreMsg::IsEventTask { id, reply } => send_reply(reply, state.is_event_task(id)),
            StoreMsg::GetTask { id, reply } => send_reply(reply, state.get_task(id)),
            StoreMsg::GetProcessGroupExitEvidence { id, reply } => {
                send_reply(reply, state.process_group_exit_evidence(id));
            }
            StoreMsg::ListTasks {
                statuses,
                thread,
                reply,
            } => send_reply(reply, state.list_tasks(&statuses, thread)),
            StoreMsg::TaskPresentations { ids, reply } => {
                send_reply(reply, state.task_presentations(&ids));
            }
            StoreMsg::NonTerminal { reply } => send_reply(reply, state.non_terminal()),
            StoreMsg::InFlightCount { reply } => send_reply(reply, state.in_flight_count()),
            StoreMsg::CasStatus {
                id,
                from,
                to,
                reply,
            } => send_reply(reply, state.cas_status(id, from, to)),
            StoreMsg::CasExit {
                id,
                from,
                reason,
                process_group_exit_evidence,
                reply,
            } => send_reply(
                reply,
                state.cas_exit_with_evidence(id, from, &reason, process_group_exit_evidence),
            ),
            StoreMsg::SetPid { id, pid, reply } => send_reply(reply, state.set_pid(id, pid)),
            StoreMsg::RequestCancel { id, reply } => send_reply(reply, state.request_cancel(id)),
            StoreMsg::ProduceAttentionEvent { id, reply } => {
                send_reply(reply, state.produce_attention_event(id));
            }
            StoreMsg::ReleaseAttention { id, reply } => {
                send_reply(reply, state.release_attention(id));
            }
            StoreMsg::Reports { id, reply } => send_reply(reply, state.reports(id)),
        }
        Ok(())
    }
}
