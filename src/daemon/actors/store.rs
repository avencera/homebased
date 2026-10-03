//! `StoreActor` owns the daemon's single SQLite connection

use std::collections::HashMap;
use std::path::PathBuf;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::cancellation::{
    CancellationReceipt, CancellationRequest, CancellationRequestIdentity, ExecutorCancelState,
};
use crate::daemon::actors::send_reply;
use crate::dependency::{DependencyLookup, HeldCancellation, TaskDependencies};
use crate::domain::{
    ExitReason, ProcessStatus, TaskExitEvidence, TaskId, TaskReport, TaskRow, ThreadId,
};
use crate::error::AppError;
use std::num::NonZeroU64;

use crate::events::{
    DeliveryOutcome, EventAcceptance, EventError, EventRouteStatus, FailedInboxEvent, InboxEvent,
    OutboxEvent, TaskEvent, WaitingInboxEvent,
};
use crate::machine::MachineId;
use crate::message::{
    MessageAttempt, MessageDelivery, MessageId, MessageReceipt, MessageSendRequest,
    OutboundMessageBinding, Recipient,
};
use crate::spec::NormalizedSpec;
use crate::store::{
    CancelResult, HeldCancel, IdentityError, LocalAdmission, Store, TaskPresentation,
    UnlaunchedTask,
};
use crate::submission::{
    CallbackExecutable, DependentRoute, ExecutorIdentity, OriginRoute, RejectionTombstone,
    RequestId, SubmissionState,
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

fn event_error(error: EventError) -> AppError {
    match error {
        EventError::RouteNotFound { task } => AppError::RouteNotFound { task },
        EventError::OwnerConflict { task } => AppError::ClusterTaskConflict { task },
        EventError::ContentConflict { task, seq } => AppError::EventContentConflict { task, seq },
        EventError::Invalid { message } => AppError::Usage { message },
        EventError::Storage(error) => error,
    }
}

/// Messages for daemon SQLite operations
pub(crate) enum StoreMsg {
    /// Retained authority events for sequence gap recovery
    QueueEvents {
        job: crate::queue::JobId,
        reply: RpcReplyPort<Result<Vec<crate::queue::JobEvent>, AppError>>,
    },
    /// Public queue operation, dispatched only through the store API
    QueueInterface {
        machine: MachineId,
        request: Box<crate::store::queue::interface::QueueRequest>,
        env: crate::domain::TaskEnv,
        reply: RpcReplyPort<Result<serde_json::Value, AppError>>,
    },
    /// Origin route lookup
    JobRoute {
        job: crate::queue::JobId,
        reply: RpcReplyPort<Result<Option<crate::queue::delivery::JobRoute>, AppError>>,
    },
    /// Save origin routing before any submit reaches the authority
    InsertJobRoute {
        route: Box<crate::queue::delivery::JobRoute>,
        reply: RpcReplyPort<Result<crate::queue::delivery::JobRoute, AppError>>,
    },
    /// Retain the authority's submission result
    ResolveJobRoute {
        job: crate::queue::JobId,
        submission: crate::queue::delivery::JobSubmission,
        target: Option<crate::queue::Target>,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// First unacknowledged event per authority job
    PendingJobOutbox {
        reply: RpcReplyPort<Result<Vec<crate::queue::delivery::RoutedJobEvent>, AppError>>,
    },
    /// Retain the origin's acknowledgement
    AckJobEvent {
        job: crate::queue::JobId,
        seq: u64,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Deduplicate and accept an authority event
    AcceptJobEvent {
        event: Box<crate::queue::delivery::RoutedJobEvent>,
        reply: RpcReplyPort<Result<EventAcceptance, AppError>>,
    },
    /// First undelivered event per origin job
    PendingJobInbox {
        reply: RpcReplyPort<
            Result<
                Vec<(
                    crate::queue::delivery::JobRoute,
                    crate::queue::delivery::RoutedJobEvent,
                )>,
                AppError,
            >,
        >,
    },
    /// Settle an obsolete blocked notice without callback delivery
    SuppressJobNotice {
        job: crate::queue::JobId,
        seq: u64,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Advance only after callback delivery succeeds
    SettleJobEvent {
        job: crate::queue::JobId,
        seq: u64,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Wake the local queue after task mutations; worker commits also have a recovery scan
    WatchQueue {
        queue: ActorRef<super::queue::QueueMsg>,
    },
    /// Fail executable resolution without exposing a launchable reservation
    QueueLaunchFailed {
        machine: MachineId,
        job: crate::queue::JobId,
        resource: crate::queue::ResourceId,
        task: TaskId,
        message: String,
        now: chrono::DateTime<chrono::Utc>,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Typed queue store operation
    QueueDetect {
        machine: MachineId,
        detected: Vec<crate::queue::gpu::DetectedResource>,
        reply: RpcReplyPort<Result<Vec<crate::store::queue::ResourceRecord>, AppError>>,
    },
    /// Typed queue store operation
    QueueSnapshot {
        machine: MachineId,
        now: chrono::DateTime<chrono::Utc>,
        thresholds: crate::queue::schedule::NoticeThresholds,
        reply: RpcReplyPort<Result<crate::queue::schedule::Snapshot, AppError>>,
    },
    /// Typed queue store operation
    QueueResources {
        machine: MachineId,
        reply: RpcReplyPort<Result<Vec<crate::store::queue::ResourceRecord>, AppError>>,
    },
    /// Typed queue store operation
    QueueJob {
        id: crate::queue::JobId,
        reply: RpcReplyPort<Result<Option<crate::store::queue::JobRecord>, AppError>>,
    },
    /// Typed queue store operation
    QueueCheckpoint {
        task: TaskId,
        reply: RpcReplyPort<Result<Option<crate::queue::checkpoint::Checkpoint>, AppError>>,
    },
    /// Typed queue store operation
    QueueReserve {
        machine: MachineId,
        job: crate::queue::JobId,
        resource: crate::queue::ResourceId,
        task: TaskId,
        binary: PathBuf,
        now: chrono::DateTime<chrono::Utc>,
        reply: RpcReplyPort<Result<crate::store::queue::ReservedRun, AppError>>,
    },
    /// Typed queue store operation
    QueueStop {
        stop: crate::queue::schedule::Preempt,
        now: chrono::DateTime<chrono::Utc>,
        reply: RpcReplyPort<Result<Option<crate::queue::ActiveRun>, AppError>>,
    },
    /// Typed queue store operation
    QueueBeginCleanup {
        resource: crate::queue::ResourceId,
        task: TaskId,
        reply: RpcReplyPort<Result<u32, AppError>>,
    },
    /// Typed queue store operation
    QueueCleanupResult {
        resource: crate::queue::ResourceId,
        task: TaskId,
        attempt: u32,
        result: Result<(), crate::queue::CleanupFailure>,
        reply: RpcReplyPort<Result<Option<crate::queue::RunPhase>, AppError>>,
    },
    /// Typed queue store operation
    QueueAbandon {
        resource: crate::queue::ResourceId,
        task: TaskId,
        reply: RpcReplyPort<Result<bool, AppError>>,
    },
    /// Typed queue store operation
    QueueRecordHead {
        machine: MachineId,
        head: Option<(crate::queue::JobId, chrono::DateTime<chrono::Utc>)>,
        reply: RpcReplyPort<Result<Option<crate::queue::schedule::StoredEpisode>, AppError>>,
    },
    /// Typed queue store operation
    QueueBlocked {
        machine: MachineId,
        episode: crate::queue::schedule::StoredEpisode,
        thresholds: crate::queue::schedule::NoticeThresholds,
        now: chrono::DateTime<chrono::Utc>,
        reply: RpcReplyPort<Result<bool, AppError>>,
    },
    /// Typed queue store operation
    QueueProtected {
        reply: RpcReplyPort<Result<Vec<i32>, AppError>>,
    },
    /// Typed queue store operation
    QueueTaskJob {
        task: TaskId,
        reply: RpcReplyPort<Result<Option<crate::queue::JobId>, AppError>>,
    },
    /// Typed queue store operation
    QueueCheckDue {
        task: TaskId,
        reply: RpcReplyPort<Result<bool, AppError>>,
    },

    /// Migrate historical local rows before supervisor recovery begins
    MigrateLegacyLocal {
        machine: MachineId,
        reply: RpcReplyPort<Result<(), AppError>>,
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
    /// Read origin callbacks that wait for their origin thread
    WaitingInboxEvents {
        id: TaskId,
        reply: RpcReplyPort<Result<Vec<WaitingInboxEvent>, AppError>>,
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
    /// Durably allocate a request and its origin route before network send
    InsertOriginRoute {
        route: Box<OriginRoute>,
        reply: RpcReplyPort<Result<OriginRoute, AppError>>,
    },
    /// Allocate an origin route with the dependencies it was submitted with
    InsertOriginRouteAfter {
        route: Box<OriginRoute>,
        after: TaskDependencies,
        reply: RpcReplyPort<Result<OriginRoute, AppError>>,
    },
    /// Read the dependencies saved with one origin route
    RouteDependencies {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<TaskDependencies>, AppError>>,
    },
    /// Read the retained origin inbox of one task in sequence order
    InboundEvents {
        id: TaskId,
        reply: RpcReplyPort<Result<Vec<InboxEvent>, AppError>>,
    },
    /// Read routes that wait for dependencies or for their launch to be answered
    HeldRoutes {
        reply: RpcReplyPort<Result<Vec<DependentRoute>, AppError>>,
    },
    /// Read local tasks submitted with dependencies whose rows still wait for a worker
    UnstartedDependentTasks {
        reply: RpcReplyPort<Result<Vec<TaskId>, AppError>>,
    },
    /// Read tasks with dependencies that never launched, with their callback delivery
    UnlaunchedTasks {
        reply: RpcReplyPort<Result<Vec<UnlaunchedTask>, AppError>>,
    },
    /// Read one task with dependencies that never launched
    UnlaunchedTask {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<UnlaunchedTask>, AppError>>,
    },
    /// Read the state of each dependency, or `None` for one with no origin route here
    DependencyStates {
        tasks: Vec<TaskId>,
        reply: RpcReplyPort<Result<DependencyLookup, AppError>>,
    },
    /// Cancel a waiting held route and queue its terminal event
    CancelHeldRoute {
        id: TaskId,
        cause: HeldCancellation,
        reply: RpcReplyPort<Result<HeldCancel, AppError>>,
    },
    /// Mark a waiting remote held route as launching before its first send
    BeginHeldLaunch {
        id: TaskId,
        reply: RpcReplyPort<Result<OriginRoute, AppError>>,
    },
    /// Reject a waiting held route whose launch was refused before any task was saved
    RefuseHeldLaunch {
        id: TaskId,
        reason: String,
        reply: RpcReplyPort<Result<OriginRoute, AppError>>,
    },
    /// Persist a definitive executor result
    ResolveOriginRoute {
        id: TaskId,
        outcome: SubmissionState,
        reply: RpcReplyPort<Result<OriginRoute, AppError>>,
    },
    /// Accept a sequenced origin event and return only after commit
    AcceptInboundEvent {
        event: Box<TaskEvent>,
        reply: RpcReplyPort<Result<EventAcceptance, AppError>>,
    },
    /// Read a retained executor identity
    ExecutorIdentity {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<ExecutorIdentity>, AppError>>,
    },
    /// Store a definitive pre-acceptance rejection
    RejectExecution {
        tombstone: RejectionTombstone,
        reply: RpcReplyPort<Result<ExecutorIdentity, AppError>>,
    },
    /// Abandon an identity before acceptance
    AbandonExecution {
        id: TaskId,
        origin: MachineId,
        execution: MachineId,
        reply: RpcReplyPort<Result<ExecutorIdentity, AppError>>,
    },
    /// Insert a new local task with retained ownership and its queued event
    InsertLocalTask {
        row: Box<TaskRow>,
        spec: Box<NormalizedSpec>,
        machine: MachineId,
        admission: LocalAdmission,
        codex: CallbackExecutable,
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
    /// Fetch one task
    GetTask {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<TaskRow>, AppError>>,
    },
    /// List with optional filters
    ListTasks {
        statuses: Vec<ProcessStatus>,
        thread: Option<ThreadId>,
        reply: RpcReplyPort<Result<Vec<TaskRow>, AppError>>,
    },
    /// Read dashboard metadata for a set of task rows
    TaskPresentations {
        ids: Vec<TaskId>,
        reply: RpcReplyPort<Result<HashMap<TaskId, TaskPresentation>, AppError>>,
    },
    /// Queued and running tasks
    NonTerminal {
        reply: RpcReplyPort<Result<Vec<TaskRow>, AppError>>,
    },
    /// Count of queued or running tasks
    InFlightCount {
        reply: RpcReplyPort<Result<usize, AppError>>,
    },
    /// Compare-and-swap process status. Replies with the post-update row, or
    /// `None` when the CAS did not match
    CasStatus {
        id: TaskId,
        from: ProcessStatus,
        to: ProcessStatus,
        worker_thread: Option<crate::domain::ThreadId>,
        reply: RpcReplyPort<Result<Option<TaskRow>, AppError>>,
    },
    /// Compare-and-swap to the status the reason implies, storing the reason
    CasExit {
        id: TaskId,
        from: ProcessStatus,
        reason: ExitReason,
        evidence: TaskExitEvidence,
        reply: RpcReplyPort<Result<Option<TaskRow>, AppError>>,
    },
    /// Compare-and-swap to a terminal status and record a worker thread
    CasExitWithWorkerThread {
        id: TaskId,
        from: ProcessStatus,
        reason: ExitReason,
        evidence: TaskExitEvidence,
        worker_thread: Option<crate::domain::ThreadId>,
        reply: RpcReplyPort<Result<Option<TaskRow>, AppError>>,
    },
    /// Record the worker pid
    SetPid {
        id: TaskId,
        pid: i32,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Count one adopting worker for a running container task, within `limit`
    ClaimContainerAdoption {
        id: TaskId,
        limit: u32,
        reply: RpcReplyPort<Result<bool, AppError>>,
    },
    /// Read the saved container lifecycle of one task
    TaskContainer {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<crate::store::TaskContainerRecord>, AppError>>,
    },
    /// Publish a committed run stop without upgrading a restart to user cancellation
    QueueSignalStop {
        id: TaskId,
        reply: RpcReplyPort<Result<CancelResult, AppError>>,
    },
    /// Request cancel
    RequestCancel {
        id: TaskId,
        reply: RpcReplyPort<Result<CancelResult, AppError>>,
    },
    /// Commit a typed task's inactivity event and claim in one transaction
    ProduceAttentionEvent {
        id: TaskId,
        reply: RpcReplyPort<Result<bool, AppError>>,
    },
    /// Release a legacy direct-send claim after its owner bound passes
    ReleaseAttention {
        id: TaskId,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Reports in seq order
    Reports {
        id: TaskId,
        reply: RpcReplyPort<Result<Vec<TaskReport>, AppError>>,
    },
}

/// Owns `rusqlite::Connection` via `Store`
pub(crate) struct StoreActor;

/// Store connection and the local queue wakeup subscription
pub(crate) struct StoreState {
    store: Store,
    queue: Option<ActorRef<super::queue::QueueMsg>>,
}

impl std::ops::Deref for StoreState {
    type Target = Store;
    fn deref(&self) -> &Store {
        &self.store
    }
}

impl std::ops::DerefMut for StoreState {
    fn deref_mut(&mut self) -> &mut Store {
        &mut self.store
    }
}

impl Actor for StoreActor {
    type Msg = StoreMsg;
    type State = StoreState;
    type Arguments = PathBuf;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        db_path: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(StoreState {
            store: Store::open(&db_path)?,
            queue: None,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let wake_queue = matches!(
            &message,
            StoreMsg::CasStatus { .. }
                | StoreMsg::CasExit { .. }
                | StoreMsg::CasExitWithWorkerThread { .. }
                | StoreMsg::RequestCancel { .. }
                | StoreMsg::QueueInterface { .. }
        );
        match message {
            StoreMsg::QueueEvents { job, reply } => send_reply(reply, state.job_events(job)),
            StoreMsg::QueueInterface {
                machine,
                request,
                env,
                reply,
            } => send_reply(reply, state.queue_request(machine, &request, &env)),
            StoreMsg::JobRoute { job, reply } => send_reply(reply, state.job_route(job)),
            StoreMsg::InsertJobRoute { route, reply } => {
                send_reply(reply, state.insert_job_route(&route))
            }
            StoreMsg::ResolveJobRoute {
                job,
                submission,
                target,
                reply,
            } => send_reply(reply, state.resolve_job_route(job, submission, target)),
            StoreMsg::PendingJobOutbox { reply } => send_reply(reply, state.pending_job_outbox()),
            StoreMsg::AckJobEvent { job, seq, reply } => {
                send_reply(reply, state.acknowledge_job_event(job, seq))
            }
            StoreMsg::AcceptJobEvent { event, reply } => {
                send_reply(reply, state.accept_job_event(&event))
            }
            StoreMsg::PendingJobInbox { reply } => send_reply(reply, state.pending_job_inbox()),
            StoreMsg::SuppressJobNotice { job, seq, reply } => {
                send_reply(reply, state.suppress_job_notice(job, seq))
            }
            StoreMsg::SettleJobEvent { job, seq, reply } => {
                send_reply(reply, state.settle_job_event(job, seq))
            }
            StoreMsg::WatchQueue { queue } => state.queue = Some(queue),
            StoreMsg::QueueLaunchFailed {
                machine,
                job,
                resource,
                task,
                message,
                now,
                reply,
            } => {
                send_reply(
                    reply,
                    state.fail_job_launch(machine, job, resource, task, message, now),
                );
            }
            StoreMsg::QueueDetect {
                machine,
                detected,
                reply,
            } => send_reply(reply, state.ensure_detected_resources(machine, &detected)),
            StoreMsg::QueueSnapshot {
                machine,
                now,
                thresholds,
                reply,
            } => send_reply(reply, state.queue_snapshot(machine, now, thresholds)),
            StoreMsg::QueueResources { machine, reply } => {
                send_reply(reply, state.resources_on(machine))
            }
            StoreMsg::QueueJob { id, reply } => send_reply(reply, state.job(id)),
            StoreMsg::QueueCheckpoint { task, reply } => {
                send_reply(reply, state.run_checkpoint(task))
            }
            StoreMsg::QueueReserve {
                machine,
                job,
                resource,
                task,
                binary,
                now,
                reply,
            } => send_reply(
                reply,
                state.reserve_run(machine, job, resource, task, binary, now),
            ),
            StoreMsg::QueueStop { stop, now, reply } => {
                send_reply(reply, state.commit_preemption(stop, now))
            }
            StoreMsg::QueueBeginCleanup {
                resource,
                task,
                reply,
            } => send_reply(reply, state.begin_cleanup_attempt(resource, task)),
            StoreMsg::QueueCleanupResult {
                resource,
                task,
                attempt,
                result,
                reply,
            } => send_reply(
                reply,
                state.apply_cleanup_result(resource, task, attempt, result),
            ),
            StoreMsg::QueueAbandon {
                resource,
                task,
                reply,
            } => send_reply(reply, state.abandon_launch(resource, task)),
            StoreMsg::QueueRecordHead {
                machine,
                head,
                reply,
            } => send_reply(reply, state.record_blocked_head(machine, head)),
            StoreMsg::QueueBlocked {
                machine,
                episode,
                thresholds,
                now,
                reply,
            } => send_reply(
                reply,
                state.produce_job_blocked(machine, episode, thresholds, now),
            ),
            StoreMsg::QueueProtected { reply } => send_reply(reply, state.cleanup_protected_pids()),
            StoreMsg::QueueTaskJob { task, reply } => send_reply(reply, state.job_run_link(task)),
            StoreMsg::QueueCheckDue { task, reply } => {
                send_reply(reply, state.produce_job_check_due(task))
            }

            StoreMsg::MigrateLegacyLocal { machine, reply } => {
                send_reply(reply, state.migrate_legacy_local(machine));
            }
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
            StoreMsg::WaitingInboxEvents { id, reply } => {
                send_reply(reply, state.waiting_inbox_events(id).map_err(event_error))
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
            StoreMsg::InsertOriginRoute { route, reply } => send_reply(
                reply,
                state.insert_origin_route(&route).map_err(identity_error),
            ),
            StoreMsg::InsertOriginRouteAfter {
                route,
                after,
                reply,
            } => send_reply(
                reply,
                state
                    .insert_origin_route_after(&route, Some(&after))
                    .map_err(identity_error),
            ),
            StoreMsg::RouteDependencies { id, reply } => {
                send_reply(reply, state.route_dependencies(id));
            }
            StoreMsg::InboundEvents { id, reply } => {
                send_reply(reply, state.inbound_events(id).map_err(event_error));
            }
            StoreMsg::HeldRoutes { reply } => send_reply(reply, state.held_routes()),
            StoreMsg::UnstartedDependentTasks { reply } => {
                send_reply(reply, state.unstarted_dependent_tasks());
            }
            StoreMsg::UnlaunchedTasks { reply } => send_reply(reply, state.unlaunched_tasks()),
            StoreMsg::UnlaunchedTask { id, reply } => send_reply(reply, state.unlaunched_task(id)),
            StoreMsg::DependencyStates { tasks, reply } => {
                send_reply(reply, state.dependency_states(&tasks));
            }
            StoreMsg::CancelHeldRoute { id, cause, reply } => {
                send_reply(reply, state.cancel_held_route(id, cause));
            }
            StoreMsg::BeginHeldLaunch { id, reply } => {
                send_reply(reply, state.begin_held_launch(id));
            }
            StoreMsg::RefuseHeldLaunch { id, reason, reply } => {
                send_reply(reply, state.refuse_held_launch(id, &reason));
            }
            StoreMsg::ResolveOriginRoute { id, outcome, reply } => send_reply(
                reply,
                state
                    .resolve_origin_route(id, outcome)
                    .map_err(identity_error),
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
            StoreMsg::InsertLocalTask {
                row,
                spec,
                machine,
                admission,
                codex,
                reply,
            } => {
                send_reply(
                    reply,
                    state.admit_local_task(&row, &spec, machine, &admission, codex),
                );
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
                worker_thread,
                reply,
            } => send_reply(
                reply,
                state.cas_status_with_worker_thread(id, from, to, worker_thread),
            ),
            StoreMsg::CasExit {
                id,
                from,
                reason,
                evidence,
                reply,
            } => send_reply(
                reply,
                state.cas_exit_with_evidence(id, from, &reason, evidence),
            ),
            StoreMsg::CasExitWithWorkerThread {
                id,
                from,
                reason,
                evidence,
                worker_thread,
                reply,
            } => send_reply(
                reply,
                state.cas_exit_with_evidence_and_worker_thread(
                    id,
                    from,
                    &reason,
                    evidence,
                    worker_thread,
                ),
            ),
            StoreMsg::SetPid { id, pid, reply } => send_reply(reply, state.set_pid(id, pid)),
            StoreMsg::ClaimContainerAdoption { id, limit, reply } => {
                send_reply(reply, state.claim_task_container_adoption(id, limit));
            }
            StoreMsg::TaskContainer { id, reply } => send_reply(reply, state.task_container(id)),
            StoreMsg::RequestCancel { id, reply } => send_reply(reply, state.request_cancel(id)),
            StoreMsg::QueueSignalStop { id, reply } => {
                send_reply(reply, state.signal_committed_run_stop(id))
            }
            StoreMsg::ProduceAttentionEvent { id, reply } => {
                send_reply(reply, state.produce_attention_event(id));
            }
            StoreMsg::ReleaseAttention { id, reply } => {
                send_reply(reply, state.release_attention(id));
            }
            StoreMsg::Reports { id, reply } => send_reply(reply, state.reports(id)),
        }
        if wake_queue && let Some(queue) = &state.queue {
            let _ = queue.cast(super::queue::QueueMsg::Reconcile);
        }
        Ok(())
    }
}
