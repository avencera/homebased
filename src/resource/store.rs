//! SQLite operations for authority-local resource state.

use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::{
    AcceptanceSequence, ActionId, AssignmentRevision, CommandSpec, CommandSpecError,
    DeliveryAttemptId, IdleBoundaryDecision, Loan, LoanId, LoanPhase, LoanState, NoticeId,
    ReleaseCheckpointAction, ReleaseCheckpointPhase, ReleaseCheckpointState, ReleaseWatcherIntent,
    Resource, ResourceId, ResourceQueueAttentionReason, ResourceQueueReconcileOutcome,
    ResourceRegistrationReceipt, ResourceRequest, ResourceRequestState, ResourceRevision,
    ResourceTaskOwnershipRisk, ReturnContext, SavedReleaseWatcherIntent, SavedResourceRegistration,
    ServingReleaseProvenance, SupervisorAddress, SupervisorNotice, SupervisorNoticeDelivery,
    SupervisorNoticePayload,
};
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId, TaskState, ThreadId,
};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::command_shape::DirectSegmentCommandShapeError;
use crate::resource::ownership_lock::OwnershipLockProbeError;
use crate::resource::watcher::WatcherError;
use crate::spec::NormalizedSpec;
use crate::store::VerifiedReleaseProof;
use crate::store::{IdentityError, Store};
use crate::submission::{
    ExecutorIdentity, PreAcceptanceRejection, RejectionTombstone, RequestId, normalized_spec_sha256,
};

/// Resource tables installed by the Store migration hook.
pub(crate) const RESOURCE_SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS resources (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    authority_machine TEXT NOT NULL,
    supervisor_machine TEXT NOT NULL,
    supervisor_thread TEXT NOT NULL,
    assignment_revision INTEGER NOT NULL CHECK (assignment_revision >= 0),
    state_revision INTEGER NOT NULL CHECK (state_revision >= 0),
    registered_background_task TEXT
);

CREATE TABLE IF NOT EXISTS trainer_attempt_associations (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES tasks(id),
    resource_id TEXT NOT NULL REFERENCES resources(id),
    authority_machine TEXT NOT NULL,
    association_json TEXT NOT NULL CHECK (
        json_valid(association_json)
        AND COALESCE(json_type(association_json) = 'object', 0)
        AND COALESCE(json_extract(association_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(association_json, '$.authority_machine') = authority_machine, 0)
        AND COALESCE(json_extract(association_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_type(association_json, '$.canonical_runtime_root') = 'text', 0)
        AND COALESCE(json_type(association_json, '$.attempt_binding') = 'object', 0)
        AND COALESCE(json_type(association_json, '$.request_sha256') = 'text', 0)
        AND COALESCE(json_type(association_json, '$.ownership_lock_identity') = 'object', 0)
        AND COALESCE(json_type(association_json, '$.normalized_spec_sha256') = 'text', 0)
    )
);

CREATE INDEX IF NOT EXISTS trainer_attempt_associations_resource_task
    ON trainer_attempt_associations(resource_id, task_id);

CREATE TABLE IF NOT EXISTS resource_requests (
    acceptance_sequence INTEGER PRIMARY KEY AUTOINCREMENT CHECK (acceptance_sequence > 0),
    request_id TEXT NOT NULL UNIQUE,
    task_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    origin_machine TEXT NOT NULL,
    spec_json TEXT NOT NULL CHECK (
        json_valid(spec_json)
        AND COALESCE(json_type(spec_json) = 'object', 0)
        AND COALESCE(json_type(spec_json, '$.api_version') = 'integer', 0)
        AND COALESCE(json_type(spec_json, '$.thread') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.name') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.cwd') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.timeout') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.workload') = 'object', 0)
        AND COALESCE(json_extract(spec_json, '$.workload.type') = 'task', 0)
        AND COALESCE(json_type(spec_json, '$.workload.command') = 'array', 0)
    ),
    state_json TEXT NOT NULL CHECK (
        json_valid(state_json)
        AND COALESCE(json_type(state_json) = 'object', 0)
        AND COALESCE(json_type(state_json, '$.type') = 'text', 0)
        AND COALESCE(json_extract(state_json, '$.type') IN (
            'queued', 'assigned', 'finished', 'cancelled_before_launch', 'rejected'
        ), 0)
    )
);

CREATE INDEX IF NOT EXISTS resource_requests_fifo
    ON resource_requests(resource_id, acceptance_sequence);
CREATE INDEX IF NOT EXISTS resource_requests_queued_fifo
    ON resource_requests(resource_id, acceptance_sequence)
    WHERE json_extract(state_json, '$.type') = 'queued';

CREATE TABLE IF NOT EXISTS resource_request_preventions (
    request_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL,
    origin_machine TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resource_cancellation_receipts (
    cancellation_id TEXT PRIMARY KEY,
    request_json TEXT NOT NULL CHECK (json_valid(request_json)),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_extract(receipt_json, '$.cancellation') = cancellation_id, 0)
    )
);

CREATE TABLE IF NOT EXISTS loans (
    id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    state_json TEXT NOT NULL CHECK (
        json_valid(state_json)
        AND COALESCE(json_type(state_json) = 'object', 0)
        AND COALESCE(json_type(state_json, '$.type') = 'text', 0)
        AND COALESCE(json_extract(state_json, '$.type') IN (
            'active', 'needs_attention', 'closed'
        ), 0)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS loans_one_non_closed_per_resource
    ON loans(resource_id)
    WHERE json_extract(state_json, '$.type') != 'closed';

CREATE TABLE IF NOT EXISTS resource_supervisor_notices (
    id TEXT PRIMARY KEY,
    loan_id TEXT NOT NULL REFERENCES loans(id),
    action_id TEXT NOT NULL UNIQUE,
    notice_json TEXT NOT NULL CHECK (
        json_valid(notice_json)
        AND COALESCE(json_type(notice_json) = 'object', 0)
        AND COALESCE(json_type(notice_json, '$.id') = 'text', 0)
        AND COALESCE(json_type(notice_json, '$.loan_id') = 'text', 0)
        AND COALESCE(json_type(notice_json, '$.action_id') = 'text', 0)
        AND COALESCE(json_type(notice_json, '$.state_revision') = 'integer', 0)
        AND COALESCE(json_type(notice_json, '$.destination') = 'object', 0)
        AND COALESCE(json_type(notice_json, '$.assignment_revision') = 'integer', 0)
        AND COALESCE(json_type(notice_json, '$.payload') = 'object', 0)
        AND COALESCE(json_extract(notice_json, '$.payload.type') IN (
            'release_required', 'return_required', 'attention_required'
        ), 0)
        AND COALESCE(json_type(notice_json, '$.delivery') = 'object', 0)
        AND COALESCE(json_extract(notice_json, '$.delivery.type') IN (
            'pending', 'retry_pending', 'sending', 'delivered', 'failed'
        ), 0)
        AND COALESCE(json_extract(notice_json, '$.id') = id, 0)
        AND COALESCE(json_extract(notice_json, '$.loan_id') = loan_id, 0)
        AND COALESCE(json_extract(notice_json, '$.action_id') = action_id, 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_release_completions (
    action_id TEXT PRIMARY KEY,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_type(receipt_json, '$.action_id') = 'text', 0)
        AND COALESCE(json_extract(receipt_json, '$.action_id') = action_id, 0)
        AND COALESCE(json_type(receipt_json, '$.authority_machine') = 'text', 0)
        AND COALESCE(json_type(receipt_json, '$.resource_id') = 'text', 0)
        AND COALESCE(json_type(receipt_json, '$.expected_state_revision') = 'integer', 0)
        AND COALESCE(json_type(receipt_json, '$.return_context') = 'object', 0)
        AND COALESCE(json_type(receipt_json, '$.result') = 'object', 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_task_completions (
    task_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.request_id') = request_id, 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_release_checkpoint_states (
    action_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    state_json TEXT NOT NULL CHECK (
        json_valid(state_json)
        AND COALESCE(json_type(state_json) = 'object', 0)
        AND COALESCE(json_type(state_json, '$.action') = 'object', 0)
        AND COALESCE(json_type(state_json, '$.phase') = 'object', 0)
        AND COALESCE(json_extract(state_json, '$.action.action_id') = action_id, 0)
        AND COALESCE(json_extract(state_json, '$.action.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(state_json, '$.phase.type') IN (
            'watcher_binding_pending', 'baseline_captured', 'stop_reserved', 'cancellation_committed'
        ), 0)
    )
);

CREATE INDEX IF NOT EXISTS resource_release_checkpoint_states_resource
    ON resource_release_checkpoint_states(resource_id, action_id);

CREATE TABLE IF NOT EXISTS resource_return_decisions (
    action_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    loan_id TEXT NOT NULL REFERENCES loans(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.authority.action_id') = action_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.authority.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.authority.loan_id') = loan_id, 0)
        AND COALESCE(json_type(receipt_json, '$.decision') = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.result.type') IN (
            'closed', 'restore_bound'
        ), 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_restore_closures (
    action_id TEXT PRIMARY KEY REFERENCES resource_return_decisions(action_id),
    task_id TEXT NOT NULL UNIQUE,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.action_id') = action_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.basis.type') IN (
            'confirmed_running', 'foreground_ended', 'supervisor_resolved_end'
        ), 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_action_task_receipts (
    task_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    action_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.request_id') = request_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.authority.action_id') = action_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.authority.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.kind') IN ('release_watcher', 'return'), 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_background_launches (
    request_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.request_id') = request_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_type(receipt_json, '$.contract') = 'object', 0)
    )
);

CREATE INDEX IF NOT EXISTS resource_background_launches_resource
    ON resource_background_launches(resource_id);

CREATE TABLE IF NOT EXISTS resource_idle_openings (
    loan_id TEXT PRIMARY KEY REFERENCES loans(id),
    resource_id TEXT NOT NULL REFERENCES resources(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.loan_id') = loan_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_type(receipt_json, '$.proof') = 'object', 0)
    )
);

CREATE INDEX IF NOT EXISTS resource_supervisor_notices_pending
    ON resource_supervisor_notices(id)
    WHERE json_extract(notice_json, '$.delivery.type') IN ('pending', 'retry_pending');

CREATE TABLE IF NOT EXISTS resource_control_operations (
    operation_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    request_json TEXT NOT NULL CHECK (
        json_valid(request_json)
        AND COALESCE(json_type(request_json) = 'object', 0)
        AND COALESCE(json_extract(request_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_type(request_json, '$.action') = 'object', 0)
    ),
    attempt_id TEXT
);

CREATE TABLE IF NOT EXISTS resource_operator_attestations (
    operation_id TEXT PRIMARY KEY NOT NULL,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    task_id TEXT NOT NULL UNIQUE,
    preceding_loan TEXT,
    preceding_launch TEXT,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.operation_id') = operation_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.task_id') = task_id, 0)
        AND COALESCE(
            json_extract(receipt_json, '$.attestation.confirmation') = 'operator_confirmed_gpu_free',
            0
        )
        AND COALESCE(length(trim(json_extract(receipt_json, '$.attestation.observation'))) > 0, 0)
        AND COALESCE(json_type(receipt_json, '$.evidence') = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.outcome.type') IN (
            'release_resolved_serving', 'release_resolved_return_required',
            'idle_serving', 'idle_boundary'
        ), 0)
    )
);

CREATE INDEX IF NOT EXISTS resource_operator_attestations_resource
    ON resource_operator_attestations(resource_id);

CREATE TABLE IF NOT EXISTS resource_registration_receipts (
    resource_id TEXT PRIMARY KEY NOT NULL REFERENCES resources(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_type(receipt_json, '$.display_name') = 'text', 0)
        AND COALESCE(json_type(receipt_json, '$.authority_machine') = 'text', 0)
        AND COALESCE(json_type(receipt_json, '$.initial_supervisor') = 'object', 0)
    )
);
";

const SUPERVISOR_NOTICE_MAX_ATTEMPTS: u8 = 3;
const INTERRUPTED_DELIVERY_ERROR: &str = "delivery attempt interrupted before settlement";
const RETARGETED_DELIVERY_ERROR: &str = "delivery attempt invalidated by supervisor reassignment";

/// Failure to accept or read authority-local resource data.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResourceStoreError {
    /// An identity already belongs to different immutable content.
    #[error("resource identity conflict")]
    Conflict,
    /// An older watcher intent is readable but lacks evidence required for reuse.
    #[error("saved release watcher identity is legacy and unproven")]
    LegacyWatcherIntentUnproven,
    /// The resource identity was first registered with different content
    #[error("resource {resource:?} is already registered with different content")]
    RegistrationConflict {
        /// Resource whose saved registration differs
        resource: ResourceId,
    },
    /// The resource row predates registration receipts and its initial supervisor is unknown
    #[error("resource {resource:?} has no provable initial registration")]
    LegacyRegistrationUnproven {
        /// Resource whose first registration content was not saved
        resource: ResourceId,
    },
    /// The request was cancelled before it entered the resource queue.
    #[error("resource request was prevented before acceptance")]
    Prevented,
    /// The referenced resource does not exist.
    #[error("resource not found")]
    ResourceNotFound,
    /// The local daemon does not own the resource authority.
    #[error("resource authority mismatch: expected {expected}, found {found}")]
    WrongAuthority {
        /// Fixed authority recorded for this resource.
        expected: MachineId,
        /// Machine identity supplied by the daemon.
        found: MachineId,
    },
    /// The local origin route required for callback delivery is missing.
    #[error("resource callback route for task {task} is missing")]
    OriginRouteNotFound {
        /// Preallocated task identity whose saved origin route is missing.
        task: TaskId,
    },
    /// A prior ordinary execution acceptance won before queue cancellation.
    #[error("task identity {task} was accepted before resource cancellation")]
    ExecutorAlreadyAccepted {
        /// Task identity that was already accepted.
        task: TaskId,
    },
    /// The executor identity table rejected this atomic resource transition.
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// An agent workload cannot enter the finite command queue.
    #[error(transparent)]
    InvalidCommandSpec(#[from] CommandSpecError),
    /// The command falls outside the foreground ownership contract, so its end cannot release the resource.
    #[error("resource command can outlive or hide from its task process group ({risk:?})")]
    UnsupportedCommandOwnership {
        /// First contract violation found in the command or its entry point.
        risk: ResourceTaskOwnershipRisk,
    },
    /// The saved command cannot be prepared on the executor machine.
    #[error("resource command cannot be prepared: {0}")]
    TaskPreparation(#[from] crate::error::AppError),
    /// The accepted resource task is inconsistent with its durable task row.
    #[error("accepted resource task storage is inconsistent: {0}")]
    TaskRow(crate::error::AppError),
    /// The first durable task event is missing or conflicts with its identity.
    #[error(transparent)]
    Event(#[from] crate::events::EventError),
    /// SQLite or stored data failed.
    #[error("resource storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Failure to bind or read durable trainer-attempt evidence on its resource authority.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TrainerAttemptAssociationStoreError {
    /// Resource existence or authority ownership failed.
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// The task does not match the resource's registered background task.
    #[error("task {task_id} is not the registered background task")]
    TaskNotRegistered {
        /// Task supplied for the association.
        task_id: TaskId,
    },
    /// The registered task row is missing on this authority.
    #[error("registered background task {task_id} is missing locally")]
    TaskMissing {
        /// Exact task registered on the resource.
        task_id: TaskId,
    },
    /// The registered task row is not running.
    #[error("registered background task {task_id} is not running (state {state})")]
    TaskNotRunning {
        /// Exact task registered on the resource.
        task_id: TaskId,
        /// Process state saved on this authority.
        state: String,
    },
    /// No accepted executor identity exists for the registered task.
    #[error("accepted executor identity for task {task_id} is missing")]
    IdentityMissing {
        /// Exact task registered on the resource.
        task_id: TaskId,
    },
    /// The executor identity is a rejection, not an accepted task.
    #[error("executor identity for task {task_id} is not accepted")]
    IdentityNotAccepted {
        /// Exact task registered on the resource.
        task_id: TaskId,
    },
    /// The accepted identity does not belong to this task or authority.
    #[error("accepted executor identity for task {task_id} has a different owner")]
    IdentityMismatch {
        /// Exact task registered on the resource.
        task_id: TaskId,
    },
    /// The accepted executor identity is not running.
    #[error("accepted executor identity for task {task_id} is not running")]
    IdentityNotRunning {
        /// Exact task registered on the resource.
        task_id: TaskId,
    },
    /// The accepted identity has no current normalized request spec.
    #[error("accepted executor identity for task {task_id} has no normalized spec")]
    NormalizedSpecMissing {
        /// Exact task registered on the resource.
        task_id: TaskId,
    },
    /// The accepted normalized spec does not match the local task row.
    #[error("accepted normalized spec does not match task row {task_id}")]
    NormalizedSpecMismatch {
        /// Exact task registered on the resource.
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
    /// A non-closed loan does not await release of this exact task.
    #[error("resource {resource_id:?} has an incompatible active loan")]
    ActiveLoanConflict {
        /// Resource whose active loan blocks this association.
        resource_id: ResourceId,
    },
    /// The resource already has a different immutable association.
    #[error(
        "trainer attempt association for resource {resource_id:?} conflicts with saved evidence"
    )]
    Conflict {
        /// Resource whose immutable association conflicts with this request.
        resource_id: ResourceId,
    },
    /// The task is already associated with another resource.
    #[error("trainer task {task_id} is already associated with another resource")]
    TaskAlreadyAssociated {
        /// Task already owned by a trainer association.
        task_id: TaskId,
    },
    /// Persisted association data is malformed or disagrees with its row identity.
    #[error("invalid saved trainer attempt association: {0}")]
    InvalidStoredAssociation(String),
    /// A validated task row or JSON conversion failed.
    #[error(transparent)]
    App(#[from] crate::error::AppError),
    /// Executor identity storage failed.
    #[error(transparent)]
    Identity(#[from] crate::store::IdentityError),
    /// SQLite or stored data failed.
    #[error("trainer attempt association storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Failure to capture or reserve exact checkpoint-stop evidence
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReleaseCheckpointError {
    /// Resource authority or stored resource data failed validation.
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// Saved trainer association could not be read or does not match the action.
    #[error(transparent)]
    TrainerAssociation(#[from] TrainerAttemptAssociationStoreError),
    /// Trainer publication evidence could not be verified.
    #[error(transparent)]
    Watcher(#[from] WatcherError),
    /// The exact task row could not be read.
    #[error(transparent)]
    Task(#[from] AppError),
    /// An old release row has no evidence marker and cannot establish a fresh baseline.
    #[error("legacy release action {action_id:?} on resource {resource_id:?} is unproven")]
    LegacyUnproven {
        /// Resource whose old action has no durable checkpoint evidence.
        resource_id: ResourceId,
        /// Old release action that needs manual attention.
        action_id: ActionId,
    },
    /// The exact action is not in the expected AwaitingRelease phase.
    #[error("release action {action_id:?} is not awaiting checkpoint evidence")]
    NotAwaitingRelease {
        /// Action required by the caller.
        action_id: ActionId,
    },
    /// A watcher identity must be bound before baseline capture.
    #[error("release action {action_id:?} has no saved watcher identity")]
    WatcherIntentMissing {
        /// Action that still needs its preallocated watcher identity.
        action_id: ActionId,
    },
    /// The baseline has not been durably captured for this watcher.
    #[error("release action {action_id:?} has no durable checkpoint baseline")]
    BaselineMissing {
        /// Action whose baseline is absent.
        action_id: ActionId,
    },
    /// No exact stop decision is reserved for the requested release action.
    #[error("release action {action_id:?} has no reserved stop decision")]
    StopDecisionMissing {
        /// Action whose stop decision is absent.
        action_id: ActionId,
    },
    /// The associated trainer task is missing on the authority.
    #[error("observed trainer task {task_id} is missing locally")]
    TaskMissing {
        /// Exact task observed by the release action.
        task_id: TaskId,
    },
    /// The observed trainer task is not running on the authority.
    #[error("observed trainer task {task_id} is not running (state {state:?})")]
    TrainerTaskNotRunning {
        /// Exact task observed by the release action.
        task_id: TaskId,
        /// Current persisted process state.
        state: ProcessStatus,
    },
    /// The accepted trainer command or its task row changed after association.
    #[error("accepted trainer command binding changed for task {task_id}")]
    TrainerCommandBindingChanged {
        /// Exact task observed by the release action.
        task_id: TaskId,
    },
    /// Another cancellation was already recorded for the trainer task.
    #[error("trainer task {task_id} already has a cancellation marker")]
    TrainerCancellationConflict {
        /// Exact task observed by the release action.
        task_id: TaskId,
    },
    /// The accepted watcher identity no longer matches its saved launch intent.
    #[error("release watcher task {task_id} conflicts with its saved launch identity")]
    WatcherIdentityConflict {
        /// Exact watcher task reserved for this release action.
        task_id: TaskId,
    },
    /// The supplied stop decision is not the saved action decision.
    #[error("stop decision for release action {action_id:?} does not match its reservation")]
    StopDecisionMismatch {
        /// Action whose decision did not match.
        action_id: ActionId,
    },
    /// The release action has no saved trainer-attempt association.
    #[error("release action has no trainer association for task {task_id}")]
    TrainerAssociationMissing {
        /// Exact trainer task observed by the release action.
        task_id: TaskId,
    },
    /// The action or its saved checkpoint evidence changed.
    #[error("release checkpoint evidence conflicts with the saved action")]
    Conflict,
    /// The selected publication no longer matches its saved immutable identity.
    #[error("selected checkpoint for release action {action_id:?} changed")]
    SelectedCheckpointChanged {
        /// Action whose saved publication changed.
        action_id: ActionId,
    },
    /// Persisted evidence does not satisfy its typed invariants.
    #[error("invalid saved release checkpoint evidence: {0}")]
    InvalidStoredEvidence(&'static str),
    /// SQLite or stored data failed.
    #[error("release checkpoint evidence storage error: {0}")]
    Storage(#[from] rusqlite::Error),
    /// A typed evidence document could not be serialized.
    #[error("release checkpoint evidence encoding failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Result data retained after the trainer cancellation marker commits
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReleaseCheckpointCancellationResult {
    /// Fixed stop decision that authorized cancellation.
    pub(crate) decision: super::ReleaseCheckpointStopDecision,
    /// Exact trainer task and durable marker time.
    pub(crate) cancellation: super::ReleaseCheckpointCancellation,
}

/// Typed result of committing a reserved exact-task cancellation
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReleaseCheckpointCancellationOutcome {
    /// Decision and task marker committed in one transaction.
    Committed(ReleaseCheckpointCancellationResult),
    /// An exact retry returned the existing committed reservation.
    AlreadyCommitted(ReleaseCheckpointCancellationResult),
    /// The reserved watcher task has not reached its accepted running boundary.
    WatcherNotReady {
        /// Exact watcher task reserved for this release action.
        watcher_task_id: TaskId,
    },
}

/// Exact authority selection and executor context for accepting an assigned request.
#[derive(Debug, Clone)]
pub(crate) struct ResourceTaskAcceptanceInput {
    /// Fixed authority machine recorded on the resource.
    pub(crate) authority_machine: MachineId,
    /// Resource and serving loan that selected this request.
    pub(crate) resource_id: ResourceId,
    /// Caller retry identity saved with the request.
    pub(crate) request_id: RequestId,
    /// Preallocated task identity saved with the request.
    pub(crate) task_id: TaskId,
    /// Authority FIFO position of the selected request.
    pub(crate) acceptance_sequence: AcceptanceSequence,
    /// Serving loan that owns the selected request.
    pub(crate) loan_id: LoanId,
    /// Resource revision observed when the request was selected.
    pub(crate) expected_state_revision: ResourceRevision,
    /// Immutable command spec observed with the selected request.
    pub(crate) command_spec: CommandSpec,
    /// Executor-machine environment used to create the task row.
    pub(crate) executor_env: TaskEnv,
}

/// Exact Serving assignment to reconcile against task-layer ownership evidence
#[derive(Debug, Clone, Copy)]
pub(crate) struct AssignedResourceTaskReconcileInput {
    /// Fixed authority machine recorded on the resource
    pub(crate) authority_machine: MachineId,
    /// Resource whose active loan owns the command
    pub(crate) resource_id: ResourceId,
    /// Serving loan that reserved the command
    pub(crate) loan_id: LoanId,
    /// Exact accepted resource request
    pub(crate) request_id: RequestId,
    /// Preallocated task identity bound to the accepted request
    pub(crate) task_id: TaskId,
    /// Resource revision observed before this reconciliation
    pub(crate) expected_state_revision: ResourceRevision,
}

/// Task-layer observation and proof-gated resource completion result
#[derive(Debug, Clone)]
pub(crate) enum AssignedResourceTaskReconcileOutcome {
    /// No task-layer acceptance exists, so the one-shot launch path may proceed
    NotAccepted,
    /// The exact accepted task is not terminal yet
    Active(AssignedResourceTaskProgress),
    /// The exact terminal task was durably finished and the loan advanced once
    // boxed because the result carries two requests and a loan, while the other
    // variants are a few bytes
    Completed(Box<ResourceTaskCompletionResult>),
    /// The task remains assigned because ownership or identity needs attention
    Attention(AssignedResourceTaskAttention),
}

/// Non-terminal task-layer state of one accepted resource task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AssignedResourceTaskProgress {
    /// The task row exists, but its worker has not reached the running boundary
    Queued,
    /// The task-run worker owns a running child process group
    Running,
}

/// Typed failure that keeps an assigned resource request and its loan reserved
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AssignedResourceTaskAttention {
    /// The request row changed or no longer belongs to this task
    RequestChanged,
    /// The serving loan changed or no longer owns this request
    LoanChanged,
    /// The resource revision changed before the transaction could commit
    StaleRevision,
    /// The loan lacks durable release provenance for its Serving phase
    ServingReleaseUnverified,
    /// The exact task row is missing or disagrees with accepted identity data
    TaskIdentityMismatch,
    /// The task reached Lost without proving ownership exit
    TaskLost,
    /// The task is terminal but the owned process group is not confirmed exited
    ProcessGroupExitUnconfirmed,
    /// No-child evidence came with an outcome that no pre-spawn path records
    InvalidNoChildSpawnEvidence,
    /// The command can outlive the task-run process group
    OwnershipUncertain(ResourceTaskOwnershipRisk),
}

/// Durable result of completing one exact accepted resource task
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ResourceTaskCompletionResult {
    /// The oldest still-queued request now owns this loan
    Assigned {
        /// Request whose exact task result was recorded
        finished_request: ResourceRequest,
        /// Loan that remains reserved for the same return context
        loan: Loan,
        /// Oldest queued request selected for the next serving turn
        next_request: ResourceRequest,
        /// Resource revision committed with the assignment
        state_revision: ResourceRevision,
    },
    /// No queued request remained, so the loan now awaits its return decision
    ReturnRequired {
        /// Request whose exact task result was recorded
        finished_request: ResourceRequest,
        /// Loan that retains the same return context
        loan: Loan,
        /// Durable notice for the exact current supervisor assignment
        notice: SupervisorNotice,
    },
}

/// Proof kind that authorized one resource task to release its loan turn
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResourceTaskReleaseProof {
    /// The task-run worker confirmed that its owned process group exited
    ConfirmedProcessGroupExit,
    /// The task failed before it spawned a child process
    NoChildSpawnedAfterSpawnFailure,
    /// Cancellation won the Queued CAS, so the worker can never reach its spawn
    NoChildSpawnedAfterQueuedCancel,
}

/// Durable idempotency receipt for one exact task, request, and loan transition
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceTaskCompletionReceipt {
    authority_machine: MachineId,
    resource_id: ResourceId,
    loan_id: LoanId,
    request_id: RequestId,
    task_id: TaskId,
    expected_state_revision: ResourceRevision,
    outcome: ExitReason,
    release_proof: ResourceTaskReleaseProof,
    result: ResourceTaskCompletionResult,
}

/// Result of the atomic task-layer acceptance for one assigned resource request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResourceTaskAcceptance {
    /// The task row, accepted identity, and first queued event committed together.
    Inserted {
        /// Preallocated task identity accepted by this authority.
        task: TaskId,
    },
    /// An exact acceptance already exists and retains its current task state.
    Existing {
        /// Preallocated task identity accepted by this authority.
        task: TaskId,
        /// State retained by the task layer.
        state: crate::domain::ProcessStatus,
    },
}

/// Result of accepting the fixed identity for one local release watcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReleaseWatcherAcceptance {
    /// The watcher row, route, identity, event, and loan binding were inserted.
    Inserted { task: TaskId },
    /// An exact saved acceptance was returned without adding durable records.
    Existing {
        task: TaskId,
        state: crate::domain::TaskState,
    },
    /// The supervisor runs elsewhere and must launch through the remote action path.
    UnsupportedRemoteSupervisor {
        /// Machine that owns this resource.
        authority_machine: MachineId,
        /// Supervisor that must receive a future remote launch request.
        supervisor: SupervisorAddress,
    },
}

/// Fixed task and owner data required for one release-watcher acceptance.
pub(crate) struct ReleaseWatcherAcceptanceInput {
    /// Authority machine recorded on the resource.
    pub(crate) authority_machine: MachineId,
    /// Resource whose release action owns this watcher.
    pub(crate) resource_id: ResourceId,
    /// Supervisor address recorded on the resource.
    pub(crate) supervisor: SupervisorAddress,
    /// Fixed action, request, task, and normalized-spec identities.
    pub(crate) intent: ReleaseWatcherIntent,
    /// Queued task row to persist with the accepted command.
    pub(crate) row: crate::domain::TaskRow,
    /// Normalized command used for the accepted task and origin route.
    pub(crate) spec: NormalizedSpec,
    /// Local callback environment and executable.
    pub(crate) callback: crate::submission::CallbackContext,
}

/// Failure to accept the exact release-watcher identity.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReleaseWatcherAcceptanceError {
    /// Release action or fixed identity does not match the saved authority state.
    #[error("release watcher acceptance conflict")]
    Conflict,
    /// Resource authority state rejected the acceptance.
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// Route or executor identity data is invalid or conflicting.
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// The exact release action has no verified checkpoint baseline.
    #[error(transparent)]
    Checkpoint(#[from] ReleaseCheckpointError),
    /// Task, event, or SQLite storage failed.
    #[error(transparent)]
    Storage(#[from] crate::error::AppError),
    /// SQLite transaction or storage operation failed.
    #[error("release watcher acceptance storage error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

/// One authority-owned resource and the loan that must be restored with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResourceSnapshot {
    /// Resource whose authority matches the loading daemon.
    pub(crate) resource: Resource,
    /// The resource's current non-closed loan, if one exists.
    pub(crate) loan: Option<Loan>,
}

/// Accepted task identity retained by an authority-owned assigned request.
#[derive(Debug, Clone)]
pub(crate) struct AcceptedResourceTask {
    /// Exact resource request that owns this task identity.
    pub(crate) request: ResourceRequest,
    /// Serving loan that reserves the request and resource.
    pub(crate) loan_id: LoanId,
    /// Durable executor state, cross-checked against the task row and first event.
    pub(crate) state: ProcessStatus,
}

/// Failure to persist, query, or transition a supervisor notice.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SupervisorNoticeStoreError {
    /// The notice identity or action already belongs to different content.
    #[error("supervisor notice identity conflict")]
    Conflict,
    /// The requested notice does not exist.
    #[error("supervisor notice not found")]
    NotFound,
    /// The notice has already been delivered and cannot be retargeted.
    #[error("delivered supervisor notice cannot be retargeted")]
    AlreadyDelivered,
    /// A delivery attempt is already in flight.
    #[error("supervisor notice delivery attempt is already in flight")]
    AttemptInFlight,
    /// The supplied delivery attempt is not the current in-flight attempt.
    #[error("supervisor notice delivery attempt is stale")]
    StaleAttempt,
    /// The notice has exhausted its bounded delivery attempts.
    #[error("supervisor notice delivery attempt budget is exhausted")]
    AttemptBudgetExhausted,
    /// The notice assignment changed since the caller read it.
    #[error("supervisor notice assignment revision is stale")]
    StaleAssignmentRevision,
    /// A retarget revision must be newer than its expected revision.
    #[error("supervisor notice assignment revision must increase")]
    AssignmentRevisionMustIncrease,
    /// A new notice must begin in the untouched pending state.
    #[error("new supervisor notice must have zero pending attempts")]
    InvalidInitialDelivery,
    /// SQLite or serialized stored data failed.
    #[error("supervisor notice storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Outcome of opening a release loan for one queued resource request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenReleaseLoanResult {
    /// A new release loan and its durable supervisor notice were committed.
    Opened {
        /// Loan created for the resource interruption.
        loan: Loan,
        /// Notice saved in the same transaction as the loan.
        notice: SupervisorNotice,
    },
    /// The existing release action and its saved notice were returned unchanged.
    AlreadyAwaitingRelease {
        /// Existing active loan for this resource.
        loan: Loan,
        /// Saved notice for the existing release action.
        notice: SupervisorNotice,
    },
}

/// Failure to read or open the authority-owned queue state.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResourceQueueReconcileError {
    /// Resource authority or stored resource data failed validation.
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// Opening a release action failed.
    #[error(transparent)]
    Release(#[from] OpenReleaseLoanError),
    /// SQLite failed during queue reconciliation.
    #[error("resource queue reconciliation storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// A release loan could not be opened for the current resource state.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenReleaseLoanError {
    /// Resource authority or stored resource data failed validation.
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// The saved release notice could not be read or inserted.
    #[error(transparent)]
    Notice(#[from] SupervisorNoticeStoreError),
    /// The fresh action checkpoint state could not be persisted atomically.
    #[error(transparent)]
    Checkpoint(#[from] ReleaseCheckpointError),
    /// The caller's expected revision does not match the resource state.
    #[error("stale resource revision: expected {expected:?}, found {actual:?}")]
    StaleRevision {
        /// Resource revision supplied by the caller.
        expected: ResourceRevision,
        /// Current resource revision in SQLite.
        actual: ResourceRevision,
    },
    /// No request remains queued for this resource.
    #[error("resource has no queued request")]
    NoQueuedRequest,
    /// The resource does not have a registered background task.
    #[error("resource has no registered background task")]
    BackgroundTaskNotRegistered,
    /// The registered background task has no local task row.
    #[error("registered background task {task_id} is missing locally")]
    BackgroundTaskMissing {
        /// Exact task registered on the resource.
        task_id: TaskId,
    },
    /// The registered background task is not in the local running state.
    #[error("registered background task {task_id} is not running (state {state})")]
    BackgroundTaskNotRunning {
        /// Exact task registered on the resource.
        task_id: TaskId,
        /// Process state observed in the local task table.
        state: String,
    },
    /// Another active or attention-needed loan already owns this resource.
    #[error("resource already has a non-closed loan {loan:?}")]
    ExistingLoan {
        /// Existing non-closed loan and its typed state.
        loan: Box<Loan>,
    },
    /// A saved AwaitingRelease action has no durable notice.
    #[error("awaiting-release loan {loan_id:?} has no notice for action {action_id:?}")]
    MissingReleaseNotice {
        /// Existing loan that owns the release action.
        loan_id: LoanId,
        /// Stable release action identity.
        action_id: ActionId,
    },
    /// A saved release notice does not match the existing loan action.
    #[error("saved release notice does not match loan {loan_id:?} action {action_id:?}")]
    InvalidReleaseNotice {
        /// Existing loan that owns the release action.
        loan_id: LoanId,
        /// Stable release action identity.
        action_id: ActionId,
    },
    /// The resource state revision cannot be incremented.
    #[error("resource revision {revision:?} cannot be incremented")]
    RevisionExhausted {
        /// Current resource revision.
        revision: ResourceRevision,
    },
    /// SQLite failed during the release transaction.
    #[error("release loan storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Stable result committed when a release action completes.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ReleaseCompletionResult {
    /// The oldest queued request now owns the resource through this loan.
    Assigned {
        /// Loan updated from AwaitingRelease to Serving.
        loan: Loan,
        /// Exact request selected at completion time.
        request: ResourceRequest,
        /// Resource revision committed with the assignment.
        state_revision: ResourceRevision,
    },
    /// The queue was empty and the supervisor now owns the return decision.
    ReturnRequired {
        /// Loan updated from AwaitingRelease to AwaitingReturn.
        loan: Loan,
        /// Durable notice for the exact current supervisor assignment.
        notice: SupervisorNotice,
    },
}

/// Completion evidence for one saved release action.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseCompletionReceipt {
    action_id: ActionId,
    authority_machine: MachineId,
    resource_id: ResourceId,
    expected_state_revision: ResourceRevision,
    return_context: ReturnContext,
    /// Proof basis accepted by this completion, for either result
    ///
    /// Receipts written before the basis was saved decode as unverified
    #[serde(default)]
    release_provenance: ServingReleaseProvenance,
    result: ReleaseCompletionResult,
}

/// A release action could not be completed for the supplied evidence.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CompleteReleaseError {
    /// Resource authority or stored resource data failed validation.
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// A supervisor notice could not be read or inserted.
    #[error(transparent)]
    Notice(#[from] SupervisorNoticeStoreError),
    /// A trainer association could not be read or does not match the release task.
    #[error(transparent)]
    TrainerAssociation(#[from] TrainerAttemptAssociationStoreError),
    /// A persisted task identity could not be read or did not match its authority.
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// The accepted task row could not be read.
    #[error(transparent)]
    TaskStorage(#[from] AppError),
    /// The accepted command does not match its saved trainer association.
    #[error(transparent)]
    CommandShape(#[from] DirectSegmentCommandShapeError),
    /// The result watcher could not verify the completed publication.
    #[error(transparent)]
    Watcher(#[from] WatcherError),
    /// The saved trainer ownership lock could not be verified.
    #[error(transparent)]
    OwnershipLock(#[from] OwnershipLockProbeError),
    /// The action's durable checkpoint or cancellation record could not be verified.
    #[error(transparent)]
    Checkpoint(#[from] ReleaseCheckpointError),
    /// The action has neither an active release phase nor a completion receipt.
    #[error("release action {action_id:?} was not found")]
    ActionNotFound {
        /// Stable release action identity.
        action_id: ActionId,
    },
    /// The action is no longer in the expected AwaitingRelease phase.
    #[error("release action {action_id:?} is not awaiting release on loan {loan_id:?}")]
    NotAwaitingRelease {
        /// Loan that should own the release action.
        loan_id: LoanId,
        /// Stable release action identity.
        action_id: ActionId,
    },
    /// The action's durable release notice is missing or inconsistent.
    #[error("release action {action_id:?} has an invalid durable notice")]
    InvalidReleaseNotice {
        /// Stable release action identity.
        action_id: ActionId,
    },
    /// The caller's expected revision does not match current resource state.
    #[error("stale resource revision: expected {expected:?}, found {actual:?}")]
    StaleRevision {
        /// Resource revision associated with the release notice.
        expected: ResourceRevision,
        /// Current resource revision in SQLite.
        actual: ResourceRevision,
    },
    /// The observed task has no ordinary task row on this authority.
    #[error("observed background task {task_id} is missing locally")]
    BackgroundTaskMissing {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The ordinary task row is not terminal yet.
    #[error("observed background task {task_id} is not terminal (state {state})")]
    BackgroundTaskNotTerminal {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
        /// Process state currently saved in the task table.
        state: String,
    },
    /// A lost task row cannot establish that the resource is free.
    #[error("observed background task {task_id} is lost and needs attention")]
    BackgroundTaskLost {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The release action has no durable association for its exact trainer task.
    #[error("release task {task_id} has no trainer-attempt association")]
    TrainerAssociationMissing {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The saved trainer association no longer matches its task or resource.
    #[error("trainer-attempt association does not match release task {task_id}")]
    TrainerAssociationMismatch {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The accepted identity row is missing for the exact trainer task.
    #[error("accepted trainer identity for task {task_id} is missing")]
    TrainerIdentityMissing {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The accepted trainer identity no longer matches its saved proof.
    #[error("accepted trainer identity for task {task_id} changed")]
    TrainerIdentityChanged {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The task was not cancelled by this action's saved stop decision.
    #[error("trainer task {task_id} has no committed stop decision for this release action")]
    StoppedProofUnavailable {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The exact cancel marker no longer matches the release action record.
    #[error("trainer task {task_id} cancellation marker differs from the saved release action")]
    TrainerCancellationMarkerChanged {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The selected checkpoint publication changed during stopped release verification.
    #[error("trainer task {task_id} selected checkpoint changed during release verification")]
    StoppedCheckpointChanged {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The exact task exited successfully but did not confirm its child process group exited.
    #[error("trainer task {task_id} has no confirmed worker-exit evidence")]
    WorkerExitUnconfirmed {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The exact completed result publication is missing.
    #[error("trainer task {task_id} has no verified completed result publication")]
    CompletedResultMissing {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The verified result publication differs from the result held by the proof.
    #[error("trainer task {task_id} completed result changed during release verification")]
    CompletedResultChanged {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The verified result request differs from the request saved at registration.
    #[error("trainer task {task_id} result request differs from its registered attempt")]
    CompletedResultRequestMismatch {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The exact saved ownership lock is still held by a process.
    #[error("trainer task {task_id} ownership lock is still held")]
    OwnershipLockStillHeld {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The persisted task state or worker-exit evidence changed during proof creation.
    #[error("trainer task {task_id} state changed during release verification")]
    TaskStateChanged {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The persisted task command binding changed during release verification.
    #[error("trainer task {task_id} command binding changed during release verification")]
    TaskCommandChanged {
        /// Exact task recorded in the AwaitingRelease phase.
        task_id: TaskId,
    },
    /// The resource state revision cannot be incremented.
    #[error("resource revision {revision:?} cannot be incremented")]
    RevisionExhausted {
        /// Current resource revision.
        revision: ResourceRevision,
    },
    /// A durable receipt already exists for different input identity or evidence.
    #[error("release action {action_id:?} was retried with conflicting input")]
    ConflictingRetry {
        /// Stable release action identity.
        action_id: ActionId,
    },
    /// A concurrent or inconsistent queue change prevented assignment.
    #[error("queued request {request_id:?} changed during release completion")]
    RequestChanged {
        /// Request selected at the head of the authority FIFO.
        request_id: RequestId,
    },
    /// A concurrent or inconsistent loan change prevented completion.
    #[error("loan {loan_id:?} changed during release completion")]
    LoanChanged {
        /// Loan that owns the release action.
        loan_id: LoanId,
    },
    /// The resource revision changed while completing the action.
    #[error("resource state changed during release completion")]
    ResourceChanged,
    /// SQLite or serialized stored data failed.
    #[error("release completion storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

const RESOURCE_COLUMNS: &str = "id, display_name, authority_machine, supervisor_machine,
    supervisor_thread, assignment_revision, state_revision, registered_background_task";

const RESOURCE_SNAPSHOT_COLUMNS: &str = "r.id, r.display_name, r.authority_machine,
    r.supervisor_machine, r.supervisor_thread, r.assignment_revision, r.state_revision,
    r.registered_background_task, l.id, l.resource_id, l.state_json";

const REQUEST_COLUMNS: &str = "acceptance_sequence, request_id, task_id, resource_id,
    origin_machine, spec_json, state_json";

const PREVENTION_COLUMNS: &str = "request_id, task_id, resource_id, origin_machine";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestIdentity {
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
}

/// Result of cancelling a resource request before task activation.
#[derive(Debug, Clone)]
pub enum QueueCancellationResult {
    /// The exact request identity is retained to prevent delayed acceptance.
    PreventedBeforeAcceptance,
    /// The saved request state after cancellation or a terminal-state retry.
    Request(Box<ResourceRequest>),
}

/// Install the resource schema on an explicitly supplied connection.
pub(crate) fn install_schema(conn: &mut Connection) -> Result<(), ResourceStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(RESOURCE_SCHEMA)?;
    tx.commit()?;
    Ok(())
}

/// Insert a notice without committing the caller's loan transaction.
pub(crate) fn insert_supervisor_notice_in_transaction(
    tx: &Transaction<'_>,
    notice: &SupervisorNotice,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    let existing = {
        let mut statement = tx.prepare(
            "SELECT id, loan_id, action_id, notice_json
             FROM resource_supervisor_notices
             WHERE id = ?1 OR action_id = ?2",
        )?;
        statement
            .query_map(
                params![
                    notice.id.as_uuid().to_string(),
                    notice.action_id.as_uuid().to_string()
                ],
                decode_supervisor_notice_record,
            )?
            .collect::<Result<Vec<_>, _>>()?
    };

    if let [(saved, _)] = existing.as_slice()
        && same_supervisor_notice_content(saved, notice)
    {
        return Ok(saved.clone());
    }
    if !existing.is_empty() {
        return Err(SupervisorNoticeStoreError::Conflict);
    }

    if notice.delivery != (SupervisorNoticeDelivery::Pending { attempts: 0 }) {
        return Err(SupervisorNoticeStoreError::InvalidInitialDelivery);
    }

    tx.execute(
        "INSERT INTO resource_supervisor_notices (id, loan_id, action_id, notice_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            notice.id.as_uuid().to_string(),
            notice.loan_id.as_uuid().to_string(),
            notice.action_id.as_uuid().to_string(),
            encode_supervisor_notice(notice)?,
        ],
    )?;
    Ok(notice.clone())
}

/// Read one notice by its stable deduplication identity.
pub(crate) fn supervisor_notice(
    conn: &Connection,
    notice_id: NoticeId,
) -> Result<Option<SupervisorNotice>, SupervisorNoticeStoreError> {
    Ok(select_supervisor_notice_record(conn, notice_id)?.map(|(notice, _)| notice))
}

/// Read notices that can receive another delivery attempt in stable ID order.
pub(crate) fn pending_supervisor_notices(
    conn: &Connection,
) -> Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError> {
    let mut statement = conn.prepare(
        "SELECT id, loan_id, action_id, notice_json
         FROM resource_supervisor_notices
         WHERE json_extract(notice_json, '$.delivery.type') IN ('pending', 'retry_pending')
         ORDER BY id ASC",
    )?;
    Ok(statement
        .query_map([], decode_supervisor_notice_record)?
        .map(|record| record.map(|(notice, _)| notice))
        .collect::<Result<Vec<_>, _>>()?)
}

pub(crate) fn release_checkpoint_state_for_action(
    conn: &Connection,
    resource_id: ResourceId,
    action_id: ActionId,
) -> Result<Option<(ReleaseCheckpointState, String)>, ReleaseCheckpointError> {
    let saved = conn
        .query_row(
            "SELECT resource_id, state_json FROM resource_release_checkpoint_states
             WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((saved_resource, state_json)) = saved else {
        return Ok(None);
    };
    let saved_resource = uuid::Uuid::parse_str(&saved_resource)
        .map(ResourceId::from_uuid)
        .map_err(|_| ReleaseCheckpointError::InvalidStoredEvidence("resource id is invalid"))?;
    let state: ReleaseCheckpointState = serde_json::from_str(&state_json)?;
    state
        .validate()
        .map_err(ReleaseCheckpointError::InvalidStoredEvidence)?;
    if saved_resource != resource_id
        || state.action.resource_id != resource_id
        || state.action.action_id != action_id
    {
        return Err(ReleaseCheckpointError::InvalidStoredEvidence(
            "row identities do not match the typed state",
        ));
    }

    Ok(Some((state, state_json)))
}

fn insert_release_checkpoint_state(
    tx: &Transaction<'_>,
    state: &ReleaseCheckpointState,
) -> Result<(), ReleaseCheckpointError> {
    state
        .validate()
        .map_err(ReleaseCheckpointError::InvalidStoredEvidence)?;
    tx.execute(
        "INSERT INTO resource_release_checkpoint_states (action_id, resource_id, state_json)
         VALUES (?1, ?2, ?3)",
        params![
            state.action.action_id.as_uuid().to_string(),
            state.action.resource_id.as_uuid().to_string(),
            serde_json::to_string(state)?,
        ],
    )?;
    Ok(())
}

pub(crate) fn update_release_checkpoint_state(
    tx: &Transaction<'_>,
    previous_json: &str,
    state: &ReleaseCheckpointState,
) -> Result<(), ReleaseCheckpointError> {
    state
        .validate()
        .map_err(ReleaseCheckpointError::InvalidStoredEvidence)?;
    let changed = tx.execute(
        "UPDATE resource_release_checkpoint_states SET state_json = ?1
         WHERE action_id = ?2 AND resource_id = ?3 AND state_json = ?4",
        params![
            serde_json::to_string(state)?,
            state.action.action_id.as_uuid().to_string(),
            state.action.resource_id.as_uuid().to_string(),
            previous_json,
        ],
    )?;
    if changed != 1 {
        return Err(ReleaseCheckpointError::Conflict);
    }

    Ok(())
}

/// Reserve one bounded attempt, or return an existing reservation for an identical retry.
pub(crate) fn reserve_supervisor_notice_attempt(
    conn: &mut Connection,
    notice_id: NoticeId,
    attempt_id: DeliveryAttemptId,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (mut notice, old_json) = select_supervisor_notice_record(&tx, notice_id)?
        .ok_or(SupervisorNoticeStoreError::NotFound)?;

    let attempts = match notice.delivery {
        SupervisorNoticeDelivery::Pending { attempts }
        | SupervisorNoticeDelivery::RetryPending { attempts, .. } => attempts,
        SupervisorNoticeDelivery::Sending {
            attempt_id: reserved,
            ..
        } if reserved == attempt_id => {
            tx.commit()?;
            return Ok(notice);
        }
        SupervisorNoticeDelivery::Sending { .. } => {
            return Err(SupervisorNoticeStoreError::AttemptInFlight);
        }
        SupervisorNoticeDelivery::Delivered { .. } => {
            return Err(SupervisorNoticeStoreError::AlreadyDelivered);
        }
        SupervisorNoticeDelivery::Failed { .. } => {
            return Err(SupervisorNoticeStoreError::AttemptBudgetExhausted);
        }
    };

    if attempts >= SUPERVISOR_NOTICE_MAX_ATTEMPTS {
        return Err(SupervisorNoticeStoreError::AttemptBudgetExhausted);
    }

    let attempt = attempts + 1;
    notice.delivery = SupervisorNoticeDelivery::Sending {
        attempt_id,
        attempt,
    };
    update_supervisor_notice_cas(&tx, &notice, &old_json)?;
    tx.commit()?;
    Ok(notice)
}

/// Settle only the exact attempt that is still in flight.
pub(crate) fn settle_supervisor_notice_attempt(
    conn: &mut Connection,
    notice_id: NoticeId,
    attempt_id: DeliveryAttemptId,
    result: Result<(), String>,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (mut notice, old_json) = select_supervisor_notice_record(&tx, notice_id)?
        .ok_or(SupervisorNoticeStoreError::NotFound)?;
    let attempt = match notice.delivery {
        SupervisorNoticeDelivery::Sending {
            attempt_id: active_attempt,
            attempt,
        } if active_attempt == attempt_id => attempt,
        _ => return Err(SupervisorNoticeStoreError::StaleAttempt),
    };

    notice.delivery = match result {
        Ok(()) => SupervisorNoticeDelivery::Delivered { attempts: attempt },
        Err(last_error) if attempt >= SUPERVISOR_NOTICE_MAX_ATTEMPTS => {
            SupervisorNoticeDelivery::Failed {
                attempts: attempt,
                last_error,
            }
        }
        Err(last_error) => SupervisorNoticeDelivery::RetryPending {
            attempts: attempt,
            last_error,
        },
    };

    update_supervisor_notice_cas(&tx, &notice, &old_json)?;
    tx.commit()?;
    Ok(notice)
}

/// Recover in-flight notices after startup without treating them as delivered.
pub(crate) fn recover_sending_supervisor_notices(
    conn: &mut Connection,
) -> Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let sending = {
        let mut statement = tx.prepare(
            "SELECT id, loan_id, action_id, notice_json
             FROM resource_supervisor_notices
             WHERE json_extract(notice_json, '$.delivery.type') = 'sending'
             ORDER BY id ASC",
        )?;
        statement
            .query_map([], decode_supervisor_notice_record)?
            .collect::<Result<Vec<_>, _>>()?
    };

    let mut recovered = Vec::with_capacity(sending.len());
    for (mut notice, old_json) in sending {
        let SupervisorNoticeDelivery::Sending { attempt, .. } = notice.delivery else {
            unreachable!("query selected only sending notices")
        };
        notice.delivery = if attempt >= SUPERVISOR_NOTICE_MAX_ATTEMPTS {
            SupervisorNoticeDelivery::Failed {
                attempts: attempt,
                last_error: INTERRUPTED_DELIVERY_ERROR.into(),
            }
        } else {
            SupervisorNoticeDelivery::RetryPending {
                attempts: attempt,
                last_error: INTERRUPTED_DELIVERY_ERROR.into(),
            }
        };
        update_supervisor_notice_cas(&tx, &notice, &old_json)?;
        recovered.push(notice);
    }

    tx.commit()?;
    Ok(recovered)
}

/// Retarget an undelivered notice with an exact assignment-revision compare-and-set.
pub(crate) fn retarget_supervisor_notice(
    conn: &mut Connection,
    notice_id: NoticeId,
    expected_assignment_revision: AssignmentRevision,
    destination: SupervisorAddress,
    new_assignment_revision: AssignmentRevision,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let notice = retarget_supervisor_notice_in_transaction(
        &tx,
        notice_id,
        expected_assignment_revision,
        destination,
        new_assignment_revision,
    )?;
    tx.commit()?;
    Ok(notice)
}

/// Retarget an undelivered notice inside the caller's transaction.
pub(crate) fn retarget_supervisor_notice_in_transaction(
    tx: &Transaction<'_>,
    notice_id: NoticeId,
    expected_assignment_revision: AssignmentRevision,
    destination: SupervisorAddress,
    new_assignment_revision: AssignmentRevision,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    if new_assignment_revision.get() <= expected_assignment_revision.get() {
        return Err(SupervisorNoticeStoreError::AssignmentRevisionMustIncrease);
    }

    let (mut notice, old_json) = select_supervisor_notice_record(tx, notice_id)?
        .ok_or(SupervisorNoticeStoreError::NotFound)?;
    if notice.assignment_revision != expected_assignment_revision {
        return Err(SupervisorNoticeStoreError::StaleAssignmentRevision);
    }
    if matches!(notice.delivery, SupervisorNoticeDelivery::Delivered { .. }) {
        return Err(SupervisorNoticeStoreError::AlreadyDelivered);
    }

    if let SupervisorNoticeDelivery::Sending { attempt, .. } = notice.delivery {
        notice.delivery = if attempt >= SUPERVISOR_NOTICE_MAX_ATTEMPTS {
            SupervisorNoticeDelivery::Failed {
                attempts: attempt,
                last_error: RETARGETED_DELIVERY_ERROR.into(),
            }
        } else {
            SupervisorNoticeDelivery::RetryPending {
                attempts: attempt,
                last_error: RETARGETED_DELIVERY_ERROR.into(),
            }
        };
    }
    notice.destination = destination;
    notice.assignment_revision = new_assignment_revision;

    update_supervisor_notice_cas(tx, &notice, &old_json)?;
    Ok(notice)
}

fn same_supervisor_notice_content(left: &SupervisorNotice, right: &SupervisorNotice) -> bool {
    left.id == right.id
        && left.loan_id == right.loan_id
        && left.action_id == right.action_id
        && left.state_revision == right.state_revision
        && left.destination == right.destination
        && left.assignment_revision == right.assignment_revision
        && left.payload == right.payload
}

pub(crate) fn select_supervisor_notice_record(
    conn: &Connection,
    notice_id: NoticeId,
) -> Result<Option<(SupervisorNotice, String)>, rusqlite::Error> {
    conn.query_row(
        "SELECT id, loan_id, action_id, notice_json
         FROM resource_supervisor_notices WHERE id = ?1",
        [notice_id.as_uuid().to_string()],
        decode_supervisor_notice_record,
    )
    .optional()
}

pub(crate) fn select_supervisor_notice_record_by_action(
    conn: &Connection,
    action_id: ActionId,
) -> Result<Option<(SupervisorNotice, String)>, rusqlite::Error> {
    conn.query_row(
        "SELECT id, loan_id, action_id, notice_json
         FROM resource_supervisor_notices WHERE action_id = ?1",
        [action_id.as_uuid().to_string()],
        decode_supervisor_notice_record,
    )
    .optional()
}

pub(crate) fn decode_supervisor_notice_record(
    row: &Row<'_>,
) -> rusqlite::Result<(SupervisorNotice, String)> {
    let id = NoticeId(uuid_column(row, 0)?);
    let loan_id = LoanId(uuid_column(row, 1)?);
    let action_id = ActionId(uuid_column(row, 2)?);
    let notice_json: String = row.get(3)?;
    let notice: SupervisorNotice = decode_json(&notice_json, 3)?;
    if notice.id != id || notice.loan_id != loan_id || notice.action_id != action_id {
        return Err(stored_value_error(
            3,
            Type::Text,
            std::io::Error::other("notice identity columns do not match the typed notice"),
        ));
    }

    Ok((notice, notice_json))
}

pub(crate) fn update_supervisor_notice_cas(
    tx: &Transaction<'_>,
    notice: &SupervisorNotice,
    expected_json: &str,
) -> Result<(), SupervisorNoticeStoreError> {
    let updated = tx.execute(
        "UPDATE resource_supervisor_notices SET notice_json = ?1
         WHERE id = ?2 AND action_id = ?3 AND notice_json = ?4",
        params![
            encode_supervisor_notice(notice)?,
            notice.id.as_uuid().to_string(),
            notice.action_id.as_uuid().to_string(),
            expected_json,
        ],
    )?;
    if updated != 1 {
        return Err(SupervisorNoticeStoreError::StaleAttempt);
    }

    Ok(())
}

/// Open one release action and persist its supervisor notice atomically.
pub(crate) fn open_release_loan_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    expected_state_revision: ResourceRevision,
) -> Result<OpenReleaseLoanResult, OpenReleaseLoanError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    open_release_loan_in_transaction(tx, authority_machine, resource_id, expected_state_revision)
}

fn open_release_loan_in_transaction(
    tx: Transaction<'_>,
    authority_machine: MachineId,
    resource_id: ResourceId,
    expected_state_revision: ResourceRevision,
) -> Result<OpenReleaseLoanResult, OpenReleaseLoanError> {
    let resource =
        select_resource(&tx, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_authority(resource.authority_machine(), authority_machine)?;

    // check issued actions before queue readiness because cancellation cannot revoke them
    if let Some(loan) = select_non_closed_loan(&tx, resource_id)? {
        let LoanState::Active {
            phase:
                LoanPhase::AwaitingRelease {
                    action_id,
                    observed_background_task,
                    ..
                },
        } = &loan.state
        else {
            return Err(OpenReleaseLoanError::ExistingLoan {
                loan: Box::new(loan),
            });
        };

        let Some((notice, _)) = select_supervisor_notice_record_by_action(&tx, *action_id)? else {
            return Err(OpenReleaseLoanError::MissingReleaseNotice {
                loan_id: loan.id,
                action_id: *action_id,
            });
        };
        if notice.loan_id != loan.id
            || notice.payload
                != (SupervisorNoticePayload::ReleaseRequired {
                    task_id: *observed_background_task,
                })
        {
            return Err(OpenReleaseLoanError::InvalidReleaseNotice {
                loan_id: loan.id,
                action_id: *action_id,
            });
        }

        // retries retain the original expected revision after the opening increments it
        let expected_notice_revision = expected_state_revision
            .get()
            .checked_add(1)
            .map(ResourceRevision::new);
        if expected_notice_revision != Some(notice.state_revision)
            || resource.state_revision != notice.state_revision
        {
            return Err(OpenReleaseLoanError::StaleRevision {
                expected: expected_state_revision,
                actual: resource.state_revision,
            });
        }

        tx.commit()?;
        return Ok(OpenReleaseLoanResult::AlreadyAwaitingRelease { loan, notice });
    }

    if resource.state_revision != expected_state_revision {
        return Err(OpenReleaseLoanError::StaleRevision {
            expected: expected_state_revision,
            actual: resource.state_revision,
        });
    }

    if oldest_queued_request_inner(&tx, Some(authority_machine), resource_id)?.is_none() {
        return Err(OpenReleaseLoanError::NoQueuedRequest);
    }

    let task_id = resource
        .registered_background_task
        .ok_or(OpenReleaseLoanError::BackgroundTaskNotRegistered)?;
    match observe_registered_background_on(&tx, resource_id, task_id)? {
        RegisteredBackgroundObservation::Missing => {
            return Err(OpenReleaseLoanError::BackgroundTaskMissing { task_id });
        }
        RegisteredBackgroundObservation::Running
        | RegisteredBackgroundObservation::EndedCandidate => {}
        RegisteredBackgroundObservation::NotReleasable { state } => {
            return Err(OpenReleaseLoanError::BackgroundTaskNotRunning { task_id, state });
        }
    }

    let next_revision_value = expected_state_revision.get().checked_add(1).ok_or(
        OpenReleaseLoanError::RevisionExhausted {
            revision: expected_state_revision,
        },
    )?;
    let next_revision = ResourceRevision::new(next_revision_value);
    let expected_revision_sql = sqlite_integer(expected_state_revision.get())?;
    let next_revision_sql = sqlite_integer(next_revision_value)?;
    let action_id = ActionId::new();
    let loan = Loan {
        id: LoanId::new(),
        resource_id,
        state: LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task: task_id,
                watcher_intent: None,
            },
        },
    };
    let state_json = encode_json(&loan.state)?;
    tx.execute(
        "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
        params![
            loan.id.as_uuid().to_string(),
            resource_id.as_uuid().to_string(),
            state_json,
        ],
    )?;

    let updated = tx.execute(
        "UPDATE resources SET state_revision = ?1
         WHERE id = ?2 AND authority_machine = ?3 AND state_revision = ?4",
        params![
            next_revision_sql,
            resource_id.as_uuid().to_string(),
            authority_machine.as_uuid().to_string(),
            expected_revision_sql,
        ],
    )?;
    if updated != 1 {
        let actual = select_resource(&tx, resource_id)?
            .map_or(resource.state_revision, |saved| saved.state_revision);
        return Err(OpenReleaseLoanError::StaleRevision {
            expected: expected_state_revision,
            actual,
        });
    }

    let notice = SupervisorNotice {
        id: NoticeId::new(),
        loan_id: loan.id,
        action_id,
        state_revision: next_revision,
        destination: resource.supervisor,
        assignment_revision: resource.assignment_revision,
        payload: SupervisorNoticePayload::ReleaseRequired { task_id },
        delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
    };
    insert_supervisor_notice_in_transaction(&tx, &notice)?;
    insert_release_checkpoint_state(
        &tx,
        &ReleaseCheckpointState {
            action: ReleaseCheckpointAction {
                resource_id,
                action_id,
                state_revision: next_revision,
                observed_background_task: task_id,
            },
            phase: ReleaseCheckpointPhase::WatcherBindingPending,
        },
    )?;
    tx.commit()?;

    Ok(OpenReleaseLoanResult::Opened { loan, notice })
}

/// Reconcile the oldest queued request using only authority-owned state.
pub(crate) fn reconcile_resource_queue_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
) -> Result<ResourceQueueReconcileOutcome, ResourceQueueReconcileError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let resource =
        select_resource(&tx, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_authority(resource.authority_machine(), authority_machine)?;

    if let Some(loan) = select_non_closed_loan(&tx, resource_id)? {
        tx.commit()?;
        return Ok(ResourceQueueReconcileOutcome::LoanAlreadyActive { loan });
    }

    // a first background launch with a confirmed start becomes the registered
    // task before any queue decision reads the registration
    let resource = match crate::store::promote_started_background_launch_on(&tx, &resource)? {
        Some(promoted) => promoted,
        None => resource,
    };

    let Some(request) = oldest_queued_request_inner(&tx, Some(authority_machine), resource_id)?
    else {
        tx.commit()?;
        return Ok(ResourceQueueReconcileOutcome::NoQueuedRequest);
    };

    if let Some(task_id) = crate::store::pending_background_launch_on(&tx, &resource)? {
        tx.commit()?;
        return Ok(ResourceQueueReconcileOutcome::AttentionRequired {
            request,
            reason: ResourceQueueAttentionReason::BackgroundLaunchPending { task_id },
        });
    }

    let Some(task_id) = resource.registered_background_task else {
        let outcome = match crate::store::idle_boundary_decision_on(&tx, &resource)? {
            IdleBoundaryDecision::Proven(proof) => {
                let (loan, request) = crate::store::open_idle_serving_loan_on(
                    &tx,
                    authority_machine,
                    &resource,
                    request,
                    proof.clone(),
                )?;
                ResourceQueueReconcileOutcome::IdleServing {
                    loan,
                    request,
                    proof,
                }
            }
            IdleBoundaryDecision::Unproven(gap) => {
                ResourceQueueReconcileOutcome::AttentionRequired {
                    request,
                    reason: ResourceQueueAttentionReason::IdleNotProven { gap },
                }
            }
        };
        tx.commit()?;
        return Ok(outcome);
    };

    match observe_registered_background_on(&tx, resource_id, task_id)? {
        RegisteredBackgroundObservation::Missing => {
            tx.commit()?;
            Ok(ResourceQueueReconcileOutcome::AttentionRequired {
                request,
                reason: ResourceQueueAttentionReason::BackgroundTaskMissing { task_id },
            })
        }
        // an ended trainer opens the same release action as a running one, so
        // only the authority release proof can serve the queue from it
        RegisteredBackgroundObservation::Running
        | RegisteredBackgroundObservation::EndedCandidate => {
            let outcome = open_release_loan_in_transaction(
                tx,
                authority_machine,
                resource_id,
                resource.state_revision,
            )?;
            match outcome {
                OpenReleaseLoanResult::Opened { loan, notice }
                | OpenReleaseLoanResult::AlreadyAwaitingRelease { loan, notice } => {
                    Ok(ResourceQueueReconcileOutcome::ReleaseRequired { loan, notice })
                }
            }
        }
        RegisteredBackgroundObservation::NotReleasable { state } => {
            tx.commit()?;
            Ok(ResourceQueueReconcileOutcome::AttentionRequired {
                request,
                reason: ResourceQueueAttentionReason::BackgroundTaskNotRunning { task_id, state },
            })
        }
    }
}

/// Authority view of the registered background task when queued work needs the resource
#[derive(Debug, Clone, PartialEq, Eq)]
enum RegisteredBackgroundObservation {
    /// The registered task has no authority task row
    Missing,
    /// The task is running, so a watcher must stop it at a checkpoint
    Running,
    /// The task succeeded, failed, or was cancelled, its process-group exit is
    /// confirmed, and a trainer attempt association is saved
    ///
    /// These facts do not release the resource. They only let the release action
    /// open, and the authority release proof must still verify the released
    /// ownership lock, and any result or stop evidence, before it serves the queue
    EndedCandidate,
    /// The task ended without the facts a release proof needs
    ///
    /// A lost or unconfirmed end, or a missing association, can leave trainer
    /// work alive, so the queue stays blocked for an owner
    NotReleasable {
        /// Durable task status observed by the authority
        state: String,
    },
}

fn observe_registered_background_on(
    conn: &Connection,
    resource_id: ResourceId,
    task_id: TaskId,
) -> Result<RegisteredBackgroundObservation, ResourceStoreError> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM tasks WHERE id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(status) = status else {
        return Ok(RegisteredBackgroundObservation::Missing);
    };
    match ProcessStatus::from_storage(&status).ok() {
        Some(ProcessStatus::Running) => return Ok(RegisteredBackgroundObservation::Running),
        // a lost task never has a confirmed exit, so it is not a candidate
        Some(ProcessStatus::Succeeded | ProcessStatus::Failed | ProcessStatus::Cancelled)
            if ended_release_candidate(conn, resource_id, task_id)? =>
        {
            return Ok(RegisteredBackgroundObservation::EndedCandidate);
        }
        _ => {}
    }

    Ok(RegisteredBackgroundObservation::NotReleasable { state: status })
}

/// Check the saved facts that an ended-trainer release proof needs before it can run
fn ended_release_candidate(
    conn: &Connection,
    resource_id: ResourceId,
    task_id: TaskId,
) -> Result<bool, ResourceStoreError> {
    let evidence: Option<String> = conn.query_row(
        "SELECT process_group_exit_evidence FROM tasks WHERE id = ?1",
        [task_id.to_string()],
        |row| row.get(0),
    )?;
    if ProcessGroupExitEvidence::from_storage(evidence.as_deref())
        != ProcessGroupExitEvidence::ConfirmedExited
    {
        return Ok(false);
    }

    Ok(conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM trainer_attempt_associations
            WHERE resource_id = ?1 AND task_id = ?2
        )",
        params![resource_id.as_uuid().to_string(), task_id.to_string()],
        |row| row.get(0),
    )?)
}

/// Bind one preallocated Homebased watcher launch identity to the exact release action
pub(crate) fn bind_release_watcher_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    intent: ReleaseWatcherIntent,
) -> Result<ReleaseWatcherIntent, ResourceStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let result = bind_release_watcher_intent_on(&tx, authority_machine, resource_id, intent)?;
    tx.commit()?;
    Ok(result)
}

pub(crate) fn validate_release_watcher_for_local_acceptance(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    supervisor: SupervisorAddress,
    intent: &ReleaseWatcherIntent,
) -> Result<SupervisorAddress, ResourceStoreError> {
    let resource =
        select_resource(conn, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_authority(resource.authority_machine(), authority_machine)?;
    if resource.supervisor != supervisor {
        return Err(ResourceStoreError::Conflict);
    }
    validate_release_watcher_intent_on(conn, authority_machine, resource_id, intent)?;
    Ok(resource.supervisor)
}

pub(crate) fn bind_release_watcher_intent_on(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    intent: ReleaseWatcherIntent,
) -> Result<ReleaseWatcherIntent, ResourceStoreError> {
    let mut loan =
        validate_release_watcher_intent_on(conn, authority_machine, resource_id, &intent)?;
    let LoanState::Active {
        phase: LoanPhase::AwaitingRelease { watcher_intent, .. },
    } = &mut loan.state
    else {
        return Err(ResourceStoreError::Conflict);
    };
    if let Some(saved) = watcher_intent {
        return match saved {
            SavedReleaseWatcherIntent::Complete(saved) if saved == &intent => Ok(saved.clone()),
            SavedReleaseWatcherIntent::Complete(_) => Err(ResourceStoreError::Conflict),
            SavedReleaseWatcherIntent::LegacyUnproven(_) => {
                Err(ResourceStoreError::LegacyWatcherIntentUnproven)
            }
        };
    }

    // keep the action revision stable for completion using the release notice
    *watcher_intent = Some(SavedReleaseWatcherIntent::Complete(intent.clone()));
    let loan_state_json = encode_json(&loan.state)?;
    let changed = conn.execute(
        "UPDATE loans SET state_json = ?1
         WHERE id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'active'
           AND json_extract(state_json, '$.phase.type') = 'awaiting_release'
           AND json_extract(state_json, '$.phase.action_id') = ?4
           AND json_extract(state_json, '$.phase.observed_background_task') = ?5
           AND (
               json_type(state_json, '$.phase.watcher_intent') IS NULL
               OR json_type(state_json, '$.phase.watcher_intent') = 'null'
           )
           AND EXISTS (
               SELECT 1 FROM resources
               WHERE id = ?3 AND authority_machine = ?6
                 AND state_revision = ?7 AND registered_background_task = ?5
           )",
        params![
            loan_state_json,
            loan.id.as_uuid().to_string(),
            resource_id.as_uuid().to_string(),
            intent.action_id.as_uuid().to_string(),
            intent.observed_background_task.to_string(),
            authority_machine.as_uuid().to_string(),
            sqlite_integer(intent.state_revision.get())?,
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict);
    }
    Ok(intent)
}

pub(crate) fn validate_release_watcher_intent_on(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    intent: &ReleaseWatcherIntent,
) -> Result<Loan, ResourceStoreError> {
    let resource =
        select_resource(conn, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_authority(resource.authority_machine(), authority_machine)?;
    let watcher_task_id = intent.watcher_task_id.as_task_id();
    if resource.state_revision != intent.state_revision
        || resource.registered_background_task != Some(intent.observed_background_task)
        || watcher_task_id.0.is_nil()
        || watcher_task_id == intent.observed_background_task
        || intent.request_id.0.is_nil()
        || intent.request_id.0 == watcher_task_id.0
    {
        return Err(ResourceStoreError::Conflict);
    }

    let loan = select_non_closed_loan(conn, resource_id)?.ok_or(ResourceStoreError::Conflict)?;
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task,
                watcher_intent,
            },
    } = &loan.state
    else {
        return Err(ResourceStoreError::Conflict);
    };
    if *action_id != intent.action_id
        || *observed_background_task != intent.observed_background_task
    {
        return Err(ResourceStoreError::Conflict);
    }

    let Some((notice, _)) = select_supervisor_notice_record_by_action(conn, intent.action_id)?
    else {
        return Err(ResourceStoreError::Conflict);
    };
    if notice.loan_id != loan.id
        || notice.state_revision != intent.state_revision
        || notice.payload
            != (SupervisorNoticePayload::ReleaseRequired {
                task_id: intent.observed_background_task,
            })
    {
        return Err(ResourceStoreError::Conflict);
    }

    if release_watcher_identity_is_claimed(conn, loan.id, intent.request_id, watcher_task_id)? {
        return Err(ResourceStoreError::Conflict);
    }
    if let Some(saved) = watcher_intent {
        let Some(saved) = saved.complete() else {
            return Err(ResourceStoreError::LegacyWatcherIntentUnproven);
        };
        if saved != intent {
            return Err(ResourceStoreError::Conflict);
        }
        return Ok(loan);
    }
    if request_identity_exists(conn, intent.request_id)?
        || local_task_identity_exists(conn, watcher_task_id)?
    {
        return Err(ResourceStoreError::Conflict);
    }
    Ok(loan)
}

/// Return an exact committed release result without requiring the original artifacts
pub(crate) fn release_completion_for_retry(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    action_id: ActionId,
    expected_state_revision: ResourceRevision,
) -> Result<Option<ReleaseCompletionResult>, CompleteReleaseError> {
    let Some(receipt) = select_release_completion_receipt(conn, action_id)? else {
        return Ok(None);
    };
    if receipt.action_id != action_id
        || receipt.authority_machine != authority_machine
        || receipt.resource_id != resource_id
        || receipt.expected_state_revision != expected_state_revision
    {
        return Err(CompleteReleaseError::ConflictingRetry { action_id });
    }

    Ok(Some(receipt.result))
}

/// Whether a release notice names an assignment its action may still complete under
///
/// A replacement retargets only undelivered notices. A delivered notice keeps the
/// older assignment whose supervisor launched the watcher, so it stays valid
fn release_notice_assignment_is_valid(notice: &SupervisorNotice, resource: &Resource) -> bool {
    let current = notice.destination == resource.supervisor
        && notice.assignment_revision == resource.assignment_revision;
    let delivered_before_replacement =
        matches!(notice.delivery, SupervisorNoticeDelivery::Delivered { .. })
            && notice.assignment_revision.get() < resource.assignment_revision.get();
    current || delivered_before_replacement
}

/// Complete a saved release action with an authority-built proof and atomically assign work
pub(crate) fn complete_release_for_authority(
    conn: &mut Connection,
    proof: VerifiedReleaseProof,
) -> Result<ReleaseCompletionResult, CompleteReleaseError> {
    let authority_machine = proof.authority_machine();
    let resource_id = proof.resource_id();
    let action_id = proof.action_id();
    let expected_state_revision = proof.expected_state_revision();
    let observed_task_id = proof.task_id();
    let return_context = proof.return_context();
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    if let Some(receipt) = select_release_completion_receipt(&tx, action_id)? {
        if receipt.action_id != action_id
            || receipt.authority_machine != authority_machine
            || receipt.resource_id != resource_id
            || receipt.expected_state_revision != expected_state_revision
            || receipt.return_context != return_context
        {
            return Err(CompleteReleaseError::ConflictingRetry { action_id });
        }

        tx.commit()?;
        return Ok(receipt.result);
    }

    let resource =
        select_resource(&tx, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_authority(resource.authority_machine(), authority_machine)?;

    let (notice, _) = select_supervisor_notice_record_by_action(&tx, action_id)?
        .ok_or(CompleteReleaseError::ActionNotFound { action_id })?;
    let loan = tx
        .query_row(
            "SELECT id, resource_id, state_json FROM loans WHERE id = ?1",
            [notice.loan_id.as_uuid().to_string()],
            decode_loan,
        )
        .optional()?
        .ok_or(CompleteReleaseError::InvalidReleaseNotice { action_id })?;

    let LoanState::Active {
        phase:
            LoanPhase::AwaitingRelease {
                action_id: saved_action_id,
                observed_background_task,
                ..
            },
    } = &loan.state
    else {
        return Err(CompleteReleaseError::NotAwaitingRelease {
            loan_id: loan.id,
            action_id,
        });
    };
    if *saved_action_id != action_id {
        return Err(CompleteReleaseError::NotAwaitingRelease {
            loan_id: loan.id,
            action_id,
        });
    }
    if *observed_background_task != observed_task_id
        || resource.registered_background_task != Some(observed_task_id)
    {
        return Err(CompleteReleaseError::TrainerAssociationMismatch {
            task_id: observed_task_id,
        });
    }

    if loan.resource_id != resource_id
        || notice.loan_id != loan.id
        || notice.action_id != action_id
        || notice.payload
            != (SupervisorNoticePayload::ReleaseRequired {
                task_id: observed_task_id,
            })
        || !release_notice_assignment_is_valid(&notice, &resource)
    {
        return Err(CompleteReleaseError::InvalidReleaseNotice { action_id });
    }

    if notice.state_revision != expected_state_revision {
        return Err(CompleteReleaseError::StaleRevision {
            expected: expected_state_revision,
            actual: notice.state_revision,
        });
    }
    if resource.state_revision != expected_state_revision {
        return Err(CompleteReleaseError::StaleRevision {
            expected: expected_state_revision,
            actual: resource.state_revision,
        });
    }

    require_release_proof_matches_transaction(&tx, &proof)?;
    let release_provenance = proof.serving_release_provenance();

    let next_revision_value = expected_state_revision.get().checked_add(1).ok_or(
        CompleteReleaseError::RevisionExhausted {
            revision: expected_state_revision,
        },
    )?;
    let next_revision = ResourceRevision::new(next_revision_value);

    let result = if let Some(mut request) =
        oldest_queued_request_inner(&tx, Some(authority_machine), resource_id)?
    {
        request.state = ResourceRequestState::Assigned { loan_id: loan.id };
        let request_state_json = encode_completion_json(&request.state)?;
        let changed = tx.execute(
            "UPDATE resource_requests SET state_json = ?1
             WHERE request_id = ?2 AND resource_id = ?3 AND acceptance_sequence = ?4
               AND json_extract(state_json, '$.type') = 'queued'",
            params![
                request_state_json,
                request.request_id.0.to_string(),
                resource_id.as_uuid().to_string(),
                sqlite_integer(request.acceptance_sequence.get())?,
            ],
        )?;
        if changed != 1 {
            return Err(CompleteReleaseError::RequestChanged {
                request_id: request.request_id,
            });
        }

        let updated_loan = Loan {
            id: loan.id,
            resource_id,
            state: LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context: return_context.clone(),
                    current_request_id: request.request_id,
                    release_provenance,
                },
            },
        };
        update_release_loan(&tx, &updated_loan, action_id)?;
        update_resource_revision(
            &tx,
            authority_machine,
            resource_id,
            expected_state_revision,
            next_revision,
        )?;

        ReleaseCompletionResult::Assigned {
            loan: updated_loan,
            request,
            state_revision: next_revision,
        }
    } else {
        let return_action_id = ActionId::new();
        let updated_loan = Loan {
            id: loan.id,
            resource_id,
            state: LoanState::Active {
                phase: LoanPhase::AwaitingReturn {
                    action_id: return_action_id,
                    return_context: return_context.clone(),
                },
            },
        };
        update_release_loan(&tx, &updated_loan, action_id)?;
        update_resource_revision(
            &tx,
            authority_machine,
            resource_id,
            expected_state_revision,
            next_revision,
        )?;

        let notice = SupervisorNotice {
            id: NoticeId::new(),
            loan_id: loan.id,
            action_id: return_action_id,
            state_revision: next_revision,
            destination: resource.supervisor,
            assignment_revision: resource.assignment_revision,
            payload: SupervisorNoticePayload::ReturnRequired {
                return_context: return_context.clone(),
            },
            delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
        };
        let notice = insert_supervisor_notice_in_transaction(&tx, &notice)?;

        ReleaseCompletionResult::ReturnRequired {
            loan: updated_loan,
            notice,
        }
    };

    let receipt = ReleaseCompletionReceipt {
        action_id,
        authority_machine,
        resource_id,
        expected_state_revision,
        return_context,
        release_provenance: proof.serving_release_provenance(),
        result: result.clone(),
    };
    let receipt_json = encode_completion_json(&receipt)?;
    tx.execute(
        "INSERT INTO resource_release_completions (action_id, receipt_json)
         VALUES (?1, ?2)",
        params![action_id.as_uuid().to_string(), receipt_json],
    )?;

    proof.verify_external_evidence()?;
    tx.commit()?;

    Ok(result)
}

fn require_release_proof_matches_transaction(
    conn: &Connection,
    proof: &VerifiedReleaseProof,
) -> Result<(), CompleteReleaseError> {
    let task_id = proof.task_id();
    let current_task = crate::store::task_by_id_on(conn, task_id)?
        .ok_or(CompleteReleaseError::BackgroundTaskMissing { task_id })?;
    let saved_task = proof.task_row();
    if current_task.id != saved_task.id
        || current_task.name != saved_task.name
        || current_task.thread != saved_task.thread
        || current_task.workload != saved_task.workload
        || current_task.cwd != saved_task.cwd
        || current_task.timeout != saved_task.timeout
        || current_task.env != saved_task.env
        || current_task.binary != saved_task.binary
    {
        return Err(CompleteReleaseError::TaskCommandChanged { task_id });
    }
    if current_task.state != saved_task.state {
        return Err(CompleteReleaseError::TaskStateChanged { task_id });
    }
    match proof.stopped_decision_and_cancellation() {
        Some((expected_decision, expected_cancellation)) => {
            if !matches!(
                current_task.state,
                TaskState::Finished {
                    reason: crate::domain::ExitReason::Cancelled
                }
            ) {
                return Err(CompleteReleaseError::TaskStateChanged { task_id });
            }
            if current_task.cancel_requested_at != Some(expected_cancellation.cancel_requested_at) {
                return Err(CompleteReleaseError::TrainerCancellationMarkerChanged { task_id });
            }

            let Some((checkpoint_state, _)) =
                release_checkpoint_state_for_action(conn, proof.resource_id(), proof.action_id())?
            else {
                return Err(CompleteReleaseError::StoppedProofUnavailable { task_id });
            };
            let ReleaseCheckpointPhase::CancellationCommitted {
                decision,
                cancellation,
                ..
            } = checkpoint_state.phase
            else {
                return Err(CompleteReleaseError::StoppedProofUnavailable { task_id });
            };
            if checkpoint_state.action.resource_id != proof.resource_id()
                || checkpoint_state.action.action_id != proof.action_id()
                || checkpoint_state.action.state_revision != proof.expected_state_revision()
                || checkpoint_state.action.observed_background_task != task_id
                || *decision != *expected_decision
                || cancellation != *expected_cancellation
            {
                return Err(CompleteReleaseError::StoppedProofUnavailable { task_id });
            }
        }
        None => {
            let expected_reason = proof
                .ended_outcome()
                .cloned()
                .unwrap_or(crate::domain::ExitReason::Exit { code: 0 });
            if !matches!(
                &current_task.state,
                TaskState::Finished { reason } if *reason == expected_reason
            ) {
                return Err(CompleteReleaseError::TaskStateChanged { task_id });
            }
            // an ended cancellation is generic only while this action committed no stop
            if proof.ended_outcome() == Some(&crate::domain::ExitReason::Cancelled)
                && let Some((checkpoint_state, _)) = release_checkpoint_state_for_action(
                    conn,
                    proof.resource_id(),
                    proof.action_id(),
                )?
                && matches!(
                    checkpoint_state.phase,
                    ReleaseCheckpointPhase::CancellationCommitted { .. }
                )
            {
                return Err(CompleteReleaseError::TaskStateChanged { task_id });
            }
        }
    }
    if current_task.process_group_exit_evidence() != ProcessGroupExitEvidence::ConfirmedExited {
        return Err(CompleteReleaseError::WorkerExitUnconfirmed { task_id });
    }

    let association: Option<(String, String, String)> = conn
        .query_row(
            "SELECT resource_id, authority_machine, association_json
             FROM trainer_attempt_associations WHERE task_id = ?1",
            [task_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((resource_id, authority_machine, association_json)) = association else {
        return Err(CompleteReleaseError::TrainerAssociationMissing { task_id });
    };
    if resource_id != proof.resource_id().as_uuid().to_string()
        || authority_machine != proof.authority_machine().as_uuid().to_string()
        || association_json != proof.association_json()
    {
        return Err(CompleteReleaseError::TrainerAssociationMismatch { task_id });
    }

    let identity_json: Option<String> = conn
        .query_row(
            "SELECT identity_json FROM executor_identities WHERE task_id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(identity_json) = identity_json else {
        return Err(CompleteReleaseError::TrainerIdentityMissing { task_id });
    };
    if identity_json != proof.identity_json() {
        return Err(CompleteReleaseError::TrainerIdentityChanged { task_id });
    }

    Ok(())
}

fn update_release_loan(
    tx: &Transaction<'_>,
    loan: &Loan,
    action_id: ActionId,
) -> Result<(), CompleteReleaseError> {
    let state_json = encode_completion_json(&loan.state)?;
    let changed = tx.execute(
        "UPDATE loans SET state_json = ?1
         WHERE id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'active'
           AND json_extract(state_json, '$.phase.type') = 'awaiting_release'
           AND json_extract(state_json, '$.phase.action_id') = ?4",
        params![
            state_json,
            loan.id.as_uuid().to_string(),
            loan.resource_id.as_uuid().to_string(),
            action_id.as_uuid().to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(CompleteReleaseError::LoanChanged { loan_id: loan.id });
    }

    Ok(())
}

/// Reconcile one exact assigned task and atomically finish it before selecting more work
pub(crate) fn reconcile_assigned_resource_task_for_authority(
    conn: &mut Connection,
    input: AssignedResourceTaskReconcileInput,
) -> Result<AssignedResourceTaskReconcileOutcome, ResourceStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(receipt) = select_resource_task_completion_receipt(&tx, input.task_id)? {
        return retry_resource_task_completion(&receipt, input);
    }

    let Some(resource) = select_resource(&tx, input.resource_id)? else {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::LoanChanged,
        ));
    };
    check_authority(resource.authority_machine(), input.authority_machine)?;
    if resource.state_revision != input.expected_state_revision {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::StaleRevision,
        ));
    }

    let Some(mut request) = select_request_by_id(&tx, input.request_id)? else {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::RequestChanged,
        ));
    };
    if request.resource_id != input.resource_id || request.task_id != input.task_id {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::TaskIdentityMismatch,
        ));
    }
    if !matches!(
        &request.state,
        ResourceRequestState::Assigned { loan_id } if *loan_id == input.loan_id
    ) {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::RequestChanged,
        ));
    }

    let Some(loan) = select_non_closed_loan(&tx, input.resource_id)? else {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::LoanChanged,
        ));
    };
    if loan.id != input.loan_id || loan.resource_id != input.resource_id {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::LoanChanged,
        ));
    }
    let LoanState::Active {
        phase:
            LoanPhase::Serving {
                return_context,
                current_request_id,
                release_provenance,
            },
    } = &loan.state
    else {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::LoanChanged,
        ));
    };
    if *current_request_id != input.request_id
        || !serving_release_provenance_matches(
            &tx,
            input.authority_machine,
            &resource,
            &loan,
            return_context,
            release_provenance,
        )?
    {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            if *current_request_id != input.request_id {
                AssignedResourceTaskAttention::LoanChanged
            } else {
                AssignedResourceTaskAttention::ServingReleaseUnverified
            },
        ));
    }

    let (outcome, release_proof) =
        match match_resource_task_layer(&tx, &request, input.authority_machine)? {
            ResourceTaskLayerObservation::NotAccepted => {
                tx.commit()?;
                return Ok(AssignedResourceTaskReconcileOutcome::NotAccepted);
            }
            ResourceTaskLayerObservation::Active(progress) => {
                tx.commit()?;
                return Ok(AssignedResourceTaskReconcileOutcome::Active(progress));
            }
            ResourceTaskLayerObservation::Finished {
                outcome,
                release_proof,
            } => (outcome, release_proof),
            ResourceTaskLayerObservation::Lost => {
                return Ok(AssignedResourceTaskReconcileOutcome::Attention(
                    AssignedResourceTaskAttention::TaskLost,
                ));
            }
            ResourceTaskLayerObservation::Attention(reason) => {
                return Ok(AssignedResourceTaskReconcileOutcome::Attention(reason));
            }
        };

    let prior_work_exists: bool = tx.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM resource_requests
            WHERE resource_id = ?1 AND acceptance_sequence < ?2
              AND json_extract(state_json, '$.type') IN ('queued', 'assigned')
        )",
        params![
            input.resource_id.as_uuid().to_string(),
            sqlite_integer(request.acceptance_sequence.get())?,
        ],
        |row| row.get(0),
    )?;
    if prior_work_exists {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::RequestChanged,
        ));
    }

    request.state = ResourceRequestState::Finished {
        outcome: outcome.clone(),
    };
    let request_state_json = encode_json(&request.state)?;
    let changed = tx.execute(
        "UPDATE resource_requests SET state_json = ?1
         WHERE request_id = ?2 AND task_id = ?3 AND resource_id = ?4
           AND acceptance_sequence = ?5
           AND json_extract(state_json, '$.type') = 'assigned'
           AND json_extract(state_json, '$.loan_id') = ?6",
        params![
            request_state_json,
            request.request_id.0.to_string(),
            request.task_id.to_string(),
            request.resource_id.as_uuid().to_string(),
            sqlite_integer(request.acceptance_sequence.get())?,
            input.loan_id.as_uuid().to_string(),
        ],
    )?;
    if changed != 1 {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::RequestChanged,
        ));
    }

    let next_revision_value = input
        .expected_state_revision
        .get()
        .checked_add(1)
        .ok_or(ResourceStoreError::Conflict)?;
    let next_revision = ResourceRevision::new(next_revision_value);
    let result = if let Some(mut next_request) =
        oldest_queued_request_inner(&tx, Some(input.authority_machine), input.resource_id)?
    {
        next_request.state = ResourceRequestState::Assigned {
            loan_id: input.loan_id,
        };
        let next_request_state_json = encode_json(&next_request.state)?;
        let changed = tx.execute(
            "UPDATE resource_requests SET state_json = ?1
             WHERE request_id = ?2 AND resource_id = ?3 AND acceptance_sequence = ?4
               AND json_extract(state_json, '$.type') = 'queued'",
            params![
                next_request_state_json,
                next_request.request_id.0.to_string(),
                input.resource_id.as_uuid().to_string(),
                sqlite_integer(next_request.acceptance_sequence.get())?,
            ],
        )?;
        if changed != 1 {
            return Ok(AssignedResourceTaskReconcileOutcome::Attention(
                AssignedResourceTaskAttention::RequestChanged,
            ));
        }

        let updated_loan = Loan {
            id: loan.id,
            resource_id: input.resource_id,
            state: LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context: return_context.clone(),
                    current_request_id: next_request.request_id,
                    release_provenance: release_provenance.clone(),
                },
            },
        };
        update_serving_loan_for_task_completion(&tx, &loan, input.request_id, &updated_loan)?;
        update_resource_revision_for_task_completion(
            &tx,
            input.authority_machine,
            input.resource_id,
            input.expected_state_revision,
            next_revision,
        )?;

        ResourceTaskCompletionResult::Assigned {
            finished_request: request.clone(),
            loan: updated_loan,
            next_request,
            state_revision: next_revision,
        }
    } else {
        let action_id = ActionId::new();
        let updated_loan = Loan {
            id: loan.id,
            resource_id: input.resource_id,
            state: LoanState::Active {
                phase: LoanPhase::AwaitingReturn {
                    action_id,
                    return_context: return_context.clone(),
                },
            },
        };
        update_serving_loan_for_task_completion(&tx, &loan, input.request_id, &updated_loan)?;
        update_resource_revision_for_task_completion(
            &tx,
            input.authority_machine,
            input.resource_id,
            input.expected_state_revision,
            next_revision,
        )?;

        let notice = SupervisorNotice {
            id: NoticeId::new(),
            loan_id: loan.id,
            action_id,
            state_revision: next_revision,
            destination: resource.supervisor,
            assignment_revision: resource.assignment_revision,
            payload: SupervisorNoticePayload::ReturnRequired {
                return_context: return_context.clone(),
            },
            delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
        };
        let notice =
            insert_supervisor_notice_in_transaction(&tx, &notice).map_err(|error| match error {
                SupervisorNoticeStoreError::Storage(error) => ResourceStoreError::Storage(error),
                _ => ResourceStoreError::Conflict,
            })?;
        ResourceTaskCompletionResult::ReturnRequired {
            finished_request: request.clone(),
            loan: updated_loan,
            notice,
        }
    };

    let receipt = ResourceTaskCompletionReceipt {
        authority_machine: input.authority_machine,
        resource_id: input.resource_id,
        loan_id: input.loan_id,
        request_id: input.request_id,
        task_id: input.task_id,
        expected_state_revision: input.expected_state_revision,
        outcome,
        release_proof,
        result: result.clone(),
    };
    tx.execute(
        "INSERT INTO resource_task_completions (task_id, request_id, receipt_json)
         VALUES (?1, ?2, ?3)",
        params![
            input.task_id.to_string(),
            input.request_id.0.to_string(),
            encode_json(&receipt)?,
        ],
    )?;
    tx.commit()?;

    Ok(AssignedResourceTaskReconcileOutcome::Completed(Box::new(
        result,
    )))
}

enum ResourceTaskLayerObservation {
    NotAccepted,
    Active(AssignedResourceTaskProgress),
    Finished {
        outcome: ExitReason,
        release_proof: ResourceTaskReleaseProof,
    },
    Lost,
    Attention(AssignedResourceTaskAttention),
}

fn match_resource_task_layer(
    conn: &Connection,
    request: &ResourceRequest,
    authority_machine: MachineId,
) -> Result<ResourceTaskLayerObservation, ResourceStoreError> {
    let identity = crate::store::executor_identity_for_resource_task_on(conn, request.task_id)?;
    let task =
        crate::store::task_by_id_on(conn, request.task_id).map_err(ResourceStoreError::TaskRow)?;
    let has_event: bool = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM executor_outbox WHERE task_id=?1
            UNION ALL SELECT 1 FROM executor_event_receipts WHERE task_id=?1
            UNION ALL SELECT 1 FROM executor_event_cursors WHERE task_id=?1
            UNION ALL SELECT 1 FROM executor_event_routes WHERE task_id=?1
        )",
        [request.task_id.to_string()],
        |row| row.get(0),
    )?;
    let Some(identity) = identity else {
        return Ok(if task.is_none() && !has_event {
            ResourceTaskLayerObservation::NotAccepted
        } else {
            ResourceTaskLayerObservation::Attention(
                AssignedResourceTaskAttention::TaskIdentityMismatch,
            )
        });
    };
    let ExecutorIdentity::Accepted(record) = identity else {
        return Ok(ResourceTaskLayerObservation::Attention(
            AssignedResourceTaskAttention::TaskIdentityMismatch,
        ));
    };
    let Some(task) = task else {
        return Ok(ResourceTaskLayerObservation::Attention(
            AssignedResourceTaskAttention::TaskIdentityMismatch,
        ));
    };
    let saved_origin: Option<String> = conn
        .query_row(
            "SELECT origin_machine FROM executor_identities WHERE task_id=?1",
            [request.task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let event_matches = match crate::store::initial_queued_event_matches_on(
        conn,
        request.task_id,
        request.origin_machine,
        authority_machine,
    ) {
        Ok(matches) => matches,
        Err(_) => {
            return Ok(ResourceTaskLayerObservation::Attention(
                AssignedResourceTaskAttention::TaskIdentityMismatch,
            ));
        }
    };
    let spec_matches = record
        .current_spec()
        .map(|spec| same_spec(spec, request.spec().as_normalized()))
        .transpose()?
        .unwrap_or(false);
    let expected_origin = request.origin_machine.as_uuid().to_string();
    if record.task != request.task_id
        || record.origin_machine != request.origin_machine
        || record.execution_machine != authority_machine
        || !record.has_valid_spec_owners()
        || record.state != task.status()
        || !spec_matches
        || saved_origin.as_deref() != Some(expected_origin.as_str())
        || !has_event
        || !event_matches
        || !resource_task_row_matches(&task, request)
    {
        return Ok(ResourceTaskLayerObservation::Attention(
            AssignedResourceTaskAttention::TaskIdentityMismatch,
        ));
    }

    match &task.state {
        TaskState::Queued => Ok(ResourceTaskLayerObservation::Active(
            AssignedResourceTaskProgress::Queued,
        )),
        TaskState::Running { .. } => Ok(ResourceTaskLayerObservation::Active(
            AssignedResourceTaskProgress::Running,
        )),
        TaskState::Lost => Ok(ResourceTaskLayerObservation::Lost),
        TaskState::Finished { reason } => {
            match resource_task_release_proof(request, reason, task.process_group_exit_evidence()) {
                Ok(release_proof) => Ok(ResourceTaskLayerObservation::Finished {
                    outcome: reason.clone(),
                    release_proof,
                }),
                Err(reason) => Ok(ResourceTaskLayerObservation::Attention(reason)),
            }
        }
    }
}

fn resource_task_row_matches(task: &crate::domain::TaskRow, request: &ResourceRequest) -> bool {
    let spec = request.spec().as_normalized();
    task.id == request.task_id
        && task.name.as_ref() == Some(&spec.name)
        && task.thread == spec.thread
        && task.workload == crate::invocation::persist_workload(&spec.workload)
        && task.cwd == spec.cwd
        && task.timeout == spec.timeout
        && task.binary.is_absolute()
}

fn resource_task_release_proof(
    request: &ResourceRequest,
    outcome: &ExitReason,
    evidence: ProcessGroupExitEvidence,
) -> Result<ResourceTaskReleaseProof, AssignedResourceTaskAttention> {
    match evidence {
        ProcessGroupExitEvidence::ConfirmedExited => {
            if let Some(risk) = resource_task_ownership_risk(request.spec()) {
                return Err(AssignedResourceTaskAttention::OwnershipUncertain(risk));
            }
            Ok(ResourceTaskReleaseProof::ConfirmedProcessGroupExit)
        }
        // the task layer records NoChildSpawned only before the worker spawns its
        // child or when a store CAS leaves Queued, which the worker needs to win
        // before it spawns; any other outcome contradicts those paths
        ProcessGroupExitEvidence::NoChildSpawned => match outcome {
            ExitReason::SpawnFailed { .. } => {
                Ok(ResourceTaskReleaseProof::NoChildSpawnedAfterSpawnFailure)
            }
            ExitReason::Cancelled => Ok(ResourceTaskReleaseProof::NoChildSpawnedAfterQueuedCancel),
            ExitReason::Exit { .. } | ExitReason::Signal { .. } => {
                Err(AssignedResourceTaskAttention::InvalidNoChildSpawnEvidence)
            }
        },
        ProcessGroupExitEvidence::Unconfirmed => {
            Err(AssignedResourceTaskAttention::ProcessGroupExitUnconfirmed)
        }
    }
}

/// Classify a queued command against the foreground ownership contract
///
/// Only the foreground contract lets process-group exit release the resource
pub(crate) fn resource_task_ownership_risk(
    command: &CommandSpec,
) -> Option<ResourceTaskOwnershipRisk> {
    let command = crate::resource::foreground::task_command(command.as_normalized())?;
    crate::resource::foreground::CommandOwnershipContract::for_queued_command(command).err()
}

fn select_resource_task_completion_receipt(
    conn: &Connection,
    task_id: TaskId,
) -> Result<Option<ResourceTaskCompletionReceipt>, ResourceStoreError> {
    let receipt_json: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_task_completions WHERE task_id=?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    receipt_json
        .map(|json| decode_json(&json, 0).map_err(Into::into))
        .transpose()
}

// the receipt is the committed decision for this exact task, so a retry returns
// it even after later transitions; re-reading task rows here would let retention
// or later edits turn a settled completion into attention
fn retry_resource_task_completion(
    receipt: &ResourceTaskCompletionReceipt,
    input: AssignedResourceTaskReconcileInput,
) -> Result<AssignedResourceTaskReconcileOutcome, ResourceStoreError> {
    check_authority(receipt.authority_machine, input.authority_machine)?;
    if receipt.resource_id != input.resource_id
        || receipt.loan_id != input.loan_id
        || receipt.request_id != input.request_id
        || receipt.task_id != input.task_id
    {
        return Ok(AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::TaskIdentityMismatch,
        ));
    }

    Ok(AssignedResourceTaskReconcileOutcome::Completed(Box::new(
        receipt.result.clone(),
    )))
}

fn update_serving_loan_for_task_completion(
    tx: &Transaction<'_>,
    original: &Loan,
    request_id: RequestId,
    updated: &Loan,
) -> Result<(), ResourceStoreError> {
    let state_json = encode_json(&updated.state)?;
    let changed = tx.execute(
        "UPDATE loans SET state_json = ?1
         WHERE id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'active'
           AND json_extract(state_json, '$.phase.type') = 'serving'
           AND json_extract(state_json, '$.phase.current_request_id') = ?4",
        params![
            state_json,
            original.id.as_uuid().to_string(),
            original.resource_id.as_uuid().to_string(),
            request_id.0.to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict);
    }

    Ok(())
}

fn update_resource_revision_for_task_completion(
    tx: &Transaction<'_>,
    authority_machine: MachineId,
    resource_id: ResourceId,
    expected_revision: ResourceRevision,
    next_revision: ResourceRevision,
) -> Result<(), ResourceStoreError> {
    let changed = tx.execute(
        "UPDATE resources SET state_revision = ?1
         WHERE id = ?2 AND authority_machine = ?3 AND state_revision = ?4",
        params![
            sqlite_integer(next_revision.get())?,
            resource_id.as_uuid().to_string(),
            authority_machine.as_uuid().to_string(),
            sqlite_integer(expected_revision.get())?,
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict);
    }

    Ok(())
}

fn update_resource_revision(
    tx: &Transaction<'_>,
    authority_machine: MachineId,
    resource_id: ResourceId,
    expected_revision: ResourceRevision,
    next_revision: ResourceRevision,
) -> Result<(), CompleteReleaseError> {
    let expected_revision_sql = sqlite_integer(expected_revision.get())?;
    let next_revision_sql = sqlite_integer(next_revision.get())?;
    let changed = tx.execute(
        "UPDATE resources SET state_revision = ?1
         WHERE id = ?2 AND authority_machine = ?3 AND state_revision = ?4",
        params![
            next_revision_sql,
            resource_id.as_uuid().to_string(),
            authority_machine.as_uuid().to_string(),
            expected_revision_sql,
        ],
    )?;
    if changed != 1 {
        let actual = select_resource(tx, resource_id)?
            .ok_or(ResourceStoreError::ResourceNotFound)?
            .state_revision;
        return Err(CompleteReleaseError::StaleRevision {
            expected: expected_revision,
            actual,
        });
    }

    Ok(())
}

/// Read the action and return context that a verified release receipt gave one loan
pub(crate) fn release_completion_for_loan(
    conn: &Connection,
    resource_id: ResourceId,
    loan_id: LoanId,
) -> Result<Option<(ActionId, ReturnContext)>, rusqlite::Error> {
    let receipt_json: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_release_completions
             WHERE json_extract(receipt_json, '$.resource_id') = ?1
               AND json_extract(receipt_json, '$.result.loan.id') = ?2",
            params![
                resource_id.as_uuid().to_string(),
                loan_id.as_uuid().to_string()
            ],
            |row| row.get(0),
        )
        .optional()?;
    receipt_json
        .map(|json| {
            decode_json::<ReleaseCompletionReceipt>(&json, 0)
                .map(|receipt| (receipt.action_id, receipt.return_context))
        })
        .transpose()
}

fn select_release_completion_receipt(
    conn: &Connection,
    action_id: ActionId,
) -> Result<Option<ReleaseCompletionReceipt>, CompleteReleaseError> {
    let receipt_json: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_release_completions WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;

    receipt_json
        .map(|json| decode_json(&json, 0).map_err(CompleteReleaseError::Storage))
        .transpose()
}

fn encode_completion_json<T: Serialize>(value: &T) -> Result<String, CompleteReleaseError> {
    serde_json::to_string(value).map_err(|error| {
        CompleteReleaseError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
    })
}

/// Register one resource on its declared authority without changing fixed content.
pub(crate) fn register_resource_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource: &Resource,
) -> Result<Resource, ResourceStoreError> {
    register_resource_inner(conn, Some(authority_machine), resource)
}

#[cfg(test)]
fn register_resource(
    conn: &mut Connection,
    resource: &Resource,
) -> Result<Resource, ResourceStoreError> {
    register_resource_inner(conn, None, resource)
}

fn register_resource_inner(
    conn: &mut Connection,
    authority_machine: Option<MachineId>,
    resource: &Resource,
) -> Result<Resource, ResourceStoreError> {
    if let Some(authority_machine) = authority_machine {
        check_authority(resource.authority_machine(), authority_machine)?;
    }

    let assignment_revision = sqlite_integer(resource.assignment_revision.get())?;
    let state_revision = sqlite_integer(resource.state_revision.get())?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    let receipt = ResourceRegistrationReceipt::initial(resource);
    if let Some(saved) = select_resource(&tx, resource.id)? {
        // an exact retry matches the first registration, even after a supervisor replacement
        match select_resource_registration(&tx, resource.id)? {
            Some(SavedResourceRegistration::Recorded(first)) if first == receipt => {}
            Some(SavedResourceRegistration::Recorded(_)) => {
                return Err(ResourceStoreError::RegistrationConflict {
                    resource: resource.id,
                });
            }
            Some(SavedResourceRegistration::LegacyUnproven) | None => {
                return Err(ResourceStoreError::LegacyRegistrationUnproven {
                    resource: resource.id,
                });
            }
        }

        tx.commit()?;
        return Ok(saved);
    }

    tx.execute(
        "INSERT INTO resources (
            id, display_name, authority_machine, supervisor_machine, supervisor_thread,
            assignment_revision, state_revision, registered_background_task
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            resource.id.as_uuid().to_string(),
            resource.display_name,
            resource.authority_machine().as_uuid().to_string(),
            resource.supervisor.machine.as_uuid().to_string(),
            resource.supervisor.thread.to_string(),
            assignment_revision,
            state_revision,
            resource
                .registered_background_task
                .map(|task| task.to_string()),
        ],
    )?;
    insert_resource_registration(&tx, &receipt)?;
    tx.commit()?;
    Ok(resource.clone())
}

fn insert_resource_registration(
    conn: &Connection,
    receipt: &ResourceRegistrationReceipt,
) -> Result<(), ResourceStoreError> {
    let json = encode_json(receipt)?;
    conn.execute(
        "INSERT INTO resource_registration_receipts (resource_id, receipt_json)
         VALUES (?1, ?2)",
        params![receipt.resource_id.as_uuid().to_string(), json],
    )?;
    Ok(())
}

/// Read the first registration saved for one resource, or `None` when no resource exists
pub(crate) fn select_resource_registration(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<SavedResourceRegistration>, ResourceStoreError> {
    let row = conn
        .query_row(
            "SELECT r.receipt_json
             FROM resources s
             LEFT JOIN resource_registration_receipts r ON r.resource_id = s.id
             WHERE s.id = ?1",
            [resource_id.as_uuid().to_string()],
            |row| {
                row.get::<_, Option<String>>(0)?
                    .map(|json| decode_json::<ResourceRegistrationReceipt>(&json, 0))
                    .transpose()
            },
        )
        .optional()?;
    let Some(receipt) = row else {
        return Ok(None);
    };
    let Some(receipt) = receipt else {
        return Ok(Some(SavedResourceRegistration::LegacyUnproven));
    };
    if receipt.resource_id != resource_id {
        return Err(ResourceStoreError::Conflict);
    }
    Ok(Some(SavedResourceRegistration::Recorded(receipt)))
}

/// Save first-registration receipts that older resource rows still prove
///
/// Registration always assigned revision zero, and only a supervisor replacement
/// raises it. A row still at revision zero keeps its first supervisor, so its
/// receipt is provable. A replaced row gets no receipt, and a retry fails closed
pub(crate) fn backfill_resource_registration_receipts(
    conn: &Connection,
) -> Result<(), ResourceStoreError> {
    let ids = {
        let mut statement = conn.prepare(
            "SELECT id FROM resources
             WHERE assignment_revision = 0
               AND id NOT IN (SELECT resource_id FROM resource_registration_receipts)",
        )?;
        statement
            .query_map([], |row| uuid_column(row, 0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for id in ids {
        let resource = select_resource(conn, ResourceId::from_uuid(id))?
            .ok_or(ResourceStoreError::ResourceNotFound)?;
        insert_resource_registration(conn, &ResourceRegistrationReceipt::initial(&resource))?;
    }
    Ok(())
}

/// Accept one command request on the resource's fixed authority.
///
/// The origin route is stored by the caller before it sends this queue request.
pub(crate) fn accept_request_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
    normalized_spec: NormalizedSpec,
) -> Result<ResourceRequest, ResourceStoreError> {
    accept_request_inner(
        conn,
        Some(authority_machine),
        request_id,
        task_id,
        resource_id,
        origin_machine,
        normalized_spec,
    )
}

#[cfg(test)]
fn accept_request(
    conn: &mut Connection,
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
    normalized_spec: NormalizedSpec,
) -> Result<ResourceRequest, ResourceStoreError> {
    accept_request_inner(
        conn,
        None,
        request_id,
        task_id,
        resource_id,
        origin_machine,
        normalized_spec,
    )
}

fn accept_request_inner(
    conn: &mut Connection,
    authority_machine: Option<MachineId>,
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
    normalized_spec: NormalizedSpec,
) -> Result<ResourceRequest, ResourceStoreError> {
    let identity = RequestIdentity {
        request_id,
        task_id,
        resource_id,
        origin_machine,
    };
    {
        // an exact retry is answered from the saved request before the command
        // checks, so a file that changed after acceptance cannot reject it
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        if let Some(saved) = replay_request_on(&tx, authority_machine, identity, &normalized_spec)?
        {
            tx.commit()?;
            return Ok(saved);
        }
    }
    // the entry-point check reads the file system outside the IMMEDIATE write transaction
    check_queued_command_ownership(&normalized_spec)?;

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(saved) = replay_request_on(&tx, authority_machine, identity, &normalized_spec)? {
        tx.commit()?;
        return Ok(saved);
    }

    if task_id_exists(&tx, task_id)? {
        return Err(ResourceStoreError::Conflict);
    }

    if prevention_exists(&tx, identity)? {
        return Err(ResourceStoreError::Prevented);
    }
    if executor_identity_exists(&tx, task_id)? {
        return Err(ResourceStoreError::Conflict);
    }

    let command_spec = CommandSpec::try_from(normalized_spec)?;
    check_resource_authority(&tx, resource_id, authority_machine)?;
    let spec_json = encode_json(command_spec.as_normalized())?;
    let state_json = encode_json(&ResourceRequestState::Queued)?;

    tx.execute(
        "INSERT INTO resource_requests (
            request_id, task_id, resource_id, origin_machine, spec_json, state_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            request_id.0.to_string(),
            task_id.to_string(),
            resource_id.as_uuid().to_string(),
            origin_machine.as_uuid().to_string(),
            spec_json,
            state_json,
        ],
    )?;
    let raw_sequence = tx.last_insert_rowid();
    let acceptance_sequence = AcceptanceSequence::new(nonnegative_integer(raw_sequence, 0)?);
    let saved = ResourceRequest::new(
        request_id,
        task_id,
        resource_id,
        acceptance_sequence,
        origin_machine,
        command_spec.as_normalized().clone(),
    )?;
    tx.commit()?;
    Ok(saved)
}

/// Return the saved request for an exact retry, or a conflict for changed content
fn replay_request_on(
    conn: &Connection,
    authority_machine: Option<MachineId>,
    identity: RequestIdentity,
    normalized_spec: &NormalizedSpec,
) -> Result<Option<ResourceRequest>, ResourceStoreError> {
    let Some(saved) = select_request_by_id(conn, identity.request_id)? else {
        return Ok(None);
    };
    if saved.task_id != identity.task_id
        || saved.resource_id != identity.resource_id
        || saved.origin_machine != identity.origin_machine
    {
        return Err(ResourceStoreError::Conflict);
    }
    let command_spec = CommandSpec::try_from(normalized_spec.clone())?;
    if !same_spec(saved.spec().as_normalized(), command_spec.as_normalized())? {
        return Err(ResourceStoreError::Conflict);
    }
    check_resource_authority(conn, identity.resource_id, authority_machine)?;

    Ok(Some(saved))
}

/// Refuse a new queued command outside the foreground ownership contract
///
/// A path-qualified entry point is inspected now. A bare program name resolves
/// from the executor `PATH`, so its task binding inspects it before any spawn
fn check_queued_command_ownership(spec: &NormalizedSpec) -> Result<(), ResourceStoreError> {
    let command_spec = CommandSpec::try_from(spec.clone())?;
    if let Some(risk) = resource_task_ownership_risk(&command_spec) {
        return Err(ResourceStoreError::UnsupportedCommandOwnership { risk });
    }
    crate::resource::foreground::inspect_path_qualified_entry_point(spec)
        .map_err(|risk| ResourceStoreError::UnsupportedCommandOwnership { risk })
}

/// Validate the exact FIFO assignment that is eligible for task-layer acceptance.
pub(crate) fn assigned_resource_request_for_acceptance(
    conn: &Connection,
    input: &ResourceTaskAcceptanceInput,
) -> Result<ResourceRequest, ResourceStoreError> {
    let authority =
        check_resource_authority(conn, input.resource_id, Some(input.authority_machine))?;
    let saved =
        select_request_by_id(conn, input.request_id)?.ok_or(ResourceStoreError::Conflict)?;
    let identity = RequestIdentity {
        request_id: input.request_id,
        task_id: input.task_id,
        resource_id: input.resource_id,
        origin_machine: saved.origin_machine,
    };
    if !request_matches_identity(&saved, identity)
        || saved.acceptance_sequence != input.acceptance_sequence
        || !same_spec(
            saved.spec().as_normalized(),
            input.command_spec.as_normalized(),
        )?
    {
        return Err(ResourceStoreError::Conflict);
    }

    if prevention_exists(conn, identity)? {
        return Err(ResourceStoreError::Prevented);
    }
    if let Some(executor) = select_executor_identity(conn, input.task_id)? {
        match executor {
            ExecutorIdentity::Rejected(rejection) => {
                if rejection.origin_machine == saved.origin_machine
                    && rejection.execution_machine == authority
                    && rejection.reason == PreAcceptanceRejection::Cancelled.as_str()
                {
                    return Err(ResourceStoreError::Prevented);
                }
                return Err(ResourceStoreError::Conflict);
            }
            ExecutorIdentity::Accepted(record) => {
                let same_saved_spec = record
                    .current_spec()
                    .map(|spec| same_spec(spec, saved.spec().as_normalized()))
                    .transpose()?
                    .unwrap_or(false);
                if record.task != input.task_id
                    || record.origin_machine != saved.origin_machine
                    || record.execution_machine != authority
                    || !record.has_valid_spec_owners()
                    || !same_saved_spec
                {
                    return Err(ResourceStoreError::Conflict);
                }
            }
        }
    }
    if matches!(&saved.state, ResourceRequestState::CancelledBeforeLaunch) {
        return Err(ResourceStoreError::Prevented);
    }

    let resource =
        select_resource(conn, input.resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    if resource.state_revision != input.expected_state_revision {
        return Err(ResourceStoreError::Conflict);
    }

    let ResourceRequestState::Assigned { loan_id } = &saved.state else {
        return Err(ResourceStoreError::Conflict);
    };
    if *loan_id != input.loan_id {
        return Err(ResourceStoreError::Conflict);
    }
    let loan =
        select_non_closed_loan(conn, input.resource_id)?.ok_or(ResourceStoreError::Conflict)?;
    if loan.id != input.loan_id || loan.resource_id != input.resource_id {
        return Err(ResourceStoreError::Conflict);
    }
    let LoanState::Active {
        phase:
            LoanPhase::Serving {
                current_request_id,
                return_context,
                release_provenance,
            },
    } = &loan.state
    else {
        return Err(ResourceStoreError::Conflict);
    };
    if *current_request_id != input.request_id
        || !serving_release_provenance_matches(
            conn,
            authority,
            &resource,
            &loan,
            return_context,
            release_provenance,
        )?
    {
        return Err(ResourceStoreError::Conflict);
    }

    let earlier_active: bool = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM resource_requests
            WHERE resource_id = ?1 AND acceptance_sequence < ?2
              AND json_extract(state_json, '$.type') IN ('queued', 'assigned')
        )",
        params![
            input.resource_id.as_uuid().to_string(),
            sqlite_integer(input.acceptance_sequence.get())?,
        ],
        |row| row.get(0),
    )?;
    if earlier_active {
        return Err(ResourceStoreError::Conflict);
    }

    Ok(saved)
}

fn serving_release_provenance_matches(
    conn: &Connection,
    authority: MachineId,
    resource: &Resource,
    loan: &Loan,
    return_context: &ReturnContext,
    provenance: &ServingReleaseProvenance,
) -> Result<bool, ResourceStoreError> {
    let (action_id, task_id) = match provenance {
        ServingReleaseProvenance::Unverified => return Ok(false),
        ServingReleaseProvenance::IdleBoundary { proof } => {
            return crate::store::idle_opening_matches_on(
                conn,
                authority,
                resource,
                loan,
                return_context,
                proof,
            );
        }
        ServingReleaseProvenance::OperatorAttestedGpuFree { .. } => {
            return crate::store::operator_serving_release_matches_on(
                conn,
                authority,
                resource,
                loan,
                return_context,
                provenance,
            );
        }
        ServingReleaseProvenance::CompletedTrainerResult {
            action_id, task_id, ..
        }
        | ServingReleaseProvenance::StoppedTrainerCheckpoint {
            action_id, task_id, ..
        }
        | ServingReleaseProvenance::EndedTrainerLockReleased {
            action_id, task_id, ..
        } => (*action_id, *task_id),
    };
    if resource.registered_background_task != Some(task_id) {
        return Ok(false);
    }

    let receipt_json: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_release_completions WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(receipt_json) = receipt_json else {
        return Ok(false);
    };
    let receipt: ReleaseCompletionReceipt = decode_json(&receipt_json, 0)?;
    if receipt.action_id != action_id
        || receipt.authority_machine != authority
        || receipt.resource_id != resource.id
        || receipt.return_context != *return_context
    {
        return Ok(false);
    }
    let receipt_provenance = receipt.release_provenance;

    let ReleaseCompletionResult::Assigned {
        loan: completed_loan,
        request: completed_request,
        state_revision,
    } = receipt.result
    else {
        return Ok(false);
    };
    if completed_loan.id != loan.id
        || completed_loan.resource_id != resource.id
        || completed_request.resource_id != resource.id
        || !matches!(
            completed_request.state,
            ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
        )
        || receipt.expected_state_revision.get().checked_add(1) != Some(state_revision.get())
    {
        return Ok(false);
    }

    let receipt_has_matching_provenance = matches!(
        completed_loan.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: completed_return_context,
                release_provenance: completed_provenance,
                ..
            }
        } if completed_return_context == *return_context && completed_provenance == *provenance
    );
    if !receipt_has_matching_provenance {
        return Ok(false);
    }

    match provenance {
        // an idle opening and an operator attestation have no release completion receipt
        ServingReleaseProvenance::Unverified
        | ServingReleaseProvenance::IdleBoundary { .. }
        | ServingReleaseProvenance::OperatorAttestedGpuFree { .. } => Ok(false),
        ServingReleaseProvenance::CompletedTrainerResult { task_id, .. } => Ok(matches!(
            return_context,
            ReturnContext::AlreadyCompleted { task_id: returned_task, .. }
                if returned_task == task_id
        )),
        ServingReleaseProvenance::StoppedTrainerCheckpoint { .. } => {
            stopped_serving_release_matches_saved_proof(
                conn,
                authority,
                resource,
                receipt.expected_state_revision,
                provenance,
                return_context,
            )
        }
        // only a receipt that saved this exact basis can permit activation
        ServingReleaseProvenance::EndedTrainerLockReleased {
            task_id, outcome, ..
        } => Ok(receipt_provenance == *provenance
            && matches!(
                return_context,
                ReturnContext::EndedWithoutResult {
                    task_id: returned_task,
                    outcome: returned_outcome,
                } if returned_task == task_id && returned_outcome == outcome
            )),
    }
}

fn stopped_serving_release_matches_saved_proof(
    conn: &Connection,
    authority: MachineId,
    resource: &Resource,
    expected_state_revision: ResourceRevision,
    provenance: &ServingReleaseProvenance,
    return_context: &ReturnContext,
) -> Result<bool, ResourceStoreError> {
    let ServingReleaseProvenance::StoppedTrainerCheckpoint {
        action_id,
        task_id,
        generation_id,
        record_sha256,
        inventory_sha256,
    } = provenance
    else {
        return Ok(false);
    };
    let saved: Option<(String, String)> = conn
        .query_row(
            "SELECT resource_id, state_json FROM resource_release_checkpoint_states
             WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((saved_resource_id, state_json)) = saved else {
        return Ok(false);
    };
    if saved_resource_id != resource.id.as_uuid().to_string() {
        return Ok(false);
    }
    let Ok(state) = serde_json::from_str::<ReleaseCheckpointState>(&state_json) else {
        return Ok(false);
    };
    if state.validate().is_err()
        || state.action.resource_id != resource.id
        || state.action.action_id != *action_id
        || state.action.state_revision != expected_state_revision
    {
        return Ok(false);
    }
    let ReleaseCheckpointPhase::CancellationCommitted {
        baseline,
        decision,
        cancellation,
    } = state.phase
    else {
        return Ok(false);
    };
    let checkpoint = &decision.selected_checkpoint;
    if state.action.observed_background_task != *task_id
        || cancellation.task_id != *task_id
        || decision.binding != baseline.binding
        || baseline.binding.action.state_revision != state.action.state_revision
        || baseline.binding.association.resource_id != resource.id
        || baseline.binding.association.authority_machine != authority
        || baseline.binding.association.task_id != *task_id
        || checkpoint.generation_id != *generation_id
        || checkpoint.record_sha256 != *record_sha256
        || checkpoint.inventory_sha256 != *inventory_sha256
        || !matches!(
            return_context,
            ReturnContext::Stopped {
                task_id: returned_task,
                checkpoint_ref,
                recovery_ref,
            } if *returned_task == *task_id
                && checkpoint_ref == &format!(
                    "{}#sha256={}",
                    checkpoint.path.display(),
                    checkpoint.record_sha256
                )
                && recovery_ref == &checkpoint.generation_id
        )
    {
        return Ok(false);
    }

    let association: Option<(String, String, String)> = conn
        .query_row(
            "SELECT resource_id, authority_machine, association_json
             FROM trainer_attempt_associations WHERE task_id = ?1",
            [task_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((association_resource, association_authority, association_json)) = association else {
        return Ok(false);
    };
    let Ok(association) =
        serde_json::from_str::<super::TrainerAttemptAssociationProof>(&association_json)
    else {
        return Ok(false);
    };
    if association_resource != resource.id.as_uuid().to_string()
        || association_authority != authority.as_uuid().to_string()
        || association != baseline.binding.association
    {
        return Ok(false);
    }

    let Some(task) =
        crate::store::task_by_id_on(conn, *task_id).map_err(ResourceStoreError::TaskRow)?
    else {
        return Ok(false);
    };
    if !matches!(
        task.state,
        TaskState::Finished {
            reason: crate::domain::ExitReason::Cancelled
        }
    ) || task.cancel_requested_at != Some(cancellation.cancel_requested_at)
        || task.process_group_exit_evidence() != ProcessGroupExitEvidence::ConfirmedExited
    {
        return Ok(false);
    }

    let identity_json: Option<String> = conn
        .query_row(
            "SELECT identity_json FROM executor_identities WHERE task_id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(identity_json) = identity_json else {
        return Ok(false);
    };
    let Ok(ExecutorIdentity::Accepted(identity)) = serde_json::from_str(&identity_json) else {
        return Ok(false);
    };
    let Some(spec) = identity.current_spec() else {
        return Ok(false);
    };
    Ok(identity.task == *task_id
        && identity.execution_machine == authority
        && identity.state == ProcessStatus::Cancelled
        && identity.has_valid_spec_owners()
        && normalized_spec_sha256(spec).map_err(|error| {
            ResourceStoreError::TaskPreparation(crate::error::AppError::from(error))
        })? == association.normalized_spec_sha256)
}

/// Cancel a queued or assigned request on its fixed authority before activation.
pub(crate) fn cancel_request_before_activation_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
) -> Result<QueueCancellationResult, ResourceStoreError> {
    cancel_request_inner(
        conn,
        Some(authority_machine),
        request_id,
        task_id,
        resource_id,
        origin_machine,
    )
}

#[cfg(test)]
fn cancel_request_before_activation(
    conn: &mut Connection,
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
) -> Result<QueueCancellationResult, ResourceStoreError> {
    cancel_request_inner(conn, None, request_id, task_id, resource_id, origin_machine)
}

fn cancel_request_inner(
    conn: &mut Connection,
    authority_machine: Option<MachineId>,
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
) -> Result<QueueCancellationResult, ResourceStoreError> {
    let identity = RequestIdentity {
        request_id,
        task_id,
        resource_id,
        origin_machine,
    };
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    if let Some(mut saved) = select_request_by_id(&tx, request_id)? {
        if !request_matches_identity(&saved, identity) {
            return Err(ResourceStoreError::Conflict);
        }
        let authority = check_resource_authority(&tx, saved.resource_id, authority_machine)?;

        match saved.state.clone() {
            ResourceRequestState::Queued => {
                cancel_queued_request(&tx, &mut saved, authority)?;
            }
            ResourceRequestState::Assigned { loan_id } => {
                cancel_assigned_request(&tx, &mut saved, loan_id, authority)?;
            }
            _ => {}
        }

        tx.commit()?;
        return Ok(QueueCancellationResult::Request(Box::new(saved)));
    }

    if task_id_exists(&tx, task_id)? {
        return Err(ResourceStoreError::Conflict);
    }

    let prevented = prevention_exists(&tx, identity)?;
    let authority = check_resource_authority(&tx, resource_id, authority_machine)?;
    if !prevented {
        tx.execute(
            "INSERT INTO resource_request_preventions (
                request_id, task_id, resource_id, origin_machine
            ) VALUES (?1, ?2, ?3, ?4)",
            params![
                request_id.0.to_string(),
                task_id.to_string(),
                resource_id.as_uuid().to_string(),
                origin_machine.as_uuid().to_string(),
            ],
        )?;
    }
    let executor_identity = Store::reject_execution_in(
        &tx,
        &cancellation_tombstone(task_id, origin_machine, authority),
    )?;
    if matches!(executor_identity, ExecutorIdentity::Accepted(_)) {
        return Err(ResourceStoreError::ExecutorAlreadyAccepted { task: task_id });
    }
    tx.commit()?;
    Ok(QueueCancellationResult::PreventedBeforeAcceptance)
}

fn cancel_queued_request(
    tx: &Transaction<'_>,
    saved: &mut ResourceRequest,
    authority: MachineId,
) -> Result<(), ResourceStoreError> {
    let cancelled = ResourceRequestState::CancelledBeforeLaunch;
    let state_json = encode_json(&cancelled)?;
    let updated = tx.execute(
        "UPDATE resource_requests SET state_json = ?1
         WHERE request_id = ?2
           AND json_extract(state_json, '$.type') = 'queued'",
        params![state_json, saved.request_id.0.to_string()],
    )?;
    if updated != 1 {
        return Err(ResourceStoreError::Conflict);
    }

    let executor_identity = Store::reject_execution_in(
        tx,
        &cancellation_tombstone(saved.task_id, saved.origin_machine, authority),
    )?;
    if matches!(executor_identity, ExecutorIdentity::Accepted(_)) {
        return Err(ResourceStoreError::ExecutorAlreadyAccepted {
            task: saved.task_id,
        });
    }

    saved.state = cancelled;
    Ok(())
}

fn cancel_assigned_request(
    tx: &Transaction<'_>,
    saved: &mut ResourceRequest,
    loan_id: LoanId,
    authority: MachineId,
) -> Result<(), ResourceStoreError> {
    if matches!(
        select_executor_identity(tx, saved.task_id)?,
        Some(ExecutorIdentity::Accepted(_))
    ) {
        return Err(ResourceStoreError::ExecutorAlreadyAccepted {
            task: saved.task_id,
        });
    }
    if local_task_exists(tx, saved.task_id)? {
        return Err(ResourceStoreError::Conflict);
    }

    let executor_identity = Store::reject_execution_in(
        tx,
        &cancellation_tombstone(saved.task_id, saved.origin_machine, authority),
    )?;
    if matches!(executor_identity, ExecutorIdentity::Accepted(_)) {
        return Err(ResourceStoreError::ExecutorAlreadyAccepted {
            task: saved.task_id,
        });
    }

    let resource =
        select_resource(tx, saved.resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    let loan =
        select_non_closed_loan(tx, saved.resource_id)?.ok_or(ResourceStoreError::Conflict)?;
    if loan.id != loan_id || loan.resource_id != saved.resource_id {
        return Err(ResourceStoreError::Conflict);
    }
    let (return_context, release_provenance) = match &loan.state {
        LoanState::Active {
            phase:
                LoanPhase::Serving {
                    return_context,
                    current_request_id,
                    release_provenance,
                },
        } if *current_request_id == saved.request_id => {
            (return_context.clone(), release_provenance.clone())
        }
        _ => return Err(ResourceStoreError::Conflict),
    };

    let next_revision = resource
        .state_revision
        .get()
        .checked_add(1)
        .map(ResourceRevision::new)
        .ok_or(ResourceStoreError::Conflict)?;
    let cancelled = ResourceRequestState::CancelledBeforeLaunch;
    let state_json = encode_json(&cancelled)?;
    let changed = tx.execute(
        "UPDATE resource_requests SET state_json = ?1
         WHERE request_id = ?2 AND task_id = ?3 AND resource_id = ?4
           AND json_extract(state_json, '$.type') = 'assigned'
           AND json_extract(state_json, '$.loan_id') = ?5",
        params![
            state_json,
            saved.request_id.0.to_string(),
            saved.task_id.to_string(),
            saved.resource_id.as_uuid().to_string(),
            loan_id.as_uuid().to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict);
    }
    saved.state = cancelled;

    let updated_loan = if let Some(mut next_request) =
        oldest_queued_request_inner(tx, Some(authority), saved.resource_id)?
    {
        next_request.state = ResourceRequestState::Assigned { loan_id };
        let next_state_json = encode_json(&next_request.state)?;
        let changed = tx.execute(
            "UPDATE resource_requests SET state_json = ?1
             WHERE request_id = ?2 AND resource_id = ?3 AND acceptance_sequence = ?4
               AND json_extract(state_json, '$.type') = 'queued'",
            params![
                next_state_json,
                next_request.request_id.0.to_string(),
                saved.resource_id.as_uuid().to_string(),
                sqlite_integer(next_request.acceptance_sequence.get())?,
            ],
        )?;
        if changed != 1 {
            return Err(ResourceStoreError::Conflict);
        }

        Loan {
            id: loan.id,
            resource_id: saved.resource_id,
            state: LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context,
                    current_request_id: next_request.request_id,
                    release_provenance,
                },
            },
        }
    } else {
        let action_id = ActionId::new();
        let updated_loan = Loan {
            id: loan.id,
            resource_id: saved.resource_id,
            state: LoanState::Active {
                phase: LoanPhase::AwaitingReturn {
                    action_id,
                    return_context: return_context.clone(),
                },
            },
        };
        let notice = SupervisorNotice {
            id: NoticeId::new(),
            loan_id: loan.id,
            action_id,
            state_revision: next_revision,
            destination: resource.supervisor,
            assignment_revision: resource.assignment_revision,
            payload: SupervisorNoticePayload::ReturnRequired { return_context },
            delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
        };
        insert_supervisor_notice_in_transaction(tx, &notice).map_err(|error| match error {
            SupervisorNoticeStoreError::Storage(error) => ResourceStoreError::Storage(error),
            _ => ResourceStoreError::Conflict,
        })?;
        updated_loan
    };

    let loan_state_json = encode_json(&updated_loan.state)?;
    let changed = tx.execute(
        "UPDATE loans SET state_json = ?1
         WHERE id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'active'
           AND json_extract(state_json, '$.phase.type') = 'serving'
           AND json_extract(state_json, '$.phase.current_request_id') = ?4",
        params![
            loan_state_json,
            loan.id.as_uuid().to_string(),
            saved.resource_id.as_uuid().to_string(),
            saved.request_id.0.to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict);
    }

    let changed = tx.execute(
        "UPDATE resources SET state_revision = ?1
         WHERE id = ?2 AND authority_machine = ?3 AND state_revision = ?4",
        params![
            sqlite_integer(next_revision.get())?,
            saved.resource_id.as_uuid().to_string(),
            authority.as_uuid().to_string(),
            sqlite_integer(resource.state_revision.get())?,
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict);
    }

    Ok(())
}

/// Read all requests for one resource in authority acceptance order.
pub(crate) fn requests_for_resource_for_authority(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
    requests_for_resource_inner(conn, Some(authority_machine), resource_id)
}

/// Load assigned requests for one authority so task ownership can be checked durably.
pub(crate) fn assigned_resource_requests_for_authority(
    conn: &Connection,
    authority_machine: MachineId,
) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
    let mut statement = conn.prepare(
        "SELECT rr.acceptance_sequence, rr.request_id, rr.task_id, rr.resource_id,
                rr.origin_machine, rr.spec_json, rr.state_json
         FROM resource_requests AS rr
         JOIN resources AS r ON r.id = rr.resource_id
         WHERE r.authority_machine = ?1
           AND json_extract(rr.state_json, '$.type') = 'assigned'
         ORDER BY rr.acceptance_sequence",
    )?;
    let rows = statement.query_map([authority_machine.to_string()], decode_request)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Load each resource owned by one authority with its current non-closed loan.
pub(crate) fn resources_for_authority(
    conn: &Connection,
    authority_machine: MachineId,
) -> Result<Vec<ResourceSnapshot>, ResourceStoreError> {
    let sql = format!(
        "SELECT {RESOURCE_SNAPSHOT_COLUMNS}
         FROM resources AS r
         LEFT JOIN loans AS l
           ON l.resource_id = r.id
         WHERE r.authority_machine = ?1
         ORDER BY r.id, l.id"
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map([authority_machine.to_string()], decode_resource_snapshot)?;
    let mut snapshots: Vec<ResourceSnapshot> = Vec::new();

    for row in rows {
        let mut candidate = row?;
        match snapshots.last_mut() {
            Some(snapshot) if snapshot.resource.id == candidate.resource.id => {
                let Some(loan) = candidate.loan.take() else {
                    continue;
                };
                if matches!(&loan.state, LoanState::Closed { .. }) {
                    continue;
                }
                if snapshot.loan.is_some() {
                    return Err(ResourceStoreError::Storage(stored_value_error(
                        10,
                        Type::Text,
                        std::io::Error::other(format!(
                            "resource {:?} has multiple non-closed loans",
                            snapshot.resource.id
                        )),
                    )));
                }
                snapshot.loan = Some(loan);
            }
            _ => {
                if candidate
                    .loan
                    .as_ref()
                    .is_some_and(|loan| matches!(&loan.state, LoanState::Closed { .. }))
                {
                    candidate.loan = None;
                }
                snapshots.push(candidate);
            }
        }
    }

    Ok(snapshots)
}

#[cfg(test)]
fn requests_for_resource(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
    requests_for_resource_inner(conn, None, resource_id)
}

fn requests_for_resource_inner(
    conn: &Connection,
    authority_machine: Option<MachineId>,
    resource_id: ResourceId,
) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
    check_resource_authority(conn, resource_id, authority_machine)?;
    let sql = format!(
        "SELECT {REQUEST_COLUMNS} FROM resource_requests \
         WHERE resource_id = ?1 ORDER BY acceptance_sequence"
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map([resource_id.as_uuid().to_string()], decode_request)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Read the oldest still-queued request for one resource.
pub(crate) fn oldest_queued_request_for_authority(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
) -> Result<Option<ResourceRequest>, ResourceStoreError> {
    oldest_queued_request_inner(conn, Some(authority_machine), resource_id)
}

#[cfg(test)]
fn oldest_queued_request(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<ResourceRequest>, ResourceStoreError> {
    oldest_queued_request_inner(conn, None, resource_id)
}

fn oldest_queued_request_inner(
    conn: &Connection,
    authority_machine: Option<MachineId>,
    resource_id: ResourceId,
) -> Result<Option<ResourceRequest>, ResourceStoreError> {
    check_resource_authority(conn, resource_id, authority_machine)?;
    let sql = format!(
        "SELECT {REQUEST_COLUMNS} FROM resource_requests \
         WHERE resource_id = ?1 AND json_extract(state_json, '$.type') = 'queued' \
         ORDER BY acceptance_sequence LIMIT 1"
    );
    conn.query_row(&sql, [resource_id.as_uuid().to_string()], decode_request)
        .optional()
        .map_err(Into::into)
}

pub(crate) fn select_resource(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<Resource>, rusqlite::Error> {
    let sql = format!("SELECT {RESOURCE_COLUMNS} FROM resources WHERE id = ?1");
    conn.query_row(&sql, [resource_id.as_uuid().to_string()], decode_resource)
        .optional()
}

pub(crate) fn select_non_closed_loan(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<Loan>, rusqlite::Error> {
    conn.query_row(
        "SELECT id, resource_id, state_json FROM loans
         WHERE resource_id = ?1 AND json_extract(state_json, '$.type') != 'closed'",
        [resource_id.as_uuid().to_string()],
        decode_loan,
    )
    .optional()
}

fn decode_loan(row: &Row<'_>) -> rusqlite::Result<Loan> {
    let id = LoanId(uuid_column(row, 0)?);
    let resource_id = ResourceId::from_uuid(uuid_column(row, 1)?);
    let state_json: String = row.get(2)?;
    let state: LoanState = decode_json(&state_json, 2)?;
    Ok(Loan {
        id,
        resource_id,
        state,
    })
}

fn decode_resource_snapshot(row: &Row<'_>) -> rusqlite::Result<ResourceSnapshot> {
    let resource = decode_resource(row)?;
    let loan_id: Option<String> = row.get(8)?;
    let loan = loan_id
        .map(|loan_id| -> rusqlite::Result<Loan> {
            Ok(Loan {
                id: LoanId(parse_uuid(&loan_id, 8)?),
                resource_id: ResourceId::from_uuid(uuid_column(row, 9)?),
                state: decode_json(&row.get::<_, String>(10)?, 10)?,
            })
        })
        .transpose()?;

    Ok(ResourceSnapshot { resource, loan })
}

pub(crate) fn select_request_by_id(
    conn: &Connection,
    request_id: RequestId,
) -> Result<Option<ResourceRequest>, rusqlite::Error> {
    let sql = format!("SELECT {REQUEST_COLUMNS} FROM resource_requests WHERE request_id = ?1");
    conn.query_row(&sql, [request_id.0.to_string()], decode_request)
        .optional()
}

pub(crate) fn check_resource_authority(
    conn: &Connection,
    resource_id: ResourceId,
    authority_machine: Option<MachineId>,
) -> Result<MachineId, ResourceStoreError> {
    let expected = resource_authority(conn, resource_id)?;
    if let Some(authority_machine) = authority_machine {
        check_authority(expected, authority_machine)?;
    }

    Ok(expected)
}

fn resource_authority(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<MachineId, ResourceStoreError> {
    select_resource(conn, resource_id)?
        .map(|resource| resource.authority_machine())
        .ok_or(ResourceStoreError::ResourceNotFound)
}

fn check_authority(expected: MachineId, found: MachineId) -> Result<(), ResourceStoreError> {
    if expected == found {
        return Ok(());
    }

    Err(ResourceStoreError::WrongAuthority { expected, found })
}

fn cancellation_tombstone(
    task: TaskId,
    origin_machine: MachineId,
    execution_machine: MachineId,
) -> RejectionTombstone {
    RejectionTombstone {
        task,
        origin_machine,
        execution_machine,
        reason: PreAcceptanceRejection::Cancelled.as_str().into(),
    }
}

fn prevention_exists(
    conn: &Connection,
    identity: RequestIdentity,
) -> Result<bool, ResourceStoreError> {
    let sql = format!(
        "SELECT {PREVENTION_COLUMNS} FROM resource_request_preventions \
         WHERE request_id = ?1 OR task_id = ?2"
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(
        params![
            identity.request_id.0.to_string(),
            identity.task_id.to_string()
        ],
        decode_prevention,
    )?;
    let saved = rows.collect::<Result<Vec<_>, _>>()?;
    if saved.iter().any(|saved| *saved != identity) {
        return Err(ResourceStoreError::Conflict);
    }

    Ok(!saved.is_empty())
}

fn request_matches_identity(request: &ResourceRequest, identity: RequestIdentity) -> bool {
    request.request_id == identity.request_id
        && request.task_id == identity.task_id
        && request.resource_id == identity.resource_id
        && request.origin_machine == identity.origin_machine
}

fn task_id_exists(conn: &Connection, task_id: TaskId) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM resource_requests WHERE task_id = ?1)",
        [task_id.to_string()],
        |row| row.get(0),
    )
}

fn executor_identity_exists(conn: &Connection, task_id: TaskId) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM executor_identities WHERE task_id = ?1)",
        [task_id.to_string()],
        |row| row.get(0),
    )
}

fn select_executor_identity(
    conn: &Connection,
    task_id: TaskId,
) -> Result<Option<ExecutorIdentity>, ResourceStoreError> {
    let data: Option<String> = conn
        .query_row(
            "SELECT identity_json FROM executor_identities WHERE task_id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    data.as_deref()
        .map(|data| decode_json(data, 0).map_err(ResourceStoreError::Storage))
        .transpose()
}

fn local_task_exists(conn: &Connection, task_id: TaskId) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE id = ?1)",
        [task_id.to_string()],
        |row| row.get(0),
    )
}

fn local_task_identity_exists(conn: &Connection, task_id: TaskId) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM tasks WHERE id = ?1
            UNION ALL SELECT 1 FROM executor_identities WHERE task_id = ?1
            UNION ALL SELECT 1 FROM resource_requests WHERE task_id = ?1
            UNION ALL SELECT 1 FROM resource_request_preventions WHERE task_id = ?1
            UNION ALL SELECT 1 FROM origin_routes WHERE task_id = ?1
        )",
        [task_id.to_string()],
        |row| row.get(0),
    )
}

fn request_identity_exists(
    conn: &Connection,
    request_id: RequestId,
) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM origin_routes WHERE request_id = ?1
            UNION ALL SELECT 1 FROM resource_requests WHERE request_id = ?1
            UNION ALL SELECT 1 FROM resource_request_preventions WHERE request_id = ?1
        )",
        [request_id.0.to_string()],
        |row| row.get(0),
    )
}

fn release_watcher_identity_is_claimed(
    conn: &Connection,
    loan_id: LoanId,
    request_id: RequestId,
    task_id: TaskId,
) -> Result<bool, ResourceStoreError> {
    let mut statement = conn.prepare(
        "SELECT state_json FROM loans
         WHERE id != ?1
           AND (
               json_type(state_json, '$.phase.watcher_intent') IS NOT NULL
               OR json_type(state_json, '$.last_safe_phase.watcher_intent') IS NOT NULL
           )",
    )?;

    // decode saved bindings so incomplete identities cannot be ignored by uniqueness checks
    let states = statement.query_map([loan_id.as_uuid().to_string()], |row| {
        decode_json(&row.get::<_, String>(0)?, 0)
    })?;

    for state in states {
        let state = state?;
        let intent = match state {
            LoanState::Active {
                phase:
                    LoanPhase::AwaitingRelease {
                        watcher_intent: Some(intent),
                        ..
                    },
            }
            | LoanState::NeedsAttention {
                last_safe_phase:
                    LoanPhase::AwaitingRelease {
                        watcher_intent: Some(intent),
                        ..
                    },
                ..
            } => intent,
            _ => continue,
        };

        let Some(intent) = intent.complete() else {
            return Err(ResourceStoreError::LegacyWatcherIntentUnproven);
        };

        if intent.request_id == request_id || intent.watcher_task_id.as_task_id() == task_id {
            return Ok(true);
        }
    }

    Ok(false)
}

fn decode_resource(row: &Row<'_>) -> rusqlite::Result<Resource> {
    let id = uuid_column(row, 0)?;
    let display_name = row.get(1)?;
    let authority_machine = MachineId::from_uuid(uuid_column(row, 2)?);
    let supervisor_machine = MachineId::from_uuid(uuid_column(row, 3)?);
    let supervisor_thread = ThreadId(uuid_column(row, 4)?);
    let assignment_revision = AssignmentRevision::new(nonnegative_integer(row.get(5)?, 5)?);
    let state_revision = ResourceRevision::new(nonnegative_integer(row.get(6)?, 6)?);
    let registered_background_task = row
        .get::<_, Option<String>>(7)?
        .map(|value| parse_uuid(&value, 7).map(TaskId))
        .transpose()?;

    Ok(Resource::new(
        ResourceId::from_uuid(id),
        display_name,
        authority_machine,
        SupervisorAddress {
            machine: supervisor_machine,
            thread: supervisor_thread,
        },
        assignment_revision,
        state_revision,
        registered_background_task,
    ))
}

fn decode_request(row: &Row<'_>) -> rusqlite::Result<ResourceRequest> {
    let sequence = AcceptanceSequence::new(nonnegative_integer(row.get(0)?, 0)?);
    let request_id = RequestId(uuid_column(row, 1)?);
    let task_id = TaskId(uuid_column(row, 2)?);
    let resource_id = ResourceId::from_uuid(uuid_column(row, 3)?);
    let origin_machine = MachineId::from_uuid(uuid_column(row, 4)?);
    let spec_json: String = row.get(5)?;
    let normalized_spec: NormalizedSpec = decode_json(&spec_json, 5)?;
    let state_json: String = row.get(6)?;
    let state: ResourceRequestState = decode_json(&state_json, 6)?;

    let mut request = ResourceRequest::new(
        request_id,
        task_id,
        resource_id,
        sequence,
        origin_machine,
        normalized_spec,
    )
    .map_err(|err| stored_value_error(5, Type::Text, err))?;
    request.state = state;
    Ok(request)
}

fn decode_prevention(row: &Row<'_>) -> rusqlite::Result<RequestIdentity> {
    Ok(RequestIdentity {
        request_id: RequestId(uuid_column(row, 0)?),
        task_id: TaskId(uuid_column(row, 1)?),
        resource_id: ResourceId::from_uuid(uuid_column(row, 2)?),
        origin_machine: MachineId::from_uuid(uuid_column(row, 3)?),
    })
}

fn uuid_column(row: &Row<'_>, index: usize) -> rusqlite::Result<Uuid> {
    let value: String = row.get(index)?;
    parse_uuid(&value, index)
}

fn parse_uuid(value: &str, index: usize) -> rusqlite::Result<Uuid> {
    Uuid::parse_str(value).map_err(|err| stored_value_error(index, Type::Text, err))
}

fn nonnegative_integer(value: i64, index: usize) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|err| stored_value_error(index, Type::Integer, err))
}

fn sqlite_integer(value: u64) -> Result<i64, ResourceStoreError> {
    i64::try_from(value).map_err(|err| {
        ResourceStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
    })
}

fn encode_json<T: Serialize>(value: &T) -> Result<String, ResourceStoreError> {
    serde_json::to_string(value).map_err(|err| {
        ResourceStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
    })
}

fn encode_supervisor_notice(
    notice: &SupervisorNotice,
) -> Result<String, SupervisorNoticeStoreError> {
    serde_json::to_string(notice).map_err(|err| {
        SupervisorNoticeStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
    })
}

fn decode_json<T: DeserializeOwned>(value: &str, index: usize) -> rusqlite::Result<T> {
    serde_json::from_str(value).map_err(|err| stored_value_error(index, Type::Text, err))
}

fn stored_value_error(
    index: usize,
    column_type: Type,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, column_type, Box::new(error))
}

fn same_spec(left: &NormalizedSpec, right: &NormalizedSpec) -> Result<bool, ResourceStoreError> {
    let left = serde_json::to_value(left).map_err(|err| {
        ResourceStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
    })?;
    let right = serde_json::to_value(right).map_err(|err| {
        ResourceStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
    })?;
    Ok(left == right)
}

#[cfg(test)]
pub(crate) fn seed_verified_serving_provenance_for_test(
    conn: &mut Connection,
    mut loan: Loan,
    authority_machine: MachineId,
    resource_id: ResourceId,
    request_id: RequestId,
    expected_state_revision: ResourceRevision,
    state_revision: ResourceRevision,
) -> Result<Loan, ResourceStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let resource =
        select_resource(&tx, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    let task_id = resource
        .registered_background_task
        .unwrap_or_else(TaskId::new);
    let action_id = ActionId::new();
    let publication_sha256 = "ab".repeat(32);
    let LoanState::Active {
        phase:
            LoanPhase::Serving {
                return_context,
                release_provenance,
                ..
            },
    } = &mut loan.state
    else {
        return Err(ResourceStoreError::Conflict);
    };
    *release_provenance = ServingReleaseProvenance::CompletedTrainerResult {
        action_id,
        task_id,
        publication_sha256,
    };
    let request = select_request_by_id(&tx, request_id)?.ok_or(ResourceStoreError::Conflict)?;
    let receipt = ReleaseCompletionReceipt {
        action_id,
        authority_machine,
        resource_id,
        expected_state_revision,
        return_context: return_context.clone(),
        release_provenance: release_provenance.clone(),
        result: ReleaseCompletionResult::Assigned {
            loan: loan.clone(),
            request,
            state_revision,
        },
    };
    let state_json = encode_json(&loan.state)?;
    let changed = tx.execute(
        "UPDATE loans SET state_json = ?1 WHERE id = ?2 AND resource_id = ?3",
        params![
            state_json,
            loan.id.as_uuid().to_string(),
            resource_id.as_uuid().to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict);
    }
    tx.execute(
        "INSERT INTO resource_release_completions (action_id, receipt_json)
         VALUES (?1, ?2)",
        params![action_id.as_uuid().to_string(), encode_json(&receipt)?,],
    )?;
    tx.commit()?;

    Ok(loan)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use rusqlite::Connection;
    use serde_json::json;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::machine::MachineName;
    use crate::resource::{
        ActionId, DeliveryAttemptId, LoanClosure, LoanId, LoanPhase, LoanState, NoticeId,
        ResourceId, ReturnContext, SupervisorNotice, SupervisorNoticeDelivery,
        SupervisorNoticePayload,
    };
    use crate::submission::ExecutionRecord;

    fn connection() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        install_schema(&mut conn).unwrap();
        conn.execute_batch(
            "CREATE TABLE tasks (
                 id TEXT PRIMARY KEY,
                 status TEXT NOT NULL DEFAULT 'queued'
             );
             CREATE TABLE executor_identities (
                 task_id TEXT PRIMARY KEY,
                 origin_machine TEXT NOT NULL,
                 identity_json TEXT NOT NULL
             );
             CREATE TABLE origin_routes (
                 request_id TEXT PRIMARY KEY,
                 task_id TEXT NOT NULL UNIQUE,
                 execution_machine TEXT NOT NULL,
                 spec_json TEXT NOT NULL,
                 route_json TEXT NOT NULL
             );",
        )
        .unwrap();
        conn
    }

    fn resource() -> Resource {
        resource_for_authority(MachineId::new())
    }

    fn resource_for_authority(authority: MachineId) -> Resource {
        Resource::new(
            ResourceId::new(),
            "gpu-0".into(),
            authority,
            SupervisorAddress {
                machine: authority,
                thread: ThreadId(Uuid::now_v7()),
            },
            AssignmentRevision::new(0),
            ResourceRevision::new(0),
            None,
        )
    }

    fn resource_with_background_task(task_id: TaskId) -> Resource {
        let mut resource = resource();
        resource.registered_background_task = Some(task_id);
        resource
    }

    fn queue_request(conn: &mut Connection, resource_id: ResourceId) -> ResourceRequest {
        accept_request(
            conn,
            RequestId::new(),
            TaskId::new(),
            resource_id,
            MachineId::new(),
            command_spec(&["echo", "queued"]),
        )
        .unwrap()
    }

    fn insert_local_task_status(conn: &Connection, task_id: TaskId, status: &str) {
        conn.execute(
            "INSERT INTO tasks (id, status) VALUES (?1, ?2)",
            params![task_id.to_string(), status],
        )
        .unwrap();
    }

    fn command_spec(command: &[&str]) -> NormalizedSpec {
        serde_json::from_value(json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "gpu command",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": command }
        }))
        .unwrap()
    }

    #[test]
    fn no_child_evidence_proves_release_only_for_pre_spawn_outcomes() {
        let request = ResourceRequest::new(
            RequestId::new(),
            TaskId::new(),
            ResourceId::new(),
            AcceptanceSequence::new(1),
            MachineId::new(),
            command_spec(&["ssh", "gpu-host"]),
        )
        .unwrap();
        let spawn_failed = ExitReason::SpawnFailed {
            message: "fake spawn failure".into(),
        };
        // no child ran, so the command shape cannot have left work behind
        assert_eq!(
            resource_task_release_proof(
                &request,
                &spawn_failed,
                ProcessGroupExitEvidence::NoChildSpawned
            ),
            Ok(ResourceTaskReleaseProof::NoChildSpawnedAfterSpawnFailure)
        );
        assert_eq!(
            resource_task_release_proof(
                &request,
                &ExitReason::Cancelled,
                ProcessGroupExitEvidence::NoChildSpawned
            ),
            Ok(ResourceTaskReleaseProof::NoChildSpawnedAfterQueuedCancel)
        );
        for outcome in [
            ExitReason::Exit { code: 0 },
            ExitReason::Signal { signal: 9 },
        ] {
            assert_eq!(
                resource_task_release_proof(
                    &request,
                    &outcome,
                    ProcessGroupExitEvidence::NoChildSpawned
                ),
                Err(AssignedResourceTaskAttention::InvalidNoChildSpawnEvidence)
            );
        }
    }

    #[test]
    fn assigned_task_release_proof_rejects_commands_that_can_outlive_the_group() {
        for (command, risk) in [
            (
                &["ssh", "gpu-host"][..],
                ResourceTaskOwnershipRisk::RemoteShell,
            ),
            (
                &["docker", "run"][..],
                ResourceTaskOwnershipRisk::ContainerClient,
            ),
            (
                &["sh", "-c", "bench"][..],
                ResourceTaskOwnershipRisk::ShellWrapper,
            ),
            (
                &["setsid", "bench"][..],
                ResourceTaskOwnershipRisk::DetachedLauncher,
            ),
            // wrappers whose own exit hides a detached container or other launch
            (
                &["env", "docker", "run", "-d", "gpu"][..],
                ResourceTaskOwnershipRisk::ProgramLauncher,
            ),
            (
                &["sudo", "/opt/gpu/bench"][..],
                ResourceTaskOwnershipRisk::ProgramLauncher,
            ),
            (
                &["timeout", "1h", "bench"][..],
                ResourceTaskOwnershipRisk::ProgramLauncher,
            ),
            (
                &["/opt/tools/runner", "docker", "run", "-d"][..],
                ResourceTaskOwnershipRisk::ContainerClient,
            ),
        ] {
            // a request saved before the contract existed still cannot release
            let request = ResourceRequest::new(
                RequestId::new(),
                TaskId::new(),
                ResourceId::new(),
                AcceptanceSequence::new(1),
                MachineId::new(),
                command_spec(command),
            )
            .unwrap();
            assert_eq!(
                resource_task_release_proof(
                    &request,
                    &ExitReason::Exit { code: 0 },
                    ProcessGroupExitEvidence::ConfirmedExited,
                ),
                Err(AssignedResourceTaskAttention::OwnershipUncertain(risk))
            );
        }
    }

    fn agent_spec() -> NormalizedSpec {
        serde_json::from_value(json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "agent request",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": {
                "type": "agent",
                "agent": "codex",
                "prompt": "run this agent",
                "report_trailer": true
            }
        }))
        .unwrap()
    }

    fn request_id_at(timestamp_ms: u128) -> RequestId {
        RequestId(Uuid::from_u128(
            (timestamp_ms << 80) | (0x7 << 76) | (0x2 << 62),
        ))
    }

    fn insert_loan(
        conn: &Connection,
        loan_id: LoanId,
        resource_id: ResourceId,
        state: LoanState,
    ) -> rusqlite::Result<()> {
        conn.execute(
            "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
            params![
                loan_id.as_uuid().to_string(),
                resource_id.as_uuid().to_string(),
                serde_json::to_string(&state).unwrap(),
            ],
        )?;
        Ok(())
    }

    fn assign_request_to_serving_loan(
        conn: &Connection,
        request: &ResourceRequest,
        return_context: ReturnContext,
    ) -> Loan {
        let loan_id = LoanId::new();
        let assigned_state = ResourceRequestState::Assigned { loan_id };
        conn.execute(
            "UPDATE resource_requests SET state_json = ?1 WHERE request_id = ?2",
            params![
                serde_json::to_string(&assigned_state).unwrap(),
                request.request_id.0.to_string(),
            ],
        )
        .unwrap();
        let loan = Loan {
            id: loan_id,
            resource_id: request.resource_id,
            state: LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context,
                    current_request_id: request.request_id,
                    release_provenance: ServingReleaseProvenance::Unverified,
                },
            },
        };
        insert_loan(conn, loan.id, loan.resource_id, loan.state.clone()).unwrap();
        loan
    }

    fn insert_accepted_executor_identity(
        conn: &Connection,
        request: &ResourceRequest,
        authority: MachineId,
    ) {
        let identity = ExecutorIdentity::Accepted(ExecutionRecord {
            task: request.task_id,
            origin_machine: request.origin_machine,
            execution_machine: authority,
            spec: request.spec().as_normalized().clone().into(),
            state: crate::domain::ProcessStatus::Queued,
        });
        conn.execute(
            "INSERT INTO executor_identities (task_id, origin_machine, identity_json)
             VALUES (?1, ?2, ?3)",
            params![
                request.task_id.to_string(),
                request.origin_machine.as_uuid().to_string(),
                serde_json::to_string(&identity).unwrap(),
            ],
        )
        .unwrap();
    }

    fn connection_at(path: &Path) -> Connection {
        let mut conn = Connection::open(path).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        install_schema(&mut conn).unwrap();
        conn
    }

    fn persist_notice_for_test(
        conn: &mut Connection,
        notice: &SupervisorNotice,
    ) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let saved = insert_supervisor_notice_in_transaction(&tx, notice)?;
        tx.commit()?;
        Ok(saved)
    }

    fn insert_notice_fixture(conn: &mut Connection) -> SupervisorNotice {
        let resource = resource();
        register_resource(conn, &resource).unwrap();
        let loan_id = LoanId::new();
        let action_id = ActionId::new();
        let task_id = TaskId::new();
        let state = LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task: task_id,
                watcher_intent: None,
            },
        };
        insert_loan(conn, loan_id, resource.id, state).unwrap();

        SupervisorNotice {
            id: NoticeId::new(),
            loan_id,
            action_id,
            state_revision: resource.state_revision,
            destination: resource.supervisor,
            assignment_revision: resource.assignment_revision,
            payload: SupervisorNoticePayload::ReleaseRequired { task_id },
            delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
        }
    }

    #[test]
    fn acceptance_fifo_uses_authority_sequence_not_request_uuid_time() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let origin = MachineId::new();
        let earlier_uuid = request_id_at(1);
        let later_uuid = request_id_at(2);

        let accepted_later_uuid = accept_request(
            &mut conn,
            later_uuid,
            TaskId::new(),
            resource.id,
            origin,
            command_spec(&["echo", "later uuid"]),
        )
        .unwrap();
        let accepted_earlier_uuid = accept_request(
            &mut conn,
            earlier_uuid,
            TaskId::new(),
            resource.id,
            origin,
            command_spec(&["echo", "earlier uuid"]),
        )
        .unwrap();

        assert!(earlier_uuid.0 < later_uuid.0);
        assert!(
            accepted_later_uuid.acceptance_sequence < accepted_earlier_uuid.acceptance_sequence
        );
        let fifo = requests_for_resource(&conn, resource.id).unwrap();
        assert_eq!(fifo[0].request_id, later_uuid);
        assert_eq!(fifo[1].request_id, earlier_uuid);
        assert_eq!(
            oldest_queued_request(&conn, resource.id)
                .unwrap()
                .unwrap()
                .request_id,
            later_uuid
        );
    }

    #[test]
    fn cancellation_before_acceptance_prevents_a_delayed_acceptance() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let request_id = RequestId::new();
        let task_id = TaskId::new();
        let origin = MachineId::new();

        assert!(matches!(
            cancel_request_before_activation(&mut conn, request_id, task_id, resource.id, origin,),
            Ok(QueueCancellationResult::PreventedBeforeAcceptance)
        ));
        assert!(matches!(
            cancel_request_before_activation(&mut conn, request_id, task_id, resource.id, origin,),
            Ok(QueueCancellationResult::PreventedBeforeAcceptance)
        ));
        assert!(matches!(
            accept_request(
                &mut conn,
                request_id,
                task_id,
                resource.id,
                origin,
                command_spec(&["echo", "delayed"]),
            ),
            Err(ResourceStoreError::Prevented)
        ));

        let request_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resource_requests", [], |row| {
                row.get(0)
            })
            .unwrap();
        let prevention_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_request_preventions",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(request_count, 0);
        assert_eq!(prevention_count, 1);

        let first_accepted = accept_request(
            &mut conn,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            origin,
            command_spec(&["echo", "next"]),
        )
        .unwrap();
        assert_eq!(
            first_accepted.acceptance_sequence,
            AcceptanceSequence::new(1)
        );
    }

    #[test]
    fn cancellation_after_acceptance_retains_one_cancelled_row_and_its_sequence() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let request_id = RequestId::new();
        let task_id = TaskId::new();
        let origin = MachineId::new();
        let spec = command_spec(&["echo", "queued"]);
        let accepted = accept_request(
            &mut conn,
            request_id,
            task_id,
            resource.id,
            origin,
            spec.clone(),
        )
        .unwrap();

        let first_cancel =
            cancel_request_before_activation(&mut conn, request_id, task_id, resource.id, origin)
                .unwrap();
        let QueueCancellationResult::Request(cancelled) = first_cancel else {
            panic!("an accepted request must retain its queue row");
        };
        assert!(matches!(
            cancelled.state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert_eq!(cancelled.acceptance_sequence, accepted.acceptance_sequence);

        let retry =
            cancel_request_before_activation(&mut conn, request_id, task_id, resource.id, origin)
                .unwrap();
        let QueueCancellationResult::Request(retried) = retry else {
            panic!("a repeated cancellation must return the saved request");
        };
        assert_eq!(retried.acceptance_sequence, accepted.acceptance_sequence);
        assert!(matches!(
            retried.state,
            ResourceRequestState::CancelledBeforeLaunch
        ));

        let accepted_retry =
            accept_request(&mut conn, request_id, task_id, resource.id, origin, spec).unwrap();
        assert_eq!(
            accepted_retry.acceptance_sequence,
            accepted.acceptance_sequence
        );
        assert!(matches!(
            accepted_retry.state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert_eq!(requests_for_resource(&conn, resource.id).unwrap().len(), 1);
        assert!(oldest_queued_request(&conn, resource.id).unwrap().is_none());

        let prevention_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_request_preventions",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prevention_count, 0);
    }

    #[test]
    fn cancellation_rejects_reuse_of_either_prevented_identity() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let request_id = RequestId::new();
        let task_id = TaskId::new();
        let origin = MachineId::new();
        cancel_request_before_activation(&mut conn, request_id, task_id, resource.id, origin)
            .unwrap();

        assert!(matches!(
            cancel_request_before_activation(
                &mut conn,
                request_id,
                TaskId::new(),
                resource.id,
                origin,
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            cancel_request_before_activation(
                &mut conn,
                RequestId::new(),
                task_id,
                resource.id,
                origin,
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            cancel_request_before_activation(
                &mut conn,
                request_id,
                task_id,
                ResourceId::new(),
                origin,
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            cancel_request_before_activation(
                &mut conn,
                request_id,
                task_id,
                resource.id,
                MachineId::new(),
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            accept_request(
                &mut conn,
                request_id,
                TaskId::new(),
                resource.id,
                origin,
                command_spec(&["echo", "conflict"]),
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            accept_request(
                &mut conn,
                RequestId::new(),
                task_id,
                resource.id,
                origin,
                command_spec(&["echo", "conflict"]),
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            accept_request(
                &mut conn,
                request_id,
                task_id,
                ResourceId::new(),
                origin,
                command_spec(&["echo", "conflict"]),
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            accept_request(
                &mut conn,
                request_id,
                task_id,
                resource.id,
                MachineId::new(),
                command_spec(&["echo", "conflict"]),
            ),
            Err(ResourceStoreError::Conflict)
        ));

        let request_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resource_requests", [], |row| {
                row.get(0)
            })
            .unwrap();
        let prevention_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_request_preventions",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(request_count, 0);
        assert_eq!(prevention_count, 1);
    }

    #[test]
    fn queue_selection_skips_a_cancelled_request() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let origin = MachineId::new();
        let cancelled = accept_request(
            &mut conn,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            origin,
            command_spec(&["echo", "cancelled"]),
        )
        .unwrap();
        let ready = accept_request(
            &mut conn,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            origin,
            command_spec(&["echo", "ready"]),
        )
        .unwrap();

        cancel_request_before_activation(
            &mut conn,
            cancelled.request_id,
            cancelled.task_id,
            cancelled.resource_id,
            cancelled.origin_machine,
        )
        .unwrap();

        let selected = oldest_queued_request(&conn, resource.id).unwrap().unwrap();
        assert_eq!(selected.request_id, ready.request_id);
        assert_eq!(selected.acceptance_sequence, ready.acceptance_sequence);
        let saved = requests_for_resource(&conn, resource.id).unwrap();
        assert_eq!(saved.len(), 2);
        assert!(matches!(
            saved[0].state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert!(matches!(saved[1].state, ResourceRequestState::Queued));
    }

    #[test]
    fn cancellation_preserves_assigned_state_when_executor_won_and_terminal_states() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let origin = MachineId::new();
        let assigned = accept_request(
            &mut conn,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            origin,
            command_spec(&["echo", "assigned"]),
        )
        .unwrap();
        let loan = assign_request_to_serving_loan(
            &conn,
            &assigned,
            ReturnContext::AlreadyCompleted {
                task_id: TaskId::new(),
                result_ref: "saved-result".into(),
            },
        );
        insert_accepted_executor_identity(&conn, &assigned, resource.authority_machine());

        let result = cancel_request_before_activation(
            &mut conn,
            assigned.request_id,
            assigned.task_id,
            assigned.resource_id,
            assigned.origin_machine,
        );
        assert!(matches!(
            result,
            Err(ResourceStoreError::ExecutorAlreadyAccepted { task }) if task == assigned.task_id
        ));
        let saved_assigned = select_request_by_id(&conn, assigned.request_id)
            .unwrap()
            .unwrap();
        assert!(matches!(
            saved_assigned.state,
            ResourceRequestState::Assigned { loan_id: saved } if saved == loan.id
        ));
        assert_eq!(
            select_non_closed_loan(&conn, resource.id).unwrap(),
            Some(loan.clone())
        );
        assert_eq!(
            select_resource(&conn, resource.id)
                .unwrap()
                .unwrap()
                .state_revision,
            resource.state_revision
        );
        conn.execute(
            "DELETE FROM executor_identities WHERE task_id = ?1",
            [assigned.task_id.to_string()],
        )
        .unwrap();
        insert_local_task_status(&conn, assigned.task_id, "queued");
        assert!(matches!(
            cancel_request_before_activation(
                &mut conn,
                assigned.request_id,
                assigned.task_id,
                assigned.resource_id,
                assigned.origin_machine,
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            select_request_by_id(&conn, assigned.request_id)
                .unwrap()
                .unwrap()
                .state,
            ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
        ));
        assert_eq!(
            select_non_closed_loan(&conn, resource.id).unwrap(),
            Some(loan.clone())
        );
        let notice_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_supervisor_notices",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(notice_count, 0);

        let finished = accept_request(
            &mut conn,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            origin,
            command_spec(&["echo", "finished"]),
        )
        .unwrap();
        let finished_state = ResourceRequestState::Finished {
            outcome: crate::domain::ExitReason::Cancelled,
        };
        conn.execute(
            "UPDATE resource_requests SET state_json = ?1 WHERE request_id = ?2",
            params![
                serde_json::to_string(&finished_state).unwrap(),
                finished.request_id.0.to_string(),
            ],
        )
        .unwrap();

        let result = cancel_request_before_activation(
            &mut conn,
            finished.request_id,
            finished.task_id,
            finished.resource_id,
            finished.origin_machine,
        )
        .unwrap();
        let QueueCancellationResult::Request(saved_finished) = result else {
            panic!("a terminal request must remain in the queue store");
        };
        assert!(matches!(
            saved_finished.state,
            ResourceRequestState::Finished {
                outcome: crate::domain::ExitReason::Cancelled
            }
        ));
    }

    #[test]
    fn assigned_cancellation_assigns_oldest_queued_request_to_the_same_loan() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let cancelled = queue_request(&mut conn, resource.id);
        let next = queue_request(&mut conn, resource.id);
        let later = queue_request(&mut conn, resource.id);
        let return_context = ReturnContext::Stopped {
            task_id: TaskId::new(),
            checkpoint_ref: "checkpoint-9".into(),
            recovery_ref: "recovery-9".into(),
        };
        let loan = assign_request_to_serving_loan(&conn, &cancelled, return_context.clone());

        let result = cancel_request_before_activation_for_authority(
            &mut conn,
            resource.authority_machine(),
            cancelled.request_id,
            cancelled.task_id,
            cancelled.resource_id,
            cancelled.origin_machine,
        )
        .unwrap();
        let QueueCancellationResult::Request(cancelled) = result else {
            panic!("an assigned request must retain its row");
        };
        assert!(matches!(
            cancelled.state,
            ResourceRequestState::CancelledBeforeLaunch
        ));

        let saved = requests_for_resource(&conn, resource.id).unwrap();
        assert!(matches!(
            saved[1].state,
            ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
        ));
        assert_eq!(saved[1].request_id, next.request_id);
        assert!(matches!(saved[2].state, ResourceRequestState::Queued));
        assert_eq!(saved[2].request_id, later.request_id);
        let saved_loan = select_non_closed_loan(&conn, resource.id).unwrap().unwrap();
        assert_eq!(
            saved_loan.state,
            LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context,
                    current_request_id: next.request_id,
                    release_provenance: ServingReleaseProvenance::Unverified,
                },
            }
        );
        assert_eq!(saved_loan.id, loan.id);
        assert_eq!(
            select_resource(&conn, resource.id)
                .unwrap()
                .unwrap()
                .state_revision,
            ResourceRevision::new(1)
        );
        let identity_json: String = conn
            .query_row(
                "SELECT identity_json FROM executor_identities WHERE task_id = ?1",
                [cancelled.task_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(matches!(
            serde_json::from_str::<ExecutorIdentity>(&identity_json).unwrap(),
            ExecutorIdentity::Rejected(RejectionTombstone { task, .. }) if task == cancelled.task_id
        ));
    }

    #[test]
    fn assigned_cancellation_with_empty_queue_persists_return_notice() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let assigned = queue_request(&mut conn, resource.id);
        let return_context = ReturnContext::AlreadyCompleted {
            task_id: TaskId::new(),
            result_ref: "final-result-51".into(),
        };
        let loan = assign_request_to_serving_loan(&conn, &assigned, return_context.clone());

        let result = cancel_request_before_activation_for_authority(
            &mut conn,
            resource.authority_machine(),
            assigned.request_id,
            assigned.task_id,
            assigned.resource_id,
            assigned.origin_machine,
        )
        .unwrap();
        assert!(matches!(
            result,
            QueueCancellationResult::Request(request)
                if matches!(request.state, ResourceRequestState::CancelledBeforeLaunch)
        ));

        let saved_loan = select_non_closed_loan(&conn, resource.id).unwrap().unwrap();
        let LoanState::Active {
            phase:
                LoanPhase::AwaitingReturn {
                    action_id,
                    return_context: saved_context,
                },
        } = saved_loan.state
        else {
            panic!("an empty queue must reserve the return decision");
        };
        assert_eq!(saved_loan.id, loan.id);
        assert_eq!(saved_context, return_context);
        assert_eq!(
            select_resource(&conn, resource.id)
                .unwrap()
                .unwrap()
                .state_revision,
            ResourceRevision::new(1)
        );
        let notice = select_supervisor_notice_record_by_action(&conn, action_id)
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(notice.loan_id, loan.id);
        assert_eq!(notice.action_id, action_id);
        assert_eq!(notice.state_revision, ResourceRevision::new(1));
        assert_eq!(notice.destination, resource.supervisor);
        assert_eq!(notice.assignment_revision, resource.assignment_revision);
        assert_eq!(
            notice.payload,
            SupervisorNoticePayload::ReturnRequired { return_context }
        );
        assert_eq!(
            notice.delivery,
            SupervisorNoticeDelivery::Pending { attempts: 0 }
        );
    }

    #[test]
    fn retrying_assigned_cancellation_does_not_advance_queue_or_add_a_notice() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let assigned = queue_request(&mut conn, resource.id);
        let return_context = ReturnContext::AlreadyCompleted {
            task_id: TaskId::new(),
            result_ref: "final-result-52".into(),
        };
        assign_request_to_serving_loan(&conn, &assigned, return_context);
        let first = cancel_request_before_activation(
            &mut conn,
            assigned.request_id,
            assigned.task_id,
            assigned.resource_id,
            assigned.origin_machine,
        )
        .unwrap();
        let QueueCancellationResult::Request(first_request) = first else {
            panic!("an assigned request must retain its row");
        };
        assert!(matches!(
            first_request.state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        let first_loan = select_non_closed_loan(&conn, resource.id).unwrap().unwrap();
        let first_notice_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_supervisor_notices",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(first_notice_count, 1);

        let later = queue_request(&mut conn, resource.id);
        let retry = cancel_request_before_activation(
            &mut conn,
            assigned.request_id,
            assigned.task_id,
            assigned.resource_id,
            assigned.origin_machine,
        )
        .unwrap();
        let QueueCancellationResult::Request(retried_request) = retry else {
            panic!("a repeated cancellation must retain the saved row");
        };
        assert!(matches!(
            retried_request.state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert_eq!(
            select_non_closed_loan(&conn, resource.id).unwrap().unwrap(),
            first_loan
        );
        assert!(matches!(
            select_request_by_id(&conn, later.request_id)
                .unwrap()
                .unwrap()
                .state,
            ResourceRequestState::Queued
        ));
        let notice_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_supervisor_notices",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(notice_count, 1);
        assert_eq!(
            select_resource(&conn, resource.id)
                .unwrap()
                .unwrap()
                .state_revision,
            ResourceRevision::new(1)
        );
    }

    #[test]
    fn assigned_cancellation_refuses_a_loan_needing_attention() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let assigned = queue_request(&mut conn, resource.id);
        let return_context = ReturnContext::AlreadyCompleted {
            task_id: TaskId::new(),
            result_ref: "final-result-53".into(),
        };
        let loan = assign_request_to_serving_loan(&conn, &assigned, return_context.clone());
        let attention_state = LoanState::NeedsAttention {
            action_id: ActionId::new(),
            last_safe_phase: LoanPhase::Serving {
                return_context,
                current_request_id: assigned.request_id,
                release_provenance: ServingReleaseProvenance::Unverified,
            },
            reason: "executor state is uncertain".into(),
        };
        conn.execute(
            "UPDATE loans SET state_json = ?1 WHERE id = ?2",
            params![
                serde_json::to_string(&attention_state).unwrap(),
                loan.id.as_uuid().to_string(),
            ],
        )
        .unwrap();

        assert!(matches!(
            cancel_request_before_activation(
                &mut conn,
                assigned.request_id,
                assigned.task_id,
                assigned.resource_id,
                assigned.origin_machine,
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            select_request_by_id(&conn, assigned.request_id)
                .unwrap()
                .unwrap()
                .state,
            ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
        ));
        assert_eq!(
            select_non_closed_loan(&conn, resource.id)
                .unwrap()
                .unwrap()
                .state,
            attention_state
        );
        assert!(!executor_identity_exists(&conn, assigned.task_id).unwrap());
        assert_eq!(
            select_resource(&conn, resource.id)
                .unwrap()
                .unwrap()
                .state_revision,
            resource.state_revision
        );
    }

    #[test]
    fn identical_retry_returns_saved_request_after_its_state_changes() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let request_id = RequestId::new();
        let task_id = TaskId::new();
        let origin = MachineId::new();
        let spec = command_spec(&["echo", "hello"]);
        let first = accept_request(
            &mut conn,
            request_id,
            task_id,
            resource.id,
            origin,
            spec.clone(),
        )
        .unwrap();
        let changed_state = ResourceRequestState::Assigned {
            loan_id: LoanId::new(),
        };
        conn.execute(
            "UPDATE resource_requests SET state_json = ?1 WHERE request_id = ?2",
            params![
                serde_json::to_string(&changed_state).unwrap(),
                request_id.0.to_string(),
            ],
        )
        .unwrap();

        let retry =
            accept_request(&mut conn, request_id, task_id, resource.id, origin, spec).unwrap();
        assert_eq!(retry.acceptance_sequence, first.acceptance_sequence);
        assert_eq!(retry.state, changed_state);
        assert_eq!(requests_for_resource(&conn, resource.id).unwrap().len(), 1);
        assert!(oldest_queued_request(&conn, resource.id).unwrap().is_none());
    }

    #[test]
    fn conflicting_retry_and_task_uuid_reuse_are_rejected() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let request_id = RequestId::new();
        let task_id = TaskId::new();
        let origin = MachineId::new();
        accept_request(
            &mut conn,
            request_id,
            task_id,
            resource.id,
            origin,
            command_spec(&["echo", "first"]),
        )
        .unwrap();

        assert!(matches!(
            accept_request(
                &mut conn,
                request_id,
                task_id,
                resource.id,
                origin,
                command_spec(&["echo", "changed"]),
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            accept_request(
                &mut conn,
                RequestId::new(),
                task_id,
                resource.id,
                origin,
                command_spec(&["echo", "first"]),
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert_eq!(requests_for_resource(&conn, resource.id).unwrap().len(), 1);
    }

    #[test]
    fn agent_workloads_are_rejected_before_a_request_is_written() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();

        assert!(matches!(
            accept_request(
                &mut conn,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                MachineId::new(),
                agent_spec(),
            ),
            Err(ResourceStoreError::InvalidCommandSpec(
                CommandSpecError::AgentWorkload
            ))
        ));
        assert!(
            requests_for_resource(&conn, resource.id)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn explicit_machine_is_rejected_before_a_request_is_written() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let mut spec = command_spec(&["echo", "gpu"]);
        spec.machine = Some(MachineName::parse("other").unwrap());

        assert!(matches!(
            accept_request(
                &mut conn,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                MachineId::new(),
                spec,
            ),
            Err(ResourceStoreError::InvalidCommandSpec(
                CommandSpecError::ExplicitMachine
            ))
        ));
        assert!(
            requests_for_resource(&conn, resource.id)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn request_for_an_unknown_resource_returns_a_typed_error() {
        let mut conn = connection();

        assert!(matches!(
            accept_request(
                &mut conn,
                RequestId::new(),
                TaskId::new(),
                ResourceId::new(),
                MachineId::new(),
                command_spec(&["echo", "hello"]),
            ),
            Err(ResourceStoreError::ResourceNotFound)
        ));
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resource_requests", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn resource_registration_is_idempotent_and_does_not_replace_content() {
        let mut conn = connection();
        let resource = resource();

        assert_eq!(register_resource(&mut conn, &resource).unwrap(), resource);
        assert_eq!(register_resource(&mut conn, &resource).unwrap(), resource);
        let mut conflicting = resource.clone();
        conflicting.display_name = "replacement".into();
        assert!(matches!(
            register_resource(&mut conn, &conflicting),
            Err(ResourceStoreError::RegistrationConflict { .. })
        ));
        let conflicting_authority = Resource::new(
            resource.id,
            resource.display_name.clone(),
            MachineId::new(),
            resource.supervisor,
            resource.assignment_revision,
            resource.state_revision,
            resource.registered_background_task,
        );
        assert!(matches!(
            register_resource(&mut conn, &conflicting_authority),
            Err(ResourceStoreError::RegistrationConflict { .. })
        ));
        assert_eq!(select_resource(&conn, resource.id).unwrap(), Some(resource));
    }

    #[test]
    fn partial_index_allows_only_one_non_closed_loan_per_resource() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let closed = LoanState::Closed {
            result: LoanClosure::NoResume {
                return_context: ReturnContext::Idle,
                reason: "completed".into(),
            },
        };
        insert_loan(&conn, LoanId::new(), resource.id, closed).unwrap();

        let active = LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id: ActionId::new(),
                observed_background_task: TaskId::new(),
                watcher_intent: None,
            },
        };
        insert_loan(&conn, LoanId::new(), resource.id, active.clone()).unwrap();
        assert!(insert_loan(&conn, LoanId::new(), resource.id, active).is_err());
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM loans WHERE resource_id = ?1",
                [resource.id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn authority_snapshot_restores_only_owned_non_closed_loans_in_stable_order() {
        let mut conn = connection();
        let authority = MachineId::new();
        let other_authority = MachineId::new();
        let active_resource = resource_for_authority(authority);
        let closed_resource = resource_for_authority(authority);
        let empty_resource = resource_for_authority(authority);
        let foreign_resource = resource_for_authority(other_authority);

        for resource in [
            &active_resource,
            &closed_resource,
            &empty_resource,
            &foreign_resource,
        ] {
            register_resource(&mut conn, resource).unwrap();
        }

        let active_loan_id = LoanId::new();
        let active_state = LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id: ActionId::new(),
                observed_background_task: TaskId::new(),
                watcher_intent: None,
            },
        };
        let active_loan = Loan {
            id: active_loan_id,
            resource_id: active_resource.id,
            state: active_state.clone(),
        };
        insert_loan(
            &conn,
            active_loan_id,
            active_resource.id,
            active_loan.state.clone(),
        )
        .unwrap();
        insert_loan(
            &conn,
            LoanId::new(),
            closed_resource.id,
            LoanState::Closed {
                result: LoanClosure::NoResume {
                    return_context: ReturnContext::Idle,
                    reason: "completed".into(),
                },
            },
        )
        .unwrap();
        insert_loan(&conn, LoanId::new(), foreign_resource.id, active_state).unwrap();

        let snapshots = resources_for_authority(&conn, authority).unwrap();
        let mut expected_ids = vec![active_resource.id, closed_resource.id, empty_resource.id];
        expected_ids.sort_by_key(ResourceId::as_uuid);
        assert_eq!(
            snapshots
                .iter()
                .map(|snapshot| snapshot.resource.id)
                .collect::<Vec<_>>(),
            expected_ids
        );
        assert_eq!(
            snapshots
                .iter()
                .find(|snapshot| snapshot.resource.id == active_resource.id)
                .unwrap()
                .loan,
            Some(active_loan)
        );
        for resource_id in [closed_resource.id, empty_resource.id] {
            assert_eq!(
                snapshots
                    .iter()
                    .find(|snapshot| snapshot.resource.id == resource_id)
                    .unwrap()
                    .loan,
                None
            );
        }
        assert!(
            snapshots
                .iter()
                .all(|snapshot| snapshot.resource.id != foreign_resource.id)
        );

        let malformed_resource = resource_for_authority(authority);
        register_resource(&mut conn, &malformed_resource).unwrap();
        conn.execute(
            "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
            params![
                LoanId::new().as_uuid().to_string(),
                malformed_resource.id.as_uuid().to_string(),
                r#"{"type":"closed"}"#,
            ],
        )
        .unwrap();

        assert!(matches!(
            resources_for_authority(&conn, authority),
            Err(ResourceStoreError::Storage(_))
        ));
    }

    #[test]
    fn release_loan_requires_a_queued_request() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        insert_local_task_status(&conn, background_task, "running");

        assert!(matches!(
            open_release_loan_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                resource.state_revision,
            ),
            Err(OpenReleaseLoanError::NoQueuedRequest)
        ));
        assert_eq!(select_resource(&conn, resource.id).unwrap(), Some(resource));
        let loan_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
            .unwrap();
        assert_eq!(loan_count, 0);
    }

    #[test]
    fn queue_reconciliation_opens_one_release_action_and_keeps_fifo_requests_queued() {
        let mut conn = connection();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let mut resource = resource_for_authority(authority);
        resource.registered_background_task = Some(background_task);
        register_resource_for_authority(&mut conn, authority, &resource).unwrap();
        insert_local_task_status(&conn, background_task, "running");

        let first = accept_request_for_authority(
            &mut conn,
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            command_spec(&["echo", "first"]),
        )
        .unwrap();
        let second = accept_request_for_authority(
            &mut conn,
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            command_spec(&["echo", "second"]),
        )
        .unwrap();

        let outcome =
            reconcile_resource_queue_for_authority(&mut conn, authority, resource.id).unwrap();
        let ResourceQueueReconcileOutcome::ReleaseRequired { loan, notice } = outcome else {
            panic!("running background work must reserve one release action");
        };
        assert_eq!(notice.loan_id, loan.id);
        assert_eq!(
            loan.state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease {
                    action_id: notice.action_id,
                    observed_background_task: background_task,
                    watcher_intent: None,
                },
            }
        );

        let repeated =
            reconcile_resource_queue_for_authority(&mut conn, authority, resource.id).unwrap();
        assert!(matches!(
            repeated,
            ResourceQueueReconcileOutcome::LoanAlreadyActive { loan: repeated_loan }
                if repeated_loan.id == loan.id && repeated_loan.state == loan.state
        ));
        assert_eq!(
            select_non_closed_loan(&conn, resource.id).unwrap(),
            Some(loan.clone())
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM resource_supervisor_notices",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            1
        );
        let requests = requests_for_resource_for_authority(&conn, authority, resource.id).unwrap();
        assert_eq!(requests[0].request_id, first.request_id);
        assert_eq!(requests[1].request_id, second.request_id);
        assert!(
            requests
                .iter()
                .all(|request| matches!(request.state, ResourceRequestState::Queued))
        );
    }

    #[test]
    fn queue_reconciliation_reports_fifo_and_does_not_infer_idle_from_missing_background() {
        let mut conn = connection();
        let authority = MachineId::new();
        let resource = resource_for_authority(authority);
        register_resource_for_authority(&mut conn, authority, &resource).unwrap();
        let first = accept_request_for_authority(
            &mut conn,
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            command_spec(&["echo", "first"]),
        )
        .unwrap();
        let second = accept_request_for_authority(
            &mut conn,
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            command_spec(&["echo", "second"]),
        )
        .unwrap();

        let outcome =
            reconcile_resource_queue_for_authority(&mut conn, authority, resource.id).unwrap();
        assert!(matches!(
            outcome,
            ResourceQueueReconcileOutcome::AttentionRequired {
                request,
                reason: ResourceQueueAttentionReason::IdleNotProven {
                    gap: crate::resource::IdleProofGap::NoIdleEvidence,
                },
            } if request.request_id == first.request_id
        ));
        assert!(
            select_non_closed_loan(&conn, resource.id)
                .unwrap()
                .is_none()
        );
        let requests = requests_for_resource_for_authority(&conn, authority, resource.id).unwrap();
        assert_eq!(requests[0].request_id, first.request_id);
        assert_eq!(requests[1].request_id, second.request_id);
        assert!(
            requests
                .iter()
                .all(|request| matches!(request.state, ResourceRequestState::Queued))
        );
    }

    #[test]
    fn queue_reconciliation_requires_attention_for_missing_or_uncertain_background_state() {
        for (task_status, expected_reason) in [
            (
                None,
                ResourceQueueAttentionReason::BackgroundTaskMissing {
                    task_id: TaskId::new(),
                },
            ),
            (
                Some("lost"),
                ResourceQueueAttentionReason::BackgroundTaskNotRunning {
                    task_id: TaskId::new(),
                    state: "lost".into(),
                },
            ),
        ] {
            let mut conn = connection();
            let authority = MachineId::new();
            let background_task = match &expected_reason {
                ResourceQueueAttentionReason::BackgroundTaskMissing { task_id }
                | ResourceQueueAttentionReason::BackgroundTaskNotRunning { task_id, .. } => {
                    *task_id
                }
                ResourceQueueAttentionReason::IdleNotProven { .. }
                | ResourceQueueAttentionReason::BackgroundLaunchPending { .. }
                | ResourceQueueAttentionReason::AcceptedTaskLaunchUncertain { .. }
                | ResourceQueueAttentionReason::UnverifiedServingRelease
                | ResourceQueueAttentionReason::ReleaseProofUnavailable { .. }
                | ResourceQueueAttentionReason::AssignedTaskLaunchUncertain { .. }
                | ResourceQueueAttentionReason::AssignedTaskLost { .. }
                | ResourceQueueAttentionReason::AssignedTaskExitUnconfirmed { .. }
                | ResourceQueueAttentionReason::AssignedTaskIdentityMismatch { .. }
                | ResourceQueueAttentionReason::AssignedTaskNoChildSpawnProofInvalid { .. }
                | ResourceQueueAttentionReason::AssignedTaskOwnershipUncertain { .. }
                | ResourceQueueAttentionReason::AssignedTaskStaleRevision { .. }
                | ResourceQueueAttentionReason::AssignedTaskReconcileFailed { .. } => {
                    unreachable!()
                }
            };
            let mut resource = resource_for_authority(authority);
            resource.registered_background_task = Some(background_task);
            register_resource_for_authority(&mut conn, authority, &resource).unwrap();
            if let Some(status) = task_status {
                insert_local_task_status(&conn, background_task, status);
            }
            let request = accept_request_for_authority(
                &mut conn,
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                MachineId::new(),
                command_spec(&["echo", "queued"]),
            )
            .unwrap();

            let outcome =
                reconcile_resource_queue_for_authority(&mut conn, authority, resource.id).unwrap();
            assert!(matches!(
                outcome,
                ResourceQueueReconcileOutcome::AttentionRequired {
                    request: saved_request,
                    reason,
                } if saved_request.request_id == request.request_id
                    && reason == expected_reason
            ));
            assert!(
                select_non_closed_loan(&conn, resource.id)
                    .unwrap()
                    .is_none()
            );
            assert!(matches!(
                oldest_queued_request_for_authority(&conn, authority, resource.id)
                    .unwrap()
                    .unwrap()
                    .state,
                ResourceRequestState::Queued
            ));
        }
    }

    #[test]
    fn release_loan_rejects_the_wrong_authority() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();

        assert!(matches!(
            open_release_loan_for_authority(
                &mut conn,
                MachineId::new(),
                resource.id,
                resource.state_revision,
            ),
            Err(OpenReleaseLoanError::Resource(
                ResourceStoreError::WrongAuthority { expected, found }
            )) if expected == resource.authority_machine() && found != expected
        ));
    }

    #[test]
    fn release_loan_rejects_a_stale_resource_revision() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        queue_request(&mut conn, resource.id);
        insert_local_task_status(&conn, background_task, "running");

        assert!(matches!(
            open_release_loan_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                ResourceRevision::new(1),
            ),
            Err(OpenReleaseLoanError::StaleRevision {
                expected,
                actual,
            })
                if expected == ResourceRevision::new(1)
                    && actual == ResourceRevision::new(0)
        ));
        assert!(
            select_non_closed_loan(&conn, resource.id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn release_loan_requires_a_registered_background_task() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        queue_request(&mut conn, resource.id);

        assert!(matches!(
            open_release_loan_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                resource.state_revision,
            ),
            Err(OpenReleaseLoanError::BackgroundTaskNotRegistered)
        ));
        assert!(
            select_non_closed_loan(&conn, resource.id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn release_loan_requires_the_registered_background_task_row() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        queue_request(&mut conn, resource.id);

        assert!(matches!(
            open_release_loan_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                resource.state_revision,
            ),
            Err(OpenReleaseLoanError::BackgroundTaskMissing { task_id })
                if task_id == background_task
        ));
        assert!(
            select_non_closed_loan(&conn, resource.id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn release_loan_requires_the_registered_background_task_to_be_running() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        queue_request(&mut conn, resource.id);
        insert_local_task_status(&conn, background_task, "queued");

        assert!(matches!(
            open_release_loan_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                resource.state_revision,
            ),
            Err(OpenReleaseLoanError::BackgroundTaskNotRunning { task_id, state })
                if task_id == background_task && state == "queued"
        ));
        assert!(
            select_non_closed_loan(&conn, resource.id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn release_loan_opens_with_one_matching_pending_notice_and_revision() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        let request = queue_request(&mut conn, resource.id);
        insert_local_task_status(&conn, background_task, "running");

        let result = open_release_loan_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            resource.state_revision,
        )
        .unwrap();
        let OpenReleaseLoanResult::Opened { loan, notice } = result else {
            panic!("first call must open a new release loan");
        };
        let LoanState::Active {
            phase:
                LoanPhase::AwaitingRelease {
                    action_id,
                    observed_background_task,
                    ..
                },
        } = loan.state
        else {
            panic!("new release loan must await release");
        };

        assert_eq!(observed_background_task, background_task);
        assert_eq!(notice.loan_id, loan.id);
        assert_eq!(notice.action_id, action_id);
        assert_eq!(notice.state_revision, ResourceRevision::new(1));
        assert_eq!(notice.destination, resource.supervisor);
        assert_eq!(notice.assignment_revision, resource.assignment_revision);
        assert_eq!(
            notice.payload,
            SupervisorNoticePayload::ReleaseRequired {
                task_id: background_task,
            }
        );
        assert_eq!(
            notice.delivery,
            SupervisorNoticeDelivery::Pending { attempts: 0 }
        );

        let saved_resource = select_resource(&conn, resource.id).unwrap().unwrap();
        assert_eq!(saved_resource.state_revision, ResourceRevision::new(1));
        let saved_loan = select_non_closed_loan(&conn, resource.id).unwrap().unwrap();
        assert_eq!(saved_loan, loan);
        assert_eq!(
            oldest_queued_request(&conn, resource.id)
                .unwrap()
                .map(|queued| queued.request_id),
            Some(request.request_id)
        );
        let loan_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
            .unwrap();
        let notice_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_supervisor_notices",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(loan_count, 1);
        assert_eq!(notice_count, 1);
    }

    #[test]
    fn release_loan_retry_returns_the_saved_action_after_queue_cancellation() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        let request = queue_request(&mut conn, resource.id);
        insert_local_task_status(&conn, background_task, "running");

        let first = open_release_loan_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            resource.state_revision,
        )
        .unwrap();
        let OpenReleaseLoanResult::Opened {
            loan: first_loan,
            notice: first_notice,
        } = first
        else {
            panic!("first call must open a new release loan");
        };
        cancel_request_before_activation(
            &mut conn,
            request.request_id,
            request.task_id,
            request.resource_id,
            request.origin_machine,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status = 'succeeded' WHERE id = ?1",
            [background_task.to_string()],
        )
        .unwrap();

        let retry = open_release_loan_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            resource.state_revision,
        )
        .unwrap();
        assert_eq!(
            retry,
            OpenReleaseLoanResult::AlreadyAwaitingRelease {
                loan: first_loan,
                notice: first_notice,
            }
        );
        assert!(oldest_queued_request(&conn, resource.id).unwrap().is_none());
        let loan_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
            .unwrap();
        let notice_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_supervisor_notices",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(loan_count, 1);
        assert_eq!(notice_count, 1);
    }

    #[test]
    fn release_loan_does_not_replace_another_non_closed_loan() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        queue_request(&mut conn, resource.id);
        insert_local_task_status(&conn, background_task, "running");
        let existing_loan = Loan {
            id: LoanId::new(),
            resource_id: resource.id,
            state: LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context: ReturnContext::Idle,
                    current_request_id: RequestId::new(),
                    release_provenance: ServingReleaseProvenance::Unverified,
                },
            },
        };
        insert_loan(
            &conn,
            existing_loan.id,
            resource.id,
            existing_loan.state.clone(),
        )
        .unwrap();

        assert!(matches!(
            open_release_loan_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                resource.state_revision,
            ),
            Err(OpenReleaseLoanError::ExistingLoan { loan }) if *loan == existing_loan
        ));
        assert_eq!(
            select_resource(&conn, resource.id)
                .unwrap()
                .unwrap()
                .state_revision,
            resource.state_revision
        );
        let loan_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
            .unwrap();
        assert_eq!(loan_count, 1);
    }

    #[test]
    fn release_loan_notice_failure_rolls_back_loan_and_resource_revision() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        let request = queue_request(&mut conn, resource.id);
        insert_local_task_status(&conn, background_task, "running");
        conn.execute_batch(
            "CREATE TRIGGER reject_release_notice
             BEFORE INSERT ON resource_supervisor_notices
             BEGIN
                 SELECT RAISE(ABORT, 'injected notice insert failure');
             END;",
        )
        .unwrap();

        assert!(matches!(
            open_release_loan_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                resource.state_revision,
            ),
            Err(OpenReleaseLoanError::Notice(
                SupervisorNoticeStoreError::Storage(_)
            ))
        ));
        assert_eq!(
            select_resource(&conn, resource.id)
                .unwrap()
                .unwrap()
                .state_revision,
            resource.state_revision
        );
        assert!(
            select_non_closed_loan(&conn, resource.id)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            oldest_queued_request(&conn, resource.id)
                .unwrap()
                .map(|queued| queued.request_id),
            Some(request.request_id)
        );
        let loan_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
            .unwrap();
        let notice_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_supervisor_notices",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(loan_count, 0);
        assert_eq!(notice_count, 0);
    }

    #[test]
    fn notice_identity_is_unique_per_action_and_retries_detect_conflicts() {
        let mut conn = connection();
        let notice = insert_notice_fixture(&mut conn);

        let saved = persist_notice_for_test(&mut conn, &notice).unwrap();
        assert_eq!(saved, notice);
        assert_eq!(persist_notice_for_test(&mut conn, &notice).unwrap(), notice);

        let attempt_id = DeliveryAttemptId::new();
        let reserved = reserve_supervisor_notice_attempt(&mut conn, notice.id, attempt_id).unwrap();
        assert!(matches!(
            reserved.delivery,
            SupervisorNoticeDelivery::Sending { .. }
        ));
        assert_eq!(
            persist_notice_for_test(&mut conn, &notice).unwrap(),
            reserved
        );

        let mut action_conflict = notice.clone();
        action_conflict.id = NoticeId::new();
        assert!(matches!(
            persist_notice_for_test(&mut conn, &action_conflict),
            Err(SupervisorNoticeStoreError::Conflict)
        ));

        let mut content_conflict = notice.clone();
        content_conflict.payload = SupervisorNoticePayload::AttentionRequired {
            reason: "different action content".into(),
        };
        assert!(matches!(
            persist_notice_for_test(&mut conn, &content_conflict),
            Err(SupervisorNoticeStoreError::Conflict)
        ));
    }

    #[test]
    fn notice_insert_uses_the_callers_loan_transaction() {
        let mut conn = connection();
        let resource = resource();
        register_resource(&mut conn, &resource).unwrap();
        let loan_id = LoanId::new();
        let action_id = ActionId::new();
        let task_id = TaskId::new();
        let loan_state = LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task: task_id,
                watcher_intent: None,
            },
        };
        let notice = SupervisorNotice {
            id: NoticeId::new(),
            loan_id,
            action_id,
            state_revision: resource.state_revision,
            destination: resource.supervisor,
            assignment_revision: resource.assignment_revision,
            payload: SupervisorNoticePayload::ReleaseRequired { task_id },
            delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
        };

        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        insert_loan(&tx, loan_id, resource.id, loan_state).unwrap();
        assert_eq!(
            insert_supervisor_notice_in_transaction(&tx, &notice).unwrap(),
            notice
        );
        tx.rollback().unwrap();

        let loan_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM loans WHERE id = ?1",
                [loan_id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        let notice_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_supervisor_notices WHERE id = ?1",
                [notice.id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(loan_count, 0);
        assert_eq!(notice_count, 0);
    }

    #[test]
    fn only_the_current_attempt_can_settle_and_delivery_does_not_finish_the_loan() {
        let mut conn = connection();
        let notice = insert_notice_fixture(&mut conn);
        persist_notice_for_test(&mut conn, &notice).unwrap();
        let loan_state_before: String = conn
            .query_row(
                "SELECT state_json FROM loans WHERE id = ?1",
                [notice.loan_id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();

        let first_attempt = DeliveryAttemptId::new();
        reserve_supervisor_notice_attempt(&mut conn, notice.id, first_attempt).unwrap();
        let failed = settle_supervisor_notice_attempt(
            &mut conn,
            notice.id,
            first_attempt,
            Err("temporary transport failure".into()),
        )
        .unwrap();
        assert_eq!(
            failed.delivery,
            SupervisorNoticeDelivery::RetryPending {
                attempts: 1,
                last_error: "temporary transport failure".into(),
            }
        );

        let second_attempt = DeliveryAttemptId::new();
        reserve_supervisor_notice_attempt(&mut conn, notice.id, second_attempt).unwrap();
        assert!(matches!(
            settle_supervisor_notice_attempt(&mut conn, notice.id, first_attempt, Ok(())),
            Err(SupervisorNoticeStoreError::StaleAttempt)
        ));
        assert!(matches!(
            supervisor_notice(&conn, notice.id).unwrap().unwrap().delivery,
            SupervisorNoticeDelivery::Sending { attempt_id, attempt: 2 }
                if attempt_id == second_attempt
        ));

        let delivered =
            settle_supervisor_notice_attempt(&mut conn, notice.id, second_attempt, Ok(())).unwrap();
        assert_eq!(
            delivered.delivery,
            SupervisorNoticeDelivery::Delivered { attempts: 2 }
        );
        let loan_state_after: String = conn
            .query_row(
                "SELECT state_json FROM loans WHERE id = ?1",
                [notice.loan_id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(loan_state_after, loan_state_before);
        assert!(matches!(
            reserve_supervisor_notice_attempt(&mut conn, notice.id, DeliveryAttemptId::new()),
            Err(SupervisorNoticeStoreError::AlreadyDelivered)
        ));
    }

    #[test]
    fn retry_budget_and_last_error_survive_database_reopen() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("db");
        let notice = {
            let mut conn = connection_at(&path);
            let notice = insert_notice_fixture(&mut conn);
            persist_notice_for_test(&mut conn, &notice).unwrap();
            let attempt = DeliveryAttemptId::new();
            reserve_supervisor_notice_attempt(&mut conn, notice.id, attempt).unwrap();
            settle_supervisor_notice_attempt(
                &mut conn,
                notice.id,
                attempt,
                Err("first retry error".into()),
            )
            .unwrap();
            notice
        };

        let mut conn = connection_at(&path);
        assert_eq!(
            supervisor_notice(&conn, notice.id)
                .unwrap()
                .unwrap()
                .delivery,
            SupervisorNoticeDelivery::RetryPending {
                attempts: 1,
                last_error: "first retry error".into(),
            }
        );
        let second_attempt = DeliveryAttemptId::new();
        reserve_supervisor_notice_attempt(&mut conn, notice.id, second_attempt).unwrap();
        settle_supervisor_notice_attempt(
            &mut conn,
            notice.id,
            second_attempt,
            Err("second retry error".into()),
        )
        .unwrap();
        drop(conn);

        let mut conn = connection_at(&path);
        assert_eq!(
            supervisor_notice(&conn, notice.id)
                .unwrap()
                .unwrap()
                .delivery,
            SupervisorNoticeDelivery::RetryPending {
                attempts: 2,
                last_error: "second retry error".into(),
            }
        );
        let final_attempt = DeliveryAttemptId::new();
        reserve_supervisor_notice_attempt(&mut conn, notice.id, final_attempt).unwrap();
        let failed = settle_supervisor_notice_attempt(
            &mut conn,
            notice.id,
            final_attempt,
            Err("final retry error".into()),
        )
        .unwrap();
        assert_eq!(
            failed.delivery,
            SupervisorNoticeDelivery::Failed {
                attempts: 3,
                last_error: "final retry error".into(),
            }
        );
        drop(conn);

        let mut reopened = connection_at(&path);
        assert_eq!(
            supervisor_notice(&reopened, notice.id)
                .unwrap()
                .unwrap()
                .delivery,
            SupervisorNoticeDelivery::Failed {
                attempts: 3,
                last_error: "final retry error".into(),
            }
        );
        assert!(pending_supervisor_notices(&reopened).unwrap().is_empty());
        assert!(matches!(
            reserve_supervisor_notice_attempt(&mut reopened, notice.id, DeliveryAttemptId::new()),
            Err(SupervisorNoticeStoreError::AttemptBudgetExhausted)
        ));
    }

    #[test]
    fn startup_recovery_retries_or_fails_sending_notices_without_new_identity() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("db");
        let (notice, in_flight_attempt) = {
            let mut conn = connection_at(&path);
            let notice = insert_notice_fixture(&mut conn);
            persist_notice_for_test(&mut conn, &notice).unwrap();
            for error in ["first error", "second error"] {
                let attempt = DeliveryAttemptId::new();
                reserve_supervisor_notice_attempt(&mut conn, notice.id, attempt).unwrap();
                settle_supervisor_notice_attempt(&mut conn, notice.id, attempt, Err(error.into()))
                    .unwrap();
            }
            let in_flight_attempt = DeliveryAttemptId::new();
            reserve_supervisor_notice_attempt(&mut conn, notice.id, in_flight_attempt).unwrap();
            (notice, in_flight_attempt)
        };

        let mut reopened = connection_at(&path);
        assert!(matches!(
            supervisor_notice(&reopened, notice.id).unwrap().unwrap().delivery,
            SupervisorNoticeDelivery::Sending { attempt_id, attempt: 3 }
                if attempt_id == in_flight_attempt
        ));
        let recovered = recover_sending_supervisor_notices(&mut reopened).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].id, notice.id);
        assert_eq!(recovered[0].action_id, notice.action_id);
        assert_eq!(
            recovered[0].delivery,
            SupervisorNoticeDelivery::Failed {
                attempts: 3,
                last_error: INTERRUPTED_DELIVERY_ERROR.into(),
            }
        );
        assert!(matches!(
            settle_supervisor_notice_attempt(&mut reopened, notice.id, in_flight_attempt, Ok((),)),
            Err(SupervisorNoticeStoreError::StaleAttempt)
        ));
        assert!(
            recover_sending_supervisor_notices(&mut reopened)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn supervisor_retarget_uses_assignment_cas_and_invalidates_old_attempts() {
        let mut conn = connection();
        let notice = insert_notice_fixture(&mut conn);
        persist_notice_for_test(&mut conn, &notice).unwrap();
        let old_assignment = notice.assignment_revision;
        let next_assignment = AssignmentRevision::new(old_assignment.get() + 1);
        let next_destination = SupervisorAddress {
            machine: MachineId::new(),
            thread: ThreadId(Uuid::now_v7()),
        };
        assert!(matches!(
            retarget_supervisor_notice(
                &mut conn,
                notice.id,
                AssignmentRevision::new(old_assignment.get() + 1),
                next_destination,
                AssignmentRevision::new(old_assignment.get() + 2),
            ),
            Err(SupervisorNoticeStoreError::StaleAssignmentRevision)
        ));

        let old_attempt = DeliveryAttemptId::new();
        reserve_supervisor_notice_attempt(&mut conn, notice.id, old_attempt).unwrap();
        let retargeted = retarget_supervisor_notice(
            &mut conn,
            notice.id,
            old_assignment,
            next_destination,
            next_assignment,
        )
        .unwrap();
        assert_eq!(retargeted.id, notice.id);
        assert_eq!(retargeted.loan_id, notice.loan_id);
        assert_eq!(retargeted.action_id, notice.action_id);
        assert_eq!(retargeted.destination, next_destination);
        assert_eq!(retargeted.assignment_revision, next_assignment);
        assert_eq!(
            retargeted.delivery,
            SupervisorNoticeDelivery::RetryPending {
                attempts: 1,
                last_error: RETARGETED_DELIVERY_ERROR.into(),
            }
        );
        assert!(matches!(
            settle_supervisor_notice_attempt(&mut conn, notice.id, old_attempt, Ok(())),
            Err(SupervisorNoticeStoreError::StaleAttempt)
        ));
        assert!(matches!(
            retarget_supervisor_notice(
                &mut conn,
                notice.id,
                old_assignment,
                next_destination,
                AssignmentRevision::new(next_assignment.get() + 1),
            ),
            Err(SupervisorNoticeStoreError::StaleAssignmentRevision)
        ));

        let current_attempt = DeliveryAttemptId::new();
        reserve_supervisor_notice_attempt(&mut conn, notice.id, current_attempt).unwrap();
        settle_supervisor_notice_attempt(&mut conn, notice.id, current_attempt, Ok(())).unwrap();
        assert!(matches!(
            retarget_supervisor_notice(
                &mut conn,
                notice.id,
                next_assignment,
                next_destination,
                AssignmentRevision::new(next_assignment.get() + 1),
            ),
            Err(SupervisorNoticeStoreError::AlreadyDelivered)
        ));
        assert!(matches!(
            supervisor_notice(&conn, notice.id)
                .unwrap()
                .unwrap()
                .delivery,
            SupervisorNoticeDelivery::Delivered { attempts: 2 }
        ));
    }

    #[test]
    fn pending_notices_are_ordered_by_stable_notice_id() {
        let mut conn = connection();
        let first = insert_notice_fixture(&mut conn);
        let mut second = first.clone();
        second.id = NoticeId::from_uuid(Uuid::from_u128(2));
        second.action_id = ActionId::new();
        let mut first = first;
        first.id = NoticeId::from_uuid(Uuid::from_u128(1));
        first.action_id = ActionId::new();
        persist_notice_for_test(&mut conn, &second).unwrap();
        persist_notice_for_test(&mut conn, &first).unwrap();

        let pending = pending_supervisor_notices(&conn).unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, first.id);
        assert_eq!(pending[1].id, second.id);
        assert_eq!(supervisor_notice(&conn, first.id).unwrap(), Some(first));
    }
}
