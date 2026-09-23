//! SQLite operations for authority-local resource state.

use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::{
    AcceptanceSequence, ActionId, AssignmentRevision, CommandSpec, CommandSpecError,
    DeliveryAttemptId, Loan, LoanId, LoanPhase, LoanState, NoticeId, Resource, ResourceId,
    ResourceRequest, ResourceRequestState, ResourceRevision, ReturnContext, SupervisorAddress,
    SupervisorNotice, SupervisorNoticeDelivery, SupervisorNoticePayload,
};
use crate::domain::{TaskId, ThreadId};
use crate::machine::MachineId;
use crate::spec::NormalizedSpec;
use crate::store::{IdentityError, Store};
use crate::submission::{ExecutorIdentity, PreAcceptanceRejection, RejectionTombstone, RequestId};

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

CREATE INDEX IF NOT EXISTS resource_supervisor_notices_pending
    ON resource_supervisor_notices(id)
    WHERE json_extract(notice_json, '$.delivery.type') IN ('pending', 'retry_pending');
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
    /// SQLite or stored data failed.
    #[error("resource storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// One authority-owned resource and the loan that must be restored with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResourceSnapshot {
    /// Resource whose authority matches the loading daemon.
    pub(crate) resource: Resource,
    /// The resource's current non-closed loan, if one exists.
    pub(crate) loan: Option<Loan>,
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

/// A release loan could not be opened for the current resource state.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenReleaseLoanError {
    /// Resource authority or stored resource data failed validation.
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// The saved release notice could not be read or inserted.
    #[error(transparent)]
    Notice(#[from] SupervisorNoticeStoreError),
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
    /// Evidence cannot establish a valid return obligation.
    #[error("invalid release return context: {reason}")]
    InvalidReturnContext {
        /// Why the supplied context cannot be stored.
        reason: &'static str,
    },
    /// Return evidence must refer to the exact task named by the release action.
    #[error("return context task does not match observed background task {task_id}")]
    ReturnTaskMismatch {
        /// Exact background task observed when release was requested.
        task_id: TaskId,
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
    /// The saved request state is returned without changing assigned or terminal states.
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
    if new_assignment_revision.get() <= expected_assignment_revision.get() {
        return Err(SupervisorNoticeStoreError::AssignmentRevisionMustIncrease);
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (mut notice, old_json) = select_supervisor_notice_record(&tx, notice_id)?
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

    update_supervisor_notice_cas(&tx, &notice, &old_json)?;
    tx.commit()?;
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

fn select_supervisor_notice_record(
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

fn select_supervisor_notice_record_by_action(
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

fn decode_supervisor_notice_record(row: &Row<'_>) -> rusqlite::Result<(SupervisorNotice, String)> {
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

fn update_supervisor_notice_cas(
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
    let task_status: Option<String> = tx
        .query_row(
            "SELECT status FROM tasks WHERE id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    match task_status.as_deref() {
        None => return Err(OpenReleaseLoanError::BackgroundTaskMissing { task_id }),
        Some("running") => {}
        Some(state) => {
            return Err(OpenReleaseLoanError::BackgroundTaskNotRunning {
                task_id,
                state: state.to_owned(),
            });
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
    tx.commit()?;

    Ok(OpenReleaseLoanResult::Opened { loan, notice })
}

/// Complete a saved release action and atomically assign work or reserve return.
///
/// The ordinary task row must be terminal. This is only a database fence; the
/// caller must separately confirm that the owned process group or container has
/// exited before it supplies this evidence.
pub(crate) fn complete_release_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    action_id: ActionId,
    expected_state_revision: ResourceRevision,
    return_context: ReturnContext,
) -> Result<ReleaseCompletionResult, CompleteReleaseError> {
    validate_release_return_context(&return_context)?;

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

    let return_task_id = match &return_context {
        ReturnContext::Stopped { task_id, .. }
        | ReturnContext::AlreadyCompleted { task_id, .. } => *task_id,
        ReturnContext::Idle => {
            return Err(CompleteReleaseError::InvalidReturnContext {
                reason: "idle does not prove the observed task stopped",
            });
        }
    };
    if return_task_id != *observed_background_task {
        return Err(CompleteReleaseError::ReturnTaskMismatch {
            task_id: *observed_background_task,
        });
    }

    if loan.resource_id != resource_id
        || notice.loan_id != loan.id
        || notice.action_id != action_id
        || notice.payload
            != (SupervisorNoticePayload::ReleaseRequired {
                task_id: *observed_background_task,
            })
        || notice.destination != resource.supervisor
        || notice.assignment_revision != resource.assignment_revision
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

    require_terminal_observed_task(&tx, *observed_background_task)?;

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
        result: result.clone(),
    };
    let receipt_json = encode_completion_json(&receipt)?;
    tx.execute(
        "INSERT INTO resource_release_completions (action_id, receipt_json)
         VALUES (?1, ?2)",
        params![action_id.as_uuid().to_string(), receipt_json],
    )?;
    tx.commit()?;

    Ok(result)
}

fn validate_release_return_context(
    return_context: &ReturnContext,
) -> Result<(), CompleteReleaseError> {
    match return_context {
        ReturnContext::Stopped {
            checkpoint_ref,
            recovery_ref,
            ..
        } if checkpoint_ref.trim().is_empty() || recovery_ref.trim().is_empty() => {
            Err(CompleteReleaseError::InvalidReturnContext {
                reason: "checkpoint and recovery references must not be empty",
            })
        }
        ReturnContext::AlreadyCompleted { result_ref, .. } if result_ref.trim().is_empty() => {
            Err(CompleteReleaseError::InvalidReturnContext {
                reason: "result reference must not be empty",
            })
        }
        ReturnContext::Idle => Err(CompleteReleaseError::InvalidReturnContext {
            reason: "idle does not prove the observed task stopped",
        }),
        ReturnContext::Stopped { .. } | ReturnContext::AlreadyCompleted { .. } => Ok(()),
    }
}

fn require_terminal_observed_task(
    conn: &Connection,
    task_id: TaskId,
) -> Result<(), CompleteReleaseError> {
    let status = conn
        .query_row(
            "SELECT status FROM tasks WHERE id = ?1",
            [task_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or(CompleteReleaseError::BackgroundTaskMissing { task_id })?;
    if status == "lost" {
        return Err(CompleteReleaseError::BackgroundTaskLost { task_id });
    }
    if matches!(status.as_str(), "succeeded" | "failed" | "cancelled") {
        return Ok(());
    }

    Err(CompleteReleaseError::BackgroundTaskNotTerminal {
        task_id,
        state: status,
    })
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

    if let Some(saved) = select_resource(&tx, resource.id)? {
        if saved != *resource {
            return Err(ResourceStoreError::Conflict);
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
    tx.commit()?;
    Ok(resource.clone())
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
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    if let Some(saved) = select_request_by_id(&tx, request_id)? {
        if saved.task_id != task_id
            || saved.resource_id != resource_id
            || saved.origin_machine != origin_machine
        {
            return Err(ResourceStoreError::Conflict);
        }
        let command_spec = CommandSpec::try_from(normalized_spec)?;
        if !same_spec(saved.spec().as_normalized(), command_spec.as_normalized())? {
            return Err(ResourceStoreError::Conflict);
        }
        check_resource_authority(&tx, resource_id, authority_machine)?;

        tx.commit()?;
        return Ok(saved);
    }

    if task_id_exists(&tx, task_id)? {
        return Err(ResourceStoreError::Conflict);
    }

    let identity = RequestIdentity {
        request_id,
        task_id,
        resource_id,
        origin_machine,
    };
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

/// Cancel a queued request on its fixed authority or retain prevention before acceptance.
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

        if matches!(saved.state, ResourceRequestState::Queued) {
            let cancelled = ResourceRequestState::CancelledBeforeLaunch;
            let state_json = encode_json(&cancelled)?;
            let updated = tx.execute(
                "UPDATE resource_requests SET state_json = ?1
                 WHERE request_id = ?2
                   AND json_extract(state_json, '$.type') = 'queued'",
                params![state_json, request_id.0.to_string()],
            )?;
            if updated == 1 {
                saved.state = cancelled;
            } else {
                return Err(ResourceStoreError::Conflict);
            }

            let executor_identity = Store::reject_execution_in(
                &tx,
                &cancellation_tombstone(task_id, origin_machine, authority),
            )?;
            if matches!(executor_identity, ExecutorIdentity::Accepted(_)) {
                return Err(ResourceStoreError::ExecutorAlreadyAccepted { task: task_id });
            }
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

/// Read all requests for one resource in authority acceptance order.
pub(crate) fn requests_for_resource_for_authority(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
    requests_for_resource_inner(conn, Some(authority_machine), resource_id)
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

fn select_resource(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<Resource>, rusqlite::Error> {
    let sql = format!("SELECT {RESOURCE_COLUMNS} FROM resources WHERE id = ?1");
    conn.query_row(&sql, [resource_id.as_uuid().to_string()], decode_resource)
        .optional()
}

fn select_non_closed_loan(
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

fn select_request_by_id(
    conn: &Connection,
    request_id: RequestId,
) -> Result<Option<ResourceRequest>, rusqlite::Error> {
    let sql = format!("SELECT {REQUEST_COLUMNS} FROM resource_requests WHERE request_id = ?1");
    conn.query_row(&sql, [request_id.0.to_string()], decode_request)
        .optional()
}

fn check_resource_authority(
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

    fn begin_release_action(
        conn: &mut Connection,
        resource: &Resource,
        background_task: TaskId,
    ) -> (Loan, SupervisorNotice) {
        let result = open_release_loan_for_authority(
            conn,
            resource.authority_machine(),
            resource.id,
            resource.state_revision,
        )
        .unwrap();
        let OpenReleaseLoanResult::Opened { loan, notice } = result else {
            panic!("fixture must create a new release action");
        };
        assert!(matches!(
            &loan.state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease {
                    observed_background_task,
                    ..
                }
            } if *observed_background_task == background_task
        ));
        (loan, notice)
    }

    fn stopped_context(task_id: TaskId) -> ReturnContext {
        stopped_context_with_ref(task_id, "checkpoint-42")
    }

    fn stopped_context_with_ref(task_id: TaskId, checkpoint_ref: &str) -> ReturnContext {
        ReturnContext::Stopped {
            task_id,
            checkpoint_ref: checkpoint_ref.into(),
            recovery_ref: "recovery-plan-42".into(),
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
    fn release_completion_assigns_the_oldest_non_cancelled_request() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        insert_local_task_status(&conn, background_task, "running");
        let cancelled = queue_request(&mut conn, resource.id);
        let ready = queue_request(&mut conn, resource.id);
        cancel_request_before_activation(
            &mut conn,
            cancelled.request_id,
            cancelled.task_id,
            cancelled.resource_id,
            cancelled.origin_machine,
        )
        .unwrap();

        let (loan, release_notice) = begin_release_action(&mut conn, &resource, background_task);
        conn.execute(
            "UPDATE tasks SET status = 'succeeded' WHERE id = ?1",
            [background_task.to_string()],
        )
        .unwrap();
        let return_context = stopped_context(background_task);

        let result = complete_release_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            release_notice.action_id,
            release_notice.state_revision,
            return_context,
        )
        .unwrap();
        let ReleaseCompletionResult::Assigned {
            loan: assigned_loan,
            request,
            state_revision,
        } = result
        else {
            panic!("queued work must be assigned before return is requested");
        };

        assert_eq!(assigned_loan.id, loan.id);
        assert_eq!(state_revision, ResourceRevision::new(2));
        assert_eq!(request.request_id, ready.request_id);
        assert!(matches!(
            request.state,
            ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
        ));
        assert!(matches!(
            assigned_loan.state,
            LoanState::Active {
                phase: LoanPhase::Serving { current_request_id, .. }
            } if current_request_id == ready.request_id
        ));
        let saved = requests_for_resource(&conn, resource.id).unwrap();
        assert!(matches!(
            saved[0].state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert!(matches!(
            saved[1].state,
            ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
        ));
    }

    #[test]
    fn release_completion_with_empty_queue_persists_return_obligation_and_notice() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        insert_local_task_status(&conn, background_task, "running");
        let queued = queue_request(&mut conn, resource.id);
        let (loan, release_notice) = begin_release_action(&mut conn, &resource, background_task);
        cancel_request_before_activation(
            &mut conn,
            queued.request_id,
            queued.task_id,
            queued.resource_id,
            queued.origin_machine,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status = 'cancelled' WHERE id = ?1",
            [background_task.to_string()],
        )
        .unwrap();
        let return_context = ReturnContext::AlreadyCompleted {
            task_id: background_task,
            result_ref: "final-result-42".into(),
        };

        let result = complete_release_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            release_notice.action_id,
            release_notice.state_revision,
            return_context.clone(),
        )
        .unwrap();
        let ReleaseCompletionResult::ReturnRequired {
            loan: returned_loan,
            notice,
        } = result
        else {
            panic!("an empty queue must preserve the supervisor return decision");
        };

        assert_eq!(returned_loan.id, loan.id);
        assert_eq!(notice.state_revision, ResourceRevision::new(2));
        assert_eq!(notice.destination, resource.supervisor);
        assert_eq!(notice.assignment_revision, resource.assignment_revision);
        assert_eq!(
            notice.payload,
            SupervisorNoticePayload::ReturnRequired { return_context }
        );
        assert_ne!(notice.action_id, release_notice.action_id);
        assert!(matches!(
            notice.delivery,
            SupervisorNoticeDelivery::Pending { attempts: 0 }
        ));
        assert!(matches!(
            returned_loan.state,
            LoanState::Active {
                phase: LoanPhase::AwaitingReturn { .. }
            }
        ));
        let saved_notice = select_supervisor_notice_record_by_action(&conn, notice.action_id)
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(saved_notice, notice);
        assert!(oldest_queued_request(&conn, resource.id).unwrap().is_none());
    }

    #[test]
    fn release_completion_retry_returns_exact_saved_result_after_later_changes() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        insert_local_task_status(&conn, background_task, "running");
        let queued = queue_request(&mut conn, resource.id);
        let (loan, release_notice) = begin_release_action(&mut conn, &resource, background_task);
        conn.execute(
            "UPDATE tasks SET status = 'succeeded' WHERE id = ?1",
            [background_task.to_string()],
        )
        .unwrap();
        let return_context = stopped_context(background_task);
        let first = complete_release_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            release_notice.action_id,
            release_notice.state_revision,
            return_context.clone(),
        )
        .unwrap();

        let later_loan_state = LoanState::Closed {
            result: LoanClosure::NoResume {
                return_context: return_context.clone(),
                reason: "supervisor decision persisted later".into(),
            },
        };
        let later_request_state = ResourceRequestState::Finished {
            outcome: crate::domain::ExitReason::Exit { code: 0 },
        };
        conn.execute(
            "UPDATE loans SET state_json = ?1 WHERE id = ?2",
            params![
                serde_json::to_string(&later_loan_state).unwrap(),
                loan.id.as_uuid().to_string(),
            ],
        )
        .unwrap();
        conn.execute(
            "UPDATE resource_requests SET state_json = ?1 WHERE request_id = ?2",
            params![
                serde_json::to_string(&later_request_state).unwrap(),
                queued.request_id.0.to_string(),
            ],
        )
        .unwrap();
        conn.execute(
            "UPDATE resources SET state_revision = 25 WHERE id = ?1",
            [resource.id.as_uuid().to_string()],
        )
        .unwrap();

        let retry = complete_release_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            release_notice.action_id,
            release_notice.state_revision,
            return_context.clone(),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(retry).unwrap(),
            serde_json::to_value(&first).unwrap()
        );
        assert!(matches!(
            complete_release_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                release_notice.action_id,
                release_notice.state_revision,
                stopped_context_with_ref(background_task, "different-checkpoint"),
            ),
            Err(CompleteReleaseError::ConflictingRetry { action_id })
                if action_id == release_notice.action_id
        ));
    }

    #[test]
    fn release_completion_rejects_mismatched_stale_or_nonterminal_evidence() {
        let mut conn = connection();
        let background_task = TaskId::new();
        let resource = resource_with_background_task(background_task);
        register_resource(&mut conn, &resource).unwrap();
        insert_local_task_status(&conn, background_task, "running");
        let _queued = queue_request(&mut conn, resource.id);
        let (loan, release_notice) = begin_release_action(&mut conn, &resource, background_task);

        assert!(matches!(
            complete_release_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                release_notice.action_id,
                release_notice.state_revision,
                stopped_context(TaskId::new()),
            ),
            Err(CompleteReleaseError::ReturnTaskMismatch { task_id })
                if task_id == background_task
        ));
        assert!(matches!(
            complete_release_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                release_notice.action_id,
                release_notice.state_revision,
                ReturnContext::Idle,
            ),
            Err(CompleteReleaseError::InvalidReturnContext { .. })
        ));
        for context in [
            ReturnContext::Stopped {
                task_id: background_task,
                checkpoint_ref: "  ".into(),
                recovery_ref: "recovery".into(),
            },
            ReturnContext::Stopped {
                task_id: background_task,
                checkpoint_ref: "checkpoint".into(),
                recovery_ref: String::new(),
            },
            ReturnContext::AlreadyCompleted {
                task_id: background_task,
                result_ref: String::new(),
            },
        ] {
            assert!(matches!(
                complete_release_for_authority(
                    &mut conn,
                    resource.authority_machine(),
                    resource.id,
                    release_notice.action_id,
                    release_notice.state_revision,
                    context,
                ),
                Err(CompleteReleaseError::InvalidReturnContext { .. })
            ));
        }
        assert!(matches!(
            complete_release_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                release_notice.action_id,
                release_notice.state_revision,
                stopped_context(background_task),
            ),
            Err(CompleteReleaseError::BackgroundTaskNotTerminal { task_id, state })
                if task_id == background_task && state == "running"
        ));

        conn.execute(
            "UPDATE tasks SET status = 'lost' WHERE id = ?1",
            [background_task.to_string()],
        )
        .unwrap();
        assert!(matches!(
            complete_release_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                release_notice.action_id,
                release_notice.state_revision,
                stopped_context(background_task),
            ),
            Err(CompleteReleaseError::BackgroundTaskLost { task_id })
                if task_id == background_task
        ));

        conn.execute(
            "UPDATE tasks SET status = 'succeeded' WHERE id = ?1",
            [background_task.to_string()],
        )
        .unwrap();
        conn.execute(
            "UPDATE resources SET state_revision = 8 WHERE id = ?1",
            [resource.id.as_uuid().to_string()],
        )
        .unwrap();
        assert!(matches!(
            complete_release_for_authority(
                &mut conn,
                resource.authority_machine(),
                resource.id,
                release_notice.action_id,
                release_notice.state_revision,
                stopped_context(background_task),
            ),
            Err(CompleteReleaseError::StaleRevision {
                expected,
                actual,
            }) if expected == release_notice.state_revision && actual == ResourceRevision::new(8)
        ));
        assert!(matches!(
            select_non_closed_loan(&conn, resource.id)
                .unwrap()
                .unwrap()
                .state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease { .. }
            }
        ));
        let receipt_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_release_completions",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(receipt_count, 0);
        assert_eq!(
            select_non_closed_loan(&conn, resource.id)
                .unwrap()
                .unwrap()
                .id,
            loan.id
        );
    }

    #[test]
    fn cancellation_returns_assigned_and_terminal_states_without_overwriting_them() {
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
        let loan_id = LoanId::new();
        let assigned_state = ResourceRequestState::Assigned { loan_id };
        conn.execute(
            "UPDATE resource_requests SET state_json = ?1 WHERE request_id = ?2",
            params![
                serde_json::to_string(&assigned_state).unwrap(),
                assigned.request_id.0.to_string(),
            ],
        )
        .unwrap();
        let loan_state = LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id: ActionId::new(),
                observed_background_task: TaskId::new(),
            },
        };
        insert_loan(&conn, loan_id, resource.id, loan_state).unwrap();
        let loan_json_before: String = conn
            .query_row(
                "SELECT state_json FROM loans WHERE id = ?1",
                [loan_id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();

        let result = cancel_request_before_activation(
            &mut conn,
            assigned.request_id,
            assigned.task_id,
            assigned.resource_id,
            assigned.origin_machine,
        )
        .unwrap();
        let QueueCancellationResult::Request(saved_assigned) = result else {
            panic!("an assigned request must remain in the queue store");
        };
        assert!(matches!(
            saved_assigned.state,
            ResourceRequestState::Assigned { loan_id: saved }
                if saved == loan_id
        ));
        let loan_json_after: String = conn
            .query_row(
                "SELECT state_json FROM loans WHERE id = ?1",
                [loan_id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(loan_json_after, loan_json_before);

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
            Err(ResourceStoreError::Conflict)
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
            Err(ResourceStoreError::Conflict)
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
