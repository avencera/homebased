//! StoreActor-facing operations for authority-local resource state.

use std::path::PathBuf;

use chrono::{SecondsFormat, Utc};

mod release_proof;
pub(crate) use release_proof::VerifiedReleaseProof;
use release_proof::{ReleaseProofEvidence, VerifiedReleaseEvidence};

use super::Store;
use crate::cancellation::ResourceCancellationRequestIdentity;
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskId, TaskRow, TaskState,
};
use crate::error::AppError;
use crate::events::{EventPayload, TaskEvent};
use crate::machine::MachineId;
use crate::resource::ownership_lock::{
    OwnershipLockIdentity, OwnershipLockProbe, TrainerRequestDigest, VerifiedTrainerAttempt,
    probe_segment_ownership_lock,
};
use crate::resource::release_watcher::{
    ReleaseWatcherCommand, ReleaseWatcherPollAttention, ReleaseWatcherPollOutcome,
    ReleaseWatcherPollRequest,
};
use crate::resource::store::{
    AcceptedResourceTask, AssignedResourceTaskReconcileInput, AssignedResourceTaskReconcileOutcome,
    CompleteReleaseError, OpenReleaseLoanError, OpenReleaseLoanResult, QueueCancellationResult,
    ReleaseCheckpointCancellationOutcome, ReleaseCheckpointCancellationResult,
    ReleaseCheckpointError, ReleaseCompletionResult, ReleaseWatcherAcceptance,
    ReleaseWatcherAcceptanceError, ReleaseWatcherAcceptanceInput, ResourceQueueReconcileError,
    ResourceSnapshot, ResourceStoreError, ResourceTaskAcceptance, ResourceTaskAcceptanceInput,
    SupervisorNoticeStoreError, TrainerAttemptAssociationStoreError, accept_request_for_authority,
    assigned_resource_request_for_acceptance, assigned_resource_requests_for_authority,
    bind_release_watcher_for_authority as persist_release_watcher_binding,
    bind_release_watcher_intent_on, cancel_request_before_activation_for_authority,
    check_resource_authority,
    complete_release_for_authority as persist_release_completion_for_authority,
    oldest_queued_request_for_authority,
    open_release_loan_for_authority as persist_release_loan_for_authority,
    pending_supervisor_notices as load_pending_supervisor_notices,
    reconcile_assigned_resource_task_for_authority as persist_assigned_task_reconciliation,
    reconcile_resource_queue_for_authority as persist_resource_queue_reconciliation,
    recover_sending_supervisor_notices as recover_in_flight_supervisor_notices,
    register_resource_for_authority, release_checkpoint_state_for_action,
    release_completion_for_retry, requests_for_resource_for_authority,
    reserve_supervisor_notice_attempt as reserve_notice_attempt,
    resources_for_authority as load_resources_for_authority,
    retarget_supervisor_notice as retarget_notice, select_non_closed_loan, select_resource,
    settle_supervisor_notice_attempt as settle_notice_attempt,
    supervisor_notice as load_supervisor_notice, update_release_checkpoint_state,
    validate_release_watcher_for_local_acceptance, validate_release_watcher_intent_on,
};
use crate::resource::watcher::{
    AttemptBinding, WatchObservation, WatcherAttention, find_completed_result,
    observe_release_with_checkpoint_evidence, revalidate_checkpoint_publication, snapshot,
};
use crate::resource::{
    ActionId, AssignmentRevision, DeliveryAttemptId, LoanPhase, LoanState, NoticeId,
    ReleaseCheckpointAction, ReleaseCheckpointBaseline, ReleaseCheckpointBinding,
    ReleaseCheckpointCancellation, ReleaseCheckpointPhase, ReleaseCheckpointStopDecision,
    ReleaseCheckpointStopOutcome, ReleaseStopReservationId, ReleaseWatcherIntent, Resource,
    ResourceId, ResourceQueueReconcileOutcome, ResourceRequest, ResourceRevision,
    SupervisorAddress, SupervisorNotice, TrainerAttemptAssociation, TrainerAttemptAssociationProof,
};
#[cfg(test)]
use crate::resource::{
    Loan, LoanId, ResourceRequestState, ReturnContext, SavedReleaseWatcherIntent,
    ServingReleaseProvenance,
};
use crate::spec::NormalizedSpec;
use crate::submission::{
    CallbackContext, ExecutionRecord, ExecutorIdentity, NormalizedSpecSha256, OriginRoute,
    RequestId, ResourceCancellationIneligibleReason, ResourceCancellationOutcome,
    ResourceCancellationReceipt, ResourceRoutePhase, SubmissionState, normalized_spec_sha256,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredTrainerAttemptAssociation {
    resource_id: ResourceId,
    authority_machine: MachineId,
    task_id: TaskId,
    canonical_runtime_root: PathBuf,
    attempt_binding: AttemptBinding,
    request_sha256: String,
    ownership_lock_identity: StoredOwnershipLockIdentity,
    normalized_spec_sha256: NormalizedSpecSha256,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredOwnershipLockIdentity {
    device: u64,
    inode: u64,
}

impl From<&TrainerAttemptAssociation> for StoredTrainerAttemptAssociation {
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
            ownership_lock_identity: StoredOwnershipLockIdentity {
                device: lock.device(),
                inode: lock.inode(),
            },
            normalized_spec_sha256: association.normalized_spec_sha256(),
        }
    }
}

impl StoredTrainerAttemptAssociation {
    fn into_association(
        self,
        resource_id: ResourceId,
        authority_machine: MachineId,
        task_id: TaskId,
    ) -> Result<TrainerAttemptAssociation, TrainerAttemptAssociationStoreError> {
        if self.resource_id != resource_id
            || self.authority_machine != authority_machine
            || self.task_id != task_id
        {
            return Err(
                TrainerAttemptAssociationStoreError::InvalidStoredAssociation(
                    "JSON identities do not match the association row".into(),
                ),
            );
        }

        let request_digest =
            TrainerRequestDigest::from_hex(&self.request_sha256).ok_or_else(|| {
                TrainerAttemptAssociationStoreError::InvalidStoredAssociation(
                    "request SHA-256 is not 64 lowercase hexadecimal characters".into(),
                )
            })?;
        let verified_attempt = VerifiedTrainerAttempt::from_persisted(
            self.canonical_runtime_root,
            self.attempt_binding,
            request_digest,
            OwnershipLockIdentity::new(
                self.ownership_lock_identity.device,
                self.ownership_lock_identity.inode,
            ),
        )
        .map_err(|reason| {
            TrainerAttemptAssociationStoreError::InvalidStoredAssociation(reason.into())
        })?;
        TrainerAttemptAssociation::from_components(
            resource_id,
            authority_machine,
            task_id,
            verified_attempt,
            self.normalized_spec_sha256,
        )
        .map_err(|reason| {
            TrainerAttemptAssociationStoreError::InvalidStoredAssociation(reason.into())
        })
    }
}

fn trainer_association_json(
    association: &TrainerAttemptAssociation,
) -> Result<String, TrainerAttemptAssociationStoreError> {
    serde_json::to_string(&StoredTrainerAttemptAssociation::from(association))
        .map_err(crate::error::AppError::from)
        .map_err(Into::into)
}

fn decode_trainer_association(
    resource_id: String,
    authority_machine: String,
    task_id: String,
    association_json: String,
) -> Result<TrainerAttemptAssociation, TrainerAttemptAssociationStoreError> {
    let parse_error = |error: serde_json::Error| {
        TrainerAttemptAssociationStoreError::InvalidStoredAssociation(error.to_string())
    };
    let resource_uuid = uuid::Uuid::parse_str(&resource_id).map_err(|error| {
        TrainerAttemptAssociationStoreError::InvalidStoredAssociation(error.to_string())
    })?;
    let authority_uuid = uuid::Uuid::parse_str(&authority_machine).map_err(|error| {
        TrainerAttemptAssociationStoreError::InvalidStoredAssociation(error.to_string())
    })?;
    let task_uuid = uuid::Uuid::parse_str(&task_id).map_err(|error| {
        TrainerAttemptAssociationStoreError::InvalidStoredAssociation(error.to_string())
    })?;
    let resource_id = ResourceId::from_uuid(resource_uuid);
    let authority_machine = MachineId::from_uuid(authority_uuid);
    let task_id = TaskId(task_uuid);
    let association: StoredTrainerAttemptAssociation =
        serde_json::from_str(&association_json).map_err(parse_error)?;

    association.into_association(resource_id, authority_machine, task_id)
}

#[derive(Debug)]
struct TrainerAssociationBindSnapshot {
    task_row: TaskRow,
    normalized_spec: NormalizedSpec,
    normalized_spec_sha256: NormalizedSpecSha256,
    identity_state: ProcessStatus,
}

struct ReleaseProofPreflight {
    task_id: TaskId,
    association: TrainerAttemptAssociation,
    association_json: String,
    identity_json: String,
    task_row: TaskRow,
    normalized_spec: NormalizedSpec,
    terminal_evidence: ReleaseProofTerminalEvidence,
}

enum ReleaseProofTerminalEvidence {
    Completed,
    Stopped {
        decision: Box<ReleaseCheckpointStopDecision>,
        cancellation: ReleaseCheckpointCancellation,
    },
}

fn trainer_association_bind_snapshot(
    conn: &rusqlite::Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    task_id: TaskId,
) -> Result<TrainerAssociationBindSnapshot, TrainerAttemptAssociationStoreError> {
    let resource =
        select_resource(conn, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_resource_authority(conn, resource_id, Some(authority_machine))?;
    if resource.registered_background_task != Some(task_id) {
        return Err(TrainerAttemptAssociationStoreError::TaskNotRegistered { task_id });
    }

    let task_row = super::task_by_id_on(conn, task_id)?
        .ok_or(TrainerAttemptAssociationStoreError::TaskMissing { task_id })?;
    let identity = super::identity::executor_identity_on(conn, task_id)?
        .ok_or(TrainerAttemptAssociationStoreError::IdentityMissing { task_id })?;
    let ExecutorIdentity::Accepted(record) = identity else {
        return Err(TrainerAttemptAssociationStoreError::IdentityNotAccepted { task_id });
    };
    if record.task != task_id
        || record.execution_machine != authority_machine
        || !record.has_valid_spec_owners()
    {
        return Err(TrainerAttemptAssociationStoreError::IdentityMismatch { task_id });
    }

    let normalized_spec = record
        .current_spec()
        .ok_or(TrainerAttemptAssociationStoreError::NormalizedSpecMissing { task_id })?
        .clone();
    let normalized_spec_sha256 =
        normalized_spec_sha256(&normalized_spec).map_err(crate::error::AppError::from)?;

    Ok(TrainerAssociationBindSnapshot {
        task_row,
        normalized_spec,
        normalized_spec_sha256,
        identity_state: record.state,
    })
}

fn trainer_association_by_resource_and_task(
    conn: &rusqlite::Connection,
    resource_id: ResourceId,
    task_id: TaskId,
) -> Result<Option<TrainerAttemptAssociation>, TrainerAttemptAssociationStoreError> {
    let row = conn
        .query_row(
            "SELECT resource_id, authority_machine, task_id, association_json
             FROM trainer_attempt_associations WHERE resource_id=?1 AND task_id=?2",
            params![resource_id.as_uuid().to_string(), task_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;
    row.map(|(resource, authority, task, association)| {
        decode_trainer_association(resource, authority, task, association)
    })
    .transpose()
}

fn trainer_association_by_task(
    conn: &rusqlite::Connection,
    task_id: TaskId,
) -> Result<Option<TrainerAttemptAssociation>, TrainerAttemptAssociationStoreError> {
    let row = conn
        .query_row(
            "SELECT resource_id, authority_machine, task_id, association_json
             FROM trainer_attempt_associations WHERE task_id=?1",
            [task_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;
    row.map(|(resource, authority, task, association)| {
        decode_trainer_association(resource, authority, task, association)
    })
    .transpose()
}

fn release_checkpoint_binding_on(
    conn: &rusqlite::Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    action: &ReleaseCheckpointAction,
) -> Result<ReleaseCheckpointBinding, ReleaseCheckpointError> {
    let loan = select_non_closed_loan(conn, resource_id)?.ok_or(
        ReleaseCheckpointError::NotAwaitingRelease {
            action_id: action.action_id,
        },
    )?;
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task,
                watcher_intent,
            },
    } = loan.state
    else {
        return Err(ReleaseCheckpointError::NotAwaitingRelease {
            action_id: action.action_id,
        });
    };
    if action.resource_id != resource_id
        || action.action_id != action_id
        || action.observed_background_task != observed_background_task
    {
        return Err(ReleaseCheckpointError::Conflict);
    }
    let saved_intent = watcher_intent.ok_or(ReleaseCheckpointError::WatcherIntentMissing {
        action_id: action.action_id,
    })?;
    let Some(intent) = saved_intent.complete().cloned() else {
        return Err(ReleaseCheckpointError::LegacyUnproven {
            resource_id,
            action_id: action.action_id,
        });
    };
    validate_release_watcher_intent_on(conn, authority_machine, resource_id, &intent)?;

    let association = trainer_association_by_resource_and_task(
        conn,
        resource_id,
        action.observed_background_task,
    )?
    .ok_or(ReleaseCheckpointError::TrainerAssociationMissing {
        task_id: action.observed_background_task,
    })?;
    if association.resource_id() != resource_id
        || association.authority_machine() != authority_machine
        || association.task_id() != action.observed_background_task
    {
        return Err(ReleaseCheckpointError::Conflict);
    }

    let binding = ReleaseCheckpointBinding {
        action: action.clone(),
        association: TrainerAttemptAssociationProof::from(&association),
        attempt_binding: association.verified_attempt().binding().clone(),
        watcher_intent: intent,
    };
    binding
        .validate_for(action)
        .map_err(ReleaseCheckpointError::InvalidStoredEvidence)?;

    Ok(binding)
}

fn revalidate_selected_checkpoint(
    binding: &ReleaseCheckpointBinding,
    decision: &ReleaseCheckpointStopDecision,
    action_id: ActionId,
) -> Result<(), ReleaseCheckpointError> {
    match revalidate_checkpoint_publication(
        &binding.association.canonical_runtime_root,
        &binding.attempt_binding,
        &decision.selected_checkpoint,
    ) {
        Ok(true) => Ok(()),
        Ok(false)
        | Err(crate::resource::watcher::WatcherError::MalformedPublication { .. })
        | Err(crate::resource::watcher::WatcherError::Symlink { .. }) => {
            Err(ReleaseCheckpointError::SelectedCheckpointChanged { action_id })
        }
        Err(error) => Err(ReleaseCheckpointError::Watcher(error)),
    }
}

fn release_watcher_is_running_on(
    conn: &rusqlite::Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    intent: &ReleaseWatcherIntent,
) -> Result<bool, ReleaseCheckpointError> {
    let task_id = intent.watcher_task_id.as_task_id();
    let task = super::task_by_id_on(conn, task_id)?;
    let identity =
        super::identity::executor_identity_on(conn, task_id).map_err(ResourceStoreError::from)?;
    let by_request = super::identity::origin_route_by_request_on(conn, intent.request_id)
        .map_err(ResourceStoreError::from)?;
    let by_task = super::identity::origin_route_by_task_on(conn, task_id)
        .map_err(ResourceStoreError::from)?;
    let (Some(task), Some(identity), Some(by_request), Some(by_task)) =
        (task, identity, by_request, by_task)
    else {
        return Ok(false);
    };
    let Some(resource) = select_resource(conn, resource_id)? else {
        return Err(ResourceStoreError::ResourceNotFound.into());
    };
    if resource.authority_machine() != authority_machine {
        return Err(ResourceStoreError::WrongAuthority {
            expected: resource.authority_machine(),
            found: authority_machine,
        }
        .into());
    }
    if resource.supervisor.machine != authority_machine {
        return Err(ReleaseCheckpointError::WatcherIdentityConflict { task_id });
    }
    let ExecutorIdentity::Accepted(record) = &identity else {
        return Ok(false);
    };
    let Some(spec) = record.current_spec() else {
        return Err(ReleaseCheckpointError::WatcherIdentityConflict { task_id });
    };
    if record.task != task_id
        || record.origin_machine != authority_machine
        || record.execution_machine != authority_machine
        || !record.has_valid_spec_owners()
        || spec.machine.is_some()
        || normalized_spec_sha256(spec)? != intent.normalized_spec_sha256
        || !matches!(&spec.workload, crate::spec::NormalizedWorkload::Task(_))
        || !resource_task_row_matches(&task, task_id, spec)
    {
        return Err(ReleaseCheckpointError::WatcherIdentityConflict { task_id });
    }

    if task.status() != ProcessStatus::Running
        || task.pid().is_none()
        || record.state != task.status()
    {
        return Ok(false);
    }
    if !watcher_routes_match(
        &by_request,
        &by_task,
        (intent.request_id, task_id),
        authority_machine,
        resource.supervisor,
        &by_request.callback,
        spec,
    )? || !watcher_identity_matches(&identity, task_id, authority_machine, spec, task.status())?
    {
        return Err(ReleaseCheckpointError::WatcherIdentityConflict { task_id });
    }

    saved_initial_watcher_event(conn, task_id, authority_machine)
        .map_err(ReleaseCheckpointError::Task)
}

fn missing_release_checkpoint_state_error(
    conn: &rusqlite::Connection,
    resource_id: ResourceId,
    action_id: ActionId,
) -> Result<ReleaseCheckpointError, ReleaseCheckpointError> {
    let Some(loan) = select_non_closed_loan(conn, resource_id)? else {
        return Ok(ReleaseCheckpointError::Conflict);
    };
    match loan.state {
        LoanState::Active {
            phase:
                LoanPhase::AwaitingRelease {
                    action_id: current_action,
                    ..
                },
        } if current_action == action_id => Ok(ReleaseCheckpointError::LegacyUnproven {
            resource_id,
            action_id,
        }),
        _ => Ok(ReleaseCheckpointError::Conflict),
    }
}

fn validate_release_checkpoint_baseline_on(
    conn: &rusqlite::Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    action_id: ActionId,
) -> Result<(), ReleaseCheckpointError> {
    let Some((state, _)) = release_checkpoint_state_for_action(conn, resource_id, action_id)?
    else {
        return Err(ReleaseCheckpointError::LegacyUnproven {
            resource_id,
            action_id,
        });
    };
    let binding =
        release_checkpoint_binding_on(conn, authority_machine, resource_id, &state.action)?;
    match state.phase {
        ReleaseCheckpointPhase::WatcherBindingPending => {
            Err(ReleaseCheckpointError::BaselineMissing { action_id })
        }
        ReleaseCheckpointPhase::BaselineCaptured { baseline } if baseline.binding == binding => {
            Ok(())
        }
        ReleaseCheckpointPhase::StopReserved { baseline, decision }
        | ReleaseCheckpointPhase::CancellationCommitted {
            baseline, decision, ..
        } if baseline.binding == binding && decision.binding == binding => Ok(()),
        _ => Err(ReleaseCheckpointError::Conflict),
    }
}

/// Classify a checkpoint failure as watcher attention, keeping storage failures retryable
fn release_watcher_poll_attention(
    error: ReleaseCheckpointError,
) -> Result<ReleaseWatcherPollAttention, ReleaseCheckpointError> {
    let reason = match error {
        ReleaseCheckpointError::Storage(_)
        | ReleaseCheckpointError::Serialization(_)
        | ReleaseCheckpointError::Task(_)
        | ReleaseCheckpointError::Resource(ResourceStoreError::Storage(_))
        | ReleaseCheckpointError::TrainerAssociation(
            TrainerAttemptAssociationStoreError::Storage(_),
        ) => return Err(error),
        ReleaseCheckpointError::Resource(ResourceStoreError::WrongAuthority { .. }) => {
            ReleaseWatcherPollAttention::WrongAuthority
        }
        ReleaseCheckpointError::Resource(ResourceStoreError::LegacyWatcherIntentUnproven)
        | ReleaseCheckpointError::LegacyUnproven { .. }
        | ReleaseCheckpointError::WatcherIntentMissing { .. } => {
            ReleaseWatcherPollAttention::LegacyWatcherIntent
        }
        ReleaseCheckpointError::WatcherIdentityConflict { .. } => {
            ReleaseWatcherPollAttention::WatcherIdentityConflict
        }
        ReleaseCheckpointError::TrainerAssociation(_)
        | ReleaseCheckpointError::TrainerAssociationMissing { .. }
        | ReleaseCheckpointError::TaskMissing { .. }
        | ReleaseCheckpointError::TrainerTaskNotRunning { .. }
        | ReleaseCheckpointError::TrainerCommandBindingChanged { .. }
        | ReleaseCheckpointError::TrainerCancellationConflict { .. } => {
            ReleaseWatcherPollAttention::TrainerChanged
        }
        ReleaseCheckpointError::Watcher(_)
        | ReleaseCheckpointError::SelectedCheckpointChanged { .. } => {
            ReleaseWatcherPollAttention::PublicationChanged
        }
        ReleaseCheckpointError::Resource(_)
        | ReleaseCheckpointError::NotAwaitingRelease { .. }
        | ReleaseCheckpointError::BaselineMissing { .. }
        | ReleaseCheckpointError::StopDecisionMissing { .. }
        | ReleaseCheckpointError::StopDecisionMismatch { .. }
        | ReleaseCheckpointError::Conflict
        | ReleaseCheckpointError::InvalidStoredEvidence(_) => {
            ReleaseWatcherPollAttention::ActionNotCurrent
        }
    };

    Ok(reason)
}

fn watcher_poll_attention(attention: &WatcherAttention) -> ReleaseWatcherPollAttention {
    match attention {
        WatcherAttention::LostTask { .. } => ReleaseWatcherPollAttention::TrainerLost,
        WatcherAttention::FailedTask { .. } => ReleaseWatcherPollAttention::TrainerFailed,
        WatcherAttention::PublicationBeforeTaskStart { .. } => {
            ReleaseWatcherPollAttention::PublicationBeforeTrainerStart
        }
        WatcherAttention::SuccessfulTaskWithoutFinalResult { .. } => {
            ReleaseWatcherPollAttention::TrainerCompletedWithoutResult
        }
    }
}

fn decode_resource_json<T: DeserializeOwned>(value: &str) -> Result<T, ResourceStoreError> {
    serde_json::from_str(value).map_err(|error| {
        ResourceStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
    })
}

fn encode_resource_json<T: serde::Serialize>(value: &T) -> Result<String, ResourceStoreError> {
    serde_json::to_string(value).map_err(|error| {
        ResourceStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
    })
}

fn resource_receipt_matches(
    receipt: &ResourceCancellationReceipt,
    identity: &ResourceCancellationRequestIdentity,
) -> bool {
    receipt.cancellation == identity.cancellation
        && receipt.requester_machine == identity.requester_machine
        && receipt.request == identity.request
        && receipt.task == identity.task
        && receipt.origin_machine == identity.origin_machine
        && receipt.authority_machine == identity.authority_machine
        && receipt.resource == identity.resource
        && receipt.target_phase == identity.target_phase
}

fn resource_cancellation_phase_is_forward(
    target: &ResourceRoutePhase,
    current: &ResourceRoutePhase,
) -> bool {
    match target {
        ResourceRoutePhase::AcceptanceUnknown => true,
        ResourceRoutePhase::Waiting => !matches!(current, ResourceRoutePhase::AcceptanceUnknown),
        ResourceRoutePhase::CancelledBeforeLaunch => {
            matches!(current, ResourceRoutePhase::CancelledBeforeLaunch)
        }
        ResourceRoutePhase::Activated | ResourceRoutePhase::Rejected { .. } => target == current,
    }
}

impl Store {
    /// Register a resource only on the daemon that matches its fixed authority.
    pub(crate) fn register_resource(
        &mut self,
        authority_machine: MachineId,
        resource: &Resource,
    ) -> Result<Resource, ResourceStoreError> {
        register_resource_for_authority(&mut self.conn, authority_machine, resource)
    }

    /// Load authority-owned resources and active loans on the store connection.
    pub(crate) fn resource_snapshots_for_authority(
        &self,
        authority_machine: MachineId,
    ) -> Result<Vec<ResourceSnapshot>, ResourceStoreError> {
        load_resources_for_authority(&self.conn, authority_machine)
    }

    /// Bind exact trainer evidence and direct-segment command shape to a registered task
    ///
    /// The accepted identity and running task row are read before file-system validation
    /// A later IMMEDIATE transaction rechecks their immutable binding before insertion
    /// File paths may change between the shape check and that transaction
    /// Exact retries return the saved association without checking the current file-system layout
    /// This shape does not prove live lock use or permit release completion or `Serving`
    pub(crate) fn bind_trainer_attempt_association(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        task_id: TaskId,
        verified_attempt: VerifiedTrainerAttempt,
    ) -> Result<TrainerAttemptAssociation, TrainerAttemptAssociationStoreError> {
        let preflight = {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Deferred)?;
            let snapshot =
                trainer_association_bind_snapshot(&tx, authority_machine, resource_id, task_id)?;

            if let Some(saved) = trainer_association_by_task(&tx, task_id)? {
                if saved.resource_id() != resource_id {
                    return Err(TrainerAttemptAssociationStoreError::TaskAlreadyAssociated {
                        task_id,
                    });
                }
                if saved.authority_machine() != authority_machine
                    || saved.verified_attempt() != &verified_attempt
                    || saved.normalized_spec_sha256() != snapshot.normalized_spec_sha256
                    || !resource_task_row_matches(
                        &snapshot.task_row,
                        task_id,
                        &snapshot.normalized_spec,
                    )
                {
                    return Err(TrainerAttemptAssociationStoreError::Conflict { resource_id });
                }

                tx.commit()?;
                return Ok(saved);
            }

            if !resource_task_row_matches(&snapshot.task_row, task_id, &snapshot.normalized_spec) {
                return Err(
                    TrainerAttemptAssociationStoreError::NormalizedSpecMismatch { task_id },
                );
            }

            if select_non_closed_loan(&tx, resource_id)?.is_some_and(|loan| {
                !matches!(
                    loan.state,
                    LoanState::Active {
                        phase: LoanPhase::AwaitingRelease {
                            observed_background_task,
                            ..
                        }
                    } if observed_background_task == task_id
                )
            }) {
                return Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict {
                    resource_id,
                });
            }

            if snapshot.task_row.status() != ProcessStatus::Running {
                return Err(TrainerAttemptAssociationStoreError::TaskNotRunning {
                    task_id,
                    state: snapshot.task_row.status().to_string(),
                });
            }
            if snapshot.identity_state != ProcessStatus::Running {
                return Err(TrainerAttemptAssociationStoreError::IdentityNotRunning { task_id });
            }

            tx.commit()?;
            snapshot
        };

        // keep canonical path checks outside the IMMEDIATE write transaction
        crate::resource::command_shape::DirectSegmentCommandShape::validate_binding(
            &preflight.normalized_spec,
            &preflight.task_row,
            task_id,
            preflight.normalized_spec_sha256,
            &verified_attempt,
        )?;

        let association = TrainerAttemptAssociation::from_components(
            resource_id,
            authority_machine,
            task_id,
            verified_attempt,
            preflight.normalized_spec_sha256,
        )
        .map_err(|reason| {
            TrainerAttemptAssociationStoreError::InvalidStoredAssociation(reason.into())
        })?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current =
            trainer_association_bind_snapshot(&tx, authority_machine, resource_id, task_id)?;
        if let Some(saved) = trainer_association_by_task(&tx, task_id)? {
            if saved.resource_id() != resource_id {
                return Err(TrainerAttemptAssociationStoreError::TaskAlreadyAssociated { task_id });
            }
            if saved != association
                || current.normalized_spec_sha256 != preflight.normalized_spec_sha256
                || !same_trainer_task_binding(&current.task_row, &preflight.task_row)
                || !resource_task_row_matches(&current.task_row, task_id, &current.normalized_spec)
            {
                return Err(TrainerAttemptAssociationStoreError::Conflict { resource_id });
            }

            tx.commit()?;
            return Ok(saved);
        }

        if current.normalized_spec_sha256 != preflight.normalized_spec_sha256
            || !same_trainer_task_binding(&current.task_row, &preflight.task_row)
            || !resource_task_row_matches(&current.task_row, task_id, &current.normalized_spec)
        {
            return Err(TrainerAttemptAssociationStoreError::BindingChanged { task_id });
        }

        if select_non_closed_loan(&tx, resource_id)?.is_some_and(|loan| {
            !matches!(
                loan.state,
                LoanState::Active {
                    phase: LoanPhase::AwaitingRelease {
                        observed_background_task,
                        ..
                    }
                } if observed_background_task == task_id
            )
        }) {
            return Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict { resource_id });
        }

        if current.task_row.status() != ProcessStatus::Running {
            return Err(TrainerAttemptAssociationStoreError::TaskNotRunning {
                task_id,
                state: current.task_row.status().to_string(),
            });
        }
        if current.identity_state != ProcessStatus::Running {
            return Err(TrainerAttemptAssociationStoreError::IdentityNotRunning { task_id });
        }

        let association_json = trainer_association_json(&association)?;
        tx.execute(
            "INSERT INTO trainer_attempt_associations (
                resource_id, authority_machine, task_id, association_json
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                resource_id.as_uuid().to_string(),
                authority_machine.as_uuid().to_string(),
                task_id.to_string(),
                association_json,
            ],
        )?;
        tx.commit()?;
        Ok(association)
    }

    /// Read the association for the resource's currently registered task.
    pub(crate) fn trainer_attempt_association_for_authority(
        &self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<Option<TrainerAttemptAssociation>, TrainerAttemptAssociationStoreError> {
        check_resource_authority(&self.conn, resource_id, Some(authority_machine))?;
        let resource = select_resource(&self.conn, resource_id)?
            .ok_or(ResourceStoreError::ResourceNotFound)?;
        let Some(task_id) = resource.registered_background_task else {
            return Ok(None);
        };

        let association =
            trainer_association_by_resource_and_task(&self.conn, resource_id, task_id)?;
        if association
            .as_ref()
            .is_some_and(|saved| saved.authority_machine() != authority_machine)
        {
            return Err(
                TrainerAttemptAssociationStoreError::InvalidStoredAssociation(
                    "association authority does not match its resource".into(),
                ),
            );
        }

        Ok(association)
    }

    /// Read one historical association by exact task without authorizing release or execution.
    pub(crate) fn trainer_attempt_association_for_task_for_authority(
        &self,
        authority_machine: MachineId,
        task_id: TaskId,
    ) -> Result<Option<TrainerAttemptAssociation>, TrainerAttemptAssociationStoreError> {
        let association = trainer_association_by_task(&self.conn, task_id)?;
        let Some(saved) = association else {
            return Ok(None);
        };
        check_resource_authority(&self.conn, saved.resource_id(), Some(authority_machine))?;
        if saved.authority_machine() != authority_machine {
            return Err(
                TrainerAttemptAssociationStoreError::InvalidStoredAssociation(
                    "association authority does not match its resource".into(),
                ),
            );
        }

        Ok(Some(saved))
    }

    /// Read accepted resource task identities from their authority-owned assignments.
    pub(crate) fn accepted_resource_tasks_for_authority(
        &self,
        authority_machine: MachineId,
    ) -> Result<Vec<AcceptedResourceTask>, ResourceStoreError> {
        let requests = assigned_resource_requests_for_authority(&self.conn, authority_machine)?;
        let mut accepted = Vec::new();

        for request in requests {
            let crate::resource::ResourceRequestState::Assigned { loan_id } = &request.state else {
                continue;
            };
            let loan_id = *loan_id;
            let executor = super::identity::executor_identity_on(&self.conn, request.task_id)?;
            let Some(executor) = executor else {
                let task_row = super::task_by_id_on(&self.conn, request.task_id)
                    .map_err(ResourceStoreError::TaskRow)?;
                if task_row.is_some() || task_has_any_event(&self.conn, request.task_id)? {
                    return Err(ResourceStoreError::Conflict);
                }
                continue;
            };
            let ExecutorIdentity::Accepted(record) = executor else {
                return Err(ResourceStoreError::Conflict);
            };
            let loan_is_reserved: bool = self.conn.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM loans
                    WHERE id=?1 AND resource_id=?2
                      AND json_extract(state_json, '$.type') != 'closed'
                )",
                rusqlite::params![
                    loan_id.as_uuid().to_string(),
                    request.resource_id.as_uuid().to_string()
                ],
                |row| row.get(0),
            )?;
            if !loan_is_reserved {
                return Err(ResourceStoreError::Conflict);
            }
            let saved_origin: String = self.conn.query_row(
                "SELECT origin_machine FROM executor_identities WHERE task_id=?1",
                [request.task_id.to_string()],
                |row| row.get(0),
            )?;
            let task_row = super::task_by_id_on(&self.conn, request.task_id)
                .map_err(ResourceStoreError::TaskRow)?;
            let event_matches = super::events::initial_queued_event_matches_on(
                &self.conn,
                request.task_id,
                request.origin_machine,
                authority_machine,
            )?;
            let same_spec = record
                .current_spec()
                .map(|spec| same_resource_spec(spec, request.spec().as_normalized()))
                .transpose()?
                .unwrap_or(false);

            if record.task != request.task_id
                || record.origin_machine != request.origin_machine
                || record.execution_machine != authority_machine
                || !record.has_valid_spec_owners()
                || !same_spec
                || saved_origin != request.origin_machine.to_string()
                || !task_has_any_event(&self.conn, request.task_id)?
                || !event_matches
                || !task_row.as_ref().is_some_and(|row| {
                    resource_task_row_matches(row, request.task_id, request.spec().as_normalized())
                        && row.status() == record.state
                })
            {
                return Err(ResourceStoreError::Conflict);
            }

            accepted.push(AcceptedResourceTask {
                request,
                loan_id,
                state: record.state,
            });
        }

        Ok(accepted)
    }

    /// Accept a resource request and assign its authority-local FIFO sequence.
    ///
    /// The origin route must be persisted by the caller before it sends this request.
    pub(crate) fn accept_resource_request(
        &mut self,
        authority_machine: MachineId,
        request_id: RequestId,
        task_id: TaskId,
        resource_id: ResourceId,
        origin_machine: MachineId,
        normalized_spec: NormalizedSpec,
    ) -> Result<ResourceRequest, ResourceStoreError> {
        accept_request_for_authority(
            &mut self.conn,
            authority_machine,
            request_id,
            task_id,
            resource_id,
            origin_machine,
            normalized_spec,
        )
    }

    /// Accept one selected resource request as a task on this store connection.
    pub(crate) fn accept_assigned_resource_task(
        &mut self,
        input: ResourceTaskAcceptanceInput,
    ) -> Result<ResourceTaskAcceptance, ResourceStoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let request = assigned_resource_request_for_acceptance(&tx, &input)?;
        let normalized_spec = request.spec().as_normalized();
        let executor = super::identity::executor_identity_on(&tx, input.task_id)?;
        let executor_owner_matches = if executor.is_some() {
            let saved_origin: String = tx.query_row(
                "SELECT origin_machine FROM executor_identities WHERE task_id=?1",
                [input.task_id.to_string()],
                |row| row.get(0),
            )?;
            saved_origin == request.origin_machine.to_string()
        } else {
            true
        };
        let task_row = super::task_by_id_on(&tx, input.task_id)?;
        let has_event = task_has_any_event(&tx, input.task_id)?;
        let event_matches = super::events::initial_queued_event_matches_on(
            &tx,
            input.task_id,
            request.origin_machine,
            input.authority_machine,
        )?;

        validate_resource_task_route(
            &tx,
            &request,
            input.authority_machine,
            normalized_spec,
            executor.is_some(),
        )?;

        if let Some(executor) = executor {
            let ExecutorIdentity::Accepted(record) = executor else {
                return Err(ResourceStoreError::Conflict);
            };
            let same_spec = record
                .current_spec()
                .map(|saved| same_resource_spec(saved, normalized_spec))
                .transpose()?
                .unwrap_or(false);
            if record.task != input.task_id
                || record.origin_machine != request.origin_machine
                || record.execution_machine != input.authority_machine
                || !record.has_valid_spec_owners()
                || !same_spec
                || !executor_owner_matches
                || !has_event
                || !event_matches
                || !task_row.as_ref().is_some_and(|row| {
                    resource_task_row_matches(row, input.task_id, normalized_spec)
                        && row.env == input.executor_env
                        && row.status() == record.state
                })
            {
                return Err(ResourceStoreError::Conflict);
            }

            tx.commit()?;
            return Ok(ResourceTaskAcceptance::Existing {
                task: input.task_id,
                state: record.state,
            });
        }

        if task_row.is_some() || has_event || event_matches {
            return Err(ResourceStoreError::Conflict);
        }
        if super::release_watcher_task_id_is_reserved(&tx, input.task_id)? {
            return Err(ResourceStoreError::Conflict);
        }

        crate::spec::check_cwd(&normalized_spec.cwd)
            .map_err(ResourceStoreError::TaskPreparation)?;
        let workload = crate::invocation::persist_workload(&normalized_spec.workload);
        let binary = crate::invocation::resolve_workload_binary(
            &normalized_spec.workload,
            &input.executor_env.path,
            &normalized_spec.cwd,
        )
        .map_err(ResourceStoreError::TaskPreparation)?;
        let row = super::new_queued_task(super::NewTask {
            id: input.task_id,
            name: Some(normalized_spec.name.clone()),
            thread: normalized_spec.thread,
            workload,
            cwd: normalized_spec.cwd.clone(),
            timeout: normalized_spec.timeout,
            env: input.executor_env,
            binary,
        });
        if !resource_task_row_matches(&row, input.task_id, normalized_spec) {
            return Err(ResourceStoreError::Conflict);
        }

        let project_root = super::find_project_root(&row.cwd);
        super::insert_task_with_project_root_on(&tx, &row, project_root.as_deref())
            .map_err(ResourceStoreError::TaskPreparation)?;

        let identity = ExecutorIdentity::Accepted(ExecutionRecord {
            task: input.task_id,
            origin_machine: request.origin_machine,
            execution_machine: input.authority_machine,
            spec: normalized_spec.clone().into(),
            state: ProcessStatus::Queued,
        });
        tx.execute(
            "INSERT INTO executor_identities (task_id,origin_machine,identity_json)
             VALUES (?1,?2,?3)",
            rusqlite::params![
                input.task_id.to_string(),
                request.origin_machine.to_string(),
                encode_resource_json(&identity)?,
            ],
        )?;
        super::events::append_produced_event_on(
            &tx,
            input.task_id,
            EventPayload::State {
                status: ProcessStatus::Queued,
            },
        )?;

        tx.commit()?;
        Ok(ResourceTaskAcceptance::Inserted {
            task: input.task_id,
        })
    }

    /// Reconcile one exact Serving task and commit completion only with release evidence
    pub(crate) fn reconcile_assigned_resource_task_for_authority(
        &mut self,
        input: AssignedResourceTaskReconcileInput,
    ) -> Result<AssignedResourceTaskReconcileOutcome, ResourceStoreError> {
        persist_assigned_task_reconciliation(&mut self.conn, input)
    }

    /// Read a resource's requests in authority-assigned FIFO order.
    pub(crate) fn resource_requests(
        &self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
        requests_for_resource_for_authority(&self.conn, authority_machine, resource_id)
    }

    /// Read the oldest queued request for one resource.
    pub(crate) fn oldest_queued_resource_request(
        &self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<Option<ResourceRequest>, ResourceStoreError> {
        oldest_queued_request_for_authority(&self.conn, authority_machine, resource_id)
    }

    /// Cancel a queued request or atomically retain prevention before acceptance.
    pub(crate) fn cancel_resource_request_before_activation(
        &mut self,
        authority_machine: MachineId,
        request_id: RequestId,
        task_id: TaskId,
        resource_id: ResourceId,
        origin_machine: MachineId,
    ) -> Result<QueueCancellationResult, ResourceStoreError> {
        cancel_request_before_activation_for_authority(
            &mut self.conn,
            authority_machine,
            request_id,
            task_id,
            resource_id,
            origin_machine,
        )
    }

    /// Return the exact saved authority receipt for one cancellation identity.
    pub(crate) fn resource_cancellation_receipt(
        &self,
        identity: &ResourceCancellationRequestIdentity,
    ) -> Result<Option<ResourceCancellationReceipt>, ResourceStoreError> {
        let saved: Option<(String, String)> = self
            .conn
            .query_row(
                "SELECT request_json, receipt_json FROM resource_cancellation_receipts
                 WHERE cancellation_id=?1",
                [identity.cancellation.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((request_json, receipt_json)) = saved else {
            return Ok(None);
        };
        let saved_identity: ResourceCancellationRequestIdentity =
            decode_resource_json(&request_json)?;
        let receipt: ResourceCancellationReceipt = decode_resource_json(&receipt_json)?;
        if saved_identity != *identity || !resource_receipt_matches(&receipt, identity) {
            return Err(ResourceStoreError::Conflict);
        }
        Ok(Some(receipt))
    }

    /// Apply one resource cancellation through the queue transaction and retain its exact result.
    pub(crate) fn cancel_resource_request_with_receipt(
        &mut self,
        authority_machine: MachineId,
        identity: ResourceCancellationRequestIdentity,
        proof: crate::submission::ResourceRouteProof,
    ) -> Result<ResourceCancellationReceipt, ResourceStoreError> {
        if identity.requester_machine != identity.origin_machine
            || identity.authority_machine != authority_machine
            || proof.request != identity.request
            || proof.task != identity.task
            || proof.resource != identity.resource
            || proof.origin_machine != identity.origin_machine
            || proof.authority_machine != authority_machine
            || !resource_cancellation_phase_is_forward(&identity.target_phase, &proof.phase)
        {
            return Err(ResourceStoreError::Conflict);
        }
        if let Some(receipt) = self.resource_cancellation_receipt(&identity)? {
            return Ok(receipt);
        }

        let requests = match self.resource_requests(authority_machine, identity.resource) {
            Ok(requests) => requests,
            Err(ResourceStoreError::ResourceNotFound)
                if matches!(&proof.phase, ResourceRoutePhase::Rejected { .. }) =>
            {
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        let retained = requests.iter().find(|request| {
            request.request_id == identity.request || request.task_id == identity.task
        });
        if let Some(retained) = retained {
            let saved_digest =
                normalized_spec_sha256(retained.spec().as_normalized()).map_err(|error| {
                    ResourceStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(
                        error,
                    )))
                })?;
            if retained.request_id != identity.request
                || retained.task_id != identity.task
                || retained.resource_id != identity.resource
                || retained.origin_machine != identity.origin_machine
                || saved_digest != proof.normalized_spec_sha256
            {
                return Err(ResourceStoreError::Conflict);
            }
        } else if matches!(
            &proof.phase,
            ResourceRoutePhase::Waiting | ResourceRoutePhase::Activated
        ) {
            return Err(ResourceStoreError::Conflict);
        }

        let outcome = match proof.phase {
            ResourceRoutePhase::Activated => ResourceCancellationOutcome::NotEligible {
                reason: ResourceCancellationIneligibleReason::Activated,
            },
            ResourceRoutePhase::Rejected { reason } => ResourceCancellationOutcome::NotEligible {
                reason: ResourceCancellationIneligibleReason::Rejected { reason },
            },
            ResourceRoutePhase::AcceptanceUnknown
            | ResourceRoutePhase::Waiting
            | ResourceRoutePhase::CancelledBeforeLaunch => {
                let queue_result = self.cancel_resource_request_before_activation(
                    authority_machine,
                    identity.request,
                    identity.task,
                    identity.resource,
                    identity.origin_machine,
                );
                match queue_result {
                    Ok(
                        crate::resource::store::QueueCancellationResult::PreventedBeforeAcceptance,
                    ) => ResourceCancellationOutcome::PreventedBeforeAcceptance,
                    Ok(crate::resource::store::QueueCancellationResult::Request(request)) => {
                        match request.state {
                            crate::resource::ResourceRequestState::CancelledBeforeLaunch => {
                                ResourceCancellationOutcome::CancelledBeforeLaunch
                            }
                            crate::resource::ResourceRequestState::Finished { .. } => {
                                ResourceCancellationOutcome::NotEligible {
                                    reason: ResourceCancellationIneligibleReason::Terminal,
                                }
                            }
                            crate::resource::ResourceRequestState::Rejected { reason } => {
                                ResourceCancellationOutcome::NotEligible {
                                    reason: ResourceCancellationIneligibleReason::Rejected {
                                        reason,
                                    },
                                }
                            }
                            crate::resource::ResourceRequestState::Queued
                            | crate::resource::ResourceRequestState::Assigned { .. } => {
                                return Err(ResourceStoreError::Conflict);
                            }
                        }
                    }
                    Err(ResourceStoreError::ExecutorAlreadyAccepted { .. }) => {
                        ResourceCancellationOutcome::NotEligible {
                            reason: ResourceCancellationIneligibleReason::Activated,
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        };

        let receipt = ResourceCancellationReceipt {
            cancellation: identity.cancellation,
            requester_machine: identity.requester_machine,
            request: identity.request,
            task: identity.task,
            origin_machine: identity.origin_machine,
            authority_machine: identity.authority_machine,
            resource: identity.resource,
            target_phase: identity.target_phase.clone(),
            outcome,
        };
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let saved: Option<(String, String)> = tx
            .query_row(
                "SELECT request_json, receipt_json FROM resource_cancellation_receipts
                 WHERE cancellation_id=?1",
                [identity.cancellation.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((request_json, receipt_json)) = saved {
            let saved_identity: ResourceCancellationRequestIdentity =
                decode_resource_json(&request_json)?;
            let saved_receipt: ResourceCancellationReceipt = decode_resource_json(&receipt_json)?;
            if saved_identity != identity || !resource_receipt_matches(&saved_receipt, &identity) {
                return Err(ResourceStoreError::Conflict);
            }
            tx.commit()?;
            return Ok(saved_receipt);
        }
        tx.execute(
            "INSERT INTO resource_cancellation_receipts
             (cancellation_id, request_json, receipt_json) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                identity.cancellation.to_string(),
                encode_resource_json(&identity)?,
                encode_resource_json(&receipt)?,
            ],
        )?;
        tx.commit()?;
        Ok(receipt)
    }

    /// Open or reuse the authority's release loan on this store connection.
    pub(crate) fn open_release_loan_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        expected_state_revision: ResourceRevision,
    ) -> Result<OpenReleaseLoanResult, OpenReleaseLoanError> {
        persist_release_loan_for_authority(
            &mut self.conn,
            authority_machine,
            resource_id,
            expected_state_revision,
        )
    }

    /// Reconcile queued work from authority-owned resource, loan, and task rows.
    pub(crate) fn reconcile_resource_queue_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<ResourceQueueReconcileOutcome, ResourceQueueReconcileError> {
        persist_resource_queue_reconciliation(&mut self.conn, authority_machine, resource_id)
    }

    /// Bind one preallocated watcher launch identity to its saved release action
    pub(crate) fn bind_release_watcher_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        intent: ReleaseWatcherIntent,
    ) -> Result<ReleaseWatcherIntent, ResourceStoreError> {
        persist_release_watcher_binding(&mut self.conn, authority_machine, resource_id, intent)
    }

    /// Capture the authority-built checkpoint baseline for one bound release watcher
    pub(crate) fn capture_release_checkpoint_baseline_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        action_id: ActionId,
        expected_state_revision: ResourceRevision,
    ) -> Result<ReleaseCheckpointBaseline, ReleaseCheckpointError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some((mut state, previous_json)) =
            release_checkpoint_state_for_action(&tx, resource_id, action_id)?
        else {
            return Err(missing_release_checkpoint_state_error(
                &tx,
                resource_id,
                action_id,
            )?);
        };
        if state.action.resource_id != resource_id
            || state.action.action_id != action_id
            || state.action.state_revision != expected_state_revision
        {
            return Err(ReleaseCheckpointError::Conflict);
        }

        let binding =
            release_checkpoint_binding_on(&tx, authority_machine, resource_id, &state.action)?;
        match &state.phase {
            ReleaseCheckpointPhase::BaselineCaptured { baseline }
            | ReleaseCheckpointPhase::StopReserved { baseline, .. }
            | ReleaseCheckpointPhase::CancellationCommitted { baseline, .. } => {
                if baseline.binding != binding {
                    return Err(ReleaseCheckpointError::Conflict);
                }
                tx.commit()?;
                return Ok(baseline.clone());
            }
            ReleaseCheckpointPhase::WatcherBindingPending => {}
        }

        let checkpoint_baseline = ReleaseCheckpointBaseline {
            binding: binding.clone(),
            snapshot: snapshot(
                &binding.association.canonical_runtime_root,
                &binding.attempt_binding,
            )?,
        };
        state.phase = ReleaseCheckpointPhase::BaselineCaptured {
            baseline: checkpoint_baseline.clone(),
        };
        update_release_checkpoint_state(&tx, &previous_json, &state)?;
        tx.commit()?;

        Ok(checkpoint_baseline)
    }

    /// Reserve the exact task stop decision after a matching new checkpoint is verified
    pub(crate) fn reserve_release_checkpoint_stop_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        action_id: ActionId,
        expected_state_revision: ResourceRevision,
    ) -> Result<ReleaseCheckpointStopOutcome, ReleaseCheckpointError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some((mut state, previous_json)) =
            release_checkpoint_state_for_action(&tx, resource_id, action_id)?
        else {
            return Err(missing_release_checkpoint_state_error(
                &tx,
                resource_id,
                action_id,
            )?);
        };
        if state.action.resource_id != resource_id
            || state.action.action_id != action_id
            || state.action.state_revision != expected_state_revision
        {
            return Err(ReleaseCheckpointError::Conflict);
        }

        let binding =
            release_checkpoint_binding_on(&tx, authority_machine, resource_id, &state.action)?;
        let baseline = match &state.phase {
            ReleaseCheckpointPhase::WatcherBindingPending => {
                return Err(ReleaseCheckpointError::BaselineMissing { action_id });
            }
            ReleaseCheckpointPhase::BaselineCaptured { baseline } => baseline.clone(),
            ReleaseCheckpointPhase::StopReserved { baseline, decision }
            | ReleaseCheckpointPhase::CancellationCommitted {
                baseline, decision, ..
            } => {
                if baseline.binding != binding || decision.binding != binding {
                    return Err(ReleaseCheckpointError::Conflict);
                }
                tx.commit()?;
                return Ok(ReleaseCheckpointStopOutcome::AlreadyReserved(
                    decision.as_ref().clone(),
                ));
            }
        };
        if baseline.binding != binding {
            return Err(ReleaseCheckpointError::Conflict);
        }

        let task = super::task_by_id_on(&tx, state.action.observed_background_task)?.ok_or(
            ReleaseCheckpointError::TaskMissing {
                task_id: state.action.observed_background_task,
            },
        )?;
        let (observation, checkpoint) = observe_release_with_checkpoint_evidence(
            &binding.association.canonical_runtime_root,
            &binding.attempt_binding,
            &baseline.snapshot,
            &task.state,
        )?;
        let Some(checkpoint) = checkpoint else {
            let outcome = match observation {
                WatchObservation::WaitingForTaskStart => {
                    ReleaseCheckpointStopOutcome::WaitingForTaskStart
                }
                WatchObservation::WaitingForCheckpoint => {
                    ReleaseCheckpointStopOutcome::WaitingForCheckpoint
                }
                WatchObservation::CompletedResultAwaitingTaskExit { .. } => {
                    ReleaseCheckpointStopOutcome::CompletedResultAwaitingTaskExit
                }
                WatchObservation::AlreadyCompletedCandidate { .. } => {
                    ReleaseCheckpointStopOutcome::AlreadyCompleted
                }
                WatchObservation::Attention(attention) => {
                    ReleaseCheckpointStopOutcome::Attention(attention)
                }
                WatchObservation::StopRequestCandidate { .. } => {
                    return Err(ReleaseCheckpointError::Conflict);
                }
            };
            tx.commit()?;
            return Ok(outcome);
        };

        if !matches!(observation, WatchObservation::StopRequestCandidate { .. }) {
            return Err(ReleaseCheckpointError::Conflict);
        }

        let decision = ReleaseCheckpointStopDecision {
            binding,
            reservation_id: ReleaseStopReservationId::new(),
            selected_checkpoint: checkpoint,
        };
        state.phase = ReleaseCheckpointPhase::StopReserved {
            baseline,
            decision: Box::new(decision.clone()),
        };
        update_release_checkpoint_state(&tx, &previous_json, &state)?;
        tx.commit()?;

        Ok(ReleaseCheckpointStopOutcome::Reserved(decision))
    }

    /// Revalidate the fixed selected checkpoint without selecting a replacement
    pub(crate) fn revalidate_release_checkpoint_stop_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        action_id: ActionId,
        expected_state_revision: ResourceRevision,
    ) -> Result<ReleaseCheckpointStopDecision, ReleaseCheckpointError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some((state, _)) = release_checkpoint_state_for_action(&tx, resource_id, action_id)?
        else {
            return Err(missing_release_checkpoint_state_error(
                &tx,
                resource_id,
                action_id,
            )?);
        };
        if state.action.resource_id != resource_id
            || state.action.action_id != action_id
            || state.action.state_revision != expected_state_revision
        {
            return Err(ReleaseCheckpointError::Conflict);
        }
        let binding =
            release_checkpoint_binding_on(&tx, authority_machine, resource_id, &state.action)?;
        let decision = match state.phase {
            ReleaseCheckpointPhase::StopReserved { decision, .. }
            | ReleaseCheckpointPhase::CancellationCommitted { decision, .. } => decision,
            ReleaseCheckpointPhase::WatcherBindingPending
            | ReleaseCheckpointPhase::BaselineCaptured { .. } => {
                return Err(ReleaseCheckpointError::StopDecisionMissing { action_id });
            }
        };
        if decision.binding != binding {
            return Err(ReleaseCheckpointError::Conflict);
        }

        revalidate_selected_checkpoint(&binding, &decision, action_id)?;

        tx.commit()?;
        Ok(*decision)
    }

    /// Atomically commit the exact saved stop decision and its trainer cancel marker
    pub(crate) fn commit_release_checkpoint_cancellation_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        action_id: ActionId,
        expected_state_revision: ResourceRevision,
        expected_decision: &ReleaseCheckpointStopDecision,
    ) -> Result<ReleaseCheckpointCancellationOutcome, ReleaseCheckpointError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some((mut state, previous_json)) =
            release_checkpoint_state_for_action(&tx, resource_id, action_id)?
        else {
            return Err(missing_release_checkpoint_state_error(
                &tx,
                resource_id,
                action_id,
            )?);
        };
        if state.action.resource_id != resource_id
            || state.action.action_id != action_id
            || state.action.state_revision != expected_state_revision
        {
            return Err(ReleaseCheckpointError::Conflict);
        }

        let binding =
            release_checkpoint_binding_on(&tx, authority_machine, resource_id, &state.action)?;
        match &state.phase {
            ReleaseCheckpointPhase::CancellationCommitted {
                baseline,
                decision,
                cancellation,
            } => {
                if decision.as_ref() != expected_decision {
                    return Err(ReleaseCheckpointError::StopDecisionMismatch { action_id });
                }
                if baseline.binding != binding || decision.binding != binding {
                    return Err(ReleaseCheckpointError::Conflict);
                }
                let task = super::task_by_id_on(&tx, cancellation.task_id)?.ok_or(
                    ReleaseCheckpointError::TaskMissing {
                        task_id: cancellation.task_id,
                    },
                )?;
                if task.cancel_requested_at.is_none() {
                    return Err(ReleaseCheckpointError::TrainerCancellationConflict {
                        task_id: cancellation.task_id,
                    });
                }

                let result = ReleaseCheckpointCancellationResult {
                    decision: decision.as_ref().clone(),
                    cancellation: cancellation.clone(),
                };
                tx.commit()?;
                return Ok(ReleaseCheckpointCancellationOutcome::AlreadyCommitted(
                    result,
                ));
            }
            ReleaseCheckpointPhase::StopReserved { baseline, decision } => {
                if decision.as_ref() != expected_decision {
                    return Err(ReleaseCheckpointError::StopDecisionMismatch { action_id });
                }
                if baseline.binding != binding || decision.binding != binding {
                    return Err(ReleaseCheckpointError::Conflict);
                }
            }
            ReleaseCheckpointPhase::WatcherBindingPending
            | ReleaseCheckpointPhase::BaselineCaptured { .. } => {
                return Err(ReleaseCheckpointError::StopDecisionMissing { action_id });
            }
        }

        if !release_watcher_is_running_on(
            &tx,
            authority_machine,
            resource_id,
            &binding.watcher_intent,
        )? {
            tx.commit()?;
            return Ok(ReleaseCheckpointCancellationOutcome::WatcherNotReady {
                watcher_task_id: binding.watcher_intent.watcher_task_id.as_task_id(),
            });
        }

        let task_id = state.action.observed_background_task;
        let trainer =
            trainer_association_bind_snapshot(&tx, authority_machine, resource_id, task_id)?;
        if trainer.normalized_spec_sha256 != binding.association.normalized_spec_sha256
            || !resource_task_row_matches(&trainer.task_row, task_id, &trainer.normalized_spec)
        {
            return Err(ReleaseCheckpointError::TrainerCommandBindingChanged { task_id });
        }
        if trainer.task_row.status() != ProcessStatus::Running {
            return Err(ReleaseCheckpointError::TrainerTaskNotRunning {
                task_id,
                state: trainer.task_row.status(),
            });
        }
        if trainer.identity_state != ProcessStatus::Running {
            return Err(TrainerAttemptAssociationStoreError::IdentityNotRunning { task_id }.into());
        }
        if trainer.task_row.cancel_requested_at.is_some() {
            return Err(ReleaseCheckpointError::TrainerCancellationConflict { task_id });
        }

        let ReleaseCheckpointPhase::StopReserved { baseline, decision } = state.phase else {
            return Err(ReleaseCheckpointError::StopDecisionMissing { action_id });
        };
        revalidate_selected_checkpoint(&binding, &decision, action_id)?;

        let cancel_requested_at = Utc::now();
        let marker = cancel_requested_at.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let changed = tx.execute(
            "UPDATE tasks SET cancel_requested_at = ?1, updated_at = ?1
             WHERE id = ?2 AND status = 'running' AND cancel_requested_at IS NULL",
            params![marker, task_id.to_string()],
        )?;
        if changed != 1 {
            return Err(ReleaseCheckpointError::TrainerCancellationConflict { task_id });
        }

        let cancellation = ReleaseCheckpointCancellation {
            task_id,
            cancel_requested_at,
        };
        state.phase = ReleaseCheckpointPhase::CancellationCommitted {
            baseline,
            decision: Box::new(decision.as_ref().clone()),
            cancellation: cancellation.clone(),
        };
        update_release_checkpoint_state(&tx, &previous_json, &state)?;
        tx.commit()?;

        Ok(ReleaseCheckpointCancellationOutcome::Committed(
            ReleaseCheckpointCancellationResult {
                decision: decision.as_ref().clone(),
                cancellation,
            },
        ))
    }

    /// Accept one fixed release-watcher task and all durable ownership records.
    pub(crate) fn accept_release_watcher_for_authority(
        &mut self,
        input: ReleaseWatcherAcceptanceInput,
    ) -> Result<ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError> {
        let ReleaseWatcherAcceptanceInput {
            authority_machine,
            resource_id,
            supervisor,
            intent,
            row,
            spec,
            callback,
        } = input;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task_id = intent.watcher_task_id.as_task_id();
        let digest = normalized_spec_sha256(&spec).map_err(AppError::from)?;
        if intent.normalized_spec_sha256 != digest
            || task_id != row.id
            || row.thread != supervisor.thread
            || spec.thread != supervisor.thread
            || !row.binary.is_absolute()
            || row.workload != crate::invocation::persist_workload(&spec.workload)
            || callback.env != row.env
            || callback.cwd != row.cwd
            || !matches!(&spec.workload, crate::spec::NormalizedWorkload::Task(_))
        {
            return Err(ReleaseWatcherAcceptanceError::Conflict);
        }
        // the authority accepts only its own command for these exact release identities
        let canonical_digest = ReleaseWatcherCommand::from_intent(resource_id, &intent)
            .normalized_spec_sha256(&row.binary, supervisor.thread)
            .map_err(|_| ReleaseWatcherAcceptanceError::Conflict)?;
        if canonical_digest != digest {
            return Err(ReleaseWatcherAcceptanceError::Conflict);
        }
        super::validate_local_task_acceptance(&row, &spec, &callback)
            .map_err(|_| ReleaseWatcherAcceptanceError::Conflict)?;

        let saved_supervisor = validate_release_watcher_for_local_acceptance(
            &tx,
            authority_machine,
            resource_id,
            supervisor,
            &intent,
        )?;
        if saved_supervisor.machine != authority_machine {
            return Ok(ReleaseWatcherAcceptance::UnsupportedRemoteSupervisor {
                authority_machine,
                supervisor: saved_supervisor,
            });
        }

        validate_release_checkpoint_baseline_on(
            &tx,
            authority_machine,
            resource_id,
            intent.action_id,
        )?;
        bind_release_watcher_intent_on(&tx, authority_machine, resource_id, intent.clone())?;

        if resource_request_identity_exists(&tx, intent.request_id, task_id)? {
            return Err(ReleaseWatcherAcceptanceError::Conflict);
        }
        let saved_task = super::task_by_id_on(&tx, task_id)?;
        let route_by_request = super::identity::origin_route_by_request_on(&tx, intent.request_id)?;
        let route_by_task = super::identity::origin_route_by_task_on(&tx, task_id)?;
        let identity = super::identity::executor_identity_on(&tx, task_id)?;
        let has_event = task_has_any_event(&tx, task_id)?;

        if saved_task.is_none()
            && route_by_request.is_none()
            && route_by_task.is_none()
            && identity.is_none()
            && !has_event
        {
            super::insert_local_task_records_on(
                &tx,
                &row,
                &spec,
                authority_machine,
                intent.request_id,
                &callback,
                Some(task_id),
            )?;
            tx.commit()?;
            return Ok(ReleaseWatcherAcceptance::Inserted { task: task_id });
        }

        let (Some(saved_task), Some(route_by_request), Some(route_by_task), Some(identity)) =
            (saved_task, route_by_request, route_by_task, identity)
        else {
            return Err(ReleaseWatcherAcceptanceError::Conflict);
        };
        if !watcher_task_matches(&saved_task, &row)
            || !watcher_routes_match(
                &route_by_request,
                &route_by_task,
                (intent.request_id, task_id),
                authority_machine,
                supervisor,
                &callback,
                &spec,
            )?
            || !watcher_identity_matches(
                &identity,
                task_id,
                authority_machine,
                &spec,
                saved_task.status(),
            )?
            || !saved_initial_watcher_event(&tx, task_id, authority_machine)?
        {
            return Err(ReleaseWatcherAcceptanceError::Conflict);
        }

        let state = saved_task.state.clone();
        tx.commit()?;
        Ok(ReleaseWatcherAcceptance::Existing {
            task: task_id,
            state,
        })
    }

    /// Serve one co-located release-watcher poll on this store connection
    ///
    /// The authority, action, revision, trainer, saved watcher intent, baseline, and
    /// accepted running watcher identity are validated before any publication is read.
    /// A stop decision is reserved once and commits with the exact trainer cancel
    /// marker; retries reuse that saved decision and never select another checkpoint.
    /// Only storage failures are errors; every domain refusal is typed attention
    pub(crate) fn poll_release_watcher_for_authority(
        &mut self,
        authority_machine: MachineId,
        request: ReleaseWatcherPollRequest,
    ) -> Result<ReleaseWatcherPollOutcome, ReleaseCheckpointError> {
        match self.poll_release_watcher(authority_machine, request.watcher) {
            Ok(outcome) => Ok(outcome),
            Err(error) => release_watcher_poll_attention(error)
                .map(|reason| ReleaseWatcherPollOutcome::Attention { reason }),
        }
    }

    fn poll_release_watcher(
        &mut self,
        authority_machine: MachineId,
        watcher: ReleaseWatcherCommand,
    ) -> Result<ReleaseWatcherPollOutcome, ReleaseCheckpointError> {
        let resource_id = watcher.resource_id;
        let action_id = watcher.action_id;
        let revision = watcher.state_revision;
        let attention = |reason| Ok(ReleaseWatcherPollOutcome::Attention { reason });

        match release_completion_for_retry(
            &self.conn,
            authority_machine,
            resource_id,
            action_id,
            revision,
        ) {
            Ok(Some(_)) => return Ok(ReleaseWatcherPollOutcome::ReleaseSettled),
            Ok(None) => {}
            Err(CompleteReleaseError::Storage(error)) => return Err(error.into()),
            Err(_) => return attention(ReleaseWatcherPollAttention::ActionNotCurrent),
        }

        let Some(resource) = select_resource(&self.conn, resource_id)? else {
            return attention(ReleaseWatcherPollAttention::ActionNotCurrent);
        };
        if resource.authority_machine() != authority_machine {
            return attention(ReleaseWatcherPollAttention::WrongAuthority);
        }
        if resource.supervisor.machine != authority_machine {
            return attention(ReleaseWatcherPollAttention::RemoteSupervisorUnsupported);
        }

        let Some((state, _)) =
            release_checkpoint_state_for_action(&self.conn, resource_id, action_id)?
        else {
            return Err(missing_release_checkpoint_state_error(
                &self.conn,
                resource_id,
                action_id,
            )?);
        };
        let expected_action = ReleaseCheckpointAction {
            resource_id,
            action_id,
            state_revision: revision,
            observed_background_task: watcher.trainer_task_id,
        };
        if state.action != expected_action {
            return attention(ReleaseWatcherPollAttention::ActionNotCurrent);
        }
        let binding = release_checkpoint_binding_on(
            &self.conn,
            authority_machine,
            resource_id,
            &state.action,
        )?;
        if binding.watcher_intent.watcher_task_id != watcher.watcher_task_id {
            return attention(ReleaseWatcherPollAttention::WrongWatcher);
        }
        validate_release_checkpoint_baseline_on(
            &self.conn,
            authority_machine,
            resource_id,
            action_id,
        )?;
        if !release_watcher_is_running_on(
            &self.conn,
            authority_machine,
            resource_id,
            &binding.watcher_intent,
        )? {
            return Ok(ReleaseWatcherPollOutcome::WatcherNotRunning);
        }

        let decision = match self.reserve_release_checkpoint_stop_for_authority(
            authority_machine,
            resource_id,
            action_id,
            revision,
        )? {
            ReleaseCheckpointStopOutcome::Reserved(decision)
            | ReleaseCheckpointStopOutcome::AlreadyReserved(decision) => decision,
            ReleaseCheckpointStopOutcome::WaitingForTaskStart => {
                return Ok(ReleaseWatcherPollOutcome::WaitingForTrainerStart);
            }
            ReleaseCheckpointStopOutcome::WaitingForCheckpoint => {
                return Ok(ReleaseWatcherPollOutcome::WaitingForCheckpoint);
            }
            ReleaseCheckpointStopOutcome::CompletedResultAwaitingTaskExit => {
                return Ok(ReleaseWatcherPollOutcome::CompletedResultAwaitingTrainerExit);
            }
            ReleaseCheckpointStopOutcome::AlreadyCompleted => {
                return Ok(ReleaseWatcherPollOutcome::TrainerCompleted);
            }
            ReleaseCheckpointStopOutcome::Attention(watcher_attention) => {
                return attention(watcher_poll_attention(&watcher_attention));
            }
        };

        match self.commit_release_checkpoint_cancellation_for_authority(
            authority_machine,
            resource_id,
            action_id,
            revision,
            &decision,
        )? {
            ReleaseCheckpointCancellationOutcome::Committed(result)
            | ReleaseCheckpointCancellationOutcome::AlreadyCommitted(result) => {
                Ok(ReleaseWatcherPollOutcome::StopCommitted {
                    generation_id: result.decision.selected_checkpoint.generation_id,
                    cancel_requested_at: result.cancellation.cancel_requested_at,
                })
            }
            ReleaseCheckpointCancellationOutcome::WatcherNotReady { .. } => {
                Ok(ReleaseWatcherPollOutcome::WatcherNotRunning)
            }
        }
    }

    /// Complete a saved release action on this store connection.
    pub(crate) fn complete_release_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        action_id: ActionId,
        expected_state_revision: ResourceRevision,
    ) -> Result<ReleaseCompletionResult, CompleteReleaseError> {
        if let Some(receipt) = release_completion_for_retry(
            &self.conn,
            authority_machine,
            resource_id,
            action_id,
            expected_state_revision,
        )? {
            return Ok(receipt);
        }

        let proof = self.build_verified_release_proof(
            authority_machine,
            resource_id,
            action_id,
            expected_state_revision,
        )?;
        persist_release_completion_for_authority(&mut self.conn, proof)
    }

    fn build_verified_release_proof(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        action_id: ActionId,
        expected_state_revision: ResourceRevision,
    ) -> Result<VerifiedReleaseProof, CompleteReleaseError> {
        let preflight = {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Deferred)?;
            let resource_snapshot = load_resources_for_authority(&tx, authority_machine)?
                .into_iter()
                .find(|snapshot| snapshot.resource.id == resource_id)
                .ok_or(ResourceStoreError::ResourceNotFound)?;
            let resource = resource_snapshot.resource;
            let loan = resource_snapshot
                .loan
                .ok_or(CompleteReleaseError::ActionNotFound { action_id })?;

            let LoanState::Active {
                phase:
                    LoanPhase::AwaitingRelease {
                        action_id: saved_action_id,
                        observed_background_task,
                        ..
                    },
            } = loan.state
            else {
                return Err(CompleteReleaseError::NotAwaitingRelease {
                    loan_id: loan.id,
                    action_id,
                });
            };
            if saved_action_id != action_id {
                return Err(CompleteReleaseError::NotAwaitingRelease {
                    loan_id: loan.id,
                    action_id,
                });
            }
            if resource.state_revision != expected_state_revision {
                return Err(CompleteReleaseError::StaleRevision {
                    expected: expected_state_revision,
                    actual: resource.state_revision,
                });
            }
            if resource.registered_background_task != Some(observed_background_task) {
                return Err(CompleteReleaseError::TrainerAssociationMismatch {
                    task_id: observed_background_task,
                });
            }

            let association_json: Option<String> = tx
                .query_row(
                    "SELECT association_json FROM trainer_attempt_associations
                     WHERE resource_id = ?1 AND task_id = ?2",
                    params![
                        resource_id.as_uuid().to_string(),
                        observed_background_task.to_string(),
                    ],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(association_json) = association_json else {
                return Err(CompleteReleaseError::TrainerAssociationMissing {
                    task_id: observed_background_task,
                });
            };
            let association = trainer_association_by_resource_and_task(
                &tx,
                resource_id,
                observed_background_task,
            )?
            .ok_or(CompleteReleaseError::TrainerAssociationMissing {
                task_id: observed_background_task,
            })?;
            if association.authority_machine() != authority_machine
                || association.resource_id() != resource_id
                || association.task_id() != observed_background_task
            {
                return Err(CompleteReleaseError::TrainerAssociationMismatch {
                    task_id: observed_background_task,
                });
            }

            let task_row = super::task_by_id_on(&tx, observed_background_task)?.ok_or(
                CompleteReleaseError::BackgroundTaskMissing {
                    task_id: observed_background_task,
                },
            )?;
            let identity = super::identity::executor_identity_on(&tx, observed_background_task)?
                .ok_or(CompleteReleaseError::TrainerIdentityMissing {
                    task_id: observed_background_task,
                })?;
            let identity_json: Option<String> = tx
                .query_row(
                    "SELECT identity_json FROM executor_identities WHERE task_id = ?1",
                    [observed_background_task.to_string()],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(identity_json) = identity_json else {
                return Err(CompleteReleaseError::TrainerIdentityMissing {
                    task_id: observed_background_task,
                });
            };

            let ExecutorIdentity::Accepted(record) = identity else {
                return Err(CompleteReleaseError::TrainerIdentityChanged {
                    task_id: observed_background_task,
                });
            };
            if record.task != observed_background_task
                || record.execution_machine != authority_machine
                || !record.has_valid_spec_owners()
            {
                return Err(CompleteReleaseError::TrainerIdentityChanged {
                    task_id: observed_background_task,
                });
            }

            let terminal_evidence = match &task_row.state {
                TaskState::Finished {
                    reason: ExitReason::Exit { code: 0 },
                } => ReleaseProofTerminalEvidence::Completed,
                TaskState::Finished {
                    reason: ExitReason::Cancelled,
                } => {
                    let Some((checkpoint_state, _)) =
                        release_checkpoint_state_for_action(&tx, resource_id, action_id)?
                    else {
                        return Err(CompleteReleaseError::StoppedProofUnavailable {
                            task_id: observed_background_task,
                        });
                    };
                    if checkpoint_state.action
                        != (ReleaseCheckpointAction {
                            resource_id,
                            action_id,
                            state_revision: expected_state_revision,
                            observed_background_task,
                        })
                    {
                        return Err(CompleteReleaseError::StoppedProofUnavailable {
                            task_id: observed_background_task,
                        });
                    }
                    let ReleaseCheckpointPhase::CancellationCommitted {
                        baseline,
                        decision,
                        cancellation,
                    } = checkpoint_state.phase
                    else {
                        return Err(CompleteReleaseError::StoppedProofUnavailable {
                            task_id: observed_background_task,
                        });
                    };
                    if baseline.binding.association
                        != TrainerAttemptAssociationProof::from(&association)
                    {
                        return Err(CompleteReleaseError::TrainerAssociationMismatch {
                            task_id: observed_background_task,
                        });
                    }
                    if decision.binding != baseline.binding
                        || cancellation.task_id != observed_background_task
                    {
                        return Err(CompleteReleaseError::StoppedProofUnavailable {
                            task_id: observed_background_task,
                        });
                    }
                    if task_row.cancel_requested_at != Some(cancellation.cancel_requested_at) {
                        return Err(CompleteReleaseError::TrainerCancellationMarkerChanged {
                            task_id: observed_background_task,
                        });
                    }

                    ReleaseProofTerminalEvidence::Stopped {
                        decision,
                        cancellation,
                    }
                }
                TaskState::Finished { .. } => {
                    return Err(CompleteReleaseError::StoppedProofUnavailable {
                        task_id: observed_background_task,
                    });
                }
                TaskState::Lost => {
                    return Err(CompleteReleaseError::BackgroundTaskLost {
                        task_id: observed_background_task,
                    });
                }
                TaskState::Queued | TaskState::Running { .. } => {
                    return Err(CompleteReleaseError::BackgroundTaskNotTerminal {
                        task_id: observed_background_task,
                        state: task_row.status().to_string(),
                    });
                }
            };
            let expected_identity_state = match &terminal_evidence {
                ReleaseProofTerminalEvidence::Completed => ProcessStatus::Succeeded,
                ReleaseProofTerminalEvidence::Stopped { .. } => ProcessStatus::Cancelled,
            };
            if record.state != expected_identity_state {
                return Err(CompleteReleaseError::TrainerIdentityChanged {
                    task_id: observed_background_task,
                });
            }
            if task_row.process_group_exit_evidence() != ProcessGroupExitEvidence::ConfirmedExited {
                return Err(CompleteReleaseError::WorkerExitUnconfirmed {
                    task_id: observed_background_task,
                });
            }

            let normalized_spec = record
                .current_spec()
                .ok_or(CompleteReleaseError::TrainerAssociationMismatch {
                    task_id: observed_background_task,
                })?
                .clone();
            if normalized_spec_sha256(&normalized_spec).map_err(AppError::from)?
                != association.normalized_spec_sha256()
            {
                return Err(CompleteReleaseError::TrainerAssociationMismatch {
                    task_id: observed_background_task,
                });
            }

            tx.commit()?;
            ReleaseProofPreflight {
                task_id: observed_background_task,
                association,
                association_json,
                identity_json,
                task_row,
                normalized_spec,
                terminal_evidence,
            }
        };

        let _command_shape = crate::resource::command_shape::DirectSegmentCommandShape::validate(
            &preflight.normalized_spec,
            &preflight.task_row,
            &preflight.association,
        )?;
        let verified_attempt = preflight.association.verified_attempt();
        let release_evidence = match preflight.terminal_evidence {
            ReleaseProofTerminalEvidence::Completed => {
                let completed_result = find_completed_result(
                    verified_attempt.canonical_runtime_root(),
                    verified_attempt.binding(),
                )?
                .ok_or(CompleteReleaseError::CompletedResultMissing {
                    task_id: preflight.task_id,
                })?;
                if completed_result.binding != *verified_attempt.binding()
                    || completed_result.request_sha256 != verified_attempt.request_digest().to_hex()
                {
                    return Err(CompleteReleaseError::CompletedResultRequestMismatch {
                        task_id: preflight.task_id,
                    });
                }

                VerifiedReleaseEvidence::Completed(Box::new(completed_result))
            }
            ReleaseProofTerminalEvidence::Stopped {
                decision,
                cancellation,
            } => {
                let checkpoint = &decision.selected_checkpoint;
                if checkpoint.binding != *verified_attempt.binding() {
                    return Err(CompleteReleaseError::StoppedCheckpointChanged {
                        task_id: preflight.task_id,
                    });
                }
                match revalidate_checkpoint_publication(
                    verified_attempt.canonical_runtime_root(),
                    verified_attempt.binding(),
                    checkpoint,
                ) {
                    Ok(true) => {}
                    Ok(false)
                    | Err(crate::resource::watcher::WatcherError::MalformedPublication {
                        ..
                    })
                    | Err(crate::resource::watcher::WatcherError::Symlink { .. }) => {
                        return Err(CompleteReleaseError::StoppedCheckpointChanged {
                            task_id: preflight.task_id,
                        });
                    }
                    Err(error) => return Err(CompleteReleaseError::Watcher(error)),
                }

                VerifiedReleaseEvidence::Stopped {
                    decision,
                    cancellation,
                }
            }
        };

        let ownership_guard = match probe_segment_ownership_lock(
            verified_attempt.canonical_runtime_root(),
            verified_attempt.ownership_lock_identity(),
        ) {
            OwnershipLockProbe::OwnershipHeld => {
                return Err(CompleteReleaseError::OwnershipLockStillHeld {
                    task_id: preflight.task_id,
                });
            }
            OwnershipLockProbe::ExactOwnershipReleased(guard) => guard,
            OwnershipLockProbe::Attention(source) => {
                return Err(CompleteReleaseError::OwnershipLock(source));
            }
        };

        Ok(VerifiedReleaseProof::new(ReleaseProofEvidence {
            authority_machine,
            resource_id,
            action_id,
            expected_state_revision,
            task_id: preflight.task_id,
            association: preflight.association,
            association_json: preflight.association_json,
            identity_json: preflight.identity_json,
            task_row: preflight.task_row,
            release_evidence,
            ownership_guard,
        }))
    }

    /// Read one durable supervisor notice by its identity.
    pub(crate) fn supervisor_notice(
        &self,
        notice_id: NoticeId,
    ) -> Result<Option<SupervisorNotice>, SupervisorNoticeStoreError> {
        load_supervisor_notice(&self.conn, notice_id)
    }

    /// List notices that can receive another delivery attempt.
    pub(crate) fn pending_supervisor_notices(
        &self,
    ) -> Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError> {
        load_pending_supervisor_notices(&self.conn)
    }

    /// Reserve one bounded delivery attempt on this store connection.
    pub(crate) fn reserve_supervisor_notice_attempt(
        &mut self,
        notice_id: NoticeId,
        attempt_id: DeliveryAttemptId,
    ) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
        reserve_notice_attempt(&mut self.conn, notice_id, attempt_id)
    }

    /// Settle only the exact in-flight delivery attempt.
    pub(crate) fn settle_supervisor_notice_attempt(
        &mut self,
        notice_id: NoticeId,
        attempt_id: DeliveryAttemptId,
        result: Result<(), String>,
    ) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
        settle_notice_attempt(&mut self.conn, notice_id, attempt_id, result)
    }

    /// Recover in-flight notices after a daemon restart.
    pub(crate) fn recover_sending_supervisor_notices(
        &mut self,
    ) -> Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError> {
        recover_in_flight_supervisor_notices(&mut self.conn)
    }

    /// Retarget an undelivered notice with an assignment-revision compare-and-set.
    pub(crate) fn retarget_supervisor_notice(
        &mut self,
        notice_id: NoticeId,
        expected_assignment_revision: AssignmentRevision,
        destination: SupervisorAddress,
        new_assignment_revision: AssignmentRevision,
    ) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
        retarget_notice(
            &mut self.conn,
            notice_id,
            expected_assignment_revision,
            destination,
            new_assignment_revision,
        )
    }
}

#[cfg(test)]
impl Store {
    pub(crate) fn seed_verified_serving_loan_for_test(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        request_id: RequestId,
        return_context: ReturnContext,
    ) -> Result<(Loan, ResourceRevision), CompleteReleaseError> {
        let (mut loan, state_revision) = self.seed_serving_loan_for_test(
            authority_machine,
            resource_id,
            request_id,
            return_context,
        )?;
        let expected_state_revision = ResourceRevision::new(
            state_revision
                .get()
                .checked_sub(1)
                .ok_or(CompleteReleaseError::ResourceChanged)?,
        );
        loan = crate::resource::store::seed_verified_serving_provenance_for_test(
            &mut self.conn,
            loan,
            authority_machine,
            resource_id,
            request_id,
            expected_state_revision,
            state_revision,
        )?;
        Ok((loan, state_revision))
    }

    pub(crate) fn seed_serving_loan_for_test(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        request_id: RequestId,
        return_context: ReturnContext,
    ) -> Result<(Loan, ResourceRevision), CompleteReleaseError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let resource =
            select_resource(&tx, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
        check_resource_authority(&tx, resource_id, Some(authority_machine))?;

        let acceptance_sequence: Option<i64> = tx
            .query_row(
                "SELECT acceptance_sequence FROM resource_requests
                 WHERE request_id = ?1 AND resource_id = ?2
                   AND json_extract(state_json, '$.type') = 'queued'",
                params![request_id.0.to_string(), resource_id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(acceptance_sequence) = acceptance_sequence else {
            return Err(ResourceStoreError::Conflict.into());
        };

        let next_revision_value = resource.state_revision.get().checked_add(1).ok_or(
            CompleteReleaseError::RevisionExhausted {
                revision: resource.state_revision,
            },
        )?;
        let next_revision = ResourceRevision::new(next_revision_value);
        let current_loan = select_non_closed_loan(&tx, resource_id)?;
        let loan_id = current_loan
            .as_ref()
            .map_or_else(LoanId::new, |loan| loan.id);
        if current_loan.as_ref().is_some_and(|loan| {
            !matches!(
                loan.state,
                LoanState::Active {
                    phase: LoanPhase::AwaitingRelease { .. }
                }
            )
        }) {
            return Err(ResourceStoreError::Conflict.into());
        }
        let state_json = serde_json::to_string(&ResourceRequestState::Assigned { loan_id })
            .map_err(AppError::from)?;
        let changed = tx.execute(
            "UPDATE resource_requests SET state_json = ?1
             WHERE request_id = ?2 AND resource_id = ?3 AND acceptance_sequence = ?4
               AND json_extract(state_json, '$.type') = 'queued'",
            params![
                state_json,
                request_id.0.to_string(),
                resource_id.as_uuid().to_string(),
                acceptance_sequence,
            ],
        )?;
        if changed != 1 {
            return Err(CompleteReleaseError::RequestChanged { request_id });
        }

        let loan = Loan {
            id: loan_id,
            resource_id,
            state: LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context,
                    current_request_id: request_id,
                    release_provenance: ServingReleaseProvenance::Unverified,
                },
            },
        };
        let loan_state_json = serde_json::to_string(&loan.state).map_err(AppError::from)?;
        if current_loan.is_some() {
            let changed = tx.execute(
                "UPDATE loans SET state_json = ?1 WHERE id = ?2 AND resource_id = ?3",
                params![
                    loan_state_json,
                    loan.id.as_uuid().to_string(),
                    resource_id.as_uuid().to_string(),
                ],
            )?;
            if changed != 1 {
                return Err(CompleteReleaseError::LoanChanged { loan_id: loan.id });
            }
        } else {
            tx.execute(
                "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
                params![
                    loan.id.as_uuid().to_string(),
                    resource_id.as_uuid().to_string(),
                    loan_state_json,
                ],
            )?;
        }
        let changed = tx.execute(
            "UPDATE resources SET state_revision = ?1
             WHERE id = ?2 AND authority_machine = ?3 AND state_revision = ?4",
            params![
                i64::try_from(next_revision_value).map_err(|_| {
                    CompleteReleaseError::RevisionExhausted {
                        revision: resource.state_revision,
                    }
                })?,
                resource_id.as_uuid().to_string(),
                authority_machine.as_uuid().to_string(),
                i64::try_from(resource.state_revision.get()).map_err(|_| {
                    CompleteReleaseError::RevisionExhausted {
                        revision: resource.state_revision,
                    }
                })?,
            ],
        )?;
        if changed != 1 {
            return Err(CompleteReleaseError::ResourceChanged);
        }
        tx.commit()?;

        Ok((loan, next_revision))
    }
}

fn resource_request_identity_exists(
    conn: &rusqlite::Connection,
    request: RequestId,
    task: TaskId,
) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM resource_requests WHERE request_id=?1 OR task_id=?2
            UNION ALL
            SELECT 1 FROM resource_request_preventions WHERE request_id=?1 OR task_id=?2
        )",
        params![request.0.to_string(), task.to_string()],
        |row| row.get(0),
    )
}

fn task_has_any_event(conn: &rusqlite::Connection, task: TaskId) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM executor_outbox WHERE task_id=?1
            UNION ALL SELECT 1 FROM executor_event_receipts WHERE task_id=?1
            UNION ALL SELECT 1 FROM executor_event_cursors WHERE task_id=?1
            UNION ALL SELECT 1 FROM executor_event_routes WHERE task_id=?1
        )",
        [task.to_string()],
        |row| row.get(0),
    )
}

fn same_resource_spec(
    left: &NormalizedSpec,
    right: &NormalizedSpec,
) -> Result<bool, ResourceStoreError> {
    let left = serde_json::to_value(left).map_err(crate::error::AppError::from)?;
    let right = serde_json::to_value(right).map_err(crate::error::AppError::from)?;
    Ok(left == right)
}

fn resource_task_row_matches(row: &TaskRow, task: TaskId, spec: &NormalizedSpec) -> bool {
    row.id == task
        && row.name.as_ref() == Some(&spec.name)
        && row.thread == spec.thread
        && row.workload == crate::invocation::persist_workload(&spec.workload)
        && row.cwd == spec.cwd
        && row.timeout == spec.timeout
        && row.binary.is_absolute()
}

fn same_trainer_task_binding(left: &TaskRow, right: &TaskRow) -> bool {
    left.id == right.id
        && left.name == right.name
        && left.thread == right.thread
        && left.workload == right.workload
        && left.cwd == right.cwd
        && left.timeout == right.timeout
        && left.env == right.env
        && left.binary == right.binary
}

fn validate_resource_task_route(
    conn: &rusqlite::Connection,
    request: &ResourceRequest,
    authority: MachineId,
    spec: &NormalizedSpec,
    already_accepted: bool,
) -> Result<(), ResourceStoreError> {
    let by_request = super::identity::origin_route_by_request_on(conn, request.request_id)?;
    let by_task = super::identity::origin_route_by_task_on(conn, request.task_id)?;

    if request.origin_machine != authority {
        return if by_request.is_none() && by_task.is_none() {
            Ok(())
        } else {
            Err(ResourceStoreError::Conflict)
        };
    }

    let (by_request, by_task) = match (by_request, by_task) {
        (Some(by_request), Some(by_task)) => (by_request, by_task),
        (None, None) => {
            return Err(ResourceStoreError::OriginRouteNotFound {
                task: request.task_id,
            });
        }
        _ => return Err(ResourceStoreError::Conflict),
    };
    if serde_json::to_value(&by_request).map_err(crate::error::AppError::from)?
        != serde_json::to_value(&by_task).map_err(crate::error::AppError::from)?
    {
        return Err(ResourceStoreError::Conflict);
    }

    let SubmissionState::Resource { resource, phase } = &by_request.submission else {
        return Err(ResourceStoreError::Conflict);
    };
    if by_request.request != request.request_id
        || by_request.task != request.task_id
        || by_request.origin_machine != request.origin_machine
        || by_request.execution_machine != authority
        || by_request.thread != spec.thread
        || *resource != request.resource_id
        || !same_resource_spec(
            by_request
                .current_spec()
                .ok_or(ResourceStoreError::Conflict)?,
            spec,
        )?
    {
        return Err(ResourceStoreError::Conflict);
    }

    match phase {
        ResourceRoutePhase::AcceptanceUnknown | ResourceRoutePhase::Waiting => Ok(()),
        ResourceRoutePhase::Activated if already_accepted => Ok(()),
        ResourceRoutePhase::CancelledBeforeLaunch if !already_accepted => {
            Err(ResourceStoreError::Prevented)
        }
        ResourceRoutePhase::Activated
        | ResourceRoutePhase::CancelledBeforeLaunch
        | ResourceRoutePhase::Rejected { .. } => Err(ResourceStoreError::Conflict),
    }
}

fn watcher_task_matches(saved: &TaskRow, expected: &TaskRow) -> bool {
    saved.id == expected.id
        && saved.thread == expected.thread
        && saved.name == expected.name
        && saved.workload == expected.workload
        && saved.cwd == expected.cwd
        && saved.timeout == expected.timeout
        && saved.env == expected.env
        && saved.binary == expected.binary
}

fn watcher_routes_match(
    by_request: &OriginRoute,
    by_task: &OriginRoute,
    route_identity: (RequestId, TaskId),
    authority: MachineId,
    supervisor: SupervisorAddress,
    callback: &CallbackContext,
    spec: &NormalizedSpec,
) -> Result<bool, AppError> {
    let (request, task) = route_identity;
    let expected_spec = serde_json::to_value(spec)?;
    let matches = |route: &OriginRoute| -> Result<bool, AppError> {
        let same_spec = route
            .spec
            .current()
            .map(serde_json::to_value)
            .transpose()?
            .is_some_and(|saved| saved == expected_spec);
        Ok(route.request == request
            && route.task == task
            && route.origin_machine == authority
            && route.execution_machine == authority
            && route.thread == supervisor.thread
            && route.callback == *callback
            && matches!(&route.submission, SubmissionState::Accepted)
            && same_spec)
    };
    Ok(matches(by_request)? && matches(by_task)?)
}

fn watcher_identity_matches(
    identity: &ExecutorIdentity,
    task: TaskId,
    authority: MachineId,
    spec: &NormalizedSpec,
    state: ProcessStatus,
) -> Result<bool, AppError> {
    let ExecutorIdentity::Accepted(record) = identity else {
        return Ok(false);
    };
    let expected_spec = serde_json::to_value(spec)?;
    let same_spec = record
        .current_spec()
        .map(serde_json::to_value)
        .transpose()?
        .is_some_and(|saved| saved == expected_spec);
    Ok(record.task == task
        && record.origin_machine == authority
        && record.execution_machine == authority
        && record.has_valid_spec_owners()
        && record.state == state
        && same_spec)
}

fn saved_initial_watcher_event(
    conn: &rusqlite::Connection,
    task: TaskId,
    authority: MachineId,
) -> Result<bool, AppError> {
    let event: Option<String> = conn
        .query_row(
            "SELECT event_json FROM executor_outbox WHERE task_id=?1 AND seq=1",
            [task.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(event) = event {
        let event: TaskEvent = serde_json::from_str(&event)?;
        return Ok(event.task == task
            && event.seq.get() == 1
            && event.origin_machine == authority
            && event.execution_machine == authority
            && event.payload
                == (EventPayload::State {
                    status: ProcessStatus::Queued,
                }));
    }

    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM executor_event_receipts WHERE task_id=?1 AND seq=1
        )",
        [task.to_string()],
        |row| row.get(0),
    )
    .map_err(AppError::from)
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File, OpenOptions};
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Barrier};

    use ractor::Actor;
    use serde_json::json;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::cancellation::ResourceCancellationRequestIdentity;
    use crate::daemon::actors::resource::{ResourceActor, ResourceMsg};
    use crate::daemon::actors::supervisor::SUPERVISOR_TEST_LOCK;
    use crate::daemon::actors::{StoreActor, StoreMsg, SupervisorActor, SupervisorMsg, call};
    use crate::domain::{
        ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskState, TaskWorkload,
        ThreadId, Workload,
    };
    use crate::home::{Home, LockMode, flock_exclusive};
    use crate::machine::load_or_create_machine_id;
    use crate::resource::ownership_lock::{
        OwnershipLockIdentity, TrainerRequestDigest, VerifiedTrainerAttempt,
        build_trainer_attempt_registration_evidence, test_support as trainer_attempt_test_support,
    };
    use crate::resource::store::TrainerAttemptAssociationStoreError;
    use crate::resource::store::{
        AssignedResourceTaskAttention, OpenReleaseLoanResult, ReleaseCompletionResult,
        ResourceTaskCompletionResult,
    };
    use crate::resource::watcher::AttemptBinding;
    use crate::resource::{
        ActionId, AssignmentRevision, DeliveryAttemptId, Loan, LoanId, LoanPhase, LoanState,
        ReleaseProofAttentionReason, ReleaseWatcherIntent, ReleaseWatcherTaskId,
        ResourceQueueAttentionReason, ResourceQueueReconcileOutcome, ResourceRequestState,
        ResourceRevision, ReturnContext, ServingReleaseProvenance, SupervisorAddress,
        SupervisorNoticeDelivery, TrainerAttemptAssociation,
    };
    use crate::spec::NormalizedWorkload;
    use crate::store::{ExecutorIdentity, IdentityError, NewTask, new_queued_task};
    use crate::submission::{
        CallbackContext, CallbackExecutable, ExecutionRecord, NewResourceRoute, OriginRoute,
        PreAcceptanceRejection, RejectionTombstone, ResourceQueueOutcome, ResourceQueueReceipt,
        ResourceRoutePhase, SubmissionState,
    };

    fn resource(authority: MachineId) -> Resource {
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

    fn machine_other_than(machine: MachineId) -> MachineId {
        let first = MachineId::from_uuid(Uuid::from_u128(1));
        if first != machine {
            return first;
        }

        MachineId::from_uuid(Uuid::from_u128(2))
    }

    fn acquire_test_lock(path: &Path, create: bool) -> File {
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        if create {
            options.create_new(true);
        }
        let file = options.open(path).unwrap();
        #[allow(deprecated)]
        nix::fcntl::flock(
            file.as_raw_fd(),
            nix::fcntl::FlockArg::LockExclusiveNonblock,
        )
        .unwrap();
        file
    }

    fn spec() -> NormalizedSpec {
        serde_json::from_value(json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "resource command",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["/bin/echo", "hello"] }
        }))
        .unwrap()
    }

    fn resource_cancellation_identity(
        cancellation: uuid::Uuid,
        request: RequestId,
        task: TaskId,
        resource: ResourceId,
        origin: MachineId,
        authority: MachineId,
        target_phase: ResourceRoutePhase,
    ) -> ResourceCancellationRequestIdentity {
        ResourceCancellationRequestIdentity {
            requester_machine: origin,
            cancellation,
            request,
            task,
            origin_machine: origin,
            authority_machine: authority,
            resource,
            target_phase,
        }
    }

    fn resource_cancellation_proof(
        identity: &ResourceCancellationRequestIdentity,
        normalized_spec: &NormalizedSpec,
        phase: ResourceRoutePhase,
    ) -> crate::submission::ResourceRouteProof {
        crate::submission::ResourceRouteProof {
            request: identity.request,
            task: identity.task,
            origin_machine: identity.origin_machine,
            authority_machine: identity.authority_machine,
            resource: identity.resource,
            thread: normalized_spec.thread,
            normalized_spec_sha256: crate::submission::normalized_spec_sha256(normalized_spec)
                .unwrap(),
            phase,
        }
    }

    fn remote_task(task: TaskId, spec: &NormalizedSpec) -> crate::domain::TaskRow {
        let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
            panic!("resource spec must be a command");
        };
        new_queued_task(NewTask {
            id: task,
            name: Some(spec.name.clone()),
            thread: spec.thread,
            workload: Workload::Task(TaskWorkload {
                command: workload.command,
            }),
            cwd: spec.cwd.clone(),
            timeout: spec.timeout,
            env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
            binary: PathBuf::from("/bin/echo"),
        })
    }

    fn direct_segment_spec(
        trainer_root: &Path,
        task_file: &Path,
        input_root: &Path,
        runtime_root: &Path,
    ) -> NormalizedSpec {
        serde_json::from_value(json!({
            "api_version": 1,
            "thread": Uuid::now_v7(),
            "name": "direct segment trainer",
            "cwd": trainer_root,
            "timeout": "4h",
            "workload": {
                "type": "task",
                "command": [
                    "python3",
                    "-m",
                    "ops.run_segment",
                    "run",
                    "--task",
                    task_file,
                    "--input-root",
                    input_root,
                    "--runtime-root",
                    runtime_root,
                    "--image-digest",
                    "test-image-digest"
                ]
            }
        }))
        .unwrap()
    }

    fn trainer_task(
        task: TaskId,
        spec: &NormalizedSpec,
        python: &Path,
        bin: &Path,
        home: &Path,
    ) -> crate::domain::TaskRow {
        let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
            panic!("trainer spec must be a command");
        };
        let requested_program = workload.command.program();
        let binary = if requested_program == "python3" {
            python.to_path_buf()
        } else {
            PathBuf::from(requested_program)
        };
        new_queued_task(NewTask {
            id: task,
            name: Some(spec.name.clone()),
            thread: spec.thread,
            workload: Workload::Task(TaskWorkload {
                command: workload.command,
            }),
            cwd: spec.cwd.clone(),
            timeout: spec.timeout,
            env: TaskEnv {
                path: bin.to_string_lossy().into_owned(),
                home: home.to_string_lossy().into_owned(),
            },
            binary,
        })
    }

    struct TrainerAssociationFixture {
        _directory: tempfile::TempDir,
        database: PathBuf,
        store: Store,
        authority: MachineId,
        resource: Resource,
        task_id: TaskId,
        spec: NormalizedSpec,
        trainer_root: PathBuf,
        task_file: PathBuf,
        input_root: PathBuf,
        runtime_root: PathBuf,
        bin: PathBuf,
        python: PathBuf,
        home: PathBuf,
    }

    impl TrainerAssociationFixture {
        fn new() -> Self {
            let directory = tempdir().unwrap();
            let authority = MachineId::new();
            let database = directory.path().join("db");
            Self::new_with_database(directory, database, authority)
        }

        fn new_for_home(directory: tempfile::TempDir, home: &Home, authority: MachineId) -> Self {
            Self::new_with_database(directory, home.db_path(), authority)
        }

        fn new_with_database(
            directory: tempfile::TempDir,
            database: PathBuf,
            authority: MachineId,
        ) -> Self {
            let mut store = Store::open(&database).unwrap();
            let task_id = TaskId::new();
            let home = directory.path().canonicalize().unwrap();
            let trainer_root = home.join("trainer");
            let ops = trainer_root.join("ops");
            fs::create_dir_all(&ops).unwrap();
            fs::write(ops.join("run_segment.py"), b"# maintained trainer\n").unwrap();
            fs::write(
                ops.join("segment_artifacts.py"),
                b"# maintained artifacts\n",
            )
            .unwrap();

            let task_file = home.join("task.json");
            fs::write(&task_file, b"{}\n").unwrap();
            let input_root = home.join("inputs");
            fs::create_dir(&input_root).unwrap();
            let runtime_root = home.join("runtime");
            fs::create_dir(&runtime_root).unwrap();

            let bin = home.join("bin");
            fs::create_dir(&bin).unwrap();
            let python = bin.join("python3");
            fs::write(&python, b"#!/bin/sh\nexit 0\n").unwrap();
            fs::set_permissions(&python, fs::Permissions::from_mode(0o755)).unwrap();

            let spec = direct_segment_spec(&trainer_root, &task_file, &input_root, &runtime_root);
            let mut resource = resource(authority);
            resource.supervisor.thread = spec.thread;
            resource.registered_background_task = Some(task_id);
            store.register_resource(authority, &resource).unwrap();

            Self {
                _directory: directory,
                database,
                store,
                authority,
                resource,
                task_id,
                spec,
                trainer_root,
                task_file,
                input_root,
                runtime_root,
                bin,
                python,
                home,
            }
        }

        fn insert_accepted_running_task(&mut self) {
            let row = self.trainer_task(self.task_id, &self.spec);
            self.store
                .insert_local_task(&row, &self.spec, self.authority, PathBuf::from("/bin/echo"))
                .unwrap();
            self.store
                .cas_status(self.task_id, ProcessStatus::Queued, ProcessStatus::Running)
                .unwrap()
                .unwrap();
        }

        fn evidence(&self) -> VerifiedTrainerAttempt {
            VerifiedTrainerAttempt::from_persisted(
                self.runtime_root.clone(),
                trainer_attempt_test_support::attempt_binding("attempt-1"),
                TrainerRequestDigest::from_hex(&"42".repeat(32)).unwrap(),
                OwnershipLockIdentity::new(7, 11),
            )
            .unwrap()
        }

        fn register_release_attempt(&mut self) -> crate::resource::watcher::AttemptBinding {
            let binding = trainer_attempt_test_support::attempt_binding("attempt-1");
            crate::resource::watcher::tests::write_request_for_test(&self.runtime_root, &binding);
            let lock_path = self.runtime_root.join(".segment.lock");
            let lock_file = acquire_test_lock(&lock_path, true);
            let evidence =
                build_trainer_attempt_registration_evidence(&self.runtime_root, &binding).unwrap();
            self.bind(evidence).unwrap();
            drop(lock_file);
            binding
        }

        fn hold_saved_lock(&self) -> File {
            acquire_test_lock(&self.runtime_root.join(".segment.lock"), false)
        }

        fn trainer_task(&self, task: TaskId, spec: &NormalizedSpec) -> crate::domain::TaskRow {
            trainer_task(task, spec, &self.python, &self.bin, &self.home)
        }

        fn command(&self) -> Vec<String> {
            let NormalizedWorkload::Task(workload) = &self.spec.workload else {
                panic!("trainer spec must be a command");
            };
            workload.command.to_vec()
        }

        fn set_command(&mut self, command: Vec<String>) {
            let NormalizedWorkload::Task(workload) = &mut self.spec.workload else {
                panic!("trainer spec must be a command");
            };
            workload.command = crate::invocation::CommandLine::try_from_argv(command).unwrap();
        }

        fn remove_shape_files(&self) {
            fs::remove_file(&self.task_file).unwrap();
            fs::remove_dir(&self.input_root).unwrap();
            fs::remove_dir(&self.runtime_root).unwrap();
            fs::remove_dir_all(&self.trainer_root).unwrap();
            fs::remove_dir_all(&self.bin).unwrap();
        }

        fn finish_registered_task(&mut self) {
            self.store
                .cas_exit(
                    self.task_id,
                    ProcessStatus::Running,
                    &ExitReason::Exit { code: 0 },
                )
                .unwrap()
                .unwrap();
            self.store
                .update_execution_state(self.task_id, ProcessStatus::Succeeded)
                .unwrap();
        }

        fn finish_registered_task_with_evidence(&mut self, evidence: ProcessGroupExitEvidence) {
            self.store
                .cas_exit_with_evidence(
                    self.task_id,
                    ProcessStatus::Running,
                    &ExitReason::Exit { code: 0 },
                    evidence,
                )
                .unwrap()
                .unwrap();
            self.store
                .update_execution_state(self.task_id, ProcessStatus::Succeeded)
                .unwrap();
        }

        fn finish_registered_task_cancelled_with_evidence(
            &mut self,
            evidence: ProcessGroupExitEvidence,
        ) {
            self.store
                .cas_exit_with_evidence(
                    self.task_id,
                    ProcessStatus::Running,
                    &ExitReason::Cancelled,
                    evidence,
                )
                .unwrap()
                .unwrap();
            self.store
                .update_execution_state(self.task_id, ProcessStatus::Cancelled)
                .unwrap();
        }

        fn bind(
            &mut self,
            evidence: VerifiedTrainerAttempt,
        ) -> Result<TrainerAttemptAssociation, TrainerAttemptAssociationStoreError> {
            self.store.bind_trainer_attempt_association(
                self.authority,
                self.resource.id,
                self.task_id,
                evidence,
            )
        }

        fn add_loan(&self, state: LoanState) {
            self.store
                .conn
                .execute(
                    "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
                    params![
                        LoanId::new().as_uuid().to_string(),
                        self.resource.id.as_uuid().to_string(),
                        serde_json::to_string(&state).unwrap(),
                    ],
                )
                .unwrap();
        }
    }

    fn saved_trainer_association_json(store: &Store, task_id: TaskId) -> String {
        store
            .conn
            .query_row(
                "SELECT association_json FROM trainer_attempt_associations WHERE task_id=?1",
                [task_id.to_string()],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn awaiting_release_state(task_id: TaskId) -> LoanState {
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id: ActionId::new(),
                observed_background_task: task_id,
                watcher_intent: None,
            },
        }
    }

    #[test]
    fn trainer_attempt_association_binds_valid_direct_segment_shape_and_reads_it() {
        let mut fixture = TrainerAssociationFixture::new();
        fixture.insert_accepted_running_task();
        let evidence = fixture.evidence();

        let association = fixture.bind(evidence.clone()).unwrap();

        assert_eq!(association.resource_id(), fixture.resource.id);
        assert_eq!(association.authority_machine(), fixture.authority);
        assert_eq!(association.task_id(), fixture.task_id);
        assert_eq!(association.verified_attempt(), &evidence);
        assert_eq!(
            association.normalized_spec_sha256(),
            normalized_spec_sha256(&fixture.spec).unwrap()
        );
        assert_eq!(
            fixture
                .store
                .trainer_attempt_association_for_authority(fixture.authority, fixture.resource.id)
                .unwrap(),
            Some(association.clone())
        );
        assert_eq!(
            fixture
                .store
                .trainer_attempt_association_for_task_for_authority(
                    fixture.authority,
                    fixture.task_id
                )
                .unwrap(),
            Some(association)
        );
        let loan_count: i64 = fixture
            .store
            .conn
            .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
            .unwrap();
        assert_eq!(loan_count, 0);
    }

    #[test]
    fn trainer_attempt_association_rejects_shell_and_wrong_module_without_a_row() {
        let mut shell = TrainerAssociationFixture::new();
        let evidence = shell.evidence();
        shell.set_command(vec![
            "/bin/sh".into(),
            "-c".into(),
            "echo not a trainer".into(),
        ]);
        shell.insert_accepted_running_task();
        assert!(matches!(
            shell.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::DirectSegmentCommandShape(
                crate::resource::command_shape::DirectSegmentCommandShapeError::NotPythonExecutable {
                    ..
                }
            ))
        ));
        let shell_count: i64 = shell
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM trainer_attempt_associations",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(shell_count, 0);

        let mut wrong_module = TrainerAssociationFixture::new();
        let evidence = wrong_module.evidence();
        let mut command = wrong_module.command();
        command[2] = "ops.not_run_segment".into();
        wrong_module.set_command(command);
        wrong_module.insert_accepted_running_task();
        assert!(matches!(
            wrong_module.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::DirectSegmentCommandShape(
                crate::resource::command_shape::DirectSegmentCommandShapeError::
                    InvalidModuleInvocation { .. }
            ))
        ));
        let wrong_module_count: i64 = wrong_module
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM trainer_attempt_associations",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(wrong_module_count, 0);
    }

    #[test]
    fn trainer_attempt_association_rejects_runtime_root_mismatch_without_a_row() {
        let mut fixture = TrainerAssociationFixture::new();
        let evidence = fixture.evidence();
        let mut command = fixture.command();
        let runtime_root_index = command
            .iter()
            .position(|argument| argument == "--runtime-root")
            .unwrap();
        command[runtime_root_index + 1] = fixture.home.join("other-runtime").display().to_string();
        fixture.set_command(command);
        fixture.insert_accepted_running_task();

        assert!(matches!(
            fixture.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::DirectSegmentCommandShape(
                crate::resource::command_shape::DirectSegmentCommandShapeError::RuntimeRootMismatch {
                    ..
                }
            ))
        ));
        let association_count: i64 = fixture
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM trainer_attempt_associations",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(association_count, 0);
    }

    #[test]
    fn trainer_attempt_association_exact_retry_after_terminal_and_file_removal() {
        let mut fixture = TrainerAssociationFixture::new();
        fixture.insert_accepted_running_task();
        let evidence = fixture.evidence();
        let first = fixture.bind(evidence.clone()).unwrap();
        let before = saved_trainer_association_json(&fixture.store, fixture.task_id);
        fixture.finish_registered_task();
        fixture.remove_shape_files();
        let TrainerAssociationFixture {
            _directory,
            database,
            store,
            authority,
            resource,
            task_id,
            ..
        } = fixture;
        drop(store);

        let mut reopened = Store::open(&database).unwrap();
        let retry = reopened
            .bind_trainer_attempt_association(authority, resource.id, task_id, evidence)
            .unwrap();

        assert_eq!(retry, first);
        assert_eq!(saved_trainer_association_json(&reopened, task_id), before);
        let association_count: i64 = reopened
            .conn
            .query_row(
                "SELECT COUNT(*) FROM trainer_attempt_associations",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(association_count, 1);
    }

    #[test]
    fn trainer_attempt_association_rejects_changed_attempt_lock_and_request_digest() {
        let mut fixture = TrainerAssociationFixture::new();
        fixture.insert_accepted_running_task();
        let evidence = fixture.evidence();
        fixture.bind(evidence.clone()).unwrap();
        let saved = saved_trainer_association_json(&fixture.store, fixture.task_id);
        fixture.finish_registered_task();
        fixture.remove_shape_files();
        let changed_attempt = trainer_attempt_test_support::verified_attempt(
            trainer_attempt_test_support::attempt_binding("attempt-2"),
            [0x42; 32],
            OwnershipLockIdentity::new(7, 11),
        );
        let changed_lock = trainer_attempt_test_support::verified_attempt(
            trainer_attempt_test_support::attempt_binding("attempt-1"),
            [0x42; 32],
            OwnershipLockIdentity::new(7, 12),
        );
        let changed_request = trainer_attempt_test_support::verified_attempt(
            trainer_attempt_test_support::attempt_binding("attempt-1"),
            [0x43; 32],
            OwnershipLockIdentity::new(7, 11),
        );

        for changed in [changed_attempt, changed_lock, changed_request] {
            assert!(matches!(
                fixture.bind(changed),
                Err(TrainerAttemptAssociationStoreError::Conflict { resource_id })
                    if resource_id == fixture.resource.id
            ));
        }
        assert_eq!(
            saved_trainer_association_json(&fixture.store, fixture.task_id),
            saved
        );
    }

    #[test]
    fn trainer_attempt_associations_keep_history_and_read_only_the_current_registration() {
        let mut fixture = TrainerAssociationFixture::new();
        fixture.insert_accepted_running_task();
        let first = fixture.bind(fixture.evidence()).unwrap();
        let second_task = TaskId::new();
        let second_evidence = VerifiedTrainerAttempt::from_persisted(
            fixture.runtime_root.clone(),
            trainer_attempt_test_support::attempt_binding("attempt-2"),
            TrainerRequestDigest::from_hex(&"43".repeat(32)).unwrap(),
            OwnershipLockIdentity::new(7, 12),
        )
        .unwrap();

        fixture
            .store
            .conn
            .execute(
                "UPDATE resources SET registered_background_task=?1 WHERE id=?2",
                params![
                    second_task.to_string(),
                    fixture.resource.id.as_uuid().to_string()
                ],
            )
            .unwrap();
        let second_row = fixture.trainer_task(second_task, &fixture.spec);
        fixture
            .store
            .insert_local_task(
                &second_row,
                &fixture.spec,
                fixture.authority,
                PathBuf::from("/bin/echo"),
            )
            .unwrap();
        fixture
            .store
            .cas_status(second_task, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();

        let second = fixture
            .store
            .bind_trainer_attempt_association(
                fixture.authority,
                fixture.resource.id,
                second_task,
                second_evidence,
            )
            .unwrap();

        assert_ne!(first.task_id(), second.task_id());
        assert_eq!(
            fixture
                .store
                .trainer_attempt_association_for_authority(fixture.authority, fixture.resource.id)
                .unwrap(),
            Some(second.clone())
        );
        assert_eq!(
            fixture
                .store
                .trainer_attempt_association_for_task_for_authority(
                    fixture.authority,
                    fixture.task_id
                )
                .unwrap(),
            Some(first.clone())
        );
        assert_eq!(
            fixture
                .store
                .trainer_attempt_association_for_task_for_authority(fixture.authority, second_task)
                .unwrap(),
            Some(second)
        );
        let association_count: i64 = fixture
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM trainer_attempt_associations WHERE resource_id=?1",
                [fixture.resource.id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(association_count, 2);

        fixture
            .store
            .conn
            .execute(
                "UPDATE resources SET registered_background_task=NULL WHERE id=?1",
                [fixture.resource.id.as_uuid().to_string()],
            )
            .unwrap();
        assert_eq!(
            fixture
                .store
                .trainer_attempt_association_for_authority(fixture.authority, fixture.resource.id)
                .unwrap(),
            None
        );
        assert_eq!(
            fixture
                .store
                .trainer_attempt_association_for_task_for_authority(
                    fixture.authority,
                    fixture.task_id
                )
                .unwrap(),
            Some(first)
        );
    }

    #[test]
    fn trainer_attempt_association_rejects_wrong_authority_resource_and_task() {
        let mut fixture = TrainerAssociationFixture::new();
        fixture.insert_accepted_running_task();
        let evidence = fixture.evidence();
        let wrong_authority = MachineId::new();
        assert!(matches!(
            fixture.store.bind_trainer_attempt_association(
                wrong_authority,
                fixture.resource.id,
                fixture.task_id,
                evidence.clone(),
            ),
            Err(TrainerAttemptAssociationStoreError::Resource(
                ResourceStoreError::WrongAuthority { .. }
            ))
        ));
        assert!(matches!(
            fixture.store.bind_trainer_attempt_association(
                fixture.authority,
                ResourceId::new(),
                fixture.task_id,
                evidence.clone(),
            ),
            Err(TrainerAttemptAssociationStoreError::Resource(
                ResourceStoreError::ResourceNotFound
            ))
        ));

        let wrong_task = TaskId::new();
        assert!(matches!(
            fixture.store.bind_trainer_attempt_association(
                fixture.authority,
                fixture.resource.id,
                wrong_task,
                evidence.clone(),
            ),
            Err(TrainerAttemptAssociationStoreError::TaskNotRegistered { task_id })
                if task_id == wrong_task
        ));

        let mut other_resource = resource(fixture.authority);
        other_resource.registered_background_task = Some(TaskId::new());
        fixture
            .store
            .register_resource(fixture.authority, &other_resource)
            .unwrap();
        assert!(matches!(
            fixture.store.bind_trainer_attempt_association(
                fixture.authority,
                other_resource.id,
                fixture.task_id,
                evidence,
            ),
            Err(TrainerAttemptAssociationStoreError::TaskNotRegistered { task_id })
                if task_id == fixture.task_id
        ));
    }

    #[test]
    fn trainer_attempt_association_requires_running_task_identity_and_matching_spec() {
        let mut missing_task = TrainerAssociationFixture::new();
        let evidence = missing_task.evidence();
        assert!(matches!(
            missing_task.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::TaskMissing { task_id })
                if task_id == missing_task.task_id
        ));

        let mut queued_task = TrainerAssociationFixture::new();
        let row = queued_task.trainer_task(queued_task.task_id, &queued_task.spec);
        queued_task
            .store
            .insert_local_task(
                &row,
                &queued_task.spec,
                queued_task.authority,
                PathBuf::from("/bin/echo"),
            )
            .unwrap();
        let evidence = queued_task.evidence();
        assert!(matches!(
            queued_task.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::TaskNotRunning { state, .. })
                if state == "queued"
        ));

        let mut missing_identity = TrainerAssociationFixture::new();
        let row = missing_identity.trainer_task(missing_identity.task_id, &missing_identity.spec);
        missing_identity.store.insert_task(&row).unwrap();
        missing_identity
            .store
            .cas_status(
                missing_identity.task_id,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        let evidence = missing_identity.evidence();
        assert!(matches!(
            missing_identity.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::IdentityMissing { task_id })
                if task_id == missing_identity.task_id
        ));

        let mut stopped_identity = TrainerAssociationFixture::new();
        stopped_identity.insert_accepted_running_task();
        stopped_identity
            .store
            .update_execution_state(stopped_identity.task_id, ProcessStatus::Queued)
            .unwrap();
        let evidence = stopped_identity.evidence();
        assert!(matches!(
            stopped_identity.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::IdentityNotRunning { task_id })
                if task_id == stopped_identity.task_id
        ));

        let mut changed_row = TrainerAssociationFixture::new();
        changed_row.insert_accepted_running_task();
        changed_row
            .store
            .conn
            .execute(
                "UPDATE tasks SET cwd='/different' WHERE id=?1",
                [changed_row.task_id.to_string()],
            )
            .unwrap();
        let evidence = changed_row.evidence();
        assert!(matches!(
            changed_row.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::NormalizedSpecMismatch { task_id })
                if task_id == changed_row.task_id
        ));
    }

    #[test]
    fn trainer_attempt_association_checks_the_executor_authority_not_request_origin() {
        let mut fixture = TrainerAssociationFixture::new();
        let row = fixture.trainer_task(fixture.task_id, &fixture.spec);
        let origin = machine_other_than(fixture.authority);
        fixture
            .store
            .insert_remote_task(&row, &fixture.spec, origin, fixture.authority)
            .unwrap();
        fixture
            .store
            .cas_status(
                fixture.task_id,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        let evidence = fixture.evidence();
        assert!(fixture.bind(evidence).is_ok());

        let mut wrong_executor = TrainerAssociationFixture::new();
        let row = wrong_executor.trainer_task(wrong_executor.task_id, &wrong_executor.spec);
        let wrong_executor_machine = machine_other_than(wrong_executor.authority);
        wrong_executor
            .store
            .insert_remote_task(
                &row,
                &wrong_executor.spec,
                wrong_executor.authority,
                wrong_executor_machine,
            )
            .unwrap();
        wrong_executor
            .store
            .cas_status(
                wrong_executor.task_id,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        let evidence = wrong_executor.evidence();
        assert!(matches!(
            wrong_executor.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::IdentityMismatch { task_id })
                if task_id == wrong_executor.task_id
        ));
    }

    #[test]
    fn trainer_attempt_association_rejects_duplicate_task_across_resources() {
        let mut fixture = TrainerAssociationFixture::new();
        fixture.insert_accepted_running_task();
        let evidence = fixture.evidence();
        fixture.bind(evidence.clone()).unwrap();

        let mut second_resource = resource(fixture.authority);
        second_resource.registered_background_task = Some(fixture.task_id);
        fixture
            .store
            .register_resource(fixture.authority, &second_resource)
            .unwrap();
        assert!(matches!(
            fixture.store.bind_trainer_attempt_association(
                fixture.authority,
                second_resource.id,
                fixture.task_id,
                evidence,
            ),
            Err(TrainerAttemptAssociationStoreError::TaskAlreadyAssociated { task_id })
                if task_id == fixture.task_id
        ));
    }

    #[test]
    fn trainer_attempt_association_accepts_only_matching_awaiting_release_loan() {
        let mut awaiting = TrainerAssociationFixture::new();
        awaiting.insert_accepted_running_task();
        awaiting.add_loan(awaiting_release_state(awaiting.task_id));
        let evidence = awaiting.evidence();
        assert!(awaiting.bind(evidence).is_ok());

        let mut mismatched = TrainerAssociationFixture::new();
        mismatched.insert_accepted_running_task();
        mismatched.add_loan(awaiting_release_state(TaskId::new()));
        let evidence = mismatched.evidence();
        assert!(matches!(
            mismatched.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict { resource_id })
                if resource_id == mismatched.resource.id
        ));

        let mut serving = TrainerAssociationFixture::new();
        serving.insert_accepted_running_task();
        serving.add_loan(LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: ReturnContext::Stopped {
                    task_id: serving.task_id,
                    checkpoint_ref: "checkpoint".into(),
                    recovery_ref: "recovery".into(),
                },
                current_request_id: RequestId::new(),
                release_provenance: ServingReleaseProvenance::Unverified,
            },
        });
        let evidence = serving.evidence();
        assert!(matches!(
            serving.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict { .. })
        ));

        let mut returning = TrainerAssociationFixture::new();
        returning.insert_accepted_running_task();
        returning.add_loan(LoanState::Active {
            phase: LoanPhase::AwaitingReturn {
                action_id: ActionId::new(),
                return_context: ReturnContext::Idle,
            },
        });
        let evidence = returning.evidence();
        assert!(matches!(
            returning.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict { .. })
        ));

        let mut restoring = TrainerAssociationFixture::new();
        restoring.insert_accepted_running_task();
        restoring.add_loan(LoanState::Active {
            phase: LoanPhase::Restoring {
                action_id: ActionId::new(),
                return_context: ReturnContext::Idle,
                resume_task_id: TaskId::new(),
            },
        });
        let evidence = restoring.evidence();
        assert!(matches!(
            restoring.bind(evidence),
            Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict { .. })
        ));
    }

    #[test]
    fn trainer_attempt_association_migrates_old_resources_without_associations() {
        let fixture = TrainerAssociationFixture::new();
        let prior_resource = fixture
            .store
            .conn
            .query_row(
                "SELECT display_name, authority_machine, assignment_revision, state_revision,
                        registered_background_task
                 FROM resources WHERE id=?1",
                [fixture.resource.id.as_uuid().to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .unwrap();
        let database = fixture.database.clone();
        let resource_id = fixture.resource.id;
        let authority = fixture.authority;
        drop(fixture.store);

        let old_schema = rusqlite::Connection::open(&database).unwrap();
        old_schema
            .execute_batch(
                "DROP TABLE trainer_attempt_associations;
                 PRAGMA user_version=17;",
            )
            .unwrap();
        drop(old_schema);

        let migrated = Store::open(&database).unwrap();
        let version: i64 = migrated
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, crate::domain::SCHEMA_VERSION);
        assert_eq!(
            migrated
                .trainer_attempt_association_for_authority(authority, resource_id)
                .unwrap(),
            None
        );
        let current_resource = migrated
            .conn
            .query_row(
                "SELECT display_name, authority_machine, assignment_revision, state_revision,
                        registered_background_task
                 FROM resources WHERE id=?1",
                [resource_id.as_uuid().to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(current_resource, prior_resource);
    }

    #[test]
    fn trainer_attempt_association_v18_migration_preserves_rows_and_uses_task_key() {
        let mut fixture = TrainerAssociationFixture::new();
        fixture.insert_accepted_running_task();
        let association = fixture.bind(fixture.evidence()).unwrap();
        let database = fixture.database.clone();
        let resource_id = fixture.resource.id;
        let authority = fixture.authority;
        let task_id = fixture.task_id;
        let association_json = saved_trainer_association_json(&fixture.store, task_id);
        drop(fixture.store);

        let v18 = rusqlite::Connection::open(&database).unwrap();
        v18.execute_batch(
            "DROP TABLE trainer_attempt_associations;
             CREATE TABLE trainer_attempt_associations (
                 resource_id TEXT PRIMARY KEY REFERENCES resources(id),
                 authority_machine TEXT NOT NULL,
                 task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
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
             );",
        )
        .unwrap();
        v18.execute(
            "INSERT INTO trainer_attempt_associations (
                 resource_id, authority_machine, task_id, association_json
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                resource_id.as_uuid().to_string(),
                authority.as_uuid().to_string(),
                task_id.to_string(),
                association_json,
            ],
        )
        .unwrap();
        v18.pragma_update(None, "user_version", 18_i64).unwrap();
        drop(v18);

        let migrated = Store::open(&database).unwrap();
        let version: i64 = migrated
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, crate::domain::SCHEMA_VERSION);
        assert_eq!(
            migrated
                .trainer_attempt_association_for_authority(authority, resource_id)
                .unwrap(),
            Some(association.clone())
        );
        assert_eq!(
            migrated
                .trainer_attempt_association_for_task_for_authority(authority, task_id)
                .unwrap(),
            Some(association)
        );
        assert_eq!(
            saved_trainer_association_json(&migrated, task_id),
            association_json
        );
        let resource_primary_key: i64 = migrated
            .conn
            .query_row(
                "SELECT pk FROM pragma_table_info('trainer_attempt_associations')
                 WHERE name='resource_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let task_primary_key: i64 = migrated
            .conn
            .query_row(
                "SELECT pk FROM pragma_table_info('trainer_attempt_associations')
                 WHERE name='task_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(resource_primary_key, 0);
        assert_eq!(task_primary_key, 1);
    }

    fn open_release_for_test(
        store: &mut Store,
        authority: MachineId,
        background_task: TaskId,
    ) -> (Resource, Loan, SupervisorNotice) {
        let mut resource = resource(authority);
        resource.supervisor.thread = spec().thread;
        resource.registered_background_task = Some(background_task);
        store.register_resource(authority, &resource).unwrap();
        let trainer_spec = spec();
        let trainer_row = remote_task(background_task, &trainer_spec);
        store
            .insert_local_task(
                &trainer_row,
                &trainer_spec,
                authority,
                PathBuf::from("/bin/echo"),
            )
            .unwrap();
        store
            .cas_status(
                background_task,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        store
            .update_execution_state(background_task, ProcessStatus::Running)
            .unwrap();
        test_release_association(store, authority, resource.id, background_task);
        store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                MachineId::new(),
                spec(),
            )
            .unwrap();

        let OpenReleaseLoanResult::Opened { loan, notice } = store
            .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
            .unwrap()
        else {
            panic!("fixture must create a new release action");
        };

        (resource, loan, notice)
    }

    fn test_release_association(
        store: &mut Store,
        authority: MachineId,
        resource_id: ResourceId,
        task_id: TaskId,
    ) -> PathBuf {
        let runtime_root = store.tasks_dir.join(format!("release-proof-{task_id}"));
        fs::create_dir_all(&runtime_root).unwrap();
        let attempt_binding = AttemptBinding {
            campaign_id: "release-campaign".into(),
            campaign_revision_id: "release-revision".into(),
            task_id: "release-trainer-task".into(),
            attempt_id: format!("attempt-{}", task_id.0.simple()),
            attempt_number: 1,
            ownership_token: "release-owner".into(),
        };
        let evidence = VerifiedTrainerAttempt::from_persisted(
            runtime_root.clone(),
            attempt_binding,
            TrainerRequestDigest::from_hex(&"42".repeat(32)).unwrap(),
            OwnershipLockIdentity::new(7, 11),
        )
        .unwrap();
        let association = TrainerAttemptAssociation::from_components(
            resource_id,
            authority,
            task_id,
            evidence,
            normalized_spec_sha256(&spec()).unwrap(),
        )
        .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO trainer_attempt_associations
                 (task_id, resource_id, authority_machine, association_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    task_id.to_string(),
                    resource_id.as_uuid().to_string(),
                    authority.as_uuid().to_string(),
                    trainer_association_json(&association).unwrap(),
                ],
            )
            .unwrap();
        runtime_root
    }

    struct ServingFixture {
        directory: tempfile::TempDir,
        store: Store,
        authority: MachineId,
        origin: MachineId,
        resource: Resource,
        request: crate::resource::ResourceRequest,
        loan: Loan,
        state_revision: ResourceRevision,
        spec: NormalizedSpec,
    }

    fn resource_origin_route(
        request: RequestId,
        task: TaskId,
        resource: ResourceId,
        origin: MachineId,
        authority: MachineId,
        spec: &NormalizedSpec,
    ) -> OriginRoute {
        OriginRoute::new_resource_waiting(NewResourceRoute {
            request,
            task,
            origin_machine: origin,
            authority_machine: authority,
            thread: spec.thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: PathBuf::from("/tmp"),
                codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
            },
            spec: spec.clone(),
            resource,
        })
        .unwrap()
    }

    fn waiting_receipt(
        request: RequestId,
        task: TaskId,
        resource: ResourceId,
        origin: MachineId,
        authority: MachineId,
    ) -> ResourceQueueReceipt {
        ResourceQueueReceipt {
            request,
            task,
            origin_machine: origin,
            authority_machine: authority,
            resource,
            outcome: ResourceQueueOutcome::Waiting,
        }
    }

    fn serving_fixture(local_origin: bool, save_local_route: bool) -> ServingFixture {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = if local_origin {
            authority
        } else {
            MachineId::new()
        };
        let spec = spec();
        let background_task = TaskId::new();
        let request_id = RequestId::new();
        let task_id = TaskId::new();
        let mut resource = resource(authority);
        resource.supervisor.thread = spec.thread;
        resource.registered_background_task = Some(background_task);

        store.register_resource(authority, &resource).unwrap();
        store
            .insert_task(&remote_task(background_task, &spec))
            .unwrap();
        store
            .cas_status(
                background_task,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        if local_origin && save_local_route {
            store
                .insert_origin_route(&resource_origin_route(
                    request_id,
                    task_id,
                    resource.id,
                    origin,
                    authority,
                    &spec,
                ))
                .unwrap();
        }
        let request = store
            .accept_resource_request(
                authority,
                request_id,
                task_id,
                resource.id,
                origin,
                spec.clone(),
            )
            .unwrap();
        if local_origin && save_local_route {
            store
                .resolve_resource_route(&waiting_receipt(
                    request_id,
                    task_id,
                    resource.id,
                    origin,
                    authority,
                ))
                .unwrap();
        }

        let OpenReleaseLoanResult::Opened { .. } = store
            .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
            .unwrap()
        else {
            panic!("fixture must open one release action");
        };
        store
            .cas_exit(
                background_task,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
            )
            .unwrap()
            .unwrap();
        let (loan, state_revision) = store
            .seed_verified_serving_loan_for_test(
                authority,
                resource.id,
                request.request_id,
                ReturnContext::AlreadyCompleted {
                    task_id: background_task,
                    result_ref: "test serving fixture".into(),
                },
            )
            .unwrap();

        ServingFixture {
            directory,
            store,
            authority,
            origin,
            resource,
            request,
            loan,
            state_revision,
            spec,
        }
    }

    fn acceptance_input(fixture: &ServingFixture) -> ResourceTaskAcceptanceInput {
        ResourceTaskAcceptanceInput {
            authority_machine: fixture.authority,
            resource_id: fixture.resource.id,
            request_id: fixture.request.request_id,
            task_id: fixture.request.task_id,
            acceptance_sequence: fixture.request.acceptance_sequence,
            loan_id: fixture.loan.id,
            expected_state_revision: fixture.state_revision,
            command_spec: crate::resource::CommandSpec::try_from(fixture.spec.clone()).unwrap(),
            executor_env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
        }
    }

    fn completion(
        outcome: AssignedResourceTaskReconcileOutcome,
    ) -> Result<ResourceTaskCompletionResult, AssignedResourceTaskReconcileOutcome> {
        match outcome {
            AssignedResourceTaskReconcileOutcome::Completed(result) => Ok(*result),
            other => Err(other),
        }
    }

    fn task_reconcile_input(fixture: &ServingFixture) -> AssignedResourceTaskReconcileInput {
        AssignedResourceTaskReconcileInput {
            authority_machine: fixture.authority,
            resource_id: fixture.resource.id,
            loan_id: fixture.loan.id,
            request_id: fixture.request.request_id,
            task_id: fixture.request.task_id,
            expected_state_revision: fixture.state_revision,
        }
    }

    fn accept_and_finish_resource_task(
        fixture: &mut ServingFixture,
        outcome: ExitReason,
        evidence: ProcessGroupExitEvidence,
    ) -> AssignedResourceTaskReconcileInput {
        let acceptance = acceptance_input(fixture);
        assert_eq!(
            fixture
                .store
                .accept_assigned_resource_task(acceptance)
                .unwrap(),
            ResourceTaskAcceptance::Inserted {
                task: fixture.request.task_id,
            }
        );
        fixture
            .store
            .cas_status(
                fixture.request.task_id,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        fixture
            .store
            .cas_exit_with_evidence(
                fixture.request.task_id,
                ProcessStatus::Running,
                &outcome,
                evidence,
            )
            .unwrap()
            .unwrap();
        task_reconcile_input(fixture)
    }

    fn refresh_serving_fixture(fixture: &mut ServingFixture, loan: Loan, request: ResourceRequest) {
        fixture.loan = loan;
        fixture.request = request;
        fixture.state_revision = fixture
            .store
            .resource_snapshots_for_authority(fixture.authority)
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.resource.id == fixture.resource.id)
            .unwrap()
            .resource
            .state_revision;
    }

    fn accept_and_finish_next_resource_task(
        fixture: &mut ServingFixture,
        outcome: ExitReason,
        evidence: ProcessGroupExitEvidence,
    ) -> AssignedResourceTaskReconcileInput {
        let input = ResourceTaskAcceptanceInput {
            authority_machine: fixture.authority,
            resource_id: fixture.resource.id,
            request_id: fixture.request.request_id,
            task_id: fixture.request.task_id,
            acceptance_sequence: fixture.request.acceptance_sequence,
            loan_id: fixture.loan.id,
            expected_state_revision: fixture.state_revision,
            command_spec: fixture.request.spec().clone(),
            executor_env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
        };
        assert_eq!(
            fixture.store.accept_assigned_resource_task(input).unwrap(),
            ResourceTaskAcceptance::Inserted {
                task: fixture.request.task_id,
            }
        );
        fixture
            .store
            .cas_status(
                fixture.request.task_id,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        fixture
            .store
            .cas_exit_with_evidence(
                fixture.request.task_id,
                ProcessStatus::Running,
                &outcome,
                evidence,
            )
            .unwrap()
            .unwrap();
        task_reconcile_input(fixture)
    }

    fn acceptance_counts(store: &Store, request: RequestId, task: TaskId) -> [i64; 7] {
        [
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM tasks WHERE id=?1",
                    [task.to_string()],
                    |row| row.get(0),
                )
                .unwrap(),
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM executor_identities WHERE task_id=?1",
                    [task.to_string()],
                    |row| row.get(0),
                )
                .unwrap(),
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                    [task.to_string()],
                    |row| row.get(0),
                )
                .unwrap(),
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM executor_event_cursors WHERE task_id=?1",
                    [task.to_string()],
                    |row| row.get(0),
                )
                .unwrap(),
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM resource_requests WHERE request_id=?1",
                    [request.0.to_string()],
                    |row| row.get(0),
                )
                .unwrap(),
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM origin_routes WHERE request_id=?1",
                    [request.0.to_string()],
                    |row| row.get(0),
                )
                .unwrap(),
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM executor_event_receipts WHERE task_id=?1",
                    [task.to_string()],
                    |row| row.get(0),
                )
                .unwrap(),
        ]
    }

    #[test]
    fn assigned_resource_task_acceptance_commits_task_identity_and_first_event() {
        let mut fixture = serving_fixture(true, true);
        let input = acceptance_input(&fixture);

        assert_eq!(
            fixture
                .store
                .accept_assigned_resource_task(input.clone())
                .unwrap(),
            ResourceTaskAcceptance::Inserted {
                task: fixture.request.task_id,
            }
        );
        let task = fixture
            .store
            .get_task(fixture.request.task_id)
            .unwrap()
            .unwrap();
        assert_eq!(task.state, TaskState::Queued);
        assert!(matches!(
            fixture.store.executor_identity(task.id).unwrap(),
            Some(ExecutorIdentity::Accepted(record))
                if record.task == task.id
                    && record.origin_machine == fixture.origin
                    && record.execution_machine == fixture.authority
                    && record.state == ProcessStatus::Queued
        ));

        let events = fixture.store.pending_outbound_events(task.id).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.seq.get(), 1);
        assert_eq!(events[0].event.origin_machine, fixture.origin);
        assert_eq!(events[0].event.execution_machine, fixture.authority);
        assert_eq!(
            events[0].event.payload,
            EventPayload::State {
                status: ProcessStatus::Queued,
            }
        );
        assert!(matches!(
            fixture.store.resource_requests(fixture.authority, fixture.resource.id).unwrap()[0]
                .state,
            ResourceRequestState::Assigned { loan_id } if loan_id == fixture.loan.id
        ));
        assert!(matches!(
            fixture
                .store
                .resource_snapshots_for_authority(fixture.authority)
                .unwrap()[0]
                .loan
                .as_ref()
                .map(|loan| &loan.state),
            Some(LoanState::Active {
                phase: LoanPhase::Serving { current_request_id, .. }
            }) if *current_request_id == fixture.request.request_id
        ));
    }

    #[test]
    fn assigned_resource_task_event_failure_rolls_back_task_and_identity() {
        let mut fixture = serving_fixture(true, true);
        let input = acceptance_input(&fixture);
        fixture
            .store
            .conn
            .execute_batch(
                "CREATE TRIGGER reject_resource_task_event
                 BEFORE INSERT ON executor_outbox
                 BEGIN SELECT RAISE(ABORT, 'event storage is unavailable'); END;",
            )
            .unwrap();

        assert!(
            fixture
                .store
                .accept_assigned_resource_task(input.clone())
                .is_err()
        );
        assert_eq!(
            acceptance_counts(&fixture.store, input.request_id, input.task_id)[..4],
            [0, 0, 0, 0]
        );
        assert!(matches!(
            fixture
                .store
                .resource_requests(fixture.authority, fixture.resource.id)
                .unwrap()[0]
                .state,
            ResourceRequestState::Assigned { loan_id } if loan_id == fixture.loan.id
        ));
    }

    #[test]
    fn assigned_resource_task_retry_after_reopen_returns_existing_without_records() {
        let mut fixture = serving_fixture(true, true);
        let input = acceptance_input(&fixture);
        fixture
            .store
            .accept_assigned_resource_task(input.clone())
            .unwrap();
        let before = acceptance_counts(&fixture.store, input.request_id, input.task_id);
        let database = fixture.directory.path().join("db");
        let ServingFixture {
            directory, store, ..
        } = fixture;
        drop(store);

        let mut reopened = Store::open(&database).unwrap();
        assert_eq!(
            reopened
                .accept_assigned_resource_task(input.clone())
                .unwrap(),
            ResourceTaskAcceptance::Existing {
                task: input.task_id,
                state: ProcessStatus::Queued,
            }
        );
        assert_eq!(
            acceptance_counts(&reopened, input.request_id, input.task_id),
            before
        );
        drop(directory);
    }

    #[test]
    fn assigned_resource_task_retry_rejects_missing_row_or_changed_environment() {
        let mut fixture = serving_fixture(true, true);
        let input = acceptance_input(&fixture);
        fixture
            .store
            .accept_assigned_resource_task(input.clone())
            .unwrap();

        let mut changed_environment = input.clone();
        changed_environment.executor_env.home = "/different-home".into();
        assert!(matches!(
            fixture
                .store
                .accept_assigned_resource_task(changed_environment),
            Err(ResourceStoreError::Conflict)
        ));

        fixture
            .store
            .conn
            .execute("DELETE FROM tasks WHERE id=?1", [input.task_id.to_string()])
            .unwrap();
        assert!(matches!(
            fixture.store.accept_assigned_resource_task(input),
            Err(ResourceStoreError::Conflict)
        ));
    }

    #[test]
    fn assigned_resource_task_rejects_changed_spec_and_route() {
        let mut fixture = serving_fixture(true, true);
        let input = acceptance_input(&fixture);
        fixture
            .store
            .accept_assigned_resource_task(input.clone())
            .unwrap();

        let mut changed_spec = fixture.spec.clone();
        let NormalizedWorkload::Task(task) = &mut changed_spec.workload else {
            panic!("resource acceptance uses a command workload");
        };
        task.command = crate::invocation::CommandLine::try_from_argv(vec![
            "/bin/echo".into(),
            "changed".into(),
        ])
        .unwrap();
        let mut changed_input = input.clone();
        changed_input.command_spec = crate::resource::CommandSpec::try_from(changed_spec).unwrap();
        let before_changed_spec =
            acceptance_counts(&fixture.store, input.request_id, input.task_id);
        assert!(matches!(
            fixture.store.accept_assigned_resource_task(changed_input),
            Err(ResourceStoreError::Conflict)
        ));
        assert_eq!(
            acceptance_counts(&fixture.store, input.request_id, input.task_id),
            before_changed_spec
        );

        let mut route = fixture
            .store
            .origin_route_by_task(input.task_id)
            .unwrap()
            .unwrap();
        route.submission = SubmissionState::Resource {
            resource: ResourceId::new(),
            phase: ResourceRoutePhase::Waiting,
        };
        fixture
            .store
            .conn
            .execute(
                "UPDATE origin_routes SET route_json=?1 WHERE task_id=?2",
                rusqlite::params![
                    serde_json::to_string(&route).unwrap(),
                    input.task_id.to_string()
                ],
            )
            .unwrap();
        let before = acceptance_counts(&fixture.store, input.request_id, input.task_id);
        assert!(matches!(
            fixture.store.accept_assigned_resource_task(input.clone()),
            Err(ResourceStoreError::Conflict)
        ));
        assert_eq!(
            acceptance_counts(&fixture.store, input.request_id, input.task_id),
            before
        );
    }

    #[test]
    fn assigned_resource_task_rejects_wrong_authority_loan_request_and_revision() {
        let mut fixture = serving_fixture(true, true);
        let base = acceptance_input(&fixture);

        let mut wrong_authority = base.clone();
        wrong_authority.authority_machine = MachineId::new();
        assert!(matches!(
            fixture.store.accept_assigned_resource_task(wrong_authority),
            Err(ResourceStoreError::WrongAuthority { .. })
        ));

        let mut wrong_loan = base.clone();
        wrong_loan.loan_id = LoanId::new();
        assert!(matches!(
            fixture.store.accept_assigned_resource_task(wrong_loan),
            Err(ResourceStoreError::Conflict)
        ));

        let mut wrong_request = base.clone();
        wrong_request.request_id = RequestId::new();
        assert!(matches!(
            fixture.store.accept_assigned_resource_task(wrong_request),
            Err(ResourceStoreError::Conflict)
        ));

        let mut stale_revision = base.clone();
        stale_revision.expected_state_revision =
            ResourceRevision::new(fixture.state_revision.get() + 1);
        assert!(matches!(
            fixture.store.accept_assigned_resource_task(stale_revision),
            Err(ResourceStoreError::Conflict)
        ));
        assert_eq!(
            acceptance_counts(&fixture.store, base.request_id, base.task_id)[..4],
            [0, 0, 0, 0]
        );
    }

    #[test]
    fn assigned_resource_task_rejects_a_non_fifo_loan_selection() {
        let mut fixture = serving_fixture(true, true);
        let later_request = fixture
            .store
            .accept_resource_request(
                fixture.authority,
                RequestId::new(),
                TaskId::new(),
                fixture.resource.id,
                MachineId::new(),
                fixture.spec.clone(),
            )
            .unwrap();
        let LoanState::Active {
            phase: LoanPhase::Serving { return_context, .. },
        } = fixture.loan.state.clone()
        else {
            panic!("fixture must have one serving request");
        };

        fixture
            .store
            .conn
            .execute(
                "UPDATE resource_requests SET state_json=?1 WHERE request_id=?2",
                rusqlite::params![
                    serde_json::to_string(&ResourceRequestState::Queued).unwrap(),
                    fixture.request.request_id.0.to_string(),
                ],
            )
            .unwrap();
        fixture
            .store
            .conn
            .execute(
                "UPDATE resource_requests SET state_json=?1 WHERE request_id=?2",
                rusqlite::params![
                    serde_json::to_string(&ResourceRequestState::Assigned {
                        loan_id: fixture.loan.id,
                    })
                    .unwrap(),
                    later_request.request_id.0.to_string(),
                ],
            )
            .unwrap();
        let later_loan = Loan {
            id: fixture.loan.id,
            resource_id: fixture.resource.id,
            state: LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context,
                    current_request_id: later_request.request_id,
                    release_provenance: ServingReleaseProvenance::Unverified,
                },
            },
        };
        fixture
            .store
            .conn
            .execute(
                "UPDATE loans SET state_json=?1 WHERE id=?2",
                rusqlite::params![
                    serde_json::to_string(&later_loan.state).unwrap(),
                    fixture.loan.id.as_uuid().to_string(),
                ],
            )
            .unwrap();

        let input = ResourceTaskAcceptanceInput {
            authority_machine: fixture.authority,
            resource_id: fixture.resource.id,
            request_id: later_request.request_id,
            task_id: later_request.task_id,
            acceptance_sequence: later_request.acceptance_sequence,
            loan_id: fixture.loan.id,
            expected_state_revision: fixture.state_revision,
            command_spec: later_request.spec().clone(),
            executor_env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
        };
        assert!(matches!(
            fixture.store.accept_assigned_resource_task(input),
            Err(ResourceStoreError::Conflict)
        ));
        assert_eq!(
            acceptance_counts(
                &fixture.store,
                later_request.request_id,
                later_request.task_id
            )[..4],
            [0, 0, 0, 0]
        );
    }

    #[test]
    fn confirmed_success_failure_and_cancel_drain_three_requests_in_fifo_order() {
        let mut fixture = serving_fixture(true, true);
        let second = fixture
            .store
            .accept_resource_request(
                fixture.authority,
                RequestId::new(),
                TaskId::new(),
                fixture.resource.id,
                MachineId::new(),
                fixture.spec.clone(),
            )
            .unwrap();
        let third = fixture
            .store
            .accept_resource_request(
                fixture.authority,
                RequestId::new(),
                TaskId::new(),
                fixture.resource.id,
                MachineId::new(),
                fixture.spec.clone(),
            )
            .unwrap();

        let first_input = accept_and_finish_resource_task(
            &mut fixture,
            ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let first = fixture
            .store
            .reconcile_assigned_resource_task_for_authority(first_input)
            .unwrap();
        let (loan, next_request) = match completion(first) {
            Ok(
                ResourceTaskCompletionResult::Assigned {
                    finished_request,
                    loan,
                    next_request,
                    ..
                },
            ) => {
                assert!(matches!(
                    &finished_request.state,
                    ResourceRequestState::Finished {
                        outcome: ExitReason::Exit { code: 0 }
                    }
                ));
                (loan, next_request)
            }
            other => {
                panic!("first confirmed completion did not assign the next request: {other:?}")
            }
        };
        assert_eq!(next_request.request_id, second.request_id);
        assert_eq!(loan.id, fixture.loan.id);
        assert!(matches!(
            &next_request.state,
            ResourceRequestState::Assigned { loan_id } if *loan_id == loan.id
        ));
        assert!(matches!(
            fixture
                .store
                .resource_requests(fixture.authority, fixture.resource.id)
                .unwrap()[2]
                .state,
            ResourceRequestState::Queued
        ));
        refresh_serving_fixture(&mut fixture, loan, next_request);

        let second_input = accept_and_finish_next_resource_task(
            &mut fixture,
            ExitReason::Exit { code: 7 },
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let second_result = fixture
            .store
            .reconcile_assigned_resource_task_for_authority(second_input)
            .unwrap();
        let (loan, next_request) = match completion(second_result) {
            Ok(
                ResourceTaskCompletionResult::Assigned {
                    finished_request,
                    loan,
                    next_request,
                    ..
                },
            ) => {
                assert!(matches!(
                    &finished_request.state,
                    ResourceRequestState::Finished {
                        outcome: ExitReason::Exit { code: 7 }
                    }
                ));
                (loan, next_request)
            }
            other => {
                panic!("second confirmed completion did not assign the last request: {other:?}")
            }
        };
        assert_eq!(next_request.request_id, third.request_id);
        assert_eq!(loan.id, fixture.loan.id);
        refresh_serving_fixture(&mut fixture, loan, next_request);

        let third_input = accept_and_finish_next_resource_task(
            &mut fixture,
            ExitReason::Cancelled,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let third_result = fixture
            .store
            .reconcile_assigned_resource_task_for_authority(third_input)
            .unwrap();
        let (loan, notice) = match completion(third_result) {
            Ok(
                ResourceTaskCompletionResult::ReturnRequired {
                    finished_request,
                    loan,
                    notice,
                },
            ) => {
                assert!(matches!(
                    &finished_request.state,
                    ResourceRequestState::Finished {
                        outcome: ExitReason::Cancelled
                    }
                ));
                (loan, notice)
            }
            other => panic!("empty queue did not reserve the return: {other:?}"),
        };
        assert!(matches!(
            loan.state,
            LoanState::Active {
                phase: LoanPhase::AwaitingReturn { action_id, .. }
            } if action_id == notice.action_id
        ));
        assert_eq!(notice.loan_id, fixture.loan.id);
        assert!(matches!(
            notice.payload,
            crate::resource::SupervisorNoticePayload::ReturnRequired { .. }
        ));
        let notice_count: i64 = fixture
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM resource_supervisor_notices
                 WHERE loan_id=?1
                   AND json_extract(notice_json, '$.payload.type') = 'return_required'",
                [fixture.loan.id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(notice_count, 1);
    }

    #[test]
    fn queue_does_not_advance_until_the_exact_process_group_exit_is_confirmed() {
        let mut fixture = serving_fixture(true, true);
        let next = fixture
            .store
            .accept_resource_request(
                fixture.authority,
                RequestId::new(),
                TaskId::new(),
                fixture.resource.id,
                MachineId::new(),
                fixture.spec.clone(),
            )
            .unwrap();
        let input = acceptance_input(&fixture);
        fixture.store.accept_assigned_resource_task(input).unwrap();
        fixture
            .store
            .cas_status(
                fixture.request.task_id,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();

        assert!(matches!(
            fixture
                .store
                .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
                .unwrap(),
            AssignedResourceTaskReconcileOutcome::Active {
                state: ProcessStatus::Running
            }
        ));
        fixture
            .store
            .cas_exit_with_evidence(
                fixture.request.task_id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
                ProcessGroupExitEvidence::Unconfirmed,
            )
            .unwrap()
            .unwrap();
        assert!(matches!(
            fixture
                .store
                .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
                .unwrap(),
            AssignedResourceTaskReconcileOutcome::Attention(
                AssignedResourceTaskAttention::ProcessGroupExitUnconfirmed
            )
        ));
        let requests = fixture
            .store
            .resource_requests(fixture.authority, fixture.resource.id)
            .unwrap();
        assert!(matches!(
            &requests[0].state,
            ResourceRequestState::Assigned { loan_id } if *loan_id == fixture.loan.id
        ));
        assert!(matches!(requests[1].state, ResourceRequestState::Queued));
        assert!(fixture.store.get_task(next.task_id).unwrap().is_none());
        assert_eq!(
            fixture
                .store
                .resource_snapshots_for_authority(fixture.authority)
                .unwrap()[0]
                .resource
                .state_revision,
            fixture.state_revision
        );
    }

    #[test]
    fn lost_task_retains_its_assignment_and_reports_attention() {
        let mut fixture = serving_fixture(true, true);
        let next = fixture
            .store
            .accept_resource_request(
                fixture.authority,
                RequestId::new(),
                TaskId::new(),
                fixture.resource.id,
                MachineId::new(),
                fixture.spec.clone(),
            )
            .unwrap();
        fixture
            .store
            .accept_assigned_resource_task(acceptance_input(&fixture))
            .unwrap();
        fixture
            .store
            .cas_status(
                fixture.request.task_id,
                ProcessStatus::Queued,
                ProcessStatus::Lost,
            )
            .unwrap()
            .unwrap();

        assert!(matches!(
            fixture
                .store
                .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
                .unwrap(),
            AssignedResourceTaskReconcileOutcome::Attention(
                AssignedResourceTaskAttention::TaskLost
            )
        ));
        let requests = fixture
            .store
            .resource_requests(fixture.authority, fixture.resource.id)
            .unwrap();
        assert!(matches!(
            &requests[0].state,
            ResourceRequestState::Assigned { loan_id } if *loan_id == fixture.loan.id
        ));
        assert!(matches!(requests[1].state, ResourceRequestState::Queued));
        assert!(fixture.store.get_task(next.task_id).unwrap().is_none());
    }

    #[test]
    fn mismatched_task_executor_loan_and_revision_do_not_commit_completion() {
        let mut fixture = serving_fixture(true, true);
        let input = accept_and_finish_resource_task(
            &mut fixture,
            ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::ConfirmedExited,
        );

        let mut wrong_task = input;
        wrong_task.task_id = TaskId::new();
        assert!(matches!(
            fixture
                .store
                .reconcile_assigned_resource_task_for_authority(wrong_task)
                .unwrap(),
            AssignedResourceTaskReconcileOutcome::Attention(
                AssignedResourceTaskAttention::TaskIdentityMismatch
            )
        ));
        let mut wrong_loan = input;
        wrong_loan.loan_id = LoanId::new();
        assert!(matches!(
            fixture
                .store
                .reconcile_assigned_resource_task_for_authority(wrong_loan)
                .unwrap(),
            AssignedResourceTaskReconcileOutcome::Attention(_)
        ));
        let mut stale_revision = input;
        stale_revision.expected_state_revision =
            ResourceRevision::new(input.expected_state_revision.get() + 1);
        assert!(matches!(
            fixture
                .store
                .reconcile_assigned_resource_task_for_authority(stale_revision)
                .unwrap(),
            AssignedResourceTaskReconcileOutcome::Attention(
                AssignedResourceTaskAttention::StaleRevision
            )
        ));

        let Some(ExecutorIdentity::Accepted(mut accepted)) =
            fixture.store.executor_identity(input.task_id).unwrap()
        else {
            panic!("accepted resource task must retain an executor identity");
        };
        accepted.origin_machine = MachineId::new();
        fixture
            .store
            .conn
            .execute(
                "UPDATE executor_identities SET identity_json=?1 WHERE task_id=?2",
                rusqlite::params![
                    serde_json::to_string(&ExecutorIdentity::Accepted(accepted)).unwrap(),
                    input.task_id.to_string(),
                ],
            )
            .unwrap();
        assert!(matches!(
            fixture
                .store
                .reconcile_assigned_resource_task_for_authority(input)
                .unwrap(),
            AssignedResourceTaskReconcileOutcome::Attention(
                AssignedResourceTaskAttention::TaskIdentityMismatch
            )
        ));
        assert!(matches!(
            fixture.store.resource_requests(fixture.authority, fixture.resource.id).unwrap()[0]
                .state,
            ResourceRequestState::Assigned { loan_id } if loan_id == fixture.loan.id
        ));
        let receipt_count: i64 = fixture
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM resource_task_completions",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(receipt_count, 0);
    }

    #[test]
    fn queued_request_cancelled_during_a_serving_task_is_skipped() {
        let mut fixture = serving_fixture(true, true);
        let cancelled = fixture
            .store
            .accept_resource_request(
                fixture.authority,
                RequestId::new(),
                TaskId::new(),
                fixture.resource.id,
                MachineId::new(),
                fixture.spec.clone(),
            )
            .unwrap();
        let next = fixture
            .store
            .accept_resource_request(
                fixture.authority,
                RequestId::new(),
                TaskId::new(),
                fixture.resource.id,
                MachineId::new(),
                fixture.spec.clone(),
            )
            .unwrap();
        fixture
            .store
            .accept_assigned_resource_task(acceptance_input(&fixture))
            .unwrap();
        fixture
            .store
            .cas_status(
                fixture.request.task_id,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        fixture
            .store
            .cancel_resource_request_before_activation(
                fixture.authority,
                cancelled.request_id,
                cancelled.task_id,
                fixture.resource.id,
                cancelled.origin_machine,
            )
            .unwrap();
        fixture
            .store
            .cas_exit_with_evidence(
                fixture.request.task_id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
                ProcessGroupExitEvidence::ConfirmedExited,
            )
            .unwrap()
            .unwrap();

        let result = fixture
            .store
            .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
            .unwrap();
        assert!(matches!(
            completion(result),
            Ok(
                ResourceTaskCompletionResult::Assigned {
                    next_request,
                    ..
                }
            ) if next_request.request_id == next.request_id
        ));
        let requests = fixture
            .store
            .resource_requests(fixture.authority, fixture.resource.id)
            .unwrap();
        assert!(matches!(
            requests
                .iter()
                .find(|request| request.request_id == cancelled.request_id)
                .unwrap()
                .state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert!(matches!(
            requests.iter().find(|request| request.request_id == next.request_id).unwrap().state,
            ResourceRequestState::Assigned { loan_id } if loan_id == fixture.loan.id
        ));
    }

    #[test]
    fn no_child_spawn_after_initial_spawn_failure_is_an_explicit_release_proof() {
        let mut fixture = serving_fixture(true, true);
        fixture
            .store
            .accept_assigned_resource_task(acceptance_input(&fixture))
            .unwrap();
        fixture
            .store
            .cas_exit_with_evidence(
                fixture.request.task_id,
                ProcessStatus::Queued,
                &ExitReason::SpawnFailed {
                    message: "fake task runner could not start".into(),
                },
                ProcessGroupExitEvidence::NoChildSpawned,
            )
            .unwrap()
            .unwrap();

        let result = fixture
            .store
            .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
            .unwrap();
        assert!(matches!(
            completion(result),
            Ok(
                ResourceTaskCompletionResult::ReturnRequired { .. }
            )
        ));
        let receipt: String = fixture
            .store
            .conn
            .query_row(
                "SELECT receipt_json FROM resource_task_completions WHERE task_id=?1",
                [fixture.request.task_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&receipt).unwrap()["release_proof"],
            "no_child_spawned_after_spawn_failure"
        );
        assert!(matches!(
            fixture
                .store
                .resource_requests(fixture.authority, fixture.resource.id)
                .unwrap()[0]
                .state,
            ResourceRequestState::Finished {
                outcome: ExitReason::SpawnFailed { .. }
            }
        ));
    }

    #[test]
    fn exact_task_completion_retry_after_reopen_returns_the_same_return_notice() {
        let mut fixture = serving_fixture(true, true);
        let input = accept_and_finish_resource_task(
            &mut fixture,
            ExitReason::Cancelled,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let first = fixture
            .store
            .reconcile_assigned_resource_task_for_authority(input)
            .unwrap();
        let (action_id, notice_id, revision) = match completion(first) {
            Ok(
                ResourceTaskCompletionResult::ReturnRequired {
                    notice, ..
                },
            ) => (notice.action_id, notice.id, notice.state_revision),
            other => panic!("the empty queue must reserve one return notice: {other:?}"),
        };
        let database = fixture.directory.path().join("db");
        let committed_revision = fixture
            .store
            .resource_snapshots_for_authority(fixture.authority)
            .unwrap()[0]
            .resource
            .state_revision;
        drop(fixture.store);

        let mut reopened = Store::open(&database).unwrap();
        let retry = reopened
            .reconcile_assigned_resource_task_for_authority(input)
            .unwrap();
        assert!(matches!(
            completion(retry),
            Ok(
                ResourceTaskCompletionResult::ReturnRequired {
                    notice,
                    ..
                }
            ) if notice.action_id == action_id
                && notice.id == notice_id
                && notice.state_revision == revision
        ));
        assert_eq!(
            reopened
                .resource_snapshots_for_authority(fixture.authority)
                .unwrap()[0]
                .resource
                .state_revision,
            committed_revision
        );
        let receipt_count: i64 = reopened
            .conn
            .query_row(
                "SELECT COUNT(*) FROM resource_task_completions",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let notice_count: i64 = reopened
            .conn
            .query_row(
                "SELECT COUNT(*) FROM resource_supervisor_notices
                 WHERE json_extract(notice_json, '$.payload.type') = 'return_required'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(receipt_count, 1);
        assert_eq!(notice_count, 1);
    }

    #[test]
    fn assigned_resource_task_requires_local_callback_route_before_insert() {
        let mut fixture = serving_fixture(true, false);
        let input = acceptance_input(&fixture);

        assert!(matches!(
            fixture.store.accept_assigned_resource_task(input.clone()),
            Err(ResourceStoreError::OriginRouteNotFound { task }) if task == input.task_id
        ));
        assert_eq!(
            acceptance_counts(&fixture.store, input.request_id, input.task_id)[..4],
            [0, 0, 0, 0]
        );
    }

    #[test]
    fn assigned_remote_resource_task_uses_the_saved_origin_event_route() {
        let mut fixture = serving_fixture(false, false);
        let origin_directory = tempdir().unwrap();
        let mut origin_store = Store::open(&origin_directory.path().join("origin-db")).unwrap();
        origin_store
            .insert_origin_route(&resource_origin_route(
                fixture.request.request_id,
                fixture.request.task_id,
                fixture.resource.id,
                fixture.origin,
                fixture.authority,
                &fixture.spec,
            ))
            .unwrap();
        origin_store
            .resolve_resource_route(&waiting_receipt(
                fixture.request.request_id,
                fixture.request.task_id,
                fixture.resource.id,
                fixture.origin,
                fixture.authority,
            ))
            .unwrap();

        let input = acceptance_input(&fixture);
        assert_eq!(
            fixture
                .store
                .accept_assigned_resource_task(input.clone())
                .unwrap(),
            ResourceTaskAcceptance::Inserted {
                task: fixture.request.task_id,
            }
        );
        assert!(
            fixture
                .store
                .origin_route_by_task(fixture.request.task_id)
                .unwrap()
                .is_none()
        );
        let event = fixture
            .store
            .pending_outbound_events(input.task_id)
            .unwrap()[0]
            .event
            .clone();
        assert_eq!(event.origin_machine, fixture.origin);
        assert_eq!(event.execution_machine, fixture.authority);
        assert_eq!(
            origin_store.accept_inbound_event(&event).unwrap(),
            crate::events::EventAcceptance::Acknowledged { seq: 1 }
        );
        fixture
            .store
            .mark_outbound_acknowledged(input.task_id, event.seq)
            .unwrap();
        let route = origin_store
            .origin_route_by_task(input.task_id)
            .unwrap()
            .unwrap();
        assert!(matches!(
            route.submission,
            SubmissionState::Resource {
                phase: ResourceRoutePhase::Activated,
                ..
            }
        ));
        assert_eq!(route.origin_machine, fixture.origin);
        assert_eq!(route.execution_machine, fixture.authority);

        fixture
            .store
            .cas_status(input.task_id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();
        let running = fixture
            .store
            .pending_outbound_events(input.task_id)
            .unwrap()[0]
            .event
            .clone();
        assert_eq!(running.seq.get(), 2);
        assert_eq!(
            origin_store.accept_inbound_event(&running).unwrap(),
            crate::events::EventAcceptance::Acknowledged { seq: 2 }
        );
        fixture
            .store
            .mark_outbound_acknowledged(input.task_id, running.seq)
            .unwrap();
        fixture
            .store
            .cas_exit_with_evidence(
                input.task_id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 9 },
                ProcessGroupExitEvidence::ConfirmedExited,
            )
            .unwrap()
            .unwrap();
        assert!(matches!(
            fixture
                .store
                .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
                .unwrap(),
            AssignedResourceTaskReconcileOutcome::Completed(_)
        ));

        let terminal = fixture
            .store
            .pending_outbound_events(input.task_id)
            .unwrap()[0]
            .event
            .clone();
        assert_eq!(terminal.seq.get(), 3);
        assert_eq!(
            origin_store.accept_inbound_event(&terminal).unwrap(),
            crate::events::EventAcceptance::Acknowledged { seq: 3 }
        );
        fixture
            .store
            .mark_outbound_acknowledged(input.task_id, terminal.seq)
            .unwrap();
        let route = origin_store
            .origin_route_by_task(input.task_id)
            .unwrap()
            .unwrap();
        assert_eq!(route.last_accepted_seq, 3);
        assert_eq!(route.last_execution_state, Some(ProcessStatus::Failed));
    }

    #[test]
    fn cancellation_and_assigned_resource_task_acceptance_resolve_in_either_order() {
        let mut cancelled_first = serving_fixture(true, true);
        let input = acceptance_input(&cancelled_first);
        cancelled_first
            .store
            .cancel_resource_request_before_activation(
                cancelled_first.authority,
                input.request_id,
                input.task_id,
                input.resource_id,
                cancelled_first.origin,
            )
            .unwrap();
        assert!(matches!(
            cancelled_first
                .store
                .accept_assigned_resource_task(input.clone()),
            Err(ResourceStoreError::Prevented)
        ));
        let counts = acceptance_counts(&cancelled_first.store, input.request_id, input.task_id);
        assert_eq!(counts[0], 0);
        assert_eq!(&counts[2..4], [0, 0]);
        assert!(matches!(
            cancelled_first
                .store
                .resource_requests(cancelled_first.authority, cancelled_first.resource.id)
                .unwrap()[0]
                .state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert!(matches!(
            cancelled_first.store.executor_identity(input.task_id).unwrap(),
            Some(ExecutorIdentity::Rejected(rejection))
                if rejection.reason == PreAcceptanceRejection::Cancelled.as_str()
        ));

        let mut accepted_first = serving_fixture(true, true);
        let input = acceptance_input(&accepted_first);
        accepted_first
            .store
            .accept_assigned_resource_task(input.clone())
            .unwrap();
        let loan_before = accepted_first
            .store
            .resource_snapshots_for_authority(accepted_first.authority)
            .unwrap()[0]
            .loan
            .clone()
            .unwrap();
        let cancellation = resource_cancellation_identity(
            Uuid::now_v7(),
            input.request_id,
            input.task_id,
            input.resource_id,
            accepted_first.origin,
            accepted_first.authority,
            ResourceRoutePhase::Waiting,
        );
        let receipt = accepted_first
            .store
            .cancel_resource_request_with_receipt(
                accepted_first.authority,
                cancellation.clone(),
                resource_cancellation_proof(
                    &cancellation,
                    &accepted_first.spec,
                    ResourceRoutePhase::Waiting,
                ),
            )
            .unwrap();
        assert_eq!(
            receipt.outcome,
            crate::submission::ResourceCancellationOutcome::NotEligible {
                reason: crate::submission::ResourceCancellationIneligibleReason::Activated,
            }
        );
        assert_eq!(
            accepted_first
                .store
                .resource_snapshots_for_authority(accepted_first.authority)
                .unwrap()[0]
                .loan
                .clone()
                .unwrap(),
            loan_before
        );
        assert!(matches!(
            accepted_first
                .store
                .resource_requests(accepted_first.authority, accepted_first.resource.id)
                .unwrap()[0]
                .state,
            ResourceRequestState::Assigned { loan_id } if loan_id == accepted_first.loan.id
        ));
    }

    const TEST_WATCHER_EXECUTABLE: &str = "/bin/echo";

    fn release_watcher_intent(
        resource_id: ResourceId,
        notice: &SupervisorNotice,
        background_task: TaskId,
        watcher_task: TaskId,
    ) -> ReleaseWatcherIntent {
        let watcher_task_id = ReleaseWatcherTaskId::new(watcher_task);
        let command = ReleaseWatcherCommand {
            resource_id,
            action_id: notice.action_id,
            state_revision: notice.state_revision,
            trainer_task_id: background_task,
            watcher_task_id,
        };
        ReleaseWatcherIntent {
            action_id: notice.action_id,
            state_revision: notice.state_revision,
            observed_background_task: background_task,
            watcher_task_id,
            request_id: RequestId::new(),
            normalized_spec_sha256: command
                .normalized_spec_sha256(
                    Path::new(TEST_WATCHER_EXECUTABLE),
                    notice.destination.thread,
                )
                .unwrap(),
        }
    }

    fn watcher_spec(resource: &Resource, intent: &ReleaseWatcherIntent) -> NormalizedSpec {
        ReleaseWatcherCommand::from_intent(resource.id, intent)
            .normalized_spec(
                Path::new(TEST_WATCHER_EXECUTABLE),
                resource.supervisor.thread,
            )
            .unwrap()
    }

    fn prepare_release_checkpoint_baseline(
        store: &mut Store,
        authority: MachineId,
        resource: &Resource,
        intent: &ReleaseWatcherIntent,
    ) -> ReleaseCheckpointBaseline {
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
            .unwrap();
        store
            .capture_release_checkpoint_baseline_for_authority(
                authority,
                resource.id,
                intent.action_id,
                intent.state_revision,
            )
            .unwrap()
    }

    fn saved_release_association(
        store: &Store,
        resource_id: ResourceId,
        task_id: TaskId,
    ) -> TrainerAttemptAssociation {
        trainer_association_by_resource_and_task(&store.conn, resource_id, task_id)
            .unwrap()
            .unwrap()
    }

    fn watcher_task_and_callback(
        intent: &ReleaseWatcherIntent,
        spec: &NormalizedSpec,
    ) -> (crate::domain::TaskRow, CallbackContext) {
        let row = remote_task(intent.watcher_task_id.as_task_id(), spec);
        let callback = CallbackContext {
            env: row.env.clone(),
            cwd: row.cwd.clone(),
            codex: crate::submission::CallbackExecutable::available(PathBuf::from("/bin/echo")),
        };
        (row, callback)
    }

    fn watcher_acceptance_input(
        resource: &Resource,
        intent: ReleaseWatcherIntent,
        row: &crate::domain::TaskRow,
        spec: &NormalizedSpec,
        callback: &CallbackContext,
    ) -> ReleaseWatcherAcceptanceInput {
        ReleaseWatcherAcceptanceInput {
            authority_machine: resource.authority_machine(),
            resource_id: resource.id,
            supervisor: resource.supervisor,
            intent,
            row: row.clone(),
            spec: spec.clone(),
            callback: callback.clone(),
        }
    }

    fn accept_watcher(
        store: &mut Store,
        resource: &Resource,
        intent: ReleaseWatcherIntent,
        row: &crate::domain::TaskRow,
        spec: &NormalizedSpec,
        callback: &CallbackContext,
    ) -> Result<ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError> {
        store.accept_release_watcher_for_authority(watcher_acceptance_input(
            resource, intent, row, spec, callback,
        ))
    }

    fn checkpoint_decision_for_cancellation(
        store: &mut Store,
        authority: MachineId,
        background_task: TaskId,
    ) -> (
        Resource,
        SupervisorNotice,
        ReleaseWatcherIntent,
        ReleaseCheckpointStopDecision,
    ) {
        let (resource, _, notice) = open_release_for_test(store, authority, background_task);
        let association = saved_release_association(store, resource.id, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        prepare_release_checkpoint_baseline(store, authority, &resource, &intent);
        crate::resource::watcher::tests::write_generation_for_test(
            association.verified_attempt().canonical_runtime_root(),
            association.verified_attempt().binding(),
            "generation-cancellation",
            81,
        );
        let ReleaseCheckpointStopOutcome::Reserved(decision) = store
            .reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap()
        else {
            panic!("the exact checkpoint must reserve a stop decision");
        };

        (resource, notice, intent, decision)
    }

    fn start_release_watcher_for_test(
        store: &mut Store,
        resource: &Resource,
        intent: &ReleaseWatcherIntent,
    ) {
        let watcher_spec = watcher_spec(resource, intent);
        let (row, callback) = watcher_task_and_callback(intent, &watcher_spec);
        accept_watcher(
            store,
            resource,
            intent.clone(),
            &row,
            &watcher_spec,
            &callback,
        )
        .unwrap();
        store
            .cas_status(row.id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();
        store.set_pid(row.id, std::process::id() as i32).unwrap();
        store
            .update_execution_state(row.id, ProcessStatus::Running)
            .unwrap();
    }

    fn watcher_acceptance_counts(store: &Store, request: RequestId, task: TaskId) -> [i64; 4] {
        [
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM tasks WHERE id=?1",
                    [task.to_string()],
                    |row| row.get(0),
                )
                .unwrap(),
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM origin_routes WHERE request_id=?1",
                    [request.0.to_string()],
                    |row| row.get(0),
                )
                .unwrap(),
            identity_count(store, task),
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                    [task.to_string()],
                    |row| row.get(0),
                )
                .unwrap(),
        ]
    }

    #[test]
    fn release_watcher_acceptance_inserts_fixed_task_route_identity_and_event() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        let spec = watcher_spec(&resource, &intent);
        prepare_release_checkpoint_baseline(&mut store, authority, &resource, &intent);
        let (row, callback) = watcher_task_and_callback(&intent, &spec);

        let accepted = accept_watcher(
            &mut store,
            &resource,
            intent.clone(),
            &row,
            &spec,
            &callback,
        )
        .unwrap();
        assert_eq!(
            accepted,
            ReleaseWatcherAcceptance::Inserted { task: row.id }
        );
        assert_eq!(
            store.get_task(row.id).unwrap().unwrap().status(),
            ProcessStatus::Queued
        );

        let route = store
            .origin_route_by_request(intent.request_id)
            .unwrap()
            .unwrap();
        assert_eq!(route.task, row.id);
        assert_eq!(route.origin_machine, authority);
        assert_eq!(route.execution_machine, authority);
        assert_eq!(route.callback, callback);
        assert_eq!(
            serde_json::to_value(route.current_spec().unwrap()).unwrap(),
            serde_json::to_value(&spec).unwrap()
        );
        assert!(matches!(route.submission, SubmissionState::Accepted));
        let ExecutorIdentity::Accepted(identity) =
            store.executor_identity(row.id).unwrap().unwrap()
        else {
            panic!("watcher task must have an accepted executor identity");
        };
        assert_eq!(identity.origin_machine, authority);
        assert_eq!(identity.execution_machine, authority);
        assert_eq!(
            serde_json::to_value(identity.current_spec().unwrap()).unwrap(),
            serde_json::to_value(&spec).unwrap()
        );
        assert_eq!(identity.state, ProcessStatus::Queued);

        let event_json: String = store
            .conn
            .query_row(
                "SELECT event_json FROM executor_outbox WHERE task_id=?1 AND seq=1",
                [row.id.to_string()],
                |entry| entry.get(0),
            )
            .unwrap();
        let event: TaskEvent = serde_json::from_str(&event_json).unwrap();
        assert_eq!(event.task, row.id);
        assert_eq!(event.origin_machine, authority);
        assert_eq!(event.execution_machine, authority);
        assert_eq!(
            event.payload,
            EventPayload::State {
                status: ProcessStatus::Queued
            }
        );
        assert_eq!(
            watcher_acceptance_counts(&store, intent.request_id, row.id),
            [1, 1, 1, 1]
        );

        let snapshot = store
            .resource_snapshots_for_authority(authority)
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.resource.id == resource.id)
            .unwrap();
        assert!(matches!(
            snapshot.loan.unwrap().state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease {
                    watcher_intent: Some(saved),
                    ..
                }
            } if saved.complete() == Some(&intent)
        ));
    }

    #[test]
    fn release_watcher_acceptance_exact_retry_survives_reopen_and_old_bind_retry() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("db");
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, intent, row, callback, spec) = {
            let mut store = Store::open(&database).unwrap();
            let (resource, _, notice) =
                open_release_for_test(&mut store, authority, background_task);
            let intent =
                release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
            let spec = watcher_spec(&resource, &intent);
            prepare_release_checkpoint_baseline(&mut store, authority, &resource, &intent);
            let (row, callback) = watcher_task_and_callback(&intent, &spec);
            assert!(matches!(
                accept_watcher(
                    &mut store,
                    &resource,
                    intent.clone(),
                    &row,
                    &spec,
                    &callback
                )
                .unwrap(),
                ReleaseWatcherAcceptance::Inserted { .. }
            ));
            (resource, intent, row, callback, spec)
        };

        let mut reopened = Store::open(&database).unwrap();
        let before = watcher_acceptance_counts(&reopened, intent.request_id, row.id);
        let retry = accept_watcher(
            &mut reopened,
            &resource,
            intent.clone(),
            &row,
            &spec,
            &callback,
        )
        .unwrap();
        assert_eq!(
            retry,
            ReleaseWatcherAcceptance::Existing {
                task: row.id,
                state: TaskState::Queued
            }
        );
        assert_eq!(
            watcher_acceptance_counts(&reopened, intent.request_id, row.id),
            before
        );
        assert_eq!(before, [1, 1, 1, 1]);
        assert_eq!(
            reopened
                .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
                .unwrap(),
            intent
        );
    }

    #[test]
    fn release_watcher_acceptance_rejects_changed_content_and_owners() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        let spec = watcher_spec(&resource, &intent);
        prepare_release_checkpoint_baseline(&mut store, authority, &resource, &intent);
        let (row, callback) = watcher_task_and_callback(&intent, &spec);
        accept_watcher(
            &mut store,
            &resource,
            intent.clone(),
            &row,
            &spec,
            &callback,
        )
        .unwrap();

        let different_action = ReleaseWatcherIntent {
            action_id: ActionId::new(),
            ..intent.clone()
        };
        let different_request = ReleaseWatcherIntent {
            request_id: RequestId::new(),
            ..intent.clone()
        };
        let different_task = ReleaseWatcherIntent {
            watcher_task_id: ReleaseWatcherTaskId::new(TaskId::new()),
            ..intent.clone()
        };
        for changed in [different_action, different_request, different_task] {
            assert!(
                accept_watcher(&mut store, &resource, changed, &row, &spec, &callback).is_err()
            );
        }

        let mut changed_spec_value = serde_json::to_value(&spec).unwrap();
        changed_spec_value["workload"]["command"][1] = json!("changed");
        let changed_spec: NormalizedSpec = serde_json::from_value(changed_spec_value).unwrap();
        assert!(
            accept_watcher(
                &mut store,
                &resource,
                intent.clone(),
                &row,
                &changed_spec,
                &callback,
            )
            .is_err()
        );
        let changed_digest_intent = ReleaseWatcherIntent {
            normalized_spec_sha256: crate::submission::normalized_spec_sha256(&changed_spec)
                .unwrap(),
            ..intent.clone()
        };
        let changed_row = remote_task(row.id, &changed_spec);
        let changed_callback = CallbackContext {
            env: changed_row.env.clone(),
            cwd: changed_row.cwd.clone(),
            codex: crate::submission::CallbackExecutable::available(PathBuf::from("/bin/echo")),
        };
        assert!(
            accept_watcher(
                &mut store,
                &resource,
                changed_digest_intent,
                &changed_row,
                &changed_spec,
                &changed_callback,
            )
            .is_err()
        );

        let mut changed_callback = callback.clone();
        changed_callback.codex =
            crate::submission::CallbackExecutable::available(PathBuf::from("/bin/sh"));
        assert!(
            accept_watcher(
                &mut store,
                &resource,
                intent.clone(),
                &row,
                &spec,
                &changed_callback,
            )
            .is_err()
        );
        let mut wrong_owner_input =
            watcher_acceptance_input(&resource, intent.clone(), &row, &spec, &callback);
        wrong_owner_input.authority_machine = MachineId::new();
        assert!(
            store
                .accept_release_watcher_for_authority(wrong_owner_input)
                .is_err()
        );
        let changed_supervisor = SupervisorAddress {
            machine: authority,
            thread: ThreadId(Uuid::now_v7()),
        };
        let mut changed_supervisor_resource = resource.clone();
        changed_supervisor_resource.supervisor = changed_supervisor;
        assert!(
            accept_watcher(
                &mut store,
                &changed_supervisor_resource,
                intent,
                &row,
                &spec,
                &callback,
            )
            .is_err()
        );
    }

    #[test]
    fn remote_release_watcher_supervisor_is_typed_and_writes_no_task_records() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let remote_supervisor = MachineId::new();
        let background_task = TaskId::new();
        let spec = spec();
        let mut resource = resource(authority);
        resource.supervisor = SupervisorAddress {
            machine: remote_supervisor,
            thread: spec.thread,
        };
        resource.registered_background_task = Some(background_task);
        store.register_resource(authority, &resource).unwrap();
        store
            .insert_task(&remote_task(background_task, &spec))
            .unwrap();
        store
            .cas_status(
                background_task,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                MachineId::new(),
                spec.clone(),
            )
            .unwrap();
        let OpenReleaseLoanResult::Opened { notice, .. } = store
            .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
            .unwrap()
        else {
            panic!("fixture must create a new release action");
        };
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        let spec = watcher_spec(&resource, &intent);
        let (row, callback) = watcher_task_and_callback(&intent, &spec);

        let result = accept_watcher(
            &mut store,
            &resource,
            intent.clone(),
            &row,
            &spec,
            &callback,
        )
        .unwrap();
        assert_eq!(
            result,
            ReleaseWatcherAcceptance::UnsupportedRemoteSupervisor {
                authority_machine: authority,
                supervisor: resource.supervisor,
            }
        );
        assert_eq!(
            watcher_acceptance_counts(&store, intent.request_id, row.id),
            [0, 0, 0, 0]
        );
        let snapshot = store
            .resource_snapshots_for_authority(authority)
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.resource.id == resource.id)
            .unwrap();
        assert!(matches!(
            snapshot.loan.unwrap().state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease {
                    watcher_intent: None,
                    ..
                }
            }
        ));
    }

    #[test]
    fn release_watcher_acceptance_rolls_back_binding_and_task_records_on_event_failure() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        let spec = watcher_spec(&resource, &intent);
        prepare_release_checkpoint_baseline(&mut store, authority, &resource, &intent);
        let (row, callback) = watcher_task_and_callback(&intent, &spec);
        store
            .conn
            .execute_batch(&format!(
                "CREATE TRIGGER fail_release_watcher_event
                 BEFORE INSERT ON executor_outbox
                 WHEN NEW.task_id='{}'
                 BEGIN SELECT RAISE(ABORT, 'test event failure'); END;",
                row.id
            ))
            .unwrap();

        assert!(
            accept_watcher(
                &mut store,
                &resource,
                intent.clone(),
                &row,
                &spec,
                &callback
            )
            .is_err()
        );
        assert_eq!(
            watcher_acceptance_counts(&store, intent.request_id, row.id),
            [0, 0, 0, 0]
        );
        let snapshot = store
            .resource_snapshots_for_authority(authority)
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.resource.id == resource.id)
            .unwrap();
        assert!(matches!(
            snapshot.loan.unwrap().state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease {
                    watcher_intent: Some(saved),
                    ..
                }
            } if saved.complete() == Some(&intent)
        ));
    }

    fn prevention_count(store: &Store) -> i64 {
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM resource_request_preventions",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn identity_count(store: &Store, task: TaskId) -> i64 {
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_identities WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn assert_cancelled_tombstone(
        identity: ExecutorIdentity,
        task: TaskId,
        origin: MachineId,
        authority: MachineId,
    ) {
        let ExecutorIdentity::Rejected(tombstone) = identity else {
            panic!("pre-activation cancellation must retain a rejection");
        };
        assert_eq!(tombstone.task, task);
        assert_eq!(tombstone.origin_machine, origin);
        assert_eq!(tombstone.execution_machine, authority);
        assert_eq!(tombstone.reason, PreAcceptanceRejection::Cancelled.as_str());
    }

    #[test]
    fn release_notice_facade_operations_share_the_store_connection() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("db");
        let mut store = Store::open(&database).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let mut resource = resource(authority);
        resource.registered_background_task = Some(background_task);

        store.register_resource(authority, &resource).unwrap();
        store
            .insert_task(&remote_task(background_task, &spec()))
            .unwrap();
        assert!(
            store
                .cas_status(
                    background_task,
                    ProcessStatus::Queued,
                    ProcessStatus::Running,
                )
                .unwrap()
                .is_some()
        );
        store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                MachineId::new(),
                spec(),
            )
            .unwrap();

        let OpenReleaseLoanResult::Opened { notice, .. } = store
            .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
            .unwrap()
        else {
            panic!("first facade call must open a release loan");
        };
        assert_eq!(
            store.supervisor_notice(notice.id).unwrap(),
            Some(notice.clone())
        );
        assert_eq!(
            store.pending_supervisor_notices().unwrap(),
            vec![notice.clone()]
        );

        let destination = SupervisorAddress {
            machine: MachineId::new(),
            thread: ThreadId(Uuid::now_v7()),
        };
        let retargeted = store
            .retarget_supervisor_notice(
                notice.id,
                notice.assignment_revision,
                destination,
                AssignmentRevision::new(1),
            )
            .unwrap();
        assert_eq!(retargeted.destination, destination);

        let first_attempt = DeliveryAttemptId::new();
        assert_eq!(
            store
                .reserve_supervisor_notice_attempt(notice.id, first_attempt)
                .unwrap()
                .id,
            notice.id
        );
        assert_eq!(
            store
                .recover_sending_supervisor_notices()
                .unwrap()
                .iter()
                .map(|notice| notice.id)
                .collect::<Vec<_>>(),
            vec![notice.id]
        );

        let second_attempt = DeliveryAttemptId::new();
        store
            .reserve_supervisor_notice_attempt(notice.id, second_attempt)
            .unwrap();
        let settled = store
            .settle_supervisor_notice_attempt(notice.id, second_attempt, Ok(()))
            .unwrap();
        assert_eq!(settled.id, notice.id);
        assert!(matches!(
            settled.delivery,
            SupervisorNoticeDelivery::Delivered { .. }
        ));
        assert_eq!(store.supervisor_notice(notice.id).unwrap(), Some(settled));
    }

    #[test]
    fn release_watcher_binding_returns_the_saved_intent_on_exact_retry() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, loan, notice) =
            open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());

        let first = store
            .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
            .unwrap();
        let reserved_task = remote_task(intent.watcher_task_id.as_task_id(), &spec());
        assert!(matches!(
            store.insert_local_task(
                &reserved_task,
                &spec(),
                authority,
                PathBuf::from("/bin/echo"),
            ),
            Err(crate::error::AppError::ClusterTaskConflict { task })
                if task == reserved_task.id
        ));
        let retry = store
            .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
            .unwrap();

        assert_eq!(first, intent);
        assert_eq!(retry, first);
        let snapshot = store
            .resource_snapshots_for_authority(authority)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(snapshot.resource.state_revision, notice.state_revision);
        assert_eq!(
            snapshot.resource.registered_background_task,
            Some(background_task)
        );
        let saved_loan = snapshot.loan.unwrap();
        assert_eq!(saved_loan.id, loan.id);
        assert!(matches!(
            saved_loan.state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease {
                    action_id,
                    observed_background_task,
                    watcher_intent: Some(saved),
                }
            } if action_id == notice.action_id
                && observed_background_task == background_task
                && saved.complete() == Some(&intent)
        ));
    }

    #[test]
    fn release_watcher_binding_rejects_conflicting_identity_and_stale_action_data() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
            .unwrap();

        let different_watcher = ReleaseWatcherIntent {
            watcher_task_id: ReleaseWatcherTaskId::new(TaskId::new()),
            ..intent.clone()
        };
        let different_action = ReleaseWatcherIntent {
            action_id: ActionId::new(),
            ..intent.clone()
        };
        let different_background_task = ReleaseWatcherIntent {
            observed_background_task: TaskId::new(),
            ..intent.clone()
        };
        let stale_revision = ReleaseWatcherIntent {
            state_revision: ResourceRevision::new(notice.state_revision.get() - 1),
            ..intent.clone()
        };
        let different_request = ReleaseWatcherIntent {
            request_id: RequestId::new(),
            ..intent.clone()
        };
        let mut changed_spec = serde_json::to_value(spec()).unwrap();
        changed_spec["workload"]["command"][1] = json!("different");
        let different_digest = ReleaseWatcherIntent {
            normalized_spec_sha256: crate::submission::normalized_spec_sha256(
                &serde_json::from_value(changed_spec).unwrap(),
            )
            .unwrap(),
            ..intent.clone()
        };

        for conflicting in [
            different_watcher,
            different_action,
            different_background_task,
            stale_revision,
            different_request,
            different_digest,
        ] {
            assert!(matches!(
                store.bind_release_watcher_for_authority(authority, resource.id, conflicting,),
                Err(ResourceStoreError::Conflict)
            ));
        }

        let snapshot = store
            .resource_snapshots_for_authority(authority)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let saved_loan = snapshot.loan.unwrap();
        assert!(matches!(
            saved_loan.state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease {
                    watcher_intent: Some(saved),
                    ..
                }
            } if saved.complete() == Some(&intent)
        ));
    }

    #[test]
    fn release_watcher_binding_rejects_wrong_authority_and_reused_identities() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let first_background_task = TaskId::new();
        let (first_resource, _, first_notice) =
            open_release_for_test(&mut store, authority, first_background_task);
        let first_intent = release_watcher_intent(
            first_resource.id,
            &first_notice,
            first_background_task,
            TaskId::new(),
        );

        assert!(matches!(
            store.bind_release_watcher_for_authority(
                MachineId::new(),
                first_resource.id,
                first_intent.clone(),
            ),
            Err(ResourceStoreError::WrongAuthority { .. })
        ));
        store
            .bind_release_watcher_for_authority(authority, first_resource.id, first_intent.clone())
            .unwrap();

        let second_background_task = TaskId::new();
        let (second_resource, _, second_notice) =
            open_release_for_test(&mut store, authority, second_background_task);
        let same_watcher_task = ReleaseWatcherIntent {
            watcher_task_id: first_intent.watcher_task_id,
            ..release_watcher_intent(
                second_resource.id,
                &second_notice,
                second_background_task,
                TaskId::new(),
            )
        };
        let same_request = ReleaseWatcherIntent {
            request_id: first_intent.request_id,
            ..release_watcher_intent(
                second_resource.id,
                &second_notice,
                second_background_task,
                TaskId::new(),
            )
        };

        assert!(matches!(
            store.bind_release_watcher_for_authority(
                authority,
                second_resource.id,
                same_watcher_task,
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert!(matches!(
            store.bind_release_watcher_for_authority(authority, second_resource.id, same_request),
            Err(ResourceStoreError::Conflict)
        ));

        let second_watcher_task = TaskId::new();
        let same_request_as_task = ReleaseWatcherIntent {
            watcher_task_id: ReleaseWatcherTaskId::new(second_watcher_task),
            request_id: RequestId(second_watcher_task.0),
            ..release_watcher_intent(
                second_resource.id,
                &second_notice,
                second_background_task,
                TaskId::new(),
            )
        };
        assert!(matches!(
            store.bind_release_watcher_for_authority(
                authority,
                second_resource.id,
                same_request_as_task,
            ),
            Err(ResourceStoreError::Conflict)
        ));
    }

    #[test]
    fn release_watcher_binding_rejects_request_id_used_by_a_task_route() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .conn
            .execute(
                "INSERT INTO origin_routes (request_id, task_id, execution_machine, spec_json, route_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    intent.request_id.0.to_string(),
                    TaskId::new().to_string(),
                    authority.as_uuid().to_string(),
                    "{}",
                    "{}",
                ],
            )
            .unwrap();

        assert!(matches!(
            store.bind_release_watcher_for_authority(authority, resource.id, intent.clone()),
            Err(ResourceStoreError::Conflict)
        ));

        store
            .conn
            .execute(
                "DELETE FROM origin_routes WHERE request_id = ?1",
                [intent.request_id.0.to_string()],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO origin_routes (request_id, task_id, execution_machine, spec_json, route_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    RequestId::new().0.to_string(),
                    intent.watcher_task_id.as_task_id().to_string(),
                    authority.as_uuid().to_string(),
                    "{}",
                    "{}",
                ],
            )
            .unwrap();
        assert!(matches!(
            store.bind_release_watcher_for_authority(authority, resource.id, intent),
            Err(ResourceStoreError::Conflict)
        ));
    }

    #[test]
    fn partial_saved_watcher_identity_remains_readable_and_unproven() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, loan, notice) =
            open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
            .unwrap();

        let saved_state = LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id: notice.action_id,
                observed_background_task: background_task,
                watcher_intent: Some(SavedReleaseWatcherIntent::Complete(intent.clone())),
            },
        };
        let mut missing_identity = serde_json::to_value(&saved_state).unwrap();
        let watcher = missing_identity["phase"]["watcher_intent"]
            .as_object_mut()
            .unwrap();
        watcher.remove("request_id");
        watcher.remove("normalized_spec_sha256");
        store
            .conn
            .execute(
                "UPDATE loans SET state_json = ?1 WHERE id = ?2",
                rusqlite::params![missing_identity.to_string(), loan.id.as_uuid().to_string()],
            )
            .unwrap();
        assert!(matches!(
            store.bind_release_watcher_for_authority(authority, resource.id, intent.clone()),
            Err(ResourceStoreError::LegacyWatcherIntentUnproven)
        ));
        assert!(matches!(
            store.capture_release_checkpoint_baseline_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            ),
            Err(ReleaseCheckpointError::LegacyUnproven { .. })
        ));
        store.resource_snapshots_for_authority(authority).unwrap();

        let mut malformed_digest = serde_json::to_value(&saved_state).unwrap();
        malformed_digest["phase"]["watcher_intent"]["normalized_spec_sha256"] =
            json!("not-a-sha256");
        store
            .conn
            .execute(
                "UPDATE loans SET state_json = ?1 WHERE id = ?2",
                rusqlite::params![malformed_digest.to_string(), loan.id.as_uuid().to_string()],
            )
            .unwrap();
        store.resource_snapshots_for_authority(authority).unwrap();
    }

    #[test]
    fn release_watcher_binding_survives_store_reopen_without_changing_release_state() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("db");
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, notice, intent) = {
            let mut store = Store::open(&database).unwrap();
            let (resource, _, notice) =
                open_release_for_test(&mut store, authority, background_task);
            let intent =
                release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
            store
                .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
                .unwrap();
            (resource, notice, intent)
        };

        let mut reopened = Store::open(&database).unwrap();
        let snapshot = reopened
            .resource_snapshots_for_authority(authority)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(snapshot.resource.state_revision, notice.state_revision);
        assert_eq!(
            snapshot.resource.registered_background_task,
            Some(background_task)
        );
        let saved_loan = snapshot.loan.unwrap();
        assert!(matches!(
            saved_loan.state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease {
                    action_id,
                    observed_background_task,
                    watcher_intent: Some(saved),
                }
            } if action_id == notice.action_id
                && observed_background_task == background_task
                && saved.complete() == Some(&intent)
        ));
        assert_eq!(
            reopened
                .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
                .unwrap(),
            intent
        );
    }

    #[test]
    fn release_checkpoint_baseline_and_stop_decision_survive_reopen() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("db");
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, notice, intent, association, baseline, decision) = {
            let mut store = Store::open(&database).unwrap();
            let (resource, _, notice) =
                open_release_for_test(&mut store, authority, background_task);
            let intent =
                release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
            store
                .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
                .unwrap();
            let association = saved_release_association(&store, resource.id, background_task);
            let baseline = store
                .capture_release_checkpoint_baseline_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                )
                .unwrap();
            let runtime_root = association
                .verified_attempt()
                .canonical_runtime_root()
                .to_path_buf();
            let attempt_binding = association.verified_attempt().binding().clone();
            crate::resource::watcher::tests::write_generation_for_test(
                &runtime_root,
                &attempt_binding,
                "generation-a",
                80,
            );
            assert_eq!(
                store
                    .capture_release_checkpoint_baseline_for_authority(
                        authority,
                        resource.id,
                        notice.action_id,
                        notice.state_revision,
                    )
                    .unwrap(),
                baseline
            );
            let ReleaseCheckpointStopOutcome::Reserved(decision) = store
                .reserve_release_checkpoint_stop_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                )
                .unwrap()
            else {
                panic!("a new exact checkpoint must reserve the stop decision");
            };
            (resource, notice, intent, association, baseline, decision)
        };

        let mut reopened = Store::open(&database).unwrap();
        assert_eq!(
            reopened
                .capture_release_checkpoint_baseline_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                )
                .unwrap(),
            baseline
        );
        assert_eq!(
            reopened
                .reserve_release_checkpoint_stop_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                )
                .unwrap(),
            ReleaseCheckpointStopOutcome::AlreadyReserved(decision.clone())
        );
        assert_eq!(decision.binding.watcher_intent, intent);
        assert_eq!(
            decision.binding.association,
            TrainerAttemptAssociationProof::from(&association)
        );
        assert_eq!(decision.selected_checkpoint.generation_id, "generation-a");
        assert_eq!(decision.selected_checkpoint.record_sha256.len(), 64);
        assert_eq!(decision.selected_checkpoint.inventory_sha256.len(), 64);
    }

    #[test]
    fn pre_baseline_checkpoint_does_not_reserve_stop() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let association = saved_release_association(&store, resource.id, background_task);
        let runtime_root = association
            .verified_attempt()
            .canonical_runtime_root()
            .to_path_buf();
        let attempt_binding = association.verified_attempt().binding().clone();
        crate::resource::watcher::tests::write_generation_for_test(
            &runtime_root,
            &attempt_binding,
            "generation-before-baseline",
            80,
        );
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent)
            .unwrap();
        let baseline = store
            .capture_release_checkpoint_baseline_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap();

        assert!(
            baseline
                .snapshot
                .contains_generation("generation-before-baseline")
        );
        assert_eq!(
            store
                .reserve_release_checkpoint_stop_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                )
                .unwrap(),
            ReleaseCheckpointStopOutcome::WaitingForCheckpoint
        );
    }

    #[test]
    fn foreign_attempt_checkpoint_does_not_reserve_stop() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let association = saved_release_association(&store, resource.id, background_task);
        let runtime_root = association
            .verified_attempt()
            .canonical_runtime_root()
            .to_path_buf();
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent)
            .unwrap();
        store
            .capture_release_checkpoint_baseline_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap();
        let foreign_attempt = AttemptBinding {
            campaign_id: "foreign-campaign".into(),
            campaign_revision_id: "foreign-revision".into(),
            task_id: "foreign-trainer-task".into(),
            attempt_id: "foreign-attempt".into(),
            attempt_number: 1,
            ownership_token: "foreign-owner".into(),
        };
        crate::resource::watcher::tests::write_generation_for_test(
            &runtime_root,
            &foreign_attempt,
            "generation-foreign",
            81,
        );

        assert_eq!(
            store
                .reserve_release_checkpoint_stop_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                )
                .unwrap(),
            ReleaseCheckpointStopOutcome::WaitingForCheckpoint
        );
    }

    #[test]
    fn new_exact_checkpoint_reserves_one_stop_decision() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let association = saved_release_association(&store, resource.id, background_task);
        let runtime_root = association
            .verified_attempt()
            .canonical_runtime_root()
            .to_path_buf();
        let attempt_binding = association.verified_attempt().binding().clone();
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
            .unwrap();
        let baseline = store
            .capture_release_checkpoint_baseline_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap();
        crate::resource::watcher::tests::write_generation_for_test(
            &runtime_root,
            &attempt_binding,
            "generation-new-exact",
            81,
        );

        let ReleaseCheckpointStopOutcome::Reserved(decision) = store
            .reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap()
        else {
            panic!("the complete matching checkpoint must reserve a stop decision");
        };
        assert_eq!(decision.binding.watcher_intent, intent);
        assert_eq!(
            decision.selected_checkpoint.generation_id,
            "generation-new-exact"
        );
        assert!(
            !baseline
                .snapshot
                .contains_generation(&decision.selected_checkpoint.generation_id)
        );
        assert_eq!(
            store
                .revalidate_release_checkpoint_stop_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                )
                .unwrap(),
            decision
        );
    }

    #[test]
    fn duplicate_stop_decision_reuses_checkpoint_and_changed_action_conflicts() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let association = saved_release_association(&store, resource.id, background_task);
        let runtime_root = association
            .verified_attempt()
            .canonical_runtime_root()
            .to_path_buf();
        let attempt_binding = association.verified_attempt().binding().clone();
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent)
            .unwrap();
        store
            .capture_release_checkpoint_baseline_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap();
        crate::resource::watcher::tests::write_generation_for_test(
            &runtime_root,
            &attempt_binding,
            "generation-selected",
            81,
        );
        let ReleaseCheckpointStopOutcome::Reserved(first) = store
            .reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap()
        else {
            panic!("the first checkpoint must reserve the stop decision");
        };
        crate::resource::watcher::tests::write_generation_for_test(
            &runtime_root,
            &attempt_binding,
            "generation-later",
            82,
        );

        assert_eq!(
            store
                .reserve_release_checkpoint_stop_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                )
                .unwrap(),
            ReleaseCheckpointStopOutcome::AlreadyReserved(first.clone())
        );
        assert_eq!(
            first.selected_checkpoint.generation_id,
            "generation-selected"
        );
        assert!(matches!(
            store.reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                ActionId::new(),
                notice.state_revision,
            ),
            Err(ReleaseCheckpointError::Conflict)
        ));
    }

    #[test]
    fn changed_selected_checkpoint_fails_revalidation() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let association = saved_release_association(&store, resource.id, background_task);
        let runtime_root = association
            .verified_attempt()
            .canonical_runtime_root()
            .to_path_buf();
        let attempt_binding = association.verified_attempt().binding().clone();
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent)
            .unwrap();
        store
            .capture_release_checkpoint_baseline_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap();
        let path = crate::resource::watcher::tests::write_generation_for_test(
            &runtime_root,
            &attempt_binding,
            "generation-selected",
            81,
        );
        let ReleaseCheckpointStopOutcome::Reserved(_) = store
            .reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap()
        else {
            panic!("the exact checkpoint must reserve the stop decision");
        };
        fs::write(path.join("checkpoint.bin"), b"changed checkpoint").unwrap();

        assert!(matches!(
            store.revalidate_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            ),
            Err(ReleaseCheckpointError::SelectedCheckpointChanged { action_id })
                if action_id == notice.action_id
        ));
    }

    #[test]
    fn checkpoint_cancellation_commits_decision_and_exact_trainer_marker_atomically() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, notice, intent, decision) =
            checkpoint_decision_for_cancellation(&mut store, authority, background_task);
        start_release_watcher_for_test(&mut store, &resource, &intent);

        let ReleaseCheckpointCancellationOutcome::Committed(result) = store
            .commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &decision,
            )
            .unwrap()
        else {
            panic!("the accepted watcher must permit the exact stop decision");
        };
        assert_eq!(result.decision, decision);
        assert_eq!(result.cancellation.task_id, background_task);
        assert_eq!(
            store
                .require_task(background_task)
                .unwrap()
                .cancel_requested_at,
            Some(result.cancellation.cancel_requested_at)
        );
        let (state, _) =
            release_checkpoint_state_for_action(&store.conn, resource.id, notice.action_id)
                .unwrap()
                .unwrap();
        assert!(matches!(
            state.phase,
            ReleaseCheckpointPhase::CancellationCommitted {
                decision: saved,
                cancellation,
                ..
            } if *saved == decision && cancellation == result.cancellation
        ));
    }

    #[test]
    fn checkpoint_cancellation_rolls_back_task_marker_when_phase_update_fails() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, notice, intent, decision) =
            checkpoint_decision_for_cancellation(&mut store, authority, background_task);
        start_release_watcher_for_test(&mut store, &resource, &intent);
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER reject_checkpoint_cancellation
                 BEFORE UPDATE ON resource_release_checkpoint_states
                 WHEN json_extract(NEW.state_json, '$.phase.type') = 'cancellation_committed'
                 BEGIN SELECT RAISE(ABORT, 'forced cancellation phase failure'); END;",
            )
            .unwrap();

        assert!(matches!(
            store.commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &decision,
            ),
            Err(ReleaseCheckpointError::Storage(_))
        ));
        assert_eq!(
            store
                .require_task(background_task)
                .unwrap()
                .cancel_requested_at,
            None
        );
        let (state, _) =
            release_checkpoint_state_for_action(&store.conn, resource.id, notice.action_id)
                .unwrap()
                .unwrap();
        assert!(matches!(
            state.phase,
            ReleaseCheckpointPhase::StopReserved { decision: saved, .. }
                if *saved == decision
        ));
    }

    #[test]
    fn exact_checkpoint_cancellation_retry_reuses_the_committed_reservation() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, notice, intent, decision) =
            checkpoint_decision_for_cancellation(&mut store, authority, background_task);
        start_release_watcher_for_test(&mut store, &resource, &intent);
        let first = store
            .commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &decision,
            )
            .unwrap();
        let ReleaseCheckpointCancellationOutcome::Committed(first_result) = first else {
            panic!("the first cancellation must commit");
        };
        crate::resource::watcher::tests::write_generation_for_test(
            &decision.binding.association.canonical_runtime_root,
            &decision.binding.attempt_binding,
            "generation-later",
            82,
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(matches!(
            store.request_cancel(background_task).unwrap(),
            crate::store::CancelResult::SignalWorker(_)
        ));

        assert_eq!(
            store
                .commit_release_checkpoint_cancellation_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                    &decision,
                )
                .unwrap(),
            ReleaseCheckpointCancellationOutcome::AlreadyCommitted(first_result.clone())
        );
        assert_eq!(
            store
                .reserve_release_checkpoint_stop_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                )
                .unwrap(),
            ReleaseCheckpointStopOutcome::AlreadyReserved(decision)
        );
        assert!(
            store
                .require_task(background_task)
                .unwrap()
                .cancel_requested_at
                .is_some()
        );
    }

    #[test]
    fn checkpoint_cancellation_rejects_wrong_task_and_action_decisions() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, notice, intent, decision) =
            checkpoint_decision_for_cancellation(&mut store, authority, background_task);
        start_release_watcher_for_test(&mut store, &resource, &intent);

        let mut wrong_task = decision.clone();
        wrong_task.binding.action.observed_background_task = TaskId::new();
        assert!(matches!(
            store.commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &wrong_task,
            ),
            Err(ReleaseCheckpointError::StopDecisionMismatch { action_id })
                if action_id == notice.action_id
        ));

        let mut wrong_action = decision.clone();
        wrong_action.binding.action.action_id = ActionId::new();
        assert!(matches!(
            store.commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &wrong_action,
            ),
            Err(ReleaseCheckpointError::StopDecisionMismatch { action_id })
                if action_id == notice.action_id
        ));
        assert_eq!(
            store
                .require_task(background_task)
                .unwrap()
                .cancel_requested_at,
            None
        );
    }

    #[test]
    fn checkpoint_cancellation_waits_for_the_accepted_watcher_to_run() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, notice, intent, decision) =
            checkpoint_decision_for_cancellation(&mut store, authority, background_task);

        assert_eq!(
            store
                .commit_release_checkpoint_cancellation_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                    &decision,
                )
                .unwrap(),
            ReleaseCheckpointCancellationOutcome::WatcherNotReady {
                watcher_task_id: intent.watcher_task_id.as_task_id(),
            }
        );
        let watcher_spec = watcher_spec(&resource, &intent);
        let (watcher_row, callback) = watcher_task_and_callback(&intent, &watcher_spec);
        accept_watcher(
            &mut store,
            &resource,
            intent.clone(),
            &watcher_row,
            &watcher_spec,
            &callback,
        )
        .unwrap();
        assert_eq!(
            store
                .commit_release_checkpoint_cancellation_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                    &decision,
                )
                .unwrap(),
            ReleaseCheckpointCancellationOutcome::WatcherNotReady {
                watcher_task_id: intent.watcher_task_id.as_task_id(),
            }
        );
        assert_eq!(
            store
                .require_task(background_task)
                .unwrap()
                .cancel_requested_at,
            None
        );
    }

    #[test]
    fn checkpoint_cancellation_rejects_changed_checkpoint_and_prior_generic_marker() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let first_task = TaskId::new();
        let (resource, notice, intent, decision) =
            checkpoint_decision_for_cancellation(&mut store, authority, first_task);
        start_release_watcher_for_test(&mut store, &resource, &intent);
        fs::write(
            decision.selected_checkpoint.path.join("checkpoint.bin"),
            b"changed checkpoint",
        )
        .unwrap();

        assert!(matches!(
            store.commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &decision,
            ),
            Err(ReleaseCheckpointError::SelectedCheckpointChanged { action_id })
                if action_id == notice.action_id
        ));
        assert_eq!(
            store.require_task(first_task).unwrap().cancel_requested_at,
            None
        );

        let second_task = TaskId::new();
        let (resource, notice, intent, decision) =
            checkpoint_decision_for_cancellation(&mut store, authority, second_task);
        start_release_watcher_for_test(&mut store, &resource, &intent);
        assert!(matches!(
            store.request_cancel(second_task).unwrap(),
            crate::store::CancelResult::SignalWorker(_)
        ));
        let generic_marker = store.require_task(second_task).unwrap().cancel_requested_at;

        assert!(matches!(
            store.commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &decision,
            ),
            Err(ReleaseCheckpointError::TrainerCancellationConflict { task_id })
                if task_id == second_task
        ));
        assert_eq!(
            store.require_task(second_task).unwrap().cancel_requested_at,
            generic_marker
        );
    }

    #[test]
    fn checkpoint_cancellation_rejects_missing_or_terminal_trainers() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let missing_task = TaskId::new();
        let (resource, notice, intent, decision) =
            checkpoint_decision_for_cancellation(&mut store, authority, missing_task);
        start_release_watcher_for_test(&mut store, &resource, &intent);
        store
            .conn
            .pragma_update(None, "foreign_keys", "OFF")
            .unwrap();
        store
            .conn
            .execute("DELETE FROM tasks WHERE id=?1", [missing_task.to_string()])
            .unwrap();
        assert!(matches!(
            store.commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &decision,
            ),
            Err(ReleaseCheckpointError::TrainerAssociation(
                TrainerAttemptAssociationStoreError::TaskMissing { task_id }
            )) if task_id == missing_task
        ));

        let terminal_task = TaskId::new();
        let (resource, notice, intent, decision) =
            checkpoint_decision_for_cancellation(&mut store, authority, terminal_task);
        start_release_watcher_for_test(&mut store, &resource, &intent);
        store
            .cas_exit(
                terminal_task,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
            )
            .unwrap()
            .unwrap();
        store
            .update_execution_state(terminal_task, ProcessStatus::Succeeded)
            .unwrap();

        assert!(matches!(
            store.commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &decision,
            ),
            Err(ReleaseCheckpointError::TrainerTaskNotRunning {
                task_id,
                state: ProcessStatus::Succeeded,
            }) if task_id == terminal_task
        ));
        assert_eq!(
            store
                .require_task(terminal_task)
                .unwrap()
                .cancel_requested_at,
            None
        );
    }

    #[test]
    fn legacy_awaiting_release_and_old_watcher_intents_remain_unproven() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let first_task = TaskId::new();
        let (first_resource, _, first_notice) =
            open_release_for_test(&mut store, authority, first_task);
        let old_intent =
            release_watcher_intent(first_resource.id, &first_notice, first_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, first_resource.id, old_intent.clone())
            .unwrap();
        store
            .conn
            .execute(
                "DELETE FROM resource_release_checkpoint_states WHERE action_id=?1",
                [first_notice.action_id.as_uuid().to_string()],
            )
            .unwrap();
        assert!(matches!(
            store.capture_release_checkpoint_baseline_for_authority(
                authority,
                first_resource.id,
                first_notice.action_id,
                first_notice.state_revision,
            ),
            Err(ReleaseCheckpointError::LegacyUnproven { .. })
        ));
        let old_spec = watcher_spec(&first_resource, &old_intent);
        let (old_row, old_callback) = watcher_task_and_callback(&old_intent, &old_spec);
        assert!(matches!(
            accept_watcher(
                &mut store,
                &first_resource,
                old_intent.clone(),
                &old_row,
                &old_spec,
                &old_callback,
            ),
            Err(ReleaseWatcherAcceptanceError::Checkpoint(
                ReleaseCheckpointError::LegacyUnproven { .. }
            ))
        ));
        assert_eq!(
            watcher_acceptance_counts(&store, old_intent.request_id, old_row.id),
            [0, 0, 0, 0]
        );

        let second_task = TaskId::new();
        let (second_resource, _, second_notice) =
            open_release_for_test(&mut store, authority, second_task);
        store
            .conn
            .execute(
                "DELETE FROM resource_release_checkpoint_states WHERE action_id=?1",
                [second_notice.action_id.as_uuid().to_string()],
            )
            .unwrap();
        assert!(matches!(
            store.capture_release_checkpoint_baseline_for_authority(
                authority,
                second_resource.id,
                second_notice.action_id,
                second_notice.state_revision,
            ),
            Err(ReleaseCheckpointError::LegacyUnproven { .. })
        ));
    }

    #[test]
    fn failed_baseline_persistence_keeps_the_action_unbound_to_a_new_snapshot() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent)
            .unwrap();
        store
            .conn
            .execute_batch(&format!(
                "CREATE TRIGGER fail_release_checkpoint_update
                 BEFORE UPDATE ON resource_release_checkpoint_states
                 WHEN OLD.action_id='{}'
                 BEGIN SELECT RAISE(ABORT, 'test checkpoint persistence failure'); END;",
                notice.action_id.as_uuid()
            ))
            .unwrap();

        assert!(matches!(
            store.capture_release_checkpoint_baseline_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            ),
            Err(ReleaseCheckpointError::Storage(_))
        ));
        let (state, _) =
            release_checkpoint_state_for_action(&store.conn, resource.id, notice.action_id)
                .unwrap()
                .unwrap();
        assert_eq!(state.phase, ReleaseCheckpointPhase::WatcherBindingPending);
    }

    fn release_completion_fixture() -> (
        TrainerAssociationFixture,
        crate::resource::watcher::AttemptBinding,
        RequestId,
        ActionId,
        ResourceRevision,
        LoanId,
    ) {
        release_completion_fixture_with(TrainerAssociationFixture::new(), spec(), MachineId::new())
    }

    fn release_completion_fixture_with(
        mut fixture: TrainerAssociationFixture,
        request_spec: NormalizedSpec,
        request_origin: MachineId,
    ) -> (
        TrainerAssociationFixture,
        crate::resource::watcher::AttemptBinding,
        RequestId,
        ActionId,
        ResourceRevision,
        LoanId,
    ) {
        fixture.insert_accepted_running_task();
        let binding = fixture.register_release_attempt();
        let request_id = RequestId::new();
        fixture
            .store
            .accept_resource_request(
                fixture.authority,
                request_id,
                TaskId::new(),
                fixture.resource.id,
                request_origin,
                request_spec,
            )
            .unwrap();
        let OpenReleaseLoanResult::Opened { loan, notice } = fixture
            .store
            .open_release_loan_for_authority(
                fixture.authority,
                fixture.resource.id,
                fixture.resource.state_revision,
            )
            .unwrap()
        else {
            panic!("fixture must open one release action");
        };

        (
            fixture,
            binding,
            request_id,
            notice.action_id,
            notice.state_revision,
            loan.id,
        )
    }

    fn publish_completed_result(
        fixture: &mut TrainerAssociationFixture,
        binding: &crate::resource::watcher::AttemptBinding,
        exit_evidence: ProcessGroupExitEvidence,
    ) {
        crate::resource::watcher::tests::write_completed_result_for_test(
            &fixture.runtime_root,
            binding,
        );
        fixture.finish_registered_task_with_evidence(exit_evidence);
    }

    fn commit_stopped_release_decision(
        fixture: &mut TrainerAssociationFixture,
        action_id: ActionId,
        revision: ResourceRevision,
    ) -> (ReleaseCheckpointStopDecision, ReleaseCheckpointCancellation) {
        let notice_json: String = fixture
            .store
            .conn
            .query_row(
                "SELECT notice_json FROM resource_supervisor_notices WHERE action_id = ?1",
                [action_id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        let notice: SupervisorNotice = serde_json::from_str(&notice_json).unwrap();
        let intent =
            release_watcher_intent(fixture.resource.id, &notice, fixture.task_id, TaskId::new());
        prepare_release_checkpoint_baseline(
            &mut fixture.store,
            fixture.authority,
            &fixture.resource,
            &intent,
        );
        crate::resource::watcher::tests::write_generation_for_test(
            &fixture.runtime_root,
            &trainer_attempt_test_support::attempt_binding("attempt-1"),
            "generation-stopped",
            81,
        );
        let ReleaseCheckpointStopOutcome::Reserved(decision) = fixture
            .store
            .reserve_release_checkpoint_stop_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap()
        else {
            panic!("the saved checkpoint must reserve a stopped release decision");
        };
        start_release_watcher_for_test(&mut fixture.store, &fixture.resource, &intent);
        let ReleaseCheckpointCancellationOutcome::Committed(cancellation) = fixture
            .store
            .commit_release_checkpoint_cancellation_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
                &decision,
            )
            .unwrap()
        else {
            panic!("the running watcher must permit the saved stop decision");
        };

        (decision, cancellation.cancellation)
    }

    fn fake_resource_task_spec(root: &Path, marker: &Path) -> NormalizedSpec {
        let command = root.join("fake-resource-command");
        fs::write(
            &command,
            format!("#!/bin/sh\nprintf x >> '{}'\n", marker.display()),
        )
        .unwrap();
        fs::set_permissions(&command, fs::Permissions::from_mode(0o755)).unwrap();

        serde_json::from_value(json!({
            "api_version": 1,
            "thread": Uuid::now_v7(),
            "name": "fake resource command",
            "cwd": root,
            "timeout": "4h",
            "workload": { "type": "task", "command": [command] }
        }))
        .unwrap()
    }

    async fn wait_for_terminal_task(
        store: &ractor::ActorRef<StoreMsg>,
        task_id: TaskId,
    ) -> crate::domain::TaskRow {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                if let Some(row) = call(store, |reply| StoreMsg::GetTask { id: task_id, reply })
                    .await
                    .unwrap()
                    && row.status().is_terminal()
                {
                    return row;
                }

                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("resource task must reach a terminal state")
    }

    async fn stop_test_supervisor(
        supervisor: ractor::ActorRef<SupervisorMsg>,
        handle: ractor::concurrency::JoinHandle<()>,
    ) {
        supervisor.stop(None);
        let _ = handle.await;
    }

    async fn stop_test_resource_actor(
        actor: ractor::ActorRef<ResourceMsg>,
        actor_handle: ractor::concurrency::JoinHandle<()>,
        store: ractor::ActorRef<StoreMsg>,
        store_handle: ractor::concurrency::JoinHandle<()>,
    ) {
        actor.stop(None);
        let _ = actor_handle.await;
        store.stop(None);
        let _ = store_handle.await;
    }

    #[tokio::test]
    async fn startup_retries_completed_trainer_proof_and_launches_assigned_task_once() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
        let marker = fixture.home.join("activation-count");
        let request_spec = fake_resource_task_spec(&fixture.home, &marker);
        let (mut fixture, binding, request_id, action_id, _, loan_id) =
            release_completion_fixture_with(fixture, request_spec, machine_other_than(authority));
        publish_completed_result(
            &mut fixture,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let publication = find_completed_result(&fixture.runtime_root, &binding)
            .unwrap()
            .unwrap();
        let request = fixture
            .store
            .resource_requests(authority, fixture.resource.id)
            .unwrap()
            .into_iter()
            .find(|request| request.request_id == request_id)
            .unwrap();
        assert!(matches!(request.state, ResourceRequestState::Queued));
        let resource_id = fixture.resource.id;
        let trainer_task_id = fixture.task_id;
        drop(fixture.store);

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let row = wait_for_terminal_task(&store, request.task_id).await;
        assert_eq!(row.status(), ProcessStatus::Succeeded);

        for _ in 0..3 {
            call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
                id: resource_id,
                reply,
            })
            .await
            .unwrap();
        }
        let bytes = fs::read(&marker).unwrap();
        assert_eq!(bytes, b"x");
        let inspection = call(&supervisor, |reply| SupervisorMsg::InspectResource {
            id: resource_id,
            reply,
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            inspection.loan.as_ref().map(|loan| &loan.state),
            Some(LoanState::Active {
                phase: LoanPhase::Serving {
                    release_provenance:
                        ServingReleaseProvenance::CompletedTrainerResult {
                            action_id: saved_action,
                            task_id: saved_task,
                            publication_sha256,
                        },
                    ..
                }
            }) if *saved_action == action_id
                && *saved_task == trainer_task_id
                && *publication_sha256 == publication.publication_sha256
        ));
        assert!(matches!(
            inspection.loan.map(|loan| loan.id),
            Some(saved_loan) if saved_loan == loan_id
        ));

        stop_test_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn exact_trainer_terminal_event_reconciles_without_a_client_wake() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
        let marker = fixture.home.join("activation-count");
        let request_spec = fake_resource_task_spec(&fixture.home, &marker);
        let (fixture, binding, request_id, action_id, _, loan_id) =
            release_completion_fixture_with(fixture, request_spec, machine_other_than(authority));
        let request = fixture
            .store
            .resource_requests(authority, fixture.resource.id)
            .unwrap()
            .into_iter()
            .find(|request| request.request_id == request_id)
            .unwrap();
        let resource_id = fixture.resource.id;
        let trainer_task_id = fixture.task_id;
        let runtime_root = fixture.runtime_root.clone();
        home.prepare_task(trainer_task_id).unwrap();
        let trainer_lock = flock_exclusive(
            &home.task_paths(trainer_task_id).runner_lock,
            LockMode::NonBlocking,
        )
        .unwrap();
        drop(fixture.store);

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let before = call(&supervisor, |reply| SupervisorMsg::InspectResource {
            id: resource_id,
            reply,
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            before.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::ReleaseProofUnavailable {
                reason: ReleaseProofAttentionReason::TrainerNotCompleted,
                ..
            })
        ));

        crate::resource::watcher::tests::write_completed_result_for_test(&runtime_root, &binding);
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let terminal = call(&store, |reply| StoreMsg::CasExit {
            id: trainer_task_id,
            from: ProcessStatus::Running,
            reason: ExitReason::Exit { code: 0 },
            process_group_exit_evidence: ProcessGroupExitEvidence::ConfirmedExited,
            reply,
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(terminal.status(), ProcessStatus::Succeeded);
        drop(trainer_lock);

        let row = wait_for_terminal_task(&store, request.task_id).await;
        assert_eq!(row.status(), ProcessStatus::Succeeded);
        assert_eq!(fs::read(&marker).unwrap(), b"x");
        let after = call(&supervisor, |reply| SupervisorMsg::InspectResource {
            id: resource_id,
            reply,
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            after.loan.as_ref().map(|loan| &loan.state),
            Some(LoanState::Active {
                phase: LoanPhase::Serving {
                    release_provenance:
                        ServingReleaseProvenance::CompletedTrainerResult {
                            action_id: saved_action,
                            task_id: saved_task,
                            ..
                        },
                    ..
                }
            }) if *saved_action == action_id && *saved_task == trainer_task_id
        ));
        assert!(matches!(
            after.loan.map(|loan| loan.id),
            Some(saved_loan) if saved_loan == loan_id
        ));

        stop_test_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn pre_activation_cancellation_keeps_release_proof_for_the_next_fifo_request() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
        let marker = fixture.home.join("activation-count");
        let request_spec = fake_resource_task_spec(&fixture.home, &marker);
        let (mut fixture, binding, first_request_id, action_id, revision, loan_id) =
            release_completion_fixture_with(
                fixture,
                request_spec.clone(),
                machine_other_than(authority),
            );
        let second_origin = MachineId::new();
        let second_request = fixture
            .store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                fixture.resource.id,
                second_origin,
                request_spec,
            )
            .unwrap();
        publish_completed_result(
            &mut fixture,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        assert!(matches!(
            fixture
                .store
                .complete_release_for_authority(
                    authority,
                    fixture.resource.id,
                    action_id,
                    revision,
                )
                .unwrap(),
            ReleaseCompletionResult::Assigned { request, .. }
                if request.request_id == first_request_id
        ));
        let first_request = fixture
            .store
            .resource_requests(authority, fixture.resource.id)
            .unwrap()
            .into_iter()
            .find(|request| request.request_id == first_request_id)
            .unwrap();
        assert!(matches!(
            fixture
                .store
                .cancel_resource_request_before_activation(
                    authority,
                    first_request.request_id,
                    first_request.task_id,
                    fixture.resource.id,
                    first_request.origin_machine,
                )
                .unwrap(),
            crate::resource::store::QueueCancellationResult::Request(saved)
                if saved.request_id == first_request_id
                    && matches!(saved.state, ResourceRequestState::CancelledBeforeLaunch)
        ));
        let snapshot = fixture
            .store
            .resource_snapshots_for_authority(authority)
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.resource.id == fixture.resource.id)
            .unwrap();
        assert!(matches!(
            snapshot.loan.as_ref().map(|loan| &loan.state),
            Some(LoanState::Active {
                phase: LoanPhase::Serving {
                    current_request_id,
                    release_provenance:
                        ServingReleaseProvenance::CompletedTrainerResult {
                            action_id: saved_action,
                            ..
                        },
                    ..
                }
            }) if *current_request_id == second_request.request_id
                && *saved_action == action_id
        ));
        let resource_id = fixture.resource.id;
        let second_task_id = second_request.task_id;
        drop(fixture.store);

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let row = wait_for_terminal_task(&store, second_task_id).await;
        assert_eq!(row.status(), ProcessStatus::Succeeded);
        assert_eq!(fs::read(&marker).unwrap(), b"x");
        let requests = call(&store, |reply| StoreMsg::ResourceRequests {
            authority_machine: authority,
            resource_id,
            reply,
        })
        .await
        .unwrap();
        assert!(matches!(
            requests
                .iter()
                .find(|request| request.request_id == first_request_id),
            Some(ResourceRequest {
                state: ResourceRequestState::CancelledBeforeLaunch,
                ..
            })
        ));
        assert!(matches!(
            requests.iter().find(|request| request.request_id == second_request.request_id),
            Some(ResourceRequest {
                state: ResourceRequestState::Assigned { loan_id: assigned_loan },
                ..
            }) if *assigned_loan == loan_id
        ));

        stop_test_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn legacy_serving_loan_without_verified_provenance_does_not_launch() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let mut fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
        let marker = fixture.home.join("activation-count");
        let request_spec = fake_resource_task_spec(&fixture.home, &marker);
        let origin = machine_other_than(authority);
        let request = fixture
            .store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                fixture.resource.id,
                origin,
                request_spec,
            )
            .unwrap();
        let (loan, _) = fixture
            .store
            .seed_serving_loan_for_test(
                authority,
                fixture.resource.id,
                request.request_id,
                ReturnContext::Stopped {
                    task_id: fixture.task_id,
                    checkpoint_ref: "legacy-checkpoint".into(),
                    recovery_ref: "legacy-recovery".into(),
                },
            )
            .unwrap();
        assert!(matches!(
            loan.state,
            LoanState::Active {
                phase: LoanPhase::Serving {
                    release_provenance: ServingReleaseProvenance::Unverified,
                    ..
                }
            }
        ));
        let resource_id = fixture.resource.id;
        drop(fixture.store);

        let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
            .await
            .unwrap();
        let inspection = call(&supervisor, |reply| SupervisorMsg::InspectResource {
            id: resource_id,
            reply,
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::AttentionRequired {
                request: saved,
                reason: ResourceQueueAttentionReason::UnverifiedServingRelease,
            }) if saved.request_id == request.request_id
        ));
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        assert!(
            call(&store, |reply| StoreMsg::GetTask {
                id: request.task_id,
                reply,
            })
            .await
            .unwrap()
            .is_none()
        );
        assert!(!marker.exists());

        stop_test_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn ongoing_unproven_or_unconfirmed_trainer_keeps_request_queued_and_does_not_launch() {
        for (finish_task, publish_result, exit_evidence, expected_reason) in [
            (
                false,
                false,
                ProcessGroupExitEvidence::ConfirmedExited,
                ReleaseProofAttentionReason::TrainerNotCompleted,
            ),
            (
                true,
                false,
                ProcessGroupExitEvidence::ConfirmedExited,
                ReleaseProofAttentionReason::CompletedResultUnavailable,
            ),
            (
                true,
                true,
                ProcessGroupExitEvidence::Unconfirmed,
                ReleaseProofAttentionReason::WorkerExitUnconfirmed,
            ),
        ] {
            let directory = tempdir().unwrap();
            let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
            home.ensure().unwrap();
            let authority = load_or_create_machine_id(&home).unwrap();
            let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
            let marker = fixture.home.join("activation-count");
            let request_spec = fake_resource_task_spec(&fixture.home, &marker);
            let (mut fixture, binding, request_id, _, _, _) = release_completion_fixture_with(
                fixture,
                request_spec,
                machine_other_than(authority),
            );
            if publish_result {
                publish_completed_result(&mut fixture, &binding, exit_evidence);
            } else if finish_task {
                fixture.finish_registered_task_with_evidence(exit_evidence);
            }
            let resource_id = fixture.resource.id;
            let task_id = fixture
                .store
                .resource_requests(authority, resource_id)
                .unwrap()
                .into_iter()
                .find(|request| request.request_id == request_id)
                .unwrap()
                .task_id;
            drop(fixture.store);

            let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
                .await
                .unwrap();
            let snapshot = call(&store, |reply| StoreMsg::ResourceSnapshotsForAuthority {
                authority_machine: authority,
                reply,
            })
            .await
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.resource.id == resource_id)
            .unwrap();
            let (actor, actor_handle) = ResourceActor::spawn(
                None,
                ResourceActor,
                (
                    store.clone(),
                    None,
                    authority,
                    snapshot.resource,
                    snapshot.loan,
                ),
            )
            .await
            .unwrap();
            let inspection = call(&actor, |reply| ResourceMsg::Inspect { reply })
                .await
                .unwrap();
            assert!(matches!(
                inspection.reconcile_outcome,
                Some(ResourceQueueReconcileOutcome::ReleaseProofUnavailable {
                    reason,
                    ..
                }) if reason == expected_reason
            ));
            let requests = call(&store, |reply| StoreMsg::ResourceRequests {
                authority_machine: authority,
                resource_id,
                reply,
            })
            .await
            .unwrap();
            assert!(matches!(
                requests
                    .iter()
                    .find(|request| request.request_id == request_id),
                Some(ResourceRequest {
                    state: ResourceRequestState::Queued,
                    ..
                })
            ));
            assert!(
                call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(!marker.exists());

            stop_test_resource_actor(actor, actor_handle, store, store_handle).await;
        }
    }

    #[test]
    fn completed_release_requires_and_uses_the_authority_verified_result() {
        let (mut fixture, binding, request_id, action_id, revision, loan_id) =
            release_completion_fixture();
        publish_completed_result(
            &mut fixture,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let publication = find_completed_result(&fixture.runtime_root, &binding)
            .unwrap()
            .unwrap();

        let result = fixture
            .store
            .complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap();
        let ReleaseCompletionResult::Assigned {
            loan,
            request,
            state_revision,
        } = result
        else {
            panic!("queued work must be assigned after a verified completed result");
        };

        assert_eq!(loan.id, loan_id);
        assert_eq!(request.request_id, request_id);
        assert_eq!(state_revision, ResourceRevision::new(2));
        assert!(matches!(
            loan.state,
            LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context: ReturnContext::AlreadyCompleted {
                        task_id,
                        result_ref,
                    },
                    ..
                }
            } if task_id == fixture.task_id
                && result_ref == format!(
                    "{}#sha256={}",
                    publication.publication_path.display(),
                    publication.publication_sha256
                )
        ));
        assert!(matches!(
            fixture.store.resource_requests(fixture.authority, fixture.resource.id).unwrap()[0]
                .state,
            ResourceRequestState::Assigned { loan_id: assigned } if assigned == loan_id
        ));
    }

    #[test]
    fn exact_release_retry_does_not_need_removed_artifacts() {
        let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut fixture,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let first = fixture
            .store
            .complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap();
        fs::remove_dir_all(&fixture.runtime_root).unwrap();

        let retry = fixture
            .store
            .complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap();

        assert_eq!(
            serde_json::to_value(retry).unwrap(),
            serde_json::to_value(first).unwrap()
        );
    }

    #[test]
    fn authority_proves_a_stopped_trainer_from_its_saved_checkpoint_decision() {
        let (mut fixture, _, request_id, action_id, revision, loan_id) =
            release_completion_fixture();
        let (decision, cancellation) =
            commit_stopped_release_decision(&mut fixture, action_id, revision);
        fixture.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );

        let result = fixture
            .store
            .complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap();
        let ReleaseCompletionResult::Assigned {
            loan,
            request,
            state_revision,
        } = result
        else {
            panic!("the queued request must be assigned after a proved stop");
        };
        let checkpoint = decision.selected_checkpoint;
        assert_eq!(request.request_id, request_id);
        assert_eq!(loan.id, loan_id);
        assert_eq!(state_revision, ResourceRevision::new(2));
        assert!(matches!(
            loan.state,
            LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context: ReturnContext::Stopped {
                        task_id,
                        checkpoint_ref,
                        recovery_ref,
                    },
                    release_provenance: ServingReleaseProvenance::StoppedTrainerCheckpoint {
                        action_id: saved_action,
                        task_id: saved_task,
                        generation_id,
                        record_sha256,
                        inventory_sha256,
                    },
                    ..
                }
            } if task_id == fixture.task_id
                && saved_action == action_id
                && saved_task == fixture.task_id
                && checkpoint_ref == format!(
                    "{}#sha256={}", checkpoint.path.display(), checkpoint.record_sha256
                )
                && recovery_ref == checkpoint.generation_id
                && generation_id == checkpoint.generation_id
                && record_sha256 == checkpoint.record_sha256
                && inventory_sha256 == checkpoint.inventory_sha256
        ));
        assert_eq!(
            fixture
                .store
                .require_task(fixture.task_id)
                .unwrap()
                .cancel_requested_at,
            Some(cancellation.cancel_requested_at)
        );
    }

    #[test]
    fn stopped_release_without_queued_work_creates_a_verified_return_context() {
        let (mut fixture, _, request_id, action_id, revision, _) = release_completion_fixture();
        let queued = fixture
            .store
            .resource_requests(fixture.authority, fixture.resource.id)
            .unwrap()
            .into_iter()
            .find(|request| request.request_id == request_id)
            .unwrap();
        assert!(matches!(
            fixture
                .store
                .cancel_resource_request_before_activation(
                    fixture.authority,
                    queued.request_id,
                    queued.task_id,
                    fixture.resource.id,
                    queued.origin_machine,
                )
                .unwrap(),
            crate::resource::store::QueueCancellationResult::Request(_)
        ));
        let (decision, _) = commit_stopped_release_decision(&mut fixture, action_id, revision);
        fixture.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );

        let result = fixture
            .store
            .complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap();
        let ReleaseCompletionResult::ReturnRequired { loan, .. } = result else {
            panic!("the empty queue must return the verified stopped context");
        };
        let checkpoint = decision.selected_checkpoint;
        assert!(matches!(
            loan.state,
            LoanState::Active {
                phase: LoanPhase::AwaitingReturn {
                    return_context: ReturnContext::Stopped {
                        task_id,
                        checkpoint_ref,
                        recovery_ref,
                    },
                    ..
                }
            } if task_id == fixture.task_id
                && checkpoint_ref == format!(
                    "{}#sha256={}", checkpoint.path.display(), checkpoint.record_sha256
                )
                && recovery_ref == checkpoint.generation_id
        ));
    }

    #[test]
    fn exact_stopped_release_retry_uses_its_receipt_after_checkpoint_removal() {
        let (mut fixture, _, _, action_id, revision, _) = release_completion_fixture();
        let (decision, _) = commit_stopped_release_decision(&mut fixture, action_id, revision);
        fixture.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let first = fixture
            .store
            .complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap();
        fs::remove_dir_all(&decision.selected_checkpoint.path).unwrap();

        let retry = fixture
            .store
            .complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap();

        assert_eq!(
            serde_json::to_value(retry).unwrap(),
            serde_json::to_value(first).unwrap()
        );
    }

    #[test]
    fn stopped_release_rejects_missing_or_changed_selected_checkpoint() {
        for changed in ["missing", "contents"] {
            let (mut fixture, _, _, action_id, revision, _) = release_completion_fixture();
            let (decision, _) = commit_stopped_release_decision(&mut fixture, action_id, revision);
            fixture.finish_registered_task_cancelled_with_evidence(
                ProcessGroupExitEvidence::ConfirmedExited,
            );
            if changed == "missing" {
                fs::remove_dir_all(&decision.selected_checkpoint.path).unwrap();
            } else {
                fs::write(
                    decision.selected_checkpoint.path.join("checkpoint.bin"),
                    b"changed checkpoint contents",
                )
                .unwrap();
            }

            assert!(matches!(
                fixture.store.complete_release_for_authority(
                    fixture.authority,
                    fixture.resource.id,
                    action_id,
                    revision,
                ),
                Err(CompleteReleaseError::StoppedCheckpointChanged { task_id })
                    if task_id == fixture.task_id
            ));
        }
    }

    #[test]
    fn completed_release_rejects_missing_or_mismatched_association() {
        let (mut missing, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut missing,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        missing
            .store
            .conn
            .execute(
                "DELETE FROM trainer_attempt_associations WHERE task_id = ?1",
                [missing.task_id.to_string()],
            )
            .unwrap();
        assert!(matches!(
            missing.store.complete_release_for_authority(
                missing.authority,
                missing.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::TrainerAssociationMissing { task_id })
                if task_id == missing.task_id
        ));

        let (mut mismatched, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut mismatched,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let mut association: serde_json::Value = serde_json::from_str(
            &saved_trainer_association_json(&mismatched.store, mismatched.task_id),
        )
        .unwrap();
        association["normalized_spec_sha256"] = json!("0".repeat(64));
        mismatched
            .store
            .conn
            .execute(
                "UPDATE trainer_attempt_associations SET association_json = ?1
                 WHERE task_id = ?2",
                params![
                    serde_json::to_string(&association).unwrap(),
                    mismatched.task_id.to_string()
                ],
            )
            .unwrap();
        assert!(matches!(
            mismatched.store.complete_release_for_authority(
                mismatched.authority,
                mismatched.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
                if task_id == mismatched.task_id
        ));
    }

    #[test]
    fn completed_release_rejects_unconfirmed_worker_exit() {
        let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut fixture,
            &binding,
            ProcessGroupExitEvidence::Unconfirmed,
        );

        assert!(matches!(
            fixture.store.complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::WorkerExitUnconfirmed { task_id })
                if task_id == fixture.task_id
        ));
    }

    #[test]
    fn completed_release_rejects_a_still_held_ownership_lock() {
        let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut fixture,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let lock_holder = fixture.hold_saved_lock();

        assert!(matches!(
            fixture.store.complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::OwnershipLockStillHeld { task_id })
                if task_id == fixture.task_id
        ));
        drop(lock_holder);
    }

    #[test]
    fn completed_release_rejects_missing_replaced_and_symlinked_locks() {
        for (replacement, reason) in [
            (
                "missing",
                crate::resource::ownership_lock::OwnershipLockIdentityMismatchReason::Missing,
            ),
            (
                "replaced",
                crate::resource::ownership_lock::OwnershipLockIdentityMismatchReason::DifferentFile,
            ),
            (
                "symlink",
                crate::resource::ownership_lock::OwnershipLockIdentityMismatchReason::SymbolicLink,
            ),
        ] {
            let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
            publish_completed_result(
                &mut fixture,
                &binding,
                ProcessGroupExitEvidence::ConfirmedExited,
            );
            let lock_path = fixture.runtime_root.join(".segment.lock");
            let saved_path = fixture.runtime_root.join(".segment.lock.saved");
            if replacement == "missing" {
                fs::remove_file(&lock_path).unwrap();
            } else {
                fs::rename(&lock_path, &saved_path).unwrap();
                if replacement == "replaced" {
                    fs::write(&lock_path, b"new lock file").unwrap();
                } else {
                    std::os::unix::fs::symlink(&saved_path, &lock_path).unwrap();
                }
            }

            let result = fixture.store.complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            );
            assert!(matches!(
                result,
                Err(CompleteReleaseError::OwnershipLock(
                    crate::resource::ownership_lock::OwnershipLockProbeError::IdentityMismatch {
                        reason: found,
                        ..
                    }
                )) if found == reason
            ));
        }
    }

    #[test]
    fn completed_release_rejects_wrong_action_or_current_task() {
        let (mut fixture, binding, _, action_id, revision, loan_id) = release_completion_fixture();
        publish_completed_result(
            &mut fixture,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let wrong_action = ActionId::new();
        assert!(matches!(
            fixture.store.complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                wrong_action,
                revision,
            ),
            Err(CompleteReleaseError::NotAwaitingRelease { loan_id: found, action_id })
                if found == loan_id && action_id == wrong_action
        ));

        let different_task = TaskId::new();
        fixture
            .store
            .conn
            .execute(
                "UPDATE resources SET registered_background_task = ?1 WHERE id = ?2",
                params![
                    different_task.to_string(),
                    fixture.resource.id.as_uuid().to_string()
                ],
            )
            .unwrap();
        assert!(matches!(
            fixture.store.complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
                if task_id == fixture.task_id
        ));
    }

    #[test]
    fn completed_release_rejects_missing_or_mismatched_result() {
        let (mut missing, _, _, action_id, revision, _) = release_completion_fixture();
        missing.finish_registered_task_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
        assert!(matches!(
            missing.store.complete_release_for_authority(
                missing.authority,
                missing.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::CompletedResultMissing { task_id })
                if task_id == missing.task_id
        ));

        let (mut mismatched, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut mismatched,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        fs::write(
            mismatched
                .runtime_root
                .join("attempts")
                .join(&binding.attempt_id)
                .join("terminal.json"),
            b"{}",
        )
        .unwrap();
        assert!(matches!(
            mismatched.store.complete_release_for_authority(
                mismatched.authority,
                mismatched.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::Watcher(_))
        ));
    }

    #[test]
    fn nonzero_exit_stays_fail_closed_without_durable_stop_evidence() {
        let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
        crate::resource::watcher::tests::write_completed_result_for_test(
            &fixture.runtime_root,
            &binding,
        );
        crate::resource::watcher::tests::write_generation_for_test(
            &fixture.runtime_root,
            &binding,
            "checkpoint-after-start",
            4,
        );
        fixture
            .store
            .cas_exit_with_evidence(
                fixture.task_id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 3 },
                ProcessGroupExitEvidence::ConfirmedExited,
            )
            .unwrap()
            .unwrap();
        fixture
            .store
            .update_execution_state(fixture.task_id, ProcessStatus::Failed)
            .unwrap();

        assert!(matches!(
            fixture.store.complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::StoppedProofUnavailable { task_id })
                if task_id == fixture.task_id
        ));
    }

    #[test]
    fn stopped_release_requires_the_exact_cancellation_action_task_and_association() {
        let (mut wrong_action, _, _, action_id, revision, _) = release_completion_fixture();
        let (_, _) = commit_stopped_release_decision(&mut wrong_action, action_id, revision);
        wrong_action.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let wrong_action_id = ActionId::new();
        assert!(matches!(
            wrong_action.store.complete_release_for_authority(
                wrong_action.authority,
                wrong_action.resource.id,
                wrong_action_id,
                revision,
            ),
            Err(CompleteReleaseError::NotAwaitingRelease { action_id, .. })
                if action_id == wrong_action_id
        ));

        let (mut wrong_task, _, _, action_id, revision, _) = release_completion_fixture();
        let _ = commit_stopped_release_decision(&mut wrong_task, action_id, revision);
        wrong_task.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let replacement_task = TaskId::new();
        wrong_task
            .store
            .conn
            .execute(
                "UPDATE resources SET registered_background_task = ?1 WHERE id = ?2",
                params![
                    replacement_task.to_string(),
                    wrong_task.resource.id.as_uuid().to_string()
                ],
            )
            .unwrap();
        assert!(matches!(
            wrong_task.store.complete_release_for_authority(
                wrong_task.authority,
                wrong_task.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
                if task_id == wrong_task.task_id
        ));

        let (mut wrong_association, _, _, action_id, revision, _) = release_completion_fixture();
        let _ = commit_stopped_release_decision(&mut wrong_association, action_id, revision);
        wrong_association.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let mut association: serde_json::Value = serde_json::from_str(
            &saved_trainer_association_json(&wrong_association.store, wrong_association.task_id),
        )
        .unwrap();
        association["request_sha256"] = json!("63".repeat(32));
        wrong_association
            .store
            .conn
            .execute(
                "UPDATE trainer_attempt_associations SET association_json = ?1
                 WHERE task_id = ?2",
                params![
                    serde_json::to_string(&association).unwrap(),
                    wrong_association.task_id.to_string(),
                ],
            )
            .unwrap();
        assert!(matches!(
            wrong_association.store.complete_release_for_authority(
                wrong_association.authority,
                wrong_association.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
                if task_id == wrong_association.task_id
        ));
    }

    #[test]
    fn generic_cancellation_and_checkpoint_alone_do_not_release_the_loan() {
        let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
        crate::resource::watcher::tests::write_generation_for_test(
            &fixture.runtime_root,
            &binding,
            "generation-without-stop-decision",
            81,
        );
        assert!(matches!(
            fixture.store.request_cancel(fixture.task_id).unwrap(),
            crate::store::CancelResult::SignalWorker(_)
        ));
        fixture.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );

        assert!(matches!(
            fixture.store.complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::StoppedProofUnavailable { task_id })
                if task_id == fixture.task_id
        ));
    }

    #[test]
    fn stopped_release_rejects_nonterminal_lost_and_failed_trainer_tasks() {
        let (mut running, _, _, action_id, revision, _) = release_completion_fixture();
        assert!(matches!(
            running.store.complete_release_for_authority(
                running.authority,
                running.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::BackgroundTaskNotTerminal { task_id, .. })
                if task_id == running.task_id
        ));

        let (mut lost, _, _, action_id, revision, _) = release_completion_fixture();
        lost.store
            .conn
            .execute(
                "UPDATE tasks SET status = 'lost', exit_reason = NULL WHERE id = ?1",
                [lost.task_id.to_string()],
            )
            .unwrap();
        assert!(matches!(
            lost.store.complete_release_for_authority(
                lost.authority,
                lost.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::BackgroundTaskLost { task_id })
                if task_id == lost.task_id
        ));

        let (mut failed, _, _, action_id, revision, _) = release_completion_fixture();
        let _ = commit_stopped_release_decision(&mut failed, action_id, revision);
        failed
            .store
            .cas_exit_with_evidence(
                failed.task_id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 9 },
                ProcessGroupExitEvidence::ConfirmedExited,
            )
            .unwrap()
            .unwrap();
        failed
            .store
            .update_execution_state(failed.task_id, ProcessStatus::Failed)
            .unwrap();
        assert!(matches!(
            failed.store.complete_release_for_authority(
                failed.authority,
                failed.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::StoppedProofUnavailable { task_id })
                if task_id == failed.task_id
        ));
    }

    #[test]
    fn stopped_release_requires_confirmed_worker_exit() {
        let (mut fixture, _, _, action_id, revision, _) = release_completion_fixture();
        let _ = commit_stopped_release_decision(&mut fixture, action_id, revision);
        fixture
            .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::Unconfirmed);

        assert!(matches!(
            fixture.store.complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::WorkerExitUnconfirmed { task_id })
                if task_id == fixture.task_id
        ));
    }

    #[test]
    fn stopped_release_requires_the_saved_lock_to_be_exact_and_exclusively_free() {
        for replacement in ["held", "missing", "replaced", "symlink"] {
            let (mut fixture, _, _, action_id, revision, _) = release_completion_fixture();
            let _ = commit_stopped_release_decision(&mut fixture, action_id, revision);
            fixture.finish_registered_task_cancelled_with_evidence(
                ProcessGroupExitEvidence::ConfirmedExited,
            );
            let lock_path = fixture.runtime_root.join(".segment.lock");
            let saved_path = fixture.runtime_root.join(".segment.lock.saved");
            if replacement == "held" {
                let lock_holder = fixture.hold_saved_lock();
                assert!(matches!(
                    fixture.store.complete_release_for_authority(
                        fixture.authority,
                        fixture.resource.id,
                        action_id,
                        revision,
                    ),
                    Err(CompleteReleaseError::OwnershipLockStillHeld { task_id })
                        if task_id == fixture.task_id
                ));
                drop(lock_holder);
                continue;
            }
            if replacement == "missing" {
                fs::remove_file(&lock_path).unwrap();
            } else {
                fs::rename(&lock_path, &saved_path).unwrap();
                if replacement == "replaced" {
                    fs::write(&lock_path, b"new lock file").unwrap();
                } else {
                    std::os::unix::fs::symlink(&saved_path, &lock_path).unwrap();
                }
            }

            let expected_reason = match replacement {
                "missing" => crate::resource::ownership_lock::OwnershipLockIdentityMismatchReason::Missing,
                "replaced" => crate::resource::ownership_lock::OwnershipLockIdentityMismatchReason::DifferentFile,
                "symlink" => crate::resource::ownership_lock::OwnershipLockIdentityMismatchReason::SymbolicLink,
                _ => unreachable!(),
            };
            assert!(matches!(
                fixture.store.complete_release_for_authority(
                    fixture.authority,
                    fixture.resource.id,
                    action_id,
                    revision,
                ),
                Err(CompleteReleaseError::OwnershipLock(
                    crate::resource::ownership_lock::OwnershipLockProbeError::IdentityMismatch {
                        reason,
                        ..
                    }
                )) if reason == expected_reason
            ));
        }
    }

    #[test]
    fn proven_stopped_release_accepts_the_next_fifo_request_after_prelaunch_cancel() {
        let fixture = TrainerAssociationFixture::new();
        let authority = fixture.authority;
        let marker = fixture.home.join("assigned-command-marker");
        let request_spec = fake_resource_task_spec(&fixture.home, &marker);
        let first_origin = machine_other_than(authority);
        let (mut fixture, _, first_request_id, action_id, revision, loan_id) =
            release_completion_fixture_with(fixture, request_spec.clone(), first_origin);
        let second_request = fixture
            .store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                fixture.resource.id,
                machine_other_than(authority),
                request_spec,
            )
            .unwrap();
        let _ = commit_stopped_release_decision(&mut fixture, action_id, revision);
        fixture.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        assert!(matches!(
            fixture
                .store
                .complete_release_for_authority(
                    authority,
                    fixture.resource.id,
                    action_id,
                    revision,
                )
                .unwrap(),
            ReleaseCompletionResult::Assigned { request, .. }
                if request.request_id == first_request_id
        ));
        let first_request = fixture
            .store
            .resource_requests(authority, fixture.resource.id)
            .unwrap()
            .into_iter()
            .find(|request| request.request_id == first_request_id)
            .unwrap();
        fixture
            .store
            .cancel_resource_request_before_activation(
                authority,
                first_request.request_id,
                first_request.task_id,
                fixture.resource.id,
                first_request.origin_machine,
            )
            .unwrap();
        let snapshot = fixture
            .store
            .resource_snapshots_for_authority(authority)
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.resource.id == fixture.resource.id)
            .unwrap();
        assert!(matches!(
            snapshot.loan.as_ref().map(|loan| &loan.state),
            Some(LoanState::Active {
                phase: LoanPhase::Serving {
                    current_request_id,
                    release_provenance: ServingReleaseProvenance::StoppedTrainerCheckpoint {
                        action_id: saved_action,
                        ..
                    },
                    ..
                }
            }) if *current_request_id == second_request.request_id
                && *saved_action == action_id
        ));

        let input = ResourceTaskAcceptanceInput {
            authority_machine: authority,
            resource_id: fixture.resource.id,
            request_id: second_request.request_id,
            task_id: second_request.task_id,
            acceptance_sequence: second_request.acceptance_sequence,
            loan_id,
            expected_state_revision: snapshot.resource.state_revision,
            command_spec: second_request.spec().clone(),
            executor_env: TaskEnv::capture(),
        };
        assert_eq!(
            fixture.store.accept_assigned_resource_task(input).unwrap(),
            ResourceTaskAcceptance::Inserted {
                task: second_request.task_id,
            }
        );
        assert_eq!(
            fixture
                .store
                .get_task(second_request.task_id)
                .unwrap()
                .unwrap()
                .status(),
            ProcessStatus::Queued
        );
    }

    #[test]
    fn release_transaction_rechecks_task_state_association_and_identity() {
        for changed_record in ["task", "task_command", "association", "identity"] {
            let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
            publish_completed_result(
                &mut fixture,
                &binding,
                ProcessGroupExitEvidence::ConfirmedExited,
            );
            let proof = fixture
                .store
                .build_verified_release_proof(
                    fixture.authority,
                    fixture.resource.id,
                    action_id,
                    revision,
                )
                .unwrap();

            let expected_error = match changed_record {
                "task" => {
                    fixture
                        .store
                        .conn
                        .execute(
                            "UPDATE tasks SET status = 'failed', exit_reason = ?1
                             WHERE id = ?2",
                            params![
                                serde_json::to_string(&ExitReason::Exit { code: 1 }).unwrap(),
                                fixture.task_id.to_string(),
                            ],
                        )
                        .unwrap();
                    "task"
                }
                "task_command" => {
                    fixture
                        .store
                        .conn
                        .execute(
                            "UPDATE tasks SET cwd = ?1 WHERE id = ?2",
                            params!["/changed/task/command", fixture.task_id.to_string()],
                        )
                        .unwrap();
                    "task_command"
                }
                "association" => {
                    let mut association: serde_json::Value = serde_json::from_str(
                        &saved_trainer_association_json(&fixture.store, fixture.task_id),
                    )
                    .unwrap();
                    association["normalized_spec_sha256"] = json!("1".repeat(64));
                    fixture
                        .store
                        .conn
                        .execute(
                            "UPDATE trainer_attempt_associations SET association_json = ?1
                             WHERE task_id = ?2",
                            params![
                                serde_json::to_string(&association).unwrap(),
                                fixture.task_id.to_string(),
                            ],
                        )
                        .unwrap();
                    "association"
                }
                "identity" => {
                    let mut identity: serde_json::Value = fixture
                        .store
                        .conn
                        .query_row(
                            "SELECT identity_json FROM executor_identities WHERE task_id = ?1",
                            [fixture.task_id.to_string()],
                            |row| row.get::<_, String>(0),
                        )
                        .map(|json| serde_json::from_str(&json).unwrap())
                        .unwrap();
                    identity["state"] = json!("failed");
                    fixture
                        .store
                        .conn
                        .execute(
                            "UPDATE executor_identities SET identity_json = ?1 WHERE task_id = ?2",
                            params![
                                serde_json::to_string(&identity).unwrap(),
                                fixture.task_id.to_string()
                            ],
                        )
                        .unwrap();
                    "identity"
                }
                _ => unreachable!(),
            };

            let result = persist_release_completion_for_authority(&mut fixture.store.conn, proof);
            match expected_error {
                "task" => assert!(matches!(
                    result,
                    Err(CompleteReleaseError::TaskStateChanged { task_id })
                        if task_id == fixture.task_id
                )),
                "task_command" => assert!(matches!(
                    result,
                    Err(CompleteReleaseError::TaskCommandChanged { task_id })
                        if task_id == fixture.task_id
                )),
                "association" => assert!(matches!(
                    result,
                    Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
                        if task_id == fixture.task_id
                )),
                "identity" => assert!(matches!(
                    result,
                    Err(CompleteReleaseError::TrainerIdentityChanged { task_id })
                        if task_id == fixture.task_id
                )),
                _ => unreachable!(),
            }
            let snapshot = fixture
                .store
                .resource_snapshots_for_authority(fixture.authority)
                .unwrap()
                .into_iter()
                .find(|snapshot| snapshot.resource.id == fixture.resource.id)
                .unwrap();
            assert_eq!(snapshot.resource.state_revision, ResourceRevision::new(1));
            assert!(matches!(
                snapshot.loan.unwrap().state,
                LoanState::Active {
                    phase: LoanPhase::AwaitingRelease { .. }
                }
            ));
        }
    }

    #[test]
    fn release_transaction_rechecks_action_revision_and_current_task() {
        for changed_binding in ["action", "revision", "task"] {
            let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
            publish_completed_result(
                &mut fixture,
                &binding,
                ProcessGroupExitEvidence::ConfirmedExited,
            );
            let proof = fixture
                .store
                .build_verified_release_proof(
                    fixture.authority,
                    fixture.resource.id,
                    action_id,
                    revision,
                )
                .unwrap();

            match changed_binding {
                "action" => {
                    let mut loan_state: serde_json::Value = fixture
                        .store
                        .conn
                        .query_row(
                            "SELECT state_json FROM loans WHERE resource_id = ?1",
                            [fixture.resource.id.as_uuid().to_string()],
                            |row| row.get::<_, String>(0),
                        )
                        .map(|json| serde_json::from_str(&json).unwrap())
                        .unwrap();
                    loan_state["phase"]["action_id"] = json!(ActionId::new());
                    fixture
                        .store
                        .conn
                        .execute(
                            "UPDATE loans SET state_json = ?1 WHERE resource_id = ?2",
                            params![
                                serde_json::to_string(&loan_state).unwrap(),
                                fixture.resource.id.as_uuid().to_string(),
                            ],
                        )
                        .unwrap();
                }
                "revision" => {
                    fixture
                        .store
                        .conn
                        .execute(
                            "UPDATE resources SET state_revision = 9 WHERE id = ?1",
                            [fixture.resource.id.as_uuid().to_string()],
                        )
                        .unwrap();
                }
                "task" => {
                    fixture
                        .store
                        .conn
                        .execute(
                            "UPDATE resources SET registered_background_task = ?1 WHERE id = ?2",
                            params![
                                TaskId::new().to_string(),
                                fixture.resource.id.as_uuid().to_string(),
                            ],
                        )
                        .unwrap();
                }
                _ => unreachable!(),
            }

            let result = persist_release_completion_for_authority(&mut fixture.store.conn, proof);
            match changed_binding {
                "action" => assert!(matches!(
                    result,
                    Err(CompleteReleaseError::NotAwaitingRelease { .. })
                )),
                "revision" => assert!(matches!(
                    result,
                    Err(CompleteReleaseError::StaleRevision {
                        expected,
                        actual,
                    }) if expected == revision && actual == ResourceRevision::new(9)
                )),
                "task" => assert!(matches!(
                    result,
                    Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
                        if task_id == fixture.task_id
                )),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn release_transaction_rechecks_result_and_lock_path_before_commit() {
        let (mut changed_result, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut changed_result,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let proof = changed_result
            .store
            .build_verified_release_proof(
                changed_result.authority,
                changed_result.resource.id,
                action_id,
                revision,
            )
            .unwrap();
        fs::write(
            changed_result
                .runtime_root
                .join("published")
                .join(format!("result-{}", binding.attempt_id))
                .join("checkpoint.bin"),
            b"changed result bytes",
        )
        .unwrap();
        assert!(matches!(
            persist_release_completion_for_authority(&mut changed_result.store.conn, proof),
            Err(CompleteReleaseError::Watcher(_))
        ));

        let (mut changed_lock, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut changed_lock,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let proof = changed_lock
            .store
            .build_verified_release_proof(
                changed_lock.authority,
                changed_lock.resource.id,
                action_id,
                revision,
            )
            .unwrap();
        let lock_path = changed_lock.runtime_root.join(".segment.lock");
        let saved_path = changed_lock.runtime_root.join(".segment.lock.saved");
        fs::rename(&lock_path, &saved_path).unwrap();
        fs::write(&lock_path, b"replaced lock").unwrap();
        assert!(matches!(
            persist_release_completion_for_authority(&mut changed_lock.store.conn, proof),
            Err(CompleteReleaseError::OwnershipLock(
                crate::resource::ownership_lock::OwnershipLockProbeError::IdentityMismatch {
                    reason: crate::resource::ownership_lock::OwnershipLockIdentityMismatchReason::DifferentFile,
                    ..
                }
            ))
        ));
    }

    #[test]
    fn stopped_release_transaction_rechecks_saved_marker_checkpoint_and_lock() {
        let (mut changed_marker, _, _, action_id, revision, _) = release_completion_fixture();
        let _ = commit_stopped_release_decision(&mut changed_marker, action_id, revision);
        changed_marker.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let proof = changed_marker
            .store
            .build_verified_release_proof(
                changed_marker.authority,
                changed_marker.resource.id,
                action_id,
                revision,
            )
            .unwrap();
        changed_marker
            .store
            .conn
            .execute(
                "UPDATE tasks SET cancel_requested_at = ?1 WHERE id = ?2",
                params![
                    "2026-01-01T00:00:00.000000000Z",
                    changed_marker.task_id.to_string()
                ],
            )
            .unwrap();
        assert!(matches!(
            persist_release_completion_for_authority(&mut changed_marker.store.conn, proof),
            Err(CompleteReleaseError::TrainerCancellationMarkerChanged { task_id })
                if task_id == changed_marker.task_id
        ));

        let (mut changed_checkpoint, _, _, action_id, revision, _) = release_completion_fixture();
        let (decision, _) =
            commit_stopped_release_decision(&mut changed_checkpoint, action_id, revision);
        changed_checkpoint.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let proof = changed_checkpoint
            .store
            .build_verified_release_proof(
                changed_checkpoint.authority,
                changed_checkpoint.resource.id,
                action_id,
                revision,
            )
            .unwrap();
        fs::write(
            decision.selected_checkpoint.path.join("checkpoint.bin"),
            b"changed after proof construction",
        )
        .unwrap();
        assert!(matches!(
            persist_release_completion_for_authority(&mut changed_checkpoint.store.conn, proof),
            Err(CompleteReleaseError::StoppedCheckpointChanged { task_id })
                if task_id == changed_checkpoint.task_id
        ));

        let (mut changed_lock, _, _, action_id, revision, _) = release_completion_fixture();
        let _ = commit_stopped_release_decision(&mut changed_lock, action_id, revision);
        changed_lock.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let proof = changed_lock
            .store
            .build_verified_release_proof(
                changed_lock.authority,
                changed_lock.resource.id,
                action_id,
                revision,
            )
            .unwrap();
        let lock_path = changed_lock.runtime_root.join(".segment.lock");
        let saved_path = changed_lock.runtime_root.join(".segment.lock.saved");
        fs::rename(&lock_path, &saved_path).unwrap();
        fs::write(&lock_path, b"replaced lock").unwrap();
        assert!(matches!(
            persist_release_completion_for_authority(&mut changed_lock.store.conn, proof),
            Err(CompleteReleaseError::OwnershipLock(
                crate::resource::ownership_lock::OwnershipLockProbeError::IdentityMismatch {
                    reason: crate::resource::ownership_lock::OwnershipLockIdentityMismatchReason::DifferentFile,
                    ..
                }
            ))
        ));
    }

    #[test]
    fn queue_operations_require_the_registered_authority() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let other_machine = MachineId::new();
        let resource = resource(authority);

        assert!(matches!(
            store.register_resource(other_machine, &resource),
            Err(ResourceStoreError::WrongAuthority { expected, found })
                if expected == authority && found == other_machine
        ));
        store.register_resource(authority, &resource).unwrap();
        assert_eq!(
            store.register_resource(authority, &resource).unwrap(),
            resource
        );

        let request = RequestId::new();
        let task = TaskId::new();
        let origin = MachineId::new();
        assert!(matches!(
            store.accept_resource_request(
                other_machine,
                request,
                task,
                resource.id,
                origin,
                spec(),
            ),
            Err(ResourceStoreError::WrongAuthority { expected, found })
                if expected == authority && found == other_machine
        ));
        assert!(
            store
                .resource_requests(authority, resource.id)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .oldest_queued_resource_request(authority, resource.id)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            store.cancel_resource_request_before_activation(
                other_machine,
                request,
                task,
                resource.id,
                origin,
            ),
            Err(ResourceStoreError::WrongAuthority { expected, found })
                if expected == authority && found == other_machine
        ));
        assert_eq!(identity_count(&store, task), 0);
    }

    #[test]
    fn queue_acceptance_rejects_task_ids_owned_by_the_executor() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();

        let accepted_task = TaskId::new();
        store
            .accept_execution(&ExecutionRecord {
                task: accepted_task,
                origin_machine: origin,
                execution_machine: authority,
                spec: spec().into(),
                state: crate::domain::ProcessStatus::Queued,
            })
            .unwrap();
        let rejected_task = TaskId::new();
        store
            .reject_execution(&RejectionTombstone {
                task: rejected_task,
                origin_machine: origin,
                execution_machine: authority,
                reason: "prior rejection".into(),
            })
            .unwrap();

        for task in [accepted_task, rejected_task] {
            assert!(matches!(
                store.accept_resource_request(
                    authority,
                    RequestId::new(),
                    task,
                    resource.id,
                    origin,
                    spec(),
                ),
                Err(ResourceStoreError::Conflict)
            ));
        }
        assert!(
            store
                .resource_requests(authority, resource.id)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn durable_resource_cancellation_receipt_replays_and_conflicts_by_identity() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let identity = resource_cancellation_identity(
            Uuid::now_v7(),
            RequestId::new(),
            TaskId::new(),
            resource.id,
            origin,
            authority,
            ResourceRoutePhase::AcceptanceUnknown,
        );

        let first = store
            .cancel_resource_request_with_receipt(
                authority,
                identity.clone(),
                resource_cancellation_proof(
                    &identity,
                    &spec(),
                    ResourceRoutePhase::AcceptanceUnknown,
                ),
            )
            .unwrap();
        assert_eq!(
            first.outcome,
            crate::submission::ResourceCancellationOutcome::PreventedBeforeAcceptance
        );
        assert_eq!(
            store.resource_cancellation_receipt(&identity).unwrap(),
            Some(first.clone())
        );
        assert_eq!(
            store
                .cancel_resource_request_with_receipt(
                    authority,
                    identity.clone(),
                    resource_cancellation_proof(
                        &identity,
                        &spec(),
                        ResourceRoutePhase::AcceptanceUnknown,
                    ),
                )
                .unwrap(),
            first
        );

        let mut changed = identity;
        changed.task = TaskId::new();
        assert!(matches!(
            store.cancel_resource_request_with_receipt(
                authority,
                changed.clone(),
                resource_cancellation_proof(
                    &changed,
                    &spec(),
                    ResourceRoutePhase::AcceptanceUnknown,
                ),
            ),
            Err(ResourceStoreError::Conflict)
        ));
        assert_eq!(prevention_count(&store), 1);
    }

    #[test]
    fn durable_resource_cancellation_cancels_queued_and_assigned_requests() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let queued_request = RequestId::new();
        let queued_task = TaskId::new();
        store
            .accept_resource_request(
                authority,
                queued_request,
                queued_task,
                resource.id,
                origin,
                spec(),
            )
            .unwrap();
        let queued_identity = resource_cancellation_identity(
            Uuid::now_v7(),
            queued_request,
            queued_task,
            resource.id,
            origin,
            authority,
            ResourceRoutePhase::Waiting,
        );
        let queued_receipt = store
            .cancel_resource_request_with_receipt(
                authority,
                queued_identity.clone(),
                resource_cancellation_proof(&queued_identity, &spec(), ResourceRoutePhase::Waiting),
            )
            .unwrap();
        assert_eq!(
            queued_receipt.outcome,
            crate::submission::ResourceCancellationOutcome::CancelledBeforeLaunch
        );
        assert!(matches!(
            store.resource_requests(authority, resource.id).unwrap()[0].state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert_eq!(
            store
                .cancel_resource_request_with_receipt(
                    authority,
                    queued_identity.clone(),
                    resource_cancellation_proof(
                        &queued_identity,
                        &spec(),
                        ResourceRoutePhase::Waiting,
                    ),
                )
                .unwrap(),
            queued_receipt
        );

        let assigned_directory = tempdir().unwrap();
        let mut assigned_store = Store::open(&assigned_directory.path().join("db")).unwrap();
        let assigned_authority = MachineId::new();
        let background_task = TaskId::new();
        let (assigned_resource, _, _) =
            open_release_for_test(&mut assigned_store, assigned_authority, background_task);
        let assigned = assigned_store
            .resource_requests(assigned_authority, assigned_resource.id)
            .unwrap()[0]
            .clone();
        let next_request_id = RequestId::new();
        let next_task = TaskId::new();
        assigned_store
            .accept_resource_request(
                assigned_authority,
                next_request_id,
                next_task,
                assigned_resource.id,
                MachineId::new(),
                spec(),
            )
            .unwrap();
        let (loan, _) = assigned_store
            .seed_serving_loan_for_test(
                assigned_authority,
                assigned_resource.id,
                assigned.request_id,
                ReturnContext::Stopped {
                    task_id: background_task,
                    checkpoint_ref: "checkpoint-1".into(),
                    recovery_ref: "recovery-1".into(),
                },
            )
            .unwrap();
        let assigned_identity = resource_cancellation_identity(
            Uuid::now_v7(),
            assigned.request_id,
            assigned.task_id,
            assigned_resource.id,
            assigned.origin_machine,
            assigned_authority,
            ResourceRoutePhase::Waiting,
        );
        let assigned_receipt = assigned_store
            .cancel_resource_request_with_receipt(
                assigned_authority,
                assigned_identity.clone(),
                resource_cancellation_proof(
                    &assigned_identity,
                    &spec(),
                    ResourceRoutePhase::Waiting,
                ),
            )
            .unwrap();
        assert_eq!(
            assigned_receipt.outcome,
            crate::submission::ResourceCancellationOutcome::CancelledBeforeLaunch
        );
        let requests = assigned_store
            .resource_requests(assigned_authority, assigned_resource.id)
            .unwrap();
        assert!(matches!(
            requests[0].state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert!(matches!(
            requests[1].state,
            ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
        ));
        assert_eq!(
            assigned_store
                .cancel_resource_request_with_receipt(
                    assigned_authority,
                    assigned_identity.clone(),
                    resource_cancellation_proof(
                        &assigned_identity,
                        &spec(),
                        ResourceRoutePhase::Waiting,
                    ),
                )
                .unwrap(),
            assigned_receipt
        );
    }

    #[test]
    fn resource_cancellation_of_a_terminal_request_returns_typed_attention_receipt() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let request = RequestId::new();
        let task = TaskId::new();
        store
            .accept_resource_request(authority, request, task, resource.id, origin, spec())
            .unwrap();
        let terminal_state = ResourceRequestState::Finished {
            outcome: ExitReason::Cancelled,
        };
        store
            .conn
            .execute(
                "UPDATE resource_requests SET state_json=?1 WHERE request_id=?2",
                params![
                    serde_json::to_string(&terminal_state).unwrap(),
                    request.0.to_string(),
                ],
            )
            .unwrap();

        let identity = resource_cancellation_identity(
            Uuid::now_v7(),
            request,
            task,
            resource.id,
            origin,
            authority,
            ResourceRoutePhase::Waiting,
        );
        let receipt = store
            .cancel_resource_request_with_receipt(
                authority,
                identity.clone(),
                resource_cancellation_proof(&identity, &spec(), ResourceRoutePhase::Waiting),
            )
            .unwrap();
        assert_eq!(
            receipt.outcome,
            crate::submission::ResourceCancellationOutcome::NotEligible {
                reason: crate::submission::ResourceCancellationIneligibleReason::Terminal,
            }
        );
        assert!(matches!(
            store.resource_requests(authority, resource.id).unwrap()[0].state,
            ResourceRequestState::Finished { .. }
        ));
        assert_eq!(
            store.resource_cancellation_receipt(&identity).unwrap(),
            Some(receipt)
        );
    }

    #[test]
    fn activated_route_race_returns_attention_without_cancelling_assigned_work() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, _) = open_release_for_test(&mut store, authority, background_task);
        let assigned = store.resource_requests(authority, resource.id).unwrap()[0].clone();
        let queued_request = RequestId::new();
        let queued_task = TaskId::new();
        store
            .accept_resource_request(
                authority,
                queued_request,
                queued_task,
                resource.id,
                MachineId::new(),
                spec(),
            )
            .unwrap();
        let (loan, _) = store
            .seed_serving_loan_for_test(
                authority,
                resource.id,
                assigned.request_id,
                ReturnContext::Stopped {
                    task_id: background_task,
                    checkpoint_ref: "checkpoint-1".into(),
                    recovery_ref: "recovery-1".into(),
                },
            )
            .unwrap();

        let identity = resource_cancellation_identity(
            Uuid::now_v7(),
            assigned.request_id,
            assigned.task_id,
            resource.id,
            assigned.origin_machine,
            authority,
            ResourceRoutePhase::Waiting,
        );
        let receipt = store
            .cancel_resource_request_with_receipt(
                authority,
                identity.clone(),
                resource_cancellation_proof(&identity, &spec(), ResourceRoutePhase::Activated),
            )
            .unwrap();
        assert_eq!(
            receipt.outcome,
            crate::submission::ResourceCancellationOutcome::NotEligible {
                reason: crate::submission::ResourceCancellationIneligibleReason::Activated,
            }
        );
        let requests = store.resource_requests(authority, resource.id).unwrap();
        assert!(matches!(
            requests[0].state,
            ResourceRequestState::Assigned { loan_id: assigned_loan }
                if assigned_loan == loan.id
        ));
        assert!(matches!(requests[1].state, ResourceRequestState::Queued));
        assert_eq!(
            store.resource_cancellation_receipt(&identity).unwrap(),
            Some(receipt)
        );
    }

    #[test]
    fn cancellation_before_acceptance_fences_delayed_remote_task_acceptance() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let request = RequestId::new();
        let task = TaskId::new();

        assert!(matches!(
            store
                .cancel_resource_request_before_activation(
                    authority,
                    request,
                    task,
                    resource.id,
                    origin,
                )
                .unwrap(),
            QueueCancellationResult::PreventedBeforeAcceptance
        ));
        assert_eq!(prevention_count(&store), 1);
        assert_cancelled_tombstone(
            store.executor_identity(task).unwrap().unwrap(),
            task,
            origin,
            authority,
        );
        assert!(store.origin_route_by_task(task).unwrap().is_none());

        let execution = store
            .insert_remote_task(&remote_task(task, &spec()), &spec(), origin, authority)
            .unwrap();
        assert_cancelled_tombstone(execution, task, origin, authority);
        assert!(store.get_task(task).unwrap().is_none());
        assert_eq!(identity_count(&store, task), 1);

        assert!(matches!(
            store
                .cancel_resource_request_before_activation(
                    authority,
                    request,
                    task,
                    resource.id,
                    origin,
                )
                .unwrap(),
            QueueCancellationResult::PreventedBeforeAcceptance
        ));
        assert_eq!(prevention_count(&store), 1);
        assert_eq!(identity_count(&store, task), 1);
    }

    #[test]
    fn cancellation_after_queue_acceptance_is_atomic_and_idempotent() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let request = RequestId::new();
        let task = TaskId::new();
        let accepted = store
            .accept_resource_request(authority, request, task, resource.id, origin, spec())
            .unwrap();
        assert!(store.origin_route_by_task(task).unwrap().is_none());
        assert_eq!(
            store
                .oldest_queued_resource_request(authority, resource.id)
                .unwrap()
                .unwrap()
                .request_id,
            request
        );

        let cancelled = store
            .cancel_resource_request_before_activation(
                authority,
                request,
                task,
                resource.id,
                origin,
            )
            .unwrap();
        let QueueCancellationResult::Request(cancelled) = cancelled else {
            panic!("accepted cancellation must return its saved request");
        };
        assert_eq!(cancelled.acceptance_sequence, accepted.acceptance_sequence);
        assert!(matches!(
            cancelled.state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert!(
            store
                .oldest_queued_resource_request(authority, resource.id)
                .unwrap()
                .is_none()
        );
        assert_cancelled_tombstone(
            store.executor_identity(task).unwrap().unwrap(),
            task,
            origin,
            authority,
        );

        let retry = store
            .cancel_resource_request_before_activation(
                authority,
                request,
                task,
                resource.id,
                origin,
            )
            .unwrap();
        let QueueCancellationResult::Request(retry) = retry else {
            panic!("repeated cancellation must return the saved request");
        };
        assert_eq!(retry.acceptance_sequence, accepted.acceptance_sequence);
        assert!(matches!(
            retry.state,
            ResourceRequestState::CancelledBeforeLaunch
        ));

        let delayed_acceptance = store
            .insert_remote_task(&remote_task(task, &spec()), &spec(), origin, authority)
            .unwrap();
        assert_cancelled_tombstone(delayed_acceptance, task, origin, authority);
        assert!(store.get_task(task).unwrap().is_none());
        assert_eq!(identity_count(&store, task), 1);
    }

    #[test]
    fn concurrent_direct_acceptance_cannot_claim_a_resource_queue_id() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("db");
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        let request = RequestId::new();
        let task = TaskId::new();
        let barrier = Arc::new(Barrier::new(2));

        let mut setup = Store::open(&path).unwrap();
        setup.register_resource(authority, &resource).unwrap();
        setup
            .accept_resource_request(authority, request, task, resource.id, origin, spec())
            .unwrap();
        drop(setup);

        let accept_path = path.clone();
        let accept_barrier = barrier.clone();
        let spec_for_acceptance = spec();
        let acceptance = std::thread::spawn(move || {
            let mut store = Store::open(&accept_path).unwrap();
            accept_barrier.wait();
            store.accept_execution(&ExecutionRecord {
                task,
                origin_machine: origin,
                execution_machine: authority,
                spec: spec_for_acceptance.into(),
                state: crate::domain::ProcessStatus::Queued,
            })
        });

        let mut cancel_store = Store::open(&path).unwrap();
        barrier.wait();
        let cancellation = cancel_store.cancel_resource_request_before_activation(
            authority,
            request,
            task,
            resource.id,
            origin,
        );
        let execution_identity = acceptance.join().unwrap();

        let final_store = Store::open(&path).unwrap();
        let saved = final_store.executor_identity(task).unwrap().unwrap();
        let tombstone = match execution_identity {
            Err(IdentityError::Conflict) => match saved {
                ExecutorIdentity::Rejected(tombstone) => tombstone,
                identity => panic!("resource cancellation must retain a rejection: {identity:?}"),
            },
            Ok(ExecutorIdentity::Rejected(tombstone)) => tombstone,
            outcome => panic!("ordinary acceptance must not win this race: {outcome:?}"),
        };
        assert_eq!(tombstone.task, task);
        assert_eq!(tombstone.origin_machine, origin);
        assert_eq!(tombstone.execution_machine, authority);
        assert!(matches!(
            cancellation,
            Ok(QueueCancellationResult::Request(cancelled))
                if matches!(cancelled.state, ResourceRequestState::CancelledBeforeLaunch)
        ));
        assert_eq!(prevention_count(&final_store), 0);
    }

    #[test]
    fn tombstone_insert_failure_rolls_back_queued_request_cancellation() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let request = RequestId::new();
        let task = TaskId::new();
        store
            .accept_resource_request(authority, request, task, resource.id, origin, spec())
            .unwrap();
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER reject_executor_tombstone
                 BEFORE INSERT ON executor_identities
                 BEGIN SELECT RAISE(ABORT, 'forced tombstone failure'); END;",
            )
            .unwrap();

        assert!(
            store
                .cancel_resource_request_before_activation(
                    authority,
                    request,
                    task,
                    resource.id,
                    origin,
                )
                .is_err()
        );
        let requests = store.resource_requests(authority, resource.id).unwrap();
        assert_eq!(requests.len(), 1);
        assert!(matches!(requests[0].state, ResourceRequestState::Queued));
        assert_eq!(identity_count(&store, task), 0);
        assert!(
            store
                .oldest_queued_resource_request(authority, resource.id)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn terminal_cancellation_does_not_create_executor_tombstones() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let terminal = store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                origin,
                spec(),
            )
            .unwrap();
        let terminal_json = serde_json::to_string(&ResourceRequestState::Finished {
            outcome: crate::domain::ExitReason::Cancelled,
        })
        .unwrap();
        store
            .conn
            .execute(
                "UPDATE resource_requests SET state_json=?1 WHERE request_id=?2",
                rusqlite::params![terminal_json, terminal.request_id.0.to_string()],
            )
            .unwrap();

        let result = store
            .cancel_resource_request_before_activation(
                authority,
                terminal.request_id,
                terminal.task_id,
                resource.id,
                origin,
            )
            .unwrap();
        let QueueCancellationResult::Request(saved) = result else {
            panic!("terminal cancellation must return its saved state");
        };
        assert!(matches!(
            saved.state,
            ResourceRequestState::Finished {
                outcome: crate::domain::ExitReason::Cancelled
            }
        ));
        assert_eq!(identity_count(&store, terminal.task_id), 0);
    }

    mod release_watcher;
}
