//! Cancellation of resource requests before task activation

use rusqlite::{Transaction, params};

use super::codec::{encode_json, sqlite_integer};
use super::error::{ConflictReason, ResourceStoreError};
use super::notice::{SupervisorNoticeStoreError, insert_supervisor_notice_in_transaction};
use super::queue::{
    RequestIdentity, local_task_exists, next_queued_request_for_authority, prevention_exists,
    request_matches_identity, select_executor_identity, task_id_exists,
};
use super::revision::swap_resource_revision;
use super::rows::{
    check_resource_authority, select_non_closed_loan, select_request_by_id, select_resource,
};
use crate::domain::TaskId;
use crate::machine::MachineId;
use crate::resource::{
    ActionId, Loan, LoanId, LoanPhase, LoanState, NoticeId, ResourceId, ResourceRequest,
    ResourceRequestState, SupervisorNotice, SupervisorNoticeDelivery, SupervisorNoticePayload,
};
use crate::store::Store;
use crate::submission::{ExecutorIdentity, PreAcceptanceRejection, RejectionTombstone, RequestId};

/// Result of cancelling a resource request before task activation
#[derive(Debug, Clone)]
pub enum QueueCancellationResult {
    /// The exact request identity is retained to prevent delayed acceptance
    PreventedBeforeAcceptance,
    /// The saved request state after cancellation or a terminal-state retry
    Request(Box<ResourceRequest>),
}

/// Cancel a request before activation inside a caller-owned transaction
///
/// Nothing is committed here, so the caller can save its own record atomically
pub(crate) fn cancel_request_before_activation_on(
    tx: &Transaction<'_>,
    authority_machine: MachineId,
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

    if let Some(mut saved) = select_request_by_id(tx, request_id)? {
        if !request_matches_identity(&saved, identity) {
            return Err(ResourceStoreError::Conflict(
                ConflictReason::RequestIdentityMismatch,
            ));
        }
        check_resource_authority(tx, saved.resource_id, authority_machine)?;

        match saved.state.clone() {
            ResourceRequestState::Queued => {
                cancel_queued_request(tx, &mut saved, authority_machine)?;
            }
            ResourceRequestState::Assigned { loan_id } => {
                cancel_assigned_request(tx, &mut saved, loan_id, authority_machine)?;
            }
            _ => {}
        }

        return Ok(QueueCancellationResult::Request(Box::new(saved)));
    }

    if task_id_exists(tx, task_id)? {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::TaskIdentityInUse,
        ));
    }

    let prevented = prevention_exists(tx, identity)?;
    check_resource_authority(tx, resource_id, authority_machine)?;
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
        tx,
        &cancellation_tombstone(task_id, origin_machine, authority_machine),
    )?;
    if matches!(executor_identity, ExecutorIdentity::Accepted(_)) {
        return Err(ResourceStoreError::ExecutorAlreadyAccepted { task: task_id });
    }
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
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestStateChanged,
        ));
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
        return Err(ResourceStoreError::Conflict(
            ConflictReason::TaskIdentityInUse,
        ));
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
    let loan = select_non_closed_loan(tx, saved.resource_id)?
        .ok_or(ResourceStoreError::Conflict(ConflictReason::LoanChanged))?;
    if loan.id != loan_id || loan.resource_id != saved.resource_id {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
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
        _ => return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged)),
    };

    let next_revision = resource
        .state_revision
        .next()
        .ok_or(ResourceStoreError::Conflict(
            ConflictReason::RevisionExhausted,
        ))?;
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
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestStateChanged,
        ));
    }
    saved.state = cancelled;

    let updated_loan = if let Some(mut next_request) =
        next_queued_request_for_authority(tx, authority, saved.resource_id)?
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
            return Err(ResourceStoreError::Conflict(
                ConflictReason::RequestStateChanged,
            ));
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
            SupervisorNoticeStoreError::CorruptRecord { what, reason } => {
                ResourceStoreError::CorruptRecord { what, reason }
            }
            _ => ResourceStoreError::Conflict(ConflictReason::SupervisorNoticeRejected),
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
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    }

    if !swap_resource_revision::<ResourceStoreError>(
        tx,
        authority,
        saved.resource_id,
        resource.state_revision,
        next_revision,
    )? {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ResourceRevisionChanged,
        ));
    }

    Ok(())
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
