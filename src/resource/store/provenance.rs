//! Verification of the release provenance saved on a Serving loan

use rusqlite::{Connection, OptionalExtension};

use super::codec::stored_json;
use super::error::ResourceStoreError;
use super::release_completion::{ReleaseCompletionReceipt, ReleaseCompletionResult};
use crate::domain::{ProcessGroupExitEvidence, ProcessStatus, TaskState};
use crate::machine::MachineId;
use crate::resource::{
    Loan, LoanPhase, LoanState, ReleaseCheckpointPhase, ReleaseCheckpointState, Resource,
    ResourceRequestState, ResourceRevision, ReturnContext, ServingReleaseProvenance,
};
use crate::submission::{ExecutorIdentity, normalized_spec_sha256};

/// Whether saved authority evidence still proves the release provenance of a Serving loan
pub(super) fn serving_release_provenance_matches(
    conn: &Connection,
    authority: MachineId,
    resource: &Resource,
    loan: &Loan,
    return_context: &ReturnContext,
    provenance: &ServingReleaseProvenance,
) -> Result<bool, ResourceStoreError> {
    let (action_id, task_id) = match provenance {
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
    let receipt: ReleaseCompletionReceipt =
        stored_json("release completion receipt", &receipt_json)?;
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
        || receipt.expected_state_revision.next() != Some(state_revision)
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
        ServingReleaseProvenance::IdleBoundary { .. }
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
        serde_json::from_str::<crate::resource::TrainerAttemptAssociationProof>(&association_json)
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
