//! Seeding helpers for tests that need authority-local resource rows in exact states

use std::path::Path;

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::OperatorGpuFreeError;
use super::operator_release::saved_receipt_on;
use super::select_authority_resource;
use crate::domain::ProcessGroupExitEvidence;
use crate::domain::TaskId;
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::operator_release::{OperatorAttestationId, OperatorGpuFreeReceipt};
use crate::resource::store::{
    CompleteReleaseError, ConflictReason, OpenReleaseLoanError, OpenReleaseLoanResult,
    QueueCancellationResult, ResourceStoreError, SupervisorNoticeStoreError,
    cancel_request_before_activation_on, next_queued_request_for_authority, select_non_closed_loan,
};
use crate::resource::{
    ActionId, AssignmentRevision, Loan, LoanId, LoanPhase, LoanState, NoticeId, ResourceId,
    ResourceRequest, ResourceRequestState, ResourceRevision, ReturnContext,
    ServingReleaseProvenance, SupervisorAddress, SupervisorNotice,
};
use crate::store::Store;
use crate::submission::RequestId;

/// Serving provenance that no saved completion receipt proves
///
/// A loan that carries it stays Serving, but activation refuses it
pub(crate) fn unreceipted_release_provenance() -> ServingReleaseProvenance {
    ServingReleaseProvenance::CompletedTrainerResult {
        action_id: ActionId::new(),
        task_id: TaskId::new(),
        publication_sha256: "ab".repeat(32).parse().expect("fixture digest"),
    }
}

/// Model activation winning after an API control receipt commits
pub(crate) fn mark_request_assigned_for_race(db_path: &Path, request_id: RequestId) {
    let store = Store::open(db_path).expect("open race fixture store");
    let assigned = serde_json::to_string(&ResourceRequestState::Assigned {
        loan_id: LoanId::new(),
    })
    .expect("encode assigned fixture");
    store
        .conn
        .execute(
            "UPDATE resource_requests SET state_json = ?1 WHERE request_id = ?2",
            rusqlite::params![assigned, request_id.0.to_string()],
        )
        .expect("advance request in race fixture");
}

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
        let expected_state_revision =
            ResourceRevision::new(state_revision.get().checked_sub(1).ok_or(
                ResourceStoreError::Conflict(ConflictReason::ResourceRevisionChanged),
            )?);
        loan = crate::resource::store::test_support::seed_verified_serving_provenance_for_test(
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
        let resource = select_authority_resource(&tx, authority_machine, resource_id)?;

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
            return Err(ResourceStoreError::Conflict(ConflictReason::RequestStateChanged).into());
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
            return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged).into());
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
                    release_provenance: unreceipted_release_provenance(),
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
            return Err(
                ResourceStoreError::Conflict(ConflictReason::ResourceRevisionChanged).into(),
            );
        }
        tx.commit()?;

        Ok((loan, next_revision))
    }

    /// Read process-group evidence for one exact task identity
    pub(crate) fn process_group_exit_evidence(
        &self,
        id: TaskId,
    ) -> Result<Option<ProcessGroupExitEvidence>, AppError> {
        Ok(self
            .get_task(id)?
            .map(|row| row.process_group_exit_evidence()))
    }

    /// Read the next queued request for one resource
    pub(crate) fn next_queued_resource_request(
        &self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<Option<ResourceRequest>, ResourceStoreError> {
        next_queued_request_for_authority(&self.conn, authority_machine, resource_id)
    }

    /// Cancel a queued request or atomically retain prevention before acceptance
    pub(crate) fn cancel_resource_request_before_activation(
        &mut self,
        authority_machine: MachineId,
        request_id: RequestId,
        task_id: TaskId,
        resource_id: ResourceId,
        origin_machine: MachineId,
    ) -> Result<QueueCancellationResult, ResourceStoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = cancel_request_before_activation_on(
            &tx,
            authority_machine,
            request_id,
            task_id,
            resource_id,
            origin_machine,
        )?;
        tx.commit()?;
        Ok(result)
    }

    /// Open or reuse the authority's release loan
    pub(crate) fn open_release_loan_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        expected_state_revision: ResourceRevision,
    ) -> Result<OpenReleaseLoanResult, OpenReleaseLoanError> {
        crate::resource::store::test_support::open_release_loan_for_authority(
            &mut self.conn,
            authority_machine,
            resource_id,
            expected_state_revision,
        )
    }

    /// Retarget an undelivered notice with an assignment-revision compare-and-set
    pub(crate) fn retarget_supervisor_notice(
        &mut self,
        notice_id: NoticeId,
        expected_assignment_revision: AssignmentRevision,
        destination: SupervisorAddress,
        new_assignment_revision: AssignmentRevision,
    ) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
        crate::resource::store::test_support::retarget_supervisor_notice(
            &mut self.conn,
            notice_id,
            expected_assignment_revision,
            destination,
            new_assignment_revision,
        )
    }

    /// Read the saved receipt of one operator attestation on this authority
    pub(crate) fn operator_attestation_receipt_for_authority(
        &self,
        authority_machine: MachineId,
        operation_id: OperatorAttestationId,
    ) -> Result<Option<OperatorGpuFreeReceipt>, OperatorGpuFreeError> {
        Ok(saved_receipt_on(&self.conn, operation_id)?
            .filter(|receipt| receipt.attestation.authority_machine == authority_machine))
    }
}
