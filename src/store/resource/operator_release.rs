//! Authority-owned operator attestation for an ended trainer with no release proof
//!
//! One IMMEDIATE transaction checks the exact attestation against the current
//! resource revision, registration, loan, task row, and the resource launch
//! that registered the task. It then saves one receipt with the evidence
//! snapshot and commits the queue or loan transition that follows. A refusal
//! writes nothing. An exact retry returns the saved receipt without these
//! checks, so a later transition cannot turn it into a refusal
//!
//! With an AwaitingRelease loan the release action closes into Serving or
//! AwaitingReturn and keeps the return obligation, as a proven release would.
//! With no loan the registration clears. The oldest queued request then serves
//! from an idle loan in the same transaction, or the receipt becomes the saved
//! idle boundary that a later reconciliation or first launch reads

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use super::background::{
    AdvanceError, advance_resource_on, first_launch_of_registered_trainer_on,
    latest_launch_request_on, latest_loan_on, open_idle_serving_loan_on,
    pending_background_launch_on,
};
use super::restore::direct_segment_return_of_registered_trainer_on;
use crate::domain::{TaskId, TaskState};
use crate::machine::MachineId;
use crate::resource::operator_release::{
    AttestedTrainerAssociation, AttestedTrainerEnd, AttestedTrainerLaunch, OperatorAttestationId,
    OperatorGpuFreeAttestation, OperatorGpuFreeEvidence, OperatorGpuFreeOutcome,
    OperatorGpuFreeReceipt, OperatorGpuFreeRefusal, OperatorGpuFreeResolution,
    OperatorStateBinding,
};
use crate::resource::store::{
    ResourceStoreError, SupervisorNoticeStoreError, insert_supervisor_notice_in_transaction,
    oldest_queued_request_for_authority, select_non_closed_loan, select_resource,
    select_supervisor_notice_record_by_action,
};
use crate::resource::{
    ActionId, IdleBoundaryProof, Loan, LoanId, LoanPhase, LoanState, NoticeId, Resource,
    ResourceRequestState, ResourceRevision, ReturnContext, ServingReleaseProvenance,
    SupervisorNotice, SupervisorNoticeDelivery, SupervisorNoticePayload,
};
use crate::store::{BackgroundLaunchError, ReturnDecisionError, Store};
use crate::submission::RequestId;

/// Why an operator attestation was refused or could not be evaluated
#[derive(Debug, thiserror::Error)]
pub(crate) enum OperatorGpuFreeError {
    /// The authority refused the attestation and wrote nothing
    #[error(transparent)]
    Refused(#[from] OperatorGpuFreeRefusal),
    /// Resource storage failed or stored data is invalid
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// A durable supervisor notice could not be saved
    #[error(transparent)]
    Notice(#[from] SupervisorNoticeStoreError),
    /// The first background launch records could not be read
    #[error(transparent)]
    Launch(#[from] BackgroundLaunchError),
    /// The return decision records could not be read
    #[error(transparent)]
    Return(#[from] ReturnDecisionError),
    /// A task row could not be read
    #[error(transparent)]
    Task(#[from] crate::error::AppError),
    /// A receipt could not be encoded or decoded
    #[error("operator attestation encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    /// A concurrent change invalidated a compare-and-set update
    #[error("resource state changed during the operator attestation")]
    Changed,
    /// SQLite failed
    #[error("operator attestation storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Saved attestation that is the current idle boundary of its resource
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OperatorIdleBoundary {
    /// Attestation whose receipt holds the observation and evidence
    pub(crate) operation_id: OperatorAttestationId,
    /// Registered trainer that the attestation cleared
    pub(crate) task_id: TaskId,
    /// Latest first background launch when the attestation committed
    pub(crate) preceding_launch: Option<RequestId>,
}

impl Store {
    /// Commit one operator attestation for an ended trainer and its queue or loan transition
    ///
    /// `authority_machine` is the local daemon machine. The attestation must name
    /// it, and it must be the resource authority
    pub(crate) fn attest_trainer_gpu_free_for_authority(
        &mut self,
        authority_machine: MachineId,
        attestation: OperatorGpuFreeAttestation,
    ) -> Result<OperatorGpuFreeResolution, OperatorGpuFreeError> {
        attest_trainer_gpu_free(&mut self.conn, authority_machine, attestation)
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

fn attest_trainer_gpu_free(
    conn: &mut Connection,
    authority_machine: MachineId,
    attestation: OperatorGpuFreeAttestation,
) -> Result<OperatorGpuFreeResolution, OperatorGpuFreeError> {
    attestation.validate()?;
    if attestation.authority_machine != authority_machine {
        return Err(OperatorGpuFreeRefusal::WrongAuthority {
            expected: authority_machine,
            found: attestation.authority_machine,
        }
        .into());
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(receipt) = saved_receipt_on(&tx, attestation.operation_id)? {
        if receipt.attestation != attestation {
            return Err(OperatorGpuFreeRefusal::ConflictingRetry {
                operation_id: attestation.operation_id,
            }
            .into());
        }
        tx.commit()?;
        return Ok(OperatorGpuFreeResolution {
            receipt,
            replayed: true,
        });
    }

    let resource = current_resource(&tx, &attestation)?;
    let release = bound_release_action(&tx, &resource, &attestation)?;
    let evidence = trainer_evidence(&tx, &resource, attestation.task_id)?;
    let preceding_loan = latest_loan_on(&tx, resource.id)?.map(|loan| loan.id);
    let preceding_launch = latest_launch_request_on(&tx, resource.id)?;

    let (state_revision, outcome) = match release {
        Some((loan, action_id)) => {
            resolve_release_action(&tx, &resource, &attestation, &evidence, loan, action_id)?
        }
        None => resolve_idle(&tx, &resource, &attestation)?,
    };
    let receipt = OperatorGpuFreeReceipt {
        attestation,
        evidence,
        state_revision,
        outcome,
    };
    tx.execute(
        "INSERT INTO resource_operator_attestations (
            operation_id, resource_id, task_id, preceding_loan, preceding_launch, receipt_json
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            receipt.attestation.operation_id.as_uuid().to_string(),
            resource.id.as_uuid().to_string(),
            receipt.attestation.task_id.to_string(),
            preceding_loan.map(|loan| loan.as_uuid().to_string()),
            preceding_launch.map(|request| request.0.to_string()),
            serde_json::to_string(&receipt)?,
        ],
    )?;
    tx.commit()?;

    Ok(OperatorGpuFreeResolution {
        receipt,
        replayed: false,
    })
}

/// Read the resource and compare its authority, revision, and registration
fn current_resource(
    conn: &Connection,
    attestation: &OperatorGpuFreeAttestation,
) -> Result<Resource, OperatorGpuFreeError> {
    let resource = select_resource(conn, attestation.resource_id)?
        .ok_or(OperatorGpuFreeRefusal::ResourceNotFound)?;
    if resource.authority_machine() != attestation.authority_machine {
        return Err(OperatorGpuFreeRefusal::WrongAuthority {
            expected: resource.authority_machine(),
            found: attestation.authority_machine,
        }
        .into());
    }
    if resource.state_revision != attestation.expected_state_revision {
        return Err(OperatorGpuFreeRefusal::StaleRevision {
            expected: attestation.expected_state_revision,
            actual: resource.state_revision,
        }
        .into());
    }
    if resource.registered_background_task != Some(attestation.task_id) {
        return Err(OperatorGpuFreeRefusal::NotRegisteredTrainer {
            task_id: attestation.task_id,
            registered: resource.registered_background_task,
        }
        .into());
    }

    Ok(resource)
}

/// Compare the named state binding with the current loan
///
/// Returns the AwaitingRelease loan and its action, or `None` for no loan. Any
/// later phase can already run other GPU work, so it is never overridden
fn bound_release_action(
    conn: &Connection,
    resource: &Resource,
    attestation: &OperatorGpuFreeAttestation,
) -> Result<Option<(Loan, ActionId)>, OperatorGpuFreeError> {
    let task_id = attestation.task_id;
    let current = select_non_closed_loan(conn, resource.id)?;
    let (loan, loan_id, action_id) = match (attestation.state_binding, current) {
        (OperatorStateBinding::NoLoan, None) => {
            // a pending first launch would replace this registration once it starts
            if pending_background_launch_on(conn, resource)?.is_some() {
                return Err(OperatorGpuFreeRefusal::InconsistentHistory { task_id }.into());
            }
            return Ok(None);
        }
        (OperatorStateBinding::AwaitingRelease { loan_id, action_id }, Some(loan))
            if loan.id == loan_id =>
        {
            (loan, loan_id, action_id)
        }
        (_, current) => {
            return Err(OperatorGpuFreeRefusal::LoanStateChanged {
                current_loan: current.map(|loan| loan.id),
            }
            .into());
        }
    };

    let LoanState::Active {
        phase:
            LoanPhase::AwaitingRelease {
                action_id: saved_action,
                observed_background_task,
                ..
            },
    } = &loan.state
    else {
        return Err(OperatorGpuFreeRefusal::LoanNotAwaitingRelease { loan_id }.into());
    };
    if *saved_action != action_id || *observed_background_task != task_id {
        return Err(OperatorGpuFreeRefusal::LoanStateChanged {
            current_loan: Some(loan_id),
        }
        .into());
    }

    let notice = select_supervisor_notice_record_by_action(conn, action_id)?;
    let notice_matches = notice.is_some_and(|(notice, _)| {
        notice.loan_id == loan_id
            && notice.action_id == action_id
            && notice.payload == SupervisorNoticePayload::ReleaseRequired { task_id }
    });
    if !notice_matches {
        return Err(OperatorGpuFreeRefusal::InvalidReleaseNotice { action_id }.into());
    }

    Ok(Some((loan, action_id)))
}

/// Snapshot the task end, registering launch, identity digest, and association
///
/// The task must have ended or been lost. Its accepted identity and callback
/// route must still match the resource launch that registered it
fn trainer_evidence(
    conn: &Connection,
    resource: &Resource,
    task_id: TaskId,
) -> Result<OperatorGpuFreeEvidence, OperatorGpuFreeError> {
    let row = crate::store::task_by_id_on(conn, task_id)?
        .ok_or(OperatorGpuFreeRefusal::TaskMissing { task_id })?;
    let trainer_end = match &row.state {
        TaskState::Queued | TaskState::Running { .. } => {
            return Err(OperatorGpuFreeRefusal::TaskNotEnded {
                task_id,
                state: row.status(),
            }
            .into());
        }
        TaskState::Finished { reason } => AttestedTrainerEnd::Finished {
            outcome: reason.clone(),
            process_group_exit: row.process_group_exit_evidence(),
        },
        TaskState::Lost => AttestedTrainerEnd::Lost,
    };

    let (trainer_launch, normalized_spec_sha256) = if let Some((request_id, digest)) =
        first_launch_of_registered_trainer_on(conn, resource, task_id)?
    {
        (
            AttestedTrainerLaunch::FirstBackgroundLaunch { request_id },
            digest,
        )
    } else if let Some((action_id, request_id, digest)) =
        direct_segment_return_of_registered_trainer_on(conn, resource, task_id)?
    {
        (
            AttestedTrainerLaunch::DirectSegmentReturn {
                action_id,
                request_id,
            },
            digest,
        )
    } else {
        return Err(OperatorGpuFreeRefusal::TrainerLaunchUnproven { task_id }.into());
    };

    let association: Option<(String, Option<String>)> = conn
        .query_row(
            "SELECT resource_id, json_extract(association_json, '$.request_sha256')
             FROM trainer_attempt_associations WHERE task_id = ?1",
            [task_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let trainer_association = match association {
        None => AttestedTrainerAssociation::Missing,
        Some((saved_resource, Some(attempt_request_sha256)))
            if saved_resource == resource.id.as_uuid().to_string() =>
        {
            AttestedTrainerAssociation::Saved {
                attempt_request_sha256,
            }
        }
        Some(_) => {
            return Err(OperatorGpuFreeRefusal::InconsistentHistory { task_id }.into());
        }
    };

    Ok(OperatorGpuFreeEvidence {
        trainer_end,
        trainer_launch,
        normalized_spec_sha256,
        trainer_association,
    })
}

/// Return context of an attested trainer
///
/// The run has no result or checkpoint evidence, so it is never a completed or
/// stopped context and never permits a same-run resume
fn attested_return_context(task_id: TaskId, evidence: &OperatorGpuFreeEvidence) -> ReturnContext {
    match &evidence.trainer_end {
        AttestedTrainerEnd::Finished { outcome, .. } => ReturnContext::EndedWithoutResult {
            task_id,
            outcome: outcome.clone(),
        },
        AttestedTrainerEnd::Lost => ReturnContext::LostWithoutResult { task_id },
    }
}

/// Close the release action and select the oldest request or reserve the return decision
///
/// The trainer stays registered until the supervisor decides the return, as
/// after a proven release, so the return obligation is kept
fn resolve_release_action(
    tx: &Transaction<'_>,
    resource: &Resource,
    attestation: &OperatorGpuFreeAttestation,
    evidence: &OperatorGpuFreeEvidence,
    loan: Loan,
    action_id: ActionId,
) -> Result<(ResourceRevision, OperatorGpuFreeOutcome), OperatorGpuFreeError> {
    let return_context = attested_return_context(attestation.task_id, evidence);
    let authority = resource.authority_machine();
    let queued = oldest_queued_request_for_authority(tx, authority, resource.id)?;
    let state_revision = advance(tx, resource, resource.registered_background_task)?;

    let Some(mut request) = queued else {
        let return_action = ActionId::new();
        let loan = Loan {
            id: loan.id,
            resource_id: resource.id,
            state: LoanState::Active {
                phase: LoanPhase::AwaitingReturn {
                    action_id: return_action,
                    return_context: return_context.clone(),
                },
            },
        };
        update_awaiting_release_loan(tx, &loan, action_id)?;
        let notice = insert_supervisor_notice_in_transaction(
            tx,
            &SupervisorNotice {
                id: NoticeId::new(),
                loan_id: loan.id,
                action_id: return_action,
                state_revision,
                destination: resource.supervisor,
                assignment_revision: resource.assignment_revision,
                payload: SupervisorNoticePayload::ReturnRequired { return_context },
                delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
            },
        )?;
        return Ok((
            state_revision,
            OperatorGpuFreeOutcome::ReleaseResolvedReturnRequired { loan, notice },
        ));
    };

    assign_request(tx, &mut request, loan.id)?;
    let loan = Loan {
        id: loan.id,
        resource_id: resource.id,
        state: LoanState::Active {
            phase: LoanPhase::Serving {
                return_context,
                current_request_id: request.request_id,
                release_provenance: ServingReleaseProvenance::OperatorAttestedGpuFree {
                    operation_id: attestation.operation_id,
                    action_id,
                    task_id: attestation.task_id,
                },
            },
        },
    };
    update_awaiting_release_loan(tx, &loan, action_id)?;

    Ok((
        state_revision,
        OperatorGpuFreeOutcome::ReleaseResolvedServing { loan, request },
    ))
}

/// Clear the registration and serve the oldest request, or keep this receipt as the boundary
///
/// No committed state shows a free resource between the two steps, because both
/// commit in the caller's transaction
fn resolve_idle(
    tx: &Transaction<'_>,
    resource: &Resource,
    attestation: &OperatorGpuFreeAttestation,
) -> Result<(ResourceRevision, OperatorGpuFreeOutcome), OperatorGpuFreeError> {
    let authority = resource.authority_machine();
    let cleared_revision = advance(tx, resource, None)?;
    let Some(request) = oldest_queued_request_for_authority(tx, authority, resource.id)? else {
        return Ok((cleared_revision, OperatorGpuFreeOutcome::IdleBoundary));
    };

    let mut cleared = resource.clone();
    cleared.state_revision = cleared_revision;
    cleared.registered_background_task = None;
    let proof = IdleBoundaryProof::OperatorAttestedGpuFree {
        operation_id: attestation.operation_id,
        task_id: attestation.task_id,
    };
    let (loan, request) = open_idle_serving_loan_on(tx, authority, &cleared, request, proof)?;
    let state_revision = select_resource(tx, resource.id)?
        .ok_or(ResourceStoreError::ResourceNotFound)?
        .state_revision;

    Ok((
        state_revision,
        OperatorGpuFreeOutcome::IdleServing { loan, request },
    ))
}

fn advance(
    tx: &Transaction<'_>,
    resource: &Resource,
    registered_background_task: Option<TaskId>,
) -> Result<ResourceRevision, OperatorGpuFreeError> {
    advance_resource_on(tx, resource, registered_background_task).map_err(|error| match error {
        AdvanceError::Exhausted => OperatorGpuFreeRefusal::RevisionExhausted {
            revision: resource.state_revision,
        }
        .into(),
        AdvanceError::Changed => OperatorGpuFreeError::Changed,
        AdvanceError::Storage(error) => OperatorGpuFreeError::Storage(error),
    })
}

fn assign_request(
    tx: &Transaction<'_>,
    request: &mut crate::resource::ResourceRequest,
    loan_id: LoanId,
) -> Result<(), OperatorGpuFreeError> {
    request.state = ResourceRequestState::Assigned { loan_id };
    let changed = tx.execute(
        "UPDATE resource_requests SET state_json = ?1
         WHERE request_id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'queued'",
        params![
            serde_json::to_string(&request.state)?,
            request.request_id.0.to_string(),
            request.resource_id.as_uuid().to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(OperatorGpuFreeError::Changed);
    }
    Ok(())
}

fn update_awaiting_release_loan(
    tx: &Transaction<'_>,
    loan: &Loan,
    action_id: ActionId,
) -> Result<(), OperatorGpuFreeError> {
    let changed = tx.execute(
        "UPDATE loans SET state_json = ?1
         WHERE id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'active'
           AND json_extract(state_json, '$.phase.type') = 'awaiting_release'
           AND json_extract(state_json, '$.phase.action_id') = ?4",
        params![
            serde_json::to_string(&loan.state)?,
            loan.id.as_uuid().to_string(),
            loan.resource_id.as_uuid().to_string(),
            action_id.as_uuid().to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(OperatorGpuFreeError::Changed);
    }
    Ok(())
}

fn saved_receipt_on(
    conn: &Connection,
    operation_id: OperatorAttestationId,
) -> Result<Option<OperatorGpuFreeReceipt>, OperatorGpuFreeError> {
    let saved: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_operator_attestations WHERE operation_id = ?1",
            [operation_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(saved.map(|json| serde_json::from_str(&json)).transpose()?)
}

/// Return the saved attestation that is the current idle boundary, if any
///
/// It is current only while no loan and no first background launch was saved
/// after it and the resource is still unregistered. A later record supersedes it
pub(crate) fn current_operator_boundary_on(
    conn: &Connection,
    resource: &Resource,
) -> Result<Option<OperatorIdleBoundary>, ResourceStoreError> {
    let saved: Option<(Option<String>, Option<String>, String)> = conn
        .query_row(
            "SELECT preceding_loan, preceding_launch, receipt_json
             FROM resource_operator_attestations
             WHERE resource_id = ?1
               AND json_extract(receipt_json, '$.outcome.type') = 'idle_boundary'
             ORDER BY rowid DESC LIMIT 1",
            [resource.id.as_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((preceding_loan, preceding_launch, receipt_json)) = saved else {
        return Ok(None);
    };
    let latest_loan = latest_loan_on(conn, resource.id)?.map(|loan| loan.id.as_uuid().to_string());
    let latest_launch = latest_launch_request_on(conn, resource.id)?;
    if latest_loan != preceding_loan
        || latest_launch.map(|request| request.0.to_string()) != preceding_launch
    {
        return Ok(None);
    }

    let receipt: OperatorGpuFreeReceipt =
        serde_json::from_str(&receipt_json).map_err(|_| invalid_receipt())?;
    if receipt.attestation.resource_id != resource.id
        || receipt.attestation.authority_machine != resource.authority_machine()
        || receipt.attestation.state_binding != OperatorStateBinding::NoLoan
        || !matches!(receipt.outcome, OperatorGpuFreeOutcome::IdleBoundary)
        || resource.registered_background_task.is_some()
    {
        return Ok(None);
    }

    Ok(Some(OperatorIdleBoundary {
        operation_id: receipt.attestation.operation_id,
        task_id: receipt.attestation.task_id,
        preceding_launch: latest_launch,
    }))
}

/// Check that an operator-attested Serving provenance matches its saved receipt
///
/// The receipt must name this resource, authority, loan, release action, task,
/// return context, and provenance, and the trainer must still be registered
pub(crate) fn operator_serving_release_matches_on(
    conn: &Connection,
    authority: MachineId,
    resource: &Resource,
    loan: &Loan,
    return_context: &ReturnContext,
    provenance: &ServingReleaseProvenance,
) -> Result<bool, ResourceStoreError> {
    let ServingReleaseProvenance::OperatorAttestedGpuFree {
        operation_id,
        action_id,
        task_id,
    } = provenance
    else {
        return Ok(false);
    };
    if resource.registered_background_task != Some(*task_id) {
        return Ok(false);
    }
    let receipt = saved_receipt_on(conn, *operation_id).map_err(|error| match error {
        OperatorGpuFreeError::Storage(error) => ResourceStoreError::Storage(error),
        _ => invalid_receipt(),
    })?;
    let Some(receipt) = receipt else {
        return Ok(false);
    };
    let attestation = &receipt.attestation;
    let OperatorGpuFreeOutcome::ReleaseResolvedServing {
        loan: committed_loan,
        ..
    } = &receipt.outcome
    else {
        return Ok(false);
    };
    let committed_matches = matches!(
        &committed_loan.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: committed_context,
                release_provenance: committed_provenance,
                ..
            }
        } if committed_context == return_context && committed_provenance == provenance
    );

    Ok(committed_matches
        && committed_loan.id == loan.id
        && loan.resource_id == resource.id
        && attestation.resource_id == resource.id
        && attestation.authority_machine == authority
        && resource.authority_machine() == authority
        && attestation.task_id == *task_id
        && attestation.state_binding
            == (OperatorStateBinding::AwaitingRelease {
                loan_id: loan.id,
                action_id: *action_id,
            })
        && attested_return_context(*task_id, &receipt.evidence) == *return_context)
}

fn invalid_receipt() -> ResourceStoreError {
    ResourceStoreError::Storage(rusqlite::Error::InvalidColumnType(
        0,
        "invalid stored operator attestation receipt".into(),
        rusqlite::types::Type::Text,
    ))
}
