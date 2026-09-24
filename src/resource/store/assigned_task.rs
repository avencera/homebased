//! Proof-gated completion of an accepted resource task

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;

use super::codec::{encode_json, sqlite_integer, stored_json};
use super::error::{ConflictReason, ResourceStoreError};
use super::notice::{SupervisorNoticeStoreError, insert_supervisor_notice_in_transaction};
use super::provenance::serving_release_provenance_matches;
use super::queue::oldest_queued_request_for_authority;
use super::revision::swap_resource_revision;
use super::rows::{check_authority, select_non_closed_loan, select_request_by_id, select_resource};
use crate::domain::{ExitReason, ProcessGroupExitEvidence, TaskId, TaskState};
use crate::machine::MachineId;
use crate::resource::{
    ActionId, CommandSpec, Loan, LoanId, LoanPhase, LoanState, NoticeId, ResourceId,
    ResourceRequest, ResourceRequestState, ResourceRevision, ResourceTaskOwnershipRisk,
    SupervisorNotice, SupervisorNoticeDelivery, SupervisorNoticePayload,
};
use crate::submission::{ExecutorIdentity, RequestId};

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
pub(super) enum ResourceTaskReleaseProof {
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

    let next_revision =
        input
            .expected_state_revision
            .next()
            .ok_or(ResourceStoreError::Conflict(
                ConflictReason::RevisionExhausted,
            ))?;
    let result = if let Some(mut next_request) =
        oldest_queued_request_for_authority(&tx, input.authority_machine, input.resource_id)?
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
                SupervisorNoticeStoreError::CorruptRecord { what, reason } => {
                    ResourceStoreError::CorruptRecord { what, reason }
                }
                _ => ResourceStoreError::Conflict(ConflictReason::SupervisorNoticeRejected),
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
    let spec_matches = record.current_spec() == Some(request.spec().as_normalized());
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

pub(super) fn resource_task_release_proof(
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
pub(super) fn resource_task_ownership_risk(
    command: &CommandSpec,
) -> Option<ResourceTaskOwnershipRisk> {
    crate::resource::foreground::CommandOwnershipContract::for_queued_command(command.command())
        .err()
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
    let receipt = receipt_json
        .map(|json| stored_json("resource task completion receipt", &json))
        .transpose()?;
    Ok(receipt)
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
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
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
    if !swap_resource_revision::<ResourceStoreError>(
        tx,
        authority_machine,
        resource_id,
        expected_revision,
        next_revision,
    )? {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ResourceRevisionChanged,
        ));
    }

    Ok(())
}
