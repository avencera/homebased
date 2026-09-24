//! Proof-gated completion of a release action

use crate::domain::ExitReason;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;

use super::checkpoint::{ReleaseCheckpointError, release_checkpoint_state_for_action};
use super::codec::{sqlite_integer, stored_json};
use super::error::{ResourceStoreError, TrainerAttemptAssociationStoreError};
use super::notice::{
    SupervisorNoticeStoreError, insert_supervisor_notice_in_transaction,
    select_supervisor_notice_record_by_action,
};
use super::queue::oldest_queued_request_for_authority;
use super::revision::swap_resource_revision;
use super::rows::{check_authority, decode_loan, select_resource};
use crate::domain::{ProcessGroupExitEvidence, TaskId, TaskState};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::command_shape::DirectSegmentCommandShapeError;
use crate::resource::ownership_lock::OwnershipLockProbeError;
use crate::resource::trainer_publication::WatcherError;
use crate::resource::{
    ActionId, Loan, LoanId, LoanPhase, LoanState, NoticeId, ReleaseCheckpointPhase, Resource,
    ResourceId, ResourceRequest, ResourceRequestState, ResourceRevision, ReturnContext,
    ServingReleaseProvenance, SupervisorNotice, SupervisorNoticeDelivery, SupervisorNoticePayload,
};
use crate::store::{IdentityError, VerifiedReleaseProof};
use crate::submission::RequestId;

/// Stable result committed when a release action completes
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ReleaseCompletionResult {
    /// The oldest queued request now owns the resource through this loan
    Assigned {
        /// Loan updated from AwaitingRelease to Serving
        loan: Loan,
        /// Exact request selected at completion time
        request: ResourceRequest,
        /// Resource revision committed with the assignment
        state_revision: ResourceRevision,
    },
    /// The queue was empty and the supervisor now owns the return decision
    ReturnRequired {
        /// Loan updated from AwaitingRelease to AwaitingReturn
        loan: Loan,
        /// Durable notice for the exact current supervisor assignment
        notice: SupervisorNotice,
    },
}

/// Completion evidence for one saved release action
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReleaseCompletionReceipt {
    pub(super) action_id: ActionId,
    pub(super) authority_machine: MachineId,
    pub(super) resource_id: ResourceId,
    pub(super) expected_state_revision: ResourceRevision,
    pub(super) return_context: ReturnContext,
    /// Proof basis accepted by this completion, for either result
    pub(super) release_provenance: ServingReleaseProvenance,
    pub(super) result: ReleaseCompletionResult,
}

/// A release action could not be completed for the supplied evidence
#[derive(Debug, thiserror::Error)]
pub(crate) enum CompleteReleaseError {
    /// Resource authority or stored resource data failed validation
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// A supervisor notice could not be read or inserted
    #[error(transparent)]
    Notice(#[from] SupervisorNoticeStoreError),
    /// A trainer association could not be read or does not match the release task
    #[error(transparent)]
    TrainerAssociation(#[from] TrainerAttemptAssociationStoreError),
    /// A persisted task identity could not be read or did not match its authority
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// The accepted task row could not be read
    #[error(transparent)]
    TaskStorage(#[from] AppError),
    /// The accepted command does not match its saved trainer association
    #[error(transparent)]
    CommandShape(#[from] DirectSegmentCommandShapeError),
    /// The result watcher could not verify the completed publication
    #[error(transparent)]
    Watcher(#[from] WatcherError),
    /// The saved trainer ownership lock could not be verified
    #[error(transparent)]
    OwnershipLock(#[from] OwnershipLockProbeError),
    /// The action's durable checkpoint or cancellation record could not be verified
    #[error(transparent)]
    Checkpoint(#[from] ReleaseCheckpointError),
    /// The action has neither an active release phase nor a completion receipt
    #[error("release action {action_id:?} was not found")]
    ActionNotFound {
        /// Stable release action identity
        action_id: ActionId,
    },
    /// The action is no longer in the expected AwaitingRelease phase
    #[error("release action {action_id:?} is not awaiting release on loan {loan_id:?}")]
    NotAwaitingRelease {
        /// Loan that should own the release action
        loan_id: LoanId,
        /// Stable release action identity
        action_id: ActionId,
    },
    /// The action's durable release notice is missing or inconsistent
    #[error("release action {action_id:?} has an invalid durable notice")]
    InvalidReleaseNotice {
        /// Stable release action identity
        action_id: ActionId,
    },
    /// The caller's expected revision does not match current resource state
    #[error("stale resource revision: expected {expected:?}, found {actual:?}")]
    StaleRevision {
        /// Resource revision associated with the release notice
        expected: ResourceRevision,
        /// Current resource revision in SQLite
        actual: ResourceRevision,
    },
    /// The observed task has no ordinary task row on this authority
    #[error("observed background task {task_id} is missing locally")]
    BackgroundTaskMissing {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The ordinary task row is not terminal yet
    #[error("observed background task {task_id} is not terminal (state {state})")]
    BackgroundTaskNotTerminal {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
        /// Process state currently saved in the task table
        state: String,
    },
    /// A lost task row cannot establish that the resource is free
    #[error("observed background task {task_id} is lost and needs attention")]
    BackgroundTaskLost {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The release action has no durable association for its exact trainer task
    #[error("release task {task_id} has no trainer-attempt association")]
    TrainerAssociationMissing {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The saved trainer association no longer matches its task or resource
    #[error("trainer-attempt association does not match release task {task_id}")]
    TrainerAssociationMismatch {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The accepted identity row is missing for the exact trainer task
    #[error("accepted trainer identity for task {task_id} is missing")]
    TrainerIdentityMissing {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The accepted trainer identity no longer matches its saved proof
    #[error("accepted trainer identity for task {task_id} changed")]
    TrainerIdentityChanged {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The task was not cancelled by this action's saved stop decision
    #[error("trainer task {task_id} has no committed stop decision for this release action")]
    StoppedProofUnavailable {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The exact cancel marker no longer matches the release action record
    #[error("trainer task {task_id} cancellation marker differs from the saved release action")]
    TrainerCancellationMarkerChanged {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The selected checkpoint publication changed during stopped release verification
    #[error("trainer task {task_id} selected checkpoint changed during release verification")]
    StoppedCheckpointChanged {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The exact task exited successfully but did not confirm its child process group exited
    #[error("trainer task {task_id} has no confirmed worker-exit evidence")]
    WorkerExitUnconfirmed {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The verified result publication differs from the result held by the proof
    #[error("trainer task {task_id} completed result changed during release verification")]
    CompletedResultChanged {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The verified result request differs from the request saved at registration
    #[error("trainer task {task_id} result request differs from its registered attempt")]
    CompletedResultRequestMismatch {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The exact saved ownership lock is still held by a process
    #[error("trainer task {task_id} ownership lock is still held")]
    OwnershipLockStillHeld {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The persisted task state or worker-exit evidence changed during proof creation
    #[error("trainer task {task_id} state changed during release verification")]
    TaskStateChanged {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The persisted task command binding changed during release verification
    #[error("trainer task {task_id} command binding changed during release verification")]
    TaskCommandChanged {
        /// Exact task recorded in the AwaitingRelease phase
        task_id: TaskId,
    },
    /// The resource state revision cannot be incremented
    #[error("resource revision {revision:?} cannot be incremented")]
    RevisionExhausted {
        /// Current resource revision
        revision: ResourceRevision,
    },
    /// A durable receipt already exists for different input identity or evidence
    #[error("release action {action_id:?} was retried with conflicting input")]
    ConflictingRetry {
        /// Stable release action identity
        action_id: ActionId,
    },
    /// A concurrent or inconsistent queue change prevented assignment
    #[error("queued request {request_id:?} changed during release completion")]
    RequestChanged {
        /// Request selected at the head of the authority FIFO
        request_id: RequestId,
    },
    /// A concurrent or inconsistent loan change prevented completion
    #[error("loan {loan_id:?} changed during release completion")]
    LoanChanged {
        /// Loan that owns the release action
        loan_id: LoanId,
    },
    /// SQLite or serialized stored data failed
    #[error("release completion storage error: {0}")]
    Storage(#[from] rusqlite::Error),
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

    let (notice, _) = select_supervisor_notice_record_by_action(&tx, action_id)
        .map_err(ResourceStoreError::from)?
        .ok_or(CompleteReleaseError::ActionNotFound { action_id })?;
    let loan = tx
        .query_row(
            "SELECT id, resource_id, state_json FROM loans WHERE id = ?1",
            [notice.loan_id.as_uuid().to_string()],
            |row| Ok(decode_loan(row)),
        )
        .optional()?
        .transpose()?
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

    let next_revision =
        expected_state_revision
            .next()
            .ok_or(CompleteReleaseError::RevisionExhausted {
                revision: expected_state_revision,
            })?;

    let result = if let Some(mut request) =
        oldest_queued_request_for_authority(&tx, authority_machine, resource_id)?
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
                    reason: ExitReason::Cancelled
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
                .unwrap_or(ExitReason::Exit { code: 0 });
            if !matches!(
                &current_task.state,
                TaskState::Finished { reason } if *reason == expected_reason
            ) {
                return Err(CompleteReleaseError::TaskStateChanged { task_id });
            }
            // an ended cancellation is generic only while this action committed no stop
            if proof.ended_outcome() == Some(&ExitReason::Cancelled)
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

fn update_resource_revision(
    tx: &Transaction<'_>,
    authority_machine: MachineId,
    resource_id: ResourceId,
    expected_revision: ResourceRevision,
    next_revision: ResourceRevision,
) -> Result<(), CompleteReleaseError> {
    let swapped = swap_resource_revision::<CompleteReleaseError>(
        tx,
        authority_machine,
        resource_id,
        expected_revision,
        next_revision,
    )?;
    if !swapped {
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
) -> Result<Option<(ActionId, ReturnContext)>, ResourceStoreError> {
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
    let receipt = receipt_json
        .map(|json| stored_json::<ReleaseCompletionReceipt>("release completion receipt", &json))
        .transpose()?;
    Ok(receipt.map(|receipt| (receipt.action_id, receipt.return_context)))
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

    let receipt = receipt_json
        .map(|json| stored_json("release completion receipt", &json))
        .transpose()
        .map_err(ResourceStoreError::from)?;
    Ok(receipt)
}

fn encode_completion_json<T: Serialize>(value: &T) -> Result<String, CompleteReleaseError> {
    serde_json::to_string(value).map_err(|error| {
        CompleteReleaseError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
    })
}
