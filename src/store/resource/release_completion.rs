//! Verified release proof for one awaiting-release action on the authority

use rusqlite::{Connection, TransactionBehavior};

use super::release_checkpoint::checkpoint_publication_unchanged;
use super::release_proof::{ReleaseProofEvidence, VerifiedReleaseEvidence, VerifiedReleaseProof};
use super::trainer_association::{SavedTrainerAssociation, saved_trainer_association_on};
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskId, TaskRow, TaskState,
};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::command_shape::DirectSegmentCommandShape;
use crate::resource::ownership_lock::{
    OwnershipLockGuard, OwnershipLockProbe, probe_segment_ownership_lock,
};
use crate::resource::store::{
    CompleteReleaseError, ReleaseCompletionResult, ResourceStoreError,
    complete_release_for_authority as persist_release_completion_for_authority,
    release_checkpoint_state_for_action, release_completion_for_retry, select_non_closed_loan,
    select_resource,
};
use crate::resource::trainer_publication::find_completed_result;
use crate::resource::{
    ActionId, LoanPhase, LoanState, ReleaseCheckpointAction, ReleaseCheckpointCancellation,
    ReleaseCheckpointPhase, ReleaseCheckpointStopDecision, ResourceId, ResourceRevision,
    TrainerAttemptAssociation, TrainerAttemptAssociationProof,
};
use crate::spec::NormalizedSpec;
use crate::store::Store;
use crate::store::identity::executor_identity_with_json_on;
use crate::submission::{ExecutionRecord, ExecutorIdentity, normalized_spec_sha256};

/// Release action that the proof must name
#[derive(Clone, Copy)]
struct ReleaseTarget {
    authority_machine: MachineId,
    resource_id: ResourceId,
    action_id: ActionId,
    expected_state_revision: ResourceRevision,
}

/// Saved records read in one snapshot before any file-system evidence is checked
struct ReleaseProofPreflight {
    task_id: TaskId,
    association: SavedTrainerAssociation,
    identity_json: String,
    task_row: TaskRow,
    normalized_spec: NormalizedSpec,
    terminal_evidence: ReleaseProofTerminalEvidence,
}

enum ReleaseProofTerminalEvidence {
    /// Exit status 0; a verified result publication makes it completed, and
    /// its absence makes it an ended run with no usable result
    Completed,
    Stopped {
        decision: Box<ReleaseCheckpointStopDecision>,
        cancellation: ReleaseCheckpointCancellation,
    },
    /// Any other terminal outcome, or a cancellation without this action's
    /// committed stop; only the released lock can prove its release
    Ended { outcome: ExitReason },
}

impl ReleaseProofTerminalEvidence {
    /// Identity state that the executor must have saved for this terminal evidence
    fn expected_identity_state(&self, task_row: &TaskRow) -> ProcessStatus {
        match self {
            Self::Completed => ProcessStatus::Succeeded,
            Self::Stopped { .. } => ProcessStatus::Cancelled,
            Self::Ended { .. } => task_row.status(),
        }
    }
}

impl Store {
    /// Complete a saved release action on this store connection
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

    /// Read the release evidence and hold the released lock without committing the release
    pub(super) fn build_verified_release_proof(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        action_id: ActionId,
        expected_state_revision: ResourceRevision,
    ) -> Result<VerifiedReleaseProof, CompleteReleaseError> {
        let target = ReleaseTarget {
            authority_machine,
            resource_id,
            action_id,
            expected_state_revision,
        };
        let preflight = {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Deferred)?;
            let preflight = release_proof_preflight(&tx, target)?;
            tx.commit()?;
            preflight
        };

        DirectSegmentCommandShape::validate(
            &preflight.normalized_spec,
            &preflight.task_row,
            &preflight.association.association,
        )?;
        let release_evidence = verify_terminal_evidence(&preflight)?;
        let ownership_guard = hold_released_ownership_lock(&preflight)?;

        Ok(VerifiedReleaseProof::new(ReleaseProofEvidence {
            authority_machine: target.authority_machine,
            resource_id: target.resource_id,
            action_id: target.action_id,
            expected_state_revision: target.expected_state_revision,
            task_id: preflight.task_id,
            association: preflight.association.association,
            association_json: preflight.association.json,
            identity_json: preflight.identity_json,
            task_row: preflight.task_row,
            release_evidence,
            ownership_guard,
        }))
    }
}

/// Read the action's trainer, association, identity, and terminal state in one snapshot
fn release_proof_preflight(
    conn: &Connection,
    target: ReleaseTarget,
) -> Result<ReleaseProofPreflight, CompleteReleaseError> {
    let task_id = awaiting_release_task(conn, target)?;
    let association = saved_trainer_association_on(conn, target.resource_id, task_id)?
        .ok_or(CompleteReleaseError::TrainerAssociationMissing { task_id })?;
    let saved = &association.association;
    if saved.authority_machine() != target.authority_machine
        || saved.resource_id() != target.resource_id
        || saved.task_id() != task_id
    {
        return Err(CompleteReleaseError::TrainerAssociationMismatch { task_id });
    }

    let task_row = crate::store::task_by_id_on(conn, task_id)?
        .ok_or(CompleteReleaseError::BackgroundTaskMissing { task_id })?;
    let (identity, identity_json) = executor_identity_with_json_on(conn, task_id)?
        .ok_or(CompleteReleaseError::TrainerIdentityMissing { task_id })?;
    let record = accepted_trainer_record(identity, task_id, target.authority_machine)?;

    let terminal_evidence = terminal_evidence(conn, target, task_id, &task_row, saved)?;
    if record.state != terminal_evidence.expected_identity_state(&task_row) {
        return Err(CompleteReleaseError::TrainerIdentityChanged { task_id });
    }
    if task_row.process_group_exit_evidence() != ProcessGroupExitEvidence::ConfirmedExited {
        return Err(CompleteReleaseError::WorkerExitUnconfirmed { task_id });
    }

    let normalized_spec = record
        .current_spec()
        .ok_or(CompleteReleaseError::TrainerAssociationMismatch { task_id })?
        .clone();
    if normalized_spec_sha256(&normalized_spec).map_err(AppError::from)?
        != saved.normalized_spec_sha256()
    {
        return Err(CompleteReleaseError::TrainerAssociationMismatch { task_id });
    }

    Ok(ReleaseProofPreflight {
        task_id,
        association,
        identity_json,
        task_row,
        normalized_spec,
        terminal_evidence,
    })
}

/// Return the registered trainer that the current release action observes
fn awaiting_release_task(
    conn: &Connection,
    target: ReleaseTarget,
) -> Result<TaskId, CompleteReleaseError> {
    let ReleaseTarget {
        authority_machine,
        resource_id,
        action_id,
        expected_state_revision,
    } = target;
    // another authority's resource is not visible to this daemon
    let resource = select_resource(conn, resource_id)?
        .filter(|resource| resource.authority_machine() == authority_machine)
        .ok_or(ResourceStoreError::ResourceNotFound)?;
    let loan = select_non_closed_loan(conn, resource_id)?
        .ok_or(CompleteReleaseError::ActionNotFound { action_id })?;
    let not_awaiting = || CompleteReleaseError::NotAwaitingRelease {
        loan_id: loan.id,
        action_id,
    };
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingRelease {
                action_id: saved_action_id,
                observed_background_task,
                ..
            },
    } = loan.state
    else {
        return Err(not_awaiting());
    };
    if saved_action_id != action_id {
        return Err(not_awaiting());
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

    Ok(observed_background_task)
}

/// Require the trainer's accepted identity on this execution authority
fn accepted_trainer_record(
    identity: ExecutorIdentity,
    task_id: TaskId,
    authority_machine: MachineId,
) -> Result<ExecutionRecord, CompleteReleaseError> {
    let ExecutorIdentity::Accepted(record) = identity else {
        return Err(CompleteReleaseError::TrainerIdentityChanged { task_id });
    };
    // the trainer's callback origin may be any supervisor machine
    if !record.is_executed_by(task_id, authority_machine) {
        return Err(CompleteReleaseError::TrainerIdentityChanged { task_id });
    }

    Ok(record)
}

/// Classify the trainer's terminal task state as release evidence
fn terminal_evidence(
    conn: &Connection,
    target: ReleaseTarget,
    task_id: TaskId,
    task_row: &TaskRow,
    association: &TrainerAttemptAssociation,
) -> Result<ReleaseProofTerminalEvidence, CompleteReleaseError> {
    match &task_row.state {
        TaskState::Finished {
            reason: ExitReason::Exit { code: 0 },
        } => Ok(ReleaseProofTerminalEvidence::Completed),
        TaskState::Finished {
            reason: ExitReason::Cancelled,
        } => stopped_evidence(conn, target, task_id, task_row, association),
        // a failed run never implies that any checkpoint is resumable
        TaskState::Finished { reason } => Ok(ReleaseProofTerminalEvidence::Ended {
            outcome: reason.clone(),
        }),
        TaskState::Lost => Err(CompleteReleaseError::BackgroundTaskLost { task_id }),
        TaskState::Queued | TaskState::Running { .. } => {
            Err(CompleteReleaseError::BackgroundTaskNotTerminal {
                task_id,
                state: task_row.status().to_string(),
            })
        }
    }
}

/// Use this action's committed stop as evidence for a cancelled trainer
///
/// A cancellation that this action did not commit is a generic end; it names
/// no checkpoint that the run could resume from
fn stopped_evidence(
    conn: &Connection,
    target: ReleaseTarget,
    task_id: TaskId,
    task_row: &TaskRow,
    association: &TrainerAttemptAssociation,
) -> Result<ReleaseProofTerminalEvidence, CompleteReleaseError> {
    let ended = ReleaseProofTerminalEvidence::Ended {
        outcome: ExitReason::Cancelled,
    };
    let Some((checkpoint_state, _)) =
        release_checkpoint_state_for_action(conn, target.resource_id, target.action_id)?
    else {
        return Ok(ended);
    };
    let expected_action = ReleaseCheckpointAction {
        resource_id: target.resource_id,
        action_id: target.action_id,
        state_revision: target.expected_state_revision,
        observed_background_task: task_id,
    };
    if checkpoint_state.action != expected_action {
        return Err(CompleteReleaseError::StoppedProofUnavailable { task_id });
    }
    let ReleaseCheckpointPhase::CancellationCommitted {
        baseline,
        decision,
        cancellation,
    } = checkpoint_state.phase
    else {
        return Ok(ended);
    };
    if baseline.binding.association != TrainerAttemptAssociationProof::from(association) {
        return Err(CompleteReleaseError::TrainerAssociationMismatch { task_id });
    }
    if decision.binding != baseline.binding || cancellation.task_id != task_id {
        return Err(CompleteReleaseError::StoppedProofUnavailable { task_id });
    }
    if task_row.cancel_requested_at != Some(cancellation.cancel_requested_at) {
        return Err(CompleteReleaseError::TrainerCancellationMarkerChanged { task_id });
    }

    Ok(ReleaseProofTerminalEvidence::Stopped {
        decision,
        cancellation,
    })
}

/// Check the published result or selected checkpoint that the terminal evidence names
fn verify_terminal_evidence(
    preflight: &ReleaseProofPreflight,
) -> Result<VerifiedReleaseEvidence, CompleteReleaseError> {
    let task_id = preflight.task_id;
    let verified_attempt = preflight.association.association.verified_attempt();
    match &preflight.terminal_evidence {
        ReleaseProofTerminalEvidence::Completed => {
            // a zero exit with no publication is an end with no usable result
            let Some(completed_result) = find_completed_result(
                verified_attempt.canonical_runtime_root(),
                verified_attempt.binding(),
            )?
            else {
                return Ok(VerifiedReleaseEvidence::Ended {
                    outcome: ExitReason::Exit { code: 0 },
                });
            };
            if completed_result.binding != *verified_attempt.binding()
                || completed_result.request_sha256 != verified_attempt.request_digest()
            {
                return Err(CompleteReleaseError::CompletedResultRequestMismatch { task_id });
            }

            Ok(VerifiedReleaseEvidence::Completed(Box::new(
                completed_result,
            )))
        }
        ReleaseProofTerminalEvidence::Stopped {
            decision,
            cancellation,
        } => {
            let checkpoint = &decision.selected_checkpoint;
            if checkpoint.binding != *verified_attempt.binding()
                || !checkpoint_publication_unchanged(
                    verified_attempt.canonical_runtime_root(),
                    verified_attempt.binding(),
                    checkpoint,
                )?
            {
                return Err(CompleteReleaseError::StoppedCheckpointChanged { task_id });
            }

            Ok(VerifiedReleaseEvidence::Stopped {
                decision: decision.clone(),
                cancellation: cancellation.clone(),
            })
        }
        ReleaseProofTerminalEvidence::Ended { outcome } => Ok(VerifiedReleaseEvidence::Ended {
            outcome: outcome.clone(),
        }),
    }
}

/// Hold the exact saved ownership lock, which must be free for every release basis
///
/// The guard stays held until the release transaction commits
fn hold_released_ownership_lock(
    preflight: &ReleaseProofPreflight,
) -> Result<OwnershipLockGuard, CompleteReleaseError> {
    let verified_attempt = preflight.association.association.verified_attempt();
    match probe_segment_ownership_lock(
        verified_attempt.canonical_runtime_root(),
        verified_attempt.ownership_lock_identity(),
    ) {
        OwnershipLockProbe::OwnershipHeld => Err(CompleteReleaseError::OwnershipLockStillHeld {
            task_id: preflight.task_id,
        }),
        OwnershipLockProbe::ExactOwnershipReleased(guard) => Ok(guard),
        OwnershipLockProbe::Attention(source) => Err(CompleteReleaseError::OwnershipLock(source)),
    }
}
