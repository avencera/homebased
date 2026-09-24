//! Release checkpoint baseline, stop reservation, and trainer cancellation on the authority

use std::path::Path;

use chrono::{SecondsFormat, Utc};
use rusqlite::{Connection, TransactionBehavior, params};

use super::release_watcher::release_watcher_is_running_on;
use super::trainer_association::{
    trainer_association_bind_snapshot, trainer_association_by_resource_and_task,
};
use crate::domain::ProcessStatus;
use crate::machine::MachineId;
use crate::resource::store::{
    ReleaseCheckpointCancellationOutcome, ReleaseCheckpointCancellationResult,
    ReleaseCheckpointError, TrainerAttemptAssociationStoreError,
    release_checkpoint_state_for_action, select_non_closed_loan, update_release_checkpoint_state,
    validate_release_watcher_intent_on,
};
use crate::resource::trainer_publication::{
    AttemptBinding, VerifiedCheckpointPublication, WatchObservation, WatcherError,
    observe_release_with_checkpoint_evidence, revalidate_checkpoint_publication, snapshot,
};
use crate::resource::{
    ActionId, LoanPhase, LoanState, ReleaseCheckpointAction, ReleaseCheckpointBaseline,
    ReleaseCheckpointBinding, ReleaseCheckpointCancellation, ReleaseCheckpointPhase,
    ReleaseCheckpointState, ReleaseCheckpointStopDecision, ReleaseCheckpointStopOutcome,
    ReleaseStopReservationId, ResourceId, ResourceRevision, TrainerAttemptAssociationProof,
};
use crate::store::Store;

/// Opening a release action saves its checkpoint state in the same transaction
const CHECKPOINT_STATE_MISSING: &str = "current release action has no checkpoint state";

/// Saved checkpoint state of the current release action and its rebuilt binding
struct CheckpointForAction {
    state: ReleaseCheckpointState,
    previous_json: String,
    binding: ReleaseCheckpointBinding,
}

impl Store {
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
        let CheckpointForAction {
            mut state,
            previous_json,
            binding,
        } = load_checkpoint_for_action(
            &tx,
            authority_machine,
            resource_id,
            action_id,
            expected_state_revision,
        )?;
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
            snapshot: snapshot(
                &binding.association.canonical_runtime_root,
                &binding.attempt_binding,
            )?,
            binding,
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
        let CheckpointForAction {
            mut state,
            previous_json,
            binding,
        } = load_checkpoint_for_action(
            &tx,
            authority_machine,
            resource_id,
            action_id,
            expected_state_revision,
        )?;
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

        let task_id = state.action.observed_background_task;
        let task = crate::store::task_by_id_on(&tx, task_id)?
            .ok_or(ReleaseCheckpointError::TaskMissing { task_id })?;
        let (observation, checkpoint) = observe_release_with_checkpoint_evidence(
            &binding.association.canonical_runtime_root,
            &binding.attempt_binding,
            &baseline.snapshot,
            &task.state,
        )?;
        let Some(checkpoint) = checkpoint else {
            let outcome = unreserved_stop_outcome(observation)?;
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
        let CheckpointForAction {
            mut state,
            previous_json,
            binding,
        } = load_checkpoint_for_action(
            &tx,
            authority_machine,
            resource_id,
            action_id,
            expected_state_revision,
        )?;
        let (baseline, decision) = match state.phase {
            ReleaseCheckpointPhase::CancellationCommitted {
                baseline,
                decision,
                cancellation,
            } => {
                check_saved_decision(&binding, &baseline, &decision, expected_decision, action_id)?;
                let task = crate::store::task_by_id_on(&tx, cancellation.task_id)?.ok_or(
                    ReleaseCheckpointError::TaskMissing {
                        task_id: cancellation.task_id,
                    },
                )?;
                if task.cancel_requested_at.is_none() {
                    return Err(ReleaseCheckpointError::TrainerCancellationConflict {
                        task_id: cancellation.task_id,
                    });
                }

                tx.commit()?;
                return Ok(ReleaseCheckpointCancellationOutcome::AlreadyCommitted(
                    ReleaseCheckpointCancellationResult {
                        decision: *decision,
                        cancellation,
                    },
                ));
            }
            ReleaseCheckpointPhase::StopReserved { baseline, decision } => {
                check_saved_decision(&binding, &baseline, &decision, expected_decision, action_id)?;
                (baseline, decision)
            }
            ReleaseCheckpointPhase::WatcherBindingPending
            | ReleaseCheckpointPhase::BaselineCaptured { .. } => {
                return Err(ReleaseCheckpointError::StopDecisionMissing { action_id });
            }
        };

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
            || !trainer.task_row_matches_spec(task_id)
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
        let result = ReleaseCheckpointCancellationResult {
            decision: decision.as_ref().clone(),
            cancellation: cancellation.clone(),
        };
        state.phase = ReleaseCheckpointPhase::CancellationCommitted {
            baseline,
            decision,
            cancellation,
        };
        update_release_checkpoint_state(&tx, &previous_json, &state)?;
        tx.commit()?;

        Ok(ReleaseCheckpointCancellationOutcome::Committed(result))
    }
}

/// Load the current action's checkpoint state at the expected revision with its binding
fn load_checkpoint_for_action(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    action_id: ActionId,
    expected_state_revision: ResourceRevision,
) -> Result<CheckpointForAction, ReleaseCheckpointError> {
    let Some((state, previous_json)) =
        release_checkpoint_state_for_action(conn, resource_id, action_id)?
    else {
        return Err(missing_release_checkpoint_state_error(
            conn,
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
        release_checkpoint_binding_on(conn, authority_machine, resource_id, &state.action)?;

    Ok(CheckpointForAction {
        state,
        previous_json,
        binding,
    })
}

/// Compare a saved stop decision with the caller's decision and the current binding
fn check_saved_decision(
    binding: &ReleaseCheckpointBinding,
    baseline: &ReleaseCheckpointBaseline,
    decision: &ReleaseCheckpointStopDecision,
    expected_decision: &ReleaseCheckpointStopDecision,
    action_id: ActionId,
) -> Result<(), ReleaseCheckpointError> {
    if decision != expected_decision {
        return Err(ReleaseCheckpointError::StopDecisionMismatch { action_id });
    }
    if baseline.binding != *binding || decision.binding != *binding {
        return Err(ReleaseCheckpointError::Conflict);
    }

    Ok(())
}

/// Map an observation with no new checkpoint to the stop outcome it reports
fn unreserved_stop_outcome(
    observation: WatchObservation,
) -> Result<ReleaseCheckpointStopOutcome, ReleaseCheckpointError> {
    Ok(match observation {
        WatchObservation::WaitingForTaskStart => ReleaseCheckpointStopOutcome::WaitingForTaskStart,
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
    })
}

/// Rebuild the binding of one release action from its loan, watcher intent, and association
pub(super) fn release_checkpoint_binding_on(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    action: &ReleaseCheckpointAction,
) -> Result<ReleaseCheckpointBinding, ReleaseCheckpointError> {
    let not_awaiting = || ReleaseCheckpointError::NotAwaitingRelease {
        action_id: action.action_id,
    };
    let loan = select_non_closed_loan(conn, resource_id)?.ok_or_else(not_awaiting)?;
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task,
                watcher_intent,
            },
    } = loan.state
    else {
        return Err(not_awaiting());
    };
    if action.resource_id != resource_id
        || action.action_id != action_id
        || action.observed_background_task != observed_background_task
    {
        return Err(ReleaseCheckpointError::Conflict);
    }
    let intent = watcher_intent.ok_or(ReleaseCheckpointError::WatcherIntentMissing {
        action_id: action.action_id,
    })?;
    validate_release_watcher_intent_on(conn, authority_machine, resource_id, &intent)?;

    let task_id = action.observed_background_task;
    let association = trainer_association_by_resource_and_task(conn, resource_id, task_id)?
        .ok_or(ReleaseCheckpointError::TrainerAssociationMissing { task_id })?;
    if association.resource_id() != resource_id
        || association.authority_machine() != authority_machine
        || association.task_id() != task_id
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

/// Recheck one selected checkpoint publication against its trainer attempt
///
/// A missing, malformed, or symlinked publication means that the checkpoint
/// changed; only other watcher failures are errors
pub(super) fn checkpoint_publication_unchanged(
    runtime_root: &Path,
    binding: &AttemptBinding,
    checkpoint: &VerifiedCheckpointPublication,
) -> Result<bool, WatcherError> {
    match revalidate_checkpoint_publication(runtime_root, binding, checkpoint) {
        Err(WatcherError::MalformedPublication { .. } | WatcherError::Symlink { .. }) => Ok(false),
        result => result,
    }
}

fn revalidate_selected_checkpoint(
    binding: &ReleaseCheckpointBinding,
    decision: &ReleaseCheckpointStopDecision,
    action_id: ActionId,
) -> Result<(), ReleaseCheckpointError> {
    if !checkpoint_publication_unchanged(
        &binding.association.canonical_runtime_root,
        &binding.attempt_binding,
        &decision.selected_checkpoint,
    )? {
        return Err(ReleaseCheckpointError::SelectedCheckpointChanged { action_id });
    }

    Ok(())
}

/// Report a missing checkpoint state as corrupt only for the current release action
pub(super) fn missing_release_checkpoint_state_error(
    conn: &Connection,
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
        } if current_action == action_id => Ok(ReleaseCheckpointError::InvalidStoredEvidence(
            CHECKPOINT_STATE_MISSING,
        )),
        _ => Ok(ReleaseCheckpointError::Conflict),
    }
}

/// Require a captured baseline, and any saved decision, bound to the current action
pub(super) fn validate_release_checkpoint_baseline_on(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    action_id: ActionId,
) -> Result<(), ReleaseCheckpointError> {
    let Some((state, _)) = release_checkpoint_state_for_action(conn, resource_id, action_id)?
    else {
        return Err(ReleaseCheckpointError::InvalidStoredEvidence(
            CHECKPOINT_STATE_MISSING,
        ));
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
