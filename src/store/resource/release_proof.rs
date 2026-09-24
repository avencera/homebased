use super::release_checkpoint::checkpoint_publication_unchanged;
use crate::domain::{ExitReason, TaskId, TaskRow};
use crate::machine::MachineId;
use crate::resource::ownership_lock::{OwnershipLockGuard, verify_ownership_lock_guard};
use crate::resource::store::CompleteReleaseError;
use crate::resource::trainer_publication::{PublishedTerminalResult, find_completed_result};
use crate::resource::{
    ActionId, ReleaseCheckpointCancellation, ReleaseCheckpointStopDecision, ResourceId,
    ResourceRevision, ReturnContext, ServingReleaseProvenance, TrainerAttemptAssociation,
};

pub(super) enum VerifiedReleaseEvidence {
    Completed(Box<PublishedTerminalResult>),
    Stopped {
        decision: Box<ReleaseCheckpointStopDecision>,
        cancellation: ReleaseCheckpointCancellation,
    },
    /// The trainer ended with no usable result and no committed checkpoint stop
    ///
    /// The released lock is the only evidence, so the release names the
    /// outcome and carries no resume reference
    Ended {
        outcome: ExitReason,
    },
}

/// Evidence fields supplied only by the authority resource-store module
pub(super) struct ReleaseProofEvidence {
    pub(super) authority_machine: MachineId,
    pub(super) resource_id: ResourceId,
    pub(super) action_id: ActionId,
    pub(super) expected_state_revision: ResourceRevision,
    pub(super) task_id: TaskId,
    pub(super) association: TrainerAttemptAssociation,
    pub(super) association_json: String,
    pub(super) identity_json: String,
    pub(super) task_row: TaskRow,
    pub(super) release_evidence: VerifiedReleaseEvidence,
    pub(super) ownership_guard: OwnershipLockGuard,
}

/// Authority-created proof for one completed result, action-committed stopped checkpoint,
/// or ended trainer whose exact lock is released
///
/// This type has no serialization or public constructor. It keeps the exact lock guard alive
/// while the release transaction rechecks the saved task, identity, association, and result
#[must_use = "keep the ownership guard alive through the release transaction"]
pub(crate) struct VerifiedReleaseProof(ReleaseProofEvidence);

impl VerifiedReleaseProof {
    pub(super) fn new(evidence: ReleaseProofEvidence) -> Self {
        Self(evidence)
    }

    pub(crate) const fn authority_machine(&self) -> MachineId {
        self.0.authority_machine
    }

    pub(crate) const fn resource_id(&self) -> ResourceId {
        self.0.resource_id
    }

    pub(crate) const fn action_id(&self) -> ActionId {
        self.0.action_id
    }

    pub(crate) const fn expected_state_revision(&self) -> ResourceRevision {
        self.0.expected_state_revision
    }

    pub(crate) const fn task_id(&self) -> TaskId {
        self.0.task_id
    }

    pub(crate) fn association_json(&self) -> &str {
        &self.0.association_json
    }

    pub(crate) fn identity_json(&self) -> &str {
        &self.0.identity_json
    }

    pub(crate) fn task_row(&self) -> &TaskRow {
        &self.0.task_row
    }

    pub(crate) fn stopped_decision_and_cancellation(
        &self,
    ) -> Option<(
        &ReleaseCheckpointStopDecision,
        &ReleaseCheckpointCancellation,
    )> {
        match &self.0.release_evidence {
            VerifiedReleaseEvidence::Completed(_) | VerifiedReleaseEvidence::Ended { .. } => None,
            VerifiedReleaseEvidence::Stopped {
                decision,
                cancellation,
            } => Some((decision, cancellation)),
        }
    }

    /// Outcome of an ended trainer that has no result or stop evidence
    pub(crate) fn ended_outcome(&self) -> Option<&ExitReason> {
        match &self.0.release_evidence {
            VerifiedReleaseEvidence::Ended { outcome } => Some(outcome),
            VerifiedReleaseEvidence::Completed(_) | VerifiedReleaseEvidence::Stopped { .. } => None,
        }
    }

    pub(crate) fn return_context(&self) -> ReturnContext {
        match &self.0.release_evidence {
            VerifiedReleaseEvidence::Completed(result) => ReturnContext::AlreadyCompleted {
                task_id: self.0.task_id,
                result_ref: format!(
                    "{}#sha256={}",
                    result.publication_path.display(),
                    result.publication_sha256
                ),
            },
            VerifiedReleaseEvidence::Stopped { decision, .. } => {
                let checkpoint = &decision.selected_checkpoint;
                ReturnContext::Stopped {
                    task_id: self.0.task_id,
                    checkpoint_ref: format!(
                        "{}#sha256={}",
                        checkpoint.path.display(),
                        checkpoint.record_sha256
                    ),
                    recovery_ref: checkpoint.generation_id.clone(),
                }
            }
            VerifiedReleaseEvidence::Ended { outcome } => ReturnContext::EndedWithoutResult {
                task_id: self.0.task_id,
                outcome: outcome.clone(),
            },
        }
    }

    pub(crate) fn serving_release_provenance(&self) -> ServingReleaseProvenance {
        match &self.0.release_evidence {
            VerifiedReleaseEvidence::Completed(result) => {
                ServingReleaseProvenance::CompletedTrainerResult {
                    action_id: self.0.action_id,
                    task_id: self.0.task_id,
                    publication_sha256: result.publication_sha256,
                }
            }
            VerifiedReleaseEvidence::Stopped { decision, .. } => {
                let checkpoint = &decision.selected_checkpoint;
                ServingReleaseProvenance::StoppedTrainerCheckpoint {
                    action_id: self.0.action_id,
                    task_id: self.0.task_id,
                    generation_id: checkpoint.generation_id.clone(),
                    record_sha256: checkpoint.record_sha256,
                    inventory_sha256: checkpoint.inventory_sha256,
                }
            }
            VerifiedReleaseEvidence::Ended { outcome } => {
                ServingReleaseProvenance::EndedTrainerLockReleased {
                    action_id: self.0.action_id,
                    task_id: self.0.task_id,
                    outcome: outcome.clone(),
                    attempt_request_sha256: self.0.association.verified_attempt().request_digest(),
                }
            }
        }
    }

    pub(crate) fn verify_external_evidence(&self) -> Result<(), CompleteReleaseError> {
        match &self.0.release_evidence {
            VerifiedReleaseEvidence::Completed(expected) => {
                let current_result = find_completed_result(
                    self.0
                        .association
                        .verified_attempt()
                        .canonical_runtime_root(),
                    self.0.association.verified_attempt().binding(),
                )?;
                if current_result.as_ref() != Some(expected.as_ref()) {
                    return Err(CompleteReleaseError::CompletedResultChanged {
                        task_id: self.0.task_id,
                    });
                }
            }
            VerifiedReleaseEvidence::Stopped { decision, .. } => {
                let checkpoint = &decision.selected_checkpoint;
                if !checkpoint_publication_unchanged(
                    self.0
                        .association
                        .verified_attempt()
                        .canonical_runtime_root(),
                    self.0.association.verified_attempt().binding(),
                    checkpoint,
                )? {
                    return Err(CompleteReleaseError::StoppedCheckpointChanged {
                        task_id: self.0.task_id,
                    });
                }
            }
            // the exact released lock below is the whole external evidence
            VerifiedReleaseEvidence::Ended { .. } => {}
        }

        verify_ownership_lock_guard(
            self.0
                .association
                .verified_attempt()
                .canonical_runtime_root(),
            self.0
                .association
                .verified_attempt()
                .ownership_lock_identity(),
            &self.0.ownership_guard,
        )?;

        Ok(())
    }
}
