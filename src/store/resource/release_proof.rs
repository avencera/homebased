use crate::domain::{TaskId, TaskRow};
use crate::machine::MachineId;
use crate::resource::ownership_lock::{OwnershipLockGuard, verify_ownership_lock_guard};
use crate::resource::watcher::{
    PublishedTerminalResult, find_completed_result, revalidate_checkpoint_publication,
};
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
}

/// Authority-created proof for one completed result or action-committed stopped checkpoint
///
/// This type has no serialization or public constructor. It keeps the exact lock guard alive
/// while the release transaction rechecks the saved task, identity, association, and result
#[must_use = "keep the ownership guard alive through the release transaction"]
pub(crate) struct VerifiedReleaseProof {
    authority_machine: MachineId,
    resource_id: ResourceId,
    action_id: ActionId,
    expected_state_revision: ResourceRevision,
    task_id: TaskId,
    association: TrainerAttemptAssociation,
    association_json: String,
    identity_json: String,
    task_row: TaskRow,
    release_evidence: VerifiedReleaseEvidence,
    ownership_guard: OwnershipLockGuard,
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

impl VerifiedReleaseProof {
    pub(super) fn new(evidence: ReleaseProofEvidence) -> Self {
        Self {
            authority_machine: evidence.authority_machine,
            resource_id: evidence.resource_id,
            action_id: evidence.action_id,
            expected_state_revision: evidence.expected_state_revision,
            task_id: evidence.task_id,
            association: evidence.association,
            association_json: evidence.association_json,
            identity_json: evidence.identity_json,
            task_row: evidence.task_row,
            release_evidence: evidence.release_evidence,
            ownership_guard: evidence.ownership_guard,
        }
    }

    pub(crate) const fn authority_machine(&self) -> MachineId {
        self.authority_machine
    }

    pub(crate) const fn resource_id(&self) -> ResourceId {
        self.resource_id
    }

    pub(crate) const fn action_id(&self) -> ActionId {
        self.action_id
    }

    pub(crate) const fn expected_state_revision(&self) -> ResourceRevision {
        self.expected_state_revision
    }

    pub(crate) const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub(crate) fn association_json(&self) -> &str {
        &self.association_json
    }

    pub(crate) fn identity_json(&self) -> &str {
        &self.identity_json
    }

    pub(crate) fn task_row(&self) -> &TaskRow {
        &self.task_row
    }

    pub(crate) fn stopped_decision_and_cancellation(
        &self,
    ) -> Option<(
        &ReleaseCheckpointStopDecision,
        &ReleaseCheckpointCancellation,
    )> {
        match &self.release_evidence {
            VerifiedReleaseEvidence::Completed(_) => None,
            VerifiedReleaseEvidence::Stopped {
                decision,
                cancellation,
            } => Some((decision, cancellation)),
        }
    }

    pub(crate) fn return_context(&self) -> ReturnContext {
        match &self.release_evidence {
            VerifiedReleaseEvidence::Completed(result) => ReturnContext::AlreadyCompleted {
                task_id: self.task_id,
                result_ref: format!(
                    "{}#sha256={}",
                    result.publication_path.display(),
                    result.publication_sha256
                ),
            },
            VerifiedReleaseEvidence::Stopped { decision, .. } => {
                let checkpoint = &decision.selected_checkpoint;
                ReturnContext::Stopped {
                    task_id: self.task_id,
                    checkpoint_ref: format!(
                        "{}#sha256={}",
                        checkpoint.path.display(),
                        checkpoint.record_sha256
                    ),
                    recovery_ref: checkpoint.generation_id.clone(),
                }
            }
        }
    }

    pub(crate) fn serving_release_provenance(&self) -> ServingReleaseProvenance {
        match &self.release_evidence {
            VerifiedReleaseEvidence::Completed(result) => {
                ServingReleaseProvenance::CompletedTrainerResult {
                    action_id: self.action_id,
                    task_id: self.task_id,
                    publication_sha256: result.publication_sha256.clone(),
                }
            }
            VerifiedReleaseEvidence::Stopped { decision, .. } => {
                let checkpoint = &decision.selected_checkpoint;
                ServingReleaseProvenance::StoppedTrainerCheckpoint {
                    action_id: self.action_id,
                    task_id: self.task_id,
                    generation_id: checkpoint.generation_id.clone(),
                    record_sha256: checkpoint.record_sha256.clone(),
                    inventory_sha256: checkpoint.inventory_sha256.clone(),
                }
            }
        }
    }

    pub(crate) fn verify_external_evidence(
        &self,
    ) -> Result<(), crate::resource::store::CompleteReleaseError> {
        match &self.release_evidence {
            VerifiedReleaseEvidence::Completed(expected) => {
                let current_result = find_completed_result(
                    self.association.verified_attempt().canonical_runtime_root(),
                    self.association.verified_attempt().binding(),
                )?;
                if current_result.as_ref() != Some(expected.as_ref()) {
                    return Err(
                        crate::resource::store::CompleteReleaseError::CompletedResultChanged {
                            task_id: self.task_id,
                        },
                    );
                }
            }
            VerifiedReleaseEvidence::Stopped { decision, .. } => {
                let checkpoint = &decision.selected_checkpoint;
                match revalidate_checkpoint_publication(
                    self.association.verified_attempt().canonical_runtime_root(),
                    self.association.verified_attempt().binding(),
                    checkpoint,
                ) {
                    Ok(true) => {}
                    Ok(false)
                    | Err(crate::resource::watcher::WatcherError::MalformedPublication {
                        ..
                    })
                    | Err(crate::resource::watcher::WatcherError::Symlink { .. }) => {
                        return Err(crate::resource::store::CompleteReleaseError::StoppedCheckpointChanged {
                            task_id: self.task_id,
                        });
                    }
                    Err(error) => {
                        return Err(crate::resource::store::CompleteReleaseError::Watcher(error));
                    }
                }
            }
        }

        verify_ownership_lock_guard(
            self.association.verified_attempt().canonical_runtime_root(),
            self.association
                .verified_attempt()
                .ownership_lock_identity(),
            &self.ownership_guard,
        )?;

        Ok(())
    }
}
