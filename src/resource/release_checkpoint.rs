//! Durable checkpoint evidence for one release action
//!
//! The authority saves a baseline of trainer publications when it binds the
//! release watcher, then a stop decision for one new verified checkpoint, then
//! the exact trainer cancellation. Each phase keeps every owner identity so a
//! restart or retry can check it against the saved release action

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::trainer_publication::{
    AttemptBinding, RecoverySnapshot, VerifiedCheckpointPublication, WatcherAttention,
};
use super::{
    ActionId, ReleaseStopReservationId, ReleaseWatcherIntent, ResourceId, ResourceRevision,
    TrainerAttemptAssociationProof,
};
use crate::domain::TaskId;

/// Durable release identity captured before the watcher task can act
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointAction {
    pub(crate) resource_id: ResourceId,
    pub(crate) action_id: ActionId,
    pub(crate) state_revision: ResourceRevision,
    pub(crate) observed_background_task: TaskId,
}

/// Full authority-owned identity that scopes one checkpoint baseline and stop decision
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointBinding {
    pub(crate) action: ReleaseCheckpointAction,
    pub(crate) association: TrainerAttemptAssociationProof,
    pub(crate) attempt_binding: AttemptBinding,
    pub(crate) watcher_intent: ReleaseWatcherIntent,
}

impl ReleaseCheckpointBinding {
    pub(crate) fn validate_for(
        &self,
        action: &ReleaseCheckpointAction,
    ) -> Result<(), &'static str> {
        validate_release_checkpoint_binding(action, self)
    }
}

/// Checkpoint baseline captured from the saved trainer association
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointBaseline {
    pub(crate) binding: ReleaseCheckpointBinding,
    pub(crate) snapshot: RecoverySnapshot,
}

/// Stop decision reserved after the authority verified its exact checkpoint
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointStopDecision {
    pub(crate) binding: ReleaseCheckpointBinding,
    pub(crate) reservation_id: ReleaseStopReservationId,
    pub(crate) selected_checkpoint: VerifiedCheckpointPublication,
}

/// Exact trainer task cancellation committed with its saved stop decision
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointCancellation {
    pub(crate) task_id: TaskId,
    pub(crate) cancel_requested_at: DateTime<Utc>,
}

/// Durable checkpoint-evidence phase stored beside the resource loan
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ReleaseCheckpointPhase {
    /// New release action with no watcher identity yet
    WatcherBindingPending,
    /// Exact watcher identity is bound to a persisted pre-action baseline
    BaselineCaptured {
        /// Saved baseline and all immutable owner identities
        baseline: ReleaseCheckpointBaseline,
    },
    /// Exact-task stop request is durably reserved after checkpoint verification
    StopReserved {
        /// Baseline used to reject pre-existing publications
        baseline: ReleaseCheckpointBaseline,
        /// Fixed checkpoint selected by the authority
        decision: Box<ReleaseCheckpointStopDecision>,
    },
    /// Exact trainer cancellation committed with the saved stop decision
    CancellationCommitted {
        /// Baseline used to reject pre-existing publications
        baseline: ReleaseCheckpointBaseline,
        /// Fixed checkpoint selected by the authority
        decision: Box<ReleaseCheckpointStopDecision>,
        /// Exact task cancellation marker committed in the same transaction
        cancellation: ReleaseCheckpointCancellation,
    },
}

/// Durable evidence state for one exact AwaitingRelease action
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseCheckpointState {
    pub(crate) action: ReleaseCheckpointAction,
    pub(crate) phase: ReleaseCheckpointPhase,
}

impl ReleaseCheckpointState {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.action.observed_background_task.0.is_nil() || self.action.state_revision.get() == 0
        {
            return Err("release checkpoint action identity is invalid");
        }

        let baseline = match &self.phase {
            ReleaseCheckpointPhase::WatcherBindingPending => return Ok(()),
            ReleaseCheckpointPhase::BaselineCaptured { baseline }
            | ReleaseCheckpointPhase::StopReserved { baseline, .. }
            | ReleaseCheckpointPhase::CancellationCommitted { baseline, .. } => baseline,
        };
        validate_release_checkpoint_binding(&self.action, &baseline.binding)?;
        if !baseline.snapshot.is_for(&baseline.binding.attempt_binding) {
            return Err("checkpoint baseline belongs to a different trainer attempt");
        }

        let decision = match &self.phase {
            ReleaseCheckpointPhase::StopReserved { decision, .. }
            | ReleaseCheckpointPhase::CancellationCommitted { decision, .. } => decision,
            ReleaseCheckpointPhase::WatcherBindingPending
            | ReleaseCheckpointPhase::BaselineCaptured { .. } => return Ok(()),
        };
        if let ReleaseCheckpointPhase::CancellationCommitted { cancellation, .. } = &self.phase
            && cancellation.task_id != self.action.observed_background_task
        {
            return Err("checkpoint cancellation differs from its observed trainer task");
        }
        validate_release_checkpoint_binding(&self.action, &decision.binding)?;
        if decision.binding != baseline.binding
            || decision.selected_checkpoint.binding != baseline.binding.attempt_binding
            || baseline
                .snapshot
                .contains_generation(&decision.selected_checkpoint.generation_id)
            || decision.selected_checkpoint.path
                != baseline
                    .binding
                    .association
                    .canonical_runtime_root
                    .join("published")
                    .join(&decision.selected_checkpoint.generation_id)
        {
            return Err("checkpoint stop decision differs from its verified baseline");
        }

        Ok(())
    }
}

fn validate_release_checkpoint_binding(
    action: &ReleaseCheckpointAction,
    binding: &ReleaseCheckpointBinding,
) -> Result<(), &'static str> {
    let intent = &binding.watcher_intent;
    let association = &binding.association;
    if binding.action != *action
        || association.resource_id != action.resource_id
        || association.task_id != action.observed_background_task
        || association.authority_machine.as_uuid().is_nil()
        || association.attempt_binding != binding.attempt_binding
        || !association.canonical_runtime_root.is_absolute()
        || intent.action_id != action.action_id
        || intent.state_revision != action.state_revision
        || intent.observed_background_task != action.observed_background_task
        || intent.validate().is_err()
    {
        return Err("release checkpoint binding does not match the saved action");
    }

    binding
        .attempt_binding
        .validate()
        .map_err(|_| "release checkpoint attempt binding is invalid")
}

/// Typed result of checking whether an exact-task stop can be reserved
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReleaseCheckpointStopOutcome {
    /// A new stop decision and checkpoint identity were persisted
    Reserved(ReleaseCheckpointStopDecision),
    /// An exact retry returned the previously persisted decision
    AlreadyReserved(ReleaseCheckpointStopDecision),
    /// The exact task has not started yet
    WaitingForTaskStart,
    /// No complete new checkpoint is available yet
    WaitingForCheckpoint,
    /// A complete final result takes precedence over a checkpoint stop
    CompletedResultAwaitingTaskExit,
    /// The task already published its successful final result
    AlreadyCompleted,
    /// The saved task or publication state needs attention
    Attention(WatcherAttention),
}
