//! Exact trainer ownership-lock release for background slot and restore transitions
//!
//! The maintained direct-segment wrapper starts its GPU worker in a new session,
//! so a confirmed exit of the wrapper's process group does not show that the
//! worker exited. The worker holds the runtime `.segment.lock` until it exits
//! Before a transition lets a new GPU worker start, or closes a Restoring loan,
//! after such a task ended, the authority takes that exact lock. The immutable
//! trainer-attempt association names the lock, and the authority holds it until
//! the transaction that records the transition commits

use std::path::PathBuf;

use rusqlite::Connection;

use super::trainer_association::trainer_association_by_resource_and_task;
use crate::domain::TaskId;
use crate::machine::MachineId;
use crate::resource::command_shape::direct_segment_runtime_root;
use crate::resource::foreground::{self, CommandOwnershipContract};
use crate::resource::ownership_lock::{
    OwnershipLockGuard, OwnershipLockIdentity, OwnershipLockProbe, OwnershipLockProbeError,
    probe_segment_ownership_lock, verify_ownership_lock_guard,
};
use crate::resource::store::{ResourceStoreError, TrainerAttemptAssociationStoreError};
use crate::resource::{Resource, ResourceTaskOwnershipRisk};
use crate::spec::NormalizedSpec;
use crate::store::identity::executor_identity_on;
use crate::submission::{ExecutorIdentity, normalized_spec_sha256};

/// Why an ended task has no verified release of its trainer ownership lock
#[derive(Debug, thiserror::Error)]
pub(crate) enum TrainerLockReleaseGap {
    /// The task has no accepted identity with a spec on this authority
    #[error("task {task_id} has no accepted run records on this authority")]
    RunRecordsMissing {
        /// Task whose records are missing
        task_id: TaskId,
    },
    /// The task command has no ownership contract that a release can verify
    #[error("task {task_id} command ownership cannot be verified: {risk:?}")]
    UnsupportedCommand {
        /// Ended task
        task_id: TaskId,
        /// Why the command can hide or detach its work
        risk: ResourceTaskOwnershipRisk,
    },
    /// No trainer-attempt association names the lock that the task used
    #[error("task {task_id} has no trainer-attempt association")]
    AssociationMissing {
        /// Task whose association names the lock
        task_id: TaskId,
    },
    /// The association does not name the same run or runtime root as the ended task
    #[error("trainer-attempt association of task {task_id} does not match the ended run")]
    AssociationMismatch {
        /// Task whose association names the lock
        task_id: TaskId,
    },
    /// A process still holds the exact saved lock
    #[error("trainer ownership lock of task {task_id} is still held")]
    OwnershipHeld {
        /// Task whose association names the lock
        task_id: TaskId,
    },
    /// The exact saved lock could not be verified or held
    #[error(transparent)]
    OwnershipLock(#[from] OwnershipLockProbeError),
}

/// Failure to prove or hold one trainer ownership-lock release
#[derive(Debug)]
pub(super) enum TrainerLockReleaseError {
    /// The saved records or the lock do not prove the release
    Unproven(TrainerLockReleaseGap),
    /// SQLite or stored data failed, so the result is unknown
    Store(ResourceStoreError),
}

impl From<TrainerLockReleaseGap> for TrainerLockReleaseError {
    fn from(gap: TrainerLockReleaseGap) -> Self {
        Self::Unproven(gap)
    }
}

impl From<ResourceStoreError> for TrainerLockReleaseError {
    fn from(error: ResourceStoreError) -> Self {
        Self::Store(error)
    }
}

/// Witness that the ownership contract of an ended task's command requires
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EndedTaskWitness {
    /// Native foreground command; the confirmed process-group exit is the witness
    ProcessGroupExit,
    /// Maintained direct-segment trainer; only its released lock is the witness
    TrainerLock,
}

/// Exclusive hold on one exact released trainer lock
///
/// Keep this value alive until the transaction that relies on it commits, and
/// call [`HeldTrainerRelease::recheck`] just before that commit
#[must_use = "keep the lock held until the transaction that relies on it commits"]
#[derive(Debug)]
pub(super) struct HeldTrainerRelease {
    runtime_root: PathBuf,
    identity: OwnershipLockIdentity,
    guard: OwnershipLockGuard,
}

impl HeldTrainerRelease {
    /// Check that the guard still holds the file at the exact saved lock path
    pub(super) fn recheck(&self) -> Result<(), TrainerLockReleaseGap> {
        verify_ownership_lock_guard(&self.runtime_root, self.identity, &self.guard)
            .map_err(TrainerLockReleaseGap::from)
    }
}

/// Classify the witness that an ended task needs from its accepted command
pub(super) fn ended_task_witness(
    conn: &Connection,
    authority_machine: MachineId,
    task_id: TaskId,
) -> Result<EndedTaskWitness, TrainerLockReleaseError> {
    let spec = accepted_spec(conn, authority_machine, task_id)?;
    let command =
        foreground::task_command(&spec).ok_or(TrainerLockReleaseGap::UnsupportedCommand {
            task_id,
            risk: ResourceTaskOwnershipRisk::UninspectableEntryPoint,
        })?;
    match CommandOwnershipContract::for_return_command(command) {
        Ok(CommandOwnershipContract::ForegroundExecutable) => {
            Ok(EndedTaskWitness::ProcessGroupExit)
        }
        Ok(CommandOwnershipContract::DirectSegmentTrainer) => Ok(EndedTaskWitness::TrainerLock),
        Err(risk) => Err(TrainerLockReleaseGap::UnsupportedCommand { task_id, risk }.into()),
    }
}

/// Take the exact trainer lock that an ended direct-segment task used
///
/// `witness_task` owns the association that saved the lock identity. It is the
/// ended task itself, or the stopped run that a same-run resume continued in the
/// same runtime root. Both accepted commands must name that `--runtime-root`,
/// and the witness spec must still match its association. A held, missing,
/// replaced, or unverifiable lock fails closed
pub(super) fn hold_released_trainer_lock(
    conn: &Connection,
    resource: &Resource,
    ended_task: TaskId,
    witness_task: TaskId,
) -> Result<HeldTrainerRelease, TrainerLockReleaseError> {
    let authority_machine = resource.authority_machine();
    let mismatch = TrainerLockReleaseGap::AssociationMismatch {
        task_id: witness_task,
    };
    let association = trainer_association_by_resource_and_task(conn, resource.id, witness_task)
        .map_err(|error| match error {
            TrainerAttemptAssociationStoreError::Storage(error) => {
                TrainerLockReleaseError::Store(error.into())
            }
            TrainerAttemptAssociationStoreError::Resource(error) => {
                TrainerLockReleaseError::Store(error)
            }
            // a stored association that no longer decodes cannot name a lock
            _ => TrainerLockReleaseGap::AssociationMismatch {
                task_id: witness_task,
            }
            .into(),
        })?
        .ok_or(TrainerLockReleaseGap::AssociationMissing {
            task_id: witness_task,
        })?;
    if association.authority_machine() != authority_machine
        || association.resource_id() != resource.id
        || association.task_id() != witness_task
    {
        return Err(mismatch.into());
    }

    let witness_spec = accepted_spec(conn, authority_machine, witness_task)?;
    let witness_digest = normalized_spec_sha256(&witness_spec)
        .map_err(|error| ResourceStoreError::TaskRow(error.into()))?;
    if witness_digest != association.normalized_spec_sha256() {
        return Err(mismatch.into());
    }
    let ended_spec = if ended_task == witness_task {
        witness_spec.clone()
    } else {
        accepted_spec(conn, authority_machine, ended_task)?
    };
    let runtime_root = |spec: &NormalizedSpec| {
        foreground::task_command(spec).and_then(direct_segment_runtime_root)
    };
    let witness_root = runtime_root(&witness_spec);
    if witness_root.is_none() || witness_root != runtime_root(&ended_spec) {
        return Err(mismatch.into());
    }

    let attempt = association.verified_attempt();
    let runtime_root = attempt.canonical_runtime_root().to_path_buf();
    let identity = attempt.ownership_lock_identity();
    match probe_segment_ownership_lock(&runtime_root, identity) {
        OwnershipLockProbe::ExactOwnershipReleased(guard) => Ok(HeldTrainerRelease {
            runtime_root,
            identity,
            guard,
        }),
        OwnershipLockProbe::OwnershipHeld => Err(TrainerLockReleaseGap::OwnershipHeld {
            task_id: witness_task,
        }
        .into()),
        OwnershipLockProbe::Attention(error) => Err(TrainerLockReleaseGap::from(error).into()),
    }
}

/// Read the spec of one task's accepted identity executed by this authority
fn accepted_spec(
    conn: &Connection,
    authority_machine: MachineId,
    task_id: TaskId,
) -> Result<NormalizedSpec, TrainerLockReleaseError> {
    let identity = executor_identity_on(conn, task_id).map_err(ResourceStoreError::from)?;
    let Some(ExecutorIdentity::Accepted(record)) = identity else {
        return Err(TrainerLockReleaseGap::RunRecordsMissing { task_id }.into());
    };
    if !record.is_executed_by(task_id, authority_machine) {
        return Err(TrainerLockReleaseGap::RunRecordsMissing { task_id }.into());
    }

    record
        .current_spec()
        .cloned()
        .ok_or_else(|| TrainerLockReleaseGap::RunRecordsMissing { task_id }.into())
}
