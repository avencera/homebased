//! Durable checkpoint-stop evidence for one release action

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use super::error::{ResourceStoreError, TrainerAttemptAssociationStoreError};
use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::resource::trainer_publication::WatcherError;
use crate::resource::{ActionId, ReleaseCheckpointState, ResourceId};

/// Failure to capture or reserve exact checkpoint-stop evidence
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReleaseCheckpointError {
    /// Resource authority or stored resource data failed validation
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// Saved trainer association could not be read or does not match the action
    #[error(transparent)]
    TrainerAssociation(#[from] TrainerAttemptAssociationStoreError),
    /// Trainer publication evidence could not be verified
    #[error(transparent)]
    Watcher(#[from] WatcherError),
    /// The exact task row could not be read
    #[error(transparent)]
    Task(#[from] AppError),
    /// The exact action is not in the expected AwaitingRelease phase
    #[error("release action {action_id:?} is not awaiting checkpoint evidence")]
    NotAwaitingRelease {
        /// Action required by the caller
        action_id: ActionId,
    },
    /// A watcher identity must be bound before baseline capture
    #[error("release action {action_id:?} has no saved watcher identity")]
    WatcherIntentMissing {
        /// Action that still needs its preallocated watcher identity
        action_id: ActionId,
    },
    /// The baseline has not been durably captured for this watcher
    #[error("release action {action_id:?} has no durable checkpoint baseline")]
    BaselineMissing {
        /// Action whose baseline is absent
        action_id: ActionId,
    },
    /// No exact stop decision is reserved for the requested release action
    #[error("release action {action_id:?} has no reserved stop decision")]
    StopDecisionMissing {
        /// Action whose stop decision is absent
        action_id: ActionId,
    },
    /// The associated trainer task is missing on the authority
    #[error("observed trainer task {task_id} is missing locally")]
    TaskMissing {
        /// Exact task observed by the release action
        task_id: TaskId,
    },
    /// The observed trainer task is not running on the authority
    #[error("observed trainer task {task_id} is not running (state {state:?})")]
    TrainerTaskNotRunning {
        /// Exact task observed by the release action
        task_id: TaskId,
        /// Current persisted process state
        state: ProcessStatus,
    },
    /// The accepted trainer command or its task row changed after association
    #[error("accepted trainer command binding changed for task {task_id}")]
    TrainerCommandBindingChanged {
        /// Exact task observed by the release action
        task_id: TaskId,
    },
    /// Another cancellation was already recorded for the trainer task
    #[error("trainer task {task_id} already has a cancellation marker")]
    TrainerCancellationConflict {
        /// Exact task observed by the release action
        task_id: TaskId,
    },
    /// The accepted watcher identity no longer matches its saved launch intent
    #[error("release watcher task {task_id} conflicts with its saved launch identity")]
    WatcherIdentityConflict {
        /// Exact watcher task reserved for this release action
        task_id: TaskId,
    },
    /// The supplied stop decision is not the saved action decision
    #[error("stop decision for release action {action_id:?} does not match its reservation")]
    StopDecisionMismatch {
        /// Action whose decision did not match
        action_id: ActionId,
    },
    /// The release action has no saved trainer-attempt association
    #[error("release action has no trainer association for task {task_id}")]
    TrainerAssociationMissing {
        /// Exact trainer task observed by the release action
        task_id: TaskId,
    },
    /// The action or its saved checkpoint evidence changed
    #[error("release checkpoint evidence conflicts with the saved action")]
    Conflict,
    /// The selected publication no longer matches its saved immutable identity
    #[error("selected checkpoint for release action {action_id:?} changed")]
    SelectedCheckpointChanged {
        /// Action whose saved publication changed
        action_id: ActionId,
    },
    /// Persisted evidence does not satisfy its typed invariants
    #[error("invalid saved release checkpoint evidence: {0}")]
    InvalidStoredEvidence(&'static str),
    /// SQLite or stored data failed
    #[error("release checkpoint evidence storage error: {0}")]
    Storage(#[from] rusqlite::Error),
    /// A typed evidence document could not be serialized
    #[error("release checkpoint evidence encoding failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Result data retained after the trainer cancellation marker commits
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReleaseCheckpointCancellationResult {
    /// Fixed stop decision that authorized cancellation
    pub(crate) decision: crate::resource::ReleaseCheckpointStopDecision,
    /// Exact trainer task and durable marker time
    pub(crate) cancellation: crate::resource::ReleaseCheckpointCancellation,
}

/// Typed result of committing a reserved exact-task cancellation
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReleaseCheckpointCancellationOutcome {
    /// Decision and task marker committed in one transaction
    Committed(ReleaseCheckpointCancellationResult),
    /// An exact retry returned the existing committed reservation
    AlreadyCommitted(ReleaseCheckpointCancellationResult),
    /// The reserved watcher task has not reached its accepted running boundary
    WatcherNotReady {
        /// Exact watcher task reserved for this release action
        watcher_task_id: TaskId,
    },
}

pub(crate) fn release_checkpoint_state_for_action(
    conn: &Connection,
    resource_id: ResourceId,
    action_id: ActionId,
) -> Result<Option<(ReleaseCheckpointState, String)>, ReleaseCheckpointError> {
    let saved = conn
        .query_row(
            "SELECT resource_id, state_json FROM resource_release_checkpoint_states
             WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((saved_resource, state_json)) = saved else {
        return Ok(None);
    };
    let saved_resource = saved_resource
        .parse::<ResourceId>()
        .map_err(|_| ReleaseCheckpointError::InvalidStoredEvidence("resource id is invalid"))?;
    let state: ReleaseCheckpointState = serde_json::from_str(&state_json)
        .map_err(|error| ResourceStoreError::corrupt("release checkpoint state", error))?;
    state
        .validate()
        .map_err(ReleaseCheckpointError::InvalidStoredEvidence)?;
    if saved_resource != resource_id
        || state.action.resource_id != resource_id
        || state.action.action_id != action_id
    {
        return Err(ReleaseCheckpointError::InvalidStoredEvidence(
            "row identities do not match the typed state",
        ));
    }

    Ok(Some((state, state_json)))
}

pub(super) fn insert_release_checkpoint_state(
    tx: &Transaction<'_>,
    state: &ReleaseCheckpointState,
) -> Result<(), ReleaseCheckpointError> {
    state
        .validate()
        .map_err(ReleaseCheckpointError::InvalidStoredEvidence)?;
    tx.execute(
        "INSERT INTO resource_release_checkpoint_states (action_id, resource_id, state_json)
         VALUES (?1, ?2, ?3)",
        params![
            state.action.action_id.as_uuid().to_string(),
            state.action.resource_id.as_uuid().to_string(),
            serde_json::to_string(state)?,
        ],
    )?;
    Ok(())
}

pub(crate) fn update_release_checkpoint_state(
    tx: &Transaction<'_>,
    previous_json: &str,
    state: &ReleaseCheckpointState,
) -> Result<(), ReleaseCheckpointError> {
    state
        .validate()
        .map_err(ReleaseCheckpointError::InvalidStoredEvidence)?;
    let changed = tx.execute(
        "UPDATE resource_release_checkpoint_states SET state_json = ?1
         WHERE action_id = ?2 AND resource_id = ?3 AND state_json = ?4",
        params![
            serde_json::to_string(state)?,
            state.action.action_id.as_uuid().to_string(),
            state.action.resource_id.as_uuid().to_string(),
            previous_json,
        ],
    )?;
    if changed != 1 {
        return Err(ReleaseCheckpointError::Conflict);
    }

    Ok(())
}
