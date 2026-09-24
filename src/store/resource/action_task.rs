//! Authority acceptance of action-bound tasks whose supervisor runs on another machine
//!
//! The supervisor machine saves the callback route before it sends a launch. The
//! authority saves the task row, remote executor identity, first queued event, and
//! exact action receipt in one IMMEDIATE transaction. A retry with the same receipt
//! observes the saved task and never inserts or spawns it again

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{resource_task_row_matches, validate_release_checkpoint_baseline_on};
use crate::domain::{ProcessStatus, TaskId, TaskRow};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::bound_action::{
    ActionTaskAcceptance, ActionTaskIdentity, ActionTaskReceipt, ResourceActionKind,
    ResourceActionRejection,
};
use crate::resource::release_watcher::ReleaseWatcherCommand;
use crate::resource::store::{
    ReleaseCheckpointError, select_non_closed_loan, select_resource,
    validate_release_watcher_intent_on,
};
use crate::resource::{
    ActionId, LoanPhase, LoanState, ReleaseWatcherIntent, SavedReleaseWatcherIntent,
    SupervisorActionAuthority,
};
use crate::spec::NormalizedSpec;
use crate::submission::{ExecutorIdentity, normalized_spec_sha256};

/// A remote action request was refused, or storage failed
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResourceActionError {
    /// The request does not fit the saved action, and nothing was written
    #[error("resource action rejected: {0:?}")]
    Rejected(ResourceActionRejection),
    /// SQLite, encoding, or stored data failed
    #[error(transparent)]
    Storage(#[from] AppError),
}

impl From<rusqlite::Error> for ResourceActionError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error.into())
    }
}

impl From<serde_json::Error> for ResourceActionError {
    fn from(error: serde_json::Error) -> Self {
        Self::Storage(error.into())
    }
}

impl From<crate::store::IdentityError> for ResourceActionError {
    fn from(error: crate::store::IdentityError) -> Self {
        match error {
            crate::store::IdentityError::Storage(error) => Self::Storage(error),
            crate::store::IdentityError::Conflict | crate::store::IdentityError::RouteNotFound => {
                Self::Rejected(ResourceActionRejection::IdentityConflict)
            }
        }
    }
}

fn rejected<T>(reason: ResourceActionRejection) -> Result<T, ResourceActionError> {
    Err(ResourceActionError::Rejected(reason))
}

/// Saved receipt and acceptance result for one remote action-bound launch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AcceptedActionTask {
    /// Exact action binding saved with the task
    pub(crate) receipt: ActionTaskReceipt,
    /// Whether this call inserted the task
    pub(crate) acceptance: ActionTaskAcceptance,
}

/// Canonical watcher task and fixed identities for one remote-supervisor acceptance
#[derive(Debug, Clone)]
pub(crate) struct RemoteReleaseWatcherAcceptanceInput {
    /// Exact action authority named by the supervisor machine
    pub(crate) authority: SupervisorActionAuthority,
    /// Background task named by the release action
    pub(crate) observed_background_task: TaskId,
    /// Fixed identities and digest from the prepared watcher
    pub(crate) task: ActionTaskIdentity,
    /// Queued row with the authority's environment and watcher executable
    pub(crate) row: TaskRow,
    /// Canonical watcher spec built by the authority
    pub(crate) spec: NormalizedSpec,
}

impl crate::store::Store {
    /// Accept one remote-supervisor release watcher once for its exact saved intent
    pub(crate) fn accept_remote_release_watcher_for_authority(
        &mut self,
        input: RemoteReleaseWatcherAcceptanceInput,
    ) -> Result<AcceptedActionTask, ResourceActionError> {
        accept_remote_release_watcher(&mut self.conn, input)
    }

    /// Read the task identities of release watchers bound by loans on this authority
    pub(crate) fn release_watcher_task_ids_for_authority(
        &self,
        authority_machine: MachineId,
    ) -> Result<Vec<TaskId>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT COALESCE(
                 json_extract(l.state_json, '$.phase.watcher_intent.watcher_task_id'),
                 json_extract(l.state_json, '$.last_safe_phase.watcher_intent.watcher_task_id')
             )
             FROM loans l JOIN resources r ON r.id = l.resource_id
             WHERE r.authority_machine = ?1
               AND json_extract(l.state_json, '$.type') != 'closed'",
        )?;
        let ids = statement
            .query_map([authority_machine.as_uuid().to_string()], |row| {
                row.get::<_, Option<String>>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .flatten()
            .map(|id| {
                id.parse().map_err(|_| AppError::Internal {
                    message: format!("loan has an invalid watcher task identity {id}"),
                })
            })
            .collect()
    }
}

fn accept_remote_release_watcher(
    conn: &mut Connection,
    input: RemoteReleaseWatcherAcceptanceInput,
) -> Result<AcceptedActionTask, ResourceActionError> {
    let RemoteReleaseWatcherAcceptanceInput {
        authority,
        observed_background_task,
        task,
        row,
        spec,
    } = input;
    let receipt = ActionTaskReceipt {
        kind: ResourceActionKind::ReleaseWatcher,
        authority,
        request_id: task.request_id,
        task_id: task.task_id,
        normalized_spec_sha256: task.normalized_spec_sha256,
    };
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // a saved receipt answers a retry even after the release action moved on
    if let Some(acceptance) = replay_action_task(&tx, &receipt)? {
        tx.commit()?;
        return Ok(AcceptedActionTask {
            receipt,
            acceptance,
        });
    }

    let resource_id = authority.resource_id;
    let authority_machine = authority.authority_machine;
    let resource = current_remote_supervisor(&tx, &authority)?;
    let loan = select_non_closed_loan(&tx, resource_id)
        .map_err(AppError::from)?
        .filter(|loan| loan.id == authority.loan_id)
        .ok_or(ResourceActionError::Rejected(
            ResourceActionRejection::ActionNotPending,
        ))?;
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task: saved_task,
                watcher_intent,
            },
    } = &loan.state
    else {
        return rejected(ResourceActionRejection::ActionNotPending);
    };
    if *action_id != authority.action_id || *saved_task != observed_background_task {
        return rejected(ResourceActionRejection::ActionNotPending);
    }
    let Some(SavedReleaseWatcherIntent::Complete(intent)) = watcher_intent else {
        return rejected(ResourceActionRejection::WatcherUnavailable {
            reason: "the release action has no complete watcher identity".into(),
        });
    };
    if intent.request_id != task.request_id || intent.watcher_task_id.as_task_id() != task.task_id {
        return rejected(ResourceActionRejection::IdentityConflict);
    }
    if intent.state_revision != authority.expected_state_revision {
        return rejected(ResourceActionRejection::StaleRevision {
            expected: authority.expected_state_revision,
            actual: intent.state_revision,
        });
    }
    if validate_release_watcher_intent_on(&tx, authority_machine, resource_id, intent).is_err() {
        return rejected(ResourceActionRejection::ActionNotPending);
    }
    // the authority accepts only its own command for these exact release identities
    let canonical = ReleaseWatcherCommand::from_intent(resource_id, intent)
        .normalized_spec(&row.binary, resource.supervisor.thread)?;
    let canonical_digest = normalized_spec_sha256(&canonical)?;
    if canonical_digest != intent.normalized_spec_sha256
        || task.normalized_spec_sha256 != canonical_digest
        || normalized_spec_sha256(&spec)? != canonical_digest
        || !resource_task_row_matches(&row, task.task_id, &canonical)
    {
        return rejected(ResourceActionRejection::SpecMismatch);
    }
    if let Err(error) =
        validate_release_checkpoint_baseline_on(&tx, authority_machine, resource_id, *action_id)
    {
        return match error {
            ReleaseCheckpointError::Storage(error) => Err(error.into()),
            error => rejected(ResourceActionRejection::WatcherUnavailable {
                reason: format!("checkpoint baseline is unavailable: {error}"),
            }),
        };
    }
    if action_task_receipt_by_action(&tx, authority.action_id)?.is_some() {
        return rejected(ResourceActionRejection::ConflictingRetry);
    }

    insert_action_task(&tx, &row, &canonical, &receipt)?;
    tx.commit()?;
    Ok(AcceptedActionTask {
        receipt,
        acceptance: ActionTaskAcceptance::Inserted,
    })
}

/// Check that the request names the current remote supervisor assignment and revision
pub(super) fn current_remote_supervisor(
    conn: &Connection,
    authority: &SupervisorActionAuthority,
) -> Result<crate::resource::Resource, ResourceActionError> {
    let resource = select_resource(conn, authority.resource_id)
        .map_err(AppError::from)?
        .ok_or(ResourceActionError::Rejected(
            ResourceActionRejection::ActionNotPending,
        ))?;
    if resource.authority_machine() != authority.authority_machine {
        return rejected(ResourceActionRejection::ActionNotPending);
    }
    if resource.supervisor != authority.supervisor
        || resource.assignment_revision != authority.assignment_revision
        || resource.supervisor.machine == resource.authority_machine()
    {
        return rejected(ResourceActionRejection::NotCurrentSupervisor);
    }
    if resource.state_revision != authority.expected_state_revision {
        return rejected(ResourceActionRejection::StaleRevision {
            expected: authority.expected_state_revision,
            actual: resource.state_revision,
        });
    }
    Ok(resource)
}

/// Return the saved acceptance for an exact retry, or refuse a different one
pub(super) fn replay_action_task(
    conn: &Connection,
    receipt: &ActionTaskReceipt,
) -> Result<Option<ActionTaskAcceptance>, ResourceActionError> {
    let by_task = action_task_receipt_by_task(conn, receipt.task_id)?;
    let by_action = action_task_receipt_by_action(conn, receipt.authority.action_id)?;
    let saved = match (by_task, by_action) {
        (None, None) => return Ok(None),
        (Some(saved), _) | (None, Some(saved)) => saved,
    };
    if saved != *receipt {
        return rejected(ResourceActionRejection::ConflictingRetry);
    }
    let row = remote_action_task_row_on(conn, &saved)?.ok_or(ResourceActionError::Rejected(
        ResourceActionRejection::IdentityConflict,
    ))?;
    Ok(Some(ActionTaskAcceptance::Existing {
        state: row.status(),
    }))
}

/// Insert the task records and the exact receipt for one first acceptance
pub(super) fn insert_action_task(
    conn: &Connection,
    row: &TaskRow,
    spec: &NormalizedSpec,
    receipt: &ActionTaskReceipt,
) -> Result<(), ResourceActionError> {
    crate::store::insert_remote_action_task_records_on(conn, row, spec, receipt).map_err(
        |error| match error {
            AppError::ClusterTaskConflict { .. } => {
                ResourceActionError::Rejected(ResourceActionRejection::IdentityConflict)
            }
            error => ResourceActionError::Storage(error),
        },
    )?;
    conn.execute(
        "INSERT INTO resource_action_task_receipts
             (task_id, request_id, action_id, resource_id, receipt_json)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            receipt.task_id.to_string(),
            receipt.request_id.0.to_string(),
            receipt.authority.action_id.as_uuid().to_string(),
            receipt.authority.resource_id.as_uuid().to_string(),
            serde_json::to_string(receipt)?,
        ],
    )?;
    Ok(())
}

/// Read the exact receipt that accepted one remote action-bound task
pub(crate) fn action_task_receipt_by_task(
    conn: &Connection,
    task_id: TaskId,
) -> Result<Option<ActionTaskReceipt>, AppError> {
    let json: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_action_task_receipts WHERE task_id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(json.map(|json| serde_json::from_str(&json)).transpose()?)
}

fn action_task_receipt_by_action(
    conn: &Connection,
    action_id: ActionId,
) -> Result<Option<ActionTaskReceipt>, AppError> {
    let json: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_action_task_receipts WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(json.map(|json| serde_json::from_str(&json)).transpose()?)
}

/// Return the task row when its identity and first event match one saved receipt
///
/// The authority keeps no origin route for these tasks, so the accepted identity
/// and first queued event must name the supervisor machine as callback origin
pub(crate) fn remote_action_task_row_on(
    conn: &Connection,
    receipt: &ActionTaskReceipt,
) -> Result<Option<TaskRow>, AppError> {
    let task_id = receipt.task_id;
    let Some(row) = crate::store::task_by_id_on(conn, task_id)? else {
        return Ok(None);
    };
    let identity =
        crate::store::executor_identity_for_resource_task_on(conn, task_id).map_err(|error| {
            AppError::Internal {
                message: format!("action task identity: {error}"),
            }
        })?;
    let Some(ExecutorIdentity::Accepted(record)) = identity else {
        return Ok(None);
    };
    let Some(spec) = record.current_spec() else {
        return Ok(None);
    };
    let first_event = crate::store::initial_queued_event_matches_on(
        conn,
        task_id,
        receipt.origin_machine(),
        receipt.execution_machine(),
    )
    .map_err(|error| AppError::Internal {
        message: format!("action task first event: {error}"),
    })?;
    let matches = record.task == task_id
        && record.origin_machine == receipt.origin_machine()
        && record.execution_machine == receipt.execution_machine()
        && record.has_valid_spec_owners()
        && record.state == row.status()
        && spec.thread == receipt.authority.supervisor.thread
        && normalized_spec_sha256(spec)? == receipt.normalized_spec_sha256
        && resource_task_row_matches(&row, task_id, spec)
        && first_event;

    Ok(matches.then_some(row))
}

/// Decide whether the remote-supervisor watcher accepted under one receipt is running
///
/// The receipt is the immutable acceptance route. It must name this intent, the
/// authority, and a supervisor on another machine. A later supervisor replacement
/// does not change the owner of a watcher that was already accepted
pub(super) fn remote_release_watcher_is_running_on(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: crate::resource::ResourceId,
    intent: &ReleaseWatcherIntent,
    receipt: &ActionTaskReceipt,
) -> Result<bool, ReleaseCheckpointError> {
    let task_id = intent.watcher_task_id.as_task_id();
    let conflict = || ReleaseCheckpointError::WatcherIdentityConflict { task_id };
    if receipt.kind != ResourceActionKind::ReleaseWatcher
        || receipt.task_id != task_id
        || receipt.request_id != intent.request_id
        || receipt.normalized_spec_sha256 != intent.normalized_spec_sha256
        || receipt.authority.action_id != intent.action_id
        || receipt.authority.expected_state_revision != intent.state_revision
        || receipt.authority.resource_id != resource_id
        || receipt.authority.authority_machine != authority_machine
        || receipt.authority.supervisor.machine == authority_machine
    {
        return Err(conflict());
    }
    let row = remote_action_task_row_on(conn, receipt)?.ok_or_else(conflict)?;
    Ok(row.status() == ProcessStatus::Running && row.pid().is_some())
}
