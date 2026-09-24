//! Release-watcher identity binding for one release action

use rusqlite::{Connection, TransactionBehavior, params};

use super::checkpoint::ReleaseCheckpointError;
use super::codec::{encode_json, sqlite_integer, stored_column, stored_json};
use super::error::{ConflictReason, ResourceStoreError};
use super::notice::select_supervisor_notice_record_by_action;
use super::rows::{check_authority, select_non_closed_loan, select_resource};
use crate::domain::TaskId;
use crate::machine::MachineId;
use crate::resource::{
    Loan, LoanId, LoanPhase, LoanState, ReleaseWatcherIntent, ResourceId, SupervisorAddress,
    SupervisorNoticePayload,
};
use crate::spec::NormalizedSpec;
use crate::store::IdentityError;
use crate::submission::RequestId;

/// Result of accepting the fixed identity for one local release watcher
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReleaseWatcherAcceptance {
    /// The watcher row, route, identity, event, and loan binding were inserted
    Inserted { task: TaskId },
    /// An exact saved acceptance was returned without adding durable records
    Existing {
        task: TaskId,
        state: crate::domain::TaskState,
    },
    /// The supervisor runs elsewhere and must launch through the remote action path
    UnsupportedRemoteSupervisor {
        /// Machine that owns this resource
        authority_machine: MachineId,
        /// Supervisor that must receive a future remote launch request
        supervisor: SupervisorAddress,
    },
}

/// Fixed task and owner data required for one release-watcher acceptance
pub(crate) struct ReleaseWatcherAcceptanceInput {
    /// Authority machine recorded on the resource
    pub(crate) authority_machine: MachineId,
    /// Resource whose release action owns this watcher
    pub(crate) resource_id: ResourceId,
    /// Supervisor address recorded on the resource
    pub(crate) supervisor: SupervisorAddress,
    /// Fixed action, request, task, and normalized-spec identities
    pub(crate) intent: ReleaseWatcherIntent,
    /// Queued task row to persist with the accepted command
    pub(crate) row: crate::domain::TaskRow,
    /// Normalized command used for the accepted task and origin route
    pub(crate) spec: NormalizedSpec,
    /// Local callback environment and executable
    pub(crate) callback: crate::submission::CallbackContext,
}

/// Failure to accept the exact release-watcher identity
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReleaseWatcherAcceptanceError {
    /// Release action or fixed identity does not match the saved authority state
    #[error("release watcher acceptance conflict")]
    Conflict,
    /// Resource authority state rejected the acceptance
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// Route or executor identity data is invalid or conflicting
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// The exact release action has no verified checkpoint baseline
    #[error(transparent)]
    Checkpoint(#[from] ReleaseCheckpointError),
    /// Task, event, or SQLite storage failed
    #[error(transparent)]
    Storage(#[from] crate::error::AppError),
    /// SQLite transaction or storage operation failed
    #[error("release watcher acceptance storage error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

/// Bind one preallocated Homebased watcher launch identity to the exact release action
pub(crate) fn bind_release_watcher_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    intent: ReleaseWatcherIntent,
) -> Result<ReleaseWatcherIntent, ResourceStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let result = bind_release_watcher_intent_on(&tx, authority_machine, resource_id, intent)?;
    tx.commit()?;
    Ok(result)
}

pub(crate) fn validate_release_watcher_for_local_acceptance(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    supervisor: SupervisorAddress,
    intent: &ReleaseWatcherIntent,
) -> Result<SupervisorAddress, ResourceStoreError> {
    let resource =
        select_resource(conn, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_authority(resource.authority_machine(), authority_machine)?;
    if resource.supervisor != supervisor {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ResourceAssignmentChanged,
        ));
    }
    validate_release_watcher_intent_on(conn, authority_machine, resource_id, intent)?;
    Ok(resource.supervisor)
}

pub(crate) fn bind_release_watcher_intent_on(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    intent: ReleaseWatcherIntent,
) -> Result<ReleaseWatcherIntent, ResourceStoreError> {
    let mut loan =
        validate_release_watcher_intent_on(conn, authority_machine, resource_id, &intent)?;
    let LoanState::Active {
        phase: LoanPhase::AwaitingRelease { watcher_intent, .. },
    } = &mut loan.state
    else {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ReleaseActionChanged,
        ));
    };
    if let Some(saved) = watcher_intent {
        return if saved == &intent {
            Ok(saved.clone())
        } else {
            Err(ResourceStoreError::Conflict(
                ConflictReason::WatcherIntentMismatch,
            ))
        };
    }

    // keep the action revision stable for completion using the release notice
    *watcher_intent = Some(intent.clone());
    let loan_state_json = encode_json(&loan.state)?;
    let changed = conn.execute(
        "UPDATE loans SET state_json = ?1
         WHERE id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'active'
           AND json_extract(state_json, '$.phase.type') = 'awaiting_release'
           AND json_extract(state_json, '$.phase.action_id') = ?4
           AND json_extract(state_json, '$.phase.observed_background_task') = ?5
           AND (
               json_type(state_json, '$.phase.watcher_intent') IS NULL
               OR json_type(state_json, '$.phase.watcher_intent') = 'null'
           )
           AND EXISTS (
               SELECT 1 FROM resources
               WHERE id = ?3 AND authority_machine = ?6
                 AND state_revision = ?7 AND registered_background_task = ?5
           )",
        params![
            loan_state_json,
            loan.id.as_uuid().to_string(),
            resource_id.as_uuid().to_string(),
            intent.action_id.as_uuid().to_string(),
            intent.observed_background_task.to_string(),
            authority_machine.as_uuid().to_string(),
            sqlite_integer(intent.state_revision.get())?,
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    }
    Ok(intent)
}

pub(crate) fn validate_release_watcher_intent_on(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    intent: &ReleaseWatcherIntent,
) -> Result<Loan, ResourceStoreError> {
    let resource =
        select_resource(conn, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_authority(resource.authority_machine(), authority_machine)?;
    let watcher_task_id = intent.watcher_task_id.as_task_id();
    if resource.state_revision != intent.state_revision {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ResourceRevisionChanged,
        ));
    }
    if resource.registered_background_task != Some(intent.observed_background_task) {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ResourceAssignmentChanged,
        ));
    }
    if intent.validate().is_err() {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::WatcherIdentityInvalid,
        ));
    }

    let loan = select_non_closed_loan(conn, resource_id)?
        .ok_or(ResourceStoreError::Conflict(ConflictReason::LoanChanged))?;
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task,
                watcher_intent,
            },
    } = &loan.state
    else {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ReleaseActionChanged,
        ));
    };
    if *action_id != intent.action_id
        || *observed_background_task != intent.observed_background_task
    {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ReleaseActionChanged,
        ));
    }

    let Some((notice, _)) = select_supervisor_notice_record_by_action(conn, intent.action_id)?
    else {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ReleaseNoticeMismatch,
        ));
    };
    if notice.loan_id != loan.id
        || notice.state_revision != intent.state_revision
        || notice.payload
            != (SupervisorNoticePayload::ReleaseRequired {
                task_id: intent.observed_background_task,
            })
    {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ReleaseNoticeMismatch,
        ));
    }

    if release_watcher_identity_is_claimed(conn, loan.id, intent.request_id, watcher_task_id)? {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::WatcherIdentityClaimed,
        ));
    }
    if let Some(saved) = watcher_intent {
        if saved != intent {
            return Err(ResourceStoreError::Conflict(
                ConflictReason::WatcherIntentMismatch,
            ));
        }
        return Ok(loan);
    }
    if request_identity_exists(conn, intent.request_id)?
        || local_task_identity_exists(conn, watcher_task_id)?
    {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::WatcherIdentityClaimed,
        ));
    }
    Ok(loan)
}

fn local_task_identity_exists(conn: &Connection, task_id: TaskId) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM tasks WHERE id = ?1
            UNION ALL SELECT 1 FROM executor_identities WHERE task_id = ?1
            UNION ALL SELECT 1 FROM resource_requests WHERE task_id = ?1
            UNION ALL SELECT 1 FROM resource_request_preventions WHERE task_id = ?1
            UNION ALL SELECT 1 FROM origin_routes WHERE task_id = ?1
        )",
        [task_id.to_string()],
        |row| row.get(0),
    )
}

fn request_identity_exists(
    conn: &Connection,
    request_id: RequestId,
) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM origin_routes WHERE request_id = ?1
            UNION ALL SELECT 1 FROM resource_requests WHERE request_id = ?1
            UNION ALL SELECT 1 FROM resource_request_preventions WHERE request_id = ?1
        )",
        [request_id.0.to_string()],
        |row| row.get(0),
    )
}

fn release_watcher_identity_is_claimed(
    conn: &Connection,
    loan_id: LoanId,
    request_id: RequestId,
    task_id: TaskId,
) -> Result<bool, ResourceStoreError> {
    let mut statement = conn.prepare(
        "SELECT state_json FROM loans
         WHERE id != ?1
           AND (
               json_type(state_json, '$.phase.watcher_intent') IS NOT NULL
               OR json_type(state_json, '$.last_safe_phase.watcher_intent') IS NOT NULL
           )",
    )?;

    // decode saved bindings so incomplete identities cannot be ignored by uniqueness checks
    let states = statement.query_map([loan_id.as_uuid().to_string()], |row| {
        Ok(stored_column::<String>(row, 0, "loan state")
            .and_then(|json| stored_json::<LoanState>("loan state", &json)))
    })?;

    for state in states {
        let state = state??;
        let intent = match state {
            LoanState::Active {
                phase:
                    LoanPhase::AwaitingRelease {
                        watcher_intent: Some(intent),
                        ..
                    },
            }
            | LoanState::NeedsAttention {
                last_safe_phase:
                    LoanPhase::AwaitingRelease {
                        watcher_intent: Some(intent),
                        ..
                    },
                ..
            } => intent,
            _ => continue,
        };

        if intent.request_id == request_id || intent.watcher_task_id.as_task_id() == task_id {
            return Ok(true);
        }
    }

    Ok(false)
}
