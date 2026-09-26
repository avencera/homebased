//! Saved return decision windows and the queue serving that follows an expired one
//!
//! Every ReturnRequired notice opens the decision window of its action in the
//! same transaction, so an AwaitingReturn loan always has one. When the window
//! closes with a request queued, one IMMEDIATE transaction moves the same loan
//! to Serving, assigns the request, advances the resource revision, and saves
//! the deadline receipt that the new release provenance names. The return
//! context stays on the loan, so the next drained queue opens a new return
//! action for the same obligation

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use super::codec::{StoredReadError, encode_json, stored_id, stored_json};
use super::error::{ConflictReason, ResourceStoreError};
use super::queue::next_queued_request_for_authority;
use super::revision::swap_resource_revision;
use super::rows::{check_authority, select_non_closed_loan, select_resource};
use crate::domain::TaskId;
use crate::machine::MachineId;
use crate::resource::{
    ActionId, Loan, LoanId, LoanPhase, LoanState, NilIdentity, Resource, ResourceId,
    ResourceRequest, ResourceRequestState, ResourceRevision, ReturnContext, ReturnDecisionWindow,
    ServingReleaseProvenance,
};
use crate::submission::RequestId;

/// Evidence that queued work took the resource from an undecided return action
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReturnDeadlineReceipt {
    /// Expired window, as saved when the queue took the resource
    window: ReturnDecisionWindow,
    /// Authority that committed the transition
    authority_machine: MachineId,
    /// Return obligation that the loan keeps while it serves
    return_context: ReturnContext,
    /// Registration that the loan kept while it waited for the decision
    registered_background_task: Option<TaskId>,
    /// Request that the loan started to serve
    request_id: RequestId,
    /// Resource revision of the AwaitingReturn loan
    expected_state_revision: ResourceRevision,
    /// Resource revision committed with the transition
    state_revision: ResourceRevision,
    /// Authority time at which the transition committed
    served_at: DateTime<Utc>,
}

/// Result of checking the decision window of the current return action
#[derive(Debug, Clone)]
pub(crate) enum ReturnDeadlineOutcome {
    /// The resource has no loan that awaits a return decision
    NotAwaiting,
    /// The supervisor still has time to decide
    Open {
        /// Current window of the pending action
        window: ReturnDecisionWindow,
    },
    /// The window closed, but no request waits for the resource
    NoQueuedRequest,
    /// The window closed, and the same loan now serves the next queued request
    Served {
        // boxed because both records are large and the other variants are small
        /// Serving loan that keeps the return context
        loan: Box<Loan>,
        /// Request assigned by the transition
        request: Box<ResourceRequest>,
    },
}

/// Open the decision window of one return action in the caller's transaction
///
/// A retry of the same notice finds the saved window and keeps it
pub(crate) fn open_return_window_on(
    conn: &Connection,
    action_id: ActionId,
    loan_id: LoanId,
    opened_at: DateTime<Utc>,
) -> Result<(), rusqlite::Error> {
    let resource_id: String = conn.query_row(
        "SELECT resource_id FROM loans WHERE id = ?1",
        [loan_id.as_uuid().to_string()],
        |row| row.get(0),
    )?;
    let resource_id: ResourceId = saved_identity("loan resource identity", &resource_id)?;
    let window = ReturnDecisionWindow::open(action_id, loan_id, resource_id, opened_at);
    let window_json = serde_json::to_string(&window)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "INSERT INTO resource_return_windows (action_id, loan_id, resource_id, window_json)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (action_id) DO NOTHING",
        params![
            action_id.as_uuid().to_string(),
            loan_id.as_uuid().to_string(),
            resource_id.as_uuid().to_string(),
            window_json,
        ],
    )?;
    Ok(())
}

/// Open a window at `opened_at` for every AwaitingReturn loan saved without one
///
/// Loans that entered AwaitingReturn before windows existed get their full
/// grace from the upgrade, not from their unknown opening time
pub(crate) fn open_missing_return_windows_on(
    conn: &Connection,
    opened_at: DateTime<Utc>,
) -> Result<(), rusqlite::Error> {
    let pending = {
        let mut statement = conn.prepare(
            "SELECT json_extract(loan.state_json, '$.phase.action_id'), loan.id
             FROM loans AS loan
             WHERE json_extract(loan.state_json, '$.type') = 'active'
               AND json_extract(loan.state_json, '$.phase.type') = 'awaiting_return'
               AND NOT EXISTS (
                   SELECT 1 FROM resource_return_windows AS window
                   WHERE window.action_id = json_extract(loan.state_json, '$.phase.action_id')
               )",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for (action_id, loan_id) in pending {
        let action_id: ActionId = saved_identity("awaiting return action identity", &action_id)?;
        let loan_id: LoanId = saved_identity("awaiting return loan identity", &loan_id)?;
        open_return_window_on(conn, action_id, loan_id, opened_at)?;
    }
    Ok(())
}

/// Parse one saved identity for a step whose callers handle only SQLite errors
///
/// Notice insertion and the schema upgrade run on plain rusqlite results, so a
/// nil or malformed identity becomes a conversion failure that aborts them
fn saved_identity<T>(what: &'static str, text: &str) -> Result<T, rusqlite::Error>
where
    T: TryFrom<uuid::Uuid, Error = NilIdentity>,
{
    stored_id(what, text).map_err(|error| match error {
        StoredReadError::Storage(error) => error,
        StoredReadError::Corrupt { what, reason } => rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            format!("{what}: {reason}").into(),
        ),
    })
}

/// Read the saved decision window of one return action
pub(crate) fn return_window_on(
    conn: &Connection,
    action_id: ActionId,
) -> Result<Option<ReturnDecisionWindow>, ResourceStoreError> {
    let saved: Option<String> = conn
        .query_row(
            "SELECT window_json FROM resource_return_windows WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(saved) = saved else {
        return Ok(None);
    };
    let window: ReturnDecisionWindow = stored_json("return decision window", &saved)?;
    if window.action_id() != action_id {
        return Err(ResourceStoreError::corrupt(
            "return decision window",
            "saved window names another action",
        ));
    }
    Ok(Some(window))
}

/// Replace the deadline of a saved window in the caller's transaction
///
/// Only the deadline may change; the identities and opening time are fixed
pub(crate) fn save_held_return_window_on(
    conn: &Connection,
    saved: &ReturnDecisionWindow,
    held: &ReturnDecisionWindow,
) -> Result<(), ResourceStoreError> {
    if held.action_id() != saved.action_id()
        || held.loan_id() != saved.loan_id()
        || held.resource_id() != saved.resource_id()
        || held.opened_at() != saved.opened_at()
    {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    }
    let changed = conn.execute(
        "UPDATE resource_return_windows SET window_json = ?1
         WHERE action_id = ?2 AND window_json = ?3",
        params![
            encode_json(held)?,
            saved.action_id().as_uuid().to_string(),
            encode_json(saved)?,
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    }
    Ok(())
}

/// Serve the next queued request once the current return action's window closed
pub(crate) fn serve_after_return_deadline_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    now: DateTime<Utc>,
) -> Result<ReturnDeadlineOutcome, ResourceStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let resource =
        select_resource(&tx, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_authority(resource.authority_machine(), authority_machine)?;
    let Some(loan) = select_non_closed_loan(&tx, resource_id)? else {
        return Ok(ReturnDeadlineOutcome::NotAwaiting);
    };
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingReturn {
                action_id,
                return_context,
            },
    } = &loan.state
    else {
        return Ok(ReturnDeadlineOutcome::NotAwaiting);
    };
    let window = return_window_on(&tx, *action_id)?.ok_or_else(|| {
        ResourceStoreError::corrupt(
            "return decision window",
            "an AwaitingReturn loan has no saved window",
        )
    })?;
    if window.loan_id() != loan.id || window.resource_id() != resource_id {
        return Err(ResourceStoreError::corrupt(
            "return decision window",
            "saved window names another loan",
        ));
    }
    if !window.expired_at(now) {
        return Ok(ReturnDeadlineOutcome::Open { window });
    }
    let Some(mut request) = next_queued_request_for_authority(&tx, authority_machine, resource_id)?
    else {
        return Ok(ReturnDeadlineOutcome::NoQueuedRequest);
    };

    let state_revision = resource
        .state_revision
        .next()
        .ok_or(ResourceStoreError::Conflict(
            ConflictReason::RevisionExhausted,
        ))?;
    request.state = ResourceRequestState::Assigned { loan_id: loan.id };
    let changed = tx.execute(
        "UPDATE resource_requests SET state_json = ?1
         WHERE request_id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'queued'",
        params![
            encode_json(&request.state)?,
            request.request_id.0.to_string(),
            resource_id.as_uuid().to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestStateChanged,
        ));
    }

    let serving = Loan {
        id: loan.id,
        resource_id,
        state: LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: return_context.clone(),
                current_request_id: request.request_id,
                release_provenance: ServingReleaseProvenance::ReturnDeadlinePassed {
                    action_id: *action_id,
                },
            },
        },
    };
    let changed = tx.execute(
        "UPDATE loans SET state_json = ?1
         WHERE id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'active'
           AND json_extract(state_json, '$.phase.type') = 'awaiting_return'
           AND json_extract(state_json, '$.phase.action_id') = ?4",
        params![
            encode_json(&serving.state)?,
            loan.id.as_uuid().to_string(),
            resource_id.as_uuid().to_string(),
            action_id.as_uuid().to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    }
    if !swap_resource_revision::<ResourceStoreError>(
        &tx,
        authority_machine,
        resource_id,
        resource.state_revision,
        state_revision,
    )? {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ResourceRevisionChanged,
        ));
    }

    let receipt = ReturnDeadlineReceipt {
        window,
        authority_machine,
        return_context: return_context.clone(),
        registered_background_task: resource.registered_background_task,
        request_id: request.request_id,
        expected_state_revision: resource.state_revision,
        state_revision,
        served_at: now,
    };
    tx.execute(
        "INSERT INTO resource_return_deadline_servings
            (action_id, loan_id, resource_id, receipt_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            action_id.as_uuid().to_string(),
            loan.id.as_uuid().to_string(),
            resource_id.as_uuid().to_string(),
            encode_json(&receipt)?,
        ],
    )?;
    tx.commit()?;

    Ok(ReturnDeadlineOutcome::Served {
        loan: Box::new(serving),
        request: Box::new(request),
    })
}

/// Whether the deadline receipt of `action_id` still proves this Serving loan
pub(super) fn return_deadline_serving_matches_on(
    conn: &Connection,
    authority_machine: MachineId,
    resource: &Resource,
    loan: &Loan,
    return_context: &ReturnContext,
    action_id: ActionId,
) -> Result<bool, ResourceStoreError> {
    let saved: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_return_deadline_servings WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(saved) = saved else {
        return Ok(false);
    };
    let receipt: ReturnDeadlineReceipt = stored_json("return deadline receipt", &saved)?;
    Ok(receipt.window.action_id() == action_id
        && receipt.window.loan_id() == loan.id
        && receipt.window.resource_id() == resource.id
        && loan.resource_id == resource.id
        && receipt.authority_machine == authority_machine
        && resource.authority_machine() == authority_machine
        && receipt.return_context == *return_context
        && receipt.registered_background_task == resource.registered_background_task)
}
