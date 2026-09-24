//! Durable supervisor notices and their bounded delivery attempts

use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};

use super::codec::{StoredReadError, collect_decoded, stored_column, stored_id, stored_json};
use crate::resource::{
    ActionId, AssignmentRevision, DeliveryAttemptId, LoanId, LoanState, NoticeId,
    SupervisorAddress, SupervisorNotice, SupervisorNoticeDelivery,
};

const SUPERVISOR_NOTICE_MAX_ATTEMPTS: u8 = 3;
pub(super) const INTERRUPTED_DELIVERY_ERROR: &str =
    "delivery attempt interrupted before settlement";
pub(super) const RETARGETED_DELIVERY_ERROR: &str =
    "delivery attempt invalidated by supervisor reassignment";

/// Failure to persist, query, or transition a supervisor notice
#[derive(Debug, thiserror::Error)]
pub(crate) enum SupervisorNoticeStoreError {
    /// The notice identity or action already belongs to different content
    #[error("supervisor notice identity conflict")]
    Conflict,
    /// The requested notice does not exist
    #[error("supervisor notice not found")]
    NotFound,
    /// The notice has already been delivered and cannot be retargeted
    #[error("delivered supervisor notice cannot be retargeted")]
    AlreadyDelivered,
    /// The loan no longer waits for the decision this notice asks for
    #[error("supervisor notice action is no longer awaited")]
    ActionNoLongerAwaited,
    /// A delivery attempt is already in flight
    #[error("supervisor notice delivery attempt is already in flight")]
    AttemptInFlight,
    /// The supplied delivery attempt is not the current in-flight attempt
    #[error("supervisor notice delivery attempt is stale")]
    StaleAttempt,
    /// The notice has exhausted its bounded delivery attempts
    #[error("supervisor notice delivery attempt budget is exhausted")]
    AttemptBudgetExhausted,
    /// The notice assignment changed since the caller read it
    #[error("supervisor notice assignment revision is stale")]
    StaleAssignmentRevision,
    /// A retarget revision must be newer than its expected revision
    #[error("supervisor notice assignment revision must increase")]
    AssignmentRevisionMustIncrease,
    /// A new notice must begin in the untouched pending state
    #[error("new supervisor notice must have zero pending attempts")]
    InvalidInitialDelivery,
    /// A saved notice cannot be decoded or fails its integrity check, so a retry cannot succeed
    #[error("corrupt stored {what}: {reason}")]
    CorruptRecord {
        /// Kind of record that failed to decode
        what: &'static str,
        /// Decoder or integrity failure
        reason: String,
    },
    /// SQLite failed or a notice could not be serialized
    #[error("supervisor notice storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

impl From<StoredReadError> for SupervisorNoticeStoreError {
    fn from(error: StoredReadError) -> Self {
        match error {
            StoredReadError::Storage(error) => Self::Storage(error),
            StoredReadError::Corrupt { what, reason } => Self::CorruptRecord { what, reason },
        }
    }
}

/// Insert a notice without committing the caller's loan transaction
pub(crate) fn insert_supervisor_notice_in_transaction(
    tx: &Transaction<'_>,
    notice: &SupervisorNotice,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    let existing = {
        let mut statement = tx.prepare(
            "SELECT id, loan_id, action_id, notice_json
             FROM resource_supervisor_notices
             WHERE id = ?1 OR action_id = ?2",
        )?;
        let rows = statement.query_map(
            params![
                notice.id.as_uuid().to_string(),
                notice.action_id.as_uuid().to_string()
            ],
            |row| Ok(decode_supervisor_notice_record(row)),
        )?;
        collect_decoded::<_, StoredReadError>(rows)?
    };

    if let [(saved, _)] = existing.as_slice()
        && same_supervisor_notice_content(saved, notice)
    {
        return Ok(saved.clone());
    }
    if !existing.is_empty() {
        return Err(SupervisorNoticeStoreError::Conflict);
    }

    if notice.delivery != (SupervisorNoticeDelivery::Pending { attempts: 0 }) {
        return Err(SupervisorNoticeStoreError::InvalidInitialDelivery);
    }

    tx.execute(
        "INSERT INTO resource_supervisor_notices (id, loan_id, action_id, notice_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            notice.id.as_uuid().to_string(),
            notice.loan_id.as_uuid().to_string(),
            notice.action_id.as_uuid().to_string(),
            encode_supervisor_notice(notice)?,
        ],
    )?;
    Ok(notice.clone())
}

/// Read one notice by its stable deduplication identity
pub(crate) fn supervisor_notice(
    conn: &Connection,
    notice_id: NoticeId,
) -> Result<Option<SupervisorNotice>, SupervisorNoticeStoreError> {
    Ok(select_supervisor_notice_record(conn, notice_id)?.map(|(notice, _)| notice))
}

/// Read notices that can receive another delivery attempt in stable ID order
///
/// A notice whose loan has moved past its action keeps its delivery state but
/// is not listed, so a late copy never reaches the supervisor
pub(crate) fn pending_supervisor_notices(
    conn: &Connection,
) -> Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError> {
    let mut statement = conn.prepare(
        "SELECT notice.id, notice.loan_id, notice.action_id, notice.notice_json, loan.state_json
         FROM resource_supervisor_notices AS notice
         JOIN loans AS loan ON loan.id = notice.loan_id
         WHERE json_extract(notice.notice_json, '$.delivery.type') IN ('pending', 'retry_pending')
         ORDER BY notice.id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(
            decode_supervisor_notice_record(row).and_then(|(notice, _)| {
                let loan_state = decode_loan_state(row, 4)?;
                Ok((notice, loan_state))
            }),
        )
    })?;
    let records = collect_decoded::<_, StoredReadError>(rows)?;
    Ok(records
        .into_iter()
        .filter(|(notice, loan_state)| loan_state.awaits_notice(notice))
        .map(|(notice, _)| notice)
        .collect())
}

/// Reserve one bounded attempt, or return an existing reservation for an identical retry
pub(crate) fn reserve_supervisor_notice_attempt(
    conn: &mut Connection,
    notice_id: NoticeId,
    attempt_id: DeliveryAttemptId,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (mut notice, old_json) = select_supervisor_notice_record(&tx, notice_id)?
        .ok_or(SupervisorNoticeStoreError::NotFound)?;
    // the loan can close between the pending scan and this reservation
    if !select_loan_state(&tx, notice.loan_id)?.awaits_notice(&notice) {
        return Err(SupervisorNoticeStoreError::ActionNoLongerAwaited);
    }

    let attempts = match notice.delivery {
        SupervisorNoticeDelivery::Pending { attempts }
        | SupervisorNoticeDelivery::RetryPending { attempts, .. } => attempts,
        SupervisorNoticeDelivery::Sending {
            attempt_id: reserved,
            ..
        } if reserved == attempt_id => {
            tx.commit()?;
            return Ok(notice);
        }
        SupervisorNoticeDelivery::Sending { .. } => {
            return Err(SupervisorNoticeStoreError::AttemptInFlight);
        }
        SupervisorNoticeDelivery::Delivered { .. } => {
            return Err(SupervisorNoticeStoreError::AlreadyDelivered);
        }
        SupervisorNoticeDelivery::Failed { .. } => {
            return Err(SupervisorNoticeStoreError::AttemptBudgetExhausted);
        }
    };

    if attempts >= SUPERVISOR_NOTICE_MAX_ATTEMPTS {
        return Err(SupervisorNoticeStoreError::AttemptBudgetExhausted);
    }

    let attempt = attempts + 1;
    notice.delivery = SupervisorNoticeDelivery::Sending {
        attempt_id,
        attempt,
    };
    update_supervisor_notice_cas(&tx, &notice, &old_json)?;
    tx.commit()?;
    Ok(notice)
}

/// Settle only the exact attempt that is still in flight
pub(crate) fn settle_supervisor_notice_attempt(
    conn: &mut Connection,
    notice_id: NoticeId,
    attempt_id: DeliveryAttemptId,
    result: Result<(), String>,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (mut notice, old_json) = select_supervisor_notice_record(&tx, notice_id)?
        .ok_or(SupervisorNoticeStoreError::NotFound)?;
    let attempt = match notice.delivery {
        SupervisorNoticeDelivery::Sending {
            attempt_id: active_attempt,
            attempt,
        } if active_attempt == attempt_id => attempt,
        _ => return Err(SupervisorNoticeStoreError::StaleAttempt),
    };

    notice.delivery = match result {
        Ok(()) => SupervisorNoticeDelivery::Delivered { attempts: attempt },
        Err(last_error) if attempt >= SUPERVISOR_NOTICE_MAX_ATTEMPTS => {
            SupervisorNoticeDelivery::Failed {
                attempts: attempt,
                last_error,
            }
        }
        Err(last_error) => SupervisorNoticeDelivery::RetryPending {
            attempts: attempt,
            last_error,
        },
    };

    update_supervisor_notice_cas(&tx, &notice, &old_json)?;
    tx.commit()?;
    Ok(notice)
}

/// Recover in-flight notices after startup without treating them as delivered
pub(crate) fn recover_sending_supervisor_notices(
    conn: &mut Connection,
) -> Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let sending = {
        let mut statement = tx.prepare(
            "SELECT id, loan_id, action_id, notice_json
             FROM resource_supervisor_notices
             WHERE json_extract(notice_json, '$.delivery.type') = 'sending'
             ORDER BY id ASC",
        )?;
        let rows = statement.query_map([], |row| Ok(decode_supervisor_notice_record(row)))?;
        collect_decoded::<_, StoredReadError>(rows)?
    };

    let mut recovered = Vec::with_capacity(sending.len());
    for (mut notice, old_json) in sending {
        let SupervisorNoticeDelivery::Sending { attempt, .. } = notice.delivery else {
            unreachable!("query selected only sending notices")
        };
        notice.delivery = if attempt >= SUPERVISOR_NOTICE_MAX_ATTEMPTS {
            SupervisorNoticeDelivery::Failed {
                attempts: attempt,
                last_error: INTERRUPTED_DELIVERY_ERROR.into(),
            }
        } else {
            SupervisorNoticeDelivery::RetryPending {
                attempts: attempt,
                last_error: INTERRUPTED_DELIVERY_ERROR.into(),
            }
        };
        update_supervisor_notice_cas(&tx, &notice, &old_json)?;
        recovered.push(notice);
    }

    tx.commit()?;
    Ok(recovered)
}

/// Retarget an undelivered notice inside the caller's transaction
pub(crate) fn retarget_supervisor_notice_in_transaction(
    tx: &Transaction<'_>,
    notice_id: NoticeId,
    expected_assignment_revision: AssignmentRevision,
    destination: SupervisorAddress,
    new_assignment_revision: AssignmentRevision,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    if new_assignment_revision.get() <= expected_assignment_revision.get() {
        return Err(SupervisorNoticeStoreError::AssignmentRevisionMustIncrease);
    }

    let (mut notice, old_json) = select_supervisor_notice_record(tx, notice_id)?
        .ok_or(SupervisorNoticeStoreError::NotFound)?;
    if notice.assignment_revision != expected_assignment_revision {
        return Err(SupervisorNoticeStoreError::StaleAssignmentRevision);
    }
    if matches!(notice.delivery, SupervisorNoticeDelivery::Delivered { .. }) {
        return Err(SupervisorNoticeStoreError::AlreadyDelivered);
    }

    if let SupervisorNoticeDelivery::Sending { attempt, .. } = notice.delivery {
        notice.delivery = if attempt >= SUPERVISOR_NOTICE_MAX_ATTEMPTS {
            SupervisorNoticeDelivery::Failed {
                attempts: attempt,
                last_error: RETARGETED_DELIVERY_ERROR.into(),
            }
        } else {
            SupervisorNoticeDelivery::RetryPending {
                attempts: attempt,
                last_error: RETARGETED_DELIVERY_ERROR.into(),
            }
        };
    }
    notice.destination = destination;
    notice.assignment_revision = new_assignment_revision;

    update_supervisor_notice_cas(tx, &notice, &old_json)?;
    Ok(notice)
}

fn same_supervisor_notice_content(left: &SupervisorNotice, right: &SupervisorNotice) -> bool {
    left.id == right.id
        && left.loan_id == right.loan_id
        && left.action_id == right.action_id
        && left.state_revision == right.state_revision
        && left.destination == right.destination
        && left.assignment_revision == right.assignment_revision
        && left.payload == right.payload
}

pub(crate) fn select_supervisor_notice_record(
    conn: &Connection,
    notice_id: NoticeId,
) -> Result<Option<(SupervisorNotice, String)>, StoredReadError> {
    conn.query_row(
        "SELECT id, loan_id, action_id, notice_json
         FROM resource_supervisor_notices WHERE id = ?1",
        [notice_id.as_uuid().to_string()],
        |row| Ok(decode_supervisor_notice_record(row)),
    )
    .optional()?
    .transpose()
}

pub(crate) fn select_supervisor_notice_record_by_action(
    conn: &Connection,
    action_id: ActionId,
) -> Result<Option<(SupervisorNotice, String)>, StoredReadError> {
    conn.query_row(
        "SELECT id, loan_id, action_id, notice_json
         FROM resource_supervisor_notices WHERE action_id = ?1",
        [action_id.as_uuid().to_string()],
        |row| Ok(decode_supervisor_notice_record(row)),
    )
    .optional()?
    .transpose()
}

fn select_loan_state(conn: &Connection, loan_id: LoanId) -> Result<LoanState, StoredReadError> {
    conn.query_row(
        "SELECT state_json FROM loans WHERE id = ?1",
        [loan_id.as_uuid().to_string()],
        |row| Ok(decode_loan_state(row, 0)),
    )?
}

fn decode_loan_state(row: &Row<'_>, index: usize) -> Result<LoanState, StoredReadError> {
    stored_json(
        "loan state",
        &stored_column::<String>(row, index, "loan state")?,
    )
}

/// Decode `id, loan_id, action_id, notice_json` columns and check that they agree
pub(crate) fn decode_supervisor_notice_record(
    row: &Row<'_>,
) -> Result<(SupervisorNotice, String), StoredReadError> {
    let text = |index, what| stored_column::<String>(row, index, what);
    let id: NoticeId = stored_id(
        "supervisor notice identity",
        &text(0, "supervisor notice identity")?,
    )?;
    let loan_id: LoanId = stored_id(
        "supervisor notice loan identity",
        &text(1, "supervisor notice loan identity")?,
    )?;
    let action_id: ActionId = stored_id(
        "supervisor notice action identity",
        &text(2, "supervisor notice action identity")?,
    )?;
    let notice_json = text(3, "supervisor notice")?;
    let notice: SupervisorNotice = stored_json("supervisor notice", &notice_json)?;
    if notice.id != id || notice.loan_id != loan_id || notice.action_id != action_id {
        return Err(StoredReadError::corrupt(
            "supervisor notice",
            "identity columns do not match the typed notice",
        ));
    }

    Ok((notice, notice_json))
}

pub(crate) fn update_supervisor_notice_cas(
    tx: &Transaction<'_>,
    notice: &SupervisorNotice,
    expected_json: &str,
) -> Result<(), SupervisorNoticeStoreError> {
    let updated = tx.execute(
        "UPDATE resource_supervisor_notices SET notice_json = ?1
         WHERE id = ?2 AND action_id = ?3 AND notice_json = ?4",
        params![
            encode_supervisor_notice(notice)?,
            notice.id.as_uuid().to_string(),
            notice.action_id.as_uuid().to_string(),
            expected_json,
        ],
    )?;
    if updated != 1 {
        return Err(SupervisorNoticeStoreError::StaleAttempt);
    }

    Ok(())
}

fn encode_supervisor_notice(
    notice: &SupervisorNotice,
) -> Result<String, SupervisorNoticeStoreError> {
    serde_json::to_string(notice).map_err(|err| {
        SupervisorNoticeStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
    })
}
