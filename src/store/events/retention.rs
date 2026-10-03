//! Compaction of settled event payloads into digest receipts

use chrono::{Duration as ChronoDuration, Utc};
use rusqlite::{TransactionBehavior, params};

use super::{decode, encode, event_digest, is_terminal_callback_event, storage, stored_event};
use crate::error::AppError;
use crate::events::{DeliveryState, EventError, OutboxState};
use crate::store::{Store, fmt_time};

const EVENT_RETENTION_DAYS: i64 = 30;
pub(super) const EVENT_RETENTION_BATCH_SIZE: i64 = 64;

/// Result of one bounded event-retention cleanup transaction
pub(crate) struct EventRetentionBatch {
    /// Number of payload rows compacted
    pub(crate) compacted: usize,
    /// Whether either table filled its batch and may have more eligible rows
    pub(crate) has_more: bool,
}

impl Store {
    /// Compact settled executor and origin event payloads older than 30 days
    ///
    /// Each side processes at most 64 rows in one immediate transaction. The
    /// corresponding receipt is inserted before its payload row is deleted
    pub(crate) fn compact_old_event_payloads(&mut self) -> Result<EventRetentionBatch, EventError> {
        let cutoff = fmt_time(Utc::now() - ChronoDuration::days(EVENT_RETENTION_DAYS));
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;

        let outbox_rows = {
            let mut statement = tx
                .prepare(
                    "SELECT task_id,seq,event_json FROM executor_outbox
                     WHERE state='acknowledged' AND acknowledged_at <= ?1
                       AND NOT EXISTS (
                           SELECT 1 FROM executor_event_routes r
                           WHERE r.task_id=executor_outbox.task_id
                       )
                     ORDER BY acknowledged_at,task_id,seq LIMIT ?2",
                )
                .map_err(storage)?;
            statement
                .query_map(params![cutoff, EVENT_RETENTION_BATCH_SIZE], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage)?
        };
        for (task, seq, event_json) in &outbox_rows {
            let event = stored_event(task, *seq, event_json)?;
            tx.execute(
                "INSERT INTO executor_event_receipts
                 (task_id,seq,event_digest,result_json,terminal_callback)
                 VALUES (?1,?2,?3,?4,?5)",
                params![
                    task,
                    seq,
                    event_digest(&event)?,
                    encode(&OutboxState::Acknowledged)?,
                    is_terminal_callback_event(&event),
                ],
            )
            .map_err(storage)?;
            let deleted = tx
                .execute(
                    "DELETE FROM executor_outbox
                     WHERE task_id=?1 AND seq=?2 AND state='acknowledged'
                       AND NOT EXISTS (
                           SELECT 1 FROM executor_event_routes r
                           WHERE r.task_id=executor_outbox.task_id
                       )",
                    params![task, seq],
                )
                .map_err(storage)?;
            if deleted != 1 {
                return Err(EventError::Storage(AppError::Internal {
                    message: "acknowledged outbox event changed during compaction".into(),
                }));
            }
        }

        let inbox_rows = {
            let mut statement = tx
                .prepare(
                    "SELECT i.task_id,i.seq,i.event_json,i.delivery_json
                     FROM origin_inbox i
                     JOIN origin_routes r ON r.task_id=i.task_id
                     WHERE i.settled_at <= ?1
                       AND json_extract(i.delivery_json,'$.type') IN ('not_required','delivered')
                       AND i.seq <= json_extract(r.route_json,'$.last_settled_seq')
                     ORDER BY i.settled_at,i.task_id,i.seq LIMIT ?2",
                )
                .map_err(storage)?;
            statement
                .query_map(params![cutoff, EVENT_RETENTION_BATCH_SIZE], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage)?
        };
        for (task, seq, event_json, delivery_json) in &inbox_rows {
            let event = stored_event(task, *seq, event_json)?;
            let delivery: DeliveryState = decode(delivery_json)?;
            if !matches!(
                &delivery,
                DeliveryState::NotRequired | DeliveryState::Delivered { .. }
            ) {
                return Err(EventError::Storage(AppError::Internal {
                    message: "inbox retention query selected an unsettled result".into(),
                }));
            }
            tx.execute(
                "INSERT INTO origin_event_receipts
                 (task_id,seq,event_digest,delivery_json,terminal_callback)
                 VALUES (?1,?2,?3,?4,?5)",
                params![
                    task,
                    seq,
                    event_digest(&event)?,
                    encode(&delivery)?,
                    is_terminal_callback_event(&event),
                ],
            )
            .map_err(storage)?;
            let deleted = tx
                .execute(
                    "DELETE FROM origin_inbox
                     WHERE task_id=?1 AND seq=?2
                       AND json_extract(delivery_json,'$.type') IN ('not_required','delivered')",
                    params![task, seq],
                )
                .map_err(storage)?;
            if deleted != 1 {
                return Err(EventError::Storage(AppError::Internal {
                    message: "settled inbox event changed during compaction".into(),
                }));
            }
        }

        tx.commit().map_err(storage)?;
        Ok(EventRetentionBatch {
            compacted: outbox_rows.len() + inbox_rows.len(),
            has_more: outbox_rows.len() == EVENT_RETENTION_BATCH_SIZE as usize
                || inbox_rows.len() == EVENT_RETENTION_BATCH_SIZE as usize,
        })
    }
}
