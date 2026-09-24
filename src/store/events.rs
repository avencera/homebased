//! Executor outbox and origin inbox persistence

use std::num::NonZeroU64;

use chrono::{Duration as ChronoDuration, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

use super::Store;
use crate::callback::EventKind;
use crate::domain::{API_VERSION, SUMMARY_MAX_BYTES, TaskId};
use crate::error::AppError;
use crate::events::{
    DeliveryOutcome, DeliveryState, EventAcceptance, EventError, EventPayload, EventRouteState,
    EventRouteStatus, FailedInboxEvent, InboxEvent, OutboxEvent, OutboxState, TaskEvent,
};
use crate::machine::MachineId;
use crate::submission::{
    ExecutorIdentity, OriginRoute, ResourceActionRoutePhase, ResourceBackgroundRoutePhase,
    ResourceRoutePhase, SubmissionState,
};

const EVENT_RETENTION_DAYS: i64 = 30;
const EVENT_RETENTION_BATCH_SIZE: i64 = 64;

/// Result of one bounded event-retention cleanup transaction.
pub(crate) struct EventRetentionBatch {
    /// Number of payload rows compacted.
    pub(crate) compacted: usize,
    /// Whether either table filled its batch and may have more eligible rows.
    pub(crate) has_more: bool,
}

fn storage(error: rusqlite::Error) -> EventError {
    EventError::Storage(error.into())
}

fn encode<T: serde::Serialize>(value: &T) -> Result<String, EventError> {
    Ok(serde_json::to_string(value).map_err(AppError::from)?)
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, EventError> {
    Ok(serde_json::from_str(value).map_err(AppError::from)?)
}

fn decode_route(value: &str) -> Result<OriginRoute, EventError> {
    let route: OriginRoute = decode(value)?;
    route.validate().map_err(|error| {
        EventError::Storage(AppError::Internal {
            message: format!("invalid saved resource origin route: {error}"),
        })
    })?;
    Ok(route)
}

fn sql_seq(seq: u64) -> Result<i64, EventError> {
    i64::try_from(seq).map_err(|_| EventError::Invalid {
        message: "event sequence exceeds SQLite range".into(),
    })
}

fn event_digest(event: &TaskEvent) -> Result<String, EventError> {
    // sort every JSON object so struct field order is not part of receipt identity
    let value = canonical_json(serde_json::to_value(event).map_err(AppError::from)?);
    let canonical = serde_json::to_vec(&value).map_err(AppError::from)?;
    let digest = Sha256::digest(canonical);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        encoded.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
    }
    Ok(encoded)
}

fn canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(fields) => {
            let mut entries: Vec<_> = fields.into_iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = serde_json::Map::new();
            for (key, value) in entries {
                sorted.insert(key, canonical_json(value));
            }
            serde_json::Value::Object(sorted)
        }
        value => value,
    }
}

fn stored_event(task_id: &str, seq: i64, event_json: &str) -> Result<TaskEvent, EventError> {
    let task: TaskId = task_id.parse().map_err(|_| EventError::Invalid {
        message: "invalid retained event task UUID".into(),
    })?;
    let event: TaskEvent = decode(event_json)?;
    validate(&event)?;
    if event.task != task || sql_seq(event.seq.get())? != seq {
        return Err(EventError::Storage(AppError::Internal {
            message: "retained event identity disagrees with its storage key".into(),
        }));
    }
    Ok(event)
}

fn validate(event: &TaskEvent) -> Result<(), EventError> {
    if let EventPayload::Callback {
        event: callback, ..
    } = &event.payload
        && (callback.task != event.task || callback.api_version != API_VERSION)
    {
        return Err(EventError::Invalid {
            message: "callback task or API version does not match event envelope".into(),
        });
    }
    let reports: &[crate::callback::ReportView] = match &event.payload {
        EventPayload::Callback { event, .. } => &event.reports,
        EventPayload::Report { report } => std::slice::from_ref(report),
        EventPayload::State { .. } => &[],
    };
    if reports
        .iter()
        .any(|report| report.seq <= 0 || report.summary.len() > SUMMARY_MAX_BYTES)
    {
        return Err(EventError::Invalid {
            message: "event report sequence or summary is invalid".into(),
        });
    }
    sql_seq(event.seq.get())?;
    Ok(())
}

impl Store {
    /// Compact settled executor and origin event payloads older than 30 days
    ///
    /// Each side processes at most 64 rows in one immediate transaction. The
    /// corresponding receipt is inserted before its payload row is deleted.
    pub(crate) fn compact_old_event_payloads(&mut self) -> Result<EventRetentionBatch, EventError> {
        let cutoff = super::fmt_time(Utc::now() - ChronoDuration::days(EVENT_RETENTION_DAYS));
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
                    super::is_terminal_callback_event(&event),
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
                    super::is_terminal_callback_event(&event),
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

    /// Find tasks with pending events whose route is not orphaned
    pub fn pending_outbound_tasks(&self) -> Result<Vec<TaskId>, EventError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT o.task_id FROM executor_outbox o
             LEFT JOIN executor_event_routes r ON r.task_id=o.task_id
             WHERE o.state='pending' AND r.task_id IS NULL ORDER BY o.task_id",
            )
            .map_err(storage)?;
        stmt.query_map([], |row| row.get::<_, String>(0))
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?
            .into_iter()
            .map(|id| {
                id.parse().map_err(|_| EventError::Invalid {
                    message: "invalid outbox task UUID".into(),
                })
            })
            .collect()
    }

    /// Read the fixed destination and retained event counts for inspection
    pub fn outbound_route_status(
        &self,
        task: TaskId,
    ) -> Result<Option<EventRouteStatus>, EventError> {
        let identity: Option<String> = self
            .conn
            .query_row(
                "SELECT identity_json FROM executor_identities WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        let Some(identity) = identity else {
            return Ok(None);
        };
        let ExecutorIdentity::Accepted(record) = decode(&identity)? else {
            return Ok(None);
        };
        let (pending, acknowledged): (i64, i64) = self
            .conn
            .query_row(
                "SELECT COUNT(*) FILTER (WHERE state='pending'),
                    COUNT(*) FILTER (WHERE state='acknowledged') +
                        (SELECT COUNT(*) FROM executor_event_receipts WHERE task_id=?1)
             FROM executor_outbox WHERE task_id=?1",
                [task.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(storage)?;
        let reason: Option<String> = self
            .conn
            .query_row(
                "SELECT reason FROM executor_event_routes WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        let state = if reason.is_some() {
            EventRouteState::Orphaned
        } else if pending > 0 {
            EventRouteState::Pending
        } else {
            EventRouteState::Acknowledged
        };
        Ok(Some(EventRouteStatus {
            api_version: API_VERSION,
            task,
            origin_machine: record.origin_machine,
            state,
            pending: pending as u64,
            acknowledged: acknowledged as u64,
            reason,
        }))
    }

    /// Stop automatic sends after the verified origin reports a missing route
    pub fn orphan_outbound_route(&self, task: TaskId) -> Result<(), EventError> {
        if self.outbound_route_status(task)?.is_none() {
            return Err(EventError::OwnerConflict { task });
        }
        self.conn
            .execute(
                "INSERT OR IGNORE INTO executor_event_routes (task_id,state,reason)
             SELECT task_id,'orphaned','route_not_found' FROM executor_identities WHERE task_id=?1",
                [task.to_string()],
            )
            .map_err(storage)?;
        Ok(())
    }

    /// Read the earliest pending row without loading the task's full history
    pub fn first_pending_outbound(&self, task: TaskId) -> Result<Option<OutboxEvent>, EventError> {
        let seq: Option<i64> = self.conn.query_row(
            "SELECT seq FROM executor_outbox WHERE task_id=?1 AND state='pending' ORDER BY seq LIMIT 1",
            [task.to_string()], |row| row.get(0),
        ).optional().map_err(storage)?;
        let Some(seq) = seq else { return Ok(None) };
        let seq = NonZeroU64::new(seq as u64).ok_or_else(|| EventError::Invalid {
            message: "invalid pending event sequence".into(),
        })?;
        self.outbound_event_at_or_after(task, seq)
    }

    /// Read one retained row at or after a requested sequence, including prior acknowledgements
    pub fn outbound_event_at_or_after(
        &self,
        task: TaskId,
        seq: NonZeroU64,
    ) -> Result<Option<OutboxEvent>, EventError> {
        let row: Option<(String, bool, String)> = self
            .conn
            .query_row(
                "SELECT event_json,notification_required,state FROM executor_outbox
             WHERE task_id=?1 AND seq>=?2 ORDER BY seq LIMIT 1",
                params![task.to_string(), sql_seq(seq.get())?],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(storage)?;
        row.map(|(json, notification_required, state)| {
            let event: TaskEvent = decode(&json)?;
            validate(&event)?;
            if event.payload.notification_required() != notification_required {
                return Err(EventError::Storage(AppError::Internal {
                    message: "outbox notification flag disagrees with payload".into(),
                }));
            }
            let state = match state.as_str() {
                "pending" => OutboxState::Pending,
                "acknowledged" => OutboxState::Acknowledged,
                _ => {
                    return Err(EventError::Invalid {
                        message: "invalid outbox state".into(),
                    });
                }
            };
            Ok(OutboxEvent {
                event,
                state,
                notification_required,
            })
        })
        .transpose()
    }
    /// Task IDs with an unsettled callback, including those left by a prior daemon
    pub fn pending_inbox_tasks(&self) -> Result<Vec<TaskId>, EventError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT task_id FROM origin_inbox
             WHERE json_extract(delivery_json, '$.type') = 'pending_delivery' ORDER BY task_id",
            )
            .map_err(storage)?;
        stmt.query_map([], |row| row.get::<_, String>(0))
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?
            .into_iter()
            .map(|id| {
                id.parse().map_err(|_| EventError::Invalid {
                    message: "invalid inbox task UUID".into(),
                })
            })
            .collect()
    }

    /// Select only the first unsettled event after the route's contiguous cursor
    pub fn earliest_unsettled_inbox(
        &mut self,
        task: TaskId,
    ) -> Result<Option<InboxEvent>, EventError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(storage)?;
        let route_json: String = tx
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?
            .ok_or(EventError::RouteNotFound { task })?;
        let route = decode_route(&route_json)?;
        let row: Option<(String, bool, String)> = tx
            .query_row(
                "SELECT event_json,notification_required,delivery_json FROM origin_inbox
             WHERE task_id=?1 AND seq>?2 ORDER BY seq LIMIT 1",
                params![task.to_string(), sql_seq(route.last_settled_seq)?],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(storage)?;
        tx.commit().map_err(storage)?;
        row.map(|(event, notification_required, delivery)| {
            Ok(InboxEvent {
                event: decode(&event)?,
                notification_required,
                delivery: decode(&delivery)?,
            })
        })
        .transpose()
    }

    /// Reserve one real command attempt before invoking the queue executable
    pub fn reserve_inbox_attempt(
        &mut self,
        task: TaskId,
        seq: NonZeroU64,
    ) -> Result<Option<InboxEvent>, EventError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let route_json: String = tx
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?
            .ok_or(EventError::RouteNotFound { task })?;
        let route = decode_route(&route_json)?;
        let row: Option<(i64, String, bool, String)> = tx
            .query_row(
                "SELECT seq,event_json,notification_required,delivery_json FROM origin_inbox
             WHERE task_id=?1 AND seq>?2 ORDER BY seq LIMIT 1",
                params![task.to_string(), sql_seq(route.last_settled_seq)?],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(storage)?;
        let Some((first_seq, event_json, notification_required, delivery_json)) = row else {
            return Ok(None);
        };
        if first_seq != sql_seq(seq.get())? {
            return Ok(None);
        }
        let DeliveryState::PendingDelivery {
            attempts,
            last_error,
        } = decode(&delivery_json)?
        else {
            return Ok(None);
        };
        if attempts >= 3 {
            return Ok(None);
        }
        let delivery = DeliveryState::PendingDelivery {
            attempts: attempts + 1,
            last_error,
        };
        tx.execute(
            "UPDATE origin_inbox SET delivery_json=?1 WHERE task_id=?2 AND seq=?3",
            params![encode(&delivery)?, task.to_string(), first_seq],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(Some(InboxEvent {
            event: decode(&event_json)?,
            notification_required,
            delivery,
        }))
    }

    /// Settle one earliest pending event and advance only a contiguous settled prefix
    pub fn settle_inbox_attempt(
        &mut self,
        task: TaskId,
        seq: NonZeroU64,
        outcome: DeliveryOutcome,
    ) -> Result<InboxEvent, EventError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let route_json: String = tx
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?
            .ok_or(EventError::RouteNotFound { task })?;
        let mut route = decode_route(&route_json)?;
        let row: (String, bool, String) = tx.query_row(
            "SELECT event_json,notification_required,delivery_json FROM origin_inbox WHERE task_id=?1 AND seq=?2",
            params![task.to_string(), sql_seq(seq.get())?],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).map_err(storage)?;
        let DeliveryState::PendingDelivery {
            attempts,
            last_error,
        } = decode(&row.2)?
        else {
            return Err(EventError::Invalid {
                message: "inbox event is not pending".into(),
            });
        };
        if seq.get() != route.last_settled_seq + 1 {
            return Err(EventError::Invalid {
                message: "inbox settlement is out of order".into(),
            });
        }
        let delivery = match outcome {
            DeliveryOutcome::Delivered if attempts > 0 => DeliveryState::Delivered {
                attempts,
                last_error,
            },
            DeliveryOutcome::Retryable(error) if attempts >= 3 => DeliveryState::DeliveryFailed {
                attempts,
                last_error: error,
            },
            DeliveryOutcome::Retryable(error) => DeliveryState::PendingDelivery {
                attempts,
                last_error: Some(error),
            },
            DeliveryOutcome::Permanent(error) => DeliveryState::DeliveryFailed {
                attempts,
                last_error: error,
            },
            DeliveryOutcome::Delivered => {
                return Err(EventError::Invalid {
                    message: "success without a reserved attempt".into(),
                });
            }
        };
        let settled = !matches!(&delivery, DeliveryState::PendingDelivery { .. });
        tx.execute(
            "UPDATE origin_inbox
             SET delivery_json=?1,
                 settled_at=CASE WHEN ?2 THEN COALESCE(settled_at,?3) ELSE NULL END
             WHERE task_id=?4 AND seq=?5",
            params![
                encode(&delivery)?,
                settled,
                super::fmt_time(Utc::now()),
                task.to_string(),
                sql_seq(seq.get())?,
            ],
        )
        .map_err(storage)?;
        if matches!(delivery, DeliveryState::Delivered { .. }) {
            let event: TaskEvent = decode(&row.0)?;
            if let EventPayload::Callback {
                event: callback, ..
            } = event.payload
                && callback.event == EventKind::TaskReported
                && let Some(report) = callback.reports.first()
            {
                tx.execute(
                    "UPDATE reports SET notified_at=?1 WHERE task_id=?2 AND seq=?3",
                    params![
                        chrono::Utc::now().to_rfc3339(),
                        task.to_string(),
                        report.seq
                    ],
                )
                .map_err(storage)?;
            }
        }
        if !matches!(delivery, DeliveryState::PendingDelivery { .. }) {
            let mut cursor = route.last_settled_seq;
            while let Some(next_json) = tx
                .query_row(
                    "SELECT delivery_json FROM origin_inbox WHERE task_id=?1 AND seq=?2",
                    params![task.to_string(), sql_seq(cursor + 1)?],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(storage)?
            {
                if matches!(
                    decode::<DeliveryState>(&next_json)?,
                    DeliveryState::PendingDelivery { .. }
                ) {
                    break;
                }
                cursor += 1;
            }
            route.last_settled_seq = cursor;
            tx.execute(
                "UPDATE origin_routes SET route_json=?1 WHERE task_id=?2",
                params![encode(&route)?, task.to_string()],
            )
            .map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        Ok(InboxEvent {
            event: decode(&row.0)?,
            notification_required: row.1,
            delivery,
        })
    }

    /// List durable callback failures without changing task process status
    pub fn failed_inbox_events(&self, task: TaskId) -> Result<Vec<FailedInboxEvent>, EventError> {
        Ok(self
            .inbound_events(task)?
            .into_iter()
            .filter_map(|entry| match entry.delivery {
                DeliveryState::DeliveryFailed {
                    attempts,
                    last_error,
                } => Some(FailedInboxEvent {
                    seq: entry.event.seq.get(),
                    attempts,
                    error: last_error,
                }),
                _ => None,
            })
            .collect())
    }

    /// Append one immutable executor event after its retained owner identity
    ///
    /// This does not update task detail or retained process state. Producers
    /// must use a combined transaction before relying on crash-safe state sync
    pub fn append_outbound_event(
        &mut self,
        task: TaskId,
        origin_machine: MachineId,
        execution_machine: MachineId,
        payload: EventPayload,
    ) -> Result<OutboxEvent, EventError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let row = append_outbound_event_on(&tx, task, origin_machine, execution_machine, payload)?;
        tx.commit().map_err(storage)?;
        Ok(row)
    }

    /// Append from an enclosing producer transaction without a second commit
    pub(super) fn append_produced_event(
        &self,
        task: TaskId,
        payload: EventPayload,
    ) -> Result<(), AppError> {
        append_produced_event_on(&self.conn, task, payload)
    }
}

pub(super) fn append_produced_event_on(
    conn: &Connection,
    task: TaskId,
    payload: EventPayload,
) -> Result<(), AppError> {
    let identity: String = conn.query_row(
        "SELECT identity_json FROM executor_identities WHERE task_id=?1",
        [task.to_string()],
        |row| row.get(0),
    )?;
    let ExecutorIdentity::Accepted(record) = serde_json::from_str(&identity)? else {
        return Err(AppError::ClusterTaskConflict { task });
    };
    append_outbound_event_on(
        conn,
        task,
        record.origin_machine,
        record.execution_machine,
        payload,
    )
    .map_err(|error| match error {
        EventError::Storage(error) => error,
        EventError::OwnerConflict { task } => AppError::ClusterTaskConflict { task },
        other => AppError::Internal {
            message: other.to_string(),
        },
    })?;
    Ok(())
}

pub(super) fn initial_queued_event_matches_on(
    conn: &Connection,
    task: TaskId,
    origin_machine: MachineId,
    execution_machine: MachineId,
) -> Result<bool, EventError> {
    let expected = TaskEvent {
        task,
        seq: NonZeroU64::MIN,
        origin_machine,
        execution_machine,
        payload: EventPayload::State {
            status: crate::domain::ProcessStatus::Queued,
        },
    };

    let outbox: Option<(String, bool)> = conn
        .query_row(
            "SELECT event_json, notification_required FROM executor_outbox
             WHERE task_id=?1 AND seq=1",
            [task.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    if let Some((event_json, notification_required)) = outbox {
        let event: TaskEvent = decode(&event_json)?;
        validate(&event)?;
        return Ok(event == expected && !notification_required);
    }

    let receipt: Option<(String, String)> = conn
        .query_row(
            "SELECT event_digest, result_json FROM executor_event_receipts
             WHERE task_id=?1 AND seq=1",
            [task.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    let Some((saved_digest, result_json)) = receipt else {
        return Ok(false);
    };
    Ok(saved_digest == event_digest(&expected)?
        && result_json == encode(&OutboxState::Acknowledged)?)
}

fn append_outbound_event_on(
    conn: &Connection,
    task: TaskId,
    origin_machine: MachineId,
    execution_machine: MachineId,
    payload: EventPayload,
) -> Result<OutboxEvent, EventError> {
    let identity: Option<String> = conn
        .query_row(
            "SELECT identity_json FROM executor_identities WHERE task_id=?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    let ExecutorIdentity::Accepted(record) = decode(
        identity
            .as_deref()
            .ok_or(EventError::OwnerConflict { task })?,
    )?
    else {
        return Err(EventError::OwnerConflict { task });
    };
    if record.origin_machine != origin_machine || record.execution_machine != execution_machine {
        return Err(EventError::OwnerConflict { task });
    }
    let last: Option<i64> = conn
        .query_row(
            "SELECT last_seq FROM executor_event_cursors WHERE task_id=?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    let next = last
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| EventError::Invalid {
            message: "event sequence exhausted".into(),
        })?;
    let event = TaskEvent {
        task,
        seq: NonZeroU64::new(next as u64).ok_or_else(|| EventError::Invalid {
            message: "event sequence must start at one".into(),
        })?,
        origin_machine,
        execution_machine,
        payload,
    };
    validate(&event)?;
    let row = OutboxEvent {
        notification_required: event.payload.notification_required(),
        event,
        state: OutboxState::Pending,
    };
    conn.execute(
            "INSERT INTO executor_outbox (task_id,seq,origin_machine,execution_machine,event_json,notification_required,state)
             VALUES (?1,?2,?3,?4,?5,?6,'pending')",
            params![
                task.to_string(),
                next,
                origin_machine.to_string(),
                execution_machine.to_string(),
                encode(&row.event)?,
                row.notification_required,
            ],
        )
        .map_err(storage)?;
    conn.execute(
        "INSERT INTO executor_event_cursors (task_id,last_seq) VALUES (?1,?2)
             ON CONFLICT(task_id) DO UPDATE SET last_seq=excluded.last_seq",
        params![task.to_string(), next],
    )
    .map_err(storage)?;
    Ok(row)
}

impl Store {
    /// Read retained, unacknowledged executor events in sequence order
    pub fn pending_outbound_events(&self, task: TaskId) -> Result<Vec<OutboxEvent>, EventError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT event_json,notification_required FROM executor_outbox
                 WHERE task_id=?1 AND state='pending' ORDER BY seq",
            )
            .map_err(storage)?;
        let rows = stmt
            .query_map([task.to_string()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        rows.into_iter()
            .map(|(json, notification_required)| {
                let event: TaskEvent = decode(&json)?;
                validate(&event)?;
                if event.payload.notification_required() != notification_required {
                    return Err(EventError::Storage(AppError::Internal {
                        message: "outbox notification flag disagrees with payload".into(),
                    }));
                }
                Ok(OutboxEvent {
                    event,
                    state: OutboxState::Pending,
                    notification_required,
                })
            })
            .collect()
    }

    /// Mark one retained executor event acknowledged after an origin response
    pub fn mark_outbound_acknowledged(
        &self,
        task: TaskId,
        seq: NonZeroU64,
    ) -> Result<(), EventError> {
        let changed = self
            .conn
            .execute(
                "UPDATE executor_outbox
                 SET state='acknowledged', acknowledged_at=COALESCE(acknowledged_at,?1)
                 WHERE task_id=?2 AND seq=?3",
                params![
                    super::fmt_time(Utc::now()),
                    task.to_string(),
                    sql_seq(seq.get())?
                ],
            )
            .map_err(storage)?;
        if changed == 0 {
            let acknowledged: bool = self
                .conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM executor_event_receipts
                         WHERE task_id=?1 AND seq=?2 AND result_json='\"acknowledged\"'
                     )",
                    params![task.to_string(), sql_seq(seq.get())?],
                    |row| row.get(0),
                )
                .map_err(storage)?;
            if !acknowledged {
                return Err(EventError::Invalid {
                    message: "outbound event not found".into(),
                });
            }
        }
        Ok(())
    }

    /// Accept only the next event for an existing route and exact execution owner
    ///
    /// Acknowledgement is returned only after the inbox, route cursor, and
    /// cached process state commit in one immediate transaction
    pub fn accept_inbound_event(
        &mut self,
        event: &TaskEvent,
    ) -> Result<EventAcceptance, EventError> {
        validate(event)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let route_json: Option<String> = tx
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [event.task.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        let mut route = decode_route(
            route_json
                .as_deref()
                .ok_or(EventError::RouteNotFound { task: event.task })?,
        )?;
        if route.origin_machine != event.origin_machine
            || route.execution_machine != event.execution_machine
        {
            return Err(EventError::OwnerConflict { task: event.task });
        }
        let next = route
            .last_accepted_seq
            .checked_add(1)
            .ok_or_else(|| EventError::Invalid {
                message: "origin event sequence exhausted".into(),
            })?;
        if event.seq.get() > next {
            return Ok(EventAcceptance::Expected { seq: next });
        }
        if event.seq.get() < next {
            let saved: Option<String> = tx
                .query_row(
                    "SELECT event_json FROM origin_inbox WHERE task_id=?1 AND seq=?2",
                    params![event.task.to_string(), sql_seq(event.seq.get())?],
                    |row| row.get(0),
                )
                .optional()
                .map_err(storage)?;
            let digest = if let Some(saved) = saved {
                event_digest(&decode::<TaskEvent>(&saved)?)?
            } else {
                let receipt: Option<String> = tx
                    .query_row(
                        "SELECT event_digest FROM origin_event_receipts WHERE task_id=?1 AND seq=?2",
                        params![event.task.to_string(), sql_seq(event.seq.get())?],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(storage)?;
                receipt.ok_or(EventError::ContentConflict {
                    task: event.task,
                    seq: event.seq.get(),
                })?
            };
            if digest != event_digest(event)? {
                return Err(EventError::ContentConflict {
                    task: event.task,
                    seq: event.seq.get(),
                });
            }
            return Ok(EventAcceptance::Acknowledged {
                seq: event.seq.get(),
            });
        }
        let activate_resource = match &route.submission {
            // the first queued event proves that the authority accepted the fixed
            // identity, even when its launch reply was lost
            SubmissionState::ResourceAction { phase, .. } => match phase {
                ResourceActionRoutePhase::AcceptanceUnknown => {
                    if event.payload.process_state() != Some(crate::domain::ProcessStatus::Queued) {
                        return Err(EventError::Invalid {
                            message: "first resource action task event must report queued state"
                                .into(),
                        });
                    }
                    true
                }
                ResourceActionRoutePhase::Accepted => false,
                ResourceActionRoutePhase::Rejected { .. } => {
                    return Err(EventError::Invalid {
                        message: "resource action route was rejected before task acceptance".into(),
                    });
                }
            },
            // the authority's durable first queued event proves acceptance even
            // after a refusal was saved, since a delayed send may have won; the
            // callback owner must keep the running trainer's results
            SubmissionState::ResourceBackground { phase, .. } => match phase {
                ResourceBackgroundRoutePhase::AcceptanceUnknown
                | ResourceBackgroundRoutePhase::Rejected { .. } => {
                    if event.payload.process_state() != Some(crate::domain::ProcessStatus::Queued) {
                        return Err(EventError::Invalid {
                            message: "first background launch event must report queued state"
                                .into(),
                        });
                    }
                    true
                }
                ResourceBackgroundRoutePhase::Accepted => false,
            },
            SubmissionState::Resource { phase, .. } => match phase {
                ResourceRoutePhase::AcceptanceUnknown | ResourceRoutePhase::Waiting => {
                    if event.payload.process_state() != Some(crate::domain::ProcessStatus::Queued) {
                        return Err(EventError::Invalid {
                            message: "first resource task event must report queued state".into(),
                        });
                    }
                    true
                }
                ResourceRoutePhase::Activated => false,
                ResourceRoutePhase::CancelledBeforeLaunch | ResourceRoutePhase::Rejected { .. } => {
                    return Err(EventError::Invalid {
                        message: "resource route was closed before task activation".into(),
                    });
                }
            },
            _ => false,
        };
        let delivery = if event.payload.notification_required() {
            DeliveryState::PendingDelivery {
                attempts: 0,
                last_error: None,
            }
        } else {
            DeliveryState::NotRequired
        };
        let accepted_at = super::fmt_time(Utc::now());
        let settled_at = (!event.payload.notification_required()).then(|| accepted_at.clone());
        tx.execute(
            "INSERT INTO origin_inbox
             (task_id,seq,origin_machine,execution_machine,event_json,notification_required,delivery_json,settled_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                event.task.to_string(),
                sql_seq(event.seq.get())?,
                event.origin_machine.to_string(),
                event.execution_machine.to_string(),
                encode(event)?,
                event.payload.notification_required(),
                encode(&delivery)?,
                settled_at,
            ],
        )
        .map_err(storage)?;
        route.last_accepted_seq = event.seq.get();
        route.last_updated_at = Some(Utc::now());
        if let Some(state) = event.payload.process_state() {
            route.last_execution_state = Some(state);
        }
        if activate_resource {
            match &mut route.submission {
                SubmissionState::Resource { phase, .. } => *phase = ResourceRoutePhase::Activated,
                SubmissionState::ResourceAction { phase, .. } => {
                    *phase = ResourceActionRoutePhase::Accepted;
                }
                SubmissionState::ResourceBackground { phase, .. } => {
                    *phase = ResourceBackgroundRoutePhase::Accepted;
                }
                SubmissionState::AcceptanceUnknown
                | SubmissionState::Accepted
                | SubmissionState::Rejected { .. } => {}
            }
        }
        route.validate().map_err(|error| {
            EventError::Storage(AppError::Internal {
                message: format!("invalid resource origin route transition: {error}"),
            })
        })?;
        if matches!(delivery, DeliveryState::NotRequired)
            && route.last_settled_seq == event.seq.get() - 1
        {
            route.last_settled_seq = event.seq.get();
        }
        tx.execute(
            "UPDATE origin_routes SET route_json=?1 WHERE task_id=?2",
            params![encode(&route)?, event.task.to_string()],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(EventAcceptance::Acknowledged {
            seq: event.seq.get(),
        })
    }

    /// Read retained inbox entries with their separate callback delivery results
    pub fn inbound_events(&self, task: TaskId) -> Result<Vec<InboxEvent>, EventError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT event_json,notification_required,delivery_json FROM origin_inbox
                 WHERE task_id=?1 ORDER BY seq",
            )
            .map_err(storage)?;
        let rows = stmt
            .query_map([task.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        rows.into_iter()
            .map(|(event, notification_required, delivery)| {
                let event: TaskEvent = decode(&event)?;
                let delivery: DeliveryState = decode(&delivery)?;
                validate(&event)?;
                if event.payload.notification_required() != notification_required
                    || matches!(delivery, DeliveryState::NotRequired) == notification_required
                {
                    return Err(EventError::Storage(AppError::Internal {
                        message: "inbox delivery state disagrees with notification flag".into(),
                    }));
                }
                Ok(InboxEvent {
                    event,
                    notification_required,
                    delivery,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;

    use super::*;
    use crate::callback::ReportView;
    use crate::domain::TaskEnv;
    use crate::submission::{
        CallbackContext, ExecutionRecord, OriginRoute, RequestId, ResourceQueueOutcome,
        ResourceQueueReceipt, ResourceRoutePhase, SubmissionState,
    };

    fn route() -> OriginRoute {
        let spec: crate::spec::NormalizedSpec = serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "event task",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["echo", "hello"] }
        }))
        .unwrap();
        OriginRoute {
            request: RequestId::new(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            execution_machine: MachineId::new(),
            thread: spec.thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: Path::new("/tmp").to_path_buf(),
                codex: Path::new("/bin/echo").to_path_buf().into(),
            },
            spec: spec.into(),
            submission: SubmissionState::AcceptanceUnknown,
            last_execution_state: None,
            last_updated_at: Some(chrono::Utc::now()),
            last_accepted_seq: 0,
            last_settled_seq: 0,
        }
    }

    fn event(route: &OriginRoute, seq: u64, status: crate::domain::ProcessStatus) -> TaskEvent {
        TaskEvent {
            task: route.task,
            seq: NonZeroU64::new(seq).unwrap(),
            origin_machine: route.origin_machine,
            execution_machine: route.execution_machine,
            payload: EventPayload::State { status },
        }
    }

    fn callback_event(
        route: &OriginRoute,
        seq: u64,
        state: Option<crate::domain::ProcessStatus>,
    ) -> TaskEvent {
        let callback = serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "event": if state.is_some() { "TASK_SUCCEEDED" } else { "TASK_REPORTED" },
            "task": route.task,
            "display_name": "event task",
            "workload": { "type": "task", "command": ["echo", "hello"] },
            "thread": route.thread,
            "cwd": "/tmp",
            "evidence": "/tmp/evidence",
            "reports": [],
            "process": null,
            "next_action": "read_report"
        }))
        .unwrap();
        TaskEvent {
            task: route.task,
            seq: NonZeroU64::new(seq).unwrap(),
            origin_machine: route.origin_machine,
            execution_machine: route.execution_machine,
            payload: EventPayload::Callback {
                event: Box::new(callback),
                state,
            },
        }
    }

    fn age_event_payloads(store: &Store) {
        store
            .conn
            .execute_batch(
                "UPDATE executor_outbox
                 SET acknowledged_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 days');
                 UPDATE origin_inbox
                 SET settled_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 days');",
            )
            .unwrap();
    }

    fn resource_route() -> OriginRoute {
        let mut route = route();
        route.submission = SubmissionState::Resource {
            resource: crate::resource::ResourceId::new(),
            phase: ResourceRoutePhase::AcceptanceUnknown,
        };
        route
    }

    #[test]
    fn first_queued_resource_event_activates_in_the_cursor_transaction() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = resource_route();
        store.insert_origin_route(&route).unwrap();

        let queued = event(&route, 1, crate::domain::ProcessStatus::Queued);
        assert_eq!(
            store.accept_inbound_event(&queued).unwrap(),
            EventAcceptance::Acknowledged { seq: 1 }
        );
        let activated = store.origin_route_by_task(route.task).unwrap().unwrap();
        assert!(matches!(
            activated.submission,
            SubmissionState::Resource {
                phase: ResourceRoutePhase::Activated,
                ..
            }
        ));
        assert_eq!(activated.last_accepted_seq, 1);
        assert_eq!(
            activated.last_execution_state,
            Some(crate::domain::ProcessStatus::Queued)
        );

        store
            .accept_inbound_event(&event(&route, 2, crate::domain::ProcessStatus::Running))
            .unwrap();
        let advanced = store.origin_route_by_task(route.task).unwrap().unwrap();
        assert!(matches!(
            advanced.submission,
            SubmissionState::Resource {
                phase: ResourceRoutePhase::Activated,
                ..
            }
        ));
        assert_eq!(advanced.last_accepted_seq, 2);
        assert_eq!(
            advanced.last_execution_state,
            Some(crate::domain::ProcessStatus::Running)
        );
    }

    #[test]
    fn resource_route_rejects_a_nonqueued_first_task_event() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = resource_route();
        store.insert_origin_route(&route).unwrap();

        assert!(matches!(
            store.accept_inbound_event(&event(&route, 1, crate::domain::ProcessStatus::Running,)),
            Err(EventError::Invalid { .. })
        ));
        let saved = store.origin_route_by_task(route.task).unwrap().unwrap();
        assert!(matches!(
            saved.submission,
            SubmissionState::Resource {
                phase: ResourceRoutePhase::AcceptanceUnknown,
                ..
            }
        ));
        assert_eq!(saved.last_accepted_seq, 0);
        assert!(store.inbound_events(route.task).unwrap().is_empty());
    }

    #[test]
    fn rejected_resource_route_does_not_accept_a_task_event() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = resource_route();
        store.insert_origin_route(&route).unwrap();
        let SubmissionState::Resource { resource, .. } = &route.submission else {
            unreachable!();
        };
        store
            .resolve_resource_route(&ResourceQueueReceipt {
                request: route.request,
                task: route.task,
                origin_machine: route.origin_machine,
                authority_machine: route.execution_machine,
                resource: *resource,
                outcome: ResourceQueueOutcome::Rejected {
                    reason: "queue rejected".into(),
                },
            })
            .unwrap();

        assert!(matches!(
            store.accept_inbound_event(&event(&route, 1, crate::domain::ProcessStatus::Queued,)),
            Err(EventError::Invalid { .. })
        ));
        assert_eq!(
            store
                .origin_route_by_task(route.task)
                .unwrap()
                .unwrap()
                .last_accepted_seq,
            0
        );
    }

    #[test]
    fn first_duplicate_conflict_gap_and_unknown_route() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = route();
        store.insert_origin_route(&route).unwrap();
        let first = event(&route, 1, crate::domain::ProcessStatus::Running);
        assert_eq!(
            store.accept_inbound_event(&first).unwrap(),
            EventAcceptance::Acknowledged { seq: 1 }
        );
        assert_eq!(
            store.accept_inbound_event(&first).unwrap(),
            EventAcceptance::Acknowledged { seq: 1 }
        );
        let different = event(&route, 1, crate::domain::ProcessStatus::Succeeded);
        assert!(matches!(
            store.accept_inbound_event(&different),
            Err(EventError::ContentConflict { seq: 1, .. })
        ));
        assert_eq!(
            store
                .accept_inbound_event(&event(&route, 3, crate::domain::ProcessStatus::Succeeded))
                .unwrap(),
            EventAcceptance::Expected { seq: 2 }
        );
        let missing = TaskEvent {
            task: TaskId::new(),
            ..first.clone()
        };
        assert!(matches!(
            store.accept_inbound_event(&missing),
            Err(EventError::RouteNotFound { .. })
        ));
        let saved = store.origin_route_by_task(route.task).unwrap().unwrap();
        assert_eq!(saved.last_accepted_seq, 1);
        assert_eq!(saved.last_settled_seq, 1);
        assert_eq!(
            saved.last_execution_state,
            Some(crate::domain::ProcessStatus::Running)
        );
        let inbox = store.inbound_events(route.task).unwrap();
        assert_eq!(inbox.len(), 1);
        assert!(!inbox[0].notification_required);
        assert_eq!(inbox[0].delivery, DeliveryState::NotRequired);
    }

    #[test]
    fn aged_local_and_remote_events_compact_and_late_duplicates_use_receipts() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let mut route = route();
        if route.execution_machine == route.origin_machine {
            route.execution_machine = MachineId::new();
        }
        let mut store = Store::open(&path).unwrap();
        store.insert_origin_route(&route).unwrap();
        store
            .accept_execution(&ExecutionRecord {
                task: route.task,
                origin_machine: route.origin_machine,
                execution_machine: route.execution_machine,
                spec: route.spec.clone(),
                state: crate::domain::ProcessStatus::Queued,
            })
            .unwrap();
        let outbox = store
            .append_outbound_event(
                route.task,
                route.origin_machine,
                route.execution_machine,
                EventPayload::State {
                    status: crate::domain::ProcessStatus::Running,
                },
            )
            .unwrap();
        store.accept_inbound_event(&outbox.event).unwrap();
        store
            .mark_outbound_acknowledged(route.task, outbox.event.seq)
            .unwrap();
        let reordered_json = format!(
            "{{\"payload\":{{\"status\":\"running\",\"type\":\"state\"}},\
             \"execution_machine\":{},\"origin_machine\":{},\"seq\":{},\"task\":{}}}",
            serde_json::to_string(&outbox.event.execution_machine).unwrap(),
            serde_json::to_string(&outbox.event.origin_machine).unwrap(),
            outbox.event.seq,
            serde_json::to_string(&outbox.event.task).unwrap(),
        );
        store
            .conn
            .execute(
                "UPDATE origin_inbox SET event_json=?1 WHERE task_id=?2 AND seq=1",
                params![reordered_json, route.task.to_string()],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE executor_outbox SET event_json=?1 WHERE task_id=?2 AND seq=1",
                params![reordered_json, route.task.to_string()],
            )
            .unwrap();
        age_event_payloads(&store);

        assert_eq!(store.compact_old_event_payloads().unwrap().compacted, 2);
        assert_eq!(store.inbound_events(route.task).unwrap().len(), 0);
        assert!(
            store
                .outbound_event_at_or_after(route.task, NonZeroU64::MIN)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .outbound_route_status(route.task)
                .unwrap()
                .unwrap()
                .acknowledged,
            1
        );
        drop(store);

        let mut reopened = Store::open(&path).unwrap();
        reopened
            .mark_outbound_acknowledged(route.task, outbox.event.seq)
            .unwrap();
        assert_eq!(
            reopened.accept_inbound_event(&outbox.event).unwrap(),
            EventAcceptance::Acknowledged { seq: 1 }
        );
        let route_after_compaction = reopened.origin_route_by_task(route.task).unwrap().unwrap();
        assert_eq!(route_after_compaction.last_accepted_seq, 1);
        assert_eq!(route_after_compaction.last_settled_seq, 1);
        let changed = event(&route, 1, crate::domain::ProcessStatus::Succeeded);
        assert!(matches!(
            reopened.accept_inbound_event(&changed),
            Err(EventError::ContentConflict { seq: 1, .. })
        ));
    }

    #[test]
    fn compaction_preserves_pending_and_failed_callback_payloads() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();

        let pending_route = route();
        store.insert_origin_route(&pending_route).unwrap();
        store
            .accept_execution(&ExecutionRecord {
                task: pending_route.task,
                origin_machine: pending_route.origin_machine,
                execution_machine: pending_route.execution_machine,
                spec: pending_route.spec.clone(),
                state: crate::domain::ProcessStatus::Queued,
            })
            .unwrap();
        let pending = callback_event(&pending_route, 1, None);
        store
            .append_outbound_event(
                pending_route.task,
                pending_route.origin_machine,
                pending_route.execution_machine,
                pending.payload.clone(),
            )
            .unwrap();
        store.accept_inbound_event(&pending).unwrap();
        store
            .mark_outbound_acknowledged(pending_route.task, pending.seq)
            .unwrap();

        let failed_route = route();
        store.insert_origin_route(&failed_route).unwrap();
        store
            .accept_execution(&ExecutionRecord {
                task: failed_route.task,
                origin_machine: failed_route.origin_machine,
                execution_machine: failed_route.execution_machine,
                spec: failed_route.spec.clone(),
                state: crate::domain::ProcessStatus::Queued,
            })
            .unwrap();
        let failed = callback_event(&failed_route, 1, None);
        store
            .append_outbound_event(
                failed_route.task,
                failed_route.origin_machine,
                failed_route.execution_machine,
                failed.payload.clone(),
            )
            .unwrap();
        store.accept_inbound_event(&failed).unwrap();
        store
            .mark_outbound_acknowledged(failed_route.task, failed.seq)
            .unwrap();
        store
            .reserve_inbox_attempt(failed_route.task, failed.seq)
            .unwrap()
            .unwrap();
        store
            .settle_inbox_attempt(
                failed_route.task,
                failed.seq,
                DeliveryOutcome::Permanent("callback unavailable".into()),
            )
            .unwrap();
        age_event_payloads(&store);

        assert_eq!(store.compact_old_event_payloads().unwrap().compacted, 2);
        let pending_inbox = store.inbound_events(pending_route.task).unwrap();
        assert_eq!(pending_inbox.len(), 1);
        assert_eq!(pending_inbox[0].event, pending);
        assert!(matches!(
            pending_inbox[0].delivery,
            DeliveryState::PendingDelivery { .. }
        ));
        let failed_inbox = store.inbound_events(failed_route.task).unwrap();
        assert_eq!(failed_inbox.len(), 1);
        assert_eq!(failed_inbox[0].event, failed);
        assert!(matches!(
            &failed_inbox[0].delivery,
            DeliveryState::DeliveryFailed { last_error, .. }
                if last_error == "callback unavailable"
        ));
    }

    #[test]
    fn compaction_rolls_back_receipts_when_payload_delete_fails() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = route();
        store.insert_origin_route(&route).unwrap();
        store
            .accept_execution(&ExecutionRecord {
                task: route.task,
                origin_machine: route.origin_machine,
                execution_machine: route.execution_machine,
                spec: route.spec.clone(),
                state: crate::domain::ProcessStatus::Queued,
            })
            .unwrap();
        let outbox = store
            .append_outbound_event(
                route.task,
                route.origin_machine,
                route.execution_machine,
                EventPayload::State {
                    status: crate::domain::ProcessStatus::Running,
                },
            )
            .unwrap();
        store.accept_inbound_event(&outbox.event).unwrap();
        store
            .mark_outbound_acknowledged(route.task, outbox.event.seq)
            .unwrap();
        age_event_payloads(&store);
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER reject_inbox_compaction BEFORE DELETE ON origin_inbox
                 BEGIN SELECT RAISE(ABORT, 'injected inbox delete failure'); END;",
            )
            .unwrap();

        assert!(store.compact_old_event_payloads().is_err());
        for table in ["executor_event_receipts", "origin_event_receipts"] {
            let count: i64 = store
                .conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0);
        }
        let outbox_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                [route.task.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        let inbox_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM origin_inbox WHERE task_id=?1",
                [route.task.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(outbox_count, 1);
        assert_eq!(inbox_count, 1);
    }

    #[test]
    fn compaction_keeps_acknowledged_events_for_orphaned_routes() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = route();
        store
            .accept_execution(&ExecutionRecord {
                task: route.task,
                origin_machine: route.origin_machine,
                execution_machine: route.execution_machine,
                spec: route.spec.clone(),
                state: crate::domain::ProcessStatus::Queued,
            })
            .unwrap();
        let outbox = store
            .append_outbound_event(
                route.task,
                route.origin_machine,
                route.execution_machine,
                EventPayload::State {
                    status: crate::domain::ProcessStatus::Running,
                },
            )
            .unwrap();
        store
            .mark_outbound_acknowledged(route.task, outbox.event.seq)
            .unwrap();
        store.orphan_outbound_route(route.task).unwrap();
        age_event_payloads(&store);

        assert_eq!(store.compact_old_event_payloads().unwrap().compacted, 0);
        assert!(
            store
                .outbound_event_at_or_after(route.task, outbox.event.seq)
                .unwrap()
                .is_some()
        );
        let receipts: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_event_receipts WHERE task_id=?1",
                [route.task.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(receipts, 0);
    }

    #[test]
    fn compaction_processes_bounded_batches_and_leaves_recent_rows() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        for index in 0..=EVENT_RETENTION_BATCH_SIZE + 1 {
            let task = TaskId::new();
            let event = TaskEvent {
                task,
                seq: NonZeroU64::MIN,
                origin_machine: MachineId::new(),
                execution_machine: MachineId::new(),
                payload: EventPayload::State {
                    status: crate::domain::ProcessStatus::Running,
                },
            };
            let age = if index > EVENT_RETENTION_BATCH_SIZE {
                "strftime('%Y-%m-%dT%H:%M:%fZ','now')"
            } else {
                "strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 days')"
            };
            store
                .conn
                .execute(
                    &format!(
                        "INSERT INTO executor_outbox
                         (task_id,seq,origin_machine,execution_machine,event_json,
                          notification_required,state,acknowledged_at)
                         VALUES (?1,1,?2,?3,?4,0,'acknowledged',{age})"
                    ),
                    params![
                        task.to_string(),
                        event.origin_machine.to_string(),
                        event.execution_machine.to_string(),
                        serde_json::to_string(&event).unwrap(),
                    ],
                )
                .unwrap();
        }
        let mut store = store;

        let first = store.compact_old_event_payloads().unwrap();
        assert_eq!(first.compacted, EVENT_RETENTION_BATCH_SIZE as usize);
        assert!(first.has_more);
        let second = store.compact_old_event_payloads().unwrap();
        assert_eq!(second.compacted, 1);
        assert!(!second.has_more);
        let retained: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM executor_outbox", [], |row| row.get(0))
            .unwrap();
        assert_eq!(retained, 1);
    }

    #[test]
    fn owner_conflict_and_receiver_identity_leave_route_unchanged() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = route();
        store.insert_origin_route(&route).unwrap();
        let mut wrong_owner = event(&route, 1, crate::domain::ProcessStatus::Running);
        wrong_owner.execution_machine = MachineId::new();
        assert!(matches!(
            store.accept_inbound_event(&wrong_owner),
            Err(EventError::OwnerConflict { .. })
        ));
        let local = crate::machine::LocalIdentity {
            machine: route.origin_machine,
            boot: crate::machine::BootId::new(),
        };
        assert!(matches!(
            local.check_destination(MachineId::new()),
            Err(AppError::MachineIdentityMismatch { .. })
        ));
        assert_eq!(
            store
                .origin_route_by_task(route.task)
                .unwrap()
                .unwrap()
                .last_accepted_seq,
            0
        );
        assert!(store.inbound_events(route.task).unwrap().is_empty());
    }

    #[test]
    fn callback_pending_does_not_settle_later_state_only_event() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = route();
        store.insert_origin_route(&route).unwrap();
        let callback = serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "event": "TASK_REPORTED",
            "task": route.task,
            "display_name": "event task",
            "workload": { "type": "task", "command": ["echo", "hello"] },
            "thread": route.thread,
            "cwd": "/tmp",
            "evidence": "/tmp/evidence",
            "reports": [],
            "process": null,
            "next_action": "read_report"
        }))
        .unwrap();
        let notified = TaskEvent {
            task: route.task,
            seq: NonZeroU64::new(1).unwrap(),
            origin_machine: route.origin_machine,
            execution_machine: route.execution_machine,
            payload: EventPayload::Callback {
                event: Box::new(callback),
                state: None,
            },
        };
        store.accept_inbound_event(&notified).unwrap();
        store
            .accept_inbound_event(&event(&route, 2, crate::domain::ProcessStatus::Running))
            .unwrap();
        let report = TaskEvent {
            task: route.task,
            seq: NonZeroU64::new(3).unwrap(),
            origin_machine: route.origin_machine,
            execution_machine: route.execution_machine,
            payload: EventPayload::Report {
                report: ReportView {
                    seq: 1,
                    outcome: crate::domain::ReportOutcome::Blocked,
                    summary: "need input".into(),
                },
            },
        };
        store.accept_inbound_event(&report).unwrap();
        let inbox = store.inbound_events(route.task).unwrap();
        assert!(inbox[0].notification_required);
        assert_eq!(
            inbox[0].delivery,
            DeliveryState::PendingDelivery {
                attempts: 0,
                last_error: None
            }
        );
        assert!(!inbox[1].notification_required);
        assert_eq!(inbox[1].delivery, DeliveryState::NotRequired);
        assert!(!inbox[2].notification_required);
        assert_eq!(inbox[2].delivery, DeliveryState::NotRequired);
        let saved = store.origin_route_by_task(route.task).unwrap().unwrap();
        assert_eq!(saved.last_accepted_seq, 3);
        assert_eq!(saved.last_settled_seq, 0);
    }

    #[test]
    fn outbox_and_inbox_survive_reopen_and_version_three_migrates() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let route = route();
        {
            let mut store = Store::open(&path).unwrap();
            store.insert_origin_route(&route).unwrap();
            store
                .accept_execution(&ExecutionRecord {
                    task: route.task,
                    origin_machine: route.origin_machine,
                    execution_machine: route.execution_machine,
                    spec: route.spec.clone(),
                    state: crate::domain::ProcessStatus::Queued,
                })
                .unwrap();
            let outbox = store
                .append_outbound_event(
                    route.task,
                    route.origin_machine,
                    route.execution_machine,
                    EventPayload::State {
                        status: crate::domain::ProcessStatus::Running,
                    },
                )
                .unwrap();
            assert_eq!(outbox.event.seq.get(), 1);
            store.accept_inbound_event(&outbox.event).unwrap();
        }
        {
            let mut store = Store::open(&path).unwrap();
            let pending = store.pending_outbound_events(route.task).unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(store.inbound_events(route.task).unwrap().len(), 1);
            store
                .mark_outbound_acknowledged(route.task, pending[0].event.seq)
                .unwrap();
            assert!(
                store
                    .pending_outbound_events(route.task)
                    .unwrap()
                    .is_empty()
            );
            store
                .conn
                .execute(
                    "DELETE FROM executor_outbox WHERE task_id=?1 AND seq=1",
                    [route.task.to_string()],
                )
                .unwrap();
            let next = store
                .append_outbound_event(
                    route.task,
                    route.origin_machine,
                    route.execution_machine,
                    EventPayload::State {
                        status: crate::domain::ProcessStatus::Succeeded,
                    },
                )
                .unwrap();
            assert_eq!(next.event.seq.get(), 2);
            store.orphan_outbound_route(route.task).unwrap();
        }
        let reopened = Store::open(&path).unwrap();
        assert_eq!(
            reopened
                .outbound_route_status(route.task)
                .unwrap()
                .unwrap()
                .state,
            EventRouteState::Orphaned
        );
        assert!(reopened.pending_outbound_tasks().unwrap().is_empty());
        let legacy = dir.path().join("v3.db");
        {
            let mut store = Store::open(&legacy).unwrap();
            store.insert_origin_route(&route).unwrap();
            store
                .conn
                .execute_batch(
                    "DROP TABLE executor_outbox; DROP TABLE executor_event_cursors;
                 DROP TABLE executor_event_routes; DROP TABLE origin_inbox;
                 DROP TABLE executor_cancellations; DROP TABLE cancellation_requests;
                 ALTER TABLE tasks DROP COLUMN project_root;
                 PRAGMA user_version=3;",
                )
                .unwrap();
        }
        let store = Store::open(&legacy).unwrap();
        assert_eq!(
            store
                .origin_route_by_task(route.task)
                .unwrap()
                .unwrap()
                .task,
            route.task
        );
        assert!(store.inbound_events(route.task).unwrap().is_empty());
        assert_eq!(
            store
                .conn
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            crate::domain::SCHEMA_VERSION
        );
    }

    #[test]
    fn version_four_adds_durable_route_state() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("v4.db");
        {
            let store = Store::open(&path).unwrap();
            store
                .conn
                .execute_batch(
                    "DROP TABLE executor_event_routes;
                     DROP TABLE executor_cancellations; DROP TABLE cancellation_requests;
                     ALTER TABLE tasks DROP COLUMN project_root;
                     PRAGMA user_version=4;",
                )
                .unwrap();
        }
        let store = Store::open(&path).unwrap();
        let exists: bool = store
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='executor_event_routes')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists);
    }

    fn action_route() -> OriginRoute {
        let base = route();
        OriginRoute::new_resource_action(crate::submission::NewResourceActionRoute {
            request: base.request,
            task: base.task,
            callback: base.callback.clone(),
            spec: base.current_spec().unwrap().clone(),
            binding: crate::submission::ResourceActionRouteBinding {
                kind: crate::resource::bound_action::ResourceActionKind::ReleaseWatcher,
                authority: crate::resource::SupervisorActionAuthority {
                    authority_machine: base.execution_machine,
                    resource_id: crate::resource::ResourceId::new(),
                    loan_id: crate::resource::LoanId::new(),
                    action_id: crate::resource::ActionId::new(),
                    expected_state_revision: crate::resource::ResourceRevision::new(1),
                    supervisor: crate::resource::SupervisorAddress {
                        machine: base.origin_machine,
                        thread: base.thread,
                    },
                    assignment_revision: crate::resource::AssignmentRevision::new(0),
                },
            },
            launch: crate::resource::bound_action::ResourceActionLaunch::ReleaseWatcher {
                observed_background_task: TaskId::new(),
            },
        })
        .unwrap()
    }

    #[test]
    fn first_queued_event_accepts_an_action_route_and_duplicates_settle_once() {
        use crate::domain::ProcessStatus;

        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = action_route();
        store.insert_origin_route(&route).unwrap();

        assert!(matches!(
            store.accept_inbound_event(&event(&route, 1, ProcessStatus::Running)),
            Err(EventError::Invalid { .. })
        ));
        assert_eq!(
            store
                .accept_inbound_event(&event(&route, 1, ProcessStatus::Queued))
                .unwrap(),
            EventAcceptance::Acknowledged { seq: 1 }
        );
        let saved = store.origin_route_by_task(route.task).unwrap().unwrap();
        assert!(matches!(
            saved.submission,
            SubmissionState::ResourceAction {
                phase: crate::submission::ResourceActionRoutePhase::Accepted,
                ..
            }
        ));
        // a duplicate is acknowledged from the saved inbox, a later event is ordered
        assert_eq!(
            store
                .accept_inbound_event(&event(&route, 1, ProcessStatus::Queued))
                .unwrap(),
            EventAcceptance::Acknowledged { seq: 1 }
        );
        assert_eq!(
            store
                .accept_inbound_event(&event(&route, 3, ProcessStatus::Running))
                .unwrap(),
            EventAcceptance::Expected { seq: 2 }
        );
        assert_eq!(
            store
                .accept_inbound_event(&event(&route, 2, ProcessStatus::Running))
                .unwrap(),
            EventAcceptance::Acknowledged { seq: 2 }
        );
        assert_eq!(store.inbound_events(route.task).unwrap().len(), 2);
        // an event from another execution owner is refused
        let mut foreign = event(&route, 3, ProcessStatus::Running);
        foreign.execution_machine = MachineId::new();
        assert!(matches!(
            store.accept_inbound_event(&foreign),
            Err(EventError::OwnerConflict { .. })
        ));
    }

    #[test]
    fn rejected_action_route_refuses_task_events() {
        use crate::domain::ProcessStatus;

        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = action_route();
        store.insert_origin_route(&route).unwrap();
        store
            .resolve_resource_action_route(
                route.task,
                &crate::store::ResourceActionRouteResult::Rejected(
                    crate::resource::bound_action::ResourceActionRejection::ActionNotPending,
                ),
            )
            .unwrap();
        assert!(matches!(
            store.accept_inbound_event(&event(&route, 1, ProcessStatus::Queued)),
            Err(EventError::Invalid { .. })
        ));
        assert!(store.inbound_events(route.task).unwrap().is_empty());
    }

    fn background_route() -> OriginRoute {
        let base = route();
        OriginRoute::new_resource_background(crate::submission::NewResourceBackgroundRoute {
            request: base.request,
            task: base.task,
            callback: base.callback.clone(),
            spec: base.current_spec().unwrap().clone(),
            binding: crate::resource::background_launch::BackgroundLaunchBinding {
                assignment: crate::resource::background_launch::BackgroundSupervisorAssignment {
                    authority_machine: base.execution_machine,
                    resource_id: crate::resource::ResourceId::new(),
                    supervisor: crate::resource::SupervisorAddress {
                        machine: base.origin_machine,
                        thread: base.thread,
                    },
                    assignment_revision: crate::resource::AssignmentRevision::new(0),
                },
                expected_state_revision: crate::resource::ResourceRevision::new(2),
            },
        })
        .unwrap()
    }

    #[test]
    fn background_route_keeps_a_trainer_that_the_authority_accepted_after_a_saved_refusal() {
        use crate::domain::ProcessStatus;
        use crate::resource::background_launch::{
            RemoteBackgroundLaunchReceipt, ResourceBackgroundRejection,
        };
        use crate::store::ResourceBackgroundRouteResult;
        use crate::submission::ResourceBackgroundRoutePhase;

        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = background_route();
        store.insert_origin_route(&route).unwrap();
        let SubmissionState::ResourceBackground { binding, .. } = &route.submission else {
            unreachable!();
        };
        let receipt = RemoteBackgroundLaunchReceipt {
            binding: *binding,
            request_id: route.request,
            task_id: route.task,
            normalized_spec_sha256: crate::submission::normalized_spec_sha256(
                route.current_spec().unwrap(),
            )
            .unwrap(),
        };
        // a receipt for another assignment cannot accept this route
        let mut other = receipt;
        other.binding.assignment.assignment_revision = crate::resource::AssignmentRevision::new(1);
        assert!(matches!(
            store.resolve_resource_background_route(
                route.task,
                &ResourceBackgroundRouteResult::Accepted(other)
            ),
            Err(crate::store::IdentityError::Conflict)
        ));

        // a delayed send may win after a refusal was saved; its first queued
        // event is durable acceptance, so the trainer keeps its callback owner
        store
            .resolve_resource_background_route(
                route.task,
                &ResourceBackgroundRouteResult::Rejected(
                    ResourceBackgroundRejection::LaunchPending {
                        task_id: TaskId::new(),
                    },
                ),
            )
            .unwrap();
        assert!(matches!(
            store.accept_inbound_event(&event(&route, 1, ProcessStatus::Running)),
            Err(EventError::Invalid { .. })
        ));
        assert_eq!(
            store
                .accept_inbound_event(&event(&route, 1, ProcessStatus::Queued))
                .unwrap(),
            EventAcceptance::Acknowledged { seq: 1 }
        );
        let saved = store.origin_route_by_task(route.task).unwrap().unwrap();
        assert!(matches!(
            saved.submission,
            SubmissionState::ResourceBackground {
                phase: ResourceBackgroundRoutePhase::Accepted,
                ..
            }
        ));

        // an accepted route never moves back to a refusal; the exact receipt is idempotent
        assert!(matches!(
            store.resolve_resource_background_route(
                route.task,
                &ResourceBackgroundRouteResult::Rejected(
                    ResourceBackgroundRejection::NotCurrentSupervisor
                )
            ),
            Err(crate::store::IdentityError::Conflict)
        ));
        store
            .resolve_resource_background_route(
                route.task,
                &ResourceBackgroundRouteResult::Accepted(receipt),
            )
            .unwrap();
    }
}
