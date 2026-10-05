//! Origin inbox: accepted events and their callback delivery results

use std::num::NonZeroU64;

use chrono::Utc;
use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::{decode, encode, event_digest, origin_route_on, sql_seq, storage, task_ids, validate};
use crate::callback::EventKind;
use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::events::{
    DeliveryOutcome, DeliveryState, EventAcceptance, EventError, EventPayload, FailedInboxEvent,
    InboxEvent, THREAD_WAIT_LIMIT, TaskEvent, WaitingInboxEvent,
};
use crate::store::{Store, dependency, fmt_time};
use crate::submission::{HeldPhase, SubmissionState};

impl Store {
    /// Task IDs with an unsettled callback, including those left by a prior daemon
    pub fn pending_inbox_tasks(&self) -> Result<Vec<TaskId>, EventError> {
        task_ids(
            &self.conn,
            "SELECT DISTINCT task_id FROM origin_inbox
             WHERE json_extract(delivery_json, '$.type') IN ('pending_delivery','awaiting_thread')
             ORDER BY task_id",
            "inbox",
        )
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
        let route = origin_route_on(&tx, task)?;
        let row: Option<(String, String)> = tx
            .query_row(
                "SELECT event_json,delivery_json FROM origin_inbox
             WHERE task_id=?1 AND seq>?2 ORDER BY seq LIMIT 1",
                params![task.to_string(), sql_seq(route.last_settled_seq)?],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(storage)?;
        tx.commit().map_err(storage)?;
        row.map(|(event, delivery)| {
            Ok(InboxEvent {
                event: decode(&event)?,
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
        let route = origin_route_on(&tx, task)?;
        let row: Option<(i64, String, String)> = tx
            .query_row(
                "SELECT seq,event_json,delivery_json FROM origin_inbox
             WHERE task_id=?1 AND seq>?2 ORDER BY seq LIMIT 1",
                params![task.to_string(), sql_seq(route.last_settled_seq)?],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(storage)?;
        let Some((first_seq, event_json, delivery_json)) = row else {
            return Ok(None);
        };
        if first_seq != sql_seq(seq.get())? {
            return Ok(None);
        }
        let current_delivery: DeliveryState = decode(&delivery_json)?;
        let delivery = match current_delivery {
            DeliveryState::PendingDelivery {
                attempts,
                last_error,
            } if attempts < 3 => DeliveryState::PendingDelivery {
                attempts: attempts + 1,
                last_error,
            },
            DeliveryState::AwaitingThread {
                attempts,
                since,
                reason,
            } if attempts < 3 => DeliveryState::AwaitingThread {
                attempts: attempts + 1,
                since,
                reason,
            },
            _ => return Ok(None),
        };
        tx.execute(
            "UPDATE origin_inbox SET delivery_json=?1 WHERE task_id=?2 AND seq=?3",
            params![encode(&delivery)?, task.to_string(), first_seq],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(Some(InboxEvent {
            event: decode(&event_json)?,
            delivery,
        }))
    }

    /// Record an outcome for the earliest inbox event and advance only a contiguous settled prefix
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
        let mut route = origin_route_on(&tx, task)?;
        let (event_json, delivery_json): (String, String) = tx
            .query_row(
                "SELECT event_json,delivery_json FROM origin_inbox WHERE task_id=?1 AND seq=?2",
                params![task.to_string(), sql_seq(seq.get())?],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(storage)?;
        let current_delivery: DeliveryState = decode(&delivery_json)?;
        let (attempts, last_error, waiting_since) = match current_delivery {
            DeliveryState::PendingDelivery {
                attempts,
                last_error,
            } => (attempts, last_error, None),
            DeliveryState::AwaitingThread {
                attempts,
                since,
                reason,
            } => (attempts, Some(reason), Some(since)),
            _ => {
                return Err(EventError::Invalid {
                    message: "inbox event is not unsettled".into(),
                });
            }
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
            DeliveryOutcome::Deferred(reason) if attempts > 0 => {
                let since = waiting_since.unwrap_or_else(Utc::now);
                let attempts = attempts.saturating_sub(1);
                if Utc::now() - since >= THREAD_WAIT_LIMIT {
                    DeliveryState::DeliveryFailed {
                        attempts,
                        last_error: format!(
                            "gave up after waiting {} days for the origin thread: {reason}",
                            THREAD_WAIT_LIMIT.num_days()
                        ),
                    }
                } else {
                    DeliveryState::AwaitingThread {
                        attempts,
                        since,
                        reason,
                    }
                }
            }
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
            DeliveryOutcome::Delivered | DeliveryOutcome::Deferred(_) => {
                return Err(EventError::Invalid {
                    message: "delivery outcome without a reserved attempt".into(),
                });
            }
        };
        let settled = !delivery.is_unsettled();
        tx.execute(
            "UPDATE origin_inbox
             SET delivery_json=?1,
                 settled_at=CASE WHEN ?2 THEN COALESCE(settled_at,?3) ELSE NULL END
             WHERE task_id=?4 AND seq=?5",
            params![
                encode(&delivery)?,
                settled,
                fmt_time(Utc::now()),
                task.to_string(),
                sql_seq(seq.get())?,
            ],
        )
        .map_err(storage)?;
        if matches!(delivery, DeliveryState::Delivered { .. }) {
            let event: TaskEvent = decode(&event_json)?;
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
        if settled {
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
                if decode::<DeliveryState>(&next_json)?.is_unsettled() {
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
            event: decode(&event_json)?,
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

    /// List callbacks that wait for their origin thread, with the time each wait ends
    pub fn waiting_inbox_events(&self, task: TaskId) -> Result<Vec<WaitingInboxEvent>, EventError> {
        Ok(self
            .inbound_events(task)?
            .into_iter()
            .filter_map(|entry| match entry.delivery {
                DeliveryState::AwaitingThread { since, reason, .. } => Some(WaitingInboxEvent {
                    seq: entry.event.seq.get(),
                    since,
                    until: since + THREAD_WAIT_LIMIT,
                    reason,
                }),
                _ => None,
            })
            .collect())
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
        let mut route = origin_route_on(&tx, event.task)?;
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
        let accept_held_launch = match &route.submission {
            // the executor's first queued event proves that a released launch
            // was accepted, even when its reply was lost
            SubmissionState::Held {
                phase: HeldPhase::Launching,
            } => {
                if event.payload.process_state() != Some(ProcessStatus::Queued) {
                    return Err(EventError::Invalid {
                        message: "first released task event must report queued state".into(),
                    });
                }

                true
            }
            SubmissionState::Held {
                phase: HeldPhase::Waiting | HeldPhase::Cancelled { .. },
            } => {
                return Err(EventError::Invalid {
                    message: "held route has not launched a task".into(),
                });
            }
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
        let accepted_at = fmt_time(Utc::now());
        let settled_at = (!event.payload.notification_required()).then(|| accepted_at.clone());
        tx.execute(
            "INSERT INTO origin_inbox (task_id,seq,event_json,delivery_json,settled_at)
             VALUES (?1,?2,?3,?4,?5)",
            params![
                event.task.to_string(),
                sql_seq(event.seq.get())?,
                encode(event)?,
                encode(&delivery)?,
                settled_at,
            ],
        )
        .map_err(storage)?;
        route.last_accepted_seq = event.seq.get();
        route.last_updated_at = Utc::now();
        if let Some(state) = event.payload.process_state() {
            route.last_execution_state = Some(state);
        }
        if accept_held_launch {
            route.submission = SubmissionState::Accepted;
        }
        route.validate().map_err(|error| {
            EventError::Storage(AppError::Internal {
                message: format!("invalid origin route transition: {error}"),
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
        if let Some(outcome) = dependency::terminal_outcome(&event.payload) {
            dependency::record_outcome_on(&tx, event.task, outcome).map_err(EventError::Storage)?;
        }
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
                "SELECT event_json,delivery_json FROM origin_inbox
                 WHERE task_id=?1 ORDER BY seq",
            )
            .map_err(storage)?;
        let rows = stmt
            .query_map([task.to_string()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        rows.into_iter()
            .map(|(event, delivery)| {
                let event: TaskEvent = decode(&event)?;
                let delivery: DeliveryState = decode(&delivery)?;
                validate(&event)?;
                if matches!(delivery, DeliveryState::NotRequired)
                    == event.payload.notification_required()
                {
                    return Err(EventError::Storage(AppError::Internal {
                        message: "inbox delivery state disagrees with its event".into(),
                    }));
                }
                Ok(InboxEvent { event, delivery })
            })
            .collect()
    }
}
