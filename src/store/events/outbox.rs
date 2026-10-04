//! Executor outbox: sequenced events a task's execution machine sends its origin

use std::num::NonZeroU64;

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{decode, encode, executor_identity_on, sql_seq, storage, task_ids, validate};
use crate::domain::{API_VERSION, TaskId};
use crate::error::AppError;
use crate::events::{
    EventError, EventPayload, EventRouteState, EventRouteStatus, OutboxEvent, OutboxState,
    TaskEvent,
};
use crate::machine::MachineId;
use crate::store::{Store, fmt_time};
use crate::submission::ExecutorIdentity;

impl Store {
    /// Find tasks with pending events whose route is not orphaned
    pub fn pending_outbound_tasks(&self) -> Result<Vec<TaskId>, EventError> {
        task_ids(
            &self.conn,
            "SELECT DISTINCT o.task_id FROM executor_outbox o
             LEFT JOIN executor_event_routes r ON r.task_id=o.task_id
             WHERE o.state='pending' AND r.task_id IS NULL ORDER BY o.task_id",
            "outbox",
        )
    }

    /// Read the fixed destination and retained event counts for inspection
    pub fn outbound_route_status(
        &self,
        task: TaskId,
    ) -> Result<Option<EventRouteStatus>, EventError> {
        let Some(ExecutorIdentity::Accepted(record)) = executor_identity_on(&self.conn, task)?
        else {
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
        let row: Option<(String, String)> = self
            .conn
            .query_row(
                "SELECT event_json,state FROM executor_outbox
             WHERE task_id=?1 AND seq>=?2 ORDER BY seq LIMIT 1",
                params![task.to_string(), sql_seq(seq.get())?],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(storage)?;
        row.map(|(json, state)| {
            let event: TaskEvent = decode(&json)?;
            validate(&event)?;
            let state = match state.as_str() {
                "pending" => OutboxState::Pending,
                "acknowledged" => OutboxState::Acknowledged,
                _ => {
                    return Err(EventError::Invalid {
                        message: "invalid outbox state".into(),
                    });
                }
            };
            Ok(OutboxEvent { event, state })
        })
        .transpose()
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
    pub(in crate::store) fn append_produced_event(
        &self,
        task: TaskId,
        payload: EventPayload,
    ) -> Result<(), AppError> {
        append_produced_event_on(&self.conn, task, payload)
    }

    /// Read retained, unacknowledged executor events in sequence order
    pub fn pending_outbound_events(&self, task: TaskId) -> Result<Vec<OutboxEvent>, EventError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT event_json FROM executor_outbox
                 WHERE task_id=?1 AND state='pending' ORDER BY seq",
            )
            .map_err(storage)?;
        let rows = stmt
            .query_map([task.to_string()], |row| row.get::<_, String>(0))
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        rows.into_iter()
            .map(|json| {
                let event: TaskEvent = decode(&json)?;
                validate(&event)?;
                Ok(OutboxEvent {
                    event,
                    state: OutboxState::Pending,
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
                params![fmt_time(Utc::now()), task.to_string(), sql_seq(seq.get())?],
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
}

pub(in crate::store) fn append_produced_event_on(
    conn: &Connection,
    task: TaskId,
    payload: EventPayload,
) -> Result<(), AppError> {
    let identity = crate::store::identity::executor_identity_on(conn, task)
        .map_err(|error| error.into_task_error(task))?;
    let ExecutorIdentity::Accepted(record) = identity.ok_or_else(|| AppError::Internal {
        message: format!("executor identity for task {task} is missing"),
    })?
    else {
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

fn append_outbound_event_on(
    conn: &Connection,
    task: TaskId,
    origin_machine: MachineId,
    execution_machine: MachineId,
    payload: EventPayload,
) -> Result<OutboxEvent, EventError> {
    let Some(ExecutorIdentity::Accepted(record)) = executor_identity_on(conn, task)? else {
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
        event,
        state: OutboxState::Pending,
    };
    conn.execute(
        "INSERT INTO executor_outbox (task_id,seq,event_json,state) VALUES (?1,?2,?3,'pending')",
        params![task.to_string(), next, encode(&row.event)?],
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
