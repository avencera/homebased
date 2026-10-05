//! Executor outbox and origin inbox persistence: shared event codecs and checks

use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest, Sha256};

use super::IdentityError;
use crate::callback::EventKind;
use crate::domain::{API_VERSION, ProcessStatus, SUMMARY_MAX_BYTES, TaskId};
use crate::error::AppError;
use crate::events::{EventError, EventPayload, TaskEvent};
use crate::submission::{ExecutorIdentity, OriginRoute};

mod inbox;
mod outbox;
mod retention;
#[cfg(test)]
mod tests;

pub(super) use outbox::append_produced_event_on;
pub(crate) use retention::EventRetentionBatch;

/// Whether an event carries a task's terminal callback
pub(super) fn is_terminal_callback_event(event: &TaskEvent) -> bool {
    matches!(
        &event.payload,
        EventPayload::Callback {
            state: Some(status),
            ..
        } if status.is_terminal()
    )
}

fn storage(error: rusqlite::Error) -> EventError {
    EventError::Storage(error.into())
}

/// Read one executor identity, treating an invalid saved owner as an owner conflict
fn executor_identity_on(
    conn: &Connection,
    task: TaskId,
) -> Result<Option<ExecutorIdentity>, EventError> {
    super::identity::executor_identity_on(conn, task).map_err(|error| match error {
        IdentityError::Storage(error) => EventError::Storage(error),
        IdentityError::Conflict | IdentityError::RouteNotFound => {
            EventError::OwnerConflict { task }
        }
    })
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
            message: format!("invalid saved origin route: {error}"),
        })
    })?;
    Ok(route)
}

/// Read the saved origin route of `task`
fn origin_route_on(conn: &Connection, task: TaskId) -> Result<OriginRoute, EventError> {
    let route_json: String = conn
        .query_row(
            "SELECT route_json FROM origin_routes WHERE task_id=?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?
        .ok_or(EventError::RouteNotFound { task })?;
    decode_route(&route_json)
}

/// Parse the task UUID column that `sql` selects from the `table` it names
fn task_ids(conn: &Connection, sql: &str, table: &str) -> Result<Vec<TaskId>, EventError> {
    let mut stmt = conn.prepare(sql).map_err(storage)?;
    stmt.query_map([], |row| row.get::<_, String>(0))
        .map_err(storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(storage)?
        .into_iter()
        .map(|id| {
            id.parse().map_err(|_| EventError::Invalid {
                message: format!("invalid {table} task UUID"),
            })
        })
        .collect()
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
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
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
    // a preempted run is not a failure; its event and state must agree, so a
    // dependency reads the same outcome from either
    if let EventPayload::Callback {
        event: callback,
        state,
    } = &event.payload
        && (callback.event == EventKind::TaskPreempted)
            != (*state == Some(ProcessStatus::Preempted))
    {
        return Err(EventError::Invalid {
            message: "TASK_PREEMPTED and the preempted state must appear together".into(),
        });
    }
    // a parked run names its handover, ended with exit 0, and never crosses machines
    if let EventPayload::Callback {
        event: callback,
        state,
    } = &event.payload
    {
        let waiting = callback.event == EventKind::TaskWaiting;
        let handover = (
            callback.waiting_on.is_some(),
            callback.continuation.is_some(),
        );
        let valid = if waiting {
            handover == (true, true)
                && *state == Some(ProcessStatus::Succeeded)
                && event.origin_machine == event.execution_machine
        } else {
            handover == (false, false)
        };
        if !valid {
            return Err(EventError::Invalid {
                message: "TASK_WAITING needs its handover, exit 0, and one machine".into(),
            });
        }
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
