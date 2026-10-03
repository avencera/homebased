//! Dashboard metadata read with task rows: owners and callback delivery

use std::collections::HashMap;
use std::path::PathBuf;

use rusqlite::{OptionalExtension, params_from_iter};

use super::Store;
use super::events::is_terminal_callback_event;
use crate::callback::EventKind;
use crate::domain::{CallbackStatus, ProcessStatus, TaskId, TaskRow, ThreadId};
use crate::error::AppError;
use crate::events::{DeliveryState, EventPayload, TaskEvent};
use crate::machine::MachineId;
use crate::submission::{ExecutorIdentity, OriginRoute};

/// Both machine owners of one accepted execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskOwners {
    /// Machine that owns callbacks for the task
    pub origin_machine: MachineId,
    /// Machine that runs the task
    pub execution_machine: MachineId,
}

/// Dashboard metadata read with a task row
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPresentation {
    /// Task identity
    pub id: TaskId,
    /// Nearest Git worktree root, captured when the executor accepted the task
    pub project_root: Option<PathBuf>,
    /// Codex thread created by this task's worker, when the log included one
    pub worker_thread: Option<ThreadId>,
    /// Owners from an accepted executor identity. Queue runs have none
    pub owners: Option<TaskOwners>,
    /// Delivery of the terminal event, or `None` when this machine owns no
    /// callback for the task: the origin of a remote task delivers it, and a
    /// queue run reports through its job
    pub terminal_callback: Option<CallbackStatus>,
    /// Whether this task's inactivity reminder reached the origin queue
    pub attention_delivered: bool,
}

/// Which machine delivers a task's callbacks
enum CallbackOwner {
    /// No route or identity: a queue run, whose job reports
    Untracked,
    /// The executor of a remote task; its origin machine delivers
    OtherMachine,
    /// A local task whose origin inbox on this machine delivers
    ThisMachine,
}

impl Store {
    /// Read project roots and complete accepted machine-owner pairs for task IDs
    pub fn task_presentations(
        &self,
        ids: &[TaskId],
    ) -> Result<HashMap<TaskId, TaskPresentation>, AppError> {
        let mut presentations = HashMap::with_capacity(ids.len());

        for chunk in ids.chunks(500) {
            let id_values: Vec<String> = chunk.iter().map(ToString::to_string).collect();
            let placeholders = (1..=id_values.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT t.id, t.project_root, e.identity_json, t.worker_thread
                 FROM tasks t LEFT JOIN executor_identities e ON e.task_id=t.id
                 WHERE t.id IN ({placeholders})"
            );
            let mut statement = self.conn.prepare(&sql)?;
            let rows = statement
                .query_map(params_from_iter(id_values.iter()), presentation_from_row)?
                .collect::<Result<Vec<_>, _>>()?;
            drop(statement);

            for mut presentation in rows {
                let task = self.require_task(presentation.id)?;
                presentation.terminal_callback = self.terminal_callback(&task)?;
                presentation.attention_delivered = self.attention_callback_delivered(&task)?;
                presentations.insert(presentation.id, presentation);
            }
        }

        Ok(presentations)
    }

    /// Whether a terminal callback still waits for its inbox result to settle
    pub fn has_pending_terminal_callbacks(&self) -> Result<bool, AppError> {
        // waiting threads can sleep for hours; their events remain durable across restarts
        let rows = self.list_tasks(
            &[
                ProcessStatus::Succeeded,
                ProcessStatus::Failed,
                ProcessStatus::Cancelled,
                ProcessStatus::Lost,
                ProcessStatus::Preempted,
            ],
            None,
        )?;
        for row in rows {
            if matches!(
                self.terminal_callback(&row)?,
                Some(CallbackStatus::Pending | CallbackStatus::Sending)
            ) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn callback_owner(&self, row: &TaskRow) -> Result<CallbackOwner, AppError> {
        let route_json: Option<String> = self
            .conn
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [row.id.to_string()],
                |entry| entry.get(0),
            )
            .optional()?;
        let Some(route_json) = route_json else {
            let identity = self
                .executor_identity(row.id)
                .map_err(|error| error.into_task_error(row.id))?;
            return match identity {
                Some(ExecutorIdentity::Accepted(record))
                    if record.task == row.id
                        && record.origin_machine != record.execution_machine =>
                {
                    Ok(CallbackOwner::OtherMachine)
                }
                Some(_) => Err(AppError::ClusterTaskConflict { task: row.id }),
                None => Ok(CallbackOwner::Untracked),
            };
        };
        if !self.is_event_task(row.id)? {
            return Err(AppError::ClusterTaskConflict { task: row.id });
        }
        let route: OriginRoute = serde_json::from_str(&route_json)?;
        if route.task != row.id {
            return Err(AppError::Internal {
                message: format!("origin route task does not match task row {}", row.id),
            });
        }
        if route.origin_machine != route.execution_machine {
            return Ok(CallbackOwner::OtherMachine);
        }
        Ok(CallbackOwner::ThisMachine)
    }

    /// Delivery of a task's terminal event from this machine's origin inbox
    ///
    /// `None` when this machine owns no callback for the task: an executor
    /// whose origin is another machine, or a queue run, whose job reports
    fn terminal_callback(&self, row: &TaskRow) -> Result<Option<CallbackStatus>, AppError> {
        match self.callback_owner(row)? {
            CallbackOwner::Untracked | CallbackOwner::OtherMachine => return Ok(None),
            CallbackOwner::ThisMachine => {}
        }
        if !row.state.is_terminal() {
            return Ok(Some(CallbackStatus::Pending));
        }
        let Some(delivery) = self.terminal_callback_delivery(row.id)? else {
            // the terminal event has not reached the origin inbox yet
            return Ok(Some(CallbackStatus::Pending));
        };
        delivery
            .callback_status()
            .map(Some)
            .ok_or_else(|| AppError::Internal {
                message: format!("terminal callback event {} is marked not required", row.id),
            })
    }

    pub(super) fn terminal_callback_delivery(
        &self,
        id: TaskId,
    ) -> Result<Option<DeliveryState>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT seq,event_json,delivery_json FROM origin_inbox
             WHERE task_id=?1 ORDER BY seq",
        )?;
        let rows = statement
            .query_map([id.to_string()], |entry| {
                Ok((
                    entry.get::<_, i64>(0)?,
                    entry.get::<_, String>(1)?,
                    entry.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut latest: Option<(i64, DeliveryState)> = None;
        for (seq, event_json, delivery_json) in rows {
            let event: TaskEvent = serde_json::from_str(&event_json)?;
            if event.task != id {
                return Err(AppError::Internal {
                    message: format!("inbox event task does not match task row {id}"),
                });
            }
            if is_terminal_callback_event(&event) {
                latest = Some((seq, serde_json::from_str(&delivery_json)?));
            }
        }
        let mut statement = self.conn.prepare(
            "SELECT seq,delivery_json FROM origin_event_receipts
             WHERE task_id=?1 AND terminal_callback=1 ORDER BY seq",
        )?;
        let receipts = statement
            .query_map([id.to_string()], |entry| {
                Ok((entry.get::<_, i64>(0)?, entry.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (seq, delivery_json) in receipts {
            if latest
                .as_ref()
                .is_none_or(|(latest_seq, _)| seq > *latest_seq)
            {
                latest = Some((seq, serde_json::from_str(&delivery_json)?));
            }
        }
        Ok(latest.map(|(_, delivery)| delivery))
    }

    fn attention_callback_delivered(&self, row: &TaskRow) -> Result<bool, AppError> {
        match self.callback_owner(row)? {
            CallbackOwner::Untracked => return Ok(row.check_due_at.is_some()),
            CallbackOwner::OtherMachine => return Ok(false),
            CallbackOwner::ThisMachine => {}
        }

        let mut statement = self.conn.prepare(
            "SELECT event_json,delivery_json FROM origin_inbox
             WHERE task_id=?1 ORDER BY seq",
        )?;
        let rows = statement
            .query_map([row.id.to_string()], |entry| {
                Ok((entry.get::<_, String>(0)?, entry.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut latest = None;
        for (event_json, delivery_json) in rows {
            let event: TaskEvent = serde_json::from_str(&event_json)?;
            if is_check_due_event(&event) {
                latest = Some(serde_json::from_str::<DeliveryState>(&delivery_json)?);
            }
        }
        if let Some(delivery) = latest {
            return Ok(matches!(delivery, DeliveryState::Delivered { .. }));
        }

        let mut statement = self.conn.prepare(
            "SELECT event_json FROM executor_outbox
             WHERE task_id=?1 AND state='pending' ORDER BY seq",
        )?;
        let events = statement
            .query_map([row.id.to_string()], |entry| entry.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for event_json in events {
            let event: TaskEvent = serde_json::from_str(&event_json)?;
            if is_check_due_event(&event) {
                return Ok(false);
            }
        }

        // the reminder event may be compacted away; the row keeps when it was produced
        Ok(row.check_due_at.is_some())
    }
}

fn is_check_due_event(event: &TaskEvent) -> bool {
    matches!(
        &event.payload,
        EventPayload::Callback { event, .. } if event.event == EventKind::TaskCheckDue
    )
}

fn presentation_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskPresentation> {
    let raw_id: String = row.get(0)?;
    let project_root: Option<String> = row.get(1)?;
    let identity_json: Option<String> = row.get(2)?;
    let raw_worker_thread: Option<String> = row.get(3)?;
    let id: TaskId = raw_id
        .parse()
        .map_err(|error: AppError| conversion_error(0, error))?;
    let worker_thread = raw_worker_thread
        .map(|raw| raw.parse::<ThreadId>())
        .transpose()
        .map_err(|error| conversion_error(0, error))?;
    let identity = identity_json
        .map(|json| serde_json::from_str::<ExecutorIdentity>(&json))
        .transpose()
        .map_err(|error| conversion_error(2, error))?;
    let owners = match identity {
        Some(ExecutorIdentity::Accepted(record)) if record.task == id => Some(TaskOwners {
            origin_machine: record.origin_machine,
            execution_machine: record.execution_machine,
        }),
        Some(ExecutorIdentity::Accepted(_)) => {
            let error = AppError::Internal {
                message: format!("executor identity task does not match task row {id}"),
            };
            return Err(conversion_error(0, error));
        }
        Some(ExecutorIdentity::Rejected(_)) | None => None,
    };
    Ok(TaskPresentation {
        id,
        project_root: project_root.map(PathBuf::from),
        worker_thread,
        owners,
        terminal_callback: None,
        attention_delivered: false,
    })
}

fn conversion_error(
    column: usize,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, rusqlite::types::Type::Text, Box::new(error))
}
