//! Held origin routes, their dependencies, and the terminal outcomes that release them
//!
//! A task's outcome is saved on its origin route in the same transaction that
//! accepts its terminal event, or that ends a held route before launch, so the
//! dependency release reads one durable value after any restart

use std::num::NonZeroU64;
use std::path::Path;

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::Store;
use crate::callback::{UnlaunchedEnding, unlaunched_event};
use crate::dependency::{
    DependencyLookup, DependencyOutcome, DependencyState, HeldCancellation, TaskDependencies,
    TaskOutcome,
};
use crate::domain::{CallbackStatus, ProcessStatus, TaskId};
use crate::error::AppError;
use crate::events::{DeliveryState, EventPayload, TaskEvent};
use crate::submission::{
    DependentRoute, HeldPhase, OriginRoute, PreAcceptanceRejection, SubmissionState,
};

fn decode_route(json: &str) -> Result<OriginRoute, AppError> {
    let route: OriginRoute = serde_json::from_str(json)?;
    route.validate().map_err(|error| AppError::Internal {
        message: format!("invalid saved origin route: {error}"),
    })?;
    Ok(route)
}

fn decode_after(json: Option<&str>) -> Result<Option<TaskDependencies>, AppError> {
    Ok(json.map(serde_json::from_str).transpose()?)
}

/// Read one route and its dependencies inside the caller's transaction
fn dependent_route_on(
    conn: &Connection,
    task: TaskId,
) -> Result<Option<(OriginRoute, Option<TaskDependencies>)>, AppError> {
    let saved: Option<(String, Option<String>)> = conn
        .query_row(
            "SELECT route_json,after_json FROM origin_routes WHERE task_id=?1",
            [task.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((route_json, after_json)) = saved else {
        return Ok(None);
    };
    let route = decode_route(&route_json)?;
    if route.task != task {
        return Err(AppError::ClusterTaskConflict { task });
    }
    Ok(Some((route, decode_after(after_json.as_deref())?)))
}

fn save_route_on(conn: &Connection, route: &OriginRoute) -> Result<(), AppError> {
    route.validate().map_err(|error| AppError::Internal {
        message: format!("invalid held route transition: {error}"),
    })?;
    conn.execute(
        "UPDATE origin_routes SET route_json=?1 WHERE task_id=?2",
        params![serde_json::to_string(route)?, route.task.to_string()],
    )?;
    Ok(())
}

/// Save how a task ended; the first terminal outcome wins
pub(super) fn record_outcome_on(
    conn: &Connection,
    task: TaskId,
    outcome: TaskOutcome,
) -> Result<(), rusqlite::Error> {
    conn.execute(
        "UPDATE origin_routes SET outcome=?1 WHERE task_id=?2 AND outcome IS NULL",
        params![outcome.as_str(), task.to_string()],
    )?;
    Ok(())
}

/// Outcome named by a terminal callback event, or `None` for any other event
pub(super) fn terminal_outcome(payload: &EventPayload) -> Option<TaskOutcome> {
    match payload {
        EventPayload::Callback {
            event,
            state: Some(state),
        } if state.is_terminal() => TaskOutcome::from_event(event.event),
        EventPayload::Callback { .. }
        | EventPayload::State { .. }
        | EventPayload::Report { .. } => None,
    }
}

/// Queue the origin's own terminal event for a route that never launched
///
/// The event takes the next origin sequence and waits for delivery like an
/// executor event. The route keeps no process state, because no process ran
pub(super) fn append_unlaunched_event_on(
    conn: &Connection,
    tasks_dir: &Path,
    route: &mut OriginRoute,
    ending: UnlaunchedEnding,
) -> Result<(), AppError> {
    let spec = route.current_spec().ok_or_else(|| AppError::Internal {
        message: format!("held task {} has no saved spec", route.task),
    })?;
    let (state, outcome) = match &ending {
        UnlaunchedEnding::Cancelled(_) => (ProcessStatus::Cancelled, TaskOutcome::Cancelled),
        UnlaunchedEnding::LaunchRefused(_) => (ProcessStatus::Failed, TaskOutcome::Failed),
    };
    let evidence = tasks_dir.join(route.task.to_string());
    let callback = unlaunched_event(route.task, spec, evidence, ending);
    let seq = route
        .last_accepted_seq
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .ok_or_else(|| AppError::Internal {
            message: "origin event sequence exhausted".into(),
        })?;
    let event = TaskEvent {
        task: route.task,
        seq,
        origin_machine: route.origin_machine,
        execution_machine: route.execution_machine,
        payload: EventPayload::Callback {
            event: Box::new(callback),
            state: Some(state),
        },
    };
    let delivery = DeliveryState::PendingDelivery {
        attempts: 0,
        last_error: None,
    };
    conn.execute(
        "INSERT INTO origin_inbox
         (task_id,seq,origin_machine,execution_machine,event_json,notification_required,delivery_json,settled_at)
         VALUES (?1,?2,?3,?4,?5,1,?6,NULL)",
        params![
            route.task.to_string(),
            i64::try_from(seq.get()).map_err(|_| AppError::Internal {
                message: "origin event sequence exceeds storage".into(),
            })?,
            route.origin_machine.to_string(),
            route.execution_machine.to_string(),
            serde_json::to_string(&event)?,
            serde_json::to_string(&delivery)?,
        ],
    )?;
    route.last_accepted_seq = seq.get();
    route.last_updated_at = Some(Utc::now());
    save_route_on(conn, route)?;
    record_outcome_on(conn, route.task, outcome)?;
    Ok(())
}

/// How a released held route ends when its executor refused it before acceptance
pub(super) fn refused_launch_ending(reason: &str) -> UnlaunchedEnding {
    if reason == PreAcceptanceRejection::Cancelled.as_str() {
        UnlaunchedEnding::Cancelled(HeldCancellation::Requested)
    } else {
        UnlaunchedEnding::LaunchRefused(reason.to_owned())
    }
}

/// What the origin knows about one dependency from its route alone
///
/// A route that closed before any task ran never produces a terminal event,
/// so its closure is its ending
fn closed_route_outcome(route: &OriginRoute) -> Option<TaskOutcome> {
    match &route.submission {
        SubmissionState::Rejected { .. } => Some(TaskOutcome::Failed),
        SubmissionState::Held {
            phase: HeldPhase::Cancelled { .. },
        } => Some(TaskOutcome::Cancelled),
        SubmissionState::AcceptanceUnknown
        | SubmissionState::Accepted
        | SubmissionState::Held { .. } => None,
    }
}

/// How a dependency with no saved outcome ended, or `None` while it runs
///
/// Every terminal event saves its outcome with the route's terminal state, so
/// a terminal state without one means the event that named it is gone. That
/// happens only to a task that ended before outcomes were saved, and its
/// process status cannot stand in for the outcome
fn unrecorded_outcome(route: &OriginRoute) -> Option<DependencyOutcome> {
    if let Some(outcome) = closed_route_outcome(route) {
        return Some(outcome.into());
    }
    route
        .last_execution_state
        .is_some_and(ProcessStatus::is_terminal)
        .then_some(DependencyOutcome::Unknown)
}

/// Routes with dependencies whose task never launched: held, cancelled, or refused
const UNLAUNCHED: &str = "json_extract(route_json, '$.submission.type') IN ('held', 'rejected')";

/// One task that its origin holds or ended before launch, for the task list
#[derive(Debug, Clone)]
pub struct UnlaunchedTask {
    /// Saved route and its dependencies
    pub held: DependentRoute,
    /// Delivery of the terminal event, once the origin ended the task
    pub callback: CallbackStatus,
}

/// Result of asking the origin to cancel a held route
#[derive(Debug, Clone)]
pub enum HeldCancel {
    /// The route is cancelled before launch, by this call or an earlier one
    Cancelled(OriginRoute),
    /// The route already launched or closed, so ordinary cancellation applies
    NotHeld(OriginRoute),
}

impl Store {
    /// Dependencies saved with a route, or `None` for a route submitted without `after`
    pub fn route_dependencies(&self, task: TaskId) -> Result<Option<TaskDependencies>, AppError> {
        let saved: Option<Option<String>> = self
            .conn
            .query_row(
                "SELECT after_json FROM origin_routes WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        decode_after(saved.flatten().as_deref())
    }

    /// Routes that still wait for dependencies or for their remote launch to be answered
    pub fn held_routes(&self) -> Result<Vec<DependentRoute>, AppError> {
        self.dependent_routes(
            "json_extract(route_json, '$.submission.type') = 'held'
             AND json_extract(route_json, '$.submission.phase.type') IN ('waiting', 'launching')",
        )
    }

    /// Local tasks submitted with dependencies whose committed rows still wait for a worker
    ///
    /// A released task has no caller to retry its launch, so its origin
    /// resumes it, as a submit retry resumes an ordinary one. A row stays
    /// queued until its worker claims it, so a task here never ran
    pub fn unstarted_dependent_tasks(&self) -> Result<Vec<TaskId>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT r.task_id FROM origin_routes r JOIN tasks t ON t.id = r.task_id
             WHERE r.after_json IS NOT NULL AND t.status = 'queued' AND json_valid(r.route_json)
               AND json_extract(r.route_json, '$.submission.type') = 'accepted'
               AND json_extract(r.route_json, '$.origin_machine')
                   = json_extract(r.route_json, '$.execution_machine')
             ORDER BY r.task_id",
        )?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|task| {
                task.parse().map_err(|_| AppError::Internal {
                    message: format!("invalid task id {task} in origin routes"),
                })
            })
            .collect()
    }

    /// Routes with dependencies that never launched a task: held, cancelled, or refused
    ///
    /// The origin is the only machine with a record of these tasks, so its task
    /// list shows them
    pub fn unlaunched_tasks(&self) -> Result<Vec<UnlaunchedTask>, AppError> {
        self.dependent_routes(UNLAUNCHED)?
            .into_iter()
            .map(|held| self.unlaunched(held))
            .collect()
    }

    /// One task with dependencies that never launched, or `None` for any other task
    pub fn unlaunched_task(&self, task: TaskId) -> Result<Option<UnlaunchedTask>, AppError> {
        let Some((route, Some(after))) = dependent_route_on(&self.conn, task)? else {
            return Ok(None);
        };
        if !matches!(
            route.submission,
            SubmissionState::Held { .. } | SubmissionState::Rejected { .. }
        ) {
            return Ok(None);
        }
        self.unlaunched(DependentRoute { route, after }).map(Some)
    }

    fn unlaunched(&self, held: DependentRoute) -> Result<UnlaunchedTask, AppError> {
        let callback = match self.terminal_callback_delivery(held.route.task)? {
            None
            | Some(DeliveryState::NotRequired)
            | Some(DeliveryState::PendingDelivery { attempts: 0, .. }) => CallbackStatus::Pending,
            Some(DeliveryState::PendingDelivery { .. }) => CallbackStatus::Sending,
            Some(DeliveryState::AwaitingThread { .. }) => CallbackStatus::Waiting,
            Some(DeliveryState::Delivered { .. }) => CallbackStatus::Sent,
            Some(DeliveryState::DeliveryFailed { .. }) => CallbackStatus::Failed,
        };
        Ok(UnlaunchedTask { held, callback })
    }

    fn dependent_routes(&self, filter: &str) -> Result<Vec<DependentRoute>, AppError> {
        let sql = format!(
            "SELECT task_id,route_json,after_json FROM origin_routes
             WHERE after_json IS NOT NULL AND json_valid(route_json) AND {filter}
             ORDER BY task_id"
        );
        let mut statement = self.conn.prepare(&sql)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(task, route_json, after_json)| {
                let route = decode_route(&route_json)?;
                if route.task.to_string() != task {
                    return Err(AppError::ClusterTaskConflict { task: route.task });
                }
                let after = serde_json::from_str(&after_json)?;
                Ok(DependentRoute { route, after })
            })
            .collect()
    }

    /// State of each dependency, or `None` for a task with no origin route here
    pub fn dependency_states(&self, tasks: &[TaskId]) -> Result<DependencyLookup, AppError> {
        tasks
            .iter()
            .map(|task| {
                let saved: Option<(String, Option<String>)> = self
                    .conn
                    .query_row(
                        "SELECT route_json,outcome FROM origin_routes WHERE task_id=?1",
                        [task.to_string()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                let Some((route_json, outcome)) = saved else {
                    return Ok((*task, None));
                };
                let outcome = match outcome {
                    Some(outcome) => Some(DependencyOutcome::Known(
                        TaskOutcome::from_storage(&outcome).ok_or_else(|| AppError::Internal {
                            message: format!("unknown task outcome {outcome} for {task}"),
                        })?,
                    )),
                    None => unrecorded_outcome(&decode_route(&route_json)?),
                };
                let state = outcome.map_or(DependencyState::Pending, DependencyState::Ended);
                Ok((*task, Some(state)))
            })
            .collect()
    }

    /// Cancel a held route before launch and queue its `TASK_CANCELLED` event
    ///
    /// Only a waiting route can be cancelled here. A route whose launch began
    /// is returned unchanged, because its executor may already hold the task
    pub fn cancel_held_route(
        &mut self,
        task: TaskId,
        cause: HeldCancellation,
    ) -> Result<HeldCancel, AppError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (mut route, _) =
            dependent_route_on(&tx, task)?.ok_or(AppError::TaskNotFound { id: task })?;
        let result = match &route.submission {
            SubmissionState::Held {
                phase: HeldPhase::Waiting,
            } => {
                route.submission = SubmissionState::Held {
                    phase: HeldPhase::Cancelled { cause },
                };
                append_unlaunched_event_on(
                    &tx,
                    &self.tasks_dir,
                    &mut route,
                    UnlaunchedEnding::Cancelled(cause),
                )?;
                HeldCancel::Cancelled(route)
            }
            SubmissionState::Held {
                phase: HeldPhase::Cancelled { .. },
            } => HeldCancel::Cancelled(route),
            _ => HeldCancel::NotHeld(route),
        };
        tx.commit()?;
        Ok(result)
    }

    /// Mark a waiting remote route as launching before its first send
    ///
    /// A launching route is returned unchanged so a resend keeps its identity;
    /// any other route is returned as saved for the caller to inspect
    pub fn begin_held_launch(&mut self, task: TaskId) -> Result<OriginRoute, AppError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (mut route, _) =
            dependent_route_on(&tx, task)?.ok_or(AppError::TaskNotFound { id: task })?;
        if matches!(
            route.submission,
            SubmissionState::Held {
                phase: HeldPhase::Waiting
            }
        ) {
            if route.origin_machine == route.execution_machine {
                return Err(AppError::Internal {
                    message: format!("local held task {task} has no remote launch"),
                });
            }
            route.submission = SubmissionState::Held {
                phase: HeldPhase::Launching,
            };
            route.last_updated_at = Some(Utc::now());
            save_route_on(&tx, &route)?;
        }
        tx.commit()?;
        Ok(route)
    }

    /// Close a waiting route whose launch was refused before any task was saved
    ///
    /// The route is rejected with `reason`, and its thread gets `TASK_FAILED`
    pub fn refuse_held_launch(
        &mut self,
        task: TaskId,
        reason: &str,
    ) -> Result<OriginRoute, AppError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (mut route, _) =
            dependent_route_on(&tx, task)?.ok_or(AppError::TaskNotFound { id: task })?;
        if matches!(
            route.submission,
            SubmissionState::Held {
                phase: HeldPhase::Waiting
            }
        ) {
            route.submission = SubmissionState::Rejected {
                reason: reason.to_owned(),
            };
            append_unlaunched_event_on(
                &tx,
                &self.tasks_dir,
                &mut route,
                UnlaunchedEnding::LaunchRefused(reason.to_owned()),
            )?;
        }
        tx.commit()?;
        Ok(route)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;

    use super::HeldCancel;
    use crate::dependency::{DependencyState, HeldCancellation, TaskDependencies, TaskOutcome};
    use crate::domain::{TaskEnv, TaskId};
    use crate::events::EventPayload;
    use crate::machine::MachineId;
    use crate::store::Store;
    use crate::submission::{
        CallbackContext, HeldPhase, NewHeldRoute, OriginRoute, RequestId, SubmissionState,
    };

    fn held_route(execution_machine: MachineId) -> OriginRoute {
        let spec = serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "held task",
            "cwd": "/tmp",
            "machine": "executor",
            "timeout": "4h",
            "workload": {"type": "task", "command": ["echo", "hello"]}
        }))
        .unwrap();
        OriginRoute::new_held(NewHeldRoute {
            request: RequestId::new(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            execution_machine,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: Path::new("/tmp").to_path_buf(),
                codex: Path::new("/bin/echo").to_path_buf().into(),
            },
            spec,
        })
    }

    fn after() -> TaskDependencies {
        TaskDependencies::new(vec![TaskId::new()]).unwrap()
    }

    fn terminal_callback(store: &Store, task: TaskId) -> crate::callback::HomebasedEvent {
        let events = store.inbound_events(task).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0].event.payload {
            EventPayload::Callback { event, state } => {
                assert!(state.is_some_and(crate::domain::ProcessStatus::is_terminal));
                (**event).clone()
            }
            other => panic!("expected a terminal callback, got {other:?}"),
        }
    }

    #[test]
    fn a_held_route_needs_dependencies_and_a_retry_must_repeat_them() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = held_route(MachineId::new());
        assert!(store.insert_origin_route(&route).is_err());

        let after = after();
        store
            .insert_origin_route_after(&route, Some(&after))
            .unwrap();
        let mut retry = route.clone();
        retry.task = TaskId::new();
        assert_eq!(
            store
                .insert_origin_route_after(&retry, Some(&after))
                .unwrap()
                .task,
            route.task
        );
        assert!(
            store
                .insert_origin_route_after(&retry, Some(&self::after()))
                .is_err()
        );
        assert_eq!(store.route_dependencies(route.task).unwrap(), Some(after));
        assert_eq!(
            store.dependency_states(&[route.task]).unwrap(),
            vec![(route.task, Some(DependencyState::Pending))]
        );
    }

    #[test]
    fn cancelling_a_held_route_is_idempotent_and_ends_it_as_cancelled() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = held_route(MachineId::new());
        store
            .insert_origin_route_after(&route, Some(&after()))
            .unwrap();

        let cause = HeldCancellation::Requested;
        assert!(matches!(
            store.cancel_held_route(route.task, cause).unwrap(),
            HeldCancel::Cancelled(_)
        ));
        assert!(matches!(
            store.cancel_held_route(route.task, cause).unwrap(),
            HeldCancel::Cancelled(_)
        ));
        let event = terminal_callback(&store, route.task);
        assert_eq!(event.event, crate::callback::EventKind::TaskCancelled);
        assert_eq!(event.cancel_reason, Some(cause));
        assert_eq!(
            store.dependency_states(&[route.task]).unwrap(),
            vec![(
                route.task,
                Some(DependencyState::Ended(TaskOutcome::Cancelled.into()))
            )]
        );
        // a cancelled route never launches
        let saved = store.begin_held_launch(route.task).unwrap();
        assert!(matches!(
            saved.submission,
            SubmissionState::Held {
                phase: HeldPhase::Cancelled { .. }
            }
        ));
    }

    #[test]
    fn a_refused_remote_launch_tells_the_thread_and_fails_dependents() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = held_route(MachineId::new());
        store
            .insert_origin_route_after(&route, Some(&after()))
            .unwrap();
        let launching = store.begin_held_launch(route.task).unwrap();
        assert!(matches!(
            launching.submission,
            SubmissionState::Held {
                phase: HeldPhase::Launching
            }
        ));
        // a launching route may be on its executor, so it cannot be cancelled locally
        assert!(matches!(
            store
                .cancel_held_route(route.task, HeldCancellation::Requested)
                .unwrap(),
            HeldCancel::NotHeld(_)
        ));

        let rejected = store
            .resolve_origin_route(
                route.task,
                SubmissionState::Rejected {
                    reason: "cwd_not_found".into(),
                },
            )
            .unwrap();
        assert_eq!(rejected.last_accepted_seq, 1);
        let event = terminal_callback(&store, route.task);
        assert_eq!(event.event, crate::callback::EventKind::TaskFailed);
        assert_eq!(
            event.process,
            Some(crate::callback::ProcessPayload::SpawnFailed {
                message: "cwd_not_found".into()
            })
        );
        assert_eq!(
            store.dependency_states(&[route.task]).unwrap(),
            vec![(
                route.task,
                Some(DependencyState::Ended(TaskOutcome::Failed.into()))
            )]
        );
    }
}
