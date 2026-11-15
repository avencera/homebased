//! Sleep holds: targets whose progress events wait for their final event
//!
//! A hold is written when `homebased wait --long` registers a target, and each
//! settlement that concerns it changes it in the same transaction: a held
//! progress event joins its list, a parked run moves it to the continuation,
//! and the final event deletes it. A restart therefore neither loses a held
//! event nor leaves a hold on work that ended

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use super::{Store, fmt_time, parse_time};
use crate::callback::{EventKind, HomebasedEvent};
use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::events::{DeliveryState, EventPayload, TaskEvent};
use crate::queue::JobId;
use crate::task_wait::{EventClass, HeldEvent, HoldTarget, SleepHold, SleepOutcome};

/// Where a task chain stands, from the settled events of its runs
enum ChainEnd {
    /// The final event of the chain's last run settled
    Ended,
    /// This run still runs, or its next events have not settled
    Live(TaskId),
}

impl Store {
    /// Hold task chain `task` for `thread`, from the run that carries on its
    /// work now
    ///
    /// A hold already on that run is kept with its events
    pub fn hold_task(
        &self,
        task: TaskId,
        thread: ThreadId,
        now: DateTime<Utc>,
    ) -> Result<SleepOutcome, AppError> {
        self.immediate(|| {
            let ChainEnd::Live(current) = follow_chain(&self.conn, task)? else {
                return Ok(SleepOutcome::AlreadyEnded);
            };
            insert_hold(&self.conn, HoldTarget::Task(current), thread, now)?;
            Ok(if current == task {
                SleepOutcome::Held
            } else {
                SleepOutcome::FollowedTo(current)
            })
        })
    }

    /// Hold job `job` for `thread` unless its final event already settled
    ///
    /// A hold already on the job is kept with its events
    pub fn hold_job(
        &self,
        job: JobId,
        thread: ThreadId,
        now: DateTime<Utc>,
    ) -> Result<SleepOutcome, AppError> {
        self.immediate(|| {
            let ended = self
                .settled_job_events(job)?
                .iter()
                .any(|event| EventClass::job(event.event, event.gpu_idle.as_ref()).is_final());
            if ended {
                return Ok(SleepOutcome::AlreadyEnded);
            }
            insert_hold(&self.conn, HoldTarget::Job(job), thread, now)?;
            Ok(SleepOutcome::Held)
        })
    }

    /// The hold on `target`, if a sleep holds it
    pub fn sleep_hold(&self, target: HoldTarget) -> Result<Option<SleepHold>, AppError> {
        let row = self
            .conn
            .query_row(
                "SELECT thread_id,created_at,held_json FROM sleep_holds
                 WHERE target_kind=?1 AND target_id=?2",
                params![target.kind(), target.id().to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((thread, created_at, held)) = row else {
            return Ok(None);
        };
        Ok(Some(SleepHold {
            target,
            thread: thread.parse()?,
            created_at: parse_time(&created_at)?,
            held: serde_json::from_str(&held)?,
        }))
    }

    /// Threads with at least one hold, which a final event or alert will wake
    pub fn sleeping_threads(&self) -> Result<Vec<ThreadId>, AppError> {
        let mut statement = self
            .conn
            .prepare("SELECT DISTINCT thread_id FROM sleep_holds ORDER BY thread_id")?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.iter().map(|thread| thread.parse()).collect()
    }

    /// Whether `thread` has at least one hold
    pub fn thread_sleeps(&self, thread: ThreadId) -> Result<bool, AppError> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1 FROM sleep_holds WHERE thread_id=?1 LIMIT 1",
                [thread.to_string()],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }
}

/// Settle the hold of task `task` for its callback event `seq`, whose
/// delivery just settled as `delivery`
///
/// A held event joins the hold; a park moves the hold to the continuation,
/// and the chain's final event ends it
pub(super) fn settle_task_hold(
    conn: &Connection,
    task: TaskId,
    seq: u64,
    event: &HomebasedEvent,
    delivery: &DeliveryState,
    now: DateTime<Utc>,
) -> Result<(), AppError> {
    let target = HoldTarget::Task(task);
    if matches!(delivery, DeliveryState::Held { .. }) {
        if EventClass::task(event.event) != EventClass::Progress {
            return Err(AppError::Internal {
                message: format!("task {task} event {seq} is not a progress event to hold"),
            });
        }
        append_held(conn, target, &HeldEvent::task(event, seq, now))?;
    }
    if EventClass::task(event.event).is_final() {
        return end_hold(conn, target);
    }
    if event.event == EventKind::TaskWaiting
        && let Some(continuation) = event.continuation
    {
        return move_task_hold(conn, task, continuation);
    }
    Ok(())
}

/// Follow the settled parks of the chain from `task` to the run that carries
/// on its work now
fn follow_chain(conn: &Connection, task: TaskId) -> Result<ChainEnd, AppError> {
    let mut current = task;
    let mut seen = HashSet::from([task]);
    'runs: loop {
        let mut statement = conn.prepare(
            "SELECT event_json,delivery_json FROM origin_inbox WHERE task_id=?1 ORDER BY seq",
        )?;
        let rows = statement
            .query_map([current.to_string()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (event_json, delivery_json) in rows {
            let delivery: DeliveryState = serde_json::from_str(&delivery_json)?;
            // events past an unsettled one have not reached the thread yet
            if delivery.is_unsettled() {
                return Ok(ChainEnd::Live(current));
            }
            let event: TaskEvent = serde_json::from_str(&event_json)?;
            let EventPayload::Callback { event, .. } = event.payload else {
                continue;
            };
            if EventClass::task(event.event).is_final() {
                return Ok(ChainEnd::Ended);
            }
            if event.event == EventKind::TaskWaiting
                && let Some(continuation) = event.continuation
            {
                if !seen.insert(continuation) {
                    return Err(AppError::Internal {
                        message: format!("task chain of {task} parks in a cycle"),
                    });
                }
                current = continuation;
                continue 'runs;
            }
        }
        return Ok(ChainEnd::Live(current));
    }
}

fn insert_hold(
    conn: &Connection,
    target: HoldTarget,
    thread: ThreadId,
    now: DateTime<Utc>,
) -> Result<(), AppError> {
    conn.execute(
        "INSERT INTO sleep_holds (target_kind,target_id,thread_id,created_at)
         VALUES (?1,?2,?3,?4)
         ON CONFLICT (target_kind,target_id) DO NOTHING",
        params![
            target.kind(),
            target.id().to_string(),
            thread.to_string(),
            fmt_time(now)
        ],
    )?;
    Ok(())
}

fn held_events(conn: &Connection, target: HoldTarget) -> Result<Option<Vec<HeldEvent>>, AppError> {
    let held = conn
        .query_row(
            "SELECT held_json FROM sleep_holds WHERE target_kind=?1 AND target_id=?2",
            params![target.kind(), target.id().to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    held.map(|held| serde_json::from_str(&held).map_err(AppError::from))
        .transpose()
}

fn write_held(conn: &Connection, target: HoldTarget, held: &[HeldEvent]) -> Result<(), AppError> {
    conn.execute(
        "UPDATE sleep_holds SET held_json=?3 WHERE target_kind=?1 AND target_id=?2",
        params![
            target.kind(),
            target.id().to_string(),
            serde_json::to_string(held)?
        ],
    )?;
    Ok(())
}

/// Add one held event to the hold on `target`, which must exist
pub(super) fn append_held(
    conn: &Connection,
    target: HoldTarget,
    event: &HeldEvent,
) -> Result<(), AppError> {
    let Some(mut held) = held_events(conn, target)? else {
        return Err(AppError::Internal {
            message: format!("no sleep hold on {} {}", target.kind(), target.id()),
        });
    };
    let repeat = held
        .iter()
        .any(|entry| entry.task == event.task && entry.seq == event.seq);
    if !repeat {
        held.push(event.clone());
    }
    write_held(conn, target, &held)
}

/// End the hold on `target`, if there is one
pub(super) fn end_hold(conn: &Connection, target: HoldTarget) -> Result<(), AppError> {
    conn.execute(
        "DELETE FROM sleep_holds WHERE target_kind=?1 AND target_id=?2",
        params![target.kind(), target.id().to_string()],
    )?;
    Ok(())
}

/// Move the hold on parked run `from` to the run that carries on from its
/// continuation `to`, or end it when that chain already ended
fn move_task_hold(conn: &Connection, from: TaskId, to: TaskId) -> Result<(), AppError> {
    let source = HoldTarget::Task(from);
    let Some(mut held) = held_events(conn, source)? else {
        return Ok(());
    };
    let ChainEnd::Live(current) = follow_chain(conn, to)? else {
        return end_hold(conn, source);
    };
    let target = HoldTarget::Task(current);
    // the continuation may hold events of its own when a sleep named it directly
    if let Some(existing) = held_events(conn, target)? {
        held.extend(existing);
        write_held(conn, target, &held)?;
        return end_hold(conn, source);
    }
    conn.execute(
        "UPDATE sleep_holds SET target_id=?2, held_json=?3
         WHERE target_kind='task' AND target_id=?1",
        params![
            from.to_string(),
            current.to_string(),
            serde_json::to_string(&held)?
        ],
    )?;
    Ok(())
}
