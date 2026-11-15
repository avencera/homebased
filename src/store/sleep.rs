//! Sleep holds: targets whose progress events wait for their final event
//!
//! A hold is written when `homebased wait --long` registers a target, and each
//! settlement that concerns it changes it in the same transaction: a held
//! progress event joins its list, a parked run moves it to the continuation,
//! and the final event deletes it. A restart therefore neither loses a held
//! event nor leaves a hold on work that ended
//!
//! A held chain's run that parks also holds the live tasks and jobs it parks
//! on, as [`HoldRole::Parked`] under the chain's current run. A worker's
//! submissions report to the worker's own thread, which is the sleeping one,
//! and the continuation reports on them, so their final events are progress
//! of the chain and join its list

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use super::identity::{JOB_WAIT_LIMIT, OwedTarget};
use super::{Store, fmt_time, parse_time};
use crate::callback::{EventKind, HomebasedEvent};
use crate::child_thread::OpenWork;
use crate::dependency::Dependency;
use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::events::{DeliveryState, EventPayload, TaskEvent};
use crate::queue::JobId;
use crate::queue::delivery::RoutedJobEvent;
use crate::task_wait::{EventClass, HeldEvent, HoldRole, HoldTarget, SleepHold, SleepOutcome};
use crate::waiting::WaitTargets;

/// Where a task chain stands, from the settled events of its runs
enum ChainEnd {
    /// The final event of the chain's last run settled
    Ended,
    /// `run` still runs, or its next events have not settled
    Live {
        /// The run that carries on the chain's work now
        run: TaskId,
        /// What the last settled park before `run` waits on, when the walk
        /// followed one
        parked_on: Option<WaitTargets>,
    },
}

/// Holds a job counts for no longer than the waiting list counts the job, so
/// a job that never sends its final event stops marking its thread asleep;
/// `?1` is the oldest job route time that still counts
const LIVE_HOLD: &str = "(h.target_kind = 'task' OR EXISTS (
    SELECT 1 FROM resource_job_routes r
    WHERE r.job_id = h.target_id AND r.created_at >= ?1))";

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
            let ChainEnd::Live {
                run: current,
                parked_on,
            } = follow_chain(&self.conn, task)?
            else {
                return Ok(SleepOutcome::AlreadyEnded);
            };
            let target = HoldTarget::Task(current);
            insert_hold(&self.conn, target, thread, HoldRole::Sleep, now)?;
            // a hold registered under another chain's sleep now wakes on its own
            make_sleep(&self.conn, target)?;
            if let Some(targets) = parked_on {
                hold_parked_targets(&self.conn, current, &targets, thread, now)?;
            }
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
            insert_hold(
                &self.conn,
                HoldTarget::Job(job),
                thread,
                HoldRole::Sleep,
                now,
            )?;
            make_sleep(&self.conn, HoldTarget::Job(job))?;
            Ok(SleepOutcome::Held)
        })
    }

    /// The hold on `target`, if a sleep holds it
    pub fn sleep_hold(&self, target: HoldTarget) -> Result<Option<SleepHold>, AppError> {
        read_hold(&self.conn, target)
    }

    /// Threads with at least one hold, which a final event or alert will wake
    ///
    /// A job hold counts only as long as the waiting list counts its job
    pub fn sleeping_threads(&self) -> Result<Vec<ThreadId>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "SELECT DISTINCT h.thread_id FROM sleep_holds h WHERE {LIVE_HOLD} ORDER BY h.thread_id"
        ))?;
        let rows = statement
            .query_map([job_cutoff()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.iter().map(|thread| thread.parse()).collect()
    }

    /// Whether `thread` has at least one hold, by the rule of
    /// [`Self::sleeping_threads`]
    pub fn thread_sleeps(&self, thread: ThreadId) -> Result<bool, AppError> {
        Ok(self
            .conn
            .query_row(
                &format!(
                    "SELECT 1 FROM sleep_holds h WHERE h.thread_id=?2 AND {LIVE_HOLD} LIMIT 1"
                ),
                params![job_cutoff(), thread.to_string()],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    /// Each hold as open work of its thread, by the rule of
    /// [`Self::sleeping_threads`]
    pub fn hold_targets(&self) -> Result<Vec<OwedTarget>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "SELECT h.thread_id, h.target_id FROM sleep_holds h WHERE {LIVE_HOLD}
             ORDER BY h.thread_id, h.target_kind DESC, h.target_id"
        ))?;
        let rows = statement
            .query_map([job_cutoff()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.iter()
            .map(|(thread, target)| {
                Ok(OwedTarget {
                    thread: thread.parse()?,
                    work: OpenWork::Hold {
                        id: target.parse().map_err(|_| AppError::Internal {
                            message: format!("sleep hold target {target} is not a UUID"),
                        })?,
                    },
                })
            })
            .collect()
    }
}

/// Settle the hold of task `task` for its callback event `seq`, whose
/// delivery just settled as `delivery`
///
/// A held event joins the hold's list; a park moves the hold to the
/// continuation and holds what the run parks on, and the chain's final event
/// ends it
pub(super) fn settle_task_hold(
    conn: &Connection,
    task: TaskId,
    seq: u64,
    event: &HomebasedEvent,
    delivery: &DeliveryState,
    now: DateTime<Utc>,
) -> Result<(), AppError> {
    let held = matches!(delivery, DeliveryState::Held { .. });
    let Some(hold) = read_hold(conn, HoldTarget::Task(task))? else {
        if held {
            return Err(AppError::Internal {
                message: format!("task {task} event {seq} was held with no sleep hold"),
            });
        }
        return Ok(());
    };
    let class = EventClass::task(event.event);
    if held {
        if !hold.holds(class) {
            return Err(AppError::Internal {
                message: format!("task {task} event {seq} is not an event its hold keeps"),
            });
        }
        record_held(conn, &hold, &HeldEvent::task(event, seq, now))?;
    }
    if class.is_final() {
        return end_hold(conn, hold.target);
    }
    if event.event == EventKind::TaskWaiting
        && let Some(continuation) = event.continuation
    {
        return move_task_hold(conn, task, continuation, event.waiting_on.as_ref(), now);
    }
    Ok(())
}

/// The hold on `target`, if there is one
pub(crate) fn read_hold(
    conn: &Connection,
    target: HoldTarget,
) -> Result<Option<SleepHold>, AppError> {
    let row = conn
        .query_row(
            "SELECT thread_id,parked_under,created_at,held_json FROM sleep_holds
             WHERE target_kind=?1 AND target_id=?2",
            params![target.kind(), target.id().to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((thread, parked_under, created_at, held)) = row else {
        return Ok(None);
    };
    let role = match parked_under {
        None => HoldRole::Sleep,
        Some(chain) => HoldRole::Parked {
            chain: chain.parse()?,
        },
    };
    Ok(Some(SleepHold {
        target,
        thread: thread.parse()?,
        role,
        created_at: parse_time(&created_at)?,
        held: serde_json::from_str(&held)?,
    }))
}

/// Oldest job route time whose job still counts as owed, as the waiting
/// list's rule has it
fn job_cutoff() -> String {
    fmt_time(Utc::now() - JOB_WAIT_LIMIT)
}

/// Follow the settled parks of the chain from `task` to the run that carries
/// on its work now
fn follow_chain(conn: &Connection, task: TaskId) -> Result<ChainEnd, AppError> {
    let mut current = task;
    let mut parked_on = None;
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
                return Ok(ChainEnd::Live {
                    run: current,
                    parked_on,
                });
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
                parked_on = event.waiting_on;
                continue 'runs;
            }
        }
        return Ok(ChainEnd::Live {
            run: current,
            parked_on,
        });
    }
}

/// Hold `target` for `thread` in `role`, keeping a hold already on it
fn insert_hold(
    conn: &Connection,
    target: HoldTarget,
    thread: ThreadId,
    role: HoldRole,
    now: DateTime<Utc>,
) -> Result<(), AppError> {
    conn.execute(
        "INSERT INTO sleep_holds (target_kind,target_id,thread_id,parked_under,created_at)
         VALUES (?1,?2,?3,?4,?5)
         ON CONFLICT (target_kind,target_id) DO NOTHING",
        params![
            target.kind(),
            target.id().to_string(),
            thread.to_string(),
            parked_under(role),
            fmt_time(now)
        ],
    )?;
    Ok(())
}

fn parked_under(role: HoldRole) -> Option<String> {
    match role {
        HoldRole::Sleep => None,
        HoldRole::Parked { chain } => Some(chain.to_string()),
    }
}

/// Make the hold on `target` a sleep's own, for a sleep that names a target
/// another chain's park already holds
fn make_sleep(conn: &Connection, target: HoldTarget) -> Result<(), AppError> {
    conn.execute(
        "UPDATE sleep_holds SET parked_under=NULL WHERE target_kind=?1 AND target_id=?2",
        params![target.kind(), target.id().to_string()],
    )?;
    Ok(())
}

/// Hold each live target that run `parked` of a chain held under `chain`
/// waits on, as [`HoldRole::Parked`] under `chain`
///
/// Only a target whose events go to `thread` is held: the hold is read by
/// target alone, so one on another thread's work would keep that thread's
/// events. A target that already has a hold keeps it, and a task target that
/// parked itself is held from its current run, with what that run parks on
fn hold_parked_targets(
    conn: &Connection,
    chain: TaskId,
    targets: &WaitTargets,
    thread: ThreadId,
    now: DateTime<Utc>,
) -> Result<(), AppError> {
    let role = HoldRole::Parked { chain };
    let mut pending: Vec<Dependency> = targets.targets().to_vec();
    let mut seen: Vec<Dependency> = Vec::new();
    while let Some(target) = pending.pop() {
        if seen.contains(&target) {
            continue;
        }
        seen.push(target);
        match target {
            Dependency::Task(task) => {
                if task_thread(conn, task)? != Some(thread) {
                    continue;
                }
                let ChainEnd::Live { run, parked_on } = follow_chain(conn, task)? else {
                    continue;
                };
                insert_hold(conn, HoldTarget::Task(run), thread, role, now)?;
                if let Some(more) = parked_on {
                    pending.extend(more.targets().iter().copied());
                }
            }
            Dependency::Job(job) => {
                if job_thread(conn, job)? != Some(thread) || job_ended(conn, job)? {
                    continue;
                }
                insert_hold(conn, HoldTarget::Job(job), thread, role, now)?;
            }
        }
    }
    Ok(())
}

/// Thread the origin route of `task` here sends its events to
fn task_thread(conn: &Connection, task: TaskId) -> Result<Option<ThreadId>, AppError> {
    let thread = conn
        .query_row(
            "SELECT json_extract(route_json, '$.thread') FROM origin_routes WHERE task_id=?1",
            [task.to_string()],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    thread.map(|thread| thread.parse()).transpose()
}

/// Thread the origin route of `job` here sends its events to
fn job_thread(conn: &Connection, job: JobId) -> Result<Option<ThreadId>, AppError> {
    let thread = conn
        .query_row(
            "SELECT json_extract(route_json, '$.thread') FROM resource_job_routes WHERE job_id=?1",
            [job.to_string()],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    thread.map(|thread| thread.parse()).transpose()
}

/// Whether a final event of `job` already settled
fn job_ended(conn: &Connection, job: JobId) -> Result<bool, AppError> {
    let mut statement = conn.prepare(
        "SELECT i.event_json FROM resource_job_inbox i
         JOIN resource_job_routes r ON r.job_id=i.job_id
         WHERE i.job_id=?1 AND i.seq<=r.settled_seq ORDER BY i.seq",
    )?;
    let rows = statement
        .query_map([job.to_string()], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for json in rows {
        let event = serde_json::from_str::<RoutedJobEvent>(&json)?.event;
        if EventClass::job(event.event, event.gpu_idle.as_ref()).is_final() {
            return Ok(true);
        }
    }
    Ok(false)
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

/// Add one held event to the list of `hold`: its own for a sleep's hold,
/// its chain's for a parked target
///
/// A parked target whose chain's hold already ended adds nothing: the chain's
/// final event woke the thread, and the continuation reported on the target
pub(crate) fn record_held(
    conn: &Connection,
    hold: &SleepHold,
    event: &HeldEvent,
) -> Result<(), AppError> {
    let owner = match hold.role {
        HoldRole::Sleep => hold.target,
        HoldRole::Parked { chain } => HoldTarget::Task(chain),
    };
    let Some(mut held) = held_events(conn, owner)? else {
        if hold.role == HoldRole::Sleep {
            return Err(AppError::Internal {
                message: format!("no sleep hold on {} {}", owner.kind(), owner.id()),
            });
        }
        return Ok(());
    };
    let repeat = held
        .iter()
        .any(|entry| entry.task == event.task && entry.job == event.job && entry.seq == event.seq);
    if !repeat {
        held.push(event.clone());
    }
    write_held(conn, owner, &held)
}

/// End the hold on `target`, if there is one
pub(super) fn end_hold(conn: &Connection, target: HoldTarget) -> Result<(), AppError> {
    conn.execute(
        "DELETE FROM sleep_holds WHERE target_kind=?1 AND target_id=?2",
        params![target.kind(), target.id().to_string()],
    )?;
    Ok(())
}

/// Move the hold on `from`, a run that parked, to the run that carries on
/// from its continuation `to`, or end it when that chain already ended
///
/// The moved hold keeps its role. The live targets of the park, `waiting_on`
/// unless the continuation parked again, are held under the chain, and
/// targets parked under the run that parked now count under its successor
fn move_task_hold(
    conn: &Connection,
    from: TaskId,
    to: TaskId,
    waiting_on: Option<&WaitTargets>,
    now: DateTime<Utc>,
) -> Result<(), AppError> {
    // read again: the park's own held event joined the list just now
    let Some(hold) = read_hold(conn, HoldTarget::Task(from))? else {
        return Ok(());
    };
    let ChainEnd::Live {
        run: current,
        parked_on,
    } = follow_chain(conn, to)?
    else {
        return end_hold(conn, hold.target);
    };
    let target = HoldTarget::Task(current);
    // the continuation may have a hold of its own when a sleep named it
    // directly, or another chain parked on it; a sleep's own hold wins
    let (role, held) = match read_hold(conn, target)? {
        Some(existing) => {
            let role = if existing.role == HoldRole::Sleep {
                HoldRole::Sleep
            } else {
                hold.role
            };
            (role, [hold.held.clone(), existing.held].concat())
        }
        None => (hold.role, hold.held.clone()),
    };
    end_hold(conn, hold.target)?;
    end_hold(conn, target)?;
    insert_hold(conn, target, hold.thread, role, hold.created_at)?;
    write_held(conn, target, &held)?;

    let chain = match role {
        HoldRole::Sleep => current,
        HoldRole::Parked { chain } => chain,
    };
    conn.execute(
        "UPDATE sleep_holds SET parked_under=?2 WHERE parked_under=?1",
        params![from.to_string(), chain.to_string()],
    )?;
    if let Some(targets) = parked_on.as_ref().or(waiting_on) {
        hold_parked_targets(conn, chain, targets, hold.thread, now)?;
    }
    Ok(())
}
