//! Thread watches and the events each owes its caller
//!
//! A check that fires, re-arms, or ends a watch commits its events and the
//! watch's new state together, so a restart neither repeats nor loses an event.
//! Delivery then settles events in sequence, like a job's origin inbox

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Row, params};
use serde_json::Value;

use super::{Store, fmt_time, parse_time};
use crate::domain::ThreadId;
use crate::error::AppError;
use crate::thread_watch::{
    ThreadWatch, WatchEndReason, WatchEvent, WatchEventKind, WatchId, WatchView,
};

/// One watch with its live state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchRecord {
    /// The stored watch
    pub watch: ThreadWatch,
    /// Whether the next climb over the limit fires
    pub armed: bool,
    /// Last event sequence
    pub seq: u64,
    /// When it ended
    pub ended_at: Option<DateTime<Utc>>,
    /// Why it ended
    pub end_reason: Option<WatchEndReason>,
}

impl WatchRecord {
    /// The record as the watch routes show it
    #[must_use]
    pub fn view(&self, created: bool) -> WatchView {
        WatchView {
            watch: self.watch.id,
            caller: self.watch.caller,
            target: self.watch.target.clone(),
            reference: self.watch.reference.clone(),
            title: self.watch.title.clone(),
            threshold: self.watch.threshold,
            action: self.watch.action,
            once: self.watch.once,
            armed: self.armed,
            seq: self.seq,
            created_at: self.watch.created_at,
            ended_at: self.ended_at,
            end_reason: self.end_reason,
            created,
        }
    }
}

/// What one check of a watch decided
#[derive(Debug, Clone, PartialEq)]
pub struct WatchUpdate {
    /// Whether the watch is armed afterwards
    pub armed: bool,
    /// Events to append in order; each payload gets its `seq`
    pub events: Vec<(WatchEventKind, Value)>,
    /// End the watch for this reason
    pub end: Option<WatchEndReason>,
}

const WATCH_COLUMNS: &str = "watch_json, armed, event_seq, ended_at, end_reason";

/// The earliest unsettled event of one watch, with when it was recorded
pub type PendingWatchEvent = (ThreadWatch, WatchEvent, DateTime<Utc>);

impl Store {
    /// Save a watch, or find the live watch the same caller has with the same
    /// target, limit, and action, and return it with whether it is new
    ///
    /// An existing watch keeps its id, sequence, and armed state, and takes the
    /// new `once` and callback environment
    pub fn upsert_watch(&self, watch: &ThreadWatch) -> Result<(WatchRecord, bool), AppError> {
        self.immediate(|| {
            let key = watch.key();
            let existing: Option<String> = self
                .conn
                .query_row(
                    "SELECT id FROM thread_watches
                     WHERE caller_thread = ?1 AND watch_key = ?2 AND ended_at IS NULL",
                    params![watch.caller.to_string(), key],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(id) = existing {
                let id: WatchId = id.parse()?;
                let mut record = self
                    .watch_record(id)?
                    .ok_or(AppError::WatchNotFound { watch: id })?;
                record.watch.once = watch.once;
                record.watch.callback = watch.callback.clone();
                self.conn.execute(
                    "UPDATE thread_watches SET watch_json = ?2 WHERE id = ?1",
                    params![id.to_string(), serde_json::to_string(&record.watch)?],
                )?;
                return Ok((record, false));
            }
            self.conn.execute(
                "INSERT INTO thread_watches
                 (id, caller_thread, watch_key, watch_json, armed, event_seq, created_at)
                 VALUES (?1, ?2, ?3, ?4, 1, 0, ?5)",
                params![
                    watch.id.to_string(),
                    watch.caller.to_string(),
                    key,
                    serde_json::to_string(watch)?,
                    fmt_time(watch.created_at)
                ],
            )?;
            Ok((
                WatchRecord {
                    watch: watch.clone(),
                    armed: true,
                    seq: 0,
                    ended_at: None,
                    end_reason: None,
                },
                true,
            ))
        })
    }

    /// One watch, live or ended
    pub fn watch_record(&self, id: WatchId) -> Result<Option<WatchRecord>, AppError> {
        self.conn
            .query_row(
                &format!("SELECT {WATCH_COLUMNS} FROM thread_watches WHERE id = ?1"),
                [id.to_string()],
                read_record,
            )
            .optional()?
            .transpose()
    }

    /// Watches oldest first: the live ones, or every one with `ended`, of
    /// `caller` or of every caller
    pub fn watch_records(
        &self,
        caller: Option<ThreadId>,
        ended: bool,
    ) -> Result<Vec<WatchRecord>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "SELECT {WATCH_COLUMNS} FROM thread_watches
             WHERE (?1 IS NULL OR caller_thread = ?1) AND (?2 OR ended_at IS NULL)
             ORDER BY created_at, id"
        ))?;
        let rows = statement
            .query_map(
                params![caller.map(|caller| caller.to_string()), ended],
                read_record,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter().collect()
    }

    /// Apply one check of a live watch, and return the events it added
    ///
    /// A watch that ended meanwhile, for example by `unwatch`, changes no more
    pub fn update_watch(
        &self,
        id: WatchId,
        update: WatchUpdate,
        now: DateTime<Utc>,
    ) -> Result<Vec<WatchEvent>, AppError> {
        self.immediate(|| {
            let Some(record) = self.watch_record(id)? else {
                return Err(AppError::WatchNotFound { watch: id });
            };
            if record.ended_at.is_some() {
                return Ok(Vec::new());
            }
            let mut seq = record.seq;
            let mut added = Vec::new();
            for (kind, mut payload) in update.events {
                seq += 1;
                if let Some(object) = payload.as_object_mut() {
                    object.insert("seq".into(), seq.into());
                }
                let event = WatchEvent {
                    watch: id,
                    seq,
                    kind,
                    payload,
                };
                self.conn.execute(
                    "INSERT INTO thread_watch_events (watch_id, seq, event_json, created_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        id.to_string(),
                        to_sql_seq(seq)?,
                        serde_json::to_string(&event)?,
                        fmt_time(now)
                    ],
                )?;
                added.push(event);
            }
            // stored as the bare wire name, `once` rather than `"once"`
            let end_reason = update
                .end
                .map(serde_json::to_value)
                .transpose()?
                .and_then(|value| value.as_str().map(str::to_owned));
            self.conn.execute(
                "UPDATE thread_watches
                 SET armed = ?2, event_seq = ?3,
                     ended_at = CASE WHEN ?4 IS NULL THEN NULL ELSE ?5 END, end_reason = ?4
                 WHERE id = ?1",
                params![
                    id.to_string(),
                    update.armed,
                    to_sql_seq(seq)?,
                    end_reason,
                    fmt_time(now)
                ],
            )?;
            Ok(added)
        })
    }

    /// The earliest unsettled event of each watch, with its watch
    pub fn pending_watch_events(&self) -> Result<Vec<PendingWatchEvent>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT w.watch_json, e.event_json, e.created_at FROM thread_watch_events e
             JOIN thread_watches w ON w.id = e.watch_id
             WHERE e.settled_at IS NULL
               AND e.seq = (SELECT MIN(seq) FROM thread_watch_events
                            WHERE watch_id = e.watch_id AND settled_at IS NULL)
             ORDER BY e.created_at, e.watch_id",
        )?;
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
            .map(|(watch, event, created)| {
                Ok((
                    serde_json::from_str(&watch)?,
                    serde_json::from_str(&event)?,
                    parse_time(&created)?,
                ))
            })
            .collect()
    }

    /// Settle one event as delivered, printed, or failed, as `delivery` says
    pub fn settle_watch_event(
        &self,
        id: WatchId,
        seq: u64,
        delivery: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE thread_watch_events SET settled_at = ?3, delivery = ?4
             WHERE watch_id = ?1 AND seq = ?2 AND settled_at IS NULL",
            params![id.to_string(), to_sql_seq(seq)?, fmt_time(now), delivery],
        )?;
        Ok(())
    }

    /// Settled events of one watch in sequence, for a waiter catching up
    pub fn settled_watch_events(&self, id: WatchId) -> Result<Vec<WatchEvent>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT event_json FROM thread_watch_events
             WHERE watch_id = ?1 AND settled_at IS NOT NULL ORDER BY seq",
        )?;
        let rows = statement
            .query_map([id.to_string()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.iter()
            .map(|json| serde_json::from_str(json).map_err(AppError::from))
            .collect()
    }
}

/// The agent session of one local task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerThread {
    /// Claude Code session or Codex thread of the worker
    pub thread: ThreadId,
    /// Its task
    pub task: crate::domain::TaskId,
    /// Whether the task is queued or running
    pub running: bool,
    /// Agent CLI of the worker, which only Claude and Codex workers record
    pub provider: crate::thread_ref::Provider,
}

impl Store {
    /// Worker sessions of tasks on this machine
    ///
    /// Codex records its thread only at exit, but a follow-up resumes the
    /// thread it names, so a running follow-up is that thread's worker from
    /// the start. A Claude follow-up forks into its own session instead,
    /// which it records at launch
    pub fn worker_threads(&self) -> Result<Vec<WorkerThread>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT thread, id, running, claude FROM (
                 SELECT COALESCE(worker_thread,
                            CASE WHEN json_extract(workload_json, '$.type') = 'agent'
                                  AND json_extract(workload_json, '$.agent') = 'codex'
                                THEN json_extract(workload_json, '$.resume_thread') END) AS thread,
                        id, status IN ('queued', 'running') AS running,
                        COALESCE(json_extract(workload_json, '$.agent') = 'claude', 0) AS claude
                 FROM tasks)
             WHERE thread IS NOT NULL",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, bool>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        // a row whose ids do not parse names no session that can be looked up
        Ok(rows
            .into_iter()
            .filter_map(|(thread, task, running, claude)| {
                Some(WorkerThread {
                    thread: thread.parse().ok()?,
                    task: task.parse().ok()?,
                    running,
                    provider: if claude {
                        crate::thread_ref::Provider::Claude
                    } else {
                        crate::thread_ref::Provider::Codex
                    },
                })
            })
            .collect())
    }
}

fn read_record(row: &Row<'_>) -> rusqlite::Result<Result<WatchRecord, AppError>> {
    let json: String = row.get(0)?;
    let armed: bool = row.get(1)?;
    let seq: i64 = row.get(2)?;
    let ended_at: Option<String> = row.get(3)?;
    let end_reason: Option<String> = row.get(4)?;
    Ok((|| {
        Ok(WatchRecord {
            watch: serde_json::from_str(&json)?,
            armed,
            seq: u64::try_from(seq).map_err(|_| AppError::Internal {
                message: format!("watch sequence {seq} is negative"),
            })?,
            ended_at: ended_at.as_deref().map(parse_time).transpose()?,
            end_reason: end_reason
                .map(|reason| serde_json::from_value(Value::String(reason)))
                .transpose()?,
        })
    })())
}

fn to_sql_seq(seq: u64) -> Result<i64, AppError> {
    i64::try_from(seq).map_err(|_| AppError::Internal {
        message: format!("watch sequence {seq} is too large"),
    })
}
