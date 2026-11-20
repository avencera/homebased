//! Database facts a retention pass prunes by, and the tombstones it leaves
//!
//! The pass reads every fact in one read transaction, so a task cannot look
//! ended in one query and running in another

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use uuid::Uuid;

use super::{Store, fmt_time, parse_time};
use crate::domain::ThreadId;
use crate::error::AppError;
use crate::queue::delivery::{JobRoute, JobSubmission};
use crate::retention::{LiveReason, OutputKind, PruneReason};

/// Where a task or job stands for retention
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Standing {
    /// It may still be read or written
    Live(LiveReason),
    /// It ended at this time
    Ended(DateTime<Utc>),
}

/// A task or job row on this machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordFact {
    /// Thread its events go to
    pub(crate) thread: ThreadId,
    /// Whether it may be pruned
    pub(crate) standing: Standing,
}

/// A CI slot lane on this machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaneFact {
    /// Whether it holds a run now
    pub(crate) busy: bool,
}

/// The runs a slot clone saw, from the task rows that ran in it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CloneUse {
    /// When its latest run last changed
    pub(crate) last: DateTime<Utc>,
    /// Whether one of its runs has not ended
    pub(crate) live: bool,
}

/// Everything a retention pass needs from the database
#[derive(Debug, Default)]
pub(crate) struct RetentionFacts {
    /// Task rows
    pub(crate) tasks: HashMap<Uuid, RecordFact>,
    /// Origin routes of tasks that run on another machine, which have no task
    /// row here
    pub(crate) task_routes: HashMap<Uuid, Standing>,
    /// Task ids other tables name, such as received events of a remote task
    pub(crate) task_mentions: HashSet<Uuid>,
    /// Queue job rows
    pub(crate) jobs: HashMap<Uuid, RecordFact>,
    /// Origin routes of jobs whose queue is on another machine
    pub(crate) job_routes: HashMap<Uuid, Standing>,
    /// Ended tasks and jobs that something still needs, and why
    pub(crate) needed: HashMap<Uuid, LiveReason>,
    /// Tombstones
    pub(crate) pruned: HashSet<(OutputKind, Uuid)>,
    /// CI slot lanes by name
    pub(crate) lanes: HashMap<String, LaneFact>,
    /// Slot clones by path, from the `cwd` of their runs
    pub(crate) clones: HashMap<PathBuf, CloneUse>,
}

impl RetentionFacts {
    /// What still needs `id`, if anything
    pub(crate) fn needed(&self, id: Uuid) -> Option<LiveReason> {
        self.needed.get(&id).copied()
    }
}

/// Ended tasks and jobs that open work still needs, with the first reason
/// found
#[derive(Debug, Default)]
struct Needs(HashMap<Uuid, LiveReason>);

impl Needs {
    fn need(&mut self, id: Uuid, reason: LiveReason) {
        self.0.entry(id).or_insert(reason);
    }
}

/// A prune the pass is about to make, saved before the files go
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tombstone {
    /// Task or job
    pub(crate) kind: OutputKind,
    /// Its id
    pub(crate) id: Uuid,
    /// Bytes the directory held
    pub(crate) bytes: u64,
    /// Age or size
    pub(crate) reason: PruneReason,
}

const TERMINAL_TASK: &str = "status NOT IN ('queued', 'running')";

/// Whether task or job `?1` has an unsettled event, or changed or had an
/// event settle after `?2`
const CHANGED_SINCE: &str = "SELECT
    EXISTS (SELECT 1 FROM executor_outbox
        WHERE task_id = ?1 AND (state = 'pending' OR acknowledged_at >= ?2))
    OR EXISTS (SELECT 1 FROM origin_inbox
        WHERE task_id = ?1 AND (settled_at IS NULL OR settled_at >= ?2))
    OR EXISTS (SELECT 1 FROM tasks WHERE id = ?1 AND updated_at >= ?2)
    OR EXISTS (SELECT 1 FROM resource_jobs WHERE id = ?1 AND updated_at >= ?2)
    OR EXISTS (SELECT 1 FROM resource_job_routes
        WHERE job_id = ?1 AND settled_seq < accepted_seq)";

impl Store {
    /// Read every fact a retention pass needs in one read transaction
    ///
    /// `checkout_root` is where CI slot clones live; runs whose `cwd` is under
    /// it date their clone's last use
    pub(crate) fn retention_facts(&self, checkout_root: &Path) -> Result<RetentionFacts, AppError> {
        self.conn.execute_batch("BEGIN DEFERRED")?;
        let facts = read_facts(&self.conn, checkout_root);
        // a read transaction has nothing to keep, so a failed read rolls back
        let end = if facts.is_ok() { "COMMIT" } else { "ROLLBACK" };
        self.conn.execute_batch(end)?;
        facts
    }

    /// Recheck what open work needs, then save tombstones for prunes about to
    /// happen, in one write transaction
    ///
    /// A pass plans from facts read before it walks the disk, so work submitted
    /// during the walk, such as an `after` dependent of an old task, may need
    /// an entry the plan prunes. Rechecking under the write lock leaves only the
    /// removal itself for such a submit to race. An entry whose task or job
    /// changed after `since`, when the pass began, such as by a late event, is
    /// kept too, since the plan dated it from older facts. Returns the ids
    /// among `ids` to keep; their entries get no tombstone
    ///
    /// A tombstone is saved before its files are removed, so a crash between
    /// the two leaves a tombstone whose leftover files the next pass removes,
    /// never a task whose log is gone without one
    pub(crate) fn save_tombstones(
        &self,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        ids: &[Uuid],
        tombstones: &[Tombstone],
    ) -> Result<HashSet<Uuid>, AppError> {
        self.immediate(|| {
            let jobs = job_facts(&self.conn)?;
            let (_, needs) = needs(&self.conn, &jobs)?;
            let mut changed = self.conn.prepare(CHANGED_SINCE)?;
            let since = fmt_time(since);
            let mut kept = HashSet::new();
            for &id in ids {
                if needs.0.contains_key(&id)
                    || changed.query_row(params![id.to_string(), since], |row| row.get(0))?
                {
                    kept.insert(id);
                }
            }

            let mut insert = self.conn.prepare(
                "INSERT OR IGNORE INTO pruned_outputs (kind, id, pruned_at, bytes, reason)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for tombstone in tombstones.iter().filter(|t| !kept.contains(&t.id)) {
                insert.execute(params![
                    tombstone.kind.as_str(),
                    tombstone.id.to_string(),
                    fmt_time(now),
                    i64::try_from(tombstone.bytes).unwrap_or(i64::MAX),
                    tombstone.reason.as_str(),
                ])?;
            }
            Ok(kept)
        })
    }

    /// When a task's or job's output was pruned, if it was
    pub fn pruned_at(&self, kind: OutputKind, id: Uuid) -> Result<Option<DateTime<Utc>>, AppError> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT pruned_at FROM pruned_outputs WHERE kind = ?1 AND id = ?2",
                params![kind.as_str(), id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        raw.as_deref().map(parse_time).transpose()
    }

    /// Move an idle slot clone to `trash`, or leave a busy one in place
    ///
    /// A run is reserved on its slot in a write transaction before its clone
    /// is created or entered, so holding the write lock while the slot is seen
    /// idle and the clone is renamed means no run can be inside the clone. A
    /// run reserved after the rename creates a fresh clone. A run that came
    /// and went after the pass dated the clone at `last_use` used it again, so
    /// it keeps the clone too. Returns whether the clone moved
    pub(crate) fn retire_slot_clone(
        &self,
        lane: &str,
        clone: &Path,
        last_use: DateTime<Utc>,
        trash: &Path,
    ) -> Result<bool, AppError> {
        self.immediate(|| {
            let busy: bool = self.conn.query_row(
                "SELECT EXISTS (SELECT 1 FROM resources
                     WHERE name = ?1 AND ci_class IS NOT NULL AND run_task IS NOT NULL)
                 OR EXISTS (SELECT 1 FROM tasks
                     WHERE (cwd = ?2 OR substr(cwd, 1, length(?2) + 1) = ?2 || '/')
                     AND (status IN ('queued', 'running') OR updated_at > ?3))",
                params![lane, clone.to_string_lossy(), fmt_time(last_use)],
                |row| row.get(0),
            )?;
            if busy {
                return Ok(false);
            }
            std::fs::create_dir_all(trash.parent().unwrap_or(trash))?;
            match std::fs::rename(clone, trash) {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(error.into()),
            }
        })
    }

    /// The database file's allocated and free bytes
    pub(crate) fn database_usage(&self) -> Result<(u64, u64), AppError> {
        let page_size: i64 = self
            .conn
            .pragma_query_value(None, "page_size", |row| row.get(0))?;
        let pages: i64 = self
            .conn
            .pragma_query_value(None, "page_count", |row| row.get(0))?;
        let free: i64 = self
            .conn
            .pragma_query_value(None, "freelist_count", |row| row.get(0))?;
        let bytes = |count: i64| u64::try_from(count.saturating_mul(page_size)).unwrap_or(0);
        Ok((bytes(pages), bytes(free)))
    }

    /// Rebuild the database to return free pages to the filesystem, then
    /// truncate the WAL; returns the new size in bytes
    ///
    /// It holds the database for the whole rebuild, so the daemon runs it only
    /// on request, on its own store connection
    pub(crate) fn vacuum(&self) -> Result<u64, AppError> {
        self.conn.execute_batch("VACUUM")?;
        self.conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
        Ok(self.database_usage()?.0)
    }
}

fn read_facts(conn: &Connection, checkout_root: &Path) -> Result<RetentionFacts, AppError> {
    let jobs = job_facts(conn)?;
    let (job_routes, needs) = needs(conn, &jobs)?;
    Ok(RetentionFacts {
        tasks: task_facts(conn)?,
        task_routes: task_route_facts(conn)?,
        task_mentions: task_mentions(conn)?,
        jobs,
        job_routes,
        needed: needs.0,
        pruned: tombstones(conn)?,
        lanes: lane_facts(conn)?,
        clones: clone_uses(conn, checkout_root)?,
    })
}

/// Job route standings, and everything open work needs
fn needs(
    conn: &Connection,
    jobs: &HashMap<Uuid, RecordFact>,
) -> Result<(HashMap<Uuid, Standing>, Needs), AppError> {
    let (job_routes, route_after) = job_route_facts(conn, jobs)?;
    let mut needs = Needs::default();
    for id in route_after {
        needs.need(id, LiveReason::Dependents);
    }
    needed_by_tasks(conn, &mut needs)?;
    Ok((job_routes, needs))
}

fn uuid(raw: &str) -> Result<Uuid, AppError> {
    Uuid::parse_str(raw).map_err(|error| AppError::Internal {
        message: format!("stored id {raw}: {error}"),
    })
}

fn task_facts(conn: &Connection) -> Result<HashMap<Uuid, RecordFact>, AppError> {
    let mut statement = conn.prepare(&format!(
        "SELECT t.id, t.thread_id, t.{TERMINAL_TASK},
             MAX(t.updated_at,
                 COALESCE((SELECT MAX(o.acknowledged_at) FROM executor_outbox o
                     WHERE o.task_id = t.id), ''),
                 COALESCE((SELECT MAX(o.acknowledged_at) FROM executor_event_receipts o
                     WHERE o.task_id = t.id), ''),
                 COALESCE((SELECT MAX(i.settled_at) FROM origin_inbox i
                     WHERE i.task_id = t.id), ''),
                 COALESCE((SELECT MAX(i.settled_at) FROM origin_event_receipts i
                     WHERE i.task_id = t.id), '')),
             EXISTS (SELECT 1 FROM executor_outbox o WHERE o.task_id = t.id AND o.state = 'pending')
             OR EXISTS (SELECT 1 FROM origin_inbox i WHERE i.task_id = t.id AND i.settled_at IS NULL)
         FROM tasks t"
    ))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, bool>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    rows.into_iter()
        .map(|(id, thread, ended, updated, unsettled)| {
            let standing = match (ended, unsettled) {
                (false, _) => Standing::Live(LiveReason::Running),
                (true, true) => Standing::Live(LiveReason::UnsettledEvents),
                // dated by the later of the terminal transition and the last
                // event settling: an origin acknowledges a remote event before
                // it reaches the thread, and a thread notified late must find
                // the evidence still there for `min_age`. Receipts keep the
                // settle time once compaction removes the event row, so a
                // `max_age` past the 30-day event retention still holds
                (true, false) => Standing::Ended(parse_time(&updated)?),
            };
            Ok((
                uuid(&id)?,
                RecordFact {
                    thread: ThreadId(uuid(&thread)?),
                    standing,
                },
            ))
        })
        .collect()
}

/// Routes of tasks with no row here: their output is on the executor, and this
/// origin keeps only the callback delivery files
fn task_route_facts(conn: &Connection) -> Result<HashMap<Uuid, Standing>, AppError> {
    let mut statement = conn.prepare(
        "SELECT r.task_id, r.outcome IS NOT NULL,
             EXISTS (SELECT 1 FROM origin_inbox i WHERE i.task_id = r.task_id AND i.settled_at IS NULL),
             (SELECT MAX(settled_at) FROM (
                 SELECT settled_at FROM origin_inbox WHERE task_id = r.task_id
                 UNION ALL SELECT settled_at FROM origin_event_receipts WHERE task_id = r.task_id))
         FROM origin_routes r
         WHERE NOT EXISTS (SELECT 1 FROM tasks t WHERE t.id = r.task_id)",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    rows.into_iter()
        .map(|(id, ended, unsettled, settled)| {
            let standing = match (ended, unsettled) {
                (false, _) => Standing::Live(LiveReason::OpenRoute),
                (true, true) => Standing::Live(LiveReason::UnsettledEvents),
                // the inventory dates it by its files when no event was kept
                (true, false) => Standing::Ended(
                    settled
                        .as_deref()
                        .map(parse_time)
                        .transpose()?
                        .unwrap_or(DateTime::UNIX_EPOCH),
                ),
            };
            Ok((uuid(&id)?, standing))
        })
        .collect()
}

fn task_mentions(conn: &Connection) -> Result<HashSet<Uuid>, AppError> {
    let mut statement = conn.prepare(
        "SELECT task_id FROM origin_inbox
         UNION SELECT task_id FROM origin_event_receipts
         UNION SELECT task_id FROM executor_outbox
         UNION SELECT task_id FROM executor_event_receipts
         UNION SELECT task_id FROM executor_identities",
    )?;
    let ids = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    ids.iter().map(|id| uuid(id)).collect()
}

fn job_facts(conn: &Connection) -> Result<HashMap<Uuid, RecordFact>, AppError> {
    let mut statement = conn.prepare(
        "SELECT j.id, j.thread_id, j.state IN ('succeeded', 'failed', 'cancelled'), j.updated_at,
             j.event_seq > COALESCE(
                 (SELECT d.acknowledged_seq FROM resource_job_delivery d WHERE d.job_id = j.id), 0)
             OR EXISTS (SELECT 1 FROM resource_job_routes r
                 WHERE r.job_id = j.id AND r.settled_seq < r.accepted_seq)
         FROM resource_jobs j",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, bool>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    rows.into_iter()
        .map(|(id, thread, ended, updated, unsettled)| {
            let standing = match (ended, unsettled) {
                (false, _) => Standing::Live(LiveReason::Running),
                (true, true) => Standing::Live(LiveReason::UnsettledEvents),
                // `end_job` stamps `updated_at`, and a terminal job never moves
                (true, false) => Standing::Ended(parse_time(&updated)?),
            };
            Ok((
                uuid(&id)?,
                RecordFact {
                    thread: ThreadId(uuid(&thread)?),
                    standing,
                },
            ))
        })
        .collect()
}

/// Standing of job routes whose job has no row here, and the `after` entries
/// of every job route that has not ended
///
/// A held or unknown submission still waits here. A job with a row here is
/// open until that row ends. A job accepted by a queue on another machine runs
/// there, where nothing can read this machine's files by path, so its
/// dependencies here stop mattering once it is sent
fn job_route_facts(
    conn: &Connection,
    jobs: &HashMap<Uuid, RecordFact>,
) -> Result<(HashMap<Uuid, Standing>, Vec<Uuid>), AppError> {
    let mut statement = conn.prepare(
        "SELECT job_id, route_json, settled_seq < accepted_seq FROM resource_job_routes",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let mut standings = HashMap::new();
    let mut after = Vec::new();
    for (id, json, unsettled) in rows {
        let id = uuid(&id)?;
        let route: JobRoute = serde_json::from_str(&json)?;
        let waits_here = matches!(
            route.submission,
            JobSubmission::Held | JobSubmission::Unknown
        );
        let open = match jobs.get(&id) {
            Some(job) => matches!(job.standing, Standing::Live(_)),
            None => waits_here,
        };
        if open && let Some(dependencies) = &route.after {
            after.extend(dependencies.ids().iter().map(|entry| entry.as_task().0));
        }
        if jobs.contains_key(&id) {
            continue;
        }
        let standing = match (waits_here, unsettled) {
            (true, _) => Standing::Live(LiveReason::OpenRoute),
            (false, true) => Standing::Live(LiveReason::UnsettledEvents),
            // the inventory dates delivery files by their newest change
            (false, false) => Standing::Ended(DateTime::UNIX_EPOCH),
        };
        standings.insert(id, standing);
    }

    Ok((standings, after))
}

/// Mark what open tasks, chains, holds, and jobs still need
fn needed_by_tasks(conn: &Connection, needs: &mut Needs) -> Result<(), AppError> {
    // `after` of every route whose task has not ended, held continuations too
    let mut statement = conn.prepare(
        "SELECT after_json FROM origin_routes WHERE outcome IS NULL AND after_json IS NOT NULL",
    )?;
    let lists = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for list in lists {
        for id in uuids_in(&serde_json::from_str(&list)?) {
            needs.need(id, LiveReason::Dependents);
        }
    }

    // targets of waiting reports, which a parked worker's continuation reads
    // with `task log` once they end
    let mut statement = conn.prepare(&format!(
        "SELECT r.waiting_on FROM reports r
         WHERE r.waiting_on IS NOT NULL AND (
             r.task_id IN (SELECT id FROM tasks WHERE status IN ('queued', 'running'))
             OR r.task_id IN (SELECT task_id FROM chain_runs WHERE chain_id IN ({OPEN_CHAINS})))"
    ))?;
    let targets = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for target in targets {
        for id in uuids_in(&serde_json::from_str(&target)?) {
            needs.need(id, LiveReason::Dependents);
        }
    }

    let mut statement = conn.prepare(&format!(
        "SELECT task_id FROM chain_runs WHERE chain_id IN ({OPEN_CHAINS})"
    ))?;
    for id in column(&mut statement)? {
        needs.need(id, LiveReason::OpenChain);
    }

    let mut statement = conn.prepare(
        "SELECT target_id FROM sleep_holds
         UNION SELECT parked_under FROM sleep_holds WHERE parked_under IS NOT NULL",
    )?;
    for id in column(&mut statement)? {
        needs.need(id, LiveReason::SleepHold);
    }

    // Claude logs the usage backfill has not read yet; the same rule as
    // `claude_tasks_without_usage`, which skips pruned tasks for good
    let mut statement = conn.prepare(&format!(
        "SELECT id FROM tasks
         WHERE {TERMINAL_TASK}
           AND json_extract(workload_json, '$.type') = 'agent'
           AND json_extract(workload_json, '$.agent') = 'claude'
           AND id NOT IN (SELECT task_id FROM task_usage)"
    ))?;
    for id in column(&mut statement)? {
        needs.need(id, LiveReason::UsageUnread);
    }

    // earlier runs of a job that has not ended, such as a preempted attempt
    let mut statement = conn.prepare(&format!(
        "SELECT t.id FROM tasks t JOIN resource_jobs j ON j.id = t.resource_job_id
         WHERE t.{TERMINAL_TASK} AND j.state IN ('queued', 'active')"
    ))?;
    for id in column(&mut statement)? {
        needs.need(id, LiveReason::OpenJob);
    }

    Ok(())
}

/// Chains with a run that has not ended, such as a held continuation that
/// has not launched; a run ended before launch has an outcome on its route
const OPEN_CHAINS: &str = "SELECT c.chain_id FROM chain_runs c
    WHERE NOT EXISTS (SELECT 1 FROM tasks t
        WHERE t.id = c.task_id AND t.status NOT IN ('queued', 'running'))
    AND NOT EXISTS (SELECT 1 FROM origin_routes r
        WHERE r.task_id = c.task_id AND r.outcome IS NOT NULL)";

fn column(statement: &mut rusqlite::Statement<'_>) -> Result<Vec<Uuid>, AppError> {
    let ids = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    ids.iter().map(|id| uuid(id)).collect()
}

/// Every UUID string anywhere in `value`
///
/// Dependency lists and waiting targets are read this way rather than by their
/// types, so a target kind a later release adds still protects what it names
fn uuids_in(value: &Value) -> Vec<Uuid> {
    let mut found = Vec::new();
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            Value::String(raw) => found.extend(Uuid::parse_str(raw).ok()),
            Value::Array(items) => pending.extend(items),
            Value::Object(fields) => pending.extend(fields.values()),
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    found
}

fn tombstones(conn: &Connection) -> Result<HashSet<(OutputKind, Uuid)>, AppError> {
    let mut statement = conn.prepare("SELECT kind, id FROM pruned_outputs")?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(kind, id)| {
            let kind = match kind.as_str() {
                "task" => OutputKind::Task,
                "job" => OutputKind::Job,
                other => {
                    return Err(AppError::Internal {
                        message: format!("stored pruned output kind {other}"),
                    });
                }
            };
            Ok((kind, uuid(&id)?))
        })
        .collect()
}

fn lane_facts(conn: &Connection) -> Result<HashMap<String, LaneFact>, AppError> {
    let mut statement = conn.prepare(
        "SELECT name, MAX(run_task IS NOT NULL) FROM resources
         WHERE ci_class IS NOT NULL GROUP BY name",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows
        .into_iter()
        .map(|(name, busy)| (name, LaneFact { busy }))
        .collect())
}

fn clone_uses(
    conn: &Connection,
    checkout_root: &Path,
) -> Result<HashMap<PathBuf, CloneUse>, AppError> {
    let mut prefix = checkout_root.to_string_lossy().into_owned();
    if !prefix.ends_with('/') {
        prefix.push('/');
    }
    let mut statement = conn.prepare(
        "SELECT cwd, MAX(updated_at), MAX(status IN ('queued', 'running')) FROM tasks
         WHERE substr(cwd, 1, length(?1)) = ?1 GROUP BY cwd",
    )?;
    let rows = statement
        .query_map([prefix], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(cwd, last, live)| {
            Ok((
                PathBuf::from(cwd),
                CloneUse {
                    last: parse_time(&last)?,
                    live,
                },
            ))
        })
        .collect()
}
