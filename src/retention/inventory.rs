//! Join the directories on disk with what the database says about their owners

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::disk::{self, Usage};
use super::{Entry, EntryState, LiveReason, OutputKind, Owner};
use crate::error::AppError;
use crate::home::RUNNER_LOCK_FILE;
use crate::store::{RecordFact, RetentionFacts, Standing};

/// Where a pass looks
#[derive(Debug, Clone)]
pub(super) struct Roots {
    /// `tasks/`
    pub(super) tasks: PathBuf,
    /// `jobs/`
    pub(super) jobs: PathBuf,
    /// CI slot clone root
    pub(super) checkouts: PathBuf,
}

/// Every entry under `roots`, measured, with its state from `facts`
///
/// An entry that cannot be measured is skipped: it is never pruned blind
pub(super) fn entries(
    roots: &Roots,
    facts: &RetentionFacts,
    stop: &dyn Fn() -> bool,
) -> Result<Vec<Entry>, AppError> {
    let mut entries = Vec::new();
    for (id, path) in disk::uuid_dirs(&roots.tasks)? {
        if stop() {
            return Ok(entries);
        }
        entries.extend(task_entry(facts, id, path));
    }
    for (id, path) in disk::uuid_dirs(&roots.jobs)? {
        if stop() {
            return Ok(entries);
        }
        entries.extend(job_entry(facts, id, path));
    }
    for (lane, path) in disk::slot_clones(&roots.checkouts)? {
        if stop() {
            return Ok(entries);
        }
        entries.extend(clone_entry(facts, lane, path));
    }

    Ok(entries)
}

fn task_entry(facts: &RetentionFacts, id: Uuid, path: PathBuf) -> Option<Entry> {
    let usage = disk::measure(&path).ok()?;
    let (owner, state) = if let Some(record) = facts.tasks.get(&id) {
        (
            Owner::Record { id },
            record_state(facts, OutputKind::Task, id, record),
        )
    } else if let Some(standing) = facts.task_routes.get(&id) {
        (
            Owner::Delivery { id },
            delivery_state(facts, id, *standing, &usage),
        )
    } else if facts.task_mentions.contains(&id) {
        // events of a task this machine knows only by its events; leave them
        // to the event retention that owns those rows
        return None;
    } else {
        (Owner::Unreferenced, unreferenced_state(&path, &usage))
    };

    Some(Entry {
        kind: OutputKind::Task,
        path,
        owner,
        bytes: usage.bytes,
        state,
    })
}

fn job_entry(facts: &RetentionFacts, id: Uuid, path: PathBuf) -> Option<Entry> {
    let usage = disk::measure(&path).ok()?;
    let (owner, state) = if let Some(record) = facts.jobs.get(&id) {
        (
            Owner::Record { id },
            record_state(facts, OutputKind::Job, id, record),
        )
    } else if let Some(standing) = facts.job_routes.get(&id) {
        (
            Owner::Delivery { id },
            delivery_state(facts, id, *standing, &usage),
        )
    } else {
        (Owner::Unreferenced, unreferenced_state(&path, &usage))
    };

    Some(Entry {
        kind: OutputKind::Job,
        path,
        owner,
        bytes: usage.bytes,
        state,
    })
}

fn record_state(
    facts: &RetentionFacts,
    kind: OutputKind,
    id: Uuid,
    record: &RecordFact,
) -> EntryState {
    if facts.pruned.contains(&(kind, id)) {
        return EntryState::Tombstoned;
    }
    match (record.standing, facts.needed(id)) {
        (Standing::Live(reason), _) | (Standing::Ended(_), Some(reason)) => {
            EntryState::Live { reason }
        }
        (Standing::Ended(at), None) => EntryState::Ended {
            at,
            thread: Some(record.thread),
        },
    }
}

/// Delivery files of work that ran elsewhere, dated by their newest change
/// when that is later than the last settled event
fn delivery_state(
    facts: &RetentionFacts,
    id: Uuid,
    standing: Standing,
    usage: &Usage,
) -> EntryState {
    match (standing, facts.needed(id)) {
        (Standing::Live(reason), _) | (Standing::Ended(_), Some(reason)) => {
            EntryState::Live { reason }
        }
        (Standing::Ended(settled), None) => EntryState::Ended {
            at: settled.max(usage.newest),
            thread: None,
        },
    }
}

/// A directory nothing names, dated by its newest change
///
/// A task directory exists for a moment before its row during a submit, and a
/// worker of a database this release never opens may still run, so a recent
/// change or a held runner lock keeps it
fn unreferenced_state(path: &Path, usage: &Usage) -> EntryState {
    if disk::lock_held(&path.join(RUNNER_LOCK_FILE)) {
        return EntryState::Live {
            reason: LiveReason::Locked,
        };
    }
    EntryState::Ended {
        at: usage.newest,
        thread: None,
    }
}

/// A slot clone, dated by its latest run, or by its own directory when no
/// run row names it
///
/// A clone is not dated by its newest file: a later run's build can leave
/// files whose times say nothing about use, and the runs already say when the
/// slot last used it
fn clone_entry(facts: &RetentionFacts, lane: String, path: PathBuf) -> Option<Entry> {
    let usage = disk::measure(&path).ok()?;
    let runs = facts.clones.get(&path);
    let busy = facts.lanes.get(&lane).is_some_and(|fact| fact.busy);
    let state = if busy || runs.is_some_and(|runs| runs.live) {
        EntryState::Live {
            reason: LiveReason::SlotBusy,
        }
    } else {
        EntryState::Ended {
            at: runs.map_or_else(|| own_mtime(&path), |runs| runs.last),
            thread: None,
        }
    };

    Some(Entry {
        kind: OutputKind::Checkout,
        path,
        owner: Owner::Slot { lane },
        bytes: usage.bytes,
        state,
    })
}

fn own_mtime(path: &Path) -> DateTime<Utc> {
    std::fs::symlink_metadata(path)
        .and_then(|meta| meta.modified())
        .map_or(DateTime::UNIX_EPOCH, DateTime::from)
}
