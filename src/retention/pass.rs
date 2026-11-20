//! One retention pass: read the facts, plan, then tombstone and remove

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;

use super::disk;
use super::inventory::{self, Roots};
use super::{
    DatabaseReport, Entry, EntryState, GcReport, KeepReason, KindReport, OutputKind, Owner,
    PruneFailure, PruneReason, PrunedEntry, RetentionConfig, Verdict, plan,
};
use crate::error::AppError;
use crate::home::Home;
use crate::store::{Store, Tombstone};

/// Whether a pass removes what it plans to prune
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassMode {
    /// Plan and report only
    DryRun,
    /// Plan, then tombstone and remove
    Apply,
}

/// Wait this long for the daemon's store actor to release the write lock;
/// each write of a pass is one short transaction
const BUSY_TIMEOUT: Duration = Duration::from_secs(30);

/// Tombstones saved per transaction before their directories are removed
const TOMBSTONE_BATCH: usize = 256;

/// Run one pass over `home` with its own database connection
///
/// It reads large directory trees and removes files, so it runs on a blocking
/// thread and never holds the daemon's store actor. `stop` is checked between
/// entries; a stopped pass reports what it did so far
pub fn run_pass(
    home: &Home,
    checkout_root: &Path,
    config: &RetentionConfig,
    mode: PassMode,
    stop: &dyn Fn() -> bool,
) -> Result<GcReport, AppError> {
    let started_at = Utc::now();
    let store = Store::open_with_busy_timeout(&home.db_path(), BUSY_TIMEOUT)?;
    let trash = disk::trash_dir(checkout_root);
    let mut failures = Vec::new();
    if mode == PassMode::Apply
        && let Err(error) = disk::empty_trash(&trash)
    {
        failures.push(PruneFailure {
            path: trash.clone(),
            error: error.to_string(),
        });
    }

    let facts = store.retention_facts(checkout_root)?;
    let roots = Roots {
        tasks: home.tasks_dir(),
        jobs: home.jobs_dir(),
        checkouts: checkout_root.to_path_buf(),
    };
    let entries = inventory::entries(&roots, &facts, stop)?;
    let verdicts = plan(config, started_at, &entries);

    let decided: Vec<(&Entry, Verdict)> = entries.iter().zip(verdicts).collect();
    let done = match mode {
        PassMode::DryRun => decided
            .iter()
            .map(|(_, verdict)| matches!(verdict, Verdict::Prune(_)))
            .collect(),
        PassMode::Apply => apply(&store, started_at, &decided, &trash, &mut failures, stop)?,
    };

    let (bytes, free_bytes) = store.database_usage()?;
    Ok(report(
        mode,
        started_at,
        &decided,
        &done,
        failures,
        DatabaseReport {
            bytes,
            free_bytes,
            vacuumed_bytes: None,
        },
    ))
}

/// Prune every entry with a prune verdict; returns which entries are gone
fn apply(
    store: &Store,
    started_at: chrono::DateTime<Utc>,
    decided: &[(&Entry, Verdict)],
    trash: &Path,
    failures: &mut Vec<PruneFailure>,
    stop: &dyn Fn() -> bool,
) -> Result<Vec<bool>, AppError> {
    let mut done = vec![false; decided.len()];
    let pruned: Vec<usize> = (0..decided.len())
        .filter(|&index| matches!(decided[index].1, Verdict::Prune(_)))
        .collect();
    for batch in pruned.chunks(TOMBSTONE_BATCH) {
        if stop() {
            break;
        }
        let ids: Vec<Uuid> = batch
            .iter()
            .filter_map(|&index| entry_id(decided[index].0))
            .collect();
        let tombstones: Vec<Tombstone> = batch
            .iter()
            .filter_map(|&index| tombstone(decided[index].0, decided[index].1))
            .collect();
        let needed = store.save_tombstones(Utc::now(), started_at, &ids, &tombstones)?;

        for &index in batch {
            let entry = decided[index].0;
            if entry_id(entry).is_some_and(|id| needed.contains(&id)) {
                continue;
            }
            match remove(store, entry, trash) {
                Ok(removed) => done[index] = removed,
                Err(error) => failures.push(PruneFailure {
                    path: entry.path.clone(),
                    error: error.to_string(),
                }),
            }
        }
    }

    Ok(done)
}

/// The task or job id an entry's directory is named by; slot clones have none
const fn entry_id(entry: &Entry) -> Option<Uuid> {
    match entry.owner {
        Owner::Record { id } | Owner::Delivery { id } => Some(id),
        Owner::Unreferenced | Owner::Slot { .. } => None,
    }
}

/// The tombstone a pruned task or job row gets; a leftover already has one,
/// and other owners have no row to mark
fn tombstone(entry: &Entry, verdict: Verdict) -> Option<Tombstone> {
    let Verdict::Prune(reason @ (PruneReason::Age | PruneReason::Size)) = verdict else {
        return None;
    };
    let Owner::Record { id } = entry.owner else {
        return None;
    };
    Some(Tombstone {
        kind: entry.kind,
        id,
        bytes: entry.bytes,
        reason,
    })
}

/// Remove one entry; returns `false` for a slot clone a run took meanwhile
fn remove(store: &Store, entry: &Entry, trash: &Path) -> Result<bool, AppError> {
    let Owner::Slot { lane } = &entry.owner else {
        disk::remove(&entry.path)?;
        return Ok(true);
    };
    // only an ended clone gets a prune verdict
    let EntryState::Ended { at: last_use, .. } = entry.state else {
        return Ok(false);
    };
    let moved = trash.join(trash_name(&entry.path));
    if !store.retire_slot_clone(lane, &entry.path, last_use, &moved)? {
        return Ok(false);
    }
    disk::remove(&moved)?;
    Ok(true)
}

/// `<remote>--<lane>--<nanoseconds>`, so neither two clones nor one clone
/// retired again after a failed removal ever meet in the trash
fn trash_name(clone: &Path) -> PathBuf {
    let name = |path: Option<&Path>| {
        path.and_then(Path::file_name)
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    };
    let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    PathBuf::from(format!(
        "{}--{}--{stamp}",
        name(clone.parent()),
        name(Some(clone))
    ))
}

fn report(
    mode: PassMode,
    started_at: chrono::DateTime<Utc>,
    decided: &[(&Entry, Verdict)],
    done: &[bool],
    failures: Vec<PruneFailure>,
    database: DatabaseReport,
) -> GcReport {
    let kinds = OutputKind::ALL
        .iter()
        .map(|&kind| kind_report(kind, decided, done))
        .collect();
    let mut pruned: Vec<PrunedEntry> = decided
        .iter()
        .zip(done)
        .filter_map(|((entry, verdict), &done)| {
            let Verdict::Prune(reason) = verdict else {
                return None;
            };
            done.then(|| PrunedEntry {
                kind: entry.kind,
                path: entry.path.clone(),
                bytes: entry.bytes,
                reason: *reason,
                ended_at: match entry.state {
                    EntryState::Ended { at, .. } => Some(at),
                    EntryState::Live { .. } | EntryState::Tombstoned => None,
                },
            })
        })
        .collect();
    pruned.sort_by_key(|entry| std::cmp::Reverse(entry.bytes));

    GcReport {
        dry_run: mode == PassMode::DryRun,
        started_at,
        kinds,
        pruned,
        failures,
        database,
    }
}

fn kind_report(kind: OutputKind, decided: &[(&Entry, Verdict)], done: &[bool]) -> KindReport {
    let mut report = KindReport {
        kind,
        entries: 0,
        bytes: 0,
        pruned: 0,
        pruned_bytes: 0,
        kept_live: 0,
        kept_recent: 0,
        kept_young: 0,
    };
    for ((entry, verdict), &done) in decided.iter().zip(done) {
        if entry.kind != kind {
            continue;
        }
        report.entries += 1;
        report.bytes += entry.bytes;
        match verdict {
            Verdict::Prune(_) if done => {
                report.pruned += 1;
                report.pruned_bytes += entry.bytes;
            }
            // a prune that failed, lost its slot to a run, or that new work came
            // to need stays on disk
            Verdict::Prune(_) => {}
            Verdict::Keep(KeepReason::Live { .. }) => report.kept_live += 1,
            Verdict::Keep(KeepReason::Recent) => report.kept_recent += 1,
            Verdict::Keep(KeepReason::Young) => report.kept_young += 1,
        }
    }

    report
}
