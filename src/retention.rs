//! Retention of the on-disk output that finished tasks, jobs, and CI slots leave
//! in the state directory
//!
//! A pass lists every task directory, job directory, and CI slot clone, joins
//! each one with what the database says about its owner ([`Entry`]), and lets
//! [`plan`] decide per kind which to prune under a [`RetentionConfig`]. Only an
//! entry whose owner has ended and that nothing may still read can be pruned:
//! the inventory marks every other entry [`EntryState::Live`], and the plan never
//! prunes a live entry. Pruning a task or job row leaves a tombstone, so
//! `homebased task log` says the output was pruned instead of reading an empty
//! file

mod disk;
mod inventory;
mod pass;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::ThreadId;

pub use pass::{PassMode, run_pass};

/// Age and size limits of one kind of output
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Limits {
    /// Ended output older than this is pruned
    #[serde(with = "humantime_serde")]
    pub max_age: Duration,
    /// Ceiling on the kind's total bytes on disk; while the total is above it,
    /// the oldest ended output at least [`RetentionConfig::min_age`] old is
    /// pruned too. `None` sets no ceiling
    pub max_size: Option<u64>,
}

/// Limits of output that belongs to a thread's task or job
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RecordRule {
    /// Age and size limits
    #[serde(flatten)]
    pub limits: Limits,
    /// The newest ended entries of each thread that are always kept, whatever
    /// their age or the size limit
    pub keep_last: u32,
}

/// Retention settings, from `[retention]` in `config.toml`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RetentionConfig {
    /// Time between the daemon's own passes; `None` leaves pruning to
    /// `homebased gc`
    #[serde(with = "humantime_serde")]
    pub interval: Option<Duration>,
    /// Ended output younger than this is never pruned, not even over a size
    /// limit, so a thread that just got a final event can still read it
    #[serde(with = "humantime_serde")]
    pub min_age: Duration,
    /// Task directories: `tasks/<id>`, which hold `output.log` and the
    /// `evidence` of task events
    pub tasks: RecordRule,
    /// GPU and CI queue job directories: `jobs/<id>`, which hold the
    /// checkpoint state all runs of a job share
    pub jobs: RecordRule,
    /// CI slot clones under the checkout root, which a later CI job on the
    /// same slot reuses; age counts from the slot's last run
    pub ci: Limits,
}

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            interval: Some(Duration::from_secs(6 * 60 * 60)),
            min_age: DAY,
            tasks: RecordRule {
                limits: Limits {
                    max_age: 14 * DAY,
                    max_size: None,
                },
                keep_last: 3,
            },
            jobs: RecordRule {
                limits: Limits {
                    max_age: 14 * DAY,
                    max_size: None,
                },
                keep_last: 1,
            },
            ci: Limits {
                max_age: 7 * DAY,
                max_size: None,
            },
        }
    }
}

impl RetentionConfig {
    fn limits(&self, kind: OutputKind) -> Limits {
        match kind {
            OutputKind::Task => self.tasks.limits,
            OutputKind::Job => self.jobs.limits,
            OutputKind::Checkout => self.ci,
        }
    }

    fn keep_last(&self, kind: OutputKind) -> u32 {
        match kind {
            OutputKind::Task => self.tasks.keep_last,
            OutputKind::Job => self.jobs.keep_last,
            // a clone belongs to a slot, not a thread
            OutputKind::Checkout => 0,
        }
    }
}

/// Kind of pruned output, each with its own limits
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputKind {
    /// `tasks/<id>`
    Task,
    /// `jobs/<id>`
    Job,
    /// `<checkout root>/<remote>/<slot>`
    Checkout,
}

impl OutputKind {
    /// Every kind, in report order
    pub const ALL: [Self; 3] = [Self::Task, Self::Job, Self::Checkout];

    /// Storage and wire name
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Job => "job",
            Self::Checkout => "checkout",
        }
    }
}

/// What owns a directory, which decides whether pruning leaves a tombstone
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "owner", rename_all = "snake_case")]
pub enum Owner {
    /// A task or job row on this machine; pruning saves a tombstone for it
    Record {
        /// Task or job id
        id: Uuid,
    },
    /// Callback delivery files this origin keeps for a task or job that ran on
    /// another machine, whose output lives there
    Delivery {
        /// Task or job id
        id: Uuid,
    },
    /// A directory no record on this machine names, such as one left by a
    /// database from an earlier release
    Unreferenced,
    /// A CI slot's clone
    Slot {
        /// Slot lane name, such as `rust-2`
        lane: String,
    },
}

/// Why an entry may still be read or written, so no limit prunes it
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveReason {
    /// The task or job has not ended
    Running,
    /// An event of it has not been delivered and settled yet
    UnsettledEvents,
    /// `homebased wait --long` still holds it for a sleeping thread
    SleepHold,
    /// A task or job that has not ended waits on it with `after`, or a parked
    /// worker's report names it
    Dependents,
    /// It belongs to a parked agent's chain whose latest run has not ended
    OpenChain,
    /// It is a run of a job that has not ended
    OpenJob,
    /// Its origin route has no outcome yet
    OpenRoute,
    /// A worker still holds its runner lock
    Locked,
    /// Its CI slot has a run
    SlotBusy,
}

/// What the database and the directory say about one entry
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum EntryState {
    /// Something may still use it
    Live {
        /// Why
        reason: LiveReason,
    },
    /// Its owner ended, and nothing named here still needs it
    Ended {
        /// When it ended or was last used
        at: DateTime<Utc>,
        /// The thread its owner reported to, when known
        thread: Option<ThreadId>,
    },
    /// A tombstone says it was pruned, but files remain from a pass that was
    /// interrupted or failed to remove them
    Tombstoned,
}

/// One directory a pass may prune
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Entry {
    /// Kind, which picks the limits
    pub kind: OutputKind,
    /// Absolute path
    pub path: PathBuf,
    /// What owns it
    #[serde(flatten)]
    pub owner: Owner,
    /// Bytes allocated on disk
    pub bytes: u64,
    /// Whether it may be pruned
    #[serde(flatten)]
    pub state: EntryState,
}

/// Why the plan keeps an entry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(tag = "kept", rename_all = "snake_case")]
pub enum KeepReason {
    /// Something may still use it
    Live {
        /// Why
        reason: LiveReason,
    },
    /// One of the newest `keep_last` ended entries of its thread
    Recent,
    /// Younger than the age limit, and the kind is within its size limit
    Young,
}

/// Why the plan prunes an entry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PruneReason {
    /// Older than the kind's `max_age`
    Age,
    /// The kind was over its `max_size`, and this was among its oldest entries
    Size,
    /// Files left behind by an earlier prune
    Leftover,
}

impl PruneReason {
    /// Storage name
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Age => "age",
            Self::Size => "size",
            Self::Leftover => "leftover",
        }
    }
}

/// The plan's decision for one entry
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Leave it
    Keep(KeepReason),
    /// Remove it
    Prune(PruneReason),
}

/// Decide, for every entry, whether to keep or prune it, in entry order
///
/// Per kind: the newest `keep_last` ended entries of each thread are kept,
/// tombstoned leftovers are pruned, other ended entries older than `max_age`
/// are pruned, and while the kind's total bytes stay above `max_size` the
/// oldest remaining ended entries at least `min_age` old are pruned as well.
/// A live entry is always kept
#[must_use]
pub fn plan(config: &RetentionConfig, now: DateTime<Utc>, entries: &[Entry]) -> Vec<Verdict> {
    let mut verdicts: Vec<Option<Verdict>> = vec![None; entries.len()];
    for kind in OutputKind::ALL {
        plan_kind(config, now, kind, entries, &mut verdicts);
    }

    verdicts
        .into_iter()
        .map(|verdict| verdict.unwrap_or(Verdict::Keep(KeepReason::Young)))
        .collect()
}

fn plan_kind(
    config: &RetentionConfig,
    now: DateTime<Utc>,
    kind: OutputKind,
    entries: &[Entry],
    verdicts: &mut [Option<Verdict>],
) {
    let limits = config.limits(kind);
    let indices: Vec<usize> = (0..entries.len())
        .filter(|&index| entries[index].kind == kind)
        .collect();
    let recent = recent_per_thread(entries, &indices, config.keep_last(kind));

    let mut size_candidates = Vec::new();
    for &index in &indices {
        let entry = &entries[index];
        verdicts[index] = Some(match &entry.state {
            EntryState::Live { reason } => Verdict::Keep(KeepReason::Live { reason: *reason }),
            EntryState::Tombstoned => Verdict::Prune(PruneReason::Leftover),
            EntryState::Ended { .. } if recent.contains(&index) => {
                Verdict::Keep(KeepReason::Recent)
            }
            EntryState::Ended { at, .. } if age(now, *at) >= limits.max_age => {
                Verdict::Prune(PruneReason::Age)
            }
            EntryState::Ended { at, .. } => {
                if age(now, *at) >= config.min_age {
                    size_candidates.push((*at, index));
                }
                Verdict::Keep(KeepReason::Young)
            }
        });
    }

    let Some(max_size) = limits.max_size else {
        return;
    };
    let mut remaining: u64 = indices
        .iter()
        .filter(|&&index| !matches!(verdicts[index], Some(Verdict::Prune(_))))
        .map(|&index| entries[index].bytes)
        .sum();
    size_candidates.sort();
    for (_, index) in size_candidates {
        if remaining <= max_size {
            break;
        }
        verdicts[index] = Some(Verdict::Prune(PruneReason::Size));
        remaining = remaining.saturating_sub(entries[index].bytes);
    }
}

/// Indices of the newest `keep_last` ended entries of each known thread
fn recent_per_thread(entries: &[Entry], indices: &[usize], keep_last: u32) -> HashSet<usize> {
    if keep_last == 0 {
        return HashSet::new();
    }
    let mut by_thread: HashMap<ThreadId, Vec<(DateTime<Utc>, usize)>> = HashMap::new();
    for &index in indices {
        if let EntryState::Ended {
            at,
            thread: Some(thread),
        } = entries[index].state
        {
            by_thread.entry(thread).or_default().push((at, index));
        }
    }

    let keep = usize::try_from(keep_last).unwrap_or(usize::MAX);
    by_thread
        .into_values()
        .flat_map(|mut ended| {
            ended.sort_by(|left, right| right.cmp(left));
            ended.into_iter().take(keep).map(|(_, index)| index)
        })
        .collect()
}

/// Age of `at`, with a future time counting as zero
fn age(now: DateTime<Utc>, at: DateTime<Utc>) -> Duration {
    (now - at).to_std().unwrap_or_default()
}

/// What one pass found and pruned, or would prune under `--dry-run`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    /// Whether the pass only planned
    pub dry_run: bool,
    /// When the pass started
    pub started_at: DateTime<Utc>,
    /// Per kind totals, in [`OutputKind::ALL`] order
    pub kinds: Vec<KindReport>,
    /// Each entry pruned, or that would be under `--dry-run`, largest first
    pub pruned: Vec<PrunedEntry>,
    /// Entries the pass decided to prune but could not remove
    pub failures: Vec<PruneFailure>,
    /// Size of the database file
    pub database: DatabaseReport,
}

impl GcReport {
    /// Bytes pruned, or that would be under `--dry-run`
    #[must_use]
    pub fn freed(&self) -> u64 {
        self.kinds.iter().map(|kind| kind.pruned_bytes).sum()
    }
}

/// Totals of one kind
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KindReport {
    /// Kind
    pub kind: OutputKind,
    /// Entries found
    pub entries: usize,
    /// Their bytes on disk
    pub bytes: u64,
    /// Entries pruned
    pub pruned: usize,
    /// Bytes pruned
    pub pruned_bytes: u64,
    /// Entries kept because something may still use them
    pub kept_live: usize,
    /// Entries kept as their thread's newest
    pub kept_recent: usize,
    /// Entries kept as younger than the limits
    pub kept_young: usize,
}

/// One pruned entry
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrunedEntry {
    /// Kind
    pub kind: OutputKind,
    /// Directory
    pub path: PathBuf,
    /// Bytes it held
    pub bytes: u64,
    /// Why
    pub reason: PruneReason,
    /// When its owner ended or last used it, unless it was a leftover
    pub ended_at: Option<DateTime<Utc>>,
}

/// An entry the pass could not remove; a tombstoned one is retried next pass
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneFailure {
    /// Directory
    pub path: PathBuf,
    /// What went wrong
    pub error: String,
}

/// The database file's size and reusable space
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseReport {
    /// Pages in use plus free pages, in bytes
    pub bytes: u64,
    /// Free pages a vacuum would return to the filesystem, in bytes
    pub free_bytes: u64,
    /// Size after `--vacuum`, when it ran
    pub vacuumed_bytes: Option<u64>,
}
