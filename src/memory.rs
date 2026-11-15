//! Host memory admission: start work with a declared peak only while it fits
//!
//! Every task and GPU queue run shares the daemon's cgroup and the
//! `run-*.scope` cgroups that tasks start beside it, so one item's peak can
//! OOM the others. An item submitted with a peak starts only when the memory
//! in use, plus the room already promised to items that started or wait ahead
//! of it, plus its own peak, stays at or under a bar.
//!
//! The pieces here do no scheduling. [`Gauge`] reads what is in use,
//! [`outstanding`] measures how much of their peak running items have not
//! allocated yet, and [`Room`] hands out what is left in the caller's order.
//! The daemon's memory actor decides that order

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

/// Memory an item may use at its peak, in whole MiB
///
/// Written as a number of GiB, which may be fractional, and stored rounded up
/// to the next MiB. It serializes back as the GiB that MiB count is, which
/// is exact, so a spec's canonical form and digest are stable
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemPeak(u64);

impl MemPeak {
    /// Parse a number of GiB, with an optional `G` or `GiB` suffix
    pub fn parse(text: &str) -> Result<Self, String> {
        let trimmed = text.trim();
        let number = trimmed
            .strip_suffix("GiB")
            .or_else(|| trimmed.strip_suffix('G'))
            .unwrap_or(trimmed);
        let gib: f64 = number
            .trim()
            .parse()
            .map_err(|_| format!("`{text}` is not a number of GiB, such as 17 or 2.5"))?;
        Self::from_gib(gib)
    }

    /// The peak for a number of GiB
    pub fn from_gib(gib: f64) -> Result<Self, String> {
        // a terabyte peak is a typo, and the bound keeps the cast below exact
        if !gib.is_finite() || gib <= 0.0 || gib > 1024.0 {
            return Err(format!(
                "a memory peak must be more than 0 and at most 1024 GiB, not {gib}"
            ));
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let mib = (gib * 1024.0).ceil() as u64;
        Ok(Self(mib))
    }

    /// The peak for a stored MiB count
    pub fn from_mib(mib: u64) -> Result<Self, String> {
        if mib == 0 || mib > 1024 * 1024 {
            return Err(format!("a memory peak of {mib} MiB is out of range"));
        }
        Ok(Self(mib))
    }

    /// The peak in MiB, its stored form
    #[must_use]
    pub fn mib(self) -> u64 {
        self.0
    }

    /// The MiB count as SQLite stores it; `from_mib` bounds it far below `i64::MAX`
    #[must_use]
    #[allow(clippy::cast_possible_wrap)]
    pub fn stored(self) -> i64 {
        self.0 as i64
    }

    /// The peak in bytes
    #[must_use]
    pub fn bytes(self) -> u64 {
        self.0 * MIB
    }

    /// The peak in GiB
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn gib(self) -> f64 {
        self.0 as f64 / 1024.0
    }
}

impl fmt::Display for MemPeak {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Gib(self.bytes()))
    }
}

impl std::str::FromStr for MemPeak {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::parse(text)
    }
}

impl Serialize for MemPeak {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_f64(self.gib())
    }
}

impl<'de> Deserialize<'de> for MemPeak {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let gib = f64::deserialize(deserializer)?;
        Self::from_gib(gib).map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for MemPeak {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "MemPeak".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "number",
            "exclusiveMinimum": 0,
            "maximum": 1024,
            "description": "Peak host memory in GiB"
        })
    }
}

/// A byte count shown in GiB with one decimal
#[derive(Debug, Clone, Copy)]
pub struct Gib(pub u64);

impl fmt::Display for Gib {
    #[allow(clippy::cast_precision_loss)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.1}G", self.0 as f64 / GIB as f64)
    }
}

/// Why an item with a peak has not started
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemoryWait {
    /// Its peak does not fit the room left under the bar
    Room {
        /// Its peak
        peak: MemPeak,
        /// Bytes left for it under the bar, after the items ahead of it
        free: u64,
        /// The bar in bytes
        bar: u64,
    },
    /// The memory in use could not be read
    Unreadable {
        /// What failed
        error: String,
    },
}

impl fmt::Display for MemoryWait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Room { peak, free, bar } => write!(
                f,
                "waiting for memory: peak {peak}, {} free under the {} bar",
                Gib(*free),
                Gib(*bar)
            ),
            Self::Unreadable { error } => {
                write!(f, "waiting for memory: memory use is unreadable: {error}")
            }
        }
    }
}

/// The room left under the bar, handed out in the caller's order
///
/// An item that fits takes its peak. An item that does not fit reserves its
/// peak anyway, so no item behind it can start into the room it waits for.
/// The room can go below zero, and then nothing behind it starts
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Room {
    /// What is in use was read
    Known {
        /// Bytes left under the bar; negative when over it
        free: i128,
        /// The bar in bytes
        bar: u64,
    },
    /// What is in use could not be read, so no item with a peak starts
    Unreadable {
        /// What failed
        error: String,
    },
}

impl Room {
    /// The room under `bar` with `used` bytes in use and `outstanding` bytes
    /// promised to started items that have not allocated them yet
    #[must_use]
    pub fn new(bar: u64, used: u64, outstanding: u64) -> Self {
        Self::Known {
            free: i128::from(bar) - i128::from(used) - i128::from(outstanding),
            bar,
        }
    }

    /// Take room for an item, or reserve it and say why the item waits
    pub fn admit(&mut self, peak: MemPeak) -> Result<(), MemoryWait> {
        match self {
            Self::Known { free, bar } => {
                let before = *free;
                *free -= i128::from(peak.bytes());
                if i128::from(peak.bytes()) <= before {
                    return Ok(());
                }
                Err(MemoryWait::Room {
                    peak,
                    free: u64::try_from(before.max(0)).unwrap_or(u64::MAX),
                    bar: *bar,
                })
            }
            Self::Unreadable { error } => Err(MemoryWait::Unreadable {
                error: error.clone(),
            }),
        }
    }

    /// Whether an item of `peak` would fit once `freed` more bytes come back
    #[must_use]
    pub fn fits_after(&self, peak: MemPeak, freed: u64) -> bool {
        match self {
            Self::Known { free, .. } => i128::from(peak.bytes()) <= free + i128::from(freed),
            Self::Unreadable { .. } => false,
        }
    }

    /// Remove room that items ahead of the caller's reserved
    pub fn reserve(&mut self, bytes: u64) {
        if let Self::Known { free, .. } = self {
            *free -= i128::from(bytes);
        }
    }
}

/// An item with a peak that started and has not ended
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Claim {
    /// Its declared peak
    pub peak: MemPeak,
    /// Its child process, once spawned; `None` counts the whole peak as outstanding
    pub root: Option<u32>,
}

/// Memory in use, and the room started items still need, from one measurement
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Usage {
    /// Bytes in use, as [`Gauge::read`] counts them
    pub used: u64,
    /// Bytes of their peaks that started items have not allocated yet
    pub outstanding: u64,
}

/// Bytes of their peaks that started items have not allocated yet, given
/// what each one's process tree holds
///
/// The gauge sees only what an item has allocated, so an item that just
/// started still needs most of its peak. Each item counts its peak less what
/// it holds, never less than zero
fn outstanding(claims: &[Claim], held: &[u64]) -> u64 {
    claims
        .iter()
        .zip(held)
        .map(|(claim, held)| claim.peak.bytes().saturating_sub(*held))
        .sum()
}

/// What each claim's process tree holds now; one without a process holds nothing
fn held_by(claims: &[Claim]) -> Vec<u64> {
    claims
        .iter()
        .map(|claim| claim.root.map_or(0, tree_resident))
        .collect()
}

/// Where the memory in use is read
///
/// The default is the daemon's own cgroup, read from `/proc/self/cgroup`,
/// which is `homebased.service` under systemd. Every `run-*.scope` beside it
/// counts too: a task that starts a `systemd-run --scope` moves that process
/// out of the daemon's cgroup but not out of its machine's memory
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gauge {
    cgroup: Option<PathBuf>,
}

impl Gauge {
    /// A gauge on `cgroup`, or on the daemon's own cgroup when `None`
    #[must_use]
    pub fn new(cgroup: Option<PathBuf>) -> Self {
        Self {
            cgroup: cgroup.or_else(own_cgroup),
        }
    }

    /// The cgroup directory this gauge reads, when there is one
    #[must_use]
    pub fn cgroup(&self) -> Option<&Path> {
        self.cgroup.as_deref()
    }

    /// Memory in use and the room `claims` still need, read together
    ///
    /// The cgroup and each claim's process tree cannot be read at one
    /// instant. Each claim's residence is read before and after the cgroup,
    /// and the lower one counts, so memory a worker allocates or frees between
    /// the reads is at worst counted twice, never missed
    pub fn measure(&self, claims: &[Claim]) -> Result<Usage, String> {
        let before = held_by(claims);
        let used = self.read()?;
        let after = held_by(claims);
        let held: Vec<u64> = before
            .into_iter()
            .zip(after)
            .map(|(a, b)| a.min(b))
            .collect();
        Ok(Usage {
            used,
            outstanding: outstanding(claims, &held),
        })
    }

    /// Bytes in use: `memory.current` less its file pages other than shmem,
    /// for the cgroup and each `run-*.scope` beside it
    ///
    /// The kernel reclaims page cache, active or inactive, before it
    /// OOM-kills anything under the cgroup's cap, so counting it would hold
    /// work back for memory that is free in practice; data reads can fill
    /// most of the cgroup with active cache. Shmem (tmpfs) and unevictable
    /// (mlocked) pages cannot be reclaimed, so they stay counted
    pub fn read(&self) -> Result<u64, String> {
        let cgroup = self
            .cgroup
            .as_deref()
            .ok_or("this machine has no cgroup v2 memory controller")?;
        let mut used = cgroup_used(cgroup)?;
        let Some(parent) = cgroup.parent() else {
            return Ok(used);
        };
        let Ok(entries) = fs::read_dir(parent) else {
            return Ok(used);
        };
        for entry in entries.flatten() {
            // a configured cgroup that is itself a scope is counted above
            if entry.path() == cgroup {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("run-") && name.ends_with(".scope") {
                // a scope can end between the listing and the read
                used += cgroup_used(&entry.path()).unwrap_or(0);
            }
        }
        Ok(used)
    }
}

fn cgroup_used(dir: &Path) -> Result<u64, String> {
    let read = |name: &str| {
        fs::read_to_string(dir.join(name))
            .map_err(|error| format!("read {}: {error}", dir.join(name).display()))
    };
    let current = || -> Result<u64, String> {
        read("memory.current")?
            .trim()
            .parse()
            .map_err(|error| format!("parse {}/memory.current: {error}", dir.display()))
    };

    // the two files cannot be read at one instant, so `memory.current` is
    // read on both sides of `memory.stat` and the higher reading kept: a
    // burst between the reads can then only overcount, never let the
    // reclaimable estimate cancel memory that `current` has not seen yet
    let before = current()?;
    let stat = read("memory.stat")?;
    let current = before.max(current()?);
    let field = |key: &str| {
        stat.lines()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix(' '))
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    // `file` counts shmem too, which reclaim cannot free, and mlocked file
    // pages; `unevictable` does not say which of its pages are file pages, so
    // leaving all of it counted can only understate what reclaim frees
    let reclaimable = field("file")
        .saturating_sub(field("shmem"))
        .saturating_sub(field("unevictable"));
    Ok(current.saturating_sub(reclaimable))
}

/// The daemon's cgroup v2 directory, when it has one with a memory controller
fn own_cgroup() -> Option<PathBuf> {
    let text = fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = text.lines().find_map(|line| line.strip_prefix("0::"))?;
    let dir = Path::new("/sys/fs/cgroup").join(relative.trim().trim_start_matches('/'));
    dir.join("memory.current").exists().then_some(dir)
}

/// Resident memory of `root` and every descendant
///
/// Children come from `/proc/<pid>/task/<tid>/children`, so a walk reads
/// only the tree, not every process on the machine. A process that exits
/// mid-walk counts nothing
fn tree_resident(root: u32) -> u64 {
    let mut total = 0;
    let mut frontier = vec![root];
    let mut seen = std::collections::HashSet::new();
    while let Some(pid) = frontier.pop() {
        if !seen.insert(pid) {
            continue;
        }
        total += resident_bytes(pid);
        frontier.extend(children(pid));
    }
    total
}

fn children(pid: u32) -> Vec<u32> {
    let Ok(threads) = fs::read_dir(format!("/proc/{pid}/task")) else {
        return Vec::new();
    };
    threads
        .flatten()
        .filter_map(|thread| fs::read_to_string(thread.path().join("children")).ok())
        .flat_map(|list| {
            list.split_whitespace()
                .filter_map(|child| child.parse().ok())
                .collect::<Vec<u32>>()
        })
        .collect()
}

/// A process's share of its anonymous and shared-memory resident bytes
///
/// Proportional counts split a page among every process that maps it, so
/// forked workers that still share their parent's pages are not counted once
/// each, which would leave too little room promised. Kernels without the
/// proportional fields fall back to resident counts. File-backed pages are
/// left out: the gauge already subtracts the reclaimable ones, and a smaller
/// count here only keeps more room promised
fn resident_bytes(pid: u32) -> u64 {
    let proportional = fs::read_to_string(format!("/proc/{pid}/smaps_rollup"))
        .ok()
        .and_then(|text| Some(kib(&text, "Pss_Anon:")? + kib(&text, "Pss_Shmem:")?));
    let kib_total = proportional.unwrap_or_else(|| {
        let status = fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
        kib(&status, "RssAnon:").unwrap_or(0) + kib(&status, "RssShmem:").unwrap_or(0)
    });
    kib_total * 1024
}

/// The kB value of the `key` line of a procfs file
fn kib(text: &str, key: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| line.strip_prefix(key))
        .and_then(|value| value.trim().trim_end_matches("kB").trim().parse().ok())
}

#[cfg(test)]
mod tests {
    use super::{Claim, Gauge, MemPeak, MemoryWait, Room, cgroup_used, kib, outstanding};

    fn gib(value: f64) -> MemPeak {
        MemPeak::from_gib(value).unwrap()
    }

    #[test]
    fn peaks_round_up_to_a_mib_and_serialize_back_exactly() {
        let peak = MemPeak::parse("1.3G").unwrap();
        assert_eq!(peak.bytes(), 1332 << 20);
        let json = serde_json::to_value(peak).unwrap();
        assert_eq!(serde_json::from_value::<MemPeak>(json).unwrap(), peak);
        assert_eq!(MemPeak::parse("17").unwrap(), gib(17.0));
        assert_eq!(MemPeak::parse("2.5GiB").unwrap(), gib(2.5));
        assert!(MemPeak::parse("0").is_err());
        assert!(MemPeak::parse("lots").is_err());
    }

    #[test]
    fn a_waiting_item_reserves_its_room_from_every_item_behind_it() {
        let gib_bytes = 1 << 30;
        let mut room = Room::new(40 * gib_bytes, 15 * gib_bytes, 0);
        room.admit(gib(10.0)).unwrap();
        // 15 left: the 20 GiB head waits and reserves, so 4 GiB cannot slip in
        let head = room.admit(gib(20.0)).unwrap_err();
        assert_eq!(
            head,
            MemoryWait::Room {
                peak: gib(20.0),
                free: 15 * gib_bytes,
                bar: 40 * gib_bytes,
            }
        );
        assert!(room.admit(gib(4.0)).is_err());
    }

    #[test]
    fn an_unreadable_gauge_starts_nothing() {
        let mut room = Room::Unreadable {
            error: "gone".into(),
        };
        assert!(matches!(
            room.admit(gib(1.0)),
            Err(MemoryWait::Unreadable { .. })
        ));
    }

    #[test]
    fn a_claim_without_a_process_counts_its_whole_peak() {
        let claims = [Claim {
            peak: gib(3.0),
            root: None,
        }];
        assert_eq!(super::held_by(&claims), [0]);
        assert_eq!(outstanding(&claims, &[0]), 3 << 30);
        // a claim counts the lower of its two readings
        assert_eq!(outstanding(&claims, &[1 << 30]), 2 << 30);
    }

    #[test]
    fn proportional_fields_are_read_from_smaps_rollup_text() {
        let rollup = "Rss:             2048 kB\nPss:             1100 kB\n\
                      Pss_Anon:         900 kB\nPss_File:         150 kB\n\
                      Pss_Shmem:         50 kB\n";
        assert_eq!(kib(rollup, "Pss_Anon:"), Some(900));
        assert_eq!(kib(rollup, "Pss_Shmem:"), Some(50));
        // an older kernel without the split fields falls back to the status counts
        assert_eq!(kib("Pss: 1100 kB\n", "Pss_Anon:"), None);
    }

    #[test]
    fn use_leaves_out_active_and_inactive_page_cache_but_keeps_shmem() {
        let dir = tempfile::tempdir().unwrap();
        let gib = 1u64 << 30;
        // the 11-14 reading: 31 GiB charged, 25.5 GiB of it active cache
        std::fs::write(dir.path().join("memory.current"), (31 * gib).to_string()).unwrap();
        std::fs::write(
            dir.path().join("memory.stat"),
            format!(
                "anon {}\nfile {}\nshmem {}\nfile_mapped 0\nactive_file {}\ninactive_file {}\n",
                9 * gib,
                22 * gib,
                gib,
                19 * gib,
                2 * gib,
            ),
        )
        .unwrap();
        // 31 less the 21 GiB of file pages that are not shmem
        assert_eq!(cgroup_used(dir.path()).unwrap(), 10 * gib);
    }

    #[test]
    fn unevictable_pages_stay_counted() {
        let dir = tempfile::tempdir().unwrap();
        let gib = 1u64 << 30;
        std::fs::write(dir.path().join("memory.current"), (10 * gib).to_string()).unwrap();
        std::fs::write(
            dir.path().join("memory.stat"),
            format!("file {}\nshmem {}\nunevictable {}\n", 6 * gib, gib, 2 * gib),
        )
        .unwrap();
        // 10 less the 6 GiB of file pages, minus 1 of shmem and 2 locked
        assert_eq!(cgroup_used(dir.path()).unwrap(), 7 * gib);
    }

    #[test]
    fn a_configured_scope_is_counted_once() {
        let dir = tempfile::tempdir().unwrap();
        let gib = 1u64 << 30;
        let write = |name: &str, used: u64| {
            let scope = dir.path().join(name);
            std::fs::create_dir(&scope).unwrap();
            std::fs::write(scope.join("memory.current"), used.to_string()).unwrap();
            std::fs::write(scope.join("memory.stat"), "file 0\n").unwrap();
            scope
        };
        let measured = write("run-example.scope", 25 * gib);
        write("run-sibling.scope", gib);

        assert_eq!(Gauge::new(Some(measured)).read().unwrap(), 26 * gib);
    }
}
