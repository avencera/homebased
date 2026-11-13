//! Cleanup of processes that a run left behind
//!
//! A run's process group is cleaned up by its worker. Work that left the
//! group, such as a double-forked daemon or a child in a new session, still
//! carries the run's `HOMEBASED_TASK_ID` in its environment, so a sweep reads
//! every process's environment and stops the ones that carry the marker. Work
//! that also dropped the marker is tied to the run only by the descendants its
//! worker recorded while the run was alive, see [`DescendantTracker`]
//!
//! A same-user process with an unreadable environment that started at or after
//! the workload start is judged by its ancestry, see [`Sweeper::lineage`].
//! Environments are inherited, so it is run work only if its nearest readable
//! ancestor is: one that carries the marker holds cleanup for a machine check,
//! as does a chain that ends at a process that adopts orphans or cannot be
//! read. A suspect is never signalled. One under a
//! readable unmarked process, such as a sandboxed browser renderer, or under
//! an older process, such as an ssh login's `sshd-session`, is not run work
//! and does not hold cleanup
//!
//! Cleanup reports whether attributable cleanup completed. It is not proof
//! that a GPU is free: a process with a cleared environment, another user's
//! process, or an Apple platform binary (which exposes an empty environment on
//! macOS) cannot be attributed and is never signalled
//!
//! Every target is identified by `(pid, start time)`. Before each signal the
//! sweep rereads both the start time and the marker and skips the process if
//! either changed. On Linux the signal goes through a pidfd opened before that
//! reread, so the checked process is the one signalled. On macOS a small window
//! between the check and `kill` remains; reusing the PID inside it requires the
//! PID space to wrap
//!
//! The calls block on system calls and sleeps for up to
//! [`CleanupTiming::deadline`], so async callers run them on a blocking thread

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::sys::signal::{Signal, killpg};
use nix::unistd::{Pid, geteuid};
use tracing::{info, warn};

use crate::domain::TaskId;
use crate::run_env;

mod descendants;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(test)]
mod tests;

#[cfg(target_os = "linux")]
use linux as os;
#[cfg(target_os = "macos")]
use macos as os;

pub use descendants::DescendantTracker;

/// Kernel start time of a process, in an OS-specific unit
///
/// macOS reports microseconds since the epoch and Linux reports clock ticks
/// since boot. Compare values only on the same machine and in the same OS unit,
/// to identify a process or to bound when run descendants could have started
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct ProcessStartTime(u64);

impl ProcessStartTime {
    /// Wrap a raw value read from storage
    #[must_use]
    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Raw value for storage
    #[must_use]
    pub fn as_raw(self) -> u64 {
        self.0
    }
}

/// One process, told apart from any later process that reuses its PID
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ProcessIdentity {
    /// Process ID
    #[serde(with = "pid_serde")]
    pub pid: Pid,
    /// Kernel start time
    pub start: ProcessStartTime,
}

impl fmt::Display for ProcessIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pid {} started {}", self.pid, self.start.0)
    }
}

/// Read a live process's identity
///
/// # Errors
///
/// `ESRCH` when the process is gone or is a zombie, or the error the kernel
/// returned for the read
pub fn process_identity(pid: Pid) -> io::Result<ProcessIdentity> {
    match os::process_info(pid) {
        Read::Found(info) => Ok(ProcessIdentity {
            pid,
            start: info.start,
        }),
        Read::Exited(_) | Read::Gone => Err(io::Error::from(Errno::ESRCH)),
        Read::Refused(errno) => Err(io::Error::from(errno)),
    }
}

/// Every live process whose `HOMEBASED_TASK_ID` is `marker`
///
/// The same scan a sweep starts with, without signalling anything. Processes
/// whose environment cannot be read, such as another user's, are skipped
///
/// # Errors
///
/// The process table cannot be listed
pub fn marked_processes(marker: TaskId) -> io::Result<Vec<ProcessIdentity>> {
    let mut source = SystemProcesses::new()?;
    let marker = marker.to_string().into_bytes();
    let mut found = Vec::new();
    for pid in source.list()? {
        let Read::Found(environment) = source.environment(pid) else {
            continue;
        };
        if !environment.carries(&marker) {
            continue;
        }
        if let Read::Found(info) = source.info(pid) {
            found.push(ProcessIdentity {
                pid,
                start: info.start,
            });
        }
    }
    Ok(found)
}

/// Consecutive empty scans that finish cleanup even when listed processes
/// keep exiting before inspection, as they do on a busy machine
const BUSY_EMPTY_SCANS: u32 = 8;

/// Deepest ancestor chain a suspect check climbs, a bound against a cycle
/// seen through PID reuse between reads
const MAX_ANCESTRY: usize = 64;

/// Waits and bounds for one cleanup
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupTiming {
    /// How long targets get to exit after SIGTERM before SIGKILL
    pub term_grace: Duration,
    /// How long killed targets get to disappear before the next round
    pub kill_grace: Duration,
    /// Pause between rescans while waiting
    pub poll: Duration,
    /// Most SIGTERM and SIGKILL rounds
    pub rounds: u32,
    /// Bound on the whole cleanup
    pub deadline: Duration,
}

impl CleanupTiming {
    /// 10 seconds of SIGTERM grace, at most 3 rounds or 60 seconds
    pub const STANDARD: Self = Self {
        term_grace: Duration::from_secs(10),
        kill_grace: Duration::from_secs(2),
        poll: Duration::from_millis(250),
        rounds: 3,
        deadline: Duration::from_secs(60),
    };
}

/// Why cleanup could not finish; the resource needs a machine check before release
///
/// It is stored with the resource's `Attention`, so it has a serde form
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CleanupFailure {
    /// The process table could not be read, so an empty result proves nothing
    EnumerationFailed {
        /// The kernel's error
        message: String,
    },
    /// Empty scans did not reach a stable confirmation within the cleanup bounds
    EmptyUnconfirmed,
    /// These targets still carried the marker after the last round
    TargetsSurvived {
        /// Remaining targets
        survivors: Vec<ProcessIdentity>,
    },
    /// Same-user processes started during the run, but their markers could not
    /// be read and their ancestry did not clear them
    UnreadableSuspects {
        /// Processes to check, with the ancestry that held each
        suspects: Vec<Suspect>,
    },
    /// A protected process, such as the daemon or a task worker, carried the
    /// marker; it was never signalled
    ProtectedCarriesMarker {
        /// Protected processes that carried the marker
        #[serde(with = "pids_serde")]
        pids: Vec<Pid>,
    },
    /// A lost worker's process group holds a protected process; it was never signalled
    ProtectedInGroup {
        /// Process group
        #[serde(with = "pid_serde")]
        pgid: Pid,
        /// Protected members
        #[serde(with = "pids_serde")]
        pids: Vec<Pid>,
    },
    /// A lost worker's process group still exists, but neither its leader nor a
    /// readable member ties it to the run
    GroupUnattributed {
        /// Process group
        #[serde(with = "pid_serde")]
        pgid: Pid,
    },
    /// A lost worker's process group still had members after SIGKILL
    GroupSurvived {
        /// Process group
        #[serde(with = "pid_serde")]
        pgid: Pid,
    },
}

/// A PID as its raw number, since `nix` gives `Pid` no serde form
mod pid_serde {
    use nix::unistd::Pid;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(pid: &Pid, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i32(pid.as_raw())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Pid, D::Error> {
        i32::deserialize(deserializer).map(Pid::from_raw)
    }
}

/// A PID list as raw numbers
mod pids_serde {
    use nix::unistd::Pid;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(pids: &[Pid], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(pids.iter().map(|pid| pid.as_raw()))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Pid>, D::Error> {
        let raw = Vec::<i32>::deserialize(deserializer)?;
        Ok(raw.into_iter().map(Pid::from_raw).collect())
    }
}

/// Short command name the kernel keeps for a process: `comm` on Linux and
/// `p_comm` on macOS, both at most 16 bytes
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ProcessName {
    bytes: [u8; 16],
    len: u8,
}

impl ProcessName {
    /// The name up to its first NUL, cut to 16 bytes
    pub(crate) fn new(raw: &[u8]) -> Self {
        let raw = raw.split(|byte| *byte == 0).next().unwrap_or_default();
        let mut name = Self::default();
        let len = raw.len().min(name.bytes.len());
        name.bytes[..len].copy_from_slice(&raw[..len]);
        name.len = u8::try_from(len).unwrap_or(u8::MAX);
        name
    }

    fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

impl fmt::Display for ProcessName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&String::from_utf8_lossy(self.as_bytes()))
    }
}

impl fmt::Debug for ProcessName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", String::from_utf8_lossy(self.as_bytes()))
    }
}

/// A process named in a cleanup report
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct NamedProcess {
    /// PID and kernel start time
    #[serde(flatten)]
    pub identity: ProcessIdentity,
    /// Kernel command name, absent in reports written before names were kept
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl NamedProcess {
    fn new(identity: ProcessIdentity, name: ProcessName) -> Self {
        Self {
            identity,
            name: (!name.as_bytes().is_empty()).then(|| name.to_string()),
        }
    }
}

impl fmt::Display for NamedProcess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.name {
            Some(name) => write!(f, "pid {} ({name})", self.identity.pid),
            None => write!(f, "pid {}", self.identity.pid),
        }
    }
}

/// An unreadable same-user process that holds cleanup
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Suspect {
    /// The process
    #[serde(flatten)]
    pub process: NamedProcess,
    /// What its ancestry showed, absent in reports written before it was kept
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<Lineage>,
}

/// Why an unreadable suspect's ancestry ties it to the run, or cannot clear it
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Lineage {
    /// The nearest readable ancestor carries the run marker, so the suspect
    /// inherited the run's environment
    RunWork {
        /// That ancestor
        ancestor: NamedProcess,
    },
    /// The chain reaches init, the daemon, a worker, or one of the daemon's
    /// own ancestors before a readable process, so the tree was orphaned there
    /// and its origin is lost
    Orphaned {
        /// The adopting process; `None` for init
        adopter: Option<NamedProcess>,
        /// The topmost process of the orphaned tree
        root: NamedProcess,
    },
    /// An ancestor could not be read, or its start time shows its PID was reused
    Unknown {
        /// PID of the ancestor the walk stopped at
        #[serde(with = "pid_serde")]
        at: Pid,
        /// What went wrong
        reason: String,
    },
}

impl fmt::Display for Lineage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RunWork { ancestor } => {
                write!(f, "under {ancestor}, which carries the run marker")
            }
            Self::Orphaned {
                adopter: Some(adopter),
                root,
            } => write!(f, "orphaned tree of {root}, adopted by {adopter}"),
            Self::Orphaned {
                adopter: None,
                root,
            } => write!(f, "orphaned tree of {root}, adopted by init"),
            Self::Unknown { at, reason } => write!(f, "ancestry unknown at pid {at}: {reason}"),
        }
    }
}

impl fmt::Display for CleanupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EnumerationFailed { message } => {
                write!(f, "could not list processes: {message}")
            }
            Self::EmptyUnconfirmed => write!(f, "no stable empty process scan was confirmed"),
            Self::TargetsSurvived { survivors } => {
                write!(f, "{} marked processes survived SIGKILL", survivors.len())
            }
            Self::UnreadableSuspects { suspects } => {
                write!(f, "could not read run markers; check same-user processes")?;
                // a sandboxed tree holds many suspects with one lineage, so
                // each lineage is named once after the processes it holds
                let mut groups: Vec<(Option<&Lineage>, Vec<&Suspect>)> = Vec::new();
                for suspect in suspects {
                    let lineage = suspect.lineage.as_ref();
                    match groups.iter_mut().find(|(seen, _)| *seen == lineage) {
                        Some((_, members)) => members.push(suspect),
                        None => groups.push((lineage, vec![suspect])),
                    }
                }

                for (lineage, members) in groups {
                    let mut separator = "; ";
                    for suspect in members {
                        let process = &suspect.process;
                        write!(
                            f,
                            "{separator}{process} started {}",
                            process.identity.start.0
                        )?;
                        separator = ", ";
                    }

                    if let Some(lineage) = lineage {
                        write!(f, ": {lineage}")?;
                    }
                }

                Ok(())
            }
            Self::ProtectedCarriesMarker { pids } => {
                write!(f, "protected processes {pids:?} carry the run marker")
            }
            Self::ProtectedInGroup { pgid, pids } => {
                write!(f, "process group {pgid} holds protected processes {pids:?}")
            }
            Self::GroupUnattributed { pgid } => write!(
                f,
                "process group {pgid} still exists, but nothing ties it to the run"
            ),
            Self::GroupSurvived { pgid } => {
                write!(f, "process group {pgid} survived SIGKILL")
            }
        }
    }
}

/// What a marker sweep established
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SweepOutcome {
    /// No attributable process carries the marker and no unreadable suspect remains
    Completed {
        /// Targets that were sent at least one signal
        signalled: Vec<ProcessIdentity>,
    },
    /// Cleanup could not finish
    Incomplete(CleanupFailure),
}

/// Why a lost worker's process group was attributed to the run
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupAttribution {
    /// No live member remained, so nothing was signalled
    AlreadyGone,
    /// The recorded leader still had its identity
    Leader,
    /// The leader was gone, and this readable member carried the marker
    MarkedMember(ProcessIdentity),
}

/// What lost-worker group cleanup established
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupOutcome {
    /// The group has no live member left
    Completed(GroupAttribution),
    /// Cleanup could not finish
    Incomplete(CleanupFailure),
}

/// Stop every attributable process whose `HOMEBASED_TASK_ID` is `marker`
///
/// Sends SIGTERM, waits up to [`CleanupTiming::term_grace`] while rescanning,
/// sends SIGKILL to the remaining targets, and rescans, for at most
/// [`CleanupTiming::rounds`] rounds or [`CleanupTiming::deadline`]. Rescans
/// catch descendants that appeared during cleanup. A process in `protected`,
/// and this process, are never signalled; if one carries the marker the
/// outcome is incomplete. With `run_start`, a same-user process whose environment
/// read is refused and whose start is at or after that bound prevents an empty
/// confirmation. Such suspects are never signalled and are reported incomplete
/// if they remain at the cleanup bound. `run_start` must be the recorded workload
/// child's kernel start time on this machine. `None` ignores unreadable processes
#[must_use]
pub fn sweep_marker(
    marker: TaskId,
    run_start: Option<ProcessStartTime>,
    protected: &BTreeSet<Pid>,
    timing: CleanupTiming,
) -> SweepOutcome {
    match SystemProcesses::new() {
        Ok(mut source) => sweep_marker_with(&mut source, marker, run_start, protected, timing),
        Err(error) => SweepOutcome::Incomplete(CleanupFailure::EnumerationFailed {
            message: error.to_string(),
        }),
    }
}

/// Stop what a finished run left running outside its process group
///
/// Targets are the processes that carry `marker` and the recorded descendants
/// in `adopted`, which may have dropped the marker. An adopted process is
/// matched by PID and start time, so a later process that reused its PID is
/// never signalled. Protection, signalling, rescans, and bounds are those of
/// [`sweep_marker`]; unreadable processes are ignored
#[must_use]
pub fn sweep_run(
    marker: TaskId,
    adopted: &BTreeSet<ProcessIdentity>,
    protected: &BTreeSet<Pid>,
    timing: CleanupTiming,
) -> SweepOutcome {
    match SystemProcesses::new() {
        Ok(mut source) => sweep_with(&mut source, marker, None, adopted, protected, timing),
        Err(error) => SweepOutcome::Incomplete(CleanupFailure::EnumerationFailed {
            message: error.to_string(),
        }),
    }
}

/// Stop the process group of a worker that was lost with its child
///
/// The group is signalled only while its recorded leader keeps its identity,
/// or, once the leader is gone, while a readable member carries the marker.
/// A group that holds a protected process, or that nothing ties to the run, is
/// left alone and reported incomplete
#[must_use]
pub fn cleanup_lost_group(
    leader: ProcessIdentity,
    marker: TaskId,
    protected: &BTreeSet<Pid>,
    timing: CleanupTiming,
) -> GroupOutcome {
    match SystemProcesses::new() {
        Ok(mut source) => cleanup_lost_group_with(&mut source, leader, marker, protected, timing),
        Err(error) => GroupOutcome::Incomplete(CleanupFailure::EnumerationFailed {
            message: error.to_string(),
        }),
    }
}

pub(crate) fn sweep_marker_with<S: ProcessSource>(
    source: &mut S,
    marker: TaskId,
    run_start: Option<ProcessStartTime>,
    protected: &BTreeSet<Pid>,
    timing: CleanupTiming,
) -> SweepOutcome {
    sweep_with(
        source,
        marker,
        run_start,
        &BTreeSet::new(),
        protected,
        timing,
    )
}

fn sweep_with<S: ProcessSource>(
    source: &mut S,
    marker: TaskId,
    run_start: Option<ProcessStartTime>,
    adopted: &BTreeSet<ProcessIdentity>,
    protected: &BTreeSet<Pid>,
    timing: CleanupTiming,
) -> SweepOutcome {
    let mut sweeper = Sweeper::new(source, marker, run_start, protected, timing);
    sweeper.adopted = adopted.clone();
    let result = sweeper.sweep();
    let signalled: Vec<_> = sweeper.signalled.iter().copied().collect();
    if let Err(failure) = result {
        warn!(%marker, "cleanup incomplete: {failure}");
        return SweepOutcome::Incomplete(failure);
    }
    if !sweeper.protected_hits.is_empty() {
        let failure = CleanupFailure::ProtectedCarriesMarker {
            pids: sweeper.protected_hits.iter().copied().collect(),
        };
        warn!(%marker, "cleanup incomplete: {failure}");
        return SweepOutcome::Incomplete(failure);
    }
    if !signalled.is_empty() {
        info!(%marker, count = signalled.len(), "Cleanup stopped marked processes");
    }
    SweepOutcome::Completed { signalled }
}

pub(crate) fn cleanup_lost_group_with<S: ProcessSource>(
    source: &mut S,
    leader: ProcessIdentity,
    marker: TaskId,
    protected: &BTreeSet<Pid>,
    timing: CleanupTiming,
) -> GroupOutcome {
    let mut sweeper = Sweeper::new(source, marker, None, protected, timing);
    match sweeper.lost_group(leader) {
        Ok(attribution) => GroupOutcome::Completed(attribution),
        Err(failure) => {
            warn!(%marker, "lost group cleanup incomplete: {failure}");
            GroupOutcome::Incomplete(failure)
        }
    }
}

/// Result of one read about another process
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Read<T> {
    /// The read succeeded, and the process is live
    Found(T),
    /// The kernel confirms an exited process whose PID has not yet been reaped
    /// (a zombie); it holds no resources
    Exited(ProcessIdentity),
    /// The process does not exist
    Gone,
    /// The kernel refused the read
    Refused(Errno),
}

/// What the kernel reports about one live process
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcessInfo {
    pub(crate) start: ProcessStartTime,
    pub(crate) ppid: Pid,
    pub(crate) pgid: Pid,
    pub(crate) uid: u32,
    pub(crate) name: ProcessName,
}

impl ProcessInfo {
    fn named(&self, pid: Pid) -> NamedProcess {
        NamedProcess::new(
            ProcessIdentity {
                pid,
                start: self.start,
            },
            self.name,
        )
    }
}

/// A process's environment block: `KEY=VALUE` entries, each ending in NUL
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Environment(Vec<u8>);

impl Environment {
    pub(crate) fn from_block(block: Vec<u8>) -> Self {
        Self(block)
    }

    fn entries(&self) -> impl Iterator<Item = &[u8]> {
        self.0
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
    }

    /// Whether the block has no entries, as an Apple platform binary's does
    fn is_empty(&self) -> bool {
        self.entries().next().is_none()
    }

    /// Whether any `HOMEBASED_TASK_ID` entry names `marker`
    fn carries(&self, marker: &[u8]) -> bool {
        self.entries().any(|entry| {
            entry
                .strip_prefix(run_env::TASK_ID.as_bytes())
                .and_then(|rest| rest.strip_prefix(b"="))
                .is_some_and(|value| value == marker)
        })
    }
}

/// Process table access, injectable so tests can change what a reread sees
pub(crate) trait ProcessSource {
    /// What a signal is sent through: a pidfd on Linux, the PID on macOS
    type Handle;

    /// Every PID; an error is never an empty table
    fn list(&mut self) -> io::Result<Vec<Pid>>;

    fn info(&mut self, pid: Pid) -> Read<ProcessInfo>;

    fn environment(&mut self, pid: Pid) -> Read<Environment>;

    /// Pin the process so a later send cannot reach a process that reused its PID
    fn pin(&mut self, pid: Pid) -> Read<Self::Handle>;

    fn send(&mut self, handle: &Self::Handle, signal: Signal) -> Result<(), Errno>;

    fn send_group(&mut self, pgid: Pid, signal: Signal) -> Result<(), Errno>;
}

/// The running machine's process table
pub(crate) struct SystemProcesses {
    environment: os::EnvironmentReader,
}

impl SystemProcesses {
    pub(crate) fn new() -> io::Result<Self> {
        Ok(Self {
            environment: os::EnvironmentReader::new()?,
        })
    }
}

impl ProcessSource for SystemProcesses {
    type Handle = os::SignalHandle;

    fn list(&mut self) -> io::Result<Vec<Pid>> {
        let pids = os::list_pids()?;
        // the table always holds this process, so a list without it was not read
        let own = Pid::this();
        if !pids.contains(&own) {
            return Err(io::Error::other(format!(
                "process list of {} entries is missing this process",
                pids.len()
            )));
        }
        Ok(pids)
    }

    fn info(&mut self, pid: Pid) -> Read<ProcessInfo> {
        os::process_info(pid)
    }

    fn environment(&mut self, pid: Pid) -> Read<Environment> {
        self.environment.read(pid)
    }

    fn pin(&mut self, pid: Pid) -> Read<Self::Handle> {
        os::pin(pid)
    }

    fn send(&mut self, handle: &Self::Handle, signal: Signal) -> Result<(), Errno> {
        os::send(handle, signal)
    }

    fn send_group(&mut self, pgid: Pid, signal: Signal) -> Result<(), Errno> {
        killpg(pgid, signal)
    }
}

/// What a live process's environment establishes about the run
#[derive(Debug, Clone, PartialEq, Eq)]
enum Observation {
    /// The process carries the marker or is an adopted descendant
    Marked(ProcessIdentity),
    Suspect(Suspect),
    /// The process exited between enumeration and inspection, so a child it
    /// forked first may be missing from this scan's PID list
    Vanished,
}

/// Marked targets and unreadable suspects seen by one scan
#[derive(Debug, Default)]
struct Scan {
    targets: BTreeMap<Pid, ProcessIdentity>,
    suspects: BTreeMap<ProcessIdentity, Suspect>,
    /// A listed process exited before it was inspected
    unsettled: bool,
    confirmed_empty: bool,
}

impl Scan {
    fn is_clear(&self) -> bool {
        self.confirmed_empty
    }

    fn survivors(&self) -> Vec<ProcessIdentity> {
        self.targets.values().copied().collect()
    }
}

struct Sweeper<'a, S> {
    source: &'a mut S,
    marker: TaskId,
    marker_value: Vec<u8>,
    run_start: Option<ProcessStartTime>,
    /// Recorded run descendants, targets even without the marker
    adopted: BTreeSet<ProcessIdentity>,
    protected: BTreeSet<Pid>,
    timing: CleanupTiming,
    own_uid: u32,
    signalled: BTreeSet<ProcessIdentity>,
    protected_hits: BTreeSet<Pid>,
    /// Same-user refusals already logged, so rescans do not repeat the warning
    warned: BTreeSet<Pid>,
    /// Processes whose adoption of an orphan says nothing about its origin:
    /// this process and its ancestors, read on first use
    adopters: Option<BTreeSet<ProcessIdentity>>,
    /// Start of the current run of consecutive empty scans, and its length
    empty_since: Option<Instant>,
    empty_scans: u32,
    /// Start of the current run of consecutive empty scans in which no listed
    /// process exited before inspection
    settled_since: Option<Instant>,
}

impl<'a, S: ProcessSource> Sweeper<'a, S> {
    fn new(
        source: &'a mut S,
        marker: TaskId,
        run_start: Option<ProcessStartTime>,
        protected: &BTreeSet<Pid>,
        timing: CleanupTiming,
    ) -> Self {
        let mut protected = protected.clone();
        // the sweeping process never signals itself
        protected.insert(Pid::this());
        Self {
            source,
            marker,
            marker_value: marker.to_string().into_bytes(),
            run_start,
            adopted: BTreeSet::new(),
            protected,
            timing,
            own_uid: geteuid().as_raw(),
            signalled: BTreeSet::new(),
            protected_hits: BTreeSet::new(),
            warned: BTreeSet::new(),
            adopters: None,
            empty_since: None,
            empty_scans: 0,
            settled_since: None,
        }
    }

    fn sweep(&mut self) -> Result<(), CleanupFailure> {
        let deadline = Instant::now() + self.timing.deadline;
        let mut scan = self.scan()?;
        for _ in 0..self.timing.rounds {
            if Instant::now() >= deadline {
                break;
            }
            if scan.is_clear() {
                return Ok(());
            }

            let mut termed = BTreeSet::new();
            self.signal_new(&scan, &mut termed, Signal::SIGTERM);
            let until = bounded(self.timing.term_grace, deadline);
            // a descendant that appears during the grace gets SIGTERM too, and
            // SIGKILL with the rest when the grace ends
            scan = self.wait_until_clear(until, |sweeper, scan| {
                sweeper.signal_new(scan, &mut termed, Signal::SIGTERM);
            })?;
            if scan.is_clear() && Instant::now() < deadline {
                return Ok(());
            }

            if Instant::now() >= deadline {
                break;
            }
            self.signal_new(&scan, &mut BTreeSet::new(), Signal::SIGKILL);
            let until = bounded(self.timing.kill_grace, deadline);
            scan = self.wait_until_clear(until, |_, _| {})?;
            if Instant::now() >= deadline {
                break;
            }
        }
        if scan.is_clear() && Instant::now() < deadline {
            return Ok(());
        }
        if !scan.suspects.is_empty() {
            return Err(CleanupFailure::UnreadableSuspects {
                suspects: scan.suspects.into_values().collect(),
            });
        }
        if scan.targets.is_empty() {
            return Err(CleanupFailure::EmptyUnconfirmed);
        }
        Err(CleanupFailure::TargetsSurvived {
            survivors: scan.survivors(),
        })
    }

    /// Rescan until no target remains or `until` passes; `on_scan` sees every rescan
    fn wait_until_clear(
        &mut self,
        until: Instant,
        mut on_scan: impl FnMut(&mut Self, &Scan),
    ) -> Result<Scan, CleanupFailure> {
        loop {
            let now = Instant::now();
            std::thread::sleep(self.timing.poll.min(until.saturating_duration_since(now)));
            let scan = self.scan()?;
            if scan.is_clear() || Instant::now() >= until {
                return Ok(scan);
            }
            on_scan(self, &scan);
        }
    }

    /// Signal each target in `scan` that is not already in `sent`
    fn signal_new(&mut self, scan: &Scan, sent: &mut BTreeSet<ProcessIdentity>, signal: Signal) {
        for target in scan.targets.values() {
            if sent.insert(*target) && self.signal_verified(*target, signal) {
                self.signalled.insert(*target);
            }
        }
    }

    /// Send `signal` only if `target` still has its identity and the marker
    fn signal_verified(&mut self, target: ProcessIdentity, signal: Signal) -> bool {
        let handle = match self.source.pin(target.pid) {
            Read::Found(handle) => handle,
            Read::Exited(_) | Read::Gone => return false,
            Read::Refused(errno) => {
                warn!(marker = %self.marker, %target, "pin before {signal}: {errno}");
                return false;
            }
        };
        if self.observe(target.pid) != Some(Observation::Marked(target)) {
            info!(marker = %self.marker, %target, "Skipped {signal}: identity or marker changed");
            return false;
        }
        match self.source.send(&handle, signal) {
            Ok(()) => true,
            Err(Errno::ESRCH) => false,
            Err(errno) => {
                warn!(marker = %self.marker, %target, "{signal}: {errno}");
                false
            }
        }
    }

    /// Every PID; a failed listing is never an empty table
    fn list(&mut self) -> Result<Vec<Pid>, CleanupFailure> {
        self.source
            .list()
            .map_err(|error| CleanupFailure::EnumerationFailed {
                message: error.to_string(),
            })
    }

    fn scan(&mut self) -> Result<Scan, CleanupFailure> {
        let pids = self.list()?;
        let mut scan = Scan::default();
        for pid in pids {
            let identity = match self.observe(pid) {
                Some(Observation::Marked(identity)) => identity,
                Some(Observation::Suspect(suspect)) => {
                    scan.suspects.insert(suspect.process.identity, suspect);
                    continue;
                }
                Some(Observation::Vanished) => {
                    scan.unsettled = true;
                    continue;
                }
                None => continue,
            };

            if self.protected.contains(&pid) {
                if self.protected_hits.insert(pid) {
                    warn!(marker = %self.marker, %pid, "Protected process carries the run marker");
                }
                continue;
            }
            scan.targets.insert(pid, identity);
        }
        // unreadable suspects cannot prove an empty scan, but they remain
        // separate from targets because their marker cannot authorize a signal
        if !scan.targets.is_empty() || !scan.suspects.is_empty() {
            self.empty_since = None;
            self.empty_scans = 0;
            self.settled_since = None;
            return Ok(scan);
        }

        // a marked parent that forks and exits during a scan leaves its child
        // outside that scan's PID list, but the next scan lists the child. So
        // cleanup finishes on two empty scans a poll apart in which no listed
        // process exited before inspection. On a busy machine short-lived
        // processes exit during almost every scan, so a longer run of empty
        // scans also finishes. A chain that escapes it must replace itself
        // during every one of those scans, and GPU contexts do not survive fork
        let now = Instant::now();
        let empty_since = *self.empty_since.get_or_insert(now);
        self.empty_scans += 1;
        let settled = if scan.unsettled {
            self.settled_since = None;
            false
        } else {
            let since = *self.settled_since.get_or_insert(now);
            now.duration_since(since) >= self.timing.poll
        };
        let busy = self.empty_scans >= BUSY_EMPTY_SCANS
            && now.duration_since(empty_since) >= self.timing.poll;
        scan.confirmed_empty = settled || busy;
        Ok(scan)
    }

    /// A live marked or adopted target, or a same-user unreadable suspect within the run bound
    fn observe(&mut self, pid: Pid) -> Option<Observation> {
        let info = match self.source.info(pid) {
            Read::Found(info) => info,
            Read::Exited(_) | Read::Gone => return Some(Observation::Vanished),
            Read::Refused(_) => return None,
        };
        let identity = ProcessIdentity {
            pid,
            start: info.start,
        };
        // the start time was just read, so a PID reused since it was recorded
        // does not match
        if self.adopted.contains(&identity) {
            return Some(Observation::Marked(identity));
        }
        let environment = match self.source.environment(pid) {
            Read::Found(environment) => environment,
            Read::Exited(_) | Read::Gone => return Some(Observation::Vanished),
            Read::Refused(errno) => {
                return self
                    .note_refusal(pid, info, errno)
                    .map(Observation::Suspect);
            }
        };
        // the identity must still hold after the environment read, or the
        // environment may belong to a process that reused the PID
        if !self.still_same(pid, info) {
            return Some(Observation::Vanished);
        }
        if !environment.carries(&self.marker_value) {
            return None;
        }
        Some(Observation::Marked(ProcessIdentity {
            pid,
            start: info.start,
        }))
    }

    fn still_same(&mut self, pid: Pid, info: ProcessInfo) -> bool {
        match self.source.info(pid) {
            Read::Found(after) if after.start == info.start => true,
            Read::Found(_) | Read::Exited(_) | Read::Gone | Read::Refused(_) => false,
        }
    }

    /// Warn once per same-user refusal and identify suspects within the run bound
    fn note_refusal(&mut self, pid: Pid, info: ProcessInfo, errno: Errno) -> Option<Suspect> {
        // a refused environment read can also mean the process vanished
        if !self.still_same(pid, info) || info.uid != self.own_uid {
            return None;
        }
        let first = self.warned.insert(pid);

        // older processes cannot be run descendants; long-lived agents may
        // deny environment reads, so they must not hold a later run's resource
        let Some(run_start) = self.run_start.filter(|start| info.start >= *start) else {
            if first {
                warn!(marker = %self.marker, %pid, "Same-user environment read refused: {errno}");
            }
            return None;
        };

        let process = info.named(pid);
        match self.lineage(pid, info, run_start) {
            Verdict::Hold(lineage) => {
                if first {
                    warn!(marker = %self.marker, %process, "Unreadable process may be run work ({errno}): {lineage}");
                }

                Some(Suspect {
                    process,
                    lineage: Some(lineage),
                })
            }
            // a sandboxed browser has many such processes, so this stays quiet
            Verdict::Clear(reason) => {
                if first {
                    info!(marker = %self.marker, %process, "Unreadable process is not run work: {reason}");
                }

                None
            }
        }
    }

    /// Whether an unreadable same-user process may be run work, from its ancestry
    ///
    /// Environments are inherited, so the suspect is judged by its nearest
    /// readable ancestor: run work only if that ancestor carries the marker.
    /// Chrome's sandboxed zygote and renderers, and
    /// other setuid or namespace sandboxes, hide their environment under a
    /// readable browser process, which is how an unrelated headless browser
    /// held a GPU. Unreadable links are climbed: other users' processes, as
    /// an ssh login's root `sshd-session` monitor, and same-user sandboxes.
    /// Any link that started before the run clears the suspect, since run
    /// descendants all start after the workload child and only adopters
    /// predate it
    ///
    /// Init, this daemon, a protected worker, and the daemon's same-user
    /// ancestors, such as `systemd --user`, hold the suspect: they adopt orphaned run
    /// work, so reaching one means the readable process that started the tree
    /// is gone. A link that cannot be read, or that started after its child
    /// and so is a later process that reused the parent's PID, holds it too
    ///
    /// Residual risks, which add no machinery because neither is likely:
    /// - run work that handed a launch to an unrelated readable daemon, such
    ///   as a browser automation server started outside the run, or that
    ///   dropped the marker but kept other entries, reads as unmarked. The
    ///   marker sweep has no recorded descendants and already cannot attribute
    ///   such readable work, so its sandboxed children are treated the same; a
    ///   GPU job has no reason to do either
    /// - the adopters are this process's current ancestors. The installed
    ///   daemon always runs under its systemd or launchd unit, which also
    ///   adopts orphaned run work, even across daemon restarts. Only a daemon
    ///   run by hand under a different parent than the run's worker had would
    ///   miss that adopter and could clear an orphaned tree
    fn lineage(&mut self, suspect: Pid, info: ProcessInfo, run_start: ProcessStartTime) -> Verdict {
        let mut child = info.named(suspect);
        let mut parent = info.ppid;
        for _ in 0..MAX_ANCESTRY {
            // init adopts orphaned run work, so it attributes nothing; macOS
            // reports PID 0 as the parent of launchd
            if parent.as_raw() <= 1 {
                return Verdict::Hold(Lineage::Orphaned {
                    adopter: None,
                    root: child,
                });
            }

            let info = match self.source.info(parent) {
                Read::Found(info) => info,
                Read::Exited(_) | Read::Gone => return Verdict::unknown(parent, "exited"),
                Read::Refused(errno) => {
                    return Verdict::unknown(parent, format!("read refused: {errno}"));
                }
            };
            let ancestor = info.named(parent);
            // a parent always starts no later than its child
            if info.start > child.identity.start {
                return Verdict::unknown(parent, "started after its child; PID reused");
            }
            if self.adopts_orphans(ancestor.identity) {
                return Verdict::Hold(Lineage::Orphaned {
                    adopter: Some(ancestor),
                    root: child,
                });
            }

            let older = info.start < run_start;
            if info.uid != self.own_uid {
                if older {
                    return Verdict::Clear(Cleared::Older(ancestor));
                }
                child = ancestor;
                parent = info.ppid;
                continue;
            }

            // an empty block says nothing about what was inherited: macOS
            // shows one for platform binaries such as a run's `/bin/bash`, and
            // `env -i` makes one, so it is climbed like a refused read
            let environment = match self.source.environment(parent) {
                Read::Found(environment) if !environment.is_empty() => Some(environment),
                Read::Found(_) | Read::Refused(_) => None,
                Read::Exited(_) | Read::Gone => return Verdict::unknown(parent, "exited"),
            };
            let Some(environment) = environment else {
                if older {
                    return Verdict::Clear(Cleared::Older(ancestor));
                }
                child = ancestor;
                parent = info.ppid;
                continue;
            };
            if !self.still_same(parent, info) {
                return Verdict::unknown(parent, "exited");
            }
            if environment.carries(&self.marker_value) {
                return Verdict::Hold(Lineage::RunWork { ancestor });
            }
            return Verdict::Clear(Cleared::Unmarked(ancestor));
        }

        Verdict::unknown(parent, format!("ancestry deeper than {MAX_ANCESTRY}"))
    }

    /// Whether `ancestor` is a protected process or this process's same-user ancestor
    fn adopts_orphans(&mut self, ancestor: ProcessIdentity) -> bool {
        if self.protected.contains(&ancestor.pid) {
            return true;
        }
        if self.adopters.is_none() {
            self.adopters = Some(self.own_ancestry());
        }

        self.adopters
            .as_ref()
            .is_some_and(|adopters| adopters.contains(&ancestor))
    }

    /// This process and its live same-user ancestors below init
    ///
    /// Other users' ancestors are left out: a daemon run by hand inside an ssh
    /// login sits under root `sshd`, which adopts no orphans, and counting it
    /// would hold every ssh login made during a run
    fn own_ancestry(&mut self) -> BTreeSet<ProcessIdentity> {
        let mut ancestry = BTreeSet::new();
        let mut pid = Pid::this();
        for _ in 0..MAX_ANCESTRY {
            if pid.as_raw() <= 1 {
                break;
            }
            let Read::Found(info) = self.source.info(pid) else {
                break;
            };
            if info.uid == self.own_uid {
                ancestry.insert(ProcessIdentity {
                    pid,
                    start: info.start,
                });
            }
            pid = info.ppid;
        }

        ancestry
    }

    fn lost_group(&mut self, leader: ProcessIdentity) -> Result<GroupAttribution, CleanupFailure> {
        let deadline = Instant::now() + self.timing.deadline;
        let pgid = leader.pid;
        let Some(attribution) = self.attribute_group(leader)? else {
            return Ok(GroupAttribution::AlreadyGone);
        };

        for (signal, grace) in [
            (Signal::SIGTERM, self.timing.term_grace),
            (Signal::SIGKILL, self.timing.kill_grace),
        ] {
            // attribution is rechecked before every signal: the leader may have
            // exited and its PID been reused since the last check
            if self.attribute_group(leader)?.is_none() {
                return Ok(attribution);
            }
            match self.source.send_group(pgid, signal) {
                Ok(()) => {}
                Err(Errno::ESRCH) => return Ok(attribution),
                Err(errno) => warn!(marker = %self.marker, %pgid, "{signal} group: {errno}"),
            }
            if self.wait_group_gone(pgid, bounded(grace, deadline))? {
                return Ok(attribution);
            }
        }
        Err(CleanupFailure::GroupSurvived { pgid })
    }

    /// Why the group may be signalled, or `None` when it has no live member
    fn attribute_group(
        &mut self,
        leader: ProcessIdentity,
    ) -> Result<Option<GroupAttribution>, CleanupFailure> {
        let pgid = leader.pid;
        let members = self.group_members(pgid)?;
        if members.is_empty() {
            return Ok(None);
        }
        let protected: Vec<_> = members
            .iter()
            .filter(|member| self.protected.contains(member))
            .copied()
            .collect();
        if !protected.is_empty() {
            return Err(CleanupFailure::ProtectedInGroup {
                pgid,
                pids: protected,
            });
        }
        if let Read::Found(info) = self.source.info(leader.pid)
            && info.start == leader.start
            && info.pgid == pgid
        {
            return Ok(Some(GroupAttribution::Leader));
        }
        for member in members {
            if let Some(Observation::Marked(identity)) = self.observe(member) {
                return Ok(Some(GroupAttribution::MarkedMember(identity)));
            }
        }
        Err(CleanupFailure::GroupUnattributed { pgid })
    }

    /// Live processes in the group
    fn group_members(&mut self, pgid: Pid) -> Result<Vec<Pid>, CleanupFailure> {
        let pids = self.list()?;
        Ok(pids
            .into_iter()
            .filter(|pid| matches!(self.source.info(*pid), Read::Found(info) if info.pgid == pgid))
            .collect())
    }

    fn wait_group_gone(&mut self, pgid: Pid, until: Instant) -> Result<bool, CleanupFailure> {
        loop {
            let now = Instant::now();
            std::thread::sleep(self.timing.poll.min(until.saturating_duration_since(now)));
            if self.group_members(pgid)?.is_empty() {
                return Ok(true);
            }
            if Instant::now() >= until {
                return Ok(false);
            }
        }
    }
}

/// What an unreadable suspect's ancestry established
#[derive(Debug)]
enum Verdict {
    /// The suspect may be run work
    Hold(Lineage),
    /// The suspect is not run work
    Clear(Cleared),
}

impl Verdict {
    fn unknown(at: Pid, reason: impl Into<String>) -> Self {
        Self::Hold(Lineage::Unknown {
            at,
            reason: reason.into(),
        })
    }
}

/// Why an unreadable suspect is not run work
#[derive(Debug)]
enum Cleared {
    /// An ancestor that adopts no orphans started before the run
    Older(NamedProcess),
    /// The nearest readable ancestor does not carry the marker
    Unmarked(NamedProcess),
}

impl fmt::Display for Cleared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Older(ancestor) => write!(f, "under {ancestor}, which predates the run"),
            Self::Unmarked(ancestor) => {
                write!(f, "under {ancestor}, which does not carry the run marker")
            }
        }
    }
}

/// The earlier of `now + wait` and `deadline`
fn bounded(wait: Duration, deadline: Instant) -> Instant {
    (Instant::now() + wait).min(deadline)
}
