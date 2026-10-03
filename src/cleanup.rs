//! Cleanup of processes that a run left behind
//!
//! A run's process group is cleaned up by its worker. Work that left the
//! group, such as a double-forked daemon or a child in a new session, still
//! carries the run's `HOMEBASED_TASK_ID` in its environment, so a sweep reads
//! every process's environment and stops the ones that carry the marker.
//!
//! Cleanup reports whether attributable cleanup completed. It is not proof
//! that a GPU is free: a process with a cleared environment, another user's
//! process, or an Apple platform binary (which exposes an empty environment on
//! macOS) cannot be attributed and is never signalled.
//!
//! Every target is identified by `(pid, start time)`. Before each signal the
//! sweep rereads both the start time and the marker and skips the process if
//! either changed. On Linux the signal goes through a pidfd opened before that
//! reread, so the checked process is the one signalled. On macOS a small window
//! between the check and `kill` remains; reusing the PID inside it requires the
//! PID space to wrap.
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

/// Kernel start time of a process, in an OS-specific unit
///
/// macOS reports microseconds since the epoch and Linux reports clock ticks
/// since boot. Values are compared only for equality on the same machine, to
/// tell a process apart from a later one that reused its PID
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessIdentity {
    /// Process ID
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
        Read::Found(info) if !info.zombie => Ok(ProcessIdentity {
            pid,
            start: info.start,
        }),
        Read::Found(_) | Read::Gone => Err(io::Error::from(Errno::ESRCH)),
        Read::Refused(errno) => Err(io::Error::from(errno)),
    }
}

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

impl Default for CleanupTiming {
    fn default() -> Self {
        Self::STANDARD
    }
}

/// Why cleanup could not finish; the resource needs a person to check the machine
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupFailure {
    /// The process table could not be read, so an empty result proves nothing
    EnumerationFailed {
        /// The kernel's error
        message: String,
    },
    /// These targets still carried the marker after the last round
    TargetsSurvived {
        /// Remaining targets
        survivors: Vec<ProcessIdentity>,
    },
    /// A protected process, such as the daemon or a task worker, carried the
    /// marker; it was never signalled
    ProtectedCarriesMarker {
        /// Protected processes that carried the marker
        pids: Vec<Pid>,
    },
    /// A lost worker's process group holds a protected process; it was never signalled
    ProtectedInGroup {
        /// Process group
        pgid: Pid,
        /// Protected members
        pids: Vec<Pid>,
    },
    /// A lost worker's process group still exists, but neither its leader nor a
    /// readable member ties it to the run
    GroupUnattributed {
        /// Process group
        pgid: Pid,
    },
    /// A lost worker's process group still had members after SIGKILL
    GroupSurvived {
        /// Process group
        pgid: Pid,
    },
}

impl fmt::Display for CleanupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EnumerationFailed { message } => {
                write!(f, "could not list processes: {message}")
            }
            Self::TargetsSurvived { survivors } => {
                write!(f, "{} marked processes survived SIGKILL", survivors.len())
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
    /// No attributable process carries the marker any more
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
/// outcome is incomplete
#[must_use]
pub fn sweep_marker(
    marker: TaskId,
    protected: &BTreeSet<Pid>,
    timing: CleanupTiming,
) -> SweepOutcome {
    match SystemProcesses::new() {
        Ok(mut source) => sweep_marker_with(&mut source, marker, protected, timing),
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
    protected: &BTreeSet<Pid>,
    timing: CleanupTiming,
) -> SweepOutcome {
    let mut sweeper = Sweeper::new(source, marker, protected, timing);
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
    let mut sweeper = Sweeper::new(source, marker, protected, timing);
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
    /// The read succeeded
    Found(T),
    /// The process does not exist
    Gone,
    /// The kernel refused the read
    Refused(Errno),
}

/// What the kernel reports about one process
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcessInfo {
    pub(crate) start: ProcessStartTime,
    pub(crate) pgid: Pid,
    pub(crate) uid: u32,
    /// An exited process that its parent has not reaped; it holds no resources
    pub(crate) zombie: bool,
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

/// Marked processes seen by one scan, keyed by PID
#[derive(Debug, Default)]
struct Scan {
    targets: BTreeMap<Pid, ProcessIdentity>,
}

impl Scan {
    fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    fn survivors(&self) -> Vec<ProcessIdentity> {
        self.targets.values().copied().collect()
    }
}

struct Sweeper<'a, S> {
    source: &'a mut S,
    marker: TaskId,
    marker_value: Vec<u8>,
    protected: BTreeSet<Pid>,
    timing: CleanupTiming,
    own_uid: u32,
    signalled: BTreeSet<ProcessIdentity>,
    protected_hits: BTreeSet<Pid>,
    /// Same-user refusals already logged, so rescans do not repeat the warning
    warned: BTreeSet<Pid>,
}

impl<'a, S: ProcessSource> Sweeper<'a, S> {
    fn new(
        source: &'a mut S,
        marker: TaskId,
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
            protected,
            timing,
            own_uid: geteuid().as_raw(),
            signalled: BTreeSet::new(),
            protected_hits: BTreeSet::new(),
            warned: BTreeSet::new(),
        }
    }

    fn sweep(&mut self) -> Result<(), CleanupFailure> {
        let deadline = Instant::now() + self.timing.deadline;
        let mut scan = self.scan()?;
        for _ in 0..self.timing.rounds {
            if scan.is_empty() {
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
            if scan.is_empty() {
                return Ok(());
            }

            self.signal_new(&scan, &mut BTreeSet::new(), Signal::SIGKILL);
            let until = bounded(self.timing.kill_grace, deadline);
            scan = self.wait_until_clear(until, |_, _| {})?;
            if Instant::now() >= deadline {
                break;
            }
        }
        if scan.is_empty() {
            return Ok(());
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
            if scan.is_empty() || Instant::now() >= until {
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
            Read::Gone => return false,
            Read::Refused(errno) => {
                warn!(marker = %self.marker, %target, "pin before {signal}: {errno}");
                return false;
            }
        };
        if self.observe(target.pid) != Some(target) {
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

    fn scan(&mut self) -> Result<Scan, CleanupFailure> {
        let pids = self
            .source
            .list()
            .map_err(|error| CleanupFailure::EnumerationFailed {
                message: error.to_string(),
            })?;
        let mut scan = Scan::default();
        for pid in pids {
            let Some(identity) = self.observe(pid) else {
                continue;
            };
            if self.protected.contains(&pid) {
                if self.protected_hits.insert(pid) {
                    warn!(marker = %self.marker, %pid, "Protected process carries the run marker");
                }
                continue;
            }
            scan.targets.insert(pid, identity);
        }
        Ok(scan)
    }

    /// Identity of `pid` if it is live and its readable environment carries the marker
    fn observe(&mut self, pid: Pid) -> Option<ProcessIdentity> {
        let info = match self.source.info(pid) {
            Read::Found(info) if !info.zombie => info,
            Read::Found(_) | Read::Gone | Read::Refused(_) => return None,
        };
        let environment = match self.source.environment(pid) {
            Read::Found(environment) => environment,
            Read::Gone => return None,
            Read::Refused(errno) => {
                self.note_refusal(pid, info, errno);
                return None;
            }
        };
        // Apple platform binaries expose an empty environment, so they never
        // carry the marker; such a process cannot be attributed, which is a
        // documented escape
        if !environment.carries(&self.marker_value) {
            return None;
        }
        // the identity must still hold after the environment read, or the
        // environment may belong to a process that reused the PID
        match self.source.info(pid) {
            Read::Found(after) if !after.zombie && after.start == info.start => {
                Some(ProcessIdentity {
                    pid,
                    start: info.start,
                })
            }
            Read::Found(_) | Read::Gone | Read::Refused(_) => None,
        }
    }

    /// Other users' processes are out of scope; a same-user refusal is unexpected
    fn note_refusal(&mut self, pid: Pid, info: ProcessInfo, errno: Errno) {
        if info.uid != self.own_uid || !self.warned.insert(pid) {
            return;
        }
        // the process may have exited between the reads, which is not a refusal
        if !matches!(self.source.info(pid), Read::Found(after) if after.start == info.start && !after.zombie)
        {
            return;
        }
        warn!(marker = %self.marker, %pid, "Same-user environment read refused: {errno}");
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
            && !info.zombie
            && info.start == leader.start
            && info.pgid == pgid
        {
            return Ok(Some(GroupAttribution::Leader));
        }
        for member in members {
            if let Some(identity) = self.observe(member) {
                return Ok(Some(GroupAttribution::MarkedMember(identity)));
            }
        }
        Err(CleanupFailure::GroupUnattributed { pgid })
    }

    /// Live, non-zombie processes in the group
    fn group_members(&mut self, pgid: Pid) -> Result<Vec<Pid>, CleanupFailure> {
        let pids = self
            .source
            .list()
            .map_err(|error| CleanupFailure::EnumerationFailed {
                message: error.to_string(),
            })?;
        Ok(pids
            .into_iter()
            .filter(|pid| {
                matches!(self.source.info(*pid), Read::Found(info) if info.pgid == pgid && !info.zombie)
            })
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

/// The earlier of `now + wait` and `deadline`
fn bounded(wait: Duration, deadline: Instant) -> Instant {
    (Instant::now() + wait).min(deadline)
}
