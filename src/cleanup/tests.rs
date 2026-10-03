//! Cleanup against real processes
//!
//! Apple platform binaries such as `/bin/sleep` expose an empty environment on
//! macOS, so marked test processes are this test binary re-executed into
//! [`helper_process`], which is an ordinary binary on both platforms

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::sys::signal::{SigHandler, Signal, kill, signal};
use nix::unistd::{Pid, setsid};

use super::{
    CleanupFailure, CleanupTiming, Environment, GroupAttribution, GroupOutcome, ProcessIdentity,
    ProcessInfo, ProcessSource, ProcessStartTime, Read, SweepOutcome, SystemProcesses,
    cleanup_lost_group_with, process_identity, sweep_marker, sweep_marker_with,
};
use crate::domain::TaskId;
use crate::run_env;

/// Selects the helper behaviour when this binary is re-executed
const HELPER_MODE: &str = "HOMEBASED_CLEANUP_TEST_HELPER";
/// File where a helper appends the PIDs it started
const HELPER_PIDFILE: &str = "HOMEBASED_CLEANUP_TEST_PIDFILE";
const HELPER_TEST: &str = "cleanup::tests::helper_process";
/// How long a helper lives if nothing stops it, so a failed test leaks nothing for long
const HELPER_LIFETIME: Duration = Duration::from_secs(120);

const FAST: CleanupTiming = CleanupTiming {
    term_grace: Duration::from_secs(2),
    kill_grace: Duration::from_secs(2),
    poll: Duration::from_millis(50),
    rounds: 3,
    deadline: Duration::from_secs(30),
};

/// Helper behaviours, selected by [`HELPER_MODE`]
///
/// - `sleep`: live until killed
/// - `escape`: start a `sleep` helper in a new session, record it, and exit, so
///   the helper is orphaned like a double-forked daemon
/// - `spawner`: ignore SIGTERM and start a new-session `sleep` helper every
///   100 ms, recording each, until killed
/// - `group-leader`: start a `sleep` helper in this process group, record it, and live
/// - `group-orphan`: like `group-leader`, but exit after two seconds
#[test]
fn helper_process() {
    let Ok(mode) = std::env::var(HELPER_MODE) else {
        return;
    };
    let pidfile = std::env::var_os(HELPER_PIDFILE).map(PathBuf::from);
    match mode.as_str() {
        "sleep" => {
            // a spawner's SIGTERM disposition is inherited through exec
            // SAFETY: restoring the default disposition installs no handler
            unsafe { signal(Signal::SIGTERM, SigHandler::SigDfl) }.unwrap();
            std::thread::sleep(HELPER_LIFETIME);
        }
        "escape" => {
            let child = helper_command("sleep", None).new_session().spawn().unwrap();
            record(pidfile.as_deref(), child);
        }
        "spawner" => {
            // SAFETY: ignoring a signal installs no handler
            unsafe { signal(Signal::SIGTERM, SigHandler::SigIgn) }.unwrap();
            let until = Instant::now() + HELPER_LIFETIME;
            while Instant::now() < until {
                let child = helper_command("sleep", None).new_session().spawn().unwrap();
                record(pidfile.as_deref(), child);
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        "group-leader" | "group-orphan" => {
            let child = helper_command("sleep", None).spawn().unwrap();
            record(pidfile.as_deref(), child);
            let lifetime = if mode == "group-leader" {
                HELPER_LIFETIME
            } else {
                Duration::from_secs(2)
            };
            std::thread::sleep(lifetime);
        }
        other => panic!("unknown helper mode {other}"),
    }
    std::process::exit(0);
}

/// Append a started child's PID and let it go unreaped
///
/// Helpers model work that outlives its parent: the helper exits or is killed
/// without waiting, the child is reparented, and the test stops it through
/// cleanup or [`Spawned`]
fn record(pidfile: Option<&Path>, child: Child) {
    use std::io::Write;
    let pid = child.id();
    let path = pidfile.expect("helper needs a pidfile");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{pid}").unwrap();
}

/// This test binary, re-executed into [`helper_process`] in `mode`
///
/// The command inherits this process's environment, so callers set or
/// remove the marker explicitly
fn helper_command(mode: &str, pidfile: Option<&Path>) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([HELPER_TEST, "--exact", "--nocapture", "--test-threads=1"])
        .env(HELPER_MODE, mode)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(pidfile) = pidfile {
        command.env(HELPER_PIDFILE, pidfile);
    }
    command
}

trait NewSession {
    fn new_session(&mut self) -> &mut Self;
}

impl NewSession for Command {
    /// Leave this process group and session, as a detached daemon does
    fn new_session(&mut self) -> &mut Self {
        // SAFETY: setsid is async-signal-safe and touches no parent state
        unsafe {
            self.pre_exec(|| setsid().map(drop).map_err(io::Error::from));
        }
        self
    }
}

/// Processes one test started; every one still alive is killed on drop, even
/// when the test fails
struct Spawned {
    children: Vec<Child>,
    others: Vec<ProcessIdentity>,
    _dir: tempfile::TempDir,
}

impl Spawned {
    fn new() -> (Self, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let pidfile = dir.path().join("pids");
        let spawned = Self {
            children: Vec::new(),
            others: Vec::new(),
            _dir: dir,
        };
        (spawned, pidfile)
    }

    /// Start a helper, marked with `marker` when given, and return its identity
    fn start(&mut self, command: &mut Command, marker: Option<TaskId>) -> ProcessIdentity {
        match marker {
            Some(marker) => command.env(run_env::TASK_ID, marker.to_string()),
            None => command.env_remove(run_env::TASK_ID),
        };
        let child = command.spawn().unwrap();
        let pid = Pid::from_raw(i32::try_from(child.id()).unwrap());
        self.children.push(child);
        wait_for_identity(pid)
    }

    /// Wait until `pidfile` lists `count` PIDs and track each one
    fn recorded(&mut self, pidfile: &Path, count: usize) -> Vec<ProcessIdentity> {
        let pids = wait_for_pids(pidfile, count);
        let identities: Vec<_> = pids.into_iter().map(wait_for_identity).collect();
        self.track(&identities);
        identities
    }

    fn track(&mut self, identities: &[ProcessIdentity]) {
        for identity in identities {
            if !self.others.contains(identity) {
                self.others.push(*identity);
            }
        }
    }

    /// Reap a direct child that is expected to exit on its own
    fn reap(&mut self, pid: Pid) {
        if let Some(child) = self
            .children
            .iter_mut()
            .find(|child| i64::from(child.id()) == i64::from(pid.as_raw()))
        {
            child.wait().unwrap();
        }
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        for identity in &self.others {
            if is_alive(*identity) {
                let _ = kill(identity.pid, Signal::SIGKILL);
            }
        }
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_for_pids(pidfile: &Path, count: usize) -> Vec<Pid> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let pids: Vec<Pid> = fs::read_to_string(pidfile)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| line.trim().parse().ok())
            .map(Pid::from_raw)
            .collect();
        if pids.len() >= count {
            return pids;
        }
        assert!(
            Instant::now() < deadline,
            "helpers recorded {} of {count} PIDs",
            pids.len()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Identity of a just-started process; the helper's environment is readable
/// once exec has finished, which a successful identity read does not prove,
/// so this also waits until the environment names the helper mode
fn wait_for_identity(pid: Pid) -> ProcessIdentity {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut source = SystemProcesses::new().unwrap();
    loop {
        if let Ok(identity) = process_identity(pid)
            && let Read::Found(environment) = source.environment(pid)
            && environment
                .entries()
                .any(|entry| entry.starts_with(HELPER_MODE.as_bytes()))
        {
            return identity;
        }
        assert!(Instant::now() < deadline, "helper {pid} did not start");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn is_alive(identity: ProcessIdentity) -> bool {
    process_identity(identity.pid).is_ok_and(|current| current == identity)
}

fn wait_dead(identity: ProcessIdentity) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while is_alive(identity) {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    true
}

fn signalled(outcome: &SweepOutcome) -> &[ProcessIdentity] {
    match outcome {
        SweepOutcome::Completed { signalled } => signalled,
        SweepOutcome::Incomplete(failure) => panic!("sweep incomplete: {failure}"),
    }
}

#[test]
fn sweep_kills_marked_processes_that_left_the_group_and_spares_others() {
    let marker = TaskId::new();
    let (mut spawned, pidfile) = Spawned::new();

    // a double-forked daemon: its parent exits, leaving it in a new session
    let parent = spawned.start(&mut helper_command("escape", Some(&pidfile)), Some(marker));
    let daemon = spawned.recorded(&pidfile, 1)[0];
    spawned.reap(parent.pid);
    let detached = spawned.start(helper_command("sleep", None).new_session(), Some(marker));
    let unmarked = spawned.start(helper_command("sleep", None).new_session(), None);
    let other_run = spawned.start(&mut helper_command("sleep", None), Some(TaskId::new()));

    let outcome = sweep_marker(marker, &BTreeSet::new(), FAST);

    let signalled = signalled(&outcome);
    assert!(signalled.contains(&daemon), "{outcome:?}");
    assert!(signalled.contains(&detached), "{outcome:?}");
    assert!(!signalled.contains(&unmarked));
    assert!(!signalled.contains(&other_run));
    assert!(wait_dead(daemon));
    spawned.reap(detached.pid);
    assert!(!is_alive(detached));
    assert!(
        is_alive(unmarked),
        "an unmarked process must not be signalled"
    );
    assert!(
        is_alive(other_run),
        "another run's process must not be signalled"
    );
}

#[test]
fn rescans_catch_a_descendant_created_during_cleanup() {
    let marker = TaskId::new();
    let (mut spawned, pidfile) = Spawned::new();
    let spawner = spawned.start(&mut helper_command("spawner", Some(&pidfile)), Some(marker));
    let before: BTreeSet<Pid> = spawned
        .recorded(&pidfile, 2)
        .iter()
        .map(|identity| identity.pid)
        .collect();

    let outcome = sweep_marker(marker, &BTreeSet::new(), FAST);

    spawned.reap(spawner.pid);
    let signalled = signalled(&outcome);
    let signalled_pids: BTreeSet<Pid> = signalled.iter().map(|identity| identity.pid).collect();
    // children that started after the first scan are dead now, so only their PIDs remain
    let late: Vec<Pid> = wait_for_pids(&pidfile, 0)
        .into_iter()
        .filter(|pid| !before.contains(pid))
        .collect();
    assert!(
        !late.is_empty(),
        "the spawner ignores SIGTERM, so it must have started children during the grace"
    );
    assert!(signalled_pids.contains(&spawner.pid));
    for pid in late {
        assert!(
            signalled_pids.contains(&pid),
            "child {pid} started during cleanup was not found by a rescan"
        );
    }
    for identity in signalled {
        assert!(wait_dead(*identity), "{identity} survived");
    }
}

#[test]
fn protected_process_with_the_marker_is_reported_and_never_signalled() {
    let marker = TaskId::new();
    let (mut spawned, _) = Spawned::new();
    let protected = spawned.start(&mut helper_command("sleep", None), Some(marker));
    let other = spawned.start(&mut helper_command("sleep", None), Some(marker));

    let outcome = sweep_marker(marker, &BTreeSet::from([protected.pid]), FAST);

    assert_eq!(
        outcome,
        SweepOutcome::Incomplete(CleanupFailure::ProtectedCarriesMarker {
            pids: vec![protected.pid]
        })
    );
    assert!(
        is_alive(protected),
        "a protected process must not be signalled"
    );
    spawned.reap(other.pid);
    assert!(!is_alive(other), "other targets are still cleaned up");
}

/// Wraps the real process table; once the sweep pins `target`, rereads see a
/// different process at that PID, as if it was reused
struct ReusedPid {
    inner: SystemProcesses,
    target: Pid,
    reused: bool,
    sent: Vec<(Pid, Signal)>,
}

impl ProcessSource for ReusedPid {
    type Handle = (Pid, <SystemProcesses as ProcessSource>::Handle);

    fn list(&mut self) -> io::Result<Vec<Pid>> {
        self.inner.list()
    }

    fn info(&mut self, pid: Pid) -> Read<ProcessInfo> {
        match self.inner.info(pid) {
            Read::Found(info) if self.reused && pid == self.target => Read::Found(ProcessInfo {
                start: ProcessStartTime(info.start.0 + 1),
                ..info
            }),
            read => read,
        }
    }

    fn environment(&mut self, pid: Pid) -> Read<Environment> {
        if self.reused && pid == self.target {
            return Read::Found(Environment::default());
        }
        self.inner.environment(pid)
    }

    fn pin(&mut self, pid: Pid) -> Read<Self::Handle> {
        if pid == self.target {
            self.reused = true;
        }
        match self.inner.pin(pid) {
            Read::Found(handle) => Read::Found((pid, handle)),
            Read::Exited(identity) => Read::Exited(identity),
            Read::Gone => Read::Gone,
            Read::Refused(errno) => Read::Refused(errno),
        }
    }

    fn send(&mut self, handle: &Self::Handle, signal: Signal) -> Result<(), Errno> {
        self.sent.push((handle.0, signal));
        self.inner.send(&handle.1, signal)
    }

    fn send_group(&mut self, pgid: Pid, signal: Signal) -> Result<(), Errno> {
        self.sent.push((pgid, signal));
        self.inner.send_group(pgid, signal)
    }
}

#[test]
fn identity_change_between_scan_and_signal_skips_the_signal() {
    let marker = TaskId::new();
    let (mut spawned, _) = Spawned::new();
    let target = spawned.start(&mut helper_command("sleep", None), Some(marker));
    let mut source = ReusedPid {
        inner: SystemProcesses::new().unwrap(),
        target: target.pid,
        reused: false,
        sent: Vec::new(),
    };

    let outcome = sweep_marker_with(&mut source, marker, &BTreeSet::new(), FAST);

    assert!(source.reused, "the sweep must have found the target");
    assert_eq!(outcome, SweepOutcome::Completed { signalled: vec![] });
    assert!(source.sent.is_empty(), "sent {:?}", source.sent);
    assert!(is_alive(target));
}

/// A process table that cannot be read
struct Unreadable;

impl ProcessSource for Unreadable {
    type Handle = ();

    fn list(&mut self) -> io::Result<Vec<Pid>> {
        Err(io::Error::from(Errno::EPERM))
    }

    fn info(&mut self, _: Pid) -> Read<ProcessInfo> {
        Read::Gone
    }

    fn environment(&mut self, _: Pid) -> Read<Environment> {
        Read::Gone
    }

    fn pin(&mut self, _: Pid) -> Read<()> {
        Read::Gone
    }

    fn send(&mut self, (): &(), _: Signal) -> Result<(), Errno> {
        Err(Errno::ESRCH)
    }

    fn send_group(&mut self, _: Pid, _: Signal) -> Result<(), Errno> {
        Err(Errno::ESRCH)
    }
}

#[test]
fn failed_enumeration_is_incomplete_not_empty() {
    let marker = TaskId::new();
    let outcome = sweep_marker_with(&mut Unreadable, marker, &BTreeSet::new(), FAST);
    assert!(matches!(
        outcome,
        SweepOutcome::Incomplete(CleanupFailure::EnumerationFailed { .. })
    ));

    let leader = ProcessIdentity {
        pid: Pid::this(),
        start: ProcessStartTime(1),
    };
    let outcome = cleanup_lost_group_with(&mut Unreadable, leader, marker, &BTreeSet::new(), FAST);
    assert!(matches!(
        outcome,
        GroupOutcome::Incomplete(CleanupFailure::EnumerationFailed { .. })
    ));
}

#[test]
fn lost_group_is_signalled_while_its_leader_keeps_its_identity() {
    let marker = TaskId::new();
    let (mut spawned, pidfile) = Spawned::new();
    let mut command = helper_command("group-leader", Some(&pidfile));
    // the leader carries no marker: its identity alone attributes the group
    let leader = spawned.start(command.process_group(0), None);
    let member = spawned.recorded(&pidfile, 1)[0];

    let outcome = super::cleanup_lost_group(leader, marker, &BTreeSet::new(), FAST);

    assert_eq!(outcome, GroupOutcome::Completed(GroupAttribution::Leader));
    spawned.reap(leader.pid);
    assert!(wait_dead(member));
}

#[test]
fn lost_group_without_its_leader_is_signalled_only_for_a_marked_member() {
    let (mut spawned, pidfile) = Spawned::new();
    let mut command = helper_command("group-orphan", Some(&pidfile));
    let marker = TaskId::new();
    let leader = spawned.start(command.process_group(0), Some(marker));
    let member = spawned.recorded(&pidfile, 1)[0];
    spawned.reap(leader.pid);

    let outcome = super::cleanup_lost_group(leader, marker, &BTreeSet::new(), FAST);

    assert_eq!(
        outcome,
        GroupOutcome::Completed(GroupAttribution::MarkedMember(member))
    );
    assert!(wait_dead(member));

    let (mut spawned, pidfile) = Spawned::new();
    let mut command = helper_command("group-orphan", Some(&pidfile));
    let leader = spawned.start(command.process_group(0), None);
    let member = spawned.recorded(&pidfile, 1)[0];
    spawned.reap(leader.pid);

    let outcome = super::cleanup_lost_group(leader, marker, &BTreeSet::new(), FAST);

    assert_eq!(
        outcome,
        GroupOutcome::Incomplete(CleanupFailure::GroupUnattributed { pgid: leader.pid })
    );
    assert!(
        is_alive(member),
        "an unattributed group must not be signalled"
    );
}

#[test]
fn lost_group_holding_a_protected_process_is_never_signalled() {
    let marker = TaskId::new();
    let (mut spawned, pidfile) = Spawned::new();
    let mut command = helper_command("group-leader", Some(&pidfile));
    let leader = spawned.start(command.process_group(0), Some(marker));
    let member = spawned.recorded(&pidfile, 1)[0];

    let outcome = super::cleanup_lost_group(leader, marker, &BTreeSet::from([member.pid]), FAST);

    assert_eq!(
        outcome,
        GroupOutcome::Incomplete(CleanupFailure::ProtectedInGroup {
            pgid: leader.pid,
            pids: vec![member.pid]
        })
    );
    assert!(is_alive(leader));
    assert!(is_alive(member));
}

#[test]
fn identity_of_a_live_process_is_stable_and_a_missing_one_errors() {
    let own = process_identity(Pid::this()).unwrap();
    assert_eq!(process_identity(Pid::this()).unwrap(), own);
    assert_eq!(own.pid, Pid::this());

    let error = process_identity(Pid::from_raw(i32::MAX)).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(Errno::ESRCH as i32));
}

/// Known limitation: with System Integrity Protection on, Apple platform
/// binaries expose an empty environment, so a marked `/bin/sleep` in its own
/// session escapes the sweep
#[cfg(target_os = "macos")]
#[test]
fn apple_platform_binary_exposes_an_empty_environment() {
    let marker = TaskId::new();
    let mut command = Command::new("/bin/sleep");
    command
        .arg("60")
        .env(run_env::TASK_ID, marker.to_string())
        .stdin(Stdio::null())
        .new_session();
    let mut child = command.spawn().unwrap();
    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap());
    // exec replaces the forked image, whose environment is still readable
    std::thread::sleep(Duration::from_millis(200));

    let mut source = SystemProcesses::new().unwrap();
    let environment = source.environment(pid);
    let outcome = sweep_marker(marker, &BTreeSet::new(), FAST);
    let alive = process_identity(pid).is_ok();
    let _ = child.kill();
    let _ = child.wait();

    // with System Integrity Protection on, macOS hides a platform binary's
    // environment, so the sweep cannot attribute it; hosts without that
    // protection, such as some CI runners, expose it and the sweep stops it
    if environment == Read::Found(Environment::default()) {
        assert_eq!(outcome, SweepOutcome::Completed { signalled: vec![] });
        assert!(
            alive,
            "the sweep cannot see the marker, so it leaves the process"
        );
        return;
    }
    let Read::Found(readable) = environment else {
        panic!("unexpected read of a live child: {environment:?}");
    };
    assert!(readable.carries(marker.to_string().as_bytes()));
    assert!(matches!(outcome, SweepOutcome::Completed { signalled } if !signalled.is_empty()));
}

/// Fork a marked child after enumeration, then lose the parent before inspection
struct ForkThenExit {
    marker: TaskId,
    scans: u32,
    hidden: bool,
    dead_parent: bool,
    alive: bool,
    sent: Vec<Pid>,
}

impl ProcessSource for ForkThenExit {
    type Handle = Pid;

    fn list(&mut self) -> io::Result<Vec<Pid>> {
        self.scans += 1;
        Ok(
            if self.scans == 1 || (self.dead_parent && self.scans == 2) || self.hidden {
                vec![Pid::from_raw(12345)]
            } else if self.alive {
                vec![Pid::from_raw(12346)]
            } else {
                vec![]
            },
        )
    }

    fn info(&mut self, pid: Pid) -> Read<ProcessInfo> {
        if pid.as_raw() == 12345 && self.dead_parent {
            return Read::Exited(ProcessIdentity {
                pid,
                start: ProcessStartTime(u64::from(self.scans)),
            });
        }
        if pid.as_raw() == 12345 || !self.alive {
            return Read::Gone;
        }
        Read::Found(ProcessInfo {
            start: ProcessStartTime(1),
            pgid: pid,
            uid: nix::unistd::geteuid().as_raw(),
            zombie: false,
        })
    }

    fn environment(&mut self, _: Pid) -> Read<Environment> {
        Read::Found(Environment(
            format!("HOMEBASED_TASK_ID={}\0", self.marker).into_bytes(),
        ))
    }

    fn pin(&mut self, pid: Pid) -> Read<Pid> {
        Read::Found(pid)
    }

    fn send(&mut self, pid: &Pid, _: Signal) -> Result<(), Errno> {
        self.sent.push(*pid);
        self.alive = false;
        Ok(())
    }

    fn send_group(&mut self, _: Pid, _: Signal) -> Result<(), Errno> {
        unreachable!()
    }
}

#[test]
fn review_fix_cleanup_rescans_a_parent_that_forks_then_exits() {
    let marker = TaskId::new();
    let mut source = ForkThenExit {
        marker,
        scans: 0,
        hidden: false,
        dead_parent: false,
        alive: true,
        sent: vec![],
    };
    let timing = CleanupTiming {
        poll: Duration::from_millis(1),
        ..FAST
    };
    let outcome = sweep_marker_with(&mut source, marker, &BTreeSet::new(), timing);
    assert!(matches!(outcome, SweepOutcome::Completed { .. }));
    assert_eq!(source.sent, vec![Pid::from_raw(12346)]);
    assert!(!source.alive);
    assert!(
        source.scans >= 4,
        "two stable empty scans must follow cleanup"
    );
}

#[test]
fn review_fix_cleanup_reports_incomplete_without_stable_empty_confirmation() {
    let marker = TaskId::new();
    let mut source = ForkThenExit {
        marker,
        scans: 0,
        hidden: true,
        dead_parent: false,
        alive: true,
        sent: vec![],
    };
    let timing = CleanupTiming {
        term_grace: Duration::from_millis(3),
        kill_grace: Duration::from_millis(3),
        poll: Duration::from_millis(1),
        rounds: 1,
        deadline: Duration::from_millis(6),
    };
    let outcome = sweep_marker_with(&mut source, marker, &BTreeSet::new(), timing);
    assert!(matches!(outcome, SweepOutcome::Incomplete(_)));
    assert!(source.alive);
    assert!(source.sent.is_empty());
}

#[cfg(target_os = "macos")]
#[test]
fn review_fix_unreaped_child_is_exited_not_a_fresh_disappearance() {
    let mut child = Command::new("/usr/bin/true").spawn().unwrap();
    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap());
    let mut source = SystemProcesses::new().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let observed = loop {
        let observed = source.info(pid);
        if !matches!(observed, Read::Found(info) if !info.zombie) {
            break observed;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child did not exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    child.wait().unwrap();
    assert!(matches!(observed, Read::Exited(_)), "{observed:?}");
}

#[test]
fn review_fix_cleanup_never_confirms_two_newly_exited_parent_scans() {
    let marker = TaskId::new();
    let mut source = ForkThenExit {
        marker,
        scans: 0,
        hidden: false,
        dead_parent: true,
        alive: true,
        sent: vec![],
    };
    let timing = CleanupTiming {
        poll: Duration::from_millis(1),
        ..FAST
    };
    let outcome = sweep_marker_with(&mut source, marker, &BTreeSet::new(), timing);
    assert!(matches!(outcome, SweepOutcome::Completed { .. }));
    assert_eq!(source.sent, vec![Pid::from_raw(12346)]);
    assert!(!source.alive);
}
