//! Cleanup against real processes
//!
//! Apple platform binaries such as `/bin/sleep` expose an empty environment on
//! macOS, so marked test processes are this test binary re-executed into
//! [`helper_process`], which is an ordinary binary on both platforms

use std::collections::{BTreeMap, BTreeSet};
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
    CleanupFailure, CleanupTiming, DescendantTracker, Environment, GroupAttribution, GroupOutcome,
    Lineage, NamedProcess, ProcessIdentity, ProcessInfo, ProcessName, ProcessSource,
    ProcessStartTime, Read, Suspect, SweepOutcome, SystemProcesses, cleanup_lost_group_with,
    process_identity, sweep_marker, sweep_marker_with, sweep_run,
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
/// - `detach-and-live`: start an unmarked `sleep` helper in a new session,
///   record it, and live, as Codex does with an MCP server
/// - `spawner`: ignore SIGTERM and start a new-session `sleep` helper every
///   100 ms, recording each, until killed
/// - `group-leader`: start a `sleep` helper in this process group, record it, and live
/// - `group-orphan`: like `group-leader`, but exit after two seconds
/// - `undumpable`: hide this process's environment from same-user readers, as
///   a sandboxed browser renderer does, and live until killed
/// - `sandbox-parent`: start an `undumpable` helper, record it, and live
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
        "detach-and-live" => {
            let child = helper_command("sleep", None)
                .new_session()
                .env_remove(run_env::TASK_ID)
                .spawn()
                .unwrap();
            record(pidfile.as_deref(), child);
            std::thread::sleep(HELPER_LIFETIME);
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
        #[cfg(target_os = "linux")]
        "undumpable" => {
            // SAFETY: restoring the default disposition installs no handler
            unsafe { signal(Signal::SIGTERM, SigHandler::SigDfl) }.unwrap();
            nix::sys::prctl::set_dumpable(false).unwrap();
            std::thread::sleep(HELPER_LIFETIME);
        }
        "sandbox-parent" => {
            let child = helper_command("undumpable", None).spawn().unwrap();
            record(pidfile.as_deref(), child);
            std::thread::sleep(HELPER_LIFETIME);
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
        wait_for_identity(self.launch(command, marker))
    }

    /// Start a helper without waiting for it, for one that may exit at once
    fn launch(&mut self, command: &mut Command, marker: Option<TaskId>) -> Pid {
        match marker {
            Some(marker) => command.env(run_env::TASK_ID, marker.to_string()),
            None => command.env_remove(run_env::TASK_ID),
        };
        let child = command.spawn().unwrap();
        let pid = Pid::from_raw(i32::try_from(child.id()).unwrap());
        self.children.push(child);
        pid
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
    // the parent exits as soon as it records the orphan, so it can be a zombie
    // before its identity is read; only the orphan matters here
    let parent = spawned.launch(&mut helper_command("escape", Some(&pidfile)), Some(marker));
    let daemon = spawned.recorded(&pidfile, 1)[0];
    spawned.reap(parent);
    let detached = spawned.start(helper_command("sleep", None).new_session(), Some(marker));
    let unmarked = spawned.start(helper_command("sleep", None).new_session(), None);
    let other_run = spawned.start(&mut helper_command("sleep", None), Some(TaskId::new()));

    let outcome = sweep_marker(marker, None, &BTreeSet::new(), FAST);

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
fn recorded_descendants_without_the_marker_are_stopped_after_their_parent_exits() {
    let marker = TaskId::new();
    let (mut spawned, pidfile) = Spawned::new();
    let root = spawned.start(
        &mut helper_command("detach-and-live", Some(&pidfile)),
        Some(marker),
    );
    let detached = spawned.recorded(&pidfile, 1)[0];
    let bystander = spawned.start(helper_command("sleep", None).new_session(), None);

    // helpers run this test binary, so a boundary named after it would stop the walk
    let mut tracker = DescendantTracker::with_boundary(root, Some("not-a-helper".into()));
    tracker.observe();
    assert!(tracker.recorded().contains(&detached), "{tracker:?}");

    // once the root exits the detached helper is reparented, so only the
    // record still ties it to the run
    kill(root.pid, Signal::SIGKILL).unwrap();
    spawned.reap(root.pid);
    tracker.observe();
    assert!(tracker.recorded().contains(&detached), "{tracker:?}");

    let outcome = sweep_run(marker, &tracker.into_adopted(), &BTreeSet::new(), FAST);

    assert!(signalled(&outcome).contains(&detached), "{outcome:?}");
    assert!(wait_dead(detached));
    assert!(
        is_alive(bystander),
        "an unrecorded, unmarked process must not be signalled"
    );
}

#[test]
fn descendant_tracking_stops_at_the_boundary_executable() {
    let (mut spawned, pidfile) = Spawned::new();
    let root = spawned.start(&mut helper_command("detach-and-live", Some(&pidfile)), None);
    let detached = spawned.recorded(&pidfile, 1)[0];
    // helpers run this test binary, which stands in for a homebased daemon
    // started from inside a run
    let own = std::env::current_exe().unwrap();

    let mut tracker = DescendantTracker::with_boundary(root, own.file_name().map(Into::into));
    tracker.observe();

    assert!(!tracker.recorded().contains(&detached), "{tracker:?}");
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

    let outcome = sweep_marker(marker, None, &BTreeSet::new(), FAST);

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

    let outcome = sweep_marker(marker, None, &BTreeSet::from([protected.pid]), FAST);

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

    let outcome = sweep_marker_with(&mut source, marker, None, &BTreeSet::new(), FAST);

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
    let outcome = sweep_marker_with(&mut Unreadable, marker, None, &BTreeSet::new(), FAST);
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

/// A live process whose environment is refused, with an optional exit on a later scan
struct RefusedEnvironment {
    identity: ProcessIdentity,
    uid: u32,
    scans: u32,
    exit_on_scan: Option<u32>,
}

impl ProcessSource for RefusedEnvironment {
    type Handle = Pid;

    fn list(&mut self) -> io::Result<Vec<Pid>> {
        self.scans += 1;
        Ok(vec![self.identity.pid])
    }

    fn info(&mut self, _: Pid) -> Read<ProcessInfo> {
        if self.exit_on_scan.is_some_and(|scan| self.scans >= scan) {
            return Read::Gone;
        }

        Read::Found(ProcessInfo {
            start: self.identity.start,
            ppid: Pid::from_raw(1),
            pgid: self.identity.pid,
            uid: self.uid,
            name: ProcessName::default(),
        })
    }

    fn environment(&mut self, _: Pid) -> Read<Environment> {
        Read::Refused(Errno::EPERM)
    }

    fn pin(&mut self, _: Pid) -> Read<Pid> {
        panic!("an unreadable process must never be a signal target")
    }

    fn send(&mut self, _: &Pid, _: Signal) -> Result<(), Errno> {
        panic!("an unreadable process must never be signalled")
    }

    fn send_group(&mut self, _: Pid, _: Signal) -> Result<(), Errno> {
        panic!("a marker sweep must never signal a group")
    }
}

const REFUSAL_TIMING: CleanupTiming = CleanupTiming {
    term_grace: Duration::from_millis(50),
    kill_grace: Duration::from_millis(50),
    poll: Duration::from_millis(1),
    rounds: 1,
    deadline: Duration::from_secs(2),
};

#[test]
fn unreadable_environment_rules_use_run_start_and_user() {
    let own_uid = nix::unistd::geteuid().as_raw();
    let run_start = ProcessStartTime(100);
    let cases = [
        ("after run start", own_uid, 101, Some(run_start), true),
        ("at run start", own_uid, 100, Some(run_start), true),
        ("before run start", own_uid, 99, Some(run_start), false),
        (
            "other user",
            own_uid.wrapping_add(1),
            101,
            Some(run_start),
            false,
        ),
        ("no run start", own_uid, 101, None, false),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    // check both the round bound and the deadline without skipping compatibility cases
    for timing in [
        REFUSAL_TIMING,
        CleanupTiming {
            rounds: 3,
            term_grace: Duration::from_secs(2),
            deadline: Duration::from_secs(1),
            ..REFUSAL_TIMING
        },
    ] {
        for (name, uid, start, bound, suspect) in cases {
            let identity = ProcessIdentity {
                pid: Pid::from_raw(12345),
                start: ProcessStartTime(start),
            };
            let process = NamedProcess {
                identity,
                name: None,
            };
            let mut source = RefusedEnvironment {
                identity,
                uid,
                scans: 0,
                exit_on_scan: None,
            };
            let outcome =
                sweep_marker_with(&mut source, TaskId::new(), bound, &BTreeSet::new(), timing);
            actual.push((name, outcome));
            expected.push((
                name,
                if suspect {
                    // its parent is init, so the walk finds no readable ancestor
                    SweepOutcome::Incomplete(CleanupFailure::UnreadableSuspects {
                        suspects: vec![Suspect {
                            process: process.clone(),
                            lineage: Some(Lineage::Orphaned {
                                adopter: None,
                                root: process,
                            }),
                        }],
                    })
                } else {
                    SweepOutcome::Completed { signalled: vec![] }
                },
            ));
        }
    }

    assert_eq!(actual, expected);

    let SweepOutcome::Incomplete(failure) = &actual[0].1 else {
        panic!("the newer same-user process must need Attention");
    };
    let queue_failure = crate::queue::CleanupFailure::from(failure.clone());
    let message = queue_failure.to_string();
    assert!(message.contains("pid 12345 started 101"), "{message}");
    let encoded = serde_json::to_value(&queue_failure).unwrap();
    assert_eq!(encoded["failure"]["kind"], "unreadable_suspects");
    assert_eq!(encoded["failure"]["suspects"][0]["pid"], 12345);
    assert_eq!(encoded["failure"]["suspects"][0]["start"], 101);
    assert_eq!(
        serde_json::from_value::<crate::queue::CleanupFailure>(encoded).unwrap(),
        queue_failure
    );

    // an attention stored before suspects carried names and lineage still loads
    let stored = serde_json::json!({
        "kind": "processes",
        "failure": {
            "kind": "unreadable_suspects",
            "suspects": [{ "pid": 12345, "start": 101 }],
        },
    });
    let old = serde_json::from_value::<crate::queue::CleanupFailure>(stored).unwrap();
    assert_eq!(
        old.to_string(),
        "could not read run markers; check same-user processes; pid 12345 started 101"
    );
}

#[test]
fn unreadable_suspect_exit_requires_two_new_empty_scans() {
    let mut source = RefusedEnvironment {
        identity: ProcessIdentity {
            pid: Pid::from_raw(12345),
            start: ProcessStartTime(101),
        },
        uid: nix::unistd::geteuid().as_raw(),
        scans: 0,
        exit_on_scan: Some(4),
    };
    let outcome = sweep_marker_with(
        &mut source,
        TaskId::new(),
        Some(ProcessStartTime(100)),
        &BTreeSet::new(),
        CleanupTiming {
            poll: Duration::from_millis(1),
            ..FAST
        },
    );
    assert_eq!(outcome, SweepOutcome::Completed { signalled: vec![] });
    assert!(
        source.scans >= 5,
        "cleanup finished after {} scans, before the suspect exited and two empty scans followed",
        source.scans
    );
}

/// How a process in an [`Ancestry`] table answers an environment read
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Env {
    /// Refused, as for a sandboxed renderer or another user's process
    Hidden,
    /// Readable, without the run marker
    Unmarked,
    /// Readable, with the run marker
    Marked,
    /// Readable but empty, as for an Apple platform binary
    Empty,
}

/// One process in an [`Ancestry`] chain
#[derive(Debug, Clone, Copy)]
struct Link {
    uid: u32,
    start: u64,
    env: Env,
    name: &'static str,
}

fn own(start: u64, env: Env, name: &'static str) -> Link {
    let uid = nix::unistd::geteuid().as_raw();
    Link {
        uid,
        start,
        env,
        name,
    }
}

fn other(start: u64, name: &'static str) -> Link {
    Link {
        uid: nix::unistd::geteuid().as_raw().wrapping_add(1),
        start,
        env: Env::Hidden,
        name,
    }
}

/// A process table holding one chain, from an unreadable same-user suspect
/// up to its oldest listed ancestor, whose parent is `top`
struct Ancestry {
    marker: TaskId,
    pids: Vec<Pid>,
    table: BTreeMap<Pid, (ProcessInfo, Env)>,
}

/// Fake PIDs, far above any this test process is likely to have
const ANCESTRY_BASE: i32 = 3_000_000;

impl Ancestry {
    fn new(marker: TaskId, chain: &[Link], top: Pid) -> Self {
        let pids: Vec<_> = (0..chain.len())
            .map(|index| Pid::from_raw(ANCESTRY_BASE + i32::try_from(index).unwrap()))
            .collect();
        let table = chain
            .iter()
            .enumerate()
            .map(|(index, link)| {
                let info = ProcessInfo {
                    start: ProcessStartTime(link.start),
                    ppid: pids.get(index + 1).copied().unwrap_or(top),
                    pgid: pids[index],
                    uid: link.uid,
                    name: ProcessName::new(link.name.as_bytes()),
                };
                (pids[index], (info, link.env))
            })
            .collect();

        Self {
            marker,
            pids,
            table,
        }
    }

    /// The listed process at `index` in the chain, as a report names it
    fn named(&self, index: usize) -> NamedProcess {
        let pid = self.pids[index];
        let (info, _) = self.table[&pid];
        NamedProcess {
            identity: ProcessIdentity {
                pid,
                start: info.start,
            },
            name: Some(info.name.to_string()),
        }
    }
}

impl ProcessSource for Ancestry {
    type Handle = Pid;

    fn list(&mut self) -> io::Result<Vec<Pid>> {
        Ok(self.table.keys().copied().collect())
    }

    fn info(&mut self, pid: Pid) -> Read<ProcessInfo> {
        self.table
            .get(&pid)
            .map_or(Read::Gone, |(info, _)| Read::Found(*info))
    }

    fn environment(&mut self, pid: Pid) -> Read<Environment> {
        match self.table.get(&pid) {
            Some((_, Env::Hidden)) => Read::Refused(Errno::EACCES),
            Some((_, Env::Unmarked)) => Read::Found(Environment(b"PATH=/usr/bin\0".to_vec())),
            Some((_, Env::Empty)) => Read::Found(Environment::default()),
            Some((_, Env::Marked)) => Read::Found(Environment(
                format!("{}={}\0", run_env::TASK_ID, self.marker).into_bytes(),
            )),
            None => Read::Gone,
        }
    }

    fn pin(&mut self, pid: Pid) -> Read<Pid> {
        // only a marked ancestor may be pinned; the suspect never is
        assert_ne!(
            pid, self.pids[0],
            "an unreadable process must never be a signal target"
        );
        Read::Found(pid)
    }

    fn send(&mut self, pid: &Pid, _: Signal) -> Result<(), Errno> {
        assert_ne!(
            *pid, self.pids[0],
            "an unreadable process must never be signalled"
        );
        Ok(())
    }

    fn send_group(&mut self, _: Pid, _: Signal) -> Result<(), Errno> {
        panic!("a marker sweep must never signal a group")
    }
}

/// What a case expects of the suspect at index 0
#[derive(Debug, Clone, Copy)]
enum Expect {
    /// Not run work; cleanup completes
    Clear,
    /// The ancestor at this index carries the marker
    RunWork(usize),
    /// Init adopted the tree whose topmost listed process is at this index
    OrphanedByInit(usize),
    /// The adopter at index `.1` adopted the tree topped by index `.0`
    OrphanedBy(usize, usize),
    /// The walk stopped at the ancestor at this index
    Unknown(usize),
}

struct AncestryCase {
    name: &'static str,
    chain: Vec<Link>,
    /// Indexes of ancestors that are protected workers
    protected: Vec<usize>,
    /// Indexes of the unreadable processes that hold cleanup, all with the
    /// lineage `expect` gives; an unreadable ancestor started during the run
    /// is a suspect too
    held: Vec<usize>,
    expect: Expect,
}

#[test]
fn unreadable_suspect_is_judged_by_its_nearest_readable_ancestor() {
    use Env::{Empty, Hidden, Marked, Unmarked};

    let run_start = 100;
    let case = |name, chain, expect| AncestryCase {
        name,
        chain,
        protected: vec![],
        held: vec![0],
        expect,
    };
    let cases = [
        // the Nov 13 stall: agent-browser's headless Chrome, started by an
        // agent during a GPU run, behind its setuid sandbox
        case(
            "sandboxed browser under an unmarked browser started during the run",
            vec![
                own(160, Hidden, "chrome"),
                own(150, Hidden, "chrome-sandbox"),
                own(140, Unmarked, "chrome"),
                own(130, Unmarked, "agent-browser"),
            ],
            Expect::Clear,
        ),
        case(
            "unreadable child of a marked parent",
            vec![own(150, Hidden, "renderer"), own(140, Marked, "train")],
            Expect::RunWork(1),
        ),
        case(
            "parent started after its child, so its PID was reused",
            vec![own(150, Hidden, "renderer"), own(170, Unmarked, "reused")],
            Expect::Unknown(1),
        ),
        case(
            "unreadable parent PID reused under a readable grandparent",
            vec![
                own(150, Hidden, "renderer"),
                own(170, Hidden, "reused"),
                own(140, Unmarked, "chrome"),
            ],
            Expect::Unknown(1),
        ),
        // macOS shows a run's `/bin/bash` with an empty environment
        case(
            "unreadable child of an empty-environment shell under marked run work",
            vec![
                own(150, Hidden, "tool"),
                own(140, Empty, "bash"),
                own(130, Marked, "train"),
            ],
            Expect::RunWork(2),
        ),
        case(
            "unreadable child of an empty-environment shell from before the run",
            vec![own(150, Hidden, "tool"), own(50, Empty, "bash")],
            Expect::Clear,
        ),
        case(
            "ssh login during the run",
            vec![
                own(150, Hidden, "sshd-session"),
                other(150, "sshd-session"),
                other(50, "sshd"),
            ],
            Expect::Clear,
        ),
        case(
            "same-user sandbox that predates the run",
            vec![own(150, Hidden, "renderer"), own(50, Hidden, "zygote")],
            Expect::Clear,
        ),
        case(
            "sudo from a marked run shell",
            vec![
                own(150, Hidden, "tool"),
                other(150, "sudo"),
                own(120, Marked, "bash"),
            ],
            Expect::RunWork(2),
        ),
        case(
            "other-user parent adopted by init",
            vec![own(150, Hidden, "tool"), other(150, "helper")],
            Expect::OrphanedByInit(1),
        ),
        case(
            "orphan adopted by init",
            vec![own(150, Hidden, "renderer")],
            Expect::OrphanedByInit(0),
        ),
        AncestryCase {
            protected: vec![2],
            held: vec![0, 1],
            ..case(
                "orphaned tree adopted by a worker",
                vec![
                    own(150, Hidden, "renderer"),
                    own(140, Hidden, "zygote"),
                    own(10, Unmarked, "homebased"),
                ],
                Expect::OrphanedBy(1, 2),
            )
        },
    ];

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases {
        let marker = TaskId::new();
        let mut source = Ancestry::new(marker, &case.chain, Pid::from_raw(1));
        let protected: BTreeSet<_> = case
            .protected
            .iter()
            .map(|&index| source.pids[index])
            .collect();
        let lineage = match case.expect {
            Expect::Clear => None,
            Expect::RunWork(ancestor) => Some(Lineage::RunWork {
                ancestor: source.named(ancestor),
            }),
            Expect::OrphanedByInit(root) => Some(Lineage::Orphaned {
                adopter: None,
                root: source.named(root),
            }),
            Expect::OrphanedBy(root, adopter) => Some(Lineage::Orphaned {
                adopter: Some(source.named(adopter)),
                root: source.named(root),
            }),
            Expect::Unknown(at) => Some(Lineage::Unknown {
                at: source.pids[at],
                reason: "started after its child; PID reused".into(),
            }),
        };
        expected.push((
            case.name,
            match lineage {
                None => SweepOutcome::Completed { signalled: vec![] },
                Some(lineage) => SweepOutcome::Incomplete(CleanupFailure::UnreadableSuspects {
                    suspects: case
                        .held
                        .iter()
                        .map(|&index| Suspect {
                            process: source.named(index),
                            lineage: Some(lineage.clone()),
                        })
                        .collect(),
                }),
            },
        ));

        let outcome = sweep_marker_with(
            &mut source,
            marker,
            Some(ProcessStartTime(run_start)),
            &protected,
            REFUSAL_TIMING,
        );
        // a marked ancestor is a target in its own right, which
        // the fake never lets exit; only the suspect's verdict matters here
        let outcome = match outcome {
            SweepOutcome::Completed { .. } => SweepOutcome::Completed { signalled: vec![] },
            outcome => outcome,
        };
        actual.push((case.name, outcome));
    }

    assert_eq!(actual, expected);
}

#[test]
fn daemon_ancestors_adopt_orphans_and_clear_nothing() {
    // a tree orphaned under `systemd --user`, the daemon's own parent, was
    // started by a readable process that is gone, so it may be run work
    let marker = TaskId::new();
    let mut source = Ancestry::new(
        marker,
        &[
            own(150, Env::Hidden, "renderer"),
            own(5, Env::Unmarked, "systemd"),
        ],
        Pid::from_raw(1),
    );
    let session = source.pids[1];
    let this = ProcessInfo {
        start: ProcessStartTime(10),
        ppid: session,
        pgid: Pid::this(),
        uid: nix::unistd::geteuid().as_raw(),
        name: ProcessName::new(b"homebased"),
    };
    source.table.insert(Pid::this(), (this, Env::Unmarked));

    let outcome = sweep_marker_with(
        &mut source,
        marker,
        Some(ProcessStartTime(100)),
        &BTreeSet::new(),
        REFUSAL_TIMING,
    );
    let SweepOutcome::Incomplete(failure) = outcome else {
        panic!("an orphan under the daemon's session manager must hold cleanup: {outcome:?}");
    };
    assert_eq!(
        failure,
        CleanupFailure::UnreadableSuspects {
            suspects: vec![Suspect {
                process: source.named(0),
                lineage: Some(Lineage::Orphaned {
                    adopter: Some(source.named(1)),
                    root: source.named(0),
                }),
            }],
        }
    );
    assert_eq!(
        failure.to_string(),
        format!(
            "could not read run markers; check same-user processes; pid {suspect} (renderer) \
             started 150: orphaned tree of pid {suspect} (renderer), adopted by pid {session} (systemd)",
            suspect = source.pids[0],
        )
    );
}

#[test]
fn other_user_daemon_ancestors_adopt_nothing() {
    // a daemon run by hand inside an ssh login sits under root `sshd`, which
    // adopts no orphans, so a later ssh login under that listener is no suspect
    let marker = TaskId::new();
    let mut source = Ancestry::new(
        marker,
        &[
            own(150, Env::Hidden, "sshd-session"),
            other(150, "sshd-session"),
            other(5, "sshd"),
        ],
        Pid::from_raw(1),
    );
    let this = ProcessInfo {
        start: ProcessStartTime(10),
        ppid: source.pids[2],
        pgid: Pid::this(),
        uid: nix::unistd::geteuid().as_raw(),
        name: ProcessName::new(b"homebased"),
    };
    source.table.insert(Pid::this(), (this, Env::Unmarked));

    let outcome = sweep_marker_with(
        &mut source,
        marker,
        Some(ProcessStartTime(100)),
        &BTreeSet::new(),
        REFUSAL_TIMING,
    );
    assert_eq!(outcome, SweepOutcome::Completed { signalled: vec![] });
}

#[test]
fn attention_names_each_suspect_tree_once() {
    let marker = TaskId::new();
    let mut source = Ancestry::new(
        marker,
        &[
            own(160, Env::Hidden, "chrome"),
            own(150, Env::Hidden, "chrome-sandbox"),
            own(140, Env::Marked, "chrome"),
        ],
        Pid::from_raw(1),
    );
    let outcome = sweep_marker_with(
        &mut source,
        marker,
        Some(ProcessStartTime(100)),
        &BTreeSet::new(),
        REFUSAL_TIMING,
    );
    let SweepOutcome::Incomplete(failure) = outcome else {
        panic!("a sandbox under marked run work must hold cleanup: {outcome:?}");
    };
    let [renderer, sandbox, browser] = [0, 1, 2].map(|index| source.pids[index]);
    assert_eq!(
        failure.to_string(),
        format!(
            "could not read run markers; check same-user processes; pid {renderer} (chrome) \
             started 160, pid {sandbox} (chrome-sandbox) started 150: under pid {browser} \
             (chrome), which carries the run marker"
        )
    );
}

/// An undumpable helper under a readable parent, started as a run's workload
///
/// Returns the parent and the undumpable child. The child hides its
/// environment the way Chrome's sandboxed processes do on Linux: same owner,
/// environment read refused
#[cfg(target_os = "linux")]
fn sandboxed_tree(
    spawned: &mut Spawned,
    pidfile: &Path,
    marker: Option<TaskId>,
) -> [ProcessIdentity; 2] {
    let parent = spawned.start(&mut helper_command("sandbox-parent", Some(pidfile)), marker);
    let [pid] = wait_for_pids(pidfile, 1)[..] else {
        panic!("sandbox parent records one child");
    };
    // the child hides its environment soon after exec, so a refused read,
    // not the helper mode that `wait_for_identity` reads, shows it started
    let until = Instant::now() + Duration::from_secs(20);
    let mut source = SystemProcesses::new().unwrap();
    while !matches!(source.environment(pid), Read::Refused(_)) {
        assert!(
            Instant::now() < until,
            "helper {pid} never hid its environment"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let child = process_identity(pid).unwrap();
    spawned.track(&[child]);
    [parent, child]
}

/// One short round, for a sweep expected to end with its suspect still held;
/// a sweep that must confirm an empty real process table uses [`FAST`],
/// which allows for a loaded machine
#[cfg(target_os = "linux")]
const SANDBOX_TIMING: CleanupTiming = CleanupTiming {
    term_grace: Duration::from_millis(500),
    kill_grace: Duration::from_millis(500),
    rounds: 1,
    ..FAST
};

/// The real process table, listing only one test's processes
///
/// A whole-machine sweep would also see other suites' undumpable helpers,
/// which come and go and keep its empty scans from settling. Ancestry and
/// environment reads still go to the real table
#[cfg(target_os = "linux")]
struct Scoped {
    system: SystemProcesses,
    pids: BTreeSet<Pid>,
}

#[cfg(target_os = "linux")]
impl Scoped {
    fn new(processes: &[ProcessIdentity]) -> Self {
        Self {
            system: SystemProcesses::new().unwrap(),
            pids: processes.iter().map(|process| process.pid).collect(),
        }
    }
}

#[cfg(target_os = "linux")]
impl ProcessSource for Scoped {
    type Handle = <SystemProcesses as ProcessSource>::Handle;

    fn list(&mut self) -> io::Result<Vec<Pid>> {
        let mut pids = self.system.list()?;
        pids.retain(|pid| self.pids.contains(pid));
        Ok(pids)
    }

    fn info(&mut self, pid: Pid) -> Read<ProcessInfo> {
        self.system.info(pid)
    }

    fn environment(&mut self, pid: Pid) -> Read<Environment> {
        self.system.environment(pid)
    }

    fn pin(&mut self, pid: Pid) -> Read<Self::Handle> {
        self.system.pin(pid)
    }

    fn send(&mut self, handle: &Self::Handle, signal: Signal) -> Result<(), Errno> {
        self.system.send(handle, signal)
    }

    fn send_group(&mut self, pgid: Pid, signal: Signal) -> Result<(), Errno> {
        self.system.send_group(pgid, signal)
    }
}

#[cfg(target_os = "linux")]
#[test]
fn real_sandboxed_child_of_an_unmarked_parent_does_not_hold_cleanup() {
    let (mut spawned, pidfile) = Spawned::new();
    let [parent, child] = sandboxed_tree(&mut spawned, &pidfile, None);

    let outcome = sweep_marker_with(
        &mut Scoped::new(&[parent, child]),
        TaskId::new(),
        Some(parent.start),
        &BTreeSet::new(),
        FAST,
    );
    assert_eq!(outcome, SweepOutcome::Completed { signalled: vec![] });
    assert!(
        is_alive(parent) && is_alive(child),
        "unrelated processes are never signalled"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn real_sandboxed_child_of_a_marked_parent_holds_cleanup() {
    let marker = TaskId::new();
    let (mut spawned, pidfile) = Spawned::new();
    let [parent, child] = sandboxed_tree(&mut spawned, &pidfile, Some(marker));

    let outcome = sweep_marker_with(
        &mut Scoped::new(&[parent, child]),
        marker,
        Some(parent.start),
        &BTreeSet::new(),
        SANDBOX_TIMING,
    );
    let SweepOutcome::Incomplete(CleanupFailure::UnreadableSuspects { suspects }) = &outcome else {
        panic!("a sandboxed child of marked run work must hold cleanup: {outcome:?}");
    };
    let [suspect] = &suspects[..] else {
        panic!("one suspect expected: {suspects:?}");
    };
    assert_eq!(suspect.process.identity, child);
    // the sweep stops the marked parent, so the child is held either as run
    // work or, once it is reparented, as an orphan whose origin is lost
    assert!(
        matches!(
            &suspect.lineage,
            Some(Lineage::RunWork { ancestor }) if ancestor.identity == parent
        ) || matches!(&suspect.lineage, Some(Lineage::Orphaned { root, .. }) if root.identity == child),
        "{suspect:?}"
    );
    assert!(is_alive(child), "an unreadable process is never signalled");
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
    let outcome = sweep_marker(marker, None, &BTreeSet::new(), FAST);
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
            ppid: Pid::from_raw(1),
            pgid: pid,
            uid: nix::unistd::geteuid().as_raw(),
            name: ProcessName::default(),
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
fn cleanup_rescans_a_parent_that_forks_then_exits() {
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
    let outcome = sweep_marker_with(&mut source, marker, None, &BTreeSet::new(), timing);
    assert!(matches!(outcome, SweepOutcome::Completed { .. }));
    assert_eq!(source.sent, vec![Pid::from_raw(12346)]);
    assert!(!source.alive);
    assert!(
        source.scans >= 4,
        "two stable empty scans must follow cleanup"
    );
}

/// A chain of marked processes, each forking its successor and exiting while
/// the scan that listed it inspects it, then a long-lived marked descendant
struct ForkChain {
    marker: TaskId,
    generations: u32,
    scans: u32,
    alive: bool,
    sent: Vec<Pid>,
}

const CHAIN_DESCENDANT: i32 = 22000;

impl ProcessSource for ForkChain {
    type Handle = Pid;

    fn list(&mut self) -> io::Result<Vec<Pid>> {
        self.scans += 1;
        let pid = if self.scans <= self.generations {
            21000 + i32::try_from(self.scans).unwrap()
        } else {
            CHAIN_DESCENDANT
        };
        Ok(if pid == CHAIN_DESCENDANT && !self.alive {
            vec![]
        } else {
            vec![Pid::from_raw(pid)]
        })
    }

    fn info(&mut self, pid: Pid) -> Read<ProcessInfo> {
        if pid.as_raw() != CHAIN_DESCENDANT || !self.alive {
            return Read::Gone;
        }
        Read::Found(ProcessInfo {
            start: ProcessStartTime(1),
            ppid: Pid::from_raw(1),
            pgid: pid,
            uid: nix::unistd::geteuid().as_raw(),
            name: ProcessName::default(),
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

// two processes in a row exit while their scans inspect them, so neither
// scan may count as empty, and the descendant they left is found and stopped
#[test]
fn cleanup_keeps_scanning_after_a_two_generation_fork_chain() {
    let marker = TaskId::new();
    let mut source = ForkChain {
        marker,
        generations: 2,
        scans: 0,
        alive: true,
        sent: vec![],
    };
    let timing = CleanupTiming {
        poll: Duration::from_millis(1),
        ..FAST
    };
    let outcome = sweep_marker_with(&mut source, marker, None, &BTreeSet::new(), timing);
    assert!(
        matches!(outcome, SweepOutcome::Completed { .. }),
        "{outcome:?}"
    );
    assert_eq!(source.sent, vec![Pid::from_raw(CHAIN_DESCENDANT)]);
    assert!(!source.alive);
}

// on a busy machine short-lived processes exit between enumeration and
// inspection in every scan; that must not keep cleanup from finishing
#[test]
fn cleanup_finishes_while_listed_processes_keep_exiting() {
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
        poll: Duration::from_millis(1),
        ..FAST
    };
    let outcome = sweep_marker_with(&mut source, marker, None, &BTreeSet::new(), timing);
    assert_eq!(outcome, SweepOutcome::Completed { signalled: vec![] });
    assert!(source.sent.is_empty());
}

#[cfg(target_os = "macos")]
#[test]
fn unreaped_child_is_exited_not_a_fresh_disappearance() {
    let mut child = Command::new("/usr/bin/true").spawn().unwrap();
    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap());
    let mut source = SystemProcesses::new().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let observed = loop {
        let observed = source.info(pid);
        if !matches!(observed, Read::Found(_)) {
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
    // the unreaped child is a zombie, which is not a target
    assert!(matches!(observed, Read::Exited(_)), "{observed:?}");
}
