//! `task-run`: lock, setsid, spawn, process-group cleanup, `exit.json`, event.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

use nix::errno::Errno;
use nix::fcntl::{FcntlArg, FdFlag, fcntl};
#[cfg(target_os = "linux")]
use nix::sys::prctl;
use nix::sys::signal::{Signal, kill};
use nix::unistd::{Pid, setsid};
use tokio::io::AsyncWriteExt;
use tokio::process::Command as TokioCommand;
use tokio::signal::unix::{Signal as SignalStream, SignalKind, signal};
use tokio::time;
use tracing::{info, warn};

use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskExitEvidence, TaskId, TaskIdentity,
    TaskRow, TaskState, Workload,
};
use crate::error::AppError;
use crate::home::{self, Home, LockMode, TaskPaths};
use crate::invocation::{ChildInvocation, StdinPolicy, invocation_from_workload_for_identity};
use crate::report::REPORT_TRAILER;
use crate::store::{self, Store};

mod container;

const KILL_GRACE: Duration = Duration::from_secs(10);
const KILL_REAP_GRACE: Duration = Duration::from_secs(2);
const GROUP_POLL: Duration = Duration::from_millis(50);
const CANCELLATION_POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy)]
enum ChildCancellationCause {
    PersistedMarker,
    Signal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskRunExit {
    reason: ExitReason,
    process_group_exit_evidence: ProcessGroupExitEvidence,
}

#[derive(Debug, Clone, Copy)]
struct CleanupTiming {
    term_grace: Duration,
    kill_grace: Duration,
    poll: Duration,
}

const CLEANUP_TIMING: CleanupTiming = CleanupTiming {
    term_grace: KILL_GRACE,
    kill_grace: KILL_REAP_GRACE,
    poll: GROUP_POLL,
};

#[cfg(test)]
static TASK_RUN_EXECUTABLE_FOR_TESTS: std::sync::Mutex<Option<std::path::PathBuf>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn set_task_run_executable_for_tests(executable: std::path::PathBuf) {
    *TASK_RUN_EXECUTABLE_FOR_TESTS.lock().unwrap() = Some(executable);
}

/// Spawn `homebased task-run` with an inherited exclusive flock.
pub fn spawn_task_run(home: &Home, id: TaskId, lock: File) -> Result<u32, AppError> {
    let exe = task_run_executable()?;
    let fd = lock.as_raw_fd();
    let mut cmd = StdCommand::new(&exe);
    cmd.arg("task-run")
        .arg("--home")
        .arg(home.root())
        .arg("--id")
        .arg(id.to_string())
        .arg("--lock-fd")
        .arg(fd.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        cmd.pre_exec(move || prepare_worker(fd));
    }
    let child = cmd.spawn().map_err(|err| AppError::Internal {
        message: format!("spawn task-run: {err}"),
    })?;
    let pid = child.id();
    // closing the parent's descriptor keeps the lock: the worker inherited the
    // same open file description
    drop(lock);
    std::thread::spawn(move || {
        let mut child = child;
        // reap the direct child so it does not linger as a zombie; the worker
        // outlives this process and its exit status is read from exit.json
        if let Err(err) = child.wait() {
            warn!("reap task-run: {err}");
        }
    });
    Ok(pid)
}

pub(crate) fn task_run_executable() -> Result<std::path::PathBuf, AppError> {
    #[cfg(test)]
    if let Some(executable) = TASK_RUN_EXECUTABLE_FOR_TESTS
        .lock()
        .ok()
        .and_then(|executable| executable.clone())
    {
        return Ok(executable);
    }

    std::env::current_exe().map_err(|err| AppError::Internal {
        message: format!("current_exe: {err}"),
    })
}

fn prepare_worker(fd: RawFd) -> io::Result<()> {
    setsid().map_err(io::Error::from)?;
    let borrowed = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
    let flags = fcntl(borrowed, FcntlArg::F_GETFD).map_err(io::Error::from)?;
    let mut fdflag = FdFlag::from_bits_truncate(flags);
    fdflag.remove(FdFlag::FD_CLOEXEC);
    fcntl(borrowed, FcntlArg::F_SETFD(fdflag)).map_err(io::Error::from)?;
    Ok(())
}

/// Worker entry: hold the inherited lock until exit.json and event commit.
pub async fn run(home: Home, id: TaskId, lock_fd: i32) -> Result<(), AppError> {
    let _lock = unsafe { File::from_raw_fd(lock_fd) };
    // install the SIGTERM handler before any other work. Everything below (the
    // Queued->Running CAS, set_pid, the feed read) is a window in which the
    // default disposition would kill this worker outright, leaving the task
    // Lost instead of Cancelled. Tokio latches a signal that arrives before the
    // first `recv()`, so a cancel in this window is still seen.
    let mut sigterm = signal(SignalKind::terminate()).map_err(|err| AppError::Internal {
        message: format!("signal: {err}"),
    })?;
    home.ensure()?;
    let store = Store::open(&home.db_path())?;
    let started = store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)?
        .is_some();
    let row = store.require_task(id)?;
    // a container keeps running under dockerd after its worker stops, so a running
    // container task whose lock this worker holds is adopted, never relaunched
    let adopt_container = !started
        && matches!(row.workload, Workload::Container(_))
        && matches!(row.state, TaskState::Running { .. });
    if !started && !adopt_container {
        info!(%id, "CAS Queued→Running failed; exiting without spawn");
        return Ok(());
    }
    let pid = std::process::id() as i32;
    store.set_pid(id, pid)?;
    let paths = home.task_paths(id);
    if let Workload::Container(workload) = &row.workload {
        let entry = if adopt_container {
            container::Entry::Adopt
        } else {
            container::Entry::Launch
        };
        return container::run(&store, &row, workload, &paths, entry, &mut sigterm).await;
    }
    let exit = match invocation_from_workload_for_identity(
        &row.workload,
        &row.binary,
        &row.cwd,
        &paths.feed,
        TaskIdentity::Actual(id),
    ) {
        Ok(invocation) => {
            // only stdin-fed agents need the bytes; Grok reads the feed path from argv
            let feed = match invocation.stdin {
                StdinPolicy::PromptFeed => match std::fs::read(&paths.feed) {
                    Ok(feed) => Ok(Some(feed)),
                    Err(err) => {
                        let message = format!("read prompt feed: {err}");
                        std::fs::write(&paths.output, format!("homebased: {message}\n"))?;
                        Err(TaskRunExit {
                            reason: ExitReason::SpawnFailed { message },
                            process_group_exit_evidence: ProcessGroupExitEvidence::NoChildSpawned,
                        })
                    }
                },
                StdinPolicy::Null => Ok(None),
            };
            match feed {
                Ok(feed) => {
                    match run_child(&invocation, &row, &home, &paths, feed, &store, &mut sigterm)
                        .await
                    {
                        Ok(exit) => exit,
                        Err(err) => TaskRunExit {
                            reason: ExitReason::SpawnFailed {
                                message: err.to_string(),
                            },
                            process_group_exit_evidence: ProcessGroupExitEvidence::NoChildSpawned,
                        },
                    }
                }
                Err(exit) => exit,
            }
        }
        Err(err) => {
            let message = err.to_string();
            std::fs::write(&paths.output, format!("homebased: {message}\n"))?;
            TaskRunExit {
                reason: ExitReason::SpawnFailed { message },
                process_group_exit_evidence: ProcessGroupExitEvidence::NoChildSpawned,
            }
        }
    };

    record_exit(
        &store,
        id,
        &paths,
        &exit.reason,
        exit.process_group_exit_evidence.into(),
    )
}

/// Write `exit.json`, then commit the terminal state it describes
///
/// The file lets the daemon apply the exit if this worker stops before the commit
fn record_exit(
    store: &Store,
    id: TaskId,
    paths: &TaskPaths,
    reason: &ExitReason,
    evidence: TaskExitEvidence,
) -> Result<(), AppError> {
    store::write_exit_json_with_evidence(&paths.exit_json, reason, evidence.clone())?;
    if store
        .cas_exit_with_evidence(id, ProcessStatus::Running, reason, evidence)?
        .is_none()
    {
        let current = store.require_task(id)?;
        warn!(%id, status = %current.status(), "cas_exit failed");
    }
    Ok(())
}

async fn run_child(
    invocation: &ChildInvocation,
    row: &TaskRow,
    home: &Home,
    paths: &TaskPaths,
    feed: Option<Vec<u8>>,
    store: &Store,
    sigterm: &mut SignalStream,
) -> Result<TaskRunExit, AppError> {
    let id = row.id;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.output)?;
    let stdout = log.try_clone()?;
    let stderr = log;

    let mut cmd = TokioCommand::new(&invocation.program);
    cmd.args(&invocation.args)
        .current_dir(&row.cwd)
        .env("PATH", &row.env.path)
        .env("HOME", &row.env.home)
        .env("HOMEBASED_TASK_ID", id.to_string())
        .env("HOMEBASED_HOME", home.root())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .process_group(0);
    for (key, value) in invocation.environment.iter() {
        cmd.env(key, value);
    }
    match invocation.stdin {
        StdinPolicy::PromptFeed => {
            cmd.stdin(Stdio::piped());
        }
        StdinPolicy::Null => {
            cmd.stdin(Stdio::null());
        }
    }
    #[cfg(target_os = "linux")]
    unsafe {
        cmd.pre_exec(|| {
            prctl::set_pdeathsig(Some(Signal::SIGKILL)).map_err(io::Error::from)?;
            Ok(())
        });
    }

    let mut child = cmd.spawn().map_err(|err| AppError::Internal {
        message: format!("spawn child: {err}"),
    })?;
    let Some(child_pgid) = child.id().map(|pid| pid as i32) else {
        let reason =
            status_to_reason(child.wait().await).unwrap_or_else(|err| ExitReason::SpawnFailed {
                message: err.to_string(),
            });
        return Ok(TaskRunExit {
            reason,
            process_group_exit_evidence: ProcessGroupExitEvidence::Unconfirmed,
        });
    };
    if invocation.stdin == StdinPolicy::PromptFeed
        && let Some(feed) = feed
        && let Some(mut stdin) = child.stdin.take()
    {
        tokio::spawn(async move {
            match stdin.write_all(&feed).await {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::BrokenPipe => {}
                Err(err) => warn!("prompt stdin write: {err}"),
            }
        });
    }

    let mut cancellation_poll = time::interval(CANCELLATION_POLL);
    cancellation_poll.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let cause = loop {
        tokio::select! {
            status = child.wait() => {
                let reason = status_to_reason(status).unwrap_or_else(|err| {
                    ExitReason::SpawnFailed {
                        message: err.to_string(),
                    }
                });
                // direct-child exit is not proof the process group is empty
                let process_group_exit_evidence = cleanup_process_group(child_pgid).await;
                return Ok(TaskRunExit { reason, process_group_exit_evidence });
            }
            _ = sigterm.recv() => break ChildCancellationCause::Signal,
            _ = cancellation_poll.tick() => match store.require_task(id) {
                Ok(row) if row.cancel_requested_at.is_some() => {
                    break ChildCancellationCause::PersistedMarker;
                }
                Ok(_) => {}
                Err(err) => warn!(%id, "read cancellation state: {err}"),
            }
        }
    };

    let cancelled = match cause {
        ChildCancellationCause::PersistedMarker => true,
        ChildCancellationCause::Signal => match store.require_task(id) {
            Ok(row) => row.cancel_requested_at.is_some(),
            Err(err) => {
                warn!(%id, "read cancellation state: {err}");
                false
            }
        },
    };
    match cause {
        ChildCancellationCause::PersistedMarker => {
            warn!(%id, "persisted cancellation marker: forwarding to child group")
        }
        ChildCancellationCause::Signal => {
            warn!(%id, cancelled, "SIGTERM: forwarding to child group")
        }
    }
    let process_group_exit_evidence = cancel_child_group(&mut child, child_pgid).await;
    let reason = if cancelled {
        ExitReason::Cancelled
    } else {
        ExitReason::Signal { signal: 15 }
    };
    Ok(TaskRunExit {
        reason,
        process_group_exit_evidence,
    })
}

/// A Unix wait status is either an exit code or a terminating signal.
fn status_to_reason(status: io::Result<std::process::ExitStatus>) -> Result<ExitReason, AppError> {
    let status = match status {
        Ok(status) => status,
        Err(err) => {
            return Ok(ExitReason::SpawnFailed {
                message: err.to_string(),
            });
        }
    };
    if let Some(code) = status.code() {
        return Ok(ExitReason::Exit { code });
    }
    if let Some(signal) = status.signal() {
        return Ok(ExitReason::Signal { signal });
    }
    Err(AppError::Internal {
        message: format!("child wait status has neither code nor signal: {status:?}"),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessGroupProbe {
    Alive,
    Exited,
    Unknown,
}

/// Probe one process group without treating unexpected errors as evidence of exit.
fn process_group_probe(pgid: i32) -> ProcessGroupProbe {
    match kill(Pid::from_raw(-pgid), None) {
        Ok(()) => ProcessGroupProbe::Alive,
        Err(Errno::ESRCH) => ProcessGroupProbe::Exited,
        Err(err) => {
            warn!(pgid, "process group probe: {err}");
            ProcessGroupProbe::Unknown
        }
    }
}

/// Reap the direct child while giving the full group one shared TERM grace.
async fn cancel_child_group(
    child: &mut tokio::process::Child,
    child_pgid: i32,
) -> ProcessGroupExitEvidence {
    let mut child_reaped = false;
    let evidence = cleanup_process_group_with(
        child_pgid,
        || process_group_probe(child_pgid),
        |signal| kill(Pid::from_raw(-child_pgid), signal),
        || {
            if child_reaped {
                return;
            }
            match child.try_wait() {
                Ok(Some(_)) => child_reaped = true,
                Ok(None) => {}
                Err(err) => {
                    warn!(child_pgid, "reap cancelled child: {err}");
                    child_reaped = true;
                }
            }
        },
        CLEANUP_TIMING,
    )
    .await;
    if !child_reaped && let Err(err) = time::timeout(KILL_REAP_GRACE, child.wait()).await {
        warn!(child_pgid, "reap cancelled child timed out: {err}");
    }
    evidence
}

/// Terminate remaining members of the child process group and confirm the result.
async fn cleanup_process_group(child_pgid: i32) -> ProcessGroupExitEvidence {
    cleanup_process_group_with(
        child_pgid,
        || process_group_probe(child_pgid),
        |signal| kill(Pid::from_raw(-child_pgid), signal),
        || {},
        CLEANUP_TIMING,
    )
    .await
}

async fn cleanup_process_group_with(
    child_pgid: i32,
    mut probe: impl FnMut() -> ProcessGroupProbe,
    mut send_signal: impl FnMut(Signal) -> Result<(), Errno>,
    mut reap_child: impl FnMut(),
    timing: CleanupTiming,
) -> ProcessGroupExitEvidence {
    reap_child();
    if probe() == ProcessGroupProbe::Exited {
        return ProcessGroupExitEvidence::ConfirmedExited;
    }

    if let Err(err) = send_signal(Signal::SIGTERM) {
        warn!(child_pgid, "SIGTERM child group: {err}");
    }
    let term_deadline = time::Instant::now() + timing.term_grace;
    while time::Instant::now() < term_deadline {
        reap_child();
        if probe() == ProcessGroupProbe::Exited {
            return ProcessGroupExitEvidence::ConfirmedExited;
        }
        time::sleep(
            timing
                .poll
                .min(term_deadline.saturating_duration_since(time::Instant::now())),
        )
        .await;
    }

    reap_child();
    let mut killed_group = false;
    if probe() != ProcessGroupProbe::Exited {
        killed_group = true;
        if let Err(err) = send_signal(Signal::SIGKILL) {
            warn!(child_pgid, "SIGKILL child group: {err}");
        }
        let kill_deadline = time::Instant::now() + timing.kill_grace;
        while time::Instant::now() < kill_deadline {
            reap_child();
            if probe() == ProcessGroupProbe::Exited {
                return ProcessGroupExitEvidence::ConfirmedExited;
            }
            time::sleep(
                timing
                    .poll
                    .min(kill_deadline.saturating_duration_since(time::Instant::now())),
            )
            .await;
        }
    }

    reap_child();
    match probe() {
        ProcessGroupProbe::Exited if !killed_group => ProcessGroupExitEvidence::ConfirmedExited,
        ProcessGroupProbe::Exited => {
            warn!(
                child_pgid,
                "child process group exit was not confirmed within the kill grace"
            );
            ProcessGroupExitEvidence::Unconfirmed
        }
        ProcessGroupProbe::Alive | ProcessGroupProbe::Unknown => {
            warn!(child_pgid, "child process group exit remains unconfirmed");
            ProcessGroupExitEvidence::Unconfirmed
        }
    }
}

/// Write prompt evidence for an agent workload.
pub fn write_task_files(
    paths: &TaskPaths,
    prompt: &str,
    report_trailer: bool,
) -> Result<(), AppError> {
    let trailer = if report_trailer {
        Some(REPORT_TRAILER)
    } else {
        None
    };
    paths.write_prompt(prompt, trailer)
}

/// Open `runner.lock` and take the exclusive flock before spawn.
pub fn lock_before_spawn(paths: &TaskPaths) -> Result<File, AppError> {
    home::flock_exclusive(&paths.runner_lock, LockMode::Blocking)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::process::{Command, Stdio};

    #[test]
    fn inherited_flock_held_after_parent_closes() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("runner.lock");
        let lock = home::flock_exclusive(&lock_path, LockMode::Blocking).unwrap();
        let fd = lock.as_raw_fd();
        // `sleep` directly, not `sh -c`: a shell that forks instead of exec-ing
        // leaves a grandchild holding the inherited fd after the child is killed
        let mut child = unsafe {
            let mut cmd = Command::new("sleep");
            cmd.arg("2")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            cmd.pre_exec(move || prepare_worker(fd));
            cmd.spawn().unwrap()
        };
        drop(lock);
        let blocked = home::flock_exclusive(&lock_path, LockMode::NonBlocking);
        assert!(
            matches!(blocked, Err(AppError::LockHeld { .. })),
            "child should still hold the lock after parent close"
        );
        child.kill().unwrap();
        child.wait().unwrap();
        home::flock_exclusive(&lock_path, LockMode::NonBlocking).unwrap();
    }

    #[test]
    fn write_prompt_keeps_prompt_txt_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let paths = TaskPaths {
            prompt: dir.path().join("prompt.txt"),
            trailer: dir.path().join("prompt.trailer.txt"),
            feed: dir.path().join("prompt.feed.txt"),
            output: dir.path().join("output.log"),
            runner_lock: dir.path().join("runner.lock"),
            exit_json: dir.path().join("exit.json"),
            callback_log: dir.path().join("callback.log"),
            delivery_lock: dir.path().join("delivery.lock"),
            container_cid: dir.path().join("container.cid"),
            dir: dir.path().to_path_buf(),
        };
        std::fs::create_dir_all(&paths.dir).unwrap();
        write_task_files(&paths, "hello", true).unwrap();
        let mut buf = String::new();
        File::open(&paths.prompt)
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        assert_eq!(buf, "hello");
        let feed = std::fs::read_to_string(&paths.feed).unwrap();
        assert!(feed.starts_with("hello"));
        assert!(feed.contains("--- homebased ---"));
    }

    #[tokio::test]
    async fn foreground_child_exit_is_confirmed_by_process_group_probe() {
        let mut cmd = TokioCommand::new("/usr/bin/true");
        cmd.process_group(0).kill_on_drop(true);
        let mut child = cmd.spawn().unwrap();
        let child_pgid = child.id().unwrap() as i32;
        assert!(child.wait().await.unwrap().success());
        let mut sent_signals = Vec::new();

        assert_eq!(
            cleanup_process_group_with(
                child_pgid,
                || ProcessGroupProbe::Exited,
                |signal| {
                    sent_signals.push(signal);
                    Ok(())
                },
                || {},
                CleanupTiming {
                    term_grace: Duration::ZERO,
                    kill_grace: Duration::ZERO,
                    poll: Duration::ZERO,
                },
            )
            .await,
            ProcessGroupExitEvidence::ConfirmedExited
        );
        assert!(sent_signals.is_empty());
    }

    #[tokio::test]
    async fn cancellation_cleanup_reaps_and_confirms_its_owned_group() {
        let mut cmd = TokioCommand::new("/bin/sleep");
        cmd.arg("30").process_group(0).kill_on_drop(true);
        let mut child = cmd.spawn().unwrap();
        let child_pgid = child.id().unwrap() as i32;

        assert_eq!(
            cancel_child_group(&mut child, child_pgid).await,
            ProcessGroupExitEvidence::ConfirmedExited
        );
        assert!(child.try_wait().unwrap().is_some());
    }

    #[tokio::test]
    async fn worker_self_cancels_from_its_persisted_marker() {
        let directory = tempfile::tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let id = TaskId::new();
        let paths = home.prepare_task(id).unwrap();
        let store = Store::open(&home.db_path()).unwrap();
        let mut row = store::new_queued_task(crate::store::NewTask {
            id,
            name: Some(crate::domain::TaskName::parse("self-cancel test").unwrap()),
            thread: crate::domain::ThreadId(uuid::Uuid::now_v7()),
            workload: crate::domain::Workload::Task(crate::domain::TaskWorkload {
                command: crate::invocation::CommandLine::try_from_argv(vec![
                    "/bin/sleep".into(),
                    "30".into(),
                ])
                .unwrap(),
            }),
            cwd: "/tmp".into(),
            timeout: Duration::from_secs(30),
            env: crate::domain::TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
            binary: "/bin/sleep".into(),
        });
        row.state = crate::domain::TaskState::Running {
            pid: Some(std::process::id() as i32),
        };
        store.insert_task(&row).unwrap();
        let _runner_lock = home::flock_exclusive(&paths.runner_lock, LockMode::Blocking).unwrap();
        let invocation = ChildInvocation {
            program: "/bin/sleep".into(),
            args: vec!["30".into()],
            stdin: StdinPolicy::Null,
            environment: Default::default(),
            managed_environment: None,
        };

        let database = home.db_path();
        let cancellation = tokio::spawn(async move {
            time::sleep(Duration::from_millis(50)).await;
            let cancellation_store = Store::open(&database).unwrap();
            assert!(matches!(
                cancellation_store.request_cancel(id).unwrap(),
                store::CancelResult::SignalWorker(_)
            ));
        });
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let exit = time::timeout(
            Duration::from_secs(5),
            run_child(&invocation, &row, &home, &paths, None, &store, &mut sigterm),
        )
        .await
        .unwrap()
        .unwrap();
        cancellation.await.unwrap();

        assert_eq!(exit.reason, ExitReason::Cancelled);
        assert_eq!(
            exit.process_group_exit_evidence,
            ProcessGroupExitEvidence::ConfirmedExited
        );
    }

    #[tokio::test]
    async fn unknown_or_surviving_group_is_not_confirmed_by_signal_success() {
        let timing = CleanupTiming {
            term_grace: Duration::ZERO,
            kill_grace: Duration::ZERO,
            poll: Duration::ZERO,
        };
        for probe_result in [ProcessGroupProbe::Unknown, ProcessGroupProbe::Alive] {
            let mut sent_signals = Vec::new();
            let evidence = cleanup_process_group_with(
                999_999,
                || probe_result,
                |signal| {
                    sent_signals.push(signal);
                    Ok(())
                },
                || {},
                timing,
            )
            .await;

            assert_eq!(evidence, ProcessGroupExitEvidence::Unconfirmed);
            assert_eq!(sent_signals, [Signal::SIGTERM, Signal::SIGKILL]);
        }

        let mut probes = [
            ProcessGroupProbe::Alive,
            ProcessGroupProbe::Alive,
            ProcessGroupProbe::Exited,
        ]
        .into_iter();
        assert_eq!(
            cleanup_process_group_with(
                999_999,
                || probes.next().unwrap_or(ProcessGroupProbe::Exited),
                |_| Ok(()),
                || {},
                timing,
            )
            .await,
            ProcessGroupExitEvidence::Unconfirmed,
            "an exit probe after the kill grace must not hide the grace timeout"
        );
    }
}
