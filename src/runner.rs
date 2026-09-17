//! `task-run`: lock, setsid, spawn, timeout, `exit.json`, callback.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

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

use crate::agents::{ArgvInputs, build_argv};
use crate::callback::{deliver_exit_event, exit_event};
use crate::domain::{AgentKind, ExitReason, ProcessStatus, TaskId, TaskRow};
use crate::error::AppError;
use crate::home::{self, Home, LockMode, TaskPaths};
use crate::report::REPORT_TRAILER;
use crate::store::{self, Store};

const KILL_GRACE: Duration = Duration::from_secs(10);

/// Spawn `homebased task-run` with an inherited exclusive flock.
pub fn spawn_task_run(home: &Home, id: TaskId, lock: File) -> Result<u32, AppError> {
    let exe = std::env::current_exe().map_err(|err| AppError::Internal {
        message: format!("current_exe: {err}"),
    })?;
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

fn prepare_worker(fd: RawFd) -> io::Result<()> {
    setsid().map_err(io::Error::from)?;
    let borrowed = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
    let flags = fcntl(borrowed, FcntlArg::F_GETFD).map_err(io::Error::from)?;
    let mut fdflag = FdFlag::from_bits_truncate(flags);
    fdflag.remove(FdFlag::FD_CLOEXEC);
    fcntl(borrowed, FcntlArg::F_SETFD(fdflag)).map_err(io::Error::from)?;
    Ok(())
}

/// Worker entry: hold the inherited lock until exit.json and callback complete.
pub async fn run(home: Home, id: TaskId, lock_fd: i32) -> Result<(), AppError> {
    // keep the inherited flock open for the process lifetime
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
    if store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)?
        .is_none()
    {
        info!(%id, "CAS Queued→Running failed; exiting without spawn");
        return Ok(());
    }
    let pid = std::process::id() as i32;
    store.set_pid(id, pid)?;
    let row = store.require_task(id)?;
    let paths = home.task_paths(id);
    let prompt_file = if row.agent.kind == AgentKind::Grok {
        Some(paths.feed.as_path())
    } else {
        None
    };
    let argv = build_argv(&ArgvInputs::from(&row), &row.binary, prompt_file);
    // the daemon always writes the feed before spawning this worker
    let feed = std::fs::read(&paths.feed)?;

    let reason = match run_agent(&argv, &row, &home, &paths, feed, &store, &mut sigterm).await {
        Ok(reason) => reason,
        Err(err) => ExitReason::SpawnFailed {
            message: err.to_string(),
        },
    };

    store::write_exit_json(&paths.exit_json, &reason)?;
    let row = match store.cas_exit(id, ProcessStatus::Running, &reason)? {
        Some(row) => row,
        None => {
            let current = store.require_task(id)?;
            warn!(%id, status = %current.status(), "cas_exit failed");
            current
        }
    };
    let reports = store.reports(id)?;
    let event = exit_event(&row, &reports, paths.dir.clone());
    deliver_exit_event(&store, &home, &row, &event)?;
    Ok(())
}

async fn run_agent(
    argv: &crate::agents::ChildArgv,
    row: &TaskRow,
    home: &Home,
    paths: &TaskPaths,
    feed: Vec<u8>,
    store: &Store,
    sigterm: &mut SignalStream,
) -> Result<ExitReason, AppError> {
    let id = row.id;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.output)?;
    let stdout = log.try_clone()?;
    let stderr = log;

    let mut cmd = TokioCommand::new(&argv.program);
    cmd.args(&argv.args)
        .current_dir(&row.cwd)
        .env("PATH", &row.env.path)
        .env("HOME", &row.env.home)
        .env("HOMEBASED_TASK_ID", id.to_string())
        .env("HOMEBASED_HOME", home.root())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .process_group(0);
    if argv.stdin_prompt {
        cmd.stdin(Stdio::piped());
    } else {
        cmd.stdin(Stdio::null());
    }
    #[cfg(target_os = "linux")]
    unsafe {
        cmd.pre_exec(|| {
            prctl::set_pdeathsig(Some(Signal::SIGKILL)).map_err(io::Error::from)?;
            Ok(())
        });
    }

    let mut child = cmd.spawn().map_err(|err| AppError::Internal {
        message: format!("spawn agent: {err}"),
    })?;
    let agent_pgid = child.id().ok_or_else(|| AppError::Internal {
        message: "agent pid missing after spawn".into(),
    })? as i32;
    if argv.stdin_prompt
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

    tokio::select! {
        status = child.wait() => {
            status_to_reason(status)
        }
        _ = sigterm.recv() => {
            let cancelled = store.require_task(id)?.cancel_requested_at.is_some();
            warn!(%id, cancelled, "SIGTERM: forwarding to agent group");
            forward_sigterm(agent_pgid);
            wait_then_kill(&mut child, agent_pgid).await;
            if cancelled {
                Ok(ExitReason::Cancelled)
            } else {
                Ok(ExitReason::Signal { signal: 15 })
            }
        }
        _ = time::sleep(row.timeout) => {
            warn!(%id, "timeout: killing agent group");
            forward_sigterm(agent_pgid);
            wait_then_kill(&mut child, agent_pgid).await;
            Ok(ExitReason::Timeout {
                secs: row.timeout.as_secs(),
            })
        }
    }
}

/// A Unix wait status is either an exit code or a terminating signal; neither
/// means the child was only stopped, which `wait` on a non-traced child never
/// reports.
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
        message: format!("agent wait status has neither code nor signal: {status:?}"),
    })
}

async fn wait_then_kill(child: &mut tokio::process::Child, agent_pgid: i32) {
    if time::timeout(KILL_GRACE, child.wait()).await.is_ok() {
        return;
    }
    if let Err(err) = kill(Pid::from_raw(-agent_pgid), Signal::SIGKILL) {
        warn!(agent_pgid, "SIGKILL agent group: {err}");
    }
    if let Err(err) = child.wait().await {
        warn!(agent_pgid, "wait after SIGKILL: {err}");
    }
}

fn forward_sigterm(agent_pgid: i32) {
    if let Err(err) = kill(Pid::from_raw(-agent_pgid), Signal::SIGTERM) {
        warn!(agent_pgid, "SIGTERM agent group: {err}");
    }
}

/// Write prompt files for a new task.
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
}
