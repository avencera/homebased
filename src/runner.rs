//! `task-run`: lock, setsid, spawn, timeout, `exit.json`, callback.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

use nix::fcntl::{fcntl, FcntlArg, FdFlag};
#[cfg(target_os = "linux")]
use nix::sys::prctl;
use nix::sys::signal::{kill, Signal};
use nix::unistd::{setsid, Pid};
use tokio::io::AsyncWriteExt;
use tokio::process::Command as TokioCommand;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{self, Instant};
use tracing::{info, warn};

use crate::agents::build_argv;
use crate::callback::{deliver_exit_event, exit_event};
use crate::domain::{status_from_exit, ExitReason, ProcessStatus, TaskId};
use crate::error::AppError;
use crate::home::{self, Home, TaskPaths};
use crate::report::trailer_text;
use crate::spec::NormalizedSpec;
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
    drop(lock);
    std::thread::spawn(move || {
        let mut child = child;
        let _ = child.wait();
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
    home.ensure()?;
    let store = Store::open(&home.db_path())?;
    if !store.cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)? {
        info!(%id, "CAS Queued→Running failed; exiting without spawn");
        return Ok(());
    }
    let pid = std::process::id() as i32;
    store.set_pid(id, pid)?;
    let row = store.require_task(id)?;
    let paths = home.task_paths(id);
    let spec = NormalizedSpec {
        api_version: crate::domain::API_VERSION,
        agent: row.agent.kind,
        model: row.agent.model.clone(),
        thread: row.thread,
        cwd: row.cwd.clone(),
        prompt: String::new(),
        timeout: row.timeout,
        extra_args: row.extra_args.clone(),
        report_trailer: row.report_trailer,
    };
    let prompt_file = if spec.agent == crate::domain::AgentKind::Grok {
        Some(paths.feed.as_path())
    } else {
        None
    };
    let argv = build_argv(&spec, &row.binary, prompt_file);
    let feed = std::fs::read(&paths.feed).or_else(|_| std::fs::read(&paths.prompt))?;

    let reason = match run_agent(
        &argv,
        &row.cwd,
        &row.env.path,
        &row.env.home,
        id,
        &home,
        &paths,
        feed,
        row.timeout,
        &store,
    )
    .await
    {
        Ok(reason) => reason,
        Err(err) => ExitReason::SpawnFailed {
            message: err.to_string(),
        },
    };

    store::write_exit_json(&paths.exit_json, &reason)?;
    let to = status_from_exit(&reason);
    if !store.cas_exit(id, ProcessStatus::Running, to, &reason)? {
        let current = store.require_task(id)?;
        warn!(%id, status = %current.status, "cas_exit failed");
    }
    let row = store.require_task(id)?;
    let reports = store.reports(id)?;
    let event = exit_event(&row, &reports, paths.dir.clone());
    deliver_exit_event(&store, &home, &row, &event)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_agent(
    argv: &crate::agents::ChildArgv,
    cwd: &Path,
    path: &str,
    home_env: &str,
    id: TaskId,
    home: &Home,
    paths: &TaskPaths,
    feed: Vec<u8>,
    timeout: Duration,
    store: &Store,
) -> Result<ExitReason, AppError> {
    let mut sigterm = signal(SignalKind::terminate()).map_err(|err| AppError::Internal {
        message: format!("signal: {err}"),
    })?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.output)?;
    let stdout = log.try_clone()?;
    let stderr = log;

    let mut cmd = TokioCommand::new(&argv.program);
    cmd.args(&argv.args)
        .current_dir(cwd)
        .env("PATH", path)
        .env("HOME", home_env)
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
    if argv.stdin_prompt {
        if let Some(mut stdin) = child.stdin.take() {
            tokio::spawn(async move {
                match stdin.write_all(&feed).await {
                    Ok(()) => {}
                    Err(err) if err.kind() == io::ErrorKind::BrokenPipe => {}
                    Err(err) => warn!("prompt stdin write: {err}"),
                }
            });
        }
    }

    let deadline = Instant::now() + timeout;
    let remaining = deadline.saturating_duration_since(Instant::now());
    tokio::select! {
        status = child.wait() => {
            Ok(status_to_reason(status))
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
        _ = time::sleep(remaining) => {
            warn!(%id, "timeout: killing agent group");
            forward_sigterm(agent_pgid);
            wait_then_kill(&mut child, agent_pgid).await;
            Ok(ExitReason::Timeout {
                secs: timeout.as_secs(),
            })
        }
    }
}

fn status_to_reason(status: io::Result<std::process::ExitStatus>) -> ExitReason {
    match status {
        Ok(st) => {
            if let Some(code) = st.code() {
                ExitReason::Exit { code }
            } else {
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    if let Some(sig) = st.signal() {
                        return ExitReason::Signal { signal: sig };
                    }
                }
                ExitReason::Exit { code: -1 }
            }
        }
        Err(err) => ExitReason::SpawnFailed {
            message: err.to_string(),
        },
    }
}

async fn wait_then_kill(child: &mut tokio::process::Child, agent_pgid: i32) {
    match time::timeout(KILL_GRACE, child.wait()).await {
        Ok(_) => {}
        Err(_) => {
            let _ = kill(Pid::from_raw(-agent_pgid), Signal::SIGKILL);
            let _ = child.wait().await;
        }
    }
}

fn forward_sigterm(agent_pgid: i32) {
    let _ = kill(Pid::from_raw(-agent_pgid), Signal::SIGTERM);
}

/// Write prompt files for a new task.
pub fn write_task_files(
    paths: &TaskPaths,
    prompt: &str,
    report_trailer: bool,
) -> Result<(), AppError> {
    let trailer = if report_trailer {
        Some(trailer_text())
    } else {
        None
    };
    paths.write_prompt(prompt, trailer)
}

/// Open `runner.lock` and take the exclusive flock before spawn.
pub fn lock_before_spawn(paths: &TaskPaths) -> Result<File, AppError> {
    home::flock_exclusive(&paths.runner_lock, false)
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
        let lock = home::flock_exclusive(&lock_path, false).unwrap();
        let fd = lock.as_raw_fd();
        let mut child = unsafe {
            let mut cmd = Command::new("/bin/sh");
            cmd.arg("-c")
                .arg("sleep 2")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            cmd.pre_exec(move || prepare_worker(fd));
            cmd.spawn().unwrap()
        };
        drop(lock);
        let blocked = home::flock_exclusive(&lock_path, true);
        assert!(
            blocked.is_err(),
            "child should still hold the lock after parent close"
        );
        let _ = child.kill();
        let _ = child.wait();
        let _ = home::flock_exclusive(&lock_path, true).unwrap();
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
