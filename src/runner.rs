//! `task-run`: lock, setsid, spawn, timeout, `exit.json`, callback.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

use nix::sys::signal::{kill, Signal};
use nix::unistd::{getpgrp, Pid};
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
    if unsafe { libc::setsid() } == -1 {
        return Err(io::Error::last_os_error());
    }
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
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
    let _ = store.cas_exit(id, ProcessStatus::Running, to, &reason)?;
    let row = store.require_task(id)?;
    let reports = store.reports(id)?;
    let lost = false;
    let event = exit_event(&row, &reports, paths.dir.clone(), lost);
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
) -> Result<ExitReason, AppError> {
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
        .kill_on_drop(true);
    if argv.stdin_prompt {
        cmd.stdin(Stdio::piped());
    } else {
        cmd.stdin(Stdio::null());
    }
    #[cfg(target_os = "linux")]
    unsafe {
        cmd.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let mut child = cmd.spawn().map_err(|err| AppError::Internal {
        message: format!("spawn agent: {err}"),
    })?;
    if argv.stdin_prompt {
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(&feed)
                .await
                .map_err(|err| AppError::Internal {
                    message: format!("write prompt: {err}"),
                })?;
        }
    }

    let mut sigterm = signal(SignalKind::terminate()).map_err(|err| AppError::Internal {
        message: format!("signal: {err}"),
    })?;
    let deadline = Instant::now() + timeout;
    let remaining = deadline.saturating_duration_since(Instant::now());
    tokio::select! {
        status = child.wait() => {
            Ok(status_to_reason(status, false, timeout))
        }
        _ = sigterm.recv() => {
            warn!(%id, "SIGTERM: forwarding to agent");
            forward_sigterm();
            Ok(wait_then_kill(&mut child, true, timeout).await)
        }
        _ = time::sleep(remaining) => {
            warn!(%id, "timeout: killing agent group");
            forward_sigterm();
            let _ = wait_then_kill(&mut child, false, timeout).await;
            Ok(ExitReason::Timeout {
                secs: timeout.as_secs(),
            })
        }
    }
}

fn status_to_reason(
    status: io::Result<std::process::ExitStatus>,
    cancel_requested: bool,
    timeout: Duration,
) -> ExitReason {
    let _ = timeout;
    if cancel_requested {
        return ExitReason::Cancelled;
    }
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

async fn wait_then_kill(
    child: &mut tokio::process::Child,
    cancel_requested: bool,
    timeout: Duration,
) -> ExitReason {
    match time::timeout(KILL_GRACE, child.wait()).await {
        Ok(status) => status_to_reason(status, cancel_requested, timeout),
        Err(_) => {
            kill_group_except_self();
            let status = child.wait().await;
            if cancel_requested {
                ExitReason::Cancelled
            } else {
                status_to_reason(status, false, timeout)
            }
        }
    }
}

fn forward_sigterm() {
    let pgid = getpgrp();
    let _ = kill(Pid::from_raw(-pgid.as_raw()), Signal::SIGTERM);
}

fn kill_group_except_self() {
    let me = std::process::id() as i32;
    let pgid = getpgrp().as_raw();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        if pid == me {
            continue;
        }
        if proc_pgid(pid) == Some(pgid) {
            let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
        }
    }
}

fn proc_pgid(pid: i32) -> Option<i32> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = stat.rsplit_once(')')?.1;
    let mut fields = rest.split_whitespace();
    let _state = fields.next()?;
    let _ppid = fields.next()?;
    fields.next()?.parse().ok()
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
