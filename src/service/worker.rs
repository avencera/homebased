//! `service-run`: the worker that supervises one service
//!
//! Like `task-run`, it runs in its own session and holds the service's
//! `runner.lock` for its whole life, so the daemon can restart without
//! touching the service and can tell a vanished worker from a live one. It
//! spawns the command in its own process group, decides readiness, enforces
//! the readiness timeout and the `--max` lifetime, stops the group when asked,
//! and records how the service ended exactly once

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

use chrono::Utc;
use nix::fcntl::{FcntlArg, FdFlag, fcntl};
#[cfg(target_os = "linux")]
use nix::sys::prctl;
#[cfg(target_os = "linux")]
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use regex::Regex;
use tokio::net::TcpStream;
use tokio::process::Command as TokioCommand;
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::{self, Instant, MissedTickBehavior};
use tracing::warn;

use super::{Ending, Readiness, Service, ServiceEnd, ServiceId};
use crate::cleanup;
use crate::domain::ExitReason;
use crate::error::AppError;
use crate::home::{self, Home, LockMode, ServicePaths};
use crate::run_env;
use crate::runner;
use crate::store::Store;

/// How often the worker checks readiness and its deadlines
const TICK: Duration = Duration::from_millis(250);

/// Ticks between reads of a stop request, which SIGTERM normally brings at once
const STOP_POLL_TICKS: u32 = 4;

/// How long one port probe may take
const PROBE_TIMEOUT: Duration = Duration::from_millis(250);

/// Spawn the `service-run` worker of `id` with the caller's environment
///
/// The environment goes to the worker process and from there to the
/// service; it is never written to disk. The worker inherits the service's
/// lock, taken here before the spawn, so the service is never unwatched
pub fn spawn(home: &Home, id: ServiceId, env: &BTreeMap<String, String>) -> Result<u32, AppError> {
    let paths = home.service_paths(id);
    std::fs::create_dir_all(&paths.dir)?;
    let lock = home::flock_exclusive(&paths.runner_lock, LockMode::NonBlocking)?;
    let fd = lock.as_raw_fd();
    let worker_stderr = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.worker_log)
    {
        Ok(log) => Stdio::from(log),
        Err(err) => {
            warn!(%id, "open service worker log: {err}");
            Stdio::null()
        }
    };
    let mut cmd = StdCommand::new(runner::task_run_executable()?);
    cmd.env_clear().envs(env);
    run_env::scrub(&mut cmd)
        .arg("service-run")
        .arg("--home")
        .arg(home.root())
        .arg("--id")
        .arg(id.to_string())
        .arg("--lock-fd")
        .arg(fd.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(worker_stderr);
    unsafe {
        cmd.pre_exec(move || runner::prepare_worker(fd));
    }
    let child = cmd.spawn().map_err(|err| AppError::Internal {
        message: format!("spawn service-run: {err}"),
    })?;
    let pid = child.id();
    // the worker holds the same open file description, so the lock stays held
    drop(lock);
    std::thread::spawn(move || {
        let mut child = child;
        // reap the worker so it does not linger as a zombie of the daemon
        if let Err(err) = child.wait() {
            warn!("reap service-run: {err}");
        }
    });
    Ok(pid)
}

/// Worker entry: supervise the service until it ends, then record the end
pub async fn run(home: Home, id: ServiceId, lock_fd: i32) -> Result<(), AppError> {
    let lock = unsafe { File::from_raw_fd(lock_fd) };
    // installed first, so a stop in the startup window is latched, not fatal
    let mut sigterm = signal(SignalKind::terminate()).map_err(|err| AppError::Internal {
        message: format!("signal: {err}"),
    })?;
    // the service must not hold the lock, or a leftover could hide a lost worker
    fcntl(&lock, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)).map_err(io::Error::from)?;

    let store = Store::open_with_busy_timeout(&home.db_path(), runner::WORKER_BUSY_TIMEOUT)?;
    let record = store
        .service_record(id)?
        .ok_or(AppError::ServiceNotFound { service: id })?;
    if !record.is_live() {
        return Ok(());
    }
    if record.stop_request.is_some() {
        store.end_service(id, ending(ServiceEnd::Stopped, None, None))?;
        return Ok(());
    }

    let paths = home.service_paths(id);
    let ending = supervise(&store, &record.service, &paths, &mut sigterm).await;
    store.end_service(id, ending)?;
    drop(lock);
    Ok(())
}

fn ending(end: ServiceEnd, exit: Option<ExitReason>, detail: Option<String>) -> Ending {
    Ending {
        end,
        exit,
        detail,
        at: Utc::now(),
    }
}

/// Why the worker stops a service that is still running
enum Stop {
    /// A stop request or SIGTERM; the store applies a recorded request
    Signalled,
    /// Not ready by the readiness timeout
    NotReady,
    /// The `--max` lifetime elapsed
    MaxElapsed,
}

async fn supervise(
    store: &Store,
    service: &Service,
    paths: &ServicePaths,
    sigterm: &mut tokio::signal::unix::Signal,
) -> Ending {
    // a port that already answers would make someone else's server count as this one
    if let Readiness::Port { port } = service.readiness
        && port_open(port).await
    {
        return ending(
            ServiceEnd::NotReady,
            None,
            Some(format!(
                "port {port} already accepts connections before the service started; stop what listens there or pick another port"
            )),
        );
    }

    let mut child = match spawn_child(service, &paths.output) {
        Ok(child) => child,
        Err(err) => {
            let message = format!("could not start {}: {err}", service.command[0]);
            if let Err(write) = append(&paths.output, &format!("homebased: {message}\n")) {
                warn!(id = %service.id, "write service log: {write}");
            }
            return ending(
                ServiceEnd::NotReady,
                Some(ExitReason::SpawnFailed {
                    message: err.to_string(),
                }),
                Some(message),
            );
        }
    };
    let Some(pgid) = child.id().map(|pid| pid as i32) else {
        let exit = runner::status_to_reason(child.wait().await).ok();
        return ending(ServiceEnd::NotReady, exit, Some("exited at once".into()));
    };
    let identity = cleanup::process_identity(Pid::from_raw(pgid)).ok();
    let mut ready = service.readiness == Readiness::Spawned;
    if let Err(err) = store.set_service_started(service.id, identity, ready, Utc::now()) {
        warn!(id = %service.id, "record service start: {err}");
    }

    let started = Instant::now();
    let ready_deadline = started + service.ready_timeout;
    let max_deadline = started + service.max;
    let mut probe = Probe::new(&service.readiness, &paths.output);
    let mut tick = time::interval(TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut ticks: u32 = 0;
    let stop = loop {
        tokio::select! {
            status = child.wait() => {
                let exit = runner::status_to_reason(status).ok();
                // the leader is gone, but what it started may still hold the port
                runner::cleanup_process_group(pgid).await;
                return match ready {
                    true => ending(ServiceEnd::Exited, exit, None),
                    false => ending(ServiceEnd::NotReady, exit, Some("exited before it was ready".into())),
                };
            }
            _ = sigterm.recv() => break Stop::Signalled,
            _ = tick.tick() => {
                ticks = ticks.wrapping_add(1);
                let now = Instant::now();
                if !ready && probe.ready().await {
                    ready = true;
                    if let Err(err) = store.set_service_ready(service.id, Utc::now()) {
                        warn!(id = %service.id, "record service ready: {err}");
                    }
                }
                if !ready && now >= ready_deadline {
                    break Stop::NotReady;
                }
                if now >= max_deadline {
                    break Stop::MaxElapsed;
                }
                if ticks.is_multiple_of(STOP_POLL_TICKS) && stop_requested(store, service.id) {
                    break Stop::Signalled;
                }
            }
        }
    };

    let (_, natural) = runner::cancel_child_group(&mut child, pgid).await;
    match stop {
        Stop::NotReady => ending(
            ServiceEnd::NotReady,
            natural,
            Some(format!(
                "not ready within {} ({})",
                humantime::format_duration(service.ready_timeout),
                service.readiness
            )),
        ),
        Stop::MaxElapsed => ending(ServiceEnd::MaxElapsed, natural, None),
        // a recorded request replaces this end when the store applies it; a
        // SIGTERM nobody asked homebased for, such as a shutdown, is an end
        // the owner did not choose
        Stop::Signalled => ending(
            ServiceEnd::Exited,
            natural.or(Some(ExitReason::Signal { signal: 15 })),
            Some("the worker was sent SIGTERM".into()),
        ),
    }
}

fn stop_requested(store: &Store, id: ServiceId) -> bool {
    match store.service_record(id) {
        Ok(Some(record)) => record.stop_request.is_some(),
        Ok(None) => false,
        Err(err) => {
            warn!(%id, "read service stop request: {err}");
            false
        }
    }
}

fn spawn_child(service: &Service, output: &Path) -> io::Result<tokio::process::Child> {
    let log = OpenOptions::new().create(true).append(true).open(output)?;
    let stderr = log.try_clone()?;
    let mut cmd = TokioCommand::new(&service.command[0]);
    run_env::scrub(cmd.as_std_mut());
    cmd.args(&service.command[1..])
        .current_dir(&service.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .process_group(0);
    // Python block-buffers stdout into a file, which would hold a ready line
    // back until the buffer fills; an explicit setting from the caller wins
    if std::env::var_os("PYTHONUNBUFFERED").is_none() {
        cmd.env("PYTHONUNBUFFERED", "1");
    }
    #[cfg(target_os = "linux")]
    unsafe {
        cmd.pre_exec(|| {
            prctl::set_pdeathsig(Some(Signal::SIGKILL)).map_err(io::Error::from)?;
            Ok(())
        });
    }
    cmd.spawn()
}

fn append(path: &Path, text: &str) -> io::Result<()> {
    use std::io::Write;
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?
        .write_all(text.as_bytes())
}

/// Whether a local TCP port accepts a connection on 127.0.0.1 or ::1
///
/// Both, because a server bound to `localhost` may listen only on ::1
pub(crate) async fn port_open(port: u16) -> bool {
    for host in ["127.0.0.1", "::1"] {
        let connect = TcpStream::connect((host, port));
        if matches!(time::timeout(PROBE_TIMEOUT, connect).await, Ok(Ok(_))) {
            return true;
        }
    }
    false
}

/// Readiness check of one service
enum Probe {
    Spawned,
    Port(u16),
    Line(LineScan),
}

impl Probe {
    fn new(readiness: &Readiness, output: &Path) -> Self {
        match readiness {
            Readiness::Spawned => Self::Spawned,
            Readiness::Port { port } => Self::Port(*port),
            // validated at start; an invalid pattern never matches
            Readiness::Line { pattern } => Self::Line(LineScan {
                path: output.to_path_buf(),
                pattern: Regex::new(pattern).ok(),
                offset: 0,
                partial: String::new(),
            }),
        }
    }

    async fn ready(&mut self) -> bool {
        match self {
            Self::Spawned => true,
            Self::Port(port) => port_open(*port).await,
            Self::Line(scan) => scan.matched().unwrap_or_else(|err| {
                warn!("read service output: {err}");
                false
            }),
        }
    }
}

/// Reads the output log from where it last stopped and matches each line
struct LineScan {
    path: PathBuf,
    pattern: Option<Regex>,
    offset: u64,
    partial: String,
}

impl LineScan {
    /// Whether a line written since the last call matches
    fn matched(&mut self) -> io::Result<bool> {
        let Some(pattern) = &self.pattern else {
            return Ok(false);
        };
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err),
        };
        file.seek(SeekFrom::Start(self.offset))?;
        let mut bytes = Vec::new();
        self.offset += file.read_to_end(&mut bytes)? as u64;
        self.partial.push_str(&String::from_utf8_lossy(&bytes));

        let mut found = false;
        while let Some(end) = self.partial.find('\n') {
            let line: String = self.partial.drain(..=end).collect();
            let line = line.trim_end_matches(['\n', '\r']);
            found |= pattern.is_match(&strip_ansi(line));
        }
        // a prompt such as `Ready >` may never end its line
        Ok(found || pattern.is_match(&strip_ansi(&self.partial)))
    }
}

/// `text` without terminal color and cursor sequences, which dev servers
/// print around the words a ready pattern names
fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            out.push(c);
            continue;
        }
        if chars.peek() != Some(&'[') {
            continue;
        }
        chars.next();
        // parameters and intermediates run until a final byte in `@`..=`~`
        for c in chars.by_ref() {
            if ('@'..='~').contains(&c) {
                break;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ready_line_matches_through_color_and_across_partial_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.log");
        let mut scan = LineScan {
            path: path.clone(),
            pattern: Regex::new(r"^Ready on http://localhost:\d+$").ok(),
            offset: 0,
            partial: String::new(),
        };
        assert!(!scan.matched().unwrap(), "no log yet");

        append(&path, "building...\n\u{1b}[32mReady on http://loc").unwrap();
        assert!(!scan.matched().unwrap());
        append(&path, "alhost:8787\u{1b}[0m\nmore\n").unwrap();
        assert!(scan.matched().unwrap());
        // the matched line is consumed, so later output alone does not match
        append(&path, "request served\n").unwrap();
        assert!(!scan.matched().unwrap());
    }
}
