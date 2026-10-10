//! Origin inbox delivery: find the saved origin, then send through T3, the
//! Claude session socket, or `codex queue`
//!
//! Every send takes a [`SendGate`]. A gate with a check rechecks a numbered
//! notice under the delivery lock just before the write; a gate without one
//! sends unconditionally

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, BorrowedFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tracing::warn;

use super::claude_inbox::{ClaudeInbox, ClaudeSession};
use super::send_check::{SendFailure, SendGate};
use super::stale_context::ContextUse;
use crate::daemon::compaction_log::{self, Trigger};
use crate::domain::{TaskEnv, ThreadId};
use crate::error::AppError;
use crate::submission::CallbackContext;
use crate::t3::{
    Protocol, ProviderThread, T3Env, TurnStart, WakeOutcome, compact_thread, owner,
    wake_thread_checked,
};

/// Per-attempt bound for one `codex queue` child, including cleanup.
pub const QUEUE_ATTEMPT_TIMEOUT_SECS: u64 = 20;
/// [`QUEUE_ATTEMPT_TIMEOUT_SECS`] as a [`Duration`].
pub const QUEUE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(QUEUE_ATTEMPT_TIMEOUT_SECS);

/// Extra time to reap a child after its process group receives SIGKILL.
const QUEUE_REAP_TIMEOUT_SECS: u64 = 2;

/// Maximum drain wait for each of stdout and stderr after direct-child exit.
const QUEUE_PIPE_DRAIN_TIMEOUT_SECS: u64 = 2;

/// Check immutable origin context before reserving a command attempt
pub(crate) fn check_saved_callback(context: &CallbackContext) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    if !context.cwd.is_absolute() || !context.cwd.is_dir() {
        return Err(format!(
            "saved callback directory is unavailable: {}",
            context.cwd.display()
        ));
    }
    let binary = context.codex.path().ok_or_else(|| {
        context
            .codex
            .unavailable_reason()
            .unwrap_or("saved Codex executable is unavailable")
            .to_owned()
    })?;
    if !binary.is_absolute() {
        return Err("saved Codex executable path is not absolute".into());
    }
    let metadata = std::fs::metadata(binary)
        .map_err(|error| format!("saved Codex executable is unavailable: {error}"))?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(format!(
            "saved Codex executable is not executable: {}",
            binary.display()
        ));
    }
    Ok(())
}

/// Saved origin for one task thread
pub(crate) enum OriginSession {
    /// A live Claude session can take the event now
    Reachable(ReachableOrigin),
    /// A live Claude session that a T3 orchestration V2 thread owns
    ///
    /// V2 keeps the Claude process between turns, but shows a turn that a
    /// socket message starts as "Background task completed.", so T3 takes the
    /// event and the socket is the fallback
    T3Claude(ReachableOrigin),
    /// A Claude session with a transcript but no live process
    Stopped,
    /// A Codex thread that may belong to T3 will be tried there first
    T3Codex,
    /// A Codex thread without a T3 owner uses `codex queue`
    Codex,
}

/// Saved origin that accepts an event without a wake
pub(crate) struct ReachableOrigin(ClaudeInbox);

impl ReachableOrigin {
    /// Make one bounded send through the Claude session socket
    pub(crate) fn send(
        self,
        thread: ThreadId,
        line: &str,
        log_path: &Path,
        gate: SendGate<'_>,
    ) -> Result<(), SendFailure> {
        self.0.send(thread, line, log_path, gate)
    }
}

/// Find which saved origin owns `thread` for a direct message
///
/// A known Claude session with no live process is [`OriginSession::Stopped`]
pub(super) fn find_saved_origin(
    context: &CallbackContext,
    thread: ThreadId,
) -> Result<OriginSession, String> {
    match ClaudeInbox::find(Path::new(&context.env.home), thread)? {
        ClaudeSession::Live(inbox) => {
            let origin = ReachableOrigin(inbox);
            match owner(&t3_env(context), ProviderThread::Claude(thread)) {
                Ok(Some(Protocol::V2)) => Ok(OriginSession::T3Claude(origin)),
                Ok(Some(Protocol::V1) | None) => Ok(OriginSession::Reachable(origin)),
                Err(error) => {
                    warn!(%thread, "T3 ownership unknown, using the Claude session socket: {error}");
                    Ok(OriginSession::Reachable(origin))
                }
            }
        }
        ClaudeSession::Stopped => Ok(OriginSession::Stopped),
        ClaudeSession::Unknown => Ok(OriginSession::Codex),
    }
}

fn t3_env(context: &CallbackContext) -> T3Env {
    T3Env::new(
        PathBuf::from(&context.env.home),
        context.env.path.clone().into(),
    )
}

/// Find which saved origin owns `thread` for an inbox callback
pub(crate) fn find_saved_inbox_origin(
    context: &CallbackContext,
    thread: ThreadId,
) -> Result<OriginSession, String> {
    match find_saved_origin(context, thread)? {
        OriginSession::Codex => match owner(&t3_env(context), ProviderThread::Codex(thread)) {
            Ok(None) => Ok(OriginSession::Codex),
            // try T3 when its state database could not confirm ownership
            Ok(Some(_)) | Err(_) => Ok(OriginSession::T3Codex),
        },
        origin => Ok(origin),
    }
}

/// Durable record that T3 may already have taken one event
///
/// T3 drops a repeated command, so once a send's reply is lost every later
/// attempt goes through T3 alone. A socket or `codex queue` fallback could
/// deliver a second copy. The record survives a daemon restart
pub(crate) struct PendingT3Send<I = IntentFs>(PathBuf, I);

/// File operations for saving a T3 intent before a request
pub(crate) trait IntentIo {
    /// Create the staging file
    fn create(&self, path: &Path) -> io::Result<fs::File> {
        fs::File::create(path)
    }

    /// Write the complete provider record
    fn write(&self, file: &mut fs::File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(bytes)
    }

    /// Make the record data durable
    fn sync_file(&self, file: &fs::File) -> io::Result<()> {
        file.sync_all()
    }

    /// Make the final record name durable
    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        fs::File::open(path)?.sync_all()
    }
}

/// Filesystem operations used by production delivery
pub(crate) struct IntentFs;

impl IntentIo for IntentFs {}

impl PendingT3Send {
    /// Use the filesystem to keep the intent at `path`
    pub(crate) fn new(path: PathBuf) -> Self {
        Self(path, IntentFs)
    }
}

impl<I: IntentIo> PendingT3Send<I> {
    fn temporary_path(&self) -> PathBuf {
        let mut path = self.0.as_os_str().to_os_string();
        path.push(".tmp");
        PathBuf::from(path)
    }

    fn parent(&self) -> &Path {
        self.0
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
    }

    fn get(&self, thread: ThreadId) -> Result<Option<ProviderThread>, SendFailure> {
        // staging files cannot represent a send; discard remnants of an interrupted save
        let _ = fs::remove_file(self.temporary_path());
        let content = match fs::read_to_string(&self.0) {
            Ok(content) => content,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("read pending T3 send: {error}").into()),
        };
        match content.trim() {
            "claude" => Ok(Some(ProviderThread::Claude(thread))),
            "codex" => Ok(Some(ProviderThread::Codex(thread))),
            _ => Err("invalid pending T3 send record; fallback is blocked"
                .to_string()
                .into()),
        }
    }

    fn record(&self, provider_thread: ProviderThread) -> Result<(), SendFailure> {
        let provider = match provider_thread {
            ProviderThread::Claude(_) => "claude",
            ProviderThread::Codex(_) => "codex",
        };
        let temporary = self.temporary_path();
        let save = || -> io::Result<()> {
            let mut file = self.1.create(&temporary)?;
            self.1.write(&mut file, provider.as_bytes())?;
            self.1.sync_file(&file)?;
            fs::rename(&temporary, &self.0)?;
            self.1.sync_directory(self.parent())
        };

        let result = save();
        let _ = fs::remove_file(&temporary);
        result
            .map_err(|error| format!("record pending T3 send {}: {error}", self.0.display()).into())
    }

    fn sync(&self) -> Result<(), SendFailure> {
        let sync = || -> io::Result<()> {
            self.1.sync_file(&fs::File::open(&self.0)?)?;
            self.1.sync_directory(self.parent())
        };
        sync().map_err(|error| format!("sync pending T3 send {}: {error}", self.0.display()).into())
    }

    fn clear(&self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Result of a retry that may go only through T3
pub(crate) enum PendingRetry {
    /// T3 took the event, or already had it
    Delivered,
    /// T3 again may or may not have taken it
    Uncertain(String),
    /// T3 cannot take it now and nothing was sent
    Blocked(String),
}

/// Retry through T3 alone when an earlier send may have reached it
///
/// `None` when no earlier T3 send is pending, so any route may deliver
pub(crate) fn retry_pending_t3(
    context: &CallbackContext,
    thread: ThreadId,
    line: &str,
    log_path: &Path,
    pending: &PendingT3Send<impl IntentIo>,
    gate: SendGate<'_>,
) -> Result<Option<PendingRetry>, SendFailure> {
    let Some(provider_thread) = pending.get(thread)? else {
        return Ok(None);
    };
    let retry = match wake_provider_thread(context, provider_thread, line, log_path, pending, gate)?
    {
        WakeOutcome::Woken { .. } => PendingRetry::Delivered,
        WakeOutcome::Uncertain(detail) => PendingRetry::Uncertain(detail),
        WakeOutcome::NotT3Thread => {
            PendingRetry::Blocked("T3 may already have this event; no T3 thread owns it now".into())
        }
        WakeOutcome::Unavailable(detail)
        | WakeOutcome::Refused(detail)
        | WakeOutcome::ApiChanged(detail) => PendingRetry::Blocked(format!(
            "T3 may already have this event, so it waits for T3: {detail}"
        )),
    };
    Ok(Some(retry))
}

/// Make exactly one bounded direct-message delivery attempt
///
/// A live Claude session takes the line through its socket inbox, or through
/// T3 when a T3 V2 thread owns it. A stopped Claude session gets a new turn from
/// the T3 thread that owns it, because a host such as T3 Code before V2 runs no
/// Claude process between turns. A Codex thread takes it as a turn in the T3
/// thread that owns it, else through `codex queue`, so only that path needs the
/// Codex executable. After a T3 send whose reply was lost, only T3 is tried
pub(crate) fn send_saved_queue_attempt(
    context: &CallbackContext,
    thread: ThreadId,
    line: &str,
    log_path: &Path,
    delivery_lock: &Path,
    pending: &PendingT3Send,
) -> Result<(), String> {
    let gate = SendGate {
        path: delivery_lock,
        check: None,
    };
    send_direct_message(context, thread, line, log_path, pending, gate)
        .map_err(|error| error.to_string())
}

fn send_direct_message(
    context: &CallbackContext,
    thread: ThreadId,
    line: &str,
    log_path: &Path,
    pending: &PendingT3Send,
    gate: SendGate<'_>,
) -> Result<(), SendFailure> {
    match retry_pending_t3(context, thread, line, log_path, pending, gate)? {
        Some(PendingRetry::Delivered) => return Ok(()),
        Some(PendingRetry::Uncertain(detail) | PendingRetry::Blocked(detail)) => {
            return Err(format!(
                "T3 may already have this message; retry with the same message id: {detail}"
            )
            .into());
        }
        None => {}
    }

    match find_saved_inbox_origin(context, thread)? {
        OriginSession::Reachable(origin) => origin.send(thread, line, log_path, gate),
        OriginSession::T3Claude(origin) => {
            send_t3_claude(context, thread, origin, line, log_path, gate, pending)
        }
        OriginSession::Stopped => {
            wake_stopped_session(context, thread, line, log_path, pending, gate)
        }
        // T3 V2 runs a `codex queue` message without showing it in the thread,
        // so a thread that T3 owns takes the message as a T3 turn
        OriginSession::T3Codex => {
            match wake_codex_thread(context, thread, line, log_path, pending, gate)? {
                Ok(()) => Ok(()),
                Err(CodexWakeError::Uncertain(detail)) => Err(format!(
                    "T3 may have taken the message for Codex thread {thread}; retry with the same message id: {detail}"
                )
                .into()),
                Err(CodexWakeError::NotStarted(_)) => {
                    check_saved_callback(context)?;
                    send_codex_queue_attempt(context, thread, line, log_path, gate)
                }
            }
        }
        OriginSession::Codex => {
            check_saved_callback(context)?;
            send_codex_queue_attempt(context, thread, line, log_path, gate)
        }
    }
}

/// Make one bounded `codex queue` attempt with the saved origin context
pub(crate) fn send_codex_queue_attempt(
    context: &CallbackContext,
    thread: ThreadId,
    line: &str,
    log_path: &Path,
    gate: SendGate<'_>,
) -> Result<(), SendFailure> {
    let binary = context.codex.path().ok_or_else(|| {
        context
            .codex
            .unavailable_reason()
            .unwrap_or("saved Codex executable is unavailable")
            .to_owned()
    })?;
    queue_command_once(
        binary,
        thread,
        &context.env,
        &context.cwd,
        line,
        log_path,
        gate,
    )
}

/// Start a turn in the T3 thread that owns a stopped Claude session
///
/// An error explains why T3 could not take the event
pub(crate) fn wake_stopped_session(
    context: &CallbackContext,
    thread: ThreadId,
    line: &str,
    log_path: &Path,
    pending: &PendingT3Send,
    gate: SendGate<'_>,
) -> Result<(), SendFailure> {
    let provider_thread = ProviderThread::Claude(thread);
    let message =
        match wake_provider_thread(context, provider_thread, line, log_path, pending, gate)? {
            WakeOutcome::Woken { .. } => return Ok(()),
            WakeOutcome::NotT3Thread => {
                format!("Claude session {thread} is not running; no T3 thread owns it")
            }
            WakeOutcome::Unavailable(detail) => {
                format!("T3 unavailable for Claude session {thread}: {detail}")
            }
            WakeOutcome::Refused(detail) => format!("T3 refused Claude session {thread}: {detail}"),
            WakeOutcome::Uncertain(detail) => {
                format!("T3 may have started a turn for Claude session {thread}: {detail}")
            }
            WakeOutcome::ApiChanged(detail) => {
                format!("T3 API changed for Claude session {thread}: {detail}")
            }
        };
    Err(SendFailure::Failed(message))
}

/// Send to a live Claude session through the T3 thread that owns it
///
/// Falls back to the session socket whenever T3 did not take the event, so the
/// agent always gets it even if T3 shows it poorly. A send whose reply was lost
/// returns an error instead, and later attempts go only through T3
pub(crate) fn send_t3_claude(
    context: &CallbackContext,
    thread: ThreadId,
    origin: ReachableOrigin,
    line: &str,
    log_path: &Path,
    gate: SendGate<'_>,
    pending: &PendingT3Send,
) -> Result<(), SendFailure> {
    let provider_thread = ProviderThread::Claude(thread);
    let reason = match wake_provider_thread(
        context,
        provider_thread,
        line,
        log_path,
        pending,
        gate,
    )? {
        WakeOutcome::Woken { .. } => return Ok(()),
        WakeOutcome::Uncertain(detail) => {
            return Err(format!(
                "T3 may have taken the event for Claude session {thread}; retrying through T3: {detail}"
            )
            .into());
        }
        WakeOutcome::NotT3Thread => "no T3 thread owns the session".to_string(),
        WakeOutcome::Unavailable(detail)
        | WakeOutcome::Refused(detail)
        | WakeOutcome::ApiChanged(detail) => detail,
    };

    if let Ok(mut log) = OpenOptions::new().create(true).append(true).open(log_path) {
        let _ = writeln!(log, "t3 send failed, using the session socket: {reason}");
    }
    origin.send(thread, line, log_path, gate)
}

/// Why a T3 Codex wake did not deliver
pub(crate) enum CodexWakeError {
    /// T3 may have started the turn, so only a retry through T3 is safe
    Uncertain(String),
    /// T3 did not start the turn, so `codex queue` may deliver instead
    NotStarted(String),
}

/// Start a T3 turn for a mapped Codex thread
///
/// The outer error stops the attempt; the inner one says whether `codex queue`
/// may deliver instead
pub(crate) fn wake_codex_thread(
    context: &CallbackContext,
    thread: ThreadId,
    line: &str,
    log_path: &Path,
    pending: &PendingT3Send,
    gate: SendGate<'_>,
) -> Result<Result<(), CodexWakeError>, SendFailure> {
    let provider_thread = ProviderThread::Codex(thread);
    let woken = match wake_provider_thread(context, provider_thread, line, log_path, pending, gate)?
    {
        WakeOutcome::Woken { .. } => Ok(()),
        WakeOutcome::NotT3Thread => Err(CodexWakeError::NotStarted(
            "no T3 thread owns this Codex thread".into(),
        )),
        WakeOutcome::Uncertain(detail) => Err(CodexWakeError::Uncertain(detail)),
        WakeOutcome::Unavailable(detail)
        | WakeOutcome::Refused(detail)
        | WakeOutcome::ApiChanged(detail) => Err(CodexWakeError::NotStarted(detail)),
    };
    Ok(woken)
}

/// Ask T3 to start the turn, keeping `pending` in step with the outcome
fn wake_provider_thread(
    context: &CallbackContext,
    provider_thread: ProviderThread,
    line: &str,
    log_path: &Path,
    pending: &PendingT3Send<impl IntentIo>,
    gate: SendGate<'_>,
) -> Result<WakeOutcome, SendFailure> {
    let _lock = gate.lock()?;
    let thread = match provider_thread {
        ProviderThread::Claude(thread) | ProviderThread::Codex(thread) => thread,
    };
    let was_pending = pending.get(thread)?.is_some();
    // persist before the request: a lost reply must never enable another route
    if was_pending {
        // a prior save may have reached rename but failed its durability step
        pending.sync()?;
    } else {
        pending.record(provider_thread)?;
    }

    let start = turn_start(context, provider_thread, log_path, gate);
    let outcome =
        match wake_thread_checked(&t3_env(context), provider_thread, line, gate.check, start) {
            Ok(outcome) => outcome,
            // with no notice to recheck, a failed dispatch only leaves T3 unavailable
            Err(failure) if gate.check.is_none() => WakeOutcome::Unavailable(failure.to_string()),
            Err(failure) => {
                if !was_pending {
                    pending.clear();
                }
                return Err(failure);
            }
        };
    if !was_pending
        && !matches!(
            outcome,
            WakeOutcome::Woken { .. } | WakeOutcome::Uncertain(_)
        )
    {
        pending.clear();
    }
    record_wake(&outcome, log_path, pending);
    Ok(outcome)
}

/// How the T3 thread takes the line, from the Claude session's transcript
///
/// A large context whose cache lapsed is compacted first. Its turn would write
/// the whole context to the one-hour cache, while Claude Code's compaction
/// request writes it at the cheaper five-minute rate, and every later request
/// reads the summary instead of the whole context. The line then waits behind
/// the compaction. A retry reuses the idle period's
/// key, which T3 drops, or finds the compaction in the transcript
fn turn_start(
    context: &CallbackContext,
    provider_thread: ProviderThread,
    log_path: &Path,
    gate: SendGate<'_>,
) -> TurnStart {
    let ProviderThread::Claude(thread) = provider_thread else {
        return TurnStart::Auto;
    };
    let Some(usage) = ContextUse::read(Path::new(&context.env.home), thread) else {
        return TurnStart::Auto;
    };
    let now = Utc::now();
    // a line that may no longer be sent must not start a compaction either
    if usage.cold_compaction_due(now) && gate.check().is_ok() {
        let outcome = compact_thread(&t3_env(context), thread, &usage.compaction_key());
        compaction_log::record_default(Trigger::Callback, thread, &usage, &outcome, now);
        if let Ok(mut log) = OpenOptions::new().create(true).append(true).open(log_path) {
            let _ = writeln!(
                log,
                "t3 compact first: {} context tokens past the prompt cache: {outcome:?}",
                usage.tokens()
            );
        }
    }
    usage.turn_start(now)
}

fn record_wake(outcome: &WakeOutcome, log_path: &Path, pending: &PendingT3Send<impl IntentIo>) {
    if let WakeOutcome::Woken { t3_thread } = outcome {
        pending.clear();
        if let Ok(mut log) = OpenOptions::new().create(true).append(true).open(log_path) {
            let _ = writeln!(log, "t3 wake thread={t3_thread}");
        }
    }
}

fn queue_command_once(
    binary: &Path,
    thread: ThreadId,
    env: &TaskEnv,
    cwd: &Path,
    line: &str,
    log_path: &Path,
    gate: SendGate<'_>,
) -> Result<(), SendFailure> {
    let mut cmd = Command::new(binary);
    cmd.args(["queue", "--thread", &thread.to_string(), "--message", line])
        .env("PATH", &env.path)
        .env("HOME", &env.home)
        .current_dir(cwd);
    match run_command_deadline(&mut cmd, QUEUE_ATTEMPT_TIMEOUT, gate) {
        Ok(Some(out)) => {
            let _ = std::fs::write(log_path, transcript(&out));
            if out.status.success() {
                Ok(())
            } else {
                Err(SendFailure::Failed(format!(
                    "codex queue exit={} stderr={}",
                    out.status.code().unwrap_or(-1),
                    String::from_utf8_lossy(&out.stderr)
                )))
            }
        }
        Ok(None) => Err(SendFailure::Suppressed),
        Err(error) => Err(SendFailure::Failed(error)),
    }
}

/// Drive one child to completion or deadline. Drains stdout/stderr on helper
/// threads so a chatty child cannot deadlock a filled pipe, and kills the
/// process group when the deadline elapses
pub(super) fn run_command_deadline(
    cmd: &mut Command,
    timeout: Duration,
    gate: SendGate<'_>,
) -> Result<Option<std::process::Output>, String> {
    let held = gate.lock().map_err(|error| error.to_string())?;
    let lock = match held {
        Some(lock) => lock,
        None => OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(gate.path)
            .map_err(|err| format!("open delivery lock {}: {err}", gate.path.display()))?,
    };
    let lock_fd = lock.as_raw_fd();
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    unsafe {
        cmd.pre_exec(move || lock_delivery_in_child(lock_fd));
    }
    match gate.check() {
        Ok(()) => {}
        Err(SendFailure::Suppressed) => return Ok(None),
        Err(SendFailure::Failed(message)) => return Err(message),
    }
    clear_close_on_exec(lock_fd).map_err(|err| format!("prepare delivery lock: {err}"))?;
    let mut child = cmd.spawn().map_err(|err| err.to_string())?;
    // the queue process and any descendants now own the locked open-file
    // description; the daemon must not keep its inherited copy alive
    drop(lock);
    let pid = child.id() as i32;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "missing stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "missing stderr".to_string())?;
    let (tx_out, rx_out) = mpsc::channel();
    let (tx_err, rx_err) = mpsc::channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        let mut stdout = stdout;
        let _ = stdout.read_to_end(&mut buf);
        let _ = tx_out.send(buf);
    });
    thread::spawn(move || {
        let mut buf = Vec::new();
        let mut stderr = stderr;
        let _ = stderr.read_to_end(&mut buf);
        let _ = tx_err.send(buf);
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() >= deadline => {
                return Err(terminate_timed_out_child(
                    &mut child, pid, timeout, &rx_out, &rx_err,
                ));
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(err) => return Err(err.to_string()),
        }
    };

    let drain_timeout = Duration::from_secs(QUEUE_PIPE_DRAIN_TIMEOUT_SECS);
    let stdout = rx_out.recv_timeout(drain_timeout).unwrap_or_default();
    let stderr = rx_err.recv_timeout(drain_timeout).unwrap_or_default();
    Ok(Some(std::process::Output {
        status,
        stdout,
        stderr,
    }))
}

fn terminate_timed_out_child(
    child: &mut Child,
    pid: i32,
    timeout: Duration,
    stdout: &mpsc::Receiver<Vec<u8>>,
    stderr: &mpsc::Receiver<Vec<u8>>,
) -> String {
    // kill the whole group, then reap; never leave an unbounded wait
    let _ = kill(Pid::from_raw(-pid), Signal::SIGKILL);
    let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
    let reap_deadline = Instant::now() + Duration::from_secs(QUEUE_REAP_TIMEOUT_SECS);

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let _ = stdout.recv_timeout(Duration::from_millis(100));
                let _ = stderr.recv_timeout(Duration::from_millis(100));
                let timeout_secs = timeout.as_secs();
                let exit = status
                    .code()
                    .unwrap_or_else(|| status.signal().unwrap_or(-1));

                return format!("codex queue timed out after {timeout_secs}s (exit={exit})");
            }
            Ok(None) if Instant::now() >= reap_deadline => {
                let timeout_secs = timeout.as_secs();
                return format!(
                    "codex queue timed out after {timeout_secs}s and child did not reap"
                );
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(err) => return err.to_string(),
        }
    }
}

fn clear_close_on_exec(fd: i32) -> io::Result<()> {
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let flags =
        nix::fcntl::fcntl(borrowed, nix::fcntl::FcntlArg::F_GETFD).map_err(io::Error::from)?;
    let mut flags = nix::fcntl::FdFlag::from_bits_truncate(flags);
    flags.remove(nix::fcntl::FdFlag::FD_CLOEXEC);
    nix::fcntl::fcntl(borrowed, nix::fcntl::FcntlArg::F_SETFD(flags)).map_err(io::Error::from)?;
    Ok(())
}

/// Take the flock in the queue child, not the daemon. Descendants inherit the
/// same open-file description, so no part of an old delivery can overlap a
/// replacement sender. The alarm starts before the blocking lock call and
/// bounds both lock contention and the queue process itself.
fn lock_delivery_in_child(fd: i32) -> io::Result<()> {
    unsafe {
        nix::libc::alarm(QUEUE_ATTEMPT_TIMEOUT_SECS as u32);
    }
    if unsafe { nix::libc::flock(fd, nix::libc::LOCK_EX) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn transcript(out: &std::process::Output) -> Vec<u8> {
    let mut body = out.stdout.clone();
    body.extend_from_slice(&out.stderr);
    body
}

/// Append the event line and last stderr to the fallback log.
pub(crate) fn append_fallback(path: &Path, line: &str, stderr: &str) -> Result<(), AppError> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{line}")?;
    writeln!(file, "{stderr}")?;
    Ok(())
}

#[cfg(test)]
mod tests;
