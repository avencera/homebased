//! `HOMEBASED_EVENT` formatting and origin inbox delivery: `codex queue` for a
//! Codex thread, or the session socket for a Claude Code session.

mod claude_inbox;

use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, BorrowedFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};

use crate::container::GpuRequest;
use crate::domain::{
    AgentKind, ExitReason, ReportOutcome, TaskId, TaskName, TaskReport, TaskRow, TaskState,
    ThreadId, Workload,
};
use crate::error::AppError;
use crate::submission::CallbackContext;

use crate::t3::{T3Env, WakeOutcome, wake_claude_session};
use claude_inbox::{ClaudeInbox, ClaudeSession};

/// Per-attempt bound for one `codex queue` child, including cleanup.
pub const QUEUE_ATTEMPT_TIMEOUT_SECS: u64 = 20;
/// [`QUEUE_ATTEMPT_TIMEOUT_SECS`] as a [`Duration`].
pub const QUEUE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(QUEUE_ATTEMPT_TIMEOUT_SECS);

const QUEUE_ATTEMPTS: u32 = 3;

/// Extra time to reap a child after its process group receives SIGKILL.
const QUEUE_REAP_TIMEOUT_SECS: u64 = 2;

/// Maximum drain wait for each of stdout and stderr after direct-child exit.
const QUEUE_PIPE_DRAIN_TIMEOUT_SECS: u64 = 2;

/// Worst-case backoff across retries: attempt 2 waits 200ms, attempt 3 waits 400ms.
const QUEUE_RETRY_BACKOFF_TOTAL_MS: u64 = 200 + 400;

/// Slack after the last attempt returns for draining pipes, writing the
/// callback log, and store settlement before the attention claim can release.
const QUEUE_SETTLEMENT_SLACK_SECS: u64 = 5;

/// A sender inherited from a dead daemon self-terminates within one attempt.
const QUEUE_STALE_OWNER_MAX_SECS: u64 = QUEUE_ATTEMPT_TIMEOUT_SECS;

/// Worst-case wall time for three bounded attempts, their backoff, and
/// post-attempt settlement. The attention release valve must stay above this
/// so a live send cannot outlive the claim.
const QUEUE_ATTEMPT_WORST_CASE_SECS: u64 =
    QUEUE_ATTEMPT_TIMEOUT_SECS + QUEUE_REAP_TIMEOUT_SECS + QUEUE_PIPE_DRAIN_TIMEOUT_SECS * 2;
const QUEUE_SEND_WORST_CASE_SECS: u64 = QUEUE_ATTEMPT_WORST_CASE_SECS * QUEUE_ATTEMPTS as u64
    + QUEUE_RETRY_BACKOFF_TOTAL_MS.div_ceil(1000)
    + QUEUE_STALE_OWNER_MAX_SECS
    + QUEUE_SETTLEMENT_SLACK_SECS;

/// How long recovery waits for a legacy attention sender before releasing its
/// persisted claim. Must exceed the worst-case live `codex queue` send.
pub const ATTENTION_SETTLE_SECS: u64 = 120;
/// [`ATTENTION_SETTLE_SECS`] as a [`Duration`].
pub const ATTENTION_SETTLE: Duration = Duration::from_secs(ATTENTION_SETTLE_SECS);

const _: () = assert!(
    ATTENTION_SETTLE_SECS > QUEUE_SEND_WORST_CASE_SECS,
    "attention settlement must outlast the worst-case queue send"
);

/// Public workload view. Omits private prompt and extra-arg fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkloadView {
    /// Agent CLI identity.
    Agent {
        /// Agent kind.
        agent: AgentKind,
        /// Model alias, or null.
        model: Option<String>,
        /// Reasoning effort from the agent argv, when the caller set one.
        ///
        /// The public view omits `extra_args`. This keeps the level those args
        /// named (`model_reasoning_effort`, `--effort`, or `--reasoning-effort`).
        #[serde(skip_serializing_if = "Option::is_none")]
        reasoning: Option<String>,
    },
    /// Task argv preview.
    Task {
        /// Full argv including the program.
        command: Vec<String>,
    },
    /// Container identity. Omits environment values, which may hold secrets.
    Container {
        /// Image pinned by digest.
        image: String,
        /// Argv head that replaces the image entrypoint, when set.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entrypoint: Option<Vec<String>>,
        /// Arguments after the image.
        args: Vec<String>,
        /// GPUs the container may use, when set.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gpus: Option<GpuRequest>,
    },
}

impl From<&Workload> for WorkloadView {
    fn from(workload: &Workload) -> Self {
        match workload {
            Workload::Agent(agent) => Self::Agent {
                agent: agent.agent.kind,
                model: agent.agent.model.clone(),
                reasoning: reasoning_from_extra_args(&agent.extra_args),
            },
            Workload::Task(task) => Self::Task {
                command: task.command.to_vec(),
            },
            Workload::Container(container) => Self::Container {
                image: container.image.as_str().to_owned(),
                entrypoint: container
                    .entrypoint
                    .as_ref()
                    .map(|entrypoint| entrypoint.as_slice().to_vec()),
                args: container.args.clone(),
                gpus: container.gpus.clone(),
            },
        }
    }
}

/// Last reasoning level named in agent argv.
///
/// Codex passes `model_reasoning_effort` through `-c` or `--config`. Claude
/// passes `--effort`. Grok passes `--effort` or `--reasoning-effort`. A later
/// flag replaces an earlier one. Other config keys stay private.
fn reasoning_from_extra_args(extra_args: &[String]) -> Option<String> {
    let mut found = None;
    let mut index = 0;
    while index < extra_args.len() {
        let arg = &extra_args[index];
        if let Some(level) = inline_config_reasoning(arg) {
            found = Some(level);
        } else if (arg == "--config" || arg == "-c")
            && let Some(next) = extra_args.get(index + 1)
        {
            if let Some(level) = config_reasoning_value(next) {
                found = Some(level);
            }
            index += 1;
        } else if let Some(level) = inline_effort_flag(arg) {
            found = Some(level);
        } else if (arg == "--effort" || arg == "--reasoning-effort")
            && let Some(next) = extra_args.get(index + 1)
        {
            if let Some(level) = reasoning_token(next) {
                found = Some(level);
            }
            index += 1;
        }
        index += 1;
    }
    found
}

fn inline_config_reasoning(arg: &str) -> Option<String> {
    let value = arg
        .strip_prefix("--config=")
        .or_else(|| arg.strip_prefix("-c="))?;
    config_reasoning_value(value)
}

fn config_reasoning_value(value: &str) -> Option<String> {
    let (key, raw) = value.split_once('=')?;
    if key.trim() != "model_reasoning_effort" {
        return None;
    }
    reasoning_token(raw)
}

fn inline_effort_flag(arg: &str) -> Option<String> {
    let raw = arg
        .strip_prefix("--reasoning-effort=")
        .or_else(|| arg.strip_prefix("--effort="))?;
    reasoning_token(raw)
}

/// One reasoning level token, with one or more wrapping quote layers removed.
fn reasoning_token(raw: &str) -> Option<String> {
    let mut value = raw.trim();
    loop {
        let bytes = value.as_bytes();
        if bytes.len() < 2 {
            break;
        }
        let (first, last) = (bytes[0], bytes[bytes.len() - 1]);
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            value = &value[1..value.len() - 1];
            continue;
        }
        break;
    }
    let value = value.trim();
    let mut chars = value.chars();
    if value.len() > 32 || !chars.next().is_some_and(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return None;
    }
    Some(value.to_string())
}

/// Derived event name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventKind {
    /// Interim `--notify` report.
    TaskReported,
    /// Output was inactive for the configured time; child still running.
    TaskCheckDue,
    /// Process cancelled.
    TaskCancelled,
    /// Worker gone without `exit.json`.
    TaskLost,
    /// Last report is blocked.
    TaskBlocked,
    /// Failure or non-zero exit.
    TaskFailed,
    /// Success.
    TaskSucceeded,
}

/// Suggested orchestrator next step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NextAction {
    /// Read the interim report.
    ReadReport,
    /// Inspect status and recent logs after an inactivity reminder.
    InspectTask,
    /// Nothing further.
    None,
    /// Inspect the output log.
    InspectLog,
    /// Answer a blocked report and resubmit.
    AnswerAndResubmit,
    /// Review successful output.
    ReviewOutput,
}

/// Tagged process payload in the event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProcessPayload {
    /// Process exited.
    Exit {
        /// Exit code the process returned.
        code: i32,
    },
    /// Signalled.
    Signal {
        /// Signal number that ended the process.
        signal: i32,
    },
    /// Cancelled.
    Cancelled,
    /// Spawn failed.
    SpawnFailed {
        /// Why the spawn failed.
        message: String,
    },
    /// Runner lost.
    RunnerLost,
}

impl From<&ExitReason> for ProcessPayload {
    fn from(reason: &ExitReason) -> Self {
        match reason {
            ExitReason::Exit { code } => Self::Exit { code: *code },
            ExitReason::Signal { signal } => Self::Signal { signal: *signal },
            ExitReason::Cancelled => Self::Cancelled,
            ExitReason::SpawnFailed { message } => Self::SpawnFailed {
                message: message.clone(),
            },
        }
    }
}

/// One report in the event payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportView {
    /// Sequence number.
    pub seq: i64,
    /// Outcome.
    pub outcome: ReportOutcome,
    /// Summary.
    pub summary: String,
}

impl From<&TaskReport> for ReportView {
    fn from(report: &TaskReport) -> Self {
        Self {
            seq: report.seq,
            outcome: report.outcome,
            summary: report.summary.clone(),
        }
    }
}

/// Event object shared by `codex queue` and `task show --json` `last_event`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HomebasedEvent {
    /// Schema version. First key.
    pub api_version: u32,
    /// Derived event name.
    pub event: EventKind,
    /// Task id.
    pub task: TaskId,
    /// Submitted name. Omitted only for rows stored before name was required.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<TaskName>,
    /// Non-empty server-derived label.
    pub display_name: String,
    /// Workload view.
    pub workload: WorkloadView,
    /// Codex thread.
    pub thread: ThreadId,
    /// Task cwd.
    pub cwd: PathBuf,
    /// Absolute task directory.
    pub evidence: PathBuf,
    /// Reports in seq order.
    pub reports: Vec<ReportView>,
    /// Process payload, or null for interim events.
    pub process: Option<ProcessPayload>,
    /// Configured output-inactivity timeout in seconds. Present on check-due events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// Suggested next action.
    pub next_action: NextAction,
}

impl HomebasedEvent {
    /// Prefix plus JSON object, one line.
    pub fn to_message_line(&self) -> Result<String, AppError> {
        let json = serde_json::to_string(self)?;
        Ok(format!("HOMEBASED_EVENT {json}"))
    }
}

/// Build an exit event from the row's stored reason.
#[must_use]
pub fn exit_event(row: &TaskRow, reports: &[TaskReport], evidence: PathBuf) -> HomebasedEvent {
    build_event(
        row,
        reports,
        evidence,
        row.exit_reason().map(ProcessPayload::from),
        None,
    )
}

/// Build a lost-runner event.
#[must_use]
pub fn lost_event(row: &TaskRow, reports: &[TaskReport], evidence: PathBuf) -> HomebasedEvent {
    build_event(
        row,
        reports,
        evidence,
        Some(ProcessPayload::RunnerLost),
        None,
    )
}

/// Output-inactivity reminder. Never changes task status.
#[must_use]
pub fn check_due_event(row: &TaskRow, reports: &[TaskReport], evidence: PathBuf) -> HomebasedEvent {
    HomebasedEvent {
        api_version: crate::domain::API_VERSION,
        event: EventKind::TaskCheckDue,
        task: row.id,
        name: row.name.clone(),
        display_name: row.display_name(),
        workload: WorkloadView::from(&row.workload),
        thread: row.thread,
        cwd: row.cwd.clone(),
        evidence,
        reports: reports.iter().map(ReportView::from).collect(),
        process: None,
        timeout_secs: Some(row.timeout.as_secs()),
        next_action: NextAction::InspectTask,
    }
}

fn build_event(
    row: &TaskRow,
    reports: &[TaskReport],
    evidence: PathBuf,
    process: Option<ProcessPayload>,
    timeout_secs: Option<u64>,
) -> HomebasedEvent {
    let (event, next_action) = derive_exit(process.as_ref(), reports);
    HomebasedEvent {
        api_version: crate::domain::API_VERSION,
        event,
        task: row.id,
        name: row.name.clone(),
        display_name: row.display_name(),
        workload: WorkloadView::from(&row.workload),
        thread: row.thread,
        cwd: row.cwd.clone(),
        evidence,
        reports: reports.iter().map(ReportView::from).collect(),
        process,
        timeout_secs,
        next_action,
    }
}

/// Interim `--notify` event carrying one report and `process: null`.
#[must_use]
pub fn notify_event(row: &TaskRow, report: &TaskReport, evidence: PathBuf) -> HomebasedEvent {
    HomebasedEvent {
        api_version: crate::domain::API_VERSION,
        event: EventKind::TaskReported,
        task: row.id,
        name: row.name.clone(),
        display_name: row.display_name(),
        workload: WorkloadView::from(&row.workload),
        thread: row.thread,
        cwd: row.cwd.clone(),
        evidence,
        reports: vec![ReportView::from(report)],
        process: None,
        timeout_secs: None,
        next_action: NextAction::ReadReport,
    }
}

fn derive_exit(
    process: Option<&ProcessPayload>,
    reports: &[TaskReport],
) -> (EventKind, NextAction) {
    if matches!(process, Some(ProcessPayload::Cancelled)) {
        return (EventKind::TaskCancelled, NextAction::None);
    }
    if matches!(process, Some(ProcessPayload::RunnerLost)) {
        return (EventKind::TaskLost, NextAction::InspectLog);
    }
    let last = reports.last().map(|r| r.outcome);
    if last == Some(ReportOutcome::Blocked) {
        return (EventKind::TaskBlocked, NextAction::AnswerAndResubmit);
    }
    let exit_zero = matches!(process, Some(ProcessPayload::Exit { code: 0 }));
    if last == Some(ReportOutcome::Failed) || !exit_zero {
        return (EventKind::TaskFailed, NextAction::InspectLog);
    }
    (EventKind::TaskSucceeded, NextAction::ReviewOutput)
}

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

/// Saved origin found before the callback actor decides on a T3 wake
pub(crate) enum OriginSession {
    /// A live Claude session or a Codex thread can take the event now
    Reachable(ReachableOrigin),
    /// A Claude session with a transcript but no live process
    Stopped,
}

/// Saved origin that accepts an event without a wake
pub(crate) struct ReachableOrigin(Reachable);

enum Reachable {
    Claude(ClaudeInbox),
    Codex,
}

impl ReachableOrigin {
    /// Make one bounded send through the Claude socket or `codex queue`
    pub(crate) fn send(
        self,
        context: &CallbackContext,
        thread: ThreadId,
        line: &str,
        log_path: &Path,
        delivery_lock: &Path,
    ) -> Result<(), String> {
        if let Reachable::Claude(inbox) = self.0 {
            return inbox.send(thread, line, log_path, delivery_lock);
        }
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
            delivery_lock,
        )
    }
}

/// Find which saved origin owns `thread`
///
/// A known Claude session with no live process is [`OriginSession::Stopped`];
/// an id that Claude Code does not know belongs to Codex
pub(crate) fn find_saved_origin(
    context: &CallbackContext,
    thread: ThreadId,
) -> Result<OriginSession, String> {
    let reachable = match ClaudeInbox::find(Path::new(&context.env.home), thread)? {
        ClaudeSession::Live(inbox) => Reachable::Claude(inbox),
        ClaudeSession::Stopped => return Ok(OriginSession::Stopped),
        ClaudeSession::Unknown => Reachable::Codex,
    };
    Ok(OriginSession::Reachable(ReachableOrigin(reachable)))
}

/// Make exactly one bounded delivery attempt using the saved origin context
///
/// A stopped Claude session is an error; only the origin inbox dispatcher
/// may wake it through T3
pub(crate) fn send_saved_queue_attempt(
    context: &CallbackContext,
    thread: ThreadId,
    line: &str,
    log_path: &Path,
    delivery_lock: &Path,
) -> Result<(), String> {
    match find_saved_origin(context, thread)? {
        OriginSession::Reachable(origin) => {
            origin.send(context, thread, line, log_path, delivery_lock)
        }
        OriginSession::Stopped => Err(format!("Claude session {thread} is not running")),
    }
}

/// Start a turn in the T3 thread that owns a stopped Claude session
///
/// An error explains why T3 could not take the event
pub(crate) fn wake_stopped_session(
    context: &CallbackContext,
    thread: ThreadId,
    line: &str,
    log_path: &Path,
) -> Result<(), String> {
    let env = T3Env::new(
        PathBuf::from(&context.env.home),
        context.env.path.clone().into(),
    );
    match wake_claude_session(&env, thread, line) {
        WakeOutcome::Woken { t3_thread } => {
            if let Ok(mut log) = OpenOptions::new().create(true).append(true).open(log_path) {
                let _ = writeln!(log, "t3 wake thread={t3_thread}");
            }
            Ok(())
        }
        WakeOutcome::NotT3Thread => Err(format!(
            "Claude session {thread} is not running; no T3 thread owns it"
        )),
        WakeOutcome::Unavailable(detail) => Err(format!(
            "T3 unavailable for Claude session {thread}: {detail}"
        )),
        WakeOutcome::Refused(detail) => {
            Err(format!("T3 refused Claude session {thread}: {detail}"))
        }
        WakeOutcome::ApiChanged(detail) => Err(format!(
            "T3 API changed for Claude session {thread}: {detail}"
        )),
    }
}

fn queue_command_once(
    binary: &Path,
    thread: ThreadId,
    env: &crate::domain::TaskEnv,
    cwd: &Path,
    line: &str,
    log_path: &Path,
    delivery_lock: &Path,
) -> Result<(), String> {
    let mut cmd = Command::new(binary);
    cmd.args(["queue", "--thread", &thread.to_string(), "--message", line])
        .env("PATH", &env.path)
        .env("HOME", &env.home)
        .current_dir(cwd);
    match run_command_deadline(&mut cmd, QUEUE_ATTEMPT_TIMEOUT, delivery_lock) {
        Ok(out) => {
            let _ = std::fs::write(log_path, transcript(&out));
            if out.status.success() {
                Ok(())
            } else {
                Err(format!(
                    "codex queue exit={} stderr={}",
                    out.status.code().unwrap_or(-1),
                    String::from_utf8_lossy(&out.stderr)
                ))
            }
        }
        Err(error) => Err(error),
    }
}

/// Drive one child to completion or deadline. Drains stdout/stderr on helper
/// threads so a chatty child cannot deadlock a filled pipe, and kills the
/// process group when the deadline elapses.
fn run_command_deadline(
    cmd: &mut Command,
    timeout: Duration,
    delivery_lock: &Path,
) -> Result<std::process::Output, String> {
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(delivery_lock)
        .map_err(|err| format!("open delivery lock {}: {err}", delivery_lock.display()))?;
    let lock_fd = lock.as_raw_fd();
    clear_close_on_exec(lock_fd).map_err(|err| format!("prepare delivery lock: {err}"))?;
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    unsafe {
        cmd.pre_exec(move || lock_delivery_in_child(lock_fd));
    }
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
                // kill the whole group, then reap; never leave an unbounded wait
                let _ = kill(Pid::from_raw(-pid), Signal::SIGKILL);
                let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
                let reap_deadline = Instant::now() + Duration::from_secs(QUEUE_REAP_TIMEOUT_SECS);
                loop {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            let _ = rx_out.recv_timeout(Duration::from_millis(100));
                            let _ = rx_err.recv_timeout(Duration::from_millis(100));
                            return Err(format!(
                                "codex queue timed out after {}s (exit={})",
                                timeout.as_secs(),
                                status
                                    .code()
                                    .unwrap_or_else(|| status.signal().unwrap_or(-1))
                            ));
                        }
                        Ok(None) if Instant::now() >= reap_deadline => {
                            return Err(format!(
                                "codex queue timed out after {}s and child did not reap",
                                timeout.as_secs()
                            ));
                        }
                        Ok(None) => thread::sleep(Duration::from_millis(20)),
                        Err(err) => return Err(err.to_string()),
                    }
                }
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(err) => return Err(err.to_string()),
        }
    };

    let drain_timeout = Duration::from_secs(QUEUE_PIPE_DRAIN_TIMEOUT_SECS);
    let stdout = rx_out.recv_timeout(drain_timeout).unwrap_or_default();
    let stderr = rx_err.recv_timeout(drain_timeout).unwrap_or_default();
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
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

/// The event a terminal row owes its thread: `TASK_LOST` for a lost runner,
/// otherwise the exit event. Every exit-callback path builds its event here so
/// the lost-versus-exit choice lives in one place.
#[must_use]
pub fn terminal_event(row: &TaskRow, reports: &[TaskReport], evidence: PathBuf) -> HomebasedEvent {
    match row.state {
        TaskState::Finished { .. } => exit_event(row, reports, evidence),
        TaskState::Queued | TaskState::Running { .. } | TaskState::Lost => {
            lost_event(row, reports, evidence)
        }
    }
}

/// Last event for `task show`, if the process is terminal or a notify happened.
#[must_use]
pub fn last_event_for_row(
    row: &TaskRow,
    reports: &[TaskReport],
    evidence: PathBuf,
) -> Option<HomebasedEvent> {
    match row.state {
        TaskState::Queued | TaskState::Running { .. } => {
            let report = reports
                .iter()
                .rev()
                .find(|report| report.notified_at.is_some());
            match (row.attention.delivered_at(), report) {
                (Some(attention_at), Some(report))
                    if report
                        .notified_at
                        .is_some_and(|reported_at| reported_at > attention_at) =>
                {
                    Some(notify_event(row, report, evidence))
                }
                (Some(_), _) => Some(check_due_event(row, reports, evidence)),
                (None, Some(report)) => Some(notify_event(row, report, evidence)),
                (None, None) => None,
            }
        }
        TaskState::Lost => Some(lost_event(row, reports, evidence)),
        TaskState::Finished { .. } => Some(exit_event(row, reports, evidence)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        Agent, AgentWorkload, AttentionState, CallbackStatus, ProcessGroupExitEvidence, TaskEnv,
        Workload,
    };
    use chrono::Utc;
    use std::time::Duration;

    fn row(state: TaskState) -> TaskRow {
        TaskRow {
            id: TaskId::new(),
            name: None,
            thread: "01a0ab97-a7aa-7463-a5b0-8d500e40e431".parse().unwrap(),
            workload: Workload::Agent(AgentWorkload {
                agent: Agent::new(AgentKind::Claude, Some("fable".into())),
                extra_args: vec![],
                report_trailer: true,
            }),
            cwd: PathBuf::from("/work"),
            timeout: Duration::from_secs(4 * 3600),
            env: TaskEnv {
                path: "/bin".into(),
                home: "/home/u".into(),
            },
            binary: PathBuf::from("/bin/claude"),
            state,
            process_group_exit_evidence: ProcessGroupExitEvidence::Unconfirmed,
            container_exit_evidence: crate::domain::ContainerExitEvidence::Unconfirmed,
            callback_status: CallbackStatus::Pending,
            attention: AttentionState::Pending,
            cancel_requested_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn direct_message_to_stopped_claude_session_does_not_wake() {
        let home = tempfile::tempdir().unwrap();
        let thread = row(TaskState::Queued).thread;
        let project = home.path().join(".claude/projects/-work");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join(format!("{thread}.jsonl")), "").unwrap();
        let context = CallbackContext {
            env: TaskEnv {
                path: String::new(),
                home: home.path().to_string_lossy().into_owned(),
            },
            cwd: home.path().to_path_buf(),
            codex: PathBuf::from("/bin/true").into(),
        };
        assert!(matches!(
            find_saved_origin(&context, thread),
            Ok(OriginSession::Stopped)
        ));
        let log = home.path().join("callback.log");
        let lock = home.path().join("delivery.lock");
        let error = send_saved_queue_attempt(&context, thread, "HOMEBASED_MESSAGE {}", &log, &lock)
            .unwrap_err();
        assert_eq!(error, format!("Claude session {thread} is not running"));
        assert!(!log.exists());
    }

    fn report(seq: i64, outcome: ReportOutcome) -> TaskReport {
        TaskReport {
            seq,
            outcome,
            summary: format!("s{seq}"),
            reported_at: Utc::now(),
            notified_at: None,
        }
    }

    #[test]
    fn workload_view_publishes_reasoning_level() {
        let workload = Workload::Agent(AgentWorkload {
            agent: Agent::new(AgentKind::Codex, Some("gpt-5.6-luna".into())),
            extra_args: vec!["--config".into(), "model_reasoning_effort=\"max\"".into()],
            report_trailer: true,
        });
        let json = serde_json::to_value(WorkloadView::from(&workload)).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "type": "agent",
                "agent": "codex",
                "model": "gpt-5.6-luna",
                "reasoning": "max"
            })
        );
    }

    #[test]
    fn workload_view_omits_unset_reasoning() {
        let json =
            serde_json::to_value(WorkloadView::from(&row(TaskState::Queued).workload)).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "agent", "agent": "claude", "model": "fable"})
        );
    }

    #[test]
    fn cancelled_wins() {
        let event = exit_event(
            &row(TaskState::Finished {
                reason: ExitReason::Cancelled,
            }),
            &[report(1, ReportOutcome::Succeeded)],
            PathBuf::from("/e"),
        );
        assert_eq!(event.event, EventKind::TaskCancelled);
        assert_eq!(event.next_action, NextAction::None);
    }

    #[test]
    fn lost_runner_event() {
        let event = lost_event(&row(TaskState::Lost), &[], PathBuf::from("/e"));
        assert_eq!(event.event, EventKind::TaskLost);
        assert!(matches!(event.process, Some(ProcessPayload::RunnerLost)));
    }

    #[test]
    fn blocked_then_succeeded() {
        let event = exit_event(
            &row(TaskState::Finished {
                reason: ExitReason::Exit { code: 0 },
            }),
            &[
                report(1, ReportOutcome::Blocked),
                report(2, ReportOutcome::Succeeded),
            ],
            PathBuf::from("/e"),
        );
        assert_eq!(event.event, EventKind::TaskSucceeded);
        assert_eq!(event.reports.len(), 2);
    }

    fn codex_luna(extra_args: &[String]) -> Workload {
        Workload::Agent(AgentWorkload {
            agent: Agent::new(AgentKind::Codex, Some("gpt-5.6-luna".into())),
            extra_args: extra_args.to_vec(),
            report_trailer: true,
        })
    }

    #[test]
    fn reasoning_comes_from_agent_argv() {
        let cases: &[(&[&str], Option<&str>)] = &[
            (&[], None),
            (&["--config", "model_reasoning_effort=\"max\""], Some("max")),
            (&["-c", "model_reasoning_effort=max"], Some("max")),
            (&["-c", "model_reasoning_effort=\"low\""], Some("low")),
            (&["--config=model_reasoning_effort='xhigh'"], Some("xhigh")),
            (&["--effort", "high"], Some("high")),
            (&["--reasoning-effort", "high"], Some("high")),
            (&["--effort=medium"], Some("medium")),
            (
                &[
                    "-c",
                    "model_reasoning_effort=\"low\"",
                    "--config",
                    "sandbox_mode=\"danger-full-access\"",
                    "-c",
                    "model_reasoning_effort=\"max\"",
                ],
                Some("max"),
            ),
            (&["--add-dir", "/tmp"], None),
            (&["-c", "model=\"gpt-5.6-luna\""], None),
            (&["--effort", "--add-dir"], None),
        ];
        for (args, expected) in cases {
            let owned: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
            let view = WorkloadView::from(&codex_luna(&owned));
            let WorkloadView::Agent { reasoning, .. } = view else {
                panic!("agent view");
            };
            assert_eq!(reasoning.as_deref(), *expected, "{args:?}");
        }
    }

    #[test]
    fn no_reports_exit_zero() {
        let event = exit_event(
            &row(TaskState::Finished {
                reason: ExitReason::Exit { code: 0 },
            }),
            &[],
            PathBuf::from("/e"),
        );
        assert_eq!(event.event, EventKind::TaskSucceeded);
        assert!(event.reports.is_empty());
        assert!(matches!(
            event.workload,
            WorkloadView::Agent {
                agent: AgentKind::Claude,
                ..
            }
        ));
    }

    #[test]
    fn check_due_has_null_process() {
        let r = row(TaskState::Running { pid: Some(1) });
        let event = check_due_event(&r, &[], PathBuf::from("/e"));
        assert_eq!(event.event, EventKind::TaskCheckDue);
        assert!(event.process.is_none());
        assert_eq!(event.next_action, NextAction::InspectTask);
        assert_eq!(event.timeout_secs, Some(4 * 3600));
        let line = event.to_message_line().unwrap();
        assert!(line.contains("\"process\":null"));
        assert!(line.contains("TASK_CHECK_DUE"));
    }

    #[test]
    fn notify_has_null_process() {
        let r = row(TaskState::Running { pid: Some(1) });
        let report = report(1, ReportOutcome::Blocked);
        let event = notify_event(&r, &report, PathBuf::from("/e"));
        assert_eq!(event.event, EventKind::TaskReported);
        assert!(event.process.is_none());
        let line = event.to_message_line().unwrap();
        assert!(line.starts_with("HOMEBASED_EVENT {"));
        assert!(line.contains("\"process\":null"));
        assert!(line.contains("\"api_version\":1"));
    }

    #[test]
    fn last_event_uses_the_latest_delivery_timestamp() {
        let now = Utc::now();
        let mut r = row(TaskState::Running { pid: Some(1) });
        r.attention = AttentionState::Delivered { at: now };
        let mut later_report = report(1, ReportOutcome::Blocked);
        later_report.notified_at = Some(now + chrono::TimeDelta::seconds(1));
        let event = last_event_for_row(&r, &[later_report], PathBuf::from("/e")).unwrap();
        assert_eq!(event.event, EventKind::TaskReported);

        let mut earlier_report = report(2, ReportOutcome::Succeeded);
        earlier_report.notified_at = Some(now - chrono::TimeDelta::seconds(1));
        let event = last_event_for_row(&r, &[earlier_report], PathBuf::from("/e")).unwrap();
        assert_eq!(event.event, EventKind::TaskCheckDue);
    }

    #[test]
    fn key_order_starts_with_api_version_event_task() {
        let event = exit_event(
            &row(TaskState::Finished {
                reason: ExitReason::Exit { code: 0 },
            }),
            &[],
            PathBuf::from("/e"),
        );
        let line = event.to_message_line().unwrap();
        let json = line.strip_prefix("HOMEBASED_EVENT ").unwrap();
        let start = &json[..80];
        assert!(start.starts_with("{\"api_version\":1,\"event\":\"TASK_SUCCEEDED\",\"task\":"));
    }

    #[test]
    fn attention_settle_outlasts_worst_case_queue_send() {
        // relationship is enforced by the compile-time assert above; lock the
        // public durations so a silent edit cannot shrink them independently
        assert_eq!(QUEUE_ATTEMPT_TIMEOUT, Duration::from_secs(20));
        assert_eq!(ATTENTION_SETTLE, Duration::from_secs(120));
        assert_eq!(QUEUE_ATTEMPT_WORST_CASE_SECS, 20 + 2 + 2 * 2);
        assert_eq!(
            QUEUE_SEND_WORST_CASE_SECS,
            (20 + 2 + 2 * 2) * 3 + 1 + 20 + 5
        );
    }

    #[test]
    fn queue_attempt_deadline_stops_the_child_and_returns() {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("slow-queue.sh");
        let pid_file = dir.path().join("pid");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\necho $$ > '{}'\n# flood stdout so an undrained pipe would block\n\
                 dd if=/dev/zero bs=1024 count=256 2>/dev/null\n\
                 sleep 1000\n",
                pid_file.display()
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&script).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&script, perms).unwrap();
        }

        let mut cmd = Command::new(&script);
        let started = Instant::now();
        let err = run_command_deadline(
            &mut cmd,
            Duration::from_secs(1),
            &dir.path().join("delivery.lock"),
        )
        .unwrap_err();
        let elapsed = started.elapsed();
        assert!(
            err.contains("timed out"),
            "expected timeout error, got {err}"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "send returned too slowly: {elapsed:?}"
        );
        assert!(
            elapsed >= Duration::from_millis(900),
            "deadline returned too early: {elapsed:?}"
        );

        // child (and its sleep descendant) must be gone
        let pid_raw = std::fs::read_to_string(&pid_file).unwrap();
        let pid: i32 = pid_raw.trim().parse().unwrap();
        let still_alive = kill(Pid::from_raw(pid), None).is_ok();
        assert!(!still_alive, "timed-out child pid={pid} is still alive");
    }

    #[test]
    fn delivery_lock_serializes_queue_processes() {
        let dir = tempfile::tempdir().unwrap();
        let gate = dir.path().join("gate");
        let entered = dir.path().join("entered");
        let order = dir.path().join("order");
        let delivery_lock = dir.path().join("delivery.lock");
        std::fs::write(&gate, "closed").unwrap();

        let first_lock = delivery_lock.clone();
        let first_gate = gate.clone();
        let first_entered = entered.clone();
        let first_order = order.clone();
        let first = thread::spawn(move || {
            let mut cmd = Command::new("/bin/sh");
            cmd.args([
                "-c",
                "touch \"$ENTERED\"; while test -e \"$GATE\"; do sleep 0.02; done; printf 'first\\n' >> \"$ORDER\"",
            ])
            .env("ENTERED", first_entered)
            .env("GATE", first_gate)
            .env("ORDER", first_order);
            run_command_deadline(&mut cmd, Duration::from_secs(3), &first_lock).unwrap()
        });
        while !entered.exists() {
            thread::sleep(Duration::from_millis(10));
        }

        let second_lock = delivery_lock.clone();
        let second_order = order.clone();
        let second = thread::spawn(move || {
            let mut cmd = Command::new("/bin/sh");
            cmd.args(["-c", "printf 'second\\n' >> \"$ORDER\""])
                .env("ORDER", second_order);
            run_command_deadline(&mut cmd, Duration::from_secs(3), &second_lock).unwrap()
        });
        thread::sleep(Duration::from_millis(100));
        assert!(
            !order.exists(),
            "second sender passed the held delivery lock"
        );

        std::fs::remove_file(gate).unwrap();
        assert!(first.join().unwrap().status.success());
        assert!(second.join().unwrap().status.success());
        assert_eq!(std::fs::read_to_string(order).unwrap(), "first\nsecond\n");
    }
}
