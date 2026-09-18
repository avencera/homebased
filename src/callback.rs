//! `HOMEBASED_EVENT` formatting and `codex queue` invocation.

use std::fs::OpenOptions;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, BorrowedFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde::Serialize;

use crate::domain::{
    AgentKind, CallbackStatus, ExitReason, ReportOutcome, TaskId, TaskReport, TaskRow, TaskState,
    ThreadId, Workload,
};
use crate::error::AppError;
use crate::home::Home;
use crate::invocation::resolve_agent_binary;
use crate::store::{CallbackClaim, Store};

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

/// How long a terminal callback waits for an in-flight attention reminder
/// before it releases a claim stranded by a dead daemon. Must exceed the
/// worst-case live `codex queue` send.
pub const ATTENTION_SETTLE_SECS: u64 = 120;
/// [`ATTENTION_SETTLE_SECS`] as a [`Duration`].
pub const ATTENTION_SETTLE: Duration = Duration::from_secs(ATTENTION_SETTLE_SECS);

const _: () = assert!(
    ATTENTION_SETTLE_SECS > QUEUE_SEND_WORST_CASE_SECS,
    "attention settlement must outlast the worst-case queue send"
);

/// Poll interval while waiting for that claim to settle.
pub const ATTENTION_POLL: Duration = Duration::from_millis(100);

/// Public workload view. Omits private prompt and extra-arg fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkloadView {
    /// Agent CLI identity.
    Agent {
        /// Agent kind.
        agent: AgentKind,
        /// Model alias, or null.
        model: Option<String>,
    },
    /// Task argv preview.
    Task {
        /// Full argv including the program.
        command: Vec<String>,
    },
}

impl From<&Workload> for WorkloadView {
    fn from(workload: &Workload) -> Self {
        match workload {
            Workload::Agent(agent) => Self::Agent {
                agent: agent.agent.kind,
                model: agent.agent.model.clone(),
            },
            Workload::Task(task) => Self::Task {
                command: task.command.to_vec(),
            },
        }
    }
}

/// Derived event name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventKind {
    /// Interim `--notify` report.
    TaskReported,
    /// Attention timer expired; child still running.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NextAction {
    /// Read the interim report.
    ReadReport,
    /// Inspect status and recent logs after an attention reminder.
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HomebasedEvent {
    /// Schema version. First key.
    pub api_version: u32,
    /// Derived event name.
    pub event: EventKind,
    /// Task id.
    pub task: TaskId,
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
    /// Configured attention timeout in seconds. Present on check-due events.
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

/// Attention-timer reminder. Never changes task status.
#[must_use]
pub fn check_due_event(row: &TaskRow, reports: &[TaskReport], evidence: PathBuf) -> HomebasedEvent {
    HomebasedEvent {
        api_version: crate::domain::API_VERSION,
        event: EventKind::TaskCheckDue,
        task: row.id,
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

/// Claim, send via `codex queue` (3 attempts), record Sent/Failed.
pub fn deliver_exit_event(
    store: &Store,
    home: &Home,
    row: &TaskRow,
    event: &HomebasedEvent,
) -> Result<(), AppError> {
    if !claim_exit_callback(store, row.id)? {
        return Ok(());
    }
    let line = event.to_message_line()?;
    let paths = home.task_paths(row.id);
    let result = send_queue(row, &line, &paths.callback_log, &paths.delivery_lock);
    match result {
        Ok(()) => store.finish_callback(row.id, CallbackStatus::Sent)?,
        Err(err) => {
            store.finish_callback(row.id, CallbackStatus::Failed)?;
            append_fallback(&home.fallback_log_path(), &line, &err.to_string())?;
        }
    }
    Ok(())
}

/// Claim the terminal callback, waiting out an in-flight `TASK_CHECK_DUE` so
/// the reminder can never land after the terminal event. Blocking; the daemon
/// has its own async claim loop over the same store calls.
fn claim_exit_callback(store: &Store, id: TaskId) -> Result<bool, AppError> {
    let deadline = std::time::Instant::now() + ATTENTION_SETTLE;
    loop {
        match store.claim_callback(id)? {
            CallbackClaim::Claimed => return Ok(true),
            CallbackClaim::NotOurs => return Ok(false),
            CallbackClaim::WaitForAttention if std::time::Instant::now() >= deadline => {
                tracing::warn!(%id, "attention claim stranded; releasing it to deliver the terminal event");
                store.release_attention(id)?;
            }
            CallbackClaim::WaitForAttention => thread::sleep(ATTENTION_POLL),
        }
    }
}

/// Best-effort interim notify. Does not claim the exit callback.
pub fn deliver_notify(home: &Home, row: &TaskRow, event: &HomebasedEvent) -> Result<(), AppError> {
    let line = event.to_message_line()?;
    let paths = home.task_paths(row.id);
    send_queue(row, &line, &paths.callback_log, &paths.delivery_lock)
}

/// Run `codex queue` up to three bounded attempts. Blocking; call from
/// `spawn_blocking` in the daemon.
pub(crate) fn send_queue(
    row: &TaskRow,
    line: &str,
    log_path: &Path,
    delivery_lock: &Path,
) -> Result<(), AppError> {
    let binary = resolve_agent_binary(AgentKind::Codex, &row.env.path, &row.cwd)?;
    let mut last_err = String::new();
    for attempt in 0..QUEUE_ATTEMPTS {
        if attempt > 0 {
            thread::sleep(Duration::from_millis(200 * u64::from(attempt)));
        }
        let mut cmd = Command::new(&binary);
        cmd.args([
            "queue",
            "--thread",
            &row.thread.to_string(),
            "--message",
            line,
        ])
        .env("PATH", &row.env.path)
        .env("HOME", &row.env.home)
        .current_dir(&row.cwd);
        match run_command_deadline(&mut cmd, QUEUE_ATTEMPT_TIMEOUT, delivery_lock) {
            Ok(out) if out.status.success() => {
                let _ = std::fs::write(log_path, transcript(&out));
                return Ok(());
            }
            Ok(out) => {
                last_err = format!(
                    "codex queue exit={} stderr={}",
                    out.status.code().unwrap_or(-1),
                    String::from_utf8_lossy(&out.stderr)
                );
                let _ = std::fs::write(log_path, transcript(&out));
            }
            Err(err) => {
                last_err = err;
            }
        }
    }
    Err(AppError::Internal { message: last_err })
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
    use std::io::Write;
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
    use crate::domain::{Agent, AgentWorkload, AttentionState, TaskEnv, Workload};
    use chrono::Utc;
    use std::time::Duration;

    fn row(state: TaskState) -> TaskRow {
        TaskRow {
            id: TaskId::new(),
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
            callback_status: CallbackStatus::Pending,
            attention: AttentionState::Pending,
            cancel_requested_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
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
