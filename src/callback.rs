//! `HOMEBASED_EVENT` formatting and `codex queue` invocation.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;

use crate::domain::{
    AgentKind, AgentReport, CallbackStatus, ExitReason, ProcessStatus, ReportOutcome, TaskId,
    TaskRow, ThreadId,
};
use crate::error::AppError;
use crate::home::Home;
use crate::store::Store;

/// Derived event name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventKind {
    /// Interim `--notify` report.
    TaskReported,
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
    Exit { code: i32 },
    /// Signalled.
    Signal { signal: i32 },
    /// Timed out.
    Timeout { secs: u64 },
    /// Cancelled.
    Cancelled,
    /// Spawn failed.
    SpawnFailed { message: String },
    /// Runner lost.
    RunnerLost,
}

impl From<&ExitReason> for ProcessPayload {
    fn from(reason: &ExitReason) -> Self {
        match reason {
            ExitReason::Exit { code } => Self::Exit { code: *code },
            ExitReason::Signal { signal } => Self::Signal { signal: *signal },
            ExitReason::Timeout { secs } => Self::Timeout { secs: *secs },
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

impl From<&AgentReport> for ReportView {
    fn from(report: &AgentReport) -> Self {
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
    /// Agent kind.
    pub agent: AgentKind,
    /// Model, possibly null.
    pub model: Option<String>,
    /// Codex thread.
    pub thread: ThreadId,
    /// Task cwd.
    pub cwd: PathBuf,
    /// Absolute task directory.
    pub evidence: PathBuf,
    /// Reports in seq order.
    pub reports: Vec<ReportView>,
    /// Process payload, or null for `TASK_REPORTED`.
    pub process: Option<ProcessPayload>,
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

/// Build an exit event from row + reports + process.
#[must_use]
pub fn exit_event(
    row: &TaskRow,
    reports: &[AgentReport],
    evidence: PathBuf,
    lost: bool,
) -> HomebasedEvent {
    let process = if lost {
        Some(ProcessPayload::RunnerLost)
    } else {
        row.exit_reason.as_ref().map(ProcessPayload::from)
    };
    let (event, next_action) = derive_exit(process.as_ref(), reports);
    HomebasedEvent {
        api_version: crate::domain::API_VERSION,
        event,
        task: row.id,
        agent: row.agent.kind,
        model: row.agent.model.clone(),
        thread: row.thread,
        cwd: row.cwd.clone(),
        evidence,
        reports: reports.iter().map(ReportView::from).collect(),
        process,
        next_action,
    }
}

/// Interim `--notify` event carrying one report and `process: null`.
#[must_use]
pub fn notify_event(row: &TaskRow, report: &AgentReport, evidence: PathBuf) -> HomebasedEvent {
    HomebasedEvent {
        api_version: crate::domain::API_VERSION,
        event: EventKind::TaskReported,
        task: row.id,
        agent: row.agent.kind,
        model: row.agent.model.clone(),
        thread: row.thread,
        cwd: row.cwd.clone(),
        evidence,
        reports: vec![ReportView::from(report)],
        process: None,
        next_action: NextAction::ReadReport,
    }
}

fn derive_exit(
    process: Option<&ProcessPayload>,
    reports: &[AgentReport],
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
    if !store.claim_callback(row.id)? {
        return Ok(());
    }
    let line = event.to_message_line()?;
    let result = send_queue(home, row, &line, &home.task_paths(row.id).callback_log);
    match result {
        Ok(()) => store.finish_callback(row.id, CallbackStatus::Sent)?,
        Err(err) => {
            store.finish_callback(row.id, CallbackStatus::Failed)?;
            append_fallback(home, &line, &err.to_string())?;
        }
    }
    Ok(())
}

/// Best-effort interim notify. Does not claim the exit callback.
pub fn deliver_notify(home: &Home, row: &TaskRow, event: &HomebasedEvent) -> Result<(), AppError> {
    let line = event.to_message_line()?;
    let log = home.task_paths(row.id).callback_log;
    send_queue(home, row, &line, &log)
}

fn send_queue(home: &Home, row: &TaskRow, line: &str, log_path: &Path) -> Result<(), AppError> {
    let binary = resolve_codex(&row.env.path, &row.cwd)?;
    let mut last_err = String::new();
    for attempt in 0..3 {
        if attempt > 0 {
            thread::sleep(Duration::from_millis(200 * attempt as u64));
        }
        let output = Command::new(&binary)
            .args([
                "queue",
                "--thread",
                &row.thread.to_string(),
                "--message",
                line,
            ])
            .env("PATH", &row.env.path)
            .env("HOME", &row.env.home)
            .current_dir(&row.cwd)
            .output();
        match output {
            Ok(out) if out.status.success() => {
                let mut body = out.stdout.clone();
                body.extend_from_slice(&out.stderr);
                let _ = std::fs::write(log_path, body);
                return Ok(());
            }
            Ok(out) => {
                last_err = format!(
                    "codex queue exit={} stderr={}",
                    out.status.code().unwrap_or(-1),
                    String::from_utf8_lossy(&out.stderr)
                );
                let mut body = out.stdout.clone();
                body.extend_from_slice(&out.stderr);
                let _ = std::fs::write(log_path, body);
            }
            Err(err) => {
                last_err = err.to_string();
            }
        }
    }
    let _ = home;
    Err(AppError::Internal { message: last_err })
}

fn resolve_codex(path: &str, cwd: &Path) -> Result<PathBuf, AppError> {
    if let Ok(override_path) = std::env::var("HOMEBASED_CODEX") {
        if !override_path.is_empty() && Path::new(&override_path).is_file() {
            return Ok(PathBuf::from(override_path));
        }
    }
    which::which_in("codex", Some(OsString::from(path)), cwd).map_err(|_| {
        AppError::AgentBinaryMissing {
            agent: AgentKind::Codex,
        }
    })
}

fn append_fallback(home: &Home, line: &str, stderr: &str) -> Result<(), AppError> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(home.fallback_log_path())?;
    writeln!(file, "{line}")?;
    writeln!(file, "{stderr}")?;
    Ok(())
}

/// JSON value of an event (no prefix), for `last_event`.
pub fn event_value(event: &HomebasedEvent) -> Result<Value, AppError> {
    Ok(serde_json::to_value(event)?)
}

/// Last event for `task show`, if the process is terminal or a notify happened.
pub fn last_event_for_row(
    row: &TaskRow,
    reports: &[AgentReport],
    evidence: PathBuf,
) -> Option<HomebasedEvent> {
    match row.status {
        ProcessStatus::Queued | ProcessStatus::Running => reports
            .iter()
            .rev()
            .find(|r| r.notified_at.is_some())
            .map(|r| notify_event(row, r, evidence)),
        ProcessStatus::Lost => Some(exit_event(row, reports, evidence, true)),
        _ => Some(exit_event(row, reports, evidence, false)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Agent, TaskEnv};
    use chrono::Utc;
    use std::time::Duration;

    fn row(status: ProcessStatus, reason: Option<ExitReason>) -> TaskRow {
        TaskRow {
            id: TaskId::new(),
            thread: "01a0ab97-a7aa-7463-a5b0-8d500e40e431".parse().unwrap(),
            agent: Agent::new(AgentKind::Claude, Some("fable".into())),
            cwd: PathBuf::from("/work"),
            timeout: Duration::from_secs(4),
            extra_args: vec![],
            report_trailer: true,
            env: TaskEnv {
                path: "/bin".into(),
                home: "/home/u".into(),
            },
            binary: PathBuf::from("/bin/claude"),
            status,
            exit_reason: reason,
            callback_status: CallbackStatus::Pending,
            pid: Some(1),
            cancel_requested_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn report(seq: i64, outcome: ReportOutcome) -> AgentReport {
        AgentReport {
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
            &row(ProcessStatus::Cancelled, Some(ExitReason::Cancelled)),
            &[report(1, ReportOutcome::Succeeded)],
            PathBuf::from("/e"),
            false,
        );
        assert_eq!(event.event, EventKind::TaskCancelled);
        assert_eq!(event.next_action, NextAction::None);
    }

    #[test]
    fn lost_event() {
        let event = exit_event(
            &row(ProcessStatus::Lost, None),
            &[],
            PathBuf::from("/e"),
            true,
        );
        assert_eq!(event.event, EventKind::TaskLost);
        assert!(matches!(event.process, Some(ProcessPayload::RunnerLost)));
    }

    #[test]
    fn blocked_then_succeeded() {
        let event = exit_event(
            &row(ProcessStatus::Succeeded, Some(ExitReason::Exit { code: 0 })),
            &[
                report(1, ReportOutcome::Blocked),
                report(2, ReportOutcome::Succeeded),
            ],
            PathBuf::from("/e"),
            false,
        );
        assert_eq!(event.event, EventKind::TaskSucceeded);
        assert_eq!(event.reports.len(), 2);
        assert_eq!(event.reports[0].seq, 1);
        assert_eq!(event.reports[1].seq, 2);
    }

    #[test]
    fn no_reports_exit_zero() {
        let event = exit_event(
            &row(ProcessStatus::Succeeded, Some(ExitReason::Exit { code: 0 })),
            &[],
            PathBuf::from("/e"),
            false,
        );
        assert_eq!(event.event, EventKind::TaskSucceeded);
        assert!(event.reports.is_empty());
    }

    #[test]
    fn notify_has_null_process() {
        let r = row(ProcessStatus::Running, None);
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
    fn key_order_starts_with_api_version_event_task() {
        let event = exit_event(
            &row(ProcessStatus::Succeeded, Some(ExitReason::Exit { code: 0 })),
            &[],
            PathBuf::from("/e"),
            false,
        );
        let line = event.to_message_line().unwrap();
        let json = line.strip_prefix("HOMEBASED_EVENT ").unwrap();
        let start = &json[..80];
        assert!(start.starts_with("{\"api_version\":1,\"event\":\"TASK_SUCCEEDED\",\"task\":"));
    }
}
