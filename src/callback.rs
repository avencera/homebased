//! `HOMEBASED_EVENT` formatting and origin inbox delivery through T3, Codex
//! queue, or a Claude Code session socket.

pub(crate) mod claude_inbox;
mod delivery;
pub mod destination;
pub(crate) mod send_check;
pub(crate) mod stale_context;
#[cfg(test)]
mod tests;

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::container::GpuRequest;
use crate::dependency::HeldCancellation;
use crate::domain::{
    AgentKind, ExitReason, ReportKind, ReportOutcome, TaskId, TaskName, TaskReport, TaskRow,
    TaskState, ThreadId, Workload,
};
use crate::spec::NormalizedSpec;
use crate::waiting::{Parking, WaitTargets};

pub(crate) use delivery::{
    CodexWakeError, OriginSession, PendingRetry, PendingT3Send, ReachableOrigin, append_fallback,
    check_saved_callback, find_saved_inbox_origin, retry_pending_t3, send_codex_queue_attempt,
    send_saved_queue_attempt, send_t3_claude, wake_codex_thread, wake_stopped_session,
};
pub use delivery::{QUEUE_ATTEMPT_TIMEOUT, QUEUE_ATTEMPT_TIMEOUT_SECS};

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
    /// The worker parked on other tasks; its continuation runs once they end.
    TaskWaiting,
    /// Failure or non-zero exit.
    TaskFailed,
    /// Success.
    TaskSucceeded,
    /// Stopped for higher-priority work; its job is queued again.
    TaskPreempted,
}

/// Why an event names the outcome it does, when its fields alone do not say
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventReason {
    /// An agent with the report trailer exited 0 without reporting
    NoReport,
}

impl EventReason {
    /// Reason an event implies, derived from its own fields
    ///
    /// Only a missing report turns exit 0 into `TASK_FAILED` with no reports,
    /// so the reason never travels between machines: each reader derives it,
    /// and an executor stays readable by an origin that predates the field
    #[must_use]
    pub fn derive(event: &HomebasedEvent) -> Option<Self> {
        let no_report = event.event == EventKind::TaskFailed
            && event.process == Some(ProcessPayload::Exit { code: 0 })
            && event.reports.is_empty();
        no_report.then_some(Self::NoReport)
    }
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
    pub outcome: ReportKind,
    /// Summary.
    pub summary: String,
}

impl From<&TaskReport> for ReportView {
    fn from(report: &TaskReport) -> Self {
        Self {
            seq: report.seq,
            outcome: report.outcome.kind(),
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
    /// Submitted name.
    pub name: TaskName,
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
    /// Why the origin cancelled a held task before it launched. Present only on that `TASK_CANCELLED`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<HeldCancellation>,
    /// Tasks the worker waits on. Present only on `TASK_WAITING`, with `continuation`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_on: Option<WaitTargets>,
    /// Held task that continues the work once every `waiting_on` task ends.
    /// Present only on `TASK_WAITING`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<TaskId>,
    /// Why the event names its outcome, such as `no_report`. Derived by each
    /// reader with [`EventReason::derive`]; never stored or sent between machines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<EventReason>,
}

impl HomebasedEvent {
    /// The event with its derived `reason` filled in, as readers see it
    #[must_use]
    pub fn with_derived_reason(mut self) -> Self {
        self.reason = EventReason::derive(&self);
        self
    }
}

/// Build an exit event from the row's stored reason
///
/// `parking` is the handover the store saved when the run parked, so a
/// waiting exit names its targets and continuation
#[must_use]
pub fn exit_event(
    row: &TaskRow,
    reports: &[TaskReport],
    evidence: PathBuf,
    parking: Option<&Parking>,
) -> HomebasedEvent {
    build_event(
        row,
        reports,
        evidence,
        row.exit_reason().map(ProcessPayload::from),
        parking,
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

/// Build the event of a run stopped for higher-priority work
///
/// The process payload still says how the process ended, such as exit 75 after
/// a yield, but the run did not fail, so nothing needs inspecting
#[must_use]
pub fn preempted_event(row: &TaskRow, reports: &[TaskReport], evidence: PathBuf) -> HomebasedEvent {
    HomebasedEvent {
        event: EventKind::TaskPreempted,
        next_action: NextAction::None,
        ..exit_event(row, reports, evidence, None)
    }
}

/// Output-inactivity reminder. Never changes task status.
#[must_use]
pub fn check_due_event(row: &TaskRow, reports: &[TaskReport], evidence: PathBuf) -> HomebasedEvent {
    HomebasedEvent {
        api_version: crate::domain::API_VERSION,
        event: EventKind::TaskCheckDue,
        task: row.id,
        name: row.name.clone(),
        workload: WorkloadView::from(&row.workload),
        thread: row.thread,
        cwd: row.cwd.clone(),
        evidence,
        reports: reports.iter().map(ReportView::from).collect(),
        process: None,
        timeout_secs: Some(row.timeout.as_secs()),
        next_action: NextAction::InspectTask,
        cancel_reason: None,
        waiting_on: None,
        continuation: None,
        reason: None,
    }
}

fn build_event(
    row: &TaskRow,
    reports: &[TaskReport],
    evidence: PathBuf,
    process: Option<ProcessPayload>,
    parking: Option<&Parking>,
) -> HomebasedEvent {
    let exit = classify_exit(
        process.as_ref(),
        reports,
        ExitPolicy::of(&row.workload),
        row.cancel_requested_at.is_some(),
    );
    let (event, next_action, parking) = match exit {
        ExitClass::Parked => match parking {
            Some(parking) => (EventKind::TaskWaiting, NextAction::None, Some(parking)),
            // a waiting exit always parks in the transaction that ends it, so a
            // missing handover means the store refused it; nothing will resume
            None => (EventKind::TaskFailed, NextAction::InspectLog, None),
        },
        other => {
            let (event, next_action) = other.event();
            (event, next_action, None)
        }
    };
    HomebasedEvent {
        api_version: crate::domain::API_VERSION,
        event,
        task: row.id,
        name: row.name.clone(),
        workload: WorkloadView::from(&row.workload),
        thread: row.thread,
        cwd: row.cwd.clone(),
        evidence,
        reports: reports.iter().map(ReportView::from).collect(),
        process,
        timeout_secs: None,
        next_action,
        cancel_reason: None,
        waiting_on: parking.map(|parking| parking.on.clone()),
        continuation: parking.map(|parking| parking.continuation),
        reason: None,
    }
}

/// How the origin ended a held task that never launched
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnlaunchedEnding {
    /// A dependency ended without success, or cancel was requested
    Cancelled(HeldCancellation),
    /// Every dependency succeeded, but the launch was refused for this reason
    LaunchRefused(String),
}

/// Terminal event for a held task that the origin ended before any launch
///
/// No process ran, so there are no reports. A cancellation is `TASK_CANCELLED`
/// with `cancel_reason`; a refused launch is `TASK_FAILED` with a
/// `spawn_failed` process naming the refusal
#[must_use]
pub fn unlaunched_event(
    task: TaskId,
    spec: &NormalizedSpec,
    evidence: PathBuf,
    ending: UnlaunchedEnding,
) -> HomebasedEvent {
    let (event, process, cancel_reason) = match ending {
        UnlaunchedEnding::Cancelled(reason) => (
            EventKind::TaskCancelled,
            ProcessPayload::Cancelled,
            Some(reason),
        ),
        UnlaunchedEnding::LaunchRefused(message) => (
            EventKind::TaskFailed,
            ProcessPayload::SpawnFailed { message },
            None,
        ),
    };
    HomebasedEvent {
        api_version: crate::domain::API_VERSION,
        event,
        task,
        name: spec.name.clone(),
        workload: WorkloadView::from(&crate::invocation::persist_workload(&spec.workload)),
        thread: spec.thread,
        cwd: spec.cwd.clone(),
        evidence,
        reports: Vec::new(),
        process: Some(process),
        timeout_secs: None,
        next_action: NextAction::None,
        cancel_reason,
        waiting_on: None,
        continuation: None,
        reason: None,
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
        workload: WorkloadView::from(&row.workload),
        thread: row.thread,
        cwd: row.cwd.clone(),
        evidence,
        reports: vec![ReportView::from(report)],
        process: None,
        timeout_secs: None,
        next_action: NextAction::ReadReport,
        cancel_reason: None,
        waiting_on: None,
        continuation: None,
        reason: None,
    }
}

/// What the persisted workload says about an exit with no report
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitPolicy {
    /// An agent fed the report trailer: it was told to report, so silence is a failure
    ReportingAgent,
    /// An agent without the trailer: exit 0 with no report is success
    SilentAgent,
    /// A command or a container: exit 0 is success, and it cannot park
    NotAgent,
}

impl ExitPolicy {
    /// Policy of a persisted workload
    #[must_use]
    pub fn of(workload: &Workload) -> Self {
        match workload {
            Workload::Agent(agent) if agent.report_trailer => Self::ReportingAgent,
            Workload::Agent(_) => Self::SilentAgent,
            Workload::Task(_) | Workload::Container(_) => Self::NotAgent,
        }
    }
}

/// How a finished process is classified, from its raw ending and last report
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitClass {
    /// Cancelled, whatever the worker reported
    Cancelled,
    /// The runner was lost
    Lost,
    /// An agent exited 0 after a final `waiting` report: the store parks it
    Parked,
    /// The last report is blocked
    Blocked,
    /// Non-zero exit, signal, spawn failure, or a final `failed` report
    Failed,
    /// A reporting agent exited 0 without any report
    NoReport,
    /// Exit 0 with a final `succeeded` report, or no report where none is required
    Succeeded,
}

impl ExitClass {
    /// Event and next action of every class except [`Self::Parked`], whose
    /// event depends on the saved handover
    #[must_use]
    pub fn event(self) -> (EventKind, NextAction) {
        match self {
            Self::Cancelled => (EventKind::TaskCancelled, NextAction::None),
            Self::Lost => (EventKind::TaskLost, NextAction::InspectLog),
            Self::Parked => (EventKind::TaskWaiting, NextAction::None),
            Self::Blocked => (EventKind::TaskBlocked, NextAction::AnswerAndResubmit),
            Self::Failed | Self::NoReport => (EventKind::TaskFailed, NextAction::InspectLog),
            Self::Succeeded => (EventKind::TaskSucceeded, NextAction::ReviewOutput),
        }
    }
}

/// Classify a finished process; raw process status stays in the event's `process`
///
/// | Process            | Last report       | Class     |
/// | ------------------ | ----------------- | --------- |
/// | cancelled          | any               | Cancelled |
/// | runner lost        | any               | Lost      |
/// | cancel requested   | waiting           | Cancelled |
/// | exit 0, agent      | waiting           | Parked    |
/// | other ending       | waiting           | Failed    |
/// | any exit           | blocked           | Blocked   |
/// | other ending       | other             | Failed    |
/// | exit 0             | failed            | Failed    |
/// | exit 0             | succeeded         | Succeeded |
/// | exit 0             | none, reporting   | NoReport  |
/// | exit 0             | none, other       | Succeeded |
#[must_use]
pub fn classify_exit(
    process: Option<&ProcessPayload>,
    reports: &[TaskReport],
    policy: ExitPolicy,
    cancel_requested: bool,
) -> ExitClass {
    let exit_zero = matches!(process, Some(ProcessPayload::Exit { code: 0 }));
    match (process, reports.last().map(|report| &report.outcome)) {
        (Some(ProcessPayload::Cancelled), _) => ExitClass::Cancelled,
        (Some(ProcessPayload::RunnerLost), _) => ExitClass::Lost,
        // a run whose cancel was requested never parks, even if it beat the kill
        (_, Some(ReportOutcome::Waiting(_))) if cancel_requested => ExitClass::Cancelled,
        (_, Some(ReportOutcome::Waiting(_))) if exit_zero && policy != ExitPolicy::NotAgent => {
            ExitClass::Parked
        }
        (_, Some(ReportOutcome::Waiting(_))) => ExitClass::Failed,
        (_, Some(ReportOutcome::Blocked)) => ExitClass::Blocked,
        _ if !exit_zero => ExitClass::Failed,
        (_, Some(ReportOutcome::Failed)) => ExitClass::Failed,
        (_, Some(ReportOutcome::Succeeded)) => ExitClass::Succeeded,
        (_, None) if policy == ExitPolicy::ReportingAgent => ExitClass::NoReport,
        (_, None) => ExitClass::Succeeded,
    }
}

/// The event a terminal row owes its thread: `TASK_LOST` for a lost runner,
/// `TASK_PREEMPTED` for a run stopped for higher-priority work, otherwise the
/// exit event. Every exit-callback path builds its event here so
/// the lost-versus-exit choice lives in one place.
#[must_use]
pub fn terminal_event(
    row: &TaskRow,
    reports: &[TaskReport],
    evidence: PathBuf,
    parking: Option<&Parking>,
) -> HomebasedEvent {
    match row.state {
        TaskState::Finished { .. } => exit_event(row, reports, evidence, parking),
        TaskState::Preempted { .. } => preempted_event(row, reports, evidence),
        TaskState::Queued | TaskState::Running { .. } | TaskState::Lost => {
            lost_event(row, reports, evidence)
        }
    }
}

/// Last event for `task show`, if the process is terminal or a notify happened,
/// with its derived `reason`
#[must_use]
pub fn last_event_for_row(
    row: &TaskRow,
    reports: &[TaskReport],
    evidence: PathBuf,
    parking: Option<&Parking>,
) -> Option<HomebasedEvent> {
    last_event_without_reason(row, reports, evidence, parking)
        .map(HomebasedEvent::with_derived_reason)
}

fn last_event_without_reason(
    row: &TaskRow,
    reports: &[TaskReport],
    evidence: PathBuf,
    parking: Option<&Parking>,
) -> Option<HomebasedEvent> {
    match row.state {
        TaskState::Queued | TaskState::Running { .. } => {
            let report = reports
                .iter()
                .rev()
                .find(|report| report.notified_at.is_some());
            match (row.check_due_at, report) {
                (Some(check_due_at), Some(report))
                    if report
                        .notified_at
                        .is_some_and(|notified_at| notified_at > check_due_at) =>
                {
                    Some(notify_event(row, report, evidence))
                }
                (Some(_), _) => Some(check_due_event(row, reports, evidence)),
                (None, Some(report)) => Some(notify_event(row, report, evidence)),
                (None, None) => None,
            }
        }
        TaskState::Lost => Some(lost_event(row, reports, evidence)),
        TaskState::Finished { .. } => Some(exit_event(row, reports, evidence, parking)),
        TaskState::Preempted { .. } => Some(preempted_event(row, reports, evidence)),
    }
}
