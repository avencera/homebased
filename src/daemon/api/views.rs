//! Typed JSON bodies shared by the socket API, the CLI, and the dashboard.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::callback::{HomebasedEvent, WorkloadView};
use crate::dependency::{Dependency, DependencyId, DependencyState};
use crate::domain::{
    API_VERSION, CallbackStatus, ContainerExitEvidence, ContainerId, ExitReason, ProcessStatus,
    TaskId, TaskName, TaskReport, TaskRow, TaskStatus, ThreadId, Workload,
};
use crate::machine::MachineId;
use crate::store::{TaskPresentation, UnlaunchedTask};
use crate::submission::{HeldPhase, SubmissionState};
use crate::waiting::ChainView;

/// `GET /v1/status`.
#[derive(Debug, Clone, Serialize)]
pub struct StatusBody {
    /// Schema version.
    pub api_version: u32,
    /// Crate version of the running daemon.
    pub version: &'static str,
    /// Daemon pid.
    pub pid: u32,
    /// Unix socket path.
    pub socket: String,
    /// Dashboard base URL, or `None` when the TCP listener is off or failed to bind.
    pub web: Option<String>,
    /// Queued plus running tasks.
    pub in_flight: usize,
}

/// Inactivity-reminder state for the check timeout. Public and two-valued: a
/// send that is still in flight has not been delivered, so it reads `pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckTimeoutStatus {
    /// Reminder not yet delivered.
    Pending,
    /// Reminder delivered successfully.
    Sent,
}

/// One task in `GET /v1/tasks` and the head of `GET /v1/tasks/{id}`. Also read
/// back from peers for the fleet task list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSummary {
    /// Task id.
    pub id: TaskId,
    /// Submitted name.
    pub name: TaskName,
    /// Process status, or `held` while the origin waits for dependencies.
    pub status: TaskStatus,
    /// Workload view.
    pub workload: WorkloadView,
    /// Submitting Codex thread.
    pub thread: ThreadId,
    /// Worker thread: a Claude worker's session from launch, or a Codex
    /// worker's thread once it exits, when the worker printed one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_thread: Option<ThreadId>,
    /// Working directory.
    pub cwd: PathBuf,
    /// Git worktree root captured when the executor accepted the task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_root: Option<PathBuf>,
    /// Machine that owns callbacks. Present with `execution_machine`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_machine: Option<MachineId>,
    /// Machine that runs the task. Present with `origin_machine`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_machine: Option<MachineId>,
    /// Worker pid while running. Display only.
    pub pid: Option<i32>,
    /// Terminal callback delivery state, or null when this machine delivers no
    /// callback for the task: the executor of a task whose origin is another
    /// machine, or a queue run, which reports through its job.
    pub callback: Option<CallbackStatus>,
    /// Output-inactivity timeout in seconds.
    pub timeout_secs: u64,
    /// Whether the inactivity reminder is pending or sent.
    pub check_timeout: CheckTimeoutStatus,
    /// Why the process ended, once known.
    pub exit_reason: Option<ExitReason>,
    /// When cancel was requested.
    pub cancel_requested_at: Option<DateTime<Utc>>,
    /// Insert time.
    pub created_at: DateTime<Utc>,
    /// Last row update. For a terminal task this is the finish time.
    pub updated_at: DateTime<Utc>,
    /// Tasks and jobs that must succeed before this one starts, with their current state.
    /// Present only for a task submitted with `after`, and for a held
    /// continuation, which starts once each of them ended in any way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Vec<DependencyView>>,
    /// The chain of an agent run that parked or continues parked work: the
    /// whole chain's state, so a parked run reads `waiting`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<ChainView>,
}

/// One entry of a task's or a held job's `after` list.
///
/// On the wire, `task` holds the entry's UUID, and a job entry adds
/// `"kind": "job"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(into = "WireDependencyView", from = "WireDependencyView")]
pub struct DependencyView {
    /// The task or job the entry names.
    pub dependency: Dependency,
    /// `pending`, or `ended` with the dependency's `outcome`.
    pub state: DependencyState,
}

/// Wire form of [`DependencyView`]
///
/// Peers on the previous release decode `task` from every entry, and a single
/// entry they cannot read would make them refuse the machine's whole task
/// list. A job entry therefore keeps its UUID in `task` and adds `kind`,
/// which those peers ignore
#[derive(Serialize, Deserialize)]
struct WireDependencyView {
    task: DependencyId,
    // a task entry keeps the previous release's shape, which had no kind
    #[serde(default, skip_serializing_if = "DependencyKind::is_task")]
    kind: DependencyKind,
    #[serde(flatten)]
    state: DependencyState,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DependencyKind {
    #[default]
    Task,
    Job,
}

impl DependencyKind {
    const fn is_task(&self) -> bool {
        matches!(self, Self::Task)
    }
}

impl From<DependencyView> for WireDependencyView {
    fn from(view: DependencyView) -> Self {
        let kind = match view.dependency {
            Dependency::Task(_) => DependencyKind::Task,
            Dependency::Job(_) => DependencyKind::Job,
        };
        Self {
            task: view.dependency.id(),
            kind,
            state: view.state,
        }
    }
}

impl From<WireDependencyView> for DependencyView {
    fn from(wire: WireDependencyView) -> Self {
        let dependency = match wire.kind {
            DependencyKind::Task => Dependency::Task(wire.task.as_task()),
            DependencyKind::Job => Dependency::Job(wire.task.as_job()),
        };
        Self {
            dependency,
            state: wire.state,
        }
    }
}

impl TaskSummary {
    /// Build an API view from one task row and its optional stored metadata.
    #[must_use]
    pub fn from_row(row: &TaskRow, presentation: Option<&TaskPresentation>) -> Self {
        let owners = presentation.and_then(|presentation| presentation.owners);
        Self {
            id: row.id,
            name: row.name.clone(),
            status: row.status().into(),
            workload: WorkloadView::from(&row.workload),
            thread: row.thread,
            worker_thread: presentation.and_then(|presentation| presentation.worker_thread),
            cwd: row.cwd.clone(),
            project_root: presentation.and_then(|presentation| presentation.project_root.clone()),
            origin_machine: owners.map(|owners| owners.origin_machine),
            execution_machine: owners.map(|owners| owners.execution_machine),
            pid: row.pid(),
            callback: presentation.and_then(|presentation| presentation.terminal_callback),
            timeout_secs: row.timeout.as_secs(),
            check_timeout: if presentation.map_or_else(
                || row.check_due_at.is_some(),
                |presentation| presentation.attention_delivered,
            ) {
                CheckTimeoutStatus::Sent
            } else {
                CheckTimeoutStatus::Pending
            },
            exit_reason: row.exit_reason().cloned(),
            cancel_requested_at: row.cancel_requested_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
            after: None,
            chain: presentation.and_then(|presentation| presentation.chain.clone()),
        }
    }

    /// Build the view of a task that never launched from its origin route alone.
    ///
    /// Its status is `held` while it waits, `cancelled` once the origin cancelled
    /// it, and `failed` when its launch was refused.
    #[must_use]
    pub fn from_unlaunched(task: &UnlaunchedTask, after: Vec<DependencyView>) -> Option<Self> {
        let route = &task.held.route;
        let spec = &route.spec;
        let (status, exit_reason) = match &route.submission {
            SubmissionState::Held {
                phase: HeldPhase::Cancelled { .. },
            } => (
                TaskStatus::Process(ProcessStatus::Cancelled),
                Some(ExitReason::Cancelled),
            ),
            SubmissionState::Held { .. } => (TaskStatus::Held, None),
            SubmissionState::Rejected { reason } => (
                TaskStatus::Process(ProcessStatus::Failed),
                Some(ExitReason::SpawnFailed {
                    message: reason.clone(),
                }),
            ),
            _ => return None,
        };
        // a UUID v7 task id carries its creation time
        let created_at = route
            .task
            .0
            .get_timestamp()
            .and_then(|stamp| {
                let (seconds, nanos) = stamp.to_unix();
                DateTime::from_timestamp(i64::try_from(seconds).ok()?, nanos)
            })
            .unwrap_or(route.last_updated_at);
        Some(Self {
            id: route.task,
            name: spec.name.clone(),
            status,
            workload: WorkloadView::from(&crate::invocation::persist_workload(&spec.workload)),
            thread: route.thread,
            worker_thread: None,
            cwd: spec.cwd.clone(),
            project_root: None,
            origin_machine: Some(route.origin_machine),
            execution_machine: Some(route.execution_machine),
            pid: None,
            callback: Some(task.callback),
            timeout_secs: spec.timeout.as_secs(),
            check_timeout: CheckTimeoutStatus::Pending,
            exit_reason,
            cancel_requested_at: None,
            created_at,
            updated_at: route.last_updated_at,
            // a task that waits only for its machine has no `after`
            after: task.held.after.as_ref().map(|_| after),
            chain: task.chain.clone(),
        })
    }
}

/// Private agent settings used to resume a task from the local CLI.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskFollowupSource {
    /// Public API version.
    pub api_version: u32,
    /// Agent CLI that ran the source task, Codex or Claude.
    pub agent: crate::domain::AgentKind,
    /// Model selected by the source task.
    pub model: Option<String>,
    /// Extra arguments selected by the source task.
    pub extra_args: Vec<String>,
    /// Whether the source task fed the reporting trailer to its agent.
    pub report_trailer: bool,
    /// Whether the source task saved its Claude session. Always false for Codex.
    pub persist_session: bool,
}

/// `GET /v1/tasks`.
#[derive(Debug, Clone, Serialize)]
pub struct TaskList {
    /// Schema version.
    pub api_version: u32,
    /// Tasks in id order, which is creation order for UUID v7.
    pub tasks: Vec<TaskSummary>,
}

impl TaskList {
    /// Build from task rows and their store-owned metadata.
    #[must_use]
    pub fn from_rows(
        rows: &[TaskRow],
        presentations: &std::collections::HashMap<TaskId, TaskPresentation>,
    ) -> Self {
        Self {
            api_version: API_VERSION,
            tasks: rows
                .iter()
                .map(|row| TaskSummary::from_row(row, presentations.get(&row.id)))
                .collect(),
        }
    }
}

/// `GET /v1/tasks/{id}`.
#[derive(Debug, Clone, Serialize)]
pub struct TaskDetail {
    /// Schema version.
    pub api_version: u32,
    /// Fields shared with the list.
    #[serde(flatten)]
    pub summary: TaskSummary,
    /// Worker reports in seq order.
    pub reports: Vec<TaskReport>,
    /// Combined stdout and stderr of the child.
    pub output_log: PathBuf,
    /// Task directory.
    pub evidence: PathBuf,
    /// Event already sent, or the one that will be sent. `None` while running
    /// with no interim event.
    pub last_event: Option<HomebasedEvent>,
    /// Container of a container task and its witness. Omitted for other workloads.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container: Option<ContainerDetail>,
}

/// Container that a container task owns, as the task layer saved it.
#[derive(Debug, Clone, Serialize)]
pub struct ContainerDetail {
    /// Fixed container name.
    pub name: String,
    /// Image pinned by digest.
    pub image: String,
    /// Container ID, once the worker saved it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_id: Option<ContainerId>,
    /// When a worker first saw the container start.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// Witness that the container stopped and was removed. Unconfirmed until the task ends.
    pub exit_evidence: ContainerExitEvidence,
}

impl ContainerDetail {
    /// Build the view of a container task, or `None` for other workloads.
    #[must_use]
    pub fn from_row(
        row: &TaskRow,
        record: Option<&crate::store::TaskContainerRecord>,
    ) -> Option<Self> {
        let Workload::Container(workload) = &row.workload else {
            return None;
        };
        Some(Self {
            name: crate::container::docker::container_name(row.id),
            image: workload.image.as_str().to_owned(),
            container_id: record.and_then(|record| record.container_id.clone()),
            started_at: record.and_then(|record| record.started_at),
            exit_evidence: row.container_exit_evidence.clone(),
        })
    }
}

/// `GET /v1/tasks/{id}/log`.
#[derive(Debug, Clone, Serialize)]
pub struct LogTail {
    /// Schema version.
    pub api_version: u32,
    /// Task id.
    pub id: TaskId,
    /// Log text. Empty while the child has written nothing.
    pub log: String,
    /// Whether earlier lines were dropped by `tail`.
    pub truncated: bool,
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::DependencyView;
    use crate::dependency::{Dependency, DependencyOutcome, DependencyState, TaskOutcome};
    use crate::domain::TaskId;
    use crate::queue::JobId;

    /// The `after` entry of a task view as the previous release reads it
    #[derive(Deserialize)]
    struct PreviousDependencyView {
        task: TaskId,
        #[serde(flatten)]
        state: DependencyState,
    }

    #[test]
    fn the_previous_release_reads_every_after_entry() {
        let job = JobId::new();
        let task = TaskId::new();
        let ended = DependencyState::Ended(DependencyOutcome::Known(TaskOutcome::Succeeded));
        let views = vec![
            DependencyView {
                dependency: Dependency::Job(job),
                state: DependencyState::Pending,
            },
            DependencyView {
                dependency: Dependency::Task(task),
                state: ended,
            },
        ];
        let wire = serde_json::to_value(&views).unwrap();
        assert_eq!(
            wire,
            json!([
                { "task": job, "kind": "job", "state": "pending" },
                { "task": task, "state": "ended", "outcome": "succeeded" },
            ])
        );

        let previous: Vec<PreviousDependencyView> = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(previous[0].task.0, job.as_uuid());
        assert_eq!(previous[0].state, DependencyState::Pending);
        assert_eq!(previous[1].task, task);
        assert_eq!(previous[1].state, ended);

        // and this release reads both kinds back
        let read: Vec<DependencyView> = serde_json::from_value(wire).unwrap();
        assert_eq!(read[0].dependency, Dependency::Job(job));
        assert_eq!(read[1].dependency, Dependency::Task(task));
    }
}
