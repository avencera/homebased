//! Dependencies: tasks and GPU queue jobs that must succeed before a held
//! task or job launches
//!
//! The origin daemon owns every dependency. A submission with `after` names
//! tasks or jobs that this daemon is the origin for, and the daemon holds the
//! new work until each of them succeeds. Any other ending cancels the held
//! work before launch, and that cancellation is itself an ending for work held
//! on it. A job's events reach the machine that submitted it, so a job queued
//! on another Fleet machine can release work here

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::callback::EventKind;
use crate::domain::TaskId;
use crate::queue::{JobEventKind, JobId};

/// Most tasks and jobs one submission may wait on
pub const MAX_DEPENDENCIES: usize = 16;

/// One `after` entry: the UUID of a task or a job submitted through this origin
///
/// The wire form is the bare UUID, as it was when `after` named only tasks,
/// so saved specs and routes keep their content and retry identity. The
/// origin resolves which kind it names when it reads its state
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DependencyId(Uuid);

impl DependencyId {
    /// The entry read as a task UUID
    #[must_use]
    pub const fn as_task(self) -> TaskId {
        TaskId(self.0)
    }

    /// The entry read as a job UUID
    #[must_use]
    pub const fn as_job(self) -> JobId {
        JobId::from_uuid(self.0)
    }
}

impl From<TaskId> for DependencyId {
    fn from(task: TaskId) -> Self {
        Self(task.0)
    }
}

impl From<JobId> for DependencyId {
    fn from(job: JobId) -> Self {
        Self(job.as_uuid())
    }
}

impl fmt::Display for DependencyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for DependencyId {
    type Err = uuid::Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(raw).map(Self)
    }
}

/// What a resolved `after` entry names
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dependency {
    /// A task, or the chain of agent runs it started
    Task(TaskId),
    /// A GPU queue job; its terminal job event decides
    Job(JobId),
}

impl Dependency {
    /// The `after` entry that names it
    #[must_use]
    pub fn id(self) -> DependencyId {
        match self {
            Self::Task(task) => task.into(),
            Self::Job(job) => job.into(),
        }
    }
}

impl fmt::Display for Dependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task(task) => write!(f, "task {task}"),
            Self::Job(job) => write!(f, "job {job}"),
        }
    }
}

/// Validated `after` list: non-empty, without duplicates, and at most [`MAX_DEPENDENCIES`]
///
/// The entries are kept sorted, so two requests that name the same work in a
/// different order carry the same content for retry conflict checks
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<DependencyId>", into = "Vec<DependencyId>")]
pub struct Dependencies(Vec<DependencyId>);

/// Why a submitted `after` list is invalid
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DependencyListError {
    /// `after` was present but named nothing
    #[error("after must name at least one task or job; omit it for no dependencies")]
    Empty,
    /// The same entry appears twice
    #[error("after names {id} more than once")]
    Duplicate {
        /// Position of the repeated entry in the submitted list
        index: usize,
        /// Repeated entry
        id: DependencyId,
    },
    /// The list is longer than [`MAX_DEPENDENCIES`]
    #[error("after names {count} tasks and jobs; the limit is {MAX_DEPENDENCIES}")]
    TooMany {
        /// Number of submitted entries
        count: usize,
    },
}

impl Dependencies {
    /// Validate a submitted list
    ///
    /// # Errors
    ///
    /// Refuses an empty list, a repeated entry, and more than [`MAX_DEPENDENCIES`] entries
    pub fn new(ids: Vec<DependencyId>) -> Result<Self, DependencyListError> {
        if ids.is_empty() {
            return Err(DependencyListError::Empty);
        }
        if ids.len() > MAX_DEPENDENCIES {
            return Err(DependencyListError::TooMany { count: ids.len() });
        }
        for (index, id) in ids.iter().enumerate() {
            if ids[..index].contains(id) {
                return Err(DependencyListError::Duplicate { index, id: *id });
            }
        }
        let mut ids = ids;
        ids.sort_unstable();
        Ok(Self(ids))
    }

    /// A list of tasks, such as the targets of a parked worker
    ///
    /// # Errors
    ///
    /// As [`Self::new`]
    pub fn of_tasks(tasks: &[TaskId]) -> Result<Self, DependencyListError> {
        Self::new(tasks.iter().copied().map(DependencyId::from).collect())
    }

    /// Entries in UUID order, which is creation order for UUID v7 ids
    #[must_use]
    pub fn ids(&self) -> &[DependencyId] {
        &self.0
    }
}

impl TryFrom<Vec<DependencyId>> for Dependencies {
    type Error = DependencyListError;

    fn try_from(ids: Vec<DependencyId>) -> Result<Self, Self::Error> {
        Self::new(ids)
    }
}

impl From<Dependencies> for Vec<DependencyId> {
    fn from(dependencies: Dependencies) -> Self {
        dependencies.0
    }
}

/// How a finished task ended, as its terminal `HOMEBASED_EVENT` named it
///
/// Only [`Self::Succeeded`] releases a held `after` task. It is exactly the
/// ending that produces `TASK_SUCCEEDED`: exit 0 and a last report of
/// succeeded, or no report from a workload that need not report
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskOutcome {
    /// `TASK_SUCCEEDED`
    Succeeded,
    /// `TASK_FAILED`
    Failed,
    /// `TASK_BLOCKED`
    Blocked,
    /// `TASK_CANCELLED`
    Cancelled,
    /// `TASK_LOST`
    Lost,
    /// `TASK_PREEMPTED`: the run stopped for higher-priority work and its job
    /// runs again in a new task, so this run did not succeed
    Preempted,
}

impl TaskOutcome {
    /// Outcome named by a terminal event, or `None` for an interim event
    ///
    /// `TASK_WAITING` names none: the run parked, and its chain's last run
    /// decides the outcome. Recording one here would be permanent, because the
    /// first saved outcome wins
    #[must_use]
    pub fn from_event(kind: EventKind) -> Option<Self> {
        match kind {
            EventKind::TaskSucceeded => Some(Self::Succeeded),
            EventKind::TaskFailed => Some(Self::Failed),
            EventKind::TaskBlocked => Some(Self::Blocked),
            EventKind::TaskCancelled => Some(Self::Cancelled),
            EventKind::TaskLost => Some(Self::Lost),
            EventKind::TaskPreempted => Some(Self::Preempted),
            EventKind::TaskReported | EventKind::TaskCheckDue | EventKind::TaskWaiting => None,
        }
    }

    /// Outcome a job's final event names, or `None` while the job can still run
    ///
    /// `JOB_PREEMPTED` names none: the job is queued again and resumes
    #[must_use]
    pub fn from_job_event(kind: JobEventKind) -> Option<Self> {
        match kind {
            JobEventKind::JobSucceeded => Some(Self::Succeeded),
            JobEventKind::JobFailed => Some(Self::Failed),
            JobEventKind::JobCancelled => Some(Self::Cancelled),
            JobEventKind::JobPreempted
            | JobEventKind::JobAttention
            | JobEventKind::JobBlocked
            | JobEventKind::JobCheckDue
            | JobEventKind::JobGpuIdle => None,
        }
    }

    /// Whether work held on this one may launch
    #[must_use]
    pub fn is_success(self) -> bool {
        matches!(self, Self::Succeeded)
    }

    /// SQLite storage tag
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
            Self::Lost => "lost",
            Self::Preempted => "preempted",
        }
    }

    /// Parse a storage tag
    #[must_use]
    pub fn from_storage(value: &str) -> Option<Self> {
        match value {
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "blocked" => Some(Self::Blocked),
            "cancelled" => Some(Self::Cancelled),
            "lost" => Some(Self::Lost),
            "preempted" => Some(Self::Preempted),
            _ => None,
        }
    }
}

impl fmt::Display for TaskOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a finished dependency ended, as far as its origin can prove
///
/// Process status is not an outcome: a worker can exit 0 after reporting
/// `blocked`. A task that ended with no saved outcome, or whose route is
/// missing, is [`Self::Unknown`], and an unknown ending never releases a held
/// task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyOutcome {
    /// The terminal event, or the route's closure before launch, named this outcome
    Known(TaskOutcome),
    /// The task ended, but no record says how; it reads `unknown`
    Unknown,
}

impl DependencyOutcome {
    /// Whether a task held on this one may launch
    #[must_use]
    pub fn is_success(self) -> bool {
        matches!(self, Self::Known(outcome) if outcome.is_success())
    }

    /// Wire tag: the known outcome's tag, or `unknown`
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Known(outcome) => outcome.as_str(),
            Self::Unknown => "unknown",
        }
    }
}

impl From<TaskOutcome> for DependencyOutcome {
    fn from(outcome: TaskOutcome) -> Self {
        Self::Known(outcome)
    }
}

impl fmt::Display for DependencyOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for DependencyOutcome {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DependencyOutcome {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value == Self::Unknown.as_str() {
            return Ok(Self::Unknown);
        }
        TaskOutcome::from_storage(&value)
            .map(Self::Known)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid task outcome {value}")))
    }
}

/// What a held task knows about one dependency
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "outcome")]
pub enum DependencyState {
    /// The dependency has not finished
    Pending,
    /// The dependency finished with this outcome
    Ended(DependencyOutcome),
}

/// Each named entry in order, with what it names and its state, or `None`
/// for an entry with no task or job route here
pub type DependencyLookup = Vec<(DependencyId, Option<(Dependency, DependencyState)>)>;

/// One dependency that ended without success
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DependencyFailure {
    /// Dependency task or job
    pub dependency: Dependency,
    /// How it ended
    pub outcome: DependencyOutcome,
}

/// Next step for a held task, given the state of each dependency
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeldDecision {
    /// At least one dependency has not finished, and none failed
    Wait,
    /// Every dependency succeeded
    Release,
    /// This dependency ended without success
    Cancel(DependencyFailure),
}

/// Which endings of its dependencies release a held task
///
/// `after` is public and releases only on success. A continuation is held
/// internally until its targets end with any outcome, because it must read
/// and handle a failure itself
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReleaseRule {
    /// Every dependency must succeed; any other ending cancels the held task
    #[default]
    Succeeded,
    /// Every dependency must end, with any outcome
    Ended,
}

impl ReleaseRule {
    /// SQLite storage tag
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Ended => "ended",
        }
    }

    /// Parse a storage tag
    #[must_use]
    pub fn from_storage(value: &str) -> Option<Self> {
        match value {
            "succeeded" => Some(Self::Succeeded),
            "ended" => Some(Self::Ended),
            _ => None,
        }
    }
}

/// Decide a held task's next step under its release rule
///
/// Under [`ReleaseRule::Succeeded`] the first failed dependency in order wins.
/// Under [`ReleaseRule::Ended`] nothing cancels: the task waits until every
/// dependency ended
#[must_use]
pub fn decide(
    states: impl IntoIterator<Item = (Dependency, DependencyState)>,
    rule: ReleaseRule,
) -> HeldDecision {
    let mut pending = false;
    for (dependency, state) in states {
        match state {
            DependencyState::Ended(_) if rule == ReleaseRule::Ended => {}
            DependencyState::Ended(outcome) if !outcome.is_success() => {
                return HeldDecision::Cancel(DependencyFailure {
                    dependency,
                    outcome,
                });
            }
            DependencyState::Ended(_) => {}
            DependencyState::Pending => pending = true,
        }
    }
    if pending {
        HeldDecision::Wait
    } else {
        HeldDecision::Release
    }
}

/// Why the origin cancelled a held task before it launched
///
/// The terminal `TASK_CANCELLED` event of the held task carries it as `cancel_reason`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HeldCancellation {
    /// A dependency ended without success
    DependencyEnded {
        /// Dependency task or job
        dependency: DependencyId,
        /// How it ended
        outcome: DependencyOutcome,
    },
    /// `task cancel` named the held task
    Requested,
    /// A newer task with the same `coalesce` key and machine was accepted
    Superseded {
        /// The newer task
        by: TaskId,
    },
}

impl From<DependencyFailure> for HeldCancellation {
    fn from(failure: DependencyFailure) -> Self {
        Self::DependencyEnded {
            dependency: failure.dependency.id(),
            outcome: failure.outcome,
        }
    }
}

impl fmt::Display for HeldCancellation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DependencyEnded {
                dependency,
                outcome: DependencyOutcome::Unknown,
            } => write!(f, "dependency {dependency} ended with an unknown outcome"),
            Self::DependencyEnded {
                dependency,
                outcome,
            } => write!(f, "dependency {dependency} ended {outcome}"),
            Self::Requested => f.write_str("cancel was requested before launch"),
            Self::Superseded { by } => write!(f, "superseded by task {by} before launch"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Dependencies, Dependency, DependencyFailure, DependencyId, DependencyListError,
        DependencyOutcome, DependencyState, HeldDecision, MAX_DEPENDENCIES, ReleaseRule,
        TaskOutcome, decide,
    };
    use crate::callback::EventKind;
    use crate::domain::TaskId;
    use crate::queue::{JobEventKind, JobId};

    fn id() -> DependencyId {
        TaskId::new().into()
    }

    #[test]
    fn list_refuses_empty_duplicate_and_oversized_input() {
        assert_eq!(
            Dependencies::new(Vec::new()),
            Err(DependencyListError::Empty)
        );
        let task = id();
        assert_eq!(
            Dependencies::new(vec![task, JobId::new().into(), task]),
            Err(DependencyListError::Duplicate { index: 2, id: task })
        );
        let many: Vec<_> = (0..=MAX_DEPENDENCIES).map(|_| id()).collect();
        assert_eq!(
            Dependencies::new(many),
            Err(DependencyListError::TooMany {
                count: MAX_DEPENDENCIES + 1
            })
        );
    }

    #[test]
    fn list_order_does_not_change_its_content() {
        let first = id();
        let second = DependencyId::from(JobId::new());
        assert_eq!(
            Dependencies::new(vec![second, first]).unwrap(),
            Dependencies::new(vec![first, second]).unwrap()
        );
        let wire = serde_json::to_value(Dependencies::new(vec![second, first]).unwrap());
        assert_eq!(wire.unwrap(), serde_json::json!([first, second]));
        assert!(serde_json::from_value::<Dependencies>(serde_json::json!([])).is_err());
    }

    #[test]
    fn only_a_succeeded_event_counts_as_success() {
        for (kind, success) in [
            (EventKind::TaskSucceeded, true),
            (EventKind::TaskFailed, false),
            (EventKind::TaskBlocked, false),
            (EventKind::TaskCancelled, false),
            (EventKind::TaskLost, false),
            (EventKind::TaskPreempted, false),
        ] {
            let outcome = TaskOutcome::from_event(kind).unwrap();
            assert_eq!(DependencyOutcome::from(outcome).is_success(), success);
            assert_eq!(TaskOutcome::from_storage(outcome.as_str()), Some(outcome));
        }
        assert!(!DependencyOutcome::Unknown.is_success());
        assert_eq!(TaskOutcome::from_event(EventKind::TaskReported), None);
        assert_eq!(TaskOutcome::from_event(EventKind::TaskCheckDue), None);
        assert_eq!(TaskOutcome::from_event(EventKind::TaskWaiting), None);
        // a job ends only at success, failure, or cancel; a preempted job runs again
        for (kind, outcome) in [
            (JobEventKind::JobSucceeded, Some(TaskOutcome::Succeeded)),
            (JobEventKind::JobFailed, Some(TaskOutcome::Failed)),
            (JobEventKind::JobCancelled, Some(TaskOutcome::Cancelled)),
            (JobEventKind::JobPreempted, None),
            (JobEventKind::JobAttention, None),
            (JobEventKind::JobBlocked, None),
            (JobEventKind::JobCheckDue, None),
            (JobEventKind::JobGpuIdle, None),
        ] {
            assert_eq!(TaskOutcome::from_job_event(kind), outcome, "{kind:?}");
        }
    }

    #[test]
    fn a_continuation_waits_for_every_ending_and_never_cancels() {
        let a = Dependency::Task(TaskId::new());
        let b = Dependency::Job(JobId::new());
        let failed = DependencyState::Ended(TaskOutcome::Failed.into());
        assert_eq!(
            decide(
                [(a, failed), (b, DependencyState::Pending)],
                ReleaseRule::Ended
            ),
            HeldDecision::Wait
        );
        assert_eq!(
            decide(
                [
                    (a, failed),
                    (b, DependencyState::Ended(DependencyOutcome::Unknown))
                ],
                ReleaseRule::Ended
            ),
            HeldDecision::Release
        );
    }

    #[test]
    fn decision_waits_releases_or_cancels_on_the_first_failure() {
        let a = Dependency::Task(TaskId::new());
        let b = Dependency::Job(JobId::new());
        let succeeded = DependencyState::Ended(TaskOutcome::Succeeded.into());
        assert_eq!(
            decide(
                [(a, succeeded), (b, DependencyState::Pending)],
                ReleaseRule::Succeeded
            ),
            HeldDecision::Wait
        );
        assert_eq!(
            decide([(a, succeeded), (b, succeeded)], ReleaseRule::Succeeded),
            HeldDecision::Release
        );
        assert_eq!(
            decide(
                [
                    (a, DependencyState::Pending),
                    (b, DependencyState::Ended(TaskOutcome::Blocked.into()))
                ],
                ReleaseRule::Succeeded
            ),
            HeldDecision::Cancel(DependencyFailure {
                dependency: b,
                outcome: TaskOutcome::Blocked.into()
            })
        );
    }

    #[test]
    fn an_unknown_ending_cancels_and_reads_unknown() {
        let a = Dependency::Task(TaskId::new());
        let unknown = DependencyState::Ended(DependencyOutcome::Unknown);
        assert_eq!(
            decide([(a, unknown)], ReleaseRule::Succeeded),
            HeldDecision::Cancel(DependencyFailure {
                dependency: a,
                outcome: DependencyOutcome::Unknown
            })
        );
        let wire = serde_json::to_value(unknown).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({"state": "ended", "outcome": "unknown"})
        );
        assert_eq!(
            serde_json::from_value::<DependencyState>(wire).unwrap(),
            unknown
        );
    }
}
