//! Task dependencies: tasks that must succeed before a held task launches
//!
//! The origin daemon owns every dependency. A submission with `after` names
//! tasks that this daemon is the origin for, and the daemon holds the new task
//! until each of them succeeds. Any other ending cancels the held task before
//! launch, and that cancellation is itself an ending for tasks held on it

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::callback::EventKind;
use crate::domain::TaskId;

/// Most tasks one submission may wait on
pub const MAX_DEPENDENCIES: usize = 16;

/// Validated `after` list: non-empty, without duplicates, and at most [`MAX_DEPENDENCIES`]
///
/// The tasks are kept sorted, so two requests that name the same tasks in a
/// different order carry the same content for retry conflict checks
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<TaskId>", into = "Vec<TaskId>")]
pub struct TaskDependencies(Vec<TaskId>);

/// Why a submitted `after` list is invalid
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DependencyListError {
    /// `after` was present but named no task
    #[error("after must name at least one task; omit it for no dependencies")]
    Empty,
    /// The same task appears twice
    #[error("after names task {task} more than once")]
    Duplicate {
        /// Position of the repeated entry in the submitted list
        index: usize,
        /// Repeated task
        task: TaskId,
    },
    /// The list is longer than [`MAX_DEPENDENCIES`]
    #[error("after names {count} tasks; the limit is {MAX_DEPENDENCIES}")]
    TooMany {
        /// Number of submitted entries
        count: usize,
    },
}

impl TaskDependencies {
    /// Validate a submitted list
    ///
    /// # Errors
    ///
    /// Refuses an empty list, a repeated task, and more than [`MAX_DEPENDENCIES`] tasks
    pub fn new(tasks: Vec<TaskId>) -> Result<Self, DependencyListError> {
        if tasks.is_empty() {
            return Err(DependencyListError::Empty);
        }
        if tasks.len() > MAX_DEPENDENCIES {
            return Err(DependencyListError::TooMany { count: tasks.len() });
        }
        for (index, task) in tasks.iter().enumerate() {
            if tasks[..index].contains(task) {
                return Err(DependencyListError::Duplicate { index, task: *task });
            }
        }
        let mut tasks = tasks;
        tasks.sort_by_key(|task| task.0);
        Ok(Self(tasks))
    }

    /// Dependencies in UUID order, which is creation order for UUID v7 tasks
    #[must_use]
    pub fn tasks(&self) -> &[TaskId] {
        &self.0
    }
}

impl TryFrom<Vec<TaskId>> for TaskDependencies {
    type Error = DependencyListError;

    fn try_from(tasks: Vec<TaskId>) -> Result<Self, Self::Error> {
        Self::new(tasks)
    }
}

impl From<TaskDependencies> for Vec<TaskId> {
    fn from(dependencies: TaskDependencies) -> Self {
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

    /// Whether a task held on this one may launch
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

/// State of each named dependency, in order, or `None` for a task with no origin route here
pub type DependencyLookup = Vec<(TaskId, Option<DependencyState>)>;

/// One dependency that ended without success
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyFailure {
    /// Dependency task
    pub dependency: TaskId,
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
    states: impl IntoIterator<Item = (TaskId, DependencyState)>,
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
        /// Dependency task
        dependency: TaskId,
        /// How it ended
        outcome: DependencyOutcome,
    },
    /// `task cancel` named the held task
    Requested,
}

impl From<DependencyFailure> for HeldCancellation {
    fn from(failure: DependencyFailure) -> Self {
        Self::DependencyEnded {
            dependency: failure.dependency,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DependencyFailure, DependencyListError, DependencyOutcome, DependencyState, HeldDecision,
        MAX_DEPENDENCIES, ReleaseRule, TaskDependencies, TaskOutcome, decide,
    };
    use crate::callback::EventKind;
    use crate::domain::TaskId;

    #[test]
    fn list_refuses_empty_duplicate_and_oversized_input() {
        assert_eq!(
            TaskDependencies::new(Vec::new()),
            Err(DependencyListError::Empty)
        );
        let task = TaskId::new();
        assert_eq!(
            TaskDependencies::new(vec![task, TaskId::new(), task]),
            Err(DependencyListError::Duplicate { index: 2, task })
        );
        let many: Vec<_> = (0..=MAX_DEPENDENCIES).map(|_| TaskId::new()).collect();
        assert_eq!(
            TaskDependencies::new(many),
            Err(DependencyListError::TooMany {
                count: MAX_DEPENDENCIES + 1
            })
        );
    }

    #[test]
    fn list_order_does_not_change_its_content() {
        let first = TaskId::new();
        let second = TaskId::new();
        assert_eq!(
            TaskDependencies::new(vec![second, first]).unwrap(),
            TaskDependencies::new(vec![first, second]).unwrap()
        );
        let wire = serde_json::to_value(TaskDependencies::new(vec![second, first]).unwrap());
        assert_eq!(wire.unwrap(), serde_json::json!([first, second]));
        assert!(serde_json::from_value::<TaskDependencies>(serde_json::json!([])).is_err());
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
    }

    #[test]
    fn a_continuation_waits_for_every_ending_and_never_cancels() {
        let a = TaskId::new();
        let b = TaskId::new();
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
        let a = TaskId::new();
        let b = TaskId::new();
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
        let a = TaskId::new();
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
