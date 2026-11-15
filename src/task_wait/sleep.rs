//! Wire and storage types for `homebased wait --long`, which ends the turn
//! instead of waiting in it
//!
//! A sleep registers one hold per live task or job. The daemon then settles
//! the target's progress events without waking the thread and keeps a short
//! summary of each in the hold. A final event or an alert wakes the thread as
//! normal delivery would; the final event's line lists the held events and
//! ends the hold. A connected `homebased wait` stream still takes events
//! first. See [`EventClass`](super::EventClass) for which event is which

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{WaitIds, WaitKey, WaitKeysError};
use crate::callback::{EventKind, HomebasedEvent};
use crate::domain::{TaskId, ThreadId};
use crate::queue::{JobEvent, JobEventKind, JobId};

/// Route that registers a sleep's holds
pub const SLEEP_ROUTE: &str = "/v1/tasks/sleep";

/// Longest report summary a held event keeps, in characters
const SUMMARY_CHARS: usize = 160;

/// Body of a sleep request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SleepRequest {
    /// Thread of the sleeping session; it must be the origin thread of every target
    pub thread: ThreadId,
    /// Task, job, or watch ids for the daemon to resolve
    pub ids: Vec<Uuid>,
}

impl SleepRequest {
    /// Request a sleep on `ids` for `thread`
    #[must_use]
    pub fn new(thread: ThreadId, ids: &WaitIds) -> Self {
        Self {
            thread,
            ids: ids.ids().to_vec(),
        }
    }

    /// Validated ids of the request
    ///
    /// # Errors
    ///
    /// Refuses the ids [`WaitIds::new`] refuses
    pub fn ids(&self) -> Result<WaitIds, WaitKeysError> {
        WaitIds::new(self.ids.clone())
    }
}

/// What a sleep registered for one target
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SleepOutcome {
    /// The daemon holds the target's progress events until its final event
    Held,
    /// The target's final event was already delivered, so nothing is held
    AlreadyEnded,
    /// The task parked, and the hold is on the continuation run that carries
    /// on its work
    FollowedTo(TaskId),
    /// A thread watch wakes the thread on each of its events, so it needs no hold
    Watched,
}

impl fmt::Display for SleepOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Held => f.write_str("held"),
            Self::AlreadyEnded => f.write_str("already_ended"),
            Self::FollowedTo(continuation) => write!(f, "followed_to {continuation}"),
            Self::Watched => f.write_str("watched"),
        }
    }
}

/// The outcome of a sleep for one named target
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SleepTarget {
    /// What the named id resolved to
    pub target: WaitKey,
    /// What the daemon registered for it
    pub outcome: SleepOutcome,
}

/// Response to a sleep request, one entry per requested id in request order
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SleepResponse {
    /// Outcome of each target
    pub targets: Vec<SleepTarget>,
}

impl SleepResponse {
    /// Whether any target is held, so its thread wakes later
    #[must_use]
    pub fn holds_any(&self) -> bool {
        self.targets.iter().any(|target| {
            matches!(
                target.outcome,
                SleepOutcome::Held | SleepOutcome::FollowedTo(_) | SleepOutcome::Watched
            )
        })
    }
}

/// A target a sleep can hold: a task chain or a GPU queue job
///
/// Watches wake their thread on every event, so they have no hold
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HoldTarget {
    /// The current run of a task chain
    Task(TaskId),
    /// A GPU queue job
    Job(JobId),
}

impl HoldTarget {
    /// SQLite tag of the target's kind
    #[must_use]
    pub fn kind(self) -> &'static str {
        match self {
            Self::Task(_) => "task",
            Self::Job(_) => "job",
        }
    }

    /// The target's id
    #[must_use]
    pub fn id(self) -> Uuid {
        match self {
            Self::Task(task) => task.0,
            Self::Job(job) => job.as_uuid(),
        }
    }
}

impl From<HoldTarget> for WaitKey {
    fn from(target: HoldTarget) -> Self {
        match target {
            HoldTarget::Task(task) => Self::Task(task),
            HoldTarget::Job(job) => Self::Job(job),
        }
    }
}

/// One sleep hold: a target whose progress events wait for its final event
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleepHold {
    /// Held target; a task hold moves to the continuation when its run parks
    pub target: HoldTarget,
    /// Thread that sleeps on the target
    pub thread: ThreadId,
    /// When the sleep registered the hold
    pub created_at: DateTime<Utc>,
    /// Progress events settled without waking the thread, in order
    pub held: Vec<HeldEvent>,
}

/// Name of a held event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HeldKind {
    /// A `TASK_*` event of a task chain
    Task(EventKind),
    /// A `JOB_*` event of a GPU queue job
    Job(JobEventKind),
}

impl fmt::Display for HeldKind {
    /// The event name, as `HOMEBASED_EVENT` lines spell it
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = serde_json::to_value(self).map_err(|_| fmt::Error)?;
        f.write_str(name.as_str().unwrap_or_default())
    }
}

/// One progress event a hold settled without waking its thread, as the final
/// event's `held_events` lists it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeldEvent {
    /// Event name
    pub event: HeldKind,
    /// Run of a task chain the event belongs to, since a chain's runs number
    /// their events separately
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskId>,
    /// Per-task or per-job event sequence
    pub seq: u64,
    /// When the event happened, or for a task event when the daemon held it
    pub at: DateTime<Utc>,
    /// One line on what the event reported
    pub summary: String,
}

impl HeldEvent {
    /// Held entry for task event `seq`, held at `at`
    #[must_use]
    pub fn task(event: &HomebasedEvent, seq: u64, at: DateTime<Utc>) -> Self {
        let summary = match (event.event, event.continuation, event.reports.last()) {
            (EventKind::TaskWaiting, Some(continuation), _) => {
                format!("parked; continues as task {continuation}")
            }
            (_, _, Some(report)) => format!(
                "report {} ({}): {}",
                report.seq,
                report.outcome,
                shorten(&report.summary)
            ),
            _ => HeldKind::Task(event.event).to_string(),
        };
        Self {
            event: HeldKind::Task(event.event),
            task: Some(event.task),
            seq,
            at,
            summary,
        }
    }

    /// Held entry for a job event
    #[must_use]
    pub fn job(event: &JobEvent) -> Self {
        let summary = match (event.event, &event.gpu_idle, event.run) {
            (JobEventKind::JobGpuIdle, Some(notice), _) => format!(
                "GPU idle reminder {} of episode {}: idle {} min",
                notice.reminder,
                notice.episode,
                notice.idle_secs / 60
            ),
            (JobEventKind::JobPreempted, _, Some(run)) => {
                let resource = event
                    .resource_name
                    .as_ref()
                    .map_or_else(|| run.resource.to_string(), ToString::to_string);
                format!("run {} preempted on {resource}; requeued", run.run_number)
            }
            (JobEventKind::JobPreempted, _, None) => "preempted; requeued".to_owned(),
            _ => HeldKind::Job(event.event).to_string(),
        };
        Self {
            event: HeldKind::Job(event.event),
            task: None,
            seq: event.seq,
            at: event.at,
            summary,
        }
    }
}

/// `summary` cut to [`SUMMARY_CHARS`] characters on one line
fn shorten(summary: &str) -> String {
    let line = summary.lines().next().unwrap_or_default().trim();
    if line.chars().count() <= SUMMARY_CHARS {
        return line.to_owned();
    }
    let cut: String = line.chars().take(SUMMARY_CHARS - 1).collect();
    format!("{cut}…")
}
