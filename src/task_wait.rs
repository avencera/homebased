//! Wire types and chain bookkeeping for `homebased task wait`
//!
//! A waiter holds one request on the daemon socket and receives the callback
//! lines of its tasks and GPU queue jobs as newline-delimited JSON frames, in
//! place of the origin thread's normal delivery. The daemon and the CLI both
//! track which targets are still pending with [`WaitSet`], so a reconnect after
//! a daemon restart asks only for the work that is left
//!
//! The daemon settles an event as delivered only once the client confirms, on a
//! separate request, that it printed the line. A client that dies between
//! receiving and printing never confirms, so the event is delivered normally
//!
//! Job support was added after task waits shipped. Task-only requests, frames,
//! and acks keep their original shape, so clients and daemons from either side
//! of that change still understand each other while `homebased update`
//! restarts the daemon under a connected waiter

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::callback::EventKind;
use crate::domain::{TaskId, ThreadId};
use crate::queue::{JobEventKind, JobId};
use crate::waiting::MAX_WAIT_TARGETS;

/// Route that streams callback events to a waiter
pub const WAIT_ROUTE: &str = "/v1/tasks/wait";

/// Route a waiter confirms printed events on
pub const WAIT_ACK_ROUTE: &str = "/v1/tasks/wait/ack";

/// Identifies one wait stream; the stream's first frame names it
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WaiterId(Uuid);

impl Default for WaiterId {
    fn default() -> Self {
        Self(Uuid::now_v7())
    }
}

/// One target of a wait, whose events arrive in its own sequence
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WaitKey {
    /// A task and its waiting chain, reported through `TASK_*` events
    Task(TaskId),
    /// A GPU queue job, reported through `JOB_*` events
    Job(JobId),
}

impl fmt::Display for WaitKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task(task) => write!(f, "task {task}"),
            Self::Job(job) => write!(f, "job {job}"),
        }
    }
}

/// Targets of one wait: 1 to [`MAX_WAIT_TARGETS`] tasks and jobs in total,
/// without duplicates, tasks first in the order they were named
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitKeys(Vec<WaitKey>);

/// Why the targets of a wait are invalid
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WaitKeysError {
    /// No target was named
    #[error("name at least one task, or a job with --job, to wait on")]
    Empty,
    /// The same target appears twice
    #[error("{key} is named more than once")]
    Duplicate {
        /// Repeated target
        key: WaitKey,
    },
    /// More than [`MAX_WAIT_TARGETS`] targets
    #[error("{count} tasks and jobs named; the limit is {MAX_WAIT_TARGETS}")]
    TooMany {
        /// Number of named targets
        count: usize,
    },
}

impl WaitKeys {
    /// Validate the tasks and jobs one wait names
    ///
    /// # Errors
    ///
    /// Refuses no targets, a repeated target, and more than [`MAX_WAIT_TARGETS`]
    /// tasks and jobs together
    pub fn new(tasks: Vec<TaskId>, jobs: Vec<JobId>) -> Result<Self, WaitKeysError> {
        let keys: Vec<WaitKey> = tasks
            .into_iter()
            .map(WaitKey::Task)
            .chain(jobs.into_iter().map(WaitKey::Job))
            .collect();
        if keys.is_empty() {
            return Err(WaitKeysError::Empty);
        }
        if keys.len() > MAX_WAIT_TARGETS {
            return Err(WaitKeysError::TooMany { count: keys.len() });
        }
        for (index, key) in keys.iter().enumerate() {
            if keys[..index].contains(key) {
                return Err(WaitKeysError::Duplicate { key: *key });
            }
        }
        Ok(Self(keys))
    }

    /// Targets in order
    #[must_use]
    pub fn keys(&self) -> &[WaitKey] {
        &self.0
    }
}

/// Body of a wait request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitRequest {
    /// Thread of the waiting session; it must be the origin thread of every target
    pub thread: ThreadId,
    /// Tasks to wait on
    #[serde(default)]
    pub tasks: Vec<TaskId>,
    /// GPU queue jobs to wait on
    // omitted when empty so a task-only request reads the same to a daemon
    // that predates job waits
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<JobId>,
}

impl WaitRequest {
    /// Request for every target still pending in `set`
    #[must_use]
    pub fn new(thread: ThreadId, set: &WaitSet) -> Self {
        let (mut tasks, mut jobs) = (Vec::new(), Vec::new());
        for key in set.pending() {
            match key {
                WaitKey::Task(task) => tasks.push(*task),
                WaitKey::Job(job) => jobs.push(*job),
            }
        }
        Self {
            thread,
            tasks,
            jobs,
        }
    }

    /// Validated targets of the request
    ///
    /// # Errors
    ///
    /// Refuses the targets [`WaitKeys::new`] refuses
    pub fn targets(&self) -> Result<WaitKeys, WaitKeysError> {
        WaitKeys::new(self.tasks.clone(), self.jobs.clone())
    }
}

/// Body of an ack: the client printed one event of its stream
///
/// Names exactly one of `task` and `job`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitAck {
    /// Stream the event arrived on
    pub waiter: WaiterId,
    /// Task the event belongs to
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskId>,
    /// Job the event belongs to
    // optional so a task ack keeps the shape older daemons and clients use
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<JobId>,
    /// Per-target event sequence
    pub seq: u64,
}

impl WaitAck {
    /// Ack for event `seq` of `key`
    #[must_use]
    pub fn new(waiter: WaiterId, key: WaitKey, seq: u64) -> Self {
        let (task, job) = match key {
            WaitKey::Task(task) => (Some(task), None),
            WaitKey::Job(job) => (None, Some(job)),
        };
        Self {
            waiter,
            task,
            job,
            seq,
        }
    }

    /// Target the ack names, or `None` when it names neither or both
    #[must_use]
    pub fn key(&self) -> Option<WaitKey> {
        match (self.task, self.job) {
            (Some(task), None) => Some(WaitKey::Task(task)),
            (None, Some(job)) => Some(WaitKey::Job(job)),
            _ => None,
        }
    }
}

/// One newline-delimited frame of a wait response
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitFrame {
    /// First frame of every stream; names it for acks
    Open {
        /// Id to send with each ack
        waiter: WaiterId,
    },
    /// A callback event for a pending task
    Event {
        /// Task the event belongs to
        task: TaskId,
        /// Per-task event sequence
        seq: u64,
        /// What the event reports
        kind: EventKind,
        /// Continuation run, on `TASK_WAITING`
        continuation: Option<TaskId>,
        /// The `HOMEBASED_EVENT` line, exactly as normal delivery would send it
        line: String,
    },
    /// A callback event for a pending job
    ///
    /// Sent only on streams that asked for jobs, so clients that predate job
    /// waits never see it
    JobEvent {
        /// Job the event belongs to
        job: JobId,
        /// Per-job event sequence
        seq: u64,
        /// What the event reports
        kind: JobEventKind,
        /// The `HOMEBASED_EVENT` line, exactly as normal delivery would send it
        line: String,
    },
    /// Keeps an idle stream observable; carries nothing
    Ping,
    /// The daemon is shutting down; reconnect to continue
    Restart,
    /// Every target reached its final outcome
    Done,
}

/// Whether an event ends the wait for its task
///
/// `TASK_REPORTED` and `TASK_CHECK_DUE` leave the task running. `TASK_WAITING`
/// parks the work but hands it to a continuation run. Every other event is final,
/// including `TASK_PREEMPTED`: a preempted queue run has no origin route, so its
/// job continues under new runs that report through `JOB_*` events, not here
#[must_use]
pub fn ends_wait(kind: EventKind) -> bool {
    match kind {
        EventKind::TaskReported | EventKind::TaskCheckDue | EventKind::TaskWaiting => false,
        EventKind::TaskCancelled
        | EventKind::TaskLost
        | EventKind::TaskBlocked
        | EventKind::TaskFailed
        | EventKind::TaskSucceeded
        | EventKind::TaskPreempted => true,
    }
}

/// Whether an event ends the wait for its job
///
/// Every way a job ends records one of `JOB_SUCCEEDED`, `JOB_FAILED`, and
/// `JOB_CANCELLED`. `JOB_PREEMPTED` requeues the job, and `JOB_BLOCKED` and
/// `JOB_CHECK_DUE` are notices about a job that is still queued or running.
/// `JOB_ATTENTION` reports a failed cleanup of one run's resource and says
/// nothing about the job; when it follows the final event of the job's last
/// run, the wait has already ended and normal delivery carries it
#[must_use]
pub fn job_ends_wait(kind: JobEventKind) -> bool {
    match kind {
        JobEventKind::JobPreempted
        | JobEventKind::JobAttention
        | JobEventKind::JobBlocked
        | JobEventKind::JobCheckDue => false,
        JobEventKind::JobSucceeded | JobEventKind::JobFailed | JobEventKind::JobCancelled => true,
    }
}

/// Tasks and jobs a waiter still waits on, following waiting chains
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitSet {
    pending: Vec<WaitKey>,
}

impl WaitSet {
    /// Wait on every target
    #[must_use]
    pub fn new(targets: &WaitKeys) -> Self {
        Self {
            pending: targets.keys().to_vec(),
        }
    }

    /// Targets still pending, in the order they were first named
    #[must_use]
    pub fn pending(&self) -> &[WaitKey] {
        &self.pending
    }

    /// Whether any pending target is a job
    #[must_use]
    pub fn has_jobs(&self) -> bool {
        self.pending
            .iter()
            .any(|key| matches!(key, WaitKey::Job(_)))
    }

    /// Whether every target reached its final outcome
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.pending.is_empty()
    }

    /// Whether `key` is still pending
    #[must_use]
    pub fn contains(&self, key: WaitKey) -> bool {
        self.pending.contains(&key)
    }

    /// Account for one task event and return the continuation to wait on from now on
    ///
    /// An event for a task that is not pending changes nothing. A `TASK_WAITING`
    /// event without a continuation ends the wait for its task, since nothing
    /// is left to follow
    pub fn apply(
        &mut self,
        task: TaskId,
        kind: EventKind,
        continuation: Option<TaskId>,
    ) -> Option<TaskId> {
        let index = self
            .pending
            .iter()
            .position(|pending| *pending == WaitKey::Task(task))?;
        if kind == EventKind::TaskWaiting
            && let Some(next) = continuation
        {
            if self.contains(WaitKey::Task(next)) {
                self.pending.remove(index);
                return None;
            }
            self.pending[index] = WaitKey::Task(next);
            return Some(next);
        }
        if ends_wait(kind) || kind == EventKind::TaskWaiting {
            self.pending.remove(index);
        }
        None
    }

    /// Account for one job event; a final event ends the wait for its job
    pub fn apply_job(&mut self, job: JobId, kind: JobEventKind) {
        if job_ends_wait(kind) {
            self.pending.retain(|pending| *pending != WaitKey::Job(job));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{WaitAck, WaitKey, WaitKeys, WaitKeysError, WaitRequest, WaitSet, WaiterId};
    use crate::callback::EventKind;
    use crate::domain::{TaskId, ThreadId};
    use crate::queue::JobId;
    use crate::waiting::MAX_WAIT_TARGETS;

    fn set(tasks: &[TaskId]) -> WaitSet {
        WaitSet::new(&WaitKeys::new(tasks.to_vec(), Vec::new()).unwrap())
    }

    fn keys(tasks: &[TaskId]) -> Vec<WaitKey> {
        tasks.iter().copied().map(WaitKey::Task).collect()
    }

    #[test]
    fn interim_events_keep_the_wait_and_final_events_end_it() {
        let (first, second) = (TaskId::new(), TaskId::new());
        let mut waiting = set(&[first, second]);

        assert_eq!(waiting.apply(first, EventKind::TaskReported, None), None);
        assert_eq!(waiting.apply(first, EventKind::TaskCheckDue, None), None);
        assert_eq!(waiting.pending(), keys(&[first, second]));

        waiting.apply(first, EventKind::TaskSucceeded, None);
        assert_eq!(waiting.pending(), keys(&[second]));
        waiting.apply(second, EventKind::TaskFailed, None);
        assert!(waiting.is_done());
    }

    #[test]
    fn a_waiting_event_hands_the_wait_to_its_continuation() {
        let (worker, continuation, other) = (TaskId::new(), TaskId::new(), TaskId::new());
        let mut waiting = set(&[worker, other]);

        assert_eq!(
            waiting.apply(worker, EventKind::TaskWaiting, Some(continuation)),
            Some(continuation)
        );
        assert_eq!(waiting.pending(), keys(&[continuation, other]));

        waiting.apply(worker, EventKind::TaskSucceeded, None);
        assert_eq!(waiting.pending(), keys(&[continuation, other]));
        waiting.apply(continuation, EventKind::TaskSucceeded, None);
        assert_eq!(waiting.pending(), keys(&[other]));
    }

    #[test]
    fn targets_count_tasks_and_jobs_together() {
        let job = JobId::new();
        assert_eq!(
            WaitKeys::new(Vec::new(), vec![job, job]),
            Err(WaitKeysError::Duplicate {
                key: WaitKey::Job(job)
            })
        );
        let tasks = (0..MAX_WAIT_TARGETS).map(|_| TaskId::new()).collect();
        assert_eq!(
            WaitKeys::new(tasks, vec![job]),
            Err(WaitKeysError::TooMany {
                count: MAX_WAIT_TARGETS + 1
            })
        );
        assert_eq!(
            WaitKeys::new(Vec::new(), Vec::new()),
            Err(WaitKeysError::Empty)
        );
    }

    #[test]
    fn a_task_only_request_keeps_the_shape_older_daemons_read() {
        let (thread, task) = (ThreadId::DETACHED, TaskId::new());
        let request = WaitRequest::new(thread, &set(&[task]));
        let wire = serde_json::to_value(&request).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({ "thread": thread, "tasks": [task] })
        );

        let waiter = WaiterId::default();
        let ack = serde_json::to_value(WaitAck::new(waiter, WaitKey::Task(task), 3)).unwrap();
        assert_eq!(
            ack,
            serde_json::json!({ "waiter": waiter, "task": task, "seq": 3 })
        );
    }
}
