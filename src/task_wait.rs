//! Wire types and chain bookkeeping for `homebased task wait`
//!
//! A waiter holds one request on the daemon socket and receives the callback
//! lines of its tasks as newline-delimited JSON frames, in place of the origin
//! thread's normal delivery. The daemon and the CLI both track which tasks are
//! still pending with [`WaitSet`], so a reconnect after a daemon restart asks
//! only for the work that is left
//!
//! The daemon settles an event as delivered only once the client confirms, on a
//! separate request, that it printed the line. A client that dies between
//! receiving and printing never confirms, so the event is delivered normally

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::callback::EventKind;
use crate::domain::{TaskId, ThreadId};
use crate::waiting::WaitTargets;

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

/// Body of a wait request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitRequest {
    /// Thread of the waiting session; it must be the origin thread of every task
    pub thread: ThreadId,
    /// Tasks to wait on
    pub tasks: WaitTargets,
}

/// Body of an ack: the client printed one event of its stream
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitAck {
    /// Stream the event arrived on
    pub waiter: WaiterId,
    /// Task the event belongs to
    pub task: TaskId,
    /// Per-task event sequence
    pub seq: u64,
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

/// Tasks a waiter still waits on, following waiting chains
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitSet {
    pending: Vec<TaskId>,
}

impl WaitSet {
    /// Wait on every target
    #[must_use]
    pub fn new(targets: &WaitTargets) -> Self {
        Self {
            pending: targets.tasks().to_vec(),
        }
    }

    /// Tasks still pending, in the order they were first named
    #[must_use]
    pub fn pending(&self) -> &[TaskId] {
        &self.pending
    }

    /// Whether every target reached its final outcome
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.pending.is_empty()
    }

    /// Whether `task` is still pending
    #[must_use]
    pub fn contains(&self, task: TaskId) -> bool {
        self.pending.contains(&task)
    }

    /// Account for one event and return the continuation to wait on from now on
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
        let index = self.pending.iter().position(|pending| *pending == task)?;
        if kind == EventKind::TaskWaiting
            && let Some(next) = continuation
        {
            if self.pending.contains(&next) {
                self.pending.remove(index);
                return None;
            }
            self.pending[index] = next;
            return Some(next);
        }
        if ends_wait(kind) || kind == EventKind::TaskWaiting {
            self.pending.remove(index);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::WaitSet;
    use crate::callback::EventKind;
    use crate::domain::TaskId;
    use crate::waiting::WaitTargets;

    fn set(tasks: &[TaskId]) -> WaitSet {
        WaitSet::new(&WaitTargets::new(tasks.to_vec()).unwrap())
    }

    #[test]
    fn interim_events_keep_the_wait_and_final_events_end_it() {
        let (first, second) = (TaskId::new(), TaskId::new());
        let mut waiting = set(&[first, second]);

        assert_eq!(waiting.apply(first, EventKind::TaskReported, None), None);
        assert_eq!(waiting.apply(first, EventKind::TaskCheckDue, None), None);
        assert_eq!(waiting.pending(), &[first, second]);

        waiting.apply(first, EventKind::TaskSucceeded, None);
        assert_eq!(waiting.pending(), &[second]);
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
        assert_eq!(waiting.pending(), &[continuation, other]);

        waiting.apply(worker, EventKind::TaskSucceeded, None);
        assert_eq!(waiting.pending(), &[continuation, other]);
        waiting.apply(continuation, EventKind::TaskSucceeded, None);
        assert_eq!(waiting.pending(), &[other]);
    }
}
