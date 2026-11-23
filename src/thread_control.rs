//! `homebased thread interrupt` and `homebased thread answer`: act on a T3
//! thread on whichever Fleet machine runs it
//!
//! Both resolve the thread the way `thread compact` does, then run on the
//! machine whose T3 owns it, forwarded with the same destination and protocol
//! checks as other Fleet writes

use serde::{Deserialize, Serialize};

use crate::child_thread::LinkId;
use crate::thread_ref::{ThreadContext, ThreadSelector};

/// What to do to a thread
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ThreadControl {
    /// End the thread's active run, as the composer's stop does
    Interrupt,
    /// Stop the thread as its Stop button does: end the active run, hold its
    /// queued messages, and stop its T3 subagents
    Stop,
    /// Answer a question the thread's agent asked
    Answer {
        /// T3's runtime request id
        request: String,
        /// One answer per question, in the order the request asks them
        answers: Vec<String>,
    },
}

/// `POST /v1/threads/control` body
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadControlRequest {
    /// Public API version
    pub api_version: u32,
    /// Thread to act on; it must resolve to exactly one
    pub thread: ThreadSelector,
    /// The caller's T3 thread, whose message any T3 chip in `thread` came in
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carried_by: Option<String>,
    /// Look only on this machine, by name or UUID
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    /// What to do
    pub control: ThreadControl,
}

/// What T3 did
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ControlOutcome {
    /// T3 took the interrupt of the run that was active
    Interrupted {
        /// The run
        run: String,
    },
    /// No run was active, so nothing was interrupted
    Idle,
    /// T3 took the stop of the thread
    Stopped,
    /// T3 took the answer
    Answered {
        /// The request answered
        request: String,
    },
}

impl ControlOutcome {
    /// The word quiet output prints
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Interrupted { .. } => "interrupted",
            Self::Idle => "idle",
            Self::Stopped => "stopped",
            Self::Answered { .. } => "answered",
        }
    }
}

/// `POST /v1/threads/control` answer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadControlResponse {
    /// Public API version
    pub api_version: u32,
    /// What T3 did
    #[serde(flatten)]
    pub outcome: ControlOutcome,
    /// The thread acted on
    pub thread: ThreadContext,
    /// The child-thread link a stop closed on this machine, so its parent no
    /// longer waits on a report
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_link: Option<LinkId>,
}
