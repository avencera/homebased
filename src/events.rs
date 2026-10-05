//! Durable fleet event types, separate from callback command delivery

use std::num::NonZeroU64;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::callback::{HomebasedEvent, ReportView};
use crate::domain::{CallbackStatus, ProcessStatus, TaskId};
use crate::error::AppError;
use crate::machine::MachineId;

/// Immutable content of one executor event
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventPayload {
    /// A callback payload in the existing `HOMEBASED_EVENT` version 1 format
    Callback {
        /// Existing payload; task and API version are checked at append and receive
        event: Box<HomebasedEvent>,
        /// New process state, when this callback also synchronizes state
        state: Option<ProcessStatus>,
    },
    /// Synchronize process state without a Codex notification
    State {
        /// New process state
        status: ProcessStatus,
    },
    /// Synchronize one report without a Codex notification
    Report {
        /// Immutable report content
        report: ReportView,
    },
}

impl EventPayload {
    /// Whether this event needs one origin callback delivery result
    #[must_use]
    pub fn notification_required(&self) -> bool {
        matches!(self, Self::Callback { .. })
    }

    /// State to cache at the origin, if this event carries one
    #[must_use]
    pub fn process_state(&self) -> Option<ProcessStatus> {
        match self {
            Self::Callback { state, .. } => *state,
            Self::State { status } => Some(*status),
            Self::Report { .. } => None,
        }
    }
}

/// One sequenced event addressed to a fixed origin and execution owner
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskEvent {
    /// Global task UUID
    pub task: TaskId,
    /// Monotonic per-task sequence, starting at one
    pub seq: NonZeroU64,
    /// Callback owner and event destination
    pub origin_machine: MachineId,
    /// Machine that produced the event
    pub execution_machine: MachineId,
    /// Immutable callback or state content
    pub payload: EventPayload,
}

impl TaskEvent {
    /// Format a callback with additive version-1 identity fields for later dispatch
    pub fn callback_message_line(&self) -> Result<Option<String>, AppError> {
        let EventPayload::Callback { event, .. } = &self.payload else {
            return Ok(None);
        };

        // the reason is derived here, so an executor that predates it is read the same way
        let event = event.as_ref().clone().with_derived_reason();
        let mut value = serde_json::to_value(event)?;
        let object = value.as_object_mut().ok_or_else(|| AppError::Internal {
            message: "callback payload is not an object".into(),
        })?;

        object.insert("seq".into(), serde_json::json!(self.seq.get()));
        object.insert(
            "origin_machine".into(),
            serde_json::json!(self.origin_machine),
        );
        object.insert(
            "execution_machine".into(),
            serde_json::json!(self.execution_machine),
        );

        Ok(Some(format!("HOMEBASED_EVENT {value}")))
    }
}

/// Executor transport state; callback delivery is tracked only at the origin
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxState {
    /// Waiting for a durable origin acknowledgement
    Pending,
    /// The origin committed the event
    Acknowledged,
}

/// Aggregate transport route state for one executor task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventRouteState {
    /// At least one event awaits durable origin receipt
    Pending,
    /// Every retained event has durable origin receipt
    Acknowledged,
    /// The verified origin has no route; automatic sends have stopped
    Orphaned,
}

/// Read-only executor event transport status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRouteStatus {
    /// Public API version
    pub api_version: u32,
    /// Task that owns the retained events
    pub task: TaskId,
    /// Fixed origin UUID from the accepted executor identity
    pub origin_machine: MachineId,
    /// Aggregate route state
    pub state: EventRouteState,
    /// Number of retained events without origin receipt
    pub pending: u64,
    /// Number of retained events with origin receipt
    pub acknowledged: u64,
    /// Durable failure reason, when orphaned
    pub reason: Option<String>,
}

/// One retained executor outbox row
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxEvent {
    /// Immutable event
    pub event: TaskEvent,
    /// Transport acknowledgement state
    pub state: OutboxState,
}

/// How long an event may wait for its origin thread before delivery gives up
///
/// Some hosts, such as T3 Code, run one Claude Code process per turn, so a
/// session can have no live process for hours between turns. After this limit
/// the event settles as failed and goes to the fallback log, which lets later
/// events for the task proceed
pub const THREAD_WAIT_LIMIT: chrono::TimeDelta = chrono::TimeDelta::days(3);

/// Origin callback result, separate from transport acknowledgement
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeliveryState {
    /// A state-only event needs no callback
    NotRequired,
    /// Callback delivery remains eligible for a future dispatcher
    PendingDelivery {
        /// Reserved command attempts
        attempts: u8,
        /// Last retryable error, if any
        last_error: Option<String>,
    },
    /// The origin thread cannot take messages now, for example its Claude Code session has no live
    /// process and cannot be woken; the dispatcher keeps retrying without counting these checks
    /// against the send budget
    AwaitingThread {
        /// Reserved command attempts, excluding refunded checks
        attempts: u8,
        /// When this event first began waiting for its origin thread
        since: DateTime<Utc>,
        /// Why the origin thread could not take a message
        reason: String,
    },
    /// Queue delivery succeeded
    Delivered {
        /// Reserved command attempts
        attempts: u8,
        /// Last failed attempt before success, if any
        last_error: Option<String>,
    },
    /// Queue delivery ended without success; the result remains visible
    DeliveryFailed {
        /// Reserved command attempts
        attempts: u8,
        /// Final error
        last_error: String,
    },
}

impl DeliveryState {
    /// Callback status of a notifying event in this state, or `None` for a
    /// state-only event, which has no callback
    #[must_use]
    pub fn callback_status(&self) -> Option<CallbackStatus> {
        match self {
            Self::NotRequired => None,
            Self::PendingDelivery { attempts: 0, .. } => Some(CallbackStatus::Pending),
            Self::PendingDelivery { .. } => Some(CallbackStatus::Sending),
            Self::AwaitingThread { .. } => Some(CallbackStatus::Waiting),
            Self::Delivered { .. } => Some(CallbackStatus::Sent),
            Self::DeliveryFailed { .. } => Some(CallbackStatus::Failed),
        }
    }

    /// Whether the dispatcher still owns this event
    #[must_use]
    pub fn is_unsettled(&self) -> bool {
        matches!(
            self,
            Self::PendingDelivery { .. } | Self::AwaitingThread { .. }
        )
    }
}

/// An event that waits for its origin thread, retained for task inspection
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WaitingInboxEvent {
    /// Per-task event sequence
    pub seq: u64,
    /// When the event began waiting
    pub since: DateTime<Utc>,
    /// When delivery gives up if the origin thread still cannot take it
    pub until: DateTime<Utc>,
    /// Why the origin thread could not take the event
    pub reason: String,
}

/// Result of one reserved origin queue attempt
pub enum DeliveryOutcome {
    /// The queue command accepted the event
    Delivered,
    /// The attempt found the origin thread unable to take messages and sent nothing that could have
    /// been delivered, so the attempt is refunded
    Deferred(String),
    /// The command failed and can be retried within the durable budget
    Retryable(String),
    /// The saved callback context or command cannot be used again
    Permanent(String),
}

/// Failure retained for origin task inspection, independent of process status
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FailedInboxEvent {
    /// Per-task event sequence
    pub seq: u64,
    /// Reserved queue-command attempts
    pub attempts: u8,
    /// Final delivery error
    pub error: String,
}

/// One retained origin inbox row
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboxEvent {
    /// Immutable event
    pub event: TaskEvent,
    /// Per-event callback delivery result
    pub delivery: DeliveryState,
}

/// Result of origin sequence validation after the transaction commits
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventAcceptance {
    /// This sequence was committed before the response, or was already present
    Acknowledged {
        /// Accepted sequence
        seq: u64,
    },
    /// The origin needs an earlier sequence first
    Expected {
        /// Next required sequence
        seq: u64,
    },
}

/// A durable event operation failed without acknowledging new content
#[derive(Debug, thiserror::Error)]
pub enum EventError {
    /// No origin route exists for this task UUID
    #[error("origin route not found: {task}")]
    RouteNotFound {
        /// Missing task UUID
        task: TaskId,
    },
    /// The origin route names another executor or origin
    #[error("cluster task owner conflict: {task}")]
    OwnerConflict {
        /// Conflicting task UUID
        task: TaskId,
    },
    /// An accepted sequence has different immutable content
    #[error("event content conflict: {task} sequence {seq}")]
    ContentConflict {
        /// Conflicting task UUID
        task: TaskId,
        /// Conflicting sequence
        seq: u64,
    },
    /// Invalid event identity or sequence
    #[error("invalid event: {message}")]
    Invalid {
        /// Validation detail
        message: String,
    },
    /// Storage or stored data failed
    #[error(transparent)]
    Storage(#[from] AppError),
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, path::PathBuf};

    use chrono::Utc;

    use super::{DeliveryState, EventPayload, TaskEvent};
    use crate::callback::{EventKind, HomebasedEvent, NextAction, WorkloadView};
    use crate::domain::{TaskId, ThreadId};
    use crate::machine::MachineId;

    #[test]
    fn awaiting_thread_delivery_state_uses_its_durable_tag() {
        let state = DeliveryState::AwaitingThread {
            attempts: 2,
            since: Utc::now(),
            reason: "origin thread is asleep".into(),
        };

        let value = serde_json::to_value(&state).unwrap();
        assert_eq!(value["type"], "awaiting_thread");
        assert_eq!(
            serde_json::from_value::<DeliveryState>(value).unwrap(),
            state
        );
    }

    #[test]
    fn callback_adds_identity_without_changing_legacy_fields() {
        let task = TaskId::new();
        let callback = HomebasedEvent {
            api_version: 1,
            event: EventKind::TaskReported,
            task,
            name: crate::domain::TaskName::parse("test task").unwrap(),
            workload: WorkloadView::Task {
                command: vec!["echo".into(), "hello".into()],
            },
            thread: ThreadId(uuid::Uuid::now_v7()),
            cwd: PathBuf::from("/tmp"),
            evidence: PathBuf::from("/tmp/evidence"),
            reports: Vec::new(),
            process: None,
            timeout_secs: None,
            next_action: NextAction::ReadReport,
            cancel_reason: None,
            waiting_on: None,
            continuation: None,
            reason: None,
        };
        let event = TaskEvent {
            task,
            seq: NonZeroU64::new(1).unwrap(),
            origin_machine: MachineId::new(),
            execution_machine: MachineId::new(),
            payload: EventPayload::Callback {
                event: Box::new(callback.clone()),
                state: None,
            },
        };
        assert!(event.payload.notification_required());
        let line = event.callback_message_line().unwrap().unwrap();
        let value: serde_json::Value =
            serde_json::from_str(line.strip_prefix("HOMEBASED_EVENT ").unwrap()).unwrap();
        assert_eq!(value["api_version"], 1);
        assert_eq!(value["event"], "TASK_REPORTED");
        assert_eq!(value["task"], serde_json::json!(task));
        assert_eq!(value["seq"], 1);
        assert_eq!(
            value["origin_machine"],
            serde_json::json!(event.origin_machine)
        );
        assert_eq!(
            value["execution_machine"],
            serde_json::json!(event.execution_machine)
        );
        assert_eq!(value["reports"], serde_json::json!(callback.reports));

        let mut unknown = serde_json::to_value(&event).unwrap();
        unknown["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<TaskEvent>(unknown).is_err());
        let mut zero = serde_json::to_value(&event).unwrap();
        zero["seq"] = serde_json::json!(0);
        assert!(serde_json::from_value::<TaskEvent>(zero).is_err());
    }
}
