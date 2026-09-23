//! Durable cancellation identities and delivery results

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{ProcessStatus, TaskId};
use crate::machine::MachineId;

/// One caller-owned cancellation identity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancellationRequest {
    /// Machine that accepted the local request
    pub requester_machine: MachineId,
    /// Stable identity used for retries
    pub cancellation: Uuid,
    /// Task to stop or prevent
    pub task: TaskId,
    /// Original submission owner, needed for a pre-acceptance tombstone
    pub origin_machine: MachineId,
    /// Fixed execution owner
    pub execution_machine: MachineId,
    /// Durable delivery state
    pub delivery: CancellationDelivery,
}

/// Caller-owned delivery state, independent of process state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum CancellationDelivery {
    /// The executor has not acknowledged durable receipt
    Pending,
    /// The executor acknowledged durable receipt
    Delivered { result: ExecutorCancelState },
}

/// Executor-owned state of one received cancellation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutorCancelState {
    /// An accepted task still needs the normal termination path
    PendingApplication,
    /// The request reached an accepted task
    Applied { status: ProcessStatus },
    /// A previously terminal task kept its actual outcome
    AlreadyTerminal { status: ProcessStatus },
    /// A tombstone prevents any process from starting
    PreventedBeforeStart { reason: String },
    /// Another definitive tombstone already owns the UUID
    Rejected { reason: String },
}

/// Durable executor receipt returned by the versioned cluster route
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancellationReceipt {
    /// Request identity, including the fixed owners
    pub request: CancellationRequestIdentity,
    /// Executor application state
    pub state: ExecutorCancelState,
}

/// Immutable fields shared by a request and its receipt
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancellationRequestIdentity {
    /// Machine that accepted the local request
    pub requester_machine: MachineId,
    /// Stable cancellation UUID
    pub cancellation: Uuid,
    /// Task UUID
    pub task: TaskId,
    /// Original submission owner
    pub origin_machine: MachineId,
    /// Fixed execution owner
    pub execution_machine: MachineId,
}

impl CancellationRequest {
    /// Extract immutable fields for a versioned cluster request
    #[must_use]
    pub fn identity(&self) -> CancellationRequestIdentity {
        CancellationRequestIdentity {
            requester_machine: self.requester_machine,
            cancellation: self.cancellation,
            task: self.task,
            origin_machine: self.origin_machine,
            execution_machine: self.execution_machine,
        }
    }
}
