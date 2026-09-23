//! Durable cancellation identities and delivery results

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{ProcessStatus, TaskId};
use crate::machine::MachineId;
use crate::resource::ResourceId;
use crate::submission::{RequestId, ResourceCancellationReceipt, ResourceRoutePhase};

/// Typed owner of one cancellation target
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationOwner {
    /// An ordinary executor-owned task
    Execution(ExecutionCancellationTarget),
    /// A task that belongs to a resource authority
    Resource(ResourceCancellationTarget),
}

/// Exact ordinary execution identity selected for cancellation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionCancellationTarget {
    /// Caller retry UUID from the exact origin route, absent on older peers
    pub request_id: Option<RequestId>,
    /// Task UUID
    pub task: TaskId,
    /// Original submission owner
    pub origin_machine: MachineId,
    /// Fixed execution owner
    pub execution_machine: MachineId,
}

/// Exact resource route identity selected for cancellation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceCancellationTarget {
    /// Authority request UUID
    pub request_id: RequestId,
    /// Preallocated global task UUID
    pub task_id: TaskId,
    /// Resource whose authority owns the request
    pub resource_id: ResourceId,
    /// Machine that owns the origin route
    pub origin_machine: MachineId,
    /// Machine that owns the resource and queue
    pub authority_machine: MachineId,
    /// Durable phase retained by the origin route
    pub phase: ResourceRoutePhase,
}

/// Immutable identity sent to a resource authority for cancellation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCancellationRequestIdentity {
    /// Origin machine that persisted this cancellation intent
    pub requester_machine: MachineId,
    /// Stable cancellation UUID
    pub cancellation: Uuid,
    /// Authority request UUID
    pub request: RequestId,
    /// Preallocated global task UUID
    pub task: TaskId,
    /// Machine that owns the origin route
    pub origin_machine: MachineId,
    /// Fixed resource authority and execution owner
    pub authority_machine: MachineId,
    /// Resource whose queue owns the request
    pub resource: ResourceId,
    /// Resource route phase captured when the intent was persisted
    pub target_phase: ResourceRoutePhase,
}

/// Durable requester-side target for one cancellation intent
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CancellationTarget {
    /// Older saved intents did not retain their typed origin route
    #[default]
    LegacyExecution,
    /// Ordinary execution route proven before the intent was saved
    Execution {
        /// Caller retry UUID from the exact origin route, absent for old routes
        request_id: Option<RequestId>,
    },
    /// Resource request that must use authority-owned cancellation
    Resource(ResourceCancellationTarget),
}

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
    /// Typed route target, absent on older saved intents
    #[serde(default)]
    pub target: CancellationTarget,
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
    /// The resource authority returned a durable typed cancellation receipt
    ResourceDelivered {
        /// Resource request outcome, separate from executor process state
        result: ResourceCancellationReceipt,
    },
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

    /// Extract the exact authority request identity from a resource intent
    #[must_use]
    pub fn resource_identity(&self) -> Option<ResourceCancellationRequestIdentity> {
        let CancellationTarget::Resource(target) = &self.target else {
            return None;
        };
        Some(ResourceCancellationRequestIdentity {
            requester_machine: self.requester_machine,
            cancellation: self.cancellation,
            request: target.request_id,
            task: target.task_id,
            origin_machine: target.origin_machine,
            authority_machine: target.authority_machine,
            resource: target.resource_id,
            target_phase: target.phase.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_executor_intents_decode_as_unverified_legacy_targets() {
        let value = serde_json::json!({
            "requester_machine": MachineId::new(),
            "cancellation": Uuid::now_v7(),
            "task": TaskId::new(),
            "origin_machine": MachineId::new(),
            "execution_machine": MachineId::new(),
            "delivery": { "state": "pending" }
        });

        let request: CancellationRequest = serde_json::from_value(value).unwrap();
        assert_eq!(request.target, CancellationTarget::LegacyExecution);
    }
}
