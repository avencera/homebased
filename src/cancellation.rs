//! Durable cancellation identities and delivery results

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::submission::{HeldPhase, OriginRoute, RequestId, SubmissionState};

/// Exact execution identity that owns the cancellation of one task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancellationOwner {
    /// Caller retry UUID from the exact origin route
    pub request_id: RequestId,
    /// Task UUID
    pub task: TaskId,
    /// Original submission owner
    pub origin_machine: MachineId,
    /// Fixed execution owner
    pub execution_machine: MachineId,
}

/// Origin route fields that decide which owner may cancel a task
///
/// Local routes and fleet inspection summaries both reduce to this view, so
/// every cancellation entry point applies the same ownership rule
#[derive(Debug, Clone, Copy)]
pub struct CancellationRoute<'a> {
    /// Global task UUID named by the route
    pub task: TaskId,
    /// Caller retry UUID
    pub request_id: RequestId,
    /// Machine that owns the route and its callbacks
    pub origin_machine: MachineId,
    /// Fixed execution owner
    pub execution_machine: MachineId,
    /// Durable submission result
    pub submission: &'a SubmissionState,
}

impl<'a> From<&'a OriginRoute> for CancellationRoute<'a> {
    fn from(route: &'a OriginRoute) -> Self {
        Self {
            task: route.task,
            request_id: route.request,
            origin_machine: route.origin_machine,
            execution_machine: route.execution_machine,
            submission: &route.submission,
        }
    }
}

/// Typed reason that a route or owner cannot yield a cancellation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationRefusal {
    /// The route names another task or origin
    RouteMismatch,
    /// The executor rejected the task before it started
    NotStarted,
    /// The task is held on its origin, which alone can cancel it before launch
    HeldOnOrigin,
}

impl CancellationRefusal {
    /// Convert the refusal into the API error for one task
    #[must_use]
    pub fn into_error(self, task: TaskId) -> AppError {
        match self {
            Self::RouteMismatch => AppError::ClusterTaskConflict { task },
            Self::NotStarted => AppError::TaskNotStarted { task },
            Self::HeldOnOrigin => AppError::Usage {
                message: format!(
                    "task {task} is held on its origin machine until its dependencies succeed; \
                     cancel it on that machine"
                ),
            },
        }
    }
}

impl CancellationOwner {
    /// Select the cancellation owner for the route reported by `route_machine`
    ///
    /// # Errors
    ///
    /// Refuses a route for another task or origin, a rejected submission, and
    /// a route still held on its origin
    pub fn from_route(
        task: TaskId,
        route_machine: MachineId,
        route: CancellationRoute<'_>,
    ) -> Result<Self, CancellationRefusal> {
        if route.task != task || route.origin_machine != route_machine {
            return Err(CancellationRefusal::RouteMismatch);
        }
        match route.submission {
            SubmissionState::Rejected { .. }
            | SubmissionState::Held {
                phase: HeldPhase::Cancelled { .. },
            } => Err(CancellationRefusal::NotStarted),
            // the origin cancels a waiting held route itself, before this lookup
            SubmissionState::Held {
                phase: HeldPhase::Waiting,
            } => Err(CancellationRefusal::HeldOnOrigin),
            // a launching held route may already be on its executor
            SubmissionState::AcceptanceUnknown
            | SubmissionState::Accepted
            | SubmissionState::Held {
                phase: HeldPhase::Launching,
            } => Ok(Self {
                request_id: route.request_id,
                task,
                origin_machine: route.origin_machine,
                execution_machine: route.execution_machine,
            }),
        }
    }

    /// Build the intent that `requester_machine` saves and delivers to cancel `task`
    ///
    /// Any requester may save an intent for an ordinary execution
    ///
    /// # Errors
    ///
    /// Refuses an owner for another task
    pub fn request(
        self,
        requester_machine: MachineId,
        task: TaskId,
        cancellation: Uuid,
    ) -> Result<CancellationRequest, CancellationRefusal> {
        if self.task != task {
            return Err(CancellationRefusal::RouteMismatch);
        }
        Ok(CancellationRequest {
            requester_machine,
            cancellation,
            task,
            origin_machine: self.origin_machine,
            execution_machine: self.execution_machine,
            target: CancellationTarget::Execution {
                request_id: self.request_id,
            },
            delivery: CancellationDelivery::Pending,
        })
    }
}

/// Durable requester-side target for one cancellation intent
/// Durable requester-side target for one cancellation intent
///
/// The tagged form is the stored and wire shape of saved intents, so it stays
/// an enum with one variant
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CancellationTarget {
    /// Ordinary execution route proven before the intent was saved
    Execution {
        /// Caller retry UUID from the exact origin route
        request_id: RequestId,
    },
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
    /// Typed route target
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

#[cfg(test)]
mod tests {
    use super::{CancellationOwner, CancellationRefusal, CancellationRoute, CancellationTarget};
    use crate::domain::TaskId;
    use crate::machine::MachineId;
    use crate::submission::{RequestId, SubmissionState};
    use uuid::Uuid;

    fn route(
        task: TaskId,
        origin_machine: MachineId,
        submission: &SubmissionState,
    ) -> CancellationRoute<'_> {
        CancellationRoute {
            task,
            request_id: RequestId::new(),
            origin_machine,
            execution_machine: MachineId::new(),
            submission,
        }
    }

    #[test]
    fn rejected_or_mismatched_routes_are_refused() {
        let task = TaskId::new();
        let origin_machine = MachineId::new();
        let rejected = SubmissionState::Rejected {
            reason: "abandoned_before_acceptance".into(),
        };
        let accepted = SubmissionState::Accepted;

        assert_eq!(
            CancellationOwner::from_route(
                task,
                origin_machine,
                route(task, origin_machine, &rejected)
            ),
            Err(CancellationRefusal::NotStarted)
        );
        assert_eq!(
            CancellationOwner::from_route(
                TaskId::new(),
                origin_machine,
                route(task, origin_machine, &accepted)
            ),
            Err(CancellationRefusal::RouteMismatch)
        );
        assert_eq!(
            CancellationOwner::from_route(
                task,
                MachineId::new(),
                route(task, origin_machine, &accepted)
            ),
            Err(CancellationRefusal::RouteMismatch)
        );
    }

    #[test]
    fn cancellation_owner_task_mismatch_is_refused() {
        let owner = CancellationOwner {
            request_id: RequestId::new(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            execution_machine: MachineId::new(),
        };

        assert_eq!(
            owner.request(MachineId::new(), TaskId::new(), Uuid::now_v7()),
            Err(CancellationRefusal::RouteMismatch)
        );
    }

    #[test]
    fn ordinary_execution_cancellation_keeps_the_generic_path() {
        let task = TaskId::new();
        let request_id = RequestId::new();
        let origin_machine = MachineId::new();
        let execution_machine = MachineId::new();
        let cancellation = Uuid::now_v7();
        let owner = CancellationOwner {
            request_id,
            task,
            origin_machine,
            execution_machine,
        };

        let request = owner
            .request(MachineId::new(), task, cancellation)
            .expect("ordinary execution must use its generic cancellation intent");
        assert_eq!(request.task, task);
        assert_eq!(request.origin_machine, origin_machine);
        assert_eq!(request.execution_machine, execution_machine);
        assert_eq!(request.cancellation, cancellation);
        assert_eq!(request.target, CancellationTarget::Execution { request_id });
    }
}
