//! Durable cancellation identities and delivery results

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::ResourceId;
use crate::submission::{
    OriginRoute, RequestId, ResourceActionRoutePhase, ResourceBackgroundRoutePhase,
    ResourceCancellationReceipt, ResourceRoutePhase, SubmissionState,
};

/// Typed owner of one cancellation target
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationOwner {
    /// An ordinary executor-owned task
    Execution(ExecutionCancellationTarget),
    /// A task that belongs to a resource authority
    Resource(ResourceCancellationTarget),
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
    /// Fixed execution owner, or the resource authority for resource routes
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
    /// The executor or resource authority rejected the task before it started
    NotStarted,
    /// An action-bound or background launch has no accepted task yet
    ///
    /// Only the launch's own retry may resolve it; a cancellation could fence
    /// the fixed identity before the authority accepts it
    LaunchUnresolved,
}

impl CancellationRefusal {
    /// Convert the refusal into the API error for one task
    #[must_use]
    pub fn into_error(self, task: TaskId) -> AppError {
        match self {
            Self::RouteMismatch | Self::LaunchUnresolved => AppError::ClusterTaskConflict { task },
            Self::NotStarted => AppError::TaskNotStarted { task },
        }
    }
}

/// Next step for one requester after ownership is known
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationPlan {
    /// The retained route already proves cancellation before launch
    AlreadyCancelled,
    /// Save this requester-owned intent and deliver it
    Deliver(Box<CancellationRequest>),
    /// The resource route's origin owns the intent, so ask it to save one
    ForwardToOrigin(ResourceCancellationTarget),
}

impl CancellationOwner {
    /// Select the cancellation owner for the route reported by `route_machine`
    ///
    /// A resource route stays resource-owned in every phase, because only its
    /// origin may save the intent; [`Self::plan`] picks the delivery path from
    /// the retained phase
    ///
    /// # Errors
    ///
    /// Refuses a route for another task or origin, a rejected submission, and
    /// an action-bound or background launch that the authority has not
    /// accepted
    pub fn from_route(
        task: TaskId,
        route_machine: MachineId,
        route: CancellationRoute<'_>,
    ) -> Result<Self, CancellationRefusal> {
        if route.task != task || route.origin_machine != route_machine {
            return Err(CancellationRefusal::RouteMismatch);
        }
        match route.submission {
            SubmissionState::Rejected { .. } => Err(CancellationRefusal::NotStarted),
            SubmissionState::ResourceAction {
                phase: ResourceActionRoutePhase::AcceptanceUnknown,
                ..
            }
            | SubmissionState::ResourceAction {
                phase: ResourceActionRoutePhase::Rejected { .. },
                ..
            }
            | SubmissionState::ResourceBackground {
                phase: ResourceBackgroundRoutePhase::AcceptanceUnknown,
                ..
            }
            | SubmissionState::ResourceBackground {
                phase: ResourceBackgroundRoutePhase::Rejected { .. },
                ..
            } => Err(CancellationRefusal::LaunchUnresolved),
            SubmissionState::Resource { resource, phase } => {
                Ok(Self::Resource(ResourceCancellationTarget {
                    request_id: route.request_id,
                    task_id: task,
                    resource_id: *resource,
                    origin_machine: route.origin_machine,
                    authority_machine: route.execution_machine,
                    phase: phase.clone(),
                }))
            }
            SubmissionState::AcceptanceUnknown
            | SubmissionState::Accepted
            | SubmissionState::ResourceAction {
                phase: ResourceActionRoutePhase::Accepted,
                ..
            }
            | SubmissionState::ResourceBackground {
                phase: ResourceBackgroundRoutePhase::Accepted,
                ..
            } => Ok(Self::Execution(ExecutionCancellationTarget {
                request_id: route.request_id,
                task,
                origin_machine: route.origin_machine,
                execution_machine: route.execution_machine,
            })),
        }
    }

    /// Machines that may retain the origin route and the execution
    #[must_use]
    pub const fn machines(&self) -> (MachineId, MachineId) {
        match self {
            Self::Execution(target) => (target.origin_machine, target.execution_machine),
            Self::Resource(target) => (target.origin_machine, target.authority_machine),
        }
    }

    /// Decide how `requester_machine` cancels `task` under this owner
    ///
    /// Ordinary execution accepts an intent from any requester. A resource
    /// route accepts an intent only from its origin: before activation the
    /// authority cancels the queued request, and after activation the
    /// authority's executor cancels the task on the ordinary path
    ///
    /// # Errors
    ///
    /// Refuses an owner for another task and a resource route that the
    /// authority rejected
    pub fn plan(
        self,
        requester_machine: MachineId,
        task: TaskId,
        cancellation: Uuid,
    ) -> Result<CancellationPlan, CancellationRefusal> {
        let target = match self {
            Self::Execution(target) if target.task == task => target,
            Self::Resource(target) if target.task_id == task => {
                return plan_resource(target, requester_machine, cancellation);
            }
            Self::Execution(_) | Self::Resource(_) => {
                return Err(CancellationRefusal::RouteMismatch);
            }
        };
        Ok(CancellationPlan::Deliver(Box::new(CancellationRequest {
            requester_machine,
            cancellation,
            task,
            origin_machine: target.origin_machine,
            execution_machine: target.execution_machine,
            target: CancellationTarget::Execution {
                request_id: target.request_id,
            },
            delivery: CancellationDelivery::Pending,
        })))
    }
}

fn plan_resource(
    target: ResourceCancellationTarget,
    requester_machine: MachineId,
    cancellation: Uuid,
) -> Result<CancellationPlan, CancellationRefusal> {
    if target.origin_machine != requester_machine {
        return Ok(CancellationPlan::ForwardToOrigin(target));
    }
    let request_target = match &target.phase {
        ResourceRoutePhase::CancelledBeforeLaunch => return Ok(CancellationPlan::AlreadyCancelled),
        ResourceRoutePhase::Rejected { .. } => return Err(CancellationRefusal::NotStarted),
        ResourceRoutePhase::Activated => CancellationTarget::Execution {
            request_id: target.request_id,
        },
        ResourceRoutePhase::AcceptanceUnknown | ResourceRoutePhase::Waiting => {
            CancellationTarget::Resource(target.clone())
        }
    };
    Ok(CancellationPlan::Deliver(Box::new(CancellationRequest {
        requester_machine,
        cancellation,
        task: target.task_id,
        origin_machine: target.origin_machine,
        execution_machine: target.authority_machine,
        target: request_target,
        delivery: CancellationDelivery::Pending,
    })))
}

/// Exact ordinary execution identity selected for cancellation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionCancellationTarget {
    /// Caller retry UUID from the exact origin route
    pub request_id: RequestId,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CancellationTarget {
    /// Ordinary execution route proven before the intent was saved
    Execution {
        /// Caller retry UUID from the exact origin route
        request_id: RequestId,
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
    use super::{
        CancellationOwner, CancellationPlan, CancellationRefusal, CancellationRoute,
        CancellationTarget, ExecutionCancellationTarget, ResourceCancellationTarget,
    };
    use crate::domain::TaskId;
    use crate::machine::MachineId;
    use crate::resource::ResourceId;
    use crate::submission::{RequestId, ResourceRoutePhase, SubmissionState};
    use uuid::Uuid;

    fn resource_owner(
        task: TaskId,
        origin_machine: MachineId,
        authority_machine: MachineId,
        phase: ResourceRoutePhase,
    ) -> (RequestId, CancellationOwner) {
        let request_id = RequestId::new();
        let owner = CancellationOwner::Resource(ResourceCancellationTarget {
            request_id,
            task_id: task,
            resource_id: ResourceId::new(),
            origin_machine,
            authority_machine,
            phase,
        });
        (request_id, owner)
    }

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
    fn activated_resource_route_stays_origin_owned() {
        let task = TaskId::new();
        let origin_machine = MachineId::new();
        let submission = SubmissionState::Resource {
            resource: ResourceId::new(),
            phase: ResourceRoutePhase::Activated,
        };

        let owner = CancellationOwner::from_route(
            task,
            origin_machine,
            route(task, origin_machine, &submission),
        )
        .unwrap();
        let CancellationOwner::Resource(target) = owner.clone() else {
            panic!("an activated resource route must keep its origin-owned target");
        };
        assert!(matches!(
            owner.plan(MachineId::new(), task, Uuid::now_v7()),
            Ok(CancellationPlan::ForwardToOrigin(forwarded)) if forwarded == target
        ));
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
    fn resource_cancellation_uses_the_typed_path_before_activation() {
        let task = TaskId::new();
        for phase in [
            ResourceRoutePhase::AcceptanceUnknown,
            ResourceRoutePhase::Waiting,
        ] {
            let origin_machine = MachineId::new();
            let (_, owner) = resource_owner(task, origin_machine, MachineId::new(), phase);

            let Ok(CancellationPlan::Deliver(request)) =
                owner.plan(origin_machine, task, Uuid::now_v7())
            else {
                panic!("a pre-activation resource request needs resource cancellation");
            };
            assert!(matches!(request.target, CancellationTarget::Resource(_)));
        }
    }

    #[test]
    fn activated_resource_cancellation_uses_the_executor_task_path() {
        let task = TaskId::new();
        let origin_machine = MachineId::new();
        let authority_machine = MachineId::new();
        let (request_id, owner) = resource_owner(
            task,
            origin_machine,
            authority_machine,
            ResourceRoutePhase::Activated,
        );

        let Ok(CancellationPlan::Deliver(request)) =
            owner.plan(origin_machine, task, Uuid::now_v7())
        else {
            panic!("activated resource work must use ordinary execution cancellation");
        };
        assert_eq!(request.execution_machine, authority_machine);
        assert_eq!(request.target, CancellationTarget::Execution { request_id });
    }

    #[test]
    fn settled_resource_routes_do_not_build_a_cancellation_request() {
        let task = TaskId::new();
        let origin_machine = MachineId::new();
        let (_, rejected) = resource_owner(
            task,
            origin_machine,
            MachineId::new(),
            ResourceRoutePhase::Rejected {
                reason: "resource request rejected".into(),
            },
        );
        let (_, cancelled) = resource_owner(
            task,
            origin_machine,
            MachineId::new(),
            ResourceRoutePhase::CancelledBeforeLaunch,
        );

        assert_eq!(
            rejected.plan(origin_machine, task, Uuid::now_v7()),
            Err(CancellationRefusal::NotStarted)
        );
        assert_eq!(
            cancelled.plan(origin_machine, task, Uuid::now_v7()),
            Ok(CancellationPlan::AlreadyCancelled)
        );
    }

    #[test]
    fn cancellation_owner_task_mismatch_is_refused() {
        let task = TaskId::new();
        let (_, owner) = resource_owner(
            task,
            MachineId::new(),
            MachineId::new(),
            ResourceRoutePhase::Waiting,
        );

        assert_eq!(
            owner.plan(MachineId::new(), TaskId::new(), Uuid::now_v7()),
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
        let owner = CancellationOwner::Execution(ExecutionCancellationTarget {
            request_id,
            task,
            origin_machine,
            execution_machine,
        });

        let Ok(CancellationPlan::Deliver(request)) =
            owner.plan(MachineId::new(), task, cancellation)
        else {
            panic!("ordinary execution must use its generic cancellation intent");
        };
        assert_eq!(request.task, task);
        assert_eq!(request.origin_machine, origin_machine);
        assert_eq!(request.execution_machine, execution_machine);
        assert_eq!(request.cancellation, cancellation);
        assert_eq!(request.target, CancellationTarget::Execution { request_id });
    }
}
