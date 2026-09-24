//! Versioned Fleet protocol for tasks bound to one resource action
//!
//! A supervisor on another machine owns the callback route for its release watcher
//! or return task. The resource authority owns the loan, the action, the canonical
//! command, and the only spawn. The supervisor saves its fixed-ID origin route
//! before it sends a launch, and every retry repeats the same identities

use serde::{Deserialize, Serialize};

use super::{
    ActionId, Loan, ResourceRevision, ReturnDecision, ReturnLaunch, ReturnWork,
    SupervisorActionAuthority, validate_release_watcher_identity,
};
use crate::domain::{API_VERSION, ProcessStatus, TaskId};
use crate::machine::MachineId;
use crate::spec::NormalizedSpec;
use crate::submission::{NormalizedSpecSha256, RequestId, normalized_spec_sha256};

/// Version of the resource-action request and response documents
pub const RESOURCE_ACTION_PROTOCOL_VERSION: u32 = 1;

/// Authority route that serves every resource-action operation
pub const RESOURCE_ACTION_PATH: &str = "/v1/cluster/resource-actions";

/// Origin route that proves the supervisor saved one action-bound callback route
pub const RESOURCE_ACTION_ROUTE_PROOF_PATH: &str = "/v1/cluster/origin/resource-action-routes";

/// Socket-only route on the supervisor machine that starts one resource action
pub const RESOURCE_ACTION_SUBMIT_PATH: &str = "/v1/internal/resource-actions";

/// Kind of task that one resource action binds
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceActionKind {
    /// The authority-built watcher for a release action
    ReleaseWatcher,
    /// The returning background task for a return action
    Return,
}

/// Supervisor choice that one action-bound route launches
///
/// The route keeps it so a retry after a lost reply or restart resends exactly
/// the same launch without asking the caller again
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceActionLaunch {
    /// The watcher bound to the release action for this background task
    ReleaseWatcher {
        /// Background task named by the release action
        observed_background_task: TaskId,
    },
    /// The returning background task for the return action
    Return {
        /// Typed work chosen by the supervisor
        work: ReturnWork,
    },
}

impl ResourceActionLaunch {
    /// Kind of task this choice launches
    #[must_use]
    pub const fn kind(&self) -> ResourceActionKind {
        match self {
            Self::ReleaseWatcher { .. } => ResourceActionKind::ReleaseWatcher,
            Self::Return { .. } => ResourceActionKind::Return,
        }
    }
}

/// One operation that a remote supervisor asks the authority to apply
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceActionOperation {
    /// Return the watcher identity and canonical command that the authority bound
    PrepareReleaseWatcher {
        /// Background task named by the release action
        observed_background_task: TaskId,
    },
    /// Accept the exact prepared watcher once and start it only on first insertion
    LaunchReleaseWatcher {
        /// Background task named by the release action
        observed_background_task: TaskId,
        /// Fixed identities and digest from the prepared watcher
        task: ActionTaskIdentity,
    },
    /// Derive the canonical return task for one launch decision without binding it
    PrepareReturn {
        /// Supervisor-chosen fixed identities and typed work
        launch: ReturnLaunch,
    },
    /// Bind the exact prepared return task once and start it only on first insertion
    LaunchReturn {
        /// Supervisor-chosen fixed identities and typed work
        launch: ReturnLaunch,
        /// Digest of the prepared canonical spec
        normalized_spec_sha256: NormalizedSpecSha256,
    },
    /// Close the return action without starting background work
    NoResume {
        /// Supervisor's durable decision reason
        reason: String,
    },
    /// Close a Restoring loan whose bound task ended before a confirmed start
    ResolveEndedRestore {
        /// Bound return task that ended
        task_id: TaskId,
        /// Supervisor's durable resolution reason
        reason: String,
    },
}

impl ResourceActionOperation {
    /// Kind of task this operation prepares or launches, if any
    #[must_use]
    pub fn task_kind(&self) -> Option<ResourceActionKind> {
        match self {
            Self::PrepareReleaseWatcher { .. } | Self::LaunchReleaseWatcher { .. } => {
                Some(ResourceActionKind::ReleaseWatcher)
            }
            Self::PrepareReturn { .. } | Self::LaunchReturn { .. } => {
                Some(ResourceActionKind::Return)
            }
            Self::NoResume { .. } | Self::ResolveEndedRestore { .. } => None,
        }
    }

    /// Fixed task identity that a launch operation asks the authority to accept
    #[must_use]
    pub fn launch_identity(&self) -> Option<ActionTaskIdentity> {
        match self {
            Self::LaunchReleaseWatcher { task, .. } => Some(*task),
            Self::LaunchReturn {
                launch,
                normalized_spec_sha256,
            } => Some(ActionTaskIdentity {
                request_id: launch.request_id,
                task_id: launch.task_id,
                normalized_spec_sha256: *normalized_spec_sha256,
            }),
            Self::PrepareReleaseWatcher { .. }
            | Self::PrepareReturn { .. }
            | Self::NoResume { .. }
            | Self::ResolveEndedRestore { .. } => None,
        }
    }
}

/// Fixed request and task identities of one action-bound task with its spec digest
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionTaskIdentity {
    /// Stable retry identity, distinct from the task identity
    pub request_id: RequestId,
    /// Preallocated global task identity
    pub task_id: TaskId,
    /// Digest of the canonical normalized spec
    pub normalized_spec_sha256: NormalizedSpecSha256,
}

/// Strict versioned request from a remote supervisor to the resource authority
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceActionRequest {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Resource-action document version
    pub action_protocol_version: u32,
    /// Intended resource authority
    pub destination_machine: MachineId,
    /// Supervisor machine that sent the request and owns the callback route
    pub source_machine: MachineId,
    /// Exact resource, loan, action, revision, and supervisor assignment
    pub authority: SupervisorActionAuthority,
    /// Requested operation
    pub operation: ResourceActionOperation,
}

/// Why a resource-action request is malformed before any saved state is read
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResourceActionRequestError {
    /// The API or document version is not supported
    #[error("unsupported resource-action version")]
    UnsupportedVersion,
    /// The destination is not the authority named by the action
    #[error("resource-action destination is not the named authority")]
    DestinationMismatch,
    /// The sender is not the assigned supervisor machine
    #[error("resource-action source is not the supervisor machine")]
    SourceMismatch,
    /// The supervisor runs on the authority and must use the local path
    #[error("a co-located supervisor must use the local action path")]
    CoLocatedSupervisor,
    /// An identity is nil or one UUID has two roles
    #[error("resource-action identities are invalid")]
    InvalidIdentity,
    /// A decision or resolution reason is empty
    #[error("resource-action reason must not be empty")]
    EmptyReason,
}

impl ResourceActionRequest {
    /// Build a current-version request from the supervisor machine
    #[must_use]
    pub fn new(
        protocol_version: u32,
        authority: SupervisorActionAuthority,
        operation: ResourceActionOperation,
    ) -> Self {
        Self {
            api_version: API_VERSION,
            protocol_version,
            action_protocol_version: RESOURCE_ACTION_PROTOCOL_VERSION,
            destination_machine: authority.authority_machine,
            source_machine: authority.supervisor.machine,
            authority,
            operation,
        }
    }

    /// Validate versions, route owners, and identity shape without saved state
    pub fn validate(&self) -> Result<(), ResourceActionRequestError> {
        if self.api_version != API_VERSION
            || self.action_protocol_version != RESOURCE_ACTION_PROTOCOL_VERSION
        {
            return Err(ResourceActionRequestError::UnsupportedVersion);
        }
        let authority = &self.authority;
        if self.destination_machine != authority.authority_machine {
            return Err(ResourceActionRequestError::DestinationMismatch);
        }
        if self.source_machine != authority.supervisor.machine {
            return Err(ResourceActionRequestError::SourceMismatch);
        }
        if self.source_machine == self.destination_machine {
            return Err(ResourceActionRequestError::CoLocatedSupervisor);
        }
        if self.source_machine.as_uuid().is_nil()
            || self.destination_machine.as_uuid().is_nil()
            || authority.supervisor.thread.0.is_nil()
        {
            return Err(ResourceActionRequestError::InvalidIdentity);
        }

        match &self.operation {
            ResourceActionOperation::PrepareReleaseWatcher {
                observed_background_task,
            } if observed_background_task.0.is_nil() => {
                Err(ResourceActionRequestError::InvalidIdentity)
            }
            ResourceActionOperation::LaunchReleaseWatcher {
                observed_background_task,
                task,
            } if observed_background_task.0.is_nil()
                || validate_release_watcher_identity(
                    *observed_background_task,
                    task.request_id,
                    task.task_id,
                )
                .is_err() =>
            {
                Err(ResourceActionRequestError::InvalidIdentity)
            }
            ResourceActionOperation::PrepareReturn { launch }
            | ResourceActionOperation::LaunchReturn { launch, .. }
                if !distinct_identity(launch.request_id, launch.task_id) =>
            {
                Err(ResourceActionRequestError::InvalidIdentity)
            }
            ResourceActionOperation::NoResume { reason }
            | ResourceActionOperation::ResolveEndedRestore { reason, .. }
                if reason.trim().is_empty() =>
            {
                Err(ResourceActionRequestError::EmptyReason)
            }
            ResourceActionOperation::ResolveEndedRestore { task_id, .. } if task_id.0.is_nil() => {
                Err(ResourceActionRequestError::InvalidIdentity)
            }
            _ => Ok(()),
        }
    }
}

fn distinct_identity(request_id: RequestId, task_id: TaskId) -> bool {
    !request_id.0.is_nil() && !task_id.0.is_nil() && request_id.0 != task_id.0
}

/// Canonical task that the authority derived for one pending action
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedActionTask {
    /// Stable retry identity for the task
    pub request_id: RequestId,
    /// Preallocated global task identity
    pub task_id: TaskId,
    /// Canonical normalized spec that the supervisor saves in its route
    pub spec: NormalizedSpec,
    /// Digest of `spec`
    pub normalized_spec_sha256: NormalizedSpecSha256,
}

impl PreparedActionTask {
    /// Whether the digest covers the carried spec exactly
    #[must_use]
    pub fn digest_matches(&self) -> bool {
        normalized_spec_sha256(&self.spec).is_ok_and(|digest| digest == self.normalized_spec_sha256)
    }
}

/// Exact action binding that the authority saved with one accepted task
///
/// The supervisor machine is the callback origin and the authority is the
/// execution machine. A retry must match every field
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionTaskReceipt {
    /// Kind of bound task
    pub kind: ResourceActionKind,
    /// Exact action authority that accepted the task
    pub authority: SupervisorActionAuthority,
    /// Stable retry identity for the task
    pub request_id: RequestId,
    /// Preallocated global task identity
    pub task_id: TaskId,
    /// Digest of the canonical normalized spec
    pub normalized_spec_sha256: NormalizedSpecSha256,
}

impl ActionTaskReceipt {
    /// Machine that owns the task's callback route
    #[must_use]
    pub const fn origin_machine(&self) -> MachineId {
        self.authority.supervisor.machine
    }

    /// Machine that executes the task
    #[must_use]
    pub const fn execution_machine(&self) -> MachineId {
        self.authority.authority_machine
    }
}

/// Whether one launch request inserted its task or only observed an earlier insertion
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActionTaskAcceptance {
    /// This request committed the task records, so it made the only spawn attempt
    Inserted,
    /// An exact earlier acceptance existed; the authority did not spawn again
    Existing {
        /// State retained by the task layer
        state: ProcessStatus,
    },
}

/// Definitive authority reason for refusing one resource action
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceActionRejection {
    /// The request does not come from the current supervisor assignment
    NotCurrentSupervisor,
    /// The loan does not have this pending action in the required phase
    ActionNotPending,
    /// The request names a stale resource revision
    StaleRevision {
        /// Revision named by the request
        expected: ResourceRevision,
        /// Revision saved by the authority
        actual: ResourceRevision,
    },
    /// The spec digest differs from the canonical spec that the authority derived
    SpecMismatch,
    /// A fixed request or task identity already belongs to other records
    IdentityConflict,
    /// A saved receipt holds a different request for the same action
    ConflictingRetry,
    /// The supervisor machine has no saved callback route for this task
    RouteEvidenceMissing,
    /// The supervisor machine's saved callback route names other identities or content
    RouteEvidenceMismatch,
    /// The authority cannot bind a watcher for this release action now
    WatcherUnavailable {
        /// Authority-side reason
        reason: String,
    },
    /// The typed return decision does not fit the saved action
    DecisionRejected {
        /// Authority-side reason
        reason: String,
    },
    /// The return task has not ended with proven process release
    RestoreNotResolvable {
        /// Authority-side reason
        reason: String,
    },
}

/// Typed authority result for one resource-action request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceActionOutcome {
    /// The canonical task for the requested launch
    Prepared {
        /// Identities, spec, and digest to save in the origin route
        task: PreparedActionTask,
    },
    /// The authority accepted the fixed task for the action
    Accepted {
        /// Saved exact action binding
        receipt: ActionTaskReceipt,
        /// Whether this request inserted the task
        acceptance: ActionTaskAcceptance,
    },
    /// The decision closed the loan without another task
    Closed {
        /// Closed loan with its retained return context
        loan: Loan,
        /// Resource revision committed with the closure
        state_revision: ResourceRevision,
    },
    /// The authority refused the request and wrote nothing
    Rejected {
        /// Definitive reason
        reason: ResourceActionRejection,
    },
}

/// Strict versioned response from the resource authority
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceActionResponse {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Resource-action document version
    pub action_protocol_version: u32,
    /// Authority that handled the request
    pub destination_machine: MachineId,
    /// Action named by the request
    pub action_id: ActionId,
    /// Typed result
    pub outcome: ResourceActionOutcome,
}

impl ResourceActionResponse {
    /// Build the current-version response to one validated request
    #[must_use]
    pub fn new(request: &ResourceActionRequest, outcome: ResourceActionOutcome) -> Self {
        Self {
            api_version: API_VERSION,
            protocol_version: request.protocol_version,
            action_protocol_version: RESOURCE_ACTION_PROTOCOL_VERSION,
            destination_machine: request.destination_machine,
            action_id: request.authority.action_id,
            outcome,
        }
    }
}

/// Supervisor choice sent to its own daemon for one pending resource action
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceActionChoice {
    /// Launch the watcher that the authority bound to a release action
    ReleaseWatcher {
        /// Background task named by the release notice
        observed_background_task: TaskId,
    },
    /// Apply one return decision
    Return {
        /// No-resume or fixed-identity launch decision
        decision: ReturnDecision,
    },
    /// Close a Restoring loan whose bound task ended before a confirmed start
    ResolveEndedRestore {
        /// Bound return task that ended
        task_id: TaskId,
        /// Supervisor's durable resolution reason
        reason: String,
    },
}

/// Socket request from the supervisor thread's machine to its own daemon
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceActionSubmitRequest {
    /// Public API schema version
    pub api_version: u32,
    /// Exact action authority from the supervisor notice
    pub authority: SupervisorActionAuthority,
    /// Supervisor choice
    pub choice: ResourceActionChoice,
}

/// Durable result of one supervisor-side resource action submission
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceActionSubmitOutcome {
    /// The authority accepted the bound task, and this machine owns its callback route
    Accepted {
        /// Exact receipt saved by the authority
        receipt: ActionTaskReceipt,
        /// Last task state learned by this machine, if any
        last_execution_state: Option<ProcessStatus>,
    },
    /// This machine is the authority, and it bound the return task with its own callback route
    ///
    /// No remote receipt exists for this task: the route committed with the task
    /// records in the authority's return transaction
    LocalReturnAccepted {
        /// Exact action authority and fixed identities of the bound task
        receipt: LocalReturnReceipt,
        /// Whether this request inserted the task or observed the earlier binding
        acceptance: LocalReturnAcceptance,
    },
    /// The decision closed the loan without another task
    Closed {
        /// Closed loan with its retained return context
        loan: Loan,
        /// Resource revision committed with the closure
        state_revision: ResourceRevision,
    },
    /// The authority refused the action and wrote nothing
    Rejected {
        /// Definitive reason
        reason: ResourceActionRejection,
    },
}

/// Exact binding of one return task on the machine that is both supervisor and authority
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalReturnReceipt {
    /// Exact action authority that bound the task
    pub authority: SupervisorActionAuthority,
    /// Stable retry identity for the task
    pub request_id: RequestId,
    /// Preallocated global task identity
    pub task_id: TaskId,
}

/// Whether one co-located return decision inserted its task or found the earlier binding
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LocalReturnAcceptance {
    /// This request committed the task records and made the only spawn attempt
    Inserted {
        /// Restoring loan that keeps the resource reserved
        loan: Box<Loan>,
        /// Resource revision committed with the binding
        state_revision: ResourceRevision,
    },
    /// An exact earlier decision bound the task; nothing was spawned again
    Existing {
        /// State retained by the task layer
        state: ProcessStatus,
    },
}

/// Socket response for one supervisor-side resource action submission
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceActionSubmitResponse {
    /// Public API schema version
    pub api_version: u32,
    /// Durable result
    pub outcome: ResourceActionSubmitOutcome,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        ActionTaskIdentity, PreparedActionTask, RESOURCE_ACTION_PROTOCOL_VERSION,
        ResourceActionKind, ResourceActionOperation, ResourceActionRequest,
        ResourceActionRequestError,
    };
    use crate::domain::{TaskId, ThreadId};
    use crate::machine::MachineId;
    use crate::resource::{
        ActionId, AssignmentRevision, LoanId, ResourceId, ResourceRevision, ReturnLaunch,
        ReturnWork, SupervisorActionAuthority, SupervisorAddress,
    };
    use crate::spec::NormalizedSpec;
    use crate::submission::{RequestId, normalized_spec_sha256};

    fn authority() -> SupervisorActionAuthority {
        SupervisorActionAuthority {
            authority_machine: MachineId::new(),
            resource_id: ResourceId::new(),
            loan_id: LoanId::new(),
            action_id: ActionId::new(),
            expected_state_revision: ResourceRevision::new(3),
            supervisor: SupervisorAddress {
                machine: MachineId::new(),
                thread: ThreadId(Uuid::now_v7()),
            },
            assignment_revision: AssignmentRevision::new(1),
        }
    }

    fn launch_watcher(authority: SupervisorActionAuthority) -> ResourceActionRequest {
        ResourceActionRequest::new(
            1,
            authority,
            ResourceActionOperation::LaunchReleaseWatcher {
                observed_background_task: TaskId::new(),
                task: ActionTaskIdentity {
                    request_id: RequestId::new(),
                    task_id: TaskId::new(),
                    normalized_spec_sha256: serde_json::from_value(json!("0".repeat(64))).unwrap(),
                },
            },
        )
    }

    #[test]
    fn request_rejects_unknown_fields_and_keeps_its_wire_shape() {
        let request = launch_watcher(authority());
        let mut wire = serde_json::to_value(&request).unwrap();
        assert_eq!(wire["operation"]["type"], "launch_release_watcher");
        serde_json::from_value::<ResourceActionRequest>(wire.clone()).unwrap();

        wire["operation"]["command"] = json!(["/bin/sh", "-c", "anything"]);
        assert!(serde_json::from_value::<ResourceActionRequest>(wire.clone()).is_err());
        wire["operation"].as_object_mut().unwrap().remove("command");
        wire["callback"] = json!({"cwd": "/tmp"});
        assert!(serde_json::from_value::<ResourceActionRequest>(wire).is_err());
    }

    #[test]
    fn request_validation_checks_route_owners_and_identities() {
        let request = launch_watcher(authority());
        assert_eq!(request.validate(), Ok(()));

        let mut wrong_destination = request.clone();
        wrong_destination.destination_machine = MachineId::new();
        assert_eq!(
            wrong_destination.validate(),
            Err(ResourceActionRequestError::DestinationMismatch)
        );

        let mut wrong_source = request.clone();
        wrong_source.source_machine = MachineId::new();
        assert_eq!(
            wrong_source.validate(),
            Err(ResourceActionRequestError::SourceMismatch)
        );

        let mut co_located = authority();
        co_located.supervisor.machine = co_located.authority_machine;
        assert_eq!(
            launch_watcher(co_located).validate(),
            Err(ResourceActionRequestError::CoLocatedSupervisor)
        );

        let mut reused = request.clone();
        let ResourceActionOperation::LaunchReleaseWatcher { task, .. } = &mut reused.operation
        else {
            unreachable!();
        };
        task.request_id = RequestId(task.task_id.0);
        assert_eq!(
            reused.validate(),
            Err(ResourceActionRequestError::InvalidIdentity)
        );

        let mut version = request;
        version.action_protocol_version = RESOURCE_ACTION_PROTOCOL_VERSION + 1;
        assert_eq!(
            version.validate(),
            Err(ResourceActionRequestError::UnsupportedVersion)
        );

        let no_resume = ResourceActionRequest::new(
            1,
            authority(),
            ResourceActionOperation::NoResume { reason: " ".into() },
        );
        assert_eq!(
            no_resume.validate(),
            Err(ResourceActionRequestError::EmptyReason)
        );
    }

    #[test]
    fn launch_identity_comes_only_from_launch_operations() {
        let spec: NormalizedSpec = serde_json::from_value(json!({
            "api_version": 1,
            "thread": ThreadId(Uuid::now_v7()),
            "name": "return",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["/bin/echo", "resume"] }
        }))
        .unwrap();
        let launch = ReturnLaunch {
            request_id: RequestId::new(),
            task_id: TaskId::new(),
            work: ReturnWork::NewBackgroundWork {
                spec: crate::resource::CommandSpec::try_from(spec.clone()).unwrap(),
            },
        };
        let digest = normalized_spec_sha256(&spec).unwrap();
        let operation = ResourceActionOperation::LaunchReturn {
            launch: launch.clone(),
            normalized_spec_sha256: digest,
        };
        assert_eq!(
            operation.launch_identity(),
            Some(ActionTaskIdentity {
                request_id: launch.request_id,
                task_id: launch.task_id,
                normalized_spec_sha256: digest,
            })
        );
        assert_eq!(operation.task_kind(), Some(ResourceActionKind::Return));
        assert!(
            ResourceActionOperation::PrepareReturn { launch }
                .launch_identity()
                .is_none()
        );

        let prepared = PreparedActionTask {
            request_id: RequestId::new(),
            task_id: TaskId::new(),
            spec: spec.clone(),
            normalized_spec_sha256: digest,
        };
        assert!(prepared.digest_matches());
        let mut changed = prepared;
        changed.spec.name = crate::domain::TaskName::parse("other").unwrap();
        assert!(!changed.digest_matches());
    }
}
