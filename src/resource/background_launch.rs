//! Versioned Fleet protocol for a first background launch from a remote supervisor
//!
//! A first background launch has no loan or action, so it cannot use the
//! action-bound protocol. The supervisor machine owns the callback route and
//! saves it with a fixed request, task, spec, and supervisor assignment before
//! it sends the launch. The resource authority owns the GPU, the task row, and
//! the only spawn. The same versioned document binds the trainer attempt of the
//! running task, which the authority verifies from its own runtime evidence

use serde::{Deserialize, Serialize};

use super::api::ResourceBackgroundSubmitOutcome;
use super::trainer_publication::AttemptBinding;
use super::{
    AssignmentRevision, LoanId, Resource, ResourceId, ResourceRevision, SupervisorAddress,
};
use crate::domain::{API_VERSION, ProcessStatus, TaskId};
use crate::machine::MachineId;
use crate::spec::NormalizedSpec;
use crate::submission::{NormalizedSpecSha256, RequestId, normalized_spec_sha256};

/// Version of the remote background request and response documents
pub const RESOURCE_BACKGROUND_PROTOCOL_VERSION: u32 = 1;

/// Authority route that serves every remote background operation
pub const RESOURCE_BACKGROUND_PATH: &str = "/v1/cluster/resource-background";

/// Origin route that proves the supervisor saved one background launch route
pub const RESOURCE_BACKGROUND_ROUTE_PROOF_PATH: &str =
    "/v1/cluster/origin/resource-background-routes";

/// Supervisor assignment that one remote background operation acts for
///
/// The authority accepts an operation only while the resource still names this
/// exact supervisor machine, thread, and assignment revision
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackgroundSupervisorAssignment {
    /// Fixed resource authority and execution machine
    pub authority_machine: MachineId,
    /// Resource whose background slot the operation uses
    pub resource_id: ResourceId,
    /// Assigned supervisor machine and thread
    pub supervisor: SupervisorAddress,
    /// Supervisor assignment revision
    pub assignment_revision: AssignmentRevision,
}

impl BackgroundSupervisorAssignment {
    /// Read the current assignment from one resource
    #[must_use]
    pub fn of(resource: &Resource) -> Self {
        Self {
            authority_machine: resource.authority_machine(),
            resource_id: resource.id,
            supervisor: resource.supervisor,
            assignment_revision: resource.assignment_revision,
        }
    }

    /// Whether the resource still names this exact assignment
    #[must_use]
    pub fn is_current(&self, resource: &Resource) -> bool {
        *self == Self::of(resource)
    }
}

/// Fixed binding that one remote first background launch saves on both machines
///
/// The expected revision is the resource revision that the supervisor machine
/// read before it saved the route. The authority refuses a launch after any
/// later resource transition
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackgroundLaunchBinding {
    /// Supervisor assignment that chose the launch
    pub assignment: BackgroundSupervisorAssignment,
    /// Resource revision observed before the route was saved
    pub expected_state_revision: ResourceRevision,
}

impl BackgroundLaunchBinding {
    /// Machine that owns the task's callback route
    #[must_use]
    pub const fn origin_machine(&self) -> MachineId {
        self.assignment.supervisor.machine
    }

    /// Machine that executes the task
    #[must_use]
    pub const fn execution_machine(&self) -> MachineId {
        self.assignment.authority_machine
    }
}

/// Exact binding that the authority saved with one accepted remote launch
///
/// A retry must match every field
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteBackgroundLaunchReceipt {
    /// Supervisor assignment and resource revision that the launch bound
    pub binding: BackgroundLaunchBinding,
    /// Stable retry identity, distinct from the task identity
    pub request_id: RequestId,
    /// Preallocated global task identity
    pub task_id: TaskId,
    /// Digest of the full normalized spec
    pub normalized_spec_sha256: NormalizedSpecSha256,
}

/// One operation that a remote supervisor asks the authority to apply
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceBackgroundOperation {
    /// Accept the exact saved launch once and start it only on first insertion
    Launch {
        /// Resource revision observed before the route was saved
        expected_state_revision: ResourceRevision,
        /// Stable retry identity
        request_id: RequestId,
        /// Preallocated global task identity
        task_id: TaskId,
        /// Full normalized trainer spec saved in the route
        spec: NormalizedSpec,
        /// Digest of `spec`
        normalized_spec_sha256: NormalizedSpecSha256,
    },
    /// Bind the named trainer attempt to the running registered background task
    ///
    /// The request names only the attempt identity. The authority reads the
    /// attempt request and probes the held ownership lock itself
    BindTrainerAttempt {
        /// Registered background task
        task_id: TaskId,
        /// Trainer identity of the running attempt
        attempt_binding: AttemptBinding,
    },
}

/// Strict versioned request from a remote supervisor to the resource authority
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceBackgroundRequest {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Remote background document version
    pub background_protocol_version: u32,
    /// Intended resource authority
    pub destination_machine: MachineId,
    /// Supervisor machine that sent the request and owns the callback route
    pub source_machine: MachineId,
    /// Exact resource and supervisor assignment
    pub assignment: BackgroundSupervisorAssignment,
    /// Requested operation
    pub operation: ResourceBackgroundOperation,
}

/// Why a remote background request is malformed before any saved state is read
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResourceBackgroundRequestError {
    /// The API or document version is not supported
    #[error("unsupported remote background version")]
    UnsupportedVersion,
    /// The destination is not the authority named by the assignment
    #[error("remote background destination is not the named authority")]
    DestinationMismatch,
    /// The sender is not the assigned supervisor machine
    #[error("remote background source is not the supervisor machine")]
    SourceMismatch,
    /// The supervisor runs on the authority and must use the local path
    #[error("a co-located supervisor must use the local background path")]
    CoLocatedSupervisor,
    /// An identity is nil or one UUID has two roles
    #[error("remote background identities are invalid")]
    InvalidIdentity,
    /// The spec does not fit a remote background launch or its digest
    #[error("remote background spec is invalid")]
    InvalidSpec,
}

impl ResourceBackgroundRequest {
    /// Build a current-version request from the supervisor machine
    #[must_use]
    pub fn new(
        protocol_version: u32,
        assignment: BackgroundSupervisorAssignment,
        operation: ResourceBackgroundOperation,
    ) -> Self {
        Self {
            api_version: API_VERSION,
            protocol_version,
            background_protocol_version: RESOURCE_BACKGROUND_PROTOCOL_VERSION,
            destination_machine: assignment.authority_machine,
            source_machine: assignment.supervisor.machine,
            assignment,
            operation,
        }
    }

    /// Validate versions, route owners, identity shape, and spec digest without saved state
    pub fn validate(&self) -> Result<(), ResourceBackgroundRequestError> {
        use ResourceBackgroundRequestError as Error;

        if self.api_version != API_VERSION
            || self.background_protocol_version != RESOURCE_BACKGROUND_PROTOCOL_VERSION
        {
            return Err(Error::UnsupportedVersion);
        }
        let assignment = &self.assignment;
        if self.destination_machine != assignment.authority_machine {
            return Err(Error::DestinationMismatch);
        }
        if self.source_machine != assignment.supervisor.machine {
            return Err(Error::SourceMismatch);
        }
        if self.source_machine == self.destination_machine {
            return Err(Error::CoLocatedSupervisor);
        }
        if self.source_machine.as_uuid().is_nil()
            || self.destination_machine.as_uuid().is_nil()
            || assignment.supervisor.thread.0.is_nil()
        {
            return Err(Error::InvalidIdentity);
        }

        match &self.operation {
            ResourceBackgroundOperation::Launch {
                request_id,
                task_id,
                spec,
                normalized_spec_sha256: digest,
                ..
            } => {
                if request_id.0.is_nil() || task_id.0.is_nil() || request_id.0 == task_id.0 {
                    return Err(Error::InvalidIdentity);
                }
                let matches = normalized_spec_sha256(spec).is_ok_and(|saved| saved == *digest);
                if !matches
                    || spec.machine.is_some()
                    || spec.thread != assignment.supervisor.thread
                    || !matches!(spec.workload, crate::spec::NormalizedWorkload::Task(_))
                {
                    return Err(Error::InvalidSpec);
                }
                Ok(())
            }
            ResourceBackgroundOperation::BindTrainerAttempt { task_id, .. }
                if task_id.0.is_nil() =>
            {
                Err(Error::InvalidIdentity)
            }
            ResourceBackgroundOperation::BindTrainerAttempt { .. } => Ok(()),
        }
    }

    /// Fixed launch receipt that a launch operation asks the authority to accept
    #[must_use]
    pub fn launch_receipt(&self) -> Option<RemoteBackgroundLaunchReceipt> {
        let ResourceBackgroundOperation::Launch {
            expected_state_revision,
            request_id,
            task_id,
            normalized_spec_sha256,
            ..
        } = &self.operation
        else {
            return None;
        };
        Some(RemoteBackgroundLaunchReceipt {
            binding: BackgroundLaunchBinding {
                assignment: self.assignment,
                expected_state_revision: *expected_state_revision,
            },
            request_id: *request_id,
            task_id: *task_id,
            normalized_spec_sha256: *normalized_spec_sha256,
        })
    }
}

/// Definitive authority reason for refusing one remote background operation
///
/// Every refusal writes nothing on the authority
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceBackgroundRejection {
    /// The authority does not own this resource
    ResourceNotFound,
    /// The request does not come from the current supervisor assignment
    NotCurrentSupervisor,
    /// The resource changed after the supervisor machine read it
    StaleRevision {
        /// Revision named by the request
        expected: ResourceRevision,
        /// Revision saved by the authority
        actual: ResourceRevision,
    },
    /// The supervisor machine has no saved callback route for this task
    RouteEvidenceMissing,
    /// The supervisor machine's saved route names other identities or content
    RouteEvidenceMismatch,
    /// The request identity belongs to a different launch or task
    ConflictingRetry,
    /// A fixed request or task identity already belongs to other records
    IdentityConflict,
    /// A non-closed loan owns the resource; background work returns through its action
    ActiveLoan {
        /// Non-closed loan
        loan_id: LoanId,
    },
    /// Queued resource work blocks a new background launch
    QueuedWorkAhead {
        /// Queued request that blocks the launch
        request_id: RequestId,
    },
    /// The registered background task has not ended
    BackgroundTaskActive {
        /// Registered task
        task_id: TaskId,
        /// Task-layer state
        state: ProcessStatus,
    },
    /// The registered background task has no task record on the authority
    BackgroundTaskMissing {
        /// Registered task
        task_id: TaskId,
    },
    /// An earlier first background launch has not reached a confirmed start or an end
    LaunchPending {
        /// Pending launch task
        task_id: TaskId,
    },
    /// An earlier background task has no verified release evidence
    PredecessorReleaseUnproven {
        /// Earlier task that may still own the resource
        task_id: TaskId,
    },
    /// The command has no ownership contract that release proof can verify
    UnsupportedCommand {
        /// Authority-side reason
        reason: String,
    },
    /// The spec cannot run on the authority as written
    InvalidSpec {
        /// Authority-side reason
        reason: String,
    },
    /// The authority cannot verify the named trainer attempt for this task now
    TrainerAttemptRefused {
        /// Authority-side reason
        reason: String,
    },
    /// The task is already associated with a different trainer attempt
    TrainerAttemptConflict,
}

/// Typed authority result for one remote background request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceBackgroundOutcome {
    /// The authority accepted the exact launch
    Accepted {
        /// Saved exact launch binding
        receipt: RemoteBackgroundLaunchReceipt,
        /// Whether this request inserted the task or observed an earlier insertion
        acceptance: ResourceBackgroundSubmitOutcome,
        /// Authoritative resource after the acceptance
        resource: Resource,
    },
    /// The authority associated the exact trainer attempt with the task
    TrainerAttemptBound {
        /// Authoritative resource after the association
        resource: Resource,
        /// Registered task bound to the attempt
        task_id: TaskId,
        /// Canonical runtime root whose lock the trainer held
        runtime_root: std::path::PathBuf,
        /// Trainer identity of the associated attempt
        attempt_binding: AttemptBinding,
    },
    /// The authority refused the operation and wrote nothing
    Rejected {
        /// Definitive reason
        reason: ResourceBackgroundRejection,
    },
}

/// Strict versioned response from the resource authority
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceBackgroundResponse {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Remote background document version
    pub background_protocol_version: u32,
    /// Authority that handled the request
    pub destination_machine: MachineId,
    /// Resource named by the request
    pub resource_id: ResourceId,
    /// Typed result
    pub outcome: ResourceBackgroundOutcome,
}

impl ResourceBackgroundResponse {
    /// Build the current-version response to one validated request
    #[must_use]
    pub fn new(request: &ResourceBackgroundRequest, outcome: ResourceBackgroundOutcome) -> Self {
        Self {
            api_version: API_VERSION,
            protocol_version: request.protocol_version,
            background_protocol_version: RESOURCE_BACKGROUND_PROTOCOL_VERSION,
            destination_machine: request.destination_machine,
            resource_id: request.assignment.resource_id,
            outcome,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        BackgroundSupervisorAssignment, RESOURCE_BACKGROUND_PROTOCOL_VERSION,
        ResourceBackgroundOperation, ResourceBackgroundRequest, ResourceBackgroundRequestError,
    };
    use crate::domain::{TaskId, ThreadId};
    use crate::machine::MachineId;
    use crate::resource::trainer_publication::AttemptBinding;
    use crate::resource::{AssignmentRevision, ResourceId, ResourceRevision, SupervisorAddress};
    use crate::spec::NormalizedSpec;
    use crate::submission::{RequestId, normalized_spec_sha256};

    fn assignment() -> BackgroundSupervisorAssignment {
        BackgroundSupervisorAssignment {
            authority_machine: MachineId::new(),
            resource_id: ResourceId::new(),
            supervisor: SupervisorAddress {
                machine: MachineId::new(),
                thread: ThreadId(Uuid::now_v7()),
            },
            assignment_revision: AssignmentRevision::new(2),
        }
    }

    fn spec(thread: ThreadId) -> NormalizedSpec {
        serde_json::from_value(json!({
            "api_version": 1,
            "thread": thread,
            "name": "trainer",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["python3", "-m", "ops.run_segment", "run"] }
        }))
        .unwrap()
    }

    fn launch(assignment: BackgroundSupervisorAssignment) -> ResourceBackgroundRequest {
        let spec = spec(assignment.supervisor.thread);
        ResourceBackgroundRequest::new(
            1,
            assignment,
            ResourceBackgroundOperation::Launch {
                expected_state_revision: ResourceRevision::new(4),
                request_id: RequestId::new(),
                task_id: TaskId::new(),
                normalized_spec_sha256: normalized_spec_sha256(&spec).unwrap(),
                spec,
            },
        )
    }

    #[test]
    fn request_rejects_unknown_fields_and_callback_context() {
        let request = launch(assignment());
        let mut wire = serde_json::to_value(&request).unwrap();
        assert_eq!(wire["operation"]["type"], "launch");
        serde_json::from_value::<ResourceBackgroundRequest>(wire.clone()).unwrap();

        wire["operation"]["callback"] = json!({"cwd": "/tmp"});
        assert!(serde_json::from_value::<ResourceBackgroundRequest>(wire.clone()).is_err());
        wire["operation"]
            .as_object_mut()
            .unwrap()
            .remove("callback");
        wire["env"] = json!({"path": "/bin", "home": "/tmp"});
        assert!(serde_json::from_value::<ResourceBackgroundRequest>(wire).is_err());
    }

    #[test]
    fn validation_checks_owners_identities_and_the_carried_digest() {
        use ResourceBackgroundRequestError as Error;

        let request = launch(assignment());
        assert_eq!(request.validate(), Ok(()));

        let mut wrong_destination = request.clone();
        wrong_destination.destination_machine = MachineId::new();
        assert_eq!(
            wrong_destination.validate(),
            Err(Error::DestinationMismatch)
        );
        let mut wrong_source = request.clone();
        wrong_source.source_machine = MachineId::new();
        assert_eq!(wrong_source.validate(), Err(Error::SourceMismatch));

        let mut co_located = assignment();
        co_located.supervisor.machine = co_located.authority_machine;
        assert_eq!(
            launch(co_located).validate(),
            Err(Error::CoLocatedSupervisor)
        );

        let mut reused = request.clone();
        let ResourceBackgroundOperation::Launch {
            request_id,
            task_id,
            ..
        } = &mut reused.operation
        else {
            unreachable!();
        };
        *request_id = RequestId(task_id.0);
        assert_eq!(reused.validate(), Err(Error::InvalidIdentity));

        let mut changed = request.clone();
        let ResourceBackgroundOperation::Launch { spec, .. } = &mut changed.operation else {
            unreachable!();
        };
        spec.name = crate::domain::TaskName::parse("changed").unwrap();
        assert_eq!(changed.validate(), Err(Error::InvalidSpec));

        let mut other_thread = request.clone();
        let ResourceBackgroundOperation::Launch {
            spec,
            normalized_spec_sha256: digest,
            ..
        } = &mut other_thread.operation
        else {
            unreachable!();
        };
        *spec = super::tests::spec(ThreadId(Uuid::now_v7()));
        *digest = normalized_spec_sha256(spec).unwrap();
        assert_eq!(other_thread.validate(), Err(Error::InvalidSpec));

        let mut version = request;
        version.background_protocol_version = RESOURCE_BACKGROUND_PROTOCOL_VERSION + 1;
        assert_eq!(version.validate(), Err(Error::UnsupportedVersion));
    }

    #[test]
    fn launch_receipt_comes_only_from_a_launch() {
        let request = launch(assignment());
        let receipt = request.launch_receipt().unwrap();
        assert_eq!(receipt.binding.assignment, request.assignment);
        assert_eq!(
            receipt.binding.expected_state_revision,
            ResourceRevision::new(4)
        );
        assert_eq!(receipt.binding.origin_machine(), request.source_machine);
        assert_eq!(
            receipt.binding.execution_machine(),
            request.destination_machine
        );

        let bind = ResourceBackgroundRequest::new(
            1,
            request.assignment,
            ResourceBackgroundOperation::BindTrainerAttempt {
                task_id: TaskId::new(),
                attempt_binding: AttemptBinding {
                    campaign_id: "campaign".into(),
                    campaign_revision_id: "revision".into(),
                    task_id: "task".into(),
                    attempt_id: "attempt".into(),
                    attempt_number: 1,
                    ownership_token: "owner".into(),
                },
            },
        );
        assert_eq!(bind.validate(), Ok(()));
        assert!(bind.launch_receipt().is_none());
    }
}
