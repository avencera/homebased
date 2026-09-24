//! Resource-owned first background launch and the idle boundary that follows it
//!
//! A first background launch binds its stable request, preallocated task, full
//! normalized spec, callback route, accepted executor identity, queued row, and
//! first event in one IMMEDIATE transaction with an immutable launch receipt
//! The receipt does not register the task. The queue owner registers it only
//! after the task layer records a confirmed start, so a queued row is never
//! mistaken for live background work
//!
//! The latest launch receipt and the latest loan form the resource history that
//! the idle boundary reads. An unregistered resource serves queued work only
//! when that history names an explicit reason why no background work holds the
//! GPU. A missing task row is never such a reason
//!
//! A supervisor on another machine owns the callback route of its launch. It
//! saves that route before it sends the launch, so the authority keeps only the
//! task row, the accepted remote identity, the first event, and a receipt that
//! names the exact supervisor assignment and resource revision

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::domain::{
    ExitReason, ProcessStatus, TaskEnv, TaskId, TaskRow, TaskState, ThreadId, WorkExitEvidence,
};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::background_launch::{BackgroundLaunchBinding, RemoteBackgroundLaunchReceipt};
use crate::resource::command_shape::{DirectSegmentCommandShape, DirectSegmentCommandShapeError};
use crate::resource::store::{
    ConflictReason, ResourceStoreError, next_queued_request_for_authority, select_non_closed_loan,
};
use crate::resource::{
    BackgroundCommandContract, IdleBoundaryDecision, IdleBoundaryProof, IdleProofGap, Loan,
    LoanClosure, LoanId, LoanPhase, LoanState, Resource, ResourceId, ResourceRequest,
    ResourceRequestState, ResourceRevision, ReturnContext, ServingReleaseProvenance,
    SupervisorAddress,
};
use crate::spec::NormalizedSpec;
use crate::store::IdentityError;
use crate::store::identity::{executor_identity_on, origin_route_by_request_on};
use crate::submission::{
    CallbackContext, CallbackExecutable, ExecutorIdentity, NormalizedSpecSha256, RequestId,
    normalized_spec_sha256,
};

use super::trainer_lock::{
    EndedTaskWitness, HeldTrainerRelease, TrainerLockReleaseError, TrainerLockReleaseGap,
    ended_task_witness, hold_released_trainer_lock,
};
use super::{encode_resource_json, select_authority_resource};

/// Fixed identities and executor context for one first background launch
#[derive(Debug, Clone)]
pub(crate) struct BackgroundLaunchInput {
    /// Machine that owns the resource and executes the task
    pub(crate) authority_machine: MachineId,
    /// Resource whose background slot receives the task
    pub(crate) resource_id: ResourceId,
    /// Stable caller retry identity
    pub(crate) request_id: RequestId,
    /// Task identity used only when this call inserts the launch
    pub(crate) task_id: TaskId,
    /// Full normalized command spec
    pub(crate) spec: NormalizedSpec,
    /// Executor environment captured by the co-located supervisor
    pub(crate) env: TaskEnv,
    /// Codex executable that delivers callbacks to the supervisor thread
    pub(crate) callback_codex: CallbackExecutable,
}

/// Remote supervisor launch that the supervisor machine saved before sending it
#[derive(Debug, Clone)]
pub(crate) struct RemoteBackgroundLaunchInput {
    /// Fixed identities, digest, supervisor assignment, and observed revision
    pub(crate) receipt: RemoteBackgroundLaunchReceipt,
    /// Full normalized command spec saved in the supervisor's route
    pub(crate) spec: NormalizedSpec,
    /// Executor environment of the authority daemon
    pub(crate) env: TaskEnv,
}

/// Result of binding one first background launch
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BackgroundLaunchAcceptance {
    /// The task records and launch receipt committed in this call, so it alone may spawn
    Inserted {
        /// Bound task identity
        task: TaskId,
        /// Resource revision committed with the launch receipt
        state_revision: ResourceRevision,
    },
    /// An exact earlier launch exists; its task is only observed
    Existing {
        /// Bound task identity
        task: TaskId,
        /// State retained by the task layer
        state: ProcessStatus,
    },
    /// The supervisor thread runs on another machine, so nothing was written
    UnsupportedRemoteSupervisor {
        /// Machine that owns the resource
        authority_machine: MachineId,
        /// Supervisor that would need a remote callback route
        supervisor: SupervisorAddress,
    },
}

/// Why a first background launch cannot bind
#[derive(Debug, thiserror::Error)]
pub(crate) enum BackgroundLaunchError {
    /// Resource authority or stored resource data failed validation
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// The remote launch does not come from the current supervisor assignment
    #[error("the background launch does not come from the current supervisor assignment")]
    NotCurrentSupervisor,
    /// The resource changed after the supervisor machine read it
    #[error("resource revision changed from {expected:?} to {actual:?}")]
    StaleRevision {
        /// Revision that the supervisor machine observed
        expected: ResourceRevision,
        /// Revision saved by the authority
        actual: ResourceRevision,
    },
    /// The launch callback thread is not the assigned supervisor thread
    #[error("background launch thread {found} is not the supervisor thread {expected}")]
    NotSupervisorThread {
        /// Assigned supervisor thread
        expected: ThreadId,
        /// Thread named by the spec
        found: ThreadId,
    },
    /// A non-closed loan owns the resource; background work returns through its action
    #[error("loan {loan_id:?} owns the resource")]
    ActiveLoan {
        /// Non-closed loan
        loan_id: LoanId,
    },
    /// Queued resource work blocks a new background launch
    #[error("queued resource request {request_id:?} blocks the background launch")]
    QueuedWorkAhead {
        /// Queued request that blocks the launch
        request_id: RequestId,
    },
    /// The registered background task has not ended
    #[error("registered background task {task_id} is {state}")]
    BackgroundTaskActive {
        /// Registered task
        task_id: TaskId,
        /// Task-layer state
        state: ProcessStatus,
    },
    /// The registered background task has no task row on the authority
    #[error("registered background task {task_id} has no task record")]
    BackgroundTaskMissing {
        /// Registered task
        task_id: TaskId,
    },
    /// An earlier first background launch has not reached a confirmed start or an end
    #[error("background launch task {task_id} is still pending")]
    LaunchPending {
        /// Pending launch task
        task_id: TaskId,
    },
    /// An earlier background task ended without confirmed exit or no-child evidence
    ///
    /// It is lost, its process-group exit is unconfirmed, or its records do not
    /// match, so it may still hold the GPU and needs an owner decision
    #[error("background task {task_id} ended without proof that it released the resource")]
    PredecessorReleaseUnproven {
        /// Earlier background task that may still hold the resource
        task_id: TaskId,
    },
    /// An earlier background task's wrapper exited, but its trainer lock is not proven free
    ///
    /// The direct-segment worker runs in its own session, so only the exact lock
    /// named by the trainer-attempt association, held through this transaction,
    /// proves that the worker released the GPU
    #[error("background task {task_id} has no verified trainer lock release: {gap}")]
    PredecessorOwnershipUnproven {
        /// Earlier background task that may still hold the resource
        task_id: TaskId,
        /// Missing or failed part of the lock proof
        gap: TrainerLockReleaseGap,
    },
    /// The request identity belongs to a different launch or task
    #[error("request {request_id:?} was retried with different content")]
    ConflictingRetry {
        /// Reused request identity
        request_id: RequestId,
    },
    /// The saved launch records do not match the receipt
    #[error("background launch task {task_id} records do not match its receipt")]
    IdentityConflict {
        /// Bound task identity
        task_id: TaskId,
    },
    /// The command has no ownership contract that release proof can verify
    #[error("background command ownership cannot be verified: {0}")]
    UnsupportedCommand(#[from] DirectSegmentCommandShapeError),
    /// The resource revision cannot be incremented
    #[error("resource revision {revision:?} cannot be incremented")]
    RevisionExhausted {
        /// Current revision
        revision: ResourceRevision,
    },
    /// Route or executor identity data is invalid
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// Task preparation or task record insertion failed
    #[error("background launch task records failed: {0}")]
    TaskRecords(#[from] AppError),
    /// A receipt could not be encoded or decoded
    #[error("background launch encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    /// SQLite failed
    #[error("background launch storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Callback owner of one first background launch
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum BackgroundLaunchOrigin {
    /// The supervisor thread runs on the authority, which saved the callback route
    CoLocated,
    /// The supervisor machine saved the callback route before it sent the launch
    RemoteSupervisor {
        /// Resource revision that the supervisor machine observed
        expected_state_revision: ResourceRevision,
    },
}

/// Immutable binding of one first background launch
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackgroundLaunchReceipt {
    resource_id: ResourceId,
    authority_machine: MachineId,
    request_id: RequestId,
    task_id: TaskId,
    normalized_spec_sha256: NormalizedSpecSha256,
    supervisor: SupervisorAddress,
    assignment_revision: crate::resource::AssignmentRevision,
    accepted_state_revision: ResourceRevision,
    // the ended registration that this launch replaces once it starts
    replaces_task: Option<TaskId>,
    // the latest loan when the launch was accepted; a later loan supersedes it
    preceding_loan: Option<LoanId>,
    contract: BackgroundCommandContract,
    origin: BackgroundLaunchOrigin,
}

impl BackgroundLaunchReceipt {
    /// Machine that owns the task's callback route
    fn origin_machine(&self) -> MachineId {
        match self.origin {
            BackgroundLaunchOrigin::CoLocated => self.authority_machine,
            BackgroundLaunchOrigin::RemoteSupervisor { .. } => self.supervisor.machine,
        }
    }

    /// Whether this receipt saved exactly one remote supervisor's launch
    fn matches_remote(&self, expected: &RemoteBackgroundLaunchReceipt) -> bool {
        let BackgroundLaunchBinding {
            assignment,
            expected_state_revision,
        } = expected.binding;
        self.origin
            == BackgroundLaunchOrigin::RemoteSupervisor {
                expected_state_revision,
            }
            && self.resource_id == assignment.resource_id
            && self.authority_machine == assignment.authority_machine
            && self.supervisor == assignment.supervisor
            && self.assignment_revision == assignment.assignment_revision
            && self.request_id == expected.request_id
            && self.task_id == expected.task_id
            && self.normalized_spec_sha256 == expected.normalized_spec_sha256
    }
}

/// Loan-opening receipt that permits serving without a registered task
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdleOpeningReceipt {
    loan_id: LoanId,
    resource_id: ResourceId,
    authority_machine: MachineId,
    request_id: RequestId,
    state_revision: ResourceRevision,
    proof: IdleBoundaryProof,
}

/// Task-layer phase of the latest first background launch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackgroundLaunchPhase {
    /// A later loan exists, so this launch no longer describes the resource
    Superseded,
    /// The row is queued; its worker may be starting or may have lost its spawn
    Queued,
    /// The row is running and waits for the queue owner to register it
    StartedUnregistered,
    /// The task is the registered background task
    Registered,
    /// The task ended before registration
    EndedBeforeRegistration {
        /// Task-layer evidence about the process group that the task started
        release: EndedLaunchRelease,
    },
    /// The launch records do not match the receipt
    IdentityMismatch,
}

/// Task-layer evidence for a first background launch that ended before registration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndedLaunchRelease {
    /// The task layer recorded that no child process started
    NeverSpawned,
    /// The worker confirmed that its owned process group exited
    ConfirmedExited,
    /// The task is lost, or its process-group exit is not confirmed
    Unproven,
}

/// Latest first background launch and its derived phase
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackgroundLaunchView {
    /// Stable launch request identity
    pub(crate) request_id: RequestId,
    /// Bound task identity
    pub(crate) task_id: TaskId,
    /// Derived task-layer phase
    pub(crate) phase: BackgroundLaunchPhase,
}

impl BackgroundLaunchView {
    /// Return the task that keeps the resource reserved before registration
    pub(crate) fn pending_task(&self) -> Option<TaskId> {
        matches!(
            self.phase,
            BackgroundLaunchPhase::Queued | BackgroundLaunchPhase::StartedUnregistered
        )
        .then_some(self.task_id)
    }

    /// Whether the launch ended before registration with no automatic release proof
    ///
    /// The maintained trainer starts its worker in another session, and an
    /// unregistered task cannot bind the trainer attempt whose lock would prove
    /// that worker's exit. Only a saved operator attestation releases it
    pub(crate) fn awaits_operator_release(&self) -> bool {
        matches!(
            self.phase,
            BackgroundLaunchPhase::EndedBeforeRegistration {
                release: EndedLaunchRelease::ConfirmedExited | EndedLaunchRelease::Unproven,
            }
        )
    }
}

/// Proof that no earlier background task can still hold the GPU
///
/// Keep it alive until the launch transaction commits, because it holds each
/// released trainer lock that the proof relies on
#[must_use = "keep the released trainer locks held until the launch commits"]
struct BackgroundSlotClearance {
    held_locks: Vec<(TaskId, HeldTrainerRelease)>,
}

impl BackgroundSlotClearance {
    /// Recheck every held lock against its saved path just before the commit
    fn recheck(&self) -> Result<(), BackgroundLaunchError> {
        for (task_id, held) in &self.held_locks {
            held.recheck()
                .map_err(|gap| BackgroundLaunchError::PredecessorOwnershipUnproven {
                    task_id: *task_id,
                    gap,
                })?;
        }
        Ok(())
    }
}

impl crate::store::Store {
    /// Bind one first background launch; only an `Inserted` result may spawn
    pub(crate) fn accept_background_launch_for_authority(
        &mut self,
        input: BackgroundLaunchInput,
    ) -> Result<BackgroundLaunchAcceptance, BackgroundLaunchError> {
        accept_background_launch_for_authority(&mut self.conn, input)
    }

    /// Bind one remote supervisor's first background launch; only `Inserted` may spawn
    pub(crate) fn accept_remote_background_launch_for_authority(
        &mut self,
        input: RemoteBackgroundLaunchInput,
    ) -> Result<BackgroundLaunchAcceptance, BackgroundLaunchError> {
        accept_remote_background_launch_for_authority(&mut self.conn, input)
    }

    /// Read the latest first background launch of one authority-owned resource
    pub(crate) fn background_launch_for_authority(
        &self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<Option<BackgroundLaunchView>, ResourceStoreError> {
        let resource = select_authority_resource(&self.conn, authority_machine, resource_id)?;
        current_launch_on(&self.conn, &resource)
    }

    /// Read the tasks of first background launches that are still queued
    ///
    /// Startup keeps these rows queued for the resource owner because a free
    /// runner lock cannot show whether the first spawn happened
    pub(crate) fn queued_background_launch_tasks_for_authority(
        &self,
        authority_machine: MachineId,
    ) -> Result<Vec<TaskId>, ResourceStoreError> {
        let mut statement = self.conn.prepare(
            "SELECT l.task_id FROM resource_background_launches l
             JOIN resources r ON r.id = l.resource_id
             JOIN tasks t ON t.id = l.task_id
             WHERE r.authority_machine = ?1 AND t.status = 'queued'",
        )?;
        let ids = statement
            .query_map([authority_machine.as_uuid().to_string()], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| {
                id.parse()
                    .map_err(|error| ResourceStoreError::corrupt("background launch task", error))
            })
            .collect()
    }
}

fn accept_background_launch_for_authority(
    conn: &mut Connection,
    input: BackgroundLaunchInput,
) -> Result<BackgroundLaunchAcceptance, BackgroundLaunchError> {
    let digest = normalized_spec_sha256(&input.spec)?;
    {
        // an exact retry is answered before any file-system check, so a path
        // that changed after the launch cannot turn it into a rejection
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        if let Some(existing) = replay_on(&tx, &input, digest)? {
            tx.commit()?;
            return Ok(existing);
        }
        let resource = select_authority_resource(&tx, input.authority_machine, input.resource_id)?;
        if let Some(unsupported) = remote_supervisor(&resource) {
            return Ok(unsupported);
        }
        tx.commit()?;
    }

    // canonical path checks stay outside the IMMEDIATE write transaction
    let (row, contract) = prepare_launch_row(input.task_id, &input.spec, &input.env)?;

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(existing) = replay_on(&tx, &input, digest)? {
        tx.commit()?;
        return Ok(existing);
    }
    let resource = select_authority_resource(&tx, input.authority_machine, input.resource_id)?;
    if let Some(unsupported) = remote_supervisor(&resource) {
        return Ok(unsupported);
    }
    if input.spec.thread != resource.supervisor.thread {
        return Err(BackgroundLaunchError::NotSupervisorThread {
            expected: resource.supervisor.thread,
            found: input.spec.thread,
        });
    }
    let clearance = require_launch_slot(&tx, input.authority_machine, &resource)?;

    let task_id = input.task_id;
    if super::task_identity_is_used(&tx, input.request_id, task_id)? {
        return Err(BackgroundLaunchError::IdentityConflict { task_id });
    }
    let callback = CallbackContext {
        env: input.env,
        cwd: input.spec.cwd.clone(),
        codex: input.callback_codex,
    };
    crate::store::insert_local_task_records_on(
        &tx,
        &row,
        &input.spec,
        input.authority_machine,
        input.request_id,
        &callback,
        None,
    )
    .map_err(|error| match error {
        AppError::ClusterTaskConflict { .. } => BackgroundLaunchError::IdentityConflict { task_id },
        error => BackgroundLaunchError::TaskRecords(error),
    })?;
    let state_revision = record_launch_on(
        &tx,
        &resource,
        input.request_id,
        task_id,
        digest,
        contract,
        BackgroundLaunchOrigin::CoLocated,
    )?;
    clearance.recheck()?;
    tx.commit()?;
    // the new worker may start only after the commit, so the locks stay held until then
    drop(clearance);

    Ok(BackgroundLaunchAcceptance::Inserted {
        task: task_id,
        state_revision,
    })
}

/// Bind one remote supervisor's launch after its route evidence was checked
///
/// The receipt names the supervisor assignment and the resource revision that
/// the supervisor machine observed. The authority rechecks both, the empty
/// background slot, and the task identity in one IMMEDIATE transaction, then
/// saves the task row, remote identity, first event, and receipt together
fn accept_remote_background_launch_for_authority(
    conn: &mut Connection,
    input: RemoteBackgroundLaunchInput,
) -> Result<BackgroundLaunchAcceptance, BackgroundLaunchError> {
    let RemoteBackgroundLaunchInput {
        receipt: expected,
        spec,
        env,
    } = input;
    let request_id = expected.request_id;
    if normalized_spec_sha256(&spec)? != expected.normalized_spec_sha256 {
        return Err(BackgroundLaunchError::ConflictingRetry { request_id });
    }
    let assignment = expected.binding.assignment;
    {
        // an exact retry is answered before the assignment, revision, or path
        // checks, so a later transition cannot turn it into a rejection
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        if let Some(existing) = replay_remote_on(&tx, &expected)? {
            tx.commit()?;
            return Ok(existing);
        }
        let resource =
            select_authority_resource(&tx, assignment.authority_machine, assignment.resource_id)?;
        check_remote_assignment(&resource, &expected.binding, &spec)?;
        tx.commit()?;
    }

    let task_id = expected.task_id;
    let (row, contract) = prepare_launch_row(task_id, &spec, &env)?;

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(existing) = replay_remote_on(&tx, &expected)? {
        tx.commit()?;
        return Ok(existing);
    }
    let resource =
        select_authority_resource(&tx, assignment.authority_machine, assignment.resource_id)?;
    check_remote_assignment(&resource, &expected.binding, &spec)?;
    let clearance = require_launch_slot(&tx, assignment.authority_machine, &resource)?;
    if super::task_identity_is_used(&tx, request_id, task_id)? {
        return Err(BackgroundLaunchError::IdentityConflict { task_id });
    }
    crate::store::insert_remote_origin_task_records_on(
        &tx,
        &row,
        &spec,
        &crate::store::RemoteOriginTask {
            request_id,
            origin_machine: expected.binding.origin_machine(),
            execution_machine: expected.binding.execution_machine(),
            thread: assignment.supervisor.thread,
            reserved_watcher: false,
        },
    )
    .map_err(|error| match error {
        AppError::ClusterTaskConflict { .. } => BackgroundLaunchError::IdentityConflict { task_id },
        error => BackgroundLaunchError::TaskRecords(error),
    })?;
    let state_revision = record_launch_on(
        &tx,
        &resource,
        request_id,
        task_id,
        expected.normalized_spec_sha256,
        contract,
        BackgroundLaunchOrigin::RemoteSupervisor {
            expected_state_revision: expected.binding.expected_state_revision,
        },
    )?;
    clearance.recheck()?;
    tx.commit()?;
    drop(clearance);

    Ok(BackgroundLaunchAcceptance::Inserted {
        task: task_id,
        state_revision,
    })
}

/// Check that a remote launch names the current supervisor assignment and revision
fn check_remote_assignment(
    resource: &Resource,
    binding: &BackgroundLaunchBinding,
    spec: &NormalizedSpec,
) -> Result<(), BackgroundLaunchError> {
    // a co-located supervisor keeps its callback route on the authority
    if resource.supervisor.machine == resource.authority_machine()
        || !binding.assignment.is_current(resource)
    {
        return Err(BackgroundLaunchError::NotCurrentSupervisor);
    }
    if resource.state_revision != binding.expected_state_revision {
        return Err(BackgroundLaunchError::StaleRevision {
            expected: binding.expected_state_revision,
            actual: resource.state_revision,
        });
    }
    if spec.thread != resource.supervisor.thread {
        return Err(BackgroundLaunchError::NotSupervisorThread {
            expected: resource.supervisor.thread,
            found: spec.thread,
        });
    }
    Ok(())
}

/// Build the queued row and verify the maintained trainer command before any write
fn prepare_launch_row(
    task_id: TaskId,
    spec: &NormalizedSpec,
    env: &TaskEnv,
) -> Result<(TaskRow, BackgroundCommandContract), BackgroundLaunchError> {
    crate::spec::check_cwd(&spec.cwd)?;
    let binary = crate::invocation::resolve_workload_binary(&spec.workload, &env.path, &spec.cwd)?;
    let row = crate::store::new_queued_task(crate::store::NewTask {
        id: task_id,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: crate::invocation::persist_workload(&spec.workload),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: env.clone(),
        binary,
    });
    let shape = DirectSegmentCommandShape::validate_launch(spec, &row)?;
    let contract = BackgroundCommandContract::DirectSegmentTrainer {
        runtime_root: shape.runtime_root().to_path_buf(),
    };
    Ok((row, contract))
}

/// Refuse a launch while a loan, earlier queued work, or background work holds the resource
fn require_launch_slot(
    conn: &Connection,
    authority_machine: MachineId,
    resource: &Resource,
) -> Result<BackgroundSlotClearance, BackgroundLaunchError> {
    if let Some(loan) = select_non_closed_loan(conn, resource.id)? {
        return Err(BackgroundLaunchError::ActiveLoan { loan_id: loan.id });
    }
    if let Some(request) = next_queued_request_for_authority(conn, authority_machine, resource.id)?
    {
        return Err(BackgroundLaunchError::QueuedWorkAhead {
            request_id: request.request_id,
        });
    }
    require_background_slot_free(conn, resource)
}

/// Advance the resource revision and save the immutable launch receipt
fn record_launch_on(
    tx: &Transaction<'_>,
    resource: &Resource,
    request_id: RequestId,
    task_id: TaskId,
    normalized_spec_sha256: NormalizedSpecSha256,
    contract: BackgroundCommandContract,
    origin: BackgroundLaunchOrigin,
) -> Result<ResourceRevision, BackgroundLaunchError> {
    // the launch changes what the resource may do next, but it registers nothing
    let state_revision = advance_resource_on(tx, resource, resource.registered_background_task)
        .map_err(|error| match error {
            AdvanceError::Exhausted => BackgroundLaunchError::RevisionExhausted {
                revision: resource.state_revision,
            },
            AdvanceError::Changed => BackgroundLaunchError::Resource(ResourceStoreError::Conflict(
                ConflictReason::ResourceRevisionChanged,
            )),
            AdvanceError::Storage(error) => BackgroundLaunchError::Storage(error),
        })?;
    let receipt = BackgroundLaunchReceipt {
        resource_id: resource.id,
        authority_machine: resource.authority_machine(),
        request_id,
        task_id,
        normalized_spec_sha256,
        supervisor: resource.supervisor,
        assignment_revision: resource.assignment_revision,
        accepted_state_revision: state_revision,
        replaces_task: resource.registered_background_task,
        preceding_loan: latest_loan_on(tx, resource.id)?.map(|loan| loan.id),
        contract,
        origin,
    };
    tx.execute(
        "INSERT INTO resource_background_launches (request_id, task_id, resource_id, receipt_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            request_id.0.to_string(),
            task_id.to_string(),
            resource.id.as_uuid().to_string(),
            serde_json::to_string(&receipt)?,
        ],
    )?;
    Ok(state_revision)
}

// a local route for a remote supervisor thread would send callbacks to the wrong
// machine, so this path writes nothing for it
fn remote_supervisor(resource: &Resource) -> Option<BackgroundLaunchAcceptance> {
    (resource.supervisor.machine != resource.authority_machine()).then_some(
        BackgroundLaunchAcceptance::UnsupportedRemoteSupervisor {
            authority_machine: resource.authority_machine(),
            supervisor: resource.supervisor,
        },
    )
}

/// Answer an exact retry from its receipt without writing or spawning
fn replay_on(
    conn: &Connection,
    input: &BackgroundLaunchInput,
    digest: NormalizedSpecSha256,
) -> Result<Option<BackgroundLaunchAcceptance>, BackgroundLaunchError> {
    let conflict = BackgroundLaunchError::ConflictingRetry {
        request_id: input.request_id,
    };
    let Some(receipt) = receipt_by_request_on(conn, input.request_id)? else {
        // an ordinary task or another resource path already owns this request identity
        if origin_route_by_request_on(conn, input.request_id)?.is_some()
            || super::resource_request_identity_exists(conn, input.request_id, input.task_id)?
        {
            return Err(conflict);
        }
        return Ok(None);
    };
    if receipt.origin != BackgroundLaunchOrigin::CoLocated
        || receipt.resource_id != input.resource_id
        || receipt.authority_machine != input.authority_machine
        || receipt.normalized_spec_sha256 != digest
    {
        return Err(conflict);
    }
    let task_id = receipt.task_id;
    let row = bound_launch_row_on(conn, &receipt)?
        .ok_or(BackgroundLaunchError::IdentityConflict { task_id })?;

    Ok(Some(BackgroundLaunchAcceptance::Existing {
        task: task_id,
        state: row.status(),
    }))
}

/// Answer an exact remote retry from its receipt without writing or spawning
fn replay_remote_on(
    conn: &Connection,
    expected: &RemoteBackgroundLaunchReceipt,
) -> Result<Option<BackgroundLaunchAcceptance>, BackgroundLaunchError> {
    let request_id = expected.request_id;
    let conflict = BackgroundLaunchError::ConflictingRetry { request_id };
    let saved = match receipt_by_request_on(conn, request_id)? {
        Some(saved) => saved,
        // another launch owns the task identity, or another path owns the request
        None if receipt_by_task_on(conn, expected.task_id)?.is_some()
            || origin_route_by_request_on(conn, request_id)?.is_some()
            || super::resource_request_identity_exists(conn, request_id, expected.task_id)? =>
        {
            return Err(conflict);
        }
        None => return Ok(None),
    };
    if !saved.matches_remote(expected) {
        return Err(conflict);
    }
    let task_id = saved.task_id;
    let row = bound_launch_row_on(conn, &saved)?
        .ok_or(BackgroundLaunchError::IdentityConflict { task_id })?;

    Ok(Some(BackgroundLaunchAcceptance::Existing {
        task: task_id,
        state: row.status(),
    }))
}

/// Refuse a launch while any predecessor background task may still hold the resource
///
/// The latest launch and the registered task are the only background tasks that
/// can hold the GPU without a non-closed loan; every other background task was
/// released through a loan transition that required its own proof. Each must be
/// terminal with no-child evidence, or with confirmed exit and the witness its
/// command contract needs, before a new launch commits. A direct-segment
/// trainer's witness is its exact released lock, which the returned clearance
/// holds
fn require_background_slot_free(
    conn: &Connection,
    resource: &Resource,
) -> Result<BackgroundSlotClearance, BackgroundLaunchError> {
    let mut clearance = BackgroundSlotClearance {
        held_locks: Vec::new(),
    };
    if let Some((receipt, phase)) = current_launch_record_on(conn, resource)? {
        let task_id = receipt.task_id;
        match phase {
            BackgroundLaunchPhase::Queued | BackgroundLaunchPhase::StartedUnregistered => {
                return Err(BackgroundLaunchError::LaunchPending { task_id });
            }
            BackgroundLaunchPhase::IdentityMismatch
            | BackgroundLaunchPhase::EndedBeforeRegistration {
                release: EndedLaunchRelease::Unproven,
            } => {
                return Err(BackgroundLaunchError::PredecessorReleaseUnproven { task_id });
            }
            // every first launch is the direct-segment trainer, whose wrapper
            // exit does not cover its worker
            BackgroundLaunchPhase::EndedBeforeRegistration {
                release: EndedLaunchRelease::ConfirmedExited,
            } => {
                let BackgroundCommandContract::DirectSegmentTrainer { .. } = receipt.contract;
                let held = hold_predecessor_lock(conn, resource, task_id)?;
                clearance.held_locks.push((task_id, held));
            }
            BackgroundLaunchPhase::Superseded
            | BackgroundLaunchPhase::Registered
            | BackgroundLaunchPhase::EndedBeforeRegistration {
                release: EndedLaunchRelease::NeverSpawned,
            } => {}
        }
    }
    let Some(task_id) = resource.registered_background_task else {
        return Ok(clearance);
    };
    let row = crate::store::task_by_id_on(conn, task_id)?
        .ok_or(BackgroundLaunchError::BackgroundTaskMissing { task_id })?;
    if !row.state.is_terminal() {
        return Err(BackgroundLaunchError::BackgroundTaskActive {
            task_id,
            state: row.status(),
        });
    }
    match ended_task_release(&row) {
        EndedLaunchRelease::Unproven => {
            return Err(BackgroundLaunchError::PredecessorReleaseUnproven { task_id });
        }
        EndedLaunchRelease::NeverSpawned => {}
        // the confirmed evidence must be the kind that the task's contract names,
        // and a trainer also needs its released lock
        EndedLaunchRelease::ConfirmedExited => {
            let witness = ended_task_witness(conn, resource.authority_machine(), task_id)
                .map_err(|error| predecessor_lock_error(task_id, error))?;
            if !witness.accepts(&row.work_exit_evidence()) {
                return Err(BackgroundLaunchError::PredecessorReleaseUnproven { task_id });
            }
            if witness == EndedTaskWitness::TrainerLock {
                let held = hold_predecessor_lock(conn, resource, task_id)?;
                clearance.held_locks.push((task_id, held));
            }
        }
    }

    Ok(clearance)
}

/// Take the exact released lock named by one ended predecessor's own association
fn hold_predecessor_lock(
    conn: &Connection,
    resource: &Resource,
    task_id: TaskId,
) -> Result<HeldTrainerRelease, BackgroundLaunchError> {
    hold_released_trainer_lock(conn, resource, task_id, task_id)
        .map_err(|error| predecessor_lock_error(task_id, error))
}

fn predecessor_lock_error(
    task_id: TaskId,
    error: TrainerLockReleaseError,
) -> BackgroundLaunchError {
    match error {
        TrainerLockReleaseError::Unproven(gap) => {
            BackgroundLaunchError::PredecessorOwnershipUnproven { task_id, gap }
        }
        TrainerLockReleaseError::Store(error) => BackgroundLaunchError::Resource(error),
    }
}

/// Promote a started first background launch to the registered background task
///
/// The task layer must record a running row and a running accepted identity
/// The registration compares the prior registration saved in the receipt, so a
/// stale launch cannot replace a newer background task
pub(crate) fn promote_started_background_launch_on(
    tx: &Transaction<'_>,
    resource: &Resource,
) -> Result<Option<Resource>, ResourceStoreError> {
    let Some((receipt, phase)) = current_launch_record_on(tx, resource)? else {
        return Ok(None);
    };
    if phase != BackgroundLaunchPhase::StartedUnregistered
        || resource.registered_background_task != receipt.replaces_task
        || select_non_closed_loan(tx, resource.id)?.is_some()
    {
        return Ok(None);
    }

    let state_revision =
        advance_resource_on(tx, resource, Some(receipt.task_id)).map_err(|error| match error {
            AdvanceError::Exhausted => {
                ResourceStoreError::Conflict(ConflictReason::RevisionExhausted)
            }
            AdvanceError::Changed => {
                ResourceStoreError::Conflict(ConflictReason::ResourceRevisionChanged)
            }
            AdvanceError::Storage(error) => ResourceStoreError::Storage(error),
        })?;
    let mut promoted = resource.clone();
    promoted.state_revision = state_revision;
    promoted.registered_background_task = Some(receipt.task_id);

    Ok(Some(promoted))
}

/// Return the first background launch task that still keeps the resource reserved
pub(crate) fn pending_background_launch_on(
    conn: &Connection,
    resource: &Resource,
) -> Result<Option<TaskId>, ResourceStoreError> {
    Ok(current_launch_on(conn, resource)?
        .as_ref()
        .and_then(BackgroundLaunchView::pending_task))
}

/// Return the latest first background launch and the registration it replaces
pub(super) fn current_launch_and_predecessor_on(
    conn: &Connection,
    resource: &Resource,
) -> Result<Option<(BackgroundLaunchView, Option<TaskId>)>, ResourceStoreError> {
    Ok(
        current_launch_record_on(conn, resource)?.map(|(receipt, phase)| {
            let view = BackgroundLaunchView {
                request_id: receipt.request_id,
                task_id: receipt.task_id,
                phase,
            };
            (view, receipt.replaces_task)
        }),
    )
}

/// Decide whether saved history proves that an unregistered resource is idle
///
/// The latest record wins. A launch newer than every loan decides by its task
/// outcome; otherwise the latest loan decides by its closure. A resource with
/// neither record has no evidence, and a missing task row is never evidence
pub(crate) fn idle_boundary_decision_on(
    conn: &Connection,
    resource: &Resource,
) -> Result<IdleBoundaryDecision, ResourceStoreError> {
    use IdleBoundaryDecision::{Proven, Unproven};

    if resource.registered_background_task.is_some() {
        return Ok(Unproven(IdleProofGap::InconsistentHistory));
    }
    // a current operator attestation is newer than every launch and loan it names
    if let Some(boundary) = super::operator_release::current_operator_boundary_on(conn, resource)? {
        return Ok(Proven(IdleBoundaryProof::OperatorAttestedGpuFree {
            operation_id: boundary.operation_id,
            task_id: boundary.task_id,
        }));
    }
    if let Some((receipt, phase)) = current_launch_record_on(conn, resource)? {
        return Ok(match phase {
            BackgroundLaunchPhase::Superseded => loan_idle_decision(conn, resource)?,
            BackgroundLaunchPhase::EndedBeforeRegistration {
                release: EndedLaunchRelease::NeverSpawned,
            } => Proven(IdleBoundaryProof::BackgroundLaunchNeverSpawned {
                request_id: receipt.request_id,
                task_id: receipt.task_id,
            }),
            // the maintained trainer starts its worker in another session, so its
            // own process-group exit does not prove that the worker released the GPU
            BackgroundLaunchPhase::EndedBeforeRegistration {
                release: EndedLaunchRelease::ConfirmedExited | EndedLaunchRelease::Unproven,
            } => Unproven(IdleProofGap::BackgroundLaunchReleaseUnproven {
                task_id: receipt.task_id,
            }),
            BackgroundLaunchPhase::Queued
            | BackgroundLaunchPhase::StartedUnregistered
            | BackgroundLaunchPhase::Registered
            | BackgroundLaunchPhase::IdentityMismatch => {
                Unproven(IdleProofGap::InconsistentHistory)
            }
        });
    }
    // an initial attestation counts only before any loan or launch exists
    if let Some(operation_id) = super::initial_idle::initial_idle_boundary_on(conn, resource)? {
        return Ok(Proven(IdleBoundaryProof::OperatorAttestedInitialIdle {
            operation_id,
        }));
    }

    loan_idle_decision(conn, resource)
}

fn loan_idle_decision(
    conn: &Connection,
    resource: &Resource,
) -> Result<IdleBoundaryDecision, ResourceStoreError> {
    use IdleBoundaryDecision::{Proven, Unproven};

    let Some(loan) = latest_loan_on(conn, resource.id)? else {
        return Ok(Unproven(IdleProofGap::NoIdleEvidence));
    };
    let LoanState::Closed { result } = &loan.state else {
        return Ok(Unproven(IdleProofGap::InconsistentHistory));
    };
    Ok(match result {
        // the no-resume decision required the returned run to have ended after a
        // verified or operator-attested release, or an idle context with no registered task
        LoanClosure::NoResume { .. } => {
            Proven(IdleBoundaryProof::SupervisorNoResume { loan_id: loan.id })
        }
        // the resolution required a terminal return task with proven process release
        LoanClosure::RestoreEnded { task_id, .. } => {
            Proven(IdleBoundaryProof::RestoreEndedWithProvenRelease {
                loan_id: loan.id,
                task_id: *task_id,
            })
        }
        // the closure required a successful foreground end with a confirmed
        // process-group exit, and it cleared the background registration
        LoanClosure::ForegroundReturnEnded { task_id, .. } => {
            Proven(IdleBoundaryProof::ForegroundReturnEnded {
                loan_id: loan.id,
                task_id: *task_id,
            })
        }
        // only the exact saved attestation receipt that closed this loan is evidence
        LoanClosure::OperatorAttestedRestoreEnded {
            task_id,
            operation_id,
            ..
        } => {
            if super::operator_release::operator_restore_closure_matches_on(conn, resource, &loan)?
            {
                Proven(IdleBoundaryProof::OperatorAttestedGpuFree {
                    operation_id: *operation_id,
                    task_id: *task_id,
                })
            } else {
                Unproven(IdleProofGap::InconsistentHistory)
            }
        }
        // these closures keep or register a background task
        LoanClosure::Resumed { .. } | LoanClosure::NotStopped { .. } => {
            Unproven(IdleProofGap::InconsistentHistory)
        }
    })
}

/// Open a Serving loan for the next request in serving order from a proven idle boundary
///
/// The loan, request assignment, resource revision, and opening receipt commit
/// in the caller's transaction. The receipt is the only provenance that lets the
/// assigned command start without a registered background task
pub(crate) fn open_idle_serving_loan_on(
    tx: &Transaction<'_>,
    authority_machine: MachineId,
    resource: &Resource,
    mut request: ResourceRequest,
    proof: IdleBoundaryProof,
) -> Result<(Loan, ResourceRequest), ResourceStoreError> {
    if resource.registered_background_task.is_some() {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ResourceAssignmentChanged,
        ));
    }
    if request.resource_id != resource.id || request.state != ResourceRequestState::Queued {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestStateChanged,
        ));
    }
    let loan = Loan {
        id: LoanId::new(),
        resource_id: resource.id,
        state: LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: ReturnContext::Idle,
                current_request_id: request.request_id,
                release_provenance: ServingReleaseProvenance::IdleBoundary {
                    proof: proof.clone(),
                },
            },
        },
    };
    tx.execute(
        "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
        params![
            loan.id.as_uuid().to_string(),
            resource.id.as_uuid().to_string(),
            encode_resource_json(&loan.state)?,
        ],
    )?;

    request.state = ResourceRequestState::Assigned { loan_id: loan.id };
    let changed = tx.execute(
        "UPDATE resource_requests SET state_json = ?1
         WHERE request_id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'queued'",
        params![
            encode_resource_json(&request.state)?,
            request.request_id.0.to_string(),
            resource.id.as_uuid().to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestStateChanged,
        ));
    }

    let state_revision = advance_resource_on(tx, resource, None).map_err(|error| match error {
        AdvanceError::Exhausted => ResourceStoreError::Conflict(ConflictReason::RevisionExhausted),
        AdvanceError::Changed => {
            ResourceStoreError::Conflict(ConflictReason::ResourceRevisionChanged)
        }
        AdvanceError::Storage(error) => ResourceStoreError::Storage(error),
    })?;
    let receipt = IdleOpeningReceipt {
        loan_id: loan.id,
        resource_id: resource.id,
        authority_machine,
        request_id: request.request_id,
        state_revision,
        proof,
    };
    tx.execute(
        "INSERT INTO resource_idle_openings (loan_id, resource_id, receipt_json)
         VALUES (?1, ?2, ?3)",
        params![
            loan.id.as_uuid().to_string(),
            resource.id.as_uuid().to_string(),
            encode_resource_json(&receipt)?,
        ],
    )?;

    Ok((loan, request))
}

/// Check that an idle Serving provenance matches its saved loan-opening receipt
pub(crate) fn idle_opening_matches_on(
    conn: &Connection,
    authority_machine: MachineId,
    resource: &Resource,
    loan: &Loan,
    return_context: &ReturnContext,
    proof: &IdleBoundaryProof,
) -> Result<bool, ResourceStoreError> {
    let saved: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_idle_openings WHERE loan_id = ?1",
            [loan.id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(saved) = saved else {
        return Ok(false);
    };
    let receipt: IdleOpeningReceipt = serde_json::from_str(&saved)
        .map_err(|error| ResourceStoreError::corrupt("idle opening receipt", error))?;

    Ok(receipt.loan_id == loan.id
        && receipt.resource_id == resource.id
        && loan.resource_id == resource.id
        && receipt.authority_machine == authority_machine
        && resource.authority_machine() == authority_machine
        && receipt.proof == *proof
        && *return_context == ReturnContext::Idle
        && resource.registered_background_task.is_none())
}

/// Read the latest first background launch of one resource with its derived phase
pub(super) fn current_launch_on(
    conn: &Connection,
    resource: &Resource,
) -> Result<Option<BackgroundLaunchView>, ResourceStoreError> {
    Ok(
        current_launch_record_on(conn, resource)?.map(|(receipt, phase)| BackgroundLaunchView {
            request_id: receipt.request_id,
            task_id: receipt.task_id,
            phase,
        }),
    )
}

fn current_launch_record_on(
    conn: &Connection,
    resource: &Resource,
) -> Result<Option<(BackgroundLaunchReceipt, BackgroundLaunchPhase)>, ResourceStoreError> {
    let saved: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_background_launches
             WHERE resource_id = ?1 ORDER BY rowid DESC LIMIT 1",
            [resource.id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(saved) = saved else {
        return Ok(None);
    };
    let receipt: BackgroundLaunchReceipt = serde_json::from_str(&saved)
        .map_err(|error| ResourceStoreError::corrupt("background launch receipt", error))?;
    if receipt.resource_id != resource.id
        || receipt.authority_machine != resource.authority_machine()
    {
        return Ok(Some((receipt, BackgroundLaunchPhase::IdentityMismatch)));
    }
    if latest_loan_on(conn, resource.id)?.map(|loan| loan.id) != receipt.preceding_loan {
        return Ok(Some((receipt, BackgroundLaunchPhase::Superseded)));
    }
    // an operator attestation saved after this launch cleared the task it registered
    if super::operator_release::current_operator_boundary_on(conn, resource)?
        .is_some_and(|boundary| boundary.preceding_launch == Some(receipt.request_id))
    {
        return Ok(Some((receipt, BackgroundLaunchPhase::Superseded)));
    }
    if resource.registered_background_task == Some(receipt.task_id) {
        return Ok(Some((receipt, BackgroundLaunchPhase::Registered)));
    }

    let Some(row) = bound_launch_row_on(conn, &receipt).map_err(|error| match error {
        BackgroundLaunchError::Resource(error) => error,
        BackgroundLaunchError::Storage(error) => ResourceStoreError::Storage(error),
        error => ResourceStoreError::corrupt("background launch records", error),
    })?
    else {
        return Ok(Some((receipt, BackgroundLaunchPhase::IdentityMismatch)));
    };
    let phase = match &row.state {
        TaskState::Queued => BackgroundLaunchPhase::Queued,
        TaskState::Running { .. } => BackgroundLaunchPhase::StartedUnregistered,
        TaskState::Lost | TaskState::Finished { .. } => {
            BackgroundLaunchPhase::EndedBeforeRegistration {
                release: ended_task_release(&row),
            }
        }
    };

    Ok(Some((receipt, phase)))
}

/// Classify the task-layer release evidence of one background task row
///
/// The task layer records that no work started only when it failed or
/// cancelled the row before any worker child or container started, so that
/// evidence with any other outcome proves nothing. A lost or non-terminal row is
/// never released
fn ended_task_release(row: &TaskRow) -> EndedLaunchRelease {
    let TaskState::Finished { reason } = &row.state else {
        return EndedLaunchRelease::Unproven;
    };
    match row.work_exit_evidence() {
        WorkExitEvidence::ProcessGroupExited | WorkExitEvidence::ContainerRemoved { .. } => {
            EndedLaunchRelease::ConfirmedExited
        }
        WorkExitEvidence::NoWorkStarted
            if matches!(
                reason,
                ExitReason::SpawnFailed { .. } | ExitReason::Cancelled
            ) =>
        {
            EndedLaunchRelease::NeverSpawned
        }
        WorkExitEvidence::NoWorkStarted | WorkExitEvidence::Unconfirmed => {
            EndedLaunchRelease::Unproven
        }
    }
}

/// Return the task row when its callback route and accepted identity match the receipt
///
/// A co-located launch keeps its route on the authority. A remote supervisor's
/// route lives on its own machine, so the accepted identity and the first queued
/// event must name that machine as the callback origin
fn bound_launch_row_on(
    conn: &Connection,
    receipt: &BackgroundLaunchReceipt,
) -> Result<Option<TaskRow>, BackgroundLaunchError> {
    let task_id = receipt.task_id;
    let authority = receipt.authority_machine;
    let origin = receipt.origin_machine();
    let Some(row) = crate::store::task_by_id_on(conn, task_id)? else {
        return Ok(None);
    };
    let Some(ExecutorIdentity::Accepted(record)) = executor_identity_on(conn, task_id)? else {
        return Ok(None);
    };
    let Some(spec) = record.current_spec() else {
        return Ok(None);
    };
    let identity_matches = record.is_owned_by(task_id, origin, authority)
        && record.state == row.status()
        && normalized_spec_sha256(spec)? == receipt.normalized_spec_sha256
        && row.thread == spec.thread
        && row.thread == receipt.supervisor.thread;
    if !identity_matches {
        return Ok(None);
    }
    let local_route = origin_route_by_request_on(conn, receipt.request_id)?;
    let route_matches = match receipt.origin {
        BackgroundLaunchOrigin::CoLocated => local_route.is_some_and(|route| {
            route.task == task_id
                && route.origin_machine == authority
                && route.execution_machine == authority
                && route.thread == row.thread
        }),
        BackgroundLaunchOrigin::RemoteSupervisor { .. } => {
            local_route.is_none()
                && origin != authority
                && super::resource_task_row_matches(&row, task_id, spec)
                && crate::store::initial_queued_event_matches_on(conn, task_id, origin, authority)
                    .map_err(|error| {
                    BackgroundLaunchError::TaskRecords(AppError::Internal {
                        message: format!("background launch first event: {error}"),
                    })
                })?
        }
    };

    Ok(route_matches.then_some(row))
}

fn receipt_by_request_on(
    conn: &Connection,
    request_id: RequestId,
) -> Result<Option<BackgroundLaunchReceipt>, BackgroundLaunchError> {
    let saved: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_background_launches WHERE request_id = ?1",
            [request_id.0.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(saved.map(|json| serde_json::from_str(&json)).transpose()?)
}

fn receipt_by_task_on(
    conn: &Connection,
    task_id: TaskId,
) -> Result<Option<BackgroundLaunchReceipt>, BackgroundLaunchError> {
    let saved: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_background_launches WHERE task_id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(saved.map(|json| serde_json::from_str(&json)).transpose()?)
}

/// Return the first background launch that registered one ended trainer
///
/// The receipt, task row, accepted identity, and callback route must all name
/// this resource and authority. The result is the launch request and the digest
/// of the accepted normalized spec
pub(super) fn first_launch_of_registered_trainer_on(
    conn: &Connection,
    resource: &Resource,
    task_id: TaskId,
) -> Result<Option<(RequestId, NormalizedSpecSha256)>, BackgroundLaunchError> {
    let Some(receipt) = receipt_by_task_on(conn, task_id)? else {
        return Ok(None);
    };
    if receipt.task_id != task_id
        || receipt.resource_id != resource.id
        || receipt.authority_machine != resource.authority_machine()
        || bound_launch_row_on(conn, &receipt)?.is_none()
    {
        return Ok(None);
    }

    Ok(Some((receipt.request_id, receipt.normalized_spec_sha256)))
}

/// Return the request identity of the latest first background launch of one resource
pub(super) fn latest_launch_request_on(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<RequestId>, ResourceStoreError> {
    let saved: Option<String> = conn
        .query_row(
            "SELECT request_id FROM resource_background_launches
             WHERE resource_id = ?1 ORDER BY rowid DESC LIMIT 1",
            [resource_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;
    saved
        .map(|id| {
            uuid::Uuid::parse_str(&id).map(RequestId).map_err(|error| {
                ResourceStoreError::corrupt("background launch request identity", error)
            })
        })
        .transpose()
}

pub(super) fn latest_loan_on(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<Loan>, ResourceStoreError> {
    let saved: Option<(String, String)> = conn
        .query_row(
            "SELECT id, state_json FROM loans WHERE resource_id = ?1 ORDER BY rowid DESC LIMIT 1",
            [resource_id.as_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((id, state)) = saved else {
        return Ok(None);
    };
    let id = id
        .parse::<LoanId>()
        .map_err(|error| ResourceStoreError::corrupt("loan identity", error))?;
    let state: LoanState = serde_json::from_str(&state)
        .map_err(|error| ResourceStoreError::corrupt("loan state", error))?;

    Ok(Some(Loan {
        id,
        resource_id,
        state,
    }))
}

pub(super) enum AdvanceError {
    Exhausted,
    Changed,
    Storage(rusqlite::Error),
}

impl From<rusqlite::Error> for AdvanceError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error)
    }
}

/// Advance the revision and set the registration, comparing the saved resource
pub(super) fn advance_resource_on(
    tx: &Connection,
    resource: &Resource,
    registered_background_task: Option<TaskId>,
) -> Result<ResourceRevision, AdvanceError> {
    let next = resource
        .state_revision
        .get()
        .checked_add(1)
        .map(ResourceRevision::new)
        .ok_or(AdvanceError::Exhausted)?;
    let sql_integer = |value: u64| i64::try_from(value).map_err(|_| AdvanceError::Exhausted);
    // SQLite cannot hold a larger assignment, so no saved row can match the compare
    let Ok(assignment_revision) = i64::try_from(resource.assignment_revision.get()) else {
        return Err(AdvanceError::Changed);
    };
    let changed = tx.execute(
        "UPDATE resources SET state_revision = ?1, registered_background_task = ?2
         WHERE id = ?3 AND authority_machine = ?4 AND state_revision = ?5
           AND supervisor_machine = ?6 AND supervisor_thread = ?7
           AND assignment_revision = ?8 AND registered_background_task IS ?9",
        params![
            sql_integer(next.get())?,
            registered_background_task.map(|task| task.to_string()),
            resource.id.as_uuid().to_string(),
            resource.authority_machine().as_uuid().to_string(),
            sql_integer(resource.state_revision.get())?,
            resource.supervisor.machine.as_uuid().to_string(),
            resource.supervisor.thread.to_string(),
            assignment_revision,
            resource
                .registered_background_task
                .map(|task| task.to_string()),
        ],
    )?;
    if changed != 1 {
        return Err(AdvanceError::Changed);
    }

    Ok(next)
}
