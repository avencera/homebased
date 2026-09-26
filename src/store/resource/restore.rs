//! Supervisor return decisions and the fixed-identity restore that follows a drained queue
//!
//! An AwaitingReturn loan is the scheduling boundary. Only the exact current
//! supervisor assignment can decide it, and each decision commits its loan
//! transition, task records, resource revision, and replay receipt in one
//! IMMEDIATE transaction. The accepting transaction also saves the task's typed
//! execution mode. A direct-segment trainer closes its Restoring loan on a
//! confirmed start and becomes the registered background task. A native
//! foreground or container task keeps the loan reserved while it runs and closes
//! it only after a successful end with its confirmed exit witness: a
//! process-group exit, or a removed container. Any other end needs an explicit
//! supervisor resolution

use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use super::background::{AdvanceError, advance_resource_on};
use super::trainer_association::trainer_association_by_task;
use super::trainer_lock::{
    HeldTrainerRelease, TrainerLockReleaseError, TrainerLockReleaseGap, hold_released_trainer_lock,
};
use super::{LoanActionPhase, replace_loan_in_action_phase_on, select_authority_resource};
use crate::domain::{
    ContainerId, ExitReason, ProcessStatus, TaskEnv, TaskId, TaskRow, TaskState, WorkExitEvidence,
};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::bound_action::{ActionTaskReceipt, ResourceActionKind};
use crate::resource::command_shape::{DirectSegmentCommandShape, same_run_resume_command};
use crate::resource::foreground::{self, CommandOwnershipContract};
use crate::resource::store::{
    ResourceStoreError, release_checkpoint_state_for_action, release_completion_for_loan,
    return_window_on, save_held_return_window_on, select_non_closed_loan, select_resource,
    select_supervisor_notice_record_by_action,
};
use crate::resource::trainer_publication::revalidate_checkpoint_publication;
use crate::resource::{
    ActionId, Loan, LoanClosure, LoanId, LoanPhase, LoanState, ReleaseCheckpointPhase, Resource,
    ResourceId, ResourceRevision, ResourceTaskOwnershipRisk, RestoreAttentionReason, ReturnContext,
    ReturnDecision, ReturnDecisionRejection, ReturnDecisionWindow, ReturnExecutionMode,
    ReturnHoldRejection, ReturnLaunch, ReturnWork, SameRunResumeGap, SupervisorActionAuthority,
    SupervisorAddress, SupervisorNoticePayload,
};
use crate::spec::{NormalizedSpec, NormalizedTaskWorkload, NormalizedWorkload};
use crate::store::IdentityError;
use crate::store::identity::{executor_identity_on, origin_route_by_request_on};
use crate::submission::{
    CallbackContext, CallbackExecutable, ExecutorIdentity, NormalizedSpecSha256, RequestId,
    normalized_spec_sha256,
};

/// Loan closed by one return decision or restore reconciliation
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReturnClosure {
    /// Closed loan with its retained return context
    pub(crate) loan: Loan,
    /// Resource revision committed with the closure
    pub(crate) state_revision: ResourceRevision,
}

/// Fixed identities, typed work, and executor context for one return launch
#[derive(Debug, Clone)]
pub(crate) struct ReturnTaskAcceptanceInput {
    /// Exact supervisor authority for the pending return action
    pub(crate) authority: SupervisorActionAuthority,
    /// Fixed request and task identities with the chosen work
    pub(crate) launch: ReturnLaunch,
    /// Executor environment for a supervisor-supplied command
    pub(crate) executor_env: TaskEnv,
    /// Where the supervisor callback route lives
    pub(crate) origin: ReturnTaskOrigin,
}

/// Canonical return task derived for one pending decision
#[derive(Debug, Clone)]
pub(crate) struct PreparedReturnTask {
    /// Spec the remote supervisor saves in its route
    pub(crate) spec: NormalizedSpec,
    /// Digest of `spec`
    pub(crate) normalized_spec_sha256: NormalizedSpecSha256,
}

/// Owner of the callback route for one return task
#[derive(Debug, Clone)]
pub(crate) enum ReturnTaskOrigin {
    /// The supervisor thread runs on the authority, so the route commits with the task
    Local {
        /// Codex executable that delivers the supervisor callback
        callback_codex: CallbackExecutable,
    },
    /// The supervisor machine saved the route and sent the prepared spec digest
    Remote {
        /// Digest of the spec that the supervisor saved in its route
        normalized_spec_sha256: NormalizedSpecSha256,
    },
}

/// Result of binding one fixed return task to its action
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReturnTaskAcceptance {
    /// The task records, Restoring loan, and receipt committed in this transaction
    Inserted {
        /// Restoring loan that keeps the resource reserved
        loan: Box<Loan>,
        /// Bound task identity
        task: TaskId,
        /// Resource revision committed with the binding
        state_revision: ResourceRevision,
    },
    /// An exact earlier binding exists, and its current task state is returned
    Existing {
        /// Bound task identity
        task: TaskId,
        /// State retained by the task layer
        state: ProcessStatus,
    },
    /// The supervisor thread runs on another machine, so no task record was written
    UnsupportedRemoteSupervisor {
        /// Machine that owns the resource
        authority_machine: MachineId,
        /// Supervisor that would need a remote callback route
        supervisor: SupervisorAddress,
    },
}

/// Supervisor resolution for a return task that ended before a confirmed start
#[derive(Debug, Clone)]
pub(crate) struct EndedRestoreResolution {
    /// Exact supervisor authority for the Restoring loan
    pub(crate) authority: SupervisorActionAuthority,
    /// Bound task that ended
    pub(crate) task_id: TaskId,
    /// Supervisor's durable resolution reason
    pub(crate) reason: String,
}

/// Authority observation of one Restoring loan
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RestoreReconcileOutcome {
    /// The resource has no Restoring loan
    NotRestoring,
    /// The bound task row is queued and has not reached its running boundary
    Queued {
        /// Restoring loan that keeps the resource reserved
        loan: Loan,
        /// Return action bound to the task
        action_id: ActionId,
        /// Bound task
        task_id: TaskId,
    },
    /// The bound direct-segment task has a confirmed start, so the loan closed with it registered
    Closed {
        /// Closed loan and committed revision
        closure: ReturnClosure,
        /// Task now registered as the resource background task
        task_id: TaskId,
    },
    /// The bound native foreground task is running, so the loan keeps the resource reserved
    ForegroundRunning {
        /// Restoring loan that keeps the resource reserved
        loan: Loan,
        /// Return action bound to the task
        action_id: ActionId,
        /// Bound task
        task_id: TaskId,
    },
    /// The bound native foreground task ended successfully with a confirmed
    /// process-group exit, so the loan closed and no task is registered
    ForegroundEnded {
        /// Closed loan and committed revision
        closure: ReturnClosure,
        /// Native foreground task that ended
        task_id: TaskId,
    },
    /// The loan stays reserved because the bound task needs attention
    Attention {
        /// Restoring loan that keeps the resource reserved
        loan: Loan,
        /// Return action bound to the task
        action_id: ActionId,
        /// Bound task
        task_id: TaskId,
        /// Authority-classified reason
        reason: RestoreAttentionReason,
    },
}

/// A return decision, restore reconciliation, or resolution failed
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReturnDecisionError {
    /// Resource authority or stored resource data failed validation
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// The typed decision does not fit the saved action
    #[error(transparent)]
    Rejected(#[from] ReturnDecisionRejection),
    /// The hold cannot move the deadline of the saved action
    #[error(transparent)]
    HoldRejected(#[from] ReturnHoldRejection),
    /// The decision does not come from the current supervisor assignment
    #[error("return decision is not from the current supervisor assignment")]
    NotCurrentSupervisor,
    /// The loan does not have this pending return action
    #[error("return action {action_id:?} is not pending on loan {loan_id:?}")]
    ActionNotPending {
        /// Loan named by the decision
        loan_id: LoanId,
        /// Action named by the decision
        action_id: ActionId,
    },
    /// The saved return notice does not match the loan action
    #[error("return action {action_id:?} has an invalid durable notice")]
    InvalidReturnNotice {
        /// Action named by the decision
        action_id: ActionId,
    },
    /// The caller's expected revision does not match the saved state
    #[error("stale resource revision: expected {expected:?}, found {actual:?}")]
    StaleRevision {
        /// Revision supplied by the caller
        expected: ResourceRevision,
        /// Revision saved by the authority
        actual: ResourceRevision,
    },
    /// The registered background task does not match the return context or is still live
    #[error("registered background task does not match the return context")]
    BackgroundTaskMismatch,
    /// The bound return task has not reached a terminal state
    #[error("return task {task_id} has not ended")]
    RestoreNotEnded {
        /// Bound task
        task_id: TaskId,
    },
    /// The bound return task ended without proof that its process released the resource
    #[error("return task {task_id} ended without proven process release")]
    RestoreReleaseUnproven {
        /// Bound task
        task_id: TaskId,
    },
    /// The bound direct-segment return task's wrapper exited, but its trainer lock is not proven free
    ///
    /// The worker runs in its own session, so only the exact lock named by a
    /// trainer-attempt association, held through the closing transaction, proves
    /// that the worker released the GPU
    #[error("return task {task_id} has no verified trainer lock release: {gap}")]
    RestoreOwnershipUnproven {
        /// Bound task
        task_id: TaskId,
        /// Missing or failed part of the lock proof
        gap: TrainerLockReleaseGap,
    },
    /// A saved receipt holds a different decision for the same action
    #[error("return action {action_id:?} was retried with a different decision")]
    ConflictingRetry {
        /// Action named by the decision
        action_id: ActionId,
    },
    /// The derived return spec differs from the spec the remote supervisor saved
    #[error("return task spec differs from the prepared spec")]
    SpecMismatch,
    /// The callback origin does not match where the assigned supervisor runs
    #[error("return task origin does not match the supervisor machine")]
    OriginMismatch,
    /// The fixed request or task identity already belongs to other records
    #[error("return task identity {task_id} is already used")]
    IdentityConflict {
        /// Task named by the decision
        task_id: TaskId,
    },
    /// The resource revision cannot be incremented
    #[error("resource revision {revision:?} cannot be incremented")]
    RevisionExhausted {
        /// Current resource revision
        revision: ResourceRevision,
    },
    /// Route or executor identity data is invalid
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// Task preparation or task record insertion failed
    #[error("return task records failed: {0}")]
    TaskRecords(#[from] AppError),
    /// A typed receipt could not be encoded or decoded
    #[error("return decision encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    /// SQLite failed
    #[error("return decision storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Saved result of one supervisor return decision
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum SavedReturnResult {
    /// The no-resume decision closed the loan
    Closed {
        loan: Loan,
        state_revision: ResourceRevision,
    },
    /// The launch decision bound one task and entered Restoring
    RestoreBound {
        loan: Loan,
        request_id: RequestId,
        task_id: TaskId,
        normalized_spec_sha256: NormalizedSpecSha256,
        state_revision: ResourceRevision,
        /// Mode fixed by the validated ownership contract at acceptance
        execution_mode: ReturnExecutionMode,
    },
}

/// Replay receipt for one supervisor return decision
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReturnDecisionReceipt {
    authority: SupervisorActionAuthority,
    decision: ReturnDecision,
    result: SavedReturnResult,
}

/// Evidence that closed one Restoring loan
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RestoreClosureBasis {
    /// The authority observed the bound direct-segment task in its running state
    ConfirmedRunning,
    /// The bound native foreground task ended successfully, and the task layer
    /// confirmed that its owned process group exited
    ForegroundEnded { outcome: ExitReason },
    /// The bound container task exited with code 0, and the worker removed the
    /// exact container and confirmed that its ID is absent
    ContainerEnded {
        outcome: ExitReason,
        container_id: ContainerId,
    },
    /// The supervisor explicitly accepted an end that had proven process release
    SupervisorResolvedEnd {
        authority: SupervisorActionAuthority,
        reason: String,
        outcome: ExitReason,
    },
}

/// Replay receipt for one Restoring loan closure
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreClosureReceipt {
    resource_id: ResourceId,
    loan_id: LoanId,
    action_id: ActionId,
    task_id: TaskId,
    basis: RestoreClosureBasis,
    loan: Loan,
    state_revision: ResourceRevision,
}

impl ReturnDecisionReceipt {
    /// Execution mode of a bound launch, or `None` for a no-resume closure
    fn execution_mode(&self) -> Option<ReturnExecutionMode> {
        match &self.result {
            SavedReturnResult::RestoreBound { execution_mode, .. } => Some(*execution_mode),
            SavedReturnResult::Closed { .. } => None,
        }
    }
}

/// Read the accepted mode only when a saved decision still proves the current Restoring loan
pub(crate) fn current_return_execution_mode_on(
    conn: &Connection,
    resource: &Resource,
    loan: Option<&Loan>,
) -> Result<Option<ReturnExecutionMode>, ReturnDecisionError> {
    let Some(loan) = loan else {
        return Ok(None);
    };
    let LoanState::Active {
        phase:
            LoanPhase::Restoring {
                action_id,
                resume_task_id,
                ..
            },
    } = &loan.state
    else {
        return Ok(None);
    };

    let Some(receipt) = saved_decision(conn, *action_id)? else {
        return Ok(None);
    };
    let ReturnDecision::Launch(launch) = &receipt.decision else {
        return Ok(None);
    };
    let SavedReturnResult::RestoreBound {
        loan: bound_loan,
        request_id,
        task_id,
        normalized_spec_sha256,
        execution_mode,
        ..
    } = &receipt.result
    else {
        return Ok(None);
    };
    if bound_loan != loan
        || *task_id != *resume_task_id
        || launch.request_id != *request_id
        || launch.task_id != *task_id
        || receipt.authority.action_id != *action_id
        || receipt.authority.loan_id != loan.id
        || receipt.authority.resource_id != resource.id
        || receipt.authority.authority_machine != resource.authority_machine()
    {
        return Ok(None);
    }
    if bound_restore_task(
        conn,
        &receipt.authority,
        *request_id,
        *task_id,
        *normalized_spec_sha256,
    )?
    .is_none()
    {
        return Ok(None);
    }

    Ok(Some(*execution_mode))
}

/// Bound return task with the evidence its reconciliation needs
struct BoundRestoreTask {
    row: TaskRow,
    execution_mode: ReturnExecutionMode,
    /// Stopped run that a same-run resume continues in the same runtime root
    resumed_run: Option<TaskId>,
}

/// AwaitingReturn state that the exact current supervisor may decide
struct PendingReturn {
    resource: Resource,
    loan: Loan,
    return_context: ReturnContext,
}

impl crate::store::Store {
    /// Close one exact AwaitingReturn loan without starting background work
    pub(crate) fn record_no_resume_for_authority(
        &mut self,
        authority: SupervisorActionAuthority,
        reason: String,
    ) -> Result<ReturnClosure, ReturnDecisionError> {
        record_no_resume_for_authority(&mut self.conn, authority, reason)
    }

    /// Move the decision deadline of one exact pending return action
    pub(crate) fn hold_return_for_authority(
        &mut self,
        authority: SupervisorActionAuthority,
        hold: Duration,
    ) -> Result<ReturnDecisionWindow, ReturnDecisionError> {
        hold_return_for_authority(&mut self.conn, authority, hold, Utc::now())
    }

    /// Derive the canonical return task for one pending decision without binding it
    pub(crate) fn prepare_return_task_for_authority(
        &mut self,
        authority: SupervisorActionAuthority,
        launch: ReturnLaunch,
        executor_env: TaskEnv,
    ) -> Result<PreparedReturnTask, ReturnDecisionError> {
        prepare_return_task_for_authority(&mut self.conn, authority, launch, executor_env)
    }

    /// Bind one fixed return task and enter Restoring on this store connection
    pub(crate) fn accept_return_task_for_authority(
        &mut self,
        input: ReturnTaskAcceptanceInput,
    ) -> Result<ReturnTaskAcceptance, ReturnDecisionError> {
        accept_return_task_for_authority(&mut self.conn, input)
    }

    /// Observe one Restoring loan and close it only on a confirmed start
    pub(crate) fn reconcile_restoring_loan_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<RestoreReconcileOutcome, ReturnDecisionError> {
        reconcile_restoring_loan_for_authority(&mut self.conn, authority_machine, resource_id)
    }

    /// Close a Restoring loan after the supervisor resolves an early end
    pub(crate) fn resolve_ended_restore_for_authority(
        &mut self,
        resolution: EndedRestoreResolution,
    ) -> Result<ReturnClosure, ReturnDecisionError> {
        resolve_ended_restore_for_authority(&mut self.conn, resolution)
    }

    /// Read the task identities bound by Restoring loans on this authority
    pub(crate) fn restoring_task_ids_for_authority(
        &self,
        authority_machine: MachineId,
    ) -> Result<Vec<TaskId>, ReturnDecisionError> {
        restoring_task_ids_for_authority(&self.conn, authority_machine)
    }
}

/// Move the decision deadline of one exact AwaitingReturn action
///
/// A hold is not a decision: it changes neither the loan nor the resource
/// revision, so the supervisor decides later with the same pending action
fn hold_return_for_authority(
    conn: &mut Connection,
    authority: SupervisorActionAuthority,
    hold: Duration,
    now: DateTime<Utc>,
) -> Result<ReturnDecisionWindow, ReturnDecisionError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let pending = pending_return(&tx, &authority)?;
    let window = return_window_on(&tx, authority.action_id)?
        .filter(|window| window.loan_id == pending.loan.id)
        .ok_or(ReturnDecisionError::ActionNotPending {
            loan_id: authority.loan_id,
            action_id: authority.action_id,
        })?;
    let held = window.hold(now, hold)?;
    save_held_return_window_on(&tx, &window, &held)?;
    tx.commit()?;
    Ok(held)
}

/// Close one exact AwaitingReturn loan without starting background work
fn record_no_resume_for_authority(
    conn: &mut Connection,
    authority: SupervisorActionAuthority,
    reason: String,
) -> Result<ReturnClosure, ReturnDecisionError> {
    let decision = ReturnDecision::NoResume { reason };
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(receipt) = saved_decision(&tx, authority.action_id)? {
        let SavedReturnResult::Closed {
            loan,
            state_revision,
        } = replayed_result(receipt, &authority, &decision)?
        else {
            return Err(ReturnDecisionError::ConflictingRetry {
                action_id: authority.action_id,
            });
        };
        tx.commit()?;
        return Ok(ReturnClosure {
            loan,
            state_revision,
        });
    }

    let pending = pending_return(&tx, &authority)?;
    decision.validate_for(&pending.return_context, authority.supervisor.thread)?;
    require_prior_background_ended(&tx, &pending)?;
    let ReturnDecision::NoResume { reason } = &decision else {
        unreachable!("the no-resume decision was built above");
    };

    let loan = Loan {
        id: pending.loan.id,
        resource_id: pending.loan.resource_id,
        state: LoanState::Closed {
            result: LoanClosure::NoResume {
                return_context: pending.return_context.clone(),
                reason: reason.clone(),
            },
        },
    };
    // neither the stopped nor the completed run is live, and no work replaces it
    let state_revision = advance_resource(&tx, &pending.resource, None)?;
    update_active_loan(
        &tx,
        &loan,
        LoanActionPhase::AwaitingReturn,
        authority.action_id,
    )?;
    insert_decision_receipt(
        &tx,
        &ReturnDecisionReceipt {
            authority,
            decision: decision.clone(),
            result: SavedReturnResult::Closed {
                loan: loan.clone(),
                state_revision,
            },
        },
    )?;
    tx.commit()?;

    Ok(ReturnClosure {
        loan,
        state_revision,
    })
}

/// Derive the canonical return task for one pending decision without binding it
///
/// A remote supervisor saves this exact spec in its callback route before it
/// sends the launch. A decision already bound returns the spec it accepted
fn prepare_return_task_for_authority(
    conn: &mut Connection,
    authority: SupervisorActionAuthority,
    launch: ReturnLaunch,
    executor_env: TaskEnv,
) -> Result<PreparedReturnTask, ReturnDecisionError> {
    let decision = ReturnDecision::Launch(Box::new(launch.clone()));
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    if let Some(receipt) = saved_decision(&tx, authority.action_id)? {
        let SavedReturnResult::RestoreBound { task_id, .. } =
            replayed_result(receipt, &authority, &decision)?
        else {
            return Err(ReturnDecisionError::ConflictingRetry {
                action_id: authority.action_id,
            });
        };
        let Some(ExecutorIdentity::Accepted(record)) = executor_identity_on(&tx, task_id)? else {
            return Err(ReturnDecisionError::IdentityConflict { task_id });
        };
        let spec = record
            .current_spec()
            .cloned()
            .ok_or(ReturnDecisionError::IdentityConflict { task_id })?;
        let normalized_spec_sha256 = normalized_spec_sha256(&spec)?;
        return Ok(PreparedReturnTask {
            spec,
            normalized_spec_sha256,
        });
    }

    let pending = pending_return(&tx, &authority)?;
    decision.validate_for(&pending.return_context, authority.supervisor.thread)?;
    require_prior_background_ended(&tx, &pending)?;
    let (spec, _, _) = return_task_spec(&tx, &pending, &authority, &launch, executor_env)?;
    crate::spec::check_cwd(&spec.cwd)?;
    crate::spec::check_workload_host(&spec.workload)?;
    let normalized_spec_sha256 = normalized_spec_sha256(&spec)?;
    Ok(PreparedReturnTask {
        spec,
        normalized_spec_sha256,
    })
}

/// Derive the spec, executor environment, and binary for one return launch
fn return_task_spec(
    conn: &Connection,
    pending: &PendingReturn,
    authority: &SupervisorActionAuthority,
    launch: &ReturnLaunch,
    executor_env: TaskEnv,
) -> Result<(NormalizedSpec, TaskEnv, std::path::PathBuf), ReturnDecisionError> {
    let Some(spec) = launch.work.supervisor_spec() else {
        return same_run_resume(conn, pending, authority.supervisor.thread);
    };
    let spec = spec.as_normalized().clone();
    let binary =
        crate::invocation::resolve_workload_binary(&spec.workload, &executor_env.path, &spec.cwd)?;
    Ok((spec, executor_env, binary))
}

/// Check the return task row against the ownership contract its work selected
///
/// A foreground command needs an inspectable native entry point. A trainer argv
/// must pass the full direct-segment check, because only that shape has an
/// ownership lock that later release proof can verify. A container needs no
/// entry point: Homebased drives the `docker` CLI itself and removes the
/// container. The validated contract fixes the task's execution mode
fn check_return_command_ownership(
    launch: &ReturnLaunch,
    spec: &NormalizedSpec,
    row: &TaskRow,
) -> Result<ReturnExecutionMode, ReturnDecisionError> {
    // the resume argv comes from the stopped run's verified trainer shape
    if matches!(launch.work, ReturnWork::SameRunResume { .. }) {
        return Ok(ReturnExecutionMode::DirectSegmentTrainer);
    }
    let unsupported = |risk| {
        ReturnDecisionError::Rejected(ReturnDecisionRejection::UnsupportedCommandOwnership { risk })
    };
    let contract =
        CommandOwnershipContract::for_return_work(&spec.workload).map_err(unsupported)?;
    match contract {
        CommandOwnershipContract::ForegroundExecutable => {
            foreground::inspect_foreground_entry_point(&row.binary).map_err(unsupported)?;
        }
        CommandOwnershipContract::DirectSegmentTrainer => {
            DirectSegmentCommandShape::validate_launch(spec, row)
                .map_err(|_| unsupported(ResourceTaskOwnershipRisk::Interpreter))?;
        }
        CommandOwnershipContract::Container => {}
    }

    Ok(contract.into())
}

/// Bind one fixed return task to an exact AwaitingReturn action and enter Restoring
///
/// The task row, accepted executor identity, first queued event, Restoring phase,
/// resource revision, and receipt commit together. A local origin also commits its
/// callback route; a remote origin commits the exact action-task receipt instead,
/// because its route was saved on the supervisor machine. The caller may spawn a
/// worker only for an `Inserted` result
fn accept_return_task_for_authority(
    conn: &mut Connection,
    input: ReturnTaskAcceptanceInput,
) -> Result<ReturnTaskAcceptance, ReturnDecisionError> {
    let ReturnTaskAcceptanceInput {
        authority,
        launch,
        executor_env,
        origin,
    } = input;
    let decision = ReturnDecision::Launch(Box::new(launch.clone()));
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(receipt) = saved_decision(&tx, authority.action_id)? {
        let SavedReturnResult::RestoreBound {
            request_id,
            task_id,
            normalized_spec_sha256,
            ..
        } = replayed_result(receipt, &authority, &decision)?
        else {
            return Err(ReturnDecisionError::ConflictingRetry {
                action_id: authority.action_id,
            });
        };
        if let ReturnTaskOrigin::Remote {
            normalized_spec_sha256: requested,
        } = &origin
            && *requested != normalized_spec_sha256
        {
            return Err(ReturnDecisionError::SpecMismatch);
        }
        let row = bound_restore_task(&tx, &authority, request_id, task_id, normalized_spec_sha256)?
            .ok_or(ReturnDecisionError::IdentityConflict { task_id })?;
        tx.commit()?;
        return Ok(ReturnTaskAcceptance::Existing {
            task: task_id,
            state: row.status(),
        });
    }

    let pending = pending_return(&tx, &authority)?;
    decision.validate_for(&pending.return_context, authority.supervisor.thread)?;
    let remote_supervisor = authority.supervisor.machine != authority.authority_machine;
    match &origin {
        // a local route for a remote supervisor thread would send callbacks to the
        // wrong machine, so the local path writes nothing for it
        ReturnTaskOrigin::Local { .. } if remote_supervisor => {
            return Ok(ReturnTaskAcceptance::UnsupportedRemoteSupervisor {
                authority_machine: authority.authority_machine,
                supervisor: authority.supervisor,
            });
        }
        ReturnTaskOrigin::Remote { .. } if !remote_supervisor => {
            return Err(ReturnDecisionError::OriginMismatch);
        }
        ReturnTaskOrigin::Local { .. } | ReturnTaskOrigin::Remote { .. } => {}
    }
    require_prior_background_ended(&tx, &pending)?;

    let task_id = launch.task_id;
    let (spec, env, binary) = return_task_spec(&tx, &pending, &authority, &launch, executor_env)?;
    if super::task_identity_is_used(&tx, launch.request_id, task_id)? {
        return Err(ReturnDecisionError::IdentityConflict { task_id });
    }

    crate::spec::check_cwd(&spec.cwd)?;
    crate::spec::check_workload_host(&spec.workload)?;
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
    let execution_mode = check_return_command_ownership(&launch, &spec, &row)?;
    match origin {
        ReturnTaskOrigin::Local { callback_codex } => {
            let callback = CallbackContext {
                env,
                cwd: spec.cwd.clone(),
                codex: callback_codex,
            };
            crate::store::insert_local_task_records_on(
                &tx,
                &row,
                &spec,
                authority.authority_machine,
                launch.request_id,
                &callback,
                None,
            )
            .map_err(|error| match error {
                AppError::ClusterTaskConflict { .. } => {
                    ReturnDecisionError::IdentityConflict { task_id }
                }
                error => ReturnDecisionError::TaskRecords(error),
            })?;
        }
        ReturnTaskOrigin::Remote {
            normalized_spec_sha256: requested,
        } => {
            if requested != normalized_spec_sha256(&spec)? {
                return Err(ReturnDecisionError::SpecMismatch);
            }
            let receipt = ActionTaskReceipt {
                kind: ResourceActionKind::Return,
                authority,
                request_id: launch.request_id,
                task_id,
                normalized_spec_sha256: requested,
            };
            super::action_task::insert_action_task(&tx, &row, &spec, &receipt).map_err(
                |error| match error {
                    super::action_task::ResourceActionError::Rejected(_) => {
                        ReturnDecisionError::IdentityConflict { task_id }
                    }
                    super::action_task::ResourceActionError::Storage(error) => {
                        ReturnDecisionError::TaskRecords(error)
                    }
                },
            )?;
        }
    }

    let loan = Loan {
        id: pending.loan.id,
        resource_id: pending.loan.resource_id,
        state: LoanState::Active {
            phase: LoanPhase::Restoring {
                action_id: authority.action_id,
                return_context: pending.return_context.clone(),
                resume_task_id: task_id,
            },
        },
    };
    // the prior task stays registered until a direct-segment task has a confirmed
    // start or the loan closes without registering its foreground task
    let state_revision = advance_resource(
        &tx,
        &pending.resource,
        pending.resource.registered_background_task,
    )?;
    update_active_loan(
        &tx,
        &loan,
        LoanActionPhase::AwaitingReturn,
        authority.action_id,
    )?;
    insert_decision_receipt(
        &tx,
        &ReturnDecisionReceipt {
            authority,
            decision,
            result: SavedReturnResult::RestoreBound {
                loan: loan.clone(),
                request_id: launch.request_id,
                task_id,
                normalized_spec_sha256: normalized_spec_sha256(&spec)?,
                state_revision,
                execution_mode,
            },
        },
    )?;
    tx.commit()?;

    Ok(ReturnTaskAcceptance::Inserted {
        loan: Box::new(loan),
        task: task_id,
        state_revision,
    })
}

/// Observe one Restoring loan and close it by the task's saved execution mode
///
/// A direct-segment task closes the loan on a confirmed start and becomes the
/// registered background task. A native foreground or container task keeps the
/// loan while it runs and closes it only after a successful end with its
/// confirmed exit witness, leaving no registered task. Every other state keeps
/// the loan reserved
fn reconcile_restoring_loan_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
) -> Result<RestoreReconcileOutcome, ReturnDecisionError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let resource = select_authority_resource(&tx, authority_machine, resource_id)?;
    let Some(loan) = select_non_closed_loan(&tx, resource_id)? else {
        return Ok(RestoreReconcileOutcome::NotRestoring);
    };
    let LoanState::Active {
        phase:
            LoanPhase::Restoring {
                action_id,
                return_context,
                resume_task_id,
            },
    } = &loan.state
    else {
        return Ok(RestoreReconcileOutcome::NotRestoring);
    };
    let (action_id, task_id) = (*action_id, *resume_task_id);
    let attention = |reason| {
        Ok(RestoreReconcileOutcome::Attention {
            loan: loan.clone(),
            action_id,
            task_id,
            reason,
        })
    };

    let Some(bound) = restoring_task_row(&tx, &resource, &loan, action_id, task_id)? else {
        return attention(RestoreAttentionReason::IdentityMismatch);
    };
    let (closed, registered, basis) = match (&bound.row.state, bound.execution_mode) {
        (TaskState::Queued, _) => {
            return Ok(RestoreReconcileOutcome::Queued {
                loan: loan.clone(),
                action_id,
                task_id,
            });
        }
        (TaskState::Lost, _) => return attention(RestoreAttentionReason::Lost),
        (TaskState::Running { .. }, ReturnExecutionMode::DirectSegmentTrainer) => (
            LoanClosure::Resumed {
                return_context: return_context.clone(),
                task_id,
            },
            Some(task_id),
            RestoreClosureBasis::ConfirmedRunning,
        ),
        (TaskState::Finished { .. }, ReturnExecutionMode::DirectSegmentTrainer) => {
            return attention(RestoreAttentionReason::EndedBeforeConfirmedStart {
                state: bound.row.status(),
            });
        }
        (
            TaskState::Running { .. },
            ReturnExecutionMode::NativeForeground | ReturnExecutionMode::Container,
        ) => {
            return Ok(RestoreReconcileOutcome::ForegroundRunning {
                loan: loan.clone(),
                action_id,
                task_id,
            });
        }
        (TaskState::Finished { reason }, mode) => {
            let basis = match loan_holding_end(&bound.row, reason, mode) {
                Ok(basis) => basis,
                Err(reason) => return attention(reason),
            };
            (
                LoanClosure::ForegroundReturnEnded {
                    return_context: return_context.clone(),
                    task_id,
                    outcome: reason.clone(),
                },
                None,
                basis,
            )
        }
    };

    let closed = Loan {
        id: loan.id,
        resource_id,
        state: LoanState::Closed { result: closed },
    };
    // a foreground end also clears the ended prior registration, so the queue
    // can serve its next request from this closure's idle boundary
    let state_revision = advance_resource(&tx, &resource, registered)?;
    update_active_loan(&tx, &closed, LoanActionPhase::Restoring, action_id)?;
    insert_closure_receipt(
        &tx,
        &RestoreClosureReceipt {
            resource_id,
            loan_id: loan.id,
            action_id,
            task_id,
            basis,
            loan: closed.clone(),
            state_revision,
        },
    )?;
    tx.commit()?;

    let closure = ReturnClosure {
        loan: closed,
        state_revision,
    };
    Ok(match registered {
        Some(task_id) => RestoreReconcileOutcome::Closed { closure, task_id },
        None => RestoreReconcileOutcome::ForegroundEnded { closure, task_id },
    })
}

/// Return the closure basis of a return task that held its loan, or why its end cannot close it
///
/// Only a zero exit with the witness of the task's mode is free: a confirmed
/// owned process-group exit for a native command, or a removed container for a
/// container. A failed end needs the supervisor's explicit resolution, and an
/// unconfirmed witness is not proof that the work released the GPU
fn loan_holding_end(
    row: &TaskRow,
    reason: &ExitReason,
    mode: ReturnExecutionMode,
) -> Result<RestoreClosureBasis, RestoreAttentionReason> {
    let state = row.status();
    let success = *reason == ExitReason::Exit { code: 0 };
    let evidence = row.work_exit_evidence();
    match (mode, evidence) {
        (ReturnExecutionMode::NativeForeground, WorkExitEvidence::ProcessGroupExited)
            if success =>
        {
            Ok(RestoreClosureBasis::ForegroundEnded {
                outcome: reason.clone(),
            })
        }
        (
            ReturnExecutionMode::Container,
            WorkExitEvidence::ContainerRemoved { container_id, .. },
        ) if success => Ok(RestoreClosureBasis::ContainerEnded {
            outcome: reason.clone(),
            container_id,
        }),
        (
            ReturnExecutionMode::NativeForeground,
            WorkExitEvidence::ProcessGroupExited | WorkExitEvidence::NoWorkStarted,
        ) => Err(RestoreAttentionReason::ForegroundEnded { state }),
        (ReturnExecutionMode::NativeForeground, _) => {
            Err(RestoreAttentionReason::ForegroundExitUnconfirmed { state })
        }
        (
            ReturnExecutionMode::Container,
            WorkExitEvidence::ContainerRemoved { .. } | WorkExitEvidence::NoWorkStarted,
        ) => Err(RestoreAttentionReason::ContainerEnded { state }),
        (ReturnExecutionMode::Container, _) => {
            Err(RestoreAttentionReason::ContainerExitUnconfirmed { state })
        }
        (ReturnExecutionMode::DirectSegmentTrainer, _) => {
            Err(RestoreAttentionReason::EndedBeforeConfirmedStart { state })
        }
    }
}

/// Close a Restoring loan whose bound task ended without a mode-specific closure
///
/// This covers a direct-segment task that ended before a confirmed start, and a
/// native foreground or container task that ended without success. The exact
/// current supervisor must name the action, task, and current revision. The task
/// must be terminal with the proven release its mode needs; a lost task stays
/// reserved
fn resolve_ended_restore_for_authority(
    conn: &mut Connection,
    resolution: EndedRestoreResolution,
) -> Result<ReturnClosure, ReturnDecisionError> {
    let EndedRestoreResolution {
        authority,
        task_id,
        reason,
    } = resolution;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(receipt) = saved_closure(&tx, authority.action_id)? {
        let RestoreClosureBasis::SupervisorResolvedEnd {
            authority: saved_authority,
            reason: saved_reason,
            ..
        } = &receipt.basis
        else {
            return Err(ReturnDecisionError::ActionNotPending {
                loan_id: authority.loan_id,
                action_id: authority.action_id,
            });
        };
        if *saved_authority != authority || *saved_reason != reason || receipt.task_id != task_id {
            return Err(ReturnDecisionError::ConflictingRetry {
                action_id: authority.action_id,
            });
        }
        tx.commit()?;
        return Ok(ReturnClosure {
            loan: receipt.loan,
            state_revision: receipt.state_revision,
        });
    }

    if reason.trim().is_empty() {
        return Err(ReturnDecisionRejection::EmptyReason.into());
    }
    let resource = current_supervisor_resource(&tx, &authority)?;
    let not_pending = || ReturnDecisionError::ActionNotPending {
        loan_id: authority.loan_id,
        action_id: authority.action_id,
    };
    let loan = select_non_closed_loan(&tx, authority.resource_id)?
        .filter(|loan| loan.id == authority.loan_id)
        .ok_or_else(not_pending)?;
    let LoanState::Active {
        phase:
            LoanPhase::Restoring {
                action_id,
                return_context,
                resume_task_id,
            },
    } = &loan.state
    else {
        return Err(not_pending());
    };
    if *action_id != authority.action_id || *resume_task_id != task_id {
        return Err(not_pending());
    }
    if resource.state_revision != authority.expected_state_revision {
        return Err(ReturnDecisionError::StaleRevision {
            expected: authority.expected_state_revision,
            actual: resource.state_revision,
        });
    }

    let bound = restoring_task_row(&tx, &resource, &loan, *action_id, task_id)?
        .ok_or(ReturnDecisionError::IdentityConflict { task_id })?;
    let row = &bound.row;
    let outcome = match &row.state {
        TaskState::Finished { reason } => reason.clone(),
        TaskState::Lost => return Err(ReturnDecisionError::RestoreReleaseUnproven { task_id }),
        TaskState::Queued | TaskState::Running { .. } => {
            return Err(ReturnDecisionError::RestoreNotEnded { task_id });
        }
    };
    // the task layer records no started work only before its child spawns or its
    // container starts, so the other outcomes need the exit witness of the task's
    // mode, and a trainer also needs its released lock
    let held_lock = match (row.work_exit_evidence(), bound.execution_mode) {
        (
            WorkExitEvidence::ProcessGroupExited,
            ReturnExecutionMode::NativeForeground | ReturnExecutionMode::DirectSegmentTrainer,
        ) => hold_restore_release(&tx, &resource, &bound)?,
        (WorkExitEvidence::ContainerRemoved { .. }, ReturnExecutionMode::Container) => None,
        (WorkExitEvidence::NoWorkStarted, _)
            if matches!(
                outcome,
                ExitReason::SpawnFailed { .. } | ExitReason::Cancelled
            ) =>
        {
            None
        }
        _ => return Err(ReturnDecisionError::RestoreReleaseUnproven { task_id }),
    };

    let closed = Loan {
        id: loan.id,
        resource_id: loan.resource_id,
        state: LoanState::Closed {
            result: LoanClosure::RestoreEnded {
                return_context: return_context.clone(),
                task_id,
                outcome: outcome.clone(),
                reason: reason.clone(),
            },
        },
    };
    let state_revision = advance_resource(&tx, &resource, None)?;
    update_active_loan(
        &tx,
        &closed,
        LoanActionPhase::Restoring,
        authority.action_id,
    )?;
    insert_closure_receipt(
        &tx,
        &RestoreClosureReceipt {
            resource_id: loan.resource_id,
            loan_id: loan.id,
            action_id: authority.action_id,
            task_id,
            basis: RestoreClosureBasis::SupervisorResolvedEnd {
                authority,
                reason,
                outcome,
            },
            loan: closed.clone(),
            state_revision,
        },
    )?;
    if let Some(held) = &held_lock {
        held.recheck()
            .map_err(|gap| ReturnDecisionError::RestoreOwnershipUnproven { task_id, gap })?;
    }
    tx.commit()?;
    // the closure lets new GPU work start, so the lock stays held until it commits
    drop(held_lock);

    Ok(ReturnClosure {
        loan: closed,
        state_revision,
    })
}

/// Prove that a return task whose wrapper exited released the GPU
///
/// The saved execution mode selects the witness. A native foreground command
/// needs only its confirmed process-group exit. A direct-segment trainer needs
/// its exact released lock. A same-run resume keeps the stopped run's runtime
/// root, so that run's association names the lock; other trainer work has no
/// association before a confirmed start and fails closed
fn hold_restore_release(
    conn: &Connection,
    resource: &Resource,
    bound: &BoundRestoreTask,
) -> Result<Option<HeldTrainerRelease>, ReturnDecisionError> {
    let task_id = bound.row.id;
    let mode = bound.execution_mode;
    if mode.holds_loan_while_running() {
        return Ok(None);
    }
    let witness_task = bound.resumed_run.unwrap_or(task_id);

    hold_released_trainer_lock(conn, resource, task_id, witness_task)
        .map(Some)
        .map_err(|error| match error {
            TrainerLockReleaseError::Unproven(gap) => {
                ReturnDecisionError::RestoreOwnershipUnproven { task_id, gap }
            }
            TrainerLockReleaseError::Store(error) => ReturnDecisionError::Resource(error),
        })
}

/// Read the task identities bound by Restoring loans on this authority
fn restoring_task_ids_for_authority(
    conn: &Connection,
    authority_machine: MachineId,
) -> Result<Vec<TaskId>, ReturnDecisionError> {
    let mut statement = conn.prepare(
        "SELECT json_extract(l.state_json, '$.phase.resume_task_id')
         FROM loans l JOIN resources r ON r.id = l.resource_id
         WHERE r.authority_machine = ?1
           AND json_extract(l.state_json, '$.type') = 'active'
           AND json_extract(l.state_json, '$.phase.type') = 'restoring'",
    )?;
    let ids = statement
        .query_map([authority_machine.as_uuid().to_string()], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    ids.into_iter()
        .map(|id| {
            id.parse().map_err(|_| {
                ReturnDecisionError::TaskRecords(AppError::Internal {
                    message: format!("restoring loan has an invalid task identity {id}"),
                })
            })
        })
        .collect()
}

fn current_supervisor_resource(
    conn: &Connection,
    authority: &SupervisorActionAuthority,
) -> Result<Resource, ReturnDecisionError> {
    if authority.supervisor.thread.0.is_nil() {
        return Err(ReturnDecisionRejection::InvalidIdentity.into());
    }
    let resource =
        select_authority_resource(conn, authority.authority_machine, authority.resource_id)?;
    if resource.supervisor != authority.supervisor
        || resource.assignment_revision != authority.assignment_revision
    {
        return Err(ReturnDecisionError::NotCurrentSupervisor);
    }

    Ok(resource)
}

fn pending_return(
    conn: &Connection,
    authority: &SupervisorActionAuthority,
) -> Result<PendingReturn, ReturnDecisionError> {
    let resource = current_supervisor_resource(conn, authority)?;
    let not_pending = || ReturnDecisionError::ActionNotPending {
        loan_id: authority.loan_id,
        action_id: authority.action_id,
    };
    let loan = select_non_closed_loan(conn, authority.resource_id)?
        .filter(|loan| loan.id == authority.loan_id)
        .ok_or_else(not_pending)?;
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingReturn {
                action_id,
                return_context,
            },
    } = &loan.state
    else {
        return Err(not_pending());
    };
    if *action_id != authority.action_id {
        return Err(not_pending());
    }

    let (notice, _) = select_supervisor_notice_record_by_action(conn, authority.action_id)
        .map_err(ResourceStoreError::from)?
        .ok_or(ReturnDecisionError::InvalidReturnNotice {
            action_id: authority.action_id,
        })?;
    if notice.loan_id != loan.id
        || notice.payload
            != (SupervisorNoticePayload::ReturnRequired {
                return_context: return_context.clone(),
            })
    {
        return Err(ReturnDecisionError::InvalidReturnNotice {
            action_id: authority.action_id,
        });
    }
    // the reservation revision is the decision boundary; queued requests accepted
    // after it do not change the revision, and only the expired decision window
    // lets one of them supersede this action
    for actual in [notice.state_revision, resource.state_revision] {
        if actual != authority.expected_state_revision {
            return Err(ReturnDecisionError::StaleRevision {
                expected: authority.expected_state_revision,
                actual,
            });
        }
    }

    let return_context = return_context.clone();
    Ok(PendingReturn {
        resource,
        loan,
        return_context,
    })
}

// the return context names the run that released the resource; it must still be
// the registered task and must not be live before anything replaces it
fn require_prior_background_ended(
    conn: &Connection,
    pending: &PendingReturn,
) -> Result<(), ReturnDecisionError> {
    let registered = pending.resource.registered_background_task;
    let Some(task_id) = pending.return_context.released_task() else {
        return match registered {
            None => Ok(()),
            Some(_) => Err(ReturnDecisionError::BackgroundTaskMismatch),
        };
    };
    if registered != Some(task_id) {
        return Err(ReturnDecisionError::BackgroundTaskMismatch);
    }
    let row = crate::store::task_by_id_on(conn, task_id)?
        .ok_or(ReturnDecisionError::BackgroundTaskMismatch)?;
    // only an operator attestation releases a lost trainer, and it saved the lost context
    let ended = match &pending.return_context {
        ReturnContext::LostWithoutResult { .. } => row.state.is_terminal(),
        ReturnContext::Stopped { .. }
        | ReturnContext::AlreadyCompleted { .. }
        | ReturnContext::EndedWithoutResult { .. }
        | ReturnContext::Idle => matches!(row.state, TaskState::Finished { .. }),
    };
    if !ended {
        return Err(ReturnDecisionError::BackgroundTaskMismatch);
    }

    Ok(())
}

/// Return the direct-segment return launch that registered one ended trainer
///
/// The closure receipt must record the confirmed start that registered the
/// task, and the bound decision must name this resource, a direct-segment
/// execution mode, and the task's matching accepted identity and callback owner
/// The result is the return action, its request, and the accepted spec digest
pub(super) fn direct_segment_return_of_registered_trainer_on(
    conn: &Connection,
    resource: &Resource,
    task_id: TaskId,
) -> Result<Option<(ActionId, RequestId, NormalizedSpecSha256)>, ReturnDecisionError> {
    let closure: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_restore_closures WHERE task_id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(closure) = closure else {
        return Ok(None);
    };
    let closure: RestoreClosureReceipt = serde_json::from_str(&closure)?;
    if closure.task_id != task_id
        || closure.resource_id != resource.id
        || !matches!(closure.basis, RestoreClosureBasis::ConfirmedRunning)
    {
        return Ok(None);
    }
    let Some(decision) = saved_decision(conn, closure.action_id)? else {
        return Ok(None);
    };
    let SavedReturnResult::RestoreBound {
        request_id,
        task_id: bound_task,
        normalized_spec_sha256,
        ..
    } = &decision.result
    else {
        return Ok(None);
    };
    let (request_id, digest) = (*request_id, *normalized_spec_sha256);
    if *bound_task != task_id
        || decision.authority.resource_id != resource.id
        || decision.authority.authority_machine != resource.authority_machine()
        || decision.execution_mode() != Some(ReturnExecutionMode::DirectSegmentTrainer)
        || bound_restore_task(conn, &decision.authority, request_id, task_id, digest)?.is_none()
    {
        return Ok(None);
    }

    Ok(Some((closure.action_id, request_id, digest)))
}

/// Return the return task of one execution mode that one Restoring loan still binds
///
/// The saved decision must bind this exact task to the loan and action with
/// `mode` as its accepted execution mode, and the task's accepted identity and
/// callback owner must still match. The result is the return request and the
/// accepted spec digest. A decision whose mode cannot be proven never matches
pub(super) fn restoring_return_of_mode_on(
    conn: &Connection,
    resource: &Resource,
    loan: &Loan,
    action_id: ActionId,
    task_id: TaskId,
    mode: ReturnExecutionMode,
) -> Result<Option<(RequestId, NormalizedSpecSha256)>, ReturnDecisionError> {
    let Some(decision) = saved_decision(conn, action_id)? else {
        return Ok(None);
    };
    let SavedReturnResult::RestoreBound {
        loan: bound_loan,
        request_id,
        task_id: bound_task,
        normalized_spec_sha256,
        ..
    } = &decision.result
    else {
        return Ok(None);
    };
    let (request_id, digest) = (*request_id, *normalized_spec_sha256);
    if *bound_task != task_id
        || bound_loan.id != loan.id
        || decision.authority.action_id != action_id
        || decision.authority.loan_id != loan.id
        || decision.authority.resource_id != resource.id
        || decision.authority.authority_machine != resource.authority_machine()
        || decision.execution_mode() != Some(mode)
        || bound_restore_task(conn, &decision.authority, request_id, task_id, digest)?.is_none()
    {
        return Ok(None);
    }

    Ok(Some((request_id, digest)))
}

/// Derive the resume command from the stopped run's saved records
///
/// The verified release receipt, committed checkpoint decision, trainer
/// association, accepted identity, and task row must all name the same run
/// Only the callback thread and `--resume` generation differ from that run
fn same_run_resume(
    conn: &Connection,
    pending: &PendingReturn,
    supervisor_thread: crate::domain::ThreadId,
) -> Result<(NormalizedSpec, TaskEnv, std::path::PathBuf), ReturnDecisionError> {
    let ReturnContext::Stopped {
        task_id,
        checkpoint_ref,
        recovery_ref,
    } = &pending.return_context
    else {
        return Err(ReturnDecisionRejection::ResumeRequiresStoppedContext.into());
    };
    let unproven = |gap| ReturnDecisionError::from(ReturnDecisionRejection::ResumeUnproven { gap });
    let resource = &pending.resource;

    let (release_action, released_context) =
        release_completion_for_loan(conn, resource.id, pending.loan.id)?
            .ok_or_else(|| unproven(SameRunResumeGap::ReleaseReceiptMissing))?;
    if released_context != pending.return_context {
        return Err(unproven(SameRunResumeGap::ReleaseReceiptMissing));
    }
    let (checkpoint_state, _) =
        release_checkpoint_state_for_action(conn, resource.id, release_action)
            .ok()
            .flatten()
            .ok_or_else(|| unproven(SameRunResumeGap::CheckpointDecisionMismatch))?;
    let ReleaseCheckpointPhase::CancellationCommitted { decision, .. } = checkpoint_state.phase
    else {
        return Err(unproven(SameRunResumeGap::CheckpointDecisionMismatch));
    };
    let checkpoint = &decision.selected_checkpoint;
    if checkpoint_state.action.observed_background_task != *task_id
        || checkpoint.generation_id != *recovery_ref
        || *checkpoint_ref
            != format!(
                "{}#sha256={}",
                checkpoint.path.display(),
                checkpoint.record_sha256
            )
    {
        return Err(unproven(SameRunResumeGap::CheckpointDecisionMismatch));
    }

    let association = trainer_association_by_task(conn, *task_id)
        .ok()
        .flatten()
        .filter(|association| {
            association.resource_id() == resource.id
                && association.authority_machine() == resource.authority_machine()
        })
        .ok_or_else(|| unproven(SameRunResumeGap::AssociationMissing))?;
    let Some(ExecutorIdentity::Accepted(record)) = executor_identity_on(conn, *task_id)? else {
        return Err(unproven(SameRunResumeGap::RunRecordsChanged));
    };
    let original = record
        .current_spec()
        .ok_or_else(|| unproven(SameRunResumeGap::RunRecordsChanged))?;
    let row = crate::store::task_by_id_on(conn, *task_id)?
        .ok_or_else(|| unproven(SameRunResumeGap::RunRecordsChanged))?;
    if !record.is_executed_by(*task_id, resource.authority_machine())
        || normalized_spec_sha256(original)? != association.normalized_spec_sha256()
        || !super::resource_task_row_matches(&row, *task_id, original)
        || !matches!(
            row.state,
            TaskState::Finished {
                reason: ExitReason::Cancelled
            }
        )
    {
        return Err(unproven(SameRunResumeGap::RunRecordsChanged));
    }

    let attempt = association.verified_attempt();
    if !matches!(
        revalidate_checkpoint_publication(
            attempt.canonical_runtime_root(),
            attempt.binding(),
            checkpoint
        ),
        Ok(true)
    ) {
        return Err(unproven(SameRunResumeGap::CheckpointUnavailable));
    }

    let NormalizedWorkload::Task(workload) = &original.workload else {
        return Err(unproven(SameRunResumeGap::CommandShapeInvalid));
    };
    let command = same_run_resume_command(&workload.command, recovery_ref)
        .map_err(|_| unproven(SameRunResumeGap::CommandShapeInvalid))?;
    let spec = NormalizedSpec {
        api_version: original.api_version,
        thread: supervisor_thread,
        name: original.name.clone(),
        cwd: row.cwd.clone(),
        machine: None,
        timeout: original.timeout,
        workload: NormalizedWorkload::Task(NormalizedTaskWorkload { command }),
    };
    let binary =
        crate::invocation::resolve_workload_binary(&spec.workload, &row.env.path, &row.cwd)
            .map_err(|_| unproven(SameRunResumeGap::InterpreterChanged))?;
    if binary != row.binary {
        return Err(unproven(SameRunResumeGap::InterpreterChanged));
    }

    Ok((spec, row.env, binary))
}

/// Return the bound task when its receipt, route, and accepted identity match
fn restoring_task_row(
    conn: &Connection,
    resource: &Resource,
    loan: &Loan,
    action_id: ActionId,
    task_id: TaskId,
) -> Result<Option<BoundRestoreTask>, ReturnDecisionError> {
    let Some(receipt) = saved_decision(conn, action_id)? else {
        return Ok(None);
    };
    let ReturnDecision::Launch(launch) = &receipt.decision else {
        return Ok(None);
    };
    let resumed_run = match &launch.work {
        ReturnWork::SameRunResume { stopped_task, .. } => Some(*stopped_task),
        ReturnWork::EvaluationOrNextEpoch { .. }
        | ReturnWork::NewBackgroundWork { .. }
        | ReturnWork::AfterEndedRun { .. } => None,
    };
    let SavedReturnResult::RestoreBound {
        request_id,
        task_id: bound_task,
        normalized_spec_sha256,
        execution_mode,
        ..
    } = receipt.result
    else {
        return Ok(None);
    };
    if bound_task != task_id
        || receipt.authority.loan_id != loan.id
        || receipt.authority.resource_id != resource.id
        || receipt.authority.authority_machine != resource.authority_machine()
    {
        return Ok(None);
    }

    let row = bound_restore_task(
        conn,
        &receipt.authority,
        request_id,
        task_id,
        normalized_spec_sha256,
    )?;
    Ok(row.map(|row| BoundRestoreTask {
        row,
        execution_mode,
        resumed_run,
    }))
}

/// Return the bound task row when its identity and callback owner match the decision
///
/// The deciding supervisor's machine is the callback origin. A local origin keeps
/// its route on the authority; a remote origin keeps the exact action-task receipt
fn bound_restore_task(
    conn: &Connection,
    authority: &SupervisorActionAuthority,
    request_id: RequestId,
    task_id: TaskId,
    digest: NormalizedSpecSha256,
) -> Result<Option<TaskRow>, ReturnDecisionError> {
    let authority_machine = authority.authority_machine;
    if authority.supervisor.machine != authority_machine {
        let expected = ActionTaskReceipt {
            kind: ResourceActionKind::Return,
            authority: *authority,
            request_id,
            task_id,
            normalized_spec_sha256: digest,
        };
        let saved = super::action_task::action_task_receipt_by_task(conn, task_id)?;
        if saved != Some(expected) {
            return Ok(None);
        }
        return Ok(super::action_task::remote_action_task_row_on(
            conn, &expected,
        )?);
    }
    let Some(row) = crate::store::task_by_id_on(conn, task_id)? else {
        return Ok(None);
    };
    let Some(ExecutorIdentity::Accepted(record)) = executor_identity_on(conn, task_id)? else {
        return Ok(None);
    };
    let Some(route) = origin_route_by_request_on(conn, request_id)? else {
        return Ok(None);
    };
    let spec_matches = record
        .current_spec()
        .map(normalized_spec_sha256)
        .transpose()?
        .is_some_and(|saved| saved == digest);

    Ok(
        (record.is_owned_by(task_id, authority_machine, authority_machine)
            && record.state == row.status()
            && spec_matches
            && route.task == task_id
            && route.origin_machine == authority_machine
            && route.execution_machine == authority_machine
            && route.thread == row.thread)
            .then_some(row),
    )
}

fn replayed_result(
    receipt: ReturnDecisionReceipt,
    authority: &SupervisorActionAuthority,
    decision: &ReturnDecision,
) -> Result<SavedReturnResult, ReturnDecisionError> {
    if receipt.authority != *authority || receipt.decision != *decision {
        return Err(ReturnDecisionError::ConflictingRetry {
            action_id: authority.action_id,
        });
    }

    Ok(receipt.result)
}

/// Advance the resource revision under the shared compare-and-set
pub(super) fn advance_resource(
    tx: &Transaction<'_>,
    resource: &Resource,
    registered_background_task: Option<TaskId>,
) -> Result<ResourceRevision, ReturnDecisionError> {
    match advance_resource_on(tx, resource, registered_background_task) {
        Ok(next) => Ok(next),
        Err(AdvanceError::Exhausted) => Err(ReturnDecisionError::RevisionExhausted {
            revision: resource.state_revision,
        }),
        Err(AdvanceError::Changed) => {
            let actual = select_resource(tx, resource.id)?
                .map_or(resource.state_revision, |saved| saved.state_revision);
            Err(ReturnDecisionError::StaleRevision {
                expected: resource.state_revision,
                actual,
            })
        }
        Err(AdvanceError::Storage(error)) => Err(error.into()),
    }
}

fn update_active_loan(
    tx: &Transaction<'_>,
    loan: &Loan,
    phase: LoanActionPhase,
    action_id: ActionId,
) -> Result<(), ReturnDecisionError> {
    if !replace_loan_in_action_phase_on(tx, loan, phase, action_id)? {
        return Err(ReturnDecisionError::ActionNotPending {
            loan_id: loan.id,
            action_id,
        });
    }

    Ok(())
}

fn saved_decision(
    conn: &Connection,
    action_id: ActionId,
) -> Result<Option<ReturnDecisionReceipt>, ReturnDecisionError> {
    let receipt: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_return_decisions WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(receipt
        .map(|json| serde_json::from_str(&json))
        .transpose()?)
}

fn insert_decision_receipt(
    tx: &Transaction<'_>,
    receipt: &ReturnDecisionReceipt,
) -> Result<(), ReturnDecisionError> {
    tx.execute(
        "INSERT INTO resource_return_decisions (action_id, resource_id, loan_id, receipt_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            receipt.authority.action_id.as_uuid().to_string(),
            receipt.authority.resource_id.as_uuid().to_string(),
            receipt.authority.loan_id.as_uuid().to_string(),
            serde_json::to_string(receipt)?,
        ],
    )?;
    Ok(())
}

fn saved_closure(
    conn: &Connection,
    action_id: ActionId,
) -> Result<Option<RestoreClosureReceipt>, ReturnDecisionError> {
    let receipt: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM resource_restore_closures WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(receipt
        .map(|json| serde_json::from_str(&json))
        .transpose()?)
}

fn insert_closure_receipt(
    tx: &Transaction<'_>,
    receipt: &RestoreClosureReceipt,
) -> Result<(), ReturnDecisionError> {
    tx.execute(
        "INSERT INTO resource_restore_closures (action_id, task_id, receipt_json)
         VALUES (?1, ?2, ?3)",
        params![
            receipt.action_id.as_uuid().to_string(),
            receipt.task_id.to_string(),
            serde_json::to_string(receipt)?,
        ],
    )?;
    Ok(())
}
