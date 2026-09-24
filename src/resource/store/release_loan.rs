//! Queue reconciliation and release-loan opening

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use super::checkpoint::{ReleaseCheckpointError, insert_release_checkpoint_state};
use super::codec::encode_json;
use super::error::ResourceStoreError;
use super::notice::{
    SupervisorNoticeStoreError, insert_supervisor_notice_in_transaction,
    select_supervisor_notice_record_by_action,
};
use super::queue::next_queued_request_for_authority;
use super::revision::swap_resource_revision;
use super::rows::{check_authority, select_non_closed_loan, select_resource};
use crate::domain::{ProcessGroupExitEvidence, ProcessStatus, TaskId};
use crate::machine::MachineId;
use crate::resource::{
    ActionId, IdleBoundaryDecision, Loan, LoanId, LoanPhase, LoanState, NoticeId,
    ReleaseCheckpointAction, ReleaseCheckpointPhase, ReleaseCheckpointState, ResourceId,
    ResourceQueueAttentionReason, ResourceQueueReconcileOutcome, ResourceRevision,
    SupervisorNotice, SupervisorNoticeDelivery, SupervisorNoticePayload,
};

/// Outcome of opening a release loan for one queued resource request
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenReleaseLoanResult {
    /// A new release loan and its durable supervisor notice were committed
    Opened {
        /// Loan created for the resource interruption
        loan: Loan,
        /// Notice saved in the same transaction as the loan
        notice: SupervisorNotice,
    },
    /// The existing release action and its saved notice were returned unchanged
    AlreadyAwaitingRelease {
        /// Existing active loan for this resource
        loan: Loan,
        /// Saved notice for the existing release action
        notice: SupervisorNotice,
    },
}

/// Failure to read or open the authority-owned queue state
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResourceQueueReconcileError {
    /// Resource authority or stored resource data failed validation
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// Opening a release action failed
    #[error(transparent)]
    Release(#[from] OpenReleaseLoanError),
    /// SQLite failed during queue reconciliation
    #[error("resource queue reconciliation storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// A release loan could not be opened for the current resource state
#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenReleaseLoanError {
    /// Resource authority or stored resource data failed validation
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// The saved release notice could not be read or inserted
    #[error(transparent)]
    Notice(#[from] SupervisorNoticeStoreError),
    /// The fresh action checkpoint state could not be persisted atomically
    #[error(transparent)]
    Checkpoint(#[from] ReleaseCheckpointError),
    /// The caller's expected revision does not match the resource state
    #[error("stale resource revision: expected {expected:?}, found {actual:?}")]
    StaleRevision {
        /// Resource revision supplied by the caller
        expected: ResourceRevision,
        /// Current resource revision in SQLite
        actual: ResourceRevision,
    },
    /// No request remains queued for this resource
    #[error("resource has no queued request")]
    NoQueuedRequest,
    /// The resource does not have a registered background task
    #[error("resource has no registered background task")]
    BackgroundTaskNotRegistered,
    /// The registered background task has no local task row
    #[error("registered background task {task_id} is missing locally")]
    BackgroundTaskMissing {
        /// Exact task registered on the resource
        task_id: TaskId,
    },
    /// The registered background task is not in the local running state
    #[error("registered background task {task_id} is not running (state {state})")]
    BackgroundTaskNotRunning {
        /// Exact task registered on the resource
        task_id: TaskId,
        /// Process state observed in the local task table
        state: String,
    },
    /// Another active or attention-needed loan already owns this resource
    #[error("resource already has a non-closed loan {loan:?}")]
    ExistingLoan {
        /// Existing non-closed loan and its typed state
        loan: Box<Loan>,
    },
    /// A saved AwaitingRelease action has no durable notice
    #[error("awaiting-release loan {loan_id:?} has no notice for action {action_id:?}")]
    MissingReleaseNotice {
        /// Existing loan that owns the release action
        loan_id: LoanId,
        /// Stable release action identity
        action_id: ActionId,
    },
    /// A saved release notice does not match the existing loan action
    #[error("saved release notice does not match loan {loan_id:?} action {action_id:?}")]
    InvalidReleaseNotice {
        /// Existing loan that owns the release action
        loan_id: LoanId,
        /// Stable release action identity
        action_id: ActionId,
    },
    /// The resource state revision cannot be incremented
    #[error("resource revision {revision:?} cannot be incremented")]
    RevisionExhausted {
        /// Current resource revision
        revision: ResourceRevision,
    },
    /// SQLite failed during the release transaction
    #[error("release loan storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Open one release action and persist its supervisor notice atomically
pub(super) fn open_release_loan_in_transaction(
    tx: Transaction<'_>,
    authority_machine: MachineId,
    resource_id: ResourceId,
    expected_state_revision: ResourceRevision,
) -> Result<OpenReleaseLoanResult, OpenReleaseLoanError> {
    let resource =
        select_resource(&tx, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_authority(resource.authority_machine(), authority_machine)?;

    // check issued actions before queue readiness because cancellation cannot revoke them
    if let Some(loan) = select_non_closed_loan(&tx, resource_id)? {
        let LoanState::Active {
            phase:
                LoanPhase::AwaitingRelease {
                    action_id,
                    observed_background_task,
                    ..
                },
        } = &loan.state
        else {
            return Err(OpenReleaseLoanError::ExistingLoan {
                loan: Box::new(loan),
            });
        };

        let Some((notice, _)) = select_supervisor_notice_record_by_action(&tx, *action_id)
            .map_err(ResourceStoreError::from)?
        else {
            return Err(OpenReleaseLoanError::MissingReleaseNotice {
                loan_id: loan.id,
                action_id: *action_id,
            });
        };
        if notice.loan_id != loan.id
            || notice.payload
                != (SupervisorNoticePayload::ReleaseRequired {
                    task_id: *observed_background_task,
                })
        {
            return Err(OpenReleaseLoanError::InvalidReleaseNotice {
                loan_id: loan.id,
                action_id: *action_id,
            });
        }

        // retries retain the original expected revision after the opening increments it
        if expected_state_revision.next() != Some(notice.state_revision)
            || resource.state_revision != notice.state_revision
        {
            return Err(OpenReleaseLoanError::StaleRevision {
                expected: expected_state_revision,
                actual: resource.state_revision,
            });
        }

        tx.commit()?;
        return Ok(OpenReleaseLoanResult::AlreadyAwaitingRelease { loan, notice });
    }

    if resource.state_revision != expected_state_revision {
        return Err(OpenReleaseLoanError::StaleRevision {
            expected: expected_state_revision,
            actual: resource.state_revision,
        });
    }

    if next_queued_request_for_authority(&tx, authority_machine, resource_id)?.is_none() {
        return Err(OpenReleaseLoanError::NoQueuedRequest);
    }

    let task_id = resource
        .registered_background_task
        .ok_or(OpenReleaseLoanError::BackgroundTaskNotRegistered)?;
    match observe_registered_background_on(&tx, resource_id, task_id)? {
        RegisteredBackgroundObservation::Missing => {
            return Err(OpenReleaseLoanError::BackgroundTaskMissing { task_id });
        }
        RegisteredBackgroundObservation::Running
        | RegisteredBackgroundObservation::EndedCandidate => {}
        RegisteredBackgroundObservation::NotReleasable { state } => {
            return Err(OpenReleaseLoanError::BackgroundTaskNotRunning { task_id, state });
        }
    }

    let next_revision =
        expected_state_revision
            .next()
            .ok_or(OpenReleaseLoanError::RevisionExhausted {
                revision: expected_state_revision,
            })?;
    let action_id = ActionId::new();
    let loan = Loan {
        id: LoanId::new(),
        resource_id,
        state: LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task: task_id,
                watcher_intent: None,
            },
        },
    };
    let state_json = encode_json(&loan.state)?;
    tx.execute(
        "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
        params![
            loan.id.as_uuid().to_string(),
            resource_id.as_uuid().to_string(),
            state_json,
        ],
    )?;

    let swapped = swap_resource_revision::<OpenReleaseLoanError>(
        &tx,
        authority_machine,
        resource_id,
        expected_state_revision,
        next_revision,
    )?;
    if !swapped {
        let actual = select_resource(&tx, resource_id)?
            .map_or(resource.state_revision, |saved| saved.state_revision);
        return Err(OpenReleaseLoanError::StaleRevision {
            expected: expected_state_revision,
            actual,
        });
    }

    let notice = SupervisorNotice {
        id: NoticeId::new(),
        loan_id: loan.id,
        action_id,
        state_revision: next_revision,
        destination: resource.supervisor,
        assignment_revision: resource.assignment_revision,
        payload: SupervisorNoticePayload::ReleaseRequired { task_id },
        delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
    };
    insert_supervisor_notice_in_transaction(&tx, &notice)?;
    insert_release_checkpoint_state(
        &tx,
        &ReleaseCheckpointState {
            action: ReleaseCheckpointAction {
                resource_id,
                action_id,
                state_revision: next_revision,
                observed_background_task: task_id,
            },
            phase: ReleaseCheckpointPhase::WatcherBindingPending,
        },
    )?;
    tx.commit()?;

    Ok(OpenReleaseLoanResult::Opened { loan, notice })
}

/// Reconcile the next queued request using only authority-owned state
pub(crate) fn reconcile_resource_queue_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
) -> Result<ResourceQueueReconcileOutcome, ResourceQueueReconcileError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let resource =
        select_resource(&tx, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    check_authority(resource.authority_machine(), authority_machine)?;

    if let Some(loan) = select_non_closed_loan(&tx, resource_id)? {
        tx.commit()?;
        return Ok(ResourceQueueReconcileOutcome::LoanAlreadyActive { loan });
    }

    // a first background launch with a confirmed start becomes the registered
    // task before any queue decision reads the registration
    let resource = match crate::store::promote_started_background_launch_on(&tx, &resource)? {
        Some(promoted) => promoted,
        None => resource,
    };

    let Some(request) = next_queued_request_for_authority(&tx, authority_machine, resource_id)?
    else {
        tx.commit()?;
        return Ok(ResourceQueueReconcileOutcome::NoQueuedRequest);
    };

    if let Some(task_id) = crate::store::pending_background_launch_on(&tx, &resource)? {
        tx.commit()?;
        return Ok(ResourceQueueReconcileOutcome::AttentionRequired {
            request,
            reason: ResourceQueueAttentionReason::BackgroundLaunchPending { task_id },
        });
    }

    let Some(task_id) = resource.registered_background_task else {
        let outcome = match crate::store::idle_boundary_decision_on(&tx, &resource)? {
            IdleBoundaryDecision::Proven(proof) => {
                let (loan, request) = crate::store::open_idle_serving_loan_on(
                    &tx,
                    authority_machine,
                    &resource,
                    request,
                    proof.clone(),
                )?;
                ResourceQueueReconcileOutcome::IdleServing {
                    loan,
                    request,
                    proof,
                }
            }
            IdleBoundaryDecision::Unproven(gap) => {
                ResourceQueueReconcileOutcome::AttentionRequired {
                    request,
                    reason: ResourceQueueAttentionReason::IdleNotProven { gap },
                }
            }
        };
        tx.commit()?;
        return Ok(outcome);
    };

    match observe_registered_background_on(&tx, resource_id, task_id)? {
        RegisteredBackgroundObservation::Missing => {
            tx.commit()?;
            Ok(ResourceQueueReconcileOutcome::AttentionRequired {
                request,
                reason: ResourceQueueAttentionReason::BackgroundTaskMissing { task_id },
            })
        }
        // an ended trainer opens the same release action as a running one, so
        // only the authority release proof can serve the queue from it
        RegisteredBackgroundObservation::Running
        | RegisteredBackgroundObservation::EndedCandidate => {
            let outcome = open_release_loan_in_transaction(
                tx,
                authority_machine,
                resource_id,
                resource.state_revision,
            )?;
            match outcome {
                OpenReleaseLoanResult::Opened { loan, notice }
                | OpenReleaseLoanResult::AlreadyAwaitingRelease { loan, notice } => {
                    Ok(ResourceQueueReconcileOutcome::ReleaseRequired { loan, notice })
                }
            }
        }
        RegisteredBackgroundObservation::NotReleasable { state } => {
            tx.commit()?;
            Ok(ResourceQueueReconcileOutcome::AttentionRequired {
                request,
                reason: ResourceQueueAttentionReason::BackgroundTaskNotRunning { task_id, state },
            })
        }
    }
}

/// Authority view of the registered background task when queued work needs the resource
#[derive(Debug, Clone, PartialEq, Eq)]
enum RegisteredBackgroundObservation {
    /// The registered task has no authority task row
    Missing,
    /// The task is running, so a watcher must stop it at a checkpoint
    Running,
    /// The task succeeded, failed, or was cancelled, its process-group exit is
    /// confirmed, and a trainer attempt association is saved
    ///
    /// These facts do not release the resource. They only let the release action
    /// open, and the authority release proof must still verify the released
    /// ownership lock, and any result or stop evidence, before it serves the queue
    EndedCandidate,
    /// The task ended without the facts a release proof needs
    ///
    /// A lost or unconfirmed end, or a missing association, can leave trainer
    /// work alive, so the queue stays blocked for an owner
    NotReleasable {
        /// Durable task status observed by the authority
        state: String,
    },
}

fn observe_registered_background_on(
    conn: &Connection,
    resource_id: ResourceId,
    task_id: TaskId,
) -> Result<RegisteredBackgroundObservation, ResourceStoreError> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM tasks WHERE id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(status) = status else {
        return Ok(RegisteredBackgroundObservation::Missing);
    };
    match ProcessStatus::from_storage(&status).ok() {
        Some(ProcessStatus::Running) => return Ok(RegisteredBackgroundObservation::Running),
        // a lost task never has a confirmed exit, so it is not a candidate
        Some(ProcessStatus::Succeeded | ProcessStatus::Failed | ProcessStatus::Cancelled)
            if ended_release_candidate(conn, resource_id, task_id)? =>
        {
            return Ok(RegisteredBackgroundObservation::EndedCandidate);
        }
        _ => {}
    }

    Ok(RegisteredBackgroundObservation::NotReleasable { state: status })
}

/// Check the saved facts that an ended-trainer release proof needs before it can run
fn ended_release_candidate(
    conn: &Connection,
    resource_id: ResourceId,
    task_id: TaskId,
) -> Result<bool, ResourceStoreError> {
    let evidence: Option<String> = conn.query_row(
        "SELECT process_group_exit_evidence FROM tasks WHERE id = ?1",
        [task_id.to_string()],
        |row| row.get(0),
    )?;
    if ProcessGroupExitEvidence::from_storage(evidence.as_deref())
        != ProcessGroupExitEvidence::ConfirmedExited
    {
        return Ok(false);
    }

    Ok(conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM trainer_attempt_associations
            WHERE resource_id = ?1 AND task_id = ?2
        )",
        params![resource_id.as_uuid().to_string(), task_id.to_string()],
        |row| row.get(0),
    )?)
}
