//! A registered trainer that ended without a usable result or a saved checkpoint stop
//!
//! Only its exact trainer-attempt association, a confirmed exit, and the exact
//! saved lock held free through the transition can release the resource. The
//! release names the outcome and never permits a same-run resume

use super::fixtures::{
    TrainerAssociationFixture, release_completion_fixture, saved_trainer_association_json, spec,
};
use crate::domain::{ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId};
use crate::machine::MachineId;
use crate::resource::ownership_lock::TrainerRequestDigest;
use crate::resource::store::{
    CompleteReleaseError, ReleaseCompletionResult, ResourceSnapshot, ResourceTaskAcceptance,
    ResourceTaskAcceptanceInput,
};
use crate::resource::{
    ActionId, CommandSpec, LoanClosure, LoanPhase, LoanState, ResourceQueueReconcileOutcome,
    ResourceRequest, ResourceRequestState, ResourceRevision, ReturnContext,
    ReturnDecisionRejection, ReturnLaunch, ReturnWork, ServingReleaseProvenance,
    SupervisorActionAuthority, SupervisorNoticePayload,
};
use crate::store::{ReturnDecisionError, Store};
use crate::submission::{RequestId, normalized_spec_sha256};

fn queue(fixture: &mut TrainerAssociationFixture) -> ResourceRequest {
    fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            spec(),
        )
        .unwrap()
}

fn fail_trainer(
    fixture: &mut TrainerAssociationFixture,
    code: i32,
    evidence: ProcessGroupExitEvidence,
) {
    fixture
        .store
        .cas_exit_with_evidence(
            fixture.task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code },
            evidence,
        )
        .unwrap()
        .unwrap();
    fixture
        .store
        .update_execution_state(fixture.task_id, ProcessStatus::Failed)
        .unwrap();
}

fn complete(
    fixture: &mut TrainerAssociationFixture,
    action_id: ActionId,
    revision: ResourceRevision,
) -> Result<ReleaseCompletionResult, CompleteReleaseError> {
    fixture.store.complete_release_for_authority(
        fixture.authority,
        fixture.resource.id,
        action_id,
        revision,
    )
}

fn saved_snapshot(fixture: &TrainerAssociationFixture) -> ResourceSnapshot {
    fixture
        .store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()
        .remove(0)
}

fn request_state(
    fixture: &TrainerAssociationFixture,
    request_id: RequestId,
) -> ResourceRequestState {
    fixture
        .store
        .resource_requests(fixture.authority, fixture.resource.id)
        .unwrap()
        .into_iter()
        .find(|request| request.request_id == request_id)
        .unwrap()
        .state
}

fn saved_request_digest(fixture: &TrainerAssociationFixture) -> TrainerRequestDigest {
    let association: serde_json::Value = serde_json::from_str(&saved_trainer_association_json(
        &fixture.store,
        fixture.task_id,
    ))
    .unwrap();
    TrainerRequestDigest::from_hex(association["request_sha256"].as_str().unwrap()).unwrap()
}

#[test]
fn failed_trainer_with_its_released_lock_serves_the_oldest_request() {
    let (mut fixture, binding, first_request, action_id, revision, loan_id) =
        release_completion_fixture();
    let second = queue(&mut fixture);
    // artifacts on disk do not change the basis of a failed run
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &fixture.runtime_root,
        &binding,
        "generation-before-failure",
        12,
    );
    fail_trainer(&mut fixture, 3, ProcessGroupExitEvidence::ConfirmedExited);

    let result = complete(&mut fixture, action_id, revision).unwrap();
    let ReleaseCompletionResult::Assigned {
        loan,
        request,
        state_revision,
    } = &result
    else {
        panic!("the queued request must be assigned after the ended release");
    };
    assert_eq!(loan.id, loan_id);
    assert_eq!(request.request_id, first_request);
    assert_eq!(*state_revision, ResourceRevision::new(revision.get() + 1));
    let LoanState::Active {
        phase:
            LoanPhase::Serving {
                return_context,
                release_provenance,
                ..
            },
    } = &loan.state
    else {
        panic!("the assignment must serve the request");
    };
    assert_eq!(
        *return_context,
        ReturnContext::EndedWithoutResult {
            task_id: fixture.task_id,
            outcome: ExitReason::Exit { code: 3 },
        }
    );
    assert_eq!(
        *release_provenance,
        ServingReleaseProvenance::EndedTrainerLockReleased {
            action_id,
            task_id: fixture.task_id,
            outcome: ExitReason::Exit { code: 3 },
            attempt_request_sha256: saved_request_digest(&fixture),
        }
    );
    assert!(matches!(
        request_state(&fixture, second.request_id),
        ResourceRequestState::Queued
    ));

    // the saved receipt is the provenance that lets the assigned command start
    let input = ResourceTaskAcceptanceInput {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        request_id: request.request_id,
        task_id: request.task_id,
        acceptance_sequence: request.acceptance_sequence,
        loan_id,
        expected_state_revision: *state_revision,
        command_spec: request.spec().clone(),
        executor_env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
    };
    assert_eq!(
        fixture.store.accept_assigned_resource_task(input).unwrap(),
        ResourceTaskAcceptance::Inserted {
            task: request.task_id
        }
    );
}

#[test]
fn failed_trainer_with_a_held_lock_stays_reserved_until_the_lock_is_free() {
    let (mut fixture, _, request_id, action_id, revision, _) = release_completion_fixture();
    fail_trainer(&mut fixture, 1, ProcessGroupExitEvidence::ConfirmedExited);
    let before = saved_snapshot(&fixture);
    let lock_holder = fixture.hold_saved_lock();

    assert!(matches!(
        complete(&mut fixture, action_id, revision),
        Err(CompleteReleaseError::OwnershipLockStillHeld { task_id }) if task_id == fixture.task_id
    ));
    let after = saved_snapshot(&fixture);
    assert_eq!(after.loan, before.loan);
    assert_eq!(after.resource, before.resource);
    assert!(matches!(
        request_state(&fixture, request_id),
        ResourceRequestState::Queued
    ));

    // the escaped worker exits, so the same action now has its proof
    drop(lock_holder);
    let result = complete(&mut fixture, action_id, revision).unwrap();
    assert!(matches!(
        result,
        ReleaseCompletionResult::Assigned { request, .. } if request.request_id == request_id
    ));
}

#[test]
fn lost_unconfirmed_or_unassociated_ended_trainers_are_never_free() {
    let (mut lost, _, _, action_id, revision, _) = release_completion_fixture();
    lost.store
        .cas_status(lost.task_id, ProcessStatus::Running, ProcessStatus::Lost)
        .unwrap()
        .unwrap();
    assert!(matches!(
        complete(&mut lost, action_id, revision),
        Err(CompleteReleaseError::BackgroundTaskLost { task_id }) if task_id == lost.task_id
    ));

    let (mut unconfirmed, _, _, action_id, revision, _) = release_completion_fixture();
    fail_trainer(&mut unconfirmed, 2, ProcessGroupExitEvidence::Unconfirmed);
    assert!(matches!(
        complete(&mut unconfirmed, action_id, revision),
        Err(CompleteReleaseError::WorkerExitUnconfirmed { task_id })
            if task_id == unconfirmed.task_id
    ));

    // no saved association names a lock, so no fallback clears the loan
    let (mut unassociated, _, request_id, action_id, revision, _) = release_completion_fixture();
    fail_trainer(
        &mut unassociated,
        2,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    unassociated
        .store
        .conn
        .execute(
            "DELETE FROM trainer_attempt_associations WHERE task_id = ?1",
            [unassociated.task_id.to_string()],
        )
        .unwrap();
    assert!(matches!(
        complete(&mut unassociated, action_id, revision),
        Err(CompleteReleaseError::TrainerAssociationMissing { task_id })
            if task_id == unassociated.task_id
    ));
    assert!(matches!(
        request_state(&unassociated, request_id),
        ResourceRequestState::Queued
    ));
}

#[test]
fn ended_release_with_an_empty_queue_keeps_the_supervisor_decision_boundary() {
    let (mut fixture, _, request_id, action_id, revision, loan_id) = release_completion_fixture();
    let queued = fixture
        .store
        .resource_requests(fixture.authority, fixture.resource.id)
        .unwrap()
        .into_iter()
        .find(|request| request.request_id == request_id)
        .unwrap();
    fixture
        .store
        .cancel_resource_request_before_activation(
            fixture.authority,
            queued.request_id,
            queued.task_id,
            fixture.resource.id,
            queued.origin_machine,
        )
        .unwrap();
    assert!(matches!(
        fixture.store.request_cancel(fixture.task_id).unwrap(),
        crate::store::CancelResult::SignalWorker(_)
    ));
    fixture
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);

    let ReleaseCompletionResult::ReturnRequired { loan, notice } =
        complete(&mut fixture, action_id, revision).unwrap()
    else {
        panic!("the empty queue must reserve the supervisor's return decision");
    };
    let ended = ReturnContext::EndedWithoutResult {
        task_id: fixture.task_id,
        outcome: ExitReason::Cancelled,
    };
    assert!(matches!(
        &loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingReturn { action_id: return_action, return_context }
        } if *return_action == notice.action_id && *return_context == ended
    ));
    assert_eq!(
        notice.payload,
        SupervisorNoticePayload::ReturnRequired {
            return_context: ended.clone()
        }
    );

    let authority = SupervisorActionAuthority {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        loan_id,
        action_id: notice.action_id,
        expected_state_revision: notice.state_revision,
        supervisor: fixture.resource.supervisor,
        assignment_revision: fixture.resource.assignment_revision,
    };
    let executor_env = TaskEnv {
        path: fixture.bin.to_string_lossy().into_owned(),
        home: fixture.home.to_string_lossy().into_owned(),
    };
    let launch = |work| ReturnLaunch {
        request_id: RequestId::new(),
        task_id: TaskId::new(),
        work,
    };

    // the ended run has no resume evidence, whatever checkpoint exists
    assert!(matches!(
        fixture.store.prepare_return_task_for_authority(
            authority,
            launch(ReturnWork::SameRunResume {
                stopped_task: fixture.task_id,
                recovery_ref: "generation-after-cancel".into(),
            }),
            executor_env.clone(),
        ),
        Err(ReturnDecisionError::Rejected(
            ReturnDecisionRejection::ResumeRequiresStoppedContext
        ))
    ));
    // new work named for the ended run is a valid supervisor choice
    let prepared = fixture
        .store
        .prepare_return_task_for_authority(
            authority,
            launch(ReturnWork::AfterEndedRun {
                ended_task: fixture.task_id,
                spec: CommandSpec::try_from(fixture.spec.clone()).unwrap(),
            }),
            executor_env,
        )
        .unwrap();
    assert_eq!(
        prepared.normalized_spec_sha256,
        normalized_spec_sha256(&fixture.spec).unwrap()
    );

    let closure = fixture
        .store
        .record_no_resume_for_authority(authority, "ended run is not retried".into())
        .unwrap();
    assert!(matches!(
        closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::NoResume { return_context, .. }
        } if return_context == ended
    ));
    assert_eq!(
        saved_snapshot(&fixture).resource.registered_background_task,
        None
    );
}

#[test]
fn exact_ended_release_retry_survives_restart_and_stale_input_conflicts() {
    let (mut fixture, _, _, action_id, revision, _) = release_completion_fixture();
    fail_trainer(&mut fixture, 5, ProcessGroupExitEvidence::ConfirmedExited);
    let stale = ResourceRevision::new(revision.get() + 1);
    assert!(matches!(
        complete(&mut fixture, action_id, stale),
        Err(CompleteReleaseError::StaleRevision { expected, actual })
            if expected == stale && actual == revision
    ));
    let other_action = ActionId::new();
    assert!(matches!(
        complete(&mut fixture, other_action, revision),
        Err(CompleteReleaseError::NotAwaitingRelease { action_id, .. }) if action_id == other_action
    ));

    let first = complete(&mut fixture, action_id, revision).unwrap();
    // a restarted daemon reads the receipt; it needs no lock and relaunches nothing
    fixture.store = Store::open(&fixture.database).unwrap();
    let _lock_holder = fixture.hold_saved_lock();
    let retry = complete(&mut fixture, action_id, revision).unwrap();
    assert_eq!(
        serde_json::to_value(&retry).unwrap(),
        serde_json::to_value(&first).unwrap()
    );
    assert!(matches!(
        complete(&mut fixture, action_id, stale),
        Err(CompleteReleaseError::ConflictingRetry { action_id: conflict }) if conflict == action_id
    ));
}

#[test]
fn trainer_that_failed_before_a_loan_opens_a_release_action_and_serves_fifo() {
    let mut fixture = TrainerAssociationFixture::new();
    fixture.insert_accepted_running_task();
    fixture.register_release_attempt();
    fail_trainer(&mut fixture, 7, ProcessGroupExitEvidence::ConfirmedExited);
    let first = queue(&mut fixture);
    let second = queue(&mut fixture);

    // the ended trainer opens the ordinary release action, not a free resource
    let ResourceQueueReconcileOutcome::ReleaseRequired { loan, notice } = fixture
        .store
        .reconcile_resource_queue_for_authority(fixture.authority, fixture.resource.id)
        .unwrap()
    else {
        panic!("an ended trainer with its association must open one release action");
    };
    assert!(matches!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease { observed_background_task, .. }
        } if observed_background_task == fixture.task_id
    ));
    assert!(matches!(
        request_state(&fixture, first.request_id),
        ResourceRequestState::Queued
    ));

    let result = complete(&mut fixture, notice.action_id, notice.state_revision).unwrap();
    let ReleaseCompletionResult::Assigned { request, .. } = &result else {
        panic!("the proved release must assign the oldest request");
    };
    assert_eq!(request.request_id, first.request_id);
    assert_eq!(
        saved_snapshot(&fixture).loan.map(|loan| loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: ReturnContext::EndedWithoutResult {
                    task_id: fixture.task_id,
                    outcome: ExitReason::Exit { code: 7 },
                },
                current_request_id: first.request_id,
                release_provenance: ServingReleaseProvenance::EndedTrainerLockReleased {
                    action_id: notice.action_id,
                    task_id: fixture.task_id,
                    outcome: ExitReason::Exit { code: 7 },
                    attempt_request_sha256: saved_request_digest(&fixture),
                },
            }
        })
    );
    assert!(matches!(
        request_state(&fixture, second.request_id),
        ResourceRequestState::Queued
    ));
}
