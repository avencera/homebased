//! A registered trainer that ended outside a loan and the queued work that follows it

use super::fixtures::{
    TrainerAssociationFixture, completion, machine_other_than, publish_completed_result, spec,
};
use crate::domain::{ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId};
use crate::machine::MachineId;
use crate::resource::store::{
    AssignedResourceTaskReconcileInput, CompleteReleaseError, ReleaseCompletionResult,
    ResourceTaskAcceptance, ResourceTaskAcceptanceInput, ResourceTaskCompletionResult,
};
use crate::resource::{
    DeliveryAttemptId, Loan, LoanPhase, LoanState, ResourceQueueAttentionReason,
    ResourceQueueReconcileOutcome, ResourceRequest, ResourceRequestState, ResourceRevision,
    ReturnContext, ServingReleaseProvenance, SupervisorAddress,
};
use crate::submission::RequestId;

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

fn reconcile(fixture: &mut TrainerAssociationFixture) -> ResourceQueueReconcileOutcome {
    fixture
        .store
        .reconcile_resource_queue_for_authority(fixture.authority, fixture.resource.id)
        .unwrap()
}

fn saved_revision(fixture: &TrainerAssociationFixture) -> ResourceRevision {
    fixture
        .store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()[0]
        .resource
        .state_revision
}

fn saved_loan(fixture: &TrainerAssociationFixture) -> Option<Loan> {
    fixture
        .store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()
        .remove(0)
        .loan
}

/// Accept the assigned request, end it with confirmed exit, and record its result
fn run_assigned(
    fixture: &mut TrainerAssociationFixture,
    loan: &Loan,
    request: &ResourceRequest,
) -> ResourceTaskCompletionResult {
    let input = ResourceTaskAcceptanceInput {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        request_id: request.request_id,
        task_id: request.task_id,
        acceptance_sequence: request.acceptance_sequence,
        loan_id: loan.id,
        expected_state_revision: saved_revision(fixture),
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
    fixture
        .store
        .cas_status(
            request.task_id,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    fixture
        .store
        .cas_exit_with_evidence(
            request.task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
    let result = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(AssignedResourceTaskReconcileInput {
            authority_machine: fixture.authority,
            resource_id: fixture.resource.id,
            loan_id: loan.id,
            request_id: request.request_id,
            task_id: request.task_id,
            expected_state_revision: saved_revision(fixture),
        })
        .unwrap();
    completion(result).unwrap()
}

#[test]
fn completed_trainer_serves_queued_work_through_its_release_proof_and_returns() {
    let mut fixture = TrainerAssociationFixture::new();
    fixture.insert_accepted_running_task();
    let binding = fixture.register_release_attempt();
    // the epoch ends normally while no loan exists
    publish_completed_result(
        &mut fixture,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let first = queue(&mut fixture);
    let second = queue(&mut fixture);

    // the completed trainer opens the ordinary release action, not a free resource
    let ResourceQueueReconcileOutcome::ReleaseRequired { loan, notice } = reconcile(&mut fixture)
    else {
        panic!("a completed trainer must open one release action");
    };
    assert!(matches!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease { observed_background_task, .. }
        } if observed_background_task == fixture.task_id
    ));

    // only the authority-built proof of the final result serves the oldest request
    let completed = fixture
        .store
        .complete_release_for_authority(
            fixture.authority,
            fixture.resource.id,
            notice.action_id,
            notice.state_revision,
        )
        .unwrap();
    let ReleaseCompletionResult::Assigned { loan, request, .. } = completed.clone() else {
        panic!("the verified completed result must assign the oldest request");
    };
    assert_eq!(request.request_id, first.request_id);
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
    assert!(matches!(
        return_context,
        ReturnContext::AlreadyCompleted { task_id, result_ref }
            if *task_id == fixture.task_id && result_ref.contains("#sha256=")
    ));
    assert!(matches!(
        release_provenance,
        ServingReleaseProvenance::CompletedTrainerResult { action_id, task_id, .. }
            if *action_id == notice.action_id && *task_id == fixture.task_id
    ));
    // an exact retry answers from the saved receipt
    let retried = fixture
        .store
        .complete_release_for_authority(
            fixture.authority,
            fixture.resource.id,
            notice.action_id,
            notice.state_revision,
        )
        .unwrap();
    assert!(matches!(
        retried,
        ReleaseCompletionResult::Assigned { loan: same, .. } if same == loan
    ));

    // the queue drains under the same loan and return context, then reserves the return
    let ResourceTaskCompletionResult::Assigned {
        loan, next_request, ..
    } = run_assigned(&mut fixture, &loan, &request)
    else {
        panic!("the second request must keep the loan");
    };
    assert_eq!(next_request.request_id, second.request_id);
    let ResourceTaskCompletionResult::ReturnRequired {
        loan: returning, ..
    } = run_assigned(&mut fixture, &loan, &next_request)
    else {
        panic!("the drained queue must reserve the return");
    };
    assert!(matches!(
        &returning.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingReturn { return_context: saved, .. }
        } if saved == return_context
    ));
}

#[test]
fn unsafe_trainer_ends_keep_queued_work_blocked() {
    type Setup = fn(&mut TrainerAssociationFixture);
    let cases: [(&str, Setup); 4] = [
        ("failed unconfirmed", |fixture| {
            fixture.register_release_attempt();
            fixture
                .store
                .cas_exit_with_evidence(
                    fixture.task_id,
                    ProcessStatus::Running,
                    &ExitReason::Exit { code: 1 },
                    ProcessGroupExitEvidence::Unconfirmed,
                )
                .unwrap()
                .unwrap();
        }),
        ("lost", |fixture| {
            fixture.register_release_attempt();
            fixture
                .store
                .cas_status(fixture.task_id, ProcessStatus::Running, ProcessStatus::Lost)
                .unwrap()
                .unwrap();
        }),
        ("unconfirmed", |fixture| {
            let binding = fixture.register_release_attempt();
            publish_completed_result(fixture, &binding, ProcessGroupExitEvidence::Unconfirmed);
        }),
        ("no association", |fixture| {
            fixture.finish_registered_task_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
        }),
    ];
    for (name, setup) in cases {
        let mut fixture = TrainerAssociationFixture::new();
        fixture.insert_accepted_running_task();
        setup(&mut fixture);
        let request = queue(&mut fixture);
        let before = saved_revision(&fixture);

        assert!(
            matches!(
                reconcile(&mut fixture),
                ResourceQueueReconcileOutcome::AttentionRequired {
                    request: blocked,
                    reason: ResourceQueueAttentionReason::BackgroundTaskNotRunning { task_id, .. },
                } if blocked.request_id == request.request_id && task_id == fixture.task_id
            ),
            "{name}"
        );
        assert!(saved_loan(&fixture).is_none(), "{name}");
        assert_eq!(saved_revision(&fixture), before, "{name}");
    }
}

#[test]
fn completed_trainer_without_a_verifiable_release_keeps_its_action_reserved() {
    // an unpublished final result with a held lock, or a published result with a
    // held lock, is not a release
    for published in [false, true] {
        let mut fixture = TrainerAssociationFixture::new();
        fixture.insert_accepted_running_task();
        let binding = fixture.register_release_attempt();
        if published {
            publish_completed_result(
                &mut fixture,
                &binding,
                ProcessGroupExitEvidence::ConfirmedExited,
            );
        } else {
            fixture.finish_registered_task_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
        }
        let _lock = fixture.hold_saved_lock();
        let request = queue(&mut fixture);
        let ResourceQueueReconcileOutcome::ReleaseRequired { loan, notice } =
            reconcile(&mut fixture)
        else {
            panic!("the saved facts must open the release action");
        };

        let error = fixture
            .store
            .complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap_err();
        assert!(
            matches!(&error, CompleteReleaseError::OwnershipLockStillHeld { .. }),
            "unexpected release error: {error:?}"
        );
        assert_eq!(saved_loan(&fixture), Some(loan));
        assert!(matches!(
            fixture
                .store
                .resource_requests(fixture.authority, fixture.resource.id)
                .unwrap()
                .into_iter()
                .find(|saved| saved.request_id == request.request_id)
                .map(|saved| saved.state),
            Some(ResourceRequestState::Queued)
        ));
    }
}

#[test]
fn delivered_release_completes_after_the_supervisor_is_replaced() {
    let mut fixture = TrainerAssociationFixture::new();
    fixture.insert_accepted_running_task();
    let binding = fixture.register_release_attempt();
    publish_completed_result(
        &mut fixture,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let request = queue(&mut fixture);
    let ResourceQueueReconcileOutcome::ReleaseRequired { notice, .. } = reconcile(&mut fixture)
    else {
        panic!("a completed trainer must open one release action");
    };
    let attempt = DeliveryAttemptId::new();
    fixture
        .store
        .reserve_supervisor_notice_attempt(notice.id, attempt)
        .unwrap();
    fixture
        .store
        .settle_supervisor_notice_attempt(notice.id, attempt, Ok(()))
        .unwrap();

    // the delivered notice keeps the assignment whose supervisor received it
    let replaced = fixture
        .store
        .replace_resource_supervisor(
            fixture.authority,
            fixture.resource.id,
            saved_revision(&fixture),
            SupervisorAddress {
                machine: machine_other_than(fixture.resource.supervisor.machine),
                thread: fixture.resource.supervisor.thread,
            },
        )
        .unwrap();
    assert!(replaced.retargeted.is_empty());

    let completed = fixture
        .store
        .complete_release_for_authority(
            fixture.authority,
            fixture.resource.id,
            notice.action_id,
            notice.state_revision,
        )
        .unwrap();
    assert!(matches!(
        completed,
        ReleaseCompletionResult::Assigned { request: assigned, .. }
            if assigned.request_id == request.request_id
    ));
}
