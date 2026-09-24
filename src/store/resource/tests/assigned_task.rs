//! Assigned resource task acceptance, FIFO drain, and completion tests

use super::fixtures::{
    ServingFixture, accept_and_finish_next_resource_task, accept_and_finish_resource_task,
    acceptance_counts, acceptance_input, completion, refresh_serving_fixture,
    resource_cancellation_identity, resource_cancellation_proof, resource_origin_route,
    serving_fixture, task_reconcile_input, waiting_receipt,
};
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId, TaskState,
};
use crate::events::{EventAcceptance, EventPayload};
use crate::machine::MachineId;
use crate::resource::store::{
    AssignedResourceTaskAttention, AssignedResourceTaskReconcileOutcome, ResourceStoreError,
    ResourceTaskAcceptance, ResourceTaskAcceptanceInput, ResourceTaskCompletionResult,
};
use crate::resource::{
    Loan, LoanId, LoanPhase, LoanState, ResourceId, ResourceRequestState, ResourceRevision,
};
use crate::spec::NormalizedWorkload;
use crate::store::{ExecutorIdentity, Store};
use crate::submission::{PreAcceptanceRejection, RequestId, ResourceRoutePhase, SubmissionState};
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn assigned_resource_task_acceptance_commits_task_identity_and_first_event() {
    let mut fixture = serving_fixture(true, true);
    let input = acceptance_input(&fixture);

    assert_eq!(
        fixture
            .store
            .accept_assigned_resource_task(input.clone())
            .unwrap(),
        ResourceTaskAcceptance::Inserted {
            task: fixture.request.task_id,
        }
    );
    let task = fixture
        .store
        .get_task(fixture.request.task_id)
        .unwrap()
        .unwrap();
    assert_eq!(task.state, TaskState::Queued);
    assert!(matches!(
        fixture.store.executor_identity(task.id).unwrap(),
        Some(ExecutorIdentity::Accepted(record))
            if record.task == task.id
                && record.origin_machine == fixture.origin
                && record.execution_machine == fixture.authority
                && record.state == ProcessStatus::Queued
    ));

    let events = fixture.store.pending_outbound_events(task.id).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.seq.get(), 1);
    assert_eq!(events[0].event.origin_machine, fixture.origin);
    assert_eq!(events[0].event.execution_machine, fixture.authority);
    assert_eq!(
        events[0].event.payload,
        EventPayload::State {
            status: ProcessStatus::Queued,
        }
    );
    assert!(matches!(
        fixture.store.resource_requests(fixture.authority, fixture.resource.id).unwrap()[0]
            .state,
        ResourceRequestState::Assigned { loan_id } if loan_id == fixture.loan.id
    ));
    assert!(matches!(
        fixture
            .store
            .resource_snapshots_for_authority(fixture.authority)
            .unwrap()[0]
            .loan
            .as_ref()
            .map(|loan| &loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Serving { current_request_id, .. }
        }) if *current_request_id == fixture.request.request_id
    ));
}

#[test]
fn assigned_resource_task_event_failure_rolls_back_task_and_identity() {
    let mut fixture = serving_fixture(true, true);
    let input = acceptance_input(&fixture);
    fixture
        .store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_resource_task_event
             BEFORE INSERT ON executor_outbox
             BEGIN SELECT RAISE(ABORT, 'event storage is unavailable'); END;",
        )
        .unwrap();

    assert!(
        fixture
            .store
            .accept_assigned_resource_task(input.clone())
            .is_err()
    );
    assert_eq!(
        acceptance_counts(&fixture.store, input.request_id, input.task_id)[..4],
        [0, 0, 0, 0]
    );
    assert!(matches!(
        fixture
            .store
            .resource_requests(fixture.authority, fixture.resource.id)
            .unwrap()[0]
            .state,
        ResourceRequestState::Assigned { loan_id } if loan_id == fixture.loan.id
    ));
}

#[test]
fn assigned_resource_task_retry_after_reopen_returns_existing_without_records() {
    let mut fixture = serving_fixture(true, true);
    let input = acceptance_input(&fixture);
    fixture
        .store
        .accept_assigned_resource_task(input.clone())
        .unwrap();
    let before = acceptance_counts(&fixture.store, input.request_id, input.task_id);
    let database = fixture.directory.path().join("db");
    let ServingFixture {
        directory, store, ..
    } = fixture;
    drop(store);

    let mut reopened = Store::open(&database).unwrap();
    assert_eq!(
        reopened
            .accept_assigned_resource_task(input.clone())
            .unwrap(),
        ResourceTaskAcceptance::Existing {
            task: input.task_id,
            state: ProcessStatus::Queued,
        }
    );
    assert_eq!(
        acceptance_counts(&reopened, input.request_id, input.task_id),
        before
    );
    drop(directory);
}

#[test]
fn assigned_resource_task_retry_rejects_missing_row_or_changed_environment() {
    let mut fixture = serving_fixture(true, true);
    let input = acceptance_input(&fixture);
    fixture
        .store
        .accept_assigned_resource_task(input.clone())
        .unwrap();

    let mut changed_environment = input.clone();
    changed_environment.executor_env.home = "/different-home".into();
    assert!(matches!(
        fixture
            .store
            .accept_assigned_resource_task(changed_environment),
        Err(ResourceStoreError::Conflict(_))
    ));

    fixture
        .store
        .conn
        .execute("DELETE FROM tasks WHERE id=?1", [input.task_id.to_string()])
        .unwrap();
    assert!(matches!(
        fixture.store.accept_assigned_resource_task(input),
        Err(ResourceStoreError::Conflict(_))
    ));
}

#[test]
fn assigned_resource_task_rejects_changed_spec_and_route() {
    let mut fixture = serving_fixture(true, true);
    let input = acceptance_input(&fixture);
    fixture
        .store
        .accept_assigned_resource_task(input.clone())
        .unwrap();

    let mut changed_spec = fixture.spec.clone();
    let NormalizedWorkload::Task(task) = &mut changed_spec.workload else {
        panic!("resource acceptance uses a command workload");
    };
    task.command =
        crate::invocation::CommandLine::try_from_argv(vec!["/bin/echo".into(), "changed".into()])
            .unwrap();
    let mut changed_input = input.clone();
    changed_input.command_spec = crate::resource::CommandSpec::try_from(changed_spec).unwrap();
    let before_changed_spec = acceptance_counts(&fixture.store, input.request_id, input.task_id);
    assert!(matches!(
        fixture.store.accept_assigned_resource_task(changed_input),
        Err(ResourceStoreError::Conflict(_))
    ));
    assert_eq!(
        acceptance_counts(&fixture.store, input.request_id, input.task_id),
        before_changed_spec
    );

    let mut route = fixture
        .store
        .origin_route_by_task(input.task_id)
        .unwrap()
        .unwrap();
    route.submission = SubmissionState::Resource {
        resource: ResourceId::new(),
        phase: ResourceRoutePhase::Waiting,
    };
    fixture
        .store
        .conn
        .execute(
            "UPDATE origin_routes SET route_json=?1 WHERE task_id=?2",
            rusqlite::params![
                serde_json::to_string(&route).unwrap(),
                input.task_id.to_string()
            ],
        )
        .unwrap();
    let before = acceptance_counts(&fixture.store, input.request_id, input.task_id);
    assert!(matches!(
        fixture.store.accept_assigned_resource_task(input.clone()),
        Err(ResourceStoreError::Conflict(_))
    ));
    assert_eq!(
        acceptance_counts(&fixture.store, input.request_id, input.task_id),
        before
    );
}

#[test]
fn assigned_resource_task_rejects_wrong_authority_loan_request_and_revision() {
    let mut fixture = serving_fixture(true, true);
    let base = acceptance_input(&fixture);

    let mut wrong_authority = base.clone();
    wrong_authority.authority_machine = MachineId::new();
    assert!(matches!(
        fixture.store.accept_assigned_resource_task(wrong_authority),
        Err(ResourceStoreError::WrongAuthority { .. })
    ));

    let mut wrong_loan = base.clone();
    wrong_loan.loan_id = LoanId::new();
    assert!(matches!(
        fixture.store.accept_assigned_resource_task(wrong_loan),
        Err(ResourceStoreError::Conflict(_))
    ));

    let mut wrong_request = base.clone();
    wrong_request.request_id = RequestId::new();
    assert!(matches!(
        fixture.store.accept_assigned_resource_task(wrong_request),
        Err(ResourceStoreError::Conflict(_))
    ));

    let mut stale_revision = base.clone();
    stale_revision.expected_state_revision =
        ResourceRevision::new(fixture.state_revision.get() + 1);
    assert!(matches!(
        fixture.store.accept_assigned_resource_task(stale_revision),
        Err(ResourceStoreError::Conflict(_))
    ));
    assert_eq!(
        acceptance_counts(&fixture.store, base.request_id, base.task_id)[..4],
        [0, 0, 0, 0]
    );
}

#[test]
fn assigned_resource_task_rejects_a_non_fifo_loan_selection() {
    let mut fixture = serving_fixture(true, true);
    let later_request = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            fixture.spec.clone(),
        )
        .unwrap();
    let LoanState::Active {
        phase: LoanPhase::Serving { return_context, .. },
    } = fixture.loan.state.clone()
    else {
        panic!("fixture must have one serving request");
    };

    fixture
        .store
        .conn
        .execute(
            "UPDATE resource_requests SET state_json=?1 WHERE request_id=?2",
            rusqlite::params![
                serde_json::to_string(&ResourceRequestState::Queued).unwrap(),
                fixture.request.request_id.0.to_string(),
            ],
        )
        .unwrap();
    fixture
        .store
        .conn
        .execute(
            "UPDATE resource_requests SET state_json=?1 WHERE request_id=?2",
            rusqlite::params![
                serde_json::to_string(&ResourceRequestState::Assigned {
                    loan_id: fixture.loan.id,
                })
                .unwrap(),
                later_request.request_id.0.to_string(),
            ],
        )
        .unwrap();
    let later_loan = Loan {
        id: fixture.loan.id,
        resource_id: fixture.resource.id,
        state: LoanState::Active {
            phase: LoanPhase::Serving {
                return_context,
                current_request_id: later_request.request_id,
                release_provenance: crate::store::unreceipted_release_provenance(),
            },
        },
    };
    fixture
        .store
        .conn
        .execute(
            "UPDATE loans SET state_json=?1 WHERE id=?2",
            rusqlite::params![
                serde_json::to_string(&later_loan.state).unwrap(),
                fixture.loan.id.as_uuid().to_string(),
            ],
        )
        .unwrap();

    let input = ResourceTaskAcceptanceInput {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        request_id: later_request.request_id,
        task_id: later_request.task_id,
        acceptance_sequence: later_request.acceptance_sequence,
        loan_id: fixture.loan.id,
        expected_state_revision: fixture.state_revision,
        command_spec: later_request.spec().clone(),
        executor_env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
    };
    assert!(matches!(
        fixture.store.accept_assigned_resource_task(input),
        Err(ResourceStoreError::Conflict(_))
    ));
    assert_eq!(
        acceptance_counts(
            &fixture.store,
            later_request.request_id,
            later_request.task_id
        )[..4],
        [0, 0, 0, 0]
    );
}

#[test]
fn confirmed_success_failure_and_cancel_drain_three_requests_in_fifo_order() {
    let mut fixture = serving_fixture(true, true);
    let second = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            fixture.spec.clone(),
        )
        .unwrap();
    let third = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            fixture.spec.clone(),
        )
        .unwrap();

    let first_input = accept_and_finish_resource_task(
        &mut fixture,
        ExitReason::Exit { code: 0 },
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let first = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(first_input)
        .unwrap();
    let (loan, next_request) = match completion(first) {
        Ok(ResourceTaskCompletionResult::Assigned {
            finished_request,
            loan,
            next_request,
            ..
        }) => {
            assert!(matches!(
                &finished_request.state,
                ResourceRequestState::Finished {
                    outcome: ExitReason::Exit { code: 0 }
                }
            ));
            (loan, next_request)
        }
        other => {
            panic!("first confirmed completion did not assign the next request: {other:?}")
        }
    };
    assert_eq!(next_request.request_id, second.request_id);
    assert_eq!(loan.id, fixture.loan.id);
    assert!(matches!(
        &next_request.state,
        ResourceRequestState::Assigned { loan_id } if *loan_id == loan.id
    ));
    assert!(matches!(
        fixture
            .store
            .resource_requests(fixture.authority, fixture.resource.id)
            .unwrap()[2]
            .state,
        ResourceRequestState::Queued
    ));
    let committed_revision = fixture
        .store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()[0]
        .resource
        .state_revision;
    // a duplicate terminal event returns the stored assignment without a new revision
    let duplicate = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(first_input)
        .unwrap();
    assert!(matches!(
        completion(duplicate),
        Ok(ResourceTaskCompletionResult::Assigned {
            next_request: duplicate_next,
            state_revision,
            ..
        }) if duplicate_next.request_id == second.request_id
            && state_revision == committed_revision
    ));
    assert_eq!(
        fixture
            .store
            .resource_snapshots_for_authority(fixture.authority)
            .unwrap()[0]
            .resource
            .state_revision,
        committed_revision
    );
    refresh_serving_fixture(&mut fixture, loan, next_request);

    let second_input = accept_and_finish_next_resource_task(
        &mut fixture,
        ExitReason::Exit { code: 7 },
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let second_result = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(second_input)
        .unwrap();
    let (loan, next_request) = match completion(second_result) {
        Ok(ResourceTaskCompletionResult::Assigned {
            finished_request,
            loan,
            next_request,
            ..
        }) => {
            assert!(matches!(
                &finished_request.state,
                ResourceRequestState::Finished {
                    outcome: ExitReason::Exit { code: 7 }
                }
            ));
            (loan, next_request)
        }
        other => {
            panic!("second confirmed completion did not assign the last request: {other:?}")
        }
    };
    assert_eq!(next_request.request_id, third.request_id);
    assert_eq!(loan.id, fixture.loan.id);
    refresh_serving_fixture(&mut fixture, loan, next_request);

    let third_input = accept_and_finish_next_resource_task(
        &mut fixture,
        ExitReason::Cancelled,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let third_result = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(third_input)
        .unwrap();
    let (loan, notice) = match completion(third_result) {
        Ok(ResourceTaskCompletionResult::ReturnRequired {
            finished_request,
            loan,
            notice,
        }) => {
            assert!(matches!(
                &finished_request.state,
                ResourceRequestState::Finished {
                    outcome: ExitReason::Cancelled
                }
            ));
            (loan, notice)
        }
        other => panic!("empty queue did not reserve the return: {other:?}"),
    };
    assert!(matches!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingReturn { action_id, .. }
        } if action_id == notice.action_id
    ));
    assert_eq!(notice.loan_id, fixture.loan.id);
    assert!(matches!(
        notice.payload,
        crate::resource::SupervisorNoticePayload::ReturnRequired { .. }
    ));
    let notice_count: i64 = fixture
        .store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices
             WHERE loan_id=?1
               AND json_extract(notice_json, '$.payload.type') = 'return_required'",
            [fixture.loan.id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(notice_count, 1);
}

#[test]
fn queue_does_not_advance_until_the_exact_process_group_exit_is_confirmed() {
    let mut fixture = serving_fixture(true, true);
    let next = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            fixture.spec.clone(),
        )
        .unwrap();
    let input = acceptance_input(&fixture);
    fixture.store.accept_assigned_resource_task(input).unwrap();
    fixture
        .store
        .cas_status(
            fixture.request.task_id,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();

    assert!(matches!(
        fixture
            .store
            .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
            .unwrap(),
        AssignedResourceTaskReconcileOutcome::Active(
            crate::resource::store::AssignedResourceTaskProgress::Running
        )
    ));
    fixture
        .store
        .cas_exit_with_evidence(
            fixture.request.task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::Unconfirmed,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
            .unwrap(),
        AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::ProcessGroupExitUnconfirmed
        )
    ));
    let requests = fixture
        .store
        .resource_requests(fixture.authority, fixture.resource.id)
        .unwrap();
    assert!(matches!(
        &requests[0].state,
        ResourceRequestState::Assigned { loan_id } if *loan_id == fixture.loan.id
    ));
    assert!(matches!(requests[1].state, ResourceRequestState::Queued));
    assert!(fixture.store.get_task(next.task_id).unwrap().is_none());
    assert_eq!(
        fixture
            .store
            .resource_snapshots_for_authority(fixture.authority)
            .unwrap()[0]
            .resource
            .state_revision,
        fixture.state_revision
    );
}

#[test]
fn lost_task_retains_its_assignment_and_reports_attention() {
    let mut fixture = serving_fixture(true, true);
    let next = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            fixture.spec.clone(),
        )
        .unwrap();
    fixture
        .store
        .accept_assigned_resource_task(acceptance_input(&fixture))
        .unwrap();
    fixture
        .store
        .cas_status(
            fixture.request.task_id,
            ProcessStatus::Queued,
            ProcessStatus::Lost,
        )
        .unwrap()
        .unwrap();

    assert!(matches!(
        fixture
            .store
            .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
            .unwrap(),
        AssignedResourceTaskReconcileOutcome::Attention(AssignedResourceTaskAttention::TaskLost)
    ));
    let requests = fixture
        .store
        .resource_requests(fixture.authority, fixture.resource.id)
        .unwrap();
    assert!(matches!(
        &requests[0].state,
        ResourceRequestState::Assigned { loan_id } if *loan_id == fixture.loan.id
    ));
    assert!(matches!(requests[1].state, ResourceRequestState::Queued));
    assert!(fixture.store.get_task(next.task_id).unwrap().is_none());
}

#[test]
fn mismatched_task_executor_loan_and_revision_do_not_commit_completion() {
    let mut fixture = serving_fixture(true, true);
    let input = accept_and_finish_resource_task(
        &mut fixture,
        ExitReason::Exit { code: 0 },
        ProcessGroupExitEvidence::ConfirmedExited,
    );

    let mut wrong_task = input;
    wrong_task.task_id = TaskId::new();
    assert!(matches!(
        fixture
            .store
            .reconcile_assigned_resource_task_for_authority(wrong_task)
            .unwrap(),
        AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::TaskIdentityMismatch
        )
    ));
    let mut wrong_loan = input;
    wrong_loan.loan_id = LoanId::new();
    assert!(matches!(
        fixture
            .store
            .reconcile_assigned_resource_task_for_authority(wrong_loan)
            .unwrap(),
        AssignedResourceTaskReconcileOutcome::Attention(_)
    ));
    let mut stale_revision = input;
    stale_revision.expected_state_revision =
        ResourceRevision::new(input.expected_state_revision.get() + 1);
    assert!(matches!(
        fixture
            .store
            .reconcile_assigned_resource_task_for_authority(stale_revision)
            .unwrap(),
        AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::StaleRevision
        )
    ));

    let Some(ExecutorIdentity::Accepted(mut accepted)) =
        fixture.store.executor_identity(input.task_id).unwrap()
    else {
        panic!("accepted resource task must retain an executor identity");
    };
    accepted.origin_machine = MachineId::new();
    fixture
        .store
        .conn
        .execute(
            "UPDATE executor_identities SET identity_json=?1 WHERE task_id=?2",
            rusqlite::params![
                serde_json::to_string(&ExecutorIdentity::Accepted(accepted)).unwrap(),
                input.task_id.to_string(),
            ],
        )
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .reconcile_assigned_resource_task_for_authority(input)
            .unwrap(),
        AssignedResourceTaskReconcileOutcome::Attention(
            AssignedResourceTaskAttention::TaskIdentityMismatch
        )
    ));
    assert!(matches!(
        fixture.store.resource_requests(fixture.authority, fixture.resource.id).unwrap()[0]
            .state,
        ResourceRequestState::Assigned { loan_id } if loan_id == fixture.loan.id
    ));
    let receipt_count: i64 = fixture
        .store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_task_completions",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipt_count, 0);
}

#[test]
fn queued_request_cancelled_during_a_serving_task_is_skipped() {
    let mut fixture = serving_fixture(true, true);
    let cancelled = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            fixture.spec.clone(),
        )
        .unwrap();
    let next = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            fixture.spec.clone(),
        )
        .unwrap();
    fixture
        .store
        .accept_assigned_resource_task(acceptance_input(&fixture))
        .unwrap();
    fixture
        .store
        .cas_status(
            fixture.request.task_id,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    fixture
        .store
        .cancel_resource_request_before_activation(
            fixture.authority,
            cancelled.request_id,
            cancelled.task_id,
            fixture.resource.id,
            cancelled.origin_machine,
        )
        .unwrap();
    fixture
        .store
        .cas_exit_with_evidence(
            fixture.request.task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();

    let result = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
        .unwrap();
    assert!(matches!(
        completion(result),
        Ok(
            ResourceTaskCompletionResult::Assigned {
                next_request,
                ..
            }
        ) if next_request.request_id == next.request_id
    ));
    let requests = fixture
        .store
        .resource_requests(fixture.authority, fixture.resource.id)
        .unwrap();
    assert!(matches!(
        requests
            .iter()
            .find(|request| request.request_id == cancelled.request_id)
            .unwrap()
            .state,
        ResourceRequestState::CancelledBeforeLaunch
    ));
    assert!(matches!(
        requests.iter().find(|request| request.request_id == next.request_id).unwrap().state,
        ResourceRequestState::Assigned { loan_id } if loan_id == fixture.loan.id
    ));
}

#[test]
fn no_child_spawn_after_initial_spawn_failure_is_an_explicit_release_proof() {
    let mut fixture = serving_fixture(true, true);
    fixture
        .store
        .accept_assigned_resource_task(acceptance_input(&fixture))
        .unwrap();
    fixture
        .store
        .cas_exit_with_evidence(
            fixture.request.task_id,
            ProcessStatus::Queued,
            &ExitReason::SpawnFailed {
                message: "fake task runner could not start".into(),
            },
            ProcessGroupExitEvidence::NoChildSpawned,
        )
        .unwrap()
        .unwrap();

    let result = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
        .unwrap();
    assert!(matches!(
        completion(result),
        Ok(ResourceTaskCompletionResult::ReturnRequired { .. })
    ));
    let receipt: String = fixture
        .store
        .conn
        .query_row(
            "SELECT receipt_json FROM resource_task_completions WHERE task_id=?1",
            [fixture.request.task_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&receipt).unwrap()["release_proof"],
        "no_child_spawned_after_spawn_failure"
    );
    assert!(matches!(
        fixture
            .store
            .resource_requests(fixture.authority, fixture.resource.id)
            .unwrap()[0]
            .state,
        ResourceRequestState::Finished {
            outcome: ExitReason::SpawnFailed { .. }
        }
    ));
}

#[test]
fn cancelling_an_accepted_task_before_its_worker_starts_is_an_explicit_release_proof() {
    let mut fixture = serving_fixture(true, true);
    let next = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            fixture.spec.clone(),
        )
        .unwrap();
    fixture
        .store
        .accept_assigned_resource_task(acceptance_input(&fixture))
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .request_cancel(fixture.request.task_id)
            .unwrap(),
        crate::store::CancelResult::CancelledQueued(_)
    ));

    let result = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
        .unwrap();
    assert!(matches!(
        completion(result),
        Ok(ResourceTaskCompletionResult::Assigned { next_request, .. })
            if next_request.request_id == next.request_id
    ));
    let receipt: String = fixture
        .store
        .conn
        .query_row(
            "SELECT receipt_json FROM resource_task_completions WHERE task_id=?1",
            [fixture.request.task_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&receipt).unwrap()["release_proof"],
        "no_child_spawned_after_queued_cancel"
    );
}

#[test]
fn no_child_evidence_after_a_worker_started_requires_a_spawn_failure() {
    let mut fixture = serving_fixture(true, true);
    fixture
        .store
        .accept_assigned_resource_task(acceptance_input(&fixture))
        .unwrap();
    fixture
        .store
        .cas_status(
            fixture.request.task_id,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();

    assert!(
        fixture
            .store
            .cas_exit_with_evidence(
                fixture.request.task_id,
                ProcessStatus::Running,
                &ExitReason::Cancelled,
                ProcessGroupExitEvidence::NoChildSpawned,
            )
            .is_err()
    );
    assert_eq!(
        fixture
            .store
            .get_task(fixture.request.task_id)
            .unwrap()
            .unwrap()
            .status(),
        ProcessStatus::Running
    );
}

#[test]
fn exact_task_completion_retry_after_reopen_returns_the_same_return_notice() {
    let mut fixture = serving_fixture(true, true);
    let input = accept_and_finish_resource_task(
        &mut fixture,
        ExitReason::Cancelled,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let first = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(input)
        .unwrap();
    let (action_id, notice_id, revision) = match completion(first) {
        Ok(ResourceTaskCompletionResult::ReturnRequired { notice, .. }) => {
            (notice.action_id, notice.id, notice.state_revision)
        }
        other => panic!("the empty queue must reserve one return notice: {other:?}"),
    };
    let database = fixture.directory.path().join("db");
    let committed_revision = fixture
        .store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()[0]
        .resource
        .state_revision;
    drop(fixture.store);

    let mut reopened = Store::open(&database).unwrap();
    let retry = reopened
        .reconcile_assigned_resource_task_for_authority(input)
        .unwrap();
    assert!(matches!(
        completion(retry),
        Ok(
            ResourceTaskCompletionResult::ReturnRequired {
                notice,
                ..
            }
        ) if notice.action_id == action_id
            && notice.id == notice_id
            && notice.state_revision == revision
    ));
    assert_eq!(
        reopened
            .resource_snapshots_for_authority(fixture.authority)
            .unwrap()[0]
            .resource
            .state_revision,
        committed_revision
    );
    let receipt_count: i64 = reopened
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_task_completions",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let notice_count: i64 = reopened
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices
             WHERE json_extract(notice_json, '$.payload.type') = 'return_required'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipt_count, 1);
    assert_eq!(notice_count, 1);
}

#[test]
fn assigned_resource_task_requires_local_callback_route_before_insert() {
    let mut fixture = serving_fixture(true, false);
    let input = acceptance_input(&fixture);

    assert!(matches!(
        fixture.store.accept_assigned_resource_task(input.clone()),
        Err(ResourceStoreError::OriginRouteNotFound { task }) if task == input.task_id
    ));
    assert_eq!(
        acceptance_counts(&fixture.store, input.request_id, input.task_id)[..4],
        [0, 0, 0, 0]
    );
}

#[test]
fn assigned_remote_resource_task_uses_the_saved_origin_event_route() {
    let mut fixture = serving_fixture(false, false);
    let origin_directory = tempdir().unwrap();
    let mut origin_store = Store::open(&origin_directory.path().join("origin-db")).unwrap();
    origin_store
        .insert_origin_route(&resource_origin_route(
            fixture.request.request_id,
            fixture.request.task_id,
            fixture.resource.id,
            fixture.origin,
            fixture.authority,
            &fixture.spec,
        ))
        .unwrap();
    origin_store
        .resolve_resource_route(&waiting_receipt(
            fixture.request.request_id,
            fixture.request.task_id,
            fixture.resource.id,
            fixture.origin,
            fixture.authority,
        ))
        .unwrap();

    let input = acceptance_input(&fixture);
    assert_eq!(
        fixture
            .store
            .accept_assigned_resource_task(input.clone())
            .unwrap(),
        ResourceTaskAcceptance::Inserted {
            task: fixture.request.task_id,
        }
    );
    assert!(
        fixture
            .store
            .origin_route_by_task(fixture.request.task_id)
            .unwrap()
            .is_none()
    );
    let event = fixture
        .store
        .pending_outbound_events(input.task_id)
        .unwrap()[0]
        .event
        .clone();
    assert_eq!(event.origin_machine, fixture.origin);
    assert_eq!(event.execution_machine, fixture.authority);
    assert_eq!(
        origin_store.accept_inbound_event(&event).unwrap(),
        EventAcceptance::Acknowledged { seq: 1 }
    );
    fixture
        .store
        .mark_outbound_acknowledged(input.task_id, event.seq)
        .unwrap();
    let route = origin_store
        .origin_route_by_task(input.task_id)
        .unwrap()
        .unwrap();
    assert!(matches!(
        route.submission,
        SubmissionState::Resource {
            phase: ResourceRoutePhase::Activated,
            ..
        }
    ));
    assert_eq!(route.origin_machine, fixture.origin);
    assert_eq!(route.execution_machine, fixture.authority);

    fixture
        .store
        .cas_status(input.task_id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    let running = fixture
        .store
        .pending_outbound_events(input.task_id)
        .unwrap()[0]
        .event
        .clone();
    assert_eq!(running.seq.get(), 2);
    assert_eq!(
        origin_store.accept_inbound_event(&running).unwrap(),
        EventAcceptance::Acknowledged { seq: 2 }
    );
    fixture
        .store
        .mark_outbound_acknowledged(input.task_id, running.seq)
        .unwrap();
    fixture
        .store
        .cas_exit_with_evidence(
            input.task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 9 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
            .unwrap(),
        AssignedResourceTaskReconcileOutcome::Completed(_)
    ));

    let terminal = fixture
        .store
        .pending_outbound_events(input.task_id)
        .unwrap()[0]
        .event
        .clone();
    assert_eq!(terminal.seq.get(), 3);
    assert_eq!(
        origin_store.accept_inbound_event(&terminal).unwrap(),
        EventAcceptance::Acknowledged { seq: 3 }
    );
    fixture
        .store
        .mark_outbound_acknowledged(input.task_id, terminal.seq)
        .unwrap();
    let route = origin_store
        .origin_route_by_task(input.task_id)
        .unwrap()
        .unwrap();
    assert_eq!(route.last_accepted_seq, 3);
    assert_eq!(route.last_execution_state, Some(ProcessStatus::Failed));
}

#[test]
fn cancellation_and_assigned_resource_task_acceptance_resolve_in_either_order() {
    let mut cancelled_first = serving_fixture(true, true);
    let input = acceptance_input(&cancelled_first);
    cancelled_first
        .store
        .cancel_resource_request_before_activation(
            cancelled_first.authority,
            input.request_id,
            input.task_id,
            input.resource_id,
            cancelled_first.origin,
        )
        .unwrap();
    assert!(matches!(
        cancelled_first
            .store
            .accept_assigned_resource_task(input.clone()),
        Err(ResourceStoreError::Prevented)
    ));
    let counts = acceptance_counts(&cancelled_first.store, input.request_id, input.task_id);
    assert_eq!(counts[0], 0);
    assert_eq!(&counts[2..4], [0, 0]);
    assert!(matches!(
        cancelled_first
            .store
            .resource_requests(cancelled_first.authority, cancelled_first.resource.id)
            .unwrap()[0]
            .state,
        ResourceRequestState::CancelledBeforeLaunch
    ));
    assert!(matches!(
        cancelled_first.store.executor_identity(input.task_id).unwrap(),
        Some(ExecutorIdentity::Rejected(rejection))
            if rejection.reason == PreAcceptanceRejection::Cancelled.as_str()
    ));

    let mut accepted_first = serving_fixture(true, true);
    let input = acceptance_input(&accepted_first);
    accepted_first
        .store
        .accept_assigned_resource_task(input.clone())
        .unwrap();
    let loan_before = accepted_first
        .store
        .resource_snapshots_for_authority(accepted_first.authority)
        .unwrap()[0]
        .loan
        .clone()
        .unwrap();
    let cancellation = resource_cancellation_identity(
        Uuid::now_v7(),
        input.request_id,
        input.task_id,
        input.resource_id,
        accepted_first.origin,
        accepted_first.authority,
        ResourceRoutePhase::Waiting,
    );
    let receipt = accepted_first
        .store
        .cancel_resource_request_with_receipt(
            accepted_first.authority,
            cancellation.clone(),
            resource_cancellation_proof(
                &cancellation,
                &accepted_first.spec,
                ResourceRoutePhase::Waiting,
            ),
        )
        .unwrap();
    assert_eq!(
        receipt.outcome,
        crate::submission::ResourceCancellationOutcome::NotEligible {
            reason: crate::submission::ResourceCancellationIneligibleReason::Activated,
        }
    );
    assert_eq!(
        accepted_first
            .store
            .resource_snapshots_for_authority(accepted_first.authority)
            .unwrap()[0]
            .loan
            .clone()
            .unwrap(),
        loan_before
    );
    assert!(matches!(
        accepted_first
            .store
            .resource_requests(accepted_first.authority, accepted_first.resource.id)
            .unwrap()[0]
            .state,
        ResourceRequestState::Assigned { loan_id } if loan_id == accepted_first.loan.id
    ));
}
