//! Daemon-driven startup, trainer reconcile, and assigned task launch tests

use super::fixtures::{
    TrainerAssociationFixture, fake_resource_task_spec, gated_resource_task_spec,
    machine_other_than, publish_completed_result, release_completion_fixture_with,
    return_notice_count, stop_test_resource_actor, stop_test_supervisor, wait_for_awaiting_return,
    wait_for_running_task, wait_for_terminal_task,
};
use crate::daemon::actors::resource::{ResourceActor, ResourceMsg};
use crate::daemon::actors::supervisor::SUPERVISOR_TEST_LOCK;
use crate::daemon::actors::{
    StoreActor, StoreMsg, SupervisorActor, SupervisorArgs, SupervisorMsg, call,
};
use crate::domain::{ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskId};
use crate::home::{Home, LockMode, flock_exclusive};
use crate::machine::{MachineId, load_or_create_machine_id};
use crate::resource::store::ReleaseCompletionResult;
use crate::resource::trainer_publication::find_completed_result;
use crate::resource::{
    LoanPhase, LoanState, ReleaseProofAttentionReason, ResourceQueueReconcileOutcome,
    ResourceRequest, ResourceRequestState, ServingReleaseProvenance,
};
use crate::submission::RequestId;
use ractor::Actor;
use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn startup_retries_completed_trainer_proof_and_launches_assigned_task_once() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
    let marker = fixture.home.join("activation-count");
    let gate = fixture.home.join("release-commands");
    let request_spec = gated_resource_task_spec(&fixture.home, &marker, &gate);
    let (mut fixture, binding, request_id, action_id, _, loan_id) = release_completion_fixture_with(
        fixture,
        request_spec.clone(),
        machine_other_than(authority),
    );
    let second = fixture
        .store
        .accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            machine_other_than(authority),
            request_spec,
        )
        .unwrap();
    publish_completed_result(
        &mut fixture,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let publication = find_completed_result(&fixture.runtime_root, &binding)
        .unwrap()
        .unwrap();
    let request = fixture
        .store
        .resource_requests(authority, fixture.resource.id)
        .unwrap()
        .into_iter()
        .find(|request| request.request_id == request_id)
        .unwrap();
    assert!(matches!(request.state, ResourceRequestState::Queued));
    let resource_id = fixture.resource.id;
    let trainer_task_id = fixture.task_id;
    drop(fixture.store);

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    wait_for_running_task(&store, request.task_id).await;
    for _ in 0..3 {
        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource_id,
            reply,
        })
        .await
        .unwrap();
    }
    let inspection = call(&supervisor, |reply| SupervisorMsg::InspectResource {
        id: resource_id,
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        inspection.loan.as_ref().map(|loan| &loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Serving {
                current_request_id,
                release_provenance:
                    ServingReleaseProvenance::CompletedTrainerResult {
                        action_id: saved_action,
                        task_id: saved_task,
                        publication_sha256,
                    },
                ..
            }
        }) if *current_request_id == request_id
            && *saved_action == action_id
            && *saved_task == trainer_task_id
            && *publication_sha256 == publication.publication_sha256
    ));
    // the next queued request waits for the running task's confirmed exit
    assert!(
        call(&store, |reply| StoreMsg::GetTask {
            id: second.task_id,
            reply,
        })
        .await
        .unwrap()
        .is_none()
    );

    fs::write(&gate, b"").unwrap();
    let row = wait_for_terminal_task(&store, request.task_id).await;
    assert_eq!(row.status(), ProcessStatus::Succeeded);
    let second_row = wait_for_terminal_task(&store, second.task_id).await;
    assert_eq!(second_row.status(), ProcessStatus::Succeeded);
    let loan = wait_for_awaiting_return(&supervisor, resource_id).await;
    assert_eq!(loan.id, loan_id);
    for _ in 0..3 {
        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource_id,
            reply,
        })
        .await
        .unwrap();
    }
    assert_eq!(fs::read(&marker).unwrap(), b"xx");
    assert_eq!(return_notice_count(&home, loan_id), 1);
    let requests = call(&store, |reply| StoreMsg::ResourceRequests {
        authority_machine: authority,
        resource_id,
        reply,
    })
    .await
    .unwrap();
    assert!(requests.iter().all(|request| matches!(
        request.state,
        ResourceRequestState::Finished {
            outcome: ExitReason::Exit { code: 0 }
        }
    )));

    stop_test_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn exact_trainer_terminal_event_reconciles_without_a_client_wake() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
    let marker = fixture.home.join("activation-count");
    let gate = fixture.home.join("release-commands");
    let request_spec = gated_resource_task_spec(&fixture.home, &marker, &gate);
    let (fixture, binding, request_id, action_id, _, loan_id) =
        release_completion_fixture_with(fixture, request_spec, machine_other_than(authority));
    let request = fixture
        .store
        .resource_requests(authority, fixture.resource.id)
        .unwrap()
        .into_iter()
        .find(|request| request.request_id == request_id)
        .unwrap();
    let resource_id = fixture.resource.id;
    let trainer_task_id = fixture.task_id;
    let runtime_root = fixture.runtime_root.clone();
    home.prepare_task(trainer_task_id).unwrap();
    let trainer_lock = flock_exclusive(
        &home.task_paths(trainer_task_id).runner_lock,
        LockMode::NonBlocking,
    )
    .unwrap();
    drop(fixture.store);

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let before = call(&supervisor, |reply| SupervisorMsg::InspectResource {
        id: resource_id,
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        before.reconcile_outcome,
        Some(ResourceQueueReconcileOutcome::ReleaseProofUnavailable {
            reason: ReleaseProofAttentionReason::TrainerNotCompleted,
            ..
        })
    ));

    crate::resource::trainer_publication::tests::write_completed_result_for_test(
        &runtime_root,
        &binding,
    );
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let terminal = call(&store, |reply| StoreMsg::CasExit {
        id: trainer_task_id,
        from: ProcessStatus::Running,
        reason: ExitReason::Exit { code: 0 },
        evidence: crate::domain::ProcessGroupExitEvidence::ConfirmedExited.into(),
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(terminal.status(), ProcessStatus::Succeeded);
    drop(trainer_lock);

    wait_for_running_task(&store, request.task_id).await;
    let after = call(&supervisor, |reply| SupervisorMsg::InspectResource {
        id: resource_id,
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        after.loan.as_ref().map(|loan| &loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Serving {
                release_provenance:
                    ServingReleaseProvenance::CompletedTrainerResult {
                        action_id: saved_action,
                        task_id: saved_task,
                        ..
                    },
                ..
            }
        }) if *saved_action == action_id && *saved_task == trainer_task_id
    ));
    assert!(matches!(
        after.loan.map(|loan| loan.id),
        Some(saved_loan) if saved_loan == loan_id
    ));

    fs::write(&gate, b"").unwrap();
    let row = wait_for_terminal_task(&store, request.task_id).await;
    assert_eq!(row.status(), ProcessStatus::Succeeded);
    assert_eq!(fs::read(&marker).unwrap(), b"x");
    let loan = wait_for_awaiting_return(&supervisor, resource_id).await;
    assert_eq!(loan.id, loan_id);
    assert_eq!(return_notice_count(&home, loan_id), 1);

    stop_test_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn pre_activation_cancellation_keeps_release_proof_for_the_next_request() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
    let marker = fixture.home.join("activation-count");
    let request_spec = fake_resource_task_spec(&fixture.home, &marker);
    let (mut fixture, binding, first_request_id, action_id, revision, loan_id) =
        release_completion_fixture_with(
            fixture,
            request_spec.clone(),
            machine_other_than(authority),
        );
    let second_origin = MachineId::new();
    let second_request = fixture
        .store
        .accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            second_origin,
            request_spec,
        )
        .unwrap();
    publish_completed_result(
        &mut fixture,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    assert!(matches!(
        fixture
            .store
            .complete_release_for_authority(
                authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap(),
        ReleaseCompletionResult::Assigned { request, .. }
            if request.request_id == first_request_id
    ));
    let first_request = fixture
        .store
        .resource_requests(authority, fixture.resource.id)
        .unwrap()
        .into_iter()
        .find(|request| request.request_id == first_request_id)
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .cancel_resource_request_before_activation(
                authority,
                first_request.request_id,
                first_request.task_id,
                fixture.resource.id,
                first_request.origin_machine,
            )
            .unwrap(),
        crate::resource::store::QueueCancellationResult::Request(saved)
            if saved.request_id == first_request_id
                && matches!(saved.state, ResourceRequestState::CancelledBeforeLaunch)
    ));
    let snapshot = fixture
        .store
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == fixture.resource.id)
        .unwrap();
    assert!(matches!(
        snapshot.loan.as_ref().map(|loan| &loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Serving {
                current_request_id,
                release_provenance:
                    ServingReleaseProvenance::CompletedTrainerResult {
                        action_id: saved_action,
                        ..
                    },
                ..
            }
        }) if *current_request_id == second_request.request_id
            && *saved_action == action_id
    ));
    let resource_id = fixture.resource.id;
    let second_task_id = second_request.task_id;
    drop(fixture.store);

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let row = wait_for_terminal_task(&store, second_task_id).await;
    assert_eq!(row.status(), ProcessStatus::Succeeded);
    let loan = wait_for_awaiting_return(&supervisor, resource_id).await;
    assert_eq!(loan.id, loan_id);
    assert_eq!(fs::read(&marker).unwrap(), b"x");
    let requests = call(&store, |reply| StoreMsg::ResourceRequests {
        authority_machine: authority,
        resource_id,
        reply,
    })
    .await
    .unwrap();
    assert!(matches!(
        requests
            .iter()
            .find(|request| request.request_id == first_request_id),
        Some(ResourceRequest {
            state: ResourceRequestState::CancelledBeforeLaunch,
            ..
        })
    ));
    assert!(matches!(
        requests
            .iter()
            .find(|request| request.request_id == second_request.request_id),
        Some(ResourceRequest {
            state: ResourceRequestState::Finished {
                outcome: ExitReason::Exit { code: 0 }
            },
            ..
        })
    ));

    stop_test_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn ongoing_unproven_or_unconfirmed_trainer_keeps_request_queued_and_does_not_launch() {
    // an ended run with no result is released by its lock, so a held lock keeps it reserved
    for (finish_task, publish_result, exit_evidence, expected_reason) in [
        (
            false,
            false,
            ProcessGroupExitEvidence::ConfirmedExited,
            ReleaseProofAttentionReason::TrainerNotCompleted,
        ),
        (
            true,
            false,
            ProcessGroupExitEvidence::ConfirmedExited,
            ReleaseProofAttentionReason::OwnershipLockUnverified,
        ),
        (
            true,
            true,
            ProcessGroupExitEvidence::Unconfirmed,
            ReleaseProofAttentionReason::WorkerExitUnconfirmed,
        ),
    ] {
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
        let marker = fixture.home.join("activation-count");
        let request_spec = fake_resource_task_spec(&fixture.home, &marker);
        let (mut fixture, binding, request_id, _, _, _) =
            release_completion_fixture_with(fixture, request_spec, machine_other_than(authority));
        let mut lock_holder = None;
        if publish_result {
            publish_completed_result(&mut fixture, &binding, exit_evidence);
        } else if finish_task {
            fixture.finish_registered_task_with_evidence(exit_evidence);
            lock_holder = Some(fixture.hold_saved_lock());
        }
        let resource_id = fixture.resource.id;
        let task_id = fixture
            .store
            .resource_requests(authority, resource_id)
            .unwrap()
            .into_iter()
            .find(|request| request.request_id == request_id)
            .unwrap()
            .task_id;
        drop(fixture.store);

        let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
            .await
            .unwrap();
        let snapshot = call(&store, |reply| StoreMsg::ResourceSnapshotsForAuthority {
            authority_machine: authority,
            reply,
        })
        .await
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource_id)
        .unwrap();
        let (actor, actor_handle) = ResourceActor::spawn(
            None,
            ResourceActor,
            (
                store.clone(),
                crate::daemon::actors::resource::test_support::spawn_stub_supervisor()
                    .await
                    .0,
                authority,
                snapshot.resource,
                snapshot.loan,
            ),
        )
        .await
        .unwrap();
        let inspection = call(&actor, |reply| ResourceMsg::Inspect { reply })
            .await
            .unwrap();
        assert!(matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::ReleaseProofUnavailable {
                reason,
                ..
            }) if reason == expected_reason
        ));
        let requests = call(&store, |reply| StoreMsg::ResourceRequests {
            authority_machine: authority,
            resource_id,
            reply,
        })
        .await
        .unwrap();
        assert!(matches!(
            requests
                .iter()
                .find(|request| request.request_id == request_id),
            Some(ResourceRequest {
                state: ResourceRequestState::Queued,
                ..
            })
        ));
        assert!(
            call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                .is_none()
        );
        assert!(!marker.exists());

        stop_test_resource_actor(actor, actor_handle, store, store_handle).await;
        drop(lock_holder);
    }
}

#[tokio::test]
async fn an_undecided_return_serves_late_queued_work_after_its_deadline() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
    let marker = fixture.home.join("activation-count");
    let request_spec = fake_resource_task_spec(&fixture.home, &marker);
    let (mut fixture, binding, _, _, _, loan_id) = release_completion_fixture_with(
        fixture,
        request_spec.clone(),
        machine_other_than(authority),
    );
    publish_completed_result(
        &mut fixture,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let resource_id = fixture.resource.id;
    drop(fixture.store);

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let loan = wait_for_awaiting_return(&supervisor, resource_id).await;
    let LoanState::Active {
        phase: LoanPhase::AwaitingReturn {
            action_id: expired, ..
        },
    } = loan.state
    else {
        unreachable!("the helper returns only an AwaitingReturn loan");
    };

    let late = call(&store, |reply| StoreMsg::AcceptResourceRequest {
        authority_machine: authority,
        request_id: RequestId::new(),
        task_id: TaskId::new(),
        resource_id,
        origin_machine: machine_other_than(authority),
        normalized_spec: Box::new(request_spec),
        reply,
    })
    .await
    .unwrap();
    call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource_id,
        reply,
    })
    .await
    .unwrap();
    // the open window keeps the late request queued
    assert!(
        call(&store, |reply| StoreMsg::GetTask {
            id: late.task_id,
            reply,
        })
        .await
        .unwrap()
        .is_none()
    );

    // close the window now instead of waiting for the default grace
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    rusqlite::Connection::open(home.db_path())
        .unwrap()
        .execute(
            "UPDATE resource_return_windows
             SET window_json = json_set(window_json, '$.deadline_at', ?1)
             WHERE action_id = ?2",
            rusqlite::params![now, expired.as_uuid().to_string()],
        )
        .unwrap();
    call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource_id,
        reply,
    })
    .await
    .unwrap();

    let row = wait_for_terminal_task(&store, late.task_id).await;
    assert_eq!(row.status(), ProcessStatus::Succeeded);
    let loan = wait_for_awaiting_return(&supervisor, resource_id).await;
    assert_eq!(loan.id, loan_id);
    assert!(matches!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingReturn { action_id, .. }
        } if action_id != expired
    ));

    stop_test_supervisor(supervisor, handle).await;
}
