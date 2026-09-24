//! Container workloads as queued requests and return work

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use serde_json::json;

use super::fixtures::{
    ServingFixture, accept_and_finish_resource_task, completion, refresh_serving_fixture,
    serving_fixture, task_reconcile_input,
};
use super::operator_release::attestation_for;
use super::restore::{
    accept_post_return_request, awaiting_return_fixture, completed_task,
    read_return_execution_mode, reconcile_restore, request_state, restore_closure_basis,
    saved_loan, saved_resource, start_task,
};
use crate::domain::{
    ContainerExitEvidence, ContainerId, ExitReason, ProcessGroupExitEvidence, ProcessStatus,
    TaskEnv, TaskExitEvidence, TaskId,
};
use crate::machine::MachineId;
use crate::resource::operator_release::{
    AttestedTrainerEnd, AttestedTrainerLaunch, OperatorGpuFreeOutcome, OperatorStateBinding,
};
use crate::resource::store::{
    AssignedResourceTaskAttention, AssignedResourceTaskReconcileOutcome, ResourceTaskAcceptance,
    ResourceTaskAcceptanceInput, ResourceTaskCompletionResult,
};
use crate::resource::{
    CommandSpec, CommandSpecError, IdleBoundaryProof, LoanClosure, LoanPhase, LoanState,
    ResourceQueueReconcileOutcome, ResourceRequestState, RestoreAttentionReason, ReturnContext,
    ReturnDecision, ReturnExecutionMode, ReturnLaunch, ReturnWork, SupervisorActionAuthority,
};
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::{
    EndedRestoreResolution, RestoreReconcileOutcome, ReturnDecisionError, ReturnTaskAcceptance,
    ReturnTaskAcceptanceInput, ReturnTaskOrigin, Store,
};
use crate::submission::{CallbackExecutable, RequestId};

fn container_spec(fixture: &ServingFixture, gpus: bool) -> NormalizedSpec {
    let mut workload = json!({
        "image": format!("eval@sha256:{}", "0".repeat(64)),
        "args": ["--checkpoint", "/data/ckpt"],
        "memory": "8g",
        "mounts": [{ "source": fixture.directory.path(), "target": "/data", "read_only": true }]
    });
    if gpus {
        workload["gpus"] = json!("all");
    }
    let mut spec = fixture.spec.clone();
    spec.name = crate::domain::TaskName::parse("container evaluation").unwrap();
    spec.workload = NormalizedWorkload::Container(Box::new(
        crate::container::ContainerWorkload::from_value(&workload).unwrap(),
    ));
    spec
}

/// Executor environment with a `docker` CLI on its PATH
///
/// Store transitions only resolve the CLI; they never run it
fn docker_env(fixture: &ServingFixture) -> TaskEnv {
    let bin = fixture.directory.path().join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let docker = bin.join("docker");
    std::fs::write(&docker, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    TaskEnv {
        path: bin.display().to_string(),
        home: "/tmp".into(),
    }
}

fn container_id(byte: &str) -> ContainerId {
    ContainerId::parse(&byte.repeat(64)).unwrap()
}

fn removed(exit_code: i32) -> TaskExitEvidence {
    TaskExitEvidence {
        process_group: ProcessGroupExitEvidence::Unconfirmed,
        container: ContainerExitEvidence::Confirmed {
            container_id: container_id("c"),
            exit_code,
        },
    }
}

fn finish(store: &Store, task_id: TaskId, reason: ExitReason, evidence: TaskExitEvidence) {
    store
        .cas_exit_with_evidence(task_id, ProcessStatus::Running, &reason, evidence)
        .unwrap()
        .unwrap();
}

#[test]
fn resource_containers_must_name_their_gpus() {
    let fixture = serving_fixture(true, true);
    assert!(matches!(
        CommandSpec::try_from(container_spec(&fixture, false)),
        Err(CommandSpecError::ContainerWithoutGpus)
    ));
    CommandSpec::try_from(container_spec(&fixture, true)).unwrap();
}

/// Serve the fixture's first request, then assign a queued container request
fn assigned_container_request() -> ServingFixture {
    let mut fixture = serving_fixture(true, true);
    let spec = container_spec(&fixture, true);
    let container = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            spec,
        )
        .unwrap();
    let input = accept_and_finish_resource_task(
        &mut fixture,
        ExitReason::Exit { code: 0 },
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let Ok(ResourceTaskCompletionResult::Assigned {
        loan, next_request, ..
    }) = completion(
        fixture
            .store
            .reconcile_assigned_resource_task_for_authority(input)
            .unwrap(),
    )
    else {
        panic!("the queued container request must be assigned next");
    };
    assert_eq!(next_request.request_id, container.request_id);
    refresh_serving_fixture(&mut fixture, loan, next_request);
    fixture
}

fn accept_container_task(fixture: &mut ServingFixture) -> TaskId {
    let env = docker_env(fixture);
    let input = ResourceTaskAcceptanceInput {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        request_id: fixture.request.request_id,
        task_id: fixture.request.task_id,
        acceptance_sequence: fixture.request.acceptance_sequence,
        loan_id: fixture.loan.id,
        expected_state_revision: fixture.state_revision,
        command_spec: fixture.request.spec().clone(),
        executor_env: env,
    };
    assert_eq!(
        fixture.store.accept_assigned_resource_task(input).unwrap(),
        ResourceTaskAcceptance::Inserted {
            task: fixture.request.task_id,
        }
    );
    let task_id = fixture.request.task_id;
    let row = fixture.store.require_task(task_id).unwrap();
    assert!(
        row.binary.ends_with("bin/docker"),
        "{}",
        row.binary.display()
    );
    start_task(&fixture.store, task_id);
    task_id
}

#[test]
fn queued_container_releases_its_turn_only_after_its_container_is_removed() {
    let mut fixture = assigned_container_request();
    let task_id = accept_container_task(&mut fixture);

    finish(
        &fixture.store,
        task_id,
        ExitReason::Exit { code: 4 },
        removed(4),
    );
    let Ok(ResourceTaskCompletionResult::ReturnRequired {
        finished_request, ..
    }) = completion(
        fixture
            .store
            .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
            .unwrap(),
    )
    else {
        panic!("a removed container releases the serving turn");
    };
    assert_eq!(
        finished_request.state,
        ResourceRequestState::Finished {
            outcome: ExitReason::Exit { code: 4 }
        }
    );
    let receipt: String = fixture
        .store
        .conn
        .query_row(
            "SELECT json_extract(receipt_json, '$.release_proof')
             FROM resource_task_completions WHERE task_id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(receipt.contains("confirmed_container_removed"), "{receipt}");
}

#[test]
fn queued_container_without_container_evidence_keeps_the_resource() {
    for (name, evidence) in [
        (
            "unconfirmed container",
            TaskExitEvidence {
                process_group: ProcessGroupExitEvidence::Unconfirmed,
                container: ContainerExitEvidence::Unconfirmed,
            },
        ),
        // the worker's own Docker clients exited, which says nothing about the container
        (
            "process group only",
            ProcessGroupExitEvidence::ConfirmedExited.into(),
        ),
    ] {
        let mut fixture = assigned_container_request();
        let task_id = accept_container_task(&mut fixture);
        finish(
            &fixture.store,
            task_id,
            ExitReason::Exit { code: 0 },
            evidence,
        );
        let outcome = fixture
            .store
            .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
            .unwrap();
        assert!(
            matches!(
                outcome,
                AssignedResourceTaskReconcileOutcome::Attention(
                    AssignedResourceTaskAttention::ExitWitnessUnconfirmed
                )
            ),
            "{name}: {outcome:?}"
        );
        assert!(matches!(
            request_state(&fixture, fixture.request.request_id),
            Some(ResourceRequestState::Assigned { .. })
        ));
    }
}

#[test]
fn a_container_that_never_started_releases_its_turn() {
    let mut fixture = assigned_container_request();
    let task_id = accept_container_task(&mut fixture);
    finish(
        &fixture.store,
        task_id,
        ExitReason::SpawnFailed {
            message: "No such image".into(),
        },
        TaskExitEvidence {
            process_group: ProcessGroupExitEvidence::Unconfirmed,
            container: ContainerExitEvidence::NeverStarted,
        },
    );
    assert!(
        completion(
            fixture
                .store
                .reconcile_assigned_resource_task_for_authority(task_reconcile_input(&fixture))
                .unwrap(),
        )
        .is_ok()
    );
}

fn container_launch(fixture: &ServingFixture) -> ReturnLaunch {
    ReturnLaunch {
        request_id: RequestId::new(),
        task_id: TaskId::new(),
        work: ReturnWork::EvaluationOrNextEpoch {
            completed_task: completed_task(fixture),
            spec: CommandSpec::try_from(container_spec(fixture, true)).unwrap(),
        },
    }
}

fn container_input(
    fixture: &ServingFixture,
    authority: SupervisorActionAuthority,
    launch: ReturnLaunch,
) -> ReturnTaskAcceptanceInput {
    ReturnTaskAcceptanceInput {
        authority,
        launch,
        executor_env: docker_env(fixture),
        origin: ReturnTaskOrigin::Local {
            callback_codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
        },
    }
}

/// Accept one container evaluation and a later queued request
fn container_restore_fixture() -> (
    ServingFixture,
    SupervisorActionAuthority,
    ReturnLaunch,
    RequestId,
) {
    let (mut fixture, authority) = awaiting_return_fixture();
    let later = accept_post_return_request(&mut fixture);
    let launch = container_launch(&fixture);
    let decision = ReturnDecision::Launch(Box::new(launch.clone()));
    decision
        .validate_for(
            &ReturnContext::AlreadyCompleted {
                task_id: completed_task(&fixture),
                result_ref: "test serving fixture".into(),
            },
            fixture.spec.thread,
        )
        .unwrap();
    let ReturnTaskAcceptance::Inserted { state_revision, .. } = fixture
        .store
        .accept_return_task_for_authority(container_input(&fixture, authority, launch.clone()))
        .unwrap()
    else {
        panic!("the first exact container launch must insert its task");
    };
    let mut current = authority;
    current.expected_state_revision = state_revision;
    (fixture, current, launch, later.request_id)
}

fn assert_container_restore_reserved(fixture: &ServingFixture, task_id: TaskId, later: RequestId) {
    assert!(matches!(
        saved_loan(&fixture.store, fixture.authority).map(|loan| loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Restoring { resume_task_id, .. }
        }) if resume_task_id == task_id
    ));
    assert_ne!(
        saved_resource(&fixture.store, fixture.authority).registered_background_task,
        Some(task_id)
    );
    assert_eq!(
        request_state(fixture, later),
        Some(ResourceRequestState::Queued)
    );
}

#[test]
fn container_return_holds_its_loan_and_closes_only_on_a_confirmed_removal() {
    let (mut fixture, authority, launch, later) = container_restore_fixture();
    let task_id = launch.task_id;
    assert_eq!(
        read_return_execution_mode(&fixture),
        Some(ReturnExecutionMode::Container)
    );

    start_task(&fixture.store, task_id);
    assert!(matches!(
        reconcile_restore(&mut fixture),
        RestoreReconcileOutcome::ForegroundRunning { task_id: running, .. } if running == task_id
    ));
    assert_container_restore_reserved(&fixture, task_id, later);

    finish(
        &fixture.store,
        task_id,
        ExitReason::Exit { code: 0 },
        removed(0),
    );
    let RestoreReconcileOutcome::ForegroundEnded { closure, .. } = reconcile_restore(&mut fixture)
    else {
        panic!("exit 0 with a removed container must close the loan");
    };
    assert!(matches!(
        closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::ForegroundReturnEnded { task_id: ended, .. }
        } if ended == task_id
    ));
    assert_eq!(
        restore_closure_basis(&fixture.store, authority.action_id).as_deref(),
        Some("container_ended")
    );
    let ResourceQueueReconcileOutcome::IdleServing { request, proof, .. } = fixture
        .store
        .reconcile_resource_queue_for_authority(fixture.authority, fixture.resource.id)
        .unwrap()
    else {
        panic!("the removed container must let the queue continue");
    };
    assert_eq!(request.request_id, later);
    assert_eq!(
        proof,
        IdleBoundaryProof::ForegroundReturnEnded {
            loan_id: authority.loan_id,
            task_id,
        }
    );
}

#[test]
fn container_return_without_success_or_witness_stays_reserved_until_resolved() {
    let unconfirmed = TaskExitEvidence {
        process_group: ProcessGroupExitEvidence::Unconfirmed,
        container: ContainerExitEvidence::Unconfirmed,
    };
    let cases: [(
        &str,
        ExitReason,
        TaskExitEvidence,
        RestoreAttentionReason,
        bool,
    ); 4] = [
        (
            "failed with a removed container",
            ExitReason::Exit { code: 2 },
            removed(2),
            RestoreAttentionReason::ContainerEnded {
                state: ProcessStatus::Failed,
            },
            true,
        ),
        (
            "never started",
            ExitReason::SpawnFailed {
                message: "No such image".into(),
            },
            TaskExitEvidence {
                process_group: ProcessGroupExitEvidence::Unconfirmed,
                container: ContainerExitEvidence::NeverStarted,
            },
            RestoreAttentionReason::ContainerEnded {
                state: ProcessStatus::Failed,
            },
            true,
        ),
        (
            "succeeded without removal",
            ExitReason::Exit { code: 0 },
            unconfirmed,
            RestoreAttentionReason::ContainerExitUnconfirmed {
                state: ProcessStatus::Succeeded,
            },
            false,
        ),
        (
            "only the Docker clients exited",
            ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::ConfirmedExited.into(),
            RestoreAttentionReason::ContainerExitUnconfirmed {
                state: ProcessStatus::Succeeded,
            },
            false,
        ),
    ];
    for (name, reason, evidence, expected, resolvable) in cases {
        let (mut fixture, authority, launch, later) = container_restore_fixture();
        let task_id = launch.task_id;
        start_task(&fixture.store, task_id);
        finish(&fixture.store, task_id, reason, evidence);

        match reconcile_restore(&mut fixture) {
            RestoreReconcileOutcome::Attention { reason, .. } => {
                assert_eq!(reason, expected, "{name}");
            }
            other => panic!("{name}: expected attention, got {other:?}"),
        }
        assert_container_restore_reserved(&fixture, task_id, later);

        let result = fixture
            .store
            .resolve_ended_restore_for_authority(EndedRestoreResolution {
                authority,
                task_id,
                reason: format!("{name} evaluation"),
            });
        if resolvable {
            assert!(
                matches!(
                    result,
                    Ok(ref closure) if matches!(
                        closure.loan.state,
                        LoanState::Closed { result: LoanClosure::RestoreEnded { .. } }
                    )
                ),
                "{name}: {result:?}"
            );
        } else {
            assert!(
                matches!(
                    result,
                    Err(ReturnDecisionError::RestoreReleaseUnproven { .. })
                ),
                "{name}: {result:?}"
            );
            assert_container_restore_reserved(&fixture, task_id, later);
        }
    }
}

#[test]
fn an_unconfirmed_container_return_is_released_by_the_foreground_operator_binding() {
    let (mut fixture, authority, launch, _later) = container_restore_fixture();
    let task_id = launch.task_id;
    start_task(&fixture.store, task_id);
    finish(
        &fixture.store,
        task_id,
        ExitReason::Exit { code: 0 },
        TaskExitEvidence::default(),
    );

    let direct = attestation_for(
        &fixture.store,
        fixture.authority,
        fixture.resource.id,
        task_id,
        OperatorStateBinding::RestoringReturn {
            loan_id: authority.loan_id,
            action_id: authority.action_id,
        },
    );
    assert!(
        fixture
            .store
            .attest_trainer_gpu_free_for_authority(fixture.authority, direct)
            .is_err(),
        "the direct-segment binding names another execution mode"
    );

    let attestation = attestation_for(
        &fixture.store,
        fixture.authority,
        fixture.resource.id,
        task_id,
        OperatorStateBinding::RestoringForegroundReturn {
            loan_id: authority.loan_id,
            action_id: authority.action_id,
        },
    );
    let receipt = fixture
        .store
        .attest_trainer_gpu_free_for_authority(fixture.authority, attestation)
        .unwrap()
        .receipt;
    assert_eq!(
        receipt.evidence.trainer_end,
        AttestedTrainerEnd::Finished {
            outcome: ExitReason::Exit { code: 0 },
            process_group_exit: ProcessGroupExitEvidence::Unconfirmed,
            container_exit: Some(ContainerExitEvidence::Unconfirmed),
        }
    );
    assert!(matches!(
        receipt.evidence.trainer_launch,
        AttestedTrainerLaunch::ContainerReturn { action_id, .. } if action_id == authority.action_id
    ));
    assert!(matches!(
        receipt.outcome,
        OperatorGpuFreeOutcome::RestoreClosedServing { .. }
    ));
}

#[test]
fn the_container_mount_sources_must_exist_on_the_authority() {
    let (fixture, authority) = awaiting_return_fixture();
    let mut launch = container_launch(&fixture);
    let mut spec = container_spec(&fixture, true);
    let NormalizedWorkload::Container(container) = &mut spec.workload else {
        unreachable!("the spec is a container");
    };
    container.mounts[0].source = Path::new("/nonexistent/homebased-test").to_path_buf();
    launch.work = ReturnWork::EvaluationOrNextEpoch {
        completed_task: completed_task(&fixture),
        spec: CommandSpec::try_from(spec).unwrap(),
    };
    let mut store = fixture.store;
    assert!(
        store
            .accept_return_task_for_authority(ReturnTaskAcceptanceInput {
                authority,
                launch,
                executor_env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                origin: ReturnTaskOrigin::Local {
                    callback_codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
                },
            })
            .is_err()
    );
}
