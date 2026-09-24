//! Operator attestation for a trainer that ended with no release proof
//!
//! The trainer is the registered trainer, a first background launch task, or a
//! Restoring loan's direct-segment return task that ended before registration
//! A Restoring loan's native foreground return task that ended or was lost
//! without release proof has its own binding
//! The automatic proof stays fail-closed for an unbound trainer. Only one exact,
//! explicit attestation releases it, and the attestation commits its receipt and
//! the queue or loan transition together. It never claims a confirmed exit, a
//! released trainer lock, a completed result, or a resumable stop

use super::background::{LaunchFixture, registered_launch, saved_loan_for};
use super::fixtures::{ServingFixture, TrainerAssociationFixture, resource, spec};
use super::restore::{
    awaiting_return_fixture, evaluation, launch_input, resume_input, start_task,
    stopped_return_fixture,
};
use crate::daemon::actors::{StoreActor, StoreMsg, call};
use crate::domain::{ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskId, ThreadId};
use crate::machine::MachineId;
use crate::resource::operator_release::{
    AttestedTrainerAssociation, AttestedTrainerEnd, AttestedTrainerLaunch, OperatorAttestationId,
    OperatorGpuFreeAttestation, OperatorGpuFreeConfirmation, OperatorGpuFreeEvidence,
    OperatorGpuFreeOutcome, OperatorGpuFreeRefusal, OperatorGpuFreeResolution, OperatorObservation,
    OperatorStateBinding,
};
use crate::resource::store::ResourceTaskAcceptanceInput;
use crate::resource::{
    ActionId, IdleBoundaryDecision, IdleBoundaryProof, IdleProofGap, Loan, LoanClosure, LoanId,
    LoanPhase, LoanState, Resource, ResourceId, ResourceQueueAttentionReason,
    ResourceQueueReconcileOutcome, ResourceRequest, ResourceRequestState, ResourceRevision,
    RestoreAttentionReason, ReturnContext, ServingReleaseProvenance, SupervisorActionAuthority,
    SupervisorNoticePayload,
};
use crate::store::{
    BackgroundLaunchAcceptance, BackgroundLaunchError, BackgroundLaunchPhase,
    EndedRestoreResolution, OperatorGpuFreeError, RestoreReconcileOutcome, ReturnDecisionError,
    ReturnTaskAcceptance, Store,
};
use crate::submission::{RequestId, normalized_spec_sha256};
use ractor::Actor;
use rusqlite::params;
use serde_json::json;
use tempfile::tempdir;
use uuid::Uuid;

const OBSERVATION: &str = "nvidia-smi on the authority lists no trainer process";

pub(super) fn attestation_for(
    store: &Store,
    authority: MachineId,
    resource_id: ResourceId,
    task_id: TaskId,
    state_binding: OperatorStateBinding,
) -> OperatorGpuFreeAttestation {
    let revision = store
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource_id)
        .unwrap()
        .resource
        .state_revision;
    OperatorGpuFreeAttestation {
        operation_id: OperatorAttestationId::new(),
        resource_id,
        authority_machine: authority,
        task_id,
        expected_state_revision: revision,
        state_binding,
        observation: OperatorObservation::try_from(OBSERVATION.to_owned()).unwrap(),
        confirmation: OperatorGpuFreeConfirmation::OperatorConfirmedGpuFree,
    }
}

fn attestation(
    fixture: &LaunchFixture,
    task_id: TaskId,
    state_binding: OperatorStateBinding,
) -> OperatorGpuFreeAttestation {
    attestation_for(
        &fixture.store,
        fixture.authority,
        fixture.resource.id,
        task_id,
        state_binding,
    )
}

fn attest(
    fixture: &mut LaunchFixture,
    attestation: OperatorGpuFreeAttestation,
) -> Result<OperatorGpuFreeResolution, OperatorGpuFreeError> {
    fixture
        .store
        .attest_trainer_gpu_free_for_authority(fixture.authority, attestation)
}

/// Build a fresh attestation for the current revision and commit it
fn attest_binding(
    fixture: &mut LaunchFixture,
    task_id: TaskId,
    state_binding: OperatorStateBinding,
) -> Result<OperatorGpuFreeResolution, OperatorGpuFreeError> {
    let attestation = attestation(fixture, task_id, state_binding);
    attest(fixture, attestation)
}

fn refusal(
    result: Result<OperatorGpuFreeResolution, OperatorGpuFreeError>,
) -> OperatorGpuFreeRefusal {
    match result {
        Err(OperatorGpuFreeError::Refused(refusal)) => refusal,
        other => panic!("expected a typed refusal, got {other:?}"),
    }
}

fn attestation_count(store: &Store) -> i64 {
    store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_operator_attestations",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

fn fail(fixture: &LaunchFixture, task_id: TaskId, code: i32, evidence: ProcessGroupExitEvidence) {
    fixture
        .store
        .cas_exit_with_evidence(
            task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code },
            evidence,
        )
        .unwrap()
        .unwrap();
}

fn lose(fixture: &LaunchFixture, task_id: TaskId) {
    fixture
        .store
        .cas_status(task_id, ProcessStatus::Running, ProcessStatus::Lost)
        .unwrap()
        .unwrap();
}

/// Check the activation gate that an assigned command task must pass
fn activation_accepts(fixture: &LaunchFixture, loan: &Loan, request: &ResourceRequest) -> bool {
    let input = ResourceTaskAcceptanceInput {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        request_id: request.request_id,
        task_id: request.task_id,
        acceptance_sequence: request.acceptance_sequence,
        loan_id: loan.id,
        expected_state_revision: fixture.saved_resource().state_revision,
        command_spec: request.spec().clone(),
        executor_env: fixture.trainer.env.clone(),
    };
    crate::resource::store::assigned_resource_request_for_acceptance(&fixture.store.conn, &input)
        .is_ok()
}

/// Open the release action for a running registered trainer and return its identities
fn awaiting_release(fixture: &mut LaunchFixture) -> (Loan, ActionId, ResourceRevision) {
    let ResourceQueueReconcileOutcome::ReleaseRequired { loan, notice } = fixture.reconcile()
    else {
        panic!("a running registered trainer must open a release action for queued work");
    };
    let LoanState::Active {
        phase: LoanPhase::AwaitingRelease { action_id, .. },
    } = loan.state
    else {
        panic!("the new loan must await release");
    };
    (loan, action_id, notice.state_revision)
}

#[test]
fn unbound_failed_trainer_stays_reserved_until_the_attestation_serves_the_queue() {
    let mut fixture = LaunchFixture::new();
    let launch_request = RequestId::new();
    let task = registered_launch(&mut fixture, launch_request);
    fail(&fixture, task, 3, ProcessGroupExitEvidence::Unconfirmed);
    let request = fixture.queue_request();

    // no association names a lock, so no release action can open and the queue waits
    for _ in 0..2 {
        assert!(matches!(
            fixture.reconcile(),
            ResourceQueueReconcileOutcome::AttentionRequired {
                reason: ResourceQueueAttentionReason::BackgroundTaskNotRunning { task_id, .. },
                ..
            } if task_id == task
        ));
    }
    assert!(saved_loan_for(&fixture).is_none());
    let before = fixture.saved_resource();

    let resolution = attest_binding(&mut fixture, task, OperatorStateBinding::NoLoan).unwrap();
    assert!(!resolution.replayed);
    let receipt = resolution.receipt;
    // the snapshot keeps the unconfirmed wrapper exit; the attestation proves nothing more
    assert_eq!(
        receipt.evidence,
        OperatorGpuFreeEvidence {
            trainer_end: AttestedTrainerEnd::Finished {
                outcome: ExitReason::Exit { code: 3 },
                process_group_exit: ProcessGroupExitEvidence::Unconfirmed,
                container_exit: None,
            },
            trainer_launch: AttestedTrainerLaunch::FirstBackgroundLaunch {
                request_id: launch_request,
            },
            normalized_spec_sha256: normalized_spec_sha256(&fixture.trainer_spec()).unwrap(),
            trainer_association: AttestedTrainerAssociation::Missing,
        }
    );

    // the registration clears and the next request in serving order serves in the same transaction
    let OperatorGpuFreeOutcome::IdleServing {
        loan,
        request: selected,
    } = &receipt.outcome
    else {
        panic!("queued work must be selected with the attestation");
    };
    assert_eq!(selected.request_id, request.request_id);
    assert_eq!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: ReturnContext::Idle,
                current_request_id: request.request_id,
                release_provenance: ServingReleaseProvenance::IdleBoundary {
                    proof: IdleBoundaryProof::OperatorAttestedGpuFree {
                        operation_id: receipt.attestation.operation_id,
                        task_id: task,
                    },
                },
            },
        }
    );
    let saved = fixture.saved_resource();
    assert_eq!(saved.registered_background_task, None);
    assert_eq!(saved.state_revision.get(), before.state_revision.get() + 2);
    assert_eq!(receipt.state_revision, saved.state_revision);
    assert_eq!(saved_loan_for(&fixture), Some(loan.clone()));
    assert!(activation_accepts(&fixture, loan, selected));
}

#[test]
fn running_trainer_wrong_identities_and_stale_state_are_refused_without_records() {
    let mut fixture = LaunchFixture::new();
    let task = registered_launch(&mut fixture, RequestId::new());

    // a running wrapper still owns the GPU
    assert_eq!(
        refusal(attest_binding(
            &mut fixture,
            task,
            OperatorStateBinding::NoLoan
        )),
        OperatorGpuFreeRefusal::TaskNotEnded {
            task_id: task,
            state: ProcessStatus::Running,
        }
    );

    lose(&fixture, task);
    let before = fixture.saved_resource();
    let other_machine = MachineId::new();
    let other_task = TaskId::new();
    type Change = fn(&mut OperatorGpuFreeAttestation, MachineId, TaskId);
    let cases: [(&str, Change, MachineId, OperatorGpuFreeRefusal); 6] = [
        (
            "unregistered task",
            |attestation, _, task| attestation.task_id = task,
            fixture.authority,
            OperatorGpuFreeRefusal::NotRegisteredTrainer {
                task_id: other_task,
                registered: Some(task),
            },
        ),
        (
            "stale revision",
            |attestation, _, _| {
                attestation.expected_state_revision =
                    ResourceRevision::new(attestation.expected_state_revision.get() - 1);
            },
            fixture.authority,
            OperatorGpuFreeRefusal::StaleRevision {
                expected: ResourceRevision::new(before.state_revision.get() - 1),
                actual: before.state_revision,
            },
        ),
        (
            "attestation names another authority",
            |attestation, machine, _| attestation.authority_machine = machine,
            fixture.authority,
            OperatorGpuFreeRefusal::WrongAuthority {
                expected: fixture.authority,
                found: other_machine,
            },
        ),
        (
            "another daemon receives it",
            |attestation, machine, _| attestation.authority_machine = machine,
            other_machine,
            OperatorGpuFreeRefusal::WrongAuthority {
                expected: fixture.authority,
                found: other_machine,
            },
        ),
        (
            "binding names a release action that does not exist",
            |attestation, _, _| {
                attestation.state_binding = OperatorStateBinding::AwaitingRelease {
                    loan_id: LoanId::new(),
                    action_id: ActionId::new(),
                };
            },
            fixture.authority,
            OperatorGpuFreeRefusal::LoanStateChanged { current_loan: None },
        ),
        (
            "unknown resource",
            |attestation, _, _| attestation.resource_id = ResourceId::new(),
            fixture.authority,
            OperatorGpuFreeRefusal::ResourceNotFound,
        ),
    ];
    for (name, change, daemon, expected) in cases {
        let mut changed = attestation(&fixture, task, OperatorStateBinding::NoLoan);
        change(&mut changed, other_machine, other_task);
        let result = fixture
            .store
            .attest_trainer_gpu_free_for_authority(daemon, changed);
        assert_eq!(refusal(result), expected, "{name}");
        assert_eq!(attestation_count(&fixture.store), 0, "{name}");
        assert_eq!(fixture.saved_resource(), before, "{name}");
    }

    // the observation and the explicit confirmation are required at the boundary
    assert_eq!(
        OperatorObservation::try_from("  ".to_owned()),
        Err(OperatorGpuFreeRefusal::EmptyObservation)
    );
    let mut encoded =
        serde_json::to_value(attestation(&fixture, task, OperatorStateBinding::NoLoan)).unwrap();
    encoded["observation"] = json!(" ");
    assert!(serde_json::from_value::<OperatorGpuFreeAttestation>(encoded.clone()).is_err());
    encoded["observation"] = json!(OBSERVATION);
    encoded.as_object_mut().unwrap().remove("confirmation");
    assert!(serde_json::from_value::<OperatorGpuFreeAttestation>(encoded).is_err());
}

#[test]
fn missing_or_changed_launch_records_are_refused() {
    let change_thread = |fixture: &LaunchFixture, task: TaskId| {
        fixture
            .store
            .conn
            .execute(
                "UPDATE tasks SET thread_id = ?1 WHERE id = ?2",
                params![ThreadId(Uuid::now_v7()).to_string(), task.to_string()],
            )
            .unwrap();
    };
    let drop_receipt = |fixture: &LaunchFixture, task: TaskId| {
        fixture
            .store
            .conn
            .execute(
                "DELETE FROM resource_background_launches WHERE task_id = ?1",
                [task.to_string()],
            )
            .unwrap();
    };
    for (name, change) in [
        (
            "no launch receipt",
            &drop_receipt as &dyn Fn(&LaunchFixture, TaskId),
        ),
        ("task row no longer matches its identity", &change_thread),
    ] {
        let mut fixture = LaunchFixture::new();
        let task = registered_launch(&mut fixture, RequestId::new());
        fail(&fixture, task, 1, ProcessGroupExitEvidence::ConfirmedExited);
        change(&fixture, task);
        let before = fixture.saved_resource();

        assert_eq!(
            refusal(attest_binding(
                &mut fixture,
                task,
                OperatorStateBinding::NoLoan
            )),
            OperatorGpuFreeRefusal::TrainerLaunchUnproven { task_id: task },
            "{name}"
        );
        assert_eq!(attestation_count(&fixture.store), 0, "{name}");
        assert_eq!(fixture.saved_resource(), before, "{name}");
    }
}

#[test]
fn lost_trainer_release_keeps_the_return_obligation_for_the_supervisor() {
    let mut fixture = LaunchFixture::new();
    let task = registered_launch(&mut fixture, RequestId::new());
    let request = fixture.queue_request();
    let (loan, action_id, revision) = awaiting_release(&mut fixture);
    fixture
        .store
        .cancel_resource_request_before_activation(
            fixture.authority,
            request.request_id,
            request.task_id,
            fixture.resource.id,
            request.origin_machine,
        )
        .unwrap();
    lose(&fixture, task);

    // the automatic proof stays fail-closed for a lost, unbound trainer
    assert!(
        fixture
            .store
            .complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision
            )
            .is_err()
    );
    assert_eq!(saved_loan_for(&fixture), Some(loan.clone()));

    // the binding must name the exact release action
    for binding in [
        OperatorStateBinding::NoLoan,
        OperatorStateBinding::AwaitingRelease {
            loan_id: loan.id,
            action_id: ActionId::new(),
        },
    ] {
        assert_eq!(
            refusal(attest_binding(&mut fixture, task, binding)),
            OperatorGpuFreeRefusal::LoanStateChanged {
                current_loan: Some(loan.id)
            }
        );
    }
    assert_eq!(attestation_count(&fixture.store), 0);

    let binding = OperatorStateBinding::AwaitingRelease {
        loan_id: loan.id,
        action_id,
    };
    let receipt = attest_binding(&mut fixture, task, binding).unwrap().receipt;
    assert_eq!(receipt.evidence.trainer_end, AttestedTrainerEnd::Lost);
    let lost = ReturnContext::LostWithoutResult { task_id: task };
    let OperatorGpuFreeOutcome::ReleaseResolvedReturnRequired {
        loan: returned,
        notice,
    } = &receipt.outcome
    else {
        panic!("an empty queue must reserve the supervisor return decision");
    };
    assert_eq!(
        returned.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingReturn {
                action_id: notice.action_id,
                return_context: lost.clone(),
            },
        }
    );
    assert_eq!(
        notice.payload,
        SupervisorNoticePayload::ReturnRequired {
            return_context: lost.clone()
        }
    );
    assert_eq!(notice.state_revision, receipt.state_revision);
    // the trainer stays registered until the supervisor decides the return
    assert_eq!(
        fixture.saved_resource().registered_background_task,
        Some(task)
    );

    // the supervisor can close the lost run without resuming it
    let authority = SupervisorActionAuthority {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        loan_id: returned.id,
        action_id: notice.action_id,
        expected_state_revision: notice.state_revision,
        supervisor: fixture.resource.supervisor,
        assignment_revision: fixture.resource.assignment_revision,
    };
    let closure = fixture
        .store
        .record_no_resume_for_authority(authority, "operator resolved the lost run".into())
        .unwrap();
    assert!(matches!(
        closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::NoResume { return_context, .. }
        } if return_context == lost
    ));
    assert_eq!(fixture.saved_resource().registered_background_task, None);
    fixture.launch(RequestId::new());
}

#[test]
fn failed_trainer_release_serves_requests_in_order_and_replays_after_restart() {
    let mut fixture = LaunchFixture::new();
    let task = registered_launch(&mut fixture, RequestId::new());
    let first = fixture.queue_request();
    let second = fixture.queue_request();
    let (loan, action_id, revision) = awaiting_release(&mut fixture);
    // a confirmed wrapper exit does not cover the detached worker without an association
    fail(&fixture, task, 3, ProcessGroupExitEvidence::ConfirmedExited);
    assert!(
        fixture
            .store
            .complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision
            )
            .is_err()
    );

    let binding = OperatorStateBinding::AwaitingRelease {
        loan_id: loan.id,
        action_id,
    };
    let saved_attestation = attestation(&fixture, task, binding);
    let receipt = attest(&mut fixture, saved_attestation.clone())
        .unwrap()
        .receipt;
    let OperatorGpuFreeOutcome::ReleaseResolvedServing {
        loan: serving,
        request: selected,
    } = &receipt.outcome
    else {
        panic!("the next queued request must serve after the attestation");
    };
    assert_eq!(selected.request_id, first.request_id);
    // an ended run without a result is never a completed or resumable context
    assert_eq!(
        serving.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: ReturnContext::EndedWithoutResult {
                    task_id: task,
                    outcome: ExitReason::Exit { code: 3 },
                },
                current_request_id: first.request_id,
                release_provenance: ServingReleaseProvenance::OperatorAttestedGpuFree {
                    operation_id: saved_attestation.operation_id,
                    action_id,
                    task_id: task,
                },
            },
        }
    );
    assert_eq!(
        fixture
            .store
            .resource_requests(fixture.authority, fixture.resource.id)
            .unwrap()
            .into_iter()
            .find(|request| request.request_id == second.request_id)
            .unwrap()
            .state,
        ResourceRequestState::Queued
    );
    assert_eq!(
        fixture.saved_resource().registered_background_task,
        Some(task)
    );
    assert!(activation_accepts(&fixture, serving, selected));

    // a Serving loan can already run other GPU work, so it is never overridden
    assert_eq!(
        refusal(attest_binding(&mut fixture, task, binding)),
        OperatorGpuFreeRefusal::LoanNotAwaitingRelease { loan_id: loan.id }
    );

    fixture.reopen();
    let replay = attest(&mut fixture, saved_attestation.clone()).unwrap();
    assert!(replay.replayed);
    assert_eq!(
        serde_json::to_value(&replay.receipt).unwrap(),
        serde_json::to_value(&receipt).unwrap()
    );
    let mut changed = saved_attestation.clone();
    changed.observation = OperatorObservation::try_from("a different account".to_owned()).unwrap();
    assert_eq!(
        refusal(attest(&mut fixture, changed)),
        OperatorGpuFreeRefusal::ConflictingRetry {
            operation_id: saved_attestation.operation_id
        }
    );
    assert_eq!(attestation_count(&fixture.store), 1);
    assert!(
        fixture
            .store
            .operator_attestation_receipt_for_authority(
                fixture.authority,
                saved_attestation.operation_id
            )
            .unwrap()
            .is_some()
    );
}

#[test]
fn idle_boundary_survives_restart_and_serves_a_later_request() {
    let mut fixture = LaunchFixture::new();
    let task = registered_launch(&mut fixture, RequestId::new());
    lose(&fixture, task);
    let before = fixture.saved_resource();

    let saved_attestation = attestation(&fixture, task, OperatorStateBinding::NoLoan);
    let receipt = attest(&mut fixture, saved_attestation.clone())
        .unwrap()
        .receipt;
    assert!(matches!(
        receipt.outcome,
        OperatorGpuFreeOutcome::IdleBoundary
    ));
    let saved = fixture.saved_resource();
    assert_eq!(saved.registered_background_task, None);
    assert_eq!(saved.state_revision.get(), before.state_revision.get() + 1);

    fixture.reopen();
    assert!(
        attest(&mut fixture, saved_attestation.clone())
            .unwrap()
            .replayed
    );
    let request = fixture.queue_request();
    assert!(matches!(
        fixture.reconcile(),
        ResourceQueueReconcileOutcome::IdleServing {
            request: served,
            proof: IdleBoundaryProof::OperatorAttestedGpuFree { operation_id, task_id },
            ..
        } if served.request_id == request.request_id
            && operation_id == saved_attestation.operation_id
            && task_id == task
    ));
}

#[test]
fn first_background_launch_proceeds_only_through_the_saved_boundary() {
    let mut fixture = LaunchFixture::new();
    let task = registered_launch(&mut fixture, RequestId::new());
    fail(&fixture, task, 1, ProcessGroupExitEvidence::Unconfirmed);

    let blocked = fixture.input(RequestId::new(), fixture.trainer_spec());
    assert!(matches!(
        fixture.store.accept_background_launch_for_authority(blocked),
        Err(BackgroundLaunchError::PredecessorReleaseUnproven { task_id }) if task_id == task
    ));

    attest_binding(&mut fixture, task, OperatorStateBinding::NoLoan).unwrap();
    let next = fixture.input(RequestId::new(), fixture.trainer_spec());
    let next_task = next.task_id;
    assert!(matches!(
        fixture.store.accept_background_launch_for_authority(next),
        Ok(BackgroundLaunchAcceptance::Inserted { task, .. }) if task == next_task
    ));
    // the new launch supersedes the boundary, so queued work waits for its start
    let request = fixture.queue_request();
    assert!(matches!(
        fixture.reconcile(),
        ResourceQueueReconcileOutcome::AttentionRequired {
            request: waiting,
            reason: ResourceQueueAttentionReason::BackgroundLaunchPending { task_id },
        } if waiting.request_id == request.request_id && task_id == next_task
    ));
}

#[test]
fn direct_segment_return_trainer_that_failed_before_binding_can_be_attested() {
    let (mut fixture, authority, decision) = stopped_return_fixture();
    let input = resume_input(
        authority,
        fixture.task_id,
        &decision.selected_checkpoint.generation_id,
    );
    let (request_id, task_id) = (input.launch.request_id, input.launch.task_id);
    assert!(matches!(
        fixture
            .store
            .accept_return_task_for_authority(input)
            .unwrap(),
        ReturnTaskAcceptance::Inserted { .. }
    ));
    start_task(&fixture.store, task_id);
    assert!(matches!(
        fixture
            .store
            .reconcile_restoring_loan_for_authority(fixture.authority, fixture.resource.id)
            .unwrap(),
        RestoreReconcileOutcome::Closed { .. }
    ));
    fixture
        .store
        .cas_exit_with_evidence(
            task_id,
            ProcessStatus::Running,
            &ExitReason::Signal { signal: 9 },
            ProcessGroupExitEvidence::Unconfirmed,
        )
        .unwrap()
        .unwrap();

    let attestation = attestation_for(
        &fixture.store,
        fixture.authority,
        fixture.resource.id,
        task_id,
        OperatorStateBinding::NoLoan,
    );
    let receipt = fixture
        .store
        .attest_trainer_gpu_free_for_authority(fixture.authority, attestation)
        .unwrap()
        .receipt;
    assert_eq!(
        receipt.evidence.trainer_launch,
        AttestedTrainerLaunch::DirectSegmentReturn {
            action_id: authority.action_id,
            request_id,
        }
    );
    assert!(matches!(
        receipt.outcome,
        OperatorGpuFreeOutcome::IdleBoundary
    ));
}

#[test]
fn attestation_table_checks_the_outcome_and_the_explicit_confirmation() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();

    let insert = |outcome: &str, confirmation: serde_json::Value| {
        let (operation, task) = (Uuid::now_v7(), TaskId::new());
        let receipt = json!({
            "attestation": {
                "operation_id": operation,
                "resource_id": resource.id,
                "task_id": task,
                "observation": OBSERVATION,
                "confirmation": confirmation,
            },
            "evidence": {},
            "outcome": { "type": outcome },
        });
        store.conn.execute(
            "INSERT INTO resource_operator_attestations
                (operation_id, resource_id, task_id, receipt_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                operation.to_string(),
                resource.id.as_uuid().to_string(),
                task.to_string(),
                receipt.to_string()
            ],
        )
    };
    let confirmed = || json!("operator_confirmed_gpu_free");
    for outcome in [
        "release_resolved_serving",
        "release_resolved_return_required",
        "idle_serving",
        "idle_boundary",
        "restore_closed_serving",
        "restore_closed_idle_boundary",
    ] {
        assert!(insert(outcome, confirmed()).is_ok(), "{outcome}");
    }
    assert!(insert("unknown", confirmed()).is_err());
    // a receipt without the explicit confirmation cannot be stored
    assert!(insert("idle_boundary", json!(null)).is_err());
    assert!(insert("idle_boundary", json!("confirmed")).is_err());
}

#[tokio::test]
async fn store_actor_commits_and_replays_one_attestation() {
    let mut fixture = LaunchFixture::new();
    let task = registered_launch(&mut fixture, RequestId::new());
    lose(&fixture, task);
    let saved_attestation = attestation(&fixture, task, OperatorStateBinding::NoLoan);
    let authority = fixture.authority;

    let (store, handle) = StoreActor::spawn(None, StoreActor, fixture.db_path())
        .await
        .unwrap();
    let attest = |attestation: OperatorGpuFreeAttestation| {
        call(&store, move |reply| {
            StoreMsg::AttestTrainerGpuFreeForAuthority {
                authority_machine: authority,
                attestation: Box::new(attestation),
                reply,
            }
        })
    };
    let first = attest(saved_attestation.clone()).await.unwrap().unwrap();
    assert!(!first.replayed);
    let replay = attest(saved_attestation.clone()).await.unwrap().unwrap();
    assert!(replay.replayed);
    let saved = fixture
        .store
        .operator_attestation_receipt_for_authority(authority, saved_attestation.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(saved.attestation, saved_attestation);
    store.stop(None);
    let _ = handle.await;
}

/// Start a first background launch and end it before the owner registers its start
fn launch_ended_before_registration(
    fixture: &mut LaunchFixture,
    request_id: RequestId,
    evidence: ProcessGroupExitEvidence,
) -> TaskId {
    let task = fixture.launch(request_id);
    start_task(&fixture.store, task);
    fail(fixture, task, 1, evidence);
    task
}

fn resource_count_state(fixture: &LaunchFixture) -> (Resource, Option<Loan>, i64) {
    (
        fixture.saved_resource(),
        saved_loan_for(fixture),
        attestation_count(&fixture.store),
    )
}

#[test]
fn first_launch_that_ended_before_registration_releases_only_through_its_exact_attestation() {
    let mut fixture = LaunchFixture::new();
    let launch_request = RequestId::new();
    let task = launch_ended_before_registration(
        &mut fixture,
        launch_request,
        ProcessGroupExitEvidence::Unconfirmed,
    );
    // the ended task is not registered, and neither the slot nor the queue is free
    assert_eq!(fixture.saved_resource().registered_background_task, None);
    let blocked = fixture.input(RequestId::new(), fixture.trainer_spec());
    assert!(matches!(
        fixture.store.accept_background_launch_for_authority(blocked),
        Err(BackgroundLaunchError::PredecessorReleaseUnproven { task_id }) if task_id == task
    ));
    let before = resource_count_state(&fixture);

    let binding = OperatorStateBinding::FirstBackgroundLaunch {
        request_id: launch_request,
    };
    let other_task = TaskId::new();
    let other_request = RequestId::new();
    let cases = [
        (
            "the registered-trainer binding names no registration",
            attestation(&fixture, task, OperatorStateBinding::NoLoan),
            OperatorGpuFreeRefusal::NotRegisteredTrainer {
                task_id: task,
                registered: None,
            },
        ),
        (
            "another launch request",
            attestation(
                &fixture,
                task,
                OperatorStateBinding::FirstBackgroundLaunch {
                    request_id: other_request,
                },
            ),
            OperatorGpuFreeRefusal::LaunchNotAwaitingRelease {
                request_id: other_request,
                current_launch: Some(launch_request),
            },
        ),
        (
            "another task",
            attestation(&fixture, other_task, binding),
            OperatorGpuFreeRefusal::NotBoundTask {
                task_id: other_task,
                bound: task,
            },
        ),
        (
            "a restore binding with no loan",
            attestation(
                &fixture,
                task,
                OperatorStateBinding::RestoringReturn {
                    loan_id: LoanId::new(),
                    action_id: ActionId::new(),
                },
            ),
            OperatorGpuFreeRefusal::LoanStateChanged { current_loan: None },
        ),
    ];
    for (name, changed, expected) in cases {
        assert_eq!(refusal(attest(&mut fixture, changed)), expected, "{name}");
        assert_eq!(resource_count_state(&fixture), before, "{name}");
    }
    let other_authority = MachineId::new();
    let saved_attestation = attestation(&fixture, task, binding);
    assert_eq!(
        refusal(
            fixture
                .store
                .attest_trainer_gpu_free_for_authority(other_authority, saved_attestation.clone())
        ),
        OperatorGpuFreeRefusal::WrongAuthority {
            expected: other_authority,
            found: fixture.authority,
        }
    );
    assert_eq!(resource_count_state(&fixture), before);

    let receipt = attest(&mut fixture, saved_attestation.clone())
        .unwrap()
        .receipt;
    assert_eq!(
        receipt.evidence,
        OperatorGpuFreeEvidence {
            trainer_end: AttestedTrainerEnd::Finished {
                outcome: ExitReason::Exit { code: 1 },
                process_group_exit: ProcessGroupExitEvidence::Unconfirmed,
                container_exit: None,
            },
            trainer_launch: AttestedTrainerLaunch::FirstBackgroundLaunch {
                request_id: launch_request,
            },
            normalized_spec_sha256: normalized_spec_sha256(&fixture.trainer_spec()).unwrap(),
            trainer_association: AttestedTrainerAssociation::Missing,
        }
    );
    assert!(matches!(
        receipt.outcome,
        OperatorGpuFreeOutcome::IdleBoundary
    ));
    let saved = fixture.saved_resource();
    assert_eq!(saved.registered_background_task, None);
    assert_eq!(
        saved.state_revision.get(),
        before.0.state_revision.get() + 1
    );
    assert_eq!(
        fixture
            .store
            .background_launch_for_authority(fixture.authority, fixture.resource.id)
            .unwrap()
            .unwrap()
            .phase,
        BackgroundLaunchPhase::Superseded
    );

    // the exact retry replays after a restart even though the revision moved
    fixture.reopen();
    let replay = attest(&mut fixture, saved_attestation.clone()).unwrap();
    assert!(replay.replayed);
    assert_eq!(
        serde_json::to_value(&replay.receipt).unwrap(),
        serde_json::to_value(&receipt).unwrap()
    );
    let mut changed = saved_attestation.clone();
    changed.observation = OperatorObservation::try_from("another account".to_owned()).unwrap();
    assert_eq!(
        refusal(attest(&mut fixture, changed)),
        OperatorGpuFreeRefusal::ConflictingRetry {
            operation_id: saved_attestation.operation_id
        }
    );

    // only the saved boundary lets the queue and the next launch proceed
    let request = fixture.queue_request();
    assert!(matches!(
        fixture.reconcile(),
        ResourceQueueReconcileOutcome::IdleServing {
            request: served,
            proof: IdleBoundaryProof::OperatorAttestedGpuFree { operation_id, task_id },
            ..
        } if served.request_id == request.request_id
            && operation_id == saved_attestation.operation_id
            && task_id == task
    ));
}

#[test]
fn queued_running_or_never_spawned_first_launch_cannot_be_attested() {
    let mut fixture = LaunchFixture::new();
    let launch_request = RequestId::new();
    let task = fixture.launch(launch_request);
    let binding = OperatorStateBinding::FirstBackgroundLaunch {
        request_id: launch_request,
    };
    let before = resource_count_state(&fixture);
    assert_eq!(
        refusal(attest_binding(&mut fixture, task, binding)),
        OperatorGpuFreeRefusal::TaskNotEnded {
            task_id: task,
            state: ProcessStatus::Queued,
        }
    );
    // a started launch that the owner has not registered yet is still live work
    start_task(&fixture.store, task);
    assert_eq!(
        refusal(attest_binding(&mut fixture, task, binding)),
        OperatorGpuFreeRefusal::TaskNotEnded {
            task_id: task,
            state: ProcessStatus::Running,
        }
    );
    assert_eq!(resource_count_state(&fixture), before);

    // a launch with automatic no-child proof needs no attestation
    let mut fixture = LaunchFixture::new();
    let never = fixture.launch(launch_request);
    fixture
        .store
        .cas_exit(never, ProcessStatus::Queued, &ExitReason::Cancelled)
        .unwrap()
        .unwrap();
    assert_eq!(
        refusal(attest_binding(&mut fixture, never, binding)),
        OperatorGpuFreeRefusal::LaunchNotAwaitingRelease {
            request_id: launch_request,
            current_launch: Some(launch_request),
        }
    );
    assert_eq!(attestation_count(&fixture.store), 0);
}

#[test]
fn lost_first_launch_attestation_serves_the_queue_in_order() {
    let mut fixture = LaunchFixture::new();
    let launch_request = RequestId::new();
    let task = fixture.launch(launch_request);
    start_task(&fixture.store, task);
    lose(&fixture, task);
    let first = fixture.queue_request();
    let second = fixture.queue_request();
    for _ in 0..2 {
        assert!(matches!(
            fixture.reconcile(),
            ResourceQueueReconcileOutcome::AttentionRequired {
                reason: ResourceQueueAttentionReason::IdleNotProven {
                    gap: IdleProofGap::BackgroundLaunchReleaseUnproven { task_id },
                },
                ..
            } if task_id == task
        ));
    }
    assert!(saved_loan_for(&fixture).is_none());

    let receipt = attest_binding(
        &mut fixture,
        task,
        OperatorStateBinding::FirstBackgroundLaunch {
            request_id: launch_request,
        },
    )
    .unwrap()
    .receipt;
    assert_eq!(receipt.evidence.trainer_end, AttestedTrainerEnd::Lost);
    let OperatorGpuFreeOutcome::IdleServing { loan, request } = &receipt.outcome else {
        panic!("the next request in serving order must serve with the attestation");
    };
    assert_eq!(request.request_id, first.request_id);
    assert!(matches!(
        &loan.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                release_provenance: ServingReleaseProvenance::IdleBoundary {
                    proof: IdleBoundaryProof::OperatorAttestedGpuFree { operation_id, task_id },
                },
                ..
            },
        } if *operation_id == receipt.attestation.operation_id && *task_id == task
    ));
    assert_eq!(saved_loan_for(&fixture).as_ref(), Some(loan));
    assert!(activation_accepts(&fixture, loan, request));
    assert_eq!(
        fixture
            .store
            .resource_requests(fixture.authority, fixture.resource.id)
            .unwrap()
            .into_iter()
            .find(|saved| saved.request_id == second.request_id)
            .unwrap()
            .state,
        ResourceRequestState::Queued
    );
}

/// Bind a same-run resume and end its task before the owner observes its start
fn restore_ended_before_confirmed_start() -> (TrainerAssociationFixture, Loan, ActionId, TaskId) {
    let (mut fixture, authority, decision) = stopped_return_fixture();
    let input = resume_input(
        authority,
        fixture.task_id,
        &decision.selected_checkpoint.generation_id,
    );
    let task_id = input.launch.task_id;
    let ReturnTaskAcceptance::Inserted { loan, .. } = fixture
        .store
        .accept_return_task_for_authority(input)
        .unwrap()
    else {
        panic!("the first exact return launch must insert its task");
    };
    start_task(&fixture.store, task_id);
    fixture
        .store
        .cas_exit_with_evidence(
            task_id,
            ProcessStatus::Running,
            &ExitReason::Signal { signal: 9 },
            ProcessGroupExitEvidence::Unconfirmed,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .reconcile_restoring_loan_for_authority(fixture.authority, fixture.resource.id)
            .unwrap(),
        RestoreReconcileOutcome::Attention { task_id: attention, .. } if attention == task_id
    ));
    // the unconfirmed wrapper exit gives the supervisor resolution no release proof
    assert!(matches!(
        fixture
            .store
            .resolve_ended_restore_for_authority(EndedRestoreResolution {
                authority: SupervisorActionAuthority {
                    loan_id: loan.id,
                    expected_state_revision: fixture
                        .store
                        .resource_snapshots_for_authority(fixture.authority)
                        .unwrap()[0]
                        .resource
                        .state_revision,
                    ..authority
                },
                task_id,
                reason: "ended before its start".into(),
            }),
        Err(ReturnDecisionError::RestoreReleaseUnproven { .. })
    ));
    (fixture, *loan, authority.action_id, task_id)
}

fn restore_state(fixture: &TrainerAssociationFixture) -> (Resource, Option<Loan>, i64) {
    let snapshot = fixture
        .store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()
        .remove(0);
    (
        snapshot.resource,
        snapshot.loan,
        attestation_count(&fixture.store),
    )
}

#[test]
fn restoring_return_that_ended_before_its_start_closes_through_its_exact_attestation() {
    let (mut fixture, loan, action_id, task_id) = restore_ended_before_confirmed_start();
    let (authority, resource_id) = (fixture.authority, fixture.resource.id);
    let attest_on = |store: &mut Store, attestation| {
        store.attest_trainer_gpu_free_for_authority(authority, attestation)
    };
    let binding = OperatorStateBinding::RestoringReturn {
        loan_id: loan.id,
        action_id,
    };
    let before = restore_state(&fixture);
    let other_task = TaskId::new();
    let cases = [
        (
            "the unregistered return task has no registered-trainer binding",
            OperatorStateBinding::NoLoan,
            task_id,
            OperatorGpuFreeRefusal::NotRegisteredTrainer {
                task_id,
                registered: before.0.registered_background_task,
            },
        ),
        (
            "another return action",
            OperatorStateBinding::RestoringReturn {
                loan_id: loan.id,
                action_id: ActionId::new(),
            },
            task_id,
            OperatorGpuFreeRefusal::LoanStateChanged {
                current_loan: Some(loan.id),
            },
        ),
        (
            "another loan",
            OperatorStateBinding::RestoringReturn {
                loan_id: LoanId::new(),
                action_id,
            },
            task_id,
            OperatorGpuFreeRefusal::LoanStateChanged {
                current_loan: Some(loan.id),
            },
        ),
        (
            "another task",
            binding,
            other_task,
            OperatorGpuFreeRefusal::NotBoundTask {
                task_id: other_task,
                bound: task_id,
            },
        ),
        (
            "a first launch binding while a loan reserves the resource",
            OperatorStateBinding::FirstBackgroundLaunch {
                request_id: RequestId::new(),
            },
            task_id,
            OperatorGpuFreeRefusal::LoanStateChanged {
                current_loan: Some(loan.id),
            },
        ),
    ];
    for (name, state_binding, task, expected) in cases {
        let changed = attestation_for(&fixture.store, authority, resource_id, task, state_binding);
        assert_eq!(
            refusal(attest_on(&mut fixture.store, changed)),
            expected,
            "{name}"
        );
        assert_eq!(restore_state(&fixture), before, "{name}");
    }

    // queued work waits behind the reservation and serves with the closure
    let first = fixture
        .store
        .accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            resource_id,
            MachineId::new(),
            spec(),
        )
        .unwrap();
    let saved_attestation =
        attestation_for(&fixture.store, authority, resource_id, task_id, binding);
    let receipt = attest_on(&mut fixture.store, saved_attestation.clone())
        .unwrap()
        .receipt;
    let AttestedTrainerLaunch::DirectSegmentReturn {
        action_id: bound_action,
        ..
    } = receipt.evidence.trainer_launch
    else {
        panic!("the evidence must name the return decision that bound the task");
    };
    assert_eq!(bound_action, action_id);
    assert_eq!(
        receipt.evidence.trainer_association,
        AttestedTrainerAssociation::Missing
    );
    let OperatorGpuFreeOutcome::RestoreClosedServing {
        closed,
        loan: serving,
        request,
    } = &receipt.outcome
    else {
        panic!("the closure must serve the next queued request");
    };
    assert_eq!(closed.id, loan.id);
    assert!(matches!(
        &closed.state,
        LoanState::Closed {
            result: LoanClosure::OperatorAttestedRestoreEnded { task_id: ended, operation_id, .. },
        } if *ended == task_id && *operation_id == saved_attestation.operation_id
    ));
    assert_eq!(request.request_id, first.request_id);
    assert!(matches!(
        &serving.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                release_provenance: ServingReleaseProvenance::IdleBoundary {
                    proof: IdleBoundaryProof::OperatorAttestedGpuFree { task_id: ended, .. },
                },
                ..
            },
        } if *ended == task_id
    ));
    let (resource, current, count) = restore_state(&fixture);
    assert_eq!(resource.registered_background_task, None);
    assert_eq!(resource.state_revision, receipt.state_revision);
    assert_eq!(current.as_ref(), Some(serving));
    assert_eq!(count, 1);

    fixture.store = Store::open(&fixture.database).unwrap();
    let replay = attest_on(&mut fixture.store, saved_attestation.clone()).unwrap();
    assert!(replay.replayed);
    assert_eq!(
        serde_json::to_value(&replay.receipt).unwrap(),
        serde_json::to_value(&receipt).unwrap()
    );
    let mut changed = saved_attestation.clone();
    changed.state_binding = OperatorStateBinding::NoLoan;
    assert_eq!(
        refusal(attest_on(&mut fixture.store, changed)),
        OperatorGpuFreeRefusal::ConflictingRetry {
            operation_id: saved_attestation.operation_id
        }
    );
}

#[test]
fn restore_closure_is_an_idle_boundary_only_while_its_receipt_is_saved() {
    let (mut fixture, loan, action_id, task_id) = restore_ended_before_confirmed_start();
    let (authority, resource_id) = (fixture.authority, fixture.resource.id);
    let saved_attestation = attestation_for(
        &fixture.store,
        authority,
        resource_id,
        task_id,
        OperatorStateBinding::RestoringReturn {
            loan_id: loan.id,
            action_id,
        },
    );
    let receipt = fixture
        .store
        .attest_trainer_gpu_free_for_authority(authority, saved_attestation.clone())
        .unwrap()
        .receipt;
    assert!(matches!(
        &receipt.outcome,
        OperatorGpuFreeOutcome::RestoreClosedIdleBoundary { closed } if closed.id == loan.id
    ));
    let (resource, current, _) = restore_state(&fixture);
    assert_eq!(resource.registered_background_task, None);
    assert_eq!(current, None);

    // a restarted authority reads the closure and its receipt as the boundary
    fixture.store = Store::open(&fixture.database).unwrap();
    let saved = fixture
        .store
        .resource_snapshots_for_authority(authority)
        .unwrap()[0]
        .resource
        .clone();
    assert!(matches!(
        crate::store::idle_boundary_decision_on(&fixture.store.conn, &saved).unwrap(),
        IdleBoundaryDecision::Proven(IdleBoundaryProof::OperatorAttestedGpuFree {
            operation_id,
            task_id: ended,
        }) if operation_id == saved_attestation.operation_id && ended == task_id
    ));

    // the closure without its receipt is not evidence
    fixture
        .store
        .conn
        .execute(
            "DELETE FROM resource_operator_attestations WHERE operation_id = ?1",
            [saved_attestation.operation_id.as_uuid().to_string()],
        )
        .unwrap();
    assert_eq!(
        crate::store::idle_boundary_decision_on(&fixture.store.conn, &saved).unwrap(),
        IdleBoundaryDecision::Unproven(IdleProofGap::InconsistentHistory)
    );
}

/// Bind a native foreground return task and return it still queued
fn queued_foreground_return() -> (ServingFixture, Loan, ActionId, TaskId) {
    let (mut fixture, authority) = awaiting_return_fixture();
    let launch = evaluation(&fixture, &["/bin/echo", "evaluate"]);
    let task_id = launch.task_id;
    let ReturnTaskAcceptance::Inserted { loan, .. } = fixture
        .store
        .accept_return_task_for_authority(launch_input(authority, launch))
        .unwrap()
    else {
        panic!("the first exact return launch must insert its task");
    };
    (fixture, *loan, authority.action_id, task_id)
}

fn foreground_state(fixture: &ServingFixture) -> (Resource, Option<Loan>, i64) {
    let snapshot = fixture
        .store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()
        .remove(0);
    (
        snapshot.resource,
        snapshot.loan,
        attestation_count(&fixture.store),
    )
}

fn foreground_attestation(
    fixture: &ServingFixture,
    task_id: TaskId,
    state_binding: OperatorStateBinding,
) -> OperatorGpuFreeAttestation {
    attestation_for(
        &fixture.store,
        fixture.authority,
        fixture.resource.id,
        task_id,
        state_binding,
    )
}

fn attest_foreground(
    fixture: &mut ServingFixture,
    attestation: OperatorGpuFreeAttestation,
) -> Result<OperatorGpuFreeResolution, OperatorGpuFreeError> {
    fixture
        .store
        .attest_trainer_gpu_free_for_authority(fixture.authority, attestation)
}

fn restore_closure_count(store: &Store) -> i64 {
    store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_restore_closures",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn foreground_return_attestation_refuses_live_tasks_and_every_mismatch() {
    let (mut fixture, loan, action_id, task_id) = queued_foreground_return();
    let binding = OperatorStateBinding::RestoringForegroundReturn {
        loan_id: loan.id,
        action_id,
    };

    // a queued or running foreground task still owns the GPU
    let queued = foreground_attestation(&fixture, task_id, binding);
    assert_eq!(
        refusal(attest_foreground(&mut fixture, queued)),
        OperatorGpuFreeRefusal::TaskNotEnded {
            task_id,
            state: ProcessStatus::Queued,
        }
    );
    start_task(&fixture.store, task_id);
    let running = foreground_attestation(&fixture, task_id, binding);
    assert_eq!(
        refusal(attest_foreground(&mut fixture, running)),
        OperatorGpuFreeRefusal::TaskNotEnded {
            task_id,
            state: ProcessStatus::Running,
        }
    );
    fixture
        .store
        .cas_status(task_id, ProcessStatus::Running, ProcessStatus::Lost)
        .unwrap()
        .unwrap();

    let before = foreground_state(&fixture);
    let other_task = TaskId::new();
    let cases = [
        (
            "the direct-segment binding names another execution mode",
            OperatorStateBinding::RestoringReturn {
                loan_id: loan.id,
                action_id,
            },
            task_id,
            OperatorGpuFreeRefusal::TrainerLaunchUnproven { task_id },
        ),
        (
            "another return action",
            OperatorStateBinding::RestoringForegroundReturn {
                loan_id: loan.id,
                action_id: ActionId::new(),
            },
            task_id,
            OperatorGpuFreeRefusal::LoanStateChanged {
                current_loan: Some(loan.id),
            },
        ),
        (
            "another loan",
            OperatorStateBinding::RestoringForegroundReturn {
                loan_id: LoanId::new(),
                action_id,
            },
            task_id,
            OperatorGpuFreeRefusal::LoanStateChanged {
                current_loan: Some(loan.id),
            },
        ),
        (
            "another task",
            binding,
            other_task,
            OperatorGpuFreeRefusal::NotBoundTask {
                task_id: other_task,
                bound: task_id,
            },
        ),
        (
            "a registered-trainer binding",
            OperatorStateBinding::NoLoan,
            task_id,
            OperatorGpuFreeRefusal::NotRegisteredTrainer {
                task_id,
                registered: before.0.registered_background_task,
            },
        ),
    ];
    for (name, state_binding, task, expected) in cases {
        let changed = foreground_attestation(&fixture, task, state_binding);
        assert_eq!(
            refusal(attest_foreground(&mut fixture, changed)),
            expected,
            "{name}"
        );
        assert_eq!(foreground_state(&fixture), before, "{name}");
    }

    let current = before.0.state_revision;
    let stale = OperatorGpuFreeAttestation {
        expected_state_revision: ResourceRevision::new(current.get() + 1),
        ..foreground_attestation(&fixture, task_id, binding)
    };
    assert_eq!(
        refusal(attest_foreground(&mut fixture, stale.clone())),
        OperatorGpuFreeRefusal::StaleRevision {
            expected: stale.expected_state_revision,
            actual: current,
        }
    );
    let other_authority = MachineId::new();
    let named_elsewhere = OperatorGpuFreeAttestation {
        authority_machine: other_authority,
        ..foreground_attestation(&fixture, task_id, binding)
    };
    assert_eq!(
        refusal(attest_foreground(&mut fixture, named_elsewhere)),
        OperatorGpuFreeRefusal::WrongAuthority {
            expected: fixture.authority,
            found: other_authority,
        }
    );
    let exact = foreground_attestation(&fixture, task_id, binding);
    assert_eq!(
        refusal(
            fixture
                .store
                .attest_trainer_gpu_free_for_authority(other_authority, exact)
        ),
        OperatorGpuFreeRefusal::WrongAuthority {
            expected: other_authority,
            found: fixture.authority,
        }
    );

    // a registration that names the foreground task is not its Restoring reservation
    fixture
        .store
        .conn
        .execute(
            "UPDATE resources SET registered_background_task = ?1 WHERE id = ?2",
            params![
                task_id.to_string(),
                fixture.resource.id.as_uuid().to_string()
            ],
        )
        .unwrap();
    let registered = foreground_attestation(&fixture, task_id, binding);
    assert_eq!(
        refusal(attest_foreground(&mut fixture, registered)),
        OperatorGpuFreeRefusal::InconsistentHistory { task_id }
    );
    assert_eq!(attestation_count(&fixture.store), 0);
    assert_eq!(foreground_state(&fixture).1, before.1);
}

#[test]
fn lost_foreground_return_closes_through_its_exact_attestation_and_serves_queued_work() {
    let (mut fixture, loan, action_id, task_id) = queued_foreground_return();
    start_task(&fixture.store, task_id);
    fixture
        .store
        .cas_status(task_id, ProcessStatus::Running, ProcessStatus::Lost)
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .reconcile_restoring_loan_for_authority(fixture.authority, fixture.resource.id)
            .unwrap(),
        RestoreReconcileOutcome::Attention {
            reason: RestoreAttentionReason::Lost,
            ..
        }
    ));
    let (authority, resource_id) = (fixture.authority, fixture.resource.id);
    let mut queue = Vec::new();
    for _ in 0..2 {
        queue.push(
            fixture
                .store
                .accept_resource_request(
                    authority,
                    RequestId::new(),
                    TaskId::new(),
                    resource_id,
                    MachineId::new(),
                    spec(),
                )
                .unwrap(),
        );
    }
    // queued work does not release the reservation of a lost foreground task
    assert_eq!(foreground_state(&fixture).1, Some(loan.clone()));

    let saved_attestation = foreground_attestation(
        &fixture,
        task_id,
        OperatorStateBinding::RestoringForegroundReturn {
            loan_id: loan.id,
            action_id,
        },
    );
    let resolution = attest_foreground(&mut fixture, saved_attestation.clone()).unwrap();
    assert!(!resolution.replayed);
    let receipt = resolution.receipt;
    assert_eq!(receipt.evidence.trainer_end, AttestedTrainerEnd::Lost);
    assert!(matches!(
        receipt.evidence.trainer_launch,
        AttestedTrainerLaunch::NativeForegroundReturn { action_id: bound, .. } if bound == action_id
    ));
    let OperatorGpuFreeOutcome::RestoreClosedServing {
        closed,
        loan: serving,
        request,
    } = &receipt.outcome
    else {
        panic!("the closure must serve the next queued request");
    };
    assert!(matches!(
        &closed.state,
        LoanState::Closed {
            result: LoanClosure::OperatorAttestedRestoreEnded { task_id: ended, operation_id, .. },
        } if *ended == task_id && *operation_id == saved_attestation.operation_id
    ));
    assert_eq!(request.request_id, queue[0].request_id);
    assert!(matches!(
        &serving.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                release_provenance: ServingReleaseProvenance::IdleBoundary {
                    proof: IdleBoundaryProof::OperatorAttestedGpuFree { task_id: ended, .. },
                },
                ..
            },
        } if *ended == task_id
    ));
    let (resource, current, count) = foreground_state(&fixture);
    assert_eq!(resource.registered_background_task, None);
    assert_eq!(resource.state_revision, receipt.state_revision);
    assert_eq!(current.as_ref(), Some(serving));
    assert_eq!(count, 1);
    assert_eq!(
        fixture
            .store
            .resource_requests(authority, resource_id)
            .unwrap()
            .into_iter()
            .find(|saved| saved.request_id == queue[1].request_id)
            .unwrap()
            .state,
        ResourceRequestState::Queued
    );
    // the human attestation is not saved as task or restore-closure exit evidence
    assert_eq!(
        fixture.store.get_task(task_id).unwrap().unwrap().status(),
        ProcessStatus::Lost
    );
    assert_eq!(restore_closure_count(&fixture.store), 0);

    // an exact retry replays after restart, although the loan has since closed
    fixture.store = Store::open(&fixture.directory.path().join("db")).unwrap();
    let replay = attest_foreground(&mut fixture, saved_attestation.clone()).unwrap();
    assert!(replay.replayed);
    assert_eq!(
        serde_json::to_value(&replay.receipt).unwrap(),
        serde_json::to_value(&receipt).unwrap()
    );
    let changed = OperatorGpuFreeAttestation {
        observation: OperatorObservation::try_from("a different inspection".to_owned()).unwrap(),
        ..saved_attestation.clone()
    };
    assert_eq!(
        refusal(attest_foreground(&mut fixture, changed)),
        OperatorGpuFreeRefusal::ConflictingRetry {
            operation_id: saved_attestation.operation_id
        }
    );
    assert_eq!(attestation_count(&fixture.store), 1);
}

#[test]
fn unconfirmed_foreground_exit_closes_into_the_saved_idle_boundary() {
    let (mut fixture, loan, action_id, task_id) = queued_foreground_return();
    start_task(&fixture.store, task_id);
    fixture
        .store
        .cas_exit_with_evidence(
            task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::Unconfirmed,
        )
        .unwrap()
        .unwrap();
    // neither the owner nor the supervisor treats an unconfirmed exit as release
    assert!(matches!(
        fixture
            .store
            .reconcile_restoring_loan_for_authority(fixture.authority, fixture.resource.id)
            .unwrap(),
        RestoreReconcileOutcome::Attention {
            reason: RestoreAttentionReason::ForegroundExitUnconfirmed { .. },
            ..
        }
    ));
    let revision = foreground_state(&fixture).0.state_revision;
    assert!(matches!(
        fixture
            .store
            .resolve_ended_restore_for_authority(EndedRestoreResolution {
                authority: SupervisorActionAuthority {
                    authority_machine: fixture.authority,
                    resource_id: fixture.resource.id,
                    loan_id: loan.id,
                    action_id,
                    expected_state_revision: revision,
                    supervisor: fixture.resource.supervisor,
                    assignment_revision: fixture.resource.assignment_revision,
                },
                task_id,
                reason: "exit not confirmed".into(),
            }),
        Err(ReturnDecisionError::RestoreReleaseUnproven { .. })
    ));

    let saved_attestation = foreground_attestation(
        &fixture,
        task_id,
        OperatorStateBinding::RestoringForegroundReturn {
            loan_id: loan.id,
            action_id,
        },
    );
    let receipt = attest_foreground(&mut fixture, saved_attestation.clone())
        .unwrap()
        .receipt;
    assert_eq!(
        receipt.evidence.trainer_end,
        AttestedTrainerEnd::Finished {
            outcome: ExitReason::Exit { code: 0 },
            process_group_exit: ProcessGroupExitEvidence::Unconfirmed,
            container_exit: None,
        }
    );
    assert!(matches!(
        &receipt.outcome,
        OperatorGpuFreeOutcome::RestoreClosedIdleBoundary { closed } if closed.id == loan.id
    ));
    assert_eq!(
        fixture.store.process_group_exit_evidence(task_id).unwrap(),
        Some(ProcessGroupExitEvidence::Unconfirmed)
    );
    assert_eq!(restore_closure_count(&fixture.store), 0);

    // a restarted authority reads the closure and its receipt as the idle boundary
    fixture.store = Store::open(&fixture.directory.path().join("db")).unwrap();
    let (saved, current, _) = foreground_state(&fixture);
    assert_eq!(current, None);
    assert_eq!(saved.registered_background_task, None);
    assert!(matches!(
        crate::store::idle_boundary_decision_on(&fixture.store.conn, &saved).unwrap(),
        IdleBoundaryDecision::Proven(IdleBoundaryProof::OperatorAttestedGpuFree {
            operation_id,
            task_id: ended,
        }) if operation_id == saved_attestation.operation_id && ended == task_id
    ));
    let replay = attest_foreground(&mut fixture, saved_attestation).unwrap();
    assert!(replay.replayed);
}

#[test]
fn saved_restoring_receipts_decode_unchanged_and_bindings_refuse_unknown_fields() {
    // a direct-segment receipt as saved before the foreground binding existed
    let (operation, task, loan, action, request) = (
        Uuid::now_v7(),
        TaskId::new(),
        LoanId::new(),
        ActionId::new(),
        RequestId::new(),
    );
    let closed = json!({
        "id": loan,
        "resource_id": Uuid::now_v7(),
        "state": {
            "type": "closed",
            "result": {
                "type": "operator_attested_restore_ended",
                "return_context": { "type": "idle" },
                "task_id": task,
                "operation_id": operation,
            },
        },
    });
    let legacy = json!({
        "attestation": {
            "operation_id": operation,
            "resource_id": closed["resource_id"],
            "authority_machine": Uuid::now_v7(),
            "task_id": task,
            "expected_state_revision": 9,
            "state_binding": { "type": "restoring_return", "loan_id": loan, "action_id": action },
            "observation": OBSERVATION,
            "confirmation": "operator_confirmed_gpu_free",
        },
        "evidence": {
            "trainer_end": {
                "type": "finished",
                "outcome": { "kind": "signal", "signal": 9 },
                "process_group_exit": "unconfirmed",
            },
            "trainer_launch": {
                "type": "direct_segment_return",
                "action_id": action,
                "request_id": request,
            },
            "normalized_spec_sha256": "42".repeat(32),
            "trainer_association": { "type": "missing" },
        },
        "state_revision": 10,
        "outcome": { "type": "restore_closed_idle_boundary", "closed": closed },
    });
    let saved: crate::resource::operator_release::OperatorGpuFreeReceipt =
        serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(
        saved.attestation.state_binding,
        OperatorStateBinding::RestoringReturn {
            loan_id: loan,
            action_id: action,
        }
    );
    assert_eq!(serde_json::to_value(&saved).unwrap(), legacy);

    // the tag-only no_loan binding keeps its shape and refuses extra fields
    let no_loan: OperatorStateBinding =
        serde_json::from_value(json!({ "type": "no_loan" })).unwrap();
    assert_eq!(no_loan, OperatorStateBinding::NoLoan);
    assert_eq!(
        serde_json::to_value(no_loan).unwrap(),
        json!({ "type": "no_loan" })
    );
    for binding in [
        json!({ "type": "no_loan", "loan_id": loan }),
        json!({
            "type": "restoring_foreground_return",
            "loan_id": loan,
            "action_id": action,
            "task_id": task,
        }),
    ] {
        assert!(serde_json::from_value::<OperatorStateBinding>(binding).is_err());
    }
}

#[test]
fn schema_27_receipts_survive_the_restore_outcome_migration() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    let authority = MachineId::new();
    let resource = resource(authority);
    let (operation, task, request) = (Uuid::now_v7(), TaskId::new(), RequestId::new());
    // a receipt exactly as schema 27 saved it for a registered trainer
    let legacy = json!({
        "attestation": {
            "operation_id": operation,
            "resource_id": resource.id,
            "authority_machine": authority,
            "task_id": task,
            "expected_state_revision": 4,
            "state_binding": { "type": "no_loan" },
            "observation": OBSERVATION,
            "confirmation": "operator_confirmed_gpu_free",
        },
        "evidence": {
            "trainer_end": { "type": "lost" },
            "trainer_launch": { "type": "first_background_launch", "request_id": request },
            "normalized_spec_sha256": "42".repeat(32),
            "trainer_association": { "type": "missing" },
        },
        "state_revision": 5,
        "outcome": { "type": "idle_boundary" },
    });
    {
        let mut store = Store::open(&path).unwrap();
        store.register_resource(authority, &resource).unwrap();
        store
            .conn
            .execute_batch(&format!(
                "DROP TABLE resource_operator_attestations;
                 CREATE TABLE resource_operator_attestations (
                     operation_id TEXT PRIMARY KEY NOT NULL,
                     resource_id TEXT NOT NULL REFERENCES resources(id),
                     task_id TEXT NOT NULL UNIQUE,
                     preceding_loan TEXT,
                     preceding_launch TEXT,
                     receipt_json TEXT NOT NULL CHECK (
                         json_extract(receipt_json, '$.outcome.type') IN (
                             'release_resolved_serving', 'release_resolved_return_required',
                             'idle_serving', 'idle_boundary'
                         )
                     )
                 );
                 INSERT INTO resource_operator_attestations
                     (operation_id, resource_id, task_id, preceding_launch, receipt_json)
                 VALUES ('{operation}', '{}', '{task}', '{}', '{legacy}');
                 ALTER TABLE tasks DROP COLUMN container_exit_evidence;
                 DROP TABLE task_containers;
                 PRAGMA user_version = 27;",
                resource.id.as_uuid(),
                request.0
            ))
            .unwrap();
    }

    let store = Store::open(&path).unwrap();
    let version: i64 = store
        .conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, crate::domain::SCHEMA_VERSION);
    let saved = store
        .operator_attestation_receipt_for_authority(
            authority,
            OperatorAttestationId::from_uuid(operation).unwrap(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        saved.attestation.state_binding,
        OperatorStateBinding::NoLoan
    );
    assert!(matches!(
        saved.outcome,
        OperatorGpuFreeOutcome::IdleBoundary
    ));
    assert_eq!(serde_json::to_value(&saved).unwrap(), legacy);

    // the rebuilt table accepts the new outcomes and keeps its content checks
    let insert = |outcome: &str, confirmation: &str| {
        let (operation, task) = (Uuid::now_v7(), TaskId::new());
        let receipt = json!({
            "attestation": {
                "operation_id": operation,
                "resource_id": resource.id,
                "task_id": task,
                "observation": OBSERVATION,
                "confirmation": confirmation,
            },
            "evidence": {},
            "outcome": { "type": outcome },
        });
        store.conn.execute(
            "INSERT INTO resource_operator_attestations
                (operation_id, resource_id, task_id, receipt_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                operation.to_string(),
                resource.id.as_uuid().to_string(),
                task.to_string(),
                receipt.to_string()
            ],
        )
    };
    assert!(
        insert(
            "restore_closed_idle_boundary",
            "operator_confirmed_gpu_free"
        )
        .is_ok()
    );
    assert!(insert("restore_closed_serving", "operator_confirmed_gpu_free").is_ok());
    assert!(insert("unknown", "operator_confirmed_gpu_free").is_err());
    assert!(insert("idle_boundary", "confirmed").is_err());
}
