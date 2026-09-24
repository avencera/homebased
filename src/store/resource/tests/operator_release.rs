//! Operator attestation for a registered trainer that ended with no release proof
//!
//! The automatic proof stays fail-closed for an unbound trainer. Only one exact,
//! explicit attestation releases it, and the attestation commits its receipt and
//! the queue or loan transition together. It never claims a confirmed exit, a
//! released trainer lock, a completed result, or a resumable stop

use super::background::{LaunchFixture, registered_launch, saved_loan_for};
use super::restore::{resume_input, start_task, stopped_return_fixture};
use super::*;
use crate::resource::IdleBoundaryProof;
use crate::resource::operator_release::{
    AttestedTrainerAssociation, AttestedTrainerEnd, AttestedTrainerLaunch, OperatorAttestationId,
    OperatorGpuFreeAttestation, OperatorGpuFreeConfirmation, OperatorGpuFreeEvidence,
    OperatorGpuFreeOutcome, OperatorGpuFreeRefusal, OperatorGpuFreeResolution, OperatorObservation,
    OperatorStateBinding,
};
use crate::resource::store::ResourceTaskAcceptanceInput;
use crate::resource::{LoanClosure, SupervisorActionAuthority, SupervisorNoticePayload};
use crate::store::{
    BackgroundLaunchAcceptance, BackgroundLaunchError, OperatorGpuFreeError,
    RestoreReconcileOutcome, ReturnTaskAcceptance,
};

const OBSERVATION: &str = "nvidia-smi on the authority lists no trainer process";

fn attestation_for(
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
            },
            trainer_launch: AttestedTrainerLaunch::FirstBackgroundLaunch {
                request_id: launch_request,
            },
            normalized_spec_sha256: normalized_spec_sha256(&fixture.trainer_spec()).unwrap(),
            trainer_association: AttestedTrainerAssociation::Missing,
        }
    );

    // the registration clears and the oldest request serves in the same transaction
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
    let cases: [(&str, Change, MachineId, OperatorGpuFreeRefusal); 7] = [
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
        (
            "nil operation identity",
            |attestation, _, _| {
                attestation.operation_id = OperatorAttestationId::from_uuid(Uuid::nil());
            },
            fixture.authority,
            OperatorGpuFreeRefusal::InvalidIdentity,
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
fn failed_trainer_release_serves_fifo_and_replays_after_restart() {
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
        panic!("the oldest queued request must serve after the attestation");
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
fn schema_26_database_gains_the_attestation_table_and_its_content_checks() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    {
        let store = Store::open(&path).unwrap();
        store
            .conn
            .execute_batch(
                "DROP TABLE resource_operator_attestations;
                 PRAGMA user_version = 26;",
            )
            .unwrap();
    }

    let store = Store::open(&path).unwrap();
    let version: i64 = store
        .conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, crate::domain::SCHEMA_VERSION);
    let authority = MachineId::new();
    let resource = resource(authority);
    let mut store = store;
    store.register_resource(authority, &resource).unwrap();

    // a receipt without the explicit confirmation cannot be stored
    let (operation, task) = (Uuid::now_v7(), TaskId::new());
    let insert = |confirmation: serde_json::Value| {
        let receipt = json!({
            "attestation": {
                "operation_id": operation,
                "resource_id": resource.id,
                "task_id": task,
                "observation": OBSERVATION,
                "confirmation": confirmation,
            },
            "evidence": {},
            "outcome": { "type": "idle_boundary" },
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
    assert!(insert(json!(null)).is_err());
    assert!(insert(json!("operator_confirmed_gpu_free")).is_ok());
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
    let saved = call(&store, |reply| {
        StoreMsg::OperatorAttestationReceiptForAuthority {
            authority_machine: authority,
            operation_id: saved_attestation.operation_id,
            reply,
        }
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(saved.attestation, saved_attestation);
    store.stop(None);
    let _ = handle.await;
}
