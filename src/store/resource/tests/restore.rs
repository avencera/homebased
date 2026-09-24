//! Supervisor return decisions, fixed-identity restore binding, and restore closure

use super::fixtures::{
    ServingFixture, TrainerAssociationFixture, accept_and_finish_resource_task,
    commit_stopped_release_decision, completion, machine_other_than, release_completion_fixture,
    resource, serving_fixture,
};
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId, ThreadId, Workload,
};
use crate::machine::MachineId;
use crate::resource::ownership_lock::test_support::start_fake_lock_process;
use crate::resource::ownership_lock::{
    OwnershipLockIdentity, OwnershipLockIdentityMismatchReason, OwnershipLockProbe,
    OwnershipLockProbeError, probe_segment_ownership_lock,
};
use crate::resource::store::{
    ReleaseCompletionResult, ResourceStoreError, ResourceTaskCompletionResult,
};
use crate::resource::{
    ActionId, AssignmentRevision, CommandSpec, IdleBoundaryProof, Loan, LoanClosure, LoanId,
    LoanPhase, LoanState, ReleaseCheckpointStopDecision, Resource, ResourceQueueReconcileOutcome,
    ResourceRequest, ResourceRequestState, ResourceRevision, ResourceTaskOwnershipRisk,
    RestoreAttentionReason, ReturnContext, ReturnDecisionRejection, ReturnExecutionMode,
    ReturnLaunch, ReturnWork, SameRunResumeGap, SupervisorActionAuthority,
};
use crate::spec::NormalizedWorkload;
use crate::store::resource::trainer_lock::TrainerLockReleaseGap;
use crate::store::{
    EndedRestoreResolution, ExecutorIdentity, RestoreReconcileOutcome, ReturnClosure,
    ReturnDecisionError, ReturnTaskAcceptance, ReturnTaskAcceptanceInput, ReturnTaskOrigin, Store,
};
use crate::submission::{
    CallbackExecutable, NormalizedSpecSha256, RequestId, SubmissionState, normalized_spec_sha256,
};
use rusqlite::{OptionalExtension, params};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use tempfile::tempdir;
use uuid::Uuid;

/// Drain the serving fixture's only request so the loan awaits its return decision
pub(super) fn awaiting_return_fixture() -> (ServingFixture, SupervisorActionAuthority) {
    let mut fixture = serving_fixture(true, true);
    let input = accept_and_finish_resource_task(
        &mut fixture,
        ExitReason::Exit { code: 0 },
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let result = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(input)
        .unwrap();
    let Ok(ResourceTaskCompletionResult::ReturnRequired { loan, notice, .. }) = completion(result)
    else {
        panic!("the drained queue must reserve the return");
    };
    let authority = SupervisorActionAuthority {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        loan_id: loan.id,
        action_id: notice.action_id,
        expected_state_revision: notice.state_revision,
        supervisor: fixture.resource.supervisor,
        assignment_revision: fixture.resource.assignment_revision,
    };
    fixture.loan = loan;
    fixture.state_revision = notice.state_revision;
    (fixture, authority)
}

/// Invalid authority, its no-resume reason, and the exact expected error
type Rejection = (
    SupervisorActionAuthority,
    &'static str,
    fn(&ReturnDecisionError) -> bool,
);

fn completed_task(fixture: &ServingFixture) -> TaskId {
    fixture.resource.registered_background_task.unwrap()
}

fn command(fixture: &ServingFixture, argv: &[&str]) -> CommandSpec {
    let mut spec = fixture.spec.clone();
    spec.workload = NormalizedWorkload::Task(crate::spec::NormalizedTaskWorkload {
        command: crate::invocation::CommandLine::try_from_argv(
            argv.iter().map(|argument| (*argument).to_owned()).collect(),
        )
        .unwrap(),
    });
    CommandSpec::try_from(spec).unwrap()
}

pub(super) fn evaluation(fixture: &ServingFixture, argv: &[&str]) -> ReturnLaunch {
    ReturnLaunch {
        request_id: RequestId::new(),
        task_id: TaskId::new(),
        work: ReturnWork::EvaluationOrNextEpoch {
            completed_task: completed_task(fixture),
            spec: command(fixture, argv),
        },
    }
}

pub(super) fn launch_input(
    authority: SupervisorActionAuthority,
    launch: ReturnLaunch,
) -> ReturnTaskAcceptanceInput {
    ReturnTaskAcceptanceInput {
        authority,
        launch,
        executor_env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
        origin: ReturnTaskOrigin::Local {
            callback_codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
        },
    }
}

fn saved_loan(store: &Store, authority: MachineId) -> Option<Loan> {
    store.resource_snapshots_for_authority(authority).unwrap()[0]
        .loan
        .clone()
}

fn saved_resource(store: &Store, authority: MachineId) -> Resource {
    store.resource_snapshots_for_authority(authority).unwrap()[0]
        .resource
        .clone()
}

fn assert_still_awaiting_return(fixture: &ServingFixture, authority: &SupervisorActionAuthority) {
    assert!(matches!(
        saved_loan(&fixture.store, fixture.authority).map(|loan| loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::AwaitingReturn { action_id, .. }
        }) if action_id == authority.action_id
    ));
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).state_revision,
        authority.expected_state_revision
    );
}

fn accept_post_return_request(fixture: &mut ServingFixture) -> ResourceRequest {
    fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            fixture.spec.clone(),
        )
        .unwrap()
}

#[test]
fn no_resume_closes_only_the_exact_current_return_action_and_replays_its_receipt() {
    let (mut fixture, authority) = awaiting_return_fixture();
    let completed = completed_task(&fixture);
    // a request accepted after the reservation waits and cannot supersede the action
    let later = accept_post_return_request(&mut fixture);
    assert!(matches!(
        fixture
            .store
            .reconcile_resource_queue_for_authority(fixture.authority, fixture.resource.id)
            .unwrap(),
        ResourceQueueReconcileOutcome::LoanAlreadyActive { .. }
    ));

    let mut other_thread = authority;
    other_thread.supervisor.thread = ThreadId(Uuid::now_v7());
    let mut stale_assignment = authority;
    stale_assignment.assignment_revision = AssignmentRevision::new(7);
    let mut stale_revision = authority;
    stale_revision.expected_state_revision = ResourceRevision::new(1);
    let mut other_action = authority;
    other_action.action_id = ActionId::new();
    let mut other_authority = authority;
    other_authority.authority_machine = machine_other_than(fixture.authority);
    let rejections: [Rejection; 6] = [
        (other_thread, "done", |error| {
            matches!(error, ReturnDecisionError::NotCurrentSupervisor)
        }),
        (stale_assignment, "done", |error| {
            matches!(error, ReturnDecisionError::NotCurrentSupervisor)
        }),
        (stale_revision, "done", |error| {
            matches!(error, ReturnDecisionError::StaleRevision { .. })
        }),
        (other_action, "done", |error| {
            matches!(error, ReturnDecisionError::ActionNotPending { .. })
        }),
        (other_authority, "done", |error| {
            matches!(
                error,
                ReturnDecisionError::Resource(ResourceStoreError::WrongAuthority { .. })
            )
        }),
        (authority, "  ", |error| {
            matches!(
                error,
                ReturnDecisionError::Rejected(ReturnDecisionRejection::EmptyReason)
            )
        }),
    ];
    for (candidate, reason, expected) in rejections {
        let error = fixture
            .store
            .record_no_resume_for_authority(candidate, reason.into())
            .unwrap_err();
        assert!(expected(&error), "unexpected rejection: {error:?}");
        assert_still_awaiting_return(&fixture, &authority);
    }

    let closure = fixture
        .store
        .record_no_resume_for_authority(authority, "evaluation is not needed".into())
        .unwrap();
    let next_revision = ResourceRevision::new(authority.expected_state_revision.get() + 1);
    assert_eq!(closure.state_revision, next_revision);
    assert!(matches!(
        &closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::NoResume {
                return_context: ReturnContext::AlreadyCompleted { task_id, .. },
                reason,
            }
        } if *task_id == completed && reason == "evaluation is not needed"
    ));
    let resource = saved_resource(&fixture.store, fixture.authority);
    assert_eq!(resource.state_revision, next_revision);
    assert_eq!(resource.registered_background_task, None);
    assert!(saved_loan(&fixture.store, fixture.authority).is_none());

    let replay = fixture
        .store
        .record_no_resume_for_authority(authority, "evaluation is not needed".into())
        .unwrap();
    assert_eq!(replay, closure);
    assert!(matches!(
        fixture
            .store
            .record_no_resume_for_authority(authority, "another reason".into()),
        Err(ReturnDecisionError::ConflictingRetry { .. })
    ));
    assert!(matches!(
        fixture.store.accept_return_task_for_authority(launch_input(
            authority,
            evaluation(&fixture, &["/bin/echo", "evaluate"])
        )),
        Err(ReturnDecisionError::ConflictingRetry { .. })
    ));
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).state_revision,
        next_revision
    );

    // after closure the later request uses the normal next-loan rules; the
    // explicit no-resume closure is the saved proof that nothing holds the GPU
    assert!(matches!(
        fixture
            .store
            .reconcile_resource_queue_for_authority(fixture.authority, fixture.resource.id)
            .unwrap(),
        ResourceQueueReconcileOutcome::IdleServing {
            loan,
            request,
            proof: IdleBoundaryProof::SupervisorNoResume { loan_id },
        } if request.request_id == later.request_id
            && loan_id == closure.loan.id
            && matches!(
                &loan.state,
                LoanState::Active {
                    phase: LoanPhase::Serving {
                        return_context: ReturnContext::Idle,
                        ..
                    }
                }
            )
    ));
}

#[test]
fn return_launch_binds_one_fixed_task_with_its_callback_route_and_replays_it() {
    let (fixture, authority) = awaiting_return_fixture();
    let mut fixture = fixture;
    let completed = completed_task(&fixture);

    // choices that do not fit the completed context write nothing
    let mut other_thread_spec = fixture.spec.clone();
    other_thread_spec.thread = ThreadId(Uuid::now_v7());
    // a script entry point hides its interpreter and code from the contract
    let script = fixture.directory.path().join("evaluate");
    fs::write(&script, "#!/bin/sh\nnohup trainer &\n").unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    for (launch, expected) in [
        (
            ReturnLaunch {
                request_id: RequestId::new(),
                task_id: TaskId::new(),
                work: ReturnWork::NewBackgroundWork {
                    spec: command(&fixture, &["/bin/echo", "new"]),
                },
            },
            ReturnDecisionRejection::NewWorkRequiresIdleContext,
        ),
        (
            ReturnLaunch {
                request_id: RequestId::new(),
                task_id: TaskId::new(),
                work: ReturnWork::SameRunResume {
                    stopped_task: completed,
                    recovery_ref: "generation".into(),
                },
            },
            ReturnDecisionRejection::ResumeRequiresStoppedContext,
        ),
        (
            ReturnLaunch {
                request_id: RequestId::new(),
                task_id: TaskId::new(),
                work: ReturnWork::EvaluationOrNextEpoch {
                    completed_task: TaskId::new(),
                    spec: command(&fixture, &["/bin/echo", "evaluate"]),
                },
            },
            ReturnDecisionRejection::EvaluationRequiresCompletedContext,
        ),
        (
            ReturnLaunch {
                request_id: RequestId::new(),
                task_id: TaskId::new(),
                work: ReturnWork::EvaluationOrNextEpoch {
                    completed_task: completed,
                    spec: CommandSpec::try_from(other_thread_spec.clone()).unwrap(),
                },
            },
            ReturnDecisionRejection::ThreadMismatch,
        ),
        (
            evaluation(&fixture, &["/bin/sh", "-c", "nohup trainer &"]),
            ReturnDecisionRejection::UnsupportedCommandOwnership {
                risk: ResourceTaskOwnershipRisk::ShellWrapper,
            },
        ),
        (
            evaluation(
                &fixture,
                &["/usr/bin/env", "docker", "run", "-d", "trainer"],
            ),
            ReturnDecisionRejection::UnsupportedCommandOwnership {
                risk: ResourceTaskOwnershipRisk::ProgramLauncher,
            },
        ),
        (
            evaluation(&fixture, &["python3", "-m", "ops.evaluate_checkpoint"]),
            ReturnDecisionRejection::UnsupportedCommandOwnership {
                risk: ResourceTaskOwnershipRisk::Interpreter,
            },
        ),
        (
            evaluation(&fixture, &[script.to_str().unwrap()]),
            ReturnDecisionRejection::UnsupportedCommandOwnership {
                risk: ResourceTaskOwnershipRisk::ScriptEntryPoint,
            },
        ),
    ] {
        let task_id = launch.task_id;
        let error = fixture
            .store
            .accept_return_task_for_authority(launch_input(authority, launch))
            .unwrap_err();
        assert!(
            matches!(&error, ReturnDecisionError::Rejected(rejection) if *rejection == expected),
            "unexpected rejection: {error:?}"
        );
        assert!(fixture.store.get_task(task_id).unwrap().is_none());
        assert_still_awaiting_return(&fixture, &authority);
    }

    let launch = evaluation(&fixture, &["/bin/echo", "evaluate"]);
    let (request_id, task_id) = (launch.request_id, launch.task_id);
    let ReturnTaskAcceptance::Inserted {
        loan,
        task,
        state_revision,
    } = fixture
        .store
        .accept_return_task_for_authority(launch_input(authority, launch.clone()))
        .unwrap()
    else {
        panic!("the first exact launch must insert its task");
    };
    assert_eq!(task, task_id);
    assert_eq!(
        state_revision,
        ResourceRevision::new(authority.expected_state_revision.get() + 1)
    );
    assert!(matches!(
        &loan.state,
        LoanState::Active {
            phase: LoanPhase::Restoring {
                action_id,
                return_context: ReturnContext::AlreadyCompleted { task_id: context_task, .. },
                resume_task_id,
            }
        } if *action_id == authority.action_id
            && *context_task == completed
            && *resume_task_id == task_id
    ));
    assert_eq!(saved_loan(&fixture.store, fixture.authority), Some(*loan));
    // the prior run stays registered until the return task has a confirmed start
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).registered_background_task,
        Some(completed)
    );

    let row = fixture.store.get_task(task_id).unwrap().unwrap();
    assert_eq!(row.status(), ProcessStatus::Queued);
    assert_eq!(row.thread, authority.supervisor.thread);
    let route = crate::store::identity::origin_route_by_request_on(&fixture.store.conn, request_id)
        .unwrap()
        .unwrap();
    assert_eq!(route.task, task_id);
    assert_eq!(route.origin_machine, fixture.authority);
    assert_eq!(route.execution_machine, fixture.authority);
    assert_eq!(route.thread, authority.supervisor.thread);
    assert!(matches!(route.submission, SubmissionState::Accepted));
    assert!(matches!(
        crate::store::identity::executor_identity_on(&fixture.store.conn, task_id).unwrap(),
        Some(ExecutorIdentity::Accepted(record))
            if record.origin_machine == fixture.authority
                && record.execution_machine == fixture.authority
                && record.state == ProcessStatus::Queued
    ));
    assert!(
        crate::store::initial_queued_event_matches_on(
            &fixture.store.conn,
            task_id,
            fixture.authority,
            fixture.authority,
        )
        .unwrap()
    );

    // an exact retry observes the saved binding without new records
    assert_eq!(
        fixture
            .store
            .accept_return_task_for_authority(launch_input(authority, launch.clone()))
            .unwrap(),
        ReturnTaskAcceptance::Existing {
            task: task_id,
            state: ProcessStatus::Queued,
        }
    );
    let mut changed_command = launch.clone();
    changed_command.work = ReturnWork::EvaluationOrNextEpoch {
        completed_task: completed,
        spec: command(&fixture, &["/bin/echo", "other"]),
    };
    let mut changed_task = launch;
    changed_task.task_id = TaskId::new();
    for changed in [changed_command, changed_task] {
        assert!(matches!(
            fixture
                .store
                .accept_return_task_for_authority(launch_input(authority, changed)),
            Err(ReturnDecisionError::ConflictingRetry { .. })
        ));
    }
    assert!(matches!(
        fixture
            .store
            .record_no_resume_for_authority(authority, "changed mind".into()),
        Err(ReturnDecisionError::ConflictingRetry { .. })
    ));
    let task_rows: i64 = fixture
        .store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM tasks WHERE thread_id = ?1",
            [authority.supervisor.thread.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    // the completed background task, the drained request, and one return task
    assert_eq!(task_rows, 3);
}

#[test]
fn remote_supervisor_return_launch_is_unsupported_before_any_task_record() {
    let (mut fixture, mut authority) = awaiting_return_fixture();
    let remote = machine_other_than(fixture.authority);
    fixture
        .store
        .conn
        .execute(
            "UPDATE resources SET supervisor_machine = ?1 WHERE id = ?2",
            params![
                remote.to_string(),
                fixture.resource.id.as_uuid().to_string()
            ],
        )
        .unwrap();
    authority.supervisor.machine = remote;

    let launch = evaluation(&fixture, &["/bin/echo", "evaluate"]);
    let task_id = launch.task_id;
    assert_eq!(
        fixture
            .store
            .accept_return_task_for_authority(launch_input(authority, launch))
            .unwrap(),
        ReturnTaskAcceptance::UnsupportedRemoteSupervisor {
            authority_machine: fixture.authority,
            supervisor: authority.supervisor,
        }
    );
    assert!(fixture.store.get_task(task_id).unwrap().is_none());
    assert_still_awaiting_return(&fixture, &authority);
    let receipts: i64 = fixture
        .store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_return_decisions",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipts, 0);
}

fn reconcile_restore(fixture: &mut ServingFixture) -> RestoreReconcileOutcome {
    fixture
        .store
        .reconcile_restoring_loan_for_authority(fixture.authority, fixture.resource.id)
        .unwrap()
}

pub(super) fn start_task(store: &Store, task_id: TaskId) {
    store
        .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
}

fn finish_running_task(
    store: &Store,
    task_id: TaskId,
    reason: ExitReason,
    evidence: ProcessGroupExitEvidence,
) {
    store
        .cas_exit_with_evidence(task_id, ProcessStatus::Running, &reason, evidence)
        .unwrap()
        .unwrap();
}

fn restore_closure_basis(store: &Store, action_id: ActionId) -> Option<String> {
    store
        .conn
        .query_row(
            "SELECT json_extract(receipt_json, '$.basis.type')
             FROM resource_restore_closures WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
}

fn request_state(fixture: &ServingFixture, request_id: RequestId) -> Option<ResourceRequestState> {
    fixture
        .store
        .resource_requests(fixture.authority, fixture.resource.id)
        .unwrap()
        .into_iter()
        .find(|request| request.request_id == request_id)
        .map(|request| request.state)
}

/// Accept one native foreground evaluation and a later queued request
fn native_restore_fixture() -> (
    ServingFixture,
    SupervisorActionAuthority,
    ReturnLaunch,
    ResourceRequest,
) {
    let (mut fixture, authority) = awaiting_return_fixture();
    let later = accept_post_return_request(&mut fixture);
    let launch = evaluation(&fixture, &["/bin/echo", "evaluate"]);
    let ReturnTaskAcceptance::Inserted { state_revision, .. } = fixture
        .store
        .accept_return_task_for_authority(launch_input(authority, launch.clone()))
        .unwrap()
    else {
        panic!("the first exact launch must insert its task");
    };
    let mut current = authority;
    current.expected_state_revision = state_revision;
    (fixture, current, launch, later)
}

fn assert_native_restore_reserved(fixture: &mut ServingFixture, task_id: TaskId, later: RequestId) {
    assert!(matches!(
        saved_loan(&fixture.store, fixture.authority).map(|loan| loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Restoring { resume_task_id, .. }
        }) if resume_task_id == task_id
    ));
    // the foreground task never becomes the registered trainer
    assert_ne!(
        saved_resource(&fixture.store, fixture.authority).registered_background_task,
        Some(task_id)
    );
    assert!(matches!(
        fixture
            .store
            .reconcile_resource_queue_for_authority(fixture.authority, fixture.resource.id)
            .unwrap(),
        ResourceQueueReconcileOutcome::LoanAlreadyActive { .. }
    ));
    assert_eq!(
        request_state(fixture, later),
        Some(ResourceRequestState::Queued)
    );
}

#[test]
fn native_foreground_return_stays_reserved_while_running_and_its_confirmed_end_serves_the_queue() {
    let (mut fixture, authority, launch, later) = native_restore_fixture();
    let task_id = launch.task_id;
    let prior = completed_task(&fixture);

    // a queued row is not a confirmed start, and the queue stays behind the loan
    assert!(matches!(
        reconcile_restore(&mut fixture),
        RestoreReconcileOutcome::Queued { task_id: queued, .. } if queued == task_id
    ));
    assert_native_restore_reserved(&mut fixture, task_id, later.request_id);

    // a running foreground task keeps the loan; no release action is opened for it
    start_task(&fixture.store, task_id);
    assert!(matches!(
        reconcile_restore(&mut fixture),
        RestoreReconcileOutcome::ForegroundRunning { task_id: running, action_id, .. }
            if running == task_id && action_id == authority.action_id
    ));
    assert_native_restore_reserved(&mut fixture, task_id, later.request_id);
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).registered_background_task,
        Some(prior)
    );
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).state_revision,
        authority.expected_state_revision
    );
    assert_eq!(
        restore_closure_basis(&fixture.store, authority.action_id),
        None
    );

    // a restart reads the saved mode instead of classifying the command again
    fixture.store = Store::open(&fixture.directory.path().join("db")).unwrap();
    assert!(matches!(
        reconcile_restore(&mut fixture),
        RestoreReconcileOutcome::ForegroundRunning { task_id: running, .. } if running == task_id
    ));
    let mut original = authority;
    original.expected_state_revision = fixture.state_revision;
    assert_eq!(
        fixture
            .store
            .accept_return_task_for_authority(launch_input(original, launch.clone()))
            .unwrap(),
        ReturnTaskAcceptance::Existing {
            task: task_id,
            state: ProcessStatus::Running,
        }
    );
    assert!(matches!(
        fixture
            .store
            .resolve_ended_restore_for_authority(EndedRestoreResolution {
                authority,
                task_id,
                reason: "still running".into(),
            }),
        Err(ReturnDecisionError::RestoreNotEnded { .. })
    ));

    finish_running_task(
        &fixture.store,
        task_id,
        ExitReason::Exit { code: 0 },
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let RestoreReconcileOutcome::ForegroundEnded {
        closure,
        task_id: ended,
    } = reconcile_restore(&mut fixture)
    else {
        panic!("a successful confirmed foreground end must close the loan");
    };
    assert_eq!(ended, task_id);
    assert_eq!(
        closure.state_revision,
        ResourceRevision::new(authority.expected_state_revision.get() + 1)
    );
    assert!(matches!(
        closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::ForegroundReturnEnded {
                task_id: ended,
                outcome: ExitReason::Exit { code: 0 },
                ..
            }
        } if ended == task_id
    ));
    assert_eq!(
        restore_closure_basis(&fixture.store, authority.action_id).as_deref(),
        Some("foreground_ended")
    );
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).registered_background_task,
        None
    );
    assert_eq!(
        reconcile_restore(&mut fixture),
        RestoreReconcileOutcome::NotRestoring
    );
    assert_eq!(
        fixture
            .store
            .accept_return_task_for_authority(launch_input(original, launch))
            .unwrap(),
        ReturnTaskAcceptance::Existing {
            task: task_id,
            state: ProcessStatus::Succeeded,
        }
    );

    // the post-return request is served from the foreground-end idle boundary
    let ResourceQueueReconcileOutcome::IdleServing {
        loan,
        request,
        proof,
    } = fixture
        .store
        .reconcile_resource_queue_for_authority(fixture.authority, fixture.resource.id)
        .unwrap()
    else {
        panic!("the confirmed foreground end must let the queue continue");
    };
    assert_ne!(loan.id, authority.loan_id);
    assert_eq!(request.request_id, later.request_id);
    assert_eq!(
        proof,
        IdleBoundaryProof::ForegroundReturnEnded {
            loan_id: authority.loan_id,
            task_id,
        }
    );
}

#[test]
fn native_foreground_return_without_a_successful_confirmed_end_stays_reserved() {
    type Finish = fn(&Store, TaskId);
    let failed: Finish = |store, task_id| {
        finish_running_task(
            store,
            task_id,
            ExitReason::Exit { code: 1 },
            ProcessGroupExitEvidence::ConfirmedExited,
        );
    };
    let unconfirmed: Finish = |store, task_id| {
        finish_running_task(
            store,
            task_id,
            ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::Unconfirmed,
        );
    };
    let lost: Finish = |store, task_id| {
        store
            .cas_status(task_id, ProcessStatus::Running, ProcessStatus::Lost)
            .unwrap()
            .unwrap();
    };
    let changed_identity: Finish = |store, task_id| {
        finish_running_task(
            store,
            task_id,
            ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        store
            .conn
            .execute(
                "UPDATE tasks SET thread_id = ?1 WHERE id = ?2",
                params![Uuid::now_v7().to_string(), task_id.to_string()],
            )
            .unwrap();
    };
    type Expected = fn(&RestoreAttentionReason) -> bool;
    type Resolved = fn(&Result<ReturnClosure, ReturnDecisionError>) -> bool;
    let cases: [(&str, Finish, Expected, Resolved); 4] = [
        (
            "failed",
            failed,
            |reason| {
                *reason
                    == RestoreAttentionReason::ForegroundEnded {
                        state: ProcessStatus::Failed,
                    }
            },
            |result| {
                matches!(
                    result,
                    Ok(ReturnClosure {
                        loan: Loan {
                            state: LoanState::Closed {
                                result: LoanClosure::RestoreEnded {
                                    outcome: ExitReason::Exit { code: 1 },
                                    ..
                                }
                            },
                            ..
                        },
                        ..
                    })
                )
            },
        ),
        (
            "unconfirmed exit",
            unconfirmed,
            |reason| {
                *reason
                    == RestoreAttentionReason::ForegroundExitUnconfirmed {
                        state: ProcessStatus::Succeeded,
                    }
            },
            |result| {
                matches!(
                    result,
                    Err(ReturnDecisionError::RestoreReleaseUnproven { .. })
                )
            },
        ),
        (
            "lost",
            lost,
            |reason| *reason == RestoreAttentionReason::Lost,
            |result| {
                matches!(
                    result,
                    Err(ReturnDecisionError::RestoreReleaseUnproven { .. })
                )
            },
        ),
        (
            "changed identity",
            changed_identity,
            |reason| *reason == RestoreAttentionReason::IdentityMismatch,
            |result| matches!(result, Err(ReturnDecisionError::IdentityConflict { .. })),
        ),
    ];
    for (name, finish, expected, resolved) in cases {
        let (mut fixture, authority, launch, later) = native_restore_fixture();
        let task_id = launch.task_id;
        start_task(&fixture.store, task_id);
        finish(&fixture.store, task_id);

        match reconcile_restore(&mut fixture) {
            RestoreReconcileOutcome::Attention {
                task_id: attention,
                reason,
                ..
            } => {
                assert_eq!(attention, task_id, "{name}");
                assert!(expected(&reason), "{name}: {reason:?}");
            }
            other => panic!("{name}: expected attention, got {other:?}"),
        }
        assert_native_restore_reserved(&mut fixture, task_id, later.request_id);
        assert_eq!(
            restore_closure_basis(&fixture.store, authority.action_id),
            None,
            "{name}"
        );

        // only an exact supervisor resolution with proven release can close it
        let result = fixture
            .store
            .resolve_ended_restore_for_authority(EndedRestoreResolution {
                authority,
                task_id,
                reason: format!("{name} evaluation"),
            });
        assert!(resolved(&result), "{name}: {result:?}");
        if result.is_err() {
            assert_native_restore_reserved(&mut fixture, task_id, later.request_id);
            continue;
        }
        assert_eq!(
            saved_resource(&fixture.store, fixture.authority).registered_background_task,
            None,
            "{name}"
        );
        assert!(
            matches!(
                fixture
                    .store
                    .reconcile_resource_queue_for_authority(fixture.authority, fixture.resource.id)
                    .unwrap(),
                ResourceQueueReconcileOutcome::IdleServing { request, .. }
                    if request.request_id == later.request_id
            ),
            "{name}"
        );
    }
}

fn read_return_execution_mode(fixture: &ServingFixture) -> Option<ReturnExecutionMode> {
    read_return_execution_mode_for(&fixture.store, fixture.authority, fixture.resource.id)
}

fn read_return_execution_mode_for(
    store: &Store,
    authority: MachineId,
    resource: crate::resource::ResourceId,
) -> Option<ReturnExecutionMode> {
    store
        .resource_read_models(authority, Some(resource))
        .unwrap()
        .pop()
        .unwrap()
        .return_execution_mode
}

#[test]
fn resource_read_model_uses_saved_return_modes_only_while_restoring() {
    let (mut awaiting, authority) = awaiting_return_fixture();
    assert_eq!(read_return_execution_mode(&awaiting), None);
    awaiting
        .store
        .record_no_resume_for_authority(authority, "no return task".into())
        .unwrap();
    assert_eq!(read_return_execution_mode(&awaiting), None);

    let (native, _, _, _) = native_restore_fixture();
    assert_eq!(
        read_return_execution_mode(&native),
        Some(ReturnExecutionMode::NativeForeground)
    );

    let (mut direct, direct_authority, decision) = stopped_return_fixture();
    let input = resume_input(
        direct_authority,
        direct.task_id,
        &decision.selected_checkpoint.generation_id,
    );
    let ReturnTaskAcceptance::Inserted { .. } = direct
        .store
        .accept_return_task_for_authority(input)
        .unwrap()
    else {
        panic!("the direct-segment return task must be accepted");
    };
    assert_eq!(
        read_return_execution_mode_for(&direct.store, direct.authority, direct.resource.id),
        Some(ReturnExecutionMode::DirectSegmentTrainer)
    );
}

#[test]
fn resource_read_model_hides_mismatched_return_modes() {
    let (stale_action, _, _, _) = native_restore_fixture();
    stale_action
        .store
        .conn
        .execute(
            "UPDATE loans
             SET state_json = json_set(state_json, '$.phase.action_id', ?1)
             WHERE id = ?2",
            rusqlite::params![
                ActionId::new().as_uuid().to_string(),
                stale_action.loan.id.as_uuid().to_string(),
            ],
        )
        .unwrap();
    assert_eq!(read_return_execution_mode(&stale_action), None);

    for identity in ["loan", "task"] {
        let (fixture, current_authority, launch, _) = native_restore_fixture();
        let (column, replacement) = match identity {
            "loan" => ("$.result.loan.id", LoanId::new().as_uuid().to_string()),
            "task" => ("$.result.task_id", TaskId::new().to_string()),
            _ => unreachable!(),
        };
        fixture
            .store
            .conn
            .execute(
                &format!(
                    "UPDATE resource_return_decisions SET receipt_json = json_set(receipt_json, '{column}', ?1) WHERE action_id = ?2"
                ),
                rusqlite::params![replacement, current_authority.action_id.as_uuid().to_string()],
            )
            .unwrap();
        assert_eq!(
            read_return_execution_mode(&fixture),
            None,
            "mismatched {identity} identity must not expose a mode for task {}",
            launch.task_id
        );
    }
}

#[test]
fn lost_restore_stays_reserved_and_cannot_be_resolved() {
    let (mut fixture, authority) = awaiting_return_fixture();
    let launch = evaluation(&fixture, &["/bin/echo", "evaluate"]);
    let task_id = launch.task_id;
    let ReturnTaskAcceptance::Inserted { state_revision, .. } = fixture
        .store
        .accept_return_task_for_authority(launch_input(authority, launch))
        .unwrap()
    else {
        panic!("the first exact launch must insert its task");
    };
    fixture
        .store
        .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Lost)
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
    let mut current = authority;
    current.expected_state_revision = state_revision;
    assert!(matches!(
        fixture
            .store
            .resolve_ended_restore_for_authority(EndedRestoreResolution {
                authority: current,
                task_id,
                reason: "lost worker".into(),
            }),
        Err(ReturnDecisionError::RestoreReleaseUnproven { task_id: lost }) if lost == task_id
    ));
    assert!(matches!(
        saved_loan(&fixture.store, fixture.authority).map(|loan| loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Restoring { resume_task_id, .. }
        }) if resume_task_id == task_id
    ));
}

#[test]
fn restore_that_ended_early_closes_only_by_explicit_supervisor_resolution() {
    let (mut fixture, authority) = awaiting_return_fixture();
    let launch = evaluation(&fixture, &["/bin/echo", "evaluate"]);
    let task_id = launch.task_id;
    let ReturnTaskAcceptance::Inserted { state_revision, .. } = fixture
        .store
        .accept_return_task_for_authority(launch_input(authority, launch))
        .unwrap()
    else {
        panic!("the first exact launch must insert its task");
    };
    let mut current = authority;
    current.expected_state_revision = state_revision;
    let resolution = |authority, reason: &str| EndedRestoreResolution {
        authority,
        task_id,
        reason: reason.into(),
    };
    assert!(matches!(
        fixture
            .store
            .resolve_ended_restore_for_authority(resolution(current, "not started")),
        Err(ReturnDecisionError::RestoreNotEnded { .. })
    ));

    // the task starts and fails between two observations
    fixture
        .store
        .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    fixture
        .store
        .cas_exit_with_evidence(
            task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 1 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .reconcile_restoring_loan_for_authority(fixture.authority, fixture.resource.id)
            .unwrap(),
        RestoreReconcileOutcome::Attention {
            reason: RestoreAttentionReason::ForegroundEnded {
                state: ProcessStatus::Failed
            },
            ..
        }
    ));

    let mut other_thread = current;
    other_thread.supervisor.thread = ThreadId(Uuid::now_v7());
    assert!(matches!(
        fixture
            .store
            .resolve_ended_restore_for_authority(resolution(other_thread, "failed")),
        Err(ReturnDecisionError::NotCurrentSupervisor)
    ));
    assert!(matches!(
        fixture
            .store
            .resolve_ended_restore_for_authority(resolution(authority, "failed")),
        Err(ReturnDecisionError::StaleRevision { .. })
    ));

    let closure = fixture
        .store
        .resolve_ended_restore_for_authority(resolution(current, "evaluation failed"))
        .unwrap();
    assert!(matches!(
        &closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::RestoreEnded {
                task_id: ended,
                outcome: ExitReason::Exit { code: 1 },
                reason,
                ..
            }
        } if *ended == task_id && reason == "evaluation failed"
    ));
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).registered_background_task,
        None
    );
    assert_eq!(
        fixture
            .store
            .resolve_ended_restore_for_authority(resolution(current, "evaluation failed"))
            .unwrap(),
        closure
    );
    assert!(matches!(
        fixture
            .store
            .resolve_ended_restore_for_authority(resolution(current, "other reason")),
        Err(ReturnDecisionError::ConflictingRetry { .. })
    ));
}

/// Release a stopped trainer with no queued work so its checkpoint context awaits return
pub(super) fn stopped_return_fixture() -> (
    TrainerAssociationFixture,
    SupervisorActionAuthority,
    ReleaseCheckpointStopDecision,
) {
    let (mut fixture, _, request_id, action_id, revision, _) = release_completion_fixture();
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
    let (decision, _) = commit_stopped_release_decision(&mut fixture, action_id, revision);
    fixture
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
    let ReleaseCompletionResult::ReturnRequired { loan, notice } = fixture
        .store
        .complete_release_for_authority(fixture.authority, fixture.resource.id, action_id, revision)
        .unwrap()
    else {
        panic!("the empty queue must return the verified stopped context");
    };
    let authority = SupervisorActionAuthority {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        loan_id: loan.id,
        action_id: notice.action_id,
        expected_state_revision: notice.state_revision,
        supervisor: fixture.resource.supervisor,
        assignment_revision: fixture.resource.assignment_revision,
    };
    (fixture, authority, decision)
}

pub(super) fn resume_input(
    authority: SupervisorActionAuthority,
    stopped_task: TaskId,
    recovery_ref: &str,
) -> ReturnTaskAcceptanceInput {
    ReturnTaskAcceptanceInput {
        authority,
        launch: ReturnLaunch {
            request_id: RequestId::new(),
            task_id: TaskId::new(),
            work: ReturnWork::SameRunResume {
                stopped_task,
                recovery_ref: recovery_ref.into(),
            },
        },
        // a resume keeps the run's saved environment, not the daemon's current one
        executor_env: TaskEnv {
            path: "/usr/bin".into(),
            home: "/nonexistent".into(),
        },
        origin: ReturnTaskOrigin::Local {
            callback_codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
        },
    }
}

#[test]
fn same_run_resume_derives_the_saved_run_command_with_its_selected_checkpoint() {
    let (mut fixture, authority, decision) = stopped_return_fixture();
    let generation = decision.selected_checkpoint.generation_id.clone();

    assert!(matches!(
        fixture.store.accept_return_task_for_authority(resume_input(
            authority,
            fixture.task_id,
            "other"
        )),
        Err(ReturnDecisionError::Rejected(
            ReturnDecisionRejection::ResumeRequiresStoppedContext
        ))
    ));

    let input = resume_input(authority, fixture.task_id, &generation);
    let task_id = input.launch.task_id;
    assert!(matches!(
        fixture.store.accept_return_task_for_authority(input).unwrap(),
        ReturnTaskAcceptance::Inserted { task, .. } if task == task_id
    ));
    let original = fixture.store.get_task(fixture.task_id).unwrap().unwrap();
    let resumed = fixture.store.get_task(task_id).unwrap().unwrap();
    let mut expected = fixture.command();
    expected.extend(["--resume".to_owned(), generation]);
    let Workload::Task(workload) = &resumed.workload else {
        panic!("a resume must be a command task");
    };
    assert_eq!(workload.command.to_vec(), expected);
    assert_eq!(resumed.binary, original.binary);
    assert_eq!(resumed.env, original.env);
    assert_eq!(resumed.cwd, original.cwd);
    assert_eq!(resumed.thread, authority.supervisor.thread);
}

#[test]
fn same_run_resume_fails_closed_before_any_record_when_the_checkpoint_is_gone() {
    let (mut fixture, authority, decision) = stopped_return_fixture();
    fs::remove_dir_all(&decision.selected_checkpoint.path).unwrap();

    let input = resume_input(
        authority,
        fixture.task_id,
        &decision.selected_checkpoint.generation_id,
    );
    let task_id = input.launch.task_id;
    assert!(matches!(
        fixture.store.accept_return_task_for_authority(input),
        Err(ReturnDecisionError::Rejected(
            ReturnDecisionRejection::ResumeUnproven {
                gap: SameRunResumeGap::CheckpointUnavailable
            }
        ))
    ));
    assert!(fixture.store.get_task(task_id).unwrap().is_none());
    assert!(matches!(
        fixture
            .store
            .resource_snapshots_for_authority(fixture.authority)
            .unwrap()[0]
            .loan
            .as_ref()
            .map(|loan| &loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::AwaitingReturn { .. }
        })
    ));
}

/// Bind a same-run resume, then let it start and fail with a confirmed wrapper exit
///
/// Returns the resolution authority at the Restoring revision and the resume task
fn resumed_wrapper_exit_fixture() -> (TrainerAssociationFixture, SupervisorActionAuthority, TaskId)
{
    let (mut fixture, authority, decision) = stopped_return_fixture();
    let input = resume_input(
        authority,
        fixture.task_id,
        &decision.selected_checkpoint.generation_id,
    );
    let task_id = input.launch.task_id;
    let ReturnTaskAcceptance::Inserted { state_revision, .. } = fixture
        .store
        .accept_return_task_for_authority(input)
        .unwrap()
    else {
        panic!("the first exact resume must insert its task");
    };
    fixture
        .store
        .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    fixture
        .store
        .cas_exit_with_evidence(
            task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 1 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
    let mut current = authority;
    current.expected_state_revision = state_revision;
    (fixture, current, task_id)
}

fn resolve(
    fixture: &mut TrainerAssociationFixture,
    authority: SupervisorActionAuthority,
    task_id: TaskId,
) -> Result<ReturnClosure, ReturnDecisionError> {
    fixture
        .store
        .resolve_ended_restore_for_authority(EndedRestoreResolution {
            authority,
            task_id,
            reason: "resume failed".into(),
        })
}

fn assert_still_restoring(fixture: &TrainerAssociationFixture, task_id: TaskId) {
    assert!(matches!(
        saved_loan(&fixture.store, fixture.authority).map(|loan| loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Restoring { resume_task_id, .. }
        }) if resume_task_id == task_id
    ));
}

#[test]
fn resumed_trainer_wrapper_exit_closes_restore_only_after_the_exact_lock_is_free() {
    let (mut fixture, authority, task_id) = resumed_wrapper_exit_fixture();
    let stopped = fixture.task_id;
    let lock_path = fixture.runtime_root.join(".segment.lock");

    // the resumed worker runs in its own session and still holds the run's lock
    let mut worker = start_fake_lock_process(&lock_path);
    assert!(matches!(
        resolve(&mut fixture, authority, task_id),
        Err(ReturnDecisionError::RestoreOwnershipUnproven {
            task_id: ended,
            gap: TrainerLockReleaseGap::OwnershipHeld { task_id: witness },
        }) if ended == task_id && witness == stopped
    ));
    assert_still_restoring(&fixture, task_id);
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).state_revision,
        authority.expected_state_revision
    );

    worker.release();
    let closure = resolve(&mut fixture, authority, task_id).unwrap();
    assert!(matches!(
        &closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::RestoreEnded {
                task_id: ended,
                outcome: ExitReason::Exit { code: 1 },
                ..
            }
        } if *ended == task_id
    ));
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).registered_background_task,
        None
    );
    // the authority held the lock only through the closing transaction
    let identity = OwnershipLockIdentity::from_metadata(&fs::metadata(&lock_path).unwrap());
    assert!(matches!(
        probe_segment_ownership_lock(&fixture.runtime_root, identity),
        OwnershipLockProbe::ExactOwnershipReleased(_)
    ));
}

#[test]
fn resumed_trainer_wrapper_exit_with_changed_or_missing_lock_evidence_stays_reserved() {
    type Change = fn(&TrainerAssociationFixture);
    let replace_lock: Change = |fixture| {
        let lock_path = fixture.runtime_root.join(".segment.lock");
        fs::rename(&lock_path, fixture.runtime_root.join(".segment.lock.saved")).unwrap();
        fs::write(&lock_path, b"").unwrap();
    };
    let remove_association: Change = |fixture| {
        fixture
            .store
            .conn
            .execute(
                "DELETE FROM trainer_attempt_associations WHERE task_id = ?1",
                [fixture.task_id.to_string()],
            )
            .unwrap();
    };
    type Expected = fn(&TrainerLockReleaseGap, TaskId) -> bool;
    let different_file: Expected = |gap, _| {
        matches!(
            gap,
            TrainerLockReleaseGap::OwnershipLock(OwnershipLockProbeError::IdentityMismatch {
                reason: OwnershipLockIdentityMismatchReason::DifferentFile,
                ..
            })
        )
    };
    let missing: Expected = |gap, stopped| matches!(gap, TrainerLockReleaseGap::AssociationMissing { task_id } if *task_id == stopped);
    for (name, change, expected) in [
        ("replaced lock file", replace_lock, different_file),
        ("missing association", remove_association, missing),
    ] {
        let (mut fixture, authority, task_id) = resumed_wrapper_exit_fixture();
        change(&fixture);

        match resolve(&mut fixture, authority, task_id) {
            Err(ReturnDecisionError::RestoreOwnershipUnproven {
                task_id: ended,
                gap,
            }) => {
                assert_eq!(ended, task_id, "{name}");
                assert!(expected(&gap, fixture.task_id), "{name}: {gap:?}");
            }
            other => panic!("{name}: expected a fail-closed restore, got {other:?}"),
        }
        assert_still_restoring(&fixture, task_id);
    }
}

#[test]
fn resume_that_never_spawned_closes_without_a_lock_witness() {
    let (mut fixture, authority, decision) = stopped_return_fixture();
    let input = resume_input(
        authority,
        fixture.task_id,
        &decision.selected_checkpoint.generation_id,
    );
    let task_id = input.launch.task_id;
    let ReturnTaskAcceptance::Inserted { state_revision, .. } = fixture
        .store
        .accept_return_task_for_authority(input)
        .unwrap()
    else {
        panic!("the first exact resume must insert its task");
    };
    // cancelling a queued row records that no worker child started
    fixture
        .store
        .cas_exit(task_id, ProcessStatus::Queued, &ExitReason::Cancelled)
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture
            .store
            .get_task(task_id)
            .unwrap()
            .unwrap()
            .process_group_exit_evidence(),
        ProcessGroupExitEvidence::NoChildSpawned
    );
    // no association can name a lock, and none is needed
    fixture
        .store
        .conn
        .execute(
            "DELETE FROM trainer_attempt_associations WHERE task_id = ?1",
            [fixture.task_id.to_string()],
        )
        .unwrap();

    let mut current = authority;
    current.expected_state_revision = state_revision;
    let closure = resolve(&mut fixture, current, task_id).unwrap();
    assert!(matches!(
        &closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::RestoreEnded {
                task_id: ended,
                outcome: ExitReason::Cancelled,
                ..
            }
        } if *ended == task_id
    ));
}

#[test]
fn same_run_resume_closes_on_its_confirmed_start_and_registers_the_trainer() {
    let (mut fixture, authority, decision) = stopped_return_fixture();
    let input = resume_input(
        authority,
        fixture.task_id,
        &decision.selected_checkpoint.generation_id,
    );
    let task_id = input.launch.task_id;
    let ReturnTaskAcceptance::Inserted { state_revision, .. } = fixture
        .store
        .accept_return_task_for_authority(input)
        .unwrap()
    else {
        panic!("the first exact resume must insert its task");
    };
    start_task(&fixture.store, task_id);

    let RestoreReconcileOutcome::Closed {
        closure,
        task_id: registered,
    } = fixture
        .store
        .reconcile_restoring_loan_for_authority(fixture.authority, fixture.resource.id)
        .unwrap()
    else {
        panic!("a confirmed trainer start must close the loan");
    };
    assert_eq!(registered, task_id);
    assert_eq!(
        closure.state_revision,
        ResourceRevision::new(state_revision.get() + 1)
    );
    assert!(matches!(
        closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::Resumed { task_id: resumed, .. }
        } if resumed == task_id
    ));
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).registered_background_task,
        Some(task_id)
    );
    assert_eq!(
        restore_closure_basis(&fixture.store, authority.action_id).as_deref(),
        Some("confirmed_running")
    );
}

/// Move the drained fixture's supervisor to another machine
fn remote_awaiting_return_fixture() -> (ServingFixture, SupervisorActionAuthority) {
    let (fixture, mut authority) = awaiting_return_fixture();
    let remote = machine_other_than(fixture.authority);
    fixture
        .store
        .conn
        .execute(
            "UPDATE resources SET supervisor_machine = ?1 WHERE id = ?2",
            params![
                remote.to_string(),
                fixture.resource.id.as_uuid().to_string()
            ],
        )
        .unwrap();
    authority.supervisor.machine = remote;
    (fixture, authority)
}

fn remote_input(
    authority: SupervisorActionAuthority,
    launch: ReturnLaunch,
    normalized_spec_sha256: NormalizedSpecSha256,
) -> ReturnTaskAcceptanceInput {
    ReturnTaskAcceptanceInput {
        authority,
        launch,
        executor_env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
        origin: ReturnTaskOrigin::Remote {
            normalized_spec_sha256,
        },
    }
}

fn prepared_digest(
    fixture: &mut ServingFixture,
    authority: SupervisorActionAuthority,
    launch: &ReturnLaunch,
) -> NormalizedSpecSha256 {
    fixture
        .store
        .prepare_return_task_for_authority(
            authority,
            launch.clone(),
            TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
        )
        .unwrap()
        .normalized_spec_sha256
}

fn action_receipt_count(store: &Store) -> i64 {
    store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_action_task_receipts",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn remote_native_return_binds_once_and_closes_only_after_its_confirmed_end() {
    let (mut fixture, authority) = remote_awaiting_return_fixture();
    let launch = evaluation(&fixture, &["/bin/echo", "evaluate"]);
    let task_id = launch.task_id;
    let digest = prepared_digest(&mut fixture, authority, &launch);
    // preparing derives the spec without writing any task or decision record
    assert!(fixture.store.get_task(task_id).unwrap().is_none());
    assert_still_awaiting_return(&fixture, &authority);

    let ReturnTaskAcceptance::Inserted { .. } = fixture
        .store
        .accept_return_task_for_authority(remote_input(authority, launch.clone(), digest))
        .unwrap()
    else {
        panic!("the first remote launch must insert its task");
    };
    assert_eq!(action_receipt_count(&fixture.store), 1);
    let Some(ExecutorIdentity::Accepted(record)) =
        fixture.store.executor_identity(task_id).unwrap()
    else {
        panic!("the return task must have an accepted identity");
    };
    assert_eq!(record.origin_machine, authority.supervisor.machine);
    assert_eq!(record.execution_machine, fixture.authority);
    // no authority-local callback route is substituted for the remote supervisor
    assert!(
        fixture
            .store
            .origin_route_by_task(task_id)
            .unwrap()
            .is_none()
    );

    // exact retries observe the binding; the local path observes it without a route
    for input in [
        remote_input(authority, launch.clone(), digest),
        launch_input(authority, launch.clone()),
    ] {
        assert_eq!(
            fixture
                .store
                .accept_return_task_for_authority(input)
                .unwrap(),
            ReturnTaskAcceptance::Existing {
                task: task_id,
                state: ProcessStatus::Queued,
            }
        );
    }
    let other_digest = normalized_spec_sha256(&fixture.spec).unwrap();
    assert!(matches!(
        fixture.store.accept_return_task_for_authority(remote_input(
            authority,
            launch.clone(),
            other_digest
        )),
        Err(ReturnDecisionError::SpecMismatch)
    ));

    assert!(matches!(
        fixture
            .store
            .reconcile_restoring_loan_for_authority(fixture.authority, fixture.resource.id)
            .unwrap(),
        RestoreReconcileOutcome::Queued { task_id: queued, .. } if queued == task_id
    ));
    start_task(&fixture.store, task_id);
    assert!(matches!(
        reconcile_restore(&mut fixture),
        RestoreReconcileOutcome::ForegroundRunning { task_id: running, .. } if running == task_id
    ));
    assert_eq!(
        fixture
            .store
            .accept_return_task_for_authority(remote_input(authority, launch.clone(), digest))
            .unwrap(),
        ReturnTaskAcceptance::Existing {
            task: task_id,
            state: ProcessStatus::Running,
        }
    );
    finish_running_task(
        &fixture.store,
        task_id,
        ExitReason::Exit { code: 0 },
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let RestoreReconcileOutcome::ForegroundEnded {
        closure,
        task_id: ended,
    } = reconcile_restore(&mut fixture)
    else {
        panic!("a successful confirmed end must close the remote restore");
    };
    assert_eq!(ended, task_id);
    assert!(matches!(
        closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::ForegroundReturnEnded { task_id: ended, .. }
        } if ended == task_id
    ));
    // the remote callback owner keeps its receipt, and no local route replaces it
    assert_eq!(action_receipt_count(&fixture.store), 1);
    assert!(
        fixture
            .store
            .origin_route_by_task(task_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .store
            .accept_return_task_for_authority(remote_input(authority, launch, digest))
            .unwrap(),
        ReturnTaskAcceptance::Existing {
            task: task_id,
            state: ProcessStatus::Succeeded,
        }
    );
}

#[test]
fn remote_return_rejects_changed_spec_and_wrong_origin_without_records() {
    let (mut fixture, authority) = remote_awaiting_return_fixture();
    let launch = evaluation(&fixture, &["/bin/echo", "evaluate"]);
    let task_id = launch.task_id;
    let wrong = normalized_spec_sha256(&fixture.spec).unwrap();
    assert!(matches!(
        fixture.store.accept_return_task_for_authority(remote_input(
            authority,
            launch.clone(),
            wrong
        )),
        Err(ReturnDecisionError::SpecMismatch)
    ));
    assert!(fixture.store.get_task(task_id).unwrap().is_none());
    assert_eq!(action_receipt_count(&fixture.store), 0);
    assert_still_awaiting_return(&fixture, &authority);

    // a co-located supervisor cannot use the remote origin
    let (mut local, local_authority) = awaiting_return_fixture();
    let launch = evaluation(&local, &["/bin/echo", "evaluate"]);
    let digest = prepared_digest(&mut local, local_authority, &launch);
    assert!(matches!(
        local
            .store
            .accept_return_task_for_authority(remote_input(local_authority, launch, digest)),
        Err(ReturnDecisionError::OriginMismatch)
    ));
    assert_eq!(action_receipt_count(&local.store), 0);
    assert_still_awaiting_return(&local, &local_authority);
}

#[test]
fn remote_return_early_end_and_no_resume_need_the_exact_remote_supervisor() {
    let (mut fixture, authority) = remote_awaiting_return_fixture();
    let launch = evaluation(&fixture, &["/bin/echo", "evaluate"]);
    let task_id = launch.task_id;
    let digest = prepared_digest(&mut fixture, authority, &launch);
    let ReturnTaskAcceptance::Inserted { state_revision, .. } = fixture
        .store
        .accept_return_task_for_authority(remote_input(authority, launch, digest))
        .unwrap()
    else {
        panic!("the first remote launch must insert its task");
    };
    fixture
        .store
        .cas_exit_with_evidence(
            task_id,
            ProcessStatus::Queued,
            &ExitReason::Cancelled,
            ProcessGroupExitEvidence::NoChildSpawned,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .reconcile_restoring_loan_for_authority(fixture.authority, fixture.resource.id)
            .unwrap(),
        RestoreReconcileOutcome::Attention {
            reason: RestoreAttentionReason::ForegroundEnded {
                state: ProcessStatus::Cancelled
            },
            ..
        }
    ));
    let mut current = authority;
    current.expected_state_revision = state_revision;
    let mut replaced = current;
    replaced.supervisor.machine = fixture.authority;
    assert!(matches!(
        fixture
            .store
            .resolve_ended_restore_for_authority(EndedRestoreResolution {
                authority: replaced,
                task_id,
                reason: "never started".into(),
            }),
        Err(ReturnDecisionError::NotCurrentSupervisor)
    ));
    let closure = fixture
        .store
        .resolve_ended_restore_for_authority(EndedRestoreResolution {
            authority: current,
            task_id,
            reason: "never started".into(),
        })
        .unwrap();
    assert!(matches!(
        closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::RestoreEnded { task_id: ended, .. }
        } if ended == task_id
    ));

    let (mut no_resume, no_resume_authority) = remote_awaiting_return_fixture();
    let closure = no_resume
        .store
        .record_no_resume_for_authority(no_resume_authority, "training finished".into())
        .unwrap();
    assert!(matches!(
        closure.loan.state,
        LoanState::Closed {
            result: LoanClosure::NoResume { .. }
        }
    ));
}

#[test]
fn unstorable_assignment_revision_is_a_failed_compare_not_a_supervisor_change() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let saved = resource(authority);
    store.register_resource(authority, &saved).unwrap();
    // no stored row can hold this revision, so the compare can never match
    let mut snapshot = saved.clone();
    snapshot.assignment_revision = AssignmentRevision::new(u64::MAX);

    let tx = store.conn.transaction().unwrap();
    let result = crate::store::resource::restore::advance_resource(&tx, &snapshot, None);

    assert!(
        matches!(
            result,
            Err(ReturnDecisionError::StaleRevision { expected, actual })
                if expected == saved.state_revision && actual == saved.state_revision
        ),
        "{result:?}"
    );
}
