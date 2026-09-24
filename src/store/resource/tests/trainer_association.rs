//! Trainer attempt association binding, validation, and migration tests

use super::fixtures::{
    TrainerAssociationFixture, awaiting_release_state, machine_other_than, resource,
    saved_trainer_association_json,
};
use crate::domain::{ProcessStatus, TaskId};
use crate::machine::MachineId;
use crate::resource::command_shape::DirectSegmentCommandShapeError;
use crate::resource::ownership_lock::{
    OwnershipLockIdentity, TrainerRequestDigest, VerifiedTrainerAttempt,
    test_support as trainer_attempt_test_support,
};
use crate::resource::store::{ResourceStoreError, TrainerAttemptAssociationStoreError};
use crate::resource::{ActionId, LoanPhase, LoanState, ResourceId, ReturnContext};
use crate::store::Store;
use crate::submission::{RequestId, normalized_spec_sha256};
use rusqlite::params;
use std::path::PathBuf;

#[test]
fn trainer_attempt_association_binds_valid_direct_segment_shape_and_reads_it() {
    let mut fixture = TrainerAssociationFixture::new();
    fixture.insert_accepted_running_task();
    let evidence = fixture.evidence();

    let association = fixture.bind(evidence.clone()).unwrap();

    assert_eq!(association.resource_id(), fixture.resource.id);
    assert_eq!(association.authority_machine(), fixture.authority);
    assert_eq!(association.task_id(), fixture.task_id);
    assert_eq!(association.verified_attempt(), &evidence);
    assert_eq!(
        association.normalized_spec_sha256(),
        normalized_spec_sha256(&fixture.spec).unwrap()
    );
    assert_eq!(
        fixture
            .store
            .trainer_attempt_association_for_task_for_authority(fixture.authority, fixture.task_id)
            .unwrap(),
        Some(association)
    );
    let loan_count: i64 = fixture
        .store
        .conn
        .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
        .unwrap();
    assert_eq!(loan_count, 0);
}

#[test]
fn trainer_attempt_association_rejects_shell_and_wrong_module_without_a_row() {
    let mut shell = TrainerAssociationFixture::new();
    let evidence = shell.evidence();
    shell.set_command(vec![
        "/bin/sh".into(),
        "-c".into(),
        "echo not a trainer".into(),
    ]);
    shell.insert_accepted_running_task();
    assert!(matches!(
        shell.bind(evidence),
        Err(
            TrainerAttemptAssociationStoreError::DirectSegmentCommandShape(
                DirectSegmentCommandShapeError::NotPythonExecutable { .. }
            )
        )
    ));
    let shell_count: i64 = shell
        .store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM trainer_attempt_associations",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(shell_count, 0);

    let mut wrong_module = TrainerAssociationFixture::new();
    let evidence = wrong_module.evidence();
    let mut command = wrong_module.command();
    command[2] = "ops.not_run_segment".into();
    wrong_module.set_command(command);
    wrong_module.insert_accepted_running_task();
    assert!(matches!(
        wrong_module.bind(evidence),
        Err(
            TrainerAttemptAssociationStoreError::DirectSegmentCommandShape(
                DirectSegmentCommandShapeError::InvalidModuleInvocation { .. }
            )
        )
    ));
    let wrong_module_count: i64 = wrong_module
        .store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM trainer_attempt_associations",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(wrong_module_count, 0);
}

#[test]
fn trainer_attempt_association_rejects_runtime_root_mismatch_without_a_row() {
    let mut fixture = TrainerAssociationFixture::new();
    let evidence = fixture.evidence();
    let mut command = fixture.command();
    let runtime_root_index = command
        .iter()
        .position(|argument| argument == "--runtime-root")
        .unwrap();
    command[runtime_root_index + 1] = fixture.home.join("other-runtime").display().to_string();
    fixture.set_command(command);
    fixture.insert_accepted_running_task();

    assert!(matches!(
        fixture.bind(evidence),
        Err(
            TrainerAttemptAssociationStoreError::DirectSegmentCommandShape(
                DirectSegmentCommandShapeError::RuntimeRootMismatch { .. }
            )
        )
    ));
    let association_count: i64 = fixture
        .store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM trainer_attempt_associations",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(association_count, 0);
}

#[test]
fn trainer_attempt_association_exact_retry_after_terminal_and_file_removal() {
    let mut fixture = TrainerAssociationFixture::new();
    fixture.insert_accepted_running_task();
    let evidence = fixture.evidence();
    let first = fixture.bind(evidence.clone()).unwrap();
    let before = saved_trainer_association_json(&fixture.store, fixture.task_id);
    fixture.finish_registered_task();
    fixture.remove_shape_files();
    let TrainerAssociationFixture {
        _directory,
        database,
        store,
        authority,
        resource,
        task_id,
        ..
    } = fixture;
    drop(store);

    let mut reopened = Store::open(&database).unwrap();
    let retry = reopened
        .bind_trainer_attempt_association(authority, resource.id, task_id, evidence)
        .unwrap();

    assert_eq!(retry, first);
    assert_eq!(saved_trainer_association_json(&reopened, task_id), before);
    let association_count: i64 = reopened
        .conn
        .query_row(
            "SELECT COUNT(*) FROM trainer_attempt_associations",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(association_count, 1);
}

#[test]
fn trainer_attempt_association_rejects_changed_attempt_lock_and_request_digest() {
    let mut fixture = TrainerAssociationFixture::new();
    fixture.insert_accepted_running_task();
    let evidence = fixture.evidence();
    fixture.bind(evidence.clone()).unwrap();
    let saved = saved_trainer_association_json(&fixture.store, fixture.task_id);
    fixture.finish_registered_task();
    fixture.remove_shape_files();
    let changed_attempt = trainer_attempt_test_support::verified_attempt(
        trainer_attempt_test_support::attempt_binding("attempt-2"),
        [0x42; 32],
        OwnershipLockIdentity::new(7, 11),
    );
    let changed_lock = trainer_attempt_test_support::verified_attempt(
        trainer_attempt_test_support::attempt_binding("attempt-1"),
        [0x42; 32],
        OwnershipLockIdentity::new(7, 12),
    );
    let changed_request = trainer_attempt_test_support::verified_attempt(
        trainer_attempt_test_support::attempt_binding("attempt-1"),
        [0x43; 32],
        OwnershipLockIdentity::new(7, 11),
    );

    for changed in [changed_attempt, changed_lock, changed_request] {
        assert!(matches!(
            fixture.bind(changed),
            Err(TrainerAttemptAssociationStoreError::Conflict { resource_id })
                if resource_id == fixture.resource.id
        ));
    }
    assert_eq!(
        saved_trainer_association_json(&fixture.store, fixture.task_id),
        saved
    );
}

#[test]
fn trainer_attempt_associations_keep_history_and_read_only_the_current_registration() {
    let mut fixture = TrainerAssociationFixture::new();
    fixture.insert_accepted_running_task();
    let first = fixture.bind(fixture.evidence()).unwrap();
    let second_task = TaskId::new();
    let second_evidence = VerifiedTrainerAttempt::from_persisted(
        fixture.runtime_root.clone(),
        trainer_attempt_test_support::attempt_binding("attempt-2"),
        TrainerRequestDigest::from_hex(&"43".repeat(32)).unwrap(),
        OwnershipLockIdentity::new(7, 12),
    )
    .unwrap();

    fixture
        .store
        .conn
        .execute(
            "UPDATE resources SET registered_background_task=?1 WHERE id=?2",
            params![
                second_task.to_string(),
                fixture.resource.id.as_uuid().to_string()
            ],
        )
        .unwrap();
    let second_row = fixture.trainer_task(second_task, &fixture.spec);
    fixture
        .store
        .insert_local_task(
            &second_row,
            &fixture.spec,
            fixture.authority,
            PathBuf::from("/bin/echo").into(),
        )
        .unwrap();
    fixture
        .store
        .cas_status(second_task, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();

    let second = fixture
        .store
        .bind_trainer_attempt_association(
            fixture.authority,
            fixture.resource.id,
            second_task,
            second_evidence,
        )
        .unwrap();

    assert_ne!(first.task_id(), second.task_id());
    assert_eq!(
        fixture
            .store
            .trainer_attempt_association_for_task_for_authority(fixture.authority, fixture.task_id)
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        fixture
            .store
            .trainer_attempt_association_for_task_for_authority(fixture.authority, second_task)
            .unwrap(),
        Some(second)
    );
    let association_count: i64 = fixture
        .store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM trainer_attempt_associations WHERE resource_id=?1",
            [fixture.resource.id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(association_count, 2);

    fixture
        .store
        .conn
        .execute(
            "UPDATE resources SET registered_background_task=NULL WHERE id=?1",
            [fixture.resource.id.as_uuid().to_string()],
        )
        .unwrap();
    assert_eq!(
        fixture
            .store
            .trainer_attempt_association_for_task_for_authority(fixture.authority, fixture.task_id)
            .unwrap(),
        Some(first)
    );
}

#[test]
fn trainer_attempt_association_rejects_wrong_authority_resource_and_task() {
    let mut fixture = TrainerAssociationFixture::new();
    fixture.insert_accepted_running_task();
    let evidence = fixture.evidence();
    let wrong_authority = MachineId::new();
    assert!(matches!(
        fixture.store.bind_trainer_attempt_association(
            wrong_authority,
            fixture.resource.id,
            fixture.task_id,
            evidence.clone(),
        ),
        Err(TrainerAttemptAssociationStoreError::Resource(
            ResourceStoreError::WrongAuthority { .. }
        ))
    ));
    assert!(matches!(
        fixture.store.bind_trainer_attempt_association(
            fixture.authority,
            ResourceId::new(),
            fixture.task_id,
            evidence.clone(),
        ),
        Err(TrainerAttemptAssociationStoreError::Resource(
            ResourceStoreError::ResourceNotFound
        ))
    ));

    let wrong_task = TaskId::new();
    assert!(matches!(
        fixture.store.bind_trainer_attempt_association(
            fixture.authority,
            fixture.resource.id,
            wrong_task,
            evidence.clone(),
        ),
        Err(TrainerAttemptAssociationStoreError::TaskNotRegistered { task_id })
            if task_id == wrong_task
    ));

    let mut other_resource = resource(fixture.authority);
    other_resource.registered_background_task = Some(TaskId::new());
    fixture
        .store
        .register_resource(fixture.authority, &other_resource)
        .unwrap();
    assert!(matches!(
        fixture.store.bind_trainer_attempt_association(
            fixture.authority,
            other_resource.id,
            fixture.task_id,
            evidence,
        ),
        Err(TrainerAttemptAssociationStoreError::TaskNotRegistered { task_id })
            if task_id == fixture.task_id
    ));
}

#[test]
fn trainer_attempt_association_requires_running_task_identity_and_matching_spec() {
    let mut missing_task = TrainerAssociationFixture::new();
    let evidence = missing_task.evidence();
    assert!(matches!(
        missing_task.bind(evidence),
        Err(TrainerAttemptAssociationStoreError::TaskMissing { task_id })
            if task_id == missing_task.task_id
    ));

    let mut queued_task = TrainerAssociationFixture::new();
    let row = queued_task.trainer_task(queued_task.task_id, &queued_task.spec);
    queued_task
        .store
        .insert_local_task(
            &row,
            &queued_task.spec,
            queued_task.authority,
            PathBuf::from("/bin/echo").into(),
        )
        .unwrap();
    let evidence = queued_task.evidence();
    assert!(matches!(
        queued_task.bind(evidence),
        Err(TrainerAttemptAssociationStoreError::TaskNotRunning { state, .. })
            if state == "queued"
    ));

    let mut missing_identity = TrainerAssociationFixture::new();
    let row = missing_identity.trainer_task(missing_identity.task_id, &missing_identity.spec);
    missing_identity.store.insert_task(&row).unwrap();
    missing_identity
        .store
        .cas_status(
            missing_identity.task_id,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    let evidence = missing_identity.evidence();
    assert!(matches!(
        missing_identity.bind(evidence),
        Err(TrainerAttemptAssociationStoreError::IdentityMissing { task_id })
            if task_id == missing_identity.task_id
    ));

    let mut stopped_identity = TrainerAssociationFixture::new();
    stopped_identity.insert_accepted_running_task();
    stopped_identity
        .store
        .update_execution_state(stopped_identity.task_id, ProcessStatus::Queued)
        .unwrap();
    let evidence = stopped_identity.evidence();
    assert!(matches!(
        stopped_identity.bind(evidence),
        Err(TrainerAttemptAssociationStoreError::IdentityNotRunning { task_id })
            if task_id == stopped_identity.task_id
    ));

    let mut changed_row = TrainerAssociationFixture::new();
    changed_row.insert_accepted_running_task();
    changed_row
        .store
        .conn
        .execute(
            "UPDATE tasks SET cwd='/different' WHERE id=?1",
            [changed_row.task_id.to_string()],
        )
        .unwrap();
    let evidence = changed_row.evidence();
    assert!(matches!(
        changed_row.bind(evidence),
        Err(TrainerAttemptAssociationStoreError::NormalizedSpecMismatch { task_id })
            if task_id == changed_row.task_id
    ));
}

#[test]
fn trainer_attempt_association_checks_the_executor_authority_not_request_origin() {
    let mut fixture = TrainerAssociationFixture::new();
    let row = fixture.trainer_task(fixture.task_id, &fixture.spec);
    let origin = machine_other_than(fixture.authority);
    fixture
        .store
        .insert_remote_task(&row, &fixture.spec, origin, fixture.authority)
        .unwrap();
    fixture
        .store
        .cas_status(
            fixture.task_id,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    let evidence = fixture.evidence();
    assert!(fixture.bind(evidence).is_ok());

    let mut wrong_executor = TrainerAssociationFixture::new();
    let row = wrong_executor.trainer_task(wrong_executor.task_id, &wrong_executor.spec);
    let wrong_executor_machine = machine_other_than(wrong_executor.authority);
    wrong_executor
        .store
        .insert_remote_task(
            &row,
            &wrong_executor.spec,
            wrong_executor.authority,
            wrong_executor_machine,
        )
        .unwrap();
    wrong_executor
        .store
        .cas_status(
            wrong_executor.task_id,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    let evidence = wrong_executor.evidence();
    assert!(matches!(
        wrong_executor.bind(evidence),
        Err(TrainerAttemptAssociationStoreError::IdentityMismatch { task_id })
            if task_id == wrong_executor.task_id
    ));
}

#[test]
fn trainer_attempt_association_rejects_duplicate_task_across_resources() {
    let mut fixture = TrainerAssociationFixture::new();
    fixture.insert_accepted_running_task();
    let evidence = fixture.evidence();
    fixture.bind(evidence.clone()).unwrap();

    let mut second_resource = resource(fixture.authority);
    second_resource.registered_background_task = Some(fixture.task_id);
    fixture
        .store
        .register_resource(fixture.authority, &second_resource)
        .unwrap();
    assert!(matches!(
        fixture.store.bind_trainer_attempt_association(
            fixture.authority,
            second_resource.id,
            fixture.task_id,
            evidence,
        ),
        Err(TrainerAttemptAssociationStoreError::TaskAlreadyAssociated { task_id })
            if task_id == fixture.task_id
    ));
}

#[test]
fn trainer_attempt_association_accepts_only_matching_awaiting_release_loan() {
    let mut awaiting = TrainerAssociationFixture::new();
    awaiting.insert_accepted_running_task();
    awaiting.add_loan(awaiting_release_state(awaiting.task_id));
    let evidence = awaiting.evidence();
    assert!(awaiting.bind(evidence).is_ok());

    let mut mismatched = TrainerAssociationFixture::new();
    mismatched.insert_accepted_running_task();
    mismatched.add_loan(awaiting_release_state(TaskId::new()));
    let evidence = mismatched.evidence();
    assert!(matches!(
        mismatched.bind(evidence),
        Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict { resource_id })
            if resource_id == mismatched.resource.id
    ));

    let mut serving = TrainerAssociationFixture::new();
    serving.insert_accepted_running_task();
    serving.add_loan(LoanState::Active {
        phase: LoanPhase::Serving {
            return_context: ReturnContext::Stopped {
                task_id: serving.task_id,
                checkpoint_ref: "checkpoint".into(),
                recovery_ref: "recovery".into(),
            },
            current_request_id: RequestId::new(),
            release_provenance: crate::store::unreceipted_release_provenance(),
        },
    });
    let evidence = serving.evidence();
    assert!(matches!(
        serving.bind(evidence),
        Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict { .. })
    ));

    let mut returning = TrainerAssociationFixture::new();
    returning.insert_accepted_running_task();
    returning.add_loan(LoanState::Active {
        phase: LoanPhase::AwaitingReturn {
            action_id: ActionId::new(),
            return_context: ReturnContext::Idle,
        },
    });
    let evidence = returning.evidence();
    assert!(matches!(
        returning.bind(evidence),
        Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict { .. })
    ));

    let mut restoring = TrainerAssociationFixture::new();
    restoring.insert_accepted_running_task();
    restoring.add_loan(LoanState::Active {
        phase: LoanPhase::Restoring {
            action_id: ActionId::new(),
            return_context: ReturnContext::Idle,
            resume_task_id: TaskId::new(),
        },
    });
    let evidence = restoring.evidence();
    assert!(matches!(
        restoring.bind(evidence),
        Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict { .. })
    ));
}
