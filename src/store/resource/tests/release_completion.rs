//! Completed and stopped release proof tests

use super::fixtures::{
    TrainerAssociationFixture, assert_ended_release, commit_stopped_release_decision,
    fake_resource_task_spec, machine_other_than, publish_completed_result,
    release_completion_fixture, release_completion_fixture_with, saved_trainer_association_json,
};
use crate::domain::{ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId};
use crate::resource::ownership_lock::{
    OwnershipLockIdentityMismatchReason, OwnershipLockProbeError,
};
use crate::resource::store::{
    CompleteReleaseError, ReleaseCompletionResult, ResourceTaskAcceptance,
    ResourceTaskAcceptanceInput,
};
use crate::resource::trainer_publication::find_completed_result;
use crate::resource::{
    ActionId, LoanPhase, LoanState, ResourceRequestState, ResourceRevision, ReturnContext,
    ServingReleaseProvenance,
};
use crate::submission::RequestId;
use rusqlite::params;
use serde_json::json;
use std::fs;

#[test]
fn completed_release_requires_and_uses_the_authority_verified_result() {
    let (mut fixture, binding, request_id, action_id, revision, loan_id) =
        release_completion_fixture();
    publish_completed_result(
        &mut fixture,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let publication = find_completed_result(&fixture.runtime_root, &binding)
        .unwrap()
        .unwrap();

    let result = fixture
        .store
        .complete_release_for_authority(fixture.authority, fixture.resource.id, action_id, revision)
        .unwrap();
    let ReleaseCompletionResult::Assigned {
        loan,
        request,
        state_revision,
    } = result
    else {
        panic!("queued work must be assigned after a verified completed result");
    };

    assert_eq!(loan.id, loan_id);
    assert_eq!(request.request_id, request_id);
    assert_eq!(state_revision, ResourceRevision::new(2));
    assert!(matches!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: ReturnContext::AlreadyCompleted {
                    task_id,
                    result_ref,
                },
                ..
            }
        } if task_id == fixture.task_id
            && result_ref == format!(
                "{}#sha256={}",
                publication.publication_path.display(),
                publication.publication_sha256
            )
    ));
    assert!(matches!(
        fixture.store.resource_requests(fixture.authority, fixture.resource.id).unwrap()[0]
            .state,
        ResourceRequestState::Assigned { loan_id: assigned } if assigned == loan_id
    ));
}

#[test]
fn exact_release_retry_does_not_need_removed_artifacts() {
    let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
    publish_completed_result(
        &mut fixture,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let first = fixture
        .store
        .complete_release_for_authority(fixture.authority, fixture.resource.id, action_id, revision)
        .unwrap();
    fs::remove_dir_all(&fixture.runtime_root).unwrap();

    let retry = fixture
        .store
        .complete_release_for_authority(fixture.authority, fixture.resource.id, action_id, revision)
        .unwrap();

    assert_eq!(
        serde_json::to_value(retry).unwrap(),
        serde_json::to_value(first).unwrap()
    );
}

#[test]
fn authority_proves_a_stopped_trainer_from_its_saved_checkpoint_decision() {
    let (mut fixture, _, request_id, action_id, revision, loan_id) = release_completion_fixture();
    let (decision, cancellation) =
        commit_stopped_release_decision(&mut fixture, action_id, revision);
    fixture
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);

    let result = fixture
        .store
        .complete_release_for_authority(fixture.authority, fixture.resource.id, action_id, revision)
        .unwrap();
    let ReleaseCompletionResult::Assigned {
        loan,
        request,
        state_revision,
    } = result
    else {
        panic!("the queued request must be assigned after a proved stop");
    };
    let checkpoint = decision.selected_checkpoint;
    assert_eq!(request.request_id, request_id);
    assert_eq!(loan.id, loan_id);
    assert_eq!(state_revision, ResourceRevision::new(2));
    assert!(matches!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: ReturnContext::Stopped {
                    task_id,
                    checkpoint_ref,
                    recovery_ref,
                },
                release_provenance: ServingReleaseProvenance::StoppedTrainerCheckpoint {
                    action_id: saved_action,
                    task_id: saved_task,
                    generation_id,
                    record_sha256,
                    inventory_sha256,
                },
                ..
            }
        } if task_id == fixture.task_id
            && saved_action == action_id
            && saved_task == fixture.task_id
            && checkpoint_ref == format!(
                "{}#sha256={}", checkpoint.path.display(), checkpoint.record_sha256
            )
            && recovery_ref == checkpoint.generation_id
            && generation_id == checkpoint.generation_id
            && record_sha256 == checkpoint.record_sha256
            && inventory_sha256 == checkpoint.inventory_sha256
    ));
    assert_eq!(
        fixture
            .store
            .require_task(fixture.task_id)
            .unwrap()
            .cancel_requested_at,
        Some(cancellation.cancel_requested_at)
    );
}

#[test]
fn stopped_release_without_queued_work_creates_a_verified_return_context() {
    let (mut fixture, _, request_id, action_id, revision, _) = release_completion_fixture();
    let queued = fixture
        .store
        .resource_requests(fixture.authority, fixture.resource.id)
        .unwrap()
        .into_iter()
        .find(|request| request.request_id == request_id)
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .cancel_resource_request_before_activation(
                fixture.authority,
                queued.request_id,
                queued.task_id,
                fixture.resource.id,
                queued.origin_machine,
            )
            .unwrap(),
        crate::resource::store::QueueCancellationResult::Request(_)
    ));
    let (decision, _) = commit_stopped_release_decision(&mut fixture, action_id, revision);
    fixture
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);

    let result = fixture
        .store
        .complete_release_for_authority(fixture.authority, fixture.resource.id, action_id, revision)
        .unwrap();
    let ReleaseCompletionResult::ReturnRequired { loan, .. } = result else {
        panic!("the empty queue must return the verified stopped context");
    };
    let checkpoint = decision.selected_checkpoint;
    assert!(matches!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingReturn {
                return_context: ReturnContext::Stopped {
                    task_id,
                    checkpoint_ref,
                    recovery_ref,
                },
                ..
            }
        } if task_id == fixture.task_id
            && checkpoint_ref == format!(
                "{}#sha256={}", checkpoint.path.display(), checkpoint.record_sha256
            )
            && recovery_ref == checkpoint.generation_id
    ));
}

#[test]
fn exact_stopped_release_retry_uses_its_receipt_after_checkpoint_removal() {
    let (mut fixture, _, _, action_id, revision, _) = release_completion_fixture();
    let (decision, _) = commit_stopped_release_decision(&mut fixture, action_id, revision);
    fixture
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
    let first = fixture
        .store
        .complete_release_for_authority(fixture.authority, fixture.resource.id, action_id, revision)
        .unwrap();
    fs::remove_dir_all(&decision.selected_checkpoint.path).unwrap();

    let retry = fixture
        .store
        .complete_release_for_authority(fixture.authority, fixture.resource.id, action_id, revision)
        .unwrap();

    assert_eq!(
        serde_json::to_value(retry).unwrap(),
        serde_json::to_value(first).unwrap()
    );
}

#[test]
fn stopped_release_rejects_missing_or_changed_selected_checkpoint() {
    for changed in ["missing", "contents"] {
        let (mut fixture, _, _, action_id, revision, _) = release_completion_fixture();
        let (decision, _) = commit_stopped_release_decision(&mut fixture, action_id, revision);
        fixture.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        if changed == "missing" {
            fs::remove_dir_all(&decision.selected_checkpoint.path).unwrap();
        } else {
            fs::write(
                decision.selected_checkpoint.path.join("checkpoint.bin"),
                b"changed checkpoint contents",
            )
            .unwrap();
        }

        assert!(matches!(
            fixture.store.complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::StoppedCheckpointChanged { task_id })
                if task_id == fixture.task_id
        ));
    }
}

#[test]
fn completed_release_rejects_missing_or_mismatched_association() {
    let (mut missing, binding, _, action_id, revision, _) = release_completion_fixture();
    publish_completed_result(
        &mut missing,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    missing
        .store
        .conn
        .execute(
            "DELETE FROM trainer_attempt_associations WHERE task_id = ?1",
            [missing.task_id.to_string()],
        )
        .unwrap();
    assert!(matches!(
        missing.store.complete_release_for_authority(
            missing.authority,
            missing.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::TrainerAssociationMissing { task_id })
            if task_id == missing.task_id
    ));

    let (mut mismatched, binding, _, action_id, revision, _) = release_completion_fixture();
    publish_completed_result(
        &mut mismatched,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let mut association: serde_json::Value = serde_json::from_str(&saved_trainer_association_json(
        &mismatched.store,
        mismatched.task_id,
    ))
    .unwrap();
    association["normalized_spec_sha256"] = json!("0".repeat(64));
    mismatched
        .store
        .conn
        .execute(
            "UPDATE trainer_attempt_associations SET association_json = ?1
             WHERE task_id = ?2",
            params![
                serde_json::to_string(&association).unwrap(),
                mismatched.task_id.to_string()
            ],
        )
        .unwrap();
    assert!(matches!(
        mismatched.store.complete_release_for_authority(
            mismatched.authority,
            mismatched.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
            if task_id == mismatched.task_id
    ));
}

#[test]
fn completed_release_rejects_unconfirmed_worker_exit() {
    let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
    publish_completed_result(
        &mut fixture,
        &binding,
        ProcessGroupExitEvidence::Unconfirmed,
    );

    assert!(matches!(
        fixture.store.complete_release_for_authority(
            fixture.authority,
            fixture.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::WorkerExitUnconfirmed { task_id })
            if task_id == fixture.task_id
    ));
}

#[test]
fn completed_release_rejects_a_still_held_ownership_lock() {
    let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
    publish_completed_result(
        &mut fixture,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let lock_holder = fixture.hold_saved_lock();

    assert!(matches!(
        fixture.store.complete_release_for_authority(
            fixture.authority,
            fixture.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::OwnershipLockStillHeld { task_id })
            if task_id == fixture.task_id
    ));
    drop(lock_holder);
}

#[test]
fn completed_release_rejects_missing_replaced_and_symlinked_locks() {
    for (replacement, reason) in [
        ("missing", OwnershipLockIdentityMismatchReason::Missing),
        (
            "replaced",
            OwnershipLockIdentityMismatchReason::DifferentFile,
        ),
        ("symlink", OwnershipLockIdentityMismatchReason::SymbolicLink),
    ] {
        let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut fixture,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let lock_path = fixture.runtime_root.join(".segment.lock");
        let saved_path = fixture.runtime_root.join(".segment.lock.saved");
        if replacement == "missing" {
            fs::remove_file(&lock_path).unwrap();
        } else {
            fs::rename(&lock_path, &saved_path).unwrap();
            if replacement == "replaced" {
                fs::write(&lock_path, b"new lock file").unwrap();
            } else {
                std::os::unix::fs::symlink(&saved_path, &lock_path).unwrap();
            }
        }

        let result = fixture.store.complete_release_for_authority(
            fixture.authority,
            fixture.resource.id,
            action_id,
            revision,
        );
        assert!(matches!(
            result,
            Err(CompleteReleaseError::OwnershipLock(
                OwnershipLockProbeError::IdentityMismatch {
                    reason: found,
                    ..
                }
            )) if found == reason
        ));
    }
}

#[test]
fn completed_release_rejects_wrong_action_or_current_task() {
    let (mut fixture, binding, _, action_id, revision, loan_id) = release_completion_fixture();
    publish_completed_result(
        &mut fixture,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let wrong_action = ActionId::new();
    assert!(matches!(
        fixture.store.complete_release_for_authority(
            fixture.authority,
            fixture.resource.id,
            wrong_action,
            revision,
        ),
        Err(CompleteReleaseError::NotAwaitingRelease { loan_id: found, action_id })
            if found == loan_id && action_id == wrong_action
    ));

    let different_task = TaskId::new();
    fixture
        .store
        .conn
        .execute(
            "UPDATE resources SET registered_background_task = ?1 WHERE id = ?2",
            params![
                different_task.to_string(),
                fixture.resource.id.as_uuid().to_string()
            ],
        )
        .unwrap();
    assert!(matches!(
        fixture.store.complete_release_for_authority(
            fixture.authority,
            fixture.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
            if task_id == fixture.task_id
    ));
}

#[test]
fn zero_exit_without_result_is_ended_and_a_mismatched_result_fails_closed() {
    let (mut missing, _, _, action_id, revision, _) = release_completion_fixture();
    missing.finish_registered_task_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
    let result = missing
        .store
        .complete_release_for_authority(missing.authority, missing.resource.id, action_id, revision)
        .unwrap();
    assert_ended_release(
        &result,
        missing.task_id,
        action_id,
        &ExitReason::Exit { code: 0 },
    );

    let (mut mismatched, binding, _, action_id, revision, _) = release_completion_fixture();
    publish_completed_result(
        &mut mismatched,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    fs::write(
        mismatched
            .runtime_root
            .join("attempts")
            .join(&binding.attempt_id)
            .join("terminal.json"),
        b"{}",
    )
    .unwrap();
    assert!(matches!(
        mismatched.store.complete_release_for_authority(
            mismatched.authority,
            mismatched.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::Watcher(_))
    ));
}

#[test]
fn nonzero_exit_releases_only_as_an_ended_run_despite_saved_artifacts() {
    let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
    crate::resource::trainer_publication::tests::write_completed_result_for_test(
        &fixture.runtime_root,
        &binding,
    );
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &fixture.runtime_root,
        &binding,
        "checkpoint-after-start",
        4,
    );
    fixture
        .store
        .cas_exit_with_evidence(
            fixture.task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 3 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
    fixture
        .store
        .update_execution_state(fixture.task_id, ProcessStatus::Failed)
        .unwrap();

    // neither the result file nor the checkpoint makes a failed run resumable
    let result = fixture
        .store
        .complete_release_for_authority(fixture.authority, fixture.resource.id, action_id, revision)
        .unwrap();
    assert_ended_release(
        &result,
        fixture.task_id,
        action_id,
        &ExitReason::Exit { code: 3 },
    );
}

#[test]
fn stopped_release_requires_the_exact_cancellation_action_task_and_association() {
    let (mut wrong_action, _, _, action_id, revision, _) = release_completion_fixture();
    let (_, _) = commit_stopped_release_decision(&mut wrong_action, action_id, revision);
    wrong_action
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
    let wrong_action_id = ActionId::new();
    assert!(matches!(
        wrong_action.store.complete_release_for_authority(
            wrong_action.authority,
            wrong_action.resource.id,
            wrong_action_id,
            revision,
        ),
        Err(CompleteReleaseError::NotAwaitingRelease { action_id, .. })
            if action_id == wrong_action_id
    ));

    let (mut wrong_task, _, _, action_id, revision, _) = release_completion_fixture();
    let _ = commit_stopped_release_decision(&mut wrong_task, action_id, revision);
    wrong_task
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
    let replacement_task = TaskId::new();
    wrong_task
        .store
        .conn
        .execute(
            "UPDATE resources SET registered_background_task = ?1 WHERE id = ?2",
            params![
                replacement_task.to_string(),
                wrong_task.resource.id.as_uuid().to_string()
            ],
        )
        .unwrap();
    assert!(matches!(
        wrong_task.store.complete_release_for_authority(
            wrong_task.authority,
            wrong_task.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
            if task_id == wrong_task.task_id
    ));

    let (mut wrong_association, _, _, action_id, revision, _) = release_completion_fixture();
    let _ = commit_stopped_release_decision(&mut wrong_association, action_id, revision);
    wrong_association
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
    let mut association: serde_json::Value = serde_json::from_str(&saved_trainer_association_json(
        &wrong_association.store,
        wrong_association.task_id,
    ))
    .unwrap();
    association["request_sha256"] = json!("63".repeat(32));
    wrong_association
        .store
        .conn
        .execute(
            "UPDATE trainer_attempt_associations SET association_json = ?1
             WHERE task_id = ?2",
            params![
                serde_json::to_string(&association).unwrap(),
                wrong_association.task_id.to_string(),
            ],
        )
        .unwrap();
    assert!(matches!(
        wrong_association.store.complete_release_for_authority(
            wrong_association.authority,
            wrong_association.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
            if task_id == wrong_association.task_id
    ));
}

#[test]
fn generic_cancellation_and_checkpoint_release_only_as_an_ended_run() {
    let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &fixture.runtime_root,
        &binding,
        "generation-without-stop-decision",
        81,
    );
    assert!(matches!(
        fixture.store.request_cancel(fixture.task_id).unwrap(),
        crate::store::CancelResult::SignalWorker(_)
    ));
    fixture
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);

    // a cancellation that no stop decision committed names no resumable checkpoint
    let result = fixture
        .store
        .complete_release_for_authority(fixture.authority, fixture.resource.id, action_id, revision)
        .unwrap();
    assert_ended_release(&result, fixture.task_id, action_id, &ExitReason::Cancelled);
}

#[test]
fn stopped_release_rejects_nonterminal_and_lost_trainers_and_ends_a_failed_stop() {
    let (mut running, _, _, action_id, revision, _) = release_completion_fixture();
    assert!(matches!(
        running.store.complete_release_for_authority(
            running.authority,
            running.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::BackgroundTaskNotTerminal { task_id, .. })
            if task_id == running.task_id
    ));

    let (mut lost, _, _, action_id, revision, _) = release_completion_fixture();
    lost.store
        .conn
        .execute(
            "UPDATE tasks SET status = 'lost', exit_reason = NULL WHERE id = ?1",
            [lost.task_id.to_string()],
        )
        .unwrap();
    assert!(matches!(
        lost.store.complete_release_for_authority(
            lost.authority,
            lost.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::BackgroundTaskLost { task_id })
            if task_id == lost.task_id
    ));

    let (mut failed, _, _, action_id, revision, _) = release_completion_fixture();
    let _ = commit_stopped_release_decision(&mut failed, action_id, revision);
    failed
        .store
        .cas_exit_with_evidence(
            failed.task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 9 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
    failed
        .store
        .update_execution_state(failed.task_id, ProcessStatus::Failed)
        .unwrap();
    // a saved stop decision does not make a failed run resumable
    let result = failed
        .store
        .complete_release_for_authority(failed.authority, failed.resource.id, action_id, revision)
        .unwrap();
    assert_ended_release(
        &result,
        failed.task_id,
        action_id,
        &ExitReason::Exit { code: 9 },
    );
}

#[test]
fn stopped_release_requires_confirmed_worker_exit() {
    let (mut fixture, _, _, action_id, revision, _) = release_completion_fixture();
    let _ = commit_stopped_release_decision(&mut fixture, action_id, revision);
    fixture.finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::Unconfirmed);

    assert!(matches!(
        fixture.store.complete_release_for_authority(
            fixture.authority,
            fixture.resource.id,
            action_id,
            revision,
        ),
        Err(CompleteReleaseError::WorkerExitUnconfirmed { task_id })
            if task_id == fixture.task_id
    ));
}

#[test]
fn stopped_release_requires_the_saved_lock_to_be_exact_and_exclusively_free() {
    for replacement in ["held", "missing", "replaced", "symlink"] {
        let (mut fixture, _, _, action_id, revision, _) = release_completion_fixture();
        let _ = commit_stopped_release_decision(&mut fixture, action_id, revision);
        fixture.finish_registered_task_cancelled_with_evidence(
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let lock_path = fixture.runtime_root.join(".segment.lock");
        let saved_path = fixture.runtime_root.join(".segment.lock.saved");
        if replacement == "held" {
            let lock_holder = fixture.hold_saved_lock();
            assert!(matches!(
                fixture.store.complete_release_for_authority(
                    fixture.authority,
                    fixture.resource.id,
                    action_id,
                    revision,
                ),
                Err(CompleteReleaseError::OwnershipLockStillHeld { task_id })
                    if task_id == fixture.task_id
            ));
            drop(lock_holder);
            continue;
        }
        if replacement == "missing" {
            fs::remove_file(&lock_path).unwrap();
        } else {
            fs::rename(&lock_path, &saved_path).unwrap();
            if replacement == "replaced" {
                fs::write(&lock_path, b"new lock file").unwrap();
            } else {
                std::os::unix::fs::symlink(&saved_path, &lock_path).unwrap();
            }
        }

        let expected_reason = match replacement {
            "missing" => OwnershipLockIdentityMismatchReason::Missing,
            "replaced" => OwnershipLockIdentityMismatchReason::DifferentFile,
            "symlink" => OwnershipLockIdentityMismatchReason::SymbolicLink,
            _ => unreachable!(),
        };
        assert!(matches!(
            fixture.store.complete_release_for_authority(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            ),
            Err(CompleteReleaseError::OwnershipLock(
                OwnershipLockProbeError::IdentityMismatch {
                    reason,
                    ..
                }
            )) if reason == expected_reason
        ));
    }
}

#[test]
fn proven_stopped_release_accepts_the_next_fifo_request_after_prelaunch_cancel() {
    let fixture = TrainerAssociationFixture::new();
    let authority = fixture.authority;
    let marker = fixture.home.join("assigned-command-marker");
    let request_spec = fake_resource_task_spec(&fixture.home, &marker);
    let first_origin = machine_other_than(authority);
    let (mut fixture, _, first_request_id, action_id, revision, loan_id) =
        release_completion_fixture_with(fixture, request_spec.clone(), first_origin);
    let second_request = fixture
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
    let _ = commit_stopped_release_decision(&mut fixture, action_id, revision);
    fixture
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
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
    fixture
        .store
        .cancel_resource_request_before_activation(
            authority,
            first_request.request_id,
            first_request.task_id,
            fixture.resource.id,
            first_request.origin_machine,
        )
        .unwrap();
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
                release_provenance: ServingReleaseProvenance::StoppedTrainerCheckpoint {
                    action_id: saved_action,
                    ..
                },
                ..
            }
        }) if *current_request_id == second_request.request_id
            && *saved_action == action_id
    ));

    let input = ResourceTaskAcceptanceInput {
        authority_machine: authority,
        resource_id: fixture.resource.id,
        request_id: second_request.request_id,
        task_id: second_request.task_id,
        acceptance_sequence: second_request.acceptance_sequence,
        loan_id,
        expected_state_revision: snapshot.resource.state_revision,
        command_spec: second_request.spec().clone(),
        executor_env: TaskEnv::capture(),
    };
    assert_eq!(
        fixture.store.accept_assigned_resource_task(input).unwrap(),
        ResourceTaskAcceptance::Inserted {
            task: second_request.task_id,
        }
    );
    assert_eq!(
        fixture
            .store
            .get_task(second_request.task_id)
            .unwrap()
            .unwrap()
            .status(),
        ProcessStatus::Queued
    );
}
