//! Release transaction recheck tests

use super::fixtures::{
    commit_stopped_release_decision, publish_completed_result, release_completion_fixture,
    saved_trainer_association_json,
};
use crate::domain::{ExitReason, ProcessGroupExitEvidence, TaskId};
use crate::resource::ownership_lock::{
    OwnershipLockIdentityMismatchReason, OwnershipLockProbeError,
};
use crate::resource::store::{
    CompleteReleaseError,
    complete_release_for_authority as persist_release_completion_for_authority,
};
use crate::resource::{ActionId, LoanPhase, LoanState, ResourceRevision};
use rusqlite::params;
use serde_json::json;
use std::fs;

#[test]
fn release_transaction_rechecks_task_state_association_and_identity() {
    for changed_record in ["task", "task_command", "association", "identity"] {
        let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut fixture,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let proof = fixture
            .store
            .build_verified_release_proof(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap();

        let expected_error = match changed_record {
            "task" => {
                fixture
                    .store
                    .conn
                    .execute(
                        "UPDATE tasks SET status = 'failed', exit_reason = ?1
                         WHERE id = ?2",
                        params![
                            serde_json::to_string(&ExitReason::Exit { code: 1 }).unwrap(),
                            fixture.task_id.to_string(),
                        ],
                    )
                    .unwrap();
                "task"
            }
            "task_command" => {
                fixture
                    .store
                    .conn
                    .execute(
                        "UPDATE tasks SET cwd = ?1 WHERE id = ?2",
                        params!["/changed/task/command", fixture.task_id.to_string()],
                    )
                    .unwrap();
                "task_command"
            }
            "association" => {
                let mut association: serde_json::Value = serde_json::from_str(
                    &saved_trainer_association_json(&fixture.store, fixture.task_id),
                )
                .unwrap();
                association["normalized_spec_sha256"] = json!("1".repeat(64));
                fixture
                    .store
                    .conn
                    .execute(
                        "UPDATE trainer_attempt_associations SET association_json = ?1
                         WHERE task_id = ?2",
                        params![
                            serde_json::to_string(&association).unwrap(),
                            fixture.task_id.to_string(),
                        ],
                    )
                    .unwrap();
                "association"
            }
            "identity" => {
                let mut identity: serde_json::Value = fixture
                    .store
                    .conn
                    .query_row(
                        "SELECT identity_json FROM executor_identities WHERE task_id = ?1",
                        [fixture.task_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .map(|json| serde_json::from_str(&json).unwrap())
                    .unwrap();
                identity["state"] = json!("failed");
                fixture
                    .store
                    .conn
                    .execute(
                        "UPDATE executor_identities SET identity_json = ?1 WHERE task_id = ?2",
                        params![
                            serde_json::to_string(&identity).unwrap(),
                            fixture.task_id.to_string()
                        ],
                    )
                    .unwrap();
                "identity"
            }
            _ => unreachable!(),
        };

        let result = persist_release_completion_for_authority(&mut fixture.store.conn, proof);
        match expected_error {
            "task" => assert!(matches!(
                result,
                Err(CompleteReleaseError::TaskStateChanged { task_id })
                    if task_id == fixture.task_id
            )),
            "task_command" => assert!(matches!(
                result,
                Err(CompleteReleaseError::TaskCommandChanged { task_id })
                    if task_id == fixture.task_id
            )),
            "association" => assert!(matches!(
                result,
                Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
                    if task_id == fixture.task_id
            )),
            "identity" => assert!(matches!(
                result,
                Err(CompleteReleaseError::TrainerIdentityChanged { task_id })
                    if task_id == fixture.task_id
            )),
            _ => unreachable!(),
        }
        let snapshot = fixture
            .store
            .resource_snapshots_for_authority(fixture.authority)
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.resource.id == fixture.resource.id)
            .unwrap();
        assert_eq!(snapshot.resource.state_revision, ResourceRevision::new(1));
        assert!(matches!(
            snapshot.loan.unwrap().state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease { .. }
            }
        ));
    }
}

#[test]
fn release_transaction_rechecks_action_revision_and_current_task() {
    for changed_binding in ["action", "revision", "task"] {
        let (mut fixture, binding, _, action_id, revision, _) = release_completion_fixture();
        publish_completed_result(
            &mut fixture,
            &binding,
            ProcessGroupExitEvidence::ConfirmedExited,
        );
        let proof = fixture
            .store
            .build_verified_release_proof(
                fixture.authority,
                fixture.resource.id,
                action_id,
                revision,
            )
            .unwrap();

        match changed_binding {
            "action" => {
                let mut loan_state: serde_json::Value = fixture
                    .store
                    .conn
                    .query_row(
                        "SELECT state_json FROM loans WHERE resource_id = ?1",
                        [fixture.resource.id.as_uuid().to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .map(|json| serde_json::from_str(&json).unwrap())
                    .unwrap();
                loan_state["phase"]["action_id"] = json!(ActionId::new());
                fixture
                    .store
                    .conn
                    .execute(
                        "UPDATE loans SET state_json = ?1 WHERE resource_id = ?2",
                        params![
                            serde_json::to_string(&loan_state).unwrap(),
                            fixture.resource.id.as_uuid().to_string(),
                        ],
                    )
                    .unwrap();
            }
            "revision" => {
                fixture
                    .store
                    .conn
                    .execute(
                        "UPDATE resources SET state_revision = 9 WHERE id = ?1",
                        [fixture.resource.id.as_uuid().to_string()],
                    )
                    .unwrap();
            }
            "task" => {
                fixture
                    .store
                    .conn
                    .execute(
                        "UPDATE resources SET registered_background_task = ?1 WHERE id = ?2",
                        params![
                            TaskId::new().to_string(),
                            fixture.resource.id.as_uuid().to_string(),
                        ],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }

        let result = persist_release_completion_for_authority(&mut fixture.store.conn, proof);
        match changed_binding {
            "action" => assert!(matches!(
                result,
                Err(CompleteReleaseError::NotAwaitingRelease { .. })
            )),
            "revision" => assert!(matches!(
                result,
                Err(CompleteReleaseError::StaleRevision {
                    expected,
                    actual,
                }) if expected == revision && actual == ResourceRevision::new(9)
            )),
            "task" => assert!(matches!(
                result,
                Err(CompleteReleaseError::TrainerAssociationMismatch { task_id })
                    if task_id == fixture.task_id
            )),
            _ => unreachable!(),
        }
    }
}

#[test]
fn release_transaction_rechecks_result_and_lock_path_before_commit() {
    let (mut changed_result, binding, _, action_id, revision, _) = release_completion_fixture();
    publish_completed_result(
        &mut changed_result,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let proof = changed_result
        .store
        .build_verified_release_proof(
            changed_result.authority,
            changed_result.resource.id,
            action_id,
            revision,
        )
        .unwrap();
    fs::write(
        changed_result
            .runtime_root
            .join("published")
            .join(format!("result-{}", binding.attempt_id))
            .join("checkpoint.bin"),
        b"changed result bytes",
    )
    .unwrap();
    assert!(matches!(
        persist_release_completion_for_authority(&mut changed_result.store.conn, proof),
        Err(CompleteReleaseError::Watcher(_))
    ));

    let (mut changed_lock, binding, _, action_id, revision, _) = release_completion_fixture();
    publish_completed_result(
        &mut changed_lock,
        &binding,
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let proof = changed_lock
        .store
        .build_verified_release_proof(
            changed_lock.authority,
            changed_lock.resource.id,
            action_id,
            revision,
        )
        .unwrap();
    let lock_path = changed_lock.runtime_root.join(".segment.lock");
    let saved_path = changed_lock.runtime_root.join(".segment.lock.saved");
    fs::rename(&lock_path, &saved_path).unwrap();
    fs::write(&lock_path, b"replaced lock").unwrap();
    assert!(matches!(
        persist_release_completion_for_authority(&mut changed_lock.store.conn, proof),
        Err(CompleteReleaseError::OwnershipLock(
            OwnershipLockProbeError::IdentityMismatch {
                reason: OwnershipLockIdentityMismatchReason::DifferentFile,
                ..
            }
        ))
    ));
}

#[test]
fn stopped_release_transaction_rechecks_saved_marker_checkpoint_and_lock() {
    let (mut changed_marker, _, _, action_id, revision, _) = release_completion_fixture();
    let _ = commit_stopped_release_decision(&mut changed_marker, action_id, revision);
    changed_marker
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
    let proof = changed_marker
        .store
        .build_verified_release_proof(
            changed_marker.authority,
            changed_marker.resource.id,
            action_id,
            revision,
        )
        .unwrap();
    changed_marker
        .store
        .conn
        .execute(
            "UPDATE tasks SET cancel_requested_at = ?1 WHERE id = ?2",
            params![
                "2026-01-01T00:00:00.000000000Z",
                changed_marker.task_id.to_string()
            ],
        )
        .unwrap();
    assert!(matches!(
        persist_release_completion_for_authority(&mut changed_marker.store.conn, proof),
        Err(CompleteReleaseError::TrainerCancellationMarkerChanged { task_id })
            if task_id == changed_marker.task_id
    ));

    let (mut changed_checkpoint, _, _, action_id, revision, _) = release_completion_fixture();
    let (decision, _) =
        commit_stopped_release_decision(&mut changed_checkpoint, action_id, revision);
    changed_checkpoint
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
    let proof = changed_checkpoint
        .store
        .build_verified_release_proof(
            changed_checkpoint.authority,
            changed_checkpoint.resource.id,
            action_id,
            revision,
        )
        .unwrap();
    fs::write(
        decision.selected_checkpoint.path.join("checkpoint.bin"),
        b"changed after proof construction",
    )
    .unwrap();
    assert!(matches!(
        persist_release_completion_for_authority(&mut changed_checkpoint.store.conn, proof),
        Err(CompleteReleaseError::StoppedCheckpointChanged { task_id })
            if task_id == changed_checkpoint.task_id
    ));

    let (mut changed_lock, _, _, action_id, revision, _) = release_completion_fixture();
    let _ = commit_stopped_release_decision(&mut changed_lock, action_id, revision);
    changed_lock
        .finish_registered_task_cancelled_with_evidence(ProcessGroupExitEvidence::ConfirmedExited);
    let proof = changed_lock
        .store
        .build_verified_release_proof(
            changed_lock.authority,
            changed_lock.resource.id,
            action_id,
            revision,
        )
        .unwrap();
    let lock_path = changed_lock.runtime_root.join(".segment.lock");
    let saved_path = changed_lock.runtime_root.join(".segment.lock.saved");
    fs::rename(&lock_path, &saved_path).unwrap();
    fs::write(&lock_path, b"replaced lock").unwrap();
    assert!(matches!(
        persist_release_completion_for_authority(&mut changed_lock.store.conn, proof),
        Err(CompleteReleaseError::OwnershipLock(
            OwnershipLockProbeError::IdentityMismatch {
                reason: OwnershipLockIdentityMismatchReason::DifferentFile,
                ..
            }
        ))
    ));
}
