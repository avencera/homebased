//! Release checkpoint baseline, stop decision, and checkpoint cancellation tests

use super::fixtures::{
    accept_watcher, checkpoint_decision_for_cancellation, open_release_for_test,
    release_watcher_intent, saved_release_association, start_release_watcher_for_test,
    watcher_spec, watcher_task_and_callback,
};
use crate::domain::{ExitReason, ProcessStatus, TaskId};
use crate::machine::MachineId;
use crate::resource::store::{
    ReleaseCheckpointCancellationOutcome, ReleaseCheckpointError,
    TrainerAttemptAssociationStoreError, release_checkpoint_state_for_action,
};
use crate::resource::trainer_publication::AttemptBinding;
use crate::resource::{
    ActionId, ReleaseCheckpointPhase, ReleaseCheckpointStopOutcome, TrainerAttemptAssociationProof,
};
use crate::store::{CancelResult, Store};
use std::fs;
use tempfile::tempdir;

#[test]
fn release_checkpoint_baseline_and_stop_decision_survive_reopen() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("db");
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, notice, intent, association, baseline, decision) = {
        let mut store = Store::open(&database).unwrap();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
            .unwrap();
        let association = saved_release_association(&store, resource.id, background_task);
        let baseline = store
            .capture_release_checkpoint_baseline_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap();
        let runtime_root = association
            .verified_attempt()
            .canonical_runtime_root()
            .to_path_buf();
        let attempt_binding = association.verified_attempt().binding().clone();
        crate::resource::trainer_publication::tests::write_generation_for_test(
            &runtime_root,
            &attempt_binding,
            "generation-a",
            80,
        );
        assert_eq!(
            store
                .capture_release_checkpoint_baseline_for_authority(
                    authority,
                    resource.id,
                    notice.action_id,
                    notice.state_revision,
                )
                .unwrap(),
            baseline
        );
        let ReleaseCheckpointStopOutcome::Reserved(decision) = store
            .reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap()
        else {
            panic!("a new exact checkpoint must reserve the stop decision");
        };
        (resource, notice, intent, association, baseline, decision)
    };

    let mut reopened = Store::open(&database).unwrap();
    assert_eq!(
        reopened
            .capture_release_checkpoint_baseline_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap(),
        baseline
    );
    assert_eq!(
        reopened
            .reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap(),
        ReleaseCheckpointStopOutcome::AlreadyReserved(decision.clone())
    );
    assert_eq!(decision.binding.watcher_intent, intent);
    assert_eq!(
        decision.binding.association,
        TrainerAttemptAssociationProof::from(&association)
    );
    assert_eq!(decision.selected_checkpoint.generation_id, "generation-a");
    assert_eq!(
        decision.selected_checkpoint.record_sha256.to_hex().len(),
        64
    );
    assert_eq!(
        decision.selected_checkpoint.inventory_sha256.to_hex().len(),
        64
    );
}

#[test]
fn pre_baseline_checkpoint_does_not_reserve_stop() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
    let association = saved_release_association(&store, resource.id, background_task);
    let runtime_root = association
        .verified_attempt()
        .canonical_runtime_root()
        .to_path_buf();
    let attempt_binding = association.verified_attempt().binding().clone();
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &runtime_root,
        &attempt_binding,
        "generation-before-baseline",
        80,
    );
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    store
        .bind_release_watcher_for_authority(authority, resource.id, intent)
        .unwrap();
    let baseline = store
        .capture_release_checkpoint_baseline_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
        )
        .unwrap();

    assert!(
        baseline
            .snapshot
            .contains_generation("generation-before-baseline")
    );
    assert_eq!(
        store
            .reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap(),
        ReleaseCheckpointStopOutcome::WaitingForCheckpoint
    );
}

#[test]
fn foreign_attempt_checkpoint_does_not_reserve_stop() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
    let association = saved_release_association(&store, resource.id, background_task);
    let runtime_root = association
        .verified_attempt()
        .canonical_runtime_root()
        .to_path_buf();
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    store
        .bind_release_watcher_for_authority(authority, resource.id, intent)
        .unwrap();
    store
        .capture_release_checkpoint_baseline_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
        )
        .unwrap();
    let foreign_attempt = AttemptBinding {
        campaign_id: "foreign-campaign".into(),
        campaign_revision_id: "foreign-revision".into(),
        task_id: "foreign-trainer-task".into(),
        attempt_id: "foreign-attempt".into(),
        attempt_number: 1,
        ownership_token: "foreign-owner".into(),
    };
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &runtime_root,
        &foreign_attempt,
        "generation-foreign",
        81,
    );

    assert_eq!(
        store
            .reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap(),
        ReleaseCheckpointStopOutcome::WaitingForCheckpoint
    );
}

#[test]
fn new_exact_checkpoint_reserves_one_stop_decision() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
    let association = saved_release_association(&store, resource.id, background_task);
    let runtime_root = association
        .verified_attempt()
        .canonical_runtime_root()
        .to_path_buf();
    let attempt_binding = association.verified_attempt().binding().clone();
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    store
        .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
        .unwrap();
    let baseline = store
        .capture_release_checkpoint_baseline_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
        )
        .unwrap();
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &runtime_root,
        &attempt_binding,
        "generation-new-exact",
        81,
    );

    let ReleaseCheckpointStopOutcome::Reserved(decision) = store
        .reserve_release_checkpoint_stop_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
        )
        .unwrap()
    else {
        panic!("the complete matching checkpoint must reserve a stop decision");
    };
    assert_eq!(decision.binding.watcher_intent, intent);
    assert_eq!(
        decision.selected_checkpoint.generation_id,
        "generation-new-exact"
    );
    assert!(
        !baseline
            .snapshot
            .contains_generation(&decision.selected_checkpoint.generation_id)
    );
}

#[test]
fn duplicate_stop_decision_reuses_checkpoint_and_changed_action_conflicts() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
    let association = saved_release_association(&store, resource.id, background_task);
    let runtime_root = association
        .verified_attempt()
        .canonical_runtime_root()
        .to_path_buf();
    let attempt_binding = association.verified_attempt().binding().clone();
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    store
        .bind_release_watcher_for_authority(authority, resource.id, intent)
        .unwrap();
    store
        .capture_release_checkpoint_baseline_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
        )
        .unwrap();
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &runtime_root,
        &attempt_binding,
        "generation-selected",
        81,
    );
    let ReleaseCheckpointStopOutcome::Reserved(first) = store
        .reserve_release_checkpoint_stop_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
        )
        .unwrap()
    else {
        panic!("the first checkpoint must reserve the stop decision");
    };
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &runtime_root,
        &attempt_binding,
        "generation-later",
        82,
    );

    assert_eq!(
        store
            .reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap(),
        ReleaseCheckpointStopOutcome::AlreadyReserved(first.clone())
    );
    assert_eq!(
        first.selected_checkpoint.generation_id,
        "generation-selected"
    );
    assert!(matches!(
        store.reserve_release_checkpoint_stop_for_authority(
            authority,
            resource.id,
            ActionId::new(),
            notice.state_revision,
        ),
        Err(ReleaseCheckpointError::Conflict)
    ));
}

#[test]
fn checkpoint_cancellation_commits_decision_and_exact_trainer_marker_atomically() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, notice, intent, decision) =
        checkpoint_decision_for_cancellation(&mut store, authority, background_task);
    start_release_watcher_for_test(&mut store, &resource, &intent);

    let ReleaseCheckpointCancellationOutcome::Committed(result) = store
        .commit_release_checkpoint_cancellation_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
            &decision,
        )
        .unwrap()
    else {
        panic!("the accepted watcher must permit the exact stop decision");
    };
    assert_eq!(result.decision, decision);
    assert_eq!(result.cancellation.task_id, background_task);
    assert_eq!(
        store
            .require_task(background_task)
            .unwrap()
            .cancel_requested_at,
        Some(result.cancellation.cancel_requested_at)
    );
    let (state, _) =
        release_checkpoint_state_for_action(&store.conn, resource.id, notice.action_id)
            .unwrap()
            .unwrap();
    assert!(matches!(
        state.phase,
        ReleaseCheckpointPhase::CancellationCommitted {
            decision: saved,
            cancellation,
            ..
        } if *saved == decision && cancellation == result.cancellation
    ));
}

#[test]
fn checkpoint_cancellation_rolls_back_task_marker_when_phase_update_fails() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, notice, intent, decision) =
        checkpoint_decision_for_cancellation(&mut store, authority, background_task);
    start_release_watcher_for_test(&mut store, &resource, &intent);
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_checkpoint_cancellation
             BEFORE UPDATE ON resource_release_checkpoint_states
             WHEN json_extract(NEW.state_json, '$.phase.type') = 'cancellation_committed'
             BEGIN SELECT RAISE(ABORT, 'forced cancellation phase failure'); END;",
        )
        .unwrap();

    assert!(matches!(
        store.commit_release_checkpoint_cancellation_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
            &decision,
        ),
        Err(ReleaseCheckpointError::Storage(_))
    ));
    assert_eq!(
        store
            .require_task(background_task)
            .unwrap()
            .cancel_requested_at,
        None
    );
    let (state, _) =
        release_checkpoint_state_for_action(&store.conn, resource.id, notice.action_id)
            .unwrap()
            .unwrap();
    assert!(matches!(
        state.phase,
        ReleaseCheckpointPhase::StopReserved { decision: saved, .. }
            if *saved == decision
    ));
}

#[test]
fn exact_checkpoint_cancellation_retry_reuses_the_committed_reservation() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, notice, intent, decision) =
        checkpoint_decision_for_cancellation(&mut store, authority, background_task);
    start_release_watcher_for_test(&mut store, &resource, &intent);
    let first = store
        .commit_release_checkpoint_cancellation_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
            &decision,
        )
        .unwrap();
    let ReleaseCheckpointCancellationOutcome::Committed(first_result) = first else {
        panic!("the first cancellation must commit");
    };
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &decision.binding.association.canonical_runtime_root,
        &decision.binding.attempt_binding,
        "generation-later",
        82,
    );
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert!(matches!(
        store.request_cancel(background_task).unwrap(),
        CancelResult::SignalWorker(_)
    ));

    assert_eq!(
        store
            .commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &decision,
            )
            .unwrap(),
        ReleaseCheckpointCancellationOutcome::AlreadyCommitted(first_result.clone())
    );
    assert_eq!(
        store
            .reserve_release_checkpoint_stop_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
            )
            .unwrap(),
        ReleaseCheckpointStopOutcome::AlreadyReserved(decision)
    );
    assert!(
        store
            .require_task(background_task)
            .unwrap()
            .cancel_requested_at
            .is_some()
    );
}

#[test]
fn checkpoint_cancellation_rejects_wrong_task_and_action_decisions() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, notice, intent, decision) =
        checkpoint_decision_for_cancellation(&mut store, authority, background_task);
    start_release_watcher_for_test(&mut store, &resource, &intent);

    let mut wrong_task = decision.clone();
    wrong_task.binding.action.observed_background_task = TaskId::new();
    assert!(matches!(
        store.commit_release_checkpoint_cancellation_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
            &wrong_task,
        ),
        Err(ReleaseCheckpointError::StopDecisionMismatch { action_id })
            if action_id == notice.action_id
    ));

    let mut wrong_action = decision.clone();
    wrong_action.binding.action.action_id = ActionId::new();
    assert!(matches!(
        store.commit_release_checkpoint_cancellation_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
            &wrong_action,
        ),
        Err(ReleaseCheckpointError::StopDecisionMismatch { action_id })
            if action_id == notice.action_id
    ));
    assert_eq!(
        store
            .require_task(background_task)
            .unwrap()
            .cancel_requested_at,
        None
    );
}

#[test]
fn checkpoint_cancellation_waits_for_the_accepted_watcher_to_run() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, notice, intent, decision) =
        checkpoint_decision_for_cancellation(&mut store, authority, background_task);

    assert_eq!(
        store
            .commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &decision,
            )
            .unwrap(),
        ReleaseCheckpointCancellationOutcome::WatcherNotReady {
            watcher_task_id: intent.watcher_task_id.as_task_id(),
        }
    );
    let watcher_spec = watcher_spec(&resource, &intent);
    let (watcher_row, callback) = watcher_task_and_callback(&intent, &watcher_spec);
    accept_watcher(
        &mut store,
        &resource,
        intent.clone(),
        &watcher_row,
        &watcher_spec,
        &callback,
    )
    .unwrap();
    assert_eq!(
        store
            .commit_release_checkpoint_cancellation_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                &decision,
            )
            .unwrap(),
        ReleaseCheckpointCancellationOutcome::WatcherNotReady {
            watcher_task_id: intent.watcher_task_id.as_task_id(),
        }
    );
    assert_eq!(
        store
            .require_task(background_task)
            .unwrap()
            .cancel_requested_at,
        None
    );
}

#[test]
fn checkpoint_cancellation_rejects_changed_checkpoint_and_prior_generic_marker() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let first_task = TaskId::new();
    let (resource, notice, intent, decision) =
        checkpoint_decision_for_cancellation(&mut store, authority, first_task);
    start_release_watcher_for_test(&mut store, &resource, &intent);
    fs::write(
        decision.selected_checkpoint.path.join("checkpoint.bin"),
        b"changed checkpoint",
    )
    .unwrap();

    assert!(matches!(
        store.commit_release_checkpoint_cancellation_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
            &decision,
        ),
        Err(ReleaseCheckpointError::SelectedCheckpointChanged { action_id })
            if action_id == notice.action_id
    ));
    assert_eq!(
        store.require_task(first_task).unwrap().cancel_requested_at,
        None
    );

    let second_task = TaskId::new();
    let (resource, notice, intent, decision) =
        checkpoint_decision_for_cancellation(&mut store, authority, second_task);
    start_release_watcher_for_test(&mut store, &resource, &intent);
    assert!(matches!(
        store.request_cancel(second_task).unwrap(),
        CancelResult::SignalWorker(_)
    ));
    let generic_marker = store.require_task(second_task).unwrap().cancel_requested_at;

    assert!(matches!(
        store.commit_release_checkpoint_cancellation_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
            &decision,
        ),
        Err(ReleaseCheckpointError::TrainerCancellationConflict { task_id })
            if task_id == second_task
    ));
    assert_eq!(
        store.require_task(second_task).unwrap().cancel_requested_at,
        generic_marker
    );
}

#[test]
fn checkpoint_cancellation_rejects_missing_or_terminal_trainers() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let missing_task = TaskId::new();
    let (resource, notice, intent, decision) =
        checkpoint_decision_for_cancellation(&mut store, authority, missing_task);
    start_release_watcher_for_test(&mut store, &resource, &intent);
    store
        .conn
        .pragma_update(None, "foreign_keys", "OFF")
        .unwrap();
    store
        .conn
        .execute("DELETE FROM tasks WHERE id=?1", [missing_task.to_string()])
        .unwrap();
    assert!(matches!(
        store.commit_release_checkpoint_cancellation_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
            &decision,
        ),
        Err(ReleaseCheckpointError::TrainerAssociation(
            TrainerAttemptAssociationStoreError::TaskMissing { task_id }
        )) if task_id == missing_task
    ));

    let terminal_task = TaskId::new();
    let (resource, notice, intent, decision) =
        checkpoint_decision_for_cancellation(&mut store, authority, terminal_task);
    start_release_watcher_for_test(&mut store, &resource, &intent);
    store
        .cas_exit(
            terminal_task,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
        )
        .unwrap()
        .unwrap();
    store
        .update_execution_state(terminal_task, ProcessStatus::Succeeded)
        .unwrap();

    assert!(matches!(
        store.commit_release_checkpoint_cancellation_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
            &decision,
        ),
        Err(ReleaseCheckpointError::TrainerTaskNotRunning {
            task_id,
            state: ProcessStatus::Succeeded,
        }) if task_id == terminal_task
    ));
    assert_eq!(
        store
            .require_task(terminal_task)
            .unwrap()
            .cancel_requested_at,
        None
    );
}

#[test]
fn failed_baseline_persistence_keeps_the_action_unbound_to_a_new_snapshot() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    store
        .bind_release_watcher_for_authority(authority, resource.id, intent)
        .unwrap();
    store
        .conn
        .execute_batch(&format!(
            "CREATE TRIGGER fail_release_checkpoint_update
             BEFORE UPDATE ON resource_release_checkpoint_states
             WHEN OLD.action_id='{}'
             BEGIN SELECT RAISE(ABORT, 'test checkpoint persistence failure'); END;",
            notice.action_id.as_uuid()
        ))
        .unwrap();

    assert!(matches!(
        store.capture_release_checkpoint_baseline_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
        ),
        Err(ReleaseCheckpointError::Storage(_))
    ));
    let (state, _) =
        release_checkpoint_state_for_action(&store.conn, resource.id, notice.action_id)
            .unwrap()
            .unwrap();
    assert_eq!(state.phase, ReleaseCheckpointPhase::WatcherBindingPending);
}
