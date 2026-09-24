//! Release watcher task acceptance tests

use super::fixtures::{
    accept_watcher, open_release_for_test, prepare_release_checkpoint_baseline,
    release_watcher_intent, remote_task, resource, spec, watcher_acceptance_counts,
    watcher_acceptance_input, watcher_spec, watcher_task_and_callback,
};
use crate::domain::{ProcessStatus, TaskId, TaskState, ThreadId};
use crate::events::{EventPayload, TaskEvent};
use crate::machine::MachineId;
use crate::resource::store::{OpenReleaseLoanResult, ReleaseWatcherAcceptance};
use crate::resource::{
    ActionId, LoanPhase, LoanState, ReleaseWatcherIntent, ReleaseWatcherTaskId, SupervisorAddress,
};
use crate::spec::NormalizedSpec;
use crate::store::{ExecutorIdentity, Store};
use crate::submission::{CallbackContext, CallbackExecutable, RequestId, SubmissionState};
use serde_json::json;
use std::path::PathBuf;
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn release_watcher_acceptance_inserts_fixed_task_route_identity_and_event() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    let spec = watcher_spec(&resource, &intent);
    prepare_release_checkpoint_baseline(&mut store, authority, &resource, &intent);
    let (row, callback) = watcher_task_and_callback(&intent, &spec);

    let accepted = accept_watcher(
        &mut store,
        &resource,
        intent.clone(),
        &row,
        &spec,
        &callback,
    )
    .unwrap();
    assert_eq!(
        accepted,
        ReleaseWatcherAcceptance::Inserted { task: row.id }
    );
    assert_eq!(
        store.get_task(row.id).unwrap().unwrap().status(),
        ProcessStatus::Queued
    );

    let route = store
        .origin_route_by_request(intent.request_id)
        .unwrap()
        .unwrap();
    assert_eq!(route.task, row.id);
    assert_eq!(route.origin_machine, authority);
    assert_eq!(route.execution_machine, authority);
    assert_eq!(route.callback, callback);
    assert_eq!(
        serde_json::to_value(route.current_spec().unwrap()).unwrap(),
        serde_json::to_value(&spec).unwrap()
    );
    assert!(matches!(route.submission, SubmissionState::Accepted));
    let ExecutorIdentity::Accepted(identity) = store.executor_identity(row.id).unwrap().unwrap()
    else {
        panic!("watcher task must have an accepted executor identity");
    };
    assert_eq!(identity.origin_machine, authority);
    assert_eq!(identity.execution_machine, authority);
    assert_eq!(
        serde_json::to_value(identity.current_spec().unwrap()).unwrap(),
        serde_json::to_value(&spec).unwrap()
    );
    assert_eq!(identity.state, ProcessStatus::Queued);

    let event_json: String = store
        .conn
        .query_row(
            "SELECT event_json FROM executor_outbox WHERE task_id=?1 AND seq=1",
            [row.id.to_string()],
            |entry| entry.get(0),
        )
        .unwrap();
    let event: TaskEvent = serde_json::from_str(&event_json).unwrap();
    assert_eq!(event.task, row.id);
    assert_eq!(event.origin_machine, authority);
    assert_eq!(event.execution_machine, authority);
    assert_eq!(
        event.payload,
        EventPayload::State {
            status: ProcessStatus::Queued
        }
    );
    assert_eq!(
        watcher_acceptance_counts(&store, intent.request_id, row.id),
        [1, 1, 1, 1]
    );

    let snapshot = store
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource.id)
        .unwrap();
    assert!(matches!(
        snapshot.loan.unwrap().state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                watcher_intent: Some(saved),
                ..
            }
        } if saved == intent
    ));
}

#[test]
fn release_watcher_acceptance_exact_retry_survives_reopen_and_old_bind_retry() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("db");
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, intent, row, callback, spec) = {
        let mut store = Store::open(&database).unwrap();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        let spec = watcher_spec(&resource, &intent);
        prepare_release_checkpoint_baseline(&mut store, authority, &resource, &intent);
        let (row, callback) = watcher_task_and_callback(&intent, &spec);
        assert!(matches!(
            accept_watcher(
                &mut store,
                &resource,
                intent.clone(),
                &row,
                &spec,
                &callback
            )
            .unwrap(),
            ReleaseWatcherAcceptance::Inserted { .. }
        ));
        (resource, intent, row, callback, spec)
    };

    let mut reopened = Store::open(&database).unwrap();
    let before = watcher_acceptance_counts(&reopened, intent.request_id, row.id);
    let retry = accept_watcher(
        &mut reopened,
        &resource,
        intent.clone(),
        &row,
        &spec,
        &callback,
    )
    .unwrap();
    assert_eq!(
        retry,
        ReleaseWatcherAcceptance::Existing {
            task: row.id,
            state: TaskState::Queued
        }
    );
    assert_eq!(
        watcher_acceptance_counts(&reopened, intent.request_id, row.id),
        before
    );
    assert_eq!(before, [1, 1, 1, 1]);
    assert_eq!(
        reopened
            .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
            .unwrap(),
        intent
    );
}

#[test]
fn release_watcher_acceptance_rejects_changed_content_and_owners() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    let spec = watcher_spec(&resource, &intent);
    prepare_release_checkpoint_baseline(&mut store, authority, &resource, &intent);
    let (row, callback) = watcher_task_and_callback(&intent, &spec);
    accept_watcher(
        &mut store,
        &resource,
        intent.clone(),
        &row,
        &spec,
        &callback,
    )
    .unwrap();

    let different_action = ReleaseWatcherIntent {
        action_id: ActionId::new(),
        ..intent.clone()
    };
    let different_request = ReleaseWatcherIntent {
        request_id: RequestId::new(),
        ..intent.clone()
    };
    let different_task = ReleaseWatcherIntent {
        watcher_task_id: ReleaseWatcherTaskId::new(TaskId::new()),
        ..intent.clone()
    };
    for changed in [different_action, different_request, different_task] {
        assert!(accept_watcher(&mut store, &resource, changed, &row, &spec, &callback).is_err());
    }

    let mut changed_spec_value = serde_json::to_value(&spec).unwrap();
    changed_spec_value["workload"]["command"][1] = json!("changed");
    let changed_spec: NormalizedSpec = serde_json::from_value(changed_spec_value).unwrap();
    assert!(
        accept_watcher(
            &mut store,
            &resource,
            intent.clone(),
            &row,
            &changed_spec,
            &callback,
        )
        .is_err()
    );
    let changed_digest_intent = ReleaseWatcherIntent {
        normalized_spec_sha256: crate::submission::normalized_spec_sha256(&changed_spec).unwrap(),
        ..intent.clone()
    };
    let changed_row = remote_task(row.id, &changed_spec);
    let changed_callback = CallbackContext {
        env: changed_row.env.clone(),
        cwd: changed_row.cwd.clone(),
        codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
    };
    assert!(
        accept_watcher(
            &mut store,
            &resource,
            changed_digest_intent,
            &changed_row,
            &changed_spec,
            &changed_callback,
        )
        .is_err()
    );

    let mut changed_callback = callback.clone();
    changed_callback.codex = CallbackExecutable::available(PathBuf::from("/bin/sh"));
    assert!(
        accept_watcher(
            &mut store,
            &resource,
            intent.clone(),
            &row,
            &spec,
            &changed_callback,
        )
        .is_err()
    );
    let mut wrong_owner_input =
        watcher_acceptance_input(&resource, intent.clone(), &row, &spec, &callback);
    wrong_owner_input.authority_machine = MachineId::new();
    assert!(
        store
            .accept_release_watcher_for_authority(wrong_owner_input)
            .is_err()
    );
    let changed_supervisor = SupervisorAddress {
        machine: authority,
        thread: ThreadId(Uuid::now_v7()),
    };
    let mut changed_supervisor_resource = resource.clone();
    changed_supervisor_resource.supervisor = changed_supervisor;
    assert!(
        accept_watcher(
            &mut store,
            &changed_supervisor_resource,
            intent,
            &row,
            &spec,
            &callback,
        )
        .is_err()
    );
}

#[test]
fn remote_release_watcher_supervisor_is_typed_and_writes_no_task_records() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let remote_supervisor = MachineId::new();
    let background_task = TaskId::new();
    let spec = spec();
    let mut resource = resource(authority);
    resource.supervisor = SupervisorAddress {
        machine: remote_supervisor,
        thread: spec.thread,
    };
    resource.registered_background_task = Some(background_task);
    store.register_resource(authority, &resource).unwrap();
    store
        .insert_task(&remote_task(background_task, &spec))
        .unwrap();
    store
        .cas_status(
            background_task,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    store
        .accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            spec.clone(),
        )
        .unwrap();
    let OpenReleaseLoanResult::Opened { notice, .. } = store
        .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
        .unwrap()
    else {
        panic!("fixture must create a new release action");
    };
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    let spec = watcher_spec(&resource, &intent);
    let (row, callback) = watcher_task_and_callback(&intent, &spec);

    let result = accept_watcher(
        &mut store,
        &resource,
        intent.clone(),
        &row,
        &spec,
        &callback,
    )
    .unwrap();
    assert_eq!(
        result,
        ReleaseWatcherAcceptance::UnsupportedRemoteSupervisor {
            authority_machine: authority,
            supervisor: resource.supervisor,
        }
    );
    assert_eq!(
        watcher_acceptance_counts(&store, intent.request_id, row.id),
        [0, 0, 0, 0]
    );
    let snapshot = store
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource.id)
        .unwrap();
    assert!(matches!(
        snapshot.loan.unwrap().state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                watcher_intent: None,
                ..
            }
        }
    ));
}

#[test]
fn release_watcher_acceptance_rolls_back_binding_and_task_records_on_event_failure() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    let spec = watcher_spec(&resource, &intent);
    prepare_release_checkpoint_baseline(&mut store, authority, &resource, &intent);
    let (row, callback) = watcher_task_and_callback(&intent, &spec);
    store
        .conn
        .execute_batch(&format!(
            "CREATE TRIGGER fail_release_watcher_event
             BEFORE INSERT ON executor_outbox
             WHEN NEW.task_id='{}'
             BEGIN SELECT RAISE(ABORT, 'test event failure'); END;",
            row.id
        ))
        .unwrap();

    assert!(
        accept_watcher(
            &mut store,
            &resource,
            intent.clone(),
            &row,
            &spec,
            &callback
        )
        .is_err()
    );
    assert_eq!(
        watcher_acceptance_counts(&store, intent.request_id, row.id),
        [0, 0, 0, 0]
    );
    let snapshot = store
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource.id)
        .unwrap();
    assert!(matches!(
        snapshot.loan.unwrap().state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                watcher_intent: Some(saved),
                ..
            }
        } if saved == intent
    ));
}
