//! Release notice facade and release watcher binding tests

use super::fixtures::{open_release_for_test, release_watcher_intent, remote_task, resource, spec};
use crate::domain::{ProcessStatus, TaskId, ThreadId};
use crate::machine::MachineId;
use crate::resource::store::{OpenReleaseLoanResult, ResourceStoreError};
use crate::resource::{
    ActionId, AssignmentRevision, DeliveryAttemptId, LoanPhase, LoanState, ReleaseWatcherIntent,
    ReleaseWatcherTaskId, ResourceRevision, SupervisorAddress, SupervisorNoticeDelivery,
};
use crate::store::Store;
use crate::submission::RequestId;
use serde_json::json;
use std::path::PathBuf;
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn release_notice_facade_operations_share_the_store_connection() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("db");
    let mut store = Store::open(&database).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let mut resource = resource(authority);
    resource.registered_background_task = Some(background_task);

    store.register_resource(authority, &resource).unwrap();
    store
        .insert_task(&remote_task(background_task, &spec()))
        .unwrap();
    assert!(
        store
            .cas_status(
                background_task,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .is_some()
    );
    store
        .accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            spec(),
        )
        .unwrap();

    let OpenReleaseLoanResult::Opened { notice, .. } = store
        .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
        .unwrap()
    else {
        panic!("first facade call must open a release loan");
    };
    assert_eq!(
        store.supervisor_notice(notice.id).unwrap(),
        Some(notice.clone())
    );
    assert_eq!(
        store.pending_supervisor_notices().unwrap(),
        vec![notice.clone()]
    );

    let destination = SupervisorAddress {
        machine: MachineId::new(),
        thread: ThreadId(Uuid::now_v7()),
    };
    let retargeted = store
        .retarget_supervisor_notice(
            notice.id,
            notice.assignment_revision,
            destination,
            AssignmentRevision::new(1),
        )
        .unwrap();
    assert_eq!(retargeted.destination, destination);

    let first_attempt = DeliveryAttemptId::new();
    assert_eq!(
        store
            .reserve_supervisor_notice_attempt(notice.id, first_attempt)
            .unwrap()
            .id,
        notice.id
    );
    assert_eq!(
        store
            .recover_sending_supervisor_notices()
            .unwrap()
            .iter()
            .map(|notice| notice.id)
            .collect::<Vec<_>>(),
        vec![notice.id]
    );

    let second_attempt = DeliveryAttemptId::new();
    store
        .reserve_supervisor_notice_attempt(notice.id, second_attempt)
        .unwrap();
    let settled = store
        .settle_supervisor_notice_attempt(notice.id, second_attempt, Ok(()))
        .unwrap();
    assert_eq!(settled.id, notice.id);
    assert!(matches!(
        settled.delivery,
        SupervisorNoticeDelivery::Delivered { .. }
    ));
    assert_eq!(store.supervisor_notice(notice.id).unwrap(), Some(settled));
}

#[test]
fn release_watcher_binding_returns_the_saved_intent_on_exact_retry() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, loan, notice) = open_release_for_test(&mut store, authority, background_task);
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());

    let first = store
        .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
        .unwrap();
    let reserved_task = remote_task(intent.watcher_task_id.as_task_id(), &spec());
    assert!(matches!(
        store.insert_local_task(
            &reserved_task,
            &spec(),
            authority,
            PathBuf::from("/bin/echo").into(),
        ),
        Err(crate::error::AppError::ClusterTaskConflict { task })
            if task == reserved_task.id
    ));
    let retry = store
        .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
        .unwrap();

    assert_eq!(first, intent);
    assert_eq!(retry, first);
    let snapshot = store
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(snapshot.resource.state_revision, notice.state_revision);
    assert_eq!(
        snapshot.resource.registered_background_task,
        Some(background_task)
    );
    let saved_loan = snapshot.loan.unwrap();
    assert_eq!(saved_loan.id, loan.id);
    assert!(matches!(
        saved_loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task,
                watcher_intent: Some(saved),
            }
        } if action_id == notice.action_id
            && observed_background_task == background_task
            && saved == intent
    ));
}

#[test]
fn release_watcher_binding_rejects_conflicting_identity_and_stale_action_data() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    store
        .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
        .unwrap();

    let different_watcher = ReleaseWatcherIntent {
        watcher_task_id: ReleaseWatcherTaskId::new(TaskId::new()),
        ..intent.clone()
    };
    let different_action = ReleaseWatcherIntent {
        action_id: ActionId::new(),
        ..intent.clone()
    };
    let different_background_task = ReleaseWatcherIntent {
        observed_background_task: TaskId::new(),
        ..intent.clone()
    };
    let stale_revision = ReleaseWatcherIntent {
        state_revision: ResourceRevision::new(notice.state_revision.get() - 1),
        ..intent.clone()
    };
    let different_request = ReleaseWatcherIntent {
        request_id: RequestId::new(),
        ..intent.clone()
    };
    let mut changed_spec = serde_json::to_value(spec()).unwrap();
    changed_spec["workload"]["command"][1] = json!("different");
    let different_digest = ReleaseWatcherIntent {
        normalized_spec_sha256: crate::submission::normalized_spec_sha256(
            &serde_json::from_value(changed_spec).unwrap(),
        )
        .unwrap(),
        ..intent.clone()
    };

    for conflicting in [
        different_watcher,
        different_action,
        different_background_task,
        stale_revision,
        different_request,
        different_digest,
    ] {
        assert!(matches!(
            store.bind_release_watcher_for_authority(authority, resource.id, conflicting,),
            Err(ResourceStoreError::Conflict(_))
        ));
    }

    let snapshot = store
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let saved_loan = snapshot.loan.unwrap();
    assert!(matches!(
        saved_loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                watcher_intent: Some(saved),
                ..
            }
        } if saved == intent
    ));
}

#[test]
fn release_watcher_binding_rejects_wrong_authority_and_reused_identities() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let first_background_task = TaskId::new();
    let (first_resource, _, first_notice) =
        open_release_for_test(&mut store, authority, first_background_task);
    let first_intent = release_watcher_intent(
        first_resource.id,
        &first_notice,
        first_background_task,
        TaskId::new(),
    );

    assert!(matches!(
        store.bind_release_watcher_for_authority(
            MachineId::new(),
            first_resource.id,
            first_intent.clone(),
        ),
        Err(ResourceStoreError::WrongAuthority { .. })
    ));
    store
        .bind_release_watcher_for_authority(authority, first_resource.id, first_intent.clone())
        .unwrap();

    let second_background_task = TaskId::new();
    let (second_resource, _, second_notice) =
        open_release_for_test(&mut store, authority, second_background_task);
    let same_watcher_task = ReleaseWatcherIntent {
        watcher_task_id: first_intent.watcher_task_id,
        ..release_watcher_intent(
            second_resource.id,
            &second_notice,
            second_background_task,
            TaskId::new(),
        )
    };
    let same_request = ReleaseWatcherIntent {
        request_id: first_intent.request_id,
        ..release_watcher_intent(
            second_resource.id,
            &second_notice,
            second_background_task,
            TaskId::new(),
        )
    };

    assert!(matches!(
        store.bind_release_watcher_for_authority(authority, second_resource.id, same_watcher_task,),
        Err(ResourceStoreError::Conflict(_))
    ));
    assert!(matches!(
        store.bind_release_watcher_for_authority(authority, second_resource.id, same_request),
        Err(ResourceStoreError::Conflict(_))
    ));

    let second_watcher_task = TaskId::new();
    let same_request_as_task = ReleaseWatcherIntent {
        watcher_task_id: ReleaseWatcherTaskId::new(second_watcher_task),
        request_id: RequestId(second_watcher_task.0),
        ..release_watcher_intent(
            second_resource.id,
            &second_notice,
            second_background_task,
            TaskId::new(),
        )
    };
    assert!(matches!(
        store.bind_release_watcher_for_authority(
            authority,
            second_resource.id,
            same_request_as_task,
        ),
        Err(ResourceStoreError::Conflict(_))
    ));
}

#[test]
fn release_watcher_binding_rejects_request_id_used_by_a_task_route() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    store
        .conn
        .execute(
            "INSERT INTO origin_routes (request_id, task_id, execution_machine, spec_json, route_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                intent.request_id.0.to_string(),
                TaskId::new().to_string(),
                authority.as_uuid().to_string(),
                "{}",
                "{}",
            ],
        )
        .unwrap();

    assert!(matches!(
        store.bind_release_watcher_for_authority(authority, resource.id, intent.clone()),
        Err(ResourceStoreError::Conflict(_))
    ));

    store
        .conn
        .execute(
            "DELETE FROM origin_routes WHERE request_id = ?1",
            [intent.request_id.0.to_string()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO origin_routes (request_id, task_id, execution_machine, spec_json, route_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                RequestId::new().0.to_string(),
                intent.watcher_task_id.as_task_id().to_string(),
                authority.as_uuid().to_string(),
                "{}",
                "{}",
            ],
        )
        .unwrap();
    assert!(matches!(
        store.bind_release_watcher_for_authority(authority, resource.id, intent),
        Err(ResourceStoreError::Conflict(_))
    ));
}

#[test]
fn release_watcher_binding_survives_store_reopen_without_changing_release_state() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("db");
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, notice, intent) = {
        let mut store = Store::open(&database).unwrap();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        store
            .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
            .unwrap();
        (resource, notice, intent)
    };

    let mut reopened = Store::open(&database).unwrap();
    let snapshot = reopened
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(snapshot.resource.state_revision, notice.state_revision);
    assert_eq!(
        snapshot.resource.registered_background_task,
        Some(background_task)
    );
    let saved_loan = snapshot.loan.unwrap();
    assert!(matches!(
        saved_loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task,
                watcher_intent: Some(saved),
            }
        } if action_id == notice.action_id
            && observed_background_task == background_task
            && saved == intent
    ));
    assert_eq!(
        reopened
            .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
            .unwrap(),
        intent
    );
}
