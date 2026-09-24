//! Operator controls for queued resource requests

use super::fixtures::{queue_waiting_request, resource, spec};
use crate::machine::MachineId;
use crate::resource::api::{BrowserResourceAction, QueuePlacement};
use crate::resource::{DeliveryAttemptId, ResourceId, ResourceRequestState, ResourceRevision};
use crate::store::{ResourceControlEffect, ResourceControlError, ResourceControlRequest, Store};
use crate::submission::RequestId;
use rusqlite::params;
use tempfile::tempdir;
use uuid::Uuid;

fn queue_request(store: &mut Store, authority: MachineId, resource_id: ResourceId) -> RequestId {
    queue_waiting_request(store, authority, resource_id, MachineId::new(), &spec()).request_id
}

fn state_revision(
    store: &Store,
    authority: MachineId,
    resource_id: ResourceId,
) -> ResourceRevision {
    store
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource_id)
        .unwrap()
        .resource
        .state_revision
}

fn serving_order(store: &Store, authority: MachineId, resource_id: ResourceId) -> Vec<RequestId> {
    store
        .resource_read_models(authority, Some(resource_id))
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .requests
        .into_iter()
        .filter(|request| request.state == ResourceRequestState::Queued)
        .map(|request| request.request_id)
        .collect()
}

fn queue_ranks(store: &Store, resource_id: ResourceId) -> Vec<i64> {
    store
        .conn
        .prepare(
            "SELECT queue_rank FROM resource_requests
             WHERE resource_id = ?1 ORDER BY queue_rank",
        )
        .unwrap()
        .query_map([resource_id.as_uuid().to_string()], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn move_request(
    store: &mut Store,
    authority: MachineId,
    resource_id: ResourceId,
    request_id: RequestId,
    placement: QueuePlacement,
    expected_revision: ResourceRevision,
    operation_id: Uuid,
) -> Result<crate::store::ResourceControlStart, ResourceControlError> {
    store.begin_resource_control(
        authority,
        operation_id,
        &ResourceControlRequest {
            resource_id,
            expected_revision,
            action: BrowserResourceAction::MoveQueued {
                request_id,
                placement,
            },
        },
        DeliveryAttemptId::new(),
    )
}

#[test]
fn queued_moves_change_serving_order_keep_ranks_bounded_and_append_at_the_back() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let first = queue_request(&mut store, authority, resource.id);
    let second = queue_request(&mut store, authority, resource.id);
    let third = queue_request(&mut store, authority, resource.id);
    let fourth = queue_request(&mut store, authority, resource.id);
    let original_ranks = queue_ranks(&store, resource.id);
    let cases = [
        (
            third,
            QueuePlacement::Front,
            vec![third, first, second, fourth],
        ),
        (
            third,
            QueuePlacement::Back,
            vec![first, second, fourth, third],
        ),
        (
            fourth,
            QueuePlacement::Before { request_id: second },
            vec![first, fourth, second, third],
        ),
        (
            fourth,
            QueuePlacement::After { request_id: third },
            vec![first, second, third, fourth],
        ),
    ];

    for (step, (request_id, placement, expected_order)) in cases.into_iter().enumerate() {
        let previous_revision = state_revision(&store, authority, resource.id);
        let started = move_request(
            &mut store,
            authority,
            resource.id,
            request_id,
            placement,
            previous_revision,
            Uuid::now_v7(),
        )
        .unwrap();
        assert!(matches!(started.effect, ResourceControlEffect::Reordered));
        assert!(!started.replayed);
        let current_revision = state_revision(&store, authority, resource.id);
        assert_eq!(
            current_revision.get(),
            previous_revision.get() + 1,
            "step {step}"
        );
        assert_eq!(
            serving_order(&store, authority, resource.id),
            expected_order
        );
        assert_eq!(
            store
                .next_queued_resource_request(authority, resource.id)
                .unwrap()
                .unwrap()
                .request_id,
            expected_order[0],
            "step {step}"
        );
        assert_eq!(queue_ranks(&store, resource.id), original_ranks);
    }

    let appended = queue_request(&mut store, authority, resource.id);
    let mut expected = vec![first, second, third, fourth];
    expected.push(appended);
    assert_eq!(serving_order(&store, authority, resource.id), expected);
    assert_eq!(
        queue_ranks(&store, resource.id),
        [original_ranks, vec![5]].concat()
    );
}

#[test]
fn queued_move_revision_replay_noop_and_operation_conflict_are_atomic() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let first = queue_request(&mut store, authority, resource.id);
    let second = queue_request(&mut store, authority, resource.id);
    let revision = state_revision(&store, authority, resource.id);
    let operation_id = Uuid::now_v7();

    let first_result = move_request(
        &mut store,
        authority,
        resource.id,
        first,
        QueuePlacement::Front,
        revision,
        operation_id,
    )
    .unwrap();
    assert!(!first_result.replayed);
    assert_eq!(
        state_revision(&store, authority, resource.id).get(),
        revision.get() + 1
    );
    assert_eq!(
        serving_order(&store, authority, resource.id),
        [first, second]
    );

    let replay = move_request(
        &mut store,
        authority,
        resource.id,
        first,
        QueuePlacement::Front,
        revision,
        operation_id,
    )
    .unwrap();
    assert!(replay.replayed);
    assert!(matches!(replay.effect, ResourceControlEffect::Reordered));
    assert_eq!(
        state_revision(&store, authority, resource.id).get(),
        revision.get() + 1
    );
    assert_eq!(
        serving_order(&store, authority, resource.id),
        [first, second]
    );

    assert!(matches!(
        move_request(
            &mut store,
            authority,
            resource.id,
            first,
            QueuePlacement::Back,
            revision,
            operation_id,
        ),
        Err(ResourceControlError::Conflict)
    ));
    assert_eq!(
        state_revision(&store, authority, resource.id).get(),
        revision.get() + 1
    );
    assert_eq!(
        serving_order(&store, authority, resource.id),
        [first, second]
    );
}

#[test]
fn queued_move_refuses_nonqueued_requests_invalid_anchors_and_stale_revisions() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let main_resource = resource(authority);
    store.register_resource(authority, &main_resource).unwrap();
    let moving = queue_request(&mut store, authority, main_resource.id);
    let queued_anchor = queue_request(&mut store, authority, main_resource.id);
    let nonqueued = queue_request(&mut store, authority, main_resource.id);
    let other_resource = resource(authority);
    store.register_resource(authority, &other_resource).unwrap();
    let other_anchor = queue_request(&mut store, authority, other_resource.id);
    store
        .conn
        .execute(
            "UPDATE resource_requests SET state_json = ?1 WHERE request_id = ?2",
            params![
                serde_json::to_string(&ResourceRequestState::CancelledBeforeLaunch).unwrap(),
                nonqueued.0.to_string(),
            ],
        )
        .unwrap();
    let revision = state_revision(&store, authority, main_resource.id);

    let invalid = [
        (nonqueued, QueuePlacement::Front),
        (moving, QueuePlacement::Before { request_id: moving }),
        (
            moving,
            QueuePlacement::Before {
                request_id: nonqueued,
            },
        ),
        (
            moving,
            QueuePlacement::After {
                request_id: other_anchor,
            },
        ),
        (other_anchor, QueuePlacement::Front),
    ];
    for (request_id, placement) in invalid {
        assert!(matches!(
            move_request(
                &mut store,
                authority,
                main_resource.id,
                request_id,
                placement,
                revision,
                Uuid::now_v7(),
            ),
            Err(ResourceControlError::NotAllowed(_))
        ));
    }

    assert!(matches!(
        move_request(
            &mut store,
            authority,
            main_resource.id,
            moving,
            QueuePlacement::Back,
            ResourceRevision::new(revision.get() + 1),
            Uuid::now_v7(),
        ),
        Err(ResourceControlError::StaleRevision { current }) if current == revision
    ));
    assert_eq!(
        state_revision(&store, authority, main_resource.id),
        revision
    );
    assert_eq!(
        serving_order(&store, authority, main_resource.id),
        [moving, queued_anchor]
    );
}
