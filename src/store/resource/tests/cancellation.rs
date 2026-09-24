//! Durable resource cancellation and tombstone tests

use super::fixtures::{
    assert_cancelled_tombstone, identity_count, open_release_for_test, prevention_count,
    remote_task, resource, resource_cancellation_identity, resource_cancellation_proof, spec,
};
use crate::domain::{ExitReason, TaskId};
use crate::machine::MachineId;
use crate::resource::store::{QueueCancellationResult, ResourceStoreError};
use crate::resource::{ResourceRequestState, ReturnContext};
use crate::store::{ExecutorIdentity, IdentityError, Store};
use crate::submission::{
    ExecutionRecord, RequestId, ResourceCancellationIneligibleReason, ResourceCancellationOutcome,
    ResourceRoutePhase,
};
use rusqlite::params;
use std::sync::{Arc, Barrier};
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn durable_resource_cancellation_receipt_replays_and_conflicts_by_identity() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let origin = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let identity = resource_cancellation_identity(
        Uuid::now_v7(),
        RequestId::new(),
        TaskId::new(),
        resource.id,
        origin,
        authority,
        ResourceRoutePhase::AcceptanceUnknown,
    );

    let first = store
        .cancel_resource_request_with_receipt(
            authority,
            identity.clone(),
            resource_cancellation_proof(&identity, &spec(), ResourceRoutePhase::AcceptanceUnknown),
        )
        .unwrap();
    assert_eq!(
        first.outcome,
        ResourceCancellationOutcome::PreventedBeforeAcceptance
    );
    assert_eq!(
        store.resource_cancellation_receipt(&identity).unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        store
            .cancel_resource_request_with_receipt(
                authority,
                identity.clone(),
                resource_cancellation_proof(
                    &identity,
                    &spec(),
                    ResourceRoutePhase::AcceptanceUnknown,
                ),
            )
            .unwrap(),
        first
    );

    let mut changed = identity;
    changed.task = TaskId::new();
    assert!(matches!(
        store.cancel_resource_request_with_receipt(
            authority,
            changed.clone(),
            resource_cancellation_proof(&changed, &spec(), ResourceRoutePhase::AcceptanceUnknown,),
        ),
        Err(ResourceStoreError::Conflict(_))
    ));
    assert_eq!(prevention_count(&store), 1);
}

#[test]
fn durable_resource_cancellation_cancels_queued_and_assigned_requests() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let origin = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let queued_request = RequestId::new();
    let queued_task = TaskId::new();
    store
        .accept_resource_request(
            authority,
            queued_request,
            queued_task,
            resource.id,
            origin,
            spec(),
        )
        .unwrap();
    let queued_identity = resource_cancellation_identity(
        Uuid::now_v7(),
        queued_request,
        queued_task,
        resource.id,
        origin,
        authority,
        ResourceRoutePhase::Waiting,
    );
    let queued_receipt = store
        .cancel_resource_request_with_receipt(
            authority,
            queued_identity.clone(),
            resource_cancellation_proof(&queued_identity, &spec(), ResourceRoutePhase::Waiting),
        )
        .unwrap();
    assert_eq!(
        queued_receipt.outcome,
        ResourceCancellationOutcome::CancelledBeforeLaunch
    );
    assert!(matches!(
        store.resource_requests(authority, resource.id).unwrap()[0].state,
        ResourceRequestState::CancelledBeforeLaunch
    ));
    assert_eq!(
        store
            .cancel_resource_request_with_receipt(
                authority,
                queued_identity.clone(),
                resource_cancellation_proof(
                    &queued_identity,
                    &spec(),
                    ResourceRoutePhase::Waiting,
                ),
            )
            .unwrap(),
        queued_receipt
    );

    let assigned_directory = tempdir().unwrap();
    let mut assigned_store = Store::open(&assigned_directory.path().join("db")).unwrap();
    let assigned_authority = MachineId::new();
    let background_task = TaskId::new();
    let (assigned_resource, _, _) =
        open_release_for_test(&mut assigned_store, assigned_authority, background_task);
    let assigned = assigned_store
        .resource_requests(assigned_authority, assigned_resource.id)
        .unwrap()[0]
        .clone();
    let next_request_id = RequestId::new();
    let next_task = TaskId::new();
    assigned_store
        .accept_resource_request(
            assigned_authority,
            next_request_id,
            next_task,
            assigned_resource.id,
            MachineId::new(),
            spec(),
        )
        .unwrap();
    let (loan, _) = assigned_store
        .seed_serving_loan_for_test(
            assigned_authority,
            assigned_resource.id,
            assigned.request_id,
            ReturnContext::Stopped {
                task_id: background_task,
                checkpoint_ref: "checkpoint-1".into(),
                recovery_ref: "recovery-1".into(),
            },
        )
        .unwrap();
    let assigned_identity = resource_cancellation_identity(
        Uuid::now_v7(),
        assigned.request_id,
        assigned.task_id,
        assigned_resource.id,
        assigned.origin_machine,
        assigned_authority,
        ResourceRoutePhase::Waiting,
    );
    let assigned_receipt = assigned_store
        .cancel_resource_request_with_receipt(
            assigned_authority,
            assigned_identity.clone(),
            resource_cancellation_proof(&assigned_identity, &spec(), ResourceRoutePhase::Waiting),
        )
        .unwrap();
    assert_eq!(
        assigned_receipt.outcome,
        ResourceCancellationOutcome::CancelledBeforeLaunch
    );
    let requests = assigned_store
        .resource_requests(assigned_authority, assigned_resource.id)
        .unwrap();
    assert!(matches!(
        requests[0].state,
        ResourceRequestState::CancelledBeforeLaunch
    ));
    assert!(matches!(
        requests[1].state,
        ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
    ));
    assert_eq!(
        assigned_store
            .cancel_resource_request_with_receipt(
                assigned_authority,
                assigned_identity.clone(),
                resource_cancellation_proof(
                    &assigned_identity,
                    &spec(),
                    ResourceRoutePhase::Waiting,
                ),
            )
            .unwrap(),
        assigned_receipt
    );
}

#[test]
fn resource_cancellation_of_a_terminal_request_returns_typed_attention_receipt() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let origin = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let request = RequestId::new();
    let task = TaskId::new();
    store
        .accept_resource_request(authority, request, task, resource.id, origin, spec())
        .unwrap();
    let terminal_state = ResourceRequestState::Finished {
        outcome: ExitReason::Cancelled,
    };
    store
        .conn
        .execute(
            "UPDATE resource_requests SET state_json=?1 WHERE request_id=?2",
            params![
                serde_json::to_string(&terminal_state).unwrap(),
                request.0.to_string(),
            ],
        )
        .unwrap();

    let identity = resource_cancellation_identity(
        Uuid::now_v7(),
        request,
        task,
        resource.id,
        origin,
        authority,
        ResourceRoutePhase::Waiting,
    );
    let receipt = store
        .cancel_resource_request_with_receipt(
            authority,
            identity.clone(),
            resource_cancellation_proof(&identity, &spec(), ResourceRoutePhase::Waiting),
        )
        .unwrap();
    assert_eq!(
        receipt.outcome,
        ResourceCancellationOutcome::NotEligible {
            reason: ResourceCancellationIneligibleReason::Terminal,
        }
    );
    assert!(matches!(
        store.resource_requests(authority, resource.id).unwrap()[0].state,
        ResourceRequestState::Finished { .. }
    ));
    assert_eq!(
        store.resource_cancellation_receipt(&identity).unwrap(),
        Some(receipt)
    );
}

#[test]
fn activated_route_race_returns_attention_without_cancelling_assigned_work() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let (resource, _, _) = open_release_for_test(&mut store, authority, background_task);
    let assigned = store.resource_requests(authority, resource.id).unwrap()[0].clone();
    let queued_request = RequestId::new();
    let queued_task = TaskId::new();
    store
        .accept_resource_request(
            authority,
            queued_request,
            queued_task,
            resource.id,
            MachineId::new(),
            spec(),
        )
        .unwrap();
    let (loan, _) = store
        .seed_serving_loan_for_test(
            authority,
            resource.id,
            assigned.request_id,
            ReturnContext::Stopped {
                task_id: background_task,
                checkpoint_ref: "checkpoint-1".into(),
                recovery_ref: "recovery-1".into(),
            },
        )
        .unwrap();

    let identity = resource_cancellation_identity(
        Uuid::now_v7(),
        assigned.request_id,
        assigned.task_id,
        resource.id,
        assigned.origin_machine,
        authority,
        ResourceRoutePhase::Waiting,
    );
    let receipt = store
        .cancel_resource_request_with_receipt(
            authority,
            identity.clone(),
            resource_cancellation_proof(&identity, &spec(), ResourceRoutePhase::Activated),
        )
        .unwrap();
    assert_eq!(
        receipt.outcome,
        ResourceCancellationOutcome::NotEligible {
            reason: ResourceCancellationIneligibleReason::Activated,
        }
    );
    let requests = store.resource_requests(authority, resource.id).unwrap();
    assert!(matches!(
        requests[0].state,
        ResourceRequestState::Assigned { loan_id: assigned_loan }
            if assigned_loan == loan.id
    ));
    assert!(matches!(requests[1].state, ResourceRequestState::Queued));
    assert_eq!(
        store.resource_cancellation_receipt(&identity).unwrap(),
        Some(receipt)
    );
}

#[test]
fn cancellation_before_acceptance_fences_delayed_remote_task_acceptance() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let origin = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let request = RequestId::new();
    let task = TaskId::new();

    assert!(matches!(
        store
            .cancel_resource_request_before_activation(
                authority,
                request,
                task,
                resource.id,
                origin,
            )
            .unwrap(),
        QueueCancellationResult::PreventedBeforeAcceptance
    ));
    assert_eq!(prevention_count(&store), 1);
    assert_cancelled_tombstone(
        store.executor_identity(task).unwrap().unwrap(),
        task,
        origin,
        authority,
    );
    assert!(store.origin_route_by_task(task).unwrap().is_none());

    let execution = store
        .insert_remote_task(&remote_task(task, &spec()), &spec(), origin, authority)
        .unwrap();
    assert_cancelled_tombstone(execution, task, origin, authority);
    assert!(store.get_task(task).unwrap().is_none());
    assert_eq!(identity_count(&store, task), 1);

    assert!(matches!(
        store
            .cancel_resource_request_before_activation(
                authority,
                request,
                task,
                resource.id,
                origin,
            )
            .unwrap(),
        QueueCancellationResult::PreventedBeforeAcceptance
    ));
    assert_eq!(prevention_count(&store), 1);
    assert_eq!(identity_count(&store, task), 1);
}

#[test]
fn cancellation_after_queue_acceptance_is_atomic_and_idempotent() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let origin = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let request = RequestId::new();
    let task = TaskId::new();
    let accepted = store
        .accept_resource_request(authority, request, task, resource.id, origin, spec())
        .unwrap();
    assert!(store.origin_route_by_task(task).unwrap().is_none());
    assert_eq!(
        store
            .next_queued_resource_request(authority, resource.id)
            .unwrap()
            .unwrap()
            .request_id,
        request
    );

    let cancelled = store
        .cancel_resource_request_before_activation(authority, request, task, resource.id, origin)
        .unwrap();
    let QueueCancellationResult::Request(cancelled) = cancelled else {
        panic!("accepted cancellation must return its saved request");
    };
    assert_eq!(cancelled.acceptance_sequence, accepted.acceptance_sequence);
    assert!(matches!(
        cancelled.state,
        ResourceRequestState::CancelledBeforeLaunch
    ));
    assert!(
        store
            .next_queued_resource_request(authority, resource.id)
            .unwrap()
            .is_none()
    );
    assert_cancelled_tombstone(
        store.executor_identity(task).unwrap().unwrap(),
        task,
        origin,
        authority,
    );

    let retry = store
        .cancel_resource_request_before_activation(authority, request, task, resource.id, origin)
        .unwrap();
    let QueueCancellationResult::Request(retry) = retry else {
        panic!("repeated cancellation must return the saved request");
    };
    assert_eq!(retry.acceptance_sequence, accepted.acceptance_sequence);
    assert!(matches!(
        retry.state,
        ResourceRequestState::CancelledBeforeLaunch
    ));

    let delayed_acceptance = store
        .insert_remote_task(&remote_task(task, &spec()), &spec(), origin, authority)
        .unwrap();
    assert_cancelled_tombstone(delayed_acceptance, task, origin, authority);
    assert!(store.get_task(task).unwrap().is_none());
    assert_eq!(identity_count(&store, task), 1);
}

#[test]
fn concurrent_direct_acceptance_cannot_claim_a_resource_queue_id() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    let authority = MachineId::new();
    let origin = MachineId::new();
    let resource = resource(authority);
    let request = RequestId::new();
    let task = TaskId::new();
    let barrier = Arc::new(Barrier::new(2));

    let mut setup = Store::open(&path).unwrap();
    setup.register_resource(authority, &resource).unwrap();
    setup
        .accept_resource_request(authority, request, task, resource.id, origin, spec())
        .unwrap();
    drop(setup);

    let accept_path = path.clone();
    let accept_barrier = barrier.clone();
    let spec_for_acceptance = spec();
    let acceptance = std::thread::spawn(move || {
        let mut store = Store::open(&accept_path).unwrap();
        accept_barrier.wait();
        store.accept_execution(&ExecutionRecord {
            task,
            origin_machine: origin,
            execution_machine: authority,
            spec: spec_for_acceptance.into(),
            state: crate::domain::ProcessStatus::Queued,
        })
    });

    let mut cancel_store = Store::open(&path).unwrap();
    barrier.wait();
    let cancellation = cancel_store.cancel_resource_request_before_activation(
        authority,
        request,
        task,
        resource.id,
        origin,
    );
    let execution_identity = acceptance.join().unwrap();

    let final_store = Store::open(&path).unwrap();
    let saved = final_store.executor_identity(task).unwrap().unwrap();
    let tombstone = match execution_identity {
        Err(IdentityError::Conflict) => match saved {
            ExecutorIdentity::Rejected(tombstone) => tombstone,
            identity => panic!("resource cancellation must retain a rejection: {identity:?}"),
        },
        Ok(ExecutorIdentity::Rejected(tombstone)) => tombstone,
        outcome => panic!("ordinary acceptance must not win this race: {outcome:?}"),
    };
    assert_eq!(tombstone.task, task);
    assert_eq!(tombstone.origin_machine, origin);
    assert_eq!(tombstone.execution_machine, authority);
    assert!(matches!(
        cancellation,
        Ok(QueueCancellationResult::Request(cancelled))
            if matches!(cancelled.state, ResourceRequestState::CancelledBeforeLaunch)
    ));
    assert_eq!(prevention_count(&final_store), 0);
}

#[test]
fn tombstone_insert_failure_rolls_back_queued_request_cancellation() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let origin = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let request = RequestId::new();
    let task = TaskId::new();
    store
        .accept_resource_request(authority, request, task, resource.id, origin, spec())
        .unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_executor_tombstone
             BEFORE INSERT ON executor_identities
             BEGIN SELECT RAISE(ABORT, 'forced tombstone failure'); END;",
        )
        .unwrap();

    assert!(
        store
            .cancel_resource_request_before_activation(
                authority,
                request,
                task,
                resource.id,
                origin,
            )
            .is_err()
    );
    let requests = store.resource_requests(authority, resource.id).unwrap();
    assert_eq!(requests.len(), 1);
    assert!(matches!(requests[0].state, ResourceRequestState::Queued));
    assert_eq!(identity_count(&store, task), 0);
    assert!(
        store
            .next_queued_resource_request(authority, resource.id)
            .unwrap()
            .is_some()
    );
}

#[test]
fn terminal_cancellation_does_not_create_executor_tombstones() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let origin = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let terminal = store
        .accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            origin,
            spec(),
        )
        .unwrap();
    let terminal_json = serde_json::to_string(&ResourceRequestState::Finished {
        outcome: ExitReason::Cancelled,
    })
    .unwrap();
    store
        .conn
        .execute(
            "UPDATE resource_requests SET state_json=?1 WHERE request_id=?2",
            rusqlite::params![terminal_json, terminal.request_id.0.to_string()],
        )
        .unwrap();

    let result = store
        .cancel_resource_request_before_activation(
            authority,
            terminal.request_id,
            terminal.task_id,
            resource.id,
            origin,
        )
        .unwrap();
    let QueueCancellationResult::Request(saved) = result else {
        panic!("terminal cancellation must return its saved state");
    };
    assert!(matches!(
        saved.state,
        ResourceRequestState::Finished {
            outcome: ExitReason::Cancelled
        }
    ));
    assert_eq!(identity_count(&store, terminal.task_id), 0);
}

#[test]
fn failed_receipt_write_rolls_back_the_queue_cancellation() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let origin = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let request = RequestId::new();
    let task = TaskId::new();
    store
        .accept_resource_request(authority, request, task, resource.id, origin, spec())
        .unwrap();
    let queued = resource_cancellation_identity(
        Uuid::now_v7(),
        request,
        task,
        resource.id,
        origin,
        authority,
        ResourceRoutePhase::Waiting,
    );
    let unknown = resource_cancellation_identity(
        Uuid::now_v7(),
        RequestId::new(),
        TaskId::new(),
        resource.id,
        origin,
        authority,
        ResourceRoutePhase::AcceptanceUnknown,
    );
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_receipt BEFORE INSERT ON resource_cancellation_receipts
             BEGIN SELECT RAISE(ABORT, 'receipt insert failed'); END;",
        )
        .unwrap();

    // a crash before the receipt must leave no cancellation or prevention behind
    for (identity, phase) in [
        (&queued, ResourceRoutePhase::Waiting),
        (&unknown, ResourceRoutePhase::AcceptanceUnknown),
    ] {
        let proof = resource_cancellation_proof(identity, &spec(), phase);
        assert!(
            store
                .cancel_resource_request_with_receipt(authority, identity.clone(), proof)
                .is_err()
        );
    }
    assert!(matches!(
        store.resource_requests(authority, resource.id).unwrap()[0].state,
        ResourceRequestState::Queued
    ));
    assert_eq!(prevention_count(&store), 0);

    store
        .conn
        .execute_batch("DROP TRIGGER reject_receipt")
        .unwrap();
    let receipt = store
        .cancel_resource_request_with_receipt(
            authority,
            queued.clone(),
            resource_cancellation_proof(&queued, &spec(), ResourceRoutePhase::Waiting),
        )
        .unwrap();
    assert_eq!(
        receipt.outcome,
        ResourceCancellationOutcome::CancelledBeforeLaunch
    );
}
