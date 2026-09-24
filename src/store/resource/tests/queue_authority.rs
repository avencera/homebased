//! Resource queue authority tests

use super::fixtures::{identity_count, resource, spec};
use crate::domain::TaskId;
use crate::machine::MachineId;
use crate::resource::store::ResourceStoreError;
use crate::store::Store;
use crate::submission::{ExecutionRecord, RejectionTombstone, RequestId};
use tempfile::tempdir;

#[test]
fn queue_operations_require_the_registered_authority() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let other_machine = MachineId::new();
    let resource = resource(authority);

    assert!(matches!(
        store.register_resource(other_machine, &resource),
        Err(ResourceStoreError::WrongAuthority { expected, found })
            if expected == authority && found == other_machine
    ));
    store.register_resource(authority, &resource).unwrap();
    assert_eq!(
        store.register_resource(authority, &resource).unwrap(),
        resource
    );

    let request = RequestId::new();
    let task = TaskId::new();
    let origin = MachineId::new();
    assert!(matches!(
        store.accept_resource_request(
            other_machine,
            request,
            task,
            resource.id,
            origin,
            spec(),
        ),
        Err(ResourceStoreError::WrongAuthority { expected, found })
            if expected == authority && found == other_machine
    ));
    assert!(
        store
            .resource_requests(authority, resource.id)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .oldest_queued_resource_request(authority, resource.id)
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store.cancel_resource_request_before_activation(
            other_machine,
            request,
            task,
            resource.id,
            origin,
        ),
        Err(ResourceStoreError::WrongAuthority { expected, found })
            if expected == authority && found == other_machine
    ));
    assert_eq!(identity_count(&store, task), 0);
}

#[test]
fn queue_acceptance_rejects_task_ids_owned_by_the_executor() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let origin = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();

    let accepted_task = TaskId::new();
    store
        .accept_execution(&ExecutionRecord {
            task: accepted_task,
            origin_machine: origin,
            execution_machine: authority,
            spec: spec().into(),
            state: crate::domain::ProcessStatus::Queued,
        })
        .unwrap();
    let rejected_task = TaskId::new();
    store
        .reject_execution(&RejectionTombstone {
            task: rejected_task,
            origin_machine: origin,
            execution_machine: authority,
            reason: "prior rejection".into(),
        })
        .unwrap();

    for task in [accepted_task, rejected_task] {
        assert!(matches!(
            store.accept_resource_request(
                authority,
                RequestId::new(),
                task,
                resource.id,
                origin,
                spec(),
            ),
            Err(ResourceStoreError::Conflict(_))
        ));
    }
    assert!(
        store
            .resource_requests(authority, resource.id)
            .unwrap()
            .is_empty()
    );
}
