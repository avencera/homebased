//! Registration retries compare the saved first registration, not the current row

use super::fixtures::{machine_other_than, resource};
use crate::domain::ThreadId;
use crate::machine::MachineId;
use crate::resource::store::ResourceStoreError;
use crate::resource::{AssignmentRevision, Resource, SupervisorAddress};
use crate::store::Store;
use tempfile::tempdir;
use uuid::Uuid;

fn replace_supervisor(store: &mut Store, authority: MachineId, machine: MachineId) -> Resource {
    let current = store.resource_snapshots_for_authority(authority).unwrap()[0]
        .resource
        .clone();
    store
        .replace_resource_supervisor(
            authority,
            current.id,
            current.state_revision,
            SupervisorAddress {
                machine,
                thread: ThreadId(Uuid::now_v7()),
            },
        )
        .unwrap()
        .resource
}

#[test]
fn exact_registration_retry_succeeds_after_a_supervisor_replacement() {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let first = resource(authority);
    assert_eq!(store.register_resource(authority, &first).unwrap(), first);
    assert_eq!(store.register_resource(authority, &first).unwrap(), first);

    let replaced = replace_supervisor(&mut store, authority, machine_other_than(authority));
    assert_eq!(replaced.assignment_revision, AssignmentRevision::new(1));

    // the retry names the first supervisor and answers with the current assignment
    assert_eq!(
        store.register_resource(authority, &first).unwrap(),
        replaced
    );
    let models = store
        .resource_read_models(authority, Some(first.id))
        .unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].resource, replaced);

    // a registration that names the current supervisor was never the first one
    let mut current_supervisor = first.clone();
    current_supervisor.supervisor = replaced.supervisor;
    let mut other_supervisor = first.clone();
    other_supervisor.supervisor.thread = ThreadId(Uuid::now_v7());
    let mut other_name = first.clone();
    other_name.display_name = "another GPU".into();
    for changed in [current_supervisor, other_supervisor, other_name] {
        assert!(matches!(
            store.register_resource(authority, &changed),
            Err(ResourceStoreError::RegistrationConflict { resource }) if resource == first.id
        ));
    }
    let other_authority = machine_other_than(authority);
    let moved = Resource::new(
        first.id,
        first.display_name.clone(),
        other_authority,
        first.supervisor,
        first.assignment_revision,
        first.state_revision,
        None,
    );
    assert!(matches!(
        store.register_resource(other_authority, &moved),
        Err(ResourceStoreError::RegistrationConflict { .. })
    ));
    assert_eq!(
        store.resource_snapshots_for_authority(authority).unwrap()[0].resource,
        replaced
    );
}
