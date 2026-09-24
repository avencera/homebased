//! Registration retries compare the saved first registration, not the current row

use super::*;
use crate::resource::store::ResourceStoreError;

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

#[test]
fn schema_24_backfills_only_registrations_that_still_prove_their_first_supervisor() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    let authority = MachineId::new();
    let kept = resource(authority);
    let replaced = resource(authority);
    let replaced_now = {
        let mut store = Store::open(&path).unwrap();
        store.register_resource(authority, &kept).unwrap();
        store.register_resource(authority, &replaced).unwrap();
        let replaced_now = {
            let current = store
                .resource_snapshots_for_authority(authority)
                .unwrap()
                .into_iter()
                .find(|snapshot| snapshot.resource.id == replaced.id)
                .unwrap()
                .resource;
            store
                .replace_resource_supervisor(
                    authority,
                    current.id,
                    current.state_revision,
                    SupervisorAddress {
                        machine: machine_other_than(authority),
                        thread: ThreadId(Uuid::now_v7()),
                    },
                )
                .unwrap()
                .resource
        };
        // a v24 database has resource rows and no registration receipts
        store
            .conn
            .execute_batch(
                "DROP TABLE resource_registration_receipts;
                 PRAGMA user_version = 24;",
            )
            .unwrap();
        replaced_now
    };

    let mut store = Store::open(&path).unwrap();
    let version: i64 = store
        .conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, crate::domain::SCHEMA_VERSION);
    let receipts: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_registration_receipts",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipts, 1);
    let mut rows = store
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .map(|snapshot| snapshot.resource)
        .collect::<Vec<_>>();
    rows.sort_by_key(|resource| resource.id.as_uuid());
    let mut expected = vec![kept.clone(), replaced_now.clone()];
    expected.sort_by_key(|resource| resource.id.as_uuid());
    assert_eq!(rows, expected);

    // an unreplaced row proves its first supervisor, so an exact retry still matches
    assert_eq!(store.register_resource(authority, &kept).unwrap(), kept);
    let mut renamed = kept.clone();
    renamed.display_name = "another GPU".into();
    assert!(matches!(
        store.register_resource(authority, &renamed),
        Err(ResourceStoreError::RegistrationConflict { .. })
    ));
    // a replaced row cannot prove which supervisor came first, so no retry matches
    for retry in [replaced.clone(), {
        let mut current = replaced.clone();
        current.supervisor = replaced_now.supervisor;
        current
    }] {
        assert!(matches!(
            store.register_resource(authority, &retry),
            Err(ResourceStoreError::LegacyRegistrationUnproven { resource })
                if resource == replaced.id
        ));
    }
    assert_eq!(
        store
            .resource_read_models(authority, Some(replaced.id))
            .unwrap()[0]
            .resource,
        replaced_now
    );
}
