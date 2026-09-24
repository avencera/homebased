//! Initial idle attestation for a resource with no history

use tempfile::tempdir;

use super::fixtures::{resource, serving_fixture, spec};
use crate::domain::TaskId;
use crate::machine::MachineId;
use crate::resource::initial_idle::{InitialIdleAttestation, InitialIdleRefusal, ResourceHistory};
use crate::resource::operator_release::{
    OperatorAttestationId, OperatorGpuFreeConfirmation, OperatorObservation,
};
use crate::resource::{
    IdleBoundaryProof, IdleProofGap, Resource, ResourceQueueAttentionReason,
    ResourceQueueReconcileOutcome, ResourceRevision,
};
use crate::store::{InitialIdleError, Store};
use crate::submission::RequestId;

struct FreshResource {
    _directory: tempfile::TempDir,
    store: Store,
    authority: MachineId,
    resource: Resource,
}

fn fresh_resource() -> FreshResource {
    let directory = tempdir().unwrap();
    let mut store = Store::open(&directory.path().join("db")).unwrap();
    let authority = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    FreshResource {
        _directory: directory,
        store,
        authority,
        resource,
    }
}

fn attestation(fixture: &FreshResource, revision: ResourceRevision) -> InitialIdleAttestation {
    InitialIdleAttestation {
        operation_id: OperatorAttestationId::new(),
        resource_id: fixture.resource.id,
        authority_machine: fixture.authority,
        expected_state_revision: revision,
        observation: OperatorObservation::try_from(
            "nvidia-smi on the authority shows no compute processes".to_owned(),
        )
        .unwrap(),
        confirmation: OperatorGpuFreeConfirmation::OperatorConfirmedGpuFree,
    }
}

fn refusal(result: Result<impl std::fmt::Debug, InitialIdleError>) -> InitialIdleRefusal {
    match result {
        Err(InitialIdleError::Refused(refusal)) => refusal,
        other => panic!("expected a typed refusal, got {other:?}"),
    }
}

fn reconcile(fixture: &mut FreshResource) -> ResourceQueueReconcileOutcome {
    fixture
        .store
        .reconcile_resource_queue_for_authority(fixture.authority, fixture.resource.id)
        .unwrap()
}

#[test]
fn a_new_resource_serves_its_first_request_only_after_an_initial_idle_attestation() {
    let mut fixture = fresh_resource();
    let request = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            MachineId::new(),
            spec(),
        )
        .unwrap();
    assert!(matches!(
        reconcile(&mut fixture),
        ResourceQueueReconcileOutcome::AttentionRequired {
            reason: ResourceQueueAttentionReason::IdleNotProven {
                gap: IdleProofGap::NoIdleEvidence
            },
            ..
        }
    ));

    let saved = attestation(&fixture, fixture.resource.state_revision);
    let resolution = fixture
        .store
        .attest_initial_idle_for_authority(fixture.authority, saved.clone())
        .unwrap();
    assert!(!resolution.replayed);
    assert_eq!(resolution.receipt.attestation, saved);
    assert_eq!(
        resolution.receipt.state_revision,
        ResourceRevision::new(fixture.resource.state_revision.get() + 1)
    );

    // an exact retry replays; changed content under the same id conflicts
    let replay = fixture
        .store
        .attest_initial_idle_for_authority(fixture.authority, saved.clone())
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, resolution.receipt);
    let mut changed = saved.clone();
    changed.observation = OperatorObservation::try_from("changed".to_owned()).unwrap();
    assert!(matches!(
        refusal(
            fixture
                .store
                .attest_initial_idle_for_authority(fixture.authority, changed)
        ),
        InitialIdleRefusal::ConflictingRetry { .. }
    ));

    let ResourceQueueReconcileOutcome::IdleServing {
        request: served,
        proof,
        ..
    } = reconcile(&mut fixture)
    else {
        panic!("the attested idle boundary must serve the next request in serving order");
    };
    assert_eq!(served.request_id, request.request_id);
    assert_eq!(
        proof,
        IdleBoundaryProof::OperatorAttestedInitialIdle {
            operation_id: saved.operation_id,
        }
    );

    // the resource now has a loan, so a second initial attestation is refused
    let current = fixture
        .store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()
        .remove(0)
        .resource
        .state_revision;
    assert!(matches!(
        refusal(
            fixture
                .store
                .attest_initial_idle_for_authority(fixture.authority, attestation(&fixture, current))
        ),
        InitialIdleRefusal::AlreadyAttested { operation_id } if operation_id == saved.operation_id
    ));
}

#[test]
fn an_initial_idle_attestation_is_refused_for_the_wrong_state() {
    let mut fixture = fresh_resource();
    let stale = attestation(
        &fixture,
        ResourceRevision::new(fixture.resource.state_revision.get() + 1),
    );
    assert!(matches!(
        refusal(
            fixture
                .store
                .attest_initial_idle_for_authority(fixture.authority, stale)
        ),
        InitialIdleRefusal::StaleRevision { .. }
    ));
    let other_authority = attestation(&fixture, fixture.resource.state_revision);
    assert!(matches!(
        refusal(
            fixture
                .store
                .attest_initial_idle_for_authority(MachineId::new(), other_authority)
        ),
        InitialIdleRefusal::WrongAuthority { .. }
    ));

    // a resource with history decides its idle state from that history
    let mut serving = serving_fixture(true, true);
    let history = InitialIdleAttestation {
        operation_id: OperatorAttestationId::new(),
        resource_id: serving.resource.id,
        authority_machine: serving.authority,
        expected_state_revision: serving.state_revision,
        observation: OperatorObservation::try_from("checked".to_owned()).unwrap(),
        confirmation: OperatorGpuFreeConfirmation::OperatorConfirmedGpuFree,
    };
    assert!(matches!(
        refusal(
            serving
                .store
                .attest_initial_idle_for_authority(serving.authority, history)
        ),
        InitialIdleRefusal::HistoryExists {
            history: ResourceHistory::RegisteredTask
        }
    ));
    let count: i64 = serving
        .store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_initial_idle_attestations",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "a refusal writes nothing");
}
