use crate::resource::{Loan, SupervisorNoticePayload};
use crate::spec::NormalizedWorkload;
use std::path::PathBuf;

use serde_json::json;
use tempfile::tempdir;
use uuid::Uuid;

use super::test_support::spawn_stub_supervisor;
use super::{
    ResourceActor, ResourceActorInspection, ResourceMsg, resource_actor_name,
    resource_id_from_actor_name,
};
use crate::daemon::actors::{StoreActor, StoreMsg, call};
use crate::domain::{ExitReason, ProcessStatus, TaskEnv, TaskId, TaskWorkload, ThreadId, Workload};
use crate::home::Home;
use crate::machine::{MachineId, load_or_create_machine_id};
use crate::resource::store::{ResourceTaskAcceptance, ResourceTaskAcceptanceInput};
use crate::resource::{
    AssignmentRevision, LoanPhase, LoanState, Resource, ResourceId, ResourceQueueAttentionReason,
    ResourceQueueReconcileOutcome, ResourceRequestState, ResourceRevision, ReturnContext,
    SupervisorAddress,
};
use crate::spec::NormalizedSpec;
use crate::store::{NewTask, Store, new_queued_task};
use crate::submission::RequestId;
use ractor::{Actor, ActorRef};

fn resource(authority: MachineId, background_task: Option<TaskId>) -> Resource {
    Resource::new(
        ResourceId::new(),
        "gpu-test".into(),
        authority,
        SupervisorAddress {
            machine: authority,
            thread: ThreadId(Uuid::now_v7()),
        },
        AssignmentRevision::new(0),
        ResourceRevision::new(0),
        background_task,
    )
}

fn command_spec() -> NormalizedSpec {
    serde_json::from_value(json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "resource reconcile test",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": { "type": "task", "command": ["/bin/echo", "hello"] }
    }))
    .unwrap()
}

fn seed_queue(home: &Home, task_status: Option<ProcessStatus>) -> (MachineId, Resource, RequestId) {
    let authority = load_or_create_machine_id(home).unwrap();
    let background_task = task_status.map(|_| TaskId::new());
    let resource = resource(authority, background_task);
    let mut store = Store::open(&home.db_path()).unwrap();
    store.register_resource(authority, &resource).unwrap();

    if let (Some(task_id), Some(status)) = (background_task, task_status) {
        let spec = command_spec();
        let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
            panic!("resource test must use a command workload");
        };
        let row = new_queued_task(NewTask {
            id: task_id,
            name: Some(spec.name.clone()),
            thread: spec.thread,
            workload: Workload::Task(TaskWorkload {
                command: workload.command,
            }),
            cwd: spec.cwd.clone(),
            timeout: spec.timeout,
            env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
            binary: PathBuf::from("/bin/echo"),
        });
        store.insert_task(&row).unwrap();
        match status {
            ProcessStatus::Queued => {}
            ProcessStatus::Running => {
                store
                    .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
                    .unwrap()
                    .unwrap();
            }
            ProcessStatus::Lost => {
                store
                    .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Lost)
                    .unwrap()
                    .unwrap();
            }
            other => {
                let reason = match other {
                    ProcessStatus::Succeeded | ProcessStatus::Failed => {
                        ExitReason::Exit { code: 0 }
                    }
                    ProcessStatus::Cancelled => ExitReason::Cancelled,
                    ProcessStatus::Queued | ProcessStatus::Running | ProcessStatus::Lost => {
                        unreachable!()
                    }
                };
                store
                    .cas_exit(task_id, ProcessStatus::Queued, &reason)
                    .unwrap()
                    .unwrap();
            }
        }
    }

    let request_id = RequestId::new();
    store
        .accept_resource_request(
            authority,
            request_id,
            TaskId::new(),
            resource.id,
            authority,
            command_spec(),
        )
        .unwrap();
    drop(store);
    (authority, resource, request_id)
}

fn seed_serving_task(home: &Home) -> (MachineId, Resource, RequestId, TaskId) {
    let authority = load_or_create_machine_id(home).unwrap();
    let spec = command_spec();
    let background_task = TaskId::new();
    let request_id = RequestId::new();
    let task_id = TaskId::new();
    let origin = MachineId::new();
    let mut resource = resource(authority, Some(background_task));
    resource.supervisor.thread = spec.thread;
    let mut store = Store::open(&home.db_path()).unwrap();
    store.register_resource(authority, &resource).unwrap();

    let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
        panic!("resource task fixture must use a command workload");
    };
    store
        .insert_task(&new_queued_task(NewTask {
            id: background_task,
            name: Some(spec.name.clone()),
            thread: spec.thread,
            workload: Workload::Task(TaskWorkload {
                command: workload.command,
            }),
            cwd: spec.cwd.clone(),
            timeout: spec.timeout,
            env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
            binary: PathBuf::from("/bin/echo"),
        }))
        .unwrap();
    store
        .cas_status(
            background_task,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    let request = store
        .accept_resource_request(
            authority,
            request_id,
            task_id,
            resource.id,
            origin,
            spec.clone(),
        )
        .unwrap();
    assert!(matches!(
        store
            .open_release_loan_for_authority(authority, resource.id, resource.state_revision,)
            .unwrap(),
        crate::resource::store::OpenReleaseLoanResult::Opened { .. }
    ));
    store
        .cas_exit(
            background_task,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
        )
        .unwrap()
        .unwrap();
    let (loan, state_revision) = store
        .seed_verified_serving_loan_for_test(
            authority,
            resource.id,
            request_id,
            ReturnContext::AlreadyCompleted {
                task_id: background_task,
                result_ref: "actor fixture".into(),
            },
        )
        .unwrap();
    assert_eq!(request.task_id, task_id);
    assert!(matches!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::Serving { current_request_id, .. }
        } if current_request_id == request_id
    ));
    assert!(matches!(
        store
            .accept_assigned_resource_task(ResourceTaskAcceptanceInput {
                authority_machine: authority,
                resource_id: resource.id,
                request_id,
                task_id,
                acceptance_sequence: request.acceptance_sequence,
                loan_id: loan.id,
                expected_state_revision: state_revision,
                command_spec: crate::resource::CommandSpec::try_from(spec).unwrap(),
                executor_env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
            })
            .unwrap(),
        ResourceTaskAcceptance::Inserted { task } if task == task_id
    ));
    store
        .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();

    (authority, resource, request_id, task_id)
}

async fn start_resource_actor(
    home: &Home,
    authority: MachineId,
    resource: Resource,
) -> (
    ActorRef<ResourceMsg>,
    ractor::concurrency::JoinHandle<()>,
    ActorRef<StoreMsg>,
    ractor::concurrency::JoinHandle<()>,
) {
    let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let snapshot = call(&store, |reply| StoreMsg::ResourceSnapshotsForAuthority {
        authority_machine: authority,
        reply,
    })
    .await
    .unwrap()
    .into_iter()
    .find(|snapshot| snapshot.resource.id == resource.id)
    .unwrap();
    let (supervisor, _supervisor_handle) = spawn_stub_supervisor().await;
    let (actor, actor_handle) = ResourceActor::spawn(
        None,
        ResourceActor,
        (
            store.clone(),
            supervisor,
            authority,
            snapshot.resource,
            snapshot.loan,
        ),
    )
    .await
    .unwrap();
    (actor, actor_handle, store, store_handle)
}

async fn stop_actor(actor: ActorRef<ResourceMsg>, handle: ractor::concurrency::JoinHandle<()>) {
    actor.stop(None);
    let _ = handle.await;
}

async fn stop_store(store: ActorRef<StoreMsg>, handle: ractor::concurrency::JoinHandle<()>) {
    store.stop(None);
    let _ = handle.await;
}

#[tokio::test]
async fn exact_terminal_wake_and_actor_restart_finish_one_assigned_task() {
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let (authority, resource, request_id, task_id) = seed_serving_task(&home);

    let (actor, actor_handle, store, store_handle) =
        start_resource_actor(&home, authority, resource.clone()).await;
    let before_exit = call(&actor, |reply| ResourceMsg::Inspect { reply })
        .await
        .unwrap();
    assert!(matches!(
        before_exit.loan,
        Some(Loan {
            state: LoanState::Active {
                phase: LoanPhase::Serving { current_request_id: current, .. }
            },
            ..
        }) if current == request_id
    ));

    let task_store = Store::open(&home.db_path()).unwrap();
    task_store
        .cas_exit_with_evidence(
            task_id,
            ProcessStatus::Running,
            &ExitReason::Cancelled,
            crate::domain::ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
    drop(task_store);

    actor
        .cast(ResourceMsg::TaskTerminal {
            task_id: TaskId::new(),
        })
        .unwrap();
    actor.cast(ResourceMsg::TaskTerminal { task_id }).unwrap();
    let after_exit = call(&actor, |reply| ResourceMsg::Inspect { reply })
        .await
        .unwrap();
    assert!(matches!(
        after_exit.loan,
        Some(Loan {
            state: LoanState::Active {
                phase: LoanPhase::AwaitingReturn { .. }
            },
            ..
        })
    ));
    assert!(matches!(
        call(&store, |reply| StoreMsg::ResourceRequests {
            authority_machine: authority,
            resource_id: resource.id,
            reply,
        })
        .await
        .unwrap()[0]
            .state,
        ResourceRequestState::Finished {
            outcome: ExitReason::Cancelled
        }
    ));
    let notices = call(&store, |reply| StoreMsg::PendingSupervisorNotices { reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        notices
            .iter()
            .filter(|notice| matches!(
                notice.payload,
                SupervisorNoticePayload::ReturnRequired { .. }
            ))
            .count(),
        1
    );
    let committed_revision = after_exit.resource.state_revision;

    stop_actor(actor, actor_handle).await;
    stop_store(store, store_handle).await;

    let (restarted, restarted_handle, reopened_store, reopened_store_handle) =
        start_resource_actor(&home, authority, resource.clone()).await;
    let after_restart = call(&restarted, |reply| ResourceMsg::Inspect { reply })
        .await
        .unwrap();
    assert_eq!(after_restart.resource.state_revision, committed_revision);
    let notices = call(&reopened_store, |reply| {
        StoreMsg::PendingSupervisorNotices { reply }
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        notices
            .iter()
            .filter(|notice| matches!(
                notice.payload,
                SupervisorNoticePayload::ReturnRequired { .. }
            ))
            .count(),
        1
    );

    stop_actor(restarted, restarted_handle).await;
    stop_store(reopened_store, reopened_store_handle).await;
}

#[tokio::test]
async fn actor_startup_recovers_a_queued_request_into_one_release_action() {
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let (authority, resource, request_id) = seed_queue(&home, Some(ProcessStatus::Running));

    let (actor, actor_handle, store, store_handle) =
        start_resource_actor(&home, authority, resource.clone()).await;
    let inspection = call(&actor, |reply| ResourceMsg::Inspect { reply })
        .await
        .unwrap();
    let ResourceQueueReconcileOutcome::ReleaseProofUnavailable { loan, .. } =
        inspection.reconcile_outcome.unwrap()
    else {
        panic!("startup must retain the running task behind the release proof gate");
    };
    assert!(matches!(
        &loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                observed_background_task,
                ..
            }
        } if Some(*observed_background_task) == resource.registered_background_task
    ));
    assert_eq!(inspection.loan, Some(loan));
    assert_eq!(inspection.resource.state_revision, ResourceRevision::new(1));
    assert_eq!(
        call(&store, |reply| StoreMsg::ResourceRequests {
            authority_machine: authority,
            resource_id: resource.id,
            reply,
        })
        .await
        .unwrap()[0]
            .request_id,
        request_id
    );

    stop_actor(actor, actor_handle).await;
    stop_store(store, store_handle).await;
}

#[tokio::test]
async fn duplicate_wakes_keep_one_loan_and_one_notice() {
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let (authority, resource, _) = seed_queue(&home, Some(ProcessStatus::Running));

    let (actor, actor_handle, store, store_handle) =
        start_resource_actor(&home, authority, resource.clone()).await;
    let first = call(&actor, |reply| ResourceMsg::Inspect { reply })
        .await
        .unwrap();
    let first_loan = first.loan.unwrap();
    let first_loan_id = first_loan.id;

    for _ in 0..2 {
        let result = call(&actor, |reply| ResourceMsg::Reconcile { reply })
            .await
            .unwrap();
        assert!(matches!(
            result,
            ResourceQueueReconcileOutcome::ReleaseProofUnavailable { loan, .. }
                if loan.id == first_loan_id
        ));
    }

    let notices = call(&store, |reply| StoreMsg::PendingSupervisorNotices { reply })
        .await
        .unwrap();
    let notices = notices.unwrap();
    assert_eq!(notices.len(), 1);
    let snapshot = call(&actor, |reply| ResourceMsg::Inspect { reply })
        .await
        .unwrap();
    assert_eq!(snapshot.loan.unwrap().id, first_loan_id);

    stop_actor(actor, actor_handle).await;
    stop_store(store, store_handle).await;
}

#[tokio::test]
async fn uncertain_background_state_stays_queued_and_is_inspectable() {
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let (authority, resource, request_id) = seed_queue(&home, Some(ProcessStatus::Lost));

    let (actor, actor_handle, store, store_handle) =
        start_resource_actor(&home, authority, resource.clone()).await;
    let inspection = call(&actor, |reply| ResourceMsg::Inspect { reply })
        .await
        .unwrap();
    assert!(inspection.loan.is_none());
    assert!(matches!(
        inspection.reconcile_outcome,
        Some(ResourceQueueReconcileOutcome::AttentionRequired {
            request,
            reason: ResourceQueueAttentionReason::BackgroundTaskNotRunning {
                state,
                ..
            },
        }) if request.request_id == request_id && state == "lost"
    ));
    let requests = call(&store, |reply| StoreMsg::ResourceRequests {
        authority_machine: authority,
        resource_id: resource.id,
        reply,
    })
    .await
    .unwrap();
    assert!(matches!(requests[0].state, ResourceRequestState::Queued));
    assert!(
        Store::open(&home.db_path())
            .unwrap()
            .next_queued_resource_request(authority, resource.id)
            .unwrap()
            .is_some()
    );

    stop_actor(actor, actor_handle).await;
    stop_store(store, store_handle).await;
}

#[tokio::test]
async fn ended_unregistered_first_launch_stays_reserved_across_restart_until_attested() {
    use crate::resource::command_shape::test_support::FakeTrainer;
    use crate::resource::operator_release::{
        OperatorAttestationId, OperatorGpuFreeAttestation, OperatorGpuFreeConfirmation,
        OperatorObservation, OperatorStateBinding,
    };
    use crate::store::{BackgroundLaunchAcceptance, BackgroundLaunchInput};
    use crate::submission::CallbackExecutable;

    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().join("state"))).unwrap();
    home.ensure().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let trainer = FakeTrainer::new(&root);
    let authority = load_or_create_machine_id(&home).unwrap();
    let resource = resource(authority, None);
    let (request_id, task_id) = (RequestId::new(), TaskId::new());
    {
        // the fake runner starts and ends the launch before any owner observes its start
        let mut store = Store::open(&home.db_path()).unwrap();
        store.register_resource(authority, &resource).unwrap();
        let accepted = store
            .accept_background_launch_for_authority(BackgroundLaunchInput {
                authority_machine: authority,
                resource_id: resource.id,
                request_id,
                task_id,
                spec: trainer.spec(resource.supervisor.thread, "direct segment trainer"),
                env: trainer.env.clone(),
                callback_codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
            })
            .unwrap();
        assert!(matches!(
            accepted,
            BackgroundLaunchAcceptance::Inserted { .. }
        ));
        store
            .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();
        store
            .cas_exit(
                task_id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 1 },
            )
            .unwrap()
            .unwrap();
    }
    let unproven = |inspection: &ResourceActorInspection| {
        inspection.resource.registered_background_task.is_none()
            && inspection.loan.is_none()
            && matches!(
                inspection.reconcile_outcome,
                Some(ResourceQueueReconcileOutcome::BackgroundLaunchReleaseUnproven {
                    request_id: request,
                    task_id: task,
                }) if request == request_id && task == task_id
            )
    };

    // an empty queue is not an idle resource, before and after a restart
    for _ in 0..2 {
        let (actor, actor_handle, store, store_handle) =
            start_resource_actor(&home, authority, resource.clone()).await;
        let inspection = call(&actor, |reply| ResourceMsg::Inspect { reply })
            .await
            .unwrap();
        assert!(unproven(&inspection), "{:?}", inspection.reconcile_outcome);
        stop_actor(actor, actor_handle).await;
        stop_store(store, store_handle).await;
    }

    let (actor, actor_handle, store, store_handle) =
        start_resource_actor(&home, authority, resource.clone()).await;
    let revision = call(&actor, |reply| ResourceMsg::Inspect { reply })
        .await
        .unwrap()
        .resource
        .state_revision;
    let attestation = OperatorGpuFreeAttestation {
        operation_id: OperatorAttestationId::new(),
        resource_id: resource.id,
        authority_machine: authority,
        task_id,
        expected_state_revision: revision,
        state_binding: OperatorStateBinding::FirstBackgroundLaunch { request_id },
        observation: OperatorObservation::try_from(
            "nvidia-smi on the authority lists no trainer process".to_owned(),
        )
        .unwrap(),
        confirmation: OperatorGpuFreeConfirmation::OperatorConfirmedGpuFree,
    };
    call(&store, |reply| StoreMsg::AttestTrainerGpuFreeForAuthority {
        authority_machine: authority,
        attestation: Box::new(attestation),
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    let outcome = call(&actor, |reply| ResourceMsg::Reconcile { reply })
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        ResourceQueueReconcileOutcome::NoQueuedRequest
    ));
    stop_actor(actor, actor_handle).await;
    stop_store(store, store_handle).await;
}

#[test]
fn resource_ids_round_trip_in_actor_names() {
    let id = ResourceId::new();
    assert_eq!(
        resource_id_from_actor_name(Some(resource_actor_name(id))),
        Some(id)
    );
    assert!(resource_id_from_actor_name(Some("homebased.resource.invalid".into())).is_none());
}

#[tokio::test]
async fn a_fired_deadline_wake_no_longer_covers_its_deadline() {
    let target = super::ReturnWakeTarget::Deadline {
        action_id: crate::resource::ActionId::new(),
        deadline_at: chrono::Utc::now(),
    };
    let pending = tokio::spawn(std::future::pending::<()>());
    let armed = super::ReturnDeadlineWake::new(target, pending.abort_handle());
    assert!(armed.covers(target));
    assert!(!armed.covers(super::ReturnWakeTarget::Retry));

    // a wake that fired before the wall-clock deadline must let the next reconcile arm again
    let fired = tokio::spawn(async {});
    let handle = fired.abort_handle();
    fired.await.unwrap();
    let fired = super::ReturnDeadlineWake::new(target, handle);
    assert!(!fired.covers(target));
}
