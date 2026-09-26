use crate::resource::{IdleBoundaryProof, RestoreAttentionReason, ReturnLaunch};
use serde_json::json;
use std::io::Write;
use tempfile::tempdir;
use uuid::Uuid;

use super::recovery::{StartupRecoveryAction, startup_recovery_action};
use super::{
    BackgroundLaunch, ReturnDecisionOutcome, SUPERVISOR_TEST_LOCK, SupervisorActor, SupervisorArgs,
    SupervisorMsg, callback_executable,
};
use crate::daemon::actors::resource::ResourceActorInspection;
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId, TaskRow, TaskWorkload,
    ThreadId, Workload,
};
use crate::error::AppError;
use crate::home::{Home, LockMode, flock_exclusive};
use crate::machine::{MachineId, load_or_create_machine_id};
use crate::resource::bound_action::{
    ActionTaskAcceptance, ResourceActionOperation, ResourceActionOutcome, ResourceActionRequest,
};
use crate::resource::command_shape::test_support::FakeTrainer;
use crate::resource::store::{
    OpenReleaseLoanResult, ResourceStoreError, ResourceTaskAcceptance, ResourceTaskAcceptanceInput,
};
use crate::resource::{
    AssignmentRevision, CommandSpec, Loan, LoanPhase, LoanState, Resource, ResourceId,
    ResourceQueueAttentionReason, ResourceQueueReconcileOutcome, ResourceRequestState,
    ResourceRevision, ReturnContext, ReturnDecision, ReturnWork, SupervisorActionAuthority,
    SupervisorAddress,
};
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::{
    BackgroundLaunchAcceptance, BackgroundLaunchError, BackgroundLaunchInput, CancelResult,
    EndedRestoreResolution, NewTask, ReturnTaskAcceptance, ReturnTaskAcceptanceInput,
    ReturnTaskOrigin, Store, new_queued_task,
};
use crate::submission::{
    CallbackContext, CallbackExecutable, ExecutionRecord, ExecutorIdentity, NewResourceRoute,
    OriginRoute, RequestId,
};
use ractor::{Actor, ActorRef};
use std::path::PathBuf;

fn resource(authority: MachineId, background_task: Option<TaskId>) -> Resource {
    Resource::new(
        ResourceId::new(),
        "gpu-0".into(),
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

fn resource_command() -> NormalizedSpec {
    serde_json::from_value(json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "resource startup test",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": { "type": "task", "command": ["/bin/echo", "hello"] }
    }))
    .unwrap()
}

// a native foreground command prints the marker, then blocks on the FIFO
fn resource_command_waiting_on_fifo(fifo: &std::path::Path) -> NormalizedSpec {
    let marker = fifo.with_extension("marker");
    std::fs::write(&marker, "launch-marker\n").unwrap();
    serde_json::from_value(json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "resource one-shot launch test",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": { "type": "task", "command": ["/bin/cat", marker, fifo] }
    }))
    .unwrap()
}

fn seed_active_loan(home: &Home, authority: MachineId) -> (Resource, Loan) {
    let mut store = Store::open(&home.db_path()).unwrap();
    let background_task = TaskId::new();
    let resource = resource(authority, Some(background_task));
    store.register_resource(authority, &resource).unwrap();

    let spec = resource_command();
    let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
        panic!("resource startup seed must use a command workload");
    };
    let row = new_queued_task(NewTask {
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
        binary: "/bin/echo".into(),
    });
    store.insert_task(&row).unwrap();
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
            authority,
            spec,
        )
        .unwrap();
    let OpenReleaseLoanResult::Opened { loan, .. } = store
        .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
        .unwrap()
    else {
        panic!("startup seed must open a release loan");
    };
    store
        .cas_exit(
            background_task,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
        )
        .unwrap()
        .unwrap();

    let saved_resource = store
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource.id)
        .unwrap()
        .resource;
    (saved_resource, loan)
}

fn seed_queued_resource_request(home: &Home, authority: MachineId) -> (Resource, TaskId) {
    let mut store = Store::open(&home.db_path()).unwrap();
    let background_task = TaskId::new();
    let resource = resource(authority, Some(background_task));
    store.register_resource(authority, &resource).unwrap();

    let spec = resource_command();
    let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
        panic!("startup seed must use a command workload");
    };
    let row = new_queued_task(NewTask {
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
        binary: "/bin/echo".into(),
    });
    store.insert_task(&row).unwrap();
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
            authority,
            spec,
        )
        .unwrap();
    home.prepare_task(background_task).unwrap();

    (resource, background_task)
}

fn seed_serving_assigned_resource_task(
    home: &Home,
    authority: MachineId,
    origin: MachineId,
) -> (
    Resource,
    Loan,
    RequestId,
    TaskId,
    ResourceTaskAcceptanceInput,
) {
    seed_serving_assigned_resource_task_with_spec(home, authority, origin, resource_command())
}

fn seed_serving_assigned_resource_task_with_spec(
    home: &Home,
    authority: MachineId,
    origin: MachineId,
    spec: NormalizedSpec,
) -> (
    Resource,
    Loan,
    RequestId,
    TaskId,
    ResourceTaskAcceptanceInput,
) {
    let mut store = Store::open(&home.db_path()).unwrap();
    let background_task = TaskId::new();
    let request_id = RequestId::new();
    let task_id = TaskId::new();
    let mut resource = resource(authority, Some(background_task));
    resource.supervisor.thread = spec.thread;
    store.register_resource(authority, &resource).unwrap();

    let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
        panic!("resource acceptance seed must use a command workload");
    };
    let background = new_queued_task(NewTask {
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
        binary: "/bin/echo".into(),
    });
    store.insert_task(&background).unwrap();
    store
        .cas_status(
            background_task,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();

    if origin == authority {
        store
            .insert_origin_route(
                &OriginRoute::new_resource_waiting(NewResourceRoute {
                    request: request_id,
                    task: task_id,
                    origin_machine: origin,
                    authority_machine: authority,
                    thread: spec.thread,
                    callback: CallbackContext {
                        env: TaskEnv {
                            path: "/bin".into(),
                            home: "/tmp".into(),
                        },
                        cwd: "/tmp".into(),
                        codex: CallbackExecutable::available("/bin/echo".into()),
                    },
                    spec: spec.clone(),
                    resource: resource.id,
                })
                .unwrap(),
            )
            .unwrap();
    }
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
    let OpenReleaseLoanResult::Opened { .. } = store
        .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
        .unwrap()
    else {
        panic!("resource acceptance seed must open one release loan");
    };
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
            request.request_id,
            ReturnContext::AlreadyCompleted {
                task_id: background_task,
                result_ref: "result-1".into(),
            },
        )
        .unwrap();
    let input = ResourceTaskAcceptanceInput {
        authority_machine: authority,
        resource_id: resource.id,
        request_id,
        task_id,
        acceptance_sequence: request.acceptance_sequence,
        loan_id: loan.id,
        expected_state_revision: state_revision,
        command_spec: CommandSpec::try_from(spec).unwrap(),
        executor_env: TaskEnv::capture(),
    };
    home.prepare_task(task_id).unwrap();

    (resource, loan, request_id, task_id, input)
}

fn seed_accepted_resource_task(
    home: &Home,
    authority: MachineId,
    origin: MachineId,
) -> (Resource, Loan, RequestId, TaskId) {
    let (resource, loan, request_id, task_id, input) =
        seed_serving_assigned_resource_task(home, authority, origin);
    assert!(matches!(
        Store::open(&home.db_path())
            .unwrap()
            .accept_assigned_resource_task(input)
            .unwrap(),
        ResourceTaskAcceptance::Inserted { task } if task == task_id
    ));

    (resource, loan, request_id, task_id)
}

fn configure_task_runner() {
    crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
}

async fn acquire_runner_lock_after_task_exit(home: &Home, task_id: TaskId) -> std::fs::File {
    let lock_path = home.task_paths(task_id).runner_lock;
    tokio::task::spawn_blocking(move || flock_exclusive(&lock_path, LockMode::Blocking))
        .await
        .unwrap()
        .unwrap()
}

async fn call_assigned_resource_launch(
    supervisor: &ActorRef<SupervisorMsg>,
    input: ResourceTaskAcceptanceInput,
) -> Result<ResourceTaskAcceptance, ResourceStoreError> {
    call(supervisor, |reply| {
        SupervisorMsg::LaunchAssignedResourceTask {
            input: Box::new(input),
            reply,
        }
    })
    .await
    .unwrap()
}

async fn wait_for_running_task(
    store: &ActorRef<StoreMsg>,
    home: &Home,
    task_id: TaskId,
) -> TaskRow {
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let row = call(store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                .unwrap();
            if row.status() == ProcessStatus::Running && row.pid().is_some() {
                return row;
            }

            tokio::task::yield_now().await;
        }
    })
    .await;
    match result {
        Ok(row) => row,
        Err(_) => {
            let row = call(store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                .unwrap();
            panic!(
                "task {task_id} did not run: state={:?}, reason={:?}, output={:?}",
                row.state,
                row.exit_reason(),
                std::fs::read_to_string(home.task_paths(task_id).output)
            );
        }
    }
}

async fn inspect_resource(
    supervisor: &ActorRef<SupervisorMsg>,
    id: ResourceId,
) -> Option<ResourceActorInspection> {
    call(supervisor, |reply| SupervisorMsg::InspectResource {
        id,
        reply,
    })
    .await
    .unwrap()
}

async fn stop_supervisor(
    supervisor: ActorRef<SupervisorMsg>,
    handle: ractor::concurrency::JoinHandle<()>,
) {
    supervisor.stop(None);
    let _ = handle.await;
}

#[tokio::test]
async fn startup_restores_resource_and_active_loan_snapshot() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (saved_resource, saved_loan) = seed_active_loan(&home, authority);

    let (supervisor, handle) =
        SupervisorActor::spawn(None, SupervisorActor, SupervisorArgs::new(home, None))
            .await
            .unwrap();
    let inspection = inspect_resource(&supervisor, saved_resource.id)
        .await
        .unwrap();

    assert_eq!(inspection.resource, saved_resource);
    assert_eq!(inspection.loan, Some(saved_loan));
    assert!(inspection.actor_id.is_local());

    stop_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn daemon_startup_reconciles_accepted_work_without_relaunching_background_task() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (resource, background_task) = seed_queued_resource_request(&home, authority);
    let runner_lock = flock_exclusive(
        &home.task_paths(background_task).runner_lock,
        LockMode::NonBlocking,
    )
    .unwrap();

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
    assert!(matches!(
        inspection.reconcile_outcome,
        Some(ResourceQueueReconcileOutcome::ReleaseProofUnavailable { .. })
    ));
    assert!(matches!(
        inspection.loan.map(|loan| loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                observed_background_task: saved_task,
                ..
            }
        }) if saved_task == background_task
    ));
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    assert_eq!(
        call(&store, |reply| StoreMsg::PendingSupervisorNotices { reply })
            .await
            .unwrap()
            .unwrap()
            .len(),
        1
    );

    stop_supervisor(supervisor, handle).await;
    drop(runner_lock);
}

#[tokio::test]
async fn restart_defers_accepted_queued_resource_tasks_from_local_and_remote_origins() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    for local_origin in [false, true] {
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let origin = if local_origin {
            authority
        } else {
            MachineId::new()
        };
        let (resource, loan, request_id, task_id) =
            seed_accepted_resource_task(&home, authority, origin);

        let (supervisor, handle) = SupervisorActor::spawn(
            None,
            SupervisorActor,
            SupervisorArgs::new(home.clone(), None),
        )
        .await
        .unwrap();
        let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
        assert!(matches!(
            &inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::AttentionRequired {
                request,
                reason: ResourceQueueAttentionReason::AcceptedTaskLaunchUncertain {
                    task_id: attention_task,
                },
            }) if request.request_id == request_id
                && request.task_id == task_id
                && *attention_task == task_id
        ));
        assert!(matches!(
            &inspection.loan,
            Some(Loan {
                id,
                state: LoanState::Active {
                    phase: LoanPhase::Serving {
                        current_request_id, ..
                    },
                },
                ..
            }) if *id == loan.id && *current_request_id == request_id
        ));

        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status(), ProcessStatus::Queued);
        assert_eq!(row.pid(), None);
        assert!(!home.task_paths(task_id).output.exists());
        assert!(matches!(
            call(&store, |reply| StoreMsg::ResourceRequests {
                authority_machine: authority,
                resource_id: resource.id,
                reply,
            })
            .await
            .unwrap()[0]
                .state,
            ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
        ));

        let runner_lock =
            flock_exclusive(&home.task_paths(task_id).runner_lock, LockMode::NonBlocking).unwrap();
        drop(runner_lock);
        stop_supervisor(supervisor, handle).await;
    }
}

#[tokio::test]
async fn restart_observes_accepted_running_resource_task_without_relaunching_it() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (resource, loan, request_id, task_id) =
        seed_accepted_resource_task(&home, authority, MachineId::new());
    let saved = Store::open(&home.db_path()).unwrap();
    saved
        .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    drop(saved);
    let runner_lock =
        flock_exclusive(&home.task_paths(task_id).runner_lock, LockMode::NonBlocking).unwrap();

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
    assert!(matches!(
        inspection.reconcile_outcome,
        Some(ResourceQueueReconcileOutcome::LoanAlreadyActive { loan: ref active })
            if active.id == loan.id
    ));
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status(), ProcessStatus::Running);
    assert_eq!(row.pid(), None);
    assert!(matches!(
        inspection.loan.map(|active| active.state),
        Some(LoanState::Active {
            phase: LoanPhase::Serving {
                current_request_id: current,
                ..
            },
        }) if current == request_id
    ));

    stop_supervisor(supervisor, handle).await;
    drop(runner_lock);
}

#[tokio::test]
async fn fresh_serving_assignment_starts_one_command_and_retry_only_observes_it() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let fifo = directory.path().join("command-gate");
    nix::unistd::mkfifo(
        &fifo,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();
    let spec = resource_command_waiting_on_fifo(&fifo);
    let (resource, loan, request_id, task_id, input) =
        seed_serving_assigned_resource_task_with_spec(&home, authority, authority, spec);

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let running = wait_for_running_task(&store, &home, task_id).await;
    assert_eq!(running.id, task_id);
    assert!(running.pid().is_some_and(|pid| pid > 0));
    assert!(matches!(
        call_assigned_resource_launch(&supervisor, input.clone()).await,
        Ok(ResourceTaskAcceptance::Existing {
            task,
            state: ProcessStatus::Running,
        }) if task == task_id
    ));

    tokio::task::spawn_blocking(move || {
        let mut writer = std::fs::OpenOptions::new().write(true).open(fifo)?;
        writer.write_all(b"released\n")
    })
    .await
    .unwrap()
    .unwrap();
    let runner_lock = acquire_runner_lock_after_task_exit(&home, task_id).await;
    let output = std::fs::read_to_string(home.task_paths(task_id).output).unwrap();
    assert_eq!(output, "launch-marker\nreleased\n");
    let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status(), ProcessStatus::Succeeded);
    assert!(matches!(
        row.exit_reason(),
        Some(ExitReason::Exit { code: 0 })
    ));
    assert_eq!(
        Store::open(&home.db_path())
            .unwrap()
            .process_group_exit_evidence(task_id)
            .unwrap(),
        Some(ProcessGroupExitEvidence::ConfirmedExited)
    );
    assert_eq!(resource.id, input.resource_id);
    assert_eq!(loan.id, input.loan_id);
    assert_eq!(request_id, input.request_id);
    drop(runner_lock);
    stop_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn existing_queued_assignment_retries_without_launch_even_with_free_lock() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (resource, _loan, request_id, task_id, input) =
        seed_serving_assigned_resource_task(&home, authority, authority);
    assert!(matches!(
        Store::open(&home.db_path())
            .unwrap()
            .accept_assigned_resource_task(input.clone())
            .unwrap(),
        ResourceTaskAcceptance::Inserted { task } if task == task_id
    ));

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
    assert!(matches!(
        inspection.reconcile_outcome,
        Some(ResourceQueueReconcileOutcome::AttentionRequired {
            request,
            reason: ResourceQueueAttentionReason::AcceptedTaskLaunchUncertain {
                task_id: attention_task,
            },
        }) if request.request_id == request_id
            && request.task_id == task_id
            && attention_task == task_id
    ));
    let free_lock =
        flock_exclusive(&home.task_paths(task_id).runner_lock, LockMode::NonBlocking).unwrap();
    drop(free_lock);

    for _ in 0..2 {
        assert!(matches!(
            call_assigned_resource_launch(&supervisor, input.clone()).await,
            Ok(ResourceTaskAcceptance::Existing {
                task,
                state: ProcessStatus::Queued,
            }) if task == task_id
        ));
    }

    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status(), ProcessStatus::Queued);
    assert_eq!(row.pid(), None);
    assert!(!home.task_paths(task_id).output.exists());
    let free_lock =
        flock_exclusive(&home.task_paths(task_id).runner_lock, LockMode::NonBlocking).unwrap();
    drop(free_lock);
    let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
    assert!(matches!(
        inspection.reconcile_outcome,
        Some(ResourceQueueReconcileOutcome::AttentionRequired {
            reason: ResourceQueueAttentionReason::AcceptedTaskLaunchUncertain {
                task_id: attention_task,
            },
            ..
        }) if attention_task == task_id
    ));
    assert!(matches!(
        inspection.loan.map(|saved| saved.state),
        Some(LoanState::Active {
            phase: LoanPhase::Serving {
                current_request_id: current,
                ..
            },
        }) if current == request_id
    ));

    stop_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn cancellation_before_acceptance_prevents_resource_task_spawn() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (resource, _loan, _request_id, task_id, input) =
        seed_serving_assigned_resource_task(&home, authority, authority);
    Store::open(&home.db_path())
        .unwrap()
        .cancel_resource_request_before_activation(
            authority,
            input.request_id,
            task_id,
            resource.id,
            authority,
        )
        .unwrap();

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let result = call(&supervisor, |reply| {
        SupervisorMsg::LaunchAssignedResourceTask {
            input: Box::new(input),
            reply,
        }
    })
    .await
    .unwrap();
    assert!(matches!(result, Err(ResourceStoreError::Prevented)));
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    assert!(
        call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
            .await
            .unwrap()
            .is_none()
    );
    assert!(!home.task_paths(task_id).output.exists());
    assert!(!home.task_paths(task_id).runner_lock.exists());

    stop_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn failed_first_resource_spawn_records_no_child_proof_and_reserves_return() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (resource, loan, _request_id, task_id, _input) =
        seed_serving_assigned_resource_task(&home, authority, authority);
    crate::runner::set_task_run_executable_for_tests(
        directory.path().join("missing-homebased-binary"),
    );

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let row = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(row) = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                && row.status() == ProcessStatus::Failed
            {
                return row;
            }

            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed resource spawn must reach a durable failed state");
    assert_eq!(row.status(), ProcessStatus::Failed);
    assert!(matches!(
        row.exit_reason(),
        Some(ExitReason::SpawnFailed { .. })
    ));
    assert_eq!(
        Store::open(&home.db_path())
            .unwrap()
            .process_group_exit_evidence(task_id)
            .unwrap(),
        Some(ProcessGroupExitEvidence::NoChildSpawned)
    );
    call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource.id,
        reply,
    })
    .await
    .unwrap();
    let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
    assert!(matches!(
        inspection.reconcile_outcome,
        Some(ResourceQueueReconcileOutcome::LoanAlreadyActive {
            loan: Loan {
                id,
                state: LoanState::Active {
                    phase: LoanPhase::AwaitingReturn { .. },
                },
                ..
            }
        }) if id == loan.id
    ));
    let snapshot = call(&store, |reply| StoreMsg::ResourceSnapshotsForAuthority {
        authority_machine: authority,
        reply,
    })
    .await
    .unwrap()
    .into_iter()
    .find(|snapshot| snapshot.resource.id == resource.id)
    .unwrap();
    assert!(matches!(
        snapshot.loan,
        Some(Loan {
            id,
            state: LoanState::Active {
                phase: LoanPhase::AwaitingReturn { .. },
            },
            ..
        }) if id == loan.id
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
            outcome: ExitReason::SpawnFailed { .. }
        }
    ));
    let notices = call(&store, |reply| StoreMsg::PendingSupervisorNotices { reply })
        .await
        .unwrap()
        .unwrap();
    let return_notices = notices
        .iter()
        .filter(|notice| {
            matches!(
                notice.payload,
                crate::resource::SupervisorNoticePayload::ReturnRequired { .. }
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(return_notices.len(), 1);
    assert_eq!(return_notices[0].loan_id, loan.id);

    let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.id, task_id);

    stop_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn direct_launch_still_starts_its_command() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let spec = resource_command();
    let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
        panic!("direct launch test must use a command workload");
    };
    let id = TaskId::new();
    let row = new_queued_task(NewTask {
        id,
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
        binary: "/bin/echo".into(),
    });
    home.prepare_task(id).unwrap();

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    call(&supervisor, |reply| SupervisorMsg::Launch {
        row: Box::new(row),
        spec: Box::new(spec),
        reply,
    })
    .await
    .unwrap();
    let runner_lock = acquire_runner_lock_after_task_exit(&home, id).await;
    assert_eq!(
        std::fs::read_to_string(home.task_paths(id).output).unwrap(),
        "hello\n"
    );
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let row = call(&store, |reply| StoreMsg::GetTask { id, reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status(), ProcessStatus::Succeeded);

    drop(runner_lock);
    stop_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn direct_launch_keeps_an_unresolved_callback_codex_unavailable() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().join("home"))).unwrap();
    home.ensure().unwrap();
    // a PATH with no executables, so no Codex can be found for callbacks
    let empty_path = directory.path().join("empty-bin");
    std::fs::create_dir(&empty_path).unwrap();
    let spec = resource_command();
    let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
        panic!("direct launch test must use a command workload");
    };
    let id = TaskId::new();
    let row = new_queued_task(NewTask {
        id,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: Workload::Task(TaskWorkload {
            command: workload.command,
        }),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: TaskEnv {
            path: empty_path.to_string_lossy().into_owned(),
            home: "/tmp".into(),
        },
        binary: "/bin/echo".into(),
    });
    home.prepare_task(id).unwrap();

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    call(&supervisor, |reply| SupervisorMsg::Launch {
        row: Box::new(row),
        spec: Box::new(spec),
        reply,
    })
    .await
    .unwrap();
    let runner_lock = acquire_runner_lock_after_task_exit(&home, id).await;
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let route = call(&store, |reply| StoreMsg::OriginRoute { id, reply })
        .await
        .unwrap()
        .expect("direct launch saves an origin route");

    assert_eq!(route.callback.codex.path(), None);
    assert!(route.callback.codex.unavailable_reason().is_some());

    drop(runner_lock);
    stop_supervisor(supervisor, handle).await;
}

#[test]
fn direct_queued_tasks_keep_their_existing_startup_actions() {
    let spec = resource_command();
    let id = TaskId::new();
    let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
        panic!("startup test must use a command workload");
    };
    let row = new_queued_task(NewTask {
        id,
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
        binary: "/bin/echo".into(),
    });
    assert_eq!(
        startup_recovery_action(&row, None, false, None),
        StartupRecoveryAction::Observe
    );

    let origin_machine = MachineId::new();
    let mut execution_machine = MachineId::new();
    while execution_machine == origin_machine {
        execution_machine = MachineId::new();
    }
    let identity = ExecutorIdentity::Accepted(ExecutionRecord {
        task: id,
        origin_machine,
        execution_machine,
        spec: spec.into(),
        state: ProcessStatus::Queued,
    });
    assert_eq!(
        startup_recovery_action(&row, None, false, Some(&identity)),
        StartupRecoveryAction::LaunchAccepted
    );
    // a remote supervisor's bound watcher may already have spawned, so it is only observed
    assert_eq!(
        startup_recovery_action(&row, None, true, Some(&identity)),
        StartupRecoveryAction::Observe
    );
}

#[tokio::test]
async fn resource_registration_retry_keeps_one_actor_identity() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let resource = resource(authority, None);

    let (supervisor, handle) =
        SupervisorActor::spawn(None, SupervisorActor, SupervisorArgs::new(home, None))
            .await
            .unwrap();
    let registered = call(&supervisor, |reply| SupervisorMsg::RegisterResource {
        resource: Box::new(resource.clone()),
        reply,
    })
    .await
    .unwrap();
    assert_eq!(registered, resource);
    let before_retry = inspect_resource(&supervisor, resource.id).await.unwrap();

    let retried = call(&supervisor, |reply| SupervisorMsg::RegisterResource {
        resource: Box::new(resource.clone()),
        reply,
    })
    .await
    .unwrap();
    let after_retry = inspect_resource(&supervisor, resource.id).await.unwrap();

    assert_eq!(retried, resource);
    assert_eq!(before_retry.actor_id, after_retry.actor_id);
    assert_eq!(before_retry.resource, after_retry.resource);
    assert_eq!(before_retry.loan, None);

    stop_supervisor(supervisor, handle).await;
}

/// Drain one accepted request so its loan awaits the supervisor return decision
fn seed_awaiting_return(
    home: &Home,
    authority: MachineId,
) -> (Resource, SupervisorActionAuthority) {
    let (resource, _, request_id, task_id, input) = seed_serving_assigned_resource_task_with_spec(
        home,
        authority,
        MachineId::new(),
        resource_command(),
    );
    let (loan_id, expected_state_revision) = (input.loan_id, input.expected_state_revision);
    let mut store = Store::open(&home.db_path()).unwrap();
    assert!(matches!(
        store.accept_assigned_resource_task(input).unwrap(),
        ResourceTaskAcceptance::Inserted { .. }
    ));
    store
        .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    store
        .cas_exit_with_evidence(
            task_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
    let crate::resource::store::AssignedResourceTaskReconcileOutcome::Completed(result) = store
        .reconcile_assigned_resource_task_for_authority(
            crate::resource::store::AssignedResourceTaskReconcileInput {
                authority_machine: authority,
                resource_id: resource.id,
                loan_id,
                request_id,
                task_id,
                expected_state_revision,
            },
        )
        .unwrap()
    else {
        panic!("the drained request must complete");
    };
    let crate::resource::store::ResourceTaskCompletionResult::ReturnRequired {
        loan, notice, ..
    } = *result
    else {
        panic!("the drained queue must reserve the return");
    };
    let authority = SupervisorActionAuthority {
        authority_machine: authority,
        resource_id: resource.id,
        loan_id: loan.id,
        action_id: notice.action_id,
        expected_state_revision: notice.state_revision,
        supervisor: resource.supervisor,
        assignment_revision: resource.assignment_revision,
    };
    (resource, authority)
}

/// Evaluation command that stays in its foreground process group until the gate exists
fn gated_return_launch(
    root: &std::path::Path,
    resource: &Resource,
    marker: &std::path::Path,
    gate: &std::path::Path,
) -> ReturnLaunch {
    let command = crate::resource::foreground::test_support::native_fake_command();
    let spec: NormalizedSpec = serde_json::from_value(json!({
        "api_version": 1,
        "thread": resource.supervisor.thread,
        "name": "gated evaluation",
        "cwd": root,
        "timeout": "4h",
        "workload": { "type": "task", "command": [command, marker, gate] }
    }))
    .unwrap();
    ReturnLaunch {
        request_id: RequestId::new(),
        task_id: TaskId::new(),
        work: ReturnWork::EvaluationOrNextEpoch {
            completed_task: resource.registered_background_task.unwrap(),
            spec: CommandSpec::try_from(spec).unwrap(),
        },
    }
}

async fn decide_return_launch(
    supervisor: &ActorRef<SupervisorMsg>,
    authority: SupervisorActionAuthority,
    launch: ReturnLaunch,
) -> ReturnTaskAcceptance {
    let outcome = call(supervisor, |reply| SupervisorMsg::DecideReturn {
        authority,
        decision: Box::new(ReturnDecision::Launch(Box::new(launch))),
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    let ReturnDecisionOutcome::Launch(acceptance) = outcome else {
        panic!("a launch decision must return its task acceptance");
    };
    acceptance
}

/// Native foreground return that keeps its Restoring loan, with no registration
fn foreground_restoring(inspection: &ResourceActorInspection, task_id: TaskId) -> bool {
    matches!(
        inspection.loan.as_ref().map(|loan| &loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Restoring { resume_task_id, .. }
        }) if *resume_task_id == task_id
    ) && inspection.resource.registered_background_task != Some(task_id)
        && matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::LoanAlreadyActive { .. })
        )
}

#[tokio::test]
async fn native_return_keeps_its_loan_while_running_and_its_end_serves_the_next_request() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let home = Home::resolve(Some(root.join("home"))).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (resource, return_authority) = seed_awaiting_return(&home, authority);
    // a request accepted after the return reservation waits for the next loan;
    // its own gate keeps it serving until the test inspects that loan
    let (later_marker, later_gate) = (root.join("later-marker"), root.join("later-gate"));
    let mut later_spec = resource_command();
    later_spec.workload = NormalizedWorkload::Task(crate::spec::NormalizedTaskWorkload {
        command: crate::invocation::CommandLine::try_from_argv(vec![
            crate::resource::foreground::test_support::native_fake_command()
                .display()
                .to_string(),
            later_marker.display().to_string(),
            later_gate.display().to_string(),
        ])
        .unwrap(),
    });
    let later = Store::open(&home.db_path())
        .unwrap()
        .accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            later_spec,
        )
        .unwrap();
    let (marker, gate) = (root.join("marker"), root.join("gate"));
    let launch = gated_return_launch(&root, &resource, &marker, &gate);
    let task_id = launch.task_id;

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    assert!(matches!(
        decide_return_launch(&supervisor, return_authority, launch.clone()).await,
        ReturnTaskAcceptance::Inserted { task, .. } if task == task_id
    ));
    let running = wait_for_running_task(&store, &home, task_id).await;
    assert_eq!(running.thread, resource.supervisor.thread);

    // the running foreground task never opens a release for the queued request
    call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource.id,
        reply,
    })
    .await
    .unwrap();
    let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
    assert!(
        foreground_restoring(&inspection, task_id),
        "{:?}",
        (&inspection.loan, &inspection.reconcile_outcome)
    );
    let requests = call(&store, |reply| StoreMsg::ResourceRequests {
        authority_machine: authority,
        resource_id: resource.id,
        reply,
    })
    .await
    .unwrap();
    assert!(
        requests
            .iter()
            .any(|request| request.request_id == later.request_id
                && request.state == ResourceRequestState::Queued)
    );

    // an exact retry observes the running task and never respawns it
    assert_eq!(
        decide_return_launch(&supervisor, return_authority, launch).await,
        ReturnTaskAcceptance::Existing {
            task: task_id,
            state: ProcessStatus::Running,
        }
    );
    std::fs::write(&gate, b"").unwrap();
    let ended = wait_for_terminal_task(&store, task_id).await;
    assert_eq!(ended.status(), ProcessStatus::Succeeded);
    assert_eq!(std::fs::read(&marker).unwrap(), b"x");

    // the confirmed end closes the loan and queue reconciliation serves the request
    let inspection = wait_for_inspection(&supervisor, resource.id, |inspection| {
        idle_serving(inspection).is_some()
    })
    .await;
    assert_eq!(
        idle_serving(&inspection),
        Some(&IdleBoundaryProof::ForegroundReturnEnded {
            loan_id: return_authority.loan_id,
            task_id,
        })
    );
    assert!(matches!(
        inspection.loan.as_ref().map(|loan| &loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Serving { current_request_id, .. }
        }) if *current_request_id == later.request_id
    ));
    assert_eq!(inspection.resource.registered_background_task, None);
    std::fs::write(&later_gate, b"").unwrap();
    wait_for_terminal_task(&store, later.task_id).await;
    assert_eq!(std::fs::read(&later_marker).unwrap(), b"x");

    stop_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn startup_keeps_an_unspawned_return_task_reserved_until_explicit_resolution() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let home = Home::resolve(Some(root.join("home"))).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (resource, return_authority) = seed_awaiting_return(&home, authority);
    let (marker, gate) = (root.join("marker"), root.join("gate"));
    let launch = gated_return_launch(&root, &resource, &marker, &gate);
    let task_id = launch.task_id;
    // the binding commits, then the daemon stops before it spawns a worker
    let ReturnTaskAcceptance::Inserted { state_revision, .. } = Store::open(&home.db_path())
        .unwrap()
        .accept_return_task_for_authority(ReturnTaskAcceptanceInput {
            authority: return_authority,
            launch: launch.clone(),
            executor_env: TaskEnv::capture(),
            origin: ReturnTaskOrigin::Local {
                callback_codex: CallbackExecutable::available("/bin/echo".into()),
            },
        })
        .unwrap()
    else {
        panic!("the first exact launch must insert its task");
    };

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let attention = |inspection: &ResourceActorInspection| {
        matches!(
            &inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::RestoreAttentionRequired {
                task_id: attention_task,
                reason: RestoreAttentionReason::LaunchUncertain,
                ..
            }) if *attention_task == task_id
        )
    };
    assert!(attention(
        &inspect_resource(&supervisor, resource.id).await.unwrap()
    ));
    assert_eq!(
        decide_return_launch(&supervisor, return_authority, launch).await,
        ReturnTaskAcceptance::Existing {
            task: task_id,
            state: ProcessStatus::Queued,
        }
    );
    call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource.id,
        reply,
    })
    .await
    .unwrap();
    assert!(attention(
        &inspect_resource(&supervisor, resource.id).await.unwrap()
    ));
    let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status(), ProcessStatus::Queued);
    assert!(!marker.exists());

    // cancelling the queued row proves no child spawned, so the supervisor can resolve it
    assert!(matches!(
        call(&supervisor, |reply| SupervisorMsg::Cancel {
            id: task_id,
            reply
        })
        .await
        .unwrap(),
        CancelResult::CancelledQueued(_)
    ));
    let mut current = return_authority;
    current.expected_state_revision = state_revision;
    let closure = call(&supervisor, |reply| SupervisorMsg::ResolveEndedRestore {
        resolution: Box::new(EndedRestoreResolution {
            authority: current,
            task_id,
            reason: "return launch never started".into(),
        }),
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        closure.loan.state,
        LoanState::Closed {
            result: crate::resource::LoanClosure::RestoreEnded {
                outcome: ExitReason::Cancelled,
                ..
            }
        }
    ));
    call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource.id,
        reply,
    })
    .await
    .unwrap();
    let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
    assert_eq!(inspection.loan, None);
    assert_eq!(inspection.resource.registered_background_task, None);
    assert!(!marker.exists());

    stop_supervisor(supervisor, handle).await;
}

/// Reassign the seeded supervisor thread to another machine
fn move_supervisor_to_another_machine(
    home: &Home,
    resource: &Resource,
    action: &mut SupervisorActionAuthority,
) -> MachineId {
    let mut remote = MachineId::new();
    while remote == action.authority_machine {
        remote = MachineId::new();
    }
    rusqlite::Connection::open(home.db_path())
        .unwrap()
        .execute(
            "UPDATE resources SET supervisor_machine = ?1 WHERE id = ?2",
            rusqlite::params![remote.to_string(), resource.id.as_uuid().to_string()],
        )
        .unwrap();
    action.supervisor.machine = remote;
    remote
}

async fn remote_action(
    supervisor: &ActorRef<SupervisorMsg>,
    action: SupervisorActionAuthority,
    operation: ResourceActionOperation,
) -> ResourceActionOutcome {
    call(supervisor, |reply| SupervisorMsg::ResourceAction {
        request: Box::new(ResourceActionRequest::new(1, action, operation)),
        reply,
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn remote_native_return_spawns_once_and_closes_after_its_confirmed_end() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let home = Home::resolve(Some(root.join("home"))).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (resource, mut action) = seed_awaiting_return(&home, authority);
    let remote = move_supervisor_to_another_machine(&home, &resource, &mut action);
    let (marker, gate) = (root.join("marker"), root.join("gate"));
    let launch = gated_return_launch(&root, &resource, &marker, &gate);
    let task_id = launch.task_id;

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let ResourceActionOutcome::Prepared { task } = remote_action(
        &supervisor,
        action,
        ResourceActionOperation::PrepareReturn {
            launch: launch.clone(),
        },
    )
    .await
    else {
        panic!("the authority must prepare the return task");
    };
    assert_eq!(task.task_id, task_id);
    assert!(task.digest_matches());
    assert!(
        call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
            .await
            .unwrap()
            .is_none()
    );

    let operation = ResourceActionOperation::LaunchReturn {
        launch,
        normalized_spec_sha256: task.normalized_spec_sha256,
    };
    let ResourceActionOutcome::Accepted {
        receipt,
        acceptance: ActionTaskAcceptance::Inserted,
    } = remote_action(&supervisor, action, operation.clone()).await
    else {
        panic!("the first remote launch must insert its task");
    };
    assert_eq!(receipt.origin_machine(), remote);
    wait_for_running_task(&store, &home, task_id).await;
    // the authority keeps no callback route for the remote supervisor's task
    assert!(
        call(&store, |reply| StoreMsg::OriginRoute { id: task_id, reply })
            .await
            .unwrap()
            .is_none()
    );

    call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource.id,
        reply,
    })
    .await
    .unwrap();
    let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
    assert!(
        foreground_restoring(&inspection, task_id),
        "{:?}",
        (&inspection.loan, &inspection.reconcile_outcome)
    );
    assert!(matches!(
        remote_action(&supervisor, action, operation).await,
        ResourceActionOutcome::Accepted {
            acceptance: ActionTaskAcceptance::Existing {
                state: ProcessStatus::Running
            },
            ..
        }
    ));

    std::fs::write(&gate, b"").unwrap();
    wait_for_terminal_task(&store, task_id).await;
    // the retry observed the running worker, so the command ran exactly once
    assert_eq!(std::fs::read(&marker).unwrap(), b"x");
    // the Restoring loan was the only non-closed loan, so its closure leaves none
    let inspection = wait_for_inspection(&supervisor, resource.id, |inspection| {
        inspection.loan.is_none()
    })
    .await;
    assert_eq!(inspection.resource.registered_background_task, None);
    // the remote supervisor keeps its route; the authority adds none
    assert!(
        call(&store, |reply| StoreMsg::OriginRoute { id: task_id, reply })
            .await
            .unwrap()
            .is_none()
    );
    stop_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn remote_return_accepted_before_spawn_is_never_relaunched_after_restart() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let home = Home::resolve(Some(root.join("home"))).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (resource, mut action) = seed_awaiting_return(&home, authority);
    move_supervisor_to_another_machine(&home, &resource, &mut action);
    let (marker, gate) = (root.join("marker"), root.join("gate"));
    let launch = gated_return_launch(&root, &resource, &marker, &gate);
    let task_id = launch.task_id;
    // the remote acceptance commits, then the daemon stops before it spawns a worker
    let mut store = Store::open(&home.db_path()).unwrap();
    let digest = store
        .prepare_return_task_for_authority(action, launch.clone(), TaskEnv::capture())
        .unwrap()
        .normalized_spec_sha256;
    assert!(matches!(
        store
            .accept_return_task_for_authority(ReturnTaskAcceptanceInput {
                authority: action,
                launch: launch.clone(),
                executor_env: TaskEnv::capture(),
                origin: ReturnTaskOrigin::Remote {
                    normalized_spec_sha256: digest,
                },
            })
            .unwrap(),
        ReturnTaskAcceptance::Inserted { .. }
    ));
    drop(store);

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let uncertain = |inspection: &ResourceActorInspection| {
        matches!(
            &inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::RestoreAttentionRequired {
                task_id: attention_task,
                reason: RestoreAttentionReason::LaunchUncertain,
                ..
            }) if *attention_task == task_id
        )
    };
    assert!(uncertain(
        &inspect_resource(&supervisor, resource.id).await.unwrap()
    ));
    // the lost-reply retry observes the queued row and starts nothing
    assert!(matches!(
        remote_action(
            &supervisor,
            action,
            ResourceActionOperation::LaunchReturn {
                launch,
                normalized_spec_sha256: digest,
            },
        )
        .await,
        ResourceActionOutcome::Accepted {
            acceptance: ActionTaskAcceptance::Existing {
                state: ProcessStatus::Queued
            },
            ..
        }
    ));
    call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource.id,
        reply,
    })
    .await
    .unwrap();
    assert!(uncertain(
        &inspect_resource(&supervisor, resource.id).await.unwrap()
    ));
    let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status(), ProcessStatus::Queued);
    assert!(row.pid().is_none());
    assert!(!marker.exists());
    stop_supervisor(supervisor, handle).await;
}

async fn launch_background_task(
    supervisor: &ActorRef<SupervisorMsg>,
    launch: BackgroundLaunch,
) -> Result<BackgroundLaunchAcceptance, BackgroundLaunchError> {
    call(supervisor, |reply| SupervisorMsg::LaunchBackground {
        launch: Box::new(launch),
        reply,
    })
    .await
    .unwrap()
}

fn trainer_launch(
    trainer: &FakeTrainer,
    resource: &Resource,
    request_id: RequestId,
) -> BackgroundLaunch {
    BackgroundLaunch {
        resource_id: resource.id,
        request_id,
        spec: trainer.spec(resource.supervisor.thread, "direct segment trainer"),
        env: trainer.env.clone(),
        callback_cwd: trainer.cwd.clone(),
    }
}

async fn wait_for_inspection(
    supervisor: &ActorRef<SupervisorMsg>,
    id: ResourceId,
    predicate: impl Fn(&ResourceActorInspection) -> bool,
) -> ResourceActorInspection {
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let inspection = inspect_resource(supervisor, id).await.unwrap();
            if predicate(&inspection) {
                return inspection;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    match result {
        Ok(inspection) => inspection,
        Err(_) => panic!(
            "resource never reached the expected state: {:?}",
            inspect_resource(supervisor, id)
                .await
                .map(|inspection| (inspection.loan, inspection.reconcile_outcome))
        ),
    }
}

async fn wait_for_terminal_task(store: &ActorRef<StoreMsg>, task_id: TaskId) -> TaskRow {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let row = call(store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap();
            if let Some(row) = row.filter(|row| row.state.is_terminal()) {
                return row;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}

fn idle_serving(inspection: &ResourceActorInspection) -> Option<&IdleBoundaryProof> {
    match inspection.loan.as_ref().map(|loan| &loan.state) {
        Some(LoanState::Active {
            phase:
                LoanPhase::Serving {
                    return_context: ReturnContext::Idle,
                    release_provenance:
                        crate::resource::ServingReleaseProvenance::IdleBoundary { proof },
                    ..
                },
        }) => Some(proof),
        _ => None,
    }
}

#[tokio::test]
async fn first_background_launch_spawns_once_and_registers_after_its_start_event() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let home = Home::resolve(Some(root.join("home"))).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let trainer = FakeTrainer::new(&root);
    let resource = resource(authority, None);
    Store::open(&home.db_path())
        .unwrap()
        .register_resource(authority, &resource)
        .unwrap();

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let request_id = RequestId::new();
    let BackgroundLaunchAcceptance::Inserted { task, .. } =
        launch_background_task(&supervisor, trainer_launch(&trainer, &resource, request_id))
            .await
            .unwrap()
    else {
        panic!("the first exact launch must insert and spawn its task");
    };
    wait_for_running_task(&store, &home, task).await;

    // the delivered start event wakes the owner; no caller reconcile is needed
    // The event sender is not running here, so send the same inbox hint it sends
    supervisor
        .cast(SupervisorMsg::DispatchInbox { id: task })
        .unwrap();
    wait_for_inspection(&supervisor, resource.id, |inspection| {
        inspection.resource.registered_background_task == Some(task)
    })
    .await;

    // an exact retry observes the running task and never spawns a second one
    assert_eq!(
        launch_background_task(&supervisor, trainer_launch(&trainer, &resource, request_id))
            .await
            .unwrap(),
        BackgroundLaunchAcceptance::Existing {
            task,
            state: ProcessStatus::Running,
        }
    );
    // another request cannot start a competing background task
    assert!(matches!(
        launch_background_task(
            &supervisor,
            trainer_launch(&trainer, &resource, RequestId::new())
        )
        .await,
        Err(BackgroundLaunchError::BackgroundTaskActive { task_id, .. }) if task_id == task
    ));

    std::fs::write(&trainer.gate, b"").unwrap();
    wait_for_terminal_task(&store, task).await;
    assert_eq!(trainer.starts(), 1);

    stop_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn committed_unspawned_background_launch_is_never_respawned_after_restart() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let home = Home::resolve(Some(root.join("home"))).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let trainer = FakeTrainer::new(&root);
    let resource = resource(authority, None);
    let request_id = RequestId::new();
    // the binding commits, then the daemon stops before it spawns a worker
    let (task, later) = {
        let mut store = Store::open(&home.db_path()).unwrap();
        store.register_resource(authority, &resource).unwrap();
        let launch = trainer_launch(&trainer, &resource, request_id);
        let BackgroundLaunchAcceptance::Inserted { task, .. } = store
            .accept_background_launch_for_authority(BackgroundLaunchInput {
                authority_machine: authority,
                resource_id: resource.id,
                request_id,
                task_id: TaskId::new(),
                spec: launch.spec,
                env: launch.env,
                callback_codex: CallbackExecutable::available("/bin/echo".into()),
            })
            .unwrap()
        else {
            panic!("the first exact launch must insert its task");
        };
        let later = store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                MachineId::new(),
                resource_command(),
            )
            .unwrap();
        (task, later)
    };

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let uncertain = |inspection: &ResourceActorInspection| {
        matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::BackgroundLaunchUncertain { task_id })
                if task_id == task
        )
    };
    assert!(uncertain(
        &inspect_resource(&supervisor, resource.id).await.unwrap()
    ));
    assert_eq!(
        launch_background_task(&supervisor, trainer_launch(&trainer, &resource, request_id))
            .await
            .unwrap(),
        BackgroundLaunchAcceptance::Existing {
            task,
            state: ProcessStatus::Queued,
        }
    );
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let inspection = inspect_resource(&supervisor, resource.id).await.unwrap();
    assert!(uncertain(&inspection));
    assert_eq!(inspection.resource.registered_background_task, None);
    assert_eq!(inspection.loan, None);
    let row = call(&store, |reply| StoreMsg::GetTask { id: task, reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status(), ProcessStatus::Queued);
    assert_eq!(trainer.starts(), 0);

    // cancelling the queued row proves no child started, which is the idle proof
    assert!(matches!(
        call(&supervisor, |reply| SupervisorMsg::Cancel {
            id: task,
            reply
        })
        .await
        .unwrap(),
        CancelResult::CancelledQueued(_)
    ));
    call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource.id,
        reply,
    })
    .await
    .unwrap();
    let inspection = wait_for_inspection(&supervisor, resource.id, |inspection| {
        idle_serving(inspection).is_some()
    })
    .await;
    assert_eq!(
        idle_serving(&inspection),
        Some(&IdleBoundaryProof::BackgroundLaunchNeverSpawned {
            request_id,
            task_id: task,
        })
    );
    wait_for_terminal_task(&store, later.task_id).await;
    assert_eq!(trainer.starts(), 0);

    stop_supervisor(supervisor, handle).await;
}

#[tokio::test]
async fn no_resume_closure_runs_a_later_request_from_the_idle_boundary() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let home = Home::resolve(Some(root.join("home"))).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let (resource, return_authority) = seed_awaiting_return(&home, authority);
    let later = Store::open(&home.db_path())
        .unwrap()
        .accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            resource_command(),
        )
        .unwrap();

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    // the reservation stays in place until the supervisor decides
    assert!(matches!(
        inspect_resource(&supervisor, resource.id)
            .await
            .unwrap()
            .loan
            .map(|loan| loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::AwaitingReturn { .. }
        })
    ));
    let outcome = call(&supervisor, |reply| SupervisorMsg::DecideReturn {
        authority: return_authority,
        decision: Box::new(ReturnDecision::NoResume {
            reason: "training is complete".into(),
        }),
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(outcome, ReturnDecisionOutcome::Closed(_)));

    let inspection = wait_for_inspection(&supervisor, resource.id, |inspection| {
        idle_serving(inspection).is_some()
    })
    .await;
    assert_eq!(
        idle_serving(&inspection),
        Some(&IdleBoundaryProof::SupervisorNoResume {
            loan_id: return_authority.loan_id,
        })
    );
    wait_for_terminal_task(&store, later.task_id).await;

    stop_supervisor(supervisor, handle).await;
}

/// Socket-route `submit` on a supervisor that is also the resource authority
mod co_located_action {
    use super::{
        configure_task_runner, gated_return_launch, seed_awaiting_return, stop_supervisor,
        wait_for_running_task, wait_for_terminal_task,
    };
    use crate::daemon::AppState;
    use crate::daemon::actors::supervisor::{
        SUPERVISOR_TEST_LOCK, SupervisorActor, SupervisorArgs, SupervisorMsg,
    };
    use crate::daemon::actors::{StoreMsg, call};
    use crate::daemon::resource_action::submit;
    use crate::domain::{ExitReason, ProcessStatus, TaskEnv, TaskId, ThreadId};
    use crate::error::AppError;
    use crate::files::StreamSlots;
    use crate::fleet::FleetState;
    use crate::fleet::directory::LocalMachine;
    use crate::fleet::protocol::SUPPORTED_PROTOCOLS;
    use crate::home::Home;
    use crate::machine::{LocalIdentity, MachineId, MachineName, load_or_create_machine_id};
    use crate::resource::bound_action::{
        LocalReturnAcceptance, LocalReturnReceipt, ResourceActionChoice, ResourceActionRejection,
        ResourceActionSubmitOutcome,
    };
    use crate::resource::{
        AssignmentRevision, LoanState, ResourceId, ResourceRevision, ReturnDecision,
        SupervisorActionAuthority, SupervisorAddress,
    };
    use crate::store::{
        CancelResult, ReturnTaskAcceptance, ReturnTaskAcceptanceInput, ReturnTaskOrigin, Store,
    };
    use crate::submission::{CallbackExecutable, RequestId};
    use ractor::{Actor, ActorRef};
    use tempfile::tempdir;
    use uuid::Uuid;

    fn app_state(
        home: &Home,
        supervisor: &ActorRef<SupervisorMsg>,
        store: ActorRef<StoreMsg>,
    ) -> AppState {
        AppState {
            home: home.clone(),
            store,
            supervisor: supervisor.clone(),
            web: None,
            content: None,
            stream_slots: StreamSlots::new(),
            machine: LocalMachine {
                identity: LocalIdentity::start(home).unwrap(),
                name: MachineName::fallback(),
                protocol: SUPPORTED_PROTOCOLS,
            },
            fleet: FleetState::Disabled,
            message_receiver: crate::daemon::message_receiver::MessageReceiver::default(),
            locks: crate::daemon::DaemonLocks::default(),
            thread_titles: None,
        }
    }

    fn launch_choice(launch: crate::resource::ReturnLaunch) -> ResourceActionChoice {
        ResourceActionChoice::Return {
            decision: ReturnDecision::Launch(Box::new(launch)),
        }
    }

    fn no_resume(reason: &str) -> ResourceActionChoice {
        ResourceActionChoice::Return {
            decision: ReturnDecision::NoResume {
                reason: reason.into(),
            },
        }
    }

    fn conflicting_retry(outcome: &ResourceActionSubmitOutcome) -> bool {
        matches!(
            outcome,
            ResourceActionSubmitOutcome::Rejected {
                reason: ResourceActionRejection::ConflictingRetry
            }
        )
    }

    #[tokio::test]
    async fn return_launch_spawns_once_replays_and_refuses_other_content() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let home = Home::resolve(Some(root.join("home"))).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, return_authority) = seed_awaiting_return(&home, authority);
        let (marker, gate) = (root.join("marker"), root.join("gate"));
        let launch = gated_return_launch(&root, &resource, &marker, &gate);
        let (request_id, task_id) = (launch.request_id, launch.task_id);

        let (supervisor, handle) = SupervisorActor::spawn(
            None,
            SupervisorActor,
            SupervisorArgs::new(home.clone(), None),
        )
        .await
        .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let state = app_state(&home, &supervisor, store.clone());
        let receipt = LocalReturnReceipt {
            authority: return_authority,
            request_id,
            task_id,
        };

        let first = submit(&state, return_authority, launch_choice(launch.clone()))
            .await
            .unwrap();
        let ResourceActionSubmitOutcome::LocalReturnAccepted {
            receipt: first_receipt,
            acceptance: LocalReturnAcceptance::Inserted { loan, .. },
        } = first
        else {
            panic!("the first co-located launch must insert its task: {first:?}");
        };
        assert_eq!(first_receipt, receipt);
        assert_eq!(loan.id, return_authority.loan_id);
        wait_for_running_task(&store, &home, task_id).await;

        // an exact retry observes the bound task and never spawns it again
        let retry = submit(&state, return_authority, launch_choice(launch.clone()))
            .await
            .unwrap();
        assert!(matches!(
            retry,
            ResourceActionSubmitOutcome::LocalReturnAccepted {
                receipt: retry_receipt,
                acceptance: LocalReturnAcceptance::Existing {
                    state: ProcessStatus::Running
                },
            } if retry_receipt == receipt
        ));

        let mut other = launch;
        other.request_id = RequestId::new();
        other.task_id = TaskId::new();
        let other_task = other.task_id;
        let conflict = submit(&state, return_authority, launch_choice(other))
            .await
            .unwrap();
        assert!(conflicting_retry(&conflict), "{conflict:?}");
        let absent = call(&store, |reply| StoreMsg::GetTask {
            id: other_task,
            reply,
        })
        .await
        .unwrap();
        assert!(absent.is_none());

        // the resource actor owns a co-located watcher, so the socket path refuses one
        let watcher = submit(
            &state,
            return_authority,
            ResourceActionChoice::ReleaseWatcher {
                observed_background_task: task_id,
            },
        )
        .await;
        assert!(matches!(
            watcher,
            Err(AppError::ResourceActionNotAllowed { .. })
        ));

        std::fs::write(&gate, b"").unwrap();
        wait_for_terminal_task(&store, task_id).await;
        assert_eq!(std::fs::read(&marker).unwrap(), b"x");

        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn no_resume_closes_once_and_replays_the_saved_closure() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let home = Home::resolve(Some(root.join("home"))).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (_, return_authority) = seed_awaiting_return(&home, authority);

        let (supervisor, handle) = SupervisorActor::spawn(
            None,
            SupervisorActor,
            SupervisorArgs::new(home.clone(), None),
        )
        .await
        .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let state = app_state(&home, &supervisor, store);

        let first = submit(&state, return_authority, no_resume("training is complete"))
            .await
            .unwrap();
        let ResourceActionSubmitOutcome::Closed {
            loan,
            state_revision,
        } = first
        else {
            panic!("no-resume must close the loan: {first:?}");
        };
        assert_eq!(loan.id, return_authority.loan_id);
        assert!(matches!(loan.state, LoanState::Closed { .. }));

        let retry = submit(&state, return_authority, no_resume("training is complete"))
            .await
            .unwrap();
        assert!(matches!(
            retry,
            ResourceActionSubmitOutcome::Closed {
                loan: ref replayed,
                state_revision: replayed_revision,
            } if *replayed == loan && replayed_revision == state_revision
        ));

        let conflict = submit(&state, return_authority, no_resume("another reason"))
            .await
            .unwrap();
        assert!(conflicting_retry(&conflict), "{conflict:?}");

        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn queued_return_after_restart_is_observed_then_resolved_without_spawn() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        configure_task_runner();
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let home = Home::resolve(Some(root.join("home"))).unwrap();
        home.ensure().unwrap();
        let authority = load_or_create_machine_id(&home).unwrap();
        let (resource, return_authority) = seed_awaiting_return(&home, authority);
        let (marker, gate) = (root.join("marker"), root.join("gate"));
        let launch = gated_return_launch(&root, &resource, &marker, &gate);
        let task_id = launch.task_id;
        // the binding commits, then the daemon stops before it spawns a worker
        let ReturnTaskAcceptance::Inserted { state_revision, .. } = Store::open(&home.db_path())
            .unwrap()
            .accept_return_task_for_authority(ReturnTaskAcceptanceInput {
                authority: return_authority,
                launch: launch.clone(),
                executor_env: TaskEnv::capture(),
                origin: ReturnTaskOrigin::Local {
                    callback_codex: CallbackExecutable::available("/bin/echo".into()),
                },
            })
            .unwrap()
        else {
            panic!("the first exact launch must insert its task");
        };

        let (supervisor, handle) = SupervisorActor::spawn(
            None,
            SupervisorActor,
            SupervisorArgs::new(home.clone(), None),
        )
        .await
        .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let state = app_state(&home, &supervisor, store.clone());

        // the queued return found after restart is only observed
        let observed = submit(&state, return_authority, launch_choice(launch))
            .await
            .unwrap();
        assert!(matches!(
            observed,
            ResourceActionSubmitOutcome::LocalReturnAccepted {
                receipt: LocalReturnReceipt { task_id: observed_task, .. },
                acceptance: LocalReturnAcceptance::Existing {
                    state: ProcessStatus::Queued
                },
            } if observed_task == task_id
        ));
        let row = call(&store, |reply| StoreMsg::GetTask { id: task_id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status(), ProcessStatus::Queued);

        let mut current = return_authority;
        current.expected_state_revision = state_revision;
        let resolve = |reason: &str| ResourceActionChoice::ResolveEndedRestore {
            task_id,
            reason: reason.into(),
        };
        // a queued task has not ended, so resolution is refused without a write
        let early = submit(&state, current, resolve("launch never started"))
            .await
            .unwrap();
        assert!(matches!(
            early,
            ResourceActionSubmitOutcome::Rejected {
                reason: ResourceActionRejection::RestoreNotResolvable { .. }
            }
        ));

        // cancelling the queued row proves no child spawned
        assert!(matches!(
            call(&supervisor, |reply| SupervisorMsg::Cancel {
                id: task_id,
                reply
            })
            .await
            .unwrap(),
            CancelResult::CancelledQueued(_)
        ));
        let closed = submit(&state, current, resolve("launch never started"))
            .await
            .unwrap();
        let ResourceActionSubmitOutcome::Closed { loan, .. } = &closed else {
            panic!("resolution must close the loan: {closed:?}");
        };
        assert_eq!(loan.id, return_authority.loan_id);
        assert!(matches!(
            loan.state,
            LoanState::Closed {
                result: crate::resource::LoanClosure::RestoreEnded {
                    outcome: ExitReason::Cancelled,
                    ..
                }
            }
        ));
        let replay = submit(&state, current, resolve("launch never started"))
            .await
            .unwrap();
        assert!(matches!(
            &replay,
            ResourceActionSubmitOutcome::Closed { loan: replayed, .. } if replayed == loan
        ));
        let conflict = submit(&state, current, resolve("another reason"))
            .await
            .unwrap();
        assert!(conflicting_retry(&conflict), "{conflict:?}");
        assert!(!marker.exists());

        stop_supervisor(supervisor, handle).await;
    }

    #[tokio::test]
    async fn remote_authority_keeps_the_route_path_and_foreign_supervisor_is_refused() {
        let _guard = SUPERVISOR_TEST_LOCK.lock().await;
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().join("home"))).unwrap();
        home.ensure().unwrap();
        let local = load_or_create_machine_id(&home).unwrap();
        let (supervisor, handle) = SupervisorActor::spawn(
            None,
            SupervisorActor,
            SupervisorArgs::new(home.clone(), None),
        )
        .await
        .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let state = app_state(&home, &supervisor, store);
        let remote = SupervisorActionAuthority {
            authority_machine: MachineId::new(),
            resource_id: ResourceId::new(),
            loan_id: crate::resource::LoanId::new(),
            action_id: crate::resource::ActionId::new(),
            expected_state_revision: ResourceRevision::new(1),
            supervisor: SupervisorAddress {
                machine: local,
                thread: ThreadId(Uuid::now_v7()),
            },
            assignment_revision: AssignmentRevision::new(1),
        };

        // a remote watcher still goes through the authority, which is unreachable here
        let watcher = submit(
            &state,
            remote,
            ResourceActionChoice::ReleaseWatcher {
                observed_background_task: TaskId::new(),
            },
        )
        .await;
        assert!(
            matches!(watcher, Err(AppError::MachineUnavailable { machine, .. }) if machine == remote.authority_machine)
        );
        let closure = submit(&state, remote, no_resume("done")).await;
        assert!(matches!(closure, Err(AppError::MachineUnavailable { .. })));

        let mut foreign = remote;
        foreign.supervisor.machine = MachineId::new();
        foreign.authority_machine = local;
        assert!(matches!(
            submit(&state, foreign, no_resume("done")).await,
            Err(AppError::Usage { .. })
        ));

        stop_supervisor(supervisor, handle).await;
    }
}

#[test]
fn unresolved_callback_codex_keeps_the_resolution_error() {
    let error = AppError::ExecutableMissing {
        program: "codex".into(),
    };
    let expected = error.to_string();

    let codex = callback_executable(Err(error));

    let reason = codex.unavailable_reason().expect("unavailable callback");
    assert!(reason.contains(&expected), "reason={reason}");
    assert_eq!(codex.path(), None);
}

#[test]
fn resolved_callback_codex_is_available() {
    let path = PathBuf::from("/bin/codex");

    let codex = callback_executable(Ok(path.clone()));

    assert_eq!(codex, CallbackExecutable::available(path));
}
