use serde_json::json;
use tempfile::tempdir;

use super::recovery::{StartupRecoveryAction, startup_recovery_action};
use super::{
    SUPERVISOR_TEST_LOCK, SupervisorActor, SupervisorArgs, SupervisorMsg, callback_executable,
};
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{ProcessStatus, TaskEnv, TaskId, TaskRow, TaskWorkload, Workload};
use crate::error::AppError;
use crate::home::{Home, LockMode, flock_exclusive};
use crate::machine::{MachineId, load_or_create_machine_id};
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::{NewTask, Store, new_queued_task};
use crate::submission::{
    CallbackContext, CallbackExecutable, ExecutionRecord, ExecutorIdentity, OriginRoute, RequestId,
};
use ractor::{Actor, ActorRef};
use std::path::PathBuf;

fn echo_command() -> NormalizedSpec {
    serde_json::from_value(json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "supervisor test",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": { "type": "task", "command": ["/bin/echo", "hello"] }
    }))
    .unwrap()
}

fn command_task_row(id: TaskId, spec: &NormalizedSpec, path: String, binary: &str) -> TaskRow {
    let NormalizedWorkload::Task(workload) = &spec.workload else {
        panic!("supervisor test must use a command workload");
    };

    new_queued_task(NewTask {
        id,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: Workload::Task(TaskWorkload {
            command: workload.command.clone(),
        }),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: TaskEnv {
            path,
            home: "/tmp".into(),
        },
        binary: binary.into(),
    })
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

async fn stop_supervisor(
    supervisor: ActorRef<SupervisorMsg>,
    handle: ractor::concurrency::JoinHandle<()>,
) {
    supervisor.stop(None);
    let _ = handle.await;
}

#[tokio::test]
async fn direct_launch_still_starts_its_command() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let spec = echo_command();
    let id = TaskId::new();
    let row = command_task_row(id, &spec, "/bin".into(), "/bin/echo");
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
        admission: crate::store::LocalAdmission::Submitted {
            request: RequestId::new(),
            after: None,
        },
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
async fn resume_local_starts_a_committed_row_whose_launch_stopped() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let spec = echo_command();
    let id = TaskId::new();
    let row = command_task_row(id, &spec, "/bin".into(), "/bin/echo");
    home.prepare_task(id).unwrap();
    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    // the row commits under its request after startup recovery ran, as when the
    // submit's caller timed out and the supervisor never spawned the worker
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let machine = load_or_create_machine_id(&home).unwrap();
    call(&store, |reply| StoreMsg::InsertLocalTask {
        row: Box::new(row),
        spec: Box::new(spec),
        machine,
        admission: crate::store::LocalAdmission::Submitted {
            request: RequestId::new(),
            after: None,
        },
        codex: CallbackExecutable::available("/bin/echo".into()),
        reply,
    })
    .await
    .unwrap();

    let status = call(&supervisor, |reply| SupervisorMsg::ResumeLocal {
        id,
        reply,
    })
    .await
    .unwrap();
    // the status is read after the launch, so the worker may already have claimed or finished the task
    assert!(
        matches!(
            status,
            ProcessStatus::Queued | ProcessStatus::Running | ProcessStatus::Succeeded
        ),
        "{status:?}"
    );
    let runner_lock = acquire_runner_lock_after_task_exit(&home, id).await;
    assert_eq!(
        std::fs::read_to_string(home.task_paths(id).output).unwrap(),
        "hello\n"
    );
    let status = call(&supervisor, |reply| SupervisorMsg::ResumeLocal {
        id,
        reply,
    })
    .await
    .unwrap();
    assert_eq!(status, ProcessStatus::Succeeded);

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
    let spec = echo_command();
    let id = TaskId::new();
    let row = command_task_row(
        id,
        &spec,
        empty_path.to_string_lossy().into_owned(),
        "/bin/echo",
    );

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
        admission: crate::store::LocalAdmission::Submitted {
            request: RequestId::new(),
            after: None,
        },
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
    let spec = echo_command();
    let id = TaskId::new();
    let row = command_task_row(id, &spec, "/bin".into(), "/bin/echo");
    assert_eq!(
        startup_recovery_action(&row, None, false),
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
        startup_recovery_action(&row, Some(&identity), false),
        StartupRecoveryAction::LaunchAccepted
    );
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

/// Command task whose row and spec match, running `script` under `/bin/sh` in `/tmp`
fn shell_task(script: &str) -> (TaskRow, NormalizedSpec) {
    let spec: NormalizedSpec = serde_json::from_value(json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "released task",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": { "type": "task", "command": ["/bin/sh", "-c", script] }
    }))
    .unwrap();

    let row = command_task_row(TaskId::new(), &spec, "/bin".into(), "/bin/sh");
    (row, spec)
}

/// Save the waiting held route a local submit with `after` saves for this task
fn save_held_local_route(home: &Home, row: &TaskRow, spec: &NormalizedSpec) -> RequestId {
    let machine = load_or_create_machine_id(home).unwrap();
    let request = RequestId::new();
    let route = OriginRoute::new_held(crate::submission::NewHeldRoute {
        request,
        task: row.id,
        origin_machine: machine,
        execution_machine: machine,
        callback: CallbackContext {
            env: row.env.clone(),
            cwd: row.cwd.clone(),
            codex: CallbackExecutable::available("/bin/echo".into()),
        },
        spec: spec.clone(),
    });
    let after = crate::dependency::TaskDependencies::new(vec![TaskId::new()]).unwrap();
    Store::open(&home.db_path())
        .unwrap()
        .insert_origin_route_after(&route, Some(&after))
        .unwrap();
    request
}

async fn release_launch(
    supervisor: &ActorRef<SupervisorMsg>,
    row: &TaskRow,
    spec: &NormalizedSpec,
    request: RequestId,
) -> Result<(), AppError> {
    call(supervisor, |reply| SupervisorMsg::Launch {
        row: Box::new(row.clone()),
        spec: Box::new(spec.clone()),
        admission: crate::store::LocalAdmission::Released { request },
        reply,
    })
    .await
}

#[tokio::test]
async fn startup_launches_a_released_task_whose_worker_never_started() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let (row, spec) = shell_task("echo released");
    let id = row.id;
    let request = save_held_local_route(&home, &row, &spec);
    // the release committed its row, then the daemon stopped before the worker started
    home.prepare_task(id).unwrap();
    Store::open(&home.db_path())
        .unwrap()
        .admit_local_task(
            &row,
            &spec,
            load_or_create_machine_id(&home).unwrap(),
            &crate::store::LocalAdmission::Released { request },
            CallbackExecutable::available("/bin/echo".into()),
        )
        .unwrap();

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let runner_lock = acquire_runner_lock_after_task_exit(&home, id).await;
    assert_eq!(
        std::fs::read_to_string(home.task_paths(id).output).unwrap(),
        "released\n"
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
async fn a_resent_release_leaves_the_running_task_and_its_files_alone() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    configure_task_runner();
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let gate = directory.path().join("gate");
    let (row, spec) = shell_task(&format!(
        "while [ ! -f '{}' ]; do sleep 0.1; done; echo released",
        gate.display()
    ));
    let id = row.id;
    let request = save_held_local_route(&home, &row, &spec);
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

    release_launch(&supervisor, &row, &spec, request)
        .await
        .unwrap();
    // the loop resends a release whose first attempt timed out but still committed
    release_launch(&supervisor, &row, &spec, request)
        .await
        .unwrap();
    assert!(home.task_paths(id).dir.is_dir());

    // a refused launch under the same task UUID keeps the committed task's files too
    let refused = call(&supervisor, |reply| SupervisorMsg::Launch {
        row: Box::new(row.clone()),
        spec: Box::new(spec.clone()),
        admission: crate::store::LocalAdmission::Submitted {
            request: RequestId::new(),
            after: None,
        },
        reply,
    })
    .await;
    assert!(refused.is_err());
    assert!(home.task_paths(id).dir.is_dir());

    std::fs::write(&gate, "").unwrap();
    let runner_lock = acquire_runner_lock_after_task_exit(&home, id).await;
    assert_eq!(
        std::fs::read_to_string(home.task_paths(id).output).unwrap(),
        "released\n"
    );
    let row = call(&store, |reply| StoreMsg::GetTask { id, reply })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status(), ProcessStatus::Succeeded);

    drop(runner_lock);
    stop_supervisor(supervisor, handle).await;
}
