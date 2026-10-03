use std::path::Path;
use std::sync::{Arc, Barrier};

use tempfile::tempdir;

use super::IdentityError;
use crate::domain::{ProcessStatus, TaskEnv, TaskId};
use crate::machine::MachineId;
use crate::store::Store;
use crate::submission::{
    CallbackContext, ExecutionRecord, ExecutorIdentity, OriginRoute, PreAcceptanceRejection,
    RejectionTombstone, RequestId, SubmissionState,
};
use rusqlite::params;

fn spec() -> crate::spec::NormalizedSpec {
    serde_json::from_value(serde_json::json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "test task",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": {"type": "task", "command": ["echo", "hello"]}
    }))
    .unwrap()
}

fn route() -> OriginRoute {
    let spec = spec();
    OriginRoute {
        request: RequestId::new(),
        task: TaskId::new(),
        origin_machine: MachineId::new(),
        execution_machine: MachineId::new(),
        thread: spec.thread,
        callback: CallbackContext {
            env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
            cwd: Path::new("/tmp").to_path_buf(),
            codex: Path::new("/bin/echo").to_path_buf().into(),
        },
        spec,
        submission: SubmissionState::AcceptanceUnknown,
        last_execution_state: None,
        last_updated_at: chrono::Utc::now(),
        last_accepted_seq: 0,
        last_settled_seq: 0,
    }
}

#[test]
fn request_identity_and_origin_transition() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let mut first = route();
    first.spec.machine = Some("remote-executor".parse().unwrap());
    store.insert_origin_route(&first).unwrap();
    let saved = store.insert_origin_route(&first).unwrap();
    assert_eq!(saved.task, first.task);
    assert_eq!(saved.callback, first.callback);
    let mut changed_origin = first.clone();
    changed_origin.origin_machine = MachineId::new();
    assert!(matches!(
        store.insert_origin_route(&changed_origin),
        Err(IdentityError::Conflict)
    ));
    let mut retry = first.clone();
    retry.task = TaskId::new();
    retry.execution_machine = MachineId::new();
    retry.callback.cwd = Path::new("/other-origin-directory").to_path_buf();
    let saved = store.insert_origin_route(&retry).unwrap();
    assert_eq!(saved.task, first.task);
    assert_eq!(saved.execution_machine, first.execution_machine);
    assert_eq!(saved.callback, first.callback);
    let mut changed_callback = first.clone();
    changed_callback.callback.cwd = Path::new("/other-origin-directory").to_path_buf();
    let saved = store.insert_origin_route(&changed_callback).unwrap();
    assert_eq!(saved.task, first.task);
    assert_eq!(saved.callback, first.callback);
    let mut reused_task = first.clone();
    reused_task.request = RequestId::new();
    assert!(matches!(
        store.insert_origin_route(&reused_task),
        Err(IdentityError::Conflict)
    ));
    let mut changed_content = first.clone();
    changed_content.spec.cwd = Path::new("/different").to_path_buf();
    assert!(matches!(
        store.insert_origin_route(&changed_content),
        Err(IdentityError::Conflict)
    ));
    assert_eq!(
        store
            .origin_route_by_request(first.request)
            .unwrap()
            .unwrap()
            .task,
        first.task
    );
    assert_eq!(
        store
            .origin_route_by_task(first.task)
            .unwrap()
            .unwrap()
            .request,
        first.request
    );
    let resolved = store
        .resolve_origin_route(first.task, SubmissionState::Accepted)
        .unwrap();
    assert!(matches!(resolved.submission, SubmissionState::Accepted));
    assert!(matches!(
        store.resolve_origin_route(
            first.task,
            SubmissionState::Rejected {
                reason: "late".into()
            }
        ),
        Err(IdentityError::Conflict)
    ));
}

#[test]
fn saved_direct_route_json_is_readable_and_recoverable() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    let json = serde_json::to_value(&route).unwrap();
    store
        .conn
        .execute(
            "INSERT INTO origin_routes (request_id,task_id,route_json) VALUES (?1,?2,?3)",
            params![
                route.request.0.to_string(),
                route.task.to_string(),
                serde_json::to_string(&json).unwrap(),
            ],
        )
        .unwrap();

    let saved = store
        .origin_route_by_request(route.request)
        .unwrap()
        .unwrap();
    assert!(matches!(
        saved.submission,
        SubmissionState::AcceptanceUnknown
    ));
    let unknown = store.unknown_origin_routes().unwrap();
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].request, saved.request);
}

#[test]
fn accept_and_abandon_serialize_across_connections() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let task = TaskId::new();
    let origin = MachineId::new();
    let execution = MachineId::new();
    let barrier = Arc::new(Barrier::new(2));
    Store::open(&path).unwrap();
    let left_path = path.clone();
    let left_barrier = barrier.clone();
    let handle = std::thread::spawn(move || {
        let mut store = Store::open(&left_path).unwrap();
        left_barrier.wait();
        store
            .accept_execution(&ExecutionRecord {
                task,
                origin_machine: origin,
                execution_machine: execution,
                spec: spec(),
                state: ProcessStatus::Queued,
            })
            .unwrap()
    });
    let mut store = Store::open(&path).unwrap();
    barrier.wait();
    let abandoned = store
        .abandon_before_acceptance(task, origin, execution)
        .unwrap();
    let accepted = handle.join().unwrap();
    assert_eq!(
        std::mem::discriminant(&abandoned),
        std::mem::discriminant(&accepted)
    );
    let saved = Store::open(&path)
        .unwrap()
        .executor_identity(task)
        .unwrap()
        .unwrap();
    assert_eq!(
        std::mem::discriminant(&saved),
        std::mem::discriminant(&accepted)
    );
}

#[test]
fn concurrent_direct_origin_route_insertions_share_the_first_route() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut first = route();
    first.spec.machine = Some("remote-executor".parse().unwrap());
    let mut contender = first.clone();
    contender.task = TaskId::new();
    contender.execution_machine = MachineId::new();
    contender.callback.cwd = Path::new("/other-origin-directory").to_path_buf();
    Store::open(&path).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let left_path = path.clone();
    let left_barrier = barrier.clone();
    let left_route = first.clone();
    let left = std::thread::spawn(move || {
        let mut store = Store::open(&left_path).unwrap();
        left_barrier.wait();
        store.insert_origin_route(&left_route).unwrap()
    });
    let mut store = Store::open(&path).unwrap();
    barrier.wait();
    let right = store.insert_origin_route(&contender).unwrap();
    let left = left.join().unwrap();

    assert_eq!(left.task, right.task);
    assert_eq!(left.origin_machine, right.origin_machine);
    assert_eq!(left.execution_machine, right.execution_machine);
    assert_eq!(left.callback, right.callback);
    let saved = Store::open(&path)
        .unwrap()
        .origin_route_by_request(first.request)
        .unwrap()
        .unwrap();
    assert_eq!(saved.task, left.task);
    assert_eq!(saved.execution_machine, left.execution_machine);
    assert_eq!(saved.callback, left.callback);
}

#[test]
fn tombstone_survives_reopen_and_blocks_submission() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let task = TaskId::new();
    let origin = MachineId::new();
    let execution = MachineId::new();
    Store::open(&path)
        .unwrap()
        .reject_execution(&RejectionTombstone {
            task,
            origin_machine: origin,
            execution_machine: execution,
            reason: PreAcceptanceRejection::Cancelled.as_str().into(),
        })
        .unwrap();
    let mut reopened = Store::open(&path).unwrap();
    let found = reopened
        .accept_execution(&ExecutionRecord {
            task,
            origin_machine: origin,
            execution_machine: execution,
            spec: spec(),
            state: ProcessStatus::Queued,
        })
        .unwrap();
    assert!(
        matches!(found, ExecutorIdentity::Rejected(t) if t.reason == "cancelled_before_acceptance")
    );
}

#[test]
fn accepted_identity_wins_retries_and_rejects_changed_content() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let mut record = ExecutionRecord {
        task: TaskId::new(),
        origin_machine: MachineId::new(),
        execution_machine: MachineId::new(),
        spec: spec(),
        state: ProcessStatus::Queued,
    };
    store.accept_execution(&record).unwrap();
    record.state = ProcessStatus::Running;
    assert!(
        matches!(store.accept_execution(&record).unwrap(), ExecutorIdentity::Accepted(saved) if saved.state == ProcessStatus::Queued)
    );
    record.spec.cwd = Path::new("/different").to_path_buf();
    assert!(matches!(
        store.accept_execution(&record),
        Err(IdentityError::Conflict)
    ));
}
