use super::{CancelResult, NewTask, Store, new_queued_task, read_exit_json, write_exit_json};
use crate::callback::EventKind;
use crate::daemon::api::views::TaskSummary;
use crate::domain::{
    Agent, AgentKind, AgentWorkload, CallbackStatus, ContainerExitEvidence, ContainerId,
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, ReportOutcome, SUMMARY_MAX_BYTES, TaskEnv,
    TaskExitEvidence, TaskId, TaskName, TaskRow, TaskState, TaskWorkload, ThreadId, Workload,
};
use crate::error::AppError;
use crate::events::{DeliveryOutcome, EventPayload};
use crate::invocation::CommandLine;
use crate::machine::MachineId;
use crate::spec::NormalizedSpec;
use crate::submission::{CallbackExecutable, ExecutorIdentity};
use rusqlite::params;
use serde_json::json;
use std::num::NonZeroU64;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use tempfile::tempdir;

fn agent_row(id: TaskId) -> TaskRow {
    new_queued_task(NewTask {
        id,
        name: TaskName::parse("agent job").unwrap(),
        thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
        workload: Workload::Agent(AgentWorkload {
            agent: Agent::new(AgentKind::Claude, Some("fable".into())),
            extra_args: vec!["--verbose".into()],
            report_trailer: true,
            resume_thread: None,
        }),
        cwd: Path::new("/tmp").to_path_buf(),
        timeout: Duration::from_secs(4 * 3600),
        env: TaskEnv {
            path: "/bin".into(),
            home: "/home/u".into(),
        },
        binary: Path::new("/bin/true").to_path_buf(),
    })
}

fn task_row(id: TaskId) -> TaskRow {
    new_queued_task(NewTask {
        id,
        name: TaskName::parse("command job").unwrap(),
        thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
        workload: Workload::Task(TaskWorkload {
            command: CommandLine::try_from_argv(vec![
                "cargo".into(),
                "build".into(),
                "--release".into(),
            ])
            .unwrap(),
        }),
        cwd: Path::new("/tmp").to_path_buf(),
        timeout: Duration::from_secs(4 * 3600),
        env: TaskEnv {
            path: "/bin".into(),
            home: "/home/u".into(),
        },
        binary: Path::new("/bin/cargo").to_path_buf(),
    })
}

fn local_spec(row: &TaskRow) -> NormalizedSpec {
    serde_json::from_value(serde_json::json!({
        "api_version": 1,
        "thread": row.thread,
        "name": "local task",
        "cwd": row.cwd,
        "timeout": "4h",
        "workload": { "type": "task", "command": ["true"] }
    }))
    .unwrap()
}

fn insert_local(store: &Store, id: TaskId) {
    let mut row = task_row(id);
    row.name = TaskName::parse("local task").unwrap();
    let spec = local_spec(&row);
    store
        .insert_local_task(
            &row,
            &spec,
            MachineId::new(),
            crate::submission::RequestId::new(),
            CallbackExecutable::available("/bin/true".into()),
        )
        .unwrap();
}

#[test]
fn terminal_process_group_evidence_survives_restart_with_its_event() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    let id = TaskId::new();
    {
        let store = Store::open(&path).unwrap();
        insert_local(&store, id);
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        store
            .cas_exit_with_evidence(
                id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
                ProcessGroupExitEvidence::ConfirmedExited,
                None,
            )
            .unwrap();
    }

    let store = Store::open(&path).unwrap();
    let row = store.require_task(id).unwrap();
    assert_eq!(row.status(), ProcessStatus::Succeeded);
    assert_eq!(
        row.process_group_exit_evidence(),
        ProcessGroupExitEvidence::ConfirmedExited
    );
    assert_eq!(
        store
            .get_task(id)
            .unwrap()
            .map(|row| row.process_group_exit_evidence()),
        Some(ProcessGroupExitEvidence::ConfirmedExited)
    );
    let terminal_events = store
        .pending_outbound_events(id)
        .unwrap()
        .into_iter()
        .filter(|event| {
            matches!(
                &event.event.payload,
                EventPayload::Callback {
                    state: Some(ProcessStatus::Succeeded),
                    ..
                }
            )
        })
        .count();
    assert_eq!(terminal_events, 1);
}

#[test]
fn terminal_evidence_rolls_back_when_its_event_cannot_commit() {
    let directory = tempdir().unwrap();
    let store = Store::open(&directory.path().join("db")).unwrap();
    let id = TaskId::new();
    insert_local(&store, id);
    store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_terminal_event BEFORE INSERT ON executor_outbox
             WHEN NEW.seq = 3
             BEGIN SELECT RAISE(ABORT, 'terminal event unavailable'); END;",
        )
        .unwrap();

    assert!(
        store
            .cas_exit_with_evidence(
                id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
                ProcessGroupExitEvidence::ConfirmedExited,
                None,
            )
            .is_err()
    );

    let row = store.require_task(id).unwrap();
    assert_eq!(row.status(), ProcessStatus::Running);
    assert_eq!(row.exit_reason(), None);
    assert_eq!(
        row.process_group_exit_evidence(),
        ProcessGroupExitEvidence::Unconfirmed
    );
    assert_eq!(store.pending_outbound_events(id).unwrap().len(), 2);
}

#[test]
fn queued_cancel_records_that_no_child_was_spawned() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    let id = TaskId::new();
    {
        let store = Store::open(&path).unwrap();
        store.insert_task(&agent_row(id)).unwrap();
        let CancelResult::CancelledQueued(row) = store.request_cancel(id).unwrap() else {
            panic!("queued task should cancel before a worker can spawn its child");
        };
        assert_eq!(
            row.process_group_exit_evidence(),
            ProcessGroupExitEvidence::NoChildSpawned
        );
    }

    let store = Store::open(&path).unwrap();
    assert_eq!(
        store
            .get_task(id)
            .unwrap()
            .map(|row| row.process_group_exit_evidence()),
        Some(ProcessGroupExitEvidence::NoChildSpawned)
    );
}

#[test]
fn exit_json_round_trips_typed_process_group_evidence() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("exit.json");
    write_exit_json(
        &path,
        &ExitReason::Cancelled,
        ProcessGroupExitEvidence::ConfirmedExited,
    )
    .unwrap();

    assert_eq!(
        read_exit_json(&path)
            .unwrap()
            .unwrap()
            .process_group_exit_evidence,
        ProcessGroupExitEvidence::ConfirmedExited
    );
}

fn deliver_outbound_events(
    store: &mut Store,
    id: TaskId,
    mut outcome_for_seq: impl FnMut(u64) -> DeliveryOutcome,
) {
    let mut seq = 1_u64;
    while let Some(outbox) = store
        .outbound_event_at_or_after(id, NonZeroU64::new(seq).unwrap())
        .unwrap()
    {
        assert_eq!(outbox.event.seq.get(), seq);
        store.accept_inbound_event(&outbox.event).unwrap();
        if outbox.event.payload.notification_required() {
            store
                .reserve_inbox_attempt(id, outbox.event.seq)
                .unwrap()
                .expect("callback attempt reservation");
            store
                .settle_inbox_attempt(id, outbox.event.seq, outcome_for_seq(seq))
                .unwrap();
        }
        seq += 1;
    }
}

fn row_at(id: TaskId, cwd: &Path) -> (TaskRow, NormalizedSpec) {
    let mut row = task_row(id);
    row.name = TaskName::parse("local task").unwrap();
    row.cwd = cwd.to_path_buf();
    let spec = local_spec(&row);
    (row, spec)
}

fn insert_local_at(store: &Store, id: TaskId, cwd: &Path, machine: MachineId) {
    let (row, spec) = row_at(id, cwd);
    store
        .insert_local_task(
            &row,
            &spec,
            machine,
            crate::submission::RequestId::new(),
            CallbackExecutable::available("/bin/true".into()),
        )
        .unwrap();
}

#[test]
fn project_metadata_uses_the_nearest_nested_git_root() {
    let dir = tempdir().unwrap();
    let repository = dir.path().join("project");
    let cwd = repository.join("packages/app/src");
    std::fs::create_dir_all(cwd.as_path()).unwrap();
    std::fs::create_dir(repository.join(".git")).unwrap();

    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    insert_local_at(&store, id, &cwd, MachineId::new());

    let presentation = store
        .task_presentations(&[id])
        .unwrap()
        .remove(&id)
        .unwrap();
    assert_eq!(
        presentation.project_root.as_deref(),
        Some(repository.as_path())
    );
}

#[test]
fn project_metadata_recognizes_linked_worktree_git_files() {
    let dir = tempdir().unwrap();
    let worktree = dir.path().join("worktree");
    let cwd = worktree.join("nested");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::write(
        worktree.join(".git"),
        "gitdir: /some/repository/worktrees/topic\n",
    )
    .unwrap();

    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    insert_local_at(&store, id, &cwd, MachineId::new());

    let presentation = store
        .task_presentations(&[id])
        .unwrap()
        .remove(&id)
        .unwrap();
    assert_eq!(
        presentation.project_root.as_deref(),
        Some(worktree.as_path())
    );
}

#[test]
fn project_metadata_keeps_cwd_fallback_and_serializes_accepted_owners() {
    let dir = tempdir().unwrap();
    let local_cwd = dir.path().join("standalone");
    let remote_cwd = dir.path().join("remote-project/nested");
    std::fs::create_dir_all(&local_cwd).unwrap();
    std::fs::create_dir_all(&remote_cwd).unwrap();
    std::fs::create_dir(remote_cwd.parent().unwrap().join(".git")).unwrap();

    let store = Store::open(&dir.path().join("db")).unwrap();
    let local_id = TaskId::new();
    let local_machine = MachineId::new();
    insert_local_at(&store, local_id, &local_cwd, local_machine);

    let remote_id = TaskId::new();
    let origin_machine = MachineId::new();
    let execution_machine = MachineId::new();
    let (remote_row, remote_spec) = row_at(remote_id, &remote_cwd);
    store
        .insert_remote_task(&remote_row, &remote_spec, origin_machine, execution_machine)
        .unwrap();

    // a row with neither identity nor route, the shape of a queue run
    let run_id = TaskId::new();
    let run_row = task_row(run_id);
    store.insert_task(&run_row).unwrap();

    let presentations = store
        .task_presentations(&[local_id, remote_id, run_id])
        .unwrap();
    let local = presentations.get(&local_id).unwrap();
    assert_eq!(local.project_root, None);
    assert_eq!(local.owners.unwrap().origin_machine, local_machine);
    assert_eq!(local.owners.unwrap().execution_machine, local_machine);
    let local_json = serde_json::to_value(TaskSummary::from_row(
        &store.require_task(local_id).unwrap(),
        Some(local),
    ))
    .unwrap();
    assert!(local_json.get("project_root").is_none());
    assert_eq!(local_json["cwd"], json!(local_cwd));
    assert_eq!(local_json["origin_machine"], json!(local_machine));
    assert_eq!(local_json["execution_machine"], json!(local_machine));

    let remote = presentations.get(&remote_id).unwrap();
    assert_eq!(remote.project_root.as_deref(), remote_cwd.parent());
    assert_eq!(remote.owners.unwrap().origin_machine, origin_machine);
    assert_eq!(remote.owners.unwrap().execution_machine, execution_machine);
    let remote_json = serde_json::to_value(TaskSummary::from_row(
        &store.require_task(remote_id).unwrap(),
        Some(remote),
    ))
    .unwrap();
    assert_eq!(remote_json["project_root"], json!(remote_cwd.parent()));
    assert_eq!(remote_json["origin_machine"], json!(origin_machine));
    assert_eq!(remote_json["execution_machine"], json!(execution_machine));
    // the origin, not this executor, delivers the callback
    assert_eq!(remote_json["callback"], json!(null));

    let run = presentations.get(&run_id).unwrap();
    assert_eq!(run.owners, None);
    let run_json = serde_json::to_value(TaskSummary::from_row(&run_row, Some(run))).unwrap();
    assert!(run_json.get("origin_machine").is_none());
    assert!(run_json.get("execution_machine").is_none());
    assert_eq!(run_json["callback"], json!(null));
}

#[test]
fn project_metadata_is_not_recomputed_after_acceptance() {
    let dir = tempdir().unwrap();
    let repository = dir.path().join("project");
    let cwd = repository.join("nested");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::create_dir(repository.join(".git")).unwrap();

    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    insert_local_at(&store, id, &cwd, MachineId::new());
    let moved_repository = dir.path().join("moved-project");
    std::fs::rename(&repository, &moved_repository).unwrap();

    let presentation = store
        .task_presentations(&[id])
        .unwrap()
        .remove(&id)
        .unwrap();
    assert_eq!(
        presentation.project_root.as_deref(),
        Some(repository.as_path())
    );
    assert!(!repository.exists());
    assert!(moved_repository.exists());
}

#[test]
fn remote_acceptance_rolls_back_with_event_and_spawn_failure_retains_state() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    let origin = MachineId::new();
    let execution = MachineId::new();
    let mut row = task_row(id);
    row.name = TaskName::parse("local task").unwrap();
    let spec = local_spec(&row);
    row.workload = crate::invocation::persist_workload(&spec.workload);
    row.binary = Path::new("/bin/true").into();
    store.conn.execute_batch("CREATE TRIGGER reject_outbox BEFORE INSERT ON executor_outbox BEGIN SELECT RAISE(ABORT, 'event insert failed'); END;").unwrap();
    assert!(
        store
            .insert_remote_task(&row, &spec, origin, execution)
            .is_err()
    );
    assert!(store.get_task(id).unwrap().is_none());
    assert!(store.executor_identity(id).unwrap().is_none());
    store
        .conn
        .execute_batch("DROP TRIGGER reject_outbox")
        .unwrap();

    let accepted = store
        .insert_remote_task(&row, &spec, origin, execution)
        .unwrap();
    assert!(matches!(accepted, ExecutorIdentity::Accepted(_)));
    assert!(store.is_event_task(id).unwrap());
    store
        .cas_exit(
            id,
            ProcessStatus::Queued,
            &ExitReason::SpawnFailed {
                message: "failed to fork".into(),
            },
        )
        .unwrap();
    assert!(
        matches!(store.executor_identity(id).unwrap(), Some(ExecutorIdentity::Accepted(record))
        if record.state == ProcessStatus::Failed)
    );
    let outbox_count: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
            [id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(outbox_count, 2);
    assert!(!store.has_pending_terminal_callbacks().unwrap());
}

#[test]
fn local_producer_sequences_reports_and_terminal_state() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("db");
    let store = Store::open(&db).unwrap();
    let id = TaskId::new();
    insert_local(&store, id);
    store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    store
        .append_report(id, ReportOutcome::Blocked, "silent", false)
        .unwrap();
    store
        .append_report(id, ReportOutcome::Succeeded, "notify", true)
        .unwrap();
    store
        .cas_exit(id, ProcessStatus::Running, &ExitReason::Exit { code: 0 })
        .unwrap();
    let events = store.pending_outbound_events(id).unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.event.seq.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.event.payload.notification_required())
            .collect::<Vec<_>>(),
        vec![false, false, false, true, true]
    );
    let EventPayload::Report { report } = &events[2].event.payload else {
        panic!("silent report payload")
    };
    assert_eq!(report.summary, "silent");
    let EventPayload::Callback { event, .. } = &events[3].event.payload else {
        panic!("notify payload")
    };
    assert_eq!(event.reports.len(), 1);
    assert_eq!(event.reports[0].summary, "notify");
    let EventPayload::Callback { event, state } = &events[4].event.payload else {
        panic!("terminal payload")
    };
    assert_eq!(*state, Some(ProcessStatus::Succeeded));
    assert_eq!(event.reports.len(), 2);
    assert_eq!(event.reports[0].summary, "silent");
    assert_eq!(event.reports[1].summary, "notify");
    drop(store);
    let reopened = Store::open(&db).unwrap();
    assert_eq!(reopened.pending_outbound_events(id).unwrap().len(), 5);
    assert!(reopened.has_pending_terminal_callbacks().unwrap());
}

#[test]
fn local_producer_rolls_back_state_and_report_when_event_insert_fails() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    insert_local(&store, id);
    store.conn.execute_batch("CREATE TRIGGER reject_outbox BEFORE INSERT ON executor_outbox BEGIN SELECT RAISE(ABORT, 'event insert failed'); END;").unwrap();
    assert!(
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .is_err()
    );
    assert_eq!(
        store.require_task(id).unwrap().status(),
        ProcessStatus::Queued
    );
    assert!(
        store
            .append_report(id, ReportOutcome::Succeeded, "body", true)
            .is_err()
    );
    assert!(store.reports(id).unwrap().is_empty());
    assert_eq!(store.pending_outbound_events(id).unwrap().len(), 1);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                [id.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .executor_identity(id)
            .unwrap()
            .and_then(|identity| match identity {
                ExecutorIdentity::Accepted(row) => Some(row.state),
                ExecutorIdentity::Rejected(_) => None,
            }),
        Some(ProcessStatus::Queued)
    );
}

#[test]
fn local_acceptance_rejects_mismatched_task_without_partial_insert() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    let row = task_row(id);
    let spec = local_spec(&row);
    assert!(
        store
            .insert_local_task(
                &row,
                &spec,
                MachineId::new(),
                crate::submission::RequestId::new(),
                CallbackExecutable::available("/bin/true".into())
            )
            .is_err()
    );
    assert!(store.get_task(id).unwrap().is_none());
    assert!(store.origin_route_by_task(id).unwrap().is_none());
    assert!(store.executor_identity(id).unwrap().is_none());
}

#[test]
fn queued_cancel_and_runner_loss_each_keep_one_terminal_event() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let cancelled = TaskId::new();
    insert_local(&store, cancelled);
    assert!(matches!(
        store.request_cancel(cancelled).unwrap(),
        CancelResult::CancelledQueued(_)
    ));
    assert_eq!(store.pending_outbound_events(cancelled).unwrap().len(), 2);
    assert!(matches!(
        store.request_cancel(cancelled).unwrap(),
        CancelResult::AlreadyTerminal(_)
    ));
    assert_eq!(store.pending_outbound_events(cancelled).unwrap().len(), 2);
    let lost = TaskId::new();
    insert_local(&store, lost);
    store
        .cas_status(lost, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    store
        .cas_status(lost, ProcessStatus::Running, ProcessStatus::Lost)
        .unwrap();
    let events = store.pending_outbound_events(lost).unwrap();
    assert_eq!(events.len(), 3);
    let EventPayload::Callback { event, .. } = &events[2].event.payload else {
        panic!("loss payload")
    };
    assert_eq!(event.event, EventKind::TaskLost);
    let spawn_failed = TaskId::new();
    insert_local(&store, spawn_failed);
    store
        .cas_exit(
            spawn_failed,
            ProcessStatus::Queued,
            &ExitReason::SpawnFailed {
                message: "no runner".into(),
            },
        )
        .unwrap();
    let events = store.pending_outbound_events(spawn_failed).unwrap();
    assert_eq!(events.len(), 2);
    let EventPayload::Callback { event, state } = &events[1].event.payload else {
        panic!("spawn failure payload")
    };
    assert_eq!(*state, Some(ProcessStatus::Failed));
    assert_eq!(event.event, EventKind::TaskFailed);
}

#[test]
fn inactivity_reminder_is_one_event_with_current_reports() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    insert_local(&store, id);
    store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    store
        .append_report(id, ReportOutcome::Blocked, "waiting", false)
        .unwrap();
    assert!(store.produce_attention_event(id).unwrap());
    assert!(!store.produce_attention_event(id).unwrap());
    let events = store.pending_outbound_events(id).unwrap();
    assert_eq!(events.len(), 4);
    let EventPayload::Callback { event, state } = &events[3].event.payload else {
        panic!("reminder payload")
    };
    assert_eq!(event.event, EventKind::TaskCheckDue);
    assert_eq!(event.reports[0].summary, "waiting");
    assert_eq!(*state, None);
}

#[test]
fn child_identity_is_recorded_once_while_the_task_runs() {
    use crate::cleanup::{ProcessIdentity, ProcessStartTime};
    use nix::unistd::Pid;

    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let id = TaskId::new();
    let child = ProcessIdentity {
        pid: Pid::from_raw(4242),
        start: ProcessStartTime::from_raw(1_790_000_000_123_456),
    };
    {
        let store = Store::open(&path).unwrap();
        insert_local(&store, id);
        assert!(!store.set_child_identity(id, child).unwrap(), "queued");
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        assert!(store.set_child_identity(id, child).unwrap());
        let later = ProcessIdentity {
            pid: Pid::from_raw(4343),
            ..child
        };
        assert!(!store.set_child_identity(id, later).unwrap(), "already set");
    }

    let store = Store::open(&path).unwrap();
    assert_eq!(store.require_task(id).unwrap().child, Some(child));
}

#[test]
fn preempted_run_keeps_its_state_event_and_outcome_and_is_never_success() {
    use crate::callback::{EventKind, NextAction};
    use crate::dependency::{DependencyState, TaskOutcome};

    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let id = TaskId::new();
    {
        let mut store = Store::open(&path).unwrap();
        insert_local(&store, id);
        store
            .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        // nothing produces a preemption before the queue's terminal commit
        // exists, so the row takes the state that commit will write
        store
            .conn
            .execute(
                "UPDATE tasks SET status = 'preempted', exit_reason = ?1 WHERE id = ?2",
                params![
                    serde_json::to_string(&ExitReason::Exit { code: 75 }).unwrap(),
                    id.to_string()
                ],
            )
            .unwrap();
        let row = store.require_task(id).unwrap();
        store.produce_state_event(&row).unwrap();
        deliver_outbound_events(&mut store, id, |_| DeliveryOutcome::Delivered);
    }

    let store = Store::open(&path).unwrap();
    let row = store.require_task(id).unwrap();
    assert_eq!(
        row.state,
        TaskState::Preempted {
            reason: ExitReason::Exit { code: 75 }
        }
    );
    assert_eq!(row.status(), ProcessStatus::Preempted);

    let last = store.inbound_events(id).unwrap().pop().unwrap();
    let EventPayload::Callback { event, state } = &last.event.payload else {
        panic!("terminal event is a callback: {last:?}");
    };
    assert_eq!(*state, Some(ProcessStatus::Preempted));
    assert_eq!(event.event, EventKind::TaskPreempted);
    assert_eq!(event.next_action, NextAction::None);
    let wire = serde_json::to_value(event).unwrap();
    assert_eq!(wire["event"], "TASK_PREEMPTED");
    assert_eq!(
        wire["process"],
        serde_json::json!({"kind": "exit", "code": 75})
    );

    let outcome: String = store
        .conn
        .query_row(
            "SELECT outcome FROM origin_routes WHERE task_id = ?1",
            [id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(outcome, "preempted");
    let states = store.dependency_states(&[id]).unwrap();
    assert_eq!(
        states,
        vec![(
            id,
            Some(DependencyState::Ended(TaskOutcome::Preempted.into()))
        )]
    );
    assert!(!TaskOutcome::Preempted.is_success());
}

#[test]
fn container_evidence_is_refused_where_no_container_path_records_it() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    insert_local(&store, id);
    store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    let confirmed = |exit_code| TaskExitEvidence {
        process_group: ProcessGroupExitEvidence::Unconfirmed,
        container: ContainerExitEvidence::Confirmed {
            container_id: ContainerId::parse(&"a".repeat(64)).unwrap(),
            exit_code,
        },
    };
    // a command task has no container, so container evidence cannot be its witness
    assert!(
        store
            .cas_exit_with_evidence(
                id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
                confirmed(0),
                None,
            )
            .is_err()
    );
    // the confirmed exit code must be the task's exit code
    assert!(
        store
            .cas_exit_with_evidence(
                id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
                confirmed(1),
                None,
            )
            .is_err()
    );
    let never_started = TaskExitEvidence {
        process_group: ProcessGroupExitEvidence::Unconfirmed,
        container: ContainerExitEvidence::NeverStarted,
    };
    assert!(
        store
            .cas_exit_with_evidence(
                id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
                never_started,
                None,
            )
            .is_err(),
        "a container that never started cannot exit with a code"
    );
    assert_eq!(
        store.require_task(id).unwrap().status(),
        ProcessStatus::Running
    );
}

#[test]
fn insert_list_get_agent_and_task() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let agent_id = TaskId::new();
    let task_id = TaskId::new();
    store.insert_task(&agent_row(agent_id)).unwrap();
    store.insert_task(&task_row(task_id)).unwrap();
    let got = store.require_task(agent_id).unwrap();
    assert!(matches!(got.workload, Workload::Agent(_)));
    assert_eq!(got.name.as_str(), "agent job");
    let got = store.require_task(task_id).unwrap();
    assert!(matches!(got.workload, Workload::Task(_)));
    assert_eq!(got.name.as_str(), "command job");
    let listed = store.list_tasks(&[ProcessStatus::Queued], None).unwrap();
    assert_eq!(listed.len(), 2);
}

#[test]
fn fleet_disabled_local_notify_and_terminal_callback_use_the_durable_inbox() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut store = Store::open(&path).unwrap();
    let id = TaskId::new();
    insert_local(&store, id);
    store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    store
        .append_report(id, ReportOutcome::Succeeded, "interim", true)
        .unwrap();
    store
        .cas_exit(id, ProcessStatus::Running, &ExitReason::Exit { code: 0 })
        .unwrap();

    let outbox = store.pending_outbound_events(id).unwrap();
    assert_eq!(outbox.len(), 4);
    assert!(outbox[2].event.payload.notification_required());
    assert!(outbox[3].event.payload.notification_required());
    assert_eq!(
        store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
        Some(CallbackStatus::Pending)
    );

    deliver_outbound_events(&mut store, id, |_| DeliveryOutcome::Delivered);

    assert_eq!(
        store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
        Some(CallbackStatus::Sent)
    );
    assert!(store.reports(id).unwrap()[0].notified_at.is_some());
    assert!(!store.has_pending_terminal_callbacks().unwrap());
    assert_eq!(
        serde_json::to_value(TaskSummary::from_row(
            &store.require_task(id).unwrap(),
            Some(&store.task_presentations(&[id]).unwrap()[&id]),
        ))
        .unwrap()["callback"],
        "sent"
    );

    for seq in 1..=4 {
        store
            .mark_outbound_acknowledged(id, NonZeroU64::new(seq).unwrap())
            .unwrap();
    }
    store
        .conn
        .execute_batch(
            "UPDATE executor_outbox
             SET acknowledged_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 days');
             UPDATE origin_inbox
             SET settled_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 days');",
        )
        .unwrap();
    assert_eq!(store.compact_old_event_payloads().unwrap().compacted, 8);
    let route = store.origin_route_by_task(id).unwrap().unwrap();
    assert_eq!(route.last_accepted_seq, 4);
    assert_eq!(route.last_settled_seq, 4);
    assert_eq!(
        store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
        Some(CallbackStatus::Sent)
    );
    drop(store);

    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.task_presentations(&[id]).unwrap()[&id].terminal_callback,
        Some(CallbackStatus::Sent)
    );
    assert!(!reopened.has_pending_terminal_callbacks().unwrap());
}

#[test]
fn interim_failure_stays_visible_after_terminal_success_and_terminal_failure_is_projected() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let interim_failure_id = TaskId::new();
    insert_local(&store, interim_failure_id);
    store
        .cas_status(
            interim_failure_id,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap();
    store
        .append_report(
            interim_failure_id,
            ReportOutcome::Blocked,
            "interim failure",
            true,
        )
        .unwrap();
    store
        .cas_exit(
            interim_failure_id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
        )
        .unwrap();
    deliver_outbound_events(&mut store, interim_failure_id, |seq| {
        if seq == 3 {
            DeliveryOutcome::Permanent("interim callback failed".into())
        } else {
            DeliveryOutcome::Delivered
        }
    });

    assert_eq!(
        store.task_presentations(&[interim_failure_id]).unwrap()[&interim_failure_id]
            .terminal_callback,
        Some(CallbackStatus::Sent)
    );
    let failed = store.failed_inbox_events(interim_failure_id).unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].seq, 3);
    assert_eq!(failed[0].error, "interim callback failed");

    let terminal_failure_id = TaskId::new();
    insert_local(&store, terminal_failure_id);
    store
        .cas_exit(
            terminal_failure_id,
            ProcessStatus::Queued,
            &ExitReason::Cancelled,
        )
        .unwrap();
    deliver_outbound_events(&mut store, terminal_failure_id, |_| {
        DeliveryOutcome::Permanent("terminal callback failed".into())
    });
    assert_eq!(
        store.task_presentations(&[terminal_failure_id]).unwrap()[&terminal_failure_id]
            .terminal_callback,
        Some(CallbackStatus::Failed)
    );
    assert_eq!(
        serde_json::to_value(TaskSummary::from_row(
            &store.require_task(terminal_failure_id).unwrap(),
            Some(&store.task_presentations(&[terminal_failure_id]).unwrap()[&terminal_failure_id]),
        ))
        .unwrap()["callback"],
        "failed"
    );
}

#[test]
fn terminal_callback_waiting_projects_to_callback_status() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    insert_local(&store, id);
    store
        .cas_exit(id, ProcessStatus::Queued, &ExitReason::Cancelled)
        .unwrap();

    deliver_outbound_events(&mut store, id, |_| {
        DeliveryOutcome::Deferred("origin thread is asleep".into())
    });

    assert_eq!(
        store.task_presentations(&[id]).unwrap()[&id].terminal_callback,
        Some(CallbackStatus::Waiting)
    );
    assert_eq!(
        serde_json::to_value(TaskSummary::from_row(
            &store.require_task(id).unwrap(),
            Some(&store.task_presentations(&[id]).unwrap()[&id]),
        ))
        .unwrap()["callback"],
        "waiting"
    );
}

#[test]
fn terminal_event_follows_one_durable_inactivity_event() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    insert_local(&store, id);
    store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    assert!(store.produce_attention_event(id).unwrap());
    assert!(!store.produce_attention_event(id).unwrap());

    store
        .cas_exit(id, ProcessStatus::Running, &ExitReason::Exit { code: 0 })
        .unwrap();
    let events = store.pending_outbound_events(id).unwrap();
    assert_eq!(events.len(), 4);
    let EventPayload::Callback {
        event: attention, ..
    } = &events[2].event.payload
    else {
        panic!("inactivity callback payload")
    };
    assert_eq!(attention.event, EventKind::TaskCheckDue);
    let EventPayload::Callback {
        event: terminal,
        state,
    } = &events[3].event.payload
    else {
        panic!("terminal callback payload")
    };
    assert_eq!(*state, Some(ProcessStatus::Succeeded));
    assert_eq!(terminal.event, EventKind::TaskSucceeded);
    assert!(store.require_task(id).unwrap().check_due_at.is_some());
}

#[test]
fn inactivity_event_cannot_be_produced_while_queued_or_after_terminal() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let queued_id = TaskId::new();
    insert_local(&store, queued_id);
    assert!(!store.produce_attention_event(queued_id).unwrap());
    assert_eq!(store.require_task(queued_id).unwrap().check_due_at, None);

    let terminal_id = TaskId::new();
    insert_local(&store, terminal_id);
    store
        .cas_exit(terminal_id, ProcessStatus::Queued, &ExitReason::Cancelled)
        .unwrap();
    assert!(!store.produce_attention_event(terminal_id).unwrap());
    assert_eq!(store.require_task(terminal_id).unwrap().check_due_at, None);
}

#[test]
fn timeout_seconds_above_i64_max_round_trip() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    let mut row = agent_row(id);
    row.timeout = Duration::from_secs(u64::MAX);
    store.insert_task(&row).unwrap();
    assert_eq!(
        store.require_task(id).unwrap().timeout,
        Duration::from_secs(u64::MAX),
        "a timeout wider than SQLite INTEGER must survive persistence"
    );
}

#[test]
fn reports_keep_order_and_refuse_overflow_long_summaries_and_terminal_tasks() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let id = TaskId::new();
    store.insert_task(&agent_row(id)).unwrap();
    store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    let reports = store
        .append_report(id, ReportOutcome::Blocked, "need x", false)
        .unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].seq, 1);
    let reports = store
        .append_report(id, ReportOutcome::Succeeded, "got x", false)
        .unwrap();
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[1].seq, 2);

    let long = "x".repeat(SUMMARY_MAX_BYTES + 1);
    let err = store
        .append_report(id, ReportOutcome::Succeeded, &long, false)
        .unwrap_err();
    assert!(matches!(err, AppError::SummaryTooLong { .. }));

    for i in 0..18 {
        store
            .append_report(id, ReportOutcome::Succeeded, &format!("n{i}"), false)
            .unwrap();
    }
    let err = store
        .append_report(id, ReportOutcome::Succeeded, "overflow", false)
        .unwrap_err();
    assert!(matches!(err, AppError::TooManyReports { count: 20 }));

    let terminal = TaskId::new();
    store.insert_task(&agent_row(terminal)).unwrap();
    store
        .cas_exit(terminal, ProcessStatus::Queued, &ExitReason::Cancelled)
        .unwrap()
        .expect("queued task cancels");
    let err = store
        .append_report(terminal, ReportOutcome::Succeeded, "late", false)
        .unwrap_err();
    assert!(matches!(err, AppError::TaskTerminal { .. }));
}
#[test]
fn cancel_race_reports_cancelled_queued() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("db");
    let store = Store::open(&db).unwrap();
    let worker = Store::open(&db).unwrap();
    let id = TaskId::new();
    store.insert_task(&agent_row(id)).unwrap();

    worker.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    worker
        .conn
        .execute(
            "UPDATE tasks SET status = 'running' WHERE id = ?1 AND status = 'queued'",
            params![id.to_string()],
        )
        .unwrap();

    let cancel = std::thread::spawn(move || {
        let result = store.request_cancel(id).unwrap();
        (store, result)
    });
    std::thread::sleep(Duration::from_millis(200));
    worker.conn.execute_batch("COMMIT").unwrap();

    let (store, result) = cancel.join().unwrap();
    let row = store.require_task(id).unwrap();
    assert_eq!(row.status(), ProcessStatus::Running);
    assert!(
        matches!(result, CancelResult::SignalWorker(_)),
        "a row that reached Running must be signalled, not reported cancelled: {result:?}"
    );
}
