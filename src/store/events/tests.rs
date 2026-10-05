use crate::domain::ProcessStatus;
use std::path::Path;

use tempfile::tempdir;

use super::encode;
use super::retention::EVENT_RETENTION_BATCH_SIZE;
use crate::callback::ReportView;
use crate::domain::{TaskEnv, TaskId};
use crate::error::AppError;
use crate::events::{
    DeliveryOutcome, DeliveryState, EventAcceptance, EventError, EventPayload, EventRouteState,
    THREAD_WAIT_LIMIT, TaskEvent,
};
use crate::machine::MachineId;
use crate::store::Store;
use crate::submission::{
    CallbackContext, ExecutionRecord, OriginRoute, RequestId, SubmissionState,
};
use chrono::Utc;
use rusqlite::params;
use std::num::NonZeroU64;

fn route() -> OriginRoute {
    let spec: crate::spec::NormalizedSpec = serde_json::from_value(serde_json::json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "event task",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": { "type": "task", "command": ["echo", "hello"] }
    }))
    .unwrap();
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

fn event(route: &OriginRoute, seq: u64, status: ProcessStatus) -> TaskEvent {
    TaskEvent {
        task: route.task,
        seq: NonZeroU64::new(seq).unwrap(),
        origin_machine: route.origin_machine,
        execution_machine: route.execution_machine,
        payload: EventPayload::State { status },
    }
}

fn callback_event(route: &OriginRoute, seq: u64, state: Option<ProcessStatus>) -> TaskEvent {
    let callback = serde_json::from_value(serde_json::json!({
        "api_version": 1,
        "event": if state.is_some() { "TASK_SUCCEEDED" } else { "TASK_REPORTED" },
        "task": route.task,
        "name": "event task",
        "workload": { "type": "task", "command": ["echo", "hello"] },
        "thread": route.thread,
        "cwd": "/tmp",
        "evidence": "/tmp/evidence",
        "reports": [],
        "process": null,
        "next_action": "read_report"
    }))
    .unwrap();
    TaskEvent {
        task: route.task,
        seq: NonZeroU64::new(seq).unwrap(),
        origin_machine: route.origin_machine,
        execution_machine: route.execution_machine,
        payload: EventPayload::Callback {
            event: Box::new(callback),
            state,
        },
    }
}

fn age_event_payloads(store: &Store) {
    store
        .conn
        .execute_batch(
            "UPDATE executor_outbox
             SET acknowledged_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 days');
             UPDATE origin_inbox
             SET settled_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 days');",
        )
        .unwrap();
}

#[test]
fn preempted_event_and_state_must_agree() {
    let route = route();
    let with_kind = |kind: &str, state| {
        let mut event = callback_event(&route, 1, state);
        let EventPayload::Callback {
            event: callback, ..
        } = &mut event.payload
        else {
            unreachable!("callback_event builds a callback");
        };
        callback.event = serde_json::from_value(serde_json::json!(kind)).unwrap();
        event
    };

    assert!(super::validate(&with_kind("TASK_PREEMPTED", Some(ProcessStatus::Preempted))).is_ok());
    for event in [
        with_kind("TASK_PREEMPTED", Some(ProcessStatus::Failed)),
        with_kind("TASK_PREEMPTED", None),
        with_kind("TASK_FAILED", Some(ProcessStatus::Preempted)),
    ] {
        assert!(
            matches!(super::validate(&event), Err(EventError::Invalid { .. })),
            "{event:?}"
        );
    }
}

#[test]
fn first_duplicate_conflict_gap_and_unknown_route() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store.insert_origin_route(&route).unwrap();
    let first = event(&route, 1, ProcessStatus::Running);
    assert_eq!(
        store.accept_inbound_event(&first).unwrap(),
        EventAcceptance::Acknowledged { seq: 1 }
    );
    assert_eq!(
        store.accept_inbound_event(&first).unwrap(),
        EventAcceptance::Acknowledged { seq: 1 }
    );
    let different = event(&route, 1, ProcessStatus::Succeeded);
    assert!(matches!(
        store.accept_inbound_event(&different),
        Err(EventError::ContentConflict { seq: 1, .. })
    ));
    assert_eq!(
        store
            .accept_inbound_event(&event(&route, 3, ProcessStatus::Succeeded))
            .unwrap(),
        EventAcceptance::Expected { seq: 2 }
    );
    let missing = TaskEvent {
        task: TaskId::new(),
        ..first.clone()
    };
    assert!(matches!(
        store.accept_inbound_event(&missing),
        Err(EventError::RouteNotFound { .. })
    ));
    let saved = store.origin_route_by_task(route.task).unwrap().unwrap();
    assert_eq!(saved.last_accepted_seq, 1);
    assert_eq!(saved.last_settled_seq, 1);
    assert_eq!(saved.last_execution_state, Some(ProcessStatus::Running));
    let inbox = store.inbound_events(route.task).unwrap();
    assert_eq!(inbox.len(), 1);
    assert!(!inbox[0].event.payload.notification_required());
    assert_eq!(inbox[0].delivery, DeliveryState::NotRequired);
}

#[test]
fn aged_local_and_remote_events_compact_and_late_duplicates_use_receipts() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut route = route();
    if route.execution_machine == route.origin_machine {
        route.execution_machine = MachineId::new();
    }
    let mut store = Store::open(&path).unwrap();
    store.insert_origin_route(&route).unwrap();
    store
        .accept_execution(&ExecutionRecord {
            task: route.task,
            origin_machine: route.origin_machine,
            execution_machine: route.execution_machine,
            spec: route.spec.clone(),
            state: ProcessStatus::Queued,
        })
        .unwrap();
    let outbox = store
        .append_outbound_event(
            route.task,
            route.origin_machine,
            route.execution_machine,
            EventPayload::State {
                status: ProcessStatus::Running,
            },
        )
        .unwrap();
    store.accept_inbound_event(&outbox.event).unwrap();
    store
        .mark_outbound_acknowledged(route.task, outbox.event.seq)
        .unwrap();
    let reordered_json = format!(
        "{{\"payload\":{{\"status\":\"running\",\"type\":\"state\"}},\
         \"execution_machine\":{},\"origin_machine\":{},\"seq\":{},\"task\":{}}}",
        serde_json::to_string(&outbox.event.execution_machine).unwrap(),
        serde_json::to_string(&outbox.event.origin_machine).unwrap(),
        outbox.event.seq,
        serde_json::to_string(&outbox.event.task).unwrap(),
    );
    store
        .conn
        .execute(
            "UPDATE origin_inbox SET event_json=?1 WHERE task_id=?2 AND seq=1",
            params![reordered_json, route.task.to_string()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE executor_outbox SET event_json=?1 WHERE task_id=?2 AND seq=1",
            params![reordered_json, route.task.to_string()],
        )
        .unwrap();
    age_event_payloads(&store);

    assert_eq!(store.compact_old_event_payloads().unwrap().compacted, 2);
    assert_eq!(store.inbound_events(route.task).unwrap().len(), 0);
    assert!(
        store
            .outbound_event_at_or_after(route.task, NonZeroU64::MIN)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .outbound_route_status(route.task)
            .unwrap()
            .unwrap()
            .acknowledged,
        1
    );
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    reopened
        .mark_outbound_acknowledged(route.task, outbox.event.seq)
        .unwrap();
    assert_eq!(
        reopened.accept_inbound_event(&outbox.event).unwrap(),
        EventAcceptance::Acknowledged { seq: 1 }
    );
    let route_after_compaction = reopened.origin_route_by_task(route.task).unwrap().unwrap();
    assert_eq!(route_after_compaction.last_accepted_seq, 1);
    assert_eq!(route_after_compaction.last_settled_seq, 1);
    let changed = event(&route, 1, ProcessStatus::Succeeded);
    assert!(matches!(
        reopened.accept_inbound_event(&changed),
        Err(EventError::ContentConflict { seq: 1, .. })
    ));
}

#[test]
fn compaction_preserves_unsettled_and_failed_callback_payloads() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let pending_route = route();

    store.insert_origin_route(&pending_route).unwrap();
    store
        .accept_execution(&ExecutionRecord {
            task: pending_route.task,
            origin_machine: pending_route.origin_machine,
            execution_machine: pending_route.execution_machine,
            spec: pending_route.spec.clone(),
            state: ProcessStatus::Queued,
        })
        .unwrap();
    let pending = callback_event(&pending_route, 1, None);
    store
        .append_outbound_event(
            pending_route.task,
            pending_route.origin_machine,
            pending_route.execution_machine,
            pending.payload.clone(),
        )
        .unwrap();
    store.accept_inbound_event(&pending).unwrap();
    store
        .mark_outbound_acknowledged(pending_route.task, pending.seq)
        .unwrap();

    let failed_route = route();
    store.insert_origin_route(&failed_route).unwrap();
    store
        .accept_execution(&ExecutionRecord {
            task: failed_route.task,
            origin_machine: failed_route.origin_machine,
            execution_machine: failed_route.execution_machine,
            spec: failed_route.spec.clone(),
            state: ProcessStatus::Queued,
        })
        .unwrap();
    let failed = callback_event(&failed_route, 1, None);
    store
        .append_outbound_event(
            failed_route.task,
            failed_route.origin_machine,
            failed_route.execution_machine,
            failed.payload.clone(),
        )
        .unwrap();
    store.accept_inbound_event(&failed).unwrap();
    store
        .mark_outbound_acknowledged(failed_route.task, failed.seq)
        .unwrap();
    store
        .reserve_inbox_attempt(failed_route.task, failed.seq)
        .unwrap()
        .unwrap();
    store
        .settle_inbox_attempt(
            failed_route.task,
            failed.seq,
            DeliveryOutcome::Permanent("callback unavailable".into()),
        )
        .unwrap();
    let waiting_route = route();
    store.insert_origin_route(&waiting_route).unwrap();
    store
        .accept_execution(&ExecutionRecord {
            task: waiting_route.task,
            origin_machine: waiting_route.origin_machine,
            execution_machine: waiting_route.execution_machine,
            spec: waiting_route.spec.clone(),
            state: ProcessStatus::Queued,
        })
        .unwrap();
    let waiting = callback_event(&waiting_route, 1, None);
    store
        .append_outbound_event(
            waiting_route.task,
            waiting_route.origin_machine,
            waiting_route.execution_machine,
            waiting.payload.clone(),
        )
        .unwrap();
    store.accept_inbound_event(&waiting).unwrap();
    store
        .mark_outbound_acknowledged(waiting_route.task, waiting.seq)
        .unwrap();
    store
        .reserve_inbox_attempt(waiting_route.task, waiting.seq)
        .unwrap()
        .unwrap();
    store
        .settle_inbox_attempt(
            waiting_route.task,
            waiting.seq,
            DeliveryOutcome::Deferred("origin thread is asleep".into()),
        )
        .unwrap();
    age_event_payloads(&store);

    assert_eq!(store.compact_old_event_payloads().unwrap().compacted, 3);
    let pending_inbox = store.inbound_events(pending_route.task).unwrap();
    assert_eq!(pending_inbox.len(), 1);
    assert_eq!(pending_inbox[0].event, pending);
    assert!(matches!(
        pending_inbox[0].delivery,
        DeliveryState::PendingDelivery { .. }
    ));
    let failed_inbox = store.inbound_events(failed_route.task).unwrap();
    assert_eq!(failed_inbox.len(), 1);
    assert_eq!(failed_inbox[0].event, failed);
    assert!(matches!(
        &failed_inbox[0].delivery,
        DeliveryState::DeliveryFailed { last_error, .. }
            if last_error == "callback unavailable"
    ));
    let waiting_inbox = store.inbound_events(waiting_route.task).unwrap();
    assert_eq!(waiting_inbox.len(), 1);
    assert_eq!(waiting_inbox[0].event, waiting);
    assert!(matches!(
        &waiting_inbox[0].delivery,
        DeliveryState::AwaitingThread { reason, .. }
            if reason == "origin thread is asleep"
    ));
}

#[test]
fn compaction_rolls_back_receipts_when_payload_delete_fails() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store.insert_origin_route(&route).unwrap();
    store
        .accept_execution(&ExecutionRecord {
            task: route.task,
            origin_machine: route.origin_machine,
            execution_machine: route.execution_machine,
            spec: route.spec.clone(),
            state: ProcessStatus::Queued,
        })
        .unwrap();
    let outbox = store
        .append_outbound_event(
            route.task,
            route.origin_machine,
            route.execution_machine,
            EventPayload::State {
                status: ProcessStatus::Running,
            },
        )
        .unwrap();
    store.accept_inbound_event(&outbox.event).unwrap();
    store
        .mark_outbound_acknowledged(route.task, outbox.event.seq)
        .unwrap();
    age_event_payloads(&store);
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_inbox_compaction BEFORE DELETE ON origin_inbox
             BEGIN SELECT RAISE(ABORT, 'injected inbox delete failure'); END;",
        )
        .unwrap();

    assert!(store.compact_old_event_payloads().is_err());
    for table in ["executor_event_receipts", "origin_event_receipts"] {
        let count: i64 = store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }
    let outbox_count: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
            [route.task.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let inbox_count: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM origin_inbox WHERE task_id=?1",
            [route.task.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(outbox_count, 1);
    assert_eq!(inbox_count, 1);
}

#[test]
fn compaction_keeps_acknowledged_events_for_orphaned_routes() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store
        .accept_execution(&ExecutionRecord {
            task: route.task,
            origin_machine: route.origin_machine,
            execution_machine: route.execution_machine,
            spec: route.spec.clone(),
            state: ProcessStatus::Queued,
        })
        .unwrap();
    let outbox = store
        .append_outbound_event(
            route.task,
            route.origin_machine,
            route.execution_machine,
            EventPayload::State {
                status: ProcessStatus::Running,
            },
        )
        .unwrap();
    store
        .mark_outbound_acknowledged(route.task, outbox.event.seq)
        .unwrap();
    store.orphan_outbound_route(route.task).unwrap();
    age_event_payloads(&store);

    assert_eq!(store.compact_old_event_payloads().unwrap().compacted, 0);
    assert!(
        store
            .outbound_event_at_or_after(route.task, outbox.event.seq)
            .unwrap()
            .is_some()
    );
    let receipts: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM executor_event_receipts WHERE task_id=?1",
            [route.task.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipts, 0);
}

#[test]
fn compaction_processes_bounded_batches_and_leaves_recent_rows() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    for index in 0..=EVENT_RETENTION_BATCH_SIZE + 1 {
        let task = TaskId::new();
        let event = TaskEvent {
            task,
            seq: NonZeroU64::MIN,
            origin_machine: MachineId::new(),
            execution_machine: MachineId::new(),
            payload: EventPayload::State {
                status: ProcessStatus::Running,
            },
        };
        let age = if index > EVENT_RETENTION_BATCH_SIZE {
            "strftime('%Y-%m-%dT%H:%M:%fZ','now')"
        } else {
            "strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 days')"
        };
        store
            .conn
            .execute(
                &format!(
                    "INSERT INTO executor_outbox (task_id,seq,event_json,state,acknowledged_at)
                     VALUES (?1,1,?2,'acknowledged',{age})"
                ),
                params![task.to_string(), serde_json::to_string(&event).unwrap()],
            )
            .unwrap();
    }
    let mut store = store;

    let first = store.compact_old_event_payloads().unwrap();
    assert_eq!(first.compacted, EVENT_RETENTION_BATCH_SIZE as usize);
    assert!(first.has_more);
    let second = store.compact_old_event_payloads().unwrap();
    assert_eq!(second.compacted, 1);
    assert!(!second.has_more);
    let retained: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM executor_outbox", [], |row| row.get(0))
        .unwrap();
    assert_eq!(retained, 1);
}

#[test]
fn owner_conflict_and_receiver_identity_leave_route_unchanged() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store.insert_origin_route(&route).unwrap();
    let mut wrong_owner = event(&route, 1, ProcessStatus::Running);
    wrong_owner.execution_machine = MachineId::new();
    assert!(matches!(
        store.accept_inbound_event(&wrong_owner),
        Err(EventError::OwnerConflict { .. })
    ));
    let local = crate::machine::LocalIdentity {
        machine: route.origin_machine,
        boot: crate::machine::BootId::new(),
    };
    assert!(matches!(
        local.check_destination(MachineId::new()),
        Err(AppError::MachineIdentityMismatch { .. })
    ));
    assert_eq!(
        store
            .origin_route_by_task(route.task)
            .unwrap()
            .unwrap()
            .last_accepted_seq,
        0
    );
    assert!(store.inbound_events(route.task).unwrap().is_empty());
}

#[test]
fn callback_pending_does_not_settle_later_state_only_event() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store.insert_origin_route(&route).unwrap();
    let callback = serde_json::from_value(serde_json::json!({
        "api_version": 1,
        "event": "TASK_REPORTED",
        "task": route.task,
        "name": "event task",
        "workload": { "type": "task", "command": ["echo", "hello"] },
        "thread": route.thread,
        "cwd": "/tmp",
        "evidence": "/tmp/evidence",
        "reports": [],
        "process": null,
        "next_action": "read_report"
    }))
    .unwrap();
    let notified = TaskEvent {
        task: route.task,
        seq: NonZeroU64::new(1).unwrap(),
        origin_machine: route.origin_machine,
        execution_machine: route.execution_machine,
        payload: EventPayload::Callback {
            event: Box::new(callback),
            state: None,
        },
    };
    store.accept_inbound_event(&notified).unwrap();
    store
        .accept_inbound_event(&event(&route, 2, ProcessStatus::Running))
        .unwrap();
    let report = TaskEvent {
        task: route.task,
        seq: NonZeroU64::new(3).unwrap(),
        origin_machine: route.origin_machine,
        execution_machine: route.execution_machine,
        payload: EventPayload::Report {
            report: ReportView {
                seq: 1,
                outcome: crate::domain::ReportKind::Blocked,
                summary: "need input".into(),
            },
        },
    };
    store.accept_inbound_event(&report).unwrap();
    let inbox = store.inbound_events(route.task).unwrap();
    assert!(inbox[0].event.payload.notification_required());
    assert_eq!(
        inbox[0].delivery,
        DeliveryState::PendingDelivery {
            attempts: 0,
            last_error: None
        }
    );
    assert!(!inbox[1].event.payload.notification_required());
    assert_eq!(inbox[1].delivery, DeliveryState::NotRequired);
    assert!(!inbox[2].event.payload.notification_required());
    assert_eq!(inbox[2].delivery, DeliveryState::NotRequired);
    let saved = store.origin_route_by_task(route.task).unwrap().unwrap();
    assert_eq!(saved.last_accepted_seq, 3);
    assert_eq!(saved.last_settled_seq, 0);
}

#[test]
fn deferred_callback_refunds_attempt_and_blocks_later_events() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let pending_route = route();
    let route = route();
    store.insert_origin_route(&route).unwrap();
    let callback = callback_event(&route, 1, None);
    store.accept_inbound_event(&callback).unwrap();
    store
        .accept_inbound_event(&event(&route, 2, ProcessStatus::Running))
        .unwrap();
    store.insert_origin_route(&pending_route).unwrap();
    store
        .accept_inbound_event(&callback_event(&pending_route, 1, None))
        .unwrap();

    let reserved = store
        .reserve_inbox_attempt(route.task, callback.seq)
        .unwrap()
        .unwrap();
    assert!(matches!(
        reserved.delivery,
        DeliveryState::PendingDelivery { attempts: 1, .. }
    ));
    let before = chrono::Utc::now();
    let deferred = store
        .settle_inbox_attempt(
            route.task,
            callback.seq,
            DeliveryOutcome::Deferred("origin thread is asleep".into()),
        )
        .unwrap();
    let after = chrono::Utc::now();
    let DeliveryState::AwaitingThread {
        attempts,
        since,
        reason,
    } = deferred.delivery
    else {
        panic!("deferred callback state")
    };
    assert_eq!(attempts, 0);
    assert!(since >= before && since <= after);
    assert_eq!(reason, "origin thread is asleep");
    assert_eq!(
        store
            .origin_route_by_task(route.task)
            .unwrap()
            .unwrap()
            .last_settled_seq,
        0
    );
    let settled_at: Option<String> = store
        .conn
        .query_row(
            "SELECT settled_at FROM origin_inbox WHERE task_id=?1 AND seq=1",
            [route.task.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(settled_at, None);

    let earliest = store.earliest_unsettled_inbox(route.task).unwrap().unwrap();
    assert_eq!(earliest.event.seq, callback.seq);
    assert!(matches!(
        earliest.delivery,
        DeliveryState::AwaitingThread { attempts: 0, .. }
    ));
    assert!(
        store
            .reserve_inbox_attempt(route.task, NonZeroU64::new(2).unwrap())
            .unwrap()
            .is_none()
    );
    assert!(store.pending_inbox_tasks().unwrap().contains(&route.task));
}

#[test]
fn repeated_deferral_keeps_the_original_waiting_time() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store.insert_origin_route(&route).unwrap();
    let callback = callback_event(&route, 1, None);
    store.accept_inbound_event(&callback).unwrap();
    store
        .reserve_inbox_attempt(route.task, callback.seq)
        .unwrap()
        .unwrap();
    store
        .settle_inbox_attempt(
            route.task,
            callback.seq,
            DeliveryOutcome::Deferred("thread is asleep".into()),
        )
        .unwrap();
    let DeliveryState::AwaitingThread { since, .. } =
        &store.inbound_events(route.task).unwrap()[0].delivery
    else {
        panic!("awaiting thread state")
    };

    let reserved = store
        .reserve_inbox_attempt(route.task, callback.seq)
        .unwrap()
        .unwrap();
    assert!(matches!(
        reserved.delivery,
        DeliveryState::AwaitingThread {
            attempts: 1,
            since: reserved_since,
            reason: reserved_reason,
        } if reserved_since == *since && reserved_reason == "thread is asleep"
    ));
    store
        .settle_inbox_attempt(
            route.task,
            callback.seq,
            DeliveryOutcome::Deferred("thread is still asleep".into()),
        )
        .unwrap();

    assert!(matches!(
        &store.inbound_events(route.task).unwrap()[0].delivery,
        DeliveryState::AwaitingThread {
            attempts: 0,
            since: original_since,
            reason,
        } if *original_since == *since && reason == "thread is still asleep"
    ));
}

#[test]
fn waiting_past_the_limit_fails_and_releases_later_events() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store.insert_origin_route(&route).unwrap();
    let callback = callback_event(&route, 1, None);
    store.accept_inbound_event(&callback).unwrap();
    store
        .accept_inbound_event(&event(&route, 2, ProcessStatus::Running))
        .unwrap();
    let expired = DeliveryState::AwaitingThread {
        attempts: 0,
        since: Utc::now() - THREAD_WAIT_LIMIT,
        reason: "Claude session is not running".into(),
    };
    store
        .conn
        .execute(
            "UPDATE origin_inbox SET delivery_json=?1 WHERE task_id=?2 AND seq=1",
            params![encode(&expired).unwrap(), route.task.to_string()],
        )
        .unwrap();
    let waiting = store.waiting_inbox_events(route.task).unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0].until - waiting[0].since, THREAD_WAIT_LIMIT);

    store
        .reserve_inbox_attempt(route.task, callback.seq)
        .unwrap()
        .unwrap();
    let settled = store
        .settle_inbox_attempt(
            route.task,
            callback.seq,
            DeliveryOutcome::Deferred("Claude session is not running".into()),
        )
        .unwrap();

    assert!(matches!(
        &settled.delivery,
        DeliveryState::DeliveryFailed { attempts: 0, last_error }
            if last_error.starts_with("gave up after waiting 3 days")
    ));
    assert!(store.waiting_inbox_events(route.task).unwrap().is_empty());
    assert_eq!(
        store
            .origin_route_by_task(route.task)
            .unwrap()
            .unwrap()
            .last_settled_seq,
        2
    );
}

#[test]
fn awaiting_callback_can_deliver_and_records_its_wait_reason() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store.insert_origin_route(&route).unwrap();
    let callback = callback_event(&route, 1, None);
    store.accept_inbound_event(&callback).unwrap();
    store
        .accept_inbound_event(&event(&route, 2, ProcessStatus::Running))
        .unwrap();
    store
        .reserve_inbox_attempt(route.task, callback.seq)
        .unwrap()
        .unwrap();
    store
        .settle_inbox_attempt(
            route.task,
            callback.seq,
            DeliveryOutcome::Deferred("origin thread is asleep".into()),
        )
        .unwrap();
    let reserved = store
        .reserve_inbox_attempt(route.task, callback.seq)
        .unwrap()
        .unwrap();
    assert!(matches!(
        reserved.delivery,
        DeliveryState::AwaitingThread { attempts: 1, .. }
    ));

    let delivered = store
        .settle_inbox_attempt(route.task, callback.seq, DeliveryOutcome::Delivered)
        .unwrap();
    assert_eq!(
        delivered.delivery,
        DeliveryState::Delivered {
            attempts: 1,
            last_error: Some("origin thread is asleep".into()),
        }
    );
    assert_eq!(
        store
            .origin_route_by_task(route.task)
            .unwrap()
            .unwrap()
            .last_settled_seq,
        2
    );
}

#[test]
fn awaiting_retryable_failure_resumes_the_three_attempt_budget() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store.insert_origin_route(&route).unwrap();
    let callback = callback_event(&route, 1, None);
    store.accept_inbound_event(&callback).unwrap();
    store
        .reserve_inbox_attempt(route.task, callback.seq)
        .unwrap()
        .unwrap();
    store
        .settle_inbox_attempt(
            route.task,
            callback.seq,
            DeliveryOutcome::Deferred("origin thread is asleep".into()),
        )
        .unwrap();

    for attempts in 1..=3 {
        let reserved = store
            .reserve_inbox_attempt(route.task, callback.seq)
            .unwrap()
            .unwrap();
        if attempts == 1 {
            assert!(matches!(
                reserved.delivery,
                DeliveryState::AwaitingThread { attempts: 1, .. }
            ));
        } else {
            assert!(matches!(
                reserved.delivery,
                DeliveryState::PendingDelivery {
                    attempts: reserved_attempts,
                    ..
                } if reserved_attempts == attempts
            ));
        }
        let result = store
            .settle_inbox_attempt(
                route.task,
                callback.seq,
                DeliveryOutcome::Retryable(format!("send failed {attempts}")),
            )
            .unwrap();
        if attempts < 3 {
            assert_eq!(
                result.delivery,
                DeliveryState::PendingDelivery {
                    attempts,
                    last_error: Some(format!("send failed {attempts}")),
                }
            );
        } else {
            assert_eq!(
                result.delivery,
                DeliveryState::DeliveryFailed {
                    attempts,
                    last_error: "send failed 3".into(),
                }
            );
        }
    }
    assert!(store.pending_inbox_tasks().unwrap().is_empty());
}

#[test]
fn deferred_outcome_without_a_reserved_attempt_is_rejected() {
    let dir = tempdir().unwrap();
    let mut store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store.insert_origin_route(&route).unwrap();
    let callback = callback_event(&route, 1, None);
    store.accept_inbound_event(&callback).unwrap();

    assert!(matches!(
        store.settle_inbox_attempt(
            route.task,
            callback.seq,
            DeliveryOutcome::Deferred("origin thread is asleep".into())
        ),
        Err(EventError::Invalid { .. })
    ));
    assert_eq!(
        store.inbound_events(route.task).unwrap()[0].delivery,
        DeliveryState::PendingDelivery {
            attempts: 0,
            last_error: None,
        }
    );
}

#[test]
fn outbox_and_inbox_survive_reopen_and_version_three_migrates() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let route = route();
    {
        let mut store = Store::open(&path).unwrap();
        store.insert_origin_route(&route).unwrap();
        store
            .accept_execution(&ExecutionRecord {
                task: route.task,
                origin_machine: route.origin_machine,
                execution_machine: route.execution_machine,
                spec: route.spec.clone(),
                state: ProcessStatus::Queued,
            })
            .unwrap();
        let outbox = store
            .append_outbound_event(
                route.task,
                route.origin_machine,
                route.execution_machine,
                EventPayload::State {
                    status: ProcessStatus::Running,
                },
            )
            .unwrap();
        assert_eq!(outbox.event.seq.get(), 1);
        store.accept_inbound_event(&outbox.event).unwrap();
    }
    {
        let mut store = Store::open(&path).unwrap();
        let pending = store.pending_outbound_events(route.task).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(store.inbound_events(route.task).unwrap().len(), 1);
        store
            .mark_outbound_acknowledged(route.task, pending[0].event.seq)
            .unwrap();
        assert!(
            store
                .pending_outbound_events(route.task)
                .unwrap()
                .is_empty()
        );
        store
            .conn
            .execute(
                "DELETE FROM executor_outbox WHERE task_id=?1 AND seq=1",
                [route.task.to_string()],
            )
            .unwrap();
        let next = store
            .append_outbound_event(
                route.task,
                route.origin_machine,
                route.execution_machine,
                EventPayload::State {
                    status: ProcessStatus::Succeeded,
                },
            )
            .unwrap();
        assert_eq!(next.event.seq.get(), 2);
        store.orphan_outbound_route(route.task).unwrap();
    }
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened
            .outbound_route_status(route.task)
            .unwrap()
            .unwrap()
            .state,
        EventRouteState::Orphaned
    );
    assert!(reopened.pending_outbound_tasks().unwrap().is_empty());
}

/// Callback shape that release 0.14 decodes, the last one before waiting
/// handoff: closed objects, and only the event kinds and report outcomes it knew
#[expect(
    dead_code,
    reason = "the fields only describe the shape an older origin decodes"
)]
mod v0_14 {
    use serde::Deserialize;
    use serde_json::Value;

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct TaskEvent {
        pub(super) task: Value,
        pub(super) seq: u64,
        pub(super) origin_machine: Value,
        pub(super) execution_machine: Value,
        pub(super) payload: Payload,
    }

    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    pub(super) enum Payload {
        Callback {
            event: Box<Event>,
            state: Option<Value>,
        },
        State {
            status: Value,
        },
        Report {
            report: Report,
        },
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct Event {
        pub(super) api_version: u32,
        pub(super) event: Kind,
        pub(super) task: Value,
        pub(super) name: Value,
        pub(super) workload: Value,
        pub(super) thread: Value,
        pub(super) cwd: Value,
        pub(super) evidence: Value,
        pub(super) reports: Vec<Report>,
        pub(super) process: Option<Value>,
        #[serde(default)]
        pub(super) timeout_secs: Option<u64>,
        pub(super) next_action: NextAction,
        #[serde(default)]
        pub(super) cancel_reason: Option<Value>,
    }

    #[derive(Debug, PartialEq, Eq, Deserialize)]
    pub(super) enum Kind {
        #[serde(rename = "TASK_REPORTED")]
        Reported,
        #[serde(rename = "TASK_CHECK_DUE")]
        CheckDue,
        #[serde(rename = "TASK_CANCELLED")]
        Cancelled,
        #[serde(rename = "TASK_LOST")]
        Lost,
        #[serde(rename = "TASK_BLOCKED")]
        Blocked,
        #[serde(rename = "TASK_FAILED")]
        Failed,
        #[serde(rename = "TASK_SUCCEEDED")]
        Succeeded,
        #[serde(rename = "TASK_PREEMPTED")]
        Preempted,
    }

    #[derive(Debug, PartialEq, Eq, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(super) enum NextAction {
        ReadReport,
        InspectTask,
        None,
        InspectLog,
        AnswerAndResubmit,
        ReviewOutput,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct Report {
        pub(super) seq: i64,
        pub(super) outcome: Outcome,
        pub(super) summary: String,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub(super) enum Outcome {
        Succeeded,
        Failed,
        Blocked,
    }
}

#[test]
fn an_older_origin_reads_a_no_report_failure_and_a_current_origin_derives_its_reason() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let spec: crate::spec::NormalizedSpec = serde_json::from_value(serde_json::json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "remote agent",
        "cwd": "/tmp",
        "machine": "executor",
        "timeout": "4h",
        "workload": { "type": "agent", "agent": "claude", "prompt": "work", "report_trailer": true }
    }))
    .unwrap();
    let id = TaskId::new();
    let row = crate::store::new_queued_task(crate::store::NewTask {
        id,
        name: spec.name.clone(),
        thread: spec.thread,
        workload: crate::invocation::persist_workload(&spec.workload),
        cwd: "/tmp".into(),
        timeout: spec.timeout,
        env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
        binary: "/bin/claude".into(),
    });
    store
        .insert_remote_task(&row, &spec, MachineId::new(), MachineId::new())
        .unwrap();
    store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    store
        .cas_exit(
            id,
            ProcessStatus::Running,
            &crate::domain::ExitReason::Exit { code: 0 },
        )
        .unwrap()
        .unwrap();

    let terminal = store
        .pending_outbound_events(id)
        .unwrap()
        .pop()
        .unwrap()
        .event;
    let wire = serde_json::to_value(&terminal).unwrap();
    assert!(
        wire["payload"]["event"].get("reason").is_none(),
        "the reason never travels: {wire}"
    );
    let old: v0_14::TaskEvent = serde_json::from_value(wire.clone()).unwrap();
    let v0_14::Payload::Callback { event, .. } = old.payload else {
        panic!("the terminal event is a callback");
    };
    assert_eq!(event.event, v0_14::Kind::Failed);
    assert_eq!(event.next_action, v0_14::NextAction::InspectLog);

    let current: TaskEvent = serde_json::from_value(wire).unwrap();
    let line = current.callback_message_line().unwrap().unwrap();
    let delivered: serde_json::Value =
        serde_json::from_str(line.strip_prefix("HOMEBASED_EVENT ").unwrap()).unwrap();
    assert_eq!(delivered["event"], "TASK_FAILED");
    assert_eq!(delivered["reason"], "no_report");
}
