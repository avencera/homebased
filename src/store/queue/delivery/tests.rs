use chrono::Utc;
use tempfile::tempdir;

use super::Store;
use crate::domain::TaskEnv;
use crate::events::EventAcceptance;
use crate::machine::MachineId;
use crate::queue::delivery::{JobRoute, JobSubmission, RoutedJobEvent};
use crate::queue::spec::JobSpec;
use crate::queue::{JobEvent, JobEventKind, JobId, QueueError};
use crate::submission::{CallbackContext, CallbackExecutable};

fn route() -> JobRoute {
    route_for("77777777-7777-4777-8777-777777777777")
}

fn route_for(thread: &str) -> JobRoute {
    let spec = JobSpec::parse_value(&serde_json::json!({
        "api_version": 1, "thread": thread,
        "name": "route test", "cwd": "/tmp", "priority": "low", "preempt": { "mode": "wait" },
        "workload": { "type": "task", "command": ["/bin/true"] }
    }))
    .unwrap();
    JobRoute {
        job: JobId::new(),
        origin: MachineId::new(),
        authority: MachineId::new(),
        thread: spec.thread,
        digest: spec.digest().unwrap(),
        spec,
        callback: CallbackContext {
            env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
            cwd: "/tmp".into(),
            codex: CallbackExecutable::available("/bin/true".into()),
        },
        target: None,
        submission: JobSubmission::Unknown,
    }
}

fn event(route: &JobRoute, seq: u64) -> RoutedJobEvent {
    RoutedJobEvent {
        origin: route.origin,
        authority: route.authority,
        digest: route.digest.clone(),
        event: JobEvent {
            job: route.job,
            seq,
            event: JobEventKind::JobCancelled,
            run: None,
            process: None,
            attention: None,
            at: Utc::now(),
            blocked: None,
        },
    }
}

#[test]
fn job_inbox_checks_owners_content_gaps_and_settlement_after_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let route = route();
    store.insert_job_route(&route).unwrap();
    let mut second = event(&route, 2);
    assert_eq!(
        store.accept_job_event(&second).unwrap(),
        EventAcceptance::Expected { seq: 1 }
    );
    let first = event(&route, 1);
    let mut wrong_owner = first.clone();
    wrong_owner.authority = MachineId::new();
    assert!(matches!(
        store.accept_job_event(&wrong_owner),
        Err(crate::error::AppError::Queue(
            QueueError::JobConflict { .. }
        ))
    ));
    assert_eq!(
        store.accept_job_event(&first).unwrap(),
        EventAcceptance::Acknowledged { seq: 1 }
    );
    assert_eq!(
        store.accept_job_event(&first).unwrap(),
        EventAcceptance::Acknowledged { seq: 1 }
    );
    assert!(matches!(
        store.job_route(route.job).unwrap().unwrap().submission,
        JobSubmission::Accepted
    ));
    let mut different = first.clone();
    different.event.at += chrono::Duration::seconds(1);
    assert!(store.accept_job_event(&different).is_err());
    assert_eq!(store.pending_job_inbox().unwrap().len(), 1);
    store.accept_job_event(&second).unwrap();
    assert!(store.settle_job_event(route.job, 2).is_err());
    store.settle_job_event(route.job, 1).unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.pending_job_inbox().unwrap()[0].1.event.seq, 2);
    store.settle_job_event(route.job, 2).unwrap();
    store.settle_job_event(route.job, 2).unwrap();
    assert!(store.pending_job_inbox().unwrap().is_empty());
    assert_eq!(
        store.job_route_cursors(route.job).unwrap().unwrap().settled,
        2
    );
    second.event.seq = u64::MAX;
    assert!(store.accept_job_event(&second).is_err());
}

#[test]
fn job_route_retries_keep_context_and_refuse_spec_or_authority_changes() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let saved = route();
    store.insert_job_route(&saved).unwrap();
    let mut retry = saved.clone();
    retry.callback.env.home = "/elsewhere".into();
    assert_eq!(store.insert_job_route(&retry).unwrap(), saved);
    retry.authority = MachineId::new();
    assert!(store.insert_job_route(&retry).is_err());
    let mut retry = saved.clone();
    retry.spec.priority = crate::queue::Priority::High;
    retry.digest = retry.spec.digest().unwrap();
    assert!(store.insert_job_route(&retry).is_err());
}

#[test]
fn suppressed_inbox_notice_retains_content_and_settles_in_order() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let route = route();
    store.insert_job_route(&route).unwrap();
    let mut notice = event(&route, 1);
    notice.event.event = JobEventKind::JobBlocked;
    let success = event(&route, 2);
    store.accept_job_event(&notice).unwrap();
    store.accept_job_event(&success).unwrap();
    assert!(store.suppress_job_notice(route.job, 2).is_err());
    store.suppress_job_notice(route.job, 1).unwrap();
    store.suppress_job_notice(route.job, 1).unwrap();
    assert_eq!(
        store.accept_job_event(&notice).unwrap(),
        EventAcceptance::Acknowledged { seq: 1 }
    );
    assert_eq!(store.pending_job_inbox().unwrap()[0].1, success);
    let suppressed: bool = store
        .conn
        .query_row(
            "SELECT suppressed_at IS NOT NULL FROM resource_job_inbox WHERE job_id=?1 AND seq=1",
            [route.job.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(suppressed);
    store.settle_job_event(route.job, 2).unwrap();
    assert!(store.pending_job_inbox().unwrap().is_empty());
}

#[test]
fn a_job_thread_waits_until_an_ending_reaches_it() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let thread = |n: u8| format!("{n}{n}{n}{n}{n}{n}{n}{n}-7777-4777-8777-777777777777");
    let queued = route_for(&thread(1));
    let ended = route_for(&thread(2));
    let ending_unsent = route_for(&thread(3));
    let notified = route_for(&thread(4));
    let rejected = route_for(&thread(5));
    let abandoned = route_for(&thread(6));
    for route in [
        &queued,
        &ended,
        &ending_unsent,
        &notified,
        &rejected,
        &abandoned,
    ] {
        store.insert_job_route(route).unwrap();
    }

    store.accept_job_event(&event(&ended, 1)).unwrap();
    store.settle_job_event(ended.job, 1).unwrap();
    store.accept_job_event(&event(&ending_unsent, 1)).unwrap();
    let mut notice = event(&notified, 1);
    notice.event.event = JobEventKind::JobBlocked;
    store.accept_job_event(&notice).unwrap();
    store.settle_job_event(notified.job, 1).unwrap();
    let refusal = JobSubmission::Rejected {
        error: serde_json::json!({ "error": "invalid_spec" }),
        status: 400,
    };
    store
        .resolve_job_route(rejected.job, refusal, None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE resource_job_routes SET created_at = '2026-01-01T00:00:00.000Z'
             WHERE job_id = ?1",
            [abandoned.job.to_string()],
        )
        .unwrap();

    let waiting: Vec<_> = store
        .waiting_threads()
        .unwrap()
        .into_iter()
        .map(|(thread, _)| thread)
        .collect();
    assert_eq!(
        waiting,
        [queued.thread, ending_unsent.thread, notified.thread]
    );
}
