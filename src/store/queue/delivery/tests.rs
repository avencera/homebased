use chrono::Utc;
use tempfile::tempdir;

use super::Store;
use crate::domain::{TaskEnv, TaskId};
use crate::events::EventAcceptance;
use crate::machine::MachineId;
use crate::queue::delivery::{JobRoute, JobSubmission, RoutedJobEvent};
use crate::queue::spec::JobSpec;
use crate::queue::{JobEvent, JobEventKind, JobId, QueueError, ResourceName};
use crate::store::queue::NewJob;
use crate::submission::{CallbackContext, CallbackExecutable};

fn route() -> JobRoute {
    let spec = JobSpec::parse_value(&serde_json::json!({
        "api_version": 1, "thread": "77777777-7777-4777-8777-777777777777",
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
        last_accepted_seq: 0,
        last_settled_seq: 0,
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
fn phase5_job_inbox_checks_owners_content_gaps_and_settlement_after_restart() {
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
        store
            .job_route(route.job)
            .unwrap()
            .unwrap()
            .last_settled_seq,
        2
    );
    second.event.seq = u64::MAX;
    assert!(store.accept_job_event(&second).is_err());
}

#[test]
fn phase5_job_route_retries_keep_context_and_refuse_spec_or_authority_changes() {
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
fn phase5_schema35_upgrade_preserves_jobs_and_active_runs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let route = route();
    let resource = store
        .register_resource(route.authority, ResourceName::gpu(0), None)
        .unwrap()
        .resource
        .id;
    store
        .submit_job(&NewJob {
            id: route.job,
            machine: route.authority,
            origin: route.origin,
            spec: route.spec,
            env: route.callback.env,
        })
        .unwrap();
    let task = TaskId::new();
    store
        .reserve_run(
            route.authority,
            route.job,
            resource,
            task,
            "/bin/true".into(),
            Utc::now(),
        )
        .unwrap();
    store
        .conn
        .execute_batch(
            "ALTER TABLE resources DROP COLUMN origin;
        ALTER TABLE resource_job_events DROP COLUMN suppressed_at;
        DROP TABLE resource_run_history; DROP TABLE resource_job_delivery;
        DROP TABLE resource_job_inbox; DROP TABLE resource_job_routes; PRAGMA user_version=35;",
        )
        .unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.job(route.job).unwrap().unwrap().runs, 1);
    assert_eq!(
        store.resource(resource).unwrap().unwrap().run.unwrap().task,
        task
    );
    let history = store.job_runs(route.job).unwrap();
    assert_eq!(history[0].resource, Some(resource));
    assert_eq!(history[0].task, task);
    assert!(store.pending_job_inbox().unwrap().is_empty());
}

#[test]
fn review_fix_suppressed_inbox_notice_retains_content_and_settles_in_order() {
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
fn review_fix_schema36_upgrade_retains_manual_registration_and_inbox_content() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let route = route();
    let resource = store
        .register_resource(route.authority, ResourceName::gpu(0), None)
        .unwrap();
    store.insert_job_route(&route).unwrap();
    let envelope = event(&route, 1);
    store.accept_job_event(&envelope).unwrap();
    store
        .conn
        .execute_batch(
            "ALTER TABLE resources DROP COLUMN origin;
        ALTER TABLE resource_job_events DROP COLUMN suppressed_at;
        ALTER TABLE resource_job_inbox DROP COLUMN suppressed_at;
        PRAGMA user_version=36;",
        )
        .unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(
        store.resource(resource.resource.id).unwrap().unwrap(),
        resource
    );
    assert_eq!(store.pending_job_inbox().unwrap()[0].1, envelope);
}
