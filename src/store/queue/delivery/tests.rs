use chrono::Utc;
use tempfile::tempdir;

use super::Store;
use crate::domain::TaskEnv;
use crate::events::EventAcceptance;
use crate::machine::MachineId;
use crate::queue::delivery::{HeldJobEnding, JobRefusal, JobRoute, JobSubmission, RoutedJobEvent};
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
        after: None,
        owes_final_event: false,
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
            resource_name: None,
            resource_label: None,
            process: None,
            attention: None,
            at: Utc::now(),
            blocked: None,
            held_ending: None,
            gpu_idle: None,
            gpu_summaries: Vec::new(),
            lease_idle: None,
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

    let waiting = store.waiting_threads().unwrap();
    assert_eq!(
        waiting,
        [queued.thread, ending_unsent.thread, notified.thread]
    );
}

#[test]
fn a_refused_held_job_ends_with_its_final_event_whichever_send_saw_the_refusal() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("homebased.db")).unwrap();
    let after = crate::dependency::Dependencies::of_tasks(&[crate::domain::TaskId::new()]).unwrap();
    let refusal = || JobRefusal {
        code: "invalid_cwd".into(),
        message: "cwd /gone does not exist".into(),
        error: serde_json::json!({ "error": { "code": "invalid_cwd" } }),
        status: 400,
    };

    // released after its build, and the reply to its send was lost
    let mut held = route();
    held.submission = JobSubmission::Held;
    held.after = Some(after.clone());
    held.owes_final_event = true;
    let held = store.insert_job_route(&held).unwrap();
    let released = store.release_held_job(held.job).unwrap();
    assert_eq!(released.submission, JobSubmission::Unknown);
    // a submit retry sends it again and is the one to see the refusal
    store.refuse_job(held.job, refusal()).unwrap();
    let ended = store.job_route(held.job).unwrap().unwrap();
    assert!(
        matches!(&ended.submission, JobSubmission::Ended { ending: HeldJobEnding::Refused { code, .. } } if code == "invalid_cwd"),
        "{:?}",
        ended.submission
    );
    let (_, final_event) = store
        .pending_job_inbox()
        .unwrap()
        .into_iter()
        .find(|(route, _)| route.job == held.job)
        .expect("the held job's waiters get no final event");
    assert_eq!(final_event.event.event, JobEventKind::JobFailed);

    // a job that was never held is refused for its submitter to read, with no event
    let mut ready = route();
    ready.after = Some(after);
    let ready = store.insert_job_route(&ready).unwrap();
    store.refuse_job(ready.job, refusal()).unwrap();
    assert!(matches!(
        store.job_route(ready.job).unwrap().unwrap().submission,
        JobSubmission::Rejected { status: 400, .. }
    ));
    assert!(
        store
            .pending_job_inbox()
            .unwrap()
            .iter()
            .all(|(route, _)| route.job != ready.job)
    );
}

#[test]
fn a_route_saved_before_job_dependencies_reads_and_saves_unchanged() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("homebased.db")).unwrap();
    let task = "0199a000-0000-7000-8000-000000000001";
    // the saved form of a held job that a failed task cancelled, from the
    // release before `after` took jobs
    let mut saved = serde_json::to_value(route()).unwrap();
    saved["after"] = serde_json::json!([task]);
    saved["owes_final_event"] = serde_json::json!(true);
    saved["submission"] = serde_json::json!({
        "state": "ended",
        "ending": { "reason": "dependency", "task": task, "outcome": "failed" }
    });
    let json = serde_json::to_string(&saved).unwrap();
    let job: JobId = saved["job"].as_str().unwrap().parse().unwrap();
    store
        .conn
        .execute(
            "INSERT INTO resource_job_routes(job_id,route_json) VALUES (?1,?2)",
            rusqlite::params![job.to_string(), json],
        )
        .unwrap();

    let read = store.job_route(job).unwrap().unwrap();
    let crate::queue::delivery::JobSubmission::Ended {
        ending: HeldJobEnding::Dependency { dependency, .. },
    } = &read.submission
    else {
        panic!("{:?}", read.submission);
    };
    assert_eq!(
        *dependency,
        crate::dependency::Dependency::Task(task.parse().unwrap())
    );
    assert_eq!(read.after.as_ref().unwrap().ids(), &[task.parse().unwrap()]);
    assert_eq!(serde_json::to_value(&read).unwrap(), saved);

    // a job ending names its `job`, and an ending must name exactly one of them
    let other = JobId::new();
    let ending = HeldJobEnding::Dependency {
        dependency: crate::dependency::Dependency::Job(other),
        outcome: crate::dependency::TaskOutcome::Cancelled.into(),
    };
    assert_eq!(
        serde_json::to_value(&ending).unwrap(),
        serde_json::json!({ "reason": "dependency", "job": other, "outcome": "cancelled" })
    );
    for both in [
        serde_json::json!({ "reason": "dependency", "outcome": "failed" }),
        serde_json::json!({ "reason": "dependency", "task": task, "job": other, "outcome": "failed" }),
    ] {
        assert!(serde_json::from_value::<HeldJobEnding>(both).is_err());
    }
}

/// A route exactly as v0.25.4 saved it, before specs had `gpu_idle`
const ROUTE_BEFORE_GPU_IDLE: &str = r#"{"job":"01a1bed3-41be-7471-b472-12bc75437215","origin":"01a1bed3-41bf-763b-9b53-114a4a3d3cb4","authority":"01a1bed3-41bf-763b-9b53-114b1045a8c1","thread":"77777777-7777-4777-8777-777777777777","callback":{"env":{"path":"/bin","home":"/tmp"},"cwd":"/tmp","codex":{"type":"available","path":"/bin/true"}},"spec":{"api_version":1,"thread":"77777777-7777-4777-8777-777777777777","name":"route test","cwd":"/tmp","timeout":"1h","priority":"low","preempt":{"mode":"wait"},"steps":[{"type":"task","command":["/bin/true"]}]},"digest":"9efe2b4b0da17021ea901eb18a773a1b51258cfe6900be2b8957e64e816b5eb1","target":null,"submission":{"state":"unknown"}}"#;

#[test]
fn a_route_saved_before_gpu_idle_keeps_its_digest() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    let job: JobId = "01a1bed3-41be-7471-b472-12bc75437215".parse().unwrap();
    store
        .conn
        .execute(
            "INSERT INTO resource_job_routes (job_id, route_json) VALUES (?1, ?2)",
            rusqlite::params![job.to_string(), ROUTE_BEFORE_GPU_IDLE],
        )
        .unwrap();
    // reading checks the saved digest against the spec's canonical form
    let route = store.job_route(job).unwrap().unwrap();
    assert_eq!(route.spec.gpu_idle, None);
    assert_eq!(
        route.digest,
        "9efe2b4b0da17021ea901eb18a773a1b51258cfe6900be2b8957e64e816b5eb1"
    );
    let saved: serde_json::Value = serde_json::from_str(ROUTE_BEFORE_GPU_IDLE).unwrap();
    assert_eq!(serde_json::to_value(&route.spec).unwrap(), saved["spec"]);
}
