use super::{Daemon, add_peer, dependency_fleet, register_thread, wait_until};
use std::fs;
use std::time::Duration;

use homebased::domain::{TaskEnv, ThreadId};
use homebased::fleet::http::ClusterClient;
use homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
use homebased::queue::delivery::RoutedJobEvent;
use homebased::queue::{JobId, OperationId};
use homebased::store::Store;
use serde_json::{Value, json};

struct Jobs<'a> {
    executor: &'a Daemon,
    ids: Vec<JobId>,
}

impl Drop for Jobs<'_> {
    fn drop(&mut self) {
        for job in &self.ids {
            let _ = self
                .executor
                .cmd()
                .args(["resource", "job", "cancel", &job.to_string()])
                .output();
        }
        let _ = wait_until(Duration::from_secs(12), || {
            let Ok(store) = Store::open(&self.executor.home.join(homebased::home::DB_NAME)) else {
                return false;
            };
            self.ids.iter().all(|job| {
                store.job_runs(*job).is_ok_and(|runs| {
                    runs.iter().all(|run| {
                        run.status.is_terminal()
                            && homebased::home::flock_exclusive(
                                &self
                                    .executor
                                    .home
                                    .join("tasks")
                                    .join(run.task.to_string())
                                    .join("runner.lock"),
                                homebased::home::LockMode::NonBlocking,
                            )
                            .is_ok()
                    })
                })
            })
        });
    }
}

fn spec(origin: &Daemon, executor: &Daemon, script: &str) -> Value {
    let thread = ThreadId(uuid::Uuid::now_v7());
    register_thread(&origin.user_home, &thread.to_string());
    json!({ "api_version": 1, "thread": thread, "machine": "remote-executor", "name": "Fleet job",
        "cwd": executor.home, "priority": "medium", "preempt": { "mode": "wait" },
        "resource": "gpu0", "workload": { "type": "task", "command": ["/bin/sh", "-c", script] } })
}

fn submit(origin: &Daemon, job: JobId, spec: &Value) -> std::process::Output {
    let path = origin.home.join(format!("job-{job}.json"));
    fs::write(&path, serde_json::to_vec(spec).unwrap()).unwrap();
    origin
        .cmd()
        .current_dir(&origin.user_home)
        .args([
            "--json",
            "resource",
            "job",
            "submit",
            "--job-id",
            &job.to_string(),
            "--spec",
        ])
        .arg(path)
        .output()
        .unwrap()
}

fn cli(daemon: &Daemon, args: &[&str]) -> Value {
    let output = daemon.cmd().arg("--json").args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["api_version"], 1);
    value
}

fn submit_ok(origin: &Daemon, job: JobId, spec: &Value) -> Value {
    let output = submit(origin, job, spec);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn store(daemon: &Daemon) -> Store {
    Store::open(&daemon.home.join(homebased::home::DB_NAME)).unwrap()
}

fn events(origin: &Daemon, job: JobId) -> Vec<Value> {
    fs::read_to_string(origin.user_home.join("callbacks"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            line.split_once("HOMEBASED_EVENT ")
                .map(|(_, payload)| payload)
        })
        .map(|payload| serde_json::from_str::<Value>(payload).unwrap())
        .filter(|event| event["job"] == job.to_string())
        .collect()
}

fn wait_event(origin: &Daemon, job: JobId, count: u64) -> Vec<Value> {
    assert!(
        wait_until(Duration::from_secs(20), || store(origin)
            .job_route_cursors(job)
            .unwrap()
            .is_some_and(|cursors| cursors.settled == count)),
        "job callback missing: {:?}",
        events(origin, job)
    );
    events(origin, job)
}

#[tokio::test(flavor = "multi_thread")]
async fn fleet_cli_submit_keeps_events_at_origin_and_deduplicates() {
    let (origin, executor) = dependency_fleet("job-route");
    let job = JobId::new();
    let _jobs = Jobs {
        executor: &executor,
        ids: vec![job],
    };
    let spec = spec(&origin, &executor, "echo remote > ran; exit 0");
    let value = submit_ok(&origin, job, &spec);
    assert_eq!(value["machine"], executor.machine_id().to_string());
    let received = wait_event(&origin, job, 1);
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["event"], "JOB_SUCCEEDED");
    assert_eq!(received[0]["thread"], spec["thread"]);
    assert!(executor.home.join("ran").exists());
    assert!(!origin.home.join("ran").exists());
    assert!(!executor.user_home.join("callbacks").exists());
    let route = store(&origin).job_route(job).unwrap().unwrap();
    assert_eq!(route.authority, executor.machine_id());
    let cursors = store(&origin).job_route_cursors(job).unwrap().unwrap();
    assert_eq!(cursors.accepted, 1);
    let run = store(&executor).job_runs(job).unwrap().remove(0);
    assert!(
        store(&executor)
            .origin_route_by_task(run.task)
            .unwrap()
            .is_none()
    );
    let envelope = RoutedJobEvent {
        origin: origin.machine_id(),
        authority: executor.machine_id(),
        digest: route.digest,
        event: store(&executor).job_events(job).unwrap().remove(0),
    };
    let body = json!({
        "api_version": 1,
        "protocol_version": CLUSTER_PROTOCOL_VERSION,
        "destination_machine": origin.machine_id(),
        "event": envelope,
    });
    let response = ClusterClient::default()
        .post_json(&origin.address(), "/v1/cluster/job-events", &body)
        .await
        .unwrap();
    assert_eq!(response.status.as_u16(), 200);
    let ack: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(ack["result"]["seq"], 1);
    assert_eq!(events(&origin, job).len(), 1);
    let detail = cli(&origin, &["resource", "job", "show", &job.to_string()]);
    assert_eq!(detail["runs"][0]["task"], run.task.to_string());
    let remote_resources = cli(
        &origin,
        &["resource", "list", "--machine", "remote-executor"],
    );
    assert_eq!(
        remote_resources["machine"],
        executor.machine_id().to_string()
    );
}

#[test]
fn fleet_lost_submit_response_retries_and_conflicts_without_duplicate_work() {
    use std::io::Write;
    use std::os::unix::net::UnixStream;

    let (origin, executor) = dependency_fleet("job-retry");
    let job = JobId::new();
    let _jobs = Jobs {
        executor: &executor,
        ids: vec![job],
    };
    let spec = spec(&origin, &executor, "echo one >> count; exit 0");
    let env = TaskEnv {
        path: std::env::var("PATH").unwrap(),
        home: origin.user_home.display().to_string(),
    };
    let body = serde_json::to_vec(&json!({
        "job_id": job,
        "spec": spec,
        "env": env,
        "callback_cwd": origin.user_home,
    }))
    .unwrap();
    let mut socket = UnixStream::connect(origin.home.join("homebased.sock")).unwrap();
    let length = body.len();
    write!(
        socket,
        "POST /v1/resource/jobs HTTP/1.1\r\nHost: localhost\r\n\
         Content-Type: application/json\r\nContent-Length: {length}\r\n\r\n"
    )
    .unwrap();
    socket.write_all(&body).unwrap();
    assert!(wait_until(Duration::from_secs(10), || store(&executor)
        .job(job)
        .unwrap()
        .is_some()));
    drop(socket);
    let retry = submit_ok(&origin, job, &spec);
    assert_eq!(retry["job_id"], job.to_string());
    wait_event(&origin, job, 1);
    assert_eq!(store(&executor).job_runs(job).unwrap().len(), 1);
    assert_eq!(
        fs::read_to_string(executor.home.join("count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    let mut changed = spec.clone();
    changed["priority"] = json!("high");
    let conflict = submit(&origin, job, &changed);
    assert_eq!(conflict.status.code(), Some(5));
    let error: Value = serde_json::from_slice(&conflict.stderr).unwrap();
    assert_eq!(error["error"]["code"], "job_conflict");
    assert_eq!(store(&executor).job_runs(job).unwrap().len(), 1);
}

#[test]
fn fleet_offline_origin_receives_stored_job_events_after_return() {
    let (mut origin, executor) = dependency_fleet("job-offline");
    let job = JobId::new();
    let _jobs = Jobs {
        executor: &executor,
        ids: vec![job],
    };
    let spec = spec(
        &origin,
        &executor,
        "echo ready > ready; n=0; while [ ! -f finish ] && [ $n -lt 150 ]; do sleep 0.1; n=$((n+1)); done; exit 0",
    );
    submit_ok(&origin, job, &spec);
    assert!(wait_until(Duration::from_secs(10), || executor
        .home
        .join("ready")
        .exists()));
    origin.stop();
    fs::write(executor.home.join("finish"), "").unwrap();
    assert!(wait_until(Duration::from_secs(10), || store(&executor)
        .job_events(job)
        .unwrap()
        .len()
        == 1));
    assert!(
        store(&executor)
            .pending_job_outbox()
            .unwrap()
            .iter()
            .any(|event| event.event.job == job)
    );
    assert!(events(&origin, job).is_empty());
    origin.restart();
    assert_eq!(wait_event(&origin, job, 1)[0]["event"], "JOB_SUCCEEDED");
    assert!(wait_until(Duration::from_secs(10), || store(&executor)
        .pending_job_outbox()
        .unwrap()
        .iter()
        .all(|event| event.event.job != job)));
}

#[test]
fn fleet_moves_and_cancels_use_saved_authority_and_explicit_machine() {
    let (origin, executor) = dependency_fleet("job-operations");
    let a = JobId::new();
    let b = JobId::new();
    let c = JobId::new();
    let _jobs = Jobs {
        executor: &executor,
        ids: vec![a, b, c],
    };
    submit_ok(
        &origin,
        a,
        &spec(
            &origin,
            &executor,
            "echo ready > ready; n=0; while [ ! -f finish ] && [ $n -lt 150 ]; do sleep 0.1; n=$((n+1)); done",
        ),
    );
    assert!(wait_until(Duration::from_secs(10), || executor
        .home
        .join("ready")
        .exists()));
    submit_ok(&origin, b, &spec(&origin, &executor, "exit 0"));
    submit_ok(&origin, c, &spec(&origin, &executor, "exit 0"));
    let operation = OperationId::new().to_string();
    let args = [
        "resource",
        "job",
        "move",
        &c.to_string(),
        "--front",
        "--operation-id",
        &operation,
    ];
    let first = cli(&origin, &args);
    cli(
        &executor,
        &["resource", "job", "move", &b.to_string(), "--front"],
    );
    assert_eq!(first, cli(&origin, &args));
    let queue = cli(
        &origin,
        &["resource", "jobs", "--machine", "remote-executor"],
    );
    assert_eq!(queue["jobs"][0]["id"], b.to_string());
    assert_eq!(queue["jobs"][1]["id"], c.to_string());
    let conflict = origin
        .cmd()
        .args([
            "--json",
            "resource",
            "job",
            "cancel",
            &c.to_string(),
            "--operation-id",
            &operation,
        ])
        .output()
        .unwrap();
    assert_eq!(conflict.status.code(), Some(5));
    let outside = Daemon::start("outside-origin", true);
    add_peer(&outside, &executor);
    cli(
        &outside,
        &[
            "resource",
            "job",
            "cancel",
            &b.to_string(),
            "--machine",
            "remote-executor",
        ],
    );
    assert_eq!(wait_event(&origin, b, 1)[0]["event"], "JOB_CANCELLED");
    cli(&origin, &["resource", "job", "cancel", &a.to_string()]);
    assert_eq!(wait_event(&origin, a, 1)[0]["event"], "JOB_CANCELLED");
    assert_eq!(wait_event(&origin, c, 1)[0]["event"], "JOB_SUCCEEDED");
}

#[test]
fn fleet_offline_origin_suppresses_an_ended_blocked_notice() {
    use homebased::queue::schedule::NoticeThresholds;
    let (mut origin, executor) = dependency_fleet("notice-offline");
    let low = JobId::new();
    let head = JobId::new();
    let _jobs = Jobs {
        executor: &executor,
        ids: vec![low, head],
    };
    submit_ok(
        &origin,
        low,
        &spec(
            &origin,
            &executor,
            "echo ready > ready; n=0; while [ ! -f finish ] && [ $n -lt 600 ]; do sleep 0.1; n=$((n+1)); done; exit 0",
        ),
    );
    assert!(wait_until(Duration::from_secs(10), || executor
        .home
        .join("ready")
        .exists()));
    let mut head_spec = spec(&origin, &executor, "exit 0");
    head_spec["priority"] = json!("high");
    submit_ok(&origin, head, &head_spec);
    origin.stop();
    assert!(wait_until(Duration::from_secs(10), || store(&executor)
        .blocked_episode(executor.machine_id())
        .unwrap()
        .is_some_and(|episode| episode.job == head)));
    let authority = store(&executor);
    let episode = authority
        .blocked_episode(executor.machine_id())
        .unwrap()
        .unwrap();
    assert!(
        authority
            .produce_job_blocked(
                executor.machine_id(),
                episode,
                NoticeThresholds {
                    after_yield: Duration::ZERO,
                    after_wait: Duration::ZERO
                },
                chrono::Utc::now()
            )
            .unwrap()
    );
    fs::write(executor.home.join("finish"), "").unwrap();
    assert!(wait_until(Duration::from_secs(15), || store(&executor)
        .job_events(head)
        .unwrap()
        .len()
        == 2));
    assert!(events(&origin, head).is_empty());
    origin.restart();
    let received = wait_event(&origin, head, 2);
    assert_eq!(
        received.len(),
        1,
        "an obsolete blocked notice must not reach the callback"
    );
    assert_eq!(received[0]["event"], "JOB_SUCCEEDED");
    assert_eq!(received[0]["seq"], 2);
    assert_eq!(store(&executor).job_events(head).unwrap().len(), 2);
}
