use super::{Harness, THREAD, event_json, register_thread, wait_until};
use std::fs;
use std::time::Duration;

use homebased::domain::{TaskEnv, TaskId};
use homebased::machine::MachineId;
use homebased::queue::spec::JobSpec;
use homebased::queue::{CleanupFailure, JobId, OperationId, RunPhase};
use homebased::store::Store;
use homebased::store::queue::NewJob;
use serde_json::{Value, json};

struct Jobs<'a> {
    harness: &'a Harness,
    ids: Vec<JobId>,
}

impl<'a> Jobs<'a> {
    fn new(harness: &'a Harness) -> Self {
        Self {
            harness,
            ids: Vec::new(),
        }
    }

    fn submit(&mut self, spec: &Value) -> JobId {
        let job = JobId::new();
        self.ids.push(job);
        let path = self.harness.home.join(format!("job-{job}.json"));
        fs::write(&path, serde_json::to_vec(spec).unwrap()).unwrap();
        let output = self
            .harness
            .cmd()
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
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["job_id"], job.to_string());
        assert_eq!(value["api_version"], 1);
        job
    }
}

impl Drop for Jobs<'_> {
    fn drop(&mut self) {
        for job in &self.ids {
            let _ = self
                .harness
                .cmd()
                .args(["resource", "job", "cancel", &job.to_string()])
                .output();
        }
        let _ = wait_until(Duration::from_secs(12), || {
            let Ok(store) = Store::open(&self.harness.home.join("homebased.sqlite")) else {
                return false;
            };
            self.ids.iter().all(|job| {
                store.job_runs(*job).is_ok_and(|runs| {
                    runs.iter().all(|run| {
                        run.status.is_terminal()
                            && homebased::home::flock_exclusive(
                                &self
                                    .harness
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

/// Report readiness, then run until the test creates `finish`, for at most 15 seconds
const WAIT_FOR_FINISH: &str = "echo ready > ready; n=0; while [ ! -f finish ] && [ $n -lt 150 ]; do sleep 0.1; n=$((n+1)); done; exit 0";

fn spec(h: &Harness, priority: &str, mode: &str, script: &str) -> Value {
    json!({ "api_version": 1, "thread": THREAD, "name": "CLI queue test", "cwd": h.home,
        "priority": priority, "preempt": { "mode": mode }, "resource": "gpu0",
        "workload": { "type": "task", "command": ["/bin/sh", "-c", script] } })
}

fn cli(h: &Harness, args: &[&str]) -> Value {
    let output = h.cmd().arg("--json").args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["api_version"], 1);
    value
}

fn show(h: &Harness, job: JobId) -> Value {
    cli(h, &["resource", "job", "show", &job.to_string()])
}

fn wait_state(h: &Harness, job: JobId, state: &str) -> Value {
    assert!(
        wait_until(
            Duration::from_secs(30),
            || show(h, job)["job"]["state"]["state"] == state
        ),
        "job {job}: {}",
        show(h, job)
    );
    show(h, job)
}

fn callbacks(h: &Harness, job: JobId) -> Vec<Value> {
    h.queue_messages()
        .iter()
        .map(|line| event_json(line))
        .filter(|event| event["job"] == job.to_string())
        .collect()
}

fn wait_callbacks(h: &Harness, job: JobId, count: usize) -> Vec<Value> {
    assert!(
        wait_until(Duration::from_secs(15), || {
            Store::open(&h.home.join("homebased.sqlite"))
                .unwrap()
                .job_route(job)
                .unwrap()
                .is_some_and(|route| route.last_settled_seq == count as u64)
        }),
        "callbacks: {:?}",
        h.queue_messages()
    );
    callbacks(h, job)
}

#[test]
fn cli_steps_emit_only_one_final_job_callback_and_refuse_run_dependencies() {
    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    let mut steps = spec(&h, "medium", "wait", "exit 0");
    steps.as_object_mut().unwrap().remove("workload");
    steps["steps"] = json!([
        { "type": "task", "command": ["/bin/sh", "-c", "echo first > first; exit 0"] },
        { "type": "task", "command": ["/bin/sh", "-c", "echo second > second; n=0; while [ ! -f finish ] && [ $n -lt 150 ]; do sleep 0.1; n=$((n+1)); done; exit 0"] }
    ]);
    let job = jobs.submit(&steps);
    // the next step starts only after cleanup confirms, which takes several
    // scans while parallel tests keep starting and exiting processes
    assert!(wait_until(Duration::from_secs(30), || h
        .home
        .join("second")
        .exists()));
    assert!(
        callbacks(&h, job).is_empty(),
        "intermediate step emitted a callback"
    );
    let detail = show(&h, job);
    assert_eq!(detail["runs"].as_array().unwrap().len(), 2);
    let run = detail["runs"][0]["task"].as_str().unwrap();
    let mut ordinary = Harness::task_spec(&["/bin/echo", "after"]);
    ordinary["after"] = json!([run]);
    let path = h.home.join("after-run.json");
    fs::write(&path, serde_json::to_vec(&ordinary).unwrap()).unwrap();
    let refused = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(path)
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&refused.stderr).contains("run task"));
    fs::write(h.home.join("finish"), "").unwrap();
    wait_state(&h, job, "succeeded");
    let events = wait_callbacks(&h, job, 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event"], "JOB_SUCCEEDED");
    assert_eq!(events[0]["step"], 1);
    assert_eq!(events[0]["run_number"], 2);
    assert_eq!(events[0]["thread"], THREAD);
    assert_eq!(events[0]["seq"], 1);
    assert_eq!(events[0]["task"], show(&h, job)["runs"][1]["task"]);
    assert!(wait_until(
        Duration::from_secs(10),
        || show(&h, job)["runs"][1]["cleanup"]["Ok"].is_null()
            && Store::open(&h.home.join("homebased.sqlite"))
                .unwrap()
                .resources_on(machine(&h))
                .unwrap()
                .iter()
                .all(|r| r.run.is_none())
    ));
}

fn machine(h: &Harness) -> MachineId {
    fs::read_to_string(h.home.join("machine-id"))
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn yield_callbacks_are_ordered_and_keep_each_thread() {
    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    let body = "if [ \"$HOMEBASED_RESUME\" = 1 ]; then exit 0; fi; echo ready > ready; n=0; while [ ! -f \"$HOMEBASED_YIELD_FILE\" ] && [ $n -lt 150 ]; do sleep 0.1; n=$((n+1)); done; exit 75";
    let low = jobs.submit(&spec(&h, "low", "yield", body));
    assert!(wait_until(Duration::from_secs(10), || h
        .home
        .join("ready")
        .exists()));
    let thread = uuid::Uuid::now_v7().to_string();
    register_thread(&h.user_home, &thread);
    let mut high_spec = spec(&h, "high", "wait", "exit 0");
    high_spec["thread"] = json!(thread);
    let high = jobs.submit(&high_spec);
    wait_state(&h, high, "succeeded");
    wait_state(&h, low, "succeeded");
    let low_events = wait_callbacks(&h, low, 2);
    assert_eq!(
        low_events
            .iter()
            .map(|event| event["event"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["JOB_PREEMPTED", "JOB_SUCCEEDED"]
    );
    assert_eq!(low_events[0]["seq"], 1);
    assert_eq!(low_events[1]["seq"], 2);
    assert_ne!(low_events[0]["task"], low_events[1]["task"]);
    assert!(low_events.iter().all(|event| event["thread"] == THREAD));
    assert_eq!(wait_callbacks(&h, high, 1)[0]["thread"], thread);
    assert_eq!(show(&h, low)["last_stop_cause"], "yield");
}

#[test]
fn cli_queue_moves_replay_stored_results_and_cancel() {
    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    let active = jobs.submit(&spec(&h, "medium", "wait", WAIT_FOR_FINISH));
    assert!(wait_until(Duration::from_secs(10), || h
        .home
        .join("ready")
        .exists()));
    cli(&h, &["resource", "register", "--name", "manual"]);
    let resources = cli(&h, &["resource", "list"]);
    let names = resources["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["resource"]["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(names.contains(&"gpu0") && names.contains(&"manual"));
    let b = jobs.submit(&spec(&h, "low", "wait", "exit 0"));
    let c = jobs.submit(&spec(&h, "low", "wait", "exit 0"));
    let operation = OperationId::new().to_string();
    let args = [
        "resource",
        "job",
        "move",
        &c.to_string(),
        "--priority",
        "high",
        "--front",
        "--operation-id",
        &operation,
    ];
    let first = cli(&h, &args);
    cli(
        &h,
        &[
            "resource",
            "job",
            "move",
            &b.to_string(),
            "--before",
            &c.to_string(),
        ],
    );
    let replay = cli(&h, &args);
    assert_eq!(first, replay);
    let order = cli(&h, &["resource", "jobs"]);
    assert_eq!(order["jobs"][0]["id"], b.to_string());
    assert_eq!(order["jobs"][1]["id"], c.to_string());
    assert_eq!(order["jobs"][0]["target"]["type"], "pinned");
    let conflict = h
        .cmd()
        .args([
            "--json",
            "resource",
            "job",
            "move",
            &c.to_string(),
            "--back",
            "--operation-id",
            &operation,
        ])
        .output()
        .unwrap();
    assert_eq!(conflict.status.code(), Some(5));
    let error: Value = serde_json::from_slice(&conflict.stderr).unwrap();
    assert_eq!(error["error"]["code"], "operation_conflict");
    let cancel_op = OperationId::new().to_string();
    let cancel_args = [
        "resource",
        "job",
        "cancel",
        &b.to_string(),
        "--operation-id",
        &cancel_op,
    ];
    assert_eq!(cli(&h, &cancel_args), cli(&h, &cancel_args));
    wait_state(&h, b, "cancelled");
    cli(&h, &["resource", "job", "cancel", &active.to_string()]);
    wait_state(&h, active, "cancelled");
    wait_state(&h, c, "succeeded");
    assert_eq!(wait_callbacks(&h, b, 1)[0]["event"], "JOB_CANCELLED");
    assert_eq!(wait_callbacks(&h, active, 1)[0]["event"], "JOB_CANCELLED");
}

#[test]
fn attention_release_refuses_stale_identity_and_replays() {
    let mut h = Harness::new();
    assert!(
        cli(&h, &["resource", "list"])["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["resource"]["name"] == "gpu0")
    );
    h.stop_daemon();
    let store = Store::open(&h.home.join("homebased.sqlite")).unwrap();
    let machine = machine(&h);
    let resource = store.resources_on(machine).unwrap()[0].resource.id;
    let job = JobId::new();
    let task = TaskId::new();
    store
        .submit_job(&NewJob {
            id: job,
            machine,
            origin: machine,
            spec: JobSpec::parse_value(&spec(&h, "low", "wait", "exit 0")).unwrap(),
            env: TaskEnv {
                path: "/bin".into(),
                home: h.user_home.display().to_string(),
            },
        })
        .unwrap();
    store
        .reserve_run(
            machine,
            job,
            resource,
            task,
            "/bin/sh".into(),
            chrono::Utc::now(),
        )
        .unwrap();
    store
        .cas_status(
            task,
            homebased::domain::ProcessStatus::Queued,
            homebased::domain::ProcessStatus::Running,
        )
        .unwrap();
    store
        .mark_run_executing(resource, task, chrono::Utc::now())
        .unwrap();
    store
        .cas_exit(
            task,
            homebased::domain::ProcessStatus::Running,
            &homebased::domain::ExitReason::Exit { code: 0 },
        )
        .unwrap();
    let attempt = store.begin_cleanup_attempt(resource, task).unwrap();
    let Some(RunPhase::Attention { id, .. }) = store
        .apply_cleanup_result(
            resource,
            task,
            attempt,
            Err(CleanupFailure::ProcessGroupUnconfirmed),
        )
        .unwrap()
    else {
        panic!("attention missing");
    };
    let job_spec = store.job(job).unwrap().unwrap().spec;
    let route = homebased::queue::delivery::JobRoute {
        job,
        origin: machine,
        authority: machine,
        thread: job_spec.thread,
        digest: job_spec.digest().unwrap(),
        spec: job_spec,
        callback: homebased::submission::CallbackContext {
            env: TaskEnv {
                path: h.path.clone(),
                home: h.user_home.display().to_string(),
            },
            cwd: h.home.clone(),
            codex: homebased::submission::CallbackExecutable::available(super::fixture(
                "fake-codex",
            )),
        },
        target: None,
        submission: homebased::queue::delivery::JobSubmission::Unknown,
        last_accepted_seq: 0,
        last_settled_seq: 0,
    };
    store.insert_job_route(&route).unwrap();
    store
        .resolve_job_route(
            job,
            homebased::queue::delivery::JobSubmission::Accepted,
            Some(store.job(job).unwrap().unwrap().target),
        )
        .unwrap();
    h.start_daemon();
    let delivered = wait_callbacks(&h, job, 2);
    assert_eq!(delivered[0]["event"], "JOB_SUCCEEDED");
    assert_eq!(delivered[1]["event"], "JOB_ATTENTION");
    assert_eq!(delivered[1]["attention"], id.to_string());
    assert_eq!(delivered[1]["task"], task.to_string());
    let stale = homebased::queue::AttentionId::new().to_string();
    let refused = h
        .cmd()
        .args(["--json", "resource", "release", "--attention", &stale])
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(3));
    let operation = OperationId::new().to_string();
    let attention = id.to_string();
    let args = [
        "resource",
        "release",
        "--attention",
        &attention,
        "--operation-id",
        &operation,
    ];
    assert_eq!(cli(&h, &args), cli(&h, &args));
    assert!(store.resource(resource).unwrap().unwrap().run.is_none());
}

#[test]
fn blocked_and_check_due_callbacks_only_reach_the_affected_job() {
    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    let active = jobs.submit(&spec(&h, "low", "wait", WAIT_FOR_FINISH));
    assert!(wait_until(Duration::from_secs(10), || h
        .home
        .join("ready")
        .exists()));
    let mut high_spec = spec(&h, "high", "wait", "exit 0");
    let high_thread = uuid::Uuid::now_v7().to_string();
    register_thread(&h.user_home, &high_thread);
    high_spec["thread"] = json!(high_thread);
    let blocked = jobs.submit(&high_spec);
    let store = h.store();
    assert!(wait_until(Duration::from_secs(10), || store
        .blocked_episode(machine(&h))
        .unwrap()
        .is_some_and(|e| e.job == blocked)));
    let episode = store.blocked_episode(machine(&h)).unwrap().unwrap();
    let thresholds = homebased::queue::schedule::NoticeThresholds {
        after_yield: Duration::from_secs(15 * 60),
        after_wait: Duration::from_secs(30 * 60),
    };
    assert!(
        store
            .produce_job_blocked(
                machine(&h),
                episode,
                thresholds,
                chrono::Utc::now() + chrono::Duration::minutes(31)
            )
            .unwrap()
    );
    assert!(
        !store
            .produce_job_blocked(
                machine(&h),
                episode,
                thresholds,
                chrono::Utc::now() + chrono::Duration::minutes(32)
            )
            .unwrap()
    );
    let run = store.job_runs(active).unwrap()[0].task;
    assert!(store.produce_job_check_due(run).unwrap());
    assert!(!store.produce_job_check_due(run).unwrap());
    let blocked_events = wait_callbacks(&h, blocked, 1);
    let active_events = wait_callbacks(&h, active, 1);
    assert_eq!(blocked_events[0]["event"], "JOB_BLOCKED");
    assert_eq!(blocked_events[0]["thread"], high_thread);
    assert_eq!(
        blocked_events[0]["blocked"]["blockers"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(active_events[0]["event"], "JOB_CHECK_DUE");
    assert_eq!(active_events[0]["task"], run.to_string());
    assert_eq!(active_events[0]["thread"], THREAD);
    assert_eq!(show(&h, active)["job"]["state"]["state"], "active");
    fs::write(h.home.join("finish"), "").unwrap();
    wait_state(&h, active, "succeeded");
    wait_state(&h, blocked, "succeeded");
    assert_eq!(wait_callbacks(&h, active, 2)[1]["event"], "JOB_SUCCEEDED");
    assert_eq!(wait_callbacks(&h, blocked, 2)[1]["event"], "JOB_SUCCEEDED");
}

#[test]
fn web_serves_only_guarded_queue_controls() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let h = Harness::with_dashboard();
    let addr = super::dashboard_addr(&h);
    let request = |path: &str, origin: &str, body: &str| {
        let mut stream = TcpStream::connect(&addr).unwrap();
        let length = body.len();
        write!(
            stream,
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nOrigin: {origin}\r\n\
             Content-Type: application/json\r\nContent-Length: {length}\r\n\
             Connection: close\r\n\r\n{body}"
        )
        .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    };
    let same = format!("http://{addr}");

    // a job runs arbitrary commands, so the TCP listener must never accept one
    for path in ["/v1/resources", "/v1/resource/jobs"] {
        let response = request(path, &same, "{}");
        assert!(response.starts_with("HTTP/1.1 405"), "{path}: {response}");
    }

    let cancel = format!("/v1/resource/jobs/{}/cancel", JobId::new());
    let body = format!("{{\"operation_id\":\"{}\"}}", uuid::Uuid::now_v7());
    assert!(request(&cancel, "http://other.example", &body).starts_with("HTTP/1.1 403"));
    assert!(request(&cancel, "null", &body).starts_with("HTTP/1.1 403"));
    let allowed = request(&cancel, &same, &body);
    assert!(!allowed.starts_with("HTTP/1.1 403"), "{allowed}");
    assert!(allowed.contains("\"error\""), "{allowed}");
    let huge = format!("{{\"operation_id\":\"{}\"}}", "a".repeat(2 * 1024 * 1024));
    assert!(request(&cancel, &same, &huge).contains("invalid_spec"));

    let resources = super::http_get(&addr, "/v1/resources");
    assert_eq!(resources.status, 200);
    let schema = cli(&h, &["resource", "schema"]);
    assert!(schema["properties"].is_object());
}

#[test]
fn job_submit_checks_worker_parent_thread_and_accepts_stdin() {
    use std::io::Write;
    use std::process::Stdio;

    let h = Harness::new();
    let parent = h.submit(&Harness::task_spec(&["/bin/sh", "-c", "exit 0"]));
    h.wait_status(&parent, "succeeded");
    let mut jobs = Jobs::new(&h);
    let job = JobId::new();
    jobs.ids.push(job);
    let mut spec = spec(&h, "low", "wait", "exit 0");
    let other = uuid::Uuid::now_v7().to_string();
    register_thread(&h.user_home, &other);
    spec["thread"] = json!(other);
    let file = h.home.join("thread-job.json");
    fs::write(&file, serde_json::to_vec(&spec).unwrap()).unwrap();
    let refused = h
        .cmd()
        .env("HOMEBASED_TASK_ID", &parent)
        .args([
            "--json",
            "resource",
            "job",
            "submit",
            "--job-id",
            &job.to_string(),
            "--spec",
        ])
        .arg(&file)
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(2));
    let error: Value = serde_json::from_slice(&refused.stderr).unwrap();
    assert_eq!(error["error"]["code"], "thread_mismatch");
    assert!(h.store().job_route(job).unwrap().is_none());
    let mut child = h
        .cmd()
        .env("HOMEBASED_TASK_ID", &parent)
        .args([
            "--json",
            "resource",
            "job",
            "submit",
            "--job-id",
            &job.to_string(),
            "--spec",
            "-",
            "--allow-other-thread",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&spec).unwrap())
        .unwrap();
    let accepted = child.wait_with_output().unwrap();
    assert!(
        accepted.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    assert_eq!(wait_callbacks(&h, job, 1)[0]["thread"], other);
}

#[test]
fn cli_transport_retry_keeps_generated_operation_identity() {
    use std::io::{Read, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::thread;

    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    let active = jobs.submit(&spec(&h, "medium", "wait", WAIT_FOR_FINISH));
    assert!(wait_until(Duration::from_secs(10), || h
        .home
        .join("ready")
        .exists()));
    let queued = jobs.submit(&spec(&h, "low", "wait", "exit 0"));
    let proxy_home = h.dir.path().join("proxy");
    fs::create_dir_all(&proxy_home).unwrap();
    let listener = UnixListener::bind(proxy_home.join("homebased.sock")).unwrap();
    listener.set_nonblocking(true).unwrap();
    let real_socket = h.home.join("homebased.sock");
    let worker = thread::spawn(move || {
        let mut bodies = Vec::new();
        for attempt in 0..2 {
            let mut incoming = None;
            assert!(
                wait_until(Duration::from_secs(10), || {
                    incoming = listener.accept().ok().map(|(stream, _)| stream);
                    incoming.is_some()
                }),
                "CLI did not retry"
            );
            let mut incoming = incoming.unwrap();
            incoming
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut header = Vec::new();
            let mut byte = [0];
            while !header.ends_with(b"\r\n\r\n") {
                incoming.read_exact(&mut byte).unwrap();
                header.push(byte[0]);
            }
            let header = String::from_utf8(header).unwrap();
            let length: usize = header
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| value.trim().parse().unwrap())
                })
                .unwrap();
            let mut body = vec![0; length];
            incoming.read_exact(&mut body).unwrap();
            bodies.push(serde_json::from_slice::<Value>(&body).unwrap());
            let mut authority = UnixStream::connect(&real_socket).unwrap();
            authority
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            authority.write_all(header.trim_end().as_bytes()).unwrap();
            authority
                .write_all(b"\r\nConnection: close\r\n\r\n")
                .unwrap();
            authority.write_all(&body).unwrap();
            let mut response = Vec::new();
            authority.read_to_end(&mut response).unwrap();
            if attempt == 1 {
                incoming.write_all(&response).unwrap();
            }
        }
        bodies
    });
    let output = h
        .cmd()
        .arg("--home")
        .arg(&proxy_home)
        .args([
            "--json",
            "resource",
            "job",
            "move",
            &queued.to_string(),
            "--front",
        ])
        .output()
        .unwrap();
    let bodies = worker.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(bodies[0], bodies[1]);
    assert!(
        bodies[0]["operation_id"]
            .as_str()
            .unwrap()
            .parse::<uuid::Uuid>()
            .is_ok()
    );
    cli(&h, &["resource", "job", "cancel", &active.to_string()]);
    wait_state(&h, queued, "succeeded");
}

#[test]
fn cli_failure_reports_the_job_and_run_without_task_callbacks() {
    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    let mut failing = spec(&h, "medium", "wait", "exit 0");
    failing.as_object_mut().unwrap().remove("workload");
    failing["steps"] = json!([
        { "type": "task", "command": ["/bin/sh", "-c", "echo failed-output; exit 9"] },
        { "type": "task", "command": ["/bin/sh", "-c", "echo should-not-run > later"] }
    ]);
    let job = jobs.submit(&failing);
    let detail = wait_state(&h, job, "failed");
    let received = wait_callbacks(&h, job, 1);
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["event"], "JOB_FAILED");
    assert_eq!(received[0]["task"], detail["runs"][0]["task"]);
    assert_eq!(detail["runs"].as_array().unwrap().len(), 1);
    assert!(!h.home.join("later").exists());
    let task = detail["runs"][0]["task"].as_str().unwrap();
    let log = cli(&h, &["task", "log", task]);
    assert!(log["log"].as_str().unwrap().contains("failed-output"));
    assert!(
        !h.queue_messages()
            .iter()
            .map(|line| event_json(line))
            .any(|event| event["task"] == task
                && event["event"].as_str().unwrap().starts_with("TASK_"))
    );
}

#[test]
fn task_cancel_during_yield_commits_user_cancel_before_signalling() {
    use homebased::queue::StopCause;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    struct ResumeWorker(Pid);
    impl Drop for ResumeWorker {
        fn drop(&mut self) {
            let _ = kill(self.0, Signal::SIGCONT);
        }
    }
    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    let job = jobs.submit(&spec(&h, "low", "yield",
        "echo ready > ready; n=0; while [ ! -f \"$HOMEBASED_YIELD_FILE\" ] && [ $n -lt 600 ]; do sleep 0.05; n=$((n+1)); done; echo yield > yielded; while [ ! -f finish ] && [ $n -lt 1200 ]; do sleep 0.05; n=$((n+1)); done; exit 75"));
    assert!(wait_until(Duration::from_secs(10), || h
        .home
        .join("ready")
        .exists()));
    let store = Store::open(&h.home.join("homebased.sqlite")).unwrap();
    let checkpoint = store
        .resources_on(machine(&h))
        .unwrap()
        .into_iter()
        .find_map(|record| record.run.filter(|run| run.job == job))
        .unwrap();
    store
        .commit_stop(
            checkpoint.resource,
            checkpoint.task,
            StopCause::Yield,
            chrono::Utc::now(),
        )
        .unwrap();
    assert!(wait_until(Duration::from_secs(10), || h
        .home
        .join("yielded")
        .exists()));
    let row = store.require_task(checkpoint.task).unwrap();
    let worker = ResumeWorker(Pid::from_raw(row.pid().unwrap()));
    kill(worker.0, Signal::SIGSTOP).unwrap();
    cli(&h, &["task", "cancel", &checkpoint.task.to_string()]);
    assert_eq!(
        store.job_runs(job).unwrap()[0].stop_cause,
        Some(StopCause::UserCancel)
    );
    fs::write(h.home.join("finish"), "").unwrap();
    drop(worker);
    wait_state(&h, job, "cancelled");
    assert_eq!(wait_callbacks(&h, job, 1)[0]["event"], "JOB_CANCELLED");
}
