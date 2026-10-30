//! `task wait` streams callback events to a Monitor in place of normal delivery

use super::resource_jobs::{Jobs, callbacks, spec, wait_callbacks};
use super::{Harness, THREAD, event_json, wait_until};
use homebased::client::{Client, LineStream};
use homebased::queue::JobId;
use homebased::store::Store;
use homebased::task_wait::WAIT_ROUTE;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader};
use std::process::{Child, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::Duration;

const OTHER_THREAD: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e432";

/// A running `homebased task wait` and the stdout lines it has printed
struct Waiter {
    child: Child,
    lines: Receiver<String>,
}

impl Waiter {
    fn next_line(&self) -> Value {
        let line = self
            .lines
            .recv_timeout(Duration::from_secs(20))
            .expect("the waiter printed no line");
        event_json(&line)
    }

    /// Exit code once the waiter ends, with the lines it printed before
    fn finish(mut self) -> (i32, Vec<Value>) {
        let mut code = None;
        assert!(
            wait_until(Duration::from_secs(20), || {
                code = self.child.try_wait().unwrap().map(|s| s.code().unwrap());
                code.is_some()
            }),
            "the waiter did not exit"
        );
        let rest = self
            .lines
            .try_iter()
            .map(|line| event_json(&line))
            .collect();
        (code.unwrap(), rest)
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A wait stream that reads frames but never confirms a print
///
/// Dropping it plays a client that was killed after receiving an event
struct SilentWaiter {
    runtime: tokio::runtime::Runtime,
    stream: LineStream,
}

impl SilentWaiter {
    fn event(&mut self) -> Value {
        loop {
            let line = self
                .runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(20), self.stream.next_line()).await
                })
                .expect("no frame arrived")
                .unwrap()
                .expect("the stream ended");
            let frame: Value = serde_json::from_slice(&line).unwrap();
            if frame["type"] == "event" {
                return frame;
            }
        }
    }
}

impl Harness {
    fn open_silent_wait(&self, task: &str) -> SilentWaiter {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = Client::new(self.home.join("homebased.sock"));
        let body = json!({ "thread": THREAD, "tasks": [task] });
        let stream = runtime
            .block_on(client.post_lines(WAIT_ROUTE, &body))
            .unwrap();
        SilentWaiter { runtime, stream }
    }

    fn spawn_wait(&self, thread: &str, args: &[&str]) -> Waiter {
        let mut child = self
            .cmd()
            .env("CODEX_THREAD_ID", thread)
            .args(["task", "wait"])
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, lines) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        // the daemon registers a waiter only while the client is connected
        std::thread::sleep(Duration::from_millis(800));
        Waiter { child, lines }
    }

    /// A target that runs until its gate opens and exits 0
    fn gated(&self, gate: &str) -> String {
        self.submit(&self.gated_target(gate, 0))
    }

    /// A queue job that runs until its gate opens and exits 0
    fn gated_job(&self, jobs: &mut Jobs<'_>, gate: &str) -> JobId {
        let gate = self.record.join(gate);
        let script = format!(
            "n=0; while [ ! -f '{}' ] && [ $n -lt 300 ]; do sleep 0.1; n=$((n+1)); done; exit 0",
            gate.display()
        );
        jobs.submit(&spec(self, "medium", "wait", &script))
    }

    /// Last job event settled in the origin inbox
    fn job_settled(&self, job: JobId) -> u64 {
        Store::open(&self.home.join(homebased::home::DB_NAME))
            .unwrap()
            .job_route_cursors(job)
            .unwrap()
            .map_or(0, |cursors| cursors.settled)
    }

    /// Callbacks that normal delivery sent for `id`
    fn normally_delivered(&self, id: &str) -> Vec<String> {
        self.queue_messages()
            .into_iter()
            .filter(|line| event_json(line)["task"] == id)
            .collect()
    }
}

#[test]
fn waiter_prints_the_events_of_its_task_and_normal_delivery_stays_quiet() {
    let h = Harness::new();
    let task = h.gated("print");
    let waiter = h.spawn_wait(THREAD, &[&task]);

    h.release_gate("print");
    let (code, lines) = waiter.finish();

    assert_eq!(code, 0);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["event"], "TASK_SUCCEEDED");
    assert_eq!(lines[0]["task"], task);
    assert!(wait_until(
        Duration::from_secs(10),
        || h.show(&task)["callback"] == "sent"
    ));
    assert_eq!(h.normally_delivered(&task), Vec::<String>::new());
}

#[test]
fn waiter_follows_a_waiting_chain_to_the_end_of_the_work() {
    let h = Harness::new();
    let target = h.gated("chain-target");
    h.set_control("waiting", &format!("{target}\nCheck the target.\n"));
    h.set_control("report", "succeeded\nDone.");
    h.set_control("sleep", "1");
    let worker = h.submit(&Harness::spec("codex", "train the model"));
    let waiter = h.spawn_wait(THREAD, &[&worker]);

    let parked = waiter.next_line();
    assert_eq!(parked["event"], "TASK_WAITING");
    assert_eq!(parked["task"], worker);
    let continuation = parked["continuation"].as_str().unwrap().to_string();

    h.release_gate("chain-target");
    let (code, rest) = waiter.finish();

    assert_eq!(code, 0);
    assert_eq!(rest.len(), 1, "{rest:?}");
    assert_eq!(rest[0]["event"], "TASK_SUCCEEDED");
    assert_eq!(rest[0]["task"], continuation);
    assert_eq!(h.normally_delivered(&worker), Vec::<String>::new());
    assert_eq!(h.normally_delivered(&continuation), Vec::<String>::new());
}

#[test]
fn waiter_on_a_task_that_ended_and_was_delivered_exits_without_output() {
    let h = Harness::new();
    let task = h.submit(&Harness::task_spec(&["true"]));
    h.wait_for_event(&task, "TASK_SUCCEEDED");
    assert!(wait_until(
        Duration::from_secs(10),
        || h.show(&task)["callback"] == "sent"
    ));

    let (code, lines) = h.spawn_wait(THREAD, &[&task]).finish();

    assert_eq!(code, 0);
    assert!(lines.is_empty(), "{lines:?}");
}

#[test]
fn waiter_is_refused_for_a_task_of_another_thread_or_an_unknown_task() {
    let h = Harness::new();
    let task = h.gated("refused");
    let unknown = homebased::domain::TaskId::new().to_string();

    for (thread, id, code) in [
        (OTHER_THREAD, &task, "wait_unavailable"),
        (THREAD, &unknown, "task_not_found"),
    ] {
        let out = h
            .cmd()
            .env("CODEX_THREAD_ID", thread)
            .args(["--json", "task", "wait", id])
            .output()
            .unwrap();
        assert!(out.stdout.is_empty());
        let error: Value = serde_json::from_slice(&out.stderr).unwrap();
        assert_eq!(error["error"]["code"], code, "{error}");
        assert_ne!(out.status.code(), Some(0));
    }
    h.release_gate("refused");
    h.wait_for_event(&task, "TASK_SUCCEEDED");
}

#[test]
fn a_waiter_that_disconnected_leaves_events_to_normal_delivery() {
    let h = Harness::new();
    let task = h.gated("gone");
    let mut waiter = h.spawn_wait(THREAD, &[&task]);
    waiter.child.kill().unwrap();
    waiter.child.wait().unwrap();

    h.release_gate("gone");

    h.wait_for_event(&task, "TASK_SUCCEEDED");
    assert_eq!(h.normally_delivered(&task).len(), 1);
}

#[test]
fn daemon_shutdown_ends_the_stream_promptly_and_the_waiter_reconnects() {
    let mut h = Harness::new();
    let task = h.gated("restart");
    let waiter = h.spawn_wait(THREAD, &[&task]);

    let mut daemon = h.daemon.take().unwrap();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(daemon.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || daemon
            .try_wait()
            .unwrap()
            .is_some()),
        "the daemon did not stop with a waiter connected"
    );
    h.start_daemon();

    h.release_gate("restart");
    let (code, lines) = waiter.finish();

    assert_eq!(code, 0);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["task"], task);
    assert_eq!(lines[0]["event"], "TASK_SUCCEEDED");
}

#[test]
fn an_event_the_client_never_confirmed_goes_through_normal_delivery() {
    let h = Harness::new();
    let task = h.gated("silent-drop");
    let mut silent = h.open_silent_wait(&task);

    h.release_gate("silent-drop");
    let frame = silent.event();
    assert_eq!(frame["task"], task);
    // the client dies before it prints the line
    drop(silent);

    h.wait_for_event(&task, "TASK_SUCCEEDED");
    assert!(
        wait_until(Duration::from_secs(20), || h
            .normally_delivered(&task)
            .len()
            == 1),
        "an unconfirmed event was lost"
    );
}

#[test]
fn an_unconfirmed_event_goes_to_the_waiter_that_connected_next() {
    let h = Harness::new();
    let task = h.gated("silent-next");
    let mut silent = h.open_silent_wait(&task);

    h.release_gate("silent-next");
    silent.event();
    // the first stream stays connected but never confirms; a re-armed monitor
    // must not mistake the event for one that was consumed
    let waiter = h.spawn_wait(THREAD, &[&task]);
    let (code, lines) = waiter.finish();

    assert_eq!(code, 0);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["event"], "TASK_SUCCEEDED");
    assert_eq!(lines[0]["task"], task);
    assert_eq!(h.normally_delivered(&task), Vec::<String>::new());
    drop(silent);
}

#[test]
fn task_run_from_a_session_reports_to_it_and_its_waiter_gets_the_event() {
    let h = Harness::new();
    let gate = h.record.join("run");
    let script = format!("while [ ! -f '{}' ]; do sleep 0.1; done", gate.display());
    let out = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args(["--quiet", "task", "run", "--", "/bin/sh", "-c", &script])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let task = String::from_utf8(out.stdout).unwrap().trim().to_string();

    let shown = h.show(&task);
    assert_eq!(shown["thread"], THREAD, "{shown}");
    let name = shown["name"].as_str().unwrap();
    assert!(name.starts_with("/bin/sh -c while [ ! -f"), "{name}");
    assert!(name.chars().count() <= 60, "{name}");

    let waiter = h.spawn_wait(THREAD, &[&task]);
    h.release_gate("run");
    let (code, lines) = waiter.finish();

    assert_eq!(code, 0);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["event"], "TASK_SUCCEEDED");
}

#[test]
fn waiter_names_a_task_submitted_without_a_thread() {
    let h = Harness::new();
    let mut spec = h.gated_target("detached", 0);
    spec.as_object_mut().unwrap().remove("thread");
    let task = h.submit(&spec);

    let out = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args(["--json", "task", "wait", &task])
        .output()
        .unwrap();

    let error: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(error["error"]["code"], "wait_unavailable", "{error}");
    assert_eq!(error["error"]["input"]["reason"], "detached", "{error}");
    h.release_gate("detached");
}

#[test]
fn one_waiter_prints_task_and_job_events_and_settles_them() {
    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    let task = h.gated("mixed-task");
    let job = h.gated_job(&mut jobs, "mixed-job");
    let waiter = h.spawn_wait(THREAD, &[&task, "--job", &job.to_string()]);

    h.release_gate("mixed-job");
    let line = waiter.next_line();
    assert_eq!(line["event"], "JOB_SUCCEEDED", "{line}");
    assert_eq!(line["job"], job.to_string());
    assert_eq!(line["thread"], THREAD);
    assert!(
        wait_until(Duration::from_secs(10), || h.job_settled(job) == 1),
        "the printed job event was not settled"
    );

    h.release_gate("mixed-task");
    let (code, rest) = waiter.finish();

    assert_eq!(code, 0);
    assert_eq!(rest.len(), 1, "{rest:?}");
    assert_eq!(rest[0]["event"], "TASK_SUCCEEDED");
    assert_eq!(rest[0]["task"], task);
    // the job sender rescans its inbox several times a second, so a repeat
    // would show up well within this pause
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(callbacks(&h, job), Vec::<Value>::new());
    assert_eq!(h.normally_delivered(&task), Vec::<String>::new());
}

#[test]
fn job_waiter_on_a_delivered_job_exits_without_output() {
    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    let job = jobs.submit(&spec(&h, "medium", "wait", "exit 0"));
    assert_eq!(wait_callbacks(&h, job, 1)[0]["event"], "JOB_SUCCEEDED");

    let (code, lines) = h.spawn_wait(THREAD, &["--job", &job.to_string()]).finish();

    assert_eq!(code, 0);
    assert!(lines.is_empty(), "{lines:?}");
}

#[test]
fn waiter_names_a_job_given_as_a_task_and_refuses_an_unknown_job() {
    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    let job = h.gated_job(&mut jobs, "job-as-task").to_string();
    let unknown = JobId::new().to_string();

    for (args, code) in [
        (vec![job.as_str()], "wait_target_is_job"),
        (vec!["--job", unknown.as_str()], "job_not_found"),
    ] {
        let out = h
            .cmd()
            .env("CODEX_THREAD_ID", THREAD)
            .args(["--json", "task", "wait"])
            .args(&args)
            .output()
            .unwrap();
        assert!(out.stdout.is_empty());
        let error: Value = serde_json::from_slice(&out.stderr).unwrap();
        assert_eq!(error["error"]["code"], code, "{error}");
        assert_ne!(out.status.code(), Some(0));
        if code == "wait_target_is_job" {
            let message = error["error"]["message"].as_str().unwrap();
            assert!(message.contains(&format!("--job {job}")), "{message}");
        }
    }
    h.release_gate("job-as-task");
}
