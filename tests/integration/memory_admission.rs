//! Host memory admission, entry placement of held jobs, and GPU hiding for tasks

use super::resource_jobs::{Jobs, spec};
use super::{Harness, THREAD, wait_until};
use homebased::queue::JobId;
use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

const GIB: u64 = 1 << 30;

/// A cgroup directory the daemon's memory gauge reads instead of its own
pub(super) struct FakeCgroup(PathBuf);

impl FakeCgroup {
    pub(super) fn new(h: &Harness) -> Self {
        let dir = h.dir.path().join("cgroup/homebased.service");
        fs::create_dir_all(&dir).unwrap();
        let cgroup = Self(dir);
        cgroup.set_used(0);
        cgroup
    }

    /// Report `bytes` in use on top of reclaimable page cache, a quarter of
    /// the in-use bytes being tmpfs that reclaim cannot free
    fn set_used(&self, bytes: u64) {
        let (active, inactive) = (3 * GIB, GIB / 4);
        let shmem = bytes / 4;
        let file = active + inactive + shmem;
        let current = bytes - shmem + file;
        fs::write(self.0.join("memory.current"), current.to_string()).unwrap();
        fs::write(
            self.0.join("memory.stat"),
            format!(
                "anon {}\nfile {file}\nshmem {shmem}\nactive_file {active}\ninactive_file {inactive}\n",
                bytes - shmem
            ),
        )
        .unwrap();
    }

    /// Add a transient `run-*.scope` beside the daemon, as a shell command started outside it
    fn add_scope(&self, bytes: u64) -> PathBuf {
        let scope = self.0.parent().unwrap().join("run-p1-i2.scope");
        fs::create_dir_all(&scope).unwrap();
        fs::write(scope.join("memory.current"), bytes.to_string()).unwrap();
        fs::write(scope.join("memory.stat"), "inactive_file 0\n").unwrap();
        scope
    }
}

/// Restart the daemon with a 10 GiB bar over `cgroup`
pub(super) fn with_memory_gate(h: &mut Harness, cgroup: &FakeCgroup, env: &[(&str, &str)]) {
    with_memory_config(h, cgroup, "", env);
}

/// Restart the daemon with a 10 GiB bar over `cgroup` and `extra` lines in `[memory]`
fn with_memory_config(h: &mut Harness, cgroup: &FakeCgroup, extra: &str, env: &[(&str, &str)]) {
    let config = h.dir.path().join("config.toml");
    fs::write(
        &config,
        format!(
            "[memory]\nbar_gib = 10\ncgroup = \"{}\"\n{extra}",
            cgroup.0.display()
        ),
    )
    .unwrap();
    let mut env = env.to_vec();
    env.push(("HOMEBASED_CONFIG", config.to_str().unwrap()));
    h.restart_daemon_with_env(&env);
}

fn cli(h: &Harness, args: &[&str]) -> Value {
    let output = h.cmd().arg("--json").args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn task(h: &Harness, id: &str) -> Value {
    cli(h, &["task", "list"])["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["id"] == id)
        .cloned()
        .unwrap_or(Value::Null)
}

fn job_state(h: &Harness, job: JobId) -> Value {
    cli(h, &["resource", "job", "show", &job.to_string()])["job"]["state"]["state"].clone()
}

fn job_memory_wait(h: &Harness, job: JobId) -> Value {
    cli(h, &["resource", "jobs"])["memory_waits"][job.to_string()].clone()
}

/// A task spec holding `peak` GiB that runs until `gate` exists
fn peak_task(h: &Harness, gate: &str, peak: f64) -> Value {
    let mut spec = h.gated_target(gate, 0);
    spec["mem_peak"] = json!(peak);
    spec
}

/// Whether the origin inbox settled event `seq` of task `id`, read from the store
fn delivery_settled(h: &Harness, id: &str, seq: i64) -> bool {
    let db = rusqlite::Connection::open(h.home.join(homebased::home::DB_NAME)).unwrap();
    let settled_at: Option<String> = db
        .query_row(
            "SELECT settled_at FROM origin_inbox WHERE task_id=?1 AND seq=?2",
            rusqlite::params![id, seq],
            |row| row.get(0),
        )
        .unwrap();
    settled_at.is_some()
}

fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    assert!(wait_until(Duration::from_secs(20), &mut check), "{what}");
}

#[test]
fn memory_admission_serves_in_order_and_holds_room_for_started_and_waiting_work() {
    let mut h = Harness::new();
    let cgroup = FakeCgroup::new(&h);
    // 2 GiB of the service plus 1 GiB in a scope beside it, so 7 GiB is free
    cgroup.set_used(2 * GIB);
    let scope = cgroup.add_scope(GIB);
    with_memory_gate(&mut h, &cgroup, &[]);
    let mut jobs = Jobs::new(&h);

    // the GPU job heads the order, needs 9 GiB, and waits on its free resource
    let big = JobId::new();
    let script = "while [ ! -f finish ]; do sleep 0.1; done";
    jobs.submit_with(
        big,
        &spec(&h, "medium", "restart", script),
        &["--mem-peak", "9"],
    );
    eventually("the big job never reported its memory wait", || {
        job_memory_wait(&h, big)["reason"] == "room"
    });
    let wait = job_memory_wait(&h, big);
    assert_eq!(wait["bar"], 10 * GIB, "{wait}");
    assert_eq!(wait["free"], 7 * GIB, "{wait}");
    assert_eq!(job_state(&h, big), "queued");

    // 1 GiB fits the 7 GiB free, but not behind the 9 GiB the job reserves
    let small = h.submit(&peak_task(&h, "small", 1.0));
    eventually("the small task never reported its memory wait", || {
        task(&h, &small)["memory_wait"]["reason"] == "room"
    });
    let listed = task(&h, &small);
    assert_eq!(listed["status"], "held", "{listed}");
    assert_eq!(listed["mem_peak"], 1.0, "{listed}");
    assert_eq!(listed["memory_wait"]["free"], 0, "{listed}");
    let human = h.cmd().args(["task", "list"]).output().unwrap();
    let human = String::from_utf8_lossy(&human.stdout);
    assert!(
        human.contains("waiting for memory: peak 1.0G, 0.0G free under the 10.0G bar"),
        "{human}"
    );

    // work without a peak is not gated
    let plain = h.submit(&Harness::task_spec(&["true"]));
    h.wait_status(&plain, "succeeded");
    assert_eq!(task(&h, &small)["status"], "held");

    // once memory frees, both start, in order, each within the room the other left
    cgroup.set_used(0);
    fs::remove_dir_all(&scope).unwrap();
    eventually("the big job never started", || {
        job_state(&h, big) == "active"
    });
    h.wait_status(&small, "running");
    assert_eq!(job_memory_wait(&h, big), Value::Null);

    // started work still claims the peak it has not reached yet
    let late = h.submit(&peak_task(&h, "late", 2.0));
    eventually("the late task never reported its memory wait", || {
        task(&h, &late)["memory_wait"]["reason"] == "room"
    });
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(task(&h, &late)["status"], "held");

    // the job ending returns its claim
    fs::write(h.home.join("finish"), "").unwrap();
    h.release_gate("late");
    h.wait_status(&late, "succeeded");
    h.release_gate("small");
    h.wait_status(&small, "succeeded");
}

#[test]
fn a_job_next_for_a_busy_gpu_holds_room_from_tasks_only_when_asked() {
    let mut h = Harness::new();
    let cgroup = FakeCgroup::new(&h);
    with_memory_gate(&mut h, &cgroup, &[]);
    let mut jobs = Jobs::new(&h);
    let waits_for = |file: &str| format!("while [ ! -f {file} ]; do sleep 0.1; done");

    // a 2 GiB predict holds gpu0
    let predict = JobId::new();
    jobs.submit_with(
        predict,
        &spec(&h, "medium", "restart", &waits_for("finish-predict")),
        &["--mem-peak", "2"],
    );
    eventually("the predict never started", || {
        job_state(&h, predict) == "active"
    });

    // an 8 GiB trace queued behind it without --hold-room keeps no room
    let plain = JobId::new();
    jobs.submit_with(
        plain,
        &spec(&h, "medium", "restart", "true"),
        &["--mem-peak", "8"],
    );
    let early = h.submit(&peak_task(&h, "early", 3.0));
    h.wait_status(&early, "running");
    cli(&h, &["resource", "job", "cancel", &plain.to_string()]);
    h.release_gate("early");
    h.wait_status(&early, "succeeded");

    // with it, the trace holds all but the predict's 2 GiB, so a 3 GiB task
    // that fits the 8 GiB free waits for the trace instead
    let trace = JobId::new();
    jobs.submit_with(
        trace,
        &spec(&h, "medium", "restart", &waits_for("finish-trace")),
        &["--mem-peak", "8", "--hold-room"],
    );
    eventually("the trace never held its room", || {
        cli(&h, &["resource", "jobs"])["memory_holds"][trace.to_string()]["held"] == 6 * GIB
    });
    let human = h.cmd().args(["resource", "jobs"]).output().unwrap();
    let human = String::from_utf8_lossy(&human.stdout);
    assert!(
        human.contains("holding 6.0G of its 8.0G peak from tasks until its resource is free"),
        "{human}"
    );
    let late = h.submit(&peak_task(&h, "late", 3.0));
    eventually("the late task never reported its memory wait", || {
        task(&h, &late)["memory_wait"]["reason"] == "room"
    });
    assert_eq!(task(&h, &late)["status"], "held");

    // the predict ends and the trace takes its room; the trace's 8 GiB claim
    // still leaves the task waiting until it ends too
    fs::write(h.home.join("finish-predict"), "").unwrap();
    eventually("the trace never started", || {
        job_state(&h, trace) == "active"
    });
    assert_eq!(cli(&h, &["resource", "jobs"])["memory_holds"], Value::Null);
    assert_eq!(task(&h, &late)["status"], "held");
    fs::write(h.home.join("finish-trace"), "").unwrap();
    h.wait_status(&late, "running");
    h.release_gate("late");
    h.wait_status(&late, "succeeded");
}

#[test]
fn hold_room_without_a_peak_is_refused() {
    let h = Harness::new();
    let path = h.home.join("hold-room.json");
    fs::write(
        &path,
        serde_json::to_vec(&spec(&h, "medium", "restart", "true")).unwrap(),
    )
    .unwrap();
    let output = h
        .cmd()
        .args([
            "--json",
            "resource",
            "job",
            "submit",
            "--hold-room",
            "--job-id",
        ])
        .arg(JobId::new().to_string())
        .arg("--spec")
        .arg(&path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("/hold_room"), "{stderr}");
}

#[test]
fn a_peak_over_the_bar_is_refused_at_submit() {
    let mut h = Harness::new();
    let cgroup = FakeCgroup::new(&h);
    with_memory_gate(&mut h, &cgroup, &[]);

    let mut job = spec(&h, "medium", "restart", "exit 0");
    job["mem_peak"] = json!(11);
    let path = h.home.join("too-big.json");
    fs::write(&path, serde_json::to_vec(&job).unwrap()).unwrap();
    let refused = h
        .cmd()
        .args([
            "--json",
            "resource",
            "job",
            "submit",
            "--job-id",
            &JobId::new().to_string(),
            "--spec",
        ])
        .arg(&path)
        .output()
        .unwrap();
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("memory bar"), "{stderr}");

    let task = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .args(["--json", "task", "run", "--mem-peak", "11", "--", "true"])
        .output()
        .unwrap();
    assert!(!task.status.success());
    let stderr = String::from_utf8_lossy(&task.stderr);
    assert!(stderr.contains("memory bar"), "{stderr}");
}

#[test]
fn a_task_retry_with_another_memory_peak_is_a_conflict() {
    let mut h = Harness::new();
    let cgroup = FakeCgroup::new(&h);
    with_memory_gate(&mut h, &cgroup, &[]);
    let request = uuid::Uuid::now_v7().to_string();
    let run = |peak: &str| {
        h.cmd()
            .env("CODEX_THREAD_ID", THREAD)
            .args(["--json", "task", "run", "--request-id", &request])
            .args(["--mem-peak", peak, "--", "true"])
            .output()
            .unwrap()
    };

    let first = run("2");
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert!(first["id"].is_string(), "{first}");

    let changed = run("3");
    assert!(!changed.status.success());
    let error: Value = serde_json::from_slice(&changed.stderr).unwrap();
    assert_eq!(error["error"]["code"], "submission_conflict", "{error}");

    let repeated = run("2");
    assert!(repeated.status.success());
    let repeated: Value = serde_json::from_slice(&repeated.stdout).unwrap();
    assert_eq!(repeated["id"], first["id"], "{repeated}");
}

/// Ids of queued jobs, in serving order
fn queued_order(h: &Harness) -> Vec<String> {
    cli(h, &["resource", "jobs"])["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|job| job["state"]["state"] == "queued")
        .map(|job| job["id"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn held_jobs_take_their_entry_placement_when_released() {
    let h = Harness::new();
    let mut jobs = Jobs::new(&h);
    jobs.submit(&spec(
        &h,
        "medium",
        "restart",
        "echo ready > ready; n=0; while [ ! -f finish ] && [ $n -lt 900 ]; do sleep 0.1; n=$((n+1)); done",
    ));
    eventually("the blocker never started", || {
        h.home.join("ready").exists()
    });
    let queued = |jobs: &mut Jobs| jobs.submit(&spec(&h, "medium", "restart", "exit 0"));
    let b = queued(&mut jobs);
    let c = queued(&mut jobs);
    // an anchor that ends before the held job is released
    let ended = queued(&mut jobs);
    cli(&h, &["resource", "job", "cancel", &ended.to_string()]);
    let missing = JobId::new();

    let mut held = |gate: &str, flags: &[&str]| {
        let build = h.submit(&h.gated_target(gate, 0));
        let job = JobId::new();
        let mut args = vec!["--after", build.as_str()];
        args.extend_from_slice(flags);
        let (value, _) = jobs.submit_with(job, &spec(&h, "medium", "restart", "exit 0"), &args);
        assert_eq!(value["held"]["state"], "held", "{value}");
        job
    };
    let front = held("front", &["--place-front"]);
    let before_c = held("before", &["--place-before", &c.to_string()]);
    let after_b = held("after", &["--place-after", &b.to_string()]);
    let after_ended = held("ended", &["--place-after", &ended.to_string()]);
    let after_missing = held("missing", &["--place-before", &missing.to_string()]);

    let release = |gate: &str, job: JobId| {
        h.release_gate(gate);
        eventually("the held job never entered the queue", || {
            h.store().job(job).unwrap().is_some()
        });
        queued_order(&h)
    };
    let ids = |jobs: &[JobId]| jobs.iter().map(ToString::to_string).collect::<Vec<_>>();
    assert_eq!(release("front", front), ids(&[front, b, c]));
    assert_eq!(release("before", before_c), ids(&[front, b, before_c, c]));
    assert_eq!(
        release("after", after_b),
        ids(&[front, b, after_b, before_c, c])
    );
    // an ended anchor left its slot at the front of its level
    assert_eq!(
        release("ended", after_ended),
        ids(&[after_ended, front, b, after_b, before_c, c])
    );
    // an anchor this queue never held leaves the job where a plain submit goes
    assert_eq!(
        release("missing", after_missing),
        ids(&[after_ended, front, b, after_b, before_c, c, after_missing])
    );

    // a placement applies at submit too, and the result says where the job went
    let now = JobId::new();
    let (value, _) = jobs.submit_with(
        now,
        &spec(&h, "medium", "restart", "exit 0"),
        &["--place-after", &b.to_string()],
    );
    assert_eq!(value["entered"]["outcome"], "next", "{value}");
    assert_eq!(queued_order(&h)[3], now.to_string());
    fs::write(h.home.join("finish"), "").unwrap();
}

#[test]
fn tasks_run_with_gpus_hidden_unless_they_ask_to_see_them() {
    let mut h = Harness::new();
    h.restart_daemon_with_env(&[("CUDA_VISIBLE_DEVICES", "3")]);
    let seen = |name: &str, flags: &[&str]| {
        let path = h.record.join(name);
        let script = format!(
            "printf '%s' \"${{CUDA_VISIBLE_DEVICES-unset}}\" > '{}'",
            path.display()
        );
        let out = h
            .cmd()
            .env("CODEX_THREAD_ID", THREAD)
            .arg("--quiet")
            .args(["task", "run"])
            .args(flags)
            .args(["--", "/bin/sh", "-c", &script])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let id = String::from_utf8(out.stdout).unwrap().trim().to_owned();
        h.wait_status(&id, "succeeded");
        fs::read_to_string(path).unwrap()
    };
    assert_eq!(seen("hidden", &[]), "");
    // visible removes the variable, as every task did before, so CUDA sees every GPU
    assert_eq!(seen("visible", &["--gpu-visible"]), "unset");
}

#[test]
fn held_tasks_are_admitted_by_priority_then_submit_order() {
    let mut h = Harness::new();
    let cgroup = FakeCgroup::new(&h);
    cgroup.set_used(10 * GIB);
    with_memory_gate(&mut h, &cgroup, &[]);
    let with_priority = |gate: &str, priority: &str| {
        let mut spec = peak_task(&h, gate, 1.0);
        spec["priority"] = json!(priority);
        spec
    };

    // nothing fits yet; `last` is submitted last and raised once it waits
    let first = h.submit(&peak_task(&h, "first", 1.0));
    let low = h.submit(&with_priority("low", "low"));
    let last = h.submit(&peak_task(&h, "last", 1.0));
    eventually("the last task never reported its memory wait", || {
        task(&h, &last)["memory_wait"]["reason"] == "room"
    });
    assert_eq!(task(&h, &low)["priority"], "low");
    let raised = cli(&h, &["task", "priority", &last, "high"]);
    assert_eq!(raised["priority"], "high", "{raised}");
    assert_eq!(task(&h, &last)["priority"], "high");

    // room for one task at a time: high first, then medium, then low
    cgroup.set_used(9 * GIB);
    h.wait_status(&last, "running");
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(task(&h, &first)["status"], "held");
    assert_eq!(task(&h, &low)["status"], "held");

    // a started task no longer waits, so its level is fixed
    let refused = h
        .cmd()
        .args(["--json", "task", "priority", &last, "low"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("no longer waits for memory"), "{stderr}");

    cgroup.set_used(8 * GIB);
    h.wait_status(&first, "running");
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(task(&h, &low)["status"], "held");

    cgroup.set_used(7 * GIB);
    h.wait_status(&low, "running");
    for gate in ["first", "low", "last"] {
        h.release_gate(gate);
    }
    h.wait_status(&low, "succeeded");
}

#[test]
fn a_priority_without_a_memory_peak_is_refused() {
    let h = Harness::new();
    let mut spec = Harness::task_spec(&["true"]);
    spec["priority"] = json!("high");
    let path = h.home.join("priority.json");
    fs::write(&path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let refused = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("needs mem_peak"), "{stderr}");
}

#[test]
fn require_peak_refuses_command_tasks_without_one() {
    let mut h = Harness::new();
    let cgroup = FakeCgroup::new(&h);
    with_memory_config(&mut h, &cgroup, "require_peak = true\n", &[]);
    let run = |flags: &[&str]| {
        h.cmd()
            .env("CODEX_THREAD_ID", THREAD)
            .args(["--json", "task", "run"])
            .args(flags)
            .args(["--", "true"])
            .output()
            .unwrap()
    };

    let refused = run(&[]);
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("requires mem_peak"), "{stderr}");

    let declared = run(&["--mem-peak", "0.1"]);
    assert!(
        declared.status.success(),
        "{}",
        String::from_utf8_lossy(&declared.stderr)
    );
    let declared: Value = serde_json::from_slice(&declared.stdout).unwrap();
    h.wait_status(declared["id"].as_str().unwrap(), "succeeded");
}

#[cfg(target_os = "linux")]
#[test]
fn a_task_child_runs_with_a_higher_oom_score_than_the_daemon() {
    let h = Harness::new();
    // the daemon inherits this process's score
    let daemon: i32 = fs::read_to_string("/proc/self/oom_score_adj")
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let path = h.record.join("oom");
    let script = format!("cat /proc/self/oom_score_adj > '{}'", path.display());
    let id = h.submit(&Harness::task_spec(&["/bin/sh", "-c", &script]));
    h.wait_status(&id, "succeeded");
    let task: i32 = fs::read_to_string(path).unwrap().trim().parse().unwrap();
    assert_eq!(task, (daemon + 300).min(1000));
}

#[test]
fn a_task_retry_keeps_its_route_after_its_priority_changes() {
    let mut h = Harness::new();
    let cgroup = FakeCgroup::new(&h);
    cgroup.set_used(10 * GIB);
    with_memory_gate(&mut h, &cgroup, &[]);
    let request = uuid::Uuid::now_v7().to_string();
    let run = |flags: &[&str]| {
        let out = h
            .cmd()
            .env("CODEX_THREAD_ID", THREAD)
            .args(["--json", "task", "run", "--request-id", &request])
            .args(["--mem-peak", "1"])
            .args(flags)
            .args(["--", "true"])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"].clone()
    };

    let id = run(&["--priority", "low"]);
    let id_text = id.as_str().unwrap();
    eventually("the task never reported its memory wait", || {
        task(&h, id_text)["memory_wait"]["reason"] == "room"
    });
    cli(&h, &["task", "priority", id_text, "high"]);

    // priority is not part of the request identity, and the retry keeps the saved level
    assert_eq!(run(&[]), id);
    assert_eq!(run(&["--priority", "low"]), id);
    assert_eq!(task(&h, id_text)["priority"], "high");
}

#[test]
fn a_retry_accepted_before_require_peak_still_resolves_to_its_task() {
    let mut h = Harness::new();
    let cgroup = FakeCgroup::new(&h);
    with_memory_gate(&mut h, &cgroup, &[]);
    let gate = h.submit(&h.gated_target("gate", 0));
    let request = uuid::Uuid::now_v7().to_string();
    let run = |h: &Harness| {
        h.cmd()
            .env("CODEX_THREAD_ID", THREAD)
            .args(["--json", "task", "run", "--request-id", &request])
            .args(["--after", &gate, "--", "true"])
            .output()
            .unwrap()
    };
    let first = run(&h);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first: Value = serde_json::from_slice(&first.stdout).unwrap();

    with_memory_config(&mut h, &cgroup, "require_peak = true\n", &[]);
    let retry = run(&h);
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let retry: Value = serde_json::from_slice(&retry.stdout).unwrap();
    assert_eq!(retry["id"], first["id"], "{retry}");
    h.release_gate("gate");
}

#[test]
fn a_running_task_whose_unused_peak_alone_keeps_a_task_waiting_gets_one_memory_review() {
    let mut h = Harness::new();
    let cgroup = FakeCgroup::new(&h);
    cgroup.set_used(GIB);
    let review_in_a_second = [("HOMEBASED_TEST_MEMORY_REVIEW_SECS", "1")];
    with_memory_gate(&mut h, &cgroup, &review_in_a_second);
    let reviews = |h: &Harness, id: &str| -> Vec<Value> {
        h.queue_messages()
            .iter()
            .map(|line| super::event_json(line))
            .filter(|event| event["task"] == id && event["event"] == "TASK_MEMORY_REVIEW")
            .collect()
    };

    // 8 GiB declared and a shell's worth used, so about 1 GiB is left
    let holder = h.submit(&peak_task(&h, "holder", 8.0));
    h.wait_status(&holder, "running");
    let listed = task(&h, &holder);
    assert_eq!(
        listed["mem_peak"], 8.0,
        "a started task shows its peak: {listed}"
    );
    assert_eq!(listed["priority"], "medium", "{listed}");
    assert_eq!(h.show(&holder)["mem_peak"], 8.0);

    // 9.5 GiB would not fit even if the holder gave back all it has not
    // allocated, so the holder alone is not what keeps it out
    let huge = h.submit(&peak_task(&h, "huge", 9.5));
    eventually("the huge task never reported its memory wait", || {
        task(&h, &huge)["memory_wait"]["reason"] == "room"
    });
    std::thread::sleep(Duration::from_secs(3));
    assert!(
        reviews(&h, &holder).is_empty(),
        "{:?}",
        reviews(&h, &holder)
    );
    let cancelled = h
        .cmd()
        .args(["--json", "task", "cancel", &huge])
        .output()
        .unwrap();
    assert!(cancelled.status.success());

    // 3 GiB fits once the holder gives back what it has not allocated
    let waiter = h.submit(&peak_task(&h, "waiter", 3.0));
    h.wait_for_event(&holder, "TASK_MEMORY_REVIEW");
    let review = reviews(&h, &holder).remove(0);
    assert_eq!(review["next_action"], "review_memory_peak", "{review}");
    assert!(review["process"].is_null(), "{review}");
    let detail = &review["memory_review"];
    assert_eq!(detail["mem_peak"], 8.0, "{review}");
    assert_eq!(detail["waiting_task"], waiter.as_str(), "{review}");
    assert_eq!(detail["waiting_peak"], 3.0, "{review}");
    assert!(detail["blocked_secs"].as_u64().unwrap() >= 1, "{review}");
    let held = detail["held_bytes"].as_u64().unwrap();
    // process trees are read through procfs, so only Linux, the one platform
    // whose daemon accepts a peak, measures the shell; elsewhere the fake
    // cgroup runs the gate but every tree holds nothing
    if cfg!(target_os = "linux") {
        assert!(held > 0 && held < GIB, "{review}");
    } else {
        assert_eq!(held, 0, "{review}");
    }
    let shown = h.show(&holder);
    assert_eq!(
        shown["last_event"]["event"], "TASK_MEMORY_REVIEW",
        "{shown}"
    );
    assert_eq!(
        shown["last_event"]["memory_review"]["waiting_task"],
        waiter.as_str(),
        "{shown}"
    );
    // the review only asks; the gate still holds the peak
    assert_eq!(task(&h, &holder)["status"], "running");
    assert_eq!(task(&h, &waiter)["status"], "held");

    // asked once, also after a restart forgets the wait. The queued message
    // lands before its delivery settles, and a daemon killed in between sends
    // the same event again, as at-least-once delivery allows, so the restart
    // waits for the settle; any second line is then a second review
    let seq = review["seq"].as_i64().unwrap();
    eventually("the review's delivery never settled", || {
        delivery_settled(&h, &holder, seq)
    });
    with_memory_gate(&mut h, &cgroup, &review_in_a_second);
    std::thread::sleep(Duration::from_secs(4));
    assert_eq!(reviews(&h, &holder).len(), 1, "{:?}", reviews(&h, &holder));

    // the holder ending lets the waiter in
    h.release_gate("holder");
    h.wait_status(&holder, "succeeded");
    h.wait_status(&waiter, "running");
    h.release_gate("waiter");
    h.wait_status(&waiter, "succeeded");
}
