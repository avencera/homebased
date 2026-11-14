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
    let config = h.dir.path().join("config.toml");
    fs::write(
        &config,
        format!(
            "[memory]\nbar_gib = 10\ncgroup = \"{}\"\n",
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
    let plain = h.submit(&Harness::task_spec(&["/bin/true"]));
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
