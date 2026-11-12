//! CI jobs placed across real daemons: balancing, pins, drains, capacity
//! changes, hard kills, git checkouts, and wait metrics

use super::{Daemon, add_peer, register_thread, wait_until};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use homebased::domain::ThreadId;
use homebased::queue::delivery::JobSubmission;
use homebased::queue::{JobEventKind, JobId};
use homebased::store::Store;
use serde_json::Value;

/// Admission open whatever the test host's load and disk
const OPEN_ADMISSION: &str = "\n[ci]\nmax_load_per_core = 1000\nmin_free_disk_gib = 0\n";

/// A daemon whose callbacks append to `$HOME/callbacks`, with open admission
fn ci_daemon(name: &str) -> Daemon {
    let mut daemon = Daemon::start(name, true);
    let codex = daemon.user_home.join("codex");
    fs::write(
        &codex,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/callbacks\"\n",
    )
    .unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    daemon.codex_override = Some(codex);
    let mut config = fs::read_to_string(&daemon.config).unwrap();
    config.push_str(OPEN_ADMISSION);
    fs::write(&daemon.config, config).unwrap();
    daemon.restart();
    daemon
}

/// Two peered daemons, `ci-a` and `ci-b`
fn ci_pair() -> (Daemon, Daemon) {
    let a = ci_daemon("ci-a");
    let b = ci_daemon("ci-b");
    add_peer(&a, &b);
    add_peer(&b, &a);
    (a, b)
}

fn store(daemon: &Daemon) -> Store {
    Store::open(&daemon.home.join(homebased::home::DB_NAME)).unwrap()
}

fn cli(daemon: &Daemon, thread: ThreadId, args: &[&str]) -> Value {
    let output = daemon
        .cmd()
        .env("CODEX_THREAD_ID", thread.to_string())
        .current_dir(&daemon.user_home)
        .arg("--json")
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn thread(daemon: &Daemon) -> ThreadId {
    let thread = ThreadId(uuid::Uuid::now_v7());
    register_thread(&daemon.user_home, &thread.to_string());
    thread
}

/// Submit `script` as a CI job of `class` and return its id
fn run(daemon: &Daemon, thread: ThreadId, class: &str, extra: &[&str], script: &str) -> JobId {
    let job = JobId::new();
    let job_text = job.to_string();
    let mut args = vec!["ci", "run", "--class", class, "--job-id", &job_text];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--", "/bin/sh", "-c", script]);
    cli(daemon, thread, &args);
    job
}

/// The machine a job was sent to, once placement sent it
fn placed_on(origin: &Daemon, job: JobId) -> Option<homebased::machine::MachineId> {
    store(origin)
        .job_route(job)
        .unwrap()
        .filter(|route| !matches!(route.submission, JobSubmission::Held))
        .map(|route| route.authority)
}

fn wait_placed(origin: &Daemon, job: JobId) -> homebased::machine::MachineId {
    let mut placed = None;
    assert!(
        wait_until(Duration::from_secs(20), || {
            placed = placed_on(origin, job);
            placed.is_some()
        }),
        "job {job} was never placed"
    );
    placed.unwrap()
}

/// The job's final event, as the origin's inbox settled it
fn wait_final(origin: &Daemon, job: JobId) -> homebased::queue::JobEvent {
    let mut last = None;
    assert!(
        wait_until(Duration::from_secs(30), || {
            last = store(origin)
                .settled_job_events(job)
                .unwrap()
                .into_iter()
                .find(|event| {
                    matches!(
                        event.event,
                        JobEventKind::JobSucceeded
                            | JobEventKind::JobFailed
                            | JobEventKind::JobCancelled
                    )
                });
            last.is_some()
        }),
        "job {job} never ended"
    );
    last.unwrap()
}

fn capacity(daemon: &Daemon, thread: ThreadId, class: &str, count: u32, machine: Option<&str>) {
    let count = count.to_string();
    let mut args = vec!["ci", "capacity", class, &count];
    if let Some(machine) = machine {
        args.extend_from_slice(&["--machine", machine]);
    }
    cli(daemon, thread, &args);
}

#[test]
fn ci_jobs_balance_across_machines_honor_pins_and_drains_and_report_waits() {
    let (a, b) = ci_pair();
    let thread = thread(&a);
    capacity(&a, thread, "rust", 1, None);
    capacity(&a, thread, "rust", 1, Some("ci-b"));
    let gate = a.user_home.join("gate");

    // two jobs, one slot on each machine: placement uses both
    let hold = format!("while [ ! -e {} ]; do sleep 0.1; done", gate.display());
    let first = run(&a, thread, "rust", &[], &hold);
    let second = run(&a, thread, "rust", &[], &hold);
    let machines = [wait_placed(&a, first), wait_placed(&a, second)];
    assert_ne!(machines[0], machines[1], "both jobs went to one machine");

    // a pinned job waits for its machine's slot even though nothing else is free
    let pinned = run(&a, thread, "rust", &["--machine", "ci-b"], "exit 0");
    // a job for any machine, with ci-a drained, can only go to ci-b
    cli(&a, thread, &["ci", "drain", "--reason", "maintenance"]);
    let drained = run(&a, thread, "rust", &[], "exit 0");
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        placed_on(&a, pinned),
        None,
        "pinned job left before a slot freed"
    );
    assert_eq!(placed_on(&a, drained), None, "job left before a slot freed");

    // moving the waiting job to high keeps it on the origin with the new level
    let moved = cli(
        &a,
        thread,
        &["ci", "move", &drained.to_string(), "--priority", "high"],
    );
    assert_eq!(moved["priority"], "high");

    fs::write(&gate, "").unwrap();
    for job in [first, second] {
        assert_eq!(wait_final(&a, job).event, JobEventKind::JobSucceeded);
    }
    assert_eq!(wait_placed(&a, pinned), b.machine_id());
    assert_eq!(wait_placed(&a, drained), b.machine_id());
    for job in [pinned, drained] {
        assert_eq!(wait_final(&a, job).event, JobEventKind::JobSucceeded);
    }
    cli(&a, thread, &["ci", "undrain"]);

    // waits and run times per machine and class, across the fleet
    let stats = cli(&a, thread, &["ci", "stats", "--since", "1h"]);
    let rows = stats["stats"].as_array().unwrap();
    let jobs: u64 = rows.iter().map(|row| row["jobs"].as_u64().unwrap()).sum();
    assert_eq!(jobs, 4, "{stats}");
    let on_b = rows
        .iter()
        .find(|row| row["machine"] == b.machine_id().to_string())
        .unwrap();
    assert_eq!(on_b["class"], "rust");
    assert_eq!(on_b["jobs"], 3);
    assert_eq!(on_b["waited"]["count"], 3);
    // the pinned job waited on the origin for ci-b's slot, which counts
    assert!(
        on_b["waited"]["max_secs"].as_f64().unwrap() >= 2.0,
        "{on_b}"
    );
    assert!(on_b["ran"]["p90_secs"].as_f64().is_some());
}

#[test]
fn ci_submit_refuses_a_class_no_machine_offers() {
    let a = ci_daemon("ci-solo");
    let thread = thread(&a);
    capacity(&a, thread, "rust", 1, None);
    let output = a
        .cmd()
        .env("CODEX_THREAD_ID", thread.to_string())
        .current_dir(&a.user_home)
        .args(["--json", "ci", "run", "--class", "xcode", "--", "true"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "class_not_offered", "{error}");
}

#[test]
fn ci_kill_after_kills_the_process_group_and_fails_the_job() {
    let a = ci_daemon("ci-timeout");
    let thread = thread(&a);
    capacity(&a, thread, "sim", 1, None);
    let pid_file = a.user_home.join("child.pid");
    // a background child in the run's process group must die with it
    let script = format!("sleep 300 & echo $! > {}; wait", pid_file.display());
    let job = run(&a, thread, "sim", &["--kill-after", "2s"], &script);
    let event = wait_final(&a, job);
    assert_eq!(event.event, JobEventKind::JobFailed);
    let timeout = event.timeout.expect("the final event names the timeout");
    assert_eq!(timeout.limit.get(), Duration::from_secs(2));
    assert!(timeout.ran_secs >= 2);
    let pid: i32 = fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(10), || {
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err()
        }),
        "background child {pid} survived the kill"
    );
    let stats = cli(
        &a,
        thread,
        &["ci", "stats", "--since", "1h", "--class", "sim"],
    );
    assert_eq!(stats["stats"][0]["timed_out"], 1, "{stats}");
}

#[test]
fn ci_capacity_changes_at_runtime_and_a_cut_lets_running_jobs_finish() {
    let a = ci_daemon("ci-capacity");
    let thread = thread(&a);
    capacity(&a, thread, "android", 2, None);
    let gate = a.user_home.join("gate");
    let hold = format!("while [ ! -e {} ]; do sleep 0.1; done", gate.display());
    let jobs = [
        run(&a, thread, "android", &[], &hold),
        run(&a, thread, "android", &[], &hold),
    ];
    let busy = |daemon: &Daemon| {
        let slots = cli(daemon, thread, &["ci", "slots"]);
        slots["statuses"][0]["offer"]["classes"][0]["busy"]
            .as_u64()
            .unwrap()
    };
    assert!(wait_until(Duration::from_secs(20), || busy(&a) == 2));

    // a cut retires slot 2; its run continues and no third job starts
    capacity(&a, thread, "android", 1, None);
    let third = run(&a, thread, "android", &[], "exit 0");
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(placed_on(&a, third), None);
    let slots = cli(&a, thread, &["ci", "slots"]);
    let status = &slots["statuses"][0];
    assert_eq!(status["offer"]["classes"][0]["capacity"], 1);
    let retired = status["slots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|slot| slot["resource"]["name"] == "android-2")
        .unwrap();
    assert_eq!(retired["resource"]["slot"]["retired"], true);
    assert!(retired["run"].is_object());

    fs::write(&gate, "").unwrap();
    for job in jobs.into_iter().chain([third]) {
        assert_eq!(wait_final(&a, job).event, JobEventKind::JobSucceeded);
    }
}

#[test]
fn ci_git_jobs_run_the_commit_in_the_slot_clone() {
    let a = ci_daemon("ci-git");
    let thread = thread(&a);
    capacity(&a, thread, "rust", 1, None);
    let repo = a.user_home.join("repo");
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["-c", "user.name=t", "-c", "user.email=t@example.com"])
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    };
    fs::create_dir_all(repo.join("sub")).unwrap();
    git(&["init", "-q"]);
    fs::write(repo.join("sub/value"), "first\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-qm", "first"]);
    let first = git(&["rev-parse", "HEAD"]);
    fs::write(repo.join("sub/value"), "second\n").unwrap();
    git(&["commit", "-qam", "second"]);

    let out = a.user_home.join("out");
    let script = format!(
        "cat value > {out}; pwd >> {out}; touch stray",
        out = out.display()
    );
    let url = format!("file://{}", repo.display());
    let job = run(
        &a,
        thread,
        "rust",
        &["--repo", &url, "--commit", &first, "--workdir", "sub"],
        &script,
    );
    assert_eq!(wait_final(&a, job).event, JobEventKind::JobSucceeded);
    let text = fs::read_to_string(&out).unwrap();
    let mut lines = text.lines();
    assert_eq!(lines.next(), Some("first"));
    let ran_in = lines.next().unwrap();
    assert!(
        ran_in.starts_with(&a.home.join("ci/checkouts").display().to_string())
            && ran_in.ends_with("rust-1/sub"),
        "{ran_in}"
    );

    // a run killed mid-checkout leaves git's lock behind; the next run in the
    // slot must clear it rather than fail on it
    let clone = Path::new(ran_in).parent().unwrap();
    fs::write(clone.join(".git/index.lock"), "").unwrap();

    // the next run in the same slot reuses the clone and cleans untracked files
    let check = format!("test ! -e stray && cat value > {}", out.display());
    let again = run(
        &a,
        thread,
        "rust",
        &["--repo", &url, "--commit", &first, "--workdir", "sub"],
        &check,
    );
    assert_eq!(wait_final(&a, again).event, JobEventKind::JobSucceeded);
    assert_eq!(fs::read_to_string(&out).unwrap(), "first\n");
}
