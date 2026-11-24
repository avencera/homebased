//! Pools of temporary machines against a fake cloudlease: machines are
//! requested for waiting CI jobs, "boot" as real daemons from the boot data,
//! join, run the jobs with the pool's variables, renew only while their work
//! makes progress, release themselves when idle, and leave the Fleet once
//! cloudlease confirms them gone

use super::fake_cloudlease::{CLIENT_TOKEN, FakeCloudlease, FakeLease};
use super::{Daemon, register_thread, wait_until};
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use homebased::domain::ThreadId;
use homebased::queue::{JobEvent, JobEventKind, JobId};
use homebased::store::Store;
use serde_json::{Value, json};

/// Admission open whatever the test host's load and disk
const OPEN_ADMISSION: &str = "\n[ci]\nmax_load_per_core = 1000\nmin_free_disk_gib = 0\n";

/// A coordinator whose callbacks append to `$HOME/callbacks` and whose
/// `[pools]` points at `fake`
fn coordinator(fake: &FakeCloudlease) -> Daemon {
    let mut daemon = Daemon::start("pool-coordinator", true);
    let codex = daemon.user_home.join("codex");
    fs::write(
        &codex,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/callbacks\"\n",
    )
    .unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    daemon.codex_override = Some(codex);
    let token = daemon.user_home.join("cloudlease-token");
    fs::write(&token, format!("{CLIENT_TOKEN}\n")).unwrap();
    let mut config = fs::read_to_string(&daemon.config).unwrap();
    config.push_str(&format!(
        "\n[pools]\ncloudlease_url = \"{}\"\ncloudlease_token_file = \"{}\"\n\
         homebased_version = \"0.0.0-test\"\nreconcile_interval = \"1s\"\n\
         link = {{ kind = \"tailnet\", join_address = \"{}\" }}\n",
        fake.base(),
        token.display(),
        daemon.address(),
    ));
    config.push_str(OPEN_ADMISSION);
    fs::write(&daemon.config, config).unwrap();
    // the restart keeps the port the join address names
    daemon.restart();
    daemon
}

/// Machines "booted" from the fake's leases, by lease id
#[derive(Default)]
struct Machines {
    daemons: BTreeMap<String, Daemon>,
}

impl Machines {
    /// Boot a daemon for every running lease that has none, as cloud-init
    /// would: write `lease.json`, the boot data's config, and start it
    fn boot_new(&mut self, fake: &FakeCloudlease) {
        for lease in fake.leases() {
            if lease.state != "running" || self.daemons.contains_key(&lease.lease_id) {
                continue;
            }
            let daemon = boot(fake, &lease);
            self.daemons.insert(lease.lease_id.clone(), daemon);
        }
    }

    /// Stop the daemon of every lease cloudlease confirmed gone, as deleting
    /// the machine would
    fn stop_gone(&mut self, fake: &FakeCloudlease) {
        for lease in fake.leases() {
            if lease.state == "released" {
                self.daemons.remove(&lease.lease_id);
            }
        }
    }

    /// Boot and stop machines until `done` holds
    fn drive(
        &mut self,
        fake: &FakeCloudlease,
        budget: Duration,
        mut done: impl FnMut() -> bool,
    ) -> bool {
        wait_until(budget, || {
            self.boot_new(fake);
            self.stop_gone(fake);
            done()
        })
    }
}

fn boot(fake: &FakeCloudlease, lease: &FakeLease) -> Daemon {
    let mut daemon = Daemon::start(&lease.hostname, true);
    let lease_file = daemon.home.parent().unwrap().join("lease.json");
    fs::write(
        &lease_file,
        json!({
            "lease_id": lease.lease_id,
            "token": lease.token,
            "api_base": fake.base(),
            "hard_deadline_at": lease.hard_deadline_at,
            "heartbeat_ttl_secs": 4,
        })
        .to_string(),
    )
    .unwrap();
    let files = lease.boot["files"].as_array().unwrap();
    let config = files
        .iter()
        .find(|file| file["path"] == "/etc/homebased/config.toml")
        .unwrap()["content"]
        .as_str()
        .unwrap()
        .replace(
            "lease_file = \"/etc/cloudlease/lease.json\"",
            &format!(
                "lease_file = \"{}\"\nrenew_interval = \"1s\"",
                lease_file.display()
            ),
        );
    fs::write(&daemon.config, format!("{config}{OPEN_ADMISSION}")).unwrap();
    daemon.restart();
    daemon
}

fn store(daemon: &Daemon) -> Store {
    Store::open(&daemon.home.join(homebased::home::DB_NAME)).unwrap()
}

fn cli_output(daemon: &Daemon, thread: ThreadId, args: &[&str]) -> std::process::Output {
    daemon
        .cmd()
        .env("CODEX_THREAD_ID", thread.to_string())
        .current_dir(&daemon.user_home)
        .arg("--json")
        .args(args)
        .output()
        .unwrap()
}

fn cli(daemon: &Daemon, thread: ThreadId, args: &[&str]) -> Value {
    let output = cli_output(daemon, thread, args);
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

fn run(daemon: &Daemon, thread: ThreadId, class: &str, script: &str) -> JobId {
    let job = JobId::new();
    let job_text = job.to_string();
    cli(
        daemon,
        thread,
        &[
            "ci",
            "run",
            "--class",
            class,
            "--job-id",
            &job_text,
            "--kill-after",
            "10m",
            "--",
            "/bin/sh",
            "-c",
            script,
        ],
    );
    job
}

fn final_event(origin: &Daemon, job: JobId) -> Option<JobEvent> {
    store(origin)
        .settled_job_events(job)
        .unwrap()
        .into_iter()
        .find(|event| {
            matches!(
                event.event,
                JobEventKind::JobSucceeded | JobEventKind::JobFailed | JobEventKind::JobCancelled
            )
        })
}

fn pool(origin: &Daemon, thread: ThreadId, name: &str) -> Value {
    cli(origin, thread, &["pool", "show", name])
}

fn machine_states(pool: &Value) -> Vec<String> {
    pool["machines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|machine| machine["state"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn a_pool_boots_machines_for_jobs_hands_over_secrets_and_releases_idle_machines() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    let env_file = origin.user_home.join("pool.env");
    fs::write(&env_file, "HF_TOKEN=s3cret-upload-token\n").unwrap();
    let env_text = env_file.display().to_string();
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "afv",
            "--machine-class",
            "do-c16",
            "--max",
            "2",
            "--deadline",
            "1h",
            "--idle-limit",
            "3s",
            "--env-file",
            &env_text,
        ],
    );

    // a job whose kill limit outlasts the pool is refused at submit
    let late = cli_output(
        &origin,
        thread,
        &[
            "ci",
            "run",
            "--class",
            "afv",
            "--kill-after",
            "2h",
            "--",
            "true",
        ],
    );
    assert!(!late.status.success());
    let refusal = format!(
        "{}{}",
        String::from_utf8_lossy(&late.stdout),
        String::from_utf8_lossy(&late.stderr)
    );
    assert!(refusal.contains("pool_deadline_too_close"), "{refusal}");

    // two waiting jobs and one slot per machine: the pool asks for two machines
    let outputs = origin.user_home.join("outputs");
    fs::create_dir_all(&outputs).unwrap();
    let script = format!(
        "printf '%s' \"$HF_TOKEN\" > {}/$HOMEBASED_JOB_ID; sleep 1",
        outputs.display()
    );
    let jobs = [
        run(&origin, thread, "afv", &script),
        run(&origin, thread, "afv", &script),
    ];

    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(90), || {
            jobs.iter().all(|job| final_event(&origin, *job).is_some())
        }),
        "jobs never ended: {}\n{}\n{}",
        pool(&origin, thread, "afv"),
        cli(&origin, thread, &["ci", "slots"]),
        cli(
            &origin,
            thread,
            &["resource", "job", "show", &jobs[0].to_string()]
        )
    );
    for job in jobs {
        assert_eq!(
            final_event(&origin, job).unwrap().event,
            JobEventKind::JobSucceeded
        );
        // the secret reached the run through the join, never through boot data
        let written = fs::read_to_string(outputs.join(job.to_string())).unwrap();
        assert_eq!(written, "s3cret-upload-token");
    }
    let leases = fake.leases();
    assert_eq!(
        leases.len(),
        2,
        "the pool asked for more machines than jobs"
    );
    for lease in &leases {
        assert!(!lease.boot.to_string().contains("s3cret"));
        assert!(lease.renewals > 0, "{} never renewed", lease.hostname);
    }

    // idle machines release themselves; the coordinator drops each from the
    // Fleet once cloudlease confirms it gone
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || {
            machine_states(&pool(&origin, thread, "afv")) == ["released", "released"]
        }),
        "machines never released: {}",
        pool(&origin, thread, "afv")
    );
    for lease in fake.leases() {
        assert_eq!(
            lease.release.as_ref().unwrap(),
            &("idle".to_owned(), "machine")
        );
    }
    for machine in pool(&origin, thread, "afv")["machines"].as_array().unwrap() {
        assert_eq!(machine["reason"], "idle");
    }
    let fleet = cli(&origin, thread, &["fleet", "machines"]);
    for lease in &leases {
        assert!(
            !fleet.to_string().contains(&lease.hostname),
            "{} is still in the Fleet: {fleet}",
            lease.hostname
        );
    }

    // releasing the pool releases its run and ends it
    cli(&origin, thread, &["pool", "release", "afv"]);
    assert!(wait_until(Duration::from_secs(20), || {
        pool(&origin, thread, "afv")["state"] == "released"
    }));
}

#[test]
fn renewal_lasts_while_output_flows_and_a_stalled_job_fails_with_its_machine() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "stall",
            "--machine-class",
            "do-c16",
            "--max",
            "1",
            "--deadline",
            "1h",
            "--stall-after",
            "3s",
        ],
    );
    // output every second for 10 s, well past the 4 s heartbeat, then silence
    let job = run(
        &origin,
        thread,
        "stall",
        "for i in 1 2 3 4 5 6 7 8 9 10; do echo tick $i; sleep 1; done; sleep 600",
    );

    let mut machines = Machines::default();
    // while output flows the lease stays alive past several heartbeats
    let mut ticking = None;
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || {
            ticking = fake
                .leases()
                .into_iter()
                .find(|lease| lease.state == "running" && lease.renewals >= 6);
            ticking.is_some()
        }),
        "the lease was never renewed while output flowed: {:?}",
        fake.leases()
    );

    // silence for the stall window stops renewal; cloudlease lets the
    // heartbeat lapse and the coordinator fails the job with its machine
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || final_event(&origin, job)
            .is_some()),
        "the stalled job never ended: {}",
        pool(&origin, thread, "stall")
    );
    let event = final_event(&origin, job).unwrap();
    assert_eq!(event.event, JobEventKind::JobFailed);
    let released = event.machine_released.unwrap();
    assert_eq!(released.reason.to_string(), "heartbeat_lapsed");
    assert!(released.output_lost);
    let lease = &fake.leases()[0];
    assert_eq!(
        lease.release.as_ref().unwrap(),
        &("heartbeat_lapsed".to_owned(), "cloudlease")
    );
    assert_eq!(
        machine_states(&pool(&origin, thread, "stall")),
        ["released"]
    );
}
