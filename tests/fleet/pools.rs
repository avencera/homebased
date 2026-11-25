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
    // a fixed link port, as in production, so boot data's addresses survive
    // the coordinator's restarts
    let link_port = std::net::UdpSocket::bind("0.0.0.0:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    fs::write(&token, format!("{CLIENT_TOKEN}\n")).unwrap();
    let mut config = fs::read_to_string(&daemon.config).unwrap();
    config.push_str(&format!(
        "\n[pools]\ncloudlease_url = \"{}\"\ncloudlease_token_file = \"{}\"\n\
         homebased_version = \"0.0.0-test\"\nreconcile_interval = \"1s\"\n\
         \n[fleet.iroh]\nrelays = \"disabled\"\nport = {link_port}\n",
        fake.base(),
        token.display(),
    ));
    config.push_str(OPEN_ADMISSION);
    fs::write(&daemon.config, config).unwrap();
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
    // the deployed cloudlease Worker still writes `interruption_notice`,
    // which a machine must ignore
    let credential = json!({
        "lease_id": lease.lease_id,
        "token": lease.token,
        "api_base": fake.base(),
        "hard_deadline_at": lease.hard_deadline_at,
        "heartbeat_ttl_secs": 4,
        "interruption_notice": "aws_imds",
    });
    fs::write(&lease_file, credential.to_string()).unwrap();
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
                "lease_file = \"{}\"\nrenew_interval = \"1s\"\nlink_listen = \"127.0.0.1:0\"",
                lease_file.display()
            ),
        );
    // the boot data's own `[ci]` sets the pool's disk floor; keep it, and
    // open admission whatever the test host's load
    let config = config.replace("\n[ci]\n", "\n[ci]\nmax_load_per_core = 1000\n");
    fs::write(&daemon.config, config).unwrap();
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
    run_with_kill(daemon, thread, class, "10m", script)
}

fn run_with_kill(
    daemon: &Daemon,
    thread: ThreadId,
    class: &str,
    kill: &str,
    script: &str,
) -> JobId {
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
            kill,
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

/// One HTTP exchange over the pool link, as an endpoint with `key` would
/// make it: the status, or `None` when the coordinator closed the connection
/// instead of answering
fn link_request(lease: &FakeLease, key: iroh::SecretKey, request: &str) -> Option<u16> {
    link_answer(lease, key, request)?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

/// The raw HTTP answer to one request over the coordinator's link
fn link_answer(lease: &FakeLease, key: iroh::SecretKey, request: &str) -> Option<String> {
    let config = lease.boot["files"][0]["content"].as_str().unwrap();
    let temporary = &toml::from_str::<toml::Value>(config).unwrap()["temporary"];
    let hub: iroh::EndpointId = temporary["hub"].as_str().unwrap().parse().unwrap();
    let addrs = temporary["hub_addrs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|addr| iroh::TransportAddr::Ip(addr.as_str().unwrap().parse().unwrap()));
    let hub = iroh::EndpointAddr::from_parts(hub, addrs);
    let request = request.to_owned();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                .relay_mode(iroh::RelayMode::Disabled)
                .secret_key(key)
                .bind()
                .await
                .unwrap();
            let connection = endpoint.connect(hub, b"homebased/fleet/1").await.unwrap();
            let (mut send, mut recv) = connection.open_bi().await.ok()?;
            send.write_all(request.as_bytes()).await.ok()?;
            send.finish().ok()?;
            let answer = recv.read_to_end(64 * 1024).await.ok()?;
            endpoint.close().await;
            Some(String::from_utf8_lossy(&answer).into_owned())
        })
}

fn get(path: &str) -> String {
    format!("GET {path} HTTP/1.1\r\nHost: hub\r\nConnection: close\r\n\r\n")
}

fn post(path: &str, body: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: hub\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// The iroh key a booted machine made for itself
fn machine_key(daemon: &Daemon) -> iroh::SecretKey {
    let bytes: [u8; 32] = fs::read(daemon.home.join("iroh/secret.key"))
        .unwrap()
        .try_into()
        .unwrap();
    iroh::SecretKey::from_bytes(&bytes)
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

    // even with a lease's own token, a join that claims an id the coordinator
    // already knows is refused: when the lease ends, the coordinator fails
    // the jobs and forgets the Fleet entry of the id it bound
    assert!(wait_until(Duration::from_secs(30), || !fake
        .leases()
        .is_empty()));
    let first = fake.leases().remove(0);
    let config = first.boot["files"][0]["content"].as_str().unwrap();
    let temporary = &toml::from_str::<toml::Value>(config).unwrap()["temporary"];
    let coordinator_id = fs::read_to_string(origin.home.join("machine-id")).unwrap();
    let claim = json!({
        "api_version": 1,
        "pool": "afv",
        "slot": temporary["slot"].as_str().unwrap(),
        "machine": coordinator_id.trim(),
        "lease": first.lease_id,
        "join_token": temporary["join_token"].as_str().unwrap(),
    });
    let answer = link_answer(
        &first,
        iroh::SecretKey::generate(),
        &post("/v1/cluster/pool/join", &claim.to_string()),
    )
    .unwrap();
    assert!(answer.contains("already known"), "{answer}");

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

    // the link serves an unknown endpoint only the join route, and closes
    // its connection once that fails
    let probe = get("/v1/cluster/machine?api_version=1");
    let stranger = iroh::SecretKey::generate();
    assert_eq!(
        link_request(&leases[0], stranger.clone(), &probe),
        Some(404)
    );
    let forged_join = post("/v1/cluster/pool/join", "{}");
    assert!(link_request(&leases[0], stranger, &forged_join).is_some_and(|status| status >= 400));
    // a joined machine reaches the member routes, and nothing that starts
    // work or reads other machines' tasks
    let member = machine_key(&machines.daemons[&leases[0].lease_id]);
    assert_eq!(link_request(&leases[0], member.clone(), &probe), Some(200));
    for forbidden in [
        post("/v1/cluster/executions", "{}"),
        post("/v1/cluster/messages", "{}"),
        get("/v1/cluster/tasks"),
        post("/v1/pools", "{}"),
    ] {
        assert_eq!(
            link_request(&leases[0], member.clone(), &forbidden),
            Some(404),
            "{forbidden}"
        );
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
    // once its lease ended, the machine's id gets only the join route again
    assert_eq!(link_request(&leases[0], member, &probe), Some(404));
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
    let mut origin = coordinator(&fake);
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

    // a coordinator restart keeps its endpoint id and serves the joined
    // machine again under the id its lease is bound to
    let lease = ticking.unwrap();
    let member = machine_key(&machines.daemons[&lease.lease_id]);
    origin.restart();
    let probe = get("/v1/cluster/machine?api_version=1");
    assert!(
        wait_until(Duration::from_secs(20), || {
            link_request(&lease, member.clone(), &probe) == Some(200)
        }),
        "the restarted coordinator never served its machine again"
    );

    // silence for the stall window stops renewal; cloudlease lets the
    // heartbeat lapse and the coordinator fails the job with its machine
    assert!(
        // the machine renews while it owes events; after the restart it
        // delivers them once the link is back and its retries come round
        machines.drive(&fake, Duration::from_secs(90), || final_event(&origin, job)
            .is_some()),
        "the stalled job never ended: {}\n{:?}",
        pool(&origin, thread, "stall"),
        fake.leases()
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

#[test]
fn a_pool_whose_run_cloudlease_lost_ends_and_fails_its_waiting_jobs() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "lost",
            "--machine-class",
            "do-c16",
            "--max",
            "1",
            "--deadline",
            "1h",
        ],
    );
    // the coordinator asks for a machine, which never boots here
    let job = run(&origin, thread, "lost", "true");
    assert!(wait_until(Duration::from_secs(30), || !fake
        .leases()
        .is_empty()));

    fake.forget_runs();
    assert!(
        wait_until(Duration::from_secs(30), || {
            pool(&origin, thread, "lost")["state"] == "released"
        }),
        "the pool never ended: {}",
        pool(&origin, thread, "lost")
    );
    let shown = pool(&origin, thread, "lost");
    assert_eq!(shown["code"], "run_not_found", "{shown}");
    assert!(wait_until(Duration::from_secs(20), || final_event(
        &origin, job
    )
    .is_some()));
    assert_eq!(
        final_event(&origin, job).unwrap().event,
        JobEventKind::JobFailed
    );
}

/// Error code a job that never got a machine ended with
fn held_code(event: &JobEvent) -> Value {
    serde_json::to_value(&event.held_ending).unwrap()["code"].clone()
}

/// Slots of `class` a daemon offers, as `ci slots` shows them
fn class_capacity(daemon: &Daemon, thread: ThreadId, class: &str) -> Option<u64> {
    fn find(value: &Value, class: &str) -> Option<u64> {
        match value {
            Value::Object(fields) if fields.get("class").is_some_and(|name| name == class) => {
                fields.get("capacity")?.as_u64()
            }
            Value::Object(fields) => fields.values().find_map(|value| find(value, class)),
            Value::Array(items) => items.iter().find_map(|value| find(value, class)),
            _ => None,
        }
    }
    find(&cli(daemon, thread, &["ci", "slots"]), class)
}

#[test]
fn a_pool_stops_asking_after_machines_the_provider_cannot_create() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    fake.fail_creates(10);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "flaky",
            "--machine-class",
            "do-c16",
            "--max",
            "2",
            "--deadline",
            "1h",
            "--spend-cap",
            "12.50",
        ],
    );
    let job = run(&origin, thread, "flaky", "true");

    // three failed creates, with a pause after each, and then no more
    assert!(
        wait_until(Duration::from_secs(60), || {
            pool(&origin, thread, "flaky")["state"] == "released"
        }),
        "the pool never stopped: {}",
        pool(&origin, thread, "flaky")
    );
    let shown = pool(&origin, thread, "flaky");
    assert_eq!(shown["reason"], "create_failed", "{shown}");
    let leases = fake.leases();
    assert_eq!(leases.len(), 3, "{leases:?}");
    let created: Vec<_> = leases.iter().map(|lease| lease.created_at).collect();
    assert!(
        created
            .windows(2)
            .all(|pair| pair[1] - pair[0] >= chrono::Duration::seconds(3)),
        "creates did not back off: {created:?}"
    );
    for machine in shown["machines"].as_array().unwrap() {
        assert_eq!(machine["reason"], "create_failed", "{shown}");
    }
    assert_eq!(fake.spend_caps()["flaky"], Some(1250));

    assert!(wait_until(Duration::from_secs(20), || final_event(
        &origin, job
    )
    .is_some()));
    let event = final_event(&origin, job).unwrap();
    assert_eq!(event.event, JobEventKind::JobFailed);
    assert_eq!(held_code(&event), "create_failed");
}

#[test]
fn creates_whose_answers_were_lost_still_count_as_failed_in_a_row() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    // each create's answer is lost, and the lease shows provisioning for a few
    // passes before the provider fails it
    fake.fail_creates(10);
    fake.slow_creates(10);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "slow",
            "--machine-class",
            "do-c16",
            "--max",
            "1",
            "--deadline",
            "1h",
        ],
    );
    run(&origin, thread, "slow", "true");

    assert!(
        wait_until(Duration::from_secs(90), || {
            pool(&origin, thread, "slow")["state"] == "released"
        }),
        "the pool never stopped: {}",
        pool(&origin, thread, "slow")
    );
    assert_eq!(pool(&origin, thread, "slow")["reason"], "create_failed");
    assert_eq!(fake.leases().len(), 3);
}

#[test]
fn a_refused_create_stops_the_pool_and_running_jobs_finish() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "repriced",
            "--machine-class",
            "do-c16",
            "--max",
            "2",
            "--deadline",
            "1h",
            "--idle-limit",
            "3s",
        ],
    );
    let started = origin.user_home.join("started");
    let running = run(
        &origin,
        thread,
        "repriced",
        &format!("touch {}; sleep 8", started.display()),
    );
    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || started.exists()),
        "the first job never started: {}",
        pool(&origin, thread, "repriced")
    );

    // the class was repriced: cloudlease refuses new machines with run_ended
    fake.refuse_creates(409, "run_ended");
    let waiting = run(&origin, thread, "repriced", "true");
    assert!(
        machines.drive(&fake, Duration::from_secs(30), || final_event(
            &origin, waiting
        )
        .is_some()),
        "the waiting job never ended: {}",
        pool(&origin, thread, "repriced")
    );
    let event = final_event(&origin, waiting).unwrap();
    assert_eq!(event.event, JobEventKind::JobFailed);
    assert_eq!(held_code(&event), "run_ended");
    let shown = pool(&origin, thread, "repriced");
    assert_eq!(shown["state"], "stopped", "{shown}");
    assert_eq!(shown["code"], "run_ended", "{shown}");

    // a stopped pool takes no new job, and says why
    let late = cli_output(
        &origin,
        thread,
        &["ci", "run", "--class", "repriced", "--", "true"],
    );
    assert!(!late.status.success());
    let refusal =
        String::from_utf8_lossy(&late.stderr).into_owned() + &String::from_utf8_lossy(&late.stdout);
    assert!(refusal.contains("run_ended"), "{refusal}");

    // the running job keeps its machine; the machine then idles out and
    // the pool ends with it
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || {
            pool(&origin, thread, "repriced")["state"] == "released"
        }),
        "the stopped pool never ended: {}",
        pool(&origin, thread, "repriced")
    );
    assert_eq!(
        final_event(&origin, running).unwrap().event,
        JobEventKind::JobSucceeded
    );
    assert_eq!(fake.leases().len(), 1);
    assert_eq!(
        fake.leases()[0].release.as_ref().unwrap(),
        &("idle".to_owned(), "machine")
    );
}

#[test]
fn a_machine_whose_lease_ended_stops_taking_jobs() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "capped",
            "--machine-class",
            "do-c16",
            "--max",
            "1",
            "--deadline",
            "1h",
        ],
    );
    let started = origin.user_home.join("capped-started");
    let job = run(
        &origin,
        thread,
        "capped",
        &format!("touch {}; sleep 600", started.display()),
    );
    let mut machines = Machines::default();
    // the job must hold the machine's slot before the lease ends, or it
    // waits for another machine
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || started.exists()),
        "the job never started: {:?}",
        fake.leases()
    );
    let lease = fake.leases()[0].clone();
    let machine = &machines.daemons[&lease.lease_id];
    let machine_thread = self::thread(machine);
    assert!(wait_until(Duration::from_secs(20), || {
        class_capacity(machine, machine_thread, "capped") == Some(1)
    }));

    // cloudlease ends the lease, as a reached spend cap does: the next renew
    // gets 410 lease_ended and the machine closes its class at once
    fake.end_lease(&lease.lease_id, "spend_cap");
    assert!(
        wait_until(Duration::from_secs(20), || {
            class_capacity(machine, machine_thread, "capped") == Some(0)
        }),
        "the machine still offers its class after its lease ended"
    );
    assert!(wait_until(Duration::from_secs(20), || final_event(
        &origin, job
    )
    .is_some()));
    let event = final_event(&origin, job).unwrap();
    assert_eq!(event.event, JobEventKind::JobFailed);
    assert_eq!(
        event.machine_released.unwrap().reason.to_string(),
        "spend_cap"
    );
}

/// `ci run` of a job of `class` with `kill_after` and `extra` arguments
/// before the command: its id and the command's output
fn ci_run(
    daemon: &Daemon,
    thread: ThreadId,
    class: &str,
    kill_after: &str,
    extra: &[&str],
    script: &str,
) -> (JobId, std::process::Output) {
    let job = JobId::new();
    let job_text = job.to_string();
    let mut args = vec![
        "ci",
        "run",
        "--class",
        class,
        "--job-id",
        &job_text,
        "--kill-after",
        kill_after,
    ];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--", "/bin/sh", "-c", script]);
    (job, cli_output(daemon, thread, &args))
}

/// The job of a `ci run`, asserting it was accepted
fn accepted((job, output): (JobId, std::process::Output)) -> JobId {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    job
}

/// The disk floor of the first offer `ci slots` shows closed for low disk
fn low_disk_floor(value: &Value) -> Option<u64> {
    match value {
        Value::Object(fields) if fields.get("state").is_some_and(|state| state == "low_disk") => {
            fields.get("floor")?.as_u64()
        }
        Value::Object(fields) => fields.values().find_map(low_disk_floor),
        Value::Array(items) => items.iter().find_map(low_disk_floor),
        _ => None,
    }
}

/// A task on `daemon` that sleeps `secs`, for jobs to wait on
fn sleeper(daemon: &Daemon, thread: ThreadId, secs: u32) -> String {
    let task = cli(
        daemon,
        thread,
        &["task", "run", "--", "sleep", &secs.to_string()],
    );
    task["id"].as_str().unwrap().to_owned()
}

#[test]
fn a_job_no_pool_machine_can_run_gets_no_machine_and_fails() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "tight",
            "--machine-class",
            "do-c16",
            "--max",
            "2",
            "--deadline",
            "3m",
            "--boot-timeout",
            "10s",
            "--idle-limit",
            "3s",
        ],
    );

    // inside the deadline, but not once a new machine has booted: refused
    let (_, late) = ci_run(&origin, thread, "tight", "2m55s", &[], "true");
    assert!(!late.status.success());
    let refusal =
        String::from_utf8_lossy(&late.stderr).into_owned() + &String::from_utf8_lossy(&late.stdout);
    assert!(refusal.contains("pool_deadline_too_close"), "{refusal}");

    // a new machine could still run this job now, but it waits for a task
    // that ends only after no machine could
    let task = sleeper(&origin, thread, 25);
    let job = accepted(ci_run(
        &origin,
        thread,
        "tight",
        "2m40s",
        &["--after", &task],
        "true",
    ));
    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(45), || final_event(&origin, job)
            .is_some()),
        "the job never ended: {}\n{:?}",
        pool(&origin, thread, "tight"),
        fake.leases()
    );
    let event = final_event(&origin, job).unwrap();
    assert_eq!(event.event, JobEventKind::JobFailed);
    assert_eq!(held_code(&event), "pool_deadline_too_close");
    // no machine was ever asked for, so none idled out to be asked for again
    assert!(fake.leases().is_empty(), "{:?}", fake.leases());
    assert_eq!(pool(&origin, thread, "tight")["state"], "open");
}

#[test]
fn a_pool_machine_admits_jobs_by_the_pools_disk_floor() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    // a floor no disk meets
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "disk",
            "--machine-class",
            "do-c16",
            "--max",
            "1",
            "--deadline",
            "1h",
            "--min-free-disk",
            "1000000000",
        ],
    );
    let job = run(&origin, thread, "disk", "true");
    let mut machines = Machines::default();
    let mut floor = None;
    assert!(
        wait_until(Duration::from_secs(60), || {
            machines.boot_new(&fake);
            let Some(daemon) = machines_first(&fake, &machines) else {
                return false;
            };
            floor = low_disk_floor(&cli(daemon, thread, &["ci", "slots"]));
            floor.is_some()
        }),
        "the machine never closed admission for disk: {}",
        pool(&origin, thread, "disk")
    );
    assert_eq!(floor, Some(1_000_000_000_u64 << 30));
    assert!(final_event(&origin, job).is_none());
    cli(&origin, thread, &["pool", "release", "disk"]);
}

/// The daemon of the pool's first lease, once it booted
fn machines_first<'a>(fake: &FakeCloudlease, machines: &'a Machines) -> Option<&'a Daemon> {
    let lease = fake.leases().into_iter().next()?;
    machines.daemons.get(&lease.lease_id)
}

#[test]
fn a_run_cloudlease_ends_outside_the_coordinator_ends_the_pool() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "capped",
            "--machine-class",
            "do-c16",
            "--max",
            "2",
            "--deadline",
            "1h",
        ],
    );
    let started = origin.user_home.join("started");
    let running = run(
        &origin,
        thread,
        "capped",
        &format!("touch {}; sleep 60", started.display()),
    );
    // a job that waits on a task asks for no machine yet
    let task = sleeper(&origin, thread, 120);
    let waiting = accepted(ci_run(
        &origin,
        thread,
        "capped",
        "10m",
        &["--after", &task],
        "true",
    ));
    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || started.exists()),
        "the first job never started: {}",
        pool(&origin, thread, "capped")
    );

    // the run's spend cap: cloudlease releases every lease of it
    fake.end_runs("spend_cap");
    assert!(
        machines.drive(&fake, Duration::from_secs(30), || {
            pool(&origin, thread, "capped")["state"] == "released"
        }),
        "the pool never ended: {}",
        pool(&origin, thread, "capped")
    );
    let shown = pool(&origin, thread, "capped");
    assert_eq!(shown["reason"], "run_ended", "{shown}");
    assert_eq!(shown["cause"], "spend_cap", "{shown}");
    assert_eq!(machine_states(&shown), ["released"]);

    assert!(wait_until(Duration::from_secs(20), || {
        final_event(&origin, waiting).is_some() && final_event(&origin, running).is_some()
    }));
    let event = final_event(&origin, waiting).unwrap();
    assert_eq!(event.event, JobEventKind::JobFailed);
    assert_eq!(held_code(&event), "run_ended");
    let event = final_event(&origin, running).unwrap();
    assert_eq!(event.event, JobEventKind::JobFailed);
    assert_eq!(
        event.machine_released.unwrap().reason.to_string(),
        "spend_cap"
    );
    assert_eq!(fake.leases().len(), 1);
}

#[test]
fn a_machine_that_never_joins_is_released_before_its_boot_grace_ends() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    // a lease's first heartbeat deadline gives the boot grace; no flag
    fake.set_boot_grace(Duration::from_secs(8));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "slowboot",
            "--machine-class",
            "do-c16",
            "--max",
            "1",
            "--deadline",
            "1h",
        ],
    );
    run(&origin, thread, "slowboot", "true");

    // the machine never boots here: the coordinator releases it a quarter of
    // the grace before cloudlease would let its heartbeat lapse
    assert!(
        wait_until(Duration::from_secs(30), || {
            fake.leases()
                .first()
                .is_some_and(|lease| lease.release.is_some())
        }),
        "the machine was never released: {}",
        pool(&origin, thread, "slowboot")
    );
    let lease = fake.leases().remove(0);
    assert_eq!(
        lease.release.as_ref().unwrap(),
        &("requested".to_owned(), "client"),
        "{lease:?}"
    );
    assert!(
        wait_until(Duration::from_secs(10), || {
            pool(&origin, thread, "slowboot")["machines"][0]["reason"] == "boot_timeout"
        }),
        "{}",
        pool(&origin, thread, "slowboot")
    );
    cli(&origin, thread, &["pool", "release", "slowboot"]);
}

#[test]
fn a_released_pool_leaves_later_jobs_of_its_class_to_the_fleet() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "shared",
            "--machine-class",
            "do-c16",
            "--max",
            "1",
            "--deadline",
            "1h",
        ],
    );
    let task = sleeper(&origin, thread, 12);
    let during = accepted(ci_run(
        &origin,
        thread,
        "shared",
        "1m",
        &["--after", &task],
        "true",
    ));
    cli(&origin, thread, &["pool", "release", "shared"]);
    assert!(
        wait_until(Duration::from_secs(30), || {
            pool(&origin, thread, "shared")["state"] == "released"
        }),
        "the pool never released: {}",
        pool(&origin, thread, "shared")
    );
    assert!(wait_until(Duration::from_secs(30), || final_event(
        &origin, during
    )
    .is_some()));
    let ended = final_event(&origin, during).unwrap();
    assert_eq!(ended.event, JobEventKind::JobFailed);
    assert_eq!(held_code(&ended), "pool_released", "{ended:?}");

    // the coordinator now offers the class itself, as a permanent Fleet
    // machine would, so a new job waits for its slot and runs there
    cli(&origin, thread, &["ci", "capacity", "shared", "1"]);
    let after = accepted(ci_run(
        &origin,
        thread,
        "shared",
        "1m",
        &["--after", &task],
        "true",
    ));
    assert!(
        wait_until(Duration::from_secs(40), || final_event(&origin, after)
            .is_some()),
        "the job submitted after the release never ended"
    );
    let event = final_event(&origin, after).unwrap();
    assert_eq!(event.event, JobEventKind::JobSucceeded, "{event:?}");
}

#[test]
fn a_deleted_pool_machine_leaves_every_job_an_ending_and_a_status() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "gone",
            "--machine-class",
            "do-c16",
            "--max",
            "1",
            "--deadline",
            "1h",
            "--idle-limit",
            "4s",
        ],
    );
    let done = run(&origin, thread, "gone", "true");
    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || final_event(
            &origin, done
        )
        .is_some()),
        "the job never ended: {}",
        pool(&origin, thread, "gone")
    );
    assert_eq!(
        final_event(&origin, done).unwrap().event,
        JobEventKind::JobSucceeded
    );

    // a job pinned to the machine waits on a task that outlasts its idle limit
    let hostname = fake.leases()[0].hostname.clone();
    let task = sleeper(&origin, thread, 60);
    let pinned = accepted(ci_run(
        &origin,
        thread,
        "gone",
        "1m",
        &["--machine", &hostname, "--after", &task],
        "true",
    ));

    assert!(
        machines.drive(&fake, Duration::from_secs(40), || {
            machine_states(&pool(&origin, thread, "gone")) == ["released"]
        }),
        "the machine never released: {}",
        pool(&origin, thread, "gone")
    );
    assert!(
        wait_until(Duration::from_secs(20), || final_event(&origin, pinned)
            .is_some()),
        "the pinned job never ended once its machine was gone"
    );
    assert_eq!(
        final_event(&origin, pinned).unwrap().event,
        JobEventKind::JobFailed
    );

    // the origin still answers for a job whose machine is gone
    let shown = cli(
        &origin,
        thread,
        &["resource", "job", "show", &done.to_string()],
    );
    assert_eq!(shown["ended"]["state"], "succeeded", "{shown}");
    let shown = cli(
        &origin,
        thread,
        &["resource", "job", "show", &pinned.to_string()],
    );
    assert_eq!(shown["held"]["state"], "failed", "{shown}");
}

#[test]
fn an_idle_machine_stays_until_its_paid_time_ends_and_takes_a_job_meanwhile() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "metal",
            "--machine-class",
            "pnap-s2",
            "--max",
            "1",
            "--deadline",
            "1h",
            "--idle-limit",
            "2s",
        ],
    );

    // the first job runs until the test lets it end, so the hold starts
    // from a known moment however long the machine took to boot
    let marker = origin.user_home.join("first-may-end");
    let first = run(
        &origin,
        thread,
        "metal",
        &format!("while [ ! -e {} ]; do sleep 0.2; done", marker.display()),
    );
    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(90), || {
            fake.leases().iter().any(|lease| lease.renewals > 0)
        }),
        "the machine never renewed: {}",
        pool(&origin, thread, "metal")
    );
    let hold_until = chrono::Utc::now() + chrono::Duration::seconds(20);
    fake.hold_idle_until(hold_until);
    fs::write(&marker, "").unwrap();
    assert!(machines.drive(&fake, Duration::from_secs(30), || {
        final_event(&origin, first).is_some()
    }));
    let idle_from = chrono::Utc::now();

    // well past its 2 second idle limit the machine is still there
    machines.drive(&fake, Duration::from_secs(6), || false);
    let leases = fake.leases();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].state, "running", "released at its idle limit");
    assert!(chrono::Utc::now() - idle_from >= chrono::Duration::seconds(5));

    // a job that arrives while it waits runs on it, with no new machine
    let second = run(&origin, thread, "metal", "true");
    assert!(machines.drive(&fake, Duration::from_secs(30), || {
        final_event(&origin, second).is_some()
    }));
    assert_eq!(
        final_event(&origin, second).unwrap().event,
        JobEventKind::JobSucceeded
    );
    assert_eq!(fake.leases().len(), 1, "the job asked for a new machine");

    // once its paid time ends it releases itself as idle, and not before
    assert!(
        machines.drive(&fake, Duration::from_secs(40), || {
            fake.leases()[0].release_asked_at.is_some()
        }),
        "the machine never released: {}",
        pool(&origin, thread, "metal")
    );
    let lease = fake.leases().remove(0);
    assert_eq!(lease.release.unwrap(), ("idle".to_owned(), "machine"));
    let asked = lease.release_asked_at.unwrap();
    assert!(
        asked >= hold_until,
        "released at {asked}, held until {hold_until}"
    );
    assert!(asked - hold_until < chrono::Duration::seconds(10));
}

#[path = "pool_repos.rs"]
mod repos;

/// Every event of a job its origin delivered, in order
fn events(origin: &Daemon, job: JobId) -> Vec<JobEvent> {
    store(origin).settled_job_events(job).unwrap()
}

/// Lines in `path`, 0 while it does not exist
fn lines(path: &std::path::Path) -> usize {
    fs::read_to_string(path).map_or(0, |text| text.lines().count())
}

/// A job that appends a line to `starts` each time it starts; its first
/// run works for two minutes and fails, any later run succeeds at once
fn second_run_succeeds(starts: &std::path::Path) -> String {
    let starts = starts.display();
    format!(
        "echo start >> {starts}; if [ \"$(wc -l < {starts})\" -ge 2 ]; then echo second run; \
         exit 0; fi; for i in $(seq 120); do echo working; sleep 1; done; exit 1"
    )
}

/// A job that appends a line to `starts` each time it starts and works for
/// two minutes
fn always_works(starts: &std::path::Path) -> String {
    format!(
        "echo start >> {}; for i in $(seq 120); do echo working; sleep 1; done",
        starts.display()
    )
}

/// The lease cloudlease has not ended yet; the tests' pools have one
fn live_lease(fake: &FakeCloudlease) -> Option<FakeLease> {
    fake.leases()
        .into_iter()
        .find(|lease| lease.state == "running")
}

fn create_pool(origin: &Daemon, thread: ThreadId, name: &str, extra: &[&str]) {
    let mut args = vec![
        "pool",
        "create",
        name,
        "--machine-class",
        "aws-c8a-4xl",
        "--deadline",
        "1h",
    ];
    args.extend_from_slice(extra);
    cli(origin, thread, &args);
}

#[test]
fn an_interrupted_job_runs_again_on_a_replacement_machine_and_succeeds() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    create_pool(&origin, thread, "spot", &["--max", "1"]);
    let starts = origin.user_home.join("starts");
    let job = run(&origin, thread, "spot", &second_run_succeeds(&starts));

    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || lines(&starts) == 1),
        "the job never started: {}",
        pool(&origin, thread, "spot")
    );
    // AWS took the instance back, and cloudlease's presence check ended
    // its lease
    let first = live_lease(&fake).unwrap();
    fake.end_lease(&first.lease_id, "interrupted");

    assert!(
        machines.drive(&fake, Duration::from_secs(90), || final_event(&origin, job)
            .is_some()),
        "the job never ended: {}\n{:?}",
        pool(&origin, thread, "spot"),
        fake.leases()
    );
    let event = final_event(&origin, job).unwrap();
    assert_eq!(event.event, JobEventKind::JobSucceeded, "{event:?}");
    assert_eq!(lines(&starts), 2);

    // the submitter saw the rerun, and the second machine's events follow
    // the first run's in one sequence
    let events = events(&origin, job);
    let seqs: Vec<u64> = events.iter().map(|event| event.seq).collect();
    assert_eq!(seqs, (1..=events.len() as u64).collect::<Vec<_>>());
    let preempted: Vec<&JobEvent> = events
        .iter()
        .filter(|event| event.event == JobEventKind::JobPreempted)
        .collect();
    assert_eq!(preempted.len(), 1, "{events:?}");
    let lost = preempted[0].machine_lost.as_ref().unwrap();
    assert_eq!(lost.reason.to_string(), "interrupted");
    assert_eq!(lost.machine.to_string(), first.hostname);
    assert_eq!((lost.attempt, lost.max_attempts), (2, 3));
    assert_eq!(fake.leases().len(), 2);
}

#[test]
fn a_job_interrupted_on_every_machine_stops_at_its_retry_limit() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    create_pool(
        &origin,
        thread,
        "flaky",
        &["--max", "1", "--job-retries", "1"],
    );
    let starts = origin.user_home.join("starts");
    let job = run(&origin, thread, "flaky", &always_works(&starts));

    let mut machines = Machines::default();
    for attempt in 1..=2 {
        assert!(
            machines.drive(&fake, Duration::from_secs(90), || lines(&starts) == attempt),
            "run {attempt} never started: {}",
            pool(&origin, thread, "flaky")
        );
        let lease = live_lease(&fake).unwrap();
        fake.end_lease(&lease.lease_id, "interrupted");
    }

    assert!(
        machines.drive(&fake, Duration::from_secs(60), || final_event(&origin, job)
            .is_some()),
        "the job never ended: {}",
        pool(&origin, thread, "flaky")
    );
    let event = final_event(&origin, job).unwrap();
    assert_eq!(event.event, JobEventKind::JobFailed);
    let released = event.machine_released.unwrap();
    assert_eq!(released.reason.to_string(), "interrupted");
    assert_eq!(released.attempt, 2);
    assert_eq!(
        serde_json::to_value(released.not_rerun).unwrap(),
        json!({ "cause": "retries_exhausted", "max_attempts": 2 })
    );
    let preempted = events(&origin, job)
        .iter()
        .filter(|event| event.event == JobEventKind::JobPreempted)
        .count();
    assert_eq!(preempted, 1);
    // the pool asks for no machine for a job that ended
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(fake.leases().len(), 2);
}

#[test]
fn a_rerun_that_would_outlast_the_pool_deadline_fails_instead() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    // a long grace, so the pool's boot timeout bounds a new machine's join
    fake.set_boot_grace(Duration::from_secs(600));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    let created = std::time::Instant::now();
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "late",
            "--machine-class",
            "aws-c8a-4xl",
            "--max",
            "1",
            "--deadline",
            "20m",
            "--boot-timeout",
            "150s",
        ],
    );
    // a new machine joins within 150 s, so a 17 minute job fits one for the
    // first 30 s; the running machine still fits it for 3 minutes
    let starts = origin.user_home.join("starts");
    let job = run_with_kill(&origin, thread, "late", "17m", &always_works(&starts));

    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || lines(&starts) == 1),
        "the job never started: {}",
        pool(&origin, thread, "late")
    );
    while created.elapsed() < Duration::from_secs(40) {
        std::thread::sleep(Duration::from_millis(200));
    }
    let lease = live_lease(&fake).unwrap();
    fake.end_lease(&lease.lease_id, "interrupted");

    assert!(
        machines.drive(&fake, Duration::from_secs(60), || final_event(&origin, job)
            .is_some()),
        "the job never ended: {}",
        pool(&origin, thread, "late")
    );
    let event = final_event(&origin, job).unwrap();
    assert_eq!(event.event, JobEventKind::JobFailed);
    let released = event.machine_released.unwrap();
    assert_eq!(released.attempt, 1);
    assert_eq!(
        serde_json::to_value(released.not_rerun).unwrap(),
        json!({ "cause": "out_of_reach" })
    );
    assert_eq!(fake.leases().len(), 1);
}

#[test]
fn a_rerun_the_spend_cap_refuses_a_machine_for_fails_with_the_pool() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    create_pool(&origin, thread, "capped", &["--max", "1"]);
    let starts = origin.user_home.join("starts");
    let job = run(&origin, thread, "capped", &always_works(&starts));

    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || lines(&starts) == 1),
        "the job never started: {}",
        pool(&origin, thread, "capped")
    );
    // the first machine spent what the run may, so cloudlease refuses the
    // replacement's create
    fake.refuse_creates(429, "spend_cap_reached");
    let lease = live_lease(&fake).unwrap();
    fake.end_lease(&lease.lease_id, "interrupted");

    assert!(
        machines.drive(&fake, Duration::from_secs(60), || final_event(&origin, job)
            .is_some()),
        "the job never ended: {}",
        pool(&origin, thread, "capped")
    );
    let event = final_event(&origin, job).unwrap();
    assert_eq!(event.event, JobEventKind::JobFailed);
    assert_eq!(held_code(&event), "spend_cap_reached");
    let kinds: Vec<JobEventKind> = events(&origin, job)
        .iter()
        .map(|event| event.event)
        .collect();
    assert_eq!(
        kinds,
        [JobEventKind::JobPreempted, JobEventKind::JobFailed],
        "the job was queued again, then ended when the pool stopped"
    );
    assert_eq!(fake.leases().len(), 1);
    // with no machine left, the stopped pool winds down
    let capped = pool(&origin, thread, "capped");
    assert_eq!(capped["state"], "released");
    assert_eq!(
        (&capped["reason"], &capped["code"]),
        (&json!("refused"), &json!("spend_cap_reached"))
    );
}

#[test]
fn a_job_cancelled_before_its_machine_is_interrupted_is_not_run_again() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    create_pool(&origin, thread, "cancel", &["--max", "2"]);
    let starts = origin.user_home.join("starts");
    // the run takes its time to stop, so the interruption lands mid-cancel
    let script = format!(
        "echo run >> {}; trap 'sleep 30; exit 0' TERM; while :; do sleep 1; done",
        starts.display()
    );
    let job = run(&origin, thread, "cancel", &script);

    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || lines(&starts) == 1),
        "the job never started: {}",
        pool(&origin, thread, "cancel")
    );
    let job_text = job.to_string();
    cli(&origin, thread, &["resource", "job", "cancel", &job_text]);
    let first = live_lease(&fake).unwrap();
    fake.end_lease(&first.lease_id, "interrupted");

    assert!(
        machines.drive(&fake, Duration::from_secs(60), || final_event(&origin, job)
            .is_some()),
        "the cancelled job never ended: {}",
        pool(&origin, thread, "cancel")
    );
    let kinds: Vec<JobEventKind> = events(&origin, job)
        .iter()
        .map(|event| event.event)
        .collect();
    assert_eq!(kinds, [JobEventKind::JobFailed]);
    let released = final_event(&origin, job).unwrap().machine_released.unwrap();
    assert_eq!(
        serde_json::to_value(released.not_rerun).unwrap(),
        json!({ "cause": "cancelled" })
    );
    assert_eq!(lines(&starts), 1);
}
