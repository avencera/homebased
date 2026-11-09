//! GPU idle alerts end to end: a fake Netdata agent and a fake `nvidia-smi`
//! stand in for the GPU, and second-scale thresholds keep each case short

use super::resource_jobs::{Jobs, callbacks, spec};
use super::{Harness, wait_until};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use homebased::domain::TaskEnv;
use homebased::machine::MachineId;
use homebased::queue::spec::JobSpec;
use homebased::queue::{JobEventKind, JobId};
use homebased::store::queue::NewJob;
use serde_json::{Value, json};

const GPU0: &str = "GPU-11111111-2222-3333-4444-555555555555";
const GPU1: &str = "GPU-66666666-7777-8888-9999-aaaaaaaaaaaa";

/// Run until the test creates `finish`, for at most 60 seconds
const RUN_UNTIL_FINISH: &str = "echo ready > ready; n=0; while [ ! -f finish ] && [ $n -lt 600 ]; do sleep 0.1; n=$((n+1)); done; exit 0";

/// Write an `nvidia-smi` that lists two GPUs and reports gpu0's utilization
/// from the `util` file beside it, or `[N/A]` without one
fn fake_nvidia_smi(dir: &Path) -> PathBuf {
    let bin = dir.join("fake-gpu");
    fs::create_dir_all(&bin).unwrap();
    let script = format!(
        r#"#!/bin/sh
here=$(dirname "$0")
case "$*" in
  -L)
    echo "GPU 0: Fake GPU (UUID: {GPU0})"
    echo "GPU 1: Fake GPU (UUID: {GPU1})"
    ;;
  *index,uuid,utilization*)
    util=$(cat "$here/util" 2>/dev/null || echo "[N/A]")
    echo "0, {GPU0}, $util, 40.00, 500.00, 2048, 32768"
    echo "1, {GPU1}, 0, 30.00, 500.00, 1024, 32768"
    ;;
  *uuid,enforced.power.limit*)
    echo "{GPU0}, 500.00"
    echo "{GPU1}, 500.00"
    ;;
  *) exit 2 ;;
esac
"#
    );
    let path = bin.join("nvidia-smi");
    fs::write(&path, script).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    bin
}

/// gpu0's utilization over time: each phase holds from its start second on
#[derive(Clone, Default)]
struct Timeline(Arc<Mutex<Vec<(i64, f64)>>>);

impl Timeline {
    fn set(&self, util: f64) {
        self.0.lock().unwrap().push((Utc::now().timestamp(), util));
    }

    fn at(&self, second: i64) -> f64 {
        self.0
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(start, _)| *start <= second)
            .map_or(80.0, |(_, util)| *util)
    }
}

/// A Netdata agent with the go.d `nvidia_smi` charts of both GPUs: gpu0's
/// carry a `uuid` label, gpu1's only name the UUID in their ids
fn fake_netdata(timeline: Timeline) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let timeline = timeline.clone();
            thread::spawn(move || serve(stream, &timeline));
        }
    });
    port
}

fn chart(uuid: &str, context: &str, suffix: &str, labeled: bool) -> (String, Value) {
    let id = format!(
        "nvidia_smi_local.gpu_{}_{suffix}",
        uuid.to_ascii_lowercase()
    );
    let mut chart = json!({ "id": id, "context": context, "update_every": 1 });
    if labeled {
        chart["chart_labels"] = json!({ "uuid": uuid, "product_name": "Fake GPU" });
    }
    (id, chart)
}

fn serve(mut stream: TcpStream, timeline: &Timeline) {
    let mut request = Vec::new();
    let mut buffer = [0; 4096];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => return,
            Ok(read) => request.extend_from_slice(&buffer[..read]),
        }
    }
    let request = String::from_utf8_lossy(&request);
    let target = request.split_whitespace().nth(1).unwrap_or("/").to_owned();
    let body = if target == "/api/v1/charts" {
        let mut charts = serde_json::Map::new();
        for (uuid, labeled) in [(GPU0, true), (GPU1, false)] {
            for (context, suffix) in [
                ("nvidia_smi.gpu_utilization", "gpu_utilization"),
                ("nvidia_smi.gpu_power_draw", "power_draw"),
                (
                    "nvidia_smi.gpu_frame_buffer_memory_usage",
                    "frame_buffer_memory_usage",
                ),
            ] {
                let (id, chart) = chart(uuid, context, suffix, labeled);
                charts.insert(id, chart);
            }
        }
        json!({ "charts": charts })
    } else if let Some(query) = target.strip_prefix("/api/v1/data?") {
        data(query, timeline)
    } else {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        return;
    };
    let body = body.to_string();
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

/// One row per second in `(after, before]`, newest first like Netdata, and
/// never past the present
fn data(query: &str, timeline: &Timeline) -> Value {
    let param = |name: &str| {
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(&format!("{name}=")))
            .unwrap_or_default()
            .to_owned()
    };
    let chart = param("chart").to_ascii_lowercase();
    let after: i64 = param("after").parse().unwrap();
    let before: i64 = param("before").parse().unwrap();
    let last = before.min(Utc::now().timestamp());
    let gpu0 = chart.contains(&GPU0.to_ascii_lowercase());
    let (labels, row): (Value, Box<dyn Fn(i64) -> Value>) = if chart.contains("utilization") {
        let timeline = timeline.clone();
        (
            json!(["time", "gpu"]),
            Box::new(move |at| json!([at, if gpu0 { timeline.at(at) } else { 0.0 }])),
        )
    } else if chart.contains("power_draw") {
        (
            json!(["time", "power_draw"]),
            Box::new(|at| json!([at, 50.0])),
        )
    } else {
        let gib = 1024.0 * 1024.0 * 1024.0;
        (
            json!(["time", "free", "used", "reserved"]),
            Box::new(move |at| json!([at, 30.0 * gib, 2.0 * gib, 0.0])),
        )
    };
    let rows: Vec<Value> = ((after + 1)..=last).rev().map(row).collect();
    json!({ "labels": labels, "data": rows })
}

fn machine(h: &Harness) -> MachineId {
    fs::read_to_string(h.home.join("machine-id"))
        .unwrap()
        .parse()
        .unwrap()
}

/// Restart the daemon with the fake GPUs on `PATH` and second-scale thresholds
fn restart_with_gpus(h: &mut Harness, bin: &Path, netdata_port: u16) {
    let config = h.dir.path().join("config.toml");
    fs::write(
        &config,
        format!(
            "[resource.gpu_idle]\nnetdata_url = \"http://127.0.0.1:{netdata_port}\"\n\
             interval = \"1s\"\nafter = \"4s\"\nrecover_after = \"2s\"\nremind_after = \"3s\"\n"
        ),
    )
    .unwrap();
    let path = format!("{}:{}", bin.display(), h.path);
    h.restart_daemon_with_env(&[
        ("PATH", path.as_str()),
        ("HOMEBASED_CONFIG", config.to_str().unwrap()),
    ]);
}

fn cli(h: &Harness, args: &[&str]) -> std::process::Output {
    h.cmd().arg("--json").args(args).output().unwrap()
}

fn json_cli(h: &Harness, args: &[&str]) -> Value {
    let output = cli(h, args);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn show(h: &Harness, job: JobId) -> Value {
    json_cli(h, &["resource", "job", "show", &job.to_string()])
}

fn alerts(h: &Harness, job: JobId) -> Vec<Value> {
    callbacks(h, job)
        .into_iter()
        .filter(|event| event["event"] == "JOB_GPU_IDLE")
        .collect()
}

fn wait_alerts(h: &Harness, job: JobId, count: usize) -> Vec<Value> {
    assert!(
        wait_until(Duration::from_secs(30), || alerts(h, job).len() >= count),
        "alerts for {job}: {:?}; show: {}",
        alerts(h, job),
        show(h, job)
    );
    alerts(h, job)
}

/// The watcher's accounting of the job's only attempt
fn attempt(h: &Harness, job: JobId) -> Value {
    show(h, job)["gpu_idle"]["attempts"]
        .get(0)
        .cloned()
        .unwrap_or(Value::Null)
}

#[test]
fn netdata_idle_alerts_respect_snooze_recovery_and_old_origins() {
    let mut h = Harness::new();
    let bin = fake_nvidia_smi(h.dir.path());
    let timeline = Timeline::default();
    let port = fake_netdata(timeline.clone());
    restart_with_gpus(&mut h, &bin, port);

    // a job from an origin on cluster protocol 3 runs on gpu1 and stays idle
    h.stop_daemon();
    let store = h.store();
    let old_origin = JobId::new();
    let mut old_spec = spec(&h, "low", "wait", RUN_UNTIL_FINISH);
    old_spec["resource"] = json!("gpu1");
    store
        .submit_job(&NewJob {
            id: old_origin,
            machine: machine(&h),
            origin: MachineId::new(),
            spec: JobSpec::parse_value(&old_spec).unwrap(),
            env: TaskEnv::capture(),
            origin_protocol: homebased::fleet::protocol::ClusterProtocolVersion(3),
        })
        .unwrap();
    restart_with_gpus(&mut h, &bin, port);

    let mut jobs = Jobs::new(&h);
    let job = jobs.submit(&spec(&h, "low", "wait", RUN_UNTIL_FINISH));
    assert!(wait_until(Duration::from_secs(15), || {
        attempt(&h, job)["busy_buckets"].as_u64() >= Some(4)
    }));
    assert!(alerts(&h, job).is_empty(), "a busy GPU must not alert");

    timeline.set(0.0);
    let first = wait_alerts(&h, job, 1).remove(0);
    let notice = &first["gpu_idle"];
    assert_eq!(notice["episode"], 1, "{first}");
    assert_eq!(notice["reminder"], 0);
    assert_eq!(notice["never_busy"], false);
    assert_eq!(notice["source"], "netdata");
    assert_eq!(notice["gpu_uuid"], GPU0);
    assert_eq!(notice["util_below"], 10);
    assert_eq!(notice["util_peak_pct"], 0);
    assert_eq!(notice["power_peak_watts"], 50);
    assert_eq!(notice["power_peak_pct"], 10);
    assert_eq!(notice["memory_used_mib"], 2048);
    assert_eq!(notice["memory_total_mib"], 32768);
    assert!(notice["process"]["marked"].as_u64() >= Some(1), "{first}");
    assert!(
        notice["next_steps"]
            .as_str()
            .unwrap()
            .contains(&format!("resource job cancel {job}"))
    );

    // a silence needs a reason, and the snooze holds the reminder until it ends
    let refused = cli(
        &h,
        &[
            "resource",
            "job",
            "gpu-idle",
            &job.to_string(),
            "--snooze",
            "6s",
        ],
    );
    assert!(!refused.status.success());
    let snoozed = json_cli(
        &h,
        &[
            "resource",
            "job",
            "gpu-idle",
            &job.to_string(),
            "--snooze",
            "6s",
            "--reason",
            "checkpoint write",
        ],
    );
    assert_eq!(snoozed["silence"]["state"], "snoozed", "{snoozed}");
    let until: DateTime<Utc> = snoozed["silence"]["until"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let quiet = (until - Utc::now() - chrono::Duration::seconds(1))
        .to_std()
        .unwrap();
    assert!(!wait_until(quiet, || alerts(&h, job).len() > 1));
    let reminder = wait_alerts(&h, job, 2).remove(1);
    assert!(Utc::now() >= until);
    assert_eq!(reminder["gpu_idle"]["episode"], 1, "{reminder}");
    assert_eq!(reminder["gpu_idle"]["reminder"], 1);
    let shown = show(&h, job);
    assert_eq!(
        shown["gpu_idle"]["controls"][0]["reason"],
        "checkpoint write"
    );

    // once the GPU is busy again the episode ends and its notices are stale
    timeline.set(90.0);
    assert!(wait_until(Duration::from_secs(15), || {
        attempt(&h, job)["episode"] == 2
    }));
    let seq = reminder["seq"].as_u64().unwrap();
    assert!(!h.store().job_notice_is_current(job, seq).unwrap());

    let old = show(&h, old_origin);
    assert!(
        old["gpu_idle"]["attempts"][0]["idle_buckets"].as_u64() > Some(4),
        "{old}"
    );
    assert_eq!(old["gpu_idle"]["attempts"][0]["alerts"], 0);
    assert!(
        h.store()
            .job_events(old_origin)
            .unwrap()
            .iter()
            .all(|event| event.event != JobEventKind::JobGpuIdle)
    );
    fs::write(h.home.join("finish"), "").unwrap();
}

#[test]
fn nvidia_smi_fallback_counts_missing_readings_as_unknown_and_alerts_never_busy_runs() {
    let mut h = Harness::new();
    let bin = fake_nvidia_smi(h.dir.path());
    // nothing listens here, so the watcher falls back to nvidia-smi
    let closed = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    restart_with_gpus(&mut h, &bin, closed);

    let mut jobs = Jobs::new(&h);
    let job = jobs.submit(&spec(&h, "low", "wait", RUN_UNTIL_FINISH));
    assert!(wait_until(Duration::from_secs(20), || {
        attempt(&h, job)["unknown_buckets"].as_u64() >= Some(10)
    }));
    let counted = attempt(&h, job);
    assert_eq!(counted["source"], "nvidia_smi", "{counted}");
    assert_eq!(counted["idle_buckets"], 0);
    assert!(
        alerts(&h, job).is_empty(),
        "missing readings must never alert"
    );

    fs::write(bin.join("util"), "3").unwrap();
    let first = wait_alerts(&h, job, 1).remove(0);
    let notice = &first["gpu_idle"];
    assert_eq!(notice["never_busy"], true, "{first}");
    assert_eq!(notice["source"], "nvidia_smi");
    assert_eq!(notice["util_peak_pct"], 3);
    assert_eq!(notice["power_peak_pct"], 8);

    let off = json_cli(
        &h,
        &[
            "resource",
            "job",
            "gpu-idle",
            &job.to_string(),
            "--off",
            "--reason",
            "CPU scoring by design",
        ],
    );
    assert_eq!(off["silence"]["state"], "off", "{off}");
    let cleared = json_cli(
        &h,
        &["resource", "job", "gpu-idle", &job.to_string(), "--clear"],
    );
    assert_eq!(cleared["silence"]["state"], "watching", "{cleared}");
    assert_eq!(
        show(&h, job)["gpu_idle"]["controls"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    fs::write(h.home.join("finish"), "").unwrap();
}
