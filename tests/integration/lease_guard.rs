//! The GPU lease guard end to end: a fake `nvidia-smi` lists the compute
//! processes a step writes to its `apps` file, and second-scale thresholds
//! keep each case short

use super::resource_jobs::{Jobs, callbacks, spec};
use super::{Harness, wait_until};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use homebased::queue::JobId;
use serde_json::Value;

const GPU0: &str = "GPU-11111111-2222-3333-4444-555555555555";

/// Write an `nvidia-smi` that lists gpu0 and reports as compute processes
/// the lines of the `apps` file beside it
fn fake_nvidia_smi(dir: &Path) -> PathBuf {
    let bin = dir.join("fake-gpu");
    fs::create_dir_all(&bin).unwrap();
    let script = format!(
        r#"#!/bin/sh
here=$(dirname "$0")
case "$*" in
  -L) echo "GPU 0: Fake GPU (UUID: {GPU0})" ;;
  *compute-apps*) cat "$here/apps" 2>/dev/null; exit 0 ;;
  *index,uuid,utilization*) echo "0, {GPU0}, 0, 40.00, 500.00, 2048, 32768" ;;
  *uuid,enforced.power.limit*) echo "{GPU0}, 500.00" ;;
  *) exit 2 ;;
esac
"#
    );
    let path = bin.join("nvidia-smi");
    fs::write(&path, script).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    bin
}

fn restart_with_guard(h: &mut Harness, bin: &Path) {
    let config = h.dir.path().join("config.toml");
    fs::write(
        &config,
        "[resource.gpu_idle]\nsource = \"off\"\n\
         [resource.lease_guard]\nidle_after = \"3s\"\ninterval = \"1s\"\nyield_grace = \"3s\"\n",
    )
    .unwrap();
    let path = format!("{}:{}", bin.display(), h.path);
    h.restart_daemon_with_env(&[
        ("PATH", path.as_str()),
        ("HOMEBASED_CONFIG", config.to_str().unwrap()),
    ]);
    // a run records its GPU's UUID only once detection listed it
    assert!(wait_until(Duration::from_secs(20), || {
        json_cli(h, &["resource", "list"])
            .to_string()
            .contains(GPU0)
    }));
}

fn json_cli(h: &Harness, args: &[&str]) -> Value {
    let output = h.cmd().arg("--json").args(args).output().unwrap();
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

/// The job's delivered callbacks of kind `event`, once there are `count`
fn wait_events(h: &Harness, job: JobId, event: &str, count: usize) -> Vec<Value> {
    let found = || -> Vec<Value> {
        callbacks(h, job)
            .into_iter()
            .filter(|callback| callback["event"] == event)
            .collect()
    };
    assert!(
        wait_until(Duration::from_secs(40), || found().len() >= count),
        "{count} {event} for {job}: {:?}; show: {}",
        callbacks(h, job),
        show(h, job)
    );
    found()
}

#[test]
fn the_guard_releases_leases_with_no_cuda_context_by_preemption_mode() {
    let mut h = Harness::new();
    let bin = fake_nvidia_smi(h.dir.path());
    let apps = bin.join("apps");
    restart_with_guard(&mut h, &bin);
    let mut jobs = Jobs::new(&h);

    // a wait job that holds a context longer than the threshold is left
    // alone; once it drops the context, the guard cancels it
    let holds = format!(
        "echo \"{GPU0}, $$\" > {apps}; sleep 5; : > {apps}; touch dropped; sleep 60",
        apps = apps.display()
    );
    let job = jobs.submit(&spec(&h, "low", "wait", &holds));
    let cancelled = wait_events(&h, job, "JOB_CANCELLED", 1).remove(0);
    assert!(
        h.home.join("dropped").exists(),
        "tripped while the run held a context: {cancelled}"
    );
    let trip = &cancelled["lease_idle"];
    assert_eq!(trip["action"], "cancel", "{cancelled}");
    assert_eq!(trip["gpu_uuid"], GPU0);
    assert_eq!(trip["trip"], 1);
    assert!(
        trip["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("CUDA context") && reason.contains("--after")),
        "{trip}"
    );
    assert_eq!(show(&h, job)["lease_idle"][0]["action"], "cancel");

    // a yield job that honors the yield queues again, and its second trip
    // cancels it, so CPU work in the lease cannot loop
    let honors = "if [ \"$HOMEBASED_RESUME\" = 1 ]; then sleep 60; exit 0; fi; \
                  while [ ! -f \"$HOMEBASED_YIELD_FILE\" ]; do sleep 0.1; done; exit 75";
    let job = jobs.submit(&spec(&h, "low", "yield", honors));
    let preempted = wait_events(&h, job, "JOB_PREEMPTED", 1).remove(0);
    assert_eq!(preempted["lease_idle"]["action"], "yield", "{preempted}");
    let cancelled = wait_events(&h, job, "JOB_CANCELLED", 1).remove(0);
    assert_eq!(cancelled["lease_idle"]["action"], "cancel", "{cancelled}");
    assert_eq!(cancelled["lease_idle"]["trip"], 2);

    // a job labelled yield that never reads the yield file shows the pending
    // request in `resource jobs`, then is cancelled after the grace
    let job = jobs.submit(&spec(&h, "low", "yield", "sleep 60"));
    assert!(
        wait_until(Duration::from_secs(20), || {
            json_cli(&h, &["resource", "jobs"])["lease_notes"][job.to_string()]["yield_requested_at"]
                .is_string()
        }),
        "no pending yield note: {}",
        show(&h, job)
    );
    let cancelled = wait_events(&h, job, "JOB_CANCELLED", 1).remove(0);
    let trip = &cancelled["lease_idle"];
    assert_eq!(trip["action"], "yield", "{cancelled}");
    assert!(trip["escalated_at"].is_string(), "{cancelled}");
    assert!(
        trip["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("ignored the yield")),
        "{trip}"
    );
}
