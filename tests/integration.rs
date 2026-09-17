#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tempfile::TempDir;

const THREAD: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e431";

struct Harness {
    _dir: TempDir,
    home: PathBuf,
    record: PathBuf,
    hb: PathBuf,
    path: String,
    daemon: Option<Child>,
}

impl Harness {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let home = dir.path().join("state");
        let record = dir.path().join("record");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&record).unwrap();
        let hb = assert_cmd::cargo::cargo_bin("homebased");
        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let path = format!(
            "{}:{}:{}",
            fixtures.display(),
            hb.parent().unwrap().display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut h = Self {
            _dir: dir,
            home,
            record,
            hb,
            path,
            daemon: None,
        };
        h.start_daemon();
        h
    }

    fn cmd(&self) -> Command {
        let mut cmd = Command::new(&self.hb);
        cmd.env("PATH", &self.path)
            .env("HOMEBASED_HOME", &self.home)
            .env("HOMEBASED_CODEX", fixture("fake-codex"))
            .env("HOMEBASED_CLAUDE", fixture("fake-claude"))
            .env("HOMEBASED_GROK", fixture("fake-grok"))
            .env("FAKE_RECORD_DIR", &self.record)
            .env(
                "HOME",
                std::env::var("HOME").unwrap_or_else(|_| "/tmp".into()),
            )
            .current_dir(std::env::temp_dir());
        cmd
    }

    fn start_daemon(&mut self) {
        let child = self
            .cmd()
            .args(["daemon", "serve", "--home"])
            .arg(&self.home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        self.daemon = Some(child);
        assert!(
            wait_until(Duration::from_secs(5), || self
                .home
                .join("homebased.sock")
                .exists()),
            "socket did not appear"
        );
    }

    fn restart_daemon(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let sock = self.home.join("homebased.sock");
        let _ = fs::remove_file(&sock);
        self.start_daemon();
    }

    fn submit(&self, spec: &Value) -> String {
        let spec_path = self.home.join("spec.json");
        fs::write(&spec_path, serde_json::to_vec(spec).unwrap()).unwrap();
        let out = self
            .cmd()
            .args(["--json", "task", "submit", "--spec"])
            .arg(&spec_path)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "submit failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let v: Value = serde_json::from_slice(&out.stdout).unwrap();
        v["id"].as_str().unwrap().to_string()
    }

    fn show(&self, id: &str) -> Value {
        let out = self
            .cmd()
            .args(["--json", "task", "show", id])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "show failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).unwrap()
    }

    fn wait_status(&self, id: &str, want: &str) -> Value {
        let ok = wait_until(Duration::from_secs(20), || {
            let out = self
                .cmd()
                .args(["--json", "task", "show", id])
                .output()
                .ok();
            out.and_then(|o| serde_json::from_slice::<Value>(&o.stdout).ok())
                .and_then(|v| v["status"].as_str().map(|s| s == want))
                .unwrap_or(false)
        });
        assert!(ok, "task {id} did not reach {want}");
        self.show(id)
    }

    fn queue_messages(&self) -> Vec<String> {
        let path = self.record.join("queue-messages.txt");
        if !path.exists() {
            return Vec::new();
        }
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_string)
            .filter(|l| !l.is_empty())
            .collect()
    }

    fn spec(agent: &str, prompt: &str) -> Value {
        json!({
            "api_version": 1,
            "agent": agent,
            "model": "fable",
            "thread": THREAD,
            "cwd": std::env::temp_dir(),
            "prompt": prompt,
            "timeout": "15s"
        })
    }

    fn set_control(&self, name: &str, body: &str) {
        fs::write(self.record.join(name), body).unwrap();
    }

    fn clear_controls(&self) {
        for name in [
            "sleep",
            "report",
            "report2",
            "notify",
            "exit",
            "queue-fails",
        ] {
            let _ = fs::remove_file(self.record.join(name));
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn wait_until(budget: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < budget {
        if pred() {
            return true;
        }
        thread::sleep(Duration::from_millis(40));
    }
    pred()
}

fn event_json(line: &str) -> Value {
    let json = line.strip_prefix("HOMEBASED_EVENT ").unwrap_or(line);
    serde_json::from_str(json).unwrap()
}

#[test]
fn submit_then_fake_codex_receives_event() {
    let h = Harness::new();
    let id = h.submit(&Harness::spec("claude", "do the work"));
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(show["thread"], THREAD);
    let msgs = h.queue_messages();
    assert_eq!(msgs.len(), 1, "{msgs:?}");
    let ev = event_json(&msgs[0]);
    assert_eq!(ev["event"], "TASK_SUCCEEDED");
    assert_eq!(ev["task"], id);
    assert_eq!(ev["thread"], THREAD);
    assert!(ev["reports"].as_array().unwrap().is_empty());
    assert_eq!(ev["process"]["kind"], "exit");
    assert_eq!(ev["process"]["code"], 0);
    let meta = fs::read_to_string(h.record.join("agent-meta.txt")).unwrap();
    assert!(meta.contains("HOMEBASED_TASK_ID="), "{meta}");
    assert!(meta.contains(&format!("HOMEBASED_TASK_ID={id}")), "{meta}");
    assert!(meta.contains("PATH="), "{meta}");
    assert!(meta.contains("fake"), "{meta}");
}

#[test]
fn daemon_restart_keeps_worker() {
    let mut h = Harness::new();
    let mut spec = Harness::spec("claude", "sleeping");
    spec["timeout"] = json!("20s");
    h.set_control("sleep", "8");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || {
        h.show(&id)["status"] == "running"
    }));
    let pid = h.show(&id)["pid"].as_i64().unwrap() as u32;
    assert!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok(),
        "worker pid {pid} should be alive"
    );
    h.restart_daemon();
    assert!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok(),
        "worker should survive serve restart"
    );
    h.wait_status(&id, "succeeded");
    let msgs = h.queue_messages();
    assert_eq!(msgs.len(), 1, "{msgs:?}");
}

#[test]
fn kill9_worker_marks_lost() {
    let h = Harness::new();
    let mut spec = Harness::spec("claude", "hold");
    spec["timeout"] = json!("30s");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "20");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let pid = h.show(&id)["pid"].as_i64().unwrap() as i32;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    h.wait_status(&id, "lost");
    let msgs = h.queue_messages();
    assert_eq!(msgs.len(), 1, "{msgs:?}");
    let ev = event_json(&msgs[0]);
    assert_eq!(ev["event"], "TASK_LOST");
    assert_eq!(ev["process"]["kind"], "runner_lost");
    #[cfg(target_os = "linux")]
    {
        let _ = pid;
        let leftover = wait_until(Duration::from_secs(2), || {
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err()
        });
        assert!(leftover, "worker group should be gone");
    }
}

#[test]
fn timeout_and_cancel() {
    let h = Harness::new();
    let mut spec = Harness::spec("claude", "timeout me");
    spec["timeout"] = json!("1s");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "20");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let show = h.wait_status(&id, "failed");
    assert_eq!(show["exit_reason"]["kind"], "timeout");
    assert_eq!(show["exit_reason"]["secs"], 1);
    let ev = event_json(&h.queue_messages()[0]);
    assert_eq!(ev["event"], "TASK_FAILED");
    assert_eq!(ev["process"]["kind"], "timeout");

    let spec_path = h.home.join("spec2.json");
    let mut spec = Harness::spec("claude", "cancel me");
    spec["timeout"] = json!("30s");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "20");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let pid = h.show(&id)["pid"].as_i64().unwrap() as i32;
    let out = h.cmd().args(["task", "cancel", &id]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    h.wait_status(&id, "cancelled");
    let msgs = h.queue_messages();
    let last = event_json(msgs.last().unwrap());
    assert_eq!(last["event"], "TASK_CANCELLED");
    let gone = wait_until(Duration::from_secs(5), || {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err()
    });
    assert!(gone, "cancelled group still alive");
}

#[test]
fn report_variants() {
    let h = Harness::new();

    let id = h.submit(&Harness::spec("claude", "no report"));
    h.wait_status(&id, "succeeded");
    let ev = event_json(&h.queue_messages()[0]);
    assert_eq!(ev["reports"], json!([]));

    let spec_path = h.home.join("spec-r.json");
    let spec = Harness::spec("claude", "reports");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("report", "blocked\nNeed the DB name.");
    h.set_control("report2", "succeeded\nFound it.");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(show["reports"].as_array().unwrap().len(), 2);
    let ev = event_json(h.queue_messages().last().unwrap());
    assert_eq!(ev["event"], "TASK_SUCCEEDED");
    assert_eq!(ev["reports"].as_array().unwrap().len(), 2);

    let spec_path = h.home.join("spec-n.json");
    fs::write(
        &spec_path,
        serde_json::to_vec(&Harness::spec("claude", "notify")).unwrap(),
    )
    .unwrap();
    h.clear_controls();
    h.set_control("report", "blocked\nblocked now");
    h.set_control("notify", "");
    let before = h.queue_messages().len();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    h.wait_status(&id, "succeeded");
    let msgs = h.queue_messages();
    let new: Vec<_> = msgs.iter().skip(before).cloned().collect();
    assert_eq!(new.len(), 2, "{new:?}");
    let first = event_json(&new[0]);
    let second = event_json(&new[1]);
    assert_eq!(first["event"], "TASK_REPORTED");
    assert!(first["process"].is_null());
    assert_eq!(second["event"], "TASK_BLOCKED");

    let terminal_id = id.clone();
    let out = h
        .cmd()
        .args([
            "--json",
            "task",
            "report",
            "--id",
            &terminal_id,
            "--outcome",
            "succeeded",
            "--summary",
            "late",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(5));

    let stdin = fs::read_to_string(h.record.join("stdin.txt")).unwrap();
    assert!(stdin.contains("--- homebased ---"), "{stdin}");
    assert!(
        stdin.contains("do the work")
            || stdin.contains("notify")
            || stdin.contains("reports")
            || stdin.contains("no report")
    );
}

#[test]
fn report_with_daemon_stopped_and_trailer_off() {
    let mut h = Harness::new();
    let mut spec = Harness::spec("claude", "keep running");
    spec["timeout"] = json!("30s");
    spec["report_trailer"] = json!(false);
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "8");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    if let Some(mut child) = h.daemon.take() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = fs::remove_file(h.home.join("homebased.sock"));
    let out = h
        .cmd()
        .args([
            "--json",
            "task",
            "report",
            "--id",
            &id,
            "--outcome",
            "succeeded",
            "--summary",
            "from worker while daemon down",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let feed = h.home.join("tasks").join(&id).join("prompt.txt");
    let prompt = fs::read_to_string(feed).unwrap();
    assert_eq!(prompt, "keep running");
}

#[test]
fn stop_refusal_and_yes() {
    let h = Harness::new();
    let mut spec = Harness::spec("claude", "hold");
    spec["timeout"] = json!("30s");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "20");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let pid = h.show(&id)["pid"].as_i64().unwrap() as i32;
    let out = h.cmd().args(["daemon", "stop"]).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(5),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok());
    let out = h.cmd().args(["daemon", "stop", "--yes"]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!h.home.join("homebased.sock").exists());
    let msgs = h.queue_messages();
    assert!(!msgs.is_empty());
}

#[test]
fn callback_failure_fallback() {
    let h = Harness::new();
    let spec_path = h.home.join("spec.json");
    fs::write(
        &spec_path,
        serde_json::to_vec(&Harness::spec("claude", "fail queue")).unwrap(),
    )
    .unwrap();
    h.set_control("queue-fails", "3");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    h.wait_status(&id, "succeeded");
    assert!(
        wait_until(Duration::from_secs(10), || {
            h.show(&id)["callback"] == "failed"
        }),
        "callback={}",
        h.show(&id)["callback"]
    );
    let show = h.show(&id);
    assert_eq!(show["callback"], "failed");
    let fallback = fs::read_to_string(h.home.join("callback-fallback.log")).unwrap();
    assert!(fallback.contains("HOMEBASED_EVENT"), "{fallback}");
}

#[test]
fn summary_too_long() {
    let h = Harness::new();
    let id = h.submit(&Harness::spec("claude", "x"));
    h.wait_status(&id, "succeeded");
    // submit a running task to report against
    let mut spec = Harness::spec("claude", "running");
    spec["timeout"] = json!("30s");
    let spec_path = h.home.join("spec2.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "15");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let long = "x".repeat(4097);
    let out = h
        .cmd()
        .args([
            "--json",
            "task",
            "report",
            "--id",
            &id,
            "--outcome",
            "succeeded",
            "--summary",
            &long,
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn grok_gets_feed_file() {
    let h = Harness::new();
    let id = h.submit(&Harness::spec("grok", "grok prompt"));
    h.wait_status(&id, "succeeded");
    let feed = fs::read_to_string(h.record.join("prompt-file.txt")).unwrap();
    assert!(feed.contains("grok prompt"), "{feed}");
    assert!(feed.contains("--- homebased ---"), "{feed}");
}

#[test]
fn submit_dry_run_and_schema() {
    let h = Harness::new();
    let spec = Harness::spec("claude", "preview");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--dry-run", "--spec", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // write stdin
    let mut child = out;
    {
        use std::io::Write;
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(serde_json::to_vec(&spec).unwrap().as_slice())
            .unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(v.get("argv").is_some(), "{v}");
    assert!(v["argv"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a.as_str() == Some("-p")));

    let out = h.cmd().args(["task", "schema"]).output().unwrap();
    assert!(out.status.success());
    let schema: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(schema.is_object());
}

#[test]
fn help_lists_tree_and_exit_codes() {
    let hb = assert_cmd::cargo::cargo_bin("homebased");
    for args in [
        vec!["--help"],
        vec!["task", "--help"],
        vec!["daemon", "--help"],
    ] {
        let out = Command::new(&hb).args(&args).output().unwrap();
        assert!(out.status.success());
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(text.contains("Exit codes"), "{text}");
        if args.len() == 1 {
            assert!(text.contains("daemon"), "{text}");
            assert!(text.contains("task"), "{text}");
        }
        if args[0] == "task" {
            assert!(text.contains("submit"), "{text}");
        }
        if args[0] == "daemon" {
            assert!(text.contains("install"), "{text}");
            assert!(text.contains("stop"), "{text}");
        }
    }
    let report_help = Command::new(&hb)
        .args(["task", "report", "--help"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&report_help.stdout);
    assert!(text.contains("succeeded"), "{text}");
    assert!(text.contains("failed"), "{text}");
    assert!(text.contains("blocked"), "{text}");
    let list_help = Command::new(&hb)
        .args(["task", "list", "--help"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&list_help.stdout);
    assert!(text.contains("queued"), "{text}");
    assert!(text.contains("running"), "{text}");
}

#[test]
fn install_dry_run_text() {
    let hb = assert_cmd::cargo::cargo_bin("homebased");
    let dir = TempDir::new().unwrap();
    let out = Command::new(&hb)
        .args(["daemon", "install", "--dry-run", "--home"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("KillMode=process"), "{text}");
    assert!(!text.contains("ExecStop"), "{text}");
    assert!(text.contains("ExecStart="), "{text}");
    assert!(text.contains("Environment=PATH="), "{text}");
}

#[test]
fn status_responds_while_callback_hangs() {
    let h = Harness::new();
    h.set_control("queue-sleep", "5");
    let id = h.submit(&Harness::spec("claude", "fast"));
    thread::sleep(Duration::from_millis(100));
    let start = Instant::now();
    let list = h.cmd().args(["--json", "task", "list"]).output().unwrap();
    let list_elapsed = start.elapsed();
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(
        list_elapsed < Duration::from_millis(500),
        "task list took {list_elapsed:?}"
    );
    let start = Instant::now();
    let status = h
        .cmd()
        .args(["--json", "daemon", "status"])
        .output()
        .unwrap();
    let status_elapsed = start.elapsed();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(
        status_elapsed < Duration::from_millis(500),
        "daemon status took {status_elapsed:?}"
    );
    let _ = id;
}

#[test]
fn sigterm_without_cancel_is_failed() {
    let h = Harness::new();
    let mut spec = Harness::spec("claude", "hold");
    spec["timeout"] = json!("30s");
    h.set_control("sleep", "20");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let pid = h.show(&id)["pid"].as_i64().unwrap() as i32;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    let show = h.wait_status(&id, "failed");
    assert_eq!(show["exit_reason"]["kind"], "signal");
    assert_eq!(show["exit_reason"]["signal"], 15);
    assert!(show["cancel_requested_at"].is_null(), "{show}");
    let ev = event_json(h.queue_messages().last().unwrap());
    assert_eq!(ev["event"], "TASK_FAILED");
}

#[test]
fn cancel_immediately_after_submit() {
    let h = Harness::new();
    let mut spec = Harness::spec("claude", "hold");
    spec["timeout"] = json!("30s");
    h.set_control("sleep", "20");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    for _ in 0..20 {
        let _ = h.cmd().args(["task", "cancel", &id]).output();
    }
    let show = h.wait_status(&id, "cancelled");
    assert_eq!(show["status"], "cancelled");
    let msgs = h.queue_messages();
    assert!(
        msgs.iter()
            .any(|m| event_json(m)["event"] == "TASK_CANCELLED"),
        "{msgs:?}"
    );
    assert!(
        msgs.iter().all(|m| event_json(m)["event"] != "TASK_LOST"),
        "{msgs:?}"
    );
}

#[test]
fn large_prompt_early_exit_keeps_code() {
    let h = Harness::new();
    h.set_control("no-stdin", "");
    h.set_control("exit", "3");
    let mut spec = Harness::spec("claude", &"x".repeat(1024 * 1024));
    spec["timeout"] = json!("15s");
    let spec_path = h.home.join("spec-big.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let show = h.wait_status(&id, "failed");
    assert_eq!(show["exit_reason"]["kind"], "exit");
    assert_eq!(show["exit_reason"]["code"], 3);
    let ev = event_json(h.queue_messages().last().unwrap());
    assert_eq!(ev["event"], "TASK_FAILED");
    assert_ne!(ev["process"]["kind"], "spawn_failed");
}

#[test]
fn stuck_agent_is_killed_after_grace() {
    let h = Harness::new();
    h.set_control("ignore-term", "");
    h.set_control("sleep", "30");
    let mut spec = Harness::spec("claude", "trap");
    spec["timeout"] = json!("1s");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let start = Instant::now();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let show = h.wait_status(&id, "failed");
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "took {:?}",
        start.elapsed()
    );
    assert_eq!(show["exit_reason"]["kind"], "timeout");
    if let Some(pid) = show["pid"].as_i64() {
        let gone = wait_until(Duration::from_secs(2), || {
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_err()
        });
        assert!(gone, "agent/worker still alive");
    }
}

#[test]
fn counter_evidence_scan() {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let systemd = src.join("install/systemd.rs");
    let execstop: Vec<_> = fs::read_to_string(&systemd)
        .unwrap()
        .lines()
        .enumerate()
        .filter(|(_, line)| line.trim_start().starts_with("ExecStop="))
        .map(|(i, line)| format!("{}:{}:{line}", systemd.display(), i + 1))
        .collect();
    assert!(execstop.is_empty(), "ExecStop= in systemd.rs: {execstop:?}");
    let cli = src.join("cli");
    let flags = grep_src(&cli, r#"arg\("--prompt"|arg\("--agent""#);
    assert!(flags.is_empty(), "per-field submit flags: {flags:?}");
    let report = src.join("report.rs");
    let http = grep_src(&report, r"UnixStream|/v1/");
    assert!(http.is_empty(), "report talks HTTP: {http:?}");

    let daemon = src.join("daemon");
    let mutex = grep_src(&daemon, "Mutex");
    assert!(mutex.is_empty(), "Mutex in daemon: {mutex:?}");
    let rwlock = grep_src(&daemon, "RwLock");
    assert!(rwlock.is_empty(), "RwLock in daemon: {rwlock:?}");
    let sleep = grep_daemon_except_callback(&daemon, "thread::sleep");
    assert!(sleep.is_empty(), "thread::sleep in daemon: {sleep:?}");
    let cmd = grep_daemon_except_callback(&daemon, "Command::new");
    assert!(cmd.is_empty(), "Command::new in daemon: {cmd:?}");
    let open = grep_daemon_except_store(&daemon, "Store::open");
    assert!(open.is_empty(), "Store::open in daemon: {open:?}");
    let conn = grep_daemon_except_store(&daemon, "Connection::open");
    assert!(conn.is_empty(), "Connection::open in daemon: {conn:?}");
}

fn grep_daemon_except_callback(daemon: &Path, pattern: &str) -> Vec<String> {
    grep_src(daemon, pattern)
        .into_iter()
        .filter(|line| !line.contains("/actors/callback.rs:"))
        .collect()
}

fn grep_daemon_except_store(daemon: &Path, pattern: &str) -> Vec<String> {
    grep_src(daemon, pattern)
        .into_iter()
        .filter(|line| !line.contains("/actors/store.rs:"))
        .collect()
}

fn grep_src(path: &Path, pattern: &str) -> Vec<String> {
    let mut matches = Vec::new();
    let files = if path.is_file() {
        vec![path.to_path_buf()]
    } else {
        walkdir_files(path)
    };
    let re = regex_lite(pattern);
    for file in files {
        if file.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        if let Ok(text) = fs::read_to_string(&file) {
            for (i, line) in text.lines().enumerate() {
                if re(line) {
                    matches.push(format!("{}:{}:{line}", file.display(), i + 1));
                }
            }
        }
    }
    matches
}

fn walkdir_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn rec(dir: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(rd) = fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    rec(&p, out);
                } else {
                    out.push(p);
                }
            }
        }
    }
    rec(root, &mut out);
    out
}

fn regex_lite(pattern: &str) -> impl Fn(&str) -> bool {
    let pattern = pattern.to_string();
    move |line: &str| {
        if pattern.contains('|') && pattern.contains("arg(") {
            return line.contains("arg(\"--prompt") || line.contains("arg(\"--agent");
        }
        if pattern.contains('|') {
            return pattern.split('|').any(|p| line.contains(p));
        }
        line.contains(&pattern)
    }
}
