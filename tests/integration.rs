#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use homebased::domain::{
    Agent, AgentKind, AgentWorkload, CallbackStatus, ExitReason, ProcessStatus, TaskEnv, TaskId,
    Workload,
};
use homebased::store::{NewTask, Store, new_queued_task};
use serde_json::{Value, json};
use tempfile::TempDir;

const THREAD: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e431";

/// `HOMEBASED_WEB_LISTEN` for a harness daemon. Tests run in parallel, so the
/// default port would be shared; only the dashboard test binds one, on port 0.
const WEB_OFF: &str = "off";
const WEB_EPHEMERAL: &str = "127.0.0.1:0";

struct Harness {
    dir: TempDir,
    /// Private `$HOME` so unit paths never touch the developer's real home.
    user_home: PathBuf,
    home: PathBuf,
    record: PathBuf,
    hb: PathBuf,
    path: String,
    web_listen: String,
    supervisor_log: PathBuf,
    daemon: Option<Child>,
}

impl Harness {
    fn new() -> Self {
        Self::with_web_listen(WEB_OFF)
    }

    /// Harness whose daemon also serves the dashboard on a free port.
    fn with_dashboard() -> Self {
        Self::with_web_listen(WEB_EPHEMERAL)
    }

    fn with_web_listen(web_listen: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let user_home = dir.path().join("user-home");
        let home = dir.path().join("state");
        let record = dir.path().join("record");
        fs::create_dir_all(&user_home).unwrap();
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
        let supervisor_log = dir.path().join("supervisor.log");
        let mut h = Self {
            dir,
            user_home,
            home,
            record,
            hb,
            path,
            web_listen: web_listen.to_string(),
            supervisor_log,
            daemon: None,
        };
        h.start_daemon();
        h
    }

    fn cmd(&self) -> Command {
        let mut cmd = Command::new(&self.hb);
        cmd.env("PATH", &self.path)
            .env("HOMEBASED_HOME", &self.home)
            .env("HOMEBASED_WEB_LISTEN", &self.web_listen)
            .env("HOMEBASED_CODEX", fixture("fake-codex"))
            .env("HOMEBASED_CLAUDE", fixture("fake-claude"))
            .env("HOMEBASED_GROK", fixture("fake-grok"))
            .env("FAKE_RECORD_DIR", &self.record)
            .env("HOME", &self.user_home)
            .env("HARNESS_SUPERVISOR_LOG", &self.supervisor_log)
            .current_dir(std::env::temp_dir());
        if let Some(child) = &self.daemon {
            cmd.env("HARNESS_DAEMON_PID", child.id().to_string());
        }
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

    /// Pid the fake agent recorded for itself. VER-05 is about the agent
    /// process, not the worker pid the test signalled.
    fn agent_pid(&self, id: &str) -> i32 {
        let path = self.record.join(format!("agent-pid-{id}.txt"));
        assert!(
            wait_until(Duration::from_secs(10), || path.exists()),
            "agent for {id} never recorded its pid"
        );
        fs::read_to_string(&path).unwrap().trim().parse().unwrap()
    }

    /// Bytes the fake agent read on stdin for exactly this task.
    fn agent_stdin(&self, id: &str) -> String {
        fs::read_to_string(self.record.join(format!("stdin-{id}.txt"))).unwrap()
    }

    fn store(&self) -> Store {
        Store::open(&self.home.join("homebased.sqlite")).unwrap()
    }

    fn stop_daemon(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_file(self.home.join("homebased.sock"));
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
            "thread": THREAD,
            "cwd": std::env::temp_dir(),
            "timeout": "2h",
            "workload": {
                "type": "agent",
                "agent": agent,
                "model": "fable",
                "prompt": prompt,
            }
        })
    }

    fn task_spec(command: &[&str]) -> Value {
        json!({
            "api_version": 1,
            "thread": THREAD,
            "cwd": std::env::temp_dir(),
            "timeout": "2h",
            "workload": {
                "type": "task",
                "command": command,
            }
        })
    }

    fn backdate_created_at(&self, id: &str, hours_ago: i64) {
        let ts = (Utc::now() - ChronoDuration::hours(hours_ago))
            .to_rfc3339_opts(SecondsFormat::Millis, true);
        let conn = rusqlite::Connection::open(self.home.join("homebased.sqlite")).unwrap();
        let n = conn
            .execute(
                "UPDATE tasks SET created_at = ?1 WHERE id = ?2",
                rusqlite::params![ts, id],
            )
            .unwrap();
        assert_eq!(n, 1, "backdate missed task {id}");
    }

    fn output_log(&self, id: &str) -> PathBuf {
        self.home.join("tasks").join(id).join("output.log")
    }

    fn set_timeout_secs(&self, id: &str, secs: u64) {
        let conn = rusqlite::Connection::open(self.home.join("homebased.sqlite")).unwrap();
        let n = conn
            .execute(
                "UPDATE tasks SET timeout_secs = ?1 WHERE id = ?2",
                rusqlite::params![secs.to_string(), id],
            )
            .unwrap();
        assert_eq!(n, 1, "timeout update missed task {id}");
    }

    fn set_control(&self, name: &str, body: &str) {
        fs::write(self.record.join(name), body).unwrap();
    }

    fn clear_control(&self, name: &str) {
        let _ = fs::remove_file(self.record.join(name));
    }

    fn clear_controls(&self) {
        for name in [
            "sleep",
            "report",
            "report2",
            "notify",
            "exit",
            "queue-fails",
            "stdout",
            "ignore-term",
            "no-stdin",
        ] {
            let _ = fs::remove_file(self.record.join(name));
        }
    }

    /// Put a fake `systemctl`/`launchctl` ahead of PATH and clear its log.
    fn install_fake_supervisor(&mut self) {
        let bin = self.dir.path().join("fake-bin");
        fs::create_dir_all(&bin).unwrap();
        let name = if cfg!(target_os = "macos") {
            "launchctl"
        } else {
            "systemctl"
        };
        let dest = bin.join(name);
        let _ = fs::remove_file(&dest);
        std::os::unix::fs::symlink(fixture(&format!("fake-{name}")), &dest).unwrap();
        self.path = format!("{}:{}", bin.display(), self.path);
        let _ = fs::remove_file(&self.supervisor_log);
    }

    fn supervisor_calls(&self) -> Vec<String> {
        if !self.supervisor_log.exists() {
            return Vec::new();
        }
        fs::read_to_string(&self.supervisor_log)
            .unwrap()
            .lines()
            .map(str::to_string)
            .filter(|line| !line.is_empty())
            .collect()
    }

    fn clear_supervisor_log(&self) {
        let _ = fs::remove_file(&self.supervisor_log);
    }

    /// Write a host unit under the harness private `$HOME`.
    fn write_host_unit(&self, configured_home: &Path) {
        #[cfg(target_os = "linux")]
        {
            let path = self
                .user_home
                .join(".config/systemd/user/homebased.service");
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let text = format!(
                "[Unit]\nDescription=homebased test\n[Service]\n\
                 ExecStart=/usr/bin/homebased daemon serve --home {}\n\
                 KillMode=process\nRestart=on-failure\n\
                 [Install]\nWantedBy=default.target\n",
                configured_home.display()
            );
            fs::write(path, text).unwrap();
        }
        #[cfg(target_os = "macos")]
        {
            let path = self
                .user_home
                .join("Library/LaunchAgents/dev.praveen.homebased.plist");
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let text = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>dev.praveen.homebased</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/homebased</string>
    <string>daemon</string>
    <string>serve</string>
    <string>--home</string>
    <string>{}</string>
  </array>
</dict>
</plist>
"#,
                configured_home.display()
            );
            fs::write(path, text).unwrap();
        }
    }

    fn host_unit_path(&self) -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            self.user_home
                .join(".config/systemd/user/homebased.service")
        }
        #[cfg(target_os = "macos")]
        {
            self.user_home
                .join("Library/LaunchAgents/dev.praveen.homebased.plist")
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            self.user_home.join("homebased.service")
        }
    }

    fn socket_pid(&self) -> u64 {
        use std::os::unix::net::UnixStream;
        let mut stream = UnixStream::connect(self.home.join("homebased.sock")).unwrap();
        stream
            .write_all(b"GET /v1/status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut buf = String::new();
        stream.read_to_string(&mut buf).unwrap();
        let body = buf.split("\r\n\r\n").nth(1).expect("http body missing");
        let v: Value = serde_json::from_str(body).unwrap();
        v["pid"].as_u64().expect("status pid")
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
            let _ = fs::remove_file(self.home.join("homebased.sock"));
            return;
        }
        if self.home.join("homebased.sock").exists() {
            let _ = self.cmd().args(["daemon", "stop", "--yes"]).status();
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
    assert_eq!(
        ev["workload"],
        json!({"type": "agent", "agent": "claude", "model": "fable"})
    );
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
    let spec = Harness::spec("claude", "sleeping");
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

#[cfg(target_os = "linux")]
#[test]
fn kill9_worker_marks_lost() {
    let h = Harness::new();
    let spec = Harness::spec("claude", "hold");
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
    let agent_pid = h.agent_pid(&id);
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
    let gone = wait_until(Duration::from_secs(5), || {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(agent_pid), None).is_err()
    });
    assert!(gone, "fake agent {agent_pid} outlived the killed worker");
}

#[test]
fn attention_reminder_and_cancel() {
    let mut h = Harness::new();
    h.set_control("sleep", "12");
    // empty output.log is not activity, so a backdated created_at is overdue
    let id = h.submit(&Harness::spec("claude", "attention me"));
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let before = h.queue_messages().len();
    h.backdate_created_at(&id, 3);
    // re-arm from persisted created_at; overdue deadline fires immediately
    h.restart_daemon();
    assert!(
        wait_until(Duration::from_secs(10), || {
            h.queue_messages()
                .iter()
                .skip(before)
                .any(|m| event_json(m)["event"] == "TASK_CHECK_DUE")
        }),
        "no TASK_CHECK_DUE after backdate+restart: {:?}",
        h.queue_messages()
    );
    let check = h
        .queue_messages()
        .iter()
        .skip(before)
        .map(|m| event_json(m))
        .find(|ev| ev["event"] == "TASK_CHECK_DUE")
        .unwrap();
    assert!(check["process"].is_null(), "{check}");
    assert_eq!(check["next_action"], "inspect_task");
    assert_eq!(check["timeout_secs"], 2 * 3600);
    assert_eq!(
        check["workload"],
        json!({"type": "agent", "agent": "claude", "model": "fable"})
    );
    assert_eq!(h.show(&id)["status"], "running", "child must keep running");
    assert!(
        wait_until(Duration::from_secs(5), || h.show(&id)["check_timeout"]
            == "sent"),
        "check timeout stayed pending after TASK_CHECK_DUE: {}",
        h.show(&id)
    );
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(show["status"], "succeeded");
    let terminal = h
        .queue_messages()
        .iter()
        .skip(before)
        .map(|m| event_json(m))
        .find(|ev| ev["event"] == "TASK_SUCCEEDED")
        .expect("terminal event after check due");
    assert_eq!(terminal["task"], id);

    h.clear_controls();
    h.set_control("sleep", "20");
    let id = h.submit(&Harness::spec("claude", "cancel me"));
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let agent_pid = h.agent_pid(&id);
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
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(agent_pid), None).is_err()
    });
    assert!(gone, "cancelled agent {agent_pid} still alive");
}

#[test]
fn recent_output_defers_an_overdue_inactivity_check() {
    let mut h = Harness::new();
    h.set_control("sleep", "60");
    h.set_control("stdout", "still working\n");
    let id = h.submit(&Harness::spec("claude", "keep writing"));
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let output = h.output_log(&id);
    assert!(
        wait_until(Duration::from_secs(5), || {
            fs::metadata(&output).map(|m| m.len() != 0).unwrap_or(false)
        }),
        "agent never wrote output.log"
    );

    // created_at is already overdue, but the initial output starts a fresh
    // inactivity window when the daemon comes back
    h.stop_daemon();
    h.backdate_created_at(&id, 5);
    h.set_timeout_secs(&id, 6);
    h.start_daemon();
    thread::sleep(Duration::from_secs(2));

    // write while the timer is armed; this must move the deadline
    {
        let mut log = fs::OpenOptions::new().append(true).open(&output).unwrap();
        log.write_all(b"later\n").unwrap();
    }

    let fired_while_fresh = wait_until(Duration::from_secs(5), || {
        h.queue_messages()
            .iter()
            .any(|m| event_json(m)["event"] == "TASK_CHECK_DUE")
    });
    assert!(
        !fired_while_fresh,
        "recent non-empty output must defer TASK_CHECK_DUE: {:?}",
        h.queue_messages()
    );
    assert_eq!(h.show(&id)["status"], "running");
    assert_eq!(h.show(&id)["check_timeout"], "pending");

    assert!(
        wait_until(Duration::from_secs(15), || {
            h.queue_messages()
                .iter()
                .any(|m| event_json(m)["event"] == "TASK_CHECK_DUE")
        }),
        "no TASK_CHECK_DUE after output went stale: {:?}",
        h.queue_messages()
    );
    let check = h
        .queue_messages()
        .iter()
        .map(|m| event_json(m))
        .find(|ev| ev["event"] == "TASK_CHECK_DUE")
        .unwrap();
    assert!(check["process"].is_null(), "{check}");
    assert_eq!(check["next_action"], "inspect_task");
    assert_eq!(check["timeout_secs"], 6);
    assert_eq!(h.show(&id)["status"], "running", "child must keep running");
    assert!(
        wait_until(Duration::from_secs(5), || h.show(&id)["check_timeout"]
            == "sent"),
        "check timeout stayed pending after TASK_CHECK_DUE: {}",
        h.show(&id)
    );

    let out = h.cmd().args(["task", "cancel", &id]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    h.wait_status(&id, "cancelled");
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
            "late",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(5));

    let stdin = h.agent_stdin(&id);
    assert!(stdin.starts_with("notify"), "{stdin}");
    assert!(stdin.contains("--- homebased ---"), "{stdin}");
}

#[test]
fn report_with_daemon_stopped_and_trailer_off() {
    let mut h = Harness::new();
    let mut spec = Harness::spec("claude", "keep running");
    spec["workload"]["report_trailer"] = json!(false);
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
    h.stop_daemon();
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
    let spec = Harness::spec("claude", "hold");
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
    let last = event_json(msgs.last().unwrap());
    assert_eq!(last["event"], "TASK_CANCELLED", "{msgs:?}");
    assert_eq!(last["task"], id);
    let row = h.store().require_task(id.parse().unwrap()).unwrap();
    assert_eq!(row.callback_status, CallbackStatus::Sent);
}

#[test]
fn stop_ignores_other_home_unit() {
    let mut h = Harness::new();
    h.install_fake_supervisor();
    let other = h.dir.path().join("other-home");
    fs::create_dir_all(&other).unwrap();
    h.write_host_unit(&other);
    h.clear_supervisor_log();

    let before_pid = h.socket_pid();
    let out = h.cmd().args(["daemon", "stop", "--yes"]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!h.home.join("homebased.sock").exists());
    assert!(
        h.supervisor_calls().is_empty(),
        "other-home stop must not call the host supervisor: {:?}",
        h.supervisor_calls()
    );
    // reap before kill(0): a zombie still owned by the Child handle looks alive
    if let Some(mut child) = h.daemon.take() {
        assert_eq!(child.id() as u64, before_pid);
        let status = child.wait().unwrap();
        assert!(
            status.success() || status.code().is_some(),
            "standalone stop should have ended the selected daemon: {status}"
        );
    }
}

#[test]
fn restart_ignores_other_home_unit() {
    let mut h = Harness::new();
    h.install_fake_supervisor();
    let other = h.dir.path().join("other-home");
    fs::create_dir_all(&other).unwrap();
    h.write_host_unit(&other);
    h.clear_supervisor_log();

    let before_pid = h.socket_pid();
    let out = h.cmd().args(["daemon", "restart"]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        wait_until(Duration::from_secs(5), || h
            .home
            .join("homebased.sock")
            .exists()),
        "socket did not return after standalone restart"
    );
    let after_pid = h.socket_pid();
    assert_ne!(
        before_pid, after_pid,
        "standalone restart must spawn a new daemon"
    );
    assert!(
        h.supervisor_calls().is_empty(),
        "other-home restart must not call the host supervisor: {:?}",
        h.supervisor_calls()
    );
    if let Some(mut child) = h.daemon.take() {
        let _ = child.wait();
    }
}

#[test]
fn uninstall_refuses_other_home_unit() {
    let mut h = Harness::new();
    h.install_fake_supervisor();
    let other = h.dir.path().join("other-home");
    fs::create_dir_all(&other).unwrap();
    h.write_host_unit(&other);
    h.clear_supervisor_log();

    let before_pid = h.socket_pid();
    let unit_path = h.host_unit_path();
    let unit_before = fs::read_to_string(&unit_path).unwrap();
    let out = h
        .cmd()
        .args(["--json", "daemon", "uninstall", "--yes"])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(5),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["code"], "host_unit_home_mismatch");
    assert_eq!(
        PathBuf::from(err["error"]["input"]["selected"].as_str().unwrap()),
        h.home
    );
    assert_eq!(
        PathBuf::from(err["error"]["input"]["configured"].as_str().unwrap()),
        other
    );
    assert!(
        h.supervisor_calls().is_empty(),
        "refused uninstall must not call the host supervisor: {:?}",
        h.supervisor_calls()
    );
    assert_eq!(fs::read_to_string(&unit_path).unwrap(), unit_before);
    assert!(h.home.join("homebased.sock").exists());
    assert_eq!(h.socket_pid(), before_pid);
    assert!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(before_pid as i32), None).is_ok(),
        "selected daemon must keep running after refused uninstall"
    );
}

#[test]
fn uninstall_refuses_unrecognized_unit() {
    let mut h = Harness::new();
    h.install_fake_supervisor();
    let unit_path = h.host_unit_path();
    fs::create_dir_all(unit_path.parent().unwrap()).unwrap();
    fs::write(&unit_path, "not a host unit\n").unwrap();
    h.clear_supervisor_log();

    let before_pid = h.socket_pid();
    let out = h
        .cmd()
        .args(["--json", "daemon", "uninstall", "--yes"])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["code"], "unit_invalid");
    assert!(
        h.supervisor_calls().is_empty(),
        "{:?}",
        h.supervisor_calls()
    );
    assert_eq!(fs::read_to_string(&unit_path).unwrap(), "not a host unit\n");
    assert_eq!(h.socket_pid(), before_pid);
}

#[test]
fn matching_home_stop_uses_supervisor() {
    let mut h = Harness::new();
    h.install_fake_supervisor();
    h.write_host_unit(&h.home);
    h.clear_supervisor_log();

    let out = h.cmd().args(["daemon", "stop", "--yes"]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let calls = h.supervisor_calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    if cfg!(target_os = "linux") {
        assert_eq!(calls[0], "--user stop homebased.service");
    } else if cfg!(target_os = "macos") {
        let uid = nix::unistd::getuid().as_raw();
        assert_eq!(calls[0], format!("bootout gui/{uid}/dev.praveen.homebased"));
    }
    assert!(!h.home.join("homebased.sock").exists());
    if let Some(mut child) = h.daemon.take() {
        let _ = child.wait();
    }
}

#[test]
fn matching_home_uninstall_uses_supervisor_and_removes_unit() {
    let mut h = Harness::new();
    h.install_fake_supervisor();
    h.write_host_unit(&h.home);
    h.clear_supervisor_log();
    let unit_path = h.host_unit_path();

    let out = h
        .cmd()
        .args(["daemon", "uninstall", "--yes"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let calls = h.supervisor_calls();
    if cfg!(target_os = "linux") {
        assert_eq!(
            calls,
            [
                "--user stop homebased.service",
                "--user disable --now homebased.service",
                "--user daemon-reload",
            ]
        );
    } else if cfg!(target_os = "macos") {
        let uid = nix::unistd::getuid().as_raw();
        assert_eq!(
            calls,
            [
                format!("bootout gui/{uid}/dev.praveen.homebased"),
                format!("bootout gui/{uid} {}", unit_path.display()),
            ]
        );
    }
    assert!(!unit_path.exists());
    assert!(!h.home.join("homebased.sock").exists());
}

#[test]
fn matching_home_restart_uses_supervisor() {
    let mut h = Harness::new();
    h.install_fake_supervisor();
    h.write_host_unit(&h.home);
    h.clear_supervisor_log();

    let before_pid = h.socket_pid();
    let out = h.cmd().args(["daemon", "restart"]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let calls = h.supervisor_calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    if cfg!(target_os = "linux") {
        assert_eq!(calls[0], "--user restart homebased.service");
    } else if cfg!(target_os = "macos") {
        let uid = nix::unistd::getuid().as_raw();
        assert_eq!(
            calls[0],
            format!("kickstart -k gui/{uid}/dev.praveen.homebased")
        );
    }
    // fake restart is a no-op, so the original daemon stays up
    assert_eq!(h.socket_pid(), before_pid);
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
    let spec = Harness::spec("claude", "running");
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
    let mut child = h
        .cmd()
        .args(["--json", "task", "submit", "--dry-run", "--spec", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
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
    assert_eq!(v["stdin"], "prompt_feed");
    let argv = v["argv"].as_array().unwrap();
    assert!(argv.iter().any(|a| a.as_str() == Some("-p")));
    assert!(
        !argv
            .iter()
            .any(|a| a.as_str().is_some_and(|s| s.contains("prompt.feed.txt"))),
        "claude dry-run must not put the feed path in argv: {v}"
    );

    let grok = Harness::spec("grok", "preview");
    let mut child = h
        .cmd()
        .args(["--json", "task", "submit", "--dry-run", "--spec", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write;
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(serde_json::to_vec(&grok).unwrap().as_slice())
            .unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["stdin"], "null");
    let argv = v["argv"].as_array().unwrap();
    assert!(
        argv.iter().any(|a| a
            .as_str()
            .is_some_and(|s| s.contains("tasks/<task-id>/prompt.feed.txt"))),
        "grok dry-run must include the deterministic feed placeholder: {v}"
    );

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
        .env_remove("HOMEBASED_WEB_LISTEN")
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
    #[cfg(target_os = "linux")]
    {
        assert!(text.contains("KillMode=process"), "{text}");
        assert!(!text.contains("ExecStop"), "{text}");
        assert!(text.contains("ExecStart="), "{text}");
        assert!(text.contains("Environment=PATH="), "{text}");
    }
    #[cfg(target_os = "macos")]
    {
        assert!(text.contains("<key>AbandonProcessGroup</key>"), "{text}");
        assert!(text.contains("<key>ProgramArguments</key>"), "{text}");
        assert!(text.contains("<key>PATH</key>"), "{text}");
    }
    assert!(!text.contains("HOMEBASED_WEB_LISTEN"), "{text}");

    // the installing shell's bind is baked in, and a bad one fails install
    let out = Command::new(&hb)
        .env("HOMEBASED_WEB_LISTEN", "0.0.0.0:7677")
        .args(["daemon", "install", "--dry-run", "--home"])
        .arg(dir.path())
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    #[cfg(target_os = "linux")]
    assert!(
        text.contains("Environment=HOMEBASED_WEB_LISTEN=0.0.0.0:7677"),
        "{text}"
    );
    #[cfg(target_os = "macos")]
    assert!(
        text.contains("<key>HOMEBASED_WEB_LISTEN</key>")
            && text.contains("<string>0.0.0.0:7677</string>"),
        "{text}"
    );
    let out = Command::new(&hb)
        .env("HOMEBASED_WEB_LISTEN", "lan")
        .args(["daemon", "install", "--dry-run", "--home"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
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
    let spec = Harness::spec("claude", "hold");
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
    let spec = Harness::spec("claude", "hold");
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
    let spec = Harness::spec("claude", &"x".repeat(1024 * 1024));
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
fn process_group_cleaned_when_child_leaves_descendant() {
    let h = Harness::new();
    let pid_file = h.home.join("descendant.pid");
    let script = h.home.join("leave-descendant.sh");
    fs::write(
        &script,
        format!(
            "#!/bin/sh\n# nested sh so $$ is the descendant, not this script\nsh -c 'trap \"\" TERM; printf \"%s\\n\" \"$$\" > \"{pid}\"; sleep 60' &\nwhile [ ! -f '{pid}' ]; do sleep 0.01; done\nexit 0\n",
            pid = pid_file.display()
        ),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).unwrap();
    }
    let script_s = script.to_string_lossy().into_owned();
    let start = Instant::now();
    let id = h.submit(&Harness::task_spec(&[script_s.as_str()]));
    assert!(
        wait_until(Duration::from_secs(5), || pid_file.exists()),
        "descendant never recorded its pid"
    );
    let descendant: i32 = fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(descendant), None).is_ok(),
        "descendant {descendant} should be alive before group cleanup"
    );
    // direct child exited 0; runner must still reap the ignore-TERM descendant
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(show["exit_reason"]["kind"], "exit");
    assert_eq!(show["exit_reason"]["code"], 0);
    assert!(
        start.elapsed() < Duration::from_secs(25),
        "group cleanup took {:?}",
        start.elapsed()
    );
    let gone = wait_until(Duration::from_secs(5), || {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(descendant), None).is_err()
    });
    assert!(
        gone,
        "descendant {descendant} survived process-group cleanup"
    );
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
    for flag in ["arg(\"--prompt", "arg(\"--agent"] {
        let flags = grep_src(&cli, flag);
        assert!(flags.is_empty(), "per-field submit flag {flag}: {flags:?}");
    }
    let report = src.join("report.rs");
    for token in ["UnixStream", "/v1/"] {
        let http = grep_src(&report, token);
        assert!(http.is_empty(), "report talks HTTP ({token}): {http:?}");
    }

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

fn grep_daemon_except_callback(daemon: &Path, needle: &str) -> Vec<String> {
    grep_src(daemon, needle)
        .into_iter()
        .filter(|line| !line.contains("/actors/callback.rs:"))
        .collect()
}

fn grep_daemon_except_store(daemon: &Path, needle: &str) -> Vec<String> {
    grep_src(daemon, needle)
        .into_iter()
        .filter(|line| !line.contains("/actors/store.rs:"))
        .collect()
}

/// Plain substring scan over `.rs` files under `path`.
fn grep_src(path: &Path, needle: &str) -> Vec<String> {
    let mut matches = Vec::new();
    let files = if path.is_file() {
        vec![path.to_path_buf()]
    } else {
        walkdir_files(path)
    };
    for file in files {
        if file.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        if let Ok(text) = fs::read_to_string(&file) {
            for (i, line) in text.lines().enumerate() {
                if line.contains(needle) {
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

#[test]
fn cancel_on_terminal_task_is_idempotent() {
    let h = Harness::new();
    let id = h.submit(&Harness::spec("claude", "quick"));
    h.wait_status(&id, "succeeded");
    let before = h.queue_messages().len();
    let out = h
        .cmd()
        .args(["--json", "task", "cancel", &id])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "cancel on a terminal task must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["status"], "succeeded");
    assert_eq!(
        h.queue_messages().len(),
        before,
        "cancel re-sent a callback"
    );
}

#[test]
fn daemon_status_reports_socket_down() {
    let mut h = Harness::new();
    let id = h.submit(&Harness::spec("claude", "quick"));
    h.wait_status(&id, "succeeded");
    let out = h
        .cmd()
        .args(["--json", "daemon", "status"])
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["socket"], "up");

    h.stop_daemon();
    let out = h
        .cmd()
        .args(["--json", "daemon", "status"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["socket"], "down");
    assert_eq!(value["in_flight"], 0);

    let out = h.cmd().args(["daemon", "status"]).output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("socket: down"), "{text}");
}

#[test]
fn list_filters_by_status_and_thread() {
    let h = Harness::new();
    let other_thread = "01a0ab97-a7aa-7463-a5b0-8d500e40e999";
    let done = h.submit(&Harness::spec("claude", "done"));
    h.wait_status(&done, "succeeded");

    let mut spec = Harness::spec("claude", "still running");
    spec["thread"] = json!(other_thread);
    let spec_path = h.home.join("spec-running.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    h.set_control("sleep", "20");
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let running = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(
        Duration::from_secs(5),
        || h.show(&running)["status"] == "running"
    ));

    assert_eq!(list_ids(&h, &["--status", "succeeded"]), vec![done.clone()]);
    assert_eq!(
        list_ids(&h, &["--status", "running"]),
        vec![running.clone()]
    );
    let mut both = list_ids(&h, &["--status", "succeeded,running"]);
    both.sort();
    let mut want = vec![done.clone(), running.clone()];
    want.sort();
    assert_eq!(both, want);
    assert_eq!(list_ids(&h, &["--thread", THREAD]), vec![done]);
    assert_eq!(list_ids(&h, &["--thread", other_thread]), vec![running]);
    assert!(list_ids(&h, &["--status", "lost"]).is_empty());
}

fn list_ids(h: &Harness, args: &[&str]) -> Vec<String> {
    let out = h
        .cmd()
        .args(["--quiet", "task", "list"])
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

#[test]
fn log_tail_returns_the_last_lines() {
    let h = Harness::new();
    h.set_control("stdout", "line one\nline two\nline three\n");
    let id = h.submit(&Harness::spec("claude", "logging"));
    h.wait_status(&id, "succeeded");

    let out = h.cmd().args(["task", "log", &id]).output().unwrap();
    let full = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(full.contains("line one"), "{full}");

    let out = h
        .cmd()
        .args(["task", "log", &id, "--tail", "1"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let tail = String::from_utf8_lossy(&out.stdout).into_owned();
    assert_eq!(tail, "line three\n");

    let out = h
        .cmd()
        .args(["task", "log", &id, "--tail", "2"])
        .output()
        .unwrap();
    let tail = String::from_utf8_lossy(&out.stdout).into_owned();
    assert_eq!(tail, "line two\nline three\n");
}

#[test]
fn web_listener_serves_read_only_api() {
    let h = Harness::with_dashboard();
    h.set_control("stdout", "line one\nline two\nline three\n");
    let id = h.submit(&Harness::spec("claude", "dashboard"));
    h.wait_status(&id, "succeeded");

    let list = h.cmd().args(["--json", "task", "list"]).output().unwrap();
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    let socket_list: Value = serde_json::from_slice(&list.stdout).unwrap();
    assert!(
        socket_list["tasks"][0]["created_at"].is_string(),
        "{socket_list}"
    );

    let addr = dashboard_addr(&h);

    let tasks = http_get(&addr, "/v1/tasks");
    assert_eq!(tasks.status, 200, "{tasks:?}");
    assert!(tasks.content_type_contains("application/json"), "{tasks:?}");
    let body: Value = serde_json::from_str(&tasks.body).unwrap();
    let first = &body["tasks"][0];
    assert_eq!(first["id"], id, "{body}");
    assert!(first["created_at"].is_string(), "{body}");
    assert_eq!(first["timeout_secs"], 2 * 3600, "{body}");
    assert_eq!(
        first["workload"],
        json!({"type": "agent", "agent": "claude", "model": "fable"}),
        "{body}"
    );

    let log = http_get(&addr, &format!("/v1/tasks/{id}/log?tail=1"));
    assert_eq!(log.status, 200, "{log:?}");
    let body: Value = serde_json::from_str(&log.body).unwrap();
    assert_eq!(body["log"], "line three", "{body}");
    assert_eq!(body["truncated"], true, "{body}");

    // mutations stay on the 0600 socket; the TCP router rejects write methods here
    let submit = http_request(&addr, "POST", "/v1/tasks", None, None);
    assert_eq!(submit.status, 405, "{submit:?}");

    let index = http_get(&addr, "/");
    assert!(
        index.status == 200 || index.status == 503,
        "unexpected index status: {index:?}"
    );
    assert!(index.content_type_contains("text/html"), "{index:?}");

    let missing = http_get(&addr, "/v1/nope");
    assert_eq!(missing.status, 404, "{missing:?}");
    let body: Value = serde_json::from_str(&missing.body).unwrap();
    assert_eq!(body["error"]["code"], "not_found", "{body}");
}

#[test]
fn named_and_unnamed_tasks_expose_display_name() {
    let h = Harness::new();
    let mut named = Harness::task_spec(&["true"]);
    named["name"] = json!("named job");
    let named_id = h.submit(&named);
    let unnamed_id = h.submit(&Harness::task_spec(&["echo", "hi", "there", "x"]));

    let named_show: Value = serde_json::from_slice(
        &h.cmd()
            .args(["--json", "task", "show", &named_id])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    assert_eq!(named_show["name"], "named job");
    assert_eq!(named_show["display_name"], "named job");

    let unnamed_show: Value = serde_json::from_slice(
        &h.cmd()
            .args(["--json", "task", "show", &unnamed_id])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    assert!(unnamed_show.get("name").is_none() || unnamed_show["name"].is_null());
    assert_eq!(unnamed_show["display_name"], "echo hi there…");
}

#[test]
fn dashboard_file_browser_and_content_origin() {
    let h = Harness::with_dashboard();
    let addr = dashboard_addr(&h);
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("nested");
    fs::create_dir(&nested).unwrap();
    let text = nested.join("note.txt");
    fs::write(&text, b"hello files").unwrap();
    let html = nested.join("page.html");
    fs::write(
        &html,
        b"<html><body><a href=\"note.txt\">n</a></body></html>",
    )
    .unwrap();
    let bin = nested.join("blob.bin");
    fs::write(&bin, b"\0\x01\x02\x03binary").unwrap();

    let resolve = http_post_json(
        &addr,
        "/v1/files/resolve",
        &json!({ "path": nested.to_string_lossy() }),
    );
    assert_eq!(resolve.status, 200, "{resolve:?}");
    let resolved: Value = serde_json::from_str(&resolve.body).unwrap();
    assert_eq!(resolved["kind"], "directory");
    let token = resolved["token"].as_str().unwrap();

    let listing = http_get(&addr, &format!("/v1/files/{token}"));
    assert_eq!(listing.status, 200, "{listing:?}");
    let listing_body: Value = serde_json::from_str(&listing.body).unwrap();
    assert!(listing_body["entries"].as_array().unwrap().len() >= 3);

    let origin = http_get(&addr, "/v1/files/origin");
    assert_eq!(origin.status, 200, "{origin:?}");
    let origin_body: Value = serde_json::from_str(&origin.body).unwrap();
    let content_port = origin_body["port"].as_u64().unwrap() as u16;
    let content_host = addr.split(':').next().unwrap();
    let content_addr = format!("{content_host}:{content_port}");

    let mirrored = format!("/raw{}", text.to_string_lossy());
    let text_resp = http_get(&content_addr, &mirrored);
    assert_eq!(text_resp.status, 200, "{text_resp:?}");
    assert!(
        text_resp.content_type_contains("text/plain"),
        "{text_resp:?}"
    );
    assert!(
        text_resp.head.contains("content-disposition: inline"),
        "{text_resp:?}"
    );
    assert!(
        text_resp.head.contains("x-content-type-options: nosniff"),
        "{text_resp:?}"
    );
    assert!(
        text_resp
            .head
            .contains("cross-origin-resource-policy: same-origin"),
        "{text_resp:?}"
    );
    assert!(
        text_resp.head.contains("referrer-policy: no-referrer"),
        "{text_resp:?}"
    );
    assert_eq!(text_resp.body, "hello files");
    assert!(
        !text_resp.head.contains("access-control-allow-origin"),
        "{text_resp:?}"
    );

    let html_resp = http_get(&content_addr, &format!("/raw{}", html.to_string_lossy()));
    assert_eq!(html_resp.status, 200, "{html_resp:?}");
    assert!(
        html_resp.content_type_contains("text/html"),
        "{html_resp:?}"
    );
    assert!(
        html_resp.head.contains("content-disposition: inline"),
        "{html_resp:?}"
    );

    let bin_resp = http_get(&content_addr, &format!("/raw{}", bin.to_string_lossy()));
    assert_eq!(bin_resp.status, 200, "{bin_resp:?}");
    assert!(
        bin_resp.content_type_contains("application/octet-stream"),
        "{bin_resp:?}"
    );
    assert!(
        bin_resp.head.contains("content-disposition: attachment"),
        "{bin_resp:?}"
    );

    // content origin has no dashboard API
    let leaked = http_get(&content_addr, "/v1/tasks");
    assert_eq!(leaked.status, 404, "{leaked:?}");

    // unexpected Host is rejected on both origins
    let bad_host = http_get_host(&addr, "/v1/status", "evil.example");
    assert_eq!(bad_host.status, 400, "{bad_host:?}");
    let bad_content = http_get_host(&content_addr, &mirrored, "evil.example");
    assert_eq!(bad_content.status, 400, "{bad_content:?}");

    assert!(
        !origin.head.contains("access-control-allow-origin"),
        "{origin:?}"
    );
}

/// `host:port` of the running dashboard, from the daemon's own status body.
fn dashboard_addr(h: &Harness) -> String {
    let out = h
        .cmd()
        .args(["--json", "daemon", "status"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let status: Value = serde_json::from_slice(&out.stdout).unwrap();
    let url = status["web"]
        .as_str()
        .unwrap_or_else(|| panic!("daemon status has no dashboard url: {status}"));
    url.trim_start_matches("http://").to_string()
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    /// Response headers, lowercased, for substring checks.
    head: String,
    body: String,
}

impl HttpResponse {
    fn content_type_contains(&self, needle: &str) -> bool {
        self.head
            .lines()
            .filter(|line| line.starts_with("content-type:"))
            .any(|line| line.contains(needle))
    }
}

fn http_get(addr: &str, path: &str) -> HttpResponse {
    http_request(addr, "GET", path, None, None)
}

fn http_get_host(addr: &str, path: &str, host: &str) -> HttpResponse {
    http_request(addr, "GET", path, None, Some(host))
}

fn http_post_json(addr: &str, path: &str, body: &Value) -> HttpResponse {
    http_request(addr, "POST", path, Some(body), None)
}

/// Hand-written HTTP/1.1 over a plain socket: the assertions are about what a
/// browser sees, so no client crate sits in between.
fn http_request(
    addr: &str,
    method: &str,
    path: &str,
    json_body: Option<&Value>,
    host: Option<&str>,
) -> HttpResponse {
    let host = host.unwrap_or(addr);
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
    let body_bytes = json_body.map(|value| serde_json::to_vec(value).unwrap());
    if let Some(bytes) = &body_bytes {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", bytes.len()));
    }
    request.push_str("\r\n");
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    if let Some(bytes) = body_bytes {
        stream.write_all(&bytes).unwrap();
    }
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response: {text}"));
    HttpResponse {
        status,
        head: head.to_lowercase(),
        body: body.to_string(),
    }
}

#[test]
fn report_reads_summary_from_stdin() {
    let h = Harness::new();
    let spec = Harness::spec("claude", "running");
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

    let mut child = h
        .cmd()
        .args([
            "--json",
            "task",
            "report",
            "--id",
            &id,
            "--outcome",
            "blocked",
            "--summary-file",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write;
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"summary read from stdin")
            .unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["seq"], 1);
    assert_eq!(value["reports"][0]["summary"], "summary read from stdin");
    assert_eq!(value["reports"][0]["outcome"], "blocked");
}

#[test]
fn report_without_id_or_env_is_usage_error() {
    let h = Harness::new();
    let out = h
        .cmd()
        .env_remove("HOMEBASED_TASK_ID")
        .args([
            "task",
            "report",
            "--outcome",
            "succeeded",
            "--summary",
            "orphan",
        ])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("HOMEBASED_TASK_ID"), "{stderr}");
}

#[test]
fn daemon_exits_on_sigterm_with_a_live_worker() {
    let mut h = Harness::new();
    let spec = Harness::spec("claude", "hold");
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

    let mut daemon = h.daemon.take().unwrap();
    let daemon_pid = daemon.id() as i32;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(daemon_pid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    let exited = wait_until(Duration::from_secs(2), || {
        matches!(daemon.try_wait(), Ok(Some(_)))
    });
    let _ = daemon.kill();
    let _ = daemon.wait();
    assert!(
        exited,
        "daemon {daemon_pid} did not exit within 2s of SIGTERM"
    );
}

#[test]
fn sigterm_right_after_cas_is_cancelled_not_lost() {
    let h = Harness::new();
    // the window runs from the Queued->Running CAS to `signal()` inside
    // `run_agent`, and the only work in it is the feed read. Swap a very large
    // feed in behind the daemon so that read takes long enough to signal into.
    let big = h.home.join("big-feed.txt");
    fs::write(&big, vec![b'x'; 512 * 1024 * 1024]).unwrap();

    let mut spec = Harness::spec("claude", "widen the window");
    spec["workload"]["report_trailer"] = json!(false);
    h.set_control("no-stdin", "");
    h.set_control("sleep", "20");
    let spec_path = h.home.join("spec.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    let id: TaskId = serde_json::from_slice::<Value>(&out.stdout).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    // rename is atomic and instant, unlike writing the bytes here
    fs::rename(
        &big,
        h.home
            .join("tasks")
            .join(id.to_string())
            .join("prompt.feed.txt"),
    )
    .unwrap();

    // poll SQLite directly with no back-off and signal from this process:
    // going through the socket would cost more than the window is wide.
    let store = h.store();
    let deadline = Instant::now() + Duration::from_secs(10);
    let pid = loop {
        assert!(Instant::now() < deadline, "task never reached running");
        let row = store.require_task(id).unwrap();
        if row.status() == ProcessStatus::Running {
            break row.pid().expect("running row records the worker pid");
        }
    };
    store.request_cancel(id).unwrap();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    drop(store);

    let show = h.wait_status(&id.to_string(), "cancelled");
    assert_eq!(show["exit_reason"]["kind"], "cancelled");
    // the worker delivers the exit event just after the CAS `wait_status` saw
    let delivered = wait_until(Duration::from_secs(10), || {
        h.queue_messages()
            .iter()
            .any(|m| event_json(m)["task"] == id.to_string())
    });
    assert!(delivered, "worker never delivered an exit event for {id}");
    let last = event_json(h.queue_messages().last().unwrap());
    assert_eq!(last["event"], "TASK_CANCELLED");
}

#[test]
fn reconcile_delivers_a_pending_callback_on_a_terminal_row() {
    let mut h = Harness::new();
    h.stop_daemon();

    let id = TaskId::new();
    let store = h.store();
    fs::create_dir_all(h.home.join("tasks").join(id.to_string())).unwrap();
    store
        .insert_task(&new_queued_task(NewTask {
            id,
            name: None,
            thread: THREAD.parse().unwrap(),
            workload: Workload::Agent(AgentWorkload {
                agent: Agent::new(AgentKind::Claude, None),
                extra_args: vec![],
                report_trailer: false,
            }),
            cwd: std::env::temp_dir(),
            timeout: Duration::from_secs(2 * 3600),
            env: TaskEnv {
                path: h.path.clone(),
                home: std::env::var("HOME").unwrap_or_else(|_| "/tmp".into()),
            },
            binary: fixture("fake-claude"),
        }))
        .unwrap();
    // the daemon died between the exit CAS and FinishCallback
    let row = store
        .cas_exit(id, ProcessStatus::Queued, &ExitReason::Cancelled)
        .unwrap()
        .expect("queued row cancels");
    assert_eq!(row.callback_status, CallbackStatus::Pending);
    drop(store);

    let before = h.queue_messages().len();
    h.start_daemon();
    let delivered = wait_until(Duration::from_secs(10), || {
        h.queue_messages()
            .iter()
            .skip(before)
            .any(|m| event_json(m)["task"] == id.to_string())
    });
    assert!(delivered, "reconcile never delivered the pending callback");
    assert_eq!(
        h.store().require_task(id).unwrap().callback_status,
        CallbackStatus::Sent
    );
}

#[test]
fn task_workload_success() {
    let h = Harness::new();
    let id = h.submit(&Harness::task_spec(&["/bin/echo", "hello"]));
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(
        show["workload"],
        json!({"type": "task", "command": ["/bin/echo", "hello"]})
    );
    assert_eq!(show["exit_reason"]["kind"], "exit");
    assert_eq!(show["exit_reason"]["code"], 0);
    let ev = event_json(h.queue_messages().last().unwrap());
    assert_eq!(ev["event"], "TASK_SUCCEEDED");
    assert_eq!(
        ev["workload"],
        json!({"type": "task", "command": ["/bin/echo", "hello"]})
    );
    let log = fs::read_to_string(h.home.join("tasks").join(&id).join("output.log")).unwrap();
    assert!(log.contains("hello"), "{log}");
    assert!(
        !h.home.join("tasks").join(&id).join("prompt.txt").exists(),
        "task workloads must not write prompt evidence"
    );
}

#[test]
fn task_workload_nonzero_exit() {
    let h = Harness::new();
    let id = h.submit(&Harness::task_spec(&["false"]));
    let show = h.wait_status(&id, "failed");
    assert_eq!(show["exit_reason"]["kind"], "exit");
    assert_ne!(show["exit_reason"]["code"], 0);
    let ev = event_json(h.queue_messages().last().unwrap());
    assert_eq!(ev["event"], "TASK_FAILED");
    assert_eq!(ev["process"]["kind"], "exit");
}

#[test]
fn task_argv_fidelity_empty_and_space_args() {
    let h = Harness::new();
    let script = h.home.join("argv-dump.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'argc=%s\\n' \"$#\"\ni=1\nfor a in \"$@\"; do\n  printf 'arg%s=%s\\n' \"$i\" \"$a\"\n  i=$((i+1))\ndone\n",
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).unwrap();
    }
    let script_s = script.to_string_lossy().into_owned();
    let id = h.submit(&Harness::task_spec(&[script_s.as_str(), "", " ", "ok"]));
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(
        show["workload"]["command"],
        json!([script_s, "", " ", "ok"])
    );
    let log = fs::read_to_string(h.home.join("tasks").join(&id).join("output.log")).unwrap();
    assert!(log.contains("argc=3"), "{log}");
    assert!(log.contains("arg1=\n"), "{log}");
    assert!(log.contains("arg2= \n") || log.contains("arg2= "), "{log}");
    assert!(log.contains("arg3=ok"), "{log}");
}

#[test]
fn timeout_thirty_minutes_is_accepted() {
    let h = Harness::new();
    let mut spec = Harness::spec("claude", "min timeout");
    spec["timeout"] = json!("30m");
    let id = h.submit(&spec);
    let show = h.wait_status(&id, "succeeded");
    assert_eq!(show["timeout_secs"], 30 * 60);
}

#[test]
fn timeout_below_thirty_minutes_rejected() {
    let h = Harness::new();
    let mut spec = Harness::spec("claude", "too short");
    spec["timeout"] = json!("29m");
    let spec_path = h.home.join("spec-short.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["code"], "invalid_spec");
    assert_eq!(err["error"]["input"]["pointer"], "/timeout");
    assert!(h.store().list_tasks(&[], None).unwrap().is_empty());
}

#[test]
fn missing_executable_returns_executable_missing() {
    let h = Harness::new();
    let missing = h.home.join("no-such-bin");
    let spec = Harness::task_spec(&[missing.to_str().unwrap(), "x"]);
    let spec_path = h.home.join("spec-missing.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let out = h
        .cmd()
        .args(["--json", "task", "submit", "--spec"])
        .arg(&spec_path)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(3),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["code"], "executable_missing");
    assert!(h.store().list_tasks(&[], None).unwrap().is_empty());
}

#[test]
fn task_dry_run_returns_exact_argv_and_creates_no_row() {
    let h = Harness::new();
    let spec = Harness::task_spec(&["/bin/echo", "", " spaced ", "hi"]);
    let mut child = h
        .cmd()
        .args(["--json", "task", "submit", "--dry-run", "--spec", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
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
    assert_eq!(v["argv"], json!(["/bin/echo", "", " spaced ", "hi"]), "{v}");
    assert_eq!(v["stdin"], "null");
    assert!(h.store().list_tasks(&[], None).unwrap().is_empty());
    let tasks_dir = h.home.join("tasks");
    assert!(!tasks_dir.exists() || fs::read_dir(&tasks_dir).unwrap().next().is_none());
}

/// A terminal event must never overtake a `TASK_CHECK_DUE` that is already on
/// the wire. The gated fake queue makes `queue-messages.txt` record completion
/// order, so the assertion is about real delivery order, not about timing.
///
/// Also covers the settlement boundary past the old 15s release valve without
/// waiting the full 90s settle budget: the gate stays closed past 15s from
/// queue entry, still under the 20s attempt deadline, and the terminal event
/// must not leak.
#[test]
fn terminal_event_waits_for_an_in_flight_check_due() {
    use homebased::callback::{ATTENTION_SETTLE, QUEUE_ATTEMPT_TIMEOUT};
    assert!(
        ATTENTION_SETTLE > QUEUE_ATTEMPT_TIMEOUT * 3,
        "settle must outlast three bounded queue attempts"
    );

    let mut h = Harness::new();
    h.set_control("sleep", "40");
    let id = h.submit(&Harness::spec("claude", "race the terminal event"));
    assert!(wait_until(Duration::from_secs(5), || h.show(&id)["status"]
        == "running"));
    let before = h.queue_messages().len();

    // hold the check-due send open, then make the reminder overdue
    h.set_control("queue-gate", "TASK_CHECK_DUE");
    h.backdate_created_at(&id, 3);
    h.restart_daemon();
    let entered = h.record.join("queue-gate-entered");
    assert!(
        wait_until(Duration::from_secs(15), || entered.exists()),
        "the check-due send never reached the queue: {:?}",
        h.queue_messages()
    );
    let entered_at = Instant::now();
    assert_eq!(h.show(&id)["check_timeout"], "pending", "not delivered yet");

    // the child ends while the reminder is still in flight
    let out = h.cmd().args(["task", "cancel", &id]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    h.wait_status(&id, "cancelled");

    // hold past the old 15s valve, still under the 20s attempt deadline, and
    // prove the terminal event cannot overtake the live claim
    let hold_until = entered_at + Duration::from_secs(16);
    while Instant::now() < hold_until {
        assert_eq!(
            h.queue_messages().len(),
            before,
            "a terminal event was delivered while TASK_CHECK_DUE was still sending: {:?}",
            h.queue_messages()
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    h.clear_control("queue-gate");
    assert!(
        wait_until(Duration::from_secs(20), || {
            let events: Vec<String> = h
                .queue_messages()
                .iter()
                .skip(before)
                .map(|m| {
                    event_json(m)["event"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string()
                })
                .collect();
            events.iter().any(|e| e == "TASK_CANCELLED")
        }),
        "terminal event never arrived: {:?}",
        h.queue_messages()
    );
    let events: Vec<String> = h
        .queue_messages()
        .iter()
        .skip(before)
        .map(|m| {
            event_json(m)["event"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    let check = events.iter().position(|e| e == "TASK_CHECK_DUE");
    let terminal = events.iter().position(|e| e == "TASK_CANCELLED");
    assert_eq!(check, Some(0), "check-due must land first: {events:?}");
    assert!(
        terminal > check,
        "terminal event landed before the reminder: {events:?}"
    );
    assert_eq!(h.show(&id)["check_timeout"], "sent");
}
