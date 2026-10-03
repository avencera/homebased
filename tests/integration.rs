#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use homebased::store::Store;
use serde_json::{Value, json};
use tempfile::TempDir;

const THREAD: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e431";

/// `HOMEBASED_WEB_LISTEN` for a harness daemon. The dashboard is off unless a
/// test opts in; dashboard tests bind `127.0.0.1:0` so they do not share a port
const WEB_OFF: &str = "off";
const WEB_EPHEMERAL: &str = "127.0.0.1:0";

/// Give `thread` a Codex session file under `user_home` so the submitting CLI
/// accepts it as a callback thread
fn register_thread(user_home: &Path, thread: &str) {
    let day = user_home.join(".codex/sessions/2026/01/01");
    fs::create_dir_all(&day).unwrap();
    fs::write(
        day.join(format!("rollout-2026-01-01T00-00-00-{thread}.jsonl")),
        "",
    )
    .unwrap();
}

struct Harness {
    dir: TempDir,
    /// Private `$HOME` so unit paths never touch the developer's real home
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

    /// Harness whose daemon also serves the dashboard on a free port
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
        register_thread(&user_home, THREAD);
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
            .env("HOMEBASED_OPENCODE", fixture("fake-opencode"))
            .env("FAKE_RECORD_DIR", &self.record)
            .env("HOME", &self.user_home)
            .env_remove("CODEX_HOME")
            .env_remove("HOMEBASED_TASK_ID")
            .env("HARNESS_SUPERVISOR_LOG", &self.supervisor_log)
            .current_dir(std::env::temp_dir());
        if let Some(child) = &self.daemon {
            cmd.env("HARNESS_DAEMON_PID", child.id().to_string());
        }
        cmd
    }

    fn start_daemon(&mut self) {
        self.start_daemon_with_env(&[]);
    }

    /// Start the daemon with extra environment, as if inherited from its launcher
    fn start_daemon_with_env(&mut self, env: &[(&str, &str)]) {
        let mut command = self.cmd();
        command.envs(env.iter().copied());
        let child = command
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
        self.restart_daemon_with_env(&[]);
    }

    fn restart_daemon_with_env(&mut self, env: &[(&str, &str)]) {
        self.stop_daemon();
        self.start_daemon_with_env(env);
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
    /// process, not the worker pid the test signalled
    fn agent_pid(&self, id: &str) -> i32 {
        let path = self.record.join(format!("agent-pid-{id}.txt"));
        assert!(
            wait_until(Duration::from_secs(10), || path.exists()),
            "agent for {id} never recorded its pid"
        );
        fs::read_to_string(&path).unwrap().trim().parse().unwrap()
    }

    /// Bytes the fake agent read on stdin for exactly this task
    fn agent_stdin(&self, id: &str) -> String {
        fs::read_to_string(self.record.join(format!("stdin-{id}.txt"))).unwrap()
    }

    fn agent_meta(&self, id: &str) -> String {
        fs::read_to_string(self.record.join(format!("agent-meta-{id}.txt"))).unwrap()
    }

    fn opencode_meta(&self, id: &str) -> String {
        let path = self.record.join(format!("opencode-meta-{id}.txt"));
        assert!(wait_until(Duration::from_secs(10), || path.exists()));
        fs::read_to_string(path).unwrap()
    }

    fn opencode_stdin(&self, id: &str) -> String {
        let path = self.record.join(format!("opencode-stdin-{id}.txt"));
        assert!(wait_until(Duration::from_secs(10), || path.exists()));
        fs::read_to_string(path).unwrap()
    }

    fn opencode_pid(&self, id: &str, kind: &str) -> i32 {
        let path = self.record.join(format!("opencode-{kind}-pid-{id}.txt"));
        assert!(wait_until(Duration::from_secs(10), || path.exists()));
        fs::read_to_string(path).unwrap().trim().parse().unwrap()
    }

    fn store(&self) -> Store {
        Store::open(&self.home.join(homebased::home::DB_NAME)).unwrap()
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

    fn wait_for_event(&self, id: &str, event: &str) -> Vec<String> {
        assert!(
            wait_until(Duration::from_secs(10), || self
                .queue_messages()
                .iter()
                .any(|message| {
                    let payload = event_json(message);
                    payload["task"] == id && payload["event"] == event
                })),
            "callback {event} for task {id} was not delivered: {:?}",
            self.queue_messages()
        );
        self.queue_messages()
    }

    fn spec(agent: &str, prompt: &str) -> Value {
        Self::spec_with_cwd(agent, prompt, std::env::temp_dir())
    }

    fn spec_with_cwd(agent: &str, prompt: &str, cwd: impl AsRef<Path>) -> Value {
        json!({
            "api_version": 1,
            "thread": THREAD,
            "name": "test agent",
            "cwd": cwd.as_ref(),
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
            "name": "test task",
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
        let conn = rusqlite::Connection::open(self.home.join(homebased::home::DB_NAME)).unwrap();
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
        let conn = rusqlite::Connection::open(self.home.join(homebased::home::DB_NAME)).unwrap();
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
            "opencode-hold",
            "opencode-ignore-term",
            "opencode-tool-ignore-term",
            "opencode-exit",
        ] {
            let _ = fs::remove_file(self.record.join(name));
        }
    }

    /// Put a fake `systemctl`/`launchctl` ahead of PATH and clear its log
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

    /// Write a host unit under the harness private `$HOME`
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

fn process_is_live(pid: i32) -> bool {
    if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err() {
        return false;
    }
    #[cfg(target_os = "linux")]
    if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
        return stat
            .split_whitespace()
            .nth(2)
            .is_some_and(|state| state != "Z");
    }
    true
}

fn event_json(line: &str) -> Value {
    let json = line.strip_prefix("HOMEBASED_EVENT ").unwrap_or(line);
    serde_json::from_str(json).unwrap()
}

/// `host:port` of the running dashboard, from the daemon's own status body
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
    /// Response headers, lowercased, for substring checks
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
/// browser sees, so no client crate sits in between
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

/// Run `homebased --json <args> --spec <file>` and return the exit code and error JSON
fn submit_error(h: &Harness, spec: &Value, args: &[&str], worker: Option<&str>) -> (i32, Value) {
    let spec_path = h.home.join("rejected-spec.json");
    fs::write(&spec_path, serde_json::to_vec(spec).unwrap()).unwrap();
    let mut cmd = h.cmd();
    if let Some(task) = worker {
        cmd.env("HOMEBASED_TASK_ID", task);
    }
    let out = cmd
        .arg("--json")
        .args(args)
        .arg("--spec")
        .arg(&spec_path)
        .output()
        .unwrap();
    let error = serde_json::from_slice(&out.stderr).unwrap_or(Value::Null);
    (out.status.code().unwrap(), error)
}

#[path = "integration/containers.rs"]
mod containers;
#[path = "integration/dashboard.rs"]
mod dashboard;
#[path = "integration/dependencies.rs"]
mod dependencies;
#[path = "integration/lifecycle.rs"]
mod lifecycle;
#[path = "integration/reports.rs"]
mod reports;
#[path = "integration/resource_jobs.rs"]
mod resource_jobs;
#[path = "integration/service.rs"]
mod service;
#[path = "integration/source_rules.rs"]
mod source_rules;
#[path = "integration/submission.rs"]
mod submission;
#[path = "integration/workers.rs"]
mod workers;
