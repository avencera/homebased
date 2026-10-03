//! Service install, stop, restart, uninstall, status, and self-update

use super::{Harness, WEB_OFF, event_json, fixture, wait_until};
use homebased::domain::{CallbackStatus, TaskId};
use serde_json::Value;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

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
    let task = id.parse::<TaskId>().unwrap();
    assert_eq!(
        h.store().task_presentations(&[task]).unwrap()[&task].terminal_callback,
        Some(CallbackStatus::Sent)
    );
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
    let out = h
        .cmd()
        .args(["daemon", "restart"])
        .env("HARNESS_SUPERVISOR_KEEP_DAEMON", "1")
        .output()
        .unwrap();
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
    let out = h
        .cmd()
        .args(["daemon", "restart"])
        .env("HARNESS_SUPERVISOR_KEEP_DAEMON", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // restart reloads the unit from disk so an updated unit takes effect
    let calls = h.supervisor_calls();
    if cfg!(target_os = "linux") {
        assert_eq!(
            calls,
            ["--user daemon-reload", "--user restart homebased.service"]
        );
    } else if cfg!(target_os = "macos") {
        let uid = nix::unistd::getuid().as_raw();
        assert_eq!(
            calls,
            [
                format!("bootout gui/{uid}/dev.praveen.homebased"),
                format!("bootstrap gui/{uid} {}", h.host_unit_path().display()),
            ]
        );
    }
    // fake restart is a no-op, so the original daemon stays up
    assert_eq!(h.socket_pid(), before_pid);
}

#[test]
fn update_dry_run_json() {
    let hb = assert_cmd::cargo::cargo_bin("homebased");
    let dir = TempDir::new().unwrap();
    let dest = dir.path().join("bin");
    let state = dir.path().join("state");
    fs::create_dir_all(&state).unwrap();
    let out = Command::new(&hb)
        .args(["--json", "update", "--dry-run", "--tag", "v0.2.0", "--to"])
        .arg(&dest)
        .arg("--home")
        .arg(&state)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["api_version"], 1);
    assert_eq!(v["tag"], "v0.2.0");
    assert_eq!(v["dry_run"], true);
    assert_eq!(v["restarted"], false);
    assert_eq!(
        v["path"].as_str().unwrap(),
        dest.join("homebased").to_str().unwrap()
    );
    assert!(
        v["url"].as_str().unwrap().contains("homebased-v0.2.0-"),
        "{v}"
    );
    assert!(
        !dest.join("homebased").exists(),
        "dry-run must not write the binary"
    );
}

#[test]
fn update_installs_archive_and_restarts_daemon() {
    use std::os::unix::fs::PermissionsExt;

    let hb = assert_cmd::cargo::cargo_bin("homebased");
    let dir = TempDir::new().unwrap();
    let dest = dir.path().join("bin");
    let state = dir.path().join("state");
    let user_home = dir.path().join("user-home");
    fs::create_dir_all(&state).unwrap();
    fs::create_dir_all(&user_home).unwrap();

    let payload = dir.path().join("payload");
    fs::create_dir(&payload).unwrap();
    let fake = payload.join("homebased");
    fs::write(&fake, b"updated-binary\n").unwrap();
    let mut perms = fs::metadata(&fake).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&fake, perms).unwrap();
    let archive = dir.path().join("homebased.tar.gz");
    let tar = Command::new("tar")
        .args([
            "-C",
            payload.to_str().unwrap(),
            "-czf",
            archive.to_str().unwrap(),
            "homebased",
        ])
        .status()
        .unwrap();
    assert!(tar.success());
    let body = fs::read(&archive).unwrap();
    let (base, server) = serve_http_bytes(body);

    let out = Command::new(&hb)
        .env("HOME", &user_home)
        .env("HOMEBASED_WEB_LISTEN", WEB_OFF)
        .env("HOMEBASED_UPDATE_BASE_URL", &base)
        .args(["--json", "update", "--tag", "v9.9.9", "--to"])
        .arg(&dest)
        .arg("--home")
        .arg(&state)
        .output()
        .unwrap();
    let _ = server.join();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["tag"], "v9.9.9");
    assert_eq!(v["restarted"], true);
    assert_eq!(v["dry_run"], false);
    assert_eq!(
        fs::read(dest.join("homebased")).unwrap(),
        b"updated-binary\n"
    );
    assert!(
        state.join("homebased.sock").exists(),
        "daemon did not restart"
    );

    let stop = Command::new(&hb)
        .env("HOME", &user_home)
        .env("HOMEBASED_WEB_LISTEN", WEB_OFF)
        .args(["--json", "daemon", "stop", "--home"])
        .arg(&state)
        .output()
        .unwrap();
    assert!(
        stop.status.success(),
        "{}",
        String::from_utf8_lossy(&stop.stderr)
    );
}

fn serve_http_bytes(body: Vec<u8>) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut buf = [0u8; 2048];
        let _ = stream.read(&mut buf);
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(&body);
    });
    (format!("http://{addr}"), handle)
}

#[test]
fn install_dry_run_text() {
    let hb = assert_cmd::cargo::cargo_bin("homebased");
    let dir = TempDir::new().unwrap();
    let out = Command::new(&hb)
        .env_remove("HOMEBASED_WEB_LISTEN")
        .env("HOMEBASED_OPENCODE", fixture("fake-opencode"))
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
        assert!(
            text.contains(&format!(
                "Environment=HOMEBASED_OPENCODE={}",
                fixture("fake-opencode").display()
            )),
            "{text}"
        );
    }
    #[cfg(target_os = "macos")]
    {
        assert!(text.contains("<key>AbandonProcessGroup</key>"), "{text}");
        assert!(text.contains("<key>ProgramArguments</key>"), "{text}");
        assert!(text.contains("<key>PATH</key>"), "{text}");
        assert!(
            text.contains("<key>HOMEBASED_OPENCODE</key>")
                && text.contains(&format!(
                    "<string>{}</string>",
                    fixture("fake-opencode").display()
                )),
            "{text}"
        );
    }
    assert!(!text.contains("HOMEBASED_WEB_LISTEN"), "{text}");

    // the installing shell's bind is baked in, and a bad one fails install
    // unset means the dashboard stays off; the unit does not invent a bind
    let out = Command::new(&hb)
        .env("HOMEBASED_WEB_LISTEN", "0.0.0.0:7677")
        .env("HOMEBASED_OPENCODE", fixture("fake-opencode"))
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
        .env("HOMEBASED_OPENCODE", fixture("fake-opencode"))
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
