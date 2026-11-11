//! `homebased service`: readiness, stop, ownership cleanup, and the one
//! `SERVICE_EXITED` event, end to end

use super::{Harness, THREAD, event_json, process_is_live, wait_until};
use serde_json::Value;
use std::net::TcpListener;
use std::process::{Command, Output};
use std::time::Duration;

impl Harness {
    /// A `homebased service` command, run by `owner` when given and by a
    /// plain terminal otherwise
    fn service_cmd(&self, owner: Option<&str>) -> Command {
        let mut cmd = self.cmd();
        cmd.env_remove("T3CODE_THREAD_ID");
        if let Some(owner) = owner {
            cmd.env("CODEX_THREAD_ID", owner);
        }
        cmd.args(["--json", "service"]);
        cmd
    }

    /// Start a service and return its id, failing the test if it was not ready
    fn start_service(&self, owner: Option<&str>, args: &[&str]) -> String {
        let out = self
            .service_cmd(owner)
            .arg("start")
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "service start failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let view: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(view["state"], "ready", "{view}");
        view["id"].as_str().unwrap().to_owned()
    }

    /// One service as `service list --ended --all-threads` shows it
    fn service_view(&self, id: &str) -> Value {
        let out = self
            .service_cmd(None)
            .args(["list", "--ended", "--all-threads"])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let listed: Value = serde_json::from_slice(&out.stdout).unwrap();
        listed["services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|service| service["id"] == id)
            .unwrap_or_else(|| panic!("{id} is not listed: {listed}"))
            .clone()
    }

    /// Why `id` ended, once it has
    fn service_end(&self, id: &str) -> Option<String> {
        self.service_view(id)["end"].as_str().map(str::to_owned)
    }

    /// `SERVICE_EXITED` lines normal delivery sent for `id`
    fn service_events(&self, id: &str) -> Vec<Value> {
        self.queue_messages()
            .iter()
            .map(|line| event_json(line))
            .filter(|event| event["event"] == "SERVICE_EXITED" && event["service"] == id)
            .collect()
    }

    /// How the event of `id` settled, read from the store
    fn service_event_delivery(&self, id: &str) -> (bool, Option<String>) {
        let db = rusqlite::Connection::open(self.home.join(homebased::home::DB_NAME)).unwrap();
        db.query_row(
            "SELECT event_due, event_delivery FROM services WHERE id = ?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }
}

/// A port nothing listens on right now
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn refused(out: &Output, code: &str) -> Value {
    assert_ne!(out.status.code(), Some(0), "start should fail");
    let error: Value = serde_json::from_slice(&out.stderr)
        .unwrap_or_else(|_| panic!("{}", String::from_utf8_lossy(&out.stderr)));
    assert_eq!(error["error"]["code"], code, "{error}");
    error
}

fn pid(view: &Value) -> i32 {
    i32::try_from(view["pid"].as_i64().unwrap()).unwrap()
}

#[test]
fn a_port_service_is_ready_once_it_listens_and_stops_without_an_event() {
    let h = Harness::new();
    let port = free_port().to_string();
    let id = h.start_service(
        Some(THREAD),
        &[
            "--name",
            "fixtures",
            "--port",
            &port,
            "--",
            "python3",
            "-m",
            "http.server",
            &port,
            "--bind",
            "127.0.0.1",
        ],
    );
    // ready means it answers now
    std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    let view = h.service_view(&id);
    assert_eq!(view["state"], "ready");
    let server = pid(&view);
    assert!(process_is_live(server));

    let out = h
        .service_cmd(Some(THREAD))
        .args(["stop", &id])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stopped: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(stopped["state"], "ended", "{stopped}");
    assert_eq!(stopped["end"], "stopped");
    assert!(!process_is_live(server), "the server outlived its stop");
    assert!(std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_err());

    // stopping again changes nothing
    let again = h
        .service_cmd(Some(THREAD))
        .args(["stop", &id])
        .output()
        .unwrap();
    assert!(again.status.success());
    let again: Value = serde_json::from_slice(&again.stdout).unwrap();
    assert_eq!(again["end"], "stopped");

    std::thread::sleep(Duration::from_secs(2));
    assert!(h.service_events(&id).is_empty(), "{:?}", h.queue_messages());
    assert_eq!(h.service_event_delivery(&id), (false, None));
}

#[test]
fn a_ready_line_counts_once_matched_and_the_service_sees_the_caller_environment() {
    let h = Harness::new();
    let id = {
        let out = h
            .service_cmd(Some(THREAD))
            .env("SERVICE_PROBE", "from-the-agent-shell")
            .args([
                "start",
                "--name",
                "liner",
                "--ready-line",
                r"^listening with from-the-agent-shell$",
                "--",
                "sh",
                "-c",
                "echo booting; sleep 1; printf '\\033[32mlistening with %s\\033[0m\\n' \"$SERVICE_PROBE\"; exec sleep 60",
            ])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let view: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(view["state"], "ready", "{view}");
        view["id"].as_str().unwrap().to_owned()
    };

    let log = h
        .service_cmd(None)
        .args(["log", &id, "--tail", "5"])
        .output()
        .unwrap();
    let log: Value = serde_json::from_slice(&log.stdout).unwrap();
    assert!(
        log["log"]
            .as_str()
            .unwrap()
            .contains("from-the-agent-shell"),
        "{log}"
    );

    // the caller's own services are listed by default, a stranger's are not
    let mine = h.service_cmd(Some(THREAD)).arg("list").output().unwrap();
    let mine: Value = serde_json::from_slice(&mine.stdout).unwrap();
    assert_eq!(mine["services"].as_array().unwrap().len(), 1, "{mine}");
    let other = h
        .service_cmd(Some("01a0ab97-a7aa-7463-a5b0-8d500e40e432"))
        .arg("list")
        .output()
        .unwrap();
    let other: Value = serde_json::from_slice(&other.stdout).unwrap();
    assert!(other["services"].as_array().unwrap().is_empty(), "{other}");

    h.service_cmd(None).args(["stop", &id]).output().unwrap();
    assert_eq!(h.service_end(&id).as_deref(), Some("stopped"));
}

#[test]
fn a_service_that_is_not_ready_in_time_or_exits_first_is_stopped_and_start_fails() {
    let h = Harness::new();
    let port = free_port().to_string();
    let out = h
        .service_cmd(Some(THREAD))
        .args([
            "start",
            "--name",
            "silent",
            "--port",
            &port,
            "--ready-timeout",
            "1s",
            "--",
            "sh",
            "-c",
            "echo never listens; exec sleep 60",
        ])
        .output()
        .unwrap();
    let error = refused(&out, "service_not_ready");
    let input = &error["error"]["input"];
    assert!(
        input["reason"]
            .as_str()
            .unwrap()
            .contains("not ready within 1s"),
        "{error}"
    );
    assert_eq!(input["output_tail"][0], "never listens", "{error}");
    let id = input["service"].as_str().unwrap();
    let view = h.service_view(id);
    assert_eq!(view["end"], "not_ready");
    assert!(
        !process_is_live(pid(&view)),
        "the unready service kept running"
    );

    let out = h
        .service_cmd(Some(THREAD))
        .args([
            "start",
            "--name",
            "crasher",
            "--port",
            &port,
            "--",
            "sh",
            "-c",
            "echo boom; exit 3",
        ])
        .output()
        .unwrap();
    let error = refused(&out, "service_not_ready");
    let input = &error["error"]["input"];
    assert_eq!(input["output_tail"][0], "boom", "{error}");
    let crashed = input["service"].as_str().unwrap();
    assert_eq!(h.service_view(crashed)["exit"]["code"], 3);

    // with no readiness check, a command that dies at once, such as on a bad
    // flag, fails `start` rather than passing it and sending an event after
    let out = h
        .service_cmd(Some(THREAD))
        .args([
            "start",
            "--name",
            "bad-flag",
            "--",
            "sh",
            "-c",
            "echo unknown flag; exit 2",
        ])
        .output()
        .unwrap();
    let error = refused(&out, "service_not_ready");
    let bad_flag = error["error"]["input"]["service"].as_str().unwrap();
    assert_eq!(h.service_view(bad_flag)["exit"]["code"], 2);

    // a failed start is reported to `start` itself, never as an event
    std::thread::sleep(Duration::from_secs(2));
    assert!(h.service_events(id).is_empty());
    assert!(h.service_events(crashed).is_empty());
    assert!(h.service_events(bad_flag).is_empty());
}

#[test]
fn a_service_that_exits_on_its_own_sends_its_owner_one_event() {
    let h = Harness::new();
    let id = h.start_service(
        Some(THREAD),
        &[
            "--name",
            "flaky",
            "--",
            "sh",
            "-c",
            "echo serving; sleep 3; echo out of memory; exit 7",
        ],
    );
    assert!(
        wait_until(Duration::from_secs(20), || !h
            .service_events(&id)
            .is_empty()),
        "no SERVICE_EXITED: {:?}",
        h.queue_messages()
    );
    std::thread::sleep(Duration::from_secs(2));
    let events = h.service_events(&id);
    assert_eq!(events.len(), 1, "{events:?}");
    let event = &events[0];
    assert_eq!(event["thread"], THREAD);
    assert_eq!(event["end"], "exited");
    assert_eq!(event["exit"]["code"], 7);
    assert_eq!(event["output_tail"][1], "out of memory", "{event}");
    assert!(
        event["next_action"]
            .as_str()
            .unwrap()
            .contains("homebased service log"),
        "{event}"
    );
    assert_eq!(
        h.service_event_delivery(&id),
        (true, Some("delivered".into()))
    );
}

#[test]
fn a_service_stops_without_an_event_once_its_max_lifetime_elapses() {
    let h = Harness::new();
    let id = h.start_service(
        Some(THREAD),
        &["--name", "short", "--max", "2s", "--", "sleep", "60"],
    );
    let server = pid(&h.service_view(&id));
    assert!(
        wait_until(Duration::from_secs(15), || h.service_end(&id).is_some()),
        "the service outlived --max"
    );
    assert_eq!(h.service_end(&id).as_deref(), Some("max_elapsed"));
    assert!(!process_is_live(server));
    std::thread::sleep(Duration::from_secs(2));
    assert!(h.service_events(&id).is_empty());
}

#[test]
fn services_stop_without_an_event_once_the_owners_t3_thread_is_archived_or_settled() {
    const ARCHIVED: &str = "mcp:launch:service-archived";
    const SETTLED: &str = "mcp:launch:service-settled";
    const SETTLED_SESSION: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e499";
    let mut h = Harness::new();
    let cwd = h.user_home.clone();
    super::register_thread(&h.user_home, SETTLED_SESSION);
    h.t3_thread(ARCHIVED, "archived owner", Some(THREAD));
    h.codex_session(THREAD, &cwd, 20_000);
    h.t3_thread(SETTLED, "settled owner", Some(SETTLED_SESSION));
    h.codex_session(SETTLED_SESSION, &cwd, 20_000);
    h.restart_daemon_with_env(&[("HOMEBASED_TEST_SCAN_SECS", "1")]);

    let archived = h.start_service(Some(THREAD), &["--name", "a", "--", "sleep", "120"]);
    let settled = h.start_service(
        Some(SETTLED_SESSION),
        &["--name", "s", "--", "sleep", "120"],
    );
    let detached = h.start_service(None, &["--name", "d", "--", "sleep", "120"]);
    // several scans of live owners stop nothing
    std::thread::sleep(Duration::from_secs(3));
    for id in [&archived, &settled, &detached] {
        assert_eq!(h.service_end(id), None, "{id} ended early");
    }

    h.archive_t3_thread(ARCHIVED);
    h.settle_t3_thread(SETTLED, true);
    for id in [&archived, &settled] {
        assert!(
            wait_until(Duration::from_secs(20), || h.service_end(id).is_some()),
            "{id} kept running after its owner was done"
        );
        assert_eq!(h.service_end(id).as_deref(), Some("owner_done"));
        assert!(!process_is_live(pid(&h.service_view(id))));
    }
    // a service with no T3 owner is left to `stop` and `--max`
    assert_eq!(h.service_end(&detached), None);
    h.service_cmd(None)
        .args(["stop", &detached])
        .output()
        .unwrap();

    std::thread::sleep(Duration::from_secs(2));
    assert!(h.service_events(&archived).is_empty());
    assert!(h.service_events(&settled).is_empty());
}
