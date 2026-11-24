//! A fake cloudlease API for pool tests
//!
//! It keeps runs and leases in memory and follows the contract's lifetime
//! rules on every read: a lease past its heartbeat or hard deadline starts
//! releasing, and a releasing lease is confirmed gone on the next read, as a
//! provider that deletes at once would. Tests read its leases and boot data
//! to "boot" machines as real daemons

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use chrono::{DateTime, Duration, Utc};
use serde_json::{Value, json};

pub const CLIENT_TOKEN: &str = "client-token";

/// One lease as the fake holds it
#[derive(Debug, Clone)]
pub struct FakeLease {
    pub lease_id: String,
    pub run_id: String,
    pub request_id: String,
    pub hostname: String,
    pub token: String,
    pub boot: Value,
    pub state: &'static str,
    pub created_at: DateTime<Utc>,
    pub hard_deadline_at: DateTime<Utc>,
    pub heartbeat_deadline_at: DateTime<Utc>,
    pub release: Option<(String, &'static str)>,
    pub gone_at: Option<DateTime<Utc>>,
    pub renewals: u32,
    /// While the provider still works on a slow create: when it answers,
    /// and whether it fails
    pub provisioning: Option<(DateTime<Utc>, bool)>,
}

#[derive(Debug, Clone)]
struct FakeRun {
    run_id: String,
    request_id: String,
    name: String,
    class: String,
    max_machines: u64,
    deadline_at: DateTime<Utc>,
    spend_cap_cents: Option<u64>,
    state: &'static str,
    created_at: DateTime<Utc>,
}

#[derive(Default)]
struct State {
    runs: BTreeMap<String, FakeRun>,
    leases: Vec<FakeLease>,
    next: u64,
    /// New machine creates the provider fails, answered `201` with the
    /// lease already releasing, as cloudlease does
    failing_creates: u32,
    /// Status and code every new machine create is refused with
    refuse_creates: Option<(u16, &'static str)>,
    /// New machine creates answered `503 busy` while the provider is still
    /// working, as when cloudlease's answer is lost; the lease shows
    /// `provisioning` for a few seconds before its outcome
    slow_creates: u32,
}

/// The running fake
pub struct FakeCloudlease {
    base: String,
    state: Arc<Mutex<State>>,
    heartbeat_ttl: Duration,
}

impl FakeCloudlease {
    /// Serve on a free loopback port; renewals push the heartbeat out by `heartbeat_ttl`
    pub fn start(heartbeat_ttl: std::time::Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State::default()));
        let fake = Self {
            base,
            state: state.clone(),
            heartbeat_ttl: Duration::from_std(heartbeat_ttl).unwrap(),
        };
        let ttl = fake.heartbeat_ttl;
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let state = state.clone();
                thread::spawn(move || serve(stream, &state, ttl));
            }
        });
        fake
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// Lose every run and lease, as a cloudlease that no longer has them
    pub fn forget_runs(&self) {
        let mut state = self.state.lock().unwrap();
        state.runs.clear();
        state.leases.clear();
    }

    /// Let the provider fail the next `count` machine creates
    pub fn fail_creates(&self, count: u32) {
        self.state.lock().unwrap().failing_creates = count;
    }

    /// Lose the answers of the next `count` machine creates, which stay
    /// `provisioning` for a few seconds
    pub fn slow_creates(&self, count: u32) {
        self.state.lock().unwrap().slow_creates = count;
    }

    /// Refuse every new machine create with `status` and `code`, as a run
    /// whose class was repriced (`409 run_ended`) or a reached spend cap
    pub fn refuse_creates(&self, status: u16, code: &'static str) {
        self.state.lock().unwrap().refuse_creates = Some((status, code));
    }

    /// End a lease from cloudlease's side, as a spend cap would
    pub fn end_lease(&self, lease_id: &str, reason: &str) {
        let mut state = self.state.lock().unwrap();
        let lease = state
            .leases
            .iter_mut()
            .find(|lease| lease.lease_id == lease_id)
            .unwrap();
        lease.state = "releasing";
        lease.release = Some((reason.into(), "cloudlease"));
    }

    /// `spend_cap_cents` of each run create, by run name
    pub fn spend_caps(&self) -> BTreeMap<String, Option<u64>> {
        let state = self.state.lock().unwrap();
        state
            .runs
            .values()
            .map(|run| (run.name.clone(), run.spend_cap_cents))
            .collect()
    }

    /// Every lease, oldest first, after applying the lifetime rules
    pub fn leases(&self) -> Vec<FakeLease> {
        let mut state = self.state.lock().unwrap();
        advance(&mut state);
        state.leases.clone()
    }
}

/// Apply heartbeat and hard deadlines, then confirm releasing leases gone
fn advance(state: &mut State) {
    let now = Utc::now();
    for lease in &mut state.leases {
        if let Some((until, failed)) = lease.provisioning {
            if now < until {
                continue;
            }
            lease.provisioning = None;
            lease.state = "running";
            if failed {
                lease.state = "releasing";
                lease.release = Some(("create_failed".into(), "cloudlease"));
            }
            continue;
        }
        if lease.state == "releasing" {
            lease.state = "released";
            lease.gone_at = Some(now);
            continue;
        }
        if lease.state == "released" {
            continue;
        }
        let reason = if now >= lease.hard_deadline_at {
            Some("hard_deadline")
        } else if now >= lease.heartbeat_deadline_at {
            Some("heartbeat_lapsed")
        } else {
            None
        };
        if let Some(reason) = reason {
            lease.state = "releasing";
            lease.release = Some((reason.into(), "cloudlease"));
        }
    }
}

fn lease_json(lease: &FakeLease) -> Value {
    json!({
        "lease_id": lease.lease_id,
        "run_id": lease.run_id,
        "request_id": lease.request_id,
        "hostname": lease.hostname,
        "state": lease.state,
        "created_at": lease.created_at,
        "hard_deadline_at": lease.hard_deadline_at,
        "heartbeat_deadline_at": lease.heartbeat_deadline_at,
        "release": lease.release.as_ref().map(|(reason, by)| json!({
            "reason": reason, "by": by, "at": Utc::now(),
        })),
        "gone_at": lease.gone_at,
    })
}

fn run_json(state: &State, run: &FakeRun) -> Value {
    json!({
        "run_id": run.run_id,
        "name": run.name,
        "class": run.class,
        "max_machines": run.max_machines,
        "deadline_at": run.deadline_at,
        "state": run.state,
        "created_at": run.created_at,
        "machines": state.leases.iter()
            .filter(|lease| lease.run_id == run.run_id)
            .map(lease_json)
            .collect::<Vec<_>>(),
    })
}

fn error(status: u16, code: &str) -> (u16, Value) {
    (
        status,
        json!({ "error": { "code": code, "message": code, "retryable": false } }),
    )
}

fn route(
    state: &mut State,
    ttl: Duration,
    method: &str,
    path: &str,
    bearer: &str,
    body: &Value,
) -> (u16, Value) {
    advance(state);
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let client = bearer == CLIENT_TOKEN;
    match (method, parts.as_slice()) {
        ("POST", ["v1", "runs"]) if client => {
            let request = body["request_id"].as_str().unwrap_or_default().to_owned();
            if let Some(run) = state.runs.values().find(|run| run.request_id == request) {
                return (200, run_json(state, run));
            }
            state.next += 1;
            let run = FakeRun {
                run_id: format!("run-{}", state.next),
                request_id: request,
                name: body["name"].as_str().unwrap_or_default().into(),
                class: body["class"].as_str().unwrap_or_default().into(),
                max_machines: body["max_machines"].as_u64().unwrap_or(0),
                deadline_at: serde_json::from_value(body["deadline_at"].clone()).unwrap(),
                spend_cap_cents: body["spend_cap_cents"].as_u64(),
                state: "open",
                created_at: Utc::now(),
            };
            state.runs.insert(run.run_id.clone(), run.clone());
            (201, run_json(state, &run))
        }
        ("GET", ["v1", "runs", run]) if client => match state.runs.get(*run) {
            Some(run) => (200, run_json(state, run)),
            None => error(404, "run_not_found"),
        },
        ("POST", ["v1", "runs", run, "release"]) if client => {
            let Some(found) = state.runs.get_mut(*run) else {
                return error(404, "run_not_found");
            };
            found.state = "releasing";
            for lease in state.leases.iter_mut().filter(|lease| lease.run_id == *run) {
                if lease.state != "released" && lease.state != "releasing" {
                    lease.state = "releasing";
                    lease.release = Some(("run_released".into(), "client"));
                }
            }
            (202, json!({}))
        }
        ("POST", ["v1", "runs", run, "machines"]) if client => {
            let Some(found) = state.runs.get(*run).cloned() else {
                return error(404, "run_not_found");
            };
            let request = body["request_id"].as_str().unwrap_or_default().to_owned();
            if let Some(lease) = state
                .leases
                .iter()
                .find(|lease| lease.request_id == request)
            {
                return (200, lease_json(lease));
            }
            if found.state != "open" {
                return error(409, "run_ended");
            }
            if let Some((status, code)) = state.refuse_creates {
                return error(status, code);
            }
            let live = state
                .leases
                .iter()
                .filter(|lease| lease.run_id == found.run_id && lease.state != "released")
                .count() as u64;
            if live >= found.max_machines {
                return error(409, "run_full");
            }
            state.next += 1;
            let now = Utc::now();
            let mut lease = FakeLease {
                lease_id: format!("lease-{}", state.next),
                run_id: found.run_id.clone(),
                request_id: request,
                hostname: body["hostname"].as_str().unwrap_or_default().into(),
                token: format!("lease-token-{}", state.next),
                boot: body["boot"].clone(),
                state: "running",
                created_at: now,
                hard_deadline_at: found.deadline_at,
                // boot grace before the first renewal
                heartbeat_deadline_at: now + Duration::seconds(60),
                release: None,
                gone_at: None,
                renewals: 0,
                provisioning: None,
            };
            let failed = state.failing_creates > 0;
            state.failing_creates = state.failing_creates.saturating_sub(1);
            if state.slow_creates > 0 {
                state.slow_creates -= 1;
                lease.state = "provisioning";
                lease.provisioning = Some((now + Duration::seconds(3), failed));
                state.leases.push(lease);
                return error(503, "busy");
            }
            if failed {
                lease.state = "releasing";
                lease.release = Some(("create_failed".into(), "cloudlease"));
            }
            let answer = lease_json(&lease);
            state.leases.push(lease);
            (201, answer)
        }
        ("POST", ["v1", "leases", lease, action]) => {
            let Some(found) = state
                .leases
                .iter_mut()
                .find(|found| found.lease_id == *lease)
            else {
                return error(404, "lease_not_found");
            };
            let own = bearer == found.token;
            match *action {
                "renew" if own => {
                    if found.state != "running" {
                        return error(410, "lease_ended");
                    }
                    found.renewals += 1;
                    found.heartbeat_deadline_at = (Utc::now() + ttl).min(found.hard_deadline_at);
                    (
                        200,
                        json!({
                            "heartbeat_deadline_at": found.heartbeat_deadline_at,
                            "hard_deadline_at": found.hard_deadline_at,
                        }),
                    )
                }
                "release" if own || client => {
                    if found.state == "running" {
                        found.state = "releasing";
                        let reason = body["reason"].as_str().unwrap_or("requested").to_owned();
                        found.release = Some((reason, if own { "machine" } else { "client" }));
                    }
                    (202, lease_json(found))
                }
                _ => error(401, "unauthorized"),
            }
        }
        _ => error(401, "unauthorized"),
    }
}

fn serve(stream: TcpStream, state: &Mutex<State>, ttl: Duration) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let mut words = line.split_whitespace();
    let method = words.next().unwrap_or_default().to_owned();
    let path = words.next().unwrap_or_default().to_owned();
    let mut length = 0;
    let mut bearer = String::new();
    let mut expect_continue = false;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).is_err() || header.trim().is_empty() {
            break;
        }
        let (name, value) = header.split_once(':').unwrap_or_default();
        let value = value.trim();
        match name.to_ascii_lowercase().as_str() {
            "content-length" => length = value.parse().unwrap_or(0),
            "authorization" => bearer = value.trim_start_matches("Bearer ").to_owned(),
            "expect" => expect_continue = value.eq_ignore_ascii_case("100-continue"),
            _ => {}
        }
    }
    let mut writer = stream;
    if expect_continue {
        let _ = writer.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
    }
    let mut body = vec![0; length];
    if reader.read_exact(&mut body).is_err() {
        return;
    }
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let (status, answer) = route(
        &mut state.lock().unwrap(),
        ttl,
        &method,
        &path,
        &bearer,
        &body,
    );
    let text = answer.to_string();
    let _ = write!(
        writer,
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
        text.len()
    );
}
