//! A fake EC2 instance metadata service, version 2, for pool tests
//!
//! Each machine gets its own endpoint, `{base}/{name}`, so a test gives one
//! machine a Spot interruption notice while the others see none. A read
//! without the session token from `PUT /latest/api/token` is refused, as
//! IMDSv2 refuses it
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use chrono::{DateTime, Utc};

const TOKEN: &str = "imds-session-token";

#[derive(Default)]
struct State {
    /// Machines with a notice, and when the notice says they end
    noticed: BTreeSet<String>,
    ends_at: Option<DateTime<Utc>>,
    /// Polls of the instance action, any machine
    polls: u64,
}

/// The running fake
pub struct FakeImds {
    base: String,
    state: Arc<Mutex<State>>,
}

impl FakeImds {
    /// Serve on a free loopback port
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let state = shared.clone();
                thread::spawn(move || serve(stream, &state));
            }
        });
        Self { base, state }
    }

    /// Base of every machine's endpoint, which is `{base}/{name}`
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Give machine `name` a notice that it ends at `ends_at`
    pub fn interrupt(&self, name: &str, ends_at: DateTime<Utc>) {
        let mut state = self.state.lock().unwrap();
        state.noticed.insert(name.to_owned());
        state.ends_at = Some(ends_at);
    }

    /// How many times any machine read its instance action
    pub fn polls(&self) -> u64 {
        self.state.lock().unwrap().polls
    }
}

fn serve(stream: TcpStream, state: &Mutex<State>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let mut words = line.split_whitespace();
    let method = words.next().unwrap_or_default().to_owned();
    let path = words.next().unwrap_or_default().to_owned();
    let mut token = None;
    let mut ttl = None;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).is_err() || header.trim().is_empty() {
            break;
        }
        let (name, value) = header.split_once(':').unwrap_or_default();
        match name.to_ascii_lowercase().as_str() {
            "x-aws-ec2-metadata-token" => token = Some(value.trim().to_owned()),
            "x-aws-ec2-metadata-token-ttl-seconds" => ttl = Some(value.trim().to_owned()),
            _ => {}
        }
    }
    let parts: Vec<&str> = path.trim_start_matches('/').splitn(2, '/').collect();
    let (machine, rest) = match parts.as_slice() {
        [machine, rest] => (*machine, *rest),
        _ => ("", ""),
    };
    let (status, body) = match (method.as_str(), rest) {
        ("PUT", "latest/api/token") if ttl.is_some() => (200, TOKEN.to_owned()),
        ("GET", "latest/meta-data/spot/instance-action") if token.as_deref() == Some(TOKEN) => {
            let mut state = state.lock().unwrap();
            state.polls += 1;
            match state.ends_at.filter(|_| state.noticed.contains(machine)) {
                Some(at) => (
                    200,
                    format!(
                        r#"{{"action":"terminate","time":"{}"}}"#,
                        at.format("%Y-%m-%dT%H:%M:%SZ")
                    ),
                ),
                None => (404, String::new()),
            }
        }
        ("GET", _) => (401, String::new()),
        _ => (405, String::new()),
    };
    let mut writer = stream;
    let _ = write!(
        writer,
        "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}
