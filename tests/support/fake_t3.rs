//! A fake T3 server and the T3 user data that points a daemon at it
//!
//! It answers the environment descriptor, websocket tickets, `server.getConfig`,
//! `orchestration.launchThread`, and `orchestration.dispatchCommand`, and
//! records each RPC. With a state database it also does what T3 would to it:
//! a launch adds the thread, and `thread.settle` settles it, so the daemon's
//! own reads see the change

// each test binary that includes this uses only part of it
#![allow(dead_code)]

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::{Value, json};
use tungstenite::Message;

/// What the fake does with requests, which a test can change while it runs
#[derive(Default)]
pub struct Behavior {
    /// Close each launch socket without a reply, as if the reply were lost
    pub drop_launch_replies: AtomicBool,
    /// Close each `message.dispatch` socket without a reply
    pub drop_message_replies: AtomicBool,
    /// Refuse `thread.settle` with T3's typed dispatch error
    pub refuse_settle: AtomicBool,
}

/// One running fake T3 server
pub struct FakeT3 {
    /// `http://127.0.0.1:<port>`
    pub origin: String,
    pub behavior: Arc<Behavior>,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl FakeT3 {
    /// Serve, applying launches and settles to the state database at
    /// `state` when given
    pub fn start(state: Option<PathBuf>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let behavior = Arc::new(Behavior::default());
        let server = Server {
            requests: Arc::clone(&requests),
            behavior: Arc::clone(&behavior),
            state,
        };
        let stopped = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            while !stopped.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => server.serve(stream),
                    Err(_) => thread::sleep(Duration::from_millis(5)),
                }
            }
        });
        Self {
            origin,
            behavior,
            requests,
            stop,
            worker: Some(worker),
        }
    }

    /// Every launch request, in order
    pub fn launches(&self) -> Vec<Value> {
        self.rpcs("orchestration.launchThread")
    }

    /// Every dispatched command payload of `kind`, such as `thread.settle`
    pub fn dispatched(&self, kind: &str) -> Vec<Value> {
        self.rpcs("orchestration.dispatchCommand")
            .into_iter()
            .map(|request| request["payload"].clone())
            .filter(|payload| payload["type"] == kind)
            .collect()
    }

    fn rpcs(&self, tag: &str) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["tag"] == tag)
            .cloned()
            .collect()
    }
}

impl Drop for FakeT3 {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct Server {
    requests: Arc<Mutex<Vec<Value>>>,
    behavior: Arc<Behavior>,
    state: Option<PathBuf>,
}

impl Server {
    fn serve(&self, stream: TcpStream) {
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut head = [0_u8; 2048];
        let Ok(read) = stream.peek(&mut head) else {
            return;
        };
        let head = String::from_utf8_lossy(&head[..read]).into_owned();
        if head.starts_with("GET /ws?") {
            self.serve_rpc(stream);
            return;
        }

        let body = if head.starts_with("GET /.well-known/t3/environment") {
            json!({ "orchestrationProtocolVersion": 2 })
        } else if head.starts_with("POST /api/auth/websocket-ticket") {
            json!({ "ticket": "fake-ticket" })
        } else {
            json!({ "error": "unexpected request" })
        };
        respond(stream, &body);
    }

    fn serve_rpc(&self, stream: TcpStream) {
        let Ok(mut socket) = tungstenite::accept(stream) else {
            return;
        };
        let Ok(Message::Text(text)) = socket.read() else {
            return;
        };
        let request: Value = serde_json::from_str(text.as_str()).unwrap();
        self.requests.lock().unwrap().push(request.clone());
        let payload = &request["payload"];
        let reply = match request["tag"].as_str() {
            Some("server.getConfig") => Some(success(json!({ "providers": [
                { "instanceId": "codex", "driver": "codex", "enabled": true },
                { "instanceId": "claudeAgent", "driver": "claudeAgent", "enabled": false }
            ]}))),
            Some("orchestration.launchThread") => {
                if self.behavior.drop_launch_replies.load(Ordering::Relaxed) {
                    None
                } else {
                    self.launched(payload);
                    Some(success(json!({
                        "threadId": payload["threadId"], "projection": {}, "resumed": false
                    })))
                }
            }
            Some("orchestration.dispatchCommand") => self.dispatch(payload),
            _ => Some(json!({ "_tag": "Exit", "requestId": "1", "exit": {
                "_tag": "Failure", "cause": [{ "_tag": "Die" }]
            }})),
        };
        if let Some(reply) = reply {
            let _ = socket.send(Message::text(reply.to_string()));
        } else {
            let _ = socket.close(None);
        }
        while socket.read().is_ok() {}
    }

    fn dispatch(&self, payload: &Value) -> Option<Value> {
        match payload["type"].as_str() {
            Some("message.dispatch")
                if self.behavior.drop_message_replies.load(Ordering::Relaxed) =>
            {
                None
            }
            Some("thread.settle") if self.behavior.refuse_settle.load(Ordering::Relaxed) => {
                Some(json!({ "_tag": "Exit", "requestId": "1", "exit": {
                    "_tag": "Failure",
                    "cause": [{ "_tag": "Fail", "error": {
                        "_tag": "OrchestrationV2DispatchCommandError"
                    }}]
                }}))
            }
            Some("thread.settle") => {
                self.with_state(|db| {
                    db.execute(
                        "UPDATE orchestration_v2_projection_threads
                         SET payload_json = json_set(payload_json, '$.settledOverride', 'settled')
                         WHERE thread_id = ?1",
                        [payload["threadId"].as_str().unwrap_or_default()],
                    )
                });
                Some(success(json!({})))
            }
            // T3 ends the run, as its stop buttons do
            Some("run.interrupt" | "thread.stop") => {
                self.with_state(|db| {
                    db.execute(
                        "UPDATE orchestration_v2_projection_runs SET status = 'interrupted'
                         WHERE thread_id = ?1 AND status IN ('preparing', 'starting', 'running', 'waiting')",
                        [payload["threadId"].as_str().unwrap_or_default()],
                    )
                });
                Some(success(json!({})))
            }
            Some("runtime-request.respond") => {
                self.with_state(|db| {
                    db.execute(
                        "UPDATE orchestration_v2_projection_runtime_requests SET status = 'resolved'
                         WHERE runtime_request_id = ?1",
                        [payload["requestId"].as_str().unwrap_or_default()],
                    )
                });
                Some(success(json!({})))
            }
            _ => Some(success(json!({}))),
        }
    }

    /// Add the launched thread to the state, as T3 does before it replies
    fn launched(&self, payload: &Value) {
        let thread = payload["threadId"].as_str().unwrap_or_default();
        let title = payload["title"].as_str().unwrap_or("launched");
        self.with_state(|db| {
            db.execute(
                "INSERT OR IGNORE INTO orchestration_v2_projection_threads (thread_id, title)
                 VALUES (?1, ?2)",
                [thread, title],
            )
        });
    }

    fn with_state(&self, change: impl FnOnce(&rusqlite::Connection) -> rusqlite::Result<usize>) {
        let Some(path) = &self.state else {
            return;
        };
        let db = rusqlite::Connection::open(path).unwrap();
        change(&db).unwrap();
    }
}

fn success(value: Value) -> Value {
    json!({
        "_tag": "Exit",
        "requestId": "1",
        "exit": { "_tag": "Success", "value": value }
    })
}

fn respond(mut stream: TcpStream, body: &Value) {
    // drain the request so closing the socket does not reset it
    let mut request = [0_u8; 4096];
    let _ = stream.read(&mut request);
    let body = body.to_string();
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

/// The tables of T3's state database that homebased reads, laid out as T3's
/// projections lay them out
pub const STATE_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS orchestration_v2_projection_threads (
        thread_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL DEFAULT 'project',
        title TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT '2026-10-02T19:00:00Z',
        default_provider TEXT NOT NULL DEFAULT 'codex',
        active_provider_thread_id TEXT,
        provider_instance_id TEXT,
        archived_at TEXT,
        deleted_at TEXT,
        payload_json TEXT NOT NULL DEFAULT '{}'
    );
    CREATE TABLE IF NOT EXISTS projection_projects (
        project_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        workspace_root TEXT NOT NULL,
        deleted_at TEXT,
        default_model_selection_json TEXT
    );
    CREATE TABLE IF NOT EXISTS orchestration_v2_projection_provider_threads (
        provider_thread_id TEXT PRIMARY KEY,
        thread_id TEXT,
        provider TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        driver TEXT,
        provider_instance_id TEXT
    );
    CREATE TABLE IF NOT EXISTS orchestration_v2_projection_runs (
        run_id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        provider TEXT NOT NULL DEFAULT 'codex',
        status TEXT NOT NULL,
        requested_at TEXT NOT NULL DEFAULT '2026-10-02T20:00:00Z',
        payload_json TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS orchestration_v2_projection_runtime_requests (
        runtime_request_id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        node_id TEXT,
        kind TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT '2026-10-02T20:00:00Z',
        payload_json TEXT NOT NULL DEFAULT '{}'
    );
    CREATE TABLE IF NOT EXISTS orchestration_v2_projection_turn_items (
        item_id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        type TEXT NOT NULL,
        payload_json TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS orchestration_v2_projection_messages (
        message_id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user',
        created_at TEXT NOT NULL DEFAULT '2026-10-02T20:00:00Z',
        updated_at TEXT NOT NULL DEFAULT '2026-10-02T20:00:00Z',
        payload_json TEXT NOT NULL
    );";

/// T3's state database at `path`, with [`STATE_TABLES`] made where missing
pub fn open_state(path: &Path) -> rusqlite::Connection {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let db = rusqlite::Connection::open(path).unwrap();
    db.execute_batch(STATE_TABLES).unwrap();
    db
}

/// Make the Codex thread `session` the one T3 runs `thread` in next
pub fn set_session(db: &rusqlite::Connection, thread: &str, session: &str) {
    let id = format!("provider-thread:{session}");
    let payload = json!({ "nativeThreadRef": { "driver": "codex", "nativeId": session } });
    db.execute(
        "INSERT INTO orchestration_v2_projection_provider_threads
         (provider_thread_id, thread_id, provider, driver, provider_instance_id, updated_at, payload_json)
         VALUES (?1, ?2, 'codex', 'codex', 'codex', '2026-10-02T20:00:00Z', ?3)",
        rusqlite::params![id, thread, payload.to_string()],
    )
    .unwrap();
    db.execute(
        "UPDATE orchestration_v2_projection_threads
         SET active_provider_thread_id = ?1, provider_instance_id = 'codex' WHERE thread_id = ?2",
        rusqlite::params![id, thread],
    )
    .unwrap();
}

/// Add the run `run` of `thread` after its others, or move it to `status`
pub fn set_run(db: &rusqlite::Connection, thread: &str, run: &str, status: &str) {
    db.execute(
        "INSERT INTO orchestration_v2_projection_runs (run_id, thread_id, ordinal, status, payload_json)
         VALUES (?1, ?2,
                 (SELECT COALESCE(MAX(ordinal), 0) + 1 FROM orchestration_v2_projection_runs
                  WHERE thread_id = ?2),
                 ?3, '{}')
         ON CONFLICT (run_id) DO UPDATE SET status = excluded.status",
        [run, thread, status],
    )
    .unwrap();
}

/// Make `thread` wait on the question `request`, with T3's card for it
pub fn ask(db: &rusqlite::Connection, thread: &str, request: &str, questions: &Value) {
    db.execute(
        "INSERT INTO orchestration_v2_projection_runtime_requests
         (runtime_request_id, thread_id, kind, status) VALUES (?1, ?2, 'user_input', 'pending')",
        [request, thread],
    )
    .unwrap();
    let card = json!({ "requestId": request, "questions": questions });
    db.execute(
        "INSERT INTO orchestration_v2_projection_turn_items (item_id, thread_id, type, payload_json)
         VALUES (?1, ?2, 'user_input_request', ?3)",
        rusqlite::params![format!("item-{request}"), thread, card.to_string()],
    )
    .unwrap();
}

/// T3 user data for `home`: a running server at `origin`, one project at
/// `~/code/app`, a default model, and a `t3` CLI that issues fake tokens.
/// Returns the state database's path
pub fn install_t3(home: &Path, origin: &str) -> PathBuf {
    let userdata = home.join(".t3/userdata");
    fs::create_dir_all(&userdata).unwrap();
    let runtime = json!({
        "version": 1,
        "pid": std::process::id(),
        "origin": origin,
        "startedAt": "2026-10-20T12:00:00.000Z",
        "serviceManaged": true
    });
    fs::write(userdata.join("server-runtime.json"), runtime.to_string()).unwrap();
    fs::write(userdata.join("environment-id"), "executor-env\n").unwrap();
    fs::write(
        userdata.join("settings.json"),
        json!({ "defaultModelSelection": { "instanceId": "codex", "model": "gpt-6.1-sol" } })
            .to_string(),
    )
    .unwrap();

    let root = home.join("code/app");
    fs::create_dir_all(&root).unwrap();
    let state = userdata.join("statev2.sqlite");
    let db = rusqlite::Connection::open(&state).unwrap();
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS projection_projects (
            project_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            workspace_root TEXT NOT NULL,
            deleted_at TEXT,
            default_model_selection_json TEXT
         );",
    )
    .unwrap();
    db.execute(
        "INSERT OR IGNORE INTO projection_projects (project_id, title, workspace_root)
         VALUES ('project-app', 'app', ?1)",
        [root.to_string_lossy()],
    )
    .unwrap();

    // the desktop app's CLI launcher, which homebased uses when the server
    // process is not a `t3` binary
    let bin = home.join(".t3/bin");
    fs::create_dir_all(&bin).unwrap();
    let cli = bin.join("t3");
    fs::write(
        &cli,
        "#!/bin/sh\ncase \"$1 $2 $3\" in\n  'auth session issue') printf '%s\\n' '{\"sessionId\":\"fake-session\",\"token\":\"fake-token\",\"method\":\"bearer-access-token\",\"scopes\":[]}' ;;\n  'auth session revoke') exit 0 ;;\n  *) exit 3 ;;\nesac\n",
    )
    .unwrap();
    fs::set_permissions(&cli, fs::Permissions::from_mode(0o755)).unwrap();
    state
}
