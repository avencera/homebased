//! T3 thread launch on another machine, through two real daemons and a fake T3 server

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use homebased::domain::ThreadId;
use serde_json::{Value, json};
use tungstenite::Message;

use super::{Daemon, add_peer};

/// T3 server that answers the environment, ticket, and launch requests, and
/// records each launch payload
struct FakeT3 {
    origin: String,
    /// Close each launch socket without a reply, as if the reply were lost
    drop_replies: Arc<AtomicBool>,
    launches: Arc<Mutex<Vec<Value>>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl FakeT3 {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let launches = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let drop_replies = Arc::new(AtomicBool::new(false));
        let recorded = Arc::clone(&launches);
        let stopped = Arc::clone(&stop);
        let dropping = Arc::clone(&drop_replies);
        let worker = thread::spawn(move || {
            while !stopped.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => serve(stream, &recorded, dropping.load(Ordering::Relaxed)),
                    Err(_) => thread::sleep(Duration::from_millis(5)),
                }
            }
        });
        Self {
            origin,
            drop_replies,
            launches,
            stop,
            worker: Some(worker),
        }
    }

    fn launches(&self) -> Vec<Value> {
        self.launches.lock().unwrap().clone()
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

fn serve(stream: TcpStream, launches: &Mutex<Vec<Value>>, drop_reply: bool) {
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
        serve_launch(stream, launches, drop_reply);
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

fn serve_launch(stream: TcpStream, launches: &Mutex<Vec<Value>>, drop_reply: bool) {
    let Ok(mut socket) = tungstenite::accept(stream) else {
        return;
    };
    let Ok(Message::Text(text)) = socket.read() else {
        return;
    };
    let request: Value = serde_json::from_str(text.as_str()).unwrap();
    let thread = request["payload"]["threadId"].clone();
    launches.lock().unwrap().push(request);
    if drop_reply {
        let _ = socket.close(None);
        while socket.read().is_ok() {}
        return;
    }
    let exit = json!({
        "_tag": "Exit",
        "requestId": "1",
        "exit": { "_tag": "Success", "value": { "threadId": thread, "projection": {}, "resumed": false } }
    });
    let _ = socket.send(Message::text(exit.to_string()));
    while socket.read().is_ok() {}
}

/// T3 user data for `home`: a running server at `origin`, one project, a
/// default model, and a `t3` CLI that issues fake tokens
fn install_t3(home: &Path, origin: &str) {
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
    let db = rusqlite::Connection::open(userdata.join("statev2.sqlite")).unwrap();
    db.execute_batch(
        "CREATE TABLE projection_projects (
            project_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            workspace_root TEXT NOT NULL,
            deleted_at TEXT,
            default_model_selection_json TEXT
         );",
    )
    .unwrap();
    db.execute(
        "INSERT INTO projection_projects (project_id, title, workspace_root) VALUES ('project-app', 'app', ?1)",
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
}

fn run_json(daemon: &Daemon, args: &[&str]) -> (i32, Value) {
    let output = daemon.cmd().arg("--json").args(args).output().unwrap();
    let stream = if output.status.success() {
        &output.stdout
    } else {
        &output.stderr
    };
    let value = serde_json::from_slice(stream).unwrap_or_else(|_| {
        panic!(
            "not JSON: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.code().unwrap_or(-1), value)
}

#[test]
fn launch_starts_a_t3_thread_on_the_named_machine_with_a_reply_route() {
    if which::which("curl").is_err() {
        return;
    }
    let origin = Daemon::start("t3-origin", true);
    let executor = Daemon::start("t3-executor", true);
    add_peer(&origin, &executor);
    let t3 = FakeT3::start();
    install_t3(&executor.user_home, &t3.origin);
    let source = ThreadId(uuid::Uuid::now_v7()).to_string();
    let request = uuid::Uuid::now_v7().to_string();
    // what the shell passes for an unquoted `~/code/app` on the origin, whose
    // home differs from the executor's
    let project = format!("{}/code/app", origin.user_home.display());
    let launch = [
        "t3",
        "launch",
        "--machine",
        "t3-executor",
        "--project",
        &project,
        "--message",
        "Fix the flaky upload test",
        "--source-thread",
        &source,
        "--request-id",
        &request,
    ];

    let (code, launched) = run_json(&origin, &launch);
    assert_eq!(code, 0, "{launched}");
    let (code, retried) = run_json(&origin, &launch);
    assert_eq!(code, 0, "{retried}");

    let thread = launched["thread"]["thread_id"].as_str().unwrap();
    assert_eq!(launched["machine"], executor.machine_id().to_string());
    assert_eq!(launched["machine_name"], "t3-executor");
    assert_eq!(launched["thread"]["project_id"], "project-app");
    assert_eq!(
        launched["thread"]["link"],
        format!("t3code://threads/executor-env/{thread}")
    );
    assert_eq!(retried["thread"]["thread_id"], thread);

    let launches = t3.launches();
    assert_eq!(launches.len(), 2);
    assert_eq!(launches[0]["payload"], launches[1]["payload"]);
    let payload = &launches[0]["payload"];
    assert_eq!(payload["threadId"], thread);
    assert_eq!(payload["commandId"], format!("homebased:{request}"));
    assert_eq!(
        payload["modelSelection"],
        json!({ "instanceId": "codex", "model": "gpt-6.1-sol" })
    );
    let text = payload["initialMessage"]["text"].as_str().unwrap();
    assert!(text.starts_with("Fix the flaky upload test\n"), "{text}");
    assert!(
        text.contains(&format!(
            "homebased message send --machine t3-origin --thread {source}"
        )),
        "{text}"
    );

    let (code, listed) = run_json(&origin, &["t3", "projects", "--machine", "t3-executor"]);
    assert_eq!(code, 0, "{listed}");
    assert_eq!(listed["projects"][0]["id"], "project-app");
    assert_eq!(
        listed["projects"][0]["default_model"]["model"],
        "gpt-6.1-sol"
    );

    let (code, missing) = run_json(
        &origin,
        &[
            "t3",
            "launch",
            "--machine",
            "t3-executor",
            "--project",
            "~/code/missing",
            "--title",
            "Nothing",
        ],
    );
    assert_eq!(code, 3, "{missing}");
    assert_eq!(missing["error"]["code"], "t3_project_not_found");
    assert_eq!(t3.launches().len(), 2);
}

#[test]
fn a_lost_t3_reply_reaches_the_caller_with_the_request_id_to_retry() {
    if which::which("curl").is_err() {
        return;
    }
    let origin = Daemon::start("t3-lost-origin", true);
    let executor = Daemon::start("t3-lost-executor", true);
    add_peer(&origin, &executor);
    let t3 = FakeT3::start();
    t3.drop_replies.store(true, Ordering::Relaxed);
    install_t3(&executor.user_home, &t3.origin);
    let request = uuid::Uuid::now_v7().to_string();

    let (code, lost) = run_json(
        &origin,
        &[
            "t3",
            "launch",
            "--machine",
            "t3-lost-executor",
            "--project",
            "app",
            "--title",
            "Lost reply",
            "--request-id",
            &request,
            "--no-reply-route",
        ],
    );

    assert_eq!(code, 1, "{lost}");
    assert_eq!(lost["error"]["code"], "t3_launch_outcome_unknown");
    assert_eq!(lost["error"]["retryable"], true);
    assert_eq!(lost["error"]["input"]["request_id"], request);
    assert_eq!(
        lost["error"]["input"]["machine"],
        executor.machine_id().to_string()
    );
    assert_eq!(t3.launches().len(), 1);
}
