//! Fake T3 server and state databases for tests

use std::io::{Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rusqlite::Connection;
use serde_json::{Value, json};
use tungstenite::Message;

/// One canned reply, used in order; the environment descriptor is answered
/// separately and never consumes a reply
pub(crate) enum FakeResponse {
    /// Plain HTTP response
    Http { status: u16, body: String },
    /// Accept a WebSocket, read one message, and send these frames; with no
    /// frames, close at once as if the reply were lost
    WebSocket(Vec<Value>),
}

impl FakeResponse {
    pub(crate) fn http(status: u16, body: &Value) -> Self {
        Self::Http {
            status,
            body: body.to_string(),
        }
    }
}

/// Request the fake server received, except environment descriptor reads
#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    /// HTTP method, or `WS` for a WebSocket message
    pub(crate) method: String,
    /// Request path with query
    pub(crate) path: String,
    /// Raw header lines
    pub(crate) headers: String,
    /// Request body, or the WebSocket message text
    pub(crate) body: String,
}

/// Local HTTP and WebSocket server that answers like T3
pub(crate) struct FakeT3Server {
    /// `http://127.0.0.1:<port>`
    pub(crate) origin: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl FakeT3Server {
    /// Serve `responses` in order to a server reporting `protocol`; `None`
    /// omits the field as T3 `0.0.42` and earlier do
    pub(crate) fn start(protocol: Option<u32>, responses: Vec<FakeResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let recorded = Arc::clone(&requests);
        let stopped = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            let mut responses = responses.into_iter();
            while !stopped.load(Ordering::Relaxed) {
                let Ok((stream, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                serve(stream, protocol, &mut responses, &recorded);
            }
        });
        Self {
            origin: format!("http://{address}"),
            requests,
            stop,
            worker: Some(worker),
        }
    }

    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for FakeT3Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn serve(
    mut stream: TcpStream,
    protocol: Option<u32>,
    responses: &mut impl Iterator<Item = FakeResponse>,
    recorded: &Mutex<Vec<RecordedRequest>>,
) {
    // BSD sockets inherit the listener's non-blocking flag
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    // route on the whole header, since a client may send it in pieces
    let Ok(head) = read_head(&mut stream) else {
        return;
    };
    let text = String::from_utf8_lossy(&head).into_owned();
    let mut lines = text.split("\r\n");
    let mut first_line = lines.next().unwrap_or_default().split_whitespace();
    let method = first_line.next().unwrap_or_default().to_owned();
    let path = first_line.next().unwrap_or_default().to_owned();
    let headers = lines
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\r\n");
    if path.starts_with("/.well-known/t3/environment") {
        let body = protocol.map_or_else(
            || json!({ "serverVersion": "0.0.42" }),
            |protocol| json!({ "orchestrationProtocolVersion": protocol }),
        );
        let _ = respond(&mut stream, 200, &body.to_string());
        return;
    }

    let (status, body) = match responses.next() {
        Some(FakeResponse::WebSocket(frames)) => {
            let replay = Replay {
                head: Cursor::new(head),
                stream,
            };
            serve_websocket(replay, path, &frames, recorded);
            return;
        }
        Some(FakeResponse::Http { status, body }) => (status, body),
        None => (500, "{}".to_string()),
    };
    let Ok(request_body) = read_body(&mut stream, &headers) else {
        return;
    };
    recorded.lock().unwrap().push(RecordedRequest {
        method,
        path,
        headers,
        body: request_body,
    });
    let _ = respond(&mut stream, status, &body);
}

/// Request bytes through the blank line that ends the header
fn read_head(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte)?;
        head.push(byte[0]);
    }
    Ok(head)
}

fn read_body(stream: &mut TcpStream, headers: &str) -> std::io::Result<String> {
    let length = headers
        .split("\r\n")
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, value)| value.trim().parse::<usize>().unwrap());
    let mut body = vec![0; length];
    stream.read_exact(&mut body)?;
    Ok(String::from_utf8(body).unwrap())
}

fn respond(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status} Fake\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()
}

/// A stream that first replays the header bytes routing already read
struct Replay {
    head: Cursor<Vec<u8>>,
    stream: TcpStream,
}

impl Read for Replay {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.head.read(buf)? {
            0 => self.stream.read(buf),
            read => Ok(read),
        }
    }
}

impl Write for Replay {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

fn serve_websocket(
    stream: Replay,
    path: String,
    frames: &[Value],
    recorded: &Mutex<Vec<RecordedRequest>>,
) {
    let Ok(mut socket) = tungstenite::accept(stream) else {
        return;
    };
    let Ok(Message::Text(text)) = socket.read() else {
        return;
    };
    recorded.lock().unwrap().push(RecordedRequest {
        method: "WS".into(),
        path,
        headers: String::new(),
        body: text.as_str().to_owned(),
    });
    for frame in frames {
        let _ = socket.send(Message::text(frame.to_string()));
    }
    if frames.is_empty() {
        let _ = socket.close(None);
    }
    // wait for the client to close
    while socket.read().is_ok() {}
}

/// `Exit` frame for request `1`
pub(crate) fn rpc_exit(exit: Value) -> Value {
    json!({ "_tag": "Exit", "requestId": "1", "exit": exit })
}

/// Write a service-managed `server-runtime.json`
pub(crate) fn write_runtime(userdata: &Path, origin: &str, pid: i32) {
    let port = origin
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .unwrap_or(1);
    let runtime = json!({
        "version": 1,
        "pid": pid,
        "port": port,
        "origin": origin,
        "startedAt": "2026-09-26T17:03:50.514Z",
        "serviceManaged": true
    });
    std::fs::write(userdata.join("server-runtime.json"), runtime.to_string()).unwrap();
}

/// Builder for the tables of `statev2.sqlite` that homebased reads
pub(crate) struct V2State(Connection);

impl V2State {
    pub(crate) fn create(path: &Path) -> Self {
        let db = Connection::open(path).unwrap();
        db.execute_batch(
            "CREATE TABLE orchestration_v2_projection_threads (
                thread_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL DEFAULT 'project',
                title TEXT NOT NULL,
                archived_at TEXT,
                deleted_at TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}'
             );
             CREATE TABLE orchestration_v2_projection_provider_threads (
                provider_thread_id TEXT PRIMARY KEY,
                thread_id TEXT,
                provider TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
             );",
        )
        .unwrap();
        Self(db)
    }

    pub(crate) fn thread(self, thread_id: &str, title: &str, archived: bool) -> Self {
        let archived_at = archived.then_some("2026-10-02T20:02:28.417Z");
        self.0
            .execute(
                "INSERT INTO orchestration_v2_projection_threads (thread_id, title, archived_at)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![thread_id, title, archived_at],
            )
            .unwrap();
        self
    }

    /// A provider thread whose native id is the Claude session or Codex thread
    pub(crate) fn native(self, provider: &str, native_id: &str, thread_id: &str) -> Self {
        let payload = json!({ "nativeThreadRef": { "driver": provider, "nativeId": native_id } });
        self.0
            .execute(
                "INSERT INTO orchestration_v2_projection_provider_threads
                 (provider_thread_id, thread_id, provider, updated_at, payload_json)
                 VALUES (?1, ?2, ?3, '2026-10-02T20:00:00Z', ?4)",
                rusqlite::params![
                    format!("provider-thread:{native_id}"),
                    thread_id,
                    provider,
                    payload.to_string()
                ],
            )
            .unwrap();
        self
    }

    /// The copied V1 session table that maps a thread imported from V1
    pub(crate) fn legacy(
        self,
        provider: &str,
        cursor: &str,
        session: &str,
        thread_id: &str,
    ) -> Self {
        self.0
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS provider_session_runtime (
                    thread_id TEXT PRIMARY KEY,
                    provider_name TEXT,
                    last_seen_at TEXT,
                    resume_cursor_json TEXT
                 );",
            )
            .unwrap();
        self.0
            .execute(
                "INSERT INTO provider_session_runtime
                 (thread_id, provider_name, last_seen_at, resume_cursor_json)
                 VALUES (?1, ?2, '2026-09-26T17:00:00Z', ?3)",
                rusqlite::params![
                    thread_id,
                    provider,
                    json!({ (cursor): session }).to_string()
                ],
            )
            .unwrap();
        self
    }
}
