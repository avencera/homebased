//! Claude Code session inbox, the Claude equivalent of `codex queue`
//!
//! A running Claude Code session publishes `~/.claude/sessions/<pid>.json` with
//! its `sessionId` and `messagingSocketPath`, and a peer key file
//! `<pid>.<sha256(socket path)>.key` beside it. One auth line and one `user`
//! message line written to that socket queue the message as the session's next
//! user turn. The protocol is internal to Claude Code, so the sender accepts
//! only the peer protocol version it knows

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use nix::sys::signal::kill;
use nix::unistd::Pid;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::{Builder, Uuid};

use super::QUEUE_ATTEMPT_TIMEOUT;
use crate::domain::ThreadId;

/// Peer protocol version this sender speaks
const PEER_PROTOCOL: u64 = 1;

/// Receiver limit for one framed message, including the auth line
const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Socket write bound
const SOCKET_TIMEOUT: Duration = Duration::from_secs(5);

/// The receiver reads the sender's peer credentials after accept. Claude Code
/// keeps its own sender connection open this long on macOS before it closes
const MACOS_CLOSE_DELAY: Duration = Duration::from_millis(150);

/// How long to wait for the receiver to close after the write half closes
const CLOSE_WAIT: Duration = Duration::from_secs(1);

/// Registry record that a live Claude Code session writes for its inbox
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionRecord {
    pid: i32,
    session_id: Option<String>,
    messaging_socket_path: Option<PathBuf>,
    peer_protocol: Option<u64>,
    #[serde(default)]
    updated_at: i64,
}

/// Peer key file content
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PeerKey {
    peer_token: String,
}

/// Inbox of a live Claude Code session
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaudeInbox {
    pid: i32,
    socket: PathBuf,
    token: String,
}

impl ClaudeInbox {
    /// Find the live Claude Code session whose session id is `thread`
    ///
    /// `Ok(None)` means Claude Code does not know the id, so the thread belongs
    /// to Codex. An error means the id is a Claude Code session that cannot take
    /// messages now, for example because it is not running
    pub(crate) fn find(home: &Path, thread: ThreadId) -> Result<Option<Self>, String> {
        let claude = home.join(".claude");
        if let Some(inbox) = Self::find_live(&claude.join("sessions"), thread)? {
            return Ok(Some(inbox));
        }
        if has_transcript(&claude.join("projects"), thread) {
            return Err(format!("Claude session {thread} is not running"));
        }
        Ok(None)
    }

    fn find_live(sessions: &Path, thread: ThreadId) -> Result<Option<Self>, String> {
        let entries = match fs::read_dir(sessions) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(format!(
                    "read Claude session registry {}: {error}",
                    sessions.display()
                ));
            }
        };
        let newest = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .filter_map(|path| read_record(&path))
            .filter(|record| record_owns(record, thread) && process_alive(record.pid))
            .max_by_key(|record| record.updated_at);
        let Some(record) = newest else {
            return Ok(None);
        };
        Self::from_record(sessions, record, thread).map(Some)
    }

    fn from_record(
        sessions: &Path,
        record: SessionRecord,
        thread: ThreadId,
    ) -> Result<Self, String> {
        let pid = record.pid;
        if record.peer_protocol != Some(PEER_PROTOCOL) {
            return Err(format!(
                "Claude session {thread} (pid {pid}) uses peer protocol {:?}; homebased supports {PEER_PROTOCOL}",
                record.peer_protocol
            ));
        }
        let socket = record.messaging_socket_path.ok_or_else(|| {
            format!("Claude session {thread} (pid {pid}) has no messaging socket")
        })?;
        let key_path = sessions.join(key_file_name(pid, &socket));
        let key = fs::read_to_string(&key_path)
            .map_err(|error| format!("read Claude peer key {}: {error}", key_path.display()))?;
        let key: PeerKey = serde_json::from_str(&key)
            .map_err(|error| format!("parse Claude peer key {}: {error}", key_path.display()))?;
        Ok(Self {
            pid,
            socket,
            token: key.peer_token,
        })
    }

    /// Queue `line` as the session's next user turn
    ///
    /// Holds `delivery_lock` for the whole send so no other delivery for the
    /// task can overlap it
    pub(crate) fn send(
        &self,
        thread: ThreadId,
        line: &str,
        log_path: &Path,
        delivery_lock: &Path,
    ) -> Result<(), String> {
        let frame = frame(&self.token, thread, line)?;
        let _lock = lock_delivery(delivery_lock)?;
        let transcript = format!(
            "claude inbox pid={} socket={}\n",
            self.pid,
            self.socket.display()
        );
        let result = write_frame(&self.socket, &frame)
            .map_err(|error| format!("claude inbox {}: {error}", self.socket.display()));
        let _ = fs::write(log_path, transcript);
        result
    }
}

/// Claude Code keeps each session transcript at `projects/<cwd key>/<id>.jsonl`
fn has_transcript(projects: &Path, thread: ThreadId) -> bool {
    let Ok(entries) = fs::read_dir(projects) else {
        return false;
    };
    let file = format!("{thread}.jsonl");
    entries
        .filter_map(Result::ok)
        .any(|entry| entry.path().join(&file).is_file())
}

fn read_record(path: &Path) -> Option<SessionRecord> {
    let body = fs::read_to_string(path).ok()?;
    serde_json::from_str(&body).ok()
}

fn record_owns(record: &SessionRecord, thread: ThreadId) -> bool {
    record
        .session_id
        .as_deref()
        .and_then(|id| Uuid::parse_str(id).ok())
        .is_some_and(|id| id == thread.0)
}

fn process_alive(pid: i32) -> bool {
    pid > 0 && kill(Pid::from_raw(pid), None).is_ok()
}

/// Claude Code names the key after the socket path as written, not its
/// realpath. On macOS `/tmp` and `/private/tmp` give different names
fn key_file_name(pid: i32, socket: &Path) -> String {
    let digest = Sha256::digest(socket.as_os_str().as_encoded_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{pid}.{hex}.key")
}

/// Auth line plus one message line
///
/// `session_id` makes the receiver drop the message if a different session
/// now owns the socket. The message id is derived from the thread and line, so
/// a retry of the same event carries the same id
fn frame(token: &str, thread: ThreadId, line: &str) -> Result<Vec<u8>, String> {
    let auth = json!({ "type": "auth", "token": token });
    let message = json!({
        "msgV": 1,
        "msg_id": message_id(thread, line).to_string(),
        "type": "user",
        "message": { "role": "user", "content": line },
        "priority": "next",
        "session_id": thread.to_string(),
    });
    let frame = format!("{auth}\n{message}\n").into_bytes();
    if frame.len() > MAX_FRAME_BYTES {
        return Err(format!(
            "message is {} bytes; the Claude inbox accepts at most {MAX_FRAME_BYTES}",
            frame.len()
        ));
    }
    Ok(frame)
}

fn message_id(thread: ThreadId, line: &str) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(thread.0.as_bytes());
    hasher.update(line.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Builder::from_custom_bytes(bytes).into_uuid()
}

fn write_frame(socket: &Path, frame: &[u8]) -> io::Result<()> {
    let mut stream = UnixStream::connect(socket)?;
    stream.set_write_timeout(Some(SOCKET_TIMEOUT))?;
    stream.set_read_timeout(Some(CLOSE_WAIT))?;
    stream.write_all(frame)?;
    if cfg!(target_os = "macos") {
        thread::sleep(MACOS_CLOSE_DELAY);
    }
    stream.shutdown(Shutdown::Write)?;
    // the receiver sends nothing back; wait briefly for its close so it reads
    // the frame before this end goes away, and treat a slow close as sent
    let mut sink = [0_u8; 256];
    loop {
        match stream.read(&mut sink) {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    }
}

/// Take the task's delivery flock in this process, bounded by one queue attempt
fn lock_delivery(path: &Path) -> Result<Flock<File>, String> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(|error| format!("open delivery lock {}: {error}", path.display()))?;
    let deadline = Instant::now() + QUEUE_ATTEMPT_TIMEOUT;
    loop {
        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => return Ok(lock),
            Err((returned, Errno::EWOULDBLOCK)) if Instant::now() < deadline => {
                file = returned;
                thread::sleep(Duration::from_millis(50));
            }
            Err((_, error)) => {
                return Err(format!("delivery lock {}: {error}", path.display()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;
    use std::str::FromStr;

    use serde_json::Value;
    use tempfile::TempDir;

    use super::*;

    const THREAD: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e431";
    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    fn thread() -> ThreadId {
        ThreadId::from_str(THREAD).unwrap()
    }

    /// Home with a Claude session registry entry for `pid`
    fn home_with_session(pid: i32, protocol: u64) -> (TempDir, PathBuf) {
        let home = tempfile::tempdir().unwrap();
        let sessions = home.path().join(".claude/sessions");
        fs::create_dir_all(&sessions).unwrap();
        let socket = home.path().join("inbox.sock");
        let record = json!({
            "pid": pid,
            "sessionId": THREAD,
            "messagingSocketPath": socket,
            "peerProtocol": protocol,
            "updatedAt": 1,
        });
        fs::write(sessions.join(format!("{pid}.json")), record.to_string()).unwrap();
        fs::write(
            sessions.join(key_file_name(pid, &socket)),
            json!({ "peerToken": TOKEN }).to_string(),
        )
        .unwrap();
        (home, socket)
    }

    fn live_pid() -> i32 {
        i32::try_from(std::process::id()).unwrap()
    }

    #[test]
    fn unknown_thread_belongs_to_codex() {
        let home = tempfile::tempdir().unwrap();
        assert_eq!(ClaudeInbox::find(home.path(), thread()), Ok(None));
    }

    #[test]
    fn live_session_sends_auth_and_session_bound_user_turn() {
        let (home, socket) = home_with_session(live_pid(), PEER_PROTOCOL);
        let listener = UnixListener::bind(&socket).unwrap();
        let receiver = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut body = String::new();
            stream.read_to_string(&mut body).unwrap();
            body
        });

        let inbox = ClaudeInbox::find(home.path(), thread()).unwrap().unwrap();
        let line = "HOMEBASED_EVENT {\"api_version\":1}";
        inbox
            .send(
                thread(),
                line,
                &home.path().join("callback.log"),
                &home.path().join("delivery.lock"),
            )
            .unwrap();

        let body = receiver.join().unwrap();
        let lines: Vec<Value> = body
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines[0], json!({ "type": "auth", "token": TOKEN }));
        assert_eq!(lines[1]["type"], "user");
        assert_eq!(lines[1]["session_id"], THREAD);
        assert_eq!(
            lines[1]["message"],
            json!({ "role": "user", "content": line })
        );
        assert_eq!(
            lines[1]["msg_id"],
            message_id(thread(), line).to_string(),
            "a retry of the same event reuses its message id"
        );
    }

    #[test]
    fn stopped_session_is_not_sent_to_codex() {
        let home = tempfile::tempdir().unwrap();
        let project = home.path().join(".claude/projects/-work");
        fs::create_dir_all(&project).unwrap();
        fs::write(project.join(format!("{THREAD}.jsonl")), "").unwrap();

        let error = ClaudeInbox::find(home.path(), thread()).unwrap_err();
        assert!(error.contains("not running"), "{error}");
    }

    #[test]
    fn dead_session_record_is_ignored() {
        // pid 0 addresses the process group, never one session
        let (home, _socket) = home_with_session(0, PEER_PROTOCOL);
        assert_eq!(ClaudeInbox::find(home.path(), thread()), Ok(None));
    }

    #[test]
    fn unknown_peer_protocol_is_refused() {
        let (home, _socket) = home_with_session(live_pid(), PEER_PROTOCOL + 1);
        let error = ClaudeInbox::find(home.path(), thread()).unwrap_err();
        assert!(error.contains("peer protocol"), "{error}");
    }
}
