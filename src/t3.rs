//! T3 Code client for waking stopped Claude Code sessions and checking its local API
//!
//! # T3 Code internals this depends on
//!
//! T3 Code has no public API for these operations. This client pins the local
//! contract observed in T3 Code at commit `95030dc67` on 2026-09-26. An update
//! can change these details without notice. Run `homebased t3 check` after a T3
//! update
//!
//! - User data lives in `~/.t3/userdata`; `server-runtime.json` has `version`,
//!   `pid`, `host`, `port`, `origin`, and `startedAt`. Version 1 and a live PID
//!   identify a running server. The process check uses `kill(pid, None)`
//! - `state.sqlite` has `provider_session_runtime(thread_id,
//!   resume_cursor_json,last_seen_at)` and
//!   `projection_threads(thread_id,deleted_at)`. Claude's session id is
//!   `json_extract(resume_cursor_json, '$.resume')`; `thread_id` is T3's thread
//!   id. A join on `thread_id` excludes deleted threads, and the newest
//!   `last_seen_at` wins
//! - `t3 auth session issue --json --ttl 5m --label homebased` returns
//!   `sessionId` and `token` for a bearer session. Revoke it with
//!   `t3 auth session revoke <sessionId>` after every use; `t3 --version`
//!   supplies the optional probe version
//! - `GET /api/orchestration/threads/{threadId}?turnLimit=1` returns a `thread`
//!   with `id`, `runtimeMode`, `interactionMode`, `archivedAt`, and `deletedAt`
//!   on HTTP 200. HTTP 401 or 403 with a fresh token indicates a changed API;
//!   HTTP 404 has a typed error object with `_tag` and optional `reason`
//! - `POST /api/orchestration/dispatch` accepts `thread.turn.start` with
//!   `commandId`, `threadId`, `message`, `runtimeMode`, `interactionMode`, and
//!   `createdAt`; its `message` has `messageId`, `role`, `text`, and
//!   `attachments`. `modelSelection` is omitted. HTTP 200 returns integer
//!   `sequence`; HTTP 400 indicates a changed command shape. Unknown thread ids
//!   return HTTP 500 with `reason: orchestration_dispatch_failed`. HTTP 401 or
//!   403 indicates a changed API, and HTTP 404 can return a typed error object
//!
//! Calls use the blocking system `curl` helper. The callback dispatcher must
//! call this module from `spawn_blocking`

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use nix::errno::Errno;
use nix::sys::signal::kill;
use nix::unistd::Pid;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::{Builder, Uuid};

use crate::curl::{CurlMethod, CurlRequest, CurlResponse};
use crate::domain::ThreadId;

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_millis(250);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const CLAUDE_THREAD_SQL: &str = "
    SELECT r.thread_id
    FROM provider_session_runtime r
    JOIN projection_threads t ON t.thread_id = r.thread_id
    WHERE t.deleted_at IS NULL
      AND CASE WHEN json_valid(r.resume_cursor_json)
          THEN json_extract(r.resume_cursor_json, '$.resume') = ?1
          ELSE 0 END
    ORDER BY r.last_seen_at DESC
    LIMIT 1";
const REQUIRED_STATE_SQL: &str = "
    SELECT r.thread_id, r.resume_cursor_json, r.last_seen_at,
           t.thread_id, t.deleted_at
    FROM provider_session_runtime r
    JOIN projection_threads t ON t.thread_id = r.thread_id
    LIMIT 0";
const LATEST_THREAD_SQL: &str = "
    SELECT r.thread_id
    FROM provider_session_runtime r
    JOIN projection_threads t ON t.thread_id = r.thread_id
    WHERE t.deleted_at IS NULL
    ORDER BY r.last_seen_at DESC
    LIMIT 1";

/// Where to find T3 Code for one user
#[derive(Debug, Clone)]
pub struct T3Env {
    /// User home directory that contains `.t3/userdata`
    pub home: PathBuf,
    /// PATH value used to find the `t3` executable
    pub path: OsString,
}

impl T3Env {
    /// Create a T3 environment from a home directory and PATH value
    #[must_use]
    pub fn new(home: PathBuf, path: OsString) -> Self {
        Self { home, path }
    }

    /// Read the home directory and PATH from the current process environment
    #[must_use]
    pub fn from_env() -> Self {
        let home = std::env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .map_or_else(|| PathBuf::from("/nonexistent"), PathBuf::from);
        let path = match std::env::var_os("PATH") {
            Some(path) => path,
            None => OsString::new(),
        };
        Self { home, path }
    }

    fn userdata(&self) -> PathBuf {
        self.home.join(".t3/userdata")
    }

    fn state_path(&self) -> PathBuf {
        self.userdata().join("state.sqlite")
    }
}

/// Result of asking T3 Code to start a turn in a Claude session's owning thread
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeOutcome {
    /// T3 accepted the new turn
    Woken { t3_thread: String },
    /// No T3 thread owns this Claude session
    NotT3Thread,
    /// T3 is not installed or running, or a transport failed
    Unavailable(String),
    /// T3 is reachable but will not run the turn
    Refused(String),
    /// A response did not match the pinned T3 contract
    ApiChanged(String),
}

/// Probe result status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeStatus {
    /// T3 user data does not exist
    NotInstalled,
    /// T3 runtime metadata is missing, invalid, or its process is gone
    NotRunning,
    /// Every pinned local API check passed
    Compatible,
    /// A required local API check failed
    Changed,
}

/// One result in a T3 API probe
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProbeCheck {
    /// Stable name of the check
    pub name: &'static str,
    /// Whether the check passed
    pub ok: bool,
    /// Human-readable result detail
    pub detail: String,
}

/// Report from checking the installed T3 Code API
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProbeReport {
    /// Overall compatibility status
    pub status: ProbeStatus,
    /// T3 CLI version, when `t3 --version` succeeds
    pub t3_version: Option<String>,
    /// Checks in probe order
    pub checks: Vec<ProbeCheck>,
}

impl ProbeReport {
    /// Stable text for failed checks, used to de-duplicate compatibility alerts
    #[must_use]
    pub fn fingerprint(&self) -> Option<String> {
        if self.status != ProbeStatus::Changed {
            return None;
        }

        let failures = self
            .checks
            .iter()
            .filter(|check| !check.ok)
            .map(|check| format!("{}: {}", check.name, check.detail))
            .collect::<Vec<_>>();
        (!failures.is_empty()).then(|| failures.join("\n"))
    }
}

/// Start a T3 turn for the T3 thread that owns a Claude Code session
#[must_use]
pub fn wake_claude_session(env: &T3Env, session: ThreadId, text: &str) -> WakeOutcome {
    let userdata = env.userdata();
    if !userdata.is_dir() || !env.state_path().is_file() {
        return WakeOutcome::NotT3Thread;
    }

    let Some(t3_thread) = find_claude_thread(&env.state_path(), session) else {
        return WakeOutcome::NotT3Thread;
    };

    let runtime = match load_runtime(&userdata) {
        Ok(runtime) => runtime,
        Err(detail) => return WakeOutcome::Unavailable(detail),
    };
    let executable = match resolve_t3(env) {
        Ok(executable) => executable,
        Err(detail) => return WakeOutcome::Unavailable(detail),
    };
    let token = match issue_token(&executable) {
        Ok(token) => token,
        Err(IssueError::Unavailable(detail)) => return WakeOutcome::Unavailable(detail),
        Err(IssueError::Changed(detail)) => return WakeOutcome::ApiChanged(detail),
    };
    let snapshot = match fetch_snapshot(&runtime.origin, &t3_thread, &token.value) {
        Ok(snapshot) => snapshot,
        Err(error) => return error.into_wake_outcome(),
    };
    if snapshot.archived {
        return WakeOutcome::Refused("T3 thread is archived".into());
    }
    if snapshot.deleted {
        return WakeOutcome::Refused("T3 thread is deleted".into());
    }

    match dispatch_turn(
        &runtime.origin,
        &t3_thread,
        &snapshot.runtime_mode,
        &snapshot.interaction_mode,
        text,
        &token.value,
    ) {
        Ok(()) => WakeOutcome::Woken { t3_thread },
        Err(error) => error.into_wake_outcome(),
    }
}

/// Check the T3 local API without starting a turn in an existing thread
#[must_use]
pub fn probe(env: &T3Env) -> ProbeReport {
    let userdata = env.userdata();
    if !userdata.is_dir() {
        return report(
            ProbeStatus::NotInstalled,
            None,
            vec![failed("userdata", "T3 user data directory is missing")],
        );
    }

    let mut checks = vec![passed("userdata", "T3 user data directory exists")];
    let runtime = match load_runtime(&userdata) {
        Ok(runtime) => runtime,
        Err(detail) => {
            checks.push(failed("server", detail));
            return report(ProbeStatus::NotRunning, None, checks);
        }
    };
    checks.push(passed(
        "server",
        "T3 server runtime is valid and its process is alive",
    ));

    let executable = resolve_t3(env);
    let version = executable.as_ref().ok().and_then(|path| t3_version(path));

    if !state_schema_matches(&env.state_path()) {
        checks.push(failed(
            "state_schema",
            "state.sqlite does not have the required thread columns",
        ));
        return report(ProbeStatus::Changed, version, checks);
    }
    checks.push(passed(
        "state_schema",
        "required state.sqlite columns exist",
    ));

    let executable = match executable {
        Ok(executable) => executable,
        Err(detail) => {
            checks.push(failed("token_issue", detail));
            return report(ProbeStatus::Changed, version, checks);
        }
    };
    let token = match issue_token(&executable) {
        Ok(token) => token,
        Err(error) => {
            checks.push(failed("token_issue", error.detail()));
            return report(ProbeStatus::Changed, version, checks);
        }
    };
    checks.push(passed("token_issue", "short-lived session token issued"));

    let latest_thread = match latest_thread(&env.state_path()) {
        Ok(thread) => thread,
        Err(detail) => {
            checks.push(failed("thread_snapshot", detail));
            return report(ProbeStatus::Changed, version, checks);
        }
    };
    if let Some(thread_id) = latest_thread {
        match fetch_snapshot(&runtime.origin, &thread_id, &token.value) {
            Ok(_) => checks.push(passed(
                "thread_snapshot",
                "thread snapshot has the required fields",
            )),
            Err(error) => {
                checks.push(failed("thread_snapshot", error.detail()));
                return report(ProbeStatus::Changed, version, checks);
            }
        }
    } else {
        checks.push(passed(
            "thread_snapshot",
            "skipped because no non-deleted T3 thread is available",
        ));
    }

    let unknown_thread = Uuid::now_v7().to_string();
    let probe_body = dispatch_body(
        &unknown_thread,
        &Uuid::now_v7().to_string(),
        &Uuid::now_v7().to_string(),
        "homebased compatibility probe",
        "full-access",
        "default",
    );
    let response = match send_json(
        CurlMethod::Post,
        &format!("{}/api/orchestration/dispatch", runtime.origin),
        &token.value,
        &probe_body,
    ) {
        Ok(response) => response,
        Err(detail) => {
            checks.push(failed("dispatch", detail));
            return report(ProbeStatus::Changed, version, checks);
        }
    };
    if !is_unknown_thread_response(&response) {
        checks.push(failed("dispatch", dispatch_probe_failure(&response)));
        return report(ProbeStatus::Changed, version, checks);
    }
    checks.push(passed(
        "dispatch",
        "unknown thread returned orchestration_dispatch_failed",
    ));

    report(ProbeStatus::Compatible, version, checks)
}

#[derive(Debug, Deserialize)]
struct RuntimeFile {
    version: u32,
    pid: i32,
    host: String,
    port: u16,
    origin: String,
    #[serde(rename = "startedAt")]
    started_at: String,
}

fn load_runtime(userdata: &Path) -> Result<RuntimeFile, String> {
    let path = userdata.join("server-runtime.json");
    let bytes = fs::read(path).map_err(|_| "T3 server runtime file is missing".to_string())?;
    let runtime: RuntimeFile = serde_json::from_slice(&bytes)
        .map_err(|_| "T3 server runtime file is invalid".to_string())?;
    if runtime.version != 1
        || runtime.pid <= 0
        || runtime.host.trim().is_empty()
        || runtime.port == 0
        || runtime.started_at.trim().is_empty()
        || !valid_origin(&runtime.origin)
    {
        return Err("T3 server runtime file is invalid".into());
    }
    if !process_is_alive(runtime.pid) {
        return Err("T3 server process is not running".into());
    }
    Ok(runtime)
}

fn valid_origin(origin: &str) -> bool {
    let Some(authority) = origin.strip_prefix("http://") else {
        return false;
    };
    !authority.is_empty()
        && !authority.contains('/')
        && !authority.contains('?')
        && !authority.contains('#')
        && !authority.contains('@')
        && !authority.chars().any(char::is_whitespace)
}

fn process_is_alive(pid: i32) -> bool {
    match kill(Pid::from_raw(pid), None) {
        Ok(()) | Err(Errno::EPERM) => true,
        Err(_) => false,
    }
}

fn state_connection(path: &Path) -> Result<Connection, rusqlite::Error> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let connection = Connection::open_with_flags(path, flags)?;
    connection.busy_timeout(SQLITE_BUSY_TIMEOUT)?;
    Ok(connection)
}

fn find_claude_thread(path: &Path, session: ThreadId) -> Option<String> {
    let db = state_connection(path).ok()?;
    db.query_row(CLAUDE_THREAD_SQL, [session.to_string()], |row| row.get(0))
        .optional()
        .ok()?
}

fn state_schema_matches(path: &Path) -> bool {
    let Ok(db) = state_connection(path) else {
        return false;
    };
    db.prepare(REQUIRED_STATE_SQL).is_ok()
}

fn latest_thread(path: &Path) -> Result<Option<String>, String> {
    let db = state_connection(path).map_err(|_| "state.sqlite cannot be read".to_string())?;
    db.query_row(LATEST_THREAD_SQL, [], |row| row.get(0))
        .optional()
        .map_err(|_| "state.sqlite thread lookup failed".to_string())
}

fn resolve_t3(env: &T3Env) -> Result<PathBuf, String> {
    let cwd =
        std::env::current_dir().map_err(|_| "could not read current directory".to_string())?;
    which::which_in("t3", Some(env.path.clone()), &cwd)
        .map_err(|_| "t3 is not installed or is not on PATH".to_string())
}

enum IssueError {
    Unavailable(String),
    Changed(String),
}

impl IssueError {
    fn detail(&self) -> String {
        match self {
            Self::Unavailable(detail) | Self::Changed(detail) => detail.clone(),
        }
    }
}

struct IssuedToken {
    value: String,
    _revoker: TokenRevoker,
}

struct TokenRevoker {
    executable: PathBuf,
    session_id: String,
}

impl Drop for TokenRevoker {
    fn drop(&mut self) {
        let _ = Command::new(&self.executable)
            .args(["auth", "session", "revoke", &self.session_id])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn issue_token(executable: &Path) -> Result<IssuedToken, IssueError> {
    let output = Command::new(executable)
        .args([
            "auth",
            "session",
            "issue",
            "--json",
            "--ttl",
            "5m",
            "--label",
            "homebased",
        ])
        .stdin(Stdio::null())
        .output()
        .map_err(|_| IssueError::Unavailable("could not start t3 auth session issue".into()))?;
    let parsed = serde_json::from_slice::<Value>(&output.stdout);
    if !output.status.success() {
        if let Ok(value) = &parsed
            && let Some(session_id) = value
                .get("sessionId")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
        {
            let revoker = TokenRevoker {
                executable: executable.to_path_buf(),
                session_id: session_id.to_owned(),
            };
            drop(revoker);
        }
        return Err(IssueError::Unavailable(
            "t3 auth session issue failed".into(),
        ));
    }

    let value =
        parsed.map_err(|_| IssueError::Changed("t3 token output is not valid JSON".into()))?;
    let Some(session_id) = value
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    else {
        return Err(IssueError::Changed(
            "t3 token output has no sessionId".into(),
        ));
    };
    let revoker = TokenRevoker {
        executable: executable.to_path_buf(),
        session_id: session_id.to_owned(),
    };
    if value.get("method").and_then(Value::as_str) != Some("bearer-access-token") {
        drop(revoker);
        return Err(IssueError::Changed(
            "t3 token output has an unexpected method".into(),
        ));
    }
    if value.get("scopes").and_then(Value::as_array).is_none() {
        drop(revoker);
        return Err(IssueError::Changed(
            "t3 token output has no scopes array".into(),
        ));
    }
    let Some(token) = value
        .get("token")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    else {
        drop(revoker);
        return Err(IssueError::Changed("t3 token output has no token".into()));
    };

    Ok(IssuedToken {
        value: token.to_owned(),
        _revoker: revoker,
    })
}

fn t3_version(executable: &Path) -> Option<String> {
    let output = Command::new(executable).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!version.is_empty()).then_some(version)
}

struct ThreadSnapshot {
    runtime_mode: String,
    interaction_mode: String,
    archived: bool,
    deleted: bool,
}

#[derive(Debug)]
enum ApiFailure {
    Unavailable(String),
    Refused(String),
    Changed(String),
}

impl ApiFailure {
    fn detail(&self) -> String {
        match self {
            Self::Unavailable(detail) | Self::Refused(detail) | Self::Changed(detail) => {
                detail.clone()
            }
        }
    }

    fn into_wake_outcome(self) -> WakeOutcome {
        match self {
            Self::Unavailable(detail) => WakeOutcome::Unavailable(detail),
            Self::Refused(detail) => WakeOutcome::Refused(detail),
            Self::Changed(detail) => WakeOutcome::ApiChanged(detail),
        }
    }
}

fn fetch_snapshot(
    origin: &str,
    thread_id: &str,
    token: &str,
) -> Result<ThreadSnapshot, ApiFailure> {
    let url = format!(
        "{origin}/api/orchestration/threads/{}?turnLimit=1",
        encode_path_segment(thread_id)
    );
    let response = send_empty(CurlMethod::Get, &url, token).map_err(ApiFailure::Unavailable)?;
    if response.status == 401 || response.status == 403 {
        return Err(ApiFailure::Changed(format!(
            "thread snapshot returned HTTP {} with a fresh session token",
            response.status
        )));
    }
    if response.status == 404 {
        return Err(classify_typed_404(&response.body, "thread snapshot"));
    }
    if response.status != 200 {
        return Err(ApiFailure::Unavailable(format!(
            "thread snapshot returned HTTP {}",
            response.status
        )));
    }

    let value: Value = serde_json::from_str(&response.body)
        .map_err(|_| ApiFailure::Changed("thread snapshot is not valid JSON".into()))?;
    let Some(thread) = value.get("thread").and_then(Value::as_object) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no thread object".into(),
        ));
    };
    let Some(id) = thread.get("id").and_then(Value::as_str) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no thread id".into(),
        ));
    };
    if id != thread_id {
        return Err(ApiFailure::Changed(
            "thread snapshot id does not match the requested thread".into(),
        ));
    }
    let Some(runtime_mode) = non_empty_string(thread.get("runtimeMode")) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no valid runtimeMode".into(),
        ));
    };
    let Some(interaction_mode) = non_empty_string(thread.get("interactionMode")) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no valid interactionMode".into(),
        ));
    };
    let Some(archived_at) = timestamp_field(thread.get("archivedAt")) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no valid archivedAt".into(),
        ));
    };
    let Some(deleted_at) = timestamp_field(thread.get("deletedAt")) else {
        return Err(ApiFailure::Changed(
            "thread snapshot has no valid deletedAt".into(),
        ));
    };

    Ok(ThreadSnapshot {
        runtime_mode,
        interaction_mode,
        archived: archived_at,
        deleted: deleted_at,
    })
}

fn non_empty_string(value: Option<&Value>) -> Option<String> {
    let value = value?.as_str()?;
    (!value.trim().is_empty()).then(|| value.to_owned())
}

fn timestamp_field(value: Option<&Value>) -> Option<bool> {
    match value? {
        Value::Null => Some(false),
        Value::String(timestamp) if !timestamp.trim().is_empty() => Some(true),
        _ => None,
    }
}

fn dispatch_turn(
    origin: &str,
    thread_id: &str,
    runtime_mode: &str,
    interaction_mode: &str,
    text: &str,
    token: &str,
) -> Result<(), ApiFailure> {
    let command_id = deterministic_id("homebased-t3-command", thread_id, text);
    let message_id = deterministic_id("homebased-t3-message", thread_id, text);
    let body = dispatch_body(
        thread_id,
        &command_id,
        &message_id,
        text,
        runtime_mode,
        interaction_mode,
    );
    let response = send_json(
        CurlMethod::Post,
        &format!("{origin}/api/orchestration/dispatch"),
        token,
        &body,
    )
    .map_err(ApiFailure::Unavailable)?;
    if response.status == 401 || response.status == 403 {
        return Err(ApiFailure::Changed(format!(
            "turn dispatch returned HTTP {} with a fresh session token",
            response.status
        )));
    }
    if response.status == 404 {
        return Err(classify_typed_404(&response.body, "turn dispatch"));
    }
    if response.status == 400 {
        return Err(ApiFailure::Changed(
            "turn dispatch returned HTTP 400".into(),
        ));
    }
    if response.status == 500 && has_dispatch_failed_reason(&response.body) {
        return Err(ApiFailure::Refused(
            "T3 could not dispatch the turn for this thread".into(),
        ));
    }
    if response.status != 200 {
        return Err(ApiFailure::Unavailable(format!(
            "turn dispatch returned HTTP {}",
            response.status
        )));
    }

    let value: Value = serde_json::from_str(&response.body)
        .map_err(|_| ApiFailure::Changed("turn dispatch response is not valid JSON".into()))?;
    if value.get("sequence").and_then(Value::as_u64).is_none() {
        return Err(ApiFailure::Changed(
            "turn dispatch response has no integer sequence".into(),
        ));
    }
    Ok(())
}

fn dispatch_body(
    thread_id: &str,
    command_id: &str,
    message_id: &str,
    text: &str,
    runtime_mode: &str,
    interaction_mode: &str,
) -> Value {
    json!({
        "type": "thread.turn.start",
        "commandId": command_id,
        "threadId": thread_id,
        "message": {
            "messageId": message_id,
            "role": "user",
            "text": text,
            "attachments": []
        },
        "runtimeMode": runtime_mode,
        "interactionMode": interaction_mode,
        "createdAt": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
    })
}

fn deterministic_id(salt: &str, thread_id: &str, text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt.as_bytes());
    hasher.update([0]);
    hasher.update(thread_id.as_bytes());
    hasher.update([0]);
    hasher.update(text.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Builder::from_custom_bytes(bytes).into_uuid().to_string()
}

fn send_empty(method: CurlMethod, url: &str, token: &str) -> Result<CurlResponse, String> {
    crate::curl::send(&CurlRequest {
        method,
        url,
        bearer: Some(token),
        json_body: None,
        timeout: HTTP_TIMEOUT,
    })
}

fn send_json(
    method: CurlMethod,
    url: &str,
    token: &str,
    body: &Value,
) -> Result<CurlResponse, String> {
    crate::curl::send(&CurlRequest {
        method,
        url,
        bearer: Some(token),
        json_body: Some(body),
        timeout: HTTP_TIMEOUT,
    })
}

fn classify_typed_404(body: &str, operation: &str) -> ApiFailure {
    let typed_error = serde_json::from_str::<Value>(body)
        .ok()
        .filter(Value::is_object);
    if let Some(tag) = typed_error
        .as_ref()
        .and_then(|value| value.get("_tag"))
        .and_then(Value::as_str)
        .filter(|tag| !tag.trim().is_empty())
    {
        let reason = typed_error
            .as_ref()
            .and_then(|value| value.get("reason"))
            .and_then(Value::as_str)
            .filter(|reason| !reason.trim().is_empty());
        let detail = reason.map_or_else(
            || format!("{operation} returned typed T3 error {tag}"),
            |reason| format!("{operation} returned typed T3 error {tag}: {reason}"),
        );
        ApiFailure::Refused(detail)
    } else {
        ApiFailure::Changed(format!(
            "{operation} returned HTTP 404 without a typed T3 error"
        ))
    }
}

fn has_dispatch_failed_reason(body: &str) -> bool {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|reason| reason == "orchestration_dispatch_failed")
}

fn is_unknown_thread_response(response: &CurlResponse) -> bool {
    response.status == 500 && has_dispatch_failed_reason(&response.body)
}

fn dispatch_probe_failure(response: &CurlResponse) -> String {
    if response.status == 400 {
        return "dispatch returned HTTP 400; command shape changed".into();
    }
    if response.status == 500 {
        return "dispatch did not return orchestration_dispatch_failed".into();
    }
    format!("dispatch contract probe returned HTTP {}", response.status)
}

fn encode_path_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn report(status: ProbeStatus, t3_version: Option<String>, checks: Vec<ProbeCheck>) -> ProbeReport {
    ProbeReport {
        status,
        t3_version,
        checks,
    }
}

fn passed(name: &'static str, detail: &str) -> ProbeCheck {
    ProbeCheck {
        name,
        ok: true,
        detail: detail.into(),
    }
}

fn failed(name: &'static str, detail: impl Into<String>) -> ProbeCheck {
    ProbeCheck {
        name,
        ok: false,
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use rusqlite::Connection;
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::*;

    const SESSION_ID: &str = "d74100ef-c9c2-4d79-85f2-62712b391e88";
    const T3_THREAD_ID: &str = "31c5fd73-3cc4-4ecb-a1cd-8f01c39fcb85";
    const TOKEN: &str = "fake-secret-token";

    #[derive(Clone)]
    struct FakeResponse {
        status: u16,
        body: String,
    }

    #[derive(Debug, Clone)]
    struct RecordedRequest {
        method: String,
        path: String,
        headers: String,
        body: String,
    }

    struct FakeServer {
        origin: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        worker: Option<JoinHandle<()>>,
    }

    impl FakeServer {
        fn start(responses: Vec<FakeResponse>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&requests);
            let worker = thread::spawn(move || {
                for response in responses {
                    let deadline = Instant::now() + Duration::from_secs(5);
                    let (mut stream, _) = loop {
                        match listener.accept() {
                            Ok(connection) => break connection,
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                if Instant::now() >= deadline {
                                    return;
                                }
                                thread::sleep(Duration::from_millis(10));
                            }
                            Err(_) => return,
                        }
                    };
                    let request = read_request(&mut stream);
                    if let Ok(request) = request
                        && let Ok(mut requests) = recorded.lock()
                    {
                        requests.push(request);
                    }
                    let reason = match response.status {
                        200 => "OK",
                        400 => "Bad Request",
                        401 => "Unauthorized",
                        403 => "Forbidden",
                        404 => "Not Found",
                        500 => "Internal Server Error",
                        _ => "Response",
                    };
                    let response_text = format!(
                        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response.status,
                        reason,
                        response.body.len(),
                        response.body
                    );
                    let _ = stream.write_all(response_text.as_bytes());
                    let _ = stream.flush();
                }
            });
            Self {
                origin: format!("http://{address}"),
                requests,
                worker: Some(worker),
            }
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl Drop for FakeServer {
        fn drop(&mut self) {
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    fn read_request(stream: &mut TcpStream) -> std::io::Result<RecordedRequest> {
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut first_line = String::new();
        reader.read_line(&mut first_line)?;
        let mut headers = String::new();
        let mut content_length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            if line == "\r\n" || line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                content_length = value.trim().parse::<usize>().unwrap();
            }
            headers.push_str(&line);
        }
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body)?;
        let mut parts = first_line.split_whitespace();
        let method = parts.next().unwrap().to_owned();
        let path = parts.next().unwrap().to_owned();
        Ok(RecordedRequest {
            method,
            path,
            headers,
            body: String::from_utf8(body).unwrap(),
        })
    }

    fn response(status: u16, body: Value) -> FakeResponse {
        FakeResponse {
            status,
            body: serde_json::to_string(&body).unwrap(),
        }
    }

    fn snapshot(thread_id: &str) -> Value {
        json!({
            "snapshotSequence": 9,
            "thread": {
                "id": thread_id,
                "title": "Fake thread",
                "modelSelection": { "model": "keep-existing" },
                "runtimeMode": "full-access",
                "interactionMode": "default",
                "archivedAt": null,
                "deletedAt": null,
                "session": { "status": "ready", "activeTurnId": null }
            },
            "page": {}
        })
    }

    fn dispatch_ok() -> FakeResponse {
        response(200, json!({ "sequence": 10 }))
    }

    fn curl_available() -> bool {
        which::which("curl").is_ok()
    }

    fn thread_id() -> ThreadId {
        ThreadId(Uuid::parse_str(SESSION_ID).unwrap())
    }

    struct Fixture {
        _dir: TempDir,
        env: T3Env,
        revoke_log: PathBuf,
    }

    impl Fixture {
        fn new(server: Option<&FakeServer>, mapped: bool, pid: i32) -> Self {
            let dir = TempDir::new().unwrap();
            let userdata = dir.path().join(".t3/userdata");
            fs::create_dir_all(&userdata).unwrap();
            let db = Connection::open(userdata.join("state.sqlite")).unwrap();
            db.execute_batch(
                "CREATE TABLE provider_session_runtime (
                    thread_id TEXT PRIMARY KEY,
                    provider_name TEXT,
                    adapter_key TEXT,
                    runtime_mode TEXT,
                    status TEXT,
                    last_seen_at TEXT,
                    resume_cursor_json TEXT,
                    runtime_payload_json TEXT,
                    provider_instance_id TEXT
                 );
                 CREATE TABLE projection_threads (
                    thread_id TEXT PRIMARY KEY,
                    title TEXT,
                    deleted_at TEXT
                 );",
            )
            .unwrap();
            if mapped {
                db.execute(
                    "INSERT INTO projection_threads (thread_id, title) VALUES (?1, 'Fake thread')",
                    [T3_THREAD_ID],
                )
                .unwrap();
                db.execute(
                    "INSERT INTO provider_session_runtime
                     (thread_id, provider_name, last_seen_at, resume_cursor_json)
                     VALUES (?1, 'claude', '2026-09-26T17:00:00Z', ?2)",
                    rusqlite::params![T3_THREAD_ID, json!({"resume": SESSION_ID}).to_string()],
                )
                .unwrap();
            }
            drop(db);

            let origin = server.map_or_else(
                || "http://127.0.0.1:1".to_string(),
                |server| server.origin.clone(),
            );
            let port = origin
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok())
                .unwrap_or(1);
            let runtime = json!({
                "version": 1,
                "pid": pid,
                "host": "127.0.0.1",
                "port": port,
                "origin": origin,
                "startedAt": "2026-09-26T17:03:50.514Z"
            });
            fs::write(
                userdata.join("server-runtime.json"),
                serde_json::to_vec(&runtime).unwrap(),
            )
            .unwrap();

            let bin = dir.path().join("bin");
            fs::create_dir_all(&bin).unwrap();
            let revoke_log = dir.path().join("revocations.log");
            let script_path = bin.join("t3");
            let script = format!(
                "#!/bin/sh\ncase \"$1 $2 $3\" in\n  'auth session issue') printf '%s\\n' '{{\"sessionId\":\"fake-session\",\"token\":\"{TOKEN}\",\"method\":\"bearer-access-token\",\"scopes\":[]}}' ;;\n  'auth session revoke') printf '%s\\n' \"$4\" >> {} ;;\n  *) if [ \"$1\" = '--version' ]; then printf '%s\\n' 't3 1.2.3'; else exit 3; fi ;;\nesac\n",
                shell_quote(&revoke_log.to_string_lossy())
            );
            fs::write(&script_path, script).unwrap();
            let mut permissions = fs::metadata(&script_path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script_path, permissions).unwrap();

            Self {
                env: T3Env::new(dir.path().to_path_buf(), bin.into_os_string()),
                _dir: dir,
                revoke_log,
            }
        }

        fn revocations(&self) -> Vec<String> {
            fs::read_to_string(&self.revoke_log)
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect()
        }
    }

    fn shell_quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\\''"))
    }

    fn live_pid() -> i32 {
        i32::try_from(std::process::id()).unwrap()
    }

    #[test]
    fn wake_success_reuses_stable_ids_and_revokes_tokens() {
        if !curl_available() {
            return;
        }
        let server = FakeServer::start(vec![
            response(200, snapshot(T3_THREAD_ID)),
            dispatch_ok(),
            response(200, snapshot(T3_THREAD_ID)),
            dispatch_ok(),
        ]);
        let fixture = Fixture::new(Some(&server), true, live_pid());
        let first = wake_claude_session(&fixture.env, thread_id(), "resume this task");
        let second = wake_claude_session(&fixture.env, thread_id(), "resume this task");

        assert_eq!(
            first,
            WakeOutcome::Woken {
                t3_thread: T3_THREAD_ID.into()
            }
        );
        assert_eq!(second, first);
        let requests = server.requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].method, "GET");
        assert_eq!(
            requests[0].path,
            format!("/api/orchestration/threads/{T3_THREAD_ID}?turnLimit=1")
        );
        assert!(requests[0].headers.contains(&format!("Bearer {TOKEN}")));
        let first_body: Value = serde_json::from_str(&requests[1].body).unwrap();
        let second_body: Value = serde_json::from_str(&requests[3].body).unwrap();
        assert_eq!(first_body["runtimeMode"], "full-access");
        assert_eq!(first_body["interactionMode"], "default");
        assert_eq!(first_body["message"]["text"], "resume this task");
        assert!(first_body.get("modelSelection").is_none());
        assert_eq!(first_body["commandId"], second_body["commandId"]);
        assert_eq!(
            first_body["message"]["messageId"],
            second_body["message"]["messageId"]
        );
        assert_eq!(fixture.revocations(), ["fake-session", "fake-session"]);
    }

    #[test]
    fn wake_returns_not_t3_thread_without_a_mapping() {
        let fixture = Fixture::new(None, false, live_pid());
        assert_eq!(
            wake_claude_session(&fixture.env, thread_id(), "message"),
            WakeOutcome::NotT3Thread
        );
        assert!(fixture.revocations().is_empty());
    }

    #[test]
    fn wake_returns_unavailable_for_a_dead_server_pid() {
        let fixture = Fixture::new(None, true, i32::MAX);
        assert!(matches!(
            wake_claude_session(&fixture.env, thread_id(), "message"),
            WakeOutcome::Unavailable(_)
        ));
        assert!(fixture.revocations().is_empty());
    }

    #[test]
    fn wake_refuses_archived_threads_and_revokes_the_token() {
        if !curl_available() {
            return;
        }
        let mut archived = snapshot(T3_THREAD_ID);
        archived["thread"]["archivedAt"] = json!("2026-09-26T17:00:00Z");
        let server = FakeServer::start(vec![response(200, archived)]);
        let fixture = Fixture::new(Some(&server), true, live_pid());

        assert_eq!(
            wake_claude_session(&fixture.env, thread_id(), "message"),
            WakeOutcome::Refused("T3 thread is archived".into())
        );
        assert_eq!(server.requests().len(), 1);
        assert_eq!(fixture.revocations(), ["fake-session"]);
    }

    #[test]
    fn wake_marks_dispatch_400_as_changed_and_revokes_the_token() {
        if !curl_available() {
            return;
        }
        let server = FakeServer::start(vec![
            response(200, snapshot(T3_THREAD_ID)),
            response(400, json!({})),
        ]);
        let fixture = Fixture::new(Some(&server), true, live_pid());

        assert_eq!(
            wake_claude_session(&fixture.env, thread_id(), "message"),
            WakeOutcome::ApiChanged("turn dispatch returned HTTP 400".into())
        );
        assert_eq!(fixture.revocations(), ["fake-session"]);
    }

    #[test]
    fn probe_reports_compatible_api_and_version() {
        if !curl_available() {
            return;
        }
        let server = FakeServer::start(vec![
            response(200, snapshot(T3_THREAD_ID)),
            response(
                500,
                json!({
                    "_tag": "EnvironmentInternalError",
                    "code": "internal_error",
                    "reason": "orchestration_dispatch_failed",
                    "traceId": "discard-this"
                }),
            ),
        ]);
        let fixture = Fixture::new(Some(&server), true, live_pid());

        let report = probe(&fixture.env);
        assert_eq!(report.status, ProbeStatus::Compatible);
        assert_eq!(report.t3_version.as_deref(), Some("t3 1.2.3"));
        assert!(report.checks.iter().all(|check| check.ok));
        assert!(fixture.revocations().contains(&"fake-session".to_string()));
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        let dispatch: Value = serde_json::from_str(&requests[1].body).unwrap();
        assert_eq!(dispatch["type"], "thread.turn.start");
        assert_ne!(dispatch["threadId"], T3_THREAD_ID);
        assert!(dispatch.get("modelSelection").is_none());
    }

    #[test]
    fn probe_fingerprints_a_missing_snapshot_field_stably() {
        if !curl_available() {
            return;
        }
        let mut invalid = snapshot(T3_THREAD_ID);
        invalid["thread"]
            .as_object_mut()
            .unwrap()
            .remove("runtimeMode");
        let server =
            FakeServer::start(vec![response(200, invalid.clone()), response(200, invalid)]);
        let fixture = Fixture::new(Some(&server), true, live_pid());

        let first = probe(&fixture.env);
        let second = probe(&fixture.env);
        assert_eq!(first.status, ProbeStatus::Changed);
        assert_eq!(first.t3_version.as_deref(), Some("t3 1.2.3"));
        assert_eq!(
            first.fingerprint().as_deref(),
            Some("thread_snapshot: thread snapshot has no valid runtimeMode")
        );
        assert_eq!(first.fingerprint(), second.fingerprint());
        assert_eq!(fixture.revocations(), ["fake-session", "fake-session"]);
    }

    #[test]
    fn probe_reports_not_installed_and_not_running() {
        let dir = TempDir::new().unwrap();
        let no_userdata = T3Env::new(dir.path().to_path_buf(), OsString::new());
        assert_eq!(probe(&no_userdata).status, ProbeStatus::NotInstalled);

        let fixture = Fixture::new(None, true, i32::MAX);
        assert_eq!(probe(&fixture.env).status, ProbeStatus::NotRunning);
    }

    #[test]
    fn typed_404_drops_trace_id_from_refusal_detail() {
        let error = classify_typed_404(
            r#"{"_tag":"SomeT3Error","reason":"thread_closed","traceId":"secret-trace"}"#,
            "thread snapshot",
        );
        assert!(
            matches!(error, ApiFailure::Refused(detail) if detail == "thread snapshot returned typed T3 error SomeT3Error: thread_closed")
        );
    }

    #[test]
    fn deterministic_ids_are_salted_and_stable() {
        let first = deterministic_id("command", T3_THREAD_ID, "text");
        assert_eq!(first, deterministic_id("command", T3_THREAD_ID, "text"));
        assert_ne!(first, deterministic_id("message", T3_THREAD_ID, "text"));
    }
}
