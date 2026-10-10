//! T3 Code client for waking provider sessions and checking its local API
//!
//! # T3 Code internals this depends on
//!
//! T3 Code has no public API for these operations, so this client pins local
//! contracts that an update can change without notice. Run `homebased t3 check`
//! after a T3 update
//!
//! T3 speaks one of two orchestration protocols: [`v1`] in stable `0.0.45` and
//! earlier, and [`v2`] after pingdotgg/t3code#2829. Every wake and probe asks
//! the running server which one it speaks, so an update that ships V2 needs no
//! homebased change unless V2 changed again first
//!
//! Both protocols share:
//!
//! - User data lives in `~/.t3/userdata`; `server-runtime.json` has `version`,
//!   `pid`, and `origin`. Version 1 and a live PID identify a running server.
//!   The process check uses `kill(pid, None)`. The desktop app also writes
//!   `host`, `port`, and `startedAt`; a service-managed server omits `host`
//!   and adds `serviceManaged`, so the client ignores those fields
//! - `GET /.well-known/t3/environment` needs no token and returns
//!   `orchestrationProtocolVersion`, `1` or `2`
//! - The `t3` CLI is the running server's executable
//!   (`~/.t3/runtime/versions/<version>/t3`), found through `/proc/<pid>/exe`
//!   or `ps`, else `t3` on PATH. The macOS desktop app does not put it on PATH
//! - `t3 auth session issue --json --ttl 5m --label homebased` returns
//!   `sessionId` and `token` for a bearer session. Revoke it with
//!   `t3 auth session revoke <sessionId>` after every use; `t3 --version`
//!   supplies the optional probe version
//!
//! Calls use the blocking system `curl` helper, a blocking WebSocket, and `t3`
//! commands with a deadline. The callback dispatcher must call this module from
//! `spawn_blocking`

mod rpc;
mod v1;
mod v2;

use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Builder;

use crate::callback::send_check::{SendCheck, SendFailure};
use crate::curl::{CurlMethod, CurlRequest, CurlResponse};
use crate::domain::ThreadId;

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_millis(250);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
// the descriptor read gates live Claude deliveries, so a stalled T3 must not hold them long
const DESCRIPTOR_TIMEOUT: Duration = Duration::from_secs(2);
const CLI_TIMEOUT: Duration = Duration::from_secs(10);
const CLI_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const CLI_OUTPUT_LIMIT: u64 = 64 * 1024;

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
    ///
    /// Without `HOME`, the home directory comes from the password database
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            home: std::env::home_dir().unwrap_or_default(),
            path: std::env::var_os("PATH").unwrap_or_default(),
        }
    }

    fn userdata(&self) -> PathBuf {
        self.home.join(".t3/userdata")
    }
}

/// T3 orchestration protocol, which decides the state database and turn API
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// `state.sqlite` and HTTP dispatch, see [`v1`]
    V1,
    /// `statev2.sqlite` and WebSocket RPC dispatch, see [`v2`]
    V2,
}

impl Protocol {
    /// Wire number that T3 reports as `orchestrationProtocolVersion`
    #[must_use]
    pub fn number(self) -> u32 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }
}

/// Provider session that T3 can resume in its owning thread
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderThread {
    /// Claude session id
    Claude(ThreadId),
    /// Codex thread id
    Codex(ThreadId),
}

impl ProviderThread {
    /// T3 provider name, the V1 resume cursor path, and the native id
    fn mapping(self) -> (&'static str, &'static str, ThreadId) {
        match self {
            Self::Claude(thread) => ("claudeAgent", "$.resume", thread),
            Self::Codex(thread) => ("codex", "$.threadId", thread),
        }
    }
}

/// Result of asking T3 Code to start a turn in a provider session's owning thread
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeOutcome {
    /// T3 accepted the new turn
    Woken { t3_thread: String },
    /// No T3 thread owns this provider session
    NotT3Thread,
    /// T3 is not installed or running, or a transport failed
    Unavailable(String),
    /// T3 is reachable but will not run the turn
    Refused(String),
    /// The request reached T3 but its reply did not come back, so T3 may have
    /// started the turn. Only a retry through T3 is safe, because T3 drops a
    /// repeated command
    Uncertain(String),
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
    /// T3 runs, but homebased cannot use it now, for example its CLI cannot be found
    Unavailable,
}

impl ProbeStatus {
    /// Stable name, matching the JSON value
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotInstalled => "not_installed",
            Self::NotRunning => "not_running",
            Self::Compatible => "compatible",
            Self::Changed => "changed",
            Self::Unavailable => "unavailable",
        }
    }
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
    /// Orchestration protocol the server reported, when it answered
    pub orchestration_protocol: Option<u32>,
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

    fn finish(mut self, status: ProbeStatus) -> Self {
        self.status = status;
        self
    }
}

/// Protocol of the T3 thread that owns this provider session, if one does
///
/// Reads every state database on disk first, so a session that no database
/// maps needs no request to T3. A mapped session counts only in the database of
/// the running server's protocol; an error means T3 could not say which
pub fn owner(env: &T3Env, provider_thread: ProviderThread) -> Result<Option<Protocol>, String> {
    let userdata = env.userdata();
    if !userdata.is_dir() {
        return Ok(None);
    }

    let lookups = Store::lookups(&userdata, provider_thread);
    if lookups.iter().all(|(_, found)| matches!(found, Ok(None))) {
        return Ok(None);
    }
    let server = connect(&userdata).map_err(|failure| failure.detail())?;
    match Store::active(lookups, server.protocol) {
        Some((store, Ok(Some(_)))) => Ok(Some(store.protocol())),
        Some((_, Err(detail))) => Err(detail),
        Some((_, Ok(None))) | None => Ok(None),
    }
}

/// Message text that T3 runs as a compaction turn instead of a prompt
const COMPACT_COMMAND: &str = "/compact";

/// How a woken T3 V2 thread runs the message relative to its active run
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnStart {
    /// T3 starts the message, steers the active run with it, or queues it
    Auto,
    /// The message waits for the active run, which may be a compaction that
    /// T3 refuses to steer into
    AfterActive,
}

/// Start a T3 turn for the thread that owns this provider session
///
/// `before_send` rechecks an unsent notice at the final dispatch boundary.
/// Protocol 1 ignores `start` and lets T3 place the message
pub(crate) fn wake_thread_checked(
    env: &T3Env,
    provider_thread: ProviderThread,
    text: &str,
    before_send: Option<&SendCheck>,
    start: TurnStart,
) -> Result<WakeOutcome, SendFailure> {
    with_owner(env, provider_thread, |owner| match owner.store {
        Store::V1(_) => v1::start_turn(owner.origin, owner.thread, text, owner.token, before_send),
        Store::V2(path) => v2::start_turn(
            owner.origin,
            path,
            owner.thread,
            text,
            owner.token,
            before_send,
            start,
        ),
    })
}

/// Start a `/compact` turn in the T3 thread that owns a Claude session
///
/// `key` names the idle period being compacted, so T3 drops a repeated
/// request for it. T3 refuses it while a turn runs
pub(crate) fn compact_thread(env: &T3Env, thread: ThreadId, key: &str) -> WakeOutcome {
    with_owner(env, ProviderThread::Claude(thread), |owner| {
        match owner.store {
            Store::V1(_) => v1::compact(owner.origin, owner.thread, key, owner.token),
            Store::V2(_) => v2::compact(owner.origin, owner.thread, key, owner.token, None),
        }
    })
    .unwrap_or_else(|failure| WakeOutcome::Unavailable(failure.to_string()))
}

/// Open T3 thread that owns a Claude session
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenClaudeThread {
    /// Claude Code session id
    pub(crate) session: ThreadId,
    /// T3 thread id
    pub(crate) t3_thread: String,
    /// T3 thread title
    pub(crate) title: String,
}

/// Session id, T3 thread id, and title of one row in a state database
type ClaudeThreadRow = (String, String, String);

/// Claude sessions whose T3 thread is neither archived nor deleted, newest first
///
/// Reads the state database on disk, so the server need not run. V2 stops
/// writing `state.sqlite` once it copied it, so a usable `statev2.sqlite` wins
pub(crate) fn open_claude_threads(env: &T3Env) -> Result<Vec<OpenClaudeThread>, String> {
    let userdata = env.userdata();
    let store = [Protocol::V2, Protocol::V1]
        .into_iter()
        .map(|protocol| Store::path(&userdata, protocol))
        .find(|store| store.file().is_file() && store.schema_matches());
    let rows = match &store {
        Some(Store::V1(path)) => v1::open_claude_threads(path)?,
        Some(Store::V2(path)) => v2::open_claude_threads(path)?,
        None => return Ok(Vec::new()),
    };

    let mut threads: Vec<OpenClaudeThread> = Vec::new();
    for (session, t3_thread, title) in rows {
        // a session id that is not a UUID cannot be a Claude Code session
        let Ok(session) = session.parse::<ThreadId>() else {
            continue;
        };
        if !threads.iter().any(|thread| thread.session == session) {
            threads.push(OpenClaudeThread {
                session,
                t3_thread,
                title,
            });
        }
    }
    Ok(threads)
}

/// Every row of a query that selects a session id, T3 thread id, and title
fn claude_thread_rows(path: &Path, sql: &str) -> Result<Vec<ClaudeThreadRow>, String> {
    let db = state_connection(path).map_err(|_| "T3 state cannot be read".to_string())?;
    let mut statement = db
        .prepare(sql)
        .map_err(|_| "T3 thread list query failed".to_string())?;
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .and_then(Iterator::collect)
        .map_err(|_| "T3 thread list query failed".to_string())
}

/// Running T3 server, thread, and bearer token for one provider session
struct Owner<'a> {
    origin: &'a str,
    store: &'a Store,
    thread: &'a str,
    token: &'a str,
}

/// Run `action` against the T3 thread that owns `provider_thread`
///
/// The bearer token is revoked after `action` returns
fn with_owner(
    env: &T3Env,
    provider_thread: ProviderThread,
    action: impl FnOnce(Owner<'_>) -> Result<(), ApiFailure>,
) -> Result<WakeOutcome, SendFailure> {
    let userdata = env.userdata();
    if !userdata.is_dir() {
        return Ok(WakeOutcome::NotT3Thread);
    }

    let lookups = Store::lookups(&userdata, provider_thread);
    if lookups.iter().all(|(_, found)| matches!(found, Ok(None))) {
        return Ok(WakeOutcome::NotT3Thread);
    }
    let server = match connect(&userdata) {
        Ok(server) => server,
        Err(failure) => return Ok(failure.into_wake_outcome()),
    };
    let (store, t3_thread) = match Store::active(lookups, server.protocol) {
        Some((store, Ok(Some(t3_thread)))) => (store, t3_thread),
        Some((_, Err(detail))) => return Ok(WakeOutcome::Unavailable(detail)),
        Some((_, Ok(None))) | None => return Ok(WakeOutcome::NotT3Thread),
    };
    let cli = match T3Cli::resolve(env, server.pid) {
        Ok(cli) => cli,
        Err(detail) => return Ok(WakeOutcome::Unavailable(detail)),
    };
    let token = match cli.issue_token() {
        Ok(token) => token,
        Err(IssueError::Unavailable(detail)) => return Ok(WakeOutcome::Unavailable(detail)),
        Err(IssueError::Changed(detail)) => return Ok(WakeOutcome::ApiChanged(detail)),
    };

    let done = action(Owner {
        origin: &server.origin,
        store: &store,
        thread: &t3_thread,
        token: &token.value,
    });
    match done {
        Ok(()) => Ok(WakeOutcome::Woken { t3_thread }),
        Err(ApiFailure::Delivery(failure)) => Err(failure),
        Err(failure) => Ok(failure.into_wake_outcome()),
    }
}

/// Check the T3 local API without starting a turn in an existing thread
#[must_use]
pub fn probe(env: &T3Env) -> ProbeReport {
    let userdata = env.userdata();
    let mut report = ProbeReport {
        status: ProbeStatus::Compatible,
        t3_version: None,
        orchestration_protocol: None,
        checks: Vec::new(),
    };
    if !userdata.is_dir() {
        report
            .checks
            .push(failed("userdata", "T3 user data directory is missing"));
        return report.finish(ProbeStatus::NotInstalled);
    }
    report
        .checks
        .push(passed("userdata", "T3 user data directory exists"));

    let runtime = match load_runtime(&userdata) {
        Ok(runtime) => runtime,
        Err(detail) => {
            report.checks.push(failed("server", detail));
            return report.finish(ProbeStatus::NotRunning);
        }
    };
    report.checks.push(passed(
        "server",
        "T3 server runtime is valid and its process is alive",
    ));

    let cli = T3Cli::resolve(env, runtime.pid);
    report.t3_version = cli.as_ref().ok().and_then(T3Cli::version);

    let protocol = match server_protocol(&runtime.origin) {
        Ok(protocol) => protocol,
        Err(failure) => {
            report.checks.push(failed("protocol", failure.detail()));
            return report.finish(failure.probe_status());
        }
    };
    report.orchestration_protocol = Some(protocol.number());
    report.checks.push(passed(
        "protocol",
        &format!("server speaks orchestration protocol {}", protocol.number()),
    ));

    let store = Store::path(&userdata, protocol);
    if !store.schema_matches() {
        report.checks.push(failed(
            "state_schema",
            format!(
                "{} does not have the required thread columns",
                store.file_name()
            ),
        ));
        return report.finish(ProbeStatus::Changed);
    }
    report.checks.push(passed(
        "state_schema",
        &format!("required {} columns exist", store.file_name()),
    ));

    let cli = match cli {
        Ok(cli) => cli,
        Err(detail) => {
            report.checks.push(failed("token_issue", detail));
            return report.finish(ProbeStatus::Unavailable);
        }
    };
    let token = match cli.issue_token() {
        Ok(token) => token,
        Err(IssueError::Unavailable(detail)) => {
            report.checks.push(failed("token_issue", detail));
            return report.finish(ProbeStatus::Unavailable);
        }
        Err(IssueError::Changed(detail)) => {
            report.checks.push(failed("token_issue", detail));
            return report.finish(ProbeStatus::Changed);
        }
    };
    report
        .checks
        .push(passed("token_issue", "short-lived session token issued"));

    let status = match &store {
        Store::V1(path) => v1::probe_api(&runtime.origin, path, &token.value, &mut report.checks),
        Store::V2(_) => v2::probe_api(&runtime.origin, &token.value, &mut report.checks),
    };
    report.finish(status)
}

/// T3 thread that one state database maps a provider session to
type Lookup = Result<Option<String>, String>;

/// State database of one orchestration protocol
enum Store {
    /// `state.sqlite`
    V1(PathBuf),
    /// `statev2.sqlite`
    V2(PathBuf),
}

impl Store {
    fn path(userdata: &Path, protocol: Protocol) -> Self {
        match protocol {
            Protocol::V1 => Self::V1(userdata.join("state.sqlite")),
            Protocol::V2 => Self::V2(userdata.join("statev2.sqlite")),
        }
    }

    /// The lookup of `provider_thread` in each state database on disk
    ///
    /// Each database answers on its own, so an unreadable inactive database
    /// cannot hide a mapping in the active one
    fn lookups(userdata: &Path, provider_thread: ProviderThread) -> Vec<(Self, Lookup)> {
        [Protocol::V2, Protocol::V1]
            .into_iter()
            .map(|protocol| Self::path(userdata, protocol))
            .filter(|store| store.file().is_file())
            .map(|store| {
                let found = store.find_thread(provider_thread);
                (store, found)
            })
            .collect()
    }

    /// The lookup in the running server's database
    ///
    /// V2 copies `state.sqlite` into `statev2.sqlite` once and then stops
    /// writing `state.sqlite`, so only that database is current
    fn active(lookups: Vec<(Self, Lookup)>, protocol: Protocol) -> Option<(Self, Lookup)> {
        lookups
            .into_iter()
            .find(|(store, _)| store.protocol() == protocol)
    }

    fn file(&self) -> &Path {
        match self {
            Self::V1(path) | Self::V2(path) => path,
        }
    }

    fn file_name(&self) -> &'static str {
        match self {
            Self::V1(_) => "state.sqlite",
            Self::V2(_) => "statev2.sqlite",
        }
    }

    fn protocol(&self) -> Protocol {
        match self {
            Self::V1(_) => Protocol::V1,
            Self::V2(_) => Protocol::V2,
        }
    }

    fn find_thread(&self, provider_thread: ProviderThread) -> Result<Option<String>, String> {
        match self {
            Self::V1(path) => v1::find_thread(path, provider_thread),
            Self::V2(path) => v2::find_thread(path, provider_thread),
        }
    }

    fn schema_matches(&self) -> bool {
        match self {
            Self::V1(path) => v1::schema_matches(path),
            Self::V2(path) => v2::schema_matches(path),
        }
    }
}

/// Running T3 server and the orchestration protocol it reports
#[derive(Debug, Clone)]
struct Server {
    pid: i32,
    origin: String,
    protocol: Protocol,
}

fn connect(userdata: &Path) -> Result<Server, ApiFailure> {
    let runtime = load_runtime(userdata).map_err(ApiFailure::Unavailable)?;
    let protocol = server_protocol(&runtime.origin)?;
    Ok(Server {
        pid: runtime.pid,
        origin: runtime.origin,
        protocol,
    })
}

fn server_protocol(origin: &str) -> Result<Protocol, ApiFailure> {
    let response = crate::curl::send(&CurlRequest {
        method: CurlMethod::Get,
        url: &format!("{origin}/.well-known/t3/environment"),
        bearer: None,
        json_body: None,
        timeout: DESCRIPTOR_TIMEOUT,
    })
    .map_err(ApiFailure::Unavailable)?;
    if response.status >= 500 {
        return Err(ApiFailure::Unavailable(format!(
            "environment descriptor returned HTTP {}",
            response.status
        )));
    }
    if response.status != 200 {
        return Err(ApiFailure::Changed(format!(
            "environment descriptor returned HTTP {}",
            response.status
        )));
    }

    let value: Value = serde_json::from_str(&response.body)
        .map_err(|_| ApiFailure::Changed("environment descriptor is not valid JSON".into()))?;
    let version = match value.get("orchestrationProtocolVersion") {
        None | Some(Value::Null) => None,
        Some(version) => Some(version.as_u64().ok_or_else(|| {
            ApiFailure::Changed("orchestrationProtocolVersion is not an integer".into())
        })?),
    };
    match version {
        // T3 `0.0.42` and earlier omit the field, which T3 defines as protocol 1
        None | Some(1) => Ok(Protocol::V1),
        Some(2) => Ok(Protocol::V2),
        Some(version) => Err(ApiFailure::Changed(format!(
            "server speaks unsupported orchestration protocol {version}"
        ))),
    }
}

#[derive(Debug, Deserialize)]
struct RuntimeFile {
    version: u32,
    pid: i32,
    origin: String,
}

fn load_runtime(userdata: &Path) -> Result<RuntimeFile, String> {
    let path = userdata.join("server-runtime.json");
    let bytes = fs::read(path).map_err(|_| "T3 server runtime file is missing".to_string())?;
    let runtime: RuntimeFile = serde_json::from_slice(&bytes)
        .map_err(|_| "T3 server runtime file is invalid".to_string())?;
    // a service-managed server omits `host`, so read only the fields the client uses
    if runtime.version != 1 || runtime.pid <= 0 || !valid_origin(&runtime.origin) {
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

fn query_thread(
    path: &Path,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Option<String>, String> {
    let db = state_connection(path).map_err(|_| "state.sqlite cannot be read".to_string())?;
    db.query_row(sql, params, |row| row.get(0))
        .optional()
        .map_err(|_| "state.sqlite thread lookup failed".to_string())
}

/// The `t3` executable, run with the PATH and HOME that located it
#[derive(Debug, Clone)]
struct T3Cli {
    executable: PathBuf,
    env: T3Env,
    timeout: Duration,
}

/// Exit result and bounded stdout from one `t3` command
struct CliOutput {
    success: bool,
    stdout: Vec<u8>,
}

/// Executable of the running T3 server, when it is the `t3` binary
///
/// The server binary is also the CLI and always matches the server version.
/// The macOS desktop app starts it from `~/.t3/runtime/versions/<version>/t3`
/// without putting `t3` on PATH
fn server_executable(pid: i32) -> Option<PathBuf> {
    let executable = fs::read_link(format!("/proc/{pid}/exe"))
        .ok()
        .or_else(|| process_path_from_ps(pid))?;
    let is_t3 = executable.file_name().is_some_and(|name| name == "t3");
    (is_t3 && executable.is_file()).then_some(executable)
}

/// macOS has no `/proc`; `ps -o comm=` prints the full executable path there
fn process_path_from_ps(pid: i32) -> Option<PathBuf> {
    let output = Command::new("ps")
        .args(["-o", "comm=", "-p", &pid.to_string()])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    let path = String::from_utf8(output.stdout).ok()?;
    let path = path.trim();
    (output.status.success() && path.starts_with('/')).then(|| PathBuf::from(path))
}

impl T3Cli {
    fn resolve(env: &T3Env, server_pid: i32) -> Result<Self, String> {
        let executable = match server_executable(server_pid) {
            Some(executable) => executable,
            None => {
                let cwd = std::env::current_dir()
                    .map_err(|_| "could not read current directory".to_string())?;
                which::which_in("t3", Some(&env.path), &cwd)
                    .map_err(|_| "t3 is not installed or is not on PATH".to_string())?
            }
        };
        Ok(Self {
            executable,
            env: env.clone(),
            timeout: CLI_TIMEOUT,
        })
    }

    /// Run one command and kill its process group at the deadline
    ///
    /// A hung `t3` would otherwise hold a callback worker and a blocking
    /// thread forever
    fn run(&self, label: &str, args: &[&str]) -> Result<CliOutput, String> {
        // `t3` prefers an inherited `T3CODE_HOME` over `HOME`, so pin it to the
        // user data this client reads; a shell inside T3 Code sets it
        let mut child = Command::new(&self.executable)
            .args(args)
            .env("PATH", &self.env.path)
            .env("HOME", &self.env.home)
            .env("T3CODE_HOME", self.env.home.join(".t3"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .map_err(|_| format!("could not start {label}"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| format!("could not read {label} output"))?;
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let mut bytes = Vec::new();
            let _ = stdout.take(CLI_OUTPUT_LIMIT).read_to_end(&mut bytes);
            let _ = sender.send(bytes);
        });

        let deadline = Instant::now() + self.timeout;
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|_| format!("could not wait for {label}"))?
            {
                break status;
            }
            if Instant::now() >= deadline {
                if let Ok(pid) = i32::try_from(child.id()) {
                    let _ = kill(Pid::from_raw(-pid), Signal::SIGKILL);
                }
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "{label} timed out after {}s",
                    self.timeout.as_secs_f32()
                ));
            }
            thread::sleep(Duration::from_millis(20));
        };
        // a descendant can keep the pipe open after the direct child exits
        let stdout = receiver.recv_timeout(CLI_DRAIN_TIMEOUT).unwrap_or_default();
        Ok(CliOutput {
            success: status.success(),
            stdout,
        })
    }

    fn issue_token(&self) -> Result<IssuedToken, IssueError> {
        let output = self
            .run(
                "t3 auth session issue",
                &[
                    "auth",
                    "session",
                    "issue",
                    "--json",
                    "--ttl",
                    "5m",
                    "--label",
                    "homebased",
                ],
            )
            .map_err(IssueError::Unavailable)?;
        let parsed = serde_json::from_slice::<Value>(&output.stdout);
        // dropping the revoker on any early return revokes a session that was
        // issued, even when the command reports failure
        let revoker = parsed
            .as_ref()
            .ok()
            .and_then(|value| non_empty_string(value.get("sessionId")))
            .map(|session_id| TokenRevoker {
                cli: self.clone(),
                session_id,
            });
        if !output.success {
            return Err(IssueError::Unavailable(
                "t3 auth session issue failed".into(),
            ));
        }

        let value =
            parsed.map_err(|_| IssueError::Changed("t3 token output is not valid JSON".into()))?;
        let Some(revoker) = revoker else {
            return Err(IssueError::Changed(
                "t3 token output has no sessionId".into(),
            ));
        };
        if value.get("method").and_then(Value::as_str) != Some("bearer-access-token") {
            return Err(IssueError::Changed(
                "t3 token output has an unexpected method".into(),
            ));
        }
        if value.get("scopes").and_then(Value::as_array).is_none() {
            return Err(IssueError::Changed(
                "t3 token output has no scopes array".into(),
            ));
        }
        let Some(token) = non_empty_string(value.get("token")) else {
            return Err(IssueError::Changed("t3 token output has no token".into()));
        };

        Ok(IssuedToken {
            value: token,
            _revoker: revoker,
        })
    }

    fn version(&self) -> Option<String> {
        let output = self.run("t3 --version", &["--version"]).ok()?;
        if !output.success {
            return None;
        }
        let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        (!version.is_empty()).then_some(version)
    }
}

enum IssueError {
    Unavailable(String),
    Changed(String),
}

struct IssuedToken {
    value: String,
    _revoker: TokenRevoker,
}

/// Revokes one issued T3 session when dropped
struct TokenRevoker {
    cli: T3Cli,
    session_id: String,
}

impl Drop for TokenRevoker {
    fn drop(&mut self) {
        let _ = self.cli.run(
            "t3 auth session revoke",
            &["auth", "session", "revoke", &self.session_id],
        );
    }
}

#[derive(Debug)]
enum ApiFailure {
    Delivery(SendFailure),
    Unavailable(String),
    Refused(String),
    /// Sent, but the reply was lost; see [`WakeOutcome::Uncertain`]
    Uncertain(String),
    Changed(String),
}

impl ApiFailure {
    fn detail(&self) -> String {
        match self {
            Self::Delivery(failure) => failure.to_string(),
            Self::Unavailable(detail)
            | Self::Refused(detail)
            | Self::Uncertain(detail)
            | Self::Changed(detail) => detail.clone(),
        }
    }

    fn into_wake_outcome(self) -> WakeOutcome {
        match self {
            Self::Delivery(failure) => WakeOutcome::Unavailable(failure.to_string()),
            Self::Unavailable(detail) => WakeOutcome::Unavailable(detail),
            Self::Refused(detail) => WakeOutcome::Refused(detail),
            Self::Uncertain(detail) => WakeOutcome::Uncertain(detail),
            Self::Changed(detail) => WakeOutcome::ApiChanged(detail),
        }
    }

    fn probe_status(&self) -> ProbeStatus {
        match self {
            Self::Delivery(_) => ProbeStatus::Unavailable,
            Self::Unavailable(_) | Self::Refused(_) | Self::Uncertain(_) => {
                ProbeStatus::Unavailable
            }
            Self::Changed(_) => ProbeStatus::Changed,
        }
    }
}

fn non_empty_string(value: Option<&Value>) -> Option<String> {
    let value = value?.as_str()?;
    (!value.trim().is_empty()).then(|| value.to_owned())
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

/// Percent-encode everything except RFC 3986 unreserved bytes
fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
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
pub(crate) mod test_support;

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use rusqlite::Connection;
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::test_support::{FakeResponse, FakeT3Server, V2State, rpc_exit, write_runtime};
    use super::{
        ProbeStatus, Protocol, ProviderThread, T3Cli, T3Env, TurnStart, WakeOutcome,
        compact_thread, deterministic_id, load_runtime, owner, probe, wake_thread_checked,
    };
    use crate::domain::ThreadId;

    /// Start a T3 turn with no notice to recheck
    fn wake_thread(env: &T3Env, provider_thread: ProviderThread, text: &str) -> WakeOutcome {
        wake_thread_checked(env, provider_thread, text, None, TurnStart::Auto)
            .unwrap_or_else(|failure| WakeOutcome::Unavailable(failure.to_string()))
    }

    const SESSION_ID: &str = "d74100ef-c9c2-4d79-85f2-62712b391e88";
    const T3_THREAD_ID: &str = "31c5fd73-3cc4-4ecb-a1cd-8f01c39fcb85";
    const TOKEN: &str = "fake-secret-token";

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
        FakeResponse::http(200, &json!({ "sequence": 10 }))
    }

    fn ticket() -> FakeResponse {
        FakeResponse::http(
            200,
            &json!({ "ticket": "fake-ticket", "expiresAt": "later" }),
        )
    }

    fn dispatched() -> FakeResponse {
        FakeResponse::WebSocket(vec![rpc_exit(
            json!({ "_tag": "Success", "value": { "sequence": 47 } }),
        )])
    }

    fn unknown_thread_error() -> FakeResponse {
        FakeResponse::WebSocket(vec![rpc_exit(json!({
            "_tag": "Failure",
            "cause": [{
                "_tag": "Fail",
                "error": {
                    "_tag": "OrchestrationV2DispatchCommandError",
                    "commandType": "message.dispatch",
                    "message": "No orchestration projection exists for thread 0000."
                }
            }]
        }))])
    }

    fn curl_available() -> bool {
        which::which("curl").is_ok()
    }

    fn thread_id() -> ThreadId {
        ThreadId(Uuid::parse_str(SESSION_ID).unwrap())
    }

    fn claude_thread() -> ProviderThread {
        ProviderThread::Claude(thread_id())
    }

    fn codex_thread() -> ProviderThread {
        ProviderThread::Codex(thread_id())
    }

    struct Fixture {
        dir: TempDir,
        env: T3Env,
        revoke_log: PathBuf,
    }

    impl Fixture {
        fn new(server: Option<&FakeT3Server>, mapped: bool, pid: i32) -> Self {
            let mapping = mapped.then_some(("claudeAgent", "resume", SESSION_ID));
            Self::with_mapping(server, mapping, pid)
        }

        /// A V1 `state.sqlite`, optionally mapping one provider session
        fn with_mapping(
            server: Option<&FakeT3Server>,
            mapping: Option<(&str, &str, &str)>,
            pid: i32,
        ) -> Self {
            let fixture = Self::empty(server, pid);
            fixture.v1_state(mapping);
            fixture
        }

        fn v1_state(&self, mapping: Option<(&str, &str, &str)>) {
            let db = Connection::open(self.userdata().join("state.sqlite")).unwrap();
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
            let Some((provider, cursor, session_id)) = mapping else {
                return;
            };
            db.execute(
                "INSERT INTO projection_threads (thread_id, title) VALUES (?1, 'Fake thread')",
                [T3_THREAD_ID],
            )
            .unwrap();
            db.execute(
                "INSERT INTO provider_session_runtime
                 (thread_id, provider_name, last_seen_at, resume_cursor_json)
                 VALUES (?1, ?2, '2026-09-26T17:00:00Z', ?3)",
                rusqlite::params![
                    T3_THREAD_ID,
                    provider,
                    json!({(cursor): session_id}).to_string()
                ],
            )
            .unwrap();
        }

        /// User data with a runtime file and a fake `t3` CLI, but no state database
        fn empty(server: Option<&FakeT3Server>, pid: i32) -> Self {
            let dir = TempDir::new().unwrap();
            let userdata = dir.path().join(".t3/userdata");
            fs::create_dir_all(&userdata).unwrap();
            let origin = server.map_or("http://127.0.0.1:1", |server| server.origin.as_str());
            write_runtime(&userdata, origin, pid);

            let bin = dir.path().join("bin");
            fs::create_dir_all(&bin).unwrap();
            let revoke_log = dir.path().join("revocations.log");
            let script_path = bin.join("t3");
            let script = format!(
                "#!/bin/sh\ncase \"$1 $2 $3\" in\n  'auth session issue') printf '%s\\n' '{{\"sessionId\":\"fake-session\",\"token\":\"{TOKEN}\",\"method\":\"bearer-access-token\",\"scopes\":[]}}' ;;\n  'auth session revoke') printf '%s\\n' \"$4\" >> {} ;;\n  *) if [ \"$1\" = '--version' ]; then printf '%s\\n' 't3 1.2.3'; else exit 3; fi ;;\nesac\n",
                shell_quote(&revoke_log.to_string_lossy())
            );
            fs::write(&script_path, script).unwrap();
            fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();

            Self {
                env: T3Env::new(dir.path().to_path_buf(), bin.into_os_string()),
                dir,
                revoke_log,
            }
        }

        fn userdata(&self) -> PathBuf {
            self.dir.path().join(".t3/userdata")
        }

        fn v2_state(&self) -> V2State {
            V2State::create(&self.userdata().join("statev2.sqlite"))
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
        let server = FakeT3Server::start(
            Some(1),
            vec![
                FakeResponse::http(200, &snapshot(T3_THREAD_ID)),
                dispatch_ok(),
                FakeResponse::http(200, &snapshot(T3_THREAD_ID)),
                dispatch_ok(),
            ],
        );
        let fixture = Fixture::new(Some(&server), true, live_pid());
        assert_eq!(
            owner(&fixture.env, claude_thread()).unwrap(),
            Some(Protocol::V1)
        );
        assert_eq!(owner(&fixture.env, codex_thread()).unwrap(), None);
        let first = wake_thread(&fixture.env, claude_thread(), "resume this task");
        let second = wake_thread(&fixture.env, claude_thread(), "resume this task");

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
    fn provider_mapping_keeps_claude_and_codex_sessions_separate() {
        // no T3 server answers here: an unmapped session needs none, and a
        // mapped one cannot be confirmed without it
        let codex_fixture =
            Fixture::with_mapping(None, Some(("codex", "threadId", SESSION_ID)), live_pid());
        assert!(owner(&codex_fixture.env, codex_thread()).is_err());
        assert_eq!(owner(&codex_fixture.env, claude_thread()).unwrap(), None);
        assert_eq!(
            wake_thread(&codex_fixture.env, claude_thread(), "message"),
            WakeOutcome::NotT3Thread
        );

        let claude_fixture = Fixture::new(None, true, live_pid());
        assert!(owner(&claude_fixture.env, claude_thread()).is_err());
        assert_eq!(owner(&claude_fixture.env, codex_thread()).unwrap(), None);
        assert_eq!(
            wake_thread(&claude_fixture.env, codex_thread(), "message"),
            WakeOutcome::NotT3Thread
        );
    }

    #[test]
    fn codex_thread_maps_by_provider_and_thread_id_and_wakes() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(
            Some(1),
            vec![
                FakeResponse::http(200, &snapshot(T3_THREAD_ID)),
                dispatch_ok(),
            ],
        );
        let fixture = Fixture::with_mapping(
            Some(&server),
            Some(("codex", "threadId", SESSION_ID)),
            live_pid(),
        );

        assert!(owner(&fixture.env, codex_thread()).unwrap().is_some());
        assert_eq!(owner(&fixture.env, claude_thread()).unwrap(), None);
        assert_eq!(
            wake_thread(&fixture.env, codex_thread(), "resume this task"),
            WakeOutcome::Woken {
                t3_thread: T3_THREAD_ID.into()
            }
        );
        assert_eq!(server.requests().len(), 2);
        assert_eq!(fixture.revocations(), ["fake-session"]);
    }

    #[test]
    fn wake_returns_not_t3_thread_without_a_mapping() {
        let fixture = Fixture::new(None, false, live_pid());
        assert_eq!(
            wake_thread(&fixture.env, claude_thread(), "message"),
            WakeOutcome::NotT3Thread
        );
        assert!(fixture.revocations().is_empty());
    }

    #[test]
    fn wake_returns_unavailable_for_a_dead_server_pid() {
        let fixture = Fixture::new(None, true, i32::MAX);
        assert!(matches!(
            wake_thread(&fixture.env, claude_thread(), "message"),
            WakeOutcome::Unavailable(_)
        ));
        assert!(fixture.revocations().is_empty());
    }

    #[test]
    fn bugfix_deleted_archived_thread_is_not_unarchived() {
        if !curl_available() {
            return;
        }
        let mut deleted = snapshot(T3_THREAD_ID);
        deleted["thread"]["archivedAt"] = json!("2026-09-26T17:00:00Z");
        deleted["thread"]["deletedAt"] = json!("2026-09-26T17:00:00Z");
        let server = FakeT3Server::start(
            Some(1),
            vec![
                FakeResponse::http(200, &deleted),
                FakeResponse::http(200, &json!({ "sequence": 11 })),
            ],
        );
        let fixture = Fixture::new(Some(&server), true, live_pid());
        assert!(matches!(
            wake_thread(&fixture.env, claude_thread(), "message"),
            WakeOutcome::Refused(_)
        ));
        assert_eq!(server.requests().len(), 1);
    }

    #[test]
    fn wake_unarchives_an_archived_thread_before_its_turn() {
        if !curl_available() {
            return;
        }
        // V1 records a turn in an archived thread but never starts it
        let mut archived = snapshot(T3_THREAD_ID);
        archived["thread"]["archivedAt"] = json!("2026-09-26T17:00:00Z");
        let server = FakeT3Server::start(
            Some(1),
            vec![
                FakeResponse::http(200, &archived),
                FakeResponse::http(200, &json!({ "sequence": 11 })),
                dispatch_ok(),
            ],
        );
        let fixture = Fixture::new(Some(&server), true, live_pid());

        assert_eq!(
            wake_thread(&fixture.env, claude_thread(), "message"),
            WakeOutcome::Woken {
                t3_thread: T3_THREAD_ID.into()
            }
        );
        let types = server
            .requests()
            .iter()
            .skip(1)
            .map(|request| serde_json::from_str::<Value>(&request.body).unwrap()["type"].clone())
            .collect::<Vec<_>>();
        assert_eq!(types, ["thread.unarchive", "thread.turn.start"]);
        assert_eq!(fixture.revocations(), ["fake-session"]);
    }

    #[test]
    fn wake_marks_dispatch_400_as_changed_and_revokes_the_token() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(
            Some(1),
            vec![
                FakeResponse::http(200, &snapshot(T3_THREAD_ID)),
                FakeResponse::http(400, &json!({})),
            ],
        );
        let fixture = Fixture::new(Some(&server), true, live_pid());

        assert_eq!(
            wake_thread(&fixture.env, claude_thread(), "message"),
            WakeOutcome::ApiChanged("turn dispatch returned HTTP 400".into())
        );
        assert_eq!(fixture.revocations(), ["fake-session"]);
    }

    #[test]
    fn probe_reports_compatible_api_and_version() {
        if !curl_available() {
            return;
        }
        // T3 `0.0.42` and earlier have no protocol field, which means V1
        let server = FakeT3Server::start(
            None,
            vec![
                FakeResponse::http(200, &snapshot(T3_THREAD_ID)),
                FakeResponse::http(
                    500,
                    &json!({
                        "_tag": "EnvironmentInternalError",
                        "code": "internal_error",
                        "reason": "orchestration_dispatch_failed",
                        "traceId": "discard-this"
                    }),
                ),
            ],
        );
        let fixture = Fixture::new(Some(&server), true, live_pid());

        let report = probe(&fixture.env);
        assert_eq!(report.status, ProbeStatus::Compatible);
        assert_eq!(report.t3_version.as_deref(), Some("t3 1.2.3"));
        assert_eq!(report.orchestration_protocol, Some(1));
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
        let server = FakeT3Server::start(
            Some(1),
            vec![
                FakeResponse::http(200, &invalid),
                FakeResponse::http(200, &invalid),
            ],
        );
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
    fn v1_compaction_sends_compact_with_ids_keyed_by_idle_period() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(
            Some(1),
            vec![
                FakeResponse::http(200, &snapshot(T3_THREAD_ID)),
                dispatch_ok(),
                FakeResponse::http(200, &snapshot(T3_THREAD_ID)),
                dispatch_ok(),
                FakeResponse::http(200, &snapshot(T3_THREAD_ID)),
                dispatch_ok(),
            ],
        );
        let fixture = Fixture::new(Some(&server), true, live_pid());

        let woken = WakeOutcome::Woken {
            t3_thread: T3_THREAD_ID.into(),
        };
        for key in ["idle since 1", "idle since 1", "idle since 2"] {
            assert_eq!(compact_thread(&fixture.env, thread_id(), key), woken);
        }
        let dispatches: Vec<Value> = server
            .requests()
            .iter()
            .filter(|request| request.path == "/api/orchestration/dispatch")
            .map(|request| serde_json::from_str(&request.body).unwrap())
            .collect();
        assert_eq!(dispatches.len(), 3);
        assert!(
            dispatches
                .iter()
                .all(|body| body["message"]["text"] == "/compact")
        );
        // T3 replays a repeated command id, so a retry in one idle period is a no-op
        assert_eq!(dispatches[0]["commandId"], dispatches[1]["commandId"]);
        assert_ne!(dispatches[1]["commandId"], dispatches[2]["commandId"]);
        assert_ne!(
            dispatches[1]["message"]["messageId"],
            dispatches[2]["message"]["messageId"]
        );
    }

    #[test]
    fn v2_wake_dispatches_a_message_over_the_websocket_with_stable_ids() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(
            Some(2),
            vec![ticket(), dispatched(), ticket(), dispatched()],
        );
        let fixture = Fixture::empty(Some(&server), live_pid());
        // V2 stops writing state.sqlite, so its mappings must not count
        fixture.v1_state(Some(("codex", "threadId", SESSION_ID)));
        fixture
            .v2_state()
            .thread(T3_THREAD_ID, "V2 thread", false)
            .native("claudeAgent", SESSION_ID, T3_THREAD_ID);

        assert_eq!(
            owner(&fixture.env, claude_thread()).unwrap(),
            Some(Protocol::V2)
        );
        assert_eq!(owner(&fixture.env, codex_thread()).unwrap(), None);
        let first = wake_thread(&fixture.env, claude_thread(), "resume this task");
        let second = wake_thread(&fixture.env, claude_thread(), "resume this task");

        assert_eq!(
            first,
            WakeOutcome::Woken {
                t3_thread: T3_THREAD_ID.into()
            }
        );
        assert_eq!(second, first);
        let requests = server.requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].path, "/api/auth/websocket-ticket");
        assert!(requests[0].headers.contains(&format!("Bearer {TOKEN}")));
        assert_eq!(requests[1].method, "WS");
        assert_eq!(
            requests[1].path,
            "/ws?wsTicket=fake-ticket&orchestrationProtocol=2"
        );
        let first_rpc: Value = serde_json::from_str(&requests[1].body).unwrap();
        let second_rpc: Value = serde_json::from_str(&requests[3].body).unwrap();
        assert_eq!(first_rpc["_tag"], "Request");
        assert_eq!(first_rpc["tag"], "orchestration.dispatchCommand");
        let payload = &first_rpc["payload"];
        assert_eq!(payload["type"], "message.dispatch");
        assert_eq!(payload["threadId"], T3_THREAD_ID);
        assert_eq!(payload["text"], "resume this task");
        assert_eq!(payload["deliveryIntent"], "auto");
        assert_eq!(payload["commandId"], second_rpc["payload"]["commandId"]);
        assert_eq!(payload["messageId"], second_rpc["payload"]["messageId"]);
        assert_eq!(fixture.revocations(), ["fake-session", "fake-session"]);
    }

    #[test]
    fn v2_wakes_an_imported_v1_session_and_unarchives_its_thread_first() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(
            Some(2),
            vec![ticket(), dispatched(), ticket(), dispatched()],
        );
        let fixture = Fixture::empty(Some(&server), live_pid());
        fixture
            .v2_state()
            .thread(T3_THREAD_ID, "Imported thread", true)
            .legacy("claudeAgent", "resume", SESSION_ID, T3_THREAD_ID);

        assert_eq!(
            owner(&fixture.env, claude_thread()).unwrap(),
            Some(Protocol::V2)
        );
        assert_eq!(
            wake_thread(&fixture.env, claude_thread(), "message"),
            WakeOutcome::Woken {
                t3_thread: T3_THREAD_ID.into()
            }
        );
        let types = server
            .requests()
            .iter()
            .filter(|request| request.method == "WS")
            .map(|request| {
                serde_json::from_str::<Value>(&request.body).unwrap()["payload"]["type"].clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(types, ["thread.unarchive", "message.dispatch"]);
    }

    #[test]
    fn v2_reply_after_the_request_left_is_uncertain_unless_t3_refused() {
        if !curl_available() {
            return;
        }
        let malformed = FakeResponse::WebSocket(vec![rpc_exit(json!({ "_tag": "Weird" }))]);
        let lost = FakeResponse::WebSocket(Vec::new());
        let server = FakeT3Server::start(
            Some(2),
            vec![
                ticket(),
                malformed,
                ticket(),
                lost,
                ticket(),
                unknown_thread_error(),
            ],
        );
        let fixture = Fixture::empty(Some(&server), live_pid());
        fixture
            .v2_state()
            .thread(T3_THREAD_ID, "V2 thread", false)
            .native("claudeAgent", SESSION_ID, T3_THREAD_ID);

        let wake = || wake_thread(&fixture.env, claude_thread(), "message");
        assert!(matches!(wake(), WakeOutcome::Uncertain(_)));
        assert!(matches!(wake(), WakeOutcome::Uncertain(_)));
        assert!(matches!(wake(), WakeOutcome::Refused(_)));
    }

    #[test]
    fn an_unreadable_inactive_database_does_not_hide_the_active_one() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(Some(1), Vec::new());
        let fixture = Fixture::new(Some(&server), true, live_pid());
        fs::write(fixture.userdata().join("statev2.sqlite"), b"not a database").unwrap();

        assert_eq!(
            owner(&fixture.env, claude_thread()).unwrap(),
            Some(Protocol::V1)
        );
        assert_eq!(owner(&fixture.env, codex_thread()).unwrap(), None);
    }

    #[test]
    fn v2_probe_reports_the_protocol_and_accepts_the_typed_unknown_thread_error() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(Some(2), vec![ticket(), unknown_thread_error()]);
        let fixture = Fixture::empty(Some(&server), live_pid());
        fixture.v2_state();

        let report = probe(&fixture.env);

        assert_eq!(report.status, ProbeStatus::Compatible, "{report:?}");
        assert_eq!(report.orchestration_protocol, Some(2));
        assert!(report.checks.iter().all(|check| check.ok));
        let rpc: Value = serde_json::from_str(&server.requests()[1].body).unwrap();
        assert_eq!(rpc["payload"]["type"], "message.dispatch");
        assert_ne!(rpc["payload"]["threadId"], T3_THREAD_ID);
        assert_eq!(fixture.revocations(), ["fake-session"]);
    }

    #[test]
    fn v2_probe_flags_a_rejected_websocket_protocol() {
        if !curl_available() {
            return;
        }
        let rejected = FakeResponse::http(
            426,
            &json!({ "code": "orchestration_protocol_incompatible", "orchestrationProtocolVersion": 3 }),
        );
        let server = FakeT3Server::start(Some(2), vec![ticket(), rejected]);
        let fixture = Fixture::empty(Some(&server), live_pid());
        fixture.v2_state();

        let report = probe(&fixture.env);

        assert_eq!(report.status, ProbeStatus::Changed);
        assert_eq!(
            report.fingerprint().as_deref(),
            Some(
                "dispatch: WebSocket upgrade returned HTTP 426; T3 no longer accepts orchestration protocol 2"
            )
        );
    }

    #[test]
    fn unsupported_protocol_is_an_api_change() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(Some(3), Vec::new());
        let fixture = Fixture::new(Some(&server), true, live_pid());

        let report = probe(&fixture.env);

        assert_eq!(report.status, ProbeStatus::Changed);
        assert_eq!(
            report.fingerprint().as_deref(),
            Some("protocol: server speaks unsupported orchestration protocol 3")
        );
        assert!(matches!(
            wake_thread(&fixture.env, claude_thread(), "message"),
            WakeOutcome::ApiChanged(_)
        ));
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
    fn desktop_runtime_file_with_host_still_loads() {
        let dir = TempDir::new().unwrap();
        let pid = i32::try_from(std::process::id()).unwrap();
        let runtime = json!({
            "version": 1,
            "pid": pid,
            "host": "127.0.0.1",
            "port": 3773,
            "origin": "http://127.0.0.1:3773",
            "startedAt": "2026-09-26T21:09:01.839Z"
        });
        fs::write(
            dir.path().join("server-runtime.json"),
            serde_json::to_vec(&runtime).unwrap(),
        )
        .unwrap();

        assert_eq!(
            load_runtime(dir.path()).unwrap().origin,
            "http://127.0.0.1:3773"
        );
    }

    #[test]
    fn missing_cli_is_unavailable_not_an_api_change() {
        let pid = i32::try_from(std::process::id()).unwrap();
        let mut fixture = Fixture::new(None, true, pid);
        fixture.env = T3Env::new(fixture.env.home.clone(), OsString::new());

        let report = probe(&fixture.env);

        assert_eq!(report.status, ProbeStatus::Unavailable);
        assert_eq!(report.fingerprint(), None);
    }

    #[test]
    fn hung_t3_cli_is_killed_at_the_deadline() {
        let dir = TempDir::new().unwrap();
        let script = dir.path().join("t3");
        fs::write(&script, "#!/bin/sh\nsleep 30\n").unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let cli = T3Cli {
            executable: script,
            env: T3Env::new(dir.path().to_path_buf(), std::env::var_os("PATH").unwrap()),
            timeout: Duration::from_millis(200),
        };

        let started = Instant::now();
        let error = cli.run("t3 auth session issue", &[]).err().unwrap();

        assert!(error.contains("timed out"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn deterministic_ids_are_salted_and_stable() {
        let first = deterministic_id("command", T3_THREAD_ID, "text");
        assert_eq!(first, deterministic_id("command", T3_THREAD_ID, "text"));
        assert_ne!(first, deterministic_id("message", T3_THREAD_ID, "text"));
    }
    #[test]
    fn t3_notice_rechecks_eligibility_after_preparation() {
        use crate::callback::send_check::{SendCheck, SendFailure};
        if !curl_available() {
            return;
        }
        let server = std::sync::Arc::new(FakeT3Server::start(
            Some(1),
            vec![
                FakeResponse::http(200, &snapshot(T3_THREAD_ID)),
                dispatch_ok(),
            ],
        ));
        let fixture = Fixture::new(Some(&server), true, live_pid());
        let prepared = server.clone();
        let check: SendCheck = std::sync::Arc::new(move || {
            let requests = prepared.requests();
            assert_eq!(
                requests.len(),
                1,
                "eligibility must follow the T3 snapshot lookup"
            );
            assert_eq!(requests[0].method, "GET");
            Err(SendFailure::Suppressed)
        });
        let outcome = super::wake_thread_checked(
            &fixture.env,
            claude_thread(),
            "JOB_BLOCKED",
            Some(&check),
            TurnStart::Auto,
        );
        assert!(matches!(outcome, Err(SendFailure::Suppressed)));
        assert_eq!(
            server.requests().len(),
            1,
            "the ended notice must not be dispatched"
        );
        assert_eq!(fixture.revocations(), ["fake-session"]);
    }
}
