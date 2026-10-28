//! T3 Code client for waking provider sessions, launching threads, and
//! checking its local API
//!
//! # T3 Code internals this depends on
//!
//! T3 Code has no public API for these operations, so this client pins local
//! contracts that an update can change without notice. Run `homebased t3 check`
//! after a T3 update
//!
//! T3 Code `0.1.0` is the first stable release with orchestration protocol 2,
//! after pingdotgg/t3code#2829, and the only one supported; see [`v2`]. Every
//! wake and probe asks the running server which protocol it speaks and reports
//! an older or newer T3 as an incompatible API
//!
//! The local contracts are:
//!
//! - User data lives in `~/.t3/userdata`; `server-runtime.json` has `version`,
//!   `pid`, and `origin`. Version 1 and a live PID identify a running server.
//!   The process check uses `kill(pid, None)`. The desktop app also writes
//!   `host`, `port`, and `startedAt`; a service-managed server omits `host`
//!   and adds `serviceManaged`, so the client ignores those fields
//! - `GET /.well-known/t3/environment` needs no token and returns
//!   `orchestrationProtocolVersion`; T3 `0.0.42` and earlier omit it, which T3
//!   defines as protocol 1, like the value `1` itself
//! - The `t3` CLI is the running server's executable
//!   (`~/.t3/runtime/versions/<version>/t3`), found through `/proc/<pid>/exe`
//!   or `ps`, else the desktop app's launcher `~/.t3/bin/t3`, else `t3` on
//!   PATH. The desktop app puts neither on PATH
//! - `t3 auth session issue --json --ttl 5m --label homebased` returns
//!   `sessionId` and `token` for a bearer session. Revoke it with
//!   `t3 auth session revoke <sessionId>` after every use; `t3 --version`
//!   supplies the optional probe version
//!
//! Launching a new thread adds the contracts listed in [`launch`]
//!
//! Calls use the blocking system `curl` helper, a blocking WebSocket, and `t3`
//! commands with a deadline. The callback dispatcher must call this module from
//! `spawn_blocking`

mod hold;
mod launch;
mod rpc;
mod v2;

pub use launch::{
    AddedProject, IconChange, IconColor, InteractionMode, LaunchError, LaunchSpec, LaunchedThread,
    MESSAGE_MAX_BYTES as LAUNCH_MESSAGE_MAX_BYTES, ModelChoice, ModelOption, OptionValue,
    ProjectIcon, ProjectSpec, RuntimeMode, T3Project, Workspace, add_project, launch_thread,
    projects,
};

use std::ffi::OsString;
use std::fs;
use std::io::{self, Read};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Builder;

use crate::callback::send_check::{SendCheck, SendFailure};
use crate::curl::{CurlMethod, CurlRequest};
use crate::domain::ThreadId;

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_millis(250);
/// Orchestration protocol this client speaks
const ORCHESTRATION_PROTOCOL: u32 = 2;
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

/// Provider session that T3 can resume in its owning thread
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderThread {
    /// Claude session id
    Claude(ThreadId),
    /// Codex thread id
    Codex(ThreadId),
}

impl ProviderThread {
    /// T3 provider name, the resume cursor path of a session imported from the
    /// previous orchestrator, and the native id
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

/// Whether a T3 thread owns this provider session
///
/// Reads the state database on disk first, so a session it does not map needs
/// no request to T3. A mapped session needs a server that speaks the supported
/// protocol; an error means T3 could not say
pub fn owner(env: &T3Env, provider_thread: ProviderThread) -> Result<bool, String> {
    let userdata = env.userdata();
    if find_thread(&userdata, provider_thread)?.is_none() {
        return Ok(false);
    }
    connect(&userdata).map_err(|failure| failure.detail())?;
    Ok(true)
}

/// Message text that T3 runs as a compaction turn instead of a prompt
const COMPACT_COMMAND: &str = "/compact";

/// How a woken T3 thread runs the message relative to its active run
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
/// `before_send` rechecks an unsent notice at the final dispatch boundary
pub(crate) fn wake_thread_checked(
    env: &T3Env,
    provider_thread: ProviderThread,
    text: &str,
    before_send: Option<&SendCheck>,
    start: TurnStart,
) -> Result<WakeOutcome, SendFailure> {
    with_owner(env, provider_thread, |owner| {
        v2::start_turn(
            owner.origin,
            owner.state,
            owner.thread,
            text,
            owner.token,
            before_send,
            start,
        )
    })
}

/// Start a `/compact` turn in the T3 thread that owns a Claude session
///
/// `key` names the idle period being compacted, so T3 drops a repeated
/// request for it. T3 refuses it while a turn runs
pub(crate) fn compact_thread(env: &T3Env, thread: ThreadId, key: &str) -> WakeOutcome {
    with_owner(env, ProviderThread::Claude(thread), |owner| {
        v2::compact(owner.origin, owner.thread, key, owner.token, None)
    })
    .unwrap_or_else(|failure| WakeOutcome::Unavailable(failure.to_string()))
}

/// End the placeholder runs that `--hold` left in T3, and return how many ended
///
/// A T3 that is not running needs nothing: its restart cancels them. See [`hold`]
pub(crate) fn end_leftover_holds(env: &T3Env) -> Result<usize, String> {
    let userdata = env.userdata();
    let state = state_path(&userdata);
    if !state.is_file() {
        return Ok(0);
    }
    let holds = hold::leftover(&state)?;
    if holds.is_empty() {
        return Ok(0);
    }

    let server = connect(&userdata).map_err(|failure| failure.detail())?;
    let token = T3Cli::resolve(env, server.pid)?
        .issue_token()
        .map_err(|(IssueError::Unavailable(detail) | IssueError::Changed(detail))| detail)?;
    let mut failures = Vec::new();
    for leftover in &holds {
        if let Err(failure) = hold::end(&server.origin, &token.value, leftover) {
            failures.push(failure.detail());
        }
    }
    if let Some(first) = failures.first() {
        return Err(format!(
            "{} of {} placeholders did not end: {first}",
            failures.len(),
            holds.len()
        ));
    }
    Ok(holds.len())
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
    /// Whether the thread is settled in T3's sidebar
    pub(crate) settled: bool,
    /// Project, branch, model, and other context T3 records for the thread
    pub(crate) details: ThreadDetails,
}

/// What T3 records about a thread beyond its title, for display
///
/// Every field is optional: older threads lack some of them, and they come
/// from T3 payloads that no protocol check covers
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ThreadDetails {
    /// T3 project title
    pub project: Option<String>,
    /// Directory the thread works in: its worktree, else the project root
    pub workspace: Option<String>,
    /// Git branch of the thread
    pub branch: Option<String>,
    /// Model the thread runs
    pub model: Option<String>,
    /// Reasoning effort of the model
    pub effort: Option<String>,
    /// Title of the thread that started this one as a subagent
    pub parent_title: Option<String>,
    /// Latest pull request linked to the thread
    pub pull_request: Option<PullRequest>,
    /// When T3 created the thread
    pub created_at: Option<String>,
}

/// Pull request linked to a T3 thread
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PullRequest {
    /// Pull request number
    pub number: i64,
    /// Web address of the pull request
    pub url: String,
    /// `open`, `closed`, or `merged` when T3 last synced it
    pub state: Option<String>,
}

/// Session id, T3 thread id, title, and settled flag of one row in a state database
type ClaudeThreadRow = (String, String, String, bool);

/// Claude sessions whose T3 thread is neither archived nor deleted, newest first
///
/// Reads the state database on disk, so the server need not run
pub(crate) fn open_claude_threads(env: &T3Env) -> Result<Vec<OpenClaudeThread>, String> {
    let state = state_path(&env.userdata());
    if !state.is_file() || !v2::schema_matches(&state) {
        return Ok(Vec::new());
    }
    let rows = v2::open_claude_threads(&state)?;
    let details = v2::thread_details(&state);

    let mut threads: Vec<OpenClaudeThread> = Vec::new();
    for (session, t3_thread, title, settled) in rows {
        // a session id that is not a UUID cannot be a Claude Code session
        let Ok(session) = session.parse::<ThreadId>() else {
            continue;
        };
        // rows come newest first, and a T3 thread resumes only its newest
        // session, so an older session of a listed thread is left out
        let listed = threads
            .iter()
            .any(|thread| thread.session == session || thread.t3_thread == t3_thread);
        if !listed {
            let details = details.get(&t3_thread).cloned().unwrap_or_default();
            threads.push(OpenClaudeThread {
                session,
                t3_thread,
                title,
                settled,
                details,
            });
        }
    }
    Ok(threads)
}

/// Every row of a query that selects a session id, T3 thread id, title, and
/// settled flag
fn claude_thread_rows(path: &Path, sql: &str) -> Result<Vec<ClaudeThreadRow>, String> {
    let db = state_connection(path).map_err(|_| "T3 state cannot be read".to_string())?;
    let mut statement = db
        .prepare(sql)
        .map_err(|_| "T3 thread list query failed".to_string())?;
    statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .and_then(Iterator::collect)
        .map_err(|_| "T3 thread list query failed".to_string())
}

/// Running T3 server, thread, and bearer token for one provider session
struct Owner<'a> {
    origin: &'a str,
    state: &'a Path,
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
    let t3_thread = match find_thread(&userdata, provider_thread) {
        Ok(Some(t3_thread)) => t3_thread,
        Ok(None) => return Ok(WakeOutcome::NotT3Thread),
        Err(detail) => return Ok(WakeOutcome::Unavailable(detail)),
    };
    let server = match connect(&userdata) {
        Ok(server) => server,
        Err(failure) => return Ok(failure.into_wake_outcome()),
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
        state: &state_path(&userdata),
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

    if let Err(failure) = require_protocol(&runtime.origin) {
        report.checks.push(failed("protocol", failure.detail()));
        return report.finish(failure.probe_status());
    }
    report.orchestration_protocol = Some(ORCHESTRATION_PROTOCOL);
    report.checks.push(passed(
        "protocol",
        &format!("server speaks orchestration protocol {ORCHESTRATION_PROTOCOL}"),
    ));

    if !v2::schema_matches(&state_path(&userdata)) {
        report.checks.push(failed(
            "state_schema",
            "statev2.sqlite does not have the required thread columns",
        ));
        return report.finish(ProbeStatus::Changed);
    }
    report.checks.push(passed(
        "state_schema",
        "required statev2.sqlite columns exist",
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

    let status = v2::probe_api(&runtime.origin, &token.value, &mut report.checks);
    report.finish(status)
}

/// State database of the running server
///
/// Protocol 2 copies the previous orchestrator's `state.sqlite` into it once
/// and never writes `state.sqlite` again, so only this database is current
fn state_path(userdata: &Path) -> PathBuf {
    userdata.join("statev2.sqlite")
}

/// T3 thread that the state database maps a provider session to, if any
///
/// A missing database maps nothing
fn find_thread(userdata: &Path, provider_thread: ProviderThread) -> Result<Option<String>, String> {
    let state = state_path(userdata);
    if !state.is_file() {
        return Ok(None);
    }
    v2::find_thread(&state, provider_thread)
}

/// Running T3 server whose protocol this client supports
#[derive(Debug, Clone)]
struct Server {
    pid: i32,
    origin: String,
}

fn connect(userdata: &Path) -> Result<Server, ApiFailure> {
    let runtime = load_runtime(userdata).map_err(ApiFailure::Unavailable)?;
    require_protocol(&runtime.origin)?;
    Ok(Server {
        pid: runtime.pid,
        origin: runtime.origin,
    })
}

/// Fail unless the server at `origin` speaks [`ORCHESTRATION_PROTOCOL`]
fn require_protocol(origin: &str) -> Result<(), ApiFailure> {
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
        None | Some(1) => Err(ApiFailure::Changed(
            "T3 Code older than 0.1.0 is not supported; update T3 Code".into(),
        )),
        Some(version) if version == u64::from(ORCHESTRATION_PROTOCOL) => Ok(()),
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
/// The background service starts it from `~/.t3/runtime/versions/<version>/t3`
/// without putting `t3` on PATH. The desktop app instead runs its bundled
/// server under Electron, so its server process is not a `t3` binary; see
/// [`DESKTOP_CLI_SHIM`]
fn server_executable(pid: i32) -> Option<PathBuf> {
    let executable = fs::read_link(format!("/proc/{pid}/exe"))
        .ok()
        .or_else(|| process_path_from_ps(pid))?;
    let is_t3 = executable.file_name().is_some_and(|name| name == "t3");
    (is_t3 && executable.is_file()).then_some(executable)
}

/// Launcher for the desktop app's bundled CLI, relative to the user's home
///
/// T3 Code `0.1.0` and later desktop apps write it and keep it off PATH so it
/// never shadows another `t3`; `apps/desktop/src/app/DesktopCliShim.ts`
const DESKTOP_CLI_SHIM: &str = ".t3/bin/t3";

/// The desktop app's CLI launcher, when one is installed
fn desktop_cli_shim(env: &T3Env) -> Option<PathBuf> {
    let shim = env.home.join(DESKTOP_CLI_SHIM);
    shim.is_file().then_some(shim)
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
        let executable = match server_executable(server_pid).or_else(|| desktop_cli_shim(env)) {
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
        let (sender, receiver) = mpsc::channel::<Vec<u8>>();
        thread::spawn(move || {
            let mut stdout = stdout.take(CLI_OUTPUT_LIMIT);
            let mut chunk = [0_u8; 8192];
            // send each read as it arrives, so output written before exit
            // survives a pipe that never reaches EOF
            loop {
                match stdout.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(read) => {
                        if sender.send(chunk[..read].to_vec()).is_err() {
                            break;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => break,
                }
            }
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
        // a descendant, or any process that inherited the write end while the
        // daemon spawned it, can keep the pipe open after the direct child
        // exits; keep what arrived instead of discarding it
        let drain_deadline = Instant::now() + CLI_DRAIN_TIMEOUT;
        let mut stdout = Vec::new();
        while let Some(left) = drain_deadline.checked_duration_since(Instant::now()) {
            match receiver.recv_timeout(left) {
                Ok(chunk) => stdout.extend_from_slice(&chunk),
                Err(_) => break,
            }
        }

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

        if output.stdout.iter().all(u8::is_ascii_whitespace) {
            return Err(IssueError::Changed("t3 token output is empty".into()));
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
    use std::time::{Duration, Instant};

    use serde_json::{Value, json};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::test_support::{
        FakeResponse, FakeT3Server, Fixture, SESSION_ID, T3_THREAD_ID, TOKEN, dispatched, live_pid,
        rpc_exit, ticket,
    };
    use super::{
        ProbeStatus, ProviderThread, PullRequest, T3Cli, T3Env, ThreadDetails, TurnStart,
        WakeOutcome, compact_thread, deterministic_id, load_runtime, open_claude_threads, owner,
        probe, wake_thread_checked,
    };
    use crate::domain::ThreadId;

    /// Start a T3 turn with no notice to recheck
    fn wake_thread(env: &T3Env, provider_thread: ProviderThread, text: &str) -> WakeOutcome {
        wake_thread_checked(env, provider_thread, text, None, TurnStart::Auto)
            .unwrap_or_else(|failure| WakeOutcome::Unavailable(failure.to_string()))
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

    #[test]
    fn provider_mapping_keeps_claude_and_codex_sessions_separate() {
        // no T3 server answers here: an unmapped session needs none, and a
        // mapped one cannot be confirmed without it
        let codex_fixture = Fixture::empty(None, live_pid());
        codex_fixture
            .v2_state()
            .thread(T3_THREAD_ID, "Fake thread", false)
            .native("codex", SESSION_ID, T3_THREAD_ID);
        assert!(owner(&codex_fixture.env, codex_thread()).is_err());
        assert_eq!(owner(&codex_fixture.env, claude_thread()), Ok(false));
        assert_eq!(
            wake_thread(&codex_fixture.env, claude_thread(), "message"),
            WakeOutcome::NotT3Thread
        );

        let claude_fixture = Fixture::mapped(None, live_pid());
        assert!(owner(&claude_fixture.env, claude_thread()).is_err());
        assert_eq!(owner(&claude_fixture.env, codex_thread()), Ok(false));
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
        let server = FakeT3Server::start(Some(2), vec![ticket(), dispatched()]);
        let fixture = Fixture::empty(Some(&server), live_pid());
        fixture
            .v2_state()
            .thread(T3_THREAD_ID, "Fake thread", false)
            .native("codex", SESSION_ID, T3_THREAD_ID);

        assert_eq!(owner(&fixture.env, codex_thread()), Ok(true));
        assert_eq!(owner(&fixture.env, claude_thread()), Ok(false));
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
        let fixture = Fixture::empty(None, live_pid());
        assert_eq!(
            wake_thread(&fixture.env, claude_thread(), "message"),
            WakeOutcome::NotT3Thread
        );

        fixture.v2_state();
        assert_eq!(
            wake_thread(&fixture.env, claude_thread(), "message"),
            WakeOutcome::NotT3Thread
        );
        assert!(fixture.revocations().is_empty());
    }

    #[test]
    fn wake_returns_unavailable_for_a_dead_server_pid() {
        let fixture = Fixture::mapped(None, i32::MAX);
        assert!(matches!(
            wake_thread(&fixture.env, claude_thread(), "message"),
            WakeOutcome::Unavailable(_)
        ));
        assert!(fixture.revocations().is_empty());
    }

    #[test]
    fn compaction_sends_compact_with_ids_keyed_by_idle_period() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(
            Some(2),
            vec![
                ticket(),
                dispatched(),
                ticket(),
                dispatched(),
                ticket(),
                dispatched(),
            ],
        );
        let fixture = Fixture::mapped(Some(&server), live_pid());

        let woken = WakeOutcome::Woken {
            t3_thread: T3_THREAD_ID.into(),
        };
        for key in ["idle since 1", "idle since 1", "idle since 2"] {
            assert_eq!(compact_thread(&fixture.env, thread_id(), key), woken);
        }
        let dispatches: Vec<Value> = server
            .requests()
            .iter()
            .filter(|request| request.method == "WS")
            .map(|request| serde_json::from_str::<Value>(&request.body).unwrap()["payload"].clone())
            .collect();
        assert_eq!(dispatches.len(), 3);
        assert!(
            dispatches
                .iter()
                .all(|payload| payload["type"] == "message.dispatch"
                    && payload["text"] == "/compact")
        );
        // T3 replays a repeated command id, so a retry in one idle period is a no-op
        assert_eq!(dispatches[0]["commandId"], dispatches[1]["commandId"]);
        assert_ne!(dispatches[1]["commandId"], dispatches[2]["commandId"]);
        assert_ne!(dispatches[1]["messageId"], dispatches[2]["messageId"]);
    }

    #[test]
    fn wake_dispatches_a_message_over_the_websocket_with_stable_ids() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(
            Some(2),
            vec![ticket(), dispatched(), ticket(), dispatched()],
        );
        let fixture = Fixture::mapped(Some(&server), live_pid());

        assert_eq!(owner(&fixture.env, claude_thread()), Ok(true));
        assert_eq!(owner(&fixture.env, codex_thread()), Ok(false));
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
    fn reads_the_project_branch_model_and_parent_of_open_threads() {
        let fixture = Fixture::empty(None, live_pid());
        let parent_session = "0b9f1a5e-3a8e-4a57-9d55-0d3c1b8f6e21";
        fixture
            .v2_state()
            .project("homebased", "/code/homebased")
            .thread("parent", "Plan the release", false)
            .native("claudeAgent", parent_session, "parent")
            .thread(T3_THREAD_ID, "Port the renderer", false)
            .payload(
                T3_THREAD_ID,
                &json!({
                    "branch": "t3/port-renderer",
                    "worktreePath": "/worktrees/t3-7d0d1506",
                    "modelSelection": {
                        "model": "claude-opus-5-5",
                        "options": [{ "id": "thinking", "value": true }, { "id": "effort", "value": "high" }]
                    },
                    "lineage": { "parentThreadId": "parent" },
                    "pullRequests": [
                        { "number": 7, "url": "https://example.test/pull/7", "snapshot": { "state": "closed" } },
                        { "number": 9, "url": "https://example.test/pull/9", "snapshot": { "state": "open" } }
                    ]
                }),
            )
            .native("claudeAgent", SESSION_ID, T3_THREAD_ID);

        let open = open_claude_threads(&fixture.env).unwrap();
        let details = |t3_thread: &str| {
            let thread = open.iter().find(|thread| thread.t3_thread == t3_thread);
            thread.unwrap().details.clone()
        };

        assert_eq!(
            details(T3_THREAD_ID),
            ThreadDetails {
                project: Some("homebased".to_owned()),
                workspace: Some("/worktrees/t3-7d0d1506".to_owned()),
                branch: Some("t3/port-renderer".to_owned()),
                model: Some("claude-opus-5-5".to_owned()),
                effort: Some("high".to_owned()),
                parent_title: Some("Plan the release".to_owned()),
                pull_request: Some(PullRequest {
                    number: 9,
                    url: "https://example.test/pull/9".to_owned(),
                    state: Some("open".to_owned()),
                }),
                created_at: Some("2026-10-02T19:00:00Z".to_owned()),
            }
        );
        // a thread without a worktree works in its project root
        assert_eq!(
            details("parent"),
            ThreadDetails {
                project: Some("homebased".to_owned()),
                workspace: Some("/code/homebased".to_owned()),
                created_at: Some("2026-10-02T19:00:00Z".to_owned()),
                ..ThreadDetails::default()
            }
        );
    }

    #[test]
    fn lists_only_the_session_a_t3_thread_resumes() {
        let fixture = Fixture::empty(None, live_pid());
        let imported = "5f0c9a52-8e44-4c3e-9a63-6b1d2c7e8f10";
        fixture
            .v2_state()
            .thread(T3_THREAD_ID, "Imported then resumed", false)
            .native("claudeAgent", SESSION_ID, T3_THREAD_ID)
            .legacy("claudeAgent", "resume", imported, T3_THREAD_ID);

        let open = open_claude_threads(&fixture.env).unwrap();
        let sessions: Vec<String> = open
            .iter()
            .map(|thread| thread.session.to_string())
            .collect();
        assert_eq!(sessions, [SESSION_ID]);
    }

    #[test]
    fn finds_a_session_of_a_configured_provider_instance_by_its_driver() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(Some(2), Vec::new());
        let fixture = Fixture::empty(Some(&server), live_pid());
        fixture
            .v2_state()
            .thread(T3_THREAD_ID, "V2 thread", false)
            .instance("claude-work", "claudeAgent", SESSION_ID, T3_THREAD_ID);

        assert_eq!(owner(&fixture.env, claude_thread()), Ok(true));
        let open = open_claude_threads(&fixture.env).unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].t3_thread, T3_THREAD_ID);
    }

    #[test]
    fn wakes_an_imported_session_and_unarchives_its_thread_first() {
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

        assert_eq!(owner(&fixture.env, claude_thread()), Ok(true));
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
    fn reply_after_the_request_left_is_uncertain_unless_t3_refused() {
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
        let fixture = Fixture::mapped(Some(&server), live_pid());

        let wake = || wake_thread(&fixture.env, claude_thread(), "message");
        assert!(matches!(wake(), WakeOutcome::Uncertain(_)));
        assert!(matches!(wake(), WakeOutcome::Uncertain(_)));
        assert!(matches!(wake(), WakeOutcome::Refused(_)));
    }

    #[test]
    fn probe_reports_the_protocol_and_accepts_the_typed_unknown_thread_error() {
        if !curl_available() {
            return;
        }
        let server = FakeT3Server::start(Some(2), vec![ticket(), unknown_thread_error()]);
        let fixture = Fixture::empty(Some(&server), live_pid());
        fixture.v2_state();

        let report = probe(&fixture.env);

        assert_eq!(report.status, ProbeStatus::Compatible, "{report:?}");
        assert_eq!(report.t3_version.as_deref(), Some("t3 1.2.3"));
        assert_eq!(report.orchestration_protocol, Some(2));
        assert!(report.checks.iter().all(|check| check.ok));
        let rpc: Value = serde_json::from_str(&server.requests()[1].body).unwrap();
        assert_eq!(rpc["payload"]["type"], "message.dispatch");
        assert_ne!(rpc["payload"]["threadId"], T3_THREAD_ID);
        assert_eq!(fixture.revocations(), ["fake-session"]);
    }

    #[test]
    fn probe_flags_a_rejected_websocket_protocol() {
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
        let fixture = Fixture::mapped(Some(&server), live_pid());

        let report = probe(&fixture.env);

        assert_eq!(report.status, ProbeStatus::Changed);
        assert_eq!(report.orchestration_protocol, None);
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
    fn t3_older_than_protocol_2_is_an_api_change_that_says_to_update() {
        if !curl_available() {
            return;
        }
        let detail = "T3 Code older than 0.1.0 is not supported; update T3 Code";
        // T3 `0.0.42` and earlier omit the protocol field
        for protocol in [None, Some(1)] {
            let server = FakeT3Server::start(protocol, Vec::new());
            let fixture = Fixture::mapped(Some(&server), live_pid());

            let report = probe(&fixture.env);

            assert_eq!(report.status, ProbeStatus::Changed);
            assert_eq!(report.orchestration_protocol, None);
            assert_eq!(report.fingerprint(), Some(format!("protocol: {detail}")));
            assert_eq!(
                wake_thread(&fixture.env, claude_thread(), "message"),
                WakeOutcome::ApiChanged(detail.into())
            );
            assert_eq!(owner(&fixture.env, claude_thread()), Err(detail.into()));
        }
    }

    #[test]
    fn probe_reports_not_installed_and_not_running() {
        let dir = TempDir::new().unwrap();
        let no_userdata = T3Env::new(dir.path().to_path_buf(), OsString::new());
        assert_eq!(probe(&no_userdata).status, ProbeStatus::NotInstalled);

        let fixture = Fixture::empty(None, i32::MAX);
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
        let mut fixture = Fixture::empty(None, pid);
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
    fn bugfix_output_survives_a_pipe_held_open_after_exit() {
        let dir = TempDir::new().unwrap();
        let script = dir.path().join("t3");
        fs::write(
            &script,
            "#!/bin/sh\nprintf '{\"ok\":true}\\n'\nsleep 5 &\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let cli = T3Cli {
            executable: script,
            env: T3Env::new(dir.path().to_path_buf(), std::env::var_os("PATH").unwrap()),
            timeout: Duration::from_secs(5),
        };

        let output = cli.run("t3 auth session issue", &[]).unwrap();

        assert!(output.success);
        assert_eq!(output.stdout, b"{\"ok\":true}\n");
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
        let server =
            std::sync::Arc::new(FakeT3Server::start(Some(2), vec![ticket(), dispatched()]));
        let fixture = Fixture::mapped(Some(&server), live_pid());
        let prepared = server.clone();
        let check: SendCheck = std::sync::Arc::new(move || {
            let requests = prepared.requests();
            assert_eq!(
                requests.len(),
                1,
                "eligibility must follow the websocket ticket and precede the message"
            );
            assert_eq!(requests[0].path, "/api/auth/websocket-ticket");
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
