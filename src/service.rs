//! Services: long-lived processes an agent starts and does not wait on, such
//! as dev servers, binding proxies, emulators, and device test runners
//!
//! A service runs on this machine under a `service-run` worker, which the
//! daemon starts detached from the agent's shell, in the caller's directory
//! and with the caller's whole environment. It belongs to the thread that
//! started it, identified like the thread `task run` reports to. It ends when
//! `service stop` asks, when that thread's T3 thread is archived, settled, or
//! deleted, when its `--max` lifetime elapses, or when it exits on its own.
//! Only the last, and a worker that vanished, send its owner one
//! `SERVICE_EXITED` event, so an agent learns that its server died but is not
//! woken by a stop it asked for or that cleanup made
//!
//! Services are not tasks: they have no output-inactivity check, never make
//! a thread wait, and `homebased wait` does not take their ids

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::cleanup::ProcessIdentity;
use crate::domain::{API_VERSION, ExitReason, TaskEnv, ThreadId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::submission::CallbackContext;

pub mod worker;

/// Event a service's owner gets when the service exits without being asked to
pub const EXITED_EVENT: &str = "SERVICE_EXITED";

/// Output lines a readiness failure and `SERVICE_EXITED` carry
pub const TAIL_LINES: usize = 20;

/// A service, chosen by the daemon
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServiceId(Uuid);

impl ServiceId {
    /// Allocate a new v7 id
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for ServiceId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ServiceId {
    type Err = AppError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(raw).map(Self).map_err(|_| AppError::Usage {
            message: format!("service id must be a UUID: {raw}"),
        })
    }
}

/// When `service start` counts a service as up
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Readiness {
    /// As soon as the process is spawned
    Spawned,
    /// Once a local TCP port accepts a connection
    Port {
        /// Port on 127.0.0.1 or ::1
        port: u16,
    },
    /// Once a line of its output matches a regular expression
    Line {
        /// Regular expression, checked when the service is started
        pattern: String,
    },
}

impl Readiness {
    /// Check that a line pattern compiles, so a typo fails `start` at once
    /// instead of after the readiness timeout
    pub fn validate(&self) -> Result<(), AppError> {
        match self {
            Self::Line { pattern } => {
                regex::Regex::new(pattern)
                    .map(|_| ())
                    .map_err(|error| AppError::Usage {
                        message: format!("--ready-line is not a valid regular expression: {error}"),
                    })
            }
            Self::Spawned | Self::Port { .. } => Ok(()),
        }
    }
}

impl fmt::Display for Readiness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spawned => f.write_str("spawned"),
            Self::Port { port } => write!(f, "port {port}"),
            Self::Line { pattern } => write!(f, "line /{pattern}/"),
        }
    }
}

/// A stored service: what runs and who owns it, fixed when it starts
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    /// Service id
    pub id: ServiceId,
    /// Label from `--name`
    pub name: String,
    /// Thread that owns the service and gets its `SERVICE_EXITED` event;
    /// [`ThreadId::DETACHED`] for a caller outside any agent session
    pub owner: ThreadId,
    /// The owner's T3 thread on this machine when it runs in one, so the
    /// daemon can tell when it is archived, settled, or deleted; `None` looks
    /// it up by the owner's session on each scan
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_t3: Option<String>,
    /// Origin-only environment of the owner, for event delivery
    pub callback: CallbackContext,
    /// Working directory
    pub cwd: PathBuf,
    /// Program and arguments
    pub command: Vec<String>,
    /// When `start` counts it as up
    pub readiness: Readiness,
    /// How long `start` waits for readiness before stopping it
    #[serde(with = "humantime_serde")]
    pub ready_timeout: Duration,
    /// Longest it may run before it is stopped
    #[serde(with = "humantime_serde")]
    pub max: Duration,
    /// When it was started
    pub created_at: DateTime<Utc>,
}

/// A stop asked of a service from outside its worker
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopRequest {
    /// `homebased service stop`
    Stopped,
    /// The owner's T3 thread was archived, settled, or deleted
    OwnerDone,
}

impl StopRequest {
    /// Storage tag
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::OwnerDone => "owner_done",
        }
    }

    /// Parse a storage tag
    pub fn from_storage(value: &str) -> Result<Self, AppError> {
        match value {
            "stopped" => Ok(Self::Stopped),
            "owner_done" => Ok(Self::OwnerDone),
            other => Err(AppError::Internal {
                message: format!("unknown service stop request: {other}"),
            }),
        }
    }
}

/// Why a service ended
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceEnd {
    /// `homebased service stop`
    Stopped,
    /// The owner's T3 thread was archived, settled, or deleted
    OwnerDone,
    /// Its `--max` lifetime elapsed
    MaxElapsed,
    /// It exited, failed to spawn, or stayed unready before `--ready-timeout`
    NotReady,
    /// It exited on its own after it was ready
    Exited,
    /// Its worker vanished without recording how it ended
    Lost,
}

impl ServiceEnd {
    /// Whether the owner gets a `SERVICE_EXITED` event for this end
    ///
    /// A stop someone asked for, cleanup, and the lifetime limit are expected,
    /// and a readiness failure is reported to `start` itself, so only an end
    /// nobody chose wakes the owner
    #[must_use]
    pub fn sends_event(self) -> bool {
        matches!(self, Self::Exited | Self::Lost)
    }

    /// Lowercase name, as on the wire
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::OwnerDone => "owner_done",
            Self::MaxElapsed => "max_elapsed",
            Self::NotReady => "not_ready",
            Self::Exited => "exited",
            Self::Lost => "lost",
        }
    }
}

impl From<StopRequest> for ServiceEnd {
    fn from(request: StopRequest) -> Self {
        match request {
            StopRequest::Stopped => Self::Stopped,
            StopRequest::OwnerDone => Self::OwnerDone,
        }
    }
}

/// How a service ended, recorded once
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ending {
    /// Why it ended
    pub end: ServiceEnd,
    /// How its process ended, when the worker saw it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<ExitReason>,
    /// What went wrong, for a readiness failure or a lost worker
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// When it ended
    pub at: DateTime<Utc>,
}

/// Where a service stands
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceState {
    /// Spawned or spawning, and not ready yet
    Starting,
    /// Ready and running
    Ready,
    /// Asked to stop, and not ended yet
    Stopping,
    /// Ended; see its [`Ending`]
    Ended,
}

impl ServiceState {
    /// Lowercase name, as on the wire
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Stopping => "stopping",
            Self::Ended => "ended",
        }
    }
}

/// One stored service with its live state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRecord {
    /// What runs and who owns it
    pub service: Service,
    /// The `service-run` worker, once spawned
    pub worker_pid: Option<i32>,
    /// The service process, leader of its own process group, once spawned
    pub child: Option<ProcessIdentity>,
    /// When the process was spawned
    pub started_at: Option<DateTime<Utc>>,
    /// When it became ready
    pub ready_at: Option<DateTime<Utc>>,
    /// A stop asked from outside the worker
    pub stop_request: Option<StopRequest>,
    /// How it ended
    pub ending: Option<Ending>,
}

impl ServiceRecord {
    /// Where the service stands
    #[must_use]
    pub fn state(&self) -> ServiceState {
        match (&self.ending, self.stop_request, self.ready_at) {
            (Some(_), _, _) => ServiceState::Ended,
            (None, Some(_), _) => ServiceState::Stopping,
            (None, None, Some(_)) => ServiceState::Ready,
            (None, None, None) => ServiceState::Starting,
        }
    }

    /// Whether the service has not ended
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.ending.is_none()
    }

    /// The record as the service routes show it
    #[must_use]
    pub fn view(&self, log: PathBuf) -> ServiceView {
        let service = &self.service;
        ServiceView {
            id: service.id,
            name: service.name.clone(),
            owner: service.owner,
            state: self.state(),
            readiness: service.readiness.clone(),
            cwd: service.cwd.clone(),
            command: service.command.clone(),
            pid: self.child.map(|child| child.pid.as_raw()),
            max_secs: service.max.as_secs(),
            created_at: service.created_at,
            ready_at: self.ready_at,
            ended_at: self.ending.as_ref().map(|ending| ending.at),
            end: self.ending.as_ref().map(|ending| ending.end),
            exit: self.ending.as_ref().and_then(|ending| ending.exit.clone()),
            detail: self
                .ending
                .as_ref()
                .and_then(|ending| ending.detail.clone()),
            log,
        }
    }
}

/// A service as `service start`, `list`, and `stop` print it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceView {
    /// Service id
    pub id: ServiceId,
    /// Label
    pub name: String,
    /// Owning thread
    pub owner: ThreadId,
    /// Where it stands
    pub state: ServiceState,
    /// When `start` counts it as up
    pub readiness: Readiness,
    /// Working directory
    pub cwd: PathBuf,
    /// Program and arguments
    pub command: Vec<String>,
    /// Process id of the service, once spawned
    pub pid: Option<i32>,
    /// Longest lifetime in seconds
    pub max_secs: u64,
    /// When it was started
    pub created_at: DateTime<Utc>,
    /// When it became ready
    pub ready_at: Option<DateTime<Utc>>,
    /// When it ended
    pub ended_at: Option<DateTime<Utc>>,
    /// Why it ended
    pub end: Option<ServiceEnd>,
    /// How its process ended
    pub exit: Option<ExitReason>,
    /// What went wrong, for a readiness failure or a lost worker
    pub detail: Option<String>,
    /// Combined stdout and stderr
    pub log: PathBuf,
}

impl ServiceView {
    /// One line for human output
    #[must_use]
    pub fn human_line(&self) -> String {
        let state = match self.end {
            Some(end) => format!("ended: {}{}", end.as_str(), exit_suffix(self.exit.as_ref())),
            None => self.state.as_str().to_owned(),
        };
        let pid = self
            .pid
            .map(|pid| format!(" pid {pid}"))
            .unwrap_or_default();
        format!(
            "{} {} {state} ({}){pid} [{}]",
            self.id,
            self.name,
            self.readiness,
            self.cwd.display()
        )
    }
}

fn exit_suffix(exit: Option<&ExitReason>) -> String {
    match exit {
        Some(ExitReason::Exit { code }) => format!(", exit {code}"),
        Some(ExitReason::Signal { signal }) => format!(", signal {signal}"),
        Some(ExitReason::SpawnFailed { message }) => format!(", spawn failed: {message}"),
        Some(ExitReason::Cancelled) | None => String::new(),
    }
}

/// `POST /v1/services` body
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartRequest {
    /// Public API version
    pub api_version: u32,
    /// Label
    pub name: String,
    /// Owning thread, [`ThreadId::DETACHED`] for none
    pub owner: ThreadId,
    /// Absolute working directory
    pub cwd: PathBuf,
    /// Program and arguments
    pub command: Vec<String>,
    /// The caller's whole environment, which the service runs with. It goes
    /// straight to the worker process and is never stored
    pub env: BTreeMap<String, String>,
    /// When `start` counts it as up
    pub readiness: Readiness,
    /// How long to wait for readiness
    #[serde(with = "humantime_serde")]
    pub ready_timeout: Duration,
    /// Longest lifetime
    #[serde(with = "humantime_serde")]
    pub max: Duration,
    /// Caller's `PATH` and `HOME`, for event delivery
    pub callback_env: TaskEnv,
    /// Caller's working directory, for event delivery
    pub callback_cwd: PathBuf,
}

/// Answer of `GET /v1/services`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicesResponse {
    /// Public API version
    pub api_version: u32,
    /// Services, oldest first
    pub services: Vec<ServiceView>,
}

/// The `HOMEBASED_EVENT` payload of a service's `SERVICE_EXITED`
///
/// `tail` is the last lines of its output, which no longer change once it
/// has ended
#[must_use]
pub fn exited_payload(
    machine: MachineId,
    record: &ServiceRecord,
    ending: &Ending,
    tail: &[String],
) -> Value {
    let service = &record.service;
    let what = match ending.end {
        ServiceEnd::Lost => {
            "Its worker vanished without recording how it ended, so the service may be gone or half gone"
        }
        _ => "It exited on its own; nobody asked it to stop",
    };
    json!({
        "api_version": API_VERSION,
        "event": EXITED_EVENT,
        "service": service.id,
        "name": service.name,
        "thread": service.owner,
        "machine": machine,
        "cwd": service.cwd,
        "command": service.command,
        "end": ending.end,
        "exit": ending.exit,
        "detail": ending.detail,
        "started_at": record.started_at,
        "ready_at": record.ready_at,
        "ended_at": ending.at,
        "output_tail": tail,
        "next_action": format!(
            "Service {} ({}) is no longer running. {what}. Read its output with `homebased service log {}` and, if you still need it, start it again with `homebased service start`",
            service.id, service.name, service.id
        ),
    })
}

/// The last [`TAIL_LINES`] lines of `text`
#[must_use]
pub fn tail_lines(text: &str) -> Vec<String> {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(TAIL_LINES);
    lines[start..]
        .iter()
        .map(|line| (*line).to_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_end_nobody_chose_sends_an_event() {
        let sending: Vec<_> = [
            ServiceEnd::Stopped,
            ServiceEnd::OwnerDone,
            ServiceEnd::MaxElapsed,
            ServiceEnd::NotReady,
            ServiceEnd::Exited,
            ServiceEnd::Lost,
        ]
        .into_iter()
        .filter(|end| end.sends_event())
        .collect();
        assert_eq!(sending, vec![ServiceEnd::Exited, ServiceEnd::Lost]);
    }

    #[test]
    fn a_bad_ready_line_is_refused_before_anything_starts() {
        let bad = Readiness::Line {
            pattern: "listening (".into(),
        };
        assert!(matches!(bad.validate(), Err(AppError::Usage { .. })));
        let good = Readiness::Line {
            pattern: r"listening on \d+".into(),
        };
        assert!(good.validate().is_ok());
    }
}
