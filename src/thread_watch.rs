//! Thread watches: a stored condition on another thread's context that sends
//! `THREAD_CONTEXT_OVER` to the thread that asked, and `THREAD_WATCH_ENDED`
//! when the watch ends
//!
//! A watch lives on the machine of the thread that made it, which delivers its
//! events like a GPU queue job's. The watched thread may run on any Fleet
//! machine: the daemon reads its context there each minute through the same
//! lookup `homebased thread context` uses, and compacts it there for
//! `--action compact`

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{TaskEnv, ThreadId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::submission::CallbackContext;
use crate::thread_ref::{Caller, Provider, ThreadContext, ThreadSelector, short_tokens};

/// Event a watch sends when its thread's context is over the limit
pub const CONTEXT_OVER_EVENT: &str = "THREAD_CONTEXT_OVER";

/// Final event of every watch
pub const WATCH_ENDED_EVENT: &str = "THREAD_WATCH_ENDED";

/// A thread watch, chosen by the daemon
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WatchId(Uuid);

impl WatchId {
    /// Allocate a new v7 id
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wrap an existing UUID
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Underlying UUID
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for WatchId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for WatchId {
    type Err = AppError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(raw).map(Self).map_err(|_| AppError::Usage {
            message: format!("watch id must be a UUID: {raw}"),
        })
    }
}

/// Context size a watch fires above
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "unit", content = "value", rename_all = "snake_case")]
pub enum Threshold {
    /// Context tokens
    Tokens(u64),
    /// Percent of the model's context window, which only Codex records
    Percent(u8),
}

impl Threshold {
    /// Token limit for a thread with `window`, `None` when a percent has no window
    #[must_use]
    pub fn limit(self, window: Option<u64>) -> Option<u64> {
        match self {
            Self::Tokens(tokens) => Some(tokens),
            Self::Percent(percent) => window.map(|window| window * u64::from(percent) / 100),
        }
    }
}

impl FromStr for Threshold {
    type Err = AppError;

    /// `150000`, `150k`, `1.2m`, or `80%`
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let usage = || AppError::Usage {
            message: format!(
                "--context-above takes tokens (150000), thousands (150k), millions (1m), or a percent (80%), not {raw:?}"
            ),
        };
        let text = raw.trim().to_ascii_lowercase();
        if let Some(percent) = text.strip_suffix('%') {
            let percent: u8 = percent.trim().parse().map_err(|_| usage())?;
            if percent == 0 || percent > 100 {
                return Err(usage());
            }
            return Ok(Self::Percent(percent));
        }
        let (number, scale) = if let Some(number) = text.strip_suffix('k') {
            (number, 1_000.0)
        } else if let Some(number) = text.strip_suffix('m') {
            (number, 1_000_000.0)
        } else {
            (text.as_str(), 1.0)
        };
        let value: f64 = number.trim().parse().map_err(|_| usage())?;
        if !value.is_finite() || value <= 0.0 || value * scale > 1e12 {
            return Err(usage());
        }
        // checked above to be a positive count well inside u64
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        Ok(Self::Tokens((value * scale).round() as u64))
    }
}

impl fmt::Display for Threshold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tokens(tokens) => f.write_str(&short_tokens(*tokens)),
            Self::Percent(percent) => write!(f, "{percent}%"),
        }
    }
}

/// What the daemon does when a watch fires
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum WatchAction {
    /// Send `THREAD_CONTEXT_OVER` and leave the decision to the caller
    #[default]
    Notify,
    /// Compact the thread, then send `THREAD_CONTEXT_OVER` with the outcome
    Compact,
}

/// The thread a watch follows
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WatchTarget {
    /// A T3 thread, whose session is looked up again on every check
    T3 {
        /// Machine whose T3 runs it
        machine: MachineId,
        /// T3 thread id
        t3_thread: String,
    },
    /// One Claude Code session or Codex thread; the watch ends when it is gone
    Session {
        /// Machine that runs it
        machine: MachineId,
        /// Agent of the session
        provider: Provider,
        /// Session or thread id
        session: ThreadId,
    },
}

impl WatchTarget {
    /// Machine that runs the watched thread
    #[must_use]
    pub fn machine(&self) -> MachineId {
        match self {
            Self::T3 { machine, .. } | Self::Session { machine, .. } => *machine,
        }
    }

    /// Exact reference that names the target on its machine
    #[must_use]
    pub fn reference(&self) -> String {
        match self {
            Self::T3 { t3_thread, .. } => format!("thread_{t3_thread}"),
            Self::Session { session, .. } => session.to_string(),
        }
    }

    /// Key of the followed thread, part of [`ThreadWatch::key`]
    #[must_use]
    pub fn key(&self) -> String {
        match self {
            Self::T3 { machine, t3_thread } => format!("{machine}/t3/{t3_thread}"),
            Self::Session {
                machine, session, ..
            } => format!("{machine}/session/{session}"),
        }
    }
}

/// Why a watch ended
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchEndReason {
    /// A `--once` watch fired
    Once,
    /// `homebased thread unwatch`
    Unwatched,
    /// The T3 thread was archived
    Archived,
    /// The T3 thread was deleted
    Deleted,
    /// The watched session has no transcript or rollout any more
    SessionGone,
}

/// Kind of a watch event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchEventKind {
    /// The context is over the limit while the thread is idle
    #[serde(rename = "THREAD_CONTEXT_OVER")]
    ContextOver,
    /// The watch ended; always its last event
    #[serde(rename = "THREAD_WATCH_ENDED")]
    Ended,
}

/// A stored watch
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadWatch {
    /// Watch id
    pub id: WatchId,
    /// Thread that receives the events
    pub caller: ThreadId,
    /// Origin-only environment of the caller, for event delivery
    pub callback: CallbackContext,
    /// Followed thread
    pub target: WatchTarget,
    /// Reference the caller passed, echoed in events
    pub reference: String,
    /// Title when the watch was made
    pub title: Option<String>,
    /// Limit the context must pass
    pub threshold: Threshold,
    /// What firing does
    pub action: WatchAction,
    /// End after the first firing
    pub once: bool,
    /// When the watch was made
    pub created_at: DateTime<Utc>,
}

impl ThreadWatch {
    /// Key that makes a repeat of the same watch by one caller that watch
    ///
    /// Two limits or actions on one thread are two watches, so a caller can be
    /// told at one size and have the thread compacted at a larger one; only a
    /// retry of all four finds the existing watch
    #[must_use]
    pub fn key(&self) -> String {
        let threshold = match self.threshold {
            Threshold::Tokens(tokens) => format!("tokens:{tokens}"),
            Threshold::Percent(percent) => format!("percent:{percent}"),
        };
        let action = match self.action {
            WatchAction::Notify => "notify",
            WatchAction::Compact => "compact",
        };
        format!("{}/{threshold}/{action}", self.target.key())
    }
}

/// A watch and where it stands
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchView {
    /// Watch id; `homebased wait --next` takes it
    pub watch: WatchId,
    /// Thread that receives the events
    pub caller: ThreadId,
    /// Followed thread
    pub target: WatchTarget,
    /// Reference the caller passed
    pub reference: String,
    /// Title when the watch was made
    pub title: Option<String>,
    /// Limit the context must pass
    pub threshold: Threshold,
    /// What firing does
    pub action: WatchAction,
    /// End after the first firing
    pub once: bool,
    /// Whether the next climb over the limit fires; false until the context
    /// drops below it again
    pub armed: bool,
    /// Last event sequence
    pub seq: u64,
    /// When the watch was made
    pub created_at: DateTime<Utc>,
    /// When it ended, if it did
    pub ended_at: Option<DateTime<Utc>>,
    /// Why it ended, if it did
    pub end_reason: Option<WatchEndReason>,
    /// Whether this request made the watch; false when it already existed
    #[serde(default)]
    pub created: bool,
}

impl WatchView {
    /// One line for human output
    #[must_use]
    pub fn human_line(&self) -> String {
        let title = self.title.as_deref().unwrap_or("(untitled)");
        let action = match self.action {
            WatchAction::Notify => "notify",
            WatchAction::Compact => "compact",
        };
        let once = if self.once { ", once" } else { "" };
        let status = match (self.end_reason, self.armed) {
            (Some(reason), _) => format!("ended: {}", end_reason_str(reason)),
            (None, true) => "armed".into(),
            (None, false) => "fired, re-arms below the limit".into(),
        };
        format!(
            "{} {title} above {} ({action}{once}) {status} [{}]",
            self.watch, self.threshold, self.reference
        )
    }
}

fn end_reason_str(reason: WatchEndReason) -> &'static str {
    match reason {
        WatchEndReason::Once => "once",
        WatchEndReason::Unwatched => "unwatched",
        WatchEndReason::Archived => "archived",
        WatchEndReason::Deleted => "deleted",
        WatchEndReason::SessionGone => "session_gone",
    }
}

/// One stored watch event: the `HOMEBASED_EVENT` payload, fixed when it is made
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchEvent {
    /// Watch id
    pub watch: WatchId,
    /// Per-watch sequence, from one
    pub seq: u64,
    /// What the event reports
    pub kind: WatchEventKind,
    /// Full payload of the `HOMEBASED_EVENT` line
    pub payload: serde_json::Value,
}

impl WatchEvent {
    /// The line delivery sends
    #[must_use]
    pub fn line(&self) -> String {
        format!("HOMEBASED_EVENT {}", self.payload)
    }
}

/// `POST /v1/threads/watches` body
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchRequest {
    /// Public API version
    pub api_version: u32,
    /// Thread that receives the events
    pub caller: ThreadId,
    /// The caller as its environment names it, for `children`
    #[serde(default)]
    pub caller_ids: Caller,
    /// Caller's environment, for event delivery
    pub env: TaskEnv,
    /// Caller's working directory, for event delivery
    pub callback_cwd: PathBuf,
    /// Threads to watch
    #[serde(default)]
    pub threads: Vec<ThreadSelector>,
    /// Watch the caller's T3 children too
    #[serde(default)]
    pub children: bool,
    /// Look only on this machine, by name or UUID
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    /// Limit the context must pass
    pub threshold: Threshold,
    /// What firing does
    #[serde(default)]
    pub action: WatchAction,
    /// End after the first firing
    #[serde(default)]
    pub once: bool,
}

/// Answer of the watch routes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchesResponse {
    /// Public API version
    pub api_version: u32,
    /// Watches, oldest first
    pub watches: Vec<WatchView>,
}

/// `POST /v1/threads/compact` body
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactRequest {
    /// Public API version
    pub api_version: u32,
    /// Thread to compact; it must resolve to exactly one
    pub thread: ThreadSelector,
    /// Look only on this machine, by name or UUID
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
}

/// Result of a compaction request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactOutcome {
    /// T3 started the compaction
    Started,
    /// No open T3 thread owns the session
    NotFound,
    /// T3 would not compact now, for example while a turn runs
    Refused,
    /// T3 could not be reached, or its reply was lost
    Unavailable,
}

impl CompactOutcome {
    /// Lowercase name, as on the wire
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::NotFound => "not_found",
            Self::Refused => "refused",
            Self::Unavailable => "unavailable",
        }
    }
}

/// `POST /v1/threads/compact` answer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactResponse {
    /// Public API version
    pub api_version: u32,
    /// What happened
    pub outcome: CompactOutcome,
    /// Why, when T3 did not start the compaction
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The compacted thread, as it was before the compaction
    pub thread: ThreadContext,
}

#[cfg(test)]
mod tests {
    use super::Threshold;

    #[test]
    fn thresholds_read_tokens_thousands_millions_and_percents() {
        let parse = |text: &str| text.parse::<Threshold>().ok();
        assert_eq!(parse("150000"), Some(Threshold::Tokens(150_000)));
        assert_eq!(parse("150k"), Some(Threshold::Tokens(150_000)));
        assert_eq!(parse("1.2M"), Some(Threshold::Tokens(1_200_000)));
        assert_eq!(parse("80%"), Some(Threshold::Percent(80)));
        for bad in ["0", "-5k", "120%", "0%", "lots", ""] {
            assert_eq!(parse(bad), None, "{bad}");
        }
        assert_eq!(Threshold::Percent(80).limit(Some(258_400)), Some(206_720));
        assert_eq!(Threshold::Percent(80).limit(None), None);
    }
}
