//! JSON lines record of every Claude session compaction request
//!
//! Each line says what asked for the compaction, the session's size and idle
//! time then, and what T3 answered. The size after compaction is in the session
//! transcript's `compact_boundary` line, so analysis joins the two by session

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tracing::warn;

use crate::callback::stale_context::ContextUse;
use crate::domain::ThreadId;
use crate::t3::WakeOutcome;

/// What asked for a compaction
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// The daemon's scan of idle threads that wait on a task
    Idle,
    /// The dashboard's Compact button
    Manual,
    /// A callback that arrived after the prompt cache lapsed
    Callback,
}

/// Log the daemon writes callback compactions to
///
/// Callback delivery runs deep in transport code that has no homebased home,
/// so the daemon sets the path once at startup instead of threading it through
static DEFAULT_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Set the log [`record_default`] writes to, once per process
pub fn set_default(path: PathBuf) {
    let _ = DEFAULT_PATH.set(path);
}

/// [`record`] to the log set by [`set_default`], if any
pub fn record_default(
    trigger: Trigger,
    session: ThreadId,
    usage: &ContextUse,
    outcome: &WakeOutcome,
    now: DateTime<Utc>,
) {
    if let Some(path) = DEFAULT_PATH.get() {
        record(path, trigger, session, usage, outcome, now);
    }
}

/// One compaction request
#[derive(Debug, Serialize)]
struct Record<'a> {
    at: DateTime<Utc>,
    trigger: Trigger,
    session: ThreadId,
    t3_thread: Option<&'a str>,
    tokens: u64,
    last_active: DateTime<Utc>,
    idle_secs: i64,
    cache_warm: bool,
    outcome: &'static str,
    detail: Option<&'a str>,
}

/// Append one request and T3's answer to the log at `path`
///
/// The log is for analysis, so a write failure is only reported
pub fn record(
    path: &Path,
    trigger: Trigger,
    session: ThreadId,
    usage: &ContextUse,
    outcome: &WakeOutcome,
    now: DateTime<Utc>,
) {
    let (label, t3_thread, detail) = match outcome {
        WakeOutcome::Woken { t3_thread } => ("started", Some(t3_thread.as_str()), None),
        WakeOutcome::NotT3Thread => ("not_t3_thread", None, None),
        WakeOutcome::Unavailable(detail) => ("unavailable", None, Some(detail.as_str())),
        WakeOutcome::Refused(detail) => ("refused", None, Some(detail.as_str())),
        WakeOutcome::Uncertain(detail) => ("uncertain", None, Some(detail.as_str())),
        WakeOutcome::ApiChanged(detail) => ("api_changed", None, Some(detail.as_str())),
    };
    let line = Record {
        at: now,
        trigger,
        session,
        t3_thread,
        tokens: usage.tokens(),
        last_active: usage.last_active(),
        idle_secs: (now - usage.last_active()).num_seconds(),
        cache_warm: usage.cache_warm(now),
        outcome: label,
        detail,
    };

    let written = serde_json::to_string(&line)
        .map_err(std::io::Error::other)
        .and_then(|mut json| {
            json.push('\n');
            // one append-mode write per line, so the scan and the dashboard never interleave
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?
                .write_all(json.as_bytes())
        });
    if let Err(error) = written {
        warn!("compaction log {}: {error}", path.display());
    }
}
