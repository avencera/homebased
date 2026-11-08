//! Thread references that `homebased thread` and `message send --thread`
//! accept, and the wire types of thread lookups
//!
//! Agents pass on whatever they were given: T3 chip text
//! (`[Thread: <title>; ref=thread_<id>]`), `thread_<id>`, a
//! `t3-thread://v2/<environment>/<thread>` link, a bare T3 thread id, or a
//! Claude Code session or Codex thread UUID. T3 ids come in several shapes, a
//! bare UUID, `mcp:<uuid>`, `mcp:launch:<hex>`, `thread:delegated-task:<...>`,
//! so they are opaque strings matched exactly. Only UUID-shaped text, bare or
//! after `mcp:`, may be a prefix, and only for reads

use std::fmt;

use chrono::{DateTime, Datelike, NaiveDate, TimeDelta, Utc, Weekday};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::t3::ThreadDetails;

/// Scheme of the T3 links this parses and prints
pub const T3_LINK_PREFIX: &str = "t3-thread://v2/";

/// Older link form that `homebased t3 launch` printed
const T3_LAUNCH_LINK_PREFIX: &str = "t3code://threads/";

/// Shortest UUID prefix a read accepts, so a stray character matches nothing
pub const MIN_PREFIX_CHARS: usize = 4;

/// What one reference names before any lookup
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadRef {
    /// A T3 thread id, from chip text, `thread_<id>`, or a link
    T3 {
        /// T3 environment from a link, which picks the machine
        environment: Option<String>,
        /// T3 thread id
        thread: String,
    },
    /// A bare id: a T3 thread id, or a Claude session or Codex thread UUID
    Id(String),
    /// The `ref` of T3 chip text, which is the id of a context record in the
    /// message that carried the chip, not a thread id
    ///
    /// T3 makes it `thread_<id>` for a thread id of plain characters, and
    /// otherwise a slug and hash of the id, or a fresh UUID for a chip pasted
    /// on mobile. Only the record maps it back to its thread
    Chip {
        /// The context record id after `ref=`
        context: String,
    },
}

impl ThreadRef {
    /// Read one reference as the caller passed it
    ///
    /// # Errors
    ///
    /// `usage` for empty text or a link without a thread
    pub fn parse(text: &str) -> Result<Self, AppError> {
        let text = text.trim();
        let usage = |message: String| AppError::Usage { message };
        if text.is_empty() {
            return Err(usage("a thread reference cannot be empty".into()));
        }
        // a title can hold anything, so the id is after the last marker
        if text.starts_with('[')
            && let Some(start) = text.rfind("ref=")
        {
            let rest = &text[start + "ref=".len()..];
            let end = rest
                .find(|c: char| c == ']' || c == ';' || c.is_whitespace())
                .unwrap_or(rest.len());
            let context = &rest[..end];
            if context.is_empty() {
                return Err(usage(format!("thread chip {text:?} has no id after ref=")));
            }
            return Ok(Self::Chip {
                context: context.to_owned(),
            });
        }
        // the `threadId:` line of a `<t3_context>` block, copied with its label
        if let Some(thread) = text.strip_prefix("threadId:").map(str::trim) {
            return Self::parse(thread);
        }
        for prefix in [T3_LINK_PREFIX, T3_LAUNCH_LINK_PREFIX] {
            if let Some(rest) = text.strip_prefix(prefix) {
                let (environment, thread) = rest
                    .split_once('/')
                    .filter(|(environment, thread)| !environment.is_empty() && !thread.is_empty())
                    .ok_or_else(|| {
                        usage(format!(
                            "T3 link {text:?} must look like {T3_LINK_PREFIX}<environment>/<thread>"
                        ))
                    })?;
                return Ok(Self::T3 {
                    environment: Some(environment.to_owned()),
                    thread: thread.to_owned(),
                });
            }
        }
        if let Some(thread) = text.strip_prefix("thread_").filter(|id| !id.is_empty()) {
            return Ok(Self::T3 {
                environment: None,
                thread: thread.to_owned(),
            });
        }
        Ok(Self::Id(text.to_owned()))
    }

    /// The thread a chip names when T3 has no record of it: `thread_<id>` is
    /// the thread id itself for ids of plain characters, while any other
    /// `ref` cannot be read back
    #[must_use]
    pub fn chip_fallback(&self) -> Option<Self> {
        let Self::Chip { context } = self else {
            return None;
        };
        let thread = context
            .strip_prefix("thread_")
            .filter(|id| !id.is_empty())?;
        Some(Self::T3 {
            environment: None,
            thread: thread.to_owned(),
        })
    }

    /// The full UUID a bare id is, if it is one
    #[must_use]
    pub fn uuid(&self) -> Option<Uuid> {
        match self {
            Self::Id(id) => Uuid::parse_str(id).ok(),
            Self::T3 { .. } | Self::Chip { .. } => None,
        }
    }

    /// The UUID prefix a bare id may be: UUID-shaped text, bare or after
    /// `mcp:`, that is not a whole UUID
    #[must_use]
    pub fn uuid_prefix(&self) -> Option<&str> {
        let Self::Id(id) = self else {
            return None;
        };
        let digits = id.strip_prefix("mcp:").unwrap_or(id);
        let shaped = digits.len() >= MIN_PREFIX_CHARS
            && digits.len() < 36
            && digits.chars().all(|c| c.is_ascii_hexdigit() || c == '-');
        shaped.then_some(id.as_str())
    }
}

impl fmt::Display for ThreadRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::T3 {
                environment: Some(environment),
                thread,
            } => write!(f, "{T3_LINK_PREFIX}{environment}/{thread}"),
            Self::T3 { thread, .. } => write!(f, "thread_{thread}"),
            Self::Id(id) => f.write_str(id),
            Self::Chip { context } => write!(f, "[Thread: ; ref={context}]"),
        }
    }
}

/// Link to a T3 thread that every reader of this module accepts
#[must_use]
pub fn t3_link(environment: &str, thread: &str) -> String {
    format!("{T3_LINK_PREFIX}{environment}/{thread}")
}

/// One thread to look up
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ThreadSelector {
    /// Any reference form [`ThreadRef::parse`] reads
    Reference {
        /// Text as the caller passed it
        text: String,
    },
    /// The origin thread of a task submitted from the machine that looks it up
    Task {
        /// Task id
        task: TaskId,
    },
    /// The worker session of a task running on the machine that looks it up
    Worker {
        /// Task id
        task: TaskId,
    },
}

impl fmt::Display for ThreadSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reference { text } => f.write_str(text),
            Self::Task { task } => write!(f, "--task {task}"),
            Self::Worker { task } => write!(f, "--worker {task}"),
        }
    }
}

/// The calling agent's own thread, as its environment names it
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Caller {
    /// `T3CODE_THREAD_ID`, when T3 Code runs the caller
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t3_thread: Option<String>,
    /// Claude Code session or Codex thread of the caller
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<ThreadId>,
}

/// Env var T3 Code sets to the T3 thread id of the agent it runs
pub const T3_THREAD_ENV: &str = "T3CODE_THREAD_ID";

impl Caller {
    /// Read the caller's thread from this process's environment
    ///
    /// # Errors
    ///
    /// `usage` when a session variable is set but is not a UUID
    pub fn from_env() -> Result<Self, AppError> {
        Ok(Self {
            t3_thread: std::env::var(T3_THREAD_ENV)
                .ok()
                .map(|id| id.trim().to_owned())
                .filter(|id| !id.is_empty()),
            session: crate::domain::session_thread()?,
        })
    }

    /// The caller's own thread as a reference, for a command given none
    #[must_use]
    pub fn selector(&self) -> Option<ThreadSelector> {
        let text = match (&self.t3_thread, self.session) {
            (Some(thread), _) => format!("thread_{thread}"),
            (None, Some(session)) => session.to_string(),
            (None, None) => return None,
        };
        Some(ThreadSelector::Reference { text })
    }
}

/// `POST /v1/threads/context` body
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadContextRequest {
    /// Public API version
    pub api_version: u32,
    /// Threads to look up
    #[serde(default)]
    pub threads: Vec<ThreadSelector>,
    /// List the T3 children of this caller too
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub children_of: Option<Caller>,
    /// Leave out children that are settled or archived
    #[serde(default)]
    pub live: bool,
    /// Look only on this machine, by name or UUID
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    /// Let UUID-shaped text match as a prefix
    #[serde(default)]
    pub prefixes: bool,
    /// The caller's T3 thread, whose message any T3 chip in `threads` came in
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carried_by: Option<String>,
}

/// `POST /v1/threads/context` answer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadContextResponse {
    /// Public API version
    pub api_version: u32,
    /// One entry per requested thread, in order
    pub threads: Vec<ThreadContext>,
    /// The caller's T3 children, newest first, when asked for
    #[serde(default)]
    pub children: Vec<ThreadContext>,
    /// With children asked for: whether every Fleet machine asked answered,
    /// so `children` is the whole list
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub children_complete: Option<bool>,
    /// With children asked for: each machine that did not answer, with why
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unanswered: Vec<String>,
}

/// What hosts a thread, which decides what can be done to it
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadHost {
    /// A T3 Code thread, which homebased can compact
    T3,
    /// A Claude Code or Codex session outside T3 and homebased
    Terminal,
    /// The agent session of a homebased task
    Worker,
}

impl ThreadHost {
    /// Lowercase name, as on the wire
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::T3 => "t3",
            Self::Terminal => "terminal",
            Self::Worker => "worker",
        }
    }
}

/// Agent that runs a thread
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// Claude Code
    Claude,
    /// Codex
    Codex,
}

impl From<crate::t3::ProviderThread> for Provider {
    fn from(thread: crate::t3::ProviderThread) -> Self {
        match thread {
            crate::t3::ProviderThread::Claude(_) => Self::Claude,
            crate::t3::ProviderThread::Codex(_) => Self::Codex,
        }
    }
}

/// What a thread is doing
///
/// T3 threads are read from T3's own run records. A session outside T3 has
/// none, so it counts as running while its transcript changed in the last
/// [`ACTIVE_WITHOUT_T3`] and idle after
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadState {
    /// A turn is running
    Running,
    /// No turn runs and nothing is pending
    Idle,
    /// No turn runs, and homebased tasks or jobs it submitted still owe it events
    Waiting,
    /// T3 settled the thread: its work is done
    Settled,
    /// The T3 thread is archived
    Archived,
    /// No transcript or run record says
    Unknown,
}

impl ThreadState {
    /// Lowercase name, as on the wire
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Idle => "idle",
            Self::Waiting => "waiting",
            Self::Settled => "settled",
            Self::Archived => "archived",
            Self::Unknown => "unknown",
        }
    }

    /// Whether a turn may be running, so the thread must not be disturbed
    #[must_use]
    pub fn busy(self) -> bool {
        matches!(self, Self::Running | Self::Unknown)
    }

    /// Whether the thread finished or was put away
    #[must_use]
    pub fn finished(self) -> bool {
        matches!(self, Self::Settled | Self::Archived)
    }
}

/// How long a session outside T3 counts as running after its last activity
pub const ACTIVE_WITHOUT_T3: TimeDelta = TimeDelta::minutes(2);

/// One thread and how full its context is
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadContext {
    /// Reference this entry answers, as the caller passed it; children have none
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    /// Machine that runs the thread
    pub machine: MachineId,
    /// Name of that machine
    pub machine_name: String,
    /// What hosts the thread
    pub host: ThreadHost,
    /// Agent of the thread, once it has a session
    pub provider: Option<Provider>,
    /// Claude Code session or Codex thread, once T3 started one
    pub session: Option<ThreadId>,
    /// Whether `session` still has its transcript or rollout on disk; `None`
    /// when there is no session, or from a peer that predates the field
    #[serde(default)]
    pub session_exists: Option<bool>,
    /// T3 thread id
    pub t3_thread: Option<String>,
    /// `t3-thread://v2/<environment>/<thread>`, when the machine's T3 names its environment
    pub t3_link: Option<String>,
    /// T3 thread title
    pub title: Option<String>,
    /// What the thread is doing
    pub state: ThreadState,
    /// Context tokens of the last request or compaction, once there was one
    pub tokens: Option<u64>,
    /// Whether `tokens` is an estimate after a compaction, until the next request
    pub tokens_estimated: bool,
    /// Context window of the model, which Codex records and Claude does not
    pub context_window: Option<u64>,
    /// `tokens` as a percent of `context_window`, when both are known
    pub percent: Option<f64>,
    /// Time of the last session activity
    pub last_active: Option<DateTime<Utc>>,
    /// Whether the one-hour prompt cache is still warm, so compacting is cheap
    pub cache_warm: Option<bool>,
    /// Last compaction, when the transcript's or rollout's tail shows one
    pub compacted_at: Option<DateTime<Utc>>,
    /// T3 thread that started this one, for a child
    pub parent_t3_thread: Option<String>,
    /// Project, branch, model, and other context T3 records
    #[serde(flatten)]
    pub details: ThreadDetails,
}

impl ThreadContext {
    /// A reference that names exactly this thread, for commands that need one
    #[must_use]
    pub fn exact_reference(&self) -> String {
        match (&self.t3_link, &self.t3_thread, self.session) {
            (Some(link), _, _) => link.clone(),
            (None, Some(thread), _) => format!("thread_{thread}"),
            (None, None, Some(session)) => session.to_string(),
            (None, None, None) => self.reference.clone().unwrap_or_default(),
        }
    }

    /// One line for human output
    #[must_use]
    pub fn human_line(&self) -> String {
        let title = self.title.as_deref().unwrap_or("(untitled)");
        let size = match (self.tokens, self.percent) {
            (Some(tokens), Some(percent)) => format!("{} ({percent:.0}%)", short_tokens(tokens)),
            (Some(tokens), None) => short_tokens(tokens),
            (None, _) => "no context yet".into(),
        };
        let estimate = if self.tokens_estimated { " est." } else { "" };
        let active = self
            .last_active
            .map(|at| format!(", active {}", central_time(at)))
            .unwrap_or_default();
        let cache = match self.cache_warm {
            Some(true) => ", cache warm",
            Some(false) => ", cache cold",
            None => "",
        };
        let model = self
            .details
            .model
            .as_deref()
            .map(|model| format!(", {model}"))
            .unwrap_or_default();
        format!(
            "{title} [{}{estimate}, {}{active}{cache}{model}] on {} {}",
            size,
            self.state.as_str(),
            self.machine_name,
            self.exact_reference()
        )
    }
}

/// `250k`, `1.2M`, or the plain count below a thousand
#[must_use]
pub fn short_tokens(tokens: u64) -> String {
    // display only, so float rounding is fine
    #[allow(clippy::cast_precision_loss)]
    let value = tokens as f64;
    if tokens >= 1_000_000 {
        format!("{:.1}M", value / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.0}k", value / 1_000.0)
    } else {
        tokens.to_string()
    }
}

/// `2026-11-07 21:15 CST`: US Central time, which human output uses
///
/// Applies the US daylight saving rule since 2007, second Sunday of March to
/// the first Sunday of November at 2am local, so no time zone database is needed
#[must_use]
pub fn central_time(at: DateTime<Utc>) -> String {
    let standard = at - TimeDelta::hours(6);
    let year = standard.year();
    let start = nth_sunday(year, 3, 2).and_hms_opt(2, 0, 0);
    let end = nth_sunday(year, 11, 1).and_hms_opt(1, 0, 0);
    let local = standard.naive_utc();
    let daylight = start
        .zip(end)
        .is_some_and(|(start, end)| local >= start && local < end);
    let (shown, zone) = if daylight {
        (local + TimeDelta::hours(1), "CDT")
    } else {
        (local, "CST")
    };
    format!("{} {zone}", shown.format("%Y-%m-%d %H:%M"))
}

/// Day of the `nth` Sunday of `month` in `year`
fn nth_sunday(year: i32, month: u32, nth: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).unwrap_or_default();
    let offset = (7 - first.weekday().num_days_from_sunday()) % 7;
    let day = 1 + offset + 7 * (nth - 1);
    debug_assert_eq!(
        NaiveDate::from_ymd_opt(year, month, day).map(|date| date.weekday()),
        Some(Weekday::Sun)
    );
    NaiveDate::from_ymd_opt(year, month, day).unwrap_or(first)
}

#[cfg(test)]
mod tests {
    use super::{ThreadRef, central_time};

    fn t3(thread: &str) -> ThreadRef {
        ThreadRef::T3 {
            environment: None,
            thread: thread.into(),
        }
    }

    #[test]
    fn reads_every_reference_form_as_given() {
        let delegated = "thread:delegated-task:command%3Amcp%3A08207a13%3Adelegate-task%3Ax";
        let chip =
            ThreadRef::parse(&format!("[Thread: Fix [the] bug; ref=thread_{delegated}]")).unwrap();
        assert_eq!(
            chip,
            ThreadRef::Chip {
                context: format!("thread_{delegated}")
            }
        );
        // with no record of the chip, only the `thread_<id>` form reads back
        assert_eq!(chip.chip_fallback(), Some(t3(delegated)));
        let pasted =
            ThreadRef::parse("[Thread: Decide; ref=910af7b5-c1a3-4b38-8ad3-95f52dca76f5]").unwrap();
        assert_eq!(pasted.chip_fallback(), None);
        assert_eq!(pasted.uuid(), None);
        assert_eq!(
            ThreadRef::parse("threadId: mcp:launch:a9edc0e1").unwrap(),
            ThreadRef::Id("mcp:launch:a9edc0e1".into())
        );
        assert_eq!(
            ThreadRef::parse("thread_mcp:launch:8bd44cf5").unwrap(),
            t3("mcp:launch:8bd44cf5")
        );
        assert_eq!(
            ThreadRef::parse("t3-thread://v2/67fe8ce7-5fcf-4a08-b5dc-c0c475ba033c/mcp:launch:ab")
                .unwrap(),
            ThreadRef::T3 {
                environment: Some("67fe8ce7-5fcf-4a08-b5dc-c0c475ba033c".into()),
                thread: "mcp:launch:ab".into()
            }
        );
        let bare = ThreadRef::parse(" 6c8e923e-f25a-4727-9f05-83cedd5d3fd8 ").unwrap();
        assert!(bare.uuid().is_some());
        assert_eq!(bare.uuid_prefix(), None);

        assert_eq!(
            ThreadRef::parse("6c8e923e").unwrap().uuid_prefix(),
            Some("6c8e923e")
        );
        assert_eq!(
            ThreadRef::parse("mcp:6c8e").unwrap().uuid_prefix(),
            Some("mcp:6c8e")
        );
        // an opaque T3 id is never a prefix
        assert_eq!(
            ThreadRef::parse("mcp:launch:8bd4").unwrap().uuid_prefix(),
            None
        );
        assert!(ThreadRef::parse("t3-thread://v2/env-only").is_err());
        assert!(ThreadRef::parse("  ").is_err());
    }

    #[test]
    fn central_time_follows_daylight_saving() {
        let at = |text: &str| chrono::DateTime::parse_from_rfc3339(text).unwrap().to_utc();
        assert_eq!(
            central_time(at("2026-07-01T17:00:00Z")),
            "2026-07-01 12:00 CDT"
        );
        assert_eq!(
            central_time(at("2026-12-01T17:00:00Z")),
            "2026-12-01 11:00 CST"
        );
        // 2026 changes on March 8 and November 1
        assert_eq!(
            central_time(at("2026-03-08T07:59:00Z")),
            "2026-03-08 01:59 CST"
        );
        assert_eq!(
            central_time(at("2026-03-08T08:00:00Z")),
            "2026-03-08 03:00 CDT"
        );
        assert_eq!(
            central_time(at("2026-11-01T06:59:00Z")),
            "2026-11-01 01:59 CDT"
        );
        assert_eq!(
            central_time(at("2026-11-01T07:00:00Z")),
            "2026-11-01 01:00 CST"
        );
    }
}
