//! Whether a Claude session should compact before a callback wakes it
//!
//! Claude Code caches the conversation prefix for one hour. A callback that
//! arrives later pays to read the whole context again, and every later request
//! carries it too. Interactive Claude Code compacts a long idle session before
//! the cache lapses, but Agent SDK hosts such as T3 Code do not, so homebased
//! asks the host to compact while a callback is still pending, at 55 idle
//! minutes, while the cache is warm. A callback that arrives after the cache
//! lapsed compacts first: the compaction's cold write costs less than the
//! callback's turn would pay, and every later request then reads the small
//! summary instead of the whole context
//!
//! The session transcript (`~/.claude/projects/<cwd key>/<id>.jsonl`) is the
//! source: each assistant line carries the API `usage` of its request, and a
//! `compact_boundary` system line carries the size after a compaction

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use chrono::{DateTime, TimeDelta, Utc};
use serde::Deserialize;

use super::claude_inbox::transcript_path;
use crate::domain::ThreadId;
use crate::t3::TurnStart;

/// Idle time after which the one-hour prompt cache has lapsed
const CACHE_LIFETIME: TimeDelta = TimeDelta::hours(1);

/// Idle time after which an idle compaction runs while the cache is still warm
///
/// The compaction request reads the cached prefix, so it costs a fraction of
/// a cold read. The margin before [`CACHE_LIFETIME`] covers a slow start
const IDLE_COMPACTION_AFTER: TimeDelta = TimeDelta::minutes(55);

/// How long before an idle compaction a message already waits for it
///
/// A message read as not yet due can reach T3 seconds after the compaction
/// started, and T3 refuses to steer it into the compaction
const QUEUE_MARGIN: TimeDelta = TimeDelta::minutes(1);

/// Smallest context worth an idle compaction, which runs even if no callback
/// comes back before the cache would lapse
pub(crate) const MIN_IDLE_CONTEXT_TOKENS: u64 = 200_000;

/// Transcript suffix scanned for the last request
///
/// The final assistant line of a turn is usually short text, so this only
/// misses when tool output after it fills the suffix, and then nothing compacts
const TAIL_SCAN_BYTES: u64 = 4 * 1024 * 1024;

/// Context size and last activity of a Claude session
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContextUse {
    tokens: u64,
    last_active: DateTime<Utc>,
}

impl ContextUse {
    /// Read the session `thread` from its transcript under `home`
    ///
    /// `None` when there is no transcript or its tail has no request
    pub(crate) fn read(home: &Path, thread: ThreadId) -> Option<Self> {
        let path = transcript_path(&home.join(".claude/projects"), thread)?;
        Self::from_tail(&read_tail(&path)?)
    }

    /// How a T3 thread takes a message for this session at `now`
    ///
    /// A thread due an idle compaction may be running one, which T3 refuses to
    /// steer into, so the message waits for the active run. The transcript
    /// shows a compaction only once it finishes
    pub(crate) fn turn_start(&self, now: DateTime<Utc>) -> TurnStart {
        let idle = now - self.last_active;
        if idle >= IDLE_COMPACTION_AFTER - QUEUE_MARGIN && self.tokens >= MIN_IDLE_CONTEXT_TOKENS {
            TurnStart::AfterActive
        } else {
            TurnStart::Auto
        }
    }

    /// Whether to compact an idle session at `now`, while its cache is warm
    pub(crate) fn idle_compaction_due(&self, now: DateTime<Utc>) -> bool {
        let idle = now - self.last_active;
        self.tokens >= MIN_IDLE_CONTEXT_TOKENS
            && idle >= IDLE_COMPACTION_AFTER
            && idle < CACHE_LIFETIME
    }

    /// Whether to compact before a callback at `now`, after the cache lapsed
    pub(crate) fn cold_compaction_due(&self, now: DateTime<Utc>) -> bool {
        self.tokens >= MIN_IDLE_CONTEXT_TOKENS && !self.cache_warm(now)
    }

    /// Key naming this idle period for a compaction request
    ///
    /// Every compaction of one idle period shares it, so T3 drops a repeat from
    /// the idle scan, a callback, or the dashboard while the first still runs
    pub(crate) fn compaction_key(&self) -> String {
        format!("idle since {}", self.last_active.to_rfc3339())
    }

    /// Whether the prompt cache from the last request is still alive at `now`
    pub(crate) fn cache_warm(&self, now: DateTime<Utc>) -> bool {
        now - self.last_active < CACHE_LIFETIME
    }

    /// Context tokens of the last request or compaction
    pub(crate) fn tokens(&self) -> u64 {
        self.tokens
    }

    /// Time of the last session activity
    pub(crate) fn last_active(&self) -> DateTime<Utc> {
        self.last_active
    }

    fn from_tail(tail: &[u8]) -> Option<Self> {
        let mut last_active = None;
        for raw in tail.rsplit(|byte| *byte == b'\n') {
            // the first line of the tail may be cut off and does not parse
            let Ok(line) = serde_json::from_slice::<Line>(raw) else {
                continue;
            };
            if line.is_sidechain
                || !matches!(line.kind.as_deref(), Some("user" | "assistant" | "system"))
            {
                continue;
            }
            let Some(at) = line.timestamp else {
                continue;
            };
            let last_active = *last_active.get_or_insert(at);
            if let Some(tokens) = line.context_tokens() {
                return Some(Self {
                    tokens,
                    last_active,
                });
            }
        }
        None
    }
}

/// Transcript line fields this module reads
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Line {
    #[serde(rename = "type")]
    kind: Option<String>,
    subtype: Option<String>,
    timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    is_sidechain: bool,
    message: Option<Message>,
    compact_metadata: Option<CompactMetadata>,
}

impl Line {
    fn context_tokens(&self) -> Option<u64> {
        if self.subtype.as_deref() == Some("compact_boundary") {
            // an unknown size after compaction is small enough to skip
            let after = self
                .compact_metadata
                .as_ref()
                .and_then(|meta| meta.post_tokens);
            return Some(after.unwrap_or(0));
        }
        let usage = self.message.as_ref()?.usage.as_ref()?;
        // the reply joins the context of the next request
        let tokens = [
            usage.input_tokens,
            usage.cache_creation_input_tokens,
            usage.cache_read_input_tokens,
            usage.output_tokens,
        ]
        .into_iter()
        .map(|count| count.unwrap_or(0))
        .sum();
        // error and interrupt lines carry zero usage and say nothing of the context
        (tokens > 0).then_some(tokens)
    }
}

#[derive(Deserialize)]
struct Message {
    usage: Option<Usage>,
}

#[derive(Deserialize)]
struct Usage {
    input_tokens: Option<u64>,
    cache_creation_input_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
    output_tokens: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompactMetadata {
    post_tokens: Option<u64>,
}

fn read_tail(path: &Path) -> Option<Vec<u8>> {
    let mut file = File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    file.seek(SeekFrom::Start(len.saturating_sub(TAIL_SCAN_BYTES)))
        .ok()?;
    let mut tail = Vec::new();
    file.read_to_end(&mut tail).ok()?;
    Some(tail)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::str::FromStr;

    use serde_json::{Value, json};

    use super::*;

    const THREAD: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e431";

    fn at(minutes_ago: i64) -> String {
        (now() - TimeDelta::minutes(minutes_ago)).to_rfc3339()
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-09T12:00:00Z")
            .unwrap()
            .to_utc()
    }

    fn assistant(minutes_ago: i64, cache_read: u64) -> Value {
        json!({
            "type": "assistant",
            "timestamp": at(minutes_ago),
            "message": { "usage": {
                "input_tokens": 2,
                "cache_creation_input_tokens": 1000,
                "cache_read_input_tokens": cache_read,
                "output_tokens": 300,
            }}
        })
    }

    fn context(lines: &[Value]) -> Option<ContextUse> {
        let home = tempfile::tempdir().unwrap();
        let project = home.path().join(".claude/projects/-work-repo");
        fs::create_dir_all(&project).unwrap();
        let body: Vec<String> = lines.iter().map(Value::to_string).collect();
        fs::write(project.join(format!("{THREAD}.jsonl")), body.join("\n")).unwrap();
        ContextUse::read(home.path(), ThreadId::from_str(THREAD).unwrap())
    }

    #[test]
    fn reads_the_last_request_and_skips_lines_that_are_not_context() {
        let metadata = json!({ "type": "last-prompt", "lastPrompt": "hi" });
        let idle = context(&[assistant(56, 250_000), metadata]).unwrap();
        assert_eq!(idle.tokens(), 251_302);
        assert!(idle.idle_compaction_due(now()));

        // a later user line is activity even before a reply has usage
        let answered = context(&[
            assistant(56, 250_000),
            json!({ "type": "user", "timestamp": at(5), "message": { "content": "go" } }),
        ])
        .unwrap();
        assert!(!answered.idle_compaction_due(now()));

        // an API error line has zero usage and hides nothing
        let errored = context(&[assistant(56, 250_000), {
            let mut line = assistant(56, 0);
            line["message"]["usage"] = json!({ "input_tokens": 0, "output_tokens": 0 });
            line
        }])
        .unwrap();
        assert_eq!(errored.tokens(), 251_302);

        // a subagent's request is not the session's context
        let sidechain = context(&[assistant(56, 250_000), {
            let mut line = assistant(5, 10);
            line["isSidechain"] = json!(true);
            line
        }])
        .unwrap();
        assert!(sidechain.idle_compaction_due(now()));
    }

    #[test]
    fn idle_compaction_runs_between_55_minutes_and_the_cache_lapse() {
        let large = |minutes_ago| context(&[assistant(minutes_ago, 250_000)]).unwrap();

        assert!(!large(50).idle_compaction_due(now()));
        assert!(large(56).idle_compaction_due(now()));
        assert!(!large(61).idle_compaction_due(now()));
        let medium = context(&[assistant(56, 150_000)]).unwrap();
        assert!(!medium.idle_compaction_due(now()));

        // a message arriving mid-compaction queues instead of steering into it
        assert_eq!(large(56).turn_start(now()), TurnStart::AfterActive);
        assert_eq!(large(54).turn_start(now()), TurnStart::AfterActive);
        assert_eq!(large(50).turn_start(now()), TurnStart::Auto);
        // the compaction may still run once the window closed
        assert_eq!(large(90).turn_start(now()), TurnStart::AfterActive);
        assert_eq!(medium.turn_start(now()), TurnStart::Auto);
    }

    #[test]
    fn a_compaction_since_the_last_request_resets_the_size() {
        let compacted = context(&[
            assistant(200, 700_000),
            json!({
                "type": "system",
                "subtype": "compact_boundary",
                "timestamp": at(120),
                "compactMetadata": { "preTokens": 701_302, "postTokens": 14_131 }
            }),
        ])
        .unwrap();

        assert_eq!(compacted.tokens(), 14_131);
        assert_eq!(compacted.turn_start(now()), TurnStart::Auto);
    }
}
