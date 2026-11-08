//! Context size and last activity of a Codex thread, from its rollout
//!
//! Codex writes each thread to
//! `~/.codex/sessions/YYYY/MM/DD/rollout-<local time>-<thread id>.jsonl`. After
//! every model request it appends an `event_msg` whose payload is a
//! `token_count`, with `info.last_token_usage` of that request and the model's
//! `info.model_context_window`. Its `total_tokens`, input plus output, is the
//! context size Codex shows itself. Right after a compaction Codex appends one
//! whose counts are all zero except `total_tokens`, its estimate of the
//! compacted history, until the next request reports the real size. A
//! `token_count` without `info` carries only rate limits
//!
//! Activity is a line a turn writes while it works: a request, an item, or a
//! message. A compaction turn writes `task_started` and `turn_context` as it
//! starts and its first request only once it finishes, and resuming a thread
//! can write `thread_settings_applied` without a turn, so none of those count.
//! A compaction in progress then still reads as the idle period it compacts,
//! as a Claude transcript does
//!
//! T3 Code runs Codex with the default home, `~/.codex`, unless a provider
//! instance sets `homePath`, which this does not follow

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

use super::{ContextUse, last_in_tail};
use crate::domain::ThreadId;

/// Rollout files under one Codex sessions directory, by thread id
///
/// Codex names the day directory by local time, so one walk of the whole tree
/// finds a file without guessing its day. A few thousand files take under a
/// millisecond
#[derive(Debug, Default)]
pub(crate) struct CodexRollouts(HashMap<ThreadId, PathBuf>);

impl CodexRollouts {
    /// Index the rollouts of the Codex home under `home`
    pub(crate) fn scan(home: &Path) -> Self {
        Self::in_sessions(&home.join(".codex/sessions"))
    }

    /// Index the rollouts under the Codex `sessions` directory
    pub(crate) fn in_sessions(sessions: &Path) -> Self {
        let mut rollouts = HashMap::new();
        let mut pending = vec![sessions.to_path_buf()];
        while let Some(directory) = pending.pop() {
            let entries = match fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) => {
                    // a missing Codex home has no rollouts, which is not worth a line
                    if error.kind() != std::io::ErrorKind::NotFound {
                        tracing::debug!("read {}: {error}", directory.display());
                    }
                    continue;
                }
            };
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                // a symlinked day directory is followed
                if path.is_dir() {
                    pending.push(path);
                } else if let Some(id) = rollout_id(&path) {
                    rollouts.insert(ThreadId(id), path);
                }
            }
        }
        Self(rollouts)
    }

    /// Thread ids that have a rollout
    pub(crate) fn ids(&self) -> impl Iterator<Item = ThreadId> + '_ {
        self.0.keys().copied()
    }

    /// Context of the Codex thread `thread`
    ///
    /// `None` when it has no rollout or the rollout's tail has no request
    pub(crate) fn context(&self, thread: ThreadId) -> Option<ContextUse> {
        last_in_tail(self.0.get(&thread)?, from_tail)
    }
}

/// Thread id in a `rollout-<timestamp>-<thread id>.jsonl` file name
fn rollout_id(path: &Path) -> Option<Uuid> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_prefix("rollout-")?.strip_suffix(".jsonl")?;
    let start = stem.len().checked_sub(36)?;
    Uuid::parse_str(stem.get(start..)?).ok()
}

fn from_tail(tail: &[u8]) -> Option<ContextUse> {
    let mut last_active = None;
    for raw in tail.rsplit(|byte| *byte == b'\n') {
        // the first line of the tail may be cut off and does not parse
        let Ok(line) = serde_json::from_slice::<Line>(raw) else {
            continue;
        };
        let Some(at) = line.timestamp.filter(|_| line.is_activity()) else {
            continue;
        };
        let last_active = *last_active.get_or_insert(at);
        let Some(info) = line.payload.and_then(|payload| payload.info) else {
            continue;
        };
        let usage = info.last_token_usage;
        // a zero reading says nothing of the context
        if usage.total_tokens == 0 {
            continue;
        }
        return Some(ContextUse {
            tokens: usage.total_tokens,
            estimated: usage.input_tokens == 0 && usage.output_tokens == 0,
            window: info.model_context_window,
            last_active,
        });
    }
    None
}

/// Rollout line fields this module reads
#[derive(Deserialize)]
struct Line {
    #[serde(rename = "type")]
    kind: Option<String>,
    timestamp: Option<DateTime<Utc>>,
    payload: Option<Payload>,
}

impl Line {
    fn is_activity(&self) -> bool {
        let event = self
            .payload
            .as_ref()
            .and_then(|payload| payload.kind.as_deref());
        match self.kind.as_deref() {
            Some("response_item" | "compacted") => true,
            Some("event_msg") => matches!(
                event,
                Some(
                    "token_count"
                        | "user_message"
                        | "agent_message"
                        | "task_complete"
                        | "turn_aborted"
                )
            ),
            _ => false,
        }
    }
}

#[derive(Deserialize)]
struct Payload {
    #[serde(rename = "type")]
    kind: Option<String>,
    // only a `token_count` event has it
    info: Option<Info>,
}

#[derive(Deserialize)]
struct Info {
    last_token_usage: Usage,
    model_context_window: Option<u64>,
}

#[derive(Deserialize)]
struct Usage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    total_tokens: u64,
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::fs;
    use std::path::Path;

    use chrono::{DateTime, Utc};
    use serde_json::{Value, json};

    /// Write the rollout of Codex thread `thread` under `home` with `lines`
    pub(crate) fn rollout(home: &Path, thread: &str, lines: &[Value]) {
        let day = home.join(".codex/sessions/2026/10/09");
        fs::create_dir_all(&day).unwrap();
        let body: Vec<String> = lines.iter().map(Value::to_string).collect();
        let name = format!("rollout-2026-10-09T07-00-00-{thread}.jsonl");
        fs::write(day.join(name), body.join("\n")).unwrap();
    }

    /// `token_count` event of a request that read `input` and wrote `output`
    pub(crate) fn token_count(at: DateTime<Utc>, input: u64, output: u64) -> Value {
        json!({
            "timestamp": at.to_rfc3339(),
            "type": "event_msg",
            "payload": { "type": "token_count", "info": {
                "total_token_usage": { "input_tokens": 9_000_000, "total_tokens": 9_100_000 },
                "last_token_usage": {
                    "input_tokens": input,
                    "cached_input_tokens": input / 2,
                    "output_tokens": output,
                    "total_tokens": input + output,
                },
                "model_context_window": 258_400,
            }}
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use serde_json::{Value, json};

    use super::test_support::{rollout, token_count};
    use super::*;
    use crate::t3::TurnStart;

    const THREAD: &str = "01a1b66a-67fb-7641-994a-c2b3d199eb4b";

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-09T12:00:00Z")
            .unwrap()
            .to_utc()
    }

    fn at(minutes_ago: i64) -> DateTime<Utc> {
        now() - TimeDelta::minutes(minutes_ago)
    }

    fn event(minutes_ago: i64, kind: &str) -> Value {
        json!({
            "timestamp": at(minutes_ago).to_rfc3339(),
            "type": "event_msg",
            "payload": { "type": kind }
        })
    }

    fn context(lines: &[Value]) -> Option<ContextUse> {
        let home = tempfile::tempdir().unwrap();
        rollout(home.path(), THREAD, lines);
        CodexRollouts::scan(home.path()).context(THREAD.parse().unwrap())
    }

    #[test]
    fn reads_the_last_request_with_the_window_and_the_last_activity() {
        let meta = json!({
            "timestamp": at(200).to_rfc3339(),
            "type": "session_meta",
            "payload": { "id": THREAD, "base_instructions": { "text": "x".repeat(40_000) } }
        });
        let found = context(&[
            meta,
            token_count(at(90), 120_000, 1_000),
            token_count(at(57), 240_000, 2_000),
            event(56, "task_complete"),
            // a rate limit update has no usage
            json!({
                "timestamp": at(56).to_rfc3339(),
                "type": "event_msg",
                "payload": { "type": "token_count", "info": null, "rate_limits": {} }
            }),
            // a resume without a turn is not activity
            event(1, "thread_settings_applied"),
        ])
        .unwrap();

        assert_eq!(found.tokens(), 242_000);
        assert!(!found.tokens_estimated());
        assert_eq!(found.context_window(), Some(258_400));
        assert_eq!(found.last_active(), at(56));
        assert!(found.idle_compaction_due(now()));
    }

    #[test]
    fn a_compaction_reads_as_the_estimate_until_the_next_request() {
        let compacted = json!({
            "timestamp": at(30).to_rfc3339(),
            "type": "compacted",
            "payload": { "message": "", "replacement_history": [] }
        });
        let estimate = {
            let mut line = token_count(at(30), 0, 0);
            line["payload"]["info"]["last_token_usage"]["total_tokens"] = json!(24_740);
            line
        };
        let before = [token_count(at(70), 251_007, 5_683)];

        // a compaction turn that started is still the idle period it compacts
        let running = context(&[before[0].clone(), event(2, "task_started"), {
            let mut line = event(2, "turn_context");
            line["type"] = json!("turn_context");
            line
        }])
        .unwrap();
        assert_eq!(running.tokens(), 256_690);
        assert!(running.cold_compaction_due(now()));
        assert_eq!(running.turn_start(now()), TurnStart::AfterActive);

        let landed = context(&[before[0].clone(), compacted.clone(), estimate.clone()]).unwrap();
        assert_eq!(landed.tokens(), 24_740);
        assert!(landed.tokens_estimated());
        assert!(!landed.cold_compaction_due(now()));

        let measured = context(&[
            before[0].clone(),
            compacted,
            estimate,
            token_count(at(29), 21_532, 213),
        ])
        .unwrap();
        assert_eq!(measured.tokens(), 21_745);
        assert!(!measured.tokens_estimated());
    }

    #[test]
    fn indexes_rollouts_by_thread_id_in_any_day_directory() {
        let home = tempfile::tempdir().unwrap();
        rollout(home.path(), THREAD, &[token_count(at(5), 10_000, 100)]);
        let other = home.path().join(".codex/sessions/2026/11/07");
        fs::create_dir_all(&other).unwrap();
        let second = "01a1b8ed-bad7-79b0-a505-6a4fa4605789";
        fs::write(
            other.join(format!("rollout-2026-11-07T20-12-30-{second}.jsonl")),
            token_count(at(1), 30_000, 300).to_string(),
        )
        .unwrap();
        fs::write(other.join("notes.jsonl"), "").unwrap();

        let rollouts = CodexRollouts::scan(home.path());
        let mut ids: Vec<String> = rollouts.ids().map(|id| id.to_string()).collect();
        ids.sort();
        assert_eq!(ids, [THREAD, second]);
        let second = rollouts.context(second.parse().unwrap()).unwrap();
        assert_eq!(second.tokens(), 30_300);
        // no Codex home is no rollout, not an error
        assert!(
            CodexRollouts::scan(&home.path().join("missing"))
                .ids()
                .next()
                .is_none()
        );
    }
}
