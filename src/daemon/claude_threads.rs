//! Open Claude threads with a large context, for manual compaction from the
//! dashboard
//!
//! A thread is listed when T3 Code owns its Claude session, the T3 thread is
//! neither archived nor deleted, and the session transcript shows at least
//! [`MIN_IDLE_CONTEXT_TOKENS`] context tokens, or the dashboard asked for every
//! thread or for its session, to follow its size after a compaction.
//! Compaction goes through T3, which holds messages that arrive during it

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::AppState;
use super::compaction_log::{self, Trigger};
use crate::callback::stale_context::{ContextUse, MIN_IDLE_CONTEXT_TOKENS};
use crate::domain::{API_VERSION, ThreadId};
use crate::error::AppError;
use crate::t3::{T3Env, ThreadDetails, WakeOutcome, compact_thread, open_claude_threads};

/// One open Claude thread with a large context
#[derive(Debug, Serialize)]
pub struct LargeClaudeThread {
    /// Claude Code session id
    pub session: ThreadId,
    /// T3 thread id
    pub t3_thread: String,
    /// T3 thread title
    pub title: String,
    /// Whether the thread is settled in T3's sidebar
    pub settled: bool,
    /// Context tokens of the last request or compaction
    pub tokens: u64,
    /// Whether `tokens` is an estimate after a compaction, until the next
    /// request reports the real size
    pub tokens_estimated: bool,
    /// Time of the last session activity
    pub last_active: DateTime<Utc>,
    /// Whether the one-hour prompt cache is still alive, so compacting reads
    /// the context at the cached price
    pub cache_warm: bool,
    /// Project, branch, model, and other context T3 records for the thread
    pub details: ThreadDetails,
}

/// `GET /v1/claude/threads` answer
#[derive(Debug, Serialize)]
pub struct LargeClaudeThreads {
    /// Wire version
    pub api_version: u32,
    /// Context size a thread needs to be listed
    pub min_tokens: u64,
    /// Listed threads, largest context first
    pub threads: Vec<LargeClaudeThread>,
}

/// Result of a compaction request
#[derive(Debug, Serialize)]
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

/// `POST /v1/claude/threads/{session}/compact` answer
#[derive(Debug, Serialize)]
pub struct CompactAnswer {
    /// Wire version
    pub api_version: u32,
    /// What happened
    pub outcome: CompactOutcome,
    /// Why, when T3 did not start the compaction
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Dashboard list of large open threads
pub fn read_routes() -> Router<AppState> {
    Router::new().route("/v1/claude/threads", get(list))
}

/// Manual compaction, for the browser write guard
pub fn control_routes() -> Router<AppState> {
    Router::new().route("/v1/claude/threads/{session}/compact", post(compact))
}

/// `GET /v1/claude/threads` query
#[derive(Debug, Default, Deserialize)]
struct ListQuery {
    /// List every open thread, whatever its size
    #[serde(default)]
    all: bool,
    /// Comma-separated sessions to list below the size too, so the dashboard
    /// can show a thread's size after it compacts
    include: Option<String>,
}

/// Which open threads a listing returns
#[derive(Debug, Default)]
struct Listing {
    all: bool,
    include: Vec<ThreadId>,
}

impl Listing {
    fn lists(&self, session: ThreadId, tokens: u64) -> bool {
        self.all || tokens >= MIN_IDLE_CONTEXT_TOKENS || self.include.contains(&session)
    }
}

impl From<ListQuery> for Listing {
    fn from(query: ListQuery) -> Self {
        let ids = query.include.as_deref().unwrap_or_default().split(',');
        Self {
            all: query.all,
            // a stale or mistyped id names no thread, so it is skipped
            include: ids.filter_map(|id| id.trim().parse().ok()).collect(),
        }
    }
}

async fn list(Query(query): Query<ListQuery>) -> Result<Json<LargeClaudeThreads>, AppError> {
    let listing = Listing::from(query);
    let threads =
        run_blocking(move || large_threads(&T3Env::from_env(), Utc::now(), &listing)).await??;
    Ok(Json(LargeClaudeThreads {
        api_version: API_VERSION,
        min_tokens: MIN_IDLE_CONTEXT_TOKENS,
        threads,
    }))
}

async fn compact(
    State(state): State<AppState>,
    Path(session): Path<ThreadId>,
) -> Result<Json<CompactAnswer>, AppError> {
    let log = state.home.compaction_log_path();
    let (outcome, detail) =
        run_blocking(move || compact_session(&T3Env::from_env(), session, &log)).await?;
    Ok(Json(CompactAnswer {
        api_version: API_VERSION,
        outcome,
        detail,
    }))
}

/// Open T3 Claude threads whose context reached the listing size, or the
/// others that `listing` asks for
fn large_threads(
    env: &T3Env,
    now: DateTime<Utc>,
    listing: &Listing,
) -> Result<Vec<LargeClaudeThread>, AppError> {
    let open = open_claude_threads(env).map_err(|message| AppError::Internal { message })?;
    let mut threads: Vec<LargeClaudeThread> = open
        .into_iter()
        .filter_map(|thread| {
            let usage = ContextUse::read(&env.home, thread.session)?;
            listing
                .lists(thread.session, usage.tokens())
                .then(|| LargeClaudeThread {
                    session: thread.session,
                    t3_thread: thread.t3_thread,
                    title: thread.title,
                    settled: thread.settled,
                    tokens: usage.tokens(),
                    tokens_estimated: usage.tokens_estimated(),
                    last_active: usage.last_active(),
                    cache_warm: usage.cache_warm(now),
                    details: thread.details,
                })
        })
        .collect();
    threads.sort_by_key(|thread| std::cmp::Reverse(thread.tokens));
    Ok(threads)
}

/// Ask T3 to compact `session`, once per idle period, recording the request
/// in the compaction log at `log`
fn compact_session(
    env: &T3Env,
    session: ThreadId,
    log: &std::path::Path,
) -> (CompactOutcome, Option<String>) {
    let Some(usage) = ContextUse::read(&env.home, session) else {
        return (
            CompactOutcome::NotFound,
            Some("no Claude transcript for this session".into()),
        );
    };

    // a repeated click in the same idle period reuses the command id, which T3 drops
    let outcome = compact_thread(env, session, &usage.compaction_key());
    compaction_log::record(log, Trigger::Manual, session, &usage, &outcome, Utc::now());
    match outcome {
        WakeOutcome::Woken { .. } => (CompactOutcome::Started, None),
        WakeOutcome::NotT3Thread => (
            CompactOutcome::NotFound,
            Some("no T3 thread owns this session".into()),
        ),
        WakeOutcome::Refused(detail) | WakeOutcome::ApiChanged(detail) => {
            (CompactOutcome::Refused, Some(detail))
        }
        WakeOutcome::Unavailable(detail) | WakeOutcome::Uncertain(detail) => {
            (CompactOutcome::Unavailable, Some(detail))
        }
    }
}

async fn run_blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, AppError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| AppError::Internal {
            message: format!("Claude thread read join: {error}"),
        })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use chrono::{TimeDelta, Utc};
    use serde_json::json;
    use uuid::Uuid;

    use super::{Listing, large_threads};
    use crate::t3::T3Env;
    use crate::t3::test_support::V2State;

    /// Claude transcript whose last request carried `tokens` `minutes_ago`
    fn transcript(home: &Path, session: &str, minutes_ago: i64, tokens: u64) {
        let project = home.join(".claude/projects/-work");
        fs::create_dir_all(&project).unwrap();
        let line = json!({
            "type": "assistant",
            "timestamp": (Utc::now() - TimeDelta::minutes(minutes_ago)).to_rfc3339(),
            "message": { "usage": { "input_tokens": 2, "cache_read_input_tokens": tokens } }
        });
        fs::write(project.join(format!("{session}.jsonl")), line.to_string()).unwrap();
    }

    #[test]
    fn lists_open_t3_threads_with_a_large_context_largest_first() {
        let home = tempfile::tempdir().unwrap();
        let userdata = home.path().join(".t3/userdata");
        fs::create_dir_all(&userdata).unwrap();
        let mut state = V2State::create(&userdata.join("statev2.sqlite"));
        let mut sessions = Vec::new();
        let threads = [
            ("warm", "claudeAgent", false, 300_000, 10),
            ("cold", "claudeAgent", false, 500_000, 120),
            ("small", "claudeAgent", false, 50_000, 10),
            ("archived", "claudeAgent", true, 400_000, 10),
            ("codex", "codex", false, 400_000, 10),
        ];
        for (name, driver, archived, tokens, minutes_ago) in threads {
            let session = Uuid::now_v7().to_string();
            state = state
                .thread(name, name, archived)
                .native(driver, &session, name);
            transcript(home.path(), &session, minutes_ago, tokens);
            sessions.push(session);
        }
        let (warm, cold, small) = (
            sessions[0].clone(),
            sessions[1].clone(),
            sessions[2].clone(),
        );
        let env = T3Env::new(home.path().to_path_buf(), "/bin".into());
        let listed = |listing: Listing| -> Vec<(String, String, bool)> {
            let threads = large_threads(&env, Utc::now(), &listing).unwrap();
            threads
                .into_iter()
                .map(|thread| (thread.session.to_string(), thread.title, thread.cache_warm))
                .collect()
        };

        assert_eq!(
            listed(Listing::default()),
            [
                (cold.clone(), "cold".to_owned(), false),
                (warm.clone(), "warm".to_owned(), true)
            ]
        );
        // a requested session is listed below the size, but never a closed thread
        let include = vec![sessions[2].parse().unwrap(), sessions[3].parse().unwrap()];
        let with_small = [
            (cold, "cold".to_owned(), false),
            (warm, "warm".to_owned(), true),
            (small, "small".to_owned(), true),
        ];
        let requested = Listing {
            all: false,
            include,
        };
        assert_eq!(listed(requested), with_small);
        let all = Listing {
            all: true,
            include: Vec::new(),
        };
        assert_eq!(listed(all), with_small);
    }
}
