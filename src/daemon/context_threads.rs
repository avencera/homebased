//! Context size of open T3 Claude and Codex threads, for the dashboard, a
//! lookup by thread id, and manual compaction
//!
//! A thread is listed when T3 Code owns its Claude session or Codex thread,
//! the T3 thread is neither archived nor deleted, and the session transcript
//! or Codex rollout shows at least [`MIN_IDLE_CONTEXT_TOKENS`] context tokens,
//! or the request asked for every thread, for its session, or for its id.
//! Compaction goes through T3, which holds messages that arrive during it,
//! and queues a manual compaction behind a running turn

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::AppState;
use super::compaction_log::{self, Trigger};
use crate::callback::stale_context::{CodexRollouts, ContextUse, MIN_IDLE_CONTEXT_TOKENS};
use crate::domain::{API_VERSION, ThreadId};
use crate::error::AppError;
use crate::t3::{
    CompactRequest, OpenThread, ProviderThread, T3Env, ThreadDetails, WakeOutcome, compact_thread,
    open_threads, request_compaction,
};
use crate::thread_ref::Provider;
use crate::thread_watch::CompactOutcome;

/// One open T3 thread and the size of its context
#[derive(Debug, Serialize)]
pub struct ContextThread {
    /// Agent that runs the thread
    pub provider: Provider,
    /// Claude Code session id or Codex thread id
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
    /// Context window of the model, which Codex records and Claude does not
    pub context_window: Option<u64>,
    /// Time of the last session activity
    pub last_active: DateTime<Utc>,
    /// Whether the prompt cache, an hour for both providers, is still alive,
    /// so compacting reads the context at the cached price
    pub cache_warm: bool,
    /// Project, branch, model, and other context T3 records for the thread
    pub details: ThreadDetails,
}

/// `GET /v1/threads` answer
#[derive(Debug, Serialize)]
pub struct ContextThreads {
    /// Wire version
    pub api_version: u32,
    /// Context size a thread needs to be listed
    pub min_tokens: u64,
    /// Listed threads, largest context first
    pub threads: Vec<ContextThread>,
}

/// `POST /v1/threads/{session}/compact` answer
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

/// Large open threads, and a lookup by thread id
pub fn read_routes() -> Router<AppState> {
    Router::new().route("/v1/threads", get(list))
}

/// Manual compaction, for the browser write guard
pub fn control_routes() -> Router<AppState> {
    Router::new().route("/v1/threads/{session}/compact", post(compact))
}

/// `GET /v1/threads` query
#[derive(Debug, Default, Deserialize)]
struct ListQuery {
    /// List every open thread, whatever its size
    #[serde(default)]
    all: bool,
    /// Comma-separated sessions to list below the size too, so the dashboard
    /// can show a thread's size after it compacts
    include: Option<String>,
    /// List only the threads whose T3 thread id, Claude session id, or Codex
    /// thread id starts with this, whatever their size
    thread: Option<String>,
}

/// Which open threads a listing returns
#[derive(Debug, Default)]
struct Listing {
    all: bool,
    include: Vec<ThreadId>,
    prefix: Option<String>,
}

impl Listing {
    /// Whether `thread` can be listed before its size is known
    fn considers(&self, thread: &OpenThread) -> bool {
        self.prefix.as_deref().is_none_or(|prefix| {
            thread.t3_thread.starts_with(prefix)
                || thread.session.id().to_string().starts_with(prefix)
        })
    }

    fn lists(&self, session: ThreadId, tokens: u64) -> bool {
        self.all
            || self.prefix.is_some()
            || tokens >= MIN_IDLE_CONTEXT_TOKENS
            || self.include.contains(&session)
    }
}

impl From<ListQuery> for Listing {
    fn from(query: ListQuery) -> Self {
        let ids = query.include.as_deref().unwrap_or_default().split(',');
        Self {
            all: query.all,
            // a stale or mistyped id names no thread, so it is skipped
            include: ids.filter_map(|id| id.trim().parse().ok()).collect(),
            prefix: query
                .thread
                .map(|prefix| prefix.trim().to_owned())
                .filter(|prefix| !prefix.is_empty()),
        }
    }
}

async fn list(Query(query): Query<ListQuery>) -> Result<Json<ContextThreads>, AppError> {
    let listing = Listing::from(query);
    let threads =
        run_blocking(move || context_threads(&T3Env::from_env(), Utc::now(), &listing)).await??;
    Ok(Json(ContextThreads {
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
        run_blocking(move || compact_session(&T3Env::from_env(), session, &log, Trigger::Manual))
            .await?;
    Ok(Json(CompactAnswer {
        api_version: API_VERSION,
        outcome,
        detail,
    }))
}

/// Open T3 threads whose context reached the listing size, or the others
/// that `listing` asks for
///
/// A thread whose transcript or rollout has no request yet is never listed
fn context_threads(
    env: &T3Env,
    now: DateTime<Utc>,
    listing: &Listing,
) -> Result<Vec<ContextThread>, AppError> {
    let open = open_threads(env).map_err(|message| AppError::Internal { message })?;
    let open: Vec<OpenThread> = open
        .into_iter()
        .filter(|thread| listing.considers(thread))
        .collect();
    // one walk of the Codex sessions finds every rollout, and none is needed without one
    let codex = open
        .iter()
        .any(|thread| matches!(thread.session, ProviderThread::Codex(_)));
    let rollouts = if codex {
        CodexRollouts::scan(&env.home)
    } else {
        CodexRollouts::default()
    };

    let mut threads: Vec<ContextThread> = open
        .into_iter()
        .filter_map(|thread| {
            let usage = match thread.session {
                ProviderThread::Claude(session) => ContextUse::read_claude(&env.home, session),
                ProviderThread::Codex(id) => rollouts.context(id),
            }?;
            let session = thread.session.id();
            listing
                .lists(session, usage.tokens())
                .then(|| ContextThread {
                    provider: thread.session.into(),
                    session,
                    t3_thread: thread.t3_thread,
                    title: thread.title,
                    settled: thread.settled,
                    tokens: usage.tokens(),
                    tokens_estimated: usage.tokens_estimated(),
                    context_window: usage.context_window(),
                    last_active: usage.last_active(),
                    cache_warm: usage.cache_warm(now),
                    details: thread.details,
                })
        })
        .collect();
    threads.sort_by_key(|thread| std::cmp::Reverse(thread.tokens));
    Ok(threads)
}

/// Ask T3 to compact `session`, once per idle period or turn, recording the
/// request in the compaction log at `log` under `trigger`
///
/// A manual request waits behind a running turn, so a thread can ask to
/// compact itself mid-turn. A watch compacts only now: it fired on an idle
/// reading, and a turn that started since then is new work, not the idle
/// period it fired for
pub(super) fn compact_session(
    env: &T3Env,
    session: ThreadId,
    log: &std::path::Path,
    trigger: Trigger,
) -> (CompactOutcome, Option<String>) {
    let Some((thread, usage)) = ContextUse::find_next(env, session) else {
        return (
            CompactOutcome::NotFound,
            Some("no Claude transcript or Codex rollout for the session T3 runs next".into()),
        );
    };

    // a repeated click in the same idle period or turn reuses the command id, which T3 drops
    let key = usage.compaction_key();
    let request = match trigger {
        Trigger::Manual => request_compaction(env, thread, &key),
        Trigger::Idle | Trigger::Watch | Trigger::Callback => CompactRequest {
            outcome: compact_thread(env, thread, &key),
            behind_run: None,
        },
    };
    let behind_run = request.behind_run.as_deref();
    compaction_log::record(
        log,
        trigger,
        thread,
        &usage,
        &request.outcome,
        behind_run,
        Utc::now(),
    );
    match request.outcome {
        WakeOutcome::Woken { .. } if behind_run.is_some() => (
            CompactOutcome::Queued,
            Some("T3 compacts the thread once its running turn ends".into()),
        ),
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

pub(super) async fn run_blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, AppError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| AppError::Internal {
            message: format!("thread context read join: {error}"),
        })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use chrono::{TimeDelta, Utc};
    use serde_json::json;
    use uuid::Uuid;

    use super::{Listing, Provider, compact_session, context_threads};
    use crate::callback::stale_context::codex_rollout::{rollout, token_count};
    use crate::daemon::compaction_log::Trigger;
    use crate::t3::T3Env;
    use crate::t3::test_support::{
        FakeT3Server, Fixture, SESSION_ID, T3_THREAD_ID, V2State, dispatched, live_pid, ticket,
    };
    use crate::thread_watch::CompactOutcome;

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

    /// Listed threads as (title, provider, tokens, warm), in listing order
    type Row = (String, Provider, u64, bool);

    #[test]
    fn lists_open_claude_and_codex_threads_with_a_large_context_largest_first() {
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
            ("codex", "codex", false, 240_000, 10),
            ("codex small", "codex", false, 20_000, 10),
            ("codex archived", "codex", true, 250_000, 10),
        ];
        for (name, driver, archived, tokens, minutes_ago) in threads {
            let session = Uuid::now_v7().to_string();
            state = state
                .thread(name, name, archived)
                .native(driver, &session, name);
            if driver == "codex" {
                let at = Utc::now() - TimeDelta::minutes(minutes_ago);
                rollout(
                    home.path(),
                    &session,
                    &[token_count(at, tokens - 1_000, 1_000)],
                );
            } else {
                transcript(home.path(), &session, minutes_ago, tokens);
            }
            sessions.push(session);
        }
        // a T3 thread whose Codex thread never ran has no rollout and is not listed
        let idle = Uuid::now_v7().to_string();
        state
            .thread("idle", "idle", false)
            .native("codex", &idle, "idle");

        let env = T3Env::new(home.path().to_path_buf(), "/bin".into());
        let listed = |listing: Listing| -> Vec<Row> {
            let threads = context_threads(&env, Utc::now(), &listing).unwrap();
            threads
                .into_iter()
                .map(|thread| {
                    (
                        thread.title,
                        thread.provider,
                        thread.tokens,
                        thread.cache_warm,
                    )
                })
                .collect()
        };
        let row = |title: &str, provider, tokens, warm| (title.to_owned(), provider, tokens, warm);
        let large = [
            row("cold", Provider::Claude, 500_002, false),
            row("warm", Provider::Claude, 300_002, true),
            row("codex", Provider::Codex, 240_000, true),
        ];

        assert_eq!(listed(Listing::default()), large);
        // a requested session is listed below the size, but never a closed thread
        let requested = Listing {
            include: [2, 3, 5]
                .map(|index| sessions[index].parse().unwrap())
                .into(),
            ..Listing::default()
        };
        let mut with_small = large.to_vec();
        with_small.extend([
            row("small", Provider::Claude, 50_002, true),
            row("codex small", Provider::Codex, 20_000, true),
        ]);
        assert_eq!(listed(requested), with_small);
        let all = Listing {
            all: true,
            ..Listing::default()
        };
        assert_eq!(listed(all), with_small);
    }

    #[test]
    fn looks_up_a_thread_by_a_prefix_of_its_t3_or_provider_id_at_any_size() {
        let home = tempfile::tempdir().unwrap();
        let userdata = home.path().join(".t3/userdata");
        fs::create_dir_all(&userdata).unwrap();
        let codex = "01a1b8ed-bad7-79b0-a505-6a4fa4605789";
        let claude = "0b9f1a5e-3a8e-4a57-9d55-0d3c1b8f6e21";
        V2State::create(&userdata.join("statev2.sqlite"))
            .thread("mcp:launch:8bd44cf5", "Codex review", false)
            .native("codex", codex, "mcp:launch:8bd44cf5")
            .thread("mcp:launch:8bd9e1aa", "Claude plan", false)
            .native("claudeAgent", claude, "mcp:launch:8bd9e1aa");
        rollout(home.path(), codex, &[token_count(Utc::now(), 12_000, 345)]);
        transcript(home.path(), claude, 5, 9_000);

        let env = T3Env::new(home.path().to_path_buf(), "/bin".into());
        let lookup = |prefix: &str| -> Vec<(String, u64, Option<u64>)> {
            let listing = Listing {
                prefix: Some(prefix.to_owned()),
                ..Listing::default()
            };
            let threads = context_threads(&env, Utc::now(), &listing).unwrap();
            threads
                .into_iter()
                .map(|thread| (thread.t3_thread, thread.tokens, thread.context_window))
                .collect()
        };

        let codex_row = ("mcp:launch:8bd44cf5".to_owned(), 12_345, Some(258_400));
        assert_eq!(lookup("mcp:launch:8bd4"), std::slice::from_ref(&codex_row));
        assert_eq!(lookup("01a1b8ed"), [codex_row]);
        assert_eq!(
            lookup("mcp:launch:8bd9e1aa"),
            [("mcp:launch:8bd9e1aa".to_owned(), 9_002, None)]
        );
        // an ambiguous prefix lists every match, for the caller to narrow
        assert_eq!(lookup("mcp:launch:8bd").len(), 2);
        assert!(lookup("nothing").is_empty());
    }

    #[test]
    fn a_manual_compaction_queues_behind_a_running_turn_once_per_turn() {
        let server = FakeT3Server::start(
            Some(2),
            (0..4).flat_map(|_| [ticket(), dispatched()]).collect(),
        );
        let fixture = Fixture::empty(Some(&server), live_pid());
        fixture
            .v2_state()
            .thread(T3_THREAD_ID, "Busy thread", false)
            .native("claudeAgent", SESSION_ID, T3_THREAD_ID)
            .run(T3_THREAD_ID, 1, "running", "long build");
        transcript(fixture.dir.path(), SESSION_ID, 2, 180_000);
        let log = fixture.dir.path().join("compactions.jsonl");
        let session = SESSION_ID.parse().unwrap();
        let compact = |trigger| compact_session(&fixture.env, session, &log, trigger).0;

        // a thread asking to compact itself mid-turn, twice
        assert_eq!(compact(Trigger::Manual), CompactOutcome::Queued);
        assert_eq!(compact(Trigger::Manual), CompactOutcome::Queued);
        // a watch fired on an idle reading, so it does not wait for the turn
        assert_eq!(compact(Trigger::Watch), CompactOutcome::Started);
        rusqlite::Connection::open(fixture.userdata().join("statev2.sqlite"))
            .unwrap()
            .execute(
                "UPDATE orchestration_v2_projection_runs SET status = 'completed'",
                [],
            )
            .unwrap();
        assert_eq!(compact(Trigger::Manual), CompactOutcome::Started);

        let payloads: Vec<serde_json::Value> = server
            .requests()
            .iter()
            .filter(|request| request.method == "WS")
            .map(|request| {
                serde_json::from_str::<serde_json::Value>(&request.body).unwrap()["payload"].clone()
            })
            .collect();
        assert_eq!(payloads.len(), 4, "{payloads:?}");
        assert!(payloads.iter().all(|payload| payload["text"] == "/compact"));
        let queued = json!({ "type": "queue_after_active" });
        // without `deliveryIntent`, T3 queues instead of refusing a steer
        for manual in [&payloads[0], &payloads[1], &payloads[3]] {
            assert_eq!(manual["dispatchMode"], queued);
            assert!(manual.get("deliveryIntent").is_none(), "{manual}");
        }
        assert_eq!(payloads[2]["deliveryIntent"], "auto");
        // one turn queues one compaction, which T3 replays for the same command id
        assert_eq!(payloads[0]["commandId"], payloads[1]["commandId"]);
        assert_ne!(payloads[1]["commandId"], payloads[3]["commandId"]);

        let logged: Vec<serde_json::Value> = fs::read_to_string(&log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let outcomes: Vec<(&str, &str)> = logged
            .iter()
            .map(|line| {
                (
                    line["trigger"].as_str().unwrap(),
                    line["outcome"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            outcomes,
            [
                ("manual", "queued"),
                ("manual", "queued"),
                ("watch", "started"),
                ("manual", "started"),
            ]
        );
        let run = format!("run-{T3_THREAD_ID}-1");
        assert_eq!(logged[0]["behind_run"], run.as_str());
        assert!(logged[3].get("behind_run").is_none());
    }
}
