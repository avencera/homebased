//! Compact idle Claude sessions and Codex threads that wait on a task before
//! their prompt cache lapses
//!
//! A thread that waits on a task callback is checked every minute, so a
//! compaction starts inside the window that
//! [`ContextUse::idle_compaction_due`] allows. The transcript or rollout
//! records any activity since, so a thread that moved on is left alone
//!
//! The same loop then checks every `homebased thread watch`

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use super::compaction_log::{self, Trigger};
use super::thread_api::watch_scan::{self, WatchMemory};
use crate::callback::stale_context::ContextUse;
use crate::domain::ThreadId;
use crate::submission::CallbackContext;
use crate::t3::{T3Env, WakeOutcome, compact_thread};

const SCAN_INTERVAL: Duration = Duration::from_secs(60);

/// Seconds between scans instead of [`SCAN_INTERVAL`], so integration tests
/// see several watch scans without waiting minutes
const SCAN_SECS_ENV: &str = "HOMEBASED_TEST_SCAN_SECS";

/// Seconds a finished thread must stay quiet instead of
/// [`watch_scan::QUIET_FINISHED`], for the same tests
const WATCH_QUIET_SECS_ENV: &str = "HOMEBASED_TEST_WATCH_QUIET_SECS";

/// `name` as a whole number of seconds, when the daemon's environment sets it
fn seconds_from_env(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse().ok()
}

/// Idle period of each thread that already had a compaction attempt
pub(crate) type Attempts = HashMap<ThreadId, DateTime<Utc>>;

/// Check waiting threads for as long as the daemon runs
pub(super) async fn run(state: AppState) {
    let mut attempts = Attempts::new();
    let interval = seconds_from_env(SCAN_SECS_ENV)
        .filter(|seconds| *seconds > 0)
        .map_or(SCAN_INTERVAL, Duration::from_secs);
    let quiet = seconds_from_env(WATCH_QUIET_SECS_ENV)
        .and_then(|seconds| i64::try_from(seconds).ok())
        .map_or(watch_scan::QUIET_FINISHED, TimeDelta::seconds);
    let mut watches = WatchMemory::new(quiet);
    let log = state.home.compaction_log_path();
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let threads = match call(&state.store, |reply| StoreMsg::WaitingThreads { reply }).await {
            Ok(threads) => threads,
            Err(error) => {
                warn!("idle compaction scan: {error}");
                continue;
            }
        };
        let log = log.clone();
        attempts =
            match tokio::task::spawn_blocking(move || scan(threads, attempts, Utc::now(), &log))
                .await
            {
                Ok(attempts) => attempts,
                Err(error) => {
                    warn!("idle compaction scan join: {error}");
                    Attempts::new()
                }
            };
        // after the idle compactions, so a watch reads a size they already changed
        watch_scan::scan(&state, &mut watches, Utc::now()).await;
    }
}

/// Start one compaction per idle period of each due thread, recording each
/// request in the compaction log at `log`
///
/// T3 also drops a repeated request for the same period, so a lost attempt
/// record costs only a token round trip
pub(crate) fn scan(
    threads: Vec<(ThreadId, CallbackContext)>,
    mut attempts: Attempts,
    now: DateTime<Utc>,
    log: &Path,
) -> Attempts {
    attempts.retain(|thread, _| threads.iter().any(|(waiting, _)| waiting == thread));
    for (thread, context) in threads {
        let env = T3Env::new(
            PathBuf::from(&context.env.home),
            context.env.path.clone().into(),
        );
        // a waiting thread is only an id, so its provider is whichever record names it
        let Some((provider_thread, usage)) = ContextUse::find_next(&env, thread) else {
            continue;
        };
        if !usage.idle_compaction_due(now) || attempts.get(&thread) == Some(&usage.last_active()) {
            continue;
        }

        attempts.insert(thread, usage.last_active());
        let outcome = compact_thread(&env, provider_thread, &usage.compaction_key());
        compaction_log::record(log, Trigger::Idle, provider_thread, &usage, &outcome, now);
        match outcome {
            WakeOutcome::Woken { t3_thread } => info!(
                %thread,
                t3_thread,
                provider = provider_thread.provider(),
                tokens = usage.tokens(),
                "compacting an idle thread before its prompt cache lapses"
            ),
            // a terminal session compacts itself, and the socket cannot run `/compact`
            WakeOutcome::NotT3Thread => debug!(%thread, "idle session has no T3 thread"),
            outcome => warn!(%thread, "idle compaction did not start: {outcome:?}"),
        }
    }
    attempts
}
