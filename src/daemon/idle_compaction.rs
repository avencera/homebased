//! Compact idle T3 threads before their prompt cache lapses
//!
//! Every minute the daemon reads this machine's T3 threads and compacts those
//! that [`LocalIndex::idle_compaction_due`] names: threads that run no turn,
//! will likely run again, and whose transcript or rollout shows them idle
//! inside the window [`ContextUse::idle_compaction_due`] allows. That covers a
//! thread waiting on a homebased task or job, a root waiting on delegated
//! tasks, and an interactive thread waiting on the user alike. A terminal
//! session has no T3 thread to send `/compact` to, so it compacts itself
//!
//! The same loop then checks every `homebased thread watch`

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use super::AppState;
use super::compaction_log::{self, Trigger};
use super::thread_api::local::LocalIndex;
use super::thread_api::local_sources;
use super::thread_api::watch_scan::{self, WatchMemory};
use crate::callback::stale_context::ContextUse;
use crate::domain::ThreadId;
use crate::t3::{WakeOutcome, compact_thread};

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

/// Context of each session at its last compaction attempt, whose last
/// activity names the idle period it was for
pub(crate) type Attempts = HashMap<ThreadId, ContextUse>;

/// Check idle T3 threads for as long as the daemon runs
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
        match local_sources(&state).await {
            Ok(sources) => {
                let log = log.clone();
                let scanned = tokio::task::spawn_blocking(move || {
                    match LocalIndex::load(sources, Utc::now()) {
                        Ok(index) => scan(&index, attempts, &log),
                        Err(error) => {
                            warn!("idle compaction scan cannot read T3 threads: {error}");
                            attempts
                        }
                    }
                })
                .await;
                attempts = scanned.unwrap_or_else(|error| {
                    warn!("idle compaction scan join: {error}");
                    Attempts::new()
                });
            }
            Err(error) => warn!("idle compaction scan: {error}"),
        }
        // after the idle compactions, so a watch reads a size they already changed
        watch_scan::scan(&state, &mut watches, Utc::now()).await;
    }
}

/// Start one compaction per idle period of each thread `index` finds due,
/// recording each request in the compaction log at `log`
///
/// T3 also drops a repeated request for the same period, so a lost attempt
/// record costs only a token round trip
pub(crate) fn scan(index: &LocalIndex, mut attempts: Attempts, log: &Path) -> Attempts {
    let now = index.now();
    // an idle period whose cache lapsed is never due again
    attempts.retain(|_, tried| tried.cache_warm(now));
    for (session, usage) in index.idle_compaction_due() {
        let thread = session.id();
        let tried = attempts.get(&thread).map(ContextUse::last_active);
        if tried == Some(usage.last_active()) {
            continue;
        }

        attempts.insert(thread, usage);
        let outcome = compact_thread(index.env(), session, &usage.compaction_key());
        compaction_log::record(log, Trigger::Idle, session, &usage, &outcome, None, now);
        match outcome {
            WakeOutcome::Woken { t3_thread } => info!(
                %thread,
                t3_thread,
                provider = session.provider(),
                tokens = usage.tokens(),
                "compacting an idle thread before its prompt cache lapses"
            ),
            outcome => warn!(%thread, "idle compaction did not start: {outcome:?}"),
        }
    }
    attempts
}
