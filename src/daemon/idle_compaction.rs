//! Compact idle Claude sessions that wait on a task before their prompt cache
//! lapses
//!
//! A thread that waits on a task callback is checked every minute, so a
//! compaction starts inside the window that
//! [`ContextUse::idle_compaction_due`] allows. The transcript records any
//! activity since, so a thread that moved on is left alone

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use super::compaction_log::{self, Trigger};
use crate::callback::stale_context::ContextUse;
use crate::domain::ThreadId;
use crate::submission::CallbackContext;
use crate::t3::{T3Env, WakeOutcome, compact_thread};

const SCAN_INTERVAL: Duration = Duration::from_secs(60);

/// Idle period of each thread that already had a compaction attempt
pub(crate) type Attempts = HashMap<ThreadId, DateTime<Utc>>;

/// Check waiting threads for as long as the daemon runs
pub(super) async fn run(state: AppState) {
    let mut attempts = Attempts::new();
    let log = state.home.compaction_log_path();
    let mut tick = tokio::time::interval(SCAN_INTERVAL);
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
        let Some(usage) = ContextUse::read(Path::new(&context.env.home), thread) else {
            continue;
        };
        if !usage.idle_compaction_due(now) || attempts.get(&thread) == Some(&usage.last_active()) {
            continue;
        }

        attempts.insert(thread, usage.last_active());
        let env = T3Env::new(
            PathBuf::from(&context.env.home),
            context.env.path.clone().into(),
        );
        let outcome = compact_thread(&env, thread, &usage.compaction_key());
        compaction_log::record(log, Trigger::Idle, thread, &usage, &outcome, now);
        match outcome {
            WakeOutcome::Woken { t3_thread } => info!(
                %thread,
                t3_thread,
                tokens = usage.tokens(),
                "compacting an idle Claude session before its prompt cache lapses"
            ),
            // a terminal session compacts itself, and the socket cannot run `/compact`
            WakeOutcome::NotT3Thread => debug!(%thread, "idle session has no T3 thread"),
            outcome => warn!(%thread, "idle compaction did not start: {outcome:?}"),
        }
    }
    attempts
}
