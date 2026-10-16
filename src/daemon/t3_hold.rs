//! Keep T3 threads in the Working shelf while their held tasks run
//!
//! A task submitted with `--hold` keeps the Codex thread that submitted it
//! working in T3 until the task and its waiting chain are done and every event
//! has reached the thread. T3 shows a thread with an active run as working, so
//! an idle thread gets a placeholder run from [`crate::t3`]'s `hold_thread`
//!
//! Every tick reads the pending holds from the store and each thread's runs
//! from T3's state database, and then
//! - holds a thread that stayed idle for [`IDLE_TICKS_TO_HOLD`] ticks, so a
//!   thread between two turns, or one whose event turn is about to start, is
//!   not held. A thread with another run, an interrupted placeholder (the
//!   user pressed Stop), or an archived thread is left alone
//! - releases a hold that has another run queued behind it, which is a
//!   message the user typed, and a hold whose tasks are all done
//!
//! The daemon keeps no record of which placeholder it created. A restart reads
//! the threads of every held task once, and the first tick releases the
//! placeholders that no pending task needs. Any doubt about T3 leaves the
//! thread as it is, and delivery of events never waits on this loop

use std::collections::HashMap;
use std::fmt::Write as _;
use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use crate::domain::ThreadId;
use crate::store::PendingHold;
use crate::t3::{
    HOLD_MARKER, T3Env, ThreadActivity, WakeOutcome, hold_thread, hold_view, release_hold,
};

const POLL_INTERVAL: Duration = Duration::from_secs(3);
// an event turn starts a moment after its hold ends, so a quick look at an
// idle thread would hold it again; two ticks outlast that gap
const IDLE_TICKS_TO_HOLD: u8 = 2;
// after a failed hold or release, wait this many ticks before trying again
const RETRY_TICKS: u8 = 10;
// task names listed in the placeholder before "and N more"
const NAMES_SHOWN: usize = 3;

#[derive(Debug, Default)]
struct Track {
    idle_ticks: u8,
    cooldown: u8,
    /// The loop made, or found, a placeholder in this thread
    holding: bool,
}

/// Per-thread memory of the hold loop
#[derive(Debug, Default)]
pub(crate) struct Holder {
    threads: HashMap<ThreadId, Track>,
}

impl Holder {
    /// A holder that releases leftover placeholders in `threads` on its first step
    pub(crate) fn after_restart(threads: Vec<ThreadId>) -> Self {
        Self {
            threads: threads
                .into_iter()
                .map(|thread| {
                    let track = Track {
                        holding: true,
                        ..Track::default()
                    };
                    (thread, track)
                })
                .collect(),
        }
    }
}

/// Hold and release threads for as long as the daemon runs
pub(super) async fn run(state: AppState) {
    let held = call(&state.store, |reply| StoreMsg::HeldT3Threads { reply })
        .await
        .unwrap_or_else(|error| {
            warn!("read held threads: {error}");
            Vec::new()
        });
    let mut holder = Holder::after_restart(held);
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let pending = match call(&state.store, |reply| StoreMsg::PendingT3Holds { reply }).await {
            Ok(pending) => pending,
            Err(error) => {
                warn!("read pending holds: {error}");
                continue;
            }
        };
        if pending.is_empty() && holder.threads.is_empty() {
            continue;
        }
        holder = match tokio::task::spawn_blocking(move || {
            step(&T3Env::from_env(), &pending, &mut holder);
            holder
        })
        .await
        {
            Ok(holder) => holder,
            Err(error) => {
                warn!("T3 hold step join: {error}");
                Holder::default()
            }
        };
    }
}

/// Look at every thread with a pending hold, and at those the loop holds
///
/// Blocking, since it reads T3's state and calls its API
pub(crate) fn step(env: &T3Env, pending: &[PendingHold], holder: &mut Holder) {
    let mut wanted: Vec<(ThreadId, Vec<&str>)> = Vec::new();
    for hold in pending {
        match wanted.iter_mut().find(|(thread, _)| *thread == hold.thread) {
            Some((_, names)) => names.push(&hold.name),
            None => wanted.push((hold.thread, vec![&hold.name])),
        }
    }

    for (thread, names) in &wanted {
        let track = holder.threads.entry(*thread).or_default();
        track.cooldown = track.cooldown.saturating_sub(1);
        let activity = match hold_view(env, *thread) {
            Ok(Some(view)) => view.activity,
            // a session T3 does not own, or state that cannot be read, is left alone
            Ok(None) => {
                holder.threads.remove(thread);
                continue;
            }
            Err(error) => {
                debug!(%thread, "T3 thread state: {error}");
                track.idle_ticks = 0;
                continue;
            }
        };
        match activity {
            ThreadActivity::Idle => {
                track.holding = false;
                track.idle_ticks = track.idle_ticks.saturating_add(1);
                if track.idle_ticks >= IDLE_TICKS_TO_HOLD && track.cooldown == 0 {
                    hold(env, *thread, names, track);
                }
            }
            ThreadActivity::Held {
                run, queued_behind, ..
            } => {
                track.holding = true;
                track.idle_ticks = 0;
                if queued_behind && track.cooldown == 0 {
                    release(env, *thread, &run, track);
                }
            }
            ThreadActivity::Busy | ThreadActivity::Stopped | ThreadActivity::Archived => {
                track.holding = false;
                track.idle_ticks = 0;
            }
        }
    }

    // threads with nothing pending: end a hold the loop made, else forget them
    let finished = holder
        .threads
        .keys()
        .filter(|thread| !wanted.iter().any(|(wanted, _)| wanted == *thread))
        .copied()
        .collect::<Vec<_>>();
    for thread in finished {
        let Some(track) = holder.threads.get_mut(&thread) else {
            continue;
        };
        track.cooldown = track.cooldown.saturating_sub(1);
        let keep = track.holding
            && match hold_view(env, thread) {
                Ok(Some(view)) => match view.activity {
                    ThreadActivity::Held { run, .. } => {
                        track.cooldown > 0 || !release(env, thread, &run, track)
                    }
                    _ => false,
                },
                _ => false,
            };
        if !keep {
            holder.threads.remove(&thread);
        }
    }
}

fn hold(env: &T3Env, thread: ThreadId, names: &[&str], track: &mut Track) {
    match hold_thread(env, thread, &placeholder(names)) {
        WakeOutcome::Woken { t3_thread } => {
            info!(%thread, t3_thread, "holding a T3 thread while its tasks run");
            track.holding = true;
            track.idle_ticks = 0;
        }
        // the thread started a run since it was read, or T3 is not there
        outcome => {
            debug!(%thread, "T3 hold not placed: {outcome:?}");
            track.cooldown = RETRY_TICKS;
        }
    }
}

/// Whether the placeholder ended
fn release(env: &T3Env, thread: ThreadId, run: &str, track: &mut Track) -> bool {
    match release_hold(env, thread, run) {
        WakeOutcome::Woken { t3_thread } => {
            info!(%thread, t3_thread, "released a T3 hold");
            track.holding = false;
            true
        }
        outcome => {
            warn!(%thread, "T3 hold not released: {outcome:?}");
            track.cooldown = RETRY_TICKS;
            false
        }
    }
}

/// The text of the placeholder message; it starts with [`HOLD_MARKER`]
fn placeholder(names: &[&str]) -> String {
    let mut text = format!(
        "{HOLD_MARKER}: {}",
        names[..names.len().min(NAMES_SHOWN)].join(", ")
    );
    if names.len() > NAMES_SHOWN {
        let _ = write!(text, " and {} more", names.len() - NAMES_SHOWN);
    }
    text
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use uuid::Uuid;

    use super::{Holder, PendingHold, step};
    use crate::domain::{TaskId, ThreadId};
    use crate::t3::HOLD_MARKER;
    use crate::t3::test_support::{
        FakeT3Server, Fixture, SESSION_ID, T3_THREAD_ID, V2State, dispatched, live_pid, ticket,
    };

    fn thread() -> ThreadId {
        ThreadId(Uuid::parse_str(SESSION_ID).unwrap())
    }

    fn pending() -> Vec<PendingHold> {
        vec![PendingHold {
            task: TaskId(Uuid::now_v7()),
            name: "long-build".into(),
            thread: thread(),
        }]
    }

    fn hold_text() -> String {
        format!("{HOLD_MARKER}: long-build")
    }

    /// A T3 that accepts every command, and a home whose state has `runs` for a thread
    /// that `driver` owns
    fn setup(driver: &str, runs: impl FnOnce(V2State) -> V2State) -> (FakeT3Server, Fixture) {
        let responses = (0..8).flat_map(|_| [ticket(), dispatched()]).collect();
        let server = FakeT3Server::start(Some(2), responses);
        let fixture = Fixture::empty(Some(&server), live_pid());
        runs(
            fixture
                .v2_state()
                .thread(T3_THREAD_ID, "Thread", false)
                .native(driver, SESSION_ID, T3_THREAD_ID),
        );
        (server, fixture)
    }

    /// The command of each RPC call the server received
    fn commands(server: &FakeT3Server) -> Vec<Value> {
        server
            .requests()
            .iter()
            .filter(|request| request.method == "WS")
            .map(|request| {
                let request: Value = serde_json::from_str(&request.body).unwrap();
                request["payload"].clone()
            })
            .collect()
    }

    #[test]
    fn a_thread_is_held_only_when_it_is_idle() {
        let (busy, fixture) = setup("codex", |state| {
            state.run(T3_THREAD_ID, 1, "running", "do the work")
        });
        let mut holder = Holder::default();
        for _ in 0..4 {
            step(&fixture.env, &pending(), &mut holder);
        }
        assert!(commands(&busy).is_empty());

        let (idle, fixture) = setup("codex", |state| {
            state.run(T3_THREAD_ID, 1, "completed", "do the work")
        });
        let mut holder = Holder::default();
        step(&fixture.env, &pending(), &mut holder);
        // one quiet look is not enough: an event turn may be about to start
        assert!(commands(&idle).is_empty());
        step(&fixture.env, &pending(), &mut holder);

        let sent = commands(&idle);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["type"], "message.dispatch");
        assert_eq!(sent[0]["dispatchMode"]["type"], "defer_start");
        assert_eq!(sent[0]["threadId"], T3_THREAD_ID);
        assert_eq!(sent[0]["text"], hold_text());
    }

    #[test]
    fn a_message_the_user_queued_releases_the_hold() {
        let (server, fixture) = setup("codex", |state| {
            state.run(T3_THREAD_ID, 1, "preparing", &hold_text()).run(
                T3_THREAD_ID,
                2,
                "queued",
                "what is the status?",
            )
        });
        let mut holder = Holder::default();
        step(&fixture.env, &pending(), &mut holder);

        let sent = commands(&server);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["type"], "run.interrupt");
        assert_eq!(sent[0]["runId"], format!("run-{T3_THREAD_ID}-1"));
        // the Stop button holds the queue; releasing a hold must run it
        assert!(sent[0].get("holdQueue").is_none());
    }

    #[test]
    fn a_hold_with_nothing_behind_it_stays_while_tasks_are_pending() {
        let (server, fixture) = setup("codex", |state| {
            state.run(T3_THREAD_ID, 1, "preparing", &hold_text())
        });
        let mut holder = Holder::default();
        for _ in 0..3 {
            step(&fixture.env, &pending(), &mut holder);
        }
        assert!(commands(&server).is_empty());

        // the same hold ends once its tasks are done
        step(&fixture.env, &[], &mut holder);
        let sent = commands(&server);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["type"], "run.interrupt");
    }

    #[test]
    fn a_hold_left_by_a_previous_daemon_ends_when_no_task_needs_it() {
        let (server, fixture) = setup("codex", |state| {
            state.run(T3_THREAD_ID, 1, "preparing", &hold_text())
        });
        let mut holder = Holder::after_restart(vec![thread()]);
        step(&fixture.env, &[], &mut holder);
        step(&fixture.env, &[], &mut holder);

        let sent = commands(&server);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["type"], "run.interrupt");
    }

    #[test]
    fn a_thread_is_not_held_again_once_its_tasks_are_done() {
        let (server, fixture) = setup("codex", |state| {
            state.run(T3_THREAD_ID, 1, "completed", "do the work")
        });
        let mut holder = Holder::default();
        step(&fixture.env, &pending(), &mut holder);
        step(&fixture.env, &pending(), &mut holder);
        assert_eq!(commands(&server).len(), 1);

        // the final event was delivered and settled, so nothing is pending
        for _ in 0..5 {
            step(&fixture.env, &[], &mut holder);
        }
        assert_eq!(commands(&server).len(), 1);
    }

    #[test]
    fn a_thread_the_user_stopped_is_not_held_again() {
        let (server, fixture) = setup("codex", |state| {
            state.run(T3_THREAD_ID, 1, "interrupted", &hold_text())
        });
        let mut holder = Holder::default();
        for _ in 0..4 {
            step(&fixture.env, &pending(), &mut holder);
        }
        assert!(commands(&server).is_empty());
    }

    #[test]
    fn a_session_that_is_not_a_codex_thread_in_t3_is_ignored() {
        let (server, fixture) = setup("claudeAgent", |state| {
            state.run(T3_THREAD_ID, 1, "completed", "do the work")
        });
        let mut holder = Holder::default();
        for _ in 0..4 {
            step(&fixture.env, &pending(), &mut holder);
        }
        // not even a token or ticket request
        assert!(server.requests().is_empty());
    }
}
