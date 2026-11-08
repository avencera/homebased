//! One check of every live thread watch, run after each idle compaction scan
//!
//! Each machine that runs a watched thread is asked once per scan, for the
//! watched threads only, so only their transcript or rollout tails are read.
//! A watch fires when its thread is over the limit and no turn runs, then
//! stays quiet until the context drops below the limit again. A machine that
//! does not answer leaves its watches as they are until the next scan

use std::collections::HashMap;

use serde_json::{Value, json};
use tracing::{debug, warn};

use super::{compact_resolved, find_on};
use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, call};
use crate::daemon::compaction_log::Trigger;
use crate::domain::API_VERSION;
use crate::machine::MachineId;
use crate::store::{WatchRecord, WatchUpdate};
use crate::thread_ref::{ThreadContext, ThreadHost, ThreadState};
use crate::thread_watch::{
    CompactOutcome, ThreadWatch, WatchAction, WatchEndReason, WatchEventKind, WatchTarget,
};

/// Check every live watch once
pub(crate) async fn scan(state: &AppState) {
    let records = match call(&state.store, |reply| StoreMsg::ListWatches {
        caller: None,
        ended: false,
        reply,
    })
    .await
    {
        Ok(records) => records,
        Err(error) => {
            warn!("thread watch scan: {error}");
            return;
        }
    };
    if records.is_empty() {
        return;
    }

    let mut by_machine: HashMap<MachineId, Vec<String>> = HashMap::new();
    for record in &records {
        let references = by_machine.entry(record.watch.target.machine()).or_default();
        let reference = record.watch.target.reference();
        if !references.contains(&reference) {
            references.push(reference);
        }
    }
    let mut found: HashMap<(MachineId, String), Vec<ThreadContext>> = HashMap::new();
    for (machine, references) in by_machine {
        match find_on(state, machine, references.clone()).await {
            Ok(results) => {
                for (reference, matches) in references.into_iter().zip(results) {
                    found.insert((machine, reference), matches);
                }
            }
            Err(message) => debug!(%machine, "thread watch scan skipped a machine: {message}"),
        }
    }

    for record in records {
        let key = (
            record.watch.target.machine(),
            record.watch.target.reference(),
        );
        let Some(matches) = found.get(&key) else {
            continue;
        };
        let Some(update) = check(state, &record, matches).await else {
            continue;
        };
        let id = record.watch.id;
        if let Err(error) = call(&state.store, |reply| StoreMsg::UpdateWatch {
            id,
            update,
            reply,
        })
        .await
        {
            warn!(watch = %id, "thread watch update: {error}");
        }
    }
}

/// What one check of a watch finds, before any compaction
#[derive(Debug, PartialEq)]
enum Decision<'a> {
    /// Nothing changes
    Unchanged,
    /// The context dropped below the limit, so the next climb fires again
    Rearm,
    /// The watched thread is gone or put away
    End(WatchEndReason, Option<&'a ThreadContext>),
    /// Over the limit while no turn runs, and armed
    Fire(&'a ThreadContext),
}

fn decide<'a>(record: &WatchRecord, matches: &'a [ThreadContext]) -> Decision<'a> {
    let watch = &record.watch;
    let thread = match matches {
        [] => {
            let reason = match watch.target {
                WatchTarget::T3 { .. } => WatchEndReason::Deleted,
                WatchTarget::Session { .. } => WatchEndReason::SessionGone,
            };
            return Decision::End(reason, None);
        }
        [thread] => thread,
        _ => {
            // an exact id names one thread per machine, so this is a store oddity to skip
            debug!(watch = %watch.id, "watched reference names several threads");
            return Decision::Unchanged;
        }
    };
    if matches!(watch.target, WatchTarget::T3 { .. }) && thread.state == ThreadState::Archived {
        return Decision::End(WatchEndReason::Archived, Some(thread));
    }

    let limit = watch.threshold.limit(thread.context_window);
    let over = thread
        .tokens
        .zip(limit)
        .is_some_and(|(tokens, limit)| tokens > limit);
    match (over, record.armed) {
        (false, false) => Decision::Rearm,
        (false, true) | (true, false) => Decision::Unchanged,
        (true, true) if thread.state.busy() => Decision::Unchanged,
        (true, true) => Decision::Fire(thread),
    }
}

/// What this check changes about one watch, if anything
async fn check(
    state: &AppState,
    record: &WatchRecord,
    matches: &[ThreadContext],
) -> Option<WatchUpdate> {
    let local = state.machine.identity.machine;
    match decide(record, matches) {
        Decision::Unchanged => None,
        Decision::Rearm => Some(WatchUpdate {
            armed: true,
            events: Vec::new(),
            end: None,
        }),
        Decision::End(reason, thread) => Some(ended(local, record, reason, thread)),
        Decision::Fire(thread) => {
            let compaction = match record.watch.action {
                WatchAction::Notify => None,
                WatchAction::Compact => Some(
                    compact_resolved(state, thread, Trigger::Watch)
                        .await
                        .unwrap_or_else(|error| {
                            (CompactOutcome::Unavailable, Some(error.to_string()))
                        }),
                ),
            };
            Some(fired(local, &record.watch, thread, compaction.as_ref()))
        }
    }
}

/// The last event of a watch that ends for `reason`
pub(crate) fn ended(
    local: MachineId,
    record: &WatchRecord,
    reason: WatchEndReason,
    thread: Option<&ThreadContext>,
) -> WatchUpdate {
    WatchUpdate {
        armed: record.armed,
        events: vec![(
            WatchEventKind::Ended,
            payload(
                local,
                &record.watch,
                WatchEventKind::Ended,
                thread,
                None,
                Some(reason),
            ),
        )],
        end: Some(reason),
    }
}

/// `THREAD_CONTEXT_OVER`, and the end of a `--once` watch
fn fired(
    local: MachineId,
    watch: &ThreadWatch,
    thread: &ThreadContext,
    compaction: Option<&(CompactOutcome, Option<String>)>,
) -> WatchUpdate {
    let mut events = vec![(
        WatchEventKind::ContextOver,
        payload(
            local,
            watch,
            WatchEventKind::ContextOver,
            Some(thread),
            compaction,
            None,
        ),
    )];
    let end = watch.once.then_some(WatchEndReason::Once);
    if let Some(reason) = end {
        events.push((
            WatchEventKind::Ended,
            payload(
                local,
                watch,
                WatchEventKind::Ended,
                Some(thread),
                None,
                Some(reason),
            ),
        ));
    }
    WatchUpdate {
        armed: false,
        events,
        end,
    }
}

/// The `HOMEBASED_EVENT` payload of one watch event; the store adds `seq`
pub(crate) fn payload(
    local: MachineId,
    watch: &ThreadWatch,
    kind: WatchEventKind,
    thread: Option<&ThreadContext>,
    compaction: Option<&(CompactOutcome, Option<String>)>,
    reason: Option<WatchEndReason>,
) -> Value {
    let exact = thread.map_or_else(|| watch.target.reference(), ThreadContext::exact_reference);
    let title = thread
        .and_then(|thread| thread.title.clone())
        .or_else(|| watch.title.clone());
    let limit = thread.and_then(|thread| watch.threshold.limit(thread.context_window));
    let context = thread.map(|thread| {
        json!({
            "tokens": thread.tokens,
            "tokens_estimated": thread.tokens_estimated,
            "context_window": thread.context_window,
            "percent": thread.percent,
            "state": thread.state,
            "last_active": thread.last_active,
            "cache_warm": thread.cache_warm,
            "compacted_at": thread.compacted_at,
        })
    });
    json!({
        "api_version": API_VERSION,
        "event": kind,
        "watch": watch.id,
        "thread": watch.caller,
        "machine": local,
        "target": {
            "reference": watch.reference,
            "exact_reference": exact,
            "machine": watch.target.machine(),
            "machine_name": thread.map(|thread| thread.machine_name.clone()),
            "host": thread.map(|thread| thread.host),
            "t3_thread": thread.and_then(|thread| thread.t3_thread.clone()),
            "t3_link": thread.and_then(|thread| thread.t3_link.clone()),
            "session": thread.and_then(|thread| thread.session),
            "provider": thread.and_then(|thread| thread.provider),
            "title": title,
        },
        "condition": {
            "context_above": watch.threshold.to_string(),
            "limit": limit,
        },
        "action": watch.action,
        "context": context,
        "compaction": compaction.map(|(outcome, detail)| json!({
            "outcome": outcome,
            "detail": detail,
        })),
        "reason": reason,
        "next_action": next_action(watch, kind, thread, compaction, reason, &exact),
    })
}

fn next_action(
    watch: &ThreadWatch,
    kind: WatchEventKind,
    thread: Option<&ThreadContext>,
    compaction: Option<&(CompactOutcome, Option<String>)>,
    reason: Option<WatchEndReason>,
    exact: &str,
) -> String {
    let rearm = "the watch fires again after the context drops below the limit and climbs back";
    match (kind, reason) {
        (WatchEventKind::ContextOver, _) => match compaction {
            Some((CompactOutcome::Started, _)) => {
                format!("Compaction started; nothing to do, and {rearm}")
            }
            Some((outcome, detail)) => format!(
                "Compaction did not start ({}{}). Run `homebased thread compact {exact}` once the thread is idle, or hand its work to a fresh thread",
                outcome.as_str(),
                detail
                    .as_deref()
                    .map(|detail| format!(": {detail}"))
                    .unwrap_or_default()
            ),
            None if thread.is_some_and(|thread| thread.host == ThreadHost::T3) => format!(
                "Compact it with `homebased thread compact {exact}`, or hand its work to a fresh thread; {rearm}"
            ),
            None => format!(
                "homebased compacts only T3 threads, so tell this session to run /compact or hand its work to a fresh thread; {rearm}"
            ),
        },
        (WatchEventKind::Ended, Some(WatchEndReason::Once)) => {
            "The --once watch fired and ended; nothing more will arrive for it".into()
        }
        (WatchEventKind::Ended, Some(WatchEndReason::Unwatched)) => {
            "The watch was removed; nothing more will arrive for it".into()
        }
        (WatchEventKind::Ended, reason) => {
            let why = match reason {
                Some(WatchEndReason::Archived) => "the T3 thread was archived",
                Some(WatchEndReason::Deleted) => "the T3 thread no longer exists",
                _ => "the session has no transcript or rollout any more",
            };
            format!(
                "The watch ended because {why}. Watch {} again with `homebased thread watch` if it still matters",
                watch.reference
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{Decision, decide, ended, fired};
    use crate::domain::{TaskEnv, ThreadId};
    use crate::machine::MachineId;
    use crate::store::{Store, WatchRecord};
    use crate::submission::{CallbackContext, CallbackExecutable};
    use crate::t3::ThreadDetails;
    use crate::thread_ref::{ThreadContext, ThreadHost, ThreadState};
    use crate::thread_watch::{
        ThreadWatch, Threshold, WatchAction, WatchEndReason, WatchEventKind, WatchId, WatchTarget,
    };

    fn watch(caller: ThreadId, target: WatchTarget, once: bool) -> ThreadWatch {
        ThreadWatch {
            id: WatchId::new(),
            caller,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: "/tmp".into(),
                codex: CallbackExecutable::available("/bin/true".into()),
            },
            target,
            reference: "thread_mcp:launch:feed".into(),
            title: Some("child".into()),
            threshold: Threshold::Tokens(100_000),
            action: WatchAction::Notify,
            once,
            created_at: Utc::now(),
        }
    }

    fn thread(machine: MachineId, tokens: u64, state: ThreadState) -> ThreadContext {
        ThreadContext {
            reference: None,
            machine,
            machine_name: "here".into(),
            host: ThreadHost::T3,
            provider: None,
            session: None,
            t3_thread: Some("mcp:launch:feed".into()),
            t3_link: None,
            title: Some("child".into()),
            state,
            tokens: Some(tokens),
            tokens_estimated: false,
            context_window: None,
            percent: None,
            last_active: None,
            cache_warm: None,
            compacted_at: None,
            parent_t3_thread: None,
            details: ThreadDetails::default(),
        }
    }

    /// Run one check against the store the way the scan does, and return the
    /// kinds of the events it added
    fn step(
        store: &Store,
        local: MachineId,
        id: WatchId,
        now: &[ThreadContext],
    ) -> Vec<WatchEventKind> {
        let record: WatchRecord = store.watch_record(id).unwrap().unwrap();
        let update = match decide(&record, now) {
            Decision::Unchanged => return Vec::new(),
            Decision::Rearm => crate::store::WatchUpdate {
                armed: true,
                events: Vec::new(),
                end: None,
            },
            Decision::End(reason, thread) => ended(local, &record, reason, thread),
            Decision::Fire(thread) => fired(local, &record.watch, thread, None),
        };
        store
            .update_watch(id, update, Utc::now())
            .unwrap()
            .into_iter()
            .map(|event| event.kind)
            .collect()
    }

    #[test]
    fn a_watch_fires_once_per_climb_while_idle_and_ends_with_its_thread() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("state.db")).unwrap();
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let target = WatchTarget::T3 {
            machine: local,
            t3_thread: "mcp:launch:feed".into(),
        };
        let (first, created) = store
            .upsert_watch(&watch(caller, target.clone(), false))
            .unwrap();
        assert!(created);
        // the same caller watching the same thread again changes the existing watch
        let mut again = watch(caller, target.clone(), false);
        again.threshold = Threshold::Tokens(120_000);
        let (second, created) = store.upsert_watch(&again).unwrap();
        assert!(!created);
        assert_eq!(second.watch.id, first.watch.id);
        assert_eq!(second.watch.threshold, Threshold::Tokens(120_000));
        let id = first.watch.id;

        let over = |state| [thread(local, 150_000, state)];
        let below = [thread(local, 40_000, ThreadState::Idle)];
        assert!(step(&store, local, id, &below).is_empty());
        // a running turn holds the event until the thread is idle
        assert!(step(&store, local, id, &over(ThreadState::Running)).is_empty());
        assert_eq!(
            step(&store, local, id, &over(ThreadState::Waiting)),
            [WatchEventKind::ContextOver]
        );
        // still over the limit: no second event for the same climb
        assert!(step(&store, local, id, &over(ThreadState::Idle)).is_empty());
        assert!(step(&store, local, id, &below).is_empty());
        assert_eq!(
            step(&store, local, id, &over(ThreadState::Idle)),
            [WatchEventKind::ContextOver]
        );
        assert_eq!(step(&store, local, id, &[]), [WatchEventKind::Ended]);
        let record = store.watch_record(id).unwrap().unwrap();
        assert_eq!(record.end_reason, Some(WatchEndReason::Deleted));
        // an ended watch changes no more
        assert!(step(&store, local, id, &over(ThreadState::Idle)).is_empty());

        // events go out one at a time, in sequence, until each settles
        let pending = |store: &Store| -> Vec<u64> {
            store
                .pending_watch_events()
                .unwrap()
                .into_iter()
                .map(|(_, event)| event.seq)
                .collect()
        };
        assert_eq!(pending(&store), [1]);
        store
            .settle_watch_event(id, 1, "printed", Utc::now())
            .unwrap();
        assert_eq!(pending(&store), [2]);
        store
            .settle_watch_event(id, 2, "delivered", Utc::now())
            .unwrap();
        store
            .settle_watch_event(id, 3, "delivered", Utc::now())
            .unwrap();
        let settled = store.settled_watch_events(id).unwrap();
        assert_eq!(
            settled.iter().map(|event| event.seq).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(settled[0].payload["seq"], 1);
        assert_eq!(settled[0].payload["watch"], id.to_string());
        assert_eq!(settled[2].payload["reason"], "deleted");

        // a finished watch frees its target: watching it again makes a new watch
        let (fresh, created) = store.upsert_watch(&watch(caller, target, true)).unwrap();
        assert!(created);
        assert_ne!(fresh.watch.id, id);
        assert_eq!(
            step(&store, local, fresh.watch.id, &over(ThreadState::Settled)),
            [WatchEventKind::ContextOver, WatchEventKind::Ended]
        );
        let record = store.watch_record(fresh.watch.id).unwrap().unwrap();
        assert_eq!(record.end_reason, Some(WatchEndReason::Once));
    }

    #[test]
    fn a_session_watch_ends_when_the_session_is_gone_and_a_t3_watch_when_archived() {
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let session = WatchTarget::Session {
            machine: local,
            provider: crate::thread_ref::Provider::Claude,
            session: ThreadId(uuid::Uuid::now_v7()),
        };
        let t3 = WatchTarget::T3 {
            machine: local,
            t3_thread: "mcp:launch:feed".into(),
        };
        let record = |target| WatchRecord {
            watch: watch(caller, target, false),
            armed: true,
            seq: 0,
            ended_at: None,
            end_reason: None,
        };
        assert_eq!(
            decide(&record(session.clone()), &[]),
            Decision::End(WatchEndReason::SessionGone, None)
        );
        let archived = [thread(local, 10, ThreadState::Archived)];
        assert_eq!(
            decide(&record(t3), &archived),
            Decision::End(WatchEndReason::Archived, Some(&archived[0]))
        );
        // a session watch follows the session, whatever its T3 thread does
        assert_eq!(decide(&record(session), &archived), Decision::Unchanged);
    }
}
