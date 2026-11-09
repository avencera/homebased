//! One check of every live thread watch, run after each idle compaction scan
//!
//! Each machine that runs a watched thread is asked once per scan, for the
//! watched threads only, so only their transcript or rollout tails are read.
//! A context watch fires when its thread is over the limit and no turn runs,
//! then stays quiet until the context drops below the limit again. A stall
//! watch fires when its thread has sat idle past the grace, with no turn
//! running and nothing due to wake it, then stays quiet until the thread is
//! active again. A machine that does not answer leaves its watches as they
//! are until the next scan
//!
//! A watch also ends once nothing it could report is left: when its T3 thread
//! stays finished, settled or idle after its delegated task ended, for
//! [`QUIET_FINISHED`] without activity, and when the T3 thread that receives
//! its events, always on this machine, is archived or deleted or stays
//! finished as long. A finished thread never fires, so a watch never compacts
//! a thread whose work is done, whether the watch names it or its session. A thread found gone ends its watch only after
//! [`GONE_SCANS`] scans in a row, so one bad read does not. The counts and
//! clocks live in memory: a restart starts them over, which only delays an end

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value, json};
use tracing::{debug, warn};

use super::{compact_resolved, find_on};
use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, call};
use crate::daemon::compaction_log::Trigger;
use crate::domain::API_VERSION;
use crate::machine::MachineId;
use crate::store::{WatchRecord, WatchUpdate};
use crate::thread_ref::{Provider, ThreadContext, ThreadHost, ThreadState};
use crate::thread_watch::{
    CompactOutcome, StallAfter, ThreadWatch, Threshold, WatchAction, WatchCondition,
    WatchEndReason, WatchEventKind, WatchId, WatchTarget,
};

/// Scans in a row that must find a watched thread or its caller gone before
/// the watch ends, so a T3 file that is briefly missing ends nothing
const GONE_SCANS: u8 = 2;

/// How long a finished thread must stay quiet before the watches on it, or
/// the watches it made, end
pub(crate) const QUIET_FINISHED: TimeDelta = TimeDelta::hours(1);

/// What the scan remembers of each watch from one run to the next
#[derive(Debug)]
pub(crate) struct WatchMemory {
    /// How long a finished thread must stay quiet, [`QUIET_FINISHED`] but in tests
    quiet: TimeDelta,
    targets: HashMap<WatchId, Track>,
    callers: HashMap<WatchId, Track>,
}

impl WatchMemory {
    /// Nothing seen yet, ending watches of threads finished and quiet for `quiet`
    pub(crate) fn new(quiet: TimeDelta) -> Self {
        Self {
            quiet,
            targets: HashMap::new(),
            callers: HashMap::new(),
        }
    }

    /// Forget watches that are no longer live
    fn keep(&mut self, live: &HashSet<WatchId>) {
        self.targets.retain(|id, _| live.contains(id));
        self.callers.retain(|id, _| live.contains(id));
    }

    /// Note a scan that could not tell whether the watched thread of `id` is there
    fn target_unanswered(&mut self, id: WatchId) {
        if let Some(track) = self.targets.get_mut(&id) {
            track.unanswered();
        }
    }

    /// Note a scan that could not tell whether the caller of `id` is there
    fn caller_unanswered(&mut self, id: WatchId) {
        if let Some(track) = self.callers.get_mut(&id) {
            track.unanswered();
        }
    }
}

/// What earlier scans saw of one thread
#[derive(Debug, Default, Clone, PartialEq)]
struct Track {
    /// Scans in a row that found it gone
    gone: u8,
    /// When it was first seen finished with the reading it still has
    quiet: Option<Quiet>,
    /// When a scan last saw it running or waiting, which a stall watch
    /// counts from as well as `last_active`
    active_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
struct Quiet {
    since: DateTime<Utc>,
    tokens: Option<u64>,
    last_active: Option<DateTime<Utc>>,
}

impl Track {
    /// A scan that could not look breaks a run of scans that found the
    /// thread gone, so the next one starts the count over
    fn unanswered(&mut self) {
        self.gone = 0;
    }

    /// Count one scan that found the thread gone, and whether that makes enough
    fn gone(&mut self) -> bool {
        self.quiet = None;
        self.gone = self.gone.saturating_add(1);
        self.gone >= GONE_SCANS
    }

    /// Note one reading of a thread that is there, and whether it has stayed
    /// `finished` with that same reading for the quiet time
    ///
    /// New tokens or activity, or work that starts again, starts the clock over
    fn quiet_for(&mut self, thread: &ThreadContext, finished: bool, clock: Clock) -> bool {
        self.gone = 0;
        if !finished {
            self.quiet = None;
            return false;
        }
        let reading = (thread.tokens, thread.last_active);
        match &self.quiet {
            Some(quiet) if (quiet.tokens, quiet.last_active) == reading => {
                clock.now - quiet.since >= clock.quiet
            }
            _ => {
                self.quiet = Some(Quiet {
                    since: clock.now,
                    tokens: reading.0,
                    last_active: reading.1,
                });
                false
            }
        }
    }
}

/// The time of one scan, and how long a finished thread must stay quiet
#[derive(Debug, Clone, Copy)]
struct Clock {
    now: DateTime<Utc>,
    quiet: TimeDelta,
}

/// Check every live watch once, at `now`
pub(crate) async fn scan(state: &AppState, memory: &mut WatchMemory, now: DateTime<Utc>) {
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
    memory.keep(&records.iter().map(|record| record.watch.id).collect());
    if records.is_empty() {
        return;
    }

    // a caller is on this machine, where its watches live, so its lookup
    // joins the one for this machine's watched threads
    let local = state.machine.identity.machine;
    let mut by_machine: HashMap<MachineId, Vec<String>> = HashMap::new();
    for record in &records {
        let wanted = [
            (
                record.watch.target.machine(),
                record.watch.target.reference(),
            ),
            (local, caller_reference(&record.watch)),
        ];
        for (machine, reference) in wanted {
            let references = by_machine.entry(machine).or_default();
            if !references.contains(&reference) {
                references.push(reference);
            }
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

    let clock = Clock {
        now,
        quiet: memory.quiet,
    };
    for record in records {
        let target = found
            .get(&(
                record.watch.target.machine(),
                record.watch.target.reference(),
            ))
            .map(Vec::as_slice);
        let caller = found
            .get(&(local, caller_reference(&record.watch)))
            .map(Vec::as_slice);
        let Some(update) = check(state, memory, &record, target, caller, clock).await else {
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

/// The reference that finds a watch's caller here
///
/// A watch from before `caller_t3` knows only the caller's session, which
/// names its T3 thread while T3 runs that session next
fn caller_reference(watch: &ThreadWatch) -> String {
    match &watch.caller_t3 {
        Some(thread) => format!("thread_{thread}"),
        None => watch.caller.to_string(),
    }
}

/// What the scan found of a watch's caller
#[derive(Debug, Clone, Copy, PartialEq)]
enum CallerSeen<'a> {
    /// No T3 thread the scan can follow, such as a session outside T3
    Untracked,
    /// The caller's T3 thread is gone
    Gone,
    /// The caller's T3 thread
    T3(&'a ThreadContext),
}

fn caller_seen<'a>(watch: &ThreadWatch, matches: &'a [ThreadContext]) -> CallerSeen<'a> {
    let t3 = match &watch.caller_t3 {
        Some(id) => {
            let found = matches
                .iter()
                .find(|thread| thread.t3_thread.as_deref() == Some(id.as_str()));
            return found.map_or(CallerSeen::Gone, CallerSeen::T3);
        }
        // a session that no T3 thread runs may be a terminal session, or a T3
        // thread that is deleted, which the scan cannot tell apart
        None => matches.iter().find(|thread| thread.host == ThreadHost::T3),
    };
    t3.map_or(CallerSeen::Untracked, CallerSeen::T3)
}

/// Whether a watch's caller is gone for good, so nothing would read its events
fn caller_gone(seen: CallerSeen<'_>, track: &mut Track, clock: Clock) -> bool {
    match seen {
        CallerSeen::Untracked => {
            *track = Track::default();
            false
        }
        CallerSeen::Gone => track.gone(),
        CallerSeen::T3(thread) if thread.state == ThreadState::Archived => true,
        CallerSeen::T3(thread) => track.quiet_for(thread, thread.done(), clock),
    }
}

/// What one check of a watch finds, before any compaction
#[derive(Debug, PartialEq)]
enum Decision<'a> {
    /// Nothing changes
    Unchanged,
    /// The context dropped below the limit, or the stalled thread is active
    /// again, so the condition fires again when next met
    Rearm,
    /// The watched thread is gone, put away, or done
    End(WatchEndReason, Option<&'a ThreadContext>),
    /// The condition is met, and the watch armed
    Fire(&'a ThreadContext),
}

fn decide<'a>(
    record: &WatchRecord,
    matches: &'a [ThreadContext],
    track: &mut Track,
    clock: Clock,
) -> Decision<'a> {
    let watch = &record.watch;
    let thread = match matches {
        [] => {
            let reason = match watch.target {
                WatchTarget::T3 { .. } => WatchEndReason::Deleted,
                WatchTarget::Session { .. } => WatchEndReason::SessionGone,
            };
            return if track.gone() {
                Decision::End(reason, None)
            } else {
                Decision::Unchanged
            };
        }
        [thread] => thread,
        _ => {
            // an exact id names one thread per machine, so this is a store oddity to skip
            debug!(watch = %watch.id, "watched reference names several threads");
            return Decision::Unchanged;
        }
    };
    match watch.target {
        WatchTarget::T3 { .. } if thread.state == ThreadState::Archived => {
            return Decision::End(WatchEndReason::Archived, Some(thread));
        }
        // T3 can still name a session whose transcript or rollout is gone
        WatchTarget::Session { .. } if thread.session_exists == Some(false) => {
            return if track.gone() {
                Decision::End(WatchEndReason::SessionGone, Some(thread))
            } else {
                Decision::Unchanged
            };
        }
        _ => {}
    }
    // the watch refuses a percent limit on a Claude thread, but a T3 thread
    // can start on Claude after the watch is made, and then it never fires
    if matches!(
        watch.condition,
        WatchCondition::ContextAbove(Threshold::Percent(_))
    ) && thread.provider == Some(Provider::Claude)
    {
        return Decision::End(WatchEndReason::NoContextWindow, Some(thread));
    }

    // a finished thread grows no more and owes no report, so compacting or
    // reporting it costs a turn for nothing; it counts again once it starts
    // new work. The thread a session names is the T3 thread that runs it, so
    // a session watch of a T3 thread follows the same rule. Only T3 records a
    // thread as done
    let finished = thread.done();
    if track.quiet_for(thread, finished, clock) {
        return Decision::End(WatchEndReason::Finished, Some(thread));
    }
    match watch.condition {
        WatchCondition::ContextAbove(threshold) => {
            context_decision(record.armed, thread, threshold, finished)
        }
        WatchCondition::StalledAfter(after) => {
            stall_decision(record.armed, thread, after, finished, track, clock.now)
        }
    }
}

/// [`decide`] for a context watch, once its thread is known to be there
fn context_decision(
    armed: bool,
    thread: &ThreadContext,
    threshold: Threshold,
    finished: bool,
) -> Decision<'_> {
    // only a known reading on one side of the limit moves the watch: a missing
    // token count or window, or a reading right at the limit, changes nothing
    let reading = thread.tokens.zip(threshold.limit(thread.context_window));
    let over = reading.is_some_and(|(tokens, limit)| tokens > limit);
    let under = reading.is_some_and(|(tokens, limit)| tokens < limit);

    if finished {
        // re-arming is silent, so a watch that fired before the thread
        // finished still fires when new work takes it over the limit again
        return if !armed && under {
            Decision::Rearm
        } else {
            Decision::Unchanged
        };
    }

    match armed {
        true if over && !thread.state.busy() => Decision::Fire(thread),
        false if under => Decision::Rearm,
        _ => Decision::Unchanged,
    }
}

/// [`decide`] for a stall watch, once its thread is known to be there
///
/// A thread stalls when it is idle, its work is not done, and its last
/// activity is older than the grace. `idle` already leaves out a thread that
/// homebased work or a T3 delegated task still owes a wake, which shows as
/// `waiting`. A fired watch re-arms only on activity: a turn, a wait, or new
/// session activity within the grace, so one stall fires once
///
/// Waiting writes nothing to the session, so after a long wait `last_active`
/// is old. The wake that ends a wait lands a moment after the thread stops
/// showing `waiting`, and one failed T3 read can hide a delegated task's wake
/// too, so the grace also counts from the last scan that saw the thread
/// running or waiting
fn stall_decision<'a>(
    armed: bool,
    thread: &'a ThreadContext,
    after: StallAfter,
    finished: bool,
    track: &mut Track,
    now: DateTime<Utc>,
) -> Decision<'a> {
    if matches!(thread.state, ThreadState::Running | ThreadState::Waiting) {
        track.active_at = Some(now);
    }
    let since = thread.last_active.max(track.active_at);
    let recent = since.map(|last| now - last < after.duration());
    let active = match thread.state {
        ThreadState::Running | ThreadState::Waiting => true,
        ThreadState::Idle => recent == Some(true),
        // a settled thread is finished, and an unknown one tells nothing
        ThreadState::Settled | ThreadState::Archived | ThreadState::Unknown => false,
    };
    let stalled = !finished && thread.state == ThreadState::Idle && recent == Some(false);

    match armed {
        true if stalled => Decision::Fire(thread),
        false if active && !finished => Decision::Rearm,
        _ => Decision::Unchanged,
    }
}

/// What this check changes about one watch, if anything
///
/// `target` and `caller` are what the lookups found, `None` for a lookup
/// that did not answer. A target that did not answer leaves the watch as it
/// was, and so does a caller, whose target is then not checked either
async fn check(
    state: &AppState,
    memory: &mut WatchMemory,
    record: &WatchRecord,
    target: Option<&[ThreadContext]>,
    caller: Option<&[ThreadContext]>,
    clock: Clock,
) -> Option<WatchUpdate> {
    let local = state.machine.identity.machine;
    let id = record.watch.id;
    // a caller this machine cannot read may be archived already, and a remote
    // target must not fire, compact, or end the watch before that is known
    let Some(matches) = caller else {
        memory.caller_unanswered(id);
        memory.target_unanswered(id);
        return None;
    };
    let seen = caller_seen(&record.watch, matches);
    let learned = match seen {
        CallerSeen::T3(thread) if record.watch.caller_t3.is_none() => thread.t3_thread.clone(),
        _ => None,
    };

    let track = memory.callers.entry(id).or_default();
    if caller_gone(seen, track, clock) {
        let thread = match target {
            Some([thread]) => Some(thread),
            _ => None,
        };
        return Some(ended(local, record, WatchEndReason::CallerGone, thread));
    }

    let update = check_target(state, memory, record, target, clock).await;
    // a watch from before `caller_t3` saves the caller's T3 thread the first
    // time a scan finds it, with whatever else the check changed
    match (update, learned) {
        (update, None) => update,
        (Some(update), learned) => Some(WatchUpdate {
            caller_t3: learned,
            ..update
        }),
        (None, learned) => Some(WatchUpdate {
            armed: record.armed,
            events: Vec::new(),
            end: None,
            caller_t3: learned,
        }),
    }
}

/// What this check changes about one watch because of its watched thread
async fn check_target(
    state: &AppState,
    memory: &mut WatchMemory,
    record: &WatchRecord,
    target: Option<&[ThreadContext]>,
    clock: Clock,
) -> Option<WatchUpdate> {
    let local = state.machine.identity.machine;
    let id = record.watch.id;
    let Some(matches) = target else {
        memory.target_unanswered(id);
        return None;
    };
    let track = memory.targets.entry(id).or_default();
    match decide(record, matches, track, clock) {
        Decision::Unchanged => None,
        Decision::Rearm => Some(WatchUpdate {
            armed: true,
            events: Vec::new(),
            end: None,
            caller_t3: None,
        }),
        Decision::End(reason, thread) => Some(ended(local, record, reason, thread)),
        Decision::Fire(thread) => {
            let compaction = match record.watch.action {
                WatchAction::Notify => None,
                WatchAction::Compact => {
                    // the lookups can take seconds on a slow Fleet, and an
                    // `unwatch` meanwhile must not still compact the thread
                    if !still_armed(state, id).await {
                        return None;
                    }
                    Some(
                        compact_resolved(state, thread, Trigger::Watch)
                            .await
                            .unwrap_or_else(|error| {
                                (CompactOutcome::Unavailable, Some(error.to_string()))
                            }),
                    )
                }
            };
            Some(fired(local, &record.watch, thread, compaction.as_ref()))
        }
    }
}

/// Whether the stored watch `id` is still live and armed; a failed read
/// counts as not, so the next scan decides again
async fn still_armed(state: &AppState, id: WatchId) -> bool {
    match call(&state.store, |reply| StoreMsg::GetWatch { id, reply }).await {
        Ok(Some(record)) => record.ended_at.is_none() && record.armed,
        Ok(None) => false,
        Err(error) => {
            warn!(watch = %id, "thread watch re-read: {error}");
            false
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
        caller_t3: None,
    }
}

/// `THREAD_CONTEXT_OVER` or `THREAD_STALLED`, which also ends a `--once` watch
///
/// The event says so itself, so a `--once` watch wakes its caller once and
/// sends no `THREAD_WATCH_ENDED`
fn fired(
    local: MachineId,
    watch: &ThreadWatch,
    thread: &ThreadContext,
    compaction: Option<&(CompactOutcome, Option<String>)>,
) -> WatchUpdate {
    let end = watch.once.then_some(WatchEndReason::Once);
    let kind = watch.condition.fires();
    WatchUpdate {
        armed: false,
        events: vec![(
            kind,
            payload(local, watch, kind, Some(thread), compaction, end),
        )],
        end,
        caller_t3: None,
    }
}

/// The `HOMEBASED_EVENT` payload of one watch event; the store adds `seq`
///
/// `reason` is why the watch ends with this event, if it does
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
    let condition = match watch.condition {
        WatchCondition::ContextAbove(threshold) => json!({
            "context_above": threshold.to_string(),
            "limit": thread.and_then(|thread| threshold.limit(thread.context_window)),
        }),
        WatchCondition::StalledAfter(after) => json!({
            "stalled_after": after.to_string(),
        }),
    };
    let context = thread.map(|thread| {
        json!({
            "tokens": thread.tokens,
            "tokens_estimated": thread.tokens_estimated,
            "context_window": thread.context_window,
            "percent": thread.percent,
            "state": thread.state,
            "task_finished": thread.task_finished,
            "settled": thread.settled,
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
        "condition": condition,
        "action": watch.action,
        "context": context,
        "compaction": compaction.map(|(outcome, detail)| json!({
            "outcome": outcome,
            "detail": detail,
        })),
        "reason": reason.filter(|_| kind == WatchEventKind::Ended),
        "watch_ended": reason.is_some(),
        "end_reason": reason,
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
    let once = reason == Some(WatchEndReason::Once);
    let after = if once {
        "this --once watch ended with this event, so nothing more arrives for it"
    } else {
        "the watch fires again after the context drops below the limit and climbs back"
    };
    let again = format!(
        "Watch {} again with `homebased thread watch` if it still matters",
        watch.reference
    );
    match kind {
        WatchEventKind::ContextOver => match compaction {
            Some((CompactOutcome::Started, _)) => {
                format!("Compaction started; nothing to do, and {after}")
            }
            Some((outcome, detail)) => format!(
                "Compaction did not start ({}{}). Run `homebased thread compact {exact}` once the thread is idle, or hand its work to a fresh thread; {after}",
                outcome.as_str(),
                detail
                    .as_deref()
                    .map(|detail| format!(": {detail}"))
                    .unwrap_or_default()
            ),
            None if thread.is_some_and(|thread| thread.host == ThreadHost::T3) => format!(
                "Compact it with `homebased thread compact {exact}`, or hand its work to a fresh thread; {after}"
            ),
            None => format!(
                "homebased compacts only T3 threads, so tell this session to run /compact or hand its work to a fresh thread; {after}"
            ),
        },
        WatchEventKind::Stalled => {
            let grace = match watch.condition {
                WatchCondition::StalledAfter(after) => after.to_string(),
                WatchCondition::ContextAbove(_) => "the grace".into(),
            };
            let then = if once {
                "This --once watch ended with this event, so nothing more arrives for it".to_owned()
            } else {
                format!(
                    "The watch fires again only after the thread is active and then stalls again. If it already sent its final report, end the watch with `homebased thread unwatch {}`",
                    watch.id
                )
            };
            format!(
                "The thread has sat idle for over {grace} with no turn running and nothing due to wake it, so it may have ended its turn without its report or a registered wait. Read its last turn with `t3_thread_read` or `homebased thread context {exact}`, then send it a message to carry on or report, or hand its work to a fresh thread. {then}"
            )
        }
        WatchEventKind::Ended => match reason {
            Some(WatchEndReason::Once) => {
                "The --once watch fired and ended; nothing more will arrive for it".into()
            }
            Some(WatchEndReason::Unwatched) => {
                "The watch was removed; nothing more will arrive for it".into()
            }
            Some(WatchEndReason::NoContextWindow) => format!(
                "The watch ended because the thread runs Claude Code, which records no context window for a percent limit. Watch {} again with a token count, such as `--context-above 150k`{}",
                watch.reference,
                match watch.action {
                    WatchAction::Compact => ", and keep `--action compact`",
                    WatchAction::Notify => "",
                }
            ),
            Some(WatchEndReason::Finished) => format!(
                "The watch ended because the thread is done: it stayed settled, or idle after its delegated task ended, with no activity for an hour. Nothing to do; if it takes new work, watch {} again with `homebased thread watch`",
                watch.reference
            ),
            Some(WatchEndReason::CallerGone) => {
                "The watch ended because the thread it reports to was archived or deleted, or stayed done with no activity for an hour; nothing more will arrive for it".into()
            }
            Some(WatchEndReason::Archived) => {
                format!("The watch ended because the T3 thread was archived. {again}")
            }
            Some(WatchEndReason::Deleted) => {
                format!("The watch ended because the T3 thread no longer exists. {again}")
            }
            Some(WatchEndReason::SessionGone) | None => format!(
                "The watch ended because the session has no transcript or rollout any more. {again}"
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{DateTime, TimeDelta, Utc};

    use super::{
        CallerSeen, Clock, Decision, QUIET_FINISHED, Track, WatchMemory, caller_gone, caller_seen,
        decide, ended, fired,
    };
    use crate::domain::{TaskEnv, ThreadId};
    use crate::machine::MachineId;
    use crate::store::{Store, WatchRecord, WatchUpdate};
    use crate::submission::{CallbackContext, CallbackExecutable};
    use crate::t3::ThreadDetails;
    use crate::thread_ref::{ThreadContext, ThreadHost, ThreadState};
    use crate::thread_watch::{
        StallAfter, ThreadWatch, Threshold, WatchAction, WatchCondition, WatchEndReason,
        WatchEventKind, WatchId, WatchTarget,
    };

    fn watch(caller: ThreadId, target: WatchTarget, once: bool) -> ThreadWatch {
        ThreadWatch {
            id: WatchId::new(),
            caller,
            caller_t3: None,
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
            condition: WatchCondition::ContextAbove(Threshold::Tokens(100_000)),
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
            session_exists: None,
            t3_thread: Some("mcp:launch:feed".into()),
            t3_link: None,
            title: Some("child".into()),
            state,
            task_finished: false,
            settled: state == ThreadState::Settled,
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

    fn t3_target(machine: MachineId) -> WatchTarget {
        WatchTarget::T3 {
            machine,
            t3_thread: "mcp:launch:feed".into(),
        }
    }

    fn record(watch: ThreadWatch, armed: bool) -> WatchRecord {
        WatchRecord {
            watch,
            armed,
            seq: 0,
            ended_at: None,
            end_reason: None,
        }
    }

    fn start() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-08T12:00:00Z")
            .unwrap()
            .to_utc()
    }

    fn at(minutes: i64) -> Clock {
        Clock {
            now: start() + TimeDelta::minutes(minutes),
            quiet: QUIET_FINISHED,
        }
    }

    /// One check at a time, remembering each watch's track the way the scan does
    #[derive(Default)]
    struct Scans(HashMap<WatchId, Track>);

    impl Scans {
        fn decide<'a>(
            &mut self,
            record: &WatchRecord,
            now: &'a [ThreadContext],
            clock: Clock,
        ) -> Decision<'a> {
            decide(
                record,
                now,
                self.0.entry(record.watch.id).or_default(),
                clock,
            )
        }

        /// Run one check against the store, and return the kinds of the
        /// events it added
        fn step(
            &mut self,
            store: &Store,
            local: MachineId,
            id: WatchId,
            now: &[ThreadContext],
        ) -> Vec<WatchEventKind> {
            self.step_at(store, local, id, now, at(0))
        }

        /// [`Self::step`] at the time `clock` says
        fn step_at(
            &mut self,
            store: &Store,
            local: MachineId,
            id: WatchId,
            now: &[ThreadContext],
            clock: Clock,
        ) -> Vec<WatchEventKind> {
            let record: WatchRecord = store.watch_record(id).unwrap().unwrap();
            let update = match self.decide(&record, now, clock) {
                Decision::Unchanged => return Vec::new(),
                Decision::Rearm => WatchUpdate {
                    armed: true,
                    events: Vec::new(),
                    end: None,
                    caller_t3: None,
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
    }

    #[test]
    fn a_stall_watch_fires_once_per_idle_stretch_and_never_on_a_waiting_or_done_thread() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("state.db")).unwrap();
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let mut stall = watch(caller, t3_target(local), false);
        stall.condition = WatchCondition::StalledAfter("20m".parse::<StallAfter>().unwrap());
        // a context watch on the same thread is a second watch
        let (context, _) = store
            .upsert_watch(&watch(caller, t3_target(local), false))
            .unwrap();
        let (record, created) = store.upsert_watch(&stall).unwrap();
        assert!(created);
        assert_ne!(record.watch.id, context.watch.id);
        let id = record.watch.id;
        let mut scans = Scans::default();

        let seen = |state, active_at: i64| {
            let mut thread = thread(local, 10, state);
            thread.last_active = Some(start() + TimeDelta::minutes(active_at));
            [thread]
        };
        let mut step =
            |now: &[ThreadContext], minute| scans.step_at(&store, local, id, now, at(minute));
        assert!(step(&seen(ThreadState::Idle, 0), 10).is_empty());
        assert_eq!(
            step(&seen(ThreadState::Idle, 0), 25),
            [WatchEventKind::Stalled]
        );
        // the same idle stretch fires once
        assert!(step(&seen(ThreadState::Idle, 0), 40).is_empty());
        // activity re-arms it silently, and the next stall fires again
        assert!(step(&seen(ThreadState::Running, 45), 45).is_empty());
        assert!(step(&seen(ThreadState::Idle, 46), 60).is_empty());
        assert_eq!(
            step(&seen(ThreadState::Idle, 46), 70),
            [WatchEventKind::Stalled]
        );
        // homebased work or a delegated task owes it a wake, so it is no
        // stall; a long wait writes nothing, so the wake that ends it finds an
        // old `last_active`, and the grace counts from the last scan that saw
        // it waiting
        assert!(step(&seen(ThreadState::Waiting, 46), 80).is_empty());
        assert!(step(&seen(ThreadState::Waiting, 46), 120).is_empty());
        assert!(step(&seen(ThreadState::Idle, 46), 121).is_empty());
        assert!(step(&seen(ThreadState::Idle, 46), 135).is_empty());
        assert_eq!(
            step(&seen(ThreadState::Idle, 46), 141),
            [WatchEventKind::Stalled]
        );

        let event = store.settled_watch_events(id).unwrap();
        assert!(event.is_empty(), "nothing settles events here");
        let pending = store.pending_watch_events().unwrap();
        let (_, first) = pending.iter().find(|(watch, _)| watch.id == id).unwrap();
        assert_eq!(first.payload["event"], "THREAD_STALLED");
        assert_eq!(first.payload["condition"]["stalled_after"], "20m");
        assert!(first.wakes_caller(None));
        let next = first.payload["next_action"].as_str().unwrap();
        assert!(
            next.contains(&format!("homebased thread unwatch {id}")),
            "{next}"
        );

        // a delegate whose task ended, or a settled thread, owes nothing
        for finish in [
            |thread: &mut ThreadContext| thread.task_finished = true,
            |thread: &mut ThreadContext| {
                thread.settled = true;
                thread.state = ThreadState::Settled;
            },
        ] {
            let mut quiet = watch(caller, t3_target(local), false);
            quiet.condition = stall.condition;
            quiet.reference = format!("quiet-{}", WatchId::new());
            quiet.target = WatchTarget::T3 {
                machine: local,
                t3_thread: quiet.reference.clone(),
            };
            let (record, _) = store.upsert_watch(&quiet).unwrap();
            let mut done = seen(ThreadState::Idle, 0);
            finish(&mut done[0]);
            let mut scans = Scans::default();
            assert!(
                scans
                    .step_at(&store, local, record.watch.id, &done, at(30))
                    .is_empty()
            );
            assert_eq!(
                scans.step_at(&store, local, record.watch.id, &done, at(95)),
                [WatchEventKind::Ended]
            );
            let ended = store.watch_record(record.watch.id).unwrap().unwrap();
            assert_eq!(ended.end_reason, Some(WatchEndReason::Finished));
        }
    }

    #[test]
    fn a_watch_fires_once_per_climb_while_idle_and_ends_with_its_thread() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("state.db")).unwrap();
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let target = t3_target(local);
        let (first, created) = store
            .upsert_watch(&watch(caller, target.clone(), false))
            .unwrap();
        assert!(created);
        // the same watch again is the existing one
        let (second, created) = store
            .upsert_watch(&watch(caller, target.clone(), false))
            .unwrap();
        assert!(!created);
        assert_eq!(second.watch.id, first.watch.id);
        let id = first.watch.id;
        let mut scans = Scans::default();

        let over = |state| [thread(local, 150_000, state)];
        let below = [thread(local, 40_000, ThreadState::Idle)];
        assert!(scans.step(&store, local, id, &below).is_empty());
        // a running turn holds the event until the thread is idle
        assert!(
            scans
                .step(&store, local, id, &over(ThreadState::Running))
                .is_empty()
        );
        assert_eq!(
            scans.step(&store, local, id, &over(ThreadState::Waiting)),
            [WatchEventKind::ContextOver]
        );
        // still over the limit: no second event for the same climb
        assert!(
            scans
                .step(&store, local, id, &over(ThreadState::Idle))
                .is_empty()
        );
        assert!(scans.step(&store, local, id, &below).is_empty());
        assert_eq!(
            scans.step(&store, local, id, &over(ThreadState::Idle)),
            [WatchEventKind::ContextOver]
        );
        // one scan that finds the thread gone may be a bad read
        assert!(scans.step(&store, local, id, &[]).is_empty());
        assert_eq!(scans.step(&store, local, id, &[]), [WatchEventKind::Ended]);
        let record = store.watch_record(id).unwrap().unwrap();
        assert_eq!(record.end_reason, Some(WatchEndReason::Deleted));
        // an ended watch changes no more
        assert!(
            scans
                .step(&store, local, id, &over(ThreadState::Idle))
                .is_empty()
        );

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
        assert_eq!(settled[0].payload["watch_ended"], false);
        assert_eq!(settled[2].payload["reason"], "deleted");
        assert_eq!(settled[2].payload["watch_ended"], true);
        assert!(!settled[0].ends_watch());
        assert!(settled[2].ends_watch());

        // a finished watch frees its target: watching it again makes a new watch
        let (fresh, created) = store.upsert_watch(&watch(caller, target, true)).unwrap();
        assert!(created);
        assert_ne!(fresh.watch.id, id);
        // a --once watch ends with the event that fires it, and sends no other
        assert_eq!(
            scans.step(&store, local, fresh.watch.id, &over(ThreadState::Idle)),
            [WatchEventKind::ContextOver]
        );
        let record = store.watch_record(fresh.watch.id).unwrap().unwrap();
        assert_eq!(record.end_reason, Some(WatchEndReason::Once));
        let events = store.pending_watch_events().unwrap();
        let (_, last) = events
            .iter()
            .find(|(watch, _)| watch.id == fresh.watch.id)
            .unwrap();
        assert_eq!(
            (&last.payload["watch_ended"], &last.payload["end_reason"]),
            (&serde_json::json!(true), &serde_json::json!("once"))
        );
        assert!(last.ends_watch());
        store
            .settle_watch_event(fresh.watch.id, 1, "delivered", Utc::now())
            .unwrap();
        assert!(
            store
                .pending_watch_events()
                .unwrap()
                .iter()
                .all(|(watch, _)| watch.id != fresh.watch.id)
        );
    }

    #[test]
    fn a_scan_that_cannot_look_breaks_a_run_of_missing_reads() {
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let watched = record(watch(caller, t3_target(local), false), true);
        let id = watched.watch.id;
        let mut memory = WatchMemory::new(QUIET_FINISHED);
        let target = |memory: &mut WatchMemory, minute| {
            decide(
                &watched,
                &[],
                memory.targets.entry(id).or_default(),
                at(minute),
            )
        };

        // gone, then a lookup that failed, then gone again is not two in a row
        assert_eq!(target(&mut memory, 0), Decision::Unchanged);
        memory.target_unanswered(id);
        assert_eq!(target(&mut memory, 2), Decision::Unchanged);
        assert_eq!(
            target(&mut memory, 3),
            Decision::End(WatchEndReason::Deleted, None)
        );

        // the same holds for a caller found gone around a failed lookup
        let caller_scan = |memory: &mut WatchMemory, minute| {
            caller_gone(
                CallerSeen::Gone,
                memory.callers.entry(id).or_default(),
                at(minute),
            )
        };
        assert!(!caller_scan(&mut memory, 0));
        memory.caller_unanswered(id);
        assert!(!caller_scan(&mut memory, 2));
        assert!(caller_scan(&mut memory, 3));
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
        let t3 = t3_target(local);
        let session_watch = record(watch(caller, session.clone(), false), true);
        let t3_watch = record(watch(caller, t3, false), true);
        let mut scans = Scans::default();

        assert_eq!(
            scans.decide(&session_watch, &[], at(0)),
            Decision::Unchanged
        );
        assert_eq!(
            scans.decide(&session_watch, &[], at(1)),
            Decision::End(WatchEndReason::SessionGone, None)
        );
        // archiving is a deliberate act that no bad read fakes, so it ends at once
        let archived = [thread(local, 10, ThreadState::Archived)];
        assert_eq!(
            scans.decide(&t3_watch, &archived, at(0)),
            Decision::End(WatchEndReason::Archived, Some(&archived[0]))
        );
        // a session watch follows the session, whatever its T3 thread does
        let mut scans = Scans::default();
        assert_eq!(
            scans.decide(&session_watch, &archived, at(0)),
            Decision::Unchanged
        );

        // T3 still names the session, but its transcript is gone
        let mut removed = thread(local, 10, ThreadState::Idle);
        removed.session_exists = Some(false);
        let removed = [removed];
        assert_eq!(
            scans.decide(&session_watch, &removed, at(0)),
            Decision::Unchanged
        );
        assert_eq!(
            scans.decide(&session_watch, &removed, at(1)),
            Decision::End(WatchEndReason::SessionGone, Some(&removed[0]))
        );
        // a T3 watch moves on to the thread's next session instead
        assert_eq!(
            scans.decide(&t3_watch, &removed, at(0)),
            Decision::Unchanged
        );
        // a peer that predates the field cannot say, so the watch goes on
        let mut unknown = removed[0].clone();
        unknown.session_exists = None;
        let mut scans = Scans::default();
        for minute in 0..3 {
            assert_eq!(
                scans.decide(&session_watch, std::slice::from_ref(&unknown), at(minute)),
                Decision::Unchanged
            );
        }
    }

    #[test]
    fn a_thread_found_again_between_two_missing_reads_keeps_its_watch() {
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let watched = record(watch(caller, t3_target(local), false), true);
        let here = [thread(local, 10, ThreadState::Idle)];
        let mut scans = Scans::default();

        assert_eq!(scans.decide(&watched, &[], at(0)), Decision::Unchanged);
        assert_eq!(scans.decide(&watched, &here, at(1)), Decision::Unchanged);
        // the count starts over once the thread is seen again
        assert_eq!(scans.decide(&watched, &[], at(2)), Decision::Unchanged);
        assert_eq!(
            scans.decide(&watched, &[], at(3)),
            Decision::End(WatchEndReason::Deleted, None)
        );
    }

    #[test]
    fn a_finished_target_never_fires_and_its_watch_ends_after_an_hour_of_quiet() {
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let mut compact = watch(caller, t3_target(local), false);
        compact.action = WatchAction::Compact;
        let armed = record(compact, true);
        let settled = [thread(local, 204_000, ThreadState::Settled)];
        let delegate_done = [{
            let mut done = thread(local, 204_000, ThreadState::Idle);
            done.task_finished = true;
            done
        }];
        let mut scans = Scans::default();

        // over the limit and armed, but its work is done, so nothing compacts it
        assert_eq!(scans.decide(&armed, &settled, at(0)), Decision::Unchanged);
        assert_eq!(
            scans.decide(&armed, &delegate_done, at(1)),
            Decision::Unchanged
        );
        // unsettled, it is a live thread again and fires
        let unsettled = [thread(local, 204_000, ThreadState::Idle)];
        assert_eq!(
            scans.decide(&armed, &unsettled, at(2)),
            Decision::Fire(&unsettled[0])
        );
        // a task that ended while its run still finishes up is not done yet
        let running = [{
            let mut running = thread(local, 204_000, ThreadState::Running);
            running.task_finished = true;
            running
        }];
        assert_eq!(scans.decide(&armed, &running, at(3)), Decision::Unchanged);

        // quiet and finished for the full hour ends the watch
        let mut scans = Scans::default();
        assert_eq!(scans.decide(&armed, &settled, at(0)), Decision::Unchanged);
        assert_eq!(scans.decide(&armed, &settled, at(59)), Decision::Unchanged);
        assert_eq!(
            scans.decide(&armed, &settled, at(60)),
            Decision::End(WatchEndReason::Finished, Some(&settled[0]))
        );

        // new tokens or activity start the clock over, and so does new work
        let mut scans = Scans::default();
        assert_eq!(scans.decide(&armed, &settled, at(0)), Decision::Unchanged);
        let grew = [thread(local, 205_000, ThreadState::Settled)];
        assert_eq!(scans.decide(&armed, &grew, at(30)), Decision::Unchanged);
        assert_eq!(scans.decide(&armed, &grew, at(89)), Decision::Unchanged);
        let active = [{
            let mut active = grew[0].clone();
            active.last_active = Some(start() + TimeDelta::minutes(80));
            active
        }];
        assert_eq!(scans.decide(&armed, &active, at(90)), Decision::Unchanged);
        assert_eq!(
            scans.decide(
                &armed,
                &[thread(local, 205_000, ThreadState::Running)],
                at(100)
            ),
            Decision::Unchanged
        );
        assert_eq!(scans.decide(&armed, &active, at(101)), Decision::Unchanged);
        assert_eq!(scans.decide(&armed, &active, at(160)), Decision::Unchanged);
        assert_eq!(
            scans.decide(&armed, &active, at(161)),
            Decision::End(WatchEndReason::Finished, Some(&active[0]))
        );

        // a watch by session reads the T3 thread that runs the session, and
        // follows the same rule, while a session outside T3 is never done
        let session = WatchTarget::Session {
            machine: local,
            provider: crate::thread_ref::Provider::Codex,
            session: ThreadId(uuid::Uuid::now_v7()),
        };
        let by_session = record(watch(caller, session, false), true);
        let mut scans = Scans::default();
        assert_eq!(
            scans.decide(&by_session, &settled, at(0)),
            Decision::Unchanged
        );
        assert_eq!(
            scans.decide(&by_session, &delegate_done, at(30)),
            Decision::Unchanged
        );
        assert_eq!(
            scans.decide(&by_session, &delegate_done, at(90)),
            Decision::End(WatchEndReason::Finished, Some(&delegate_done[0]))
        );
        let terminal = [{
            let mut terminal = thread(local, 204_000, ThreadState::Idle);
            terminal.host = ThreadHost::Terminal;
            terminal.t3_thread = None;
            terminal
        }];
        assert_eq!(
            scans.decide(&by_session, &terminal, at(91)),
            Decision::Fire(&terminal[0])
        );
    }

    #[test]
    fn a_done_thread_that_homebased_work_still_owes_events_never_fires() {
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let mut compact = watch(caller, t3_target(local), false);
        compact.action = WatchAction::Compact;
        let armed = record(compact, true);
        // a pending homebased task shows the thread as waiting, over T3's own records
        let waiting = |task_finished, settled| {
            let mut waiting = thread(local, 204_000, ThreadState::Waiting);
            waiting.task_finished = task_finished;
            waiting.settled = settled;
            waiting
        };

        for done in [[waiting(true, false)], [waiting(false, true)]] {
            let mut scans = Scans::default();
            assert_eq!(scans.decide(&armed, &done, at(0)), Decision::Unchanged);
            assert_eq!(
                scans.decide(&armed, &done, at(60)),
                Decision::End(WatchEndReason::Finished, Some(&done[0]))
            );
        }
        // waiting with work T3 still runs is no done thread
        let working = [waiting(false, false)];
        assert_eq!(
            Scans::default().decide(&armed, &working, at(0)),
            Decision::Fire(&working[0])
        );
    }

    #[test]
    fn a_finished_target_rearms_silently_so_new_work_fires_again() {
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let fired = record(watch(caller, t3_target(local), false), false);
        let done = |tokens| {
            let mut done = thread(local, tokens, ThreadState::Idle);
            done.task_finished = true;
            done
        };
        let mut scans = Scans::default();

        // compacted and then finished: below the limit it re-arms, sending nothing
        let compacted = [done(40_000)];
        assert_eq!(scans.decide(&fired, &compacted, at(0)), Decision::Rearm);
        // over the limit while still done, it neither fires nor re-arms
        let armed = record(fired.watch.clone(), true);
        let over = [done(204_000)];
        assert_eq!(scans.decide(&armed, &over, at(1)), Decision::Unchanged);
        assert_eq!(scans.decide(&fired, &over, at(2)), Decision::Unchanged);
        // new work takes it over the limit within the hour, and it fires
        let working = [thread(local, 204_000, ThreadState::Idle)];
        assert_eq!(
            scans.decide(&armed, &working, at(10)),
            Decision::Fire(&working[0])
        );
    }

    #[test]
    fn a_watch_ends_when_its_caller_is_archived_deleted_or_done_and_quiet() {
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let mut known = watch(caller, t3_target(local), false);
        known.caller_t3 = Some("mcp:launch:caller".into());
        let older = watch(caller, t3_target(local), false);
        let t3_caller = |state| {
            let mut found = thread(local, 80_000, state);
            found.t3_thread = Some("mcp:launch:caller".into());
            found
        };
        let gone =
            |seen: CallerSeen<'_>, track: &mut Track, minute| caller_gone(seen, track, at(minute));

        // archived ends at once
        let archived = [t3_caller(ThreadState::Archived)];
        assert_eq!(caller_seen(&known, &archived), CallerSeen::T3(&archived[0]));
        assert!(gone(
            caller_seen(&known, &archived),
            &mut Track::default(),
            0
        ));

        // a deleted caller is no match, and ends only on the second scan
        let mut track = Track::default();
        assert_eq!(caller_seen(&known, &[]), CallerSeen::Gone);
        assert!(!gone(caller_seen(&known, &[]), &mut track, 0));
        assert!(gone(caller_seen(&known, &[]), &mut track, 1));

        // settled and quiet for an hour, the caller will read nothing more
        let settled = [t3_caller(ThreadState::Settled)];
        let mut track = Track::default();
        assert!(!gone(caller_seen(&known, &settled), &mut track, 0));
        assert!(!gone(caller_seen(&known, &settled), &mut track, 59));
        assert!(gone(caller_seen(&known, &settled), &mut track, 60));
        // a caller that is busy or idle is not done
        let mut track = Track::default();
        for (minute, state) in [(0, ThreadState::Idle), (90, ThreadState::Waiting)] {
            assert!(!gone(
                caller_seen(&known, &[t3_caller(state)]),
                &mut track,
                minute
            ));
        }

        // a watch from before `caller_t3` finds the caller by its session,
        // which a T3 thread names while it runs it
        assert_eq!(caller_seen(&older, &archived), CallerSeen::T3(&archived[0]));
        // a session no T3 thread runs is never ended for its caller
        let terminal = [{
            let mut terminal = thread(local, 10, ThreadState::Idle);
            terminal.host = ThreadHost::Terminal;
            terminal.t3_thread = None;
            terminal
        }];
        let mut track = Track::default();
        for minute in [0, 1, 120] {
            assert_eq!(caller_seen(&older, &terminal), CallerSeen::Untracked);
            assert_eq!(caller_seen(&older, &[]), CallerSeen::Untracked);
            assert!(!gone(caller_seen(&older, &[]), &mut track, minute));
        }
    }

    #[test]
    fn a_fired_watch_rearms_only_on_a_known_reading_below_its_limit() {
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let target = t3_target(local);
        let with = |threshold, armed| {
            let mut watch = watch(caller, target.clone(), false);
            watch.condition = WatchCondition::ContextAbove(threshold);
            record(watch, armed)
        };
        let reading = |tokens: Option<u64>, window: Option<u64>| {
            let mut thread = thread(local, 0, ThreadState::Idle);
            thread.tokens = tokens;
            thread.context_window = window;
            [thread]
        };
        let fired = with(Threshold::Tokens(100_000), false);
        let mut scans = Scans::default();
        let mut decide = |record: &WatchRecord, now: &[ThreadContext]| {
            // a reading cannot borrow past the call, so only the kind is kept
            matches!(scans.decide(record, now, at(0)), Decision::Rearm)
        };

        // a transcript with no request yet says nothing about the size
        assert!(!decide(&fired, &reading(None, None)));
        // right at the limit is neither over nor below it
        assert!(!decide(&fired, &reading(Some(100_000), None)));
        // a percent limit needs the window to compare against
        let by_percent = with(Threshold::Percent(50), false);
        assert!(!decide(&by_percent, &reading(Some(10), None)));
        assert!(decide(&by_percent, &reading(Some(10), Some(200_000))));
        assert!(decide(&fired, &reading(Some(99_999), None)));
        let armed = with(Threshold::Tokens(100_000), true);
        let at_limit = reading(Some(100_000), None);
        assert_eq!(
            Scans::default().decide(&armed, &at_limit, at(0)),
            Decision::Unchanged
        );
    }

    #[test]
    fn a_percent_watch_ends_once_its_thread_runs_claude() {
        let local = MachineId::new();
        let mut watch = watch(ThreadId(uuid::Uuid::now_v7()), t3_target(local), false);
        watch.condition = WatchCondition::ContextAbove(Threshold::Percent(80));
        let record = record(watch, true);
        let mut scans = Scans::default();
        // no session yet, so no provider: the watch waits for the first turn
        let fresh = [thread(local, 0, ThreadState::Idle)];
        assert_eq!(scans.decide(&record, &fresh, at(0)), Decision::Unchanged);
        let mut claude = fresh[0].clone();
        claude.provider = Some(crate::thread_ref::Provider::Claude);
        let claude = [claude];
        assert_eq!(
            scans.decide(&record, &claude, at(1)),
            Decision::End(WatchEndReason::NoContextWindow, Some(&claude[0]))
        );
        let update = ended(
            local,
            &record,
            WatchEndReason::NoContextWindow,
            Some(&claude[0]),
        );
        let payload = &update.events[0].1;
        assert_eq!(payload["reason"], "no_context_window");
        assert!(
            payload["next_action"]
                .as_str()
                .unwrap()
                .contains("--context-above 150k"),
            "{payload}"
        );
    }

    #[test]
    fn a_notify_and_a_compact_watch_on_one_thread_fire_on_their_own() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("state.db")).unwrap();
        let local = MachineId::new();
        let caller = ThreadId(uuid::Uuid::now_v7());
        let target = t3_target(local);
        let mut notify = watch(caller, target.clone(), false);
        notify.condition = WatchCondition::ContextAbove(Threshold::Tokens(200_000));
        let mut compact = watch(caller, target, false);
        compact.condition = WatchCondition::ContextAbove(Threshold::Tokens(250_000));
        compact.action = WatchAction::Compact;
        let (notify, created) = store.upsert_watch(&notify).unwrap();
        assert!(created);
        let (compact, created) = store.upsert_watch(&compact).unwrap();
        assert!(created, "another limit and action is another watch");
        assert_ne!(notify.watch.id, compact.watch.id);
        // a retry of either one is that watch
        assert!(!store.upsert_watch(&notify.watch).unwrap().1);
        assert!(!store.upsert_watch(&compact.watch).unwrap().1);

        let mut scans = Scans::default();
        let at = |tokens| [thread(local, tokens, ThreadState::Idle)];
        let mut both = |tokens| {
            (
                scans.step(&store, local, notify.watch.id, &at(tokens)),
                scans.step(&store, local, compact.watch.id, &at(tokens)),
            )
        };
        let over = vec![WatchEventKind::ContextOver];
        // one climb past both limits fires each watch once
        assert_eq!(both(260_000), (over.clone(), over.clone()));
        assert_eq!(both(260_000), (vec![], vec![]));
        // the compaction drops the context below both, which re-arms both
        assert_eq!(both(30_000), (vec![], vec![]));
        // the next climb past only the lower limit fires only the notify watch
        assert_eq!(both(210_000), (over.clone(), vec![]));
        assert_eq!(both(255_000), (vec![], over));
    }
}
