//! Pure scheduling decisions for one machine queue
//!
//! The queue actor builds a [`Snapshot`] from the store, calls [`decide`], and
//! carries out the decisions: each launch reserves a run in one transaction,
//! a preemption commits its stop before it signals the run, and the blocked
//! head drives the notice timer. Nothing here performs I/O

use std::cmp::Reverse;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::domain::TaskId;

use super::{
    ActiveRun, AttentionId, JobId, PreemptStop, Preemption, Priority, ResourceId, ResourceName,
    RunPhase, StopCause, Target,
};

/// One resource and its active run, if any
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceView {
    /// Resource identity
    pub id: ResourceId,
    /// Name, which orders free resources for `Any` jobs
    pub name: ResourceName,
    /// The resource's single active run; `None` means idle
    pub run: Option<ActiveRun>,
}

/// Whether a job waits in the queue or one of its runs holds a resource
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuedState {
    /// Waiting for a resource
    Queued,
    /// Its run holds this resource
    Active(ResourceId),
}

/// One non-terminal job of the machine queue
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobView {
    /// Job identity
    pub id: JobId,
    /// Current level, which may differ from the submitted one after a move
    pub priority: Priority,
    /// Resources the job may use
    pub target: Target,
    /// How its runs give up their resource
    pub preempt: Preemption,
    /// Whether it waits or runs
    pub state: QueuedState,
}

/// Notice thresholds from `[resource.notify_blocked_after]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoticeThresholds {
    /// Wait behind a run asked to yield
    pub after_yield: Duration,
    /// Wait behind anything else, including `wait` runs and `Attention`
    pub after_wait: Duration,
}

impl Default for NoticeThresholds {
    fn default() -> Self {
        Self {
            after_yield: Duration::from_secs(15 * 60),
            after_wait: Duration::from_secs(30 * 60),
        }
    }
}

/// The stored blocking episode of the machine queue
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredEpisode {
    /// Head that is blocked
    pub job: JobId,
    /// When the episode started
    pub blocked_since: DateTime<Utc>,
    /// Whether its one notice was already sent
    pub notified: bool,
}

/// Everything one scheduling decision reads
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Decision time
    pub now: DateTime<Utc>,
    /// Every resource of the machine with its active run
    pub resources: Vec<ResourceView>,
    /// Every non-terminal job of the machine queue, in serving order
    pub queue: Vec<JobView>,
    /// The open blocking episode, if any
    pub episode: Option<StoredEpisode>,
    /// Notice thresholds
    pub thresholds: NoticeThresholds,
}

/// Start `job` on `resource`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Launch {
    /// Job to start
    pub job: JobId,
    /// Free resource it may use
    pub resource: ResourceId,
}

/// Stop one executing run for the head
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Preempt {
    /// Resource whose run stops
    pub resource: ResourceId,
    /// Job of that run
    pub job: JobId,
    /// Exact run task, which the stop must name
    pub task: TaskId,
    /// Cause to commit before signalling the run
    pub cause: StopCause,
}

/// What keeps the blocked head from starting
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Blocker {
    /// A run holds one of the head's resources
    Run {
        /// Resource it holds
        resource: ResourceId,
        /// Its job
        job: JobId,
        /// Its task
        task: TaskId,
    },
    /// One of the head's resources waits for a person's release
    Attention {
        /// Resource in `Attention`
        resource: ResourceId,
        /// Identity a release must name
        attention: AttentionId,
    },
}

/// The head of the queue when it cannot start, and its notice timing
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedHead {
    /// The first queued job that could not start
    pub job: JobId,
    /// Start of its episode: the stored one for the same head, otherwise now
    pub blocked_since: DateTime<Utc>,
    /// Whether this is a new episode the store must record
    pub new_episode: bool,
    /// Threshold in force: shorter when a run was asked to yield for it
    pub threshold: Duration,
    /// When the notice falls due
    pub due_at: DateTime<Utc>,
    /// Whether the notice is due now and was not yet sent
    pub send_now: bool,
    /// What blocks it
    pub blockers: Vec<Blocker>,
}

/// What the actor should do now
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Decisions {
    /// Runs to reserve and start, in serving order
    pub launches: Vec<Launch>,
    /// At most one stop for the head
    pub preemption: Option<Preempt>,
    /// The blocked head; `None` ends any open episode
    pub blocked: Option<BlockedHead>,
}

/// Decide launches, at most one preemption, and the blocked head
///
/// 1. Fill free resources: walk the queued jobs in serving order and start
///    each on a free resource it may use, its pinned resource or, for `Any`,
///    the free resource with the lowest name. A job whose resources are all
///    busy is skipped, so it never blocks a job behind it from another
///    resource. A job whose previous run is still being cleaned up, on any
///    resource, waits for that cleanup, since leftovers of that run may still
///    write to its job directory
/// 2. Preempt for the head, the first job still queued: among its resources,
///    pick one executing run of a strictly lower level whose mode allows a
///    stop now, unless one of those resources is already stopping. The lowest
///    level goes first, then the cheapest stop, then the most recent start
#[must_use]
pub fn decide(snapshot: &Snapshot) -> Decisions {
    let mut free: Vec<&ResourceView> = snapshot
        .resources
        .iter()
        .filter(|resource| resource.run.is_none())
        .collect();
    free.sort_by(|left, right| left.name.cmp(&right.name));

    let mut launches = Vec::new();
    let mut head = None;
    for job in snapshot.queue.iter().filter(|job| startable(snapshot, job)) {
        let Some(index) = free
            .iter()
            .position(|resource| job.target.allows(resource.id))
        else {
            head.get_or_insert(job);
            continue;
        };
        let resource = free.remove(index);
        launches.push(Launch {
            job: job.id,
            resource: resource.id,
        });
    }

    let preemption = head.and_then(|head| preempt_for(snapshot, head));
    let blocked = head.map(|head| blocked_head(snapshot, head, preemption.as_ref()));
    Decisions {
        launches,
        preemption,
        blocked,
    }
}

/// A queued job whose previous run has finished its cleanup
fn startable(snapshot: &Snapshot, job: &JobView) -> bool {
    job.state == QueuedState::Queued
        && !snapshot
            .resources
            .iter()
            .filter_map(|resource| resource.run.as_ref())
            .any(|run| run.job == job.id)
}

fn preempt_for(snapshot: &Snapshot, head: &JobView) -> Option<Preempt> {
    let allowed = || {
        snapshot
            .resources
            .iter()
            .filter(|resource| head.target.allows(resource.id))
            .filter_map(|resource| resource.run.as_ref())
    };
    if allowed().any(|run| matches!(run.phase, RunPhase::Stopping { .. })) {
        return None;
    }
    let candidates = allowed().filter_map(|run| {
        let RunPhase::Executing { started_at } = run.phase else {
            return None;
        };
        let job = snapshot.queue.iter().find(|job| job.id == run.job)?;
        if job.priority >= head.priority {
            return None;
        }
        let stop = job.preempt.stop_at(started_at, snapshot.now)?;
        Some((job.priority, stop, Reverse(started_at), run))
    });
    let (_, stop, _, run) = candidates
        .min_by_key(|(level, stop, started, run)| (*level, *stop, *started, run.resource))?;
    Some(Preempt {
        resource: run.resource,
        job: run.job,
        task: run.task,
        cause: match stop {
            PreemptStop::Restart => StopCause::Restart,
            PreemptStop::Yield => StopCause::Yield,
        },
    })
}

fn blocked_head(snapshot: &Snapshot, head: &JobView, preemption: Option<&Preempt>) -> BlockedHead {
    let mut blockers = Vec::new();
    let mut asked_to_yield = preemption.is_some_and(|stop| stop.cause == StopCause::Yield);
    for resource in snapshot
        .resources
        .iter()
        .filter(|resource| head.target.allows(resource.id))
    {
        let Some(run) = &resource.run else {
            continue;
        };
        if let RunPhase::Attention { id, .. } = &run.phase {
            blockers.push(Blocker::Attention {
                resource: resource.id,
                attention: *id,
            });
            continue;
        }
        asked_to_yield |= matches!(
            run.phase,
            RunPhase::Stopping {
                cause: StopCause::Yield,
                ..
            }
        );
        blockers.push(Blocker::Run {
            resource: resource.id,
            job: run.job,
            task: run.task,
        });
    }

    let stored = snapshot.episode.filter(|episode| episode.job == head.id);
    let blocked_since = stored.map_or(snapshot.now, |episode| episode.blocked_since);
    let threshold = if asked_to_yield {
        snapshot.thresholds.after_yield
    } else {
        snapshot.thresholds.after_wait
    };
    let due_at = chrono::Duration::from_std(threshold)
        .ok()
        .and_then(|threshold| blocked_since.checked_add_signed(threshold))
        .unwrap_or(DateTime::<Utc>::MAX_UTC);
    BlockedHead {
        job: head.id,
        blocked_since,
        new_episode: stored.is_none(),
        threshold,
        due_at,
        send_now: snapshot.now >= due_at && !stored.is_some_and(|episode| episode.notified),
        blockers,
    }
}

#[cfg(test)]
mod tests;
