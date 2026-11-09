//! The GPU lease guard: releases a lease whose job holds no CUDA context
//!
//! Utilization can sit near 0% for a legitimate reason, so the guard asks a
//! narrower question: does any process of the run hold a CUDA context on the
//! run's GPU? It reads NVML's compute processes through
//! `nvidia-smi --query-compute-apps` and matches their PIDs against the
//! processes carrying the run's marker, which covers re-parented children.
//! Container steps run under `dockerd` without the marker and are not
//! watched. A run that held no context for
//! [`LeaseGuardConfig::idle_after`] trips the guard. The default of 5 minutes
//! sits well above the 40-90 seconds a fresh process needs to compile and
//! load before it opens a context
//!
//! A failed probe, or a run whose processes cannot be read, makes the state
//! unknown, which restarts the idle clock: the guard trips only on a full
//! threshold of successful observations, and a daemon restart starts every
//! clock again

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::TaskId;
use crate::queue::Preemption;
use crate::queue::gpu::GpuUuid;

/// The `[resource.lease_guard]` settings
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LeaseGuardConfig {
    /// Whether the guard runs at all
    pub enabled: bool,
    /// Time a run may hold its GPU with no CUDA context before it trips
    pub idle_after: Duration,
    /// Time between probes; a context held at least this long is always seen
    pub interval: Duration,
    /// Time a guard-requested yield may stay pending before the job is cancelled
    pub yield_grace: Duration,
}

impl Default for LeaseGuardConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            idle_after: Duration::from_secs(5 * 60),
            interval: Duration::from_secs(5),
            yield_grace: Duration::from_secs(10 * 60),
        }
    }
}

/// Age of a pending yield request after which `resource jobs` flags it
///
/// A step that polls `HOMEBASED_YIELD_FILE` exits within one unit of work;
/// one still running this long after the request likely never reads it
pub const YIELD_OVERDUE: Duration = Duration::from_secs(10 * 60);

/// What a trip does to the run, chosen by the job's preemption mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TripAction {
    /// Ask the run to stop at its next checkpoint; the job queues again
    Yield,
    /// Kill the run and queue the job again; its step starts over
    Restart,
    /// Cancel the job
    Cancel,
}

impl TripAction {
    /// The action for a run of a job in `preempt` that tripped `earlier`
    /// times before this attempt
    ///
    /// A `yield` job can stop safely at a checkpoint and a `restart` job
    /// declared its step safe to repeat, so both are released and queue
    /// again: the idle stretch may have been a stall that a later attempt
    /// passes. A `wait` job promised it is never killed for other work, so a
    /// requeue would repeat a step that may not be safe to repeat; it is
    /// cancelled, and its owner reruns the CPU part as a task. Any job that
    /// already tripped is cancelled, so CPU work in the lease costs at most
    /// two thresholds instead of looping
    #[must_use]
    pub const fn for_job(preempt: Preemption, earlier: u32) -> Self {
        if earlier > 0 {
            return Self::Cancel;
        }
        match preempt {
            Preemption::Yield { .. } => Self::Yield,
            Preemption::Restart => Self::Restart,
            Preemption::Wait { .. } => Self::Cancel,
        }
    }

    /// Stable name
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Yield => "yield",
            Self::Restart => "restart",
            Self::Cancel => "cancel",
        }
    }
}

/// One trip of the guard on one attempt, stored on the queue machine and
/// carried by the event that ends the attempt
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseIdleTrip {
    /// Run task of the attempt
    pub attempt: TaskId,
    /// GPU the attempt held
    pub gpu_uuid: GpuUuid,
    /// Since when none of the run's processes held a CUDA context on it
    pub idle_since: DateTime<Utc>,
    /// When the guard tripped
    pub tripped_at: DateTime<Utc>,
    /// The threshold in force, in seconds
    pub idle_after_secs: u64,
    /// What the guard did
    pub action: TripAction,
    /// 1 for the job's first trip, 2 for the next
    pub trip: u32,
    /// When a yield that the run did not honor became a cancel
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalated_at: Option<DateTime<Utc>>,
}

impl LeaseIdleTrip {
    /// One line for people and agents: what happened and what to do
    #[must_use]
    pub fn reason(&self) -> String {
        let minutes = (self.tripped_at - self.idle_since).num_seconds().max(0) / 60;
        let done = match (self.action, self.escalated_at) {
            (_, Some(_)) => "cancelled after the run ignored the yield request",
            (TripAction::Yield, None) => "asked to yield",
            (TripAction::Restart, None) => "killed and queued again",
            (TripAction::Cancel, None) => "cancelled",
        };
        format!(
            "GPU lease idle: no process of the run held a CUDA context on {} for {minutes}m, so the \
             job was {done}; run builds and other CPU work as `homebased task run` tasks and \
             submit the GPU job with `--after <task-id>`",
            self.gpu_uuid
        )
    }
}

/// PIDs holding a CUDA context, per GPU, from
/// `nvidia-smi --query-compute-apps=gpu_uuid,pid --format=csv,noheader`
///
/// Lines without a GPU UUID and a PID, such as a driver's
/// `No running processes found`, are skipped
#[must_use]
pub fn parse_compute_apps(output: &str) -> HashMap<GpuUuid, HashSet<i32>> {
    let mut apps: HashMap<GpuUuid, HashSet<i32>> = HashMap::new();
    for line in output.lines() {
        let Some((uuid, pid)) = line.split_once(',') else {
            continue;
        };
        let (Ok(uuid), Ok(pid)) = (GpuUuid::parse(uuid.trim()), pid.trim().parse()) else {
            continue;
        };
        apps.entry(uuid).or_default().insert(pid);
    }
    apps
}

/// What one probe saw of one run
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observation {
    /// A process of the run holds a context on the run's GPU
    Holds,
    /// The probe worked and none of the run's processes holds one
    Missing,
    /// The probe failed or the run's processes could not be read
    Unknown,
}

impl Observation {
    /// Whether `pids` of a run hold a context on `gpu`, from a probe result
    #[must_use]
    pub fn of(
        apps: Option<&HashMap<GpuUuid, HashSet<i32>>>,
        gpu: &GpuUuid,
        pids: Option<&HashSet<i32>>,
    ) -> Self {
        match (apps, pids) {
            (Some(apps), Some(pids)) if !pids.is_empty() => {
                let holds = apps
                    .get(gpu)
                    .is_some_and(|holders| !holders.is_disjoint(pids));
                if holds { Self::Holds } else { Self::Missing }
            }
            _ => Self::Unknown,
        }
    }
}

/// In-memory idle clocks, one per attempt
#[derive(Debug, Default)]
pub struct IdleClocks(HashMap<TaskId, DateTime<Utc>>);

impl IdleClocks {
    /// Record `seen` for `attempt` at `now` and return the idle start once
    /// the run went `idle_after` without a context
    ///
    /// A clock starts at the first missing observation, never at the
    /// attempt's start, so a guard that starts late, such as after a daemon
    /// restart, waits a full threshold before it trips
    pub fn observe(
        &mut self,
        attempt: TaskId,
        seen: Observation,
        now: DateTime<Utc>,
        idle_after: Duration,
    ) -> Option<DateTime<Utc>> {
        if seen != Observation::Missing {
            self.0.remove(&attempt);
            return None;
        }
        let since = *self.0.entry(attempt).or_insert(now);
        let idle = (now - since).to_std().unwrap_or(Duration::ZERO);
        (idle >= idle_after).then_some(since)
    }

    /// Forget attempts that are no longer watched
    pub fn retain(&mut self, watched: &HashSet<TaskId>) {
        self.0.retain(|attempt, _| watched.contains(attempt));
    }

    /// Forget one attempt, such as after it tripped
    pub fn forget(&mut self, attempt: TaskId) {
        self.0.remove(&attempt);
    }
}

/// Whether a guard-requested yield has waited past `grace` and should
/// become a cancel
#[must_use]
pub fn yield_overdue(requested_at: DateTime<Utc>, now: DateTime<Utc>, grace: Duration) -> bool {
    (now - requested_at).to_std().is_ok_and(|age| age >= grace)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use chrono::{TimeZone, Utc};

    use super::{IdleClocks, Observation, parse_compute_apps};
    use crate::domain::TaskId;
    use crate::queue::gpu::GpuUuid;

    const GPU: &str = "GPU-11111111-2222-3333-4444-555555555555";

    #[test]
    fn a_run_trips_only_after_a_full_threshold_of_missing_contexts() {
        let gpu = GpuUuid::parse(GPU).unwrap();
        let apps = parse_compute_apps(&format!(
            "{GPU}, 4242\nNo running processes found\n{GPU}, 77\n"
        ));
        let run: HashSet<i32> = [4242, 4243].into();
        let other: HashSet<i32> = [9].into();
        assert_eq!(
            Observation::of(Some(&apps), &gpu, Some(&run)),
            Observation::Holds
        );
        let missing = Observation::of(Some(&apps), &gpu, Some(&other));
        assert_eq!(missing, Observation::Missing);
        assert_eq!(
            Observation::of(None, &gpu, Some(&run)),
            Observation::Unknown
        );
        assert_eq!(
            Observation::of(Some(&apps), &gpu, Some(&HashSet::new())),
            Observation::Unknown
        );

        let attempt = TaskId::new();
        let limit = Duration::from_secs(300);
        let at = |secs: i64| Utc.timestamp_opt(1_000_000 + secs, 0).unwrap();
        let mut clocks = IdleClocks::default();
        // a 90 s compile gap between benchmark processes never trips
        assert_eq!(clocks.observe(attempt, missing, at(0), limit), None);
        assert_eq!(clocks.observe(attempt, missing, at(90), limit), None);
        assert_eq!(
            clocks.observe(attempt, Observation::Holds, at(100), limit),
            None
        );
        assert_eq!(clocks.observe(attempt, missing, at(110), limit), None);
        // an unknown probe restarts the clock instead of counting as idle
        assert_eq!(
            clocks.observe(attempt, Observation::Unknown, at(300), limit),
            None
        );
        assert_eq!(clocks.observe(attempt, missing, at(400), limit), None);
        assert_eq!(clocks.observe(attempt, missing, at(699), limit), None);
        assert_eq!(
            clocks.observe(attempt, missing, at(700), limit),
            Some(at(400))
        );
    }
}
