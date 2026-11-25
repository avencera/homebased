//! When a temporary machine renews its lease
//!
//! A machine keeps itself alive only while it is useful. It renews while any
//! run on it made output within the pool's stall window, while jobs wait in
//! its queue with no run in the way, while it still owes events to another
//! machine, and for the pool's idle limit after its last work ended. When
//! every run on it has stalled and it owes no event it withholds renewal, so
//! cloudlease deletes it
//! at the heartbeat deadline unless output resumes first. When it has been
//! idle for the idle limit it releases itself. The hard deadline applies
//! whatever this decides

use std::time::Duration;

use chrono::{DateTime, Utc};

/// What is on the machine now
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Activity {
    /// When each queued or running task last made progress: its last output,
    /// or its creation when it has none
    pub runs: Vec<DateTime<Utc>>,
    /// Jobs in this machine's queue that hold no run yet
    pub queued_jobs: usize,
    /// Events this machine still owes the machines that submitted its work
    pub undelivered: usize,
}

impl Activity {
    fn has_work(&self) -> bool {
        !self.runs.is_empty() || self.queued_jobs > 0 || self.undelivered > 0
    }
}

/// The pool's timings the decision uses
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Idle time after which the machine releases itself
    pub idle_limit: Duration,
    /// Time without output after which a run no longer keeps the machine
    pub stall_after: Duration,
}

/// What the machine does with its lease this round
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Renewal {
    /// Renew: it has useful work, or has been idle less than its limit
    Renew,
    /// Do not renew: every run on it stalled
    Withhold {
        /// How many runs stalled
        stalled: usize,
    },
    /// Release the lease: no work for the idle limit
    Release {
        /// When the machine became idle
        idle_since: DateTime<Utc>,
    },
}

/// Tracks when the machine last became idle across rounds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdleClock {
    idle_since: Option<DateTime<Utc>>,
}

impl IdleClock {
    /// A clock that counts the machine idle from `since`, its join
    #[must_use]
    pub fn started(since: DateTime<Utc>) -> Self {
        Self {
            idle_since: Some(since),
        }
    }

    /// Decide this round from what is on the machine now
    pub fn observe(&mut self, activity: &Activity, now: DateTime<Utc>, limits: Limits) -> Renewal {
        if activity.has_work() {
            self.idle_since = None;
            return busy_renewal(activity, now, limits.stall_after);
        }

        let since = *self.idle_since.get_or_insert(now);
        let idle = (now - since).to_std().unwrap_or_default();
        if idle >= limits.idle_limit {
            return Renewal::Release { idle_since: since };
        }

        Renewal::Renew
    }
}

fn busy_renewal(activity: &Activity, now: DateTime<Utc>, stall_after: Duration) -> Renewal {
    // queued jobs are about to start, or events are still on their way; a
    // finished job's result must reach its submitter even beside a stalled run
    if activity.runs.is_empty() || activity.undelivered > 0 {
        return Renewal::Renew;
    }

    let fresh = activity
        .runs
        .iter()
        .any(|last| (now - *last).to_std().unwrap_or_default() < stall_after);
    if fresh {
        return Renewal::Renew;
    }

    Renewal::Withhold {
        stalled: activity.runs.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: Limits = Limits {
        idle_limit: Duration::from_secs(600),
        stall_after: Duration::from_secs(1800),
    };

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap_or_default()
    }

    #[test]
    fn an_idle_machine_renews_until_its_idle_limit_then_releases() {
        let mut clock = IdleClock::started(at(0));
        let idle = Activity::default();
        assert_eq!(clock.observe(&idle, at(599), LIMITS), Renewal::Renew);
        assert_eq!(
            clock.observe(&idle, at(600), LIMITS),
            Renewal::Release { idle_since: at(0) }
        );
    }

    #[test]
    fn work_resets_the_idle_clock() {
        let mut clock = IdleClock::started(at(0));
        let busy = Activity {
            runs: vec![at(500)],
            ..Activity::default()
        };
        assert_eq!(clock.observe(&busy, at(550), LIMITS), Renewal::Renew);
        let idle = Activity::default();
        assert_eq!(clock.observe(&idle, at(700), LIMITS), Renewal::Renew);
        assert_eq!(clock.observe(&idle, at(1299), LIMITS), Renewal::Renew);
        assert_eq!(
            clock.observe(&idle, at(1300), LIMITS),
            Renewal::Release {
                idle_since: at(700)
            }
        );
    }

    #[test]
    fn stalled_runs_withhold_and_fresh_output_renews_again() {
        let mut clock = IdleClock::started(at(0));
        let stalled = Activity {
            runs: vec![at(0), at(100)],
            queued_jobs: 3,
            undelivered: 0,
        };
        assert_eq!(
            clock.observe(&stalled, at(1900), LIMITS),
            Renewal::Withhold { stalled: 2 }
        );
        let resumed = Activity {
            runs: vec![at(0), at(1950)],
            ..stalled
        };
        assert_eq!(clock.observe(&resumed, at(2000), LIMITS), Renewal::Renew);
        // a finished job's result still owed keeps the machine beside a stalled run
        let owing = Activity {
            undelivered: 1,
            ..stalled.clone()
        };
        assert_eq!(clock.observe(&owing, at(1900), LIMITS), Renewal::Renew);
    }

    #[test]
    fn queued_jobs_and_undelivered_events_keep_a_machine_without_runs() {
        let mut clock = IdleClock::started(at(0));
        let queued = Activity {
            queued_jobs: 1,
            ..Activity::default()
        };
        assert_eq!(clock.observe(&queued, at(5000), LIMITS), Renewal::Renew);
        let owing = Activity {
            undelivered: 1,
            ..Activity::default()
        };
        assert_eq!(clock.observe(&owing, at(9000), LIMITS), Renewal::Renew);
        // the idle clock starts once the last event is delivered
        assert_eq!(
            clock.observe(&Activity::default(), at(9001), LIMITS),
            Renewal::Renew
        );
    }
}
