//! Whether this host takes a task that waited for it to wake
//!
//! An origin sends a `wait_for_machine` task as soon as its machine answers.
//! A laptop also answers during a dark wake, a few seconds of network with the
//! lid closed, and then sleeps again while the task runs. So before the send,
//! the origin asks; a Mac with a battery says yes only once someone has used
//! it for [`IN_USE_FOR`], and the task keeps waiting on the origin until then

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

/// When this host takes a task that waited for it, from `power.waiting_tasks`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitingTasks {
    /// Once someone has used the host for [`IN_USE_FOR`]
    WhenInUse,
    /// As soon as an origin offers one
    Always,
}

impl WaitingTasks {
    /// The configured choice, or the host's default without one
    ///
    /// A Mac with a battery waits for use: on battery it sleeps whatever the
    /// daemon asks, and it dark-wakes with the lid closed. A desktop Mac or a
    /// server may never see a user, so it takes the task at once. Only macOS
    /// reports use, so another host always takes it at once
    #[must_use]
    pub fn effective(configured: Option<Self>) -> Self {
        if !cfg!(target_os = "macos") {
            return Self::Always;
        }
        configured.unwrap_or_else(|| {
            if has_battery() {
                Self::WhenInUse
            } else {
                Self::Always
            }
        })
    }
}

/// How long someone must have used the host before it takes a waiting task
pub const IN_USE_FOR: Duration = Duration::from_secs(120);

/// How often the daemon samples user activity
const SAMPLE_EVERY: Duration = Duration::from_secs(10);

/// A longer gap between samples means the host slept, so use starts over
const MAX_SAMPLE_GAP: Duration = Duration::from_secs(30);

/// Answer to an origin that offers a task waiting for this host
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Readiness {
    /// Take it now
    Ready,
    /// No one is using the host, as in a dark wake or with the display asleep
    Idle,
    /// Someone started using the host too recently
    Settling {
        /// Time until the host takes it, if use continues
        ready_in: Duration,
    },
}

impl Readiness {
    /// Why the host declines, or `None` when it is ready
    #[must_use]
    pub fn reason(self) -> Option<String> {
        match self {
            Self::Ready => None,
            Self::Idle => Some("no one is using it".into()),
            Self::Settling { ready_in } => Some(format!(
                "in use for under {}s; ready in {}s",
                IN_USE_FOR.as_secs(),
                ready_in.as_secs().max(1)
            )),
        }
    }
}

/// This host's readiness for waiting tasks, shared by the daemon
#[derive(Debug, Clone)]
pub struct Presence(Option<Arc<Mutex<Activity>>>);

impl Presence {
    /// A host that takes waiting tasks at once
    #[must_use]
    pub fn always() -> Self {
        Self(None)
    }

    /// Start sampling user activity when the host waits for use
    ///
    /// Call it inside the daemon's runtime: the sampler runs on it
    #[must_use]
    pub fn start(configured: Option<WaitingTasks>) -> Self {
        if configured == Some(WaitingTasks::WhenInUse) && !cfg!(target_os = "macos") {
            tracing::warn!(
                "power.waiting_tasks = \"when_in_use\" needs macOS; this host takes waiting tasks at once"
            );
        }
        match WaitingTasks::effective(configured) {
            WaitingTasks::Always => Self::always(),
            WaitingTasks::WhenInUse => Self::sample(),
        }
    }

    #[cfg(target_os = "macos")]
    fn sample() -> Self {
        let activity = Arc::new(Mutex::new(Activity::default()));
        let sampled = activity.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(SAMPLE_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Some(active) = user_is_active() {
                    observe(&sampled, active);
                }
            }
        });
        Self(Some(activity))
    }

    // `effective` never asks another host to wait for use
    #[cfg(not(target_os = "macos"))]
    fn sample() -> Self {
        Self::always()
    }

    /// Whether this host takes a waiting task now, from a fresh sample
    ///
    /// A host whose activity cannot be read takes it, as it did before readiness existed
    #[must_use]
    pub fn readiness(&self) -> Readiness {
        let Some(activity) = &self.0 else {
            return Readiness::Ready;
        };
        let Some(active) = user_is_active() else {
            tracing::debug!("user activity is unreadable; taking the waiting task");
            return Readiness::Ready;
        };
        observe(activity, active)
    }
}

#[cfg(target_os = "macos")]
fn user_is_active() -> Option<bool> {
    super::macos::user_is_active()
}

#[cfg(not(target_os = "macos"))]
fn user_is_active() -> Option<bool> {
    None
}

#[cfg(target_os = "macos")]
fn has_battery() -> bool {
    super::macos::has_internal_battery()
}

#[cfg(not(target_os = "macos"))]
fn has_battery() -> bool {
    false
}

fn observe(activity: &Mutex<Activity>, active: bool) -> Readiness {
    let now = SystemTime::now();
    // the state is two timestamps, so a panicked writer cannot leave it torn
    let mut activity = activity.lock().unwrap_or_else(PoisonError::into_inner);
    activity.observe(active, now);
    activity.readiness(now)
}

/// When the current stretch of use began
///
/// Wall-clock time, because the monotonic clock stops while a Mac sleeps and
/// would hide the gap that ends a stretch
#[derive(Debug, Default)]
struct Activity {
    since: Option<SystemTime>,
    last_sample: Option<SystemTime>,
}

impl Activity {
    fn observe(&mut self, active: bool, now: SystemTime) {
        let continuous = self
            .last_sample
            .and_then(|last| now.duration_since(last).ok())
            .is_some_and(|gap| gap <= MAX_SAMPLE_GAP);
        self.since = match self.since {
            _ if !active => None,
            Some(since) if continuous => Some(since),
            _ => Some(now),
        };
        self.last_sample = Some(now);
    }

    fn readiness(&self, now: SystemTime) -> Readiness {
        let Some(since) = self.since else {
            return Readiness::Idle;
        };
        let used = now.duration_since(since).unwrap_or_default();
        match IN_USE_FOR.checked_sub(used) {
            Some(ready_in) if !ready_in.is_zero() => Readiness::Settling { ready_in },
            _ => Readiness::Ready,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::{Activity, IN_USE_FOR, Readiness, SAMPLE_EVERY};

    /// Sample active every `SAMPLE_EVERY` from `start` for `span`, returning the end time
    fn use_for(activity: &mut Activity, start: SystemTime, span: Duration) -> SystemTime {
        let mut now = start;
        activity.observe(true, now);
        while now < start + span {
            now += SAMPLE_EVERY;
            activity.observe(true, now);
        }
        now
    }

    #[test]
    fn takes_waiting_work_only_after_unbroken_use() {
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let mut activity = Activity::default();

        activity.observe(false, start);
        assert_eq!(activity.readiness(start), Readiness::Idle);

        let now = use_for(&mut activity, start, Duration::from_secs(60));
        assert!(matches!(
            activity.readiness(now),
            Readiness::Settling { .. }
        ));
        let now = use_for(&mut activity, now, IN_USE_FOR);
        assert_eq!(activity.readiness(now), Readiness::Ready);

        // the lid closed for an hour, then a dark wake: nobody is using it
        let woke = now + Duration::from_secs(3600);
        activity.observe(false, woke);
        assert_eq!(activity.readiness(woke), Readiness::Idle);

        // the user returns; the hour asleep is a gap, so use starts over
        let mut activity = Activity::default();
        let before_sleep = use_for(&mut activity, start, IN_USE_FOR);
        let back = before_sleep + Duration::from_secs(3600);
        activity.observe(true, back);
        assert!(matches!(
            activity.readiness(back),
            Readiness::Settling { .. }
        ));
    }
}
