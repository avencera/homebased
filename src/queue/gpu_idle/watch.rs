//! Per-attempt idle accounting over fixed buckets
//!
//! Buckets are aligned to the attempt's start, so no bucket mixes samples
//! from before the child started. The state is persisted after every tick,
//! so a daemon restart resumes the same episode

use std::collections::VecDeque;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Thresholds for one attempt, from the machine config and the job spec
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Thresholds {
    /// Length of one bucket, also the watcher's tick
    pub bucket: Duration,
    /// Trailing window an alert looks at
    pub window: Duration,
    /// Alert this long into an attempt that never had a busy bucket
    pub never_busy: Duration,
    /// A bucket whose peak utilization is below this percent is idle
    pub util_below: u8,
    /// Continuous busy time that ends an idle episode
    pub recover_after: Duration,
    /// Gap before the first reminder; later gaps grow from it
    pub remind_after: Duration,
}

impl Thresholds {
    /// Buckets in the trailing window, at least one
    #[must_use]
    pub fn window_buckets(&self) -> usize {
        buckets_in(self.window, self.bucket)
    }

    fn recover_buckets(&self) -> u32 {
        u32::try_from(buckets_in(self.recover_after, self.bucket)).unwrap_or(u32::MAX)
    }

    /// Time from the last alert to the next reminder, given the alerts sent
    /// so far in the episode: the reminders land 1x, 2x, and 4x
    /// `remind_after` after the first alert, then every 4x
    fn reminder_gap(&self, sent: u32) -> Duration {
        let factor = match sent {
            0..=2 => 1,
            3 => 2,
            _ => 4,
        };
        self.remind_after.saturating_mul(factor)
    }
}

fn buckets_in(span: Duration, bucket: Duration) -> usize {
    let bucket = bucket.as_millis().max(1);
    usize::try_from(span.as_millis().div_ceil(bucket))
        .unwrap_or(usize::MAX)
        .max(1)
}

/// What one bucket says about the GPU
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Peak utilization stayed below the threshold
    Idle,
    /// Peak utilization reached the threshold
    Busy,
    /// Too few samples to tell; never counts as idle
    Unknown,
}

/// One bucket's samples, reduced
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Bucket {
    /// Start of the bucket
    pub start: DateTime<Utc>,
    /// Idle, busy, or unknown
    pub verdict: Verdict,
    /// Highest utilization percent seen
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub util_peak: Option<f64>,
    /// Highest power draw seen, in watts
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_peak_watts: Option<f64>,
    /// Highest framebuffer memory in use, in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_used_bytes: Option<u64>,
    /// Framebuffer memory size, in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_total_bytes: Option<u64>,
}

impl Bucket {
    /// A bucket with no usable samples
    #[must_use]
    pub fn unknown(start: DateTime<Utc>) -> Self {
        Self {
            start,
            verdict: Verdict::Unknown,
            util_peak: None,
            power_peak_watts: None,
            memory_used_bytes: None,
            memory_total_bytes: None,
        }
    }
}

/// Bucket counts over a whole attempt, kept for summaries
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketTotals {
    /// Idle buckets
    pub idle: u64,
    /// Busy buckets
    pub busy: u64,
    /// Buckets with too few samples
    pub unknown: u64,
}

/// State of the current idle episode
///
/// An episode ends after `recover_after` of continuous busy buckets, and the
/// next one counts only buckets after that
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Episode {
    /// Trailing buckets of this episode, at most one window
    pub window: VecDeque<Bucket>,
    /// Consecutive busy buckets at the end of the window
    pub busy_run: u32,
    /// Start of the first idle bucket of the episode
    pub idle_since: Option<DateTime<Utc>>,
    /// Alerts sent in this episode
    pub alerts: u32,
    /// When the last alert of this episode was recorded
    pub last_alert_at: Option<DateTime<Utc>>,
}

/// Idle accounting of one attempt
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttemptWatch {
    /// When the attempt's child started; buckets are aligned to it
    pub started_at: DateTime<Utc>,
    /// End of the last bucket counted
    pub processed_through: DateTime<Utc>,
    /// Start of the attempt's first busy bucket; alerts after it are armed
    pub first_busy_at: Option<DateTime<Utc>>,
    /// Current episode, starting at 1
    pub episode: u32,
    /// Bucket counts over the attempt
    pub totals: BucketTotals,
    /// Alerts recorded over the attempt
    pub alerts: u32,
    /// The current episode
    pub current: Episode,
}

/// Why an alert is due
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertDue {
    /// The first alert of the episode
    First {
        /// The attempt never had a busy bucket
        never_busy: bool,
    },
    /// A reminder while the GPU stays idle
    Reminder {
        /// Alerts already sent in the episode
        sent: u32,
    },
}

/// The trailing window, reduced for an alert
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WindowSummary {
    /// Idle buckets in the window
    pub idle: u32,
    /// Idle and busy buckets in the window
    pub known: u32,
    /// Highest utilization percent in the window
    pub util_peak: Option<f64>,
    /// Highest power draw in the window, in watts
    pub power_peak_watts: Option<f64>,
    /// Highest memory in use in the window, in bytes
    pub memory_used_bytes: Option<u64>,
    /// Memory size, in bytes
    pub memory_total_bytes: Option<u64>,
    /// Start of the episode's first idle bucket
    pub idle_since: Option<DateTime<Utc>>,
}

impl AttemptWatch {
    /// Accounting for an attempt whose child started at `started_at`
    #[must_use]
    pub fn new(started_at: DateTime<Utc>) -> Self {
        Self {
            started_at,
            processed_through: started_at,
            first_busy_at: None,
            episode: 1,
            totals: BucketTotals::default(),
            alerts: 0,
            current: Episode::default(),
        }
    }

    /// Starts of the complete buckets not yet counted that end by `end`
    #[must_use]
    pub fn due_buckets(&self, end: DateTime<Utc>, thresholds: &Thresholds) -> Vec<DateTime<Utc>> {
        let Ok(step) = chrono::Duration::from_std(thresholds.bucket) else {
            return Vec::new();
        };
        let mut starts = Vec::new();
        let mut start = self.processed_through;
        while start + step <= end {
            starts.push(start);
            start += step;
        }
        starts
    }

    /// Count buckets that can no longer be read, such as after the daemon was
    /// down longer than the source keeps samples
    ///
    /// Only one window of them enters the episode; the rest go straight to
    /// the totals, so a long gap costs no memory
    pub fn skip_to(&mut self, end: DateTime<Utc>, thresholds: &Thresholds) {
        let starts = self.due_buckets(end, thresholds);
        let keep = thresholds.window_buckets();
        let dropped = starts.len().saturating_sub(keep);
        self.totals.unknown += u64::try_from(dropped).unwrap_or(u64::MAX);
        if let Some(step) = chrono::Duration::from_std(thresholds.bucket).ok()
            && dropped > 0
        {
            let skipped = i32::try_from(dropped).unwrap_or(i32::MAX);
            self.processed_through += step * skipped;
        }
        for start in starts.into_iter().skip(dropped) {
            self.push(Bucket::unknown(start), thresholds);
        }
    }

    /// Count the next bucket, which must start at `processed_through`
    pub fn push(&mut self, bucket: Bucket, thresholds: &Thresholds) {
        match bucket.verdict {
            Verdict::Idle => self.totals.idle += 1,
            Verdict::Busy => self.totals.busy += 1,
            Verdict::Unknown => self.totals.unknown += 1,
        }
        if let Ok(step) = chrono::Duration::from_std(thresholds.bucket) {
            self.processed_through = bucket.start + step;
        }
        let episode = &mut self.current;
        match bucket.verdict {
            Verdict::Busy => {
                if self.first_busy_at.is_none() {
                    self.first_busy_at = Some(bucket.start);
                }
                episode.busy_run = episode.busy_run.saturating_add(1);
            }
            Verdict::Idle => {
                episode.busy_run = 0;
                episode.idle_since.get_or_insert(bucket.start);
            }
            // a gap in the data neither ends nor extends a busy stretch
            Verdict::Unknown => {}
        }
        episode.window.push_back(bucket);
        while episode.window.len() > thresholds.window_buckets() {
            episode.window.pop_front();
        }
        let recovered = episode.busy_run == thresholds.recover_buckets()
            && (episode.idle_since.is_some() || episode.alerts > 0);
        if recovered {
            self.episode = self.episode.saturating_add(1);
            self.current = Episode {
                busy_run: self.current.busy_run,
                ..Episode::default()
            };
        }
    }

    /// Whether the trailing window is idle enough for an alert
    #[must_use]
    pub fn idle_now(&self, now: DateTime<Utc>, thresholds: &Thresholds) -> bool {
        let full = thresholds.window_buckets();
        let summary = self.summary();
        let known = usize::try_from(summary.known).unwrap_or(usize::MAX);
        let idle = usize::try_from(summary.idle).unwrap_or(usize::MAX);
        // half the window must be readable, so a dead source never alerts
        let readable = known.saturating_mul(2) >= full && known > 0;
        if self.first_busy_at.is_some() {
            self.current.window.len() >= full && readable && idle * 10 >= known * 9
        } else {
            let elapsed = (now - self.started_at).to_std().unwrap_or(Duration::ZERO);
            elapsed >= thresholds.never_busy && readable && idle == known
        }
    }

    /// The alert due now, if any; `silenced` covers a snooze or an off switch
    #[must_use]
    pub fn alert_due(
        &self,
        now: DateTime<Utc>,
        thresholds: &Thresholds,
        silenced: bool,
    ) -> Option<AlertDue> {
        if silenced || !self.idle_now(now, thresholds) {
            return None;
        }
        let sent = self.current.alerts;
        let Some(last) = self.current.last_alert_at else {
            return Some(AlertDue::First {
                never_busy: self.first_busy_at.is_none(),
            });
        };
        let gap = chrono::Duration::from_std(thresholds.reminder_gap(sent)).ok()?;
        (now >= last + gap).then_some(AlertDue::Reminder { sent })
    }

    /// Record that the alert due now was produced
    pub fn record_alert(&mut self, now: DateTime<Utc>) {
        self.alerts = self.alerts.saturating_add(1);
        self.current.alerts = self.current.alerts.saturating_add(1);
        self.current.last_alert_at = Some(now);
    }

    /// The trailing window, reduced
    #[must_use]
    pub fn summary(&self) -> WindowSummary {
        let window = &self.current.window;
        let max_f = |values: &mut dyn Iterator<Item = f64>| values.reduce(f64::max);
        WindowSummary {
            idle: count(window, Verdict::Idle),
            known: count(window, Verdict::Idle) + count(window, Verdict::Busy),
            util_peak: max_f(&mut window.iter().filter_map(|bucket| bucket.util_peak)),
            power_peak_watts: max_f(
                &mut window.iter().filter_map(|bucket| bucket.power_peak_watts),
            ),
            memory_used_bytes: window
                .iter()
                .filter_map(|bucket| bucket.memory_used_bytes)
                .max(),
            memory_total_bytes: window
                .iter()
                .filter_map(|bucket| bucket.memory_total_bytes)
                .max(),
            idle_since: self.current.idle_since,
        }
    }
}

fn count(window: &VecDeque<Bucket>, verdict: Verdict) -> u32 {
    u32::try_from(
        window
            .iter()
            .filter(|bucket| bucket.verdict == verdict)
            .count(),
    )
    .unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{DateTime, TimeZone, Utc};

    use super::{AlertDue, AttemptWatch, Bucket, Thresholds, Verdict};

    fn thresholds() -> Thresholds {
        Thresholds {
            bucket: Duration::from_secs(10),
            window: Duration::from_secs(60),
            never_busy: Duration::from_secs(120),
            util_below: 10,
            recover_after: Duration::from_secs(30),
            remind_after: Duration::from_secs(100),
        }
    }

    fn start() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 8, 20, 0, 0).unwrap()
    }

    /// Push `count` buckets of one verdict and return the time after them
    fn feed(watch: &mut AttemptWatch, verdict: Verdict, count: usize) -> DateTime<Utc> {
        let th = thresholds();
        for _ in 0..count {
            let mut bucket = Bucket::unknown(watch.processed_through);
            bucket.verdict = verdict;
            bucket.util_peak = match verdict {
                Verdict::Idle => Some(1.0),
                Verdict::Busy => Some(95.0),
                Verdict::Unknown => None,
            };
            watch.push(bucket, &th);
        }
        watch.processed_through
    }

    #[test]
    fn alerts_follow_arming_windows_reminders_and_recovery() {
        let th = thresholds();
        let mut watch = AttemptWatch::new(start());
        assert_eq!(
            watch
                .due_buckets(start() + chrono::Duration::seconds(35), &th)
                .len(),
            3,
            "only complete buckets are due"
        );

        // warmup: busy arms the attempt, then idle needs a full window
        feed(&mut watch, Verdict::Busy, 2);
        let now = feed(&mut watch, Verdict::Idle, 5);
        assert_eq!(
            watch.alert_due(now, &th, false),
            None,
            "window not full of idle"
        );
        let now = feed(&mut watch, Verdict::Idle, 1);
        assert_eq!(
            watch.alert_due(now, &th, false),
            Some(AlertDue::First { never_busy: false })
        );
        assert_eq!(watch.alert_due(now, &th, true), None, "silenced");
        watch.record_alert(now);
        assert_eq!(
            watch.alert_due(now, &th, false),
            None,
            "reminder not due yet"
        );
        let now = feed(&mut watch, Verdict::Idle, 10);
        assert_eq!(
            watch.alert_due(now, &th, false),
            Some(AlertDue::Reminder { sent: 1 })
        );
        watch.record_alert(now);

        // a short busy blip lowers the share but does not end the episode
        feed(&mut watch, Verdict::Busy, 2);
        assert_eq!(watch.episode, 1);
        // 30s of continuous busy ends it; the next episode needs a fresh window
        feed(&mut watch, Verdict::Busy, 1);
        assert_eq!(watch.episode, 2);
        assert_eq!(watch.current.alerts, 0);
        assert!(watch.current.window.is_empty());
        let now = feed(&mut watch, Verdict::Idle, 5);
        assert_eq!(watch.alert_due(now, &th, false), None);
        let now = feed(&mut watch, Verdict::Idle, 1);
        assert_eq!(
            watch.alert_due(now, &th, false),
            Some(AlertDue::First { never_busy: false })
        );
        assert_eq!(watch.alerts, 2);
        assert_eq!((watch.totals.busy, watch.totals.idle), (5, 22));
    }

    #[test]
    fn a_never_busy_attempt_alerts_at_the_backstop_and_unknown_data_never_alerts() {
        let th = thresholds();
        let mut watch = AttemptWatch::new(start());
        let now = feed(&mut watch, Verdict::Idle, 11);
        assert_eq!(
            watch.alert_due(now, &th, false),
            None,
            "before the backstop"
        );
        let now = feed(&mut watch, Verdict::Idle, 1);
        assert_eq!(
            watch.alert_due(now, &th, false),
            Some(AlertDue::First { never_busy: true })
        );

        // a dead source: unknown buckets are never idle
        let mut dead = AttemptWatch::new(start());
        let now = feed(&mut dead, Verdict::Unknown, 30);
        assert_eq!(dead.alert_due(now, &th, false), None);
        // a long gap is skipped as unknown without growing the window
        dead.skip_to(now + chrono::Duration::hours(2), &th);
        assert_eq!(dead.current.window.len(), th.window_buckets());
        assert_eq!(dead.totals.unknown, 30 + 720);
        assert_eq!(dead.processed_through, now + chrono::Duration::hours(2));
    }
}
