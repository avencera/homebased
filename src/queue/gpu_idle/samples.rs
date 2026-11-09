//! Raw GPU samples from either source, and their reduction into buckets

use std::time::Duration;

use chrono::{DateTime, Utc};

use super::watch::{Bucket, Verdict};

/// Samples of one GPU, each at Unix milliseconds
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Samples {
    /// Utilization percent
    pub util: Vec<(i64, f64)>,
    /// Power draw in watts
    pub power_watts: Vec<(i64, f64)>,
    /// Framebuffer memory in use, in bytes
    pub memory_used_bytes: Vec<(i64, u64)>,
    /// Framebuffer memory size, in bytes
    pub memory_total_bytes: Vec<(i64, u64)>,
}

impl Samples {
    /// Drop samples before `start`, such as a previous attempt's busy tail
    pub fn clamp_from(&mut self, start: DateTime<Utc>) {
        let start = start.timestamp_millis();
        self.util.retain(|(at, _)| *at >= start);
        self.power_watts.retain(|(at, _)| *at >= start);
        self.memory_used_bytes.retain(|(at, _)| *at >= start);
        self.memory_total_bytes.retain(|(at, _)| *at >= start);
    }

    /// Append another set, such as one more probe
    pub fn extend(&mut self, other: Samples) {
        self.util.extend(other.util);
        self.power_watts.extend(other.power_watts);
        self.memory_used_bytes.extend(other.memory_used_bytes);
        self.memory_total_bytes.extend(other.memory_total_bytes);
    }

    /// Drop samples older than `cutoff`, bounding a probe buffer
    pub fn forget_before(&mut self, cutoff: i64) {
        self.util.retain(|(at, _)| *at >= cutoff);
        self.power_watts.retain(|(at, _)| *at >= cutoff);
        self.memory_used_bytes.retain(|(at, _)| *at >= cutoff);
        self.memory_total_bytes.retain(|(at, _)| *at >= cutoff);
    }
}

/// Fewest utilization samples a bucket needs to be known: half of what the
/// source collects in one bucket, at least one
#[must_use]
pub fn min_samples(bucket: Duration, sample_every: Duration) -> u32 {
    let every = sample_every.as_millis().max(1);
    let expected = bucket.as_millis() / every;
    u32::try_from(expected / 2).unwrap_or(u32::MAX).max(1)
}

/// Reduce samples into the buckets that start at `starts`
///
/// A bucket's verdict comes from its peak utilization; with fewer than
/// `min_samples` utilization samples it is unknown, whatever power and
/// memory say
#[must_use]
pub fn bucketize(
    samples: &Samples,
    starts: &[DateTime<Utc>],
    bucket: Duration,
    min_samples: u32,
    util_below: u8,
) -> Vec<Bucket> {
    let width = i64::try_from(bucket.as_millis()).unwrap_or(i64::MAX);
    starts
        .iter()
        .map(|start| {
            let from = start.timestamp_millis();
            let to = from.saturating_add(width);
            let inside = |at: i64| (from..to).contains(&at);
            let util: Vec<f64> = samples
                .util
                .iter()
                .filter(|(at, _)| inside(*at))
                .map(|(_, value)| *value)
                .collect();
            let util_peak = util.iter().copied().reduce(f64::max);
            let count = u32::try_from(util.len()).unwrap_or(u32::MAX);
            let verdict = match util_peak {
                Some(peak) if count >= min_samples => {
                    if peak < f64::from(util_below) {
                        Verdict::Idle
                    } else {
                        Verdict::Busy
                    }
                }
                _ => Verdict::Unknown,
            };
            Bucket {
                start: *start,
                verdict,
                util_peak,
                power_peak_watts: samples
                    .power_watts
                    .iter()
                    .filter(|(at, _)| inside(*at))
                    .map(|(_, value)| *value)
                    .reduce(f64::max),
                memory_used_bytes: samples
                    .memory_used_bytes
                    .iter()
                    .filter(|(at, _)| inside(*at))
                    .map(|(_, value)| *value)
                    .max(),
                memory_total_bytes: samples
                    .memory_total_bytes
                    .iter()
                    .filter(|(at, _)| inside(*at))
                    .map(|(_, value)| *value)
                    .max(),
            }
        })
        .collect()
}
