//! Per-attempt GPU usage summaries, carried on final job events
//!
//! The idle watcher folds every sample it counts into running totals stored
//! with the attempt's accounting, so a summary needs no source to answer when
//! the attempt ends. At the end, a bounded Netdata read can replace the
//! sampled fields with the whole attempt's history. Every field keeps its own
//! coverage and source, since a source can drop out for part of an attempt.
//! The numbers are device-level: any other work on the same GPU is included

use std::fmt::Write as _;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::netdata::{Group, Netdata, RangeQuery, Series};
use super::notice::{GpuIdleControlRecord, SampleSource};
use super::samples::Samples;
use super::watch::{AttemptWatch, Verdict};
use crate::domain::TaskId;
use crate::fleet::address::MachineAddress;
use crate::queue::gpu::GpuUuid;

const MIB: u64 = 1024 * 1024;

/// Milliseconds of samples from each source
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Coverage {
    /// Time covered by Netdata samples
    #[serde(default)]
    pub netdata_ms: u64,
    /// Time covered by `nvidia-smi` probes
    #[serde(default)]
    pub nvidia_smi_ms: u64,
}

impl Coverage {
    fn add(&mut self, source: SampleSource, ms: u64) {
        let slot = match source {
            SampleSource::Netdata => &mut self.netdata_ms,
            SampleSource::NvidiaSmi => &mut self.nvidia_smi_ms,
        };
        *slot = slot.saturating_add(ms);
    }

    /// Time covered by any source
    #[must_use]
    pub fn total_ms(&self) -> u64 {
        self.netdata_ms.saturating_add(self.nvidia_smi_ms)
    }

    /// The source, or `Mixed` when both contributed
    #[must_use]
    pub fn source(&self) -> Option<SummarySource> {
        match (self.netdata_ms > 0, self.nvidia_smi_ms > 0) {
            (true, true) => Some(SummarySource::Mixed),
            (true, false) => Some(SummarySource::Netdata),
            (false, true) => Some(SummarySource::NvidiaSmi),
            (false, false) => None,
        }
    }
}

/// Time-weighted running mean of one field
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct MeanTotal {
    /// Sum of value times milliseconds
    #[serde(default)]
    pub weighted: f64,
    /// Time the samples cover
    #[serde(default)]
    pub coverage: Coverage,
}

impl MeanTotal {
    fn add(&mut self, value: f64, ms: u64, source: SampleSource) {
        if !value.is_finite() || ms == 0 {
            return;
        }
        self.weighted += value * ms as f64;
        self.coverage.add(source, ms);
    }

    fn mean(&self) -> Option<f64> {
        let ms = self.coverage.total_ms();
        (ms > 0).then(|| self.weighted / ms as f64)
    }
}

/// Running peak of one field
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeakTotal {
    /// Highest value seen
    #[serde(default)]
    pub peak: Option<u64>,
    /// Time the samples cover
    #[serde(default)]
    pub coverage: Coverage,
}

impl PeakTotal {
    fn add(&mut self, value: u64, ms: u64, source: SampleSource) {
        self.peak = Some(self.peak.map_or(value, |peak| peak.max(value)));
        self.coverage.add(source, ms);
    }
}

/// Running totals of one attempt, kept with its idle accounting
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UsageTotals {
    /// Utilization percent
    #[serde(default)]
    pub util: MeanTotal,
    /// Power draw over the enforced limit read at the time, in percent
    #[serde(default)]
    pub power_pct: MeanTotal,
    /// Framebuffer memory in use, in bytes
    #[serde(default)]
    pub mem_used: PeakTotal,
    /// Framebuffer memory size, in bytes
    #[serde(default)]
    pub mem_total: PeakTotal,
    /// 100 for each idle bucket and 0 for each busy one, so the mean is the
    /// idle share of the buckets that had enough samples
    #[serde(default)]
    pub idle: MeanTotal,
    /// The last reason a sampled stretch produced no utilization
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub problem: Option<String>,
    /// The Netdata agent and charts the watcher last read for this GPU,
    /// so the worker can read the whole attempt when it ends
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub netdata: Option<NetdataCharts>,
    /// The GPU's enforced power limit when the watcher last read it, in watts
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_limit_watts: Option<f64>,
}

/// Where the worker reads an attempt's history at its end
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetdataCharts {
    /// Base URL of the local Netdata agent
    pub url: MachineAddress,
    /// Utilization chart id
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utilization: Option<String>,
    /// Power chart id
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power: Option<String>,
    /// Framebuffer memory chart id
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

impl UsageTotals {
    /// Fold the samples inside `from..to` (Unix milliseconds) into the totals
    ///
    /// Each sample stands for `sample_ms` of the attempt. A sample outside
    /// the range belongs to a bucket counted on another tick, so it is skipped
    pub fn add_samples(
        &mut self,
        samples: &Samples,
        range: (i64, i64),
        sample_ms: SampleSpacing,
        power_limit_watts: Option<f64>,
        source: SampleSource,
    ) {
        let inside = |at: &i64| (range.0..range.1).contains(at);
        for (_, value) in samples.util.iter().filter(|(at, _)| inside(at)) {
            self.util.add(*value, sample_ms.util, source);
        }
        if let Some(limit) = power_limit_watts.filter(|limit| *limit > 0.0) {
            for (_, watts) in samples.power_watts.iter().filter(|(at, _)| inside(at)) {
                self.power_pct
                    .add(watts / limit * 100.0, sample_ms.power, source);
            }
        }
        for (_, bytes) in samples
            .memory_used_bytes
            .iter()
            .filter(|(at, _)| inside(at))
        {
            self.mem_used.add(*bytes, sample_ms.memory, source);
        }
        for (_, bytes) in samples
            .memory_total_bytes
            .iter()
            .filter(|(at, _)| inside(at))
        {
            self.mem_total.add(*bytes, sample_ms.memory, source);
        }
    }

    /// Count one bucket's verdict toward the idle share; unknown buckets are left out
    pub fn add_bucket(&mut self, verdict: Verdict, bucket_ms: u64, source: SampleSource) {
        match verdict {
            Verdict::Idle => self.idle.add(100.0, bucket_ms, source),
            Verdict::Busy => self.idle.add(0.0, bucket_ms, source),
            Verdict::Unknown => {}
        }
    }
}

/// Time each sample of a field stands for, in milliseconds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleSpacing {
    /// Utilization
    pub util: u64,
    /// Power draw
    pub power: u64,
    /// Framebuffer memory
    pub memory: u64,
}

impl SampleSpacing {
    /// The same spacing for every field
    #[must_use]
    pub const fn uniform(ms: u64) -> Self {
        Self {
            util: ms,
            power: ms,
            memory: ms,
        }
    }
}

/// Where a summary field came from
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SummarySource {
    /// The local Netdata agent
    Netdata,
    /// `nvidia-smi --query-gpu` probes
    NvidiaSmi,
    /// Both, for different parts of the attempt
    Mixed,
}

impl SummarySource {
    /// Stable name
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Netdata => "netdata",
            Self::NvidiaSmi => "nvidia-smi",
            Self::Mixed => "mixed",
        }
    }

    fn join(self, other: Self) -> Self {
        if self == other { self } else { Self::Mixed }
    }
}

/// One summary field with its own coverage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metric {
    /// Whole percent, or MiB for memory
    pub value: u64,
    /// Seconds of the attempt the samples cover
    pub covered_secs: u64,
    /// `covered_secs` over the attempt's length, in whole percent
    pub coverage_pct: u32,
    /// Where the samples came from
    pub source: SummarySource,
}

/// Whether a summary has any numbers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryStatus {
    /// At least one field has samples
    Ok,
    /// No field has samples; `reason` says why
    Unavailable,
}

/// GPU usage of one run attempt, computed once when the attempt ends
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuSummary {
    /// Run task of the attempt
    pub attempt: TaskId,
    /// Attempt count across the job
    pub run_number: u32,
    /// Zero-based step the attempt ran
    pub step: u32,
    /// `ok`, or `unavailable` with a `reason`
    pub status: SummaryStatus,
    /// Why no field has samples
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// GPU the attempt launched on
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_uuid: Option<GpuUuid>,
    /// Source of every field, or `mixed` when they differ
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SummarySource>,
    /// Seconds from the child's start to the attempt's end
    pub attempt_secs: u64,
    /// Average utilization, in percent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub util_avg: Option<Metric>,
    /// Average power draw over the enforced power limit, in percent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_pct_avg: Option<Metric>,
    /// Highest framebuffer memory in use, in MiB
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_peak_mib: Option<Metric>,
    /// Framebuffer memory size, in MiB
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_total_mib: Option<Metric>,
    /// Share of the idle watcher's buckets that were idle, in percent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_pct: Option<Metric>,
    /// Seconds from the child's start to the first busy bucket
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_busy_after_secs: Option<u64>,
    /// `JOB_GPU_IDLE` alerts produced for the attempt
    pub idle_alerts: u32,
    /// Snoozes, off switches, and clears in force during the attempt
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snoozes: Vec<GpuIdleControlRecord>,
}

/// A whole-attempt read taken from Netdata when the attempt ended
///
/// Its fields replace the sampled totals of the same name; the idle share
/// always comes from the watcher's buckets
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EndReading {
    /// Average utilization
    #[serde(default)]
    pub util: MeanTotal,
    /// Average power over the limit
    #[serde(default)]
    pub power_pct: MeanTotal,
    /// Peak memory in use, in bytes
    #[serde(default)]
    pub mem_used: PeakTotal,
    /// Memory size, in bytes
    #[serde(default)]
    pub mem_total: PeakTotal,
}

/// Most points one end read asks for; Netdata averages a longer attempt
/// into this many, and moves long ranges to coarser tiers by itself
const END_READ_POINTS: i64 = 1200;

impl EndReading {
    /// Read the attempt `started..=ended` from Netdata, within `budget` in all
    ///
    /// Utilization and power are averaged per point, and memory in use is the
    /// peak per point. Memory size is the sum of the averaged dimensions: the
    /// sum is constant, so its average is the size, while a sum of per-point
    /// peaks taken at different moments would overstate it. A point with data
    /// counts the spacing Netdata returned as covered, which is wider than the
    /// one asked for when the collector or tier is coarser. `None` when the
    /// agent fails, is slow, or has no data, so the watcher's totals stand
    pub async fn read(
        charts: &NetdataCharts,
        started: DateTime<Utc>,
        ended: DateTime<Utc>,
        power_limit_watts: Option<f64>,
        budget: Duration,
    ) -> Option<Self> {
        // a point stamped `t` holds the time from `t - width` to `t`, and one
        // that begins before the start can hold the previous attempt's tail;
        // both bounds are inclusive, so the first point that can be kept is
        // the one-second point after the first whole second
        let start = started.timestamp() + i64::from(started.timestamp_subsec_nanos() > 0);
        let after = start + 1;
        let before = ended.timestamp();
        if before < after {
            return None;
        }
        let span = before - after + 1;
        let points = span.min(END_READ_POINTS);
        let width_ms = u64::try_from(span * 1000 / points).unwrap_or(1000);
        let points = u32::try_from(points).unwrap_or(1);
        let netdata = Netdata::new(charts.url.clone(), budget);
        let read = |chart: Option<&String>, group: Group| {
            let netdata = &netdata;
            let chart = chart.cloned();
            async move {
                let chart = chart?;
                let query = RangeQuery {
                    chart: &chart,
                    after,
                    before,
                    points,
                    group,
                };
                netdata.range(&query).await.ok()
            }
        };
        let (util, power, memory_peak, memory_mean) = tokio::time::timeout(budget, async {
            tokio::join!(
                read(charts.utilization.as_ref(), Group::Average),
                read(charts.power.as_ref(), Group::Average),
                read(charts.memory.as_ref(), Group::Max),
                read(charts.memory.as_ref(), Group::Average),
            )
        })
        .await
        .ok()?;

        let netdata = SampleSource::Netdata;
        // Netdata fills whole groups back from `before` and may begin the
        // first one before `after`, so every point is checked against the
        // width it came back with
        let width = |series: &Series| {
            let width = series.spacing_ms(width_ms);
            let first = start + i64::try_from(width.div_ceil(1000)).unwrap_or(i64::MAX);
            (width, move |at: &i64| *at >= first)
        };
        let mut reading = Self::default();
        if let Some(util) = &util {
            let (width, inside) = width(util);
            for (_, value) in util.values("gpu").filter(|(at, _)| inside(at)) {
                reading.util.add(value, width, netdata);
            }
        }
        if let Some(limit) = power_limit_watts.filter(|limit| *limit > 0.0)
            && let Some(power) = &power
        {
            let (width, inside) = width(power);
            for (_, watts) in power.values("power_draw").filter(|(at, _)| inside(at)) {
                reading.power_pct.add(watts / limit * 100.0, width, netdata);
            }
        }
        if let Some(memory) = &memory_peak {
            let (width, inside) = width(memory);
            let used = memory.values("used");
            for (_, used) in used.filter(|(at, bytes)| inside(at) && *bytes >= 0.0) {
                reading.mem_used.add(used as u64, width, netdata);
            }
        }
        if let Some(memory) = &memory_mean {
            let (width, inside) = width(memory);
            for (_, values) in memory.rows.iter().filter(|(at, _)| inside(at)) {
                let column = |name: &str| {
                    let index = memory
                        .dimensions
                        .iter()
                        .position(|dimension| dimension == name)?;
                    values.get(index).copied().flatten()
                };
                let total: f64 = ["free", "used", "reserved"]
                    .into_iter()
                    .filter_map(column)
                    .sum();
                if total > 0.0 {
                    reading.mem_total.add(total as u64, width, netdata);
                }
            }
        }
        let covered = [
            reading.util.coverage,
            reading.power_pct.coverage,
            reading.mem_used.coverage,
            reading.mem_total.coverage,
        ];
        covered
            .iter()
            .any(|coverage| coverage.total_ms() > 0)
            .then_some(reading)
    }
}

/// What the summary of one attempt is built from
#[derive(Debug, Clone, Copy)]
pub struct SummaryInput<'a> {
    /// Run task of the attempt
    pub attempt: TaskId,
    /// Attempt count across the job
    pub run_number: u32,
    /// Step the attempt ran
    pub step: u32,
    /// GPU the attempt launched on
    pub gpu_uuid: Option<&'a GpuUuid>,
    /// When the child started, if it did
    pub started_at: Option<DateTime<Utc>>,
    /// When the attempt ended
    pub ended_at: DateTime<Utc>,
    /// The idle watcher's accounting, if it ever read the attempt
    pub watch: Option<&'a AttemptWatch>,
    /// A whole-attempt Netdata read from the attempt's end, if one answered
    pub end_reading: Option<&'a EndReading>,
    /// Every control of the job, oldest first
    pub controls: &'a [GpuIdleControlRecord],
}

impl GpuSummary {
    /// Build the summary of one ended attempt
    #[must_use]
    pub fn build(input: &SummaryInput<'_>) -> Self {
        let attempt_ms = input.started_at.map_or(0, |started| {
            u64::try_from((input.ended_at - started).num_milliseconds()).unwrap_or(0)
        });
        let metric = |value: Option<f64>, coverage: Coverage| {
            let source = coverage.source()?;
            let value = value?;
            Some(Metric {
                value: value.round().clamp(0.0, u64::MAX as f64) as u64,
                covered_secs: coverage.total_ms().min(attempt_ms) / 1000,
                coverage_pct: percent(coverage.total_ms(), attempt_ms),
                source,
            })
        };
        let usage = input.watch.map(|watch| &watch.usage);
        // the end read and the sampled totals each cover part of the attempt;
        // the one covering more wins, so a Netdata restart that lost history
        // never hides what the watcher saw
        let covers = |end: u64, sampled: u64| end > 0 && end >= sampled;
        let pick_mean = |end: Option<&MeanTotal>, sampled: Option<&MeanTotal>| {
            let sampled_ms = sampled.map_or(0, |total| total.coverage.total_ms());
            end.filter(|total| covers(total.coverage.total_ms(), sampled_ms))
                .or(sampled)
                .and_then(|total| metric(total.mean(), total.coverage))
        };
        // both readings saw the same device during the attempt, so the peak is
        // the higher of the two; only the coverage follows the covering one
        let pick_peak = |end: Option<&PeakTotal>, sampled: Option<&PeakTotal>| {
            let sampled_ms = sampled.map_or(0, |total| total.coverage.total_ms());
            let (covering, other) =
                match end.filter(|total| covers(total.coverage.total_ms(), sampled_ms)) {
                    Some(end) => (end, sampled),
                    None => (sampled?, end),
                };
            let other_peak = other.filter(|other| other.peak > covering.peak);
            let peak = other_peak.map_or(covering.peak, |other| other.peak);
            let mut metric = metric(peak.map(|bytes| (bytes / MIB) as f64), covering.coverage)?;
            if let Some(source) = other_peak.and_then(|other| other.coverage.source()) {
                metric.source = metric.source.join(source);
            }
            Some(metric)
        };
        let end = input.end_reading;
        let util_avg = pick_mean(end.map(|end| &end.util), usage.map(|usage| &usage.util));
        let power_pct_avg = pick_mean(
            end.map(|end| &end.power_pct),
            usage.map(|usage| &usage.power_pct),
        );
        let mem_peak_mib = pick_peak(
            end.map(|end| &end.mem_used),
            usage.map(|usage| &usage.mem_used),
        );
        let mem_total_mib = pick_peak(
            end.map(|end| &end.mem_total),
            usage.map(|usage| &usage.mem_total),
        );
        let idle_pct = usage.and_then(|usage| metric(usage.idle.mean(), usage.idle.coverage));
        let metrics = [
            util_avg,
            power_pct_avg,
            mem_peak_mib,
            mem_total_mib,
            idle_pct,
        ];
        let source = metrics
            .iter()
            .flatten()
            .map(|metric| metric.source)
            .reduce(SummarySource::join);
        let reason = source.is_none().then(|| unavailable_reason(input));
        Self {
            attempt: input.attempt,
            run_number: input.run_number,
            step: input.step,
            status: if source.is_some() {
                SummaryStatus::Ok
            } else {
                SummaryStatus::Unavailable
            },
            reason,
            gpu_uuid: input.gpu_uuid.cloned(),
            source,
            attempt_secs: attempt_ms / 1000,
            util_avg,
            power_pct_avg,
            mem_peak_mib,
            mem_total_mib,
            idle_pct,
            first_busy_after_secs: input.watch.and_then(|watch| {
                let busy = watch.first_busy_at?;
                u64::try_from((busy - watch.started_at).num_seconds()).ok()
            }),
            idle_alerts: input.watch.map_or(0, |watch| watch.alerts),
            snoozes: controls_during(input),
        }
    }

    /// One line for callbacks and `resource job show`
    #[must_use]
    pub fn brief(&self) -> String {
        let mut line = format!("run {} step {}: ", self.run_number, self.step);
        if self.status == SummaryStatus::Unavailable {
            let reason = self.reason.as_deref().unwrap_or("no samples");
            let _ = write!(line, "GPU summary unavailable: {reason}");
            return line;
        }
        let mut parts = Vec::new();
        if let Some(util) = self.util_avg {
            parts.push(format!("util {}% avg", util.value));
        }
        if let Some(power) = self.power_pct_avg {
            parts.push(format!("power {}% of limit avg", power.value));
        }
        match (self.mem_peak_mib, self.mem_total_mib) {
            (Some(peak), Some(total)) => {
                parts.push(format!("mem peak {}/{} MiB", peak.value, total.value));
            }
            (Some(peak), None) => parts.push(format!("mem peak {} MiB", peak.value)),
            _ => {}
        }
        if let Some(idle) = self.idle_pct {
            parts.push(format!("idle {}% of buckets", idle.value));
        }
        if let Some(secs) = self.first_busy_after_secs {
            parts.push(format!("first busy after {secs}s"));
        }
        parts.push(format!("{} idle alerts", self.idle_alerts));
        if !self.snoozes.is_empty() {
            parts.push(format!("{} silences", self.snoozes.len()));
        }
        line.push_str(&parts.join(", "));
        let coverage = self
            .util_avg
            .or(self.idle_pct)
            .or(self.mem_peak_mib)
            .map_or(0, |metric| metric.coverage_pct);
        let source = self.source.map_or("-", SummarySource::as_str);
        let _ = write!(
            line,
            "; {source}, {coverage}% of {}s covered, device-level",
            self.attempt_secs
        );
        line
    }
}

fn percent(part_ms: u64, whole_ms: u64) -> u32 {
    if whole_ms == 0 {
        return 0;
    }
    let pct = (part_ms.min(whole_ms) as f64 / whole_ms as f64 * 100.0).round();
    pct.clamp(0.0, 100.0) as u32
}

fn unavailable_reason(input: &SummaryInput<'_>) -> String {
    if input.started_at.is_none() {
        return "the attempt ended before its child started".into();
    }
    if input.gpu_uuid.is_none() {
        return "no GPU UUID was recorded when the attempt launched".into();
    }
    match input.watch {
        None => "no GPU samples were taken: sampling is off on this machine \
                 (resource.gpu_idle.source), or the attempt ended within its first bucket"
            .into(),
        Some(watch) => watch.usage.problem.clone().map_or_else(
            || "no GPU samples were collected".into(),
            |problem| format!("no GPU samples were collected: {problem}"),
        ),
    }
}

/// The controls in force at the attempt's start, and those recorded during it
fn controls_during(input: &SummaryInput<'_>) -> Vec<GpuIdleControlRecord> {
    let Some(started) = input.started_at else {
        return Vec::new();
    };
    let before = input
        .controls
        .iter()
        .rev()
        .find(|control| control.at < started)
        .filter(|control| match control.action.as_str() {
            "off" => true,
            "snooze" => control.until.is_some_and(|until| until > started),
            _ => false,
        });
    before
        .into_iter()
        .chain(
            input
                .controls
                .iter()
                .filter(|control| control.at >= started && control.at <= input.ended_at),
        )
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use std::time::Duration;

    use serde_json::{Value, json};

    use super::{
        Coverage, EndReading, GpuSummary, NetdataCharts, PeakTotal, SampleSource, SampleSpacing,
        SummaryInput, SummarySource, SummaryStatus, UsageTotals,
    };
    use crate::domain::TaskId;
    use crate::queue::gpu::GpuUuid;
    use crate::queue::gpu_idle::samples::Samples;
    use crate::queue::gpu_idle::{AttemptWatch, GpuIdleControlRecord, Verdict};

    #[test]
    fn summaries_weight_sources_by_time_and_mark_a_mix() {
        let start = Utc.with_ymd_and_hms(2026, 11, 9, 12, 0, 0).unwrap();
        let ms = start.timestamp_millis();
        let mut watch = AttemptWatch::new(start);
        watch.first_busy_at = Some(start + chrono::Duration::seconds(10));
        watch.alerts = 1;
        // 20 one-second Netdata samples at 90%, then two 10s probes at 0%
        let netdata = Samples {
            util: (0..20).map(|second| (ms + second * 1000, 90.0)).collect(),
            power_watts: (0..20).map(|second| (ms + second * 1000, 250.0)).collect(),
            memory_used_bytes: vec![(ms, 3 << 30)],
            memory_total_bytes: vec![(ms, 32 << 30)],
        };
        watch.usage.add_samples(
            &netdata,
            (ms, ms + 20_000),
            SampleSpacing::uniform(1000),
            Some(500.0),
            SampleSource::Netdata,
        );
        let probes = Samples {
            util: vec![(ms + 20_000, 0.0), (ms + 30_000, 0.0), (ms + 40_000, 99.0)],
            ..Samples::default()
        };
        // the last probe belongs to a bucket counted on a later tick
        watch.usage.add_samples(
            &probes,
            (ms + 20_000, ms + 40_000),
            SampleSpacing::uniform(10_000),
            Some(500.0),
            SampleSource::NvidiaSmi,
        );
        for verdict in [Verdict::Busy, Verdict::Busy, Verdict::Idle, Verdict::Idle] {
            watch
                .usage
                .add_bucket(verdict, 10_000, SampleSource::NvidiaSmi);
        }
        let uuid = GpuUuid::parse("GPU-11111111-2222-3333-4444-555555555555").unwrap();
        let control = |action: &str, at: i64| GpuIdleControlRecord {
            action: action.into(),
            until: (action == "snooze").then(|| start + chrono::Duration::seconds(at + 60)),
            reason: (action != "clear").then(|| "why".into()),
            at: start + chrono::Duration::seconds(at),
        };
        // an expired snooze before the start is left out; an off switch in force is kept
        let controls = [
            control("snooze", -600),
            control("off", -5),
            control("clear", 30),
        ];
        let summary = GpuSummary::build(&SummaryInput {
            attempt: TaskId::new(),
            run_number: 2,
            step: 0,
            gpu_uuid: Some(&uuid),
            started_at: Some(start),
            ended_at: start + chrono::Duration::seconds(50),
            watch: Some(&watch),
            end_reading: None,
            controls: &controls,
        });
        assert_eq!(summary.status, SummaryStatus::Ok);
        let util = summary.util_avg.unwrap();
        // (90 * 20s + 0 * 20s) / 40s
        assert_eq!(
            (util.value, util.covered_secs, util.coverage_pct),
            (45, 40, 80)
        );
        assert_eq!(util.source, SummarySource::Mixed);
        let power = summary.power_pct_avg.unwrap();
        assert_eq!((power.value, power.source), (50, SummarySource::Netdata));
        assert_eq!(summary.mem_peak_mib.unwrap().value, 3072);
        assert_eq!(summary.mem_total_mib.unwrap().value, 32768);
        let idle = summary.idle_pct.unwrap();
        assert_eq!((idle.value, idle.source), (50, SummarySource::NvidiaSmi));
        assert_eq!(summary.source, Some(SummarySource::Mixed));
        assert_eq!(summary.first_busy_after_secs, Some(10));
        assert_eq!(summary.idle_alerts, 1);
        let kinds: Vec<_> = summary.snoozes.iter().map(|c| c.action.as_str()).collect();
        assert_eq!(kinds, ["off", "clear"]);
        assert!(
            summary.brief().contains("util 45% avg"),
            "{}",
            summary.brief()
        );

        let empty = GpuSummary::build(&SummaryInput {
            watch: Some(&AttemptWatch {
                usage: UsageTotals {
                    problem: Some("Netdata unavailable: refused".into()),
                    ..UsageTotals::default()
                },
                ..AttemptWatch::new(start)
            }),
            ..SummaryInput {
                attempt: TaskId::new(),
                run_number: 1,
                step: 0,
                gpu_uuid: Some(&uuid),
                started_at: Some(start),
                ended_at: start,
                watch: None,
                end_reading: None,
                controls: &[],
            }
        });
        assert_eq!(empty.status, SummaryStatus::Unavailable);
        assert!(empty.reason.unwrap().contains("refused"));

        // an end read covering more time keeps the higher peak the watcher saw
        let end_reading = EndReading {
            mem_used: PeakTotal {
                peak: Some(1 << 30),
                coverage: Coverage {
                    netdata_ms: 50_000,
                    nvidia_smi_ms: 0,
                },
            },
            ..EndReading::default()
        };
        let summary = GpuSummary::build(&SummaryInput {
            attempt: TaskId::new(),
            run_number: 2,
            step: 0,
            gpu_uuid: Some(&uuid),
            started_at: Some(start),
            ended_at: start + chrono::Duration::seconds(50),
            watch: Some(&watch),
            end_reading: Some(&end_reading),
            controls: &[],
        });
        let peak = summary.mem_peak_mib.unwrap();
        assert_eq!((peak.value, peak.covered_secs), (3072, 50));
    }

    /// A Netdata agent that answers every data query with one row per 5
    /// seconds, as a slow collector or a coarse tier does, and whose
    /// framebuffer split moves within each grouped point
    ///
    /// Rows are stamped back from `before`, as Netdata groups them. The
    /// `memory_tail` chart holds 30 GiB in its oldest row and 1 GiB after
    /// it, like a previous attempt's tail caught in the first group
    async fn coarse_netdata() -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = Vec::new();
                let mut buffer = [0; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => request.extend_from_slice(&buffer[..read]),
                    }
                }
                let request = String::from_utf8_lossy(&request).into_owned();
                let query = request.split_whitespace().nth(1).unwrap_or_default();
                let param = |name: &str| {
                    query
                        .split(['?', '&'])
                        .find_map(|pair| pair.strip_prefix(&format!("{name}=")))
                        .unwrap_or_default()
                        .to_owned()
                };
                let after: i64 = param("after").parse().unwrap();
                let before: i64 = param("before").parse().unwrap();
                let gib = f64::from(1u32 << 30);
                // newest first, like Netdata
                let times: Vec<i64> = (0..)
                    .map(|step| before - step * 5)
                    .take_while(|at| *at >= after)
                    .collect();
                let oldest = times.last().copied();
                let chart = param("chart");
                let (labels, row) = match (chart.as_str(), param("group").as_str()) {
                    ("util", _) => (json!(["time", "gpu"]), json!([50.0])),
                    ("memory_tail", "max") => (
                        json!(["time", "free", "used", "reserved"]),
                        json!([2.0 * gib, 30.0 * gib, 0.0]),
                    ),
                    // free and used swapped between 30 and 2 GiB inside each point
                    ("memory", "max") => (
                        json!(["time", "free", "used", "reserved"]),
                        json!([30.0 * gib, 30.0 * gib, 0.0]),
                    ),
                    ("memory" | "memory_tail", _) => (
                        json!(["time", "free", "used", "reserved"]),
                        json!([16.0 * gib, 16.0 * gib, 0.0]),
                    ),
                    _ => (json!(["time", "power_draw"]), json!([100.0])),
                };
                let rows: Vec<Value> = times
                    .into_iter()
                    .map(|at| {
                        let mut values = vec![json!(at)];
                        values.extend(row.as_array().unwrap().iter().cloned());
                        if chart == "memory_tail" && Some(at) != oldest {
                            values[2] = json!(gib);
                        }
                        Value::Array(values)
                    })
                    .collect();
                let body = json!({ "labels": labels, "data": rows }).to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        port
    }

    #[tokio::test]
    async fn an_end_read_counts_the_returned_spacing_and_the_physical_memory_size() {
        let port = coarse_netdata().await;
        let charts = NetdataCharts {
            url: format!("http://127.0.0.1:{port}").parse().unwrap(),
            utilization: Some("util".into()),
            power: Some("power".into()),
            memory: Some("memory".into()),
        };
        let start = Utc.with_ymd_and_hms(2026, 11, 9, 12, 0, 0).unwrap();
        let end = start + chrono::Duration::seconds(60);
        let reading = EndReading::read(&charts, start, end, Some(500.0), Duration::from_secs(2))
            .await
            .unwrap();

        // 12 rows 5 seconds apart, the oldest holding the minute's first five
        // seconds, cover the whole minute
        assert_eq!(reading.util.coverage.netdata_ms, 60_000);
        assert_eq!(reading.power_pct.coverage.netdata_ms, 60_000);
        let summary = GpuSummary::build(&SummaryInput {
            attempt: TaskId::new(),
            run_number: 1,
            step: 0,
            gpu_uuid: None,
            started_at: Some(start),
            ended_at: end,
            watch: None,
            end_reading: Some(&reading),
            controls: &[],
        });
        let util = summary.util_avg.unwrap();
        assert_eq!((util.value, util.coverage_pct), (50, 100));
        assert_eq!(summary.power_pct_avg.unwrap().value, 20);

        // the peak in use is the busiest moment, and the size stays physical
        assert_eq!(summary.mem_peak_mib.unwrap().value, 30 * 1024);
        assert_eq!(summary.mem_total_mib.unwrap().value, 32 * 1024);

        // two seconds later the oldest group begins before the start, so its
        // 30 GiB belongs to the previous attempt and stays out
        let charts = NetdataCharts {
            memory: Some("memory_tail".into()),
            ..charts
        };
        let end = start + chrono::Duration::seconds(62);
        let reading = EndReading::read(&charts, start, end, Some(500.0), Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(reading.mem_used.peak, Some(1 << 30));
        assert_eq!(reading.mem_used.coverage.netdata_ms, 60_000);
        assert_eq!(reading.util.coverage.netdata_ms, 60_000);
    }
}
