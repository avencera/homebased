//! The GPU idle watcher: samples each running attempt's GPU and produces
//! `JOB_GPU_IDLE` on the queue's machine
//!
//! Every tick reads the watched attempts from the store, counts the buckets
//! that completed since the last tick, and saves the accounting. Netdata is
//! read with absolute range queries clamped to the attempt's start, so a
//! restart or a slow tick catches up from history. `nvidia-smi` probes run
//! only while some attempt needs them, and a failed probe leaves its buckets
//! unknown, never idle. Linux and NVIDIA only

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use ractor::ActorRef;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use super::actors::{StoreMsg, call};
use crate::error::AppError;
use crate::home::Home;
use crate::machine::MachineId;
use crate::queue::gpu::GpuUuid;
use crate::queue::gpu_idle::netdata::{Chart, GpuCharts, Group, Netdata, RangeQuery};
use crate::queue::gpu_idle::samples::{Samples, bucketize, min_samples};
use crate::queue::gpu_idle::summary::{NetdataCharts, SampleSpacing};
use crate::queue::gpu_idle::{
    AlertDue, AttemptWatch, GpuIdleConfig, GpuIdleNotice, SampleSource, SourceChoice, Thresholds,
    next_steps, nvidia_smi, process,
};
use crate::store::queue::GpuIdleTarget;

/// Bound on one Netdata request
const NETDATA_TIMEOUT: Duration = Duration::from_secs(3);
/// Bound on one `nvidia-smi` probe
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// How often the power limits are reread
const POWER_LIMIT_EVERY: Duration = Duration::from_secs(3600);
/// Shortest gap between two chart listings for one GPU, whether it has no
/// charts or its charts went stale or failed
const REDISCOVER_EVERY: Duration = Duration::from_secs(60);
/// Netdata stores a sample a moment after it is collected
const NETDATA_LAG: Duration = Duration::from_secs(2);
/// Longest stretch one tick reads back; older buckets count as unknown
const MAX_CATCH_UP: Duration = Duration::from_secs(3600);
/// Logged when a tick's buckets got no utilization; a symptom, so it never
/// replaces the condition that caused it
const NO_SAMPLES: &str = "no utilization samples";
/// Logged when Netdata lists no utilization chart for a GPU
const NO_CHART: &str = "no Netdata utilization chart";
/// How long the busiest process is sampled for the alert text
const PROCESS_SAMPLE: Duration = Duration::from_secs(1);

/// Run the watcher until the daemon stops
pub(crate) async fn run(
    store: ActorRef<StoreMsg>,
    home: Home,
    machine: MachineId,
    config: GpuIdleConfig,
) {
    if config.source == SourceChoice::Off || !cfg!(target_os = "linux") {
        return;
    }
    let mut watcher = Watcher::new(store, home, machine, config);
    let mut ticks = tokio::time::interval(watcher.config.defaults.interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        if let Err(error) = watcher.tick().await {
            warn!("GPU idle watcher: {error}");
        }
    }
}

struct Watcher {
    store: ActorRef<StoreMsg>,
    home: Home,
    machine: MachineId,
    config: GpuIdleConfig,
    netdata: Netdata,
    charts: HashMap<GpuUuid, GpuCharts>,
    /// When each GPU's charts were last looked for because it had none
    listed_at: HashMap<GpuUuid, Instant>,
    /// When each GPU's charts were last looked for because they went stale
    /// or failed
    relisted_at: HashMap<GpuUuid, Instant>,
    power_limits: HashMap<GpuUuid, f64>,
    limits_read_at: Option<Instant>,
    probes: HashMap<GpuUuid, Samples>,
    logged: HashSet<(GpuUuid, &'static str)>,
    /// The latest condition of each lane, the reason a summary gives when
    /// an attempt got no samples
    problems: HashMap<GpuUuid, String>,
}

impl Watcher {
    fn new(
        store: ActorRef<StoreMsg>,
        home: Home,
        machine: MachineId,
        config: GpuIdleConfig,
    ) -> Self {
        Self {
            netdata: Netdata::new(config.netdata_url.clone(), NETDATA_TIMEOUT),
            store,
            home,
            machine,
            config,
            charts: HashMap::new(),
            listed_at: HashMap::new(),
            relisted_at: HashMap::new(),
            power_limits: HashMap::new(),
            limits_read_at: None,
            probes: HashMap::new(),
            logged: HashSet::new(),
            problems: HashMap::new(),
        }
    }

    /// Log a lane's condition once until it changes
    fn log_once(&mut self, uuid: &GpuUuid, what: &'static str, detail: &str) {
        let problem = format!("{what}: {detail}");
        match what {
            NO_SAMPLES => {}
            // a missing chart after a failed listing is that failure's symptom
            NO_CHART => {
                self.problems.entry(uuid.clone()).or_insert(problem);
            }
            _ => {
                self.problems.insert(uuid.clone(), problem);
            }
        }
        if self.logged.insert((uuid.clone(), what)) {
            info!(gpu = %uuid, "GPU idle watcher: {what}: {detail}");
        }
    }

    async fn tick(&mut self) -> Result<(), AppError> {
        let now = Utc::now();
        let machine = self.machine;
        let targets = call(&self.store, |reply| StoreMsg::GpuIdleTargets {
            machine,
            now,
            reply,
        })
        .await?;
        if targets.is_empty() {
            self.probes.clear();
            return Ok(());
        }
        self.refresh_power_limits().await;

        let uses_netdata = matches!(
            self.config.source,
            SourceChoice::Auto | SourceChoice::Netdata
        );
        if uses_netdata {
            let missing = targets
                .iter()
                .any(|target| self.util_chart(&target.gpu_uuid).is_none());
            if missing {
                self.discover(&targets, false).await;
            }
        }

        let mut fallback = Vec::new();
        for target in targets {
            let source = if uses_netdata && self.util_chart(&target.gpu_uuid).is_some() {
                SampleSource::Netdata
            } else if self.config.source == SourceChoice::Netdata {
                let uuid = target.gpu_uuid.clone();
                self.log_once(&uuid, NO_CHART, "buckets stay unknown");
                // no samples: every bucket is unknown, and the counters stay complete
                self.advance(target, None, Samples::default(), now).await?;
                continue;
            } else {
                fallback.push(target);
                continue;
            };
            match self.netdata_samples(&target, now).await {
                Some(samples) => self.advance(target, Some(source), samples, now).await?,
                None if self.config.source == SourceChoice::Auto => fallback.push(target),
                None => self.advance(target, None, Samples::default(), now).await?,
            }
        }
        if fallback.is_empty() {
            self.probes.clear();
            return Ok(());
        }
        self.probe(&fallback, now).await;
        for target in fallback {
            let samples = self
                .probes
                .get(&target.gpu_uuid)
                .cloned()
                .unwrap_or_default();
            self.advance(target, Some(SampleSource::NvidiaSmi), samples, now)
                .await?;
        }
        Ok(())
    }

    fn util_chart(&self, uuid: &GpuUuid) -> Option<&Chart> {
        self.charts.get(uuid)?.utilization.as_ref()
    }

    /// List Netdata's charts again for the targets' GPUs, each at most once
    /// per [`REDISCOVER_EVERY`]
    ///
    /// Without `stale`, only GPUs that have no charts are looked for. With it,
    /// the targets' charts went stale or failed and are looked for even when
    /// known; that limit is kept apart, so a GPU found a moment ago can still
    /// be relisted once. Only the listed GPUs' charts change
    async fn discover(&mut self, targets: &[GpuIdleTarget], stale: bool) {
        let now = Instant::now();
        let mut wanted = Vec::new();
        for target in targets {
            let uuid = &target.gpu_uuid;
            if !stale && self.util_chart(uuid).is_some() {
                continue;
            }
            let limits = if stale {
                &mut self.relisted_at
            } else {
                &mut self.listed_at
            };
            if limits
                .get(uuid)
                .is_some_and(|at| now.duration_since(*at) < REDISCOVER_EVERY)
            {
                continue;
            }
            limits.insert(uuid.clone(), now);
            wanted.push(uuid.clone());
        }
        if wanted.is_empty() {
            return;
        }
        match self.netdata.charts(&wanted).await {
            Ok(mut charts) => {
                for uuid in &wanted {
                    self.problems.remove(uuid);
                    match charts.remove(uuid) {
                        Some(found) => self.charts.insert(uuid.clone(), found),
                        None => self.charts.remove(uuid),
                    };
                }
            }
            Err(error) => {
                for uuid in &wanted {
                    self.charts.remove(uuid);
                    self.log_once(uuid, "Netdata unavailable", &error);
                }
            }
        }
    }

    async fn refresh_power_limits(&mut self) {
        if self
            .limits_read_at
            .is_some_and(|at| at.elapsed() < POWER_LIMIT_EVERY)
        {
            return;
        }
        self.limits_read_at = Some(Instant::now());
        match nvidia_smi::power_limits(PROBE_TIMEOUT).await {
            Ok(limits) => self.power_limits = limits,
            Err(error) => warn!("GPU power limits unavailable: {error}"),
        }
    }

    /// One `nvidia-smi` probe for every GPU that needs one, kept for a window
    async fn probe(&mut self, targets: &[GpuIdleTarget], now: DateTime<Utc>) {
        let at = now.timestamp_millis();
        match nvidia_smi::read(PROBE_TIMEOUT).await {
            Ok(readings) => {
                for reading in readings {
                    if let Some(limit) = reading.power_limit_watts {
                        self.power_limits.insert(reading.uuid.clone(), limit);
                    }
                    let samples = self.probes.entry(reading.uuid).or_default();
                    samples.extend(Samples {
                        util: reading.util.map(|value| (at, value)).into_iter().collect(),
                        power_watts: reading
                            .power_watts
                            .map(|value| (at, value))
                            .into_iter()
                            .collect(),
                        memory_used_bytes: reading
                            .memory_used_bytes
                            .map(|value| (at, value))
                            .into_iter()
                            .collect(),
                        memory_total_bytes: reading
                            .memory_total_bytes
                            .map(|value| (at, value))
                            .into_iter()
                            .collect(),
                    });
                }
            }
            Err(error) => {
                for target in targets {
                    let uuid = target.gpu_uuid.clone();
                    self.log_once(&uuid, "nvidia-smi probe failed", &error);
                }
            }
        }
        // a bucket is counted on the tick after it completes, so a few
        // buckets of history are enough
        let keep = self.config.defaults.interval.saturating_mul(6);
        let cutoff = at - i64::try_from(keep.as_millis()).unwrap_or(i64::MAX);
        let wanted: HashSet<&GpuUuid> = targets.iter().map(|target| &target.gpu_uuid).collect();
        self.probes.retain(|uuid, _| wanted.contains(uuid));
        for samples in self.probes.values_mut() {
            samples.forget_before(cutoff);
        }
    }

    /// Samples since the attempt's last counted bucket, or `None` when
    /// Netdata failed or went stale and a fresh listing found no other charts
    async fn netdata_samples(
        &mut self,
        target: &GpuIdleTarget,
        now: DateTime<Utc>,
    ) -> Option<Samples> {
        let thresholds = self.thresholds(target);
        let watch = start_watch(target, now, &thresholds);
        let end = now - chrono::Duration::from_std(NETDATA_LAG).ok()?;
        let starts = watch.due_buckets(end, &thresholds);
        let (Some(first), Some(last)) = (starts.first(), starts.last()) else {
            return Some(Samples::default());
        };
        let to = *last + chrono::Duration::from_std(thresholds.bucket).ok()?;
        // both bounds are inclusive
        let after = first.timestamp();
        let before = (to - chrono::Duration::milliseconds(1)).timestamp();
        let stale_after = thresholds
            .bucket
            .saturating_mul(2)
            .max(Duration::from_secs(5));
        let stale_before = to.timestamp() - i64::try_from(stale_after.as_secs()).unwrap_or(0);
        for attempt in 0..2 {
            let charts = self.charts.get(&target.gpu_uuid).cloned()?;
            let utilization = charts.utilization.as_ref()?;
            let read = match self.netdata.range(&range(utilization, after, before)).await {
                Ok(series) if series.newest().is_some_and(|newest| newest >= stale_before) => {
                    Ok(series)
                }
                Ok(_) => Err(("Netdata data is stale", "using nvidia-smi".to_owned())),
                Err(error) => Err(("Netdata query failed", error)),
            };
            let util = match read {
                Ok(series) => series,
                Err((what, detail)) => {
                    // a collector restart or config change can retire the saved
                    // chart ids, leaving them stale or answering 404, so list the
                    // charts again and retry only when that found different ones
                    if attempt == 0 {
                        self.discover(std::slice::from_ref(target), true).await;
                        if self.charts.get(&target.gpu_uuid) != Some(&charts) {
                            continue;
                        }
                    }
                    let uuid = target.gpu_uuid.clone();
                    self.log_once(&uuid, what, &detail);
                    return None;
                }
            };
            self.logged.retain(|(uuid, _)| uuid != &target.gpu_uuid);
            self.problems.remove(&target.gpu_uuid);
            let mut samples = Samples {
                util: millis(util.values("gpu")),
                ..Samples::default()
            };
            // power and memory only enrich the alert; their failure never
            // blocks a utilization verdict
            if let Some(chart) = &charts.power
                && let Ok(series) = self.netdata.range(&range(chart, after, before)).await
            {
                samples.power_watts = millis(series.values("power_draw"));
            }
            if let Some(chart) = &charts.memory
                && let Ok(series) = self.netdata.range(&range(chart, after, before)).await
            {
                samples.memory_used_bytes = whole(series.values("used"));
                let mut totals: HashMap<i64, f64> = HashMap::new();
                for dimension in ["free", "used", "reserved"] {
                    for (at, value) in series.values(dimension) {
                        *totals.entry(at).or_default() += value;
                    }
                }
                samples.memory_total_bytes = whole(totals.into_iter());
            }
            return Some(samples);
        }
        None
    }

    /// Time one sample of each field stands for: a chart's collection
    /// interval, or one probe per tick
    fn spacing(&self, uuid: &GpuUuid, source: SampleSource, bucket_ms: u64) -> SampleSpacing {
        let charts = match source {
            SampleSource::Netdata => self.charts.get(uuid),
            SampleSource::NvidiaSmi => None,
        };
        let Some(charts) = charts else {
            return SampleSpacing::uniform(bucket_ms);
        };
        let every = |chart: &Option<Chart>| {
            chart
                .as_ref()
                .map_or(1000, |chart| u64::from(chart.update_every) * 1000)
        };
        SampleSpacing {
            util: every(&charts.utilization),
            power: every(&charts.power),
            memory: every(&charts.memory),
        }
    }

    fn thresholds(&self, target: &GpuIdleTarget) -> Thresholds {
        self.config.defaults.thresholds(target.spec)
    }

    /// Count the attempt's completed buckets, then save them with any alert due
    async fn advance(
        &mut self,
        target: GpuIdleTarget,
        source: Option<SampleSource>,
        mut samples: Samples,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        let thresholds = self.thresholds(&target);
        let mut watch = start_watch(&target, now, &thresholds);
        samples.clamp_from(target.started_at);
        let end = match source {
            Some(SampleSource::Netdata) => {
                now - chrono::Duration::from_std(NETDATA_LAG).unwrap_or_default()
            }
            _ => now,
        };
        let starts = watch.due_buckets(end, &thresholds);
        let sample_every = match source {
            Some(SampleSource::Netdata) => self
                .util_chart(&target.gpu_uuid)
                .map_or(Duration::from_secs(1), |chart| {
                    Duration::from_secs(u64::from(chart.update_every))
                }),
            // one probe per tick, so one sample per bucket
            _ => thresholds.bucket,
        };
        let minimum = min_samples(thresholds.bucket, sample_every);
        let buckets = bucketize(
            &samples,
            &starts,
            thresholds.bucket,
            minimum,
            thresholds.util_below,
        );
        if !starts.is_empty() && buckets.iter().all(|bucket| bucket.util_peak.is_none()) {
            let uuid = target.gpu_uuid.clone();
            self.log_once(&uuid, NO_SAMPLES, "buckets stay unknown");
            let latest = self.problems.get(&uuid);
            watch.usage.problem = Some(match (source, latest) {
                (Some(source), Some(latest)) => {
                    format!("{} gave no utilization ({latest})", source.as_str())
                }
                (Some(source), None) => format!("{} gave no utilization", source.as_str()),
                (None, Some(latest)) => latest.clone(),
                (None, None) => "no utilization source".into(),
            });
        }
        let bucket_ms = u64::try_from(thresholds.bucket.as_millis()).unwrap_or(u64::MAX);
        if let (Some(source), Some(first)) = (source, starts.first()) {
            let from = first.timestamp_millis();
            let count = i64::try_from(starts.len()).unwrap_or(i64::MAX);
            let to = from
                .saturating_add(count.saturating_mul(i64::try_from(bucket_ms).unwrap_or(i64::MAX)));
            let spacing = self.spacing(&target.gpu_uuid, source, bucket_ms);
            let limit = self.power_limits.get(&target.gpu_uuid).copied();
            if limit.is_some() {
                watch.usage.power_limit_watts = limit;
            }
            if source == SampleSource::Netdata
                && let Some(charts) = self.charts.get(&target.gpu_uuid)
            {
                let id = |chart: &Option<Chart>| chart.as_ref().map(|chart| chart.id.clone());
                watch.usage.netdata = Some(NetdataCharts {
                    url: self.config.netdata_url.clone(),
                    utilization: id(&charts.utilization),
                    power: id(&charts.power),
                    memory: id(&charts.memory),
                });
            }
            watch
                .usage
                .add_samples(&samples, (from, to), spacing, limit, source);
        }
        for bucket in buckets {
            if let Some(source) = source {
                watch.usage.add_bucket(bucket.verdict, bucket_ms, source);
            }
            watch.push(bucket, &thresholds);
        }

        let held = target.silence.silenced() || !target.notifiable;
        let due = watch.alert_due(now, &thresholds, held);
        let alert = match (due, source) {
            (Some(due), Some(source)) => Some(Box::new(
                self.notice(&target, &watch, &thresholds, due, source, now)
                    .await,
            )),
            _ => None,
        };
        let task = target.run.task;
        call(&self.store, |reply| StoreMsg::GpuIdleCommit {
            task,
            watch: Box::new(watch),
            source,
            alert,
            now,
            reply,
        })
        .await?;
        Ok(())
    }

    async fn notice(
        &self,
        target: &GpuIdleTarget,
        watch: &AttemptWatch,
        thresholds: &Thresholds,
        due: AlertDue,
        source: SampleSource,
        now: DateTime<Utc>,
    ) -> GpuIdleNotice {
        let summary = watch.summary();
        let task = target.run.task;
        let idle_since = summary.idle_since.unwrap_or(target.started_at);
        let limit = self.power_limits.get(&target.gpu_uuid).copied();
        let process = tokio::task::spawn_blocking(move || process::busiest(task, PROCESS_SAMPLE))
            .await
            .ok()
            .flatten();
        let output = self.home.task_paths(task).output;
        GpuIdleNotice {
            attempt: task,
            episode: watch.episode,
            reminder: watch.current.alerts,
            never_busy: matches!(due, AlertDue::First { never_busy: true }),
            gpu_uuid: target.gpu_uuid.clone(),
            idle_since,
            idle_secs: (now - idle_since).to_std().unwrap_or_default().as_secs(),
            window_secs: thresholds.window.as_secs(),
            idle_buckets: summary.idle,
            known_buckets: summary.known,
            util_below: thresholds.util_below,
            util_peak_pct: summary.util_peak.map(rounded),
            power_peak_watts: summary.power_peak_watts.map(rounded),
            power_peak_pct: summary
                .power_peak_watts
                .zip(limit)
                .map(|(watts, limit)| rounded(watts / limit * 100.0)),
            memory_used_mib: summary.memory_used_bytes.map(mebibytes),
            memory_total_mib: summary.memory_total_bytes.map(mebibytes),
            last_output_at: super::actors::task::last_output_at(&output),
            process,
            source,
            next_steps: next_steps(target.run.job, task),
        }
    }
}

/// The attempt's stored accounting, or a new one; a gap longer than the
/// catch-up bound is counted as unknown without reading it
fn start_watch(
    target: &GpuIdleTarget,
    now: DateTime<Utc>,
    thresholds: &Thresholds,
) -> AttemptWatch {
    let mut watch = target
        .watch
        .clone()
        .unwrap_or_else(|| AttemptWatch::new(target.started_at));
    if let Ok(bound) = chrono::Duration::from_std(MAX_CATCH_UP)
        && watch.processed_through < now - bound
    {
        watch.skip_to(now - bound, thresholds);
    }
    watch
}

fn range(chart: &Chart, after: i64, before: i64) -> RangeQuery<'_> {
    let span = u32::try_from((before - after).max(1)).unwrap_or(u32::MAX);
    RangeQuery {
        chart: &chart.id,
        after,
        before,
        points: span.div_ceil(chart.update_every.max(1)),
        group: Group::Max,
    }
}

fn millis(values: impl Iterator<Item = (i64, f64)>) -> Vec<(i64, f64)> {
    values.map(|(at, value)| (at * 1000, value)).collect()
}

fn whole(values: impl Iterator<Item = (i64, f64)>) -> Vec<(i64, u64)> {
    values
        .filter(|(_, value)| value.is_finite() && *value >= 0.0)
        .map(|(at, value)| (at * 1000, value as u64))
        .collect()
}

fn rounded(value: f64) -> u32 {
    value.round().clamp(0.0, f64::from(u32::MAX)) as u32
}

fn mebibytes(bytes: u64) -> u64 {
    bytes / (1024 * 1024)
}
