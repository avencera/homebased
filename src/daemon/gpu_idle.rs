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
/// Shortest gap between two chart listings while a GPU has no charts
const REDISCOVER_EVERY: Duration = Duration::from_secs(60);
/// Netdata stores a sample a moment after it is collected
const NETDATA_LAG: Duration = Duration::from_secs(2);
/// Longest stretch one tick reads back; older buckets count as unknown
const MAX_CATCH_UP: Duration = Duration::from_secs(3600);
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
    listed_at: Option<Instant>,
    power_limits: HashMap<GpuUuid, f64>,
    limits_read_at: Option<Instant>,
    probes: HashMap<GpuUuid, Samples>,
    logged: HashSet<(GpuUuid, &'static str)>,
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
            listed_at: None,
            power_limits: HashMap::new(),
            limits_read_at: None,
            probes: HashMap::new(),
            logged: HashSet::new(),
        }
    }

    /// Log a lane's condition once until it changes
    fn log_once(&mut self, uuid: &GpuUuid, what: &'static str, detail: &str) {
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
                self.log_once(
                    &uuid,
                    "no Netdata utilization chart",
                    "buckets stay unknown",
                );
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

    /// List Netdata's charts again; `force` skips the rate limit after stale data
    async fn discover(&mut self, targets: &[GpuIdleTarget], force: bool) {
        if !force
            && self
                .listed_at
                .is_some_and(|at| at.elapsed() < REDISCOVER_EVERY)
        {
            return;
        }
        self.listed_at = Some(Instant::now());
        let wanted: Vec<GpuUuid> = targets
            .iter()
            .map(|target| target.gpu_uuid.clone())
            .collect();
        match self.netdata.charts(&wanted).await {
            Ok(charts) => self.charts = charts,
            Err(error) => {
                self.charts.clear();
                for uuid in &wanted {
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
    /// Netdata failed or its data stayed stale after a fresh listing
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
        // Netdata's lower bound is exclusive, so it starts one second early
        let after = first.timestamp() - 1;
        let before = (to - chrono::Duration::milliseconds(1)).timestamp();
        let stale_after = thresholds
            .bucket
            .saturating_mul(2)
            .max(Duration::from_secs(5));
        let stale_before = to.timestamp() - i64::try_from(stale_after.as_secs()).unwrap_or(0);
        for attempt in 0..2 {
            let charts = self.charts.get(&target.gpu_uuid).cloned()?;
            let utilization = charts.utilization.as_ref()?;
            let util = match self.netdata.range(&range(utilization, after, before)).await {
                Ok(series) => series,
                Err(error) => {
                    let uuid = target.gpu_uuid.clone();
                    self.log_once(&uuid, "Netdata query failed", &error);
                    return None;
                }
            };
            if util.newest().is_none_or(|newest| newest < stale_before) {
                if attempt == 0 {
                    // the collector may have restarted under new chart ids
                    self.discover(std::slice::from_ref(target), true).await;
                    continue;
                }
                let uuid = target.gpu_uuid.clone();
                self.log_once(&uuid, "Netdata data is stale", "using nvidia-smi");
                return None;
            }
            self.logged.retain(|(uuid, _)| uuid != &target.gpu_uuid);
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
            self.log_once(&uuid, "no utilization samples", "buckets stay unknown");
        }
        for bucket in buckets {
            watch.push(bucket, &thresholds);
        }

        let due = watch.alert_due(now, &thresholds, target.silence.silenced());
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
