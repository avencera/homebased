//! GPU idle accounting, agent controls, atomic `JOB_GPU_IDLE` production, and
//! the per-attempt GPU summaries of final job events

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, params};

use super::{event_run, fmt_time, parse_time};
use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::fleet::protocol::{ClusterProtocolVersion, GPU_IDLE_PROTOCOL};
use crate::machine::MachineId;
use crate::queue::gpu::GpuUuid;
use crate::queue::gpu_idle::summary::{EndReading, SummaryInput};
use crate::queue::gpu_idle::{
    AttemptWatch, GpuIdleAttemptView, GpuIdleControl, GpuIdleControlRecord, GpuIdleControlResult,
    GpuIdleDetail, GpuIdleNotice, GpuIdleSpec, GpuSummary, SampleSource, Silence,
};
use crate::queue::{
    ActiveRun, JobEvent, JobId, OperationId, QueueError, ResourceLabel, ResourceName, RunPhase,
    StopCause,
};
use crate::store::Store;

/// One attempt the idle watcher samples
#[derive(Debug, Clone)]
pub struct GpuIdleTarget {
    /// The running attempt
    pub run: ActiveRun,
    /// When its child started; samples before it are never counted
    pub started_at: DateTime<Utc>,
    /// GPU the attempt launched on
    pub gpu_uuid: GpuUuid,
    /// Name of the attempt's resource
    pub resource_name: ResourceName,
    /// Display label of the attempt's resource
    pub resource_label: Option<ResourceLabel>,
    /// The job spec's setting
    pub spec: Option<GpuIdleSpec>,
    /// Silence at the time of the read
    pub silence: Silence,
    /// Whether the job's origin can read `JOB_GPU_IDLE`; the attempt is
    /// still counted when it cannot, but no alert is built
    pub notifiable: bool,
    /// Stored accounting, or `None` before the first tick
    pub watch: Option<AttemptWatch>,
}

/// Whether a run's phase is one the watcher counts
///
/// A run told to yield still holds its GPU, and a hung run that ignores the
/// yield is the worst case, so it stays watched. Other stops end the run soon
fn watched_since(phase: &RunPhase) -> Option<DateTime<Utc>> {
    match phase {
        RunPhase::Executing { started_at } => Some(*started_at),
        RunPhase::Stopping {
            started_at: Some(started_at),
            cause: StopCause::Yield,
            ..
        } => Some(*started_at),
        _ => None,
    }
}

impl Store {
    /// Attempts of `machine` the idle watcher samples now
    ///
    /// Only running attempts on an indexed resource whose GPU UUID was saved
    /// at launch qualify; others cannot be matched to a GPU
    pub fn gpu_idle_targets(
        &self,
        machine: MachineId,
        now: DateTime<Utc>,
    ) -> Result<Vec<GpuIdleTarget>, AppError> {
        let mut targets = Vec::new();
        for record in self.resources_on(machine)? {
            let Some(run) = record.run else { continue };
            let Some(started_at) = watched_since(&run.phase) else {
                continue;
            };
            if record.resource.device.is_none()
                || self.require_task(run.task)?.status() != ProcessStatus::Running
            {
                continue;
            }
            let Some(gpu_uuid) = self.run_gpu_uuid(run.task)? else {
                continue;
            };
            let job = self.require_job(run.job)?;
            targets.push(GpuIdleTarget {
                started_at,
                gpu_uuid,
                resource_name: record.resource.name,
                resource_label: record.resource.label,
                spec: job.spec.gpu_idle,
                silence: self.gpu_idle_silence(run.job, now)?,
                notifiable: self.origin_reads_gpu_idle(run.job)?,
                watch: self.gpu_idle_watch(run.task)?,
                run,
            });
        }
        Ok(targets)
    }

    fn run_gpu_uuid(&self, task: TaskId) -> Result<Option<GpuUuid>, AppError> {
        let uuid: Option<Option<String>> = self
            .conn
            .query_row(
                "SELECT gpu_uuid FROM resource_run_history WHERE task_id = ?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        uuid.flatten()
            .map(|raw| GpuUuid::parse(&raw).map_err(|message| QueueError::Corrupt { message }))
            .transpose()
            .map_err(AppError::from)
    }

    /// Stored accounting of one attempt
    pub fn gpu_idle_watch(&self, task: TaskId) -> Result<Option<AttemptWatch>, AppError> {
        let json: Option<String> = self
            .conn
            .query_row(
                "SELECT watch_json FROM resource_gpu_idle_attempts WHERE task_id = ?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        json.map(|json| serde_json::from_str(&json).map_err(AppError::from))
            .transpose()
    }

    fn write_gpu_idle_watch(
        &self,
        run: &ActiveRun,
        watch: &AttemptWatch,
        source: Option<SampleSource>,
    ) -> Result<(), AppError> {
        self.conn.execute(
            "INSERT INTO resource_gpu_idle_attempts (task_id, job_id, started_at, first_busy_at,
                episode, idle_buckets, busy_buckets, unknown_buckets, alerts, source, watch_json,
                updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT (task_id) DO UPDATE SET first_busy_at = ?4, episode = ?5,
                idle_buckets = ?6, busy_buckets = ?7, unknown_buckets = ?8, alerts = ?9,
                source = COALESCE(?10, source), watch_json = ?11, updated_at = ?12",
            params![
                run.task.to_string(),
                run.job.to_string(),
                fmt_time(watch.started_at),
                watch.first_busy_at.map(fmt_time),
                watch.episode,
                sql_count(watch.totals.idle)?,
                sql_count(watch.totals.busy)?,
                sql_count(watch.totals.unknown)?,
                watch.alerts,
                source.map(SampleSource::as_str),
                serde_json::to_string(watch)?,
                fmt_time(Utc::now()),
            ],
        )?;
        Ok(())
    }

    /// Save one tick's accounting and, when `alert` is given, produce it
    ///
    /// The run, its phase, the silence, and the origin's protocol are reread
    /// in the same transaction as the append, so a run that ended or a snooze
    /// recorded since the watcher's read wins. Returns whether the alert was
    /// produced; the saved accounting records it only then
    pub fn commit_gpu_idle(
        &self,
        task: TaskId,
        mut watch: AttemptWatch,
        source: Option<SampleSource>,
        alert: Option<GpuIdleNotice>,
        now: DateTime<Utc>,
    ) -> Result<bool, AppError> {
        self.immediate(|| {
            let Some(checkpoint) = self.run_checkpoint(task)? else {
                return Ok(false);
            };
            let run = checkpoint.run;
            if watched_since(&run.phase).is_none()
                || self.require_task(task)?.status() != ProcessStatus::Running
            {
                return Ok(false);
            }
            let produced = match alert {
                Some(notice)
                    if !self.gpu_idle_silence(run.job, now)?.silenced()
                        && self.origin_reads_gpu_idle(run.job)? =>
                {
                    self.append_gpu_idle_event(run.job, event_run(&run), notice)?;
                    watch.record_alert(now);
                    true
                }
                _ => false,
            };
            self.write_gpu_idle_watch(&run, &watch, source)?;
            Ok(produced)
        })
    }

    /// Whether the job's origin can parse `JOB_GPU_IDLE`
    ///
    /// An origin on an older cluster protocol would refuse the event and stall
    /// every later event of the job behind it, so it gets none
    fn origin_reads_gpu_idle(&self, job: JobId) -> Result<bool, AppError> {
        let (machine, origin, protocol): (String, String, Option<u32>) = self.conn.query_row(
            "SELECT machine, origin_machine, origin_protocol FROM resource_jobs WHERE id = ?1",
            [job.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        Ok(machine == origin
            || protocol.is_some_and(|version| ClusterProtocolVersion(version) >= GPU_IDLE_PROTOCOL))
    }

    /// Whether a stored `JOB_GPU_IDLE` still describes the attempt's episode
    /// and is not silenced, checked before every delivery attempt
    pub(super) fn gpu_idle_notice_is_current(
        &self,
        job: JobId,
        event: &JobEvent,
    ) -> Result<bool, AppError> {
        let notice = event
            .gpu_idle
            .as_ref()
            .ok_or_else(|| super::corrupt("GPU idle event has no notice"))?;
        let running = self
            .run_checkpoint(notice.attempt)?
            .is_some_and(|checkpoint| {
                checkpoint.run.job == job && watched_since(&checkpoint.run.phase).is_some()
            });
        let episode = self
            .gpu_idle_watch(notice.attempt)?
            .map(|watch| watch.episode);
        Ok(running
            && episode == Some(notice.episode)
            && !self.gpu_idle_silence(job, Utc::now())?.silenced())
    }

    /// The job's silence at `now`, from its latest control and its spec
    pub fn gpu_idle_silence(&self, job: JobId, now: DateTime<Utc>) -> Result<Silence, AppError> {
        let spec_off = matches!(self.require_job(job)?.spec.gpu_idle, Some(GpuIdleSpec::Off));
        let latest = self.gpu_idle_controls(job)?.pop();
        Ok(Silence::from_latest(latest.as_ref(), spec_off, now))
    }

    fn gpu_idle_controls(&self, job: JobId) -> Result<Vec<GpuIdleControlRecord>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT action, until, reason, created_at FROM resource_gpu_idle_controls
             WHERE job_id = ?1 ORDER BY id",
        )?;
        let rows = statement
            .query_map([job.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(action, until, reason, at)| {
                Ok(GpuIdleControlRecord {
                    action,
                    until: until.as_deref().map(parse_time).transpose()?,
                    reason,
                    at: parse_time(&at)?,
                })
            })
            .collect()
    }

    /// Record an agent's snooze, off switch, or clear for one job
    ///
    /// A snooze's end is computed here, once: a replay of `operation` returns
    /// the stored result, so a retried command never extends the snooze
    pub fn set_gpu_idle_control(
        &self,
        machine: MachineId,
        operation: OperationId,
        job: JobId,
        control: &GpuIdleControl,
        now: DateTime<Utc>,
    ) -> Result<GpuIdleControlResult, AppError> {
        control
            .validate()
            .map_err(|message| AppError::Usage { message })?;
        let content = serde_json::json!({ "job": job, "control": control });
        self.immediate(|| {
            self.with_operation(machine, operation, "gpu_idle", &content, || {
                self.require_machine_job(machine, job)?;
                let (until, reason) = match control {
                    GpuIdleControl::Snooze { duration, reason } => {
                        let span =
                            chrono::Duration::from_std(*duration).map_err(|_| AppError::Usage {
                                message: "snooze does not fit the clock".into(),
                            })?;
                        let until = now.checked_add_signed(span).ok_or(AppError::Usage {
                            message: "snooze does not fit the clock".into(),
                        })?;
                        (Some(until), Some(reason.trim().to_owned()))
                    }
                    GpuIdleControl::Off { reason } => (None, Some(reason.trim().to_owned())),
                    GpuIdleControl::Clear => (None, None),
                };
                self.conn.execute(
                    "INSERT INTO resource_gpu_idle_controls (job_id, action, until, reason,
                        created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        job.to_string(),
                        control.action(),
                        until.map(fmt_time),
                        reason,
                        fmt_time(now),
                    ],
                )?;
                Ok(GpuIdleControlResult {
                    job_id: job,
                    control: GpuIdleControlRecord {
                        action: control.action().to_owned(),
                        until,
                        reason,
                        at: now,
                    },
                    silence: self.gpu_idle_silence(job, now)?,
                })
            })
        })
    }

    /// Controls and per-attempt accounting of one job, for `resource job show`
    pub fn gpu_idle_detail(&self, job: JobId) -> Result<GpuIdleDetail, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT task_id, episode, alerts, idle_buckets, busy_buckets, unknown_buckets,
                first_busy_at, source
             FROM resource_gpu_idle_attempts WHERE job_id = ?1 ORDER BY started_at",
        )?;
        let rows = statement
            .query_map([job.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let attempts = rows
            .into_iter()
            .map(
                |(task, episode, alerts, idle, busy, unknown, first_busy, source)| {
                    Ok(GpuIdleAttemptView {
                        task: task.parse()?,
                        episode,
                        alerts,
                        idle_buckets: read_count(idle)?,
                        busy_buckets: read_count(busy)?,
                        unknown_buckets: read_count(unknown)?,
                        first_busy_at: first_busy.as_deref().map(parse_time).transpose()?,
                        source: source.as_deref().and_then(SampleSource::from_storage),
                    })
                },
            )
            .collect::<Result<Vec<_>, AppError>>()?;
        Ok(GpuIdleDetail {
            silence: self.gpu_idle_silence(job, Utc::now())?,
            controls: self.gpu_idle_controls(job)?,
            attempts,
        })
    }
}

impl Store {
    /// Compute and save the GPU summary of an attempt that is ending
    ///
    /// Runs in the attempt's terminal transaction, before its job event is
    /// appended, and reads only stored data: the watcher's totals and any
    /// end-of-attempt read the worker saved. An attempt on a resource with no
    /// GPU index gets none. A summary already saved is returned unchanged
    pub(super) fn save_gpu_summary(
        &self,
        run: &ActiveRun,
        ended_at: DateTime<Utc>,
    ) -> Result<Option<GpuSummary>, AppError> {
        if let Some(saved) = self.gpu_summary(run.task)? {
            return Ok(Some(saved));
        }
        if self
            .require_resource(run.resource)?
            .resource
            .device
            .is_none()
        {
            return Ok(None);
        }
        let started_at = match run.phase {
            RunPhase::Executing { started_at } => Some(started_at),
            RunPhase::Stopping { started_at, .. } => started_at,
            _ => None,
        };
        let end_json: Option<String> = self
            .conn
            .query_row(
                "SELECT gpu_end_json FROM resource_run_history WHERE task_id = ?1",
                [run.task.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let end_reading: Option<EndReading> =
            end_json.as_deref().map(serde_json::from_str).transpose()?;
        let gpu_uuid = self.run_gpu_uuid(run.task)?;
        let watch = self.gpu_idle_watch(run.task)?;
        let controls = self.gpu_idle_controls(run.job)?;
        let summary = GpuSummary::build(&SummaryInput {
            attempt: run.task,
            run_number: run.run_number.get(),
            step: run.step.get(),
            gpu_uuid: gpu_uuid.as_ref(),
            started_at,
            ended_at,
            watch: watch.as_ref(),
            end_reading: end_reading.as_ref(),
            controls: &controls,
        });
        self.conn.execute(
            "UPDATE resource_run_history SET gpu_summary_json = ?2 WHERE task_id = ?1",
            params![run.task.to_string(), serde_json::to_string(&summary)?],
        )?;
        Ok(Some(summary))
    }

    /// Save the worker's whole-attempt Netdata read, just before its exit commit
    pub fn save_gpu_end_reading(&self, task: TaskId, reading: &EndReading) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE resource_run_history SET gpu_end_json = ?2
             WHERE task_id = ?1 AND gpu_summary_json IS NULL",
            params![task.to_string(), serde_json::to_string(reading)?],
        )?;
        Ok(())
    }

    /// The saved GPU summary of one attempt
    pub fn gpu_summary(&self, task: TaskId) -> Result<Option<GpuSummary>, AppError> {
        let json: Option<String> = self
            .conn
            .query_row(
                "SELECT gpu_summary_json FROM resource_run_history WHERE task_id = ?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        json.map(|json| serde_json::from_str(&json).map_err(AppError::from))
            .transpose()
    }

    /// The saved GPU summaries of every attempt of a job, oldest first
    pub(super) fn gpu_summaries(&self, job: JobId) -> Result<Vec<GpuSummary>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT h.gpu_summary_json FROM tasks t
             JOIN resource_run_history h ON h.task_id = t.id
             WHERE t.resource_job_id = ?1 AND h.gpu_summary_json IS NOT NULL
             ORDER BY t.run_number",
        )?;
        let rows = statement
            .query_map([job.to_string()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.iter()
            .map(|json| serde_json::from_str(json).map_err(AppError::from))
            .collect()
    }
}

fn sql_count(count: u64) -> Result<i64, AppError> {
    i64::try_from(count).map_err(|_| super::corrupt("bucket count exceeds SQLite range").into())
}

fn read_count(count: i64) -> Result<u64, AppError> {
    u64::try_from(count).map_err(|_| super::corrupt("negative bucket count").into())
}
