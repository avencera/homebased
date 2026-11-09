//! Stored state of the GPU lease guard: its targets and trips
//!
//! A trip and the stop it requests commit together, so the queue actor's
//! next reconcile enacts the stop exactly as it does a preemption: a yield
//! writes the yield file, a restart or cancel sets the cancellation marker

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

use super::fmt_time;
use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::fleet::protocol::{ClusterProtocolVersion, LEASE_IDLE_PROTOCOL};
use crate::machine::MachineId;
use crate::queue::gpu::GpuUuid;
use crate::queue::lease_guard::{LeaseIdleTrip, TripAction};
use crate::queue::{ActiveRun, JobId, Preemption, RunPhase, StepWorkload, StopCause};
use crate::store::Store;

/// One GPU run the guard watches
#[derive(Debug, Clone)]
pub struct LeaseTarget {
    /// The running attempt
    pub run: ActiveRun,
    /// GPU the attempt launched on
    pub gpu_uuid: GpuUuid,
    /// The job's preemption mode
    pub preempt: Preemption,
    /// This attempt's trip, once it tripped
    pub trip: Option<LeaseIdleTrip>,
}

/// Lease notes of one job for `resource jobs` and the web UI
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobLeaseNote {
    /// The job's latest guard trip
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_idle: Option<LeaseIdleTrip>,
    /// When a still-pending yield was requested of its active run
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub yield_requested_at: Option<DateTime<Utc>>,
}

impl Store {
    /// Executing GPU task steps of `machine`, and those told to yield that
    /// still hold their GPU
    pub fn lease_guard_targets(&self, machine: MachineId) -> Result<Vec<LeaseTarget>, AppError> {
        let mut targets = Vec::new();
        for record in self.resources_on(machine)? {
            let Some(run) = record.run else { continue };
            let watched = match run.phase {
                RunPhase::Executing { .. } => true,
                RunPhase::Stopping {
                    started_at: Some(_),
                    cause,
                    ..
                } => cause == StopCause::Yield,
                _ => false,
            };
            if !watched
                || record.resource.device.is_none()
                || self.require_task(run.task)?.status() != ProcessStatus::Running
            {
                continue;
            }
            let Some(gpu_uuid) = self.run_gpu_uuid(run.task)? else {
                continue;
            };
            // a container's processes run under dockerd without the run's
            // marker, so the guard cannot match them to NVML's PIDs
            let spec = self.require_job(run.job)?.spec;
            if matches!(spec.steps.get(run.step), Some(StepWorkload::Container(_))) {
                continue;
            }
            targets.push(LeaseTarget {
                preempt: spec.preempt,
                trip: self.lease_idle_trip(run.task)?,
                gpu_uuid,
                run,
            });
        }
        Ok(targets)
    }

    /// Record a trip of `task`'s attempt and commit the stop its job's mode
    /// calls for
    ///
    /// Returns `None` when the attempt is no longer the active run or
    /// already tripped. The action and the trip number are chosen here, in
    /// the same transaction as the stop
    pub fn commit_lease_trip(
        &self,
        task: TaskId,
        gpu_uuid: GpuUuid,
        idle_since: DateTime<Utc>,
        idle_after: std::time::Duration,
        now: DateTime<Utc>,
    ) -> Result<Option<(ActiveRun, LeaseIdleTrip)>, AppError> {
        self.immediate(|| {
            let Some(run) = self.run_of_task(task)? else {
                return Ok(None);
            };
            if self.lease_idle_trip(task)?.is_some() {
                return Ok(None);
            }
            let job = self.require_job(run.job)?;
            let earlier = self.lease_idle_trips(run.job)?.len();
            let earlier = u32::try_from(earlier).unwrap_or(u32::MAX);
            let action = TripAction::for_job(job.spec.preempt, earlier);
            let trip = LeaseIdleTrip {
                attempt: task,
                gpu_uuid,
                idle_since,
                tripped_at: now,
                idle_after_secs: idle_after.as_secs(),
                action,
                trip: earlier.saturating_add(1),
                escalated_at: None,
            };
            let cause = match action {
                TripAction::Yield => StopCause::Yield,
                TripAction::Restart => StopCause::Restart,
                TripAction::Cancel => StopCause::UserCancel,
            };
            let run = self.request_stop(run.resource, Some(task), cause, now)?;
            self.save_lease_trip(run.job, &trip)?;
            Ok(Some((run, trip)))
        })
    }

    /// Turn a guard-requested yield that the run has not honored into a
    /// cancel, and return the updated trip
    pub fn escalate_lease_yield(
        &self,
        task: TaskId,
        now: DateTime<Utc>,
    ) -> Result<Option<(ActiveRun, LeaseIdleTrip)>, AppError> {
        self.immediate(|| {
            let Some(run) = self.run_of_task(task)? else {
                return Ok(None);
            };
            let Some(mut trip) = self.lease_idle_trip(task)? else {
                return Ok(None);
            };
            if trip.escalated_at.is_some() || trip.action != TripAction::Yield {
                return Ok(None);
            }
            trip.escalated_at = Some(now);
            let run = self.request_stop(run.resource, Some(task), StopCause::UserCancel, now)?;
            self.save_lease_trip(run.job, &trip)?;
            Ok(Some((run, trip)))
        })
    }

    /// The trip of one attempt, if it tripped
    pub(super) fn lease_idle_trip(&self, task: TaskId) -> Result<Option<LeaseIdleTrip>, AppError> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT trip_json FROM lease_idle_trips WHERE task_id = ?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        raw.map(|raw| serde_json::from_str(&raw).map_err(AppError::from))
            .transpose()
    }

    /// The trip an event ending `task`'s attempt carries: the attempt's own,
    /// or for a final event the job's latest
    ///
    /// Each step runs as its own attempt, and one that tripped and then
    /// finished its step ends with no event, so the final event reports it
    pub(super) fn lease_idle_for_event(
        &self,
        job: JobId,
        task: TaskId,
        last: bool,
    ) -> Result<Option<LeaseIdleTrip>, AppError> {
        match self.lease_idle_trip(task)? {
            Some(trip) => Ok(Some(trip)),
            None if last => Ok(self.lease_idle_trips(job)?.pop()),
            None => Ok(None),
        }
    }

    /// Every trip of one job, oldest first
    pub(super) fn lease_idle_trips(&self, job: JobId) -> Result<Vec<LeaseIdleTrip>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT trip_json FROM lease_idle_trips WHERE job_id = ?1 ORDER BY created_at, rowid",
        )?;
        let raws = statement
            .query_map([job.to_string()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        raws.iter()
            .map(|raw| serde_json::from_str(raw).map_err(AppError::from))
            .collect()
    }

    /// Lease notes of `machine`'s jobs that have any
    pub(super) fn job_lease_notes(
        &self,
        machine: MachineId,
        jobs: impl Iterator<Item = JobId>,
    ) -> Result<BTreeMap<JobId, JobLeaseNote>, AppError> {
        let pending: BTreeMap<JobId, DateTime<Utc>> = self
            .resources_on(machine)?
            .into_iter()
            .filter_map(|record| {
                let run = record.run?;
                match run.phase {
                    RunPhase::Stopping {
                        cause: StopCause::Yield,
                        requested_at,
                        ..
                    } => Some((run.job, requested_at)),
                    _ => None,
                }
            })
            .collect();
        let mut notes = BTreeMap::new();
        for job in jobs {
            let note = JobLeaseNote {
                lease_idle: self.lease_idle_trips(job)?.pop(),
                yield_requested_at: pending.get(&job).copied(),
            };
            if note != JobLeaseNote::default() {
                notes.insert(job, note);
            }
        }
        Ok(notes)
    }

    /// Whether `job`'s origin can read `lease_idle` on its events
    ///
    /// An origin on an older protocol drops the field when it stores an
    /// event, so a retry that still carries it would then be refused
    pub(super) fn origin_reads_lease_idle(&self, job: JobId) -> Result<bool, AppError> {
        let (machine, origin, protocol): (String, String, Option<u32>) = self.conn.query_row(
            "SELECT machine, origin_machine, origin_protocol FROM resource_jobs WHERE id = ?1",
            [job.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        Ok(machine == origin
            || protocol
                .is_some_and(|version| ClusterProtocolVersion(version) >= LEASE_IDLE_PROTOCOL))
    }

    fn run_of_task(&self, task: TaskId) -> Result<Option<ActiveRun>, AppError> {
        let Some(checkpoint) = self.run_checkpoint(task)? else {
            return Ok(None);
        };
        Ok(Some(checkpoint.run).filter(|run| {
            matches!(
                run.phase,
                RunPhase::Executing { .. }
                    | RunPhase::Stopping {
                        started_at: Some(_),
                        ..
                    }
            )
        }))
    }

    fn save_lease_trip(&self, job: JobId, trip: &LeaseIdleTrip) -> Result<(), AppError> {
        self.conn.execute(
            "INSERT INTO lease_idle_trips (task_id, job_id, trip_json, created_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (task_id) DO UPDATE SET trip_json = excluded.trip_json",
            params![
                trip.attempt.to_string(),
                job.to_string(),
                serde_json::to_string(trip)?,
                fmt_time(trip.tripped_at),
            ],
        )?;
        Ok(())
    }
}
