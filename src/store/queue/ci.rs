//! CI slot lanes, drains, run timeouts, and job timings
//!
//! Slots are queue resources marked with a class, so the queue's one active
//! run per resource bounds each class's runs. Each class is scheduled as its
//! own pool beside the GPU pool, which never sees a slot or a CI job

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, params};

use super::rows::{RESOURCE_SELECT, RawResource};
use super::{JobRecord, NewJob, ResourceRecord, corrupt, parse_uuid};
use crate::domain::TaskId;
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::ci::{
    ClassOffer, Drain, JobClass, JobTiming, KillAfter, RunTimeout, SlotCapacity, SlotNumber,
    TimingOutcome,
};
use crate::queue::schedule::{JobView, NoticeThresholds, QueuedState, ResourceView, Snapshot};
use crate::queue::{
    ActiveRun, JobEventKind, JobId, JobState, QueueError, ResourceId, RunPhase, StopCause,
};
use crate::store::{Store, fmt_time, parse_time};

/// One class's slots and jobs on a machine, ready for `decide`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CiPool {
    /// Class the pool serves
    pub class: JobClass,
    /// Slots that take new runs
    pub capacity: SlotCapacity,
    /// Slots of the class holding a run, retired ones included
    pub busy: u32,
    /// Open slots with their runs, and busy retired ones, with the class's
    /// non-terminal jobs in serving order
    pub snapshot: Snapshot,
}

impl CiPool {
    /// How many more runs the class may start now: capacity minus every
    /// busy slot, so a cut takes effect as retired slots finish
    #[must_use]
    pub fn room(&self) -> usize {
        usize::try_from(self.capacity.get().saturating_sub(self.busy)).unwrap_or(0)
    }
}

/// What a CI run with `git` executes: the checkout wrapper around its
/// command, from the slot's clone
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutRun {
    /// The wrapped command
    pub workload: crate::domain::Workload,
    /// The slot's clone, which the actor creates before the reservation
    pub cwd: std::path::PathBuf,
}

impl Store {
    /// Set `class`'s capacity on `machine`, creating or reopening slots
    /// `1..=capacity` and retiring the rest
    ///
    /// A retired slot finishes its run and takes no new one. A name in
    /// `1..=capacity` already used by a resource that is not this class's
    /// slot is refused
    pub fn set_ci_capacity(
        &self,
        machine: MachineId,
        class: &JobClass,
        capacity: SlotCapacity,
    ) -> Result<Vec<ResourceRecord>, AppError> {
        self.immediate(|| {
            for number in 1..=u32::from(SlotCapacity::MAX) {
                let slot = SlotNumber::try_from(number)?;
                let name = class.lane_name(slot);
                let existing = self
                    .resources_on(machine)?
                    .into_iter()
                    .find(|record| record.resource.name == name);
                let open = number <= capacity.get();
                match existing {
                    Some(record) => {
                        let ours = record
                            .resource
                            .slot
                            .as_ref()
                            .is_some_and(|lane| &lane.class == class && lane.slot == slot);
                        // a resource that happens to share a name past the new
                        // capacity is not ours to retire, so leave it alone
                        if !ours && !open {
                            continue;
                        }
                        if !ours {
                            return Err(QueueError::InvalidCiInput {
                                what: "capacity",
                                value: capacity.get().to_string(),
                                reason: format!(
                                    "resource {name} already exists and is not a {class} slot"
                                ),
                            }
                            .into());
                        }
                        self.conn.execute(
                            "UPDATE resources SET ci_retired = ?1 WHERE id = ?2",
                            params![!open, record.resource.id.to_string()],
                        )?;
                    }
                    None if open => {
                        self.conn.execute(
                            "INSERT INTO resources (id, machine, name, created_at, origin,
                                ci_class, ci_slot, ci_retired)
                             VALUES (?1, ?2, ?3, ?4, 'manual', ?5, ?6, 0)",
                            params![
                                ResourceId::new().to_string(),
                                machine.to_string(),
                                name.as_str(),
                                fmt_time(Utc::now()),
                                class.as_str(),
                                slot.get(),
                            ],
                        )?;
                    }
                    None => {}
                }
            }
            self.ci_slots(machine)
        })
    }

    /// Every CI slot of `machine`, retired ones included, by name
    pub fn ci_slots(&self, machine: MachineId) -> Result<Vec<ResourceRecord>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "{RESOURCE_SELECT} WHERE machine = ?1 AND ci_class IS NOT NULL ORDER BY ci_class, ci_slot"
        ))?;
        let raws = statement
            .query_map([machine.to_string()], RawResource::read)?
            .collect::<Result<Vec<_>, _>>()?;
        raws.into_iter().map(RawResource::parse).collect()
    }

    /// The drain of `machine`, if any
    pub fn ci_drain(&self, machine: MachineId) -> Result<Option<Drain>, AppError> {
        let raw: Option<(String, String)> = self
            .conn
            .query_row(
                "SELECT reason, since FROM ci_drain WHERE machine = ?1",
                [machine.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        raw.map(|(reason, since)| {
            Ok(Drain {
                reason,
                since: parse_time(&since)?,
            })
        })
        .transpose()
    }

    /// Drain `machine`, or reopen it with `None`; an existing drain keeps its
    /// start time and takes the new reason
    pub fn set_ci_drain(
        &self,
        machine: MachineId,
        drain: Option<Drain>,
    ) -> Result<Option<Drain>, AppError> {
        self.immediate(|| {
            match drain {
                Some(drain) => self.conn.execute(
                    "INSERT INTO ci_drain (machine, reason, since) VALUES (?1, ?2, ?3)
                     ON CONFLICT (machine) DO UPDATE SET reason = excluded.reason",
                    params![machine.to_string(), drain.reason, fmt_time(drain.since)],
                )?,
                None => self.conn.execute(
                    "DELETE FROM ci_drain WHERE machine = ?1",
                    [machine.to_string()],
                )?,
            };
            self.ci_drain(machine)
        })
    }

    /// One pool per class with at least one slot on `machine`
    pub fn ci_pools(
        &self,
        machine: MachineId,
        now: DateTime<Utc>,
    ) -> Result<Vec<CiPool>, AppError> {
        let mut pools: BTreeMap<JobClass, CiPool> = BTreeMap::new();
        for record in self.ci_slots(machine)? {
            let Some(lane) = record.resource.slot else {
                continue;
            };
            let pool = pools.entry(lane.class.clone()).or_insert_with(|| CiPool {
                class: lane.class.clone(),
                capacity: SlotCapacity::default(),
                busy: 0,
                snapshot: empty_snapshot(now),
            });
            if !lane.retired {
                pool.capacity = pool.capacity.grow();
            }
            if record.run.is_some() {
                pool.busy += 1;
            }
            // an idle retired slot takes no run, so `decide` never sees it
            if !lane.retired || record.run.is_some() {
                pool.snapshot.resources.push(ResourceView {
                    id: record.resource.id,
                    name: record.resource.name,
                    run: record.run,
                });
            }
        }
        for job in self.machine_queue(machine)? {
            let Some(ci) = &job.spec.ci else {
                continue;
            };
            if let Some(pool) = pools.get_mut(&ci.class) {
                pool.snapshot.queue.push(job_view(&job));
            }
        }
        Ok(pools.into_values().collect())
    }

    /// Refuse a CI job whose class has no open slot on `machine`, since it
    /// would wait there forever
    pub(super) fn check_class_offered(
        &self,
        machine: MachineId,
        class: &JobClass,
    ) -> Result<(), AppError> {
        let offers = self.ci_class_offers(machine)?;
        if offers
            .iter()
            .any(|offer| &offer.class == class && offer.capacity.get() > 0)
        {
            return Ok(());
        }
        Err(QueueError::ClassNotOffered {
            class: class.clone(),
            offered: offers
                .into_iter()
                .filter(|offer| offer.capacity.get() > 0)
                .map(|offer| offer.class)
                .collect(),
        }
        .into())
    }

    /// What `machine` offers per class: capacity, busy slots, and queued jobs
    pub fn ci_class_offers(&self, machine: MachineId) -> Result<Vec<ClassOffer>, AppError> {
        Ok(self
            .ci_pools(machine, Utc::now())?
            .into_iter()
            .map(|pool| ClassOffer {
                queued: count(
                    pool.snapshot
                        .queue
                        .iter()
                        .filter(|job| job.state == QueuedState::Queued),
                ),
                class: pool.class,
                capacity: pool.capacity,
                busy: pool.busy,
            })
            .collect())
    }

    /// Stop every CI run on `machine` past its `kill_after`, recording why
    ///
    /// The stop and its timeout row commit together, so the run's end always
    /// finds the timeout and fails the job. Returns the stopping runs
    pub fn expire_ci_runs(
        &self,
        machine: MachineId,
        now: DateTime<Utc>,
    ) -> Result<Vec<ActiveRun>, AppError> {
        self.immediate(|| {
            let mut stopped = Vec::new();
            for record in self.ci_slots(machine)? {
                let Some(run) = record.run else {
                    continue;
                };
                let RunPhase::Executing { started_at } = run.phase else {
                    continue;
                };
                let job = self.require_job(run.job)?;
                let Some(ci) = &job.spec.ci else {
                    continue;
                };
                let ran = (now - started_at).to_std().unwrap_or_default();
                if ran < ci.kill_after.get() {
                    continue;
                }
                self.conn.execute(
                    "INSERT OR IGNORE INTO ci_run_timeouts
                        (task_id, job_id, limit_secs, ran_secs, requested_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        run.task.to_string(),
                        run.job.to_string(),
                        secs(ci.kill_after.get()),
                        secs(ran),
                        fmt_time(now),
                    ],
                )?;
                stopped.push(self.request_stop(
                    run.resource,
                    Some(run.task),
                    StopCause::UserCancel,
                    now,
                )?);
            }
            Ok(stopped)
        })
    }

    /// The timeout that stopped run `task`, if the queue killed it
    pub(super) fn run_timeout(&self, task: TaskId) -> Result<Option<RunTimeout>, AppError> {
        let raw: Option<(i64, i64, String)> = self
            .conn
            .query_row(
                "SELECT limit_secs, ran_secs, requested_at FROM ci_run_timeouts WHERE task_id = ?1",
                [task.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        raw.map(|(limit, ran_secs, at)| {
            let limit = u64::try_from(limit)
                .ok()
                .and_then(|limit| KillAfter::try_from(Duration::from_secs(limit)).ok())
                .ok_or_else(|| corrupt(format!("bad stored kill_after {limit}")))?;
            Ok(RunTimeout {
                limit,
                ran_secs: u64::try_from(ran_secs).map_err(|_| corrupt("negative run time"))?,
                at: parse_time(&at)?,
            })
        })
        .transpose()
    }

    /// Start the timing of a CI job the authority just accepted
    pub(super) fn open_ci_timing(&self, new: &NewJob, now: DateTime<Utc>) -> Result<(), AppError> {
        let Some(ci) = &new.spec.ci else {
            return Ok(());
        };
        // a submit time from a skewed origin clock never runs past acceptance
        let submitted_at = new.submitted_at.map_or(now, |at| at.min(now));
        self.conn.execute(
            "INSERT OR IGNORE INTO ci_job_timings
                (job_id, machine, class, submitted_at, accepted_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                new.id.to_string(),
                new.machine.to_string(),
                ci.class.as_str(),
                fmt_time(submitted_at),
                fmt_time(now),
            ],
        )?;
        Ok(())
    }

    /// Mark a CI job's run reserved; its wait ends here
    pub(super) fn start_ci_timing(&self, job: JobId, now: DateTime<Utc>) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE ci_job_timings SET started_at = ?2 WHERE job_id = ?1 AND started_at IS NULL",
            params![job.to_string(), fmt_time(now)],
        )?;
        Ok(())
    }

    /// Close a CI job's timing with its final event
    pub(super) fn close_ci_timing(
        &self,
        job: JobId,
        event: JobEventKind,
        timed_out: bool,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        let outcome = match event {
            JobEventKind::JobSucceeded => TimingOutcome::Succeeded,
            JobEventKind::JobFailed if timed_out => TimingOutcome::TimedOut,
            JobEventKind::JobFailed => TimingOutcome::Failed,
            JobEventKind::JobCancelled => TimingOutcome::Cancelled,
            _ => return Ok(()),
        };
        self.conn.execute(
            "UPDATE ci_job_timings SET ended_at = ?2, outcome = ?3
             WHERE job_id = ?1 AND ended_at IS NULL",
            params![job.to_string(), fmt_time(now), outcome.as_str()],
        )?;
        Ok(())
    }

    /// Timings of CI jobs this machine accepted that ended at or after
    /// `since`, or have not ended
    pub fn ci_timings(&self, since: DateTime<Utc>) -> Result<Vec<JobTiming>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT job_id, machine, class, submitted_at, started_at, ended_at, outcome
             FROM ci_job_timings WHERE ended_at IS NULL OR ended_at >= ?1
             ORDER BY submitted_at",
        )?;
        let raws = statement
            .query_map([fmt_time(since)], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        raws.into_iter()
            .map(
                |(job, machine, class, submitted, started, ended, outcome)| {
                    Ok(JobTiming {
                        job: JobId::from_uuid(parse_uuid(&job)?),
                        machine: machine.parse()?,
                        class: JobClass::parse(&class)
                            .map_err(|_| corrupt(format!("bad stored class {class:?}")))?,
                        submitted_at: parse_time(&submitted)?,
                        started_at: started.as_deref().map(parse_time).transpose()?,
                        ended_at: ended.as_deref().map(parse_time).transpose()?,
                        outcome: outcome.as_deref().map(TimingOutcome::parse).transpose()?,
                    })
                },
            )
            .collect()
    }
}

fn empty_snapshot(now: DateTime<Utc>) -> Snapshot {
    Snapshot {
        now,
        resources: Vec::new(),
        queue: Vec::new(),
        episode: None,
        thresholds: NoticeThresholds::default(),
    }
}

/// The scheduler's view of a job
pub(super) fn job_view(job: &JobRecord) -> JobView {
    JobView {
        id: job.id,
        priority: job.priority,
        target: job.target,
        preempt: job.spec.preempt,
        state: match job.state {
            JobState::Active { resource } => QueuedState::Active(resource),
            _ => QueuedState::Queued,
        },
    }
}

/// Whole seconds as SQLite stores them; a kill limit is at most 24 hours
fn secs(span: Duration) -> i64 {
    i64::try_from(span.as_secs()).unwrap_or(i64::MAX)
}

fn count<T>(items: impl Iterator<Item = T>) -> u32 {
    u32::try_from(items.count()).unwrap_or(u32::MAX)
}
