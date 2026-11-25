//! Durable job delivery; inbox acceptance and route cursors commit together

use chrono::Utc;
use rusqlite::{OptionalExtension, params};

use super::fmt_time;
use crate::error::AppError;
use crate::events::EventAcceptance;
use crate::machine::MachineId;
use crate::pool::{MachineLost, MachineReleased, NotRerun, ReleaseReason, Rerun, RerunPolicy};
use crate::queue::delivery::{
    self, HeldJobEnding, JobRefusal, JobRoute, JobSubmission, RoutedJobEvent,
};
use crate::queue::spec::MachineSelector;
use crate::queue::{JobEvent, JobEventKind, JobId, MoveRefusal, Priority, QueueError, Target};
use crate::store::Store;
use crate::store::sleep::{end_hold, read_hold, record_held};
use crate::task_wait::{EventClass, HeldEvent, HoldTarget};

/// Sequence cursors of one job's origin inbox
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobCursors {
    /// Last event stored in the origin inbox
    pub accepted: u64,
    /// Last event delivered to the thread or suppressed
    pub settled: u64,
}

impl Store {
    /// Read the origin's job route
    pub fn job_route(&self, job: JobId) -> Result<Option<JobRoute>, AppError> {
        let saved: Option<String> = self
            .conn
            .query_row(
                "SELECT route_json FROM resource_job_routes WHERE job_id = ?1",
                [job.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        saved
            .map(|json| {
                let route: JobRoute = serde_json::from_str(&json)?;
                if route.job != job
                    || route.thread != route.spec.thread
                    || route.digest != route.spec.digest()?
                {
                    return Err(QueueError::Corrupt {
                        message: format!("job route {job} has inconsistent identity"),
                    }
                    .into());
                }
                Ok(route)
            })
            .transpose()
    }

    /// The first final event in the job's origin inbox, which outlives the
    /// machine that sent it
    pub fn job_final_event(&self, job: JobId) -> Result<Option<JobEvent>, AppError> {
        let mut statement = self
            .conn
            .prepare("SELECT event_json FROM resource_job_inbox WHERE job_id = ?1 ORDER BY seq")?;
        let events = statement
            .query_map([job.to_string()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for json in events {
            let envelope: RoutedJobEvent = serde_json::from_str(&json)?;
            if envelope.event.event.final_state().is_some() {
                return Ok(Some(envelope.event));
            }
        }
        Ok(None)
    }

    /// Last event stored in the job's origin inbox, and last one delivered
    pub fn job_route_cursors(&self, job: JobId) -> Result<Option<JobCursors>, AppError> {
        let saved: Option<(i64, i64)> = self
            .conn
            .query_row(
                "SELECT accepted_seq, settled_seq FROM resource_job_routes WHERE job_id = ?1",
                [job.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        saved
            .map(|(accepted, settled)| {
                Ok(JobCursors {
                    accepted: read_seq(accepted)?,
                    settled: read_seq(settled)?,
                })
            })
            .transpose()
    }

    /// Save a route before sending; identical retries retain the original callback context
    pub fn insert_job_route(&self, route: &JobRoute) -> Result<JobRoute, AppError> {
        if route.thread != route.spec.thread
            || route.digest != route.spec.digest()?
            || !matches!(
                route.submission,
                JobSubmission::Unknown | JobSubmission::Held
            )
            || matches!(route.submission, JobSubmission::Held)
                && ((route.after.is_none() && route.spec.ci.is_none()) || !route.owes_final_event)
            || route.target.is_some()
        {
            return Err(QueueError::Invariant {
                message: "invalid new job route".into(),
            }
            .into());
        }
        self.immediate(|| {
            if let Some(saved) = self.job_route(route.job)? {
                if saved.digest != route.digest
                    || saved.authority != route.authority
                    || saved.origin != route.origin
                    || saved.thread != route.thread
                    || saved.after != route.after
                {
                    return Err(QueueError::JobConflict { job: route.job }.into());
                }
                return Ok(saved);
            }
            self.conn.execute(
                "INSERT INTO resource_job_routes(job_id,route_json,created_at) VALUES (?1,?2,?3)",
                params![
                    route.job.to_string(),
                    serde_json::to_string(route)?,
                    fmt_time(Utc::now())
                ],
            )?;
            Ok(route.clone())
        })
    }

    /// Routes the origin still owns sending: held for their `after` tasks, or
    /// released and not yet confirmed by the authority
    pub fn held_job_routes(&self) -> Result<Vec<JobRoute>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT job_id FROM resource_job_routes
             WHERE json_extract(route_json,'$.owes_final_event') = 1
               AND json_extract(route_json,'$.submission.state') IN ('held','unknown')
             ORDER BY created_at",
        )?;
        let jobs = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        jobs.into_iter()
            .filter_map(|job| match job.parse::<JobId>() {
                Ok(job) => self.job_route(job).transpose(),
                Err(error) => Some(Err(error.into())),
            })
            .collect()
    }

    /// Mark a held route sent, so a lost reply is resent under the same job
    /// id; any other route is returned unchanged
    pub fn release_held_job(&self, job: JobId) -> Result<JobRoute, AppError> {
        self.immediate(|| {
            let mut route = self
                .job_route(job)?
                .ok_or(QueueError::JobNotFound { job })?;
            if matches!(route.submission, JobSubmission::Held) {
                route.submission = JobSubmission::Unknown;
                self.conn.execute(
                    "UPDATE resource_job_routes SET route_json=?2 WHERE job_id=?1",
                    params![job.to_string(), serde_json::to_string(&route)?],
                )?;
            }
            Ok(route)
        })
    }

    /// Send a CI job held for placement to `authority`: mark it sent there,
    /// so a lost reply is resent under the same job id
    ///
    /// Returns `None` when the route is no longer held for placement, such as
    /// after a cancel
    pub fn place_held_job(
        &self,
        job: JobId,
        authority: MachineId,
    ) -> Result<Option<JobRoute>, AppError> {
        self.immediate(|| {
            let Some(mut route) = self.job_route(job)? else {
                return Ok(None);
            };
            if !matches!(route.submission, JobSubmission::Held) || route.spec.ci.is_none() {
                return Ok(None);
            }
            route.authority = authority;
            route.submission = JobSubmission::Unknown;
            self.save_route(&route)?;
            Ok(Some(route))
        })
    }

    /// Hold a placed CI job for placement again, after the machine it was
    /// sent to answered that it does not offer the class now
    ///
    /// That answer means the machine stored nothing, since it answers a job
    /// id it already has with the job, so another machine may take it. Only
    /// a job that may run on any machine is held again; `false` otherwise
    pub fn unplace_ci_job(&self, job: JobId) -> Result<bool, AppError> {
        self.immediate(|| {
            let Some(mut route) = self.job_route(job)? else {
                return Ok(false);
            };
            let placed = matches!(route.submission, JobSubmission::Unknown)
                && route.spec.ci.is_some()
                && route.spec.machine.is_none();
            if !placed {
                return Ok(false);
            }
            route.authority = route.origin;
            route.submission = JobSubmission::Held;
            self.save_route(&route)?;
            Ok(true)
        })
    }

    /// Change where and at what level a CI job held for placement may run
    ///
    /// `authority` is the pinned machine, or the origin when any machine may
    /// take it. Only the origin's route changes, since the job has not
    /// reached any queue
    pub fn move_held_ci_job(
        &self,
        job: JobId,
        machine: Option<MachineSelector>,
        authority: MachineId,
        priority: Option<Priority>,
    ) -> Result<JobRoute, AppError> {
        self.immediate(|| {
            let mut route = self
                .job_route(job)?
                .ok_or(QueueError::JobNotFound { job })?;
            if route.spec.ci.is_none() {
                return Err(QueueError::MoveRefused {
                    job,
                    reason: MoveRefusal::NotCi { job },
                }
                .into());
            }
            if !matches!(route.submission, JobSubmission::Held) {
                return Err(QueueError::MoveRefused {
                    job,
                    reason: MoveRefusal::Sent {
                        job,
                        machine: route.authority,
                    },
                }
                .into());
            }
            route.spec.machine = machine;
            if let Some(priority) = priority {
                route.spec.priority = priority;
            }
            route.authority = authority;
            route.digest = route.spec.digest()?;
            self.save_route(&route)?;
            Ok(route)
        })
    }

    fn save_route(&self, route: &JobRoute) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE resource_job_routes SET route_json=?2 WHERE job_id=?1",
            params![route.job.to_string(), serde_json::to_string(route)?],
        )?;
        Ok(())
    }

    /// End a held job before it joins the queue, committing its terminal
    /// event to the origin inbox in the same transaction
    ///
    /// A dependency ending applies only while the job is still held, since a
    /// sent job may already be queued; a refusal applies to the sent route
    /// that the authority refused. Anything else returns `false`
    pub fn end_held_job(&self, job: JobId, ending: HeldJobEnding) -> Result<bool, AppError> {
        self.immediate(|| self.end_held_job_inner(job, ending))
    }

    /// End a CI job its pool still holds for placement, with why the pool
    /// ended; a job placement sent to a machine meanwhile is that machine's
    /// to run or refuse, so it is left alone
    pub fn end_pool_job(&self, job: JobId, ending: HeldJobEnding) -> Result<bool, AppError> {
        self.immediate(|| {
            // a move may have pinned the job to a machine since the pool read it
            let pooled = self.job_route(job)?.is_some_and(|route| {
                matches!(route.submission, JobSubmission::Held)
                    && route.spec.ci.is_some()
                    && route.spec.machine.is_none()
            });
            if !pooled {
                return Ok(false);
            }
            self.end_held_job_inner(job, ending)
        })
    }

    /// Record the authority's definite refusal of a sent route
    ///
    /// The route's saved state decides, whichever path sent it: a job that
    /// was held owes its waiters a final event, so it ends with `JOB_FAILED`,
    /// while any other refusal is kept for identical submit retries. A route
    /// the authority confirmed meanwhile, or one that already ended, is left
    pub fn refuse_job(&self, job: JobId, refusal: JobRefusal) -> Result<(), AppError> {
        self.immediate(|| {
            let route = self
                .job_route(job)?
                .ok_or(QueueError::JobNotFound { job })?;
            if !matches!(route.submission, JobSubmission::Unknown) {
                return Ok(());
            }
            if route.owes_final_event {
                let ending = HeldJobEnding::Refused {
                    code: refusal.code,
                    message: refusal.message,
                };
                return self.end_held_job_inner(job, ending).map(drop);
            }
            let rejected = JobSubmission::Rejected {
                error: refusal.error,
                status: refusal.status,
            };
            let mut route = route;
            route.submission = rejected;
            self.conn.execute(
                "UPDATE resource_job_routes SET route_json=?2 WHERE job_id=?1",
                params![job.to_string(), serde_json::to_string(&route)?],
            )?;
            Ok(())
        })
    }

    fn end_held_job_inner(&self, job: JobId, ending: HeldJobEnding) -> Result<bool, AppError> {
        {
            let mut route = self
                .job_route(job)?
                .ok_or(QueueError::JobNotFound { job })?;
            let applies = match (&route.submission, &ending) {
                (
                    JobSubmission::Held,
                    HeldJobEnding::Dependency { .. } | HeldJobEnding::Requested,
                ) => true,
                (JobSubmission::Unknown, HeldJobEnding::Refused { .. }) => route.owes_final_event,
                // a pool that ended, or a machine that is gone, refuses a job
                // before it reached a queue
                (JobSubmission::Held, HeldJobEnding::Refused { .. }) => true,
                _ => false,
            };
            let accepted = self
                .job_route_cursors(job)?
                .ok_or(QueueError::JobNotFound { job })?
                .accepted;
            // a held job has no events of its own yet; a rerun keeps those of
            // its earlier runs, which end at its sequence base
            if !applies || accepted != route.seq_base() {
                return Ok(false);
            }
            let seq = accepted + 1;
            let stored_seq = sql_seq(seq)?;
            let kind = match ending {
                HeldJobEnding::Dependency { .. } | HeldJobEnding::Requested => {
                    JobEventKind::JobCancelled
                }
                HeldJobEnding::Refused { .. } => JobEventKind::JobFailed,
            };
            let envelope = RoutedJobEvent {
                origin: route.origin,
                authority: route.authority,
                digest: route.digest.clone(),
                event: JobEvent {
                    held_ending: Some(ending.clone()),
                    ..origin_event(job, seq, kind)
                },
            };
            route.submission = JobSubmission::Ended { ending };
            self.conn.execute(
                "INSERT INTO resource_job_inbox(job_id,seq,event_json) VALUES (?1,?2,?3)",
                params![
                    job.to_string(),
                    stored_seq,
                    serde_json::to_string(&envelope)?
                ],
            )?;
            self.conn.execute(
                "UPDATE resource_job_routes SET accepted_seq=?2,route_json=?3 WHERE job_id=?1",
                params![job.to_string(), stored_seq, serde_json::to_string(&route)?],
            )?;
            Ok(true)
        }
    }

    /// Rerun or end one job this origin sent to `machine` or holds pinned to
    /// it, once cloudlease confirmed the machine gone; `None` when the job is
    /// no longer that machine's or already has its final event
    ///
    /// The machine can never send another event. A job still held for the
    /// machine, pinned to it while it waited for its `after` entries or a
    /// slot, can never reach it, so it fails with `held_ending` code
    /// `machine_released`. A sent job `policy` lets run again gets a
    /// `JOB_PREEMPTED` carrying [`MachineLost`] and is held for another
    /// machine of its pool; any other gets a `JOB_FAILED` after the last
    /// event it sent, carrying `released`
    pub fn end_job_of_released_machine(
        &self,
        job: JobId,
        machine: MachineId,
        released: &MachineReleased,
        policy: &RerunPolicy,
    ) -> Result<Option<Rerun>, AppError> {
        self.immediate(|| {
            if self.held_route_on(job, machine)?.is_some() {
                let ending = HeldJobEnding::Refused {
                    code: "machine_released".to_owned(),
                    message: format!(
                        "machine {} of pool {} was released ({}) before the job reached it",
                        released.machine, released.pool, released.reason
                    ),
                };
                let ended = self.end_held_job_inner(job, ending)?;
                return Ok(ended.then_some(Rerun::End(None)));
            }

            let Some(route) = self.unfinished_route_on(job, machine)? else {
                return Ok(None);
            };
            let rerun = decide_rerun(policy, &released.reason, &route);
            match rerun {
                Rerun::Queue { attempt } => {
                    let lost = MachineLost {
                        pool: released.pool.clone(),
                        machine: released.machine.clone(),
                        reason: released.reason.clone(),
                        attempt,
                        max_attempts: policy.max_attempts,
                    };
                    self.queue_again(route, lost)?;
                }
                Rerun::End(not_rerun) => {
                    let released = MachineReleased {
                        attempt: route.attempt(),
                        not_rerun,
                        ..released.clone()
                    };
                    self.append_released_failure(route, &released)?;
                }
            }
            Ok(Some(rerun))
        })
    }

    /// The job's route while it was sent to `machine` and has no final event
    fn unfinished_route_on(
        &self,
        job: JobId,
        machine: MachineId,
    ) -> Result<Option<JobRoute>, AppError> {
        let route = self.job_route(job)?;
        let open = self.jobs_on(machine, OnMachine::Sent)?.contains(&job);
        Ok(route.filter(|_| open))
    }

    /// Record that someone asked to cancel a job this origin sent, before
    /// the cancel goes to `authority`; a cancel sent to any other machine
    /// cannot stop the job, so it is not recorded
    pub fn request_job_cancel(&self, job: JobId, authority: MachineId) -> Result<(), AppError> {
        let Some(mut route) = self.job_route(job)? else {
            return Ok(());
        };
        if route.cancel_requested || route.authority != authority {
            return Ok(());
        }
        route.cancel_requested = true;
        self.save_route(&route)
    }

    /// Jobs this origin sent to `machine` or holds pinned to it that have no
    /// final event, oldest first: what a machine cloudlease confirmed gone
    /// leaves to end or rerun
    pub fn jobs_of_released_machine(&self, machine: MachineId) -> Result<Vec<JobId>, AppError> {
        self.jobs_on(machine, OnMachine::SentOrHeld)
    }

    fn jobs_on(&self, machine: MachineId, which: OnMachine) -> Result<Vec<JobId>, AppError> {
        let states = match which {
            OnMachine::Sent => "('accepted', 'unknown')",
            OnMachine::SentOrHeld => "('accepted', 'unknown', 'held')",
        };
        let mut statement = self.conn.prepare(&format!(
            "SELECT r.job_id FROM resource_job_routes r
             WHERE json_extract(r.route_json, '$.authority') = ?1
               AND json_extract(r.route_json, '$.submission.state') IN {states}
               AND NOT EXISTS (
                   SELECT 1 FROM resource_job_inbox i
                   WHERE i.job_id = r.job_id
                     AND json_extract(i.event_json, '$.event.event')
                         IN ('JOB_SUCCEEDED', 'JOB_FAILED', 'JOB_CANCELLED'))
             ORDER BY r.created_at",
        ))?;
        let jobs = statement
            .query_map([machine.to_string()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        jobs.into_iter()
            .map(|job| job.parse().map_err(AppError::from))
            .collect()
    }

    /// The job's route while it is held pinned to `machine` with no final
    /// event
    fn held_route_on(&self, job: JobId, machine: MachineId) -> Result<Option<JobRoute>, AppError> {
        let held = self.jobs_on(machine, OnMachine::SentOrHeld)?.contains(&job);
        Ok(self
            .job_route(job)?
            .filter(|route| held && matches!(route.submission, JobSubmission::Held)))
    }

    /// Record that the job's machine was lost and hold it for placement
    /// again, as the attempt `lost` names
    ///
    /// The `JOB_PREEMPTED` closes the lost run's events, and the next
    /// machine's events follow it in the origin inbox
    fn queue_again(&self, mut route: JobRoute, lost: MachineLost) -> Result<(), AppError> {
        let job = route.job;
        let accepted = self
            .job_route_cursors(job)?
            .ok_or(QueueError::JobNotFound { job })?
            .accepted;
        let seq = accepted + 1;
        let stored_seq = sql_seq(seq)?;
        let envelope = RoutedJobEvent {
            origin: route.origin,
            authority: route.authority,
            digest: route.digest.clone(),
            event: JobEvent {
                machine_lost: Some(lost.clone()),
                ..origin_event(job, seq, JobEventKind::JobPreempted)
            },
        };
        route.rerun = Some(delivery::Rerun {
            attempt: lost.attempt,
            seq_base: seq,
        });
        route.authority = route.origin;
        route.target = None;
        route.submission = JobSubmission::Held;
        // the origin now owns sending it again, so it owes the final event
        route.owes_final_event = true;
        self.conn.execute(
            "INSERT INTO resource_job_inbox(job_id,seq,event_json) VALUES (?1,?2,?3)",
            params![
                job.to_string(),
                stored_seq,
                serde_json::to_string(&envelope)?
            ],
        )?;
        self.conn.execute(
            "UPDATE resource_job_routes SET accepted_seq=?2,route_json=?3 WHERE job_id=?1",
            params![job.to_string(), stored_seq, serde_json::to_string(&route)?],
        )?;
        Ok(())
    }

    fn append_released_failure(
        &self,
        route: JobRoute,
        released: &MachineReleased,
    ) -> Result<(), AppError> {
        let job = route.job;
        let accepted = self
            .job_route_cursors(job)?
            .ok_or(QueueError::JobNotFound { job })?
            .accepted;
        let seq = accepted + 1;
        let stored_seq = sql_seq(seq)?;
        let envelope = RoutedJobEvent {
            origin: route.origin,
            authority: route.authority,
            digest: route.digest.clone(),
            event: JobEvent {
                machine_released: Some(released.clone()),
                ..origin_event(job, seq, JobEventKind::JobFailed)
            },
        };
        let mut route = route;
        route.submission = JobSubmission::Accepted;
        self.conn.execute(
            "INSERT INTO resource_job_inbox(job_id,seq,event_json) VALUES (?1,?2,?3)",
            params![
                job.to_string(),
                stored_seq,
                serde_json::to_string(&envelope)?
            ],
        )?;
        self.conn.execute(
            "UPDATE resource_job_routes SET accepted_seq=?2,route_json=?3 WHERE job_id=?1",
            params![job.to_string(), stored_seq, serde_json::to_string(&route)?],
        )?;
        Ok(())
    }

    /// Resolve submission without regressing acceptance proved by an incoming event
    pub fn resolve_job_route(
        &self,
        job: JobId,
        submission: JobSubmission,
        target: Option<Target>,
    ) -> Result<(), AppError> {
        self.immediate(|| {
            let mut route = self
                .job_route(job)?
                .ok_or(QueueError::JobNotFound { job })?;
            if matches!(route.submission, JobSubmission::Accepted)
                && !matches!(submission, JobSubmission::Accepted)
            {
                return Err(QueueError::JobConflict { job }.into());
            }
            if matches!(route.submission, JobSubmission::Ended { .. })
                || matches!(route.submission, JobSubmission::Rejected { .. })
                    && route.submission != submission
                || route.target.is_some() && target.is_some() && route.target != target
            {
                return Err(QueueError::JobConflict { job }.into());
            }
            route.submission = submission;
            if target.is_some() {
                route.target = target;
            }
            self.conn.execute(
                "UPDATE resource_job_routes SET route_json=?2 WHERE job_id=?1",
                params![job.to_string(), serde_json::to_string(&route)?],
            )?;
            Ok(())
        })
    }

    /// Pending authority events, with one earliest event per job
    pub fn pending_job_outbox(&self) -> Result<Vec<RoutedJobEvent>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT e.event_json,j.origin_machine,j.machine,j.spec_digest FROM resource_jobs j
             JOIN resource_job_events e ON e.job_id=j.id
             LEFT JOIN resource_job_delivery d ON d.job_id=j.id
             WHERE e.seq=COALESCE(d.acknowledged_seq,0)+1 ORDER BY e.created_at",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(event, origin, authority, digest)| {
                Ok(RoutedJobEvent {
                    origin: origin.parse()?,
                    authority: authority.parse()?,
                    digest,
                    event: serde_json::from_str(&event)?,
                })
            })
            .collect()
    }

    /// Acknowledge only the next event, retaining all payloads for gap recovery
    pub fn acknowledge_job_event(&self, job: JobId, seq: u64) -> Result<(), AppError> {
        let seq = sql_seq(seq)?;
        self.immediate(|| {
            let previous: i64 = self
                .conn
                .query_row(
                    "SELECT acknowledged_seq FROM resource_job_delivery WHERE job_id=?1",
                    [job.to_string()],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or(0);
            if seq <= previous {
                return Ok(());
            }
            if seq != previous + 1
                || !self.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM resource_job_events WHERE job_id=?1 AND seq=?2)",
                    params![job.to_string(), seq],
                    |row| row.get::<_, bool>(0),
                )?
            {
                return Err(QueueError::Invariant {
                    message: "job acknowledgement has a sequence gap".into(),
                }
                .into());
            }
            self.conn.execute(
                "INSERT INTO resource_job_delivery(job_id,acknowledged_seq) VALUES (?1,?2)
                ON CONFLICT(job_id) DO UPDATE SET acknowledged_seq=excluded.acknowledged_seq",
                params![job.to_string(), seq],
            )?;
            Ok(())
        })
    }

    /// Accept the next event exactly once; old sequences must match stored content
    pub fn accept_job_event(&self, envelope: &RoutedJobEvent) -> Result<EventAcceptance, AppError> {
        self.immediate(|| {
            let event = &envelope.event;
            let mut route = self
                .job_route(event.job)?
                .ok_or(QueueError::JobNotFound { job: event.job })?;
            let accepted = self
                .job_route_cursors(event.job)?
                .ok_or(QueueError::JobNotFound { job: event.job })?
                .accepted;
            if route.origin != envelope.origin
                || route.authority != envelope.authority
                || route.digest != envelope.digest
                || matches!(
                    route.submission,
                    JobSubmission::Rejected { .. } | JobSubmission::Ended { .. }
                )
            {
                return Err(QueueError::JobConflict { job: event.job }.into());
            }
            if event.seq == 0 {
                return Err(AppError::Usage {
                    message: "job sequence must be positive".into(),
                });
            }
            // the authority numbers the current run's events from 1, and the
            // origin stores them after its earlier runs' events; answers stay
            // in the authority's numbering
            let base = route.seq_base();
            let mut stored = envelope.clone();
            stored.event.seq = event.seq.checked_add(base).ok_or_else(|| AppError::Usage {
                message: "job event sequence is too large".into(),
            })?;
            let seq = sql_seq(stored.event.seq)?;
            if stored.event.seq <= accepted {
                let json: String = self.conn.query_row(
                    "SELECT event_json FROM resource_job_inbox WHERE job_id=?1 AND seq=?2",
                    params![event.job.to_string(), seq],
                    |row| row.get(0),
                )?;
                let saved: RoutedJobEvent = serde_json::from_str(&json)?;
                if saved != stored {
                    return Err(QueueError::JobConflict { job: event.job }.into());
                }
                return Ok(EventAcceptance::Acknowledged { seq: event.seq });
            }
            if stored.event.seq != accepted + 1 {
                return Ok(EventAcceptance::Expected {
                    seq: accepted + 1 - base,
                });
            }
            route.submission = JobSubmission::Accepted;
            self.conn.execute(
                "INSERT INTO resource_job_inbox(job_id,seq,event_json) VALUES (?1,?2,?3)",
                params![event.job.to_string(), seq, serde_json::to_string(&stored)?],
            )?;
            self.conn.execute(
                "UPDATE resource_job_routes SET accepted_seq=?2,route_json=?3 WHERE job_id=?1",
                params![event.job.to_string(), seq, serde_json::to_string(&route)?],
            )?;
            Ok(EventAcceptance::Acknowledged { seq: event.seq })
        })
    }

    /// Earliest undelivered callback per origin job, in sequence order
    pub fn pending_job_inbox(&self) -> Result<Vec<(JobRoute, RoutedJobEvent)>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT i.job_id,i.event_json FROM resource_job_inbox i
            JOIN resource_job_routes r ON r.job_id=i.job_id WHERE i.seq=r.settled_seq+1",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(job, json)| {
                let job = job.parse()?;
                let route = self
                    .job_route(job)?
                    .ok_or(QueueError::JobNotFound { job })?;
                Ok((route, serde_json::from_str(&json)?))
            })
            .collect()
    }

    /// Events of one job's origin inbox that were delivered or suppressed, in sequence order
    ///
    /// A `homebased wait` stream replays these to catch up without printing
    /// them again; the events after them are still owed to the thread
    pub fn settled_job_events(&self, job: JobId) -> Result<Vec<JobEvent>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT i.event_json FROM resource_job_inbox i
            JOIN resource_job_routes r ON r.job_id=i.job_id
            WHERE i.job_id=?1 AND i.seq<=r.settled_seq ORDER BY i.seq",
        )?;
        let rows = statement
            .query_map([job.to_string()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|json| Ok(serde_json::from_str::<RoutedJobEvent>(&json)?.event))
            .collect()
    }

    /// Settle a callback only after delivery; a crash before this commit may repeat it
    ///
    /// A final event also ends a sleep's hold on the job
    pub fn settle_job_event(&self, job: JobId, seq: u64) -> Result<(), AppError> {
        let sql_seq = sql_seq(seq)?;
        self.immediate(|| {
            let event = self.job_inbox_event(job, sql_seq)?;
            if EventClass::job(event.event, event.gpu_idle.as_ref()).is_final() {
                end_hold(&self.conn, HoldTarget::Job(job))?;
            }
            self.settle_job_event_inner(job, seq, sql_seq)
        })
    }

    /// Settle job event `seq` into a sleep's hold on the job without waking
    /// its thread
    ///
    /// Only an event the hold keeps can be held: a progress event, or the
    /// final event of a job a held chain parked on, which also ends the hold
    pub fn hold_job_event(&self, job: JobId, seq: u64) -> Result<(), AppError> {
        let sql_seq = sql_seq(seq)?;
        self.immediate(|| {
            let event = self.job_inbox_event(job, sql_seq)?;
            let class = EventClass::job(event.event, event.gpu_idle.as_ref());
            let hold = read_hold(&self.conn, HoldTarget::Job(job))?
                .filter(|hold| hold.holds(class))
                .ok_or_else(|| QueueError::Invariant {
                    message: format!("job {job} event {seq} is not an event a hold keeps"),
                })?;
            record_held(&self.conn, &hold, &HeldEvent::job(&event))?;
            if class.is_final() {
                end_hold(&self.conn, hold.target)?;
            }
            self.settle_job_event_inner(job, seq, sql_seq)
        })
    }

    fn job_inbox_event(&self, job: JobId, sql_seq: i64) -> Result<JobEvent, AppError> {
        let json: String = self.conn.query_row(
            "SELECT event_json FROM resource_job_inbox WHERE job_id = ?1 AND seq = ?2",
            params![job.to_string(), sql_seq],
            |row| row.get(0),
        )?;
        Ok(serde_json::from_str::<RoutedJobEvent>(&json)?.event)
    }

    /// Whether one job's origin inbox already holds an event after `seq` that
    /// ends the run, which makes a notice at `seq` obsolete without asking
    /// the authority
    pub fn job_notice_overtaken(&self, job: JobId, seq: u64) -> Result<bool, AppError> {
        let mut statement = self
            .conn
            .prepare("SELECT event_json FROM resource_job_inbox WHERE job_id=?1 AND seq>?2")?;
        let rows = statement
            .query_map(params![job.to_string(), sql_seq(seq)?], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for json in rows {
            let envelope: RoutedJobEvent = serde_json::from_str(&json)?;
            if envelope.event.event.ends_run() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Retain an obsolete blocked or GPU idle notice and settle it without a callback
    pub fn suppress_job_notice(&self, job: JobId, seq: u64) -> Result<(), AppError> {
        let sql_seq = sql_seq(seq)?;
        self.immediate(|| {
            let json: String = self.conn.query_row(
                "SELECT event_json FROM resource_job_inbox WHERE job_id = ?1 AND seq = ?2",
                params![job.to_string(), sql_seq],
                |row| row.get(0),
            )?;
            let envelope: RoutedJobEvent = serde_json::from_str(&json)?;
            if !envelope.event.event.is_suppressible_notice() {
                return Err(QueueError::Invariant {
                    message: "only a blocked or GPU idle notice can be suppressed".into(),
                }
                .into());
            }
            self.conn.execute(
                "UPDATE resource_job_inbox SET suppressed_at = COALESCE(suppressed_at, ?3)
                 WHERE job_id = ?1 AND seq = ?2",
                params![job.to_string(), sql_seq, fmt_time(Utc::now())],
            )?;
            self.settle_job_event_inner(job, seq, sql_seq)
        })
    }

    fn settle_job_event_inner(&self, job: JobId, seq: u64, sql_seq: i64) -> Result<(), AppError> {
        let changed = self.conn.execute(
            "UPDATE resource_job_routes SET settled_seq=?2
                WHERE job_id=?1 AND settled_seq=?2-1 AND accepted_seq>=?2",
            params![job.to_string(), sql_seq],
        )?;
        if changed == 0
            && self
                .job_route_cursors(job)?
                .is_none_or(|cursors| cursors.settled < seq)
        {
            return Err(QueueError::Invariant {
                message: "job settlement has a sequence gap".into(),
            }
            .into());
        }
        Ok(())
    }
}

/// An event the origin writes itself for its own route, with no run
fn origin_event(job: JobId, seq: u64, event: JobEventKind) -> JobEvent {
    JobEvent {
        job,
        seq,
        event,
        run: None,
        resource_name: None,
        resource_label: None,
        process: None,
        attention: None,
        at: Utc::now(),
        blocked: None,
        held_ending: None,
        gpu_idle: None,
        gpu_summaries: Vec::new(),
        lease_idle: None,
        timeout: None,
        machine_released: None,
        machine_lost: None,
    }
}

/// Which of a machine's routes a lookup returns
#[derive(Clone, Copy)]
enum OnMachine {
    /// Jobs sent to the machine's queue
    Sent,
    /// Jobs sent to it, and jobs held pinned to it
    SentOrHeld,
}

/// What `policy` decides for a job on `route` whose machine was lost for `reason`
fn decide_rerun(policy: &RerunPolicy, reason: &ReleaseReason, route: &JobRoute) -> Rerun {
    // only CI jobs reach a pool machine; anything else is ended as before
    let Some(ci) = &route.spec.ci else {
        return Rerun::End(None);
    };
    let decision = policy.decide(
        reason,
        route.attempt(),
        ci.kill_after.get(),
        route.spec.machine.is_some(),
    );
    // the authority may lose the machine before it ends the cancelled run
    if route.cancel_requested && matches!(decision, Rerun::Queue { .. }) {
        return Rerun::End(Some(NotRerun::Cancelled));
    }
    decision
}

pub(super) fn sql_seq(seq: u64) -> Result<i64, AppError> {
    i64::try_from(seq).map_err(|_| AppError::Usage {
        message: "job event sequence is too large".into(),
    })
}

fn read_seq(seq: i64) -> Result<u64, AppError> {
    u64::try_from(seq).map_err(|_| {
        QueueError::Corrupt {
            message: "negative job event sequence".into(),
        }
        .into()
    })
}

#[cfg(test)]
mod tests;
