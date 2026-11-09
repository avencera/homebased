//! Durable job delivery; inbox acceptance and route cursors commit together

use chrono::Utc;
use rusqlite::{OptionalExtension, params};

use super::fmt_time;
use crate::error::AppError;
use crate::events::EventAcceptance;
use crate::queue::delivery::{HeldJobEnding, JobRefusal, JobRoute, JobSubmission, RoutedJobEvent};
use crate::queue::{JobEvent, JobEventKind, JobId, QueueError, Target};
use crate::store::Store;

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
                && (route.after.is_none() || !route.owes_final_event)
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

    /// End a held job before it joins the queue, committing its terminal
    /// event to the origin inbox in the same transaction
    ///
    /// A dependency ending applies only while the job is still held, since a
    /// sent job may already be queued; a refusal applies to the sent route
    /// that the authority refused. Anything else returns `false`
    pub fn end_held_job(&self, job: JobId, ending: HeldJobEnding) -> Result<bool, AppError> {
        self.immediate(|| self.end_held_job_inner(job, ending))
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
                _ => false,
            };
            let accepted = self
                .job_route_cursors(job)?
                .ok_or(QueueError::JobNotFound { job })?
                .accepted;
            if !applies || accepted != 0 {
                return Ok(false);
            }
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
                    job,
                    seq: 1,
                    event: kind,
                    run: None,
                    resource_name: None,
                    resource_label: None,
                    process: None,
                    attention: None,
                    at: Utc::now(),
                    blocked: None,
                    held_ending: Some(ending.clone()),
                    gpu_idle: None,
                },
            };
            route.submission = JobSubmission::Ended { ending };
            self.conn.execute(
                "INSERT INTO resource_job_inbox(job_id,seq,event_json) VALUES (?1,1,?2)",
                params![job.to_string(), serde_json::to_string(&envelope)?],
            )?;
            self.conn.execute(
                "UPDATE resource_job_routes SET accepted_seq=1,route_json=?2 WHERE job_id=?1",
                params![job.to_string(), serde_json::to_string(&route)?],
            )?;
            Ok(true)
        }
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
            let seq = sql_seq(event.seq)?;
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
            if event.seq <= accepted {
                let json: String = self.conn.query_row(
                    "SELECT event_json FROM resource_job_inbox WHERE job_id=?1 AND seq=?2",
                    params![event.job.to_string(), seq],
                    |row| row.get(0),
                )?;
                let saved: RoutedJobEvent = serde_json::from_str(&json)?;
                if saved != *envelope {
                    return Err(QueueError::JobConflict { job: event.job }.into());
                }
                return Ok(EventAcceptance::Acknowledged { seq: event.seq });
            }
            if event.seq != accepted + 1 {
                return Ok(EventAcceptance::Expected { seq: accepted + 1 });
            }
            route.submission = JobSubmission::Accepted;
            self.conn.execute(
                "INSERT INTO resource_job_inbox(job_id,seq,event_json) VALUES (?1,?2,?3)",
                params![event.job.to_string(), seq, serde_json::to_string(envelope)?],
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
    pub fn settle_job_event(&self, job: JobId, seq: u64) -> Result<(), AppError> {
        let sql_seq = sql_seq(seq)?;
        self.immediate(|| self.settle_job_event_inner(job, seq, sql_seq))
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
