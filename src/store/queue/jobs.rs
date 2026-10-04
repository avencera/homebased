//! Queue jobs: submission, level slots, operator operations, and job events

use serde::Serialize;
use serde::de::DeserializeOwned;

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, params};

use super::rows::{JOB_SELECT, RawJob};
use super::{
    CancelResult, JobAccepted, JobRecord, MoveResult, NewJob, ReleaseResult, SERVING_ORDER,
    corrupt, failed_run, parse_uuid,
};
use crate::domain::{ExitReason, TaskEnv};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::{
    AttentionId, BlockedNotice, EventRun, JobEvent, JobEventKind, JobId, JobState, LevelEnd,
    MoveRefusal, OperationId, Placement, Priority, QueueError, ResourceId, Side, StopCause, Target,
    sha256_hex,
};
use crate::store::{Store, fmt_time};

/// Added to every position of a level before it is rewritten, so the unique
/// slot index never sees two jobs on one position mid-renumber
const RENUMBER_OFFSET: i64 = 1 << 40;

/// Where a move inserts the job within its new level
#[derive(Debug, Clone, Copy)]
enum Slot {
    /// An end of the level
    End(LevelEnd),
    /// Beside another job of the level
    Next { target: JobId, side: Side },
}

impl Store {
    /// Accept a job at the back of its level
    ///
    /// A retry with the same id and spec digest returns the job as it is now,
    /// without a second record. The same id with another spec is a conflict.
    /// Records are kept after the job ends, so a delayed submit cannot
    /// recreate cancelled work
    pub fn submit_job(&self, new: &NewJob) -> Result<JobAccepted, AppError> {
        let digest = new.spec.digest()?;
        self.immediate(|| {
            if let Some(existing) = self.job(new.id)? {
                if existing.digest != digest
                    || existing.machine != new.machine
                    || existing.origin != new.origin
                {
                    return Err(QueueError::JobConflict { job: new.id }.into());
                }
                return Ok(accepted(&existing));
            }
            let target = match &new.spec.resource {
                None => Target::Any,
                Some(selector) => {
                    Target::Pinned(self.resolve_resource(new.machine, selector)?.resource.id)
                }
            };
            let pinned = match target {
                Target::Any => None,
                Target::Pinned(resource) => Some(resource.to_string()),
            };
            let priority = new.spec.priority;
            let position = self.level(new.machine, priority)?.len() + 1;
            let now = fmt_time(Utc::now());
            self.conn.execute(
                "INSERT INTO resource_jobs (id, machine, origin_machine, thread_id, spec_json,
                    spec_digest, env_path, env_home, target_resource, priority, position, state,
                    step, resume, last_run_number, event_seq, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'queued', 0, 0, 0, 0,
                    ?12, ?12)",
                params![
                    new.id.to_string(),
                    new.machine.to_string(),
                    new.origin.to_string(),
                    new.spec.thread.to_string(),
                    new.spec.to_canonical_json()?,
                    digest,
                    new.env.path,
                    new.env.home,
                    pinned,
                    priority.rank(),
                    sql_position(position)?,
                    now,
                ],
            )?;
            Ok(accepted(&self.require_job(new.id)?))
        })
    }

    /// One job by id, on any machine
    pub fn job(&self, id: JobId) -> Result<Option<JobRecord>, AppError> {
        self.conn
            .query_row(
                &format!("{JOB_SELECT} WHERE id = ?1"),
                [id.to_string()],
                RawJob::read,
            )
            .optional()?
            .map(RawJob::parse)
            .transpose()
    }

    /// Execution environment of a job, captured locally or on the remote authority
    pub fn job_env(&self, id: JobId) -> Result<TaskEnv, AppError> {
        self.conn
            .query_row(
                "SELECT env_path, env_home FROM resource_jobs WHERE id = ?1",
                [id.to_string()],
                |row| {
                    Ok(TaskEnv {
                        path: row.get(0)?,
                        home: row.get(1)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| QueueError::JobNotFound { job: id }.into())
    }

    pub(super) fn require_job(&self, id: JobId) -> Result<JobRecord, AppError> {
        self.job(id)?
            .ok_or_else(|| QueueError::JobNotFound { job: id }.into())
    }

    /// A job of `machine`'s queue; a job of another queue is not found here
    pub(super) fn require_machine_job(
        &self,
        machine: MachineId,
        id: JobId,
    ) -> Result<JobRecord, AppError> {
        self.job(id)?
            .filter(|job| job.machine == machine)
            .ok_or_else(|| QueueError::JobNotFound { job: id }.into())
    }

    /// Every non-terminal job of `machine`'s queue, in serving order
    pub fn machine_queue(&self, machine: MachineId) -> Result<Vec<JobRecord>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "{JOB_SELECT} WHERE machine = ?1 AND position IS NOT NULL {SERVING_ORDER}"
        ))?;
        let raws = statement
            .query_map([machine.to_string()], RawJob::read)?
            .collect::<Result<Vec<_>, _>>()?;
        raws.into_iter().map(RawJob::parse).collect()
    }

    /// Ids of one level of `machine`'s queue, by position
    pub(super) fn level(
        &self,
        machine: MachineId,
        priority: Priority,
    ) -> Result<Vec<JobId>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT id FROM resource_jobs
             WHERE machine = ?1 AND priority = ?2 AND position IS NOT NULL
             ORDER BY position",
        )?;
        let ids = statement
            .query_map(params![machine.to_string(), priority.rank()], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        ids.iter()
            .map(|id| Ok(JobId::from_uuid(parse_uuid(id)?)))
            .collect()
    }

    /// Rewrite the positions of one level to `1..=n` in the given order
    ///
    /// Every listed job takes `priority`, so a job moving between levels is
    /// written here with its new level
    fn write_level(
        &self,
        machine: MachineId,
        priority: Priority,
        order: &[JobId],
    ) -> Result<(), AppError> {
        for (index, job) in order.iter().enumerate() {
            self.conn.execute(
                "UPDATE resource_jobs SET priority = ?1, position = ?2
                 WHERE id = ?3 AND machine = ?4 AND position IS NOT NULL",
                params![
                    priority.rank(),
                    sql_position(index + 1)?,
                    job.to_string(),
                    machine.to_string(),
                ],
            )?;
        }
        Ok(())
    }

    /// Move every slot of `levels` out of the way before rewriting them
    fn lift_levels(&self, machine: MachineId, levels: &[Priority]) -> Result<(), AppError> {
        for level in levels {
            self.conn.execute(
                "UPDATE resource_jobs SET position = position + ?1
                 WHERE machine = ?2 AND priority = ?3 AND position IS NOT NULL",
                params![RENUMBER_OFFSET, machine.to_string(), level.rank()],
            )?;
        }
        Ok(())
    }

    /// Close the gap a job left in its level
    pub(super) fn renumber_level(
        &self,
        machine: MachineId,
        priority: Priority,
    ) -> Result<(), AppError> {
        let order = self.level(machine, priority)?;
        self.lift_levels(machine, &[priority])?;
        self.write_level(machine, priority, &order)
    }

    /// Move a job to a new slot
    ///
    /// Any non-terminal job may move, including the active one, which changes
    /// the level preemption compares against. A move never changes the
    /// accepted spec. A replay of `operation` returns its stored result
    pub fn move_job(
        &self,
        machine: MachineId,
        operation: OperationId,
        job: JobId,
        placement: Placement,
    ) -> Result<MoveResult, AppError> {
        let content = serde_json::json!({ "job": job, "placement": placement });
        self.immediate(|| {
            self.with_operation(machine, operation, "move", &content, || {
                self.apply_move(machine, job, placement)
            })
        })
    }

    fn apply_move(
        &self,
        machine: MachineId,
        id: JobId,
        placement: Placement,
    ) -> Result<MoveResult, AppError> {
        let job = self.require_machine_job(machine, id)?;
        if job.state.is_terminal() {
            return Err(QueueError::JobTerminal {
                job: id,
                state: job.state.as_str(),
            }
            .into());
        }
        let refuse = |reason| QueueError::MoveRefused { job: id, reason };
        let (level, slot) = match placement {
            Placement::Edge { priority, end } => (priority.unwrap_or(job.priority), Slot::End(end)),
            Placement::Relative {
                target,
                side,
                expect,
            } => {
                if target == id {
                    return Err(refuse(MoveRefusal::SelfTarget).into());
                }
                let anchor = self
                    .job(target)?
                    .filter(|anchor| anchor.machine == machine)
                    .ok_or(refuse(MoveRefusal::TargetNotFound { target }))?;
                if anchor.state.is_terminal() {
                    return Err(refuse(MoveRefusal::TargetTerminal { target }).into());
                }
                if let Some(requested) = expect
                    && requested != anchor.priority
                {
                    return Err(refuse(MoveRefusal::LevelMismatch {
                        target,
                        requested,
                        actual: anchor.priority,
                    })
                    .into());
                }
                (anchor.priority, Slot::Next { target, side })
            }
        };

        let mut order: Vec<JobId> = self
            .level(machine, level)?
            .into_iter()
            .filter(|other| *other != id)
            .collect();
        let index = match slot {
            Slot::End(LevelEnd::Front) => 0,
            Slot::End(LevelEnd::Back) => order.len(),
            Slot::Next { target, side } => {
                let at = order
                    .iter()
                    .position(|other| *other == target)
                    .ok_or_else(|| QueueError::Invariant {
                        message: format!("target {target} has no slot in its level"),
                    })?;
                match side {
                    Side::Before => at,
                    Side::After => at + 1,
                }
            }
        };
        order.insert(index, id);

        let previous: Vec<JobId> = self
            .level(machine, job.priority)?
            .into_iter()
            .filter(|other| *other != id)
            .collect();
        self.lift_levels(machine, &[job.priority, level])?;
        if job.priority != level {
            self.write_level(machine, job.priority, &previous)?;
        }
        self.write_level(machine, level, &order)?;
        self.touch_job(id)?;
        Ok(MoveResult {
            job: id,
            priority: level,
            position: u32::try_from(index + 1).map_err(|_| QueueError::Invariant {
                message: "queue position exceeds u32".into(),
            })?,
        })
    }

    /// Cancel a job
    ///
    /// A queued job, including one requeued by preemption, becomes
    /// `Cancelled` at once. An active job records `Stopping { UserCancel }`,
    /// upgrading any earlier cause, and the caller sets the run task's
    /// cancellation marker. A job that already ended keeps its result
    pub fn cancel_job(
        &self,
        machine: MachineId,
        operation: OperationId,
        job: JobId,
        now: DateTime<Utc>,
    ) -> Result<CancelResult, AppError> {
        let content = serde_json::json!({ "job": job });
        self.immediate(|| {
            self.with_operation(machine, operation, "cancel", &content, || {
                self.apply_cancel(machine, job, now)
            })
        })
    }

    fn apply_cancel(
        &self,
        machine: MachineId,
        id: JobId,
        now: DateTime<Utc>,
    ) -> Result<CancelResult, AppError> {
        let job = self.require_machine_job(machine, id)?;
        match job.state {
            JobState::Succeeded | JobState::Failed { .. } | JobState::Cancelled => {
                Ok(CancelResult::AlreadyTerminal {
                    state: job.state.as_str().to_owned(),
                })
            }
            JobState::Queued { .. } => {
                self.end_job(&job, JobState::Cancelled)?;
                self.append_job_event(id, JobEventKind::JobCancelled, None, None, None)?;
                Ok(CancelResult::Cancelled)
            }
            JobState::Active { resource } => {
                let run = self.request_stop(resource, None, StopCause::UserCancel, now)?;
                Ok(CancelResult::Stopping {
                    resource,
                    task: run.task,
                })
            }
        }
    }

    /// Return a resource in `Attention` to the queue after a person checked
    /// the machine
    ///
    /// The release names the attention id, so a stale retry cannot clear a
    /// later run's `Attention`. A replay of `operation` returns its stored result
    pub fn release_resource_attention(
        &self,
        machine: MachineId,
        operation: OperationId,
        attention: AttentionId,
    ) -> Result<ReleaseResult, AppError> {
        let content = serde_json::json!({ "attention": attention });
        self.immediate(|| {
            self.with_operation(machine, operation, "release", &content, || {
                let resource: Option<String> = self
                    .conn
                    .query_row(
                        "SELECT id FROM resources WHERE machine = ?1 AND run_attention_id = ?2",
                        params![machine.to_string(), attention.to_string()],
                        |row| row.get(0),
                    )
                    .optional()?;
                let resource = resource
                    .ok_or(QueueError::AttentionNotFound { attention })
                    .and_then(|id| parse_uuid(&id).map(ResourceId::from_uuid))?;
                self.clear_run(resource)?;
                Ok(ReleaseResult {
                    resource,
                    attention,
                })
            })
        })
    }

    /// Run `apply` once per operation id
    ///
    /// The first call stores its result with the content's digest. A replay
    /// with the same content returns the stored result without reapplying,
    /// and the same id with other content is a conflict. A refused request
    /// stores nothing, so it can be retried after the queue changes
    pub(super) fn with_operation<R: Serialize + DeserializeOwned>(
        &self,
        machine: MachineId,
        operation: OperationId,
        kind: &str,
        content: &serde_json::Value,
        apply: impl FnOnce() -> Result<R, AppError>,
    ) -> Result<R, AppError> {
        let digest = sha256_hex(
            serde_json::to_string(&serde_json::json!({
                "kind": kind,
                "machine": machine,
                "content": content,
            }))?
            .as_bytes(),
        );
        let stored: Option<(String, String)> = self
            .conn
            .query_row(
                "SELECT content_digest, result_json FROM resource_operations WHERE id = ?1",
                [operation.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((stored_digest, result)) = stored {
            if stored_digest != digest {
                return Err(QueueError::OperationConflict { operation }.into());
            }
            return Ok(serde_json::from_str(&result)?);
        }
        let result = apply()?;
        self.conn.execute(
            "INSERT INTO resource_operations (id, machine, kind, content_digest, result_json,
                created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                operation.to_string(),
                machine.to_string(),
                kind,
                digest,
                serde_json::to_string(&result)?,
                fmt_time(Utc::now()),
            ],
        )?;
        Ok(result)
    }

    /// Take a job out of the queue with a terminal state
    fn end_job(&self, job: &JobRecord, state: JobState) -> Result<(), AppError> {
        if !state.is_terminal() {
            return Err(QueueError::Invariant {
                message: format!("job {} cannot end as {}", job.id, state.as_str()),
            }
            .into());
        }
        self.conn.execute(
            "UPDATE resource_jobs SET state = ?1, position = NULL, active_resource = NULL,
                resume = NULL, failed_run = ?2, updated_at = ?3
             WHERE id = ?4",
            params![
                state.as_str(),
                failed_run(state),
                fmt_time(Utc::now()),
                job.id.to_string()
            ],
        )?;
        self.renumber_level(job.machine, job.priority)
    }

    fn touch_job(&self, id: JobId) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE resource_jobs SET updated_at = ?1 WHERE id = ?2",
            params![fmt_time(Utc::now()), id.to_string()],
        )?;
        Ok(())
    }

    pub(super) fn append_job_event(
        &self,
        job: JobId,
        event: JobEventKind,
        run: Option<EventRun>,
        process: Option<ExitReason>,
        attention: Option<AttentionId>,
    ) -> Result<JobEvent, AppError> {
        self.append_job_event_with_notice(job, event, run, process, attention, None)
    }

    pub(super) fn append_job_event_with_notice(
        &self,
        job: JobId,
        event: JobEventKind,
        run: Option<EventRun>,
        process: Option<ExitReason>,
        attention: Option<AttentionId>,
        blocked: Option<BlockedNotice>,
    ) -> Result<JobEvent, AppError> {
        self.conn.execute(
            "UPDATE resource_jobs SET event_seq = event_seq + 1 WHERE id = ?1",
            [job.to_string()],
        )?;
        let seq: i64 = self.conn.query_row(
            "SELECT event_seq FROM resource_jobs WHERE id = ?1",
            [job.to_string()],
            |row| row.get(0),
        )?;
        let event = JobEvent {
            job,
            seq: u64::try_from(seq).map_err(|_| corrupt("negative event sequence"))?,
            event,
            run,
            process,
            attention,
            at: Utc::now(),
            blocked,
        };
        self.conn.execute(
            "INSERT INTO resource_job_events (job_id, seq, event_json, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                job.to_string(),
                seq,
                serde_json::to_string(&event)?,
                fmt_time(event.at)
            ],
        )?;
        Ok(event)
    }

    /// Every stored event of a job, by sequence
    pub fn job_events(&self, job: JobId) -> Result<Vec<JobEvent>, AppError> {
        let mut statement = self
            .conn
            .prepare("SELECT event_json FROM resource_job_events WHERE job_id = ?1 ORDER BY seq")?;
        let events = statement
            .query_map([job.to_string()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        events
            .iter()
            .map(|event| serde_json::from_str(event).map_err(AppError::from))
            .collect()
    }
}

fn accepted(job: &JobRecord) -> JobAccepted {
    JobAccepted {
        job_id: job.id,
        machine: job.machine,
        target: job.target,
        state: job.state.as_str().to_owned(),
        priority: job.priority,
        position: job.position,
    }
}

fn sql_position(position: usize) -> Result<i64, AppError> {
    i64::try_from(position).map_err(|_| {
        QueueError::Invariant {
            message: "queue position exceeds SQLite range".into(),
        }
        .into()
    })
}
