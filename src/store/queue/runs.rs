//! Queue runs: reservation, launch, stop, cleanup, and the commit that ends a run task

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, params};

use super::rows::{RESOURCE_SELECT, RawResource};
use super::{JobRecord, corrupt, event_run, failed_run, parse_uuid, stale, stop_cause};
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskExitEvidence, TaskId, TaskRow,
    ThreadId, check_status_transition,
};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::classify::{JobTransition, RunEnd, RunOutcome, classify};
use crate::queue::schedule::{NoticeThresholds, Preempt, decide};
use crate::queue::{
    ActiveRun, AttentionId, CleanupFailure, JobEventKind, JobId, JobState, QueueError, ResourceId,
    RunNumber, RunPhase, StepIndex, StepWorkload, StopCause,
};
use crate::store::task::insert_accepted_task_on;
use crate::store::{NewTask, Store, fmt_time, new_queued_task};

/// Which code path ends a run task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunEndSource {
    /// The worker, the task layer, or a cancel ended it: apply the table
    Task,
    /// The actor gave up on a launch its worker never confirmed; the job
    /// returns to its slot without counting a failure
    LaunchAbandoned,
}

/// Where a run's end leaves its job, and the event that records it
struct JobAfterRun {
    state: JobState,
    /// The step a queued job runs next, or the step a terminal job ended on
    step: StepIndex,
    event: Option<JobEventKind>,
}

impl Store {
    /// Reserve a run of `job` on `resource`: insert the run task, set the job
    /// `Active`, and set the resource's run `Launching`, in one transaction
    ///
    /// The run executes the job's next step with a fresh task, so a terminal
    /// task is never reused. `binary` is the resolved executable of the step
    pub fn reserve_run(
        &self,
        machine: MachineId,
        job: JobId,
        resource: ResourceId,
        task: TaskId,
        binary: PathBuf,
        now: DateTime<Utc>,
    ) -> Result<ActiveRun, AppError> {
        self.immediate(|| self.reserve_run_inner(machine, job, resource, task, binary, now))
    }

    fn reserve_run_inner(
        &self,
        machine: MachineId,
        job: JobId,
        resource: ResourceId,
        task: TaskId,
        binary: PathBuf,
        now: DateTime<Utc>,
    ) -> Result<ActiveRun, AppError> {
        let record = self.require_machine_job(machine, job)?;
        let JobState::Queued { resume } = record.state else {
            return Err(QueueError::Invariant {
                message: format!("job {job} is {}, not queued", record.state.as_str()),
            }
            .into());
        };
        let holder = self.require_resource(resource)?;
        if holder.machine != machine {
            return Err(QueueError::ResourceNotFound {
                resource: resource.to_string(),
            }
            .into());
        }
        if let Some(run) = &holder.run {
            return Err(QueueError::Invariant {
                message: format!(
                    "resource {} already has an active run of job {} in {}",
                    holder.resource.name,
                    run.job,
                    run.phase.as_str()
                ),
            }
            .into());
        }
        if !record.target.allows(resource) {
            return Err(QueueError::Invariant {
                message: format!("job {job} is pinned to another resource"),
            }
            .into());
        }
        if let Some(other) = self.run_of_job(job)? {
            return Err(QueueError::Invariant {
                message: format!(
                    "job {job} already has an active run on resource {} in {}",
                    other.resource,
                    other.phase.as_str()
                ),
            }
            .into());
        }
        let next_step = record.step;
        let step = record
            .spec
            .steps
            .get(next_step)
            .ok_or_else(|| QueueError::Corrupt {
                message: format!("job {job} has no step {next_step}"),
            })?;
        let run_number =
            RunNumber::new(
                record
                    .runs
                    .checked_add(1)
                    .ok_or_else(|| QueueError::Invariant {
                        message: format!("job {job} reached the run count limit"),
                    })?,
            )?;

        let row = new_queued_task(NewTask {
            id: task,
            name: record.spec.name.clone(),
            thread: record.spec.thread,
            workload: step.to_workload(),
            cwd: record.spec.cwd.clone(),
            timeout: record.spec.timeout,
            env: self.job_env(job)?,
            binary,
        });
        insert_accepted_task_on(&self.conn, &row)?;
        self.conn.execute(
            "UPDATE tasks SET resource_job_id = ?1, run_number = ?2, step_index = ?3
                 WHERE id = ?4",
            params![
                job.to_string(),
                run_number.get(),
                next_step.get(),
                task.to_string()
            ],
        )?;
        self.conn.execute(
            "UPDATE resource_jobs SET state = ?1, active_resource = ?2, resume = NULL,
                    last_run_number = ?3, updated_at = ?4
                 WHERE id = ?5",
            params![
                JobState::Active { resource }.as_str(),
                resource.to_string(),
                run_number.get(),
                fmt_time(now),
                job.to_string()
            ],
        )?;
        let run = ActiveRun {
            resource,
            job,
            task,
            run_number,
            step: next_step,
            resume,
            phase: RunPhase::Launching { reserved_at: now },
        };
        self.conn.execute(
            "INSERT INTO resource_run_history(task_id,resource_id) VALUES (?1,?2)",
            params![task.to_string(), resource.to_string()],
        )?;
        self.write_run(&run)
    }

    /// Fail a scheduled step whose executable disappeared after acceptance
    ///
    /// This creates an already-terminal attempt and applies its job result in
    /// one transaction. Its diagnostic binary is never passed to a worker
    pub fn fail_job_launch(
        &self,
        machine: MachineId,
        job: JobId,
        resource: ResourceId,
        task: TaskId,
        message: String,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        self.immediate(|| {
            let record = self.require_machine_job(machine, job)?;
            let step = record
                .spec
                .steps
                .get(record.step)
                .ok_or_else(|| corrupt("job next step missing"))?;
            let program = match step {
                StepWorkload::Task(workload) => workload.command.program().to_owned(),
                StepWorkload::Container(_) => "docker".into(),
            };
            self.reserve_run_inner(machine, job, resource, task, PathBuf::from(program), now)?;
            self.commit_terminal(
                task,
                ProcessStatus::Queued,
                &ExitReason::SpawnFailed { message },
                &ProcessGroupExitEvidence::NoChildSpawned.into(),
                None,
                RunEndSource::Task,
            )?;
            Ok(())
        })
    }

    /// The worker confirmed the run's child started
    ///
    /// A run cancelled before this confirmation stays `Stopping` and gains its
    /// start time. A repeated confirmation keeps the first start time
    pub fn mark_run_executing(
        &self,
        resource: ResourceId,
        task: TaskId,
        started_at: DateTime<Utc>,
    ) -> Result<ActiveRun, AppError> {
        self.immediate(|| {
            let mut run = self.require_run(resource, task)?;
            run.phase = match run.phase {
                RunPhase::Launching { .. } => RunPhase::Executing { started_at },
                RunPhase::Stopping {
                    started_at: None,
                    cause,
                    requested_at,
                } => RunPhase::Stopping {
                    started_at: Some(started_at),
                    cause,
                    requested_at,
                },
                phase @ (RunPhase::Executing { .. } | RunPhase::Stopping { .. }) => phase,
                RunPhase::Cleaning { .. } | RunPhase::Attention { .. } => {
                    return Err(stale(resource, "the run already ended"));
                }
            };
            self.write_run(&run)
        })
    }

    /// Commit a stop for the exact run task before the caller signals it
    ///
    /// For `Yield` the caller writes the run's yield file after this commit;
    /// for `Restart` and `UserCancel` it sets the cancellation marker. Later
    /// causes only upgrade an earlier one. A preemption stops only an
    /// executing run; a person may cancel one that is still launching
    pub fn commit_stop(
        &self,
        resource: ResourceId,
        task: TaskId,
        cause: StopCause,
        now: DateTime<Utc>,
    ) -> Result<ActiveRun, AppError> {
        self.immediate(|| self.request_stop(resource, Some(task), cause, now))
    }

    /// Rerun scheduling and commit only the same candidate in one transaction
    ///
    /// Returns no run when queue changes invalidated the decision. An already
    /// committed stop stays valid and returns its stored cause
    pub fn commit_preemption(
        &self,
        stop: Preempt,
        now: DateTime<Utc>,
    ) -> Result<Option<ActiveRun>, AppError> {
        self.immediate(|| {
            let record = self.require_resource(stop.resource)?;
            let Some(run) = record.run.filter(|run| run.task == stop.task) else {
                return Ok(None);
            };
            // a committed stop remains valid even when the queue later changes
            if matches!(run.phase, RunPhase::Stopping { .. }) {
                return Ok(Some(run));
            }
            let snapshot = self.queue_snapshot(record.machine, now, NoticeThresholds::default())?;
            if decide(&snapshot).preemption != Some(stop) {
                return Ok(None);
            }
            self.request_stop(stop.resource, Some(stop.task), stop.cause, now)
                .map(Some)
        })
    }

    pub(in crate::store) fn request_stop(
        &self,
        resource: ResourceId,
        task: Option<TaskId>,
        cause: StopCause,
        now: DateTime<Utc>,
    ) -> Result<ActiveRun, AppError> {
        let mut run = match task {
            Some(task) => self.require_run(resource, task)?,
            None => self
                .require_resource(resource)?
                .run
                .ok_or_else(|| stale(resource, "no active run"))?,
        };
        run.phase = match run.phase {
            RunPhase::Launching { .. } if cause == StopCause::UserCancel => RunPhase::Stopping {
                started_at: None,
                cause,
                requested_at: now,
            },
            RunPhase::Launching { .. } => {
                return Err(stale(resource, "a preemption stops only an executing run"));
            }
            RunPhase::Executing { started_at } => RunPhase::Stopping {
                started_at: Some(started_at),
                cause,
                requested_at: now,
            },
            RunPhase::Stopping {
                started_at,
                cause: earlier,
                requested_at,
            } => RunPhase::Stopping {
                started_at,
                cause: earlier.upgrade(cause),
                requested_at,
            },
            RunPhase::Cleaning { .. } | RunPhase::Attention { .. } => {
                return Err(stale(resource, "the run already ended"));
            }
        };
        self.conn.execute(
            "UPDATE resource_run_history SET stop_cause=?2 WHERE task_id=?1",
            params![
                run.task.to_string(),
                serde_json::to_string(&stop_cause(&run.phase))?
            ],
        )?;
        self.write_run(&run)
    }

    /// Start another cleanup attempt after a crash during cleanup
    ///
    /// Returns the attempt number the next result must name
    pub fn begin_cleanup_attempt(
        &self,
        resource: ResourceId,
        task: TaskId,
    ) -> Result<u32, AppError> {
        self.immediate(|| {
            let mut run = self.require_run(resource, task)?;
            let RunPhase::Cleaning { attempt } = run.phase else {
                return Err(stale(resource, "the run is not cleaning"));
            };
            let next = attempt
                .checked_add(1)
                .ok_or_else(|| QueueError::Invariant {
                    message: format!("task {task} reached the cleanup attempt limit"),
                })?;
            run.phase = RunPhase::Cleaning { attempt: next };
            self.write_run(&run)?;
            Ok(next)
        })
    }

    /// Apply a cleanup result to the stored run and attempt
    ///
    /// Success returns the resource to the queue. A failure puts it in
    /// `Attention` with a fresh id and records `JOB_ATTENTION` for the run's
    /// job; only a person's release leaves that phase
    pub fn apply_cleanup_result(
        &self,
        resource: ResourceId,
        task: TaskId,
        attempt: u32,
        result: Result<(), CleanupFailure>,
    ) -> Result<Option<RunPhase>, AppError> {
        self.immediate(|| {
            let mut run = self.require_run(resource, task)?;
            if run.phase != (RunPhase::Cleaning { attempt }) {
                return Err(stale(
                    resource,
                    &format!(
                        "cleanup attempt {attempt} does not match the run's {}",
                        run.phase.as_str()
                    ),
                ));
            }
            self.conn.execute(
                "UPDATE resource_run_history SET cleanup_json=?2 WHERE task_id=?1",
                params![task.to_string(), serde_json::to_string(&result)?],
            )?;
            let Err(failure) = result else {
                self.clear_run(resource)?;
                return Ok(None);
            };
            let id = AttentionId::new();
            run.phase = RunPhase::Attention { id, failure };
            let run = self.write_run(&run)?;
            self.append_job_event(
                run.job,
                JobEventKind::JobAttention,
                Some(event_run(&run)),
                None,
                Some(id),
            )?;
            Ok(Some(run.phase))
        })
    }

    /// Give up a launch whose worker never confirmed it started
    ///
    /// The task fails before launch, and the job returns to `Queued` in its
    /// slot for the same step without counting a failure; cleanup then runs.
    /// Returns `false` when the worker already moved the task on, in which
    /// case nothing changes
    pub fn abandon_launch(&self, resource: ResourceId, task: TaskId) -> Result<bool, AppError> {
        self.immediate(|| {
            let run = self.require_run(resource, task)?;
            if !matches!(run.phase, RunPhase::Launching { .. }) {
                return Err(stale(resource, "the run is not launching"));
            }
            let reason = ExitReason::SpawnFailed {
                message: "the worker did not confirm the launch in time".into(),
            };
            let evidence = TaskExitEvidence::from(ProcessGroupExitEvidence::NoChildSpawned);
            let row = self.commit_terminal(
                task,
                ProcessStatus::Queued,
                &reason,
                &evidence,
                None,
                RunEndSource::LaunchAbandoned,
            )?;
            Ok(row.is_some())
        })
    }

    /// The run task's job link, if it is a run
    pub fn job_run_link(&self, task: TaskId) -> Result<Option<JobId>, AppError> {
        let job: Option<Option<String>> = self
            .conn
            .query_row(
                "SELECT resource_job_id FROM tasks WHERE id = ?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        job.flatten()
            .map(|id| parse_uuid(&id).map(JobId::from_uuid))
            .transpose()
            .map_err(AppError::from)
    }

    /// Commit a run task's terminal state, classify the run's end, and move
    /// its job, all in the caller's transaction
    ///
    /// The task stores the classified outcome, not the raw one: a yield is
    /// `preempted`, and a run a person cancelled is `cancelled` with the
    /// `cancelled` reason even when its process exited on its own; the real
    /// exit stays in `exit.json` and in the job event. No `TASK_*` event is
    /// produced, since a run ending is not a job ending
    pub(in crate::store) fn commit_job_run_exit(
        &self,
        task: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
        evidence: &TaskExitEvidence,
        worker_thread: Option<ThreadId>,
    ) -> Result<Option<TaskRow>, AppError> {
        self.commit_terminal(
            task,
            from,
            reason,
            evidence,
            worker_thread,
            RunEndSource::Task,
        )
    }

    fn commit_terminal(
        &self,
        task: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
        evidence: &TaskExitEvidence,
        worker_thread: Option<ThreadId>,
        source: RunEndSource,
    ) -> Result<Option<TaskRow>, AppError> {
        let current = self.require_task(task)?;
        if current.status() != from {
            return Ok(None);
        }
        let (run, job) = self.run_for_task(task)?;
        let raw = ProcessStatus::from(reason);
        let (outcome, transition) = match source {
            RunEndSource::LaunchAbandoned => (RunOutcome::Failed, None),
            RunEndSource::Task => {
                let end = RunEnd::from_terminal(raw, Some(reason)).ok_or_else(|| {
                    QueueError::Invariant {
                        message: format!("{raw} is not a terminal status"),
                    }
                })?;
                let class = classify(
                    end,
                    stop_cause(&run.phase),
                    job.spec.steps.is_last(run.step),
                );
                (class.outcome, Some(class.job))
            }
        };
        let to = outcome.status();
        check_status_transition(from, to)?;
        let stored_reason = match outcome {
            RunOutcome::Cancelled => ExitReason::Cancelled,
            _ => reason.clone(),
        };
        let updated = self.conn.execute(
            "UPDATE tasks SET status = ?1, exit_reason = ?2,
                process_group_exit_evidence = ?3, container_exit_evidence = ?4,
                updated_at = ?5, worker_thread = COALESCE(?6, worker_thread)
             WHERE id = ?7 AND status = ?8",
            params![
                to.as_str(),
                serde_json::to_string(&stored_reason)?,
                evidence.process_group.as_str(),
                evidence.container.to_storage()?,
                fmt_time(Utc::now()),
                worker_thread.map(|thread| thread.to_string()),
                task.to_string(),
                from.as_str()
            ],
        )?;
        if updated != 1 {
            return Ok(None);
        }
        self.finish_run(&run, &job, transition, Some(reason.clone()))?;
        Ok(Some(self.require_task(task)?))
    }

    /// Commit a lost run task and move its job, in the caller's transaction
    pub(in crate::store) fn commit_lost(
        &self,
        task: TaskId,
        from: ProcessStatus,
        worker_thread: Option<ThreadId>,
    ) -> Result<Option<TaskRow>, AppError> {
        check_status_transition(from, ProcessStatus::Lost)?;
        let (run, job) = self.run_for_task(task)?;
        let updated = self.conn.execute(
            "UPDATE tasks SET status = 'lost', updated_at = ?1,
                worker_thread = COALESCE(?2, worker_thread)
             WHERE id = ?3 AND status = ?4",
            params![
                fmt_time(Utc::now()),
                worker_thread.map(|thread| thread.to_string()),
                task.to_string(),
                from.as_str()
            ],
        )?;
        if updated != 1 {
            return Ok(None);
        }
        let class = classify(
            RunEnd::Lost,
            stop_cause(&run.phase),
            job.spec.steps.is_last(run.step),
        );
        self.finish_run(&run, &job, Some(class.job), None)?;
        Ok(Some(self.require_task(task)?))
    }

    /// The executing run of a run task and its job
    fn run_for_task(&self, task: TaskId) -> Result<(ActiveRun, JobRecord), AppError> {
        let resource: Option<String> = self
            .conn
            .query_row(
                "SELECT id FROM resources WHERE run_task = ?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let resource = resource
            .ok_or_else(|| QueueError::Invariant {
                message: format!("run task {task} has no active run"),
            })
            .and_then(|id| parse_uuid(&id).map(ResourceId::from_uuid))?;
        let run = self.require_run(resource, task)?;
        if !run.phase.holds_job() {
            return Err(stale(resource, "the run already ended"));
        }
        let job = self.require_job(run.job)?;
        Ok((run, job))
    }

    /// Apply the job transition of a run's end and set the run `Cleaning`
    ///
    /// `transition` is `None` for an abandoned launch: the job returns to its
    /// slot for the same step, keeping its resume flag
    fn finish_run(
        &self,
        run: &ActiveRun,
        job: &JobRecord,
        transition: Option<JobTransition>,
        process: Option<ExitReason>,
    ) -> Result<(), AppError> {
        let after = match transition {
            None => JobAfterRun::queued(run.step, run.resume, None),
            Some(JobTransition::NextStep) => JobAfterRun::queued(run.step.next(), false, None),
            Some(JobTransition::Requeue { resume }) => {
                JobAfterRun::queued(run.step, resume, Some(JobEventKind::JobPreempted))
            }
            Some(JobTransition::Succeeded) => {
                JobAfterRun::ended(JobState::Succeeded, run.step, JobEventKind::JobSucceeded)
            }
            Some(JobTransition::Failed) => JobAfterRun::ended(
                JobState::Failed { run: run.task },
                run.step,
                JobEventKind::JobFailed,
            ),
            Some(JobTransition::Cancelled) => {
                JobAfterRun::ended(JobState::Cancelled, run.step, JobEventKind::JobCancelled)
            }
        };
        let resume = match after.state {
            JobState::Queued { resume } => Some(resume),
            _ => None,
        };
        let terminal = after.state.is_terminal();
        self.conn.execute(
            "UPDATE resource_jobs SET state = ?1, active_resource = NULL, step = ?2,
                resume = ?3, failed_run = ?4, position = CASE WHEN ?5 THEN NULL ELSE position END,
                updated_at = ?6
             WHERE id = ?7",
            params![
                after.state.as_str(),
                after.step.get(),
                resume,
                failed_run(after.state),
                terminal,
                fmt_time(Utc::now()),
                job.id.to_string(),
            ],
        )?;
        if terminal {
            self.renumber_level(job.machine, job.priority)?;
        }
        let cleaning = ActiveRun {
            phase: RunPhase::Cleaning { attempt: 1 },
            ..run.clone()
        };
        self.write_run(&cleaning)?;
        if let Some(kind) = after.event {
            self.append_job_event(job.id, kind, Some(event_run(run)), process, None)?;
        }
        Ok(())
    }

    /// The active run of `resource`, which must belong to `task`
    fn require_run(&self, resource: ResourceId, task: TaskId) -> Result<ActiveRun, AppError> {
        let run = self
            .require_resource(resource)?
            .run
            .ok_or_else(|| stale(resource, "no active run"))?;
        if run.task != task {
            return Err(stale(
                resource,
                &format!("the active run is task {}, not {task}", run.task),
            ));
        }
        Ok(run)
    }

    /// The active run of `job` on any resource, in any phase
    pub(in crate::store) fn run_of_job(&self, job: JobId) -> Result<Option<ActiveRun>, AppError> {
        self.conn
            .query_row(
                &format!("{RESOURCE_SELECT} WHERE run_job = ?1"),
                [job.to_string()],
                RawResource::read,
            )
            .optional()?
            .map(RawResource::parse)
            .transpose()
            .map(|record| record.and_then(|record| record.run))
    }

    /// Store `run` as its resource's active run and return it as stored, so
    /// callers see the same timestamps a later read does
    fn write_run(&self, run: &ActiveRun) -> Result<ActiveRun, AppError> {
        let mut columns = RunColumns::default();
        match &run.phase {
            RunPhase::Launching { reserved_at } => columns.reserved_at = Some(*reserved_at),
            RunPhase::Executing { started_at } => columns.started_at = Some(*started_at),
            RunPhase::Stopping {
                started_at,
                cause,
                requested_at,
            } => {
                columns.started_at = *started_at;
                columns.stop_cause = Some(*cause);
                columns.stop_requested_at = Some(*requested_at);
            }
            RunPhase::Cleaning { attempt } => columns.cleanup_attempt = Some(*attempt),
            RunPhase::Attention { id, failure } => {
                columns.attention = Some((*id, serde_json::to_string(failure)?));
            }
        }
        self.conn.execute(
            "UPDATE resources SET run_job = ?1, run_task = ?2, run_number = ?3, run_step = ?4,
                run_phase = ?5, run_reserved_at = ?6, run_started_at = ?7, run_stop_cause = ?8,
                run_stop_requested_at = ?9, run_cleanup_attempt = ?10, run_attention_id = ?11,
                run_attention_failure = ?12, run_resume = ?14
             WHERE id = ?13",
            params![
                run.job.to_string(),
                run.task.to_string(),
                run.run_number.get(),
                run.step.get(),
                run.phase.as_str(),
                columns.reserved_at.map(fmt_time),
                columns.started_at.map(fmt_time),
                columns.stop_cause.map(StopCause::as_str),
                columns.stop_requested_at.map(fmt_time),
                columns.cleanup_attempt,
                columns.attention.as_ref().map(|(id, _)| id.to_string()),
                columns.attention.map(|(_, failure)| failure),
                run.resource.to_string(),
                run.resume,
            ],
        )?;
        self.require_run(run.resource, run.task)
    }

    pub(super) fn clear_run(&self, resource: ResourceId) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE resources SET run_job = NULL, run_task = NULL, run_number = NULL,
                run_step = NULL, run_phase = NULL, run_reserved_at = NULL, run_started_at = NULL,
                run_stop_cause = NULL, run_stop_requested_at = NULL, run_cleanup_attempt = NULL,
                run_attention_id = NULL, run_attention_failure = NULL, run_resume = NULL
             WHERE id = ?1",
            [resource.to_string()],
        )?;
        Ok(())
    }
}

impl JobAfterRun {
    fn queued(step: StepIndex, resume: bool, event: Option<JobEventKind>) -> Self {
        Self {
            state: JobState::Queued { resume },
            step,
            event,
        }
    }

    fn ended(state: JobState, step: StepIndex, event: JobEventKind) -> Self {
        Self {
            state,
            step,
            event: Some(event),
        }
    }
}

/// Phase-specific run columns
#[derive(Default)]
struct RunColumns {
    reserved_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    stop_cause: Option<StopCause>,
    stop_requested_at: Option<DateTime<Utc>>,
    cleanup_attempt: Option<u32>,
    attention: Option<(AttentionId, String)>,
}
