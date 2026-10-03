//! Run launch context and atomic notice production

use chrono::{DateTime, Utc};
use rusqlite::params;
use std::collections::HashSet;

use super::{Store, event_run, fmt_time};
use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::home::{LockMode, flock_exclusive};
use crate::machine::MachineId;
use crate::queue::checkpoint::{Checkpoint, StepKind};
use crate::queue::schedule::{NoticeThresholds, StoredEpisode, decide};
use crate::queue::{BlockedNotice, JobEventKind, RunPhase};

impl Store {
    /// Read the checkpoint contract of the exact run while it holds its resource
    pub fn run_checkpoint(&self, task: TaskId) -> Result<Option<Checkpoint>, AppError> {
        let Some(job) = self.job_run_link(task)? else {
            return Ok(None);
        };
        let Some(run) = self.run_of_job(job)?.filter(|run| run.task == task) else {
            return Ok(None);
        };
        let job = self
            .job(job)?
            .ok_or_else(|| super::corrupt("run job missing"))?;
        let resource = self.require_resource(run.resource)?.resource;
        let step = job
            .spec
            .steps
            .get(run.step)
            .map(StepKind::from)
            .ok_or_else(|| super::corrupt("run step missing from its job"))?;
        Ok(Some(Checkpoint {
            run,
            resource,
            resume: job.resume,
            step,
        }))
    }

    /// Confirm a queue run only after its workload child or container started
    pub fn confirm_job_run_started(&self, task: TaskId) -> Result<(), AppError> {
        let Some(checkpoint) = self.run_checkpoint(task)? else {
            return Ok(());
        };
        self.mark_run_executing(checkpoint.run.resource, task, Utc::now())?;
        Ok(())
    }

    /// Append the exact open episode's notice and its recorded flag in one commit
    ///
    /// The blockers and due time are reread inside the transaction so a delayed
    /// timer cannot announce a head that can now start or a different episode
    pub fn produce_job_blocked(
        &self,
        machine: MachineId,
        episode: StoredEpisode,
        thresholds: NoticeThresholds,
        now: DateTime<Utc>,
    ) -> Result<bool, AppError> {
        self.immediate(|| {
            if self.blocked_episode(machine)? != Some(episode) || episode.notified {
                return Ok(false);
            }
            let decision = decide(&self.queue_snapshot(machine, now, thresholds)?);
            let Some(head) = decision
                .blocked
                .filter(|head| head.job == episode.job && head.send_now)
            else {
                return Ok(false);
            };
            self.append_job_event_with_notice(
                head.job,
                JobEventKind::JobBlocked,
                None,
                None,
                None,
                Some(BlockedNotice {
                    episode: episode.id,
                    blocked_since: episode.blocked_since,
                    blockers: head.blockers,
                }),
            )?;
            self.conn.execute(
                "UPDATE resource_blocked_notices SET notified_at = ?1
                 WHERE machine = ?2 AND job_id = ?3 AND ended_at IS NULL AND notified_at IS NULL",
                params![fmt_time(now), machine.to_string(), head.job.to_string()],
            )?;
            Ok(true)
        })
    }

    /// Publish the worker marker for a committed restart or user stop without changing its cause
    pub fn signal_committed_run_stop(
        &self,
        task: TaskId,
    ) -> Result<crate::store::CancelResult, AppError> {
        self.immediate(|| {
            let row = self.require_task(task)?;
            if row.state.is_terminal() {
                return Ok(crate::store::CancelResult::AlreadyTerminal(row));
            }
            let checkpoint = self
                .run_checkpoint(task)?
                .ok_or_else(|| super::corrupt("stop marker has no active run"))?;
            if !matches!(
                checkpoint.run.phase,
                RunPhase::Stopping {
                    cause: crate::queue::StopCause::Restart | crate::queue::StopCause::UserCancel,
                    ..
                }
            ) {
                return Err(super::corrupt(
                    "stop marker requires a committed restart or user cancel",
                )
                .into());
            }
            self.request_cancel_inner(task)
        })
    }

    /// Check an authority notice at delivery and retain suppression evidence if its episode ended
    pub fn job_notice_is_current(
        &self,
        job: crate::queue::JobId,
        seq: u64,
    ) -> Result<bool, AppError> {
        let seq = super::delivery::sql_seq(seq)?;
        self.immediate(|| {
            let record = self.job(job)?.ok_or(crate::queue::QueueError::JobNotFound { job })?;
            let (json, suppressed): (String, bool) = self.conn.query_row(
                "SELECT event_json, suppressed_at IS NOT NULL FROM resource_job_events WHERE job_id=?1 AND seq=?2",
                params![job.to_string(), seq], |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let event: crate::queue::JobEvent = serde_json::from_str(&json)?;
            if event.event != JobEventKind::JobBlocked { return Err(super::corrupt("notice eligibility requires JOB_BLOCKED").into()) }
            let notice = event.blocked.ok_or_else(|| super::corrupt("blocked event has no episode"))?;
            let open: bool = self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM resource_blocked_notices WHERE id=?1 AND job_id=?2 AND machine=?3 AND ended_at IS NULL)",
                params![notice.episode, job.to_string(), record.machine.to_string()], |row| row.get(0),
            )?;
            let current = !suppressed && open && decide(&self.queue_snapshot(record.machine, Utc::now(), NoticeThresholds::default())?)
                .blocked.is_some_and(|head| head.job == job);
            if !current {
                self.conn.execute("UPDATE resource_blocked_notices SET ended_at=COALESCE(ended_at,?2) WHERE id=?1",
                    params![notice.episode, fmt_time(Utc::now())])?;
                self.conn.execute("UPDATE resource_job_events SET suppressed_at=COALESCE(suppressed_at,?3) WHERE job_id=?1 AND seq=?2",
                    params![job.to_string(), seq, fmt_time(Utc::now())])?;
            }
            Ok(current)
        })
    }

    /// Record one inactivity reminder for the exact running queue task
    ///
    /// The task reminder state and the job event share a transaction with the
    /// running-state check, so a terminal commit wins against a late timer
    pub fn produce_job_check_due(&self, task: TaskId) -> Result<bool, AppError> {
        self.immediate(|| {
            let Some(checkpoint) = self.run_checkpoint(task)? else { return Ok(false) };
            if !matches!(checkpoint.run.phase, RunPhase::Executing { .. } | RunPhase::Stopping { .. })
                || self.require_task(task)?.status() != ProcessStatus::Running
            {
                return Ok(false);
            }
            let changed = self.conn.execute(
                "UPDATE tasks SET attention_state = 'delivered', timeout_notified_at = ?1, updated_at = ?1
                 WHERE id = ?2 AND status = 'running' AND attention_state = 'pending'",
                params![fmt_time(Utc::now()), task.to_string()],
            )?;
            if changed == 0 { return Ok(false) }
            self.append_job_event(checkpoint.run.job, JobEventKind::JobCheckDue,
                Some(event_run(&checkpoint.run)), None, None)?;
            Ok(true)
        })
    }

    /// Worker PIDs to protect during cleanup, including a worker finishing its commit
    ///
    /// A terminal worker can still hold its runner lock after committing. Old
    /// rows with free locks must not protect PIDs now reused by other processes
    pub fn cleanup_protected_pids(&self) -> Result<Vec<i32>, AppError> {
        let mut statement = self
            .conn
            .prepare("SELECT id, pid FROM tasks WHERE pid > 0")?;
        let workers = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut protected = HashSet::new();
        for (task, pid) in workers {
            let task = task
                .parse::<TaskId>()
                .map_err(|error| super::corrupt(format!("worker task ID: {error}")))?;
            let lock = self.tasks_dir.join(task.to_string()).join("runner.lock");
            if !lock.exists() {
                continue;
            }

            match flock_exclusive(&lock, LockMode::NonBlocking) {
                Ok(_) => {}
                Err(AppError::LockHeld { .. }) => {
                    protected.insert(pid);
                }
                Err(error) => {
                    // an unreadable lock cannot prove that its worker has left
                    tracing::warn!(%error, %task, "Cannot check worker lock for cleanup");
                    protected.insert(pid);
                }
            }
        }

        Ok(protected.into_iter().collect())
    }
}
