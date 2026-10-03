//! Run launch context and atomic notice production

use chrono::{DateTime, Utc};
use rusqlite::params;
use std::collections::HashSet;

use super::{Store, event_run, fmt_time};
use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::home::{LockMode, flock_exclusive};
use crate::machine::MachineId;
use crate::queue::checkpoint::Checkpoint;
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
        Ok(Some(Checkpoint {
            run,
            resource,
            resume: job.resume,
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

    /// Append the exact open episode's notice and its sent flag in one commit
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
