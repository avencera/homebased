//! Store side of the GPU priority queue
//!
//! Every operation runs in one `BEGIN IMMEDIATE` transaction and keeps the
//! queue's invariants: one active run per resource and per job, the job of an
//! active run is `Active` exactly while the run still executes it, and every
//! non-terminal job holds a slot that is unique and dense within its level of
//! the machine queue. The terminal commit of a run task classifies the run's
//! end and moves its job in the same transaction

mod delivery;
pub mod interface;
mod jobs;
mod resources;
mod rows;
mod runs;
mod runtime;
#[cfg(test)]
mod tests;

pub use delivery::JobCursors;

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{TaskEnv, TaskId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::schedule::{
    JobView, NoticeThresholds, QueuedState, ResourceView, Snapshot, StoredEpisode,
};
use crate::queue::spec::JobSpec;
use crate::queue::{
    ActiveRun, AttentionId, EventRun, JobId, JobState, Priority, QueueError, Resource, ResourceId,
    RunPhase, StepIndex, StopCause, Target,
};
use crate::store::{Store, fmt_time, parse_time};

/// Serving order of a machine queue: level descending, then position
const SERVING_ORDER: &str = "ORDER BY priority DESC, position";

/// How a resource entered the queue; only detected fallbacks are reconciled
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceOrigin {
    /// The platform had no indexed GPU
    DetectedFallback,
    /// Detection selected a physical device
    DetectedDevice,
    /// A person registered this resource
    Manual,
}

impl ResourceOrigin {
    /// Storage tag
    const fn as_str(self) -> &'static str {
        match self {
            Self::DetectedFallback => "detected_fallback",
            Self::DetectedDevice => "detected_device",
            Self::Manual => "manual",
        }
    }

    fn parse(raw: &str) -> Result<Self, QueueError> {
        match raw {
            "detected_fallback" => Ok(Self::DetectedFallback),
            "detected_device" => Ok(Self::DetectedDevice),
            "manual" => Ok(Self::Manual),
            other => Err(corrupt(format!("unknown resource origin {other:?}"))),
        }
    }
}

/// A resource with its machine and single active run
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRecord {
    /// Machine whose queue the resource serves
    pub machine: MachineId,
    /// Registration owner, retained across detection retries
    pub origin: ResourceOrigin,
    /// The resource
    pub resource: Resource,
    /// Its active run; `None` means idle
    pub run: Option<ActiveRun>,
}

/// A stored job
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    /// Job identity
    pub id: JobId,
    /// Machine whose queue holds the job
    pub machine: MachineId,
    /// Machine that submitted it, where its events go
    pub origin: MachineId,
    /// The accepted spec, never changed by a move
    pub spec: JobSpec,
    /// Digest of the accepted spec
    pub digest: String,
    /// Resources the job may use
    pub target: Target,
    /// Current level, which a move may change
    pub priority: Priority,
    /// 1-based position within the level; `None` once terminal
    pub position: Option<u32>,
    /// Where the job is in its life
    pub state: JobState,
    /// Step the job is at: the step its next or current run executes, or the
    /// step it ended on
    pub step: StepIndex,
    /// Runs started so far
    pub runs: u32,
    /// Acceptance time
    pub created_at: DateTime<Utc>,
    /// Last change
    pub updated_at: DateTime<Utc>,
}

/// A job to accept into a machine queue
#[derive(Debug, Clone)]
pub struct NewJob {
    /// Identity chosen by the submitter
    pub id: JobId,
    /// Machine whose queue runs it, which is this authority
    pub machine: MachineId,
    /// Machine that submitted it
    pub origin: MachineId,
    /// Validated spec; its `resource` is resolved against the machine's resources
    pub spec: JobSpec,
    /// Submitter's environment for resolving executables
    pub env: TaskEnv,
}

/// Submit response: the job and its slot now
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct JobAccepted {
    /// Job identity
    pub job_id: JobId,
    /// Machine whose queue holds it
    pub machine: MachineId,
    /// Resources it may use
    pub target: Target,
    /// State name
    pub state: String,
    /// Current level
    pub priority: Priority,
    /// Current position; `None` once terminal
    pub position: Option<u32>,
}

/// Result of a move
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct MoveResult {
    /// Moved job
    pub job: JobId,
    /// Its level after the move
    pub priority: Priority,
    /// Its position after the move
    pub position: u32,
}

/// Result of a cancel
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum CancelResult {
    /// A queued job is now `Cancelled`
    Cancelled,
    /// The active run must stop: set this task's cancellation marker
    Stopping {
        /// Resource the run holds
        resource: ResourceId,
        /// Run task to cancel
        task: TaskId,
    },
    /// The job already ended; its result stands
    AlreadyTerminal {
        /// Terminal state name
        state: String,
    },
}

/// Result of releasing an `Attention`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ReleaseResult {
    /// Resource returned to the queue
    pub resource: ResourceId,
    /// Released attention
    pub attention: AttentionId,
}

impl Store {
    /// Record the blocked head of `machine`'s queue
    ///
    /// The same head keeps its open episode. A new head ends the open
    /// episode, which drops its unsent notice, and starts one at its
    /// `blocked_since`. `None` ends the open episode
    pub fn record_blocked_head(
        &self,
        machine: MachineId,
        head: Option<(JobId, DateTime<Utc>)>,
    ) -> Result<Option<StoredEpisode>, AppError> {
        self.immediate(|| {
            let open = self.blocked_episode(machine)?;
            if let (Some(open), Some((job, _))) = (open, head)
                && open.job == job
            {
                return Ok(Some(open));
            }
            self.conn.execute(
                "UPDATE resource_blocked_notices SET ended_at = ?1
                 WHERE machine = ?2 AND ended_at IS NULL",
                params![fmt_time(Utc::now()), machine.to_string()],
            )?;
            let Some((job, blocked_since)) = head else {
                return Ok(None);
            };
            self.conn.execute(
                "INSERT INTO resource_blocked_notices (machine, job_id, blocked_since)
                 VALUES (?1, ?2, ?3)",
                params![
                    machine.to_string(),
                    job.to_string(),
                    fmt_time(blocked_since)
                ],
            )?;
            self.blocked_episode(machine)
        })
    }

    /// The open blocking episode of `machine`'s queue
    pub fn blocked_episode(&self, machine: MachineId) -> Result<Option<StoredEpisode>, AppError> {
        let raw: Option<(i64, String, String, Option<String>)> = self
            .conn
            .query_row(
                "SELECT id, job_id, blocked_since, notified_at FROM resource_blocked_notices
                 WHERE machine = ?1 AND ended_at IS NULL",
                [machine.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        raw.map(|(id, job, since, notified)| {
            Ok(StoredEpisode {
                id,
                job: JobId::from_uuid(parse_uuid(&job)?),
                blocked_since: parse_time(&since)?,
                notified: notified.is_some(),
            })
        })
        .transpose()
    }

    /// What the scheduler reads for `machine`'s queue
    pub fn queue_snapshot(
        &self,
        machine: MachineId,
        now: DateTime<Utc>,
        thresholds: NoticeThresholds,
    ) -> Result<Snapshot, AppError> {
        let resources = self
            .resources_on(machine)?
            .into_iter()
            .map(|record| ResourceView {
                id: record.resource.id,
                name: record.resource.name,
                run: record.run,
            })
            .collect();
        let queue = self
            .machine_queue(machine)?
            .into_iter()
            .map(|job| JobView {
                id: job.id,
                priority: job.priority,
                target: job.target,
                preempt: job.spec.preempt,
                state: match job.state {
                    JobState::Active { resource } => QueuedState::Active(resource),
                    _ => QueuedState::Queued,
                },
            })
            .collect();
        Ok(Snapshot {
            now,
            resources,
            queue,
            episode: self.blocked_episode(machine)?,
            thresholds,
        })
    }
}

/// The `failed_run` column of a job in `state`
fn failed_run(state: JobState) -> Option<String> {
    match state {
        JobState::Failed { run } => Some(run.to_string()),
        _ => None,
    }
}

fn stop_cause(phase: &RunPhase) -> Option<StopCause> {
    match phase {
        RunPhase::Stopping { cause, .. } => Some(*cause),
        _ => None,
    }
}

fn event_run(run: &ActiveRun) -> EventRun {
    EventRun {
        resource: run.resource,
        task: run.task,
        run_number: run.run_number,
        step: run.step,
    }
}

fn stale(resource: ResourceId, message: &str) -> AppError {
    QueueError::StaleRun {
        resource,
        message: message.to_owned(),
    }
    .into()
}

fn corrupt(message: impl Into<String>) -> QueueError {
    QueueError::Corrupt {
        message: message.into(),
    }
}

fn parse_uuid(raw: &str) -> Result<Uuid, QueueError> {
    Uuid::parse_str(raw).map_err(|_| corrupt(format!("bad UUID {raw:?}")))
}
