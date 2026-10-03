//! Store side of the GPU priority queue
//!
//! Every operation runs in one `BEGIN IMMEDIATE` transaction and keeps the
//! queue's invariants: one active run per resource and per job, the job of an
//! active run is `Active` exactly while the run still executes it, and every
//! non-terminal job holds a slot that is unique and dense within its level of
//! the machine queue. The terminal commit of a run task classifies the run's
//! end and moves its job in the same transaction

mod delivery;
pub use delivery::JobCursors;
pub mod interface;
mod runtime;

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Row, params};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskExitEvidence, TaskId,
    TaskRow, ThreadId, check_status_transition,
};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::classify::{JobTransition, RunEnd, RunOutcome, classify};
use crate::queue::gpu::DetectedResource;
use crate::queue::schedule::{
    JobView, NoticeThresholds, Preempt, QueuedState, ResourceView, Snapshot, StoredEpisode, decide,
};
use crate::queue::spec::JobSpec;
use crate::queue::{
    ActiveRun, AttentionId, BlockedNotice, CleanupFailure, EventRun, JobEvent, JobEventKind, JobId,
    JobState, LevelEnd, MoveRefusal, OperationId, Placement, Priority, QueueError, Resource,
    ResourceId, ResourceName, ResourceSelector, RunNumber, RunPhase, Side, StepIndex, StepWorkload,
    StopCause, Target, sha256_hex,
};

use super::{NewTask, Store, find_project_root, fmt_time, new_queued_task, parse_time};

/// Added to every position of a level before it is rewritten, so the unique
/// slot index never sees two jobs on one position mid-renumber
const RENUMBER_OFFSET: i64 = 1 << 40;

const RESOURCE_SELECT: &str = "SELECT id, machine, name, device, run_job, run_task, run_number,
    run_step, run_phase, run_reserved_at, run_started_at, run_stop_cause,
    run_stop_requested_at, run_cleanup_attempt, run_attention_id, run_attention_failure, origin,
    run_resume
 FROM resources";

const JOB_SELECT: &str = "SELECT id, machine, origin_machine, spec_json, spec_digest,
    target_resource, priority, position, state, active_resource, failed_run, step, resume,
    last_run_number, created_at, updated_at
 FROM resource_jobs";

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

/// Which code path ends a run task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunEndSource {
    /// The worker, the task layer, or a cancel ended it: apply the table
    Task,
    /// The actor gave up on a launch its worker never confirmed; the job
    /// returns to its slot without counting a failure
    LaunchAbandoned,
}

/// Where a move inserts the job within its new level
#[derive(Debug, Clone, Copy)]
enum Slot {
    /// An end of the level
    End(LevelEnd),
    /// Beside another job of the level
    Next { target: JobId, side: Side },
}

/// Where a run's end leaves its job, and the event that records it
struct JobAfterRun {
    state: JobState,
    /// The step a queued job runs next, or the step a terminal job ended on
    step: StepIndex,
    event: Option<JobEventKind>,
}

impl Store {
    /// Ensure one resource per detected GPU on `machine`
    ///
    /// Idempotent: an existing resource keeps its UUID, and a detected GPU
    /// whose name is taken by a hand-registered resource is left alone.
    /// An idle fallback adopts its first device's name unless that name is taken
    /// Detected resources are never removed, so a GPU that disappears leaves
    /// its resource in place
    pub fn ensure_detected_resources(
        &self,
        machine: MachineId,
        detected: &[DetectedResource],
    ) -> Result<Vec<ResourceRecord>, AppError> {
        self.immediate(|| {
            let existing = self.resources_on(machine)?;
            let first_device = detected.iter().find_map(|resource| resource.device);
            let Some(device) = first_device else {
                // a fallback is never added beside an indexed resource
                if existing
                    .iter()
                    .any(|record| record.resource.device.is_some())
                {
                    return Ok(existing);
                }
                self.insert_detected(machine, detected)?;
                return self.resources_on(machine);
            };

            let fallback = existing
                .iter()
                .find(|record| record.origin == ResourceOrigin::DetectedFallback);
            if let Some(fallback) = fallback
                && !self.adopt_fallback_device(machine, &existing, fallback, device)?
            {
                return Ok(existing);
            }
            self.insert_detected(machine, detected)?;
            self.resources_on(machine)
        })
    }

    /// Give an idle fallback the first detected device, keeping its UUID
    ///
    /// Returns `false` when detection must wait for a later start: the fallback
    /// has an active run, or another resource already holds the device
    fn adopt_fallback_device(
        &self,
        machine: MachineId,
        existing: &[ResourceRecord],
        fallback: &ResourceRecord,
        device: u32,
    ) -> Result<bool, AppError> {
        let id = fallback.resource.id;
        if fallback.run.is_some() {
            tracing::warn!(
                %machine,
                resource = %id,
                "GPU detection deferred while the fallback has an active run"
            );
            return Ok(false);
        }
        if existing
            .iter()
            .any(|record| record.resource.device == Some(device))
        {
            tracing::warn!(
                %machine,
                device,
                "GPU detection deferred because the fallback device is already registered"
            );
            return Ok(false);
        }

        let detected_name = ResourceName::gpu(device);
        let name_taken = existing
            .iter()
            .any(|record| record.resource.id != id && record.resource.name == detected_name);
        let name = if name_taken {
            tracing::warn!(
                %machine,
                resource = %id,
                name = %detected_name,
                "GPU fallback keeps its name because the detected device name is already registered"
            );
            &fallback.resource.name
        } else {
            &detected_name
        };
        self.conn.execute(
            "UPDATE resources SET device = ?2, name = ?3, origin = ?4 WHERE id = ?1",
            params![
                id.to_string(),
                i64::from(device),
                name.as_str(),
                ResourceOrigin::DetectedDevice.as_str()
            ],
        )?;
        Ok(true)
    }

    /// Insert each detected resource whose name and device are both free
    fn insert_detected(
        &self,
        machine: MachineId,
        detected: &[DetectedResource],
    ) -> Result<(), AppError> {
        for resource in detected {
            let origin = match resource.device {
                Some(_) => ResourceOrigin::DetectedDevice,
                None => ResourceOrigin::DetectedFallback,
            };
            self.conn.execute(
                "INSERT INTO resources (id, machine, name, device, created_at, origin)
                 SELECT ?1, ?2, ?3, ?4, ?5, ?6
                 WHERE NOT EXISTS (
                     SELECT 1 FROM resources WHERE machine = ?2
                       AND (name = ?3 OR (?4 IS NOT NULL AND device = ?4))
                 )",
                params![
                    ResourceId::new().to_string(),
                    machine.to_string(),
                    resource.name.as_str(),
                    resource.device.map(i64::from),
                    fmt_time(Utc::now()),
                    origin.as_str()
                ],
            )?;
        }
        Ok(())
    }

    /// Register a resource on `machine` by hand
    pub fn register_resource(
        &self,
        machine: MachineId,
        name: ResourceName,
        device: Option<u32>,
    ) -> Result<ResourceRecord, AppError> {
        self.immediate(|| {
            let existing = self.resources_on(machine)?;
            if existing.iter().any(|record| record.resource.name == name) {
                return Err(QueueError::ResourceNameTaken { name }.into());
            }
            if let Some(device) = device
                && let Some(holder) = existing
                    .iter()
                    .find(|record| record.resource.device == Some(device))
            {
                return Err(QueueError::DeviceTaken {
                    device,
                    resource: holder.resource.name.clone(),
                }
                .into());
            }
            let id = ResourceId::new();
            self.conn.execute(
                "INSERT INTO resources (id, machine, name, device, created_at, origin)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    id.to_string(),
                    machine.to_string(),
                    name.as_str(),
                    device.map(i64::from),
                    fmt_time(Utc::now()),
                    ResourceOrigin::Manual.as_str(),
                ],
            )?;
            self.require_resource(id)
        })
    }

    /// Every resource of `machine`, by name
    pub fn resources_on(&self, machine: MachineId) -> Result<Vec<ResourceRecord>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "{RESOURCE_SELECT} WHERE machine = ?1 ORDER BY name"
        ))?;
        let raws = statement
            .query_map([machine.to_string()], RawResource::read)?
            .collect::<Result<Vec<_>, _>>()?;
        raws.into_iter().map(RawResource::parse).collect()
    }

    /// One resource by id
    pub fn resource(&self, id: ResourceId) -> Result<Option<ResourceRecord>, AppError> {
        self.conn
            .query_row(
                &format!("{RESOURCE_SELECT} WHERE id = ?1"),
                [id.to_string()],
                RawResource::read,
            )
            .optional()?
            .map(RawResource::parse)
            .transpose()
    }

    fn require_resource(&self, id: ResourceId) -> Result<ResourceRecord, AppError> {
        self.resource(id)?.ok_or_else(|| {
            QueueError::ResourceNotFound {
                resource: id.to_string(),
            }
            .into()
        })
    }

    /// Resolve a resource of `machine` by name or id
    pub fn resolve_resource(
        &self,
        machine: MachineId,
        selector: &ResourceSelector,
    ) -> Result<ResourceRecord, AppError> {
        self.resources_on(machine)?
            .into_iter()
            .find(|record| match selector {
                ResourceSelector::Id(id) => record.resource.id == *id,
                ResourceSelector::Name(name) => record.resource.name == *name,
            })
            .ok_or_else(|| {
                QueueError::ResourceNotFound {
                    resource: selector.to_string(),
                }
                .into()
            })
    }

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

    fn require_job(&self, id: JobId) -> Result<JobRecord, AppError> {
        self.job(id)?
            .ok_or_else(|| QueueError::JobNotFound { job: id }.into())
    }

    /// A job of `machine`'s queue; a job of another queue is not found here
    fn require_machine_job(&self, machine: MachineId, id: JobId) -> Result<JobRecord, AppError> {
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
    fn level(&self, machine: MachineId, priority: Priority) -> Result<Vec<JobId>, AppError> {
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
    fn renumber_level(&self, machine: MachineId, priority: Priority) -> Result<(), AppError> {
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
    fn with_operation<R: Serialize + DeserializeOwned>(
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
        let run_number = RunNumber::new(record.runs + 1)?;

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
        let project_root = find_project_root(&row.cwd);
        super::insert_task_with_project_root_on(&self.conn, &row, project_root.as_deref())?;
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

    pub(super) fn request_stop(
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
            let next = attempt + 1;
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
    pub(super) fn commit_job_run_end(
        &self,
        task: TaskId,
        from: ProcessStatus,
        reason: Option<&ExitReason>,
        evidence: Option<&TaskExitEvidence>,
        worker_thread: Option<ThreadId>,
    ) -> Result<Option<TaskRow>, AppError> {
        match (reason, evidence) {
            (Some(reason), Some(evidence)) => self.commit_terminal(
                task,
                from,
                reason,
                evidence,
                worker_thread,
                RunEndSource::Task,
            ),
            _ => self.commit_lost(task, from, worker_thread),
        }
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

    fn commit_lost(
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

    fn append_job_event(
        &self,
        job: JobId,
        event: JobEventKind,
        run: Option<EventRun>,
        process: Option<ExitReason>,
        attention: Option<AttentionId>,
    ) -> Result<JobEvent, AppError> {
        self.append_job_event_with_notice(job, event, run, process, attention, None)
    }

    fn append_job_event_with_notice(
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
    pub(super) fn run_of_job(&self, job: JobId) -> Result<Option<ActiveRun>, AppError> {
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

    fn clear_run(&self, resource: ResourceId) -> Result<(), AppError> {
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

/// The `failed_run` column of a job in `state`
fn failed_run(state: JobState) -> Option<String> {
    match state {
        JobState::Failed { run } => Some(run.to_string()),
        _ => None,
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

fn sql_position(position: usize) -> Result<i64, AppError> {
    i64::try_from(position).map_err(|_| {
        QueueError::Invariant {
            message: "queue position exceeds SQLite range".into(),
        }
        .into()
    })
}

fn opt_u32(value: Option<i64>, what: &str) -> Result<Option<u32>, QueueError> {
    value
        .map(|value| u32::try_from(value).map_err(|_| corrupt(format!("bad {what} {value}"))))
        .transpose()
}

fn opt_time(value: Option<&str>) -> Result<Option<DateTime<Utc>>, AppError> {
    value.map(parse_time).transpose()
}

/// Resource columns as read, before checks
struct RawResource {
    id: String,
    machine: String,
    name: String,
    device: Option<i64>,
    run_job: Option<String>,
    run_task: Option<String>,
    run_number: Option<i64>,
    run_step: Option<i64>,
    run_phase: Option<String>,
    reserved_at: Option<String>,
    started_at: Option<String>,
    stop_cause: Option<String>,
    stop_requested_at: Option<String>,
    cleanup_attempt: Option<i64>,
    attention_id: Option<String>,
    attention_failure: Option<String>,
    origin: String,
    run_resume: Option<bool>,
}

impl RawResource {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            machine: row.get(1)?,
            name: row.get(2)?,
            device: row.get(3)?,
            run_job: row.get(4)?,
            run_task: row.get(5)?,
            run_number: row.get(6)?,
            run_step: row.get(7)?,
            run_phase: row.get(8)?,
            reserved_at: row.get(9)?,
            started_at: row.get(10)?,
            stop_cause: row.get(11)?,
            stop_requested_at: row.get(12)?,
            cleanup_attempt: row.get(13)?,
            attention_id: row.get(14)?,
            attention_failure: row.get(15)?,
            origin: row.get(16)?,
            run_resume: row.get(17)?,
        })
    }

    /// The active run's phase from `run_phase` and its phase-specific columns
    fn phase(&self) -> Result<RunPhase, AppError> {
        let phase = match self.run_phase.as_deref().unwrap_or_default() {
            "launching" => RunPhase::Launching {
                reserved_at: required_time(self.reserved_at.as_deref(), "reserved_at")?,
            },
            "executing" => RunPhase::Executing {
                started_at: required_time(self.started_at.as_deref(), "started_at")?,
            },
            "stopping" => RunPhase::Stopping {
                started_at: opt_time(self.started_at.as_deref())?,
                cause: StopCause::parse(
                    self.stop_cause
                        .as_deref()
                        .ok_or_else(|| corrupt("stopping run has no cause"))?,
                )?,
                requested_at: required_time(
                    self.stop_requested_at.as_deref(),
                    "stop_requested_at",
                )?,
            },
            "cleaning" => RunPhase::Cleaning {
                attempt: opt_u32(self.cleanup_attempt, "cleanup attempt")?
                    .ok_or_else(|| corrupt("cleaning run has no attempt"))?,
            },
            "attention" => RunPhase::Attention {
                id: AttentionId::from_uuid(parse_uuid(
                    self.attention_id
                        .as_deref()
                        .ok_or_else(|| corrupt("attention has no id"))?,
                )?),
                failure: serde_json::from_str(
                    self.attention_failure
                        .as_deref()
                        .ok_or_else(|| corrupt("attention has no failure"))?,
                )?,
            },
            other => return Err(corrupt(format!("unknown run phase {other:?}")).into()),
        };
        Ok(phase)
    }

    fn parse(self) -> Result<ResourceRecord, AppError> {
        let id = ResourceId::from_uuid(parse_uuid(&self.id)?);
        let resource = Resource {
            id,
            name: ResourceName::parse(&self.name)?,
            device: opt_u32(self.device, "device")?,
        };
        let machine: MachineId = self.machine.parse()?;
        let run = match (
            self.run_phase.is_some(),
            &self.run_job,
            &self.run_task,
            self.run_number,
            self.run_step,
            self.run_resume,
        ) {
            (false, None, None, None, None, None) => None,
            (true, Some(job), Some(task), Some(number), Some(step), Some(resume)) => {
                Some(ActiveRun {
                    resource: id,
                    job: JobId::from_uuid(parse_uuid(job)?),
                    task: TaskId(parse_uuid(task)?),
                    run_number: RunNumber::new(
                        u32::try_from(number).map_err(|_| corrupt("bad run number"))?,
                    )?,
                    step: StepIndex::new(u32::try_from(step).map_err(|_| corrupt("bad run step"))?),
                    resume,
                    phase: self.phase()?,
                })
            }
            _ => return Err(corrupt(format!("resource {id} has a partial active run")).into()),
        };
        Ok(ResourceRecord {
            origin: ResourceOrigin::parse(&self.origin)?,
            machine,
            resource,
            run,
        })
    }
}

fn required_time(value: Option<&str>, what: &str) -> Result<DateTime<Utc>, AppError> {
    opt_time(value)?.ok_or_else(|| corrupt(format!("run has no {what}")).into())
}

/// Job columns as read, before checks
struct RawJob {
    id: String,
    machine: String,
    origin: String,
    spec: String,
    digest: String,
    target: Option<String>,
    priority: i64,
    position: Option<i64>,
    state: String,
    active_resource: Option<String>,
    failed_run: Option<String>,
    step: i64,
    resume: Option<bool>,
    runs: i64,
    created_at: String,
    updated_at: String,
}

impl RawJob {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            machine: row.get("machine")?,
            origin: row.get("origin_machine")?,
            spec: row.get("spec_json")?,
            digest: row.get("spec_digest")?,
            target: row.get("target_resource")?,
            priority: row.get("priority")?,
            position: row.get("position")?,
            state: row.get("state")?,
            active_resource: row.get("active_resource")?,
            failed_run: row.get("failed_run")?,
            step: row.get("step")?,
            resume: row.get("resume")?,
            runs: row.get("last_run_number")?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }

    fn parse(self) -> Result<JobRecord, AppError> {
        let id = JobId::from_uuid(parse_uuid(&self.id)?);
        let spec = JobSpec::parse_value(&serde_json::from_str(&self.spec)?)?;
        let step = StepIndex::new(u32::try_from(self.step).map_err(|_| corrupt("bad step"))?);
        let state = match (
            self.state.as_str(),
            self.active_resource,
            self.failed_run,
            self.resume,
        ) {
            ("queued", None, None, Some(resume)) => JobState::Queued { resume },
            ("active", Some(resource), None, None) => JobState::Active {
                resource: ResourceId::from_uuid(parse_uuid(&resource)?),
            },
            ("succeeded", None, None, None) => JobState::Succeeded,
            ("failed", None, Some(run), None) => JobState::Failed {
                run: TaskId(parse_uuid(&run)?),
            },
            ("cancelled", None, None, None) => JobState::Cancelled,
            (state, ..) => return Err(corrupt(format!("job {id} has bad state {state:?}")).into()),
        };
        let target = match self.target {
            None => Target::Any,
            Some(resource) => Target::Pinned(ResourceId::from_uuid(parse_uuid(&resource)?)),
        };
        Ok(JobRecord {
            id,
            machine: self.machine.parse()?,
            origin: self.origin.parse()?,
            spec,
            digest: self.digest,
            target,
            priority: Priority::from_rank(self.priority)
                .ok_or_else(|| corrupt(format!("job {id} has bad priority {}", self.priority)))?,
            position: opt_u32(self.position, "position")?,
            state,
            step,
            runs: u32::try_from(self.runs).map_err(|_| corrupt("bad run count"))?,
            created_at: parse_time(&self.created_at)?,
            updated_at: parse_time(&self.updated_at)?,
        })
    }
}

#[cfg(test)]
mod tests;
