//! Resource and job rows as read, and their checked conversion to records

use chrono::{DateTime, Utc};
use rusqlite::Row;

use super::{JobRecord, ResourceOrigin, ResourceRecord, corrupt, parse_uuid};
use crate::domain::TaskId;
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::spec::JobSpec;
use crate::queue::{
    ActiveRun, AttentionId, JobId, JobState, Priority, QueueError, Resource, ResourceId,
    ResourceName, RunNumber, RunPhase, StepIndex, StopCause, Target,
};
use crate::store::parse_time;

pub(super) const RESOURCE_SELECT: &str =
    "SELECT id, machine, name, device, run_job, run_task, run_number,
    run_step, run_phase, run_reserved_at, run_started_at, run_stop_cause,
    run_stop_requested_at, run_cleanup_attempt, run_attention_id, run_attention_failure, origin,
    run_resume
 FROM resources";

pub(super) const JOB_SELECT: &str = "SELECT id, machine, origin_machine, spec_json, spec_digest,
    target_resource, priority, position, state, active_resource, failed_run, step, resume,
    last_run_number, created_at, updated_at
 FROM resource_jobs";

fn opt_u32(value: Option<i64>, what: &str) -> Result<Option<u32>, QueueError> {
    value
        .map(|value| u32::try_from(value).map_err(|_| corrupt(format!("bad {what} {value}"))))
        .transpose()
}

fn opt_time(value: Option<&str>) -> Result<Option<DateTime<Utc>>, AppError> {
    value.map(parse_time).transpose()
}

/// Resource columns as read, before checks
pub(super) struct RawResource {
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
    pub(super) fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
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

    pub(super) fn parse(self) -> Result<ResourceRecord, AppError> {
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
pub(super) struct RawJob {
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
    pub(super) fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
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

    pub(super) fn parse(self) -> Result<JobRecord, AppError> {
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
