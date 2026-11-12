//! Typed authority operations and public queue inspection shapes

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::dependency::TaskOutcome;
use crate::domain::{API_VERSION, ProcessStatus, TaskEnv, TaskId};
use crate::error::AppError;
use crate::fleet::protocol::{
    CI_PROTOCOL, CLUSTER_PROTOCOL_VERSION, ClusterProtocolVersion, GPU_IDLE_PROTOCOL,
    SUPPORTED_PROTOCOLS,
};
use crate::machine::MachineId;
use crate::queue::ci::{ClassOffer, Drain, JobClass, JobTiming, MachineOffer, SlotCapacity};
use crate::queue::gpu::GpuUuid;
use crate::queue::gpu_idle::{GpuIdleControl, GpuIdleDetail, GpuSummary};
use crate::queue::lease_guard::LeaseIdleTrip;
use crate::queue::spec::JobSpec;
use crate::queue::{
    ActiveRun, AttentionId, CleanupFailure, JobEvent, JobId, OperationId, Placement, QueueError,
    Resource, ResourceId, ResourceLabel, ResourceName, ResourceSelector, StopCause, Target,
};
use crate::store::Store;

use super::{JobLeaseNote, JobRecord, NewJob, ResourceRecord};

/// One operation on the receiving machine's queue; selectors are resolved by the origin
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueueRequest {
    /// Resources and their held runs
    Resources,
    /// Non-terminal jobs in serving order
    Jobs,
    /// Resources and jobs read together, so a run and its job agree
    Overview,
    /// One job and every attempt
    Show {
        /// Job identity
        job: JobId,
    },
    /// Check whether a blocked notice's authority episode is still open
    NoticeCurrent {
        /// Job whose notice is about to reach its callback
        job: JobId,
        /// Exact numbered notice, retained even when suppressed
        seq: u64,
    },
    /// Register a hand-managed exclusive lane
    Register {
        /// Machine-local name
        name: ResourceName,
        /// Optional GPU index
        device: Option<u32>,
    },
    /// Set or clear a resource's display label
    Label {
        /// Resource by name or id
        resource: ResourceSelector,
        /// New label; `None` clears it
        label: Option<ResourceLabel>,
    },
    /// Idempotent job acceptance
    Submit {
        /// Submitter-selected identity
        job: JobId,
        /// Callback owner
        origin: MachineId,
        /// Canonical accepted spec
        spec: Box<JobSpec>,
        /// When the origin took a CI job, which starts its wait; cluster
        /// protocol 6 and later
        #[serde(default, skip_serializing_if = "Option::is_none")]
        submitted_at: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// Idempotent queue placement
    Move {
        /// Operation replay identity
        operation: OperationId,
        /// Job to move
        job: JobId,
        /// Exactly one placement
        placement: Placement,
    },
    /// Idempotent job cancellation
    Cancel {
        /// Operation replay identity
        operation: OperationId,
        /// Job to cancel
        job: JobId,
    },
    /// Release an exact Attention, never a later run
    Release {
        /// Operation replay identity
        operation: OperationId,
        /// Attention checked by the operator
        attention: AttentionId,
    },
    /// Snooze, turn off, or restore a job's GPU idle alerts; cluster
    /// protocol 4 and later
    GpuIdle {
        /// Operation replay identity
        operation: OperationId,
        /// Job whose alerts change
        job: JobId,
        /// What to do
        control: GpuIdleControl,
    },
    /// The machine's CI offer and slots; the daemon answers it, since the
    /// offer reads load and disk. Cluster protocol 6 and later
    CiStatus,
    /// Set one class's slot count; cluster protocol 6 and later
    CiCapacity {
        /// Class to change
        class: JobClass,
        /// New slot count; 0 stops the machine taking the class
        capacity: SlotCapacity,
    },
    /// Drain the machine for CI jobs, or reopen it; cluster protocol 6 and
    /// later
    CiDrain {
        /// Why it is drained; `None` reopens it
        reason: Option<String>,
    },
    /// Timings of CI jobs the machine accepted; cluster protocol 6 and later
    CiTimings {
        /// Earliest end to include; jobs that have not ended are always included
        since: chrono::DateTime<chrono::Utc>,
    },
}

impl QueueRequest {
    /// Lowest cluster protocol whose authorities understand this request
    #[must_use]
    pub fn min_protocol(&self) -> ClusterProtocolVersion {
        match self {
            Self::GpuIdle { .. } => GPU_IDLE_PROTOCOL,
            Self::CiStatus
            | Self::CiCapacity { .. }
            | Self::CiDrain { .. }
            | Self::CiTimings { .. } => CI_PROTOCOL,
            Self::Submit { spec, .. } => submit_protocol(spec),
            _ => SUPPORTED_PROTOCOLS.min(),
        }
    }
}

/// Lowest cluster protocol whose authorities accept `spec`
///
/// An older authority refuses a spec with `gpu_idle` or `ci` as an unknown
/// field
#[must_use]
pub fn submit_protocol(spec: &JobSpec) -> ClusterProtocolVersion {
    if spec.ci.is_some() {
        CI_PROTOCOL
    } else if spec.gpu_idle.is_some() {
        GPU_IDLE_PROTOCOL
    } else {
        SUPPORTED_PROTOCOLS.min()
    }
}

/// One machine's CI slots, its drain, and per class its capacity, busy
/// slots, and queued jobs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CiSlotList {
    /// Machine the slots are on
    pub machine: MachineId,
    /// Its drain, if any
    pub drain: Option<Drain>,
    /// Per-class counts
    pub classes: Vec<ClassOffer>,
    /// Every slot with its run, retired ones included
    pub slots: Vec<ResourceRecord>,
}

/// `ci_status`: what placement reads, and the slots behind it
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CiStatus {
    /// Admission, load, disk, and per-class counts
    pub offer: MachineOffer,
    /// Every slot with its run, retired ones included
    pub slots: Vec<ResourceRecord>,
}

/// CI job timings one machine recorded
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CiTimingList {
    /// Machine that accepted the jobs
    pub machine: MachineId,
    /// One per job, by submit time
    pub timings: Vec<JobTiming>,
}

impl Store {
    /// Resources of the GPU queue; CI slots show in `ci slots` instead
    fn gpu_resources_on(&self, machine: MachineId) -> Result<Vec<ResourceRecord>, AppError> {
        let mut resources = self.resources_on(machine)?;
        resources.retain(|record| record.resource.slot.is_none());
        Ok(resources)
    }

    /// Non-terminal jobs of the GPU queue; CI jobs run on CI slots
    fn gpu_machine_queue(&self, machine: MachineId) -> Result<Vec<JobRecord>, AppError> {
        let mut jobs = self.machine_queue(machine)?;
        jobs.retain(|job| job.spec.ci.is_none());
        Ok(jobs)
    }

    /// The CI slots, drain, and class counts of `machine`
    pub fn ci_slot_list(&self, machine: MachineId) -> Result<CiSlotList, AppError> {
        Ok(CiSlotList {
            machine,
            drain: self.ci_drain(machine)?,
            classes: self.ci_class_offers(machine)?,
            slots: self.ci_slots(machine)?,
        })
    }
}

/// One machine's resources, by name, with their active runs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceList {
    /// Machine whose queue the resources serve
    pub machine: MachineId,
    /// Resources and their runs
    pub resources: Vec<ResourceRecord>,
}

/// One machine's non-terminal jobs in serving order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobList {
    /// Machine whose queue holds the jobs
    pub machine: MachineId,
    /// Jobs, highest level first, then by position
    pub jobs: Vec<JobRecord>,
    /// Lease guard trips and pending yields, by job
    // an older peer sends none
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub lease_notes: BTreeMap<JobId, JobLeaseNote>,
}

/// One machine's resources and non-terminal jobs from a single store read
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueOverview {
    /// Machine whose queue this is
    pub machine: MachineId,
    /// Resources by name, with their runs
    pub resources: Vec<ResourceRecord>,
    /// Jobs, highest level first, then by position
    pub jobs: Vec<JobRecord>,
    /// Lease guard trips and pending yields, by job
    // an older peer sends none
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub lease_notes: BTreeMap<JobId, JobLeaseNote>,
}

/// One job with its active run, every attempt, and its events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDetail {
    /// The job
    pub job: JobRecord,
    /// Its run on a resource, while one holds it
    pub active_run: Option<ActiveRun>,
    /// Every attempt, oldest first
    pub runs: Vec<JobRunView>,
    /// Stop cause of the latest attempt that committed one
    pub last_stop_cause: Option<StopCause>,
    /// Job events in sequence order
    pub events: Vec<JobEvent>,
    /// The pinned resource and each resource a run held, with names and labels
    // an older peer sends none
    #[serde(default)]
    pub resources: Vec<Resource>,
    /// GPU idle silences and per-attempt accounting
    // an older peer sends none
    #[serde(default)]
    pub gpu_idle: Option<GpuIdleDetail>,
    /// Every lease guard trip of the job, oldest first
    // an older peer sends none
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lease_idle: Vec<LeaseIdleTrip>,
}

/// One historical attempt, including cleanup and the last committed stop cause
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunView {
    /// Ordinary task identity for logs
    pub task: TaskId,
    /// Attempt count across the job
    pub run_number: u32,
    /// Zero-based step index
    pub step: u32,
    /// Task process state
    pub status: ProcessStatus,
    /// Run outcome, present once terminal
    pub outcome: Option<TaskOutcome>,
    /// Resource assigned to the attempt; unknown for ended attempts before this schema
    pub resource: Option<ResourceId>,
    /// Last committed stop cause
    pub stop_cause: Option<StopCause>,
    /// Attributable cleanup result, present once cleanup finished
    pub cleanup: Option<Result<(), CleanupFailure>>,
    /// UUID of the GPU the attempt launched on, when its resource had one
    // an older peer sends none
    #[serde(default)]
    pub gpu_uuid: Option<GpuUuid>,
    /// GPU usage of the attempt, computed once when it ended
    // an older peer sends none
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_summary: Option<GpuSummary>,
}

impl Store {
    /// Apply an interface operation through the queue's transactional store API
    pub fn queue_request(
        &self,
        machine: MachineId,
        request: &QueueRequest,
        env: &TaskEnv,
    ) -> Result<Value, AppError> {
        self.queue_request_at(machine, request, env, CLUSTER_PROTOCOL_VERSION)
    }

    /// Apply an operation that arrived over cluster protocol `protocol`
    ///
    /// A submit keeps the version, so the queue sends its origin only events
    /// that version can parse
    pub fn queue_request_at(
        &self,
        machine: MachineId,
        request: &QueueRequest,
        env: &TaskEnv,
        protocol: ClusterProtocolVersion,
    ) -> Result<Value, AppError> {
        let mut result = match request {
            QueueRequest::Resources => serde_json::to_value(ResourceList {
                machine,
                resources: self.gpu_resources_on(machine)?,
            })?,
            QueueRequest::Jobs => {
                let jobs = self.gpu_machine_queue(machine)?;
                serde_json::to_value(JobList {
                    machine,
                    lease_notes: self.job_lease_notes(machine, jobs.iter().map(|job| job.id))?,
                    jobs,
                })?
            }
            QueueRequest::Overview => {
                let jobs = self.gpu_machine_queue(machine)?;
                serde_json::to_value(QueueOverview {
                    machine,
                    resources: self.gpu_resources_on(machine)?,
                    lease_notes: self.job_lease_notes(machine, jobs.iter().map(|job| job.id))?,
                    jobs,
                })?
            }
            QueueRequest::Show { job } => {
                let record = self
                    .job(*job)?
                    .filter(|record| record.machine == machine)
                    .ok_or(QueueError::JobNotFound { job: *job })?;
                let active_run = self
                    .resources_on(machine)?
                    .into_iter()
                    .filter_map(|resource| resource.run)
                    .find(|run| run.job == *job);
                let runs = self.job_runs(*job)?;
                let last_stop_cause = runs.iter().rev().find_map(|run| run.stop_cause);
                let pinned = match record.target {
                    Target::Pinned(resource) => Some(resource),
                    Target::Any => None,
                };
                let mut ids = Vec::new();
                let held = runs.iter().filter_map(|run| run.resource);
                for id in pinned
                    .into_iter()
                    .chain(active_run.as_ref().map(|run| run.resource))
                    .chain(held)
                {
                    if !ids.contains(&id) {
                        ids.push(id);
                    }
                }
                let mut resources = Vec::new();
                for id in ids {
                    if let Some(record) = self.resource(id)? {
                        resources.push(record.resource);
                    }
                }
                serde_json::to_value(JobDetail {
                    job: record,
                    active_run,
                    runs,
                    last_stop_cause,
                    events: self.job_events(*job)?,
                    resources,
                    gpu_idle: Some(self.gpu_idle_detail(*job)?),
                    lease_idle: self.lease_idle_trips(*job)?,
                })?
            }
            QueueRequest::NoticeCurrent { job, seq } => {
                self.require_machine_job(machine, *job)?;
                json!({ "current": self.job_notice_is_current(*job, *seq)? })
            }
            QueueRequest::Register { name, device } => {
                serde_json::to_value(self.register_resource(machine, name.clone(), *device)?)?
            }
            QueueRequest::Label { resource, label } => {
                serde_json::to_value(self.set_resource_label(machine, resource, label.as_ref())?)?
            }
            QueueRequest::Submit {
                job,
                origin,
                spec,
                submitted_at,
            } => serde_json::to_value(self.submit_job(&NewJob {
                id: *job,
                machine,
                origin: *origin,
                spec: *spec.clone(),
                env: env.clone(),
                origin_protocol: protocol,
                submitted_at: *submitted_at,
            })?)?,
            QueueRequest::Move {
                operation,
                job,
                placement,
            } => serde_json::to_value(self.move_job(machine, *operation, *job, *placement)?)?,
            QueueRequest::Cancel { operation, job } => serde_json::to_value(self.cancel_job(
                machine,
                *operation,
                *job,
                chrono::Utc::now(),
            )?)?,
            QueueRequest::Release {
                operation,
                attention,
            } => serde_json::to_value(
                self.release_resource_attention(machine, *operation, *attention)?,
            )?,
            QueueRequest::GpuIdle {
                operation,
                job,
                control,
            } => serde_json::to_value(self.set_gpu_idle_control(
                machine,
                *operation,
                *job,
                control,
                chrono::Utc::now(),
            )?)?,
            QueueRequest::CiStatus => {
                return Err(AppError::Internal {
                    message: "the daemon answers ci_status, not the store".into(),
                });
            }
            QueueRequest::CiCapacity { class, capacity } => {
                self.set_ci_capacity(machine, class, *capacity)?;
                serde_json::to_value(self.ci_slot_list(machine)?)?
            }
            QueueRequest::CiDrain { reason } => {
                let drain = reason.clone().map(|reason| Drain {
                    reason,
                    since: chrono::Utc::now(),
                });
                self.set_ci_drain(machine, drain)?;
                serde_json::to_value(self.ci_slot_list(machine)?)?
            }
            QueueRequest::CiTimings { since } => serde_json::to_value(CiTimingList {
                machine,
                timings: self.ci_timings(*since)?,
            })?,
        };
        if let Some(object) = result.as_object_mut() {
            object.insert("api_version".into(), json!(API_VERSION));
        }
        Ok(result)
    }

    /// Every run of the job, even after the resource starts other work
    pub fn job_runs(&self, job: JobId) -> Result<Vec<JobRunView>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT t.id, t.run_number, t.step_index, h.resource_id, h.stop_cause, h.cleanup_json,
                    h.gpu_uuid
             FROM tasks t LEFT JOIN resource_run_history h ON h.task_id = t.id
             WHERE t.resource_job_id = ?1 ORDER BY t.run_number",
        )?;
        let rows = statement
            .query_map([job.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(
                |(task, run_number, step, resource, cause, cleanup, gpu_uuid)| {
                    let task: TaskId = task.parse()?;
                    let row = self
                        .get_task(task)?
                        .ok_or(AppError::TaskNotFound { id: task })?;
                    Ok(JobRunView {
                        task,
                        run_number,
                        step,
                        status: row.status(),
                        outcome: TaskOutcome::from_storage(row.status().as_str()),
                        resource: resource.map(|value| value.parse()).transpose()?,
                        stop_cause: cause
                            .map(|value| serde_json::from_str(&value))
                            .transpose()?,
                        cleanup: cleanup
                            .map(|value| serde_json::from_str(&value))
                            .transpose()?,
                        gpu_uuid: gpu_uuid
                            .as_deref()
                            .map(GpuUuid::parse)
                            .transpose()
                            .map_err(|message| QueueError::Corrupt { message })?,
                        gpu_summary: self.gpu_summary(task)?,
                    })
                },
            )
            .collect()
    }
}
