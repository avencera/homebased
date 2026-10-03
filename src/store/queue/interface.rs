//! Typed authority operations and public queue inspection shapes

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::domain::{ProcessStatus, TaskEnv, TaskId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::spec::JobSpec;
use crate::queue::{AttentionId, JobId, OperationId, Placement, QueueError, ResourceName};
use crate::store::Store;

use super::NewJob;

/// One operation on the receiving machine's queue; selectors are resolved by the origin
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueueRequest {
    /// Resources and their held runs
    Resources,
    /// Non-terminal jobs in serving order
    Jobs,
    /// One job and every attempt
    Show {
        /// Job identity
        job: JobId,
    },
    /// Register a hand-managed exclusive lane
    Register {
        /// Machine-local name
        name: ResourceName,
        /// Optional GPU index
        device: Option<u32>,
    },
    /// Idempotent job acceptance
    Submit {
        /// Submitter-selected identity
        job: JobId,
        /// Callback owner
        origin: MachineId,
        /// Canonical accepted spec
        spec: Box<JobSpec>,
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
}

impl QueueRequest {
    /// Whether this request changes queue state
    #[must_use]
    pub const fn is_write(&self) -> bool {
        !matches!(self, Self::Resources | Self::Jobs | Self::Show { .. })
    }
}

/// One historical attempt, including cleanup and the last committed stop cause
#[derive(Debug, Clone, Serialize)]
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
    pub outcome: Option<crate::dependency::TaskOutcome>,
    /// Resource assigned to the attempt; unknown for ended attempts before this schema
    pub resource: Option<crate::queue::ResourceId>,
    /// Last committed stop cause
    pub stop_cause: Option<crate::queue::StopCause>,
    /// Attributable cleanup result, present once cleanup finished
    pub cleanup: Option<Result<(), crate::queue::CleanupFailure>>,
}

impl Store {
    /// Apply an interface operation through the queue's transactional store API
    pub fn queue_request(
        &self,
        machine: MachineId,
        request: &QueueRequest,
        env: &TaskEnv,
    ) -> Result<Value, AppError> {
        let result = match request {
            QueueRequest::Resources => {
                json!({ "machine": machine, "resources": self.resources_on(machine)? })
            }
            QueueRequest::Jobs => {
                json!({ "machine": machine, "jobs": self.machine_queue(machine)? })
            }
            QueueRequest::Show { job } => {
                let record = self
                    .job(*job)?
                    .filter(|record| record.machine == machine)
                    .ok_or(QueueError::JobNotFound { job: *job })?;
                let resources = self.resources_on(machine)?;
                let active = resources
                    .into_iter()
                    .filter_map(|resource| resource.run)
                    .find(|run| run.job == *job);
                let runs = self.job_runs(*job)?;
                let last_stop_cause = runs.iter().rev().find_map(|run| run.stop_cause);
                json!({ "job": record, "active_run": active, "runs": runs,
                    "last_stop_cause": last_stop_cause, "events": self.job_events(*job)? })
            }
            QueueRequest::Register { name, device } => {
                serde_json::to_value(self.register_resource(machine, name.clone(), *device)?)?
            }
            QueueRequest::Submit { job, origin, spec } => {
                serde_json::to_value(self.submit_job(&NewJob {
                    id: *job,
                    machine,
                    origin: *origin,
                    spec: *spec.clone(),
                    env: env.clone(),
                })?)?
            }
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
        };
        let mut result = result;
        if let Some(object) = result.as_object_mut() {
            object.insert("api_version".into(), json!(crate::domain::API_VERSION));
        }
        Ok(result)
    }

    /// Every run of the job, even after the resource starts other work
    pub fn job_runs(&self, job: JobId) -> Result<Vec<JobRunView>, AppError> {
        let mut statement = self.conn.prepare("SELECT t.id,t.run_number,t.step_index,h.resource_id,h.stop_cause,h.cleanup_json
            FROM tasks t LEFT JOIN resource_run_history h ON h.task_id=t.id WHERE t.resource_job_id=?1 ORDER BY t.run_number")?;
        let rows = statement
            .query_map([job.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(task, run_number, step, resource, cause, cleanup)| {
                let task: TaskId = task.parse()?;
                let row = self
                    .get_task(task)?
                    .ok_or(AppError::TaskNotFound { id: task })?;
                Ok(JobRunView {
                    task,
                    run_number,
                    step,
                    status: row.status(),
                    outcome: crate::dependency::TaskOutcome::from_storage(row.status().as_str()),
                    resource: resource.map(|value| value.parse()).transpose()?,
                    stop_cause: cause
                        .map(|value| serde_json::from_str(&value))
                        .transpose()?,
                    cleanup: cleanup
                        .map(|value| serde_json::from_str(&value))
                        .transpose()?,
                })
            })
            .collect()
    }
}
