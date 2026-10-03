//! The checkpoint contract of one reserved run

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use crate::container::{ContainerMount, ContainerWorkload, EnvName, GpuRequest};
use crate::error::AppError;
use crate::home::Home;
use crate::queue::{ActiveRun, Resource, StepWorkload};
use crate::run_env;

/// Run-owned paths and child environment, derived from the reserved resource
#[derive(Debug, Clone)]
pub struct Checkpoint {
    /// Exact reservation
    pub run: ActiveRun,
    /// Assigned lane and optional device
    pub resource: Resource,
    /// Whether this attempt resumes a yielded step
    pub resume: bool,
    /// What this attempt's step runs
    pub step: StepKind,
}

/// Whether a step runs on the host or in a container
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepKind {
    /// A command on the host, running as the job directory's owner
    Task,
    /// A container, whose user may have another UID
    Container,
}

impl From<&StepWorkload> for StepKind {
    fn from(step: &StepWorkload) -> Self {
        match step {
            StepWorkload::Task(_) => Self::Task,
            StepWorkload::Container(_) => Self::Container,
        }
    }
}

impl StepKind {
    /// Job directory mode for this step
    ///
    /// A host step runs as the directory's owner, so owner-only access is
    /// enough. A container user can have another UID, so a container step opens
    /// the directory to everyone; the `0700` state root still keeps other host
    /// users out
    fn job_dir_mode(self) -> u32 {
        match self {
            Self::Task => 0o700,
            Self::Container => 0o777,
        }
    }
}

impl Checkpoint {
    /// Durable host state directory shared by all attempts of this job
    pub fn job_dir(&self, home: &Home) -> PathBuf {
        home.root()
            .join("jobs")
            .join(self.run.job.to_string())
            .join("state")
    }

    /// Run-scoped directory mounted read-only in containers
    pub fn control_dir(&self, home: &Home) -> PathBuf {
        home.task_dir(self.run.task).join("control")
    }

    /// Publish the exact run's checkpoint request, safely repeatable after a crash
    pub fn request_yield(&self, home: &Home) -> Result<(), AppError> {
        std::fs::create_dir_all(self.control_dir(home))?;
        std::fs::write(self.control_dir(home).join("yield"), b"")?;
        Ok(())
    }

    /// Prepare run paths without clearing an already published yield request
    pub fn prepare(&self, home: &Home) -> Result<(), AppError> {
        home.prepare_task(self.run.task)?;
        let job_dir = self.job_dir(home);
        std::fs::create_dir_all(&job_dir)?;
        std::fs::set_permissions(
            job_dir,
            std::fs::Permissions::from_mode(self.step.job_dir_mode()),
        )?;
        std::fs::create_dir_all(self.control_dir(home))?;
        Ok(())
    }

    /// Variables for the workload child, never for the daemon or task worker
    pub fn environment(&self, home: &Home, container: bool) -> Vec<(&'static str, String)> {
        let (job_dir, yield_file) = if container {
            ("/homebased/job".into(), "/homebased/run/yield".into())
        } else {
            (
                self.job_dir(home).display().to_string(),
                self.control_dir(home).join("yield").display().to_string(),
            )
        };
        let mut env = vec![
            (run_env::TASK_ID, self.run.task.to_string()),
            (run_env::JOB_ID, self.run.job.to_string()),
            (run_env::JOB_DIR, job_dir),
            (run_env::RUN_NUMBER, self.run.run_number.to_string()),
            (run_env::STEP_INDEX, self.run.step.to_string()),
            (run_env::RESUME, if self.resume { "1" } else { "0" }.into()),
            (run_env::YIELD_FILE, yield_file),
            (run_env::RESOURCE, self.resource.name.to_string()),
        ];
        if let Some(device) = self.resource.device {
            env.push((run_env::CUDA_VISIBLE_DEVICES, device.to_string()));
        }
        env
    }

    /// Add authority-owned mounts, environment, and GPU selection to a container
    pub fn container_workload(
        &self,
        home: &Home,
        workload: &ContainerWorkload,
    ) -> Result<ContainerWorkload, AppError> {
        let mut workload = workload.clone();
        // device selection belongs to the resource even when the job supplied
        // a container variable and the selected lane has no device index
        workload
            .env
            .retain(|key, _| key.as_str() != run_env::CUDA_VISIBLE_DEVICES);
        workload.mounts.extend([
            ContainerMount {
                source: self.job_dir(home),
                target: "/homebased/job".into(),
                read_only: false,
            },
            ContainerMount {
                source: self.control_dir(home),
                target: "/homebased/run".into(),
                read_only: true,
            },
        ]);
        workload.gpus = self
            .resource
            .device
            .map(|device| GpuRequest::Devices(vec![device]));
        for (key, value) in self.environment(home, true) {
            let key = EnvName::parse(key).map_err(|message| AppError::Internal { message })?;
            workload.env.insert(key, value);
        }
        Ok(workload)
    }
}
