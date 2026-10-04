//! The checkpoint contract of one reserved run

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use crate::container::{ContainerMount, ContainerWorkload, EnvName, GpuRequest};
use crate::error::AppError;
use crate::home::Home;
use crate::queue::{ActiveRun, Resource, StepWorkload};
use crate::run_env;

/// Container mount of the job directory, under the reserved root that job
/// specs may not mount over
const CONTAINER_JOB_DIR: &str = "/homebased/job";

/// Read-only container mount of the run's control directory
const CONTAINER_RUN_DIR: &str = "/homebased/run";

/// The yield file as a container sees it
const CONTAINER_YIELD_FILE: &str = "/homebased/run/yield";

/// Name of the yield file in the run's control directory
const YIELD_FILE_NAME: &str = "yield";

/// Run-owned paths and child environment, derived from the reserved resource
#[derive(Debug, Clone)]
pub struct Checkpoint {
    /// Exact reservation
    pub run: ActiveRun,
    /// Assigned lane and optional device
    pub resource: Resource,
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
        self.prepare_control(home)?;
        let path = self.control_dir(home).join(YIELD_FILE_NAME);
        std::fs::write(&path, b"")?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644))?;
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
        self.prepare_control(home)?;
        Ok(())
    }

    fn prepare_control(&self, home: &Home) -> Result<(), AppError> {
        let path = self.control_dir(home);
        std::fs::create_dir_all(&path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))?;
        Ok(())
    }

    /// Variables for a host step's workload child, never for the daemon or
    /// task worker
    pub fn host_environment(&self, home: &Home) -> Vec<(&'static str, String)> {
        self.environment(
            self.job_dir(home).display().to_string(),
            self.control_dir(home)
                .join(YIELD_FILE_NAME)
                .display()
                .to_string(),
            self.resource.device,
        )
    }

    /// Variables for a container step, at the fixed mount paths
    fn container_environment(&self) -> Vec<(&'static str, String)> {
        // docker exposes one selected host GPU as CUDA ordinal zero
        let ordinal = self.resource.device.map(|_| 0);
        self.environment(
            CONTAINER_JOB_DIR.into(),
            CONTAINER_YIELD_FILE.into(),
            ordinal,
        )
    }

    fn environment(
        &self,
        job_dir: String,
        yield_file: String,
        cuda_ordinal: Option<u32>,
    ) -> Vec<(&'static str, String)> {
        let mut env = vec![
            (run_env::TASK_ID, self.run.task.to_string()),
            (run_env::JOB_ID, self.run.job.to_string()),
            (run_env::JOB_DIR, job_dir),
            (run_env::RUN_NUMBER, self.run.run_number.to_string()),
            (run_env::STEP_INDEX, self.run.step.to_string()),
            (
                run_env::RESUME,
                if self.run.resume { "1" } else { "0" }.into(),
            ),
            (run_env::YIELD_FILE, yield_file),
            (run_env::RESOURCE, self.resource.name.to_string()),
        ];
        if let Some(ordinal) = cuda_ordinal {
            env.push((run_env::CUDA_VISIBLE_DEVICES, ordinal.to_string()));
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
                target: CONTAINER_JOB_DIR.into(),
                read_only: false,
            },
            ContainerMount {
                source: self.control_dir(home),
                target: CONTAINER_RUN_DIR.into(),
                read_only: true,
            },
        ]);
        workload.gpus = self
            .resource
            .device
            .map(|device| GpuRequest::Devices(vec![device]));
        for (key, value) in self.container_environment() {
            let key = EnvName::parse(key).map_err(|message| AppError::Internal { message })?;
            workload.env.insert(key, value);
        }
        Ok(workload)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{CONTAINER_JOB_DIR, CONTAINER_RUN_DIR, CONTAINER_YIELD_FILE, YIELD_FILE_NAME};
    use crate::queue::spec::RESERVED_MOUNT_ROOT;

    #[test]
    fn container_paths_sit_under_the_root_job_specs_may_not_mount() {
        for path in [CONTAINER_JOB_DIR, CONTAINER_RUN_DIR, CONTAINER_YIELD_FILE] {
            assert!(Path::new(path).starts_with(RESERVED_MOUNT_ROOT), "{path}");
        }
        assert_eq!(
            Path::new(CONTAINER_RUN_DIR).join(YIELD_FILE_NAME),
            Path::new(CONTAINER_YIELD_FILE)
        );
    }
}
