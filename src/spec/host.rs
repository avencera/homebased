//! Host inputs a spec names on the machine that runs it: `cwd` and mount sources

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{NormalizedSpec, NormalizedWorkload};
use crate::error::AppError;

/// Check the host inputs that a workload names on the machine that runs it
///
/// A container's mount sources must exist there and must not expose a
/// container daemon socket. Other workloads name no host inputs beyond `cwd`
fn check_workload_host(workload: &NormalizedWorkload) -> Result<(), AppError> {
    match workload {
        NormalizedWorkload::Container(container) => {
            crate::container::check_container_host(container)
        }
        NormalizedWorkload::Agent(_) | NormalizedWorkload::Task(_) => Ok(()),
    }
}

/// Check every host input that a spec names on the machine that runs it
///
/// `cwd` is checked first, then any container mount sources
pub fn check_spec_host(spec: &NormalizedSpec) -> Result<(), HostInputError> {
    if let Some(problem) = cwd_problem(&spec.cwd) {
        return Err(HostInputError {
            rejection: HostInputRejection::Cwd(problem),
            error: invalid_cwd(spec, problem),
        });
    }
    check_workload_host(&spec.workload).map_err(|error| HostInputError {
        rejection: HostInputRejection::MountSource,
        error,
    })
}

/// Require a directory that is not a spec `cwd`, such as a callback directory,
/// to exist and be accessible
pub fn check_cwd(cwd: &Path) -> Result<(), AppError> {
    match cwd_problem(cwd) {
        None => Ok(()),
        Some(problem) => Err(AppError::InvalidCwd {
            path: cwd.to_path_buf(),
            problem,
            suggested_cwd: None,
        }),
    }
}

/// Why `cwd` cannot be used here, or `None` when it is an accessible directory
#[must_use]
pub fn cwd_problem(cwd: &Path) -> Option<CwdProblem> {
    match fs::metadata(cwd) {
        Ok(meta) if !meta.is_dir() => Some(CwdProblem::NotDirectory),
        // a child can start in a directory only with search permission on it
        Ok(_) => nix::unistd::access(cwd, nix::unistd::AccessFlags::X_OK)
            .err()
            .map(|_| CwdProblem::Inaccessible),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Some(CwdProblem::NotFound),
        Err(_) => Some(CwdProblem::Inaccessible),
    }
}

/// Typed `invalid_cwd` error for a spec, with the host path to use when `cwd`
/// names a container mount target
fn invalid_cwd(spec: &NormalizedSpec, problem: CwdProblem) -> AppError {
    AppError::InvalidCwd {
        path: spec.cwd.clone(),
        problem,
        suggested_cwd: container_cwd_suggestion(spec),
    }
}

/// Host path for a `cwd` that lies under a container mount target
///
/// Agents often copy a path from inside the container, such as the mount
/// target, but `cwd` is where the task starts on the host
fn container_cwd_suggestion(spec: &NormalizedSpec) -> Option<PathBuf> {
    let NormalizedWorkload::Container(container) = &spec.workload else {
        return None;
    };
    // the longest target wins, so a nested mount maps to its own source
    container
        .mounts
        .iter()
        .filter_map(|mount| {
            let rest = spec.cwd.strip_prefix(&mount.target).ok()?;
            Some((mount.target.components().count(), mount.source.join(rest)))
        })
        .max_by_key(|(depth, _)| *depth)
        .map(|(_, source)| source)
}

/// Why a `cwd` cannot be used on the machine that runs the task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CwdProblem {
    /// Nothing exists at the path
    NotFound,
    /// The path exists but is not a directory
    NotDirectory,
    /// The directory or a parent cannot be searched by the daemon user
    Inaccessible,
}

impl CwdProblem {
    /// Stable machine-readable name
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::NotDirectory => "not_directory",
            Self::Inaccessible => "inaccessible",
        }
    }

    /// Human-readable clause for error messages
    #[must_use]
    pub const fn describe(self) -> &'static str {
        match self {
            Self::NotFound => "does not exist",
            Self::NotDirectory => "is not a directory",
            Self::Inaccessible => "is not accessible",
        }
    }
}

/// Definitive refusal of a spec's host inputs by the machine that runs it
///
/// It crosses machines as a stable reason string, and the origin rebuilds the
/// typed error from its own copy of the spec. The strings match the reasons
/// that earlier executors already record for rejected executions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostInputRejection {
    /// `cwd` is not an accessible directory there
    Cwd(CwdProblem),
    /// A container mount source is missing there or exposes a daemon socket
    MountSource,
}

impl HostInputRejection {
    /// Durable reason saved in a rejected route or tombstone
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cwd(CwdProblem::NotFound) => "cwd_not_found",
            Self::Cwd(CwdProblem::NotDirectory) => "cwd_not_directory",
            Self::Cwd(CwdProblem::Inaccessible) => "cwd_inaccessible",
            Self::MountSource => "container_host_inputs_unavailable",
        }
    }

    /// Parse a saved rejection reason, or `None` for any other reason
    #[must_use]
    pub fn parse(reason: &str) -> Option<Self> {
        match reason {
            "cwd_not_found" => Some(Self::Cwd(CwdProblem::NotFound)),
            "cwd_not_directory" => Some(Self::Cwd(CwdProblem::NotDirectory)),
            "cwd_inaccessible" => Some(Self::Cwd(CwdProblem::Inaccessible)),
            "container_host_inputs_unavailable" => Some(Self::MountSource),
            _ => None,
        }
    }

    /// Typed error for this refusal of `spec` by another machine
    #[must_use]
    pub fn into_error(self, spec: &NormalizedSpec) -> AppError {
        match self {
            Self::Cwd(problem) => invalid_cwd(spec, problem),
            Self::MountSource => AppError::InvalidSpec {
                pointer: "/workload/mounts".into(),
                value: mount_sources(spec),
                message: "a container mount source is missing, inaccessible, or exposes the \
                          container daemon socket on the machine that runs the task"
                    .into(),
            },
        }
    }
}

fn mount_sources(spec: &NormalizedSpec) -> Value {
    match &spec.workload {
        NormalizedWorkload::Container(container) => container
            .mounts
            .iter()
            .map(|mount| json!(mount.source))
            .collect(),
        NormalizedWorkload::Agent(_) | NormalizedWorkload::Task(_) => Value::Null,
    }
}

/// Host-input check failure with its cross-machine reason
#[derive(Debug)]
pub struct HostInputError {
    /// Reason another machine can rebuild the error from
    pub rejection: HostInputRejection,
    /// Precise error for the machine that checked
    pub error: AppError,
}

impl From<HostInputError> for AppError {
    fn from(error: HostInputError) -> Self {
        error.error
    }
}
