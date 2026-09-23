//! Strict validation of the maintained direct-segment trainer invocation

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use thiserror::Error;

use crate::domain::{TaskId, TaskRow};
use crate::invocation::{CommandLine, resolve_executable};
use crate::resource::TrainerAttemptAssociation;
use crate::resource::ownership_lock::VerifiedTrainerAttempt;
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::submission::{NormalizedSpecSha256, normalized_spec_sha256};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(86_400);
const MAX_TIMEOUT_SECONDS: f64 = 7.0 * 24.0 * 60.0 * 60.0;
const REQUIRED_MODULE_ARGS: [&str; 3] = ["-m", "ops.run_segment", "run"];

/// A path role checked while binding an accepted direct-segment invocation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectSegmentPathRole {
    /// The trainer task metadata file passed with `--task`
    TaskFile,
    /// The prepared trainer input directory passed with `--input-root`
    InputRoot,
    /// The optional input-verification receipt passed to the trainer
    InputVerificationReceipt,
    /// The accepted task's effective working directory
    WorkingDirectory,
}

/// A validated argv and filesystem binding for `python -m ops.run_segment run`
///
/// This proves only that the accepted normalized specification and task row have
/// the maintained invocation shape and bind its paths to the saved canonical
/// runtime root. It does not prove which interpreter ran or that the live process
/// used the ownership lock. Release still requires exact task-exit evidence and
/// an exact held-lock guard through the release transaction
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectSegmentCommandShape {
    task_id: TaskId,
    python_executable: PathBuf,
    working_directory: PathBuf,
    task_file: PathBuf,
    input_root: PathBuf,
    runtime_root: PathBuf,
    image_argument: String,
    input_verification_receipt: Option<PathBuf>,
    resume_generation: Option<String>,
    timeout: Duration,
}

impl DirectSegmentCommandShape {
    /// Validate an accepted specification and task row against one saved attempt association
    ///
    /// The normalized-spec digest and task row must match the association's exact task. The
    /// returned shape proves invocation and path binding, not interpreter identity or live lock
    /// use. Do not use it alone to release a resource
    pub fn validate(
        spec: &NormalizedSpec,
        task: &TaskRow,
        association: &TrainerAttemptAssociation,
    ) -> Result<Self, DirectSegmentCommandShapeError> {
        Self::validate_binding(
            spec,
            task,
            association.task_id(),
            association.normalized_spec_sha256(),
            association.verified_attempt(),
        )
    }

    /// Validate an accepted specification and task row before creating its association
    ///
    /// The task ID and normalized-spec digest must come from the accepted executor identity
    /// The verified attempt carries the exact runtime root that the command must name
    pub fn validate_binding(
        spec: &NormalizedSpec,
        task: &TaskRow,
        expected_task_id: TaskId,
        expected_spec_sha256: NormalizedSpecSha256,
        verified_attempt: &VerifiedTrainerAttempt,
    ) -> Result<Self, DirectSegmentCommandShapeError> {
        if task.id != expected_task_id {
            return Err(DirectSegmentCommandShapeError::TaskIdentityMismatch {
                expected: expected_task_id,
                found: task.id,
            });
        }

        let spec_digest = normalized_spec_sha256(spec)
            .map_err(DirectSegmentCommandShapeError::NormalizedSpecEncoding)?;
        if spec_digest != expected_spec_sha256 {
            return Err(
                DirectSegmentCommandShapeError::NormalizedSpecDigestMismatch { task_id: task.id },
            );
        }

        let NormalizedWorkload::Task(spec_workload) = &spec.workload else {
            return Err(DirectSegmentCommandShapeError::NotCommandTask { task_id: task.id });
        };
        if task.name.as_ref() != Some(&spec.name)
            || task.thread != spec.thread
            || task.timeout != spec.timeout
            || task.workload != crate::invocation::persist_workload(&spec.workload)
        {
            return Err(DirectSegmentCommandShapeError::TaskRowMismatch { task_id: task.id });
        }

        let working_directory = accepted_working_directory(spec, task)?;
        check_trainer_layout(&working_directory)?;

        if !task.binary.is_absolute() {
            return Err(DirectSegmentCommandShapeError::ExecutableNotAbsolute {
                path: task.binary.clone(),
            });
        }
        let resolved_executable = resolve_executable(
            spec_workload.command.program(),
            &task.env.path,
            &working_directory,
        )
        .map_err(DirectSegmentCommandShapeError::ExecutableResolution)?;
        if resolved_executable != task.binary {
            return Err(DirectSegmentCommandShapeError::ExecutableMismatch {
                resolved: resolved_executable,
                accepted: task.binary.clone(),
            });
        }
        if !is_python_executable(&task.binary) {
            return Err(DirectSegmentCommandShapeError::NotPythonExecutable {
                path: task.binary.clone(),
            });
        }

        let command = &spec_workload.command;
        check_module_invocation(command)?;
        let parsed = parse_flags(command)?;

        let task_file_arg = required_flag(parsed.task_file, "--task")?;
        let task_file = canonical_file(DirectSegmentPathRole::TaskFile, Path::new(&task_file_arg))?;
        let input_root_arg = required_flag(parsed.input_root, "--input-root")?;
        let input_root =
            canonical_directory(DirectSegmentPathRole::InputRoot, Path::new(&input_root_arg))?;
        let runtime_root = PathBuf::from(required_flag(parsed.runtime_root, "--runtime-root")?);
        let associated_runtime_root = verified_attempt.canonical_runtime_root();
        if runtime_root.as_os_str() != associated_runtime_root.as_os_str() {
            return Err(DirectSegmentCommandShapeError::RuntimeRootMismatch {
                provided: runtime_root,
                expected: associated_runtime_root.to_path_buf(),
            });
        }
        let image_argument = required_flag(parsed.image_argument, "--image-digest")?.to_owned();
        let input_verification_receipt = parsed
            .input_verification_receipt
            .map(|path| {
                canonical_file(
                    DirectSegmentPathRole::InputVerificationReceipt,
                    Path::new(&path),
                )
            })
            .transpose()?;
        let timeout = parsed
            .timeout_seconds
            .map_or(Ok(DEFAULT_TIMEOUT), parse_timeout)?;

        Ok(Self {
            task_id: task.id,
            python_executable: task.binary.clone(),
            working_directory,
            task_file,
            input_root,
            runtime_root,
            image_argument,
            input_verification_receipt,
            resume_generation: parsed.resume_generation,
            timeout,
        })
    }

    /// Return the exact Homebased task identity bound by this shape
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Return the absolute executable path resolved and stored by the task row
    #[must_use]
    pub fn python_executable(&self) -> &Path {
        &self.python_executable
    }

    /// Return the canonical working directory used by the task row
    #[must_use]
    pub fn working_directory(&self) -> &Path {
        &self.working_directory
    }

    /// Return the canonical trainer task metadata file path
    #[must_use]
    pub fn task_file(&self) -> &Path {
        &self.task_file
    }

    /// Return the canonical prepared input root
    #[must_use]
    pub fn input_root(&self) -> &Path {
        &self.input_root
    }

    /// Return the exact saved canonical runtime root passed to the command
    #[must_use]
    pub fn runtime_root(&self) -> &Path {
        &self.runtime_root
    }

    /// Return the opaque `--image-digest` argument without scientific validation
    #[must_use]
    pub fn image_argument(&self) -> &str {
        &self.image_argument
    }

    /// Return the canonical input-verification receipt path, when supplied
    #[must_use]
    pub fn input_verification_receipt(&self) -> Option<&Path> {
        self.input_verification_receipt.as_deref()
    }

    /// Return the optional resume generation argument without checkpoint validation
    #[must_use]
    pub fn resume_generation(&self) -> Option<&str> {
        self.resume_generation.as_deref()
    }

    /// Return the parsed timeout, including the trainer's default when omitted
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// A typed reason that an accepted task does not match the direct-segment invocation contract
#[derive(Debug, Error)]
pub enum DirectSegmentCommandShapeError {
    /// The accepted task row has another task identity than its trainer-attempt binding
    #[error("binding task {expected} does not match row task {found}")]
    TaskIdentityMismatch {
        /// Exact task in the supplied trainer-attempt binding
        expected: TaskId,
        /// Exact task in the supplied row
        found: TaskId,
    },
    /// The normalized specification cannot be serialized for digest comparison
    #[error("cannot encode normalized trainer specification: {0}")]
    NormalizedSpecEncoding(#[source] serde_json::Error),
    /// The normalized specification differs from the accepted task digest
    #[error("normalized trainer specification differs from task {task_id} binding")]
    NormalizedSpecDigestMismatch {
        /// Exact task whose accepted specification was checked
        task_id: TaskId,
    },
    /// The accepted specification is not a command task
    #[error("trainer task {task_id} is not a command workload")]
    NotCommandTask {
        /// Exact task whose workload was checked
        task_id: TaskId,
    },
    /// The accepted task row does not match the normalized command workload
    #[error("task row {task_id} differs from its normalized command specification")]
    TaskRowMismatch {
        /// Exact task whose row was checked
        task_id: TaskId,
    },
    /// A working directory path is not absolute
    #[error("{role:?} path is not absolute: {path}")]
    PathNotAbsolute {
        /// Path role being checked
        role: DirectSegmentPathRole,
        /// Path supplied by the accepted task or command
        path: PathBuf,
    },
    /// An accepted remote working directory cannot be bound to the task row
    #[error("accepted working directory {accepted} does not match task row directory {row}")]
    WorkingDirectoryMismatch {
        /// Directory resolved from the normalized specification
        accepted: PathBuf,
        /// Directory saved on the accepted task row
        row: PathBuf,
    },
    /// A path cannot be inspected
    #[error("cannot inspect {role:?} path {path}: {source}")]
    PathInspection {
        /// Path role being checked
        role: DirectSegmentPathRole,
        /// Path that could not be inspected
        path: PathBuf,
        /// Filesystem error
        #[source]
        source: io::Error,
    },
    /// A path is not its exact canonical absolute spelling
    #[error("{role:?} path is not canonical: supplied {path}, canonical {canonical}")]
    PathNotCanonical {
        /// Path role being checked
        role: DirectSegmentPathRole,
        /// Path supplied by the accepted task or command
        path: PathBuf,
        /// Canonical path observed on disk
        canonical: PathBuf,
    },
    /// A checked path has the wrong filesystem type
    #[error("{role:?} path {path} is not a {expected}")]
    PathTypeMismatch {
        /// Path role being checked
        role: DirectSegmentPathRole,
        /// Path whose type was checked
        path: PathBuf,
        /// Required filesystem type
        expected: &'static str,
    },
    /// The maintained trainer files or their `ops` directory are missing or unsafe
    #[error("maintained trainer path is missing or unsafe: {path}")]
    TrainerLayoutInvalid {
        /// Trainer path that failed the layout check
        path: PathBuf,
    },
    /// The task row has no absolute resolved executable path
    #[error("resolved task executable is not absolute: {path}")]
    ExecutableNotAbsolute {
        /// Executable path saved on the task row
        path: PathBuf,
    },
    /// The command's program cannot be resolved with the accepted task environment
    #[error("cannot resolve accepted task executable: {0}")]
    ExecutableResolution(#[source] crate::error::AppError),
    /// The resolved command program differs from the executable saved on the task row
    #[error("command resolves to {resolved}, but task row records {accepted}")]
    ExecutableMismatch {
        /// Executable resolved from the command, environment, and working directory
        resolved: PathBuf,
        /// Executable saved on the task row
        accepted: PathBuf,
    },
    /// The task row executable does not have a Python executable name
    #[error("resolved executable is not a Python executable: {path}")]
    NotPythonExecutable {
        /// Executable path saved on the task row
        path: PathBuf,
    },
    /// Python argv does not have the exact module and `run` prefix
    #[error("Python argv position {position} must be {expected:?}, found {found:?}")]
    InvalidModuleInvocation {
        /// Position in argv after the executable
        position: usize,
        /// Required argument at that position
        expected: &'static str,
        /// Supplied argument at that position, when present
        found: Option<String>,
    },
    /// A token is a positional argument or an unsupported short option
    #[error("unexpected positional or short-option argument: {value}")]
    UnexpectedArgument {
        /// Unsupported argv token
        value: String,
    },
    /// A command flag is not one of the exact maintained trainer flags
    #[error("unknown direct-segment flag: {flag}")]
    UnknownFlag {
        /// Unknown flag spelling
        flag: String,
    },
    /// One exact flag appears more than once
    #[error("direct-segment flag appears more than once: {flag}")]
    DuplicateFlag {
        /// Repeated flag spelling
        flag: &'static str,
    },
    /// A flag has no value or has an empty inline value
    #[error("direct-segment flag has no value: {flag}")]
    MissingFlagValue {
        /// Flag that requires a value
        flag: &'static str,
    },
    /// A flag value starts with a dash and could be parsed as another option
    #[error("ambiguous value for direct-segment flag {flag}: {value}")]
    AmbiguousFlagValue {
        /// Flag whose value is ambiguous
        flag: &'static str,
        /// Ambiguous value
        value: String,
    },
    /// A required direct-segment flag is absent
    #[error("required direct-segment flag is missing: {flag}")]
    MissingRequiredFlag {
        /// Missing flag spelling
        flag: &'static str,
    },
    /// The command runtime root differs from the verified attempt's canonical runtime root
    #[error("runtime root {provided} does not match verified attempt root {expected}")]
    RuntimeRootMismatch {
        /// Runtime path passed in command argv
        provided: PathBuf,
        /// Canonical runtime path saved in the verified attempt
        expected: PathBuf,
    },
    /// The timeout is not a finite positive value within the trainer's seven-day limit
    #[error("invalid direct-segment timeout in seconds: {value}")]
    InvalidTimeout {
        /// Raw timeout argument
        value: String,
    },
}

fn accepted_working_directory(
    spec: &NormalizedSpec,
    task: &TaskRow,
) -> Result<PathBuf, DirectSegmentCommandShapeError> {
    let accepted = if spec.cwd.is_absolute() {
        spec.cwd.clone()
    } else if spec.machine.is_some() && spec.cwd.to_str().is_some_and(|path| path.starts_with("~/"))
    {
        let home = PathBuf::from(&task.env.home);
        if !home.is_absolute() {
            return Err(DirectSegmentCommandShapeError::PathNotAbsolute {
                role: DirectSegmentPathRole::WorkingDirectory,
                path: home,
            });
        }
        let relative = spec.cwd.strip_prefix(Path::new("~/")).map_err(|_| {
            DirectSegmentCommandShapeError::PathNotAbsolute {
                role: DirectSegmentPathRole::WorkingDirectory,
                path: spec.cwd.clone(),
            }
        })?;
        if relative.is_absolute()
            || relative
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(DirectSegmentCommandShapeError::PathNotAbsolute {
                role: DirectSegmentPathRole::WorkingDirectory,
                path: spec.cwd.clone(),
            });
        }
        home.join(relative)
    } else {
        return Err(DirectSegmentCommandShapeError::PathNotAbsolute {
            role: DirectSegmentPathRole::WorkingDirectory,
            path: spec.cwd.clone(),
        });
    };

    if accepted.as_os_str() != task.cwd.as_os_str() {
        return Err(DirectSegmentCommandShapeError::WorkingDirectoryMismatch {
            accepted,
            row: task.cwd.clone(),
        });
    }

    canonical_directory(DirectSegmentPathRole::WorkingDirectory, &task.cwd)
}

fn check_trainer_layout(cwd: &Path) -> Result<(), DirectSegmentCommandShapeError> {
    let ops = cwd.join("ops");
    let ops_metadata = fs::symlink_metadata(&ops)
        .map_err(|_| DirectSegmentCommandShapeError::TrainerLayoutInvalid { path: ops.clone() })?;
    if !ops_metadata.file_type().is_dir() {
        return Err(DirectSegmentCommandShapeError::TrainerLayoutInvalid { path: ops });
    }

    for file in ["run_segment.py", "segment_artifacts.py"] {
        let path = cwd.join("ops").join(file);
        let metadata = fs::symlink_metadata(&path).map_err(|_| {
            DirectSegmentCommandShapeError::TrainerLayoutInvalid { path: path.clone() }
        })?;
        if !metadata.file_type().is_file() {
            return Err(DirectSegmentCommandShapeError::TrainerLayoutInvalid { path });
        }
    }

    Ok(())
}

fn canonical_directory(
    role: DirectSegmentPathRole,
    path: &Path,
) -> Result<PathBuf, DirectSegmentCommandShapeError> {
    let canonical = canonical_path(role, path)?;
    let metadata = fs::symlink_metadata(path).map_err(|source| {
        DirectSegmentCommandShapeError::PathInspection {
            role,
            path: path.to_path_buf(),
            source,
        }
    })?;
    if !metadata.file_type().is_dir() {
        return Err(DirectSegmentCommandShapeError::PathTypeMismatch {
            role,
            path: path.to_path_buf(),
            expected: "directory",
        });
    }
    Ok(canonical)
}

fn canonical_file(
    role: DirectSegmentPathRole,
    path: &Path,
) -> Result<PathBuf, DirectSegmentCommandShapeError> {
    let canonical = canonical_path(role, path)?;
    let metadata = fs::symlink_metadata(path).map_err(|source| {
        DirectSegmentCommandShapeError::PathInspection {
            role,
            path: path.to_path_buf(),
            source,
        }
    })?;
    if !metadata.file_type().is_file() {
        return Err(DirectSegmentCommandShapeError::PathTypeMismatch {
            role,
            path: path.to_path_buf(),
            expected: "regular file",
        });
    }
    Ok(canonical)
}

fn canonical_path(
    role: DirectSegmentPathRole,
    path: &Path,
) -> Result<PathBuf, DirectSegmentCommandShapeError> {
    if !path.is_absolute() {
        return Err(DirectSegmentCommandShapeError::PathNotAbsolute {
            role,
            path: path.to_path_buf(),
        });
    }
    let canonical = fs::canonicalize(path).map_err(|source| {
        DirectSegmentCommandShapeError::PathInspection {
            role,
            path: path.to_path_buf(),
            source,
        }
    })?;
    if canonical.as_os_str() != path.as_os_str() {
        return Err(DirectSegmentCommandShapeError::PathNotCanonical {
            role,
            path: path.to_path_buf(),
            canonical,
        });
    }
    Ok(canonical)
}

fn is_python_executable(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    if name == "python" {
        return true;
    }

    let Some(version) = name.strip_prefix("python") else {
        return false;
    };
    let mut has_digit = false;
    let mut has_digit_after_dot = true;
    for character in version.chars() {
        match character {
            '0'..='9' => {
                has_digit = true;
                has_digit_after_dot = true;
            }
            '.' if has_digit && has_digit_after_dot => has_digit_after_dot = false,
            _ => return false,
        }
    }
    has_digit && has_digit_after_dot
}

fn check_module_invocation(command: &CommandLine) -> Result<(), DirectSegmentCommandShapeError> {
    for (position, expected) in REQUIRED_MODULE_ARGS.iter().enumerate() {
        let found = command.args().get(position).cloned();
        if found.as_deref() != Some(expected) {
            return Err(DirectSegmentCommandShapeError::InvalidModuleInvocation {
                position,
                expected,
                found,
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum DirectSegmentFlag {
    TaskFile,
    InputRoot,
    RuntimeRoot,
    ImageArgument,
    InputVerificationReceipt,
    ResumeGeneration,
    TimeoutSeconds,
}

impl DirectSegmentFlag {
    const fn name(self) -> &'static str {
        match self {
            Self::TaskFile => "--task",
            Self::InputRoot => "--input-root",
            Self::RuntimeRoot => "--runtime-root",
            Self::ImageArgument => "--image-digest",
            Self::InputVerificationReceipt => "--input-verification-receipt",
            Self::ResumeGeneration => "--resume",
            Self::TimeoutSeconds => "--timeout-seconds",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        match name {
            "--task" => Some(Self::TaskFile),
            "--input-root" => Some(Self::InputRoot),
            "--runtime-root" => Some(Self::RuntimeRoot),
            "--image-digest" => Some(Self::ImageArgument),
            "--input-verification-receipt" => Some(Self::InputVerificationReceipt),
            "--resume" => Some(Self::ResumeGeneration),
            "--timeout-seconds" => Some(Self::TimeoutSeconds),
            _ => None,
        }
    }
}

#[derive(Default)]
struct ParsedFlags {
    task_file: Option<String>,
    input_root: Option<String>,
    runtime_root: Option<String>,
    image_argument: Option<String>,
    input_verification_receipt: Option<String>,
    resume_generation: Option<String>,
    timeout_seconds: Option<String>,
}

impl ParsedFlags {
    fn contains(&self, flag: DirectSegmentFlag) -> bool {
        self.value(flag).is_some()
    }

    fn value(&self, flag: DirectSegmentFlag) -> Option<&String> {
        match flag {
            DirectSegmentFlag::TaskFile => self.task_file.as_ref(),
            DirectSegmentFlag::InputRoot => self.input_root.as_ref(),
            DirectSegmentFlag::RuntimeRoot => self.runtime_root.as_ref(),
            DirectSegmentFlag::ImageArgument => self.image_argument.as_ref(),
            DirectSegmentFlag::InputVerificationReceipt => self.input_verification_receipt.as_ref(),
            DirectSegmentFlag::ResumeGeneration => self.resume_generation.as_ref(),
            DirectSegmentFlag::TimeoutSeconds => self.timeout_seconds.as_ref(),
        }
    }

    fn set(&mut self, flag: DirectSegmentFlag, value: String) {
        match flag {
            DirectSegmentFlag::TaskFile => self.task_file = Some(value),
            DirectSegmentFlag::InputRoot => self.input_root = Some(value),
            DirectSegmentFlag::RuntimeRoot => self.runtime_root = Some(value),
            DirectSegmentFlag::ImageArgument => self.image_argument = Some(value),
            DirectSegmentFlag::InputVerificationReceipt => {
                self.input_verification_receipt = Some(value);
            }
            DirectSegmentFlag::ResumeGeneration => self.resume_generation = Some(value),
            DirectSegmentFlag::TimeoutSeconds => self.timeout_seconds = Some(value),
        }
    }
}

fn parse_flags(command: &CommandLine) -> Result<ParsedFlags, DirectSegmentCommandShapeError> {
    let args = command.args();
    let mut parsed = ParsedFlags::default();
    let mut index = REQUIRED_MODULE_ARGS.len();

    while index < args.len() {
        let argument = &args[index];
        let Some(option) = argument.strip_prefix("--") else {
            return Err(DirectSegmentCommandShapeError::UnexpectedArgument {
                value: argument.clone(),
            });
        };
        let (name, inline_value) = option
            .split_once('=')
            .map_or((option, None), |(name, value)| (name, Some(value)));
        let flag = DirectSegmentFlag::parse(&format!("--{name}")).ok_or_else(|| {
            DirectSegmentCommandShapeError::UnknownFlag {
                flag: if name.is_empty() {
                    "--".into()
                } else {
                    format!("--{name}")
                },
            }
        })?;

        if parsed.contains(flag) {
            return Err(DirectSegmentCommandShapeError::DuplicateFlag { flag: flag.name() });
        }

        let value = match inline_value {
            Some("") => {
                return Err(DirectSegmentCommandShapeError::MissingFlagValue { flag: flag.name() });
            }
            Some(value) => value.to_owned(),
            None => {
                let Some(value) = args.get(index + 1) else {
                    return Err(DirectSegmentCommandShapeError::MissingFlagValue {
                        flag: flag.name(),
                    });
                };
                if value.starts_with('-') {
                    return Err(DirectSegmentCommandShapeError::AmbiguousFlagValue {
                        flag: flag.name(),
                        value: value.clone(),
                    });
                }
                index += 1;
                value.clone()
            }
        };
        if value.is_empty() {
            return Err(DirectSegmentCommandShapeError::MissingFlagValue { flag: flag.name() });
        }
        if value.starts_with('-') {
            return Err(DirectSegmentCommandShapeError::AmbiguousFlagValue {
                flag: flag.name(),
                value,
            });
        }

        parsed.set(flag, value);
        index += 1;
    }

    Ok(parsed)
}

fn required_flag(
    value: Option<String>,
    flag: &'static str,
) -> Result<String, DirectSegmentCommandShapeError> {
    value.ok_or(DirectSegmentCommandShapeError::MissingRequiredFlag { flag })
}

fn parse_timeout(value: String) -> Result<Duration, DirectSegmentCommandShapeError> {
    let seconds = value
        .parse::<f64>()
        .ok()
        .filter(|seconds| seconds.is_finite() && 0.0 < *seconds && *seconds <= MAX_TIMEOUT_SECONDS)
        .ok_or_else(|| DirectSegmentCommandShapeError::InvalidTimeout {
            value: value.clone(),
        })?;
    Duration::try_from_secs_f64(seconds)
        .map_err(|_| DirectSegmentCommandShapeError::InvalidTimeout { value })
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::OpenOptionsExt;
    use std::path::PathBuf;

    use super::*;
    use crate::domain::{AgentKind, TaskEnv, TaskId, TaskName, TaskWorkload, Workload};
    use crate::machine::{MachineId, MachineName};
    use crate::resource::ownership_lock::{
        OwnershipLockIdentity, TrainerRequestDigest, VerifiedTrainerAttempt,
    };
    use crate::resource::watcher::AttemptBinding;
    use crate::resource::{ResourceId, TrainerAttemptAssociation};
    use crate::store::{NewTask, new_queued_task};
    use crate::submission::normalized_spec_sha256;
    use tempfile::{TempDir, tempdir};
    use uuid::Uuid;

    struct Fixture {
        _directory: TempDir,
        root: PathBuf,
        spec: NormalizedSpec,
        task: TaskRow,
        association: TrainerAttemptAssociation,
        trainer_root: PathBuf,
        runtime_root: PathBuf,
        task_file: PathBuf,
        input_root: PathBuf,
        python: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempdir().unwrap();
            let root = directory.path().canonicalize().unwrap();
            let trainer_root = root.join("trainer");
            let ops = trainer_root.join("ops");
            fs::create_dir_all(&ops).unwrap();
            fs::write(ops.join("run_segment.py"), b"# maintained runner\n").unwrap();
            fs::write(
                ops.join("segment_artifacts.py"),
                b"# maintained artifacts\n",
            )
            .unwrap();

            let runtime_root = root.join("runtime");
            fs::create_dir(&runtime_root).unwrap();
            let task_file = root.join("task.json");
            fs::write(&task_file, b"{}\n").unwrap();
            let input_root = root.join("inputs");
            fs::create_dir(&input_root).unwrap();

            let bin = root.join("bin");
            fs::create_dir(&bin).unwrap();
            let python = bin.join("python3");
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o755)
                .open(&python)
                .unwrap();

            let command = vec![
                "python3".into(),
                "-m".into(),
                "ops.run_segment".into(),
                "run".into(),
                "--task".into(),
                task_file.to_string_lossy().into_owned(),
                "--input-root".into(),
                input_root.to_string_lossy().into_owned(),
                "--runtime-root".into(),
                runtime_root.to_string_lossy().into_owned(),
                "--image-digest".into(),
                "not-validated-by-shape".into(),
            ];
            let spec: NormalizedSpec = serde_json::from_value(serde_json::json!({
                "api_version": 1,
                "thread": Uuid::now_v7(),
                "name": "direct segment trainer",
                "cwd": trainer_root,
                "machine": null,
                "timeout": "4h",
                "workload": { "type": "task", "command": command }
            }))
            .unwrap();
            let task_id = TaskId::new();
            let row = task_row(task_id, &spec, &trainer_root, &python, &bin);
            let association = association(task_id, &spec, &runtime_root);

            Self {
                _directory: directory,
                root,
                spec,
                task: row,
                association,
                trainer_root,
                runtime_root,
                task_file,
                input_root,
                python,
            }
        }

        fn set_command(&mut self, command: Vec<String>) {
            let NormalizedWorkload::Task(workload) = &mut self.spec.workload else {
                panic!("fixture workload must be a task");
            };
            workload.command = CommandLine::try_from_argv(command).unwrap();
            self.task.workload = crate::invocation::persist_workload(&self.spec.workload);
            self.association = association(self.task.id, &self.spec, &self.runtime_root);
        }

        fn argv(&self) -> Vec<String> {
            let NormalizedWorkload::Task(workload) = &self.spec.workload else {
                panic!("fixture workload must be a task");
            };
            workload.command.to_vec()
        }

        fn set_remote_cwd(&mut self) {
            self.spec.cwd = PathBuf::from("~/trainer");
            self.spec.machine = Some(MachineName::parse("remote").unwrap());
            self.task.cwd = self.trainer_root.clone();
            self.task.env.home = self.root.to_string_lossy().into_owned();
            self.association = association(self.task.id, &self.spec, &self.runtime_root);
        }
    }

    fn task_row(
        task_id: TaskId,
        spec: &NormalizedSpec,
        cwd: &Path,
        python: &Path,
        bin: &Path,
    ) -> TaskRow {
        let NormalizedWorkload::Task(workload) = &spec.workload else {
            panic!("fixture workload must be a task");
        };
        new_queued_task(NewTask {
            id: task_id,
            name: Some(spec.name.clone()),
            thread: spec.thread,
            workload: Workload::Task(TaskWorkload {
                command: workload.command.clone(),
            }),
            cwd: cwd.to_path_buf(),
            timeout: spec.timeout,
            env: TaskEnv {
                path: bin.to_string_lossy().into_owned(),
                home: cwd.parent().unwrap().to_string_lossy().into_owned(),
            },
            binary: python.to_path_buf(),
        })
    }

    fn association(
        task_id: TaskId,
        spec: &NormalizedSpec,
        runtime_root: &Path,
    ) -> TrainerAttemptAssociation {
        let digest = TrainerRequestDigest::from_hex(&"00".repeat(32)).unwrap();
        let verified = VerifiedTrainerAttempt::from_persisted(
            runtime_root.to_path_buf(),
            AttemptBinding {
                campaign_id: "campaign-1".into(),
                campaign_revision_id: "revision-1".into(),
                task_id: "trainer-task-1".into(),
                attempt_id: "attempt-1".into(),
                attempt_number: 1,
                ownership_token: "token-1".into(),
            },
            digest,
            OwnershipLockIdentity::new(1, 2),
        )
        .unwrap();
        TrainerAttemptAssociation::from_components(
            ResourceId::new(),
            MachineId::new(),
            task_id,
            verified,
            normalized_spec_sha256(spec).unwrap(),
        )
        .unwrap()
    }

    fn append_options(fixture: &Fixture, options: &[&str]) -> Vec<String> {
        let mut argv = fixture.argv();
        argv.extend(options.iter().map(|value| (*value).to_owned()));
        argv
    }

    #[test]
    fn validates_local_accepted_task_shape_and_keeps_image_opaque() {
        let fixture = Fixture::new();

        let shape =
            DirectSegmentCommandShape::validate(&fixture.spec, &fixture.task, &fixture.association)
                .unwrap();

        assert_eq!(shape.task_id(), fixture.task.id);
        assert_eq!(shape.python_executable(), fixture.python);
        assert_eq!(shape.working_directory(), fixture.trainer_root);
        assert_eq!(shape.task_file(), fixture.task_file);
        assert_eq!(shape.input_root(), fixture.input_root);
        assert_eq!(shape.runtime_root(), fixture.runtime_root);
        assert_eq!(shape.image_argument(), "not-validated-by-shape");
        assert_eq!(shape.timeout(), DEFAULT_TIMEOUT);
        assert_eq!(shape.input_verification_receipt(), None);
        assert_eq!(shape.resume_generation(), None);
    }

    #[test]
    fn validates_remote_accepted_task_with_executor_home_cwd_expansion() {
        let mut fixture = Fixture::new();
        fixture.set_remote_cwd();

        let shape =
            DirectSegmentCommandShape::validate(&fixture.spec, &fixture.task, &fixture.association)
                .unwrap();

        assert_eq!(shape.working_directory(), fixture.trainer_root);
    }

    #[test]
    fn accepts_exact_flags_in_either_value_form_and_preserves_optional_values() {
        let mut fixture = Fixture::new();
        let receipt = fixture.root.join("receipt.json");
        fs::write(&receipt, b"{}\n").unwrap();
        let mut argv = vec![
            "python3".into(),
            "-m".into(),
            "ops.run_segment".into(),
            "run".into(),
            format!("--task={}", fixture.task_file.display()),
            "--input-root".into(),
            fixture.input_root.to_string_lossy().into_owned(),
            "--runtime-root".into(),
            fixture.runtime_root.to_string_lossy().into_owned(),
            "--image-digest=opaque".into(),
            "--resume".into(),
            "generation-1".into(),
            "--input-verification-receipt".into(),
            receipt.to_string_lossy().into_owned(),
            "--timeout-seconds=120.5".into(),
        ];
        argv.shrink_to_fit();
        fixture.set_command(argv);

        let shape =
            DirectSegmentCommandShape::validate(&fixture.spec, &fixture.task, &fixture.association)
                .unwrap();

        assert_eq!(shape.image_argument(), "opaque");
        assert_eq!(shape.resume_generation(), Some("generation-1"));
        assert_eq!(shape.input_verification_receipt(), Some(receipt.as_path()));
        assert_eq!(shape.timeout(), Duration::from_millis(120_500));
    }

    #[test]
    fn rejects_shell_and_wrapper_executables() {
        let mut fixture = Fixture::new();
        let shell = fixture.root.join("bin/bash");
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o755)
            .open(&shell)
            .unwrap();
        let mut argv = fixture.argv();
        argv[0] = "bash".into();
        argv.splice(
            1..4,
            ["-lc".into(), "python3 -m ops.run_segment run".into()],
        );
        fixture.set_command(argv);
        fixture.task.binary = shell;

        assert!(matches!(
            DirectSegmentCommandShape::validate(&fixture.spec, &fixture.task, &fixture.association,),
            Err(DirectSegmentCommandShapeError::NotPythonExecutable { .. })
        ));
    }

    #[test]
    fn rejects_python_c_invocation() {
        let mut fixture = Fixture::new();
        let mut argv = fixture.argv();
        argv.splice(1.., ["-c".into(), "print(1)".into()]);
        fixture.set_command(argv);

        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &fixture.spec,
                &fixture.task,
                &fixture.association,
            ),
            Err(DirectSegmentCommandShapeError::InvalidModuleInvocation {
                position: 0,
                expected: "-m",
                found: Some(found),
            }) if found == "-c"
        ));
    }

    #[test]
    fn rejects_import_subcommand() {
        let mut fixture = Fixture::new();
        let mut argv = fixture.argv();
        argv[3] = "import".into();
        fixture.set_command(argv);

        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &fixture.spec,
                &fixture.task,
                &fixture.association,
            ),
            Err(DirectSegmentCommandShapeError::InvalidModuleInvocation {
                position: 2,
                expected: "run",
                found: Some(found),
            }) if found == "import"
        ));
    }

    #[test]
    fn rejects_unknown_duplicate_missing_and_positional_arguments() {
        let mut unknown = Fixture::new();
        let argv = append_options(&unknown, &["--unknown", "value"]);
        unknown.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(&unknown.spec, &unknown.task, &unknown.association,),
            Err(DirectSegmentCommandShapeError::UnknownFlag { .. })
        ));

        let mut duplicate = Fixture::new();
        let argv = append_options(&duplicate, &["--task", "again"]);
        duplicate.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &duplicate.spec,
                &duplicate.task,
                &duplicate.association,
            ),
            Err(DirectSegmentCommandShapeError::DuplicateFlag { flag: "--task" })
        ));

        let mut missing = Fixture::new();
        let mut argv = missing.argv();
        let runtime_index = argv.iter().position(|arg| arg == "--runtime-root").unwrap();
        argv.drain(runtime_index..=runtime_index + 1);
        missing.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(&missing.spec, &missing.task, &missing.association,),
            Err(DirectSegmentCommandShapeError::MissingRequiredFlag {
                flag: "--runtime-root"
            })
        ));

        let mut missing_value = Fixture::new();
        let argv = append_options(&missing_value, &["--resume"]);
        missing_value.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &missing_value.spec,
                &missing_value.task,
                &missing_value.association,
            ),
            Err(DirectSegmentCommandShapeError::MissingFlagValue { flag: "--resume" })
        ));

        let mut positional = Fixture::new();
        let argv = append_options(&positional, &["free-argument"]);
        positional.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &positional.spec,
                &positional.task,
                &positional.association,
            ),
            Err(DirectSegmentCommandShapeError::UnexpectedArgument { .. })
        ));
    }

    #[test]
    fn rejects_ambiguous_flag_values() {
        let mut fixture = Fixture::new();
        let argv = append_options(&fixture, &["--resume", "--timeout-seconds", "4"]);
        fixture.set_command(argv);

        assert!(matches!(
            DirectSegmentCommandShape::validate(&fixture.spec, &fixture.task, &fixture.association,),
            Err(DirectSegmentCommandShapeError::AmbiguousFlagValue {
                flag: "--resume",
                ..
            })
        ));
    }

    #[test]
    fn rejects_runtime_root_mismatch() {
        let mut fixture = Fixture::new();
        let other_runtime = fixture.root.join("other-runtime");
        fs::create_dir(&other_runtime).unwrap();
        let mut argv = fixture.argv();
        let runtime_index = argv.iter().position(|arg| arg == "--runtime-root").unwrap();
        argv[runtime_index + 1] = other_runtime.to_string_lossy().into_owned();
        fixture.set_command(argv);

        assert!(matches!(
            DirectSegmentCommandShape::validate(&fixture.spec, &fixture.task, &fixture.association,),
            Err(DirectSegmentCommandShapeError::RuntimeRootMismatch { .. })
        ));

        let mut noncanonical = Fixture::new();
        let mut argv = noncanonical.argv();
        let runtime_index = argv.iter().position(|arg| arg == "--runtime-root").unwrap();
        argv[runtime_index + 1] = format!("{}/", noncanonical.runtime_root.display());
        noncanonical.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &noncanonical.spec,
                &noncanonical.task,
                &noncanonical.association,
            ),
            Err(DirectSegmentCommandShapeError::RuntimeRootMismatch { .. })
        ));
    }

    #[test]
    fn rejects_relative_and_noncanonical_required_paths() {
        let mut relative = Fixture::new();
        let mut argv = relative.argv();
        let task_index = argv.iter().position(|arg| arg == "--task").unwrap();
        argv[task_index + 1] = "task.json".into();
        relative.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &relative.spec,
                &relative.task,
                &relative.association,
            ),
            Err(DirectSegmentCommandShapeError::PathNotAbsolute {
                role: DirectSegmentPathRole::TaskFile,
                ..
            })
        ));

        let mut noncanonical = Fixture::new();
        fs::create_dir(noncanonical.root.join("nested")).unwrap();
        let aliased_task = noncanonical.root.join("nested/../task.json");
        let mut argv = noncanonical.argv();
        let task_index = argv.iter().position(|arg| arg == "--task").unwrap();
        argv[task_index + 1] = aliased_task.to_string_lossy().into_owned();
        noncanonical.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &noncanonical.spec,
                &noncanonical.task,
                &noncanonical.association,
            ),
            Err(DirectSegmentCommandShapeError::PathNotCanonical {
                role: DirectSegmentPathRole::TaskFile,
                ..
            })
        ));

        let mut relative_input = Fixture::new();
        let mut argv = relative_input.argv();
        let input_index = argv.iter().position(|arg| arg == "--input-root").unwrap();
        argv[input_index + 1] = "inputs".into();
        relative_input.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &relative_input.spec,
                &relative_input.task,
                &relative_input.association,
            ),
            Err(DirectSegmentCommandShapeError::PathNotAbsolute {
                role: DirectSegmentPathRole::InputRoot,
                ..
            })
        ));

        let mut noncanonical_input = Fixture::new();
        fs::create_dir(noncanonical_input.root.join("nested")).unwrap();
        let aliased_input = noncanonical_input.root.join("nested/../inputs");
        let mut argv = noncanonical_input.argv();
        let input_index = argv.iter().position(|arg| arg == "--input-root").unwrap();
        argv[input_index + 1] = aliased_input.to_string_lossy().into_owned();
        noncanonical_input.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &noncanonical_input.spec,
                &noncanonical_input.task,
                &noncanonical_input.association,
            ),
            Err(DirectSegmentCommandShapeError::PathNotCanonical {
                role: DirectSegmentPathRole::InputRoot,
                ..
            })
        ));
    }

    #[test]
    fn rejects_missing_trainer_file_and_symlinked_final_component() {
        let missing = Fixture::new();
        fs::remove_file(missing.trainer_root.join("ops/segment_artifacts.py")).unwrap();
        assert!(matches!(
            DirectSegmentCommandShape::validate(&missing.spec, &missing.task, &missing.association,),
            Err(DirectSegmentCommandShapeError::TrainerLayoutInvalid { .. })
        ));

        let symlinked = Fixture::new();
        let original = symlinked.trainer_root.join("ops/run_segment.py");
        fs::remove_file(&original).unwrap();
        std::os::unix::fs::symlink("segment_artifacts.py", &original).unwrap();
        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &symlinked.spec,
                &symlinked.task,
                &symlinked.association,
            ),
            Err(DirectSegmentCommandShapeError::TrainerLayoutInvalid { path })
                if path == original
        ));
    }

    #[test]
    fn rejects_non_python_resolved_executable_and_bad_timeout() {
        let mut wrapper = Fixture::new();
        let wrapper_path = wrapper.root.join("bin/env");
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o755)
            .open(&wrapper_path)
            .unwrap();
        let mut argv = wrapper.argv();
        argv[0] = "env".into();
        wrapper.set_command(argv);
        wrapper.task.binary = wrapper_path;
        assert!(matches!(
            DirectSegmentCommandShape::validate(&wrapper.spec, &wrapper.task, &wrapper.association,),
            Err(DirectSegmentCommandShapeError::NotPythonExecutable { .. })
        ));

        let mut bad_timeout = Fixture::new();
        let argv = append_options(&bad_timeout, &["--timeout-seconds", "604800.5"]);
        bad_timeout.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(
                &bad_timeout.spec,
                &bad_timeout.task,
                &bad_timeout.association,
            ),
            Err(DirectSegmentCommandShapeError::InvalidTimeout { .. })
        ));
    }

    #[test]
    fn rejects_changed_normalized_spec_digest_and_task_row_identity() {
        let mut fixture = Fixture::new();
        fixture.spec.name = TaskName::parse("changed task").unwrap();
        assert!(matches!(
            DirectSegmentCommandShape::validate(&fixture.spec, &fixture.task, &fixture.association,),
            Err(DirectSegmentCommandShapeError::NormalizedSpecDigestMismatch { .. })
        ));

        let fixture = Fixture::new();
        let other_task = task_row(
            TaskId::new(),
            &fixture.spec,
            &fixture.trainer_root,
            &fixture.python,
            &fixture.root.join("bin"),
        );
        assert!(matches!(
            DirectSegmentCommandShape::validate(&fixture.spec, &other_task, &fixture.association,),
            Err(DirectSegmentCommandShapeError::TaskIdentityMismatch { .. })
        ));
    }

    #[test]
    fn rejects_non_command_workloads() {
        let mut fixture = Fixture::new();
        fixture.spec.workload = NormalizedWorkload::Agent(crate::spec::NormalizedAgentWorkload {
            agent: AgentKind::Codex,
            model: None,
            prompt: "not a task command".into(),
            extra_args: Vec::new(),
            report_trailer: true,
        });
        fixture.association = association(fixture.task.id, &fixture.spec, &fixture.runtime_root);

        assert!(matches!(
            DirectSegmentCommandShape::validate(&fixture.spec, &fixture.task, &fixture.association,),
            Err(DirectSegmentCommandShapeError::NotCommandTask { .. })
        ));
    }

    #[test]
    fn rejects_explicit_python_script_and_import_wrappers() {
        let mut fixture = Fixture::new();
        let mut argv = fixture.argv();
        argv.splice(1..4, ["ops/run_segment.py".into(), "run".into()]);
        fixture.set_command(argv);
        assert!(matches!(
            DirectSegmentCommandShape::validate(&fixture.spec, &fixture.task, &fixture.association,),
            Err(DirectSegmentCommandShapeError::InvalidModuleInvocation { .. })
        ));
    }
}
