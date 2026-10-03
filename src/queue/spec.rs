//! Job spec: the strict submit format of the GPU priority queue
//!
//! It is separate from the task spec, which stays unchanged. Schema and
//! structure are checked wherever a spec is parsed, including the origin.
//! The authority also checks what needs its file system at acceptance:
//! `cwd`, executables, mount sources, and the entry-point escape list

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::container::ContainerWorkload;
use crate::container::spec::container_workload_schema;
use crate::domain::{API_VERSION, MIN_TIMEOUT, TaskName, TaskWorkload, ThreadId};
use crate::error::AppError;
use crate::invocation::{CommandLine, resolve_executable};
use crate::machine::{MachineId, MachineName};
use crate::spec::{cwd_problem, default_timeout, escape_token, json_pointer};

use super::{Preemption, Priority, ResourceSelector, StepWorkload, Steps};

/// Container path under which the authority mounts the job and run directories
pub const RESERVED_MOUNT_ROOT: &str = "/homebased";

/// Prefix of the environment keys the authority sets for every run
pub const RESERVED_ENV_PREFIX: &str = "HOMEBASED_";

/// Entry points that hand work to a long-lived server outside the run, where
/// cleanup cannot attribute it; matched on the resolved executable's basename
pub const ESCAPE_ENTRY_POINTS: [&str; 9] = [
    "tmux",
    "screen",
    "launchctl",
    "systemd-run",
    "docker",
    "podman",
    "nerdctl",
    "ssh",
    "mosh",
];

/// The machine that owns the queue, by UUID or by fleet name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MachineSelector {
    /// Exact machine UUID
    Id(MachineId),
    /// Fleet machine name
    Name(MachineName),
}

impl FromStr for MachineSelector {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if let Ok(uuid) = Uuid::parse_str(raw) {
            return Ok(Self::Id(MachineId::from_uuid(uuid)));
        }
        MachineName::parse(raw)
            .map(Self::Name)
            .map_err(|error| error.to_string())
    }
}

impl fmt::Display for MachineSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Id(id) => id.as_uuid().fmt(f),
            Self::Name(name) => name.fmt(f),
        }
    }
}

impl Serialize for MachineSelector {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MachineSelector {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// A validated job spec
///
/// Its serialized form is canonical: steps are always a list, and every
/// default is written out, so a stored spec parses back to the same value and
/// its digest does not depend on how the submitter spelled it
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobSpec {
    /// Always 1
    api_version: u32,
    /// Thread that receives the job's events
    pub thread: ThreadId,
    /// Machine whose queue runs the job; the local machine when absent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub machine: Option<MachineSelector>,
    /// Human-readable name. Non-unique
    pub name: TaskName,
    /// Host directory on the authority where every step starts
    pub cwd: PathBuf,
    /// Inactivity reminder for each run; never stops the run
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// Serving level
    pub priority: Priority,
    /// How a run gives up its resource to higher-priority work
    pub preempt: Preemption,
    /// Resource the job is pinned to; any of the machine's resources when absent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<ResourceSelector>,
    /// What the job runs, in order
    pub steps: Steps,
}

impl JobSpec {
    /// Parse spec bytes, attaching a JSON pointer on failure
    pub fn parse_bytes(bytes: &[u8]) -> Result<Self, AppError> {
        let value: Value = serde_json::from_slice(bytes).map_err(|err| AppError::InvalidSpec {
            pointer: String::new(),
            value: Value::Null,
            message: format!("invalid JSON: {err}"),
        })?;
        Self::parse_value(&value)
    }

    /// Parse and check the schema and structure of a decoded spec
    pub fn parse_value(value: &Value) -> Result<Self, AppError> {
        if let Some(after) = value.get("after") {
            return Err(invalid(
                "/after",
                after.clone(),
                "job specs do not support after; submit the job once its inputs are ready",
            ));
        }
        let wire: JobSpecWire = deserialize_under(value, "")?;
        if wire.api_version != API_VERSION {
            return Err(invalid(
                "/api_version",
                json!(wire.api_version),
                format!("api_version must be {API_VERSION}"),
            ));
        }
        if wire.timeout < MIN_TIMEOUT {
            return Err(invalid(
                "/timeout",
                json!(humantime::format_duration(wire.timeout).to_string()),
                format!(
                    "timeout must be at least {}",
                    humantime::format_duration(MIN_TIMEOUT)
                ),
            ));
        }
        let preempt: Preemption = deserialize_under(&wire.preempt, "/preempt")?;
        let steps = match (&wire.workload, &wire.steps) {
            (Some(workload), None) => Steps::try_from(vec![parse_step(workload, "/workload")?])
                .map_err(|error| invalid("/workload", workload.clone(), error.to_string()))?,
            (None, Some(steps)) => parse_steps(steps)?,
            (Some(_), Some(_)) | (None, None) => {
                return Err(invalid(
                    "",
                    Value::Null,
                    "give exactly one of workload or steps",
                ));
            }
        };
        Ok(Self {
            api_version: API_VERSION,
            thread: wire.thread,
            machine: wire.machine,
            name: wire.name,
            cwd: wire.cwd,
            timeout: wire.timeout,
            priority: wire.priority,
            preempt,
            resource: wire.resource,
            steps,
        })
    }

    /// Canonical JSON, the stored form of the spec
    pub fn to_canonical_json(&self) -> Result<String, AppError> {
        Ok(serde_json::to_string(self)?)
    }

    /// SHA-256 of the canonical JSON, in lowercase hex
    ///
    /// A submit retry with the same job id must carry the same digest
    pub fn digest(&self) -> Result<String, AppError> {
        let digest = Sha256::digest(self.to_canonical_json()?.as_bytes());
        Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
    }

    /// JSON pointer of step `index` in the canonical form
    fn step_pointer(index: usize) -> String {
        format!("/steps/{index}")
    }

    /// Check what needs the authority's file system: `cwd`, each task step's
    /// executable against `env_path`, container mount sources, and the
    /// entry-point escape list
    ///
    /// Pointers follow the canonical form, where steps are always a list
    pub fn check_on_authority(&self, env_path: &str) -> Result<(), AppError> {
        if let Some(problem) = cwd_problem(&self.cwd) {
            return Err(AppError::InvalidCwd {
                path: self.cwd.clone(),
                problem,
                suggested_cwd: None,
            });
        }
        for (index, step) in self.steps.as_slice().iter().enumerate() {
            let pointer = Self::step_pointer(index);
            match step {
                StepWorkload::Task(task) => {
                    check_entry_point(&task.command, env_path, &self.cwd, &pointer)?;
                }
                StepWorkload::Container(container) => {
                    crate::container::check_container_host(container)
                        .map_err(|error| reprefix(error, &pointer))?;
                }
            }
        }
        Ok(())
    }
}

/// Wire shape before field rules. Unknown keys fail here
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "JobSpec")]
struct JobSpecWire {
    /// Must be 1
    #[schemars(schema_with = "api_version_schema")]
    api_version: u32,
    /// Thread that receives `JOB_*` events
    thread: ThreadId,
    /// Fleet machine name or UUID whose queue runs the job. Defaults to the
    /// local machine
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    machine: Option<MachineSelector>,
    /// Human-readable name. Non-unique
    name: TaskName,
    /// Existing host directory on the machine that runs the job. Every step
    /// starts here; per-step cwd is not supported
    cwd: PathBuf,
    /// Output-inactivity reminder for each run. Default 1h, minimum 30m.
    /// Never stops the run
    #[serde(default = "default_timeout", with = "humantime_serde")]
    #[schemars(schema_with = "timeout_schema")]
    timeout: Duration,
    /// Serving level by urgency: high when a decision or person is blocked on
    /// the result now, medium when it is needed soon, low when it can wait
    /// for an idle GPU
    priority: Priority,
    /// How a run gives up its GPU to higher-priority work
    #[schemars(schema_with = "preempt_schema")]
    preempt: Value,
    /// Resource name or UUID to pin the job to. Defaults to any of the
    /// machine's resources
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    resource: Option<ResourceSelector>,
    /// The job's one step. Exactly one of workload or steps
    #[serde(default)]
    #[schemars(schema_with = "step_schema")]
    workload: Option<Value>,
    /// 1 to 32 steps run in order. Exactly one of workload or steps
    #[serde(default)]
    #[schemars(schema_with = "steps_schema")]
    steps: Option<Value>,
}

fn api_version_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({ "type": "integer", "const": API_VERSION, "description": "Must be 1." })
}

fn timeout_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "string",
        "default": "1h",
        "description": "Output-inactivity reminder for each run as a humantime duration. Default 1h. Minimum 30m. Never stops the run."
    })
}

fn preempt_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let window = json!({
        "type": "string",
        "description": "Humantime duration from 1m through 24h. A run younger than this is killed and requeued instead."
    });
    schemars::json_schema!({
        "description": "How a run gives up its GPU to higher-priority work. Use wait only when the job cannot checkpoint.",
        "oneOf": [
            {
                "type": "object",
                "properties": { "mode": { "const": "restart" } },
                "required": ["mode"],
                "additionalProperties": false
            },
            {
                "type": "object",
                "properties": { "mode": { "const": "wait" }, "restart_within": window },
                "required": ["mode"],
                "additionalProperties": false
            },
            {
                "type": "object",
                "properties": { "mode": { "const": "yield" }, "restart_within": window },
                "required": ["mode"],
                "additionalProperties": false
            }
        ]
    })
}

fn step_value_schema(generator: &mut schemars::SchemaGenerator) -> Value {
    let command = <CommandLine as JsonSchema>::json_schema(generator)
        .as_value()
        .clone();
    let mut container = container_workload_schema();
    // the authority sets the GPU from the assigned resource
    if let Some(properties) = container
        .get_mut("properties")
        .and_then(Value::as_object_mut)
    {
        properties.remove("gpus");
    }
    json!({
        "oneOf": [
            {
                "title": "task",
                "description": "Arbitrary non-interactive command. No shell, no quoting.",
                "type": "object",
                "properties": { "type": { "const": "task" }, "command": command },
                "required": ["type", "command"],
                "additionalProperties": false
            },
            container
        ]
    })
}

fn step_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let mut step = step_value_schema(generator);
    if let Some(object) = step.as_object_mut() {
        object.insert(
            "description".into(),
            json!("One task or container step. Agents are refused."),
        );
    }
    schemars::Schema::try_from(step).unwrap_or_default()
}

fn steps_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let step = step_value_schema(generator);
    schemars::json_schema!({
        "type": "array",
        "minItems": 1,
        "maxItems": Steps::MAX,
        "items": step,
        "description": "Steps run in order; each step boundary is a checkpoint. Agents are refused."
    })
}

/// JSON Schema for the job spec, printed by `resource schema`
pub fn schema_json() -> Result<Value, AppError> {
    let mut schema = serde_json::to_value(schemars::schema_for!(JobSpecWire))?;
    if let Some(object) = schema.as_object_mut() {
        object.insert(
            "oneOf".into(),
            json!([{ "required": ["workload"] }, { "required": ["steps"] }]),
        );
    }
    Ok(schema)
}

fn parse_steps(steps: &Value) -> Result<Steps, AppError> {
    let Value::Array(list) = steps else {
        return Err(invalid("/steps", steps.clone(), "steps must be an array"));
    };
    let parsed = list
        .iter()
        .enumerate()
        .map(|(index, step)| parse_step(step, &JobSpec::step_pointer(index)))
        .collect::<Result<Vec<_>, _>>()?;
    Steps::try_from(parsed).map_err(|error| invalid("/steps", steps.clone(), error.to_string()))
}

/// Parse one step and check its structure; `prefix` is its JSON pointer
fn parse_step(step: &Value, prefix: &str) -> Result<StepWorkload, AppError> {
    let kind = step.get("type").and_then(Value::as_str);
    let mut content = step.clone();
    if let Value::Object(map) = &mut content {
        map.remove("type");
    }
    match kind {
        Some("task") => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct TaskWire {
                command: Vec<String>,
            }
            let task: TaskWire = deserialize_under(&content, prefix)?;
            let command = CommandLine::try_from_argv(task.command.clone()).map_err(|error| {
                invalid(
                    format!("{prefix}/command"),
                    json!(task.command),
                    error.to_string(),
                )
            })?;
            Ok(StepWorkload::Task(TaskWorkload { command }))
        }
        Some("container") => {
            let container = ContainerWorkload::from_value(&content)
                .map_err(|error| error.into_invalid_spec(prefix))?;
            check_container_structure(&container, prefix)?;
            Ok(StepWorkload::Container(Box::new(container)))
        }
        Some("agent") => Err(invalid(
            format!("{prefix}/type"),
            json!("agent"),
            "agent workloads are refused; prepare the job in an agent and submit its command",
        )),
        _ => Err(invalid(
            format!("{prefix}/type"),
            step.get("type").cloned().unwrap_or(Value::Null),
            "step type must be \"task\" or \"container\"",
        )),
    }
}

/// Refuse what the authority owns in a container step: its GPU, mounts at or
/// under `/homebased`, and `HOMEBASED_*` environment keys
fn check_container_structure(container: &ContainerWorkload, prefix: &str) -> Result<(), AppError> {
    if let Some(gpus) = &container.gpus {
        return Err(invalid(
            format!("{prefix}/gpus"),
            serde_json::to_value(gpus)?,
            "a job step must not set gpus; the authority sets it from the assigned resource",
        ));
    }
    let reserved = Path::new(RESERVED_MOUNT_ROOT);
    if let Some((index, mount)) = container
        .mounts
        .iter()
        .enumerate()
        .find(|(_, mount)| mount.target.starts_with(reserved))
    {
        return Err(invalid(
            format!("{prefix}/mounts/{index}/target"),
            json!(mount.target),
            format!("mount targets at or under {RESERVED_MOUNT_ROOT} are reserved for Homebased"),
        ));
    }
    if let Some(name) = container
        .env
        .keys()
        .find(|name| name.as_str().starts_with(RESERVED_ENV_PREFIX))
    {
        return Err(invalid(
            format!("{prefix}/env/{}", escape_token(name.as_str())),
            json!(name),
            format!(
                "environment keys starting with {RESERVED_ENV_PREFIX} are reserved for Homebased"
            ),
        ));
    }
    Ok(())
}

/// Resolve a task step's program and refuse an entry point that hands work to
/// a long-lived server
///
/// Both the resolved path and its target after symbolic links are checked, so
/// a link named otherwise cannot hide `tmux`. The check reads only the entry
/// point; a script can still call these internally
fn check_entry_point(
    command: &CommandLine,
    env_path: &str,
    cwd: &Path,
    pointer: &str,
) -> Result<(), AppError> {
    let resolved = resolve_executable(command.program(), env_path, cwd)?;
    let canonical = std::fs::canonicalize(&resolved).unwrap_or_else(|_| resolved.clone());
    let escape = [&resolved, &canonical].into_iter().find_map(|path| {
        let name = path.file_name()?.to_str()?;
        ESCAPE_ENTRY_POINTS.contains(&name).then_some(name)
    });
    match escape {
        Some(name) => Err(invalid(
            format!("{pointer}/command/0"),
            json!(command.program()),
            format!(
                "{name} hands work to a long-lived server outside the run, which cleanup cannot \
                 attribute; run the work directly"
            ),
        )),
        None => Ok(()),
    }
}

/// Move a pointer under `/workload` to the step's own pointer
fn reprefix(error: AppError, prefix: &str) -> AppError {
    match error {
        AppError::InvalidSpec {
            pointer,
            value,
            message,
        } => AppError::InvalidSpec {
            pointer: match pointer.strip_prefix("/workload") {
                Some(rest) => format!("{prefix}{rest}"),
                None => pointer,
            },
            value,
            message,
        },
        other => other,
    }
}

fn invalid(pointer: impl Into<String>, value: Value, message: impl Into<String>) -> AppError {
    AppError::InvalidSpec {
        pointer: pointer.into(),
        value,
        message: message.into(),
    }
}

fn deserialize_under<T: for<'de> Deserialize<'de>>(
    value: &Value,
    prefix: &str,
) -> Result<T, AppError> {
    serde_path_to_error::deserialize(value).map_err(|err| {
        let mut inner = json_pointer(err.path());
        if inner.is_empty()
            && let Some(name) = missing_field_name(err.inner())
        {
            inner = format!("/{}", escape_token(&name));
        }
        let field_value = value.pointer(&inner).cloned().unwrap_or(Value::Null);
        invalid(format!("{prefix}{inner}"), field_value, err.to_string())
    })
}

fn missing_field_name(err: &serde_json::Error) -> Option<String> {
    let message = err.to_string();
    let rest = message.strip_prefix("missing field `")?;
    let name = rest.split('`').next()?;
    (!name.is_empty()).then(|| name.to_owned())
}

#[cfg(test)]
mod tests;
