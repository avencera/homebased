//! Submit spec: serde, schemars, and prompt resolution

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::agents::{AgentExtraArgsError, validate_agent_extra_args};
use crate::container::ContainerWorkload;
use crate::container::spec::container_workload_schema;
use crate::dependency::{DependencyListError, MAX_DEPENDENCIES, TaskDependencies};
use crate::domain::{
    API_VERSION, AgentKind, DEFAULT_TIMEOUT, MIN_TIMEOUT, TaskId, TaskName, ThreadId,
};
use crate::error::AppError;
use crate::invocation::CommandLine;
use crate::machine::MachineName;

mod host;
#[cfg(test)]
mod tests;

pub use host::{
    CwdProblem, HostInputError, HostInputRejection, check_cwd, check_spec_host, cwd_problem,
};

/// Default output-inactivity timeout
#[must_use]
pub fn default_timeout() -> Duration {
    DEFAULT_TIMEOUT
}

/// `api_version` is a constant, not just an integer: `check_api_version`
/// rejects every other value, so the published schema says so too
fn api_version_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "integer",
        "const": API_VERSION,
        "description": "Must be 1."
    })
}

fn timeout_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "string",
        "default": "1h",
        "description": "Output-inactivity timer as a humantime duration. Default 1h. Minimum 30m. Does not kill the child."
    })
}

fn after_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "array",
        "items": { "type": "string", "format": "uuid" },
        "minItems": 1,
        "maxItems": MAX_DEPENDENCIES,
        "uniqueItems": true
    })
}

fn default_true() -> bool {
    true
}

/// Wire agent workload. `prompt` and `prompt_file` stay as two keys because
/// that is the documented JSON; validation collapses them into one source
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitAgentWorkload {
    /// Agent CLI
    pub agent: AgentKind,
    /// Optional model id. OpenCode accepts `provider/model#variant`; empty becomes unset
    #[serde(default)]
    pub model: Option<String>,
    /// Inline prompt. Mutually exclusive with `prompt_file`
    #[serde(default)]
    pub prompt: Option<String>,
    /// Prompt file. Relative paths resolve against `cwd`
    #[serde(default)]
    pub prompt_file: Option<PathBuf>,
    /// Extra argv appended after the unattended flags
    #[serde(default)]
    pub extra_args: Vec<String>,
    /// Append the reporting trailer to the child feed
    #[serde(default = "default_true")]
    pub report_trailer: bool,
    /// Resume this Codex thread
    #[serde(default)]
    pub resume_thread: Option<ThreadId>,
}

/// Wire task workload: argv only. Command is validated after deserialize so
/// JSON pointers land on `/workload/command/N`
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitTaskWorkload {
    /// Argv array. Index 0 is the program
    pub command: Vec<String>,
}

/// Wire shape of `task submit --spec`
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "SubmitSpec")]
struct SubmitSpecWire {
    /// Must be 1
    #[schemars(schema_with = "api_version_schema")]
    api_version: u32,
    /// Codex thread that receives `HOMEBASED_EVENT`
    thread: ThreadId,
    /// Human-readable name. Non-unique
    name: TaskName,
    /// Existing host directory on the machine that runs the task. For a
    /// container this is a host path, not a path inside the container; set
    /// `workload.workdir` for that
    cwd: PathBuf,
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    machine: Option<MachineName>,
    /// Output-inactivity timer. Default 1h, minimum 30m
    #[serde(default = "default_timeout", with = "humantime_serde")]
    #[schemars(schema_with = "timeout_schema")]
    timeout: Duration,
    /// Task UUIDs that must succeed before this task starts. Each must be a
    /// task this daemon is the origin for. Omit for no dependencies
    // parsed from `Value` so each list error gets its own pointer
    #[serde(default)]
    #[schemars(schema_with = "after_schema")]
    after: Option<Value>,
    /// Workload variant. Parsed from `Value` after the envelope so nested
    /// JSON pointers stay accurate under the internally tagged enum
    #[schemars(schema_with = "workload_schema")]
    workload: Value,
}

/// Version 1 workload schema, written out rather than derived
///
/// A derived internally tagged enum emits optional `prompt`/`prompt_file` and
/// an unbounded `command`, which would accept specs the parser rejects. The
/// outer `oneOf` separates the variants (each branch closed, so cross-variant
/// fields fail both), and the inner `oneOf` on the agent branch is what makes
/// the two prompt keys exactly-one rather than either-or. The agent kind and
/// the command shape are pulled from their owning types so they cannot drift
fn workload_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let agent_kind = subschema::<AgentKind>(generator);
    let resume_thread = subschema::<Option<ThreadId>>(generator);
    let command = subschema::<CommandLine>(generator);
    let container = container_workload_schema();
    schemars::json_schema!({
        "description": "Workload variant. Exactly one of agent, task, or container.",
        "oneOf": [
            {
                "title": "agent",
                "description": "Agent CLI with exactly one prompt source.",
                "type": "object",
                "properties": {
                    "type": { "const": "agent" },
                    "agent": agent_kind,
                    "model": { "type": ["string", "null"] },
                    "prompt": { "type": "string" },
                    "prompt_file": { "type": "string" },
                    "extra_args": { "type": "array", "items": { "type": "string" } },
                    "report_trailer": { "type": "boolean", "default": true },
                    "resume_thread": resume_thread
                },
                "required": ["type", "agent"],
                "additionalProperties": false,
                "oneOf": [
                    { "required": ["prompt"] },
                    { "required": ["prompt_file"] }
                ]
            },
            {
                "title": "task",
                "description": "Arbitrary non-interactive command. No shell, no quoting.",
                "type": "object",
                "properties": {
                    "type": { "const": "task" },
                    "command": command
                },
                "required": ["type", "command"],
                "additionalProperties": false
            },
            container
        ]
    })
}

/// Inline another type's schema as a plain value, so it can be embedded in a
/// hand-written schema without a `$ref` into a definitions map
fn subschema<T: JsonSchema>(generator: &mut schemars::SchemaGenerator) -> Value {
    <T as JsonSchema>::json_schema(generator).as_value().clone()
}

/// Where the prompt text comes from. Exactly one of the two wire keys
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptSource {
    /// `prompt`: the text itself
    Inline(String),
    /// `prompt_file`: a path, resolved against `cwd` when relative
    File(PathBuf),
}

impl PromptSource {
    /// Pick the source from the two mutually exclusive wire keys
    fn from_wire(
        prompt: Option<String>,
        prompt_file: Option<PathBuf>,
        raw_workload: &Value,
    ) -> Result<Self, AppError> {
        for key in ["prompt", "prompt_file"] {
            if raw_workload.get(key).is_some_and(Value::is_null) {
                return Err(AppError::InvalidSpec {
                    pointer: format!("/workload/{key}"),
                    value: Value::Null,
                    message: format!("{key} must be a string"),
                });
            }
        }
        match (prompt, prompt_file) {
            (Some(text), None) => Ok(Self::Inline(text)),
            (None, Some(path)) => Ok(Self::File(path)),
            (Some(_), Some(_)) => Err(exactly_one_prompt(
                raw_workload
                    .pointer("/prompt")
                    .cloned()
                    .unwrap_or(Value::Null),
            )),
            (None, None) => Err(exactly_one_prompt(Value::Null)),
        }
    }

    /// Read the prompt text, resolving a relative file against `cwd`
    ///
    /// A blank prompt is refused: the agent would start with only the report
    /// trailer and no task
    fn read(&self, cwd: &Path) -> Result<String, AppError> {
        let (text, pointer, value) = match self {
            Self::Inline(text) => (text.clone(), "/workload/prompt", json!(text)),
            Self::File(path) => {
                let resolved = if path.is_absolute() {
                    path.clone()
                } else {
                    cwd.join(path)
                };
                let text = fs::read_to_string(&resolved).map_err(|err| AppError::InvalidSpec {
                    pointer: "/workload/prompt_file".into(),
                    value: json!(path),
                    message: format!("failed to read prompt_file {}: {err}", resolved.display()),
                })?;
                (text, "/workload/prompt_file", json!(path))
            }
        };
        if text.trim().is_empty() {
            return Err(AppError::InvalidSpec {
                pointer: pointer.into(),
                value,
                message: "the prompt is empty; write the task for the agent".into(),
            });
        }
        Ok(text)
    }
}

fn exactly_one_prompt(value: Value) -> AppError {
    AppError::InvalidSpec {
        pointer: "/workload/prompt".into(),
        value,
        message: "exactly one of prompt or prompt_file is required".into(),
    }
}

/// Validated agent submit workload before prompt resolution
#[derive(Debug, Clone)]
pub struct SubmitAgent {
    /// Agent CLI
    pub agent: AgentKind,
    /// Optional model id. OpenCode accepts `provider/model#variant`
    pub model: Option<String>,
    /// Prompt source
    pub prompt: PromptSource,
    /// Extra argv
    pub extra_args: Vec<String>,
    /// Trailer flag
    pub report_trailer: bool,
    /// Thread to resume when the agent is Codex
    pub resume_thread: Option<ThreadId>,
}

/// Validated submit workload before prompt resolution
#[derive(Debug, Clone)]
pub enum SubmitWorkloadValidated {
    /// Agent with unresolved prompt source
    Agent(SubmitAgent),
    /// Task command
    Task {
        /// Validated argv
        command: CommandLine,
    },
    /// Docker container
    Container(Box<ContainerWorkload>),
}

/// Validated submit spec
#[derive(Debug, Clone)]
pub struct SubmitSpec {
    /// Must be 1
    pub api_version: u32,
    /// Codex thread that receives `HOMEBASED_EVENT`
    pub thread: ThreadId,
    /// Human-readable name
    pub name: TaskName,
    /// Working directory for the child
    pub cwd: PathBuf,
    /// Execution machine name, or local when absent
    pub machine: Option<MachineName>,
    /// Output-inactivity timeout
    pub timeout: Duration,
    /// Tasks that must succeed before this task starts
    ///
    /// Origin-only: it travels beside the normalized spec, never inside it, so
    /// an executor cannot receive it
    pub after: Option<TaskDependencies>,
    /// Workload variant
    pub workload: SubmitWorkloadValidated,
}

/// Normalized agent workload with inline prompt
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedAgentWorkload {
    /// Agent CLI
    pub agent: AgentKind,
    /// Optional model
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Prompt bytes as text
    pub prompt: String,
    /// Extra argv
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
    /// Trailer flag
    pub report_trailer: bool,
    /// Thread to resume when the agent is Codex
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_thread: Option<ThreadId>,
}

/// Normalized task workload
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedTaskWorkload {
    /// Validated argv
    pub command: CommandLine,
}

/// Daemon-socket workload: agent prompt is always inline
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum NormalizedWorkload {
    /// Agent with inline prompt
    Agent(NormalizedAgentWorkload),
    /// Arbitrary command
    Task(NormalizedTaskWorkload),
    /// Docker container that Homebased starts, watches, and removes
    Container(Box<ContainerWorkload>),
}

/// Spec with prompt inlined and `prompt_file` removed. This is the only shape
/// the daemon socket accepts
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedSpec {
    /// Schema version
    pub api_version: u32,
    /// Codex thread
    pub thread: ThreadId,
    /// Human-readable name
    pub name: TaskName,
    /// Working directory
    pub cwd: PathBuf,
    /// Execution machine name, or local when absent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<MachineName>,
    /// Output-inactivity timeout
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// Workload variant
    pub workload: NormalizedWorkload,
}

/// Parse a spec from a file path or `-` for stdin
pub fn load_spec(path: &str) -> Result<SubmitSpec, AppError> {
    let bytes = if path == "-" {
        let mut buf = Vec::new();
        io::stdin()
            .read_to_end(&mut buf)
            .map_err(|err| AppError::Internal {
                message: format!("failed to read spec from stdin: {err}"),
            })?;
        buf
    } else {
        fs::read(path).map_err(|err| AppError::Internal {
            message: format!("failed to read spec {path}: {err}"),
        })?
    };
    parse_spec_bytes(&bytes)
}

/// Parse spec bytes, attaching a JSON pointer on failure
pub fn parse_spec_bytes(bytes: &[u8]) -> Result<SubmitSpec, AppError> {
    let value: Value = serde_json::from_slice(bytes).map_err(|err| AppError::InvalidSpec {
        pointer: String::new(),
        value: Value::Null,
        message: format!("invalid JSON: {err}"),
    })?;
    parse_spec_value(&value)
}

/// Parse an already-decoded JSON value
pub fn parse_spec_value(value: &Value) -> Result<SubmitSpec, AppError> {
    let wire: SubmitSpecWire =
        serde_path_to_error::deserialize(value).map_err(|err| invalid_spec_from_de(value, &err))?;
    validate_spec(wire, value)
}

/// Envelope used to parse common normalized fields before the workload enum
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct NormalizedSpecEnvelope {
    api_version: u32,
    thread: ThreadId,
    name: TaskName,
    cwd: PathBuf,
    #[serde(default)]
    machine: Option<MachineName>,
    #[serde(with = "humantime_serde")]
    timeout: Duration,
    workload: Value,
}

/// Parse a normalized socket body, attaching a JSON pointer on failure
pub fn parse_normalized_value(value: &Value) -> Result<NormalizedSpec, AppError> {
    let envelope: NormalizedSpecEnvelope =
        serde_path_to_error::deserialize(value).map_err(|err| invalid_spec_from_de(value, &err))?;
    check_api_version(envelope.api_version, "/api_version")?;
    check_timeout(envelope.timeout, "/timeout")?;
    let workload = parse_normalized_workload(&envelope.workload)?;
    Ok(NormalizedSpec {
        api_version: envelope.api_version,
        thread: envelope.thread,
        name: envelope.name,
        cwd: envelope.cwd,
        machine: envelope.machine,
        timeout: envelope.timeout,
        workload,
    })
}

fn parse_normalized_workload(workload_raw: &Value) -> Result<NormalizedWorkload, AppError> {
    let kind = workload_raw
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::InvalidSpec {
            pointer: "/workload/type".into(),
            value: workload_raw.get("type").cloned().unwrap_or(Value::Null),
            message: WORKLOAD_TYPE_MESSAGE.into(),
        })?;
    let content = workload_content(workload_raw);
    validate_resume_thread_field(kind, &content)?;
    match kind {
        "agent" => {
            let agent: NormalizedAgentWorkload = deserialize_under(&content, "/workload")?;
            validate_agent_extra_args(agent.agent, &agent.extra_args)
                .map_err(|err| invalid_agent_extra_args(&content, err))?;
            Ok(NormalizedWorkload::Agent(agent))
        }
        "task" => {
            let task: SubmitTaskWorkload = deserialize_under(&content, "/workload")?;
            let command = CommandLine::try_from_argv(task.command.clone())
                .map_err(|err| err.into_invalid_spec(json!(task.command)))?;
            Ok(NormalizedWorkload::Task(NormalizedTaskWorkload { command }))
        }
        "container" => Ok(NormalizedWorkload::Container(parse_container(&content)?)),
        other => Err(AppError::InvalidSpec {
            pointer: "/workload/type".into(),
            value: json!(other),
            message: WORKLOAD_TYPE_MESSAGE.into(),
        }),
    }
}

const WORKLOAD_TYPE_MESSAGE: &str = "workload.type must be \"agent\", \"task\", or \"container\"";

fn parse_container(content: &Value) -> Result<Box<ContainerWorkload>, AppError> {
    ContainerWorkload::from_value(content)
        .map(Box::new)
        .map_err(|err| err.into_invalid_spec("/workload"))
}

/// Map a serde failure onto `invalid_spec`, including missing required fields
///
/// `serde_path_to_error` leaves the path empty when a required field is
/// absent, so the missing name is recovered from the inner serde message
pub(crate) fn invalid_spec_from_de(
    value: &Value,
    err: &serde_path_to_error::Error<serde_json::Error>,
) -> AppError {
    let mut pointer = json_pointer(err.path());
    if pointer.is_empty()
        && let Some(name) = missing_field_name(err.inner())
    {
        pointer = format!("/{}", escape_token(&name));
    }
    let field_value = value.pointer(&pointer).cloned().unwrap_or(Value::Null);
    AppError::InvalidSpec {
        pointer,
        value: field_value,
        message: err.to_string(),
    }
}

fn missing_field_name(err: &serde_json::Error) -> Option<String> {
    let message = err.to_string();
    let rest = message.strip_prefix("missing field `")?;
    let name = rest.split('`').next()?;
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Render a `serde_path_to_error` path as an RFC 6901 JSON pointer
pub(crate) fn json_pointer(path: &serde_path_to_error::Path) -> String {
    use serde_path_to_error::Segment;

    let mut pointer = String::new();
    for segment in path.iter() {
        pointer.push('/');
        match segment {
            Segment::Seq { index } => pointer.push_str(&index.to_string()),
            Segment::Map { key } => pointer.push_str(&escape_token(key)),
            Segment::Enum { variant } => pointer.push_str(&escape_token(variant)),
            Segment::Unknown => pointer.push('?'),
        }
    }
    pointer
}

/// RFC 6901 §3: `~` becomes `~0` and `/` becomes `~1`, in that order
pub(crate) fn escape_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn check_api_version(version: u32, pointer: &str) -> Result<(), AppError> {
    if version == API_VERSION {
        Ok(())
    } else {
        Err(AppError::InvalidSpec {
            pointer: pointer.into(),
            value: json!(version),
            message: format!("api_version must be {API_VERSION}"),
        })
    }
}

fn check_timeout(timeout: Duration, pointer: &str) -> Result<(), AppError> {
    if timeout >= MIN_TIMEOUT {
        Ok(())
    } else {
        Err(AppError::InvalidSpec {
            pointer: pointer.into(),
            value: json!(humantime::format_duration(timeout).to_string()),
            message: format!(
                "timeout must be at least {}",
                humantime::format_duration(MIN_TIMEOUT)
            ),
        })
    }
}

fn validate_spec(wire: SubmitSpecWire, _raw: &Value) -> Result<SubmitSpec, AppError> {
    check_api_version(wire.api_version, "/api_version")?;
    check_timeout(wire.timeout, "/timeout")?;
    let after = wire.after.as_ref().map(parse_after).transpose()?;
    let workload = parse_submit_workload(&wire.workload)?;
    Ok(SubmitSpec {
        api_version: wire.api_version,
        thread: wire.thread,
        name: wire.name,
        cwd: wire.cwd,
        machine: wire.machine,
        timeout: wire.timeout,
        after,
        workload,
    })
}

/// Parse a top-level `after` value, with pointers under `/after`
///
/// The submit spec and the daemon socket envelope both carry `after` at the
/// top level, so both report the same pointers
pub fn parse_after(value: &Value) -> Result<TaskDependencies, AppError> {
    let tasks: Vec<TaskId> = deserialize_under(value, "/after")?;
    TaskDependencies::new(tasks).map_err(|error| {
        let (pointer, value) = match error {
            DependencyListError::Duplicate { index, task } => {
                (format!("/after/{index}"), json!(task))
            }
            DependencyListError::Empty | DependencyListError::TooMany { .. } => {
                ("/after".to_owned(), value.clone())
            }
        };
        AppError::InvalidSpec {
            pointer,
            value,
            message: error.to_string(),
        }
    })
}

fn parse_submit_workload(workload_raw: &Value) -> Result<SubmitWorkloadValidated, AppError> {
    let kind = workload_raw
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::InvalidSpec {
            pointer: "/workload/type".into(),
            value: workload_raw.get("type").cloned().unwrap_or(Value::Null),
            message: WORKLOAD_TYPE_MESSAGE.into(),
        })?;
    let content = workload_content(workload_raw);
    validate_resume_thread_field(kind, &content)?;
    match kind {
        "agent" => {
            let agent: SubmitAgentWorkload = deserialize_under(&content, "/workload")?;
            validate_agent_extra_args(agent.agent, &agent.extra_args)
                .map_err(|err| invalid_agent_extra_args(&content, err))?;
            let prompt = PromptSource::from_wire(agent.prompt, agent.prompt_file, workload_raw)?;
            Ok(SubmitWorkloadValidated::Agent(SubmitAgent {
                agent: agent.agent,
                model: agent.model,
                prompt,
                extra_args: agent.extra_args,
                report_trailer: agent.report_trailer,
                resume_thread: agent.resume_thread,
            }))
        }
        "task" => {
            let task: SubmitTaskWorkload = deserialize_under(&content, "/workload")?;
            let command = CommandLine::try_from_argv(task.command.clone())
                .map_err(|err| err.into_invalid_spec(json!(task.command)))?;
            Ok(SubmitWorkloadValidated::Task { command })
        }
        "container" => Ok(SubmitWorkloadValidated::Container(parse_container(
            &content,
        )?)),
        other => Err(AppError::InvalidSpec {
            pointer: "/workload/type".into(),
            value: json!(other),
            message: WORKLOAD_TYPE_MESSAGE.into(),
        }),
    }
}

fn validate_resume_thread_field(kind: &str, workload: &Value) -> Result<(), AppError> {
    if let Some(resume_thread) = workload
        .get("resume_thread")
        .filter(|value| !value.is_null())
        && (kind != "agent" || workload.get("agent").and_then(Value::as_str) != Some("codex"))
    {
        return Err(AppError::InvalidSpec {
            pointer: "/workload/resume_thread".into(),
            value: resume_thread.clone(),
            message: "resume_thread is only valid with agent: codex".into(),
        });
    }
    Ok(())
}

fn invalid_agent_extra_args(workload: &Value, error: AgentExtraArgsError) -> AppError {
    let value = workload
        .get("extra_args")
        .and_then(Value::as_array)
        .and_then(|args| args.get(error.index))
        .cloned()
        .unwrap_or(Value::Null);
    AppError::InvalidSpec {
        pointer: format!("/workload/extra_args/{}", error.index),
        value,
        message: error.to_string(),
    }
}

/// Drop the discriminant so content structs with `deny_unknown_fields` accept the body
pub(crate) fn workload_content(workload_raw: &Value) -> Value {
    match workload_raw {
        Value::Object(map) => {
            let mut content = map.clone();
            content.remove("type");
            Value::Object(content)
        }
        other => other.clone(),
    }
}

fn deserialize_under<T: for<'de> Deserialize<'de>>(
    value: &Value,
    prefix: &str,
) -> Result<T, AppError> {
    serde_path_to_error::deserialize(value).map_err(|err| {
        let pointer = format!("{prefix}{}", json_pointer(err.path()));
        let field_value = value
            .pointer(&json_pointer(err.path()))
            .cloned()
            .unwrap_or(Value::Null);
        AppError::InvalidSpec {
            pointer,
            value: field_value,
            message: err.to_string(),
        }
    })
}

/// Resolve prompt bytes and produce a normalized spec
pub fn normalize(spec: &SubmitSpec) -> Result<NormalizedSpec, AppError> {
    if spec.machine.is_some()
        && let SubmitWorkloadValidated::Agent(agent) = &spec.workload
        && let PromptSource::File(path) = &agent.prompt
        && !path.is_absolute()
    {
        return Err(AppError::InvalidSpec {
            pointer: "/workload/prompt_file".into(),
            value: json!(path),
            message: "remote prompt_file must be an absolute origin path".into(),
        });
    }
    let workload = match &spec.workload {
        SubmitWorkloadValidated::Agent(agent) => {
            let prompt = agent.prompt.read(&spec.cwd)?;
            let model = crate::domain::Agent::new(agent.agent, agent.model.clone()).model;
            NormalizedWorkload::Agent(NormalizedAgentWorkload {
                agent: agent.agent,
                model,
                prompt,
                extra_args: agent.extra_args.clone(),
                report_trailer: agent.report_trailer,
                resume_thread: agent.resume_thread,
            })
        }
        SubmitWorkloadValidated::Task { command } => {
            NormalizedWorkload::Task(NormalizedTaskWorkload {
                command: command.clone(),
            })
        }
        SubmitWorkloadValidated::Container(container) => {
            NormalizedWorkload::Container(container.clone())
        }
    };
    Ok(NormalizedSpec {
        api_version: API_VERSION,
        thread: spec.thread,
        name: spec.name.clone(),
        cwd: spec.cwd.clone(),
        machine: spec.machine.clone(),
        timeout: spec.timeout,
        workload,
    })
}

/// JSON Schema for the submit spec
pub fn schema_json() -> Result<Value, AppError> {
    let schema = schemars::schema_for!(SubmitSpecWire);
    Ok(serde_json::to_value(&schema)?)
}
