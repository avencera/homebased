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
    let container = container_workload_schema(false);
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
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct TaskWire {
                command: Vec<String>,
            }
            let task: TaskWire = deserialize_under(&content, "/workload")?;
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
fn invalid_spec_from_de(
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
fn workload_content(workload_raw: &Value) -> Value {
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

/// Check the host inputs that a workload names on the machine that runs it
///
/// A container's mount sources must exist there and must not expose a
/// container daemon socket. Other workloads name no host inputs beyond `cwd`
pub fn check_workload_host(workload: &NormalizedWorkload) -> Result<(), AppError> {
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

#[cfg(test)]
fn example_agent_json() -> Value {
    json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "implement file browser",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": {
            "type": "agent",
            "agent": "claude",
            "model": "fable",
            "prompt": "do the work",
            "extra_args": ["--verbose"]
        }
    })
}

#[cfg(test)]
fn example_task_json() -> Value {
    json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "cargo release build",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": {
            "type": "task",
            "command": ["cargo", "build", "--release"]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        NormalizedWorkload, PromptSource, SubmitAgent, SubmitSpec, SubmitWorkloadValidated,
        default_timeout, example_agent_json, example_task_json, normalize, parse_normalized_value,
        parse_spec_value, schema_json,
    };
    use crate::domain::{AgentKind, TaskName, ThreadId};
    use crate::error::AppError;
    use serde_json::{Value, json};
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;

    #[test]
    fn absent_machine_keeps_local_spec_valid() {
        let spec = parse_spec_value(&valid_task()).unwrap();
        assert!(spec.machine.is_none());
        assert!(normalize(&spec).unwrap().machine.is_none());
    }

    #[test]
    fn remote_prompt_file_must_be_absolute_on_origin() {
        let mut value = valid_agent();
        value["machine"] = json!("code");
        value["workload"].as_object_mut().unwrap().remove("prompt");
        value["workload"]["prompt_file"] = json!("prompt.txt");
        let spec = parse_spec_value(&value).unwrap();
        assert!(matches!(
            normalize(&spec),
            Err(AppError::InvalidSpec { pointer, .. }) if pointer == "/workload/prompt_file"
        ));
    }

    #[test]
    fn blank_prompt_rejected() {
        let mut value = valid_agent();
        value["workload"]["prompt"] = json!(" \n");
        let spec = parse_spec_value(&value).unwrap();
        assert!(matches!(
            normalize(&spec),
            Err(AppError::InvalidSpec { pointer, .. }) if pointer == "/workload/prompt"
        ));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("prompt.md");
        fs::write(&file, "\n").unwrap();
        value["workload"].as_object_mut().unwrap().remove("prompt");
        value["workload"]["prompt_file"] = json!(file);
        let spec = parse_spec_value(&value).unwrap();
        assert!(matches!(
            normalize(&spec),
            Err(AppError::InvalidSpec { pointer, .. }) if pointer == "/workload/prompt_file"
        ));
    }

    fn valid_agent() -> Value {
        json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "test agent",
            "cwd": "/tmp",
            "workload": {
                "type": "agent",
                "agent": "claude",
                "prompt": "hello"
            }
        })
    }

    fn valid_task() -> Value {
        json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "test task",
            "cwd": "/tmp",
            "workload": {
                "type": "task",
                "command": ["echo", "hi"]
            }
        })
    }

    fn valid_opencode(model: Option<&str>) -> Value {
        let mut value = valid_agent();
        value["workload"]["agent"] = json!("opencode");
        if let Some(model) = model {
            value["workload"]["model"] = json!(model);
        } else {
            value["workload"].as_object_mut().unwrap().remove("model");
        }
        value
    }

    #[track_caller]
    fn after_error(after: Value) -> (String, Value) {
        let mut value = valid_task();
        value["after"] = after;
        match parse_spec_value(&value).unwrap_err() {
            AppError::InvalidSpec { pointer, value, .. } => (pointer, value),
            other => panic!("expected invalid_spec, got {other:?}"),
        }
    }

    #[test]
    fn after_is_optional_sorted_and_kept_out_of_the_normalized_spec() {
        let spec = parse_spec_value(&valid_task()).unwrap();
        assert!(spec.after.is_none());

        let first = crate::domain::TaskId::new();
        let second = crate::domain::TaskId::new();
        let mut value = valid_task();
        value["after"] = json!([second, first]);
        let spec = parse_spec_value(&value).unwrap();
        assert_eq!(spec.after.as_ref().unwrap().tasks(), &[first, second]);
        let normalized = serde_json::to_value(normalize(&spec).unwrap()).unwrap();
        assert!(normalized.get("after").is_none());
    }

    #[test]
    fn after_list_errors_point_at_the_offending_entry() {
        let (pointer, value) = after_error(json!([]));
        assert_eq!((pointer.as_str(), value), ("/after", json!([])));

        let task = crate::domain::TaskId::new();
        let (pointer, value) = after_error(json!([task, crate::domain::TaskId::new(), task]));
        assert_eq!((pointer.as_str(), value), ("/after/2", json!(task)));

        let many: Vec<_> = (0..=crate::dependency::MAX_DEPENDENCIES)
            .map(|_| crate::domain::TaskId::new())
            .collect();
        let (pointer, _) = after_error(json!(many));
        assert_eq!(pointer, "/after");

        let (pointer, value) = after_error(json!(["not-a-uuid"]));
        assert_eq!((pointer.as_str(), value), ("/after/0", json!("not-a-uuid")));
    }

    #[test]
    fn unknown_field_rejected() {
        let mut value = valid_agent();
        value["typo"] = json!(true);
        let err = parse_spec_value(&value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/typo"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn both_prompt_and_file_rejected() {
        let mut value = valid_agent();
        value["workload"]["prompt_file"] = json!("/tmp/p.txt");
        let err = parse_spec_value(&value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/workload/prompt"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn neither_prompt_rejected() {
        let mut value = valid_agent();
        value["workload"].as_object_mut().unwrap().remove("prompt");
        let err = parse_spec_value(&value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/workload/prompt"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn cross_variant_agent_fields_on_task_rejected() {
        let mut value = valid_task();
        value["workload"]["agent"] = json!("claude");
        let err = parse_spec_value(&value).unwrap_err();
        assert!(matches!(err, AppError::InvalidSpec { .. }));
    }

    #[test]
    fn empty_command_rejected() {
        let mut value = valid_task();
        value["workload"]["command"] = json!([]);
        let err = parse_spec_value(&value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => {
                assert!(pointer.starts_with("/workload/command"), "{pointer}");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn empty_program_rejected() {
        let mut value = valid_task();
        value["workload"]["command"] = json!([""]);
        let err = parse_spec_value(&value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/workload/command/0"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn timeout_below_thirty_minutes_rejected() {
        let mut value = valid_task();
        value["timeout"] = json!("29m");
        let err = parse_spec_value(&value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/timeout"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn timeout_thirty_minutes_accepted() {
        let mut value = valid_task();
        value["timeout"] = json!("30m");
        let spec = parse_spec_value(&value).unwrap();
        assert_eq!(spec.timeout, Duration::from_secs(30 * 60));
    }

    #[test]
    fn pointer_on_bad_array_element() {
        let mut value = valid_agent();
        value["workload"]["extra_args"] = json!([1]);
        let err = parse_spec_value(&value).unwrap_err();
        match err {
            AppError::InvalidSpec {
                pointer,
                value: field,
                ..
            } => {
                assert_eq!(pointer, "/workload/extra_args/0");
                assert_eq!(field, json!(1));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn resume_thread_is_rejected_for_non_codex_agents() {
        let mut value = valid_agent();
        value["workload"]["resume_thread"] = json!("01a0e487-b877-76e2-9dc2-806bff0bf685");
        let err = parse_spec_value(&value).unwrap_err();
        assert!(matches!(
            err,
            AppError::InvalidSpec { pointer, value, .. }
                if pointer == "/workload/resume_thread"
                    && value == json!("01a0e487-b877-76e2-9dc2-806bff0bf685")
        ));
    }

    #[test]
    fn null_resume_thread_is_accepted_for_non_codex_agents() {
        let mut value = valid_agent();
        value["workload"]["resume_thread"] = Value::Null;
        let parsed = parse_spec_value(&value).unwrap();
        let SubmitWorkloadValidated::Agent(agent) = parsed.workload else {
            panic!("expected agent workload");
        };
        assert_eq!(agent.resume_thread, None);
    }

    #[test]
    fn opencode_accepts_provider_qualified_models_and_preserves_variants() {
        for model in [
            Some("zai-coding-plan/glm-5.3-flash"),
            Some("other/provider#fast"),
            None,
        ] {
            let value = valid_opencode(model);
            let spec = parse_spec_value(&value).unwrap();
            let normalized = normalize(&spec).unwrap();
            let NormalizedWorkload::Agent(agent) = normalized.workload else {
                panic!("expected agent workload");
            };
            assert_eq!(agent.agent, AgentKind::OpenCode);
            assert_eq!(agent.model.as_deref(), model);
        }
    }

    #[test]
    fn opencode_forbidden_extra_args_report_their_array_pointer() {
        for extra in [
            "--agent=other",
            "--dir=/other",
            "--server=http://localhost",
            "-c",
            "--session=session",
            "--fork",
            "--model=other/provider",
            "-m=other/provider",
            "--standalone=false",
            "prompt in argv",
        ] {
            let mut value = valid_opencode(None);
            value["workload"]["extra_args"] = json!([extra]);
            let err = parse_spec_value(&value).unwrap_err();
            match err {
                AppError::InvalidSpec { pointer, value, .. } => {
                    assert_eq!(pointer, "/workload/extra_args/0");
                    assert_eq!(value, json!(extra));
                }
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn existing_agent_extra_args_keep_their_previous_rules() {
        let mut value = valid_agent();
        value["workload"]["extra_args"] = json!(["free-form-value"]);
        assert!(parse_spec_value(&value).is_ok());
    }

    #[test]
    fn relative_prompt_file_resolves_against_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("p.txt");
        fs::write(&prompt_path, "from-file").unwrap();
        let spec = SubmitSpec {
            machine: None,
            api_version: 1,
            thread: ThreadId::from_str_ok(),
            name: TaskName::parse("from file").unwrap(),
            cwd: dir.path().to_path_buf(),
            timeout: default_timeout(),
            after: None,
            workload: SubmitWorkloadValidated::Agent(SubmitAgent {
                agent: AgentKind::Claude,
                model: None,
                prompt: PromptSource::File(PathBuf::from("p.txt")),
                extra_args: vec![],
                report_trailer: true,
                resume_thread: None,
            }),
        };
        let normalized = normalize(&spec).unwrap();
        match normalized.workload {
            NormalizedWorkload::Agent(agent) => assert_eq!(agent.prompt, "from-file"),
            other => panic!("unexpected {other:?}"),
        }
    }

    /// One published schema, compiled once per assertion set
    fn validator() -> jsonschema::Validator {
        jsonschema::validator_for(&schema_json().unwrap()).unwrap()
    }

    /// The schema and the parser must reach the same verdict on every spec
    /// Asserting both here is what stops the generated document from drifting
    /// away from the runtime rules
    #[track_caller]
    fn assert_verdict(value: &Value, accepted: bool, why: &str) {
        let schema_ok = validator().is_valid(value);
        assert_eq!(
            schema_ok,
            accepted,
            "schema should {} {why}: {value}",
            if accepted { "accept" } else { "reject" }
        );
        let parser = parse_spec_value(value);
        assert_eq!(
            parser.is_ok(),
            accepted,
            "parser should {} {why}: {parser:?}",
            if accepted { "accept" } else { "reject" }
        );
    }

    fn with_workload(workload: Value) -> Value {
        json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "test agent",
            "cwd": "/tmp",
            "workload": workload
        })
    }

    #[test]
    fn schema_accepts_both_documented_examples() {
        assert_verdict(&example_agent_json(), true, "the agent example");
        assert_verdict(&example_task_json(), true, "the task example");
    }

    #[test]
    fn schema_pins_api_version_one() {
        let mut value = valid_task();
        value["api_version"] = json!(2);
        assert_verdict(&value, false, "api_version 2");
        let schema = schema_json().unwrap();
        assert_eq!(schema["properties"]["api_version"]["const"], json!(1));
    }

    #[test]
    fn schema_requires_exactly_one_agent_prompt_source() {
        assert_verdict(
            &with_workload(json!({"type": "agent", "agent": "claude", "prompt": "hi"})),
            true,
            "an inline prompt",
        );
        assert_verdict(
            &with_workload(json!({"type": "agent", "agent": "claude", "prompt_file": "/tmp/p"})),
            true,
            "a prompt file",
        );
        assert_verdict(
            &with_workload(json!({
                "type": "agent", "agent": "claude",
                "prompt": "hi", "prompt_file": "/tmp/p"
            })),
            false,
            "both prompt sources",
        );
        assert_verdict(
            &with_workload(json!({"type": "agent", "agent": "claude"})),
            false,
            "no prompt source",
        );
        assert_verdict(
            &with_workload(json!({
                "type": "agent", "agent": "claude",
                "prompt": "hi", "prompt_file": null
            })),
            false,
            "a null prompt file beside an inline prompt",
        );
        assert_verdict(
            &with_workload(json!({
                "type": "agent", "agent": "claude",
                "prompt": null, "prompt_file": "/tmp/p"
            })),
            false,
            "a null inline prompt beside a prompt file",
        );
    }

    #[test]
    fn schema_requires_a_non_empty_command() {
        assert_verdict(
            &with_workload(json!({"type": "task", "command": ["cargo", "build"]})),
            true,
            "a normal command",
        );
        assert_verdict(
            &with_workload(json!({"type": "task", "command": ["echo", "", " "]})),
            true,
            "empty and whitespace arguments after the program",
        );
        assert_verdict(
            &with_workload(json!({"type": "task", "command": []})),
            false,
            "an empty command array",
        );
        assert_verdict(
            &with_workload(json!({"type": "task", "command": [""]})),
            false,
            "an empty program",
        );
        assert_verdict(
            &with_workload(json!({"type": "task", "command": ["", "build"]})),
            false,
            "an empty program with arguments",
        );
        assert_verdict(
            &with_workload(json!({"type": "task", "command": ["echo", "a\0b"]})),
            false,
            "a NUL byte in an argument",
        );
        assert_verdict(
            &with_workload(json!({"type": "task", "command": ["car\0go"]})),
            false,
            "a NUL byte in the program",
        );
    }

    #[test]
    fn schema_rejects_cross_variant_fields() {
        assert_verdict(
            &with_workload(json!({
                "type": "task", "command": ["true"], "agent": "claude"
            })),
            false,
            "an agent field on a task",
        );
        assert_verdict(
            &with_workload(json!({
                "type": "task", "command": ["true"], "prompt": "hi"
            })),
            false,
            "a prompt on a task",
        );
        assert_verdict(
            &with_workload(json!({
                "type": "task", "command": ["true"], "report_trailer": true
            })),
            false,
            "a trailer flag on a task",
        );
        assert_verdict(
            &with_workload(json!({
                "type": "agent", "agent": "claude", "prompt": "hi", "command": ["true"]
            })),
            false,
            "a command on an agent",
        );
    }

    #[test]
    fn schema_rejects_an_unknown_variant_and_unknown_keys() {
        assert_verdict(
            &with_workload(json!({"type": "shell", "command": ["true"]})),
            false,
            "an unknown workload type",
        );
        assert_verdict(
            &with_workload(json!({
                "type": "agent", "agent": "claude", "prompt": "hi", "typo": 1
            })),
            false,
            "an unknown agent key",
        );
        assert_verdict(
            &with_workload(json!({"type": "agent", "agent": "gemini", "prompt": "hi"})),
            false,
            "an unsupported agent kind",
        );
    }

    #[test]
    fn schema_and_parser_agree_on_container_workloads() {
        let digest = format!("sha256:{}", "0".repeat(64));
        let container = |extra: Value| {
            let mut workload = json!({
                "type": "container",
                "image": format!("eval@{digest}"),
                "memory": "1g"
            });
            if let (Some(workload), Value::Object(extra)) = (workload.as_object_mut(), extra) {
                workload.extend(extra);
            }
            with_workload(workload)
        };
        for (extra, why) in [
            (json!({}), "a minimal container"),
            (json!({ "image": digest }), "an image ID"),
            (
                json!({ "image": format!("misc.local:5000/team/eval-probe@{digest}") }),
                "a registry image",
            ),
            (
                json!({
                    "entrypoint": ["/usr/bin/python3", "-m", "eval"],
                    "args": ["--ckpt", "/data/ckpt", ""],
                    "gpus": [0, 1],
                    "memory": 25_769_803_776_u64,
                    "user": "1000:1000",
                    "workdir": "/work",
                    "mounts": [{ "source": "/shared/ckpt", "target": "/data", "read_only": true }],
                    "env": { "HF_HOME": "/data/hf" }
                }),
                "every field",
            ),
            (json!({ "gpus": "all" }), "all GPUs"),
        ] {
            assert_verdict(&container(extra), true, why);
        }
        for (extra, why) in [
            (json!({ "image": "eval:latest" }), "a tag alone"),
            (
                json!({ "image": format!("eval:1.0@{digest}") }),
                "a tag beside a digest",
            ),
            (json!({ "memory": null }), "a null memory limit"),
            (json!({ "privileged": true }), "privileged mode"),
            (json!({ "pid": "host" }), "a host PID namespace"),
            (json!({ "restart": "always" }), "a restart policy"),
            (json!({ "command": ["python"] }), "a command field"),
            (json!({ "gpus": [] }), "an empty GPU list"),
            (json!({ "gpus": [0, 0] }), "a repeated GPU"),
            (json!({ "gpus": null }), "a null GPU request"),
            (json!({ "entrypoint": [] }), "an empty entrypoint"),
            (json!({ "args": "python eval.py" }), "a shell string"),
            (json!({ "user": "root" }), "a named user"),
            (json!({ "workdir": "work" }), "a relative workdir"),
            (
                json!({ "env": { "1X": "v" } }),
                "an invalid environment name",
            ),
            (
                json!({ "mounts": [{ "source": "data", "target": "/d" }] }),
                "a relative mount source",
            ),
            (
                json!({ "mounts": [{ "source": "/data", "target": "/d", "propagation": "shared" }] }),
                "an unknown mount option",
            ),
        ] {
            assert_verdict(&container(extra), false, why);
        }
        let mut missing_memory = container(json!({}));
        missing_memory["workload"]
            .as_object_mut()
            .unwrap()
            .remove("memory");
        assert_verdict(&missing_memory, false, "a missing memory limit");
    }

    #[test]
    fn container_workloads_normalize_and_survive_the_socket() {
        let digest = format!("sha256:{}", "0".repeat(64));
        let spec = parse_spec_value(&with_workload(json!({
            "type": "container",
            "image": digest,
            "memory": "2g",
            "gpus": [1],
            "env": { "B": "2", "A": "1" }
        })))
        .unwrap();
        let normalized = normalize(&spec).unwrap();
        let mut body = serde_json::to_value(&normalized).unwrap();
        assert_eq!(body["workload"]["type"], "container");
        assert_eq!(body["workload"]["memory"], json!(2_u64 << 30));
        assert_eq!(parse_normalized_value(&body).unwrap(), normalized);
        body["workload"]["privileged"] = json!(true);
        let err = parse_normalized_value(&body).unwrap_err();
        assert!(
            matches!(&err, AppError::InvalidSpec { pointer, .. } if pointer == "/workload/privileged"),
            "{err:?}"
        );
    }

    #[test]
    fn default_timeout_is_one_hour() {
        let spec = parse_spec_value(&valid_agent()).unwrap();
        assert_eq!(spec.timeout, Duration::from_secs(3600));
        assert_eq!(spec.name.as_str(), "test agent");
        match spec.workload {
            SubmitWorkloadValidated::Agent(agent) => assert!(agent.report_trailer),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn required_name_is_parsed_and_normalized() {
        let mut value = valid_task();
        value["name"] = json!("  build release  ");
        let spec = parse_spec_value(&value).unwrap();
        assert_eq!(spec.name.as_str(), "build release");
        let normalized = normalize(&spec).unwrap();
        assert_eq!(normalized.name.as_str(), "build release");
    }

    #[test]
    fn blank_name_is_rejected_at_pointer() {
        let mut value = valid_task();
        value["name"] = json!("   ");
        let err = parse_spec_value(&value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/name"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn schema_requires_name() {
        let schema = schema_json().unwrap();
        assert!(schema["properties"]["name"].is_object());
        let required = schema["required"]
            .as_array()
            .expect("schema required array");
        assert!(
            required.iter().any(|value| value == "name"),
            "schema required={required:?}"
        );
        assert_verdict(&valid_task(), true, "a named task");
        let mut nameless = valid_task();
        nameless.as_object_mut().unwrap().remove("name");
        assert_verdict(&nameless, false, "a missing name");
        let mut named = valid_task();
        named["name"] = json!("ci watch");
        assert_verdict(&named, true, "a renamed task");
        named["name"] = json!("");
        assert_verdict(&named, false, "an empty name");
        named["name"] = json!("name\n");
        assert_verdict(&named, false, "a name with a trailing line break");
        named["name"] = json!("\tname");
        assert_verdict(&named, false, "a name with a control character");

        let err = parse_spec_value(&nameless).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/name"),
            other => panic!("unexpected {other:?}"),
        }
        let mut normalized = nameless.clone();
        normalized["timeout"] = json!("4h");
        let err = parse_normalized_value(&normalized).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/name"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn normalized_rejects_short_timeout() {
        let value = json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "test task",
            "cwd": "/tmp",
            "timeout": "29m",
            "workload": { "type": "task", "command": ["true"] }
        });
        let err = parse_normalized_value(&value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/timeout"),
            other => panic!("unexpected {other:?}"),
        }
    }

    impl ThreadId {
        fn from_str_ok() -> Self {
            "01a0ab97-a7aa-7463-a5b0-8d500e40e431".parse().unwrap()
        }
    }

    #[test]
    fn container_cwd_names_the_host_path_under_the_deepest_mount_target() {
        let host = tempfile::tempdir().unwrap();
        let spec: super::NormalizedSpec = serde_json::from_value(json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "container cwd",
            "cwd": "/scratch/cache/runs",
            "timeout": "4h",
            "workload": {
                "type": "container",
                "image": format!("bench@sha256:{}", "0".repeat(64)),
                "memory": "8g",
                "mounts": [
                    { "source": host.path(), "target": "/scratch" },
                    { "source": host.path().join("cache"), "target": "/scratch/cache" }
                ]
            }
        }))
        .unwrap();

        let error = super::check_spec_host(&spec).unwrap_err();
        assert_eq!(
            error.rejection,
            super::HostInputRejection::Cwd(super::CwdProblem::NotFound)
        );
        let AppError::InvalidCwd { suggested_cwd, .. } = &error.error else {
            panic!("expected invalid_cwd, got {:?}", error.error);
        };
        assert_eq!(
            suggested_cwd.as_deref(),
            Some(host.path().join("cache/runs").as_path())
        );
    }
}
