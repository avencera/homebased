//! Submit spec: serde, schemars, and prompt resolution.

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::domain::{Agent, AgentKind, ThreadId, API_VERSION};
use crate::error::AppError;

/// Default wall-clock timeout.
pub fn default_timeout() -> Duration {
    Duration::from_secs(4 * 3600)
}

fn timeout_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "string",
        "default": "4h",
        "description": "Humantime duration. Default 4h."
    })
}

fn default_true() -> bool {
    true
}

/// JSON document accepted by `task submit --spec`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SubmitSpec {
    /// Must be 1.
    pub api_version: u32,
    /// Agent CLI.
    pub agent: AgentKind,
    /// Optional model alias. Empty becomes unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Codex thread that receives `HOMEBASED_EVENT`.
    pub thread: ThreadId,
    /// Working directory for the child.
    pub cwd: PathBuf,
    /// Inline prompt. Mutually exclusive with `prompt_file`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    /// Prompt file. Relative paths resolve against `cwd`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_file: Option<PathBuf>,
    /// Wall-clock timeout.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    #[schemars(schema_with = "timeout_schema")]
    pub timeout: Duration,
    /// Extra argv appended after the unattended flags.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
    /// Append the reporting trailer to the child feed.
    #[serde(default = "default_true")]
    pub report_trailer: bool,
}

/// Spec with prompt inlined and `prompt_file` removed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedSpec {
    /// Schema version.
    pub api_version: u32,
    /// Agent CLI.
    pub agent: AgentKind,
    /// Optional model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Codex thread.
    pub thread: ThreadId,
    /// Working directory.
    pub cwd: PathBuf,
    /// Prompt bytes as text.
    pub prompt: String,
    /// Wall-clock timeout.
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// Extra argv.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
    /// Trailer flag.
    pub report_trailer: bool,
}

impl SubmitSpec {
    /// Agent identity with blank model stripped.
    #[must_use]
    pub fn agent(&self) -> Agent {
        Agent::new(self.agent, self.model.clone())
    }
}

/// Parse a spec from a file path or `-` for stdin.
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

/// Parse spec bytes, attaching a JSON pointer on failure.
pub fn parse_spec_bytes(bytes: &[u8]) -> Result<SubmitSpec, AppError> {
    let value: Value = serde_json::from_slice(bytes).map_err(|err| AppError::InvalidSpec {
        pointer: String::new(),
        value: Value::Null,
        message: format!("invalid JSON: {err}"),
    })?;
    parse_spec_value(value)
}

/// Parse an already-decoded JSON value.
pub fn parse_spec_value(value: Value) -> Result<SubmitSpec, AppError> {
    let pointer_value = value.clone();
    let spec: SubmitSpec = serde_path_to_error::deserialize(&value).map_err(|err| {
        let path = err.path().to_string();
        let pointer = json_pointer(&path);
        let field_value = pointer_value
            .pointer(&pointer)
            .cloned()
            .unwrap_or(Value::Null);
        AppError::InvalidSpec {
            pointer,
            value: field_value,
            message: err.to_string(),
        }
    })?;
    validate_spec(&spec, &pointer_value)
}

fn json_pointer(serde_path: &str) -> String {
    if serde_path.is_empty() || serde_path == "." {
        return String::new();
    }
    let trimmed = serde_path.trim_start_matches('.');
    format!("/{trimmed}").replace('.', "/")
}

fn validate_spec(spec: &SubmitSpec, raw: &Value) -> Result<SubmitSpec, AppError> {
    if spec.api_version != API_VERSION {
        return Err(AppError::InvalidSpec {
            pointer: "/api_version".into(),
            value: json!(spec.api_version),
            message: format!("api_version must be {API_VERSION}"),
        });
    }
    match (spec.prompt.is_some(), spec.prompt_file.is_some()) {
        (true, true) => {
            return Err(AppError::InvalidSpec {
                pointer: "/prompt".into(),
                value: raw.get("prompt").cloned().unwrap_or(Value::Null),
                message: "exactly one of prompt or prompt_file is required".into(),
            });
        }
        (false, false) => {
            return Err(AppError::InvalidSpec {
                pointer: "/prompt".into(),
                value: Value::Null,
                message: "exactly one of prompt or prompt_file is required".into(),
            });
        }
        _ => {}
    }
    Ok(spec.clone())
}

/// Resolve prompt bytes and produce a normalized spec.
pub fn normalize(spec: &SubmitSpec) -> Result<NormalizedSpec, AppError> {
    let prompt = if let Some(text) = &spec.prompt {
        text.clone()
    } else if let Some(path) = &spec.prompt_file {
        let resolved = if path.is_absolute() {
            path.clone()
        } else {
            spec.cwd.join(path)
        };
        fs::read_to_string(&resolved).map_err(|err| AppError::InvalidSpec {
            pointer: "/prompt_file".into(),
            value: json!(path),
            message: format!("failed to read prompt_file {}: {err}", resolved.display()),
        })?
    } else {
        return Err(AppError::InvalidSpec {
            pointer: "/prompt".into(),
            value: Value::Null,
            message: "exactly one of prompt or prompt_file is required".into(),
        });
    };

    let model = spec.agent().model;
    Ok(NormalizedSpec {
        api_version: API_VERSION,
        agent: spec.agent,
        model,
        thread: spec.thread,
        cwd: spec.cwd.clone(),
        prompt,
        timeout: spec.timeout,
        extra_args: spec.extra_args.clone(),
        report_trailer: spec.report_trailer,
    })
}

/// JSON Schema for the submit spec.
pub fn schema_json() -> Result<Value, AppError> {
    let schema = schemars::schema_for!(SubmitSpec);
    Ok(serde_json::to_value(&schema)?)
}

/// Require `cwd` to exist and be a directory.
pub fn check_cwd(cwd: &Path) -> Result<(), AppError> {
    match fs::metadata(cwd) {
        Ok(meta) if meta.is_dir() => Ok(()),
        Ok(_) => Err(AppError::CwdNotFound {
            path: cwd.to_path_buf(),
        }),
        Err(_) => Err(AppError::CwdNotFound {
            path: cwd.to_path_buf(),
        }),
    }
}

/// Example document from the plan, used in tests.
pub fn example_json() -> Value {
    json!({
        "api_version": 1,
        "agent": "claude",
        "model": "fable",
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "cwd": "/tmp",
        "prompt": "do the work",
        "timeout": "4h",
        "extra_args": ["--verbose"]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_spec() -> Value {
        json!({
            "api_version": 1,
            "agent": "claude",
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "cwd": "/tmp",
            "prompt": "hello"
        })
    }

    #[test]
    fn unknown_field_rejected() {
        let mut value = valid_spec();
        value["typo"] = json!(true);
        let err = parse_spec_value(value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => {
                assert!(pointer.contains("typo") || pointer.is_empty() || pointer == "/typo");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn both_prompt_and_file_rejected() {
        let mut value = valid_spec();
        value["prompt_file"] = json!("/tmp/p.txt");
        let err = parse_spec_value(value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/prompt"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn neither_prompt_rejected() {
        let mut value = valid_spec();
        value.as_object_mut().unwrap().remove("prompt");
        let err = parse_spec_value(value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/prompt"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn pointer_on_bad_thread() {
        let mut value = valid_spec();
        value["thread"] = json!("not-a-uuid");
        let err = parse_spec_value(value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/thread"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn relative_prompt_file_resolves_against_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("p.txt");
        fs::write(&prompt_path, "from-file").unwrap();
        let spec = SubmitSpec {
            api_version: 1,
            agent: AgentKind::Claude,
            model: None,
            thread: ThreadId::from_str_ok(),
            cwd: dir.path().to_path_buf(),
            prompt: None,
            prompt_file: Some(PathBuf::from("p.txt")),
            timeout: default_timeout(),
            extra_args: vec![],
            report_trailer: true,
        };
        let normalized = normalize(&spec).unwrap();
        assert_eq!(normalized.prompt, "from-file");
    }

    #[test]
    fn schema_contains_example_fields() {
        let schema = schema_json().unwrap();
        let example = example_json();
        parse_spec_value(example.clone()).unwrap();
        let props = schema
            .get("properties")
            .or_else(|| schema.pointer("/$defs/SubmitSpec/properties"))
            .or_else(|| schema.pointer("/definitions/SubmitSpec/properties"))
            .expect("schema properties");
        for key in ["api_version", "agent", "thread", "cwd"] {
            assert!(props.get(key).is_some(), "missing {key} in {schema}");
        }
        assert_eq!(example["api_version"], 1);
    }

    #[test]
    fn default_timeout_is_four_hours() {
        let spec = parse_spec_value(valid_spec()).unwrap();
        assert_eq!(spec.timeout, Duration::from_secs(4 * 3600));
        assert!(spec.report_trailer);
    }

    impl ThreadId {
        fn from_str_ok() -> Self {
            "01a0ab97-a7aa-7463-a5b0-8d500e40e431".parse().unwrap()
        }
    }
}
