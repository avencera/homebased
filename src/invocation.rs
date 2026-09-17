//! Validated command lines and resolved child invocations.

use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::agents::{AgentArgvInputs, build_agent_invocation};
use crate::domain::{AgentKind, AgentWorkload, TaskWorkload, Workload};
use crate::error::AppError;
use crate::spec::{NormalizedAgentWorkload, NormalizedWorkload};

/// How the child receives stdin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StdinPolicy {
    /// `/dev/null`.
    Null,
    /// Prompt feed bytes are written to a pipe.
    PromptFeed,
}

/// Validated argv: non-empty program, no NUL bytes, later args may be empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLine {
    program: String,
    args: Vec<String>,
}

impl CommandLine {
    /// Build from an argv array. Index 0 is the program.
    pub fn try_from_argv(argv: Vec<String>) -> Result<Self, CommandLineError> {
        if argv.is_empty() {
            return Err(CommandLineError::Empty);
        }
        for (index, part) in argv.iter().enumerate() {
            if part.contains('\0') {
                return Err(CommandLineError::Nul { index });
            }
        }
        let mut iter = argv.into_iter();
        let Some(program) = iter.next() else {
            return Err(CommandLineError::Empty);
        };
        if program.is_empty() {
            return Err(CommandLineError::EmptyProgram);
        }
        Ok(Self {
            program,
            args: iter.collect(),
        })
    }

    /// Program at index 0.
    #[must_use]
    pub fn program(&self) -> &str {
        &self.program
    }

    /// Arguments after the program.
    #[must_use]
    pub fn args(&self) -> &[String] {
        &self.args
    }

    /// Full argv including the program.
    #[must_use]
    pub fn to_vec(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(1 + self.args.len());
        out.push(self.program.clone());
        out.extend(self.args.iter().cloned());
        out
    }
}

impl Serialize for CommandLine {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_vec().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CommandLine {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CommandLineVisitor;

        impl<'de> Visitor<'de> for CommandLineVisitor {
            type Value = CommandLine;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a non-empty argv array")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut argv = Vec::new();
                while let Some(item) = seq.next_element::<String>()? {
                    argv.push(item);
                }
                CommandLine::try_from_argv(argv).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_seq(CommandLineVisitor)
    }
}

impl schemars::JsonSchema for CommandLine {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CommandLine".into()
    }

    /// Mirrors `try_from_argv`: `prefixItems` carries the non-empty program
    /// rule that `items` alone cannot express, so the published schema rejects
    /// exactly what the parser rejects.
    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "array",
            "minItems": 1,
            "prefixItems": [
                {
                    "type": "string",
                    "minLength": 1,
                    "description": "Program to run. Must not be empty."
                }
            ],
            "items": { "type": "string" },
            "description": "Argv array. Index 0 is a non-empty program. Later elements may be empty. No element may contain NUL."
        })
    }
}

/// Why a command line failed validation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommandLineError {
    /// Empty argv.
    #[error("command must be a non-empty array")]
    Empty,
    /// Program at index 0 is empty.
    #[error("command[0] must be a non-empty program")]
    EmptyProgram,
    /// An element contains a NUL byte.
    #[error("command[{index}] must not contain a NUL byte")]
    Nul {
        /// Index of the bad element.
        index: usize,
    },
}

impl CommandLineError {
    /// JSON pointer under `/workload/command`.
    #[must_use]
    pub fn pointer(&self) -> String {
        match self {
            Self::Empty => "/workload/command".into(),
            Self::EmptyProgram | Self::Nul { index: 0 } => "/workload/command/0".into(),
            Self::Nul { index } => format!("/workload/command/{index}"),
        }
    }

    /// Convert to `AppError::InvalidSpec` with the raw value.
    #[must_use]
    pub fn into_invalid_spec(self, value: Value) -> AppError {
        AppError::InvalidSpec {
            pointer: self.pointer(),
            value,
            message: self.to_string(),
        }
    }
}

/// Resolved program, arguments, and stdin policy for the runner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChildInvocation {
    /// Absolute executable path.
    pub program: PathBuf,
    /// Arguments not including argv0.
    pub args: Vec<String>,
    /// Stdin policy.
    pub stdin: StdinPolicy,
}

impl ChildInvocation {
    /// Full argv including the program.
    #[must_use]
    pub fn to_vec(&self) -> Vec<String> {
        let mut out = vec![self.program.to_string_lossy().into_owned()];
        out.extend(self.args.iter().cloned());
        out
    }
}

/// Resolve a program path against captured `PATH` and task `cwd`.
pub fn resolve_executable(program: &str, path: &str, cwd: &Path) -> Result<PathBuf, AppError> {
    let candidate = PathBuf::from(program);
    let resolved = if candidate.is_absolute() {
        candidate
    } else if program.contains('/') {
        cwd.join(program)
    } else {
        which::which_in(program, Some(OsString::from(path)), cwd).map_err(|_| {
            AppError::ExecutableMissing {
                program: program.to_string(),
            }
        })?
    };
    require_executable_file(&resolved, program)
}

fn require_executable_file(path: &Path, requested: &str) -> Result<PathBuf, AppError> {
    let meta = std::fs::metadata(path).map_err(|_| AppError::ExecutableMissing {
        program: requested.to_string(),
    })?;
    if !meta.is_file() {
        return Err(AppError::ExecutableMissing {
            program: requested.to_string(),
        });
    }
    // executable bit for owner, group, or other
    if meta.permissions().mode() & 0o111 == 0 {
        return Err(AppError::ExecutableMissing {
            program: requested.to_string(),
        });
    }
    Ok(path.to_path_buf())
}

/// Resolve an agent binary: `HOMEBASED_<AGENT>` then `which` on `path`.
pub fn resolve_agent_binary(kind: AgentKind, path: &str, cwd: &Path) -> Result<PathBuf, AppError> {
    let pinned = std::env::var(kind.binary_env()).ok();
    resolve_agent_binary_with(kind, pinned.as_deref(), path, cwd)
}

/// Agent resolution with the override supplied instead of read from the
/// environment. A pin must itself be an executable file, exactly as a task
/// program must: silently falling back to whatever `PATH` finds would run a
/// different binary than the operator asked for.
fn resolve_agent_binary_with(
    kind: AgentKind,
    pinned: Option<&str>,
    path: &str,
    cwd: &Path,
) -> Result<PathBuf, AppError> {
    if let Some(pinned) = pinned.filter(|value| !value.is_empty()) {
        return require_executable_file(Path::new(pinned), pinned);
    }
    which::which_in(kind.binary_name(), Some(OsString::from(path)), cwd).map_err(|_| {
        AppError::ExecutableMissing {
            program: kind.binary_name().to_string(),
        }
    })
}

/// Build a child invocation from a normalized workload.
///
/// Agent workloads always receive a prompt-feed path; the agent policy decides
/// whether that path is an argv argument or only a stdin source.
pub fn invocation_from_normalized(
    workload: &NormalizedWorkload,
    env_path: &str,
    cwd: &Path,
    prompt_feed: Option<&Path>,
) -> Result<ChildInvocation, AppError> {
    match workload {
        NormalizedWorkload::Agent(agent) => {
            let binary = resolve_agent_binary(agent.agent, env_path, cwd)?;
            let feed = prompt_feed.ok_or_else(|| AppError::Internal {
                message: "agent invocation requires a prompt feed path".into(),
            })?;
            Ok(build_agent_invocation(
                AgentArgvInputs {
                    kind: agent.agent,
                    model: agent.model.as_deref(),
                    cwd,
                    extra_args: &agent.extra_args,
                },
                &binary,
                feed,
            ))
        }
        NormalizedWorkload::Task(task) => {
            let binary = resolve_executable(task.command.program(), env_path, cwd)?;
            Ok(ChildInvocation {
                program: binary,
                args: task.command.args().to_vec(),
                stdin: StdinPolicy::Null,
            })
        }
    }
}

/// Build a child invocation from a persisted workload and resolved binary.
///
/// `agent_prompt_feed` is the task evidence feed path. Agent policy consumes it;
/// task workloads ignore it.
pub fn invocation_from_workload(
    workload: &Workload,
    binary: &Path,
    cwd: &Path,
    agent_prompt_feed: &Path,
) -> ChildInvocation {
    match workload {
        Workload::Agent(agent) => build_agent_invocation(
            AgentArgvInputs {
                kind: agent.agent.kind,
                model: agent.agent.model.as_deref(),
                cwd,
                extra_args: &agent.extra_args,
            },
            binary,
            agent_prompt_feed,
        ),
        Workload::Task(task) => ChildInvocation {
            program: binary.to_path_buf(),
            args: task.command.args().to_vec(),
            stdin: StdinPolicy::Null,
        },
    }
}

/// Resolve the executable for a normalized workload without building argv.
pub fn resolve_workload_binary(
    workload: &NormalizedWorkload,
    env_path: &str,
    cwd: &Path,
) -> Result<PathBuf, AppError> {
    match workload {
        NormalizedWorkload::Agent(agent) => resolve_agent_binary(agent.agent, env_path, cwd),
        NormalizedWorkload::Task(task) => resolve_executable(task.command.program(), env_path, cwd),
    }
}

/// Convert a normalized agent workload into the persisted form.
#[must_use]
pub fn persist_agent_workload(agent: &NormalizedAgentWorkload) -> AgentWorkload {
    AgentWorkload {
        agent: crate::domain::Agent::new(agent.agent, agent.model.clone()),
        extra_args: agent.extra_args.clone(),
        report_trailer: agent.report_trailer,
    }
}

/// Convert a normalized task workload into the persisted form.
#[must_use]
pub fn persist_task_workload(task: &crate::spec::NormalizedTaskWorkload) -> TaskWorkload {
    TaskWorkload {
        command: task.command.clone(),
    }
}

/// Convert a normalized workload into the persisted form.
#[must_use]
pub fn persist_workload(workload: &NormalizedWorkload) -> Workload {
    match workload {
        NormalizedWorkload::Agent(agent) => Workload::Agent(persist_agent_workload(agent)),
        NormalizedWorkload::Task(task) => Workload::Task(persist_task_workload(task)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    #[test]
    fn command_rejects_empty_argv() {
        let err = CommandLine::try_from_argv(vec![]).unwrap_err();
        assert_eq!(err, CommandLineError::Empty);
    }

    #[test]
    fn command_rejects_empty_program() {
        let err = CommandLine::try_from_argv(vec![String::new(), "a".into()]).unwrap_err();
        assert_eq!(err, CommandLineError::EmptyProgram);
    }

    #[test]
    fn command_allows_empty_later_args() {
        let cmd =
            CommandLine::try_from_argv(vec!["echo".into(), String::new(), " ".into()]).unwrap();
        assert_eq!(cmd.program(), "echo");
        assert_eq!(cmd.args(), &["", " "]);
    }

    #[test]
    fn command_rejects_nul() {
        let err = CommandLine::try_from_argv(vec!["ok".into(), "a\0b".into()]).unwrap_err();
        assert_eq!(err, CommandLineError::Nul { index: 1 });
    }

    #[test]
    fn command_round_trips_json() {
        let cmd = CommandLine::try_from_argv(vec!["cargo".into(), "build".into()]).unwrap();
        let json = serde_json::to_string(&cmd).unwrap();
        let back: CommandLine = serde_json::from_str(&json).unwrap();
        assert_eq!(back, cmd);
    }

    #[test]
    fn deserialize_rejects_empty_program() {
        let err = serde_json::from_str::<CommandLine>(r#"[""]"#).unwrap_err();
        assert!(err.to_string().contains("non-empty program"));
    }

    #[test]
    fn resolve_absolute_and_bare() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("tool");
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o755)
            .open(&bin)
            .unwrap();
        let got = resolve_executable(bin.to_str().unwrap(), "", dir.path()).unwrap();
        assert_eq!(got, bin);

        let path = format!("{}:/nope", dir.path().display());
        let got = resolve_executable("tool", &path, dir.path()).unwrap();
        assert_eq!(got, bin);
    }

    #[test]
    fn resolve_relative_against_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("bin");
        std::fs::create_dir(&nested).unwrap();
        let bin = nested.join("tool");
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o755)
            .open(&bin)
            .unwrap();
        let got = resolve_executable("bin/tool", "", dir.path()).unwrap();
        assert_eq!(got, bin);
    }

    #[test]
    fn agent_override_must_be_executable() {
        let dir = tempfile::tempdir().unwrap();
        let pinned = dir.path().join("claude");
        std::fs::write(&pinned, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = std::fs::metadata(&pinned).unwrap().permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&pinned, perms).unwrap();

        let pinned_s = pinned.to_string_lossy().into_owned();
        let err = resolve_agent_binary_with(
            AgentKind::Claude,
            Some(pinned_s.as_str()),
            "/nope",
            dir.path(),
        )
        .unwrap_err();
        assert!(
            matches!(err, AppError::ExecutableMissing { ref program } if *program == pinned_s),
            "a non-executable pin must not fall back to PATH: {err:?}"
        );

        let mut perms = std::fs::metadata(&pinned).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&pinned, perms).unwrap();
        let got = resolve_agent_binary_with(
            AgentKind::Claude,
            Some(pinned_s.as_str()),
            "/nope",
            dir.path(),
        )
        .unwrap();
        assert_eq!(got, pinned);
    }

    #[test]
    fn agent_override_directory_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let as_dir = dir.path().to_string_lossy().into_owned();
        let err =
            resolve_agent_binary_with(AgentKind::Grok, Some(as_dir.as_str()), "/nope", dir.path())
                .unwrap_err();
        assert!(matches!(err, AppError::ExecutableMissing { .. }), "{err:?}");
    }

    #[test]
    fn empty_agent_override_falls_back_to_path() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("codex");
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o755)
            .open(&bin)
            .unwrap();
        let path = dir.path().to_string_lossy().into_owned();
        let got = resolve_agent_binary_with(AgentKind::Codex, Some(""), &path, dir.path()).unwrap();
        assert_eq!(got, bin);
    }

    #[test]
    fn missing_executable() {
        let err = resolve_executable("no-such-tool-xyz", "/nope", Path::new("/tmp")).unwrap_err();
        assert!(matches!(
            err,
            AppError::ExecutableMissing { program } if program == "no-such-tool-xyz"
        ));
    }
}
