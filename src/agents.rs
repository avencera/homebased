//! Agent-specific CLI policy. Generic resolution lives in `invocation`.

use std::path::Path;

use jsonc_parser::{ParseOptions, parse_to_serde_value};
use serde_json::{Value, json};

use crate::domain::{AgentKind, TaskIdentity};
use crate::error::AppError;
use crate::invocation::{
    ChildEnvironment, ChildInvocation, GeneratedAgentOverlay, ManagedEnvironmentPolicy,
    ManagedEnvironmentPreview, StdinPolicy,
};

/// Inputs for one agent argv build. Callers supply a real working directory.
#[derive(Debug, Clone, Copy)]
pub struct AgentArgvInputs<'a> {
    /// Agent CLI.
    pub kind: AgentKind,
    /// Model id, if any.
    pub model: Option<&'a str>,
    /// Working directory for the child.
    pub cwd: &'a Path,
    /// Extra argv appended after the unattended flags.
    pub extra_args: &'a [String],
}

/// Build unattended argv for an agent workload.
///
/// `prompt_feed` is always the evidence-path feed file. Codex, Claude, and
/// OpenCode read it from stdin; Grok takes it as `--prompt-file`.
#[must_use]
pub fn build_agent_invocation(
    inputs: AgentArgvInputs<'_>,
    binary: &Path,
    prompt_feed: &Path,
) -> ChildInvocation {
    if inputs.kind == AgentKind::OpenCode {
        let identity = TaskIdentity::Preview;
        return build_opencode_invocation_with_config(
            inputs,
            binary,
            prompt_feed,
            identity,
            generated_opencode_config(&identity.opencode_agent_name()),
        );
    }
    build_standard_agent_invocation(inputs, binary, prompt_feed)
}

/// Build an agent invocation with the identity and inherited child policy.
pub fn build_agent_invocation_for_identity(
    inputs: AgentArgvInputs<'_>,
    binary: &Path,
    prompt_feed: &Path,
    identity: TaskIdentity,
    inherited_opencode_config: Option<&str>,
) -> Result<ChildInvocation, AppError> {
    if inputs.kind != AgentKind::OpenCode {
        return Ok(build_standard_agent_invocation(inputs, binary, prompt_feed));
    }

    let name = identity.opencode_agent_name();
    validate_opencode_extra_args(inputs.extra_args).map_err(|err| {
        AppError::AgentConfiguration {
            agent: AgentKind::OpenCode,
            message: err.to_string(),
        }
    })?;
    let config = compose_opencode_config(inherited_opencode_config, &name)?;
    Ok(build_opencode_invocation_with_config(
        inputs,
        binary,
        prompt_feed,
        identity,
        config,
    ))
}

fn build_standard_agent_invocation(
    inputs: AgentArgvInputs<'_>,
    binary: &Path,
    prompt_feed: &Path,
) -> ChildInvocation {
    let (mut args, stdin) = match inputs.kind {
        AgentKind::Codex => {
            let mut args = vec![
                "exec".into(),
                "-C".into(),
                inputs.cwd.to_string_lossy().into_owned(),
                "-s".into(),
                "danger-full-access".into(),
                "--dangerously-bypass-approvals-and-sandbox".into(),
            ];
            if let Some(model) = inputs.model {
                args.push("-m".into());
                args.push(model.to_string());
            }
            (args, StdinPolicy::PromptFeed)
        }
        AgentKind::Claude => {
            let mut args = vec!["-p".into()];
            if let Some(model) = inputs.model {
                args.push("--model".into());
                args.push(model.to_string());
            }
            args.push("--permission-mode".into());
            args.push("auto".into());
            args.push("--no-session-persistence".into());
            add_claude_output_defaults(&mut args, inputs.extra_args);
            (args, StdinPolicy::PromptFeed)
        }
        AgentKind::Grok => {
            let mut args = vec![
                "--cwd".into(),
                inputs.cwd.to_string_lossy().into_owned(),
                "--always-approve".into(),
                "--verbatim".into(),
                "--prompt-file".into(),
                prompt_feed.to_string_lossy().into_owned(),
            ];
            if let Some(model) = inputs.model {
                args.push("-m".into());
                args.push(model.to_string());
            }
            (args, StdinPolicy::Null)
        }
        AgentKind::OpenCode => unreachable!("OpenCode uses its managed invocation builder"),
    };
    append_extra_args(&mut args, inputs.extra_args, inputs.kind);
    ChildInvocation {
        program: binary.to_path_buf(),
        args,
        stdin,
        environment: ChildEnvironment::default(),
        managed_environment: None,
    }
}

fn build_opencode_invocation_with_config(
    inputs: AgentArgvInputs<'_>,
    binary: &Path,
    _prompt_feed: &Path,
    identity: TaskIdentity,
    config: OpenCodeConfig,
) -> ChildInvocation {
    let name = identity.opencode_agent_name();
    let mut args = vec![
        "run".into(),
        "--standalone".into(),
        "--agent".into(),
        name.clone(),
    ];
    add_opencode_output_defaults(&mut args, inputs.extra_args);
    args.push("--auto".into());
    if let Some(model) = inputs.model {
        args.push("--model".into());
        args.push(model.to_string());
    }
    append_extra_args(&mut args, inputs.extra_args, inputs.kind);

    let config_content = config.serialized();
    let environment = ChildEnvironment::from_pairs(vec![
        ("PWD".into(), inputs.cwd.to_string_lossy().into_owned()),
        ("OPENCODE_PERMISSION".into(), r#"{"*":"allow"}"#.into()),
        ("OPENCODE_CONFIG_CONTENT".into(), config_content),
    ]);
    let managed_environment = ManagedEnvironmentPreview {
        policy: ManagedEnvironmentPolicy::OpenCodeFullWorkPermissions,
        working_directory: inputs.cwd.to_path_buf(),
        wildcard_permission: "allow",
        generated_agent: GeneratedAgentOverlay {
            name,
            mode: "primary",
            permission: "allow",
        },
    };
    ChildInvocation {
        program: binary.to_path_buf(),
        args,
        stdin: StdinPolicy::PromptFeed,
        environment,
        managed_environment: Some(managed_environment),
    }
}

fn append_extra_args(args: &mut Vec<String>, extra_args: &[String], kind: AgentKind) {
    let managed_flags = managed_standalone_flags(kind);
    for extra_arg in extra_args {
        let is_managed = managed_flags.contains(&extra_arg.as_str());
        if is_managed && args.iter().any(|arg| arg == extra_arg) {
            continue;
        }
        args.push(extra_arg.clone());
    }
}

fn managed_standalone_flags(kind: AgentKind) -> &'static [&'static str] {
    match kind {
        AgentKind::Codex => &["--dangerously-bypass-approvals-and-sandbox"],
        AgentKind::Claude => &["-p", "--no-session-persistence", "--verbose"],
        AgentKind::Grok => &["--always-approve", "--verbatim"],
        AgentKind::OpenCode => &["--standalone", "--auto"],
    }
}

/// Validate OpenCode extra arguments before a task row is created.
pub(crate) fn validate_opencode_extra_args(
    extra_args: &[String],
) -> Result<(), OpenCodeExtraArgsError> {
    let mut pending_value = false;
    for (index, arg) in extra_args.iter().enumerate() {
        if pending_value && !arg.starts_with('-') {
            pending_value = false;
            continue;
        }
        pending_value = false;

        if arg == "--" {
            return Err(OpenCodeExtraArgsError {
                index,
                message: "the prompt must be supplied through the prompt feed".into(),
            });
        }
        if let Some(message) = forbidden_opencode_argument(arg) {
            return Err(OpenCodeExtraArgsError { index, message });
        }
        if !arg.starts_with('-') {
            return Err(OpenCodeExtraArgsError {
                index,
                message: "positional prompt arguments are controlled by the prompt feed".into(),
            });
        }
        if is_opencode_value_flag(arg) {
            pending_value = !arg.contains('=');
        } else if !arg.contains('=') {
            // Unknown flags may gain a separate value in a future OpenCode
            // release, so preserve that form without allowing a bare prompt.
            pending_value = true;
        }
    }
    Ok(())
}

/// Validation failure for one OpenCode extra argument.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub(crate) struct OpenCodeExtraArgsError {
    /// Index in `workload.extra_args`.
    pub(crate) index: usize,
    /// Safe contract failure detail.
    pub(crate) message: String,
}

fn forbidden_opencode_argument(arg: &str) -> Option<String> {
    let (flag, has_value) = arg
        .split_once('=')
        .map_or((arg, false), |(flag, _)| (flag, true));
    let short_attached = arg.len() > 2
        && !arg.starts_with("--")
        && matches!(arg.as_bytes().get(1), Some(b'c' | b's' | b'm'));
    let forbidden = matches!(
        flag,
        "--agent"
            | "--cwd"
            | "--dir"
            | "--directory"
            | "--server"
            | "--continue"
            | "--session"
            | "--fork"
            | "--model"
            | "-c"
            | "-s"
            | "-m"
    ) || short_attached
        || (flag == "--standalone" && has_value)
        || (flag == "--auto" && has_value);
    forbidden.then(|| match flag {
        "--agent" => "--agent is managed by homebased".into(),
        "--cwd" | "--dir" | "--directory" => "working directory is managed by homebased".into(),
        "--server" => "--server is not allowed; homebased requires --standalone".into(),
        "--continue" | "-c" => "session continuation is not allowed".into(),
        "--session" | "-s" => "session selection is not allowed".into(),
        "--fork" => "session forking is not allowed".into(),
        "--model" | "-m" => "--model is reserved for workload.model".into(),
        "--standalone" => "--standalone cannot be replaced".into(),
        "--auto" => "--auto cannot be disabled".into(),
        _ => "controlled OpenCode argument is not allowed".into(),
    })
}

fn is_opencode_value_flag(arg: &str) -> bool {
    let flag = arg.split_once('=').map_or(arg, |(flag, _)| flag);
    matches!(
        flag,
        "--format" | "--file" | "-f" | "--title" | "--log-level" | "--completions"
    )
}

fn add_opencode_output_defaults(args: &mut Vec<String>, extra_args: &[String]) {
    let explicit = extra_args
        .iter()
        .any(|arg| arg == "--format" || arg.starts_with("--format="));
    if !explicit {
        args.push("--format".into());
        args.push("json".into());
    }
}

const OPENCODE_PARSE_OPTIONS: ParseOptions = ParseOptions {
    allow_comments: true,
    allow_loose_object_property_names: false,
    allow_trailing_commas: true,
    allow_missing_commas: false,
    allow_single_quoted_strings: false,
    allow_hexadecimal_numbers: false,
    allow_unary_plus_numbers: false,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenCodeConfig {
    value: Value,
}

impl OpenCodeConfig {
    fn serialized(self) -> String {
        // `Value` can always be serialized. Keeping this method infallible
        // prevents provider configuration errors from reaching the runner.
        serde_json::to_string(&self.value).unwrap_or_else(|_| "{}".into())
    }
}

fn compose_opencode_config(
    inherited: Option<&str>,
    generated_agent: &str,
) -> Result<OpenCodeConfig, AppError> {
    let mut value = match inherited.filter(|content| !content.trim().is_empty()) {
        None => json!({}),
        Some(content) => {
            let parsed: Result<Value, _> = parse_to_serde_value(content, &OPENCODE_PARSE_OPTIONS);
            parsed.map_err(|err| AppError::AgentConfiguration {
                agent: AgentKind::OpenCode,
                message: format!("OPENCODE_CONFIG_CONTENT is invalid JSONC: {err}"),
            })?
        }
    };
    let Some(root) = value.as_object_mut() else {
        return Err(AppError::AgentConfiguration {
            agent: AgentKind::OpenCode,
            message: "OPENCODE_CONFIG_CONTENT must contain a JSON object".into(),
        });
    };
    let agents = root.entry("agent").or_insert_with(|| json!({}));
    let Some(agents) = agents.as_object_mut() else {
        return Err(AppError::AgentConfiguration {
            agent: AgentKind::OpenCode,
            message: "OPENCODE_CONFIG_CONTENT.agent must contain a JSON object".into(),
        });
    };
    if agents.contains_key(generated_agent) {
        return Err(AppError::AgentConfiguration {
            agent: AgentKind::OpenCode,
            message: format!("generated agent name already exists: {generated_agent}"),
        });
    }
    agents.insert(generated_agent.to_string(), generated_agent_value());
    Ok(OpenCodeConfig { value })
}

fn generated_opencode_config(generated_agent: &str) -> OpenCodeConfig {
    let mut root = serde_json::Map::new();
    let mut agents = serde_json::Map::new();
    agents.insert(generated_agent.to_string(), generated_agent_value());
    root.insert("agent".into(), Value::Object(agents));
    OpenCodeConfig {
        value: Value::Object(root),
    }
}

fn generated_agent_value() -> Value {
    json!({
        "mode": "primary",
        "permission": "allow"
    })
}

/// Claude live output needs `stream-json` plus `--verbose`. Extra args are
/// inspected here and filtered when they are appended later, so an explicit
/// format is not duplicated.
fn add_claude_output_defaults(args: &mut Vec<String>, extra_args: &[String]) {
    let mut output_format = OutputFormatArg::Absent;
    let mut index = 0;
    while index < extra_args.len() {
        let arg = &extra_args[index];
        if let Some(value) = arg.strip_prefix("--output-format=") {
            output_format = OutputFormatArg::Value(value);
        } else if arg == "--output-format" {
            output_format = match extra_args.get(index + 1) {
                Some(value) => OutputFormatArg::Value(value),
                None => OutputFormatArg::MissingValue,
            };
            index += 1;
        }
        index += 1;
    }
    if output_format == OutputFormatArg::Absent {
        args.push("--output-format".into());
        args.push("stream-json".into());
    }
    if matches!(
        output_format,
        OutputFormatArg::Absent | OutputFormatArg::Value("stream-json")
    ) && !extra_args.iter().any(|arg| arg == "--verbose")
    {
        args.push("--verbose".into());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormatArg<'a> {
    Absent,
    MissingValue,
    Value(&'a str),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn build(kind: AgentKind, feed: &Path) -> ChildInvocation {
        let extra = vec!["--verbose".into()];
        build_with_extra(kind, Some("fable"), feed, &extra)
    }

    fn build_with_extra(
        kind: AgentKind,
        model: Option<&str>,
        feed: &Path,
        extra_args: &[String],
    ) -> ChildInvocation {
        build_agent_invocation(
            AgentArgvInputs {
                kind,
                model,
                cwd: Path::new("/work"),
                extra_args,
            },
            Path::new("/bin/agent"),
            feed,
        )
    }

    fn claude_with_extra(extra: &[String]) -> ChildInvocation {
        build_with_extra(
            AgentKind::Claude,
            None,
            Path::new("/state/tasks/id/prompt.feed.txt"),
            extra,
        )
    }

    fn count_arg(args: &[String], flag: &str) -> usize {
        args.iter().filter(|arg| *arg == flag).count()
    }

    fn opencode_with(
        model: Option<&str>,
        extra_args: &[String],
        identity: TaskIdentity,
        inherited: Option<&str>,
    ) -> Result<ChildInvocation, AppError> {
        build_agent_invocation_for_identity(
            AgentArgvInputs {
                kind: AgentKind::OpenCode,
                model,
                cwd: Path::new("/work"),
                extra_args,
            },
            Path::new("/bin/opencode"),
            Path::new("/state/tasks/id/prompt.feed.txt"),
            identity,
            inherited,
        )
    }

    fn environment_map(invocation: &ChildInvocation) -> BTreeMap<&str, &str> {
        invocation.environment.iter().collect()
    }

    #[test]
    fn claude_argv() {
        let argv = build(
            AgentKind::Claude,
            Path::new("/state/tasks/id/prompt.feed.txt"),
        );
        assert_eq!(
            argv.to_vec(),
            vec![
                "/bin/agent",
                "-p",
                "--model",
                "fable",
                "--permission-mode",
                "auto",
                "--no-session-persistence",
                "--output-format",
                "stream-json",
                "--verbose",
            ]
        );
        assert_eq!(argv.stdin, StdinPolicy::PromptFeed);
    }

    #[test]
    fn claude_defaults_to_streaming_json_and_verbose() {
        let args = claude_with_extra(&[]).to_vec();
        assert_eq!(
            args,
            vec![
                "/bin/agent",
                "-p",
                "--permission-mode",
                "auto",
                "--no-session-persistence",
                "--output-format",
                "stream-json",
                "--verbose",
            ]
        );
    }

    #[test]
    fn claude_keeps_a_single_verbose_when_extra_args_already_has_it() {
        let extra = vec!["--verbose".into()];
        let args = claude_with_extra(&extra).to_vec();
        assert_eq!(count_arg(&args, "--output-format"), 1);
        assert_eq!(count_arg(&args, "stream-json"), 1);
        assert_eq!(count_arg(&args, "--verbose"), 1);
    }

    #[test]
    fn claude_keeps_the_first_verbose_when_extra_args_repeat_it() {
        let extra = vec![
            "--custom-before".into(),
            "one".into(),
            "--verbose".into(),
            "--custom-after".into(),
            "two".into(),
            "--verbose".into(),
        ];
        let args = claude_with_extra(&extra).to_vec();
        let expected_extra: Vec<String> = [
            "--custom-before",
            "one",
            "--verbose",
            "--custom-after",
            "two",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        assert!(args.ends_with(&expected_extra));
    }

    #[test]
    fn managed_standalone_flags_are_emitted_once() {
        let cases = [
            (
                AgentKind::Codex,
                vec![
                    "--dangerously-bypass-approvals-and-sandbox".into(),
                    "--dangerously-bypass-approvals-and-sandbox".into(),
                ],
                vec!["--dangerously-bypass-approvals-and-sandbox"],
            ),
            (
                AgentKind::Claude,
                vec![
                    "-p".into(),
                    "-p".into(),
                    "--no-session-persistence".into(),
                    "--no-session-persistence".into(),
                    "--verbose".into(),
                    "--verbose".into(),
                ],
                vec!["-p", "--no-session-persistence", "--verbose"],
            ),
            (
                AgentKind::Grok,
                vec![
                    "--always-approve".into(),
                    "--always-approve".into(),
                    "--verbatim".into(),
                    "--verbatim".into(),
                ],
                vec!["--always-approve", "--verbatim"],
            ),
        ];

        for (kind, extra, managed_flags) in cases {
            let args = build_with_extra(
                kind,
                None,
                Path::new("/state/tasks/id/prompt.feed.txt"),
                &extra,
            )
            .to_vec();
            for flag in managed_flags {
                assert_eq!(
                    count_arg(&args, flag),
                    1,
                    "{kind:?} should emit {flag} once"
                );
            }
        }
    }

    #[test]
    fn grok_omits_managed_duplicates_and_keeps_other_extra_args() {
        let extra = vec![
            "--custom-before".into(),
            "one".into(),
            "--always-approve".into(),
            "--cwd".into(),
            "/extra-cwd".into(),
            "--always-approve".into(),
            "--model".into(),
            "extra-model".into(),
            "-m".into(),
            "short-model".into(),
            "-C".into(),
            "/extra-C".into(),
            "-s".into(),
            "extra-sandbox".into(),
            "--permission-mode".into(),
            "manual".into(),
            "--prompt-file".into(),
            "/extra.prompt".into(),
            "--output-format".into(),
            "json".into(),
            "--custom".into(),
            "value".into(),
            "--custom".into(),
            "value-2".into(),
            "--verbatim".into(),
            "--verbatim".into(),
        ];
        let args = build_with_extra(
            AgentKind::Grok,
            None,
            Path::new("/state/tasks/id/prompt.feed.txt"),
            &extra,
        )
        .to_vec();
        let expected_extra: Vec<String> = [
            "--custom-before",
            "one",
            "--cwd",
            "/extra-cwd",
            "--model",
            "extra-model",
            "-m",
            "short-model",
            "-C",
            "/extra-C",
            "-s",
            "extra-sandbox",
            "--permission-mode",
            "manual",
            "--prompt-file",
            "/extra.prompt",
            "--output-format",
            "json",
            "--custom",
            "value",
            "--custom",
            "value-2",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        assert!(args.ends_with(&expected_extra));
    }

    #[test]
    fn claude_explicit_output_format_replaces_streaming_default() {
        let extra = vec!["--output-format=json".into()];
        let args = claude_with_extra(&extra).to_vec();
        assert!(args.contains(&"--output-format=json".to_string()));
        assert!(!args.contains(&"stream-json".to_string()));
        assert!(!args.contains(&"--verbose".to_string()));
        assert_eq!(count_arg(&args, "--output-format"), 0);
    }

    #[test]
    fn claude_explicit_space_separated_json_skips_streaming_defaults() {
        let extra = vec!["--output-format".into(), "json".into()];
        let args = claude_with_extra(&extra).to_vec();
        assert_eq!(count_arg(&args, "--output-format"), 1);
        assert!(!args.contains(&"stream-json".to_string()));
        assert!(!args.contains(&"--verbose".to_string()));
    }

    #[test]
    fn claude_explicit_streaming_format_gets_required_verbose_flag() {
        let extra = vec!["--output-format".into(), "stream-json".into()];
        let args = claude_with_extra(&extra).to_vec();
        assert_eq!(count_arg(&args, "--output-format"), 1);
        assert_eq!(count_arg(&args, "stream-json"), 1);
        assert_eq!(count_arg(&args, "--verbose"), 1);
    }

    #[test]
    fn claude_equals_streaming_format_gets_a_single_verbose_flag() {
        let extra = vec!["--output-format=stream-json".into()];
        let args = claude_with_extra(&extra).to_vec();
        assert!(args.contains(&"--output-format=stream-json".to_string()));
        assert_eq!(count_arg(&args, "--output-format"), 0);
        assert_eq!(count_arg(&args, "--verbose"), 1);
    }

    #[test]
    fn claude_uses_the_last_explicit_output_format() {
        let extra = vec![
            "--output-format=json".into(),
            "--output-format".into(),
            "stream-json".into(),
        ];
        let args = claude_with_extra(&extra).to_vec();
        assert_eq!(count_arg(&args, "--verbose"), 1);
        assert_eq!(count_arg(&args, "--output-format"), 1);
        assert_eq!(count_arg(&args, "--output-format=json"), 1);
    }

    #[test]
    fn codex_argv() {
        let argv = build(
            AgentKind::Codex,
            Path::new("/state/tasks/id/prompt.feed.txt"),
        );
        let v = argv.to_vec();
        assert_eq!(v[0], "/bin/agent");
        assert_eq!(v[1], "exec");
        assert!(v.contains(&"--dangerously-bypass-approvals-and-sandbox".to_string()));
        assert!(v.contains(&"-C".to_string()));
        assert!(v.contains(&"/work".to_string()));
        assert!(!v.iter().any(|a| a.contains("prompt.feed")));
        assert_eq!(argv.stdin, StdinPolicy::PromptFeed);
    }

    #[test]
    fn grok_argv_uses_prompt_feed_not_dash_p() {
        let feed = Path::new("/state/tasks/id/prompt.feed.txt");
        let argv = build(AgentKind::Grok, feed);
        let v = argv.to_vec();
        assert!(!v.iter().any(|a| a == "-p"));
        assert!(v.contains(&"--always-approve".to_string()));
        assert!(v.contains(&"--verbatim".to_string()));
        assert!(v.contains(&"--prompt-file".to_string()));
        assert!(v.contains(&feed.to_string_lossy().into_owned()));
        assert_eq!(argv.stdin, StdinPolicy::Null);
    }

    #[test]
    fn opencode_argv_uses_stdin_and_provider_qualified_model() {
        let argv = opencode_with(
            Some("zai-coding-plan/glm-5.3-flash"),
            &[],
            TaskIdentity::Actual(TaskIdentityTest::id()),
            None,
        )
        .unwrap();
        assert_eq!(
            argv.to_vec(),
            vec![
                "/bin/opencode",
                "run",
                "--standalone",
                "--agent",
                "homebased-01a0ab97-a7aa-7463-a5b0-8d500e40e431",
                "--format",
                "json",
                "--auto",
                "--model",
                "zai-coding-plan/glm-5.3-flash",
            ]
        );
        assert_eq!(argv.stdin, StdinPolicy::PromptFeed);
        assert!(!argv.to_vec().iter().any(|arg| arg.contains("prompt.feed")));
    }

    #[test]
    fn opencode_accepts_variant_and_omits_model_when_unset() {
        let with_variant = opencode_with(
            Some("other/provider#fast"),
            &[],
            TaskIdentity::Preview,
            None,
        )
        .unwrap();
        assert!(
            with_variant
                .to_vec()
                .windows(2)
                .any(|pair| pair == ["--model", "other/provider#fast"])
        );

        let without_model = opencode_with(None, &[], TaskIdentity::Preview, None).unwrap();
        assert!(!without_model.to_vec().iter().any(|arg| arg == "--model"));
    }

    #[test]
    fn opencode_keeps_explicit_output_format_and_managed_flags_once() {
        let extra = vec![
            "--standalone".into(),
            "--auto".into(),
            "--format=json".into(),
            "--future".into(),
            "value".into(),
        ];
        let argv = opencode_with(Some("model"), &extra, TaskIdentity::Preview, None).unwrap();
        let args = argv.to_vec();
        assert_eq!(count_arg(&args, "--standalone"), 1);
        assert_eq!(count_arg(&args, "--auto"), 1);
        assert_eq!(count_arg(&args, "--format=json"), 1);
        assert!(!args.contains(&"json".into()));
        assert!(args.ends_with(&["--format=json".into(), "--future".into(), "value".into()]));
    }

    #[test]
    fn opencode_explicit_space_separated_output_format_is_not_duplicated() {
        let extra = vec!["--format".into(), "default".into()];
        let args = opencode_with(None, &extra, TaskIdentity::Preview, None)
            .unwrap()
            .to_vec();
        assert_eq!(count_arg(&args, "--format"), 1);
        assert!(!args.contains(&"json".into()));
    }

    #[test]
    fn opencode_managed_environment_is_scoped_and_redacted() {
        let inherited = r#"{
            // provider settings stay in the child config
            "provider": {"zai": {"apiKey": "do-not-print", "model": "glm-5.3-flash"}},
            "permission": "deny",
            "agent": {"build": {"permission": {"shell": "deny"}}}
        }"#;
        let invocation = opencode_with(None, &[], TaskIdentity::Preview, Some(inherited)).unwrap();
        let env = environment_map(&invocation);
        assert_eq!(env.get("PWD"), Some(&"/work"));
        assert_eq!(env.get("OPENCODE_PERMISSION"), Some(&r#"{"*":"allow"}"#));
        let config: Value = serde_json::from_str(env["OPENCODE_CONFIG_CONTENT"]).unwrap();
        assert_eq!(config["provider"]["zai"]["apiKey"], "do-not-print");
        assert_eq!(config["provider"]["zai"]["model"], "glm-5.3-flash");
        assert_eq!(config["permission"], "deny");
        assert_eq!(config["agent"]["build"]["permission"]["shell"], "deny");
        assert_eq!(
            config["agent"]["homebased-<task-id>"],
            json!({"mode": "primary", "permission": "allow"})
        );
        let preview = invocation.managed_environment.as_ref().unwrap();
        assert_eq!(preview.generated_agent.name, "homebased-<task-id>");
        let debug = format!("{invocation:?}");
        assert!(!debug.contains("do-not-print"), "{debug}");
        let serialized = serde_json::to_string(&invocation).unwrap();
        assert!(!serialized.contains("do-not-print"), "{serialized}");
        assert!(
            !serialized.contains("OPENCODE_CONFIG_CONTENT"),
            "{serialized}"
        );
    }

    #[test]
    fn opencode_config_rejects_malformed_and_colliding_content() {
        let malformed = opencode_with(
            None,
            &[],
            TaskIdentity::Preview,
            Some(r#"{"provider":{"zai":{"apiKey":"never-print}}"#),
        )
        .unwrap_err();
        assert!(matches!(
            malformed,
            AppError::AgentConfiguration { ref message, .. }
                if message.contains("invalid JSONC")
        ));
        assert!(!malformed.to_string().contains("never-print"));

        let collision = opencode_with(
            None,
            &[],
            TaskIdentity::Preview,
            Some(r#"{"agent":{"homebased-<task-id>":{}}}"#),
        )
        .unwrap_err();
        assert!(matches!(
            collision,
            AppError::AgentConfiguration { ref message, .. }
                if message.contains("already exists")
        ));
    }

    #[test]
    fn opencode_extra_args_reject_controlled_inputs() {
        for arg in [
            "--agent=other",
            "--cwd=/other",
            "--dir=/other",
            "--server=http://localhost",
            "--continue",
            "-c",
            "--session=ses_123",
            "-s=ses_123",
            "--fork",
            "--model=other/model",
            "-m=other/model",
            "--standalone=false",
            "--auto=false",
            "prompt supplied in argv",
        ] {
            let args = vec![arg.to_string()];
            assert!(validate_opencode_extra_args(&args).is_err(), "{arg}");
        }
    }

    struct TaskIdentityTest;

    impl TaskIdentityTest {
        fn id() -> crate::domain::TaskId {
            "01a0ab97-a7aa-7463-a5b0-8d500e40e431".parse().unwrap()
        }
    }

    #[test]
    fn feed_path_is_argv_only_for_grok() {
        let feed = Path::new("/state/tasks/id/prompt.feed.txt");
        for kind in [
            AgentKind::Codex,
            AgentKind::Claude,
            AgentKind::Grok,
            AgentKind::OpenCode,
        ] {
            let argv = build_agent_invocation(
                AgentArgvInputs {
                    kind,
                    model: None,
                    cwd: Path::new("/work"),
                    extra_args: &[],
                },
                Path::new("/bin/agent"),
                feed,
            );
            match kind {
                AgentKind::Grok => {
                    assert!(argv.to_vec().contains(&feed.to_string_lossy().into_owned()));
                    assert_eq!(argv.stdin, StdinPolicy::Null);
                }
                AgentKind::Codex | AgentKind::Claude => {
                    assert_eq!(argv.stdin, StdinPolicy::PromptFeed);
                    assert!(!argv.to_vec().iter().any(|a| a.contains("prompt.feed")));
                }
                AgentKind::OpenCode => {
                    assert_eq!(argv.stdin, StdinPolicy::PromptFeed);
                    assert!(!argv.to_vec().iter().any(|a| a.contains("prompt.feed")));
                }
            }
        }
    }
}
