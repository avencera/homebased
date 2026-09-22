//! Agent-specific CLI policy. Generic resolution lives in `invocation`.

use std::path::Path;

use crate::domain::AgentKind;
use crate::invocation::{ChildInvocation, StdinPolicy};

/// Inputs for one agent argv build. Callers supply a real working directory.
#[derive(Debug, Clone, Copy)]
pub struct AgentArgvInputs<'a> {
    /// Agent CLI.
    pub kind: AgentKind,
    /// Model alias, if any.
    pub model: Option<&'a str>,
    /// Working directory for the child.
    pub cwd: &'a Path,
    /// Extra argv appended after the unattended flags.
    pub extra_args: &'a [String],
}

/// Build unattended argv for an agent workload.
///
/// `prompt_feed` is always the evidence-path feed file. Codex and Claude read
/// it from stdin; Grok takes it as `--prompt-file`.
#[must_use]
pub fn build_agent_invocation(
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
    };
    append_extra_args(&mut args, inputs.extra_args, inputs.kind);
    ChildInvocation {
        program: binary.to_path_buf(),
        args,
        stdin,
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
    }
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
    fn feed_path_is_argv_only_for_grok() {
        let feed = Path::new("/state/tasks/id/prompt.feed.txt");
        for kind in [AgentKind::Codex, AgentKind::Claude, AgentKind::Grok] {
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
            }
        }
    }
}
