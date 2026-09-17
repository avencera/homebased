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
    args.extend(inputs.extra_args.iter().cloned());
    ChildInvocation {
        program: binary.to_path_buf(),
        args,
        stdin,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(kind: AgentKind, feed: &Path) -> ChildInvocation {
        let extra = vec!["--verbose".into()];
        build_agent_invocation(
            AgentArgvInputs {
                kind,
                model: Some("fable"),
                cwd: Path::new("/work"),
                extra_args: &extra,
            },
            Path::new("/bin/agent"),
            feed,
        )
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
                "--verbose",
            ]
        );
        assert_eq!(argv.stdin, StdinPolicy::PromptFeed);
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
