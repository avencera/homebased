//! Child argv builders and binary resolution.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::domain::AgentKind;
use crate::error::AppError;
use crate::spec::NormalizedSpec;

/// Resolved child invocation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ChildArgv {
    /// Absolute program path.
    pub program: PathBuf,
    /// Arguments not including argv0.
    pub args: Vec<String>,
    /// Whether the prompt is written to stdin.
    pub stdin_prompt: bool,
}

impl ChildArgv {
    /// Full argv including the program.
    #[must_use]
    pub fn to_vec(&self) -> Vec<String> {
        let mut out = vec![self.program.to_string_lossy().into_owned()];
        out.extend(self.args.iter().cloned());
        out
    }
}

/// Resolve the agent binary: `HOMEBASED_<AGENT>` then `which` on `path`.
pub fn resolve_binary(kind: AgentKind, path: &str, cwd: &Path) -> Result<PathBuf, AppError> {
    if let Ok(override_path) = std::env::var(kind.binary_env()) {
        if !override_path.is_empty() {
            let p = PathBuf::from(&override_path);
            if p.is_file() {
                return Ok(p);
            }
        }
    }
    which::which_in(kind.binary_name(), Some(OsString::from(path)), cwd)
        .map_err(|_| AppError::AgentBinaryMissing { agent: kind })
}

/// Build unattended argv for a normalized spec.
pub fn build_argv(spec: &NormalizedSpec, binary: &Path, prompt_file: Option<&Path>) -> ChildArgv {
    let (mut args, stdin_prompt) = match spec.agent {
        AgentKind::Codex => {
            let mut args = vec![
                "exec".into(),
                "-C".into(),
                spec.cwd.to_string_lossy().into_owned(),
                "-s".into(),
                "danger-full-access".into(),
                "--dangerously-bypass-approvals-and-sandbox".into(),
            ];
            if let Some(model) = &spec.model {
                args.push("-m".into());
                args.push(model.clone());
            }
            (args, true)
        }
        AgentKind::Claude => {
            let mut args = vec!["-p".into()];
            if let Some(model) = &spec.model {
                args.push("--model".into());
                args.push(model.clone());
            }
            args.push("--permission-mode".into());
            args.push("auto".into());
            args.push("--no-session-persistence".into());
            (args, true)
        }
        AgentKind::Grok => {
            let file = prompt_file
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| "prompt.txt".into());
            let mut args = vec![
                "--cwd".into(),
                spec.cwd.to_string_lossy().into_owned(),
                "--always-approve".into(),
                "--verbatim".into(),
                "--prompt-file".into(),
                file,
            ];
            if let Some(model) = &spec.model {
                args.push("-m".into());
                args.push(model.clone());
            }
            (args, false)
        }
    };
    args.extend(spec.extra_args.iter().cloned());
    ChildArgv {
        program: binary.to_path_buf(),
        args,
        stdin_prompt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ThreadId;
    use std::str::FromStr;
    use std::time::Duration;

    fn spec(agent: AgentKind) -> NormalizedSpec {
        NormalizedSpec {
            api_version: 1,
            agent,
            model: Some("fable".into()),
            thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
            cwd: PathBuf::from("/work"),
            prompt: "hi".into(),
            timeout: Duration::from_secs(4),
            extra_args: vec!["--verbose".into()],
            report_trailer: true,
        }
    }

    #[test]
    fn claude_argv() {
        let argv = build_argv(&spec(AgentKind::Claude), Path::new("/bin/claude"), None);
        assert_eq!(
            argv.to_vec(),
            vec![
                "/bin/claude",
                "-p",
                "--model",
                "fable",
                "--permission-mode",
                "auto",
                "--no-session-persistence",
                "--verbose",
            ]
        );
        assert!(argv.stdin_prompt);
    }

    #[test]
    fn codex_argv() {
        let argv = build_argv(&spec(AgentKind::Codex), Path::new("/bin/codex"), None);
        let v = argv.to_vec();
        assert_eq!(v[0], "/bin/codex");
        assert_eq!(v[1], "exec");
        assert!(v.contains(&"--dangerously-bypass-approvals-and-sandbox".to_string()));
        assert!(v.contains(&"-C".to_string()));
        assert!(v.contains(&"/work".to_string()));
        assert!(argv.stdin_prompt);
    }

    #[test]
    fn grok_argv_uses_prompt_file_not_dash_p() {
        let argv = build_argv(
            &spec(AgentKind::Grok),
            Path::new("/bin/grok"),
            Some(Path::new("/state/tasks/id/prompt.feed.txt")),
        );
        let v = argv.to_vec();
        assert!(!v.iter().any(|a| a == "-p"));
        assert!(v.contains(&"--always-approve".to_string()));
        assert!(v.contains(&"--verbatim".to_string()));
        assert!(v.contains(&"--prompt-file".to_string()));
        assert!(v.contains(&"/state/tasks/id/prompt.feed.txt".to_string()));
        assert!(!argv.stdin_prompt);
    }

    #[test]
    fn missing_binary() {
        let err = resolve_binary(AgentKind::Claude, "/no/such/bin", Path::new("/tmp")).unwrap_err();
        assert!(matches!(
            err,
            AppError::AgentBinaryMissing {
                agent: AgentKind::Claude
            }
        ));
    }
}
