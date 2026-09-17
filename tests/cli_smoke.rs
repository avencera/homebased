//! Opt-in checks that the real agent CLIs still accept the unattended argv
//! homebased builds. Ignored under `just test` / `just ci`; run with
//! `just smoke-cli`. These invocations use `--help` (or a fake flag) so they
//! do not start a model turn.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use homebased::agents::build_argv;
use homebased::domain::AgentKind;
use homebased::spec::NormalizedSpec;
use tempfile::TempDir;

const THREAD: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e431";
const PROBE_FLAG: &str = "--__homebased_cli_probe__";

#[test]
#[ignore = "requires the real `codex` binary on PATH; run with just smoke-cli"]
fn live_codex_exec_accepts_unattended_argv() {
    let help = assert_unattended_help(AgentKind::Codex, None);
    assert!(
        help_documents_flag(&help, "exec") || help.contains("codex exec"),
        "codex exec help did not identify the exec subcommand:\n{help}"
    );
    assert!(
        help.contains("danger-full-access"),
        "codex exec help dropped sandbox value danger-full-access:\n{help}"
    );
}

#[test]
#[ignore = "requires the real `codex` binary on PATH; run with just smoke-cli"]
fn live_codex_queue_accepts_thread_and_message() {
    let bin = live_binary(AgentKind::Codex);
    // keep in lockstep with `callback::send_queue`
    let args = vec![
        "queue".into(),
        "--thread".into(),
        THREAD.to_string(),
        "--message".into(),
        "HOMEBASED_EVENT {}".into(),
        "--help".into(),
    ];
    let out = run(&bin, &args);
    assert!(
        out.status.success(),
        "codex queue --help rejected the callback argv\n{}",
        combined(&out)
    );
    let help = combined(&out);
    for flag in ["--thread", "--message"] {
        assert!(
            help_documents_flag(&help, flag),
            "codex queue help is missing {flag}:\n{help}"
        );
    }
}

#[test]
#[ignore = "requires the real `claude` binary on PATH; run with just smoke-cli"]
fn live_claude_accepts_unattended_argv() {
    assert_unattended_help(AgentKind::Claude, None);

    // `--help` ignores unknown flags on claude; a fake flag without `--help`
    // is the parse check that would fail if an unattended flag were renamed.
    let dir = TempDir::new().unwrap();
    let argv = unattended_argv(AgentKind::Claude, dir.path(), None);
    let mut probe = argv.args.clone();
    probe.push(PROBE_FLAG.into());
    let out = run(&argv.program, &probe);
    assert!(
        !out.status.success(),
        "claude accepted {PROBE_FLAG}; unknown-flag detection would miss drift\n{}",
        combined(&out)
    );
    let text = combined(&out);
    assert!(
        text.contains(PROBE_FLAG) || text.to_lowercase().contains("unknown option"),
        "claude error did not mention the probe flag:\n{text}"
    );
    for flag in flag_tokens(&argv.args) {
        let unknown = format!("unknown option '{flag}'");
        let unexpected = format!("unexpected argument '{flag}'");
        assert!(
            !text.contains(&unknown) && !text.contains(&unexpected),
            "claude rejected unattended flag {flag}:\n{text}"
        );
    }
}

#[test]
#[ignore = "requires the real `grok` binary on PATH; run with just smoke-cli"]
fn live_grok_accepts_unattended_argv() {
    let dir = TempDir::new().unwrap();
    let prompt = dir.path().join("prompt.feed.txt");
    std::fs::write(&prompt, "smoke\n").unwrap();
    assert_unattended_help(AgentKind::Grok, Some(prompt.as_path()));
}

fn assert_unattended_help(kind: AgentKind, prompt_file: Option<&Path>) -> String {
    let dir = TempDir::new().unwrap();
    let argv = unattended_argv(kind, dir.path(), prompt_file);
    let mut args = argv.args.clone();
    args.push("--help".into());
    let out = run(&argv.program, &args);
    assert!(
        out.status.success(),
        "{} unattended argv plus --help failed (flag drift?)\nargv={:?}\n{}",
        kind.binary_name(),
        argv.to_vec(),
        combined(&out)
    );
    let help = combined(&out);
    for flag in flag_tokens(&argv.args) {
        assert!(
            help_documents_flag(&help, flag),
            "{} help is missing unattended flag {flag}:\n{help}",
            kind.binary_name()
        );
    }
    help
}

fn unattended_argv(
    kind: AgentKind,
    cwd: &Path,
    prompt_file: Option<&Path>,
) -> homebased::agents::ChildArgv {
    let spec = NormalizedSpec {
        api_version: 1,
        agent: kind,
        model: Some("smoke-model".into()),
        thread: THREAD.parse().unwrap(),
        cwd: cwd.to_path_buf(),
        prompt: "smoke".into(),
        timeout: Duration::from_secs(4),
        extra_args: vec![],
        report_trailer: false,
    };
    build_argv(&spec, &live_binary(kind), prompt_file)
}

fn live_binary(kind: AgentKind) -> PathBuf {
    which::which(kind.binary_name()).unwrap_or_else(|err| {
        panic!(
            "{} not found on PATH ({err}); install it before `just smoke-cli`",
            kind.binary_name()
        )
    })
}

fn flag_tokens(args: &[String]) -> Vec<&str> {
    args.iter()
        .filter(|arg| arg.starts_with('-'))
        .map(String::as_str)
        .collect()
}

fn help_documents_flag(help: &str, flag: &str) -> bool {
    help.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed == flag
            || trimmed.starts_with(&format!("{flag} "))
            || trimmed.starts_with(&format!("{flag},"))
            || trimmed.starts_with(&format!("{flag}\t"))
            || trimmed.contains(&format!(", {flag} "))
            || trimmed.contains(&format!(", {flag},"))
            || trimmed.contains(&format!(", {flag}\t"))
            || trimmed.ends_with(&format!(", {flag}"))
    })
}

fn run(bin: &Path, args: &[String]) -> Output {
    let mut child = Command::new(bin)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|err| {
            panic!("spawn {} {args:?}: {err}", bin.display());
        });
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                child
                    .stdout
                    .take()
                    .expect("piped stdout")
                    .read_to_end(&mut stdout)
                    .expect("read stdout");
                child
                    .stderr
                    .take()
                    .expect("piped stderr")
                    .read_to_end(&mut stderr)
                    .expect("read stderr");
                return Output {
                    status,
                    stdout,
                    stderr,
                };
            }
            Ok(None) if Instant::now() > deadline => {
                let _ = child.kill();
                let out = child.wait_with_output().ok();
                panic!(
                    "{} {args:?} timed out after 15s; output={out:?}",
                    bin.display()
                );
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(err) => panic!("wait {} {args:?}: {err}", bin.display()),
        }
    }
}

fn combined(out: &Output) -> String {
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    if !out.stderr.is_empty() {
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&String::from_utf8_lossy(&out.stderr));
    }
    text
}
