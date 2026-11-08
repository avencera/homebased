//! Submit-time hint for job steps that build on the GPU lease
//!
//! A step that compiles holds its GPU for the whole build, while other jobs
//! wait. Building in a CPU task and submitting the job with `--after` that
//! task keeps the lease for the run itself. This is only a hint: the shell
//! reading is rough, and a step it flags still submits

use super::StepWorkload;
use super::spec::JobSpec;

/// Shells whose `-c` script is read for build commands too
const SHELLS: &[&str] = &["sh", "bash", "zsh", "dash"];

/// Words that run the command after them, so the build tool is the next word
const PREFIXES: &[&str] = &["env", "exec", "time", "nice", "nohup", "sudo", "command"];

/// One warning per step that runs a build tool, naming the step and the tool
#[must_use]
pub fn build_warnings(spec: &JobSpec) -> Vec<String> {
    spec.steps
        .as_slice()
        .iter()
        .enumerate()
        .filter_map(|(index, step)| {
            let argv = match step {
                StepWorkload::Task(task) => task.command.to_vec(),
                StepWorkload::Container(container) => {
                    let mut argv = container
                        .entrypoint
                        .as_ref()
                        .map(|entrypoint| entrypoint.as_slice().to_vec())
                        .unwrap_or_default();
                    argv.extend(container.args.iter().cloned());
                    argv
                }
            };
            let tool = build_command(&argv)?;
            Some(format!(
                "step {index} runs `{tool}`, which holds the GPU while it builds; build in a CPU \
                 task first and submit the job with `--after <task-id>`"
            ))
        })
        .collect()
}

/// The first build command `argv` runs, directly or in a shell's `-c` script
fn build_command(argv: &[String]) -> Option<String> {
    let words: Vec<&str> = argv.iter().map(String::as_str).collect();
    if let Some(tool) = build_tool(&words) {
        return Some(tool);
    }
    let program = words.first().map(|program| basename(program))?;
    if !SHELLS.contains(&program) {
        return None;
    }
    // the script is the argument after `-c`, which may be combined with other flags
    let script = words
        .iter()
        .position(|word| word.starts_with('-') && !word.starts_with("--") && word.contains('c'))
        .and_then(|flag| words.get(flag + 1))?;
    script
        .split(['\n', ';', '|', '&', '(', ')', '{', '}'])
        .find_map(|command| {
            let words: Vec<&str> = command
                .split_whitespace()
                .map(|word| word.trim_matches(|c| c == '"' || c == '\''))
                .collect();
            build_tool(&words)
        })
}

/// The build tool and subcommand one simple command runs, such as `cargo build`
fn build_tool(words: &[&str]) -> Option<String> {
    // leading `NAME=value` assignments and wrappers run the command after them
    let start = words
        .iter()
        .position(|word| !is_assignment(word) && !PREFIXES.contains(&basename(word)))?;
    let words = &words[start..];
    let program = basename(words[0]);
    // cargo takes a toolchain such as `+nightly` before its subcommand
    let mut rest = words[1..]
        .iter()
        .copied()
        .filter(|word| !word.starts_with('+'));
    let sub = rest.next().unwrap_or("");
    let builds = match program {
        "nvcc" | "make" | "cmake" | "ninja" => return Some(program.to_owned()),
        "cargo" => matches!(sub, "build" | "b" | "run" | "r" | "test" | "t" | "install"),
        "pip" | "pip3" => sub == "install",
        "uv" => match sub {
            "sync" | "build" => true,
            "pip" => rest.next() == Some("install"),
            _ => false,
        },
        "npm" | "pnpm" => matches!(sub, "install" | "i" | "ci"),
        "go" => sub == "build",
        "python" | "python3" => {
            return (sub == "-m" && rest.next() == Some("pip") && rest.next() == Some("install"))
                .then(|| format!("{program} -m pip install"));
        }
        _ => false,
    };
    builds.then(|| match program {
        "uv" if sub == "pip" => "uv pip install".to_owned(),
        _ => format!("{program} {sub}"),
    })
}

fn is_assignment(word: &str) -> bool {
    word.split_once('=').is_some_and(|(name, _)| {
        !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

fn basename(word: &str) -> &str {
    word.rsplit('/').next().unwrap_or(word)
}

#[cfg(test)]
mod tests {
    use super::build_command;

    fn argv(words: &[&str]) -> Vec<String> {
        words.iter().map(|word| (*word).to_owned()).collect()
    }

    #[test]
    fn a_build_is_found_in_argv_and_in_a_shell_script() {
        let cases: &[(&[&str], Option<&str>)] = &[
            (&["cargo", "build", "--release"], Some("cargo build")),
            (&["/usr/bin/cargo", "+nightly", "run"], Some("cargo run")),
            (&["./target/release/train", "--epochs", "3"], None),
            (&["cargo", "fmt"], None),
            (
                &[
                    "bash",
                    "-lc",
                    "cd model && CUDA=1 uv pip install -e . && python train.py",
                ],
                Some("uv pip install"),
            ),
            (
                &["sh", "-c", "python3 -m pip install torch; ./train"],
                Some("python3 -m pip install"),
            ),
            (
                &["sh", "-c", "nvcc -O3 kernel.cu -o k && ./k"],
                Some("nvcc"),
            ),
            (&["sh", "-c", "./train --make-plots"], None),
            (
                &["env", "GOFLAGS=-mod=mod", "go", "build", "./..."],
                Some("go build"),
            ),
            (&["npm", "ci"], Some("npm ci")),
            (&["sh", "-c", "echo 'pip install later'"], None),
        ];
        for (words, expected) in cases {
            assert_eq!(
                build_command(&argv(words)).as_deref(),
                *expected,
                "{words:?}"
            );
        }
    }
}
