//! Submit-time check for job steps that build on the GPU lease
//!
//! A step that compiles holds its GPU for the whole build, while other jobs
//! wait. Building in a CPU task and submitting the job with `--after` that
//! task keeps the lease for the run itself. Submit refuses a flagged step
//! unless `--allow-build` is given. The shell reading is rough on purpose: a
//! wrapper script that builds slips past it, and the lease guard catches
//! that run once it holds the GPU with no CUDA context

use super::StepWorkload;
use super::spec::JobSpec;

/// Shells whose `-c` script is read for build commands too
const SHELLS: &[&str] = &["sh", "bash", "zsh", "dash"];

/// Build scripts that only compile, matched by file name
const BUILD_SCRIPTS: &[&str] = &["build-ptx.sh", "sass_mix.sh"];

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
                 `homebased task run` task first and submit the job with `--after <task-id>`"
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
    // `env FOO=1 bash -lc '...'` runs the shell after its prefixes
    let words = command_words(&words);
    let program = words.first().map(|program| basename(program))?;
    if !SHELLS.contains(&program) {
        return None;
    }
    // `bash build-ptx.sh`: the first operand is a script the shell runs
    if let Some(script) = words[1..].iter().find(|word| !word.starts_with('-'))
        && !words[1..].iter().any(|word| is_command_flag(word))
        && let Some(tool) = build_tool(&[script])
    {
        return Some(tool);
    }
    // the script is the argument after `-c`, which may be combined with other flags
    let script = words
        .iter()
        .position(|word| is_command_flag(word))
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
    let words = command_words(words);
    let program = basename(words.first()?);
    // cargo takes a toolchain such as `+nightly` before its subcommand
    let mut rest = words[1..]
        .iter()
        .copied()
        .filter(|word| !word.starts_with('+'));
    let sub = rest.next().unwrap_or("");
    let builds = match program {
        "nvcc" | "ptxas" | "make" | "cmake" | "ninja" => return Some(program.to_owned()),
        script if BUILD_SCRIPTS.contains(&script) => return Some(program.to_owned()),
        "cargo" => matches!(
            sub,
            "build" | "b" | "run" | "r" | "test" | "t" | "install" | "oxide"
        ),
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

/// A short flag group that holds `c`, such as `-c` or `-lc`
/// `words` from the command that leading `NAME=value` assignments and
/// wrappers such as `nice -n 10` run
fn command_words<'a, 'b>(words: &'b [&'a str]) -> &'b [&'a str] {
    let mut wrapped = false;
    let start = words
        .iter()
        .position(|word| {
            let prefix = PREFIXES.contains(&basename(word));
            wrapped |= prefix;
            // a wrapper's options and numeric values, such as `-n 10`, come before
            // the command it runs
            let option = wrapped && (word.starts_with('-') || word.parse::<i64>().is_ok());
            !is_assignment(word) && !prefix && !option
        })
        .unwrap_or(words.len());
    &words[start..]
}

fn is_command_flag(word: &str) -> bool {
    word.starts_with('-') && !word.starts_with("--") && word.contains('c')
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
            (
                &["env", "FOO=1", "bash", "-lc", "cargo build && ./bench"],
                Some("cargo build"),
            ),
            (&["nice", "-n", "10", "cargo", "oxide"], Some("cargo oxide")),
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
            (
                &["bash", "scripts/build-ptx.sh", "--arch", "sm_120"],
                Some("build-ptx.sh"),
            ),
            (&["./build-ptx.sh"], Some("build-ptx.sh")),
            (&["cargo", "oxide", "build"], Some("cargo oxide")),
            (
                &["sh", "-c", "ptxas -arch=sm_120 k.ptx && ./bench"],
                Some("ptxas"),
            ),
            (
                &["bash", "-lc", "scripts/sass_mix.sh a b"],
                Some("sass_mix.sh"),
            ),
            (&["bash", "bench.sh"], None),
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
