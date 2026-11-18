//! Load a command task's direnv environment, as the user's terminal would
//!
//! A task runs without a shell, so a repository's `.envrc` never loads on its
//! own, and commands that read paths or settings from it fail. Before a command
//! workload starts, the runner asks direnv what the task's directory exports
//! and adds it to the child's environment. Only an `.envrc` that direnv already
//! allows for that directory loads: homebased never runs `direnv allow`, so a
//! task sees exactly what a terminal opened there would see
//!
//! The task log names the file and the variables it set, never their values,
//! and says why when an `.envrc` exists but did not load

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::run_env;

/// Whether a command task loads its directory's direnv environment
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EnvrcPolicy {
    /// Load the `.envrc` that direnv allows for the task's directory
    #[default]
    Load,
    /// Run the command with the task environment only
    Ignore,
}

impl EnvrcPolicy {
    /// Whether this is the default, so specs and rows leave it out
    #[must_use]
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// Longest a direnv call may take before the task starts without it
///
/// An `.envrc` can build a Nix shell on its first load, so this is generous; a
/// hung `.envrc` still cannot hold the task forever
const DIRENV_BUDGET: Duration = Duration::from_secs(300);

/// Bookkeeping variables direnv sets to track what it loaded
///
/// The child gets them, so a direnv-aware tool in the task sees a loaded
/// environment, but the log does not list them as loaded variables
const DIRENV_STATE_VARS: [&str; 4] = ["DIRENV_DIR", "DIRENV_FILE", "DIRENV_DIFF", "DIRENV_WATCHES"];

/// What a task's directory contributes to its environment
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Envrc {
    /// No `.envrc` applies, or the task opted out
    Absent,
    /// The allowed `.envrc` loaded
    Loaded {
        /// The `.envrc` direnv loaded
        file: PathBuf,
        /// Variables to set (`Some`) or remove (`None`)
        changes: BTreeMap<String, Option<String>>,
    },
    /// An `.envrc` exists but did not load
    Skipped {
        /// Why, without any variable value
        reason: String,
    },
}

impl Envrc {
    /// Ask direnv what `cwd` exports, with the task's `PATH`, `HOME`, and GPU
    /// selection
    ///
    /// `gpu` holds the CUDA variables the command will get, so an `.envrc` that
    /// probes CUDA sees only the devices the task may use
    ///
    /// `direnv` is looked up on the absolute entries of the task's `PATH`: a
    /// relative entry would let the repository supply the `direnv` that runs
    /// before anything is allowed
    ///
    /// `group` holds the process group of the direnv call in progress, or 0,
    /// so a caller that drops this future can confirm that group is gone
    pub async fn resolve(
        cwd: &Path,
        path: &str,
        home: &str,
        gpu: &[(&str, String)],
        group: &AtomicI32,
    ) -> Self {
        let direnv =
            absolute_entries(path).and_then(|path| which::which_in("direnv", Some(path), cwd).ok());
        let Some(direnv) = direnv else {
            return match nearest_envrc(cwd) {
                Some(file) => Self::Skipped {
                    reason: format!(
                        "{} not loaded: direnv is not on the task's PATH",
                        file.display()
                    ),
                },
                None => Self::Absent,
            };
        };
        let probe = Probe {
            direnv,
            cwd,
            path,
            home,
            gpu,
            group,
        };
        probe
            .run()
            .await
            .unwrap_or_else(|reason| Self::Skipped { reason })
    }

    /// The task-log line that says what loaded or why nothing did
    #[must_use]
    pub fn log_line(&self) -> Option<String> {
        match self {
            Self::Absent => None,
            Self::Loaded { file, changes } => {
                let names = |set: bool| {
                    let names: Vec<&str> = changes
                        .iter()
                        .filter(|(name, value)| {
                            value.is_some() == set && !DIRENV_STATE_VARS.contains(&name.as_str())
                        })
                        .map(|(name, _)| name.as_str())
                        .collect();
                    if names.is_empty() {
                        "none".to_owned()
                    } else {
                        names.join(" ")
                    }
                };
                Some(format!(
                    "homebased: loaded {} (set: {}; unset: {})",
                    file.display(),
                    names(true),
                    names(false)
                ))
            }
            Self::Skipped { reason } => Some(format!("homebased: {reason}")),
        }
    }

    /// Apply the loaded variables to a child's environment
    ///
    /// Call it before setting Homebased's own variables, so those win. Run
    /// markers are never taken from an `.envrc`: a stray one would make cleanup
    /// attribute the child to another run
    pub fn apply(&self, command: &mut std::process::Command) {
        let Self::Loaded { changes, .. } = self else {
            return;
        };
        for (name, value) in changes {
            if run_env::RUN_MARKER_VARS.contains(&name.as_str()) {
                continue;
            }
            match value {
                Some(value) => command.env(name, value),
                None => command.env_remove(name),
            };
        }
    }
}

/// One direnv lookup for a task directory
struct Probe<'a> {
    direnv: PathBuf,
    cwd: &'a Path,
    path: &'a str,
    home: &'a str,
    gpu: &'a [(&'a str, String)],
    group: &'a AtomicI32,
}

/// `direnv status --json`, reduced to the field that says what applies
#[derive(Deserialize)]
struct Status {
    state: StatusState,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatusState {
    #[serde(rename = "foundRC")]
    found_rc: Option<FoundRc>,
}

#[derive(Deserialize)]
struct FoundRc {
    path: PathBuf,
    /// 0 allowed, 1 not yet allowed, 2 denied
    allowed: u8,
}

/// What `direnv status` says applies to a directory
enum Lookup {
    Found(FoundRc),
    NoFile,
    /// This direnv cannot report status as JSON, so only `export` can tell
    Unsupported,
}

impl Probe<'_> {
    async fn run(&self) -> Result<Envrc, String> {
        let found = match self.found_rc().await? {
            Lookup::Found(found) => Some(found),
            Lookup::NoFile => return Ok(Envrc::Absent),
            // an older direnv cannot say which file applies, so export runs
            // only when an `.envrc` exists, and what it loaded is checked below
            Lookup::Unsupported if nearest_envrc(self.cwd).is_none() => return Ok(Envrc::Absent),
            Lookup::Unsupported => None,
        };
        if let Some(found) = &found {
            let file = found.path.display();
            // with `load_dotenv`, direnv also loads an allowed `.env`, which a
            // terminal's own rules may treat differently, so only `.envrc` counts
            if !is_envrc(&found.path) {
                return Ok(Envrc::Absent);
            }
            match found.allowed {
                0 => {}
                2 => return Err(format!("{file} not loaded: direnv denies it")),
                _ => {
                    return Err(format!(
                        "{file} not loaded: direnv has not allowed it (run `direnv allow` there in a terminal)"
                    ));
                }
            }
        }

        let export = self.output(&["export", "json"]).await?;
        let label = found.as_ref().map_or_else(
            || ".envrc".to_owned(),
            |found| found.path.display().to_string(),
        );
        // direnv refuses a file it has not allowed here, which is how an older
        // direnv without `status --json` still says why
        // its stderr is not quoted: an `.envrc` writes there too, secrets included
        if !export.status.success() {
            return Err(format!(
                "{label} not loaded: `direnv export` failed ({}); run `direnv allow` or check the file in a terminal there",
                export.status
            ));
        }
        // no change at all prints nothing rather than `{}`
        let changes: BTreeMap<String, Option<String>> =
            if export.stdout.iter().all(u8::is_ascii_whitespace) {
                BTreeMap::new()
            } else {
                serde_json::from_slice(&export.stdout).map_err(|error| {
                    format!("{label} not loaded: could not read `direnv export json` ({error})")
                })?
            };
        // export searches for the file again, so what it loaded can differ
        // from what status saw, such as a `.env` once the `.envrc` is gone
        let exported = changes
            .get("DIRENV_FILE")
            .cloned()
            .flatten()
            .map(PathBuf::from);
        let file = match (found, exported) {
            (_, Some(file)) if !is_envrc(&file) => return Ok(Envrc::Absent),
            (Some(found), Some(file)) if file != found.path => {
                return Err(format!(
                    "{label} not loaded: direnv loaded {} instead",
                    file.display()
                ));
            }
            (_, Some(file)) => file,
            // the call starts without direnv's state, so a load always
            // exports `DIRENV_FILE`; nothing means direnv did not load it, as
            // with `DIRENV_DISABLE`
            (Some(_), None) => {
                return Err(format!(
                    "{label} not loaded: `direnv export` exported nothing"
                ));
            }
            (None, None) => return Ok(Envrc::Absent),
        };
        Ok(Envrc::Loaded { file, changes })
    }

    /// The `.envrc` direnv finds for the directory and whether it is allowed
    async fn found_rc(&self) -> Result<Lookup, String> {
        let status = self.output(&["status", "--json"]).await?;
        // `--json` arrived in direnv 2.33; an older one fails or prints text
        let Some(status) = status
            .status
            .success()
            .then(|| serde_json::from_slice::<Status>(&status.stdout).ok())
            .flatten()
        else {
            return Ok(Lookup::Unsupported);
        };
        Ok(match status.state.found_rc {
            Some(found) => Lookup::Found(found),
            None => Lookup::NoFile,
        })
    }

    /// Run direnv with the environment the child starts from
    ///
    /// Direnv state inherited from whoever started the daemon is removed, so
    /// the export is relative to a shell that has loaded nothing
    async fn output(&self, args: &[&str]) -> Result<std::process::Output, String> {
        let mut command = Command::new(&self.direnv);
        run_env::scrub(command.as_std_mut());
        for name in DIRENV_STATE_VARS {
            command.env_remove(name);
        }
        command
            .args(args)
            .current_dir(self.cwd)
            .env("PATH", self.path)
            .env("HOME", self.home)
            .envs(self.gpu.iter().map(|(name, value)| (name, value)))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        let child = command
            .spawn()
            .map_err(|error| format!("could not run direnv ({error}); .envrc not loaded"))?;
        let group = child.id().map(GroupGuard);
        if let Some(GroupGuard(pgid)) = &group {
            self.group
                .store(i32::try_from(*pgid).unwrap_or(0), Ordering::SeqCst);
        }
        let output = tokio::time::timeout(DIRENV_BUDGET, child.wait_with_output()).await;
        match output {
            Ok(Ok(output)) => {
                // whatever the `.envrc` left running after direnv returned is
                // left alone, as in a terminal
                std::mem::forget(group);
                Ok(output)
            }
            Ok(Err(error)) => Err(format!("could not run direnv ({error}); .envrc not loaded")),
            Err(_) => Err(format!(
                "direnv took longer than {}s; .envrc not loaded",
                DIRENV_BUDGET.as_secs()
            )),
        }
    }
}

/// Kills a direnv call's whole process group unless defused
///
/// Dropping an unfinished call, on timeout or when the task is cancelled, must
/// also stop the shell and whatever the `.envrc` started under it
struct GroupGuard(u32);

impl Drop for GroupGuard {
    fn drop(&mut self) {
        let Ok(pgid) = i32::try_from(self.0) else {
            return;
        };
        // the leader is not yet reaped, so its group ID still names this group
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pgid),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}

/// The nearest `.envrc` at or above `cwd`, without asking direnv
///
/// With `load_dotenv`, a nearer `.env` would hide it from direnv, but whether
/// that is on lives in direnv's config; the file export reports decides
fn nearest_envrc(cwd: &Path) -> Option<PathBuf> {
    cwd.ancestors()
        .map(|dir| dir.join(".envrc"))
        .find(|file| file.is_file())
}

/// Whether direnv picked an `.envrc` rather than a `.env`
fn is_envrc(file: &Path) -> bool {
    file.file_name().is_some_and(|name| name == ".envrc")
}

/// Only the absolute entries of a `PATH`, which do not depend on the cwd
///
/// `None` when none is left: an empty search path means the cwd to `which`
fn absolute_entries(path: &str) -> Option<OsString> {
    let entries: Vec<PathBuf> = std::env::split_paths(path)
        .filter(|entry| entry.is_absolute())
        .collect();
    if entries.is_empty() {
        return None;
    }
    std::env::join_paths(entries).ok()
}

#[cfg(test)]
mod tests {
    use super::absolute_entries;

    #[test]
    fn direnv_lookup_ignores_relative_path_entries() {
        assert_eq!(
            absolute_entries("bin:/usr/bin::./tools:/opt/homebrew/bin"),
            Some("/usr/bin:/opt/homebrew/bin".into())
        );
        assert_eq!(absolute_entries("bin:./tools"), None);
    }
}
