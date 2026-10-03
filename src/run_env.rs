//! Environment variables that mark a process as part of one run
//!
//! Cleanup attributes a leftover process to a run by the run's
//! `HOMEBASED_TASK_ID` in that process's environment. A marker inherited from
//! an unrelated run would make cleanup kill the wrong work, or find a
//! protected process carrying another run's marker, so every process that
//! Homebased spawns starts without these variables and gets only the values
//! that belong to it

use std::process::Command;

/// The run's task ID; the marker that cleanup sweeps for
pub const TASK_ID: &str = "HOMEBASED_TASK_ID";

/// Stable ID of the queued job that a run belongs to
pub const JOB_ID: &str = "HOMEBASED_JOB_ID";

/// Durable job directory for checkpoints, kept across runs
pub const JOB_DIR: &str = "HOMEBASED_JOB_DIR";

/// Attempt count across the whole job, starting at 1
pub const RUN_NUMBER: &str = "HOMEBASED_RUN_NUMBER";

/// 0-based index of the step that a run executes
pub const STEP_INDEX: &str = "HOMEBASED_STEP_INDEX";

/// `1` when the previous attempt of the step yielded
pub const RESUME: &str = "HOMEBASED_RESUME";

/// Run-scoped path that appears when the run should stop at its next checkpoint
pub const YIELD_FILE: &str = "HOMEBASED_YIELD_FILE";

/// Resource lane assigned to the workload
pub const RESOURCE: &str = "HOMEBASED_RESOURCE";

/// Device selection owned by the assigned resource
pub const CUDA_VISIBLE_DEVICES: &str = "CUDA_VISIBLE_DEVICES";

/// Every variable that belongs to exactly one run
///
/// Configuration such as `HOMEBASED_HOME` is not listed: it describes the
/// installation, not a run, and children need it to reach the daemon
pub const RUN_MARKER_VARS: [&str; 9] = [
    TASK_ID,
    JOB_ID,
    JOB_DIR,
    RUN_NUMBER,
    STEP_INDEX,
    RESUME,
    YIELD_FILE,
    RESOURCE,
    CUDA_VISIBLE_DEVICES,
];

/// Remove every inherited run-marker variable from a command's environment
///
/// Values set on the command afterwards still apply, so callers scrub first and
/// then set the variables that belong to the child
pub fn scrub(command: &mut Command) -> &mut Command {
    for name in RUN_MARKER_VARS {
        command.env_remove(name);
    }
    command
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::{RUN_MARKER_VARS, TASK_ID, scrub};

    #[test]
    fn scrub_removes_markers_but_keeps_later_values_and_configuration() {
        let mut command = Command::new("/usr/bin/env");
        for name in RUN_MARKER_VARS {
            command.env(name, "inherited");
        }
        command.env("HOMEBASED_HOME", "/tmp/home");
        scrub(&mut command).env(TASK_ID, "own");

        let envs: Vec<_> = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect();
        for name in RUN_MARKER_VARS.into_iter().filter(|name| *name != TASK_ID) {
            assert!(
                envs.contains(&(name.to_owned(), None)),
                "{name} must be removed: {envs:?}"
            );
        }
        assert!(envs.contains(&(TASK_ID.to_owned(), Some("own".to_owned()))));
        assert!(envs.contains(&("HOMEBASED_HOME".to_owned(), Some("/tmp/home".to_owned()))));
    }
}
