//! Ownership contract for resource work
//!
//! After resource work ends, the authority can release the GPU only from
//! task-layer evidence that the work stopped. Process-group evidence covers GPU
//! work only while the work stays in that process group, so a command must name
//! its real workload directly. The contract accepts one of three shapes:
//!
//! - A native foreground executable. Its entry point is an ELF or Mach-O file,
//!   and neither its name nor its resolved target names a known shell,
//!   interpreter, program launcher, detach tool, container client, or remote
//!   client. Those programs hide the real workload in their arguments or in code
//!   that Homebased does not inspect, or they can move it out of the group
//! - The maintained direct-segment trainer, only for background return work
//!   Its ownership lock, not its process group, is the release witness
//! - A typed container workload. Homebased starts and removes the container
//!   itself, so the container's own state is the witness. A `docker` command
//!   line is still refused, because its client can exit while the container runs
//!
//! The rule is conservative, not complete. A native executable can still start
//! work in another session, and a copied or hard-linked launcher keeps no
//! recognizable name. The contract does not prove physical GPU exclusion

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::invocation::CommandLine;
use crate::resource::ResourceTaskOwnershipRisk;
use crate::resource::ResourceTaskOwnershipRisk::{
    ContainerClient, DetachedLauncher, Interpreter, ProgramLauncher, RemoteShell, ShellWrapper,
};
use crate::resource::command_shape::direct_segment_runtime_root;
use crate::spec::{NormalizedSpec, NormalizedWorkload};

/// Release witness supported by one accepted command shape
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandOwnershipContract {
    /// Native foreground executable; the task-run process-group exit is the witness
    ForegroundExecutable,
    /// Maintained direct-segment trainer; its segment ownership lock is the witness
    ///
    /// The static argv shape only selects this contract. The launch must still
    /// pass the full direct-segment check against its task row
    DirectSegmentTrainer,
    /// Typed container workload; the exited, removed container is the witness
    Container,
}

impl CommandOwnershipContract {
    /// Classify queued work: a foreground command or a container
    pub fn for_queued_work(
        workload: &NormalizedWorkload,
    ) -> Result<Self, ResourceTaskOwnershipRisk> {
        match workload {
            NormalizedWorkload::Task(task) => {
                foreground_argv(&task.command).map(|()| Self::ForegroundExecutable)
            }
            NormalizedWorkload::Container(_) => Ok(Self::Container),
            NormalizedWorkload::Agent(_) => Err(ResourceTaskOwnershipRisk::UninspectableEntryPoint),
        }
    }

    /// Classify return work, which may also be the maintained trainer
    pub fn for_return_work(
        workload: &NormalizedWorkload,
    ) -> Result<Self, ResourceTaskOwnershipRisk> {
        if let NormalizedWorkload::Task(task) = workload
            && direct_segment_runtime_root(&task.command).is_some()
        {
            return Ok(Self::DirectSegmentTrainer);
        }
        Self::for_queued_work(workload)
    }
}

/// Return the command line of a task workload, or `None` for other workloads
#[must_use]
pub fn task_command(spec: &NormalizedSpec) -> Option<&CommandLine> {
    match &spec.workload {
        NormalizedWorkload::Task(task) => Some(&task.command),
        NormalizedWorkload::Agent(_) | NormalizedWorkload::Container(_) => None,
    }
}

/// Check the argv of a foreground command without reading the file system
///
/// The program must not be a known launcher of other work. A later argument
/// must not name a shell, interpreter, detach tool, container client, or remote
/// client, because an unrecognized launcher can run it
pub fn foreground_argv(command: &CommandLine) -> Result<(), ResourceTaskOwnershipRisk> {
    if let Some(risk) = program_risk(command.program()) {
        return Err(risk);
    }
    match command
        .args()
        .iter()
        .find_map(|arg| nested_program_risk(arg))
    {
        Some(risk) => Err(risk),
        None => Ok(()),
    }
}

/// Inspect the entry point of a queued command when its program names a path
///
/// A bare program name resolves from the executor `PATH` only when its task binds,
/// so [`inspect_foreground_entry_point`] checks it then. A container has no
/// entry point on the host; its host inputs are checked instead
pub fn inspect_path_qualified_entry_point(
    spec: &NormalizedSpec,
) -> Result<(), ResourceTaskOwnershipRisk> {
    if let NormalizedWorkload::Container(_) = &spec.workload {
        return Ok(());
    }
    let Some(command) = task_command(spec) else {
        return Err(ResourceTaskOwnershipRisk::UninspectableEntryPoint);
    };
    if !command.program().contains('/') {
        return Ok(());
    }
    inspect_foreground_entry_point(&spec.cwd.join(command.program()))
}

/// Inspect one resolved foreground entry point
///
/// The canonical target must not be a known launcher, which catches a renamed
/// symbolic link, and its first bytes must be a native executable header. A
/// script, an unknown format, or an unreadable file fails closed
pub fn inspect_foreground_entry_point(binary: &Path) -> Result<(), ResourceTaskOwnershipRisk> {
    let canonical = std::fs::canonicalize(binary)
        .map_err(|_| ResourceTaskOwnershipRisk::UninspectableEntryPoint)?;
    if let Some(risk) = canonical.to_str().and_then(program_risk) {
        return Err(risk);
    }

    let mut header = [0_u8; 4];
    File::open(&canonical)
        .and_then(|mut file| file.read_exact(&mut header))
        .map_err(|_| ResourceTaskOwnershipRisk::UninspectableEntryPoint)?;
    if header.starts_with(b"#!") {
        return Err(ResourceTaskOwnershipRisk::ScriptEntryPoint);
    }
    if NATIVE_EXECUTABLE_MAGIC.contains(&header) {
        return Ok(());
    }

    Err(ResourceTaskOwnershipRisk::UninspectableEntryPoint)
}

// ELF, then 32-bit and 64-bit Mach-O in both byte orders, then universal Mach-O
const NATIVE_EXECUTABLE_MAGIC: [[u8; 4]; 6] = [
    [0x7f, b'E', b'L', b'F'],
    [0xfe, 0xed, 0xfa, 0xce],
    [0xce, 0xfa, 0xed, 0xfe],
    [0xfe, 0xed, 0xfa, 0xcf],
    [0xcf, 0xfa, 0xed, 0xfe],
    [0xca, 0xfe, 0xba, 0xbe],
];

/// Where a known program name is refused
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    /// The name is refused as the program and as any later argument
    Anywhere,
    /// The name is refused only as the program, because it is also a common word
    ProgramOnly,
}

use Scope::{Anywhere, ProgramOnly};

const KNOWN_PROGRAMS: &[(&str, ResourceTaskOwnershipRisk, Scope)] = &[
    // remote execution and cluster schedulers run the work on another host
    ("ssh", RemoteShell, Anywhere),
    ("scp", RemoteShell, Anywhere),
    ("sftp", RemoteShell, Anywhere),
    ("mosh", RemoteShell, Anywhere),
    ("rsh", RemoteShell, Anywhere),
    ("autossh", RemoteShell, Anywhere),
    ("sshpass", RemoteShell, Anywhere),
    ("srun", RemoteShell, Anywhere),
    ("sbatch", RemoteShell, Anywhere),
    ("salloc", RemoteShell, Anywhere),
    ("qsub", RemoteShell, Anywhere),
    // container clients ask a daemon to run the work
    ("docker", ContainerClient, Anywhere),
    ("docker-compose", ContainerClient, Anywhere),
    ("nvidia-docker", ContainerClient, Anywhere),
    ("podman", ContainerClient, Anywhere),
    ("nerdctl", ContainerClient, Anywhere),
    ("ctr", ContainerClient, Anywhere),
    ("crictl", ContainerClient, Anywhere),
    ("kubectl", ContainerClient, Anywhere),
    ("apptainer", ContainerClient, Anywhere),
    ("singularity", ContainerClient, Anywhere),
    ("lxc", ContainerClient, Anywhere),
    ("incus", ContainerClient, Anywhere),
    ("runc", ContainerClient, Anywhere),
    ("crun", ContainerClient, Anywhere),
    ("enroot", ContainerClient, Anywhere),
    ("finch", ContainerClient, Anywhere),
    ("systemd-nspawn", ContainerClient, Anywhere),
    // shells run a command string that can background or detach work
    ("sh", ShellWrapper, Anywhere),
    ("bash", ShellWrapper, Anywhere),
    ("dash", ShellWrapper, Anywhere),
    ("ash", ShellWrapper, Anywhere),
    ("zsh", ShellWrapper, Anywhere),
    ("fish", ShellWrapper, Anywhere),
    ("ksh", ShellWrapper, Anywhere),
    ("mksh", ShellWrapper, Anywhere),
    ("csh", ShellWrapper, Anywhere),
    ("tcsh", ShellWrapper, Anywhere),
    ("busybox", ShellWrapper, Anywhere),
    ("pwsh", ShellWrapper, Anywhere),
    ("powershell", ShellWrapper, Anywhere),
    ("cmd", ShellWrapper, Anywhere),
    ("xonsh", ShellWrapper, Anywhere),
    ("nu", ShellWrapper, ProgramOnly),
    ("elvish", ShellWrapper, Anywhere),
    // detach tools start work that outlives the caller
    ("nohup", DetachedLauncher, Anywhere),
    ("setsid", DetachedLauncher, Anywhere),
    ("daemon", DetachedLauncher, Anywhere),
    ("daemonize", DetachedLauncher, Anywhere),
    ("start-stop-daemon", DetachedLauncher, Anywhere),
    ("screen", DetachedLauncher, Anywhere),
    ("tmux", DetachedLauncher, Anywhere),
    ("zellij", DetachedLauncher, Anywhere),
    ("dtach", DetachedLauncher, Anywhere),
    ("abduco", DetachedLauncher, Anywhere),
    ("systemd-run", DetachedLauncher, Anywhere),
    ("launchctl", DetachedLauncher, Anywhere),
    ("pm2", DetachedLauncher, Anywhere),
    ("supervisorctl", DetachedLauncher, Anywhere),
    ("at", DetachedLauncher, ProgramOnly),
    ("batch", DetachedLauncher, ProgramOnly),
    ("open", DetachedLauncher, ProgramOnly),
    ("xdg-open", DetachedLauncher, Anywhere),
    // interpreters run code that Homebased does not inspect
    ("python", Interpreter, Anywhere),
    ("pythonw", Interpreter, Anywhere),
    ("pypy", Interpreter, Anywhere),
    ("ipython", Interpreter, Anywhere),
    ("jupyter", Interpreter, Anywhere),
    ("node", Interpreter, Anywhere),
    ("nodejs", Interpreter, Anywhere),
    ("deno", Interpreter, Anywhere),
    ("bun", Interpreter, Anywhere),
    ("perl", Interpreter, Anywhere),
    ("ruby", Interpreter, Anywhere),
    ("php", Interpreter, Anywhere),
    ("lua", Interpreter, Anywhere),
    ("luajit", Interpreter, Anywhere),
    ("rscript", Interpreter, Anywhere),
    ("julia", Interpreter, Anywhere),
    ("java", Interpreter, Anywhere),
    ("osascript", Interpreter, Anywhere),
    ("tclsh", Interpreter, Anywhere),
    ("awk", Interpreter, ProgramOnly),
    ("gawk", Interpreter, Anywhere),
    ("mawk", Interpreter, Anywhere),
    ("nawk", Interpreter, Anywhere),
    // launchers run another program named in their arguments
    ("env", ProgramLauncher, ProgramOnly),
    ("sudo", ProgramLauncher, Anywhere),
    ("doas", ProgramLauncher, Anywhere),
    ("su", ProgramLauncher, ProgramOnly),
    ("runuser", ProgramLauncher, Anywhere),
    ("pkexec", ProgramLauncher, Anywhere),
    ("sg", ProgramLauncher, ProgramOnly),
    ("newgrp", ProgramLauncher, ProgramOnly),
    ("timeout", ProgramLauncher, ProgramOnly),
    ("gtimeout", ProgramLauncher, ProgramOnly),
    ("nice", ProgramLauncher, ProgramOnly),
    ("ionice", ProgramLauncher, ProgramOnly),
    ("chrt", ProgramLauncher, ProgramOnly),
    ("taskset", ProgramLauncher, ProgramOnly),
    ("numactl", ProgramLauncher, ProgramOnly),
    ("stdbuf", ProgramLauncher, ProgramOnly),
    ("unbuffer", ProgramLauncher, ProgramOnly),
    ("time", ProgramLauncher, ProgramOnly),
    ("strace", ProgramLauncher, ProgramOnly),
    ("ltrace", ProgramLauncher, ProgramOnly),
    ("perf", ProgramLauncher, ProgramOnly),
    ("valgrind", ProgramLauncher, ProgramOnly),
    ("gdb", ProgramLauncher, ProgramOnly),
    ("lldb", ProgramLauncher, ProgramOnly),
    ("nsys", ProgramLauncher, ProgramOnly),
    ("ncu", ProgramLauncher, ProgramOnly),
    ("nvprof", ProgramLauncher, ProgramOnly),
    ("flock", ProgramLauncher, ProgramOnly),
    ("xargs", ProgramLauncher, ProgramOnly),
    ("parallel", ProgramLauncher, ProgramOnly),
    ("watch", ProgramLauncher, ProgramOnly),
    ("chroot", ProgramLauncher, ProgramOnly),
    ("unshare", ProgramLauncher, ProgramOnly),
    ("nsenter", ProgramLauncher, ProgramOnly),
    ("firejail", ProgramLauncher, ProgramOnly),
    ("bwrap", ProgramLauncher, ProgramOnly),
    ("setpriv", ProgramLauncher, ProgramOnly),
    ("capsh", ProgramLauncher, ProgramOnly),
    ("prlimit", ProgramLauncher, ProgramOnly),
    ("cgexec", ProgramLauncher, ProgramOnly),
    ("sandbox-exec", ProgramLauncher, ProgramOnly),
    ("script", ProgramLauncher, ProgramOnly),
    ("expect", ProgramLauncher, ProgramOnly),
    ("caffeinate", ProgramLauncher, ProgramOnly),
    ("arch", ProgramLauncher, ProgramOnly),
    ("exec", ProgramLauncher, ProgramOnly),
    ("command", ProgramLauncher, ProgramOnly),
    ("eatmydata", ProgramLauncher, ProgramOnly),
    ("proxychains", ProgramLauncher, ProgramOnly),
    ("direnv", ProgramLauncher, ProgramOnly),
    ("entr", ProgramLauncher, ProgramOnly),
    ("chronic", ProgramLauncher, ProgramOnly),
    ("torchrun", ProgramLauncher, ProgramOnly),
    ("accelerate", ProgramLauncher, ProgramOnly),
    ("deepspeed", ProgramLauncher, ProgramOnly),
    ("mpirun", ProgramLauncher, ProgramOnly),
    ("mpiexec", ProgramLauncher, ProgramOnly),
    ("horovodrun", ProgramLauncher, ProgramOnly),
    // task and package runners run configured commands
    ("make", ProgramLauncher, ProgramOnly),
    ("gmake", ProgramLauncher, ProgramOnly),
    ("just", ProgramLauncher, ProgramOnly),
    ("cargo", ProgramLauncher, ProgramOnly),
    ("go", ProgramLauncher, ProgramOnly),
    ("npm", ProgramLauncher, ProgramOnly),
    ("npx", ProgramLauncher, ProgramOnly),
    ("pnpm", ProgramLauncher, ProgramOnly),
    ("yarn", ProgramLauncher, ProgramOnly),
    ("uv", ProgramLauncher, ProgramOnly),
    ("uvx", ProgramLauncher, ProgramOnly),
    ("pipx", ProgramLauncher, ProgramOnly),
    ("poetry", ProgramLauncher, ProgramOnly),
    ("pdm", ProgramLauncher, ProgramOnly),
    ("hatch", ProgramLauncher, ProgramOnly),
    ("conda", ProgramLauncher, ProgramOnly),
    ("mamba", ProgramLauncher, ProgramOnly),
    ("micromamba", ProgramLauncher, ProgramOnly),
    ("pixi", ProgramLauncher, ProgramOnly),
    ("tox", ProgramLauncher, ProgramOnly),
    ("nox", ProgramLauncher, ProgramOnly),
];

/// Name families whose members carry a variable suffix, such as `lxc-start`
const KNOWN_PREFIXES: &[(&str, ResourceTaskOwnershipRisk)] = &[
    ("lxc-", ContainerClient),
    ("docker-", ContainerClient),
    ("python", Interpreter),
    ("pypy", Interpreter),
];

fn program_risk(token: &str) -> Option<ResourceTaskOwnershipRisk> {
    known_program(token).map(|(risk, _)| risk)
}

fn nested_program_risk(token: &str) -> Option<ResourceTaskOwnershipRisk> {
    known_program(token)
        .filter(|(_, scope)| *scope == Anywhere)
        .map(|(risk, _)| risk)
}

/// Match the file name of one argv token against the known programs
///
/// The match ignores case, a `.exe` suffix, and a trailing version such as the
/// `3.11` in `python3.11`
fn known_program(token: &str) -> Option<(ResourceTaskOwnershipRisk, Scope)> {
    let name = Path::new(token).file_name()?.to_str()?.to_ascii_lowercase();
    let name = name.strip_suffix(".exe").unwrap_or(&name);
    let unversioned = name.trim_end_matches(|c: char| c.is_ascii_digit() || c == '.' || c == '-');
    for candidate in [name, unversioned] {
        if let Some((_, risk, scope)) = KNOWN_PROGRAMS
            .iter()
            .find(|(known, _, _)| *known == candidate)
        {
            return Some((*risk, *scope));
        }
    }

    KNOWN_PREFIXES
        .iter()
        .find(|(prefix, _)| name.starts_with(prefix))
        .map(|(_, risk)| (*risk, Anywhere))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};

    use tempfile::tempdir;

    use super::{CommandOwnershipContract, foreground_argv, inspect_foreground_entry_point};
    use crate::container::ContainerWorkload;
    use crate::invocation::CommandLine;
    use crate::resource::ResourceTaskOwnershipRisk::{
        self, ContainerClient, DetachedLauncher, Interpreter, ProgramLauncher, RemoteShell,
        ShellWrapper,
    };
    use crate::spec::{NormalizedTaskWorkload, NormalizedWorkload};
    use std::path::Path;

    fn argv(parts: &[&str]) -> CommandLine {
        CommandLine::try_from_argv(parts.iter().map(|part| (*part).to_owned()).collect()).unwrap()
    }

    fn task(parts: &[&str]) -> NormalizedWorkload {
        NormalizedWorkload::Task(NormalizedTaskWorkload {
            command: argv(parts),
        })
    }

    #[test]
    fn wrapped_or_nested_launch_shapes_are_refused() {
        for (parts, expected) in [
            // a wrapper hides the detached container behind its own exit
            (
                &["env", "docker", "run", "-d", "trainer"][..],
                ProgramLauncher,
            ),
            (
                &["/usr/bin/sudo", "-n", "/opt/gpu/bench"][..],
                ProgramLauncher,
            ),
            (&["timeout", "1h", "/opt/gpu/bench"][..], ProgramLauncher),
            (&["python3.11", "bench.py"][..], Interpreter),
            (&["/bin/bash", "-c", "bench &"][..], ShellWrapper),
            (&["setsid", "/opt/gpu/bench"][..], DetachedLauncher),
            (&["lxc-execute", "-n", "gpu"][..], ContainerClient),
            // an unknown launcher still exposes the nested client or shell
            (
                &["/opt/tools/renamed-env", "docker", "run", "-d", "x"][..],
                ContainerClient,
            ),
            (
                &["/opt/tools/profiler", "--", "/usr/bin/ssh", "gpu-host"][..],
                RemoteShell,
            ),
            (
                &["/opt/tools/runner", "/usr/bin/nohup", "/opt/gpu/bench"][..],
                DetachedLauncher,
            ),
            (&["/opt/tools/runner", "Python3", "x.py"][..], Interpreter),
        ] {
            assert_eq!(foreground_argv(&argv(parts)), Err(expected), "{parts:?}");
        }
    }

    #[test]
    fn direct_commands_keep_ordinary_arguments() {
        for parts in [
            &["/opt/gpu/bench", "--iterations", "10"][..],
            &["./bin/optimize", "--time", "30", "--env", "prod"][..],
            &["nvidia-smi", "--query-gpu=name", "--format=csv"][..],
        ] {
            assert_eq!(foreground_argv(&argv(parts)), Ok(()), "{parts:?}");
            assert_eq!(
                CommandOwnershipContract::for_queued_work(&task(parts)),
                Ok(CommandOwnershipContract::ForegroundExecutable)
            );
        }
    }

    #[test]
    fn only_return_work_may_select_the_trainer_contract() {
        let trainer = task(&[
            "python3",
            "-m",
            "ops.run_segment",
            "run",
            "--task",
            "/t/task.json",
            "--input-root",
            "/t/inputs",
            "--runtime-root",
            "/t/runtime",
            "--image-digest",
            "sha256:00",
        ]);
        assert_eq!(
            CommandOwnershipContract::for_return_work(&trainer),
            Ok(CommandOwnershipContract::DirectSegmentTrainer)
        );
        assert_eq!(
            CommandOwnershipContract::for_queued_work(&trainer),
            Err(Interpreter)
        );
        assert_eq!(
            CommandOwnershipContract::for_return_work(&task(&["python3", "evaluate.py"])),
            Err(Interpreter)
        );
    }

    #[test]
    fn a_typed_container_selects_the_container_contract_but_a_docker_command_does_not() {
        let container = NormalizedWorkload::Container(Box::new(
            ContainerWorkload::from_value(&serde_json::json!({
                "image": format!("sha256:{}", "0".repeat(64)),
                "memory": "1g",
                "gpus": "all"
            }))
            .unwrap(),
        ));
        for classify in [
            CommandOwnershipContract::for_queued_work,
            CommandOwnershipContract::for_return_work,
        ] {
            assert_eq!(
                classify(&container),
                Ok(CommandOwnershipContract::Container)
            );
            assert_eq!(
                classify(&task(&["docker", "run", "--gpus", "all", "eval"])),
                Err(ContainerClient)
            );
        }
    }

    #[test]
    fn entry_point_must_be_an_inspectable_native_executable() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let script = root.join("prepared-command");
        fs::write(&script, "#!/bin/sh\nexec /opt/gpu/bench &\n").unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let unknown = root.join("unknown-format");
        fs::write(&unknown, b"\0\0\0\0payload").unwrap();
        // a renamed link to a shell is judged by its target
        let renamed = root.join("bench");
        symlink("/bin/sh", &renamed).unwrap();

        assert_eq!(
            inspect_foreground_entry_point(&script),
            Err(ResourceTaskOwnershipRisk::ScriptEntryPoint)
        );
        assert_eq!(
            inspect_foreground_entry_point(&unknown),
            Err(ResourceTaskOwnershipRisk::UninspectableEntryPoint)
        );
        assert_eq!(
            inspect_foreground_entry_point(&root.join("missing")),
            Err(ResourceTaskOwnershipRisk::UninspectableEntryPoint)
        );
        assert!(inspect_foreground_entry_point(&renamed).is_err());
        assert_eq!(
            inspect_foreground_entry_point(Path::new("/bin/echo")),
            Ok(())
        );
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::OnceLock;

    // appends one `x` to its first argument, then waits until its optional second
    // argument exists, so a test can count launches and hold a task running
    const SOURCE: &str = r#"
#include <stdio.h>
#include <unistd.h>
int main(int argc, char **argv) {
    if (argc < 2) return 2;
    FILE *marker = fopen(argv[1], "a");
    if (marker == NULL || fputs("x", marker) < 0 || fclose(marker) != 0) return 1;
    while (argc > 2 && access(argv[2], F_OK) != 0) usleep(20000);
    return 0;
}
"#;

    /// Native stand-in for a prepared GPU command, compiled once per test process
    ///
    /// A shell script is refused by the foreground contract, and macOS kills a
    /// copied system binary, so the tests build their own native executable
    pub(crate) fn native_fake_command() -> &'static Path {
        static COMMAND: OnceLock<PathBuf> = OnceLock::new();
        COMMAND.get_or_init(|| {
            let directory = std::env::temp_dir()
                .join(format!("homebased-fake-gpu-command-{}", std::process::id()));
            std::fs::create_dir_all(&directory).unwrap();
            let source = directory.join("fake-gpu-command.c");
            std::fs::write(&source, SOURCE).unwrap();
            let binary = directory.join("fake-gpu-command");
            let status = Command::new("cc")
                .arg(&source)
                .arg("-o")
                .arg(&binary)
                .status()
                .unwrap();
            assert!(status.success(), "cc must build the fake GPU command");
            binary
        })
    }
}
