//! Docker CLI calls that Homebased builds from a typed container workload
//!
//! Every call names the container by its full saved ID or by the fixed task
//! name. Absence is proven only by a successful `docker container ls` that
//! lists nothing for that ID or name; any failure is unknown, never absent

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use tokio::process::{Child, Command};
use tokio::time;

use super::lifecycle::{
    ContainerEngine, ContainerObservation, ContainerProbe, ContainerStatus, EngineError,
    LogFollower,
};
use super::spec::{ContainerUser, ContainerWorkload};
use crate::domain::{ContainerId, TaskEnv, TaskId};
use crate::resource::ResourceId;

/// Label that names the Homebased task of a container
pub const TASK_LABEL: &str = "homebased.task";

/// Label that names the resource whose loan runs a container
pub const RESOURCE_LABEL: &str = "homebased.resource";

/// Longest wait for one Docker CLI call that does not block on the container
const CALL_TIMEOUT: Duration = Duration::from_secs(120);

/// Longest stderr text kept in one error message
const ERROR_TEXT_MAX: usize = 2000;

/// Fixed container name of one task. A second create with this name conflicts
#[must_use]
pub fn container_name(task: TaskId) -> String {
    format!("homebased-{task}")
}

/// Inputs for `docker container create` that do not come from the workload
#[derive(Debug, Clone)]
pub struct CreateContext<'a> {
    /// Task that owns the container
    pub task: TaskId,
    /// Resource whose loan runs the container, for resource work
    pub resource: Option<ResourceId>,
    /// File where Docker writes the new container ID
    pub cidfile: &'a Path,
    /// User when the workload names none: the daemon's user
    pub default_user: ContainerUser,
}

/// Build the arguments of `docker container create` for one task
///
/// The container gets `--init`, the fixed name and labels, a memory limit that
/// also caps swap, and no restart policy or `--rm`, so Homebased can read the
/// exit code before it removes the container. `--pull never` keeps launch from
/// fetching images
#[must_use]
pub fn create_args(workload: &ContainerWorkload, context: &CreateContext<'_>) -> Vec<String> {
    let memory = workload.memory.bytes().to_string();
    let mut args: Vec<String> = vec![
        "container".into(),
        "create".into(),
        "--name".into(),
        container_name(context.task),
        "--label".into(),
        format!("{TASK_LABEL}={}", context.task),
    ];
    if let Some(resource) = context.resource {
        args.push("--label".into());
        args.push(format!("{RESOURCE_LABEL}={}", resource.as_uuid()));
    }
    args.extend([
        "--init".into(),
        "--cidfile".into(),
        context.cidfile.to_string_lossy().into_owned(),
        "--pull".into(),
        "never".into(),
        "--restart".into(),
        "no".into(),
        "--memory".into(),
        memory.clone(),
        "--memory-swap".into(),
        memory,
        "--user".into(),
        workload.user.unwrap_or(context.default_user).to_string(),
    ]);
    if let Some(gpus) = &workload.gpus {
        args.push("--gpus".into());
        args.push(gpus.docker_value());
    }
    if let Some(workdir) = &workload.workdir {
        args.push("--workdir".into());
        args.push(workdir.to_string_lossy().into_owned());
    }
    if let Some(entrypoint) = &workload.entrypoint {
        args.push("--entrypoint".into());
        args.push(entrypoint.program().to_owned());
    }
    for mount in &workload.mounts {
        let mut fields = vec![
            "type=bind".to_owned(),
            csv_field(&format!("source={}", mount.source.to_string_lossy())),
            csv_field(&format!("target={}", mount.target.to_string_lossy())),
        ];
        if mount.read_only {
            fields.push("readonly".into());
        }
        args.push("--mount".into());
        args.push(fields.join(","));
    }
    for (name, value) in &workload.env {
        args.push("--env".into());
        args.push(format!("{}={value}", name.as_str()));
    }
    args.push(workload.image.as_str().to_owned());
    args.extend(workload.command_after_image());
    args
}

/// Quote one field of Docker's CSV-parsed `--mount` value when it needs it
fn csv_field(field: &str) -> String {
    if field.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_owned()
    }
}

/// Docker Engine reached through the `docker` CLI
#[derive(Debug, Clone)]
pub struct DockerCli {
    program: PathBuf,
    env: TaskEnv,
}

impl DockerCli {
    /// Use the resolved `docker` executable with the task's saved environment
    #[must_use]
    pub fn new(program: PathBuf, env: TaskEnv) -> Self {
        Self { program, env }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command
            .env("PATH", &self.env.path)
            .env("HOME", &self.env.home)
            .stdin(Stdio::null())
            .kill_on_drop(true);
        // a client that outlived a killed worker would duplicate the adopting
        // worker's log stream; the container itself runs under dockerd
        #[cfg(target_os = "linux")]
        unsafe {
            command.pre_exec(|| {
                nix::sys::prctl::set_pdeathsig(Some(nix::sys::signal::Signal::SIGKILL))
                    .map_err(std::io::Error::from)
            });
        }
        command
    }

    /// Run one bounded call and return its stdout on success
    async fn call(&self, args: &[&str]) -> Result<String, CallFailure> {
        let mut command = self.command();
        command
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = match time::timeout(CALL_TIMEOUT, command.output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                return Err(CallFailure {
                    stderr: format!("run {}: {error}", self.program.display()),
                });
            }
            Err(_) => {
                return Err(CallFailure {
                    stderr: format!(
                        "docker {} did not finish within {}s",
                        args.first().copied().unwrap_or_default(),
                        CALL_TIMEOUT.as_secs()
                    ),
                });
            }
        };
        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
        }
        Err(CallFailure {
            stderr: bounded(String::from_utf8_lossy(&output.stderr).trim()),
        })
    }

    /// List the containers whose ID or exact name matches, without inspecting them
    async fn list(&self, filter: &str) -> Result<Vec<(String, String)>, EngineError> {
        let stdout = self
            .call(&[
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                filter,
                "--format",
                "{{.ID}}\t{{.Names}}",
            ])
            .await
            .map_err(CallFailure::unavailable)?;
        Ok(stdout
            .lines()
            .filter_map(|line| line.split_once('\t'))
            .map(|(id, names)| (id.trim().to_owned(), names.trim().to_owned()))
            .collect())
    }

    async fn inspect_id(&self, id: &str) -> Result<ContainerObservation, EngineError> {
        let stdout = self
            .call(&["container", "inspect", "--format", "{{json .}}", id])
            .await
            .map_err(CallFailure::unavailable)?;
        let inspected: InspectedContainer = serde_json::from_str(stdout.trim())
            .map_err(|error| EngineError::Unavailable(format!("parse docker inspect: {error}")))?;
        inspected.observation()
    }
}

impl ContainerEngine for DockerCli {
    type Logs = DockerLogs;

    async fn create(&self, args: &[String]) -> Result<ContainerId, EngineError> {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let stdout = self.call(&args).await.map_err(CallFailure::unavailable)?;
        ContainerId::parse(stdout.trim()).map_err(|error| {
            EngineError::Unavailable(format!("docker create returned no container id: {error}"))
        })
    }

    async fn start(&self, id: &ContainerId) -> Result<(), EngineError> {
        self.call(&["container", "start", id.as_str()])
            .await
            .map(|_| ())
            .map_err(CallFailure::unavailable)
    }

    async fn probe_id(&self, id: &ContainerId) -> Result<ContainerProbe, EngineError> {
        let listed = self.list(&format!("id={id}")).await?;
        if !listed.iter().any(|(listed_id, _)| listed_id == id.as_str()) {
            return Ok(ContainerProbe::Absent);
        }
        self.inspect_id(id.as_str())
            .await
            .map(ContainerProbe::Present)
    }

    async fn probe_name(&self, name: &str) -> Result<ContainerProbe, EngineError> {
        let listed = self.list(&format!("name={name}")).await?;
        // the name filter matches substrings, so keep only the exact name
        let Some((id, _)) = listed
            .iter()
            .find(|(_, names)| names.split(',').any(|listed| listed == name))
        else {
            return Ok(ContainerProbe::Absent);
        };
        self.inspect_id(id).await.map(ContainerProbe::Present)
    }

    async fn wait(&self, id: &ContainerId) -> Result<(), EngineError> {
        let mut command = self.command();
        command
            .args(["container", "wait", id.as_str()])
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let output = command
            .output()
            .await
            .map_err(|error| EngineError::Unavailable(format!("run docker wait: {error}")))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(EngineError::Unavailable(bounded(
                String::from_utf8_lossy(&output.stderr).trim(),
            )))
        }
    }

    async fn stop(&self, id: &ContainerId, grace: Duration) -> Result<(), EngineError> {
        let seconds = grace.as_secs().max(1).to_string();
        self.call(&["container", "stop", "-t", &seconds, id.as_str()])
            .await
            .map(|_| ())
            .map_err(CallFailure::unavailable)
    }

    async fn kill(&self, id: &ContainerId) -> Result<(), EngineError> {
        self.call(&["container", "kill", id.as_str()])
            .await
            .map(|_| ())
            .map_err(CallFailure::unavailable)
    }

    async fn remove(&self, id: &ContainerId) -> Result<(), EngineError> {
        // no --force: removal must never stop a container that still runs
        self.call(&["container", "rm", id.as_str()])
            .await
            .map(|_| ())
            .map_err(CallFailure::unavailable)
    }

    fn follow_logs(
        &self,
        id: &ContainerId,
        since: Option<DateTime<Utc>>,
        output: &Path,
    ) -> Result<Self::Logs, EngineError> {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(output)
            .map_err(|error| EngineError::Unavailable(format!("open output log: {error}")))?;
        let stderr = log
            .try_clone()
            .map_err(|error| EngineError::Unavailable(format!("open output log: {error}")))?;
        let mut command = self.command();
        command.args(["container", "logs", "--follow"]);
        if let Some(since) = since {
            command.args([
                "--since",
                &since.to_rfc3339_opts(SecondsFormat::Nanos, true),
            ]);
        }
        command
            .arg(id.as_str())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr));
        let child = command
            .spawn()
            .map_err(|error| EngineError::Unavailable(format!("run docker logs: {error}")))?;
        Ok(DockerLogs { child })
    }
}

/// Running `docker container logs --follow` child that appends to `output.log`
pub struct DockerLogs {
    child: Child,
}

impl LogFollower for DockerLogs {
    async fn finish(mut self, drain: Duration) {
        // the follower ends by itself once the container stops
        if time::timeout(drain, self.child.wait()).await.is_err()
            && let Err(error) = self.child.kill().await
        {
            tracing::warn!("stop docker logs: {error}");
        }
    }
}

/// Failed Docker CLI call with its captured error text
struct CallFailure {
    stderr: String,
}

impl CallFailure {
    fn unavailable(self) -> EngineError {
        EngineError::Unavailable(self.stderr)
    }
}

fn bounded(text: &str) -> String {
    if text.len() <= ERROR_TEXT_MAX {
        return text.to_owned();
    }
    let mut end = ERROR_TEXT_MAX;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// Fields of `docker container inspect` that the witness reads
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InspectedContainer {
    id: String,
    state: InspectedState,
    #[serde(default)]
    config: Option<InspectedConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InspectedState {
    status: String,
    #[serde(default)]
    exit_code: i32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InspectedConfig {
    #[serde(default)]
    labels: Option<std::collections::BTreeMap<String, String>>,
}

impl InspectedContainer {
    fn observation(self) -> Result<ContainerObservation, EngineError> {
        let id = ContainerId::parse(&self.id)
            .map_err(|error| EngineError::Unavailable(error.to_string()))?;
        let exit_code = self.state.exit_code;
        let status = match self.state.status.as_str() {
            "created" => ContainerStatus::Created,
            "running" => ContainerStatus::Running,
            "paused" => ContainerStatus::Paused,
            "restarting" => ContainerStatus::Restarting,
            "removing" => ContainerStatus::Removing,
            "exited" => ContainerStatus::Exited { exit_code },
            "dead" => ContainerStatus::Dead { exit_code },
            other => {
                return Err(EngineError::Unavailable(format!(
                    "docker reported unknown container status {other:?}"
                )));
            }
        };
        let task_label = self
            .config
            .and_then(|config| config.labels)
            .and_then(|mut labels| labels.remove(TASK_LABEL));
        Ok(ContainerObservation {
            id,
            status,
            task_label,
        })
    }
}

/// Open the output log for a note from Homebased itself
pub(crate) fn append_note(output: &Path, note: &str) {
    use std::io::Write;

    let written = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(output)
        .and_then(|mut log: File| writeln!(log, "homebased: {note}"));
    if let Err(error) = written {
        tracing::warn!(path = %output.display(), "write container note: {error}");
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;

    use super::{CreateContext, InspectedContainer, create_args, csv_field};
    use crate::container::lifecycle::ContainerStatus;
    use crate::container::spec::{ContainerUser, ContainerWorkload};
    use crate::domain::TaskId;
    use crate::resource::ResourceId;

    const DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn create_argv_comes_only_from_the_typed_spec() {
        let task: TaskId = "01a0ab97-a7aa-7463-a5b0-8d500e40e431".parse().unwrap();
        let resource = ResourceId::new();
        let workload = ContainerWorkload::from_value(&json!({
            "image": format!("eval@{DIGEST}"),
            "entrypoint": ["/usr/bin/python3", "-m", "eval"],
            "args": ["--ckpt", "/data/ckpt", "--note", "a b; rm -rf /"],
            "gpus": [0, 1],
            "memory": "2g",
            "workdir": "/work",
            "mounts": [
                { "source": "/shared/ckpt", "target": "/data/ckpt", "read_only": true },
                { "source": "/shared/a,b", "target": "/out" }
            ],
            "env": { "B": "two words", "A": "1" }
        }))
        .unwrap();
        let args = create_args(
            &workload,
            &CreateContext {
                task,
                resource: Some(resource),
                cidfile: Path::new("/home/me/.homebased/tasks/t/container.cid"),
                default_user: ContainerUser { uid: 501, gid: 20 },
            },
        );
        let expected: Vec<String> = [
            "container",
            "create",
            "--name",
            "homebased-01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "--label",
            "homebased.task=01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "--label",
            &format!("homebased.resource={}", resource.as_uuid()),
            "--init",
            "--cidfile",
            "/home/me/.homebased/tasks/t/container.cid",
            "--pull",
            "never",
            "--restart",
            "no",
            "--memory",
            "2147483648",
            "--memory-swap",
            "2147483648",
            "--user",
            "501:20",
            "--gpus",
            "\"device=0,1\"",
            "--workdir",
            "/work",
            "--entrypoint",
            "/usr/bin/python3",
            "--mount",
            "type=bind,source=/shared/ckpt,target=/data/ckpt,readonly",
            "--mount",
            "type=bind,\"source=/shared/a,b\",target=/out",
            "--env",
            "A=1",
            "--env",
            "B=two words",
            &format!("eval@{DIGEST}"),
            "-m",
            "eval",
            "--ckpt",
            "/data/ckpt",
            "--note",
            "a b; rm -rf /",
        ]
        .iter()
        .map(|part| (*part).to_owned())
        .collect();
        assert_eq!(args, expected);

        let minimal = ContainerWorkload::from_value(&json!({
            "image": DIGEST,
            "memory": 6291456,
            "user": "1000:1000"
        }))
        .unwrap();
        let args = create_args(
            &minimal,
            &CreateContext {
                task,
                resource: None,
                cidfile: Path::new("/t/container.cid"),
                default_user: ContainerUser { uid: 501, gid: 20 },
            },
        );
        assert!(
            !args
                .iter()
                .any(|arg| arg == "--gpus" || arg == "--entrypoint")
        );
        assert!(
            !args
                .iter()
                .any(|arg| arg.starts_with("homebased.resource="))
        );
        assert_eq!(args.last().map(String::as_str), Some(DIGEST));
        let user = args.iter().position(|arg| arg == "--user").unwrap();
        assert_eq!(args[user + 1], "1000:1000");
    }

    #[test]
    fn csv_fields_quote_only_when_needed() {
        assert_eq!(csv_field("source=/a"), "source=/a");
        assert_eq!(csv_field("source=/a,b"), "\"source=/a,b\"");
        assert_eq!(csv_field("source=/a\"b"), "\"source=/a\"\"b\"");
    }

    #[test]
    fn inspect_output_maps_to_the_witness_states() {
        let id = "a".repeat(64);
        let decode = |status: &str, exit_code: i32| {
            serde_json::from_value::<InspectedContainer>(json!({
                "Id": id,
                "Name": "/homebased-x",
                "State": { "Status": status, "ExitCode": exit_code, "Running": false },
                "Config": { "Labels": { "homebased.task": "t" }, "Image": "x" }
            }))
            .unwrap()
            .observation()
        };
        assert_eq!(
            decode("created", 0).unwrap().status,
            ContainerStatus::Created
        );
        assert_eq!(
            decode("running", 0).unwrap().status,
            ContainerStatus::Running
        );
        assert_eq!(
            decode("exited", 3).unwrap().status,
            ContainerStatus::Exited { exit_code: 3 }
        );
        assert_eq!(
            decode("dead", 137).unwrap().status,
            ContainerStatus::Dead { exit_code: 137 }
        );
        assert_eq!(
            decode("exited", 0).unwrap().task_label.as_deref(),
            Some("t")
        );
        assert!(decode("unknown", 0).is_err());
    }
}
