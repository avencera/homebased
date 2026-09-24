//! Container workload spec: pinned image, argv, limits, mounts, and refusal rules
//!
//! Every field maps to one Docker CLI option that Homebased builds itself. An
//! option that the type does not model, such as privileged mode, a host PID or
//! IPC namespace, added capabilities, extra devices, or a restart policy, is an
//! unknown field and is refused. A container that could reach a Docker or
//! containerd socket could start work that no witness covers, so such a mount
//! is refused too

use std::collections::BTreeMap;
use std::fmt;
use std::os::unix::fs::FileTypeExt;
use std::path::{Component, Path, PathBuf};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};

use crate::error::AppError;

/// Smallest memory limit that Docker Engine accepts
pub const MIN_MEMORY_BYTES: u64 = 6 * 1024 * 1024;

/// Known Docker, containerd, and Podman API sockets
///
/// A mount source that is one of these paths, or a directory above one, is refused
const DAEMON_SOCKETS: &[&str] = &[
    "/var/run/docker.sock",
    "/run/docker.sock",
    "/var/run/docker/containerd/containerd.sock",
    "/run/docker/containerd/containerd.sock",
    "/var/run/containerd/containerd.sock",
    "/run/containerd/containerd.sock",
    "/var/run/podman/podman.sock",
    "/run/podman/podman.sock",
];

/// Socket file names refused wherever they appear, which covers rootless daemons
const DAEMON_SOCKET_NAMES: &[&str] = &[
    "docker.sock",
    "containerd.sock",
    "containerd.sock.ttrpc",
    "podman.sock",
];

/// Why a container workload failed validation
///
/// `pointer` is relative to the workload object, so the spec parser can prefix
/// `/workload`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ContainerSpecError {
    /// JSON pointer inside the workload object
    pub pointer: String,
    /// Offending value
    pub value: Value,
    /// Human-readable reason
    pub message: String,
}

impl ContainerSpecError {
    fn new(pointer: impl Into<String>, value: Value, message: impl Into<String>) -> Self {
        Self {
            pointer: pointer.into(),
            value,
            message: message.into(),
        }
    }

    /// Convert into `AppError::InvalidSpec` under the workload pointer
    #[must_use]
    pub fn into_invalid_spec(self, prefix: &str) -> AppError {
        AppError::InvalidSpec {
            pointer: format!("{prefix}{}", self.pointer),
            value: self.value,
            message: self.message,
        }
    }
}

/// Image pinned by digest: `sha256:<64 hex>` image ID or `name@sha256:<64 hex>`
///
/// A tag can move to other content, so a tag alone, or a tag beside a digest,
/// is refused
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImageReference(String);

impl ImageReference {
    /// Validate one pinned image reference
    pub fn parse(raw: &str) -> Result<Self, String> {
        let (name, digest) = match raw.split_once('@') {
            Some((name, digest)) => (Some(name), digest),
            None => (None, raw),
        };
        let Some(hex) = digest.strip_prefix("sha256:") else {
            return Err(
                "image must be pinned as sha256:<64 hex> or name@sha256:<64 hex>; a tag alone is refused"
                    .into(),
            );
        };
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err("image digest must be 64 lowercase hexadecimal characters".into());
        }
        if let Some(name) = name {
            validate_repository_name(name)?;
        }
        Ok(Self(raw.to_owned()))
    }

    /// Reference as passed to Docker
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Short label: the repository name, or the first 12 digest characters
    #[must_use]
    pub fn short_name(&self) -> &str {
        match self.0.split_once('@') {
            Some((name, _)) => name,
            None => &self.0[..self.0.len().min("sha256:".len() + 12)],
        }
    }
}

impl fmt::Display for ImageReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for ImageReference {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ImageReference {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(D::Error::custom)
    }
}

/// Check a repository name against the Docker reference grammar
///
/// The first component is a registry host when it contains `.` or `:`, or is
/// `localhost`. Every other component is lowercase alphanumeric runs joined by
/// `.`, `_`, `__`, or runs of `-`. A tag (`:` in the last component) is refused
fn validate_repository_name(name: &str) -> Result<(), String> {
    let invalid = || format!("image name {name:?} is not a valid repository name without a tag");
    if name.is_empty() || name.len() > 255 {
        return Err(invalid());
    }
    let mut components: Vec<&str> = name.split('/').collect();
    if components.len() > 1 {
        let first = components[0];
        if first.contains('.') || first.contains(':') || first == "localhost" {
            validate_registry_host(first).ok_or_else(invalid)?;
            components.remove(0);
        }
    }
    if components.is_empty()
        || !components
            .iter()
            .all(|component| valid_path_component(component))
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_registry_host(host: &str) -> Option<()> {
    let (host, port) = match host.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (host, None),
    };
    if let Some(port) = port
        && (port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }
    let labels_valid = !host.is_empty()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        });
    labels_valid.then_some(())
}

fn valid_path_component(component: &str) -> bool {
    let bytes = component.as_bytes();
    let alnum = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if bytes.is_empty() || !alnum(bytes[0]) || !alnum(bytes[bytes.len() - 1]) {
        return false;
    }
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if alnum(byte) {
            index += 1;
            continue;
        }
        let separator_end = match byte {
            b'.' => index + 1,
            b'_' if bytes.get(index + 1) == Some(&b'_') => index + 2,
            b'_' => index + 1,
            b'-' => {
                let mut end = index;
                while bytes.get(end) == Some(&b'-') {
                    end += 1;
                }
                end
            }
            _ => return false,
        };
        // a separator must sit between two alphanumeric runs
        if !bytes.get(separator_end).copied().is_some_and(alnum) {
            return false;
        }
        index = separator_end;
    }
    true
}

/// Entrypoint argv head that replaces the image entrypoint
///
/// Docker's `--entrypoint` takes one executable, so the first element becomes
/// that option and the rest go before `args` on the command line
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerEntrypoint(Vec<String>);

impl ContainerEntrypoint {
    /// Executable that replaces the image entrypoint
    #[must_use]
    pub fn program(&self) -> &str {
        &self.0[0]
    }

    /// Entrypoint arguments placed before the workload `args`
    #[must_use]
    pub fn args(&self) -> &[String] {
        &self.0[1..]
    }

    /// Full entrypoint argv
    #[must_use]
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }
}

impl Serialize for ContainerEntrypoint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

/// GPUs that the container may use
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GpuRequest {
    /// Every GPU on the authority
    All,
    /// Exact device indices, unique and non-empty
    Devices(Vec<u32>),
}

impl GpuRequest {
    /// Value for Docker's `--gpus` option
    ///
    /// Docker reads the value as one CSV record, so a device list is quoted
    #[must_use]
    pub fn docker_value(&self) -> String {
        match self {
            Self::All => "all".into(),
            Self::Devices(devices) => {
                let list: Vec<String> = devices.iter().map(u32::to_string).collect();
                format!("\"device={}\"", list.join(","))
            }
        }
    }
}

impl Serialize for GpuRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::All => serializer.serialize_str("all"),
            Self::Devices(devices) => devices.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for GpuRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        validate_gpus(&value).map_err(|error| D::Error::custom(error.message))
    }
}

/// Memory limit in bytes, also used as the memory-plus-swap limit
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(u64);

impl ByteSize {
    /// Parse a byte count or a size with a binary unit such as `512m` or `24GiB`
    pub fn parse(raw: &str) -> Result<Self, String> {
        let invalid =
            || format!("memory {raw:?} must be a byte count or a size such as 512m, 16g, or 24GiB");
        let trimmed = raw.trim();
        let digits_end = trimmed
            .find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(trimmed.len());
        let (digits, unit) = trimmed.split_at(digits_end);
        let value: u64 = digits.parse().map_err(|_| invalid())?;
        let shift = match unit.trim().to_ascii_lowercase().as_str() {
            "" | "b" => 0,
            "k" | "kb" | "kib" => 10,
            "m" | "mb" | "mib" => 20,
            "g" | "gb" | "gib" => 30,
            "t" | "tb" | "tib" => 40,
            _ => return Err(invalid()),
        };
        let bytes = value.checked_mul(1 << shift).ok_or_else(invalid)?;
        Self::from_bytes(bytes)
    }

    /// Validate an exact byte count
    pub fn from_bytes(bytes: u64) -> Result<Self, String> {
        if bytes < MIN_MEMORY_BYTES {
            return Err(format!(
                "memory must be at least {MIN_MEMORY_BYTES} bytes (6 MiB)"
            ));
        }
        Ok(Self(bytes))
    }

    /// Size in bytes
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.0
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0)
    }
}

/// Numeric container user and group
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContainerUser {
    /// User ID inside the container
    pub uid: u32,
    /// Group ID inside the container
    pub gid: u32,
}

impl ContainerUser {
    /// Parse `uid:gid`
    pub fn parse(raw: &str) -> Result<Self, String> {
        let invalid = || format!("user {raw:?} must be numeric uid:gid");
        let (uid, gid) = raw.split_once(':').ok_or_else(invalid)?;
        let number = |part: &str| {
            if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid());
            }
            part.parse::<u32>().map_err(|_| invalid())
        };
        Ok(Self {
            uid: number(uid)?,
            gid: number(gid)?,
        })
    }

    /// User and group of this process, the default for a container
    #[must_use]
    pub fn current() -> Self {
        Self {
            uid: nix::unistd::getuid().as_raw(),
            gid: nix::unistd::getgid().as_raw(),
        }
    }
}

impl fmt::Display for ContainerUser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.uid, self.gid)
    }
}

impl Serialize for ContainerUser {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// One host directory or file bound into the container
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContainerMount {
    /// Absolute host path. It must exist on the authority
    pub source: PathBuf,
    /// Absolute container path
    pub target: PathBuf,
    /// Mount read-only
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
}

/// Environment variable name: a letter or `_`, then letters, digits, or `_`
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct EnvName(String);

impl EnvName {
    /// Validate one environment variable name
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut bytes = raw.bytes();
        let valid = bytes
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == b'_')
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
        if valid {
            Ok(Self(raw.to_owned()))
        } else {
            Err(format!(
                "environment name {raw:?} must match [A-Za-z_][A-Za-z0-9_]*"
            ))
        }
    }

    /// Validated name
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Typed Docker container workload
///
/// Homebased builds every Docker CLI option from these fields. The container
/// always runs with `--init`, the daemon's user unless `user` is set, a memory
/// limit that also caps swap, and no restart policy
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Value")]
pub struct ContainerWorkload {
    /// Image pinned by digest
    pub image: ImageReference,
    /// Argv head that replaces the image entrypoint
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<ContainerEntrypoint>,
    /// Arguments passed unchanged; not a shell string
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// GPUs the container may use. Required for resource work
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpus: Option<GpuRequest>,
    /// Memory limit, also used as the memory-plus-swap limit
    pub memory: ByteSize,
    /// Numeric user; defaults to the daemon's user
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<ContainerUser>,
    /// Absolute working directory inside the container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<PathBuf>,
    /// Host paths bound into the container
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<ContainerMount>,
    /// Explicit environment values; nothing passes through from the host
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<EnvName, String>,
}

impl TryFrom<Value> for ContainerWorkload {
    type Error = ContainerSpecError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::from_value(&value)
    }
}

/// Wire shape before field rules. Unknown keys fail here
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContainerWorkloadWire {
    image: String,
    #[serde(default)]
    entrypoint: Option<Vec<String>>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    gpus: Option<Value>,
    memory: Value,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    workdir: Option<String>,
    #[serde(default)]
    mounts: Vec<ContainerMountWire>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContainerMountWire {
    source: String,
    target: String,
    #[serde(default)]
    read_only: bool,
}

/// Docker options that the type refuses, with the reason given to the submitter
const REFUSED_OPTIONS: &[(&str, &str)] = &[
    ("privileged", "privileged mode"),
    ("pid", "a host PID namespace"),
    ("ipc", "a host IPC namespace"),
    ("network", "a network mode"),
    ("cap_add", "added capabilities"),
    ("capabilities", "added capabilities"),
    ("devices", "devices beyond the GPU request"),
    ("device", "devices beyond the GPU request"),
    ("restart", "a restart policy"),
    ("security_opt", "security options"),
    ("volumes", "volumes; use mounts"),
    ("command", "a command; use entrypoint and args"),
    ("rm", "automatic removal"),
    ("detach", "detached clients"),
    ("name", "a container name"),
    ("labels", "labels"),
];

impl ContainerWorkload {
    /// Parse and validate a workload object without its `type` key
    ///
    /// Errors carry a pointer relative to the workload object
    pub fn from_value(value: &Value) -> Result<Self, ContainerSpecError> {
        if let Value::Object(map) = value {
            for (key, what) in REFUSED_OPTIONS {
                if map.contains_key(*key) {
                    return Err(ContainerSpecError::new(
                        format!("/{key}"),
                        map[*key].clone(),
                        format!("container workloads do not support {what}"),
                    ));
                }
            }
            for key in ["entrypoint", "gpus", "user", "workdir"] {
                if map.get(key).is_some_and(Value::is_null) {
                    return Err(ContainerSpecError::new(
                        format!("/{key}"),
                        Value::Null,
                        format!("{key} must not be null; omit it instead"),
                    ));
                }
            }
        }
        let wire: ContainerWorkloadWire =
            serde_path_to_error::deserialize(value).map_err(|err| {
                let pointer = crate::spec::json_pointer(err.path());
                let field = value.pointer(&pointer).cloned().unwrap_or(Value::Null);
                ContainerSpecError::new(pointer, field, err.inner().to_string())
            })?;
        wire.validate()
    }

    /// Full argv the container runs when an entrypoint replaces the image's
    #[must_use]
    pub fn command_after_image(&self) -> Vec<String> {
        let mut command = Vec::new();
        if let Some(entrypoint) = &self.entrypoint {
            command.extend(entrypoint.args().iter().cloned());
        }
        command.extend(self.args.iter().cloned());
        command
    }
}

impl ContainerWorkloadWire {
    fn validate(self) -> Result<ContainerWorkload, ContainerSpecError> {
        let image = ImageReference::parse(&self.image)
            .map_err(|message| ContainerSpecError::new("/image", json!(self.image), message))?;
        let entrypoint = match self.entrypoint {
            None => None,
            Some(argv) => Some(validate_entrypoint(argv)?),
        };
        for (index, arg) in self.args.iter().enumerate() {
            if arg.contains('\0') {
                return Err(ContainerSpecError::new(
                    format!("/args/{index}"),
                    json!(arg),
                    "args must not contain a NUL byte",
                ));
            }
        }
        let gpus = self.gpus.map(|value| validate_gpus(&value)).transpose()?;
        let memory = validate_memory(&self.memory)?;
        let user = self
            .user
            .map(|raw| {
                ContainerUser::parse(&raw)
                    .map_err(|message| ContainerSpecError::new("/user", json!(raw), message))
            })
            .transpose()?;
        let workdir = self
            .workdir
            .map(|raw| {
                container_path(&raw)
                    .map_err(|message| ContainerSpecError::new("/workdir", json!(raw), message))
            })
            .transpose()?;
        let mounts = validate_mounts(self.mounts)?;
        let mut env = BTreeMap::new();
        for (name, value) in self.env {
            let pointer = format!("/env/{}", crate::spec::escape_token(&name));
            let parsed = EnvName::parse(&name)
                .map_err(|message| ContainerSpecError::new(&pointer, json!(name), message))?;
            if value.contains('\0') {
                return Err(ContainerSpecError::new(
                    pointer,
                    json!(value),
                    "environment values must not contain a NUL byte",
                ));
            }
            env.insert(parsed, value);
        }
        Ok(ContainerWorkload {
            image,
            entrypoint,
            args: self.args,
            gpus,
            memory,
            user,
            workdir,
            mounts,
            env,
        })
    }
}

fn validate_entrypoint(argv: Vec<String>) -> Result<ContainerEntrypoint, ContainerSpecError> {
    if argv.first().is_none_or(String::is_empty) {
        return Err(ContainerSpecError::new(
            "/entrypoint",
            json!(argv),
            "entrypoint must be a non-empty argv whose first element is not empty",
        ));
    }
    if let Some(index) = argv.iter().position(|part| part.contains('\0')) {
        return Err(ContainerSpecError::new(
            format!("/entrypoint/{index}"),
            json!(argv[index]),
            "entrypoint must not contain a NUL byte",
        ));
    }
    Ok(ContainerEntrypoint(argv))
}

fn validate_gpus(value: &Value) -> Result<GpuRequest, ContainerSpecError> {
    let invalid = || {
        ContainerSpecError::new(
            "/gpus",
            value.clone(),
            "gpus must be \"all\" or a non-empty array of unique device indices",
        )
    };
    match value {
        Value::String(text) if text == "all" => Ok(GpuRequest::All),
        Value::Array(items) if !items.is_empty() => {
            let mut devices = Vec::with_capacity(items.len());
            for item in items {
                let index = item
                    .as_u64()
                    .and_then(|index| u32::try_from(index).ok())
                    .ok_or_else(invalid)?;
                if devices.contains(&index) {
                    return Err(invalid());
                }
                devices.push(index);
            }
            Ok(GpuRequest::Devices(devices))
        }
        _ => Err(invalid()),
    }
}

fn validate_memory(value: &Value) -> Result<ByteSize, ContainerSpecError> {
    let parsed = match value {
        Value::Number(number) => number
            .as_u64()
            .ok_or_else(|| "memory must be a non-negative whole byte count".to_owned())
            .and_then(ByteSize::from_bytes),
        Value::String(text) => ByteSize::parse(text),
        _ => Err("memory must be a byte count or a size string such as 16g".to_owned()),
    };
    parsed.map_err(|message| ContainerSpecError::new("/memory", value.clone(), message))
}

fn validate_mounts(
    mounts: Vec<ContainerMountWire>,
) -> Result<Vec<ContainerMount>, ContainerSpecError> {
    let mut validated: Vec<ContainerMount> = Vec::with_capacity(mounts.len());
    for (index, mount) in mounts.into_iter().enumerate() {
        let source = host_path(&mount.source).map_err(|message| {
            ContainerSpecError::new(
                format!("/mounts/{index}/source"),
                json!(mount.source),
                message,
            )
        })?;
        if let Some(reason) = daemon_socket_exposure(&source) {
            return Err(ContainerSpecError::new(
                format!("/mounts/{index}/source"),
                json!(mount.source),
                reason,
            ));
        }
        let target = container_path(&mount.target).map_err(|message| {
            ContainerSpecError::new(
                format!("/mounts/{index}/target"),
                json!(mount.target),
                message,
            )
        })?;
        if target == Path::new("/") {
            return Err(ContainerSpecError::new(
                format!("/mounts/{index}/target"),
                json!(mount.target),
                "mount target must not be the container root",
            ));
        }
        if validated.iter().any(|earlier| earlier.target == target) {
            return Err(ContainerSpecError::new(
                format!("/mounts/{index}/target"),
                json!(mount.target),
                "mount targets must be unique",
            ));
        }
        validated.push(ContainerMount {
            source,
            target,
            read_only: mount.read_only,
        });
    }
    Ok(validated)
}

/// Absolute host path with no `.`, `..`, or empty components
fn host_path(raw: &str) -> Result<PathBuf, String> {
    let path = normal_absolute_path(raw, "mount source")?;
    if path == Path::new("/") {
        return Err("mount source must not be the host root".into());
    }
    Ok(path)
}

/// Absolute container path with no `.` or `..` components
fn container_path(raw: &str) -> Result<PathBuf, String> {
    normal_absolute_path(raw, "container path")
}

fn normal_absolute_path(raw: &str, what: &str) -> Result<PathBuf, String> {
    if raw.contains('\0') {
        return Err(format!("{what} must not contain a NUL byte"));
    }
    if !raw.starts_with('/') {
        return Err(format!("{what} must be absolute"));
    }
    let components_normal = raw.split('/').skip(1).enumerate().all(|(index, part)| {
        // only the root path "/" has an empty final component
        (!part.is_empty() || (index == 0 && raw == "/")) && part != "." && part != ".."
    });
    if !components_normal {
        return Err(format!(
            "{what} must not contain empty, '.', or '..' components or a trailing slash"
        ));
    }
    Ok(PathBuf::from(raw))
}

/// Why a host path would expose a container daemon socket, if it would
///
/// The path is exposed when it is a known socket, a directory above one, or a
/// file with a known socket name
fn daemon_socket_exposure(path: &Path) -> Option<String> {
    let exposes_known_socket = DAEMON_SOCKETS.iter().any(|socket| {
        let socket = Path::new(socket);
        socket == path || socket.starts_with(path)
    });
    let named_socket = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| DAEMON_SOCKET_NAMES.contains(&name));
    let rootless_runtime = rootless_runtime_directory(path);
    (exposes_known_socket || named_socket || rootless_runtime).then(|| {
        format!(
            "mount source {} is or contains a Docker or containerd socket",
            path.display()
        )
    })
}

/// `/run/user` and `/run/user/<uid>` hold rootless daemon sockets
fn rootless_runtime_directory(path: &Path) -> bool {
    let components: Vec<Component<'_>> = path.components().collect();
    let is_run_user = |parts: &[Component<'_>]| {
        matches!(
            parts,
            [Component::RootDir, Component::Normal(run), Component::Normal(user), ..]
                if *run == "run" && *user == "user"
        ) || matches!(
            parts,
            [Component::RootDir, Component::Normal(var), Component::Normal(run), Component::Normal(user), ..]
                if *var == "var" && *run == "run" && *user == "user"
        )
    };
    if !is_run_user(&components) {
        return false;
    }
    let depth = if components.get(1) == Some(&Component::Normal("var".as_ref())) {
        4
    } else {
        3
    };
    // the directory itself or one user's runtime directory, not a path below it
    components.len() <= depth + 1
        || components
            .get(depth + 1)
            .is_some_and(|part| matches!(part, Component::Normal(name) if name.to_str().is_some_and(|name| name.starts_with("docker") || name.starts_with("containerd"))))
}

/// Known daemon sockets with their symbolic links resolved on this machine
fn resolved_daemon_sockets() -> Vec<PathBuf> {
    DAEMON_SOCKETS
        .iter()
        .filter_map(|socket| {
            let socket = Path::new(socket);
            std::fs::canonicalize(socket).ok().or_else(|| {
                let parent = std::fs::canonicalize(socket.parent()?).ok()?;
                Some(parent.join(socket.file_name()?))
            })
        })
        .collect()
}

/// Check the container's host inputs on the machine that will run it
///
/// Each mount source must exist. Its resolved target must not be a socket or
/// expose a known container daemon socket, which catches a symbolic link to one
pub fn check_container_host(workload: &ContainerWorkload) -> Result<(), AppError> {
    for (index, mount) in workload.mounts.iter().enumerate() {
        let pointer = format!("/workload/mounts/{index}/source");
        let invalid = |message: String| AppError::InvalidSpec {
            pointer: pointer.clone(),
            value: json!(mount.source),
            message,
        };
        let canonical = std::fs::canonicalize(&mount.source).map_err(|err| {
            invalid(format!(
                "mount source {} is not available on this machine: {err}",
                mount.source.display()
            ))
        })?;
        if let Some(reason) = daemon_socket_exposure(&canonical) {
            return Err(invalid(reason));
        }
        // a known socket path can itself sit under a symbolic link on this machine
        if let Some(socket) = resolved_daemon_sockets()
            .into_iter()
            .find(|socket| socket.starts_with(&canonical))
        {
            return Err(invalid(format!(
                "mount source {} contains the container daemon socket {}",
                mount.source.display(),
                socket.display()
            )));
        }
        let metadata = std::fs::metadata(&canonical).map_err(|err| {
            invalid(format!(
                "mount source {} is not available on this machine: {err}",
                mount.source.display()
            ))
        })?;
        if metadata.file_type().is_socket() {
            return Err(invalid(format!(
                "mount source {} is a socket",
                mount.source.display()
            )));
        }
    }
    Ok(())
}

/// JSON Schema of the container workload body, including its `type` key
///
/// `gpus_required` is set for resource work, which must name its GPUs
#[must_use]
pub fn container_workload_schema(gpus_required: bool) -> Value {
    let hex = "[0-9a-f]{64}";
    let component = "[a-z0-9]+([._-]+[a-z0-9]+)*";
    let mut required = vec!["type", "image", "memory"];
    if gpus_required {
        required.push("gpus");
    }
    json!({
        "title": "container",
        "description": "Docker container that Homebased starts, watches, stops, and removes. No shell and no Docker options beyond these fields.",
        "type": "object",
        "properties": {
            "type": { "const": "container" },
            "image": {
                "type": "string",
                "pattern": format!("^(sha256:{hex}|([a-zA-Z0-9.-]+(:[0-9]+)?/)?{component}(/{component})*@sha256:{hex})$"),
                "description": "Image pinned by digest: sha256:<64 hex> image ID or name@sha256:<64 hex>. A tag is refused. The image must already be present on the machine."
            },
            "entrypoint": {
                "type": "array",
                "minItems": 1,
                "prefixItems": [{ "type": "string", "minLength": 1, "pattern": "^[^\\u0000]*$" }],
                "items": { "type": "string", "pattern": "^[^\\u0000]*$" },
                "description": "Argv head that replaces the image entrypoint."
            },
            "args": {
                "type": "array",
                "items": { "type": "string", "pattern": "^[^\\u0000]*$" },
                "description": "Arguments passed unchanged. Not a shell string."
            },
            "gpus": {
                "oneOf": [
                    { "const": "all" },
                    {
                        "type": "array",
                        "minItems": 1,
                        "uniqueItems": true,
                        "items": { "type": "integer", "minimum": 0, "maximum": u32::MAX }
                    }
                ],
                "description": if gpus_required {
                    "\"all\" or device indices. Required for resource work."
                } else {
                    "\"all\" or device indices."
                }
            },
            "memory": {
                "oneOf": [
                    { "type": "integer", "minimum": MIN_MEMORY_BYTES },
                    { "type": "string", "pattern": "^\\s*[0-9]+\\s*([bB]|[kKmMgGtT]([bB]|i[bB]|I[bB])?)?\\s*$" }
                ],
                "description": "Memory limit in bytes or with a binary unit such as 16g. Also the swap limit. At least 6 MiB."
            },
            "user": {
                "type": "string",
                "pattern": "^[0-9]+:[0-9]+$",
                "description": "Numeric uid:gid. Defaults to the daemon's user."
            },
            "workdir": {
                "type": "string",
                "pattern": "^/",
                "description": "Absolute working directory inside the container."
            },
            "mounts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["source", "target"],
                    "properties": {
                        "source": { "type": "string", "pattern": "^/.", "description": "Absolute host path that exists on the machine. Docker and containerd sockets, and directories that contain them, are refused." },
                        "target": { "type": "string", "pattern": "^/.", "description": "Absolute container path." },
                        "read_only": { "type": "boolean", "default": false }
                    }
                }
            },
            "env": {
                "type": "object",
                "propertyNames": { "pattern": "^[A-Za-z_][A-Za-z0-9_]*$" },
                "additionalProperties": { "type": "string", "pattern": "^[^\\u0000]*$" },
                "description": "Explicit environment values. Nothing passes through from the host."
            }
        },
        "required": required,
        "additionalProperties": false
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        ByteSize, ContainerUser, ContainerWorkload, GpuRequest, ImageReference, MIN_MEMORY_BYTES,
        check_container_host, daemon_socket_exposure,
    };
    use std::path::Path;

    const DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn workload(mut extra: Value) -> Value {
        let mut base = json!({ "image": DIGEST, "memory": "1g" });
        if let (Some(base), Some(extra)) = (base.as_object_mut(), extra.as_object_mut()) {
            base.append(extra);
        }
        base
    }

    fn refused_at(value: Value) -> String {
        ContainerWorkload::from_value(&value).unwrap_err().pointer
    }

    #[test]
    fn pinned_images_are_accepted_and_tags_are_refused() {
        for accepted in [
            DIGEST.to_owned(),
            format!("alpine@{DIGEST}"),
            format!("ghcr.io/org/eval-image@{DIGEST}"),
            format!("localhost:5000/eval__probe/x-y.z@{DIGEST}"),
            format!("misc.local:5000/trainer@{DIGEST}"),
        ] {
            assert!(ImageReference::parse(&accepted).is_ok(), "{accepted}");
        }
        for refused in [
            "alpine".to_owned(),
            "alpine:3.20".to_owned(),
            format!("alpine:3.20@{DIGEST}"),
            format!("Alpine@{DIGEST}"),
            format!("-privileged@{DIGEST}"),
            format!(
                "alpine@{}",
                DIGEST.to_uppercase().replace("SHA256", "sha256")
            ),
            "sha256:abc".to_owned(),
            format!("alpine@sha512:{}", &DIGEST[7..]),
            format!("a..b@{DIGEST}"),
        ] {
            assert!(ImageReference::parse(&refused).is_err(), "{refused}");
        }
    }

    #[test]
    fn full_workload_normalizes_to_a_stable_shape() {
        let parsed = ContainerWorkload::from_value(&workload(json!({
            "image": format!("eval@{DIGEST}"),
            "entrypoint": ["/usr/bin/python3", "-m", "eval"],
            "args": ["--checkpoint", "/data/ckpt", ""],
            "gpus": [0, 1],
            "memory": "24GiB",
            "user": "1000:1000",
            "workdir": "/work",
            "mounts": [
                { "source": "/shared/ckpt", "target": "/data/ckpt", "read_only": true },
                { "source": "/shared/out", "target": "/out" }
            ],
            "env": { "HF_HOME": "/data/hf", "_X1": "" }
        })))
        .unwrap();
        assert_eq!(parsed.memory.bytes(), 24 << 30);
        assert_eq!(parsed.gpus, Some(GpuRequest::Devices(vec![0, 1])));
        assert_eq!(
            parsed.user,
            Some(ContainerUser {
                uid: 1000,
                gid: 1000
            })
        );
        assert_eq!(
            parsed.command_after_image(),
            ["-m", "eval", "--checkpoint", "/data/ckpt", ""]
        );
        let encoded = serde_json::to_value(&parsed).unwrap();
        assert_eq!(encoded["memory"], json!(24_u64 << 30));
        assert_eq!(encoded["gpus"], json!([0, 1]));
        assert_eq!(encoded["user"], json!("1000:1000"));
        assert_eq!(
            encoded["mounts"][1],
            json!({ "source": "/shared/out", "target": "/out" })
        );
        let decoded: ContainerWorkload = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(decoded, parsed);
        assert_eq!(serde_json::to_value(&decoded).unwrap(), encoded);

        let minimal = ContainerWorkload::from_value(&workload(json!({}))).unwrap();
        assert_eq!(
            serde_json::to_value(&minimal).unwrap(),
            json!({ "image": DIGEST, "memory": 1_u64 << 30 })
        );
    }

    #[test]
    fn options_the_type_does_not_model_are_refused() {
        for key in [
            "privileged",
            "pid",
            "ipc",
            "network",
            "cap_add",
            "devices",
            "restart",
            "security_opt",
            "volumes",
            "command",
            "rm",
            "name",
            "unknown_option",
        ] {
            let pointer = refused_at(workload(json!({ key: true })));
            assert_eq!(pointer, format!("/{key}"), "{key}");
        }
    }

    #[test]
    fn field_rules_report_their_pointer() {
        for (extra, pointer) in [
            (json!({ "image": "alpine:latest" }), "/image"),
            (json!({ "entrypoint": [] }), "/entrypoint"),
            (json!({ "entrypoint": [""] }), "/entrypoint"),
            (json!({ "entrypoint": null }), "/entrypoint"),
            (json!({ "entrypoint": ["sh", "a\u{0}b"] }), "/entrypoint/1"),
            (json!({ "args": ["ok", "a\u{0}b"] }), "/args/1"),
            (json!({ "args": "python eval.py" }), "/args"),
            (json!({ "gpus": [] }), "/gpus"),
            (json!({ "gpus": [0, 0] }), "/gpus"),
            (json!({ "gpus": "0" }), "/gpus"),
            (json!({ "gpus": [-1] }), "/gpus"),
            (json!({ "memory": 1024 }), "/memory"),
            (json!({ "memory": "lots" }), "/memory"),
            (json!({ "memory": "16q" }), "/memory"),
            (json!({ "user": "root" }), "/user"),
            (json!({ "user": "1000" }), "/user"),
            (json!({ "workdir": "work" }), "/workdir"),
            (
                json!({ "mounts": [{ "source": "data", "target": "/d" }] }),
                "/mounts/0/source",
            ),
            (
                json!({ "mounts": [{ "source": "/a/../b", "target": "/d" }] }),
                "/mounts/0/source",
            ),
            (
                json!({ "mounts": [{ "source": "/", "target": "/d" }] }),
                "/mounts/0/source",
            ),
            (
                json!({ "mounts": [{ "source": "/a", "target": "/" }] }),
                "/mounts/0/target",
            ),
            (
                json!({ "mounts": [{ "source": "/a", "target": "/d" }, { "source": "/b", "target": "/d" }] }),
                "/mounts/1/target",
            ),
            (
                json!({ "mounts": [{ "source": "/a", "target": "/d", "propagation": "shared" }] }),
                "/mounts/0/propagation",
            ),
            (json!({ "env": { "1BAD": "x" } }), "/env/1BAD"),
            (json!({ "env": { "A=B": "x" } }), "/env/A=B"),
            (json!({ "env": { "GOOD": "a\u{0}" } }), "/env/GOOD"),
        ] {
            assert_eq!(refused_at(workload(extra.clone())), pointer, "{extra}");
        }
        assert_eq!(
            refused_at(json!({ "image": DIGEST })),
            "",
            "a missing memory limit is refused"
        );
    }

    #[test]
    fn memory_accepts_bytes_and_binary_units() {
        assert_eq!(ByteSize::parse("512m").unwrap().bytes(), 512 << 20);
        assert_eq!(ByteSize::parse("16G").unwrap().bytes(), 16 << 30);
        assert_eq!(
            ByteSize::parse("6291456").unwrap().bytes(),
            MIN_MEMORY_BYTES
        );
        assert!(ByteSize::parse("5m").is_err());
        assert!(ByteSize::parse("99999999999999t").is_err());
        assert!(ByteSize::parse("-1g").is_err());
    }

    #[test]
    fn daemon_sockets_and_their_directories_are_refused() {
        for refused in [
            "/var/run/docker.sock",
            "/run/docker.sock",
            "/var/run",
            "/run",
            "/var",
            "/run/containerd",
            "/run/containerd/containerd.sock",
            "/var/run/docker",
            "/home/me/docker.sock",
            "/run/user",
            "/run/user/1000",
            "/run/user/1000/docker.sock",
            "/run/user/1000/docker",
        ] {
            assert!(
                daemon_socket_exposure(Path::new(refused)).is_some(),
                "{refused}"
            );
            let pointer = refused_at(workload(json!({
                "mounts": [{ "source": refused, "target": "/x" }]
            })));
            assert_eq!(pointer, "/mounts/0/source", "{refused}");
        }
        for accepted in [
            "/shared/checkpoints",
            "/var/lib/data",
            "/run/media/disk",
            "/home/me/eval",
            "/run/user/1000/eval-output",
        ] {
            assert!(
                daemon_socket_exposure(Path::new(accepted)).is_none(),
                "{accepted}"
            );
        }
    }

    #[test]
    fn host_check_resolves_links_and_refuses_sockets_and_missing_sources() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("data");
        std::fs::create_dir(&data).unwrap();
        let socket_path = directory.path().join("api.sock");
        let _socket = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let link = directory.path().join("link-to-run");
        std::os::unix::fs::symlink("/var/run", &link).unwrap();

        let with_mount = |source: &Path| {
            ContainerWorkload::from_value(&workload(json!({
                "mounts": [{ "source": source, "target": "/x" }]
            })))
            .unwrap()
        };
        check_container_host(&with_mount(&data)).unwrap();
        for refused in [
            directory.path().join("missing"),
            socket_path.clone(),
            link.clone(),
        ] {
            assert!(
                check_container_host(&with_mount(&refused)).is_err(),
                "{}",
                refused.display()
            );
        }
    }

    #[test]
    fn gpu_values_follow_docker_csv_rules() {
        assert_eq!(GpuRequest::All.docker_value(), "all");
        assert_eq!(
            GpuRequest::Devices(vec![0, 2]).docker_value(),
            "\"device=0,2\""
        );
    }
}
