//! CI job classes: slot capacity per class, placement across Fleet machines,
//! admission by load and free disk, hard kill timeouts, and wait statistics
//!
//! A CI job is a one-step queue job with a [`CiSpec`]. Each machine offers
//! classes as slot lanes, one queue resource per unit of capacity, so the
//! queue's one-run-per-resource rule bounds each class. The origin holds a CI
//! job until [`place`] finds a machine with a free slot of its class, then
//! sends it there. This module holds the types and pure rules; the store
//! keeps slots, drains, timeouts, and timings, and the actors carry them out

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::invocation::CommandLine;
use crate::machine::MachineId;

use super::{JobId, Priority, QueueError, ResourceName};

/// Kind of CI work a job needs, such as `rust`, `android`, `xcode`, or `sim`
///
/// 1 to 31 lowercase letters, digits, and inner hyphens, starting with a
/// letter, so `<class>-<slot>` is always a valid resource name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobClass(String);

impl JobClass {
    /// Longest accepted class name
    pub const MAX_LEN: usize = 31;

    /// Validate a class name exactly as given
    pub fn parse(raw: &str) -> Result<Self, QueueError> {
        let invalid = |reason: &str| QueueError::InvalidCiInput {
            what: "class",
            value: raw.to_owned(),
            reason: reason.to_owned(),
        };
        if raw.is_empty() || raw.len() > Self::MAX_LEN {
            return Err(invalid("must be 1 to 31 characters"));
        }
        if !raw.starts_with(|first: char| first.is_ascii_lowercase()) {
            return Err(invalid("must start with a lowercase letter"));
        }
        if !raw
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(invalid(
                "may use only lowercase letters, digits, and hyphens",
            ));
        }
        if raw.ends_with('-') {
            return Err(invalid("must not end with a hyphen"));
        }
        Ok(Self(raw.to_owned()))
    }

    /// Validated name
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Resource name of this class's slot `slot`, such as `rust-2`
    #[must_use]
    pub fn lane_name(&self, slot: SlotNumber) -> ResourceName {
        // a class is at most 31 characters of the resource name alphabet and
        // starts with a letter, so the name always parses
        ResourceName::parse(&format!("{}-{}", self.0, slot.get()))
            .unwrap_or_else(|_| unreachable!("a class and slot number form a valid name"))
    }
}

impl fmt::Display for JobClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for JobClass {
    type Err = QueueError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

impl Serialize for JobClass {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for JobClass {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for JobClass {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "JobClass".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^[a-z]([a-z0-9-]{0,29}[a-z0-9])?$",
            "description": "CI class such as rust, android, xcode, or sim"
        })
    }
}

/// How many runs of one class a machine admits at once: 0 through 32
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "u32", into = "u32")]
pub struct SlotCapacity(u8);

impl SlotCapacity {
    /// Most slots one class may have on one machine
    pub const MAX: u8 = 32;

    /// Number of slots
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0 as u32
    }

    /// One more slot, saturating at [`Self::MAX`]
    #[must_use]
    pub fn grow(self) -> Self {
        Self(self.0.saturating_add(1).min(Self::MAX))
    }

    /// Every slot number of this capacity, from 1
    pub fn slots(self) -> impl Iterator<Item = SlotNumber> {
        (1..=self.0).map(SlotNumber)
    }
}

impl TryFrom<u32> for SlotCapacity {
    type Error = QueueError;

    fn try_from(count: u32) -> Result<Self, Self::Error> {
        u8::try_from(count)
            .ok()
            .filter(|count| *count <= Self::MAX)
            .map(Self)
            .ok_or_else(|| QueueError::InvalidCiInput {
                what: "capacity",
                value: count.to_string(),
                reason: format!("must be from 0 through {}", Self::MAX),
            })
    }
}

impl From<SlotCapacity> for u32 {
    fn from(capacity: SlotCapacity) -> Self {
        capacity.get()
    }
}

impl FromStr for SlotCapacity {
    type Err = QueueError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let count = raw.parse::<u32>().map_err(|_| QueueError::InvalidCiInput {
            what: "capacity",
            value: raw.to_owned(),
            reason: "must be a whole number".into(),
        })?;
        Self::try_from(count)
    }
}

impl fmt::Display for SlotCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// One slot of a class, numbered from 1 through [`SlotCapacity::MAX`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct SlotNumber(u8);

impl SlotNumber {
    /// Raw number
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0 as u32
    }
}

impl TryFrom<u32> for SlotNumber {
    type Error = QueueError;

    fn try_from(number: u32) -> Result<Self, Self::Error> {
        u8::try_from(number)
            .ok()
            .filter(|number| (1..=SlotCapacity::MAX).contains(number))
            .map(Self)
            .ok_or_else(|| QueueError::Corrupt {
                message: format!("slot number {number} is outside 1 through 32"),
            })
    }
}

impl From<SlotNumber> for u32 {
    fn from(slot: SlotNumber) -> Self {
        slot.get()
    }
}

/// What makes a queue resource a CI slot rather than a GPU lane
///
/// A slot never has a device index or GPU UUID; the store refuses both
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SlotLane {
    /// Class the slot serves
    pub class: JobClass,
    /// Its number within the class
    pub slot: SlotNumber,
    /// Whether a capacity cut retired it; a retired slot finishes its run
    /// and takes no new one until the capacity grows again
    pub retired: bool,
}

/// Hard limit on one run's time, from 1 second through 24 hours
///
/// Past it the queue kills the run's process group and fails the job
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KillAfter(Duration);

impl KillAfter {
    /// Shortest accepted limit
    pub const MIN: Duration = Duration::from_secs(1);
    /// Longest accepted limit
    pub const MAX: Duration = Duration::from_secs(24 * 60 * 60);
    /// Limit of a CI job that names none
    pub const DEFAULT: Self = Self(Duration::from_secs(60 * 60));

    /// The limit
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }

    /// When a run that started at `started_at` is due to be killed
    #[must_use]
    pub fn deadline(self, started_at: DateTime<Utc>) -> DateTime<Utc> {
        // the limit is at most 24h, which always fits the clock
        started_at + chrono::Duration::from_std(self.0).unwrap_or(chrono::Duration::MAX)
    }
}

impl TryFrom<Duration> for KillAfter {
    type Error = QueueError;

    fn try_from(limit: Duration) -> Result<Self, Self::Error> {
        if (Self::MIN..=Self::MAX).contains(&limit) {
            Ok(Self(limit))
        } else {
            Err(QueueError::InvalidCiInput {
                what: "kill_after",
                value: humantime::format_duration(limit).to_string(),
                reason: "must be from 1s through 24h".into(),
            })
        }
    }
}

impl FromStr for KillAfter {
    type Err = QueueError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let limit = humantime::parse_duration(raw).map_err(|error| QueueError::InvalidCiInput {
            what: "kill_after",
            value: raw.to_owned(),
            reason: error.to_string(),
        })?;
        Self::try_from(limit)
    }
}

impl fmt::Display for KillAfter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        humantime::format_duration(self.0).fmt(f)
    }
}

impl Serialize for KillAfter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for KillAfter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// Git URL or path that the machine running a job fetches from
///
/// Non-empty, without whitespace or control characters, and not starting with
/// `-`, so git never reads it as an option
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GitRemote(String);

impl GitRemote {
    /// Validate a remote as given
    pub fn parse(raw: &str) -> Result<Self, QueueError> {
        let invalid = |reason: &str| QueueError::InvalidCiInput {
            what: "git.remote",
            value: raw.to_owned(),
            reason: reason.to_owned(),
        };
        if raw.is_empty() || raw.len() > 1024 {
            return Err(invalid("must be 1 to 1024 bytes"));
        }
        if raw.starts_with('-') {
            return Err(invalid("must not start with -"));
        }
        if raw.chars().any(|ch| ch.is_whitespace() || ch.is_control()) {
            return Err(invalid("must not contain whitespace or control characters"));
        }
        // the remote is stored in the job spec, which any reader of the job
        // can see, so a token in it would leak; keep credentials in git's
        // credential helper or ssh keys instead
        if let Some(redacted) = Self::redact_credentials(raw) {
            return Err(QueueError::InvalidCiInput {
                what: "git.remote",
                value: redacted,
                reason: "must not carry credentials; use a credential helper or ssh key".to_owned(),
            });
        }
        Ok(Self(raw.to_owned()))
    }

    /// `raw` with its user info replaced when it may hold a credential: any
    /// user info in an http(s) URL, where a token is the usual login, or a
    /// `user:password` pair in any other URL. Scp-like `git@host:path` and
    /// `ssh://git@host` name only a user, so they pass
    fn redact_credentials(raw: &str) -> Option<String> {
        let (scheme, rest) = raw.split_once("://")?;
        let authority = rest.split('/').next().unwrap_or_default();
        let (userinfo, host) = authority.rsplit_once('@')?;
        let http = scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https");
        if !http && !userinfo.contains(':') {
            return None;
        }
        let path = &rest[authority.len()..];
        Some(format!("{scheme}://***@{host}{path}"))
    }

    /// Validated remote
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Directory name for this remote's clones: its last path segment, then
    /// 16 hex digits of its digest, so two remotes never share a clone
    #[must_use]
    pub fn clone_key(&self) -> String {
        let segment = self
            .0
            .trim_end_matches('/')
            .rsplit(['/', ':'])
            .next()
            .unwrap_or_default()
            .trim_end_matches(".git");
        let mut name: String = segment
            .chars()
            .map(|ch| ch.to_ascii_lowercase())
            .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
            .take(40)
            .collect();
        if name.is_empty() || name.starts_with('.') {
            name.insert_str(0, "repo");
        }
        let digest = super::sha256_hex(self.0.as_bytes());
        format!("{name}-{}", &digest[..16])
    }
}

/// Full commit id: 40 or 64 lowercase hex digits
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommitId(String);

impl CommitId {
    /// Validate a full commit id
    pub fn parse(raw: &str) -> Result<Self, QueueError> {
        let hex = raw
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if !hex || !matches!(raw.len(), 40 | 64) {
            return Err(QueueError::InvalidCiInput {
                what: "git.commit",
                value: raw.to_owned(),
                reason: "must be a full 40- or 64-digit lowercase hex commit id".into(),
            });
        }
        Ok(Self(raw.to_owned()))
    }

    /// Validated commit id
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A relative path that stays inside the checkout: no root, no `..`, no `.`
/// segments except a lone `.`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InnerPath(PathBuf);

impl InnerPath {
    /// The checkout's top directory
    #[must_use]
    pub fn top() -> Self {
        Self(PathBuf::from("."))
    }

    /// Validate a relative path
    pub fn parse(what: &'static str, raw: &Path) -> Result<Self, QueueError> {
        let invalid = |reason: &str| QueueError::InvalidCiInput {
            what,
            value: raw.display().to_string(),
            reason: reason.to_owned(),
        };
        if raw == Path::new(".") {
            return Ok(Self::top());
        }
        if raw.as_os_str().is_empty() {
            return Err(invalid("must not be empty"));
        }
        if raw
            .to_str()
            .is_none_or(|text| text.chars().any(char::is_control))
        {
            return Err(invalid("must be UTF-8 without control characters"));
        }
        if !raw
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        {
            return Err(invalid("must be relative to the checkout, without . or .."));
        }
        Ok(Self(raw.to_path_buf()))
    }

    /// The path
    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl Serialize for GitRemote {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl Serialize for CommitId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl Serialize for InnerPath {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

/// A commit to check out in the slot's own clone before the command runs
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct GitCheckout {
    /// Where the running machine fetches the commit from
    pub remote: GitRemote,
    /// Commit to check out, detached
    pub commit: CommitId,
    /// Untracked paths `git clean` keeps; ignored files, such as `target/`,
    /// are always kept
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub keep: Vec<InnerPath>,
}

impl GitCheckout {
    /// The slot's clone of this checkout's remote under `root`
    #[must_use]
    pub fn clone_dir(&self, root: &Path, lane: &ResourceName) -> PathBuf {
        root.join(self.remote.clone_key()).join(lane.as_str())
    }

    /// Wrap `command` so it runs in `workdir` inside `clone` once the commit is
    /// checked out there
    ///
    /// The wrapper creates the clone on first use, fetches the commit only when
    /// the clone lacks it, checks it out detached, and removes untracked files
    /// except ignored ones and `keep`. It then `exec`s the command, so the
    /// command keeps the run's process group and the queue's kill reaches it
    #[must_use]
    pub fn wrap(&self, clone: &Path, workdir: &InnerPath, command: &CommandLine) -> Vec<String> {
        let excludes: String = self
            .keep
            .iter()
            .map(|path| format!(" -e {}", shell_quote(&path.as_path().display().to_string())))
            .collect();

        // a slot runs one job at a time, so any git lock in its clone was left
        // by a killed run and would fail every later run on the slot. clean
        // gets -ff so a nested repository a run created goes too, and the
        // workdir gets ./ so a name such as `-` is never read as a cd operand
        let format = match self.commit.as_str().len() {
            64 => " --object-format=sha256",
            _ => "",
        };
        let script = format!(
            "set -eu\n\
             dir={dir}\n\
             commit={commit}\n\
             remote={remote}\n\
             if [ ! -d \"$dir/.git\" ]; then mkdir -p \"$dir\" && git init -q{format} \"$dir\"; fi\n\
             cd \"$dir\"\n\
             find .git -name '*.lock' -type f -delete\n\
             if ! git cat-file -e \"$commit^{{commit}}\" 2>/dev/null; then\n\
             \x20 git fetch -q --no-tags \"$remote\" \"$commit\" \
             || git fetch -q --no-tags \"$remote\" '+refs/heads/*:refs/remotes/homebased-ci/*'\n\
             fi\n\
             git -c advice.detachedHead=false checkout -q --detach -f \"$commit\"\n\
             git clean -ffdq{excludes}\n\
             cd {workdir}\n\
             exec \"$@\"\n",
            dir = shell_quote(&clone.display().to_string()),
            commit = shell_quote(self.commit.as_str()),
            remote = shell_quote(self.remote.as_str()),
            workdir = shell_quote(&format!("./{}", workdir.as_path().display())),
        );
        let mut argv = vec![
            "sh".to_owned(),
            "-c".to_owned(),
            script,
            "homebased-ci".to_owned(),
        ];
        argv.extend(std::iter::once(command.program().to_owned()));
        argv.extend(command.args().iter().cloned());
        argv
    }
}

/// Quote `raw` for a POSIX shell as one word
fn shell_quote(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', r"'\''"))
}

/// Wire shape of [`GitCheckout`]; unknown keys fail here
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "GitCheckout")]
struct GitCheckoutWire {
    /// Git URL or path the running machine fetches from. The commit must be
    /// reachable there
    remote: String,
    /// Full commit id to check out, detached
    commit: String,
    /// Untracked paths to keep when the clone is cleaned. Ignored files are
    /// always kept
    #[serde(default)]
    keep: Vec<PathBuf>,
}

impl<'de> Deserialize<'de> for GitCheckout {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = GitCheckoutWire::deserialize(deserializer)?;
        let keep = wire
            .keep
            .iter()
            .map(|path| InnerPath::parse("git.keep", path))
            .collect::<Result<_, _>>()
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            remote: GitRemote::parse(&wire.remote).map_err(serde::de::Error::custom)?,
            commit: CommitId::parse(&wire.commit).map_err(serde::de::Error::custom)?,
            keep,
        })
    }
}

/// The CI part of a job spec
///
/// A spec with it is a CI job: one task step, `wait` preemption, no pinned
/// resource, and placement by class across the Fleet
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CiSpec {
    /// Class of slot the job needs
    pub class: JobClass,
    /// Hard limit on each run's time; past it the run is killed and the job fails
    #[schemars(with = "String")]
    pub kill_after: KillAfter,
    /// Commit to check out in the slot's clone. Without it the job runs in its
    /// absolute `cwd`, which must exist on the machine that runs it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<GitCheckoutWire>")]
    pub git: Option<GitCheckout>,
}

/// Which machines a CI job held on its origin may be sent to
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "machine", rename_all = "snake_case")]
pub enum MachineChoice {
    /// Any machine that offers the job's class
    Any,
    /// Only this machine
    Pinned(MachineId),
}

impl MachineChoice {
    /// Whether the job may run on `machine`
    #[must_use]
    pub fn allows(self, machine: MachineId) -> bool {
        match self {
            Self::Any => true,
            Self::Pinned(pinned) => pinned == machine,
        }
    }
}

/// Highest 1-minute load per core at which a machine admits CI runs
///
/// Stored in hundredths, so the config stays comparable and exact
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LoadLimit(u32);

impl LoadLimit {
    /// Admit while load per core is at most 3.0
    pub const DEFAULT: Self = Self(300);

    /// Parse a positive decimal such as `2.5`
    pub fn parse(raw: f64) -> Result<Self, String> {
        if !raw.is_finite() || raw <= 0.0 || raw > 1000.0 {
            return Err(format!(
                "max_load_per_core must be above 0 and at most 1000, got {raw}"
            ));
        }
        // in range above, so the rounded hundredths fit u32
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        Ok(Self((raw * 100.0).round() as u32))
    }

    /// The limit as load per core
    #[must_use]
    pub fn per_core(self) -> f64 {
        f64::from(self.0) / 100.0
    }
}

/// A machine's admission limits, from the `[ci]` config table
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CiConfig {
    /// Directory that holds each slot's clones; `<home>/ci/checkouts` when unset
    pub checkout_root: Option<PathBuf>,
    /// Admit CI runs only while load per core is at most this
    #[serde(serialize_with = "serialize_load_limit")]
    pub max_load_per_core: LoadLimit,
    /// Admit CI runs only while the checkout root's file system has this many
    /// bytes free
    pub min_free_disk: u64,
}

fn serialize_load_limit<S: Serializer>(
    limit: &LoadLimit,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_f64(limit.per_core())
}

impl Default for CiConfig {
    fn default() -> Self {
        Self {
            checkout_root: None,
            max_load_per_core: LoadLimit::DEFAULT,
            min_free_disk: 10 * 1024 * 1024 * 1024,
        }
    }
}

/// Load and disk readings of one machine at one moment
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Gauge {
    /// 1-minute load average
    pub load1: f64,
    /// Cores the scheduler may use, at least 1
    pub cores: u32,
    /// Bytes free on the checkout root's file system; `None` when unreadable
    pub free_disk: Option<u64>,
}

impl Gauge {
    /// 1-minute load per core
    #[must_use]
    pub fn load_per_core(&self) -> f64 {
        self.load1 / f64::from(self.cores.max(1))
    }
}

/// An operator's drain of a machine: it takes no new CI runs until undrained
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drain {
    /// Why the machine is drained
    pub reason: String,
    /// When the drain started
    pub since: DateTime<Utc>,
}

/// Whether a machine takes new CI runs now
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Admission {
    /// It takes new runs
    Open,
    /// An operator drained it
    Drained {
        /// The drain
        #[serde(flatten)]
        drain: Drain,
    },
    /// Its load per core is over the limit
    Overloaded {
        /// 1-minute load per core now
        load_per_core: f64,
        /// The configured limit
        limit: f64,
    },
    /// Its checkout root has less free disk than the floor
    LowDisk {
        /// Bytes free now
        free: u64,
        /// The configured floor
        floor: u64,
    },
}

impl Admission {
    /// Decide admission from a drain, the gauge, and the limits
    ///
    /// A drain wins over the readings. An unreadable disk does not close the
    /// machine, since the run's own writes would show the problem anyway
    #[must_use]
    pub fn decide(drain: Option<&Drain>, gauge: &Gauge, config: &CiConfig) -> Self {
        if let Some(drain) = drain {
            return Self::Drained {
                drain: drain.clone(),
            };
        }
        let limit = config.max_load_per_core.per_core();
        if gauge.load_per_core() > limit {
            return Self::Overloaded {
                load_per_core: gauge.load_per_core(),
                limit,
            };
        }
        match gauge.free_disk {
            Some(free) if free < config.min_free_disk => Self::LowDisk {
                free,
                floor: config.min_free_disk,
            },
            _ => Self::Open,
        }
    }

    /// Whether new runs may start
    #[must_use]
    pub const fn is_open(&self) -> bool {
        matches!(self, Self::Open)
    }

    /// One line for people, such as `drained: Xcode update`
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Open => "open".into(),
            Self::Drained { drain } => format!("drained: {}", drain.reason),
            Self::Overloaded {
                load_per_core,
                limit,
            } => format!("load {load_per_core:.2}/core over {limit:.2}"),
            Self::LowDisk { free, floor } => format!(
                "disk {} free under {}",
                format_bytes(*free),
                format_bytes(*floor)
            ),
        }
    }
}

/// Bytes in GiB with one decimal, such as `9.5GiB`
#[must_use]
pub fn format_bytes(bytes: u64) -> String {
    // display only; precision loss above 2^53 bytes does not matter
    #[allow(clippy::cast_precision_loss)]
    let gib = bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    format!("{gib:.1}GiB")
}

/// One class on one machine: its capacity and how much of it is in use
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassOffer {
    /// The class
    pub class: JobClass,
    /// Slots the class has now
    pub capacity: SlotCapacity,
    /// Slots of the class holding a run in any phase, retired ones included
    pub busy: u32,
    /// CI jobs of the class waiting in this machine's queue
    pub queued: u32,
}

impl ClassOffer {
    /// Slots a newly sent job could take now
    #[must_use]
    pub fn free(&self) -> u32 {
        self.capacity
            .get()
            .saturating_sub(self.busy)
            .saturating_sub(self.queued)
    }
}

/// What one machine tells placement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MachineOffer {
    /// The machine
    pub machine: MachineId,
    /// Whether it takes new CI runs now
    pub admission: Admission,
    /// Its load and disk now
    pub gauge: Gauge,
    /// Each class it offers, by name
    pub classes: Vec<ClassOffer>,
}

impl MachineOffer {
    /// Its offer of `class`, if any
    #[must_use]
    pub fn class(&self, class: &JobClass) -> Option<&ClassOffer> {
        self.classes.iter().find(|offer| &offer.class == class)
    }
}

/// A CI job held on its origin until some machine has a free slot
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingJob {
    /// The job
    pub job: JobId,
    /// Class it needs
    pub class: JobClass,
    /// Its level; higher levels are placed first
    pub priority: Priority,
    /// Machines it may use
    pub choice: MachineChoice,
    /// When it was submitted; older jobs of a level are placed first
    pub submitted_at: DateTime<Utc>,
}

/// Send `job` to `machine` now
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Assignment {
    /// Job to send
    pub job: JobId,
    /// Machine whose queue takes it
    pub machine: MachineId,
}

/// Choose a machine for each pending job that can start now
///
/// Jobs go in order of level, then submit time. Each takes a free slot of
/// its class on a machine it may use whose admission is open. Every such slot
/// can start the job at once, so the expected wait is equal and the lowest
/// load per core wins, then the most free slots, then the machine id, so the
/// choice is stable. A job with no free slot stays pending and blocks no job
/// behind it, since that job may need another class or machine
#[must_use]
pub fn place(pending: &[PendingJob], offers: &[MachineOffer]) -> Vec<Assignment> {
    let mut order: Vec<&PendingJob> = pending.iter().collect();
    order.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then(left.submitted_at.cmp(&right.submitted_at))
            .then(left.job.cmp(&right.job))
    });
    let mut taken: BTreeMap<(MachineId, &JobClass), u32> = BTreeMap::new();
    let mut assignments = Vec::new();
    for job in order {
        let best = offers
            .iter()
            .filter(|offer| offer.admission.is_open() && job.choice.allows(offer.machine))
            .filter_map(|offer| {
                let class = offer.class(&job.class)?;
                let used = taken
                    .get(&(offer.machine, &job.class))
                    .copied()
                    .unwrap_or(0);
                let free = class.free().saturating_sub(used);
                (free > 0).then_some((offer, free))
            })
            .min_by(|(left, left_free), (right, right_free)| {
                left.gauge
                    .load_per_core()
                    .total_cmp(&right.gauge.load_per_core())
                    .then(right_free.cmp(left_free))
                    .then(left.machine.cmp(&right.machine))
            });
        let Some((offer, _)) = best else {
            continue;
        };
        *taken.entry((offer.machine, &job.class)).or_default() += 1;
        assignments.push(Assignment {
            job: job.job,
            machine: offer.machine,
        });
    }
    assignments
}

/// Why the queue killed a CI run, carried by its job's `JOB_FAILED` event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunTimeout {
    /// The job's limit
    pub limit: KillAfter,
    /// How long the run had run when the kill was requested, in seconds
    pub ran_secs: u64,
    /// When the kill was requested
    pub at: DateTime<Utc>,
}

/// How a CI job ended, for its timing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimingOutcome {
    /// Its run succeeded
    Succeeded,
    /// Its run failed or was lost
    Failed,
    /// The queue killed its run at `kill_after`
    TimedOut,
    /// Someone cancelled it
    Cancelled,
}

impl TimingOutcome {
    /// Storage tag
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
        }
    }

    /// Parse a storage tag
    pub fn parse(raw: &str) -> Result<Self, QueueError> {
        match raw {
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "timed_out" => Ok(Self::TimedOut),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(QueueError::Corrupt {
                message: format!("unknown CI outcome {other:?}"),
            }),
        }
    }
}

/// When a CI job was submitted, started, and ended on the machine that ran it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobTiming {
    /// The job
    pub job: JobId,
    /// Machine whose queue ran it
    pub machine: MachineId,
    /// Its class
    pub class: JobClass,
    /// Submit time on its origin, where its wait starts
    pub submitted_at: DateTime<Utc>,
    /// Start of its first run's command; `None` while it waits
    pub started_at: Option<DateTime<Utc>>,
    /// When it ended; `None` while it waits or runs
    pub ended_at: Option<DateTime<Utc>>,
    /// How it ended
    pub outcome: Option<TimingOutcome>,
}

impl JobTiming {
    /// Seconds from submit to start, once started
    #[must_use]
    pub fn waited(&self) -> Option<f64> {
        self.started_at
            .map(|start| seconds(start - self.submitted_at))
    }

    /// Seconds from start to end, once ended after starting
    #[must_use]
    pub fn ran(&self) -> Option<f64> {
        Some(seconds(self.ended_at? - self.started_at?))
    }
}

fn seconds(span: chrono::Duration) -> f64 {
    // a clock that went backwards counts as zero
    #[allow(clippy::cast_precision_loss)]
    let secs = span.num_milliseconds().max(0) as f64 / 1000.0;
    secs
}

/// Nearest-rank percentiles of a set of durations, in seconds
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Spread {
    /// Number of values
    pub count: usize,
    /// Median
    pub p50_secs: f64,
    /// 90th percentile
    pub p90_secs: f64,
    /// Largest value
    pub max_secs: f64,
}

impl Spread {
    /// The spread of `values`, or `None` for no values
    #[must_use]
    pub fn of(mut values: Vec<f64>) -> Option<Self> {
        if values.is_empty() {
            return None;
        }
        values.sort_by(f64::total_cmp);
        let rank = |percent: usize| {
            // nearest rank: the smallest value with at least `percent` of the
            // values at or below it
            let index = (percent * values.len()).div_ceil(100).max(1) - 1;
            values[index]
        };
        Some(Self {
            count: values.len(),
            p50_secs: rank(50),
            p90_secs: rank(90),
            max_secs: values[values.len() - 1],
        })
    }
}

/// Waits and run times of one class on one machine
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClassStats {
    /// Machine that ran the jobs
    pub machine: MachineId,
    /// The class
    pub class: JobClass,
    /// Jobs submitted in the window that reached this machine
    pub jobs: usize,
    /// Of those, jobs still waiting in this machine's queue
    pub waiting: usize,
    /// Submit-to-start waits of the jobs that started
    pub waited: Option<Spread>,
    /// Run times of the jobs that ended after starting
    pub ran: Option<Spread>,
    /// Jobs the queue killed at their limit
    pub timed_out: usize,
}

/// Group timings by machine and class into stats, in machine then class order
#[must_use]
pub fn class_stats(timings: &[JobTiming]) -> Vec<ClassStats> {
    let mut groups: BTreeMap<(MachineId, &JobClass), Vec<&JobTiming>> = BTreeMap::new();
    for timing in timings {
        groups
            .entry((timing.machine, &timing.class))
            .or_default()
            .push(timing);
    }
    groups
        .into_iter()
        .map(|((machine, class), timings)| ClassStats {
            machine,
            class: class.clone(),
            jobs: timings.len(),
            waiting: timings
                .iter()
                .filter(|timing| timing.started_at.is_none() && timing.ended_at.is_none())
                .count(),
            waited: Spread::of(
                timings
                    .iter()
                    .filter_map(|timing| timing.waited())
                    .collect(),
            ),
            ran: Spread::of(timings.iter().filter_map(|timing| timing.ran()).collect()),
            timed_out: timings
                .iter()
                .filter(|timing| timing.outcome == Some(TimingOutcome::TimedOut))
                .count(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine(n: u128) -> MachineId {
        MachineId::from_uuid(uuid::Uuid::from_u128(n))
    }

    fn offer(id: MachineId, load1: f64, classes: &[(&str, u32, u32)]) -> MachineOffer {
        MachineOffer {
            machine: id,
            admission: Admission::Open,
            gauge: Gauge {
                load1,
                cores: 10,
                free_disk: None,
            },
            classes: classes
                .iter()
                .map(|(class, capacity, busy)| ClassOffer {
                    class: JobClass::parse(class).unwrap(),
                    capacity: SlotCapacity::try_from(*capacity).unwrap(),
                    busy: *busy,
                    queued: 0,
                })
                .collect(),
        }
    }

    fn pending(n: u128, class: &str, priority: Priority, choice: MachineChoice) -> PendingJob {
        PendingJob {
            job: JobId::from_uuid(uuid::Uuid::from_u128(n)),
            class: JobClass::parse(class).unwrap(),
            priority,
            choice,
            submitted_at: DateTime::from_timestamp(i64::try_from(n).unwrap(), 0).unwrap(),
        }
    }

    #[test]
    fn placement_fills_free_slots_by_level_then_load_and_respects_pins() {
        let mac = machine(1);
        let code = machine(2);
        let offers = [
            offer(mac, 150.0, &[("rust", 1, 0), ("xcode", 1, 0)]),
            offer(code, 4.0, &[("rust", 2, 1)]),
        ];
        let jobs = [
            pending(1, "rust", Priority::Low, MachineChoice::Any),
            pending(2, "rust", Priority::High, MachineChoice::Any),
            pending(3, "rust", Priority::Medium, MachineChoice::Any),
            pending(4, "xcode", Priority::Low, MachineChoice::Pinned(code)),
            pending(5, "xcode", Priority::Low, MachineChoice::Any),
        ];
        let placed = place(&jobs, &offers);
        let on = |job: u128| {
            placed
                .iter()
                .find(|assignment| assignment.job.as_uuid().as_u128() == job)
                .map(|assignment| assignment.machine)
        };
        // the high job takes code's one free rust slot, the medium one the
        // Mac's, and the low one waits; the pinned xcode job has no slot on code
        assert_eq!(on(2), Some(code));
        assert_eq!(on(3), Some(mac));
        assert_eq!(on(1), None);
        assert_eq!(on(4), None);
        assert_eq!(on(5), Some(mac));
    }

    #[test]
    fn placement_skips_closed_machines() {
        let mac = machine(1);
        let mut drained = offer(mac, 1.0, &[("rust", 2, 0)]);
        drained.admission = Admission::Drained {
            drain: Drain {
                reason: "update".into(),
                since: Utc::now(),
            },
        };
        let jobs = [pending(1, "rust", Priority::High, MachineChoice::Any)];
        assert!(place(&jobs, &[drained]).is_empty());
    }

    #[test]
    fn spread_uses_nearest_rank() {
        let spread = Spread::of((1..=10).map(f64::from).collect()).unwrap();
        assert_eq!(spread.count, 10);
        assert!((spread.p50_secs - 5.0).abs() < f64::EPSILON);
        assert!((spread.p90_secs - 9.0).abs() < f64::EPSILON);
        assert!((spread.max_secs - 10.0).abs() < f64::EPSILON);
        assert!(Spread::of(Vec::new()).is_none());
    }

    #[test]
    fn checkout_wrapper_quotes_every_value_and_execs_the_command() {
        let checkout = GitCheckout {
            remote: GitRemote::parse("https://example.com/o'brien/cove.git").unwrap(),
            commit: CommitId::parse(&"a".repeat(40)).unwrap(),
            keep: vec![InnerPath::parse("git.keep", Path::new("rust/target")).unwrap()],
        };
        let lane = JobClass::parse("rust").unwrap().lane_name(SlotNumber(2));
        let clone = checkout.clone_dir(Path::new("/ci"), &lane);
        assert!(clone.ends_with("rust-2"));
        assert!(
            clone
                .parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("cove-")
        );
        let command =
            CommandLine::try_from_argv(vec!["cargo".into(), "test".into(), "a b".into()]).unwrap();
        let argv = checkout.wrap(&clone, &InnerPath::top(), &command);
        assert_eq!(argv[..2], ["sh", "-c"]);
        assert!(argv[2].contains(r"remote='https://example.com/o'\''brien/cove.git'"));
        assert!(argv[2].contains("git clean -ffdq -e 'rust/target'"));
        assert!(argv[2].contains("git init -q \"$dir\""));
        assert!(argv[2].ends_with("exec \"$@\"\n"));
        assert_eq!(argv[3..], ["homebased-ci", "cargo", "test", "a b"]);
    }

    #[test]
    fn remotes_with_credentials_are_refused_redacted() {
        for good in [
            "git@github.com:me/app.git",
            "ssh://git@github.com/me/app.git",
            "https://github.com/me/app.git",
            "/srv/git/app.git",
        ] {
            assert!(GitRemote::parse(good).is_ok(), "{good}");
        }
        for (bad, shown) in [
            (
                "https://ghp_secret@github.com/me/app.git",
                "https://***@github.com/me/app.git",
            ),
            (
                "https://me:ghp_secret@github.com/me/app.git",
                "https://***@github.com/me/app.git",
            ),
            ("ssh://me:hunter2@host/app.git", "ssh://***@host/app.git"),
        ] {
            let Err(QueueError::InvalidCiInput { value, .. }) = GitRemote::parse(bad) else {
                panic!("{bad} was accepted");
            };
            assert_eq!(value, shown);
        }
    }

    #[test]
    fn sha256_commits_get_a_sha256_clone_and_workdirs_are_never_cd_options() {
        let checkout = GitCheckout {
            remote: GitRemote::parse("/srv/git/app.git").unwrap(),
            commit: CommitId::parse(&"b".repeat(64)).unwrap(),
            keep: Vec::new(),
        };
        let command = CommandLine::try_from_argv(vec!["true".into()]).unwrap();
        let workdir = InnerPath::parse("workdir", Path::new("-")).unwrap();
        let argv = checkout.wrap(Path::new("/ci/app/rust-1"), &workdir, &command);
        assert!(argv[2].contains("git init -q --object-format=sha256 \"$dir\""));
        assert!(argv[2].contains("cd './-'\n"));
    }

    #[test]
    fn inner_paths_stay_inside_the_checkout() {
        for bad in ["/abs", "../up", "a/../b", "./a", ""] {
            assert!(InnerPath::parse("cwd", Path::new(bad)).is_err(), "{bad}");
        }
        assert!(InnerPath::parse("cwd", Path::new("rust/crates")).is_ok());
    }
}
