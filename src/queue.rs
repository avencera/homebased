//! GPU priority queue: the domain of jobs, resources, and their runs
//!
//! Each machine has one queue shared by its resources, normally one per GPU.
//! A job is a finite command or container split into 1 to 32 steps. Each
//! attempt of a step is a run, executed by one ordinary task. Jobs serve by
//! `(level desc, position asc)`, and only a strictly higher level preempts a
//! running job, at the job's next checkpoint unless it opted into restarts
//!
//! This module holds the types and pure rules. The store keeps the queue and
//! enforces its invariants in transactions, and the queue actor executes it

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::container::ContainerWorkload;
use crate::domain::{TaskId, TaskWorkload, Workload};

pub mod checkpoint;
pub mod classify;
pub mod delivery;
mod error;
pub mod gpu;
pub mod schedule;
pub mod spec;

pub use error::{MoveRefusal, QueueCode, QueueError};

/// Lowercase hex SHA-256, the stored form of job spec and operation digests
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Declare a UUID identity with string, serde, and schema forms
macro_rules! uuid_id {
    ($(#[$meta:meta])* $name:ident, $what:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Allocate a new v7 id
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wrap an existing UUID
            #[must_use]
            pub const fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            /// Underlying UUID
            #[must_use]
            pub const fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = QueueError;

            fn from_str(raw: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(raw)
                    .map(Self)
                    .map_err(|_| QueueError::InvalidId {
                        what: $what,
                        value: raw.to_owned(),
                    })
            }
        }

        impl schemars::JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                stringify!($name).into()
            }

            fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::json_schema!({ "type": "string", "format": "uuid" })
            }
        }
    };
}

uuid_id!(
    /// A queued job, chosen by the submitter; also its retry identity
    JobId,
    "job id"
);
uuid_id!(
    /// One exclusive lane on a machine, normally one GPU
    ResourceId,
    "resource id"
);
uuid_id!(
    /// A move, cancel, or release, chosen by the caller so a retry replays it
    OperationId,
    "operation id"
);
uuid_id!(
    /// One stay of a resource in `Attention`; a release must name it, so a
    /// stale retry cannot clear a later run's `Attention`
    AttentionId,
    "attention id"
);

/// Serving level; derives `Ord`, so `High > Medium > Low`
///
/// A level states how urgently someone needs the result, not what kind of
/// work the job is. Variants are declared in ascending order so more levels
/// can be added later without changing the comparison
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    /// Nice to have; runs when the GPU would sit idle
    Low,
    /// Needed soon, nothing blocked yet
    Medium,
    /// A decision or person is blocked on the result now
    High,
}

impl Priority {
    /// Stable lowercase name, also the storage tag
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Priority {
    type Err = QueueError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            other => Err(QueueError::InvalidPriority {
                value: other.to_owned(),
            }),
        }
    }
}

/// Span from a run's start during which preemption kills and requeues it
///
/// Constructed only through `TryFrom<Duration>`, which accepts 1m through 24h
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RestartWindow(Duration);

impl RestartWindow {
    /// Shortest accepted window
    pub const MIN: Duration = Duration::from_secs(60);
    /// Longest accepted window
    pub const MAX: Duration = Duration::from_secs(24 * 60 * 60);

    /// Whether a run that started at `started_at` is still inside the window
    ///
    /// A run is inside while it is younger than the window, so at exactly the
    /// bound it is outside. A clock that went backwards counts as age zero
    #[must_use]
    pub fn contains(self, started_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        let age = (now - started_at).to_std().unwrap_or(Duration::ZERO);
        age < self.0
    }
}

impl TryFrom<Duration> for RestartWindow {
    type Error = QueueError;

    fn try_from(window: Duration) -> Result<Self, Self::Error> {
        if (Self::MIN..=Self::MAX).contains(&window) {
            Ok(Self(window))
        } else {
            Err(QueueError::InvalidRestartWindow { window })
        }
    }
}

impl fmt::Display for RestartWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        humantime::format_duration(self.0).fmt(f)
    }
}

impl Serialize for RestartWindow {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for RestartWindow {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        let window = humantime::parse_duration(&raw).map_err(serde::de::Error::custom)?;
        Self::try_from(window).map_err(serde::de::Error::custom)
    }
}

/// How the running job gives up its resource to higher-priority work
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", try_from = "PreemptionWire")]
pub enum Preemption {
    /// Killed and requeued at any age; the step restarts from scratch
    Restart,
    /// Higher-priority work waits for the step to finish, unless the run is
    /// younger than `restart_within`
    Wait {
        /// Age below which a preemption kills and requeues the run
        #[serde(default, skip_serializing_if = "Option::is_none")]
        restart_within: Option<RestartWindow>,
    },
    /// Asked to stop at its next checkpoint through the yield file, unless the
    /// run is younger than `restart_within`
    Yield {
        /// Age below which a preemption kills and requeues the run
        #[serde(default, skip_serializing_if = "Option::is_none")]
        restart_within: Option<RestartWindow>,
    },
}

/// Wire shape of `preempt`; unknown keys and a window on `restart` fail here
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreemptionWire {
    mode: PreemptionMode,
    #[serde(default)]
    restart_within: Option<RestartWindow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PreemptionMode {
    Restart,
    Wait,
    Yield,
}

impl TryFrom<PreemptionWire> for Preemption {
    type Error = String;

    fn try_from(wire: PreemptionWire) -> Result<Self, Self::Error> {
        let restart_within = wire.restart_within;
        match wire.mode {
            PreemptionMode::Restart if restart_within.is_some() => Err(
                "restart_within applies only to wait and yield; restart mode always restarts"
                    .into(),
            ),
            PreemptionMode::Restart => Ok(Self::Restart),
            PreemptionMode::Wait => Ok(Self::Wait { restart_within }),
            PreemptionMode::Yield => Ok(Self::Yield { restart_within }),
        }
    }
}

/// How a preemption may stop a run at this moment
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PreemptStop {
    /// Kill and requeue the step; the cheaper stop, chosen first
    Restart,
    /// Ask the run to stop at its next checkpoint
    Yield,
}

impl Preemption {
    /// How a run in this mode that started at `started_at` may be stopped at
    /// `now`, or `None` when higher-priority work must wait for it
    #[must_use]
    pub fn stop_at(self, started_at: DateTime<Utc>, now: DateTime<Utc>) -> Option<PreemptStop> {
        let inside = |window: Option<RestartWindow>| {
            window.is_some_and(|window| window.contains(started_at, now))
        };
        match self {
            Self::Restart => Some(PreemptStop::Restart),
            Self::Wait { restart_within } | Self::Yield { restart_within }
                if inside(restart_within) =>
            {
                Some(PreemptStop::Restart)
            }
            Self::Yield { .. } => Some(PreemptStop::Yield),
            Self::Wait { .. } => None,
        }
    }

    /// Stable mode name
    #[must_use]
    pub const fn mode(self) -> &'static str {
        match self {
            Self::Restart => "restart",
            Self::Wait { .. } => "wait",
            Self::Yield { .. } => "yield",
        }
    }
}

/// Resource name: lowercase letters, digits, and inner hyphens, unique on its
/// machine. A name that parses as a UUID is refused, so a selector that names
/// a resource is never ambiguous
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ResourceName(String);

impl ResourceName {
    /// Longest accepted name
    pub const MAX_LEN: usize = 63;

    /// Validate a name exactly as given
    pub fn parse(raw: &str) -> Result<Self, QueueError> {
        let invalid = |reason: &'static str| QueueError::InvalidResourceName {
            value: raw.to_owned(),
            reason,
        };
        if raw.is_empty() || raw.len() > Self::MAX_LEN {
            return Err(invalid("must be 1 to 63 characters"));
        }
        if !raw
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(invalid(
                "may use only lowercase letters, digits, and hyphens",
            ));
        }
        if raw.starts_with('-') || raw.ends_with('-') {
            return Err(invalid("must not start or end with a hyphen"));
        }
        if Uuid::parse_str(raw).is_ok() {
            return Err(invalid("must not be a UUID"));
        }
        Ok(Self(raw.to_owned()))
    }

    /// Name of the detected GPU at `index`, such as `gpu0`
    #[must_use]
    pub fn gpu(index: u32) -> Self {
        Self(format!("gpu{index}"))
    }

    /// Validated name
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ResourceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ResourceName {
    type Err = QueueError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

impl<'de> Deserialize<'de> for ResourceName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// One exclusive lane; `device` is the GPU index exported to runs, if any
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resource {
    /// Stable identity, kept across restarts
    pub id: ResourceId,
    /// Name unique on its machine
    pub name: ResourceName,
    /// GPU index exported as `CUDA_VISIBLE_DEVICES`, if any
    pub device: Option<u32>,
}

/// A resource named in a job spec or command, by UUID or by name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceSelector {
    /// Exact resource UUID
    Id(ResourceId),
    /// Name on the authority machine
    Name(ResourceName),
}

impl FromStr for ResourceSelector {
    type Err = QueueError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if let Ok(uuid) = Uuid::parse_str(raw) {
            return Ok(Self::Id(ResourceId(uuid)));
        }
        ResourceName::parse(raw).map(Self::Name)
    }
}

impl fmt::Display for ResourceSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Id(id) => id.fmt(f),
            Self::Name(name) => name.fmt(f),
        }
    }
}

impl Serialize for ResourceSelector {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ResourceSelector {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// Where a job may run on its machine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "resource", rename_all = "snake_case")]
pub enum Target {
    /// Whichever resource is free
    Any,
    /// Only this resource
    Pinned(ResourceId),
}

impl Target {
    /// Whether a run of this job may use `resource`
    #[must_use]
    pub fn allows(self, resource: ResourceId) -> bool {
        match self {
            Self::Any => true,
            Self::Pinned(pinned) => pinned == resource,
        }
    }
}

/// 0-based index into a job's steps
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct StepIndex(u32);

impl StepIndex {
    /// The first step
    pub const FIRST: Self = Self(0);

    /// Wrap a raw index; [`Steps::get`] decides whether it names a step
    #[must_use]
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    /// Raw index
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The step after this one
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for StepIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// What one step runs; there is no agent variant and no nesting
///
/// It has no `Deserialize`: steps come only from [`spec::JobSpec`] parsing,
/// which also refuses what the authority owns in a container step
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StepWorkload {
    /// Arbitrary non-interactive command
    Task(TaskWorkload),
    /// Docker container; the authority sets its GPU from the assigned resource
    Container(Box<ContainerWorkload>),
}

impl StepWorkload {
    /// The task workload a run of this step executes
    #[must_use]
    pub fn to_workload(&self) -> Workload {
        match self {
            Self::Task(task) => Workload::Task(task.clone()),
            Self::Container(container) => Workload::Container(container.clone()),
        }
    }
}

/// 1 through 32 steps, validated at construction
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Steps(Vec<StepWorkload>);

impl Steps {
    /// Most steps one job may have
    pub const MAX: usize = 32;

    /// Every step in order
    #[must_use]
    pub fn as_slice(&self) -> &[StepWorkload] {
        &self.0
    }

    /// Number of steps, at least 1
    #[must_use]
    pub fn count(&self) -> usize {
        self.0.len()
    }

    /// The step at `index`, if the job has it
    #[must_use]
    pub fn get(&self, index: StepIndex) -> Option<&StepWorkload> {
        usize::try_from(index.get())
            .ok()
            .and_then(|index| self.0.get(index))
    }

    /// Whether `index` names the job's last step
    #[must_use]
    pub fn is_last(&self, index: StepIndex) -> bool {
        usize::try_from(index.get()).is_ok_and(|index| index + 1 == self.0.len())
    }
}

impl TryFrom<Vec<StepWorkload>> for Steps {
    type Error = QueueError;

    fn try_from(steps: Vec<StepWorkload>) -> Result<Self, Self::Error> {
        if steps.is_empty() || steps.len() > Self::MAX {
            return Err(QueueError::InvalidStepCount { count: steps.len() });
        }
        Ok(Self(steps))
    }
}

/// Where a job is in its life
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum JobState {
    /// Waiting in the queue; `next_step` is the step the next run executes
    Queued {
        /// Step the next run executes
        next_step: StepIndex,
        /// Whether the previous attempt of `next_step` yielded at a checkpoint
        resume: bool,
    },
    /// One resource's active run belongs to this job
    Active {
        /// Resource that runs it
        resource: ResourceId,
    },
    /// The last step succeeded
    Succeeded,
    /// A run failed or was lost
    Failed {
        /// The run that failed, for `task logs`
        run: TaskId,
    },
    /// A person cancelled the job
    Cancelled,
}

impl JobState {
    /// Whether the job left the queue for good
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed { .. } | Self::Cancelled
        )
    }

    /// Stable name, also the storage tag
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued { .. } => "queued",
            Self::Active { .. } => "active",
            Self::Succeeded => "succeeded",
            Self::Failed { .. } => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Why the authority asked the active run to stop
///
/// Later causes may only upgrade, in the order `Yield` < `Restart` <
/// `UserCancel`, which is the declaration order `Ord` uses
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopCause {
    /// Stop at the next checkpoint for higher-priority work
    Yield,
    /// Killed for higher-priority work; the step starts over
    Restart,
    /// A person cancelled the job
    UserCancel,
}

impl StopCause {
    /// The cause after `later` is requested; a weaker cause never replaces a
    /// stronger one
    #[must_use]
    pub fn upgrade(self, later: Self) -> Self {
        self.max(later)
    }

    /// Stable name, also the storage tag
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Yield => "yield",
            Self::Restart => "restart",
            Self::UserCancel => "user_cancel",
        }
    }

    /// Parse a storage tag
    pub fn parse(raw: &str) -> Result<Self, QueueError> {
        match raw {
            "yield" => Ok(Self::Yield),
            "restart" => Ok(Self::Restart),
            "user_cancel" => Ok(Self::UserCancel),
            other => Err(QueueError::Corrupt {
                message: format!("unknown stop cause {other:?}"),
            }),
        }
    }
}

/// Attempt count across a whole job, starting at 1
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunNumber(u32);

impl RunNumber {
    /// A job's first run
    pub const FIRST: Self = Self(1);

    /// Wrap a stored run number, refusing 0
    pub fn new(number: u32) -> Result<Self, QueueError> {
        if number == 0 {
            return Err(QueueError::Corrupt {
                message: "run numbers start at 1".into(),
            });
        }
        Ok(Self(number))
    }

    /// Raw number
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The run after this one
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for RunNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// One resource's single active run and where it is in its lifecycle
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ActiveRun {
    /// Resource the run holds
    pub resource: ResourceId,
    /// Job the run belongs to
    pub job: JobId,
    /// Task that executes the run
    pub task: TaskId,
    /// Attempt count across the job
    pub run_number: RunNumber,
    /// Step the run executes
    pub step: StepIndex,
    /// Where the run is in its lifecycle
    pub phase: RunPhase,
}

/// Lifecycle of a resource's active run
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum RunPhase {
    /// Task row inserted and reserved; worker not yet confirmed running
    Launching {
        /// When the run was reserved
        reserved_at: DateTime<Utc>,
    },
    /// Worker confirmed the child started at `started_at`
    Executing {
        /// When the child started
        started_at: DateTime<Utc>,
    },
    /// A stop was committed; for `Yield` the file is published after the commit
    Stopping {
        /// When the child started; `None` when a person cancelled the run
        /// before its worker confirmed the start
        started_at: Option<DateTime<Utc>>,
        /// Strongest cause requested so far
        cause: StopCause,
        /// When the first stop was requested
        requested_at: DateTime<Utc>,
    },
    /// The task is terminal; cleanup must finish before anything else launches
    Cleaning {
        /// Cleanup attempt the next result must name, starting at 1
        attempt: u32,
    },
    /// Cleanup could not finish; only a person's release leaves this phase
    Attention {
        /// Identity a release must name
        id: AttentionId,
        /// Why cleanup could not finish
        failure: CleanupFailure,
    },
}

impl RunPhase {
    /// Whether the run still executes its job, so the job is `Active`
    #[must_use]
    pub const fn holds_job(&self) -> bool {
        matches!(
            self,
            Self::Launching { .. } | Self::Executing { .. } | Self::Stopping { .. }
        )
    }

    /// Stable name, also the storage tag
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Launching { .. } => "launching",
            Self::Executing { .. } => "executing",
            Self::Stopping { .. } => "stopping",
            Self::Cleaning { .. } => "cleaning",
            Self::Attention { .. } => "attention",
        }
    }
}

/// Why cleanup after a run could not finish, so its resource needs a person
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CleanupFailure {
    /// The worker could not confirm its child's process group exited
    ProcessGroupUnconfirmed,
    /// The container's exit and removal were not confirmed
    ContainerUnconfirmed,
    /// The marker sweep or lost-worker group cleanup could not finish
    Processes {
        /// What the cleanup module found
        failure: crate::cleanup::CleanupFailure,
    },
}

impl fmt::Display for CleanupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProcessGroupUnconfirmed => {
                f.write_str("the worker could not confirm its process group exited")
            }
            Self::ContainerUnconfirmed => {
                f.write_str("the container's exit and removal were not confirmed")
            }
            Self::Processes { failure } => failure.fmt(f),
        }
    }
}

impl From<crate::cleanup::CleanupFailure> for CleanupFailure {
    fn from(failure: crate::cleanup::CleanupFailure) -> Self {
        Self::Processes { failure }
    }
}

/// Which end of a level a move places the job at
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LevelEnd {
    /// Ahead of every job at the level
    Front,
    /// Behind every job at the level
    Back,
}

/// Which side of the target job a relative move places the job on
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    /// Immediately ahead of the target
    Before,
    /// Immediately behind the target
    After,
}

/// Where `resource job move` puts a job
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Placement {
    /// An end of a level; `priority` is the job's current level when absent
    Edge {
        /// Level to place the job in
        priority: Option<Priority>,
        /// Which end
        end: LevelEnd,
    },
    /// Next to another job, taking its level
    Relative {
        /// The job to move next to
        target: JobId,
        /// Which side of it
        side: Side,
        /// Level the caller expects the target to have; refused when it differs
        expect: Option<Priority>,
    },
}

/// The placement flags of `resource job move`, before they are checked
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MoveFlags {
    /// `--front`
    pub front: bool,
    /// `--back`
    pub back: bool,
    /// `--priority <level>`
    pub priority: Option<Priority>,
    /// `--before <job>`
    pub before: Option<JobId>,
    /// `--after <job>`
    pub after: Option<JobId>,
}

impl Placement {
    /// The one placement the flags name
    ///
    /// | Flags | Placement |
    /// | --- | --- |
    /// | `--front` / `--back` | That end of the job's current level |
    /// | `--priority <level>` | Back of that level |
    /// | `--priority <level> --front` | Front of that level |
    /// | `--before <job>` / `--after <job>` | Next to that job, taking its level |
    /// | `--before <job> --priority <level>` | The same, refused later unless the levels match |
    pub fn from_flags(flags: MoveFlags) -> Result<Self, MoveRefusal> {
        let relative = match (flags.before, flags.after) {
            (Some(_), Some(_)) => return Err(MoveRefusal::ConflictingFlags),
            (Some(target), None) => Some((target, Side::Before)),
            (None, Some(target)) => Some((target, Side::After)),
            (None, None) => None,
        };
        if flags.front && flags.back {
            return Err(MoveRefusal::ConflictingFlags);
        }
        match relative {
            Some(_) if flags.front || flags.back => Err(MoveRefusal::ConflictingFlags),
            Some((target, side)) => Ok(Self::Relative {
                target,
                side,
                expect: flags.priority,
            }),
            None if flags.front => Ok(Self::Edge {
                priority: flags.priority,
                end: LevelEnd::Front,
            }),
            None if flags.back || flags.priority.is_some() => Ok(Self::Edge {
                priority: flags.priority,
                end: LevelEnd::Back,
            }),
            None => Err(MoveRefusal::NoPlacement),
        }
    }
}

/// Kinds of job event; each names the job, its run when there is one, and a
/// per-job sequence number
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobEventKind {
    /// The last step succeeded
    JobSucceeded,
    /// A run failed or was lost
    JobFailed,
    /// A cancel took effect
    JobCancelled,
    /// A run stopped for higher-priority work and the job is queued again
    JobPreempted,
    /// Cleanup after this job's run entered `Attention`
    JobAttention,
    /// The open blocking episode reached its notice threshold
    JobBlocked,
    /// A running job's output reached its inactivity threshold
    JobCheckDue,
}

/// One blocked notice, tied to the exact stored episode
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedNotice {
    /// Durable identity of this blocking episode
    pub episode: i64,
    /// Start of this job's blocking episode
    pub blocked_since: DateTime<Utc>,
    /// Runs or attentions that prevent the job from starting
    pub blockers: Vec<schedule::Blocker>,
}

/// The run an event is about
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRun {
    /// Resource the run held
    pub resource: ResourceId,
    /// Run task, for `task logs`
    pub task: TaskId,
    /// Attempt count across the job
    pub run_number: RunNumber,
    /// Step the run executed
    pub step: StepIndex,
}

/// One stored job event; delivery to the job's thread is at least once, and
/// consumers deduplicate by `(job, seq)`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobEvent {
    /// Job the event is about
    pub job: JobId,
    /// Per-job sequence number, starting at 1
    pub seq: u64,
    /// What happened
    pub event: JobEventKind,
    /// The run it happened to, if any
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<EventRun>,
    /// How the run's process ended, if it ended
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<crate::domain::ExitReason>,
    /// The `Attention` a release must name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention: Option<AttentionId>,
    /// When the event was recorded
    pub at: DateTime<Utc>,
    /// Present only on a blocked notice
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<BlockedNotice>,
}

#[cfg(test)]
mod tests;
