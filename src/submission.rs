//! Durable fleet submission identities, separate from process and callback delivery

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::dependency::{HeldCancellation, TaskDependencies};
use crate::domain::{ProcessStatus, TaskEnv, TaskId, TaskStatus, ThreadId};
use crate::machine::MachineId;
use crate::spec::NormalizedSpec;

/// The executable saved for callbacks owned by the origin
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CallbackExecutable {
    /// Resolved executable path captured when the route was accepted
    Available {
        /// Absolute path to the Codex executable
        path: PathBuf,
    },
    /// Resolution failed when the route was accepted or a legacy task was migrated
    Unavailable {
        /// Durable reason shown when callback delivery settles as failed
        reason: String,
    },
}

impl CallbackExecutable {
    /// Wrap a resolved executable path
    #[must_use]
    pub fn available(path: PathBuf) -> Self {
        Self::Available { path }
    }

    /// Return the executable path when it was resolved
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Available { path } => Some(path),
            Self::Unavailable { .. } => None,
        }
    }

    /// Return the resolution failure when no executable could be resolved
    #[must_use]
    pub fn unavailable_reason(&self) -> Option<&str> {
        match self {
            Self::Available { .. } => None,
            Self::Unavailable { reason } => Some(reason),
        }
    }
}

impl From<PathBuf> for CallbackExecutable {
    fn from(path: PathBuf) -> Self {
        Self::available(path)
    }
}

/// Durable identity for a submitted or migrated task
///
/// Current requests keep the direct wire shape; migrated rows use an explicit
/// discriminant so they cannot enter request retry checks
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistedSpec {
    /// Caller-provided normalized request content
    Current(Box<NormalizedSpec>),
    /// Historical local row with no complete normalized request evidence
    MigratedLocal,
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "spec",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum TaggedPersistedSpec {
    Current(Box<NormalizedSpec>),
    MigratedLocal,
}

impl Serialize for PersistedSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Current(spec) => spec.serialize(serializer),
            Self::MigratedLocal => TaggedPersistedSpec::MigratedLocal.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for PersistedSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Compatible {
            Tagged(TaggedPersistedSpec),
            LegacyCurrent(NormalizedSpec),
        }

        Ok(match Compatible::deserialize(deserializer)? {
            Compatible::Tagged(TaggedPersistedSpec::Current(spec)) => Self::Current(spec),
            Compatible::LegacyCurrent(spec) => Self::Current(Box::new(spec)),
            Compatible::Tagged(TaggedPersistedSpec::MigratedLocal) => Self::MigratedLocal,
        })
    }
}

impl From<NormalizedSpec> for PersistedSpec {
    fn from(spec: NormalizedSpec) -> Self {
        Self::Current(Box::new(spec))
    }
}

impl PersistedSpec {
    /// Borrow normalized retry content when this identity came from a request
    #[must_use]
    pub fn current(&self) -> Option<&NormalizedSpec> {
        match self {
            Self::Current(spec) => Some(spec),
            Self::MigratedLocal => None,
        }
    }

    /// Mutably borrow normalized request content when it exists
    #[must_use]
    pub fn current_mut(&mut self) -> Option<&mut NormalizedSpec> {
        match self {
            Self::Current(spec) => Some(spec),
            Self::MigratedLocal => None,
        }
    }

    /// Consume normalized retry content when this identity came from a request
    #[must_use]
    pub fn into_current(self) -> Option<NormalizedSpec> {
        match self {
            Self::Current(spec) => Some(*spec),
            Self::MigratedLocal => None,
        }
    }

    fn valid_for_owners(&self, origin: MachineId, execution: MachineId) -> bool {
        match self {
            Self::Current(_) => true,
            Self::MigratedLocal => origin == execution,
        }
    }
}

/// Stable caller retry identity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(pub Uuid);

impl RequestId {
    /// Allocate a new request identity
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

/// Local context owned by the origin, never sent to an executor
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallbackContext {
    /// The submitter's environment
    pub env: TaskEnv,
    /// Absolute local callback directory
    pub cwd: PathBuf,
    /// Resolved local Codex executable
    pub codex: CallbackExecutable,
}

/// Durable phase of one origin route held until its dependencies succeed
///
/// A held route has no task row and no executor identity. Its dependencies
/// are saved beside it, and only the origin's dependency release moves it on:
/// a local task goes straight to [`SubmissionState::Accepted`] with its row,
/// and a remote one goes through [`Self::Launching`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HeldPhase {
    /// At least one dependency has not finished
    Waiting,
    /// Every dependency succeeded and the launch may have reached the remote executor
    ///
    /// The launch is resent with the same task UUID until the executor answers,
    /// so cancellation from here must reach the executor
    Launching,
    /// The origin cancelled the task before any launch
    Cancelled {
        /// Dependency ending or request that cancelled it
        cause: HeldCancellation,
    },
}

impl HeldPhase {
    /// Public status of a task in this phase
    #[must_use]
    pub fn status(&self) -> TaskStatus {
        match self {
            Self::Waiting | Self::Launching => TaskStatus::Held,
            Self::Cancelled { .. } => TaskStatus::Process(ProcessStatus::Cancelled),
        }
    }
}

/// Why a saved origin route breaks its invariants
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    /// The route and normalized spec must retain one exact thread identity
    #[error("route thread does not match its normalized spec")]
    ThreadMismatch,
    /// A held route cannot have accepted executor events
    #[error("route phase does not agree with its event cursor")]
    InvalidEventCursor,
    /// A migrated local identity names different origin and execution machines
    #[error("migrated local identity must have the same origin and execution machine")]
    InvalidMigratedIdentity,
    /// A migrated local route is always an accepted execution
    #[error("migrated local origin route must have accepted submission state")]
    InvalidMigratedRouteState,
}

/// Definitive result or unresolved sent request
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubmissionState {
    /// The send may have reached the executor
    AcceptanceUnknown,
    /// The executor durably accepted this identity
    Accepted,
    /// The executor durably rejected this identity
    Rejected { reason: String },
    /// This task identity waits on its origin for its dependencies to succeed
    Held {
        /// Hold, launch, or cancellation phase
        phase: HeldPhase,
    },
}

/// Origin-owned request mapping and callback route
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OriginRoute {
    /// Caller retry UUID
    pub request: RequestId,
    /// Global task UUID
    pub task: TaskId,
    /// Machine that owns callbacks
    pub origin_machine: MachineId,
    /// Fixed execution owner
    pub execution_machine: MachineId,
    /// Original Codex thread
    pub thread: ThreadId,
    /// Origin-only callback environment and directory
    pub callback: CallbackContext,
    /// Normalized request used for conflict checks
    pub spec: PersistedSpec,
    /// Submission result, independent of process state
    pub submission: SubmissionState,
    /// Last execution status learned from events
    pub last_execution_state: Option<ProcessStatus>,
    /// Time of the last route state update, when retained by this version
    #[serde(default)]
    pub last_updated_at: Option<DateTime<Utc>>,
    /// Last accepted event sequence
    pub last_accepted_seq: u64,
    /// Last settled callback sequence
    pub last_settled_seq: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginRouteFields {
    request: RequestId,
    task: TaskId,
    origin_machine: MachineId,
    execution_machine: MachineId,
    thread: ThreadId,
    callback: CallbackContext,
    spec: PersistedSpec,
    submission: SubmissionState,
    last_execution_state: Option<ProcessStatus>,
    #[serde(default)]
    last_updated_at: Option<DateTime<Utc>>,
    last_accepted_seq: u64,
    last_settled_seq: u64,
}

impl<'de> Deserialize<'de> for OriginRoute {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let fields = OriginRouteFields::deserialize(deserializer)?;
        let route = Self {
            request: fields.request,
            task: fields.task,
            origin_machine: fields.origin_machine,
            execution_machine: fields.execution_machine,
            thread: fields.thread,
            callback: fields.callback,
            spec: fields.spec,
            submission: fields.submission,
            last_execution_state: fields.last_execution_state,
            last_updated_at: fields.last_updated_at,
            last_accepted_seq: fields.last_accepted_seq,
            last_settled_seq: fields.last_settled_seq,
        };
        route.validate().map_err(D::Error::custom)?;
        Ok(route)
    }
}

/// One origin route with the dependencies it was submitted with
///
/// The store saves the dependencies beside the route, like its spec, so they
/// stay on the origin and never enter the route summary that peers read
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependentRoute {
    /// Saved origin route
    pub route: OriginRoute,
    /// Tasks that must succeed before it launches
    pub after: TaskDependencies,
}

/// Exact identity and origin-owned context used to create a held route
#[derive(Debug, Clone)]
pub struct NewHeldRoute {
    /// Caller retry UUID
    pub request: RequestId,
    /// Global task UUID assigned before any launch
    pub task: TaskId,
    /// Machine that accepted the submit and owns callbacks
    pub origin_machine: MachineId,
    /// Machine that will run the task: the origin, or the spec's Fleet machine
    pub execution_machine: MachineId,
    /// Callback context captured from the submitting shell
    pub callback: CallbackContext,
    /// Normalized request content
    pub spec: NormalizedSpec,
}

impl OriginRoute {
    /// Borrow normalized request content when this route came from a submit request
    #[must_use]
    pub fn current_spec(&self) -> Option<&NormalizedSpec> {
        self.spec.current()
    }

    /// Create a route held on its origin until its dependencies succeed
    ///
    /// The task UUID is assigned now, so the eventual launch, its retries, and
    /// every later event use the identity that the submit returned
    #[must_use]
    pub fn new_held(input: NewHeldRoute) -> Self {
        let NewHeldRoute {
            request,
            task,
            origin_machine,
            execution_machine,
            callback,
            spec,
        } = input;
        Self {
            request,
            task,
            origin_machine,
            execution_machine,
            thread: spec.thread,
            callback,
            spec: spec.into(),
            submission: SubmissionState::Held {
                phase: HeldPhase::Waiting,
            },
            last_execution_state: None,
            last_updated_at: Some(Utc::now()),
            last_accepted_seq: 0,
            last_settled_seq: 0,
        }
    }

    /// Validate migrated and held route invariants while leaving direct routes unchanged
    pub fn validate(&self) -> Result<(), RouteError> {
        if !self
            .spec
            .valid_for_owners(self.origin_machine, self.execution_machine)
        {
            return Err(RouteError::InvalidMigratedIdentity);
        }
        if matches!(&self.spec, PersistedSpec::MigratedLocal)
            && !matches!(&self.submission, SubmissionState::Accepted)
        {
            return Err(RouteError::InvalidMigratedRouteState);
        }
        match &self.submission {
            SubmissionState::Held { phase } => self.validate_held_route(phase),
            SubmissionState::AcceptanceUnknown
            | SubmissionState::Accepted
            | SubmissionState::Rejected { .. } => Ok(()),
        }
    }

    fn validate_held_route(&self, phase: &HeldPhase) -> Result<(), RouteError> {
        let Some(spec) = self.spec.current() else {
            return Err(RouteError::InvalidMigratedRouteState);
        };
        if self.thread != spec.thread {
            return Err(RouteError::ThreadMismatch);
        }
        // no executor event can reach a route that never launched; a
        // cancelled one carries only the origin's own terminal event
        let max_seq = match phase {
            HeldPhase::Waiting | HeldPhase::Launching => 0,
            HeldPhase::Cancelled { .. } => 1,
        };
        if self.last_accepted_seq > max_seq
            || self.last_settled_seq > self.last_accepted_seq
            || self.last_execution_state.is_some()
        {
            return Err(RouteError::InvalidEventCursor);
        }
        Ok(())
    }
}

/// Executor-owned accepted task identity, retained after detail cleanup
#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRecord {
    /// Global task UUID
    pub task: TaskId,
    /// Machine that owns callbacks
    pub origin_machine: MachineId,
    /// Execution owner
    pub execution_machine: MachineId,
    /// Normalized request used for conflict checks
    pub spec: PersistedSpec,
    /// Retained process state
    pub state: ProcessStatus,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionRecordFields {
    task: TaskId,
    origin_machine: MachineId,
    execution_machine: MachineId,
    spec: PersistedSpec,
    state: ProcessStatus,
}

impl<'de> Deserialize<'de> for ExecutionRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let fields = ExecutionRecordFields::deserialize(deserializer)?;
        let record = Self {
            task: fields.task,
            origin_machine: fields.origin_machine,
            execution_machine: fields.execution_machine,
            spec: fields.spec,
            state: fields.state,
        };
        if !record.has_valid_spec_owners() {
            return Err(D::Error::custom(
                "migrated local execution identity has different owners",
            ));
        }
        Ok(record)
    }
}

impl ExecutionRecord {
    /// Borrow normalized retry content when this identity came from a request
    #[must_use]
    pub fn current_spec(&self) -> Option<&NormalizedSpec> {
        self.spec.current()
    }

    /// Whether this identity's persisted spec is valid for the fixed owners
    #[must_use]
    pub fn has_valid_spec_owners(&self) -> bool {
        self.spec
            .valid_for_owners(self.origin_machine, self.execution_machine)
    }
}

/// A definitive rejection that prevents delayed submission
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectionTombstone {
    /// Global task UUID
    pub task: TaskId,
    /// Origin bound to the rejected UUID
    pub origin_machine: MachineId,
    /// Execution owner bound to the rejected UUID
    pub execution_machine: MachineId,
    /// Durable rejection reason
    pub reason: String,
}

/// One durable executor identity for a task UUID
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutorIdentity {
    /// The executor accepted the request and retained its process state
    Accepted(ExecutionRecord),
    /// The UUID can never start a child
    Rejected(RejectionTombstone),
}

/// Definitive rejection reasons created by pre-acceptance transitions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreAcceptanceRejection {
    /// Unknown acceptance was resolved by abandonment
    Abandoned,
    /// Cancellation won the acceptance race
    Cancelled,
}

impl PreAcceptanceRejection {
    /// Durable reason understood by submission recovery
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Abandoned => "abandoned_before_acceptance",
            Self::Cancelled => "cancelled_before_acceptance",
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::TaskEnv;
    use std::path::PathBuf;

    use super::{
        CallbackContext, CallbackExecutable, ExecutionRecord, OriginRoute, PersistedSpec,
        RequestId, RouteError, SubmissionState,
    };
    use crate::domain::{ProcessStatus, TaskId};
    use crate::machine::MachineId;
    use crate::spec::NormalizedSpec;

    fn spec() -> NormalizedSpec {
        serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "legacy compatible",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["echo", "hello"] }
        }))
        .unwrap()
    }

    #[test]
    fn persisted_spec_keeps_current_wire_shape_and_tags_migrated_rows() {
        let normalized = spec();
        let legacy_json = serde_json::to_value(&normalized).unwrap();
        let legacy: PersistedSpec = serde_json::from_value(legacy_json.clone()).unwrap();
        assert!(matches!(legacy, PersistedSpec::Current(_)));
        assert_eq!(serde_json::to_value(&legacy).unwrap(), legacy_json);

        let tagged: PersistedSpec = serde_json::from_value(serde_json::json!({
            "type": "current",
            "spec": legacy_json
        }))
        .unwrap();
        assert!(tagged.current().is_some());

        let migrated = PersistedSpec::MigratedLocal;
        assert_eq!(
            serde_json::to_value(&migrated).unwrap(),
            serde_json::json!({ "type": "migrated_local" })
        );
        assert!(
            serde_json::from_value::<PersistedSpec>(serde_json::json!({
                "type": "migrated_local"
            }))
            .unwrap()
            .current()
            .is_none()
        );
    }

    #[test]
    fn callback_executable_round_trips_both_typed_states() {
        let path = PathBuf::from("/bin/codex");
        let available = CallbackExecutable::available(path.clone());
        assert_eq!(
            serde_json::from_value::<CallbackExecutable>(serde_json::to_value(&available).unwrap())
                .unwrap(),
            available
        );
        let unavailable = CallbackExecutable::Unavailable {
            reason: "codex was not found".into(),
        };
        assert_eq!(
            serde_json::from_value::<CallbackExecutable>(
                serde_json::to_value(&unavailable).unwrap()
            )
            .unwrap(),
            unavailable
        );
    }

    #[test]
    fn old_execution_record_json_reads_normalized_spec() {
        let normalized = spec();
        let identity = ExecutionRecord {
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            execution_machine: MachineId::new(),
            spec: normalized.clone().into(),
            state: ProcessStatus::Queued,
        };
        let mut old_json = serde_json::to_value(identity).unwrap();
        old_json["spec"] = serde_json::to_value(normalized).unwrap();
        let old: ExecutionRecord = serde_json::from_value(old_json).unwrap();
        assert!(matches!(old.spec, PersistedSpec::Current(_)));
        assert!(old.has_valid_spec_owners());
    }

    #[test]
    fn migrated_spec_requires_one_machine_owner() {
        let spec = PersistedSpec::MigratedLocal;
        let machine = MachineId::new();
        assert!(spec.valid_for_owners(machine, machine));
        assert!(!spec.valid_for_owners(machine, MachineId::new()));
    }

    #[test]
    fn migrated_origin_route_requires_local_accepted_ownership() {
        let machine = MachineId::new();
        let mut route = OriginRoute {
            request: RequestId::new(),
            task: TaskId::new(),
            origin_machine: machine,
            execution_machine: machine,
            thread: spec().thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: PathBuf::from("/tmp"),
                codex: CallbackExecutable::available(PathBuf::from("/bin/codex")),
            },
            spec: PersistedSpec::MigratedLocal,
            submission: SubmissionState::Accepted,
            last_execution_state: Some(ProcessStatus::Queued),
            last_updated_at: None,
            last_accepted_seq: 0,
            last_settled_seq: 0,
        };
        assert!(route.validate().is_ok());

        route.submission = SubmissionState::AcceptanceUnknown;
        assert!(matches!(
            route.validate(),
            Err(RouteError::InvalidMigratedRouteState)
        ));
        route.submission = SubmissionState::Accepted;
        route.execution_machine = MachineId::new();
        assert!(matches!(
            route.validate(),
            Err(RouteError::InvalidMigratedIdentity)
        ));
    }
}
