//! Durable fleet submission identities, separate from process and callback delivery.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::domain::{ProcessStatus, TaskEnv, TaskId, ThreadId};
use crate::machine::MachineId;
use crate::resource::ResourceId;
use crate::spec::NormalizedSpec;

/// The executable saved for callbacks owned by the origin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CallbackExecutable {
    /// Resolved executable path captured when the route was accepted.
    Available {
        /// Absolute path to the Codex executable.
        path: PathBuf,
    },
    /// Resolution failed while migrating a legacy task.
    Unavailable {
        /// Durable reason shown when callback delivery settles as failed.
        reason: String,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum TaggedCallbackExecutable {
    Available { path: PathBuf },
    Unavailable { reason: String },
}

impl<'de> Deserialize<'de> for CallbackExecutable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Compatible {
            Tagged(TaggedCallbackExecutable),
            LegacyPath(PathBuf),
        }

        Ok(match Compatible::deserialize(deserializer)? {
            Compatible::Tagged(TaggedCallbackExecutable::Available { path })
            | Compatible::LegacyPath(path) => Self::Available { path },
            Compatible::Tagged(TaggedCallbackExecutable::Unavailable { reason }) => {
                Self::Unavailable { reason }
            }
        })
    }
}

impl CallbackExecutable {
    /// Wrap a resolved executable path.
    #[must_use]
    pub fn available(path: PathBuf) -> Self {
        Self::Available { path }
    }

    /// Return the executable path when it was resolved.
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Available { path } => Some(path),
            Self::Unavailable { .. } => None,
        }
    }

    /// Return the migration failure when no executable could be resolved.
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

/// Durable identity for a submitted or migrated task.
///
/// Current requests keep the direct wire shape; migrated rows use an explicit
/// discriminant so they cannot enter request retry checks
#[derive(Debug, Clone)]
pub enum PersistedSpec {
    /// Caller-provided normalized request content.
    Current(NormalizedSpec),
    /// Historical local row with no complete normalized request evidence.
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
    Current(NormalizedSpec),
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
            Compatible::Tagged(TaggedPersistedSpec::Current(spec))
            | Compatible::LegacyCurrent(spec) => Self::Current(spec),
            Compatible::Tagged(TaggedPersistedSpec::MigratedLocal) => Self::MigratedLocal,
        })
    }
}

impl From<NormalizedSpec> for PersistedSpec {
    fn from(spec: NormalizedSpec) -> Self {
        Self::Current(spec)
    }
}

impl PersistedSpec {
    /// Borrow normalized retry content when this identity came from a request.
    #[must_use]
    pub fn current(&self) -> Option<&NormalizedSpec> {
        match self {
            Self::Current(spec) => Some(spec),
            Self::MigratedLocal => None,
        }
    }

    /// Mutably borrow normalized request content when it exists.
    #[must_use]
    pub fn current_mut(&mut self) -> Option<&mut NormalizedSpec> {
        match self {
            Self::Current(spec) => Some(spec),
            Self::MigratedLocal => None,
        }
    }

    /// Consume normalized retry content when this identity came from a request.
    #[must_use]
    pub fn into_current(self) -> Option<NormalizedSpec> {
        match self {
            Self::Current(spec) => Some(spec),
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

/// Stable caller retry identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(pub Uuid);

impl RequestId {
    /// Allocate a new request identity.
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

/// Local context owned by the origin, never sent to an executor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallbackContext {
    /// The submitter's environment.
    pub env: TaskEnv,
    /// Absolute local callback directory.
    pub cwd: PathBuf,
    /// Resolved local Codex executable.
    pub codex: CallbackExecutable,
}

/// Durable phase of one resource-routed task request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceRoutePhase {
    /// The queue request may have reached the resource authority.
    AcceptanceUnknown,
    /// The authority accepted the request, but it has not started a task.
    Waiting,
    /// The first queued executor event established task activation.
    Activated,
    /// The authority confirmed cancellation before task activation.
    CancelledBeforeLaunch,
    /// The resource authority rejected the queue request.
    Rejected {
        /// Durable authority rejection reason.
        reason: String,
    },
}

/// Outcome in a definitive response to a resource queue request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceQueueOutcome {
    /// The resource request is durably waiting in the authority queue.
    Waiting,
    /// The resource authority rejected the request before queue acceptance.
    Rejected {
        /// Durable authority rejection reason.
        reason: String,
    },
}

/// Definitive response bound to the exact resource route identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceQueueReceipt {
    /// Caller retry UUID.
    pub request: RequestId,
    /// Preallocated global task UUID.
    pub task: TaskId,
    /// Machine that owns the callback route.
    pub origin_machine: MachineId,
    /// Fixed resource authority and execution owner.
    pub authority_machine: MachineId,
    /// Resource that owns the waiting request.
    pub resource: ResourceId,
    /// Definitive queue outcome.
    pub outcome: ResourceQueueOutcome,
}

/// SHA-256 digest of the compact JSON encoding of a normalized spec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NormalizedSpecSha256([u8; 32]);

impl NormalizedSpecSha256 {
    /// Return the digest as 64 lowercase hexadecimal characters
    #[must_use]
    pub fn as_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let mut encoded = String::with_capacity(self.0.len() * 2);
        for byte in self.0 {
            encoded.push(HEX[usize::from(byte >> 4)] as char);
            encoded.push(HEX[usize::from(byte & 0x0f)] as char);
        }
        encoded
    }
}

impl Serialize for NormalizedSpecSha256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&(*self).as_hex())
    }
}

impl<'de> Deserialize<'de> for NormalizedSpecSha256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let encoded = String::deserialize(deserializer)?;
        let bytes = encoded.as_bytes();
        if bytes.len() != 64 {
            return Err(D::Error::custom(
                "normalized spec SHA-256 must be 64 lowercase hexadecimal characters",
            ));
        }

        let mut digest = [0_u8; 32];
        let (pairs, remainder) = bytes.as_chunks::<2>();
        if !remainder.is_empty() {
            return Err(D::Error::custom(
                "normalized spec SHA-256 must be 64 lowercase hexadecimal characters",
            ));
        }
        for (index, [high, low]) in pairs.iter().enumerate() {
            let (Some(high), Some(low)) = (hex_digit(*high), hex_digit(*low)) else {
                return Err(D::Error::custom(
                    "normalized spec SHA-256 must be 64 lowercase hexadecimal characters",
                ));
            };
            digest[index] = (high << 4) | low;
        }
        Ok(Self(digest))
    }
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// Compute the SHA-256 digest used to bind resource-route proofs to normalized content
///
/// The digest covers `serde_json::to_vec(spec)` exactly. Keep this helper for both
/// origin proof generation and authority-side queue request verification.
pub fn normalized_spec_sha256(
    spec: &NormalizedSpec,
) -> Result<NormalizedSpecSha256, serde_json::Error> {
    let encoded = serde_json::to_vec(spec)?;
    Ok(NormalizedSpecSha256(Sha256::digest(encoded).into()))
}

/// A definitive authority result that prevents activation of a resource task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceCancellationOutcome {
    /// The authority retained a prevention record before queue acceptance.
    PreventedBeforeAcceptance,
    /// The authority cancelled the queued request before task activation.
    CancelledBeforeLaunch,
}

/// Definitive cancellation response bound to the exact resource route identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCancellationReceipt {
    /// Caller retry UUID.
    pub request: RequestId,
    /// Preallocated global task UUID.
    pub task: TaskId,
    /// Machine that owns the callback route.
    pub origin_machine: MachineId,
    /// Fixed resource authority and execution owner.
    pub authority_machine: MachineId,
    /// Resource that owns the waiting request.
    pub resource: ResourceId,
    /// Definitive pre-activation result.
    pub outcome: ResourceCancellationOutcome,
}

/// Why an origin route cannot represent a resource-waiting request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResourceRouteError {
    /// Resource work must use the resource authority, not an explicit spec machine.
    #[error("resource route spec must not name an execution machine")]
    ExplicitMachine,
    /// Resource routes accept only bounded command workloads.
    #[error("resource route requires a command workload")]
    NonCommandWorkload,
    /// The route and normalized spec must retain one exact thread identity.
    #[error("resource route thread does not match its normalized spec")]
    ThreadMismatch,
    /// Resource callbacks need absolute origin-local callback paths.
    #[error("resource callback paths must be absolute")]
    InvalidCallbackContext,
    /// Only an executor event can establish resource task activation.
    #[error("resource route phase does not agree with its event cursor")]
    InvalidEventCursor,
    /// A migrated local identity names different origin and execution machines.
    #[error("migrated local identity must have the same origin and execution machine")]
    InvalidMigratedIdentity,
    /// A migrated local route is always an accepted execution.
    #[error("migrated local origin route must have accepted submission state")]
    InvalidMigratedRouteState,
}

/// Definitive result or unresolved sent request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubmissionState {
    /// The send may have reached the executor.
    AcceptanceUnknown,
    /// The executor durably accepted this identity.
    Accepted,
    /// The executor durably rejected this identity.
    Rejected { reason: String },
    /// This task identity waits for one resource authority to activate it.
    Resource {
        /// Resource identity, retained after activation.
        resource: ResourceId,
        /// Resource-specific acceptance and activation phase.
        phase: ResourceRoutePhase,
    },
}

/// Origin-owned request mapping and callback route.
#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OriginRoute {
    /// Caller retry UUID.
    pub request: RequestId,
    /// Global task UUID.
    pub task: TaskId,
    /// Machine that owns callbacks.
    pub origin_machine: MachineId,
    /// Fixed execution owner.
    pub execution_machine: MachineId,
    /// Original Codex thread.
    pub thread: ThreadId,
    /// Origin-only callback environment and directory.
    pub callback: CallbackContext,
    /// Normalized request used for conflict checks.
    pub spec: PersistedSpec,
    /// Submission result, independent of process state.
    pub submission: SubmissionState,
    /// Last execution status learned from events.
    pub last_execution_state: Option<ProcessStatus>,
    /// Time of the last route state update, when retained by this version
    #[serde(default)]
    pub last_updated_at: Option<DateTime<Utc>>,
    /// Last accepted event sequence.
    pub last_accepted_seq: u64,
    /// Last settled callback sequence.
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

/// Exact identity and origin-owned context used to create a resource route.
#[derive(Debug, Clone)]
pub struct NewResourceRoute {
    /// Caller retry UUID allocated before the first authority request.
    pub request: RequestId,
    /// Global task UUID allocated before the first authority request.
    pub task: TaskId,
    /// Machine that owns the requesting thread and callback context.
    pub origin_machine: MachineId,
    /// Fixed resource authority and execution owner.
    pub authority_machine: MachineId,
    /// Exact original Codex thread.
    pub thread: ThreadId,
    /// Exact origin-only callback context.
    pub callback: CallbackContext,
    /// Normalized command spec with no explicit execution machine.
    pub spec: NormalizedSpec,
    /// Resource identity that remains on the route after activation.
    pub resource: ResourceId,
}

impl OriginRoute {
    /// Borrow normalized request content when this route came from a submit request.
    #[must_use]
    pub fn current_spec(&self) -> Option<&NormalizedSpec> {
        self.spec.current()
    }

    /// Create an initial origin route for one resource-waiting command.
    ///
    /// The request and task IDs are supplied by the caller so this exact route
    /// can be stored before the first authority request is sent.
    pub fn new_resource_waiting(input: NewResourceRoute) -> Result<Self, ResourceRouteError> {
        let NewResourceRoute {
            request,
            task,
            origin_machine,
            authority_machine,
            thread,
            callback,
            spec,
            resource,
        } = input;
        let route = Self {
            request,
            task,
            origin_machine,
            execution_machine: authority_machine,
            thread,
            callback,
            spec: spec.into(),
            submission: SubmissionState::Resource {
                resource,
                phase: ResourceRoutePhase::AcceptanceUnknown,
            },
            last_execution_state: None,
            last_updated_at: Some(Utc::now()),
            last_accepted_seq: 0,
            last_settled_seq: 0,
        };
        route.validate()?;
        Ok(route)
    }

    /// Validate resource-route invariants while leaving direct routes unchanged.
    pub fn validate(&self) -> Result<(), ResourceRouteError> {
        if !self
            .spec
            .valid_for_owners(self.origin_machine, self.execution_machine)
        {
            return Err(ResourceRouteError::InvalidMigratedIdentity);
        }
        if matches!(&self.spec, PersistedSpec::MigratedLocal)
            && !matches!(&self.submission, SubmissionState::Accepted)
        {
            return Err(ResourceRouteError::InvalidMigratedRouteState);
        }
        let SubmissionState::Resource { phase, .. } = &self.submission else {
            return Ok(());
        };
        let Some(spec) = self.spec.current() else {
            return Err(ResourceRouteError::NonCommandWorkload);
        };
        if spec.machine.is_some() {
            return Err(ResourceRouteError::ExplicitMachine);
        }
        if !matches!(&spec.workload, crate::spec::NormalizedWorkload::Task(_)) {
            return Err(ResourceRouteError::NonCommandWorkload);
        }
        if self.thread != spec.thread {
            return Err(ResourceRouteError::ThreadMismatch);
        }
        if !self.callback.cwd.is_absolute()
            || self
                .callback
                .codex
                .path()
                .is_some_and(|path| !path.is_absolute())
        {
            return Err(ResourceRouteError::InvalidCallbackContext);
        }
        if self.last_settled_seq > self.last_accepted_seq {
            return Err(ResourceRouteError::InvalidEventCursor);
        }
        match phase {
            ResourceRoutePhase::Activated
                if self.last_accepted_seq == 0 || self.last_execution_state.is_none() =>
            {
                Err(ResourceRouteError::InvalidEventCursor)
            }
            ResourceRoutePhase::AcceptanceUnknown
            | ResourceRoutePhase::Waiting
            | ResourceRoutePhase::CancelledBeforeLaunch
            | ResourceRoutePhase::Rejected { .. }
                if self.last_accepted_seq != 0 || self.last_execution_state.is_some() =>
            {
                Err(ResourceRouteError::InvalidEventCursor)
            }
            _ => Ok(()),
        }
    }
}

/// Safe proof of the resource route saved by its origin
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRouteProof {
    /// Caller retry UUID
    pub request: RequestId,
    /// Preallocated global task UUID
    pub task: TaskId,
    /// Machine that owns the callback route
    pub origin_machine: MachineId,
    /// Fixed resource authority and execution owner
    pub authority_machine: MachineId,
    /// Resource that owns the waiting request
    pub resource: ResourceId,
    /// Original Codex thread
    pub thread: ThreadId,
    /// SHA-256 digest of the normalized spec JSON encoding
    pub normalized_spec_sha256: NormalizedSpecSha256,
    /// Durable resource route phase
    pub phase: ResourceRoutePhase,
}

impl ResourceRouteProof {
    /// Derive a safe proof from a valid saved resource route
    #[must_use]
    pub fn from_route(route: &OriginRoute) -> Option<Self> {
        let SubmissionState::Resource { resource, phase } = &route.submission else {
            return None;
        };
        route.validate().ok()?;
        let spec = route.current_spec()?;

        Some(Self {
            request: route.request,
            task: route.task,
            origin_machine: route.origin_machine,
            authority_machine: route.execution_machine,
            resource: *resource,
            thread: route.thread,
            normalized_spec_sha256: normalized_spec_sha256(spec).ok()?,
            phase: phase.clone(),
        })
    }
}

/// Executor-owned accepted task identity, retained after detail cleanup.
#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRecord {
    /// Global task UUID.
    pub task: TaskId,
    /// Machine that owns callbacks.
    pub origin_machine: MachineId,
    /// Execution owner.
    pub execution_machine: MachineId,
    /// Normalized request used for conflict checks.
    pub spec: PersistedSpec,
    /// Retained process state.
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
    /// Borrow normalized retry content when this identity came from a request.
    #[must_use]
    pub fn current_spec(&self) -> Option<&NormalizedSpec> {
        self.spec.current()
    }

    /// Whether this identity's persisted spec is valid for the fixed owners.
    #[must_use]
    pub fn has_valid_spec_owners(&self) -> bool {
        self.spec
            .valid_for_owners(self.origin_machine, self.execution_machine)
    }
}

/// A definitive rejection that prevents delayed submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectionTombstone {
    /// Global task UUID.
    pub task: TaskId,
    /// Origin bound to the rejected UUID.
    pub origin_machine: MachineId,
    /// Execution owner bound to the rejected UUID.
    pub execution_machine: MachineId,
    /// Durable rejection reason.
    pub reason: String,
}

/// One durable executor identity for a task UUID.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutorIdentity {
    /// The executor accepted the request and retained its process state.
    Accepted(ExecutionRecord),
    /// The UUID can never start a child.
    Rejected(RejectionTombstone),
}

/// Definitive rejection reasons created by pre-acceptance transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreAcceptanceRejection {
    /// Unknown acceptance was resolved by abandonment.
    Abandoned,
    /// Cancellation won the acceptance race.
    Cancelled,
}

impl PreAcceptanceRejection {
    /// Durable reason understood by submission recovery.
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
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::domain::ProcessStatus;

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

    fn resource_route() -> OriginRoute {
        let spec = spec();
        OriginRoute::new_resource_waiting(NewResourceRoute {
            request: RequestId::new(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            authority_machine: MachineId::new(),
            thread: spec.thread,
            callback: CallbackContext {
                env: crate::domain::TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: PathBuf::from("/tmp"),
                codex: CallbackExecutable::available(PathBuf::from("/bin/codex")),
            },
            spec,
            resource: ResourceId::new(),
        })
        .unwrap()
    }

    #[test]
    fn resource_route_proof_binds_identity_phase_and_normalized_spec_digest() {
        let route = resource_route();
        let proof = ResourceRouteProof::from_route(&route).unwrap();
        let expected_digest = NormalizedSpecSha256(
            Sha256::digest(serde_json::to_vec(route.current_spec().unwrap()).unwrap()).into(),
        );

        assert_eq!(proof.request, route.request);
        assert_eq!(proof.task, route.task);
        assert_eq!(proof.origin_machine, route.origin_machine);
        assert_eq!(proof.authority_machine, route.execution_machine);
        let SubmissionState::Resource { resource, .. } = &route.submission else {
            panic!("resource route constructor must create a resource route");
        };
        assert_eq!(proof.resource, *resource);
        assert_eq!(proof.thread, route.thread);
        assert_eq!(proof.phase, ResourceRoutePhase::AcceptanceUnknown);
        assert_eq!(proof.normalized_spec_sha256, expected_digest);
        assert_eq!(proof.normalized_spec_sha256.as_hex().len(), 64);

        let wire = serde_json::to_value(&proof).unwrap();
        assert!(wire.get("callback").is_none());
        assert!(wire.get("spec").is_none());
        assert!(wire.get("cwd").is_none());
        assert!(wire.get("env").is_none());
        assert_eq!(
            serde_json::from_value::<ResourceRouteProof>(wire.clone()).unwrap(),
            proof
        );

        let mut unknown_field = wire;
        unknown_field["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ResourceRouteProof>(unknown_field).is_err());
    }

    #[test]
    fn resource_route_proof_is_absent_for_missing_direct_or_invalid_resource_routes() {
        let absent_route: Option<&OriginRoute> = None;
        assert!(
            absent_route
                .and_then(ResourceRouteProof::from_route)
                .is_none()
        );

        let mut direct_route = resource_route();
        direct_route.submission = SubmissionState::Accepted;
        assert!(ResourceRouteProof::from_route(&direct_route).is_none());

        let mut invalid_resource_route = resource_route();
        invalid_resource_route.submission = SubmissionState::Resource {
            resource: ResourceId::new(),
            phase: ResourceRoutePhase::Activated,
        };
        assert!(ResourceRouteProof::from_route(&invalid_resource_route).is_none());
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
    fn callback_executable_reads_legacy_path_and_round_trips_both_typed_states() {
        let path = PathBuf::from("/bin/codex");
        let legacy: CallbackExecutable =
            serde_json::from_value(serde_json::json!(path.to_string_lossy())).unwrap();
        assert_eq!(legacy.path(), Some(Path::new("/bin/codex")));

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

        let context: CallbackContext = serde_json::from_value(serde_json::json!({
            "env": { "path": "/bin", "home": "/tmp" },
            "cwd": "/tmp",
            "codex": "/bin/codex"
        }))
        .unwrap();
        assert_eq!(context.codex.path(), Some(Path::new("/bin/codex")));
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
                env: crate::domain::TaskEnv {
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
            Err(ResourceRouteError::InvalidMigratedRouteState)
        ));
        route.submission = SubmissionState::Accepted;
        route.execution_machine = MachineId::new();
        assert!(matches!(
            route.validate(),
            Err(ResourceRouteError::InvalidMigratedIdentity)
        ));
    }
}
