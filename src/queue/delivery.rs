//! Origin-owned job routes and ordered authority event envelopes

use serde::{Deserialize, Serialize};

use crate::domain::ThreadId;
use crate::machine::MachineId;
use crate::submission::CallbackContext;

use super::spec::JobSpec;
use super::{JobEvent, JobId, Target};

/// Durable submission result; an unknown result can be retried with the saved spec
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobSubmission {
    /// The submit may have reached the authority
    Unknown,
    /// The authority stored the job
    Accepted,
    /// The authority refused the submit before accepting it
    Rejected {
        /// Structured error response kept for identical retries
        error: serde_json::Value,
        /// HTTP status of the refusal
        status: u16,
    },
}

/// One job's callback owner, saved before submitting locally or over Fleet
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRoute {
    /// Submitter-chosen retry identity
    pub job: JobId,
    /// Machine that owns callback delivery
    pub origin: MachineId,
    /// Fixed queue owner, never resolved again on retry
    pub authority: MachineId,
    /// Thread that receives every job event
    pub thread: ThreadId,
    /// Origin-only environment and resolved callback executable
    pub callback: CallbackContext,
    /// Canonical spec saved for retries after a lost response
    pub spec: JobSpec,
    /// Digest of the canonical spec
    pub digest: String,
    /// Resolved target, present after acceptance
    pub target: Option<Target>,
    /// Submission result separate from job state
    pub submission: JobSubmission,
    /// Last event stored in the origin inbox
    pub last_accepted_seq: u64,
    /// Last event delivered to the thread
    pub last_settled_seq: u64,
}

/// Identity-bound event sent by the authority, with origin-side deduplication
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutedJobEvent {
    /// Machine that owns the origin route
    pub origin: MachineId,
    /// Machine that stored the job and its event
    pub authority: MachineId,
    /// Accepted job spec digest, checked against the route
    pub digest: String,
    /// Stored event, unchanged on retries
    pub event: JobEvent,
}
