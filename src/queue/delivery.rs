//! Origin-owned job routes and ordered authority event envelopes

use serde::{Deserialize, Serialize};

use crate::dependency::{Dependencies, Dependency, DependencyOutcome};
use crate::domain::{TaskId, ThreadId};
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
    /// Saved on the origin until every task or job in the route's `after` succeeds;
    /// the authority has not seen it
    Held,
    /// Ended on the origin before it joined the queue, with its terminal
    /// event in the origin inbox
    Ended {
        /// Why it never ran
        ending: HeldJobEnding,
    },
}

/// Why a job held for its `after` entries ended before it joined the queue
///
/// A dependency ending names its `task` or its `job`; see [`WireHeldJobEnding`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WireHeldJobEnding", into = "WireHeldJobEnding")]
pub enum HeldJobEnding {
    /// A task or job in `after` ended without succeeding, so the job was cancelled
    Dependency {
        /// The task or job that ended
        dependency: Dependency,
        /// How it ended
        outcome: DependencyOutcome,
    },
    /// Someone cancelled the job while it was held
    Requested,
    /// The queue's machine refused the job once its tasks succeeded
    Refused {
        /// Error code of the refusal
        code: String,
        /// Error message of the refusal
        message: String,
    },
}

/// Saved and sent form of [`HeldJobEnding`]
///
/// A task dependency keeps the `task` key it had before `after` took jobs,
/// and a job dependency puts its UUID under `job` instead, so readers tell
/// them apart by key without a second `reason`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case", deny_unknown_fields)]
enum WireHeldJobEnding {
    Dependency {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task: Option<TaskId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job: Option<JobId>,
        outcome: DependencyOutcome,
    },
    Requested,
    Refused {
        code: String,
        message: String,
    },
}

impl TryFrom<WireHeldJobEnding> for HeldJobEnding {
    type Error = String;

    fn try_from(wire: WireHeldJobEnding) -> Result<Self, Self::Error> {
        Ok(match wire {
            WireHeldJobEnding::Dependency { task, job, outcome } => {
                let dependency = match (task, job) {
                    (Some(task), None) => Dependency::Task(task),
                    (None, Some(job)) => Dependency::Job(job),
                    _ => return Err("a dependency ending names exactly one task or job".into()),
                };
                Self::Dependency {
                    dependency,
                    outcome,
                }
            }
            WireHeldJobEnding::Requested => Self::Requested,
            WireHeldJobEnding::Refused { code, message } => Self::Refused { code, message },
        })
    }
}

impl From<HeldJobEnding> for WireHeldJobEnding {
    fn from(ending: HeldJobEnding) -> Self {
        match ending {
            HeldJobEnding::Dependency {
                dependency,
                outcome,
            } => {
                let (task, job) = match dependency {
                    Dependency::Task(task) => (Some(task), None),
                    Dependency::Job(job) => (None, Some(job)),
                };
                Self::Dependency { task, job, outcome }
            }
            HeldJobEnding::Requested => Self::Requested,
            HeldJobEnding::Refused { code, message } => Self::Refused { code, message },
        }
    }
}

impl HeldJobEnding {
    /// One line for people, such as `task 0199… ended failed`
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Dependency {
                dependency,
                outcome,
            } => {
                format!("after {dependency} ended {outcome}, so the job never queued")
            }
            Self::Requested => "cancelled while it waited for its after entries".to_owned(),
            Self::Refused { code, message } => {
                format!(
                    "the queue refused the job once its after entries succeeded: {code}: {message}"
                )
            }
        }
    }
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
    /// Tasks and jobs on this origin that must each succeed before the job is sent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Dependencies>,
    /// Whether the job was held, so the origin owns sending it and owes its
    /// thread a final event even when the queue refuses it; set once, when
    /// the route is saved, and kept by every later send
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub owes_final_event: bool,
    /// When the origin took a CI job, which starts its wait; sent with the
    /// submit so the authority's timing counts the wait on the origin
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// The authority's definite refusal of a submit
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRefusal {
    /// Error code
    pub code: String,
    /// Error message
    pub message: String,
    /// Structured error response kept for identical retries
    pub error: serde_json::Value,
    /// HTTP status of the refusal
    pub status: u16,
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
