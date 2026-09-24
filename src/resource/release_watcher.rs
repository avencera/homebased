//! Canonical release-watcher command and its socket-only poll protocol
//!
//! The resource authority builds the only accepted watcher command from typed release
//! identities. The watcher always runs on the authority, even when its supervisor
//! thread and callback route are on another machine. The hidden watcher subcommand
//! sends the same identities back through the local daemon socket; neither side
//! accepts caller-authored command text

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ActionId, ReleaseWatcherIntent, ReleaseWatcherTaskId, ResourceId, ResourceRevision};
use crate::domain::{API_VERSION, TaskId, TaskName, ThreadId};
use crate::error::AppError;
use crate::invocation::CommandLine;
use crate::spec::{NormalizedSpec, NormalizedTaskWorkload, NormalizedWorkload};
use crate::submission::{NormalizedSpecSha256, normalized_spec_sha256};

/// Hidden Homebased subcommand that runs one bound release watcher
pub const RELEASE_WATCHER_SUBCOMMAND: &str = "resource-release-watcher";

/// Socket-only route that serves one watcher poll
pub const RELEASE_WATCHER_POLL_PATH: &str = "/v1/internal/resource-release-watcher/poll";

/// Version of the watcher poll request and response documents
pub const RELEASE_WATCHER_PROTOCOL_VERSION: u32 = 1;

const RELEASE_WATCHER_TASK_NAME: &str = "resource release watcher";
const RELEASE_WATCHER_CWD: &str = "/";

// checkpoints were measured about 47-49 minutes apart, so the ordinary
// inactivity reminder must not fire while the watcher waits silently for one
const RELEASE_WATCHER_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);

/// Exact release identities carried by one watcher command and every poll
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseWatcherCommand {
    /// Resource whose release action owns the watcher
    pub resource_id: ResourceId,
    /// Stable release action identity
    pub action_id: ActionId,
    /// Resource revision saved with the release action
    pub state_revision: ResourceRevision,
    /// Exact registered trainer task observed by the release action
    pub trainer_task_id: TaskId,
    /// Preallocated Homebased task identity of this watcher
    pub watcher_task_id: ReleaseWatcherTaskId,
}

impl ReleaseWatcherCommand {
    /// Build the command identities from one saved watcher intent
    #[must_use]
    pub fn from_intent(resource_id: ResourceId, intent: &ReleaseWatcherIntent) -> Self {
        Self {
            resource_id,
            action_id: intent.action_id,
            state_revision: intent.state_revision,
            trainer_task_id: intent.observed_background_task,
            watcher_task_id: intent.watcher_task_id,
        }
    }

    /// Return the canonical argv for `executable`
    pub fn argv(&self, executable: &Path) -> Result<Vec<String>, AppError> {
        if !executable.is_absolute() {
            return Err(AppError::Internal {
                message: format!(
                    "release watcher executable {} is not absolute",
                    executable.display()
                ),
            });
        }
        let Some(program) = executable.to_str() else {
            return Err(AppError::Internal {
                message: format!(
                    "release watcher executable {} is not UTF-8",
                    executable.display()
                ),
            });
        };

        Ok(vec![
            program.to_owned(),
            RELEASE_WATCHER_SUBCOMMAND.to_owned(),
            "--resource-id".to_owned(),
            self.resource_id.as_uuid().to_string(),
            "--action-id".to_owned(),
            self.action_id.as_uuid().to_string(),
            "--state-revision".to_owned(),
            self.state_revision.get().to_string(),
            "--trainer-task-id".to_owned(),
            self.trainer_task_id.to_string(),
            "--watcher-task-id".to_owned(),
            self.watcher_task_id.as_task_id().to_string(),
        ])
    }

    /// Build the one normalized spec the authority accepts for this watcher
    ///
    /// The supervisor thread receives the watcher's ordinary task callbacks
    pub fn normalized_spec(
        &self,
        executable: &Path,
        supervisor_thread: ThreadId,
    ) -> Result<NormalizedSpec, AppError> {
        let command = CommandLine::try_from_argv(self.argv(executable)?).map_err(|error| {
            AppError::Internal {
                message: format!("release watcher command: {error}"),
            }
        })?;
        let name =
            TaskName::parse(RELEASE_WATCHER_TASK_NAME).map_err(|error| AppError::Internal {
                message: format!("release watcher name: {error}"),
            })?;

        Ok(NormalizedSpec {
            api_version: API_VERSION,
            thread: supervisor_thread,
            name,
            cwd: PathBuf::from(RELEASE_WATCHER_CWD),
            machine: None,
            timeout: RELEASE_WATCHER_TIMEOUT,
            workload: NormalizedWorkload::Task(NormalizedTaskWorkload { command }),
        })
    }

    /// Return the digest of the canonical normalized spec
    pub fn normalized_spec_sha256(
        &self,
        executable: &Path,
        supervisor_thread: ThreadId,
    ) -> Result<NormalizedSpecSha256, AppError> {
        Ok(normalized_spec_sha256(
            &self.normalized_spec(executable, supervisor_thread)?,
        )?)
    }
}

/// Resolve the executable that the authority places in every watcher command
///
/// The watcher is a hidden subcommand of the same binary that runs `task-run`
pub(crate) fn release_watcher_executable() -> Result<PathBuf, AppError> {
    crate::runner::task_run_executable()
}

/// One strict watcher poll sent over the local daemon socket
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseWatcherPollRequest {
    /// Poll protocol version, always [`RELEASE_WATCHER_PROTOCOL_VERSION`]
    pub protocol_version: u32,
    /// Exact identities from the watcher's canonical command
    pub watcher: ReleaseWatcherCommand,
}

impl ReleaseWatcherPollRequest {
    /// Build a current-version poll for one watcher command
    #[must_use]
    pub const fn new(watcher: ReleaseWatcherCommand) -> Self {
        Self {
            protocol_version: RELEASE_WATCHER_PROTOCOL_VERSION,
            watcher,
        }
    }
}

/// Versioned authority reply to one watcher poll
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseWatcherPollResponse {
    /// Poll protocol version, always [`RELEASE_WATCHER_PROTOCOL_VERSION`]
    pub protocol_version: u32,
    /// Typed authority decision for this poll
    pub outcome: ReleaseWatcherPollOutcome,
}

/// Authority decision for one release-watcher poll
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReleaseWatcherPollOutcome {
    /// The accepted watcher task has not reached its saved running identity yet
    WatcherNotRunning,
    /// The exact trainer task has not started
    WaitingForTrainerStart,
    /// No complete checkpoint outside the saved baseline is available yet
    WaitingForCheckpoint,
    /// A matching final result exists, so the watcher waits for trainer exit without cancelling
    CompletedResultAwaitingTrainerExit,
    /// The trainer finished with its final result; the resource owner proves release
    TrainerCompleted,
    /// The saved stop decision and exact trainer cancellation marker are committed
    StopCommitted {
        /// Checkpoint generation fixed by the saved stop decision
        generation_id: String,
        /// Durable cancellation marker time on the trainer task
        cancel_requested_at: DateTime<Utc>,
    },
    /// The release action already completed with an authority-built proof
    ReleaseSettled,
    /// The loan stays reserved and the release action needs attention
    Attention {
        /// Typed reason the watcher cannot advance the release action
        reason: ReleaseWatcherPollAttention,
    },
}

impl ReleaseWatcherPollOutcome {
    /// Whether the watcher has finished its part of the release action
    #[must_use]
    pub const fn is_final(&self) -> bool {
        matches!(
            self,
            Self::TrainerCompleted
                | Self::StopCommitted { .. }
                | Self::ReleaseSettled
                | Self::Attention { .. }
        )
    }
}

/// Why a watcher poll cannot advance its release action
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseWatcherPollAttention {
    /// This daemon is not the resource authority
    WrongAuthority,
    /// The poll names an action, revision, or trainer that is not the saved release action
    ActionNotCurrent,
    /// The poll names a watcher task other than the saved watcher identity
    WrongWatcher,
    /// The release action has no saved watcher identity
    WatcherIntentMissing,
    /// The accepted watcher task no longer matches its saved launch identity
    WatcherIdentityConflict,
    /// The registered trainer task, command, association, or cancel marker changed
    TrainerChanged,
    /// Trainer publications changed or cannot be verified
    PublicationChanged,
    /// Homebased lost the trainer task, so process state is unknown
    TrainerLost,
    /// The trainer ended without a successful result or saved stop decision
    TrainerFailed,
    /// Trainer publications appeared before its task started
    PublicationBeforeTrainerStart,
    /// The trainer exited successfully without a matching final result
    TrainerCompletedWithoutResult,
    /// A saved authority record cannot be decoded, so a retry cannot succeed
    CorruptRecord,
}

#[cfg(test)]
mod tests {
    use super::{ReleaseWatcherCommand, ReleaseWatcherPollRequest};
    use crate::domain::{TaskId, ThreadId};
    use crate::resource::{ActionId, ReleaseWatcherTaskId, ResourceId, ResourceRevision};
    use std::path::Path;

    fn command() -> ReleaseWatcherCommand {
        ReleaseWatcherCommand {
            resource_id: ResourceId::new(),
            action_id: ActionId::new(),
            state_revision: ResourceRevision::new(3),
            trainer_task_id: TaskId::new(),
            watcher_task_id: ReleaseWatcherTaskId::new(TaskId::new()),
        }
    }

    #[test]
    fn every_release_identity_changes_the_canonical_digest() {
        let executable = Path::new("/usr/local/bin/homebased");
        let thread = ThreadId(uuid::Uuid::now_v7());
        let base = command();
        let digest = base.normalized_spec_sha256(executable, thread).unwrap();
        let variants = [
            ReleaseWatcherCommand {
                resource_id: ResourceId::new(),
                ..base
            },
            ReleaseWatcherCommand {
                action_id: ActionId::new(),
                ..base
            },
            ReleaseWatcherCommand {
                state_revision: ResourceRevision::new(4),
                ..base
            },
            ReleaseWatcherCommand {
                trainer_task_id: TaskId::new(),
                ..base
            },
            ReleaseWatcherCommand {
                watcher_task_id: ReleaseWatcherTaskId::new(TaskId::new()),
                ..base
            },
        ];
        for variant in variants {
            assert_ne!(
                variant.normalized_spec_sha256(executable, thread).unwrap(),
                digest
            );
        }
        assert_ne!(
            base.normalized_spec_sha256(Path::new("/tmp/homebased"), thread)
                .unwrap(),
            digest
        );
        assert_ne!(
            base.normalized_spec_sha256(executable, ThreadId(uuid::Uuid::now_v7()))
                .unwrap(),
            digest
        );
        assert_eq!(
            base.normalized_spec_sha256(executable, thread).unwrap(),
            digest
        );
    }

    #[test]
    fn relative_executable_is_rejected() {
        assert!(command().argv(Path::new("homebased")).is_err());
    }

    #[test]
    fn poll_documents_reject_unknown_fields() {
        let request = ReleaseWatcherPollRequest::new(command());
        let mut value = serde_json::to_value(request).unwrap();
        assert_eq!(
            serde_json::from_value::<ReleaseWatcherPollRequest>(value.clone()).unwrap(),
            request
        );
        value["watcher"]["command"] = serde_json::json!(["/bin/sh"]);
        assert!(serde_json::from_value::<ReleaseWatcherPollRequest>(value).is_err());
    }
}
