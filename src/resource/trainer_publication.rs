//! Read-only detection of newly published trainer recovery generations

use std::collections::BTreeSet;
use std::fs::{self, File, Metadata};
use std::io::{self, Read};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use super::ownership_lock::TrainerRequestDigest;
use crate::digest::Sha256Digest;
use crate::domain::{ExitReason, TaskState};

const PUBLICATIONS_DIRECTORY: &str = "published";
const RECOVERY_RECORD: &str = "segment-record.json";
const RECOVERY_SCHEMA: &str = "trainer-direct-recovery-v1";
const SNAPSHOT_SCHEMA: &str = "trainer-recovery-snapshot-v1";
const STAGING_PREFIX: &str = ".publish-";
const TERMINAL_RESULT_PREFIX: &str = "result-";
const TERMINAL_FILE: &str = "terminal.json";
const REQUEST_FILE: &str = "request.json";
const PROTOCOL_SCHEMA_VERSION: u64 = 1;

/// Exact trainer identity of one direct-segment attempt, not a Homebased task ID
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptBinding {
    /// Campaign identity from the trainer request
    #[serde(deserialize_with = "deserialize_identifier")]
    pub campaign_id: String,
    /// Campaign revision identity from the trainer request
    #[serde(deserialize_with = "deserialize_identifier")]
    pub campaign_revision_id: String,
    /// Trainer task identity from the trainer request
    #[serde(deserialize_with = "deserialize_identifier")]
    pub task_id: String,
    /// Direct-segment attempt identity from the trainer request
    #[serde(deserialize_with = "deserialize_identifier")]
    pub attempt_id: String,
    /// One-based attempt number from the trainer request
    #[serde(deserialize_with = "deserialize_positive_integer")]
    pub attempt_number: u64,
    /// Fencing token from the trainer request
    #[serde(deserialize_with = "deserialize_identifier")]
    pub ownership_token: String,
}

impl AttemptBinding {
    pub(crate) fn validate(&self) -> Result<(), WatcherError> {
        for identifier in [
            &self.campaign_id,
            &self.campaign_revision_id,
            &self.task_id,
            &self.attempt_id,
            &self.ownership_token,
        ] {
            validate_identifier(identifier)
                .map_err(|reason| WatcherError::InvalidAttemptBinding { reason })?;
        }

        if self.attempt_number == 0 {
            return Err(WatcherError::InvalidAttemptBinding {
                reason: "attempt_number must be positive",
            });
        }

        Ok(())
    }
}

/// Baseline generation identities captured before a release watch starts
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverySnapshot {
    binding: AttemptBinding,
    generation_ids: BTreeSet<String>,
}

impl RecoverySnapshot {
    pub(crate) fn is_for(&self, binding: &AttemptBinding) -> bool {
        self.binding == *binding
    }

    pub(crate) fn contains_generation(&self, generation_id: &str) -> bool {
        self.generation_ids.contains(generation_id)
    }
}

impl Serialize for RecoverySnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.binding.validate().map_err(ser::Error::custom)?;
        for generation_id in &self.generation_ids {
            validate_identifier(generation_id).map_err(ser::Error::custom)?;
        }

        let mut snapshot = serializer.serialize_struct("RecoverySnapshot", 3)?;
        snapshot.serialize_field("schema", SNAPSHOT_SCHEMA)?;
        snapshot.serialize_field("binding", &self.binding)?;
        snapshot.serialize_field("generation_ids", &self.generation_ids)?;
        snapshot.end()
    }
}

impl<'de> Deserialize<'de> for RecoverySnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SnapshotDocument {
            schema: String,
            binding: AttemptBinding,
            generation_ids: Vec<String>,
        }

        let document = SnapshotDocument::deserialize(deserializer)?;
        if document.schema != SNAPSHOT_SCHEMA {
            return Err(de::Error::custom("unsupported recovery snapshot schema"));
        }
        document.binding.validate().map_err(de::Error::custom)?;

        let mut generation_ids = BTreeSet::new();
        for generation_id in document.generation_ids {
            validate_identifier(&generation_id).map_err(de::Error::custom)?;
            if !generation_ids.insert(generation_id) {
                return Err(de::Error::custom("duplicate recovery generation id"));
            }
        }

        Ok(Self {
            binding: document.binding,
            generation_ids,
        })
    }
}

/// One complete recovery publication for an exact trainer attempt
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedRecoveryGeneration {
    /// Path to the immutable published generation directory
    pub path: PathBuf,
    /// Generation identity committed in the recovery record
    pub generation_id: String,
    /// Trainer update count committed in the recovery record
    pub committed_update_count: u64,
}

/// Content identity for one complete checkpoint publication
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerifiedCheckpointPublication {
    pub(crate) binding: AttemptBinding,
    pub(crate) path: PathBuf,
    pub(crate) generation_id: String,
    pub(crate) committed_update_count: u64,
    pub(crate) record_sha256: Sha256Digest,
    pub(crate) inventory_sha256: Sha256Digest,
}

impl VerifiedCheckpointPublication {
    fn observed_generation(&self) -> PublishedRecoveryGeneration {
        PublishedRecoveryGeneration {
            path: self.path.clone(),
            generation_id: self.generation_id.clone(),
            committed_update_count: self.committed_update_count,
        }
    }
}

/// One fully verified final result publication for an exact trainer attempt
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedTerminalResult {
    /// Immutable result publication directory
    pub publication_path: PathBuf,
    /// Terminal evidence written under the matching trainer attempt
    pub terminal_path: PathBuf,
    /// Exact trainer identity committed by the request and terminal
    pub binding: AttemptBinding,
    /// Output paths bound by the result receipt
    pub output_paths: Vec<String>,
    /// SHA-256 digest of the exact request bytes validated for this result
    pub request_sha256: TrainerRequestDigest,
    /// SHA-256 digest of the terminal record, request, and verified output inventory
    pub publication_sha256: Sha256Digest,
}

/// Complete publication evidence observed during one release-watch decision
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedPublications {
    /// New complete checkpoint not present in the persisted baseline
    pub new_checkpoint: Option<PublishedRecoveryGeneration>,
    /// Complete matching final result, if it has been published
    pub completed_result: Option<PublishedTerminalResult>,
}

/// Read-only conclusion from publication evidence and one exact Homebased task state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchObservation {
    /// The task is queued and has not published attempt evidence yet
    WaitingForTaskStart,
    /// The task is running and has not published a new checkpoint or final result
    WaitingForCheckpoint,
    /// A matching checkpoint is available while the task is running
    ///
    /// This only permits a caller to consider requesting a stop. It does not
    /// mean that a stop was requested, that the process exited, or that the GPU
    /// was released
    StopRequestCandidate {
        /// New complete checkpoint that prompted this candidate
        checkpoint: PublishedRecoveryGeneration,
    },
    /// A final result is published, but Homebased still reports the task as running
    ///
    /// Wait for the exact task to end. Do not request a stop based on a checkpoint
    /// observed at the same time as this result
    CompletedResultAwaitingTaskExit {
        /// Verified final result for the exact trainer attempt
        result: PublishedTerminalResult,
        /// New checkpoint observed with the result, if any
        new_checkpoint: Option<PublishedRecoveryGeneration>,
    },
    /// The exact task ended successfully and a matching final result is published
    ///
    /// This is only a candidate for the resource workflow's `AlreadyCompleted`
    /// path. The caller must still prove that the GPU-owning process or container
    /// has exited. This observation never confirms GPU release
    AlreadyCompletedCandidate {
        /// Verified final result for the exact trainer attempt
        result: PublishedTerminalResult,
        /// New checkpoint observed with the result, if any
        new_checkpoint: Option<PublishedRecoveryGeneration>,
    },
    /// The task state or its relation to the publications needs caller attention
    Attention(WatcherAttention),
}

/// Task or publication state that cannot safely advance a release watch
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatcherAttention {
    /// Homebased lost the exact task, so process state is unknown
    LostTask {
        /// Publications observed before process state became unknown
        publications: ObservedPublications,
    },
    /// The exact task ended without a successful zero exit
    FailedTask {
        /// Recorded reason for the task exit
        reason: ExitReason,
        /// Publications observed before the task ended
        publications: ObservedPublications,
    },
    /// Attempt evidence appeared before Homebased reported the task as running
    PublicationBeforeTaskStart {
        /// Publications inconsistent with the queued task state
        publications: ObservedPublications,
    },
    /// The task exited successfully but did not publish a matching final result
    SuccessfulTaskWithoutFinalResult {
        /// New checkpoint observed before the successful task exit, if any
        new_checkpoint: Option<PublishedRecoveryGeneration>,
    },
}

/// Read-only publication detection errors that need caller attention
#[derive(Debug, thiserror::Error)]
pub enum WatcherError {
    /// A filesystem operation failed while reading publication state
    #[error("cannot inspect {path}: {source}")]
    Io {
        /// Path involved in the failed filesystem operation
        path: PathBuf,
        /// Underlying filesystem error
        #[source]
        source: io::Error,
    },
    /// The detector found a symbolic link where a regular path is required
    #[error("refusing symlinked trainer publication path {path}")]
    Symlink {
        /// Symlink path that cannot be trusted as a publication
        path: PathBuf,
    },
    /// The runtime or publication root has an invalid filesystem type
    #[error("invalid trainer publication directory {path}: {reason}")]
    InvalidDirectory {
        /// Directory path with the invalid filesystem type
        path: PathBuf,
        /// Why the path cannot be used as a publication directory
        reason: &'static str,
    },
    /// A candidate publication is incomplete or does not follow the record contract
    #[error("malformed trainer publication at {path}: {reason}")]
    MalformedPublication {
        /// Candidate generation directory
        path: PathBuf,
        /// Why the candidate cannot be trusted as a complete publication
        reason: String,
    },
    /// Caller-supplied trainer attempt identity is invalid
    #[error("invalid trainer attempt binding: {reason}")]
    InvalidAttemptBinding {
        /// Invalid trainer identifier or attempt number
        reason: &'static str,
    },
    /// A baseline cannot be reused for a different trainer attempt
    #[error("recovery snapshot belongs to a different trainer attempt")]
    SnapshotBindingMismatch,
}

/// Capture complete recovery generation identities for one exact attempt
///
/// Missing runtime or publication directories produce an empty baseline
/// Malformed candidate publications and unsafe symlinks return an error so the
/// caller can retain the resource and request attention
pub fn snapshot(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
) -> Result<RecoverySnapshot, WatcherError> {
    let generations = read_matching_generations(runtime_root, expected_binding)?;
    let generation_ids = generations
        .into_iter()
        .map(|generation| generation.generation_id)
        .collect();

    Ok(RecoverySnapshot {
        binding: expected_binding.clone(),
        generation_ids,
    })
}

/// Find the first deterministic complete publication not present in the baseline
///
/// Results are ordered by generation identity. A returned publication only
/// identifies a checkpoint; it does not establish process exit or release a GPU
pub fn find_new(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
    baseline: &RecoverySnapshot,
) -> Result<Option<PublishedRecoveryGeneration>, WatcherError> {
    Ok(
        find_new_publication(runtime_root, expected_binding, baseline)?
            .map(|generation| generation.observed_generation()),
    )
}

fn find_new_publication(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
    baseline: &RecoverySnapshot,
) -> Result<Option<VerifiedCheckpointPublication>, WatcherError> {
    expected_binding.validate()?;
    if baseline.binding != *expected_binding {
        return Err(WatcherError::SnapshotBindingMismatch);
    }

    Ok(read_matching_generations(runtime_root, expected_binding)?
        .into_iter()
        .find(|generation| !baseline.generation_ids.contains(&generation.generation_id)))
}

/// Revalidate the exact complete publication selected for a stop reservation
pub(crate) fn revalidate_checkpoint_publication(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
    expected: &VerifiedCheckpointPublication,
) -> Result<bool, WatcherError> {
    if expected.binding != *expected_binding {
        return Err(WatcherError::SnapshotBindingMismatch);
    }

    Ok(read_matching_generations(runtime_root, expected_binding)?
        .into_iter()
        .find(|generation| generation.generation_id == expected.generation_id)
        .is_some_and(|generation| generation == *expected))
}

/// Find a complete successful result and matching terminal evidence for one attempt
///
/// A result is accepted only when the publication, persisted request, and
/// attempt-local terminal file agree on the exact trainer identity. The
/// publication inventory is checked against the files on disk
pub fn find_completed_result(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
) -> Result<Option<PublishedTerminalResult>, WatcherError> {
    expected_binding.validate()?;
    let publications_root = publication_root(runtime_root)?;

    let result_path = publications_root.map(|root| {
        root.join(format!(
            "{TERMINAL_RESULT_PREFIX}{}",
            expected_binding.attempt_id
        ))
    });
    let result_metadata = match &result_path {
        Some(path) => optional_directory(path)?,
        None => None,
    };

    let attempts_path = runtime_root.join("attempts");
    let attempts_metadata = optional_directory(&attempts_path)?;
    let attempt_path = attempts_path.join(&expected_binding.attempt_id);
    let attempt_metadata = if attempts_metadata.is_some() {
        optional_directory(&attempt_path)?
    } else {
        None
    };
    let terminal_path = attempt_path.join(TERMINAL_FILE);
    let terminal_metadata = if attempt_metadata.is_some() {
        optional_file(&terminal_path)?
    } else {
        None
    };

    let Some(result_metadata) = result_metadata else {
        let Some(terminal_metadata) = terminal_metadata else {
            return Ok(None);
        };
        let terminal = read_terminal_header(&terminal_path, &terminal_metadata, expected_binding)?;
        if terminal.is_completed {
            return Err(malformed_publication(
                &terminal_path,
                "completed terminal evidence has no result publication",
            ));
        }

        return Ok(None);
    };

    let Some(result_path) = result_path else {
        return Err(malformed_publication(
            runtime_root,
            "result directory exists without a publication root",
        ));
    };
    reject_symlink(&result_path, &result_metadata)?;
    if !result_metadata.is_dir() {
        return Err(malformed_publication(
            &result_path,
            "result publication is not a directory",
        ));
    }
    if attempt_metadata.is_none() {
        return Err(malformed_publication(
            &result_path,
            "result publication has no matching attempt directory",
        ));
    }
    let Some(terminal_metadata) = terminal_metadata else {
        return Err(malformed_publication(
            &terminal_path,
            "result publication has no matching terminal evidence",
        ));
    };

    let request_path = attempt_path.join(REQUEST_FILE);
    let request_metadata = optional_file(&request_path)?.ok_or_else(|| {
        malformed_publication(&request_path, "result publication has no matching request")
    })?;
    let terminal_bytes = read_regular_file(&terminal_path, &terminal_metadata)?;
    let publication_record_path = result_path.join(RECOVERY_RECORD);
    let publication_record_metadata =
        optional_file(&publication_record_path)?.ok_or_else(|| {
            malformed_publication(&result_path, "result publication has no terminal record")
        })?;
    let publication_bytes =
        read_regular_file(&publication_record_path, &publication_record_metadata)?;
    if terminal_bytes != publication_bytes {
        return Err(malformed_publication(
            &publication_record_path,
            "published terminal record differs from attempt terminal evidence",
        ));
    }

    let terminal: CompletedTerminalDocument = serde_json::from_slice(&publication_bytes)
        .map_err(|error| malformed_publication(&publication_record_path, error.to_string()))?;
    let request_bytes = read_regular_file(&request_path, &request_metadata)?;
    let request: WorkerRequestProjection = serde_json::from_slice(&request_bytes)
        .map_err(|error| malformed_publication(&request_path, error.to_string()))?;

    validate_completed_terminal(
        &terminal,
        &request,
        &request_bytes,
        expected_binding,
        &result_path,
    )?;
    validate_result_files(&result_path, &terminal.terminal.outcome.result.outputs)?;

    let request_sha256 = TrainerRequestDigest::of(&request_bytes);
    let publication_sha256 = completed_publication_sha256(
        &result_path,
        &request_bytes,
        &publication_bytes,
        &terminal.terminal.outcome.result.outputs,
    );

    Ok(Some(PublishedTerminalResult {
        publication_path: result_path,
        terminal_path,
        binding: expected_binding.clone(),
        output_paths: terminal
            .terminal
            .outcome
            .result
            .expected_outputs
            .into_iter()
            .map(|output| output.path)
            .collect(),
        request_sha256,
        publication_sha256,
    }))
}

/// Observe one release watch from its exact attempt, baseline, and Homebased task state
///
/// The task state must belong to the Homebased task that launched `expected_binding`
/// A checkpoint can only produce a stop-request candidate while that task is running
/// A final result takes precedence over a checkpoint and waits for task exit. Even a
/// successful task exit plus a final result is only an `AlreadyCompletedCandidate`;
/// the caller must independently prove that the GPU-owning process or container has
/// exited before it completes resource release. No observation confirms GPU release
///
/// Strict publication errors are returned unchanged so the caller can retain the
/// resource and request attention
pub fn observe_release(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
    baseline: &RecoverySnapshot,
    task_state: &TaskState,
) -> Result<WatchObservation, WatcherError> {
    observe_release_with_checkpoint_evidence(runtime_root, expected_binding, baseline, task_state)
        .map(|(observation, _)| observation)
}

pub(crate) fn observe_release_with_checkpoint_evidence(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
    baseline: &RecoverySnapshot,
    task_state: &TaskState,
) -> Result<(WatchObservation, Option<VerifiedCheckpointPublication>), WatcherError> {
    let verified_checkpoint = find_new_publication(runtime_root, expected_binding, baseline)?;
    let publications = ObservedPublications {
        new_checkpoint: verified_checkpoint
            .as_ref()
            .map(VerifiedCheckpointPublication::observed_generation),
        completed_result: find_completed_result(runtime_root, expected_binding)?,
    };

    let observation = match task_state {
        TaskState::Queued
            if publications.new_checkpoint.is_none() && publications.completed_result.is_none() =>
        {
            WatchObservation::WaitingForTaskStart
        }
        TaskState::Queued => {
            WatchObservation::Attention(WatcherAttention::PublicationBeforeTaskStart {
                publications,
            })
        }
        TaskState::Running { .. } => {
            let ObservedPublications {
                new_checkpoint,
                completed_result,
            } = publications;

            if let Some(result) = completed_result {
                WatchObservation::CompletedResultAwaitingTaskExit {
                    result,
                    new_checkpoint,
                }
            } else if let Some(checkpoint) = new_checkpoint {
                WatchObservation::StopRequestCandidate { checkpoint }
            } else {
                WatchObservation::WaitingForCheckpoint
            }
        }
        TaskState::Finished {
            reason: ExitReason::Exit { code: 0 },
        } => {
            let ObservedPublications {
                new_checkpoint,
                completed_result,
            } = publications;

            if let Some(result) = completed_result {
                WatchObservation::AlreadyCompletedCandidate {
                    result,
                    new_checkpoint,
                }
            } else {
                WatchObservation::Attention(WatcherAttention::SuccessfulTaskWithoutFinalResult {
                    new_checkpoint,
                })
            }
        }
        TaskState::Finished { reason } => {
            WatchObservation::Attention(WatcherAttention::FailedTask {
                reason: reason.clone(),
                publications,
            })
        }
        TaskState::Lost => WatchObservation::Attention(WatcherAttention::LostTask { publications }),
    };
    let selected_checkpoint =
        if matches!(observation, WatchObservation::StopRequestCandidate { .. }) {
            verified_checkpoint
        } else {
            None
        };

    Ok((observation, selected_checkpoint))
}

fn read_terminal_header(
    terminal_path: &Path,
    terminal_metadata: &Metadata,
    expected_binding: &AttemptBinding,
) -> Result<TerminalHeader, WatcherError> {
    let bytes = read_regular_file(terminal_path, terminal_metadata)?;
    let terminal: TerminalHeaderDocument = serde_json::from_slice(&bytes)
        .map_err(|error| malformed_publication(terminal_path, error.to_string()))?;
    if terminal.terminal.schema_version != PROTOCOL_SCHEMA_VERSION {
        return Err(malformed_publication(
            terminal_path,
            "unsupported terminal protocol schema",
        ));
    }
    terminal
        .terminal
        .binding
        .validate()
        .map_err(|error| malformed_publication(terminal_path, error.to_string()))?;
    if terminal.terminal.binding != *expected_binding {
        return Err(malformed_publication(
            terminal_path,
            "terminal evidence belongs to a different trainer attempt",
        ));
    }
    if terminal.kind != terminal.terminal.outcome.kind {
        return Err(malformed_publication(
            terminal_path,
            "terminal kind differs from its outcome",
        ));
    }
    let binding = &terminal.terminal.binding;
    let outcome = &terminal.terminal.outcome;
    match outcome.kind.as_str() {
        "completed"
            if outcome.result.is_some()
                && outcome.recovery.is_none()
                && outcome.failure.is_none() => {}
        "paused_with_recovery"
            if outcome.result.is_none()
                && outcome.recovery.is_some()
                && outcome.failure.is_none() => {}
        "failed"
            if outcome.result.is_none()
                && outcome.recovery.is_none()
                && outcome.failure.is_some() => {}
        _ => {
            return Err(malformed_publication(
                terminal_path,
                "terminal outcome has an invalid variant",
            ));
        }
    }
    if outcome.kind == "completed" {
        let Some(result) = &outcome.result else {
            unreachable!("completed outcome variant is checked above");
        };
        let result_binding: AttemptBinding =
            serde_json::from_value(result.get("binding").cloned().ok_or_else(|| {
                malformed_publication(terminal_path, "completed result has no binding")
            })?)
            .map_err(|error| malformed_publication(terminal_path, error.to_string()))?;
        if result_binding != *binding {
            return Err(malformed_publication(
                terminal_path,
                "terminal result binding differs from terminal binding",
            ));
        }
    }

    Ok(TerminalHeader {
        is_completed: outcome.kind == "completed",
    })
}

fn validate_completed_terminal(
    terminal: &CompletedTerminalDocument,
    request: &WorkerRequestProjection,
    request_bytes: &[u8],
    expected_binding: &AttemptBinding,
    publication_path: &Path,
) -> Result<(), WatcherError> {
    let terminal_binding = &terminal.terminal.binding;
    let result = &terminal.terminal.outcome.result;
    if terminal.kind != "completed"
        || terminal.terminal.schema_version != PROTOCOL_SCHEMA_VERSION
        || terminal.terminal.outcome.kind != "completed"
    {
        return Err(malformed_publication(
            publication_path,
            "result publication is not a completed terminal",
        ));
    }
    if terminal_binding != expected_binding
        || &result.binding != expected_binding
        || &result.outputs.binding != expected_binding
        || &request.binding != expected_binding
    {
        return Err(malformed_publication(
            publication_path,
            "result evidence belongs to a different trainer attempt",
        ));
    }
    if request.schema_version != PROTOCOL_SCHEMA_VERSION
        || result.schema_version != PROTOCOL_SCHEMA_VERSION
    {
        return Err(malformed_publication(
            publication_path,
            "unsupported request or result protocol schema",
        ));
    }
    validate_request_projection(request)
        .map_err(|reason| malformed_publication(publication_path, reason))?;
    validate_worker_identity(&result.worker)
        .map_err(|reason| malformed_publication(publication_path, reason))?;
    if request.worker != result.worker || request.expected_outputs != result.expected_outputs {
        return Err(malformed_publication(
            publication_path,
            "result worker or outputs differ from the persisted request",
        ));
    }
    let request_config_digest = configuration_digest(request_bytes)
        .map_err(|reason| malformed_publication(publication_path, reason))?;
    if result.config_digest != request_config_digest {
        return Err(malformed_publication(
            publication_path,
            "result configuration digest differs from the persisted request",
        ));
    }
    validate_result_receipt(result)
        .map_err(|reason| malformed_publication(publication_path, reason))
}

fn validate_result_receipt(result: &ResultReceiptProjection) -> Result<(), String> {
    if result.outputs.schema_version != PROTOCOL_SCHEMA_VERSION {
        return Err("unsupported result inventory schema".into());
    }
    validate_output_declarations(&result.expected_outputs)?;
    result
        .outputs
        .binding
        .validate()
        .map_err(|error| error.to_string())?;
    let mut inventory_paths = BTreeSet::new();
    let mut previous_path: Option<&str> = None;
    for entry in &result.outputs.entries {
        validate_artifact_path(&entry.path)?;
        if entry.kind != "file" {
            return Err("direct result inventory contains a non-file artifact".into());
        }
        if previous_path.is_some_and(|previous| previous >= entry.path.as_str()) {
            return Err("result inventory paths are not strictly sorted".into());
        }
        previous_path = Some(&entry.path);
        if !inventory_paths.insert(entry.path.as_str()) {
            return Err("result inventory contains a duplicate path".into());
        }
    }
    let declarations: BTreeSet<_> = result
        .expected_outputs
        .iter()
        .map(|output| output.path.as_str())
        .collect();
    if declarations.len() != result.expected_outputs.len() || declarations != inventory_paths {
        return Err("result inventory does not match expected outputs".into());
    }
    let mut metric_ids = BTreeSet::new();
    for metric in &result.metrics {
        validate_identifier(&metric.metric_id)?;
        if !metric_ids.insert(metric.metric_id.as_str()) {
            return Err("result contains duplicate metric ids".into());
        }
        if !matches!(
            metric.direction.as_str(),
            "higher_is_better" | "lower_is_better"
        ) || !metric.value.is_finite()
            || metric.denominator == 0
            || !metric.uncertainty.is_finite()
            || metric.uncertainty < 0.0
        {
            return Err("result contains an invalid metric".into());
        }
        validate_metric_unit(&metric.unit)?;
    }
    validate_result_evidence(&result.evidence)?;
    validate_validation_receipt(&result.validation)?;

    Ok(())
}

fn validate_request_projection(request: &WorkerRequestProjection) -> Result<(), String> {
    for document in [
        &request.adapter,
        &request.start_mode,
        &request.payload,
        &request.resources,
    ] {
        if !document.is_object() {
            return Err("persisted request contains a malformed protocol object".into());
        }
    }
    if request.inputs.iter().any(|input| !input.is_object()) {
        return Err("persisted request contains a malformed input reference".into());
    }
    request
        .binding
        .validate()
        .map_err(|error| error.to_string())?;
    if request.input_view.schema_version != PROTOCOL_SCHEMA_VERSION
        || request.input_view.revision_id != request.binding.campaign_revision_id
        || request.input_view.root.trim().is_empty()
        || request.input_view.root.chars().any(char::is_control)
    {
        return Err("persisted input view is not bound to the request revision".into());
    }
    validate_worker_identity(&request.worker)?;
    validate_output_declarations(&request.expected_outputs)
}

/// Failure while checking one persisted direct-segment attempt request
#[derive(Debug)]
pub(crate) enum AttemptRequestValidationError {
    /// The expected binding supplied by the caller is invalid
    InvalidExpectedBinding(&'static str),
    /// The request does not match the maintained trainer request projection
    Malformed(String),
    /// The request is valid but binds a different trainer attempt
    BindingMismatch(Box<AttemptBinding>),
}

/// Validate a request with the same strict projection used for terminal results
///
/// The strict projection rejects duplicate top-level and input-view fields. This
/// also computes the configuration digest used by completed-result validation
pub(crate) fn validate_attempt_request(
    request_bytes: &[u8],
    expected_binding: &AttemptBinding,
) -> Result<(), AttemptRequestValidationError> {
    expected_binding.validate().map_err(|error| match error {
        WatcherError::InvalidAttemptBinding { reason } => {
            AttemptRequestValidationError::InvalidExpectedBinding(reason)
        }
        _ => AttemptRequestValidationError::InvalidExpectedBinding(
            "trainer attempt binding is invalid",
        ),
    })?;

    let request: WorkerRequestProjection = serde_json::from_slice(request_bytes)
        .map_err(|error| AttemptRequestValidationError::Malformed(error.to_string()))?;
    if request.schema_version != PROTOCOL_SCHEMA_VERSION {
        return Err(AttemptRequestValidationError::Malformed(
            "unsupported request protocol schema".into(),
        ));
    }
    validate_request_projection(&request).map_err(AttemptRequestValidationError::Malformed)?;
    configuration_digest(request_bytes).map_err(AttemptRequestValidationError::Malformed)?;

    if request.binding != *expected_binding {
        return Err(AttemptRequestValidationError::BindingMismatch(Box::new(
            request.binding,
        )));
    }

    Ok(())
}

fn validate_metric_unit(unit: &serde_json::Value) -> Result<(), String> {
    match unit {
        serde_json::Value::String(value)
            if matches!(value.as_str(), "ratio" | "seconds" | "count" | "bytes") =>
        {
            Ok(())
        }
        serde_json::Value::Object(fields) if fields.len() == 1 => {
            let Some(serde_json::Value::Object(custom)) = fields.get("custom") else {
                return Err("result contains an invalid metric unit".into());
            };
            if custom.len() != 1
                || custom
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|name| name.trim().is_empty())
            {
                return Err("result contains an invalid custom metric unit".into());
            }
            Ok(())
        }
        _ => Err("result contains an invalid metric unit".into()),
    }
}

fn validate_result_evidence(evidence: &ResultEvidenceProjection) -> Result<(), String> {
    if evidence.source_exposures.is_empty() || evidence.update_points.is_empty() {
        return Err("result evidence is missing source or update evidence".into());
    }
    let mut source_digests = BTreeSet::new();
    for exposure in &evidence.source_exposures {
        if exposure.samples == 0 || !source_digests.insert(exposure.source_digest) {
            return Err("result evidence has an invalid source exposure".into());
        }
    }
    if !unique_values(&evidence.update_points) || !unique_values(&evidence.probe_points) {
        return Err("result evidence contains duplicate points".into());
    }
    if let Some(artifact) = &evidence.evidence_artifact {
        if artifact.schema.version != PROTOCOL_SCHEMA_VERSION
            || artifact.schema.id.trim().is_empty()
            || artifact.schema.id.len() > 128
            || artifact.schema.id.chars().any(char::is_control)
            || artifact.size == 0
        {
            return Err("result evidence artifact has an invalid schema or size".into());
        }
        validate_artifact_path(&artifact.path)?;
    }

    Ok(())
}

fn validate_validation_receipt(receipt: &ValidationReceiptProjection) -> Result<(), String> {
    validate_identifier(&receipt.validator_id)?;
    if receipt.validator_version.is_empty()
        || receipt.validator_version.len() > 32
        || !receipt.validator_version.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.' | b'+')
        })
    {
        return Err("result validation receipt has an invalid validator version".into());
    }
    Ok(())
}

fn unique_values(values: &[u64]) -> bool {
    values.iter().copied().collect::<BTreeSet<_>>().len() == values.len()
}

fn validate_worker_identity(worker: &WorkerIdentityProjection) -> Result<(), String> {
    validate_identifier(&worker.worker_id)?;
    Ok(())
}

fn validate_output_declarations(outputs: &[OutputDeclarationProjection]) -> Result<(), String> {
    if outputs.is_empty() {
        return Err("direct result declares no outputs".into());
    }
    let mut paths = BTreeSet::new();
    for output in outputs {
        validate_artifact_path(&output.path)?;
        if output.kind != "file" {
            return Err("direct result declaration contains a non-file output".into());
        }
        if !paths.insert(output.path.as_str()) {
            return Err("request contains a duplicate output path".into());
        }
    }

    Ok(())
}

fn validate_result_files(
    result_path: &Path,
    inventory: &ArtifactInventoryProjection,
) -> Result<(), WatcherError> {
    let mut expected_files = BTreeSet::from([RECOVERY_RECORD.to_owned()]);
    for entry in &inventory.entries {
        if entry.path == RECOVERY_RECORD || entry.path.starts_with(&format!("{RECOVERY_RECORD}/")) {
            return Err(malformed_publication(
                result_path,
                "result output collides with its terminal record",
            ));
        }
        verify_output_file(result_path, entry)?;
        expected_files.insert(entry.path.clone());
    }

    let mut found_files = BTreeSet::new();
    let mut found_directories = BTreeSet::new();
    collect_publication_members(
        result_path,
        result_path,
        &mut found_files,
        &mut found_directories,
    )?;
    if found_files != expected_files {
        return Err(malformed_publication(
            result_path,
            "result publication files differ from its declared inventory",
        ));
    }
    for directory in found_directories {
        if !inventory
            .entries
            .iter()
            .any(|entry| entry.path.starts_with(&format!("{directory}/")))
        {
            return Err(malformed_publication(
                result_path,
                "result publication contains an unlisted directory",
            ));
        }
    }

    Ok(())
}

fn completed_publication_sha256(
    result_path: &Path,
    request_bytes: &[u8],
    terminal_bytes: &[u8],
    inventory: &ArtifactInventoryProjection,
) -> Sha256Digest {
    let mut digest = Sha256::new();
    digest.update(b"trainer-completed-publication-v1\0");

    let publication_path = result_path.to_string_lossy();
    for value in [publication_path.as_bytes(), request_bytes, terminal_bytes] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }

    for entry in &inventory.entries {
        // the publication digest covers each entry digest as its hex text
        let entry_digest = entry.digest.to_hex();
        for value in [
            entry.path.as_bytes(),
            entry.kind.as_bytes(),
            entry_digest.as_bytes(),
        ] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        }
        digest.update(entry.size.to_be_bytes());
    }

    Sha256Digest::from(digest)
}

fn verify_output_file(
    result_path: &Path,
    entry: &InventoryEntryProjection,
) -> Result<(), WatcherError> {
    let path = result_path.join(&entry.path);
    let mut current = result_path.to_path_buf();
    for component in Path::new(&entry.path).components() {
        let std::path::Component::Normal(component) = component else {
            return Err(malformed_publication(
                result_path,
                "invalid result output path",
            ));
        };
        current.push(component);
        let current_metadata = metadata(&current)?;
        reject_symlink(&current, &current_metadata)?;
        if current != path && !current_metadata.is_dir() {
            return Err(malformed_publication(
                &current,
                "result output parent is not a directory",
            ));
        }
    }
    let file_metadata = metadata(&path)?;
    if !file_metadata.is_file() || file_metadata.len() != entry.size {
        return Err(malformed_publication(
            &path,
            "result output type or size differs from its inventory",
        ));
    }

    let mut file = File::open(&path).map_err(|source| WatcherError::Io {
        path: path.clone(),
        source,
    })?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(|source| WatcherError::Io {
            path: path.clone(),
            source,
        })?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    if Sha256Digest::from(digest) != entry.digest {
        return Err(malformed_publication(
            &path,
            "result output digest differs from its inventory",
        ));
    }

    Ok(())
}

fn collect_publication_members(
    root: &Path,
    directory: &Path,
    files: &mut BTreeSet<String>,
    directories: &mut BTreeSet<String>,
) -> Result<(), WatcherError> {
    let entries = fs::read_dir(directory).map_err(|source| WatcherError::Io {
        path: directory.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| WatcherError::Io {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let entry_metadata = metadata(&path)?;
        reject_symlink(&path, &entry_metadata)?;
        if entry_metadata.is_dir() {
            let relative = path
                .strip_prefix(root)
                .ok()
                .and_then(Path::to_str)
                .ok_or_else(|| malformed_publication(&path, "result path is not valid UTF-8"))?
                .to_owned();
            directories.insert(relative);
            collect_publication_members(root, &path, files, directories)?;
        } else if entry_metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .ok()
                .and_then(Path::to_str)
                .ok_or_else(|| malformed_publication(&path, "result path is not valid UTF-8"))?
                .to_owned();
            files.insert(relative);
        } else {
            return Err(malformed_publication(
                &path,
                "result publication contains an unsupported filesystem entry",
            ));
        }
    }

    Ok(())
}

fn optional_directory(path: &Path) -> Result<Option<Metadata>, WatcherError> {
    let Some(metadata) = optional_metadata(path)? else {
        return Ok(None);
    };
    reject_symlink(path, &metadata)?;
    if !metadata.is_dir() {
        return Err(malformed_publication(path, "expected a regular directory"));
    }

    Ok(Some(metadata))
}

fn optional_file(path: &Path) -> Result<Option<Metadata>, WatcherError> {
    let Some(metadata) = optional_metadata(path)? else {
        return Ok(None);
    };
    reject_symlink(path, &metadata)?;
    if !metadata.is_file() {
        return Err(malformed_publication(path, "expected a regular file"));
    }

    Ok(Some(metadata))
}

fn read_regular_file(path: &Path, metadata: &Metadata) -> Result<Vec<u8>, WatcherError> {
    reject_symlink(path, metadata)?;
    fs::read(path).map_err(|source| WatcherError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn validate_artifact_path(path: &str) -> Result<(), String> {
    if path.is_empty()
        || path.len() > 1024
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains('\\')
        || path.contains(':')
        || path.split('/').any(|component| {
            component.is_empty()
                || matches!(component, "." | "..")
                || component.chars().any(|character| {
                    character.is_control() || matches!(character as u32, 0x7f..=0x9f)
                })
        })
    {
        return Err("invalid trainer artifact path".into());
    }

    Ok(())
}

/// Request members that the trainer's configuration digest covers
///
/// Each member keeps its exact source bytes, so the digest matches the trainer's
/// hash of the persisted request without re-encoding any value
#[derive(Deserialize)]
struct ConfigurationMembers<'a> {
    #[serde(borrow)]
    adapter: &'a RawValue,
    #[serde(borrow)]
    start_mode: &'a RawValue,
    #[serde(borrow)]
    payload: &'a RawValue,
    #[serde(borrow)]
    input_view: InputViewConfigurationMembers<'a>,
    #[serde(borrow)]
    inputs: &'a RawValue,
    #[serde(borrow)]
    expected_outputs: &'a RawValue,
    #[serde(borrow)]
    resources: &'a RawValue,
}

/// Input-view members that the trainer's configuration digest covers
#[derive(Deserialize)]
struct InputViewConfigurationMembers<'a> {
    #[serde(borrow)]
    schema_version: &'a RawValue,
    #[serde(borrow)]
    revision_id: &'a RawValue,
}

/// Hash the configuration members of one persisted request in the trainer's order
///
/// Serde refuses a duplicate member, so no member can have two candidate values
fn configuration_digest(request: &[u8]) -> Result<Sha256Digest, String> {
    let members: ConfigurationMembers<'_> =
        serde_json::from_slice(request).map_err(|error| error.to_string())?;
    let input_view = &members.input_view;
    let canonical = [
        "[",
        members.adapter.get(),
        ",",
        members.start_mode.get(),
        ",",
        members.payload.get(),
        ",{\"schema_version\":",
        input_view.schema_version.get(),
        ",\"revision_id\":",
        input_view.revision_id.get(),
        "},",
        members.inputs.get(),
        ",",
        members.expected_outputs.get(),
        ",",
        members.resources.get(),
        "]",
    ]
    .concat();

    Ok(Sha256Digest::of(canonical))
}

fn read_matching_generations(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
) -> Result<Vec<VerifiedCheckpointPublication>, WatcherError> {
    expected_binding.validate()?;
    let Some(publications_root) = publication_root(runtime_root)? else {
        return Ok(Vec::new());
    };

    let entries = fs::read_dir(&publications_root).map_err(|source| WatcherError::Io {
        path: publications_root.clone(),
        source,
    })?;
    let mut generations = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|source| WatcherError::Io {
            path: publications_root.clone(),
            source,
        })?;
        let path = entry.path();
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| malformed_publication(&path, "publication directory name is not UTF-8"))?;

        if name.starts_with(STAGING_PREFIX) || name.starts_with(TERMINAL_RESULT_PREFIX) {
            continue;
        }

        let directory_metadata = metadata(&path)?;
        reject_symlink(&path, &directory_metadata)?;
        if !directory_metadata.is_dir() {
            return Err(malformed_publication(
                &path,
                "publication entry is not a generation directory",
            ));
        }

        let record_path = path.join(RECOVERY_RECORD);
        let record_metadata = metadata(&record_path)?;
        reject_symlink(&record_path, &record_metadata)?;
        if !record_metadata.is_file() {
            return Err(malformed_publication(
                &path,
                "recovery record is not a regular file",
            ));
        }

        let record = read_regular_file(&record_path, &record_metadata)?;
        let bindings: BindingProjection = serde_json::from_slice(&record)
            .map_err(|error| malformed_publication(&path, error.to_string()))?;
        if bindings.request.binding != *expected_binding
            || bindings.recovery.binding != *expected_binding
        {
            continue;
        }

        let attempt_request_path = runtime_root
            .join("attempts")
            .join(&expected_binding.attempt_id)
            .join(REQUEST_FILE);
        let attempt_request_metadata = optional_file(&attempt_request_path)?.ok_or_else(|| {
            malformed_publication(&path, "checkpoint has no matching attempt request")
        })?;
        let attempt_request = read_regular_file(&attempt_request_path, &attempt_request_metadata)?;
        let publication_request =
            request_document(&record).map_err(|reason| malformed_publication(&path, reason))?;
        let saved_request: serde_json::Value = serde_json::from_slice(&attempt_request)
            .map_err(|error| malformed_publication(&attempt_request_path, error.to_string()))?;
        let published_request: serde_json::Value = serde_json::from_slice(publication_request)
            .map_err(|error| malformed_publication(&path, error.to_string()))?;
        if saved_request != published_request {
            return Err(malformed_publication(
                &path,
                "checkpoint request differs from the exact trainer attempt request",
            ));
        }

        let publication: RecoveryPublicationProjection = serde_json::from_slice(&record)
            .map_err(|error| malformed_publication(&path, error.to_string()))?;
        if publication.schema != RECOVERY_SCHEMA {
            return Err(malformed_publication(
                &path,
                "unsupported direct recovery schema",
            ));
        }
        if publication.request.binding != *expected_binding
            || publication.recovery.binding != *expected_binding
        {
            continue;
        }
        if publication.request.schema_version != PROTOCOL_SCHEMA_VERSION
            || publication.recovery.schema_version != PROTOCOL_SCHEMA_VERSION
            || publication.recovery.inventory.schema_version != PROTOCOL_SCHEMA_VERSION
            || publication.recovery.inventory.binding != *expected_binding
            || !matches!(
                publication.recovery.cause.as_str(),
                "periodic_checkpoint" | "stop_requested" | "non_finite_training"
            )
        {
            return Err(malformed_publication(
                &path,
                "recovery publication has an unsupported schema or inventory binding",
            ));
        }
        validate_request_projection(&publication.request)
            .map_err(|reason| malformed_publication(&path, reason))?;
        validate_worker_identity(&publication.recovery.worker)
            .map_err(|reason| malformed_publication(&path, reason))?;
        if publication.request.worker != publication.recovery.worker
            || publication.recovery.compatibility.schema_id != "speakrs-long-run-train-v1"
            || publication.recovery.compatibility.source_digest
                != publication.request.worker.source_digest
            || publication.recovery.compatibility.config_digest
                != configuration_digest(
                    request_document(&record)
                        .map_err(|reason| malformed_publication(&path, reason))?,
                )
                .map_err(|reason| malformed_publication(&path, reason))?
        {
            return Err(malformed_publication(
                &path,
                "recovery compatibility differs from the persisted request",
            ));
        }
        validate_identifier(&publication.recovery.generation_id)
            .map_err(|reason| malformed_publication(&path, reason))?;
        if name != publication.recovery.generation_id {
            return Err(malformed_publication(
                &path,
                "directory name differs from recovery.generation_id",
            ));
        }
        validate_checkpoint_inventory(
            &path,
            &publication.recovery.inventory,
            expected_binding,
            publication.recovery.state_digest,
        )?;

        let final_record_metadata = metadata(&record_path)?;
        let final_record = read_regular_file(&record_path, &final_record_metadata)?;
        if final_record != record {
            return Err(malformed_publication(
                &record_path,
                "recovery record changed during publication verification",
            ));
        }

        let inventory_bytes = serde_json::to_vec(&publication.recovery.inventory)
            .map_err(|error| malformed_publication(&path, error.to_string()))?;

        generations.push(VerifiedCheckpointPublication {
            binding: expected_binding.clone(),
            path,
            generation_id: publication.recovery.generation_id,
            committed_update_count: publication.recovery.position.update_count,
            record_sha256: Sha256Digest::of(&record),
            inventory_sha256: Sha256Digest::of(&inventory_bytes),
        });
    }

    generations.sort_by(|left, right| left.generation_id.cmp(&right.generation_id));
    Ok(generations)
}

/// Borrow the exact source bytes of the request member of one recovery record
fn request_document(record: &[u8]) -> Result<&[u8], String> {
    #[derive(Deserialize)]
    struct RecordRequest<'a> {
        #[serde(borrow)]
        request: &'a RawValue,
    }

    let record: RecordRequest<'_> =
        serde_json::from_slice(record).map_err(|error| error.to_string())?;
    Ok(record.request.get().as_bytes())
}

fn validate_checkpoint_inventory(
    path: &Path,
    inventory: &ArtifactInventoryProjection,
    expected_binding: &AttemptBinding,
    state_digest: Sha256Digest,
) -> Result<(), WatcherError> {
    let mut previous_path: Option<&str> = None;
    let mut paths = BTreeSet::new();
    for entry in &inventory.entries {
        validate_artifact_path(&entry.path)
            .map_err(|reason| malformed_publication(path, reason))?;
        if entry.kind != "file"
            || previous_path.is_some_and(|previous| previous >= entry.path.as_str())
            || !paths.insert(entry.path.as_str())
        {
            return Err(malformed_publication(
                path,
                "recovery inventory must contain sorted unique regular files",
            ));
        }
        previous_path = Some(&entry.path);
    }

    let recovery_state = inventory
        .entries
        .iter()
        .find(|entry| entry.path == "recovery.json")
        .ok_or_else(|| malformed_publication(path, "recovery inventory omits recovery.json"))?;
    if recovery_state.digest != state_digest {
        return Err(malformed_publication(
            path,
            "recovery state digest differs from its inventory",
        ));
    }
    if inventory.binding != *expected_binding {
        return Err(malformed_publication(
            path,
            "recovery inventory belongs to a different trainer attempt",
        ));
    }

    validate_result_files(path, inventory)
}

fn publication_root(runtime_root: &Path) -> Result<Option<PathBuf>, WatcherError> {
    let Some(runtime_metadata) = optional_metadata(runtime_root)? else {
        return Ok(None);
    };
    reject_symlink(runtime_root, &runtime_metadata)?;
    if !runtime_metadata.is_dir() {
        return Err(WatcherError::InvalidDirectory {
            path: runtime_root.to_path_buf(),
            reason: "runtime root is not a directory",
        });
    }

    let publications_root = runtime_root.join(PUBLICATIONS_DIRECTORY);
    let Some(publications_metadata) = optional_metadata(&publications_root)? else {
        return Ok(None);
    };
    reject_symlink(&publications_root, &publications_metadata)?;
    if !publications_metadata.is_dir() {
        return Err(WatcherError::InvalidDirectory {
            path: publications_root,
            reason: "published root is not a directory",
        });
    }

    Ok(Some(publications_root))
}

fn optional_metadata(path: &Path) -> Result<Option<Metadata>, WatcherError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(WatcherError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn metadata(path: &Path) -> Result<Metadata, WatcherError> {
    fs::symlink_metadata(path).map_err(|source| WatcherError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn reject_symlink(path: &Path, metadata: &Metadata) -> Result<(), WatcherError> {
    if metadata.file_type().is_symlink() {
        return Err(WatcherError::Symlink {
            path: path.to_path_buf(),
        });
    }

    Ok(())
}

fn malformed_publication(path: &Path, reason: impl Into<String>) -> WatcherError {
    WatcherError::MalformedPublication {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

fn deserialize_identifier<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let identifier = String::deserialize(deserializer)?;
    validate_identifier(&identifier).map_err(de::Error::custom)?;
    Ok(identifier)
}

fn deserialize_positive_integer<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value == 0 {
        return Err(de::Error::custom("attempt_number must be positive"));
    }

    Ok(value)
}

fn validate_identifier(identifier: &str) -> Result<(), &'static str> {
    if identifier.is_empty() || identifier.len() > 128 {
        return Err("trainer identifiers must contain 1 to 128 ASCII bytes");
    }

    let mut characters = identifier.chars();
    let Some(first) = characters.next() else {
        return Err("trainer identifiers must not be empty");
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err("trainer identifier has an invalid first character");
    }
    if characters.any(|character| {
        !character.is_ascii()
            || (!character.is_ascii_lowercase()
                && !character.is_ascii_digit()
                && !matches!(character, '-' | '_' | '.'))
    }) {
        return Err("trainer identifier has an invalid character");
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct BindingProjection {
    request: RequestBindingProjection,
    recovery: RecoveryBindingProjection,
}

#[derive(Debug, Deserialize)]
struct RequestBindingProjection {
    binding: AttemptBinding,
}

#[derive(Debug, Deserialize)]
struct RecoveryBindingProjection {
    binding: AttemptBinding,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryPublicationProjection {
    schema: String,
    request: WorkerRequestProjection,
    recovery: RecoveryMetadataProjection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryMetadataProjection {
    schema_version: u64,
    binding: AttemptBinding,
    generation_id: String,
    compatibility: RecoveryCompatibilityProjection,
    worker: WorkerIdentityProjection,
    inventory: ArtifactInventoryProjection,
    state_digest: Sha256Digest,
    position: RecoveryPositionProjection,
    cause: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryCompatibilityProjection {
    schema_id: String,
    config_digest: Sha256Digest,
    source_digest: Sha256Digest,
}

#[derive(Debug, Deserialize)]
struct RecoveryPositionProjection {
    update_count: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalHeaderDocument {
    kind: String,
    terminal: TerminalHeaderProposal,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalHeaderProposal {
    schema_version: u64,
    binding: AttemptBinding,
    outcome: TerminalOutcomeHeader,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalOutcomeHeader {
    kind: String,
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    recovery: Option<serde_json::Value>,
    #[serde(default)]
    failure: Option<serde_json::Value>,
}

#[derive(Debug)]
struct TerminalHeader {
    is_completed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletedTerminalDocument {
    kind: String,
    terminal: CompletedTerminalProposal,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletedTerminalProposal {
    schema_version: u64,
    binding: AttemptBinding,
    outcome: CompletedTerminalOutcome,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletedTerminalOutcome {
    kind: String,
    result: ResultReceiptProjection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerRequestProjection {
    schema_version: u64,
    binding: AttemptBinding,
    adapter: serde_json::Value,
    start_mode: serde_json::Value,
    payload: serde_json::Value,
    input_view: PreparedInputViewProjection,
    inputs: Vec<serde_json::Value>,
    expected_outputs: Vec<OutputDeclarationProjection>,
    resources: serde_json::Value,
    worker: WorkerIdentityProjection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedInputViewProjection {
    schema_version: u64,
    revision_id: String,
    root: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultReceiptProjection {
    schema_version: u64,
    binding: AttemptBinding,
    worker: WorkerIdentityProjection,
    config_digest: Sha256Digest,
    expected_outputs: Vec<OutputDeclarationProjection>,
    outputs: ArtifactInventoryProjection,
    metrics: Vec<MetricProjection>,
    evidence: ResultEvidenceProjection,
    validation: ValidationReceiptProjection,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct WorkerIdentityProjection {
    worker_id: String,
    executable_digest: Sha256Digest,
    source_digest: Sha256Digest,
    environment_digest: Sha256Digest,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct OutputDeclarationProjection {
    path: String,
    kind: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactInventoryProjection {
    schema_version: u64,
    binding: AttemptBinding,
    entries: Vec<InventoryEntryProjection>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryEntryProjection {
    path: String,
    kind: String,
    size: u64,
    digest: Sha256Digest,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricProjection {
    metric_id: String,
    unit: serde_json::Value,
    direction: String,
    value: f64,
    denominator: u64,
    uncertainty: f64,
}

/// Decode a field only to check its form, keeping no value
///
/// Some published fields must be well formed although no rule reads them, so
/// the projection records that the form was checked instead of holding a value
fn decode_form<'de, D, T>(deserializer: D) -> Result<PhantomData<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(|_| PhantomData)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultEvidenceProjection {
    #[serde(deserialize_with = "decode_form")]
    score_contract: PhantomData<Sha256Digest>,
    #[serde(deserialize_with = "decode_form")]
    membership_digest: PhantomData<Sha256Digest>,
    source_exposures: Vec<SourceExposureProjection>,
    update_points: Vec<u64>,
    probe_points: Vec<u64>,
    #[serde(default)]
    evidence_artifact: Option<EvidenceArtifactProjection>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceExposureProjection {
    source_digest: Sha256Digest,
    samples: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceArtifactProjection {
    schema: EvidenceSchemaProjection,
    path: String,
    #[serde(deserialize_with = "decode_form")]
    digest: PhantomData<Sha256Digest>,
    size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceSchemaProjection {
    id: String,
    version: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidationReceiptProjection {
    validator_id: String,
    validator_version: String,
    #[serde(deserialize_with = "decode_form")]
    evidence_digest: PhantomData<Sha256Digest>,
    #[serde(deserialize_with = "decode_form")]
    validated_at: PhantomData<u64>,
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use serde_json::{Value, json};
    use tempfile::TempDir;

    use crate::digest::Sha256Digest;
    use crate::domain::{ExitReason, TaskState};

    use super::{
        AttemptBinding, PublishedRecoveryGeneration, WatchObservation, WatcherAttention,
        WatcherError, find_completed_result, find_new, observe_release, snapshot,
    };

    fn expected_binding() -> AttemptBinding {
        AttemptBinding {
            campaign_id: "campaign-a".into(),
            campaign_revision_id: "revision-a".into(),
            task_id: "trainer-task-a".into(),
            attempt_id: "attempt-a".into(),
            attempt_number: 1,
            ownership_token: "owner-a".into(),
        }
    }

    fn foreign_binding() -> AttemptBinding {
        AttemptBinding {
            campaign_id: "campaign-b".into(),
            campaign_revision_id: "revision-b".into(),
            task_id: "trainer-task-b".into(),
            attempt_id: "attempt-b".into(),
            attempt_number: 1,
            ownership_token: "owner-b".into(),
        }
    }

    fn runtime_root(temp: &TempDir) -> PathBuf {
        temp.path().join("runtime")
    }

    fn published_root(runtime_root: &Path) -> PathBuf {
        let published_root = runtime_root.join("published");
        fs::create_dir_all(&published_root).unwrap();
        published_root
    }

    fn record_value(
        request_binding: &AttemptBinding,
        recovery_binding: &AttemptBinding,
        generation_id: &str,
        update_count: Value,
    ) -> Value {
        const CHECKPOINT_BYTES: &[u8] = b"checkpoint bytes";
        const RECOVERY_BYTES: &[u8] = b"recovery state";
        let request = request_value(request_binding);
        let request_bytes = serde_json::to_vec(&request).unwrap();
        let worker = worker_identity();

        json!({
            "schema": "trainer-direct-recovery-v1",
            "request": request,
            "recovery": {
                "schema_version": 1,
                "binding": recovery_binding,
                "generation_id": generation_id,
                "compatibility": {
                    "schema_id": "speakrs-long-run-train-v1",
                    "config_digest": super::configuration_digest(&request_bytes).unwrap(),
                    "source_digest": "b".repeat(64),
                },
                "worker": worker,
                "inventory": {
                    "schema_version": 1,
                    "binding": recovery_binding,
                    "entries": [
                        {
                            "path": "checkpoint.bin",
                            "kind": "file",
                            "size": CHECKPOINT_BYTES.len(),
                            "digest": digest(CHECKPOINT_BYTES),
                        },
                        {
                            "path": "recovery.json",
                            "kind": "file",
                            "size": RECOVERY_BYTES.len(),
                            "digest": digest(RECOVERY_BYTES),
                        },
                    ],
                },
                "state_digest": digest(RECOVERY_BYTES),
                "position": {
                    "update_count": update_count,
                    "exposure_count": 0,
                    "sample_cursor_digest": "1".repeat(64),
                    "random_state_digest": "2".repeat(64),
                },
                "cause": "periodic_checkpoint",
            },
        })
    }

    fn write_generation(
        published_root: &Path,
        directory_name: &str,
        generation_id: &str,
        update_count: u64,
        request_binding: &AttemptBinding,
        recovery_binding: &AttemptBinding,
    ) -> PathBuf {
        write_record(
            published_root,
            directory_name,
            &record_value(
                request_binding,
                recovery_binding,
                generation_id,
                json!(update_count),
            ),
        )
    }

    fn write_record(published_root: &Path, directory_name: &str, record: &Value) -> PathBuf {
        let directory = published_root.join(directory_name);
        fs::create_dir(&directory).unwrap();
        let request = record.get("request").unwrap();
        let binding: AttemptBinding =
            serde_json::from_value(request.get("binding").unwrap().clone()).unwrap();
        let attempt_path = published_root
            .parent()
            .unwrap()
            .join("attempts")
            .join(&binding.attempt_id);
        fs::create_dir_all(&attempt_path).unwrap();
        fs::write(
            attempt_path.join(super::REQUEST_FILE),
            serde_json::to_vec(request).unwrap(),
        )
        .unwrap();
        fs::write(directory.join("checkpoint.bin"), b"checkpoint bytes").unwrap();
        fs::write(directory.join("recovery.json"), b"recovery state").unwrap();
        fs::write(
            directory.join("segment-record.json"),
            serde_json::to_vec(record).unwrap(),
        )
        .unwrap();
        directory
    }

    fn assert_generation(
        result: PublishedRecoveryGeneration,
        path: &Path,
        generation_id: &str,
        update_count: u64,
    ) {
        assert_eq!(result.path, path);
        assert_eq!(result.generation_id, generation_id);
        assert_eq!(result.committed_update_count, update_count);
    }

    fn digest(bytes: &[u8]) -> String {
        Sha256Digest::of(bytes).to_hex()
    }

    fn worker_identity() -> Value {
        json!({
            "worker_id": "trainer-worker",
            "executable_digest": "a".repeat(64),
            "source_digest": "b".repeat(64),
            "environment_digest": "c".repeat(64),
        })
    }

    fn output_declarations() -> Value {
        json!([{"path": "checkpoint.bin", "kind": "file"}])
    }

    fn request_value(binding: &AttemptBinding) -> Value {
        json!({
            "schema_version": 1,
            "binding": binding,
            "adapter": {},
            "start_mode": {},
            "payload": {},
            "input_view": {
                "schema_version": 1,
                "revision_id": binding.campaign_revision_id,
                "root": "/trainer/input",
            },
            "inputs": [],
            "expected_outputs": output_declarations(),
            "resources": {},
            "worker": worker_identity(),
        })
    }

    fn completed_terminal_value(binding: &AttemptBinding, output: &[u8]) -> Value {
        let worker = worker_identity();
        let output_digest = digest(output);
        json!({
            "kind": "completed",
            "terminal": {
                "schema_version": 1,
                "binding": binding,
                "outcome": {
                    "kind": "completed",
                    "result": {
                        "schema_version": 1,
                        "binding": binding,
                        "worker": worker,
                        "config_digest": super::configuration_digest(
                            &serde_json::to_vec(&request_value(binding)).unwrap(),
                        )
                        .unwrap(),
                        "expected_outputs": output_declarations(),
                        "outputs": {
                            "schema_version": 1,
                            "binding": binding,
                            "entries": [{
                                "path": "checkpoint.bin",
                                "kind": "file",
                                "size": output.len(),
                                "digest": output_digest,
                            }],
                        },
                        "metrics": [],
                        "evidence": {
                            "score_contract": "e".repeat(64),
                            "membership_digest": "f".repeat(64),
                            "source_exposures": [{
                                "source_digest": "1".repeat(64),
                                "samples": 1,
                            }],
                            "update_points": [1],
                            "probe_points": [],
                        },
                        "validation": {
                            "validator_id": "trainer-validator",
                            "validator_version": "1.0",
                            "evidence_digest": "2".repeat(64),
                            "validated_at": 1,
                        },
                    },
                },
            },
        })
    }

    pub(crate) fn write_request_for_test(runtime_root: &Path, binding: &AttemptBinding) -> PathBuf {
        let attempt_path = runtime_root.join("attempts").join(&binding.attempt_id);
        fs::create_dir_all(&attempt_path).unwrap();
        let request_path = attempt_path.join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec(&request_value(binding)).unwrap(),
        )
        .unwrap();
        request_path
    }

    pub(crate) fn write_generation_for_test(
        runtime_root: &Path,
        binding: &AttemptBinding,
        generation_id: &str,
        update_count: u64,
    ) -> PathBuf {
        write_generation(
            &published_root(runtime_root),
            generation_id,
            generation_id,
            update_count,
            binding,
            binding,
        )
    }

    pub(crate) fn write_completed_result_for_test(
        runtime_root: &Path,
        binding: &AttemptBinding,
    ) -> PathBuf {
        let published_root = published_root(runtime_root);
        let publication_path = published_root.join(format!("result-{}", binding.attempt_id));
        fs::create_dir(&publication_path).unwrap();
        let output = b"checkpoint bytes";
        fs::write(publication_path.join("checkpoint.bin"), output).unwrap();

        let attempt_path = runtime_root.join("attempts").join(&binding.attempt_id);
        fs::create_dir_all(&attempt_path).unwrap();
        let request_path = attempt_path.join("request.json");
        if !request_path.exists() {
            fs::write(
                &request_path,
                serde_json::to_vec(&request_value(binding)).unwrap(),
            )
            .unwrap();
        }
        fs::write(
            &request_path,
            serde_json::to_vec(&request_value(binding)).unwrap(),
        )
        .unwrap();
        let terminal = serde_json::to_vec(&completed_terminal_value(binding, output)).unwrap();
        fs::write(attempt_path.join("terminal.json"), &terminal).unwrap();
        fs::write(publication_path.join("segment-record.json"), terminal).unwrap();
        publication_path
    }

    fn write_completed_result(runtime_root: &Path, binding: &AttemptBinding) -> PathBuf {
        write_completed_result_for_test(runtime_root, binding)
    }

    #[test]
    fn a_final_result_wins_when_a_new_checkpoint_appears_at_the_same_time() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        let published_root = published_root(&runtime_root);
        write_generation(
            &published_root,
            "generation-a",
            "generation-a",
            41,
            &expected,
            &expected,
        );
        let result_path = write_completed_result(&runtime_root, &expected);

        let observation = observe_release(
            &runtime_root,
            &expected,
            &baseline,
            &TaskState::Running { pid: Some(42) },
        )
        .unwrap();

        assert!(matches!(
            observation,
            WatchObservation::CompletedResultAwaitingTaskExit {
                result,
                new_checkpoint: Some(PublishedRecoveryGeneration {
                    generation_id,
                    ..
                }),
            } if result.publication_path == result_path && generation_id == "generation-a"
        ));
    }

    #[test]
    fn a_final_result_before_task_exit_waits_without_requesting_a_stop() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        let result_path = write_completed_result(&runtime_root, &expected);

        let observation = observe_release(
            &runtime_root,
            &expected,
            &baseline,
            &TaskState::Running { pid: Some(42) },
        )
        .unwrap();

        assert!(matches!(
            observation,
            WatchObservation::CompletedResultAwaitingTaskExit {
                result,
                new_checkpoint: None,
            } if result.publication_path == result_path
        ));
    }

    #[test]
    fn a_lost_task_needs_attention_even_without_publications() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();

        let observation =
            observe_release(&runtime_root, &expected, &baseline, &TaskState::Lost).unwrap();

        assert!(matches!(
            observation,
            WatchObservation::Attention(WatcherAttention::LostTask { publications })
                if publications.new_checkpoint.is_none()
                    && publications.completed_result.is_none()
        ));
    }

    #[test]
    fn a_failed_task_needs_attention_even_without_publications() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();

        let observation = observe_release(
            &runtime_root,
            &expected,
            &baseline,
            &TaskState::Finished {
                reason: ExitReason::Exit { code: 7 },
            },
        )
        .unwrap();

        assert!(matches!(
            observation,
            WatchObservation::Attention(WatcherAttention::FailedTask {
                reason: ExitReason::Exit { code: 7 },
                publications,
            }) if publications.new_checkpoint.is_none()
                && publications.completed_result.is_none()
        ));
    }

    #[test]
    fn foreign_and_baseline_checkpoints_do_not_stop_the_running_task() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let published_root = published_root(&runtime_root);
        let expected = expected_binding();
        write_generation(
            &published_root,
            "generation-baseline",
            "generation-baseline",
            40,
            &expected,
            &expected,
        );
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        let foreign = foreign_binding();
        write_generation(
            &published_root,
            "generation-foreign",
            "generation-foreign",
            41,
            &foreign,
            &foreign,
        );

        let observation = observe_release(
            &runtime_root,
            &expected,
            &baseline,
            &TaskState::Running { pid: Some(42) },
        )
        .unwrap();

        assert_eq!(observation, WatchObservation::WaitingForCheckpoint);
    }

    #[test]
    fn a_new_checkpoint_while_running_is_only_a_stop_request_candidate() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        let path = write_generation(
            &published_root(&runtime_root),
            "generation-a",
            "generation-a",
            41,
            &expected,
            &expected,
        );

        let observation = observe_release(
            &runtime_root,
            &expected,
            &baseline,
            &TaskState::Running { pid: Some(42) },
        )
        .unwrap();

        assert!(matches!(
            observation,
            WatchObservation::StopRequestCandidate { checkpoint }
                if checkpoint.path == path
        ));
    }

    #[test]
    fn a_successful_task_and_final_result_are_only_an_already_completed_candidate() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        let result_path = write_completed_result(&runtime_root, &expected);

        let observation = observe_release(
            &runtime_root,
            &expected,
            &baseline,
            &TaskState::Finished {
                reason: ExitReason::Exit { code: 0 },
            },
        )
        .unwrap();

        assert!(matches!(
            observation,
            WatchObservation::AlreadyCompletedCandidate {
                result,
                new_checkpoint: None,
            } if result.publication_path == result_path
        ));
    }

    #[test]
    fn a_checkpoint_before_the_task_starts_needs_attention() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        write_generation(
            &published_root(&runtime_root),
            "generation-a",
            "generation-a",
            41,
            &expected,
            &expected,
        );

        let observation =
            observe_release(&runtime_root, &expected, &baseline, &TaskState::Queued).unwrap();

        assert!(matches!(
            observation,
            WatchObservation::Attention(WatcherAttention::PublicationBeforeTaskStart {
                publications,
            }) if publications.new_checkpoint.is_some()
                && publications.completed_result.is_none()
        ));
    }

    #[test]
    fn a_successful_task_and_checkpoint_without_a_result_need_attention() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        write_generation(
            &published_root(&runtime_root),
            "generation-a",
            "generation-a",
            41,
            &expected,
            &expected,
        );

        let observation = observe_release(
            &runtime_root,
            &expected,
            &baseline,
            &TaskState::Finished {
                reason: ExitReason::Exit { code: 0 },
            },
        )
        .unwrap();

        assert!(matches!(
            observation,
            WatchObservation::Attention(WatcherAttention::SuccessfulTaskWithoutFinalResult {
                new_checkpoint: Some(PublishedRecoveryGeneration { .. }),
            })
        ));
    }

    #[test]
    fn snapshot_excludes_generations_that_were_already_published() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let published_root = published_root(&runtime_root);
        let expected = expected_binding();
        write_generation(
            &published_root,
            "generation-a",
            "generation-a",
            40,
            &expected,
            &expected,
        );

        let baseline = snapshot(&runtime_root, &expected).unwrap();

        assert!(
            find_new(&runtime_root, &expected, &baseline)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn serialized_snapshot_restores_the_exact_restart_baseline() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let published_root = published_root(&runtime_root);
        let expected = expected_binding();
        write_generation(
            &published_root,
            "generation-a",
            "generation-a",
            40,
            &expected,
            &expected,
        );

        let baseline = snapshot(&runtime_root, &expected).unwrap();
        let persisted = serde_json::to_vec(&baseline).unwrap();
        let restored = serde_json::from_slice(&persisted).unwrap();
        assert_eq!(restored, baseline);
        let new_path = write_generation(
            &published_root,
            "generation-b",
            "generation-b",
            41,
            &expected,
            &expected,
        );

        assert_eq!(
            find_new(&runtime_root, &expected, &restored)
                .unwrap()
                .unwrap()
                .path,
            new_path
        );
    }

    #[test]
    fn serialized_snapshot_rejects_duplicate_and_untrusted_generation_ids() {
        let binding = expected_binding();
        let duplicate = json!({
            "schema": "trainer-recovery-snapshot-v1",
            "binding": binding,
            "generation_ids": ["generation-a", "generation-a"],
        });
        assert!(serde_json::from_value::<super::RecoverySnapshot>(duplicate).is_err());

        let untrusted = json!({
            "schema": "trainer-recovery-snapshot-v1",
            "binding": expected_binding(),
            "generation_ids": ["../outside"],
        });
        assert!(serde_json::from_value::<super::RecoverySnapshot>(untrusted).is_err());
    }

    #[test]
    fn completed_result_requires_matching_attempt_terminal_evidence_and_outputs() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let publication_path = write_completed_result(&runtime_root, &expected);

        let result = find_completed_result(&runtime_root, &expected)
            .unwrap()
            .unwrap();

        assert_eq!(result.publication_path, publication_path);
        assert_eq!(
            result.terminal_path,
            runtime_root.join("attempts/attempt-a/terminal.json")
        );
        assert_eq!(result.binding, expected);
        assert_eq!(result.output_paths, ["checkpoint.bin"]);
    }

    #[test]
    fn another_attempts_result_is_not_accepted() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let foreign = AttemptBinding {
            attempt_id: "attempt-b".into(),
            ..expected_binding()
        };
        write_completed_result(&runtime_root, &foreign);

        assert!(
            find_completed_result(&runtime_root, &expected_binding())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn completed_result_with_wrong_attempt_binding_requires_attention() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let publication_path = write_completed_result(&runtime_root, &expected);
        let foreign = foreign_binding();
        let terminal =
            serde_json::to_vec(&completed_terminal_value(&foreign, b"checkpoint bytes")).unwrap();
        fs::write(publication_path.join("segment-record.json"), &terminal).unwrap();
        fs::write(
            runtime_root.join("attempts/attempt-a/terminal.json"),
            terminal,
        )
        .unwrap();

        assert!(matches!(
            find_completed_result(&runtime_root, &expected),
            Err(WatcherError::MalformedPublication { .. })
        ));
    }

    #[test]
    fn completed_result_with_malformed_output_requires_attention() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        let publication_path = write_completed_result(&runtime_root, &expected);
        fs::write(publication_path.join("checkpoint.bin"), b"changed bytes").unwrap();

        assert!(matches!(
            find_completed_result(&runtime_root, &expected),
            Err(WatcherError::MalformedPublication { .. })
        ));
    }

    #[test]
    fn symlinked_terminal_evidence_requires_attention() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let expected = expected_binding();
        write_completed_result(&runtime_root, &expected);
        let terminal_path = runtime_root.join("attempts/attempt-a/terminal.json");
        let external_terminal = temp.path().join("external-terminal.json");
        fs::rename(&terminal_path, &external_terminal).unwrap();
        symlink(&external_terminal, terminal_path).unwrap();

        assert!(matches!(
            find_completed_result(&runtime_root, &expected),
            Err(WatcherError::Symlink { .. })
        ));
    }

    #[test]
    fn find_new_returns_a_complete_matching_publication() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let published_root = published_root(&runtime_root);
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        let path = write_generation(
            &published_root,
            "generation-a",
            "generation-a",
            41,
            &expected,
            &expected,
        );

        let result = find_new(&runtime_root, &expected, &baseline)
            .unwrap()
            .unwrap();

        assert_generation(result, &path, "generation-a", 41);
    }

    #[test]
    fn foreign_attempt_publications_are_not_in_the_baseline_or_new_result() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let published_root = published_root(&runtime_root);
        let expected = expected_binding();
        let foreign = foreign_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        write_generation(
            &published_root,
            "generation-foreign-request",
            "generation-foreign-request",
            42,
            &foreign,
            &expected,
        );
        write_generation(
            &published_root,
            "generation-foreign-recovery",
            "generation-foreign-recovery",
            43,
            &expected,
            &foreign,
        );

        assert!(
            find_new(&runtime_root, &expected, &baseline)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn trainer_staging_and_terminal_result_publications_are_ignored() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let published_root = published_root(&runtime_root);
        fs::create_dir(published_root.join(".publish-staging")).unwrap();
        fs::write(
            published_root.join(".publish-staging/segment-record.json"),
            b"not a recovery record",
        )
        .unwrap();
        fs::create_dir(published_root.join("result-attempt-a")).unwrap();
        fs::write(
            published_root.join("result-attempt-a/segment-record.json"),
            b"not a recovery record",
        )
        .unwrap();
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();

        assert!(
            find_new(&runtime_root, &expected, &baseline)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn new_generation_at_the_baseline_update_count_is_still_new() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let published_root = published_root(&runtime_root);
        let expected = expected_binding();
        write_generation(
            &published_root,
            "generation-a",
            "generation-a",
            50,
            &expected,
            &expected,
        );
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        let path = write_generation(
            &published_root,
            "generation-b",
            "generation-b",
            50,
            &expected,
            &expected,
        );

        let result = find_new(&runtime_root, &expected, &baseline)
            .unwrap()
            .unwrap();

        assert_generation(result, &path, "generation-b", 50);
    }

    #[test]
    fn malformed_matching_publication_requires_attention() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let published_root = published_root(&runtime_root);
        let expected = expected_binding();
        let baseline = snapshot(&runtime_root, &expected).unwrap();
        write_record(
            &published_root,
            "generation-a",
            &record_value(
                &expected,
                &expected,
                "generation-a",
                json!("not-an-update-count"),
            ),
        );

        assert!(matches!(
            find_new(&runtime_root, &expected, &baseline),
            Err(WatcherError::MalformedPublication { .. })
        ));
    }

    #[test]
    fn symlinked_publication_root_requires_attention() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        fs::create_dir_all(&runtime_root).unwrap();
        let external_root = temp.path().join("external-published");
        fs::create_dir(&external_root).unwrap();
        symlink(&external_root, runtime_root.join("published")).unwrap();

        assert!(matches!(
            snapshot(&runtime_root, &expected_binding()),
            Err(WatcherError::Symlink { .. })
        ));
    }

    #[test]
    fn symlinked_generation_directory_requires_attention() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let published_root = published_root(&runtime_root);
        let external_generation = temp.path().join("external-generation");
        fs::create_dir(&external_generation).unwrap();
        symlink(&external_generation, published_root.join("generation-a")).unwrap();

        assert!(matches!(
            snapshot(&runtime_root, &expected_binding()),
            Err(WatcherError::Symlink { .. })
        ));
    }

    #[test]
    fn symlinked_record_file_requires_attention() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let runtime_root = runtime_root(&temp);
        let published_root = published_root(&runtime_root);
        let expected = expected_binding();
        let generation = published_root.join("generation-a");
        fs::create_dir(&generation).unwrap();
        let external_record = temp.path().join("external-record.json");
        fs::write(
            &external_record,
            serde_json::to_vec(&record_value(
                &expected,
                &expected,
                "generation-a",
                json!(60),
            ))
            .unwrap(),
        )
        .unwrap();
        symlink(external_record, generation.join("segment-record.json")).unwrap();

        assert!(matches!(
            snapshot(&runtime_root, &expected),
            Err(WatcherError::Symlink { .. })
        ));
    }

    #[test]
    fn configuration_digest_hashes_the_exact_member_bytes() {
        let request = br#"{ "schema_version" : 1, "resources":{"accelerator" :"cuda"},
            "adapter" : {"kind":"speakrs"} , "start_mode":{ "kind": "fresh" },
            "payload": {"epochs": 4.0, "rate": 1e-3},
            "input_view": {"root": "/in", "revision_id" : "revision-a", "schema_version": 1},
            "inputs": [ ], "expected_outputs": [{"path":"a.bin","kind":"file"}] }"#;
        let expected = concat!(
            r#"[{"kind":"speakrs"},{ "kind": "fresh" },{"epochs": 4.0, "rate": 1e-3},"#,
            r#"{"schema_version":1,"revision_id":"revision-a"},[ ],"#,
            r#"[{"path":"a.bin","kind":"file"}],{"accelerator" :"cuda"}]"#,
        );

        assert_eq!(
            super::configuration_digest(request).unwrap(),
            Sha256Digest::of(expected)
        );
    }

    #[test]
    fn configuration_digest_refuses_duplicate_members() {
        let request = request_value(&expected_binding());
        let document = serde_json::to_string(&request).unwrap();
        let duplicate_top = document.replacen('{', r#"{"payload":{},"#, 1);
        let duplicate_view = document.replacen(
            r#""input_view":{"#,
            r#""input_view":{"revision_id":"other","#,
            1,
        );

        assert!(super::configuration_digest(document.as_bytes()).is_ok());
        assert!(super::configuration_digest(duplicate_top.as_bytes()).is_err());
        assert!(super::configuration_digest(duplicate_view.as_bytes()).is_err());
    }
}
