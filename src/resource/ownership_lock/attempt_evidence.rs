//! Point-in-time evidence that one live direct-segment trainer attempt holds its lock

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::fd::AsFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::SEGMENT_LOCK_FILE;
use super::lock_probe::{
    OwnershipLockIdentity, OwnershipLockProbe, OwnershipLockProbeError,
    probe_segment_ownership_lock,
};
use crate::digest::Sha256Digest;
use crate::resource::trainer_publication::{
    AttemptBinding, AttemptRequestValidationError, WatcherError, validate_attempt_request,
};

/// Why a trainer attempt path is not safe to use as registration evidence
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrainerAttemptUnsafePathReason {
    /// A path component is a symbolic link
    SymbolicLink,
    /// A path expected to be a directory is not a directory
    NotDirectory,
    /// A path expected to be a regular file is not a regular file
    NotRegularFile,
}

/// A typed failure while collecting read-only trainer attempt evidence
#[derive(Debug, Error)]
pub enum TrainerAttemptEvidenceError {
    /// A required runtime, attempt, request, or lock path is missing
    #[error("trainer attempt evidence path is missing: {path}")]
    Missing {
        /// Missing path
        path: PathBuf,
    },
    /// The supplied runtime path is not its exact canonical absolute path
    #[error("runtime root is not canonical: supplied {provided}, canonical {canonical}")]
    NonCanonicalRuntimeRoot {
        /// Runtime root supplied by the caller
        provided: PathBuf,
        /// Canonical runtime root observed on disk
        canonical: PathBuf,
    },
    /// A path has an unsafe filesystem type
    #[error("unsafe trainer attempt path {path}: {reason:?}")]
    UnsafePath {
        /// Unsafe path
        path: PathBuf,
        /// Filesystem type that makes the path unsafe
        reason: TrainerAttemptUnsafePathReason,
    },
    /// A filesystem operation failed while collecting evidence
    #[error("cannot inspect trainer attempt evidence path {path}: {source}")]
    Io {
        /// Path involved in the failed operation
        path: PathBuf,
        /// Underlying filesystem error
        #[source]
        source: io::Error,
    },
    /// The expected trainer binding is invalid
    #[error("invalid expected trainer attempt binding: {reason}")]
    InvalidExpectedBinding {
        /// Binding validation reason
        reason: &'static str,
    },
    /// The request file does not match the maintained trainer request contract
    #[error("malformed trainer request at {path}: {reason}")]
    MalformedRequest {
        /// Exact request path
        path: PathBuf,
        /// Projection or schema validation reason
        reason: String,
    },
    /// The request is valid but belongs to a different trainer attempt
    #[error("trainer request binding at {path} does not match the expected attempt")]
    BindingMismatch {
        /// Exact request path
        path: PathBuf,
        /// Expected attempt identity
        expected: Box<AttemptBinding>,
        /// Attempt identity persisted in the request
        observed: Box<AttemptBinding>,
    },
    /// The request changed while evidence was being collected
    #[error("trainer request changed during evidence collection: {path}")]
    RequestChanged {
        /// Exact request path
        path: PathBuf,
    },
    /// The ownership lock was free while evidence was collected
    #[error("trainer ownership lock is not held at probe time: {path}")]
    OwnershipLockFree {
        /// Exact runtime lock path
        path: PathBuf,
    },
    /// The lock path changed after the held-lock observation
    #[error("trainer ownership lock changed during evidence collection: {path}")]
    OwnershipLockChanged {
        /// Exact runtime lock path
        path: PathBuf,
    },
    /// The existing ownership-lock probe could not establish a safe result
    #[error("cannot verify trainer ownership lock: {source}")]
    OwnershipLock {
        /// Typed error from the existing lock probe
        #[source]
        source: OwnershipLockProbeError,
    },
}

/// SHA-256 digest of the exact bytes in one validated trainer request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TrainerRequestDigest(Sha256Digest);

impl TrainerRequestDigest {
    /// Hash the exact persisted request bytes
    pub(crate) fn of(request_bytes: &[u8]) -> Self {
        Self(Sha256Digest::of(request_bytes))
    }

    /// Return the digest as 64 lowercase hexadecimal characters
    #[must_use]
    pub fn to_hex(self) -> String {
        self.0.to_hex()
    }

    pub(crate) fn from_hex(value: &str) -> Option<Self> {
        Sha256Digest::from_hex(value).ok().map(Self)
    }
}

/// Point-in-time evidence for registering one active direct-segment trainer attempt
///
/// This binds the canonical runtime root, strict trainer `AttemptBinding`, exact
/// request bytes, and the device/inode of a lock observed as held. It does not bind
/// the exact Homebased `TaskId` or normalized command spec; the later authority-owned
/// registration must validate both. This evidence is not proof that the GPU has
/// been released and must not be used to mark a resource free. The lock observation
/// is point-in-time; the lock may be released after this value is returned
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedTrainerAttempt {
    canonical_runtime_root: PathBuf,
    binding: AttemptBinding,
    request_digest: TrainerRequestDigest,
    ownership_lock_identity: OwnershipLockIdentity,
}

impl VerifiedTrainerAttempt {
    /// Return the canonical direct-segment runtime root
    #[must_use]
    pub fn canonical_runtime_root(&self) -> &Path {
        &self.canonical_runtime_root
    }

    /// Return the exact trainer attempt binding from the validated request
    #[must_use]
    pub const fn binding(&self) -> &AttemptBinding {
        &self.binding
    }

    /// Return the digest of the exact persisted request bytes
    #[must_use]
    pub const fn request_digest(&self) -> TrainerRequestDigest {
        self.request_digest
    }

    /// Return the device and inode of the lock observed as held
    #[must_use]
    pub const fn ownership_lock_identity(&self) -> OwnershipLockIdentity {
        self.ownership_lock_identity
    }

    pub(crate) fn from_persisted(
        canonical_runtime_root: PathBuf,
        binding: AttemptBinding,
        request_digest: TrainerRequestDigest,
        ownership_lock_identity: OwnershipLockIdentity,
    ) -> Result<Self, &'static str> {
        if !canonical_runtime_root.is_absolute()
            || canonical_runtime_root.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::CurDir | std::path::Component::ParentDir
                )
            })
        {
            return Err("saved trainer runtime root is not a canonical absolute path");
        }
        binding
            .validate()
            .map_err(|_| "saved trainer attempt binding is invalid")?;

        Ok(Self {
            canonical_runtime_root,
            binding,
            request_digest,
            ownership_lock_identity,
        })
    }
}

/// Build point-in-time registration evidence for one existing active trainer attempt
///
/// The runtime root must be an absolute, exact canonical path. The builder reads
/// only `attempts/<attempt_id>/request.json` and probes the existing `.segment.lock`
/// It does not create or write trainer artifacts. It returns evidence only if the
/// request is stable and valid and the exact lock is held at probe time. The result
/// does not prove GPU release. Later resource-aware registration must also bind the
/// exact Homebased `TaskId` and normalized command spec. The lock API reports
/// contention but does not identify its owner, so the caller must not hold this lock
/// itself. The intended lock holder is the trainer process
pub fn build_trainer_attempt_registration_evidence(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
) -> Result<VerifiedTrainerAttempt, TrainerAttemptEvidenceError> {
    build_trainer_attempt_registration_evidence_inner(runtime_root, expected_binding, || {})
}

fn build_trainer_attempt_registration_evidence_inner<F>(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
    after_request_read: F,
) -> Result<VerifiedTrainerAttempt, TrainerAttemptEvidenceError>
where
    F: FnOnce(),
{
    expected_binding.validate().map_err(|error| match error {
        WatcherError::InvalidAttemptBinding { reason } => {
            TrainerAttemptEvidenceError::InvalidExpectedBinding { reason }
        }
        _ => TrainerAttemptEvidenceError::InvalidExpectedBinding {
            reason: "trainer attempt binding is invalid",
        },
    })?;

    let (canonical_runtime_root, root_directory, root_identity) =
        open_canonical_runtime_root(runtime_root)?;
    let attempts_path = canonical_runtime_root.join("attempts");
    let attempts_directory = open_directory_at(&root_directory, "attempts", &attempts_path)?;
    let attempt_path = attempts_path.join(&expected_binding.attempt_id);
    let attempt_directory = open_directory_at(
        &attempts_directory,
        &expected_binding.attempt_id,
        &attempt_path,
    )?;
    let request_path = attempt_path.join("request.json");
    let request_file = open_request_at(&attempt_directory, &request_path)?;
    let request_state = stable_file_state(&request_file, &request_path)?;

    let mut request_bytes = Vec::new();
    let mut reader = &request_file;
    reader
        .read_to_end(&mut request_bytes)
        .map_err(|source| TrainerAttemptEvidenceError::Io {
            path: request_path.clone(),
            source,
        })?;
    if stable_file_state(&request_file, &request_path)? != request_state {
        return Err(TrainerAttemptEvidenceError::RequestChanged { path: request_path });
    }

    after_request_read();
    verify_request_path(&attempt_directory, &request_path, request_state)?;

    match validate_attempt_request(&request_bytes, expected_binding) {
        Ok(()) => {}
        Err(AttemptRequestValidationError::InvalidExpectedBinding(reason)) => {
            return Err(TrainerAttemptEvidenceError::InvalidExpectedBinding { reason });
        }
        Err(AttemptRequestValidationError::Malformed(reason)) => {
            return Err(TrainerAttemptEvidenceError::MalformedRequest {
                path: request_path,
                reason,
            });
        }
        Err(AttemptRequestValidationError::BindingMismatch(observed)) => {
            return Err(TrainerAttemptEvidenceError::BindingMismatch {
                path: request_path,
                expected: Box::new(expected_binding.clone()),
                observed,
            });
        }
    }

    let lock_path = canonical_runtime_root.join(SEGMENT_LOCK_FILE);
    let lock_metadata = fs::symlink_metadata(&lock_path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            return TrainerAttemptEvidenceError::Missing {
                path: lock_path.clone(),
            };
        }

        TrainerAttemptEvidenceError::Io {
            path: lock_path.clone(),
            source,
        }
    })?;
    if lock_metadata.file_type().is_symlink() {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: lock_path,
            reason: TrainerAttemptUnsafePathReason::SymbolicLink,
        });
    }
    if !lock_metadata.is_file() {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: lock_path,
            reason: TrainerAttemptUnsafePathReason::NotRegularFile,
        });
    }

    let lock_identity = OwnershipLockIdentity::from_metadata(&lock_metadata);
    match probe_segment_ownership_lock(&canonical_runtime_root, lock_identity) {
        OwnershipLockProbe::OwnershipHeld => {}
        OwnershipLockProbe::ExactOwnershipReleased(guard) => {
            drop(guard);
            return Err(TrainerAttemptEvidenceError::OwnershipLockFree { path: lock_path });
        }
        OwnershipLockProbe::Attention(source) => {
            return Err(TrainerAttemptEvidenceError::OwnershipLock { source });
        }
    }

    verify_request_path(&attempt_directory, &request_path, request_state)?;
    verify_directory_name(
        &root_directory,
        "attempts",
        &attempts_path,
        &attempts_directory,
    )?;
    verify_directory_name(
        &attempts_directory,
        &expected_binding.attempt_id,
        &attempt_path,
        &attempt_directory,
    )?;
    verify_runtime_root_identity(&canonical_runtime_root, &root_directory, root_identity)?;
    verify_lock_path_identity(&lock_path, lock_identity)?;

    Ok(VerifiedTrainerAttempt {
        canonical_runtime_root,
        binding: expected_binding.clone(),
        request_digest: TrainerRequestDigest::of(&request_bytes),
        ownership_lock_identity: lock_identity,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StableFileState {
    identity: OwnershipLockIdentity,
    length: u64,
    mode: u32,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

fn open_canonical_runtime_root(
    runtime_root: &Path,
) -> Result<(PathBuf, File, OwnershipLockIdentity), TrainerAttemptEvidenceError> {
    if !runtime_root.is_absolute() {
        let canonical_runtime_root = runtime_root.canonicalize().map_err(|source| {
            if source.kind() == io::ErrorKind::NotFound {
                return TrainerAttemptEvidenceError::Missing {
                    path: runtime_root.to_path_buf(),
                };
            }

            TrainerAttemptEvidenceError::Io {
                path: runtime_root.to_path_buf(),
                source,
            }
        })?;
        return Err(TrainerAttemptEvidenceError::NonCanonicalRuntimeRoot {
            provided: runtime_root.to_path_buf(),
            canonical: canonical_runtime_root,
        });
    }

    let root_metadata = fs::symlink_metadata(runtime_root).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            return TrainerAttemptEvidenceError::Missing {
                path: runtime_root.to_path_buf(),
            };
        }

        TrainerAttemptEvidenceError::Io {
            path: runtime_root.to_path_buf(),
            source,
        }
    })?;
    if root_metadata.file_type().is_symlink() {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: runtime_root.to_path_buf(),
            reason: TrainerAttemptUnsafePathReason::SymbolicLink,
        });
    }
    if !root_metadata.is_dir() {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: runtime_root.to_path_buf(),
            reason: TrainerAttemptUnsafePathReason::NotDirectory,
        });
    }

    let canonical_runtime_root =
        runtime_root
            .canonicalize()
            .map_err(|source| TrainerAttemptEvidenceError::Io {
                path: runtime_root.to_path_buf(),
                source,
            })?;
    if canonical_runtime_root.as_os_str() != runtime_root.as_os_str() {
        return Err(TrainerAttemptEvidenceError::NonCanonicalRuntimeRoot {
            provided: runtime_root.to_path_buf(),
            canonical: canonical_runtime_root,
        });
    }

    let root_directory = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW)
        .open(runtime_root)
        .map_err(|source| TrainerAttemptEvidenceError::Io {
            path: runtime_root.to_path_buf(),
            source,
        })?;
    let opened_metadata =
        root_directory
            .metadata()
            .map_err(|source| TrainerAttemptEvidenceError::Io {
                path: runtime_root.to_path_buf(),
                source,
            })?;
    let root_identity = OwnershipLockIdentity::from_metadata(&root_metadata);
    if OwnershipLockIdentity::from_metadata(&opened_metadata) != root_identity {
        return Err(TrainerAttemptEvidenceError::RequestChanged {
            path: runtime_root.to_path_buf(),
        });
    }

    Ok((canonical_runtime_root, root_directory, root_identity))
}

fn open_directory_at(
    parent: &File,
    name: &str,
    path: &Path,
) -> Result<File, TrainerAttemptEvidenceError> {
    let descriptor = nix::fcntl::openat(
        parent.as_fd(),
        name,
        nix::fcntl::OFlag::O_CLOEXEC
            | nix::fcntl::OFlag::O_DIRECTORY
            | nix::fcntl::OFlag::O_NOFOLLOW
            | nix::fcntl::OFlag::O_RDONLY,
        nix::sys::stat::Mode::empty(),
    )
    .map_err(|error| open_path_error(path, error, TrainerAttemptUnsafePathReason::NotDirectory))?;
    let directory = File::from(descriptor);
    let metadata = directory
        .metadata()
        .map_err(|source| TrainerAttemptEvidenceError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.is_dir() {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: path.to_path_buf(),
            reason: TrainerAttemptUnsafePathReason::NotDirectory,
        });
    }

    Ok(directory)
}

fn open_request_at(
    attempt_directory: &File,
    request_path: &Path,
) -> Result<File, TrainerAttemptEvidenceError> {
    let descriptor = nix::fcntl::openat(
        attempt_directory.as_fd(),
        "request.json",
        nix::fcntl::OFlag::O_CLOEXEC
            | nix::fcntl::OFlag::O_NOFOLLOW
            | nix::fcntl::OFlag::O_NONBLOCK
            | nix::fcntl::OFlag::O_RDONLY,
        nix::sys::stat::Mode::empty(),
    )
    .map_err(|error| {
        open_path_error(
            request_path,
            error,
            TrainerAttemptUnsafePathReason::NotRegularFile,
        )
    })?;
    let request_file = File::from(descriptor);
    if !request_file
        .metadata()
        .map_err(|source| TrainerAttemptEvidenceError::Io {
            path: request_path.to_path_buf(),
            source,
        })?
        .is_file()
    {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: request_path.to_path_buf(),
            reason: TrainerAttemptUnsafePathReason::NotRegularFile,
        });
    }

    Ok(request_file)
}

fn stable_file_state(
    file: &File,
    path: &Path,
) -> Result<StableFileState, TrainerAttemptEvidenceError> {
    let metadata = file
        .metadata()
        .map_err(|source| TrainerAttemptEvidenceError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.is_file() {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: path.to_path_buf(),
            reason: TrainerAttemptUnsafePathReason::NotRegularFile,
        });
    }

    Ok(StableFileState {
        identity: OwnershipLockIdentity::from_metadata(&metadata),
        length: metadata.len(),
        mode: metadata.mode(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

fn verify_request_path(
    attempt_directory: &File,
    request_path: &Path,
    expected_state: StableFileState,
) -> Result<(), TrainerAttemptEvidenceError> {
    let current_request = open_request_at(attempt_directory, request_path)?;
    if stable_file_state(&current_request, request_path)? != expected_state {
        return Err(TrainerAttemptEvidenceError::RequestChanged {
            path: request_path.to_path_buf(),
        });
    }

    Ok(())
}

fn verify_directory_name(
    parent: &File,
    name: &str,
    path: &Path,
    expected_directory: &File,
) -> Result<(), TrainerAttemptEvidenceError> {
    let current_directory = open_directory_at(parent, name, path)?;
    let current_metadata =
        current_directory
            .metadata()
            .map_err(|source| TrainerAttemptEvidenceError::Io {
                path: path.to_path_buf(),
                source,
            })?;
    let expected_metadata =
        expected_directory
            .metadata()
            .map_err(|source| TrainerAttemptEvidenceError::Io {
                path: path.to_path_buf(),
                source,
            })?;
    if OwnershipLockIdentity::from_metadata(&current_metadata)
        != OwnershipLockIdentity::from_metadata(&expected_metadata)
    {
        return Err(TrainerAttemptEvidenceError::RequestChanged {
            path: path.to_path_buf(),
        });
    }

    Ok(())
}

fn verify_runtime_root_identity(
    runtime_root: &Path,
    root_directory: &File,
    expected_identity: OwnershipLockIdentity,
) -> Result<(), TrainerAttemptEvidenceError> {
    let root_metadata = fs::symlink_metadata(runtime_root).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            return TrainerAttemptEvidenceError::Missing {
                path: runtime_root.to_path_buf(),
            };
        }

        TrainerAttemptEvidenceError::Io {
            path: runtime_root.to_path_buf(),
            source,
        }
    })?;
    if root_metadata.file_type().is_symlink() {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: runtime_root.to_path_buf(),
            reason: TrainerAttemptUnsafePathReason::SymbolicLink,
        });
    }
    if !root_metadata.is_dir() {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: runtime_root.to_path_buf(),
            reason: TrainerAttemptUnsafePathReason::NotDirectory,
        });
    }
    let opened_metadata =
        root_directory
            .metadata()
            .map_err(|source| TrainerAttemptEvidenceError::Io {
                path: runtime_root.to_path_buf(),
                source,
            })?;
    let canonical_runtime_root =
        runtime_root
            .canonicalize()
            .map_err(|source| TrainerAttemptEvidenceError::Io {
                path: runtime_root.to_path_buf(),
                source,
            })?;
    if canonical_runtime_root.as_os_str() != runtime_root.as_os_str() {
        return Err(TrainerAttemptEvidenceError::NonCanonicalRuntimeRoot {
            provided: runtime_root.to_path_buf(),
            canonical: canonical_runtime_root,
        });
    }
    if OwnershipLockIdentity::from_metadata(&root_metadata) != expected_identity
        || OwnershipLockIdentity::from_metadata(&opened_metadata) != expected_identity
    {
        return Err(TrainerAttemptEvidenceError::RequestChanged {
            path: runtime_root.to_path_buf(),
        });
    }

    Ok(())
}

fn verify_lock_path_identity(
    lock_path: &Path,
    expected_identity: OwnershipLockIdentity,
) -> Result<(), TrainerAttemptEvidenceError> {
    let metadata = fs::symlink_metadata(lock_path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            return TrainerAttemptEvidenceError::Missing {
                path: lock_path.to_path_buf(),
            };
        }

        TrainerAttemptEvidenceError::Io {
            path: lock_path.to_path_buf(),
            source,
        }
    })?;
    if metadata.file_type().is_symlink() {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: lock_path.to_path_buf(),
            reason: TrainerAttemptUnsafePathReason::SymbolicLink,
        });
    }
    if !metadata.is_file() {
        return Err(TrainerAttemptEvidenceError::UnsafePath {
            path: lock_path.to_path_buf(),
            reason: TrainerAttemptUnsafePathReason::NotRegularFile,
        });
    }
    if OwnershipLockIdentity::from_metadata(&metadata) != expected_identity {
        return Err(TrainerAttemptEvidenceError::OwnershipLockChanged {
            path: lock_path.to_path_buf(),
        });
    }

    Ok(())
}

fn open_path_error(
    path: &Path,
    error: nix::errno::Errno,
    not_regular_reason: TrainerAttemptUnsafePathReason,
) -> TrainerAttemptEvidenceError {
    let raw_error = error as i32;
    if error == nix::errno::Errno::ENOENT {
        return TrainerAttemptEvidenceError::Missing {
            path: path.to_path_buf(),
        };
    }
    if error == nix::errno::Errno::ELOOP {
        return TrainerAttemptEvidenceError::UnsafePath {
            path: path.to_path_buf(),
            reason: TrainerAttemptUnsafePathReason::SymbolicLink,
        };
    }
    if error == nix::errno::Errno::ENOTDIR || error == nix::errno::Errno::EISDIR {
        return TrainerAttemptEvidenceError::UnsafePath {
            path: path.to_path_buf(),
            reason: not_regular_reason,
        };
    }

    TrainerAttemptEvidenceError::Io {
        path: path.to_path_buf(),
        source: io::Error::from_raw_os_error(raw_error),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::path::{Path, PathBuf};

    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::super::test_support::{
        FileSnapshot, LOCK_ACQUIRED, create_lock, identity, lock_path, snapshot,
        start_fake_lock_process, try_fake_lock_process,
    };
    use super::{
        AttemptBinding, OwnershipLockIdentity, TrainerAttemptEvidenceError,
        TrainerAttemptUnsafePathReason, VerifiedTrainerAttempt,
        build_trainer_attempt_registration_evidence,
        build_trainer_attempt_registration_evidence_inner,
    };
    use crate::digest::Sha256Digest;

    #[derive(Debug, PartialEq, Eq)]
    enum TreeSnapshotEntry {
        Directory(OwnershipLockIdentity),
        File(FileSnapshot),
        Symlink(PathBuf),
    }

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

    fn request_value(binding: &AttemptBinding, epochs: u64, schema_version: u64) -> Value {
        json!({
            "schema_version": schema_version,
            "binding": binding,
            "adapter": {"kind": "speakrs"},
            "start_mode": {"kind": "fresh"},
            "payload": {"training": {"epochs": epochs}},
            "input_view": {
                "schema_version": 1,
                "revision_id": binding.campaign_revision_id,
                "root": "/trainer/input",
            },
            "inputs": [],
            "expected_outputs": [{"path": "checkpoint.bin", "kind": "file"}],
            "resources": {"accelerator": "cuda"},
            "worker": {
                "worker_id": "trainer-worker",
                "executable_digest": "a".repeat(64),
                "source_digest": "b".repeat(64),
                "environment_digest": "c".repeat(64),
            },
        })
    }

    fn request_bytes(binding: &AttemptBinding) -> Vec<u8> {
        serde_json::to_vec(&request_value(binding, 4, 1)).expect("serialize request fixture")
    }

    fn create_attempt_runtime(
        temp: &TempDir,
        binding: &AttemptBinding,
        request: Option<&[u8]>,
    ) -> (PathBuf, PathBuf, File) {
        let runtime_root = temp.path().join("runtime");
        let attempt_path = runtime_root.join("attempts").join(&binding.attempt_id);
        fs::create_dir_all(&attempt_path).expect("create trainer attempt directory");
        let lock = create_lock(&runtime_root, b"preserve trainer lock bytes");
        if let Some(request) = request {
            fs::write(attempt_path.join("request.json"), request)
                .expect("write trainer request fixture");
        }

        let runtime_root = fs::canonicalize(runtime_root).expect("canonicalize runtime root");
        let attempt_path = runtime_root.join("attempts").join(&binding.attempt_id);
        (runtime_root, attempt_path, lock)
    }

    fn tree_snapshot(root: &Path) -> Vec<(PathBuf, TreeSnapshotEntry)> {
        fn visit(root: &Path, directory: &Path, entries: &mut Vec<(PathBuf, TreeSnapshotEntry)>) {
            for entry in fs::read_dir(directory).expect("read runtime tree") {
                let path = entry.expect("read runtime entry").path();
                let relative = path
                    .strip_prefix(root)
                    .expect("runtime member is contained")
                    .to_path_buf();
                let metadata = fs::symlink_metadata(&path).expect("inspect runtime entry");
                if metadata.file_type().is_symlink() {
                    entries.push((
                        relative,
                        TreeSnapshotEntry::Symlink(
                            fs::read_link(&path).expect("read runtime symlink target"),
                        ),
                    ));
                } else if metadata.is_dir() {
                    entries.push((
                        relative,
                        TreeSnapshotEntry::Directory(OwnershipLockIdentity::from_metadata(
                            &metadata,
                        )),
                    ));
                    visit(root, &path, entries);
                } else {
                    entries.push((relative, TreeSnapshotEntry::File(snapshot(&path))));
                }
            }
        }

        let mut entries = Vec::new();
        visit(root, root, &mut entries);
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    }

    fn assert_attempt_evidence(
        evidence: &VerifiedTrainerAttempt,
        runtime_root: &Path,
        binding: &AttemptBinding,
        request: &[u8],
        lock_identity: OwnershipLockIdentity,
    ) {
        assert_eq!(evidence.canonical_runtime_root(), runtime_root);
        assert_eq!(evidence.binding(), binding);
        assert_eq!(
            evidence.request_digest().to_hex(),
            Sha256Digest::of(request).to_hex()
        );
        assert_eq!(evidence.ownership_lock_identity(), lock_identity);
    }

    #[test]
    fn exact_live_attempt_returns_immutable_read_only_evidence() {
        let temp = tempfile::tempdir().expect("create temporary directory");
        let binding = expected_binding();
        let request = request_bytes(&binding);
        let (runtime_root, attempt_path, lock) =
            create_attempt_runtime(&temp, &binding, Some(&request));
        let expected_identity = identity(&lock);
        let before = tree_snapshot(&runtime_root);
        let mut owner = start_fake_lock_process(&lock_path(&runtime_root));

        let evidence = build_trainer_attempt_registration_evidence(&runtime_root, &binding)
            .expect("collect exact active attempt evidence");

        assert_attempt_evidence(
            &evidence,
            &runtime_root,
            &binding,
            &request,
            expected_identity,
        );
        assert_eq!(
            fs::read(attempt_path.join("request.json")).expect("read unchanged request"),
            request
        );
        assert_eq!(tree_snapshot(&runtime_root), before);

        owner.release();
    }

    #[test]
    fn wrong_attempt_and_wrong_token_need_attention() {
        for observed in [
            AttemptBinding {
                attempt_id: "attempt-other".into(),
                ..expected_binding()
            },
            AttemptBinding {
                ownership_token: "owner-other".into(),
                ..expected_binding()
            },
        ] {
            let temp = tempfile::tempdir().expect("create temporary directory");
            let expected = expected_binding();
            let request = request_bytes(&observed);
            let (runtime_root, _attempt_path, _lock) =
                create_attempt_runtime(&temp, &expected, Some(&request));
            let before = tree_snapshot(&runtime_root);
            let mut owner = start_fake_lock_process(&lock_path(&runtime_root));

            assert!(matches!(
                build_trainer_attempt_registration_evidence(&runtime_root, &expected),
                Err(TrainerAttemptEvidenceError::BindingMismatch {
                    expected: actual_expected,
                    observed: actual_observed,
                    ..
                }) if actual_expected.as_ref() == &expected
                    && actual_observed.as_ref() == &observed
            ));
            assert_eq!(tree_snapshot(&runtime_root), before);

            owner.release();
        }
    }

    #[test]
    fn malformed_json_and_unsupported_request_schema_need_attention() {
        let binding = expected_binding();
        for request in [
            b"{not valid JSON".to_vec(),
            serde_json::to_vec(&request_value(&binding, 4, 2))
                .expect("serialize unsupported schema fixture"),
        ] {
            let temp = tempfile::tempdir().expect("create temporary directory");
            let (runtime_root, _attempt_path, _lock) =
                create_attempt_runtime(&temp, &binding, Some(&request));
            let before = tree_snapshot(&runtime_root);

            assert!(matches!(
                build_trainer_attempt_registration_evidence(&runtime_root, &binding),
                Err(TrainerAttemptEvidenceError::MalformedRequest { .. })
            ));
            assert_eq!(tree_snapshot(&runtime_root), before);
        }
    }

    #[test]
    fn missing_request_needs_attention_without_creating_it() {
        let temp = tempfile::tempdir().expect("create temporary directory");
        let binding = expected_binding();
        let (runtime_root, attempt_path, _lock) = create_attempt_runtime(&temp, &binding, None);
        let request_path = attempt_path.join("request.json");
        let before = tree_snapshot(&runtime_root);

        assert!(matches!(
            build_trainer_attempt_registration_evidence(&runtime_root, &binding),
            Err(TrainerAttemptEvidenceError::Missing { path }) if path == request_path
        ));
        assert_eq!(tree_snapshot(&runtime_root), before);
        assert!(!request_path.exists());
    }

    #[test]
    fn symlinked_request_needs_attention_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("create temporary directory");
        let binding = expected_binding();
        let (runtime_root, attempt_path, _lock) = create_attempt_runtime(&temp, &binding, None);
        let request_target = temp.path().join("request-target.json");
        fs::write(&request_target, request_bytes(&binding)).expect("write target fixture");
        let request_target_before = snapshot(&request_target);
        let request_path = attempt_path.join("request.json");
        symlink(&request_target, &request_path).expect("create request symlink");
        let before = tree_snapshot(&runtime_root);

        assert!(matches!(
            build_trainer_attempt_registration_evidence(&runtime_root, &binding),
            Err(TrainerAttemptEvidenceError::UnsafePath {
                path,
                reason: TrainerAttemptUnsafePathReason::SymbolicLink,
            }) if path == request_path
        ));
        assert_eq!(tree_snapshot(&runtime_root), before);
        assert_eq!(snapshot(&request_target), request_target_before);
    }

    #[test]
    fn free_lock_needs_attention_without_creating_or_holding_a_lock() {
        let temp = tempfile::tempdir().expect("create temporary directory");
        let binding = expected_binding();
        let request = request_bytes(&binding);
        let (runtime_root, _attempt_path, _lock) =
            create_attempt_runtime(&temp, &binding, Some(&request));
        let lock_path = lock_path(&runtime_root);
        let before = tree_snapshot(&runtime_root);

        assert!(matches!(
            build_trainer_attempt_registration_evidence(&runtime_root, &binding),
            Err(TrainerAttemptEvidenceError::OwnershipLockFree { path }) if path == lock_path
        ));
        assert_eq!(tree_snapshot(&runtime_root), before);
        assert!(try_fake_lock_process(&lock_path).contains(LOCK_ACQUIRED));
        assert_eq!(tree_snapshot(&runtime_root), before);
    }

    #[test]
    fn noncanonical_runtime_root_needs_attention() {
        let temp = tempfile::tempdir().expect("create temporary directory");
        let binding = expected_binding();
        let (runtime_root, _attempt_path, _lock) = create_attempt_runtime(&temp, &binding, None);
        let noncanonical_root = runtime_root.join("..").join("runtime");
        let before = tree_snapshot(&runtime_root);

        assert!(matches!(
            build_trainer_attempt_registration_evidence(&noncanonical_root, &binding),
            Err(TrainerAttemptEvidenceError::NonCanonicalRuntimeRoot {
                provided,
                canonical,
            }) if provided == noncanonical_root && canonical == runtime_root
        ));
        assert_eq!(tree_snapshot(&runtime_root), before);
    }

    #[test]
    fn relative_runtime_root_reports_its_canonical_path() {
        let relative_root = PathBuf::from(".");
        let canonical_root = fs::canonicalize(&relative_root).expect("canonicalize current dir");

        assert!(matches!(
            build_trainer_attempt_registration_evidence(&relative_root, &expected_binding()),
            Err(TrainerAttemptEvidenceError::NonCanonicalRuntimeRoot {
                provided,
                canonical,
            }) if provided == relative_root && canonical == canonical_root
        ));
    }

    #[test]
    fn request_replacement_during_collection_needs_attention() {
        let temp = tempfile::tempdir().expect("create temporary directory");
        let binding = expected_binding();
        let original_request = request_bytes(&binding);
        let (runtime_root, attempt_path, lock) =
            create_attempt_runtime(&temp, &binding, Some(&original_request));
        let mut owner = start_fake_lock_process(&lock_path(&runtime_root));
        let request_path = attempt_path.join("request.json");
        let replacement_path = attempt_path.join("request.replacement");
        let replacement_request = serde_json::to_vec(&request_value(&binding, 5, 1))
            .expect("serialize replacement request");

        let result =
            build_trainer_attempt_registration_evidence_inner(&runtime_root, &binding, || {
                fs::write(&replacement_path, &replacement_request)
                    .expect("write replacement fixture");
                fs::rename(&replacement_path, &request_path)
                    .expect("replace request during evidence collection");
            });

        assert!(matches!(
            result,
            Err(TrainerAttemptEvidenceError::RequestChanged { path }) if path == request_path
        ));
        assert_eq!(
            fs::read(&request_path).expect("read replacement request"),
            replacement_request
        );
        owner.release();
        drop(lock);
    }
}
