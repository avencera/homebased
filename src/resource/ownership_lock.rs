//! Read-only probing of the exact direct-segment ownership lock

use std::fs::File;
#[cfg(unix)]
use std::fs::{self, Metadata, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use super::watcher::{AttemptBinding, AttemptRequestValidationError, validate_attempt_request};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[cfg(unix)]
const SEGMENT_LOCK_FILE: &str = ".segment.lock";

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
    /// Unix filesystem identity and flock semantics are required
    #[error("trainer attempt registration evidence is unsupported on this platform")]
    UnsupportedPlatform,
}

/// SHA-256 digest of the exact bytes in one validated trainer request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrainerRequestDigest([u8; 32]);

impl TrainerRequestDigest {
    /// Return the raw 32-byte SHA-256 digest
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Return the digest as 64 lowercase hexadecimal characters
    #[must_use]
    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    pub(crate) fn from_hex(value: &str) -> Option<Self> {
        let bytes = value.as_bytes();
        if bytes.len() != 64 {
            return None;
        }

        let mut digest = [0; 32];
        let (pairs, remainder) = bytes.as_chunks::<2>();
        if !remainder.is_empty() {
            return None;
        }
        for (index, [high, low]) in pairs.iter().enumerate() {
            let high = trainer_digest_hex_digit(*high)?;
            let low = trainer_digest_hex_digit(*low)?;
            digest[index] = (high << 4) | low;
        }

        Some(Self(digest))
    }
}

fn trainer_digest_hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

/// Point-in-time evidence for registering one active direct-segment trainer attempt
///
/// This binds the canonical runtime root, strict trainer `AttemptBinding`, exact
/// request bytes, and the device/inode of a lock observed as held. It does not bind
/// the exact Homebased `TaskId` or normalized command spec; the later authority-owned
/// registration must validate both. This evidence is not proof that the GPU has
/// been released and must not be used to mark a resource free. The lock observation
/// is point-in-time; the lock may be released after this value is returned.
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
/// only `attempts/<attempt_id>/request.json` and probes the existing `.segment.lock`.
/// It does not create or write trainer artifacts. It returns evidence only if the
/// request is stable and valid and the exact lock is held at probe time. The result
/// does not prove GPU release. Later resource-aware registration must also bind the
/// exact Homebased `TaskId` and normalized command spec. The lock API reports
/// contention but does not identify its owner, so the caller must not hold this lock
/// itself. The intended lock holder is the trainer process.
pub fn build_trainer_attempt_registration_evidence(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
) -> Result<VerifiedTrainerAttempt, TrainerAttemptEvidenceError> {
    build_trainer_attempt_registration_evidence_inner(runtime_root, expected_binding, || {})
}

#[cfg(unix)]
fn build_trainer_attempt_registration_evidence_inner<F>(
    runtime_root: &Path,
    expected_binding: &AttemptBinding,
    after_request_read: F,
) -> Result<VerifiedTrainerAttempt, TrainerAttemptEvidenceError>
where
    F: FnOnce(),
{
    expected_binding.validate().map_err(|error| match error {
        super::watcher::WatcherError::InvalidAttemptBinding { reason } => {
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

    let digest = Sha256::digest(&request_bytes).into();
    Ok(VerifiedTrainerAttempt {
        canonical_runtime_root,
        binding: expected_binding.clone(),
        request_digest: TrainerRequestDigest(digest),
        ownership_lock_identity: lock_identity,
    })
}

#[cfg(not(unix))]
fn build_trainer_attempt_registration_evidence_inner<F>(
    _runtime_root: &Path,
    _expected_binding: &AttemptBinding,
    _after_request_read: F,
) -> Result<VerifiedTrainerAttempt, TrainerAttemptEvidenceError>
where
    F: FnOnce(),
{
    Err(TrainerAttemptEvidenceError::UnsupportedPlatform)
}

#[cfg(unix)]
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

#[cfg(unix)]
fn open_canonical_runtime_root(
    runtime_root: &Path,
) -> Result<(PathBuf, File, OwnershipLockIdentity), TrainerAttemptEvidenceError> {
    use std::os::unix::fs::OpenOptionsExt;

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

#[cfg(unix)]
fn open_directory_at(
    parent: &File,
    name: &str,
    path: &Path,
) -> Result<File, TrainerAttemptEvidenceError> {
    use std::os::fd::AsFd;

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

#[cfg(unix)]
fn open_request_at(
    attempt_directory: &File,
    request_path: &Path,
) -> Result<File, TrainerAttemptEvidenceError> {
    use std::os::fd::AsFd;

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

#[cfg(unix)]
fn stable_file_state(
    file: &File,
    path: &Path,
) -> Result<StableFileState, TrainerAttemptEvidenceError> {
    use std::os::unix::fs::MetadataExt;

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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

/// Device and inode identity of one trainer ownership lock
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OwnershipLockIdentity {
    device: u64,
    inode: u64,
}

impl OwnershipLockIdentity {
    /// Create an identity from Unix device and inode values
    #[must_use]
    pub const fn new(device: u64, inode: u64) -> Self {
        Self { device, inode }
    }

    /// Capture the device and inode from an opened trainer lock file
    #[cfg(unix)]
    #[must_use]
    pub fn from_metadata(metadata: &Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;

        Self::new(metadata.dev(), metadata.ino())
    }

    /// Return the device number on Unix
    #[must_use]
    pub const fn device(self) -> u64 {
        self.device
    }

    /// Return the inode number on Unix
    #[must_use]
    pub const fn inode(self) -> u64 {
        self.inode
    }
}

/// Reason an existing runtime lock path cannot prove the expected identity
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipLockIdentityMismatchReason {
    /// The `.segment.lock` path does not exist
    Missing,
    /// The `.segment.lock` path is a symbolic link
    SymbolicLink,
    /// The path does not name a regular file
    NotRegularFile,
    /// The path names a different device and inode
    DifferentFile,
    /// The supplied runtime root is a symbolic link
    RuntimeRootSymbolicLink,
    /// The supplied runtime root is not a directory
    RuntimeRootNotDirectory,
}

/// Filesystem or platform operation involved in an ownership-lock probe
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipLockProbeOperation {
    /// Inspect the supplied runtime root without following its final component
    InspectRuntimeRoot,
    /// Inspect the lock path without following its final component
    InspectLockPath,
    /// Open the existing lock file
    OpenLockFile,
    /// Inspect the opened lock descriptor
    InspectOpenedFile,
    /// Attempt the exclusive nonblocking lock
    AcquireExclusiveLock,
}

/// A typed reason that an ownership-lock probe needs attention
#[derive(Debug, Error)]
pub enum OwnershipLockProbeError {
    /// A filesystem operation failed while probing the runtime lock
    #[error("cannot {operation:?} for runtime ownership lock {path}: {source}")]
    Io {
        /// Operation that failed
        operation: OwnershipLockProbeOperation,
        /// Path involved in the failed operation
        path: PathBuf,
        /// Underlying operating system error
        #[source]
        source: io::Error,
    },
    /// This platform does not provide the required Unix lock semantics
    #[error("runtime ownership-lock probing is unsupported on this platform")]
    UnsupportedPlatform,
    /// A lock path does not match the identity captured for this trainer attempt
    #[error(
        "runtime ownership lock identity mismatch at {path}: expected {expected:?}, observed {observed:?} ({reason:?})"
    )]
    IdentityMismatch {
        /// Path that failed the identity check
        path: PathBuf,
        /// Exact lock identity saved for this trainer attempt
        expected: OwnershipLockIdentity,
        /// Identity observed at the path, when it names a filesystem object
        observed: Option<OwnershipLockIdentity>,
        /// Why the path cannot be trusted as the saved lock
        reason: OwnershipLockIdentityMismatchReason,
    },
}

/// A guard that keeps the exact released lock exclusively held until it is dropped
#[must_use = "keep the guard alive through the release transaction"]
#[derive(Debug)]
pub struct OwnershipLockGuard {
    file: File,
}

impl OwnershipLockGuard {
    /// Return the identity of the lock held by this guard
    pub fn identity(&self) -> io::Result<OwnershipLockIdentity> {
        let metadata = self.file.metadata()?;

        #[cfg(unix)]
        {
            Ok(OwnershipLockIdentity::from_metadata(&metadata))
        }

        #[cfg(not(unix))]
        {
            let _ = metadata;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "ownership-lock identities require Unix device and inode values",
            ))
        }
    }
}

/// Conclusion from probing one exact runtime ownership lock
#[derive(Debug)]
pub enum OwnershipLockProbe {
    /// The exact lock file is still held by a process
    OwnershipHeld,
    /// The exact lock was released and is now held exclusively by this result's guard
    ExactOwnershipReleased(OwnershipLockGuard),
    /// The probe could not establish ownership state safely
    Attention(OwnershipLockProbeError),
}

#[cfg(unix)]
enum LockAttempt {
    Held,
    Acquired(File),
}

/// Probe the existing `.segment.lock` for one previously bound trainer attempt
///
/// The runtime root must be the canonical path saved with that attempt. The probe
/// does not create or write files. It returns a held guard only after the file's
/// device and inode match `expected` both before and after lock acquisition. This
/// proves only the state of this trainer lock, not that the physical GPU is free.
#[must_use]
pub fn probe_segment_ownership_lock(
    runtime_root: &Path,
    expected: OwnershipLockIdentity,
) -> OwnershipLockProbe {
    #[cfg(unix)]
    {
        match probe_unix(runtime_root, expected) {
            Ok(LockAttempt::Held) => OwnershipLockProbe::OwnershipHeld,
            Ok(LockAttempt::Acquired(file)) => {
                OwnershipLockProbe::ExactOwnershipReleased(OwnershipLockGuard { file })
            }
            Err(error) => OwnershipLockProbe::Attention(error),
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (runtime_root, expected);
        OwnershipLockProbe::Attention(OwnershipLockProbeError::UnsupportedPlatform)
    }
}

/// Recheck that an exclusive guard still holds the exact lock named by its saved runtime root
///
/// The check rejects a replaced, missing, or symbolic-link path while retaining the guard
pub fn verify_ownership_lock_guard(
    runtime_root: &Path,
    expected: OwnershipLockIdentity,
    guard: &OwnershipLockGuard,
) -> Result<(), OwnershipLockProbeError> {
    #[cfg(unix)]
    {
        let lock_path = runtime_root.join(SEGMENT_LOCK_FILE);
        let opened_identity = guard
            .identity()
            .map_err(|source| OwnershipLockProbeError::Io {
                operation: OwnershipLockProbeOperation::InspectOpenedFile,
                path: lock_path.clone(),
                source,
            })?;
        if opened_identity != expected {
            return Err(identity_mismatch(
                &lock_path,
                expected,
                Some(opened_identity),
                OwnershipLockIdentityMismatchReason::DifferentFile,
            ));
        }

        verify_runtime_root(runtime_root, expected)?;
        verify_path_identity(&lock_path, expected)
    }

    #[cfg(not(unix))]
    {
        let _ = (runtime_root, expected, guard);
        Err(OwnershipLockProbeError::UnsupportedPlatform)
    }
}

#[cfg(unix)]
fn probe_unix(
    runtime_root: &Path,
    expected: OwnershipLockIdentity,
) -> Result<LockAttempt, OwnershipLockProbeError> {
    use std::os::unix::fs::OpenOptionsExt;

    let lock_path = runtime_root.join(SEGMENT_LOCK_FILE);
    verify_runtime_root(runtime_root, expected)?;
    verify_path_identity(&lock_path, expected)?;

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(&lock_path)
        .map_err(|source| classify_open_error(&lock_path, expected, source))?;

    let opened_metadata = file
        .metadata()
        .map_err(|source| OwnershipLockProbeError::Io {
            operation: OwnershipLockProbeOperation::InspectOpenedFile,
            path: lock_path.clone(),
            source,
        })?;
    let opened_identity = verify_metadata_identity(&lock_path, expected, &opened_metadata)?;

    match flock_exclusive_nonblocking(&file) {
        Ok(()) => {
            verify_path_identity(&lock_path, opened_identity)?;
            Ok(LockAttempt::Acquired(file))
        }
        Err(error) if is_would_block(error) => {
            verify_path_identity(&lock_path, opened_identity)?;
            Ok(LockAttempt::Held)
        }
        Err(error) => Err(OwnershipLockProbeError::Io {
            operation: OwnershipLockProbeOperation::AcquireExclusiveLock,
            path: lock_path,
            source: io::Error::from_raw_os_error(error as i32),
        }),
    }
}

#[cfg(unix)]
fn is_would_block(error: nix::errno::Errno) -> bool {
    error == nix::errno::Errno::EAGAIN || error == nix::errno::Errno::EWOULDBLOCK
}

#[cfg(unix)]
#[allow(deprecated)]
fn flock_exclusive_nonblocking(file: &File) -> nix::Result<()> {
    use std::os::fd::AsRawFd;

    nix::fcntl::flock(
        file.as_raw_fd(),
        nix::fcntl::FlockArg::LockExclusiveNonblock,
    )
}

#[cfg(unix)]
fn verify_runtime_root(
    runtime_root: &Path,
    expected: OwnershipLockIdentity,
) -> Result<(), OwnershipLockProbeError> {
    let metadata =
        fs::symlink_metadata(runtime_root).map_err(|source| OwnershipLockProbeError::Io {
            operation: OwnershipLockProbeOperation::InspectRuntimeRoot,
            path: runtime_root.to_path_buf(),
            source,
        })?;

    if metadata.file_type().is_symlink() {
        return Err(identity_mismatch(
            runtime_root,
            expected,
            None,
            OwnershipLockIdentityMismatchReason::RuntimeRootSymbolicLink,
        ));
    }
    if !metadata.is_dir() {
        return Err(identity_mismatch(
            runtime_root,
            expected,
            None,
            OwnershipLockIdentityMismatchReason::RuntimeRootNotDirectory,
        ));
    }

    Ok(())
}

#[cfg(unix)]
fn verify_path_identity(
    path: &Path,
    expected: OwnershipLockIdentity,
) -> Result<(), OwnershipLockProbeError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            return identity_mismatch(
                path,
                expected,
                None,
                OwnershipLockIdentityMismatchReason::Missing,
            );
        }

        OwnershipLockProbeError::Io {
            operation: OwnershipLockProbeOperation::InspectLockPath,
            path: path.to_path_buf(),
            source,
        }
    })?;

    if metadata.file_type().is_symlink() {
        return Err(identity_mismatch(
            path,
            expected,
            None,
            OwnershipLockIdentityMismatchReason::SymbolicLink,
        ));
    }

    verify_metadata_identity(path, expected, &metadata).map(|_| ())
}

#[cfg(unix)]
fn verify_metadata_identity(
    path: &Path,
    expected: OwnershipLockIdentity,
    metadata: &Metadata,
) -> Result<OwnershipLockIdentity, OwnershipLockProbeError> {
    let observed = OwnershipLockIdentity::from_metadata(metadata);

    if !metadata.is_file() {
        return Err(identity_mismatch(
            path,
            expected,
            Some(observed),
            OwnershipLockIdentityMismatchReason::NotRegularFile,
        ));
    }

    if observed != expected {
        return Err(identity_mismatch(
            path,
            expected,
            Some(observed),
            OwnershipLockIdentityMismatchReason::DifferentFile,
        ));
    }

    Ok(observed)
}

#[cfg(unix)]
fn classify_open_error(
    path: &Path,
    expected: OwnershipLockIdentity,
    source: io::Error,
) -> OwnershipLockProbeError {
    let reason = match source.kind() {
        io::ErrorKind::NotFound => Some(OwnershipLockIdentityMismatchReason::Missing),
        io::ErrorKind::IsADirectory => Some(OwnershipLockIdentityMismatchReason::NotRegularFile),
        _ if source.raw_os_error() == Some(nix::libc::ELOOP) => {
            Some(OwnershipLockIdentityMismatchReason::SymbolicLink)
        }
        _ => None,
    };

    if let Some(reason) = reason {
        return identity_mismatch(path, expected, None, reason);
    }

    OwnershipLockProbeError::Io {
        operation: OwnershipLockProbeOperation::OpenLockFile,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    #![allow(clippy::expect_used)]

    use std::env;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Child, ChildStdout, Command, Stdio};

    use super::{
        AttemptBinding, OwnershipLockIdentity, TrainerRequestDigest, VerifiedTrainerAttempt,
    };

    pub(crate) fn attempt_binding(attempt_id: &str) -> AttemptBinding {
        AttemptBinding {
            campaign_id: "campaign-1".into(),
            campaign_revision_id: "revision-1".into(),
            task_id: "trainer-task-1".into(),
            attempt_id: attempt_id.into(),
            attempt_number: 1,
            ownership_token: "token-1".into(),
        }
    }

    pub(crate) fn verified_attempt(
        binding: AttemptBinding,
        request_sha256: [u8; 32],
        lock_identity: OwnershipLockIdentity,
    ) -> VerifiedTrainerAttempt {
        VerifiedTrainerAttempt {
            canonical_runtime_root: PathBuf::from("/trainer/runtime"),
            binding,
            request_digest: TrainerRequestDigest(request_sha256),
            ownership_lock_identity: lock_identity,
        }
    }

    pub(crate) const CHILD_PATH_ENV: &str = "HOMEBASED_OWNERSHIP_LOCK_TEST_PATH";
    pub(crate) const CHILD_MODE_ENV: &str = "HOMEBASED_OWNERSHIP_LOCK_TEST_MODE";
    pub(crate) const HELPER_TEST: &str =
        "resource::ownership_lock::tests::fake_process_lock_helper";
    pub(crate) const LOCK_ACQUIRED: &str = "FAKE_LOCK_ACQUIRED";
    pub(crate) const LOCK_CONTENDED: &str = "FAKE_LOCK_CONTENDED";

    /// Separate test process that holds one exact lock file until released or dropped
    ///
    /// flock is owned by the open file description, so a holder in another process
    /// behaves like the trainer worker that outlives its wrapper
    pub(crate) struct FakeLockProcess {
        child: Child,
        output: BufReader<ChildStdout>,
        released: bool,
    }

    impl FakeLockProcess {
        /// Let the holder release the lock and wait for it to exit
        pub(crate) fn release(&mut self) {
            self.child
                .stdin
                .as_mut()
                .expect("fake lock process stdin is piped")
                .write_all(b"x")
                .expect("fake lock process accepts release input");
            self.child.stdin.take();
            let status = self.child.wait().expect("fake lock process exits");
            let mut trailing = String::new();
            self.output
                .read_to_string(&mut trailing)
                .expect("read fake lock process output");
            assert!(status.success(), "fake lock process failed: {trailing}");
            self.released = true;
        }
    }

    impl Drop for FakeLockProcess {
        fn drop(&mut self) {
            if self.released {
                return;
            }

            if let Some(stdin) = self.child.stdin.as_mut() {
                let _ = stdin.write_all(b"x");
            }
            self.child.stdin.take();
            let _ = self.child.wait();
        }
    }

    /// Start a separate process that holds the existing lock file at `path`
    pub(crate) fn start_fake_lock_process(path: &Path) -> FakeLockProcess {
        let mut child = Command::new(env::current_exe().expect("find current test binary"))
            .arg("--exact")
            .arg(HELPER_TEST)
            .arg("--nocapture")
            .env(CHILD_PATH_ENV, path)
            .env(CHILD_MODE_ENV, "hold")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start fake lock-holder process");
        let mut process = FakeLockProcess {
            output: BufReader::new(child.stdout.take().expect("capture helper stdout")),
            child,
            released: false,
        };

        let mut line = String::new();
        loop {
            line.clear();
            let bytes = process
                .output
                .read_line(&mut line)
                .expect("read fake lock-holder readiness");
            assert_ne!(bytes, 0, "fake lock-holder exited before acquiring lock");
            if line.trim() == LOCK_ACQUIRED {
                break;
            }
        }

        process
    }
}

#[cfg(unix)]
fn identity_mismatch(
    path: &Path,
    expected: OwnershipLockIdentity,
    observed: Option<OwnershipLockIdentity>,
    reason: OwnershipLockIdentityMismatchReason,
) -> OwnershipLockProbeError {
    OwnershipLockProbeError::IdentityMismatch {
        path: path.to_path_buf(),
        expected,
        observed,
        reason,
    }
}

#[cfg(all(test, unix))]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::env;
    use std::fs::{self, File, OpenOptions};
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::time::SystemTime;

    use tempfile::TempDir;

    use super::{
        AttemptBinding, OwnershipLockIdentity, OwnershipLockIdentityMismatchReason,
        OwnershipLockProbe, OwnershipLockProbeError, TrainerAttemptEvidenceError,
        TrainerAttemptUnsafePathReason, VerifiedTrainerAttempt,
        build_trainer_attempt_registration_evidence,
        build_trainer_attempt_registration_evidence_inner, probe_segment_ownership_lock,
    };
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};

    use super::test_support::{
        CHILD_MODE_ENV, CHILD_PATH_ENV, HELPER_TEST, LOCK_ACQUIRED, LOCK_CONTENDED,
        start_fake_lock_process,
    };

    #[derive(Debug, PartialEq, Eq)]
    struct FileSnapshot {
        identity: OwnershipLockIdentity,
        contents: Vec<u8>,
        length: u64,
        modified: Option<SystemTime>,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum TreeSnapshotEntry {
        Directory(OwnershipLockIdentity),
        File(FileSnapshot),
        Symlink(PathBuf),
    }

    fn lock_path(runtime_root: &Path) -> PathBuf {
        runtime_root.join(".segment.lock")
    }

    fn create_lock(runtime_root: &Path, contents: &[u8]) -> File {
        fs::create_dir_all(runtime_root).expect("create temporary runtime root");
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(lock_path(runtime_root))
            .expect("create fake lock file");
        file.write_all(contents).expect("write fake lock file");
        file
    }

    fn identity(file: &File) -> OwnershipLockIdentity {
        OwnershipLockIdentity::from_metadata(&file.metadata().expect("read file metadata"))
    }

    fn snapshot(path: &Path) -> FileSnapshot {
        let file = File::open(path).expect("open file for a read-only snapshot");
        let metadata = file.metadata().expect("read file snapshot metadata");
        let contents = fs::read(path).expect("read file snapshot contents");

        FileSnapshot {
            identity: OwnershipLockIdentity::from_metadata(&metadata),
            contents,
            length: metadata.len(),
            modified: metadata.modified().ok(),
        }
    }

    fn directory_entries(path: &Path) -> Vec<PathBuf> {
        let mut entries = fs::read_dir(path)
            .expect("read runtime root entries")
            .map(|entry| entry.expect("read runtime root entry").path())
            .collect::<Vec<_>>();
        entries.sort();
        entries
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

    fn request_digest(bytes: &[u8]) -> String {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
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
        assert_eq!(evidence.request_digest().to_hex(), request_digest(request));
        assert_eq!(evidence.ownership_lock_identity(), lock_identity);
    }

    fn try_fake_lock_process(path: &Path) -> String {
        let output = Command::new(env::current_exe().expect("find current test binary"))
            .arg("--exact")
            .arg(HELPER_TEST)
            .arg("--nocapture")
            .env(CHILD_PATH_ENV, path)
            .env(CHILD_MODE_ENV, "try")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("run fake nonblocking lock attempt");
        assert!(
            output.status.success(),
            "fake lock attempt failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        String::from_utf8(output.stdout).expect("fake lock helper output is UTF-8")
    }

    #[allow(deprecated)]
    fn try_flock(file: &File) -> nix::Result<()> {
        use std::os::fd::AsRawFd;

        nix::fcntl::flock(
            file.as_raw_fd(),
            nix::fcntl::FlockArg::LockExclusiveNonblock,
        )
    }

    #[test]
    fn fake_process_lock_helper() -> Result<(), Box<dyn std::error::Error>> {
        let (Ok(path), Ok(mode)) = (env::var(CHILD_PATH_ENV), env::var(CHILD_MODE_ENV)) else {
            return Ok(());
        };

        let file = OpenOptions::new().read(true).write(true).open(path)?;
        match try_flock(&file) {
            Ok(()) if mode == "hold" => {
                writeln!(std::io::stdout(), "{LOCK_ACQUIRED}")?;
                std::io::stdout().flush()?;
                let mut release = [0_u8; 1];
                std::io::stdin().read_exact(&mut release)?;
            }
            Ok(()) => writeln!(std::io::stdout(), "{LOCK_ACQUIRED}")?,
            Err(error) if super::is_would_block(error) => {
                writeln!(std::io::stdout(), "{LOCK_CONTENDED}")?;
            }
            Err(error) => return Err(Box::new(error)),
        }

        Ok(())
    }

    #[test]
    fn separate_process_ownership_is_detected_and_released_guard_stays_exclusive() {
        let temp = tempfile::tempdir().expect("create temporary directory");
        let runtime_root = temp.path().join("runtime");
        let file = create_lock(&runtime_root, b"preserve lock bytes");
        let expected = identity(&file);
        let path = lock_path(&runtime_root);
        let before_file = snapshot(&path);
        let before_entries = directory_entries(&runtime_root);
        let mut owner = start_fake_lock_process(&path);

        assert!(matches!(
            probe_segment_ownership_lock(&runtime_root, expected),
            OwnershipLockProbe::OwnershipHeld
        ));
        assert_eq!(snapshot(&path), before_file);
        assert_eq!(directory_entries(&runtime_root), before_entries);

        owner.release();
        let guard = match probe_segment_ownership_lock(&runtime_root, expected) {
            OwnershipLockProbe::ExactOwnershipReleased(guard) => guard,
            outcome => panic!("expected an exclusive released-lock guard, got {outcome:?}"),
        };
        assert_eq!(guard.identity().expect("read guard identity"), expected);
        assert!(try_fake_lock_process(&path).contains(LOCK_CONTENDED));
        assert_eq!(snapshot(&path), before_file);
        assert_eq!(directory_entries(&runtime_root), before_entries);

        drop(guard);
        assert!(try_fake_lock_process(&path).contains(LOCK_ACQUIRED));
        assert_eq!(snapshot(&path), before_file);
        assert_eq!(directory_entries(&runtime_root), before_entries);
    }

    #[test]
    fn replaced_lock_path_needs_attention_without_mutating_the_replacement() {
        let temp = tempfile::tempdir().expect("create temporary directory");
        let runtime_root = temp.path().join("runtime");
        let old_file = create_lock(&runtime_root, b"old lock");
        let expected = identity(&old_file);
        let path = lock_path(&runtime_root);
        let replacement_path = runtime_root.join("replacement");
        let mut replacement = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&replacement_path)
            .expect("create replacement file");
        replacement
            .write_all(b"replacement bytes")
            .expect("write replacement file");
        let replacement_identity = identity(&replacement);
        assert_ne!(replacement_identity, expected);
        fs::rename(&replacement_path, &path).expect("replace lock path atomically");
        let before_file = snapshot(&path);
        let before_entries = directory_entries(&runtime_root);

        assert!(matches!(
            probe_segment_ownership_lock(&runtime_root, expected),
            OwnershipLockProbe::Attention(OwnershipLockProbeError::IdentityMismatch {
                reason: OwnershipLockIdentityMismatchReason::DifferentFile,
                ..
            })
        ));
        assert_eq!(snapshot(&path), before_file);
        assert_eq!(directory_entries(&runtime_root), before_entries);
        drop(old_file);
    }

    #[test]
    fn missing_lock_needs_attention_and_is_not_created() {
        let temp = tempfile::tempdir().expect("create temporary directory");
        let runtime_root = temp.path().join("runtime");
        fs::create_dir(&runtime_root).expect("create runtime root");
        let expected = OwnershipLockIdentity::new(u64::MAX, u64::MAX);
        let path = lock_path(&runtime_root);

        assert!(matches!(
            probe_segment_ownership_lock(&runtime_root, expected),
            OwnershipLockProbe::Attention(OwnershipLockProbeError::IdentityMismatch {
                reason: OwnershipLockIdentityMismatchReason::Missing,
                ..
            })
        ));
        assert!(!path.exists());
        assert!(directory_entries(&runtime_root).is_empty());
    }

    #[test]
    fn symlinked_lock_needs_attention_without_opening_or_mutating_its_target() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("create temporary directory");
        let runtime_root = temp.path().join("runtime");
        fs::create_dir(&runtime_root).expect("create runtime root");
        let target = temp.path().join("target.lock");
        let mut target_file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&target)
            .expect("create symlink target");
        target_file
            .write_all(b"target must not be touched")
            .expect("write symlink target");
        let expected = identity(&target_file);
        let path = lock_path(&runtime_root);
        symlink(&target, &path).expect("create lock symlink");
        let before_target = snapshot(&target);
        let before_entries = directory_entries(&runtime_root);

        assert!(matches!(
            probe_segment_ownership_lock(&runtime_root, expected),
            OwnershipLockProbe::Attention(OwnershipLockProbeError::IdentityMismatch {
                reason: OwnershipLockIdentityMismatchReason::SymbolicLink,
                ..
            })
        ));
        assert_eq!(snapshot(&target), before_target);
        assert_eq!(directory_entries(&runtime_root), before_entries);
        assert!(
            fs::symlink_metadata(&path)
                .expect("inspect original lock symlink")
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn nonregular_lock_path_needs_attention() {
        let temp = tempfile::tempdir().expect("create temporary directory");
        let runtime_root = temp.path().join("runtime");
        fs::create_dir(&runtime_root).expect("create runtime root");
        let path = lock_path(&runtime_root);
        fs::create_dir(&path).expect("create nonregular lock path");
        let expected = OwnershipLockIdentity::new(u64::MAX, u64::MAX);
        let before_entries = directory_entries(&runtime_root);

        assert!(matches!(
            probe_segment_ownership_lock(&runtime_root, expected),
            OwnershipLockProbe::Attention(OwnershipLockProbeError::IdentityMismatch {
                reason: OwnershipLockIdentityMismatchReason::NotRegularFile,
                ..
            })
        ));
        assert_eq!(directory_entries(&runtime_root), before_entries);
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
