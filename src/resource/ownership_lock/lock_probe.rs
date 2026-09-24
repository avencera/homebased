//! Read-only probing of the exact direct-segment ownership lock

use std::fs::{self, File, Metadata, OpenOptions, TryLockError};
use std::io;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use thiserror::Error;

use super::SEGMENT_LOCK_FILE;

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
    #[must_use]
    pub fn from_metadata(metadata: &Metadata) -> Self {
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
        Ok(OwnershipLockIdentity::from_metadata(&self.file.metadata()?))
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

enum LockAttempt {
    Held,
    Acquired(File),
}

/// Probe the existing `.segment.lock` for one previously bound trainer attempt
///
/// The runtime root must be the canonical path saved with that attempt. The probe
/// does not create or write files. It returns a held guard only after the file's
/// device and inode match `expected` both before and after lock acquisition. This
/// proves only the state of this trainer lock, not that the physical GPU is free
#[must_use]
pub fn probe_segment_ownership_lock(
    runtime_root: &Path,
    expected: OwnershipLockIdentity,
) -> OwnershipLockProbe {
    match probe_lock(runtime_root, expected) {
        Ok(LockAttempt::Held) => OwnershipLockProbe::OwnershipHeld,
        Ok(LockAttempt::Acquired(file)) => {
            OwnershipLockProbe::ExactOwnershipReleased(OwnershipLockGuard { file })
        }
        Err(error) => OwnershipLockProbe::Attention(error),
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

fn probe_lock(
    runtime_root: &Path,
    expected: OwnershipLockIdentity,
) -> Result<LockAttempt, OwnershipLockProbeError> {
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

    // std file locks are flock(2) on Unix, the same lock the trainer takes
    match file.try_lock() {
        Ok(()) => {
            verify_path_identity(&lock_path, opened_identity)?;
            Ok(LockAttempt::Acquired(file))
        }
        Err(TryLockError::WouldBlock) => {
            verify_path_identity(&lock_path, opened_identity)?;
            Ok(LockAttempt::Held)
        }
        Err(TryLockError::Error(source)) => Err(OwnershipLockProbeError::Io {
            operation: OwnershipLockProbeOperation::AcquireExclusiveLock,
            path: lock_path,
            source,
        }),
    }
}

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

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs::{self, OpenOptions, TryLockError};
    use std::io::{Read, Write};

    use super::super::test_support::{
        CHILD_MODE_ENV, CHILD_PATH_ENV, LOCK_ACQUIRED, LOCK_CONTENDED, create_lock,
        directory_entries, identity, lock_path, snapshot, start_fake_lock_process,
        try_fake_lock_process,
    };
    use super::{
        OwnershipLockIdentity, OwnershipLockIdentityMismatchReason, OwnershipLockProbe,
        OwnershipLockProbeError, probe_segment_ownership_lock,
    };

    #[test]
    fn fake_process_lock_helper() -> Result<(), Box<dyn std::error::Error>> {
        let (Ok(path), Ok(mode)) = (env::var(CHILD_PATH_ENV), env::var(CHILD_MODE_ENV)) else {
            return Ok(());
        };

        let file = OpenOptions::new().read(true).write(true).open(path)?;
        match file.try_lock() {
            Ok(()) if mode == "hold" => {
                writeln!(std::io::stdout(), "{LOCK_ACQUIRED}")?;
                std::io::stdout().flush()?;
                let mut release = [0_u8; 1];
                std::io::stdin().read_exact(&mut release)?;
            }
            Ok(()) => writeln!(std::io::stdout(), "{LOCK_ACQUIRED}")?,
            Err(TryLockError::WouldBlock) => {
                writeln!(std::io::stdout(), "{LOCK_CONTENDED}")?;
            }
            Err(TryLockError::Error(error)) => return Err(Box::new(error)),
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
}
