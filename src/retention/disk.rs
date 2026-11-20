//! Filesystem reads and removals of a retention pass

use std::fs::{self, File};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use nix::fcntl::{Flock, FlockArg};
use uuid::Uuid;

/// Space and newest change under one directory
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Usage {
    /// Bytes allocated on disk, so a sparse checkpoint counts what it holds
    pub(super) bytes: u64,
    /// Newest modification time of the directory or anything in it
    pub(super) newest: DateTime<Utc>,
}

/// Walk `root` without following symlinks
///
/// Entries that vanish during the walk are skipped. Any other read error fails
/// the walk, since a file the walk missed may be the recent change that keeps
/// the entry, and the inventory never prunes an entry it cannot measure
pub(super) fn measure(root: &Path) -> io::Result<Usage> {
    let meta = fs::symlink_metadata(root)?;
    let mut usage = Usage {
        bytes: allocated(&meta),
        newest: modified(&meta),
    };
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Some(children) = gone_is_none(fs::read_dir(&dir))? else {
            continue;
        };
        for child in children {
            let Some(child) = gone_is_none(child)? else {
                continue;
            };
            let Some(meta) = gone_is_none(child.metadata())? else {
                continue;
            };
            usage.bytes = usage.bytes.saturating_add(allocated(&meta));
            usage.newest = usage.newest.max(modified(&meta));
            if meta.is_dir() {
                pending.push(child.path());
            }
        }
    }

    Ok(usage)
}

/// `None` for something removed while the walk ran
fn gone_is_none<T>(result: io::Result<T>) -> io::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn allocated(meta: &fs::Metadata) -> u64 {
    // st_blocks counts 512-byte units whatever the filesystem block size
    meta.blocks().saturating_mul(512)
}

fn modified(meta: &fs::Metadata) -> DateTime<Utc> {
    meta.modified().unwrap_or(SystemTime::UNIX_EPOCH).into()
}

/// Child directories of `dir` named by a UUID, with their parsed id
///
/// Anything else, such as a stray file or a name a later release adds, is
/// never a retention entry. A missing `dir` has no children
pub(super) fn uuid_dirs(dir: &Path) -> io::Result<Vec<(Uuid, PathBuf)>> {
    let children = match fs::read_dir(dir) {
        Ok(children) => children,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut found = Vec::new();
    for child in children {
        let child = child?;
        let Some(id) = child
            .file_name()
            .to_str()
            .and_then(|name| Uuid::parse_str(name).ok())
        else {
            continue;
        };
        if child.file_type()?.is_dir() {
            found.push((id, child.path()));
        }
    }

    found.sort();
    Ok(found)
}

/// Slot clones under the checkout root, as `(lane, path)`
///
/// The layout is `<root>/<remote clone key>/<lane>`. Names that start with a
/// dot, such as the pass's own trash directory, are skipped
pub(super) fn slot_clones(root: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    let mut found = Vec::new();
    for remote in visible_dirs(root)? {
        for lane in visible_dirs(&remote)? {
            let Some(name) = lane.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            found.push((name.to_owned(), lane.clone()));
        }
    }

    found.sort();
    Ok(found)
}

fn visible_dirs(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let children = match fs::read_dir(dir) {
        Ok(children) => children,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut found = Vec::new();
    for child in children {
        let child = child?;
        let hidden = child.file_name().to_string_lossy().starts_with('.');
        if !hidden && child.file_type()?.is_dir() {
            found.push(child.path());
        }
    }

    Ok(found)
}

/// Whether a process holds the flock on `lock`
///
/// A worker holds its task's `runner.lock` for as long as it runs. A missing
/// lock file is not held
pub(super) fn lock_held(lock: &Path) -> bool {
    let Ok(file) = File::open(lock) else {
        return false;
    };
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        // dropping the guard releases the probe's own lock at once
        Ok(_guard) => false,
        Err((_, errno)) => errno == nix::errno::Errno::EWOULDBLOCK,
    }
}

/// Remove `path` and everything under it; a path already gone is removed
///
/// `remove_dir_all` never follows a symlink out of the tree
pub(super) fn remove(path: &Path) -> io::Result<()> {
    match fs::remove_dir_all(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

/// The directory slot clones are moved into before removal, under the
/// checkout root so the move is a rename on the same filesystem
pub(super) fn trash_dir(checkout_root: &Path) -> PathBuf {
    checkout_root.join(".homebased-gc")
}

/// Remove whatever an interrupted pass left in the trash directory
pub(super) fn empty_trash(trash: &Path) -> io::Result<()> {
    remove(trash)
}
