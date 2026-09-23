//! Path resolution and directory listing.

use std::cmp::Ordering;
use std::fs::{self, Metadata};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::files::token::{PathToken, decode_path, encode_path, name_display, path_display};

/// Kind of a navigable directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    /// Directory.
    Directory,
    /// Regular file.
    File,
    /// Symbolic link (target kind is resolved for navigation when possible).
    Symlink,
    /// Other (shown in listings but not opened as content).
    Other,
}

/// One entry in a directory listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileEntry {
    /// Lossy display name.
    pub name: String,
    /// Entry kind.
    pub kind: EntryKind,
    /// Resolved target kind for a symbolic link.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_kind: Option<EntryKind>,
    /// Opaque token for this entry's path.
    pub token: PathToken,
    /// Exact UTF-8 path for mirrored content URLs. Absent for non-UTF-8 paths.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_path: Option<String>,
    /// Size in bytes for regular files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Modification time when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified: Option<DateTime<Utc>>,
}

/// Resolved absolute path metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolvedPath {
    /// Schema version.
    pub api_version: u32,
    /// Path the caller entered.
    pub requested: String,
    /// Canonical resolved path when it differs from `requested`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved: Option<String>,
    /// Entry kind after following the final symlink for metadata.
    pub kind: EntryKind,
    /// Opaque token for the resolved path.
    pub token: PathToken,
    /// Exact UTF-8 path for mirrored content URLs. Absent for non-UTF-8 paths.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_path: Option<String>,
    /// Size for regular files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Modification time when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified: Option<DateTime<Utc>>,
}

/// `GET /v1/files/{token}` directory body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DirectoryListing {
    /// Schema version.
    pub api_version: u32,
    /// Display path of the directory.
    pub path: String,
    /// Token for this directory.
    pub token: PathToken,
    /// Parent directory token, absent at filesystem root.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<PathToken>,
    /// Sorted entries.
    pub entries: Vec<FileEntry>,
}

/// `POST /v1/files/resolve` body.
#[derive(Debug, Clone, Deserialize)]
pub struct ResolveBody {
    /// Absolute UTF-8 path.
    pub path: String,
}

/// Content-origin discovery for the web client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentOriginBody {
    /// Schema version.
    pub api_version: u32,
    /// TCP port of the content listener on the same host as the dashboard.
    pub port: u16,
}

/// Resolve a user-entered absolute UTF-8 path.
pub fn resolve_absolute_path(raw: &str) -> Result<ResolvedPath, AppError> {
    if raw.is_empty() {
        return Err(AppError::Usage {
            message: "path must not be blank".into(),
        });
    }
    let requested = raw;
    let path = Path::new(requested);
    if !path.is_absolute() {
        return Err(AppError::Usage {
            message: "path must be absolute".into(),
        });
    }
    let meta = symlink_metadata(path)?;
    if !(meta.is_dir() || meta.is_file() || meta.file_type().is_symlink()) {
        return reject_special(&meta);
    }
    let resolved_path = canonicalize_existing(path)?;
    let resolved_meta = metadata(&resolved_path)?;
    if !(resolved_meta.is_dir() || resolved_meta.is_file()) {
        return Err(AppError::UnsupportedFile {
            message: format!(
                "{} is not a regular file or directory",
                path_display(&resolved_path)
            ),
        });
    }
    let kind = classify_meta(&resolved_meta, meta.file_type().is_symlink());
    let requested_display = path_display(path);
    let resolved_display = path_display(&resolved_path);
    Ok(ResolvedPath {
        api_version: API_VERSION,
        requested: requested_display.clone(),
        resolved: if requested_display == resolved_display {
            None
        } else {
            Some(resolved_display)
        },
        kind,
        token: encode_path(&resolved_path),
        content_path: resolved_path.to_str().map(str::to_owned),
        size: size_for(&resolved_meta, kind),
        modified: modified_at(&resolved_meta),
    })
}

/// List one directory identified by an opaque token.
pub fn list_directory(token: &PathToken) -> Result<DirectoryListing, AppError> {
    let path = decode_path(token)?;
    let meta = metadata(&path)?;
    if !meta.is_dir() {
        return Err(AppError::NotDirectory {
            message: format!("{} is not a directory", path_display(&path)),
        });
    }
    let read_dir = fs::read_dir(&path).map_err(map_io(&path))?;
    let mut entries = Vec::new();
    for entry in read_dir {
        let entry = entry.map_err(|err| map_changed(&path, err))?;
        let name = entry.file_name();
        if is_dot_or_dotdot(&name) {
            continue;
        }
        let entry_path = entry.path();
        let file_type = entry.file_type().map_err(|err| map_changed(&path, err))?;
        let (kind, target_kind, size, modified) = if file_type.is_symlink() {
            match metadata(&entry_path) {
                Ok(target) => {
                    let target_kind = if target.is_dir() {
                        EntryKind::Directory
                    } else if target.is_file() {
                        EntryKind::File
                    } else {
                        EntryKind::Other
                    };
                    (
                        EntryKind::Symlink,
                        Some(target_kind),
                        size_for(&target, target_kind),
                        modified_at(&target)
                            .or_else(|| entry.metadata().ok().as_ref().and_then(modified_at)),
                    )
                }
                Err(_) => (
                    EntryKind::Symlink,
                    None,
                    None,
                    entry.metadata().ok().as_ref().and_then(modified_at),
                ),
            }
        } else if file_type.is_dir() {
            let meta = entry.metadata().map_err(|err| map_changed(&path, err))?;
            (EntryKind::Directory, None, None, modified_at(&meta))
        } else if file_type.is_file() {
            let meta = entry.metadata().map_err(|err| map_changed(&path, err))?;
            (EntryKind::File, None, Some(meta.len()), modified_at(&meta))
        } else {
            let meta = entry.metadata().ok();
            (
                EntryKind::Other,
                None,
                None,
                meta.as_ref().and_then(modified_at),
            )
        };
        entries.push(FileEntry {
            name: name_display(&name),
            kind,
            target_kind,
            token: encode_path(&entry_path),
            content_path: entry_path.to_str().map(str::to_owned),
            size,
            modified,
        });
    }
    entries.sort_by(cmp_entries);
    Ok(DirectoryListing {
        api_version: API_VERSION,
        path: path_display(&path),
        token: encode_path(&path),
        parent: parent_token(&path),
        entries,
    })
}

fn is_dot_or_dotdot(name: &std::ffi::OsStr) -> bool {
    name == std::ffi::OsStr::new(".") || name == std::ffi::OsStr::new("..")
}

fn cmp_entries(a: &FileEntry, b: &FileEntry) -> Ordering {
    let a_dir = effective_kind(a) == EntryKind::Directory;
    let b_dir = effective_kind(b) == EntryKind::Directory;
    match (a_dir, b_dir) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => a.name.cmp(&b.name),
    }
}

fn effective_kind(entry: &FileEntry) -> EntryKind {
    entry.target_kind.unwrap_or(entry.kind)
}

fn parent_token(path: &Path) -> Option<PathToken> {
    if path.parent().is_none() || path.as_os_str() == "/" {
        None
    } else {
        path.parent().map(encode_path)
    }
}

fn classify_meta(meta: &Metadata, via_symlink: bool) -> EntryKind {
    if meta.is_dir() {
        EntryKind::Directory
    } else if meta.is_file() {
        EntryKind::File
    } else if via_symlink {
        EntryKind::Symlink
    } else {
        EntryKind::Other
    }
}

fn size_for(meta: &Metadata, kind: EntryKind) -> Option<u64> {
    match kind {
        EntryKind::File => Some(meta.len()),
        EntryKind::Directory | EntryKind::Symlink | EntryKind::Other => None,
    }
}

fn modified_at(meta: &Metadata) -> Option<DateTime<Utc>> {
    meta.modified().ok().and_then(|time| {
        let duration = time.duration_since(SystemTime::UNIX_EPOCH).ok()?;
        DateTime::from_timestamp(
            i64::try_from(duration.as_secs()).ok()?,
            duration.subsec_nanos(),
        )
    })
}

fn symlink_metadata(path: &Path) -> Result<Metadata, AppError> {
    fs::symlink_metadata(path).map_err(map_io(path))
}

fn metadata(path: &Path) -> Result<Metadata, AppError> {
    fs::metadata(path).map_err(map_io(path))
}

fn canonicalize_existing(path: &Path) -> Result<PathBuf, AppError> {
    fs::canonicalize(path).map_err(map_io(path))
}

fn reject_special<T>(meta: &Metadata) -> Result<T, AppError> {
    let ft = meta.file_type();
    if ft.is_block_device() || ft.is_char_device() || ft.is_fifo() || ft.is_socket() {
        return Err(AppError::UnsupportedFile {
            message: "special files are not browsable".into(),
        });
    }
    Err(AppError::UnsupportedFile {
        message: "unsupported file type".into(),
    })
}

fn map_io(path: &Path) -> impl Fn(std::io::Error) -> AppError + '_ {
    move |err| match err.kind() {
        std::io::ErrorKind::NotFound => AppError::FileNotFound {
            message: format!("{} not found", path_display(path)),
        },
        std::io::ErrorKind::PermissionDenied => AppError::Permission {
            message: format!("permission denied: {}", path_display(path)),
        },
        _ => AppError::Internal {
            message: format!("{}: {err}", path_display(path)),
        },
    }
}

fn map_changed(path: &Path, err: std::io::Error) -> AppError {
    match err.kind() {
        std::io::ErrorKind::NotFound => AppError::ChangedDuringRead {
            message: format!("{} changed during read", path_display(path)),
        },
        std::io::ErrorKind::PermissionDenied => AppError::Permission {
            message: format!("permission denied: {}", path_display(path)),
        },
        _ => AppError::Internal {
            message: format!("{}: {err}", path_display(path)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(target_os = "macos"))]
    use std::ffi::OsString;
    #[cfg(not(target_os = "macos"))]
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::symlink;
    use tempfile::tempdir;

    #[test]
    fn lists_root_and_nested_with_sort_and_parent() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::create_dir(root.join("b_dir")).unwrap();
        fs::create_dir(root.join("a_dir")).unwrap();
        fs::write(root.join("z.txt"), b"z").unwrap();
        fs::write(root.join("a.txt"), b"a").unwrap();
        let resolved = resolve_absolute_path(&path_display(root)).unwrap();
        assert_eq!(resolved.kind, EntryKind::Directory);
        let listing = list_directory(&resolved.token).unwrap();
        let names: Vec<_> = listing.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["a_dir", "b_dir", "a.txt", "z.txt"]);
        assert!(listing.parent.is_some());
        let nested = list_directory(&listing.entries[0].token).unwrap();
        assert_eq!(
            nested.parent.as_ref().unwrap().as_str(),
            listing.token.as_str()
        );
    }

    #[test]
    fn follows_symlinks_and_reports_resolved() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("target");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("f.txt"), b"hi").unwrap();
        let link = dir.path().join("link");
        symlink(&target, &link).unwrap();
        let resolved = resolve_absolute_path(&path_display(&link)).unwrap();
        assert_eq!(resolved.kind, EntryKind::Directory);
        assert_eq!(
            resolved.resolved.as_deref(),
            Some(path_display(&fs::canonicalize(&target).unwrap()).as_str())
        );

        let parent = resolve_absolute_path(&path_display(dir.path())).unwrap();
        let listing = list_directory(&parent.token).unwrap();
        let entry = listing
            .entries
            .iter()
            .find(|entry| entry.name == "link")
            .unwrap();
        assert_eq!(entry.kind, EntryKind::Symlink);
        assert_eq!(entry.target_kind, Some(EntryKind::Directory));
    }

    #[test]
    fn rejects_relative_and_missing() {
        assert!(matches!(
            resolve_absolute_path("relative"),
            Err(AppError::Usage { .. })
        ));
        assert!(matches!(
            resolve_absolute_path("/no/such/homebased/path"),
            Err(AppError::FileNotFound { .. })
        ));
    }

    #[test]
    fn empty_file_has_zero_size() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("empty");
        fs::write(&file, b"").unwrap();
        let resolved = resolve_absolute_path(&path_display(&file)).unwrap();
        assert_eq!(resolved.kind, EntryKind::File);
        assert_eq!(resolved.size, Some(0));
    }

    #[test]
    fn preserves_whitespace_path_identity() {
        let dir = tempdir().unwrap();
        let spaced = dir.path().join(" name with spaces ");
        fs::write(&spaced, b"spaces").unwrap();
        let resolved = resolve_absolute_path(spaced.to_str().unwrap()).unwrap();
        assert_eq!(resolved.requested, spaced.to_str().unwrap());
        assert_eq!(
            decode_path(&resolved.token).unwrap(),
            fs::canonicalize(&spaced).unwrap()
        );
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn preserves_non_utf8_path_identity() {
        let dir = tempdir().unwrap();
        let native_name = OsString::from_vec(b"native-\xff.txt".to_vec());
        let native_path = dir.path().join(&native_name);
        fs::write(&native_path, b"native").unwrap();
        let canonical_native_path = fs::canonicalize(&native_path).unwrap();
        let root = resolve_absolute_path(dir.path().to_str().unwrap()).unwrap();
        let listing = list_directory(&root.token).unwrap();
        let entry = listing
            .entries
            .iter()
            .find(|entry| decode_path(&entry.token).unwrap() == canonical_native_path)
            .unwrap();
        assert_eq!(entry.kind, EntryKind::File);
        assert_eq!(entry.content_path, None);
    }
}
