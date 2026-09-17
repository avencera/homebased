//! State directory resolution and task directory layout.

use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use crate::domain::TaskId;
use crate::error::AppError;

/// Unix socket file name under `$HOMEBASED_HOME`.
pub const SOCK_NAME: &str = "homebased.sock";
/// SQLite database file name under `$HOMEBASED_HOME`.
pub const DB_NAME: &str = "homebased.sqlite";
/// Daemon singleton lock file name under `$HOMEBASED_HOME`.
pub const DAEMON_LOCK: &str = "daemon.lock";
/// Log that records callbacks `codex queue` could not deliver.
pub const FALLBACK_LOG: &str = "callback-fallback.log";

/// Resolved state root plus helpers for the on-disk layout.
#[derive(Debug, Clone)]
pub struct Home {
    root: PathBuf,
}

impl Home {
    /// Resolve `--home` / `HOMEBASED_HOME` / `$XDG_STATE_HOME/homebased` / `~/.local/state/homebased`.
    pub fn resolve(cli_home: Option<PathBuf>) -> Result<Self, AppError> {
        if let Some(path) = cli_home {
            return Ok(Self { root: path });
        }
        if let Ok(path) = std::env::var("HOMEBASED_HOME")
            && !path.is_empty()
        {
            return Ok(Self {
                root: PathBuf::from(path),
            });
        }
        if let Ok(path) = std::env::var("XDG_STATE_HOME")
            && !path.is_empty()
        {
            return Ok(Self {
                root: PathBuf::from(path).join("homebased"),
            });
        }
        let home = std::env::var("HOME").map_err(|_| AppError::Internal {
            message: "HOME is unset".into(),
        })?;
        Ok(Self {
            root: PathBuf::from(home).join(".local/state/homebased"),
        })
    }

    /// Create the root directory if needed.
    pub fn ensure(&self) -> Result<(), AppError> {
        fs::create_dir_all(self.tasks_dir())?;
        Ok(())
    }

    /// State root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Unix socket path.
    #[must_use]
    pub fn sock_path(&self) -> PathBuf {
        self.root.join(SOCK_NAME)
    }

    /// SQLite path.
    #[must_use]
    pub fn db_path(&self) -> PathBuf {
        self.root.join(DB_NAME)
    }

    /// Daemon lock path.
    #[must_use]
    pub fn daemon_lock_path(&self) -> PathBuf {
        self.root.join(DAEMON_LOCK)
    }

    /// Fallback log for failed `codex queue` attempts.
    #[must_use]
    pub fn fallback_log_path(&self) -> PathBuf {
        self.root.join(FALLBACK_LOG)
    }

    /// `tasks/` directory.
    #[must_use]
    pub fn tasks_dir(&self) -> PathBuf {
        self.root.join("tasks")
    }

    /// Directory for one task.
    #[must_use]
    pub fn task_dir(&self, id: TaskId) -> PathBuf {
        self.tasks_dir().join(id.to_string())
    }

    /// Create the task directory and return its layout.
    pub fn prepare_task(&self, id: TaskId) -> Result<TaskPaths, AppError> {
        let paths = TaskPaths::new(self.task_dir(id));
        fs::create_dir_all(&paths.dir)?;
        Ok(paths)
    }

    /// Layout for an existing task.
    #[must_use]
    pub fn task_paths(&self, id: TaskId) -> TaskPaths {
        TaskPaths::new(self.task_dir(id))
    }
}

/// Upper bound on `tail`, so one request cannot pull an unbounded log into
/// memory or into a JSON body.
pub const MAX_TAIL_LINES: usize = 5000;

/// Text of `output.log`, whole or tailed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputTail {
    /// Log text. Tailed text is the kept lines joined with `\n` and carries no
    /// trailing newline.
    pub text: String,
    /// Whether earlier lines were dropped.
    pub truncated: bool,
}

/// Files inside `tasks/<id>/`.
#[derive(Debug, Clone)]
pub struct TaskPaths {
    /// Task directory.
    pub dir: PathBuf,
    /// Caller's prompt bytes, unchanged.
    pub prompt: PathBuf,
    /// Fixed reporting trailer.
    pub trailer: PathBuf,
    /// Concatenation fed to the child (prompt + optional trailer).
    pub feed: PathBuf,
    /// Combined stdout/stderr of the agent.
    pub output: PathBuf,
    /// Exclusive flock held by `task-run`.
    pub runner_lock: PathBuf,
    /// Worker evidence of exit.
    pub exit_json: PathBuf,
    /// `codex queue` stdout/stderr.
    pub callback_log: PathBuf,
}

impl TaskPaths {
    fn new(dir: PathBuf) -> Self {
        Self {
            prompt: dir.join("prompt.txt"),
            trailer: dir.join("prompt.trailer.txt"),
            feed: dir.join("prompt.feed.txt"),
            output: dir.join("output.log"),
            runner_lock: dir.join("runner.lock"),
            exit_json: dir.join("exit.json"),
            callback_log: dir.join("callback.log"),
            dir,
        }
    }

    /// Write prompt and trailer files. `prompt.txt` is byte-identical to `prompt`.
    pub fn write_prompt(&self, prompt: &str, trailer: Option<&str>) -> Result<(), AppError> {
        fs::write(&self.prompt, prompt.as_bytes())?;
        let feed = match trailer {
            Some(text) => {
                fs::write(&self.trailer, text.as_bytes())?;
                let mut bytes = prompt.as_bytes().to_vec();
                if !bytes.ends_with(b"\n") {
                    bytes.push(b'\n');
                }
                bytes.extend_from_slice(text.as_bytes());
                if !text.ends_with('\n') {
                    bytes.push(b'\n');
                }
                bytes
            }
            None => prompt.as_bytes().to_vec(),
        };
        fs::write(&self.feed, feed)?;
        Ok(())
    }

    /// Read `output.log`, keeping only the last `tail` lines when asked.
    /// `tail` is capped at [`MAX_TAIL_LINES`]. `Ok(None)` means the file does
    /// not exist: either the agent has written nothing yet, or the task id has
    /// no directory.
    ///
    /// Agent output is arbitrary bytes, so invalid UTF-8 is replaced rather
    /// than rejected.
    pub fn read_output(&self, tail: Option<usize>) -> Result<Option<OutputTail>, AppError> {
        let bytes = match fs::read(&self.output) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let Some(tail) = tail else {
            return Ok(Some(OutputTail {
                text,
                truncated: false,
            }));
        };
        let lines: Vec<&str> = text.lines().collect();
        let start = lines.len().saturating_sub(tail.min(MAX_TAIL_LINES));
        Ok(Some(OutputTail {
            text: lines[start..].join("\n"),
            truncated: start > 0,
        }))
    }
}

/// Whether `flock_exclusive` waits for a held lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Wait until the lock is free.
    Blocking,
    /// Fail with `AppError::LockHeld` when another process holds the lock.
    NonBlocking,
}

/// Open (or create) a file and take an exclusive `flock`. The caller keeps the
/// returned `File`; the lock lives as long as any descriptor on the same open
/// file description, so a spawned child keeps it after the parent closes.
pub fn flock_exclusive(path: &Path, mode: LockMode) -> Result<File, AppError> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    let arg = match mode {
        LockMode::Blocking => nix::fcntl::FlockArg::LockExclusive,
        LockMode::NonBlocking => nix::fcntl::FlockArg::LockExclusiveNonblock,
    };
    match flock_raw(&file, arg) {
        Ok(()) => Ok(file),
        Err(nix::errno::Errno::EAGAIN | nix::errno::Errno::EWOULDBLOCK)
            if mode == LockMode::NonBlocking =>
        {
            Err(AppError::LockHeld {
                path: path.to_path_buf(),
            })
        }
        Err(err) => Err(AppError::Internal {
            message: format!("flock {}: {err}", path.display()),
        }),
    }
}

// `nix::fcntl::Flock` releases the lock in `Drop`, which would drop the lock the
// spawned worker inherits when the daemon closes its own descriptor. The
// deprecated free function keeps close-on-drop semantics
#[allow(deprecated)]
fn flock_raw(file: &File, arg: nix::fcntl::FlockArg) -> nix::Result<()> {
    nix::fcntl::flock(file.as_raw_fd(), arg)
}

/// Set a path to mode 0600.
pub fn chmod_600(path: &Path) -> Result<(), AppError> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env(pairs: &[(&str, Option<&str>)], f: impl FnOnce()) {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut old: Vec<(&str, Option<OsString>)> = Vec::new();
        for (key, value) in pairs {
            old.push((*key, std::env::var_os(key)));
            set_env(key, value.map(OsString::from).as_deref());
        }
        f();
        for (key, value) in old {
            set_env(key, value.as_deref());
        }
    }

    fn set_env(key: &str, value: Option<&std::ffi::OsStr>) {
        // SAFETY: every caller holds ENV_LOCK, so no other test thread reads or
        // writes the process environment while it is mutated here
        unsafe {
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }

    #[test]
    fn cli_home_wins() {
        let home = Home::resolve(Some(PathBuf::from("/tmp/hb"))).unwrap();
        assert_eq!(home.root(), Path::new("/tmp/hb"));
    }

    #[test]
    fn env_homebased_home() {
        with_env(
            &[
                ("HOMEBASED_HOME", Some("/tmp/from-env")),
                ("XDG_STATE_HOME", Some("/tmp/xdg")),
            ],
            || {
                let home = Home::resolve(None).unwrap();
                assert_eq!(home.root(), Path::new("/tmp/from-env"));
            },
        );
    }

    #[test]
    fn xdg_state_home() {
        with_env(
            &[
                ("HOMEBASED_HOME", None),
                ("XDG_STATE_HOME", Some("/tmp/xdg")),
            ],
            || {
                let home = Home::resolve(None).unwrap();
                assert_eq!(home.root(), Path::new("/tmp/xdg/homebased"));
            },
        );
    }

    #[test]
    fn default_under_local_state() {
        with_env(
            &[
                ("HOMEBASED_HOME", None),
                ("XDG_STATE_HOME", None),
                ("HOME", Some("/tmp/user")),
            ],
            || {
                let home = Home::resolve(None).unwrap();
                assert_eq!(home.root(), Path::new("/tmp/user/.local/state/homebased"));
            },
        );
    }

    #[test]
    fn task_layout_names() {
        let home = Home::resolve(Some(PathBuf::from("/tmp/hb"))).unwrap();
        let id: TaskId = "01234567-89ab-7cde-8f01-23456789abcd".parse().unwrap();
        let paths = home.task_paths(id);
        assert!(paths.prompt.ends_with("prompt.txt"));
        assert!(paths.trailer.ends_with("prompt.trailer.txt"));
        assert!(paths.output.ends_with("output.log"));
        assert!(paths.runner_lock.ends_with("runner.lock"));
        assert!(paths.exit_json.ends_with("exit.json"));
        assert!(paths.callback_log.ends_with("callback.log"));
    }

    #[test]
    fn flock_nonblock_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock");
        let held = flock_exclusive(&path, LockMode::Blocking).unwrap();
        let err = flock_exclusive(&path, LockMode::NonBlocking).unwrap_err();
        assert!(matches!(err, AppError::LockHeld { path: p } if p == path));
        drop(held);
        let _second = flock_exclusive(&path, LockMode::NonBlocking).unwrap();
    }
}
