//! State directory resolution and task directory layout.

use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use crate::domain::TaskId;
use crate::error::AppError;

/// Names under `$HOMEBASED_HOME`.
pub const SOCK_NAME: &str = "homebased.sock";
pub const DB_NAME: &str = "homebased.sqlite";
pub const DAEMON_LOCK: &str = "daemon.lock";
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
        if let Ok(path) = std::env::var("HOMEBASED_HOME") {
            if !path.is_empty() {
                return Ok(Self {
                    root: PathBuf::from(path),
                });
            }
        }
        if let Ok(path) = std::env::var("XDG_STATE_HOME") {
            if !path.is_empty() {
                return Ok(Self {
                    root: PathBuf::from(path).join("homebased"),
                });
            }
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
}

/// Open (or create) a file and take an exclusive `flock`.
pub fn flock_exclusive(path: &Path, nonblock: bool) -> Result<File, AppError> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    let op = if nonblock {
        libc::LOCK_EX | libc::LOCK_NB
    } else {
        libc::LOCK_EX
    };
    let rc = unsafe { libc::flock(file.as_raw_fd(), op) };
    if rc == 0 {
        return Ok(file);
    }
    let err = std::io::Error::last_os_error();
    if nonblock && matches!(err.kind(), std::io::ErrorKind::WouldBlock) {
        return Err(AppError::DaemonAlreadyRunning);
    }
    // Some platforms report EAGAIN rather than EWOULDBLOCK.
    if nonblock && err.raw_os_error() == Some(libc::EAGAIN) {
        return Err(AppError::DaemonAlreadyRunning);
    }
    Err(AppError::Internal {
        message: format!("flock {}: {err}", path.display()),
    })
}

/// Blocking exclusive flock. Returns the held file.
pub fn flock_exclusive_blocking(path: &Path) -> Result<File, AppError> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if rc == 0 {
        Ok(file)
    } else {
        Err(AppError::Internal {
            message: format!(
                "flock {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            ),
        })
    }
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
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
        f();
        for (key, value) in old {
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
        let held = flock_exclusive(&path, false).unwrap();
        let err = flock_exclusive(&path, true).unwrap_err();
        assert!(matches!(err, AppError::DaemonAlreadyRunning));
        drop(held);
        let _second = flock_exclusive(&path, true).unwrap();
    }
}
