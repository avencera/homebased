//! One-commit git bundles that carry a pool job's code to its machine
//!
//! A pool machine holds no git credential. At submit, the coordinator
//! fetches the job's commit from its remote with its own credentials into a
//! shallow mirror of the pool, and writes a bundle of that one commit with
//! no history and no other refs. The machine the job is placed on downloads
//! the bundle over the Fleet link when the run starts and checks the commit
//! out from it. Jobs on the same commit share one bundle. See
//! `docs/temporary-machines.md`

use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{PoolCode, PoolError, PoolName};
use crate::error::AppError;
use crate::home::Home;
use crate::queue::ci::{CommitId, GitCheckout};

/// Largest bundle a pool job may have; a bigger one is refused at submit
pub const MAX_BUNDLE_BYTES: u64 = 2 << 30;

/// Time limit for the coordinator's fetch of one commit
const FETCH_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Time limit for writing one bundle
const BUNDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Time limit for a git command that only reads or writes a ref
const QUERY_TIMEOUT: Duration = Duration::from_secs(60);

/// Most bytes of git's error output a refusal quotes
const STDERR_TAIL: usize = 600;

/// A bundle ready to serve, with what its download is checked against
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleFile {
    /// Where the bundle is
    #[serde(skip)]
    pub path: PathBuf,
    /// Its size in bytes
    pub len: u64,
    /// Lowercase hex sha256 of its bytes
    pub sha256: String,
}

/// The bundles of one pool's jobs on its coordinator and the mirrors they
/// are made from, under `pools/<pool>` in the state directory
///
/// The directory goes once the pool is released, so its bundles live only
/// while its jobs may still need them
#[derive(Debug, Clone)]
pub struct PoolBundles {
    root: PathBuf,
}

impl PoolBundles {
    /// The bundles of `pool` in `home`
    #[must_use]
    pub fn new(home: &Home, pool: &PoolName) -> Self {
        Self {
            root: home.root().join("pools").join(pool.class().as_str()),
        }
    }

    /// The finished bundle of `commit`, if one was made
    ///
    /// A bundle counts only once its sidecar names its size and digest, and
    /// the sidecar is written after the bundle, so a bundle cut short by a
    /// crash is never served
    pub fn bundle(&self, commit: &CommitId) -> io::Result<Option<BundleFile>> {
        let sidecar = match std::fs::read(self.sidecar_path(commit)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut file: BundleFile = serde_json::from_slice(&sidecar)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        file.path = self.bundle_path(commit);
        Ok(Some(file))
    }

    /// The bundle of `git`'s commit, fetched and written now if this pool has none
    ///
    /// Refuses with `commit_unavailable` when the remote does not give the
    /// commit, `submodules_unsupported` when its tree has a `.gitmodules`,
    /// and `bundle_too_large` past [`MAX_BUNDLE_BYTES`]. The caller holds
    /// the pool's bundle lock, since one mirror takes one fetch at a time
    pub async fn prepare(&self, git: &GitCheckout) -> Result<BundleFile, AppError> {
        if let Some(file) = self.bundle(&git.commit)? {
            return Ok(file);
        }
        let commit = git.commit.as_str();
        let mirror = self.mirror(git).await?;
        if !has_commit(&mirror, commit).await? {
            fetch(&mirror, git).await?;
        }
        if !has_commit(&mirror, commit).await? {
            return Err(PoolError::new(
                PoolCode::CommitUnavailable,
                format!("{commit} from {} is not a commit", git.remote.as_str()),
            )
            .into());
        }
        let gitmodules = format!("{commit}:.gitmodules");
        if run_git(&mirror, &["cat-file", "-e", &gitmodules], QUERY_TIMEOUT)
            .await?
            .success
        {
            return Err(PoolError::new(
                PoolCode::SubmodulesUnsupported,
                format!(
                    "commit {commit} has submodules, which pool machines cannot fetch \
                     without git credentials; vendor them or run the job on the Fleet"
                ),
            )
            .into());
        }
        self.write_bundle(&mirror, &git.commit).await
    }

    /// Remove every bundle and mirror of the pool
    pub fn remove(&self) -> io::Result<()> {
        match std::fs::remove_dir_all(&self.root) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }

    fn bundle_path(&self, commit: &CommitId) -> PathBuf {
        self.root
            .join("bundles")
            .join(format!("{}.bundle", commit.as_str()))
    }

    fn sidecar_path(&self, commit: &CommitId) -> PathBuf {
        self.root
            .join("bundles")
            .join(format!("{}.json", commit.as_str()))
    }

    /// The pool's bare mirror of `git`'s remote, created empty on first use
    ///
    /// It only ever gets commits fetched at depth 1, so every commit in it is
    /// a shallow root and a bundle of one holds that commit alone
    async fn mirror(&self, git: &GitCheckout) -> Result<PathBuf, AppError> {
        // a commit id's length names its object format, and a mirror has one,
        // so a wrong-length id for a remote never spoils the mirror its real
        // commits use
        let format = match git.commit.as_str().len() {
            64 => "sha256",
            _ => "sha1",
        };
        let mirror = self
            .root
            .join("git")
            .join(format!("{}-{format}.git", git.remote.clone_key()));
        if mirror.join("HEAD").is_file() {
            return Ok(mirror);
        }
        std::fs::create_dir_all(&mirror)?;
        let format = format!("--object-format={format}");
        let init = run_git(&mirror, &["init", "-q", "--bare", &format], QUERY_TIMEOUT).await?;
        if !init.success {
            return Err(AppError::Internal {
                message: format!("git init {}: {}", mirror.display(), init.stderr_tail()),
            });
        }
        Ok(mirror)
    }

    /// Write the bundle of `commit` from `mirror`, then its sidecar
    async fn write_bundle(&self, mirror: &Path, commit: &CommitId) -> Result<BundleFile, AppError> {
        let path = self.bundle_path(commit);
        let dir = path.parent().unwrap_or(&self.root).to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let partial = path.with_extension("bundle.part");
        // a bundle names refs, not commits, so the commit gets a ref of its own
        // for as long as the bundle takes; the pool's lock keeps it unique
        let reference = format!("refs/homebased/bundle/{}", commit.as_str());
        let set = run_git(
            mirror,
            &["update-ref", &reference, commit.as_str()],
            QUERY_TIMEOUT,
        )
        .await?;
        if !set.success {
            return Err(AppError::Internal {
                message: format!("git update-ref: {}", set.stderr_tail()),
            });
        }
        let partial_text = partial.display().to_string();
        let created = run_git(
            mirror,
            &["bundle", "create", "-q", &partial_text, &reference],
            BUNDLE_TIMEOUT,
        )
        .await;
        let _ = run_git(mirror, &["update-ref", "-d", &reference], QUERY_TIMEOUT).await;
        let created = created?;
        if !created.success {
            let _ = std::fs::remove_file(&partial);
            return Err(AppError::Internal {
                message: format!("git bundle create: {}", created.stderr_tail()),
            });
        }
        let digest = tokio::task::spawn_blocking({
            let partial = partial.clone();
            move || file_digest(&partial)
        })
        .await
        .map_err(|error| AppError::Internal {
            message: format!("bundle digest thread: {error}"),
        })??;
        if digest.len > MAX_BUNDLE_BYTES {
            let _ = std::fs::remove_file(&partial);
            return Err(PoolError::new(
                PoolCode::BundleTooLarge,
                format!(
                    "the bundle of commit {} is {} bytes, more than the {MAX_BUNDLE_BYTES} \
                     a pool machine takes",
                    commit.as_str(),
                    digest.len
                ),
            )
            .into());
        }
        std::fs::rename(&partial, &path)?;
        let sidecar = self.sidecar_path(commit);
        let sidecar_partial = sidecar.with_extension("json.part");
        std::fs::write(&sidecar_partial, serde_json::to_vec(&digest)?)?;
        std::fs::rename(&sidecar_partial, &sidecar)?;
        Ok(BundleFile { path, ..digest })
    }
}

/// `POST /v1/ci/bundles` answer, also for one attempt that failed but may
/// work when tried again
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum FetchAnswer {
    /// The bundle is at `path`, checked
    Ready {
        /// Public API version
        api_version: u32,
        /// The checked bundle
        path: PathBuf,
    },
    /// This attempt failed in a way another attempt may not
    Retry {
        /// Public API version
        api_version: u32,
        /// What went wrong
        message: String,
    },
}

/// Size and sha256 of the file at `path`
pub fn file_digest(path: &Path) -> io::Result<BundleFile> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1 << 16];
    let mut len = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        len += read as u64;
    }
    let sha256 = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(BundleFile {
        path: path.to_path_buf(),
        len,
        sha256,
    })
}

async fn has_commit(mirror: &Path, commit: &str) -> Result<bool, AppError> {
    let object = format!("{commit}^{{commit}}");
    Ok(run_git(mirror, &["cat-file", "-e", &object], QUERY_TIMEOUT)
        .await?
        .success)
}

/// Fetch only `git`'s commit, with no history and no tags
async fn fetch(mirror: &Path, git: &GitCheckout) -> Result<(), AppError> {
    let fetched = run_git(
        mirror,
        &[
            "fetch",
            "-q",
            "--no-tags",
            "--no-write-fetch-head",
            "--depth=1",
            git.remote.as_str(),
            git.commit.as_str(),
        ],
        FETCH_TIMEOUT,
    )
    .await;
    let reason = match fetched {
        Ok(run) if run.success => return Ok(()),
        Ok(run) => run.stderr_tail(),
        Err(error) => error.to_string(),
    };
    Err(PoolError::new(
        PoolCode::CommitUnavailable,
        format!(
            "the coordinator could not fetch commit {} from {}: {reason}; push it, or \
             check the coordinator's git credentials for that remote",
            git.commit.as_str(),
            git.remote.as_str(),
        ),
    )
    .into())
}

/// What one git command printed
struct GitRun {
    success: bool,
    stderr: String,
}

impl GitRun {
    /// The end of git's error output, on one line
    fn stderr_tail(&self) -> String {
        let text = self.stderr.split_whitespace().collect::<Vec<_>>().join(" ");
        let start = text.len().saturating_sub(STDERR_TAIL);
        let start = (start..text.len())
            .find(|index| text.is_char_boundary(*index))
            .unwrap_or(text.len());
        text[start..].to_owned()
    }
}

/// Run git in `dir`, never prompting, killed past `limit`
async fn run_git(dir: &Path, args: &[&str], limit: Duration) -> Result<GitRun, AppError> {
    let child = tokio::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        // a prompt would wait on a terminal the daemon does not have
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| AppError::Internal {
            message: format!("run git: {error}"),
        })?;
    let output = tokio::time::timeout(limit, child.wait_with_output())
        .await
        .map_err(|_| AppError::Internal {
            message: format!(
                "git {} took longer than {}",
                args.first().unwrap_or(&""),
                humantime::format_duration(limit)
            ),
        })??;
    Ok(GitRun {
        success: output.status.success(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}
