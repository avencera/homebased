//! Git on the launching machine, for `t3 launch`: the repository a project
//! checks out, a worktree base that must exist before T3 is asked, and a
//! clone of a repository that has no project there yet
//!
//! - A repository is known by its [`RepoKey`]: the `origin` remote as
//!   `host/owner/repo`, lowercased, without scheme, user, port, trailing slash,
//!   or `.git`. This follows T3's own `repositoryKeyFromRemote`
//!   (`packages/shared/src/git.ts` in pingdotgg/t3code), except that T3 picks
//!   a primary remote among several and homebased reads `origin` only, which is
//!   the remote a person names a repository by
//! - The base check follows T3's `ThreadLaunchService.checkWorktreeBase`: the
//!   base resolves as a commit, or with `--from-origin` its branch is fetched
//!   from the remote first, as T3 does when it prepares the worktree; a
//!   repository with no commit yet passes
//!
//! Git runs with a deadline and without a terminal, so a credential or host key
//! prompt fails instead of hanging the daemon

use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

use super::T3Env;

const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest the base check's fetch of one branch may take, which T3 then
/// repeats; the launch request waits this much longer
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
/// Longest a clone may take; the launch request waits this much longer
pub const CLONE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const OUTPUT_LIMIT: u64 = 64 * 1024;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
// enough of git's stderr to say why a clone failed
const STDERR_CHARS: usize = 400;

/// A repository as remotes name it: `host/owner/repo`, lowercased
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepoKey(String);

impl RepoKey {
    /// The key of a remote URL in SSH, scp, HTTPS, or git form; `None` for a
    /// local path or a URL with fewer than two path segments
    #[must_use]
    pub fn from_remote(url: &str) -> Option<Self> {
        let (host, path) = remote_parts(url)?;
        Some(Self(format!("{host}/{path}").to_lowercase()))
    }

    /// The host, such as `github.com`
    #[must_use]
    pub fn host(&self) -> &str {
        self.0.split_once('/').map_or(&self.0, |(host, _)| host)
    }

    /// The path after the host, such as `owner/repo`
    #[must_use]
    pub fn path(&self) -> &str {
        self.0.split_once('/').map_or("", |(_, path)| path)
    }

    /// `host/owner/repo`
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Host and path of a remote URL, in the case given, the path without `.git`
/// or slashes around it
fn remote_parts(url: &str) -> Option<(&str, String)> {
    let url = url.trim();
    let (host, path) = match url.split_once("://") {
        Some((_, rest)) => {
            let (authority, path) = rest.split_once('/')?;
            (url_host(authority)?, path)
        }
        None => scp_parts(url)?,
    };
    Some((host, repo_path(host, path)?))
}

/// The path of a repository on `host`: two segments or more, without `.git`
fn repo_path(host: &str, path: &str) -> Option<String> {
    let path = path.trim_end_matches('/');
    let path = match path.len().checked_sub(4) {
        Some(cut) if path.is_char_boundary(cut) && path[cut..].eq_ignore_ascii_case(".git") => {
            &path[..cut]
        }
        _ => path,
    };
    let segments: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    (!host.is_empty() && segments.len() >= 2).then(|| segments.join("/"))
}

/// Host of a URL authority, without user or port
fn url_host(authority: &str) -> Option<&str> {
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    match host.strip_prefix('[') {
        Some(bracketed) => Some(bracketed.split_once(']')?.0),
        None => Some(host.split_once(':').map_or(host, |(host, _)| host)),
    }
}

/// Host and path of an scp-style remote, `[user@]host:path`; a one-letter
/// host is a Windows drive, which T3 rejects too
fn scp_parts(url: &str) -> Option<(&str, &str)> {
    let (authority, path) = url.split_once(':')?;
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if host.len() < 2 || host.contains('/') || path.starts_with("//") {
        return None;
    }
    Some((host, path.trim_start_matches('/')))
}

/// A `--project` that names a repository rather than a T3 project
///
/// It keeps the case it was given, which names the clone's directory and
/// URL, and compares in lowercase, as [`RepoKey`] does
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoSelector {
    /// Host, when a URL or `host/owner/repo` names one
    host: Option<String>,
    /// `owner/repo`, or a longer path, without `.git`
    path: String,
    /// The URL as given, which a clone uses
    url: Option<String>,
}

impl RepoSelector {
    /// The repository `selector` names, or `None` when it is no repository
    /// name, such as a title with spaces
    #[must_use]
    pub fn parse(selector: &str) -> Option<Self> {
        let selector = selector.trim();
        if selector.starts_with('/') || selector.starts_with('~') {
            return None;
        }
        if selector.contains("://") || selector.contains(':') {
            let (host, path) = remote_parts(selector)?;
            return Some(Self {
                host: Some(host.to_owned()),
                path,
                url: Some(selector.to_owned()),
            });
        }

        let plain = |part: &str| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        };
        let trimmed = selector.trim_end_matches('/');
        let segments: Vec<&str> = trimmed.split('/').collect();
        if segments.len() < 2 || !segments.iter().all(|part| plain(part)) {
            return None;
        }
        // `github.com/owner/repo` names its host; `owner/repo` does not
        let (host, path) = match segments.as_slice() {
            [host, rest @ ..] if rest.len() >= 2 && host.contains('.') => {
                (Some((*host).to_owned()), rest.join("/"))
            }
            [_, _] => (None, trimmed.to_owned()),
            _ => return None,
        };
        let path = repo_path(host.as_deref().unwrap_or("any"), &path)?;
        Some(Self {
            host,
            path,
            url: None,
        })
    }

    /// Whether a project whose origin is `key` checks out this repository
    #[must_use]
    pub fn matches(&self, key: &RepoKey) -> bool {
        let host = self
            .host
            .as_ref()
            .is_none_or(|host| host.to_lowercase() == key.host());
        host && self.path.to_lowercase() == key.path()
    }

    /// The URL to clone, given the origin URLs of the projects already on
    /// the machine
    ///
    /// A URL clones as given. A name without one clones from GitHub, or from
    /// its own host, over SSH when a project on the machine already reaches
    /// that host over SSH, since then its keys are set up, else over HTTPS
    #[must_use]
    pub fn clone_url<'a>(&self, known: impl IntoIterator<Item = &'a str>) -> String {
        if let Some(url) = &self.url {
            return url.clone();
        }
        let host = self.host.as_deref().unwrap_or("github.com");
        let over_ssh = known.into_iter().any(|url| {
            let ssh = url.starts_with("ssh://") || !url.contains("://");
            ssh && RepoKey::from_remote(url).is_some_and(|key| key.host() == host.to_lowercase())
        });
        let path = &self.path;
        if over_ssh {
            format!("git@{host}:{path}.git")
        } else {
            format!("https://{host}/{path}.git")
        }
    }

    /// Directory name a clone gets: the repository's own, in its case
    #[must_use]
    pub fn name(&self) -> &str {
        self.path.rsplit('/').next().unwrap_or(&self.path)
    }
}

/// Whether a `--project` selector names a repository, as `owner/repo` or a URL
#[must_use]
pub fn names_repository(selector: &str) -> bool {
    RepoSelector::parse(selector).is_some()
}

/// Why a worktree base cannot be used
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BaseProblem {
    /// The project root is not in a git repository
    NotRepository,
    /// No commit, branch, or tag of that name resolves there
    Missing {
        /// Whether its branch was fetched from its remote first and still
        /// did not resolve
        fetched: bool,
    },
}

/// What [`Git::fetch_base`] found
enum Fetched {
    /// The branch was fetched and resolves now
    Found,
    /// The fetch ran, and the remote has no such branch
    Absent,
    /// Nothing was fetched: the base names no branch, or no such remote exists
    NotTried,
    /// The fetch could not run, such as when the remote did not answer in time
    Failed,
}

/// Git commands for one machine's user
pub struct Git<'a> {
    env: &'a T3Env,
}

/// What one git command printed
struct GitOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

impl<'a> Git<'a> {
    /// Git as the user of `env` runs it, or `None` when git is not installed
    #[must_use]
    pub fn find(env: &'a T3Env) -> Option<Self> {
        let cwd = env.home.clone();
        which::which_in("git", Some(&env.path), cwd).ok()?;
        Some(Self { env })
    }

    /// The `origin` URL of the repository at `root` as configured; `None`
    /// when there is none
    ///
    /// Read before `insteadOf` rewrites, which change how git reaches a
    /// repository, not which one it is
    #[must_use]
    pub fn origin_url(&self, root: &Path) -> Option<String> {
        let output = self
            .run(
                Some(root),
                &["config", "--get", "remote.origin.url"],
                QUERY_TIMEOUT,
            )
            .ok()?;
        let url = output.stdout.trim();
        (output.success && !url.is_empty()).then(|| url.to_owned())
    }

    /// Check that `base` resolves to a commit in the repository at `root`,
    /// fetching its branch from the remote first when `from_origin`
    ///
    /// A check that cannot run, such as one that times out, passes, so T3
    /// decides as it would have
    pub fn check_base(
        &self,
        root: &Path,
        base: &str,
        from_origin: bool,
    ) -> Result<(), BaseProblem> {
        // T3 takes a `:`-prefixed message search as valid, and `-` as the previous branch
        if base.starts_with(':') {
            return Ok(());
        }
        let base = if base == "-" { "@{-1}" } else { base };
        let Ok(inside) = self.run(Some(root), &["rev-parse", "--git-dir"], QUERY_TIMEOUT) else {
            return Ok(());
        };
        if !inside.success {
            return Err(BaseProblem::NotRepository);
        }
        if self.has_commit(root, base) {
            return Ok(());
        }
        let fetched = if from_origin {
            self.fetch_base(root, base)
        } else {
            Fetched::NotTried
        };
        // a fetch that cannot run passes like any check that cannot run
        if matches!(fetched, Fetched::Found | Fetched::Failed) {
            return Ok(());
        }
        // a repository without any commit lets T3 start from nothing
        if !self.has_commit(root, "HEAD") {
            return Ok(());
        }
        Err(BaseProblem::Missing {
            fetched: matches!(fetched, Fetched::Absent),
        })
    }

    fn has_commit(&self, root: &Path, rev: &str) -> bool {
        let spec = format!("{rev}^{{commit}}");
        let output = self.run(
            Some(root),
            &[
                "rev-parse",
                "--verify",
                "--quiet",
                "--end-of-options",
                &spec,
            ],
            QUERY_TIMEOUT,
        );
        // a check that cannot run passes, as above
        output.map_or(true, |output| output.success)
    }

    /// Fetch `base`'s branch from its remote into the remote-tracking ref, as
    /// T3 does, and say whether that ref now resolves
    fn fetch_base(&self, root: &Path, base: &str) -> Fetched {
        // a revision expression, such as `main~2`, names no remote branch
        if base == "HEAD"
            || base.starts_with('-')
            || base.contains(['~', '^', ':', '@', ' '])
            || base.contains("..")
        {
            return Fetched::NotTried;
        }
        let remotes = self
            .run(Some(root), &["remote"], QUERY_TIMEOUT)
            .ok()
            .filter(|output| output.success)
            .map(|output| output.stdout)
            .unwrap_or_default();
        let (remote, branch) = match base.split_once('/') {
            Some((remote, branch)) if remotes.lines().any(|line| line.trim() == remote) => {
                (remote, branch)
            }
            _ => ("origin", base),
        };
        if !remotes.lines().any(|line| line.trim() == remote) {
            return Fetched::NotTried;
        }
        let refspec = format!("+refs/heads/{branch}:refs/remotes/{remote}/{branch}");
        let fetched = self.run(
            Some(root),
            &["fetch", "--quiet", "--no-tags", "--", remote, &refspec],
            FETCH_TIMEOUT,
        );
        match fetched {
            Err(_) => Fetched::Failed,
            Ok(output) if !output.success => Fetched::Absent,
            Ok(_) if self.has_commit(root, &format!("refs/remotes/{remote}/{branch}")) => {
                Fetched::Found
            }
            Ok(_) => Fetched::Absent,
        }
    }

    /// Clone `url` into `into`, which must not exist
    ///
    /// # Errors
    ///
    /// Git's own reason, cut short, when the clone fails or times out
    pub fn clone_into(&self, url: &str, into: &Path) -> Result<(), String> {
        let into = into.to_string_lossy();
        let output = self.run(None, &["clone", "--quiet", "--", url, &into], CLONE_TIMEOUT)?;
        if output.success {
            return Ok(());
        }
        let reason: String = output.stderr.trim().chars().take(STDERR_CHARS).collect();
        Err(if reason.is_empty() {
            format!("git clone {url} failed")
        } else {
            format!("git clone {url} failed: {reason}")
        })
    }

    /// Run git in `dir`, or the home directory, and kill its process group at
    /// the deadline
    fn run(
        &self,
        dir: Option<&Path>,
        args: &[&str],
        timeout: Duration,
    ) -> Result<GitOutput, String> {
        let mut child = Command::new("git")
            .args(args)
            .current_dir(dir.unwrap_or(&self.env.home))
            .env("PATH", &self.env.path)
            .env("HOME", &self.env.home)
            // fail rather than wait for a credential prompt nobody answers
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .map_err(|error| format!("could not start git: {error}"))?;
        let stdout = collect(child.stdout.take());
        let stderr = collect(child.stderr.take());

        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("could not wait for git: {error}"))?
            {
                break status;
            }
            if Instant::now() >= deadline {
                if let Ok(pid) = i32::try_from(child.id()) {
                    let _ = kill(Pid::from_raw(-pid), Signal::SIGKILL);
                }
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "git {} timed out after {}s",
                    args.first().unwrap_or(&""),
                    timeout.as_secs()
                ));
            }
            thread::sleep(Duration::from_millis(20));
        };
        Ok(GitOutput {
            success: status.success(),
            stdout: drain(&stdout),
            stderr: drain(&stderr),
        })
    }
}

/// Read a pipe on its own thread, so a full pipe never blocks git
fn collect(pipe: Option<impl Read + Send + 'static>) -> mpsc::Receiver<Vec<u8>> {
    let (sender, receiver) = mpsc::channel();
    if let Some(pipe) = pipe {
        thread::spawn(move || {
            let mut bytes = Vec::new();
            let _ = pipe.take(OUTPUT_LIMIT).read_to_end(&mut bytes);
            let _ = sender.send(bytes);
        });
    }
    receiver
}

/// What a pipe carried, waiting briefly for a descendant that holds it open
fn drain(receiver: &mpsc::Receiver<Vec<u8>>) -> String {
    let bytes = receiver.recv_timeout(DRAIN_TIMEOUT).unwrap_or_default();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::{RepoKey, RepoSelector};

    #[test]
    fn remotes_of_one_repository_share_a_key_whatever_their_form() {
        let key = |url: &str| {
            RepoKey::from_remote(url).map(|key| format!("{}/{}", key.host(), key.path()))
        };
        let forms = [
            "git@github.com:Avencera/Homebased.git",
            "ssh://git@github.com:22/avencera/homebased",
            "https://github.com/avencera/homebased.git/",
            "https://user@GitHub.com/avencera/homebased",
            "git://github.com/avencera/homebased.git",
            "github.com:avencera/homebased",
        ];
        for form in forms {
            assert_eq!(
                key(form).as_deref(),
                Some("github.com/avencera/homebased"),
                "{form}"
            );
        }
        assert_eq!(
            key("https://gitlab.example.com/group/sub/app.git").as_deref(),
            Some("gitlab.example.com/group/sub/app")
        );
        assert_eq!(key("/srv/git/app.git"), None);
        assert_eq!(key("c:/code/app"), None);
        assert_eq!(key("https://github.com/avencera"), None);
    }

    #[test]
    fn a_selector_names_a_repository_only_in_a_repository_form() {
        let github = RepoKey::from_remote("git@github.com:avencera/homebased.git").unwrap();
        let gitlab = RepoKey::from_remote("https://gitlab.com/avencera/homebased").unwrap();
        let shorthand = RepoSelector::parse("Avencera/Homebased.git").unwrap();
        let url = RepoSelector::parse("https://github.com/avencera/homebased").unwrap();
        let hosted = RepoSelector::parse("gitlab.com/avencera/homebased").unwrap();

        assert!(shorthand.matches(&github) && shorthand.matches(&gitlab));
        assert!(url.matches(&github) && !url.matches(&gitlab));
        assert!(hosted.matches(&gitlab) && !hosted.matches(&github));
        for not_repository in ["app", "My App/next", "~/code/app", "/srv/app", "a/b/c"] {
            assert_eq!(
                RepoSelector::parse(not_repository),
                None,
                "{not_repository}"
            );
        }
    }

    #[test]
    fn a_clone_reaches_a_host_over_ssh_only_when_a_project_already_does() {
        let shorthand = RepoSelector::parse("avencera/homebased").unwrap();
        let url = RepoSelector::parse("https://example.com/team/app.git").unwrap();

        assert_eq!(
            shorthand.clone_url(["git@github.com:avencera/other.git"]),
            "git@github.com:avencera/homebased.git"
        );
        assert_eq!(
            shorthand.clone_url(["https://github.com/avencera/other", "git@gitlab.com:a/b"]),
            "https://github.com/avencera/homebased.git"
        );
        assert_eq!(url.clone_url([]), "https://example.com/team/app.git");
        assert_eq!(shorthand.name(), "homebased");
        assert_eq!(url.name(), "app");

        // the repository keeps its case in the clone, while matching ignores it
        let cased = RepoSelector::parse("git.example.com/Team/MyRepo").unwrap();
        assert_eq!(
            cased.clone_url([]),
            "https://git.example.com/Team/MyRepo.git"
        );
        assert_eq!(cased.name(), "MyRepo");
        assert!(cased.matches(&RepoKey::from_remote("git@git.example.com:team/myrepo").unwrap()));
    }
}
