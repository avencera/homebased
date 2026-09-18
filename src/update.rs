//! Replace the installed binary from a GitHub release.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crate::error::AppError;

/// Default GitHub repository that hosts release assets.
pub const DEFAULT_GIT: &str = "avencera/homebased";
/// Binary name inside the release archive.
pub const CRATE_NAME: &str = "homebased";

/// Where an update will write `homebased` and which asset it will fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePlan {
    /// Release tag, including the leading `v`.
    pub tag: String,
    /// Rustc target triple for the GitHub asset.
    pub target: String,
    /// Directory that will hold the binary.
    pub dest_dir: PathBuf,
    /// Final binary path.
    pub dest: PathBuf,
    /// Archive URL.
    pub url: String,
}

/// Inputs that resolve an [`UpdatePlan`].
#[derive(Debug, Clone)]
pub struct UpdateRequest {
    /// `owner/repo`. Ignored when `base_url` is set.
    pub git: String,
    /// Explicit tag, or latest when `None`.
    pub tag: Option<String>,
    /// Install directory override.
    pub dest_dir: Option<PathBuf>,
    /// Replaces `https://github.com/{git}` (tests).
    pub base_url: Option<String>,
}

/// Resolve tag, target, destination, and download URL. Downloads nothing when
/// `tag` is already known.
pub fn plan(request: &UpdateRequest) -> Result<UpdatePlan, AppError> {
    let git = request.git.trim();
    if git.is_empty() {
        return Err(AppError::Usage {
            message: "--git requires owner/repo".into(),
        });
    }
    let base = request
        .base_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map_or_else(|| format!("https://github.com/{git}"), str::to_string);
    let base = base.trim_end_matches('/');
    let tag = match request
        .tag
        .as_deref()
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
    {
        Some(tag) => tag.to_string(),
        None => resolve_latest_tag(base)?,
    };
    let target = target_triple()?.to_string();
    let dest_dir = match &request.dest_dir {
        Some(dir) => expand_dest(dir)?,
        None => default_dest_dir()?,
    };
    let dest = dest_dir.join(CRATE_NAME);
    let url = asset_url(base, &tag, &target);
    Ok(UpdatePlan {
        tag,
        target,
        dest_dir,
        dest,
        url,
    })
}

/// Download the planned archive and replace the destination binary.
pub fn install(plan: &UpdatePlan) -> Result<(), AppError> {
    need("curl")?;
    need("tar")?;
    let td = TempDir::create()?;
    let archive = td.0.join("homebased.tar.gz");
    download(&plan.url, &archive)?;
    extract_archive(&archive, &td.0)?;
    let src = find_executable(&td.0)?;
    replace_binary(&src, &plan.dest)?;
    Ok(())
}

/// Rustc target whose GitHub asset this host should download.
pub fn target_triple() -> Result<&'static str, AppError> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        ("macos", "x86_64") => Ok("x86_64-apple-darwin"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-musl"),
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-musl"),
        (os, arch) => Err(AppError::Usage {
            message: format!("unsupported OS {os}/{arch}; supported: Linux and macOS"),
        }),
    }
}

/// Directory that receives the binary when `--to` is omitted.
pub fn default_dest_dir() -> Result<PathBuf, AppError> {
    let fallback = local_bin_dir()?;
    let Ok(exe) = std::env::current_exe() else {
        return Ok(fallback);
    };
    if is_cargo_build_path(&exe) {
        return Ok(fallback);
    }
    Ok(exe
        .parent()
        .map(Path::to_path_buf)
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(fallback))
}

/// Expand a user-supplied `--to` directory, including a leading `~`.
pub fn expand_dest(dest: &Path) -> Result<PathBuf, AppError> {
    let raw = dest.to_string_lossy();
    if raw == "~" {
        return home_dir();
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        return Ok(home_dir()?.join(rest));
    }
    Ok(dest.to_path_buf())
}

/// Final URL of a GitHub `releases/latest` redirect.
pub fn tag_from_latest_url(url: &str) -> Result<String, AppError> {
    let url = url.trim().trim_end_matches('/');
    match url.rsplit_once("/releases/tag/") {
        Some((_, tag)) if !tag.is_empty() && !tag.contains('/') => Ok(tag.to_string()),
        _ => Err(AppError::Internal {
            message: format!("no GitHub release found: {url}"),
        }),
    }
}

fn asset_url(base: &str, tag: &str, target: &str) -> String {
    format!("{base}/releases/download/{tag}/{CRATE_NAME}-{tag}-{target}.tar.gz")
}

fn resolve_latest_tag(base: &str) -> Result<String, AppError> {
    need("curl")?;
    let latest = format!("{base}/releases/latest");
    let output = run(
        "curl",
        &[
            "-fsSL",
            "-o",
            "/dev/null",
            "-w",
            "%{url_effective}",
            &latest,
        ],
    )?;
    let url = String::from_utf8(output.stdout).map_err(|err| AppError::Internal {
        message: format!("latest release URL was not utf-8: {err}"),
    })?;
    tag_from_latest_url(&url)
}

fn download(url: &str, dest: &Path) -> Result<(), AppError> {
    eprintln!("downloading {url}");
    let dest = dest.to_string_lossy();
    let output = Command::new("curl")
        .args(["-fsSL", "-o", dest.as_ref(), url])
        .output()
        .map_err(|err| map_spawn("curl", err))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if output.status.code() == Some(22) {
        return Err(AppError::FileNotFound {
            message: format!("{url} does not exist"),
        });
    }
    Err(AppError::Internal {
        message: format!("curl {url} failed: {stderr}"),
    })
}

fn extract_archive(archive: &Path, dest: &Path) -> Result<(), AppError> {
    let dest = dest.to_string_lossy();
    let archive = archive.to_string_lossy();
    run("tar", &["-C", dest.as_ref(), "-xzf", archive.as_ref()])?;
    Ok(())
}

fn find_executable(dir: &Path) -> Result<PathBuf, AppError> {
    let mut found = None;
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = entry.metadata()?.permissions().mode();
            if mode & 0o111 == 0 {
                continue;
            }
        }
        found = Some(path);
        break;
    }
    found.ok_or_else(|| AppError::Internal {
        message: "archive did not contain an executable homebased binary".into(),
    })
}

/// Unlink then copy so a running binary can be replaced (macOS `ETXTBSY`).
pub fn replace_binary(src: &Path, dest: &Path) -> Result<(), AppError> {
    if let Some(dir) = dest.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let _ = std::fs::remove_file(dest);
    std::fs::copy(src, dest)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(dest)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(dest, perms)?;
    }
    Ok(())
}

fn is_cargo_build_path(exe: &Path) -> bool {
    let parts: Vec<_> = exe.iter().map(|s| s.to_string_lossy()).collect();
    parts.windows(2).any(|pair| {
        pair[0] == "target" && (pair[1] == "debug" || pair[1] == "release" || pair[1].contains('-'))
    })
}

fn local_bin_dir() -> Result<PathBuf, AppError> {
    Ok(home_dir()?.join(".local/bin"))
}

fn home_dir() -> Result<PathBuf, AppError> {
    std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| AppError::Internal {
            message: "HOME is not set".into(),
        })
}

fn need(program: &str) -> Result<(), AppError> {
    which::which(program)
        .map(|_| ())
        .map_err(|_| AppError::ExecutableMissing {
            program: program.into(),
        })
}

fn run(program: &str, args: &[&str]) -> Result<std::process::Output, AppError> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|err| map_spawn(program, err))?;
    if output.status.success() {
        return Ok(output);
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(AppError::Internal {
        message: format!("{program} {} failed: {stderr}", args.join(" ")),
    })
}

fn map_spawn(program: &str, err: std::io::Error) -> AppError {
    if err.kind() == std::io::ErrorKind::NotFound {
        AppError::ExecutableMissing {
            program: program.into(),
        }
    } else {
        AppError::Internal {
            message: format!("{program}: {err}"),
        }
    }
}

struct TempDir(PathBuf);

impl TempDir {
    fn create() -> Result<Self, AppError> {
        let dir = std::env::temp_dir().join(format!("homebased-update-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir)?;
        Ok(Self(dir))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Bound used by the CLI when waiting for `/v1/status` after restart.
pub const STATUS_WAIT: Duration = Duration::from_secs(15);

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn latest_url_yields_tag() {
        assert_eq!(
            tag_from_latest_url("https://github.com/avencera/homebased/releases/tag/v0.2.0\n")
                .unwrap(),
            "v0.2.0"
        );
        assert_eq!(
            tag_from_latest_url("http://127.0.0.1:9/owner/repo/releases/tag/v1.0.0/").unwrap(),
            "v1.0.0"
        );
        assert!(tag_from_latest_url("https://github.com/avencera/homebased").is_err());
        assert!(tag_from_latest_url("https://example/releases/tag/a/b").is_err());
    }

    #[test]
    fn asset_url_matches_release_assets() {
        assert_eq!(
            asset_url(
                "https://github.com/avencera/homebased",
                "v0.2.0",
                "x86_64-unknown-linux-musl"
            ),
            "https://github.com/avencera/homebased/releases/download/v0.2.0/homebased-v0.2.0-x86_64-unknown-linux-musl.tar.gz"
        );
    }

    #[test]
    fn cargo_build_paths_fall_back() {
        assert!(is_cargo_build_path(Path::new(
            "/home/praveen/code/homebasd/target/debug/homebased"
        )));
        assert!(is_cargo_build_path(Path::new(
            "/tmp/target/x86_64-unknown-linux-gnu/release/homebased"
        )));
        assert!(!is_cargo_build_path(Path::new(
            "/home/praveen/.local/bin/homebased"
        )));
    }

    #[test]
    fn expand_tilde_uses_home() {
        let home = home_dir().unwrap();
        assert_eq!(expand_dest(Path::new("~")).unwrap(), home);
        assert_eq!(
            expand_dest(Path::new("~/opt/bin")).unwrap(),
            home.join("opt/bin")
        );
        assert_eq!(
            expand_dest(Path::new("/opt/homebased")).unwrap(),
            PathBuf::from("/opt/homebased")
        );
    }

    #[test]
    fn plan_with_tag_skips_network() {
        let dir = tempfile::tempdir().unwrap();
        let plan = plan(&UpdateRequest {
            git: DEFAULT_GIT.into(),
            tag: Some("v0.2.0".into()),
            dest_dir: Some(dir.path().to_path_buf()),
            base_url: None,
        })
        .unwrap();
        assert_eq!(plan.tag, "v0.2.0");
        assert_eq!(plan.dest, dir.path().join("homebased"));
        assert!(plan.url.contains("v0.2.0"));
        assert!(plan.url.contains(target_triple().unwrap()));
    }

    #[test]
    fn empty_git_is_usage() {
        let err = plan(&UpdateRequest {
            git: "  ".into(),
            tag: Some("v0.2.0".into()),
            dest_dir: None,
            base_url: None,
        })
        .unwrap_err();
        assert_eq!(err.code(), "usage");
    }

    #[test]
    fn replace_binary_overwrites_and_is_executable() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dest = dir.path().join("bin/homebased");
        fs::create_dir(dest.parent().unwrap()).unwrap();
        fs::write(&src, b"#!/bin/sh\n").unwrap();
        fs::write(&dest, b"old").unwrap();
        replace_binary(&src, &dest).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"#!/bin/sh\n");
        let mode = fs::metadata(&dest).unwrap().permissions().mode();
        assert_eq!(mode & 0o111, 0o111);
    }

    #[test]
    fn install_archive_picks_executable() {
        let dir = tempfile::tempdir().unwrap();
        let payload = dir.path().join("payload");
        fs::create_dir(&payload).unwrap();
        let bin = payload.join("homebased");
        fs::write(&bin, b"#!/bin/sh\necho ok\n").unwrap();
        let mut perms = fs::metadata(&bin).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&bin, perms).unwrap();
        let archive = dir.path().join("homebased.tar.gz");
        let status = Command::new("tar")
            .args([
                "-C",
                payload.to_str().unwrap(),
                "-czf",
                archive.to_str().unwrap(),
                "homebased",
            ])
            .status()
            .unwrap();
        assert!(status.success());

        let dest_dir = dir.path().join("dest");
        let plan = UpdatePlan {
            tag: "v9.9.9".into(),
            target: "test".into(),
            dest_dir: dest_dir.clone(),
            dest: dest_dir.join("homebased"),
            url: "unused".into(),
        };
        let td = TempDir::create().unwrap();
        extract_archive(&archive, &td.0).unwrap();
        let src = find_executable(&td.0).unwrap();
        replace_binary(&src, &plan.dest).unwrap();
        assert!(plan.dest.is_file());
        let mode = fs::metadata(&plan.dest).unwrap().permissions().mode();
        assert_eq!(mode & 0o111, 0o111);
    }
}
