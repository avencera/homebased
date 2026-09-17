use std::{
    env, fs,
    path::Path,
    process::{Command, Output},
};

use color_eyre::eyre::{Result, WrapErr, bail};

pub(crate) fn local() -> Result<()> {
    let workspace_root = crate::workspace_root()?;
    // the dashboard is embedded at compile time, so it has to be built first or
    // the release binary would serve the "not built" page
    build_dashboard(&workspace_root)?;

    let status = Command::new("cargo")
        .args(["build", "--release", "--package", "homebased"])
        .current_dir(&workspace_root)
        .status()
        .wrap_err("failed to run cargo build --release")?;

    if !status.success() {
        bail!("cargo build --release failed");
    }

    let home = env::var("HOME").wrap_err("HOME is not set")?;
    let bin_dir = format!("{home}/.local/bin");
    fs::create_dir_all(&bin_dir)?;

    let src = workspace_root.join("target/release/homebased");
    let dest = format!("{bin_dir}/homebased");

    // unlink first so we can overwrite even if the binary is currently running (macOS ETXTBSY)
    let _ = fs::remove_file(&dest);
    fs::copy(&src, &dest)?;

    println!("Installed homebased to {dest}");
    Ok(())
}

pub(crate) fn github() -> Result<()> {
    let root = crate::workspace_root()?;
    let workflow = root.join(".github/workflows/release.yml");
    if !workflow.is_file() {
        bail!("missing .github/workflows/release.yml; cannot publish GitHub release assets");
    }

    if path_differs_from_head(&root, &["Cargo.toml", "Cargo.lock"])? {
        bail!(
            "Cargo.toml or Cargo.lock has uncommitted changes; commit the version bump before release"
        );
    }

    let version = crate::bump::package_version(&root)?;
    let tag = format!("v{version}");

    let origin = git_stdout(&root, &["remote", "get-url", "origin"])
        .wrap_err("git remote origin is missing; add a GitHub remote named origin")?;

    if !git_stdout(&root, &["tag", "-l", &tag])?.is_empty() {
        bail!("git tag {tag} already exists locally");
    }

    let remote_tag = git_stdout(
        &root,
        &["ls-remote", "--tags", "origin", &format!("refs/tags/{tag}")],
    )?;
    if !remote_tag.is_empty() {
        bail!("git tag {tag} already exists on origin");
    }

    git_ok(&root, &["tag", &tag])?;
    git_ok(&root, &["push", "origin", &tag])?;

    println!("Pushed tag {tag} to origin");
    println!("GitHub Actions will attach macOS and Linux binaries to the release");
    println!("Watch: gh run list --workflow Release");
    if let Some(slug) = github_repo_slug(&origin) {
        println!(
            "Install: curl -LSfs https://github.com/{slug}/releases/latest/download/install.sh | sh"
        );
    }
    Ok(())
}

/// Run `npm ci` then `npm run build` in `web/`. Either failure fails the
/// release: a binary without the dashboard is not a release build.
fn build_dashboard(root: &Path) -> Result<()> {
    let web = root.join("web");
    if !web.join("package.json").is_file() {
        bail!(
            "missing {}; the dashboard must be scaffolded before a release",
            web.join("package.json").display()
        );
    }
    for args in [["ci"].as_slice(), ["run", "build"].as_slice()] {
        let status = Command::new("npm")
            .args(args)
            .current_dir(&web)
            .status()
            .wrap_err_with(|| format!("failed to run npm {}", args.join(" ")))?;
        if !status.success() {
            bail!("npm {} failed in web/", args.join(" "));
        }
    }
    Ok(())
}

fn path_differs_from_head(root: &Path, paths: &[&str]) -> Result<bool> {
    let mut args = vec!["diff", "--quiet", "HEAD", "--"];
    args.extend(paths);
    let output = git(root, &args)?;
    match output.status.code() {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        _ => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("git diff failed: {stderr}");
        }
    }
}

fn git_ok(root: &Path, args: &[&str]) -> Result<()> {
    let output = git(root, args)?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {} failed: {stderr}", args.join(" "));
    }
    Ok(())
}

fn git_stdout(root: &Path, args: &[&str]) -> Result<String> {
    let output = git(root, args)?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {} failed: {stderr}", args.join(" "));
    }
    String::from_utf8(output.stdout)
        .wrap_err("git output was not utf-8")
        .map(|s| s.trim().to_string())
}

fn git(root: &Path, args: &[&str]) -> Result<Output> {
    Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .wrap_err_with(|| format!("failed to run git {}", args.join(" ")))
}

fn github_repo_slug(origin: &str) -> Option<String> {
    let origin = origin.trim().trim_end_matches(".git");
    origin
        .strip_prefix("git@github.com:")
        .or_else(|| origin.strip_prefix("https://github.com/"))
        .or_else(|| origin.strip_prefix("ssh://git@github.com/"))
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::github_repo_slug;

    #[test]
    fn parses_github_remote_urls() {
        assert_eq!(
            github_repo_slug("git@github.com:avencera/homebasd.git").as_deref(),
            Some("avencera/homebasd")
        );
        assert_eq!(
            github_repo_slug("https://github.com/avencera/homebasd.git").as_deref(),
            Some("avencera/homebasd")
        );
        assert_eq!(
            github_repo_slug("ssh://git@github.com/avencera/homebasd").as_deref(),
            Some("avencera/homebasd")
        );
        assert_eq!(github_repo_slug("https://example.com/not-github"), None);
    }
}
