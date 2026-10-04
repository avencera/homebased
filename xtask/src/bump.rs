use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    fs,
    path::Path,
    process::{Command, Stdio},
    str::FromStr,
};

use clap::ValueEnum;
use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use toml_edit::{DocumentMut, Item, value};

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum Part {
    Major,
    Minor,
    Patch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Version {
    major: u64,
    minor: u64,
    patch: u64,
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl FromStr for Version {
    type Err = color_eyre::eyre::Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.contains('-') || s.contains('+') {
            bail!("homebased version must be major.minor.patch (got {s})");
        }
        let mut parts = s.split('.');
        let version = Self {
            major: parse_component(parts.next(), "major", s)?,
            minor: parse_component(parts.next(), "minor", s)?,
            patch: parse_component(parts.next(), "patch", s)?,
        };
        if parts.next().is_some() {
            bail!("homebased version must be major.minor.patch (got {s})");
        }
        Ok(version)
    }
}

impl Version {
    fn bump(self, part: Part) -> Result<Self> {
        match part {
            Part::Major => Ok(Self {
                major: checked_inc(self.major, "major")?,
                minor: 0,
                patch: 0,
            }),
            Part::Minor => Ok(Self {
                major: self.major,
                minor: checked_inc(self.minor, "minor")?,
                patch: 0,
            }),
            Part::Patch => Ok(Self {
                major: self.major,
                minor: self.minor,
                patch: checked_inc(self.patch, "patch")?,
            }),
        }
    }
}

pub(crate) fn run(part: Part) -> Result<()> {
    let root = crate::workspace_root()?;
    let (current, next) = bump_at(&root, part, refresh_lockfile)?;
    println!("Bumped homebased {current} -> {next}");
    Ok(())
}

fn bump_at(
    root: &Path,
    part: Part,
    refresh: impl FnOnce(&Path) -> Result<()>,
) -> Result<(Version, Version)> {
    let cargo_toml = root.join("Cargo.toml");
    let contents = fs::read_to_string(&cargo_toml)
        .wrap_err_with(|| format!("failed to read {}", cargo_toml.display()))?;
    let current = package_version_from_toml(&contents)?;
    let next = current.bump(part)?;
    let updated = set_package_version(&contents, next)?;
    let lock_path = root.join("Cargo.lock");
    let saved_lock = match fs::read(&lock_path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).wrap_err("failed to read Cargo.lock before bump"),
    };
    fs::write(&cargo_toml, updated)
        .wrap_err_with(|| format!("failed to write {}", cargo_toml.display()))?;
    if let Err(error) = refresh(root) {
        fs::write(&cargo_toml, contents)
            .wrap_err_with(|| format!("bump failed ({error}); failed to restore Cargo.toml"))?;
        match saved_lock {
            Some(bytes) => fs::write(&lock_path, bytes)
                .wrap_err_with(|| format!("bump failed ({error}); failed to restore Cargo.lock"))?,
            None => match fs::remove_file(&lock_path) {
                Ok(()) => {}
                Err(remove_error) if remove_error.kind() == std::io::ErrorKind::NotFound => {}
                Err(remove_error) => {
                    return Err(remove_error).wrap_err_with(|| {
                        format!("bump failed ({error}); failed to remove new Cargo.lock")
                    });
                }
            },
        }
        return Err(error);
    }
    Ok((current, next))
}

pub(crate) fn package_version(root: &Path) -> Result<Version> {
    let cargo_toml = root.join("Cargo.toml");
    let contents = fs::read_to_string(&cargo_toml)
        .wrap_err_with(|| format!("failed to read {}", cargo_toml.display()))?;
    package_version_from_toml(&contents)
}

fn package_version_from_toml(contents: &str) -> Result<Version> {
    let doc = contents
        .parse::<DocumentMut>()
        .wrap_err("Cargo.toml is not valid TOML")?;
    version_from_doc(&doc)?.parse()
}

fn version_from_doc(doc: &DocumentMut) -> Result<&str> {
    doc.get("package")
        .and_then(Item::as_table)
        .and_then(|table| table.get("version"))
        .and_then(Item::as_str)
        .ok_or_else(|| eyre!("Cargo.toml is missing [package].version"))
}

fn set_package_version(contents: &str, version: Version) -> Result<String> {
    let mut doc = contents
        .parse::<DocumentMut>()
        .wrap_err("Cargo.toml is not valid TOML")?;
    let Some(package) = doc.get_mut("package").and_then(Item::as_table_mut) else {
        bail!("Cargo.toml is missing [package]");
    };
    package["version"] = value(version.to_string());
    Ok(doc.to_string())
}

/// Record the new package version in `Cargo.lock`
///
/// Release builds use `--locked`, so a stale entry fails them. `cargo metadata`
/// no longer rewrites the lockfile, so update only the homebased entry offline
fn refresh_lockfile(root: &Path) -> Result<()> {
    let output = Command::new("cargo")
        .args(["update", "--package", "homebased", "--offline"])
        .current_dir(root)
        .stdout(Stdio::null())
        .output()
        .wrap_err("failed to run cargo update")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("cargo update failed: {stderr}");
    }
    Ok(())
}

fn parse_component(part: Option<&str>, name: &str, version: &str) -> Result<u64> {
    let Some(part) = part else {
        bail!("homebased version must be major.minor.patch (got {version})");
    };
    part.parse::<u64>()
        .wrap_err_with(|| format!("invalid {name} version component in {version}"))
}

fn checked_inc(value: u64, name: &str) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| eyre!("{name} version overflow"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn bugfix_failed_lock_refresh_restores_both_files() {
        let root = std::env::temp_dir().join(format!("homebased-bump-test-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let manifest = "[package]\nname = \"homebased\"\nversion = \"1.2.3\"\n";
        std::fs::write(root.join("Cargo.toml"), manifest).unwrap();
        std::fs::write(root.join("Cargo.lock"), "original lock").unwrap();
        let result = super::bump_at(&root, super::Part::Patch, |root| {
            std::fs::write(root.join("Cargo.lock"), "partial lock")?;
            Err(color_eyre::eyre::eyre!("simulated cargo failure"))
        });
        let actual_manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
        let actual_lock = std::fs::read_to_string(root.join("Cargo.lock")).unwrap();
        std::fs::remove_dir_all(root).unwrap();
        assert!(result.is_err());
        assert_eq!(actual_manifest, manifest);
        assert_eq!(actual_lock, "original lock");
    }

    use super::{Part, Version, set_package_version};

    fn v(s: &str) -> Version {
        s.parse().unwrap()
    }

    #[test]
    fn bump_patch_minor_major() {
        assert_eq!(v("0.1.0").bump(Part::Patch).unwrap().to_string(), "0.1.1");
        assert_eq!(v("0.1.9").bump(Part::Minor).unwrap().to_string(), "0.2.0");
        assert_eq!(v("1.2.3").bump(Part::Major).unwrap().to_string(), "2.0.0");
    }

    #[test]
    fn reject_non_semver_core() {
        assert!("1.2".parse::<Version>().is_err());
        assert!("1.2.3.4".parse::<Version>().is_err());
        assert!("1.2.3-alpha".parse::<Version>().is_err());
        assert!("1.2.3+build".parse::<Version>().is_err());
    }

    #[test]
    fn set_package_version_keeps_surrounding_keys() {
        let toml = "\
[workspace]
members = [\"xtask\"]

[package]
name = \"homebased\"
version = \"0.1.0\"
edition = \"2024\"
";
        let updated = set_package_version(toml, v("0.2.0")).unwrap();
        assert!(updated.contains("name = \"homebased\""));
        assert!(updated.contains("version = \"0.2.0\""));
        assert!(!updated.contains("version = \"0.1.0\""));
        assert!(updated.contains("members = [\"xtask\"]"));
    }
}
