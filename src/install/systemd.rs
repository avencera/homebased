//! Linux user systemd unit.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::AppError;
use crate::home::Home;
use crate::install::{agent_paths, binary_path, installer_path};

/// Unit file path.
#[must_use]
pub fn unit_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home).join(".config/systemd/user/homebased.service")
}

/// Whether the unit file exists.
#[must_use]
pub fn unit_installed() -> bool {
    unit_path().exists()
}

/// Render the unit text.
pub fn render(home: &Home) -> Result<String, AppError> {
    let bin = binary_path()?;
    let mut env_lines = String::new();
    env_lines.push_str(&format!("Environment=PATH={}\n", installer_path()));
    for (key, path) in agent_paths() {
        env_lines.push_str(&format!("Environment={key}={}\n", path.display()));
    }
    Ok(format!(
        "[Unit]\n\
         Description=homebased task supervisor\n\
         After=default.target\n\
         \n\
         [Service]\n\
         ExecStart={} daemon serve --home {}\n\
         KillMode=process\n\
         Restart=on-failure\n\
         TimeoutStopSec=15\n\
         {env_lines}\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        bin.display(),
        home.root().display(),
    ))
}

/// Write, verify, enable, and start the unit.
pub fn install(home: &Home) -> Result<(), AppError> {
    let text = render(home)?;
    let path = unit_path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&path, &text)?;
    verify(&path)?;
    run_systemctl(&["daemon-reload"])?;
    run_systemctl(&["enable", "--now", "homebased.service"])?;
    warn_linger();
    Ok(())
}

/// Disable and remove the unit file. Does not delete the database.
pub fn uninstall() -> Result<(), AppError> {
    let _ = run_systemctl(&["disable", "--now", "homebased.service"]);
    let path = unit_path();
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    let _ = run_systemctl(&["daemon-reload"]);
    Ok(())
}

/// `systemctl --user stop`.
pub fn host_stop() -> Result<(), AppError> {
    run_systemctl(&["stop", "homebased.service"])
}

/// `systemctl --user restart`.
pub fn host_restart() -> Result<(), AppError> {
    run_systemctl(&["restart", "homebased.service"])
}

fn verify(path: &Path) -> Result<(), AppError> {
    let out = Command::new("systemd-analyze")
        .args(["--user", "verify"])
        .arg(path)
        .output();
    match out {
        Ok(out) if out.status.success() => Ok(()),
        Ok(out) => Err(AppError::UnitInvalid {
            message: String::from_utf8_lossy(&out.stderr).into_owned(),
        }),
        Err(err) => Err(AppError::UnitInvalid {
            message: format!("systemd-analyze: {err}"),
        }),
    }
}

fn run_systemctl(args: &[&str]) -> Result<(), AppError> {
    let status = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .map_err(|err| AppError::Internal {
            message: format!("systemctl: {err}"),
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(AppError::Internal {
            message: format!("systemctl {args:?} failed"),
        })
    }
}

fn warn_linger() {
    let out = Command::new("loginctl")
        .args(["show-user", "-p", "Linger"])
        .output();
    if let Ok(out) = out {
        let text = String::from_utf8_lossy(&out.stdout);
        if text.contains("Linger=no") {
            eprintln!("warning: user lingering is off; `loginctl enable-linger` so homebased starts at login");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::home::Home;

    #[test]
    fn unit_contains_required_fields() {
        let home = Home::resolve(Some(PathBuf::from("/tmp/hb-state"))).unwrap();
        let text = render(&home).unwrap();
        assert!(text.contains("KillMode=process"), "{text}");
        assert!(
            !text.lines().any(|line| line.starts_with("ExecStop=")),
            "{text}"
        );
        assert!(text.contains("ExecStart="), "{text}");
        assert!(text.contains("daemon serve --home /tmp/hb-state"), "{text}");
        assert!(text.contains("Environment=PATH="), "{text}");
        assert!(text.contains("WantedBy=default.target"), "{text}");
        assert!(text.contains("Restart=on-failure"), "{text}");
        assert!(text.contains("TimeoutStopSec=15"), "{text}");
        let exec = text.lines().find(|l| l.starts_with("ExecStart=")).unwrap();
        assert!(exec.contains("/homebased") || exec.starts_with("ExecStart=/"));
    }
}
