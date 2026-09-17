//! macOS LaunchAgent plist.

use std::path::PathBuf;
use std::process::Command;

use crate::error::AppError;
use crate::home::Home;
use crate::install::{agent_paths, binary_path, installer_path};

/// LaunchAgent label.
pub const LABEL: &str = "dev.praveen.homebased";

/// Plist path.
#[must_use]
pub fn plist_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home).join("Library/LaunchAgents/dev.praveen.homebased.plist")
}

/// Render the plist.
pub fn render(home: &Home) -> Result<String, AppError> {
    let bin = binary_path()?;
    let mut env = String::new();
    env.push_str(&format!(
        "    <key>PATH</key>\n    <string>{}</string>\n",
        xml_escape(&installer_path())
    ));
    for (key, path) in agent_paths() {
        env.push_str(&format!(
            "    <key>{}</key>\n    <string>{}</string>\n",
            xml_escape(key),
            xml_escape(&path.display().to_string())
        ));
    }
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LABEL}</string>
  <key>KeepAlive</key>
  <true/>
  <key>RunAtLoad</key>
  <true/>
  <key>AbandonProcessGroup</key>
  <true/>
  <key>ProgramArguments</key>
  <array>
    <string>{}</string>
    <string>daemon</string>
    <string>serve</string>
    <string>--home</string>
    <string>{}</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
{env}  </dict>
</dict>
</plist>
"#,
        xml_escape(&bin.display().to_string()),
        xml_escape(&home.root().display().to_string()),
    ))
}

/// Write, lint, and bootstrap.
pub fn install(home: &Home) -> Result<(), AppError> {
    let text = render(home)?;
    let path = plist_path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&path, &text)?;
    lint(&path)?;
    let uid = nix::unistd::getuid().as_raw();
    let _ = Command::new("launchctl")
        .args([
            "bootout",
            &format!("gui/{uid}"),
            &path.display().to_string(),
        ])
        .status();
    let status = Command::new("launchctl")
        .args(["bootstrap", &format!("gui/{uid}")])
        .arg(&path)
        .status()
        .map_err(|err| AppError::Internal {
            message: format!("launchctl bootstrap: {err}"),
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(AppError::UnitInvalid {
            message: "launchctl bootstrap failed".into(),
        })
    }
}

/// Unload and remove the plist.
pub fn uninstall() -> Result<(), AppError> {
    let path = plist_path();
    let uid = nix::unistd::getuid().as_raw();
    let _ = Command::new("launchctl")
        .args([
            "bootout",
            &format!("gui/{uid}"),
            &path.display().to_string(),
        ])
        .status();
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

/// `launchctl bootout` equivalent of stop.
pub fn host_stop() -> Result<(), AppError> {
    let uid = nix::unistd::getuid().as_raw();
    let _ = Command::new("launchctl")
        .args(["bootout", &format!("gui/{uid}/{LABEL}")])
        .status();
    Ok(())
}

/// `launchctl kickstart -k`.
pub fn host_restart() -> Result<(), AppError> {
    let uid = nix::unistd::getuid().as_raw();
    let status = Command::new("launchctl")
        .args(["kickstart", "-k", &format!("gui/{uid}/{LABEL}")])
        .status()
        .map_err(|err| AppError::Internal {
            message: format!("launchctl kickstart: {err}"),
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(AppError::Internal {
            message: "launchctl kickstart failed".into(),
        })
    }
}

fn lint(path: &std::path::Path) -> Result<(), AppError> {
    let out = Command::new("plutil").args(["-lint"]).arg(path).output();
    match out {
        Ok(out) if out.status.success() => Ok(()),
        Ok(out) => Err(AppError::UnitInvalid {
            message: String::from_utf8_lossy(&out.stderr).into_owned(),
        }),
        Err(_) => Ok(()), // plutil is macOS-only; skip on Linux
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::home::Home;

    #[test]
    fn plist_contains_required_fields() {
        let home = Home::resolve(Some(PathBuf::from("/tmp/hb-state"))).unwrap();
        let text = render(&home).unwrap();
        assert!(text.contains("AbandonProcessGroup"), "{text}");
        assert!(text.contains("<true/>"), "{text}");
        assert!(text.contains("KeepAlive"), "{text}");
        assert!(text.contains("RunAtLoad"), "{text}");
        assert!(text.contains("daemon"), "{text}");
        assert!(text.contains("serve"), "{text}");
        assert!(text.contains("/tmp/hb-state"), "{text}");
        assert!(text.contains("<string>") && text.contains("homebased") || text.contains("/"));
    }
}
