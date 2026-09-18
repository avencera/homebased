//! macOS LaunchAgent plist.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::AppError;
use crate::home::Home;
use crate::install::{
    HostUnitState, agent_paths, binary_path, classify_configured_home, installer_path,
    web_listen_env,
};

/// LaunchAgent label.
pub const LABEL: &str = "dev.praveen.homebased";

/// Plist path.
#[must_use]
pub fn plist_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home).join("Library/LaunchAgents/dev.praveen.homebased.plist")
}

/// Inspect whether the installed plist belongs to `home`.
pub fn inspect_host_unit(home: &Home) -> Result<HostUnitState, AppError> {
    inspect_host_unit_at(home, &plist_path())
}

fn inspect_host_unit_at(home: &Home, path: &Path) -> Result<HostUnitState, AppError> {
    if !path.exists() {
        return Ok(HostUnitState::Absent);
    }
    #[cfg(target_os = "macos")]
    if !plist_is_valid(path)? {
        return Ok(HostUnitState::Unrecognized);
    }
    let text = std::fs::read_to_string(path)?;
    Ok(classify_configured_home(home, parse_plist_home(&text)))
}

/// Decode the `--home` path from LaunchAgent plist text. `None` means absent,
/// ambiguous, or unrecognized daemon invocation.
pub(crate) fn parse_plist_home(text: &str) -> Option<PathBuf> {
    let trimmed = text.trim();
    if !trimmed.starts_with("<?xml ") || !trimmed.ends_with("</plist>") {
        return None;
    }
    if parse_string_value(text, "Label")?.as_str() != LABEL {
        return None;
    }
    let args = parse_program_arguments(text)?;
    home_from_program_arguments(&args)
}

/// Render the plist.
pub fn render(home: &Home) -> Result<String, AppError> {
    let bin = binary_path()?;
    let home = std::path::absolute(home.root())?;
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
    if let Some((key, value)) = web_listen_env()? {
        env.push_str(&format!(
            "    <key>{}</key>\n    <string>{}</string>\n",
            xml_escape(key),
            xml_escape(&value)
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
        xml_escape(&home.display().to_string()),
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
    // bootout fails when nothing is loaded; the bootstrap below is the real step
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
    // uninstall must succeed even when the job was never loaded
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
    // stopping an already-stopped job is a success for the caller
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

#[cfg(target_os = "macos")]
fn plist_is_valid(path: &Path) -> Result<bool, AppError> {
    let output = Command::new("plutil")
        .args(["-lint"])
        .arg(path)
        .output()
        .map_err(|err| AppError::Internal {
            message: format!("plutil: {err}"),
        })?;
    Ok(output.status.success())
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn xml_unescape(s: &str) -> String {
    s.replace("&quot;", "\"")
        .replace("&gt;", ">")
        .replace("&lt;", "<")
        .replace("&amp;", "&")
}

/// Extract the single `ProgramArguments` string list. Repeated keys or a
/// missing array yield `None`.
fn parse_program_arguments(text: &str) -> Option<Vec<String>> {
    let mut lines = text.lines().peekable();
    let mut found: Option<Vec<String>> = None;
    while let Some(line) = lines.next() {
        if line.trim() != "<key>ProgramArguments</key>" {
            continue;
        }
        if found.is_some() {
            return None;
        }
        let array_open = lines.next()?.trim();
        if array_open != "<array>" {
            return None;
        }
        let mut args = Vec::new();
        loop {
            let entry = lines.next()?.trim();
            if entry == "</array>" {
                break;
            }
            let value = entry.strip_prefix("<string>")?.strip_suffix("</string>")?;
            args.push(xml_unescape(value));
        }
        found = Some(args);
    }
    found
}

fn parse_string_value(text: &str, key: &str) -> Option<String> {
    let key_line = format!("<key>{key}</key>");
    let mut lines = text.lines();
    let mut found = None;
    while let Some(line) = lines.next() {
        if line.trim() != key_line {
            continue;
        }
        if found.is_some() {
            return None;
        }
        let value = lines
            .next()?
            .trim()
            .strip_prefix("<string>")?
            .strip_suffix("</string>")?;
        found = Some(xml_unescape(value));
    }
    found
}

fn home_from_program_arguments(args: &[String]) -> Option<PathBuf> {
    // require the exact shape generated by render so unrelated jobs cannot claim ownership
    if args.len() != 5
        || args[1] != "daemon"
        || args[2] != "serve"
        || args[3] != "--home"
        || args[4].is_empty()
    {
        return None;
    }
    Some(PathBuf::from(&args[4]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::home::Home;
    use crate::install::{HostUnitState, classify_configured_home};

    fn sample_plist(bin: &str, home: &str) -> String {
        format!(
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
    <string>{bin}</string>
    <string>daemon</string>
    <string>serve</string>
    <string>--home</string>
    <string>{home}</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/usr/bin</string>
    <key>HOMEBASED_WEB_LISTEN</key>
    <string>127.0.0.1:9</string>
  </dict>
</dict>
</plist>
"#
        )
    }

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
        assert!(
            text.contains(&format!("<key>Label</key>\n  <string>{LABEL}</string>")),
            "plist must carry the reverse-DNS label: {text}"
        );
    }

    #[test]
    fn plist_makes_a_relative_home_absolute() {
        let home = Home::resolve(Some(PathBuf::from("relative-state"))).unwrap();
        let expected = std::path::absolute(home.root()).unwrap();
        let text = render(&home).unwrap();
        assert!(
            text.contains(&format!("<string>{}</string>", expected.display())),
            "{text}"
        );
    }

    #[test]
    fn parse_selected_home_from_generated_shape() {
        let text = sample_plist("/opt/homebased", "/tmp/hb-state");
        assert_eq!(
            parse_plist_home(&text),
            Some(PathBuf::from("/tmp/hb-state"))
        );
    }

    #[test]
    fn parse_ignores_binary_and_environment_changes() {
        let a = sample_plist("/old/homebased", "/tmp/hb-state");
        let b = sample_plist("/new/homebased", "/tmp/hb-state");
        assert_eq!(parse_plist_home(&a), parse_plist_home(&b));
    }

    #[test]
    fn parse_other_home() {
        let text = sample_plist("/usr/bin/homebased", "/tmp/other-home");
        let selected = Home::resolve(Some(PathBuf::from("/tmp/selected-home"))).unwrap();
        assert_eq!(
            classify_configured_home(&selected, parse_plist_home(&text)),
            HostUnitState::OtherHome {
                configured: PathBuf::from("/tmp/other-home"),
            }
        );
    }

    #[test]
    fn parse_rejects_malformed_and_ambiguous() {
        assert_eq!(parse_plist_home("<plist></plist>\n"), None);
        assert_eq!(
            parse_plist_home(
                "<key>ProgramArguments</key>\n<array>\n<string>/bin/homebased</string>\n\
                 <string>daemon</string>\n<string>serve</string>\n<string>--home</string>\n\
                 <string>/tmp/a</string>\n</array>\n"
            ),
            None
        );
        let wrong_case = sample_plist("/bin/homebased", "/tmp/a")
            .replace("<key>ProgramArguments</key>", "<key>programarguments</key>");
        assert_eq!(parse_plist_home(&wrong_case), None);
        let wrong_label =
            sample_plist("/bin/homebased", "/tmp/a").replace(LABEL, "dev.example.other");
        assert_eq!(parse_plist_home(&wrong_label), None);
        assert_eq!(
            parse_plist_home(
                "<key>ProgramArguments</key>\n<array>\n<string>/bin/homebased</string>\n\
                 <string>serve</string>\n</array>\n"
            ),
            None
        );
        let mut dup = sample_plist("/bin/homebased", "/tmp/a");
        dup.push_str(&sample_plist("/bin/homebased", "/tmp/b"));
        assert_eq!(parse_plist_home(&dup), None);

        let mut extra = sample_plist("/bin/homebased", "/tmp/a");
        extra = extra.replace(
            "    <string>/tmp/a</string>",
            "    <string>/tmp/a</string>\n    <string>extra</string>",
        );
        assert_eq!(parse_plist_home(&extra), None);
    }

    #[test]
    fn parse_xml_unescapes_home() {
        let text = sample_plist("/bin/homebased", "/tmp/a&amp;b");
        assert_eq!(parse_plist_home(&text), Some(PathBuf::from("/tmp/a&b")));
    }

    #[test]
    fn inspect_absent_and_selected() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        std::fs::create_dir_all(&state).unwrap();
        let home = Home::resolve(Some(state.clone())).unwrap();
        let path = dir.path().join("homebased.plist");
        assert_eq!(
            inspect_host_unit_at(&home, &path).unwrap(),
            HostUnitState::Absent
        );

        std::fs::write(
            &path,
            sample_plist("/usr/bin/homebased", state.to_str().unwrap()),
        )
        .unwrap();
        assert_eq!(
            inspect_host_unit_at(&home, &path).unwrap(),
            HostUnitState::SelectedHome
        );

        std::fs::write(&path, "not a plist\n").unwrap();
        assert_eq!(
            inspect_host_unit_at(&home, &path).unwrap(),
            HostUnitState::Unrecognized
        );
    }
}
