//! Linux user systemd unit.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::CONFIG_ENV;
use crate::error::AppError;
use crate::home::Home;
use crate::install::{
    HostUnitState, agent_paths, binary_path, classify_configured_home, installer_path,
    web_listen_env,
};

/// Unit file path.
#[must_use]
pub fn unit_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home).join(".config/systemd/user/homebased.service")
}

/// Inspect whether the installed unit belongs to `home`.
pub fn inspect_host_unit(home: &Home) -> Result<HostUnitState, AppError> {
    inspect_host_unit_at(home, &unit_path())
}

fn inspect_host_unit_at(home: &Home, path: &Path) -> Result<HostUnitState, AppError> {
    if !path.exists() {
        return Ok(HostUnitState::Absent);
    }
    let text = std::fs::read_to_string(path)?;
    Ok(classify_configured_home(home, parse_unit_home(&text)))
}

/// Decode the `--home` path from unit text. `None` means absent, ambiguous, or
/// unrecognized daemon invocation.
pub(crate) fn parse_unit_home(text: &str) -> Option<PathBuf> {
    let mut service_sections = 0;
    let mut in_service = false;
    let mut exec_starts = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line == "[Service]" {
            service_sections += 1;
            in_service = true;
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_service = false;
            continue;
        }
        if let Some(exec_start) = line.strip_prefix("ExecStart=") {
            if !in_service {
                return None;
            }
            exec_starts.push(exec_start);
        }
    }
    if service_sections != 1 {
        return None;
    }
    match exec_starts.as_slice() {
        [one] => home_from_daemon_argv(&split_exec_args(one)),
        _ => None,
    }
}

/// Render the unit text.
pub fn render(home: &Home) -> Result<String, AppError> {
    render_with_config(home, None)
}

/// Render the unit with an optional explicit config file.
pub fn render_with_config(home: &Home, config: Option<&Path>) -> Result<String, AppError> {
    let bin = binary_path()?;
    let home = std::path::absolute(home.root())?;
    let mut env_lines = String::new();
    env_lines.push_str(&format!("Environment=PATH={}\n", installer_path()));
    for (key, path) in agent_paths() {
        env_lines.push_str(&format!("Environment={key}={}\n", path.display()));
    }
    if let Some((key, value)) = web_listen_env()? {
        env_lines.push_str(&format!("Environment={key}={value}\n"));
    }
    if let Some(path) = config {
        env_lines.push_str(&format!(
            "Environment={}\n",
            quote_systemd_environment(CONFIG_ENV, &path.display().to_string())?
        ));
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
        home.display(),
    ))
}

/// Write, verify, enable, and start the unit.
pub fn install(home: &Home) -> Result<(), AppError> {
    install_with_config(home, None)
}

/// Write, verify, enable, and start with an optional explicit config file.
pub fn install_with_config(home: &Home, config: Option<&Path>) -> Result<(), AppError> {
    let text = render_with_config(home, config)?;
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
    // uninstall must succeed even when the unit was never enabled
    let _ = run_systemctl(&["disable", "--now", "homebased.service"]);
    let path = unit_path();
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    // a stale generation only matters to systemd, not to this command's result
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
            eprintln!(
                "warning: user lingering is off; `loginctl enable-linger` so homebased starts at login"
            );
        }
    }
}

fn quote_systemd_environment(key: &str, value: &str) -> Result<String, AppError> {
    if value.chars().any(char::is_control) {
        return Err(AppError::UnitInvalid {
            message: "systemd cannot store a config path with control characters".into(),
        });
    }

    let mut quoted = format!("{key}=\"");
    for character in value.chars() {
        match character {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            // systemd expands percent specifiers in unit-file values
            '%' => quoted.push_str("%%"),
            character => quoted.push(character),
        }
    }
    quoted.push('"');
    Ok(quoted)
}

/// Split an `ExecStart=` value on whitespace. Generated units do not quote
/// paths; quoting support is deferred.
fn split_exec_args(rest: &str) -> Vec<&str> {
    rest.split_whitespace().collect()
}

fn home_from_daemon_argv(args: &[&str]) -> Option<PathBuf> {
    // require the exact shape generated by render so unrelated commands cannot claim ownership
    if args.len() != 5
        || args[1] != "daemon"
        || args[2] != "serve"
        || args[3] != "--home"
        || args[4].is_empty()
    {
        return None;
    }
    Some(PathBuf::from(args[4]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::home::Home;

    fn sample_unit(bin: &str, home: &str) -> String {
        format!(
            "[Unit]\n\
             Description=homebased task supervisor\n\
             After=default.target\n\
             \n\
             [Service]\n\
             ExecStart={bin} daemon serve --home {home}\n\
             KillMode=process\n\
             Restart=on-failure\n\
             TimeoutStopSec=15\n\
             Environment=PATH=/usr/bin\n\
             Environment=HOMEBASED_WEB_LISTEN=127.0.0.1:9\n\
             \n\
             [Install]\n\
             WantedBy=default.target\n"
        )
    }

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
        assert!(
            !text.contains(&format!("Environment={CONFIG_ENV}=")),
            "{text}"
        );
        assert!(text.contains("WantedBy=default.target"), "{text}");
        assert!(text.contains("Restart=on-failure"), "{text}");
        assert!(text.contains("TimeoutStopSec=15"), "{text}");
        let exec = text.lines().find(|l| l.starts_with("ExecStart=")).unwrap();
        assert!(exec.contains("/homebased") || exec.starts_with("ExecStart=/"));
    }

    #[test]
    fn unit_quotes_explicit_config_path_in_environment() {
        let home = Home::resolve(Some(PathBuf::from("/tmp/hb-state"))).unwrap();
        let config = Path::new("/tmp/config with \"quotes\" \\ and $value %specifier.toml");

        let text = render_with_config(&home, Some(config)).unwrap();

        assert!(
            text.lines().any(|line| line
                == format!(
                    "Environment={}",
                    quote_systemd_environment(CONFIG_ENV, &config.display().to_string()).unwrap()
                )),
            "{text}"
        );
        assert_eq!(parse_unit_home(&text), Some(PathBuf::from("/tmp/hb-state")));
        assert_eq!(
            quote_systemd_environment(CONFIG_ENV, "/tmp/a \"b\" \\ $HOME %i").unwrap(),
            r#"HOMEBASED_CONFIG="/tmp/a \"b\" \\ $HOME %%i""#
        );
        assert!(quote_systemd_environment(CONFIG_ENV, "/tmp/config\n.toml").is_err());
    }

    #[test]
    fn unit_makes_a_relative_home_absolute() {
        let home = Home::resolve(Some(PathBuf::from("relative-state"))).unwrap();
        let expected = std::path::absolute(home.root()).unwrap();
        let text = render(&home).unwrap();
        assert!(
            text.contains(&format!("daemon serve --home {}", expected.display())),
            "{text}"
        );
    }

    #[test]
    fn parse_selected_home_from_generated_shape() {
        let text = sample_unit("/opt/homebased", "/tmp/hb-state");
        assert_eq!(parse_unit_home(&text), Some(PathBuf::from("/tmp/hb-state")));
    }

    #[test]
    fn parse_ignores_binary_and_environment_changes() {
        let a = sample_unit("/old/homebased", "/tmp/hb-state");
        let b = sample_unit("/new/homebased", "/tmp/hb-state");
        assert_eq!(parse_unit_home(&a), parse_unit_home(&b));
        assert!(a.contains("HOMEBASED_WEB_LISTEN"));
        assert_eq!(parse_unit_home(&a), Some(PathBuf::from("/tmp/hb-state")));
    }

    #[test]
    fn parse_other_home() {
        let text = sample_unit("/usr/bin/homebased", "/tmp/other-home");
        let selected = Home::resolve(Some(PathBuf::from("/tmp/selected-home"))).unwrap();
        assert_eq!(
            classify_configured_home(&selected, parse_unit_home(&text)),
            HostUnitState::OtherHome {
                configured: PathBuf::from("/tmp/other-home"),
            }
        );
    }

    #[test]
    fn parse_rejects_malformed_and_ambiguous() {
        assert_eq!(parse_unit_home("Description=no exec\n"), None);
        assert_eq!(
            parse_unit_home("ExecStart=/bin/homebased daemon serve --home /tmp/a\n"),
            None
        );
        assert_eq!(
            parse_unit_home("ExecStart=/bin/homebased serve --home /tmp/x\n"),
            None
        );
        assert_eq!(
            parse_unit_home(
                "ExecStart=/bin/homebased daemon serve --home /tmp/a\n\
                 ExecStart=/bin/homebased daemon serve --home /tmp/b\n"
            ),
            None
        );
        assert_eq!(
            parse_unit_home(
                "ExecStart=/bin/homebased daemon serve --home /tmp/a daemon serve --home /tmp/b\n"
            ),
            None
        );
        assert_eq!(
            parse_unit_home("ExecStart=/bin/echo daemon serve --home /tmp/a extra\n"),
            None
        );
        assert_eq!(
            parse_unit_home(
                "ExecStart=/bin/homebased daemon serve --home /tmp/a\n\
                 ExecStart=/usr/bin/other\n"
            ),
            None
        );
        assert_eq!(parse_unit_home("ExecStart=/usr/bin/sleep 10\n"), None);
    }

    #[test]
    fn inspect_absent_and_selected() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        std::fs::create_dir_all(&state).unwrap();
        let home = Home::resolve(Some(state.clone())).unwrap();
        let unit = dir.path().join("homebased.service");
        assert_eq!(
            inspect_host_unit_at(&home, &unit).unwrap(),
            HostUnitState::Absent
        );

        std::fs::write(
            &unit,
            sample_unit("/usr/bin/homebased", state.to_str().unwrap()),
        )
        .unwrap();
        assert_eq!(
            inspect_host_unit_at(&home, &unit).unwrap(),
            HostUnitState::SelectedHome
        );

        std::fs::write(&unit, "not a unit\n").unwrap();
        assert_eq!(
            inspect_host_unit_at(&home, &unit).unwrap(),
            HostUnitState::Unrecognized
        );
    }
}
