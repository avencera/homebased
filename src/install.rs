//! Host-unit generators.

pub mod launchd;

#[cfg(target_os = "linux")]
pub mod systemd;

use std::path::{Path, PathBuf};

use crate::error::AppError;
use crate::home::Home;

/// Whether the installed host unit belongs to the selected home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostUnitState {
    /// No unit file at the platform path.
    Absent,
    /// Unit's `--home` matches the selected home.
    SelectedHome,
    /// Unit exists but points at a different home.
    OtherHome {
        /// Home path decoded from the unit descriptor.
        configured: PathBuf,
    },
    /// Unit exists but the daemon invocation could not be parsed.
    Unrecognized,
}

/// Render the native host unit.
pub fn render(home: &Home) -> Result<String, AppError> {
    render_with_config(home, None)
}

/// Render the native host unit with an optional explicit config file.
pub fn render_with_config(home: &Home, config: Option<&Path>) -> Result<String, AppError> {
    cfg_select! {
        target_os = "macos" => launchd::render_with_config(home, config),
        target_os = "linux" => systemd::render_with_config(home, config),
        _ => Err(unsupported_host()),
    }
}

/// Install the native host unit.
pub fn install(home: &Home) -> Result<(), AppError> {
    install_with_config(home, None)
}

/// Install the native host unit with an optional explicit config file.
pub fn install_with_config(home: &Home, config: Option<&Path>) -> Result<(), AppError> {
    cfg_select! {
        target_os = "macos" => launchd::install_with_config(home, config),
        target_os = "linux" => systemd::install_with_config(home, config),
        _ => Err(unsupported_host()),
    }
}

/// Remove the native host unit. Does not delete the database.
pub fn uninstall(home: &Home) -> Result<(), AppError> {
    let state = inspect_host_unit(home)?;
    match state {
        HostUnitState::Absent => Ok(()),
        HostUnitState::SelectedHome => {
            cfg_select! {
                target_os = "macos" => launchd::uninstall(),
                target_os = "linux" => systemd::uninstall(),
                _ => Ok(()),
            }
        }
        HostUnitState::OtherHome { configured } => Err(AppError::HostUnitHomeMismatch {
            selected: home.root().to_path_buf(),
            configured,
        }),
        HostUnitState::Unrecognized => Err(AppError::UnitInvalid {
            message: "host unit daemon invocation is unrecognized".into(),
        }),
    }
}

/// Path of the unit or plist.
#[must_use]
pub fn unit_path() -> PathBuf {
    cfg_select! {
        target_os = "macos" => launchd::plist_path(),
        target_os = "linux" => systemd::unit_path(),
        _ => PathBuf::from("homebased.service"),
    }
}

/// Inspect whether the installed host unit belongs to `home`.
pub fn inspect_host_unit(home: &Home) -> Result<HostUnitState, AppError> {
    cfg_select! {
        target_os = "macos" => launchd::inspect_host_unit(home),
        target_os = "linux" => systemd::inspect_host_unit(home),
        _ => Ok(HostUnitState::Absent),
    }
}

/// Stop via the host supervisor.
pub fn host_stop() -> Result<(), AppError> {
    cfg_select! {
        target_os = "macos" => launchd::host_stop(),
        target_os = "linux" => systemd::host_stop(),
        _ => Ok(()),
    }
}

/// Reload the unit file, then restart via the host supervisor.
pub fn host_restart() -> Result<(), AppError> {
    cfg_select! {
        target_os = "macos" => launchd::host_restart(),
        target_os = "linux" => systemd::host_restart(),
        _ => Ok(()),
    }
}

/// Environment variable that sets the dashboard bind.
pub const WEB_LISTEN_ENV: &str = "HOMEBASED_WEB_LISTEN";

/// Kind name for JSON.
#[must_use]
pub fn kind_name() -> &'static str {
    cfg_select! {
        target_os = "macos" => "launchd",
        target_os = "linux" => "systemd",
        _ => "none",
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn unsupported_host() -> AppError {
    AppError::Internal {
        message: "host unit install is Linux or macOS only".into(),
    }
}

/// Absolute paths for agent binaries from the current environment.
#[must_use]
pub fn agent_paths() -> Vec<(&'static str, PathBuf)> {
    use crate::domain::AgentKind;
    let mut out = Vec::new();
    for kind in [
        AgentKind::Codex,
        AgentKind::Claude,
        AgentKind::Grok,
        AgentKind::OpenCode,
    ] {
        if let Ok(path) = crate::invocation::resolve_agent_binary(
            kind,
            &std::env::var("PATH").unwrap_or_default(),
            Path::new("."),
        ) {
            out.push((kind.binary_env(), path));
        }
    }
    out
}

/// Dashboard bind baked into the unit, when the installing shell sets
/// `HOMEBASED_WEB_LISTEN`. Unset leaves the dashboard off. Validated here so a
/// typo fails `install`, not the unit at boot.
pub fn web_listen_env() -> Result<Option<(&'static str, String)>, AppError> {
    let Ok(raw) = std::env::var(WEB_LISTEN_ENV) else {
        return Ok(None);
    };
    let listen: crate::daemon::web::WebListen = raw.parse()?;
    Ok(Some((WEB_LISTEN_ENV, listen.to_string())))
}

/// Current PATH for the unit Environment=.
#[must_use]
pub fn installer_path() -> String {
    std::env::var("PATH").unwrap_or_default()
}

/// Absolute binary used as ExecStart / ProgramArguments.
pub fn binary_path() -> Result<PathBuf, AppError> {
    let exe = std::env::current_exe().map_err(|err| AppError::Internal {
        message: err.to_string(),
    })?;
    std::fs::canonicalize(&exe).or(Ok(exe))
}

/// Compare two home paths by filesystem identity when both exist, else by
/// absolute normalization.
pub(crate) fn homes_equivalent(selected: &Path, configured: &Path) -> bool {
    let selected_abs = absolute_normalize(selected);
    let configured_abs = absolute_normalize(configured);
    match (
        std::fs::canonicalize(&selected_abs),
        std::fs::canonicalize(&configured_abs),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => selected_abs == configured_abs,
    }
}

fn absolute_normalize(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Classify a decoded home path against the selected home.
pub(crate) fn classify_configured_home(
    selected: &Home,
    configured: Option<PathBuf>,
) -> HostUnitState {
    let Some(configured) = configured else {
        return HostUnitState::Unrecognized;
    };
    if !configured.is_absolute() {
        return HostUnitState::Unrecognized;
    }
    if homes_equivalent(selected.root(), &configured) {
        HostUnitState::SelectedHome
    } else {
        HostUnitState::OtherHome { configured }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_paths_compare_equal() {
        assert!(homes_equivalent(
            Path::new("/tmp/hb-state"),
            Path::new("/tmp/hb-state")
        ));
        assert!(!homes_equivalent(
            Path::new("/tmp/hb-a"),
            Path::new("/tmp/hb-b")
        ));
    }

    #[test]
    fn canonical_paths_compare_equal_when_both_exist() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real-home");
        std::fs::create_dir_all(&real).unwrap();
        let link = dir.path().join("link-home");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(homes_equivalent(&real, &link));
    }

    #[test]
    fn classify_none_is_unrecognized() {
        let home = Home::resolve(Some(PathBuf::from("/tmp/hb-state"))).unwrap();
        assert_eq!(
            classify_configured_home(&home, None),
            HostUnitState::Unrecognized
        );
    }

    #[test]
    fn classify_relative_configured_home_is_unrecognized() {
        let home = Home::resolve(Some(PathBuf::from("relative-state"))).unwrap();
        assert_eq!(
            classify_configured_home(&home, Some(PathBuf::from("relative-state"))),
            HostUnitState::Unrecognized
        );
    }
}
