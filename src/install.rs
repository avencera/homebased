//! Host-unit generators.

pub mod launchd;

#[cfg(target_os = "linux")]
pub mod systemd;

use crate::error::AppError;
use crate::home::Home;

/// Render the native host unit.
pub fn render(home: &Home) -> Result<String, AppError> {
    cfg_select! {
        target_os = "macos" => launchd::render(home),
        target_os = "linux" => systemd::render(home),
        _ => Err(unsupported_host()),
    }
}

/// Install the native host unit.
pub fn install(home: &Home) -> Result<(), AppError> {
    cfg_select! {
        target_os = "macos" => launchd::install(home),
        target_os = "linux" => systemd::install(home),
        _ => Err(unsupported_host()),
    }
}

/// Remove the native host unit. Does not delete the database.
pub fn uninstall() -> Result<(), AppError> {
    cfg_select! {
        target_os = "macos" => launchd::uninstall(),
        target_os = "linux" => systemd::uninstall(),
        _ => Ok(()),
    }
}

/// Path of the unit or plist.
#[must_use]
pub fn unit_path() -> std::path::PathBuf {
    cfg_select! {
        target_os = "macos" => launchd::plist_path(),
        target_os = "linux" => systemd::unit_path(),
        _ => std::path::PathBuf::from("homebased.service"),
    }
}

/// Whether a host unit is present.
#[must_use]
pub fn unit_installed() -> bool {
    cfg_select! {
        target_os = "macos" => launchd::plist_path().exists(),
        target_os = "linux" => systemd::unit_installed(),
        _ => false,
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

/// Restart via the host supervisor.
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
pub fn agent_paths() -> Vec<(&'static str, std::path::PathBuf)> {
    use crate::domain::AgentKind;
    let mut out = Vec::new();
    for kind in [AgentKind::Codex, AgentKind::Claude, AgentKind::Grok] {
        if let Ok(path) = crate::agents::resolve_binary(
            kind,
            &std::env::var("PATH").unwrap_or_default(),
            std::path::Path::new("."),
        ) {
            out.push((kind.binary_env(), path));
        }
    }
    out
}

/// Dashboard bind baked into the unit, when the installing shell sets
/// `HOMEBASED_WEB_LISTEN`. Validated here so a typo fails `install`, not the
/// unit at boot.
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
pub fn binary_path() -> Result<std::path::PathBuf, AppError> {
    let exe = std::env::current_exe().map_err(|err| AppError::Internal {
        message: err.to_string(),
    })?;
    std::fs::canonicalize(&exe).or(Ok(exe))
}
