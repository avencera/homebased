//! Host-unit generators.

#[cfg(target_os = "linux")]
pub mod systemd;
#[cfg(not(target_os = "linux"))]
pub mod systemd {
    //! Stub for non-Linux hosts.
    use crate::error::AppError;
    use crate::home::Home;
    use std::path::PathBuf;

    /// Always fails: systemd units exist only on Linux.
    pub fn render(_home: &Home) -> Result<String, AppError> {
        Err(AppError::Internal {
            message: "systemd install is Linux-only".into(),
        })
    }

    /// Always fails with the same error as `render`.
    pub fn install(home: &Home) -> Result<(), AppError> {
        render(home).map(|_| ())
    }

    /// No-op: nothing can be installed on this host.
    pub fn uninstall() -> Result<(), AppError> {
        Ok(())
    }

    /// Unit file name, for messages only.
    #[must_use]
    pub fn unit_path() -> PathBuf {
        PathBuf::from("homebased.service")
    }

    /// Always false.
    #[must_use]
    pub fn unit_installed() -> bool {
        false
    }

    /// No-op.
    pub fn host_stop() -> Result<(), AppError> {
        Ok(())
    }

    /// No-op.
    pub fn host_restart() -> Result<(), AppError> {
        Ok(())
    }
}

pub mod launchd;

use crate::error::AppError;
use crate::home::Home;

/// Render the native host unit.
pub fn render(home: &Home) -> Result<String, AppError> {
    if cfg!(target_os = "macos") {
        launchd::render(home)
    } else {
        systemd::render(home)
    }
}

/// Install the native host unit.
pub fn install(home: &Home) -> Result<(), AppError> {
    if cfg!(target_os = "macos") {
        launchd::install(home)
    } else {
        systemd::install(home)
    }
}

/// Remove the native host unit. Does not delete the database.
pub fn uninstall() -> Result<(), AppError> {
    if cfg!(target_os = "macos") {
        launchd::uninstall()
    } else {
        systemd::uninstall()
    }
}

/// Path of the unit or plist.
#[must_use]
pub fn unit_path() -> std::path::PathBuf {
    if cfg!(target_os = "macos") {
        launchd::plist_path()
    } else {
        systemd::unit_path()
    }
}

/// Whether a host unit is present.
#[must_use]
pub fn unit_installed() -> bool {
    if cfg!(target_os = "macos") {
        launchd::plist_path().exists()
    } else {
        systemd::unit_installed()
    }
}

/// Stop via the host supervisor.
pub fn host_stop() -> Result<(), AppError> {
    if cfg!(target_os = "macos") {
        launchd::host_stop()
    } else {
        systemd::host_stop()
    }
}

/// Restart via the host supervisor.
pub fn host_restart() -> Result<(), AppError> {
    if cfg!(target_os = "macos") {
        launchd::host_restart()
    } else {
        systemd::host_restart()
    }
}

/// Environment variable that sets the dashboard bind.
pub const WEB_LISTEN_ENV: &str = "HOMEBASED_WEB_LISTEN";

/// Kind name for JSON.
#[must_use]
pub fn kind_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "launchd"
    } else {
        "systemd"
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
