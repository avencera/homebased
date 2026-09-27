//! Host sleep policy for the daemon.
//!
//! A machine that serves agents, tunnels, or fleet peers must not idle-sleep:
//! outbound connections such as a T3 Connect tunnel drop on every sleep, and
//! nothing inbound wakes the host to restore them. macOS lets any user process
//! hold a power assertion, so the LaunchAgent can keep the host awake without
//! a root `pmset` change that each update or reinstall would have to repeat.

use serde::Serialize;
use tracing::warn;

#[cfg(target_os = "macos")]
mod macos;

/// Whether the daemon keeps the host awake while it runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SleepPolicy {
    /// Leave sleep to the operating system.
    #[default]
    Allow,
    /// Block idle system sleep. The display can still sleep, and an explicit
    /// sleep request still works.
    PreventIdle,
}

/// Keeps the host awake until it is dropped. Hold it for the life of the
/// daemon; the operating system also releases it when the process exits.
#[derive(Debug)]
pub struct AwakeGuard {
    #[cfg(target_os = "macos")]
    _assertion: macos::IdleSleepAssertion,
}

impl SleepPolicy {
    /// Apply the policy. `None` means the host may sleep: the policy allows
    /// it, the platform has no user-level control, or the request failed.
    /// A failure is logged and does not stop the daemon.
    #[must_use]
    pub fn apply(self) -> Option<AwakeGuard> {
        match self {
            Self::Allow => None,
            Self::PreventIdle => prevent_idle_sleep(),
        }
    }
}

#[cfg(target_os = "macos")]
fn prevent_idle_sleep() -> Option<AwakeGuard> {
    match macos::IdleSleepAssertion::create(c"homebased daemon keep_awake") {
        Ok(assertion) => {
            tracing::info!("keeping the host awake: idle system sleep is blocked");
            Some(AwakeGuard {
                _assertion: assertion,
            })
        }
        Err(code) => {
            warn!("power.keep_awake failed: IOPMAssertionCreateWithName returned {code:#x}");
            None
        }
    }
}

// systemd hosts use `systemd-inhibit` or logind settings, which need a
// session policy rather than a daemon-held handle
#[cfg(not(target_os = "macos"))]
fn prevent_idle_sleep() -> Option<AwakeGuard> {
    warn!("power.keep_awake is only supported on macOS; the host may still sleep");
    None
}
