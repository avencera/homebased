//! `config.toml`: declarative machine and fleet settings.
//!
//! The file is the source of truth for managed peers. Machine UUIDs, the
//! discovery cache, and runtime state stay in the state directory. The file is
//! optional at its default path; a path chosen with `--config` or
//! `HOMEBASED_CONFIG` must exist, because a managed installation that points at
//! a missing file is misconfigured.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::daemon::web::DEFAULT_PORT;
use crate::error::AppError;
use crate::fleet::address::MachineAddress;
use crate::machine::{MachineName, host_machine_name};

/// Environment variable that selects the config file.
pub const CONFIG_ENV: &str = "HOMEBASED_CONFIG";

/// Which config file to read and how it was chosen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "source", content = "path", rename_all = "snake_case")]
pub enum ConfigLocation {
    /// `--config` or `HOMEBASED_CONFIG`. Must exist.
    Explicit(PathBuf),
    /// `~/.config/homebased/config.toml`. Optional.
    Default(PathBuf),
}

impl ConfigLocation {
    /// Resolve from the CLI value, which clap already fills from
    /// `HOMEBASED_CONFIG`, or fall back to `$HOME/.config/homebased/config.toml`.
    pub fn resolve(explicit: Option<PathBuf>) -> Result<Self, AppError> {
        if let Some(path) = explicit.filter(|path| !path.as_os_str().is_empty()) {
            return Ok(Self::Explicit(path));
        }
        let home = std::env::var_os("HOME")
            .filter(|home| !home.is_empty())
            .ok_or_else(|| AppError::Internal {
                message: "HOME is unset".into(),
            })?;
        Ok(Self::Default(
            PathBuf::from(home).join(".config/homebased/config.toml"),
        ))
    }

    /// File path.
    #[must_use]
    pub fn path(&self) -> &Path {
        match self {
            Self::Explicit(path) | Self::Default(path) => path,
        }
    }

    /// Read and validate the file.
    pub fn load(&self) -> Result<Config, AppError> {
        let text = match fs::read_to_string(self.path()) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return match self {
                    Self::Default(_) => Ok(Config::default()),
                    Self::Explicit(path) => Err(AppError::ConfigInvalid {
                        path: path.clone(),
                        message: "file not found".into(),
                    }),
                };
            }
            Err(err) => {
                return Err(AppError::ConfigInvalid {
                    path: self.path().to_path_buf(),
                    message: err.to_string(),
                });
            }
        };
        Config::parse(&text).map_err(|message| AppError::ConfigInvalid {
            path: self.path().to_path_buf(),
            message,
        })
    }

    /// Validate the selected file for daemon installation and return an
    /// explicit path in absolute form for the installed host unit.
    pub fn validated_host_unit_override(&self) -> Result<Option<PathBuf>, AppError> {
        let explicit = match self {
            Self::Explicit(path) => Some(std::path::absolute(path)?),
            Self::Default(_) => None,
        };
        match &explicit {
            Some(path) => Self::Explicit(path.clone()).load()?,
            None => self.load()?,
        };
        Ok(explicit)
    }
}

/// Validated configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Config {
    /// Name from `fleet.machine_name`, when set.
    pub machine_name: Option<MachineName>,
    /// Fleet support.
    pub fleet: Fleet,
}

/// Whether this machine joins a fleet.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Fleet {
    /// Local tasks only. No cluster routes, discovery, or advertisement.
    #[default]
    Disabled,
    /// Cluster routes, discovery, and advertisement are on.
    Enabled(FleetSettings),
}

/// Settings that apply only when the fleet is enabled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FleetSettings {
    /// Automatic discovery providers.
    pub discovery: Discovery,
    /// Declared peer addresses, unique after normalization.
    pub machines: Vec<MachineAddress>,
}

/// Automatic discovery providers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Discovery {
    /// DNS-SD over mDNS for `_homebased._tcp.local`.
    pub mdns: bool,
    /// Tailscale peer discovery, when enabled.
    pub tailscale: Option<TailscaleDiscovery>,
}

/// Tailscale peer discovery settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TailscaleDiscovery {
    /// Homebased port to try on each Tailscale peer. Tailscale does not know
    /// which port a peer's daemon listens on.
    pub port: u16,
}

/// Where the effective machine name came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MachineNameSource {
    /// `fleet.machine_name`.
    Configured,
    /// Derived from the host name.
    Hostname,
    /// Neither gave a usable name.
    Fallback,
}

impl Config {
    /// Parse and validate TOML text. The error is a human-readable message
    /// that includes the TOML location when the parser has one.
    pub fn parse(text: &str) -> Result<Self, String> {
        let raw: RawConfig = toml::from_str(text).map_err(|err| err.to_string())?;
        raw.validate()
    }

    /// Effective machine name and its source.
    #[must_use]
    pub fn machine_name(&self) -> (MachineName, MachineNameSource) {
        if let Some(name) = &self.machine_name {
            return (name.clone(), MachineNameSource::Configured);
        }
        if let Some(name) = host_machine_name() {
            return (name, MachineNameSource::Hostname);
        }
        (MachineName::fallback(), MachineNameSource::Fallback)
    }

    /// Fleet settings when enabled.
    #[must_use]
    pub fn fleet_settings(&self) -> Option<&FleetSettings> {
        match &self.fleet {
            Fleet::Enabled(settings) => Some(settings),
            Fleet::Disabled => None,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    fleet: RawFleet,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFleet {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    machine_name: Option<MachineName>,
    #[serde(default)]
    discovery: RawDiscovery,
    #[serde(default)]
    machines: Vec<RawMachine>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDiscovery {
    #[serde(default = "default_true")]
    mdns: bool,
    #[serde(default)]
    tailscale: bool,
    #[serde(default)]
    tailscale_port: Option<u16>,
}

impl Default for RawDiscovery {
    fn default() -> Self {
        Self {
            mdns: true,
            tailscale: false,
            tailscale_port: None,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMachine {
    address: MachineAddress,
}

impl RawConfig {
    fn validate(self) -> Result<Config, String> {
        let RawFleet {
            enabled,
            machine_name,
            discovery,
            machines,
        } = self.fleet;
        let discovery = discovery.validate()?;
        let mut seen = BTreeSet::new();
        let mut addresses = Vec::with_capacity(machines.len());
        for (index, machine) in machines.into_iter().enumerate() {
            if !seen.insert(machine.address.clone()) {
                return Err(format!(
                    "fleet.machines[{index}].address {} is listed more than once",
                    machine.address
                ));
            }
            addresses.push(machine.address);
        }
        let fleet = if enabled {
            Fleet::Enabled(FleetSettings {
                discovery,
                machines: addresses,
            })
        } else {
            Fleet::Disabled
        };
        Ok(Config {
            machine_name,
            fleet,
        })
    }
}

impl RawDiscovery {
    fn validate(self) -> Result<Discovery, String> {
        if self.tailscale_port == Some(0) {
            return Err("fleet.discovery.tailscale_port must not be 0".into());
        }
        if self.tailscale_port.is_some() && !self.tailscale {
            return Err("fleet.discovery.tailscale_port requires tailscale = true".into());
        }
        let tailscale = self.tailscale.then(|| TailscaleDiscovery {
            port: self.tailscale_port.unwrap_or(DEFAULT_PORT),
        });
        Ok(Discovery {
            mdns: self.mdns,
            tailscale,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_file_disables_fleet() {
        let config = Config::parse("").unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn full_example_parses() {
        let config = Config::parse(
            r#"
[fleet]
enabled = true
machine_name = "code"

[fleet.discovery]
mdns = true
tailscale = true

[[fleet.machines]]
address = "http://main:7677"

[[fleet.machines]]
address = "http://training:7677"
"#,
        )
        .unwrap();
        assert_eq!(config.machine_name.as_ref().unwrap().as_str(), "code");
        let settings = config.fleet_settings().unwrap();
        assert!(settings.discovery.mdns);
        assert_eq!(
            settings.discovery.tailscale,
            Some(TailscaleDiscovery { port: DEFAULT_PORT })
        );
        let machines: Vec<String> = settings.machines.iter().map(ToString::to_string).collect();
        assert_eq!(machines, vec!["http://main:7677", "http://training:7677"]);
        assert_eq!(config.machine_name().1, MachineNameSource::Configured);
    }

    #[test]
    fn rejects_unknown_keys_bad_names_and_duplicates() {
        let err = Config::parse("[fleet]\nenabeld = true\n").unwrap_err();
        assert!(err.contains("enabeld"), "{err}");
        let err = Config::parse("[fleet]\nmachine_name = \"Code Box\"\n").unwrap_err();
        assert!(err.contains("machine name"), "{err}");
        let err = Config::parse(
            "[fleet]\nenabled = true\n[[fleet.machines]]\naddress = \"http://main:7677\"\n[[fleet.machines]]\naddress = \"http://MAIN:7677/\"\n",
        )
        .unwrap_err();
        assert!(err.contains("more than once"), "{err}");
        let err = Config::parse("[[fleet.machines]]\naddress = \"https://main\"\n").unwrap_err();
        assert!(err.contains("http://"), "{err}");
        let err = Config::parse("[fleet.discovery]\ntailscale_port = 7000\n").unwrap_err();
        assert!(err.contains("requires tailscale"), "{err}");
    }

    #[test]
    fn missing_default_file_is_default_but_missing_explicit_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let default_location = ConfigLocation::Default(path.clone());
        assert_eq!(default_location.load().unwrap(), Config::default());
        assert_eq!(
            default_location.validated_host_unit_override().unwrap(),
            None
        );
        let err = ConfigLocation::Explicit(path).load().unwrap_err();
        assert_eq!(err.code(), "config_invalid");
    }

    #[test]
    fn explicit_path_wins() {
        let location = ConfigLocation::resolve(Some(PathBuf::from("/etc/hb.toml"))).unwrap();
        assert_eq!(
            location,
            ConfigLocation::Explicit(PathBuf::from("/etc/hb.toml"))
        );
    }

    #[test]
    fn host_unit_override_is_absolute_and_validated() {
        let dir = tempfile::Builder::new()
            .prefix("homebased-config-")
            .tempdir_in(".")
            .unwrap();
        let path = dir.path().join("config.toml");
        fs::write(&path, "[fleet]\nenabled = false\n").unwrap();
        let relative = PathBuf::from(dir.path().file_name().unwrap()).join("config.toml");

        let override_path = ConfigLocation::Explicit(relative)
            .validated_host_unit_override()
            .unwrap()
            .unwrap();
        assert!(override_path.is_absolute());
        assert!(override_path.ends_with("config.toml"));
    }

    #[test]
    fn host_unit_override_rejects_invalid_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        fs::write(&path, "[fleet]\nenabeld = true\n").unwrap();

        let err = ConfigLocation::Explicit(path)
            .validated_host_unit_override()
            .unwrap_err();
        assert_eq!(err.code(), "config_invalid");
    }
}
