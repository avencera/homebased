//! Machine advertisement: the body of the cluster machine probe
//!
//! The probe establishes identity and negotiates the protocol, so it must stay
//! readable across a rolling update. Decoding therefore happens in two steps:
//! a lenient [`MachineHeader`] that every protocol version keeps, then the
//! full [`MachineAdvertisement`] only when the protocol ranges overlap. Other
//! cluster request and response bodies stay strict

use std::ffi::OsString;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::domain::{API_VERSION, AgentKind};
use crate::fleet::address::MachineAddress;
use crate::fleet::protocol::{ClusterProtocolVersion, Compatibility, ProtocolRange};
use crate::machine::{BootId, MachineId, MachineName};

/// Path of the cluster machine probe on every Homebased TCP listener
pub const MACHINE_PROBE_PATH: &str = "/v1/cluster/machine";

/// Identity and protocol fields that every cluster protocol version keeps
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineHeader {
    /// Stable machine UUID
    pub machine: MachineId,
    /// UUID of the answering daemon's current start
    pub boot: BootId,
    /// Display name
    pub name: MachineName,
    /// Homebased crate version
    pub version: String,
    /// Accepted cluster protocol versions
    pub protocol: ProtocolRange,
}

/// What a machine can run
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// `std::env::consts::OS`, such as `macos` or `linux`
    pub os: String,
    /// `std::env::consts::ARCH`, such as `aarch64` or `x86_64`
    pub arch: String,
    /// Agents whose binaries the daemon found. Names this build does not know
    /// are dropped while decoding, so a newer peer with a new agent stays
    /// readable
    #[serde(deserialize_with = "known_agents")]
    pub agents: Vec<AgentKind>,
}

impl Capabilities {
    /// Detect the local OS, architecture, and agent binaries on the daemon's
    /// `PATH` or pinned by the `HOMEBASED_<AGENT>` overrides
    #[must_use]
    pub fn detect(path: &str, cwd: &Path) -> Self {
        let agents = AgentKind::ALL
            .into_iter()
            .filter(|kind| crate::invocation::resolve_agent_binary(*kind, path, cwd).is_ok())
            .collect();
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            agents,
        }
    }

    /// Detect with the daemon's own `PATH` and `HOME`
    #[must_use]
    pub fn detect_for_daemon() -> Self {
        let path = std::env::var_os("PATH").unwrap_or_default();
        let home = std::env::var_os("HOME").unwrap_or_else(|| OsString::from("/"));
        Self::detect(&path.to_string_lossy(), Path::new(&home))
    }
}

fn known_agents<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<AgentKind>, D::Error> {
    let raw = Vec::<serde_json::Value>::deserialize(deserializer)?;
    let mut agents: Vec<AgentKind> = Vec::new();
    for agent in raw
        .into_iter()
        .filter_map(|value| serde_json::from_value(value).ok())
    {
        if !agents.contains(&agent) {
            agents.push(agent);
        }
    }
    Ok(agents)
}

/// Full probe body, `GET /v1/cluster/machine`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineAdvertisement {
    /// Public JSON schema version
    pub api_version: u32,
    /// Stable machine UUID
    pub machine: MachineId,
    /// UUID of the answering daemon's current start
    pub boot: BootId,
    /// Display name
    pub name: MachineName,
    /// Homebased crate version
    pub version: String,
    /// Accepted cluster protocol versions
    pub protocol: ProtocolRange,
    /// OS, architecture, and installed agents
    pub capabilities: Capabilities,
    /// Base URLs where the machine expects to be reachable. Hints only: a
    /// caller trusts an address after its own probe confirms the identity
    pub addresses: Vec<MachineAddress>,
}

impl MachineAdvertisement {
    /// Identity and protocol part
    #[must_use]
    pub fn header(&self) -> MachineHeader {
        MachineHeader {
            machine: self.machine,
            boot: self.boot,
            name: self.name.clone(),
            version: self.version.clone(),
            protocol: self.protocol,
        }
    }
}

/// Decoded probe response
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbedMachine {
    /// Protocol ranges overlap; the full advertisement is available
    Compatible {
        /// Full advertisement
        advertisement: MachineAdvertisement,
        /// Highest shared protocol version
        version: ClusterProtocolVersion,
    },
    /// No shared protocol version. The machine stays visible by identity
    Incompatible {
        /// Identity and protocol fields
        header: MachineHeader,
        /// Local accepted range
        local: ProtocolRange,
    },
}

impl ProbedMachine {
    /// Identity and protocol fields in either case
    #[must_use]
    pub fn header(&self) -> MachineHeader {
        match self {
            Self::Compatible { advertisement, .. } => advertisement.header(),
            Self::Incompatible { header, .. } => header.clone(),
        }
    }

    /// Stable machine UUID
    #[must_use]
    pub fn machine(&self) -> MachineId {
        match self {
            Self::Compatible { advertisement, .. } => advertisement.machine,
            Self::Incompatible { header, .. } => header.machine,
        }
    }

    /// Boot UUID of the answering daemon
    #[must_use]
    pub fn boot(&self) -> BootId {
        match self {
            Self::Compatible { advertisement, .. } => advertisement.boot,
            Self::Incompatible { header, .. } => header.boot,
        }
    }
}

/// Why a probe body could not be decoded
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdvertisementError {
    /// Not a Homebased machine advertisement
    #[error("invalid machine advertisement: {0}")]
    Invalid(String),
    /// Public JSON schema version this build does not read
    #[error("unsupported api_version {0}")]
    ApiVersion(u32),
}

/// Decode a probe body against the local accepted protocol range
pub fn decode_advertisement(
    body: &[u8],
    local: ProtocolRange,
) -> Result<ProbedMachine, AdvertisementError> {
    #[derive(Deserialize)]
    struct Versioned {
        api_version: u32,
        #[serde(flatten)]
        header: MachineHeader,
    }
    let versioned: Versioned =
        serde_json::from_slice(body).map_err(|err| AdvertisementError::Invalid(err.to_string()))?;
    if versioned.api_version != API_VERSION {
        return Err(AdvertisementError::ApiVersion(versioned.api_version));
    }
    let header = versioned.header;
    match local.negotiate(&header.protocol) {
        Compatibility::Incompatible { .. } => Ok(ProbedMachine::Incompatible { header, local }),
        Compatibility::Compatible { version } => {
            let advertisement = serde_json::from_slice(body)
                .map_err(|err| AdvertisementError::Invalid(err.to_string()))?;
            Ok(ProbedMachine::Compatible {
                advertisement,
                version,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AdvertisementError, ProbedMachine, decode_advertisement};
    use crate::domain::AgentKind;
    use crate::fleet::protocol::{ClusterProtocolVersion, SUPPORTED_PROTOCOLS};
    use serde_json::json;

    fn body(protocol: serde_json::Value, extra: serde_json::Value) -> Vec<u8> {
        let mut value = json!({
            "api_version": 1,
            "machine": "01234567-89ab-7cde-8f01-23456789abcd",
            "boot": "01234567-89ab-7cde-8f01-23456789abce",
            "name": "code",
            "version": "0.3.0",
            "protocol": protocol,
            "capabilities": { "os": "linux", "arch": "x86_64", "agents": ["codex", "future-agent"] },
            "addresses": ["http://code:7677"],
        });
        if let (Some(obj), Some(more)) = (value.as_object_mut(), extra.as_object()) {
            obj.extend(more.clone());
        }
        serde_json::to_vec(&value).unwrap()
    }

    #[test]
    fn compatible_probe_decodes_fully_and_drops_unknown_agents() {
        let probed = decode_advertisement(
            &body(json!({"min":2,"max":2}), json!({})),
            SUPPORTED_PROTOCOLS,
        )
        .unwrap();
        let ProbedMachine::Compatible {
            advertisement,
            version,
        } = probed
        else {
            panic!("expected compatible");
        };
        assert_eq!(version, ClusterProtocolVersion(2));
        assert_eq!(advertisement.capabilities.agents, vec![AgentKind::Codex]);
        assert_eq!(advertisement.name.as_str(), "code");
    }

    #[test]
    fn incompatible_probe_keeps_identity_even_with_new_shape() {
        let probed = decode_advertisement(
            &body(
                json!({"min":7,"max":9}),
                json!({"capabilities": "a newer shape", "addresses": 3}),
            ),
            SUPPORTED_PROTOCOLS,
        )
        .unwrap();
        let ProbedMachine::Incompatible { header, .. } = probed else {
            panic!("expected incompatible");
        };
        assert_eq!(header.name.as_str(), "code");
    }

    #[test]
    fn rejects_other_api_version_and_garbage() {
        let err = decode_advertisement(
            &body(json!({"min":2,"max":2}), json!({"api_version": 2})),
            SUPPORTED_PROTOCOLS,
        )
        .unwrap_err();
        assert_eq!(err, AdvertisementError::ApiVersion(2));
        assert!(matches!(
            decode_advertisement(b"<html>", SUPPORTED_PROTOCOLS),
            Err(AdvertisementError::Invalid(_))
        ));
    }
}
