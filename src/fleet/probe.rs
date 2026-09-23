//! Machine probes and destination identity checks.
//!
//! An address is a hint. Before a request that reads task data or changes
//! state, the caller probes the address and confirms that it answers for the
//! intended machine UUID. A mismatch means the address now reaches another
//! installation; the caller must invalidate it and keep the original
//! destination UUID.

use http::StatusCode;

use crate::domain::API_VERSION;
use crate::fleet::address::MachineAddress;
use crate::fleet::advertisement::{
    AdvertisementError, MACHINE_PROBE_PATH, MachineAdvertisement, ProbedMachine,
    decode_advertisement,
};
use crate::fleet::http::{ClusterClient, TransportError};
use crate::fleet::protocol::{ClusterProtocolVersion, ProtocolRange};
use crate::machine::{BootId, MachineId};

/// Why a probe did not return a machine advertisement.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProbeError {
    /// No HTTP response.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// HTTP response other than 200, such as a daemon with fleet disabled.
    #[error("probe {address} returned HTTP {status}")]
    Status {
        /// Probed address.
        address: MachineAddress,
        /// HTTP status.
        status: StatusCode,
    },
    /// Body is not a readable advertisement.
    #[error("probe {address}: {source}")]
    Advertisement {
        /// Probed address.
        address: MachineAddress,
        /// Decode failure.
        source: AdvertisementError,
    },
}

/// Probe one address.
pub async fn probe(
    client: &ClusterClient,
    address: &MachineAddress,
    local: ProtocolRange,
) -> Result<ProbedMachine, ProbeError> {
    let path = format!("{MACHINE_PROBE_PATH}?api_version={API_VERSION}");
    let response = client.get(address, &path).await?;
    if response.status != StatusCode::OK {
        return Err(ProbeError::Status {
            address: address.clone(),
            status: response.status,
        });
    }
    decode_advertisement(&response.body, local).map_err(|source| ProbeError::Advertisement {
        address: address.clone(),
        source,
    })
}

/// Address confirmed to answer for the intended machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedDestination {
    /// Machine UUID the request must carry.
    pub machine: MachineId,
    /// Boot UUID of the daemon that answered.
    pub boot: BootId,
    /// Address that answered.
    pub address: MachineAddress,
    /// Protocol version to speak.
    pub protocol: ClusterProtocolVersion,
    /// Full advertisement from the probe.
    pub advertisement: MachineAdvertisement,
}

/// Why an address cannot be used for a destination.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DestinationError {
    /// No usable probe response.
    #[error(transparent)]
    Unreachable(#[from] ProbeError),
    /// The address answers for another installation.
    #[error("{address} answers for machine {found}, expected {expected}")]
    IdentityMismatch {
        /// Probed address.
        address: MachineAddress,
        /// Intended destination.
        expected: MachineId,
        /// Machine that answered.
        found: MachineId,
        /// Full probe result, so the directory can rebind the address.
        probed: Box<ProbedMachine>,
    },
    /// Right machine, but no shared protocol version.
    #[error("{address} speaks cluster protocol {remote}, this daemon accepts {local}")]
    Incompatible {
        /// Probed address.
        address: MachineAddress,
        /// Local range.
        local: ProtocolRange,
        /// Remote range.
        remote: ProtocolRange,
    },
}

/// Probe `address` and confirm it answers for `expected`.
pub async fn verify_destination(
    client: &ClusterClient,
    address: &MachineAddress,
    expected: MachineId,
    local: ProtocolRange,
) -> Result<VerifiedDestination, DestinationError> {
    let probed = probe(client, address, local).await?;
    check_probed(address, expected, probed)
}

/// Identity and protocol check of a probe result, separate from the network
/// call so the fleet runtime can reuse probe results it already holds.
pub fn check_probed(
    address: &MachineAddress,
    expected: MachineId,
    probed: ProbedMachine,
) -> Result<VerifiedDestination, DestinationError> {
    let found = probed.machine();
    if found != expected {
        return Err(DestinationError::IdentityMismatch {
            address: address.clone(),
            expected,
            found,
            probed: Box::new(probed),
        });
    }
    match probed {
        ProbedMachine::Compatible {
            advertisement,
            version,
        } => Ok(VerifiedDestination {
            machine: advertisement.machine,
            boot: advertisement.boot,
            address: address.clone(),
            protocol: version,
            advertisement,
        }),
        ProbedMachine::Incompatible { header, local } => Err(DestinationError::Incompatible {
            address: address.clone(),
            local,
            remote: header.protocol,
        }),
    }
}
