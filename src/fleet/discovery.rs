//! Automatic discovery providers.
//!
//! Each provider runs as its own task and reports [`DiscoveryEvent`]s on a
//! channel. A provider never blocks daemon startup, bounds each external call
//! with a time limit, and reports its own failures as events, so one failed
//! provider does not stop another. Every reported address is only a
//! candidate: the fleet runtime probes it before it binds to a machine.

pub mod mdns;
pub mod tailscale;

use std::net::IpAddr;

use tokio::sync::mpsc;

use crate::fleet::address::MachineAddress;
use crate::fleet::directory::{Sighting, SightingProvider};
use crate::machine::{BootId, MachineId};

/// DNS-SD service type for Homebased daemons.
pub const SERVICE_TYPE: &str = "_homebased._tcp.local.";

/// Identity that a provider read from an advertisement, before any probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimedIdentity {
    /// Claimed machine UUID.
    pub machine: MachineId,
    /// Claimed boot UUID.
    pub boot: BootId,
}

/// Report from a provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryEvent {
    /// A candidate address is live.
    Seen {
        /// Address and expiry.
        sighting: Sighting,
        /// Identity from the advertisement, when the provider has one. Used
        /// only to skip this daemon's own advertisement.
        claim: Option<ClaimedIdentity>,
    },
    /// A provider reported that an address went away.
    Withdrawn {
        /// Address.
        address: MachineAddress,
        /// Provider.
        provider: SightingProvider,
    },
    /// A provider scan failed. Other providers continue.
    ProviderFailed {
        /// Provider.
        provider: SightingProvider,
        /// Failure text.
        message: String,
    },
}

/// Sender side that providers use.
pub type DiscoverySender = mpsc::Sender<DiscoveryEvent>;

/// Whether an IP is usable as a peer address from another host.
pub(crate) fn is_routable_peer_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => !(v4.is_loopback() || v4.is_unspecified() || v4.is_link_local()),
        // link-local IPv6 needs a scope id that a base URL cannot carry
        IpAddr::V6(v6) => {
            !(v6.is_loopback() || v6.is_unspecified() || (v6.segments()[0] & 0xffc0) == 0xfe80)
        }
    }
}

/// Network class for an address that a provider reported. An mDNS answer can
/// carry a Tailscale interface address; rank it as Tailscale, not LAN.
pub(crate) fn provider_for(
    address: &MachineAddress,
    reported_by: SightingProvider,
) -> SightingProvider {
    if address.is_tailscale() {
        return SightingProvider::Tailscale;
    }
    reported_by
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routable_ips() {
        assert!(is_routable_peer_ip("192.168.1.2".parse().unwrap()));
        assert!(is_routable_peer_ip("fd00::2".parse().unwrap()));
        assert!(!is_routable_peer_ip("127.0.0.1".parse().unwrap()));
        assert!(!is_routable_peer_ip("169.254.1.1".parse().unwrap()));
        assert!(!is_routable_peer_ip("fe80::1".parse().unwrap()));
    }

    #[test]
    fn tailscale_address_from_mdns_ranks_as_tailscale() {
        let address: MachineAddress = "http://100.100.1.2:7677".parse().unwrap();
        assert_eq!(
            provider_for(&address, SightingProvider::Mdns),
            SightingProvider::Tailscale
        );
        let lan: MachineAddress = "http://10.0.0.2:7677".parse().unwrap();
        assert_eq!(
            provider_for(&lan, SightingProvider::Mdns),
            SightingProvider::Mdns
        );
    }
}
