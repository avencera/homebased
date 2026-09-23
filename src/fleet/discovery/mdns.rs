//! DNS-SD over mDNS for `_homebased._tcp.local`.
//!
//! Homebased answers and browses with its own responder (the `mdns-sd`
//! crate), so no Avahi or Bonjour service is required. The advertisement uses
//! the standard DNS-SD layout, so Avahi and Bonjour can still see it.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

use chrono::Utc;
use mdns_sd::{ResolvedService, ServiceDaemon, ServiceEvent, ServiceInfo};
use tokio::task::JoinHandle;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::fleet::address::MachineAddress;
use crate::fleet::advertisement::MachineHeader;
use crate::fleet::directory::{Sighting, SightingProvider};
use crate::fleet::discovery::{
    ClaimedIdentity, DiscoveryEvent, DiscoverySender, SERVICE_TYPE, is_routable_peer_ip,
    provider_for,
};
use crate::machine::{BootId, MachineId};

/// TXT key for the machine UUID.
pub const TXT_MACHINE: &str = "machine";
/// TXT key for the boot UUID.
pub const TXT_BOOT: &str = "boot";
/// TXT key for the machine name.
pub const TXT_NAME: &str = "name";
/// TXT key for the accepted protocol range, such as `1` or `1-2`.
pub const TXT_PROTOCOL: &str = "proto";
/// TXT key for the Homebased version.
pub const TXT_VERSION: &str = "version";

/// mDNS timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MdnsTimings {
    /// How often the browse restarts, which re-reports live services.
    pub browse_refresh: Duration,
    /// How long one report counts as a live LAN discovery.
    pub sighting_ttl: Duration,
}

impl Default for MdnsTimings {
    fn default() -> Self {
        Self {
            browse_refresh: Duration::from_secs(60),
            sighting_ttl: Duration::from_secs(180),
        }
    }
}

/// What this daemon announces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MdnsAnnouncement {
    /// Identity and protocol fields for the TXT record.
    pub header: MachineHeader,
    /// Listener port.
    pub port: u16,
    /// Specific listener IP, or `None` for every interface address.
    pub ip: Option<IpAddr>,
    /// mDNS host name, such as `code.local.`.
    pub host: String,
}

impl MdnsAnnouncement {
    /// Instance name. The UUID prefix keeps two machines with one name apart,
    /// so the conflict reaches the directory instead of the mDNS responder.
    #[must_use]
    pub fn instance(&self) -> String {
        let uuid = self.header.machine.to_string();
        format!("{}-{}", self.header.name, &uuid[..8])
    }

    fn service_info(&self) -> Result<ServiceInfo, mdns_sd::Error> {
        let properties = [
            (TXT_MACHINE, self.header.machine.to_string()),
            (TXT_BOOT, self.header.boot.to_string()),
            (TXT_NAME, self.header.name.to_string()),
            (TXT_PROTOCOL, self.header.protocol.to_string()),
            (TXT_VERSION, self.header.version.clone()),
        ];
        let instance = self.instance();
        match self.ip {
            Some(ip) => ServiceInfo::new(
                SERVICE_TYPE,
                &instance,
                &self.host,
                ip,
                self.port,
                &properties[..],
            ),
            None => ServiceInfo::new(
                SERVICE_TYPE,
                &instance,
                &self.host,
                (),
                self.port,
                &properties[..],
            )
            .map(ServiceInfo::enable_addr_auto),
        }
    }
}

/// Running mDNS responder and browser.
pub struct MdnsProvider {
    daemon: ServiceDaemon,
    registered: Option<String>,
    browse: JoinHandle<()>,
}

impl MdnsProvider {
    /// Start the responder, announce `announcement` when given, and browse for
    /// peers. Returns an error only when the responder cannot start.
    pub fn start(
        announcement: Option<&MdnsAnnouncement>,
        events: DiscoverySender,
        timings: MdnsTimings,
    ) -> Result<Self, mdns_sd::Error> {
        let daemon = ServiceDaemon::new()?;
        let registered = match announcement.map(MdnsAnnouncement::service_info) {
            Some(Ok(info)) => {
                let fullname = info.get_fullname().to_string();
                daemon.register(info)?;
                Some(fullname)
            }
            Some(Err(err)) => {
                warn!("mDNS announcement skipped: {err}");
                None
            }
            None => None,
        };
        let browse = tokio::spawn(browse_loop(daemon.clone(), events, timings));
        Ok(Self {
            daemon,
            registered,
            browse,
        })
    }

    /// Withdraw the announcement and stop the responder.
    pub fn shutdown(self) {
        self.browse.abort();
        if let Some(fullname) = &self.registered
            && let Err(err) = self.daemon.unregister(fullname)
        {
            debug!("mDNS unregister: {err}");
        }
        if let Err(err) = self.daemon.shutdown() {
            debug!("mDNS shutdown: {err}");
        }
    }
}

async fn browse_loop(daemon: ServiceDaemon, events: DiscoverySender, timings: MdnsTimings) {
    let mut instances: HashMap<String, Vec<MachineAddress>> = HashMap::new();
    loop {
        let receiver = match daemon.browse(SERVICE_TYPE) {
            Ok(receiver) => receiver,
            Err(err) => {
                let failed = DiscoveryEvent::ProviderFailed {
                    provider: SightingProvider::Mdns,
                    message: format!("browse: {err}"),
                };
                if events.send(failed).await.is_err() {
                    return;
                }
                tokio::time::sleep(timings.browse_refresh).await;
                continue;
            }
        };
        let refresh = tokio::time::sleep(timings.browse_refresh);
        tokio::pin!(refresh);
        loop {
            let event = tokio::select! {
                event = receiver.recv_async() => event,
                () = &mut refresh => break,
            };
            let Ok(event) = event else {
                break;
            };
            let reports = handle_event(event, &mut instances, timings.sighting_ttl);
            for report in reports {
                if events.send(report).await.is_err() {
                    return;
                }
            }
        }
        // a stale browse only costs a warning; the next browse replaces it
        if let Err(err) = daemon.stop_browse(SERVICE_TYPE) {
            debug!("mDNS stop_browse: {err}");
        }
    }
}

fn handle_event(
    event: ServiceEvent,
    instances: &mut HashMap<String, Vec<MachineAddress>>,
    ttl: Duration,
) -> Vec<DiscoveryEvent> {
    match event {
        ServiceEvent::ServiceResolved(resolved) => {
            let (addresses, claim) = resolved_addresses(&resolved);
            instances.insert(resolved.fullname.clone(), addresses.clone());
            let expires_at = Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_default();
            addresses
                .into_iter()
                .map(|address| DiscoveryEvent::Seen {
                    sighting: Sighting {
                        provider: provider_for(&address, SightingProvider::Mdns),
                        address,
                        expires_at,
                    },
                    claim,
                })
                .collect()
        }
        ServiceEvent::ServiceRemoved(_, fullname) => instances
            .remove(&fullname)
            .unwrap_or_default()
            .into_iter()
            .map(|address| DiscoveryEvent::Withdrawn {
                provider: provider_for(&address, SightingProvider::Mdns),
                address,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Peer addresses and TXT identity claim of one resolved service.
fn resolved_addresses(
    resolved: &ResolvedService,
) -> (Vec<MachineAddress>, Option<ClaimedIdentity>) {
    if !resolved.is_valid() {
        return (Vec::new(), None);
    }
    let mut addresses: Vec<MachineAddress> = resolved
        .get_addresses()
        .iter()
        .map(mdns_sd::ScopedIp::to_ip_addr)
        .filter(|ip| is_routable_peer_ip(*ip))
        .map(|ip| MachineAddress::from_socket(SocketAddr::new(ip, resolved.port)))
        .collect();
    addresses.sort();
    addresses.dedup();
    let claim = claimed_identity(
        resolved.get_property_val_str(TXT_MACHINE),
        resolved.get_property_val_str(TXT_BOOT),
    );
    (addresses, claim)
}

fn claimed_identity(machine: Option<&str>, boot: Option<&str>) -> Option<ClaimedIdentity> {
    let machine = Uuid::from_str(machine?).ok()?;
    let boot = Uuid::from_str(boot?).ok()?;
    Some(ClaimedIdentity {
        machine: MachineId::from_uuid(machine),
        boot: BootId::from_uuid(boot),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::protocol::SUPPORTED_PROTOCOLS;
    use crate::machine::MachineName;

    fn announcement() -> MdnsAnnouncement {
        MdnsAnnouncement {
            header: MachineHeader {
                machine: MachineId::new(),
                boot: BootId::new(),
                name: MachineName::parse("code").unwrap(),
                version: "0.3.0".into(),
                protocol: SUPPORTED_PROTOCOLS,
            },
            port: 7677,
            ip: Some("10.0.0.2".parse().unwrap()),
            host: "code.local.".into(),
        }
    }

    #[test]
    fn service_info_carries_identity_txt() {
        let announcement = announcement();
        let info = announcement.service_info().unwrap();
        assert!(info.get_fullname().starts_with("code-"));
        assert!(info.get_fullname().ends_with(SERVICE_TYPE));
        assert_eq!(
            info.get_property_val_str(TXT_MACHINE),
            Some(announcement.header.machine.to_string().as_str())
        );
        assert_eq!(
            info.get_property_val_str(TXT_BOOT),
            Some(announcement.header.boot.to_string().as_str())
        );
        assert_eq!(
            info.get_property_val_str(TXT_PROTOCOL),
            Some(SUPPORTED_PROTOCOLS.to_string().as_str())
        );
    }

    #[test]
    fn claim_requires_both_uuids() {
        let machine = MachineId::new().to_string();
        let boot = BootId::new().to_string();
        assert!(claimed_identity(Some(&machine), Some(&boot)).is_some());
        assert!(claimed_identity(Some(&machine), None).is_none());
        assert!(claimed_identity(Some("x"), Some(&boot)).is_none());
    }

    #[test]
    fn removal_withdraws_the_instance_addresses() {
        let mut instances = HashMap::new();
        let address: MachineAddress = "http://10.0.0.2:7677".parse().unwrap();
        instances.insert(
            "code-1._homebased._tcp.local.".to_string(),
            vec![address.clone()],
        );
        let events = handle_event(
            ServiceEvent::ServiceRemoved(
                SERVICE_TYPE.to_string(),
                "code-1._homebased._tcp.local.".to_string(),
            ),
            &mut instances,
            Duration::from_secs(1),
        );
        assert_eq!(
            events,
            vec![DiscoveryEvent::Withdrawn {
                address,
                provider: SightingProvider::Mdns
            }]
        );
        assert!(instances.is_empty());
    }
}
