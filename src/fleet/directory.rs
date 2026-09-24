//! Durable peer directory.
//!
//! The directory merges addresses from configuration, explicit CLI additions,
//! mDNS, Tailscale, and earlier successful probes. Addresses bind to a stable
//! [`MachineId`] only after a probe confirms the identity, so a changed address
//! never changes identity and a stale address never redirects work.
//!
//! The directory is plain data. The fleet runtime owns the probe loop and
//! calls the mutation methods; callers that route work use [`PeerDirectory::route`].

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::error::AppError;
use crate::fleet::address::{AddressPreference, AddressSource, MachineAddress};
use crate::fleet::advertisement::{Capabilities, MachineHeader, ProbedMachine};
use crate::fleet::identity::{IdentityStatus, IdentityVerdict};
use crate::fleet::protocol::{Compatibility, ProtocolRange};
use crate::machine::{BootId, LocalIdentity, MachineId, MachineName, write_synced};

/// Default time a machine stays reachable after its last successful probe.
pub const DEFAULT_REACHABLE_TTL: Duration = Duration::seconds(90);

/// Automatic discovery provider that reported an address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SightingProvider {
    /// DNS-SD over mDNS on the local network.
    Mdns,
    /// Tailscale peer list.
    Tailscale,
}

/// One address reported by an automatic provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sighting {
    /// Candidate base URL.
    pub address: MachineAddress,
    /// Provider that reported it.
    pub provider: SightingProvider,
    /// When the report stops counting as a live discovery.
    pub expires_at: DateTime<Utc>,
}

/// What a probe of one address last established.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum AddressBinding {
    /// No probe has confirmed an identity for this address.
    #[default]
    Unverified,
    /// A probe confirmed this machine.
    Machine {
        /// Machine the address answered for.
        machine: MachineId,
        /// Time of the confirming probe.
        verified_at: DateTime<Utc>,
    },
    /// The address reaches this daemon. Never routed to.
    Local {
        /// Time of the confirming probe.
        verified_at: DateTime<Utc>,
    },
}

/// Last probe failure of one address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeFailure {
    /// When the probe failed.
    pub at: DateTime<Utc>,
    /// Failure text.
    pub message: String,
}

#[derive(Debug, Clone, Default)]
struct AddressRecord {
    configured: bool,
    explicit: bool,
    mdns_until: Option<DateTime<Utc>>,
    tailscale_until: Option<DateTime<Utc>>,
    binding: AddressBinding,
    last_failure: Option<ProbeFailure>,
}

impl AddressRecord {
    fn bound_to(&self, machine: MachineId) -> bool {
        matches!(self.binding, AddressBinding::Machine { machine: bound, .. } if bound == machine)
    }

    fn verified_at(&self) -> Option<DateTime<Utc>> {
        match self.binding {
            AddressBinding::Machine { verified_at, .. } | AddressBinding::Local { verified_at } => {
                Some(verified_at)
            }
            AddressBinding::Unverified => None,
        }
    }

    /// Best current source, or `None` when nothing justifies keeping it.
    fn source(&self, now: DateTime<Utc>) -> Option<AddressSource> {
        if self.configured {
            return Some(AddressSource::Configured);
        }
        if self.explicit {
            return Some(AddressSource::Explicit);
        }
        if self.mdns_until.is_some_and(|until| until > now) {
            return Some(AddressSource::Lan);
        }
        if self.tailscale_until.is_some_and(|until| until > now) {
            return Some(AddressSource::Tailscale);
        }
        matches!(self.binding, AddressBinding::Machine { .. }).then_some(AddressSource::Cached)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MachineRecord {
    header: MachineHeader,
    capabilities: Option<Capabilities>,
    advertised: Vec<MachineAddress>,
    last_seen: DateTime<Utc>,
    identity: IdentityStatus,
}

/// Local machine identity and name, as the directory needs them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalMachine {
    /// Stable and boot UUIDs.
    pub identity: LocalIdentity,
    /// Display name.
    pub name: MachineName,
    /// Accepted protocol range.
    pub protocol: ProtocolRange,
}

/// Address in routing order for one machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankedAddress {
    /// Base URL.
    pub address: MachineAddress,
    /// Best current source.
    pub source: AddressSource,
    /// Last probe that confirmed the machine at this address.
    pub verified_at: Option<DateTime<Utc>>,
    /// Last probe failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<ProbeFailure>,
}

/// Whether a peer answered recently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Reachability {
    /// A probe succeeded within the reachable window.
    Reachable {
        /// Last successful probe.
        last_seen: DateTime<Utc>,
    },
    /// Known, last-known metadata only.
    Offline {
        /// Last successful probe.
        last_seen: DateTime<Utc>,
    },
}

/// Read-only view of one known peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerView {
    /// Stable machine UUID.
    pub machine: MachineId,
    /// Name from the last probe.
    pub name: MachineName,
    /// Boot UUID from the last probe.
    pub boot: BootId,
    /// Homebased version from the last probe.
    pub version: String,
    /// Protocol range from the last probe.
    pub protocol: ProtocolRange,
    /// Compatibility with this daemon.
    pub compatibility: Compatibility,
    /// Capabilities, when the protocol is compatible.
    pub capabilities: Option<Capabilities>,
    /// Addresses the peer advertised for itself.
    pub advertised: Vec<MachineAddress>,
    /// Reachability now.
    pub reachability: Reachability,
    /// Identity status.
    pub identity: IdentityStatus,
    /// Whether another machine uses the same name.
    pub name_conflict: bool,
    /// Addresses in routing order.
    pub addresses: Vec<RankedAddress>,
}

/// Address that has not bound to a machine yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnresolvedAddress {
    /// Base URL.
    pub address: MachineAddress,
    /// Best current source.
    pub source: AddressSource,
    /// Last probe failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<ProbeFailure>,
}

/// Ordered addresses for one routable machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutePlan {
    /// Destination machine UUID. Every request carries it.
    pub machine: MachineId,
    /// Addresses to try, most preferred first. Each needs a fresh identity
    /// probe before use.
    pub addresses: Vec<RankedAddress>,
}

/// Why the directory cannot route to a machine.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    /// The UUID is the local machine; use the in-process path.
    #[error("machine {0} is the local machine")]
    Local(MachineId),
    /// No record for this UUID.
    #[error("machine {0} is not known")]
    Unknown(MachineId),
    /// Distinct live daemons claim this UUID.
    #[error("machine {0} has a duplicate live identity")]
    DuplicateIdentity(MachineId),
    /// No shared protocol version.
    #[error("machine {machine} speaks cluster protocol {remote}, this daemon accepts {local}")]
    Incompatible {
        /// Destination.
        machine: MachineId,
        /// Local range.
        local: ProtocolRange,
        /// Remote range.
        remote: ProtocolRange,
    },
    /// Known, but every address was invalidated.
    #[error("machine {0} has no known address")]
    NoAddress(MachineId),
}

impl From<RouteError> for AppError {
    fn from(err: RouteError) -> Self {
        match err {
            RouteError::Local(machine) => AppError::Internal {
                message: format!("route requested for local machine {machine}"),
            },
            RouteError::Unknown(machine) => AppError::MachineNotFound {
                machine: machine.to_string(),
            },
            RouteError::DuplicateIdentity(machine) => {
                AppError::DuplicateMachineIdentity { machine }
            }
            RouteError::Incompatible {
                machine,
                local,
                remote,
            } => AppError::ClusterProtocolIncompatible {
                machine,
                local,
                remote,
            },
            RouteError::NoAddress(machine) => AppError::MachineUnavailable {
                machine,
                message: "no known address".into(),
            },
        }
    }
}

/// Result of resolving a name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameTarget {
    /// The name is this machine.
    Local,
    /// One peer has the name.
    Peer(MachineId),
}

/// Why a name did not resolve to one machine.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    /// No machine has the name.
    #[error("no machine is named {0}")]
    Unknown(MachineName),
    /// More than one machine has the name. A configuration error.
    #[error("more than one machine is named {name}")]
    Duplicate {
        /// Conflicting name.
        name: MachineName,
        /// Machines that claim it. Includes the local machine when it does.
        machines: Vec<MachineId>,
    },
}

impl From<NameError> for AppError {
    fn from(err: NameError) -> Self {
        match err {
            NameError::Unknown(name) => AppError::MachineNotFound {
                machine: name.to_string(),
            },
            NameError::Duplicate { name, machines } => {
                AppError::DuplicateMachineName { name, machines }
            }
        }
    }
}

/// Peer directory keyed by stable machine UUID.
#[derive(Debug, Clone)]
pub struct PeerDirectory {
    local: LocalMachine,
    reachable_ttl: Duration,
    addresses: BTreeMap<MachineAddress, AddressRecord>,
    machines: BTreeMap<MachineId, MachineRecord>,
}

impl PeerDirectory {
    /// Empty directory.
    #[must_use]
    pub fn new(local: LocalMachine) -> Self {
        Self {
            local,
            reachable_ttl: DEFAULT_REACHABLE_TTL,
            addresses: BTreeMap::new(),
            machines: BTreeMap::new(),
        }
    }

    /// Override the reachable window. Tests and slow networks use this.
    #[must_use]
    pub fn with_reachable_ttl(mut self, ttl: Duration) -> Self {
        self.reachable_ttl = ttl;
        self
    }

    /// Local machine.
    #[must_use]
    pub fn local(&self) -> &LocalMachine {
        &self.local
    }

    /// Replace the configured address set. `config.toml` is the source of
    /// truth, so addresses absent from the new set lose the configured source.
    pub fn set_configured(&mut self, addresses: &[MachineAddress]) {
        for record in self.addresses.values_mut() {
            record.configured = false;
        }
        for address in addresses {
            self.addresses
                .entry(address.clone())
                .or_default()
                .configured = true;
        }
    }

    /// Add an explicit address. Returns `false` when it was already explicit.
    pub fn add_explicit(&mut self, address: MachineAddress) -> bool {
        let record = self.addresses.entry(address).or_default();
        !std::mem::replace(&mut record.explicit, true)
    }

    /// Remove the explicit source from one address. Returns `false` when the
    /// address was not explicit. Configured addresses stay.
    pub fn remove_explicit(&mut self, address: &MachineAddress) -> bool {
        self.addresses
            .get_mut(address)
            .is_some_and(|record| std::mem::replace(&mut record.explicit, false))
    }

    /// Forget a machine: drop its record, its explicit addresses, and its
    /// cached bindings. Configured addresses stay and bind again on the next
    /// probe. Returns `false` when the machine was unknown.
    pub fn forget_machine(&mut self, machine: MachineId) -> bool {
        let known = self.machines.remove(&machine).is_some();
        for record in self.addresses.values_mut() {
            if record.bound_to(machine) {
                record.binding = AddressBinding::Unverified;
                record.explicit = false;
            }
        }
        known
    }

    /// Record an automatic discovery.
    pub fn record_sighting(&mut self, sighting: Sighting) {
        let record = self.addresses.entry(sighting.address).or_default();
        let slot = match sighting.provider {
            SightingProvider::Mdns => &mut record.mdns_until,
            SightingProvider::Tailscale => &mut record.tailscale_until,
        };
        *slot = Some(slot.map_or(sighting.expires_at, |old| old.max(sighting.expires_at)));
    }

    /// End an automatic discovery early, such as an mDNS removal.
    pub fn withdraw_sighting(&mut self, address: &MachineAddress, provider: SightingProvider) {
        let Some(record) = self.addresses.get_mut(address) else {
            return;
        };
        match provider {
            SightingProvider::Mdns => record.mdns_until = None,
            SightingProvider::Tailscale => record.tailscale_until = None,
        }
    }

    /// Record a successful probe. Binds the address to the machine that
    /// answered, moving it away from any machine it answered for before.
    ///
    /// This does not change identity status; the runtime decides that from
    /// two probe rounds with [`Self::apply_verdict`].
    pub fn record_probe(
        &mut self,
        address: &MachineAddress,
        probed: &ProbedMachine,
        now: DateTime<Utc>,
    ) {
        let header = probed.header();
        let record = self.addresses.entry(address.clone()).or_default();
        record.last_failure = None;
        if self.local.identity.is_self(header.machine, header.boot) {
            record.binding = AddressBinding::Local { verified_at: now };
            return;
        }
        record.binding = AddressBinding::Machine {
            machine: header.machine,
            verified_at: now,
        };
        if header.machine == self.local.identity.machine {
            // a second daemon claims the local UUID; keep no peer record for
            // it, the runtime reports the conflict from its probe rounds
            return;
        }
        let (capabilities, advertised) = match probed {
            ProbedMachine::Compatible { advertisement, .. } => (
                Some(advertisement.capabilities.clone()),
                advertisement.addresses.clone(),
            ),
            ProbedMachine::Incompatible { .. } => (None, Vec::new()),
        };
        let identity = self
            .machines
            .get(&header.machine)
            .map_or(IdentityStatus::Consistent, |old| old.identity.clone());
        self.machines.insert(
            header.machine,
            MachineRecord {
                header,
                capabilities,
                advertised,
                last_seen: now,
                identity,
            },
        );
    }

    /// Record a failed probe. The binding stays as a cached hint.
    pub fn record_probe_failure(
        &mut self,
        address: &MachineAddress,
        message: String,
        now: DateTime<Utc>,
    ) {
        let record = self.addresses.entry(address.clone()).or_default();
        record.last_failure = Some(ProbeFailure { at: now, message });
    }

    /// Apply the identity decision for one machine.
    pub fn apply_verdict(
        &mut self,
        machine: MachineId,
        verdict: &IdentityVerdict,
        now: DateTime<Utc>,
    ) {
        let Some(record) = self.machines.get_mut(&machine) else {
            return;
        };
        match verdict {
            IdentityVerdict::Consistent { .. } => record.identity = IdentityStatus::Consistent,
            IdentityVerdict::Duplicate { boots } => {
                record.identity = IdentityStatus::DuplicateMachineIdentity {
                    boots: boots.clone(),
                    detected_at: now,
                };
            }
            IdentityVerdict::Inconclusive => {}
        }
    }

    /// Machines currently marked as duplicates.
    #[must_use]
    pub fn duplicates(&self) -> BTreeSet<MachineId> {
        self.machines
            .iter()
            .filter(|(_, record)| {
                matches!(
                    record.identity,
                    IdentityStatus::DuplicateMachineIdentity { .. }
                )
            })
            .map(|(machine, _)| *machine)
            .collect()
    }

    /// Addresses the next probe round should check: every address with a
    /// current source, including cached addresses of offline machines.
    #[must_use]
    pub fn probe_candidates(&self, now: DateTime<Utc>) -> Vec<MachineAddress> {
        self.addresses
            .iter()
            .filter(|(_, record)| record.source(now).is_some())
            .map(|(address, _)| address.clone())
            .collect()
    }

    /// Drop addresses that no source justifies any more. Machines stay as
    /// offline, last-known records.
    pub fn prune(&mut self, now: DateTime<Utc>) {
        self.addresses
            .retain(|_, record| record.source(now).is_some());
    }

    /// Ordered addresses for a machine, or why it cannot be routed.
    pub fn route(&self, machine: MachineId, now: DateTime<Utc>) -> Result<RoutePlan, RouteError> {
        if machine == self.local.identity.machine {
            return Err(RouteError::Local(machine));
        }
        let record = self
            .machines
            .get(&machine)
            .ok_or(RouteError::Unknown(machine))?;
        if matches!(
            record.identity,
            IdentityStatus::DuplicateMachineIdentity { .. }
        ) {
            return Err(RouteError::DuplicateIdentity(machine));
        }
        if let Compatibility::Incompatible { local, remote } =
            self.local.protocol.negotiate(&record.header.protocol)
        {
            return Err(RouteError::Incompatible {
                machine,
                local,
                remote,
            });
        }
        let addresses = self.ranked_addresses(machine, now);
        if addresses.is_empty() {
            return Err(RouteError::NoAddress(machine));
        }
        Ok(RoutePlan { machine, addresses })
    }

    /// Resolve a machine name. Among machines with the name, live machines
    /// win over offline records, because a stale cached name is not evidence
    /// of a conflict.
    pub fn resolve_name(
        &self,
        name: &MachineName,
        now: DateTime<Utc>,
    ) -> Result<NameTarget, NameError> {
        let matches: Vec<(MachineId, bool)> = self
            .machines
            .iter()
            .filter(|(_, record)| &record.header.name == name)
            .map(|(machine, record)| (*machine, self.is_reachable(record, now)))
            .collect();
        let live: Vec<MachineId> = matches
            .iter()
            .filter(|(_, live)| *live)
            .map(|(machine, _)| *machine)
            .collect();
        let local_matches = &self.local.name == name;
        if local_matches && live.is_empty() {
            return Ok(NameTarget::Local);
        }
        if local_matches {
            let mut machines = vec![self.local.identity.machine];
            machines.extend(live);
            return Err(NameError::Duplicate {
                name: name.clone(),
                machines,
            });
        }
        let candidates: Vec<MachineId> = if live.is_empty() {
            matches.iter().map(|(machine, _)| *machine).collect()
        } else {
            live
        };
        match candidates.as_slice() {
            [] => Err(NameError::Unknown(name.clone())),
            [one] => Ok(NameTarget::Peer(*one)),
            _ => Err(NameError::Duplicate {
                name: name.clone(),
                machines: candidates,
            }),
        }
    }

    /// All known peers, sorted by name then UUID.
    #[must_use]
    pub fn peers(&self, now: DateTime<Utc>) -> Vec<PeerView> {
        let mut views: Vec<PeerView> = self
            .machines
            .iter()
            .map(|(machine, record)| self.view(*machine, record, now))
            .collect();
        views.sort_by(|a, b| (&a.name, a.machine).cmp(&(&b.name, b.machine)));
        views
    }

    /// One peer.
    #[must_use]
    pub fn peer(&self, machine: MachineId, now: DateTime<Utc>) -> Option<PeerView> {
        self.machines
            .get(&machine)
            .map(|record| self.view(machine, record, now))
    }

    /// Addresses that have not bound to any machine.
    ///
    /// Tailscale reports every tailnet device, and most of them never run
    /// homebased, so an address known only from Tailscale is still probed but
    /// not reported.
    #[must_use]
    pub fn unresolved(&self, now: DateTime<Utc>) -> Vec<UnresolvedAddress> {
        self.addresses
            .iter()
            .filter(|(_, record)| record.binding == AddressBinding::Unverified)
            .filter_map(|(address, record)| {
                let source = record.source(now)?;
                (source != AddressSource::Tailscale).then(|| UnresolvedAddress {
                    address: address.clone(),
                    source,
                    last_failure: record.last_failure.clone(),
                })
            })
            .collect()
    }

    /// Binding of one address.
    #[must_use]
    pub fn binding(&self, address: &MachineAddress) -> Option<AddressBinding> {
        self.addresses
            .get(address)
            .map(|record| record.binding.clone())
    }

    fn view(&self, machine: MachineId, record: &MachineRecord, now: DateTime<Utc>) -> PeerView {
        let reachability = if self.is_reachable(record, now) {
            Reachability::Reachable {
                last_seen: record.last_seen,
            }
        } else {
            Reachability::Offline {
                last_seen: record.last_seen,
            }
        };
        let name_conflict = matches!(
            self.resolve_name(&record.header.name, now),
            Err(NameError::Duplicate { ref machines, .. }) if machines.contains(&machine)
        );
        PeerView {
            machine,
            name: record.header.name.clone(),
            boot: record.header.boot,
            version: record.header.version.clone(),
            protocol: record.header.protocol,
            compatibility: self.local.protocol.negotiate(&record.header.protocol),
            capabilities: record.capabilities.clone(),
            advertised: record.advertised.clone(),
            reachability,
            identity: record.identity.clone(),
            name_conflict,
            addresses: self.ranked_addresses(machine, now),
        }
    }

    fn is_reachable(&self, record: &MachineRecord, now: DateTime<Utc>) -> bool {
        now - record.last_seen <= self.reachable_ttl
    }

    fn ranked_addresses(&self, machine: MachineId, now: DateTime<Utc>) -> Vec<RankedAddress> {
        let mut ranked: Vec<RankedAddress> = self
            .addresses
            .iter()
            .filter(|(_, record)| record.bound_to(machine))
            .filter_map(|(address, record)| {
                Some(RankedAddress {
                    address: address.clone(),
                    source: record.source(now)?,
                    verified_at: record.verified_at(),
                    last_failure: record.last_failure.clone(),
                })
            })
            .collect();
        // most preferred source first, then the most recent confirmation
        ranked.sort_by(|a, b| {
            AddressPreference::from(a.source)
                .cmp(&AddressPreference::from(b.source))
                .then_with(|| b.verified_at.cmp(&a.verified_at))
                .then_with(|| a.address.cmp(&b.address))
        });
        ranked
    }
}

/// Current on-disk format version of `fleet-peers.json`.
const PERSISTED_VERSION: u32 = 1;

/// On-disk shape. Configured addresses and live sightings are not stored:
/// the config file owns the former, and the latter expire anyway.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Persisted {
    version: u32,
    addresses: Vec<PersistedAddress>,
    machines: Vec<PersistedMachine>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedAddress {
    address: MachineAddress,
    explicit: bool,
    binding: AddressBinding,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedMachine {
    machine: MachineId,
    record: MachineRecord,
}

impl PeerDirectory {
    /// Load the durable part from `path`. A missing file is an empty
    /// directory. An unreadable or corrupt file is moved aside with a warning
    /// so discovery can still start; its name is logged for recovery.
    pub fn load(local: LocalMachine, path: &Path) -> Result<Self, AppError> {
        let mut directory = Self::new(local);
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(directory),
            Err(err) => return Err(err.into()),
        };
        let persisted: Persisted = match serde_json::from_slice(&bytes) {
            Ok(persisted) => persisted,
            Err(err) => {
                let aside = path.with_extension(format!(
                    "json.corrupt-{}",
                    Utc::now().format("%Y%m%dT%H%M%SZ")
                ));
                warn!(
                    path = %path.display(),
                    aside = %aside.display(),
                    "fleet peer directory is unreadable, starting empty: {err}"
                );
                fs::rename(path, &aside)?;
                return Ok(directory);
            }
        };
        if persisted.version != PERSISTED_VERSION {
            return Err(AppError::Internal {
                message: format!(
                    "{} has format version {}, this build reads {PERSISTED_VERSION}",
                    path.display(),
                    persisted.version
                ),
            });
        }
        directory.restore(persisted);
        Ok(directory)
    }

    fn restore(&mut self, persisted: Persisted) {
        for entry in persisted.machines {
            if entry.machine == self.local.identity.machine {
                continue;
            }
            self.machines.insert(entry.machine, entry.record);
        }
        for entry in persisted.addresses {
            let record = self.addresses.entry(entry.address).or_default();
            record.explicit = entry.explicit;
            match entry.binding {
                AddressBinding::Machine { machine, .. } if self.machines.contains_key(&machine) => {
                    record.binding = entry.binding;
                }
                // a local binding belonged to an earlier boot of this daemon;
                // the next probe re-establishes it
                AddressBinding::Machine { .. }
                | AddressBinding::Local { .. }
                | AddressBinding::Unverified => {}
            }
        }
        self.addresses.retain(|_, record| {
            record.explicit || matches!(record.binding, AddressBinding::Machine { .. })
        });
    }

    /// Write the durable part to `path` atomically.
    pub fn save(&self, path: &Path) -> Result<(), AppError> {
        let persisted = Persisted {
            version: PERSISTED_VERSION,
            addresses: self
                .addresses
                .iter()
                .filter(|(_, record)| {
                    record.explicit || matches!(record.binding, AddressBinding::Machine { .. })
                })
                .map(|(address, record)| PersistedAddress {
                    address: address.clone(),
                    explicit: record.explicit,
                    binding: match record.binding {
                        AddressBinding::Machine { .. } => record.binding.clone(),
                        AddressBinding::Local { .. } | AddressBinding::Unverified => {
                            AddressBinding::Unverified
                        }
                    },
                })
                .collect(),
            machines: self
                .machines
                .iter()
                .map(|(machine, record)| PersistedMachine {
                    machine: *machine,
                    record: record.clone(),
                })
                .collect(),
        };
        let bytes = serde_json::to_vec_pretty(&persisted)?;
        let tmp = path.with_extension(format!("json.{}.tmp", uuid::Uuid::now_v7()));
        write_synced(&tmp, &bytes)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::advertisement::MachineAdvertisement;
    use crate::fleet::protocol::{
        CLUSTER_PROTOCOL_VERSION, ClusterProtocolVersion, SUPPORTED_PROTOCOLS,
    };

    fn local() -> LocalMachine {
        LocalMachine {
            identity: LocalIdentity {
                machine: MachineId::new(),
                boot: BootId::new(),
            },
            name: MachineName::parse("main").unwrap(),
            protocol: SUPPORTED_PROTOCOLS,
        }
    }

    fn addr(raw: &str) -> MachineAddress {
        raw.parse().unwrap()
    }

    fn probed(machine: MachineId, boot: BootId, name: &str) -> ProbedMachine {
        ProbedMachine::Compatible {
            advertisement: MachineAdvertisement {
                api_version: 1,
                machine,
                boot,
                name: MachineName::parse(name).unwrap(),
                version: "0.3.0".into(),
                protocol: SUPPORTED_PROTOCOLS,
                capabilities: Capabilities {
                    os: "linux".into(),
                    arch: "x86_64".into(),
                    agents: Vec::new(),
                },
                addresses: Vec::new(),
            },
            version: CLUSTER_PROTOCOL_VERSION,
        }
    }

    fn sighting(raw: &str, provider: SightingProvider, expires_at: DateTime<Utc>) -> Sighting {
        Sighting {
            address: addr(raw),
            provider,
            expires_at,
        }
    }

    #[test]
    fn merges_sources_by_machine_and_prefers_operator_then_lan_then_tailscale() {
        let now = Utc::now();
        let mut dir = PeerDirectory::new(local());
        let machine = MachineId::new();
        let boot = BootId::new();
        dir.set_configured(&[addr("http://code:7677")]);
        let later = now + Duration::seconds(60);
        dir.record_sighting(sighting(
            "http://100.64.0.2:7677",
            SightingProvider::Tailscale,
            later,
        ));
        dir.record_sighting(sighting(
            "http://10.0.0.2:7677",
            SightingProvider::Mdns,
            later,
        ));
        for raw in [
            "http://100.64.0.2:7677",
            "http://10.0.0.2:7677",
            "http://code:7677",
        ] {
            dir.record_probe(&addr(raw), &probed(machine, boot, "code"), now);
        }
        let plan = dir.route(machine, now).unwrap();
        let order: Vec<(String, AddressSource)> = plan
            .addresses
            .iter()
            .map(|ranked| (ranked.address.to_string(), ranked.source))
            .collect();
        assert_eq!(
            order,
            vec![
                ("http://code:7677".into(), AddressSource::Configured),
                ("http://10.0.0.2:7677".into(), AddressSource::Lan),
                ("http://100.64.0.2:7677".into(), AddressSource::Tailscale),
            ]
        );
        assert_eq!(dir.peers(now).len(), 1);
    }

    #[test]
    fn expired_discoveries_become_cached_and_machine_stays_offline() {
        let now = Utc::now();
        let mut dir = PeerDirectory::new(local());
        let machine = MachineId::new();
        dir.record_sighting(sighting(
            "http://10.0.0.2:7677",
            SightingProvider::Mdns,
            now + Duration::seconds(10),
        ));
        dir.record_probe(
            &addr("http://10.0.0.2:7677"),
            &probed(machine, BootId::new(), "code"),
            now,
        );
        let later = now + Duration::seconds(600);
        dir.prune(later);
        let peer = dir.peer(machine, later).unwrap();
        assert!(matches!(peer.reachability, Reachability::Offline { .. }));
        assert_eq!(peer.addresses.len(), 1);
        assert_eq!(peer.addresses[0].source, AddressSource::Cached);
    }

    #[test]
    fn unverified_automatic_address_is_pruned_after_expiry() {
        let now = Utc::now();
        let mut dir = PeerDirectory::new(local());
        dir.record_sighting(sighting(
            "http://10.0.0.7:7677",
            SightingProvider::Mdns,
            now + Duration::seconds(10),
        ));
        assert_eq!(dir.probe_candidates(now).len(), 1);
        dir.prune(now + Duration::seconds(11));
        assert!(dir.probe_candidates(now + Duration::seconds(11)).is_empty());
    }

    #[test]
    fn address_that_answers_for_another_machine_moves() {
        let now = Utc::now();
        let mut dir = PeerDirectory::new(local());
        let (old, new) = (MachineId::new(), MachineId::new());
        let address = addr("http://10.0.0.2:7677");
        dir.add_explicit(address.clone());
        dir.record_probe(&address, &probed(old, BootId::new(), "code"), now);
        dir.record_probe(&address, &probed(new, BootId::new(), "other"), now);
        assert_eq!(dir.route(old, now), Err(RouteError::NoAddress(old)));
        assert_eq!(dir.route(new, now).unwrap().addresses[0].address, address);
    }

    #[test]
    fn own_address_is_local_and_never_routed() {
        let now = Utc::now();
        let local = local();
        let mut dir = PeerDirectory::new(local.clone());
        let address = addr("http://10.0.0.1:7677");
        dir.set_configured(std::slice::from_ref(&address));
        dir.record_probe(
            &address,
            &probed(local.identity.machine, local.identity.boot, "main"),
            now,
        );
        assert!(matches!(
            dir.binding(&address),
            Some(AddressBinding::Local { .. })
        ));
        assert!(dir.peers(now).is_empty());
        assert_eq!(
            dir.route(local.identity.machine, now),
            Err(RouteError::Local(local.identity.machine))
        );
    }

    #[test]
    fn duplicate_identity_blocks_routing_until_cleared() {
        let now = Utc::now();
        let mut dir = PeerDirectory::new(local());
        let machine = MachineId::new();
        let (a, b) = (BootId::new(), BootId::new());
        dir.add_explicit(addr("http://10.0.0.2:7677"));
        dir.record_probe(
            &addr("http://10.0.0.2:7677"),
            &probed(machine, a, "code"),
            now,
        );
        dir.apply_verdict(
            machine,
            &IdentityVerdict::Duplicate {
                boots: BTreeSet::from([a, b]),
            },
            now,
        );
        assert_eq!(
            dir.route(machine, now),
            Err(RouteError::DuplicateIdentity(machine))
        );
        // a later probe alone does not clear the conflict
        dir.record_probe(
            &addr("http://10.0.0.2:7677"),
            &probed(machine, a, "code"),
            now,
        );
        assert_eq!(
            dir.route(machine, now),
            Err(RouteError::DuplicateIdentity(machine))
        );
        dir.apply_verdict(
            machine,
            &IdentityVerdict::Consistent {
                boot: a,
                name: MachineName::parse("code").unwrap(),
            },
            now,
        );
        assert!(dir.route(machine, now).is_ok());
    }

    #[test]
    fn incompatible_machine_is_visible_but_not_routable() {
        let now = Utc::now();
        let mut dir = PeerDirectory::new(local());
        let machine = MachineId::new();
        let remote =
            ProtocolRange::new(ClusterProtocolVersion(9), ClusterProtocolVersion(9)).unwrap();
        let header = MachineHeader {
            machine,
            boot: BootId::new(),
            name: MachineName::parse("future").unwrap(),
            version: "9.0.0".into(),
            protocol: remote,
        };
        dir.add_explicit(addr("http://10.0.0.9:7677"));
        dir.record_probe(
            &addr("http://10.0.0.9:7677"),
            &ProbedMachine::Incompatible {
                header,
                local: SUPPORTED_PROTOCOLS,
            },
            now,
        );
        assert_eq!(dir.peers(now).len(), 1);
        assert!(matches!(
            dir.route(machine, now),
            Err(RouteError::Incompatible { .. })
        ));
    }

    #[test]
    fn names_resolve_live_first_and_detect_duplicates() {
        let now = Utc::now();
        let mut dir = PeerDirectory::new(local());
        let (stale, live, other) = (MachineId::new(), MachineId::new(), MachineId::new());
        let name = MachineName::parse("code").unwrap();
        dir.record_probe(
            &addr("http://10.0.0.2:7677"),
            &probed(stale, BootId::new(), "code"),
            now - Duration::seconds(3600),
        );
        dir.record_probe(
            &addr("http://10.0.0.3:7677"),
            &probed(live, BootId::new(), "code"),
            now,
        );
        // a reinstalled machine reuses a name; the stale record is no conflict
        assert_eq!(dir.resolve_name(&name, now), Ok(NameTarget::Peer(live)));
        dir.record_probe(
            &addr("http://10.0.0.4:7677"),
            &probed(other, BootId::new(), "code"),
            now,
        );
        assert!(matches!(
            dir.resolve_name(&name, now),
            Err(NameError::Duplicate { .. })
        ));
        assert!(dir.peer(live, now).unwrap().name_conflict);
        assert!(!dir.peer(stale, now).unwrap().name_conflict);
        assert_eq!(
            dir.resolve_name(&MachineName::parse("main").unwrap(), now),
            Ok(NameTarget::Local)
        );
    }

    #[test]
    fn rename_updates_the_same_machine() {
        let now = Utc::now();
        let mut dir = PeerDirectory::new(local());
        let machine = MachineId::new();
        dir.record_probe(
            &addr("http://10.0.0.2:7677"),
            &probed(machine, BootId::new(), "code"),
            now,
        );
        dir.record_probe(
            &addr("http://10.0.0.2:7677"),
            &probed(machine, BootId::new(), "builder"),
            now,
        );
        let peers = dir.peers(now);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].name.as_str(), "builder");
        assert_eq!(peers[0].identity, IdentityStatus::Consistent);
    }

    #[test]
    fn persistence_keeps_explicit_and_cached_but_not_configured_or_sightings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fleet-peers.json");
        let now = Utc::now();
        let local = local();
        let mut peers = PeerDirectory::new(local.clone());
        let machine = MachineId::new();
        peers.set_configured(&[addr("http://configured:7677")]);
        peers.add_explicit(addr("http://explicit:7677"));
        peers.record_sighting(sighting(
            "http://10.0.0.5:7677",
            SightingProvider::Mdns,
            now + Duration::seconds(60),
        ));
        peers.record_probe(
            &addr("http://10.0.0.5:7677"),
            &probed(machine, BootId::new(), "code"),
            now,
        );
        peers.save(&path).unwrap();

        let loaded = PeerDirectory::load(local, &path).unwrap();
        let candidates: Vec<String> = loaded
            .probe_candidates(now)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            candidates,
            vec!["http://10.0.0.5:7677", "http://explicit:7677"]
        );
        let peer = loaded.peer(machine, now).unwrap();
        assert_eq!(peer.addresses[0].source, AddressSource::Cached);
    }

    #[test]
    fn corrupt_file_is_moved_aside() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fleet-peers.json");
        fs::write(&path, "{").unwrap();
        let loaded = PeerDirectory::load(local(), &path).unwrap();
        assert!(loaded.probe_candidates(Utc::now()).is_empty());
        assert!(!path.exists());
        let aside = fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(aside, 1);
    }

    #[test]
    fn tailscale_only_candidates_are_probed_but_not_reported_unresolved() {
        let now = Utc::now();
        let later = now + Duration::seconds(60);
        let mut dir = PeerDirectory::new(local());
        dir.set_configured(&[addr("http://code:7677")]);
        dir.record_sighting(sighting(
            "http://100.64.0.3:7677",
            SightingProvider::Tailscale,
            later,
        ));
        dir.record_sighting(sighting(
            "http://192.168.1.30:7677",
            SightingProvider::Mdns,
            later,
        ));
        for raw in [
            "http://code:7677",
            "http://100.64.0.3:7677",
            "http://192.168.1.30:7677",
        ] {
            dir.record_probe_failure(&addr(raw), "connection refused".into(), now);
        }

        let unresolved: Vec<String> = dir
            .unresolved(now)
            .iter()
            .map(|entry| entry.address.to_string())
            .collect();
        assert_eq!(
            unresolved,
            vec!["http://192.168.1.30:7677", "http://code:7677"]
        );
        assert!(
            dir.probe_candidates(now)
                .contains(&addr("http://100.64.0.3:7677"))
        );
    }

    #[test]
    fn forget_keeps_configured_addresses() {
        let now = Utc::now();
        let mut dir = PeerDirectory::new(local());
        let machine = MachineId::new();
        dir.set_configured(&[addr("http://code:7677")]);
        dir.add_explicit(addr("http://10.0.0.2:7677"));
        dir.record_probe(
            &addr("http://code:7677"),
            &probed(machine, BootId::new(), "code"),
            now,
        );
        dir.record_probe(
            &addr("http://10.0.0.2:7677"),
            &probed(machine, BootId::new(), "code"),
            now,
        );
        assert!(dir.forget_machine(machine));
        dir.prune(now);
        let unresolved: Vec<String> = dir
            .unresolved(now)
            .iter()
            .map(|entry| entry.address.to_string())
            .collect();
        assert_eq!(unresolved, vec!["http://code:7677"]);
    }
}
