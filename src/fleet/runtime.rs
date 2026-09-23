//! Fleet runtime: discovery providers, probe rounds, and destination routing.
//!
//! [`FleetRuntime::start`] returns at once. Providers and the probe loop run
//! as background tasks, so discovery never delays the local socket. Follow-on
//! cluster code routes work through [`FleetHandle::connect`], which resolves a
//! machine UUID through the directory and confirms identity before use.

use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::{Mutex, Notify, RwLock, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, error, info, warn};

use crate::config::{Discovery, FleetSettings};
use crate::error::AppError;
use crate::fleet::address::MachineAddress;
use crate::fleet::advertisement::{Capabilities, MachineAdvertisement, ProbedMachine};
use crate::fleet::directory::{
    LocalMachine, NameTarget, PeerDirectory, PeerView, UnresolvedAddress,
};
use crate::fleet::discovery::mdns::{MdnsAnnouncement, MdnsProvider, MdnsTimings};
use crate::fleet::discovery::tailscale::{TailscaleProvider, TailscaleTimings};
use crate::fleet::discovery::{DiscoveryEvent, is_routable_peer_ip};
use crate::fleet::http::ClusterClient;
use crate::fleet::identity::{self, IdentityVerdict, ProbeObservation};
use crate::fleet::probe::{DestinationError, ProbeError, VerifiedDestination, check_probed, probe};
use crate::machine::{BootId, MachineId, MachineName};

/// Probe loop timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeTimings {
    /// Time between full probe rounds.
    pub round_interval: Duration,
    /// Delay before the confirming round for suspect identities. Long enough
    /// for a restarting daemon to come back on every address.
    pub recheck_delay: Duration,
    /// Concurrent probes in one round.
    pub probe_concurrency: usize,
    /// mDNS timing.
    pub mdns: MdnsTimings,
    /// Tailscale timing.
    pub tailscale: TailscaleTimings,
}

impl Default for RuntimeTimings {
    fn default() -> Self {
        Self {
            round_interval: Duration::from_secs(30),
            recheck_delay: Duration::from_secs(2),
            probe_concurrency: 8,
            mdns: MdnsTimings::default(),
            tailscale: TailscaleTimings::default(),
        }
    }
}

/// State of the local machine UUID in the fleet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LocalIdentityStatus {
    /// No other live daemon claims the local UUID.
    Consistent,
    /// Another live daemon claims the local UUID, such as a copied state
    /// directory. Peers stop routing to this UUID.
    DuplicateMachineIdentity {
        /// Other boot UUIDs.
        boots: BTreeSet<BootId>,
        /// When the confirming round saw them.
        detected_at: DateTime<Utc>,
    },
}

/// Inputs to start the runtime.
#[derive(Debug, Clone)]
pub struct FleetStart {
    /// Local identity, name, and protocol range.
    pub local: LocalMachine,
    /// Enabled fleet settings.
    pub settings: FleetSettings,
    /// Address the TCP listener bound, or `None` when it is off. Without it
    /// the machine can discover peers but peers cannot reach it.
    pub listener: Option<SocketAddr>,
    /// `fleet-peers.json` path.
    pub peers_path: PathBuf,
    /// Timing.
    pub timings: RuntimeTimings,
}

struct Shared {
    local: LocalMachine,
    advertisement: MachineAdvertisement,
    directory: RwLock<PeerDirectory>,
    local_status: RwLock<LocalIdentityStatus>,
    client: ClusterClient,
    peers_path: PathBuf,
    timings: RuntimeTimings,
    wake: Notify,
    // serializes probe rounds so a CLI-triggered round and the timer never
    // interleave their two-round identity checks
    round: Mutex<()>,
}

/// Cheap, cloneable access to the running fleet.
#[derive(Clone)]
pub struct FleetHandle {
    shared: Arc<Shared>,
}

/// Owner of the background tasks. Dropping it without [`Self::shutdown`]
/// leaves the tasks running until the process exits.
pub struct FleetRuntime {
    handle: FleetHandle,
    tasks: Vec<JoinHandle<()>>,
    mdns: Option<MdnsProvider>,
    tailscale: Option<TailscaleProvider>,
}

impl FleetRuntime {
    /// Load the directory, apply the configured addresses, and start
    /// providers and the probe loop in the background.
    pub fn start(start: FleetStart) -> Result<Self, AppError> {
        let FleetStart {
            local,
            settings,
            listener,
            peers_path,
            timings,
        } = start;
        let mut directory = PeerDirectory::load(local.clone(), &peers_path)?;
        directory.set_configured(&settings.machines);
        let advertisement = MachineAdvertisement {
            api_version: crate::domain::API_VERSION,
            machine: local.identity.machine,
            boot: local.identity.boot,
            name: local.name.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            protocol: local.protocol,
            capabilities: Capabilities::detect_for_daemon(),
            addresses: listener.map(advertised_addresses).unwrap_or_default(),
        };
        let shared = Arc::new(Shared {
            local,
            advertisement,
            directory: RwLock::new(directory),
            local_status: RwLock::new(LocalIdentityStatus::Consistent),
            client: ClusterClient::default(),
            peers_path,
            timings,
            wake: Notify::new(),
            round: Mutex::new(()),
        });
        let handle = FleetHandle { shared };
        let (events_tx, events_rx) = mpsc::channel(256);
        let (mdns, tailscale) = start_providers(&handle, &settings.discovery, listener, events_tx);
        let tasks = vec![
            tokio::spawn(consume_events(handle.clone(), events_rx)),
            tokio::spawn(probe_loop(handle.clone())),
        ];
        Ok(Self {
            handle,
            tasks,
            mdns,
            tailscale,
        })
    }

    /// Handle for routes and cluster code.
    #[must_use]
    pub fn handle(&self) -> FleetHandle {
        self.handle.clone()
    }

    /// Withdraw the mDNS announcement, stop tasks, and save the directory.
    pub async fn shutdown(self) {
        if let Some(mdns) = self.mdns {
            mdns.shutdown();
        }
        if let Some(tailscale) = self.tailscale {
            tailscale.shutdown();
        }
        for task in self.tasks {
            task.abort();
        }
        self.handle.persist().await;
    }
}

fn start_providers(
    handle: &FleetHandle,
    discovery: &Discovery,
    listener: Option<SocketAddr>,
    events: mpsc::Sender<DiscoveryEvent>,
) -> (Option<MdnsProvider>, Option<TailscaleProvider>) {
    let timings = handle.shared.timings;
    let mdns = discovery
        .mdns
        .then(|| {
            let announcement = listener.and_then(|addr| mdns_announcement(handle, addr));
            MdnsProvider::start(announcement.as_ref(), events.clone(), timings.mdns)
                .map_err(|err| warn!("mDNS discovery unavailable: {err}"))
                .ok()
        })
        .flatten();
    let tailscale = discovery
        .tailscale
        .map(|ts| TailscaleProvider::start(ts.port, events, timings.tailscale));
    (mdns, tailscale)
}

fn mdns_announcement(handle: &FleetHandle, listener: SocketAddr) -> Option<MdnsAnnouncement> {
    let ip = listener.ip();
    if ip.is_loopback() {
        warn!(%listener, "fleet listener is loopback-only; mDNS announcement skipped");
        return None;
    }
    let host_label =
        crate::machine::host_machine_name().unwrap_or_else(|| handle.shared.local.name.clone());
    Some(MdnsAnnouncement {
        header: handle.shared.advertisement.header(),
        port: listener.port(),
        ip: (!ip.is_unspecified()).then_some(ip),
        host: format!("{host_label}.local."),
    })
}

/// Base URLs that peers can try for this listener.
fn advertised_addresses(listener: SocketAddr) -> Vec<MachineAddress> {
    let port = listener.port();
    if !listener.ip().is_unspecified() {
        return vec![MachineAddress::from_socket(listener)];
    }
    let interfaces = match if_addrs::get_if_addrs() {
        Ok(interfaces) => interfaces,
        Err(err) => {
            warn!("list interfaces for fleet advertisement: {err}");
            return Vec::new();
        }
    };
    let want_v6 = listener.is_ipv6();
    let mut addresses: Vec<MachineAddress> = interfaces
        .iter()
        .map(if_addrs::Interface::ip)
        .filter(|ip| is_routable_peer_ip(*ip))
        .filter(|ip| want_v6 || matches!(ip, IpAddr::V4(_)))
        .map(|ip| MachineAddress::from_socket(SocketAddr::new(ip, port)))
        .collect();
    addresses.sort();
    addresses.dedup();
    addresses
}

async fn consume_events(handle: FleetHandle, mut events: mpsc::Receiver<DiscoveryEvent>) {
    while let Some(event) = events.recv().await {
        handle.apply_event(event).await;
    }
}

async fn probe_loop(handle: FleetHandle) {
    let interval = handle.shared.timings.round_interval;
    loop {
        handle.run_round().await;
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            () = handle.shared.wake.notified() => {}
        }
    }
}

/// Outcome of one probe round.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct RoundReport {
    /// Addresses probed.
    pub probed: usize,
    /// Addresses that answered.
    pub answered: usize,
    /// Machines that needed a confirming round.
    pub suspects: Vec<MachineId>,
    /// Machines confirmed as duplicates in this round.
    pub duplicates: Vec<MachineId>,
}

impl FleetHandle {
    /// Local identity, name, and protocol range.
    #[must_use]
    pub fn local(&self) -> &LocalMachine {
        &self.shared.local
    }

    /// Body for `GET /v1/cluster/machine`.
    #[must_use]
    pub fn advertisement(&self) -> &MachineAdvertisement {
        &self.shared.advertisement
    }

    /// Whether another live daemon claims the local machine UUID.
    pub async fn local_identity_status(&self) -> LocalIdentityStatus {
        self.shared.local_status.read().await.clone()
    }

    /// Snapshot of known peers.
    pub async fn peers(&self) -> Vec<PeerView> {
        self.shared.directory.read().await.peers(Utc::now())
    }

    /// Addresses that have not bound to a machine.
    pub async fn unresolved(&self) -> Vec<UnresolvedAddress> {
        self.shared.directory.read().await.unresolved(Utc::now())
    }

    /// Resolve a machine name to the local machine or one peer UUID.
    pub async fn resolve_name(&self, name: &MachineName) -> Result<NameTarget, AppError> {
        Ok(self
            .shared
            .directory
            .read()
            .await
            .resolve_name(name, Utc::now())?)
    }

    /// Add an explicit address, persist it, and probe it soon.
    pub async fn add_explicit(&self, address: MachineAddress) -> bool {
        let added = self.shared.directory.write().await.add_explicit(address);
        self.persist().await;
        self.shared.wake.notify_one();
        added
    }

    /// Remove an explicit address and persist the change.
    pub async fn remove_explicit(&self, address: &MachineAddress) -> bool {
        let removed = self.shared.directory.write().await.remove_explicit(address);
        self.persist().await;
        removed
    }

    /// Forget a machine and persist the change.
    pub async fn forget_machine(&self, machine: MachineId) -> bool {
        let forgotten = self.shared.directory.write().await.forget_machine(machine);
        self.persist().await;
        forgotten
    }

    /// Run a probe round now and wait for it.
    pub async fn discover_now(&self) -> RoundReport {
        self.run_round().await
    }

    /// Probe one address. The result is recorded like any other probe.
    pub async fn probe_address(
        &self,
        address: &MachineAddress,
    ) -> Result<ProbedMachine, ProbeError> {
        let result = probe(&self.shared.client, address, self.shared.local.protocol).await;
        let now = Utc::now();
        let mut directory = self.shared.directory.write().await;
        match &result {
            Ok(probed) => directory.record_probe(address, probed, now),
            Err(err) => directory.record_probe_failure(address, err.to_string(), now),
        }
        result
    }

    /// Resolve a destination UUID to an address that answers for it now.
    ///
    /// Tries addresses in preference order and probes each one. An address
    /// that answers for another installation is rebound to that installation
    /// and skipped; the destination UUID never changes. Call this before each
    /// retry cycle instead of reusing a saved address.
    pub async fn connect(&self, machine: MachineId) -> Result<VerifiedDestination, AppError> {
        let plan = self
            .shared
            .directory
            .read()
            .await
            .route(machine, Utc::now())?;
        let mut last_failure = String::from("no address answered");
        let mut saw_mismatch = false;
        for ranked in plan.addresses {
            let address = ranked.address;
            let result = probe(&self.shared.client, &address, self.shared.local.protocol).await;
            let now = Utc::now();
            let probed = match result {
                Ok(probed) => probed,
                Err(err) => {
                    last_failure = err.to_string();
                    self.shared.directory.write().await.record_probe_failure(
                        &address,
                        last_failure.clone(),
                        now,
                    );
                    continue;
                }
            };
            self.shared
                .directory
                .write()
                .await
                .record_probe(&address, &probed, now);
            match check_probed(&address, machine, probed) {
                Ok(verified) => return Ok(verified),
                Err(DestinationError::IdentityMismatch { found, .. }) => {
                    saw_mismatch = true;
                    last_failure = format!("{address} now answers for machine {found}");
                }
                Err(DestinationError::Incompatible { local, remote, .. }) => {
                    return Err(AppError::ClusterProtocolIncompatible {
                        machine,
                        local,
                        remote,
                    });
                }
                Err(DestinationError::Unreachable(err)) => last_failure = err.to_string(),
            }
        }
        if saw_mismatch {
            // stale addresses moved; ask providers' fresh reports to be probed
            self.shared.wake.notify_one();
        }
        Err(AppError::MachineUnavailable {
            machine,
            message: last_failure,
        })
    }

    async fn apply_event(&self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::Seen { sighting, claim } => {
                // only an exact match of both UUIDs is this daemon; the same
                // machine UUID with another boot is probed like any peer
                if claim.is_some_and(|c| self.shared.local.identity.is_self(c.machine, c.boot)) {
                    return;
                }
                let mut directory = self.shared.directory.write().await;
                let new = directory.binding(&sighting.address).is_none();
                directory.record_sighting(sighting);
                drop(directory);
                if new {
                    self.shared.wake.notify_one();
                }
            }
            DiscoveryEvent::Withdrawn { address, provider } => {
                self.shared
                    .directory
                    .write()
                    .await
                    .withdraw_sighting(&address, provider);
            }
            DiscoveryEvent::ProviderFailed { provider, message } => {
                debug!(?provider, "discovery provider failed: {message}");
            }
        }
    }

    async fn run_round(&self) -> RoundReport {
        let _round = self.shared.round.lock().await;
        let candidates = self
            .shared
            .directory
            .read()
            .await
            .probe_candidates(Utc::now());
        let first = self.probe_all(candidates.clone()).await;
        let mut report = RoundReport {
            probed: candidates.len(),
            answered: first.len(),
            ..RoundReport::default()
        };
        let duplicates = self.shared.directory.read().await.duplicates();
        let suspects = identity::suspects(&first, &self.shared.local.identity, &duplicates);
        if !suspects.is_empty() {
            report.suspects = suspects.iter().copied().collect();
            tokio::time::sleep(self.shared.timings.recheck_delay).await;
            let recheck = identity::recheck_addresses(&first, &suspects);
            let second = self.probe_all(recheck.into_iter().collect()).await;
            report.duplicates = self.decide(&suspects, &first, &second).await;
        }
        self.shared.directory.write().await.prune(Utc::now());
        self.persist().await;
        report
    }

    async fn decide(
        &self,
        suspects: &BTreeSet<MachineId>,
        first: &[ProbeObservation],
        second: &[ProbeObservation],
    ) -> Vec<MachineId> {
        let local = self.shared.local.identity;
        let now = Utc::now();
        let mut duplicates = Vec::new();
        for machine in suspects {
            let verdict = identity::confirm(*machine, first, second, &local);
            if let IdentityVerdict::Duplicate { boots } = &verdict {
                duplicates.push(*machine);
                warn!(%machine, ?boots, "duplicate live machine identity");
            }
            if *machine == local.machine {
                self.apply_local_verdict(verdict, now).await;
                continue;
            }
            self.shared
                .directory
                .write()
                .await
                .apply_verdict(*machine, &verdict, now);
        }
        duplicates
    }

    async fn apply_local_verdict(&self, verdict: IdentityVerdict, now: DateTime<Utc>) {
        let IdentityVerdict::Duplicate { boots } = verdict else {
            return;
        };
        error!(
            machine = %self.shared.local.identity.machine,
            "another live daemon claims this machine UUID; give the copied installation a fresh state directory"
        );
        *self.shared.local_status.write().await = LocalIdentityStatus::DuplicateMachineIdentity {
            boots,
            detected_at: now,
        };
    }

    /// Probe addresses concurrently, record every result, and return the
    /// successful observations.
    async fn probe_all(&self, addresses: Vec<MachineAddress>) -> Vec<ProbeObservation> {
        let limit = Arc::new(tokio::sync::Semaphore::new(
            self.shared.timings.probe_concurrency.max(1),
        ));
        let mut set = JoinSet::new();
        for address in addresses {
            let client = self.shared.client;
            let protocol = self.shared.local.protocol;
            let limit = limit.clone();
            set.spawn(async move {
                // a closed semaphore only happens if this function panicked
                let _permit = limit.acquire_owned().await.ok();
                let result = probe(&client, &address, protocol).await;
                (address, result, Utc::now())
            });
        }
        let mut results: Vec<(
            MachineAddress,
            Result<ProbedMachine, ProbeError>,
            DateTime<Utc>,
        )> = Vec::new();
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(result) => results.push(result),
                Err(err) => warn!("probe task failed: {err}"),
            }
        }
        let mut directory = self.shared.directory.write().await;
        let mut observations = Vec::new();
        for (address, result, at) in results {
            match result {
                Ok(probed) => {
                    directory.record_probe(&address, &probed, at);
                    observations.push(ProbeObservation {
                        address,
                        header: probed.header(),
                        observed_at: at,
                    });
                }
                Err(err) => directory.record_probe_failure(&address, err.to_string(), at),
            }
        }
        observations
    }

    async fn persist(&self) {
        let directory = self.shared.directory.read().await;
        if let Err(err) = directory.save(&self.shared.peers_path) {
            warn!(path = %self.shared.peers_path.display(), "save fleet peer directory: {err}");
        }
    }
}

/// Log the effective fleet setup once at start.
pub fn log_start(local: &LocalMachine, listener: Option<SocketAddr>) {
    info!(
        machine = %local.identity.machine,
        boot = %local.identity.boot,
        name = %local.name,
        listener = listener.map(|addr| addr.to_string()).as_deref().unwrap_or("off"),
        "fleet enabled"
    );
}
