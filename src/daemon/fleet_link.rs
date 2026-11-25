//! The Fleet over iroh: one endpoint per daemon, peers allowed by endpoint id
//!
//! A daemon with `[fleet.iroh]` runs one iroh endpoint keyed by
//! `iroh/secret.key`, which is its identity on the link. It serves a
//! connection only from an endpoint id on its allowlist, and what that id may
//! call depends on its [`Grant`]. Which side dials is separate from what it
//! may do: a temporary machine dials its coordinator and grants it
//! [`Grant::Peer`] on that same connection.
//!
//! Each Fleet call is one HTTP/1 exchange on its own QUIC bi-stream, so the
//! Fleet API does not change. Each side's Fleet knows a link peer at a
//! loopback address, where a forwarder turns each TCP connection into a
//! stream on the link. A [`Grant::Peer`] stream reaches this daemon's own API
//! as a plain HTTP peer would; a [`Grant::PoolMember`] stream reaches only
//! the member routes. An id with no grant reaches the pool join route when
//! this daemon coordinates pools; a failed join closes its connection.
//!
//! Fleet machines whose `[[fleet.machines]]` entry names an `iroh` endpoint
//! keep one connection to it, and pools' temporary machines one to their
//! coordinator. Both ends of a pair may dial at once; [`replaces`] picks the
//! connection both ends keep, so they never close each other's in turn

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{Extension, Router};
use chrono::{DateTime, Utc};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use iroh::endpoint::{Connection, QuicTransportConfig, RecvStream, SendStream, Side, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::AppState;
use crate::config::{FleetMachine, FleetSettings, IrohConfig};
use crate::error::AppError;
use crate::fleet::address::MachineAddress;
use crate::fleet::iroh::{FLEET_ALPN, LinkAddr, Relays};
use crate::home::Home;
use crate::machine::MachineId;

/// How long a daemon waits for an ungranted id's enrollment stream
const ENROLL_WINDOW: Duration = Duration::from_secs(30);

/// How long [`FleetLink::addr`] waits to reach a relay before it answers
/// anyway; with n0's relays the id alone still finds the endpoint
const ONLINE_WAIT: Duration = Duration::from_secs(10);

/// How long a forwarded TCP connection waits for its peer's connection
const LINK_WAIT: Duration = Duration::from_secs(10);

/// Wait between attempts to reach a peer this daemon dials
pub(super) const RECONNECT: Duration = Duration::from_secs(2);

/// Longest wait between dials of a declared Fleet peer that stays
/// unreachable, such as a machine asleep for the night; each dial through
/// n0's rate-limited relays counts against them
const FLEET_REDIAL: Duration = Duration::from_secs(60);

/// How long a refused peer has to read its answer before the connection closes
const REFUSAL_GRACE: Duration = Duration::from_secs(5);

/// A connection with no packet for this long is dead; iroh's keep-alives
/// every 5 seconds hold a healthy one open. Shorter than QUIC's default 30
/// seconds so a temporary machine notices a coordinator that died,
/// reconnects, and delivers what it owes while its lease still runs
const IDLE_TIMEOUT: Duration = Duration::from_secs(12);

/// How long shutdown waits to tell peers the endpoint is closing
const CLOSE_WAIT: Duration = Duration::from_secs(2);

/// QUIC close code for an id this daemon does not serve
const REFUSED: u32 = 1;

/// QUIC close code for a connection another one to the same peer replaced
const SUPERSEDED: u32 = 2;

/// How long after a connection attaches a second one from the other end
/// counts as a simultaneous dial rather than a peer that lost the first;
/// both dials complete within a handshake of each other, a few round trips
/// even through a relay
const SETTLE: Duration = Duration::from_secs(10);

impl Attached {
    fn now(connection: &Connection) -> Self {
        Self {
            connection: connection.clone(),
            since: Utc::now(),
            at: Instant::now(),
        }
    }
}

/// What an endpoint id on the allowlist may do
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "grant", rename_all = "snake_case")]
pub enum Grant {
    /// The whole Fleet API, as a plain HTTP Fleet peer has it
    Peer,
    /// A temporary machine of a pool this daemon coordinates: the member
    /// routes only, and events only for its own work
    PoolMember {
        /// The machine the join bound to this id
        machine: MachineId,
    },
}

/// The peer of one stream on the link, as this daemon's handlers see it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkPeer {
    /// The authenticated endpoint id of the connection
    pub endpoint: EndpointId,
    /// What the id may do; `None` while it enrolls
    pub grant: Option<Grant>,
}

impl LinkPeer {
    /// Refuse unless the stream comes from the pool member `machine`, which
    /// runs the work the request is about
    pub fn require_member(&self, machine: MachineId) -> Result<(), AppError> {
        if self.grant == Some(Grant::PoolMember { machine }) {
            return Ok(());
        }
        Err(AppError::Permission {
            message: format!(
                "link peer {} may only reach its own work",
                self.endpoint.fmt_short()
            ),
        })
    }
}

/// Aborts its task when dropped, so revoking an id stops its forwarder
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One id on the allowlist
struct Allowed {
    grant: Grant,
    connection: Option<Attached>,
    forwarder: Option<(MachineAddress, AbortOnDrop)>,
}

/// The connection forwarders to one id use
struct Attached {
    connection: Connection,
    since: DateTime<Utc>,
    at: Instant,
}

/// Which end opened a connection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dialer {
    /// This daemon
    Local,
    /// The peer
    Peer,
}

impl Dialer {
    fn of(connection: &Connection) -> Self {
        match connection.side() {
            Side::Client => Self::Local,
            Side::Server => Self::Peer,
        }
    }
}

/// Whether a new connection to `peer`, opened by `new`, replaces the one
/// attached now, opened by `attached`; `settled` says the attached one is
/// older than [`SETTLE`]
///
/// Each end dials only while it has no connection attached, so a peer that
/// dials again, or dials once the attached connection has settled, has lost
/// that connection (it restarted, or saw it time out first) and the newer
/// one wins. Two fresh connections opened by different ends mean both ends
/// dialed at once: each keeps the one the smaller endpoint id opened. Both
/// ends compute the same answer, so each closes the same connection, and the
/// dialer that lost finds a connection attached and does not dial again
#[must_use]
pub fn replaces(
    local: EndpointId,
    peer: EndpointId,
    attached: Dialer,
    new: Dialer,
    settled: bool,
) -> bool {
    if attached == new || settled {
        return true;
    }
    let preferred = if local < peer {
        Dialer::Local
    } else {
        Dialer::Peer
    };
    new == preferred
}

/// One id on the allowlist, as `homebased fleet machines` shows it
// no `deny_unknown_fields`: serde does not support it with `flatten`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkPeerView {
    /// The peer's endpoint id
    pub endpoint: EndpointId,
    /// What it may do
    #[serde(flatten)]
    pub grant: Grant,
    /// The connection in use, when there is one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<LinkConnectionView>,
}

/// The connection to one peer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkConnectionView {
    /// Which end opened it
    pub dialed_by: Dialer,
    /// When this daemon started using it
    pub since: DateTime<Utc>,
}

/// This daemon's end of the Fleet link
#[derive(Clone)]
pub struct FleetLink(Arc<Shared>);

struct Shared {
    endpoint: Endpoint,
    relays: Relays,
    /// Whether ids with no grant may enroll through the pool join route
    enroll: bool,
    // a watch rather than a lock, since the daemon holds no shared lock;
    // grants, revocations, and connection changes write it
    allowed: watch::Sender<HashMap<EndpointId, Allowed>>,
}

impl FleetLink {
    /// Bind this daemon's endpoint with its kept key; `enroll` lets ids with
    /// no grant reach the pool join route
    pub async fn start(home: &Home, config: &IrohConfig, enroll: bool) -> Result<Self, AppError> {
        let key = load_key(home)?;
        let endpoint = bind_endpoint(key, config).await?;
        info!(endpoint = %endpoint.id(), relays = %config.relays, "fleet link listening");
        Ok(Self(Arc::new(Shared {
            endpoint,
            relays: config.relays.clone(),
            enroll,
            allowed: watch::Sender::new(HashMap::new()),
        })))
    }

    /// Where peers dial this daemon
    pub async fn addr(&self) -> LinkAddr {
        if self.0.relays != Relays::Disabled {
            let _ = tokio::time::timeout(ONLINE_WAIT, self.0.endpoint.online()).await;
        }
        LinkAddr::of(&self.0.endpoint.addr())
    }

    /// Relays this endpoint uses
    pub fn relays(&self) -> Relays {
        self.0.relays.clone()
    }

    /// This daemon's endpoint id
    pub fn id(&self) -> EndpointId {
        self.0.endpoint.id()
    }

    /// The allowlist and each id's connection, sorted by endpoint id
    pub fn peers(&self) -> Vec<LinkPeerView> {
        let mut peers: Vec<LinkPeerView> = self
            .0
            .allowed
            .borrow()
            .iter()
            .map(|(endpoint, allowed)| LinkPeerView {
                endpoint: *endpoint,
                grant: allowed.grant,
                connection: allowed
                    .connection
                    .as_ref()
                    .map(|attached| LinkConnectionView {
                        dialed_by: Dialer::of(&attached.connection),
                        since: attached.since,
                    }),
            })
            .collect();
        peers.sort_by_key(|peer| peer.endpoint);
        peers
    }

    /// Grant the Fleet's declared peers the whole API, and open a forwarder
    /// for each `[[fleet.machines]]` endpoint; the forwarders' addresses, by
    /// endpoint id, are where this daemon's Fleet reaches those machines
    pub async fn allow_fleet(
        &self,
        settings: &FleetSettings,
    ) -> Result<BTreeMap<EndpointId, MachineAddress>, AppError> {
        for endpoint in settings.link_peers() {
            if endpoint == self.id() {
                warn!(
                    "fleet link: {} is this daemon's own endpoint id; skipped",
                    endpoint.fmt_short()
                );
                continue;
            }
            self.grant(endpoint, Grant::Peer);
        }

        let mut forwarders = BTreeMap::new();
        for link in settings.machines.iter().filter_map(FleetMachine::iroh) {
            if link.id == self.id() {
                continue;
            }
            let address = self
                .forward_to(link.id, SocketAddr::from(([127, 0, 0, 1], 0)))
                .await?;
            forwarders.insert(link.id, address);
        }
        Ok(forwarders)
    }

    /// Keep a connection to each `[[fleet.machines]]` endpoint
    pub fn dial_fleet(&self, state: &AppState, settings: &FleetSettings) {
        for link in settings.machines.iter().filter_map(FleetMachine::iroh) {
            if link.id != self.id() {
                self.dial(state, link.endpoint_addr(), FLEET_REDIAL);
            }
        }
    }

    /// Allow `endpoint` to do what `grant` says
    pub fn grant(&self, endpoint: EndpointId, grant: Grant) {
        self.0.allowed.send_if_modified(|allowed| {
            let entry = allowed.entry(endpoint).or_insert(Allowed {
                grant,
                connection: None,
                forwarder: None,
            });
            let changed = entry.grant != grant;
            entry.grant = grant;
            changed
        });
    }

    /// Take `endpoint` off the allowlist, close its connection, and stop its
    /// forwarder
    pub fn revoke(&self, endpoint: EndpointId) {
        self.0.allowed.send_if_modified(|allowed| {
            let Some(removed) = allowed.remove(&endpoint) else {
                return false;
            };
            if let Some(attached) = &removed.connection {
                attached.connection.close(REFUSED.into(), b"access revoked");
            }
            true
        });
    }

    /// The loopback address where this daemon's Fleet reaches `endpoint`,
    /// listening at `listen` on first use; the same address while it stays
    /// allowed
    pub async fn forward_to(
        &self,
        endpoint: EndpointId,
        listen: SocketAddr,
    ) -> Result<MachineAddress, AppError> {
        if let Some(address) = self.forwarder_address(endpoint) {
            return Ok(address);
        }

        let listener = TcpListener::bind(listen).await?;
        let mut address = MachineAddress::from_socket(listener.local_addr()?);
        let link = self.clone();
        let task = AbortOnDrop(tokio::spawn(async move {
            forward(listener, move || {
                let link = link.clone();
                async move { link.wait_for_connection(endpoint).await }
            })
            .await;
        }));
        let mut missing = false;
        self.0.allowed.send_modify(|allowed| {
            let Some(entry) = allowed.get_mut(&endpoint) else {
                missing = true;
                return;
            };
            // a call that won the race keeps its forwarder; this one's stops
            // when it drops here
            match &entry.forwarder {
                Some((existing, _)) => address = existing.clone(),
                None => entry.forwarder = Some((address.clone(), task)),
            }
        });
        if missing {
            return Err(AppError::Internal {
                message: format!("endpoint {} is not allowed", endpoint.fmt_short()),
            });
        }
        Ok(address)
    }

    /// Keep a connection to `peer` open, dialing whenever none is attached,
    /// such as after it drops; the peer's streams on it are served by its
    /// grant like any other. A connection the peer opened counts, so two
    /// daemons that dial each other settle on one
    ///
    /// A failed dial is retried after [`RECONNECT`], doubling up to
    /// `longest_wait` while the peer stays unreachable
    pub fn dial(&self, state: &AppState, peer: EndpointAddr, longest_wait: Duration) {
        let (link, state) = (self.clone(), state.clone());
        tokio::spawn(async move {
            let mut wait = RECONNECT;
            loop {
                link.wait_until_unconnected(peer.id).await;
                let connect = link.0.endpoint.connect(peer.clone(), FLEET_ALPN);
                // a handshake begun while the peer was down can finish long
                // after the peer dialed in; once its connection is attached
                // this dial is only a second connection to settle
                let attempt = tokio::select! {
                    attempt = connect => attempt,
                    () = link.wait_until_connected(peer.id) => continue,
                };
                match attempt {
                    Ok(connection) => {
                        // a peer that has not allowed this endpoint yet takes
                        // the handshake and closes at once, so only a link
                        // that held resets the backoff
                        let opened = Instant::now();
                        serve_connection(&state, &link, connection.clone()).await;
                        if opened.elapsed() >= SETTLE {
                            wait = RECONNECT;
                        }
                        warn!(reason = ?connection.close_reason(), "fleet link to {} closed", peer.id.fmt_short());
                    }
                    // a peer that is off for hours fails every dial, so only
                    // the first failure in a row is a warning
                    Err(error) if wait == RECONNECT => {
                        warn!("fleet link to {}: {error}", peer.id.fmt_short());
                    }
                    Err(error) => debug!("fleet link to {}: {error}", peer.id.fmt_short()),
                }
                tokio::time::sleep(wait).await;
                if link.connected(peer.id) {
                    wait = RECONNECT;
                } else {
                    wait = (wait * 2).min(longest_wait.max(RECONNECT));
                }
            }
        });
    }

    /// Tell connected peers this endpoint is going away, so they reconnect
    /// as soon as it is back rather than at their idle timeout
    pub(super) async fn close(&self) {
        let _ = tokio::time::timeout(CLOSE_WAIT, self.0.endpoint.close()).await;
    }

    fn grant_of(&self, endpoint: EndpointId) -> Option<Grant> {
        self.0
            .allowed
            .borrow()
            .get(&endpoint)
            .map(|entry| entry.grant)
    }

    fn forwarder_address(&self, endpoint: EndpointId) -> Option<MachineAddress> {
        self.0
            .allowed
            .borrow()
            .get(&endpoint)
            .and_then(|entry| entry.forwarder.as_ref().map(|(address, _)| address.clone()))
    }

    async fn wait_for_connection(&self, endpoint: EndpointId) -> Option<Connection> {
        let mut allowed = self.0.allowed.subscribe();
        let ready = allowed.wait_for(|allowed| {
            allowed
                .get(&endpoint)
                .is_none_or(|entry| entry.connection.is_some())
        });
        let allowed = tokio::time::timeout(LINK_WAIT, ready).await.ok()?.ok()?;
        let attached = allowed.get(&endpoint)?.connection.as_ref()?;
        Some(attached.connection.clone())
    }

    fn connected(&self, endpoint: EndpointId) -> bool {
        self.0
            .allowed
            .borrow()
            .get(&endpoint)
            .is_some_and(|entry| entry.connection.is_some())
    }

    /// Wait until no connection to `endpoint` is attached
    async fn wait_until_unconnected(&self, endpoint: EndpointId) {
        self.wait_for_attached(endpoint, false).await;
    }

    /// Wait until a connection to `endpoint` is attached
    async fn wait_until_connected(&self, endpoint: EndpointId) {
        self.wait_for_attached(endpoint, true).await;
    }

    async fn wait_for_attached(&self, endpoint: EndpointId, attached: bool) {
        let mut allowed = self.0.allowed.subscribe();
        // the sender lives as long as `self`, so the wait cannot fail
        let _ = allowed
            .wait_for(|allowed| {
                let connected = allowed
                    .get(&endpoint)
                    .is_some_and(|entry| entry.connection.is_some());
                connected == attached
            })
            .await;
    }

    /// Make `connection` the one forwarders to its id use, unless the one
    /// attached now wins the tie-break; the loser is closed. Returns whether
    /// `connection` is attached
    fn attach(&self, connection: &Connection) -> bool {
        let endpoint = connection.remote_id();
        let local = self.id();
        let mut kept = true;
        self.0.allowed.send_if_modified(|allowed| {
            let Some(entry) = allowed.get_mut(&endpoint) else {
                return false;
            };
            // a connection the tie-break already closed must not come back
            // when its stream loop goes round once more
            if connection.close_reason().is_some() {
                kept = false;
                return false;
            }
            let Some(current) = &entry.connection else {
                entry.connection = Some(Attached::now(connection));
                return true;
            };
            if current.connection.stable_id() == connection.stable_id() {
                return false;
            }
            let current_live = current.connection.close_reason().is_none();
            let new_wins = !current_live
                || replaces(
                    local,
                    endpoint,
                    Dialer::of(&current.connection),
                    Dialer::of(connection),
                    current.at.elapsed() >= SETTLE,
                );
            if !new_wins {
                kept = false;
                connection.close(SUPERSEDED.into(), b"superseded");
                return false;
            }
            // a peer keeps one connection, and a revoke closes only the one
            // tracked here, so an older one must not stay open beside it
            if let Some(superseded) = entry.connection.replace(Attached::now(connection)) {
                superseded
                    .connection
                    .close(SUPERSEDED.into(), b"superseded");
            }
            true
        });
        kept
    }

    fn detach(&self, connection: &Connection) {
        let endpoint = connection.remote_id();
        self.0.allowed.send_if_modified(|allowed| {
            let Some(entry) = allowed.get_mut(&endpoint) else {
                return false;
            };
            let current = entry
                .connection
                .as_ref()
                .map(|attached| attached.connection.stable_id());
            if current != Some(connection.stable_id()) {
                return false;
            }
            entry.connection = None;
            true
        });
    }
}

/// This installation's endpoint id on the Fleet link, making its key when
/// it has none yet, so peers can allow the id before the link is turned on
pub fn endpoint_id(home: &Home) -> Result<EndpointId, AppError> {
    Ok(load_key(home)?.public())
}

/// Load this daemon's iroh key, making one on first use
///
/// The key outlives restarts, so a daemon keeps the endpoint id its peers
/// allow and the addresses they hold
fn load_key(home: &Home) -> Result<SecretKey, AppError> {
    let path = home.iroh_key_path();
    if let Some(key) = read_key(&path)? {
        return Ok(key);
    }

    let key = SecretKey::generate();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match write_new_private(&path, &key.to_bytes()) {
        Ok(()) => Ok(key),
        // `fleet endpoint-id` and a starting daemon made one at once; both
        // use the one that was written
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => read_key(&path)?
            .ok_or_else(|| AppError::Internal {
                message: format!("{} vanished while it was written", path.display()),
            }),
        Err(error) => Err(error.into()),
    }
}

fn read_key(path: &std::path::Path) -> Result<Option<SecretKey>, AppError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| AppError::Internal {
        message: format!("{} is not a 32-byte iroh key", path.display()),
    })?;
    Ok(Some(SecretKey::from_bytes(&bytes)))
}

/// Write `bytes` to a new owner-only file at `path`, failing with
/// `AlreadyExists` when it is there. The file appears only once complete,
/// so a concurrent reader never sees it half written
fn write_new_private(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    // unique per call, so two callers never write through one staging file
    let staging = path.with_extension(format!("new.{}", uuid::Uuid::now_v7()));
    let written = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&staging)
        .and_then(|mut file| {
            file.write_all(bytes)?;
            file.sync_all()
        })
        // a hard link, unlike a rename, refuses to replace a key another
        // process published first
        .and_then(|()| std::fs::hard_link(&staging, path));
    // the staging name is this call's alone, so removing it is always safe
    let _ = std::fs::remove_file(&staging);
    written
}

async fn bind_endpoint(key: SecretKey, config: &IrohConfig) -> Result<Endpoint, AppError> {
    let builder = match &config.relays {
        Relays::N0 => Endpoint::builder(presets::N0),
        Relays::Custom(urls) => {
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::custom(urls.iter().cloned()))
        }
        Relays::Disabled => Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
    };
    let idle = IDLE_TIMEOUT
        .try_into()
        .map_err(|error| AppError::Internal {
            message: format!("iroh idle timeout: {error}"),
        })?;
    let transport = QuicTransportConfig::builder()
        .max_idle_timeout(Some(idle))
        .build();
    builder
        .transport_config(transport)
        .bind_addr(SocketAddr::from(([0, 0, 0, 0], config.port)))
        .map_err(|error| AppError::Internal {
            message: format!("iroh bind address: {error}"),
        })?
        .secret_key(key)
        .alpns(vec![FLEET_ALPN.to_vec()])
        .bind()
        .await
        .map_err(|error| AppError::Internal {
            message: format!("bind iroh endpoint: {error}"),
        })
}

/// Accept connections until the endpoint closes
pub(super) async fn serve(state: AppState, link: FleetLink) {
    while let Some(incoming) = link.0.endpoint.accept().await {
        let (state, link) = (state.clone(), link.clone());
        tokio::spawn(async move {
            match incoming.await {
                Ok(connection) => serve_connection(&state, &link, connection).await,
                Err(error) => debug!("fleet link handshake: {error}"),
            }
        });
    }
}

/// Serve the streams a peer opens on `connection`, each by the peer's grant
/// at the time it arrives
async fn serve_connection(state: &AppState, link: &FleetLink, connection: Connection) {
    let endpoint = connection.remote_id();
    loop {
        let grant = link.grant_of(endpoint);
        if grant.is_some() {
            if !link.attach(&connection) {
                break;
            }
        } else if !link.0.enroll {
            connection.close(REFUSED.into(), b"not allowed");
            break;
        }

        let next = match grant {
            Some(_) => connection.accept_bi().await.ok(),
            None => tokio::time::timeout(ENROLL_WINDOW, connection.accept_bi())
                .await
                .ok()
                .and_then(Result::ok),
        };
        let Some((send, recv)) = next else {
            break;
        };
        // the grant may have been revoked while this connection waited
        let current = link.grant_of(endpoint);
        if grant.is_some() && current.is_none() {
            connection.close(REFUSED.into(), b"revoked");
            break;
        }
        let grant = current;

        let peer = LinkPeer { endpoint, grant };
        match grant {
            Some(Grant::Peer) => {
                tokio::spawn(serve_own_api(state.web, send, recv));
                continue;
            }
            Some(Grant::PoolMember { .. }) => {
                let router = routes(super::pools::member_routes(state), state, peer);
                tokio::spawn(serve_stream(router, send, recv));
                continue;
            }
            None => {}
        }

        // an unknown peer must not hold a stream open by sending a request slowly
        let router = routes(super::pools::enrollment_routes(), state, peer);
        let _ = tokio::time::timeout(ENROLL_WINDOW, serve_stream(router, send, recv)).await;
        if link.grant_of(endpoint).is_none() {
            // a close drops answer bytes the peer has not acknowledged, so
            // give it time to read the refusal and close first
            let _ = tokio::time::timeout(REFUSAL_GRACE, connection.closed()).await;
            connection.close(REFUSED.into(), b"enrollment refused");
            break;
        }
    }
    link.detach(&connection);
}

fn routes(routes: Router<AppState>, state: &AppState, peer: LinkPeer) -> Router {
    routes.layer(Extension(peer)).with_state(state.clone())
}

async fn serve_stream(router: Router, send: SendStream, recv: RecvStream) {
    let io = TokioIo::new(tokio::io::join(recv, send));
    let service = TowerToHyperService::new(router);
    // a forwarded client may finish its side once the request is sent, and
    // still waits for the answer
    let served = http1::Builder::new()
        .half_close(true)
        .serve_connection(io, service)
        .await;
    if let Err(error) = served {
        debug!("fleet link stream: {error}");
    }
}

/// Hand a [`Grant::Peer`] stream to this daemon's own listener, so the peer
/// gets exactly what a plain HTTP Fleet peer gets
async fn serve_own_api(web: Option<SocketAddr>, send: SendStream, recv: RecvStream) {
    let Some(web) = web else {
        debug!("fleet link peer stream: this daemon has no listener");
        return;
    };
    match TcpStream::connect(web).await {
        Ok(mut tcp) => {
            let mut stream = tokio::io::join(recv, send);
            if let Err(error) = tokio::io::copy_bidirectional(&mut tcp, &mut stream).await {
                debug!("fleet link peer stream: {error}");
            }
        }
        Err(error) => warn!("fleet link to own listener: {error}"),
    }
}

/// Turn each TCP connection to `listener` into a bi-stream on the
/// connection `current` gives, or drop it when there is none
async fn forward<F, Fut>(listener: TcpListener, current: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Option<Connection>> + Send + 'static,
{
    loop {
        let tcp = match listener.accept().await {
            Ok((tcp, _)) => tcp,
            Err(error) => {
                warn!("fleet link forwarder: {error}");
                continue;
            }
        };
        let connection = current();
        tokio::spawn(async move {
            if let Some(connection) = connection.await {
                pipe(tcp, &connection).await;
            }
        });
    }
}

async fn pipe(mut tcp: TcpStream, connection: &Connection) {
    let (send, recv) = match connection.open_bi().await {
        Ok(streams) => streams,
        Err(error) => {
            debug!("fleet link open stream: {error}");
            return;
        }
    };
    let mut stream = tokio::io::join(recv, send);
    if let Err(error) = tokio::io::copy_bidirectional(&mut tcp, &mut stream).await {
        debug!("fleet link stream: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::{Dialer, replaces};

    /// The connection one end keeps after seeing both, in either order:
    /// `true` for the one `a` dialed
    fn kept_at(local: iroh::EndpointId, peer: iroh::EndpointId, a_first: bool) -> bool {
        // the connection `a` dialed, as `local` sees it
        let by_a = |a: iroh::EndpointId| {
            if a == local {
                Dialer::Local
            } else {
                Dialer::Peer
            }
        };
        let (first, second) = if a_first {
            (by_a(local), by_a(peer))
        } else {
            (by_a(peer), by_a(local))
        };
        let kept = if replaces(local, peer, first, second, false) {
            second
        } else {
            first
        };
        kept == by_a(local)
    }

    #[test]
    fn both_ends_of_a_simultaneous_dial_keep_the_same_connection() {
        for _ in 0..16 {
            let a = iroh::SecretKey::generate().public();
            let b = iroh::SecretKey::generate().public();
            // whichever connection each end saw first, both keep the one the
            // smaller id dialed
            for a_first in [true, false] {
                for b_first in [true, false] {
                    let at_a = kept_at(a, b, a_first);
                    let at_b = !kept_at(b, a, b_first);
                    assert_eq!(at_a, at_b);
                    assert_eq!(at_a, a < b);
                }
            }
        }
        // a peer that dials again replaces its own earlier connection
        let (a, b) = (
            iroh::SecretKey::generate().public(),
            iroh::SecretKey::generate().public(),
        );
        assert!(replaces(a, b, Dialer::Peer, Dialer::Peer, false));
        assert!(replaces(a, b, Dialer::Local, Dialer::Local, false));
        // and once the attached connection settled, a dial from the other
        // end means that end lost it
        assert!(replaces(a, b, Dialer::Local, Dialer::Peer, true));
        assert!(replaces(a, b, Dialer::Peer, Dialer::Local, true));
    }
}
