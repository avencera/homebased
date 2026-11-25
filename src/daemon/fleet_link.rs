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
//! Today only pools use the link; other Fleet machines keep plain HTTP

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{Extension, Router};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use iroh::endpoint::{Connection, QuicTransportConfig, RecvStream, SendStream, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::AppState;
use crate::config::IrohConfig;
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
const RECONNECT: Duration = Duration::from_secs(2);

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

/// What an endpoint id on the allowlist may do
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Refuse unless the stream comes from the pool member `machine`
    pub fn require_member(&self, machine: MachineId) -> Result<(), AppError> {
        if self.grant == Some(Grant::PoolMember { machine }) {
            return Ok(());
        }
        Err(AppError::Permission {
            message: format!(
                "link peer {} may only send events for its own work",
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
    connection: Option<Connection>,
    forwarder: Option<(MachineAddress, AbortOnDrop)>,
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
            if let Some(connection) = &removed.connection {
                connection.close(REFUSED.into(), b"access revoked");
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

    /// Keep a connection to `peer` open, redialing whenever it drops; the
    /// peer's streams on it are served by its grant like any other
    pub fn dial(&self, state: &AppState, peer: EndpointAddr) {
        let (link, state) = (self.clone(), state.clone());
        tokio::spawn(async move {
            loop {
                match link.0.endpoint.connect(peer.clone(), FLEET_ALPN).await {
                    Ok(connection) => {
                        serve_connection(&state, &link, connection.clone()).await;
                        warn!(reason = ?connection.close_reason(), "fleet link to {} closed", peer.id.fmt_short());
                    }
                    Err(error) => warn!("fleet link to {}: {error}", peer.id.fmt_short()),
                }
                tokio::time::sleep(RECONNECT).await;
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
        allowed.get(&endpoint)?.connection.clone()
    }

    /// Make `connection` the one forwarders to its id use
    fn attach(&self, connection: &Connection) {
        let endpoint = connection.remote_id();
        self.0.allowed.send_if_modified(|allowed| {
            let Some(entry) = allowed.get_mut(&endpoint) else {
                return false;
            };
            let current = entry.connection.as_ref().map(Connection::stable_id);
            if current == Some(connection.stable_id()) {
                return false;
            }
            // a peer keeps one connection, and a revoke closes only the one
            // tracked here, so an older one must not stay open beside it
            if let Some(superseded) = entry.connection.replace(connection.clone()) {
                superseded.close(REFUSED.into(), b"superseded");
            }
            true
        });
    }

    fn detach(&self, connection: &Connection) {
        let endpoint = connection.remote_id();
        self.0.allowed.send_if_modified(|allowed| {
            let Some(entry) = allowed.get_mut(&endpoint) else {
                return false;
            };
            let current = entry.connection.as_ref().map(Connection::stable_id);
            if current != Some(connection.stable_id()) {
                return false;
            }
            entry.connection = None;
            true
        });
    }
}

/// Load this daemon's iroh key, making one on first use
///
/// The key outlives restarts, so a daemon keeps the endpoint id its peers
/// allow and the addresses they hold
fn load_key(home: &Home) -> Result<SecretKey, AppError> {
    let path = home.iroh_key_path();
    match std::fs::read(&path) {
        Ok(bytes) => {
            let bytes: [u8; 32] = bytes.try_into().map_err(|_| AppError::Internal {
                message: format!("{} is not a 32-byte iroh key", path.display()),
            })?;
            return Ok(SecretKey::from_bytes(&bytes));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let key = SecretKey::generate();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    write_new_private(&path, &key.to_bytes())?;
    Ok(key)
}

fn write_new_private(path: &std::path::Path, bytes: &[u8]) -> Result<(), AppError> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    Ok(())
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
            link.attach(&connection);
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
