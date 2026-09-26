//! Serve: daemon lock, socket, actors, shutdown

pub mod actors;
pub mod api;
mod cancel_delivery;
pub mod cluster;
pub mod content;
pub mod event_sender;
pub mod fleet_api;
pub mod fleet_tasks;
mod inspection;
mod keyed_locks;
pub(crate) mod message_receiver;
pub(crate) mod message_sender;
mod origin_submit;
mod peer_read;
mod release_watcher_api;
mod resource_action;
mod resource_api;
mod resource_background;
mod resource_notice_delivery;
pub(crate) mod resource_notice_sender;
mod resource_submit;
mod t3_watch;
mod thread_titles;
pub mod web;

use std::fs::File;
use std::net::SocketAddr;
use std::sync::Arc;

use ractor::{Actor, ActorRef};
use tokio::net::{TcpListener, UnixListener};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::Config;
use crate::daemon::actors::{StoreMsg, SupervisorActor, SupervisorArgs, SupervisorMsg, call};
use crate::daemon::keyed_locks::KeyedLocks;
use crate::daemon::web::WebListen;
use crate::domain::TaskId;
use crate::error::AppError;
use crate::files::StreamSlots;
use crate::fleet::FleetState;
use crate::fleet::directory::LocalMachine;
use crate::fleet::protocol::SUPPORTED_PROTOCOLS;
use crate::fleet::runtime::{FleetRuntime, FleetStart, RuntimeTimings};
use crate::home::{Home, LockMode, chmod_600, flock_exclusive};
use crate::machine::LocalIdentity;
use crate::notify::Notifier;
use crate::resource::ActionId;
use crate::submission::RequestId;
use crate::thread_title::TitleSources;

/// Axum state: actor refs plus immutable path config
#[derive(Clone)]
pub struct AppState {
    /// State directory (immutable layout)
    pub home: Home,
    /// Store actor for durable reads and writes
    pub(crate) store: ActorRef<StoreMsg>,
    /// Supervisor: owns the task lifecycle
    pub(crate) supervisor: ActorRef<SupervisorMsg>,
    /// Address the dashboard listener bound, or `None` when it is off or the
    /// bind failed
    pub web: Option<SocketAddr>,
    /// Content-origin port on the same host as the dashboard, when bound
    pub content: Option<SocketAddr>,
    /// Concurrent raw-file stream permits
    pub stream_slots: StreamSlots,
    /// Stable machine UUID, this boot's UUID, name, and protocol range
    pub machine: LocalMachine,
    /// Fleet runtime handle when `fleet.enabled` is true
    pub fleet: FleetState,
    /// Daemon-owned serializer for direct-message queue attempts
    pub(crate) message_receiver: message_receiver::MessageReceiver,
    /// Per-key locks that serialize idempotent daemon sections
    pub(crate) locks: DaemonLocks,
    /// Agent stores that name the threads on this machine, when `HOME` is known
    pub thread_titles: Option<TitleSources>,
}

/// Per-key locks this daemon holds while it resolves or sends one saved route
///
/// Each field is a separate key space. A section reads its saved state, decides,
/// and saves or sends while it holds the key, so a concurrent retry of the same
/// key observes the result instead of racing it
#[derive(Clone, Default)]
pub(crate) struct DaemonLocks {
    /// Origin-side remote task submissions, by caller request
    pub(crate) origin_submissions: KeyedLocks<RequestId>,
    /// Origin-side resource queue submissions, by caller request
    pub(crate) resource_submissions: KeyedLocks<RequestId>,
    /// Supervisor-side resource action launches, by action
    pub(crate) resource_actions: KeyedLocks<ActionId>,
    /// Supervisor-side background launches, by launch request
    pub(crate) background_launches: KeyedLocks<RequestId>,
    /// Origin-side cancellation intents, by task
    pub(crate) cancellation_intents: KeyedLocks<TaskId>,
}

/// Hold `daemon.lock`, bind the socket and optional dashboard port, start
/// actors, start fleet discovery when enabled, serve until SIGTERM
pub async fn serve(home: Home, web_listen: WebListen, config: Config) -> Result<(), AppError> {
    home.ensure()?;
    let _daemon_lock = acquire_daemon_lock(&home)?;
    // the lock is held, so no earlier boot of this installation is still serving
    let machine = LocalMachine {
        identity: LocalIdentity::start(&home)?,
        name: config.machine_name().0,
        protocol: SUPPORTED_PROTOCOLS,
    };
    let sock = home.sock_path();
    if sock.exists() {
        std::fs::remove_file(&sock)?;
    }
    let listener = UnixListener::bind(&sock).map_err(|err| AppError::Internal {
        message: format!("bind {}: {err}", sock.display()),
    })?;
    chmod_600(&sock)?;
    // bind before the actors start so `/v1/status` can report the real port
    let web_listener = web::bind(web_listen).await;
    let web_addr = web_listener.as_ref().and_then(bound_addr);
    let (content_listener, content_addr) = match web_addr {
        Some(addr) => match content::bind(addr).await {
            Some((listener, bound)) => (Some(listener), Some(bound)),
            None => (None, None),
        },
        None => (None, None),
    };
    let notifier = Notifier::from_config(&config).map(Arc::new);
    let supervisor_args = SupervisorArgs::new(
        home.clone(),
        std::env::var(crate::domain::AgentKind::Codex.binary_env()).ok(),
    )
    .with_notifications(notifier.clone(), machine.name.to_string());
    let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, supervisor_args)
        .await
        .map_err(|err| AppError::Internal {
            message: format!("spawn supervisor: {err}"),
        })?;
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply }).await?;
    let fleet_runtime = start_fleet(&home, &config, &machine, web_addr);
    let fleet = fleet_runtime
        .as_ref()
        .map_or(FleetState::Disabled, |runtime| {
            FleetState::Enabled(runtime.handle())
        });
    let state = AppState {
        home: home.clone(),
        store,
        supervisor: supervisor.clone(),
        web: web_addr,
        content: content_addr,
        stream_slots: StreamSlots::new(),
        machine,
        fleet,
        message_receiver: message_receiver::MessageReceiver::default(),
        locks: DaemonLocks::default(),
        thread_titles: TitleSources::from_env(),
    };
    let sender = tokio::spawn(event_sender::run(
        state.store.clone(),
        state.supervisor.clone(),
        state.machine.identity.machine,
        state.fleet.clone(),
    ));
    let recovery = tokio::spawn(origin_submit::recover(state.clone()));
    let resource_recovery = tokio::spawn(resource_submit::recover(state.clone()));
    let action_recovery = tokio::spawn(resource_action::recover(state.clone()));
    let background_recovery = tokio::spawn(resource_background::recover(state.clone()));
    let cancellation = tokio::spawn(cancel_delivery::run(state.clone()));
    let notice_delivery = tokio::spawn(resource_notice_delivery::run(state.clone()));
    let t3_watcher = tokio::spawn(t3_watch::run(notifier, state.machine.name.to_string()));
    // listeners share one shutdown: the signal task flips the flag once
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        // a send error means both servers already stopped
        let _ = shutdown_tx.send(true);
    });
    let socket_serve = axum::serve(listener, api::socket_router(state.clone()))
        .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone()));
    let web_serve = serve_web(web_listener, state.clone(), shutdown_rx.clone());
    let content_serve = serve_content(content_listener, content_addr, state, shutdown_rx);
    let web_url = web_addr.map(web::url_for);
    info!(
        sock = %sock.display(),
        web = web_url.as_deref().unwrap_or("off"),
        content = content_addr.map(|addr| addr.to_string()).as_deref().unwrap_or("off"),
        "homebased listening"
    );
    let mut handle = handle;
    // a supervisor that ends on its own lost the store or the callback actor;
    // there is no recovery, so stop serving and exit non-zero (DEC-20)
    let supervisor_died = tokio::select! {
        result = async {
            let (socket_result, (), ()) = tokio::join!(socket_serve, web_serve, content_serve);
            socket_result
        } => {
            result.map_err(|err| AppError::Internal {
                message: format!("serve: {err}"),
            })?;
            false
        }
        _ = &mut handle => true,
    };
    if let Some(runtime) = fleet_runtime {
        runtime.shutdown().await;
    }
    sender.abort();
    recovery.abort();
    resource_recovery.abort();
    action_recovery.abort();
    background_recovery.abort();
    cancellation.abort();
    notice_delivery.abort();
    t3_watcher.abort();
    if !supervisor_died {
        supervisor.stop(None);
        if let Err(err) = handle.await {
            warn!("supervisor shutdown: {err}");
        }
    }
    if sock.exists()
        && let Err(err) = std::fs::remove_file(&sock)
    {
        warn!(sock = %sock.display(), "remove socket: {err}");
    }
    if supervisor_died {
        return Err(AppError::Internal {
            message: "daemon supervisor stopped; see the logged actor failure".into(),
        });
    }
    info!("serve stopped");
    Ok(())
}

/// Start fleet discovery in the background when the config enables it. A
/// fleet failure never stops the daemon: local tasks keep working and the
/// cluster routes stay absent
fn start_fleet(
    home: &Home,
    config: &Config,
    machine: &LocalMachine,
    listener: Option<SocketAddr>,
) -> Option<FleetRuntime> {
    let settings = config.fleet_settings()?;
    if listener.is_none() {
        warn!("fleet is enabled but the TCP listener is off; peers cannot reach this machine");
    }
    crate::fleet::runtime::log_start(machine, listener);
    let start = FleetStart {
        local: machine.clone(),
        settings: settings.clone(),
        listener,
        peers_path: home.fleet_peers_path(),
        timings: RuntimeTimings::default(),
    };
    FleetRuntime::start(start)
        .map_err(|err| warn!("fleet disabled for this run: {err}"))
        .ok()
}

/// Address a bound listener is reachable on. A listener with no readable
/// address cannot be advertised, so the dashboard URL stays `null`
fn bound_addr(listener: &TcpListener) -> Option<SocketAddr> {
    match listener.local_addr() {
        Ok(addr) => Some(addr),
        Err(err) => {
            warn!("dashboard local address unavailable: {err}");
            None
        }
    }
}

/// Serve the dashboard until shutdown. Its failure is never the daemon's:
/// the socket API and the supervisor keep running
async fn serve_web(
    listener: Option<TcpListener>,
    state: AppState,
    shutdown: watch::Receiver<bool>,
) {
    let Some(listener) = listener else {
        return;
    };
    let Some(addr) = state.web else {
        return;
    };
    let serve = axum::serve(listener, web::router(state, addr))
        .with_graceful_shutdown(wait_for_shutdown(shutdown));
    if let Err(err) = serve.await {
        warn!("dashboard listener stopped: {err}");
    }
}

/// Serve raw file content on a separate origin. Failure is non-fatal
async fn serve_content(
    listener: Option<TcpListener>,
    addr: Option<SocketAddr>,
    state: AppState,
    shutdown: watch::Receiver<bool>,
) {
    let Some(listener) = listener else {
        return;
    };
    let Some(addr) = addr else {
        return;
    };
    let serve = axum::serve(
        listener,
        content::router(state, addr).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(wait_for_shutdown(shutdown));
    if let Err(err) = serve.await {
        warn!("content listener stopped: {err}");
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    // a closed channel means the signal task is gone, which only happens at
    // process exit; treating it as shutdown is correct either way
    let _ = shutdown.wait_for(|requested| *requested).await;
}

fn acquire_daemon_lock(home: &Home) -> Result<File, AppError> {
    flock_exclusive(&home.daemon_lock_path(), LockMode::NonBlocking).map_err(|err| match err {
        AppError::LockHeld { .. } => AppError::DaemonAlreadyRunning,
        other => other,
    })
}

async fn shutdown_signal() {
    let term = signal(SignalKind::terminate());
    match term {
        Ok(mut sigterm) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        Err(err) => {
            warn!("SIGTERM handler unavailable: {err}");
            // a ctrl_c error leaves no way to wait for shutdown; fall through
            if let Err(err) = tokio::signal::ctrl_c().await {
                warn!("ctrl_c handler unavailable: {err}");
            }
        }
    }
}
