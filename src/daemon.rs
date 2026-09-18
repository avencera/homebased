//! Serve: daemon lock, socket, actors, shutdown.

pub mod actors;
pub mod api;
pub mod web;

use std::fs::File;
use std::net::SocketAddr;

use ractor::{Actor, ActorRef};
use tokio::net::{TcpListener, UnixListener};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::daemon::actors::{StoreMsg, SupervisorActor, SupervisorMsg, call};
use crate::daemon::web::WebListen;
use crate::error::AppError;
use crate::home::{Home, LockMode, chmod_600, flock_exclusive};

/// Axum state: actor refs plus immutable path config.
#[derive(Clone)]
pub struct AppState {
    /// State directory (immutable layout).
    pub home: Home,
    /// Store actor, for reads. Every write goes through the supervisor.
    pub store: ActorRef<StoreMsg>,
    /// Supervisor: owns the task lifecycle.
    pub supervisor: ActorRef<SupervisorMsg>,
    /// Address the dashboard listener bound, or `None` when it is off or the
    /// bind failed.
    pub web: Option<SocketAddr>,
}

/// Hold `daemon.lock`, bind the socket and the dashboard port, start actors,
/// serve until SIGTERM.
pub async fn serve(home: Home, web_listen: WebListen) -> Result<(), AppError> {
    home.ensure()?;
    let _daemon_lock = acquire_daemon_lock(&home)?;
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
    let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
        .await
        .map_err(|err| AppError::Internal {
            message: format!("spawn supervisor: {err}"),
        })?;
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply }).await?;
    let state = AppState {
        home: home.clone(),
        store,
        supervisor: supervisor.clone(),
        web: web_addr,
    };
    // both listeners share one shutdown: the signal task flips the flag once
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        // a send error means both servers already stopped
        let _ = shutdown_tx.send(true);
    });
    let socket_serve = axum::serve(listener, api::socket_router(state.clone()))
        .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone()));
    let web_serve = serve_web(web_listener, state, shutdown_rx);
    let web_url = web_addr.map(web::url_for);
    info!(
        sock = %sock.display(),
        web = web_url.as_deref().unwrap_or("off"),
        "homebased listening"
    );
    let mut handle = handle;
    // a supervisor that ends on its own lost the store or the callback actor;
    // there is no recovery, so stop serving and exit non-zero (DEC-20)
    let supervisor_died = tokio::select! {
        result = async {
            let (socket_result, ()) = tokio::join!(socket_serve, web_serve);
            socket_result
        } => {
            result.map_err(|err| AppError::Internal {
                message: format!("serve: {err}"),
            })?;
            false
        }
        _ = &mut handle => true,
    };
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

/// Address a bound listener is reachable on. A listener with no readable
/// address cannot be advertised, so the dashboard URL stays `null`.
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
/// the socket API and the supervisor keep running.
async fn serve_web(
    listener: Option<TcpListener>,
    state: AppState,
    shutdown: watch::Receiver<bool>,
) {
    let Some(listener) = listener else {
        return;
    };
    let serve = axum::serve(listener, web::router(state))
        .with_graceful_shutdown(wait_for_shutdown(shutdown));
    if let Err(err) = serve.await {
        warn!("dashboard listener stopped: {err}");
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
