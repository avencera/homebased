//! Serve: daemon lock, socket, reconcile, shutdown.

pub mod api;
pub mod reconcile;

use std::fs::File;
use std::sync::{Arc, Mutex};

use tokio::net::UnixListener;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};

use crate::domain::TaskId;
use crate::error::AppError;
use crate::home::{chmod_600, flock_exclusive, Home};
use crate::store::Store;

/// Shared daemon state.
#[derive(Clone)]
pub struct AppState {
    /// State directory.
    pub home: Home,
    /// SQLite store.
    pub store: Arc<Mutex<Store>>,
}

/// Hold `daemon.lock`, bind the socket, reconcile, serve until SIGTERM.
pub async fn serve(home: Home) -> Result<(), AppError> {
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
    let store = Store::open(&home.db_path())?;
    let state = AppState {
        home: home.clone(),
        store: Arc::new(Mutex::new(store)),
    };
    reconcile::reconcile_all(&state).await?;
    let app = api::router(state.clone());
    info!(sock = %sock.display(), "homebased listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|err| AppError::Internal {
            message: format!("serve: {err}"),
        })?;
    if sock.exists() {
        let _ = std::fs::remove_file(&sock);
    }
    info!("serve stopped");
    Ok(())
}

fn acquire_daemon_lock(home: &Home) -> Result<File, AppError> {
    flock_exclusive(&home.daemon_lock_path(), true)
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
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

pub(crate) fn lock_store(store: &Mutex<Store>) -> std::sync::MutexGuard<'_, Store> {
    store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Spawn a worker after a successful insert.
pub fn spawn_and_watch(state: &AppState, id: TaskId) -> Result<(), AppError> {
    let paths = state.home.task_paths(id);
    let lock = crate::runner::lock_before_spawn(&paths)?;
    match crate::runner::spawn_task_run(&state.home, id, lock) {
        Ok(pid) => {
            {
                let store = lock_store(&state.store);
                store.set_pid(id, pid as i32)?;
            }
            let watch_state = state.clone();
            tokio::spawn(async move {
                if let Err(err) = reconcile::watch_lock(&watch_state, id).await {
                    warn!(%id, "watch: {err}");
                }
            });
            Ok(())
        }
        Err(err) => {
            let store = lock_store(&state.store);
            let reason = crate::domain::ExitReason::SpawnFailed {
                message: err.to_string(),
            };
            let _ = store.cas_exit(
                id,
                crate::domain::ProcessStatus::Queued,
                crate::domain::ProcessStatus::Failed,
                &reason,
            )?;
            let row = store.require_task(id)?;
            let reports = store.reports(id)?;
            let event = crate::callback::exit_event(&row, &reports, paths.dir, false);
            crate::callback::deliver_exit_event(&store, &state.home, &row, &event)?;
            Err(err)
        }
    }
}
