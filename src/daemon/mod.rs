//! Serve: daemon lock, socket, actors, shutdown.

pub mod actors;
pub mod api;

use std::fs::File;

use ractor::{Actor, ActorRef};
use tokio::net::UnixListener;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};

use crate::callback::exit_event;
use crate::daemon::actors::{
    call_store, flatten_call, CallbackMsg, StoreMsg, SupervisorActor, SupervisorMsg, CALL_TIMEOUT,
};
use crate::domain::{ExitReason, ProcessStatus, TaskId};
use crate::error::AppError;
use crate::home::{chmod_600, flock_exclusive, Home};

/// Axum state: actor refs plus immutable path config.
#[derive(Clone)]
pub struct AppState {
    /// State directory (immutable layout).
    pub home: Home,
    /// Store actor.
    pub store: ActorRef<StoreMsg>,
    /// Callback actor.
    pub callback: ActorRef<CallbackMsg>,
    /// Supervisor.
    pub supervisor: ActorRef<SupervisorMsg>,
}

/// Hold `daemon.lock`, bind the socket, start actors, serve until SIGTERM.
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
    let (supervisor, handle) = SupervisorActor::spawn(None, SupervisorActor, home.clone())
        .await
        .map_err(|err| AppError::Internal {
            message: format!("spawn supervisor: {err}"),
        })?;
    let refs = flatten_call(
        supervisor
            .call(|reply| SupervisorMsg::GetRefs { reply }, Some(CALL_TIMEOUT))
            .await,
    )?;
    let state = AppState {
        home: home.clone(),
        store: refs.store,
        callback: refs.callback,
        supervisor: supervisor.clone(),
    };
    let app = api::router(state);
    info!(sock = %sock.display(), "homebased listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|err| AppError::Internal {
            message: format!("serve: {err}"),
        })?;
    supervisor.stop(None);
    let _ = handle.await;
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

/// Spawn a worker after a successful insert, then ask the supervisor to watch.
pub async fn spawn_and_watch(state: &AppState, id: TaskId) -> Result<(), AppError> {
    let paths = state.home.task_paths(id);
    let lock = crate::runner::lock_before_spawn(&paths)?;
    match crate::runner::spawn_task_run(&state.home, id, lock) {
        Ok(pid) => {
            call_store(&state.store, |reply| StoreMsg::SetPid {
                id,
                pid: pid as i32,
                reply,
            })
            .await?;
            flatten_call(
                state
                    .supervisor
                    .call(
                        |reply| SupervisorMsg::SpawnTask { id, reply },
                        Some(CALL_TIMEOUT),
                    )
                    .await,
            )?;
            Ok(())
        }
        Err(err) => {
            let reason = ExitReason::SpawnFailed {
                message: err.to_string(),
            };
            let _ = call_store(&state.store, |reply| StoreMsg::CasExit {
                id,
                from: ProcessStatus::Queued,
                to: ProcessStatus::Failed,
                reason: reason.clone(),
                reply,
            })
            .await?;
            let row = call_store(&state.store, |reply| StoreMsg::GetTask { id, reply })
                .await?
                .ok_or(AppError::TaskNotFound { id })?;
            let reports = call_store(&state.store, |reply| StoreMsg::Reports { id, reply }).await?;
            let event = exit_event(&row, &reports, paths.dir);
            let _ = state.callback.cast(CallbackMsg::Deliver { row, event });
            Err(err)
        }
    }
}
