//! Lock watch and Lost handling. Blocking `flock`; do not poll.

use tracing::{info, warn};

use crate::callback::{deliver_exit_event, exit_event};
use crate::daemon::{lock_store, AppState};
use crate::domain::{status_from_exit, ExitReason, ProcessStatus, TaskId};
use crate::error::AppError;
use crate::home;
use crate::store::{self, Store};

/// Reconcile every non-terminal task at daemon start.
pub async fn reconcile_all(state: &AppState) -> Result<(), AppError> {
    let tasks = {
        let store = lock_store(&state.store);
        store.non_terminal()?
    };
    for row in tasks {
        let watch_state = state.clone();
        let id = row.id;
        tokio::spawn(async move {
            if let Err(err) = watch_lock(&watch_state, id).await {
                warn!(%id, "reconcile: {err}");
            }
        });
    }
    Ok(())
}

/// Blocking flock on a blocking thread; then apply `exit.json` or mark Lost.
pub async fn watch_lock(state: &AppState, id: TaskId) -> Result<(), AppError> {
    let paths = state.home.task_paths(id);
    let lock_path = paths.runner_lock.clone();
    let _held = tokio::task::spawn_blocking(move || home::flock_exclusive_blocking(&lock_path))
        .await
        .map_err(|err| AppError::Internal {
            message: format!("join flock watch: {err}"),
        })??;
    apply_after_lock(state, id)
}

fn apply_after_lock(state: &AppState, id: TaskId) -> Result<(), AppError> {
    let store = lock_store(&state.store);
    let Some(row) = store.get_task(id)? else {
        return Ok(());
    };
    if row.status.is_terminal()
        && row.callback_status != crate::domain::CallbackStatus::Pending
        && row.callback_status != crate::domain::CallbackStatus::Sending
    {
        return Ok(());
    }
    let paths = state.home.task_paths(id);
    match store::read_exit_json(&paths.exit_json)? {
        Some(exit) => apply_exit(&store, state, id, &exit.reason)?,
        None => {
            if !row.status.is_terminal() {
                let from = row.status;
                if from == ProcessStatus::Queued {
                    let _ = store.cas_status(id, ProcessStatus::Queued, ProcessStatus::Lost)?;
                } else if from == ProcessStatus::Running {
                    let _ = store.cas_status(id, ProcessStatus::Running, ProcessStatus::Lost)?;
                }
            }
            let row = store.require_task(id)?;
            if row.callback_status == crate::domain::CallbackStatus::Sent
                || row.callback_status == crate::domain::CallbackStatus::Failed
            {
                return Ok(());
            }
            info!(%id, "runner lost");
            let reports = store.reports(id)?;
            let event = exit_event(&row, &reports, paths.dir, true);
            deliver_exit_event(&store, &state.home, &row, &event)?;
        }
    }
    Ok(())
}

fn apply_exit(
    store: &Store,
    state: &AppState,
    id: TaskId,
    reason: &ExitReason,
) -> Result<(), AppError> {
    let row = store.require_task(id)?;
    if !row.status.is_terminal() {
        let to = status_from_exit(reason);
        let _ = store.cas_exit(id, row.status, to, reason)?;
    }
    let row = store.require_task(id)?;
    if matches!(
        row.callback_status,
        crate::domain::CallbackStatus::Pending | crate::domain::CallbackStatus::Sending
    ) {
        let reports = store.reports(id)?;
        let event = exit_event(&row, &reports, state.home.task_dir(id), false);
        deliver_exit_event(store, &state.home, &row, &event)?;
    }
    Ok(())
}
