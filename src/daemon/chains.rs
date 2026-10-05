//! Cancelling the work of a chain of agent runs
//!
//! `task cancel` on any run of a chain cancels the run that owns the work
//! now, whatever its state: a held continuation, one being launched, a
//! running run, or one that parked again meanwhile. Every run of a chain is
//! local, so this daemon resolves and cancels all of it

use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::cancel_delivery::CancelResponse;
use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::store::CancelResult;
use crate::waiting::MAX_CHAIN_RUNS;

/// Cancel the current run of `id`'s chain, or `None` when `id` is in no
/// chain or its chain already ended
///
/// The current run can move on while this runs: a release can launch the
/// held continuation, and a running run can park again. Each step re-reads the
/// chain, and a chain has at most [`MAX_CHAIN_RUNS`] runs, so the loop ends
pub(super) async fn cancel(
    state: &AppState,
    id: TaskId,
) -> Result<Option<CancelResponse>, AppError> {
    for _ in 0..=MAX_CHAIN_RUNS {
        let Some(current) = current_run(state, id).await? else {
            return Ok(None);
        };
        // a held continuation is cancelled before launch; a release in
        // progress holds the route's request lock, so this waits for it
        if let Some(response) = super::dependencies::cancel(state, current).await? {
            return Ok(Some(response));
        }
        let Some(result) = cancel_launched(state, current).await? else {
            continue;
        };
        let CancelResult::AlreadyTerminal(row) = result else {
            return Ok(Some(response(current, &result)));
        };
        // a run parks in the transaction that ends it, so an ended run that
        // is still current ended the work; its chain ends with its event
        if current_run(state, id).await? == Some(current) {
            return Ok(Some(CancelResponse::status(current, row.status())));
        }
    }
    Err(AppError::Internal {
        message: format!("the chain of task {id} kept moving while it was cancelled"),
    })
}

async fn current_run(state: &AppState, id: TaskId) -> Result<Option<TaskId>, AppError> {
    call(&state.store, |reply| StoreMsg::ChainCurrentRun {
        id,
        reply,
    })
    .await
}

/// Cancel a launched run, or `None` when it has no row yet, so the caller
/// reads the chain again
async fn cancel_launched(state: &AppState, id: TaskId) -> Result<Option<CancelResult>, AppError> {
    if call(&state.store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .is_none()
    {
        return Ok(None);
    }
    call(&state.supervisor, |reply| SupervisorMsg::Cancel {
        id,
        reply,
    })
    .await
    .map(Some)
}

fn response(id: TaskId, result: &CancelResult) -> CancelResponse {
    match result {
        CancelResult::CancelledQueued(_) => CancelResponse::status(id, ProcessStatus::Cancelled),
        CancelResult::AlreadyTerminal(row) | CancelResult::SignalWorker(row) => {
            CancelResponse::status(id, row.status())
        }
    }
}
