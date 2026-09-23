//! Socket-only internal route for co-located resource release watchers
//!
//! The route is merged only into the Unix-socket router. The dashboard and fleet
//! TCP listeners never serve it, so only local processes with socket access can
//! advance a release action, and only with identities the authority saved

use axum::body::Bytes;
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};

use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, call};
use crate::error::AppError;
use crate::resource::release_watcher::{
    RELEASE_WATCHER_POLL_PATH, RELEASE_WATCHER_PROTOCOL_VERSION, ReleaseWatcherPollOutcome,
    ReleaseWatcherPollRequest, ReleaseWatcherPollResponse,
};

/// Routes served only on the daemon Unix socket
pub(crate) fn socket_routes() -> Router<AppState> {
    Router::new().route(RELEASE_WATCHER_POLL_PATH, post(poll))
}

async fn poll(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<ReleaseWatcherPollResponse>, AppError> {
    let request: ReleaseWatcherPollRequest =
        serde_json::from_slice(&body).map_err(|error| AppError::Usage {
            message: format!("invalid release watcher poll: {error}"),
        })?;
    if request.protocol_version != RELEASE_WATCHER_PROTOCOL_VERSION {
        return Err(AppError::Usage {
            message: format!(
                "unsupported release watcher protocol version {}",
                request.protocol_version
            ),
        });
    }

    let watcher = request.watcher;
    let outcome = call(&state.store, |reply| {
        StoreMsg::PollReleaseWatcherForAuthority {
            authority_machine: state.machine.identity.machine,
            request,
            reply,
        }
    })
    .await?
    .map_err(|error| AppError::Internal {
        message: format!("release watcher poll storage: {error}"),
    })?;
    match &outcome {
        ReleaseWatcherPollOutcome::StopCommitted { generation_id, .. } => tracing::info!(
            resource = %watcher.resource_id.as_uuid(),
            action = %watcher.action_id.as_uuid(),
            trainer = %watcher.trainer_task_id,
            %generation_id,
            "release watcher committed the exact trainer stop"
        ),
        ReleaseWatcherPollOutcome::Attention { reason } => tracing::warn!(
            resource = %watcher.resource_id.as_uuid(),
            action = %watcher.action_id.as_uuid(),
            watcher = %watcher.watcher_task_id.as_task_id(),
            ?reason,
            "release watcher needs attention"
        ),
        ReleaseWatcherPollOutcome::WatcherNotRunning
        | ReleaseWatcherPollOutcome::WaitingForTrainerStart
        | ReleaseWatcherPollOutcome::WaitingForCheckpoint
        | ReleaseWatcherPollOutcome::CompletedResultAwaitingTrainerExit
        | ReleaseWatcherPollOutcome::TrainerCompleted
        | ReleaseWatcherPollOutcome::ReleaseSettled => {}
    }

    Ok(Json(ReleaseWatcherPollResponse {
        protocol_version: RELEASE_WATCHER_PROTOCOL_VERSION,
        outcome,
    }))
}
