//! Token usage of finished agent workers, for `task usage` and the dashboard

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, TimeDelta, Utc};
use serde::Deserialize;

use super::AppState;
use super::actors::{StoreMsg, call};
use crate::domain::ThreadId;
use crate::error::AppError;
use crate::usage::UsageReport;

/// Window of a request that names no start
const DEFAULT_WINDOW: TimeDelta = TimeDelta::days(7);

/// Routes that read usage
pub fn read_routes() -> Router<AppState> {
    Router::new().route("/v1/usage", get(report))
}

#[derive(Deserialize)]
struct UsageQuery {
    /// Count tasks submitted at or after this time
    since: Option<DateTime<Utc>>,
    /// Count only tasks from this thread
    thread: Option<ThreadId>,
}

async fn report(
    State(state): State<AppState>,
    Query(query): Query<UsageQuery>,
) -> Result<Json<UsageReport>, AppError> {
    let since = query.since.unwrap_or_else(|| Utc::now() - DEFAULT_WINDOW);
    let thread = query.thread;
    let tasks = call(&state.store, |reply| StoreMsg::UsageSince {
        since,
        thread,
        reply,
    })
    .await?;
    Ok(Json(UsageReport::build(&state.home, since, tasks)))
}
