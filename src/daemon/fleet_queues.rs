//! Fleet-wide GPU queue view for the dashboard
//!
//! Every machine runs its own queue, so without this view each one needs its
//! own dashboard tab. Peers answer through the existing queue transport
//!
//! Answers pass through as JSON, as the per-machine queue reads do, so a peer
//! on a newer release with extra job fields still shows during a rollout

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use serde_json::Value;

use super::AppState;
use super::fleet_view::{FleetMachine, read_fleet};
use super::queue_api::forward;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::machine::MachineId;
use crate::store::queue::interface::QueueRequest;

/// `GET /v1/fleet/queues`
#[derive(Debug, Serialize)]
pub struct FleetQueues {
    /// Public API version
    pub api_version: u32,
    /// This machine first, then every known peer and how its read went
    pub machines: Vec<FleetMachine>,
    /// One [`QueueOverview`](crate::store::queue::interface::QueueOverview) per machine that answered, in `machines` order
    pub queues: Vec<Value>,
}

/// Public read route, safe on the TCP listener
pub fn read_routes() -> Router<AppState> {
    Router::new().route("/v1/fleet/queues", get(fleet_queues))
}

async fn fleet_queues(State(state): State<AppState>) -> Result<Json<FleetQueues>, AppError> {
    let local = state.machine.identity.machine;
    let local_queue = overview(&state, local).await?;
    let read = read_fleet(&state, local_queue, |_, machine| {
        let state = state.clone();
        async move {
            overview(&state, machine)
                .await
                .map_err(|error| error.to_string())
        }
    })
    .await;

    Ok(Json(FleetQueues {
        api_version: API_VERSION,
        machines: read.machines,
        queues: read.answers.into_iter().map(|(_, queue)| queue).collect(),
    }))
}

/// One machine's [`QueueOverview`](crate::store::queue::interface::QueueOverview), checked to come from that machine
async fn overview(state: &AppState, machine: MachineId) -> Result<Value, AppError> {
    let mut value = forward(state, machine, QueueRequest::Overview).await?;
    if value.get("machine").and_then(Value::as_str) != Some(machine.to_string().as_str()) {
        return Err(AppError::MachineUnavailable {
            machine,
            message: "peer answered for another machine".into(),
        });
    }
    if let Some(object) = value.as_object_mut() {
        object.remove("api_version");
    }
    Ok(value)
}
