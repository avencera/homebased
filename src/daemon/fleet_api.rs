//! Local fleet control and read-only machine inventory.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, call};
use crate::daemon::cluster::{ExecutionBody, ExecutionView};
use crate::domain::{API_VERSION, TaskId};
use crate::error::AppError;
use crate::events::{EventRouteStatus, FailedInboxEvent};
use crate::fleet::address::MachineAddress;
use crate::fleet::advertisement::ProbedMachine;
use crate::fleet::directory::{PeerView, UnresolvedAddress};
use crate::fleet::http::ClusterClient;
use crate::fleet::runtime::{LocalIdentityStatus, RoundReport};
use crate::machine::{MachineId, MachineName};
use crate::message::{MessageSendRequest, MessageSendResponse};

/// The local machine fields shown with the fleet inventory.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalView {
    /// Stable installation UUID.
    pub machine: MachineId,
    /// Display name.
    pub name: MachineName,
    /// Advertised listener addresses.
    pub advertised: Vec<MachineAddress>,
    /// Whether another daemon claims this installation UUID.
    pub identity: LocalIdentityStatus,
}

/// Fleet inventory, including last-known offline peers.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachinesBody {
    /// Public API version.
    pub api_version: u32,
    /// This daemon's identity.
    pub local: LocalView,
    /// Known remote machines.
    pub machines: Vec<PeerView>,
    /// Addresses not yet bound to a machine UUID.
    pub unresolved: Vec<UnresolvedAddress>,
}

/// One discovery round and its resulting inventory.
#[derive(Debug, Serialize, Deserialize)]
pub struct DiscoverBody {
    /// Probe round result.
    pub round: RoundReport,
    /// Current inventory.
    #[serde(flatten)]
    pub inventory: MachinesBody,
}

/// Strict address mutation or probe request.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AddressBody {
    /// Public API version.
    pub api_version: u32,
    /// Base HTTP URL.
    pub address: MachineAddress,
}

/// Strict remove request. A known UUID or an explicit address is accepted.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveBody {
    /// Public API version.
    pub api_version: u32,
    /// Machine UUID or base HTTP URL.
    pub machine_or_address: String,
}

/// Result of a fleet control operation.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeBody {
    /// Public API version.
    pub api_version: u32,
    /// Whether the directory changed.
    pub changed: bool,
}

/// Probe result, including incompatible peers by identity.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeBody {
    /// Public API version.
    pub api_version: u32,
    /// Probe result.
    pub machine: ProbedMachineView,
}

/// Probe fields that are present for both compatible and incompatible peers.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbedMachineView {
    /// Stable UUID.
    pub machine: MachineId,
    /// Name given by the answering daemon.
    pub name: MachineName,
    /// Whether this daemon supports its cluster protocol.
    pub compatible: bool,
}

/// Strict versioned request without operation fields.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionBody {
    /// Public API version.
    pub api_version: u32,
}

/// Public machine inventory route for socket and TCP.
pub fn read_routes() -> Router<AppState> {
    Router::new().route("/v1/fleet/machines", get(machines))
}

/// Local-only control routes.
pub fn socket_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/messages/send", post(send_message))
        .route("/v1/fleet/tasks/{id}", get(lookup_task))
        .route("/v1/fleet/origin/tasks/{id}", get(origin_inbox_detail))
        .route(
            "/v1/fleet/executor/tasks/{id}/events",
            get(executor_event_detail),
        )
        .route("/v1/fleet/discover", post(discover))
        .route("/v1/fleet/probe", post(probe))
        .route("/v1/fleet/add", post(add))
        .route("/v1/fleet/remove", post(remove))
}

async fn send_message(
    State(state): State<AppState>,
    Json(body): Json<MessageSendRequest>,
) -> Result<Json<MessageSendResponse>, AppError> {
    super::message_sender::send(&state, body).await.map(Json)
}

async fn executor_event_detail(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<EventRouteStatus>, AppError> {
    let status = call(&state.store, |reply| StoreMsg::OutboundRouteStatus {
        id,
        reply,
    })
    .await?
    .ok_or(AppError::TaskNotFound { id })?;
    Ok(Json(status))
}

/// Origin-owned event delivery detail, separate from executor process state
#[derive(Debug, Serialize)]
pub struct OriginInboxDetail {
    /// Public API version
    pub api_version: u32,
    /// Global task UUID
    pub task: TaskId,
    /// Highest event sequence accepted at this origin
    pub last_accepted_seq: u64,
    /// Highest contiguous settled event sequence
    pub last_settled_seq: u64,
    /// Retained callback delivery failures
    pub failed_events: Vec<FailedInboxEvent>,
}

async fn origin_inbox_detail(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<OriginInboxDetail>, AppError> {
    let route = call(&state.store, |reply| StoreMsg::OriginRoute { id, reply })
        .await?
        .ok_or(AppError::RouteNotFound { task: id })?;
    let failed_events = call(&state.store, |reply| StoreMsg::FailedInboxEvents {
        id,
        reply,
    })
    .await?;
    Ok(Json(OriginInboxDetail {
        api_version: API_VERSION,
        task: id,
        last_accepted_seq: route.last_accepted_seq,
        last_settled_seq: route.last_settled_seq,
        failed_events,
    }))
}

async fn machines(State(state): State<AppState>) -> Json<MachinesBody> {
    Json(inventory(&state).await)
}

async fn inventory(state: &AppState) -> MachinesBody {
    let (advertised, identity, machines, unresolved) = match state.fleet.handle() {
        Some(fleet) => (
            fleet.advertisement().addresses.clone(),
            fleet.local_identity_status().await,
            fleet.peers().await,
            fleet.unresolved().await,
        ),
        None => (
            Vec::new(),
            LocalIdentityStatus::Consistent,
            Vec::new(),
            Vec::new(),
        ),
    };
    MachinesBody {
        api_version: API_VERSION,
        local: LocalView {
            machine: state.machine.identity.machine,
            name: state.machine.name.clone(),
            advertised,
            identity,
        },
        machines,
        unresolved,
    }
}

fn enabled(state: &AppState) -> Result<&crate::fleet::runtime::FleetHandle, AppError> {
    state.fleet.handle().ok_or_else(|| AppError::Usage {
        message: "fleet is disabled".into(),
    })
}

async fn discover(
    State(state): State<AppState>,
    Json(body): Json<VersionBody>,
) -> Result<Json<DiscoverBody>, AppError> {
    check_version(body.api_version)?;
    let round = enabled(&state)?.discover_now().await;
    Ok(Json(DiscoverBody {
        round,
        inventory: inventory(&state).await,
    }))
}

async fn probe(
    State(state): State<AppState>,
    Json(body): Json<AddressBody>,
) -> Result<Json<ProbeBody>, AppError> {
    check_version(body.api_version)?;
    let result = enabled(&state)?
        .probe_address(&body.address)
        .await
        .map_err(|err| AppError::Usage {
            message: err.to_string(),
        })?;
    let header = result.header();
    Ok(Json(ProbeBody {
        api_version: API_VERSION,
        machine: ProbedMachineView {
            machine: header.machine,
            name: header.name,
            compatible: matches!(result, ProbedMachine::Compatible { .. }),
        },
    }))
}

async fn add(
    State(state): State<AppState>,
    Json(body): Json<AddressBody>,
) -> Result<Json<ChangeBody>, AppError> {
    check_version(body.api_version)?;
    let changed = enabled(&state)?.add_explicit(body.address).await;
    Ok(Json(ChangeBody {
        api_version: API_VERSION,
        changed,
    }))
}

async fn remove(
    State(state): State<AppState>,
    Json(body): Json<RemoveBody>,
) -> Result<Json<ChangeBody>, AppError> {
    check_version(body.api_version)?;
    let fleet = enabled(&state)?;
    let changed = if let Ok(address) = body.machine_or_address.parse::<MachineAddress>() {
        fleet.remove_explicit(&address).await
    } else if let Ok(machine) = body.machine_or_address.parse::<MachineId>() {
        fleet.forget_machine(machine).await
    } else {
        return Err(AppError::Usage {
            message: "expected a machine UUID or HTTP address".into(),
        });
    };
    Ok(Json(ChangeBody {
        api_version: API_VERSION,
        changed,
    }))
}

fn check_version(version: u32) -> Result<(), AppError> {
    if version == API_VERSION {
        Ok(())
    } else {
        Err(AppError::Usage {
            message: "unsupported API version".into(),
        })
    }
}

/// Why one known machine could not answer a lookup.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LookupGapReason {
    /// No verified route could be established.
    Unavailable,
    /// The peer speaks no supported cluster protocol.
    Incompatible,
    /// A response did not match the strict read schema.
    InvalidResponse,
}

/// One machine that could not be checked.
#[derive(Debug, Serialize)]
pub struct LookupGap {
    /// Stable machine UUID.
    pub machine: MachineId,
    /// Why its records are unknown.
    pub reason: LookupGapReason,
}

/// Fleet-wide task lookup without inferred process state.
#[derive(Debug, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum LookupResult {
    /// One execution record was found.
    Found {
        execution: ExecutionView,
        unchecked: Vec<LookupGap>,
    },
    /// Every known machine answered with no record.
    NotFound,
    /// No record was found, but at least one machine did not answer.
    Incomplete { unchecked: Vec<LookupGap> },
    /// More than one machine claims the same execution UUID.
    OwnershipConflict {
        executions: Vec<ExecutionView>,
        unchecked: Vec<LookupGap>,
    },
}

/// Versioned lookup response.
#[derive(Debug, Serialize)]
pub struct LookupBody {
    /// Public API version.
    pub api_version: u32,
    /// Task UUID requested.
    pub task: TaskId,
    /// Exact lookup outcome.
    #[serde(flatten)]
    pub outcome: LookupResult,
}

async fn lookup_task(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<LookupBody>, AppError> {
    let local = call(&state.store, |reply| StoreMsg::GetTask { id, reply }).await?;
    if let Some(row) = local {
        return Ok(Json(LookupBody {
            api_version: API_VERSION,
            task: id,
            outcome: LookupResult::Found {
                execution: ExecutionView {
                    task: id,
                    execution_machine: state.machine.identity.machine,
                    status: row.status(),
                },
                unchecked: Vec::new(),
            },
        }));
    }
    let Some(fleet) = state.fleet.handle() else {
        return Ok(Json(LookupBody {
            api_version: API_VERSION,
            task: id,
            outcome: LookupResult::NotFound,
        }));
    };
    let mut jobs = JoinSet::new();
    for peer in fleet.peers().await {
        let fleet = fleet.clone();
        jobs.spawn(async move { lookup_peer(fleet, peer.machine, id).await });
    }
    let mut found = Vec::new();
    let mut unchecked = Vec::new();
    while let Some(result) = jobs.join_next().await {
        match result {
            Ok(Ok(Some(execution))) => found.push(execution),
            Ok(Ok(None)) => {}
            Ok(Err(gap)) => unchecked.push(gap),
            Err(err) => {
                return Err(AppError::Internal {
                    message: format!("fleet lookup worker failed: {err}"),
                });
            }
        }
    }
    let outcome = match found.len() {
        0 if unchecked.is_empty() => LookupResult::NotFound,
        0 => LookupResult::Incomplete { unchecked },
        1 => LookupResult::Found {
            execution: found.remove(0),
            unchecked,
        },
        _ => LookupResult::OwnershipConflict {
            executions: found,
            unchecked,
        },
    };
    Ok(Json(LookupBody {
        api_version: API_VERSION,
        task: id,
        outcome,
    }))
}

async fn lookup_peer(
    fleet: crate::fleet::runtime::FleetHandle,
    machine: MachineId,
    id: TaskId,
) -> Result<Option<ExecutionView>, LookupGap> {
    let destination = fleet.connect(machine).await.map_err(|err| LookupGap {
        machine,
        reason: if matches!(err, AppError::ClusterProtocolIncompatible { .. }) {
            LookupGapReason::Incompatible
        } else {
            LookupGapReason::Unavailable
        },
    })?;
    let path =
        format!("/v1/cluster/tasks/{id}?api_version={API_VERSION}&destination_machine={machine}");
    let response = ClusterClient::default()
        .get(&destination.address, &path)
        .await
        .map_err(|_| LookupGap {
            machine,
            reason: LookupGapReason::Unavailable,
        })?;
    if response.status != StatusCode::OK && response.status != StatusCode::NOT_FOUND {
        return Err(LookupGap {
            machine,
            reason: LookupGapReason::InvalidResponse,
        });
    }
    let body: ExecutionBody = serde_json::from_slice(&response.body).map_err(|_| LookupGap {
        machine,
        reason: LookupGapReason::InvalidResponse,
    })?;
    if body.api_version != API_VERSION
        || body
            .execution
            .as_ref()
            .is_some_and(|view| view.task != id || view.execution_machine != machine)
        || (response.status == StatusCode::OK) != body.execution.is_some()
    {
        return Err(LookupGap {
            machine,
            reason: LookupGapReason::InvalidResponse,
        });
    }
    Ok(body.execution)
}
