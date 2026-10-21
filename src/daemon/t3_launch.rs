//! Launch a T3 Code thread on this or another Fleet machine
//!
//! The CLI posts to `/v1/t3/launch` on the Unix socket. The daemon resolves the
//! machine, then launches in its own T3 or forwards the request to
//! `/v1/cluster/t3/launch` on the peer, whose daemon launches in that
//! machine's T3. Each daemon reaches only its own T3 server, as with message
//! delivery. Project listings take the same two hops
//!
//! The routes need no protocol change: a peer that predates them answers 404,
//! which is reported as an outdated homebased

use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::AppState;
use super::cluster::{ReadQuery, check_api_version, check_protocol};
use super::message_sender::resolve_machine;
use crate::domain::{API_VERSION, ThreadId};
use crate::error::AppError;
use crate::fleet::http::{ClusterClient, ClusterResponse, DEFAULT_MAX_BODY};
use crate::machine::{MachineId, MachineName};
use crate::t3::{
    LaunchError, LaunchSpec, LaunchedThread, T3Env, T3Project, launch_thread, projects,
};

const CLUSTER_LAUNCH_PATH: &str = "/v1/cluster/t3/launch";
const CLUSTER_PROJECTS_PATH: &str = "/v1/cluster/t3/projects";
// the peer issues a token, waits up to 30 seconds for T3, and revokes the token
const LAUNCH_REQUEST_TIMEOUT: Duration = Duration::from_secs(75);
const PROJECTS_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// `POST /v1/t3/launch` body
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T3LaunchRequest {
    /// Public API version
    pub api_version: u32,
    /// Machine name or UUID; this machine when omitted
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    /// Thread that asked for the launch, named in the first message as the
    /// route for replies
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_thread: Option<ThreadId>,
    /// What to launch
    pub launch: LaunchSpec,
}

/// `POST /v1/t3/launch` answer
#[derive(Debug, Serialize, Deserialize)]
pub struct T3LaunchResponse {
    /// Public API version
    pub api_version: u32,
    /// Machine whose T3 runs the thread
    pub machine: MachineId,
    /// Name of that machine
    pub machine_name: MachineName,
    /// The launched thread
    pub thread: LaunchedThread,
}

/// `POST /v1/t3/projects` body
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T3ProjectsRequest {
    /// Public API version
    pub api_version: u32,
    /// Machine name or UUID; this machine when omitted
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
}

/// Projects of one machine's T3, from `/v1/t3/projects` and its cluster route
#[derive(Debug, Serialize, Deserialize)]
pub struct T3ProjectsBody {
    /// Public API version
    pub api_version: u32,
    /// Machine whose T3 has these projects
    pub machine: MachineId,
    /// Projects, by title
    pub projects: Vec<T3Project>,
}

/// Launch request from the origin daemon to the launching peer
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterLaunch {
    api_version: u32,
    protocol_version: u32,
    destination_machine: MachineId,
    origin_machine: MachineId,
    launch: LaunchSpec,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterLaunchBody {
    api_version: u32,
    protocol_version: u32,
    machine: MachineId,
    thread: LaunchedThread,
}

/// Local-only routes; a launch starts an agent, so it stays off the TCP listener
pub fn socket_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/t3/launch", post(launch))
        .route("/v1/t3/projects", post(list_projects))
}

/// Peer routes for an enabled fleet, inside its trusted network boundary
pub fn cluster_routes() -> Router<AppState> {
    Router::new()
        .route(CLUSTER_LAUNCH_PATH, post(cluster_launch))
        .route(CLUSTER_PROJECTS_PATH, get(cluster_projects))
}

async fn launch(
    State(state): State<AppState>,
    Json(body): Json<T3LaunchRequest>,
) -> Result<Json<T3LaunchResponse>, AppError> {
    check_api_version(body.api_version)?;
    body.launch
        .validate()
        .map_err(|message| AppError::Usage { message })?;
    let destination = destination(&state, body.machine.as_deref()).await?;

    let mut launch = body.launch;
    if let (Some(thread), Some(message)) = (body.source_thread, launch.message.as_mut()) {
        message.push_str(&reply_route(&state.machine.name, thread));
        // the route can push a message near the limit over it, which the peer would refuse
        launch
            .validate()
            .map_err(|message| AppError::Usage { message })?;
    }
    let local = state.machine.identity.machine;
    let (thread, machine_name) = if destination == local {
        (
            launch_here(local, launch).await?,
            state.machine.name.clone(),
        )
    } else {
        forward_launch(&state, destination, launch).await?
    };

    Ok(Json(T3LaunchResponse {
        api_version: API_VERSION,
        machine: destination,
        machine_name,
        thread,
    }))
}

async fn cluster_launch(
    State(state): State<AppState>,
    Json(body): Json<ClusterLaunch>,
) -> Result<Json<ClusterLaunchBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.origin_machine)?;
    body.launch
        .validate()
        .map_err(|message| AppError::Usage { message })?;

    let machine = state.machine.identity.machine;
    let thread = launch_here(machine, body.launch).await?;
    Ok(Json(ClusterLaunchBody {
        api_version: API_VERSION,
        protocol_version: body.protocol_version,
        machine,
        thread,
    }))
}

async fn list_projects(
    State(state): State<AppState>,
    Json(body): Json<T3ProjectsRequest>,
) -> Result<Json<T3ProjectsBody>, AppError> {
    check_api_version(body.api_version)?;
    let destination = destination(&state, body.machine.as_deref()).await?;
    let projects = if destination == state.machine.identity.machine {
        projects_here().await?
    } else {
        let fleet = state
            .fleet
            .handle()
            .ok_or_else(|| AppError::MachineNotFound {
                machine: destination.to_string(),
            })?;
        let verified = fleet.connect(destination).await?;
        let path = format!(
            "{CLUSTER_PROJECTS_PATH}?api_version={API_VERSION}&destination_machine={destination}"
        );
        let response = ClusterClient::new(PROJECTS_REQUEST_TIMEOUT, DEFAULT_MAX_BODY)
            .get(&verified.address, &path)
            .await
            .map_err(|error| AppError::MachineUnavailable {
                machine: destination,
                message: error.to_string(),
            })?;
        let body: T3ProjectsBody = peer_body(destination, &response)?;
        body.projects
    };

    Ok(Json(T3ProjectsBody {
        api_version: API_VERSION,
        machine: destination,
        projects,
    }))
}

async fn cluster_projects(
    State(state): State<AppState>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<T3ProjectsBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(query.destination_machine)?;
    check_api_version(query.api_version)?;
    Ok(Json(T3ProjectsBody {
        api_version: API_VERSION,
        machine: state.machine.identity.machine,
        projects: projects_here().await?,
    }))
}

async fn destination(state: &AppState, machine: Option<&str>) -> Result<MachineId, AppError> {
    match machine {
        Some(machine) => resolve_machine(state, machine).await,
        None => Ok(state.machine.identity.machine),
    }
}

/// Launch in this machine's T3, off the async runtime since the client blocks
async fn launch_here(machine: MachineId, launch: LaunchSpec) -> Result<LaunchedThread, AppError> {
    let request = launch.request_id;
    tokio::task::spawn_blocking(move || launch_thread(&T3Env::from_env(), &launch))
        .await
        .map_err(|error| AppError::Internal {
            message: format!("T3 launch worker failed: {error}"),
        })?
        .map_err(|error| {
            t3_error(error, |message| AppError::T3LaunchOutcomeUnknown {
                request,
                machine: Some(machine),
                message,
            })
        })
}

async fn projects_here() -> Result<Vec<T3Project>, AppError> {
    tokio::task::spawn_blocking(|| projects(&T3Env::from_env()))
        .await
        .map_err(|error| AppError::Internal {
            message: format!("T3 project worker failed: {error}"),
        })?
        // reading projects sends nothing to T3, so its outcome is never unknown
        .map_err(|error| t3_error(error, |message| AppError::T3Unavailable { message }))
}

async fn forward_launch(
    state: &AppState,
    destination: MachineId,
    launch: LaunchSpec,
) -> Result<(LaunchedThread, MachineName), AppError> {
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| AppError::MachineNotFound {
            machine: destination.to_string(),
        })?;
    let verified = fleet.connect(destination).await?;
    let request = launch.request_id;
    let wire = ClusterLaunch {
        api_version: API_VERSION,
        protocol_version: verified.protocol.0,
        destination_machine: destination,
        origin_machine: state.machine.identity.machine,
        launch,
    };

    // T3 launches once per request id, so any transport failure is safe to retry
    let response = ClusterClient::new(LAUNCH_REQUEST_TIMEOUT, DEFAULT_MAX_BODY)
        .post_json(&verified.address, CLUSTER_LAUNCH_PATH, &wire)
        .await
        .map_err(|error| AppError::T3LaunchOutcomeUnknown {
            request,
            machine: Some(destination),
            message: error.to_string(),
        })?;
    // after a success status the launch happened, so an answer that cannot be
    // read still leaves a thread that only a retry with the same id can name
    let unknown = |message: String| AppError::T3LaunchOutcomeUnknown {
        request,
        machine: Some(destination),
        message,
    };
    let body: ClusterLaunchBody = match peer_body(destination, &response) {
        Ok(body) => body,
        Err(error @ AppError::MachineIdentityMismatch { .. }) => return Err(error),
        Err(error) if response.status.is_success() => return Err(unknown(error.to_string())),
        Err(error) => return Err(error),
    };
    if body.protocol_version != wire.protocol_version {
        return Err(unknown(
            "peer answered with another cluster protocol version".into(),
        ));
    }
    Ok((body.thread, verified.advertisement.name))
}

/// A peer's versioned answer for `machine`, or the error it returned
fn peer_body<T: DeserializeOwned + PeerBody>(
    machine: MachineId,
    response: &ClusterResponse,
) -> Result<T, AppError> {
    if !response.status.is_success() {
        // a peer without these routes answers with the web listener's `not_found`
        let route_missing = response.status == StatusCode::NOT_FOUND
            && serde_json::from_slice::<serde_json::Value>(&response.body).is_ok_and(|body| {
                body.pointer("/error/code").and_then(|code| code.as_str()) == Some("not_found")
            });
        if route_missing {
            return Err(AppError::T3Incompatible {
                message: format!(
                    "machine {machine} runs a homebased without T3 launch; run `homebased update` there"
                ),
            });
        }
        return Err(crate::client::map_error(response.status, &response.body));
    }

    let body: T = serde_json::from_slice(&response.body).map_err(|error| AppError::Internal {
        message: format!("machine {machine} answered with an invalid T3 body: {error}"),
    })?;
    if body.api_version() != API_VERSION {
        return Err(AppError::Internal {
            message: format!("machine {machine} answered with another API version"),
        });
    }
    if body.machine() != machine {
        return Err(AppError::MachineIdentityMismatch {
            expected: machine,
            found: Some(body.machine()),
        });
    }
    Ok(body)
}

/// Version and machine of a peer answer, checked before it is used
trait PeerBody {
    fn api_version(&self) -> u32;
    fn machine(&self) -> MachineId;
}

impl PeerBody for ClusterLaunchBody {
    fn api_version(&self) -> u32 {
        self.api_version
    }

    fn machine(&self) -> MachineId {
        self.machine
    }
}

impl PeerBody for T3ProjectsBody {
    fn api_version(&self) -> u32 {
        self.api_version
    }

    fn machine(&self) -> MachineId {
        self.machine
    }
}

/// The daemon error for a T3 failure; `uncertain` builds the one for a lost reply
fn t3_error(error: LaunchError, uncertain: impl FnOnce(String) -> AppError) -> AppError {
    match error {
        LaunchError::ProjectNotFound { selector, projects } => {
            AppError::T3ProjectNotFound { selector, projects }
        }
        LaunchError::Refused(message) => AppError::T3LaunchRefused { message },
        LaunchError::Unavailable(message) => AppError::T3Unavailable { message },
        LaunchError::Changed(message) => AppError::T3Incompatible {
            message: format!("{message}; run `homebased t3 check` on that machine"),
        },
        LaunchError::Uncertain(message) => uncertain(message),
    }
}

/// Footer that tells the launched agent how to answer the thread that asked
fn reply_route(machine: &MachineName, thread: ThreadId) -> String {
    format!(
        "\n\n---\nStarted through homebased by thread `{thread}` on machine `{machine}`. To report back, run:\n`homebased message send --machine {machine} --thread {thread} --message \"<reply>\"`"
    )
}
