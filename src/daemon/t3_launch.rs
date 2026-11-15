//! Launch a T3 Code thread on this or another Fleet machine
//!
//! The CLI posts to `/v1/t3/launch` on the Unix socket. The daemon resolves the
//! machine, then launches in its own T3 or forwards the request to
//! `/v1/cluster/t3/launch` on the peer, whose daemon launches in that
//! machine's T3. Each daemon reaches only its own T3 server, as with message
//! delivery. Project listings and project adds take the same two hops
//!
//! A child launch (`--child`) saves the link between the calling T3 thread
//! and the new one, see [`crate::child_thread`]. The child's machine saves it
//! before T3 starts the child's first run, and the parent's machine once the
//! launch succeeded; T3 fixes the new thread's id by the request id, so both
//! know it in advance
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

use chrono::Utc;

use super::AppState;
use super::actors::{StoreMsg, call};
use super::cluster::{ReadQuery, check_api_version, check_protocol};
use super::message_sender::resolve_machine;
use super::thread_api::caller_t3_thread;
use crate::child_thread::{Lineage, LinkEnd, LinkId};
use crate::domain::{API_VERSION, ThreadId};
use crate::error::AppError;
use crate::fleet::http::{ClusterClient, ClusterResponse, DEFAULT_MAX_BODY};
use crate::machine::{MachineId, MachineName};
use crate::t3::{
    AddedProject, LaunchError, LaunchSpec, LaunchedThread, ProjectSpec, ProviderInstance, T3Env,
    T3Project, add_project, launch_thread, launch_thread_id, projects, providers,
};
use crate::thread_ref::Caller;

const CLUSTER_LAUNCH_PATH: &str = "/v1/cluster/t3/launch";
const CLUSTER_PROJECTS_PATH: &str = "/v1/cluster/t3/projects";
// its own path, so a peer without it answers 404 rather than the 405 of the listing route
const CLUSTER_PROJECT_ADD_PATH: &str = "/v1/cluster/t3/projects/add";
// the peer issues a token, waits up to 30 seconds for T3, and revokes the token
const LAUNCH_REQUEST_TIMEOUT: Duration = Duration::from_secs(75);
const PROJECTS_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
// provider discovery asks the running T3, so it gets a share of the listing
// deadline and the projects go back without it when T3 is slow
const PROVIDERS_BUDGET: Duration = Duration::from_secs(4);
// the peer runs `t3 project add`, which has its own 10 second deadline
const PROJECT_ADD_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// The calling agent, when it launches a child thread: a thread that
    /// reports back to the caller's T3 thread once with `homebased thread
    /// report`. Its first message names that thread instead of `source_thread`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_of: Option<Caller>,
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
    /// Its link to the calling thread, for a child thread
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<Lineage>,
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
    /// Enabled provider instances, by id; absent from a peer that predates them
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub providers: Option<Vec<ProviderInstance>>,
}

/// `POST /v1/t3/projects/add` body
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T3ProjectAddRequest {
    /// Public API version
    pub api_version: u32,
    /// Machine name or UUID; this machine when omitted
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    /// What to add
    pub project: ProjectSpec,
}

/// `POST /v1/t3/projects/add` answer
#[derive(Debug, Serialize, Deserialize)]
pub struct T3ProjectAddResponse {
    /// Public API version
    pub api_version: u32,
    /// Machine whose T3 has the project
    pub machine: MachineId,
    /// Name of that machine
    pub machine_name: MachineName,
    /// The project added, or the one already there
    pub project: AddedProject,
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
    /// The parent, on the origin machine, of a child thread; a peer that
    /// predates child threads refuses the field
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<LinkEnd>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterLaunchBody {
    api_version: u32,
    protocol_version: u32,
    machine: MachineId,
    thread: LaunchedThread,
}

/// Project add request from the origin daemon to the adding peer
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterProjectAdd {
    api_version: u32,
    protocol_version: u32,
    destination_machine: MachineId,
    origin_machine: MachineId,
    project: ProjectSpec,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterProjectAddBody {
    api_version: u32,
    machine: MachineId,
    project: AddedProject,
}

/// Local-only routes; a launch starts an agent, so it stays off the TCP listener
pub fn socket_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/t3/launch", post(launch))
        .route("/v1/t3/projects", post(list_projects))
        .route("/v1/t3/projects/add", post(add))
}

/// Peer routes for an enabled fleet, inside its trusted network boundary
pub fn cluster_routes() -> Router<AppState> {
    Router::new()
        .route(CLUSTER_LAUNCH_PATH, post(cluster_launch))
        .route(CLUSTER_PROJECTS_PATH, get(cluster_projects))
        .route(CLUSTER_PROJECT_ADD_PATH, post(cluster_add))
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
    let local = state.machine.identity.machine;

    let mut launch = body.launch;
    let parent = match body.child_of {
        Some(caller) => Some(child_parent(&state, caller, &launch).await?),
        None => None,
    };
    let footer = match (&parent, body.source_thread) {
        (Some(parent), _) => Some(child_route(&state.machine.name, &parent.t3_thread)),
        (None, Some(thread)) => Some(reply_route(&state.machine.name, thread)),
        (None, None) => None,
    };
    if let (Some(footer), Some(message)) = (footer, launch.message.as_mut()) {
        message.push_str(&footer);
        // the route can push a message near the limit over it, which the peer would refuse
        launch
            .validate()
            .map_err(|message| AppError::Usage { message })?;
    }
    let lineage = parent.map(|parent| Lineage {
        id: LinkId::from_launch(launch.request_id),
        parent,
        child: LinkEnd {
            machine: destination,
            t3_thread: launch_thread_id(launch.request_id),
        },
        created_at: Utc::now(),
    });

    let (thread, machine_name) = if destination == local {
        let thread = launch_linked(&state, local, lineage.clone(), launch).await?;
        (thread, state.machine.name.clone())
    } else {
        let request = launch.request_id;
        let parent = lineage.as_ref().map(|lineage| lineage.parent.clone());
        let launched = forward_launch(&state, destination, launch, parent).await?;
        if let Some(lineage) = lineage.clone() {
            // the child runs already, so a link this side failed to save
            // leaves a launch only a retry with the same id can finish
            save_lineage(&state, lineage).await.map_err(|error| {
                AppError::T3LaunchOutcomeUnknown {
                    request,
                    machine: Some(destination),
                    message: format!(
                        "the child thread launched, but its link was not saved here: {error}"
                    ),
                }
            })?;
        }
        launched
    };

    Ok(Json(T3LaunchResponse {
        api_version: API_VERSION,
        machine: destination,
        machine_name,
        thread,
        lineage,
    }))
}

/// The parent end of a child launch: the caller's T3 thread here
async fn child_parent(
    state: &AppState,
    caller: Caller,
    launch: &LaunchSpec,
) -> Result<LinkEnd, AppError> {
    if launch.message.is_none() {
        return Err(AppError::Usage {
            message: "--child needs a first message, the work the child thread owns".into(),
        });
    }
    let t3_thread = caller_t3_thread(state, caller)
        .await?
        .ok_or_else(|| AppError::Usage {
            message:
                "--child needs a calling T3 thread on this machine; run it from a T3 Code thread"
                    .into(),
        })?;
    Ok(LinkEnd {
        machine: state.machine.identity.machine,
        t3_thread,
    })
}

async fn save_lineage(state: &AppState, lineage: Lineage) -> Result<Lineage, AppError> {
    call(&state.store, |reply| StoreMsg::InsertLineage {
        lineage,
        reply,
    })
    .await
}

/// Launch here, saving the link of a child thread first
///
/// The link must be there before the child's first run, which can report at
/// once. A launch that T3 then refuses leaves a link to a thread that never
/// existed, which lookups skip
async fn launch_linked(
    state: &AppState,
    machine: MachineId,
    lineage: Option<Lineage>,
    launch: LaunchSpec,
) -> Result<LaunchedThread, AppError> {
    let Some(lineage) = lineage else {
        return launch_here(machine, launch).await;
    };
    let lineage = save_lineage(state, lineage).await?;
    let thread = launch_here(machine, launch).await?;
    if thread.thread_id != lineage.child.t3_thread {
        return Err(AppError::Internal {
            message: format!(
                "T3 launched thread {} where the link names {}",
                thread.thread_id, lineage.child.t3_thread
            ),
        });
    }
    Ok(thread)
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
    let lineage = match body.parent {
        Some(parent) if parent.machine != body.origin_machine => {
            return Err(AppError::Usage {
                message: "a child thread's parent must be on the machine that launches it".into(),
            });
        }
        Some(parent) => Some(Lineage {
            id: LinkId::from_launch(body.launch.request_id),
            parent,
            child: LinkEnd {
                machine,
                t3_thread: launch_thread_id(body.launch.request_id),
            },
            created_at: Utc::now(),
        }),
        None => None,
    };
    let thread = launch_linked(&state, machine, lineage, body.launch).await?;
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
    let (projects, providers) = if destination == state.machine.identity.machine {
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
        (body.projects, body.providers)
    };

    Ok(Json(T3ProjectsBody {
        api_version: API_VERSION,
        machine: destination,
        projects,
        providers,
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
    let (projects, providers) = projects_here().await?;
    Ok(Json(T3ProjectsBody {
        api_version: API_VERSION,
        machine: state.machine.identity.machine,
        projects,
        providers,
    }))
}

async fn add(
    State(state): State<AppState>,
    Json(body): Json<T3ProjectAddRequest>,
) -> Result<Json<T3ProjectAddResponse>, AppError> {
    check_api_version(body.api_version)?;
    body.project
        .validate()
        .map_err(|message| AppError::T3ProjectRefused { message })?;
    let destination = destination(&state, body.machine.as_deref()).await?;

    let (project, machine_name) = if destination == state.machine.identity.machine {
        (add_here(body.project).await?, state.machine.name.clone())
    } else {
        forward_add(&state, destination, body.project).await?
    };
    Ok(Json(T3ProjectAddResponse {
        api_version: API_VERSION,
        machine: destination,
        machine_name,
        project,
    }))
}

async fn cluster_add(
    State(state): State<AppState>,
    Json(body): Json<ClusterProjectAdd>,
) -> Result<Json<ClusterProjectAddBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.origin_machine)?;
    body.project
        .validate()
        .map_err(|message| AppError::T3ProjectRefused { message })?;

    Ok(Json(ClusterProjectAddBody {
        api_version: API_VERSION,
        machine: state.machine.identity.machine,
        project: add_here(body.project).await?,
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

/// Projects and enabled provider instances of T3 on this machine
///
/// Providers are `None` when T3 cannot say within [`PROVIDERS_BUDGET`]; the
/// discovery left running then finishes, and revokes its token, on its own
async fn projects_here() -> Result<(Vec<T3Project>, Option<Vec<ProviderInstance>>), AppError> {
    let discovery = tokio::task::spawn_blocking(|| providers(&T3Env::from_env()));
    let projects = tokio::task::spawn_blocking(|| projects(&T3Env::from_env()))
        .await
        .map_err(|error| AppError::Internal {
            message: format!("T3 project worker failed: {error}"),
        })?
        // reading projects sends nothing to T3, so its outcome is never unknown
        .map_err(|error| t3_error(error, |message| AppError::T3Unavailable { message }))?;
    let providers = tokio::time::timeout(PROVIDERS_BUDGET, discovery)
        .await
        .ok()
        .and_then(Result::ok)
        .flatten();
    Ok((projects, providers))
}

async fn add_here(spec: ProjectSpec) -> Result<AddedProject, AppError> {
    tokio::task::spawn_blocking(move || add_project(&T3Env::from_env(), &spec))
        .await
        .map_err(|error| AppError::Internal {
            message: format!("T3 project worker failed: {error}"),
        })?
        .map_err(|error| match error {
            LaunchError::Refused(message) => AppError::T3ProjectRefused { message },
            // an add that may have happened shows up as existing on retry
            error => t3_error(error, |message| AppError::T3Unavailable { message }),
        })
}

async fn forward_add(
    state: &AppState,
    destination: MachineId,
    project: ProjectSpec,
) -> Result<(AddedProject, MachineName), AppError> {
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| AppError::MachineNotFound {
            machine: destination.to_string(),
        })?;
    let verified = fleet.connect(destination).await?;
    let wire = ClusterProjectAdd {
        api_version: API_VERSION,
        protocol_version: verified.protocol.0,
        destination_machine: destination,
        origin_machine: state.machine.identity.machine,
        project,
    };

    // a retry after a lost reply finds the project as existing, so it adds nothing twice
    let response = ClusterClient::new(PROJECT_ADD_REQUEST_TIMEOUT, DEFAULT_MAX_BODY)
        .post_json(&verified.address, CLUSTER_PROJECT_ADD_PATH, &wire)
        .await
        .map_err(|error| AppError::MachineUnavailable {
            machine: destination,
            message: error.to_string(),
        })?;
    let body: ClusterProjectAddBody = peer_body(destination, &response)?;
    Ok((body.project, verified.advertisement.name))
}

async fn forward_launch(
    state: &AppState,
    destination: MachineId,
    launch: LaunchSpec,
    parent: Option<LinkEnd>,
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
        parent,
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
    // axum refuses a body with a field it does not know before the handler
    // runs, and a peer from before child threads does not know `parent`
    if wire.parent.is_some() && response.status == StatusCode::UNPROCESSABLE_ENTITY {
        return Err(AppError::T3Incompatible {
            message: format!(
                "machine {destination} runs a homebased without child threads; run `homebased update` there"
            ),
        });
    }
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
                    "machine {machine} runs a homebased without this T3 command; run `homebased update` there"
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

impl PeerBody for ClusterProjectAddBody {
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

/// Footer that tells a child thread where it reports, and how
fn child_route(machine: &MachineName, parent: &str) -> String {
    format!(
        "\n\n---\nThis is a child thread of T3 thread `thread_{parent}` on machine `{machine}`, launched through homebased to own this work. When it is done and its homebased tasks, jobs and child threads have finished, report back once with:\n`homebased thread report --summary \"<report>\"`\nThat sends the report to the parent and settles this thread after the turn. To ask the parent something before then, run:\n`homebased message send --machine {machine} --thread thread_{parent} --message \"<message>\"`"
    )
}

/// Footer that tells the launched agent how to answer the thread that asked
fn reply_route(machine: &MachineName, thread: ThreadId) -> String {
    format!(
        "\n\n---\nStarted through homebased by thread `{thread}` on machine `{machine}`. To report back, run:\n`homebased message send --machine {machine} --thread {thread} --message \"<reply>\"`"
    )
}
