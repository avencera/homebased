//! Version 1 queue routes, with Fleet forwarding and origin-owned job submissions
//!
//! Reads return `{api_version,machine,resources}` or `{api_version,machine,jobs}`
//! Job detail returns `{api_version,job,active_run,runs,last_stop_cause,events}`
//! Writes return the store's accepted job or retained operation result plus `api_version`

use std::path::PathBuf;

use axum::extract::{FromRequest, Path, Query, Request, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use crate::domain::{API_VERSION, TaskEnv};
use crate::error::AppError;
use crate::fleet::directory::NameTarget;
use crate::fleet::http::ClusterClient;
use crate::machine::MachineId;
use crate::queue::delivery::{JobRoute, JobSubmission, RoutedJobEvent};
use crate::queue::spec::{JobSpec, MachineSelector, schema_json};
use crate::queue::{AttentionId, JobId, OperationId, Placement, QueueError, ResourceName};
use crate::store::queue::JobAccepted;
use crate::store::queue::interface::QueueRequest;
use crate::submission::CallbackContext;

/// Application JSON extractor: malformed or oversized bodies use the normal error envelope
pub(super) struct QueueJson<T>(pub(super) T);

impl<S, T> FromRequest<S> for QueueJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = AppError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let value = super::api::json_body(request, state).await?;
        serde_path_to_error::deserialize(&value)
            .map(Self)
            .map_err(|error| super::api::invalid_at(&value, "", &error))
    }
}

/// Resource and queue reads shared by socket and web
pub fn read_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/resources", get(resources))
        .route("/v1/resource/jobs", get(jobs))
        .route("/v1/resource/jobs/{job}", get(show))
        .route("/v1/resource/schema", get(schema))
}

/// Writes that create work or resources, served only on the Unix socket
///
/// A job runs arbitrary commands, so submitting one over the TCP listener
/// would let any host that reaches it execute code, as task submit would
pub fn submit_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/resources", post(register))
        .route("/v1/resource/jobs", post(submit))
}

/// Writes that only reorder, stop, or release accepted work; the web router
/// adds browser write protection
pub fn control_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/resource/jobs/{job}/move", post(move_job))
        .route("/v1/resource/jobs/{job}/cancel", post(cancel))
        .route("/v1/resource/release", post(release))
}

/// Identity-checked transport for queue commands and job events
pub fn cluster_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/cluster/queue", post(cluster_request))
        .route("/v1/cluster/job-events", post(cluster_event))
        .layer(axum::middleware::from_fn(super::web::browser_write_guard))
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct MachineQuery {
    machine: Option<MachineSelector>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterBody {
    name: ResourceName,
    device: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitBody {
    job_id: JobId,
    spec: JobSpec,
    env: TaskEnv,
    callback_cwd: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MoveBody {
    operation_id: OperationId,
    placement: Placement,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationBody {
    operation_id: OperationId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseBody {
    operation_id: OperationId,
    attention: AttentionId,
}

/// Destination-bound request carried over the existing Fleet HTTP transport
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClusterQueue {
    pub(super) api_version: u32,
    pub(super) protocol_version: u32,
    pub(super) destination_machine: MachineId,
    pub(super) request: QueueRequest,
}

/// Destination-bound job event; its owners are checked against the saved route
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClusterJobEvent {
    pub(super) api_version: u32,
    pub(super) protocol_version: u32,
    pub(super) destination_machine: MachineId,
    pub(super) event: RoutedJobEvent,
}

async fn schema() -> Result<Json<Value>, AppError> {
    let mut schema = schema_json()?;
    schema["api_version"] = json!(API_VERSION);
    Ok(Json(schema))
}

async fn resources(
    State(state): State<AppState>,
    Query(query): Query<MachineQuery>,
) -> Result<Json<Value>, AppError> {
    let machine = resolve_machine(&state, query.machine.as_ref()).await?;
    forward(&state, machine, QueueRequest::Resources)
        .await
        .map(Json)
}

async fn jobs(
    State(state): State<AppState>,
    Query(query): Query<MachineQuery>,
) -> Result<Json<Value>, AppError> {
    let machine = resolve_machine(&state, query.machine.as_ref()).await?;
    forward(&state, machine, QueueRequest::Jobs).await.map(Json)
}

async fn show(
    State(state): State<AppState>,
    Path(job): Path<JobId>,
    Query(query): Query<MachineQuery>,
) -> Result<Json<Value>, AppError> {
    let machine = job_authority(&state, job, query.machine.as_ref()).await?;
    forward(&state, machine, QueueRequest::Show { job })
        .await
        .map(Json)
}

async fn register(
    State(state): State<AppState>,
    QueueJson(body): QueueJson<RegisterBody>,
) -> Result<Json<Value>, AppError> {
    execute(
        &state,
        QueueRequest::Register {
            name: body.name,
            device: body.device,
        },
        TaskEnv::capture(),
    )
    .await
    .map(Json)
}

async fn submit(
    State(state): State<AppState>,
    QueueJson(body): QueueJson<SubmitBody>,
) -> Result<Json<Value>, AppError> {
    let _guard = state.locks.job_submissions.lock(body.job_id).await;
    let saved = call(&state.store, |reply| StoreMsg::JobRoute {
        job: body.job_id,
        reply,
    })
    .await?;
    let route = match saved {
        Some(route) => {
            if route.origin != state.machine.identity.machine
                || route.digest != body.spec.digest()?
            {
                return Err(QueueError::JobConflict { job: body.job_id }.into());
            }
            route
        }
        None => {
            let authority = resolve_machine(&state, body.spec.machine.as_ref()).await?;
            crate::spec::check_cwd(&body.callback_cwd)?;
            let codex = call(&state.supervisor, |reply| SupervisorMsg::CallbackCodex {
                path: body.env.path.clone(),
                cwd: body.callback_cwd.clone(),
                reply,
            })
            .await?;
            let route = JobRoute {
                job: body.job_id,
                origin: state.machine.identity.machine,
                authority,
                thread: body.spec.thread,
                digest: body.spec.digest()?,
                spec: body.spec,
                callback: CallbackContext {
                    env: body.env,
                    cwd: body.callback_cwd,
                    codex,
                },
                target: None,
                submission: JobSubmission::Unknown,
                last_accepted_seq: 0,
                last_settled_seq: 0,
            };
            call(&state.store, |reply| StoreMsg::InsertJobRoute {
                route: Box::new(route),
                reply,
            })
            .await?
        }
    };
    if let JobSubmission::Rejected { error, status } = &route.submission {
        return Err(crate::client::map_error(
            axum::http::StatusCode::from_u16(*status).map_err(|error| AppError::Internal {
                message: format!("invalid saved refusal status: {error}"),
            })?,
            &serde_json::to_vec(error)?,
        ));
    }
    let request = QueueRequest::Submit {
        job: route.job,
        origin: route.origin,
        spec: Box::new(route.spec.clone()),
    };
    let result = if route.authority == state.machine.identity.machine {
        execute(&state, request, route.callback.env.clone()).await
    } else {
        forward(&state, route.authority, request).await
    };
    match result {
        Ok(value) => {
            let accepted: JobAccepted = serde_json::from_value(value.clone())?;
            if accepted.machine != route.authority || accepted.job_id != route.job {
                return Err(QueueError::JobConflict { job: route.job }.into());
            }
            call(&state.store, |reply| StoreMsg::ResolveJobRoute {
                job: route.job,
                submission: JobSubmission::Accepted,
                target: Some(accepted.target),
                reply,
            })
            .await?;
            Ok(Json(value))
        }
        Err(error) => {
            if matches!(route.submission, JobSubmission::Unknown) && definite_refusal(&error) {
                call(&state.store, |reply| StoreMsg::ResolveJobRoute {
                    job: route.job,
                    submission: JobSubmission::Rejected {
                        error: error.to_json(),
                        status: error.http_status().as_u16(),
                    },
                    target: None,
                    reply,
                })
                .await?;
            }
            Err(error)
        }
    }
}

fn definite_refusal(error: &AppError) -> bool {
    matches!(
        error.code(),
        "invalid_spec"
            | "invalid_cwd"
            | "executable_missing"
            | "resource_not_found"
            | "job_conflict"
    )
}

async fn move_job(
    State(state): State<AppState>,
    Path(job): Path<JobId>,
    Query(query): Query<MachineQuery>,
    QueueJson(body): QueueJson<MoveBody>,
) -> Result<Json<Value>, AppError> {
    let machine = job_authority(&state, job, query.machine.as_ref()).await?;
    forward(
        &state,
        machine,
        QueueRequest::Move {
            operation: body.operation_id,
            job,
            placement: body.placement,
        },
    )
    .await
    .map(Json)
}

async fn cancel(
    State(state): State<AppState>,
    Path(job): Path<JobId>,
    Query(query): Query<MachineQuery>,
    QueueJson(body): QueueJson<OperationBody>,
) -> Result<Json<Value>, AppError> {
    let machine = job_authority(&state, job, query.machine.as_ref()).await?;
    forward(
        &state,
        machine,
        QueueRequest::Cancel {
            operation: body.operation_id,
            job,
        },
    )
    .await
    .map(Json)
}

async fn release(
    State(state): State<AppState>,
    Query(query): Query<MachineQuery>,
    QueueJson(body): QueueJson<ReleaseBody>,
) -> Result<Json<Value>, AppError> {
    let machine = resolve_machine(&state, query.machine.as_ref()).await?;
    forward(
        &state,
        machine,
        QueueRequest::Release {
            operation: body.operation_id,
            attention: body.attention,
        },
    )
    .await
    .map(Json)
}

async fn job_authority(
    state: &AppState,
    job: JobId,
    selector: Option<&MachineSelector>,
) -> Result<MachineId, AppError> {
    if selector.is_some() {
        return resolve_machine(state, selector).await;
    }
    if let Some(route) = call(&state.store, |reply| StoreMsg::JobRoute { job, reply }).await? {
        return Ok(route.authority);
    }
    Ok(state.machine.identity.machine)
}

async fn resolve_machine(
    state: &AppState,
    selector: Option<&MachineSelector>,
) -> Result<MachineId, AppError> {
    let local = state.machine.identity.machine;
    match selector {
        None => Ok(local),
        Some(MachineSelector::Id(machine)) => Ok(*machine),
        Some(MachineSelector::Name(name)) if name == &state.machine.name => Ok(local),
        Some(MachineSelector::Name(name)) => {
            let fleet = state
                .fleet
                .handle()
                .ok_or_else(|| AppError::MachineNotFound {
                    machine: name.to_string(),
                })?;
            match fleet.resolve_name(name).await? {
                NameTarget::Local => Ok(local),
                NameTarget::Peer(machine) => Ok(machine),
            }
        }
    }
}

pub(super) async fn forward(
    state: &AppState,
    machine: MachineId,
    request: QueueRequest,
) -> Result<Value, AppError> {
    if machine == state.machine.identity.machine {
        return execute(state, request, TaskEnv::capture()).await;
    }
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| AppError::MachineNotFound {
            machine: machine.to_string(),
        })?;
    let destination = fleet.connect(machine).await?;
    let body = ClusterQueue {
        api_version: API_VERSION,
        protocol_version: destination.protocol.0,
        destination_machine: machine,
        request,
    };
    let response = ClusterClient::default()
        .post_json(&destination.address, "/v1/cluster/queue", &body)
        .await
        .map_err(|error| AppError::MachineUnavailable {
            machine,
            message: error.to_string(),
        })?;
    if !response.status.is_success() {
        return Err(crate::client::map_error(response.status, &response.body));
    }
    let mut value: Value = serde_json::from_slice(&response.body)?;
    super::cluster::check_api_version(
        value
            .get("api_version")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(0),
    )?;
    if value.get("protocol_version").and_then(Value::as_u64)
        != Some(u64::from(destination.protocol.0))
    {
        return Err(AppError::MachineUnavailable {
            machine,
            message: "queue response protocol differs".into(),
        });
    }
    if let Some(object) = value.as_object_mut() {
        object.remove("protocol_version");
    }
    Ok(value)
}

async fn execute(state: &AppState, request: QueueRequest, env: TaskEnv) -> Result<Value, AppError> {
    if let QueueRequest::Submit { job, origin, spec } = &request {
        let stored = call(&state.store, |reply| StoreMsg::QueueJob { id: *job, reply }).await?;
        match stored {
            Some(record)
                if record.digest != spec.digest()?
                    || record.origin != *origin
                    || record.machine != state.machine.identity.machine =>
            {
                return Err(QueueError::JobConflict { job: *job }.into());
            }
            Some(_) => {}
            None => spec.check_on_authority(&env.path)?,
        }
    }
    call(&state.store, |reply| StoreMsg::QueueInterface {
        machine: state.machine.identity.machine,
        request: Box::new(request),
        env,
        reply,
    })
    .await
}

async fn cluster_request(
    State(state): State<AppState>,
    QueueJson(body): QueueJson<ClusterQueue>,
) -> Result<Json<Value>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    super::cluster::check_api_version(body.api_version)?;
    super::cluster::check_protocol(body.protocol_version, body.destination_machine)?;
    let mut value = execute(&state, body.request, TaskEnv::capture()).await?;
    value["protocol_version"] = json!(body.protocol_version);
    Ok(Json(value))
}

async fn cluster_event(
    State(state): State<AppState>,
    QueueJson(body): QueueJson<ClusterJobEvent>,
) -> Result<Json<Value>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    super::cluster::check_api_version(body.api_version)?;
    super::cluster::check_protocol(body.protocol_version, body.destination_machine)?;
    if body.event.origin != body.destination_machine {
        return Err(QueueError::JobConflict {
            job: body.event.event.job,
        }
        .into());
    }
    let result = call(&state.store, |reply| StoreMsg::AcceptJobEvent {
        event: Box::new(body.event),
        reply,
    })
    .await?;
    Ok(Json(json!({
        "api_version": API_VERSION,
        "protocol_version": body.protocol_version,
        "result": result,
    })))
}
