//! Axum routes and error mapping.

pub mod views;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::OnceLock;

use axum::extract::{FromRequest, Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::callback::last_event_for_row;
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::daemon::api::views::{LogTail, StatusBody, TaskDetail, TaskList, TaskSummary};
use crate::daemon::{AppState, web};
use crate::domain::{
    API_VERSION, ProcessStatus, TaskEnv, TaskId, TaskIdentity, ThreadId, Workload,
};
use crate::error::AppError;
use crate::files::{
    ContentOriginBody, DirectoryListing, PathToken, ResolveBody, ResolvedPath, list_directory,
    resolve_absolute_path,
};
use crate::invocation::{
    ManagedEnvironmentPreview, StdinPolicy, invocation_from_normalized_for_identity,
    persist_workload, resolve_workload_binary,
};
use crate::spec::{self, NormalizedSpec, NormalizedWorkload};
use crate::store::{self, CancelResult};
use crate::submission::RequestId;

const CANCELLATION_INTENT_SHARDS: usize = 64;
static CANCELLATION_INTENT_PERMITS: OnceLock<[Semaphore; CANCELLATION_INTENT_SHARDS]> =
    OnceLock::new();

/// Serialize cancellation-intent creation for one task on its origin daemon.
pub(super) async fn lock_cancellation_intent(
    task: TaskId,
) -> Result<SemaphorePermit<'static>, AppError> {
    let permits =
        CANCELLATION_INTENT_PERMITS.get_or_init(|| std::array::from_fn(|_| Semaphore::new(1)));
    let mut hasher = DefaultHasher::new();
    task.hash(&mut hasher);
    let shard = (hasher.finish() as usize) % CANCELLATION_INTENT_SHARDS;
    permits[shard]
        .acquire()
        .await
        .map_err(|error| AppError::Internal {
            message: format!("cancellation intent permit unavailable: {error}"),
        })
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.http_status();
        (status, Json(self.to_json())).into_response()
    }
}

/// Routes that only read state. Safe to expose on the TCP listener.
pub fn read_routes() -> Router<AppState> {
    Router::new()
        .merge(crate::daemon::fleet_api::read_routes())
        .route("/v1/status", get(status))
        .route("/v1/tasks", get(list))
        .route("/v1/tasks/{id}", get(show))
        .route("/v1/tasks/{id}/log", get(log))
        .route("/v1/files/resolve", post(files_resolve))
        .route("/v1/files/origin", get(files_origin))
        .route("/v1/files/{token}", get(files_list))
}

/// Routes that change state. Unix socket only: the socket is mode 0600, while a
/// loopback port is reachable from any page the user has open.
pub fn write_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/tasks", post(submit))
        .route("/v1/tasks/dry-run", post(dry_run))
        .route("/v1/tasks/{id}/cancel", post(cancel))
}

/// Full API for the Unix socket.
pub fn socket_router(state: AppState) -> Router {
    read_routes()
        .merge(write_routes())
        .merge(crate::daemon::fleet_api::socket_routes())
        .merge(crate::daemon::release_watcher_api::socket_routes())
        .with_state(state)
}

async fn status(State(state): State<AppState>) -> Result<Json<StatusBody>, AppError> {
    let in_flight = call(&state.store, |reply| StoreMsg::InFlightCount { reply }).await?;
    Ok(Json(StatusBody {
        api_version: API_VERSION,
        version: env!("CARGO_PKG_VERSION"),
        pid: std::process::id(),
        socket: state.home.sock_path().display().to_string(),
        web: state.web.map(web::url_for),
        in_flight,
    }))
}

/// Validated socket request. Built only by `SpecBody`, which is where the
/// envelope, the normalized spec, and the captured env are each checked.
#[derive(Debug)]
pub(super) struct SubmitBody {
    pub(super) spec: NormalizedSpec,
    pub(super) env: TaskEnv,
    pub(super) request: Option<RequestId>,
    pub(super) callback_cwd: Option<PathBuf>,
}

/// Top-level socket envelope. `spec` and `env` stay as `Value` so each can be
/// parsed with its own pointer prefix, but `deny_unknown_fields` here is what
/// makes an unknown top-level key an `invalid_spec` instead of silent input.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitEnvelope {
    #[serde(default)]
    spec: Option<Value>,
    #[serde(default)]
    env: Option<Value>,
    #[serde(default)]
    request_id: Option<RequestId>,
    #[serde(default)]
    callback_cwd: Option<PathBuf>,
}

/// Application-owned JSON body extractor that maps failures to `AppError`.
struct SpecBody(SubmitBody);

impl<S> FromRequest<S> for SpecBody
where
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = bytes::Bytes::from_request(req, state)
            .await
            .map_err(|err| AppError::InvalidSpec {
                pointer: String::new(),
                value: Value::Null,
                message: format!("invalid request body: {err}"),
            })?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|err| AppError::InvalidSpec {
            pointer: String::new(),
            value: Value::Null,
            message: format!("invalid JSON: {err}"),
        })?;
        let envelope: SubmitEnvelope =
            serde_path_to_error::deserialize(&value).map_err(|err| invalid_at(&value, "", &err))?;
        let env_value = envelope.env.ok_or_else(|| missing_field("/env", "env"))?;
        let env = serde_path_to_error::deserialize(&env_value)
            .map_err(|err| invalid_at(&env_value, "/env", &err))?;
        let spec_value = envelope
            .spec
            .ok_or_else(|| missing_field("/spec", "spec"))?;
        // parse_normalized_value already enforces api_version and min timeout
        let spec = spec::parse_normalized_value(&spec_value).map_err(|err| prefix("/spec", err))?;
        let request = match (spec.machine.is_some(), envelope.request_id) {
            (true, request_id) => Some(request_id.unwrap_or_default()),
            (false, Some(request_id)) => {
                return Err(AppError::InvalidSpec {
                    pointer: "/request_id".into(),
                    value: json!(request_id),
                    message: "request_id is only valid for remote submissions with spec.machine"
                        .into(),
                });
            }
            (false, None) => None,
        };
        Ok(Self(SubmitBody {
            spec,
            env,
            request,
            callback_cwd: envelope.callback_cwd,
        }))
    }
}

fn missing_field(pointer: &str, name: &str) -> AppError {
    AppError::InvalidSpec {
        pointer: pointer.into(),
        value: Value::Null,
        message: format!("missing field `{name}`"),
    }
}

/// Build an `invalid_spec` at the failing path, rooted at `prefix`.
fn invalid_at(
    root: &Value,
    prefix: &str,
    err: &serde_path_to_error::Error<serde_json::Error>,
) -> AppError {
    let pointer = spec::json_pointer(err.path());
    AppError::InvalidSpec {
        pointer: format!("{prefix}{pointer}"),
        value: root.pointer(&pointer).cloned().unwrap_or(Value::Null),
        message: err.to_string(),
    }
}

/// Re-root an `invalid_spec` pointer under `prefix`.
fn prefix(prefix: &str, err: AppError) -> AppError {
    match err {
        AppError::InvalidSpec {
            pointer,
            value,
            message,
        } => AppError::InvalidSpec {
            pointer: format!("{prefix}{pointer}"),
            value,
            message,
        },
        other => other,
    }
}

#[derive(Serialize)]
struct SubmitResponse {
    api_version: u32,
    id: TaskId,
    task_id: TaskId,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<RequestId>,
    status: ProcessStatus,
}

async fn submit(
    State(state): State<AppState>,
    SpecBody(body): SpecBody,
) -> Result<(StatusCode, Json<SubmitResponse>), AppError> {
    let (id, status, request_id) = if body.spec.machine.is_some() {
        crate::daemon::origin_submit::submit(&state, body).await?
    } else {
        let (id, status) = accept_task(&state, body).await?;
        (id, status, None)
    };
    Ok((
        StatusCode::OK,
        Json(SubmitResponse {
            api_version: API_VERSION,
            id,
            task_id: id,
            request_id,
            status,
        }),
    ))
}

#[derive(Serialize)]
pub(super) struct DryRunResponse {
    pub(super) api_version: u32,
    pub(super) spec: NormalizedSpec,
    pub(super) argv: Vec<String>,
    pub(super) stdin: StdinPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) execution_cwd: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) managed_environment: Option<ManagedEnvironmentPreview>,
}

async fn dry_run(
    State(state): State<AppState>,
    SpecBody(body): SpecBody,
) -> Result<Json<DryRunResponse>, AppError> {
    let spec = body.spec;
    if spec.machine.is_some() {
        return crate::daemon::origin_submit::dry_run(&state, spec, body.env)
            .await
            .map(Json);
    }
    local_dry_run(&state, spec, body.env).map(Json)
}

pub(super) fn local_dry_run(
    state: &AppState,
    spec: NormalizedSpec,
    env: TaskEnv,
) -> Result<DryRunResponse, AppError> {
    spec::check_cwd(&spec.cwd)?;
    let prompt_feed = agent_feed_placeholder(state.home.root(), &spec.workload);
    let invocation = invocation_from_normalized_for_identity(
        &spec.workload,
        &env.path,
        &spec.cwd,
        prompt_feed.as_deref(),
        TaskIdentity::Preview,
    )?;
    Ok(DryRunResponse {
        api_version: API_VERSION,
        spec,
        argv: invocation.to_vec(),
        stdin: invocation.stdin,
        execution_cwd: None,
        managed_environment: invocation.managed_environment,
    })
}

/// Deterministic dry-run feed path for any agent. Only Grok puts it in argv;
/// Codex and Claude keep it as the stdin evidence path.
fn agent_feed_placeholder(
    root: &std::path::Path,
    workload: &NormalizedWorkload,
) -> Option<PathBuf> {
    match workload {
        NormalizedWorkload::Agent(_) => {
            Some(root.join("tasks").join("<task-id>").join("prompt.feed.txt"))
        }
        NormalizedWorkload::Task(_) => None,
    }
}

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    thread: Option<String>,
}

async fn list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<TaskList>, AppError> {
    let mut statuses = Vec::new();
    if let Some(raw) = query.status {
        for part in raw.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            statuses.push(
                ProcessStatus::from_storage(part).map_err(|_| AppError::Usage {
                    message: format!("invalid status {part}"),
                })?,
            );
        }
    }
    let thread = match query.thread {
        Some(s) => Some(s.parse::<ThreadId>()?),
        None => None,
    };
    let rows = call(&state.store, |reply| StoreMsg::ListTasks {
        statuses,
        thread,
        reply,
    })
    .await?;
    let ids = rows.iter().map(|row| row.id).collect();
    let presentations = call(&state.store, |reply| StoreMsg::TaskPresentations {
        ids,
        reply,
    })
    .await?;
    Ok(Json(TaskList::from_rows(&rows, &presentations)))
}

async fn show(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<Value>, AppError> {
    crate::daemon::inspection::show(&state, id).await.map(Json)
}

pub(super) async fn local_detail(state: &AppState, id: TaskId) -> Result<TaskDetail, AppError> {
    let row = call(&state.store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .ok_or(AppError::TaskNotFound { id })?;
    let presentations = call(&state.store, |reply| StoreMsg::TaskPresentations {
        ids: vec![id],
        reply,
    })
    .await?;
    let reports = call(&state.store, |reply| StoreMsg::Reports { id, reply }).await?;
    let evidence = state.home.task_dir(id);
    let last_event = last_event_for_row(&row, &reports, evidence.clone());
    Ok(TaskDetail {
        api_version: API_VERSION,
        summary: TaskSummary::from_row(&row, presentations.get(&id)),
        reports,
        output_log: state.home.task_paths(id).output,
        evidence,
        last_event,
    })
}

#[derive(Deserialize)]
struct LogQuery {
    #[serde(default)]
    tail: Option<usize>,
}

async fn log(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
    Query(query): Query<LogQuery>,
) -> Result<Json<Value>, AppError> {
    crate::daemon::inspection::log(&state, id, query.tail)
        .await
        .map(Json)
}

pub(super) async fn local_log(
    state: &AppState,
    id: TaskId,
    tail: Option<usize>,
) -> Result<LogTail, AppError> {
    if call(&state.store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .is_none()
    {
        return Err(AppError::TaskNotFound { id });
    }
    let tail = state.home.task_paths(id).read_output(tail)?;
    let (log, truncated) = match tail {
        Some(output) => (output.text, output.truncated),
        None => (String::new(), false),
    };
    Ok(LogTail {
        api_version: API_VERSION,
        id,
        log,
        truncated,
    })
}

async fn files_resolve(Json(body): Json<ResolveBody>) -> Result<Json<ResolvedPath>, AppError> {
    Ok(Json(resolve_absolute_path(&body.path)?))
}

async fn files_list(Path(token): Path<String>) -> Result<Json<DirectoryListing>, AppError> {
    let token = PathToken::from_encoded(token);
    Ok(Json(list_directory(&token)?))
}

async fn files_origin(State(state): State<AppState>) -> Result<Json<ContentOriginBody>, AppError> {
    let port = state
        .content
        .ok_or_else(|| AppError::Internal {
            message: "content origin is not available".into(),
        })?
        .port();
    Ok(Json(ContentOriginBody {
        api_version: API_VERSION,
        port,
    }))
}

async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<Value>, AppError> {
    let _intent_guard = lock_cancellation_intent(id).await?;
    let saved = call(&state.store, |reply| StoreMsg::GetCancellationRequest {
        task: id,
        reply,
    })
    .await?;
    if let Some(saved) = saved {
        return Ok(Json(crate::daemon::cancel_delivery::response(&saved)));
    }

    let local = call(&state.store, |reply| StoreMsg::GetTask { id, reply }).await?;
    if local.is_some() && local_task_already_cancelled(&state, id).await? {
        return Ok(Json(json!({
            "api_version": API_VERSION,
            "id": id,
            "status": ProcessStatus::Cancelled,
        })));
    }

    if local.is_none() {
        let owner = crate::daemon::inspection::cancellation_owner(&state, id).await?;
        let action = cancellation_action(
            owner,
            state.machine.identity.machine,
            id,
            uuid::Uuid::now_v7(),
        )?;
        match action {
            CancellationAction::AlreadyCancelled => {
                return Ok(Json(json!({
                    "api_version": API_VERSION,
                    "id": id,
                    "status": ProcessStatus::Cancelled,
                })));
            }
            CancellationAction::Execution(request) | CancellationAction::Resource(request) => {
                let (saved, _) = call(&state.store, |reply| StoreMsg::InsertCancellationRequest {
                    request: *request,
                    reply,
                })
                .await?;
                return Ok(Json(crate::daemon::cancel_delivery::response(&saved)));
            }
            CancellationAction::ForwardResource(target) => {
                let response =
                    crate::daemon::cluster::forward_resource_cancellation_intent(&state, &target)
                        .await?;
                return Ok(Json(response));
            }
        }
    }
    let result = call(&state.supervisor, |reply| SupervisorMsg::Cancel {
        id,
        reply,
    })
    .await?;
    match result {
        CancelResult::AlreadyTerminal(row) => Ok(Json(json!({
            "api_version": API_VERSION,
            "id": row.id,
            "status": row.status(),
        }))),
        CancelResult::CancelledQueued(_) => Ok(Json(json!({
            "api_version": API_VERSION,
            "id": id,
            "status": ProcessStatus::Cancelled,
        }))),
        CancelResult::SignalWorker(row) => Ok(Json(json!({
            "api_version": API_VERSION,
            "id": id,
            "status": row.status(),
        }))),
    }
}

async fn local_task_already_cancelled(state: &AppState, id: TaskId) -> Result<bool, AppError> {
    let machine = state.machine.identity.machine;
    let route = call(&state.store, |reply| StoreMsg::OriginRoute { id, reply }).await?;
    let owner = if let Some(route) = route {
        if route.task != id || route.origin_machine != machine {
            return Err(AppError::ClusterTaskConflict { task: id });
        }
        match route.submission {
            crate::submission::SubmissionState::Rejected { .. } => {
                return Err(AppError::ClusterTaskConflict { task: id });
            }
            crate::submission::SubmissionState::Resource { resource, phase } => {
                crate::cancellation::CancellationOwner::Resource(
                    crate::cancellation::ResourceCancellationTarget {
                        request_id: route.request,
                        task_id: route.task,
                        resource_id: resource,
                        origin_machine: route.origin_machine,
                        authority_machine: route.execution_machine,
                        phase,
                    },
                )
            }
            crate::submission::SubmissionState::AcceptanceUnknown
            | crate::submission::SubmissionState::Accepted => {
                crate::cancellation::CancellationOwner::Execution(
                    crate::cancellation::ExecutionCancellationTarget {
                        request_id: Some(route.request),
                        task: route.task,
                        origin_machine: route.origin_machine,
                        execution_machine: route.execution_machine,
                    },
                )
            }
        }
    } else {
        let identity = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
            id,
            reply,
        })
        .await?;
        if identity.is_none() {
            return Ok(false);
        }
        crate::daemon::inspection::cancellation_owner(state, id).await?
    };

    let action = cancellation_action(owner, machine, id, uuid::Uuid::now_v7())?;
    match action {
        CancellationAction::AlreadyCancelled => Ok(true),
        CancellationAction::Execution(request) if request.execution_machine == machine => Ok(false),
        CancellationAction::Execution(_) => Err(AppError::ClusterTaskConflict { task: id }),
        CancellationAction::Resource(_) => Err(AppError::ClusterTaskConflict { task: id }),
        CancellationAction::ForwardResource(_) => Err(AppError::ClusterTaskConflict { task: id }),
    }
}

enum CancellationAction {
    Execution(Box<crate::cancellation::CancellationRequest>),
    Resource(Box<crate::cancellation::CancellationRequest>),
    ForwardResource(crate::cancellation::ResourceCancellationTarget),
    AlreadyCancelled,
}

fn cancellation_action(
    owner: crate::cancellation::CancellationOwner,
    requester_machine: crate::machine::MachineId,
    task: TaskId,
    cancellation: uuid::Uuid,
) -> Result<CancellationAction, AppError> {
    match owner {
        crate::cancellation::CancellationOwner::Execution(target) => {
            if target.task != task {
                return Err(AppError::ClusterTaskConflict { task });
            }
            Ok(CancellationAction::Execution(Box::new(
                crate::cancellation::CancellationRequest {
                    requester_machine,
                    cancellation,
                    task,
                    origin_machine: target.origin_machine,
                    execution_machine: target.execution_machine,
                    target: crate::cancellation::CancellationTarget::Execution {
                        request_id: target.request_id,
                    },
                    delivery: crate::cancellation::CancellationDelivery::Pending,
                },
            )))
        }
        crate::cancellation::CancellationOwner::Resource(target) => {
            if target.task_id != task {
                return Err(AppError::ClusterTaskConflict { task });
            }
            if target.origin_machine != requester_machine {
                return Ok(CancellationAction::ForwardResource(target));
            }
            match &target.phase {
                crate::submission::ResourceRoutePhase::CancelledBeforeLaunch => {
                    return Ok(CancellationAction::AlreadyCancelled);
                }
                crate::submission::ResourceRoutePhase::Rejected { .. } => {
                    return Err(AppError::TaskNotStarted { task });
                }
                crate::submission::ResourceRoutePhase::Activated => {
                    return Ok(CancellationAction::Execution(Box::new(
                        crate::cancellation::CancellationRequest {
                            requester_machine,
                            cancellation,
                            task,
                            origin_machine: target.origin_machine,
                            execution_machine: target.authority_machine,
                            target: crate::cancellation::CancellationTarget::Execution {
                                request_id: Some(target.request_id),
                            },
                            delivery: crate::cancellation::CancellationDelivery::Pending,
                        },
                    )));
                }
                crate::submission::ResourceRoutePhase::AcceptanceUnknown
                | crate::submission::ResourceRoutePhase::Waiting => {}
            }
            Ok(CancellationAction::Resource(Box::new(
                crate::cancellation::CancellationRequest {
                    requester_machine,
                    cancellation,
                    task,
                    origin_machine: target.origin_machine,
                    execution_machine: target.authority_machine,
                    target: crate::cancellation::CancellationTarget::Resource(target),
                    delivery: crate::cancellation::CancellationDelivery::Pending,
                },
            )))
        }
    }
}

pub(super) async fn accept_task(
    state: &AppState,
    body: SubmitBody,
) -> Result<(TaskId, ProcessStatus), AppError> {
    let spec = body.spec;
    if spec.machine.is_some() {
        return Err(AppError::Usage {
            message: "local task acceptance received a remote machine selector".into(),
        });
    }
    spec::check_cwd(&spec.cwd)?;
    let binary = resolve_workload_binary(&spec.workload, &body.env.path, &spec.cwd)?;
    let id = TaskId::new();
    let paths = state.home.prepare_task(id)?;
    if let NormalizedWorkload::Agent(agent) = &spec.workload {
        crate::runner::write_task_files(&paths, &agent.prompt, agent.report_trailer)?;
    }
    let workload: Workload = persist_workload(&spec.workload);
    let row = store::new_queued_task(store::NewTask {
        id,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload,
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: body.env,
        binary,
    });
    call(&state.supervisor, |reply| SupervisorMsg::Launch {
        row: Box::new(row),
        spec: Box::new(spec),
        reply,
    })
    .await?;
    Ok((id, ProcessStatus::Queued))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;

    fn body(value: &Value) -> Request {
        HttpRequest::builder()
            .method("POST")
            .uri("/v1/tasks")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(value).unwrap()))
            .unwrap()
    }

    /// Run the socket extractor on its own: no actors, no database.
    async fn extract(value: &Value) -> Result<SubmitBody, AppError> {
        SpecBody::from_request(body(value), &()).await.map(|b| b.0)
    }

    fn valid() -> Value {
        json!({
            "spec": {
                "api_version": 1,
                "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
                "name": "test task",
                "cwd": "/tmp",
                "timeout": "4h",
                "workload": { "type": "task", "command": ["true"] }
            },
            "env": { "path": "/bin", "home": "/home/u" }
        })
    }

    #[test]
    fn resource_cancellation_uses_the_typed_path_before_activation() {
        let task = TaskId::new();
        for phase in [
            crate::submission::ResourceRoutePhase::AcceptanceUnknown,
            crate::submission::ResourceRoutePhase::Waiting,
        ] {
            let origin_machine = crate::machine::MachineId::new();
            let owner = crate::cancellation::CancellationOwner::Resource(
                crate::cancellation::ResourceCancellationTarget {
                    request_id: RequestId::new(),
                    task_id: task,
                    resource_id: crate::resource::ResourceId::new(),
                    origin_machine,
                    authority_machine: crate::machine::MachineId::new(),
                    phase,
                },
            );

            let CancellationAction::Resource(request) =
                cancellation_action(owner, origin_machine, task, uuid::Uuid::now_v7()).unwrap()
            else {
                panic!("a pre-activation resource request needs resource cancellation");
            };
            assert!(matches!(
                request.target,
                crate::cancellation::CancellationTarget::Resource(_)
            ));
        }
    }

    #[test]
    fn activated_resource_cancellation_uses_the_executor_task_path() {
        let task = TaskId::new();
        let request_id = RequestId::new();
        let origin_machine = crate::machine::MachineId::new();
        let authority_machine = crate::machine::MachineId::new();
        let owner = crate::cancellation::CancellationOwner::Resource(
            crate::cancellation::ResourceCancellationTarget {
                request_id,
                task_id: task,
                resource_id: crate::resource::ResourceId::new(),
                origin_machine,
                authority_machine,
                phase: crate::submission::ResourceRoutePhase::Activated,
            },
        );
        let CancellationAction::Execution(request) =
            cancellation_action(owner, origin_machine, task, uuid::Uuid::now_v7()).unwrap()
        else {
            panic!("activated resource work must use ordinary execution cancellation");
        };
        assert_eq!(request.execution_machine, authority_machine);
        assert_eq!(
            request.target,
            crate::cancellation::CancellationTarget::Execution {
                request_id: Some(request_id)
            }
        );
    }

    #[test]
    fn rejected_resource_route_does_not_build_a_cancellation_request() {
        let task = TaskId::new();
        let origin_machine = crate::machine::MachineId::new();
        let owner = crate::cancellation::CancellationOwner::Resource(
            crate::cancellation::ResourceCancellationTarget {
                request_id: RequestId::new(),
                task_id: task,
                resource_id: crate::resource::ResourceId::new(),
                origin_machine,
                authority_machine: crate::machine::MachineId::new(),
                phase: crate::submission::ResourceRoutePhase::Rejected {
                    reason: "resource request rejected".into(),
                },
            },
        );
        assert!(matches!(
            cancellation_action(
                owner,
                origin_machine,
                task,
                uuid::Uuid::now_v7(),
            ),
            Err(AppError::TaskNotStarted { task: found }) if found == task
        ));
    }

    #[test]
    fn cancelled_resource_route_returns_its_retained_result_without_executor_request() {
        let task = TaskId::new();
        let origin_machine = crate::machine::MachineId::new();
        let owner = crate::cancellation::CancellationOwner::Resource(
            crate::cancellation::ResourceCancellationTarget {
                request_id: RequestId::new(),
                task_id: task,
                resource_id: crate::resource::ResourceId::new(),
                origin_machine,
                authority_machine: crate::machine::MachineId::new(),
                phase: crate::submission::ResourceRoutePhase::CancelledBeforeLaunch,
            },
        );

        assert!(matches!(
            cancellation_action(owner, origin_machine, task, uuid::Uuid::now_v7(),),
            Ok(CancellationAction::AlreadyCancelled)
        ));
    }

    #[test]
    fn cancellation_owner_task_mismatch_is_a_conflict() {
        let task = TaskId::new();
        let owner = crate::cancellation::CancellationOwner::Resource(
            crate::cancellation::ResourceCancellationTarget {
                request_id: RequestId::new(),
                task_id: task,
                resource_id: crate::resource::ResourceId::new(),
                origin_machine: crate::machine::MachineId::new(),
                authority_machine: crate::machine::MachineId::new(),
                phase: crate::submission::ResourceRoutePhase::Waiting,
            },
        );

        assert!(matches!(
            cancellation_action(
                owner,
                crate::machine::MachineId::new(),
                TaskId::new(),
                uuid::Uuid::now_v7(),
            ),
            Err(AppError::ClusterTaskConflict { .. })
        ));
    }

    #[test]
    fn ordinary_execution_cancellation_keeps_the_generic_path() {
        let task = TaskId::new();
        let request_id = RequestId::new();
        let origin_machine = crate::machine::MachineId::new();
        let execution_machine = crate::machine::MachineId::new();
        let cancellation = uuid::Uuid::now_v7();
        let owner = crate::cancellation::CancellationOwner::Execution(
            crate::cancellation::ExecutionCancellationTarget {
                request_id: Some(request_id),
                task,
                origin_machine,
                execution_machine,
            },
        );

        let CancellationAction::Execution(request) =
            cancellation_action(owner, crate::machine::MachineId::new(), task, cancellation)
                .unwrap()
        else {
            panic!("ordinary execution must use its generic cancellation intent");
        };
        assert_eq!(request.task, task);
        assert_eq!(request.origin_machine, origin_machine);
        assert_eq!(request.execution_machine, execution_machine);
        assert_eq!(request.cancellation, cancellation);
        assert_eq!(
            request.target,
            crate::cancellation::CancellationTarget::Execution {
                request_id: Some(request_id),
            }
        );
    }

    #[track_caller]
    fn invalid_spec(err: AppError) -> (String, Value) {
        match err {
            AppError::InvalidSpec { pointer, value, .. } => (pointer, value),
            other => panic!("expected invalid_spec, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn valid_envelope_is_accepted() {
        let body = extract(&valid()).await.unwrap();
        assert_eq!(body.env.path, "/bin");
        assert_eq!(body.spec.api_version, 1);
        assert_eq!(body.request, None);
    }

    #[tokio::test]
    async fn local_request_id_is_rejected() {
        let mut value = valid();
        value["request_id"] = json!("01a0ab97-a7aa-7463-a5b0-8d500e40e431");
        let (pointer, found) = invalid_spec(extract(&value).await.unwrap_err());
        assert_eq!(pointer, "/request_id");
        assert_eq!(found, value["request_id"]);
    }

    #[tokio::test]
    async fn remote_request_id_is_generated_or_preserved() {
        let mut value = valid();
        value["spec"]["machine"] = json!("code");
        let generated = extract(&value).await.unwrap().request.unwrap();
        assert_ne!(generated.0, uuid::Uuid::nil());

        let explicit = uuid::Uuid::parse_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap();
        value["request_id"] = json!(explicit);
        assert_eq!(
            extract(&value).await.unwrap().request,
            Some(RequestId(explicit))
        );
    }

    #[test]
    fn local_submit_response_omits_request_id() {
        let id = TaskId::new();
        let response = SubmitResponse {
            api_version: API_VERSION,
            id,
            task_id: id,
            request_id: None,
            status: ProcessStatus::Queued,
        };
        let value = serde_json::to_value(response).unwrap();
        assert!(value.get("request_id").is_none());

        let request_id = RequestId(uuid::Uuid::now_v7());
        let response = SubmitResponse {
            api_version: API_VERSION,
            id,
            task_id: id,
            request_id: Some(request_id),
            status: ProcessStatus::Queued,
        };
        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["request_id"], json!(request_id));
    }

    #[tokio::test]
    async fn unknown_top_level_key_is_rejected() {
        let mut value = valid();
        value["retries"] = json!(3);
        let err = extract(&value).await.unwrap_err();
        assert_eq!(err.code(), "invalid_spec");
        let (pointer, _) = invalid_spec(err);
        assert_eq!(pointer, "/retries");
    }

    #[tokio::test]
    async fn unknown_key_pointer_is_rfc_6901_escaped() {
        let mut value = valid();
        value["a/b~c"] = json!(true);
        let (pointer, _) = invalid_spec(extract(&value).await.unwrap_err());
        assert_eq!(pointer, "/a~1b~0c");
    }

    #[tokio::test]
    async fn missing_spec_and_env_point_at_the_missing_key() {
        let mut value = valid();
        value.as_object_mut().unwrap().remove("env");
        let (pointer, _) = invalid_spec(extract(&value).await.unwrap_err());
        assert_eq!(pointer, "/env");

        let mut value = valid();
        value.as_object_mut().unwrap().remove("spec");
        let (pointer, _) = invalid_spec(extract(&value).await.unwrap_err());
        assert_eq!(pointer, "/spec");
    }

    #[tokio::test]
    async fn nested_failures_keep_their_prefix_and_value() {
        let mut value = valid();
        value["env"]["path"] = json!(7);
        let (pointer, found) = invalid_spec(extract(&value).await.unwrap_err());
        assert_eq!(pointer, "/env/path");
        assert_eq!(found, json!(7));

        let mut value = valid();
        value["spec"]["workload"]["command"] = json!([""]);
        let (pointer, _) = invalid_spec(extract(&value).await.unwrap_err());
        assert_eq!(pointer, "/spec/workload/command/0");

        let mut value = valid();
        value["spec"]["timeout"] = json!("29m");
        let (pointer, _) = invalid_spec(extract(&value).await.unwrap_err());
        assert_eq!(pointer, "/spec/timeout");
    }

    #[tokio::test]
    async fn unknown_env_key_is_rejected() {
        let mut value = valid();
        value["env"]["token"] = json!("secret");
        let (pointer, _) = invalid_spec(extract(&value).await.unwrap_err());
        assert_eq!(pointer, "/env/token");
    }

    #[tokio::test]
    async fn malformed_json_is_an_invalid_spec_not_an_axum_rejection() {
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/tasks")
            .body(Body::from("{not json"))
            .unwrap();
        let err = SpecBody::from_request(req, &()).await.err().unwrap();
        assert_eq!(err.code(), "invalid_spec");
        assert_eq!(err.http_status(), http::StatusCode::BAD_REQUEST);
    }
}
