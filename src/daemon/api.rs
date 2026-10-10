//! Axum routes and error mapping

pub mod views;

use std::path::PathBuf;

use axum::extract::{FromRequest, Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::callback::last_event_for_row;
use crate::cancellation::{CancellationOwner, CancellationRoute};
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::daemon::api::views::{
    ContainerDetail, DependencyView, LogTail, StatusBody, TaskDetail, TaskFollowupSource, TaskList,
    TaskSummary,
};
use crate::daemon::cancel_delivery::CancelResponse;
use crate::daemon::{AppState, web};
use crate::dependency::TaskDependencies;
use crate::domain::{
    API_VERSION, AgentKind, ProcessStatus, TaskEnv, TaskId, TaskIdentity, TaskStatus, ThreadId,
    Workload,
};
use crate::error::{AppError, FollowupBlocker};
use crate::files::{
    ContentOriginBody, DirectoryListing, PathToken, ResolveBody, ResolvedPath, list_directory,
    resolve_absolute_path,
};
use crate::invocation::{ManagedEnvironmentPreview, StdinPolicy, invocation_from_normalized};
use crate::spec::{self, NormalizedSpec, NormalizedWorkload};
use crate::store::CancelResult;
use crate::submission::RequestId;

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.http_status();
        (status, Json(self.to_json())).into_response()
    }
}

/// Routes that only read state. Safe to expose on the TCP listener
pub fn read_routes() -> Router<AppState> {
    Router::new()
        .merge(crate::daemon::queue_api::read_routes())
        .merge(crate::daemon::fleet_api::read_routes())
        .merge(crate::daemon::fleet_tasks::read_routes())
        .merge(crate::daemon::thread_titles::read_routes())
        .merge(crate::daemon::claude_threads::read_routes())
        .merge(crate::daemon::usage_api::read_routes())
        .route("/v1/status", get(status))
        .route("/v1/tasks", get(list))
        .route("/v1/tasks/{id}", get(show))
        .route("/v1/tasks/{id}/log", get(log))
        .route("/v1/files/resolve", post(files_resolve))
        .route("/v1/files/origin", get(files_origin))
        .route("/v1/files/{token}", get(files_list))
}

/// Routes that change state. Unix socket only: the socket is mode 0600, while a
/// loopback port is reachable from any page the user has open
pub fn write_routes() -> Router<AppState> {
    Router::new()
        .merge(crate::daemon::queue_api::submit_routes())
        .merge(crate::daemon::queue_api::control_routes())
        .merge(crate::daemon::claude_threads::control_routes())
        .route("/v1/tasks", post(submit))
        .route("/v1/tasks/dry-run", post(dry_run))
        .route("/v1/tasks/{id}/cancel", post(cancel))
}

/// Full API for the Unix socket
pub fn socket_router(state: AppState) -> Router {
    read_routes()
        .merge(write_routes())
        .route("/v1/tasks/{id}/followup-source", get(followup_source))
        .merge(crate::daemon::fleet_api::socket_routes())
        .with_state(state)
}

async fn followup_source(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<TaskFollowupSource>, AppError> {
    if let Some(row) = call(&state.store, |reply| StoreMsg::GetTask { id, reply }).await? {
        return followup_source_from_workload(id, &row.workload).map(Json);
    }
    let route = call(&state.store, |reply| StoreMsg::OriginRoute { id, reply })
        .await?
        .ok_or(AppError::TaskNotFound { id })?;
    match &route.spec.workload {
        NormalizedWorkload::Agent(agent) if agent.agent == AgentKind::Codex => {
            Ok(Json(TaskFollowupSource {
                api_version: API_VERSION,
                model: agent.model.clone(),
                extra_args: agent.extra_args.clone(),
                report_trailer: agent.report_trailer,
            }))
        }
        _ => Err(AppError::FollowupUnavailable {
            task: id,
            reason: FollowupBlocker::NotCodex,
        }),
    }
}

fn followup_source_from_workload(
    id: TaskId,
    workload: &Workload,
) -> Result<TaskFollowupSource, AppError> {
    let Workload::Agent(agent) = workload else {
        return Err(AppError::FollowupUnavailable {
            task: id,
            reason: FollowupBlocker::NotCodex,
        });
    };
    if agent.agent.kind != AgentKind::Codex {
        return Err(AppError::FollowupUnavailable {
            task: id,
            reason: FollowupBlocker::NotCodex,
        });
    }
    Ok(TaskFollowupSource {
        api_version: API_VERSION,
        model: agent.agent.model.clone(),
        extra_args: agent.extra_args.clone(),
        report_trailer: agent.report_trailer,
    })
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
/// envelope, the normalized spec, and the captured env are each checked
#[derive(Debug)]
pub(super) struct SubmitBody {
    pub(super) spec: NormalizedSpec,
    pub(super) env: TaskEnv,
    pub(super) request: RequestId,
    pub(super) callback_cwd: Option<PathBuf>,
    /// Origin-only dependencies, kept beside the spec so no executor receives them
    pub(super) after: Option<TaskDependencies>,
}

/// Top-level socket envelope. `spec` and `env` stay as `Value` so each can be
/// parsed with its own pointer prefix, but `deny_unknown_fields` here is what
/// makes an unknown top-level key an `invalid_spec` instead of silent input
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
    #[serde(default)]
    after: Option<Value>,
}

/// Application-owned JSON body extractor that maps failures to `AppError`
struct SpecBody(SubmitBody);

impl<S> FromRequest<S> for SpecBody
where
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let value = json_body(req, state).await?;
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
        let after = envelope.after.as_ref().map(spec::parse_after).transpose()?;
        // a caller without its own request UUID gets a fresh one, so every task has a retry identity
        let request = envelope.request_id.unwrap_or_default();
        Ok(Self(SubmitBody {
            spec,
            env,
            request,
            callback_cwd: envelope.callback_cwd,
            after,
        }))
    }
}

/// Read a request body as JSON; a body that is unreadable or not JSON is an
/// `invalid_spec` at the root, like any other refused field
pub(super) async fn json_body<S: Send + Sync>(req: Request, state: &S) -> Result<Value, AppError> {
    let bytes = bytes::Bytes::from_request(req, state)
        .await
        .map_err(|err| AppError::InvalidSpec {
            pointer: String::new(),
            value: Value::Null,
            message: format!("invalid request body: {err}"),
        })?;
    serde_json::from_slice(&bytes).map_err(|err| AppError::InvalidSpec {
        pointer: String::new(),
        value: Value::Null,
        message: format!("invalid JSON: {err}"),
    })
}

pub(super) fn missing_field(pointer: &str, name: &str) -> AppError {
    AppError::InvalidSpec {
        pointer: pointer.into(),
        value: Value::Null,
        message: format!("missing field `{name}`"),
    }
}

/// Build an `invalid_spec` at the failing path, rooted at `prefix`
pub(super) fn invalid_at(
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

/// Re-root an `invalid_spec` pointer under `prefix`
pub(super) fn prefix(prefix: &str, err: AppError) -> AppError {
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
    request_id: RequestId,
    status: TaskStatus,
}

async fn submit(
    State(state): State<AppState>,
    SpecBody(body): SpecBody,
) -> Result<(StatusCode, Json<SubmitResponse>), AppError> {
    let request_id = body.request;
    let (id, status) = crate::daemon::dependencies::submit(&state, body).await?;
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
    /// Dependencies and their state when the spec has `after`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) after: Option<Vec<DependencyView>>,
}

async fn dry_run(
    State(state): State<AppState>,
    SpecBody(body): SpecBody,
) -> Result<Json<DryRunResponse>, AppError> {
    let after = match &body.after {
        Some(after) => Some(crate::daemon::dependencies::preview(&state, after).await?),
        None => None,
    };
    let spec = body.spec;
    let mut response = if spec.machine.is_some() {
        crate::daemon::origin_submit::dry_run(&state, spec, body.env).await?
    } else {
        local_dry_run(&state, spec, body.env)?
    };
    response.after = after;
    Ok(Json(response))
}

pub(super) fn local_dry_run(
    state: &AppState,
    spec: NormalizedSpec,
    env: TaskEnv,
) -> Result<DryRunResponse, AppError> {
    spec::check_spec_host(&spec)?;
    let prompt_feed = agent_feed_placeholder(state.home.root(), &spec.workload);
    let invocation = invocation_from_normalized(
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
        after: None,
    })
}

/// Deterministic dry-run feed path for any agent. Only Grok puts it in argv;
/// Codex and Claude keep it as the stdin evidence path
fn agent_feed_placeholder(
    root: &std::path::Path,
    workload: &NormalizedWorkload,
) -> Option<PathBuf> {
    match workload {
        NormalizedWorkload::Agent(_) => {
            Some(root.join("tasks").join("<task-id>").join("prompt.feed.txt"))
        }
        NormalizedWorkload::Task(_) | NormalizedWorkload::Container(_) => None,
    }
}

/// `?status=a,b&thread=<uuid>` filter shared by the local and fleet task lists
#[derive(Debug, Default, Deserialize)]
pub(super) struct ListQuery {
    #[serde(default)]
    pub(super) status: Option<String>,
    #[serde(default)]
    pub(super) thread: Option<String>,
}

/// Parsed task-list filter. An empty status list matches every status
#[derive(Debug, Clone, Default)]
pub(super) struct TaskFilter {
    pub(super) statuses: Vec<TaskStatus>,
    pub(super) thread: Option<ThreadId>,
}

impl TaskFilter {
    /// Parse the query form. An unknown status or a malformed thread is a usage error
    pub(super) fn parse(query: ListQuery) -> Result<Self, AppError> {
        let mut statuses = Vec::new();
        for part in query.status.as_deref().unwrap_or_default().split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            statuses.push(TaskStatus::parse(part).map_err(|_| AppError::Usage {
                message: format!("invalid status {part}"),
            })?);
        }
        let thread = query
            .thread
            .map(|raw| raw.parse::<ThreadId>())
            .transpose()?;
        Ok(Self { statuses, thread })
    }

    /// Whether a task with this status and thread passes the filter
    fn matches(&self, status: TaskStatus, thread: ThreadId) -> bool {
        (self.statuses.is_empty() || self.statuses.contains(&status))
            && self.thread.is_none_or(|wanted| wanted == thread)
    }

    /// Query pairs that parse back into this filter, `&`-terminated when non-empty
    pub(super) fn query_prefix(&self) -> String {
        let mut prefix = String::new();
        if !self.statuses.is_empty() {
            let statuses: Vec<&str> = self.statuses.iter().map(TaskStatus::as_str).collect();
            prefix.push_str(&format!("status={}&", statuses.join(",")));
        }
        if let Some(thread) = self.thread {
            prefix.push_str(&format!("thread={thread}&"));
        }
        prefix
    }
}

/// Public summaries of the tasks this daemon stores, in id order
///
/// Tasks held here, or ended here before launch, have no task row; their
/// origin route is the only record, so they are listed from it
pub(super) async fn local_task_summaries(
    state: &AppState,
    filter: TaskFilter,
) -> Result<Vec<TaskSummary>, AppError> {
    let processes: Vec<ProcessStatus> = filter
        .statuses
        .iter()
        .filter_map(|status| status.process())
        .collect();
    // an empty status list reads every row, so a held-only filter reads none
    let rows = if filter.statuses.is_empty() || !processes.is_empty() {
        call(&state.store, |reply| StoreMsg::ListTasks {
            statuses: processes,
            thread: filter.thread,
            reply,
        })
        .await?
    } else {
        Vec::new()
    };
    let ids = rows.iter().map(|row| row.id).collect();
    let presentations = call(&state.store, |reply| StoreMsg::TaskPresentations {
        ids,
        reply,
    })
    .await?;
    let mut tasks = TaskList::from_rows(&rows, &presentations).tasks;
    let unlaunched = call(&state.store, |reply| StoreMsg::UnlaunchedTasks { reply }).await?;
    for task in unlaunched {
        let after = crate::daemon::dependencies::views(state, &task.held.after).await?;
        if let Some(summary) = TaskSummary::from_unlaunched(&task, after)
            && filter.matches(summary.status, summary.thread)
        {
            tasks.push(summary);
        }
    }
    tasks.sort_by_key(|task| task.id.0);
    Ok(tasks)
}

async fn list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<TaskList>, AppError> {
    let tasks = local_task_summaries(&state, TaskFilter::parse(query)?).await?;
    Ok(Json(TaskList {
        api_version: API_VERSION,
        tasks,
    }))
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
    let parking = call(&state.store, |reply| StoreMsg::Parking { id, reply }).await?;
    let mut last_event = last_event_for_row(&row, &reports, evidence.clone(), parking.as_ref());
    if row.state.is_terminal()
        && let Some(event) = &mut last_event
    {
        event.usage = call(&state.store, |reply| StoreMsg::TaskUsage { id, reply }).await?;
    }
    let container = if matches!(row.workload, Workload::Container(_)) {
        let record = call(&state.store, |reply| StoreMsg::TaskContainer { id, reply }).await?;
        ContainerDetail::from_row(&row, record.as_ref())
    } else {
        None
    };
    Ok(TaskDetail {
        api_version: API_VERSION,
        summary: TaskSummary::from_row(&row, presentations.get(&id)),
        reports,
        output_log: state.home.task_paths(id).output,
        evidence,
        last_event,
        container,
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
) -> Result<Json<CancelResponse>, AppError> {
    let _intent_guard = state.locks.cancellation_intents.lock(id).await;
    let saved = call(&state.store, |reply| StoreMsg::GetCancellationRequest {
        task: id,
        reply,
    })
    .await?;
    if let Some(saved) = saved {
        return Ok(Json(CancelResponse::intent(&saved)));
    }
    if let Some(response) = crate::daemon::chains::cancel(&state, id).await? {
        return Ok(Json(response));
    }
    if let Some(response) = crate::daemon::dependencies::cancel(&state, id).await? {
        return Ok(Json(response));
    }

    let local = call(&state.store, |reply| StoreMsg::GetTask { id, reply }).await?;
    if local.is_none() {
        let owner = crate::daemon::inspection::cancellation_owner(&state, id).await?;
        let request = owner
            .request(state.machine.identity.machine, id, uuid::Uuid::now_v7())
            .map_err(|refusal| refusal.into_error(id))?;
        let (saved, _) = call(&state.store, |reply| StoreMsg::InsertCancellationRequest {
            request,
            reply,
        })
        .await?;
        return Ok(Json(CancelResponse::intent(&saved)));
    }
    check_local_cancellation_owner(&state, id).await?;
    let result = call(&state.supervisor, |reply| SupervisorMsg::Cancel {
        id,
        reply,
    })
    .await?;
    let response = match result {
        CancelResult::AlreadyTerminal(row) => CancelResponse::status(row.id, row.status()),
        CancelResult::CancelledQueued(_) => CancelResponse::status(id, ProcessStatus::Cancelled),
        CancelResult::SignalWorker(row) => CancelResponse::status(id, row.status()),
    };
    Ok(Json(response))
}

/// Refuse to cancel a local row through the supervisor unless this machine executes it
async fn check_local_cancellation_owner(state: &AppState, id: TaskId) -> Result<(), AppError> {
    let machine = state.machine.identity.machine;
    let route = call(&state.store, |reply| StoreMsg::OriginRoute { id, reply }).await?;
    let owner = if let Some(route) = route {
        CancellationOwner::from_route(id, machine, CancellationRoute::from(&route))
            .map_err(|refusal| refusal.into_error(id))?
    } else {
        let identity = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
            id,
            reply,
        })
        .await?;
        if identity.is_none() {
            return Ok(());
        }
        crate::daemon::inspection::cancellation_owner(state, id).await?
    };

    if owner.execution_machine != machine {
        return Err(AppError::ClusterTaskConflict { task: id });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SpecBody, SubmitBody, SubmitResponse};
    use crate::domain::{API_VERSION, ProcessStatus, TaskId};
    use crate::error::AppError;
    use crate::submission::RequestId;
    use axum::body::Body;
    use axum::extract::{FromRequest, Request};
    use axum::http::Request as HttpRequest;
    use serde_json::{Value, json};

    fn body(value: &Value) -> Request {
        HttpRequest::builder()
            .method("POST")
            .uri("/v1/tasks")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(value).unwrap()))
            .unwrap()
    }

    /// Run the socket extractor on its own: no actors, no database
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
        assert_ne!(body.request.0, uuid::Uuid::nil());
    }

    #[tokio::test]
    async fn request_id_is_generated_or_preserved() {
        let explicit = uuid::Uuid::parse_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap();
        for machine in [None, Some("code")] {
            let mut value = valid();
            if let Some(machine) = machine {
                value["spec"]["machine"] = json!(machine);
            }
            let generated = extract(&value).await.unwrap().request;
            assert_ne!(generated.0, uuid::Uuid::nil());

            value["request_id"] = json!(explicit);
            assert_eq!(extract(&value).await.unwrap().request, RequestId(explicit));
        }
    }

    #[test]
    fn submit_response_carries_request_id() {
        let id = TaskId::new();
        let request_id = RequestId(uuid::Uuid::now_v7());
        let response = SubmitResponse {
            api_version: API_VERSION,
            id,
            task_id: id,
            request_id,
            status: ProcessStatus::Queued.into(),
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
    async fn after_rides_beside_the_spec_with_its_own_pointer() {
        let mut value = valid();
        let task = TaskId::new();
        value["after"] = json!([task]);
        let body = extract(&value).await.unwrap();
        assert_eq!(body.after.unwrap().tasks(), &[task]);

        value["after"] = json!([]);
        let (pointer, _) = invalid_spec(extract(&value).await.unwrap_err());
        assert_eq!(pointer, "/after");
        // the normalized spec has no `after` of its own
        let mut value = valid();
        value["spec"]["after"] = json!([task]);
        let (pointer, _) = invalid_spec(extract(&value).await.unwrap_err());
        assert_eq!(pointer, "/spec/after");
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
