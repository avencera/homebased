//! Axum routes and error mapping.

pub mod views;

use std::path::PathBuf;

use axum::extract::{FromRequest, Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::callback::last_event_for_row;
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::daemon::api::views::{LogTail, StatusBody, TaskDetail, TaskList, TaskSummary};
use crate::daemon::{AppState, web};
use crate::domain::{API_VERSION, ProcessStatus, TaskEnv, TaskId, ThreadId, Workload};
use crate::error::AppError;
use crate::files::{
    ContentOriginBody, DirectoryListing, PathToken, ResolveBody, ResolvedPath, list_directory,
    resolve_absolute_path,
};
use crate::invocation::{
    StdinPolicy, invocation_from_normalized, persist_workload, resolve_workload_binary,
};
use crate::spec::{self, NormalizedSpec, NormalizedWorkload};
use crate::store::{self, CancelResult};

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.http_status();
        (status, Json(self.to_json())).into_response()
    }
}

/// Routes that only read state. Safe to expose on the TCP listener.
pub fn read_routes() -> Router<AppState> {
    Router::new()
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
    read_routes().merge(write_routes()).with_state(state)
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
struct SubmitBody {
    spec: NormalizedSpec,
    env: TaskEnv,
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
        Ok(Self(SubmitBody { spec, env }))
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
    status: ProcessStatus,
}

async fn submit(
    State(state): State<AppState>,
    SpecBody(body): SpecBody,
) -> Result<(StatusCode, Json<SubmitResponse>), AppError> {
    let (id, status) = accept_task(&state, body).await?;
    Ok((
        StatusCode::OK,
        Json(SubmitResponse {
            api_version: API_VERSION,
            id,
            status,
        }),
    ))
}

#[derive(Serialize)]
struct DryRunResponse {
    api_version: u32,
    spec: NormalizedSpec,
    argv: Vec<String>,
    stdin: StdinPolicy,
}

async fn dry_run(
    State(state): State<AppState>,
    SpecBody(body): SpecBody,
) -> Result<Json<DryRunResponse>, AppError> {
    let spec = body.spec;
    spec::check_cwd(&spec.cwd)?;
    let prompt_feed = agent_feed_placeholder(state.home.root(), &spec.workload);
    let invocation = invocation_from_normalized(
        &spec.workload,
        &body.env.path,
        &spec.cwd,
        prompt_feed.as_deref(),
    )?;
    Ok(Json(DryRunResponse {
        api_version: API_VERSION,
        spec,
        argv: invocation.to_vec(),
        stdin: invocation.stdin,
    }))
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
    Ok(Json(TaskList::from_rows(&rows)))
}

async fn show(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<TaskDetail>, AppError> {
    let row = call(&state.store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .ok_or(AppError::TaskNotFound { id })?;
    let reports = call(&state.store, |reply| StoreMsg::Reports { id, reply }).await?;
    let evidence = state.home.task_dir(id);
    let last_event = last_event_for_row(&row, &reports, evidence.clone());
    Ok(Json(TaskDetail {
        api_version: API_VERSION,
        summary: TaskSummary::from(&row),
        reports,
        output_log: state.home.task_paths(id).output,
        evidence,
        last_event,
    }))
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
) -> Result<Json<LogTail>, AppError> {
    if call(&state.store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .is_none()
    {
        return Err(AppError::TaskNotFound { id });
    }
    let tail = state.home.task_paths(id).read_output(query.tail)?;
    let (log, truncated) = match tail {
        Some(output) => (output.text, output.truncated),
        None => (String::new(), false),
    };
    Ok(Json(LogTail {
        api_version: API_VERSION,
        id,
        log,
        truncated,
    }))
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

async fn accept_task(
    state: &AppState,
    body: SubmitBody,
) -> Result<(TaskId, ProcessStatus), AppError> {
    let spec = body.spec;
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
        name: spec.name.clone(),
        thread: spec.thread,
        workload,
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: body.env,
        binary,
    });
    call(&state.supervisor, |reply| SupervisorMsg::Launch {
        row: Box::new(row),
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
