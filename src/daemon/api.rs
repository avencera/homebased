//! Axum routes and error mapping.

pub mod views;

use std::path::PathBuf;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::agents::{ArgvInputs, build_argv, resolve_binary};
use crate::callback::last_event_for_row;
use crate::daemon::actors::{CALL_TIMEOUT, StoreMsg, SupervisorMsg, call_store, flatten_call};
use crate::daemon::api::views::{LogTail, StatusBody, TaskDetail, TaskList, TaskSummary};
use crate::daemon::{AppState, spawn_and_watch, web};
use crate::domain::{API_VERSION, ProcessStatus, TaskEnv, TaskId, ThreadId};
use crate::error::AppError;
use crate::spec::{self, NormalizedSpec};
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
    let in_flight = call_store(&state.store, |reply| StoreMsg::InFlightCount { reply }).await?;
    Ok(Json(StatusBody {
        api_version: API_VERSION,
        version: env!("CARGO_PKG_VERSION"),
        pid: std::process::id(),
        socket: state.home.sock_path().display().to_string(),
        web: state.web.map(web::url_for),
        in_flight,
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitBody {
    spec: NormalizedSpec,
    env: TaskEnv,
}

#[derive(Serialize)]
struct SubmitResponse {
    api_version: u32,
    id: TaskId,
    status: ProcessStatus,
}

async fn submit(
    State(state): State<AppState>,
    Json(body): Json<SubmitBody>,
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
}

async fn dry_run(
    State(state): State<AppState>,
    Json(body): Json<SubmitBody>,
) -> Result<Json<DryRunResponse>, AppError> {
    let spec = body.spec;
    check_api_version(&spec)?;
    spec::check_cwd(&spec.cwd)?;
    let binary = resolve_binary(spec.agent, &body.env.path, &spec.cwd)?;
    let prompt_file = grok_placeholder(state.home.root(), &spec);
    let argv = build_argv(&ArgvInputs::from(&spec), &binary, prompt_file.as_deref());
    Ok(Json(DryRunResponse {
        api_version: API_VERSION,
        spec,
        argv: argv.to_vec(),
    }))
}

fn grok_placeholder(root: &std::path::Path, spec: &NormalizedSpec) -> Option<PathBuf> {
    if spec.agent == crate::domain::AgentKind::Grok {
        Some(root.join("tasks").join("<task-id>").join("prompt.feed.txt"))
    } else {
        None
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
    let rows = call_store(&state.store, |reply| StoreMsg::ListTasks {
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
    let row = call_store(&state.store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .ok_or(AppError::TaskNotFound { id })?;
    let reports = call_store(&state.store, |reply| StoreMsg::Reports { id, reply }).await?;
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
    // the row decides whether the task exists; `output.log` only appears once
    // the worker has spawned the agent
    if call_store(&state.store, |reply| StoreMsg::GetTask { id, reply })
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

async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<Value>, AppError> {
    let result = flatten_call(
        state
            .supervisor
            .call(
                |reply| SupervisorMsg::Cancel { id, reply },
                Some(CALL_TIMEOUT),
            )
            .await,
    )?;
    match result {
        CancelResult::AlreadyTerminal(row) => Ok(Json(json!({
            "api_version": API_VERSION,
            "id": row.id,
            "status": row.status(),
        }))),
        CancelResult::CancelledQueued(_) => Ok(Json(json!({
            "api_version": API_VERSION,
            "id": id,
            "status": crate::domain::ProcessStatus::Cancelled,
        }))),
        CancelResult::SignalWorker(row) => Ok(Json(json!({
            "api_version": API_VERSION,
            "id": id,
            "status": row.status(),
        }))),
    }
}

fn check_api_version(spec: &NormalizedSpec) -> Result<(), AppError> {
    if spec.api_version == API_VERSION {
        return Ok(());
    }
    Err(AppError::InvalidSpec {
        pointer: "/spec/api_version".into(),
        value: json!(spec.api_version),
        message: format!("api_version must be {API_VERSION}"),
    })
}

async fn accept_task(
    state: &AppState,
    body: SubmitBody,
) -> Result<(TaskId, ProcessStatus), AppError> {
    let spec = body.spec;
    check_api_version(&spec)?;
    spec::check_cwd(&spec.cwd)?;
    let binary = resolve_binary(spec.agent, &body.env.path, &spec.cwd)?;
    let id = TaskId::new();
    let paths = state.home.prepare_task(id)?;
    crate::runner::write_task_files(&paths, &spec.prompt, spec.report_trailer)?;
    let row = store::new_queued_task(store::NewTask {
        id,
        thread: spec.thread,
        agent: spec.agent(),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        extra_args: spec.extra_args.clone(),
        report_trailer: spec.report_trailer,
        env: body.env,
        binary,
    });
    call_store(&state.store, |reply| StoreMsg::InsertTask {
        row: Box::new(row),
        reply,
    })
    .await?;
    spawn_and_watch(state, id).await?;
    Ok((id, ProcessStatus::Queued))
}

impl NormalizedSpec {
    fn agent(&self) -> crate::domain::Agent {
        crate::domain::Agent::new(self.agent, self.model.clone())
    }
}
