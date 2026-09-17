//! Axum routes and error mapping.

use std::path::PathBuf;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::agents::{build_argv, resolve_binary};
use crate::callback::last_event_for_row;
use crate::daemon::{lock_store, spawn_and_watch, AppState};
use crate::domain::{ProcessStatus, TaskEnv, TaskId, ThreadId, API_VERSION};
use crate::error::AppError;
use crate::spec::{self, NormalizedSpec, SubmitSpec};
use crate::store::{self, CancelResult};

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.http_status();
        (status, Json(self.to_json())).into_response()
    }
}

/// Build the HTTP router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/status", get(status))
        .route("/v1/tasks", post(submit).get(list))
        .route("/v1/tasks/dry-run", post(dry_run))
        .route("/v1/tasks/{id}", get(show))
        .route("/v1/tasks/{id}/cancel", post(cancel))
        .with_state(state)
}

#[derive(Serialize)]
struct StatusBody {
    api_version: u32,
    pid: u32,
    socket: String,
    in_flight: usize,
}

async fn status(State(state): State<AppState>) -> Result<Json<StatusBody>, AppError> {
    let store = lock_store(&state.store);
    let in_flight = store.in_flight_count()?;
    Ok(Json(StatusBody {
        api_version: API_VERSION,
        pid: std::process::id(),
        socket: state.home.sock_path().display().to_string(),
        in_flight,
    }))
}

#[derive(Deserialize)]
struct SubmitBody {
    spec: Value,
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
    let (id, status) = accept_task(&state, body, false)?;
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
    let spec = parse_incoming(body.spec)?;
    spec::check_cwd(&spec.cwd)?;
    let binary = resolve_binary(spec.agent, &body.env.path, &spec.cwd)?;
    let prompt_file = grok_placeholder(&state, &spec);
    let argv = build_argv(&spec, &binary, prompt_file.as_deref());
    let _ = state;
    Ok(Json(DryRunResponse {
        api_version: API_VERSION,
        spec,
        argv: argv.to_vec(),
    }))
}

fn grok_placeholder(state: &AppState, spec: &NormalizedSpec) -> Option<PathBuf> {
    if spec.agent == crate::domain::AgentKind::Grok {
        Some(
            state
                .home
                .root()
                .join("tasks")
                .join("<task-id>")
                .join("prompt.feed.txt"),
        )
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
) -> Result<Json<Value>, AppError> {
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
    let store = lock_store(&state.store);
    let rows = store.list_tasks(&statuses, thread)?;
    let tasks: Vec<Value> = rows
        .iter()
        .map(|row| {
            json!({
                "id": row.id,
                "status": row.status,
                "agent": row.agent.kind,
                "model": row.agent.model,
                "thread": row.thread,
                "cwd": row.cwd,
                "pid": row.pid,
                "callback": row.callback_status,
            })
        })
        .collect();
    Ok(Json(json!({
        "api_version": API_VERSION,
        "tasks": tasks,
    })))
}

async fn show(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<Value>, AppError> {
    let store = lock_store(&state.store);
    let row = store.require_task(id)?;
    let reports = store.reports(id)?;
    let evidence = state.home.task_dir(id);
    let last_event = last_event_for_row(&row, &reports, evidence.clone());
    Ok(Json(json!({
        "api_version": API_VERSION,
        "id": row.id,
        "status": row.status,
        "agent": row.agent.kind,
        "model": row.agent.model,
        "thread": row.thread,
        "cwd": row.cwd,
        "pid": row.pid,
        "callback": row.callback_status,
        "timeout": humantime::format_duration(row.timeout).to_string(),
        "reports": reports,
        "output_log": state.home.task_paths(id).output,
        "evidence": evidence,
        "last_event": last_event,
        "exit_reason": row.exit_reason,
        "cancel_requested_at": row.cancel_requested_at,
    })))
}

async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<Json<Value>, AppError> {
    let result = {
        let store = lock_store(&state.store);
        store.request_cancel(id)?
    };
    match result {
        CancelResult::AlreadyTerminal(row) => Ok(Json(json!({
            "api_version": API_VERSION,
            "id": row.id,
            "status": row.status,
        }))),
        CancelResult::CancelledQueued(row) => {
            let store = lock_store(&state.store);
            let reports = store.reports(id)?;
            let event = crate::callback::exit_event(&row, &reports, state.home.task_dir(id), false);
            crate::callback::deliver_exit_event(&store, &state.home, &row, &event)?;
            Ok(Json(json!({
                "api_version": API_VERSION,
                "id": id,
                "status": crate::domain::ProcessStatus::Cancelled,
            })))
        }
        CancelResult::SignalWorker(row) => {
            if let Some(pid) = row.pid {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(-pid),
                    nix::sys::signal::Signal::SIGTERM,
                );
            }
            Ok(Json(json!({
                "api_version": API_VERSION,
                "id": id,
                "status": row.status,
            })))
        }
    }
}

fn parse_incoming(value: Value) -> Result<NormalizedSpec, AppError> {
    if value.get("prompt").is_some()
        && value.get("agent").is_some()
        && value.get("prompt_file").is_none()
    {
        if let Ok(normalized) = serde_json::from_value::<NormalizedSpec>(value.clone()) {
            if normalized.api_version == API_VERSION {
                spec::check_cwd(&normalized.cwd)?;
                return Ok(normalized);
            }
        }
    }
    let spec: SubmitSpec = spec::parse_spec_value(value)?;
    spec::check_cwd(&spec.cwd)?;
    spec::normalize(&spec)
}

fn accept_task(
    state: &AppState,
    body: SubmitBody,
    dry: bool,
) -> Result<(TaskId, ProcessStatus), AppError> {
    let spec = parse_incoming(body.spec)?;
    spec::check_cwd(&spec.cwd)?;
    let binary = resolve_binary(spec.agent, &body.env.path, &spec.cwd)?;
    if dry {
        return Ok((TaskId::new(), ProcessStatus::Queued));
    }
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
    {
        let store = lock_store(&state.store);
        store.insert_task(&row)?;
    }
    spawn_and_watch(state, id)?;
    Ok((id, ProcessStatus::Queued))
}

impl NormalizedSpec {
    fn agent(&self) -> crate::domain::Agent {
        crate::domain::Agent::new(self.agent, self.model.clone())
    }
}
