//! Service routes, the check for vanished service workers, and the scan that
//! stops the services of owners whose T3 thread is done
//!
//! Every route is socket-only: a start runs a command with the caller's
//! environment, and the others name the commands that run

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::thread_api::caller_t3_here;
use super::thread_api::find_on;
use super::thread_api::watch_scan::{CallerSeen, GONE_SCANS, owner_reference, owner_seen};
use crate::cleanup;
use crate::domain::{API_VERSION, ThreadId};
use crate::error::AppError;
use crate::home::{self, LockMode};
use crate::runner;
use crate::service::{
    Ending, Service, ServiceEnd, ServiceId, ServiceRecord, ServicesResponse, StartRequest,
    StopRequest, worker,
};
use crate::submission::CallbackContext;
use crate::thread_ref::ThreadState;

/// How often the daemon looks for service workers that vanished
const LIVENESS_INTERVAL: Duration = Duration::from_secs(2);

/// Longest service name kept
const NAME_MAX_CHARS: usize = 120;

/// Service routes for the Unix socket
pub fn socket_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/services", post(start).get(list))
        .route("/v1/services/{id}", get(show))
        .route("/v1/services/{id}/log", get(log))
        .route("/v1/services/{id}/stop", post(stop))
}

async fn start(
    State(state): State<AppState>,
    Json(body): Json<StartRequest>,
) -> Result<Json<Value>, AppError> {
    validate(&body)?;
    let codex = call(&state.supervisor, |reply| SupervisorMsg::CallbackCodex {
        path: body.callback_env.path.clone(),
        cwd: body.callback_cwd.clone(),
        reply,
    })
    .await?;
    let owner_t3 = match body.owner.is_detached() {
        true => None,
        false => caller_t3_here(&state, body.owner).await,
    };
    let name: String = body.name.trim().chars().take(NAME_MAX_CHARS).collect();
    let service = Service {
        id: ServiceId::new(),
        name,
        owner: body.owner,
        owner_t3,
        callback: CallbackContext {
            env: body.callback_env,
            cwd: body.callback_cwd,
            codex,
        },
        cwd: body.cwd,
        command: body.command,
        readiness: body.readiness,
        ready_timeout: body.ready_timeout,
        max: body.max,
        created_at: Utc::now(),
    };
    let id = service.id;
    call(&state.store, |reply| StoreMsg::InsertService {
        service: Box::new(service),
        reply,
    })
    .await?;

    match worker::spawn(&state.home, id, &body.env) {
        Ok(pid) => {
            let pid = i32::try_from(pid).map_err(|_| AppError::Internal {
                message: format!("service worker pid {pid} is out of range"),
            })?;
            call(&state.store, |reply| StoreMsg::SetServiceWorker {
                id,
                pid,
                reply,
            })
            .await?;
        }
        Err(error) => {
            let ending = Ending {
                end: ServiceEnd::NotReady,
                exit: None,
                detail: Some(format!("could not start its worker: {error}")),
                at: Utc::now(),
            };
            call(&state.store, |reply| StoreMsg::EndService {
                id,
                ending,
                reply,
            })
            .await?;
            return Err(error);
        }
    }
    Ok(Json(view(&state, require(&state, id).await?)))
}

fn validate(body: &StartRequest) -> Result<(), AppError> {
    let usage = |message: &str| {
        Err(AppError::Usage {
            message: message.into(),
        })
    };
    if body.api_version != API_VERSION {
        return usage("unsupported api_version");
    }
    if body.command.first().is_none_or(String::is_empty) {
        return usage("a service needs a command after `--`");
    }
    if body.name.trim().is_empty() {
        return usage("--name must not be empty");
    }
    if !body.cwd.is_absolute() || !body.cwd.is_dir() {
        return Err(AppError::Usage {
            message: format!("--cwd {} is not a directory", body.cwd.display()),
        });
    }
    if body.ready_timeout.is_zero() || body.max.is_zero() {
        return usage("--ready-timeout and --max must be longer than zero");
    }
    body.readiness.validate()
}

/// `GET /v1/services` query
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    /// Only this owner's services
    owner: Option<ThreadId>,
    /// Ended services too
    #[serde(default)]
    ended: bool,
}

async fn list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ServicesResponse>, AppError> {
    let records = call(&state.store, |reply| StoreMsg::ListServices {
        owner: query.owner,
        ended: query.ended,
        reply,
    })
    .await?;
    Ok(Json(ServicesResponse {
        api_version: API_VERSION,
        services: records
            .iter()
            .map(|record| record.view(state.home.service_paths(record.service.id).output))
            .collect(),
    }))
}

async fn show(
    State(state): State<AppState>,
    Path(id): Path<ServiceId>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(view(&state, require(&state, id).await?)))
}

/// `GET /v1/services/{id}/log` query
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogQuery {
    tail: Option<usize>,
}

async fn log(
    State(state): State<AppState>,
    Path(id): Path<ServiceId>,
    Query(query): Query<LogQuery>,
) -> Result<Json<Value>, AppError> {
    require(&state, id).await?;
    let paths = state.home.service_paths(id);
    let output = tokio::task::spawn_blocking(move || paths.read_output(query.tail))
        .await
        .map_err(|error| AppError::Internal {
            message: format!("read service log join: {error}"),
        })??;
    let (log, truncated) = output.map_or((String::new(), false), |output| {
        (output.text, output.truncated)
    });
    Ok(Json(json!({
        "api_version": API_VERSION,
        "id": id,
        "log": log,
        "truncated": truncated,
    })))
}

/// Ask a service to stop; stopping an ended one changes nothing
async fn stop(
    State(state): State<AppState>,
    Path(id): Path<ServiceId>,
) -> Result<Json<Value>, AppError> {
    let record = require(&state, id).await?;
    if record.is_live() {
        request_stop(&state, id, StopRequest::Stopped).await?;
    }
    Ok(Json(view(&state, require(&state, id).await?)))
}

async fn require(state: &AppState, id: ServiceId) -> Result<ServiceRecord, AppError> {
    call(&state.store, |reply| StoreMsg::GetService { id, reply })
        .await?
        .ok_or(AppError::ServiceNotFound { service: id })
}

fn view(state: &AppState, record: ServiceRecord) -> Value {
    let output = state.home.service_paths(record.service.id).output;
    let mut value = json!(record.view(output));
    if let Some(object) = value.as_object_mut() {
        object.insert("api_version".into(), API_VERSION.into());
    }
    value
}

/// Record a stop request and tell the worker
///
/// A worker not spawned yet reads the request when it starts. One that holds
/// no lock any more is gone, so the service ends here, as the request says
async fn request_stop(
    state: &AppState,
    id: ServiceId,
    request: StopRequest,
) -> Result<(), AppError> {
    let record = call(&state.store, |reply| StoreMsg::RequestServiceStop {
        id,
        request,
        reply,
    })
    .await?;
    if !record.is_live() {
        return Ok(());
    }
    let Some(pid) = record.worker_pid else {
        return Ok(());
    };
    if !worker_alive(state, id) {
        end_lost(state, &record).await;
        return Ok(());
    }
    // the worker forwards the stop to the service's own process group
    if let Err(error) = kill(Pid::from_raw(pid), Signal::SIGTERM) {
        warn!(%id, pid, "SIGTERM service worker: {error}");
    }
    Ok(())
}

/// Whether the service's worker still holds its lock
fn worker_alive(state: &AppState, id: ServiceId) -> bool {
    let lock = state.home.service_paths(id).runner_lock;
    match home::flock_exclusive(&lock, LockMode::NonBlocking) {
        Err(AppError::LockHeld { .. }) => true,
        Ok(_) => false,
        Err(error) => {
            // unable to tell, so the worker is left to end the service itself
            warn!(%id, "service worker lock: {error}");
            true
        }
    }
}

/// End a service whose worker vanished, and stop what is left of it
///
/// A recorded stop request still decides the end, so a service stopped while
/// its worker died sends no event. The process group is signalled only while
/// its leader keeps the identity the worker recorded, so a reused process id
/// is never hit
async fn end_lost(state: &AppState, record: &ServiceRecord) {
    let id = record.service.id;
    let ending = Ending {
        end: ServiceEnd::Lost,
        exit: None,
        detail: Some("its service-run worker ended without recording how the service ended".into()),
        at: Utc::now(),
    };
    match call(&state.store, |reply| StoreMsg::EndService {
        id,
        ending,
        reply,
    })
    .await
    {
        Ok(true) => info!(%id, "service worker lost"),
        Ok(false) => return,
        Err(error) => {
            warn!(%id, "end lost service: {error}");
            return;
        }
    }
    let Some(child) = record.child else {
        return;
    };
    if cleanup::process_identity(child.pid).ok() == Some(child) {
        tokio::spawn(runner::cleanup_process_group(child.pid.as_raw()));
    }
}

/// End services whose worker vanished, for as long as the daemon runs
///
/// It also covers a daemon restart: services keep running under their
/// workers, and only those whose worker is gone are ended
pub(super) async fn run(state: AppState) {
    let mut tick = tokio::time::interval(LIVENESS_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let records = match call(&state.store, |reply| StoreMsg::ListServices {
            owner: None,
            ended: false,
            reply,
        })
        .await
        {
            Ok(records) => records,
            Err(error) => {
                warn!("service liveness check: {error}");
                continue;
            }
        };
        // a row without a worker is still being started by its route
        for record in records.iter().filter(|record| record.worker_pid.is_some()) {
            if !worker_alive(&state, record.service.id) {
                end_lost(&state, record).await;
            }
        }
    }
}

/// What the owner scan remembers between runs: scans in a row that found a
/// service's owner gone
#[derive(Debug, Default)]
pub(crate) struct OwnerMemory {
    gone: HashMap<ServiceId, u8>,
}

/// Stop every live service whose owner's T3 thread is archived, settled, or
/// done with its delegated task, or gone for [`GONE_SCANS`] scans in a row
///
/// Runs in the same loop as the thread watch scan. An owner outside T3, such
/// as a terminal session, cannot be followed, so its services end only by
/// `service stop`, `--max`, or exiting
pub(crate) async fn scan(state: &AppState, memory: &mut OwnerMemory) {
    let records = match call(&state.store, |reply| StoreMsg::ListServices {
        owner: None,
        ended: false,
        reply,
    })
    .await
    {
        Ok(records) => records,
        Err(error) => {
            warn!("service owner scan: {error}");
            return;
        }
    };
    let records: Vec<_> = records
        .into_iter()
        .filter(|record| !record.service.owner.is_detached() && record.stop_request.is_none())
        .collect();
    let live: HashSet<_> = records.iter().map(|record| record.service.id).collect();
    memory.gone.retain(|id, _| live.contains(id));
    if records.is_empty() {
        return;
    }

    let mut references: Vec<String> = Vec::new();
    for record in &records {
        let reference = reference(&record.service);
        if !references.contains(&reference) {
            references.push(reference);
        }
    }
    let local = state.machine.identity.machine;
    let found = match find_on(state, local, references.clone()).await {
        Ok(found) => found,
        Err(message) => {
            // a scan that cannot look breaks a run of scans that found an owner gone
            debug!("service owner scan cannot read threads: {message}");
            memory.gone.clear();
            return;
        }
    };
    let found: HashMap<_, _> = references.into_iter().zip(found).collect();

    for record in records {
        let id = record.service.id;
        let matches = found
            .get(&reference(&record.service))
            .map_or(&[][..], Vec::as_slice);
        let done = match owner_seen(record.service.owner_t3.as_deref(), matches) {
            CallerSeen::Untracked => false,
            CallerSeen::Gone => {
                let count = memory.gone.entry(id).or_default();
                *count = count.saturating_add(1);
                *count >= GONE_SCANS
            }
            CallerSeen::T3(thread) => {
                memory.gone.remove(&id);
                thread.state == ThreadState::Archived || thread.done()
            }
        };
        if !done {
            continue;
        }
        info!(%id, owner = %record.service.owner, "Stopping service of a finished thread");
        if let Err(error) = request_stop(state, id, StopRequest::OwnerDone).await {
            warn!(%id, "stop service of a finished thread: {error}");
        }
    }
}

fn reference(service: &Service) -> String {
    owner_reference(service.owner, service.owner_t3.as_deref())
}
