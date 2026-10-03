//! Daemon-to-daemon identity and event routes under `/v1/cluster/*`

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::cancellation::{CancellationReceipt, CancellationRequestIdentity, CancellationTarget};
use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::daemon::api::views::{LogTail, TaskDetail};
use crate::domain::{API_VERSION, ProcessStatus, TaskEnv, TaskId, TaskIdentity};
use crate::error::AppError;
use crate::events::{EventAcceptance, TaskEvent};
use crate::fleet::advertisement::{MACHINE_PROBE_PATH, MachineAdvertisement};
use crate::fleet::protocol::{ClusterProtocolVersion, ProtocolRange, SUPPORTED_PROTOCOLS};
use crate::fleet::runtime::FleetHandle;
use crate::invocation::{StdinPolicy, invocation_from_normalized_for_identity};
use crate::machine::MachineId;
use crate::message::{MessageRequest, MessageResponse};
use crate::spec::{self, NormalizedSpec};
use crate::store::IdentityError;
use crate::submission::{ExecutorIdentity, RejectionTombstone, RequestId, SubmissionState};
use std::path::{Path as StdPath, PathBuf};

/// Strict destination and version for a cluster read
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadQuery {
    /// Public API version
    pub api_version: u32,
    /// UUID of the intended receiver
    pub destination_machine: MachineId,
}

/// A remote request bound to one task and two machine identities
#[derive(Debug, Serialize, Deserialize)]
pub struct SubmitExecution {
    /// Public API schema version; missing values decode as zero for a versioned usage error
    #[serde(default)]
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Intended receiver
    pub destination_machine: MachineId,
    /// Callback owner
    pub origin_machine: MachineId,
    /// Global task identity
    pub task: TaskId,
    /// Inline, normalized content; no origin environment is allowed
    pub spec: serde_json::Value,
    /// Unrecognized top-level fields make a bound request definitively invalid
    #[serde(flatten)]
    pub unknown: BTreeMap<String, serde_json::Value>,
}

/// Pre-acceptance resolution request
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbandonExecution {
    /// Public API schema version; missing values decode as zero for a versioned usage error
    #[serde(default)]
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Intended receiver
    pub destination_machine: MachineId,
    /// Callback owner
    pub origin_machine: MachineId,
    /// Global task identity
    pub task: TaskId,
}

/// Destination-checked executor cancellation request
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelExecution {
    /// Public API schema version; missing values decode as zero for a versioned usage error
    #[serde(default)]
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Immutable request and fixed owners
    pub request: CancellationRequestIdentity,
    /// Typed route target proven by the requester
    pub target: CancellationTarget,
}

/// Durable executor acknowledgement
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelBody {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Stored executor receipt
    pub receipt: CancellationReceipt,
}

/// Retained executor identity, or an absent lookup
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityBody {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Retained state, if any
    pub identity: Option<ExecutorIdentity>,
}

/// Preview request without task identity or origin callback data
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewExecution {
    /// Public API schema version; missing values decode as zero for a versioned usage error
    #[serde(default)]
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Intended execution owner
    pub destination_machine: MachineId,
    /// Inline normalized content
    pub spec: serde_json::Value,
}

/// Executor-selected invocation for a remote dry-run
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewBody {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Expanded directory on the executor
    pub cwd: PathBuf,
    /// Executor-selected argv
    pub argv: Vec<String>,
    /// How the task receives stdin
    pub stdin: StdinPolicy,
}

/// Strict remote event transport request
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiveEvent {
    /// Public API schema version; missing values decode as zero for a versioned usage error
    #[serde(default)]
    pub api_version: u32,
    /// Cluster protocol version, independent of event API version
    pub protocol_version: u32,
    /// Intended origin receiver UUID
    pub destination_machine: MachineId,
    /// Immutable sequenced event
    pub event: TaskEvent,
}

/// Durable origin acceptance or the next required sequence
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventBody {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Accepted event or missing predecessor
    pub result: EventAcceptance,
}

/// An execution record held on this daemon only
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionView {
    /// Task UUID
    pub task: TaskId,
    /// Machine that owns this execution record
    pub execution_machine: MachineId,
    /// Actual stored process state
    pub status: ProcessStatus,
}

/// One local execution lookup, including an absent record
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBody {
    /// Public API version
    pub api_version: u32,
    /// Stored execution record, if present
    pub execution: Option<ExecutionView>,
}

/// Safe callback failure fields that omit raw queue stderr
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallbackFailureSummary {
    /// Failed event sequence
    pub seq: u64,
    /// Number of reserved delivery attempts
    pub attempts: u8,
    /// Safe failure classification without raw child output
    pub error: String,
}

/// Origin-owned fields safe for a fleet inspection response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginSummary {
    /// Global task UUID
    pub task: TaskId,
    /// Caller retry UUID
    pub request_id: RequestId,
    /// Callback owner
    pub origin_machine: MachineId,
    /// Fixed execution owner
    pub execution_machine: MachineId,
    /// Original Codex thread
    pub thread: crate::domain::ThreadId,
    /// Submission state without origin-only callback context
    pub submission: SubmissionState,
    /// Last process state received from the executor
    pub last_execution_state: Option<ProcessStatus>,
    /// Time of the last origin route state update, when known
    pub last_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Last accepted event sequence
    pub last_accepted_seq: u64,
    /// Last settled callback sequence
    pub last_settled_seq: u64,
    /// Retained callback failures owned by this origin
    pub failed_events: Vec<CallbackFailureSummary>,
}

/// Retained executor identity without the private normalized specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum IdentitySummary {
    /// Accepted work, even when task detail has been removed
    Accepted {
        /// Global task UUID
        task: TaskId,
        /// Callback owner
        origin_machine: MachineId,
        /// Execution owner
        execution_machine: MachineId,
        /// Last durable process state
        status: ProcessStatus,
    },
    /// A task UUID that cannot start
    Rejected {
        /// Global task UUID
        task: TaskId,
        /// Callback owner
        origin_machine: MachineId,
        /// Execution owner
        execution_machine: MachineId,
        /// Durable rejection reason
        reason: String,
    },
}

/// Optional local origin route summary
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginBody {
    /// Public API version
    pub api_version: u32,
    /// Route, if this machine owns it
    pub origin: Option<OriginSummary>,
}

/// Optional local retained identity summary
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentitySummaryBody {
    /// Public API version
    pub api_version: u32,
    /// Identity, if this machine owns it
    pub identity: Option<IdentitySummary>,
}

/// Local execution list for future fleet inventory views
#[derive(Debug, Serialize)]
pub struct ExecutionsBody {
    /// Public API version
    pub api_version: u32,
    /// Stored executions on this daemon
    pub executions: Vec<ExecutionView>,
}

/// Cluster routes for an enabled fleet
pub fn routes(fleet: FleetHandle) -> Router<AppState> {
    Router::new()
        .route(MACHINE_PROBE_PATH, get(move || machine(fleet.clone())))
        .route("/v1/cluster/tasks", get(tasks))
        .route("/v1/cluster/tasks/{id}", get(task))
        .route("/v1/cluster/tasks/{id}/detail", get(task_detail))
        .route("/v1/cluster/tasks/{id}/log", get(task_log))
        .route("/v1/cluster/origin/tasks/{id}", get(origin_summary))
        .route("/v1/cluster/identities/{id}", get(identity_summary))
        .route("/v1/cluster/executions", post(submit_execution))
        .route("/v1/cluster/executions/preview", post(preview_execution))
        .route("/v1/cluster/executions/{id}", get(executor_identity))
        .route("/v1/cluster/executions/abandon", post(abandon_execution))
        .route("/v1/cluster/executions/cancel", post(cancel_execution))
        .route("/v1/cluster/events", post(receive_event))
        .route("/v1/cluster/messages", post(receive_message))
        .merge(super::queue_api::cluster_routes())
        .merge(super::fleet_tasks::cluster_routes())
        .merge(super::thread_titles::cluster_routes())
}

async fn receive_message(
    State(state): State<AppState>,
    Json(body): Json<MessageRequest>,
) -> Result<Json<MessageResponse>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.source.machine())?;
    body.validate()?;
    let protocol_version = body.protocol_version;
    let receipt = state.message_receiver.receive(&state, body).await?;
    Ok(Json(MessageResponse {
        api_version: API_VERSION,
        protocol_version,
        destination_machine: state.machine.identity.machine,
        receipt,
    }))
}

async fn task_detail(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<TaskDetail>, AppError> {
    check(&state, query)?;
    crate::daemon::api::local_detail(&state, id).await.map(Json)
}

async fn task_log(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
    Query(query): Query<crate::daemon::inspection::ClusterLogQuery>,
) -> Result<Json<LogTail>, AppError> {
    check(
        &state,
        ReadQuery {
            api_version: query.api_version,
            destination_machine: query.destination_machine,
        },
    )?;
    crate::daemon::api::local_log(&state, id, query.tail)
        .await
        .map(Json)
}

async fn origin_summary(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<OriginBody>, AppError> {
    check(&state, query)?;
    let route = call(&state.store, |reply| StoreMsg::OriginRoute { id, reply }).await?;
    let failed_events = if route.is_some() {
        call(&state.store, |reply| StoreMsg::FailedInboxEvents {
            id,
            reply,
        })
        .await?
        .into_iter()
        .map(|failure| CallbackFailureSummary {
            seq: failure.seq,
            attempts: failure.attempts,
            error: "callback_delivery_failed".into(),
        })
        .collect()
    } else {
        Vec::new()
    };
    Ok(Json(OriginBody {
        api_version: API_VERSION,
        origin: route.map(|route| OriginSummary {
            task: route.task,
            request_id: route.request,
            origin_machine: route.origin_machine,
            execution_machine: route.execution_machine,
            thread: route.thread,
            submission: route.submission,
            last_execution_state: route.last_execution_state,
            last_updated_at: route.last_updated_at,
            last_accepted_seq: route.last_accepted_seq,
            last_settled_seq: route.last_settled_seq,
            failed_events,
        }),
    }))
}

async fn identity_summary(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<IdentitySummaryBody>, AppError> {
    check(&state, query)?;
    let identity = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
        id,
        reply,
    })
    .await?;
    Ok(Json(IdentitySummaryBody {
        api_version: API_VERSION,
        identity: identity.map(|identity| match identity {
            ExecutorIdentity::Accepted(record) => IdentitySummary::Accepted {
                task: record.task,
                origin_machine: record.origin_machine,
                execution_machine: record.execution_machine,
                status: record.state,
            },
            ExecutorIdentity::Rejected(record) => IdentitySummary::Rejected {
                task: record.task,
                origin_machine: record.origin_machine,
                execution_machine: record.execution_machine,
                reason: record.reason,
            },
        }),
    }))
}

async fn preview_execution(
    State(state): State<AppState>,
    Json(body): Json<PreviewExecution>,
) -> Result<Json<PreviewBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.destination_machine)?;
    let spec = spec::parse_normalized_value(&body.spec)?;
    let cwd = expand_executor_cwd(&spec.cwd)?;
    spec::check_spec_host(&NormalizedSpec {
        cwd: cwd.clone(),
        ..spec.clone()
    })?;
    let env = TaskEnv::capture();
    let feed = state.home.root().join("tasks/<task-id>/prompt.feed.txt");
    let invocation = invocation_from_normalized_for_identity(
        &spec.workload,
        &env.path,
        &cwd,
        Some(&feed),
        TaskIdentity::Preview,
    )?;
    Ok(Json(PreviewBody {
        api_version: API_VERSION,
        protocol_version: body.protocol_version,
        cwd,
        argv: invocation.to_vec(),
        stdin: invocation.stdin,
    }))
}

async fn receive_event(
    State(state): State<AppState>,
    Json(body): Json<ReceiveEvent>,
) -> Result<Json<EventBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.event.execution_machine)?;
    if body.event.origin_machine != body.destination_machine {
        return Err(AppError::ClusterTaskConflict {
            task: body.event.task,
        });
    }
    let id = body.event.task;
    let result = call(&state.store, |reply| StoreMsg::AcceptInboundEvent {
        event: Box::new(body.event),
        reply,
    })
    .await?;
    if matches!(result, EventAcceptance::Acknowledged { .. }) {
        state.supervisor.cast(SupervisorMsg::DispatchInbox { id })?;
    }
    Ok(Json(EventBody {
        api_version: API_VERSION,
        protocol_version: body.protocol_version,
        result,
    }))
}

pub(super) fn check_api_version(version: u32) -> Result<(), AppError> {
    if version != API_VERSION {
        return Err(AppError::Usage {
            message: "unsupported API version".into(),
        });
    }
    Ok(())
}

pub(super) fn check_protocol(version: u32, machine: MachineId) -> Result<(), AppError> {
    let version = ClusterProtocolVersion(version);
    if SUPPORTED_PROTOCOLS.min() <= version && version <= SUPPORTED_PROTOCOLS.max() {
        return Ok(());
    }
    let Some(remote) = ProtocolRange::new(version, version) else {
        return Err(AppError::Usage {
            message: "unsupported cluster protocol version".into(),
        });
    };
    Err(AppError::ClusterProtocolIncompatible {
        machine,
        local: SUPPORTED_PROTOCOLS,
        remote,
    })
}

fn identity_error(error: IdentityError, task: TaskId) -> AppError {
    match error {
        IdentityError::Conflict => AppError::ClusterTaskConflict { task },
        IdentityError::RouteNotFound => AppError::Internal {
            message: "executor identity disappeared".into(),
        },
        IdentityError::Storage(error) => error,
    }
}

async fn submit_execution(
    State(state): State<AppState>,
    Json(body): Json<SubmitExecution>,
) -> Result<(StatusCode, Json<IdentityBody>), AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.origin_machine)?;
    let normalized = spec::parse_normalized_value(&body.spec);
    let existing = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
        id: body.task,
        reply,
    })
    .await?;
    if let Some(identity) = existing {
        match &identity {
            ExecutorIdentity::Accepted(record)
                if record.origin_machine == body.origin_machine
                    && record.execution_machine == body.destination_machine
                    && record.origin_machine != record.execution_machine
                    && body.unknown.is_empty()
                    && record.spec.current().is_some_and(|stored| {
                        normalized.as_ref().is_ok_and(|spec| stored == spec)
                    }) => {}
            ExecutorIdentity::Rejected(record)
                if record.origin_machine == body.origin_machine
                    && record.execution_machine == body.destination_machine => {}
            _ => return Err(identity_error(IdentityError::Conflict, body.task)),
        }
        return Ok((
            StatusCode::OK,
            Json(IdentityBody {
                api_version: API_VERSION,
                protocol_version: body.protocol_version,
                identity: Some(identity),
            }),
        ));
    }
    let cwd = if let Ok(spec) = &normalized {
        expand_executor_cwd(&spec.cwd)
    } else {
        Err(AppError::InvalidSpec {
            pointer: "/spec".into(),
            value: serde_json::Value::Null,
            message: "invalid normalized specification".into(),
        })
    };
    let reason = match (&normalized, &cwd) {
        _ if body.origin_machine == body.destination_machine => Some("origin_equals_executor"),
        _ if !body.unknown.is_empty() => Some("invalid_request_fields"),
        (Err(_), _) => Some("invalid_spec"),
        (_, Err(AppError::InvalidSpec { .. })) => Some("invalid_cwd"),
        (Ok(spec), Ok(cwd)) => spec::check_spec_host(&NormalizedSpec {
            cwd: cwd.clone(),
            ..spec.clone()
        })
        .err()
        .map(|error| error.rejection.as_str()),
        _ => None,
    };
    if let Some(reason) = reason {
        let tombstone = RejectionTombstone {
            task: body.task,
            origin_machine: body.origin_machine,
            execution_machine: body.destination_machine,
            reason: reason.into(),
        };
        let identity = call(&state.store, |reply| StoreMsg::RejectExecution {
            tombstone,
            reply,
        })
        .await?;
        if matches!(identity, ExecutorIdentity::Accepted(_)) {
            return Err(identity_error(IdentityError::Conflict, body.task));
        }
        return Ok((
            StatusCode::OK,
            Json(IdentityBody {
                api_version: API_VERSION,
                protocol_version: body.protocol_version,
                identity: Some(identity),
            }),
        ));
    }
    let spec = normalized?;
    let cwd = cwd?;
    let env = TaskEnv::capture();
    let feed = state.home.task_paths(body.task).feed;
    let binary = invocation_from_normalized_for_identity(
        &spec.workload,
        &env.path,
        &cwd,
        Some(&feed),
        TaskIdentity::Actual(body.task),
    )
    .map(|invocation| invocation.program)
    .map_err(|error| AppError::RemoteSubmissionUnavailable {
        message: format!("execution capability unavailable: {error}"),
    })?;
    let identity = call(&state.supervisor, |reply| SupervisorMsg::LaunchRemote {
        request: Box::new(crate::daemon::actors::supervisor::RemoteLaunch {
            task: body.task,
            origin: body.origin_machine,
            execution: body.destination_machine,
            spec,
            cwd,
            env,
            binary,
        }),
        reply,
    })
    .await?;
    Ok((
        StatusCode::OK,
        Json(IdentityBody {
            api_version: API_VERSION,
            protocol_version: body.protocol_version,
            identity: Some(identity),
        }),
    ))
}

fn expand_executor_cwd(cwd: &StdPath) -> Result<PathBuf, AppError> {
    if cwd.is_absolute() {
        return Ok(cwd.to_path_buf());
    }
    let text = cwd.to_string_lossy();
    let Some(relative) = text.strip_prefix("~/") else {
        return Err(AppError::InvalidSpec {
            pointer: "/spec/cwd".into(),
            value: serde_json::to_value(cwd).unwrap_or(serde_json::Value::Null),
            message: "remote cwd must be absolute or start with ~/".into(),
        });
    };

    if StdPath::new(relative).is_absolute()
        || StdPath::new(relative)
            .components()
            .any(|component| component == std::path::Component::ParentDir)
    {
        return Err(AppError::InvalidSpec {
            pointer: "/spec/cwd".into(),
            value: serde_json::to_value(cwd).unwrap_or(serde_json::Value::Null),
            message: "remote cwd must remain under executor HOME".into(),
        });
    }

    let home = std::env::var_os("HOME").ok_or(AppError::RemoteSubmissionUnavailable {
        message: "executor HOME is unavailable".into(),
    })?;
    let home = PathBuf::from(home);
    if !home.is_absolute() {
        return Err(AppError::RemoteSubmissionUnavailable {
            message: "executor HOME is not absolute".into(),
        });
    }

    Ok(home.join(relative))
}

async fn executor_identity(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<IdentityBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(query.destination_machine)?;
    check_api_version(query.api_version)?;
    let identity = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
        id,
        reply,
    })
    .await?;
    Ok(Json(IdentityBody {
        api_version: API_VERSION,
        protocol_version: crate::fleet::protocol::CLUSTER_PROTOCOL_VERSION.0,
        identity,
    }))
}

async fn abandon_execution(
    State(state): State<AppState>,
    Json(body): Json<AbandonExecution>,
) -> Result<Json<IdentityBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.origin_machine)?;
    let identity = call(&state.store, |reply| StoreMsg::AbandonExecution {
        id: body.task,
        origin: body.origin_machine,
        execution: body.destination_machine,
        reply,
    })
    .await?;
    Ok(Json(IdentityBody {
        api_version: API_VERSION,
        protocol_version: body.protocol_version,
        identity: Some(identity),
    }))
}

async fn cancel_execution(
    State(state): State<AppState>,
    Json(body): Json<CancelExecution>,
) -> Result<Json<CancelBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.request.execution_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.request.requester_machine)?;
    let mut receipt = call(&state.store, |reply| StoreMsg::ReceiveCancellation {
        request: body.request,
        reply,
    })
    .await?;
    if matches!(
        receipt.state,
        crate::cancellation::ExecutorCancelState::PendingApplication
    ) {
        match crate::daemon::cancel_delivery::apply_executor(&state, receipt.clone()).await {
            Ok(applied) => receipt = applied,
            Err(error) => {
                tracing::warn!(task = %body.request.task, "cancellation application: {error}")
            }
        }
    }
    Ok(Json(CancelBody {
        api_version: API_VERSION,
        protocol_version: body.protocol_version,
        receipt,
    }))
}

async fn machine(fleet: FleetHandle) -> Json<MachineAdvertisement> {
    Json(fleet.advertisement().clone())
}

fn check(state: &AppState, query: ReadQuery) -> Result<(), AppError> {
    state
        .machine
        .identity
        .check_destination(query.destination_machine)?;
    check_api_version(query.api_version)
}

async fn tasks(
    State(state): State<AppState>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<ExecutionsBody>, AppError> {
    check(&state, query)?;
    let rows = call(&state.store, |reply| StoreMsg::ListTasks {
        statuses: Vec::new(),
        thread: None,
        reply,
    })
    .await?;
    let executions = rows
        .into_iter()
        .map(|row| ExecutionView {
            task: row.id,
            execution_machine: state.machine.identity.machine,
            status: row.status(),
        })
        .collect();
    Ok(Json(ExecutionsBody {
        api_version: API_VERSION,
        executions,
    }))
}

async fn task(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
    Query(query): Query<ReadQuery>,
) -> Result<(StatusCode, Json<ExecutionBody>), AppError> {
    check(&state, query)?;
    let row = call(&state.store, |reply| StoreMsg::GetTask { id, reply }).await?;
    let execution = row.map(|row| ExecutionView {
        task: row.id,
        execution_machine: state.machine.identity.machine,
        status: row.status(),
    });
    let status = if execution.is_some() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    };
    Ok((
        status,
        Json(ExecutionBody {
            api_version: API_VERSION,
            execution,
        }),
    ))
}
