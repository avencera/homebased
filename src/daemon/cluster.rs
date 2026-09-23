//! Daemon-to-daemon identity and event routes under `/v1/cluster/*`

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::ErrorKind;

use crate::cancellation::{CancellationReceipt, CancellationRequestIdentity};
use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, call};
use crate::daemon::api::views::{LogTail, TaskDetail};
use crate::domain::{API_VERSION, ProcessStatus, TaskEnv, TaskId, TaskIdentity};
use crate::error::AppError;
use crate::events::{EventAcceptance, TaskEvent};
use crate::fleet::advertisement::{MACHINE_PROBE_PATH, MachineAdvertisement};
use crate::fleet::protocol::{ClusterProtocolVersion, ProtocolRange, SUPPORTED_PROTOCOLS};
use crate::fleet::runtime::FleetHandle;
use crate::invocation::{StdinPolicy, invocation_from_normalized_for_identity};
use crate::machine::MachineId;
use crate::message::{MessageRequest, MessageResponse, MessageSource};
use crate::resource::{
    ResourceQueueRequest, ResourceQueueResponse, ResourceRequest, ResourceRequestState,
    SupervisorNoticeRequest, SupervisorNoticeResponse,
};
use crate::spec;
use crate::store::IdentityError;
use crate::submission::{
    ExecutorIdentity, RejectionTombstone, ResourceQueueOutcome, ResourceQueueReceipt,
    ResourceRoutePhase, ResourceRouteProof, SubmissionState, normalized_spec_sha256,
};
use std::path::{Path as StdPath, PathBuf};

/// Strict destination and version for a cluster read.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadQuery {
    /// Public API version.
    pub api_version: u32,
    /// UUID of the intended receiver.
    pub destination_machine: MachineId,
}

/// A remote request bound to one task and two machine identities.
#[derive(Debug, Serialize, Deserialize)]
pub struct SubmitExecution {
    /// Public API schema version; missing values decode as zero for a versioned usage error
    #[serde(default)]
    pub api_version: u32,
    /// Cluster protocol version.
    pub protocol_version: u32,
    /// Intended receiver.
    pub destination_machine: MachineId,
    /// Callback owner.
    pub origin_machine: MachineId,
    /// Global task identity.
    pub task: TaskId,
    /// Inline, normalized content; no origin environment is allowed.
    pub spec: serde_json::Value,
    /// Unrecognized top-level fields make a bound request definitively invalid
    #[serde(flatten)]
    pub unknown: BTreeMap<String, serde_json::Value>,
}

/// Pre-acceptance resolution request.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbandonExecution {
    /// Public API schema version; missing values decode as zero for a versioned usage error
    #[serde(default)]
    pub api_version: u32,
    /// Cluster protocol version.
    pub protocol_version: u32,
    /// Intended receiver.
    pub destination_machine: MachineId,
    /// Callback owner.
    pub origin_machine: MachineId,
    /// Global task identity.
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

/// Retained executor identity, or an absent lookup.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityBody {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version.
    pub protocol_version: u32,
    /// Retained state, if any.
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

/// An execution record held on this daemon only.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionView {
    /// Task UUID.
    pub task: TaskId,
    /// Machine that owns this execution record.
    pub execution_machine: MachineId,
    /// Actual stored process state.
    pub status: ProcessStatus,
}

/// One local execution lookup, including an absent record.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBody {
    /// Public API version.
    pub api_version: u32,
    /// Stored execution record, if present.
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
    /// Callback owner
    pub origin_machine: MachineId,
    /// Fixed execution owner
    pub execution_machine: MachineId,
    /// Original Codex thread, absent when an older peer does not expose it.
    #[serde(default)]
    pub thread: Option<crate::domain::ThreadId>,
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

/// Optional safe resource-route proof retained by this origin
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginResourceRouteProofBody {
    /// Public API version
    pub api_version: u32,
    /// Proof, if this origin retains a valid resource route for the task
    pub proof: Option<ResourceRouteProof>,
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
        .route(
            "/v1/cluster/origin/resource-routes/{task}",
            get(origin_resource_route_proof),
        )
        .route("/v1/cluster/identities/{id}", get(identity_summary))
        .route("/v1/cluster/executions", post(submit_execution))
        .route("/v1/cluster/executions/preview", post(preview_execution))
        .route("/v1/cluster/executions/{id}", get(executor_identity))
        .route("/v1/cluster/executions/abandon", post(abandon_execution))
        .route("/v1/cluster/executions/cancel", post(cancel_execution))
        .route(
            "/v1/cluster/resource-requests",
            post(accept_resource_request),
        )
        .route("/v1/cluster/events", post(receive_event))
        .route("/v1/cluster/messages", post(receive_message))
        .route(
            "/v1/cluster/resource-notices",
            post(receive_supervisor_notice),
        )
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
    if matches!(body.source, MessageSource::ResourceNotice { .. }) {
        return Err(AppError::MessageInvalid {
            message: "resource notices must use the resource-notice endpoint".into(),
        });
    }
    let protocol_version = body.protocol_version;
    let receipt = state.message_receiver.receive(&state, body).await?;
    Ok(Json(MessageResponse {
        api_version: crate::domain::API_VERSION,
        protocol_version,
        destination_machine: state.machine.identity.machine,
        receipt,
    }))
}

async fn receive_supervisor_notice(
    State(state): State<AppState>,
    Json(body): Json<SupervisorNoticeRequest>,
) -> Result<Json<SupervisorNoticeResponse>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination.machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.source_machine)?;
    body.validate()?;
    let protocol_version = body.protocol_version;
    let receipt = state
        .message_receiver
        .receive_supervisor_notice(&state, body)
        .await?;
    Ok(Json(SupervisorNoticeResponse {
        api_version: crate::domain::API_VERSION,
        protocol_version,
        destination_machine: state.machine.identity.machine,
        receipt,
    }))
}

async fn accept_resource_request(
    State(state): State<AppState>,
    Json(request): Json<ResourceQueueRequest>,
) -> Result<Json<ResourceQueueResponse>, AppError> {
    let authority_machine = state.machine.identity.machine;
    state
        .machine
        .identity
        .check_destination(request.destination_machine)?;
    check_api_version(request.api_version)?;
    check_protocol(request.protocol_version, request.origin_machine)?;

    let proof = resource_route_proof(&state, &request).await?;
    let phase = validate_resource_route_proof(&request, authority_machine, proof)?;
    if let Some(reason) = rejected_resource_route_reason(&phase) {
        return Ok(resource_queue_response(
            &request,
            authority_machine,
            ResourceQueueOutcome::Rejected { reason },
        ));
    }

    if resource_route_requires_retained_request(&phase) {
        let requests = match call(&state.store, |reply| StoreMsg::ResourceRequests {
            authority_machine,
            resource_id: request.resource_id,
            reply,
        })
        .await
        {
            Ok(requests) => requests,
            Err(AppError::Usage { .. }) => {
                return Err(resource_route_retention_conflict(&request));
            }
            Err(error) => return Err(error),
        };
        let stored = retained_resource_request(&request, requests)?;

        return resource_queue_response_from_stored(&request, authority_machine, stored);
    }

    let stored = match call(&state.store, |reply| StoreMsg::AcceptResourceRequest {
        authority_machine,
        request_id: request.request_id,
        task_id: request.task_id,
        resource_id: request.resource_id,
        origin_machine: request.origin_machine,
        normalized_spec: Box::new(request.spec.as_normalized().clone()),
        reply,
    })
    .await
    {
        Ok(stored) => stored,
        Err(AppError::SubmissionRejected { reason, .. }) => {
            return Ok(resource_queue_response(
                &request,
                authority_machine,
                ResourceQueueOutcome::Rejected { reason },
            ));
        }
        Err(error) => return Err(error),
    };

    resource_queue_response_from_stored(&request, authority_machine, stored)
}

async fn resource_route_proof(
    state: &AppState,
    request: &ResourceQueueRequest,
) -> Result<Option<ResourceRouteProof>, AppError> {
    if request.origin_machine == state.machine.identity.machine {
        let route = call(&state.store, |reply| StoreMsg::OriginRoute {
            id: request.task_id,
            reply,
        })
        .await?;

        return Ok(route
            .as_ref()
            .filter(|route| route.task == request.task_id)
            .and_then(ResourceRouteProof::from_route));
    }

    let unavailable = |message: String| AppError::RemoteSubmissionUnavailable { message };
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| unavailable("origin resource-route proof is unavailable".into()))?;
    let destination = fleet
        .connect(request.origin_machine)
        .await
        .map_err(|error| {
            unavailable(format!(
                "origin resource-route proof is unavailable: {error}"
            ))
        })?;
    let path = format!(
        "/v1/cluster/origin/resource-routes/{}?api_version={API_VERSION}&destination_machine={}",
        request.task_id, request.origin_machine,
    );
    let response = crate::fleet::http::ClusterClient::default()
        .get(&destination.address, &path)
        .await
        .map_err(|error| {
            unavailable(format!(
                "origin resource-route proof is unavailable: {error}"
            ))
        })?;
    if response.status != StatusCode::OK {
        return Err(unavailable(format!(
            "origin resource-route proof is unavailable: HTTP {}",
            response.status
        )));
    }

    let body: OriginResourceRouteProofBody = serde_json::from_slice(&response.body)
        .map_err(|error| unavailable(format!("invalid origin resource-route proof: {error}")))?;
    if body.api_version != API_VERSION {
        return Err(unavailable(
            "origin resource-route proof uses an unsupported API version".into(),
        ));
    }

    Ok(body.proof)
}

fn validate_resource_route_proof(
    request: &ResourceQueueRequest,
    authority_machine: MachineId,
    proof: Option<ResourceRouteProof>,
) -> Result<ResourceRoutePhase, AppError> {
    let Some(proof) = proof else {
        return Err(AppError::RemoteSubmissionUnavailable {
            message: "origin has no valid resource-route proof for this task".into(),
        });
    };
    let digest = normalized_spec_sha256(request.spec.as_normalized()).map_err(|error| {
        AppError::Internal {
            message: format!("serialize resource command specification: {error}"),
        }
    })?;
    if proof.request != request.request_id
        || proof.task != request.task_id
        || proof.resource != request.resource_id
        || proof.origin_machine != request.origin_machine
        || proof.authority_machine != authority_machine
        || proof.thread != request.spec.as_normalized().thread
        || proof.normalized_spec_sha256 != digest
    {
        return Err(AppError::SubmissionConflict {
            request: request.request_id,
            task: request.task_id,
            message: "origin resource-route proof does not match this request".into(),
        });
    }

    Ok(proof.phase)
}

fn rejected_resource_route_reason(phase: &ResourceRoutePhase) -> Option<String> {
    match phase {
        ResourceRoutePhase::AcceptanceUnknown
        | ResourceRoutePhase::Waiting
        | ResourceRoutePhase::Activated => None,
        ResourceRoutePhase::CancelledBeforeLaunch => Some("cancelled_before_launch".into()),
        ResourceRoutePhase::Rejected { reason } => Some(reason.clone()),
    }
}

fn resource_route_requires_retained_request(phase: &ResourceRoutePhase) -> bool {
    matches!(
        phase,
        ResourceRoutePhase::Waiting | ResourceRoutePhase::Activated
    )
}

fn retained_resource_request(
    request: &ResourceQueueRequest,
    requests: Vec<ResourceRequest>,
) -> Result<ResourceRequest, AppError> {
    requests
        .into_iter()
        .find(|stored| stored.request_id == request.request_id)
        .ok_or_else(|| resource_route_retention_conflict(request))
}

fn resource_route_retention_conflict(request: &ResourceQueueRequest) -> AppError {
    AppError::SubmissionConflict {
        request: request.request_id,
        task: request.task_id,
        message: "origin route says the resource request was already accepted, but the authority has no retained request".into(),
    }
}

fn resource_queue_response_from_stored(
    request: &ResourceQueueRequest,
    authority_machine: MachineId,
    stored: ResourceRequest,
) -> Result<Json<ResourceQueueResponse>, AppError> {
    let stored_digest = normalized_spec_sha256(stored.spec().as_normalized()).map_err(|error| {
        AppError::Internal {
            message: format!("serialize stored resource command specification: {error}"),
        }
    })?;
    let request_digest = normalized_spec_sha256(request.spec.as_normalized()).map_err(|error| {
        AppError::Internal {
            message: format!("serialize resource command specification: {error}"),
        }
    })?;
    if stored.request_id != request.request_id
        || stored.task_id != request.task_id
        || stored.resource_id != request.resource_id
        || stored.origin_machine != request.origin_machine
        || stored_digest != request_digest
    {
        return Err(AppError::SubmissionConflict {
            request: request.request_id,
            task: request.task_id,
            message: "stored resource request does not match this retry".into(),
        });
    }

    let outcome = match stored.state {
        ResourceRequestState::Queued
        | ResourceRequestState::Assigned { .. }
        | ResourceRequestState::Finished { .. } => ResourceQueueOutcome::Waiting,
        ResourceRequestState::CancelledBeforeLaunch => ResourceQueueOutcome::Rejected {
            reason: "cancelled_before_launch".into(),
        },
        ResourceRequestState::Rejected { reason } => ResourceQueueOutcome::Rejected { reason },
    };
    Ok(resource_queue_response(request, authority_machine, outcome))
}

fn resource_queue_response(
    request: &ResourceQueueRequest,
    authority_machine: MachineId,
    outcome: ResourceQueueOutcome,
) -> Json<ResourceQueueResponse> {
    Json(ResourceQueueResponse::new(
        request.protocol_version,
        ResourceQueueReceipt {
            request: request.request_id,
            task: request.task_id,
            origin_machine: request.origin_machine,
            authority_machine,
            resource: request.resource_id,
            outcome,
        },
    ))
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
            origin_machine: route.origin_machine,
            execution_machine: route.execution_machine,
            thread: Some(route.thread),
            submission: route.submission,
            last_execution_state: route.last_execution_state,
            last_updated_at: route.last_updated_at,
            last_accepted_seq: route.last_accepted_seq,
            last_settled_seq: route.last_settled_seq,
            failed_events,
        }),
    }))
}

async fn origin_resource_route_proof(
    State(state): State<AppState>,
    Path(task): Path<TaskId>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<OriginResourceRouteProofBody>, AppError> {
    check(&state, query)?;
    let route = call(&state.store, |reply| StoreMsg::OriginRoute {
        id: task,
        reply,
    })
    .await?;
    let proof = route
        .as_ref()
        .filter(|route| route.task == task)
        .and_then(ResourceRouteProof::from_route);

    Ok(Json(OriginResourceRouteProofBody {
        api_version: API_VERSION,
        proof,
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
    spec::check_cwd(&cwd)?;
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
        state
            .supervisor
            .cast(crate::daemon::actors::SupervisorMsg::DispatchInbox { id })?;
    }
    Ok(Json(EventBody {
        api_version: API_VERSION,
        protocol_version: body.protocol_version,
        result,
    }))
}

fn check_api_version(version: u32) -> Result<(), AppError> {
    if version != API_VERSION {
        return Err(AppError::Usage {
            message: "unsupported API version".into(),
        });
    }
    Ok(())
}

fn check_protocol(version: u32, machine: MachineId) -> Result<(), AppError> {
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
                        normalized.as_ref().is_ok_and(|spec| {
                            serde_json::to_value(stored).ok() == serde_json::to_value(spec).ok()
                        })
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
        (_, Ok(cwd)) => match std::fs::metadata(cwd) {
            Ok(metadata) if metadata.is_dir() => None,
            Ok(_) => Some("cwd_not_directory"),
            Err(error) if error.kind() == ErrorKind::NotFound => Some("cwd_not_found"),
            Err(error) => {
                return Err(AppError::RemoteSubmissionUnavailable {
                    message: format!("cannot inspect executor cwd: {error}"),
                });
            }
        },
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
    let identity = call(&state.supervisor, |reply| {
        crate::daemon::actors::SupervisorMsg::LaunchRemote {
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
        }
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
    if let Some(relative) = text.strip_prefix("~/") {
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
        return Ok(home.join(relative));
    }
    Err(AppError::InvalidSpec {
        pointer: "/spec/cwd".into(),
        value: serde_json::to_value(cwd).unwrap_or(serde_json::Value::Null),
        message: "remote cwd must be absolute or start with ~/".into(),
    })
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
    if query.api_version != API_VERSION {
        return Err(AppError::Usage {
            message: "unsupported API version".into(),
        });
    }
    Ok(())
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

#[cfg(test)]
mod resource_queue_tests {
    use super::*;
    use crate::domain::{TaskId, ThreadId};
    use crate::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
    use crate::resource::{CommandSpec, LoanId, ResourceId};
    use crate::submission::{NormalizedSpecSha256, RequestId};
    use serde_json::json;
    use uuid::Uuid;

    fn request() -> ResourceQueueRequest {
        let thread = ThreadId(Uuid::now_v7());
        let normalized = spec::parse_normalized_value(&json!({
            "api_version": API_VERSION,
            "thread": thread,
            "name": "queued command",
            "cwd": "/tmp",
            "timeout": "30m",
            "workload": {
                "type": "task",
                "command": ["echo", "hello"]
            }
        }))
        .unwrap();
        let command = CommandSpec::try_from(normalized).unwrap();
        ResourceQueueRequest::new(
            CLUSTER_PROTOCOL_VERSION.0,
            MachineId::new(),
            MachineId::new(),
            RequestId::new(),
            TaskId(Uuid::now_v7()),
            ResourceId::new(),
            command,
        )
    }

    fn proof(
        request: &ResourceQueueRequest,
        authority_machine: MachineId,
        phase: ResourceRoutePhase,
    ) -> ResourceRouteProof {
        ResourceRouteProof {
            request: request.request_id,
            task: request.task_id,
            origin_machine: request.origin_machine,
            authority_machine,
            resource: request.resource_id,
            thread: request.spec.as_normalized().thread,
            normalized_spec_sha256: normalized_spec_sha256(request.spec.as_normalized()).unwrap(),
            phase,
        }
    }

    #[test]
    fn missing_or_mismatched_origin_proof_cannot_reach_acceptance() {
        let request = request();
        let authority_machine = request.destination_machine;

        assert!(matches!(
            validate_resource_route_proof(&request, authority_machine, None),
            Err(AppError::RemoteSubmissionUnavailable { .. })
        ));

        let mut mismatched = proof(
            &request,
            authority_machine,
            ResourceRoutePhase::AcceptanceUnknown,
        );
        mismatched.request = RequestId::new();
        assert!(matches!(
            validate_resource_route_proof(&request, authority_machine, Some(mismatched)),
            Err(AppError::SubmissionConflict { .. })
        ));
    }

    #[test]
    fn route_proof_must_bind_authority_thread_and_normalized_spec() {
        let request = request();
        let authority_machine = request.destination_machine;
        for phase in [
            ResourceRoutePhase::AcceptanceUnknown,
            ResourceRoutePhase::Waiting,
            ResourceRoutePhase::Activated,
        ] {
            assert_eq!(
                validate_resource_route_proof(
                    &request,
                    authority_machine,
                    Some(proof(&request, authority_machine, phase.clone())),
                )
                .unwrap(),
                phase
            );
        }

        let mut mismatched_proofs = Vec::new();
        let mut wrong_task = proof(
            &request,
            authority_machine,
            ResourceRoutePhase::AcceptanceUnknown,
        );
        wrong_task.task = TaskId(Uuid::now_v7());
        mismatched_proofs.push(wrong_task);

        let mut wrong_resource = proof(
            &request,
            authority_machine,
            ResourceRoutePhase::AcceptanceUnknown,
        );
        wrong_resource.resource = ResourceId::new();
        mismatched_proofs.push(wrong_resource);

        let mut wrong_origin = proof(
            &request,
            authority_machine,
            ResourceRoutePhase::AcceptanceUnknown,
        );
        wrong_origin.origin_machine = MachineId::new();
        mismatched_proofs.push(wrong_origin);

        let mut wrong_authority = proof(
            &request,
            authority_machine,
            ResourceRoutePhase::AcceptanceUnknown,
        );
        wrong_authority.authority_machine = MachineId::new();
        mismatched_proofs.push(wrong_authority);

        let mut wrong_thread = proof(
            &request,
            authority_machine,
            ResourceRoutePhase::AcceptanceUnknown,
        );
        wrong_thread.thread = ThreadId(Uuid::now_v7());
        mismatched_proofs.push(wrong_thread);

        let mut wrong_spec = proof(
            &request,
            authority_machine,
            ResourceRoutePhase::AcceptanceUnknown,
        );
        wrong_spec.normalized_spec_sha256 =
            NormalizedSpecSha256::deserialize(json!("00".repeat(32))).unwrap();
        mismatched_proofs.push(wrong_spec);

        for mismatched in mismatched_proofs {
            assert!(matches!(
                validate_resource_route_proof(&request, authority_machine, Some(mismatched)),
                Err(AppError::SubmissionConflict { .. })
            ));
        }
    }

    #[test]
    fn terminal_origin_phases_return_typed_rejections() {
        assert_eq!(
            rejected_resource_route_reason(&ResourceRoutePhase::CancelledBeforeLaunch),
            Some("cancelled_before_launch".into())
        );
        assert_eq!(
            rejected_resource_route_reason(&ResourceRoutePhase::Rejected {
                reason: "origin_cancelled".into(),
            }),
            Some("origin_cancelled".into())
        );
        assert_eq!(
            rejected_resource_route_reason(&ResourceRoutePhase::Activated),
            None
        );
    }

    #[test]
    fn accepted_origin_phases_require_a_retained_authority_request() {
        assert!(!resource_route_requires_retained_request(
            &ResourceRoutePhase::AcceptanceUnknown
        ));
        assert!(resource_route_requires_retained_request(
            &ResourceRoutePhase::Waiting
        ));
        assert!(resource_route_requires_retained_request(
            &ResourceRoutePhase::Activated
        ));
    }

    #[test]
    fn accepted_origin_retry_without_retained_authority_row_conflicts() {
        let request = request();
        assert!(matches!(
            retained_resource_request(&request, Vec::new()),
            Err(AppError::SubmissionConflict { .. })
        ));
    }

    #[test]
    fn accepted_retry_receipt_stays_waiting_after_queue_state_changes() {
        let request = request();
        let authority_machine = request.destination_machine;
        let waiting = accepted_request(&request, ResourceRequestState::Queued);
        let assigned = accepted_request(
            &request,
            ResourceRequestState::Assigned {
                loan_id: LoanId::new(),
            },
        );
        let finished = accepted_request(
            &request,
            ResourceRequestState::Finished {
                outcome: crate::domain::ExitReason::Exit { code: 0 },
            },
        );

        let first = resource_queue_response_from_stored(&request, authority_machine, waiting)
            .unwrap()
            .0;
        let retry_assigned =
            resource_queue_response_from_stored(&request, authority_machine, assigned)
                .unwrap()
                .0;
        let retry_finished =
            resource_queue_response_from_stored(&request, authority_machine, finished)
                .unwrap()
                .0;

        assert_eq!(first, retry_assigned);
        assert_eq!(first, retry_finished);
        assert_eq!(first.receipt.outcome, ResourceQueueOutcome::Waiting);

        let cancelled = accepted_request(&request, ResourceRequestState::CancelledBeforeLaunch);
        let rejected = accepted_request(
            &request,
            ResourceRequestState::Rejected {
                reason: "cancelled_before_acceptance".into(),
            },
        );
        assert_eq!(
            resource_queue_response_from_stored(&request, authority_machine, cancelled)
                .unwrap()
                .0
                .receipt
                .outcome,
            ResourceQueueOutcome::Rejected {
                reason: "cancelled_before_launch".into(),
            }
        );
        assert_eq!(
            resource_queue_response_from_stored(&request, authority_machine, rejected)
                .unwrap()
                .0
                .receipt
                .outcome,
            ResourceQueueOutcome::Rejected {
                reason: "cancelled_before_acceptance".into(),
            }
        );
    }

    fn accepted_request(
        request: &ResourceQueueRequest,
        state: ResourceRequestState,
    ) -> ResourceRequest {
        let mut stored = ResourceRequest::new(
            request.request_id,
            request.task_id,
            request.resource_id,
            crate::resource::AcceptanceSequence::new(1),
            request.origin_machine,
            request.spec.as_normalized().clone(),
        )
        .unwrap();
        stored.state = state;
        stored
    }
}
