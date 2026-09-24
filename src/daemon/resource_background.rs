//! Two-machine first background launch: supervisor-owned route and authority acceptance
//!
//! The supervisor machine reads the resource from its fixed authority, saves one
//! fixed-ID origin route with the full spec, its callback context, and the exact
//! supervisor assignment and resource revision, and only then sends the launch.
//! The authority reads that saved route back as evidence before it accepts. A
//! lost reply or restart repeats the same launch, and the authority answers an
//! exact retry from its receipt, so no second trainer starts
//!
//! The same document binds the trainer attempt of the running task. The
//! supervisor names only the attempt; the authority reads the attempt request
//! and probes the held ownership lock itself

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::OnceLock;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::warn;

use super::AppState;
use super::actors::supervisor::RemoteBackgroundLaunch;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::cluster::ReadQuery;
use crate::domain::{API_VERSION, AgentKind, TaskEnv, TaskId};
use crate::error::AppError;
use crate::fleet::http::{ClusterClient, ClusterResponse};
use crate::invocation::resolve_agent_binary;
use crate::machine::MachineId;
use crate::resource::ResourceId;
use crate::resource::api::{
    ResourceBackgroundSubmitOutcome, ResourceBackgroundSubmitResponse, TrainerAttemptResponse,
};
use crate::resource::background_launch::{
    BackgroundLaunchBinding, BackgroundSupervisorAssignment, RESOURCE_BACKGROUND_PATH,
    RESOURCE_BACKGROUND_PROTOCOL_VERSION, RESOURCE_BACKGROUND_ROUTE_PROOF_PATH,
    RemoteBackgroundLaunchReceipt, ResourceBackgroundOperation, ResourceBackgroundOutcome,
    ResourceBackgroundRejection, ResourceBackgroundRequest, ResourceBackgroundResponse,
};
use crate::resource::store::ResourceStoreError;
use crate::resource::watcher::AttemptBinding;
use crate::spec::{self, NormalizedSpec};
use crate::store::{
    BackgroundLaunchAcceptance, BackgroundLaunchError, ResourceBackgroundRouteResult,
};
use crate::submission::{
    CallbackContext, NewResourceBackgroundRoute, OriginRoute, RequestId,
    ResourceBackgroundRoutePhase, ResourceBackgroundRouteProof, SubmissionState,
    normalized_spec_sha256,
};

const LAUNCH_SHARDS: usize = 64;
static LAUNCH_PERMITS: OnceLock<[Semaphore; LAUNCH_SHARDS]> = OnceLock::new();

/// Saved route proof returned by the supervisor machine
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceBackgroundRouteProofBody {
    /// Public API version
    pub api_version: u32,
    /// Proof for the named task, when this machine saved a valid background route
    pub proof: Option<ResourceBackgroundRouteProof>,
}

/// Socket submission of one first background launch whose authority is remote
pub(super) struct RemoteBackgroundSubmit {
    /// Resource named by the socket path
    pub(super) resource: ResourceId,
    /// Fixed resource authority located by Fleet or the saved route
    pub(super) authority: MachineId,
    /// Stable caller retry identity
    pub(super) request_id: RequestId,
    /// Full normalized trainer spec
    pub(super) spec: NormalizedSpec,
    /// Environment saved for the supervisor-side callback
    pub(super) env: TaskEnv,
    /// Absolute directory used to resolve the supervisor-side Codex executable
    pub(super) callback_cwd: PathBuf,
}

/// Cluster routes: authority operations and supervisor route evidence
pub(super) fn cluster_routes() -> Router<AppState> {
    Router::new()
        .route(RESOURCE_BACKGROUND_PATH, post(receive))
        .route(
            &format!("{RESOURCE_BACKGROUND_ROUTE_PROOF_PATH}/{{task}}"),
            get(route_proof),
        )
}

// ---- authority side ----

async fn receive(
    State(state): State<AppState>,
    Json(request): Json<ResourceBackgroundRequest>,
) -> Result<Json<ResourceBackgroundResponse>, AppError> {
    state
        .machine
        .identity
        .check_destination(request.destination_machine)?;
    super::cluster::check_protocol(request.protocol_version, request.source_machine)?;
    request.validate().map_err(|error| AppError::Usage {
        message: format!("invalid remote background request: {error}"),
    })?;

    let outcome = match &request.operation {
        ResourceBackgroundOperation::Launch { spec, .. } => {
            let Some(receipt) = request.launch_receipt() else {
                return Err(AppError::Internal {
                    message: "a launch operation has no launch receipt".into(),
                });
            };
            accept_launch(&state, receipt, spec.clone()).await?
        }
        ResourceBackgroundOperation::BindTrainerAttempt {
            task_id,
            attempt_binding,
        } => {
            bind_attempt(
                &state,
                request.assignment,
                *task_id,
                attempt_binding.clone(),
            )
            .await?
        }
    };
    Ok(Json(ResourceBackgroundResponse::new(&request, outcome)))
}

/// Accept one launch after the supervisor machine proves its saved route
async fn accept_launch(
    state: &AppState,
    receipt: RemoteBackgroundLaunchReceipt,
    spec: NormalizedSpec,
) -> Result<ResourceBackgroundOutcome, AppError> {
    let proof = fetch_route_proof(state, receipt.binding.origin_machine(), receipt.task_id).await?;
    if let Err(reason) = check_route_evidence(&receipt, proof) {
        warn!(
            request = %receipt.request_id.0,
            task = %receipt.task_id,
            ?reason,
            "background launch route evidence refused"
        );
        return Ok(ResourceBackgroundOutcome::Rejected { reason });
    }

    let accepted = call(&state.supervisor, |reply| {
        SupervisorMsg::LaunchRemoteBackground {
            launch: Box::new(RemoteBackgroundLaunch { receipt, spec }),
            reply,
        }
    })
    .await?;
    let acceptance = match accepted {
        Ok(BackgroundLaunchAcceptance::Inserted { .. }) => {
            ResourceBackgroundSubmitOutcome::Inserted
        }
        Ok(BackgroundLaunchAcceptance::Existing { state, .. }) => {
            ResourceBackgroundSubmitOutcome::Existing { state }
        }
        Ok(BackgroundLaunchAcceptance::UnsupportedRemoteSupervisor { .. }) => {
            return Ok(ResourceBackgroundOutcome::Rejected {
                reason: ResourceBackgroundRejection::NotCurrentSupervisor,
            });
        }
        Err(error) => {
            return Ok(ResourceBackgroundOutcome::Rejected {
                reason: launch_rejection(error)?,
            });
        }
    };
    let resource =
        super::resource_api::local_resource(state, receipt.binding.assignment.resource_id).await?;
    Ok(ResourceBackgroundOutcome::Accepted {
        receipt,
        acceptance,
        resource,
    })
}

/// Keep storage failures retryable and turn every domain refusal into a typed rejection
fn launch_rejection(error: BackgroundLaunchError) -> Result<ResourceBackgroundRejection, AppError> {
    use BackgroundLaunchError as Error;
    use ResourceBackgroundRejection as Rejection;

    Ok(match error {
        Error::Resource(
            ResourceStoreError::ResourceNotFound | ResourceStoreError::WrongAuthority { .. },
        ) => Rejection::ResourceNotFound,
        // the request already names the assignment's thread, so another thread
        // means the assignment changed
        Error::NotCurrentSupervisor | Error::NotSupervisorThread { .. } => {
            Rejection::NotCurrentSupervisor
        }
        Error::StaleRevision { expected, actual } => Rejection::StaleRevision { expected, actual },
        Error::ActiveLoan { loan_id } => Rejection::ActiveLoan { loan_id },
        Error::QueuedWorkAhead { request_id } => Rejection::QueuedWorkAhead { request_id },
        Error::BackgroundTaskActive { task_id, state } => {
            Rejection::BackgroundTaskActive { task_id, state }
        }
        Error::BackgroundTaskMissing { task_id } => Rejection::BackgroundTaskMissing { task_id },
        Error::LaunchPending { task_id } => Rejection::LaunchPending { task_id },
        Error::PredecessorReleaseUnproven { task_id }
        | Error::PredecessorOwnershipUnproven { task_id, .. } => {
            Rejection::PredecessorReleaseUnproven { task_id }
        }
        Error::ConflictingRetry { .. } => Rejection::ConflictingRetry,
        Error::IdentityConflict { .. } | Error::Identity(crate::store::IdentityError::Conflict) => {
            Rejection::IdentityConflict
        }
        Error::UnsupportedCommand(error) => Rejection::UnsupportedCommand {
            reason: error.to_string(),
        },
        // the spec or executor environment cannot run here; nothing was written
        Error::TaskRecords(
            error @ (AppError::CwdNotFound { .. }
            | AppError::ExecutableMissing { .. }
            | AppError::InvalidSpec { .. }
            | AppError::Usage { .. }),
        ) => Rejection::InvalidSpec {
            reason: error.to_string(),
        },
        error => {
            return Err(AppError::Internal {
                message: format!("background launch owner failed: {error}"),
            });
        }
    })
}

/// Compare the supervisor machine's saved route with one launch request
fn check_route_evidence(
    receipt: &RemoteBackgroundLaunchReceipt,
    proof: Option<ResourceBackgroundRouteProof>,
) -> Result<(), ResourceBackgroundRejection> {
    let proof = proof.ok_or(ResourceBackgroundRejection::RouteEvidenceMissing)?;
    if proof.request != receipt.request_id
        || proof.task != receipt.task_id
        || proof.binding != receipt.binding
        || proof.normalized_spec_sha256 != receipt.normalized_spec_sha256
        || matches!(proof.phase, ResourceBackgroundRoutePhase::Rejected { .. })
    {
        return Err(ResourceBackgroundRejection::RouteEvidenceMismatch);
    }
    Ok(())
}

/// Read the background route that the supervisor machine saved for one task
async fn fetch_route_proof(
    state: &AppState,
    origin: MachineId,
    task: TaskId,
) -> Result<Option<ResourceBackgroundRouteProof>, AppError> {
    let unavailable = |message: String| AppError::RemoteSubmissionUnavailable {
        message: format!("supervisor route proof is unavailable: {message}"),
    };
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| unavailable("fleet is disabled".into()))?;
    let destination = fleet
        .connect(origin)
        .await
        .map_err(|error| unavailable(error.to_string()))?;
    let path = format!(
        "{RESOURCE_BACKGROUND_ROUTE_PROOF_PATH}/{task}?api_version={API_VERSION}&destination_machine={origin}"
    );
    let response = ClusterClient::default()
        .get(&destination.address, &path)
        .await
        .map_err(|error| unavailable(error.to_string()))?;
    if response.status != StatusCode::OK {
        return Err(unavailable(format!("HTTP {}", response.status)));
    }
    let body: ResourceBackgroundRouteProofBody = serde_json::from_slice(&response.body)
        .map_err(|error| unavailable(format!("invalid response: {error}")))?;
    if body.api_version != API_VERSION {
        return Err(unavailable("unsupported API version".into()));
    }
    Ok(body.proof.filter(|proof| proof.task == task))
}

/// Bind one trainer attempt for the current remote supervisor assignment
async fn bind_attempt(
    state: &AppState,
    assignment: BackgroundSupervisorAssignment,
    task_id: TaskId,
    attempt_binding: AttemptBinding,
) -> Result<ResourceBackgroundOutcome, AppError> {
    let resource_id = assignment.resource_id;
    let resource = match super::resource_api::local_resource(state, resource_id).await {
        Ok(resource) => resource,
        Err(AppError::ResourceNotFound { .. }) => {
            return Ok(ResourceBackgroundOutcome::Rejected {
                reason: ResourceBackgroundRejection::ResourceNotFound,
            });
        }
        Err(error) => return Err(error),
    };
    if resource.supervisor.machine == resource.authority_machine()
        || !assignment.is_current(&resource)
    {
        return Ok(ResourceBackgroundOutcome::Rejected {
            reason: ResourceBackgroundRejection::NotCurrentSupervisor,
        });
    }
    // the resource owner registers a confirmed start; reconcile first so a
    // running launch is not refused only because its wake-up is still queued
    call(&state.supervisor, |reply| {
        SupervisorMsg::ReconcileResource {
            id: resource_id,
            reply,
        }
    })
    .await?;

    match super::resource_api::authority_trainer_attempt(
        state,
        resource_id,
        task_id,
        attempt_binding,
    )
    .await
    {
        Ok(response) => {
            // a release action that waited for this association can now bind its watcher
            if let Err(error) = call(&state.supervisor, |reply| {
                SupervisorMsg::ReconcileResource {
                    id: resource_id,
                    reply,
                }
            })
            .await
            {
                warn!(%task_id, "resource wake after trainer association: {error}");
            }
            Ok(ResourceBackgroundOutcome::TrainerAttemptBound {
                resource: response.resource,
                task_id: response.task_id,
                runtime_root: response.runtime_root,
                attempt_binding: response.attempt_binding,
            })
        }
        Err(AppError::ResourceActionNotAllowed { message, .. }) => {
            Ok(ResourceBackgroundOutcome::Rejected {
                reason: ResourceBackgroundRejection::TrainerAttemptRefused { reason: message },
            })
        }
        Err(AppError::ResourceOperationConflict { .. }) => {
            Ok(ResourceBackgroundOutcome::Rejected {
                reason: ResourceBackgroundRejection::TrainerAttemptConflict,
            })
        }
        Err(error) => Err(error),
    }
}

/// Serve the saved background route proof for one task owned by this supervisor machine
async fn route_proof(
    State(state): State<AppState>,
    Path(task): Path<TaskId>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<ResourceBackgroundRouteProofBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(query.destination_machine)?;
    super::cluster::check_api_version(query.api_version)?;
    let route = call(&state.store, |reply| StoreMsg::OriginRoute {
        id: task,
        reply,
    })
    .await?;
    let local = state.machine.identity.machine;
    let proof = route
        .as_ref()
        .filter(|route| route.task == task && route.origin_machine == local)
        .and_then(ResourceBackgroundRouteProof::from_route);
    Ok(Json(ResourceBackgroundRouteProofBody {
        api_version: API_VERSION,
        proof,
    }))
}

// ---- supervisor side ----

async fn lock_request(request: RequestId) -> Result<SemaphorePermit<'static>, AppError> {
    let permits = LAUNCH_PERMITS.get_or_init(|| std::array::from_fn(|_| Semaphore::new(1)));
    let mut hasher = DefaultHasher::new();
    request.hash(&mut hasher);
    let shard = (hasher.finish() as usize) % LAUNCH_SHARDS;
    permits[shard]
        .acquire()
        .await
        .map_err(|error| AppError::Internal {
            message: format!("background launch permit unavailable: {error}"),
        })
}

/// Submit one first background launch to a remote authority, or answer its saved route
///
/// A new request saves its fixed-ID route before the first send. A retry with
/// the same request reuses the saved identities and spec, and different content
/// conflicts. An uncertain send leaves the route for the same retry
pub(super) async fn submit(
    state: &AppState,
    input: RemoteBackgroundSubmit,
) -> Result<ResourceBackgroundSubmitResponse, AppError> {
    let _request_guard = lock_request(input.request_id).await?;
    let saved = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request: input.request_id,
        reply,
    })
    .await?;
    let route = match saved {
        Some(route) => {
            check_retry(&route, &input)?;
            route
        }
        None => create_route(state, &input).await?,
    };
    send_saved_launch(state, &route).await
}

/// Retry background routes whose authority acceptance was unknown at daemon startup
pub(super) async fn recover(state: AppState) {
    let routes = match call(&state.store, |reply| {
        StoreMsg::UnknownResourceBackgroundRoutes { reply }
    })
    .await
    {
        Ok(routes) => routes,
        Err(error) => {
            warn!("cannot scan unresolved background launch routes: {error}");
            return;
        }
    };
    for route in routes {
        let Ok(_request_guard) = lock_request(route.request).await else {
            continue;
        };
        if let Err(error) = send_saved_launch(&state, &route).await {
            warn!(task = %route.task, "background launch recovery: {error}");
        }
    }
}

/// A retry must repeat the saved resource, authority, and full normalized spec
///
/// The saved callback context is kept; a retry cannot move it
fn check_retry(route: &OriginRoute, input: &RemoteBackgroundSubmit) -> Result<(), AppError> {
    let conflict = |message: &str| AppError::ResourceOperationConflict {
        resource: input.resource,
        operation: Some(input.request_id.0),
        message: message.into(),
    };
    let SubmissionState::ResourceBackground { binding, .. } = &route.submission else {
        return Err(conflict("request id belongs to another route"));
    };
    let Some(saved_spec) = route.current_spec() else {
        return Err(conflict("saved background route has no normalized spec"));
    };
    if binding.assignment.resource_id != input.resource
        || binding.execution_machine() != input.authority
        || serde_json::to_value(saved_spec)? != serde_json::to_value(&input.spec)?
    {
        return Err(conflict(
            "request id was retried with a different resource, authority, or spec",
        ));
    }
    Ok(())
}

/// Read the resource from its authority, then save the fixed-ID route before any send
async fn create_route(
    state: &AppState,
    input: &RemoteBackgroundSubmit,
) -> Result<OriginRoute, AppError> {
    let resource_id = input.resource;
    let unavailable = |message: String| AppError::ResourceOperationUnavailable {
        resource: resource_id,
        message: format!("{message}; nothing was written"),
    };
    let resource = super::resource_api::remote_detail(state, resource_id)
        .await?
        .resource;
    let local = state.machine.identity.machine;
    if resource.authority_machine() != input.authority {
        return Err(unavailable(format!(
            "the resource authority moved from {} to {}",
            input.authority,
            resource.authority_machine()
        )));
    }
    if resource.supervisor.machine != local {
        return Err(unavailable(format!(
            "the supervisor thread runs on machine {}; submit the launch there",
            resource.supervisor.machine
        )));
    }
    if input.spec.thread != resource.supervisor.thread {
        return Err(AppError::ResourceActionNotAllowed {
            resource: resource_id,
            message: format!(
                "background launch thread {} is not the supervisor thread {}",
                input.spec.thread, resource.supervisor.thread
            ),
        });
    }
    if !input.callback_cwd.is_absolute() {
        return Err(AppError::InvalidSpec {
            pointer: "/callback_cwd".into(),
            value: serde_json::to_value(&input.callback_cwd)?,
            message: "callback directory must be absolute".into(),
        });
    }
    spec::check_cwd(&input.callback_cwd)?;
    let codex = resolve_agent_binary(AgentKind::Codex, &input.env.path, &input.callback_cwd)?;
    let route = OriginRoute::new_resource_background(NewResourceBackgroundRoute {
        request: input.request_id,
        task: TaskId::new(),
        callback: CallbackContext {
            env: input.env.clone(),
            cwd: input.callback_cwd.clone(),
            codex: codex.into(),
        },
        spec: input.spec.clone(),
        binding: BackgroundLaunchBinding {
            assignment: BackgroundSupervisorAssignment::of(&resource),
            expected_state_revision: resource.state_revision,
        },
    })
    .map_err(|error| AppError::Usage {
        message: format!("background launch route is invalid: {error}"),
    })?;

    match call(&state.store, |reply| StoreMsg::InsertOriginRoute {
        route: Box::new(route),
        reply,
    })
    .await
    {
        Ok(saved) => Ok(saved),
        Err(error) => {
            // a concurrent first call with the same request may have saved it
            let saved = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
                request: input.request_id,
                reply,
            })
            .await?;
            let Some(saved) = saved else {
                return Err(error);
            };
            check_retry(&saved, input)?;
            Ok(saved)
        }
    }
}

/// Send the launch of one saved route and apply a definitive answer to it
///
/// A saved refusal is returned without a send. An accepted route is sent again
/// because the authority answers an exact retry from its receipt, which also
/// returns the current resource and task state
async fn send_saved_launch(
    state: &AppState,
    route: &OriginRoute,
) -> Result<ResourceBackgroundSubmitResponse, AppError> {
    let SubmissionState::ResourceBackground { binding, phase } = &route.submission else {
        return Err(AppError::ClusterTaskConflict { task: route.task });
    };
    let resource_id = binding.assignment.resource_id;
    if let ResourceBackgroundRoutePhase::Rejected { reason } = phase {
        return Err(rejection_error(resource_id, Some(route.request.0), reason));
    }
    let spec = route
        .current_spec()
        .ok_or(AppError::ClusterTaskConflict { task: route.task })?;
    let expected = RemoteBackgroundLaunchReceipt {
        binding: *binding,
        request_id: route.request,
        task_id: route.task,
        normalized_spec_sha256: normalized_spec_sha256(spec)?,
    };
    let operation = ResourceBackgroundOperation::Launch {
        expected_state_revision: binding.expected_state_revision,
        request_id: route.request,
        task_id: route.task,
        spec: spec.clone(),
        normalized_spec_sha256: expected.normalized_spec_sha256,
    };
    let unresolved = |message: String| {
        if matches!(phase, ResourceBackgroundRoutePhase::Accepted) {
            AppError::ResourceAuthorityUnavailable {
                resource: resource_id,
                machine: binding.execution_machine(),
                message: format!(
                    "the launch of task {} was accepted, but its current state is unavailable: \
                     {message}",
                    route.task
                ),
            }
        } else {
            unknown(route, resource_id, message)
        }
    };
    let outcome = send(state, binding.assignment, operation)
        .await
        .map_err(|error| unresolved(error.to_string()))?;

    match outcome {
        ResourceBackgroundOutcome::Accepted {
            receipt,
            acceptance,
            resource,
        } => {
            if receipt != expected || resource.id != resource_id {
                return Err(unresolved(
                    "the authority answered for another launch".into(),
                ));
            }
            resolve(
                state,
                route,
                resource_id,
                ResourceBackgroundRouteResult::Accepted(receipt),
            )
            .await?;
            Ok(ResourceBackgroundSubmitResponse {
                api_version: API_VERSION,
                request_id: route.request,
                task_id: route.task,
                resource,
                outcome: acceptance,
            })
        }
        ResourceBackgroundOutcome::Rejected { reason } => {
            let error = rejection_error(resource_id, Some(route.request.0), &reason);
            resolve(
                state,
                route,
                resource_id,
                ResourceBackgroundRouteResult::Rejected(reason),
            )
            .await?;
            Err(error)
        }
        ResourceBackgroundOutcome::TrainerAttemptBound { .. } => Err(unresolved(
            "the authority answered a launch with another outcome".into(),
        )),
    }
}

async fn resolve(
    state: &AppState,
    route: &OriginRoute,
    resource: ResourceId,
    result: ResourceBackgroundRouteResult,
) -> Result<OriginRoute, AppError> {
    call(&state.store, |reply| {
        StoreMsg::ResolveResourceBackgroundRoute {
            task: route.task,
            result: Box::new(result),
            reply,
        }
    })
    .await
    .map_err(|error| {
        unknown(
            route,
            resource,
            format!("cannot save the authority result: {error}"),
        )
    })
}

/// Bind one trainer attempt on the remote authority for this supervisor machine
///
/// The request names the current supervisor assignment read from the authority.
/// An exact retry returns the saved association; another attempt conflicts
pub(super) async fn bind_trainer_attempt(
    state: &AppState,
    resource_id: ResourceId,
    task_id: TaskId,
    attempt_binding: AttemptBinding,
) -> Result<TrainerAttemptResponse, AppError> {
    let resource = super::resource_api::remote_detail(state, resource_id)
        .await?
        .resource;
    if resource.supervisor.machine != state.machine.identity.machine {
        return Err(AppError::ResourceOperationUnavailable {
            resource: resource_id,
            message: format!(
                "the supervisor thread runs on machine {}; bind the trainer attempt there",
                resource.supervisor.machine
            ),
        });
    }
    let operation = ResourceBackgroundOperation::BindTrainerAttempt {
        task_id,
        attempt_binding: attempt_binding.clone(),
    };
    let unknown = |message: String| AppError::ResourceOutcomeUnknown {
        resource: resource_id,
        operation: None,
        message: format!("{message}; retry the same trainer attempt"),
    };
    let outcome = send(
        state,
        BackgroundSupervisorAssignment::of(&resource),
        operation,
    )
    .await
    .map_err(|error| unknown(error.to_string()))?;
    match outcome {
        ResourceBackgroundOutcome::TrainerAttemptBound {
            resource,
            task_id: bound_task,
            runtime_root,
            attempt_binding: bound_attempt,
        } => {
            if bound_task != task_id
                || bound_attempt != attempt_binding
                || resource.id != resource_id
            {
                return Err(unknown("the authority answered for another attempt".into()));
            }
            Ok(TrainerAttemptResponse {
                api_version: API_VERSION,
                resource,
                task_id,
                runtime_root,
                attempt_binding,
            })
        }
        ResourceBackgroundOutcome::Rejected { reason } => {
            Err(rejection_error(resource_id, None, &reason))
        }
        ResourceBackgroundOutcome::Accepted { .. } => Err(unknown(
            "the authority answered a trainer attempt with another outcome".into(),
        )),
    }
}

/// Send one operation to the authority and decode its strict typed answer
async fn send(
    state: &AppState,
    assignment: BackgroundSupervisorAssignment,
    operation: ResourceBackgroundOperation,
) -> Result<ResourceBackgroundOutcome, AppError> {
    let authority = assignment.authority_machine;
    let unavailable = |message: String| AppError::MachineUnavailable {
        machine: authority,
        message,
    };
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| unavailable("fleet is disabled".into()))?;
    let destination = fleet
        .connect(authority)
        .await
        .map_err(|error| unavailable(error.to_string()))?;
    let request = ResourceBackgroundRequest::new(destination.protocol.0, assignment, operation);
    let response = ClusterClient::default()
        .post_json(&destination.address, RESOURCE_BACKGROUND_PATH, &request)
        .await
        .map_err(|error| unavailable(error.to_string()))?;
    decode_response(&request, response)
}

fn decode_response(
    request: &ResourceBackgroundRequest,
    response: ClusterResponse,
) -> Result<ResourceBackgroundOutcome, AppError> {
    if !response.status.is_success() {
        return Err(crate::client::map_error(response.status, &response.body));
    }
    let invalid = |message: String| AppError::RemoteSubmissionUnavailable {
        message: format!("invalid resource authority response: {message}"),
    };
    let value: Value =
        serde_json::from_slice(&response.body).map_err(|error| invalid(error.to_string()))?;
    let body: ResourceBackgroundResponse =
        serde_json::from_value(value).map_err(|error| invalid(error.to_string()))?;
    if body.api_version != API_VERSION
        || body.protocol_version != request.protocol_version
        || body.background_protocol_version != RESOURCE_BACKGROUND_PROTOCOL_VERSION
        || body.destination_machine != request.destination_machine
        || body.resource_id != request.assignment.resource_id
    {
        return Err(invalid("response names another route or version".into()));
    }
    Ok(body.outcome)
}

/// Map one definitive authority refusal to the socket error for its resource
fn rejection_error(
    resource: ResourceId,
    operation: Option<uuid::Uuid>,
    reason: &ResourceBackgroundRejection,
) -> AppError {
    use ResourceBackgroundRejection as Rejection;

    let not_allowed = |message: String| AppError::ResourceActionNotAllowed { resource, message };
    let conflict = |message: String| AppError::ResourceOperationConflict {
        resource,
        operation,
        message,
    };
    match reason {
        Rejection::ResourceNotFound => AppError::ResourceNotFound { resource },
        Rejection::StaleRevision { expected, actual } => AppError::ResourceStaleRevision {
            resource,
            expected: expected.get(),
            current: actual.get(),
        },
        Rejection::NotCurrentSupervisor => {
            not_allowed("the request does not come from the current supervisor assignment".into())
        }
        Rejection::ActiveLoan { loan_id } => {
            not_allowed(format!("loan {} owns the resource", loan_id.as_uuid()))
        }
        Rejection::QueuedWorkAhead { request_id } => not_allowed(format!(
            "queued resource request {} is ahead of the background launch",
            request_id.0
        )),
        Rejection::BackgroundTaskActive { task_id, state } => {
            not_allowed(format!("registered background task {task_id} is {state}"))
        }
        Rejection::BackgroundTaskMissing { task_id } => not_allowed(format!(
            "registered background task {task_id} has no task record"
        )),
        Rejection::LaunchPending { task_id } => {
            not_allowed(format!("background launch task {task_id} is still pending"))
        }
        Rejection::PredecessorReleaseUnproven { task_id } => not_allowed(format!(
            "background task {task_id} has no verified resource release"
        )),
        Rejection::TrainerAttemptRefused { reason } => not_allowed(reason.clone()),
        Rejection::RouteEvidenceMissing | Rejection::RouteEvidenceMismatch => conflict(format!(
            "the authority did not accept this machine's saved route: {reason:?}"
        )),
        Rejection::ConflictingRetry | Rejection::IdentityConflict => conflict(format!(
            "the request or task identity is already used: {reason:?}"
        )),
        Rejection::TrainerAttemptConflict => {
            conflict("the task is associated with another trainer attempt".into())
        }
        Rejection::UnsupportedCommand { reason } => AppError::ResourceOperationUnavailable {
            resource,
            message: format!(
                "a background launch accepts only the maintained direct-segment trainer, whose \
                 ownership lock release proof can verify; this command does not match it: {reason}"
            ),
        },
        Rejection::InvalidSpec { reason } => AppError::Usage {
            message: format!("the resource authority cannot run the spec: {reason}"),
        },
    }
}

fn unknown(route: &OriginRoute, resource: ResourceId, message: String) -> AppError {
    AppError::ResourceOutcomeUnknown {
        resource,
        operation: Some(route.request.0),
        message: format!(
            "{message}; the saved route keeps task {} bound to this request; retry with the same \
             request id {}",
            route.task, route.request.0
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{launch_rejection, rejection_error};
    use crate::domain::TaskId;
    use crate::error::AppError;
    use crate::resource::ResourceId;
    use crate::resource::background_launch::ResourceBackgroundRejection;
    use crate::store::BackgroundLaunchError;

    #[test]
    fn unproven_predecessor_is_a_definitive_remote_refusal() {
        let resource = ResourceId::new();
        let task_id = TaskId::new();
        let rejection =
            launch_rejection(BackgroundLaunchError::PredecessorReleaseUnproven { task_id })
                .expect("domain refusal");
        assert_eq!(
            rejection,
            ResourceBackgroundRejection::PredecessorReleaseUnproven { task_id }
        );
        assert!(matches!(
            rejection_error(resource, None, &rejection),
            AppError::ResourceActionNotAllowed { resource: actual, .. } if actual == resource
        ));
    }
}
