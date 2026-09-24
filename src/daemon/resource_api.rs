//! Resource reads, controls, and submissions on the socket, dashboard, and Fleet
//!
//! Every view comes from the fixed authority's durable resource, loan, request,
//! notice, and task state. A peer that cannot answer is reported as an
//! unavailable authority, never as an idle resource. Every mutation runs on the
//! authority through the StoreActor and the existing owners: origin-owned
//! cancellation, the resource queue submission path, and the notice sender

use crate::resource::ReturnContext;
use crate::resource::trainer_publication::AttemptBinding;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{FromRequest, Path, Query, Request, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::task::JoinSet;
use tracing::warn;
use uuid::Uuid;

use super::AppState;
use super::actors::resource::{
    ReleaseWatcherAttentionReason, ReleaseWatcherStatus, ResourceActorInspection,
};
use super::actors::supervisor::BackgroundLaunch;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::api::views::TaskSummary;
use super::cluster::{OriginResourceCancellationOutcome, ReadQuery};
use super::resource_submit::{ResourceSubmitInput, ResourceSubmitOutcome};
use crate::cancellation::ResourceCancellationTarget;
use crate::domain::{API_VERSION, TaskEnv, TaskId, ThreadId};
use crate::error::AppError;
use crate::fleet::http::{ClusterClient, ClusterResponse};
use crate::fleet::runtime::FleetHandle;
use crate::machine::MachineId;
use crate::resource::api::{
    AttentionCode, AttentionView, BackgroundLaunchReservation, BackgroundLaunchReservationStatus,
    BrowserResourceAction, CLUSTER_RESOURCE_PENDING_PATH, CLUSTER_RESOURCES_PATH,
    ClusterPendingActions, ClusterResourceControl, ClusterResourceDetail, ClusterResourceList,
    InitialIdleBody, InitialIdleResponse, OperatorReleaseBody, OperatorReleaseResponse,
    PendingActionList, PendingActionPhase, PendingActionView, RESOURCE_PENDING_PATH,
    RESOURCE_REGISTER_PATH, RequestCancelBody, ResourceActionBody, ResourceBackgroundSubmitOutcome,
    ResourceBackgroundSubmitResponse, ResourceControl, ResourceDetail, ResourceList,
    ResourceOverview, ResourceRegisterBody, ResourceRegistration, ResourceRequestSubmitOutcome,
    ResourceRequestSubmitResponse, ResourceRequestView, ResourceTaskSummary,
    SupervisorReplacementBody, TrainerAttemptBody, TrainerAttemptResponse, UnavailableAuthority,
};
use crate::resource::initial_idle::InitialIdleRefusal;
use crate::resource::operator_release::OperatorGpuFreeRefusal;
use crate::resource::{
    AssignmentRevision, DeliveryAttemptId, IdleProofGap, Loan, LoanPhase, LoanState, Resource,
    ResourceId, ResourceQueueAttentionReason, ResourceQueueReconcileOutcome, ResourceRequest,
    ResourceRequestState, ResourceRevision, RestoreAttentionReason, SupervisorAddress,
    SupervisorNoticeDelivery,
};
use crate::spec::{self, NormalizedSpec};
use crate::store::{
    BackgroundLaunchAcceptance, BackgroundLaunchError, BackgroundLaunchPhase, InitialIdleError,
    OperatorGpuFreeError, ResourceControlEffect, ResourceControlRequest, ResourceReadModel,
    open_action_id,
};
use crate::submission::{RequestId, ResourceRoutePhase, SubmissionState};

/// Longest display name accepted for a registered resource
const DISPLAY_NAME_MAX_CHARS: usize = 120;
/// Time a control waits for its owner to apply a durable cancellation intent
const SETTLE_WAIT: Duration = Duration::from_secs(3);
const SETTLE_POLL: Duration = Duration::from_millis(100);
/// Time to read an actor snapshot before a view omits its transient attention
const INSPECT_TIMEOUT: Duration = Duration::from_secs(1);
/// Forwarded controls include the authority's settle wait and one notice attempt
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const READ_MAX_BODY: usize = 8 * 1024 * 1024;

/// Read routes shared by the Unix socket and the dashboard
pub(crate) fn read_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/resources", get(list))
        .route(RESOURCE_PENDING_PATH, get(pending))
        .route("/v1/resources/{id}", get(detail))
}

/// The three typed resource controls, shared by the socket and the dashboard origin
pub(crate) fn action_routes() -> Router<AppState> {
    Router::new().route("/v1/resources/{id}/actions", post(action))
}

/// Socket-only resource mutations for the CLI
pub(crate) fn socket_routes() -> Router<AppState> {
    Router::new()
        .route(RESOURCE_REGISTER_PATH, post(register))
        .route("/v1/resources/{id}/supervisor", post(replace_supervisor))
        .route("/v1/resources/{id}/background", post(background))
        .route(
            "/v1/resources/{id}/background/{task_id}/trainer-attempt",
            post(trainer_attempt),
        )
        .route("/v1/resources/{id}/requests", post(submit_request))
        .route(
            "/v1/resources/{id}/requests/{request_id}/cancel",
            post(cancel_request),
        )
        .route(
            "/v1/resources/{id}/operator-release",
            post(operator_release),
        )
        .route("/v1/resources/{id}/initial-idle", post(initial_idle))
        .merge(action_routes())
}

/// Fleet routes served by a resource authority
pub(super) fn cluster_routes() -> Router<AppState> {
    Router::new()
        .route(CLUSTER_RESOURCES_PATH, get(cluster_list))
        .route(CLUSTER_RESOURCE_PENDING_PATH, get(cluster_pending))
        .route("/v1/cluster/resources/{id}", get(cluster_detail))
        .route("/v1/cluster/resources/{id}/control", post(cluster_control))
}

// ---- request parsing ----

/// JSON body extractor that reports malformed or unknown input in the AppError envelope
pub(crate) struct StrictJson<T>(pub(crate) T);

impl<S, T> FromRequest<S> for StrictJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|error| AppError::Usage {
                message: format!("invalid request body: {error}"),
            })?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|error| AppError::Usage {
            message: format!("invalid JSON: {error}"),
        })?;
        serde_path_to_error::deserialize(&value)
            .map(Self)
            .map_err(|error| AppError::Usage {
                message: format!(
                    "invalid request at `{}`: {}",
                    spec::json_pointer(error.path()),
                    error.inner()
                ),
            })
    }
}

fn path_value<T>(path: Result<Path<T>, PathRejection>) -> Result<T, AppError> {
    path.map(|Path(value)| value)
        .map_err(|error| AppError::Usage {
            message: format!("invalid path: {error}"),
        })
}

fn check_version(api_version: u32) -> Result<(), AppError> {
    if api_version == API_VERSION {
        return Ok(());
    }
    Err(AppError::Usage {
        message: "unsupported API version".into(),
    })
}

/// Thread whose pending actions are listed
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingQuery {
    machine: MachineId,
    thread: ThreadId,
}

impl PendingQuery {
    fn address(&self) -> SupervisorAddress {
        SupervisorAddress {
            machine: self.machine,
            thread: self.thread,
        }
    }
}

/// Fleet read of pending actions, with the destination check fields
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterPendingQuery {
    api_version: u32,
    destination_machine: MachineId,
    machine: MachineId,
    thread: ThreadId,
}

// ---- read handlers ----

async fn list(State(state): State<AppState>) -> Result<Json<ResourceList>, AppError> {
    let mut resources = local_overviews(&state).await?;
    let mut unavailable_authorities = Vec::new();
    for (peer, result) in
        read_peers::<ClusterResourceList>(&state, |_| format!("{CLUSTER_RESOURCES_PATH}?")).await
    {
        let checked = result.and_then(|body| {
            let owned = body.machine == peer.machine
                && body
                    .resources
                    .iter()
                    .all(|overview| overview.resource.authority_machine() == peer.machine);
            if owned {
                Ok(body.resources)
            } else {
                Err("peer returned resources owned by another authority".to_owned())
            }
        });
        match checked {
            Ok(mut remote) => resources.append(&mut remote),
            Err(message) => unavailable_authorities.push(peer.unavailable(message)),
        }
    }
    resources.sort_by(|left, right| {
        left.resource
            .display_name
            .cmp(&right.resource.display_name)
            .then(left.resource.id.as_uuid().cmp(&right.resource.id.as_uuid()))
    });
    Ok(Json(ResourceList {
        api_version: API_VERSION,
        resources,
        unavailable_authorities,
    }))
}

async fn detail(
    State(state): State<AppState>,
    id: Result<Path<ResourceId>, PathRejection>,
) -> Result<Json<ResourceDetail>, AppError> {
    let id = path_value(id)?;
    if let Some(detail) = local_detail(&state, id).await? {
        return Ok(Json(detail));
    }
    remote_detail(&state, id).await.map(Json)
}

async fn pending(
    State(state): State<AppState>,
    query: Result<Query<PendingQuery>, QueryRejection>,
) -> Result<Json<PendingActionList>, AppError> {
    let Query(query) = query.map_err(|error| AppError::Usage {
        message: format!("invalid pending-action query: {error}"),
    })?;
    let address = query.address();
    let mut actions = local_pending(&state, address).await?;
    let mut unavailable_authorities = Vec::new();
    for (peer, result) in read_peers::<ClusterPendingActions>(&state, |_| {
        format!(
            "{CLUSTER_RESOURCE_PENDING_PATH}?machine={}&thread={}&",
            address.machine, address.thread
        )
    })
    .await
    {
        let checked = result.and_then(|body| {
            let owned = body.machine == peer.machine
                && body.actions.iter().all(|action| {
                    action.authority_machine == peer.machine && action.supervisor == address
                });
            if owned {
                Ok(body.actions)
            } else {
                Err("peer returned actions for another authority or thread".to_owned())
            }
        });
        match checked {
            Ok(mut remote) => actions.append(&mut remote),
            Err(message) => unavailable_authorities.push(peer.unavailable(message)),
        }
    }
    Ok(Json(PendingActionList {
        api_version: API_VERSION,
        actions,
        unavailable_authorities,
    }))
}

// ---- mutation handlers ----

async fn action(
    State(state): State<AppState>,
    id: Result<Path<ResourceId>, PathRejection>,
    StrictJson(body): StrictJson<ResourceActionBody>,
) -> Result<Json<ResourceDetail>, AppError> {
    let id = path_value(id)?;
    check_version(body.api_version)?;
    let control = ResourceControl::Action {
        expected_revision: body.expected_revision,
        operation_id: body.operation_id,
        action: body.action,
    };
    apply_control(&state, id, control).await.map(Json)
}

async fn cancel_request(
    State(state): State<AppState>,
    ids: Result<Path<(ResourceId, RequestId)>, PathRejection>,
    StrictJson(body): StrictJson<RequestCancelBody>,
) -> Result<Json<ResourceDetail>, AppError> {
    let (id, request_id) = path_value(ids)?;
    check_version(body.api_version)?;
    let control = ResourceControl::Action {
        expected_revision: body.expected_revision,
        operation_id: body.operation_id,
        action: BrowserResourceAction::CancelQueued { request_id },
    };
    apply_control(&state, id, control).await.map(Json)
}

async fn replace_supervisor(
    State(state): State<AppState>,
    id: Result<Path<ResourceId>, PathRejection>,
    StrictJson(body): StrictJson<SupervisorReplacementBody>,
) -> Result<Json<ResourceDetail>, AppError> {
    let id = path_value(id)?;
    check_version(body.api_version)?;
    check_supervisor(&body.supervisor)?;
    let control = ResourceControl::ReplaceSupervisor {
        expected_revision: body.expected_revision,
        supervisor: body.supervisor,
    };
    apply_control(&state, id, control).await.map(Json)
}

async fn register(
    State(state): State<AppState>,
    StrictJson(body): StrictJson<ResourceRegisterBody>,
) -> Result<Json<ResourceDetail>, AppError> {
    check_version(body.api_version)?;
    let registration = body.spec;
    let display_name = check_registration(&registration)?;
    let resource = Resource::new(
        registration.id,
        display_name,
        state.machine.identity.machine,
        registration.supervisor,
        AssignmentRevision::new(0),
        ResourceRevision::new(0),
        None,
    );
    // the store compares a retry with the saved first registration, so an exact
    // retry succeeds after a supervisor replacement and changed content conflicts
    call(&state.supervisor, |reply| SupervisorMsg::RegisterResource {
        resource: Box::new(resource),
        reply,
    })
    .await?;
    local_detail(&state, registration.id)
        .await?
        .ok_or_else(|| AppError::Internal {
            message: "registered resource is missing from the authority store".into(),
        })
        .map(Json)
}

fn check_registration(registration: &ResourceRegistration) -> Result<String, AppError> {
    if registration.id.as_uuid().is_nil() {
        return Err(AppError::Usage {
            message: "resource id must not be nil".into(),
        });
    }
    let name = registration.display_name.trim();
    if name.is_empty()
        || name.chars().count() > DISPLAY_NAME_MAX_CHARS
        || name.chars().any(char::is_control)
    {
        return Err(AppError::Usage {
            message: format!(
                "display_name must be 1 to {DISPLAY_NAME_MAX_CHARS} characters without control characters"
            ),
        });
    }
    check_supervisor(&registration.supervisor)?;
    Ok(name.to_owned())
}

fn check_supervisor(supervisor: &SupervisorAddress) -> Result<(), AppError> {
    if supervisor.machine.as_uuid().is_nil() || supervisor.thread.0.is_nil() {
        return Err(AppError::Usage {
            message: "supervisor machine and thread must not be nil".into(),
        });
    }
    Ok(())
}

/// Envelope shared by background and request submissions; `spec` and `env`
/// keep their own pointer prefixes when they fail validation
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceSubmitEnvelope {
    api_version: u32,
    request_id: RequestId,
    spec: Value,
    env: Value,
    callback_cwd: PathBuf,
}

struct ResourceSubmitBody {
    request_id: RequestId,
    spec: NormalizedSpec,
    env: TaskEnv,
    callback_cwd: PathBuf,
}

impl ResourceSubmitBody {
    fn parse(value: &Value) -> Result<Self, AppError> {
        let envelope: ResourceSubmitEnvelope = serde_path_to_error::deserialize(value)
            .map_err(|error| super::api::invalid_at(value, "", &error))?;
        check_version(envelope.api_version)?;
        if envelope.request_id.0.is_nil() {
            return Err(AppError::Usage {
                message: "request_id must not be nil".into(),
            });
        }
        let env = serde_path_to_error::deserialize(&envelope.env)
            .map_err(|error| super::api::invalid_at(&envelope.env, "/env", &error))?;
        let spec = spec::parse_normalized_value(&envelope.spec)
            .map_err(|error| super::api::prefix("/spec", error))?;
        if spec.machine.is_some() {
            return Err(AppError::InvalidSpec {
                pointer: "/spec/machine".into(),
                value: Value::Null,
                message: "the resource authority fixes the execution machine".into(),
            });
        }
        if !envelope.callback_cwd.is_absolute() {
            return Err(AppError::InvalidSpec {
                pointer: "/callback_cwd".into(),
                value: serde_json::to_value(&envelope.callback_cwd)?,
                message: "callback directory must be absolute".into(),
            });
        }
        Ok(Self {
            request_id: envelope.request_id,
            spec,
            env,
            callback_cwd: envelope.callback_cwd,
        })
    }
}

/// Bind one first background launch through the authority's launch owner
///
/// A co-located supervisor binds the launch directly on its own authority. A
/// supervisor on another machine saves its fixed-ID origin route here first and
/// then sends the launch to the fixed authority. A call on an authority whose
/// supervisor thread runs elsewhere writes nothing, since this machine cannot
/// own that thread's callbacks
async fn background(
    State(state): State<AppState>,
    id: Result<Path<ResourceId>, PathRejection>,
    StrictJson(value): StrictJson<Value>,
) -> Result<Json<ResourceBackgroundSubmitResponse>, AppError> {
    let resource = path_value(id)?;
    let body = ResourceSubmitBody::parse(&value)?;
    let request_id = body.request_id;
    let local = state.machine.identity.machine;
    let authority = background_authority(&state, resource, request_id).await?;
    if authority != local {
        let input = super::resource_background::RemoteBackgroundSubmit {
            resource,
            authority,
            request_id,
            spec: body.spec,
            env: body.env,
            callback_cwd: body.callback_cwd,
        };
        return super::resource_background::submit(&state, input)
            .await
            .map(Json);
    }

    let launch = BackgroundLaunch {
        resource_id: resource,
        request_id,
        spec: body.spec,
        env: body.env,
        callback_cwd: body.callback_cwd,
    };
    let unknown = |message: String| AppError::ResourceOutcomeUnknown {
        resource,
        operation: Some(request_id.0),
        message: format!("{message}; retry with the same request id {}", request_id.0),
    };
    let acceptance = call(&state.supervisor, |reply| SupervisorMsg::LaunchBackground {
        launch: Box::new(launch),
        reply,
    })
    .await
    .map_err(|error| unknown(format!("the launch owner did not answer: {error}")))?
    .map_err(|error| background_error(resource, request_id, error))?;
    let (task_id, outcome) = match acceptance {
        BackgroundLaunchAcceptance::Inserted { task, .. } => {
            (task, ResourceBackgroundSubmitOutcome::Inserted)
        }
        BackgroundLaunchAcceptance::Existing { task, state } => {
            (task, ResourceBackgroundSubmitOutcome::Existing { state })
        }
        BackgroundLaunchAcceptance::UnsupportedRemoteSupervisor { supervisor, .. } => {
            return Err(AppError::ResourceOperationUnavailable {
                resource,
                message: format!(
                    "the supervisor thread runs on machine {}; submit the background launch \
                     from that machine, which owns its callback route; nothing was written",
                    supervisor.machine
                ),
            });
        }
    };
    let saved = local_resource(&state, resource).await.map_err(|error| {
        unknown(format!(
            "the launch committed but its resource read failed: {error}"
        ))
    })?;

    Ok(Json(ResourceBackgroundSubmitResponse {
        api_version: API_VERSION,
        request_id,
        task_id,
        resource: saved,
        outcome,
    }))
}

/// Authority for a background launch; a retry reuses the saved remote route so a
/// lost response does not depend on Fleet discovery
async fn background_authority(
    state: &AppState,
    resource: ResourceId,
    request: RequestId,
) -> Result<MachineId, AppError> {
    let saved = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request,
        reply,
    })
    .await?;
    match saved.map(|route| route.submission) {
        Some(SubmissionState::ResourceBackground { binding, .. })
            if binding.assignment.resource_id == resource =>
        {
            Ok(binding.execution_machine())
        }
        // a co-located launch saves an ordinary accepted route on its authority
        Some(SubmissionState::Accepted) | None => locate_authority(state, resource).await,
        Some(_) => Err(AppError::ResourceOperationConflict {
            resource,
            operation: Some(request.0),
            message: "request id belongs to another resource or route".into(),
        }),
    }
}

fn background_error(
    resource: ResourceId,
    request_id: RequestId,
    error: BackgroundLaunchError,
) -> AppError {
    use BackgroundLaunchError as Error;
    let not_allowed = |message: String| AppError::ResourceActionNotAllowed { resource, message };
    let conflict = |message: String| AppError::ResourceOperationConflict {
        resource,
        operation: Some(request_id.0),
        message,
    };
    match error {
        Error::Resource(crate::resource::store::ResourceStoreError::ResourceNotFound) => {
            AppError::ResourceNotFound { resource }
        }
        error @ (Error::NotSupervisorThread { .. }
        | Error::ActiveLoan { .. }
        | Error::QueuedWorkAhead { .. }
        | Error::BackgroundTaskActive { .. }
        | Error::BackgroundTaskMissing { .. }
        | Error::LaunchPending { .. }
        | Error::PredecessorReleaseUnproven { .. }) => not_allowed(error.to_string()),
        error @ (Error::ConflictingRetry { .. } | Error::IdentityConflict { .. }) => {
            conflict(error.to_string())
        }
        Error::UnsupportedCommand(error) => AppError::ResourceOperationUnavailable {
            resource,
            message: format!(
                "a background launch accepts only the maintained direct-segment trainer, whose \
                 ownership lock release proof can verify; this command does not match it: {error}"
            ),
        },
        // the spec or executor environment is invalid; nothing was written
        Error::TaskRecords(
            error @ (AppError::CwdNotFound { .. }
            | AppError::ExecutableMissing { .. }
            | AppError::InvalidSpec { .. }
            | AppError::Usage { .. }),
        ) => error,
        error => AppError::ResourceOutcomeUnknown {
            resource,
            operation: Some(request_id.0),
            message: format!(
                "the launch owner failed: {error}; retry with the same request id {}",
                request_id.0
            ),
        },
    }
}

pub(super) async fn local_resource(
    state: &AppState,
    resource: ResourceId,
) -> Result<Resource, AppError> {
    let snapshots = call(&state.store, |reply| {
        StoreMsg::ResourceSnapshotsForAuthority {
            authority_machine: state.machine.identity.machine,
            reply,
        }
    })
    .await?;
    snapshots
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource)
        .map(|snapshot| snapshot.resource)
        .ok_or(AppError::ResourceNotFound { resource })
}

/// Save one authority-local operator confirmation, then wake its resource owner
async fn operator_release(
    State(state): State<AppState>,
    path: Result<Path<ResourceId>, PathRejection>,
    StrictJson(body): StrictJson<OperatorReleaseBody>,
) -> Result<Json<OperatorReleaseResponse>, AppError> {
    let resource = path_value(path)?;
    check_version(body.api_version)?;
    let operation_id = body.attestation.operation_id;
    let operation = operation_id.as_uuid();
    if body.attestation.resource_id != resource {
        return Err(AppError::Usage {
            message: "path resource id does not match the attestation".into(),
        });
    }

    let authority = state.machine.identity.machine;
    if body.attestation.authority_machine != authority {
        return Err(AppError::ResourceActionNotAllowed {
            resource,
            message: format!(
                "operator attestation names authority {}, but this daemon is {authority}",
                body.attestation.authority_machine
            ),
        });
    }
    body.attestation
        .validate()
        .map_err(|error| operator_release_refusal(resource, operation, error))?;

    let resolution = call(&state.store, |reply| {
        StoreMsg::AttestTrainerGpuFreeForAuthority {
            authority_machine: authority,
            attestation: Box::new(body.attestation),
            reply,
        }
    })
    .await
    .map_err(|error| {
        operator_release_unknown(
            resource,
            operation,
            format!("the store actor did not answer: {error}"),
        )
    })?
    .map_err(|error| match error {
        OperatorGpuFreeError::Refused(refusal) => {
            operator_release_refusal(resource, operation, refusal)
        }
        error => operator_release_unknown(
            resource,
            operation,
            format!("the authority could not confirm the transaction result: {error}"),
        ),
    })?;

    call(&state.supervisor, |reply| {
        SupervisorMsg::ReconcileResource {
            id: resource,
            reply,
        }
    })
    .await
    .map_err(|error| {
        operator_release_unknown(
            resource,
            operation,
            format!("the receipt is saved, but resource reconciliation did not complete: {error}"),
        )
    })?;

    Ok(Json(OperatorReleaseResponse {
        api_version: API_VERSION,
        receipt: resolution.receipt,
        replayed: resolution.replayed,
    }))
}

/// Save one authority-local initial idle attestation, then wake its resource owner
async fn initial_idle(
    State(state): State<AppState>,
    path: Result<Path<ResourceId>, PathRejection>,
    StrictJson(body): StrictJson<InitialIdleBody>,
) -> Result<Json<InitialIdleResponse>, AppError> {
    let resource = path_value(path)?;
    check_version(body.api_version)?;
    let operation = body.attestation.operation_id.as_uuid();
    if body.attestation.resource_id != resource {
        return Err(AppError::Usage {
            message: "path resource id does not match the attestation".into(),
        });
    }
    let authority = state.machine.identity.machine;
    if body.attestation.authority_machine != authority {
        return Err(AppError::ResourceActionNotAllowed {
            resource,
            message: format!(
                "initial idle attestation names authority {}, but this daemon is {authority}",
                body.attestation.authority_machine
            ),
        });
    }
    body.attestation
        .validate()
        .map_err(|error| initial_idle_refusal(resource, operation, error))?;

    let resolution = call(&state.store, |reply| {
        StoreMsg::AttestInitialIdleForAuthority {
            authority_machine: authority,
            attestation: Box::new(body.attestation),
            reply,
        }
    })
    .await
    .map_err(|error| {
        operator_release_unknown(
            resource,
            operation,
            format!("the store actor did not answer: {error}"),
        )
    })?
    .map_err(|error| match error {
        InitialIdleError::Refused(refusal) => initial_idle_refusal(resource, operation, refusal),
        error => operator_release_unknown(
            resource,
            operation,
            format!("the authority could not confirm the transaction result: {error}"),
        ),
    })?;

    // queued work serves from the new idle boundary through the normal reconciliation
    call(&state.supervisor, |reply| {
        SupervisorMsg::ReconcileResource {
            id: resource,
            reply,
        }
    })
    .await
    .map_err(|error| {
        operator_release_unknown(
            resource,
            operation,
            format!("the receipt is saved, but resource reconciliation did not complete: {error}"),
        )
    })?;

    Ok(Json(InitialIdleResponse {
        api_version: API_VERSION,
        receipt: resolution.receipt,
        replayed: resolution.replayed,
    }))
}

fn initial_idle_refusal(
    resource: ResourceId,
    operation: Uuid,
    refusal: InitialIdleRefusal,
) -> AppError {
    use InitialIdleRefusal as Refusal;

    match refusal {
        Refusal::InvalidIdentity | Refusal::EmptyObservation => AppError::Usage {
            message: refusal.to_string(),
        },
        Refusal::ResourceNotFound => AppError::ResourceNotFound { resource },
        Refusal::StaleRevision { expected, actual } => AppError::ResourceStaleRevision {
            resource,
            expected: expected.get(),
            current: actual.get(),
        },
        Refusal::ConflictingRetry { .. } | Refusal::RevisionExhausted { .. } => {
            AppError::ResourceOperationConflict {
                resource,
                operation: Some(operation),
                message: refusal.to_string(),
            }
        }
        Refusal::WrongAuthority { .. }
        | Refusal::HistoryExists { .. }
        | Refusal::AlreadyAttested { .. } => AppError::ResourceActionNotAllowed {
            resource,
            message: refusal.to_string(),
        },
    }
}

fn operator_release_refusal(
    resource: ResourceId,
    operation: Uuid,
    refusal: OperatorGpuFreeRefusal,
) -> AppError {
    use OperatorGpuFreeRefusal as Refusal;

    match refusal {
        Refusal::InvalidIdentity | Refusal::EmptyObservation => AppError::Usage {
            message: refusal.to_string(),
        },
        Refusal::ResourceNotFound => AppError::ResourceNotFound { resource },
        Refusal::ConflictingRetry { .. } => AppError::ResourceOperationConflict {
            resource,
            operation: Some(operation),
            message: refusal.to_string(),
        },
        Refusal::StaleRevision { expected, actual } => AppError::ResourceStaleRevision {
            resource,
            expected: expected.get(),
            current: actual.get(),
        },
        Refusal::LoanStateChanged { .. } | Refusal::RevisionExhausted { .. } => {
            AppError::ResourceOperationConflict {
                resource,
                operation: Some(operation),
                message: refusal.to_string(),
            }
        }
        refusal => AppError::ResourceActionNotAllowed {
            resource,
            message: refusal.to_string(),
        },
    }
}

fn operator_release_unknown(resource: ResourceId, operation: Uuid, detail: String) -> AppError {
    AppError::ResourceOutcomeUnknown {
        resource,
        operation: Some(operation),
        message: format!(
            "operator attestation outcome is unknown: {detail}; retry with the same operation id {operation}"
        ),
    }
}

/// Bind the supervisor-named trainer attempt to the registered background task
///
/// The trainer creates its attempt and ownership lock only after it starts, so
/// this runs after the resource owner registered the task on its confirmed start
/// The authority reads the attempt request and probes the held lock itself. An
/// exact saved association is returned without a new probe
async fn trainer_attempt(
    State(state): State<AppState>,
    path: Result<Path<(ResourceId, TaskId)>, PathRejection>,
    StrictJson(body): StrictJson<TrainerAttemptBody>,
) -> Result<Json<TrainerAttemptResponse>, AppError> {
    let (resource, task_id) = path_value(path)?;
    check_version(body.api_version)?;
    // a remote authority verifies the attempt from its own runtime evidence
    if locate_authority(&state, resource).await? != state.machine.identity.machine {
        return super::resource_background::bind_trainer_attempt(
            &state,
            resource,
            task_id,
            body.attempt_binding,
        )
        .await
        .map(Json);
    }
    authority_trainer_attempt(&state, resource, task_id, body.attempt_binding)
        .await
        .map(Json)
}

/// Bind or replay one trainer attempt association on this authority
///
/// The task must be the registered background task and must hold its lock. An
/// exact saved association is returned without a new probe; another attempt
/// for the same task conflicts
pub(super) async fn authority_trainer_attempt(
    state: &AppState,
    resource: ResourceId,
    task_id: TaskId,
    attempt_binding: AttemptBinding,
) -> Result<TrainerAttemptResponse, AppError> {
    let authority = state.machine.identity.machine;
    let not_allowed = |message: String| AppError::ResourceActionNotAllowed { resource, message };
    let saved = call(&state.store, |reply| {
        StoreMsg::TrainerAttemptAssociationForTaskForAuthority {
            authority_machine: authority,
            task_id,
            reply,
        }
    })
    .await?
    .map_err(|error| not_allowed(format!("trainer association cannot be read: {error}")))?;
    let association = match saved {
        Some(saved) if saved.resource_id() == resource => {
            if saved.verified_attempt().binding() != &attempt_binding {
                return Err(AppError::ResourceOperationConflict {
                    resource,
                    operation: None,
                    message: format!("task {task_id} is associated with another trainer attempt"),
                });
            }
            saved
        }
        Some(_) => {
            return Err(not_allowed(format!(
                "task {task_id} belongs to another resource"
            )));
        }
        None => bind_trainer_attempt(state, resource, task_id, attempt_binding).await?,
    };
    let evidence = association.verified_attempt();

    Ok(TrainerAttemptResponse {
        api_version: API_VERSION,
        resource: local_resource(state, resource).await?,
        task_id,
        runtime_root: evidence.canonical_runtime_root().to_path_buf(),
        attempt_binding: evidence.binding().clone(),
    })
}

async fn bind_trainer_attempt(
    state: &AppState,
    resource: ResourceId,
    task_id: TaskId,
    binding: AttemptBinding,
) -> Result<crate::resource::TrainerAttemptAssociation, AppError> {
    let authority = state.machine.identity.machine;
    let not_allowed = |message: String| AppError::ResourceActionNotAllowed { resource, message };
    let identity = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
        id: task_id,
        reply,
    })
    .await?;
    let Some(crate::submission::ExecutorIdentity::Accepted(record)) = identity else {
        return Err(not_allowed(format!(
            "task {task_id} has no accepted identity"
        )));
    };
    let runtime_root = match record.current_spec().map(|spec| &spec.workload) {
        Some(crate::spec::NormalizedWorkload::Task(workload)) => {
            crate::resource::command_shape::direct_segment_runtime_root(&workload.command)
        }
        _ => None,
    }
    .ok_or_else(|| {
        not_allowed(format!(
            "task {task_id} is not a direct-segment trainer command"
        ))
    })?;
    let verified = tokio::task::spawn_blocking(move || {
        crate::resource::ownership_lock::build_trainer_attempt_registration_evidence(
            &runtime_root,
            &binding,
        )
    })
    .await
    .map_err(|error| AppError::Internal {
        message: format!("trainer attempt probe failed: {error}"),
    })?
    .map_err(|error| {
        not_allowed(format!(
            "trainer attempt evidence is not available: {error}"
        ))
    })?;
    call(&state.store, |reply| {
        StoreMsg::BindTrainerAttemptAssociationForAuthority {
            authority_machine: authority,
            resource_id: resource,
            task_id,
            verified_attempt: Box::new(verified),
            reply,
        }
    })
    .await?
    .map_err(|error| not_allowed(format!("trainer attempt association refused: {error}")))
}

async fn submit_request(
    State(state): State<AppState>,
    id: Result<Path<ResourceId>, PathRejection>,
    StrictJson(value): StrictJson<Value>,
) -> Result<Json<ResourceRequestSubmitResponse>, AppError> {
    let resource = path_value(id)?;
    let body = ResourceSubmitBody::parse(&value)?;
    let authority = request_authority(&state, resource, body.request_id).await?;
    let input = ResourceSubmitInput {
        request: body.request_id,
        resource,
        authority,
        spec: body.spec,
        env: body.env,
        callback_cwd: body.callback_cwd,
    };
    let (task_id, outcome) = match super::resource_submit::submit(&state, input).await? {
        ResourceSubmitOutcome::Waiting { task } => (task, ResourceRequestSubmitOutcome::Waiting),
        ResourceSubmitOutcome::Activated { task } => {
            (task, ResourceRequestSubmitOutcome::Activated)
        }
        ResourceSubmitOutcome::Rejected { task, reason } => {
            (task, ResourceRequestSubmitOutcome::Rejected { reason })
        }
        ResourceSubmitOutcome::CancelledBeforeLaunch { task } => {
            (task, ResourceRequestSubmitOutcome::CancelledBeforeLaunch)
        }
    };
    Ok(Json(ResourceRequestSubmitResponse {
        api_version: API_VERSION,
        request_id: body.request_id,
        task_id,
        resource_id: resource,
        authority_machine: authority,
        outcome,
    }))
}

/// Authority for a request submission; a retry reuses the saved route so a lost
/// response does not depend on Fleet discovery
async fn request_authority(
    state: &AppState,
    resource: ResourceId,
    request: RequestId,
) -> Result<MachineId, AppError> {
    let saved = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request,
        reply,
    })
    .await?;
    if let Some(route) = saved {
        return match route.submission {
            SubmissionState::Resource {
                resource: saved_resource,
                ..
            } if saved_resource == resource => Ok(route.execution_machine),
            _ => Err(AppError::SubmissionConflict {
                request,
                task: route.task,
                message: "request UUID belongs to another resource or route".into(),
            }),
        };
    }
    locate_authority(state, resource).await
}

// ---- authority controls ----

/// Apply one control on the fixed authority and return its authoritative detail
async fn apply_control(
    state: &AppState,
    resource: ResourceId,
    control: ResourceControl,
) -> Result<ResourceDetail, AppError> {
    let authority = locate_authority(state, resource).await?;
    if authority == state.machine.identity.machine {
        return authority_control(state, resource, control).await;
    }
    forward_control(state, authority, resource, control).await
}

async fn authority_control(
    state: &AppState,
    resource: ResourceId,
    control: ResourceControl,
) -> Result<ResourceDetail, AppError> {
    match control {
        ResourceControl::Action {
            expected_revision,
            operation_id,
            action,
        } => {
            let request = ResourceControlRequest {
                resource_id: resource,
                expected_revision,
                action,
            };
            authority_action(state, operation_id, request).await
        }
        ResourceControl::ReplaceSupervisor {
            expected_revision,
            supervisor,
        } => {
            call(&state.store, |reply| StoreMsg::ReplaceResourceSupervisor {
                authority_machine: state.machine.identity.machine,
                resource_id: resource,
                expected_revision,
                supervisor,
                reply,
            })
            .await?;
            // the actor refreshes its snapshot and later deliveries use the new route
            if let Err(error) = call(&state.supervisor, |reply| {
                SupervisorMsg::ReconcileResource {
                    id: resource,
                    reply,
                }
            })
            .await
            {
                warn!(resource = %resource.as_uuid(), "resource reconcile after supervisor replacement: {error}");
            }
            required_local_detail(state, resource).await
        }
    }
}

async fn authority_action(
    state: &AppState,
    operation_id: Uuid,
    request: ResourceControlRequest,
) -> Result<ResourceDetail, AppError> {
    if operation_id.is_nil() {
        return Err(AppError::Usage {
            message: "operation_id must not be nil".into(),
        });
    }
    let resource = request.resource_id;
    let start = call(&state.store, |reply| StoreMsg::BeginResourceControl {
        authority_machine: state.machine.identity.machine,
        operation_id,
        request: Box::new(request),
        attempt_id: DeliveryAttemptId::new(),
        reply,
    })
    .await?;
    let unknown = |message: String| AppError::ResourceOutcomeUnknown {
        resource,
        operation: Some(operation_id),
        message,
    };
    match start.effect {
        ResourceControlEffect::CancelQueued { request } => {
            let mut settlement =
                queued_cancel_settlement(state, resource, request.request_id).await;
            if settlement == QueuedCancelSettlement::Pending {
                request_origin_cancellation(state, &request, ResourceRoutePhase::Waiting)
                    .await
                    .map_err(|error| unknown(format!("origin cancellation: {error}")))?;
                settlement = wait_for_queued_cancel(state, resource, request.request_id).await;
            }
            match settlement {
                QueuedCancelSettlement::Cancelled => {}
                QueuedCancelSettlement::NoLongerQueued => {
                    return Err(AppError::ResourceActionNotAllowed {
                        resource,
                        message: format!(
                            "request {} left the queue before cancellation settled; inspect its task before requesting a stop",
                            request.request_id.0
                        ),
                    });
                }
                QueuedCancelSettlement::Pending => {
                    return Err(unknown(
                        "queued cancellation has not settled at the resource authority".into(),
                    ));
                }
            }
        }
        ResourceControlEffect::StopActive { request } => {
            request_origin_cancellation(state, &request, ResourceRoutePhase::Activated)
                .await
                .map_err(|error| unknown(format!("origin cancellation: {error}")))?;
            if !wait_until(|| task_stop_recorded(state, request.task_id)).await {
                return Err(unknown(
                    "active task cancellation has not settled at the executor".into(),
                ));
            }
        }
        ResourceControlEffect::Renotify {
            notice_id,
            attempt_id,
        } if !start.replayed => {
            // the spawned attempt settles even when the HTTP caller disconnects
            let delivery_state = state.clone();
            let delivery = tokio::spawn(async move {
                super::resource_notice_sender::deliver_reserved_attempt(
                    &delivery_state,
                    notice_id,
                    attempt_id,
                )
                .await
            });
            match delivery.await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    return Err(unknown(format!(
                        "renotify attempt was not settled: {error}"
                    )));
                }
                Err(error) => {
                    return Err(unknown(format!("renotify attempt worker failed: {error}")));
                }
            }
        }
        ResourceControlEffect::Renotify { .. } => {}
    }
    required_local_detail(state, resource).await
}

/// Ask the request's origin to save its cancellation intent
///
/// The origin owns the callback route and delivers the cancellation to this
/// authority through the existing restart-safe delivery path
async fn request_origin_cancellation(
    state: &AppState,
    request: &ResourceRequest,
    phase: ResourceRoutePhase,
) -> Result<(), AppError> {
    if request.origin_machine == state.machine.identity.machine {
        return match super::cluster::origin_resource_cancellation_intent(state, request.task_id)
            .await?
        {
            OriginResourceCancellationOutcome::Intent { .. }
            | OriginResourceCancellationOutcome::AlreadyCancelled => Ok(()),
        };
    }
    let target = ResourceCancellationTarget {
        request_id: request.request_id,
        task_id: request.task_id,
        resource_id: request.resource_id,
        origin_machine: request.origin_machine,
        authority_machine: state.machine.identity.machine,
        phase,
    };
    super::cluster::forward_resource_cancellation_intent(state, &target)
        .await
        .map(|_| ())
}

/// Poll the owner's durable result for a bounded time
async fn wait_until<F, Fut>(mut settled: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + SETTLE_WAIT;
    while Instant::now() < deadline {
        if settled().await {
            return true;
        }
        tokio::time::sleep(SETTLE_POLL).await;
    }
    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueuedCancelSettlement {
    Pending,
    Cancelled,
    NoLongerQueued,
}

async fn wait_for_queued_cancel(
    state: &AppState,
    resource: ResourceId,
    request: RequestId,
) -> QueuedCancelSettlement {
    let deadline = Instant::now() + SETTLE_WAIT;
    while Instant::now() < deadline {
        let settlement = queued_cancel_settlement(state, resource, request).await;
        if settlement != QueuedCancelSettlement::Pending {
            return settlement;
        }
        tokio::time::sleep(SETTLE_POLL).await;
    }
    QueuedCancelSettlement::Pending
}

async fn queued_cancel_settlement(
    state: &AppState,
    resource: ResourceId,
    request: RequestId,
) -> QueuedCancelSettlement {
    let requests = call(&state.store, |reply| StoreMsg::ResourceRequests {
        authority_machine: state.machine.identity.machine,
        resource_id: resource,
        reply,
    })
    .await;
    let Ok(requests) = requests else {
        return QueuedCancelSettlement::Pending;
    };
    match requests.iter().find(|saved| saved.request_id == request) {
        Some(saved) => match saved.state {
            ResourceRequestState::Queued => QueuedCancelSettlement::Pending,
            ResourceRequestState::CancelledBeforeLaunch => QueuedCancelSettlement::Cancelled,
            ResourceRequestState::Assigned { .. }
            | ResourceRequestState::Finished { .. }
            | ResourceRequestState::Rejected { .. } => QueuedCancelSettlement::NoLongerQueued,
        },
        None => QueuedCancelSettlement::Pending,
    }
}

async fn task_stop_recorded(state: &AppState, task: TaskId) -> bool {
    let row = call(&state.store, |reply| StoreMsg::GetTask { id: task, reply }).await;
    row.ok()
        .flatten()
        .is_some_and(|row| row.cancel_requested_at.is_some() || row.status().is_terminal())
}

// ---- forwarding ----

async fn forward_control(
    state: &AppState,
    authority: MachineId,
    resource: ResourceId,
    control: ResourceControl,
) -> Result<ResourceDetail, AppError> {
    let operation = match &control {
        ResourceControl::Action { operation_id, .. } => Some(*operation_id),
        ResourceControl::ReplaceSupervisor { .. } => None,
    };
    let unavailable = |message: String| AppError::ResourceAuthorityUnavailable {
        resource,
        machine: authority,
        message,
    };
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| unavailable("fleet is disabled".into()))?;
    // nothing was sent when the authority cannot be reached
    let destination = fleet
        .connect(authority)
        .await
        .map_err(|error| unavailable(error.to_string()))?;
    let body = ClusterResourceControl {
        api_version: API_VERSION,
        protocol_version: destination.protocol.0,
        destination_machine: authority,
        source_machine: state.machine.identity.machine,
        resource_id: resource,
        control,
    };
    let path = format!("/v1/cluster/resources/{}/control", resource.as_uuid());
    let response = ClusterClient::new(CONTROL_TIMEOUT, READ_MAX_BODY)
        .post_json(&destination.address, &path, &body)
        .await
        .map_err(|error| AppError::ResourceOutcomeUnknown {
            resource,
            operation,
            message: error.to_string(),
        })?;
    decode_control_response(resource, authority, operation, &response)
}

fn decode_control_response(
    resource: ResourceId,
    authority: MachineId,
    operation: Option<Uuid>,
    response: &ClusterResponse,
) -> Result<ResourceDetail, AppError> {
    let unknown = |message: String| AppError::ResourceOutcomeUnknown {
        resource,
        operation,
        message,
    };
    if response.status == StatusCode::OK {
        let detail: ResourceDetail = serde_json::from_slice(&response.body)
            .map_err(|error| unknown(format!("invalid authority response: {error}")))?;
        if detail.api_version != API_VERSION
            || detail.resource.id != resource
            || detail.resource.authority_machine() != authority
        {
            return Err(unknown(
                "authority response names another resource or authority".into(),
            ));
        }
        return Ok(detail);
    }
    let error = remote_error(resource, operation, &response.body);
    match error {
        // a definitive authority refusal keeps its typed code
        Some(error) if response.status.is_client_error() => Err(error),
        _ => Err(unknown(format!(
            "authority returned HTTP {}",
            response.status
        ))),
    }
}

#[derive(Deserialize)]
struct RemoteErrorEnvelope {
    api_version: u32,
    error: RemoteError,
}

#[derive(Deserialize)]
struct RemoteError {
    code: String,
    message: String,
    #[serde(default)]
    input: Value,
}

/// Rebuild a typed refusal from an authority's AppError envelope
fn remote_error(resource: ResourceId, operation: Option<Uuid>, body: &[u8]) -> Option<AppError> {
    let envelope: RemoteErrorEnvelope = serde_json::from_slice(body).ok()?;
    if envelope.api_version != API_VERSION {
        return None;
    }
    let error = envelope.error;
    let detail = error
        .input
        .get("message")
        .and_then(Value::as_str)
        .map_or_else(|| error.message.clone(), str::to_owned);
    let revision = |key: &str| error.input.get(key).and_then(Value::as_u64);
    Some(match error.code.as_str() {
        "resource_stale_revision" => AppError::ResourceStaleRevision {
            resource,
            expected: revision("expected_revision")?,
            current: revision("current_revision")?,
        },
        "resource_operation_conflict" => AppError::ResourceOperationConflict {
            resource,
            operation,
            message: detail,
        },
        "resource_not_found" => AppError::ResourceNotFound { resource },
        "resource_operation_unavailable" => AppError::ResourceOperationUnavailable {
            resource,
            message: detail,
        },
        _ => AppError::ResourceActionNotAllowed {
            resource,
            message: format!("authority refused the control ({}): {detail}", error.code),
        },
    })
}

// ---- Fleet location and reads ----

#[derive(Clone)]
struct PeerRef {
    machine: MachineId,
    name: String,
}

impl PeerRef {
    fn unavailable(&self, message: String) -> UnavailableAuthority {
        UnavailableAuthority {
            machine: self.machine,
            name: Some(self.name.clone()),
            message,
        }
    }
}

async fn peers(state: &AppState) -> Vec<PeerRef> {
    let Some(fleet) = state.fleet.handle() else {
        return Vec::new();
    };
    let local = state.machine.identity.machine;
    let mut seen = BTreeSet::from([local]);
    fleet
        .peers()
        .await
        .into_iter()
        .filter(|peer| seen.insert(peer.machine))
        .map(|peer| PeerRef {
            machine: peer.machine,
            name: peer.name.to_string(),
        })
        .collect()
}

/// Read one cluster route from every known peer in parallel
///
/// `path` returns the route and any query prefix ending in `?` or `&`; the
/// destination check parameters are appended here
async fn read_peers<T>(
    state: &AppState,
    path: impl Fn(&PeerRef) -> String,
) -> Vec<(PeerRef, Result<T, String>)>
where
    T: DeserializeOwned + Send + 'static,
{
    let Some(fleet) = state.fleet.handle() else {
        return Vec::new();
    };
    let mut jobs = JoinSet::new();
    for peer in peers(state).await {
        let fleet = fleet.clone();
        let path = format!(
            "{}api_version={API_VERSION}&destination_machine={}",
            path(&peer),
            peer.machine
        );
        jobs.spawn(async move {
            let result = read_peer::<T>(&fleet, peer.machine, &path).await;
            (peer, result)
        });
    }
    let mut results = Vec::new();
    while let Some(joined) = jobs.join_next().await {
        match joined {
            Ok(result) => results.push(result),
            Err(error) => warn!("resource peer read worker failed: {error}"),
        }
    }
    results
}

async fn read_peer<T: DeserializeOwned>(
    fleet: &FleetHandle,
    machine: MachineId,
    path: &str,
) -> Result<T, String> {
    let destination = fleet
        .connect(machine)
        .await
        .map_err(|error| error.to_string())?;
    let response = ClusterClient::new(READ_TIMEOUT, READ_MAX_BODY)
        .get(&destination.address, path)
        .await
        .map_err(|error| error.to_string())?;
    if response.status != StatusCode::OK {
        return Err(format!("peer returned HTTP {}", response.status));
    }
    let value: Value = serde_json::from_slice(&response.body)
        .map_err(|error| format!("invalid peer response: {error}"))?;
    if value.get("api_version").and_then(Value::as_u64) != Some(u64::from(API_VERSION)) {
        return Err("peer response uses an unsupported API version".into());
    }
    serde_json::from_value(value).map_err(|error| format!("invalid peer response: {error}"))
}

/// Read one resource from the peer that owns it
pub(super) async fn remote_detail(
    state: &AppState,
    resource: ResourceId,
) -> Result<ResourceDetail, AppError> {
    let mut found = Vec::new();
    let mut unchecked = Vec::new();
    for (peer, result) in read_peers::<ClusterResourceDetail>(state, |_| {
        format!("/v1/cluster/resources/{}?", resource.as_uuid())
    })
    .await
    {
        match result {
            Ok(body) if body.machine != peer.machine => unchecked.push(peer.machine),
            Ok(ClusterResourceDetail {
                detail: Some(detail),
                ..
            }) if detail.resource.id == resource
                && detail.resource.authority_machine() == peer.machine =>
            {
                found.push(detail);
            }
            Ok(ClusterResourceDetail { detail: None, .. }) => {}
            Ok(_) | Err(_) => unchecked.push(peer.machine),
        }
    }
    match found.len() {
        1 => Ok(found.remove(0)),
        0 if unchecked.is_empty() => Err(AppError::ResourceNotFound { resource }),
        0 => Err(AppError::ResourceLookupIncomplete {
            resource,
            unchecked,
        }),
        _ => Err(AppError::Internal {
            message: format!(
                "more than one authority claims resource {}",
                resource.as_uuid()
            ),
        }),
    }
}

/// Fixed authority for a resource, from local ownership or the one peer that owns it
async fn locate_authority(state: &AppState, resource: ResourceId) -> Result<MachineId, AppError> {
    if !local_models(state, Some(resource)).await?.is_empty() {
        return Ok(state.machine.identity.machine);
    }
    remote_detail(state, resource)
        .await
        .map(|detail| detail.resource.authority_machine())
}

// ---- cluster handlers ----

fn check_read(state: &AppState, api_version: u32, destination: MachineId) -> Result<(), AppError> {
    state.machine.identity.check_destination(destination)?;
    check_version(api_version)
}

async fn cluster_list(
    State(state): State<AppState>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<ClusterResourceList>, AppError> {
    check_read(&state, query.api_version, query.destination_machine)?;
    Ok(Json(ClusterResourceList {
        api_version: API_VERSION,
        machine: state.machine.identity.machine,
        resources: local_overviews(&state).await?,
    }))
}

async fn cluster_detail(
    State(state): State<AppState>,
    Path(id): Path<ResourceId>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<ClusterResourceDetail>, AppError> {
    check_read(&state, query.api_version, query.destination_machine)?;
    Ok(Json(ClusterResourceDetail {
        api_version: API_VERSION,
        machine: state.machine.identity.machine,
        detail: local_detail(&state, id).await?,
    }))
}

async fn cluster_pending(
    State(state): State<AppState>,
    Query(query): Query<ClusterPendingQuery>,
) -> Result<Json<ClusterPendingActions>, AppError> {
    check_read(&state, query.api_version, query.destination_machine)?;
    let address = SupervisorAddress {
        machine: query.machine,
        thread: query.thread,
    };
    Ok(Json(ClusterPendingActions {
        api_version: API_VERSION,
        machine: state.machine.identity.machine,
        actions: local_pending(&state, address).await?,
    }))
}

async fn cluster_control(
    State(state): State<AppState>,
    Path(id): Path<ResourceId>,
    Json(body): Json<ClusterResourceControl>,
) -> Result<Json<ResourceDetail>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    super::cluster::check_api_version(body.api_version)?;
    super::cluster::check_protocol(body.protocol_version, body.source_machine)?;
    if body.resource_id != id {
        return Err(AppError::Usage {
            message: "resource path and body identities differ".into(),
        });
    }
    if let ResourceControl::ReplaceSupervisor { supervisor, .. } = &body.control {
        check_supervisor(supervisor)?;
    }
    authority_control(&state, id, body.control).await.map(Json)
}

// ---- local authority views ----

async fn local_models(
    state: &AppState,
    resource: Option<ResourceId>,
) -> Result<Vec<ResourceReadModel>, AppError> {
    call(&state.store, |reply| StoreMsg::ResourceReadModels {
        authority_machine: state.machine.identity.machine,
        resource_id: resource,
        reply,
    })
    .await
}

async fn required_local_detail(
    state: &AppState,
    resource: ResourceId,
) -> Result<ResourceDetail, AppError> {
    local_detail(state, resource)
        .await?
        .ok_or(AppError::ResourceNotFound { resource })
}

async fn local_overviews(state: &AppState) -> Result<Vec<ResourceOverview>, AppError> {
    let models = local_models(state, None).await?;
    let current: Vec<TaskId> = models.iter().filter_map(current_task_id).collect();
    let tasks = task_summaries(state, current).await?;
    let mut overviews = Vec::with_capacity(models.len());
    for model in models {
        let inspection = inspect(state, model.resource.id).await;
        overviews.push(ResourceOverview {
            queued_count: model
                .requests
                .iter()
                .filter(|request| request.state == ResourceRequestState::Queued)
                .count() as u64,
            current_task: current_task_id(&model).and_then(|id| tasks.get(&id).cloned()),
            attention: attention(&model, inspection.as_ref()),
            background_launch: background_launch_reservation(&model),
            resource: model.resource,
            loan: model.loan,
        });
    }
    Ok(overviews)
}

async fn local_detail(
    state: &AppState,
    resource: ResourceId,
) -> Result<Option<ResourceDetail>, AppError> {
    let Some(model) = local_models(state, Some(resource)).await?.pop() else {
        return Ok(None);
    };
    let current_task_id = current_task_id(&model);
    let background_task_id = model.resource.registered_background_task;
    let tasks = task_summaries(
        state,
        current_task_id
            .into_iter()
            .chain(background_task_id)
            .collect(),
    )
    .await?;
    let inspection = inspect(state, resource).await;
    let attention = attention(&model, inspection.as_ref());
    let background_launch = background_launch_reservation(&model);
    Ok(Some(ResourceDetail {
        background_launch,
        return_execution_mode: model.return_execution_mode,
        api_version: API_VERSION,
        requests: model.requests.iter().map(request_view).collect(),
        current_task: current_task_id.and_then(|id| tasks.get(&id).cloned()),
        background_task: background_task_id.and_then(|id| tasks.get(&id).cloned()),
        current_task_id,
        background_task_id,
        attention,
        resource: model.resource,
        loan: model.loan,
        notices: model.notices,
    }))
}

async fn local_pending(
    state: &AppState,
    supervisor: SupervisorAddress,
) -> Result<Vec<PendingActionView>, AppError> {
    let models = local_models(state, None).await?;
    Ok(models
        .iter()
        .filter(|model| model.resource.supervisor == supervisor)
        .filter_map(pending_action)
        .collect())
}

/// Open action of one resource, derived from its non-closed loan and notices
fn pending_action(model: &ResourceReadModel) -> Option<PendingActionView> {
    let loan = model.loan.as_ref()?;
    let action_id = open_action_id(loan)?;
    let (phase, return_context) = match &loan.state {
        LoanState::Active { phase } => pending_phase(phase)?,
        LoanState::NeedsAttention {
            last_safe_phase,
            reason,
            ..
        } => (
            PendingActionPhase::AttentionRequired {
                reason: reason.clone(),
                last_safe_phase: Box::new(last_safe_phase.clone()),
            },
            phase_return_context(last_safe_phase),
        ),
        LoanState::Closed { .. } => return None,
    };
    Some(PendingActionView {
        resource_id: model.resource.id,
        authority_machine: model.resource.authority_machine(),
        loan_id: loan.id,
        action_id,
        state_revision: model.resource.state_revision,
        supervisor: model.resource.supervisor,
        assignment_revision: model.resource.assignment_revision,
        phase,
        return_context,
        notice: model
            .notices
            .iter()
            .find(|notice| notice.action_id == action_id)
            .cloned(),
    })
}

fn pending_phase(phase: &LoanPhase) -> Option<(PendingActionPhase, Option<ReturnContext>)> {
    match phase {
        LoanPhase::AwaitingRelease {
            observed_background_task,
            ..
        } => Some((
            PendingActionPhase::ReleaseRequired {
                observed_background_task: *observed_background_task,
            },
            None,
        )),
        LoanPhase::AwaitingReturn { return_context, .. } => Some((
            PendingActionPhase::ReturnRequired,
            Some(return_context.clone()),
        )),
        LoanPhase::Restoring {
            return_context,
            resume_task_id,
            ..
        } => Some((
            PendingActionPhase::Restoring {
                resume_task_id: *resume_task_id,
            },
            Some(return_context.clone()),
        )),
        LoanPhase::Serving { .. } => None,
    }
}

fn phase_return_context(phase: &LoanPhase) -> Option<ReturnContext> {
    match phase {
        LoanPhase::Serving { return_context, .. }
        | LoanPhase::AwaitingReturn { return_context, .. }
        | LoanPhase::Restoring { return_context, .. } => Some(return_context.clone()),
        LoanPhase::AwaitingRelease { .. } => None,
    }
}

/// First background launch that still reserves the resource, from durable launch state
fn background_launch_reservation(model: &ResourceReadModel) -> Option<BackgroundLaunchReservation> {
    use BackgroundLaunchReservationStatus as Status;

    let launch = model.background_launch.as_ref()?;
    let status = match launch.phase {
        BackgroundLaunchPhase::Queued => Status::Queued,
        BackgroundLaunchPhase::StartedUnregistered => Status::StartedUnregistered,
        BackgroundLaunchPhase::IdentityMismatch => Status::IdentityMismatch,
        _ if launch.awaits_operator_release() => Status::ReleaseUnproven,
        BackgroundLaunchPhase::Superseded
        | BackgroundLaunchPhase::Registered
        | BackgroundLaunchPhase::EndedBeforeRegistration { .. } => return None,
    };
    Some(BackgroundLaunchReservation {
        request_id: launch.request_id,
        task_id: launch.task_id,
        status,
    })
}

/// Task that holds the resource in the current loan phase
fn current_task_id(model: &ResourceReadModel) -> Option<TaskId> {
    let phase = match &model.loan.as_ref()?.state {
        LoanState::Active { phase } => phase,
        LoanState::NeedsAttention {
            last_safe_phase, ..
        } => last_safe_phase,
        LoanState::Closed { .. } => return None,
    };
    match phase {
        LoanPhase::Serving {
            current_request_id, ..
        } => model
            .requests
            .iter()
            .find(|request| request.request_id == *current_request_id)
            .map(|request| request.task_id),
        LoanPhase::Restoring { resume_task_id, .. } => Some(*resume_task_id),
        LoanPhase::AwaitingRelease { .. } | LoanPhase::AwaitingReturn { .. } => None,
    }
}

fn request_view(request: &ResourceRequest) -> ResourceRequestView {
    ResourceRequestView {
        request_id: request.request_id,
        task_id: request.task_id,
        acceptance_sequence: request.acceptance_sequence,
        origin_machine: request.origin_machine,
        display_name: request.spec().as_normalized().name.as_str().to_owned(),
        state: request.state.clone(),
    }
}

async fn task_summaries(
    state: &AppState,
    ids: Vec<TaskId>,
) -> Result<HashMap<TaskId, ResourceTaskSummary>, AppError> {
    let mut rows = Vec::with_capacity(ids.len());
    for id in &ids {
        if let Some(row) = call(&state.store, |reply| StoreMsg::GetTask { id: *id, reply }).await? {
            rows.push(row);
        }
    }
    if rows.is_empty() {
        return Ok(HashMap::new());
    }
    let presentations = call(&state.store, |reply| StoreMsg::TaskPresentations {
        ids: rows.iter().map(|row| row.id).collect(),
        reply,
    })
    .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let summary = TaskSummary::from_row(row, presentations.get(&row.id));
            (row.id, task_summary(summary))
        })
        .collect())
}

fn task_summary(summary: TaskSummary) -> ResourceTaskSummary {
    ResourceTaskSummary {
        id: summary.id,
        display_name: summary.display_name,
        status: summary.status,
        origin_machine: summary.origin_machine,
        execution_machine: summary.execution_machine,
        pid: summary.pid,
        callback: summary.callback,
        exit_reason: summary.exit_reason,
        cancel_requested_at: summary.cancel_requested_at,
        created_at: summary.created_at,
        updated_at: summary.updated_at,
    }
}

async fn inspect(state: &AppState, resource: ResourceId) -> Option<ResourceActorInspection> {
    let inspection = tokio::time::timeout(
        INSPECT_TIMEOUT,
        call(&state.supervisor, |reply| SupervisorMsg::InspectResource {
            id: resource,
            reply,
        }),
    )
    .await;
    match inspection {
        Ok(Ok(inspection)) => inspection,
        Ok(Err(error)) => {
            warn!(resource = %resource.as_uuid(), "resource actor inspection failed: {error}");
            None
        }
        Err(_) => {
            warn!(resource = %resource.as_uuid(), "resource actor inspection timed out");
            None
        }
    }
}

// ---- attention ----

/// Most important attention condition, from durable state first and then the
/// actor's latest typed reconciliation for the same loan
fn attention(
    model: &ResourceReadModel,
    inspection: Option<&ResourceActorInspection>,
) -> Option<AttentionView> {
    if let Some(Loan {
        state: LoanState::NeedsAttention {
            action_id, reason, ..
        },
        ..
    }) = &model.loan
    {
        return Some(AttentionView {
            code: AttentionCode::LoanNeedsAttention,
            message: reason.clone(),
            action_id: Some(*action_id),
            task_id: None,
            notice_id: None,
        });
    }
    // durable launch state needs no actor snapshot and no queued request to show
    launch_attention(model)
        .or_else(|| inspection.and_then(|inspection| actor_attention(model, inspection)))
        .or_else(|| notice_attention(model))
}

fn launch_attention(model: &ResourceReadModel) -> Option<AttentionView> {
    let reservation = background_launch_reservation(model)?;
    let (code, message) = match reservation.status {
        BackgroundLaunchReservationStatus::ReleaseUnproven => (
            AttentionCode::BackgroundLaunchReleaseUnproven,
            "the first background launch ended before registration, and its GPU release is not \
             proven; the resource stays reserved until an operator inspects the authority GPU \
             and attests with the first_background_launch binding"
                .to_owned(),
        ),
        BackgroundLaunchReservationStatus::IdentityMismatch => (
            AttentionCode::QueueBlocked,
            "the first background launch records do not match its receipt, so the resource \
             stays reserved"
                .to_owned(),
        ),
        BackgroundLaunchReservationStatus::Queued
        | BackgroundLaunchReservationStatus::StartedUnregistered => return None,
    };
    Some(AttentionView {
        code,
        message,
        action_id: None,
        task_id: Some(reservation.task_id),
        notice_id: None,
    })
}

fn actor_attention(
    model: &ResourceReadModel,
    inspection: &ResourceActorInspection,
) -> Option<AttentionView> {
    let loan_id = model.loan.as_ref().map(|loan| loan.id);
    let current = |loan: &Loan| Some(loan.id) == loan_id;
    let reconcile = match inspection.reconcile_outcome.as_ref() {
        Some(ResourceQueueReconcileOutcome::AttentionRequired { request, reason })
            if model.requests.iter().any(|saved| {
                saved.request_id == request.request_id
                    && matches!(
                        saved.state,
                        ResourceRequestState::Queued | ResourceRequestState::Assigned { .. }
                    )
            }) =>
        {
            let (message, task_id) = queue_attention(reason);
            Some(AttentionView {
                code: AttentionCode::QueueBlocked,
                message,
                action_id: None,
                task_id: task_id.or(Some(request.task_id)),
                notice_id: None,
            })
        }
        Some(ResourceQueueReconcileOutcome::BackgroundLaunchUncertain { task_id })
            if model.loan.is_none() =>
        {
            Some(AttentionView {
                code: AttentionCode::QueueBlocked,
                message: "the first background launch row is queued without a proven worker \
                          start; cancel the task to prove that no child started"
                    .into(),
                action_id: None,
                task_id: Some(*task_id),
                notice_id: None,
            })
        }
        Some(ResourceQueueReconcileOutcome::RestoreAttentionRequired {
            loan,
            action_id,
            task_id,
            reason,
        }) if current(loan) => Some(AttentionView {
            code: AttentionCode::RestoreBlocked,
            message: restore_attention(*reason),
            action_id: Some(*action_id),
            task_id: Some(*task_id),
            notice_id: None,
        }),
        Some(ResourceQueueReconcileOutcome::ReleaseProofUnavailable {
            loan,
            action_id,
            task_id,
            reason,
        }) if current(loan) => Some(AttentionView {
            code: AttentionCode::ReleaseProofUnavailable,
            message: format!("release proof is not available: {reason:?}"),
            action_id: Some(*action_id),
            task_id: Some(*task_id),
            notice_id: None,
        }),
        _ => None,
    };
    reconcile.or_else(|| watcher_attention(model, inspection.release_watcher.as_ref()?))
}

fn watcher_attention(
    model: &ResourceReadModel,
    status: &ReleaseWatcherStatus,
) -> Option<AttentionView> {
    let ReleaseWatcherStatus::Attention { action_id, reason } = status else {
        return None;
    };
    if model.loan.as_ref().and_then(open_action_id) != Some(*action_id) {
        return None;
    }
    let task_id = match reason {
        ReleaseWatcherAttentionReason::TrainerAssociationMissing { task_id } => Some(*task_id),
        ReleaseWatcherAttentionReason::LaunchRejected { watcher_task_id }
        | ReleaseWatcherAttentionReason::LaunchUncertain { watcher_task_id }
        | ReleaseWatcherAttentionReason::WatcherTaskEnded {
            watcher_task_id, ..
        } => Some(*watcher_task_id),
        ReleaseWatcherAttentionReason::RemoteSupervisorUnsupported { .. }
        | ReleaseWatcherAttentionReason::ExecutableUnavailable
        | ReleaseWatcherAttentionReason::BindingRejected
        | ReleaseWatcherAttentionReason::BaselineUnavailable => None,
    };
    Some(AttentionView {
        code: AttentionCode::ReleaseWatcherBlocked,
        message: format!("release watcher cannot proceed: {reason:?}"),
        action_id: Some(*action_id),
        task_id,
        notice_id: None,
    })
}

fn notice_attention(model: &ResourceReadModel) -> Option<AttentionView> {
    let notice = model
        .notices
        .iter()
        .find(|notice| matches!(notice.delivery, SupervisorNoticeDelivery::Failed { .. }))?;
    let SupervisorNoticeDelivery::Failed {
        attempts,
        last_error,
    } = &notice.delivery
    else {
        return None;
    };
    Some(AttentionView {
        code: AttentionCode::NoticeDeliveryFailed,
        message: format!("supervisor notice failed after {attempts} attempts: {last_error}"),
        action_id: Some(notice.action_id),
        task_id: None,
        notice_id: Some(notice.id),
    })
}

fn queue_attention(reason: &ResourceQueueAttentionReason) -> (String, Option<TaskId>) {
    use ResourceQueueAttentionReason as Reason;
    match reason {
        Reason::IdleNotProven { gap } => idle_gap_attention(*gap),
        Reason::BackgroundLaunchPending { task_id } => (
            "the first background launch has not reached a confirmed start".into(),
            Some(*task_id),
        ),
        Reason::BackgroundTaskMissing { task_id } => (
            "the registered background task has no task record on the authority".into(),
            Some(*task_id),
        ),
        Reason::BackgroundTaskNotRunning { task_id, state } => (
            format!("the registered background task is not verifiably running ({state})"),
            Some(*task_id),
        ),
        Reason::AcceptedTaskLaunchUncertain { task_id } => (
            "an accepted command task has no proven worker start".into(),
            Some(*task_id),
        ),
        Reason::UnverifiedServingRelease => (
            "the serving loan has no verified trainer release proof".into(),
            None,
        ),
        Reason::ReleaseProofUnavailable {
            task_id, reason, ..
        } => (
            format!("the trainer release proof is not available: {reason:?}"),
            Some(*task_id),
        ),
        Reason::AssignedTaskLaunchUncertain { task_id } => (
            "the assigned command launch result is uncertain".into(),
            Some(*task_id),
        ),
        Reason::AssignedTaskLost { task_id } => (
            "the assigned command task is lost and its work may still run".into(),
            Some(*task_id),
        ),
        Reason::AssignedTaskExitUnconfirmed { task_id } => (
            "the assigned task ended without a confirmed exit: a command needs its process-group \
             exit, and a container needs its removal"
                .into(),
            Some(*task_id),
        ),
        Reason::AssignedTaskIdentityMismatch { task_id } => (
            "the assigned command identity does not match its request or route".into(),
            Some(*task_id),
        ),
        Reason::AssignedTaskNoChildSpawnProofInvalid { task_id } => (
            "the assigned command cannot prove that no child process started".into(),
            Some(*task_id),
        ),
        Reason::AssignedTaskOwnershipUncertain { task_id, risk } => (
            format!("the assigned command may outlive its process group: {risk:?}"),
            Some(*task_id),
        ),
        Reason::AssignedTaskStaleRevision { task_id } => (
            "the resource revision changed before the task completion committed".into(),
            Some(*task_id),
        ),
        Reason::AssignedTaskReconcileFailed { task_id } => (
            "the authority could not evaluate the assigned command".into(),
            Some(*task_id),
        ),
    }
}

fn idle_gap_attention(gap: IdleProofGap) -> (String, Option<TaskId>) {
    match gap {
        IdleProofGap::NoIdleEvidence => (
            "no registered background task, and no saved no-resume decision or background \
             launch result proves the GPU is idle; for a resource with no history, an operator \
             who inspected the authority GPU can run resource initial-idle"
                .into(),
            None,
        ),
        IdleProofGap::BackgroundLaunchReleaseUnproven { task_id } => (
            "the first background launch ended without proof that its process released the GPU"
                .into(),
            Some(task_id),
        ),
        IdleProofGap::InconsistentHistory => (
            "the saved loan and background launch history does not match the resource \
             registration"
                .into(),
            None,
        ),
    }
}

fn restore_attention(reason: RestoreAttentionReason) -> String {
    match reason {
        RestoreAttentionReason::LaunchUncertain => {
            "the return task launch is not proven to have started".into()
        }
        RestoreAttentionReason::EndedBeforeConfirmedStart { state } => {
            format!("the return task ended ({state}) before a confirmed start")
        }
        RestoreAttentionReason::ForegroundEnded { state } => {
            format!("the native foreground return task ended ({state}) without success")
        }
        RestoreAttentionReason::ForegroundExitUnconfirmed { state } => format!(
            "the native foreground return task ended ({state}), but its process-group exit is \
             not confirmed; the resource stays reserved until an operator inspects the authority \
             GPU and attests with the restoring_foreground_return binding"
        ),
        RestoreAttentionReason::ContainerEnded { state } => {
            format!("the container return task ended ({state}) without success")
        }
        RestoreAttentionReason::ContainerExitUnconfirmed { state } => format!(
            "the container return task ended ({state}), but Homebased did not confirm that its \
             container exited and was removed; the resource stays reserved until an operator \
             inspects the authority GPU and attests with the restoring_foreground_return binding"
        ),
        RestoreAttentionReason::Lost => "the return task is lost; the resource stays reserved \
             until an operator inspects the authority GPU and attests with the Restoring binding \
             of its execution mode"
            .into(),
        RestoreAttentionReason::IdentityMismatch => {
            "the return task identity does not match the bound action".into()
        }
        RestoreAttentionReason::ReconcileFailed => {
            "the authority could not evaluate the return task".into()
        }
    }
}

#[cfg(test)]
mod tests;
