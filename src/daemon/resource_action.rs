//! Two-machine resource actions: supervisor-owned routes and authority acceptance
//!
//! The supervisor machine asks the authority to prepare the canonical task, saves
//! a fixed-ID origin route with that exact spec, and only then sends the launch.
//! The authority reads back the saved route as evidence before it accepts. A lost
//! reply or restart repeats the same launch; nothing on this path abandons the
//! identity, because abandoning could strand the resource action
//!
//! When the supervisor thread runs on the authority itself, the socket route
//! hands the choice to the supervisor actor's one-shot return and resolution
//! owners instead, and their store receipts answer retries

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
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
use super::actors::supervisor::{ReturnDecisionOutcome, return_decision_rejection};
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::cluster::ReadQuery;
use crate::domain::{API_VERSION, AgentKind, TaskEnv, TaskId};
use crate::error::AppError;
use crate::fleet::http::{ClusterClient, ClusterResponse};
use crate::fleet::protocol::{ClusterProtocolVersion, ProtocolRange, SUPPORTED_PROTOCOLS};
use crate::invocation::resolve_agent_binary;
use crate::machine::MachineId;
use crate::resource::bound_action::{
    ActionTaskIdentity, LocalReturnAcceptance, LocalReturnReceipt, PreparedActionTask,
    RESOURCE_ACTION_PATH, RESOURCE_ACTION_PROTOCOL_VERSION, RESOURCE_ACTION_ROUTE_PROOF_PATH,
    RESOURCE_ACTION_SUBMIT_PATH, ResourceActionChoice, ResourceActionLaunch,
    ResourceActionOperation, ResourceActionOutcome, ResourceActionRejection, ResourceActionRequest,
    ResourceActionResponse, ResourceActionSubmitOutcome, ResourceActionSubmitRequest,
    ResourceActionSubmitResponse,
};
use crate::resource::{CommandSpec, ReturnDecision, ReturnLaunch, SupervisorActionAuthority};
use crate::store::{
    EndedRestoreResolution, ResourceActionRouteResult, ReturnClosure, ReturnDecisionError,
    ReturnTaskAcceptance,
};
use crate::submission::{
    CallbackContext, CallbackExecutable, NewResourceActionRoute, OriginRoute, RequestId,
    ResourceActionRouteBinding, ResourceActionRoutePhase, ResourceActionRouteProof,
    SubmissionState, normalized_spec_sha256,
};

const ACTION_SHARDS: usize = 64;
static ACTION_PERMITS: OnceLock<[Semaphore; ACTION_SHARDS]> = OnceLock::new();

/// Saved route proof returned by the supervisor machine
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceActionRouteProofBody {
    /// Public API version
    pub api_version: u32,
    /// Proof for the named task, when this machine saved a valid action route
    pub proof: Option<ResourceActionRouteProof>,
}

/// Cluster routes: authority acceptance and supervisor route evidence
pub(super) fn cluster_routes() -> Router<AppState> {
    Router::new()
        .route(RESOURCE_ACTION_PATH, post(receive))
        .route(
            &format!("{RESOURCE_ACTION_ROUTE_PROOF_PATH}/{{task}}"),
            get(route_proof),
        )
}

/// Socket-only route that starts one action from the supervisor's own machine
pub(crate) fn socket_routes() -> Router<AppState> {
    Router::new().route(RESOURCE_ACTION_SUBMIT_PATH, post(submit_from_socket))
}

// ---- authority side ----

async fn receive(
    State(state): State<AppState>,
    Json(request): Json<ResourceActionRequest>,
) -> Result<Json<ResourceActionResponse>, AppError> {
    state
        .machine
        .identity
        .check_destination(request.destination_machine)?;
    check_protocol(request.protocol_version, request.source_machine)?;
    request.validate().map_err(|error| AppError::Usage {
        message: format!("invalid resource action: {error}"),
    })?;

    if let Some(identity) = request.operation.launch_identity() {
        let proof = fetch_route_proof(&state, request.source_machine, identity.task_id).await?;
        if let Err(reason) = check_route_evidence(&request, identity, proof) {
            warn!(
                action = %request.authority.action_id.as_uuid(),
                task = %identity.task_id,
                ?reason,
                "resource action route evidence refused"
            );
            return Ok(Json(ResourceActionResponse::new(
                &request,
                ResourceActionOutcome::Rejected { reason },
            )));
        }
    }

    let outcome = call(&state.supervisor, |reply| SupervisorMsg::ResourceAction {
        request: Box::new(request.clone()),
        reply,
    })
    .await?;
    Ok(Json(ResourceActionResponse::new(&request, outcome)))
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

/// Compare the supervisor machine's saved route with one launch request
fn check_route_evidence(
    request: &ResourceActionRequest,
    identity: ActionTaskIdentity,
    proof: Option<ResourceActionRouteProof>,
) -> Result<(), ResourceActionRejection> {
    let proof = proof.ok_or(ResourceActionRejection::RouteEvidenceMissing)?;
    let kind = request.operation.task_kind();
    if proof.request != identity.request_id
        || proof.task != identity.task_id
        || Some(proof.binding.kind) != kind
        || proof.binding.authority != request.authority
        || proof.normalized_spec_sha256 != identity.normalized_spec_sha256
        || matches!(proof.phase, ResourceActionRoutePhase::Rejected { .. })
    {
        return Err(ResourceActionRejection::RouteEvidenceMismatch);
    }
    Ok(())
}

/// Read the action route that the supervisor machine saved for one task
async fn fetch_route_proof(
    state: &AppState,
    origin: MachineId,
    task: TaskId,
) -> Result<Option<ResourceActionRouteProof>, AppError> {
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
        "{RESOURCE_ACTION_ROUTE_PROOF_PATH}/{task}?api_version={API_VERSION}&destination_machine={origin}"
    );
    let response = ClusterClient::default()
        .get(&destination.address, &path)
        .await
        .map_err(|error| unavailable(error.to_string()))?;
    if response.status != StatusCode::OK {
        return Err(unavailable(format!("HTTP {}", response.status)));
    }
    let body: ResourceActionRouteProofBody = serde_json::from_slice(&response.body)
        .map_err(|error| unavailable(format!("invalid response: {error}")))?;
    if body.api_version != API_VERSION {
        return Err(unavailable("unsupported API version".into()));
    }
    Ok(body.proof.filter(|proof| proof.task == task))
}

/// Serve the saved action route proof for one task owned by this supervisor machine
async fn route_proof(
    State(state): State<AppState>,
    Path(task): Path<TaskId>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<ResourceActionRouteProofBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(query.destination_machine)?;
    if query.api_version != API_VERSION {
        return Err(AppError::Usage {
            message: "unsupported API version".into(),
        });
    }
    let route = call(&state.store, |reply| StoreMsg::OriginRoute {
        id: task,
        reply,
    })
    .await?;
    let local = state.machine.identity.machine;
    let proof = route
        .as_ref()
        .filter(|route| route.task == task && route.origin_machine == local)
        .and_then(ResourceActionRouteProof::from_route);
    Ok(Json(ResourceActionRouteProofBody {
        api_version: API_VERSION,
        proof,
    }))
}

// ---- supervisor side ----

async fn lock_action(
    action: crate::resource::ActionId,
) -> Result<SemaphorePermit<'static>, AppError> {
    let permits = ACTION_PERMITS.get_or_init(|| std::array::from_fn(|_| Semaphore::new(1)));
    let mut hasher = DefaultHasher::new();
    action.as_uuid().hash(&mut hasher);
    let shard = (hasher.finish() as usize) % ACTION_SHARDS;
    permits[shard]
        .acquire()
        .await
        .map_err(|error| AppError::Internal {
            message: format!("resource action permit unavailable: {error}"),
        })
}

async fn submit_from_socket(
    State(state): State<AppState>,
    Json(request): Json<ResourceActionSubmitRequest>,
) -> Result<Json<ResourceActionSubmitResponse>, AppError> {
    if request.api_version != API_VERSION {
        return Err(AppError::Usage {
            message: "unsupported API version".into(),
        });
    }
    let outcome = submit(&state, request.authority, request.choice).await?;
    Ok(Json(ResourceActionSubmitResponse {
        api_version: API_VERSION,
        outcome,
    }))
}

/// Apply one supervisor choice for a pending action
///
/// On a remote authority, a launch saves the fixed-ID route before it is sent,
/// and an uncertain send leaves the route for the same retry. A saved route
/// answers repeated calls. When this machine is also the authority, the
/// supervisor actor applies the choice and its store receipt answers retries
pub(crate) async fn submit(
    state: &AppState,
    authority: SupervisorActionAuthority,
    choice: ResourceActionChoice,
) -> Result<ResourceActionSubmitOutcome, AppError> {
    let local = state.machine.identity.machine;
    if authority.supervisor.machine != local {
        return Err(AppError::Usage {
            message: "the resource supervisor thread is not on this machine".into(),
        });
    }
    if authority.authority_machine == local {
        return submit_co_located(state, authority, choice).await;
    }
    let _action_guard = lock_action(authority.action_id).await?;
    match choice {
        ResourceActionChoice::ReleaseWatcher {
            observed_background_task,
        } => {
            submit_launch(
                state,
                authority,
                None,
                ResourceActionLaunch::ReleaseWatcher {
                    observed_background_task,
                },
            )
            .await
        }
        ResourceActionChoice::Return {
            decision: ReturnDecision::Launch(launch),
        } => {
            let ReturnLaunch {
                request_id,
                task_id,
                work,
            } = *launch;
            submit_launch(
                state,
                authority,
                Some((request_id, task_id)),
                ResourceActionLaunch::Return { work },
            )
            .await
        }
        ResourceActionChoice::Return {
            decision: ReturnDecision::NoResume { reason },
        } => {
            submit_closure(
                state,
                authority,
                ResourceActionOperation::NoResume { reason },
            )
            .await
        }
        ResourceActionChoice::ResolveEndedRestore { task_id, reason } => {
            submit_closure(
                state,
                authority,
                ResourceActionOperation::ResolveEndedRestore { task_id, reason },
            )
            .await
        }
    }
}

/// Apply one choice when the supervisor thread runs on the resource authority
///
/// The supervisor actor owns the one-shot decision: the store checks the exact
/// authority, action, revision, and assignment, saves the receipt with the task
/// records, and only an insertion by this call spawns. An actor or storage
/// failure stays an error, so an unknown outcome is never reported as a refusal
async fn submit_co_located(
    state: &AppState,
    authority: SupervisorActionAuthority,
    choice: ResourceActionChoice,
) -> Result<ResourceActionSubmitOutcome, AppError> {
    match choice {
        // the resource actor starts the bound watcher itself for a local supervisor
        ResourceActionChoice::ReleaseWatcher { .. } => Err(AppError::ResourceActionNotAllowed {
            resource: authority.resource_id,
            message: "the resource authority starts the release watcher itself when the supervisor runs on the same machine; wait for the release to finish".into(),
        }),
        ResourceActionChoice::Return { decision } => {
            let launch_ids = match &decision {
                ReturnDecision::Launch(launch) => Some((launch.request_id, launch.task_id)),
                ReturnDecision::NoResume { .. } => None,
            };
            let decided = call(&state.supervisor, |reply| SupervisorMsg::DecideReturn {
                authority,
                decision: Box::new(decision),
                reply,
            })
            .await?;
            let outcome = match decided {
                Ok(outcome) => outcome,
                Err(error) => return rejected(error),
            };
            co_located_return_outcome(authority, launch_ids, outcome)
        }
        ResourceActionChoice::ResolveEndedRestore { task_id, reason } => {
            let resolved = call(&state.supervisor, |reply| SupervisorMsg::ResolveEndedRestore {
                resolution: Box::new(EndedRestoreResolution {
                    authority,
                    task_id,
                    reason,
                }),
                reply,
            })
            .await?;
            match resolved {
                Ok(closure) => Ok(ResourceActionSubmitOutcome::Closed {
                    loan: closure.loan,
                    state_revision: closure.state_revision,
                }),
                Err(error) => rejected(error),
            }
        }
    }
}

/// Convert the supervisor actor's return result, checking that it names the decision
fn co_located_return_outcome(
    authority: SupervisorActionAuthority,
    launch_ids: Option<(RequestId, TaskId)>,
    outcome: ReturnDecisionOutcome,
) -> Result<ResourceActionSubmitOutcome, AppError> {
    let mismatch = |message: &str| AppError::Internal {
        message: format!("co-located return decision: {message}"),
    };
    let accepted = |request_id, task_id, acceptance| {
        Ok(ResourceActionSubmitOutcome::LocalReturnAccepted {
            receipt: LocalReturnReceipt {
                authority,
                request_id,
                task_id,
            },
            acceptance,
        })
    };
    match (launch_ids, outcome) {
        (None, ReturnDecisionOutcome::Closed(closure)) => {
            let ReturnClosure {
                loan,
                state_revision,
            } = *closure;
            Ok(ResourceActionSubmitOutcome::Closed {
                loan,
                state_revision,
            })
        }
        (
            Some((request_id, task_id)),
            ReturnDecisionOutcome::Launch(ReturnTaskAcceptance::Inserted {
                loan,
                task,
                state_revision,
            }),
        ) if task == task_id => accepted(
            request_id,
            task_id,
            LocalReturnAcceptance::Inserted {
                loan,
                state_revision,
            },
        ),
        (
            Some((request_id, task_id)),
            ReturnDecisionOutcome::Launch(ReturnTaskAcceptance::Existing { task, state }),
        ) if task == task_id => accepted(
            request_id,
            task_id,
            LocalReturnAcceptance::Existing { state },
        ),
        // the store answers this only when the assignment moved off this machine
        (
            Some(_),
            ReturnDecisionOutcome::Launch(ReturnTaskAcceptance::UnsupportedRemoteSupervisor {
                ..
            }),
        ) => Ok(ResourceActionSubmitOutcome::Rejected {
            reason: ResourceActionRejection::NotCurrentSupervisor,
        }),
        (Some(_), ReturnDecisionOutcome::Launch(_)) => {
            Err(mismatch("the bound task differs from the decision"))
        }
        (None, ReturnDecisionOutcome::Launch(_)) | (Some(_), ReturnDecisionOutcome::Closed(_)) => {
            Err(mismatch("the result does not fit the decision"))
        }
    }
}

fn rejected(error: ReturnDecisionError) -> Result<ResourceActionSubmitOutcome, AppError> {
    return_decision_rejection(error).map(|reason| ResourceActionSubmitOutcome::Rejected { reason })
}

/// Retry action routes whose authority acceptance was unknown at daemon startup
pub(super) async fn recover(state: AppState) {
    let routes = match call(&state.store, |reply| {
        StoreMsg::UnknownResourceActionRoutes { reply }
    })
    .await
    {
        Ok(routes) => routes,
        Err(error) => {
            warn!("cannot scan unresolved resource action routes: {error}");
            return;
        }
    };
    for route in routes {
        let SubmissionState::ResourceAction { binding, .. } = &route.submission else {
            continue;
        };
        let Ok(_action_guard) = lock_action(binding.authority.action_id).await else {
            continue;
        };
        if let Err(error) = send_saved_launch(&state, &route).await {
            warn!(task = %route.task, "resource action recovery: {error}");
        }
    }
}

/// Prepare, save, and send one launch, or resend the launch of a saved route
///
/// A return decision carries supervisor-chosen request and task identities; a
/// watcher uses the identities that the authority bound to its release action
async fn submit_launch(
    state: &AppState,
    authority: SupervisorActionAuthority,
    identities: Option<(RequestId, TaskId)>,
    launch: ResourceActionLaunch,
) -> Result<ResourceActionSubmitOutcome, AppError> {
    let binding = ResourceActionRouteBinding {
        kind: launch.kind(),
        authority,
    };
    let saved = call(&state.store, |reply| {
        StoreMsg::ResourceActionRouteByAction {
            action: authority.action_id,
            reply,
        }
    })
    .await?;
    let route = match saved {
        Some(route) => {
            check_saved_choice(&route, &binding, identities, &launch)?;
            route
        }
        None => match create_route(state, binding, identities, launch).await? {
            Ok(route) => route,
            // the authority refused before any route was saved, so nothing waits for a retry
            Err(reason) => return Ok(ResourceActionSubmitOutcome::Rejected { reason }),
        },
    };
    match &route.submission {
        SubmissionState::ResourceAction {
            phase: ResourceActionRoutePhase::AcceptanceUnknown,
            ..
        } => send_saved_launch(state, &route).await,
        _ => saved_outcome(&route),
    }
}

/// A retry must repeat the saved action, identities, and supervisor choice
fn check_saved_choice(
    route: &OriginRoute,
    binding: &ResourceActionRouteBinding,
    identities: Option<(RequestId, TaskId)>,
    launch: &ResourceActionLaunch,
) -> Result<(), AppError> {
    let SubmissionState::ResourceAction {
        binding: saved_binding,
        launch: saved_launch,
        ..
    } = &route.submission
    else {
        return Err(conflict(route, "saved route is not an action route"));
    };
    if saved_binding != binding || **saved_launch != *launch {
        return Err(conflict(route, "action was retried with another choice"));
    }
    if identities.is_some_and(|identities| identities != (route.request, route.task)) {
        return Err(conflict(route, "action already binds other identities"));
    }
    Ok(())
}

/// Ask the authority to prepare the canonical task, then save the fixed-ID route
///
/// A definitive refusal to prepare saves no route and is returned as its reason
async fn create_route(
    state: &AppState,
    binding: ResourceActionRouteBinding,
    identities: Option<(RequestId, TaskId)>,
    launch: ResourceActionLaunch,
) -> Result<Result<OriginRoute, ResourceActionRejection>, AppError> {
    let authority = binding.authority;
    let operation = match (&launch, identities) {
        (
            ResourceActionLaunch::ReleaseWatcher {
                observed_background_task,
            },
            None,
        ) => ResourceActionOperation::PrepareReleaseWatcher {
            observed_background_task: *observed_background_task,
        },
        (ResourceActionLaunch::Return { work }, Some((request_id, task_id))) => {
            ResourceActionOperation::PrepareReturn {
                launch: ReturnLaunch {
                    request_id,
                    task_id,
                    work: work.clone(),
                },
            }
        }
        (ResourceActionLaunch::ReleaseWatcher { .. }, Some(_))
        | (ResourceActionLaunch::Return { .. }, None) => {
            return Err(AppError::Usage {
                message: "resource action identities do not fit the chosen launch".into(),
            });
        }
    };
    let prepared = match send(state, authority, operation).await? {
        ResourceActionOutcome::Prepared { task } => task,
        ResourceActionOutcome::Rejected { reason } => return Ok(Err(reason)),
        ResourceActionOutcome::Accepted { .. } | ResourceActionOutcome::Closed { .. } => {
            return Err(AppError::RemoteSubmissionUnavailable {
                message: "resource authority answered prepare with another outcome".into(),
            });
        }
    };
    check_prepared(&prepared, &binding, identities, &launch)?;

    let env = TaskEnv::capture();
    let cwd = state.home.root().to_path_buf();
    let codex = match resolve_agent_binary(AgentKind::Codex, &env.path, &cwd) {
        Ok(path) => CallbackExecutable::available(path),
        Err(error) => CallbackExecutable::Unavailable {
            reason: error.to_string(),
        },
    };
    let route = OriginRoute::new_resource_action(NewResourceActionRoute {
        request: prepared.request_id,
        task: prepared.task_id,
        callback: CallbackContext { env, cwd, codex },
        spec: prepared.spec,
        binding,
        launch,
    })
    .map_err(|error| AppError::RemoteSubmissionUnavailable {
        message: format!("prepared resource action task is invalid: {error}"),
    })?;
    call(&state.store, |reply| StoreMsg::InsertOriginRoute {
        route: Box::new(route),
        reply,
    })
    .await
    .map(Ok)
}

/// Check the authority's prepared task before it becomes the saved route content
fn check_prepared(
    prepared: &PreparedActionTask,
    binding: &ResourceActionRouteBinding,
    identities: Option<(RequestId, TaskId)>,
    launch: &ResourceActionLaunch,
) -> Result<(), AppError> {
    let invalid = |message: &str| AppError::RemoteSubmissionUnavailable {
        message: format!("resource authority prepared an invalid task: {message}"),
    };
    if !prepared.digest_matches() {
        return Err(invalid("digest does not cover the spec"));
    }
    if prepared.spec.thread != binding.authority.supervisor.thread
        || prepared.spec.machine.is_some()
    {
        return Err(invalid("spec does not name the supervisor thread"));
    }
    if identities.is_some_and(|ids| ids != (prepared.request_id, prepared.task_id)) {
        return Err(invalid("identities differ from the decision"));
    }
    let chosen = match launch {
        ResourceActionLaunch::Return { work } => {
            work.supervisor_spec().map(CommandSpec::as_normalized)
        }
        ResourceActionLaunch::ReleaseWatcher { .. } => None,
    };
    if let Some(chosen) = chosen
        && normalized_spec_sha256(chosen)? != prepared.normalized_spec_sha256
    {
        return Err(invalid("spec differs from the supervisor's command"));
    }
    Ok(())
}

/// Send the launch for one saved route and apply a definitive answer to it
async fn send_saved_launch(
    state: &AppState,
    route: &OriginRoute,
) -> Result<ResourceActionSubmitOutcome, AppError> {
    let SubmissionState::ResourceAction {
        binding, launch, ..
    } = &route.submission
    else {
        return Err(conflict(route, "saved route is not an action route"));
    };
    let spec = route
        .current_spec()
        .ok_or_else(|| conflict(route, "action route has no spec"))?;
    let task = ActionTaskIdentity {
        request_id: route.request,
        task_id: route.task,
        normalized_spec_sha256: normalized_spec_sha256(spec)?,
    };
    let operation = match launch.as_ref() {
        ResourceActionLaunch::ReleaseWatcher {
            observed_background_task,
        } => ResourceActionOperation::LaunchReleaseWatcher {
            observed_background_task: *observed_background_task,
            task,
        },
        ResourceActionLaunch::Return { work } => ResourceActionOperation::LaunchReturn {
            launch: ReturnLaunch {
                request_id: route.request,
                task_id: route.task,
                work: work.clone(),
            },
            normalized_spec_sha256: task.normalized_spec_sha256,
        },
    };
    let outcome = send(state, binding.authority, operation)
        .await
        .map_err(|error| unknown(route, error.to_string()))?;
    let result = match outcome {
        ResourceActionOutcome::Accepted { receipt, .. } => {
            ResourceActionRouteResult::Accepted(receipt)
        }
        ResourceActionOutcome::Rejected { reason } => ResourceActionRouteResult::Rejected(reason),
        ResourceActionOutcome::Prepared { .. } | ResourceActionOutcome::Closed { .. } => {
            return Err(unknown(
                route,
                "resource authority answered a launch with another outcome",
            ));
        }
    };
    let saved = call(&state.store, |reply| StoreMsg::ResolveResourceActionRoute {
        task: route.task,
        result: Box::new(result),
        reply,
    })
    .await
    .map_err(|error| unknown(route, format!("cannot save authority result: {error}")))?;
    saved_outcome(&saved)
}

async fn submit_closure(
    state: &AppState,
    authority: SupervisorActionAuthority,
    operation: ResourceActionOperation,
) -> Result<ResourceActionSubmitOutcome, AppError> {
    match send(state, authority, operation).await? {
        ResourceActionOutcome::Closed {
            loan,
            state_revision,
        } => Ok(ResourceActionSubmitOutcome::Closed {
            loan,
            state_revision,
        }),
        ResourceActionOutcome::Rejected { reason } => {
            Ok(ResourceActionSubmitOutcome::Rejected { reason })
        }
        ResourceActionOutcome::Prepared { .. } | ResourceActionOutcome::Accepted { .. } => {
            Err(AppError::RemoteSubmissionUnavailable {
                message: "resource authority answered a closure with another outcome".into(),
            })
        }
    }
}

fn saved_outcome(route: &OriginRoute) -> Result<ResourceActionSubmitOutcome, AppError> {
    let SubmissionState::ResourceAction { binding, phase, .. } = &route.submission else {
        return Err(conflict(route, "saved route is not an action route"));
    };
    match phase {
        ResourceActionRoutePhase::AcceptanceUnknown => {
            Err(unknown(route, "authority acceptance is unresolved"))
        }
        ResourceActionRoutePhase::Accepted => {
            let spec = route
                .current_spec()
                .ok_or_else(|| conflict(route, "action route has no spec"))?;
            Ok(ResourceActionSubmitOutcome::Accepted {
                receipt: crate::resource::bound_action::ActionTaskReceipt {
                    kind: binding.kind,
                    authority: binding.authority,
                    request_id: route.request,
                    task_id: route.task,
                    normalized_spec_sha256: normalized_spec_sha256(spec)?,
                },
                last_execution_state: route.last_execution_state,
            })
        }
        ResourceActionRoutePhase::Rejected { reason } => {
            Ok(ResourceActionSubmitOutcome::Rejected {
                reason: reason.clone(),
            })
        }
    }
}

/// Send one operation to the authority and decode its strict typed answer
async fn send(
    state: &AppState,
    authority: SupervisorActionAuthority,
    operation: ResourceActionOperation,
) -> Result<ResourceActionOutcome, AppError> {
    let unavailable = |message: String| AppError::MachineUnavailable {
        machine: authority.authority_machine,
        message,
    };
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| unavailable("fleet is disabled".into()))?;
    let destination = fleet
        .connect(authority.authority_machine)
        .await
        .map_err(|error| unavailable(error.to_string()))?;
    let request = ResourceActionRequest::new(destination.protocol.0, authority, operation);
    let response = ClusterClient::default()
        .post_json(&destination.address, RESOURCE_ACTION_PATH, &request)
        .await
        .map_err(|error| unavailable(error.to_string()))?;
    decode_response(&request, response)
}

fn decode_response(
    request: &ResourceActionRequest,
    response: ClusterResponse,
) -> Result<ResourceActionOutcome, AppError> {
    if !response.status.is_success() {
        return Err(crate::client::map_error(response.status, &response.body));
    }
    let invalid = |message: String| AppError::RemoteSubmissionUnavailable {
        message: format!("invalid resource authority response: {message}"),
    };
    let value: Value =
        serde_json::from_slice(&response.body).map_err(|error| invalid(error.to_string()))?;
    let body: ResourceActionResponse =
        serde_json::from_value(value).map_err(|error| invalid(error.to_string()))?;
    if body.api_version != API_VERSION
        || body.protocol_version != request.protocol_version
        || body.action_protocol_version != RESOURCE_ACTION_PROTOCOL_VERSION
        || body.destination_machine != request.destination_machine
        || body.action_id != request.authority.action_id
    {
        return Err(invalid("response names another route or version".into()));
    }
    Ok(body.outcome)
}

fn unknown(route: &OriginRoute, message: impl Into<String>) -> AppError {
    AppError::SubmissionOutcomeUnknown {
        request: route.request,
        task: route.task,
        message: message.into(),
    }
}

fn conflict(route: &OriginRoute, message: impl Into<String>) -> AppError {
    AppError::SubmissionConflict {
        request: route.request,
        task: route.task,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::domain::{ProcessStatus, ThreadId};
    use crate::resource::{
        ActionId, AssignmentRevision, LoanId, ResourceId, ResourceRevision, SupervisorAddress,
    };

    fn co_located() -> SupervisorActionAuthority {
        let machine = MachineId::new();
        SupervisorActionAuthority {
            authority_machine: machine,
            resource_id: ResourceId::new(),
            loan_id: LoanId::new(),
            action_id: ActionId::new(),
            expected_state_revision: ResourceRevision::new(2),
            supervisor: SupervisorAddress {
                machine,
                thread: ThreadId(Uuid::now_v7()),
            },
            assignment_revision: AssignmentRevision::new(1),
        }
    }

    #[test]
    fn storage_failure_stays_unknown_and_refusals_stay_definitive() {
        let unknown = rejected(ReturnDecisionError::Storage(
            rusqlite::Error::QueryReturnedNoRows,
        ));
        assert!(unknown.is_err());

        let refused = rejected(ReturnDecisionError::ConflictingRetry {
            action_id: ActionId::new(),
        })
        .unwrap();
        assert!(matches!(
            refused,
            ResourceActionSubmitOutcome::Rejected {
                reason: ResourceActionRejection::ConflictingRetry
            }
        ));
    }

    #[test]
    fn co_located_return_result_must_name_the_decided_task() {
        let authority = co_located();
        let (request_id, task_id) = (RequestId::new(), TaskId::new());
        let existing = |task| {
            ReturnDecisionOutcome::Launch(ReturnTaskAcceptance::Existing {
                task,
                state: ProcessStatus::Queued,
            })
        };

        let outcome =
            co_located_return_outcome(authority, Some((request_id, task_id)), existing(task_id))
                .unwrap();
        assert!(matches!(
            outcome,
            ResourceActionSubmitOutcome::LocalReturnAccepted {
                receipt,
                acceptance: LocalReturnAcceptance::Existing {
                    state: ProcessStatus::Queued
                },
            } if receipt == LocalReturnReceipt { authority, request_id, task_id }
        ));

        // another bound task is an internal inconsistency, never an acceptance
        assert!(
            co_located_return_outcome(
                authority,
                Some((request_id, task_id)),
                existing(TaskId::new())
            )
            .is_err()
        );
        assert!(co_located_return_outcome(authority, None, existing(task_id)).is_err());
    }
}
