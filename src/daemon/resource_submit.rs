//! Origin-owned submission of command requests to resource authorities

use std::path::PathBuf;

use serde_json::Value;
use tracing::warn;

use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use crate::domain::{API_VERSION, AgentKind, TaskEnv, TaskId};
use crate::error::AppError;
use crate::fleet::http::{ClusterClient, ClusterResponse};
use crate::invocation::resolve_agent_binary;
use crate::machine::MachineId;
use crate::resource::{
    CommandSpec, ResourceId, ResourceQueueRequest, ResourceQueueResponse, ResourceRequest,
    ResourceRequestState,
};
use crate::spec::{self, NormalizedSpec};
use crate::submission::{
    CallbackContext, NewResourceRoute, OriginRoute, RequestId, ResourceQueueOutcome,
    ResourceQueueReceipt, ResourceRoutePhase, SubmissionState,
};

/// Input that identifies one resource-backed command submission
#[derive(Debug, Clone)]
pub(super) struct ResourceSubmitInput {
    /// Caller retry identity
    pub(super) request: RequestId,
    /// Fixed resource queue to use
    pub(super) resource: ResourceId,
    /// Machine that owns the resource queue
    pub(super) authority: MachineId,
    /// Normalized command specification
    pub(super) spec: NormalizedSpec,
    /// Environment saved for the origin-side callback
    pub(super) env: TaskEnv,
    /// Absolute directory used to resolve the origin-side Codex executable
    pub(super) callback_cwd: PathBuf,
}

/// Saved result of a resource-backed command submission
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ResourceSubmitOutcome {
    /// The authority accepted the command into its serving queue
    Waiting { task: TaskId },
    /// The authority activated the command on a resource
    Activated { task: TaskId },
    /// The authority rejected the command before activation
    Rejected { task: TaskId, reason: String },
    /// The authority cancelled the command before activation
    CancelledBeforeLaunch { task: TaskId },
}

/// Submit a resource-backed command or return its durable saved result
pub(super) async fn submit(
    state: &AppState,
    input: ResourceSubmitInput,
) -> Result<ResourceSubmitOutcome, AppError> {
    let _request_guard = state.locks.resource_submissions.lock(input.request).await;
    let saved = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request: input.request,
        reply,
    })
    .await?;

    let route = match saved {
        Some(route) => {
            validate_retry(&route, &input)?;
            route
        }
        None => create_route(state, &input).await?,
    };
    submit_route(state, &route).await
}

async fn retry_saved_route(
    state: &AppState,
    expected: &OriginRoute,
) -> Result<ResourceSubmitOutcome, AppError> {
    let _request_guard = state
        .locks
        .resource_submissions
        .lock(expected.request)
        .await;
    let saved = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request: expected.request,
        reply,
    })
    .await?;
    let Some(saved) = saved else {
        return Err(unknown(
            expected,
            "saved resource route disappeared during startup recovery",
        ));
    };
    ensure_same_recovery_identity(expected, &saved)?;
    submit_route(state, &saved).await
}

async fn submit_route(
    state: &AppState,
    route: &OriginRoute,
) -> Result<ResourceSubmitOutcome, AppError> {
    if !route_acceptance_is_unknown(route) {
        return outcome_from_route(route);
    }

    let receipt = if route.execution_machine == state.machine.identity.machine {
        submit_local(state, route).await?
    } else {
        submit_remote(state, route).await?
    };
    let saved = call(&state.store, |reply| StoreMsg::ResolveResourceRoute {
        receipt,
        reply,
    })
    .await
    .map_err(|error| {
        unknown(
            route,
            format!("cannot save resource authority result: {error}"),
        )
    })?;

    outcome_from_route(&saved)
}

fn ensure_same_recovery_identity(
    expected: &OriginRoute,
    saved: &OriginRoute,
) -> Result<(), AppError> {
    let Some(expected_resource) = resource_from_route(expected) else {
        return Err(conflict(
            expected,
            "startup recovery scan returned a non-resource route",
        ));
    };
    let Some(saved_resource) = resource_from_route(saved) else {
        return Err(conflict(
            expected,
            "saved route is no longer a resource route",
        ));
    };
    let same_spec = expected.current_spec() == saved.current_spec();
    if expected.request != saved.request
        || expected.task != saved.task
        || expected.origin_machine != saved.origin_machine
        || expected.execution_machine != saved.execution_machine
        || expected.thread != saved.thread
        || expected.callback != saved.callback
        || expected_resource != saved_resource
        || !same_spec
    {
        return Err(conflict(
            expected,
            "saved resource route identity changed during startup recovery",
        ));
    }
    Ok(())
}

/// Retry origin routes whose authority acceptance was unknown at daemon startup
pub(super) async fn recover(state: AppState) {
    let routes = match call(&state.store, |reply| {
        StoreMsg::UnknownResourceOriginRoutes { reply }
    })
    .await
    {
        Ok(routes) => routes,
        Err(error) => {
            warn!("cannot scan unresolved resource submissions: {error}");
            return;
        }
    };
    for route in routes {
        if let Err(error) = retry_saved_route(&state, &route).await {
            warn!("resource submission recovery: {error}");
        }
    }
}

async fn create_route(
    state: &AppState,
    input: &ResourceSubmitInput,
) -> Result<OriginRoute, AppError> {
    CommandSpec::try_from(input.spec.clone()).map_err(|error| AppError::Usage {
        message: error.to_string(),
    })?;
    if !input.callback_cwd.is_absolute() {
        return Err(AppError::InvalidSpec {
            pointer: "/callback_cwd".into(),
            value: serde_json::to_value(&input.callback_cwd)?,
            message: "callback directory must be absolute".into(),
        });
    }
    spec::check_cwd(&input.callback_cwd)?;
    let codex = resolve_agent_binary(AgentKind::Codex, &input.env.path, &input.callback_cwd)?;
    let task = TaskId::new();
    let route = OriginRoute::new_resource_waiting(NewResourceRoute {
        request: input.request,
        task,
        origin_machine: state.machine.identity.machine,
        authority_machine: input.authority,
        thread: input.spec.thread,
        callback: CallbackContext {
            env: input.env.clone(),
            cwd: input.callback_cwd.clone(),
            codex: codex.into(),
        },
        spec: input.spec.clone(),
        resource: input.resource,
    })
    .map_err(|error| AppError::Usage {
        message: error.to_string(),
    })?;

    match call(&state.store, |reply| StoreMsg::InsertOriginRoute {
        route: Box::new(route),
        reply,
    })
    .await
    {
        Ok(saved) => Ok(saved),
        Err(error) => {
            let saved = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
                request: input.request,
                reply,
            })
            .await?;
            let Some(saved) = saved else {
                return Err(error);
            };
            validate_retry(&saved, input)?;
            Ok(saved)
        }
    }
}

fn validate_retry(route: &OriginRoute, input: &ResourceSubmitInput) -> Result<(), AppError> {
    let SubmissionState::Resource { resource, .. } = &route.submission else {
        return Err(conflict(
            route,
            "request UUID belongs to a non-resource route",
        ));
    };
    let Some(saved_spec) = route.current_spec() else {
        return Err(conflict(
            route,
            "saved resource route has no normalized specification",
        ));
    };
    if route.execution_machine != input.authority
        || *resource != input.resource
        || *saved_spec != input.spec
    {
        return Err(conflict(
            route,
            "request UUID has different resource authority, resource, or normalized content",
        ));
    }
    Ok(())
}

async fn submit_local(
    state: &AppState,
    route: &OriginRoute,
) -> Result<ResourceQueueReceipt, AppError> {
    let resource = resource_id(route)?;
    let spec = route.current_spec().ok_or_else(|| {
        conflict(
            route,
            "saved resource route has no normalized specification",
        )
    })?;
    let result = call(&state.store, |reply| StoreMsg::AcceptResourceRequest {
        authority_machine: route.execution_machine,
        request_id: route.request,
        task_id: route.task,
        resource_id: resource,
        origin_machine: route.origin_machine,
        normalized_spec: Box::new(spec.clone()),
        reply,
    })
    .await;
    match result {
        Ok(stored) => {
            reconcile_local_request_if_waiting(state, route, &stored).await?;
            local_receipt(route, &stored)
        }
        Err(AppError::SubmissionRejected {
            request,
            task,
            reason,
        }) if request == route.request && task == route.task => {
            Ok(receipt(route, ResourceQueueOutcome::Rejected { reason })?)
        }
        Err(error) => Err(unknown(
            route,
            format!("local resource authority did not return a definitive queue result: {error}"),
        )),
    }
}

async fn reconcile_local_request_if_waiting(
    state: &AppState,
    route: &OriginRoute,
    request: &ResourceRequest,
) -> Result<(), AppError> {
    if !matches!(
        &request.state,
        ResourceRequestState::Queued | ResourceRequestState::Assigned { .. }
    ) {
        return Ok(());
    }

    call(&state.supervisor, |reply| {
        SupervisorMsg::ReconcileResource {
            id: request.resource_id,
            reply,
        }
    })
    .await
    .map_err(|error| {
        unknown(
            route,
            format!(
                "local resource queue accepted the request but could not reconcile it: {error}"
            ),
        )
    })
}

fn local_receipt(
    route: &OriginRoute,
    stored: &ResourceRequest,
) -> Result<ResourceQueueReceipt, AppError> {
    let resource = resource_id(route)?;
    let spec = route.current_spec().ok_or_else(|| {
        conflict(
            route,
            "saved resource route has no normalized specification",
        )
    })?;
    if stored.request_id != route.request
        || stored.task_id != route.task
        || stored.resource_id != resource
        || stored.origin_machine != route.origin_machine
        || stored.spec().as_normalized() != spec
    {
        return Err(unknown(
            route,
            "local resource authority returned a different request identity",
        ));
    }
    let outcome = match &stored.state {
        ResourceRequestState::Queued
        | ResourceRequestState::Assigned { .. }
        | ResourceRequestState::Finished { .. } => ResourceQueueOutcome::Waiting,
        ResourceRequestState::CancelledBeforeLaunch => ResourceQueueOutcome::Rejected {
            reason: "cancelled_before_launch".into(),
        },
        ResourceRequestState::Rejected { reason } => ResourceQueueOutcome::Rejected {
            reason: reason.clone(),
        },
    };
    receipt(route, outcome)
}

async fn submit_remote(
    state: &AppState,
    route: &OriginRoute,
) -> Result<ResourceQueueReceipt, AppError> {
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| unknown(route, "fleet is disabled"))?;
    let destination = fleet
        .connect(route.execution_machine)
        .await
        .map_err(|error| unknown(route, error.to_string()))?;
    let spec = route.current_spec().ok_or_else(|| {
        conflict(
            route,
            "saved resource route has no normalized specification",
        )
    })?;
    let command = CommandSpec::try_from(spec.clone())
        .map_err(|error| unknown(route, format!("saved resource command is invalid: {error}")))?;
    let request = ResourceQueueRequest::new(
        destination.protocol.0,
        route.execution_machine,
        route.origin_machine,
        route.request,
        route.task,
        resource_id(route)?,
        command,
    );
    let response = ClusterClient::default()
        .post_json(
            &destination.address,
            "/v1/cluster/resource-requests",
            &request,
        )
        .await
        .map_err(|error| unknown(route, error.to_string()))?;

    decode_resource_response(route, response, destination.protocol.0)
}

fn decode_resource_response(
    route: &OriginRoute,
    response: ClusterResponse,
    expected_protocol: u32,
) -> Result<ResourceQueueReceipt, AppError> {
    if !response.status.is_success() {
        return Err(unknown(
            route,
            format!("resource authority returned HTTP {}", response.status),
        ));
    }
    let value: Value = serde_json::from_slice(&response.body).map_err(|error| {
        unknown(
            route,
            format!("invalid resource authority response: {error}"),
        )
    })?;
    if value.get("api_version").and_then(Value::as_u64) != Some(u64::from(API_VERSION)) {
        return Err(unknown(
            route,
            "resource authority returned an unsupported API version",
        ));
    }
    let response: ResourceQueueResponse = serde_json::from_value(value).map_err(|error| {
        unknown(
            route,
            format!("invalid resource authority response: {error}"),
        )
    })?;
    if response.api_version != API_VERSION {
        return Err(unknown(
            route,
            "resource authority returned an unsupported API version",
        ));
    }
    if response.protocol_version != expected_protocol {
        return Err(unknown(
            route,
            "resource authority returned another protocol version",
        ));
    }
    if response.destination_machine != route.execution_machine {
        return Err(unknown(
            route,
            "resource authority response names another destination",
        ));
    }
    let receipt = response.receipt;
    if receipt.request != route.request
        || receipt.task != route.task
        || receipt.origin_machine != route.origin_machine
        || receipt.authority_machine != route.execution_machine
        || Some(receipt.resource) != resource_from_route(route)
    {
        return Err(unknown(
            route,
            "resource authority returned a receipt for another request identity",
        ));
    }
    Ok(receipt)
}

fn receipt(
    route: &OriginRoute,
    outcome: ResourceQueueOutcome,
) -> Result<ResourceQueueReceipt, AppError> {
    Ok(ResourceQueueReceipt {
        request: route.request,
        task: route.task,
        origin_machine: route.origin_machine,
        authority_machine: route.execution_machine,
        resource: resource_id(route)?,
        outcome,
    })
}

fn resource_id(route: &OriginRoute) -> Result<ResourceId, AppError> {
    resource_from_route(route)
        .ok_or_else(|| conflict(route, "request UUID belongs to a non-resource route"))
}

fn resource_from_route(route: &OriginRoute) -> Option<ResourceId> {
    match &route.submission {
        SubmissionState::Resource { resource, .. } => Some(*resource),
        SubmissionState::AcceptanceUnknown
        | SubmissionState::Accepted
        | SubmissionState::Rejected { .. }
        | SubmissionState::ResourceAction { .. }
        | SubmissionState::ResourceBackground { .. } => None,
    }
}

fn route_acceptance_is_unknown(route: &OriginRoute) -> bool {
    matches!(
        &route.submission,
        SubmissionState::Resource {
            phase: ResourceRoutePhase::AcceptanceUnknown,
            ..
        }
    )
}

fn outcome_from_route(route: &OriginRoute) -> Result<ResourceSubmitOutcome, AppError> {
    let SubmissionState::Resource { phase, .. } = &route.submission else {
        return Err(conflict(
            route,
            "request UUID belongs to a non-resource route",
        ));
    };
    match phase {
        ResourceRoutePhase::AcceptanceUnknown => Err(unknown(
            route,
            "resource queue acceptance is unresolved; retry the same request UUID",
        )),
        ResourceRoutePhase::Waiting => Ok(ResourceSubmitOutcome::Waiting { task: route.task }),
        ResourceRoutePhase::Activated => Ok(ResourceSubmitOutcome::Activated { task: route.task }),
        ResourceRoutePhase::Rejected { reason } => Ok(ResourceSubmitOutcome::Rejected {
            task: route.task,
            reason: reason.clone(),
        }),
        ResourceRoutePhase::CancelledBeforeLaunch => {
            Ok(ResourceSubmitOutcome::CancelledBeforeLaunch { task: route.task })
        }
    }
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
    use std::path::Path;

    use axum::http::StatusCode;
    use bytes::Bytes;
    use ractor::Actor;
    use tempfile::tempdir;

    use super::{
        ResourceSubmitInput, ResourceSubmitOutcome, decode_resource_response,
        ensure_same_recovery_identity, outcome_from_route, resource_from_route,
        route_acceptance_is_unknown, submit_local, validate_retry,
    };
    use crate::daemon::AppState;
    use crate::daemon::actors::{StoreMsg, SupervisorActor, SupervisorArgs, SupervisorMsg, call};
    use crate::domain::{TaskEnv, TaskId};
    use crate::error::AppError;
    use crate::files::StreamSlots;
    use crate::fleet::FleetState;
    use crate::fleet::directory::LocalMachine;
    use crate::fleet::http::ClusterResponse;
    use crate::fleet::protocol::SUPPORTED_PROTOCOLS;
    use crate::home::Home;
    use crate::machine::{LocalIdentity, MachineId, MachineName};
    use crate::resource::{
        AssignmentRevision, Resource, ResourceId, ResourceQueueAttentionReason,
        ResourceQueueReconcileOutcome, ResourceQueueResponse, ResourceRevision, SupervisorAddress,
    };
    use crate::spec::NormalizedSpec;
    use crate::submission::{
        CallbackContext, CallbackExecutable, NewResourceRoute, OriginRoute, RequestId,
        ResourceQueueOutcome, ResourceQueueReceipt, ResourceRoutePhase, SubmissionState,
    };
    use serde_json::Value;

    fn spec() -> NormalizedSpec {
        serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "resource task",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["echo", "hello"] }
        }))
        .unwrap()
    }

    fn route() -> OriginRoute {
        let spec = spec();
        OriginRoute::new_resource_waiting(NewResourceRoute {
            request: RequestId::new(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            authority_machine: MachineId::new(),
            thread: spec.thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: Path::new("/tmp").to_path_buf(),
                codex: CallbackExecutable::available(Path::new("/bin/echo").to_path_buf()),
            },
            spec,
            resource: ResourceId::new(),
        })
        .unwrap()
    }

    fn local_route(authority: MachineId, resource: ResourceId) -> OriginRoute {
        let spec = spec();
        OriginRoute::new_resource_waiting(NewResourceRoute {
            request: RequestId::new(),
            task: TaskId::new(),
            origin_machine: authority,
            authority_machine: authority,
            thread: spec.thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: Path::new("/tmp").to_path_buf(),
                codex: CallbackExecutable::available(Path::new("/bin/echo").to_path_buf()),
            },
            spec,
            resource,
        })
        .unwrap()
    }

    fn response(route: &OriginRoute, protocol_version: u32) -> ClusterResponse {
        let receipt = ResourceQueueReceipt {
            request: route.request,
            task: route.task,
            origin_machine: route.origin_machine,
            authority_machine: route.execution_machine,
            resource: resource_from_route(route).unwrap(),
            outcome: ResourceQueueOutcome::Waiting,
        };
        ClusterResponse {
            status: StatusCode::OK,
            body: Bytes::from(
                serde_json::to_vec(&ResourceQueueResponse::new(protocol_version, receipt)).unwrap(),
            ),
        }
    }

    fn input(route: &OriginRoute) -> ResourceSubmitInput {
        ResourceSubmitInput {
            request: route.request,
            resource: resource_from_route(route).unwrap(),
            authority: route.execution_machine,
            spec: route.current_spec().unwrap().clone(),
            env: TaskEnv {
                path: "/different/bin".into(),
                home: "/different/home".into(),
            },
            callback_cwd: Path::new("/different/callback").to_path_buf(),
        }
    }

    #[tokio::test]
    async fn local_acceptance_and_exact_retry_wake_the_resource_actor() {
        let _guard = crate::daemon::actors::supervisor::SUPERVISOR_TEST_LOCK
            .lock()
            .await;
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().join("state"))).unwrap();
        home.ensure().unwrap();
        let (supervisor, supervisor_handle) = SupervisorActor::spawn(
            None,
            SupervisorActor,
            SupervisorArgs::new(home.clone(), None),
        )
        .await
        .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let machine = LocalMachine {
            identity: LocalIdentity::start(&home).unwrap(),
            name: MachineName::fallback(),
            protocol: SUPPORTED_PROTOCOLS,
        };
        let authority = machine.identity.machine;
        let resource_id = ResourceId::new();
        let resource = Resource::new(
            resource_id,
            "gpu-test".into(),
            authority,
            SupervisorAddress {
                machine: authority,
                thread: spec().thread,
            },
            AssignmentRevision::new(0),
            ResourceRevision::new(0),
            None,
        );
        call(&supervisor, |reply| SupervisorMsg::RegisterResource {
            resource: Box::new(resource),
            reply,
        })
        .await
        .unwrap();
        let route = local_route(authority, resource_id);
        call(&store, |reply| StoreMsg::InsertOriginRoute {
            route: Box::new(route.clone()),
            reply,
        })
        .await
        .unwrap();
        let state = AppState {
            home: home.clone(),
            store: store.clone(),
            supervisor: supervisor.clone(),
            web: None,
            content: None,
            stream_slots: StreamSlots::new(),
            machine,
            fleet: FleetState::Disabled,
            message_receiver: crate::daemon::message_receiver::MessageReceiver::default(),
            locks: crate::daemon::DaemonLocks::default(),
            thread_titles: None,
        };

        let first = submit_local(&state, &route).await.unwrap();
        assert_eq!(first.outcome, ResourceQueueOutcome::Waiting);
        let inspection = call(&supervisor, |reply| SupervisorMsg::InspectResource {
            id: resource_id,
            reply,
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::AttentionRequired {
                request,
                reason: ResourceQueueAttentionReason::IdleNotProven {
                    gap: crate::resource::IdleProofGap::NoIdleEvidence,
                },
            }) if request.request_id == route.request
        ));

        assert_eq!(submit_local(&state, &route).await.unwrap(), first);
        let requests = call(&store, |reply| StoreMsg::ResourceRequests {
            authority_machine: authority,
            resource_id,
            reply,
        })
        .await
        .unwrap();
        assert_eq!(requests.len(), 1);

        supervisor.stop(None);
        let _ = supervisor_handle.await;
    }

    #[test]
    fn response_requires_protocol_and_exact_receipt_identity() {
        let route = route();

        assert!(decode_resource_response(&route, response(&route, 7), 7).is_ok());
        assert!(matches!(
            decode_resource_response(&route, response(&route, 6), 7),
            Err(AppError::SubmissionOutcomeUnknown { .. })
        ));

        let mut wrong_identity: Value = serde_json::from_slice(&response(&route, 7).body).unwrap();
        wrong_identity["receipt"]["task"] = serde_json::to_value(TaskId::new()).unwrap();
        let invalid = ClusterResponse {
            status: StatusCode::OK,
            body: Bytes::from(serde_json::to_vec(&wrong_identity).unwrap()),
        };
        assert!(matches!(
            decode_resource_response(&route, invalid, 7),
            Err(AppError::SubmissionOutcomeUnknown { .. })
        ));
        assert!(route_acceptance_is_unknown(&route));
    }

    #[test]
    fn failed_response_stays_unknown() {
        let route = route();
        let failed = ClusterResponse {
            status: StatusCode::BAD_GATEWAY,
            body: Bytes::new(),
        };

        assert!(matches!(
            decode_resource_response(&route, failed, 7),
            Err(AppError::SubmissionOutcomeUnknown { .. })
        ));
        assert!(route_acceptance_is_unknown(&route));
    }

    #[test]
    fn retry_compares_authority_resource_and_spec_but_reuses_saved_callback() {
        let route = route();
        let retry = input(&route);

        assert!(validate_retry(&route, &retry).is_ok());

        let mut changed_authority = retry.clone();
        changed_authority.authority = MachineId::new();
        assert!(matches!(
            validate_retry(&route, &changed_authority),
            Err(AppError::SubmissionConflict { .. })
        ));

        let mut changed_resource = retry.clone();
        changed_resource.resource = ResourceId::new();
        assert!(matches!(
            validate_retry(&route, &changed_resource),
            Err(AppError::SubmissionConflict { .. })
        ));

        let mut changed_spec = retry;
        changed_spec.spec.name = serde_json::from_value(serde_json::json!("changed")).unwrap();
        assert!(matches!(
            validate_retry(&route, &changed_spec),
            Err(AppError::SubmissionConflict { .. })
        ));
    }

    #[test]
    fn saved_route_phase_maps_to_typed_outcomes() {
        let mut route = route();
        route.submission = SubmissionState::Resource {
            resource: resource_from_route(&route).unwrap(),
            phase: ResourceRoutePhase::Rejected {
                reason: "unavailable".into(),
            },
        };

        assert_eq!(
            outcome_from_route(&route).unwrap(),
            ResourceSubmitOutcome::Rejected {
                task: route.task,
                reason: "unavailable".into(),
            }
        );
    }

    #[test]
    fn recovery_retry_requires_exact_saved_identity_and_skips_later_phases() {
        let expected = route();
        let saved = expected.clone();

        assert!(ensure_same_recovery_identity(&expected, &saved).is_ok());
        assert_eq!(saved.request, expected.request);
        assert_eq!(saved.task, expected.task);
        assert_eq!(resource_from_route(&saved), resource_from_route(&expected));
        assert_eq!(saved.execution_machine, expected.execution_machine);
        assert_eq!(saved.callback, expected.callback);
        assert_eq!(saved.current_spec(), expected.current_spec());

        let mut changed_request = saved.clone();
        changed_request.request = RequestId::new();
        assert!(matches!(
            ensure_same_recovery_identity(&expected, &changed_request),
            Err(AppError::SubmissionConflict { .. })
        ));

        let mut changed_task = saved.clone();
        changed_task.task = TaskId::new();
        assert!(matches!(
            ensure_same_recovery_identity(&expected, &changed_task),
            Err(AppError::SubmissionConflict { .. })
        ));

        let mut changed_authority = saved.clone();
        changed_authority.execution_machine = MachineId::new();
        assert!(matches!(
            ensure_same_recovery_identity(&expected, &changed_authority),
            Err(AppError::SubmissionConflict { .. })
        ));

        let mut changed_resource = saved.clone();
        let SubmissionState::Resource { phase, .. } = &saved.submission else {
            unreachable!();
        };
        changed_resource.submission = SubmissionState::Resource {
            resource: ResourceId::new(),
            phase: phase.clone(),
        };
        assert!(matches!(
            ensure_same_recovery_identity(&expected, &changed_resource),
            Err(AppError::SubmissionConflict { .. })
        ));

        let mut changed_spec = saved.clone();
        changed_spec.spec.current_mut().unwrap().name =
            serde_json::from_value(serde_json::json!("changed")).unwrap();
        assert!(matches!(
            ensure_same_recovery_identity(&expected, &changed_spec),
            Err(AppError::SubmissionConflict { .. })
        ));

        let mut changed_callback = saved.clone();
        changed_callback.callback.cwd = Path::new("/changed/callback").to_path_buf();
        assert!(matches!(
            ensure_same_recovery_identity(&expected, &changed_callback),
            Err(AppError::SubmissionConflict { .. })
        ));

        let resource = resource_from_route(&saved).unwrap();
        let mut activated = saved.clone();
        activated.submission = SubmissionState::Resource {
            resource,
            phase: ResourceRoutePhase::Activated,
        };
        assert!(ensure_same_recovery_identity(&expected, &activated).is_ok());
        assert!(!route_acceptance_is_unknown(&activated));
        assert_eq!(
            outcome_from_route(&activated).unwrap(),
            ResourceSubmitOutcome::Activated {
                task: expected.task,
            }
        );

        let mut cancelled = saved.clone();
        cancelled.submission = SubmissionState::Resource {
            resource,
            phase: ResourceRoutePhase::CancelledBeforeLaunch,
        };
        assert!(ensure_same_recovery_identity(&expected, &cancelled).is_ok());
        assert!(!route_acceptance_is_unknown(&cancelled));
        assert_eq!(
            outcome_from_route(&cancelled).unwrap(),
            ResourceSubmitOutcome::CancelledBeforeLaunch {
                task: expected.task,
            }
        );
    }

    #[test]
    fn response_identity_includes_authority_and_resource() {
        let route = route();
        let mut wrong_resource: Value = serde_json::from_slice(&response(&route, 7).body).unwrap();
        wrong_resource["receipt"]["resource"] = serde_json::to_value(ResourceId::new()).unwrap();
        let invalid = ClusterResponse {
            status: StatusCode::OK,
            body: Bytes::from(serde_json::to_vec(&wrong_resource).unwrap()),
        };
        assert!(matches!(
            decode_resource_response(&route, invalid, 7),
            Err(AppError::SubmissionOutcomeUnknown { .. })
        ));

        let mut wrong_destination: Value =
            serde_json::from_slice(&response(&route, 7).body).unwrap();
        wrong_destination["destination_machine"] = serde_json::to_value(MachineId::new()).unwrap();
        let invalid = ClusterResponse {
            status: StatusCode::OK,
            body: Bytes::from(serde_json::to_vec(&wrong_destination).unwrap()),
        };
        assert!(matches!(
            decode_resource_response(&route, invalid, 7),
            Err(AppError::SubmissionOutcomeUnknown { .. })
        ));
    }
}
