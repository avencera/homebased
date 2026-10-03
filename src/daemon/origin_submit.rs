//! Origin-owned remote submission and unknown-acceptance resolution

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::warn;

use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::api::{DryRunResponse, SubmitBody};
use super::cluster::{
    AbandonExecution, IdentityBody, PreviewBody, PreviewExecution, SubmitExecution,
};
use crate::dependency::TaskDependencies;
use crate::domain::{API_VERSION, AgentKind, ProcessStatus, TaskEnv, TaskId, TaskStatus};
use crate::error::AppError;
use crate::fleet::directory::NameTarget;
use crate::fleet::http::{ClusterClient, ClusterResponse};
use crate::fleet::protocol::{CLUSTER_PROTOCOL_VERSION, ClusterProtocolVersion};
use crate::invocation::resolve_agent_binary;
use crate::machine::{MachineId, MachineName};
use crate::spec::{self, NormalizedSpec};
use crate::submission::{
    CallbackContext, ExecutorIdentity, HeldPhase, OriginRoute, SubmissionState,
};

/// Submit a remote request once or resolve its saved outcome without resending
pub(super) async fn submit(
    state: &AppState,
    body: SubmitBody,
) -> Result<(TaskId, TaskStatus), AppError> {
    let request = body.request;
    let _request_guard = state.locks.origin_submissions.lock(request).await;
    let saved = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request,
        reply,
    })
    .await?;
    if let Some(route) = saved {
        check_retry(state, &route, &body).await?;
        return finish_saved(state, route).await;
    }

    let machine_name = body
        .spec
        .machine
        .as_ref()
        .ok_or_else(|| AppError::Internal {
            message: "remote dispatch has no machine".into(),
        })?;
    let machine = execution_machine(state, machine_name).await?;
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| AppError::MachineNotFound {
            machine: machine_name.to_string(),
        })?;
    let destination = fleet.connect(machine).await?;
    let callback = callback_context(body.env, body.callback_cwd)?;
    let task = TaskId::new();
    let route = OriginRoute {
        request,
        task,
        origin_machine: state.machine.identity.machine,
        execution_machine: machine,
        thread: body.spec.thread,
        callback,
        spec: body.spec.into(),
        submission: SubmissionState::AcceptanceUnknown,
        last_execution_state: None,
        last_updated_at: Some(chrono::Utc::now()),
        last_accepted_seq: 0,
        last_settled_seq: 0,
    };
    let saved = insert_route(state, route, body.after).await?;
    if saved.task != task {
        return finish_saved(state, saved).await;
    }
    let wire = execution_wire(&saved, destination.protocol, machine)?;
    let response = ClusterClient::default()
        .post_json(&destination.address, "/v1/cluster/executions", &wire)
        .await
        .map_err(|error| unknown(&saved, error.to_string()))?;
    let identity = decode_identity(&saved, response, destination.protocol)?;
    resolve_identity(state, saved, identity.identity).await
}

/// Save a new route with its dependencies, or return the route an identical request saved
pub(super) async fn insert_route(
    state: &AppState,
    route: OriginRoute,
    after: Option<TaskDependencies>,
) -> Result<OriginRoute, AppError> {
    let request = route.request;
    let inserted = match after {
        Some(after) => {
            call(&state.store, |reply| StoreMsg::InsertOriginRouteAfter {
                route: Box::new(route),
                after,
                reply,
            })
            .await
        }
        None => {
            call(&state.store, |reply| StoreMsg::InsertOriginRoute {
                route: Box::new(route),
                reply,
            })
            .await
        }
    };
    let error = match inserted {
        Ok(saved) => return Ok(saved),
        Err(error) => error,
    };

    let found = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request,
        reply,
    })
    .await?;

    match found {
        Some(found) => Err(conflict(&found, error.to_string())),
        None => Err(error),
    }
}

/// Refuse a retry whose spec or dependencies differ from the saved request
async fn check_retry(
    state: &AppState,
    route: &OriginRoute,
    body: &SubmitBody,
) -> Result<(), AppError> {
    if !matches!(route.submission, SubmissionState::Held { .. }) {
        ensure_direct_route(route)?;
    }
    let saved_spec = route
        .spec
        .current()
        .ok_or_else(|| conflict(route, "migrated local task has no remote request"))?;
    if *saved_spec != body.spec {
        return Err(conflict(
            route,
            "request UUID has different normalized content",
        ));
    }
    let saved_after = call(&state.store, |reply| StoreMsg::RouteDependencies {
        id: route.task,
        reply,
    })
    .await?;
    if saved_after != body.after {
        return Err(conflict(route, "request UUID has different dependencies"));
    }
    Ok(())
}

/// Resolve a spec machine name to the peer that will execute it
pub(super) async fn execution_machine(
    state: &AppState,
    machine_name: &MachineName,
) -> Result<MachineId, AppError> {
    let Some(fleet) = state.fleet.handle() else {
        if machine_name == &state.machine.name {
            return Err(local_machine_selector(machine_name));
        }
        return Err(AppError::MachineNotFound {
            machine: machine_name.to_string(),
        });
    };
    match fleet.resolve_name(machine_name).await? {
        NameTarget::Local => Err(local_machine_selector(machine_name)),
        NameTarget::Peer(machine) => Ok(machine),
    }
}

/// Origin-only callback context for a remote task, from the submitting shell
pub(super) fn callback_context(
    env: TaskEnv,
    callback_cwd: Option<std::path::PathBuf>,
) -> Result<CallbackContext, AppError> {
    let callback_cwd = callback_cwd.ok_or_else(|| AppError::InvalidSpec {
        pointer: "/callback_cwd".into(),
        value: Value::Null,
        message: "remote submission needs the CLI callback directory".into(),
    })?;
    if !callback_cwd.is_absolute() {
        return Err(AppError::InvalidSpec {
            pointer: "/callback_cwd".into(),
            value: serde_json::to_value(&callback_cwd)?,
            message: "callback directory must be absolute".into(),
        });
    }
    spec::check_cwd(&callback_cwd)?;
    let codex = resolve_agent_binary(AgentKind::Codex, &env.path, &callback_cwd)?;
    Ok(CallbackContext {
        env,
        cwd: callback_cwd,
        codex: codex.into(),
    })
}

/// The submit sent to an executor: the saved normalized spec under the saved task UUID
///
/// The spec is the route's own normalized content. Dependencies are saved
/// beside the route, so they cannot reach the executor
fn execution_wire(
    route: &OriginRoute,
    protocol: ClusterProtocolVersion,
    machine: MachineId,
) -> Result<SubmitExecution, AppError> {
    let spec = route
        .spec
        .current()
        .ok_or_else(|| conflict(route, "migrated local task has no remote request"))?;
    Ok(SubmitExecution {
        api_version: API_VERSION,
        protocol_version: protocol.0,
        destination_machine: machine,
        origin_machine: route.origin_machine,
        task: route.task,
        spec: serde_json::to_value(spec)?,
        unknown: BTreeMap::new(),
    })
}

/// Launch a held remote task whose dependencies all succeeded
///
/// The route stays waiting until the executor is reachable, then becomes
/// launching before the send. A lost reply leaves it launching, and the next
/// attempt resends the same task UUID, which the executor accepts once
pub(super) async fn release(state: &AppState, route: OriginRoute) -> Result<(), AppError> {
    let _request_guard = state.locks.origin_submissions.lock(route.request).await;
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| unknown(&route, "fleet is disabled"))?;
    let destination = fleet.connect(route.execution_machine).await?;
    let route = call(&state.store, |reply| StoreMsg::BeginHeldLaunch {
        id: route.task,
        reply,
    })
    .await?;
    if !matches!(
        route.submission,
        SubmissionState::Held {
            phase: HeldPhase::Launching
        }
    ) {
        return Ok(());
    }
    let wire = execution_wire(&route, destination.protocol, route.execution_machine)?;
    let response = ClusterClient::default()
        .post_json(&destination.address, "/v1/cluster/executions", &wire)
        .await
        .map_err(|error| unknown(&route, error.to_string()))?;
    let identity = decode_identity(&route, response, destination.protocol)?;
    let task = route.task;
    let resolved = resolve_identity(state, route, identity.identity).await;
    // a refused launch queued the held task's terminal event
    state
        .supervisor
        .cast(SupervisorMsg::DispatchInbox { id: task })?;
    match resolved {
        Ok(_) => Ok(()),
        Err(error) if is_definite_rejection(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Whether recovery ended in the executor's saved refusal rather than a failure
fn is_definite_rejection(error: &AppError) -> bool {
    match error {
        AppError::SubmissionRejected { .. } | AppError::InvalidCwd { .. } => true,
        // a refused mount source is rebuilt as this pointer
        AppError::InvalidSpec { pointer, .. } => pointer == "/workload/mounts",
        _ => false,
    }
}

/// Error for an executor refusal, typed when the executor refused a host input
fn rejected(route: &OriginRoute, reason: &str) -> AppError {
    match (
        spec::HostInputRejection::parse(reason),
        route.current_spec(),
    ) {
        (Some(rejection), Some(spec)) => rejection.into_error(spec),
        _ => AppError::SubmissionRejected {
            request: route.request,
            task: route.task,
            reason: reason.to_owned(),
        },
    }
}

fn local_machine_selector(machine: &MachineName) -> AppError {
    AppError::InvalidSpec {
        pointer: "/machine".into(),
        value: Value::String(machine.to_string()),
        message: "omit machine to submit this task locally".into(),
    }
}

async fn finish_saved(
    state: &AppState,
    route: OriginRoute,
) -> Result<(TaskId, TaskStatus), AppError> {
    match &route.submission {
        SubmissionState::Accepted => Ok((
            route.task,
            route
                .last_execution_state
                .unwrap_or(ProcessStatus::Queued)
                .into(),
        )),
        SubmissionState::Rejected { reason } => Err(rejected(&route, reason)),
        SubmissionState::AcceptanceUnknown => reconcile(state, route).await,
        // the dependency release owns a held route, including its launch
        SubmissionState::Held { phase } => Ok((route.task, phase.status())),
    }
}

async fn reconcile(state: &AppState, route: OriginRoute) -> Result<(TaskId, TaskStatus), AppError> {
    ensure_direct_route(&route)?;
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| unknown(&route, "fleet is disabled"))?;
    let destination = fleet
        .connect(route.execution_machine)
        .await
        .map_err(|error| unknown(&route, error.to_string()))?;
    let query = format!(
        "/v1/cluster/executions/{}?api_version={API_VERSION}&destination_machine={}",
        route.task, route.execution_machine,
    );
    let response = ClusterClient::default()
        .get(&destination.address, &query)
        .await
        .map_err(|error| unknown(&route, error.to_string()))?;
    let found = decode_identity(&route, response, CLUSTER_PROTOCOL_VERSION)?;
    if found.identity.is_some() {
        return resolve_identity(state, route, found.identity).await;
    }
    let destination = fleet
        .connect(route.execution_machine)
        .await
        .map_err(|error| unknown(&route, error.to_string()))?;
    let abandon = AbandonExecution {
        api_version: API_VERSION,
        protocol_version: destination.protocol.0,
        destination_machine: route.execution_machine,
        origin_machine: route.origin_machine,
        task: route.task,
    };
    let response = ClusterClient::default()
        .post_json(
            &destination.address,
            "/v1/cluster/executions/abandon",
            &abandon,
        )
        .await
        .map_err(|error| unknown(&route, error.to_string()))?;
    let found = decode_identity(&route, response, destination.protocol)?;
    resolve_identity(state, route, found.identity).await
}

/// Resolve routes left unknown by a prior daemon process without resending them
pub(super) async fn recover(state: AppState) {
    let routes = match call(&state.store, |reply| StoreMsg::UnknownOriginRoutes {
        reply,
    })
    .await
    {
        Ok(routes) => routes,
        Err(error) => {
            warn!("cannot scan unresolved origin submissions: {error}");
            return;
        }
    };
    for route in routes {
        if let Err(error) = reconcile(&state, route).await
            && !is_definite_rejection(&error)
        {
            warn!("origin submission recovery: {error}");
        }
    }
}

async fn resolve_identity(
    state: &AppState,
    route: OriginRoute,
    identity: Option<ExecutorIdentity>,
) -> Result<(TaskId, TaskStatus), AppError> {
    if !matches!(
        route.submission,
        SubmissionState::Held {
            phase: HeldPhase::Launching
        }
    ) {
        ensure_direct_route(&route)?;
    }
    let (outcome, status) = match identity {
        Some(ExecutorIdentity::Accepted(record)) => {
            if record.task != route.task
                || record.origin_machine != route.origin_machine
                || record.execution_machine != route.execution_machine
                || record.spec != route.spec
            {
                return Err(conflict(&route, "executor accepted a different identity"));
            }
            (SubmissionState::Accepted, record.state)
        }
        Some(ExecutorIdentity::Rejected(record)) => {
            if record.task != route.task
                || record.origin_machine != route.origin_machine
                || record.execution_machine != route.execution_machine
            {
                return Err(conflict(&route, "executor rejected a different owner"));
            }
            (
                SubmissionState::Rejected {
                    reason: record.reason,
                },
                ProcessStatus::Queued,
            )
        }
        None => return Err(unknown(&route, "executor returned no definitive identity")),
    };
    let saved = call(&state.store, |reply| StoreMsg::ResolveOriginRoute {
        id: route.task,
        outcome,
        reply,
    })
    .await
    .map_err(|error| unknown(&route, format!("cannot save executor result: {error}")))?;
    match saved.submission {
        SubmissionState::Accepted => Ok((
            route.task,
            saved.last_execution_state.unwrap_or(status).into(),
        )),
        SubmissionState::Rejected { reason } => Err(rejected(&route, &reason)),
        SubmissionState::AcceptanceUnknown | SubmissionState::Held { .. } => {
            Err(unknown(&route, "origin route is unresolved"))
        }
    }
}

fn decode_response<T: DeserializeOwned>(
    route: &OriginRoute,
    response: ClusterResponse,
) -> Result<T, AppError> {
    if !response.status.is_success() {
        return Err(unknown(
            route,
            format!("executor returned HTTP {}", response.status),
        ));
    }
    let value: Value = serde_json::from_slice(&response.body)
        .map_err(|error| unknown(route, format!("invalid executor response: {error}")))?;
    if value.get("api_version").and_then(Value::as_u64) != Some(u64::from(API_VERSION)) {
        return Err(unknown(
            route,
            "executor returned an unsupported API version",
        ));
    }
    serde_json::from_value(value)
        .map_err(|error| unknown(route, format!("invalid executor response: {error}")))
}

fn decode_identity(
    route: &OriginRoute,
    response: ClusterResponse,
    protocol: ClusterProtocolVersion,
) -> Result<IdentityBody, AppError> {
    let body: IdentityBody = decode_response(route, response)?;
    if body.api_version != API_VERSION {
        return Err(unknown(
            route,
            "executor returned an unsupported API version",
        ));
    }
    if body.protocol_version != protocol.0 {
        return Err(unknown(route, "executor returned another protocol version"));
    }
    Ok(body)
}

fn unknown(route: &OriginRoute, message: impl Into<String>) -> AppError {
    AppError::SubmissionOutcomeUnknown {
        request: route.request,
        task: route.task,
        message: message.into(),
    }
}

pub(super) fn conflict(route: &OriginRoute, message: impl Into<String>) -> AppError {
    AppError::SubmissionConflict {
        request: route.request,
        task: route.task,
        message: message.into(),
    }
}

fn ensure_direct_route(route: &OriginRoute) -> Result<(), AppError> {
    if route.spec.current().is_none() {
        return Err(conflict(route, "migrated local task has no remote request"));
    }
    Ok(())
}

/// Ask the execution owner to validate and expand a remote invocation without identity storage
pub(super) async fn dry_run(
    state: &AppState,
    spec: NormalizedSpec,
    env: TaskEnv,
) -> Result<DryRunResponse, AppError> {
    let name = spec.machine.as_ref().ok_or_else(|| AppError::Internal {
        message: "remote dry-run has no machine".into(),
    })?;
    let Some(fleet) = state.fleet.handle() else {
        if name == &state.machine.name {
            return local_dry_run(state, spec, env);
        }
        return Err(AppError::MachineNotFound {
            machine: name.to_string(),
        });
    };
    let machine = match fleet.resolve_name(name).await? {
        NameTarget::Local => return local_dry_run(state, spec, env),
        NameTarget::Peer(machine) => machine,
    };
    let destination = fleet.connect(machine).await?;
    let body = PreviewExecution {
        api_version: API_VERSION,
        protocol_version: destination.protocol.0,
        destination_machine: machine,
        spec: serde_json::to_value(&spec)?,
    };
    let response = ClusterClient::default()
        .post_json(
            &destination.address,
            "/v1/cluster/executions/preview",
            &body,
        )
        .await
        .map_err(|error| AppError::MachineUnavailable {
            machine,
            message: error.to_string(),
        })?;
    if !response.status.is_success() {
        return Err(crate::client::map_error(response.status, &response.body));
    }
    let value: Value = serde_json::from_slice(&response.body).map_err(|error| {
        AppError::RemoteSubmissionUnavailable {
            message: format!("invalid executor preview response: {error}"),
        }
    })?;
    let preview = decode_preview(value, destination.protocol)?;
    Ok(DryRunResponse {
        api_version: API_VERSION,
        spec,
        argv: preview.argv,
        stdin: preview.stdin,
        execution_cwd: Some(preview.cwd),
        managed_environment: None,
        after: None,
    })
}

fn decode_preview(value: Value, protocol: ClusterProtocolVersion) -> Result<PreviewBody, AppError> {
    if value.get("api_version").and_then(Value::as_u64) != Some(u64::from(API_VERSION)) {
        return Err(AppError::RemoteSubmissionUnavailable {
            message: "executor preview returned an unsupported API version".into(),
        });
    }
    let preview: PreviewBody =
        serde_json::from_value(value).map_err(|error| AppError::RemoteSubmissionUnavailable {
            message: format!("invalid executor preview response: {error}"),
        })?;
    if preview.api_version != API_VERSION {
        return Err(AppError::RemoteSubmissionUnavailable {
            message: "executor preview returned an unsupported API version".into(),
        });
    }
    if preview.protocol_version != protocol.0 {
        return Err(AppError::RemoteSubmissionUnavailable {
            message: "executor preview returned another protocol version".into(),
        });
    }
    Ok(preview)
}

fn local_dry_run(
    state: &AppState,
    mut spec: NormalizedSpec,
    env: TaskEnv,
) -> Result<DryRunResponse, AppError> {
    spec.machine = None;
    super::api::local_dry_run(state, spec, env)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{decode_identity, decode_preview};
    use crate::daemon::cluster::PreviewBody;
    use crate::domain::{API_VERSION, TaskEnv, TaskId};
    use crate::error::AppError;
    use crate::fleet::http::ClusterResponse;
    use crate::fleet::protocol::ClusterProtocolVersion;
    use crate::machine::MachineId;
    use crate::spec::NormalizedSpec;
    use crate::submission::{
        CallbackContext, CallbackExecutable, OriginRoute, RequestId, SubmissionState,
    };
    use axum::http::StatusCode;
    use bytes::Bytes;

    fn direct_route() -> OriginRoute {
        let spec: NormalizedSpec = serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "remote task",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["echo", "hello"] }
        }))
        .unwrap();
        OriginRoute {
            request: RequestId::new(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            execution_machine: MachineId::new(),
            thread: spec.thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: Path::new("/tmp").to_path_buf(),
                codex: CallbackExecutable::available(Path::new("/bin/echo").to_path_buf()),
            },
            spec: spec.into(),
            submission: SubmissionState::AcceptanceUnknown,
            last_execution_state: None,
            last_updated_at: None,
            last_accepted_seq: 0,
            last_settled_seq: 0,
        }
    }

    fn identity_response(protocol_version: u32) -> ClusterResponse {
        ClusterResponse {
            status: StatusCode::OK,
            body: Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "api_version": API_VERSION,
                    "protocol_version": protocol_version,
                    "identity": null
                }))
                .unwrap(),
            ),
        }
    }

    #[test]
    fn identity_decoder_accepts_the_selected_protocol_version() {
        let route = direct_route();
        let selected = ClusterProtocolVersion(2);

        let identity = decode_identity(&route, identity_response(2), selected).unwrap();

        assert_eq!(identity.protocol_version, selected.0);
        assert!(matches!(
            decode_identity(&route, identity_response(1), selected),
            Err(AppError::SubmissionOutcomeUnknown { .. })
        ));
    }

    #[test]
    fn preview_decoder_accepts_the_selected_protocol_version() {
        let selected = ClusterProtocolVersion(2);
        let preview = PreviewBody {
            api_version: API_VERSION,
            protocol_version: selected.0,
            cwd: Path::new("/tmp").to_path_buf(),
            argv: vec!["echo".into(), "hello".into()],
            stdin: crate::invocation::StdinPolicy::Null,
        };
        let value = serde_json::to_value(preview).unwrap();

        assert_eq!(decode_preview(value, selected).unwrap().protocol_version, 2);
        let mismatched = serde_json::json!({
            "api_version": API_VERSION,
            "protocol_version": 1,
            "cwd": "/tmp",
            "argv": ["echo", "hello"],
            "stdin": "null"
        });
        assert!(matches!(
            decode_preview(mismatched, selected),
            Err(AppError::RemoteSubmissionUnavailable { .. })
        ));
    }
}
