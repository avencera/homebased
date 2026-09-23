//! Origin-owned remote submission and unknown-acceptance resolution

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;

use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::warn;

use super::AppState;
use super::actors::{StoreMsg, call};
use super::api::{DryRunResponse, SubmitBody};
use super::cluster::{
    AbandonExecution, IdentityBody, PreviewBody, PreviewExecution, SubmitExecution,
};
use crate::domain::{API_VERSION, AgentKind, ProcessStatus, TaskEnv, TaskId};
use crate::error::AppError;
use crate::fleet::directory::NameTarget;
use crate::fleet::http::{ClusterClient, ClusterResponse};
use crate::fleet::protocol::{CLUSTER_PROTOCOL_VERSION, ClusterProtocolVersion};
use crate::invocation::resolve_agent_binary;
use crate::spec::{self, NormalizedSpec};
use crate::submission::{
    CallbackContext, ExecutorIdentity, OriginRoute, RequestId, SubmissionState,
};

const SUBMISSION_SHARDS: usize = 64;
static SUBMISSION_PERMITS: OnceLock<[Semaphore; SUBMISSION_SHARDS]> = OnceLock::new();

async fn lock_request(request: RequestId) -> Result<SemaphorePermit<'static>, AppError> {
    let permits = SUBMISSION_PERMITS.get_or_init(|| std::array::from_fn(|_| Semaphore::new(1)));
    let mut hasher = DefaultHasher::new();
    request.hash(&mut hasher);
    let shard = (hasher.finish() as usize) % SUBMISSION_SHARDS;
    permits[shard]
        .acquire()
        .await
        .map_err(|error| AppError::Internal {
            message: format!("submission permit unavailable: {error}"),
        })
}

/// Submit a remote request once or resolve its saved outcome without resending
pub(super) async fn submit(
    state: &AppState,
    body: SubmitBody,
) -> Result<(TaskId, ProcessStatus, Option<RequestId>), AppError> {
    let request = body.request.ok_or_else(|| AppError::Internal {
        message: "remote dispatch has no request UUID".into(),
    })?;
    let _request_guard = lock_request(request).await?;
    let saved = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request,
        reply,
    })
    .await?;
    if let Some(route) = saved {
        ensure_direct_route(&route)?;
        let saved_spec = route
            .spec
            .current()
            .ok_or_else(|| conflict(&route, "migrated local task has no remote request"))?;
        if serde_json::to_value(saved_spec)? != serde_json::to_value(&body.spec)? {
            return Err(conflict(
                &route,
                "request UUID has different normalized content",
            ));
        }
        let (task, status) = finish_saved(state, route).await?;
        return Ok((task, status, Some(request)));
    }

    let machine_name = body
        .spec
        .machine
        .as_ref()
        .ok_or_else(|| AppError::Internal {
            message: "remote dispatch has no machine".into(),
        })?;
    let Some(fleet) = state.fleet.handle() else {
        if machine_name == &state.machine.name {
            return Err(local_machine_selector(machine_name));
        }
        return Err(AppError::MachineNotFound {
            machine: machine_name.to_string(),
        });
    };
    let machine = match fleet.resolve_name(machine_name).await? {
        NameTarget::Local => return Err(local_machine_selector(machine_name)),
        NameTarget::Peer(machine) => machine,
    };
    let destination = fleet.connect(machine).await?;
    let callback_cwd = body.callback_cwd.ok_or_else(|| AppError::InvalidSpec {
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
    let codex = resolve_agent_binary(AgentKind::Codex, &body.env.path, &callback_cwd)?;
    let task = TaskId::new();
    let route = OriginRoute {
        request,
        task,
        origin_machine: state.machine.identity.machine,
        execution_machine: machine,
        thread: body.spec.thread,
        callback: CallbackContext {
            env: body.env,
            cwd: callback_cwd,
            codex: codex.into(),
        },
        spec: body.spec.into(),
        submission: SubmissionState::AcceptanceUnknown,
        last_execution_state: None,
        last_updated_at: Some(chrono::Utc::now()),
        last_accepted_seq: 0,
        last_settled_seq: 0,
    };
    let saved = match call(&state.store, |reply| StoreMsg::InsertOriginRoute {
        route: Box::new(route),
        reply,
    })
    .await
    {
        Ok(saved) => saved,
        Err(error) => {
            let found = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
                request,
                reply,
            })
            .await?;
            if let Some(found) = found {
                return Err(conflict(&found, error.to_string()));
            }
            return Err(error);
        }
    };
    if saved.task != task {
        let (task, status) = finish_saved(state, saved).await?;
        return Ok((task, status, Some(request)));
    }
    let wire = SubmitExecution {
        api_version: API_VERSION,
        protocol_version: destination.protocol.0,
        destination_machine: machine,
        origin_machine: saved.origin_machine,
        task,
        spec: serde_json::to_value(
            saved
                .spec
                .current()
                .ok_or_else(|| conflict(&saved, "migrated local task has no remote request"))?,
        )?,
        unknown: BTreeMap::new(),
    };
    let response = ClusterClient::default()
        .post_json(&destination.address, "/v1/cluster/executions", &wire)
        .await
        .map_err(|error| unknown(&saved, error.to_string()))?;
    let identity = decode_identity(&saved, response, destination.protocol)?;
    let (task, status) = resolve_identity(state, saved, identity.identity).await?;
    Ok((task, status, Some(request)))
}

fn local_machine_selector(machine: &crate::machine::MachineName) -> AppError {
    AppError::InvalidSpec {
        pointer: "/machine".into(),
        value: Value::String(machine.to_string()),
        message: "omit machine to submit this task locally".into(),
    }
}

async fn finish_saved(
    state: &AppState,
    route: OriginRoute,
) -> Result<(TaskId, ProcessStatus), AppError> {
    match &route.submission {
        SubmissionState::Accepted => Ok((
            route.task,
            route.last_execution_state.unwrap_or(ProcessStatus::Queued),
        )),
        SubmissionState::Rejected { reason } => Err(AppError::SubmissionRejected {
            request: route.request,
            task: route.task,
            reason: reason.clone(),
        }),
        SubmissionState::AcceptanceUnknown => reconcile(state, route).await,
        SubmissionState::Resource { .. } => {
            Err(conflict(&route, "request UUID belongs to a resource route"))
        }
    }
}

async fn reconcile(
    state: &AppState,
    route: OriginRoute,
) -> Result<(TaskId, ProcessStatus), AppError> {
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
        "/v1/cluster/executions/{}?api_version={}&destination_machine={}",
        route.task, API_VERSION, route.execution_machine,
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
            && !matches!(error, AppError::SubmissionRejected { .. })
        {
            warn!("origin submission recovery: {error}");
        }
    }
}

async fn resolve_identity(
    state: &AppState,
    route: OriginRoute,
    identity: Option<ExecutorIdentity>,
) -> Result<(TaskId, ProcessStatus), AppError> {
    ensure_direct_route(&route)?;
    let (outcome, status) = match identity {
        Some(ExecutorIdentity::Accepted(record)) => {
            if record.task != route.task
                || record.origin_machine != route.origin_machine
                || record.execution_machine != route.execution_machine
                || serde_json::to_value(&record.spec)? != serde_json::to_value(&route.spec)?
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
        SubmissionState::Accepted => Ok((route.task, saved.last_execution_state.unwrap_or(status))),
        SubmissionState::Rejected { reason } => Err(AppError::SubmissionRejected {
            request: route.request,
            task: route.task,
            reason,
        }),
        SubmissionState::AcceptanceUnknown => Err(unknown(&route, "origin route is unresolved")),
        SubmissionState::Resource { .. } => {
            Err(conflict(&saved, "request UUID belongs to a resource route"))
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

fn conflict(route: &OriginRoute, message: impl Into<String>) -> AppError {
    AppError::SubmissionConflict {
        request: route.request,
        task: route.task,
        message: message.into(),
    }
}

fn ensure_direct_route(route: &OriginRoute) -> Result<(), AppError> {
    if matches!(route.submission, SubmissionState::Resource { .. }) {
        return Err(conflict(route, "request UUID belongs to a resource route"));
    }
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

    use super::*;
    use crate::domain::TaskEnv;
    use crate::machine::MachineId;
    use crate::resource::ResourceId;
    use crate::submission::{
        CallbackContext, CallbackExecutable, NewResourceRoute, RequestId, ResourceRoutePhase,
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

    #[test]
    fn direct_retry_rejects_a_saved_resource_request_before_reconciliation() {
        let spec: NormalizedSpec = serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "resource task",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["echo", "hello"] }
        }))
        .unwrap();
        let route = OriginRoute::new_resource_waiting(NewResourceRoute {
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
                codex: Path::new("/bin/echo").to_path_buf().into(),
            },
            spec,
            resource: ResourceId::new(),
        })
        .unwrap();

        assert!(matches!(
            ensure_direct_route(&route),
            Err(AppError::SubmissionConflict { .. })
        ));
        assert!(matches!(
            route.submission,
            SubmissionState::Resource {
                phase: ResourceRoutePhase::AcceptanceUnknown,
                ..
            }
        ));
    }
}
