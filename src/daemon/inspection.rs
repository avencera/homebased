//! Fleet task inspection through the caller's local daemon

use std::collections::{BTreeMap, BTreeSet};

use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinSet;

use crate::cancellation::{CancellationOwner, CancellationRoute};
use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, call};
use crate::daemon::cluster::{
    CallbackFailureSummary, ExecutionBody, ExecutionView, IdentitySummary, IdentitySummaryBody,
    OriginBody, OriginSummary,
};
use crate::domain::{API_VERSION, TaskId};
use crate::error::AppError;
use crate::fleet::address::MachineAddress;
use crate::fleet::http::ClusterClient;
use crate::fleet::probe::{VerifiedDestination, check_probed, probe};
use crate::fleet::protocol::SUPPORTED_PROTOCOLS;
use crate::fleet::runtime::FleetHandle;
use crate::machine::MachineId;
use crate::submission::{
    ExecutorIdentity, ResourceActionRoutePhase, ResourceBackgroundRoutePhase, SubmissionState,
};

/// Strict read query for a peer log, with an optional line limit
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClusterLogQuery {
    /// Public API version
    pub api_version: u32,
    /// Intended receiver
    pub destination_machine: MachineId,
    /// Last number of lines
    pub tail: Option<usize>,
}

#[derive(Default)]
struct Records {
    executions: BTreeMap<MachineId, ExecutionView>,
    routes: BTreeMap<MachineId, OriginSummary>,
    identities: BTreeMap<MachineId, IdentitySummary>,
    unchecked: BTreeSet<MachineId>,
    addresses: BTreeMap<MachineId, MachineAddress>,
}

struct PeerRecords {
    machine: MachineId,
    execution: Option<ExecutionView>,
    route: Option<OriginSummary>,
    identity: Option<IdentitySummary>,
    incomplete: bool,
    address: Option<MachineAddress>,
}

impl Records {
    fn add(&mut self, peer: PeerRecords) {
        if let Some(execution) = peer.execution {
            self.executions.insert(peer.machine, execution);
        }
        if let Some(route) = peer.route {
            self.routes.insert(peer.machine, route);
        }
        if let Some(identity) = peer.identity {
            self.identities.insert(peer.machine, identity);
        }
        if peer.incomplete {
            self.unchecked.insert(peer.machine);
        }
        if let Some(address) = peer.address {
            self.addresses.insert(peer.machine, address);
        }
    }

    fn route(&self, task: TaskId) -> Result<Option<(&MachineId, &OriginSummary)>, AppError> {
        let mut routes = self.routes.iter();
        let first = routes.next();
        if let Some((_, route)) = first
            && routes.any(|(_, other)| {
                other.task != route.task
                    || other.request_id != route.request_id
                    || other.submission != route.submission
                    || other.execution_machine != route.execution_machine
                    || other.origin_machine != route.origin_machine
            })
        {
            return Err(AppError::ClusterTaskConflict { task });
        }
        Ok(first)
    }

    fn check_conflicts(&self, task: TaskId) -> Result<(), AppError> {
        let route = self.route(task)?.map(|(_, route)| route);
        if self.executions.len() > 1 {
            return Err(AppError::ClusterTaskConflict { task });
        }
        if let Some((machine, _)) = self.executions.first_key_value()
            && route.is_some_and(|route| {
                route.execution_machine != *machine
                    || matches!(route.submission, SubmissionState::Rejected { .. })
            })
        {
            return Err(AppError::ClusterTaskConflict { task });
        }
        for (machine, identity) in &self.identities {
            let (origin, execution) = identity.owners();
            if execution != *machine
                || route.is_some_and(|route| {
                    route.origin_machine != origin
                        || route.execution_machine != execution
                        || (matches!(route.submission, SubmissionState::Rejected { .. })
                            && matches!(identity, IdentitySummary::Accepted { .. }))
                        || (matches!(route.submission, SubmissionState::Accepted)
                            && matches!(identity, IdentitySummary::Rejected { .. }))
                        || matches!(
                            (&route.submission, identity),
                            (SubmissionState::Rejected { reason: route_reason }, IdentitySummary::Rejected { reason, .. })
                                if route_reason != reason
                        )
                })
                || self
                    .executions
                    .first_key_value()
                    .is_some_and(|(owner, _)| *owner != execution)
                || (matches!(identity, IdentitySummary::Rejected { .. })
                    && self.executions.contains_key(machine))
            {
                return Err(AppError::ClusterTaskConflict { task });
            }
        }
        if self.identities.len() > 1 {
            return Err(AppError::ClusterTaskConflict { task });
        }
        Ok(())
    }
}

impl IdentitySummary {
    fn task(&self) -> TaskId {
        match self {
            Self::Accepted { task, .. } | Self::Rejected { task, .. } => *task,
        }
    }

    fn owners(&self) -> (MachineId, MachineId) {
        match self {
            Self::Accepted {
                origin_machine,
                execution_machine,
                ..
            }
            | Self::Rejected {
                origin_machine,
                execution_machine,
                ..
            } => (*origin_machine, *execution_machine),
        }
    }
}

/// Inspect a task from the local socket, regardless of execution machine
pub(super) async fn show(state: &AppState, id: TaskId) -> Result<Value, AppError> {
    if call(&state.store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .is_some()
    {
        let mut value = serde_json::to_value(crate::daemon::api::local_detail(state, id).await?)?;
        let machine = state.machine.identity.machine;
        let origin = local_identity_origin(state, id).await?.unwrap_or(machine);
        annotate(&mut value, origin, machine, machine, "available");
        if let Some(failed_events) = local_failures(state, id).await? {
            value["failed_events"] = json!(failed_events);
        }
        return Ok(value);
    }
    let records = lookup(state, id).await?;
    if let Some((machine, _)) = records.executions.first_key_value() {
        let origin = origin_for(&records, *machine);
        match remote_read(
            state,
            &records,
            *machine,
            &format!("/v1/cluster/tasks/{id}/detail"),
            id,
        )
        .await
        {
            Ok(mut value) => {
                annotate(&mut value, origin, *machine, *machine, "available");
                if let Some(route) = records.routes.values().next() {
                    value["failed_events"] = json!(route.failed_events);
                }
                return Ok(value);
            }
            Err(_) if records.route(id)?.is_some() => return cached_route(&records, id),
            Err(_) => {
                return Err(AppError::TaskUnavailable {
                    task: id,
                    machine: *machine,
                });
            }
        }
    }
    if let Some((machine, identity)) = records.identities.first_key_value() {
        return Ok(compact_identity(id, *machine, identity));
    }
    if records.route(id)?.is_some() {
        return cached_route(&records, id);
    }
    absent(id, &records)
}

/// Read a task log through the local socket and its execution owner
pub(super) async fn log(
    state: &AppState,
    id: TaskId,
    tail: Option<usize>,
) -> Result<Value, AppError> {
    if call(&state.store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .is_some()
    {
        let mut value =
            serde_json::to_value(crate::daemon::api::local_log(state, id, tail).await?)?;
        let machine = state.machine.identity.machine;
        let origin = local_identity_origin(state, id).await?.unwrap_or(machine);
        annotate(&mut value, origin, machine, machine, "available");
        return Ok(value);
    }
    let records = lookup(state, id).await?;
    if let Some((machine, _)) = records.executions.first_key_value() {
        let mut path = format!("/v1/cluster/tasks/{id}/log");
        if let Some(tail) = tail {
            path.push_str(&format!("?tail={tail}"));
        }
        let mut value = remote_read(state, &records, *machine, &path, id)
            .await
            .map_err(|_| AppError::TaskUnavailable {
                task: id,
                machine: *machine,
            })?;
        annotate(
            &mut value,
            origin_for(&records, *machine),
            *machine,
            *machine,
            "available",
        );
        return Ok(value);
    }
    if let Some((machine, identity)) = records.identities.first_key_value() {
        return match identity {
            IdentitySummary::Rejected { .. } => Err(AppError::TaskNotStarted { task: id }),
            IdentitySummary::Accepted { .. } => Err(AppError::TaskUnavailable {
                task: id,
                machine: *machine,
            }),
        };
    }
    if let Some((_, route)) = records.route(id)? {
        return match route.submission {
            SubmissionState::Rejected { .. } => Err(AppError::TaskNotStarted { task: id }),
            _ => Err(AppError::TaskUnavailable {
                task: id,
                machine: route.execution_machine,
            }),
        };
    }
    absent(id, &records)
}

async fn local_identity_origin(
    state: &AppState,
    id: TaskId,
) -> Result<Option<MachineId>, AppError> {
    let identity = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
        id,
        reply,
    })
    .await?;
    Ok(identity.map(|identity| match identity {
        ExecutorIdentity::Accepted(record) => record.origin_machine,
        ExecutorIdentity::Rejected(record) => record.origin_machine,
    }))
}

async fn local_failures(
    state: &AppState,
    id: TaskId,
) -> Result<Option<Vec<CallbackFailureSummary>>, AppError> {
    if call(&state.store, |reply| StoreMsg::OriginRoute { id, reply })
        .await?
        .is_none()
    {
        return Ok(None);
    }
    let failures = call(&state.store, |reply| StoreMsg::FailedInboxEvents {
        id,
        reply,
    })
    .await?;
    Ok(Some(
        failures
            .into_iter()
            .map(|failure| CallbackFailureSummary {
                seq: failure.seq,
                attempts: failure.attempts,
                error: "callback_delivery_failed".into(),
            })
            .collect(),
    ))
}

async fn lookup(state: &AppState, id: TaskId) -> Result<Records, AppError> {
    let local = state.machine.identity.machine;
    let mut records = Records::default();
    if let Some(route) = call(&state.store, |reply| StoreMsg::OriginRoute { id, reply }).await? {
        let failed_events = call(&state.store, |reply| StoreMsg::FailedInboxEvents {
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
        .collect();
        records.routes.insert(
            local,
            OriginSummary {
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
            },
        );
    }
    if let Some(identity) = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
        id,
        reply,
    })
    .await?
    {
        let summary = match identity {
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
        };
        records.identities.insert(local, summary);
    }
    let Some(fleet) = state.fleet.handle() else {
        records.check_conflicts(id)?;
        return Ok(records);
    };
    let mut seen = BTreeSet::from([local]);
    let mut jobs = JoinSet::new();
    for peer in fleet.peers().await {
        if seen.insert(peer.machine) {
            let fleet = fleet.clone();
            jobs.spawn(async move { read_peer(fleet, peer.machine, id, &[]).await });
        }
    }
    while let Some(result) = jobs.join_next().await {
        records.add(result.map_err(|err| AppError::Internal {
            message: format!("fleet inspection worker failed: {err}"),
        })?);
    }
    // a saved route can name an executor that discovery had not yet listed
    let targets: BTreeSet<_> = records
        .routes
        .values()
        .map(|route| route.execution_machine)
        .collect();
    for machine in targets {
        if seen.insert(machine) {
            let origin = records
                .routes
                .iter()
                .find(|(_, route)| route.execution_machine == machine)
                .map(|(owner, _)| *owner);
            let hints = match origin {
                Some(owner) if owner != local => origin_address_hints(fleet, owner, machine).await,
                _ => Vec::new(),
            };
            records.add(read_peer(fleet.clone(), machine, id, &hints).await);
        }
    }
    records.check_conflicts(id)?;
    Ok(records)
}

/// Resolve a task to its retained origin machine and original Codex thread
pub(super) async fn message_origin_route(
    state: &AppState,
    id: TaskId,
) -> Result<(MachineId, crate::domain::ThreadId), AppError> {
    let records = lookup(state, id).await?;
    if let Some((machine, route)) = records.route(id)? {
        return Ok((*machine, route.thread));
    }

    if !records.unchecked.is_empty() {
        return Err(AppError::ClusterLookupIncomplete {
            task: id,
            unchecked: records.unchecked.iter().copied().collect(),
        });
    }

    if !records.executions.is_empty() || !records.identities.is_empty() {
        return Err(AppError::RouteNotFound { task: id });
    }

    Err(AppError::TaskNotFound { id })
}

/// Resolve cancellation ownership from the same checked fleet snapshot as inspection
pub(super) async fn cancellation_owner(
    state: &AppState,
    id: TaskId,
) -> Result<CancellationOwner, AppError> {
    let records = lookup(state, id).await?;
    let target = if let Some((route_machine, route)) = records.route(id)? {
        cancellation_target(id, *route_machine, route)?
    } else if let Some((machine, identity)) = records.identities.first_key_value() {
        let (origin_machine, execution_machine) = identity.owners();
        if *machine != execution_machine {
            return Err(AppError::ClusterTaskConflict { task: id });
        }
        if records.unchecked.contains(&origin_machine) {
            return Err(AppError::ClusterLookupIncomplete {
                task: id,
                unchecked: vec![origin_machine],
            });
        }
        return Err(AppError::ClusterTaskConflict { task: id });
    } else {
        if !records.unchecked.is_empty() {
            return Err(AppError::ClusterLookupIncomplete {
                task: id,
                unchecked: records.unchecked.iter().copied().collect(),
            });
        }
        if let Some((machine, _)) = records.executions.first_key_value() {
            return Err(AppError::ClusterLookupIncomplete {
                task: id,
                unchecked: vec![*machine],
            });
        }
        return absent(id, &records);
    };

    let (origin, execution) = target.machines();
    let unchecked: Vec<_> = records
        .unchecked
        .iter()
        .filter(|machine| **machine != origin && **machine != execution)
        .copied()
        .collect();
    if !unchecked.is_empty() {
        return Err(AppError::ClusterLookupIncomplete {
            task: id,
            unchecked,
        });
    }

    Ok(target)
}

fn cancellation_target(
    task: TaskId,
    route_machine: MachineId,
    route: &OriginSummary,
) -> Result<CancellationOwner, AppError> {
    let route = CancellationRoute {
        task: route.task,
        request_id: route.request_id,
        origin_machine: route.origin_machine,
        execution_machine: route.execution_machine,
        submission: &route.submission,
    };
    CancellationOwner::from_route(task, route_machine, route)
        .map_err(|refusal| refusal.into_error(task))
}

async fn read_peer(
    fleet: FleetHandle,
    machine: MachineId,
    id: TaskId,
    hints: &[MachineAddress],
) -> PeerRecords {
    let mut result = PeerRecords {
        machine,
        execution: None,
        route: None,
        identity: None,
        incomplete: false,
        address: None,
    };
    let Ok(destination) = connect_or_hints(&fleet, machine, hints).await else {
        result.incomplete = true;
        return result;
    };
    result.address = Some(destination.address.clone());
    let client = ClusterClient::default();
    let query = format!("?api_version={API_VERSION}&destination_machine={machine}");
    let execution_path = format!("/v1/cluster/tasks/{id}{query}");
    let origin_path = format!("/v1/cluster/origin/tasks/{id}{query}");
    let identity_path = format!("/v1/cluster/identities/{id}{query}");
    let (execution, origin, identity) = tokio::join!(
        client.get(&destination.address, &execution_path),
        client.get(&destination.address, &origin_path),
        client.get(&destination.address, &identity_path),
    );
    match execution {
        Ok(response)
            if response.status == StatusCode::OK || response.status == StatusCode::NOT_FOUND =>
        {
            match serde_json::from_slice::<ExecutionBody>(&response.body) {
                Ok(body)
                    if body.api_version == API_VERSION
                        && body.execution.as_ref().is_none_or(|view| {
                            view.task == id && view.execution_machine == machine
                        })
                        && (response.status == StatusCode::OK) == body.execution.is_some() =>
                {
                    result.execution = body.execution
                }
                _ => result.incomplete = true,
            }
        }
        _ => result.incomplete = true,
    }
    match origin {
        Ok(response) if response.status == StatusCode::OK => {
            match serde_json::from_slice::<OriginBody>(&response.body) {
                Ok(body)
                    if body.api_version == API_VERSION
                        && body.origin.as_ref().is_none_or(|route| {
                            route.task == id && route.origin_machine == machine
                        }) =>
                {
                    result.route = body.origin
                }
                _ => result.incomplete = true,
            }
        }
        _ => result.incomplete = true,
    }
    match identity {
        Ok(response) if response.status == StatusCode::OK => {
            match serde_json::from_slice::<IdentitySummaryBody>(&response.body) {
                Ok(body)
                    if body.api_version == API_VERSION
                        && body.identity.as_ref().is_none_or(|identity| {
                            identity.task() == id && identity.owners().1 == machine
                        }) =>
                {
                    result.identity = body.identity
                }
                _ => result.incomplete = true,
            }
        }
        _ => result.incomplete = true,
    }
    result
}

async fn connect_or_hints(
    fleet: &FleetHandle,
    machine: MachineId,
    hints: &[MachineAddress],
) -> Result<VerifiedDestination, AppError> {
    if let Ok(destination) = fleet.connect(machine).await {
        return Ok(destination);
    }
    let client = ClusterClient::default();
    for address in hints {
        if let Ok(probed) = probe(&client, address, SUPPORTED_PROTOCOLS).await
            && let Ok(destination) = check_probed(address, machine, probed)
        {
            return Ok(destination);
        }
    }
    Err(AppError::MachineUnavailable {
        machine,
        message: "no verified executor address".into(),
    })
}

async fn origin_address_hints(
    fleet: &FleetHandle,
    origin: MachineId,
    executor: MachineId,
) -> Vec<MachineAddress> {
    let Ok(destination) = fleet.connect(origin).await else {
        return Vec::new();
    };
    let Ok(response) = ClusterClient::default()
        .get(&destination.address, "/v1/fleet/machines")
        .await
    else {
        return Vec::new();
    };
    let Ok(body) = serde_json::from_slice::<crate::daemon::fleet_api::MachinesBody>(&response.body)
    else {
        return Vec::new();
    };
    if response.status != StatusCode::OK
        || body.api_version != API_VERSION
        || body.local.machine != origin
    {
        return Vec::new();
    }
    body.machines
        .into_iter()
        .find(|peer| peer.machine == executor)
        .map_or_else(Vec::new, |peer| {
            peer.addresses
                .into_iter()
                .map(|ranked| ranked.address)
                .collect()
        })
}

async fn remote_read(
    state: &AppState,
    records: &Records,
    machine: MachineId,
    path: &str,
    id: TaskId,
) -> Result<Value, AppError> {
    let fleet = state
        .fleet
        .handle()
        .ok_or(AppError::TaskUnavailable { task: id, machine })?;
    let hints = records
        .addresses
        .get(&machine)
        .map_or(&[][..], std::slice::from_ref);
    let destination = connect_or_hints(fleet, machine, hints).await?;
    let separator = if path.contains('?') { '&' } else { '?' };
    let path = format!("{path}{separator}api_version={API_VERSION}&destination_machine={machine}");
    let response = ClusterClient::default()
        .get(&destination.address, &path)
        .await
        .map_err(|err| AppError::MachineUnavailable {
            machine,
            message: err.to_string(),
        })?;
    if response.status != StatusCode::OK {
        return Err(AppError::TaskUnavailable { task: id, machine });
    }
    let value: Value = serde_json::from_slice(&response.body)?;
    if value.get("id") != Some(&json!(id)) || value.get("api_version") != Some(&json!(API_VERSION))
    {
        return Err(AppError::TaskUnavailable { task: id, machine });
    }
    Ok(value)
}

fn origin_for(records: &Records, executor: MachineId) -> MachineId {
    records.routes.values().next().map_or_else(
        || {
            records
                .identities
                .get(&executor)
                .map_or(executor, |identity| identity.owners().0)
        },
        |route| route.origin_machine,
    )
}

fn annotate(
    value: &mut Value,
    origin: MachineId,
    executor: MachineId,
    found: MachineId,
    availability: &str,
) {
    if let Some(object) = value.as_object_mut() {
        object.insert("origin_machine".into(), json!(origin));
        object.insert("execution_machine".into(), json!(executor));
        object.insert("found_on".into(), json!(found));
        object.insert("availability".into(), json!(availability));
    }
}

fn cached_route(records: &Records, id: TaskId) -> Result<Value, AppError> {
    let (found, route) = records.route(id)?.ok_or(AppError::TaskNotFound { id })?;
    let status = match route.submission {
        SubmissionState::AcceptanceUnknown => None,
        SubmissionState::Accepted => route.last_execution_state,
        SubmissionState::Rejected { .. } => None,
        SubmissionState::Resource { .. } => route.last_execution_state,
        SubmissionState::ResourceAction {
            phase: ResourceActionRoutePhase::Accepted,
            ..
        } => route.last_execution_state,
        SubmissionState::ResourceAction { .. } => None,
        SubmissionState::ResourceBackground {
            phase: ResourceBackgroundRoutePhase::Accepted,
            ..
        } => route.last_execution_state,
        SubmissionState::ResourceBackground { .. } => None,
    };
    let mut value = json!({
        "api_version": API_VERSION,
        "id": id,
        "status": status,
        "submission": route.submission,
        "last_accepted_seq": route.last_accepted_seq,
        "last_settled_seq": route.last_settled_seq,
        "failed_events": route.failed_events,
        "last_update": route.last_updated_at,
    });
    let availability = if matches!(
        route.submission,
        SubmissionState::Rejected { .. }
            | SubmissionState::ResourceAction {
                phase: ResourceActionRoutePhase::Rejected { .. },
                ..
            }
            | SubmissionState::ResourceBackground {
                phase: ResourceBackgroundRoutePhase::Rejected { .. },
                ..
            }
    ) {
        "rejected"
    } else if records.unchecked.contains(&route.execution_machine) {
        "executor_unavailable"
    } else {
        "detail_unavailable"
    };
    annotate(
        &mut value,
        route.origin_machine,
        route.execution_machine,
        *found,
        availability,
    );
    Ok(value)
}

fn compact_identity(id: TaskId, machine: MachineId, identity: &IdentitySummary) -> Value {
    let (origin, executor) = identity.owners();
    let mut value = match identity {
        IdentitySummary::Accepted { status, .. } => json!({
            "api_version": API_VERSION, "id": id, "status": status,
            "detail_available": false,
        }),
        IdentitySummary::Rejected { reason, .. } => json!({
            "api_version": API_VERSION, "id": id, "status": null,
            "submission": { "type": "rejected", "reason": reason },
            "reason": reason, "detail_available": false,
        }),
    };
    annotate(&mut value, origin, executor, machine, "detail_unavailable");
    value
}

fn absent<T>(id: TaskId, records: &Records) -> Result<T, AppError> {
    if records.unchecked.is_empty() {
        Err(AppError::TaskNotFound { id })
    } else {
        Err(AppError::ClusterLookupIncomplete {
            task: id,
            unchecked: records.unchecked.iter().copied().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::ThreadId;
    use uuid::Uuid;

    use super::{Records, cancellation_target};
    use crate::cancellation::CancellationOwner;
    use crate::daemon::cluster::OriginSummary;
    use crate::domain::TaskId;
    use crate::error::AppError;
    use crate::machine::MachineId;
    use crate::resource::ResourceId;
    use crate::submission::{RequestId, ResourceRoutePhase, SubmissionState};

    fn origin_summary(
        task: TaskId,
        request_id: RequestId,
        origin_machine: MachineId,
        execution_machine: MachineId,
        submission: SubmissionState,
    ) -> OriginSummary {
        OriginSummary {
            task,
            request_id,
            origin_machine,
            execution_machine,
            thread: ThreadId(Uuid::now_v7()),
            submission,
            last_execution_state: None,
            last_updated_at: None,
            last_accepted_seq: 0,
            last_settled_seq: 0,
            failed_events: Vec::new(),
        }
    }

    #[test]
    fn resource_cancellation_target_keeps_its_full_identity() {
        let task = TaskId::new();
        let request_id = RequestId::new();
        let resource_id = ResourceId::new();
        let origin_machine = MachineId::new();
        let authority_machine = MachineId::new();
        let route = origin_summary(
            task,
            request_id,
            origin_machine,
            authority_machine,
            SubmissionState::Resource {
                resource: resource_id,
                phase: ResourceRoutePhase::Waiting,
            },
        );

        let CancellationOwner::Resource(target) =
            cancellation_target(task, origin_machine, &route).unwrap()
        else {
            panic!("resource routes must keep their authority-owned target type");
        };
        assert_eq!(target.request_id, request_id);
        assert_eq!(target.task_id, task);
        assert_eq!(target.resource_id, resource_id);
        assert_eq!(target.origin_machine, origin_machine);
        assert_eq!(target.authority_machine, authority_machine);
        assert_eq!(target.phase, ResourceRoutePhase::Waiting);
    }

    #[test]
    fn resource_cancellation_target_rejects_wrong_task_or_origin_identity() {
        let task = TaskId::new();
        let origin_machine = MachineId::new();
        let route = origin_summary(
            task,
            RequestId::new(),
            origin_machine,
            MachineId::new(),
            SubmissionState::Resource {
                resource: ResourceId::new(),
                phase: ResourceRoutePhase::AcceptanceUnknown,
            },
        );

        assert!(matches!(
            cancellation_target(TaskId::new(), origin_machine, &route),
            Err(AppError::ClusterTaskConflict { .. })
        ));
        assert!(matches!(
            cancellation_target(task, MachineId::new(), &route),
            Err(AppError::ClusterTaskConflict { .. })
        ));
    }

    #[test]
    fn rejected_execution_route_cannot_become_a_cancellation_target() {
        let task = TaskId::new();
        let origin_machine = MachineId::new();
        let route = origin_summary(
            task,
            RequestId::new(),
            origin_machine,
            MachineId::new(),
            SubmissionState::Rejected {
                reason: "abandoned_before_acceptance".into(),
            },
        );

        assert!(matches!(
            cancellation_target(task, origin_machine, &route),
            Err(AppError::TaskNotStarted { task: found }) if found == task
        ));
    }

    #[test]
    fn unresolved_background_launch_cannot_become_a_cancellation_target() {
        use crate::resource::background_launch::{
            BackgroundLaunchBinding, BackgroundSupervisorAssignment, ResourceBackgroundRejection,
        };
        use crate::resource::{AssignmentRevision, ResourceRevision, SupervisorAddress};
        use crate::submission::ResourceBackgroundRoutePhase;

        let task = TaskId::new();
        let origin_machine = MachineId::new();
        let authority_machine = MachineId::new();
        let binding = BackgroundLaunchBinding {
            assignment: BackgroundSupervisorAssignment {
                authority_machine,
                resource_id: ResourceId::new(),
                supervisor: SupervisorAddress {
                    machine: origin_machine,
                    thread: ThreadId(Uuid::now_v7()),
                },
                assignment_revision: AssignmentRevision::new(1),
            },
            expected_state_revision: ResourceRevision::new(1),
        };
        // a cancellation must never fence the fixed launch identity before acceptance
        for phase in [
            ResourceBackgroundRoutePhase::AcceptanceUnknown,
            ResourceBackgroundRoutePhase::Rejected {
                reason: ResourceBackgroundRejection::ResourceNotFound,
            },
        ] {
            let route = origin_summary(
                task,
                RequestId::new(),
                origin_machine,
                authority_machine,
                SubmissionState::ResourceBackground { binding, phase },
            );

            assert!(matches!(
                cancellation_target(task, origin_machine, &route),
                Err(AppError::ClusterTaskConflict { task: found }) if found == task
            ));
        }
    }

    #[test]
    fn duplicate_routes_with_different_resource_request_ids_conflict() {
        let task = TaskId::new();
        let origin_machine = MachineId::new();
        let authority_machine = MachineId::new();
        let resource_id = ResourceId::new();
        let submission = SubmissionState::Resource {
            resource: resource_id,
            phase: ResourceRoutePhase::Waiting,
        };
        let mut records = Records::default();
        records.routes.insert(
            MachineId::new(),
            origin_summary(
                task,
                RequestId::new(),
                origin_machine,
                authority_machine,
                submission.clone(),
            ),
        );
        records.routes.insert(
            MachineId::new(),
            origin_summary(
                task,
                RequestId::new(),
                origin_machine,
                authority_machine,
                submission,
            ),
        );

        assert!(matches!(
            records.route(task),
            Err(AppError::ClusterTaskConflict { .. })
        ));
    }
}
