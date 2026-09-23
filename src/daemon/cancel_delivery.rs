//! Restart-safe requester delivery and executor application

use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use tracing::warn;
use uuid::Uuid;

use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::cluster::{CancelBody, CancelExecution};
use crate::cancellation::{
    CancellationOwner, CancellationReceipt, CancellationRequest, CancellationTarget,
    ExecutorCancelState, ResourceCancellationRequestIdentity,
};
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::http::ClusterClient;
use crate::fleet::protocol::ClusterProtocolVersion;
use crate::store::CancelResult;
use crate::submission::{
    ExecutorIdentity, ResourceCancellationOutcome, ResourceCancellationReceipt,
};

struct Retry {
    next: Instant,
    delay: Duration,
}

impl Retry {
    fn ready(&self) -> bool {
        Instant::now() >= self.next
    }

    fn failed(&mut self) {
        self.next = Instant::now() + self.delay;
        self.delay = (self.delay * 2).min(Duration::from_secs(30));
    }
}

/// Scan durable work on startup and retry transient failures with bounded backoff
pub(super) async fn run(state: AppState) {
    let mut retries: HashMap<Uuid, Retry> = HashMap::new();
    loop {
        match call(&state.store, |reply| {
            StoreMsg::PendingExecutorCancellations { reply }
        })
        .await
        {
            Ok(pending) => {
                for receipt in pending {
                    if let Err(error) = apply_executor(&state, receipt).await {
                        warn!("executor cancellation application: {error}");
                    }
                }
            }
            Err(error) => warn!("executor cancellation scan: {error}"),
        }
        match call(&state.store, |reply| {
            StoreMsg::PendingCancellationRequests { reply }
        })
        .await
        {
            Ok(pending) => {
                for request in pending {
                    let retry = retries.entry(request.cancellation).or_insert(Retry {
                        next: Instant::now(),
                        delay: Duration::from_secs(1),
                    });
                    if !retry.ready() {
                        continue;
                    }
                    match deliver(&state, &request).await {
                        Ok(()) => {
                            retries.remove(&request.cancellation);
                        }
                        Err(error) => {
                            warn!(task = %request.task, "cancellation delivery: {error}");
                            retry.failed();
                        }
                    }
                }
            }
            Err(error) => warn!("requester cancellation scan: {error}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn deliver(state: &AppState, request: &CancellationRequest) -> Result<(), AppError> {
    match &request.target {
        CancellationTarget::LegacyExecution => {
            verify_legacy_cancellation_owner(state, request).await?;
        }
        CancellationTarget::Execution { .. } => {}
        CancellationTarget::Resource(target) => {
            if target.task_id != request.task
                || target.origin_machine != request.origin_machine
                || target.authority_machine != request.execution_machine
            {
                return Err(AppError::ClusterTaskConflict { task: request.task });
            }
            return deliver_resource(state, request).await;
        }
    }

    let receipt = if request.execution_machine == state.machine.identity.machine {
        call(&state.store, |reply| StoreMsg::ReceiveCancellation {
            request: request.identity(),
            reply,
        })
        .await?
    } else {
        let fleet = state.fleet.handle().ok_or(AppError::MachineUnavailable {
            machine: request.execution_machine,
            message: "fleet is disabled".into(),
        })?;
        let destination = fleet.connect(request.execution_machine).await?;
        let body = CancelExecution {
            api_version: API_VERSION,
            protocol_version: destination.protocol.0,
            request: request.identity(),
            target: (destination.protocol == crate::fleet::protocol::CLUSTER_PROTOCOL_VERSION)
                .then(|| request.target.clone()),
        };
        let response = ClusterClient::default()
            .post_json(&destination.address, "/v1/cluster/executions/cancel", &body)
            .await
            .map_err(|error| AppError::MachineUnavailable {
                machine: request.execution_machine,
                message: error.to_string(),
            })?;
        if response.status != StatusCode::OK {
            return Err(AppError::MachineUnavailable {
                machine: request.execution_machine,
                message: format!("cancellation response status {}", response.status),
            });
        }
        let value: serde_json::Value = serde_json::from_slice(&response.body).map_err(|error| {
            AppError::MachineUnavailable {
                machine: request.execution_machine,
                message: format!("invalid cancellation acknowledgement: {error}"),
            }
        })?;
        decode_acknowledgement(value, request, destination.protocol)?
    };
    call(&state.store, |reply| StoreMsg::AcknowledgeCancellation {
        receipt,
        reply,
    })
    .await?;
    Ok(())
}

async fn deliver_resource(state: &AppState, request: &CancellationRequest) -> Result<(), AppError> {
    let identity = request
        .resource_identity()
        .ok_or(AppError::ClusterTaskConflict { task: request.task })?;
    if identity.requester_machine != identity.origin_machine
        || identity.authority_machine != request.execution_machine
    {
        return Err(AppError::ClusterTaskConflict { task: request.task });
    }
    let receipt = if identity.authority_machine == state.machine.identity.machine {
        super::cluster::process_resource_cancellation(state, identity.clone()).await?
    } else {
        deliver_remote_resource(state, &identity).await?
    };
    verify_resource_receipt(&identity, &receipt)?;
    if matches!(
        &receipt.outcome,
        ResourceCancellationOutcome::PreventedBeforeAcceptance
            | ResourceCancellationOutcome::CancelledBeforeLaunch
    ) {
        call(&state.store, |reply| {
            StoreMsg::CancelResourceRouteBeforeLaunch {
                receipt: receipt.clone(),
                reply,
            }
        })
        .await?;
    }
    call(&state.store, |reply| {
        StoreMsg::AcknowledgeResourceCancellation { receipt, reply }
    })
    .await?;
    Ok(())
}

async fn deliver_remote_resource(
    state: &AppState,
    identity: &ResourceCancellationRequestIdentity,
) -> Result<ResourceCancellationReceipt, AppError> {
    let fleet = state.fleet.handle().ok_or(AppError::MachineUnavailable {
        machine: identity.authority_machine,
        message: "fleet is disabled".into(),
    })?;
    let destination = fleet.connect(identity.authority_machine).await?;
    let body = super::cluster::CancelResourceRequest {
        api_version: API_VERSION,
        protocol_version: destination.protocol.0,
        destination_machine: identity.authority_machine,
        request: identity.clone(),
    };
    let response = ClusterClient::default()
        .post_json(
            &destination.address,
            "/v1/cluster/resource-requests/cancel",
            &body,
        )
        .await
        .map_err(|error| AppError::MachineUnavailable {
            machine: identity.authority_machine,
            message: error.to_string(),
        })?;
    if response.status != StatusCode::OK {
        return Err(AppError::MachineUnavailable {
            machine: identity.authority_machine,
            message: format!("resource cancellation response status {}", response.status),
        });
    }
    let value: serde_json::Value =
        serde_json::from_slice(&response.body).map_err(|error| AppError::MachineUnavailable {
            machine: identity.authority_machine,
            message: format!("invalid resource cancellation acknowledgement: {error}"),
        })?;
    if value.get("api_version").and_then(serde_json::Value::as_u64) != Some(u64::from(API_VERSION))
    {
        return Err(AppError::MachineUnavailable {
            machine: identity.authority_machine,
            message: "resource cancellation acknowledgement uses an unsupported API version".into(),
        });
    }
    let body: super::cluster::CancelResourceBody =
        serde_json::from_value(value).map_err(|error| AppError::MachineUnavailable {
            machine: identity.authority_machine,
            message: format!("invalid resource cancellation acknowledgement: {error}"),
        })?;
    if body.api_version != API_VERSION
        || body.protocol_version != destination.protocol.0
        || body.destination_machine != identity.authority_machine
    {
        return Err(AppError::ClusterTaskConflict {
            task: identity.task,
        });
    }
    verify_resource_receipt(identity, &body.receipt)?;
    Ok(body.receipt)
}

fn verify_resource_receipt(
    identity: &ResourceCancellationRequestIdentity,
    receipt: &ResourceCancellationReceipt,
) -> Result<(), AppError> {
    if receipt.cancellation != identity.cancellation
        || receipt.requester_machine != identity.requester_machine
        || receipt.request != identity.request
        || receipt.task != identity.task
        || receipt.origin_machine != identity.origin_machine
        || receipt.authority_machine != identity.authority_machine
        || receipt.resource != identity.resource
        || receipt.target_phase != identity.target_phase
    {
        return Err(AppError::ClusterTaskConflict {
            task: identity.task,
        });
    }
    Ok(())
}

async fn verify_legacy_cancellation_owner(
    state: &AppState,
    request: &CancellationRequest,
) -> Result<(), AppError> {
    let owner = crate::daemon::inspection::cancellation_owner(state, request.task).await?;
    validate_legacy_cancellation_owner(request, owner)
}

fn validate_legacy_cancellation_owner(
    request: &CancellationRequest,
    owner: CancellationOwner,
) -> Result<(), AppError> {
    match owner {
        CancellationOwner::Execution(target)
            if target.task == request.task
                && target.origin_machine == request.origin_machine
                && target.execution_machine == request.execution_machine =>
        {
            Ok(())
        }
        CancellationOwner::Resource(target)
            if target.task_id == request.task
                && target.origin_machine == request.origin_machine
                && target.authority_machine == request.execution_machine =>
        {
            Err(AppError::ResourceCancellationUnavailable { task: request.task })
        }
        _ => Err(AppError::ClusterTaskConflict { task: request.task }),
    }
}

fn decode_acknowledgement(
    value: serde_json::Value,
    request: &CancellationRequest,
    protocol: ClusterProtocolVersion,
) -> Result<CancellationReceipt, AppError> {
    if value.get("api_version").and_then(serde_json::Value::as_u64) != Some(u64::from(API_VERSION))
    {
        return Err(AppError::MachineUnavailable {
            machine: request.execution_machine,
            message: "cancellation acknowledgement uses an unsupported API version".into(),
        });
    }
    let body: CancelBody =
        serde_json::from_value(value).map_err(|error| AppError::MachineUnavailable {
            machine: request.execution_machine,
            message: format!("invalid cancellation acknowledgement: {error}"),
        })?;
    if body.protocol_version != protocol.0
        || body.api_version != API_VERSION
        || body.receipt.request != request.identity()
    {
        return Err(AppError::ClusterTaskConflict { task: request.task });
    }
    Ok(body.receipt)
}

pub(super) async fn apply_executor(
    state: &AppState,
    receipt: CancellationReceipt,
) -> Result<CancellationReceipt, AppError> {
    let task = receipt.request.task;
    let result = call(&state.supervisor, |reply| SupervisorMsg::Cancel {
        id: task,
        reply,
    })
    .await;
    let outcome = match result {
        Ok(CancelResult::AlreadyTerminal(row)) => ExecutorCancelState::AlreadyTerminal {
            status: row.status(),
        },
        Ok(CancelResult::CancelledQueued(row)) => ExecutorCancelState::Applied {
            status: row.status(),
        },
        Ok(CancelResult::SignalWorker(row)) => ExecutorCancelState::Applied {
            status: row.status(),
        },
        Err(AppError::TaskNotFound { .. }) => {
            let identity = call(&state.store, |reply| StoreMsg::ExecutorIdentity {
                id: task,
                reply,
            })
            .await?;
            match identity {
                Some(ExecutorIdentity::Accepted(record)) if record.state.is_terminal() => {
                    ExecutorCancelState::AlreadyTerminal {
                        status: record.state,
                    }
                }
                _ => {
                    return Err(AppError::TaskUnavailable {
                        task,
                        machine: receipt.request.execution_machine,
                    });
                }
            }
        }
        Err(error) => return Err(error),
    };
    call(&state.store, |reply| StoreMsg::FinishExecutorCancellation {
        cancellation: receipt.request.cancellation,
        state: outcome,
        reply,
    })
    .await
}

/// Render a retained delivery result for the local socket
pub(super) fn response(request: &CancellationRequest) -> serde_json::Value {
    use crate::cancellation::CancellationDelivery;
    let delivery = match &request.delivery {
        CancellationDelivery::Pending => serde_json::json!({ "state": "pending" }),
        CancellationDelivery::Delivered { result } => serde_json::json!({
            "state": "delivered", "executor": result,
        }),
        CancellationDelivery::ResourceDelivered { result } => serde_json::json!({
            "state": "resource_delivered", "resource": result,
        }),
    };
    serde_json::json!({
        "api_version": crate::domain::API_VERSION,
        "id": request.task,
        "requester_machine": request.requester_machine,
        "cancellation": request.cancellation,
        "origin_machine": request.origin_machine,
        "execution_machine": request.execution_machine,
        "delivery": delivery,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cancellation::{
        CancellationDelivery, ExecutionCancellationTarget, ResourceCancellationTarget,
    };
    use crate::domain::TaskId;
    use crate::machine::MachineId;
    use crate::resource::ResourceId;
    use crate::submission::{RequestId, ResourceRoutePhase};

    fn request() -> CancellationRequest {
        CancellationRequest {
            requester_machine: MachineId::new(),
            cancellation: Uuid::now_v7(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            execution_machine: MachineId::new(),
            target: CancellationTarget::LegacyExecution,
            delivery: CancellationDelivery::Pending,
        }
    }

    fn acknowledgement(
        request: &CancellationRequest,
        protocol: ClusterProtocolVersion,
    ) -> serde_json::Value {
        serde_json::to_value(CancelBody {
            api_version: API_VERSION,
            protocol_version: protocol.0,
            receipt: CancellationReceipt {
                request: request.identity(),
                state: ExecutorCancelState::PendingApplication,
            },
        })
        .unwrap()
    }

    #[test]
    fn cancellation_acknowledgement_uses_the_selected_protocol_version() {
        let request = request();
        let selected = ClusterProtocolVersion(2);

        assert_eq!(
            decode_acknowledgement(acknowledgement(&request, selected), &request, selected)
                .unwrap()
                .request,
            request.identity()
        );
        assert!(matches!(
            decode_acknowledgement(
                acknowledgement(&request, ClusterProtocolVersion(1)),
                &request,
                selected
            ),
            Err(AppError::ClusterTaskConflict { task }) if task == request.task
        ));
    }

    #[test]
    fn legacy_executor_intent_must_match_a_proven_ordinary_owner() {
        let request = request();
        let target = CancellationOwner::Execution(ExecutionCancellationTarget {
            request_id: Some(RequestId::new()),
            task: request.task,
            origin_machine: request.origin_machine,
            execution_machine: request.execution_machine,
        });
        assert!(validate_legacy_cancellation_owner(&request, target).is_ok());

        let wrong_owner = CancellationOwner::Execution(ExecutionCancellationTarget {
            request_id: Some(RequestId::new()),
            task: request.task,
            origin_machine: request.origin_machine,
            execution_machine: MachineId::new(),
        });
        assert!(matches!(
            validate_legacy_cancellation_owner(&request, wrong_owner),
            Err(AppError::ClusterTaskConflict { task }) if task == request.task
        ));
    }

    #[test]
    fn legacy_executor_intent_refuses_a_resource_route() {
        let request = request();
        let target = CancellationOwner::Resource(ResourceCancellationTarget {
            request_id: RequestId::new(),
            task_id: request.task,
            resource_id: ResourceId::new(),
            origin_machine: request.origin_machine,
            authority_machine: request.execution_machine,
            phase: ResourceRoutePhase::AcceptanceUnknown,
        });

        assert!(matches!(
            validate_legacy_cancellation_owner(&request, target),
            Err(AppError::ResourceCancellationUnavailable { task }) if task == request.task
        ));
    }
}
