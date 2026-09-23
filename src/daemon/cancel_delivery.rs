//! Restart-safe requester delivery and executor application

use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use tracing::warn;
use uuid::Uuid;

use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::cluster::{CancelBody, CancelExecution};
use crate::cancellation::{CancellationReceipt, CancellationRequest, ExecutorCancelState};
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::http::ClusterClient;
use crate::fleet::protocol::ClusterProtocolVersion;
use crate::store::CancelResult;
use crate::submission::ExecutorIdentity;

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
    use crate::cancellation::CancellationDelivery;
    use crate::domain::TaskId;
    use crate::machine::MachineId;

    fn request() -> CancellationRequest {
        CancellationRequest {
            requester_machine: MachineId::new(),
            cancellation: Uuid::now_v7(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            execution_machine: MachineId::new(),
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
}
