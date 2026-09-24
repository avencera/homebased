//! Local message-send routing to the receiver on the selected Fleet machine

use crate::message::MessageReceipt;
use std::time::Duration;

use axum::http::StatusCode;

use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::directory::NameTarget;
use crate::fleet::http::{ClusterClient, DEFAULT_MAX_BODY};
use crate::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
use crate::machine::{MachineId, MachineName};
use crate::message::{
    MessageRequest, MessageResponse, MessageSendRequest, MessageSendResponse, MessageSource,
    MessageSourceSelector, MessageTarget, OutboundMessageBinding, Recipient,
};

const MESSAGE_PATH: &str = "/v1/cluster/messages";
// the receiver's queue retries, cleanup, and settlement take at most 104 seconds
const MESSAGE_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Resolve and deliver one message without changing its selected machine or message identity
pub(crate) async fn send(
    state: &AppState,
    request: MessageSendRequest,
) -> Result<MessageSendResponse, AppError> {
    request.validate()?;
    let binding = call(&state.store, |reply| StoreMsg::BeginOutboundMessage {
        request: request.clone(),
        reply,
    })
    .await?;
    let binding = match binding {
        bound @ OutboundMessageBinding::Bound { .. } => bound,
        OutboundMessageBinding::Resolving { request } => {
            let (destination, recipient) = resolve_target(state, &request.target).await?;
            call(&state.store, |reply| StoreMsg::BindOutboundMessage {
                request: request.clone(),
                destination_machine: destination,
                recipient: recipient.clone(),
                reply,
            })
            .await?
        }
    };
    let OutboundMessageBinding::Bound {
        request,
        destination_machine: destination,
        recipient,
    } = binding
    else {
        return Err(AppError::Internal {
            message: "outbound message route did not resolve".into(),
        });
    };
    let local = state.machine.identity.machine;
    let source = source_route(request.source.clone(), local);
    let protocol_version;
    let receipt = if destination == local {
        protocol_version = CLUSTER_PROTOCOL_VERSION.0;
        state
            .message_receiver
            .receive(
                state,
                receiver_request(&request, destination, source, &recipient, protocol_version),
            )
            .await?
    } else {
        let fleet = state
            .fleet
            .handle()
            .ok_or_else(|| AppError::MachineNotFound {
                machine: destination.to_string(),
            })?;
        let verified = fleet.connect(destination).await?;
        protocol_version = verified.protocol.0;
        let wire = receiver_request(&request, destination, source, &recipient, protocol_version);
        let response = ClusterClient::new(MESSAGE_REQUEST_TIMEOUT, DEFAULT_MAX_BODY)
            .post_json(&verified.address, MESSAGE_PATH, &wire)
            .await
            .map_err(|error| AppError::MessageOutcomeUnknown {
                id: request.message_id,
                machine: destination,
                message: error.to_string(),
            })?;
        decode_remote_receipt(response.status, &response.body, destination, &wire)?
    };

    validate_receipt(
        &receipt,
        &request,
        destination,
        protocol_version,
        &recipient,
    )?;
    Ok(MessageSendResponse {
        api_version: API_VERSION,
        message_id: request.message_id,
        destination_machine: destination,
        destination_thread: receipt.destination_thread,
        destination_cwd: receipt.destination_cwd.clone(),
        receipt,
    })
}

async fn resolve_target(
    state: &AppState,
    target: &MessageTarget,
) -> Result<(MachineId, Recipient), AppError> {
    match target {
        MessageTarget::Machine { machine, recipient } => {
            let destination = resolve_machine(state, machine).await?;
            Ok((destination, recipient.clone()))
        }
        MessageTarget::Task { task } => {
            let (machine, thread) = super::inspection::message_origin_route(state, *task).await?;
            Ok((machine, Recipient::Thread { thread }))
        }
    }
}

async fn resolve_machine(state: &AppState, selector: &str) -> Result<MachineId, AppError> {
    if let Ok(machine) = selector.parse::<MachineId>() {
        return Ok(machine);
    }

    let name = MachineName::parse(selector).map_err(|error| AppError::Usage {
        message: error.to_string(),
    })?;
    if let Some(fleet) = state.fleet.handle() {
        return match fleet.resolve_name(&name).await? {
            NameTarget::Local => Ok(state.machine.identity.machine),
            NameTarget::Peer(machine) => Ok(machine),
        };
    }
    if name == state.machine.name {
        return Ok(state.machine.identity.machine);
    }
    Err(AppError::MachineNotFound {
        machine: selector.to_string(),
    })
}

fn source_route(source: MessageSourceSelector, local: MachineId) -> MessageSource {
    match source {
        MessageSourceSelector::Thread { thread } => MessageSource::Thread {
            machine: local,
            thread,
        },
        MessageSourceSelector::Task { task } => MessageSource::Task {
            machine: local,
            task,
        },
    }
}

fn receiver_request(
    request: &MessageSendRequest,
    destination: MachineId,
    source: MessageSource,
    recipient: &Recipient,
    protocol_version: u32,
) -> MessageRequest {
    MessageRequest {
        api_version: request.api_version,
        protocol_version,
        message_id: request.message_id,
        destination_machine: destination,
        source,
        recipient: recipient.clone(),
        body: request.body.clone(),
        reply_to: request.reply_to,
        conversation_id: request.conversation_id,
    }
}

fn decode_remote_receipt(
    status: StatusCode,
    body: &[u8],
    destination: MachineId,
    request: &MessageRequest,
) -> Result<MessageReceipt, AppError> {
    if !status.is_success() {
        return Err(crate::client::map_error(status, body));
    }
    let response: MessageResponse = serde_json::from_slice(body).map_err(|error| {
        unknown_outcome(
            request,
            destination,
            format!("receiver returned an invalid receipt: {error}"),
        )
    })?;
    if response.api_version != API_VERSION
        || response.protocol_version != request.protocol_version
        || response.receipt.api_version != API_VERSION
        || response.receipt.protocol_version != request.protocol_version
        || response.receipt.message_id != request.message_id
        || response.receipt.destination_thread.0.is_nil()
        || !response.receipt.destination_cwd.is_absolute()
    {
        return Err(unknown_outcome(
            request,
            destination,
            "receiver returned a receipt with a different version or message identity".into(),
        ));
    }
    if response.destination_machine != destination {
        return Err(AppError::MachineIdentityMismatch {
            expected: destination,
            found: Some(response.destination_machine),
        });
    }
    if let Recipient::Thread { thread } = &request.recipient
        && response.receipt.destination_thread != *thread
    {
        return Err(unknown_outcome(
            request,
            destination,
            "receiver returned a receipt for a different thread".into(),
        ));
    }
    if let Recipient::Cwd { cwd } = &request.recipient
        && cwd.is_absolute()
        && &response.receipt.destination_cwd != cwd
    {
        return Err(unknown_outcome(
            request,
            destination,
            "receiver returned a receipt for a different working directory".into(),
        ));
    }
    Ok(response.receipt)
}

fn validate_receipt(
    receipt: &MessageReceipt,
    request: &MessageSendRequest,
    destination: MachineId,
    protocol_version: u32,
    recipient: &Recipient,
) -> Result<(), AppError> {
    let recipient_mismatch = match recipient {
        Recipient::Thread { thread } => receipt.destination_thread != *thread,
        Recipient::Cwd { cwd } => cwd.is_absolute() && receipt.destination_cwd != *cwd,
    };
    if receipt.api_version != API_VERSION
        || receipt.protocol_version != protocol_version
        || receipt.message_id != request.message_id
        || receipt.destination_thread.0.is_nil()
        || !receipt.destination_cwd.is_absolute()
        || recipient_mismatch
    {
        return Err(AppError::MessageOutcomeUnknown {
            id: request.message_id,
            machine: destination,
            message: "receiver returned a receipt that does not match the request".into(),
        });
    }
    Ok(())
}

fn unknown_outcome(request: &MessageRequest, machine: MachineId, message: String) -> AppError {
    AppError::MessageOutcomeUnknown {
        id: request.message_id,
        machine,
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::decode_remote_receipt;
    use crate::domain::{API_VERSION, ThreadId};
    use crate::error::AppError;
    use crate::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
    use crate::machine::MachineId;
    use crate::message::{
        MessageId, MessageReceipt, MessageRequest, MessageResponse, MessageSource, Recipient,
    };
    use axum::http::StatusCode;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn rejects_a_success_response_from_another_machine_identity() {
        let expected = MachineId::new();
        let found = MachineId::new();
        let message_id = MessageId::new();
        let thread = ThreadId(Uuid::now_v7());
        let request = MessageRequest {
            api_version: API_VERSION,
            protocol_version: CLUSTER_PROTOCOL_VERSION.0,
            message_id,
            destination_machine: expected,
            source: MessageSource::Thread {
                machine: MachineId::new(),
                thread: ThreadId(Uuid::now_v7()),
            },
            recipient: Recipient::Thread { thread },
            body: "Review this change".into(),
            reply_to: None,
            conversation_id: message_id.as_uuid(),
        };
        let response = MessageResponse {
            api_version: API_VERSION,
            protocol_version: CLUSTER_PROTOCOL_VERSION.0,
            destination_machine: found,
            receipt: MessageReceipt {
                api_version: API_VERSION,
                protocol_version: CLUSTER_PROTOCOL_VERSION.0,
                message_id,
                destination_thread: thread,
                destination_cwd: "/repo".into(),
                delivered_at: chrono::Utc::now(),
            },
        };
        let body = serde_json::to_vec(&response).unwrap();
        let error = decode_remote_receipt(StatusCode::OK, &body, expected, &request).unwrap_err();
        assert!(matches!(
            error,
            AppError::MachineIdentityMismatch {
                expected: actual_expected,
                found: Some(actual_found),
            } if actual_expected == expected && actual_found == found
        ));
    }

    #[test]
    fn remote_response_requires_a_versioned_receipt() {
        let machine = MachineId::new();
        let message_id = MessageId::new();
        let request = MessageRequest {
            api_version: API_VERSION,
            protocol_version: CLUSTER_PROTOCOL_VERSION.0,
            message_id,
            destination_machine: machine,
            source: MessageSource::Thread {
                machine: MachineId::new(),
                thread: ThreadId(Uuid::now_v7()),
            },
            recipient: Recipient::Thread {
                thread: ThreadId(Uuid::now_v7()),
            },
            body: "Review this change".into(),
            reply_to: None,
            conversation_id: message_id.as_uuid(),
        };
        let error = decode_remote_receipt(
            StatusCode::OK,
            br#"{"api_version":1,"protocol_version":1,"destination_machine":"bad"}"#,
            machine,
            &request,
        )
        .unwrap_err();
        assert!(matches!(error, AppError::MessageOutcomeUnknown { .. }));
        assert_eq!(
            error.input()["message_id"],
            json!(request.message_id.to_string())
        );
    }
}
