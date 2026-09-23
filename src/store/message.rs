//! Durable direct-message attempts and success receipts.

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::Store;
use crate::error::AppError;
use crate::message::{
    MessageAttempt, MessageDelivery, MessageId, MessageReceipt, MessageSendRequest,
    OutboundMessageBinding, Recipient,
};

fn encode<T: serde::Serialize>(value: &T) -> Result<String, AppError> {
    Ok(serde_json::to_string(value)?)
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, AppError> {
    Ok(serde_json::from_str(value)?)
}

impl Store {
    /// Reserve a message UUID and reject changed content before route lookup
    pub fn begin_outbound_message(
        &mut self,
        request: &MessageSendRequest,
    ) -> Result<OutboundMessageBinding, AppError> {
        request.validate()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let saved: Option<String> = tx
            .query_row(
                "SELECT binding_json FROM outbound_message_bindings WHERE message_id=?1",
                [request.message_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(saved) = saved {
            let existing: OutboundMessageBinding = decode(&saved)?;
            validate_saved_binding(&existing, request.message_id)?;
            if existing.request() != request {
                return Err(AppError::MessageConflict {
                    id: request.message_id,
                });
            }
            return Ok(existing);
        }

        let binding = OutboundMessageBinding::Resolving {
            request: request.clone(),
        };
        tx.execute(
            "INSERT INTO outbound_message_bindings (message_id, binding_json) VALUES (?1, ?2)",
            params![request.message_id.to_string(), encode(&binding)?,],
        )?;
        tx.commit()?;
        Ok(binding)
    }

    /// Set the fixed receiver and recipient once destination lookup succeeds
    pub fn bind_outbound_message(
        &mut self,
        request: &MessageSendRequest,
        destination_machine: crate::machine::MachineId,
        recipient: &Recipient,
    ) -> Result<OutboundMessageBinding, AppError> {
        let binding = OutboundMessageBinding::Bound {
            request: request.clone(),
            destination_machine,
            recipient: recipient.clone(),
        };
        binding.validate()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let saved: Option<String> = tx
            .query_row(
                "SELECT binding_json FROM outbound_message_bindings WHERE message_id=?1",
                [request.message_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(saved) = saved else {
            return Err(AppError::Internal {
                message: "outbound message request was not reserved".into(),
            });
        };
        let existing: OutboundMessageBinding = decode(&saved)?;
        validate_saved_binding(&existing, request.message_id)?;
        if existing.request() != request {
            return Err(AppError::MessageConflict {
                id: request.message_id,
            });
        }
        if matches!(&existing, OutboundMessageBinding::Bound { .. }) {
            return Ok(existing);
        }
        tx.execute(
            "UPDATE outbound_message_bindings SET binding_json=?2 WHERE message_id=?1",
            params![request.message_id.to_string(), encode(&binding)?,],
        )?;
        tx.commit()?;
        Ok(binding)
    }

    /// Read the saved request binding and committed receipt for one message UUID.
    pub fn message_delivery(&self, id: MessageId) -> Result<MessageDelivery, AppError> {
        let attempt_json: Option<String> = self
            .conn
            .query_row(
                "SELECT attempt_json FROM message_attempts WHERE message_id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let receipt_json: Option<String> = self
            .conn
            .query_row(
                "SELECT receipt_json FROM message_receipts WHERE message_id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let attempt: Option<MessageAttempt> = attempt_json.as_deref().map(decode).transpose()?;
        let receipt: Option<MessageReceipt> = receipt_json.as_deref().map(decode).transpose()?;
        match (&attempt, &receipt) {
            (None, Some(_)) => {
                return Err(AppError::Internal {
                    message: "message receipt has no saved attempt".into(),
                });
            }
            (Some(attempt), Some(receipt))
                if attempt.request.message_id != id
                    || receipt.message_id != id
                    || receipt.api_version != crate::domain::API_VERSION
                    || receipt.protocol_version != attempt.request.protocol_version
                    || receipt.destination_thread != attempt.destination_thread
                    || receipt.destination_cwd != attempt.destination_cwd =>
            {
                return Err(AppError::Internal {
                    message: "message receipt does not match its saved attempt".into(),
                });
            }
            _ => {}
        }
        Ok(MessageDelivery { attempt, receipt })
    }

    /// Bind one message UUID to immutable request content and its resolved local destination.
    pub fn bind_message_attempt(
        &mut self,
        attempt: &MessageAttempt,
    ) -> Result<MessageAttempt, AppError> {
        attempt.request.validate()?;
        if !attempt.destination_cwd.is_absolute() {
            return Err(AppError::MessageInvalid {
                message: "resolved message cwd must be absolute".into(),
            });
        }

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let saved: Option<String> = tx
            .query_row(
                "SELECT attempt_json FROM message_attempts WHERE message_id=?1",
                [attempt.request.message_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(saved) = saved {
            let mut existing: MessageAttempt = decode(&saved)?;
            if existing.request.identity() != attempt.request.identity() {
                return Err(AppError::MessageConflict {
                    id: attempt.request.message_id,
                });
            }
            if existing.request.protocol_version == attempt.request.protocol_version {
                return Ok(existing);
            }

            existing.request = attempt.request.clone();
            tx.execute(
                "UPDATE message_attempts SET attempt_json=?2 WHERE message_id=?1",
                params![attempt.request.message_id.to_string(), encode(&existing)?,],
            )?;
            tx.commit()?;
            return Ok(existing);
        }
        tx.execute(
            "INSERT INTO message_attempts (message_id, attempt_json) VALUES (?1, ?2)",
            params![attempt.request.message_id.to_string(), encode(attempt)?,],
        )?;
        tx.commit()?;
        Ok(attempt.clone())
    }

    /// Commit a success receipt after the saved attempt's queue command succeeds.
    pub fn commit_message_receipt(
        &mut self,
        receipt: &MessageReceipt,
    ) -> Result<MessageReceipt, AppError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let attempt_json: Option<String> = tx
            .query_row(
                "SELECT attempt_json FROM message_attempts WHERE message_id=?1",
                [receipt.message_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(attempt_json) = attempt_json else {
            return Err(AppError::Internal {
                message: "cannot commit receipt without a saved message attempt".into(),
            });
        };
        let attempt: MessageAttempt = decode(&attempt_json)?;
        if receipt.message_id != attempt.request.message_id
            || receipt.api_version != crate::domain::API_VERSION
            || receipt.protocol_version != attempt.request.protocol_version
            || receipt.destination_thread != attempt.destination_thread
            || receipt.destination_cwd != attempt.destination_cwd
        {
            return Err(AppError::Internal {
                message: "message receipt does not match its saved attempt".into(),
            });
        }
        let saved: Option<String> = tx
            .query_row(
                "SELECT receipt_json FROM message_receipts WHERE message_id=?1",
                [receipt.message_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(saved) = saved {
            return decode(&saved);
        }
        tx.execute(
            "INSERT INTO message_receipts (message_id, receipt_json) VALUES (?1, ?2)",
            params![receipt.message_id.to_string(), encode(receipt)?],
        )?;
        tx.commit()?;
        Ok(receipt.clone())
    }
}

fn validate_saved_binding(binding: &OutboundMessageBinding, id: MessageId) -> Result<(), AppError> {
    if binding.request().message_id != id || binding.validate().is_err() {
        return Err(AppError::Internal {
            message: "saved outbound message binding is invalid".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::domain::{API_VERSION, ThreadId};
    use crate::machine::MachineId;
    use crate::message::{MessageRequest, MessageSource, MessageSourceSelector, MessageTarget};

    fn request() -> MessageSendRequest {
        let message_id = MessageId::new();
        MessageSendRequest {
            api_version: API_VERSION,
            message_id,
            target: MessageTarget::Machine {
                machine: "receiver".into(),
                recipient: Recipient::Thread {
                    thread: ThreadId(Uuid::now_v7()),
                },
            },
            source: MessageSourceSelector::Thread {
                thread: ThreadId(Uuid::now_v7()),
            },
            body: "Review the cluster change".into(),
            reply_to: None,
            conversation_id: message_id.as_uuid(),
        }
    }

    fn requested_recipient(request: &MessageSendRequest) -> Recipient {
        match &request.target {
            MessageTarget::Machine { recipient, .. } => recipient.clone(),
            MessageTarget::Task { .. } => unreachable!(),
        }
    }

    fn receiver_request(protocol_version: u32) -> MessageRequest {
        let message_id = MessageId::new();
        MessageRequest {
            api_version: API_VERSION,
            protocol_version,
            message_id,
            destination_machine: MachineId::new(),
            source: MessageSource::Thread {
                machine: MachineId::new(),
                thread: ThreadId(Uuid::now_v7()),
            },
            recipient: Recipient::Thread {
                thread: ThreadId(Uuid::now_v7()),
            },
            body: "Review the message protocol retry".into(),
            reply_to: None,
            conversation_id: message_id.as_uuid(),
        }
    }

    #[test]
    fn outbound_route_stays_fixed_after_restart_and_name_rebinding() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("homebased.sqlite");
        let request = request();
        let recipient = requested_recipient(&request);
        let original_machine = MachineId::new();
        let replacement_machine = MachineId::new();

        let mut store = Store::open(&path).unwrap();
        assert!(matches!(
            store.begin_outbound_message(&request).unwrap(),
            OutboundMessageBinding::Resolving { .. }
        ));
        let original = store
            .bind_outbound_message(&request, original_machine, &recipient)
            .unwrap();
        drop(store);

        let mut reopened = Store::open(&path).unwrap();
        let retry = reopened.begin_outbound_message(&request).unwrap();
        assert_eq!(retry, original);
        let attempted_rebind = reopened
            .bind_outbound_message(&request, replacement_machine, &recipient)
            .unwrap();
        assert_eq!(attempted_rebind, original);
        assert_eq!(
            attempted_rebind.route(),
            Some((original_machine, &recipient))
        );
    }

    #[test]
    fn changed_request_conflicts_before_destination_binding() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("homebased.sqlite");
        let mut store = Store::open(&path).unwrap();
        let request = request();
        store.begin_outbound_message(&request).unwrap();

        let mut changed = request.clone();
        changed.body.push_str(" changed");
        assert!(matches!(
            store.begin_outbound_message(&changed),
            Err(AppError::MessageConflict { id }) if id == request.message_id
        ));
    }

    #[test]
    fn protocol_retry_updates_attempt_and_returns_the_current_receipt_version() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("homebased.sqlite");
        let mut store = Store::open(&path).unwrap();
        let request = receiver_request(1);
        let attempt = MessageAttempt {
            request: request.clone(),
            destination_thread: match &request.recipient {
                Recipient::Thread { thread } => *thread,
                Recipient::Cwd { .. } => unreachable!(),
            },
            destination_cwd: PathBuf::from("/tmp"),
        };
        store.bind_message_attempt(&attempt).unwrap();

        let mut retry = attempt.clone();
        retry.request.protocol_version = 2;
        let updated = store.bind_message_attempt(&retry).unwrap();
        assert_eq!(updated.request.identity(), attempt.request.identity());
        assert_eq!(updated.request.protocol_version, 2);

        let committed = store
            .commit_message_receipt(&MessageReceipt {
                api_version: API_VERSION,
                protocol_version: 2,
                message_id: request.message_id,
                destination_thread: attempt.destination_thread,
                destination_cwd: attempt.destination_cwd.clone(),
                delivered_at: chrono::Utc::now(),
            })
            .unwrap();
        assert_eq!(committed.protocol_version, 2);

        let saved = store.message_delivery(request.message_id).unwrap();
        let saved_receipt = saved.receipt.unwrap();
        assert_eq!(saved_receipt.protocol_version, 2);
        let response_receipt = saved_receipt.for_protocol_version(3);
        assert_eq!(response_receipt.protocol_version, 3);
        assert_eq!(
            store
                .message_delivery(request.message_id)
                .unwrap()
                .receipt
                .unwrap(),
            saved_receipt
        );

        let mut changed = retry;
        changed.request.body.push_str(" changed");
        assert!(matches!(
            store.bind_message_attempt(&changed),
            Err(AppError::MessageConflict { id }) if id == request.message_id
        ));
    }

    #[test]
    fn concurrent_first_sends_with_one_uuid_converge_on_one_route() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("homebased.sqlite");
        drop(Store::open(&path).unwrap());
        let request = request();
        let recipient = requested_recipient(&request);
        let barrier = Arc::new(Barrier::new(2));
        let candidates = [MachineId::new(), MachineId::new()];
        let workers: Vec<_> = candidates
            .into_iter()
            .map(|destination| {
                let path = path.clone();
                let request = request.clone();
                let recipient = recipient.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    let mut store = Store::open(&path).unwrap();
                    barrier.wait();
                    store.begin_outbound_message(&request).unwrap();
                    barrier.wait();
                    store
                        .bind_outbound_message(&request, destination, &recipient)
                        .unwrap()
                })
            })
            .collect();
        let first = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(first[0], first[1]);
        assert!(matches!(first[0], OutboundMessageBinding::Bound { .. }));
    }
}
