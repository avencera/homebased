//! Typed direct messages delivered to Codex threads on a Fleet machine

use crate::domain::API_VERSION;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::NoticeId;

/// Maximum message body size in UTF-8 bytes
pub const MESSAGE_BODY_MAX_BYTES: usize = 16 * 1024;

/// Stable identity of one direct message and all its explicit retries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct MessageId(Uuid);

impl MessageId {
    /// Allocate a new message identity
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wrap a non-nil UUID
    pub fn from_uuid(uuid: Uuid) -> Result<Self, AppError> {
        if uuid.is_nil() {
            return Err(AppError::MessageInvalid {
                message: "message UUID must not be nil".into(),
            });
        }
        Ok(Self(uuid))
    }

    /// Underlying UUID
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for MessageId {
    type Err = AppError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::parse_str(raw).map_err(|_| AppError::MessageInvalid {
            message: "message UUID must be a full UUID".into(),
        })?;
        Self::from_uuid(uuid)
    }
}

impl<'de> Deserialize<'de> for MessageId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let uuid = Uuid::deserialize(deserializer)?;
        if uuid.is_nil() {
            return Err(serde::de::Error::custom("message UUID must not be nil"));
        }
        Ok(Self(uuid))
    }
}

/// Source identity for a message from a thread, task, or resource notice
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessageSource {
    /// Reply route to one exact source thread
    Thread {
        /// Machine that owns the source thread
        machine: MachineId,
        /// Source thread UUID
        thread: ThreadId,
    },
    /// Reply route to the machine and thread associated with one source task
    Task {
        /// Machine that owns the source task
        machine: MachineId,
        /// Source task UUID
        task: TaskId,
    },
    /// Reply route for a durable resource notice
    ResourceNotice {
        /// Machine that owns the resource authority
        machine: MachineId,
        /// Stable identity of the notice
        notice_id: NoticeId,
    },
}

impl MessageSource {
    /// Source machine for the selected route
    #[must_use]
    pub const fn machine(&self) -> MachineId {
        match self {
            Self::Thread { machine, .. }
            | Self::Task { machine, .. }
            | Self::ResourceNotice { machine, .. } => *machine,
        }
    }
}

/// Receiver-side destination selector
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Recipient {
    /// Deliver to this exact local Codex thread
    Thread {
        /// Destination thread UUID
        thread: ThreadId,
    },
    /// Deliver to the most recently active local thread with this exact cwd
    Cwd {
        /// Absolute receiver path, or a path beginning with `~/` on the receiver
        cwd: PathBuf,
    },
}

/// Source route selected by the local CLI before the daemon adds its machine identity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessageSourceSelector {
    /// Reply route to one exact source thread
    Thread {
        /// Source thread UUID
        thread: ThreadId,
    },
    /// Reply route to one source task
    Task {
        /// Source task UUID
        task: TaskId,
    },
}

/// Destination selected by the local CLI
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessageTarget {
    /// A machine name or UUID and its receiver-side selector
    Machine {
        /// Display name or stable machine UUID
        machine: String,
        /// Exact thread or receiver-side working directory
        recipient: Recipient,
    },
    /// The origin machine and thread for an existing task
    Task {
        /// Task whose origin route supplies the destination
        task: TaskId,
    },
}

/// Strict request accepted by the local Unix-socket message sender
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageSendRequest {
    /// Public API schema version
    pub api_version: u32,
    /// Stable identity reused for every explicit retry
    pub message_id: MessageId,
    /// Resolved by the local daemon before it contacts a receiver
    pub target: MessageTarget,
    /// Source thread or task. The local daemon adds its actual machine UUID
    pub source: MessageSourceSelector,
    /// Message text, limited to [`MESSAGE_BODY_MAX_BYTES`] UTF-8 bytes
    pub body: String,
    /// Prior message this message answers, when present
    pub reply_to: Option<MessageId>,
    /// Conversation UUID shared by related messages
    pub conversation_id: Uuid,
}

impl MessageSendRequest {
    /// Validate fields that do not depend on the local daemon or remote receiver
    pub fn validate(&self) -> Result<(), AppError> {
        if self.api_version != API_VERSION {
            return Err(AppError::Usage {
                message: "unsupported API version".into(),
            });
        }
        validate_body(&self.body)?;
        if self.conversation_id.is_nil() {
            return Err(AppError::MessageInvalid {
                message: "conversation UUID must not be nil".into(),
            });
        }
        match &self.source {
            MessageSourceSelector::Thread { thread } if thread.0.is_nil() => {
                return Err(AppError::MessageInvalid {
                    message: "source thread UUID must not be nil".into(),
                });
            }
            MessageSourceSelector::Task { task } if task.0.is_nil() => {
                return Err(AppError::MessageInvalid {
                    message: "source task UUID must not be nil".into(),
                });
            }
            _ => {}
        }
        match &self.target {
            MessageTarget::Machine { machine, recipient } => {
                if machine.trim().is_empty() {
                    return Err(AppError::MessageInvalid {
                        message: "destination machine must not be empty".into(),
                    });
                }
                if let Recipient::Cwd { cwd } = recipient {
                    validate_cwd_selector(cwd)?;
                }
                if let Recipient::Thread { thread } = recipient
                    && thread.0.is_nil()
                {
                    return Err(AppError::MessageInvalid {
                        message: "destination thread UUID must not be nil".into(),
                    });
                }
            }
            MessageTarget::Task { task } if task.0.is_nil() => {
                return Err(AppError::MessageInvalid {
                    message: "destination task UUID must not be nil".into(),
                });
            }
            MessageTarget::Task { .. } => {}
        }
        Ok(())
    }
}

/// Durable sender binding for one message UUID
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutboundMessageBinding {
    /// The request is reserved while the daemon resolves its destination
    Resolving {
        /// Complete caller request checked on every retry
        request: MessageSendRequest,
    },
    /// The request has one fixed receiver and recipient selector
    Bound {
        /// Complete caller request checked on every retry
        request: MessageSendRequest,
        /// Stable receiver UUID that cannot change on retry
        destination_machine: MachineId,
        /// Resolved task thread or original machine recipient selector
        recipient: Recipient,
    },
}

impl OutboundMessageBinding {
    /// Caller request stored by this binding
    #[must_use]
    pub const fn request(&self) -> &MessageSendRequest {
        match self {
            Self::Resolving { request } | Self::Bound { request, .. } => request,
        }
    }

    /// Fixed route, when destination resolution is complete
    #[must_use]
    pub const fn route(&self) -> Option<(MachineId, &Recipient)> {
        match self {
            Self::Resolving { .. } => None,
            Self::Bound {
                destination_machine,
                recipient,
                ..
            } => Some((*destination_machine, recipient)),
        }
    }

    /// Validate the saved request and any fixed route
    pub fn validate(&self) -> Result<(), AppError> {
        self.request().validate()?;
        let Self::Bound {
            destination_machine,
            recipient,
            ..
        } = self
        else {
            return Ok(());
        };
        if destination_machine.as_uuid().is_nil() {
            return Err(AppError::MessageInvalid {
                message: "destination machine UUID must not be nil".into(),
            });
        }
        let target_matches = match (&self.request().target, recipient) {
            (
                MessageTarget::Machine {
                    recipient: requested,
                    ..
                },
                resolved,
            ) => requested == resolved,
            (MessageTarget::Task { .. }, Recipient::Thread { thread }) => !thread.0.is_nil(),
            (MessageTarget::Task { .. }, Recipient::Cwd { .. }) => false,
        };
        if !target_matches {
            return Err(AppError::MessageInvalid {
                message: "saved destination does not match the message target".into(),
            });
        }
        Ok(())
    }
}

/// Versioned response from a remote message receiver
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageResponse {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Machine identity checked by the receiver
    pub destination_machine: MachineId,
    /// Durable queue receipt, including the resolved local destination
    pub receipt: MessageReceipt,
}

/// Versioned response returned to the local CLI after sender routing completes
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageSendResponse {
    /// Public API schema version
    pub api_version: u32,
    /// Stable identity of the sent message
    pub message_id: MessageId,
    /// Resolved destination machine UUID
    pub destination_machine: MachineId,
    /// Resolved receiver thread UUID
    pub destination_thread: ThreadId,
    /// Resolved receiver-side working directory
    pub destination_cwd: PathBuf,
    /// Receiver's durable success receipt
    pub receipt: MessageReceipt,
}

/// Typed request accepted by `POST /v1/cluster/messages`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageRequest {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version used for this wire attempt
    pub protocol_version: u32,
    /// Stable identity reused for every explicit retry
    pub message_id: MessageId,
    /// Intended receiving machine
    pub destination_machine: MachineId,
    /// Route that can be used for a reply
    pub source: MessageSource,
    /// Exact thread or receiver-side working directory
    pub recipient: Recipient,
    /// Message text, limited to [`MESSAGE_BODY_MAX_BYTES`] UTF-8 bytes
    pub body: String,
    /// Prior message this message answers, when present
    pub reply_to: Option<MessageId>,
    /// Conversation UUID shared by related messages
    pub conversation_id: Uuid,
}

/// Semantic request identity, excluding only the negotiated wire protocol version
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MessageIdentity {
    api_version: u32,
    message_id: MessageId,
    destination_machine: MachineId,
    source: MessageSource,
    recipient: Recipient,
    body: String,
    reply_to: Option<MessageId>,
    conversation_id: Uuid,
}

impl MessageRequest {
    pub(crate) fn identity(&self) -> MessageIdentity {
        MessageIdentity {
            api_version: self.api_version,
            message_id: self.message_id,
            destination_machine: self.destination_machine,
            source: self.source.clone(),
            recipient: self.recipient.clone(),
            body: semantic_body(&self.source, &self.body),
            reply_to: self.reply_to,
            conversation_id: self.conversation_id,
        }
    }

    /// Reject invalid body text and unsupported receiver-side cwd forms
    pub fn validate(&self) -> Result<(), AppError> {
        if self.api_version != API_VERSION {
            return Err(AppError::Usage {
                message: "unsupported API version".into(),
            });
        }
        if self.destination_machine.as_uuid().is_nil() || self.source.machine().as_uuid().is_nil() {
            return Err(AppError::MessageInvalid {
                message: "message machine UUIDs must not be nil".into(),
            });
        }
        match (&self.source, &self.recipient) {
            (MessageSource::Thread { thread, .. }, _) if thread.0.is_nil() => {
                return Err(AppError::MessageInvalid {
                    message: "source thread UUID must not be nil".into(),
                });
            }
            (MessageSource::Task { task, .. }, _) if task.0.is_nil() => {
                return Err(AppError::MessageInvalid {
                    message: "source task UUID must not be nil".into(),
                });
            }
            (_, Recipient::Thread { thread }) if thread.0.is_nil() => {
                return Err(AppError::MessageInvalid {
                    message: "destination thread UUID must not be nil".into(),
                });
            }
            _ => {}
        }
        validate_body(&self.body)?;
        if self.conversation_id.is_nil() {
            return Err(AppError::MessageInvalid {
                message: "conversation UUID must not be nil".into(),
            });
        }
        if let Recipient::Cwd { cwd } = &self.recipient {
            validate_cwd_selector(cwd)?;
        }
        Ok(())
    }
}

fn semantic_body(source: &MessageSource, body: &str) -> String {
    if !matches!(source, MessageSource::ResourceNotice { .. }) {
        return body.to_string();
    }

    // supervisor notices embed the wire version in their serialized backing body
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(body) else {
        return body.to_string();
    };
    if let Some(object) = value.as_object_mut() {
        object.remove("protocol_version");
    }
    serde_json::to_string(&value).unwrap_or_else(|_| body.to_string())
}

fn validate_body(body: &str) -> Result<(), AppError> {
    if body.trim().is_empty() {
        return Err(AppError::MessageInvalid {
            message: "message body must not be empty".into(),
        });
    }
    if body.len() > MESSAGE_BODY_MAX_BYTES {
        return Err(AppError::MessageInvalid {
            message: format!("message body exceeds {MESSAGE_BODY_MAX_BYTES} UTF-8 bytes"),
        });
    }
    if body
        .chars()
        .any(|ch| ch.is_control() && !matches!(ch, '\n' | '\r' | '\t'))
    {
        return Err(AppError::MessageInvalid {
            message: "message body contains an unsupported control character".into(),
        });
    }
    Ok(())
}

fn validate_cwd_selector(cwd: &std::path::Path) -> Result<(), AppError> {
    let text = cwd.to_string_lossy();
    if let Some(relative) = text.strip_prefix("~/") {
        if relative
            .split(std::path::MAIN_SEPARATOR)
            .any(|component| component == "..")
        {
            return Err(AppError::MessageInvalid {
                message: "message cwd must not escape receiver HOME".into(),
            });
        }
        return Ok(());
    }
    if !cwd.is_absolute() {
        return Err(AppError::MessageInvalid {
            message: "message cwd must be absolute or start with ~/".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MessageId, MessageRequest, MessageSource, Recipient};
    use crate::domain::{API_VERSION, ThreadId};
    use crate::machine::MachineId;
    use crate::resource::NoticeId;
    use uuid::Uuid;

    #[test]
    fn resource_notice_source_requires_a_non_nil_notice_identity() {
        let notice_id = NoticeId::new();
        let request = MessageRequest {
            api_version: API_VERSION,
            protocol_version: 1,
            message_id: MessageId::new(),
            destination_machine: MachineId::new(),
            source: MessageSource::ResourceNotice {
                machine: MachineId::new(),
                notice_id,
            },
            recipient: Recipient::Thread {
                thread: ThreadId(Uuid::now_v7()),
            },
            body: "Resource notice source validation".into(),
            reply_to: None,
            conversation_id: Uuid::now_v7(),
        };
        let mut wire = serde_json::to_value(&request).unwrap();
        assert!(serde_json::from_value::<MessageRequest>(wire.clone()).is_ok());

        wire["source"]["notice_id"] = serde_json::json!(Uuid::nil());
        assert!(serde_json::from_value::<MessageRequest>(wire).is_err());
    }

    #[test]
    fn semantic_identity_ignores_the_notice_wire_version_only() {
        let mut request = MessageRequest {
            api_version: API_VERSION,
            protocol_version: 1,
            message_id: MessageId::new(),
            destination_machine: MachineId::new(),
            source: MessageSource::ResourceNotice {
                machine: MachineId::new(),
                notice_id: NoticeId::new(),
            },
            recipient: Recipient::Thread {
                thread: ThreadId(Uuid::now_v7()),
            },
            body: serde_json::json!({"protocol_version": 1, "reason": "review"}).to_string(),
            reply_to: None,
            conversation_id: Uuid::now_v7(),
        };
        let identity = request.identity();

        request.protocol_version = 2;
        request.body = serde_json::json!({"protocol_version": 2, "reason": "review"}).to_string();
        assert_eq!(request.identity(), identity);

        request.body = serde_json::json!({"protocol_version": 2, "reason": "changed"}).to_string();
        assert_ne!(request.identity(), identity);
    }
}

/// Durable binding between a message request and its resolved local destination
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageAttempt {
    /// Immutable semantic content with the latest accepted wire protocol version
    pub request: MessageRequest,
    /// Resolved local destination thread
    pub destination_thread: ThreadId,
    /// Resolved local thread working directory
    pub destination_cwd: PathBuf,
}

/// Durable acknowledgement committed after one successful queue invocation
///
/// A crash after queue success and before receipt commit can cause a duplicate
/// on explicit retry, so queued messages carry their stable UUID for deduplication
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageReceipt {
    /// Public API schema version
    pub api_version: u32,
    /// Cluster protocol version
    pub protocol_version: u32,
    /// Message UUID that was queued
    pub message_id: MessageId,
    /// Destination thread used by the queue command
    pub destination_thread: ThreadId,
    /// Receiver-side working directory used by the queue command
    pub destination_cwd: PathBuf,
    /// Time when the receiver committed this receipt
    pub delivered_at: DateTime<Utc>,
}

impl MessageReceipt {
    pub(crate) fn for_protocol_version(&self, protocol_version: u32) -> Self {
        Self {
            protocol_version,
            ..self.clone()
        }
    }
}

/// Persisted attempt and optional receipt for one message UUID
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageDelivery {
    /// Bound request and selected destination, if an attempt started
    pub attempt: Option<MessageAttempt>,
    /// Durable success receipt, if queue delivery completed
    pub receipt: Option<MessageReceipt>,
}
