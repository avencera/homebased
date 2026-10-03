//! Fleet endpoint work for durable direct-message delivery

use crate::domain::API_VERSION;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::callback::claude_inbox::ClaudeInbox;
use crate::callback::{PendingT3Send, send_saved_queue_attempt};
use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, call};
use crate::daemon::keyed_locks::{KeyedGuard, KeyedLocks};
use crate::domain::{AgentKind, TaskEnv, ThreadId};
use crate::error::AppError;
use crate::invocation::resolve_agent_binary;
use crate::message::{
    MessageAttempt, MessageId, MessageReceipt, MessageRequest, MessageSource, Recipient,
};
use crate::submission::{CallbackContext, CallbackExecutable};

const SESSION_META_MAX_BYTES: usize = 64 * 1024;
const MESSAGE_PREFIX: &str = "HOMEBASED_MESSAGE ";

/// Per-message delivery permits prevent concurrent retries from queueing one UUID twice
#[derive(Clone, Default)]
pub(crate) struct MessageReceiver {
    // per-key locks, not shards: a shard collision would fail an unrelated attempt as contended
    attempts: KeyedLocks<MessageId>,
}

impl MessageReceiver {
    /// Resolve, persist, and deliver one explicit message attempt
    pub(crate) async fn receive(
        &self,
        state: &AppState,
        request: MessageRequest,
    ) -> Result<MessageReceipt, AppError> {
        request.validate()?;
        let (_attempt_permit, attempt_was_contended) =
            self.attempt_permit(request.message_id).await;
        let saved = call(&state.store, |reply| StoreMsg::MessageDelivery {
            id: request.message_id,
            reply,
        })
        .await?;
        let attempt = match saved.attempt {
            Some(mut attempt) => {
                if attempt.request.identity() != request.identity() {
                    return Err(AppError::MessageConflict {
                        id: request.message_id,
                    });
                }
                if let Some(receipt) = saved.receipt {
                    return Ok(receipt.for_protocol_version(request.protocol_version));
                }
                if attempt_was_contended {
                    return Err(attempt_wait_failed(request.message_id));
                }
                attempt.request.protocol_version = request.protocol_version;
                call(&state.store, |reply| StoreMsg::BindMessageAttempt {
                    attempt,
                    reply,
                })
                .await?
            }
            None => {
                if saved.receipt.is_some() {
                    return Err(AppError::Internal {
                        message: "message receipt has no saved attempt".into(),
                    });
                }
                let (destination_thread, destination_cwd) =
                    resolve_destination(&request.recipient).await?;
                let candidate = MessageAttempt {
                    request,
                    destination_thread,
                    destination_cwd,
                };
                call(&state.store, |reply| StoreMsg::BindMessageAttempt {
                    attempt: candidate,
                    reply,
                })
                .await?
            }
        };
        self.deliver(state, attempt).await
    }

    async fn attempt_permit(&self, id: MessageId) -> (KeyedGuard<MessageId>, bool) {
        match self.attempts.try_lock(id) {
            Some(permit) => (permit, false),
            None => (self.attempts.lock(id).await, true),
        }
    }

    async fn deliver(
        &self,
        state: &AppState,
        attempt: MessageAttempt,
    ) -> Result<MessageReceipt, AppError> {
        let message_id = attempt.request.message_id;
        let line = format!(
            "{MESSAGE_PREFIX}{}",
            serde_json::to_string(&QueuedMessage::from(&attempt))
                .map_err(|error| delivery_failed(message_id, error))?
        );
        send_queue_line(
            state,
            message_id,
            attempt.destination_thread,
            attempt.destination_cwd.clone(),
            line,
        )
        .await
        .map_err(|error| delivery_failed(message_id, error))?;

        let receipt = MessageReceipt {
            api_version: API_VERSION,
            protocol_version: attempt.request.protocol_version,
            message_id,
            destination_thread: attempt.destination_thread,
            destination_cwd: attempt.destination_cwd,
            delivered_at: chrono::Utc::now(),
        };
        call(&state.store, |reply| StoreMsg::CommitMessageReceipt {
            receipt,
            reply,
        })
        .await
    }
}

async fn send_queue_line(
    state: &AppState,
    message_id: MessageId,
    destination_thread: ThreadId,
    destination_cwd: PathBuf,
    line: String,
) -> Result<(), String> {
    let paths = state
        .home
        .prepare_message(message_id)
        .map_err(|error| error.to_string())?;
    let env = TaskEnv::capture();
    // only a Codex destination needs the executable; the send checks it there
    let codex = match resolve_agent_binary(AgentKind::Codex, &env.path, &destination_cwd) {
        Ok(path) => CallbackExecutable::available(path),
        Err(error) => CallbackExecutable::Unavailable {
            reason: error.to_string(),
        },
    };
    let context = CallbackContext {
        env,
        cwd: destination_cwd,
        codex,
    };

    let log_path = paths.queue_log;
    let lock_path = paths.delivery_lock;
    // a retry with the same message id finds this and goes only through T3
    let pending = PendingT3Send::new(paths.dir.join("t3-pending"));
    let queue_context = context.clone();
    tokio::task::spawn_blocking(move || {
        send_saved_queue_attempt(
            &queue_context,
            destination_thread,
            &line,
            &log_path,
            &lock_path,
            &pending,
        )
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())
}

#[derive(Serialize)]
struct QueuedMessage<'a> {
    message_id: MessageId,
    source: &'a MessageSource,
    destination_thread: ThreadId,
    body: &'a str,
    reply_to: Option<MessageId>,
    conversation_id: Uuid,
}

impl<'a> From<&'a MessageAttempt> for QueuedMessage<'a> {
    fn from(attempt: &'a MessageAttempt) -> Self {
        Self {
            message_id: attempt.request.message_id,
            source: &attempt.request.source,
            destination_thread: attempt.destination_thread,
            body: &attempt.request.body,
            reply_to: attempt.request.reply_to,
            conversation_id: attempt.request.conversation_id,
        }
    }
}

fn delivery_failed(message_id: MessageId, error: impl std::fmt::Display) -> AppError {
    tracing::warn!(message_id = %message_id, "message delivery attempt failed: {error}");
    AppError::MessageDeliveryFailed {
        id: message_id,
        message: format!(
            "delivery attempt failed: {error}; retry the same message UUID explicitly"
        ),
    }
}

fn attempt_wait_failed(message_id: MessageId) -> AppError {
    AppError::MessageDeliveryFailed {
        id: message_id,
        message: "another attempt completed without a receipt; retry the same UUID explicitly"
            .into(),
    }
}

async fn resolve_destination(recipient: &Recipient) -> Result<(ThreadId, PathBuf), AppError> {
    let selector = recipient.clone();
    tokio::task::spawn_blocking(move || resolve_destination_sync(&selector))
        .await
        .map_err(session_unavailable)?
}

fn resolve_destination_sync(recipient: &Recipient) -> Result<(ThreadId, PathBuf), AppError> {
    let selected = match recipient {
        Recipient::Thread { thread } => {
            // delivery also looks for a Claude session before a Codex thread
            if let Some(cwd) = claude_session_cwd(*thread)? {
                return Ok((*thread, cwd));
            }
            local_sessions()?
                .into_iter()
                .filter(|session| session.thread == *thread)
                .max_by_key(|session| session.modified)
                .ok_or_else(|| AppError::AgentThreadNotFound {
                    selector: thread.to_string(),
                })?
        }
        // a cwd selects only Codex threads: headless Claude workers share their
        // owner's cwd and registry shape, so the newest session there is often a worker
        Recipient::Cwd { cwd } => {
            let target = expand_receiver_cwd(cwd)?;
            local_sessions()?
                .into_iter()
                .filter(|session| session.cwd == target)
                .max_by_key(|session| session.modified)
                .ok_or_else(|| AppError::AgentThreadNotFound {
                    selector: target.display().to_string(),
                })?
        }
    };
    Ok((selected.thread, selected.cwd))
}

/// Working directory of the local Claude Code session with this id, if one owns it
fn claude_session_cwd(thread: ThreadId) -> Result<Option<PathBuf>, AppError> {
    let home = std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| AppError::MessageUnavailable {
            message: "receiver HOME is unavailable".into(),
        })?;
    ClaudeInbox::session_cwd(&home, thread).map_err(|error| {
        tracing::warn!(%thread, "Claude session metadata unavailable: {error}");
        AppError::MessageUnavailable {
            message: format!("cannot inspect local Claude session metadata: {error}"),
        }
    })
}

fn expand_receiver_cwd(cwd: &Path) -> Result<PathBuf, AppError> {
    if cwd.is_absolute() {
        return Ok(cwd.to_path_buf());
    }
    let relative = cwd
        .to_string_lossy()
        .strip_prefix("~/")
        .ok_or_else(|| AppError::MessageInvalid {
            message: "message cwd must be absolute or start with ~/".into(),
        })?
        .to_string();
    let home = std::env::var_os("HOME").ok_or_else(|| AppError::MessageUnavailable {
        message: "receiver HOME is unavailable".into(),
    })?;
    let home = PathBuf::from(home);
    if !home.is_absolute() {
        return Err(AppError::MessageUnavailable {
            message: "receiver HOME is not absolute".into(),
        });
    }
    Ok(home.join(relative))
}

#[derive(Debug)]
struct LocalSession {
    thread: ThreadId,
    cwd: PathBuf,
    modified: SystemTime,
}

#[derive(Deserialize)]
struct SessionMetaLine {
    #[serde(rename = "type")]
    kind: String,
    payload: SessionMetaPayload,
}

#[derive(Deserialize)]
struct SessionMetaPayload {
    id: String,
    cwd: PathBuf,
}

fn local_sessions() -> Result<Vec<LocalSession>, AppError> {
    let root = session_directory()?;
    let mut files = Vec::new();
    match fs::metadata(&root) {
        Ok(metadata) if metadata.is_dir() => collect_rollouts(&root, &mut files)?,
        Ok(_) => {
            return Err(session_unavailable(
                "Codex sessions path is not a directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(session_unavailable(error)),
    }
    files.into_iter().filter_map(read_session_meta).collect()
}

fn session_directory() -> Result<PathBuf, AppError> {
    let codex_home = std::env::var_os("CODEX_HOME");
    let root = match codex_home {
        Some(path) if path.is_empty() => {
            return Err(session_unavailable("CODEX_HOME is empty"));
        }
        Some(path) => PathBuf::from(path),
        None => {
            let home = std::env::var_os("HOME")
                .ok_or_else(|| session_unavailable("receiver HOME is unavailable"))?;
            PathBuf::from(home).join(".codex")
        }
    };
    if !root.is_absolute() {
        return Err(session_unavailable("Codex home is not absolute"));
    }
    Ok(root.join("sessions"))
}

fn collect_rollouts(directory: &Path, files: &mut Vec<PathBuf>) -> Result<(), AppError> {
    let entries = fs::read_dir(directory).map_err(session_unavailable)?;
    for entry in entries {
        let entry = entry.map_err(session_unavailable)?;
        let file_type = entry.file_type().map_err(session_unavailable)?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_rollouts(&path, files)?;
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_type.is_file() && name.starts_with("rollout-") && name.ends_with(".jsonl") {
            files.push(path);
        }
    }
    Ok(())
}

fn read_session_meta(path: PathBuf) -> Option<Result<LocalSession, AppError>> {
    match read_session_meta_inner(&path) {
        Ok(Some(session)) => Some(Ok(session)),
        Ok(None) => None,
        Err(error) => Some(Err(error)),
    }
}

fn read_session_meta_inner(path: &Path) -> Result<Option<LocalSession>, AppError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(session_unavailable(error)),
    };
    let mut reader = BufReader::new(file).take((SESSION_META_MAX_BYTES + 1) as u64);
    let mut first_line = Vec::new();
    reader
        .read_until(b'\n', &mut first_line)
        .map_err(session_unavailable)?;
    if first_line.len() > SESSION_META_MAX_BYTES {
        return Ok(None);
    }
    let meta: SessionMetaLine = match serde_json::from_slice(&first_line) {
        Ok(meta) => meta,
        Err(_) => return Ok(None),
    };
    if meta.kind != "session_meta" || !meta.payload.cwd.is_absolute() {
        return Ok(None);
    }
    let Ok(uuid) = Uuid::parse_str(&meta.payload.id) else {
        return Ok(None);
    };
    if uuid.is_nil() {
        return Ok(None);
    }
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(session_unavailable(error)),
    };
    let modified = metadata.modified().map_err(session_unavailable)?;
    Ok(Some(LocalSession {
        thread: ThreadId(uuid),
        cwd: meta.payload.cwd,
        modified,
    }))
}

fn session_unavailable(error: impl std::fmt::Display) -> AppError {
    tracing::warn!("Codex session metadata unavailable: {error}");
    AppError::MessageUnavailable {
        message: "cannot inspect local Codex session metadata".into(),
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::API_VERSION;
    use std::time::Duration;

    use super::{MESSAGE_PREFIX, MessageReceiver, QueuedMessage};
    use crate::domain::{TaskId, ThreadId};
    use crate::machine::MachineId;
    use crate::message::{MessageAttempt, MessageId, MessageRequest, MessageSource, Recipient};
    use serde_json::json;
    use std::path::PathBuf;
    use uuid::Uuid;

    #[tokio::test]
    async fn different_message_ids_do_not_queue_behind_one_delivery_permit() {
        let receiver = MessageReceiver::default();
        let first_id = MessageId::new();
        let second_id = MessageId::new();
        let (first_permit, first_was_contended) = receiver.attempt_permit(first_id).await;
        assert!(!first_was_contended);

        let (second_permit, second_was_contended) =
            tokio::time::timeout(Duration::from_secs(1), receiver.attempt_permit(second_id))
                .await
                .unwrap();
        assert!(!second_was_contended);
        drop(second_permit);

        let waiting_receiver = receiver.clone();
        let (complete_tx, mut complete_rx) = tokio::sync::oneshot::channel();
        let waiting_attempt = tokio::spawn(async move {
            let (_permit, was_contended) = waiting_receiver.attempt_permit(first_id).await;
            let _ = complete_tx.send(was_contended);
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut complete_rx)
                .await
                .is_err()
        );

        drop(first_permit);
        assert!(complete_rx.await.unwrap());
        waiting_attempt.await.unwrap();
    }

    #[test]
    fn queued_line_carries_the_message_route_and_body() {
        let request = MessageRequest {
            api_version: API_VERSION,
            protocol_version: 1,
            message_id: MessageId::new(),
            destination_machine: MachineId::new(),
            source: MessageSource::Task {
                machine: MachineId::new(),
                task: TaskId::new(),
            },
            recipient: Recipient::Thread {
                thread: ThreadId(Uuid::now_v7()),
            },
            body: "Please review this change".into(),
            reply_to: None,
            conversation_id: Uuid::now_v7(),
        };
        let attempt = MessageAttempt {
            request: request.clone(),
            destination_thread: ThreadId(Uuid::now_v7()),
            destination_cwd: PathBuf::from("/tmp"),
        };
        let line = format!(
            "{MESSAGE_PREFIX}{}",
            serde_json::to_string(&QueuedMessage::from(&attempt)).unwrap()
        );
        assert!(line.starts_with("HOMEBASED_MESSAGE "));
        let body: serde_json::Value = serde_json::from_str(&line[MESSAGE_PREFIX.len()..]).unwrap();
        assert_eq!(body["message_id"], json!(request.message_id));
        assert_eq!(
            body["destination_thread"],
            json!(attempt.destination_thread)
        );
        assert_eq!(body["body"], "Please review this change");
        assert_eq!(body["source"]["kind"], "task");
        assert!(body["source"].get("task").is_some());
    }
}
