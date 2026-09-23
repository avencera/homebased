//! `homebased message` commands.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Subcommand;
use uuid::Uuid;

use crate::client::Client;
use crate::domain::{API_VERSION, TaskId, ThreadId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::message::{
    MessageId, MessageSendRequest, MessageSendResponse, MessageSourceSelector, MessageTarget,
    Recipient,
};

use super::{Ctx, OutputMode};

/// Message commands.
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum MessageCommand {
    /// Send a direct message to a Codex thread.
    Send {
        /// Destination machine name or UUID. Required with `--thread` or `--cwd`.
        #[arg(long, conflicts_with = "task")]
        machine: Option<String>,
        /// Exact destination thread UUID.
        #[arg(long, conflicts_with_all = ["cwd", "task"])]
        thread: Option<ThreadId>,
        /// Destination working directory on the receiving machine.
        #[arg(long, conflicts_with_all = ["thread", "task"])]
        cwd: Option<PathBuf>,
        /// Send to this task's origin thread. Use alone, without `--machine`.
        #[arg(long, conflicts_with_all = ["machine", "thread", "cwd"])]
        task: Option<TaskId>,
        /// Message text.
        #[arg(long, required = true)]
        message: String,
        /// Stable UUID for an explicit retry after an unknown outcome.
        #[arg(long)]
        message_id: Option<MessageId>,
        /// UUID of the message this one answers.
        #[arg(long)]
        reply_to: Option<MessageId>,
        /// Shared conversation UUID. Defaults to the message UUID.
        #[arg(long)]
        conversation: Option<Uuid>,
        /// Explicit source thread UUID. Overrides source environment variables.
        #[arg(long, conflicts_with = "source_task")]
        source_thread: Option<ThreadId>,
        /// Explicit source task UUID. Overrides source environment variables.
        #[arg(long, conflicts_with = "source_thread")]
        source_task: Option<TaskId>,
    },
}

struct SendArgs {
    machine: Option<String>,
    thread: Option<ThreadId>,
    cwd: Option<PathBuf>,
    task: Option<TaskId>,
    body: String,
    message_id: Option<MessageId>,
    reply_to: Option<MessageId>,
    conversation: Option<Uuid>,
    source_thread: Option<ThreadId>,
    source_task: Option<TaskId>,
}

/// Run one message command.
pub async fn run(ctx: &Ctx, command: MessageCommand) -> Result<ExitCode, AppError> {
    match command {
        MessageCommand::Send {
            machine,
            thread,
            cwd,
            task,
            message,
            message_id,
            reply_to,
            conversation,
            source_thread,
            source_task,
        } => {
            send(
                ctx,
                SendArgs {
                    machine,
                    thread,
                    cwd,
                    task,
                    body: message,
                    message_id,
                    reply_to,
                    conversation,
                    source_thread,
                    source_task,
                },
            )
            .await
        }
    }
}

async fn send(ctx: &Ctx, args: SendArgs) -> Result<ExitCode, AppError> {
    let target = target(args.machine, args.thread, args.cwd, args.task)?;
    let source = source(args.source_thread, args.source_task)?;
    let message_id = args.message_id.unwrap_or_default();
    let request = MessageSendRequest {
        api_version: API_VERSION,
        message_id,
        target,
        source,
        body: args.body,
        reply_to: args.reply_to,
        conversation_id: conversation_id(args.conversation, message_id),
    };
    request.validate()?;

    let response: MessageSendResponse = Client::new(ctx.home.sock_path())
        .post_json("/v1/messages/send", &request)
        .await?;
    validate_response(&request, &response)?;

    let value = serde_json::to_value(&response)?;
    let human = format!(
        "sent {} to machine {} thread {}",
        response.message_id, response.destination_machine, response.destination_thread
    );
    match ctx.output {
        OutputMode::Quiet => println!("{}", response.message_id),
        OutputMode::Json => ctx.print_json(value)?,
        OutputMode::Human => println!("{human}"),
    }
    Ok(ExitCode::SUCCESS)
}

fn target(
    machine: Option<String>,
    thread: Option<ThreadId>,
    cwd: Option<PathBuf>,
    task: Option<TaskId>,
) -> Result<MessageTarget, AppError> {
    match (machine, thread, cwd, task) {
        (None, None, None, Some(task)) => Ok(MessageTarget::Task { task }),
        (Some(machine), Some(thread), None, None) => Ok(MessageTarget::Machine {
            machine,
            recipient: Recipient::Thread { thread },
        }),
        (Some(machine), None, Some(cwd), None) => Ok(MessageTarget::Machine {
            machine,
            recipient: Recipient::Cwd { cwd },
        }),
        _ => Err(AppError::Usage {
            message: "use --task alone, or use --machine with exactly one of --thread or --cwd"
                .into(),
        }),
    }
}

fn source(
    source_thread: Option<ThreadId>,
    source_task: Option<TaskId>,
) -> Result<MessageSourceSelector, AppError> {
    match (source_thread, source_task) {
        (Some(thread), None) => return Ok(MessageSourceSelector::Thread { thread }),
        (None, Some(task)) => return Ok(MessageSourceSelector::Task { task }),
        (Some(_), Some(_)) => {
            return Err(AppError::Usage {
                message: "use only one of --source-thread or --source-task".into(),
            });
        }
        (None, None) => {}
    }

    if let Some(value) = std::env::var_os("HOMEBASED_TASK_ID") {
        let value = value.to_str().ok_or_else(|| AppError::Usage {
            message: "HOMEBASED_TASK_ID must be a UUID".into(),
        })?;
        let task = value.parse().map_err(|error: AppError| AppError::Usage {
            message: format!("HOMEBASED_TASK_ID must be a task UUID: {error}"),
        })?;
        return Ok(MessageSourceSelector::Task { task });
    }

    for key in ["CODEX_THREAD_ID", "CODEX_SESSION_ID"] {
        let Some(value) = std::env::var_os(key) else {
            continue;
        };
        let value = value.to_str().ok_or_else(|| AppError::Usage {
            message: format!("{key} must be a UUID"),
        })?;
        let thread = value.parse().map_err(|error: AppError| AppError::Usage {
            message: format!("{key} must be a thread UUID: {error}"),
        })?;
        return Ok(MessageSourceSelector::Thread { thread });
    }

    Err(AppError::Usage {
        message: "set HOMEBASED_TASK_ID, CODEX_THREAD_ID, or CODEX_SESSION_ID, or pass an explicit source flag".into(),
    })
}

fn validate_response(
    request: &MessageSendRequest,
    response: &MessageSendResponse,
) -> Result<(), AppError> {
    let expected_machine = match &request.target {
        MessageTarget::Machine { machine, .. } => machine.parse::<MachineId>().ok(),
        MessageTarget::Task { .. } => None,
    };
    let expected_thread = match &request.target {
        MessageTarget::Machine {
            recipient: Recipient::Thread { thread },
            ..
        } => Some(*thread),
        _ => None,
    };
    if response.api_version != API_VERSION
        || response.message_id != request.message_id
        || response.destination_machine.as_uuid().is_nil()
        || response.destination_thread.0.is_nil()
        || !response.destination_cwd.is_absolute()
        || response.receipt.api_version != API_VERSION
        || response.receipt.protocol_version == 0
        || response.receipt.message_id != response.message_id
        || response.receipt.destination_thread != response.destination_thread
        || response.receipt.destination_cwd != response.destination_cwd
        || expected_machine.is_some_and(|machine| machine != response.destination_machine)
        || expected_thread.is_some_and(|thread| thread != response.destination_thread)
    {
        return Err(AppError::Internal {
            message: "message sender returned a receipt that does not match the request".into(),
        });
    }
    if let MessageTarget::Machine {
        recipient: Recipient::Cwd { cwd },
        ..
    } = &request.target
        && cwd.is_absolute()
        && cwd != &response.destination_cwd
    {
        return Err(AppError::Internal {
            message: "message sender returned a different working directory".into(),
        });
    }
    Ok(())
}

fn conversation_id(conversation: Option<Uuid>, message_id: MessageId) -> Uuid {
    conversation.unwrap_or_else(|| message_id.as_uuid())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use serde_json::json;

    #[test]
    fn target_requires_one_complete_destination_form() {
        let task = TaskId::new();
        assert!(matches!(
            target(None, None, None, Some(task)),
            Ok(MessageTarget::Task { task: found }) if found == task
        ));
        assert!(target(Some("code".into()), None, None, None).is_err());
        assert!(target(None, Some(ThreadId(Uuid::now_v7())), None, None).is_err());
        assert!(
            target(
                Some("code".into()),
                Some(ThreadId(Uuid::now_v7())),
                Some(PathBuf::from("~/repo")),
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn response_requires_a_versioned_receipt_for_the_selected_thread() {
        let message_id = MessageId::new();
        let machine = MachineId::new();
        let thread = ThreadId(Uuid::now_v7());
        let request = MessageSendRequest {
            api_version: API_VERSION,
            message_id,
            target: MessageTarget::Machine {
                machine: machine.to_string(),
                recipient: Recipient::Thread { thread },
            },
            source: MessageSourceSelector::Thread {
                thread: ThreadId(Uuid::now_v7()),
            },
            body: "Review this change".into(),
            reply_to: None,
            conversation_id: message_id.as_uuid(),
        };
        let value = json!({
            "api_version": API_VERSION,
            "message_id": message_id,
            "destination_machine": machine,
            "destination_thread": thread,
            "destination_cwd": "/repo",
            "receipt": {
                "api_version": API_VERSION,
                "protocol_version": 1,
                "message_id": message_id,
                "destination_thread": thread,
                "destination_cwd": "/repo",
                "delivered_at": chrono::Utc::now(),
            }
        });
        let response: MessageSendResponse = serde_json::from_value(value).unwrap();
        assert!(validate_response(&request, &response).is_ok());

        let mut changed = response.clone();
        changed.destination_thread = ThreadId(Uuid::now_v7());
        assert!(validate_response(&request, &changed).is_err());

        let mut changed = response;
        changed.destination_machine = MachineId::new();
        assert!(validate_response(&request, &changed).is_err());
    }

    #[test]
    fn omitted_conversation_is_stable_for_an_explicit_message_id_retry() {
        let message_id = MessageId::new();
        let first = conversation_id(None, message_id);
        let retry = conversation_id(None, message_id);
        assert_eq!(first, retry);
        assert_eq!(first, message_id.as_uuid());
    }
}
