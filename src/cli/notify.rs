//! `homebased notify` commands

use std::process::ExitCode;

use clap::Subcommand;
use serde_json::json;

use crate::error::AppError;
use crate::notify::{Notice, NoticePriority, Notifier};

use super::{Ctx, OutputMode};

/// Notification subcommands
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum NotifyCommand {
    /// Send one test push through the configured ntfy server
    Test {
        /// Replace the default test message
        #[arg(long)]
        message: Option<String>,
    },
}

/// Run one notification command
pub async fn run(ctx: &Ctx, command: NotifyCommand) -> Result<ExitCode, AppError> {
    match command {
        NotifyCommand::Test { message } => test(ctx, message).await,
    }
}

async fn test(ctx: &Ctx, message: Option<String>) -> Result<ExitCode, AppError> {
    let config = ctx.config_location()?.load()?;
    let notifier = Notifier::from_config(&config).ok_or(AppError::NotifyNotConfigured)?;
    let topic = config
        .notify
        .ntfy
        .as_ref()
        .map(|settings| settings.topic().to_string())
        .ok_or(AppError::NotifyNotConfigured)?;
    let (machine_name, _) = config.machine_name();
    let message = match message {
        Some(message) => format!("{message} (homebased on {machine_name})"),
        None => format!("Test notification from homebased on {machine_name}"),
    };
    let notice = Notice {
        title: "homebased test".into(),
        message,
        tags: vec!["test_tube".into()],
        priority: NoticePriority::Default,
    };

    tokio::task::spawn_blocking(move || notifier.send(&notice))
        .await
        .map_err(|error| AppError::NotifyFailed {
            message: format!("notification worker failed: {error}"),
        })?
        .map_err(|message| AppError::NotifyFailed { message })?;

    match ctx.output {
        OutputMode::Quiet => println!("ok"),
        OutputMode::Json => ctx.print_json(json!({ "sent": true, "topic": topic }))?,
        OutputMode::Human => println!("test notification sent to {topic}"),
    }
    Ok(ExitCode::SUCCESS)
}
