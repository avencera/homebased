//! `homebased config` commands.

use std::process::ExitCode;

use clap::Subcommand;
use serde_json::json;

use crate::config::{ConfigLocation, Fleet};
use crate::error::AppError;

use super::{Ctx, OutputMode};

/// Config subcommands.
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum ConfigCommand {
    /// Read and validate the config file. Exits 2 with `config_invalid` when
    /// the file is rejected.
    Validate,
}

/// Dispatch a config command.
pub fn run(ctx: &Ctx, command: ConfigCommand) -> Result<ExitCode, AppError> {
    match command {
        ConfigCommand::Validate => validate(ctx),
    }
}

fn validate(ctx: &Ctx) -> Result<ExitCode, AppError> {
    let location = ctx.config_location()?;
    let config = location.load()?;
    let exists = location.path().is_file();
    let (name, name_source) = config.machine_name();
    let source = match location {
        ConfigLocation::Explicit(_) => "explicit",
        ConfigLocation::Default(_) => "default",
    };
    match ctx.output {
        OutputMode::Json => ctx.print_json(json!({
            "valid": true,
            "path": location.path(),
            "source": source,
            "exists": exists,
            "machine_name": name,
            "machine_name_source": name_source,
            "fleet": config.fleet,
        }))?,
        OutputMode::Quiet => println!("{}", location.path().display()),
        OutputMode::Human => {
            let fleet = match &config.fleet {
                Fleet::Disabled => "disabled".to_string(),
                Fleet::Enabled(settings) => {
                    format!("enabled, {} configured machine(s)", settings.machines.len())
                }
            };
            let presence = if exists { "" } else { " (absent, defaults)" };
            println!("config ok: {}{presence}", location.path().display());
            println!("machine name: {name}");
            println!("fleet: {fleet}");
        }
    }
    Ok(ExitCode::SUCCESS)
}
