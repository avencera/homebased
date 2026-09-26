//! Commands for `homebased t3`

use std::process::ExitCode;

use clap::Subcommand;
use serde_json::to_value;

use crate::error::AppError;
use crate::t3::{ProbeReport, ProbeStatus, T3Env, probe};

use super::{Ctx, OutputMode};

/// T3 Code commands
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum T3Command {
    /// Check the installed T3 Code local API
    Check,
}

/// Run one T3 Code command
pub async fn run(ctx: &Ctx, command: T3Command) -> Result<ExitCode, AppError> {
    match command {
        T3Command::Check => check(ctx).await,
    }
}

async fn check(ctx: &Ctx) -> Result<ExitCode, AppError> {
    let report = tokio::task::spawn_blocking(|| probe(&T3Env::from_env()))
        .await
        .map_err(|error| AppError::Internal {
            message: format!("T3 probe worker failed: {error}"),
        })?;
    print_report(ctx, &report)?;
    if report.status == ProbeStatus::Changed {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

fn print_report(ctx: &Ctx, report: &ProbeReport) -> Result<(), AppError> {
    match ctx.output {
        OutputMode::Json => ctx.print_json(to_value(report)?),
        OutputMode::Quiet => {
            println!("{}", report.status.as_str());
            Ok(())
        }
        OutputMode::Human => {
            println!("t3 check: {}", report.status.as_str());
            for check in &report.checks {
                let result = if check.ok { "ok" } else { "failed" };
                println!("{result} {}: {}", check.name, check.detail);
            }
            Ok(())
        }
    }
}
