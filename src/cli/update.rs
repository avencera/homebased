//! `homebased update`: replace the binary and restart serve plus dashboard.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::client::Client;
use crate::error::AppError;
use crate::update::{self, UpdateRequest};

use super::daemon;
use super::{Ctx, OutputMode};

/// Replace this binary from a GitHub release and restart the daemon.
#[derive(Debug, clap::Args)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub struct UpdateArgs {
    /// Release tag (for example `v0.2.0`). Default: latest.
    #[arg(long)]
    pub tag: Option<String>,
    /// GitHub repository that hosts the release.
    #[arg(long, default_value = update::DEFAULT_GIT)]
    pub git: String,
    /// Directory for the `homebased` binary.
    #[arg(long)]
    pub to: Option<PathBuf>,
    /// Resolve the release without installing or restarting.
    #[arg(long)]
    pub dry_run: bool,
    /// Override the GitHub origin (tests).
    #[arg(long, env = "HOMEBASED_UPDATE_BASE_URL", hide = true)]
    pub base_url: Option<String>,
}

/// Download the release, replace the binary, and restart serve (and the dashboard).
pub async fn run(ctx: &Ctx, args: UpdateArgs) -> Result<ExitCode, AppError> {
    let plan = update::plan(&UpdateRequest {
        git: args.git,
        tag: args.tag,
        dest_dir: args.to,
        base_url: args.base_url,
    })?;
    if args.dry_run {
        print_result(
            ctx,
            &plan,
            false,
            true,
            None,
            Some(env!("CARGO_PKG_VERSION")),
        )?;
        return Ok(ExitCode::SUCCESS);
    }

    let previous_version = env!("CARGO_PKG_VERSION");
    update::install(&plan)?;
    daemon::restart_daemon(ctx).await?;
    let status = wait_status(ctx, update::STATUS_WAIT).await;
    let web = status
        .as_ref()
        .and_then(|body| body.get("web"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    print_result(
        ctx,
        &plan,
        true,
        false,
        web.as_deref(),
        Some(previous_version),
    )?;
    Ok(ExitCode::SUCCESS)
}

fn print_result(
    ctx: &Ctx,
    plan: &update::UpdatePlan,
    restarted: bool,
    dry_run: bool,
    web: Option<&str>,
    previous_version: Option<&str>,
) -> Result<(), AppError> {
    match ctx.output {
        OutputMode::Quiet => {
            println!("{}", plan.dest.display());
            Ok(())
        }
        OutputMode::Json => ctx.print_json(json!({
            "api_version": crate::domain::API_VERSION,
            "tag": plan.tag,
            "target": plan.target,
            "path": plan.dest,
            "url": plan.url,
            "previous_version": previous_version,
            "restarted": restarted,
            "dry_run": dry_run,
            "web": web,
        })),
        OutputMode::Human => {
            if dry_run {
                println!(
                    "would install {} to {} ({})",
                    plan.tag,
                    plan.dest.display(),
                    plan.target
                );
            } else {
                println!("installed {} to {}", plan.tag, plan.dest.display());
            }
            if restarted {
                println!("restarted  web: {}", web.unwrap_or("down"));
            }
            Ok(())
        }
    }
}

async fn wait_status(ctx: &Ctx, budget: Duration) -> Option<Value> {
    let client = Client::new(ctx.home.sock_path());
    let start = Instant::now();
    loop {
        if let Ok(body) = client.get("/v1/status").await {
            return Some(body);
        }
        if start.elapsed() > budget {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
