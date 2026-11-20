//! `homebased gc`: prune finished output now, under the `[retention]` settings

use std::process::ExitCode;

use clap::Args;
use serde_json::json;

use super::{Ctx, OutputMode};
use crate::client::Client;
use crate::error::AppError;
use crate::retention::{GcReport, OutputKind};

/// Largest pruned entries a human report lists
const LISTED: usize = 15;

/// Prune the output of finished tasks, jobs, and idle CI slot clones now
///
/// The daemon also does this on `retention.interval`. Only output whose task or
/// job has ended, whose events are settled, and that no waiting task, job,
/// chain, or sleeping thread still needs is pruned. A pruned task keeps its
/// row, events, and reports, and `task log` says when its output went
#[derive(Debug, Args)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub struct GcArgs {
    /// Print what would be pruned and how much it would free, and remove nothing
    #[arg(long)]
    pub dry_run: bool,
    /// After pruning, rebuild the database to return its free pages to disk;
    /// the daemon's other database work waits while it runs
    #[arg(long, conflicts_with = "dry_run")]
    pub vacuum: bool,
}

/// Run one pass in the daemon and print its report
pub async fn run(ctx: &Ctx, args: GcArgs) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    let body = json!({ "dry_run": args.dry_run, "vacuum": args.vacuum });
    let value = client.post("/v1/gc", &body).await?;
    let report: GcReport = serde_json::from_value(value.clone())?;
    match ctx.output {
        OutputMode::Json => ctx.print_json(value)?,
        OutputMode::Quiet => println!("{}", report.freed()),
        OutputMode::Human => print_human(&report),
    }

    Ok(if report.failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

fn print_human(report: &GcReport) {
    let verb = if report.dry_run {
        "would free"
    } else {
        "freed"
    };
    println!("{verb} {}", size(report.freed()));
    println!();
    println!(
        "{:<9} {:>8} {:>10} {:>7} {:>10} {:>6} {:>7} {:>6}",
        "kind", "entries", "size", "pruned", "freed", "live", "recent", "young"
    );
    for kind in &report.kinds {
        println!(
            "{:<9} {:>8} {:>10} {:>7} {:>10} {:>6} {:>7} {:>6}",
            kind_label(kind.kind),
            kind.entries,
            size(kind.bytes),
            kind.pruned,
            size(kind.pruned_bytes),
            kind.kept_live,
            kind.kept_recent,
            kind.kept_young,
        );
    }

    let database = report.database;
    match database.vacuumed_bytes {
        Some(after) => println!(
            "\ndatabase {} -> {} after vacuum",
            size(database.bytes),
            size(after)
        ),
        None => println!(
            "\ndatabase {}, {} free (`homebased gc --vacuum` returns it)",
            size(database.bytes),
            size(database.free_bytes)
        ),
    }

    if !report.pruned.is_empty() {
        let heading = if report.dry_run {
            "largest to prune"
        } else {
            "largest pruned"
        };
        println!("\n{heading}:");
        for entry in report.pruned.iter().take(LISTED) {
            let ended = entry.ended_at.map_or_else(String::new, |at| {
                format!("  ended {}", at.format("%Y-%m-%d"))
            });
            println!(
                "  {:>8}  {:<8} {:<8} {}{ended}",
                size(entry.bytes),
                kind_label(entry.kind),
                entry.reason.as_str(),
                entry.path.display(),
            );
        }
        if report.pruned.len() > LISTED {
            println!(
                "  ... and {} more (`--json` lists all)",
                report.pruned.len() - LISTED
            );
        }
    }

    for failure in &report.failures {
        eprintln!(
            "could not remove {}: {}",
            failure.path.display(),
            failure.error
        );
    }
}

const fn kind_label(kind: OutputKind) -> &'static str {
    match kind {
        OutputKind::Task => "tasks",
        OutputKind::Job => "jobs",
        OutputKind::Checkout => "ci",
    }
}

/// Bytes in the largest binary unit that keeps the number at least 1
fn size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    // display only; precision loss above 2^53 bytes does not matter
    #[allow(clippy::cast_precision_loss)]
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    match unit {
        0 => format!("{bytes}B"),
        _ => format!("{value:.1}{}", UNITS[unit]),
    }
}
