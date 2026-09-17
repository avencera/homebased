//! `homebased task` commands.

use std::io::{self, Read};
use std::process::ExitCode;

use clap::Subcommand;
use serde_json::{Value, json};

use crate::callback::{deliver_notify, last_event_for_row, notify_event};
use crate::client::Client;
use crate::domain::{ProcessStatus, ReportOutcome, TaskId, ThreadId};
use crate::error::AppError;
use crate::home::OutputTail;
use crate::spec::{self, load_spec};
use crate::store::Store;

use super::Ctx;

/// Task subcommands.
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum TaskCommand {
    /// Submit a JSON spec from a file or stdin (`-`).
    Submit {
        /// Spec file, or `-` for stdin.
        #[arg(long)]
        spec: String,
        /// Validate and print argv; spawn nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Print the JSON Schema for the submit spec.
    Schema,
    /// List tasks.
    List {
        /// Filter by process status. Repeat or comma-separate.
        #[arg(long, value_enum, value_delimiter = ',')]
        status: Vec<ProcessStatus>,
        /// Filter by Codex thread id.
        #[arg(long)]
        thread: Option<ThreadId>,
    },
    /// Show one task.
    Show {
        /// Full task UUID.
        id: TaskId,
    },
    /// Print `output.log`.
    Log {
        /// Full task UUID.
        id: TaskId,
        /// Last N lines.
        #[arg(long)]
        tail: Option<usize>,
    },
    /// Cancel a task. Idempotent if already terminal.
    Cancel {
        /// Full task UUID.
        id: TaskId,
    },
    /// Append a worker report. Writes SQLite directly.
    Report {
        /// Task id. Defaults to `HOMEBASED_TASK_ID`.
        #[arg(long)]
        id: Option<TaskId>,
        /// Outcome.
        #[arg(long, value_enum)]
        outcome: ReportOutcome,
        /// Summary text.
        #[arg(long, conflicts_with = "summary_file")]
        summary: Option<String>,
        /// Summary file, or `-` for stdin.
        #[arg(long)]
        summary_file: Option<String>,
        /// Send an interim `TASK_REPORTED` event.
        #[arg(long)]
        notify: bool,
    },
}

/// Dispatch a task command.
pub async fn run(ctx: &Ctx, command: TaskCommand) -> Result<ExitCode, AppError> {
    match command {
        TaskCommand::Submit { spec, dry_run } => submit(ctx, &spec, dry_run).await,
        TaskCommand::Schema => schema(ctx),
        TaskCommand::List { status, thread } => list(ctx, status, thread).await,
        TaskCommand::Show { id } => show(ctx, id).await,
        TaskCommand::Log { id, tail } => log_cmd(ctx, id, tail),
        TaskCommand::Cancel { id } => cancel(ctx, id).await,
        TaskCommand::Report {
            id,
            outcome,
            summary,
            summary_file,
            notify,
        } => report(ctx, id, outcome, summary, summary_file, notify),
    }
}

async fn submit(ctx: &Ctx, spec_path: &str, dry_run: bool) -> Result<ExitCode, AppError> {
    let spec = load_spec(spec_path)?;
    let normalized = spec::normalize(&spec)?;
    spec::check_cwd(&normalized.cwd)?;
    let env = crate::domain::TaskEnv::capture();
    let body = json!({
        "spec": normalized,
        "env": env,
    });
    let client = Client::new(ctx.home.sock_path());
    if dry_run {
        let value = client.post("/v1/tasks/dry-run", &body).await?;
        match ctx.output {
            super::OutputMode::Quiet => {
                if let Some(argv) = value.get("argv").and_then(Value::as_array) {
                    for a in argv {
                        if let Some(s) = a.as_str() {
                            println!("{s}");
                        }
                    }
                }
            }
            _ => ctx.print_json(value)?,
        }
        return Ok(ExitCode::SUCCESS);
    }
    let value = client.post("/v1/tasks", &body).await?;
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    ctx.print_id(&id, &format!("submitted {id}"), value)?;
    Ok(ExitCode::SUCCESS)
}

fn schema(ctx: &Ctx) -> Result<ExitCode, AppError> {
    let schema = spec::schema_json()?;
    match ctx.output {
        super::OutputMode::Quiet => println!(
            "{}",
            schema.get("$schema").and_then(Value::as_str).unwrap_or("")
        ),
        _ => {
            println!("{}", serde_json::to_string_pretty(&schema)?);
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn list(
    ctx: &Ctx,
    status: Vec<ProcessStatus>,
    thread: Option<ThreadId>,
) -> Result<ExitCode, AppError> {
    let mut path = String::from("/v1/tasks?");
    if !status.is_empty() {
        path.push_str("status=");
        path.push_str(
            &status
                .iter()
                .map(ProcessStatus::as_str)
                .collect::<Vec<_>>()
                .join(","),
        );
        path.push('&');
    }
    if let Some(thread) = thread {
        path.push_str("thread=");
        path.push_str(&thread.to_string());
    }
    let client = Client::new(ctx.home.sock_path());
    let value = client
        .get(path.trim_end_matches('?').trim_end_matches('&'))
        .await?;
    match ctx.output {
        super::OutputMode::Quiet => {
            if let Some(tasks) = value.get("tasks").and_then(Value::as_array) {
                for t in tasks {
                    if let Some(id) = t.get("id").and_then(Value::as_str) {
                        println!("{id}");
                    }
                }
            }
        }
        super::OutputMode::Json => ctx.print_json(value)?,
        super::OutputMode::Human => {
            if let Some(tasks) = value.get("tasks").and_then(Value::as_array) {
                for t in tasks {
                    println!(
                        "{}\t{}\t{}\t{}",
                        t.get("id").and_then(Value::as_str).unwrap_or("-"),
                        t.get("status").and_then(Value::as_str).unwrap_or("-"),
                        t.get("agent").and_then(Value::as_str).unwrap_or("-"),
                        t.get("thread").and_then(Value::as_str).unwrap_or("-"),
                    );
                }
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn show(ctx: &Ctx, id: TaskId) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    let value = client.get(&format!("/v1/tasks/{id}")).await?;
    match ctx.output {
        super::OutputMode::Quiet => println!("{id}"),
        super::OutputMode::Json => ctx.print_json(value)?,
        super::OutputMode::Human => {
            println!(
                "{id}  {}  {}",
                value.get("status").and_then(Value::as_str).unwrap_or("-"),
                value.get("agent").and_then(Value::as_str).unwrap_or("-")
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn log_cmd(ctx: &Ctx, id: TaskId, tail: Option<usize>) -> Result<ExitCode, AppError> {
    // this command reads the disk, not the socket, so a missing log is the only
    // signal it has that the id is unknown
    let OutputTail { text, truncated } = ctx
        .home
        .task_paths(id)
        .read_output(tail)?
        .ok_or(AppError::TaskNotFound { id })?;
    match ctx.output {
        super::OutputMode::Json => ctx.print_json(json!({
            "api_version": crate::domain::API_VERSION,
            "id": id,
            "log": text,
            "truncated": truncated,
        }))?,
        _ => print!("{text}"),
    }
    if !text.ends_with('\n') && ctx.output != super::OutputMode::Json {
        println!();
    }
    Ok(ExitCode::SUCCESS)
}

async fn cancel(ctx: &Ctx, id: TaskId) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    let value = client
        .post(&format!("/v1/tasks/{id}/cancel"), &json!({}))
        .await?;
    ctx.print_id(&id.to_string(), &format!("cancelled {id}"), value)?;
    Ok(ExitCode::SUCCESS)
}

fn report(
    ctx: &Ctx,
    id: Option<TaskId>,
    outcome: ReportOutcome,
    summary: Option<String>,
    summary_file: Option<String>,
    notify: bool,
) -> Result<ExitCode, AppError> {
    let id = match id {
        Some(id) => id,
        None => std::env::var("HOMEBASED_TASK_ID")
            .map_err(|_| AppError::Usage {
                message: "missing --id and HOMEBASED_TASK_ID".into(),
            })?
            .parse()?,
    };
    let summary = match (summary, summary_file) {
        (Some(text), None) => text,
        (None, Some(path)) => read_summary(&path)?,
        _ => {
            return Err(AppError::Usage {
                message: "exactly one of --summary or --summary-file is required".into(),
            });
        }
    };
    let store = Store::open(&ctx.home.db_path())?;
    let reports = store.append_report(id, outcome, &summary)?;
    if notify {
        let row = store.require_task(id)?;
        if let Some(report) = reports.last() {
            let event = notify_event(&row, report, ctx.home.task_dir(id));
            match deliver_notify(&ctx.home, &row, &event) {
                Ok(()) => store.mark_notified(id, report.seq)?,
                // an interim notify is best-effort: the exit callback still carries the report
                Err(err) => eprintln!("warning: notify failed: {err}"),
            }
        }
    }
    let seq = reports.last().map_or(0, |r| r.seq);
    ctx.print_id(
        &seq.to_string(),
        &format!("reported seq={seq}"),
        json!({
            "api_version": crate::domain::API_VERSION,
            "id": id,
            "seq": seq,
            "reports": reports,
            "last_event": last_event_for_row(
                &store.require_task(id)?,
                &reports,
                ctx.home.task_dir(id),
            ),
        }),
    )?;
    Ok(ExitCode::SUCCESS)
}

fn read_summary(path: &str) -> Result<String, AppError> {
    if path == "-" {
        let mut buf = String::new();
        io::stdin().read_to_string(&mut buf)?;
        Ok(buf)
    } else {
        std::fs::read_to_string(path).map_err(|err| AppError::Internal {
            message: format!("read {path}: {err}"),
        })
    }
}
