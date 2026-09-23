//! `homebased task` commands.

use std::io::{self, Read};
use std::process::ExitCode;

use clap::Subcommand;
use serde_json::{Value, json};

use crate::callback::{last_event_for_row, notify_event};
use crate::client::Client;
use crate::domain::{ProcessStatus, ReportOutcome, TaskId, ThreadId};
use crate::error::AppError;
use crate::spec::{self, load_spec};
use crate::store::Store;
use crate::submission::RequestId;

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
        /// Stable UUID for retry after a lost remote submission response
        #[arg(long)]
        request_id: Option<uuid::Uuid>,
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
        TaskCommand::Submit {
            spec,
            dry_run,
            request_id,
        } => submit(ctx, &spec, dry_run, request_id).await,
        TaskCommand::Schema => schema(ctx),
        TaskCommand::List { status, thread } => list(ctx, status, thread).await,
        TaskCommand::Show { id } => show(ctx, id).await,
        TaskCommand::Log { id, tail } => log_cmd(ctx, id, tail).await,
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

async fn submit(
    ctx: &Ctx,
    spec_path: &str,
    dry_run: bool,
    request_id: Option<uuid::Uuid>,
) -> Result<ExitCode, AppError> {
    let spec = load_spec(spec_path)?;
    let normalized = spec::normalize(&spec)?;
    if normalized.machine.is_none() {
        spec::check_cwd(&normalized.cwd)?;
    }
    let env = crate::domain::TaskEnv::capture();
    let callback_cwd = std::env::current_dir()?;
    let request_id = submission_request_id(normalized.machine.is_some(), request_id)?;
    let mut body = json!({
        "spec": normalized,
        "env": env,
        "callback_cwd": callback_cwd,
    });
    include_request_id(&mut body, request_id);

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

fn submission_request_id(
    remote: bool,
    request_id: Option<uuid::Uuid>,
) -> Result<Option<RequestId>, AppError> {
    if !remote && request_id.is_some() {
        return Err(AppError::Usage {
            message: "--request-id is only valid for a remote submission; set machine in the spec or omit --request-id".into(),
        });
    }

    Ok(remote.then(|| request_id.map(RequestId).unwrap_or_default()))
}

fn include_request_id(body: &mut Value, request_id: Option<RequestId>) {
    if let Some(request_id) = request_id {
        body["request_id"] = json!(request_id);
    }
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
                        workload_label(t),
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
            let check = value
                .get("check_timeout")
                .and_then(Value::as_str)
                .unwrap_or("-");
            println!(
                "{id}  {}  {}  check timeout {}",
                value.get("status").and_then(Value::as_str).unwrap_or("-"),
                workload_label(&value),
                check
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn log_cmd(ctx: &Ctx, id: TaskId, tail: Option<usize>) -> Result<ExitCode, AppError> {
    let path = match tail {
        Some(tail) => format!("/v1/tasks/{id}/log?tail={tail}"),
        None => format!("/v1/tasks/{id}/log"),
    };
    let value = Client::new(ctx.home.sock_path()).get(&path).await?;
    let text = value
        .get("log")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Internal {
            message: "daemon returned no task log".into(),
        })?
        .to_string();
    match ctx.output {
        super::OutputMode::Json => ctx.print_json(value)?,
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
    let message = if value.get("delivery").is_some() {
        format!("cancellation accepted for {id}")
    } else {
        format!("cancelled {id}")
    };
    ctx.print_id(&id.to_string(), &message, value)?;
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
    let reports = store.append_report_with_notification(id, outcome, &summary, notify)?;
    let seq = reports.last().map_or(0, |r| r.seq);
    let row = store.require_task(id)?;
    let last_event = match (notify, reports.last()) {
        (true, Some(report)) => Some(notify_event(&row, report, ctx.home.task_dir(id))),
        _ => last_event_for_row(&row, &reports, ctx.home.task_dir(id)),
    };
    ctx.print_id(
        &seq.to_string(),
        &format!("reported seq={seq}"),
        json!({
            "api_version": crate::domain::API_VERSION,
            "id": id,
            "seq": seq,
            "reports": reports,
            "last_event": last_event,
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

fn workload_label(value: &Value) -> String {
    if let Some(name) = value.get("display_name").and_then(Value::as_str)
        && !name.is_empty()
    {
        return name.to_string();
    }
    let Some(workload) = value.get("workload") else {
        return "-".into();
    };
    match workload.get("type").and_then(Value::as_str) {
        Some("agent") => {
            let agent = workload.get("agent").and_then(Value::as_str).unwrap_or("?");
            match workload.get("model").and_then(Value::as_str) {
                Some(model) => format!("{agent}/{model}"),
                None => agent.to_string(),
            }
        }
        Some("task") => {
            let Some(command) = workload.get("command").and_then(Value::as_array) else {
                return "task".into();
            };
            let parts: Vec<&str> = command.iter().take(3).filter_map(Value::as_str).collect();
            if command.len() > 3 {
                format!("{}…", parts.join(" "))
            } else {
                parts.join(" ")
            }
        }
        _ => "-".into(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::Duration;

    use serde_json::json;
    use tempfile::tempdir;

    use super::{include_request_id, report, submission_request_id};
    use crate::cli::{Ctx, OutputMode};
    use crate::domain::{
        Agent, AgentKind, AgentWorkload, ReportOutcome, TaskEnv, TaskId, ThreadId, Workload,
    };
    use crate::home::Home;
    use crate::machine::MachineId;
    use crate::store::{NewTask, Store, new_queued_task};

    #[test]
    fn local_submission_has_no_request_id() {
        let mut body = serde_json::json!({});
        include_request_id(&mut body, submission_request_id(false, None).unwrap());
        assert!(body.get("request_id").is_none());
    }

    #[test]
    fn remote_submission_generates_or_preserves_request_id() {
        let generated = submission_request_id(true, None).unwrap().unwrap();
        assert_ne!(generated.0, uuid::Uuid::nil());
        let mut body = serde_json::json!({});
        include_request_id(&mut body, Some(generated));
        assert_eq!(body["request_id"], json!(generated));

        let explicit = uuid::Uuid::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap();
        assert_eq!(
            submission_request_id(true, Some(explicit)).unwrap(),
            Some(crate::submission::RequestId(explicit))
        );
    }

    #[test]
    fn explicit_request_id_is_rejected_for_local_submission() {
        let explicit = uuid::Uuid::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap();
        assert!(matches!(
            submission_request_id(false, Some(explicit)),
            Err(crate::error::AppError::Usage { .. })
        ));
    }

    #[test]
    fn notify_report_while_daemon_is_down_is_migrated_once() {
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let id = TaskId::new();
        let row = new_queued_task(NewTask {
            id,
            name: None,
            thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
            workload: Workload::Agent(AgentWorkload {
                agent: Agent::new(AgentKind::Claude, None),
                extra_args: Vec::new(),
                report_trailer: false,
            }),
            cwd: directory.path().to_path_buf(),
            timeout: Duration::from_secs(4 * 3600),
            env: TaskEnv {
                path: "/bin".into(),
                home: directory.path().display().to_string(),
            },
            binary: std::path::PathBuf::from("/bin/true"),
        });
        {
            let store = Store::open(&home.db_path()).unwrap();
            store.insert_task(&row).unwrap();
            store
                .cas_status(
                    id,
                    crate::domain::ProcessStatus::Queued,
                    crate::domain::ProcessStatus::Running,
                )
                .unwrap();
        }

        let context = Ctx {
            output: OutputMode::Quiet,
            home,
            config: None,
        };
        report(
            &context,
            Some(id),
            ReportOutcome::Blocked,
            Some("notification requested offline".into()),
            None,
            true,
        )
        .unwrap();

        let conn = rusqlite::Connection::open(context.home.db_path()).unwrap();
        let intent_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM report_notification_intents WHERE task_id=?1",
                [id.to_string()],
                |entry| entry.get(0),
            )
            .unwrap();
        assert_eq!(intent_count, 1);

        let mut store = Store::open(&context.home.db_path()).unwrap();
        assert!(!store.is_event_task(id).unwrap());

        store.migrate_legacy_local(MachineId::new()).unwrap();
        assert_eq!(store.inbound_events(id).unwrap().len(), 1);
        assert_eq!(
            store
                .origin_route_by_task(id)
                .unwrap()
                .unwrap()
                .last_accepted_seq,
            1
        );
        store.migrate_legacy_local(MachineId::new()).unwrap();
        assert_eq!(store.inbound_events(id).unwrap().len(), 1);
        assert!(store.pending_outbound_events(id).unwrap().is_empty());
    }
}
