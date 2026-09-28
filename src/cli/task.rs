//! `homebased task` commands.

use std::io::{self, Read};
use std::process::ExitCode;
use std::time::Duration;

use clap::Subcommand;
use serde_json::{Value, json};

use crate::callback::destination::SubmitOrigin;
use crate::callback::{last_event_for_row, notify_event};
use crate::client::Client;
use crate::daemon::api::views::TaskFollowupSource;
use crate::daemon::fleet_api::MachinesBody;
use crate::domain::{
    ProcessStatus, ReportOutcome, TASK_NAME_MAX_CHARS, THREAD_ENV_VARS, TaskId, ThreadId,
};
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
        /// Stable UUID for retry after a lost submission response
        #[arg(long)]
        request_id: Option<uuid::Uuid>,
        /// Let a Homebased worker send events to a thread other than its parent task's thread
        #[arg(long)]
        allow_other_thread: bool,
    },
    /// Resume a finished Codex worker with more information.
    Followup {
        /// Full source task UUID.
        task: TaskId,
        /// Prompt text. Exactly one of this or `--message-file` is required.
        #[arg(
            long,
            required_unless_present = "message_file",
            conflicts_with = "message_file"
        )]
        message: Option<String>,
        /// Prompt file. Exactly one of this or `--message` is required.
        #[arg(long, required_unless_present = "message", conflicts_with = "message")]
        message_file: Option<String>,
        /// Thread that receives events for the follow-up task.
        #[arg(long)]
        thread: Option<ThreadId>,
        /// Name for the follow-up task.
        #[arg(long)]
        name: Option<String>,
        /// Validate and print argv; spawn nothing.
        #[arg(long)]
        dry_run: bool,
        /// Stable UUID for retry after a lost submission response
        #[arg(long)]
        request_id: Option<uuid::Uuid>,
        /// Let a Homebased worker send events to a thread other than its parent task's thread
        #[arg(long)]
        allow_other_thread: bool,
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

struct FollowupOptions {
    message: Option<String>,
    message_file: Option<String>,
    thread: Option<ThreadId>,
    name: Option<String>,
    dry_run: bool,
    request_id: Option<uuid::Uuid>,
    allow_other_thread: bool,
}

/// Dispatch a task command.
pub async fn run(ctx: &Ctx, command: TaskCommand) -> Result<ExitCode, AppError> {
    match command {
        TaskCommand::Submit {
            spec,
            dry_run,
            request_id,
            allow_other_thread,
        } => submit(ctx, &spec, dry_run, request_id, allow_other_thread).await,
        TaskCommand::Followup {
            task,
            message,
            message_file,
            thread,
            name,
            dry_run,
            request_id,
            allow_other_thread,
        } => {
            followup(
                ctx,
                task,
                FollowupOptions {
                    message,
                    message_file,
                    thread,
                    name,
                    dry_run,
                    request_id,
                    allow_other_thread,
                },
            )
            .await
        }
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
    allow_other_thread: bool,
) -> Result<ExitCode, AppError> {
    let spec = load_spec(spec_path)?;
    let normalized = spec::normalize(&spec)?;
    submit_normalized(ctx, normalized, dry_run, request_id, allow_other_thread).await
}

async fn submit_normalized(
    ctx: &Ctx,
    normalized: crate::spec::NormalizedSpec,
    dry_run: bool,
    request_id: Option<uuid::Uuid>,
    allow_other_thread: bool,
) -> Result<ExitCode, AppError> {
    if normalized.machine.is_none() {
        spec::check_cwd(&normalized.cwd)?;
    }
    SubmitOrigin::capture(&ctx.home)?.check(normalized.thread, allow_other_thread)?;
    let env = crate::domain::TaskEnv::capture();
    let callback_cwd = std::env::current_dir()?;
    let remote = normalized.machine.is_some();
    let request_id = request_id.map(RequestId).unwrap_or_default();
    let body = json!({
        "spec": normalized,
        "env": env,
        "callback_cwd": callback_cwd,
        "request_id": request_id,
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
    let announce_retries = matches!(ctx.output, super::OutputMode::Human);
    let value = post_submit(&client, &body, remote, announce_retries).await?;
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    ctx.print_id(&id, &format!("submitted {id}"), value)?;
    Ok(ExitCode::SUCCESS)
}

async fn followup(ctx: &Ctx, task: TaskId, options: FollowupOptions) -> Result<ExitCode, AppError> {
    let FollowupOptions {
        message,
        message_file,
        thread,
        name,
        dry_run,
        request_id,
        allow_other_thread,
    } = options;
    let client = Client::new(ctx.home.sock_path());
    let detail = client.get(&format!("/v1/tasks/{task}")).await?;
    let terminal = detail
        .get("status")
        .and_then(Value::as_str)
        .and_then(|status| ProcessStatus::from_storage(status).ok())
        .is_some_and(ProcessStatus::is_terminal);
    if !terminal {
        return Err(followup_unavailable(
            task,
            crate::error::FollowupBlocker::NotTerminal,
        ));
    }
    let (local_machine, execution_machine, machine) =
        followup_machines(&client, task, &detail).await?;
    if detail.get("availability").and_then(Value::as_str) != Some("available") {
        return Err(AppError::TaskUnavailable {
            task,
            machine: execution_machine,
        });
    }

    let source: TaskFollowupSource = serde_json::from_value(
        client
            .get(&format!("/v1/tasks/{task}/followup-source"))
            .await?,
    )
    .map_err(|error| AppError::Internal {
        message: format!("invalid task followup source: {error}"),
    })?;
    if source.api_version != crate::domain::API_VERSION {
        return Err(AppError::Internal {
            message: "task followup source uses an unsupported API version".into(),
        });
    }
    let resume_thread = detail
        .get("worker_thread")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<ThreadId>().ok())
        .ok_or_else(|| followup_unavailable(task, crate::error::FollowupBlocker::NoWorkerThread))?;

    let thread = select_followup_thread(thread)?;
    let name = name.unwrap_or_else(|| followup_name(detail.get("display_name")));
    let prompt = read_followup_message(message, message_file)?;
    let cwd = detail
        .get("cwd")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Internal {
            message: "task detail has no working directory".into(),
        })?;
    let timeout_secs = detail
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .ok_or_else(|| AppError::Internal {
            message: "task detail has no timeout".into(),
        })?;
    let mut submit_spec = json!({
        "api_version": crate::domain::API_VERSION,
        "thread": thread,
        "name": name,
        "cwd": cwd,
        "machine": machine,
        "timeout": format!("{timeout_secs}s"),
        "workload": {
            "type": "agent",
            "agent": "codex",
            "model": source.model,
            "prompt": prompt,
            "extra_args": source.extra_args,
            "report_trailer": source.report_trailer,
            "resume_thread": resume_thread,
        },
    });
    if local_machine == execution_machine
        && let Some(object) = submit_spec.as_object_mut()
    {
        let _ = object.remove("machine");
    }
    let parsed = spec::parse_spec_value(&submit_spec)?;
    let normalized = spec::normalize(&parsed)?;
    submit_normalized(ctx, normalized, dry_run, request_id, allow_other_thread).await
}

fn followup_unavailable(task: TaskId, reason: crate::error::FollowupBlocker) -> AppError {
    AppError::FollowupUnavailable { task, reason }
}

fn select_followup_thread(explicit: Option<ThreadId>) -> Result<ThreadId, AppError> {
    if let Some(thread) = explicit {
        return Ok(thread);
    }
    for key in THREAD_ENV_VARS {
        let Some(value) = std::env::var_os(key) else {
            continue;
        };
        let value = value.to_str().ok_or_else(|| AppError::Usage {
            message: format!("{key} must be a UUID"),
        })?;
        return value.parse().map_err(|error: AppError| AppError::Usage {
            message: format!("{key} must be a thread UUID: {error}"),
        });
    }
    Err(AppError::Usage {
        message:
            "pass --thread or set CODEX_THREAD_ID, CODEX_SESSION_ID, or CLAUDE_CODE_SESSION_ID"
                .into(),
    })
}

fn followup_name(display_name: Option<&Value>) -> String {
    let display_name = display_name.and_then(Value::as_str).unwrap_or("task");
    format!("follow up: {display_name}")
        .chars()
        .take(TASK_NAME_MAX_CHARS)
        .collect()
}

fn read_followup_message(
    message: Option<String>,
    message_file: Option<String>,
) -> Result<String, AppError> {
    match (message, message_file) {
        (Some(message), None) => Ok(message),
        (None, Some(path)) => {
            std::fs::read_to_string(&path).map_err(|error| AppError::FileNotFound {
                message: format!("read follow-up message {path}: {error}"),
            })
        }
        _ => Err(AppError::Usage {
            message: "exactly one of --message or --message-file is required".into(),
        }),
    }
}

async fn followup_machines(
    client: &Client,
    task: TaskId,
    detail: &Value,
) -> Result<
    (
        crate::machine::MachineId,
        crate::machine::MachineId,
        Option<String>,
    ),
    AppError,
> {
    let origin_machine = detail
        .get("origin_machine")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Usage {
            message: "task detail does not identify its origin machine".into(),
        })?
        .parse::<crate::machine::MachineId>()?;
    let execution_machine = detail
        .get("execution_machine")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Usage {
            message: "task detail does not identify its execution machine".into(),
        })?
        .parse::<crate::machine::MachineId>()?;
    let inventory: MachinesBody = serde_json::from_value(client.get("/v1/fleet/machines").await?)
        .map_err(|error| AppError::Internal {
        message: format!("invalid Fleet machine list: {error}"),
    })?;
    if inventory.local.machine != origin_machine && inventory.local.machine != execution_machine {
        return Err(AppError::FollowupWrongMachine {
            task,
            origin_machine,
            execution_machine,
        });
    }
    let machine = if inventory.local.machine == execution_machine {
        None
    } else {
        let peer = inventory
            .machines
            .iter()
            .find(|peer| peer.machine == execution_machine)
            .ok_or_else(|| AppError::MachineNotFound {
                machine: execution_machine.to_string(),
            })?;
        Some(peer.name.to_string())
    };
    Ok((inventory.local.machine, execution_machine, machine))
}

/// Waits between local submit attempts. The daemon has stalled for over a
/// minute when the disk was slow, so the schedule covers about 90 seconds
const LOCAL_SUBMIT_RETRY_DELAYS: [Duration; 6] = [
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(15),
    Duration::from_secs(30),
    Duration::from_secs(30),
];

/// Post a submit, retrying a local one with the same request UUID while its outcome is unknown
///
/// The request UUID makes the retry safe: the daemon returns the task an earlier
/// attempt saved instead of creating another. A remote submit returns its
/// unknown outcome at once, because a slow peer can keep it unknown for much longer
async fn post_submit(
    client: &Client,
    body: &Value,
    remote: bool,
    announce_retries: bool,
) -> Result<Value, AppError> {
    let delays: &[Duration] = if remote {
        &[]
    } else {
        &LOCAL_SUBMIT_RETRY_DELAYS
    };
    let mut delays = delays.iter();
    loop {
        let error = match client.post("/v1/tasks", body).await {
            Err(error) if retries_local_submit(&error) => error,
            result => return result,
        };
        let Some(delay) = delays.next() else {
            return Err(error);
        };
        // JSON and quiet modes keep stderr to the final error envelope
        if announce_retries {
            eprintln!("warning: {error}; retrying in {}s", delay.as_secs());
        }
        tokio::time::sleep(*delay).await;
    }
}

fn retries_local_submit(error: &AppError) -> bool {
    matches!(
        error,
        AppError::SubmissionOutcomeUnknown { .. } | AppError::DaemonBusy
    )
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
            if let Some(worker_thread) = value.get("worker_thread").and_then(Value::as_str) {
                println!("  worker thread {worker_thread}");
            }
            print_waiting_events(&value);
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// One line per callback that waits for its origin thread and when the wait ends
fn print_waiting_events(value: &Value) {
    let Some(events) = value.get("waiting_events").and_then(Value::as_array) else {
        return;
    };
    for event in events {
        let field = |name| event.get(name).and_then(Value::as_str).unwrap_or("-");
        println!(
            "  callback seq {} waits for its thread since {}, gives up at {}: {}",
            event.get("seq").and_then(Value::as_u64).unwrap_or(0),
            field("since"),
            field("until"),
            field("reason"),
        );
    }
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
        Some("container") => match workload.get("image").and_then(Value::as_str) {
            Some(image) => format!("container {}", image.split('@').next().unwrap_or(image)),
            None => "container".into(),
        },
        _ => "-".into(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::{report, retries_local_submit};
    use crate::cli::{Ctx, OutputMode};
    use crate::domain::{
        Agent, AgentKind, AgentWorkload, ReportOutcome, TaskEnv, TaskId, ThreadId, Workload,
    };
    use crate::error::AppError;
    use crate::home::Home;
    use crate::machine::MachineId;
    use crate::store::{NewTask, Store, new_queued_task};

    #[test]
    fn local_submit_retries_only_unknown_outcomes() {
        let unknown = AppError::SubmissionOutcomeUnknown {
            request: crate::submission::RequestId::new(),
            task: TaskId::new(),
            message: "busy".into(),
        };
        assert!(retries_local_submit(&unknown));
        assert!(retries_local_submit(&AppError::DaemonBusy));
        assert!(!retries_local_submit(&AppError::Internal {
            message: "boom".into()
        }));
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
                resume_thread: None,
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
