//! Resource queue commands, with stable identities for transport retries

use std::io::{self, Read};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Subcommand};
use serde_json::{Value, json};

use super::{Ctx, OutputMode};
use crate::callback::destination::SubmitOrigin;
use crate::client::Client;
use crate::domain::TaskEnv;
use crate::error::AppError;
use crate::queue::spec::{JobSpec, MachineSelector};
use crate::queue::{
    AttentionId, JobId, MoveFlags, OperationId, Placement, Priority, QueueError, ResourceName,
};

/// Resource and machine queue commands
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum ResourceCommand {
    /// Add a hand-managed resource on this machine
    Register {
        /// Unique machine-local name
        #[arg(long)]
        name: ResourceName,
        /// GPU index exported to runs
        #[arg(long)]
        device: Option<u32>,
    },
    /// Show resources and their held run phases
    List(MachineArgs),
    /// Show the machine queue in serving order
    Jobs(MachineArgs),
    /// Print the JSON schema for JobSpec
    Schema,
    /// Submit, inspect, move, and cancel jobs
    Job {
        /// Job command
        #[command(subcommand)]
        command: JobCommand,
    },
    /// Release one exact Attention after checking the machine
    Release {
        /// Attention identity shown by resource list or job show
        #[arg(long)]
        attention: AttentionId,
        /// Replay identity; generated once when omitted
        #[arg(long)]
        operation_id: Option<OperationId>,
        /// Machine whose resource needs release
        #[command(flatten)]
        machine: MachineArgs,
    },
}

/// Optional machine selector, defaulting to local or a job's saved authority
#[derive(Debug, Args)]
pub struct MachineArgs {
    /// Fleet name or UUID; job commands use their saved route when omitted
    #[arg(long)]
    pub machine: Option<MachineSelector>,
}

/// Job commands
#[derive(Debug, Subcommand)]
pub enum JobCommand {
    /// Submit a strict JobSpec from a file or stdin
    Submit {
        /// Stable UUID for retries after a lost response
        #[arg(long)]
        job_id: JobId,
        /// Spec file, or - for stdin
        #[arg(long)]
        spec: String,
        /// Let a worker send callbacks to a thread other than its parent's thread
        #[arg(long)]
        allow_other_thread: bool,
    },
    /// Show a job, its progress, and every run task for logs
    Show {
        /// Job UUID
        job: JobId,
        /// Queue authority when submitted from another origin
        #[command(flatten)]
        machine: MachineArgs,
    },
    /// Move a non-terminal job within or between levels
    Move(MoveArgs),
    /// Cancel a queued job or stop and clean up its active run
    Cancel {
        /// Job UUID
        job: JobId,
        /// Replay identity; generated once when omitted
        #[arg(long)]
        operation_id: Option<OperationId>,
        /// Queue authority when submitted from another origin
        #[command(flatten)]
        machine: MachineArgs,
    },
}

/// Exactly one placement is validated before sending a move
#[derive(Debug, Args)]
pub struct MoveArgs {
    /// Job UUID
    pub job: JobId,
    /// Move to the front of the level
    #[arg(long)]
    pub front: bool,
    /// Move to the back of the level
    #[arg(long)]
    pub back: bool,
    /// New level; without other flags, join its back
    #[arg(long)]
    pub priority: Option<Priority>,
    /// Move immediately before this job, taking its level
    #[arg(long)]
    pub before: Option<JobId>,
    /// Move immediately after this job, taking its level
    #[arg(long)]
    pub after: Option<JobId>,
    /// Replay identity; generated once when omitted
    #[arg(long)]
    pub operation_id: Option<OperationId>,
    /// Queue authority when submitted from another origin
    #[command(flatten)]
    pub machine: MachineArgs,
}

/// Dispatch one resource command
pub async fn run(ctx: &Ctx, command: ResourceCommand) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    match command {
        ResourceCommand::Schema => ctx.print_json(crate::queue::spec::schema_json()?)?,
        ResourceCommand::Register { name, device } => {
            let value = client
                .post("/v1/resources", &json!({ "name": name, "device": device }))
                .await?;
            print_result(
                ctx,
                &value,
                "registered",
                value["resource"]["id"].as_str().unwrap_or_default(),
            )?;
        }
        ResourceCommand::List(machine) => {
            let value = client.get(&path("/v1/resources", &machine)).await?;
            print_resources(ctx, value)?;
        }
        ResourceCommand::Jobs(machine) => {
            let value = client.get(&path("/v1/resource/jobs", &machine)).await?;
            print_jobs(ctx, value)?;
        }
        ResourceCommand::Job { command } => run_job(ctx, &client, command).await?,
        ResourceCommand::Release {
            attention,
            operation_id,
            machine,
        } => {
            let operation_id = operation_id.unwrap_or_default();
            let value = post_retry(
                &client,
                &path("/v1/resource/release", &machine),
                &json!({ "attention": attention, "operation_id": operation_id }),
            )
            .await?;
            print_result(ctx, &value, "released", &attention.to_string())?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn run_job(ctx: &Ctx, client: &Client, command: JobCommand) -> Result<(), AppError> {
    match command {
        JobCommand::Submit {
            job_id,
            spec,
            allow_other_thread,
        } => submit(ctx, client, job_id, &spec, allow_other_thread).await,
        JobCommand::Show { job, machine } => {
            let value = client
                .get(&path(&format!("/v1/resource/jobs/{job}"), &machine))
                .await?;
            print_job(ctx, value)
        }
        JobCommand::Move(args) => {
            let placement = Placement::from_flags(MoveFlags {
                front: args.front,
                back: args.back,
                priority: args.priority,
                before: args.before,
                after: args.after,
            })
            .map_err(|reason| QueueError::MoveRefused {
                job: args.job,
                reason,
            })?;
            let operation_id = args.operation_id.unwrap_or_default();
            let value = post_retry(
                client,
                &path(
                    &format!("/v1/resource/jobs/{}/move", args.job),
                    &args.machine,
                ),
                &json!({ "operation_id": operation_id, "placement": placement }),
            )
            .await?;
            print_result(ctx, &value, "moved", &args.job.to_string())
        }
        JobCommand::Cancel {
            job,
            operation_id,
            machine,
        } => {
            let operation_id = operation_id.unwrap_or_default();
            let value = post_retry(
                client,
                &path(&format!("/v1/resource/jobs/{job}/cancel"), &machine),
                &json!({ "operation_id": operation_id }),
            )
            .await?;
            print_result(ctx, &value, "cancel requested for", &job.to_string())
        }
    }
}

async fn submit(
    ctx: &Ctx,
    client: &Client,
    job_id: JobId,
    spec_path: &str,
    allow_other_thread: bool,
) -> Result<(), AppError> {
    let bytes = if spec_path == "-" {
        let mut bytes = Vec::new();
        io::stdin().read_to_end(&mut bytes)?;
        bytes
    } else {
        std::fs::read(spec_path)?
    };
    let spec = JobSpec::parse_bytes(&bytes)?;
    SubmitOrigin::capture(&ctx.home)?.check(spec.thread, allow_other_thread)?;
    let body = json!({ "job_id": job_id, "spec": spec, "env": TaskEnv::capture(), "callback_cwd": std::env::current_dir()? });
    let value = post_retry(client, "/v1/resource/jobs", &body).await?;
    print_result(ctx, &value, "submitted", &job_id.to_string())
}

fn path(base: &str, machine: &MachineArgs) -> String {
    match &machine.machine {
        None => base.to_owned(),
        Some(machine) => format!("{base}?machine={machine}"),
    }
}

async fn post_retry(client: &Client, path: &str, body: &Value) -> Result<Value, AppError> {
    // the immutable body holds the same job or operation identity on every retry
    for attempt in 0..3 {
        let result = client.post(path, body).await;
        if attempt == 2
            || !matches!(
                &result,
                Err(AppError::DaemonUnavailable { .. }
                    | AppError::DaemonBusy
                    | AppError::MachineUnavailable { .. })
            )
        {
            return result;
        }
        tokio::time::sleep(Duration::from_millis(200 * (attempt + 1))).await;
    }
    unreachable!("the last attempt returns")
}

fn print_result(ctx: &Ctx, value: &Value, action: &str, id: &str) -> Result<(), AppError> {
    ctx.print_id(id, &format!("{action} {id}"), value.clone())
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap_or("-")
}

fn print_resources(ctx: &Ctx, value: Value) -> Result<(), AppError> {
    if ctx.output == OutputMode::Json {
        return ctx.print_json(value);
    }
    if ctx.output == OutputMode::Human {
        println!(
            "ID                                   NAME             DEVICE STATE        RUN / YIELD AGE"
        );
    }
    for record in value["resources"].as_array().into_iter().flatten() {
        let resource = &record["resource"];
        let id = text(resource, "id");
        if ctx.output == OutputMode::Quiet {
            println!("{id}");
            continue;
        }
        let name = text(resource, "name");
        let device = resource["device"]
            .as_u64()
            .map_or_else(|| "-".into(), |device| device.to_string());
        let run = &record["run"];
        let state = run["phase"]["phase"].as_str().unwrap_or("idle");
        let task = text(run, "task");
        let age = run["phase"]["requested_at"]
            .as_str()
            .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
            .map_or_else(String::new, |at| {
                format!(
                    " / {}s",
                    (chrono::Utc::now() - at.to_utc()).num_seconds().max(0)
                )
            });
        println!("{id} {name:<16} {device:<6} {state:<12} {task}{age}");
    }
    Ok(())
}

fn print_jobs(ctx: &Ctx, value: Value) -> Result<(), AppError> {
    if ctx.output == OutputMode::Json {
        return ctx.print_json(value);
    }
    if ctx.output == OutputMode::Human {
        println!("JOB                                  PRIORITY POSITION STATE     TARGET / NAME");
    }
    for job in value["jobs"].as_array().into_iter().flatten() {
        let id = text(job, "id");
        if ctx.output == OutputMode::Quiet {
            println!("{id}");
            continue;
        }
        let priority = text(job, "priority");
        let position = job["position"].as_u64().unwrap_or(0);
        let state = text(&job["state"], "state");
        let target = job["target"]["resource"].as_str().unwrap_or("any");
        let name = text(&job["spec"], "name");
        println!("{id} {priority:<8} {position:<8} {state:<9} {target} / {name}");
    }
    Ok(())
}

fn print_job(ctx: &Ctx, value: Value) -> Result<(), AppError> {
    if ctx.output == OutputMode::Json {
        return ctx.print_json(value);
    }
    let job = &value["job"];
    let id = text(job, "id");
    if ctx.output == OutputMode::Quiet {
        println!("{id}");
        return Ok(());
    }
    let name = text(&job["spec"], "name");
    let state = text(&job["state"], "state");
    let priority = text(job, "priority");
    let target = job["target"]["resource"].as_str().unwrap_or("any");
    println!("{id}  {name}\nstate: {state}\npriority: {priority}\ntarget: {target}");
    println!(
        "step: {}\nlast stop: {}",
        job["next_step"], value["last_stop_cause"]
    );
    for run in value["runs"].as_array().into_iter().flatten() {
        let task = text(run, "task");
        let status = text(run, "status");
        println!(
            "run {} step {}: {task} {status}; cleanup: {}",
            run["run_number"], run["step"], run["cleanup"]
        );
    }
    Ok(())
}
