//! Resource queue commands, with stable identities for transport retries

use std::io::{self, Read};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Subcommand};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::{Ctx, OutputMode};
use crate::callback::destination::SubmitOrigin;
use crate::client::Client;
use crate::domain::TaskEnv;
use crate::error::AppError;
use crate::queue::spec::{JobSpec, MachineSelector, schema_json};
use crate::queue::{
    AttentionId, JobId, MoveFlags, OperationId, Placement, Priority, QueueError, ResourceName,
    RunPhase, StopCause, Target,
};
use crate::store::queue::ResourceRecord;
use crate::store::queue::interface::{JobDetail, JobList, ResourceList};

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
        ResourceCommand::Schema => ctx.print_json(schema_json()?)?,
        ResourceCommand::Register { name, device } => {
            let value = client
                .post("/v1/resources", &json!({ "name": name, "device": device }))
                .await?;
            let registered: ResourceRecord = decode(value.clone())?;
            print_result(
                ctx,
                &value,
                "registered",
                &registered.resource.id.to_string(),
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
    let body = json!({
        "job_id": job_id,
        "spec": spec,
        "env": TaskEnv::capture(),
        "callback_cwd": std::env::current_dir()?,
    });
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
    const ATTEMPTS: u64 = 3;

    // the immutable body holds the same job or operation identity on every retry
    let mut attempt = 1;
    loop {
        let result = client.post(path, body).await;
        let transient = matches!(
            &result,
            Err(AppError::DaemonUnavailable { .. }
                | AppError::DaemonBusy
                | AppError::MachineUnavailable { .. })
        );
        if !transient || attempt == ATTEMPTS {
            return result;
        }
        tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
        attempt += 1;
    }
}

fn print_result(ctx: &Ctx, value: &Value, action: &str, id: &str) -> Result<(), AppError> {
    ctx.print_id(id, &format!("{action} {id}"), value.clone())
}

/// Decode a daemon response into the typed shape the daemon serialized
fn decode<T: DeserializeOwned>(value: Value) -> Result<T, AppError> {
    serde_json::from_value(value).map_err(|error| AppError::Internal {
        message: format!("unexpected queue response: {error}"),
    })
}

fn print_resources(ctx: &Ctx, value: Value) -> Result<(), AppError> {
    if ctx.output == OutputMode::Json {
        return ctx.print_json(value);
    }
    let list: ResourceList = decode(value)?;
    if ctx.output == OutputMode::Human {
        println!(
            "ID                                   NAME             DEVICE STATE        RUN / YIELD AGE"
        );
    }
    for record in &list.resources {
        let resource = &record.resource;
        let id = resource.id;
        if ctx.output == OutputMode::Quiet {
            println!("{id}");
            continue;
        }
        let name = resource.name.as_str();
        let device = resource
            .device
            .map_or_else(|| "-".into(), |device| device.to_string());
        let run = record.run.as_ref();
        let state = run.map_or("idle", |run| run.phase.as_str());
        let task = run.map_or_else(|| "-".into(), |run| run.task.to_string());
        let age = match run.map(|run| &run.phase) {
            Some(RunPhase::Stopping { requested_at, .. }) => format!(
                " / {}s",
                (chrono::Utc::now() - *requested_at).num_seconds().max(0)
            ),
            _ => String::new(),
        };
        println!("{id} {name:<16} {device:<6} {state:<12} {task}{age}");
    }
    Ok(())
}

fn target_label(target: Target) -> String {
    match target {
        Target::Any => "any".into(),
        Target::Pinned(resource) => resource.to_string(),
    }
}

fn print_jobs(ctx: &Ctx, value: Value) -> Result<(), AppError> {
    if ctx.output == OutputMode::Json {
        return ctx.print_json(value);
    }
    let list: JobList = decode(value)?;
    if ctx.output == OutputMode::Human {
        println!("JOB                                  PRIORITY POSITION STATE     TARGET / NAME");
    }
    for job in &list.jobs {
        let id = job.id;
        if ctx.output == OutputMode::Quiet {
            println!("{id}");
            continue;
        }
        let priority = job.priority.as_str();
        let position = job.position.unwrap_or(0);
        let state = job.state.as_str();
        let target = target_label(job.target);
        let name = &job.spec.name;
        println!("{id} {priority:<8} {position:<8} {state:<9} {target} / {name}");
    }
    Ok(())
}

fn print_job(ctx: &Ctx, value: Value) -> Result<(), AppError> {
    if ctx.output == OutputMode::Json {
        return ctx.print_json(value);
    }
    let detail: JobDetail = decode(value)?;
    let job = &detail.job;
    let id = job.id;
    if ctx.output == OutputMode::Quiet {
        println!("{id}");
        return Ok(());
    }
    let name = &job.spec.name;
    let state = job.state.as_str();
    let priority = job.priority.as_str();
    let target = target_label(job.target);
    println!("{id}  {name}\nstate: {state}\npriority: {priority}\ntarget: {target}");
    let last_stop = detail.last_stop_cause.map_or("-", StopCause::as_str);
    println!("step: {}\nlast stop: {last_stop}", job.step);
    for run in &detail.runs {
        let cleanup = match &run.cleanup {
            None => "pending".to_owned(),
            Some(Ok(())) => "clean".to_owned(),
            Some(Err(failure)) => format!("failed: {failure}"),
        };
        println!(
            "run {} step {}: {} {}; cleanup: {cleanup}",
            run.run_number, run.step, run.task, run.status
        );
    }
    Ok(())
}
