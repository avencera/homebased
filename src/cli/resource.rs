//! Resource queue commands, with stable identities for transport retries

use std::process::ExitCode;
use std::time::Duration;

use clap::{ArgGroup, Args, Subcommand};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::{Ctx, OutputMode};
use crate::callback::destination::SubmitOrigin;
use crate::client::Client;
use crate::dependency::{Dependencies, DependencyId};
use crate::domain::TaskEnv;
use crate::error::AppError;
use crate::queue::build_hint::build_warnings;
use crate::queue::gpu::GpuUuid;
use crate::queue::gpu_idle::{
    GpuIdleControl, GpuIdleControlResult, GpuIdleDetail, SampleSource, Silence,
};
use crate::queue::lease_guard::{YIELD_OVERDUE, yield_overdue};
use crate::queue::spec::{JobSpec, MachineSelector, schema_json};
use crate::queue::{
    AttentionId, JobId, MoveFlags, OperationId, Placement, Priority, QueueError, Resource,
    ResourceId, ResourceLabel, ResourceName, ResourceSelector, RunPhase, StopCause, Target,
};
use crate::store::queue::interface::{JobDetail, JobList, ResourceList};
use crate::store::queue::{JobLeaseNote, ResourceRecord};

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
    /// Set or clear the nickname dashboards and lists show for a resource
    ///
    /// Specs, pins, and commands keep using the resource's name
    Label {
        /// Resource name or UUID
        resource: ResourceSelector,
        /// Nickname of 1 to 40 characters, such as "RTX 5090"
        #[arg(required_unless_present = "clear", conflicts_with = "clear")]
        label: Option<ResourceLabel>,
        /// Remove the nickname
        #[arg(long)]
        clear: bool,
        /// Machine whose resource to label
        #[command(flatten)]
        machine: MachineArgs,
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
        /// Submit even though a step runs a build tool, for the rare build
        /// that truly needs the GPU, such as an autotuner that times kernels
        #[arg(long)]
        allow_build: bool,
        /// Task or job this machine submitted that must succeed before the
        /// job joins the queue; repeat for more. The job is held under its id
        /// until then, and cancelled without using the queue if one ends any
        /// other way
        #[arg(long = "after", value_name = "TASK_OR_JOB")]
        after: Vec<DependencyId>,
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
    /// Snooze, turn off, or restore a job's GPU idle alerts
    ///
    /// Alerts only notify; they never stop the job. The latest control wins
    GpuIdle(GpuIdleArgs),
}

/// Exactly one GPU idle control, with a reason for every silence
#[derive(Debug, Args)]
#[command(group = ArgGroup::new("control").required(true).args(["snooze", "off", "clear"]))]
pub struct GpuIdleArgs {
    /// Job UUID
    pub job: JobId,
    /// Hold alerts for this long, such as 15m or 2h; any positive duration
    #[arg(long, value_name = "DURATION")]
    pub snooze: Option<humantime::Duration>,
    /// Turn alerts off for the rest of the job
    #[arg(long)]
    pub off: bool,
    /// Drop any snooze or off switch, so the spec's gpu_idle setting applies
    #[arg(long)]
    pub clear: bool,
    /// Why the GPU may sit idle; shown to other agents in job show
    #[arg(long, required_unless_present = "clear", conflicts_with = "clear")]
    pub reason: Option<String>,
    /// Replay identity; generated once when omitted
    #[arg(long)]
    pub operation_id: Option<OperationId>,
    /// Queue authority when submitted from another origin
    #[command(flatten)]
    pub machine: MachineArgs,
}

impl GpuIdleArgs {
    fn control(&self) -> GpuIdleControl {
        let reason = self.reason.clone().unwrap_or_default();
        match (self.snooze, self.off) {
            (Some(duration), _) => GpuIdleControl::Snooze {
                duration: duration.into(),
                reason,
            },
            (None, true) => GpuIdleControl::Off { reason },
            (None, false) => GpuIdleControl::Clear,
        }
    }
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
        ResourceCommand::Label {
            resource,
            label,
            clear: _,
            machine,
        } => {
            // setting a label is idempotent, so a lost reply can be retried as is
            let value = post_retry(
                &client,
                &path("/v1/resource/label", &machine),
                &json!({ "resource": resource, "label": label }),
            )
            .await?;
            let labeled: ResourceRecord = decode(value.clone())?;
            let action = if labeled.resource.label.is_some() {
                "labeled"
            } else {
                "unlabeled"
            };
            print_result(ctx, &value, action, &labeled.resource.id.to_string())?;
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
            allow_build,
            after,
        } => {
            let flags = SubmitFlags {
                allow_other_thread,
                allow_build,
            };
            submit(ctx, client, job_id, &spec, flags, after).await
        }
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
        JobCommand::GpuIdle(args) => {
            let control = args.control();
            control
                .validate()
                .map_err(|message| AppError::Usage { message })?;
            let operation_id = args.operation_id.unwrap_or_default();
            let value = post_retry(
                client,
                &path(
                    &format!("/v1/resource/jobs/{}/gpu-idle", args.job),
                    &args.machine,
                ),
                &json!({ "operation_id": operation_id, "control": control }),
            )
            .await?;
            let result: GpuIdleControlResult = decode(value.clone())?;
            let action = format!("GPU idle alerts {} for", silence_label(&result.silence));
            print_result(ctx, &value, &action, &args.job.to_string())
        }
    }
}

fn silence_label(silence: &Silence) -> String {
    match silence {
        Silence::Watching => "on".into(),
        Silence::Snoozed { until, .. } => format!("snoozed until {}", until.to_rfc3339()),
        Silence::Off { .. } => "off".into(),
        Silence::SpecOff => "off by spec".into(),
    }
}

/// Submit overrides
#[derive(Debug, Clone, Copy)]
struct SubmitFlags {
    allow_other_thread: bool,
    allow_build: bool,
}

async fn submit(
    ctx: &Ctx,
    client: &Client,
    job_id: JobId,
    spec_path: &str,
    flags: SubmitFlags,
    after_flags: Vec<DependencyId>,
) -> Result<(), AppError> {
    let origin = SubmitOrigin::capture(&ctx.home)?;
    let mut value = crate::spec::read_spec_value(spec_path)?;
    if let Some(thread) = origin.default_thread()? {
        crate::spec::fill_missing_thread(&mut value, thread);
    }
    let (spec, after) = JobSpec::parse_submission(&value)?;
    let after = merge_after(after, after_flags)?;
    origin.check(spec.thread, flags.allow_other_thread)?;
    // a build holds the GPU idle while other jobs wait; refuse it before
    // anything is sent, unless the submitter says the build needs the GPU
    let warnings = build_warnings(&spec);
    if !warnings.is_empty() && !flags.allow_build {
        return Err(AppError::InvalidSpec {
            pointer: "/steps".into(),
            value: Value::Null,
            message: format!(
                "{}; pass --allow-build only if the build itself needs the GPU",
                warnings.join("; ")
            ),
        });
    }
    let body = json!({
        "job_id": job_id,
        "spec": spec,
        "env": TaskEnv::capture(),
        "callback_cwd": std::env::current_dir()?,
        "after": after,
    });
    let mut value = post_retry(client, "/v1/resource/jobs", &body).await?;
    // printed only once the submit succeeds, so a refusal's stderr stays the
    // one structured error that `--json` promises
    for warning in &warnings {
        eprintln!("warning: {warning}");
    }
    if !warnings.is_empty() {
        value["warnings"] = json!(warnings);
    }
    let action = match value["held"]["state"].as_str() {
        Some("held") => "held until its after entries succeed:",
        Some(_) => "ended before it queued:",
        None => "submitted",
    };
    print_result(ctx, &value, action, &job_id.to_string())?;
    super::task::print_t3_wait_hint(&job_id.to_string(), spec.thread);
    Ok(())
}

/// The spec's `after` and the `--after` flags as one list, each entry once
fn merge_after(
    spec: Option<Dependencies>,
    flags: Vec<DependencyId>,
) -> Result<Option<Dependencies>, AppError> {
    let mut ids: Vec<DependencyId> = spec.map(Vec::from).unwrap_or_default();
    for id in flags {
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    if ids.is_empty() {
        return Ok(None);
    }
    crate::spec::parse_after(&json!(ids)).map(Some)
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
            "ID                                   NAME             DEVICE STATE        RUN / YIELD AGE                            GPU UUID                                 LABEL"
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
        let label = resource.label.as_ref().map_or("-", ResourceLabel::as_str);
        let gpu = resource.gpu_uuid.as_ref().map_or("-", GpuUuid::as_str);
        let run = format!("{task}{age}");
        println!("{id} {name:<16} {device:<6} {state:<12} {run:<42} {gpu:<40} {label}");
    }
    Ok(())
}

fn target_label(target: Target, known: &[Resource]) -> String {
    match target {
        Target::Any => "any".into(),
        Target::Pinned(resource) => resource_label(resource, known),
    }
}

/// `gpu0 (RTX 5090)` for a known resource, else its UUID
fn resource_label(id: ResourceId, known: &[Resource]) -> String {
    match known.iter().find(|resource| resource.id == id) {
        Some(Resource {
            name,
            label: Some(label),
            ..
        }) => format!("{name} ({label})"),
        Some(resource) => resource.name.to_string(),
        None => id.to_string(),
    }
}

/// A job the origin holds for its `after` tasks, or ended before it queued
fn print_held(ctx: &Ctx, held: &Value) -> Result<(), AppError> {
    let text = |key: &str| held[key].as_str().unwrap_or("-").to_owned();
    if ctx.output == OutputMode::Quiet {
        println!("{}", text("job_id"));
        return Ok(());
    }
    println!(
        "{}  {}\nstate: {}\nqueue: {}",
        text("job_id"),
        text("name"),
        text("state"),
        text("machine")
    );
    for dependency in held["after"].as_array().into_iter().flatten() {
        println!("after {}", super::task::dependency_line(dependency));
    }
    if let Some(reason) = held["reason"].as_str() {
        println!("reason: {reason}");
    }
    Ok(())
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
        let target = target_label(job.target, &[]);
        let name = &job.spec.name;
        println!("{id} {priority:<8} {position:<8} {state:<9} {target} / {name}");
        if let Some(note) = list.lease_notes.get(&id) {
            for line in lease_note_lines(note, chrono::Utc::now()) {
                println!("  ! {line}");
            }
        }
    }
    Ok(())
}

/// Warnings for one job in `resource jobs`: its latest guard trip, and a
/// yield request pending so long that the step likely never reads the
/// yield file
fn lease_note_lines(note: &JobLeaseNote, now: chrono::DateTime<chrono::Utc>) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(trip) = &note.lease_idle {
        lines.push(trip.reason());
    }
    if let Some(requested) = note.yield_requested_at
        && yield_overdue(requested, now, YIELD_OVERDUE)
    {
        let minutes = (now - requested).num_minutes();
        lines.push(format!(
            "yield requested {minutes}m ago and the step is still running: it may not poll \
             HOMEBASED_YIELD_FILE; fix the step or submit it with preempt `wait`"
        ));
    }
    lines
}

fn print_job(ctx: &Ctx, value: Value) -> Result<(), AppError> {
    if ctx.output == OutputMode::Json {
        return ctx.print_json(value);
    }
    if let Some(held) = value.get("held") {
        return print_held(ctx, held);
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
    let target = target_label(job.target, &detail.resources);
    println!("{id}  {name}\nstate: {state}\npriority: {priority}\ntarget: {target}");
    let last_stop = detail.last_stop_cause.map_or("-", StopCause::as_str);
    println!("step: {}\nlast stop: {last_stop}", job.step);
    for run in &detail.runs {
        let cleanup = match &run.cleanup {
            None => "pending".to_owned(),
            Some(Ok(())) => "clean".to_owned(),
            Some(Err(failure)) => format!("failed: {failure}"),
        };
        let on = run.resource.map_or_else(String::new, |resource| {
            format!(" on {}", resource_label(resource, &detail.resources))
        });
        let gpu = run
            .gpu_uuid
            .as_ref()
            .map_or_else(String::new, |uuid| format!(" ({uuid})"));
        println!(
            "run {} step {}: {} {}{on}{gpu}; cleanup: {cleanup}",
            run.run_number, run.step, run.task, run.status
        );
        if let Some(summary) = &run.gpu_summary {
            println!("  gpu: {}", summary.brief());
        }
    }
    for trip in &detail.lease_idle {
        println!("lease guard, run task {}: {}", trip.attempt, trip.reason());
    }
    // jobs that never ran on a watched GPU and were never silenced say nothing
    if let Some(idle) = detail.gpu_idle.as_ref().filter(|idle| {
        !idle.attempts.is_empty() || !idle.controls.is_empty() || idle.silence.silenced()
    }) {
        print_gpu_idle(idle);
    }
    Ok(())
}

fn print_gpu_idle(idle: &GpuIdleDetail) {
    let reason = match &idle.silence {
        Silence::Snoozed { reason, .. } | Silence::Off { reason } => format!(" ({reason})"),
        Silence::Watching | Silence::SpecOff => String::new(),
    };
    println!("gpu idle alerts: {}{reason}", silence_label(&idle.silence));
    for attempt in &idle.attempts {
        let source = attempt.source.map_or("-", SampleSource::as_str);
        println!(
            "gpu idle {}: {} alerts, episode {}; buckets {} busy, {} idle, {} unknown; source {source}",
            attempt.task,
            attempt.alerts,
            attempt.episode,
            attempt.busy_buckets,
            attempt.idle_buckets,
            attempt.unknown_buckets
        );
    }
    for control in &idle.controls {
        let until = control.until.map_or_else(String::new, |until| {
            format!(" until {}", until.to_rfc3339())
        });
        let reason = control
            .reason
            .as_ref()
            .map_or_else(String::new, |reason| format!(": {reason}"));
        println!(
            "gpu idle {} at {}{until}{reason}",
            control.action,
            control.at.to_rfc3339()
        );
    }
}
