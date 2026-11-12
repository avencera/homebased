//! CI queue commands: submit build and test jobs by class, and manage slots

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use chrono::{DateTime, Utc};
use clap::{ArgGroup, Args, Subcommand};
use serde::Deserialize;
use serde_json::{Value, json};

use super::resource::{SubmitFlags, decode, submit_value};
use super::{Ctx, OutputMode};
use crate::client::Client;
use crate::dependency::DependencyId;
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::ci::{
    ClassStats, CommitId, GitRemote, JobClass, KillAfter, SlotCapacity, Spread, format_bytes,
};
use crate::queue::spec::MachineSelector;
use crate::queue::{JobId, Priority};
use crate::store::queue::interface::CiStatus;

/// CI queue commands
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum CiCommand {
    /// Queue a build or test command on any Fleet machine that offers its class
    ///
    /// The job waits on this machine until some machine has a free slot of
    /// the class, then runs there. With `--git`, it runs at this checkout's
    /// HEAD in the slot's own clone, so HEAD must be pushed
    Run(Box<RunArgs>),
    /// Show each machine's CI admission, load, classes, and slots
    Slots(MachineArg),
    /// Set how many jobs of a class a machine runs at once
    Capacity {
        /// Class, such as rust, android, xcode, or sim
        class: JobClass,
        /// Slots, from 0 through 32; 0 stops the machine taking the class
        capacity: SlotCapacity,
        /// Machine to change; this one when omitted
        #[command(flatten)]
        machine: MachineArg,
    },
    /// Stop a machine starting CI jobs; running jobs finish
    Drain {
        /// Why, shown in `ci slots`
        #[arg(long)]
        reason: String,
        /// Machine to drain; this one when omitted
        #[command(flatten)]
        machine: MachineArg,
    },
    /// Let a drained machine start CI jobs again
    Undrain(MachineArg),
    /// Change where, or at what level, a CI job still waiting for a slot runs
    Move(MoveArgs),
    /// Queue waits and run times per machine and class, with p50 and p90
    Stats(StatsArgs),
}

/// Optional machine selector
#[derive(Debug, Args)]
pub struct MachineArg {
    /// Fleet name or UUID
    #[arg(long)]
    pub machine: Option<MachineSelector>,
}

/// `ci run`
#[derive(Debug, Args)]
#[command(group(ArgGroup::new("source").args(["git", "repo"])))]
pub struct RunArgs {
    /// Class of slot the job needs
    #[arg(long)]
    class: JobClass,
    /// Run only on this machine; any machine that offers the class when omitted
    #[arg(long)]
    machine: Option<MachineSelector>,
    /// Queue level
    #[arg(long, default_value = "medium")]
    priority: Priority,
    /// Hard limit on the run; past it the queue kills its process group and
    /// the job fails
    #[arg(long, default_value_t = KillAfter::DEFAULT)]
    kill_after: KillAfter,
    /// Run at this checkout's HEAD, fetched from its upstream remote into the
    /// slot's own clone, in the same subdirectory as the current one
    #[arg(long)]
    git: bool,
    /// Remote to fetch from with `--git`; the branch's upstream remote, else
    /// origin, when omitted
    #[arg(long, requires = "git")]
    remote: Option<String>,
    /// Git URL to fetch `--commit` from, instead of `--git`
    #[arg(long, requires = "commit")]
    repo: Option<String>,
    /// Full commit id to check out with `--repo`
    #[arg(long, requires = "repo")]
    commit: Option<String>,
    /// Directory inside the checkout to run in with `--repo`
    #[arg(long, requires = "repo")]
    workdir: Option<PathBuf>,
    /// Untracked path that the clean before each run keeps; ignored files,
    /// such as `target/`, are always kept. Repeat for more
    #[arg(long, value_name = "PATH")]
    keep: Vec<PathBuf>,
    /// Task or job that must succeed first; repeat for more
    #[arg(long = "after", value_name = "TASK_OR_JOB")]
    after: Vec<DependencyId>,
    /// Stable UUID for retries after a lost response
    #[arg(long)]
    job_id: Option<JobId>,
    /// Display name; `ci <class>` when omitted
    #[arg(long)]
    name: Option<String>,
    /// Let the job's events go to a thread other than this one
    #[arg(long)]
    allow_other_thread: bool,
    /// Command and its arguments
    #[arg(last = true, required = true, num_args = 1..)]
    command: Vec<String>,
}

/// `ci move`
#[derive(Debug, Args)]
#[command(group(ArgGroup::new("target").args(["machine", "any"])))]
pub struct MoveArgs {
    /// Job UUID
    job: JobId,
    /// Run only on this machine
    #[arg(long)]
    machine: Option<MachineSelector>,
    /// Run on any machine that offers the class
    #[arg(long)]
    any: bool,
    /// New queue level
    #[arg(long)]
    priority: Option<Priority>,
}

/// `ci stats`
#[derive(Debug, Args)]
pub struct StatsArgs {
    /// Include jobs that ended within this long, such as 24h or 7d
    #[arg(long, default_value = "24h")]
    since: humantime::Duration,
    /// Only this machine
    #[arg(long)]
    machine: Option<MachineSelector>,
    /// Only this class
    #[arg(long)]
    class: Option<JobClass>,
}

/// Run one `ci` command
pub async fn run(ctx: &Ctx, command: CiCommand) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    match command {
        CiCommand::Run(args) => submit(ctx, &client, *args).await?,
        CiCommand::Slots(machine) => {
            let value = client.get(&with_machine("/v1/ci/slots", &machine)).await?;
            print_slots(ctx, value)?;
        }
        CiCommand::Capacity {
            class,
            capacity,
            machine,
        } => {
            let body = json!({ "class": class, "capacity": capacity });
            let value = client
                .post(&with_machine("/v1/ci/capacity", &machine), &body)
                .await?;
            ctx.print_id(
                class.as_str(),
                &format!("{class} capacity set to {capacity}"),
                value,
            )?;
        }
        CiCommand::Drain { reason, machine } => {
            let value = client
                .post(
                    &with_machine("/v1/ci/drain", &machine),
                    &json!({ "reason": reason }),
                )
                .await?;
            ctx.print_id("drained", "drained; running CI jobs finish", value)?;
        }
        CiCommand::Undrain(machine) => {
            let value = client
                .post(
                    &with_machine("/v1/ci/drain", &machine),
                    &json!({ "reason": null }),
                )
                .await?;
            ctx.print_id("open", "open for CI jobs", value)?;
        }
        CiCommand::Move(args) => {
            let target = match (args.machine, args.any) {
                (Some(machine), _) => json!({ "type": "machine", "machine": machine }),
                (None, true) => json!({ "type": "any" }),
                (None, false) => Value::Null,
            };
            let body = json!({ "target": target, "priority": args.priority });
            let value = client
                .post(&format!("/v1/ci/jobs/{}/move", args.job), &body)
                .await?;
            ctx.print_id(&args.job.to_string(), &format!("moved {}", args.job), value)?;
        }
        CiCommand::Stats(args) => stats(ctx, &client, args).await?,
    }
    Ok(ExitCode::SUCCESS)
}

async fn submit(ctx: &Ctx, client: &Client, args: RunArgs) -> Result<(), AppError> {
    let cwd = std::env::current_dir()?;
    let (git, job_cwd) = if args.git {
        let (git, workdir) = head_checkout(&cwd, args.remote.as_deref(), &args.keep)?;
        (Some(git), workdir)
    } else if let (Some(repo), Some(commit)) = (&args.repo, &args.commit) {
        GitRemote::parse(repo)?;
        CommitId::parse(commit)?;
        let git = json!({ "remote": repo, "commit": commit, "keep": args.keep });
        (
            Some(git),
            args.workdir.clone().unwrap_or_else(|| PathBuf::from(".")),
        )
    } else {
        (None, cwd.clone())
    };
    let mut ci = json!({ "class": args.class, "kill_after": args.kill_after });
    if let Some(git) = git {
        ci["git"] = git;
    }
    let mut spec = json!({
        "api_version": 1,
        "name": args.name.clone().unwrap_or_else(|| format!("ci {}", args.class)),
        "cwd": job_cwd,
        "priority": args.priority,
        "preempt": { "mode": "wait" },
        "workload": { "type": "task", "command": args.command },
        "ci": ci,
    });
    if let Some(machine) = &args.machine {
        spec["machine"] = json!(machine.to_string());
    }
    let flags = SubmitFlags {
        allow_other_thread: args.allow_other_thread,
        allow_build: true,
    };
    let job = args.job_id.unwrap_or_default();
    submit_value(ctx, client, job, spec, flags, args.after).await
}

/// This checkout's HEAD as a `ci.git` value, and the current directory
/// relative to the checkout's top
///
/// HEAD must be on a branch of the remote, as last fetched, since the slot
/// fetches it from there
fn head_checkout(
    cwd: &Path,
    remote: Option<&str>,
    keep: &[PathBuf],
) -> Result<(Value, PathBuf), AppError> {
    let top = PathBuf::from(git(cwd, &["rev-parse", "--show-toplevel"])?);
    let commit = git(cwd, &["rev-parse", "HEAD"])?;
    let remote = match remote {
        Some(remote) => remote.to_owned(),
        None => git(
            cwd,
            &[
                "rev-parse",
                "--abbrev-ref",
                "--symbolic-full-name",
                "@{upstream}",
            ],
        )
        .ok()
        .and_then(|upstream| {
            upstream
                .split_once('/')
                .map(|(remote, _)| remote.to_owned())
        })
        .unwrap_or_else(|| "origin".to_owned()),
    };
    // parse before any message names the url, so a credential in it is
    // refused redacted instead of printed
    let url = GitRemote::parse(&git(cwd, &["remote", "get-url", &remote])?)?;
    let containing = git(
        cwd,
        &[
            "for-each-ref",
            "--count=1",
            "--contains",
            &commit,
            "--format=%(refname)",
            &format!("refs/remotes/{remote}"),
        ],
    )?;
    if containing.is_empty() {
        return Err(AppError::Usage {
            message: format!(
                "HEAD {commit} is not on any branch of {remote} as last fetched; push it first, since the CI slot fetches it from {}",
                url.as_str()
            ),
        });
    }
    let top = top.canonicalize()?;
    let here = cwd.canonicalize()?;
    let workdir = match here.strip_prefix(&top) {
        Ok(relative) if relative.as_os_str().is_empty() => PathBuf::from("."),
        Ok(relative) => relative.to_path_buf(),
        Err(_) => PathBuf::from("."),
    };
    Ok((
        json!({ "remote": url.as_str(), "commit": commit, "keep": keep }),
        workdir,
    ))
}

/// Run git in `cwd` and return its trimmed stdout
fn git(cwd: &Path, args: &[&str]) -> Result<String, AppError> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(AppError::Usage {
            message: format!(
                "git {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn with_machine(base: &str, machine: &MachineArg) -> String {
    match &machine.machine {
        None => base.to_owned(),
        Some(machine) => format!("{base}?machine={machine}"),
    }
}

/// The parts of a fleet read the CLI prints
#[derive(Deserialize)]
struct FleetNames {
    machines: Vec<NamedMachine>,
}

#[derive(Deserialize)]
struct NamedMachine {
    machine: MachineId,
    name: String,
    read: Value,
}

impl FleetNames {
    fn name(&self, machine: MachineId) -> String {
        self.machines
            .iter()
            .find(|named| named.machine == machine)
            .map_or_else(|| machine.to_string(), |named| named.name.clone())
    }

    /// Machines whose read failed, with why
    fn unreachable(&self) -> impl Iterator<Item = (&str, String)> {
        self.machines.iter().filter_map(|named| {
            let state = named.read["state"].as_str().unwrap_or_default();
            (state != "online").then(|| {
                let why = named.read["message"]
                    .as_str()
                    .or(named.read["error"].as_str())
                    .unwrap_or(state);
                (named.name.as_str(), why.to_owned())
            })
        })
    }
}

fn print_slots(ctx: &Ctx, value: Value) -> Result<(), AppError> {
    if ctx.output == OutputMode::Json {
        return ctx.print_json(value);
    }
    let names: FleetNames = decode(value.clone())?;
    let statuses: Vec<CiStatus> = decode(value["statuses"].clone())?;
    for status in &statuses {
        let offer = &status.offer;
        let disk = offer
            .gauge
            .free_disk
            .map_or_else(|| "unknown".to_owned(), format_bytes);
        println!(
            "{}  {}  load {:.2}/core on {} cores  disk {disk} free",
            names.name(offer.machine),
            offer.admission.describe(),
            offer.gauge.load_per_core(),
            offer.gauge.cores,
        );
        for class in &offer.classes {
            println!(
                "  {:<12} capacity {:>2}  busy {:>2}  queued {:>2}",
                class.class.as_str(),
                class.capacity.get(),
                class.busy,
                class.queued
            );
        }
        for slot in &status.slots {
            let retired = slot.resource.slot.as_ref().is_some_and(|lane| lane.retired);
            let run = slot.run.as_ref().map_or_else(
                || "idle".to_owned(),
                |run| format!("{} job {}", run.phase.as_str(), run.job),
            );
            let retired = if retired { " (retired)" } else { "" };
            println!("    {:<16} {run}{retired}", slot.resource.name.as_str());
        }
    }
    for (name, why) in names.unreachable() {
        println!("{name}  not read: {why}");
    }
    Ok(())
}

async fn stats(ctx: &Ctx, client: &Client, args: StatsArgs) -> Result<(), AppError> {
    let since: DateTime<Utc> = Utc::now()
        - chrono::Duration::from_std(*args.since).map_err(|_| AppError::Usage {
            message: "--since is too long".into(),
        })?;
    let mut query = format!(
        "/v1/ci/stats?since={}",
        urlencode(&since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
    );
    if let Some(machine) = &args.machine {
        query.push_str(&format!("&machine={}", urlencode(&machine.to_string())));
    }
    if let Some(class) = &args.class {
        query.push_str(&format!("&class={class}"));
    }
    let value = client.get(&query).await?;
    if ctx.output == OutputMode::Json {
        return ctx.print_json(value);
    }
    let names: FleetNames = decode(value.clone())?;
    let stats: Vec<ClassStats> = decode(value["stats"].clone())?;
    println!(
        "MACHINE          CLASS        JOBS WAITING  WAIT p50   p90      RUN p50    p90      TIMED OUT"
    );
    for row in &stats {
        println!(
            "{:<16} {:<12} {:>4} {:>7}  {:<10} {:<8} {:<10} {:<8} {:>9}",
            names.name(row.machine),
            row.class.as_str(),
            row.jobs,
            row.waiting,
            p50(row.waited.as_ref()),
            p90(row.waited.as_ref()),
            p50(row.ran.as_ref()),
            p90(row.ran.as_ref()),
            row.timed_out
        );
    }
    for (name, why) in names.unreachable() {
        println!("{name}  not read: {why}");
    }
    Ok(())
}

fn p50(spread: Option<&Spread>) -> String {
    spread.map_or_else(|| "-".to_owned(), |spread| span(spread.p50_secs))
}

fn p90(spread: Option<&Spread>) -> String {
    spread.map_or_else(|| "-".to_owned(), |spread| span(spread.p90_secs))
}

/// Seconds as a short span, such as `42s`, `3m05s`, or `1h20m`
fn span(secs: f64) -> String {
    // display only; spans are far below u64 range
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let secs = secs.round().max(0.0) as u64;
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m{:02}s", secs / 60, secs % 60),
        _ => format!("{}h{:02}m", secs / 3600, secs % 3600 / 60),
    }
}

/// Percent-encode a query value
fn urlencode(raw: &str) -> String {
    raw.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}
