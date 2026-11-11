//! `homebased service` commands

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::Subcommand;
use serde_json::{Value, json};

use crate::callback::destination::SubmitOrigin;
use crate::client::Client;
use crate::domain::{API_VERSION, TaskEnv, ThreadId};
use crate::error::AppError;
use crate::service::{
    Readiness, ServiceId, ServiceState, ServiceView, ServicesResponse, StartRequest, TAIL_LINES,
    tail_lines,
};

use super::{Ctx, OutputMode};

/// How often `start` and `stop` read the service while they wait
const POLL: Duration = Duration::from_millis(200);

/// Time beyond `--ready-timeout` that `start` allows for the spawn and for
/// the stop of a service that was not ready, which gets 10 seconds of TERM
/// grace before KILL
const START_SLACK: Duration = Duration::from_secs(30);

/// How long `stop` waits for the service to end
const STOP_WAIT: Duration = Duration::from_secs(30);

const START_EXAMPLES: &str = "\
Examples:
  homebased service start --name api --port 8787 -- pnpm wrangler dev --port 8787
  homebased service start --name fixtures --port 8765 -- python3 server.py 8765
  homebased service start --name emulator --ready-line 'boot completed' --ready-timeout 5m -- emulator -avd Pixel_8
  homebased service stop <ID>";

/// Service subcommands
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum ServiceCommand {
    /// Start a long-lived process on this machine and return once it is ready
    ///
    /// Runs the command under the daemon, detached from this shell, in the
    /// current directory (or `--cwd`) with this shell's whole environment. With
    /// `--port`, it is ready once 127.0.0.1 or ::1 accepts a connection on that
    /// port; with `--ready-line`, once a line of its output matches; with
    /// neither, once it has run for a second. If it exits or is not ready within
    /// `--ready-timeout`, it is stopped, its last output lines go to stderr, and
    /// this command fails.
    ///
    /// The service belongs to the calling thread, found like `task run` finds
    /// it. It is stopped by `service stop`, when that thread's T3 thread is
    /// archived, settled, or deleted, or once `--max` has elapsed. If it exits
    /// on its own, the thread gets one `SERVICE_EXITED` event with its exit and
    /// last output lines. A service has no output-inactivity check: it may stay
    /// silent for hours
    #[command(after_help = START_EXAMPLES)]
    Start {
        /// Short label for listings and events
        #[arg(long)]
        name: String,
        /// Working directory. Defaults to the current one
        #[arg(long)]
        cwd: Option<PathBuf>,
        /// Ready once this local TCP port accepts a connection
        #[arg(long, conflicts_with = "ready_line")]
        port: Option<u16>,
        /// Ready once a line of output matches this regular expression
        #[arg(long, value_name = "REGEX")]
        ready_line: Option<String>,
        /// How long to wait for readiness before stopping it, such as `30s` or `5m`
        #[arg(long, default_value = "2m", value_parser = humantime::parse_duration)]
        ready_timeout: Duration,
        /// Longest the service may run before it is stopped, such as `2h`
        #[arg(long, default_value = "12h", value_parser = humantime::parse_duration)]
        max: Duration,
        /// Program and arguments, after `--`. No shell: use `-- sh -c '...'` for shell syntax
        #[arg(required = true, num_args = 1.., last = true, value_name = "COMMAND")]
        command: Vec<String>,
    },
    /// List this thread's running services, with their state
    List {
        /// Include services that ended
        #[arg(long)]
        ended: bool,
        /// Services of every thread, not only the caller's
        #[arg(long)]
        all_threads: bool,
    },
    /// Print a service's combined stdout and stderr
    Log {
        /// Service id
        id: ServiceId,
        /// Last N lines. A service can run for hours, so the whole log is
        /// never sent at once
        #[arg(long, default_value_t = 200)]
        tail: usize,
    },
    /// Stop a service and its process group, and wait until it has ended.
    /// Idempotent: an ended service is left as it is
    Stop {
        /// Service id
        id: ServiceId,
    },
}

/// Dispatch a service command
pub async fn run(ctx: &Ctx, command: ServiceCommand) -> Result<ExitCode, AppError> {
    match command {
        ServiceCommand::Start {
            name,
            cwd,
            port,
            ready_line,
            ready_timeout,
            max,
            command,
        } => {
            let readiness = match (port, ready_line) {
                (Some(port), _) => Readiness::Port { port },
                (None, Some(pattern)) => Readiness::Line { pattern },
                (None, None) => Readiness::Spawned,
            };
            let options = StartOptions {
                name,
                cwd,
                readiness,
                ready_timeout,
                max,
                command,
            };
            start(ctx, options).await
        }
        ServiceCommand::List { ended, all_threads } => list(ctx, ended, all_threads).await,
        ServiceCommand::Log { id, tail } => log(ctx, id, tail).await,
        ServiceCommand::Stop { id } => stop(ctx, id).await,
    }
}

struct StartOptions {
    name: String,
    cwd: Option<PathBuf>,
    readiness: Readiness,
    ready_timeout: Duration,
    max: Duration,
    command: Vec<String>,
}

async fn start(ctx: &Ctx, options: StartOptions) -> Result<ExitCode, AppError> {
    options.readiness.validate()?;
    let origin = SubmitOrigin::capture(&ctx.home)?;
    let owner = origin.default_thread()?.unwrap_or(ThreadId::DETACHED);
    // the same check `task run` makes, so a SERVICE_EXITED event can reach the owner
    origin.check(owner, false)?;
    let cwd = match options.cwd {
        Some(cwd) => super::task::expand_home(cwd),
        None => std::env::current_dir()?,
    };
    let body = StartRequest {
        api_version: API_VERSION,
        name: options.name,
        owner,
        cwd: std::path::absolute(cwd)?,
        command: options.command,
        env: caller_env(),
        readiness: options.readiness,
        ready_timeout: options.ready_timeout,
        max: options.max,
        callback_env: TaskEnv::capture(),
        callback_cwd: std::env::current_dir()?,
    };

    let client = Client::new(ctx.home.sock_path());
    let started: ServiceView = decode(client.post("/v1/services", &json!(body)).await?)?;
    let id = started.id;
    let deadline = Instant::now() + options.ready_timeout + START_SLACK;
    let view = loop {
        let view = show(&client, id).await?;
        if view.state != ServiceState::Starting {
            break view;
        }
        if Instant::now() >= deadline {
            // the worker enforces the timeout; this only guards a stuck worker
            let _ = client
                .post(&format!("/v1/services/{id}/stop"), &json!({}))
                .await;
            break show(&client, id).await?;
        }
        tokio::time::sleep(POLL).await;
    };
    // a service that was ready and died before this poll started fine; its
    // SERVICE_EXITED tells the owner it died
    if view.ready_at.is_none() {
        return Err(not_ready(ctx, &client, &view).await);
    }

    let human = format!("started {id} ({}, ready: {})", view.name, view.readiness);
    ctx.print_id(&id.to_string(), &human, json!(view))?;
    if ctx.output == OutputMode::Human {
        eprintln!(
            "Stop it with `homebased service stop {id}`. It also stops when this thread is archived or settled, or after {}",
            humantime::format_duration(view_max(&view))
        );
    }
    Ok(ExitCode::SUCCESS)
}

fn view_max(view: &ServiceView) -> Duration {
    Duration::from_secs(view.max_secs)
}

/// The error for a service that did not become ready, after printing its
/// last output lines for a human reader; `--json` carries them in the error
async fn not_ready(ctx: &Ctx, client: &Client, view: &ServiceView) -> AppError {
    let id = view.id;
    let output_tail = match client
        .get(&format!("/v1/services/{id}/log?tail={TAIL_LINES}"))
        .await
    {
        Ok(value) => tail_lines(value.get("log").and_then(Value::as_str).unwrap_or_default()),
        Err(error) => vec![format!("(log unavailable: {error})")],
    };
    if ctx.output != OutputMode::Json {
        eprintln!("last output of {id}:");
        for line in &output_tail {
            eprintln!("  {line}");
        }
    }
    let reason = match (&view.detail, view.state) {
        (Some(detail), _) => detail.clone(),
        (None, ServiceState::Ended) => format!("it ended: {}", view.human_line()),
        (None, _) => format!("it is still {}", view.state.as_str()),
    };
    AppError::ServiceNotReady {
        service: id,
        reason,
        output_tail,
    }
}

/// This process's environment, which the service runs with
///
/// A variable whose name or value is not UTF-8 cannot cross the JSON API and
/// is left out
fn caller_env() -> BTreeMap<String, String> {
    std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .collect()
}

async fn show(client: &Client, id: ServiceId) -> Result<ServiceView, AppError> {
    decode(client.get(&format!("/v1/services/{id}")).await?)
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, AppError> {
    serde_json::from_value(value).map_err(|error| AppError::Internal {
        message: format!("invalid service answer from the daemon: {error}"),
    })
}

async fn list(ctx: &Ctx, ended: bool, all_threads: bool) -> Result<ExitCode, AppError> {
    // a caller outside any session owns nothing it could pick, so it sees all
    let owner = match all_threads {
        true => None,
        false => SubmitOrigin::capture(&ctx.home)?.default_thread()?,
    };
    let mut path = format!("/v1/services?ended={ended}");
    if let Some(owner) = owner {
        path.push_str(&format!("&owner={owner}"));
    }
    let value = Client::new(ctx.home.sock_path()).get(&path).await?;
    let response: ServicesResponse = decode(value.clone())?;
    match ctx.output {
        OutputMode::Json => ctx.print_json(value)?,
        OutputMode::Quiet => {
            for service in &response.services {
                println!("{}", service.id);
            }
        }
        OutputMode::Human if response.services.is_empty() => println!("no services"),
        OutputMode::Human => {
            for service in &response.services {
                println!("{}", service.human_line());
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn log(ctx: &Ctx, id: ServiceId, tail: usize) -> Result<ExitCode, AppError> {
    let path = format!("/v1/services/{id}/log?tail={tail}");
    let value = Client::new(ctx.home.sock_path()).get(&path).await?;
    if ctx.output == OutputMode::Json {
        ctx.print_json(value)?;
        return Ok(ExitCode::SUCCESS);
    }
    let text = value.get("log").and_then(Value::as_str).unwrap_or_default();
    print!("{text}");
    if !text.is_empty() && !text.ends_with('\n') {
        println!();
    }
    Ok(ExitCode::SUCCESS)
}

async fn stop(ctx: &Ctx, id: ServiceId) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    let mut view: ServiceView = decode(
        client
            .post(&format!("/v1/services/{id}/stop"), &json!({}))
            .await?,
    )?;
    let deadline = Instant::now() + STOP_WAIT;
    while view.state != ServiceState::Ended && Instant::now() < deadline {
        tokio::time::sleep(POLL).await;
        view = show(&client, id).await?;
    }
    let human = match view.state {
        ServiceState::Ended => view.human_line(),
        _ => format!(
            "{} is still stopping after {}s; check it with `homebased service list`",
            view.id,
            STOP_WAIT.as_secs()
        ),
    };
    ctx.print_id(&id.to_string(), &human, json!(view))?;
    Ok(ExitCode::SUCCESS)
}
