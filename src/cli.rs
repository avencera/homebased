//! Clap tree, `--json`/`--quiet` output, error shape.

pub mod daemon;
pub mod task;

use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use serde_json::Value;

use crate::domain::TaskId;
use crate::error::AppError;
use crate::home::Home;

pub(crate) const AFTER_HELP: &str = "\
Exit codes:
  0  success
  1  error
  2  usage
  3  not found
  4  permission
  5  conflict
";

/// Supervise long-running agent and general task workloads and report them to a Codex thread.
#[derive(Debug, Parser)]
#[command(
    name = "homebased",
    version,
    about,
    after_help = AFTER_HELP,
    after_long_help = AFTER_HELP
)]
pub struct Cli {
    /// JSON on stdout. Conflicts with `--quiet`.
    #[arg(long, global = true, conflicts_with = "quiet")]
    pub json: bool,
    /// Bare ids, one per line. Conflicts with `--json`.
    #[arg(long, global = true, conflicts_with = "json")]
    pub quiet: bool,
    /// State directory. Overrides `HOMEBASED_HOME`.
    #[arg(long, global = true, env = "HOMEBASED_HOME")]
    pub home: Option<PathBuf>,
    /// Subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Host-unit and serve lifecycle.
    Daemon {
        /// Daemon subcommand.
        #[command(subcommand)]
        command: daemon::DaemonCommand,
    },
    /// Submit, inspect, cancel, and report tasks.
    Task {
        /// Task subcommand.
        #[command(subcommand)]
        command: task::TaskCommand,
    },
    /// Print the version.
    Version,
    /// Hidden worker parent of one agent.
    #[command(name = "task-run", hide = true)]
    TaskRun {
        /// State directory.
        #[arg(long)]
        home: PathBuf,
        /// Task id.
        #[arg(long)]
        id: TaskId,
        /// Inherited flock file descriptor.
        #[arg(long)]
        lock_fd: i32,
    },
}

/// Output mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Human text, color only on TTY without `NO_COLOR`.
    Human,
    /// JSON objects with `api_version`.
    Json,
    /// Bare values.
    Quiet,
}

impl OutputMode {
    /// From global flags.
    #[must_use]
    pub fn from_flags(json: bool, quiet: bool) -> Self {
        if json {
            Self::Json
        } else if quiet {
            Self::Quiet
        } else {
            Self::Human
        }
    }

    /// Whether stdout should use color.
    #[must_use]
    pub fn color(self) -> bool {
        if self != Self::Human {
            return false;
        }
        if std::env::var_os("NO_COLOR").is_some() {
            return false;
        }
        io::stdout().is_terminal()
    }
}

/// Shared CLI context.
pub struct Ctx {
    /// Output mode.
    pub output: OutputMode,
    /// State dir.
    pub home: Home,
}

impl Ctx {
    fn new(cli: &Cli) -> Result<Self, AppError> {
        Ok(Self {
            output: OutputMode::from_flags(cli.json, cli.quiet),
            home: Home::resolve(cli.home.clone())?,
        })
    }

    /// Print a JSON object (adds `api_version` if missing).
    pub fn print_json(&self, mut value: Value) -> Result<(), AppError> {
        if let Some(obj) = value.as_object_mut() {
            obj.entry("api_version")
                .or_insert(Value::from(crate::domain::API_VERSION));
        }
        println!("{}", serde_json::to_string_pretty(&value)?);
        Ok(())
    }

    /// Print a bare id in quiet mode, JSON, or human line.
    pub fn print_id(&self, id: &str, human: &str, json: Value) -> Result<(), AppError> {
        match self.output {
            OutputMode::Quiet => {
                println!("{id}");
                Ok(())
            }
            OutputMode::Json => self.print_json(json),
            OutputMode::Human => {
                println!("{human}");
                Ok(())
            }
        }
    }

    /// Write an error to stderr.
    pub fn print_error(&self, err: &AppError) {
        print_error(self.output, err);
    }
}

/// Write an error to stderr in the requested output mode.
fn print_error(output: OutputMode, err: &AppError) {
    // stderr is already failing if this write fails; there is nowhere left to report
    let _ = match output {
        OutputMode::Json => writeln!(io::stderr(), "{}", err.to_json()),
        OutputMode::Human | OutputMode::Quiet => {
            writeln!(io::stderr(), "error: {err} [{}]", err.code())
        }
    };
}

/// Parse argv and run.
pub async fn run() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            // clap already wrote its own message
            let _ = err.print();
            let code = u8::try_from(err.exit_code()).unwrap_or(2);
            return ExitCode::from(code);
        }
    };
    // the mode comes from the parsed flags, so a failure before `Ctx` exists
    // still reports in the shape the caller asked for
    let output = OutputMode::from_flags(cli.json, cli.quiet);
    match dispatch(cli).await {
        Ok(code) => code,
        Err(err) => {
            print_error(output, &err);
            err.to_exit_code()
        }
    }
}

async fn dispatch(cli: Cli) -> Result<ExitCode, AppError> {
    if let Command::TaskRun { home, id, lock_fd } = &cli.command {
        let home = Home::resolve(Some(home.clone()))?;
        crate::runner::run(home, *id, *lock_fd).await?;
        return Ok(ExitCode::SUCCESS);
    }
    let ctx = Ctx::new(&cli)?;
    match cli.command {
        Command::Daemon { command } => daemon::run(&ctx, command).await,
        Command::Task { command } => task::run(&ctx, command).await,
        Command::Version => {
            version(&ctx)?;
            Ok(ExitCode::SUCCESS)
        }
        Command::TaskRun { .. } => unreachable!("handled above"),
    }
}

fn version(ctx: &Ctx) -> Result<(), AppError> {
    let ver = env!("CARGO_PKG_VERSION");
    ctx.print_id(
        ver,
        ver,
        serde_json::json!({
            "api_version": crate::domain::API_VERSION,
            "version": ver,
            "name": "homebased",
        }),
    )
}
