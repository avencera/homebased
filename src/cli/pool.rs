//! `homebased pool` commands: pools of temporary machines from cloudlease

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Subcommand;
use serde_json::{Value, json};

use crate::client::Client;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::pool::{MachineClass, PoolName};
use crate::queue::ci::SlotCapacity;

use super::{Ctx, OutputMode};

/// Pool subcommands
#[derive(Debug, Subcommand)]
pub enum PoolCommand {
    /// Create a pool: a CI class whose machines cloudlease creates as its jobs need them
    Create {
        /// Pool name, which is also the CI class its jobs name (`ci run --class <name>`)
        name: PoolName,
        /// cloudlease machine class, such as `do-c16`
        #[arg(long = "machine-class")]
        machine_class: MachineClass,
        /// Most machines alive at once
        #[arg(long = "max")]
        max_machines: u32,
        /// No machine of the pool lives longer than this from now, at most 24h
        #[arg(long, value_parser = humantime::parse_duration)]
        deadline: Duration,
        /// CI slots of the class on each machine
        #[arg(long, default_value = "1")]
        slots: SlotCapacity,
        /// A machine with no work for this long releases itself
        #[arg(long = "idle-limit", value_parser = humantime::parse_duration)]
        idle_limit: Option<Duration>,
        /// Running work with no output for this long stops keeping its machine
        #[arg(long = "stall-after", value_parser = humantime::parse_duration)]
        stall_after: Option<Duration>,
        /// A machine that has not joined this long after its create is released
        #[arg(long = "boot-timeout", value_parser = humantime::parse_duration)]
        boot_timeout: Option<Duration>,
        /// File of NAME=value lines every run on the pool's machines gets,
        /// read at each join; it never reaches cloudlease or boot data
        #[arg(long = "env-file")]
        env_file: Option<PathBuf>,
    },
    /// List pools with their machines
    List,
    /// Show one pool with its machines
    Show {
        /// Pool name
        name: PoolName,
    },
    /// Stop a pool: ask for no more machines, release every machine now, and
    /// fail its jobs that have not started
    Release {
        /// Pool name
        name: PoolName,
    },
}

/// Run one pool command
pub async fn run(ctx: &Ctx, command: PoolCommand) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    let value: Value = match command {
        PoolCommand::Create {
            name,
            machine_class,
            max_machines,
            deadline,
            slots,
            idle_limit,
            stall_after,
            boot_timeout,
            env_file,
        } => {
            let env_file = env_file.map(std::path::absolute).transpose()?;
            let format = |duration: Option<Duration>| {
                duration.map(|duration| humantime::format_duration(duration).to_string())
            };
            let body = json!({
                "api_version": API_VERSION,
                "name": name,
                "machine_class": machine_class,
                "max_machines": max_machines,
                "deadline": humantime::format_duration(deadline).to_string(),
                "slots": slots,
                "idle_limit": format(idle_limit),
                "stall_after": format(stall_after),
                "boot_timeout": format(boot_timeout),
                "env_file": env_file,
            });
            client.post("/v1/pools", &body).await?
        }
        PoolCommand::List => client.get("/v1/pools").await?,
        PoolCommand::Show { name } => client.get(&format!("/v1/pools/{name}")).await?,
        PoolCommand::Release { name } => {
            client
                .post(
                    &format!("/v1/pools/{name}/release"),
                    &json!({ "api_version": API_VERSION }),
                )
                .await?
        }
    };
    match ctx.output {
        OutputMode::Json => ctx.print_json(value)?,
        OutputMode::Human | OutputMode::Quiet => {
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
    }
    Ok(ExitCode::SUCCESS)
}
