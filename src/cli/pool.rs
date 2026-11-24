//! `homebased pool` commands: pools of temporary machines from cloudlease

use std::num::NonZeroU32;
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
        /// Most the pool's cloudlease run may spend, in US dollars, such as
        /// 25 or 12.50; cloudlease's own per-run cap when omitted
        #[arg(long = "spend-cap", value_parser = parse_dollars)]
        spend_cap: Option<NonZeroU32>,
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
            spend_cap,
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
                "spend_cap_cents": spend_cap,
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

/// Whole US cents from a dollar amount with at most two decimals
fn parse_dollars(text: &str) -> Result<NonZeroU32, String> {
    let invalid = || format!("{text} is not a dollar amount such as 25 or 12.50");
    let amount = text.strip_prefix('$').unwrap_or(text);
    let (dollars, cents) = amount.split_once('.').unwrap_or((amount, ""));
    let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if dollars.is_empty() || cents.len() > 2 || !digits(dollars) || !digits(cents) {
        return Err(invalid());
    }
    let dollars: u32 = dollars.parse().map_err(|_| invalid())?;
    // "12.5" is 50 cents
    let cents: u32 = format!("{cents:0<2}").parse().map_err(|_| invalid())?;
    let total = dollars
        .checked_mul(100)
        .and_then(|total| total.checked_add(cents))
        .ok_or_else(invalid)?;
    NonZeroU32::new(total).ok_or_else(|| "--spend-cap must be more than 0".to_owned())
}

#[cfg(test)]
mod tests {
    use super::parse_dollars;

    #[test]
    fn dollar_amounts_become_whole_cents() {
        let cents = |text| parse_dollars(text).map(std::num::NonZeroU32::get);
        assert_eq!(cents("25"), Ok(2500));
        assert_eq!(cents("12.5"), Ok(1250));
        assert_eq!(cents("$0.07"), Ok(7));
        for bad in ["0", "0.00", "1.234", ".5", "-1", "1e3", "ten", "99999999"] {
            assert!(parse_dollars(bad).is_err(), "{bad}");
        }
    }
}
