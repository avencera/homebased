//! `homebased fleet` commands over the local Unix socket.

use std::process::ExitCode;

use clap::Subcommand;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::client::Client;
use crate::daemon::fleet_api::{ChangeBody, DiscoverBody, MachinesBody, ProbeBody};
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::address::MachineAddress;
use crate::fleet::directory::PeerView;
use crate::machine::{MachineId, MachineName};

use super::{Ctx, OutputMode};

/// Fleet subcommands.
#[derive(Debug, Subcommand)]
pub enum FleetCommand {
    /// List local and known machines.
    Machines,
    /// Probe all known addresses now.
    Discover,
    /// Probe one machine or HTTP address.
    Probe { machine_or_address: String },
    /// Add one durable explicit address.
    Add { address: MachineAddress },
    /// Remove one explicit address or forget one machine.
    Remove { machine_or_address: String },
}

/// Run one fleet command.
pub async fn run(ctx: &Ctx, command: FleetCommand) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    let value = match command {
        FleetCommand::Machines => {
            let body: MachinesBody = decode(client.get("/v1/fleet/machines").await?)?;
            serde_json::to_value(body)?
        }
        FleetCommand::Discover => {
            let body: DiscoverBody = decode(
                client
                    .post("/v1/fleet/discover", &json!({ "api_version": API_VERSION }))
                    .await?,
            )?;
            serde_json::to_value(body)?
        }
        FleetCommand::Probe { machine_or_address } => {
            let address = if machine_or_address.starts_with("http://") {
                machine_or_address
                    .parse::<MachineAddress>()
                    .map_err(|err| AppError::Usage {
                        message: err.to_string(),
                    })?
            } else {
                let inventory: MachinesBody = decode(client.get("/v1/fleet/machines").await?)?;
                let peer = resolve_peer(&inventory, &machine_or_address)?;
                peer.addresses
                    .first()
                    .ok_or_else(|| AppError::MachineUnavailable {
                        machine: peer.machine,
                        message: "no known address".into(),
                    })?
                    .address
                    .clone()
            };
            let body: ProbeBody = decode(
                client
                    .post(
                        "/v1/fleet/probe",
                        &json!({ "api_version": API_VERSION, "address": address }),
                    )
                    .await?,
            )?;
            serde_json::to_value(body)?
        }
        FleetCommand::Add { address } => {
            let body: ChangeBody = decode(
                client
                    .post(
                        "/v1/fleet/add",
                        &json!({ "api_version": API_VERSION, "address": address }),
                    )
                    .await?,
            )?;
            serde_json::to_value(body)?
        }
        FleetCommand::Remove { machine_or_address } => {
            let target = if machine_or_address.starts_with("http://")
                || machine_or_address.parse::<MachineId>().is_ok()
            {
                machine_or_address
            } else {
                let inventory: MachinesBody = decode(client.get("/v1/fleet/machines").await?)?;
                resolve_peer(&inventory, &machine_or_address)?
                    .machine
                    .to_string()
            };
            let body: ChangeBody = decode(
                client
                    .post(
                        "/v1/fleet/remove",
                        &json!({ "api_version": API_VERSION, "machine_or_address": target }),
                    )
                    .await?,
            )?;
            serde_json::to_value(body)?
        }
    };
    match ctx.output {
        OutputMode::Json => ctx.print_json(value)?,
        OutputMode::Human | OutputMode::Quiet => {
            println!("{}", serde_json::to_string_pretty(&value)?)
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, AppError> {
    if value.get("api_version").and_then(Value::as_u64) != Some(u64::from(API_VERSION)) {
        return Err(AppError::Internal {
            message: "unexpected fleet API version".into(),
        });
    }
    serde_json::from_value(value).map_err(|err| AppError::Internal {
        message: format!("invalid fleet response: {err}"),
    })
}

fn resolve_peer<'a>(inventory: &'a MachinesBody, target: &str) -> Result<&'a PeerView, AppError> {
    let matches: Vec<_> = inventory
        .machines
        .iter()
        .filter(|peer| peer.machine.to_string() == target || peer.name.as_str() == target)
        .collect();
    if matches.len() > 1 {
        return Err(AppError::DuplicateMachineName {
            name: MachineName::parse(target).map_err(|err| AppError::Usage {
                message: err.to_string(),
            })?,
            machines: matches.iter().map(|peer| peer.machine).collect(),
        });
    }
    matches
        .first()
        .copied()
        .ok_or_else(|| AppError::MachineNotFound {
            machine: target.into(),
        })
}
