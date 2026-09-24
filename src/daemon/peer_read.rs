//! Versioned JSON reads of `/v1/cluster/*` routes on one peer daemon
//!
//! Every fleet-wide view reads its peers the same way: connect through the
//! verified directory, bound the time and body size, and refuse a body from
//! another API version before the typed parse

use std::time::Duration;

use axum::http::StatusCode;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::domain::API_VERSION;
use crate::fleet::http::ClusterClient;
use crate::fleet::runtime::FleetHandle;
use crate::machine::MachineId;

/// Time limit for one peer read, connect through body
pub(super) const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Upper bound on one peer read body
pub(super) const READ_MAX_BODY: usize = 8 * 1024 * 1024;

/// GET one cluster route on `machine` and parse its versioned body
///
/// The error is display text for a view that reports the peer as unavailable
pub(super) async fn read_peer<T: DeserializeOwned>(
    fleet: &FleetHandle,
    machine: MachineId,
    path: &str,
) -> Result<T, String> {
    let destination = fleet
        .connect(machine)
        .await
        .map_err(|error| error.to_string())?;
    let response = ClusterClient::new(READ_TIMEOUT, READ_MAX_BODY)
        .get(&destination.address, path)
        .await
        .map_err(|error| error.to_string())?;
    if response.status != StatusCode::OK {
        return Err(format!("peer returned HTTP {}", response.status));
    }
    let value: Value = serde_json::from_slice(&response.body)
        .map_err(|error| format!("invalid peer response: {error}"))?;
    if value.get("api_version").and_then(Value::as_u64) != Some(u64::from(API_VERSION)) {
        return Err("peer response uses an unsupported API version".into());
    }
    serde_json::from_value(value).map_err(|error| format!("invalid peer response: {error}"))
}
