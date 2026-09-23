//! Optional Tailscale peer discovery.
//!
//! Reads `tailscale status --json` and reports each online peer's Tailscale
//! IPv4 address on the configured Homebased port. Tailscale does not know
//! whether a peer runs Homebased, so most candidates fail their probe; that is
//! expected and costs one bounded request per peer and round.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use chrono::Utc;
use serde::Deserialize;
use tokio::process::Command;
use tokio::task::JoinHandle;

use crate::fleet::address::MachineAddress;
use crate::fleet::directory::{Sighting, SightingProvider};
use crate::fleet::discovery::{DiscoveryEvent, DiscoverySender};

/// macOS App Store and standalone builds ship the CLI inside the app bundle.
const MACOS_APP_CLI: &str = "/Applications/Tailscale.app/Contents/MacOS/Tailscale";

/// Tailscale timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailscaleTimings {
    /// Time between scans.
    pub interval: Duration,
    /// Time limit for one `tailscale status` call.
    pub timeout: Duration,
}

impl Default for TailscaleTimings {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            timeout: Duration::from_secs(5),
        }
    }
}

/// Running Tailscale scanner.
pub struct TailscaleProvider {
    scan: JoinHandle<()>,
}

impl TailscaleProvider {
    /// Start periodic scans that report peers on `port`.
    #[must_use]
    pub fn start(port: u16, events: DiscoverySender, timings: TailscaleTimings) -> Self {
        Self {
            scan: tokio::spawn(scan_loop(port, events, timings)),
        }
    }

    /// Stop scanning.
    pub fn shutdown(self) {
        self.scan.abort();
    }
}

async fn scan_loop(port: u16, events: DiscoverySender, timings: TailscaleTimings) {
    // sightings outlive two missed scans so one slow call does not demote a peer
    let ttl = chrono::Duration::from_std(timings.interval * 3).unwrap_or_default();
    loop {
        let reports = match scan(port, timings.timeout).await {
            Ok(addresses) => {
                let expires_at = Utc::now() + ttl;
                addresses
                    .into_iter()
                    .map(|address| DiscoveryEvent::Seen {
                        sighting: Sighting {
                            address,
                            provider: SightingProvider::Tailscale,
                            expires_at,
                        },
                        claim: None,
                    })
                    .collect()
            }
            Err(message) => vec![DiscoveryEvent::ProviderFailed {
                provider: SightingProvider::Tailscale,
                message,
            }],
        };
        for report in reports {
            if events.send(report).await.is_err() {
                return;
            }
        }
        tokio::time::sleep(timings.interval).await;
    }
}

async fn scan(port: u16, timeout: Duration) -> Result<Vec<MachineAddress>, String> {
    let program = cli_path().ok_or_else(|| "tailscale CLI not found".to_string())?;
    let child = Command::new(&program)
        .args(["status", "--json"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(timeout, child)
        .await
        .map_err(|_| format!("tailscale status timed out after {timeout:?}"))?
        .map_err(|err| format!("run {}: {err}", program.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("tailscale status failed: {}", stderr.trim()));
    }
    parse_status(&output.stdout, port)
}

fn cli_path() -> Option<PathBuf> {
    if let Ok(path) = which::which("tailscale") {
        return Some(path);
    }
    let app = PathBuf::from(MACOS_APP_CLI);
    app.is_file().then_some(app)
}

/// Fields read from `tailscale status --json`. Everything else is ignored:
/// this is another program's output, not a Homebased boundary.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Status {
    #[serde(default)]
    peer: BTreeMap<String, Peer>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Peer {
    #[serde(default)]
    online: bool,
    #[serde(default, rename = "TailscaleIPs")]
    tailscale_ips: Vec<IpAddr>,
}

fn parse_status(stdout: &[u8], port: u16) -> Result<Vec<MachineAddress>, String> {
    let status: Status =
        serde_json::from_slice(stdout).map_err(|err| format!("tailscale status JSON: {err}"))?;
    let mut addresses: Vec<MachineAddress> = status
        .peer
        .values()
        .filter(|peer| peer.online)
        .filter_map(|peer| peer.tailscale_ips.iter().find(|ip| ip.is_ipv4()))
        .map(|ip| MachineAddress::from_socket(SocketAddr::new(*ip, port)))
        .collect();
    addresses.sort();
    addresses.dedup();
    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_online_peers_ipv4_only() {
        let json = br#"{
            "Self": {"HostName": "main", "TailscaleIPs": ["100.64.0.1"]},
            "Peer": {
                "nodekey:a": {"HostName": "code", "Online": true, "TailscaleIPs": ["fd7a:115c:a1e0::2", "100.64.0.2"]},
                "nodekey:b": {"HostName": "old", "Online": false, "TailscaleIPs": ["100.64.0.3"]},
                "nodekey:c": {"HostName": "v6only", "Online": true, "TailscaleIPs": ["fd7a:115c:a1e0::4"]}
            }
        }"#;
        let addresses = parse_status(json, 7677).unwrap();
        let rendered: Vec<String> = addresses.iter().map(ToString::to_string).collect();
        assert_eq!(rendered, vec!["http://100.64.0.2:7677"]);
    }

    #[test]
    fn missing_peer_map_is_empty() {
        assert!(parse_status(b"{}", 7677).unwrap().is_empty());
        assert!(parse_status(b"not json", 7677).is_err());
    }
}
