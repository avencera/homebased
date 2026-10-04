//! Peer identity, discovery, configuration, and cluster reads

use super::{Daemon, wait_for_probe, wait_until};
use homebased::config::{Discovery, FleetSettings};
use homebased::fleet::address::MachineAddress;
use homebased::fleet::directory::LocalMachine;
use homebased::fleet::http::ClusterClient;
use homebased::fleet::identity::IdentityStatus;
use homebased::fleet::probe::{ProbeError, probe};
use homebased::fleet::protocol::SUPPORTED_PROTOCOLS;
use homebased::fleet::runtime::{FleetHandle, FleetRuntime, FleetStart, RuntimeTimings};
use homebased::machine::{BootId, LocalIdentity, MachineId, MachineName};
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// In-process observer with its own identity and peer file. Its background
/// tasks end with the test's tokio runtime
struct Observer {
    _dir: TempDir,
    _runtime: FleetRuntime,
    handle: FleetHandle,
}

impl Observer {
    fn start(configured: Vec<MachineAddress>) -> Self {
        Self::start_with(configured, false)
    }

    fn start_with(configured: Vec<MachineAddress>, mdns: bool) -> Self {
        let dir = TempDir::new().unwrap();
        let local = LocalMachine {
            identity: LocalIdentity {
                machine: MachineId::new(),
                boot: BootId::new(),
            },
            name: MachineName::parse("observer").unwrap(),
            protocol: SUPPORTED_PROTOCOLS,
        };
        let runtime = FleetRuntime::start(FleetStart {
            local,
            settings: FleetSettings {
                discovery: Discovery {
                    mdns,
                    tailscale: None,
                },
                machines: configured,
            },
            listener: None,
            peers_path: dir.path().join("fleet-peers.json"),
            timings: RuntimeTimings {
                round_interval: Duration::from_secs(3600),
                recheck_delay: Duration::from_millis(300),
                ..RuntimeTimings::default()
            },
        })
        .unwrap();
        let handle = runtime.handle();
        Self {
            _dir: dir,
            _runtime: runtime,
            handle,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_peers_survive_restart_and_rename_without_conflict() {
    let mut alpha = Daemon::start("alpha", true);
    let beta = Daemon::start("beta", true);
    wait_for_probe(&alpha.address()).await;
    wait_for_probe(&beta.address()).await;
    let observer = Observer::start(vec![alpha.address(), beta.address()]);

    let report = observer.handle.discover_now().await;
    assert_eq!(report.answered, 2, "{report:?}");
    assert!(report.duplicates.is_empty(), "{report:?}");
    let peers = observer.handle.peers().await;
    let names: Vec<&str> = peers.iter().map(|peer| peer.name.as_str()).collect();
    assert_eq!(names, vec!["alpha", "beta"]);
    let alpha_id = alpha.machine_id();
    let verified = observer.handle.connect(alpha_id).await.unwrap();
    assert_eq!(verified.machine, alpha_id);
    assert_eq!(verified.address, alpha.address());
    let first_boot = verified.boot;

    // ordinary restart: same machine UUID, new boot UUID, no conflict
    alpha.restart();
    wait_for_probe(&alpha.address()).await;
    let report = observer.handle.discover_now().await;
    assert!(report.duplicates.is_empty(), "{report:?}");
    let verified = observer.handle.connect(alpha_id).await.unwrap();
    assert_ne!(verified.boot, first_boot);
    assert_eq!(alpha.machine_id(), alpha_id);

    // rename across a restart updates the same machine record
    alpha.write_config("gamma", true);
    alpha.restart();
    wait_for_probe(&alpha.address()).await;
    let report = observer.handle.discover_now().await;
    assert!(report.duplicates.is_empty(), "{report:?}");
    let peer = observer
        .handle
        .peers()
        .await
        .into_iter()
        .find(|peer| peer.machine == alpha_id)
        .unwrap();
    assert_eq!(peer.name.as_str(), "gamma");
    assert_eq!(peer.identity, IdentityStatus::Consistent);
    assert_eq!(observer.handle.peers().await.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn cloned_state_directory_is_a_duplicate_identity() {
    let alpha = Daemon::start("alpha", true);
    wait_for_probe(&alpha.address()).await;
    let alpha_id = alpha.machine_id();

    // a clone copies the source installation's machine UUID
    let mut clone = Daemon::start("alpha-copy", true);
    clone.stop();
    fs::copy(alpha.home.join("machine-id"), clone.home.join("machine-id")).unwrap();
    clone.spawn();
    wait_for_probe(&clone.address()).await;
    assert_eq!(clone.machine_id(), alpha_id);

    let observer = Observer::start(vec![alpha.address(), clone.address()]);
    let report = observer.handle.discover_now().await;
    assert_eq!(report.duplicates, vec![alpha_id], "{report:?}");
    let err = observer.handle.connect(alpha_id).await.unwrap_err();
    assert_eq!(err.code(), "duplicate_machine_identity");

    // once the clone is gone, a later round clears the conflict
    clone.stop();
    let report = observer.handle.discover_now().await;
    assert!(report.duplicates.is_empty(), "{report:?}");
    assert!(observer.handle.connect(alpha_id).await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_address_is_never_used_for_another_installation() {
    let mut alpha = Daemon::start("alpha", true);
    wait_for_probe(&alpha.address()).await;
    let alpha_id = alpha.machine_id();
    let observer = Observer::start(Vec::new());
    observer.handle.add_explicit(alpha.address()).await;
    observer.handle.discover_now().await;
    observer.handle.connect(alpha_id).await.unwrap();

    // another installation takes over the port
    alpha.stop();
    let mut other = Daemon::start("other", true);
    other.stop();
    other.port = alpha.port;
    other.spawn();
    wait_for_probe(&other.address()).await;

    let err = observer.handle.connect(alpha_id).await.unwrap_err();
    assert_eq!(err.code(), "machine_unavailable", "{err}");
    let other_id = other.machine_id();
    assert_ne!(other_id, alpha_id);
    // the address moved to the installation that answered; the alpha record stays
    let verified = observer.handle.connect(other_id).await.unwrap();
    assert_eq!(verified.address, alpha.address());
    assert!(
        observer
            .handle
            .peers()
            .await
            .iter()
            .any(|peer| peer.machine == alpha_id && peer.addresses.is_empty())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fleet_disabled_daemon_has_no_cluster_routes() {
    let daemon = Daemon::start("solo", false);
    let client = ClusterClient::default();
    let address = daemon.address();
    let start = Instant::now();
    let result = loop {
        let result = probe(&client, &address, SUPPORTED_PROTOCOLS).await;
        if !matches!(result, Err(ProbeError::Transport(_))) || start.elapsed().as_secs() > 10 {
            break result;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        matches!(result, Err(ProbeError::Status { status, .. }) if status == 404),
        "{result:?}"
    );
    // machine identity still exists for local task ownership
    assert!(daemon.home.join("machine-id").is_file());
}

fn validate(config: &Path) -> (i32, Value) {
    let output = Command::new(assert_cmd::cargo::cargo_bin("homebased"))
        .args(["--json", "config", "validate"])
        .env("HOMEBASED_CONFIG", config)
        .output()
        .unwrap();
    let stream = if output.status.success() {
        &output.stdout
    } else {
        &output.stderr
    };
    (
        output.status.code().unwrap(),
        serde_json::from_slice(stream).unwrap(),
    )
}

#[test]
fn config_validate_reports_fleet_and_rejects_bad_files() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(
        &path,
        "[fleet]\nenabled = true\nmachine_name = \"code\"\n[[fleet.machines]]\naddress = \"http://main:7677\"\n",
    )
    .unwrap();
    let (code, body) = validate(&path);
    assert_eq!(code, 0, "{body}");
    assert_eq!(body["valid"], true);
    assert_eq!(body["source"], "explicit");
    assert_eq!(body["machine_name"], "code");
    assert_eq!(body["fleet"]["state"], "enabled");
    assert_eq!(body["fleet"]["machines"][0], "http://main:7677");

    fs::write(&path, "[fleet]\nenabled = yes\n").unwrap();
    let (code, body) = validate(&path);
    assert_eq!(code, 2, "{body}");
    assert_eq!(body["error"]["code"], "config_invalid");

    let (code, body) = validate(&dir.path().join("missing.toml"));
    assert_eq!(code, 2, "{body}");
    assert_eq!(body["error"]["code"], "config_invalid");
}

/// Uses real multicast on the host network, so it is opt-in:
/// `cargo test --test fleet -- --ignored mdns`
#[tokio::test(flavor = "multi_thread")]
#[ignore = "uses host multicast networking"]
async fn mdns_discovers_a_lan_peer() {
    let daemon = Daemon::start_on("lanpeer", true, true, "0.0.0.0");
    let local: MachineAddress = format!("http://127.0.0.1:{}", daemon.port).parse().unwrap();
    wait_for_probe(&local).await;
    let machine = daemon.machine_id();
    let observer = Observer::start_with(Vec::new(), true);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(20) {
        observer.handle.discover_now().await;
        if let Some(peer) = observer
            .handle
            .peers()
            .await
            .into_iter()
            .find(|peer| peer.machine == machine)
        {
            assert!(
                peer.addresses.iter().any(|ranked| ranked.source
                    == homebased::fleet::address::AddressSource::Lan
                    || ranked.source == homebased::fleet::address::AddressSource::Tailscale),
                "{peer:?}"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("mDNS never reported the peer");
}

#[tokio::test(flavor = "multi_thread")]
async fn fleet_cli_and_cluster_reads_keep_machine_boundaries() {
    let alpha = Daemon::start("alpha", true);
    let mut beta = Daemon::start("beta", true);
    wait_for_probe(&alpha.address()).await;
    wait_for_probe(&beta.address()).await;

    let output = alpha
        .cmd()
        .args(["--json", "fleet", "machines"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let inventory: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        inventory["local"]["machine"],
        alpha.machine_id().to_string()
    );
    assert!(inventory["machines"].as_array().unwrap().is_empty());

    let output = alpha
        .cmd()
        .args(["--json", "fleet", "add", &beta.address().to_string()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = alpha
        .cmd()
        .args(["--json", "fleet", "discover"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let discovered: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        discovered["machines"][0]["machine"],
        beta.machine_id().to_string()
    );
    let output = alpha
        .cmd()
        .args(["--json", "fleet", "probe", "beta"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let probed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(probed["machine"]["machine"], beta.machine_id().to_string());

    let client = ClusterClient::default();
    let task = homebased::domain::TaskId::new();
    let wrong = format!(
        "/v1/cluster/tasks/{task}?api_version=1&destination_machine={}",
        alpha.machine_id()
    );
    let response = client.get(&beta.address(), &wrong).await.unwrap();
    assert_eq!(response.status.as_u16(), 409);
    let right = format!(
        "/v1/cluster/tasks/{task}?api_version=1&destination_machine={}",
        beta.machine_id()
    );
    let response = client.get(&beta.address(), &right).await.unwrap();
    assert_eq!(response.status.as_u16(), 404);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert!(body["execution"].is_null());

    let socket = homebased::client::Client::new(alpha.home.join("homebased.sock"));
    let lookup = socket
        .get(&format!("/v1/fleet/tasks/{task}"))
        .await
        .unwrap();
    assert_eq!(lookup["result"], "not_found");
    let beta_id = beta.machine_id();
    beta.stop();
    let lookup = socket
        .get(&format!("/v1/fleet/tasks/{task}"))
        .await
        .unwrap();
    assert_eq!(lookup["result"], "incomplete");
    assert_eq!(lookup["unchecked"][0]["machine"], beta_id.to_string());
    let output = alpha
        .cmd()
        .args(["--json", "fleet", "remove", "beta"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let removed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(removed["changed"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn local_duplicate_identity_recovers_after_the_clone_goes_offline() {
    use std::io::Write;

    let original = Daemon::start("identity-original", true);
    let mut clone = Daemon::start("identity-clone", true);
    clone.stop();
    fs::copy(
        original.home.join("machine-id"),
        clone.home.join("machine-id"),
    )
    .unwrap();
    let mut config = fs::OpenOptions::new()
        .append(true)
        .open(&clone.config)
        .unwrap();
    writeln!(
        config,
        "[[fleet.machines]]\naddress = \"{}\"",
        original.address()
    )
    .unwrap();
    clone.spawn();
    assert_eq!(clone.machine_id(), original.machine_id());

    assert!(wait_until(Duration::from_secs(10), || {
        clone
            .cmd()
            .args(["--json", "fleet", "machines"])
            .output()
            .is_ok_and(|output| {
                output.status.success()
                    && serde_json::from_slice::<Value>(&output.stdout).is_ok_and(|inventory| {
                        inventory["local"]["identity"]["state"] == "duplicate_machine_identity"
                    })
            })
    }));

    let mut original = original;
    original.stop();
    for expected in ["duplicate_machine_identity", "consistent"] {
        let output = clone
            .cmd()
            .args(["--json", "fleet", "discover"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let inventory: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(inventory["local"]["identity"]["state"], expected);
    }
}
