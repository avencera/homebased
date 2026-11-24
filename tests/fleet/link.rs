//! Permanent Fleet machines over the iroh link: per-machine transports in
//! order, a peer allowlist, one connection per pair when both ends dial, and
//! a Fleet whose plain HTTP stays on loopback

use super::{Daemon, register_thread, wait_until};
use std::fs;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use homebased::domain::{TaskId, ThreadId};
use serde_json::{Value, json};

/// A free UDP port for a daemon's link endpoint, fixed so peers' direct
/// addresses hold across its restarts
fn link_port() -> u16 {
    std::net::UdpSocket::bind("0.0.0.0:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// An address of this host that is not loopback, when it has one: where
/// "the network" reaches it
fn network_ip() -> Option<Ipv4Addr> {
    if_addrs::get_if_addrs()
        .ok()?
        .into_iter()
        .find_map(|interface| match interface.ip() {
            IpAddr::V4(ip) if !ip.is_loopback() && !ip.is_link_local() => Some(ip),
            _ => None,
        })
}

fn json_cli(daemon: &Daemon, args: &[&str]) -> Value {
    let output = daemon.cmd().arg("--json").args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// The daemon's endpoint id, from `homebased fleet endpoint-id`
fn endpoint_id(daemon: &Daemon) -> String {
    let id = json_cli(daemon, &["fleet", "endpoint-id"])["endpoint_id"]
        .as_str()
        .unwrap()
        .to_owned();
    // the key outlives the command, so the id is the same every time
    assert_eq!(
        json_cli(daemon, &["fleet", "endpoint-id"])["endpoint_id"],
        id.as_str()
    );
    id
}

/// A `[[fleet.machines]]` entry
fn entry(lines: &[String]) -> String {
    format!("\n[[fleet.machines]]\n{}\n", lines.join("\n"))
}

fn iroh_lines(id: &str, ip: Ipv4Addr, port: u16) -> Vec<String> {
    vec![
        format!("iroh = \"{id}\""),
        format!("iroh_addrs = [\"{ip}:{port}\"]"),
    ]
}

/// Turn on the link at `port`, allow `peers`, declare `machines`, and restart
fn enable_link(daemon: &mut Daemon, port: u16, peers: &[String], machines: &[String]) {
    let peers: Vec<String> = peers.iter().map(|peer| format!("\"{peer}\"")).collect();
    let mut config = fs::read_to_string(&daemon.config).unwrap();
    config.push_str(&format!(
        "\n[fleet.iroh]\nrelays = \"disabled\"\nport = {port}\npeers = [{}]\n",
        peers.join(", ")
    ));
    for machine in machines {
        config.push_str(machine);
    }
    fs::write(&daemon.config, config).unwrap();
    let validate = json_cli(daemon, &["config", "validate"]);
    assert_eq!(validate["valid"], true, "{validate}");
    daemon.restart();
}

/// Callbacks of tasks this daemon submits append to `$HOME/callbacks`
fn record_callbacks(daemon: &mut Daemon) {
    let codex = daemon.user_home.join("codex");
    fs::write(
        &codex,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/callbacks\"\n",
    )
    .unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    daemon.codex_override = Some(codex);
}

/// The peer named `name` in `daemon`'s inventory, once a probe round found it
fn find_peer(daemon: &Daemon, name: &str) -> Option<Value> {
    let inventory = json_cli(daemon, &["fleet", "discover"]);
    inventory["machines"]
        .as_array()?
        .iter()
        .find(|peer| peer["name"] == name && peer["reachability"]["state"] == "reachable")
        .cloned()
}

fn wait_for_peer(daemon: &Daemon, name: &str) -> Value {
    let mut found = None;
    assert!(
        wait_until(Duration::from_secs(40), || {
            found = find_peer(daemon, name);
            found.is_some()
        }),
        "{name} never became reachable: {}",
        json_cli(daemon, &["fleet", "machines"])
    );
    found.unwrap()
}

/// Run a task on `executor` from `origin`, and wait until it ran there and
/// its callback came back to `origin`
fn run_remote(origin: &Daemon, executor: &Daemon, executor_name: &str, mark: &str) {
    let thread = ThreadId(uuid::Uuid::now_v7());
    register_thread(&origin.user_home, &thread.to_string());
    let spec = json!({
        "api_version": 1,
        "thread": thread,
        "name": format!("over the link {mark}"),
        "machine": executor_name,
        "cwd": executor.user_home,
        "timeout": "30m",
        "workload": {
            "type": "task",
            "command": ["/bin/sh", "-c", format!("echo {mark} >> \"$HOME/remote-runs\"")],
        }
    });
    let path = origin.user_home.join(format!("spec-{mark}.json"));
    fs::write(&path, spec.to_string()).unwrap();
    let output = origin
        .cmd()
        .current_dir(&origin.user_home)
        .args(["--json", "task", "submit", "--spec"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "submit {mark}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    let task: TaskId = body["id"].as_str().unwrap().parse().unwrap();

    let runs = executor.user_home.join("remote-runs");
    assert!(
        wait_until(Duration::from_secs(20), || fs::read_to_string(&runs)
            .is_ok_and(|text| text.lines().any(|line| line == mark))),
        "task {task} never ran on {executor_name}"
    );
    let callbacks = origin.user_home.join("callbacks");
    assert!(
        wait_until(Duration::from_secs(20), || fs::read_to_string(&callbacks)
            .is_ok_and(|text| text.contains(&task.to_string()))),
        "the callback of task {task} never reached the origin"
    );
}

/// The local link view's entry for `endpoint`
fn link_peer(daemon: &Daemon, endpoint: &str) -> Value {
    let inventory = json_cli(daemon, &["fleet", "machines"]);
    inventory["local"]["iroh"]["peers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|peer| peer["endpoint"] == endpoint)
        .cloned()
        .unwrap_or_else(|| panic!("{endpoint} is not on the allowlist: {inventory}"))
}

/// A TCP forwarder to `target` that can be cut, standing in for an HTTP
/// path that stops working
struct Proxy {
    address: SocketAddr,
    open: Arc<AtomicBool>,
}

impl Proxy {
    fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let open = Arc::new(AtomicBool::new(true));
        let flag = open.clone();
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                if !flag.load(Ordering::SeqCst) {
                    // a cut path: the connection closes before any answer
                    continue;
                }
                let Ok(upstream) = TcpStream::connect(target) else {
                    continue;
                };
                pipe(client.try_clone().unwrap(), upstream.try_clone().unwrap());
                pipe(upstream, client);
            }
        });
        Self { address, open }
    }

    fn cut(&self) {
        self.open.store(false, Ordering::SeqCst);
    }
}

fn pipe(mut from: TcpStream, mut to: TcpStream) {
    std::thread::spawn(move || {
        let mut buffer = [0; 16 * 1024];
        while let Ok(read) = from.read(&mut buffer) {
            if read == 0 || to.write_all(&buffer[..read]).is_err() {
                break;
            }
        }
        let _ = to.shutdown(std::net::Shutdown::Write);
    });
}

#[test]
fn http_first_falls_back_to_the_link_when_http_stops_answering() {
    let mut origin = Daemon::start("iroh-origin", true);
    let mut executor = Daemon::start("iroh-executor", true);
    let (origin_id, executor_id) = (endpoint_id(&origin), endpoint_id(&executor));
    let (origin_port, executor_port) = (link_port(), link_port());
    let proxy = Proxy::start(SocketAddr::from(([127, 0, 0, 1], executor.port)));
    let loopback = Ipv4Addr::LOCALHOST;

    record_callbacks(&mut origin);
    // the first rollout stage: HTTP as today, the link as its fallback
    let mut http_then_iroh = vec![format!("address = \"http://{}\"", proxy.address)];
    http_then_iroh.extend(iroh_lines(&executor_id, loopback, executor_port));
    enable_link(&mut origin, origin_port, &[], &[entry(&http_then_iroh)]);
    // the executor sends events back over the link alone
    let back = entry(&iroh_lines(&origin_id, loopback, origin_port));
    enable_link(&mut executor, executor_port, &[], &[back]);

    let peer = wait_for_peer(&origin, "iroh-executor");
    let wait_for_both = || {
        wait_until(Duration::from_secs(40), || {
            find_peer(&origin, "iroh-executor").is_some_and(|peer| {
                peer["addresses"].as_array().unwrap().len() == 2
                    && peer["addresses"][1]["verified_at"].is_string()
            })
        })
    };
    assert!(wait_for_both(), "{peer}");
    let peer = find_peer(&origin, "iroh-executor").unwrap();
    let addresses = peer["addresses"].as_array().unwrap();
    assert_eq!(addresses[0]["address"], format!("http://{}", proxy.address));
    assert!(addresses[0].get("iroh").is_none(), "{peer}");
    assert_eq!(addresses[1]["iroh"], executor_id.as_str());
    assert_eq!(addresses[1]["source"], "configured");
    assert_eq!(peer["iroh"], executor_id.as_str());
    run_remote(&origin, &executor, "iroh-executor", "over-http");

    proxy.cut();
    run_remote(&origin, &executor, "iroh-executor", "over-the-link");
}

#[test]
fn link_only_peers_work_while_their_http_stays_on_loopback() {
    let mut origin = Daemon::start("iroh-only-origin", true);
    let executor = Daemon::start("iroh-only-executor", true);
    let (origin_id, executor_id) = (endpoint_id(&origin), endpoint_id(&executor));
    let (origin_port, executor_port) = (link_port(), link_port());
    // the link crosses the network when the host has one, where the
    // executor's plain HTTP, bound to loopback, does not answer
    let ip = network_ip();
    if let Some(ip) = ip {
        let refused = TcpStream::connect_timeout(
            &SocketAddr::from((ip, executor.port)),
            Duration::from_secs(2),
        );
        assert!(
            refused.is_err(),
            "loopback-bound HTTP answered on {ip}:{}",
            executor.port
        );
    } else {
        eprintln!("no non-loopback IPv4 address; the link runs over loopback");
    }
    let ip = ip.unwrap_or(Ipv4Addr::LOCALHOST);

    record_callbacks(&mut origin);
    let to_executor = entry(&iroh_lines(&executor_id, ip, executor_port));
    enable_link(&mut origin, origin_port, &[], &[to_executor]);
    let mut executor = executor;
    let to_origin = entry(&iroh_lines(&origin_id, ip, origin_port));
    enable_link(&mut executor, executor_port, &[], &[to_origin]);
    // an HTTP address that does answer for the executor, which a link-only
    // peer must still never be routed to
    json_cli(
        &origin,
        &[
            "fleet",
            "add",
            &format!("http://127.0.0.1:{}", executor.port),
        ],
    );

    let peer = wait_for_peer(&origin, "iroh-only-executor");
    let addresses = peer["addresses"].as_array().unwrap();
    assert!(
        !addresses.is_empty()
            && addresses
                .iter()
                .all(|address| address["iroh"] == executor_id.as_str()),
        "a link-only peer has an HTTP route: {peer}"
    );
    run_remote(&origin, &executor, "iroh-only-executor", "link-only");
}

#[test]
fn ids_off_the_allowlist_get_no_fleet_api() {
    let mut daemon = Daemon::start("iroh-gate", true);
    let port = link_port();
    let allowed = iroh::SecretKey::generate();
    let stranger = iroh::SecretKey::generate();
    enable_link(&mut daemon, port, &[allowed.public().to_string()], &[]);
    let gate: iroh::EndpointId = endpoint_id(&daemon).parse().unwrap();
    let gate = iroh::EndpointAddr::from_parts(
        gate,
        [iroh::TransportAddr::Ip(SocketAddr::from((
            [127, 0, 0, 1],
            port,
        )))],
    );

    let probe = "GET /v1/cluster/machine HTTP/1.1\r\nHost: gate\r\nConnection: close\r\n\r\n";
    let answer = link_answer(&gate, allowed, probe).expect("an allowed id is answered");
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    assert!(answer.contains("iroh-gate"), "{answer}");

    assert_eq!(link_answer(&gate, stranger.clone(), probe), None);
    // a daemon that coordinates no pools does not take joins either
    let join = "POST /v1/cluster/pool/join HTTP/1.1\r\nHost: gate\r\n\
                Content-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
    assert_eq!(link_answer(&gate, stranger, join), None);
}

#[test]
fn two_machines_that_dial_each_other_keep_one_connection() {
    let mut left = Daemon::start("iroh-left", true);
    let mut right = Daemon::start("iroh-right", true);
    let (left_id, right_id) = (endpoint_id(&left), endpoint_id(&right));
    let (left_port, right_port) = (link_port(), link_port());
    let loopback = Ipv4Addr::LOCALHOST;

    // write both configs, then start both at once, so both dial
    left.stop();
    right.stop();
    for (daemon, port, peer, peer_port) in [
        (&mut left, left_port, &right_id, right_port),
        (&mut right, right_port, &left_id, left_port),
    ] {
        let mut config = fs::read_to_string(&daemon.config).unwrap();
        config.push_str(&format!(
            "\n[fleet.iroh]\nrelays = \"disabled\"\nport = {port}\n{}",
            entry(&iroh_lines(peer, loopback, peer_port))
        ));
        fs::write(&daemon.config, config).unwrap();
    }
    std::thread::scope(|scope| {
        scope.spawn(|| left.spawn());
        scope.spawn(|| right.spawn());
    });

    let connected = |daemon: &Daemon, peer: &str| link_peer(daemon, peer)["connection"].clone();
    let both = || (connected(&left, &right_id), connected(&right, &left_id));
    // both ends use one connection, and keep it rather than closing each
    // other's in turn: a redial comes every 2 seconds, so a churning pair
    // never holds still for 6
    let mut stable = 0;
    let mut last = both();
    assert!(
        wait_until(Duration::from_secs(30), || {
            std::thread::sleep(Duration::from_millis(500));
            let now = both();
            stable = if now == last && now.0.is_object() && now.1.is_object() {
                stable + 1
            } else {
                0
            };
            last = now;
            stable >= 12
        }),
        "the pair never settled on one connection: {last:?}"
    );
    let (at_left, at_right) = last;
    // whichever end dialed first while the other was down, or the smaller
    // id when both dialed at once, both ends name the same dialer
    let flip = |dialer: &Value| match dialer.as_str() {
        Some("local") => "peer",
        Some("peer") => "local",
        other => panic!("unexpected dialer {other:?}"),
    };
    assert_eq!(
        at_right["dialed_by"],
        flip(&at_left["dialed_by"]),
        "{at_left} {at_right}"
    );
    assert_eq!(link_peer(&left, &right_id)["grant"], "peer");

    // a peer that died without closing the connection is served again well
    // before the dead connection's 12-second idle timeout
    right.stop();
    right.spawn();
    assert!(
        wait_until(Duration::from_secs(8), || {
            let now = connected(&left, &right_id);
            now.is_object()
                && now["since"] != at_left["since"]
                && connected(&right, &left_id).is_object()
        }),
        "the restarted peer was not served again: {}",
        connected(&left, &right_id)
    );
}

/// The raw answer to one HTTP request over the link to `daemon`, as the
/// endpoint with `key`; `None` when the daemon closed the connection instead
fn link_answer(daemon: &iroh::EndpointAddr, key: iroh::SecretKey, request: &str) -> Option<String> {
    let (daemon, request) = (daemon.clone(), request.to_owned());
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                .relay_mode(iroh::RelayMode::Disabled)
                .secret_key(key)
                .bind()
                .await
                .unwrap();
            let connection = endpoint.connect(daemon, b"homebased/fleet/1").await.ok()?;
            let (mut send, mut recv) = connection.open_bi().await.ok()?;
            // the request says `Connection: close`, so the answer ends the
            // exchange; the stream stays open meanwhile, as a TCP client's would
            send.write_all(request.as_bytes()).await.ok()?;
            let answer = recv.read_to_end(64 * 1024).await.ok()?;
            endpoint.close().await;
            (!answer.is_empty()).then(|| String::from_utf8_lossy(&answer).into_owned())
        })
}
