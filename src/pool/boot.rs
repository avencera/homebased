//! What a temporary machine runs at boot
//!
//! cloudlease writes `lease.json`, then writes these files and runs these
//! commands. The script installs the static Homebased release that matches
//! the coordinator and starts its daemon on loopback; the daemon dials the
//! coordinator's iroh endpoint and joins. Nothing here is secret except the
//! single-use join token

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use serde::{Deserialize, Serialize};

use super::{JoinToken, PoolName, SlotId};
use crate::fleet::iroh::{LinkAddr, Relays};
use crate::machine::MachineName;

/// Where cloudlease writes the machine's lease credential
pub const LEASE_FILE: &str = "/etc/cloudlease/lease.json";

/// Where the boot data puts the machine's Homebased config
pub const CONFIG_FILE: &str = "/etc/homebased/config.toml";

const SCRIPT_FILE: &str = "/usr/local/lib/homebased/boot.sh";
const UNIT_FILE: &str = "/etc/systemd/system/homebased.service";

/// Loopback address where a temporary machine's Fleet reaches its coordinator
pub const DEFAULT_LINK_LISTEN: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7678));

/// Loopback address a temporary machine's daemon serves its own API on; the
/// coordinator reaches it over the link
const MACHINE_WEB_LISTEN: &str = "127.0.0.1:7677";

/// Where a temporary machine gets the Homebased it installs
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum MachineInstall {
    /// A GitHub release, such as `0.37.0`, checked against the `.sha256` of
    /// the same release
    Release {
        /// Release version, without the `v`
        version: String,
    },
    /// One archive the coordinator pins by its sha256, for a build that has
    /// no release; the archive must hold the Linux build for the machine's
    /// architecture
    Archive {
        /// `https` URL of the `.tar.gz`
        url: String,
        /// Its sha256, 64 lowercase hex digits
        sha256: String,
    },
}

impl MachineInstall {
    /// A release install, checking that `version` looks like one
    pub fn release(version: &str) -> Result<Self, String> {
        let version = version.trim_start_matches('v');
        let semver_like = !version.is_empty()
            && version
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+'));
        if !semver_like {
            return Err(format!("{version:?} is not a release version"));
        }
        Ok(Self::Release {
            version: version.to_owned(),
        })
    }

    /// An archive install, checking that the URL is safe to quote in the boot
    /// script and that the digest is a sha256
    pub fn archive(url: &str, sha256: &str) -> Result<Self, String> {
        // the script single-quotes the URL, so it may hold no quote, and no
        // whitespace or control character that could end the line
        let quotable = !url
            .chars()
            .any(|ch| ch == '\'' || ch.is_whitespace() || ch.is_control());
        if !url.starts_with("https://") || !quotable {
            return Err(format!(
                "the archive URL must be https with no quotes or whitespace, got {url:?}"
            ));
        }
        let sha256 = sha256.to_ascii_lowercase();
        if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!("{sha256:?} is not a sha256 of 64 hex digits"));
        }
        Ok(Self::Archive {
            url: url.to_owned(),
            sha256,
        })
    }

    /// The boot script that installs and starts Homebased
    fn script(&self) -> String {
        let fetch = match self {
            Self::Release { version } => RELEASE_FETCH.replace("@VERSION@", version),
            Self::Archive { url, sha256 } => ARCHIVE_FETCH
                .replace("@URL@", url)
                .replace("@SHA256@", sha256),
        };
        format!("{fetch}{INSTALL}")
    }
}

/// One file cloudlease writes before running the boot commands
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootFile {
    /// Absolute path
    pub path: String,
    /// Octal mode, such as `0600`
    pub mode: String,
    /// UTF-8 content
    pub content: String,
}

/// The `boot` object of a machine create
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootPayload {
    /// Files to write, in order
    pub files: Vec<BootFile>,
    /// Commands to run in order, as root, each an argv
    pub run: Vec<Vec<String>>,
}

/// What one machine's boot data names
pub struct BootPlan<'a> {
    /// Its pool
    pub pool: &'a PoolName,
    /// Its slot
    pub slot: SlotId,
    /// Its Fleet name and hostname
    pub name: &'a MachineName,
    /// Its single-use join token
    pub join_token: &'a JoinToken,
    /// The coordinator's iroh endpoint
    pub hub: &'a LinkAddr,
    /// Relays the machine's endpoint uses, the coordinator's own
    pub relays: &'a Relays,
    /// Where it gets the Homebased it installs
    pub install: &'a MachineInstall,
    /// Free disk, in GiB, below which it admits no CI run
    pub min_free_disk_gib: u64,
}

impl BootPlan<'_> {
    /// The boot data for cloudlease
    #[must_use]
    pub fn render(&self) -> BootPayload {
        BootPayload {
            files: vec![
                BootFile {
                    path: CONFIG_FILE.into(),
                    mode: "0600".into(),
                    content: self.config(),
                },
                BootFile {
                    path: UNIT_FILE.into(),
                    mode: "0644".into(),
                    content: UNIT.replace("@LISTEN@", MACHINE_WEB_LISTEN),
                },
                BootFile {
                    path: SCRIPT_FILE.into(),
                    mode: "0755".into(),
                    content: self.install.script(),
                },
            ],
            run: vec![vec!["/bin/sh".into(), SCRIPT_FILE.into()]],
        }
    }

    /// The machine's `config.toml`
    #[must_use]
    pub fn config(&self) -> String {
        let relay = self
            .hub
            .relay
            .as_ref()
            .map(|url| format!("hub_relay = \"{url}\"\n"))
            .unwrap_or_default();
        let addrs: Vec<String> = self
            .hub
            .addrs
            .iter()
            .map(|addr| format!("\"{addr}\""))
            .collect();
        format!(
            r#"# written at boot by the pool's coordinator through cloudlease
[fleet]
enabled = true
machine_name = "{name}"

[fleet.discovery]
mdns = false

[fleet.iroh]
relays = {relays}
port = 0

[temporary]
pool = "{pool}"
slot = "{slot}"
hub = "{hub}"
{relay}hub_addrs = [{addrs}]
join_token = "{token}"
lease_file = "{lease}"

[ci]
min_free_disk_gib = {min_free_disk_gib}
"#,
            name = self.name,
            pool = self.pool,
            slot = self.slot,
            hub = self.hub.id,
            addrs = addrs.join(", "),
            relays = self.relays.to_toml(),
            token = self.join_token.expose(),
            lease = LEASE_FILE,
            min_free_disk_gib = self.min_free_disk_gib,
        )
    }
}

const UNIT: &str = "[Unit]
Description=Homebased on a temporary machine
After=network-online.target
Wants=network-online.target

[Service]
Environment=HOME=/root
Environment=HOMEBASED_CONFIG=/etc/homebased/config.toml
Environment=HOMEBASED_WEB_LISTEN=@LISTEN@
ExecStart=/usr/local/bin/homebased daemon serve
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
";

// the checksum comes from the same release as the archive, so it catches a
// corrupt download, not a compromised release
const RELEASE_FETCH: &str = r#"#!/bin/sh
set -eu
case "$(uname -m)" in
  x86_64) target=x86_64-unknown-linux-musl ;;
  aarch64 | arm64) target=aarch64-unknown-linux-musl ;;
  *) echo "homebased boot: unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac
tag=v@VERSION@
archive="homebased-$tag-$target.tar.gz"
base="https://github.com/avencera/homebased/releases/download/$tag"
work=$(mktemp -d)
curl -fsSL --retry 5 -o "$work/$archive" "$base/$archive"
# the release names the checksum after the target, without the archive suffix
curl -fsSL --retry 5 -o "$work/$archive.sha256" "$base/homebased-$tag-$target.sha256"
expected=$(cut -d ' ' -f 1 "$work/$archive.sha256")
"#;

// the coordinator pins the checksum, so the archive's host is not trusted
const ARCHIVE_FETCH: &str = r#"#!/bin/sh
set -eu
archive=homebased.tar.gz
work=$(mktemp -d)
curl -fsSL --retry 5 -o "$work/$archive" '@URL@'
expected=@SHA256@
"#;

const INSTALL: &str = r#"actual=$(sha256sum "$work/$archive" | cut -d ' ' -f 1)
if [ "$expected" != "$actual" ]; then
  echo "homebased boot: checksum mismatch for $archive" >&2
  exit 1
fi
tar -xzf "$work/$archive" -C "$work"
install -m 0755 "$(find "$work" -type f -name homebased | head -n 1)" /usr/local/bin/homebased
systemctl daemon-reload
systemctl enable --now homebased.service
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boot_data_carries_config_unit_and_script_and_parses_as_config() {
        let pool: PoolName = "afv".parse().unwrap();
        let slot = SlotId::new();
        let name = pool.machine_name(slot);
        let token = JoinToken::new("t0ken".into());
        let hub = LinkAddr {
            id: iroh::SecretKey::generate().public(),
            relay: Some("https://relay.example.com".parse().unwrap()),
            addrs: vec!["192.0.2.1:4433".parse().unwrap()],
        };
        let plan = BootPlan {
            pool: &pool,
            slot,
            name: &name,
            join_token: &token,
            hub: &hub,
            relays: &Relays::N0,
            install: &MachineInstall::release("v0.37.0").unwrap(),
            min_free_disk_gib: 3,
        };
        let payload = plan.render();
        let paths: Vec<&str> = payload
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect();
        assert_eq!(paths, [CONFIG_FILE, UNIT_FILE, SCRIPT_FILE]);
        assert!(payload.files[2].content.contains("tag=v0.37.0"));
        for file in &payload.files {
            assert!(
                !file.content.contains('@'),
                "unfilled template in {}",
                file.path
            );
        }
        assert!(
            payload.files[1]
                .content
                .contains("HOMEBASED_WEB_LISTEN=127.0.0.1:7677")
        );

        let config = crate::config::Config::parse(&payload.files[0].content).unwrap();
        let temporary = config.temporary.clone().unwrap();
        assert_eq!(temporary.pool, pool);
        assert_eq!(temporary.slot, slot);
        assert_eq!(temporary.join_token.expose(), "t0ken");
        assert_eq!(temporary.hub, hub);
        assert_eq!(config.ci.min_free_disk, 3 << 30);
        let iroh = config.fleet_settings().unwrap().iroh.as_ref().unwrap();
        assert_eq!(iroh.relays, Relays::N0);
        assert_eq!(config.machine_name, Some(name));
        // boot data stays well under cloudlease's 32 KiB limit
        assert!(serde_json::to_vec(&payload).unwrap().len() < 8 * 1024);
    }

    #[test]
    fn archive_installs_are_checked_against_the_pinned_sha256() {
        assert!(MachineInstall::archive("http://example.com/hb.tar.gz", &"0".repeat(64)).is_err());
        assert!(MachineInstall::archive("https://example.com/a'b", &"0".repeat(64)).is_err());
        assert!(MachineInstall::archive("https://example.com/a b", &"0".repeat(64)).is_err());
        assert!(MachineInstall::archive("https://example.com/hb.tar.gz", "abc").is_err());
        assert!(MachineInstall::release("not a version").is_err());

        let url = "https://example.com/hb.tar.gz?X-Amz-Signature=ab&X-Amz-Expires=3600";
        let install = MachineInstall::archive(url, &"AB".repeat(32)).unwrap();
        let script = install.script();
        assert!(script.contains(&format!("'{url}'")), "{script}");
        assert!(
            script.contains(&format!("expected={}", "ab".repeat(32))),
            "{script}"
        );
        assert!(!script.contains("github.com"), "{script}");
    }

    /// Runs a rendered archive script with `curl`, `install`, and `systemctl`
    /// replaced, and returns whether it installed the binary
    #[cfg(target_os = "linux")]
    fn run_archive_script(archive: &std::path::Path, sha256: &str) -> bool {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let installed = dir.path().join("installed");
        let stubs = [
            // curl -fsSL --retry 5 -o <out> <url>
            (
                "curl",
                format!("#!/bin/sh\ncp '{}' \"$5\"\n", archive.display()),
            ),
            (
                "install",
                format!("#!/bin/sh\ncp \"$3\" '{}'\n", installed.display()),
            ),
            ("systemctl", "#!/bin/sh\nexit 0\n".to_owned()),
        ];
        for (name, body) in stubs {
            let path = bin.join(name);
            std::fs::write(&path, body).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let install = MachineInstall::archive("https://example.com/hb.tar.gz", sha256).unwrap();
        let script = dir.path().join("boot.sh");
        std::fs::write(&script, install.script()).unwrap();
        let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
        let status = std::process::Command::new("/bin/sh")
            .arg(&script)
            .env("PATH", path)
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        status.success() && installed.exists()
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn an_archive_script_installs_only_the_archive_it_pinned() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("homebased"), "binary").unwrap();
        let archive = dir.path().join("hb.tar.gz");
        let packed = std::process::Command::new("tar")
            .arg("-czf")
            .arg(&archive)
            .arg("-C")
            .arg(dir.path())
            .arg("homebased")
            .status()
            .unwrap();
        assert!(packed.success());
        let digest = crate::queue::sha256_hex(&std::fs::read(&archive).unwrap());

        assert!(run_archive_script(&archive, &digest));
        assert!(!run_archive_script(&archive, &"0".repeat(64)));
    }
}
