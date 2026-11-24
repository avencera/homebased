//! What a temporary machine runs at boot
//!
//! cloudlease writes `lease.json`, then writes these files and runs these
//! commands. The script installs the static Homebased release that matches
//! the coordinator, starts its daemon where the pool's [`MemberLink`] says,
//! and the daemon joins the coordinator. Nothing here is secret except the
//! single-use join token

use serde::{Deserialize, Serialize};

use super::link::MemberLink;
use super::{JoinToken, PoolName, SlotId};
use crate::machine::MachineName;

/// Where cloudlease writes the machine's lease credential
pub const LEASE_FILE: &str = "/etc/cloudlease/lease.json";

/// Where the boot data puts the machine's Homebased config
pub const CONFIG_FILE: &str = "/etc/homebased/config.toml";

const SCRIPT_FILE: &str = "/usr/local/lib/homebased/boot.sh";
const UNIT_FILE: &str = "/etc/systemd/system/homebased.service";

/// Port the machine's daemon listens on, on its link's address
pub const LISTEN_PORT: u16 = 7677;

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
    /// How it reaches its coordinator
    pub link: &'a MemberLink,
    /// Homebased release it installs, such as `0.37.0`
    pub version: &'a str,
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
                    content: UNIT.replace("@AFTER@", self.link.unit_after()),
                },
                BootFile {
                    path: SCRIPT_FILE.into(),
                    mode: "0755".into(),
                    content: SCRIPT
                        .replace("@VERSION@", self.version)
                        .replace("@LISTEN@\n", &self.link.listen_script(LISTEN_PORT)),
                },
            ],
            run: vec![vec!["/bin/sh".into(), SCRIPT_FILE.into()]],
        }
    }

    /// The machine's `config.toml`
    #[must_use]
    pub fn config(&self) -> String {
        format!(
            r#"# written at boot by the pool's coordinator through cloudlease
[fleet]
enabled = true
machine_name = "{name}"

[fleet.discovery]
mdns = false

[temporary]
pool = "{pool}"
slot = "{slot}"
coordinator = "{coordinator}"
join_token = "{token}"
lease_file = "{lease}"
"#,
            name = self.name,
            pool = self.pool,
            slot = self.slot,
            coordinator = self.link.coordinator(),
            token = self.join_token.expose(),
            lease = LEASE_FILE,
        )
    }
}

const UNIT: &str = "[Unit]
Description=Homebased on a temporary machine
After=@AFTER@
Wants=network-online.target

[Service]
Environment=HOME=/root
Environment=HOMEBASED_CONFIG=/etc/homebased/config.toml
EnvironmentFile=/etc/homebased/listen.env
ExecStart=/usr/local/bin/homebased daemon serve
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
";

// the checksum comes from the same release as the archive, so it catches a
// corrupt download, not a compromised release
const SCRIPT: &str = r#"#!/bin/sh
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
curl -fsSL --retry 5 -o "$work/$archive.sha256" "$base/$archive.sha256"
expected=$(cut -d ' ' -f 1 "$work/$archive.sha256")
actual=$(sha256sum "$work/$archive" | cut -d ' ' -f 1)
if [ "$expected" != "$actual" ]; then
  echo "homebased boot: checksum mismatch for $archive" >&2
  exit 1
fi
tar -xzf "$work/$archive" -C "$work"
install -m 0755 "$(find "$work" -type f -name homebased | head -n 1)" /usr/local/bin/homebased
@LISTEN@
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
        let link = MemberLink::Tailnet {
            join_address: "http://code:7677".parse().unwrap(),
        };
        let plan = BootPlan {
            pool: &pool,
            slot,
            name: &name,
            join_token: &token,
            link: &link,
            version: "0.37.0",
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
            payload.files[2]
                .content
                .contains("HOMEBASED_WEB_LISTEN=%s:7677")
        );

        let config = crate::config::Config::parse(&payload.files[0].content).unwrap();
        let temporary = config.temporary.unwrap();
        assert_eq!(temporary.pool, pool);
        assert_eq!(temporary.slot, slot);
        assert_eq!(temporary.join_token.expose(), "t0ken");
        assert_eq!(&temporary.coordinator, link.coordinator());
        assert_eq!(config.machine_name, Some(name));
        // boot data stays well under cloudlease's 32 KiB limit
        assert!(serde_json::to_vec(&payload).unwrap().len() < 8 * 1024);
    }
}
