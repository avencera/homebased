//! How a temporary machine and its coordinator reach each other
//!
//! The link is an open decision: Tailscale ephemeral keys, or an iroh
//! endpoint embedded in Homebased. Everything a machine's boot data does to
//! put it on a network its coordinator can reach goes through [`MemberLink`],
//! so a second link is a new variant here plus a connector for Fleet calls,
//! not a change to pools, leases, or renewal. Only the tailnet link exists

use serde::{Deserialize, Serialize};

use crate::fleet::address::MachineAddress;

/// The network a pool's machines use to join and serve their coordinator
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemberLink {
    /// cloudlease joins the machine to the tailnet before boot with an
    /// ephemeral, single-use key it minted; the machine's daemon listens on
    /// its tailnet address and joins the coordinator at `join_address`
    Tailnet {
        /// Where machines join this daemon, on the tailnet
        join_address: MachineAddress,
    },
}

impl MemberLink {
    /// Address the machine's daemon joins and reports events to
    #[must_use]
    pub fn coordinator(&self) -> &MachineAddress {
        match self {
            Self::Tailnet { join_address } => join_address,
        }
    }

    /// systemd units the machine's daemon starts after
    #[must_use]
    pub fn unit_after(&self) -> &'static str {
        match self {
            Self::Tailnet { .. } => "network-online.target tailscaled.service",
        }
    }

    /// Boot script lines that write `HOMEBASED_WEB_LISTEN` to `listen.env`
    #[must_use]
    pub fn listen_script(&self, port: u16) -> String {
        match self {
            Self::Tailnet { .. } => format!(
                "ip=$(tailscale ip -4 | head -n 1)\n\
                 printf 'HOMEBASED_WEB_LISTEN=%s:{port}\\n' \"$ip\" > /etc/homebased/listen.env\n"
            ),
        }
    }
}
