//! Machine addresses and the sources that report them.

use std::cmp::Ordering;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use http::Uri;
use serde::{Deserialize, Serialize};

use crate::daemon::web::DEFAULT_PORT;

/// Base URL of one Homebased TCP listener, such as `http://code:7677`.
///
/// Only `http` is accepted: the cluster protocol has no TLS, and LAN or tailnet
/// reachability is the trust boundary. The value has no path, query, or user
/// information, and host names are lowercase, so two spellings of one listener
/// compare equal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MachineAddress {
    host: AddressHost,
    port: u16,
}

/// Host part of a [`MachineAddress`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AddressHost {
    /// Literal IPv4 or IPv6 address.
    Ip(IpAddr),
    /// DNS, MagicDNS, mDNS, or single-label name. Lowercase, no trailing dot.
    Name(String),
}

impl MachineAddress {
    /// Address of a literal socket address.
    #[must_use]
    pub fn from_socket(addr: SocketAddr) -> Self {
        Self {
            host: AddressHost::Ip(addr.ip()),
            port: addr.port(),
        }
    }

    /// Host part.
    #[must_use]
    pub fn host(&self) -> &AddressHost {
        &self.host
    }

    /// TCP port.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// `host:port` for the HTTP `Host` header and for connecting.
    #[must_use]
    pub fn authority(&self) -> String {
        match &self.host {
            AddressHost::Ip(IpAddr::V6(v6)) => format!("[{v6}]:{}", self.port),
            AddressHost::Ip(IpAddr::V4(v4)) => format!("{v4}:{}", self.port),
            AddressHost::Name(name) => format!("{name}:{}", self.port),
        }
    }

    /// Full URL for a request path that starts with `/`.
    #[must_use]
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.authority())
    }

    /// Whether the host is a Tailscale CGNAT address or a MagicDNS name.
    #[must_use]
    pub fn is_tailscale(&self) -> bool {
        match &self.host {
            AddressHost::Ip(IpAddr::V4(v4)) => {
                let octets = v4.octets();
                octets[0] == 100 && (octets[1] & 0xc0) == 64
            }
            AddressHost::Ip(IpAddr::V6(v6)) => v6.segments()[..3] == [0xfd7a, 0x115c, 0xa1e0],
            AddressHost::Name(name) => name.ends_with(".ts.net"),
        }
    }
}

impl fmt::Display for MachineAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "http://{}", self.authority())
    }
}

impl Ord for MachineAddress {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.host, self.port).cmp(&(&other.host, other.port))
    }
}

impl PartialOrd for MachineAddress {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Why an address string was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MachineAddressError {
    /// Not a URL.
    #[error("invalid address {raw:?}: {reason}")]
    Malformed {
        /// Input text.
        raw: String,
        /// Parser message.
        reason: String,
    },
    /// Scheme other than `http`.
    #[error("address {raw:?} must use http://")]
    Scheme {
        /// Input text.
        raw: String,
    },
    /// Has a path, query, or user information.
    #[error("address {raw:?} must be a base URL such as http://code:7677")]
    NotBase {
        /// Input text.
        raw: String,
    },
}

impl FromStr for MachineAddress {
    type Err = MachineAddressError;

    /// Parse `http://host[:port][/]`. The port defaults to the usual
    /// Homebased listener port.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let malformed = |reason: String| MachineAddressError::Malformed {
            raw: raw.to_string(),
            reason,
        };
        let uri: Uri = raw
            .trim()
            .parse()
            .map_err(|err| malformed(format!("{err}")))?;
        if uri.scheme_str() != Some("http") {
            return Err(MachineAddressError::Scheme {
                raw: raw.to_string(),
            });
        }
        let authority = uri
            .authority()
            .ok_or_else(|| malformed("missing host".into()))?;
        let has_path = !matches!(uri.path(), "" | "/");
        if has_path || uri.query().is_some() || authority.as_str().contains('@') {
            return Err(MachineAddressError::NotBase {
                raw: raw.to_string(),
            });
        }
        let host = parse_host(authority.host()).ok_or_else(|| malformed("invalid host".into()))?;
        Ok(Self {
            host,
            port: authority.port_u16().unwrap_or(DEFAULT_PORT),
        })
    }
}

fn parse_host(raw: &str) -> Option<AddressHost> {
    let unbracketed = raw
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(raw);
    if let Ok(ip) = unbracketed.parse::<IpAddr>() {
        return Some(AddressHost::Ip(ip));
    }
    let name = raw.trim_end_matches('.').to_ascii_lowercase();
    let valid = !name.is_empty()
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
        });
    valid.then_some(AddressHost::Name(name))
}

impl Serialize for MachineAddress {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MachineAddress {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// Where the peer directory learned an address, in preference order.
///
/// Lower values are tried first. The order follows the fleet plan: an operator
/// choice beats automatic discovery, LAN beats Tailscale, and a cached address
/// is the last resort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressSource {
    /// Declared in `config.toml`.
    Configured,
    /// Added with an explicit CLI command.
    Explicit,
    /// Reported by mDNS on the local network and not yet expired.
    Lan,
    /// Reported by Tailscale peer discovery and not yet expired.
    Tailscale,
    /// Verified by an earlier probe; no current provider reports it.
    Cached,
}

/// Rank used to order addresses for one machine. `Configured` and `Explicit`
/// share the first rank: both are operator choices.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AddressPreference {
    /// Explicit or configured address.
    Operator,
    /// Reachable LAN name or address.
    Lan,
    /// Reachable Tailscale name or address.
    Tailscale,
    /// Last-known cached address.
    Cached,
}

impl From<AddressSource> for AddressPreference {
    fn from(source: AddressSource) -> Self {
        match source {
            AddressSource::Configured | AddressSource::Explicit => Self::Operator,
            AddressSource::Lan => Self::Lan,
            AddressSource::Tailscale => Self::Tailscale,
            AddressSource::Cached => Self::Cached,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(raw: &str) -> MachineAddress {
        raw.parse().unwrap()
    }

    #[test]
    fn parses_and_normalizes() {
        assert_eq!(addr("http://Main:7677/").to_string(), "http://main:7677");
        assert_eq!(addr("http://main").to_string(), "http://main:7677");
        assert_eq!(addr("http://main.").to_string(), "http://main:7677");
        assert_eq!(
            addr("http://192.168.1.5:8000").to_string(),
            "http://192.168.1.5:8000"
        );
        assert_eq!(addr("http://[::1]:7677").to_string(), "http://[::1]:7677");
        assert_eq!(addr("http://Main:7677"), addr("http://main:7677/"));
    }

    #[test]
    fn rejects_non_base_urls() {
        assert!(matches!(
            "https://main:7677".parse::<MachineAddress>(),
            Err(MachineAddressError::Scheme { .. })
        ));
        assert!(matches!(
            "main:7677".parse::<MachineAddress>(),
            Err(MachineAddressError::Scheme { .. } | MachineAddressError::Malformed { .. })
        ));
        assert!(matches!(
            "http://main:7677/v1".parse::<MachineAddress>(),
            Err(MachineAddressError::NotBase { .. })
        ));
        assert!(matches!(
            "http://main:7677/?x=1".parse::<MachineAddress>(),
            Err(MachineAddressError::NotBase { .. })
        ));
        assert!(matches!(
            "http://bad_host:7677".parse::<MachineAddress>(),
            Err(MachineAddressError::Malformed { .. })
        ));
    }

    #[test]
    fn tailscale_classification() {
        assert!(addr("http://100.101.102.103:7677").is_tailscale());
        assert!(addr("http://code.tail1234.ts.net:7677").is_tailscale());
        assert!(!addr("http://192.168.1.2:7677").is_tailscale());
        assert!(!addr("http://code.local:7677").is_tailscale());
    }

    #[test]
    fn preference_order() {
        assert!(
            AddressPreference::from(AddressSource::Configured)
                == AddressPreference::from(AddressSource::Explicit)
        );
        assert!(AddressPreference::Operator < AddressPreference::Lan);
        assert!(AddressPreference::Lan < AddressPreference::Tailscale);
        assert!(AddressPreference::Tailscale < AddressPreference::Cached);
    }
}
