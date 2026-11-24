//! Fleet over iroh: the transport's addresses, relays, and protocol name
//!
//! A daemon with `[fleet.iroh]` runs one iroh endpoint whose key, kept in its
//! state directory, is its identity on the link. Peers are authorized by
//! endpoint id, and each Fleet call is one HTTP/1 exchange on its own QUIC
//! stream, so the Fleet API is the same as over plain HTTP; see
//! `daemon::fleet_link`. Fleet machines opt in per peer with an `iroh`
//! transport in `[[fleet.machines]]`; pools' temporary machines always use it

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use iroh::{EndpointAddr, EndpointId, RelayUrl, TransportAddr};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// ALPN of the Fleet link, bumped when the protocol changes incompatibly
pub const FLEET_ALPN: &[u8] = b"homebased/fleet/1";

/// Default UDP port of an endpoint on IPv4, fixed so the direct addresses a
/// peer holds still hold after a restart
pub const DEFAULT_PORT: u16 = 7679;

/// Relay servers both ends of the link use to find a path to each other
///
/// A relay carries traffic only until a direct path works, and whenever none
/// does. n0's public relays are rate-limited and meant for light use;
/// `Custom` names self-hosted `iroh-relay` servers
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Relays {
    /// n0's public relays, with n0's DNS so an endpoint is found by id alone
    #[default]
    N0,
    /// These relays only, and no lookup service
    Custom(Vec<RelayUrl>),
    /// No relay: direct addresses only, for one network or tests
    Disabled,
}

impl Relays {
    /// TOML value for a config file, the same form [`Relays`] parses
    #[must_use]
    pub fn to_toml(&self) -> String {
        match self {
            Self::N0 => "\"n0\"".into(),
            Self::Disabled => "\"disabled\"".into(),
            Self::Custom(urls) => {
                let urls: Vec<String> = urls.iter().map(|url| format!("\"{url}\"")).collect();
                format!("[{}]", urls.join(", "))
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum RawRelays {
    Named(String),
    Urls(Vec<String>),
}

impl<'de> Deserialize<'de> for Relays {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let parsed = match RawRelays::deserialize(deserializer)? {
            RawRelays::Named(name) if name == "n0" => Ok(Self::N0),
            RawRelays::Named(name) if name == "disabled" => Ok(Self::Disabled),
            RawRelays::Named(name) => Err(format!(
                "relays must be \"n0\", \"disabled\", or a list of relay URLs, got {name:?}"
            )),
            RawRelays::Urls(urls) if urls.is_empty() => {
                Err("relays must name at least one relay URL".into())
            }
            RawRelays::Urls(urls) => urls
                .iter()
                .map(|url| {
                    RelayUrl::from_str(url).map_err(|error| format!("relay URL {url:?}: {error}"))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Custom),
        };
        parsed.map_err(serde::de::Error::custom)
    }
}

impl Serialize for Relays {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::N0 => RawRelays::Named("n0".into()),
            Self::Disabled => RawRelays::Named("disabled".into()),
            Self::Custom(urls) => RawRelays::Urls(urls.iter().map(ToString::to_string).collect()),
        }
        .serialize(serializer)
    }
}

impl fmt::Display for Relays {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::N0 => f.write_str("n0"),
            Self::Disabled => f.write_str("disabled"),
            Self::Custom(urls) => {
                let urls: Vec<String> = urls.iter().map(ToString::to_string).collect();
                f.write_str(&urls.join(", "))
            }
        }
    }
}

/// Where to dial a daemon on the link: its endpoint id, and the relay and
/// direct addresses it had when the address was written down
///
/// None of it is secret. With n0's relays, the id alone is enough once the
/// addresses are stale
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LinkAddr {
    /// The daemon's endpoint id, its public key
    pub id: EndpointId,
    /// Its home relay
    pub relay: Option<RelayUrl>,
    /// Its direct UDP addresses
    pub addrs: Vec<SocketAddr>,
}

impl LinkAddr {
    /// The parts of an endpoint's current address that a dialer can use
    #[must_use]
    pub fn of(addr: &EndpointAddr) -> Self {
        Self {
            id: addr.id,
            relay: addr.relay_urls().next().cloned(),
            addrs: addr.ip_addrs().copied().collect(),
        }
    }

    /// The address to dial
    #[must_use]
    pub fn endpoint_addr(&self) -> EndpointAddr {
        let relay = self.relay.iter().cloned().map(TransportAddr::Relay);
        let direct = self.addrs.iter().copied().map(TransportAddr::Ip);
        EndpointAddr::from_parts(self.id, relay.chain(direct))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize, Serialize)]
    struct Holder {
        relays: Relays,
    }

    fn parse(text: &str) -> Result<Relays, toml::de::Error> {
        toml::from_str::<Holder>(&format!("relays = {text}")).map(|holder| holder.relays)
    }

    #[test]
    fn relays_parse_the_form_boot_data_writes() {
        let custom = Relays::Custom(vec!["https://relay.example.com".parse().unwrap()]);
        for relays in [Relays::N0, Relays::Disabled, custom] {
            assert_eq!(parse(&relays.to_toml()).unwrap(), relays);
        }
        assert!(parse("\"tailnet\"").is_err());
        assert!(parse("[]").is_err());
        assert!(parse("[\"not a url\"]").is_err());
    }
}
