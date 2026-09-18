//! Host header checks that limit DNS rebinding without adding a user login step.

use std::net::{IpAddr, SocketAddr};

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::domain::API_VERSION;

/// Addresses and names accepted on the dashboard and content listeners.
#[derive(Debug, Clone)]
pub struct HostPolicy {
    /// Literal bind address of this listener.
    pub bind: SocketAddr,
}

impl HostPolicy {
    /// Whether the `Host` header value is accepted for this bind.
    #[must_use]
    pub fn allows(&self, host_header: &str) -> bool {
        let host = strip_port(host_header.trim());
        if host.is_empty() {
            return false;
        }
        if host.eq_ignore_ascii_case("localhost") {
            return true;
        }
        if let Ok(ip) = host.parse::<IpAddr>() {
            return is_local_ip(ip) || is_tailscale_ip(ip) || ip == self.bind.ip();
        }
        // Tailscale MagicDNS names end in .ts.net
        if is_magic_dns(host) {
            return true;
        }
        // configured literal hostname form of the bind address only when IP
        false
    }
}

/// Axum middleware that rejects unexpected `Host` values.
pub async fn host_guard(policy: HostPolicy, request: Request, next: Next) -> Response {
    let allowed = request
        .headers()
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|host| policy.allows(host));
    if allowed {
        return next.run(request).await;
    }
    let body = json!({
        "api_version": API_VERSION,
        "error": {
            "code": "usage",
            "message": "unexpected Host header",
            "retryable": false,
            "input": {},
        }
    });
    (StatusCode::BAD_REQUEST, axum::Json(body)).into_response()
}

fn strip_port(host: &str) -> &str {
    if let Some(rest) = host.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    host.rsplit_once(':')
        .and_then(|(name, port)| port.parse::<u16>().ok().map(|_| name))
        .unwrap_or(host)
}

fn is_local_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unique_local()
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                || v6
                    .to_ipv4_mapped()
                    .is_some_and(|v4| is_local_ip(IpAddr::V4(v4)))
        }
    }
}

/// Tailscale userspace addresses live in 100.64.0.0/10.
fn is_tailscale_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            octets[0] == 100 && (octets[1] & 0xc0) == 64
        }
        IpAddr::V6(_) => false,
    }
}

fn is_magic_dns(host: &str) -> bool {
    let lower = host.to_ascii_lowercase();
    lower.ends_with(".ts.net")
        && lower
            .trim_end_matches(".ts.net")
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '.')
        && !lower.starts_with('.')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn policy(bind: &str) -> HostPolicy {
        HostPolicy {
            bind: bind.parse().unwrap(),
        }
    }

    #[test]
    fn accepts_local_tailscale_and_bind() {
        let p = policy("127.0.0.1:7677");
        assert!(p.allows("localhost"));
        assert!(p.allows("localhost:7677"));
        assert!(p.allows("127.0.0.1"));
        assert!(p.allows("127.0.0.1:7677"));
        assert!(p.allows("[::1]:7677"));
        assert!(p.allows("192.168.1.10"));
        assert!(p.allows("100.64.1.2"));
        assert!(p.allows("100.127.0.1:9"));
        assert!(p.allows("my-box.tail1234.ts.net"));
        assert!(p.allows("127.0.0.1"));
        assert!(!p.allows("evil.example"));
        assert!(!p.allows(""));
        assert!(!p.allows("example.com"));
    }

    #[test]
    fn accepts_configured_bind_ip() {
        let p = policy("10.0.0.5:7677");
        assert!(p.allows("10.0.0.5"));
        assert!(p.allows("10.0.0.5:7677"));
    }

    #[test]
    fn tailscale_cg_nat_bounds() {
        assert!(is_tailscale_ip(IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1))));
        assert!(is_tailscale_ip(IpAddr::V4(Ipv4Addr::new(
            100, 127, 255, 255
        ))));
        assert!(!is_tailscale_ip(IpAddr::V4(Ipv4Addr::new(100, 63, 0, 1))));
        assert!(!is_tailscale_ip(IpAddr::V4(Ipv4Addr::new(100, 128, 0, 1))));
    }
}
