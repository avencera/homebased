//! Loopback TCP listener: dashboard assets plus the read-only API.
//!
//! The Unix socket stays the only place that accepts mutations. A TCP port on
//! loopback is reachable from any web page the user has open, so this router
//! never exposes submit or cancel.

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;

use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use rust_embed::RustEmbed;
use tokio::net::TcpListener;
use tracing::warn;

use crate::daemon::AppState;
use crate::daemon::api;
use crate::domain::API_VERSION;
use crate::error::AppError;
use serde_json::json;

/// Default dashboard bind.
pub const DEFAULT_PORT: u16 = 7677;

/// Where the dashboard listens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebListen {
    /// No TCP listener.
    Off,
    /// Bind this address. Port `0` picks a free port.
    Addr(SocketAddr),
}

impl Default for WebListen {
    fn default() -> Self {
        Self::Addr(SocketAddr::from((Ipv4Addr::LOCALHOST, DEFAULT_PORT)))
    }
}

impl FromStr for WebListen {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed.eq_ignore_ascii_case("off") {
            return Ok(Self::Off);
        }
        trimmed
            .parse::<SocketAddr>()
            .map(Self::Addr)
            .map_err(|_| AppError::Usage {
                message: format!("invalid --web-listen {s:?}: expected `off` or host:port"),
            })
    }
}

impl fmt::Display for WebListen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Off => f.write_str("off"),
            Self::Addr(addr) => addr.fmt(f),
        }
    }
}

/// Bind the dashboard listener. A failure is logged and swallowed: the
/// supervisor must keep running when the port is busy.
pub async fn bind(listen: WebListen) -> Option<TcpListener> {
    let WebListen::Addr(addr) = listen else {
        return None;
    };
    match TcpListener::bind(addr).await {
        Ok(listener) => Some(listener),
        Err(err) => {
            warn!(%addr, "dashboard bind failed, continuing without web UI: {err}");
            None
        }
    }
}

/// Base URL for a bound address.
#[must_use]
pub fn url_for(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

/// Read-only API plus the embedded single-page app.
pub fn router(state: AppState) -> Router {
    api::read_routes().fallback(asset).with_state(state)
}

/// Output of `npm run build` in `web/`. Empty until the dashboard is built;
/// `build.rs` creates the directory so the crate always compiles.
#[derive(RustEmbed)]
#[folder = "web/build/"]
struct Assets;

const INDEX: &str = "index.html";

/// SvelteKit writes fingerprinted files here, so they can be cached forever.
const IMMUTABLE_PREFIX: &str = "_app/immutable/";

async fn asset(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if path.starts_with("v1/") {
        return api_not_found();
    }
    let path = if path.is_empty() { INDEX } else { path };
    if let Some(file) = Assets::get(path) {
        return file_response(path, &file);
    }
    // client-side routes such as /tasks/<id> resolve to the app shell
    match Assets::get(INDEX) {
        Some(index) => file_response(INDEX, &index),
        None => not_built(),
    }
}

fn file_response(path: &str, file: &rust_embed::EmbeddedFile) -> Response {
    let cache = if path.starts_with(IMMUTABLE_PREFIX) {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    (
        [
            (header::CONTENT_TYPE, file.metadata.mimetype().to_string()),
            (header::CACHE_CONTROL, cache.to_string()),
        ],
        file.data.to_vec(),
    )
        .into_response()
}

fn api_not_found() -> Response {
    // same envelope shape as `AppError::to_json`, for a route that has no error variant
    let body = json!({
        "api_version": API_VERSION,
        "error": {
            "code": "not_found",
            "message": "no such route",
            "retryable": false,
            "input": {},
        }
    });
    (StatusCode::NOT_FOUND, Json(body)).into_response()
}

fn not_built() -> Response {
    const BODY: &str = "<!doctype html><meta charset=utf-8><title>homebased</title>\
        <body style=\"font-family:system-ui;margin:3rem\">\
        <h1>homebased dashboard is not built</h1>\
        <p>Run <code>just web-build</code>, rebuild the binary, then \
        <code>homebased daemon restart</code>.</p>\
        <p>The read-only API is still available under <code>/v1/</code>.</p>";
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        BODY,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_off_and_addresses() {
        assert_eq!("off".parse::<WebListen>().unwrap(), WebListen::Off);
        assert_eq!(" OFF ".parse::<WebListen>().unwrap(), WebListen::Off);
        assert_eq!(
            "127.0.0.1:0".parse::<WebListen>().unwrap(),
            WebListen::Addr("127.0.0.1:0".parse().unwrap())
        );
        assert_eq!(
            "[::1]:7677".parse::<WebListen>().unwrap(),
            WebListen::Addr("[::1]:7677".parse().unwrap())
        );
    }

    #[test]
    fn rejects_garbage_as_usage() {
        let err = "localhost".parse::<WebListen>().unwrap_err();
        assert!(matches!(err, AppError::Usage { .. }), "{err:?}");
        let err = "7677".parse::<WebListen>().unwrap_err();
        assert!(matches!(err, AppError::Usage { .. }), "{err:?}");
    }

    #[test]
    fn default_is_loopback_and_round_trips() {
        let listen = WebListen::default();
        assert_eq!(listen.to_string(), "127.0.0.1:7677");
        assert_eq!(listen.to_string().parse::<WebListen>().unwrap(), listen);
        assert_eq!(WebListen::Off.to_string(), "off");
    }

    #[tokio::test]
    async fn off_does_not_bind() {
        assert!(bind(WebListen::Off).await.is_none());
    }
}
