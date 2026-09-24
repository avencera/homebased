//! Optional TCP listener: dashboard assets, the read-only API, the three typed
//! resource controls, and, when the fleet is enabled, the `/v1/cluster/*` routes
//!
//! Off unless `--web-listen` / `HOMEBASED_WEB_LISTEN` is a host:port. A TCP port
//! is reachable from any web page the user has open, so this router never
//! exposes task submit or cancel. The only browser write is
//! `POST /v1/resources/{id}/actions`, which requires the exact dashboard
//! `Origin`, a JSON body within a small limit, and no CORS response headers

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use axum::extract::{DefaultBodyLimit, Request};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use rust_embed::RustEmbed;
use tokio::net::TcpListener;
use tracing::warn;

use crate::daemon::AppState;
use crate::daemon::{api, cluster, resource_api};
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::files::{HostPolicy, host_guard};
use serde_json::json;

/// Largest JSON body accepted by the dashboard resource-action route
const BROWSER_ACTION_MAX_BYTES: usize = 16 * 1024;

/// Usual dashboard port when an operator opts in
pub const DEFAULT_PORT: u16 = 7677;

/// Where the dashboard listens
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WebListen {
    /// No TCP listener
    #[default]
    Off,
    /// Bind this address. Port `0` picks a free port
    Addr(SocketAddr),
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
/// supervisor must keep running when the port is busy
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

/// Base URL for a bound address
#[must_use]
pub fn url_for(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

/// Read-only API, the typed resource controls, and the embedded single-page app
pub fn router(state: AppState, bind: SocketAddr) -> Router {
    let policy = HostPolicy { bind };
    let routes = match state.fleet.handle() {
        Some(fleet) => api::read_routes().merge(cluster::routes(fleet.clone())),
        None => api::read_routes(),
    };
    routes
        .merge(browser_action_routes())
        .fallback(asset)
        .layer(middleware::from_fn(move |request: Request, next: Next| {
            let policy = policy.clone();
            async move { host_guard(policy, request, next).await }
        }))
        .with_state(state)
}

/// The resource-action route with its browser-only request checks
fn browser_action_routes() -> Router<AppState> {
    resource_api::action_routes()
        .layer(DefaultBodyLimit::max(BROWSER_ACTION_MAX_BYTES))
        .layer(middleware::from_fn(same_origin_guard))
}

/// Refuse a browser write unless it comes from this dashboard's own origin
///
/// The Host guard runs first, so the Host value is one this listener serves
/// Browsers always send `Origin` on a cross-origin POST, and a missing value is
/// refused too, so a page on another origin cannot use this route
async fn same_origin_guard(request: Request, next: Next) -> Response {
    match check_same_origin(request.headers()) {
        Ok(()) => next.run(request).await,
        Err(message) => AppError::Permission {
            message: message.into(),
        }
        .into_response(),
    }
}

fn check_same_origin(headers: &HeaderMap) -> Result<(), &'static str> {
    let header_text = |name| headers.get(name).and_then(|value| value.to_str().ok());
    let host = header_text(header::HOST).ok_or("resource actions require a Host header")?;
    let origin = header_text(header::ORIGIN).ok_or("resource actions require an Origin header")?;
    if origin != format!("http://{host}") {
        return Err("resource actions require the exact dashboard Origin");
    }
    let fetch_site = headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok());
    if fetch_site.is_some_and(|site| site != "same-origin") {
        return Err("resource actions require a same-origin browser request");
    }
    let json = header_text(header::CONTENT_TYPE).is_some_and(|value| {
        value
            .split(';')
            .next()
            .is_some_and(|media| media.trim().eq_ignore_ascii_case("application/json"))
    });
    if !json {
        return Err("resource actions require an application/json body");
    }
    Ok(())
}

/// Output of `npm run build` in `web/`. Empty until the dashboard is built;
/// `build.rs` creates the directory so the crate always compiles
#[derive(RustEmbed)]
#[folder = "web/build/"]
struct Assets;

const INDEX: &str = "index.html";

/// SvelteKit writes fingerprinted files here, so they can be cached forever
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
    use super::{DEFAULT_PORT, WebListen, bind};
    use crate::error::AppError;
    use std::net::{Ipv4Addr, SocketAddr};

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
    fn default_is_off_and_round_trips() {
        let listen = WebListen::default();
        assert_eq!(listen, WebListen::Off);
        assert_eq!(listen.to_string(), "off");
        assert_eq!(listen.to_string().parse::<WebListen>().unwrap(), listen);
        let loopback = WebListen::Addr(SocketAddr::from((Ipv4Addr::LOCALHOST, DEFAULT_PORT)));
        assert_eq!(loopback.to_string(), "127.0.0.1:7677");
        assert_eq!(loopback.to_string().parse::<WebListen>().unwrap(), loopback);
    }

    #[tokio::test]
    async fn off_does_not_bind() {
        assert!(bind(WebListen::Off).await.is_none());
    }
}
