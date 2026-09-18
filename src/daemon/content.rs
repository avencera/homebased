//! Separate content-origin HTTP listener for raw file bytes.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use axum::extract::{ConnectInfo, Path as AxumPath, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;
use tokio::net::TcpListener;
use tracing::warn;

use crate::daemon::AppState;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::files::{
    HostPolicy, PathToken, StreamSlots, decode_path, host_guard, open_file_stream, path_display,
};

/// Bind the content listener on the same IP as the dashboard, port chosen by the OS.
pub async fn bind(dashboard: SocketAddr) -> Option<(TcpListener, SocketAddr)> {
    let bind = SocketAddr::new(dashboard.ip(), 0);
    match TcpListener::bind(bind).await {
        Ok(listener) => match listener.local_addr() {
            Ok(addr) => Some((listener, addr)),
            Err(err) => {
                warn!("content listener local address unavailable: {err}");
                None
            }
        },
        Err(err) => {
            warn!(%bind, "content origin bind failed: {err}");
            None
        }
    }
}

/// Content-only router. No dashboard API or directory listing routes.
pub fn router(state: AppState, bind: SocketAddr) -> Router {
    let policy = HostPolicy { bind };
    Router::new()
        .route("/raw/{*path}", get(raw_by_path))
        .route("/by-token/{token}", get(raw_by_token))
        .fallback(not_found)
        .layer(middleware::from_fn(move |request: Request, next: Next| {
            let policy = policy.clone();
            async move { host_guard(policy, request, next).await }
        }))
        .with_state(state)
}

async fn raw_by_path(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    AxumPath(path): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let absolute = mirror_path(&path)?;
    serve_regular_file(absolute, &headers, &state.stream_slots, peer).await
}

async fn raw_by_token(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    AxumPath(token): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let token = PathToken::from_raw(token)?;
    let path = decode_path(&token)?;
    serve_regular_file(path, &headers, &state.stream_slots, peer).await
}

async fn serve_regular_file(
    path: PathBuf,
    headers: &HeaderMap,
    slots: &StreamSlots,
    peer: SocketAddr,
) -> Result<Response, AppError> {
    let meta = tokio::fs::symlink_metadata(&path)
        .await
        .map_err(|err| map_io(&path, err))?;
    if meta.file_type().is_symlink() {
        // follow for content, but only serve regular files
        let resolved = tokio::fs::canonicalize(&path)
            .await
            .map_err(|err| map_io(&path, err))?;
        return open_file_stream(resolved, headers, slots, peer.ip()).await;
    }
    if !meta.is_file() {
        if meta.is_dir() {
            return Err(AppError::UnsupportedFile {
                message: format!("{} is a directory", path_display(&path)),
            });
        }
        return Err(AppError::UnsupportedFile {
            message: "special files are not browsable".into(),
        });
    }
    open_file_stream(path, headers, slots, peer.ip()).await
}

/// Turn `/raw/...` into an absolute filesystem path. The catch-all omits the
/// leading slash, so restore it. Reject `..` segments.
fn mirror_path(captured: &str) -> Result<PathBuf, AppError> {
    let trimmed = captured.trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(AppError::Usage {
            message: "path must be absolute".into(),
        });
    }
    for segment in trimmed.split('/') {
        if segment == ".." || segment == "." {
            return Err(AppError::Usage {
                message: "path must not contain '.' or '..' segments".into(),
            });
        }
    }
    Ok(PathBuf::from(format!("/{trimmed}")))
}

fn map_io(path: &Path, err: std::io::Error) -> AppError {
    match err.kind() {
        std::io::ErrorKind::NotFound => AppError::FileNotFound {
            message: format!("{} not found", path_display(path)),
        },
        std::io::ErrorKind::PermissionDenied => AppError::Permission {
            message: format!("permission denied: {}", path_display(path)),
        },
        _ => AppError::Internal {
            message: format!("{}: {err}", path_display(path)),
        },
    }
}

async fn not_found(_request: Request) -> Response {
    let body = json!({
        "api_version": API_VERSION,
        "error": {
            "code": "not_found",
            "message": "no such content route",
            "retryable": false,
            "input": {},
        }
    });
    (StatusCode::NOT_FOUND, Json(body)).into_response()
}

impl PathToken {
    fn from_raw(raw: String) -> Result<Self, AppError> {
        if raw.is_empty() {
            return Err(AppError::Usage {
                message: "missing path token".into(),
            });
        }
        Ok(Self::from_encoded(raw))
    }
}
