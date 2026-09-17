//! Hyper HTTP/1 client over a Unix socket.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::net::UnixStream;

use crate::error::AppError;

/// Client for the daemon Unix socket.
#[derive(Debug, Clone)]
pub struct Client {
    sock: PathBuf,
}

impl Client {
    /// Connect to `home.sock_path()`.
    #[must_use]
    pub fn new(sock: PathBuf) -> Self {
        Self { sock }
    }

    /// Socket path.
    #[must_use]
    pub fn sock(&self) -> &Path {
        &self.sock
    }

    /// GET.
    pub async fn get(&self, path: &str) -> Result<Value, AppError> {
        self.request("GET", path, None).await
    }

    /// POST JSON.
    pub async fn post(&self, path: &str, body: &Value) -> Result<Value, AppError> {
        self.request("POST", path, Some(body)).await
    }

    /// Typed GET.
    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, AppError> {
        let value = self.get(path).await?;
        serde_json::from_value(value).map_err(|err| AppError::Internal {
            message: err.to_string(),
        })
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, AppError> {
        let stream =
            UnixStream::connect(&self.sock)
                .await
                .map_err(|err| AppError::DaemonUnavailable {
                    message: format!("connect {}: {err}", self.sock.display()),
                })?;
        let io = TokioIo::new(stream);
        let (mut sender, conn) =
            http1::handshake(io)
                .await
                .map_err(|err| AppError::DaemonUnavailable {
                    message: format!("handshake: {err}"),
                })?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let payload = match body {
            Some(value) => serde_json::to_vec(value)?,
            None => Vec::new(),
        };
        let req = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "localhost")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(payload)))
            .map_err(|err| AppError::Internal {
                message: format!("build request: {err}"),
            })?;
        let res: Response<Incoming> =
            sender
                .send_request(req)
                .await
                .map_err(|err| AppError::DaemonUnavailable {
                    message: format!("send: {err}"),
                })?;
        let status = res.status();
        let bytes = res
            .into_body()
            .collect()
            .await
            .map_err(|err| AppError::Internal {
                message: format!("read body: {err}"),
            })?
            .to_bytes();
        if status.is_success() {
            if bytes.is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_slice(&bytes).map_err(|err| AppError::Internal {
                message: format!("decode: {err}"),
            });
        }
        Err(map_error(status, &bytes))
    }
}

fn map_error(status: StatusCode, bytes: &[u8]) -> AppError {
    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        if let Some(error) = value.get("error") {
            let code = error
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("internal");
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("daemon error")
                .to_string();
            let input = error.get("input").cloned().unwrap_or(Value::Null);
            return from_code(code, message, input, status);
        }
    }
    AppError::Internal {
        message: format!(
            "http {} {}",
            status.as_u16(),
            String::from_utf8_lossy(bytes)
        ),
    }
}

fn from_code(code: &str, message: String, input: Value, status: StatusCode) -> AppError {
    match code {
        "daemon_unavailable" => AppError::DaemonUnavailable { message },
        "task_not_found" => {
            let id = input
                .get("id")
                .and_then(Value::as_str)
                .and_then(|s| s.parse().ok());
            match id {
                Some(id) => AppError::TaskNotFound { id },
                None => AppError::Internal { message },
            }
        }
        "cwd_not_found" => AppError::CwdNotFound {
            path: input
                .get("cwd")
                .and_then(Value::as_str)
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
        },
        "agent_binary_missing" => AppError::AgentBinaryMissing {
            agent: input
                .get("agent")
                .and_then(Value::as_str)
                .and_then(|s| match s {
                    "codex" => Some(crate::domain::AgentKind::Codex),
                    "claude" => Some(crate::domain::AgentKind::Claude),
                    "grok" => Some(crate::domain::AgentKind::Grok),
                    _ => None,
                })
                .unwrap_or(crate::domain::AgentKind::Codex),
        },
        "invalid_spec" => AppError::InvalidSpec {
            pointer: input
                .get("pointer")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            value: input.get("value").cloned().unwrap_or(Value::Null),
            message,
        },
        "task_terminal" => {
            let id = input
                .get("id")
                .and_then(Value::as_str)
                .and_then(|s| s.parse().ok());
            let status = input
                .get("status")
                .and_then(Value::as_str)
                .and_then(|s| crate::domain::ProcessStatus::from_storage(s).ok())
                .unwrap_or(crate::domain::ProcessStatus::Failed);
            match id {
                Some(id) => AppError::TaskTerminal { id, status },
                None => AppError::Internal { message },
            }
        }
        "tasks_in_flight" => AppError::TasksInFlight {
            count: input.get("count").and_then(Value::as_u64).unwrap_or(0) as usize,
        },
        "daemon_already_running" => AppError::DaemonAlreadyRunning,
        "too_many_reports" => AppError::TooManyReports {
            count: input.get("count").and_then(Value::as_u64).unwrap_or(0) as usize,
        },
        "summary_too_long" => AppError::SummaryTooLong {
            len: input.get("len").and_then(Value::as_u64).unwrap_or(0) as usize,
        },
        _ => AppError::Internal {
            message: format!("http {} {message}", status.as_u16()),
        },
    }
}
