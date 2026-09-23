//! Hyper HTTP/1 client over a Unix socket.

use std::path::PathBuf;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Serialize, de::DeserializeOwned};
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

    /// GET.
    pub async fn get(&self, path: &str) -> Result<Value, AppError> {
        self.request("GET", path, None).await
    }

    /// POST JSON.
    pub async fn post(&self, path: &str, body: &Value) -> Result<Value, AppError> {
        self.request("POST", path, Some(body)).await
    }

    /// POST a typed JSON request and decode a typed response.
    pub async fn post_json<T, R>(&self, path: &str, body: &T) -> Result<R, AppError>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let value = serde_json::to_value(body)?;
        let response = self.post(path, &value).await?;
        serde_json::from_value(response).map_err(|err| AppError::Internal {
            message: format!("invalid daemon response: {err}"),
        })
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
            // connection errors surface on `send_request` and the body read below
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

pub(crate) fn map_error(status: StatusCode, bytes: &[u8]) -> AppError {
    if let Ok(value) = serde_json::from_slice::<Value>(bytes)
        && let Some(error) = value.get("error")
    {
        let code = error
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("internal");
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("daemon error")
            .to_string();
        let input = error.get("input").unwrap_or(&Value::Null);
        return from_code(code, message, input, status);
    }
    AppError::Internal {
        message: format!(
            "http {} {}",
            status.as_u16(),
            String::from_utf8_lossy(bytes)
        ),
    }
}

fn from_code(code: &str, message: String, input: &Value, status: StatusCode) -> AppError {
    match code {
        "daemon_unavailable" => AppError::DaemonUnavailable { message },
        "cluster_lookup_incomplete" => {
            let task = input
                .get("task")
                .cloned()
                .and_then(|v| serde_json::from_value(v).ok());
            let unchecked = input
                .get("unchecked")
                .cloned()
                .and_then(|v| serde_json::from_value(v).ok());
            match (task, unchecked) {
                (Some(task), Some(unchecked)) => {
                    AppError::ClusterLookupIncomplete { task, unchecked }
                }
                _ => AppError::Internal { message },
            }
        }
        "task_unavailable" => {
            let task = input
                .get("task")
                .cloned()
                .and_then(|v| serde_json::from_value(v).ok());
            let machine = input
                .get("machine")
                .cloned()
                .and_then(|v| serde_json::from_value(v).ok());
            match (task, machine) {
                (Some(task), Some(machine)) => AppError::TaskUnavailable { task, machine },
                _ => AppError::Internal { message },
            }
        }
        "task_not_started" => input
            .get("task")
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
            .map_or(AppError::Internal { message }, |task| {
                AppError::TaskNotStarted { task }
            }),
        "cluster_task_conflict" => input
            .get("task")
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
            .map_or(AppError::Internal { message }, |task| {
                AppError::ClusterTaskConflict { task }
            }),
        "machine_unavailable" => {
            let machine = input
                .get("machine")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            match machine {
                Some(machine) => AppError::MachineUnavailable { machine, message },
                None => AppError::Internal { message },
            }
        }
        "remote_submission_unavailable" => AppError::RemoteSubmissionUnavailable { message },
        "machine_not_found" => AppError::MachineNotFound {
            machine: input
                .get("machine")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        },
        "machine_identity_mismatch" => {
            let expected = input
                .get("expected")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            let found = input
                .get("found")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            match expected {
                Some(expected) => AppError::MachineIdentityMismatch { expected, found },
                None => AppError::Internal { message },
            }
        }
        "duplicate_machine_name" => {
            let name = input
                .get("name")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            let machines = input
                .get("machines")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            match (name, machines) {
                (Some(name), Some(machines)) => AppError::DuplicateMachineName { name, machines },
                _ => AppError::Internal { message },
            }
        }
        "cluster_protocol_incompatible" => {
            let machine = input
                .get("machine")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            let local = input
                .get("local")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            let remote = input
                .get("remote")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            match (machine, local, remote) {
                (Some(machine), Some(local), Some(remote)) => {
                    AppError::ClusterProtocolIncompatible {
                        machine,
                        local,
                        remote,
                    }
                }
                _ => AppError::Internal { message },
            }
        }
        "submission_outcome_unknown" | "submission_rejected" | "submission_conflict" => {
            let request = input
                .get("request_id")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            let task = input
                .get("task_id")
                .and_then(Value::as_str)
                .and_then(|value| value.parse().ok());
            match (request, task) {
                (Some(request), Some(task)) if code == "submission_outcome_unknown" => {
                    AppError::SubmissionOutcomeUnknown {
                        request,
                        task,
                        message,
                    }
                }
                (Some(request), Some(task)) if code == "submission_rejected" => {
                    AppError::SubmissionRejected {
                        request,
                        task,
                        reason: input
                            .get("reason")
                            .and_then(Value::as_str)
                            .unwrap_or(&message)
                            .to_string(),
                    }
                }
                (Some(request), Some(task)) => AppError::SubmissionConflict {
                    request,
                    task,
                    message,
                },
                _ => AppError::Internal { message },
            }
        }
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
        "route_not_found" => input
            .get("task")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .map_or(AppError::Internal { message }, |task| {
                AppError::RouteNotFound { task }
            }),
        "message_invalid" => AppError::MessageInvalid {
            message: input
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or(&message)
                .to_string(),
        },
        "agent_thread_not_found" => AppError::AgentThreadNotFound {
            selector: input
                .get("selector")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        },
        "message_conflict" => input
            .get("message_id")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .map_or(AppError::Internal { message }, |id| {
                AppError::MessageConflict { id }
            }),
        "message_delivery_failed" => input
            .get("message_id")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .map_or(
                AppError::Internal {
                    message: message.clone(),
                },
                |id| AppError::MessageDeliveryFailed { id, message },
            ),
        "message_outcome_unknown" => {
            let id = input
                .get("message_id")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            let machine = input
                .get("machine")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            match (id, machine) {
                (Some(id), Some(machine)) => AppError::MessageOutcomeUnknown {
                    id,
                    machine,
                    message,
                },
                _ => AppError::Internal { message },
            }
        }
        "message_receiver_unavailable" => AppError::MessageUnavailable { message },
        "cwd_not_found" => AppError::CwdNotFound {
            path: input
                .get("cwd")
                .and_then(Value::as_str)
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
        },
        "executable_missing" => AppError::ExecutableMissing {
            program: input
                .get("program")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
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
        "agent_configuration" => {
            let agent = input
                .get("agent")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            match agent {
                Some(agent) => AppError::AgentConfiguration { agent, message },
                None => AppError::Internal { message },
            }
        }
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::domain::AgentKind;

    #[test]
    fn agent_configuration_error_keeps_its_public_code() {
        let error = from_code(
            "agent_configuration",
            "opencode child configuration: invalid JSONC".into(),
            &json!({"agent": "opencode"}),
            StatusCode::INTERNAL_SERVER_ERROR,
        );

        assert!(matches!(
            error,
            AppError::AgentConfiguration {
                agent: AgentKind::OpenCode,
                ..
            }
        ));
        assert_eq!(error.code(), "agent_configuration");
    }

    #[test]
    fn remote_submit_errors_keep_retry_identity_and_retryability() {
        let request = crate::submission::RequestId::new();
        let task = crate::domain::TaskId::new();
        let unknown = from_code(
            "submission_outcome_unknown",
            "response lost".into(),
            &json!({"request_id": request, "task_id": task}),
            StatusCode::SERVICE_UNAVAILABLE,
        );
        assert!(unknown.retryable());
        assert_eq!(unknown.input()["task_id"], task.to_string());
        assert_eq!(unknown.input()["request_id"], request.0.to_string());

        let machine = crate::machine::MachineId::new();
        let unavailable = from_code(
            "machine_unavailable",
            "probe failed".into(),
            &json!({"machine": machine}),
            StatusCode::SERVICE_UNAVAILABLE,
        );
        assert_eq!(unavailable.code(), "machine_unavailable");
        assert!(unavailable.retryable());
    }
}
