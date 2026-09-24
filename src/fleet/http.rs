//! Minimal HTTP/1 client for daemon-to-daemon requests over TCP

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::client::conn::http1;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use tokio::net::TcpStream;

use crate::fleet::address::MachineAddress;

/// Default time limit for one request, connect through body
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Default upper bound on a JSON response body
pub const DEFAULT_MAX_BODY: usize = 1024 * 1024;

/// Time limit for each connect attempt except the last, so an address that
/// silently drops packets cannot use the whole request budget
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);

/// Client for `/v1/cluster/*` routes on peer daemons
#[derive(Debug, Clone, Copy)]
pub struct ClusterClient {
    timeout: Duration,
    max_body: usize,
}

impl Default for ClusterClient {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_REQUEST_TIMEOUT,
            max_body: DEFAULT_MAX_BODY,
        }
    }
}

/// Status and body of a completed request
#[derive(Debug, Clone)]
pub struct ClusterResponse {
    /// HTTP status
    pub status: StatusCode,
    /// Full body, at most the client's body limit
    pub body: Bytes,
}

/// Why a request produced no response
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    /// The host refused the TCP connection, so it is reachable but nothing
    /// listens at that port
    #[error("connect {address}: connection refused")]
    Refused {
        /// Destination
        address: MachineAddress,
    },
    /// TCP connect failed for another reason, such as an unreachable host
    #[error("connect {address}: {message}")]
    Connect {
        /// Destination
        address: MachineAddress,
        /// OS error text
        message: String,
    },
    /// HTTP exchange failed after connect
    #[error("http {address}: {message}")]
    Http {
        /// Destination
        address: MachineAddress,
        /// Protocol error text
        message: String,
    },
    /// No complete response within the time limit
    #[error("timed out after {timeout:?} waiting for {address}")]
    Timeout {
        /// Destination
        address: MachineAddress,
        /// Limit that elapsed
        timeout: Duration,
    },
    /// Body exceeded the limit
    #[error("response from {address} exceeds {limit} bytes")]
    BodyTooLarge {
        /// Destination
        address: MachineAddress,
        /// Byte limit
        limit: usize,
    },
}

impl ClusterClient {
    /// Client with a custom time limit and body limit
    #[must_use]
    pub fn new(timeout: Duration, max_body: usize) -> Self {
        Self { timeout, max_body }
    }

    /// Time limit for one request
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// GET `path` on `address`
    pub async fn get(
        &self,
        address: &MachineAddress,
        path: &str,
    ) -> Result<ClusterResponse, TransportError> {
        self.send(address, Method::GET, path, None).await
    }

    /// POST a JSON body to `path` on `address`
    pub async fn post_json<T: Serialize>(
        &self,
        address: &MachineAddress,
        path: &str,
        body: &T,
    ) -> Result<ClusterResponse, TransportError> {
        let bytes = serde_json::to_vec(body).map_err(|err| TransportError::Http {
            address: address.clone(),
            message: format!("encode body: {err}"),
        })?;
        self.send(address, Method::POST, path, Some(Bytes::from(bytes)))
            .await
    }

    async fn send(
        &self,
        address: &MachineAddress,
        method: Method,
        path: &str,
        body: Option<Bytes>,
    ) -> Result<ClusterResponse, TransportError> {
        let exchange = self.exchange(address, method, path, body);
        tokio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| TransportError::Timeout {
                address: address.clone(),
                timeout: self.timeout,
            })?
    }

    async fn exchange(
        &self,
        address: &MachineAddress,
        method: Method,
        path: &str,
        body: Option<Bytes>,
    ) -> Result<ClusterResponse, TransportError> {
        let http_err = |message: String| TransportError::Http {
            address: address.clone(),
            message,
        };
        let authority = address.authority();
        let stream = connect(&authority).await.map_err(|err| match err.kind() {
            std::io::ErrorKind::ConnectionRefused => TransportError::Refused {
                address: address.clone(),
            },
            _ => TransportError::Connect {
                address: address.clone(),
                message: err.to_string(),
            },
        })?;
        let (mut sender, conn) = http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|err| http_err(format!("handshake: {err}")))?;
        tokio::spawn(async move {
            // connection errors surface on `send_request` and the body read below
            let _ = conn.await;
        });
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header(hyper::header::HOST, &authority);
        if body.is_some() {
            builder = builder.header(hyper::header::CONTENT_TYPE, "application/json");
        }
        let request = builder
            .body(Full::new(body.unwrap_or_default()))
            .map_err(|err| http_err(format!("build request: {err}")))?;
        let response = sender
            .send_request(request)
            .await
            .map_err(|err| http_err(format!("send: {err}")))?;
        let status = response.status();
        let body = Limited::new(response.into_body(), self.max_body)
            .collect()
            .await
            .map_err(|err| {
                if err.is::<http_body_util::LengthLimitError>() {
                    TransportError::BodyTooLarge {
                        address: address.clone(),
                        limit: self.max_body,
                    }
                } else {
                    http_err(format!("read body: {err}"))
                }
            })?
            .to_bytes();
        Ok(ClusterResponse { status, body })
    }
}

/// Connect to the first answering address of `authority`
///
/// A `.local` name can resolve to several link-local IPv6 addresses before its
/// IPv4 address, and daemons listen on IPv4 by default, so IPv4 is tried first
/// and link-local IPv6 last
async fn connect(authority: &str) -> io::Result<TcpStream> {
    let mut addrs: Vec<SocketAddr> = tokio::net::lookup_host(authority).await?.collect();
    addrs.sort_by_key(|addr| connect_rank(addr.ip()));
    connect_any(&addrs).await
}

fn connect_rank(ip: IpAddr) -> u8 {
    match ip {
        IpAddr::V4(_) => 0,
        IpAddr::V6(v6) if (v6.segments()[0] & 0xffc0) == 0xfe80 => 2,
        IpAddr::V6(_) => 1,
    }
}

/// Try `addrs` in order. The last attempt is bounded only by the request timeout
///
/// The result is `ConnectionRefused` only when every address refused, because
/// callers read a refusal as "reachable, but nothing listens"
async fn connect_any(addrs: &[SocketAddr]) -> io::Result<TcpStream> {
    let mut first_other_error = None;
    let mut refused = None;
    for (index, addr) in addrs.iter().enumerate() {
        let attempt = TcpStream::connect(addr);
        let result = if index + 1 == addrs.len() {
            attempt.await
        } else {
            tokio::time::timeout(CONNECT_ATTEMPT_TIMEOUT, attempt)
                .await
                .unwrap_or_else(|_| {
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("connect {addr} timed out"),
                    ))
                })
        };
        match result {
            Ok(stream) => return Ok(stream),
            Err(err) if err.kind() == io::ErrorKind::ConnectionRefused => refused = Some(err),
            Err(err) => {
                first_other_error.get_or_insert(err);
            }
        }
    }
    Err(first_other_error
        .or(refused)
        .unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no address resolved")))
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::net::SocketAddr;

    use tokio::net::TcpListener;

    use super::connect_any;

    async fn closed_addr() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    }

    #[tokio::test]
    async fn connect_moves_past_a_refusing_address() {
        let refused = closed_addr().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let open = listener.local_addr().unwrap();

        let stream = connect_any(&[refused, open]).await.unwrap();

        assert_eq!(stream.peer_addr().unwrap(), open);
    }

    #[tokio::test]
    async fn connect_reports_refused_only_when_every_address_refuses() {
        let error = connect_any(&[closed_addr().await, closed_addr().await])
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    }
}
