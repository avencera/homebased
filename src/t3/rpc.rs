//! One blocking call over T3's client WebSocket RPC
//!
//! T3 serves Effect RPC (`effect` `4.0.0-rc.115`, `RpcSerialization.layerJson`)
//! on `/ws`. Each text frame holds one JSON message or an array of them. A call
//! sends `{"_tag":"Request","id","tag","payload","headers":[]}` and waits for
//! `{"_tag":"Exit","requestId","exit"}`, where `exit` is
//! `{"_tag":"Success","value"}` or `{"_tag":"Failure","cause":[...]}` and each
//! cause is `Fail` with `error`, `Die` with `defect`, or `Interrupt`
//!
//! The socket needs a ticket from `POST /api/auth/websocket-ticket` with the
//! bearer token, passed as `wsTicket`, and `orchestrationProtocol=2`; without
//! the protocol the upgrade returns HTTP 426

use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tungstenite::handshake::HandshakeError;
use tungstenite::{Message, WebSocket};

use super::{ApiFailure, non_empty_string, percent_encode};
use crate::curl::{self, CurlMethod, CurlRequest};

const REQUEST_ID: &str = "1";

/// Decoded `Exit` of one RPC call
#[derive(Debug)]
pub(super) enum Exit {
    /// The handler succeeded; callers need only that T3 took the request
    Success,
    /// The handler failed; each item is one encoded cause
    Failure(Vec<Value>),
}

/// A failed call, and whether any request bytes may have reached T3
#[derive(Debug)]
pub(super) struct CallError {
    pub(super) failure: ApiFailure,
    /// After a send, a failure no longer proves that T3 did not act
    pub(super) sent: bool,
}

impl From<ApiFailure> for CallError {
    fn from(failure: ApiFailure) -> Self {
        Self {
            failure,
            sent: false,
        }
    }
}

/// Send `payload` to RPC `tag` and wait for its `Exit` until `timeout` passes
pub(super) fn call(
    origin: &str,
    token: &str,
    tag: &str,
    payload: &Value,
    timeout: Duration,
) -> Result<Exit, CallError> {
    let deadline = Instant::now() + timeout;
    let ticket = issue_ticket(origin, token, timeout)?;
    let mut socket = connect(origin, &ticket, deadline)?;
    let sent = |failure| CallError {
        failure,
        sent: true,
    };
    let request = json!({
        "_tag": "Request",
        "id": REQUEST_ID,
        "tag": tag,
        "payload": payload,
        "headers": []
    });
    socket
        .send(Message::text(request.to_string()))
        .map_err(|error| sent(ApiFailure::Unavailable(format!("send RPC {tag}: {error}"))))?;

    let exit = read_exit(&mut socket).map_err(sent);
    // the stream fails at once after the deadline, so cleanup cannot block
    let _ = socket.close(None);
    let _ = socket.flush();
    exit
}

/// TCP stream whose every read and write waits only until one shared deadline
///
/// A socket timeout bounds one wait, and one WebSocket read can make many
struct DeadlineStream {
    stream: TcpStream,
    deadline: Instant,
}

impl DeadlineStream {
    fn arm(&self) -> io::Result<()> {
        let left = self
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
            .ok_or_else(|| io::Error::from(ErrorKind::TimedOut))?;
        self.stream.set_read_timeout(Some(left))?;
        self.stream.set_write_timeout(Some(left))
    }
}

impl Read for DeadlineStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.arm()?;
        self.stream.read(buf)
    }
}

impl Write for DeadlineStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.arm()?;
        self.stream.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.arm()?;
        self.stream.flush()
    }
}

fn issue_ticket(origin: &str, token: &str, timeout: Duration) -> Result<String, ApiFailure> {
    let response = curl::send(&CurlRequest {
        method: CurlMethod::Post,
        url: &format!("{origin}/api/auth/websocket-ticket"),
        bearer: Some(token),
        json_body: None,
        timeout,
    })
    .map_err(ApiFailure::Unavailable)?;
    if matches!(response.status, 401 | 403 | 404) {
        return Err(ApiFailure::Changed(format!(
            "WebSocket ticket returned HTTP {} with a fresh session token",
            response.status
        )));
    }
    if response.status != 200 {
        return Err(ApiFailure::Unavailable(format!(
            "WebSocket ticket returned HTTP {}",
            response.status
        )));
    }

    serde_json::from_str::<Value>(&response.body)
        .ok()
        .and_then(|value| non_empty_string(value.get("ticket")))
        .ok_or_else(|| ApiFailure::Changed("WebSocket ticket response has no ticket".into()))
}

fn connect(
    origin: &str,
    ticket: &str,
    deadline: Instant,
) -> Result<WebSocket<DeadlineStream>, ApiFailure> {
    // the runtime file check accepts only `http://` origins without a path
    let authority = origin
        .strip_prefix("http://")
        .ok_or_else(|| ApiFailure::Unavailable("T3 origin is not http".into()))?;
    // a name lookup could block past the deadline, and T3 writes a numeric origin
    let address = authority
        .replacen("localhost:", "127.0.0.1:", 1)
        .parse::<SocketAddr>()
        .map_err(|_| {
            ApiFailure::Unavailable(format!("T3 origin {authority} is not an IP address"))
        })?;
    let stream = TcpStream::connect_timeout(&address, remaining(deadline)?)
        .map_err(|error| ApiFailure::Unavailable(format!("connect to T3 WebSocket: {error}")))?;
    let stream = DeadlineStream { stream, deadline };

    let url = format!(
        "ws://{authority}/ws?wsTicket={}&orchestrationProtocol=2",
        percent_encode(ticket)
    );
    match tungstenite::client(url.as_str(), stream) {
        Ok((socket, _)) => Ok(socket),
        Err(HandshakeError::Failure(tungstenite::Error::Http(response))) => {
            Err(upgrade_failure(response.status().as_u16()))
        }
        Err(error) => Err(ApiFailure::Unavailable(format!(
            "T3 WebSocket upgrade failed: {error}"
        ))),
    }
}

fn upgrade_failure(status: u16) -> ApiFailure {
    match status {
        426 => ApiFailure::Changed(
            "WebSocket upgrade returned HTTP 426; T3 no longer accepts orchestration protocol 2"
                .into(),
        ),
        401 | 403 | 404 => ApiFailure::Changed(format!(
            "WebSocket upgrade returned HTTP {status} with a fresh ticket"
        )),
        _ => ApiFailure::Unavailable(format!("WebSocket upgrade returned HTTP {status}")),
    }
}

fn read_exit(socket: &mut WebSocket<DeadlineStream>) -> Result<Exit, ApiFailure> {
    loop {
        let text = match socket.read() {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) => {
                return Err(ApiFailure::Unavailable(
                    "T3 closed the WebSocket before the RPC reply".into(),
                ));
            }
            Ok(_) => continue,
            Err(tungstenite::Error::Io(error))
                if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                return Err(ApiFailure::Unavailable("T3 RPC reply timed out".into()));
            }
            Err(error) => {
                return Err(ApiFailure::Unavailable(format!(
                    "read T3 RPC reply: {error}"
                )));
            }
        };

        let value: Value = serde_json::from_str(text.as_str())
            .map_err(|_| ApiFailure::Changed("T3 RPC frame is not valid JSON".into()))?;
        let messages = match value {
            Value::Array(messages) => messages,
            message => vec![message],
        };
        for message in messages {
            if let Some(exit) = handle_message(socket, &message)? {
                return Ok(exit);
            }
        }
    }
}

/// The `Exit` for this call, if `message` is it
fn handle_message(
    socket: &mut WebSocket<DeadlineStream>,
    message: &Value,
) -> Result<Option<Exit>, ApiFailure> {
    match message.get("_tag").and_then(Value::as_str) {
        Some("Exit") if is_this_request(message.get("requestId")) => {
            decode_exit(message.get("exit")).map(Some)
        }
        Some("Ping") => {
            let pong = json!({ "_tag": "Pong" }).to_string();
            socket
                .send(Message::text(pong))
                .map_err(|error| ApiFailure::Unavailable(format!("answer RPC ping: {error}")))?;
            Ok(None)
        }
        Some("Defect" | "ClientProtocolError") => Err(ApiFailure::Changed(
            "T3 RPC reported a protocol error".into(),
        )),
        _ => Ok(None),
    }
}

fn is_this_request(id: Option<&Value>) -> bool {
    match id {
        Some(Value::String(id)) => id == REQUEST_ID,
        Some(Value::Number(id)) => id.to_string() == REQUEST_ID,
        _ => false,
    }
}

fn decode_exit(exit: Option<&Value>) -> Result<Exit, ApiFailure> {
    let exit = exit.ok_or_else(|| ApiFailure::Changed("T3 RPC Exit has no exit".into()))?;
    match exit.get("_tag").and_then(Value::as_str) {
        Some("Success") => Ok(Exit::Success),
        Some("Failure") => exit
            .get("cause")
            .and_then(Value::as_array)
            .map(|cause| Exit::Failure(cause.clone()))
            .ok_or_else(|| ApiFailure::Changed("T3 RPC failure has no cause array".into())),
        _ => Err(ApiFailure::Changed(
            "T3 RPC exit has an unknown _tag".into(),
        )),
    }
}

fn remaining(deadline: Instant) -> Result<Duration, ApiFailure> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| ApiFailure::Unavailable("T3 RPC timed out".into()))
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::TcpListener;
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{ApiFailure, connect, read_exit};

    #[test]
    fn a_trickling_reply_cannot_outlast_the_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut socket = tungstenite::accept(stream).unwrap();
            // a text frame that promises 100 bytes, then one byte at a time
            let stream = socket.get_mut();
            let _ = stream.write_all(&[0x81, 100]);
            for _ in 0..40 {
                if stream.write_all(b"x").is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
        });

        let started = Instant::now();
        let deadline = started + Duration::from_millis(300);
        let mut socket = connect(&origin, "ticket", deadline).unwrap();
        let result = read_exit(&mut socket);

        assert!(
            matches!(result, Err(ApiFailure::Unavailable(_))),
            "{result:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(socket);
        server.join().unwrap();
    }
}
