//! Blocking HTTP requests through the system `curl` executable

use std::io::{self, Read, Write};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

const BODY_LIMIT: usize = 1024 * 1024;
const STDERR_LIMIT: usize = 8 * 1024;
const STATUS_MARKER: &str = "\nHOMEBASED_HTTP_STATUS:";
const STATUS_TAIL_LIMIT: usize = 64;

/// HTTP method used by a blocking curl request
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CurlMethod {
    /// Send a GET request
    Get,
    /// Send a POST request
    Post,
}

/// Input for one blocking curl request
pub(crate) struct CurlRequest<'a> {
    /// HTTP method
    pub(crate) method: CurlMethod,
    /// Full request URL
    pub(crate) url: &'a str,
    /// Optional bearer token
    pub(crate) bearer: Option<&'a str>,
    /// Optional JSON request body
    pub(crate) json_body: Option<&'a serde_json::Value>,
    /// Maximum request time
    pub(crate) timeout: Duration,
}

/// Result from one blocking curl request
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurlResponse {
    /// HTTP response status
    pub(crate) status: u16,
    /// Response body, limited to the first 1 MiB
    pub(crate) body: String,
}

/// Send a blocking HTTP request through the system `curl` executable
pub(crate) fn send(request: &CurlRequest<'_>) -> Result<CurlResponse, String> {
    let config = render_config(request)?;
    let args = curl_args(request.timeout);
    let mut child = Command::new("curl")
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                "curl is not installed or is not on PATH".to_string()
            } else {
                format!("could not start curl: {error}")
            }
        })?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "could not read curl response".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "could not read curl errors".to_string())?;
    let stdout_reader = thread::spawn(move || read_capture(stdout, BODY_LIMIT, STATUS_TAIL_LIMIT));
    let stderr_reader = thread::spawn(move || read_capture(stderr, STDERR_LIMIT, 0));

    let write_result = child
        .stdin
        .take()
        .ok_or_else(|| "could not send curl request config".to_string())?
        .write_all(config.as_bytes());
    let status = child
        .wait()
        .map_err(|error| format!("could not wait for curl: {error}"))?;
    let mut stdout = join_capture(stdout_reader, "curl response")?;
    let stderr = join_capture(stderr_reader, "curl errors")?;

    if !status.success() {
        return Err(curl_exit_error(
            status.code(),
            &stderr.bytes,
            request.bearer,
        ));
    }
    if let Err(error) = write_result {
        return Err(format!("could not send curl request config: {error}"));
    }

    let (http_status, body_len) = parse_status(&stdout)
        .map_err(|message| append_stderr(message, &stderr.bytes, request.bearer))?;
    stdout.bytes.truncate(body_len.min(stdout.bytes.len()));
    Ok(CurlResponse {
        status: http_status,
        body: String::from_utf8_lossy(&stdout.bytes).into_owned(),
    })
}

fn render_config(request: &CurlRequest<'_>) -> Result<String, String> {
    let mut config = String::new();
    push_config_value(&mut config, "url", request.url);
    push_config_value(
        &mut config,
        "request",
        match request.method {
            CurlMethod::Get => "GET",
            CurlMethod::Post => "POST",
        },
    );

    if let Some(bearer) = request.bearer {
        push_config_value(
            &mut config,
            "header",
            &format!("Authorization: Bearer {bearer}"),
        );
    }
    if let Some(body) = request.json_body {
        let body = serde_json::to_string(body)
            .map_err(|error| format!("could not encode curl JSON body: {error}"))?;
        push_config_value(&mut config, "header", "Content-Type: application/json");
        push_config_value(&mut config, "data", &body);
    }

    Ok(config)
}

fn push_config_value(config: &mut String, key: &str, value: &str) {
    config.push_str(key);
    config.push_str(" = \"");
    for character in value.chars() {
        match character {
            '\\' => config.push_str("\\\\"),
            '"' => config.push_str("\\\""),
            '\n' => config.push_str("\\n"),
            '\r' => config.push_str("\\r"),
            '\t' => config.push_str("\\t"),
            character => config.push(character),
        }
    }
    config.push_str("\"\n");
}

fn curl_args(timeout: Duration) -> Vec<String> {
    let timeout = timeout.as_secs_f64().max(0.001);
    vec![
        "--disable".into(),
        "--silent".into(),
        "--show-error".into(),
        "--max-time".into(),
        format!("{timeout}"),
        "--write-out".into(),
        format!("{STATUS_MARKER}%{{http_code}}\n"),
        "--config".into(),
        "-".into(),
    ]
}

struct Capture {
    bytes: Vec<u8>,
    tail: Vec<u8>,
    total: usize,
}

fn read_capture(mut reader: impl Read, limit: usize, tail_limit: usize) -> io::Result<Capture> {
    let mut capture = Capture {
        bytes: Vec::with_capacity(limit),
        tail: Vec::with_capacity(tail_limit),
        total: 0,
    };
    let mut buffer = [0; 8192];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(capture);
        }

        capture.total = capture.total.saturating_add(read);
        let keep = limit.saturating_sub(capture.bytes.len()).min(read);
        capture.bytes.extend_from_slice(&buffer[..keep]);

        if tail_limit > 0 {
            capture.tail.extend_from_slice(&buffer[..read]);
            if capture.tail.len() > tail_limit {
                let excess = capture.tail.len() - tail_limit;
                capture.tail.drain(..excess);
            }
        }
    }
}

fn join_capture(
    reader: thread::JoinHandle<io::Result<Capture>>,
    name: &str,
) -> Result<Capture, String> {
    reader
        .join()
        .map_err(|_| format!("could not read {name}"))?
        .map_err(|error| format!("could not read {name}: {error}"))
}

fn parse_status(output: &Capture) -> Result<(u16, usize), String> {
    let marker = STATUS_MARKER.as_bytes();
    let Some(marker_index) = output
        .tail
        .windows(marker.len())
        .rposition(|window| window == marker)
    else {
        return Err("curl did not return an HTTP status".into());
    };
    let status_text = std::str::from_utf8(&output.tail[marker_index + marker.len()..])
        .map_err(|_| "curl returned an invalid HTTP status".to_string())?
        .trim();
    let status = status_text
        .parse::<u16>()
        .map_err(|_| "curl returned an invalid HTTP status".to_string())?;
    let tail_start = output.total.saturating_sub(output.tail.len());
    let body_len = tail_start.saturating_add(marker_index);
    Ok((status, body_len))
}

fn curl_exit_error(code: Option<i32>, stderr: &[u8], bearer: Option<&str>) -> String {
    let message = format!(
        "curl failed with exit status {}",
        code.map_or_else(|| "unknown".into(), |code| code.to_string())
    );
    append_stderr(message, stderr, bearer)
}

fn append_stderr(message: String, stderr: &[u8], bearer: Option<&str>) -> String {
    let stderr = String::from_utf8_lossy(stderr);
    let stderr = bearer.filter(|token| !token.is_empty()).map_or_else(
        || stderr.to_string(),
        |token| stderr.replace(token, "[redacted]"),
    );
    let stderr = stderr.trim();
    if stderr.is_empty() {
        message
    } else {
        let excerpt = stderr.chars().take(512).collect::<String>();
        format!("{message}: {excerpt}")
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader};
    use std::net::{TcpListener, TcpStream};
    use std::process::Command;
    use std::thread;

    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn config_quotes_and_escapes_values_without_putting_secrets_in_arguments() {
        let token = "private\\token\"\nsecond\rthird\tlast";
        let body = json!({"text": "quote \" slash \\ newline\n carriage\r tab\t"});
        let request = CurlRequest {
            method: CurlMethod::Post,
            url: "https://example.test/path?q=\"\\\n\r\t",
            bearer: Some(token),
            json_body: Some(&body),
            timeout: Duration::from_secs(5),
        };

        let config = render_config(&request).unwrap();
        let args = curl_args(request.timeout).join(" ");

        assert!(config.contains("url = \"https://example.test/path?q=\\\"\\\\\\n\\r\\t\""));
        assert!(
            config.contains("Authorization: Bearer private\\\\token\\\"\\nsecond\\rthird\\tlast")
        );
        assert!(!args.contains(token));
        assert!(!args.contains("example.test"));
        assert!(config.contains("Content-Type: application/json"));
    }

    #[test]
    fn curl_round_trip_returns_client_errors_and_the_response_body() {
        match Command::new("curl").arg("--version").output() {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                eprintln!("skipping curl round-trip test because curl is not installed");
                return;
            }
            Err(error) => panic!("could not check curl: {error}"),
            Ok(output) => assert!(output.status.success(), "curl --version failed"),
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            check_request(stream);
        });
        let body = json!({"title": "test \"quoted\"", "message": "line one\nline two"});
        let url = format!("http://{address}/notify?source=unit");
        let response = send(&CurlRequest {
            method: CurlMethod::Post,
            url: &url,
            bearer: Some("unit-test-token"),
            json_body: Some(&body),
            timeout: Duration::from_secs(5),
        })
        .unwrap();

        server.join().unwrap();
        assert_eq!(response.status, 418);
        assert_eq!(response.body, "teapot");
    }

    fn check_request(stream: TcpStream) {
        let mut reader = BufReader::new(stream);
        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();
        assert_eq!(request_line.trim_end(), "POST /notify?source=unit HTTP/1.1");

        let mut authorization = None;
        let mut content_length = 0;
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).unwrap();
            if header == "\r\n" {
                break;
            }
            if let Some((name, value)) = header.split_once(':') {
                if name.eq_ignore_ascii_case("authorization") {
                    authorization = Some(value.trim().to_string());
                }
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = value.trim().parse::<usize>().unwrap();
                }
            }
        }
        assert_eq!(authorization.as_deref(), Some("Bearer unit-test-token"));

        let mut request_body = vec![0; content_length];
        reader.read_exact(&mut request_body).unwrap();
        let request_body: Value = serde_json::from_slice(&request_body).unwrap();
        assert_eq!(request_body["title"], "test \"quoted\"");
        assert_eq!(request_body["message"], "line one\nline two");

        let stream = reader.get_mut();
        stream
            .write_all(b"HTTP/1.1 418 I'm a teapot\r\nContent-Length: 6\r\nConnection: close\r\n\r\nteapot")
            .unwrap();
    }
}
