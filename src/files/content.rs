//! Raw file streaming, media types, and range support for the content origin.

use std::collections::HashMap;
use std::fs::Metadata;
use std::io::SeekFrom;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::stream::unfold;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

use crate::error::AppError;
use crate::files::token::path_display;

/// Cap concurrent content streams so one client cannot exhaust capacity.
pub const MAX_CONCURRENT_STREAMS: usize = 32;

/// Cap streams from one peer so other devices retain capacity.
pub const MAX_STREAMS_PER_CLIENT: usize = 8;

/// Idle timeout between successful stream reads.
pub const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

const CHUNK: usize = 64 * 1024;

/// Shared permit pool for content streams.
#[derive(Clone, Debug)]
pub struct StreamSlots {
    global: Arc<Semaphore>,
    clients: Arc<Mutex<HashMap<IpAddr, usize>>>,
}

/// One global and per-client stream reservation, released on drop.
#[derive(Debug)]
pub struct StreamPermit {
    _global: OwnedSemaphorePermit,
    peer: IpAddr,
    clients: Arc<Mutex<HashMap<IpAddr, usize>>>,
}

impl Drop for StreamPermit {
    fn drop(&mut self) {
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(count) = clients.get_mut(&self.peer) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            clients.remove(&self.peer);
        }
    }
}

impl StreamSlots {
    /// Create a pool with [`MAX_CONCURRENT_STREAMS`] permits.
    #[must_use]
    pub fn new() -> Self {
        Self {
            global: Arc::new(Semaphore::new(MAX_CONCURRENT_STREAMS)),
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Acquire one stream permit, or fail when the pool is empty.
    pub fn try_acquire(&self, peer: IpAddr) -> Result<StreamPermit, AppError> {
        let global =
            Arc::clone(&self.global)
                .try_acquire_owned()
                .map_err(|_| AppError::StreamLimit {
                    message: format!(
                        "too many concurrent file streams (max {MAX_CONCURRENT_STREAMS})"
                    ),
                })?;
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = clients.entry(peer).or_default();
        if *count >= MAX_STREAMS_PER_CLIENT {
            return Err(AppError::StreamLimit {
                message: format!(
                    "too many concurrent file streams from this client (max {MAX_STREAMS_PER_CLIENT})"
                ),
            });
        }
        *count += 1;
        drop(clients);

        Ok(StreamPermit {
            _global: global,
            peer,
            clients: Arc::clone(&self.clients),
        })
    }
}

impl Default for StreamSlots {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether the browser should render inline or download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentDispositionKind {
    /// Text, raster image, or HTML.
    Inline,
    /// Everything else.
    Attachment,
}

/// Parsed `Range: bytes=` request. Only single ranges are supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// Inclusive start.
    pub start: u64,
    /// Inclusive end.
    pub end: u64,
}

/// Guess a media type from the path. Falls back to octet-stream.
#[must_use]
pub fn guess_media(path: &Path) -> String {
    mime_guess::from_path(path)
        .first_raw()
        .unwrap_or("application/octet-stream")
        .to_string()
}

/// Whether this media type opens inline in a new tab.
#[must_use]
pub fn is_inline_media(media: &str) -> bool {
    let base = media.split(';').next().unwrap_or(media).trim();
    base.starts_with("text/")
        || base == "application/json"
        || base == "application/xml"
        || base == "image/png"
        || base == "image/jpeg"
        || base == "image/gif"
        || base == "image/webp"
        || base == "image/bmp"
        || base == "image/x-icon"
        || base == "text/html"
        || base == "application/xhtml+xml"
}

/// Build content-disposition kind from media type.
#[must_use]
pub fn disposition_for(media: &str) -> ContentDispositionKind {
    if is_inline_media(media) {
        ContentDispositionKind::Inline
    } else {
        ContentDispositionKind::Attachment
    }
}

/// Parse a single `bytes=` range against `len`. Returns `None` for an absent header.
pub fn parse_range(headers: &HeaderMap, len: u64) -> Result<Option<ByteRange>, RangeError> {
    let Some(value) = headers.get(header::RANGE) else {
        return Ok(None);
    };
    let raw = value.to_str().map_err(|_| RangeError::Invalid)?;
    let Some(spec) = raw.strip_prefix("bytes=") else {
        return Err(RangeError::Invalid);
    };
    if spec.contains(',') {
        return Err(RangeError::Invalid);
    }
    let (start_raw, end_raw) = spec.split_once('-').ok_or(RangeError::Invalid)?;
    let (start, end) = if start_raw.is_empty() {
        let suffix: u64 = end_raw.parse().map_err(|_| RangeError::Invalid)?;
        if suffix == 0 || len == 0 {
            return Err(RangeError::Unsatisfiable);
        }
        let start = len.saturating_sub(suffix);
        (start, len.saturating_sub(1))
    } else {
        let start: u64 = start_raw.parse().map_err(|_| RangeError::Invalid)?;
        let end = if end_raw.is_empty() {
            if len == 0 {
                return Err(RangeError::Unsatisfiable);
            }
            len.saturating_sub(1)
        } else {
            end_raw.parse().map_err(|_| RangeError::Invalid)?
        };
        (start, end)
    };
    if len == 0 || start >= len || start > end {
        return Err(RangeError::Unsatisfiable);
    }
    let end = end.min(len.saturating_sub(1));
    Ok(Some(ByteRange { start, end }))
}

/// Range header failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeError {
    /// Malformed header.
    Invalid,
    /// Valid header that does not overlap the file.
    Unsatisfiable,
}

/// Shared response headers for raw content.
pub fn content_headers(
    path: &Path,
    meta: &Metadata,
    media: &str,
    disposition: ContentDispositionKind,
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(media)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("same-origin"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    if let Ok(etag) = etag_for(meta) {
        headers.insert(header::ETAG, etag);
    }
    if let Some(modified) = last_modified(meta) {
        headers.insert(header::LAST_MODIFIED, modified);
    }
    let file_name = path
        .file_name()
        .map(|name| {
            name.to_string_lossy()
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
        })
        .unwrap_or_else(|| "download".into());
    let disposition_value = match disposition {
        ContentDispositionKind::Inline => format!("inline; filename=\"{file_name}\""),
        ContentDispositionKind::Attachment => format!("attachment; filename=\"{file_name}\""),
    };
    if let Ok(value) = HeaderValue::from_str(&disposition_value) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }
    headers
}

fn etag_for(meta: &Metadata) -> Result<HeaderValue, ()> {
    let modified = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let tag = format!("\"{:x}-{:x}\"", meta.len(), modified);
    HeaderValue::from_str(&tag).map_err(|_| ())
}

fn last_modified(meta: &Metadata) -> Option<HeaderValue> {
    let time = meta.modified().ok()?;
    let duration = time.duration_since(SystemTime::UNIX_EPOCH).ok()?;
    let secs = i64::try_from(duration.as_secs()).ok()?;
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)?;
    HeaderValue::from_str(&dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()).ok()
}

/// Open a regular file and build a streaming HTTP response.
pub async fn open_file_stream(
    path: PathBuf,
    headers_in: &HeaderMap,
    slots: &StreamSlots,
    peer: IpAddr,
) -> Result<Response, AppError> {
    let permit = slots.try_acquire(peer)?;
    let meta = tokio::fs::metadata(&path)
        .await
        .map_err(|err| map_io(&path, err))?;
    if !meta.is_file() {
        return Err(AppError::UnsupportedFile {
            message: format!("{} is not a regular file", path_display(&path)),
        });
    }
    let len = meta.len();
    let media = {
        let guessed = guess_media(&path);
        if disposition_for(&guessed) == ContentDispositionKind::Inline {
            guessed
        } else {
            "application/octet-stream".into()
        }
    };
    let disposition = disposition_for(&media);
    let mut headers = content_headers(&path, &meta, &media, disposition);

    let range = match parse_range(headers_in, len) {
        Ok(range) => range,
        Err(RangeError::Invalid) => {
            return Err(AppError::Usage {
                message: "invalid Range header".into(),
            });
        }
        Err(RangeError::Unsatisfiable) => {
            headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{len}"))
                    .unwrap_or_else(|_| HeaderValue::from_static("bytes */0")),
            );
            return Ok((StatusCode::RANGE_NOT_SATISFIABLE, headers).into_response());
        }
    };

    let (status, start, end) = if let Some(range) = range {
        headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {}-{}/{len}", range.start, range.end))
                .unwrap_or_else(|_| HeaderValue::from_static("bytes 0-0/0")),
        );
        (StatusCode::PARTIAL_CONTENT, range.start, range.end)
    } else if len == 0 {
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        drop(permit);
        return Ok((StatusCode::OK, headers, Body::empty()).into_response());
    } else {
        (StatusCode::OK, 0, len.saturating_sub(1))
    };
    let content_len = end - start + 1;
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_len.to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("0")),
    );

    let mut file = File::open(&path).await.map_err(|err| map_io(&path, err))?;
    file.seek(SeekFrom::Start(start))
        .await
        .map_err(|err| map_io(&path, err))?;
    let body = Body::from_stream(byte_stream(file, content_len, permit));
    Ok((status, headers, body).into_response())
}

fn byte_stream(
    file: File,
    remaining: u64,
    permit: StreamPermit,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send {
    unfold(
        (file, remaining, permit),
        |(mut file, remaining, permit)| async move {
            if remaining == 0 {
                drop(permit);
                return None;
            }
            let to_read = usize::try_from(remaining.min(CHUNK as u64)).unwrap_or(CHUNK);
            let mut buf = vec![0_u8; to_read];
            match timeout(STREAM_IDLE_TIMEOUT, file.read(&mut buf)).await {
                Ok(Ok(0)) => {
                    drop(permit);
                    None
                }
                Ok(Ok(n)) => {
                    buf.truncate(n);
                    let next_remaining = remaining.saturating_sub(n as u64);
                    Some((Ok(Bytes::from(buf)), (file, next_remaining, permit)))
                }
                Ok(Err(err)) => Some((Err(err), (file, 0, permit))),
                Err(_) => Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "idle read timeout",
                    )),
                    (file, 0, permit),
                )),
            }
        },
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr};
    use tempfile::tempdir;

    #[test]
    fn inline_types_and_attachment_fallback() {
        assert!(is_inline_media("text/plain"));
        assert!(is_inline_media("text/html; charset=utf-8"));
        assert!(is_inline_media("image/png"));
        assert!(!is_inline_media("image/svg+xml"));
        assert!(!is_inline_media("application/zip"));
        assert_eq!(
            disposition_for("text/plain"),
            ContentDispositionKind::Inline
        );
        assert_eq!(
            disposition_for("application/octet-stream"),
            ContentDispositionKind::Attachment
        );
    }

    #[test]
    fn parses_byte_ranges() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=0-3"));
        assert_eq!(
            parse_range(&headers, 10).unwrap(),
            Some(ByteRange { start: 0, end: 3 })
        );
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=8-"));
        assert_eq!(
            parse_range(&headers, 10).unwrap(),
            Some(ByteRange { start: 8, end: 9 })
        );
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=-2"));
        assert_eq!(
            parse_range(&headers, 10).unwrap(),
            Some(ByteRange { start: 8, end: 9 })
        );
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=20-30"));
        assert_eq!(parse_range(&headers, 10), Err(RangeError::Unsatisfiable));
    }

    #[tokio::test]
    async fn streams_empty_and_ranged_files() {
        let dir = tempdir().unwrap();
        let empty = dir.path().join("empty.txt");
        fs::write(&empty, b"").unwrap();
        let slots = StreamSlots::new();
        let peer = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let response = open_file_stream(empty, &HeaderMap::new(), &slots, peer)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "0");

        let file = dir.path().join("data.bin");
        fs::write(&file, b"abcdefghij").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=2-5"));
        let response = open_file_stream(file, &headers, &slots, peer)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 2-5/10"
        );
        assert_eq!(
            response
                .headers()
                .get("cross-origin-resource-policy")
                .unwrap(),
            "same-origin"
        );
        assert_eq!(
            response.headers().get(header::REFERRER_POLICY).unwrap(),
            "no-referrer"
        );
        assert_eq!(
            response
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
    }

    #[tokio::test]
    async fn stream_limits_preserve_capacity_for_other_clients() {
        let slots = StreamSlots::new();
        let mut permits = Vec::new();
        let first = IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1));
        for _ in 0..MAX_STREAMS_PER_CLIENT {
            permits.push(slots.try_acquire(first).unwrap());
        }
        assert!(matches!(
            slots.try_acquire(first),
            Err(AppError::StreamLimit { .. })
        ));
        let second = IpAddr::V4(Ipv4Addr::new(100, 64, 0, 2));
        permits.push(slots.try_acquire(second).unwrap());
        drop(permits);
        assert!(slots.try_acquire(first).is_ok());
    }
}
