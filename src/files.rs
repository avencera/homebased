//! Device-wide read-only filesystem access for the dashboard file browser.

mod content;
mod host;
mod listing;
mod token;

pub use content::{
    ContentDispositionKind, MAX_CONCURRENT_STREAMS, MAX_STREAMS_PER_CLIENT, STREAM_IDLE_TIMEOUT,
    StreamSlots, content_headers, disposition_for, guess_media, is_inline_media, open_file_stream,
    parse_range,
};
pub use host::{HostPolicy, host_guard};
pub use listing::{
    ContentOriginBody, DirectoryListing, EntryKind, FileEntry, ResolveBody, ResolvedPath,
    list_directory, resolve_absolute_path,
};
pub use token::{PathToken, decode_path, encode_path, path_display};
