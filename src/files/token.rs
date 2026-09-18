//! Opaque URL-safe path tokens. Encode raw path bytes so non-UTF-8 names navigate.

use std::ffi::OsStr;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use crate::error::AppError;

/// Opaque token for a filesystem path. URL-safe base64 of the absolute path bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PathToken(String);

impl PathToken {
    /// Wrap an already-encoded token string from a route parameter.
    #[must_use]
    pub fn from_encoded(raw: String) -> Self {
        Self(raw)
    }

    /// Borrow the token string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PathToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Encode an absolute path as an opaque token.
#[must_use]
pub fn encode_path(path: &Path) -> PathToken {
    PathToken(URL_SAFE_NO_PAD.encode(path.as_os_str().as_bytes()))
}

/// Decode a token back to a path. Does not check that the path exists.
pub fn decode_path(token: &PathToken) -> Result<PathBuf, AppError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token.as_str().as_bytes())
        .map_err(|_| AppError::Usage {
            message: "invalid path token".into(),
        })?;
    if bytes.is_empty() || bytes[0] != b'/' {
        return Err(AppError::Usage {
            message: "path token must decode to an absolute path".into(),
        });
    }
    Ok(PathBuf::from(std::ffi::OsString::from_vec(bytes)))
}

/// Lossy UTF-8 display form of a path.
#[must_use]
pub fn path_display(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// Lossy UTF-8 display form of a file name component.
#[must_use]
pub fn name_display(name: &OsStr) -> String {
    name.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    #[test]
    fn round_trips_utf8_and_non_utf8() {
        let utf8 = Path::new("/tmp/hello world");
        let token = encode_path(utf8);
        assert_eq!(decode_path(&token).unwrap(), utf8);

        let raw = OsString::from_vec(b"/tmp/bad\xffname".to_vec());
        let path = PathBuf::from(raw);
        let token = encode_path(&path);
        assert_eq!(decode_path(&token).unwrap(), path);
        assert!(!path_display(&path).contains('\u{FFFD}') || path_display(&path).contains('�'));
    }

    #[test]
    fn rejects_relative_decoded_bytes() {
        let token = PathToken(URL_SAFE_NO_PAD.encode(b"relative"));
        assert!(matches!(decode_path(&token), Err(AppError::Usage { .. })));
    }
}
