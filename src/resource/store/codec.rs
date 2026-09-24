//! Column and JSON conversion helpers

use std::fmt;

use rusqlite::Row;
use rusqlite::types::FromSql;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::error::ResourceStoreError;
use crate::resource::NilIdentity;

pub(super) fn sqlite_integer(value: u64) -> Result<i64, ResourceStoreError> {
    i64::try_from(value).map_err(|err| {
        ResourceStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
    })
}

pub(super) fn encode_json<T: Serialize>(value: &T) -> Result<String, ResourceStoreError> {
    serde_json::to_string(value).map_err(|err| {
        ResourceStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
    })
}

/// Failure to read one saved value
///
/// SQLite failures stay retryable, while a value that cannot decode cannot
/// succeed on retry and needs attention
#[derive(Debug)]
pub(crate) enum StoredReadError {
    /// SQLite failed while reading the value
    Storage(rusqlite::Error),
    /// The saved value cannot decode or fails its integrity check
    Corrupt {
        /// Kind of record that failed to decode
        what: &'static str,
        /// Decoder or integrity failure
        reason: String,
    },
}

impl StoredReadError {
    /// Classify one saved value that cannot be decoded or verified
    pub(crate) fn corrupt(what: &'static str, reason: impl fmt::Display) -> Self {
        Self::Corrupt {
            what,
            reason: reason.to_string(),
        }
    }
}

impl From<rusqlite::Error> for StoredReadError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error)
    }
}

/// Read one column of a saved record, classifying a type mismatch as corruption
pub(super) fn stored_column<T: FromSql>(
    row: &Row<'_>,
    index: usize,
    what: &'static str,
) -> Result<T, StoredReadError> {
    row.get(index).map_err(|error| match error {
        rusqlite::Error::InvalidColumnType(..)
        | rusqlite::Error::FromSqlConversionFailure(..)
        | rusqlite::Error::IntegralValueOutOfRange(..) => StoredReadError::corrupt(what, error),
        error => StoredReadError::Storage(error),
    })
}

/// Decode a saved JSON record, classifying a decode failure as corruption
pub(super) fn stored_json<T: DeserializeOwned>(
    what: &'static str,
    value: &str,
) -> Result<T, StoredReadError> {
    serde_json::from_str(value).map_err(|error| StoredReadError::corrupt(what, error))
}

/// Parse a saved UUID, classifying a parse failure as corruption
pub(super) fn stored_uuid(what: &'static str, value: &str) -> Result<Uuid, StoredReadError> {
    Uuid::parse_str(value).map_err(|error| StoredReadError::corrupt(what, error))
}

/// Parse a saved non-nil record identity, classifying a nil or malformed value as corruption
pub(super) fn stored_id<T>(what: &'static str, value: &str) -> Result<T, StoredReadError>
where
    T: TryFrom<Uuid, Error = NilIdentity>,
{
    T::try_from(stored_uuid(what, value)?).map_err(|error| StoredReadError::corrupt(what, error))
}

/// Convert a saved counter, classifying a negative value as corruption
pub(super) fn stored_count(what: &'static str, value: i64) -> Result<u64, StoredReadError> {
    u64::try_from(value).map_err(|error| StoredReadError::corrupt(what, error))
}

/// Collect rows whose decoder separates SQLite failures from corrupt values
pub(super) fn collect_decoded<T, E>(
    rows: impl Iterator<Item = rusqlite::Result<Result<T, E>>>,
) -> Result<Vec<T>, E>
where
    E: From<rusqlite::Error>,
{
    rows.map(|row| row?).collect()
}
