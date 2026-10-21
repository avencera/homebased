//! Remote tasks that wait for an offline execution machine
//!
//! A remote submit with `wait_for_machine` does not fail while its machine is
//! unreachable. The origin saves it as a held task, and the loop that releases
//! `after` tasks sends it once the machine answers. A `coalesce` key names work
//! where only the newest request matters, such as an update: an accepted
//! submit cancels every older task with the same key and machine that still
//! waits on its origin

use std::fmt;

use serde::{Deserialize, Serialize};

/// Longest `coalesce` key
pub const MAX_COALESCE_KEY_LEN: usize = 64;

/// Validated `coalesce` key: 1 to [`MAX_COALESCE_KEY_LEN`] ASCII letters,
/// digits, `.`, `_`, `-`, or `:`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CoalesceKey(String);

/// Why a submitted `coalesce` key is invalid
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CoalesceKeyError {
    /// The key is empty or longer than [`MAX_COALESCE_KEY_LEN`]
    #[error("coalesce must be 1 to {MAX_COALESCE_KEY_LEN} characters")]
    Length,
    /// The key has a character outside the allowed set
    #[error("coalesce may use only ASCII letters, digits, '.', '_', '-', and ':'")]
    Character,
}

impl CoalesceKey {
    /// Validate a submitted key
    ///
    /// # Errors
    ///
    /// Refuses an empty or overlong key and any character outside the allowed set
    pub fn new(value: String) -> Result<Self, CoalesceKeyError> {
        if value.is_empty() || value.len() > MAX_COALESCE_KEY_LEN {
            return Err(CoalesceKeyError::Length);
        }
        let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':');
        if !value.chars().all(allowed) {
            return Err(CoalesceKeyError::Character);
        }
        Ok(Self(value))
    }

    /// Key text
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for CoalesceKey {
    type Error = CoalesceKeyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CoalesceKey> for String {
    fn from(key: CoalesceKey) -> Self {
        key.0
    }
}

impl fmt::Display for CoalesceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A remote submit that waits on its origin while its machine is unreachable
///
/// Origin-only, like `after`: it travels beside the normalized spec, so no
/// executor receives it
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MachineWait {
    /// Older waiting tasks with this key and machine are cancelled once this one is accepted
    pub coalesce: Option<CoalesceKey>,
}

#[cfg(test)]
mod tests {
    use super::{CoalesceKey, CoalesceKeyError, MAX_COALESCE_KEY_LEN};

    #[test]
    fn key_refuses_empty_overlong_and_unsafe_text() {
        assert_eq!(
            CoalesceKey::new(String::new()),
            Err(CoalesceKeyError::Length)
        );
        assert_eq!(
            CoalesceKey::new("a".repeat(MAX_COALESCE_KEY_LEN + 1)),
            Err(CoalesceKeyError::Length)
        );
        assert_eq!(
            CoalesceKey::new("fleet update".into()),
            Err(CoalesceKeyError::Character)
        );
        assert_eq!(
            CoalesceKey::new("fleet-update:mba_1.0".into())
                .unwrap()
                .as_str(),
            "fleet-update:mba_1.0"
        );
    }
}
