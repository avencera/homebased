//! SHA-256 digests with one lowercase hexadecimal text form

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

/// Number of bytes in one SHA-256 digest
const SHA256_LEN: usize = 32;

/// Raw SHA-256 digest
///
/// Its text, JSON, and database form is exactly 64 lowercase hexadecimal
/// characters. Decoding refuses any other text, so a saved or received digest
/// is always in canonical form and two equal digests always have equal text
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha256Digest([u8; SHA256_LEN]);

/// Text that is not 64 lowercase hexadecimal characters
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("SHA-256 digest must be 64 lowercase hexadecimal characters")]
pub struct InvalidSha256Hex;

impl Sha256Digest {
    /// Hash the exact bytes
    #[must_use]
    pub fn of(bytes: impl AsRef<[u8]>) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    /// Wrap a finished 32-byte digest
    #[must_use]
    pub const fn from_bytes(bytes: [u8; SHA256_LEN]) -> Self {
        Self(bytes)
    }

    /// Borrow the raw 32-byte digest
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; SHA256_LEN] {
        &self.0
    }

    /// Return the digest as 64 lowercase hexadecimal characters
    #[must_use]
    pub fn to_hex(self) -> String {
        self.to_string()
    }

    /// Parse exactly 64 lowercase hexadecimal characters
    pub fn from_hex(text: &str) -> Result<Self, InvalidSha256Hex> {
        let (pairs, remainder) = text.as_bytes().as_chunks::<2>();
        if pairs.len() != SHA256_LEN || !remainder.is_empty() {
            return Err(InvalidSha256Hex);
        }

        let mut digest = [0_u8; SHA256_LEN];
        for (byte, [high, low]) in digest.iter_mut().zip(pairs) {
            *byte = (hex_value(*high)? << 4) | hex_value(*low)?;
        }
        Ok(Self(digest))
    }
}

impl From<[u8; SHA256_LEN]> for Sha256Digest {
    fn from(bytes: [u8; SHA256_LEN]) -> Self {
        Self(bytes)
    }
}

impl From<Sha256> for Sha256Digest {
    fn from(hasher: Sha256) -> Self {
        Self(hasher.finalize().into())
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        for byte in self.0 {
            let high = char::from(HEX[usize::from(byte >> 4)]);
            let low = char::from(HEX[usize::from(byte & 0x0f)]);
            write!(formatter, "{high}{low}")?;
        }
        Ok(())
    }
}

// debug output shows the same hex text as logs and JSON instead of a byte array
impl fmt::Debug for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Sha256Digest({self})")
    }
}

impl FromStr for Sha256Digest {
    type Err = InvalidSha256Hex;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::from_hex(text)
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // a borrowed str is not always available, for example from a `Value`
        let text = String::deserialize(deserializer)?;
        Self::from_hex(&text).map_err(serde::de::Error::custom)
    }
}

fn hex_value(digit: u8) -> Result<u8, InvalidSha256Hex> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        _ => Err(InvalidSha256Hex),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{InvalidSha256Hex, Sha256Digest};

    #[test]
    fn hex_text_round_trips_through_json() {
        let digest = Sha256Digest::of(b"abc");
        let text = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

        assert_eq!(digest.to_hex(), text);
        assert_eq!(serde_json::to_value(digest).unwrap(), json!(text));
        assert_eq!(
            serde_json::from_value::<Sha256Digest>(json!(text)).unwrap(),
            digest
        );
    }

    #[test]
    fn decoding_refuses_noncanonical_text() {
        let upper = "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD";
        for text in [
            upper,
            "",
            "ab",
            &"a".repeat(63),
            &"a".repeat(65),
            &"g".repeat(64),
            &format!("{}é", "a".repeat(62)),
        ] {
            assert_eq!(
                Sha256Digest::from_hex(text),
                Err(InvalidSha256Hex),
                "{text}"
            );
            assert!(serde_json::from_value::<Sha256Digest>(json!(text)).is_err());
        }
    }
}
