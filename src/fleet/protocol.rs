//! Cluster protocol versions and compatibility.
//!
//! The cluster protocol version is separate from the public JSON
//! `api_version` and from the event payload version. Each machine advertises
//! the inclusive range it accepts; two machines interoperate when the ranges
//! overlap, and they speak the highest shared version.

use std::fmt;

use serde::{Deserialize, Serialize};

/// One cluster protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClusterProtocolVersion(pub u32);

impl fmt::Display for ClusterProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Version this build speaks by default.
pub const CLUSTER_PROTOCOL_VERSION: ClusterProtocolVersion = ClusterProtocolVersion(2);

/// Range this build accepts. Version 1 keeps the old cancellation wire shape
/// available during rolling updates
pub const SUPPORTED_PROTOCOLS: ProtocolRange = ProtocolRange {
    min: ClusterProtocolVersion(1),
    max: CLUSTER_PROTOCOL_VERSION,
};

/// Inclusive range of accepted cluster protocol versions. `min <= max` holds
/// for every value built by [`ProtocolRange::new`] or decoded from JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct ProtocolRange {
    min: ClusterProtocolVersion,
    max: ClusterProtocolVersion,
}

impl ProtocolRange {
    /// Build a range. `None` when `min > max`.
    #[must_use]
    pub fn new(min: ClusterProtocolVersion, max: ClusterProtocolVersion) -> Option<Self> {
        (min <= max).then_some(Self { min, max })
    }

    /// Lowest accepted version.
    #[must_use]
    pub fn min(&self) -> ClusterProtocolVersion {
        self.min
    }

    /// Highest accepted version.
    #[must_use]
    pub fn max(&self) -> ClusterProtocolVersion {
        self.max
    }

    /// Highest version both ranges accept.
    #[must_use]
    pub fn negotiate(&self, remote: &ProtocolRange) -> Compatibility {
        let high = self.max.min(remote.max);
        let low = self.min.max(remote.min);
        if low <= high {
            Compatibility::Compatible { version: high }
        } else {
            Compatibility::Incompatible {
                local: *self,
                remote: *remote,
            }
        }
    }
}

impl fmt::Display for ProtocolRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.min == self.max {
            return write!(f, "{}", self.min);
        }
        write!(f, "{}-{}", self.min, self.max)
    }
}

impl<'de> Deserialize<'de> for ProtocolRange {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            min: ClusterProtocolVersion,
            max: ClusterProtocolVersion,
        }
        let raw = Raw::deserialize(deserializer)?;
        Self::new(raw.min, raw.max)
            .ok_or_else(|| serde::de::Error::custom("protocol range min must not exceed max"))
    }
}

/// Result of comparing two protocol ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Compatibility {
    /// The machines share at least one version.
    Compatible {
        /// Highest shared version; use it for requests.
        version: ClusterProtocolVersion,
    },
    /// No shared version. The machine stays visible, but remote operations
    /// return a typed compatibility error.
    Incompatible {
        /// Local accepted range.
        local: ProtocolRange,
        /// Remote accepted range.
        remote: ProtocolRange,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(min: u32, max: u32) -> ProtocolRange {
        ProtocolRange::new(ClusterProtocolVersion(min), ClusterProtocolVersion(max)).unwrap()
    }

    #[test]
    fn negotiates_highest_shared_version() {
        assert_eq!(
            range(1, 2).negotiate(&range(2, 3)),
            Compatibility::Compatible {
                version: ClusterProtocolVersion(2)
            }
        );
        assert_eq!(
            range(1, 1).negotiate(&range(1, 1)),
            Compatibility::Compatible {
                version: ClusterProtocolVersion(1)
            }
        );
    }

    #[test]
    fn disjoint_ranges_are_incompatible() {
        assert!(matches!(
            range(1, 1).negotiate(&range(2, 3)),
            Compatibility::Incompatible { .. }
        ));
    }

    #[test]
    fn inverted_range_is_rejected() {
        assert!(ProtocolRange::new(ClusterProtocolVersion(2), ClusterProtocolVersion(1)).is_none());
        let err = serde_json::from_str::<ProtocolRange>(r#"{"min":2,"max":1}"#).unwrap_err();
        assert!(err.to_string().contains("min must not exceed max"), "{err}");
    }
}
