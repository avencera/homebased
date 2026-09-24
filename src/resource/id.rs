//! UUID identities of resource records that are never nil
//!
//! Every identity is allocated as a UUIDv7 or decoded from a saved or received
//! value. Construction and decoding both refuse the nil UUID, so code that holds
//! one of these identities never has to check for nil again

use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

/// The nil UUID was offered as a resource record identity
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("resource identities must not be the nil UUID")]
pub struct NilIdentity;

/// Text that does not name a non-nil resource record identity
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityParseError {
    /// The text is not a UUID
    #[error("invalid resource identity: {0}")]
    Uuid(#[from] uuid::Error),
    /// The text is the nil UUID
    #[error(transparent)]
    Nil(#[from] NilIdentity),
}

macro_rules! resource_uuid_id {
    ($(#[$meta:meta])* pub struct $name:ident;) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Allocate a new time-ordered identity
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wrap an existing UUID, refusing the nil UUID
            pub const fn from_uuid(uuid: Uuid) -> Result<Self, NilIdentity> {
                if uuid.is_nil() {
                    return Err(NilIdentity);
                }
                Ok(Self(uuid))
            }

            /// Return the underlying UUID value
            #[must_use]
            pub const fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl TryFrom<Uuid> for $name {
            type Error = NilIdentity;

            fn try_from(uuid: Uuid) -> Result<Self, Self::Error> {
                Self::from_uuid(uuid)
            }
        }

        impl FromStr for $name {
            type Err = IdentityParseError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Ok(Self::from_uuid(Uuid::parse_str(text)?)?)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::from_uuid(Uuid::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

resource_uuid_id! {
    /// Stable identity of one physical GPU resource
    pub struct ResourceId;
}

resource_uuid_id! {
    /// Stable identity of one interruption loan
    pub struct LoanId;
}

resource_uuid_id! {
    /// Stable identity of one required supervisor decision
    pub struct ActionId;
}

resource_uuid_id! {
    /// Stable identity of one durable supervisor notice
    pub struct NoticeId;
}

resource_uuid_id! {
    /// Stable identity of one notice delivery attempt
    pub struct DeliveryAttemptId;
}

resource_uuid_id! {
    /// Stable identity of one reserved exact-task stop request
    pub struct ReleaseStopReservationId;
}

resource_uuid_id! {
    /// Stable caller identity of one operator attestation
    ///
    /// The caller allocates it before the first send. An exact retry returns the
    /// saved receipt, and different content under the same identity conflicts
    pub struct OperatorAttestationId;
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{NilIdentity, ResourceId};

    #[test]
    fn nil_identity_is_refused_by_construction_and_decoding() {
        assert_eq!(ResourceId::from_uuid(Uuid::nil()), Err(NilIdentity));
        assert!(serde_json::from_value::<ResourceId>(json!(Uuid::nil())).is_err());
    }

    #[test]
    fn a_saved_identity_decodes_to_the_same_uuid_text() {
        let text = "019b4f42-0000-7000-8000-000000000061";
        let id = serde_json::from_value::<ResourceId>(json!(text)).unwrap();

        assert_eq!(id.as_uuid(), Uuid::parse_str(text).unwrap());
        assert_eq!(serde_json::to_value(id).unwrap(), json!(text));
    }
}
