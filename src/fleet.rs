//! Homebased Fleet foundation: machine addresses, the cluster protocol
//! version, machine advertisements, the durable peer directory, identity
//! conflict detection, discovery providers, and destination checks.
//!
//! Entry points:
//!
//! - [`runtime::FleetRuntime::start`] starts discovery in the background when
//!   the fleet is enabled.
//! - [`runtime::FleetHandle::connect`] resolves a machine UUID to an address
//!   whose probe confirms that identity.
//! - [`crate::machine::LocalIdentity::check_destination`] is the receiver-side
//!   check for every cluster request that carries a destination UUID.

pub mod address;
pub mod advertisement;
pub mod directory;
pub mod discovery;
pub mod http;
pub mod identity;
pub mod probe;
pub mod protocol;
pub mod runtime;

use crate::fleet::runtime::FleetHandle;

/// Fleet support in a running daemon.
#[derive(Clone, Default)]
pub enum FleetState {
    /// `fleet.enabled` is false. Local tasks work; cluster routes are absent.
    #[default]
    Disabled,
    /// Discovery and cluster routes are on.
    Enabled(FleetHandle),
}

impl FleetState {
    /// Handle when enabled.
    #[must_use]
    pub fn handle(&self) -> Option<&FleetHandle> {
        match self {
            Self::Enabled(handle) => Some(handle),
            Self::Disabled => None,
        }
    }
}
