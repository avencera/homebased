//! Operator attestation that a resource with no history starts with a free GPU
//!
//! Queued work serves an unregistered resource only from a saved idle
//! boundary: a closed loan, a first background launch that never spawned, or
//! an operator attestation about one ended task. A resource that was just
//! registered has none of these, so its first request waits. An operator who
//! inspected the authority GPU can record one attestation that the resource
//! starts idle. The authority accepts it only while the resource has no
//! registered task, no loan, and no first background launch, and it becomes the
//! idle boundary only while that is still true
//!
//! The attestation is a human trust decision. It is never proof that a process
//! exited or that a lock was released

use serde::{Deserialize, Serialize};

use super::operator_release::{
    OperatorAttestationId, OperatorGpuFreeConfirmation, OperatorObservation,
};
use super::{ResourceId, ResourceRevision};
use crate::machine::MachineId;

/// Immutable operator request that a resource with no history starts idle
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialIdleAttestation {
    /// Stable caller retry identity
    pub operation_id: OperatorAttestationId,
    /// Resource that starts idle
    pub resource_id: ResourceId,
    /// Authority machine that the operator inspected
    pub authority_machine: MachineId,
    /// Resource revision that the operator observed
    pub expected_state_revision: ResourceRevision,
    /// What the operator inspected and why no work holds the GPU
    pub observation: OperatorObservation,
    /// Explicit human confirmation
    pub confirmation: OperatorGpuFreeConfirmation,
}

impl InitialIdleAttestation {
    /// Check the identities that a well-formed attestation needs
    pub fn validate(&self) -> Result<(), InitialIdleRefusal> {
        if self.authority_machine.as_uuid().is_nil() {
            return Err(InitialIdleRefusal::InvalidIdentity);
        }
        if self.observation.as_str().trim().is_empty() {
            return Err(InitialIdleRefusal::EmptyObservation);
        }
        Ok(())
    }
}

/// Saved result of one initial idle attestation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialIdleReceipt {
    /// Exact attestation content
    pub attestation: InitialIdleAttestation,
    /// Resource revision committed with the attestation
    pub state_revision: ResourceRevision,
}

/// Result of one attestation call
#[derive(Debug, Clone)]
pub struct InitialIdleResolution {
    /// Saved receipt
    pub receipt: InitialIdleReceipt,
    /// Whether an earlier call committed this receipt
    pub replayed: bool,
}

/// Resource history that already decides whether the GPU is idle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceHistory {
    /// A background task is registered
    RegisteredTask,
    /// A loan exists or existed
    Loan,
    /// A first background launch exists or existed
    BackgroundLaunch,
    /// An operator attestation about an ended task exists
    OperatorAttestation,
}

/// Why the authority refused an initial idle attestation without writing any record
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InitialIdleRefusal {
    /// An identity is nil
    #[error("initial idle attestation identities must not be nil")]
    InvalidIdentity,
    /// The observation has no text
    #[error("operator observation must not be empty")]
    EmptyObservation,
    /// The resource does not exist on this authority
    #[error("resource not found")]
    ResourceNotFound,
    /// The attestation or daemon names another authority
    #[error("resource authority is {expected}, not {found}")]
    WrongAuthority {
        /// Authority saved on the resource
        expected: MachineId,
        /// Authority named by the attestation or the daemon
        found: MachineId,
    },
    /// The operation identity already names different content
    #[error(
        "initial idle attestation {} was retried with different content",
        operation_id.as_uuid()
    )]
    ConflictingRetry {
        /// Reused operation identity
        operation_id: OperatorAttestationId,
    },
    /// The resource changed after the operator read it
    #[error("stale resource revision: expected {expected:?}, found {actual:?}")]
    StaleRevision {
        /// Revision that the operator observed
        expected: ResourceRevision,
        /// Current revision
        actual: ResourceRevision,
    },
    /// The resource already has history, which decides its idle state instead
    #[error("resource already has history ({history:?}), which decides whether its GPU is idle")]
    HistoryExists {
        /// First history record found
        history: ResourceHistory,
    },
    /// Another initial idle attestation is saved for this resource
    #[error("resource already has initial idle attestation {}", operation_id.as_uuid())]
    AlreadyAttested {
        /// Saved attestation
        operation_id: OperatorAttestationId,
    },
    /// The resource revision cannot be incremented
    #[error("resource revision {revision:?} cannot be incremented")]
    RevisionExhausted {
        /// Current revision
        revision: ResourceRevision,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::InitialIdleAttestation;
    use crate::machine::MachineId;
    use crate::resource::ResourceId;

    fn document() -> serde_json::Value {
        json!({
            "operation_id": uuid::Uuid::now_v7(),
            "resource_id": ResourceId::new().as_uuid(),
            "authority_machine": MachineId::new(),
            "expected_state_revision": 0,
            "observation": "nvidia-smi shows no compute processes",
            "confirmation": "operator_confirmed_gpu_free"
        })
    }

    #[test]
    fn the_document_is_strict_and_needs_a_confirmation_and_observation() {
        let parsed: InitialIdleAttestation = serde_json::from_value(document()).unwrap();
        parsed.validate().unwrap();
        for (key, value) in [
            ("confirmation", json!("yes")),
            ("observation", json!("   ")),
            ("task_id", json!(uuid::Uuid::now_v7())),
        ] {
            let mut changed = document();
            changed[key] = value;
            assert!(
                serde_json::from_value::<InitialIdleAttestation>(changed).is_err(),
                "{key}"
            );
        }
        let mut missing = document();
        missing.as_object_mut().unwrap().remove("confirmation");
        assert!(serde_json::from_value::<InitialIdleAttestation>(missing).is_err());
    }
}
