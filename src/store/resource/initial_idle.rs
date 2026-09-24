//! Authority-owned initial idle attestation for a resource with no history
//!
//! One IMMEDIATE transaction checks the attestation against the current
//! resource revision and every record that could hold or release the GPU: a
//! registered task, a loan, a first background launch, and an operator
//! attestation about an ended task. With none of them, it saves one receipt and
//! advances the revision. Queued work then serves through the normal idle
//! reconciliation, which reads the receipt as the idle boundary only while the
//! resource still has no loan and no first background launch. A refusal writes
//! nothing, and an exact retry returns the saved receipt

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::background::{
    AdvanceError, advance_resource_on, latest_launch_request_on, latest_loan_on,
};
use crate::machine::MachineId;
use crate::resource::initial_idle::{
    InitialIdleAttestation, InitialIdleReceipt, InitialIdleRefusal, InitialIdleResolution,
    ResourceHistory,
};
use crate::resource::operator_release::OperatorAttestationId;
use crate::resource::store::{ResourceStoreError, select_resource};
use crate::resource::{Resource, ResourceId};
use crate::store::Store;

/// Why an initial idle attestation was refused or could not be evaluated
#[derive(Debug, thiserror::Error)]
pub(crate) enum InitialIdleError {
    /// The authority refused the attestation and wrote nothing
    #[error(transparent)]
    Refused(#[from] InitialIdleRefusal),
    /// Resource storage failed or stored data is invalid
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    /// A receipt could not be encoded or decoded
    #[error("initial idle attestation encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    /// A concurrent change invalidated the revision compare-and-set
    #[error("resource state changed during the initial idle attestation")]
    Changed,
    /// SQLite failed
    #[error("initial idle attestation storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

impl Store {
    /// Save one initial idle attestation for a resource with no history
    ///
    /// `authority_machine` is the local daemon machine. The attestation must
    /// name it, and it must be the resource authority
    pub(crate) fn attest_initial_idle_for_authority(
        &mut self,
        authority_machine: MachineId,
        attestation: InitialIdleAttestation,
    ) -> Result<InitialIdleResolution, InitialIdleError> {
        attest_initial_idle(&mut self.conn, authority_machine, attestation)
    }
}

fn attest_initial_idle(
    conn: &mut Connection,
    authority_machine: MachineId,
    attestation: InitialIdleAttestation,
) -> Result<InitialIdleResolution, InitialIdleError> {
    attestation.validate()?;
    if attestation.authority_machine != authority_machine {
        return Err(InitialIdleRefusal::WrongAuthority {
            expected: authority_machine,
            found: attestation.authority_machine,
        }
        .into());
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(receipt) = receipt_by_operation_on(&tx, attestation.operation_id)? {
        if receipt.attestation != attestation {
            return Err(InitialIdleRefusal::ConflictingRetry {
                operation_id: attestation.operation_id,
            }
            .into());
        }
        tx.commit()?;
        return Ok(InitialIdleResolution {
            receipt,
            replayed: true,
        });
    }

    let resource = select_resource(&tx, attestation.resource_id)?
        .ok_or(InitialIdleRefusal::ResourceNotFound)?;
    if resource.authority_machine() != attestation.authority_machine {
        return Err(InitialIdleRefusal::WrongAuthority {
            expected: resource.authority_machine(),
            found: attestation.authority_machine,
        }
        .into());
    }
    if let Some(saved) = receipt_by_resource_on(&tx, resource.id)? {
        return Err(InitialIdleRefusal::AlreadyAttested {
            operation_id: saved.attestation.operation_id,
        }
        .into());
    }
    if resource.state_revision != attestation.expected_state_revision {
        return Err(InitialIdleRefusal::StaleRevision {
            expected: attestation.expected_state_revision,
            actual: resource.state_revision,
        }
        .into());
    }
    if let Some(history) = resource_history_on(&tx, &resource)? {
        return Err(InitialIdleRefusal::HistoryExists { history }.into());
    }

    let state_revision =
        advance_resource_on(&tx, &resource, None).map_err(|error| match error {
            AdvanceError::Exhausted => {
                InitialIdleError::Refused(InitialIdleRefusal::RevisionExhausted {
                    revision: resource.state_revision,
                })
            }
            AdvanceError::Changed => InitialIdleError::Changed,
            AdvanceError::Storage(error) => InitialIdleError::Storage(error),
        })?;
    let receipt = InitialIdleReceipt {
        attestation,
        state_revision,
    };
    tx.execute(
        "INSERT INTO resource_initial_idle_attestations (operation_id, resource_id, receipt_json)
         VALUES (?1, ?2, ?3)",
        params![
            receipt.attestation.operation_id.as_uuid().to_string(),
            resource.id.as_uuid().to_string(),
            serde_json::to_string(&receipt)?,
        ],
    )?;
    tx.commit()?;

    Ok(InitialIdleResolution {
        receipt,
        replayed: false,
    })
}

/// First record that already decides whether the resource's GPU is idle
fn resource_history_on(
    conn: &Connection,
    resource: &Resource,
) -> Result<Option<ResourceHistory>, ResourceStoreError> {
    if resource.registered_background_task.is_some() {
        return Ok(Some(ResourceHistory::RegisteredTask));
    }
    if latest_loan_on(conn, resource.id)?.is_some() {
        return Ok(Some(ResourceHistory::Loan));
    }
    if latest_launch_request_on(conn, resource.id)?.is_some() {
        return Ok(Some(ResourceHistory::BackgroundLaunch));
    }
    let attested: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM resource_operator_attestations WHERE resource_id = ?1)",
        [resource.id.as_uuid().to_string()],
        |row| row.get(0),
    )?;
    Ok(attested.then_some(ResourceHistory::OperatorAttestation))
}

/// Initial idle attestation that is the current idle boundary of a resource
///
/// The receipt counts only while the resource still has no registered task,
/// loan, or first background launch; every later record decides instead
pub(crate) fn initial_idle_boundary_on(
    conn: &Connection,
    resource: &Resource,
) -> Result<Option<OperatorAttestationId>, ResourceStoreError> {
    let Some(receipt) = receipt_by_resource_on(conn, resource.id)? else {
        return Ok(None);
    };
    if receipt.attestation.resource_id != resource.id
        || receipt.attestation.authority_machine != resource.authority_machine()
        || resource_history_on(conn, resource)?.is_some()
    {
        return Ok(None);
    }
    Ok(Some(receipt.attestation.operation_id))
}

fn receipt_by_operation_on(
    conn: &Connection,
    operation_id: OperatorAttestationId,
) -> Result<Option<InitialIdleReceipt>, ResourceStoreError> {
    receipt_where(conn, "operation_id", &operation_id.as_uuid().to_string())
}

fn receipt_by_resource_on(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<InitialIdleReceipt>, ResourceStoreError> {
    receipt_where(conn, "resource_id", &resource_id.as_uuid().to_string())
}

fn receipt_where(
    conn: &Connection,
    column: &str,
    value: &str,
) -> Result<Option<InitialIdleReceipt>, ResourceStoreError> {
    let saved: Option<String> = conn
        .query_row(
            &format!(
                "SELECT receipt_json FROM resource_initial_idle_attestations WHERE {column} = ?1"
            ),
            [value],
            |row| row.get(0),
        )
        .optional()?;
    saved
        .map(|json| {
            serde_json::from_str(&json).map_err(|error| {
                ResourceStoreError::corrupt("initial idle attestation receipt", error)
            })
        })
        .transpose()
}
