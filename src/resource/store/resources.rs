//! Resource registration and authority-wide resource reads

use rusqlite::{Connection, Row, TransactionBehavior, params};

use super::codec::{collect_decoded, encode_json, sqlite_integer, stored_column, stored_json};
use super::error::{ConflictReason, ResourceStoreError};
use super::rows::{check_authority, decode_loan_at, decode_resource, select_resource};
use crate::machine::MachineId;
use crate::resource::{Loan, LoanState, Resource, ResourceId, ResourceRegistrationReceipt};

/// One authority-owned resource and the loan that must be restored with it
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResourceSnapshot {
    /// Resource whose authority matches the loading daemon
    pub(crate) resource: Resource,
    /// The resource's current non-closed loan, if one exists
    pub(crate) loan: Option<Loan>,
}

const RESOURCE_SNAPSHOT_COLUMNS: &str = "r.id, r.display_name, r.authority_machine,
    r.supervisor_machine, r.supervisor_thread, r.assignment_revision, r.state_revision,
    r.registered_background_task, l.id, l.resource_id, l.state_json";

/// Register one resource on its declared authority without changing fixed content
pub(crate) fn register_resource_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource: &Resource,
) -> Result<Resource, ResourceStoreError> {
    check_authority(resource.authority_machine(), authority_machine)?;

    let assignment_revision = sqlite_integer(resource.assignment_revision.get())?;
    let state_revision = sqlite_integer(resource.state_revision.get())?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    let receipt = ResourceRegistrationReceipt::initial(resource);
    if let Some(saved) = select_resource(&tx, resource.id)? {
        // an exact retry matches the first registration, even after a supervisor replacement
        if select_resource_registration(&tx, resource.id)? != receipt {
            return Err(ResourceStoreError::RegistrationConflict {
                resource: resource.id,
            });
        }

        tx.commit()?;
        return Ok(saved);
    }

    tx.execute(
        "INSERT INTO resources (
            id, display_name, authority_machine, supervisor_machine, supervisor_thread,
            assignment_revision, state_revision, registered_background_task
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            resource.id.as_uuid().to_string(),
            resource.display_name,
            resource.authority_machine().as_uuid().to_string(),
            resource.supervisor.machine.as_uuid().to_string(),
            resource.supervisor.thread.to_string(),
            assignment_revision,
            state_revision,
            resource
                .registered_background_task
                .map(|task| task.to_string()),
        ],
    )?;
    insert_resource_registration(&tx, &receipt)?;
    tx.commit()?;
    Ok(resource.clone())
}

fn insert_resource_registration(
    conn: &Connection,
    receipt: &ResourceRegistrationReceipt,
) -> Result<(), ResourceStoreError> {
    let json = encode_json(receipt)?;
    conn.execute(
        "INSERT INTO resource_registration_receipts (resource_id, receipt_json)
         VALUES (?1, ?2)",
        params![receipt.resource_id.as_uuid().to_string(), json],
    )?;
    Ok(())
}

/// Read the first registration saved for one existing resource
///
/// Registration saves the resource row and this receipt in one transaction, so
/// a missing receipt is a storage error
fn select_resource_registration(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<ResourceRegistrationReceipt, ResourceStoreError> {
    let receipt: ResourceRegistrationReceipt = stored_json(
        "resource registration receipt",
        &conn.query_row(
            "SELECT receipt_json FROM resource_registration_receipts WHERE resource_id = ?1",
            [resource_id.as_uuid().to_string()],
            |row| row.get::<_, String>(0),
        )?,
    )?;
    if receipt.resource_id != resource_id {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RegistrationReceiptMismatch,
        ));
    }
    Ok(receipt)
}

/// Load each resource owned by one authority with its current non-closed loan
pub(crate) fn resources_for_authority(
    conn: &Connection,
    authority_machine: MachineId,
) -> Result<Vec<ResourceSnapshot>, ResourceStoreError> {
    let sql = format!(
        "SELECT {RESOURCE_SNAPSHOT_COLUMNS}
         FROM resources AS r
         LEFT JOIN loans AS l
           ON l.resource_id = r.id
         WHERE r.authority_machine = ?1
         ORDER BY r.id, l.id"
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map([authority_machine.to_string()], |row| {
        Ok(decode_resource_snapshot(row))
    })?;
    let mut snapshots: Vec<ResourceSnapshot> = Vec::new();

    for mut candidate in collect_decoded(rows)? {
        match snapshots.last_mut() {
            Some(snapshot) if snapshot.resource.id == candidate.resource.id => {
                let Some(loan) = candidate.loan.take() else {
                    continue;
                };
                if matches!(&loan.state, LoanState::Closed { .. }) {
                    continue;
                }
                if snapshot.loan.is_some() {
                    return Err(ResourceStoreError::corrupt(
                        "resource loans",
                        format!(
                            "resource {:?} has multiple non-closed loans",
                            snapshot.resource.id
                        ),
                    ));
                }
                snapshot.loan = Some(loan);
            }
            _ => {
                if candidate
                    .loan
                    .as_ref()
                    .is_some_and(|loan| matches!(&loan.state, LoanState::Closed { .. }))
                {
                    candidate.loan = None;
                }
                snapshots.push(candidate);
            }
        }
    }

    Ok(snapshots)
}

fn decode_resource_snapshot(row: &Row<'_>) -> Result<ResourceSnapshot, ResourceStoreError> {
    let resource = decode_resource(row)?;
    // the left join leaves every loan column null for a resource with no loans
    let loan = stored_column::<Option<String>>(row, 8, "loan identity")?
        .map(|_| decode_loan_at(row, 8))
        .transpose()?;

    Ok(ResourceSnapshot { resource, loan })
}
