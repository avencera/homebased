//! Compare-and-swap of the resource state revision

use rusqlite::{Connection, params};

use super::codec::sqlite_integer;
use super::error::ResourceStoreError;
use crate::machine::MachineId;
use crate::resource::{ResourceId, ResourceRevision};

/// Advance the revision of an authority-owned resource only from `expected`
///
/// Returns `false` when the resource, its authority, or its revision no longer
/// match, so each caller can report the stale state in its own error type
pub(super) fn swap_resource_revision<E>(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    expected: ResourceRevision,
    next: ResourceRevision,
) -> Result<bool, E>
where
    E: From<rusqlite::Error> + From<ResourceStoreError>,
{
    let changed = conn.execute(
        "UPDATE resources SET state_revision = ?1
         WHERE id = ?2 AND authority_machine = ?3 AND state_revision = ?4",
        params![
            sqlite_integer(next.get())?,
            resource_id.as_uuid().to_string(),
            authority_machine.as_uuid().to_string(),
            sqlite_integer(expected.get())?,
        ],
    )?;
    Ok(changed == 1)
}
