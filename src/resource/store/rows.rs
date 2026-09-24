//! Shared row reads and authority checks
//!
//! Decoders return `ResourceStoreError` so a saved value that cannot decode
//! surfaces as `CorruptRecord` for attention instead of a retryable SQLite error

use rusqlite::{Connection, OptionalExtension, Row};

use super::codec::{stored_column, stored_count, stored_id, stored_json, stored_uuid};
use super::error::ResourceStoreError;
use crate::domain::{TaskId, ThreadId};
use crate::machine::MachineId;
use crate::resource::{
    AcceptanceSequence, AssignmentRevision, Loan, LoanId, LoanState, Resource, ResourceId,
    ResourceRequest, ResourceRequestState, ResourceRevision, SupervisorAddress,
};
use crate::spec::NormalizedSpec;
use crate::submission::RequestId;

const RESOURCE_COLUMNS: &str = "id, display_name, authority_machine, supervisor_machine,
    supervisor_thread, assignment_revision, state_revision, registered_background_task";

pub(super) const REQUEST_COLUMNS: &str = "acceptance_sequence, request_id, task_id, resource_id,
    origin_machine, spec_json, state_json";

pub(crate) fn select_resource(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<Resource>, ResourceStoreError> {
    let sql = format!("SELECT {RESOURCE_COLUMNS} FROM resources WHERE id = ?1");
    conn.query_row(&sql, [resource_id.as_uuid().to_string()], |row| {
        Ok(decode_resource(row))
    })
    .optional()?
    .transpose()
}

pub(crate) fn select_non_closed_loan(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<Loan>, ResourceStoreError> {
    conn.query_row(
        "SELECT id, resource_id, state_json FROM loans
         WHERE resource_id = ?1 AND json_extract(state_json, '$.type') != 'closed'",
        [resource_id.as_uuid().to_string()],
        |row| Ok(decode_loan(row)),
    )
    .optional()?
    .transpose()
}

/// Decode a loan from `id, resource_id, state_json` columns starting at `first`
pub(super) fn decode_loan_at(row: &Row<'_>, first: usize) -> Result<Loan, ResourceStoreError> {
    let id: LoanId = stored_id(
        "loan identity",
        &stored_column::<String>(row, first, "loan identity")?,
    )?;
    let resource_id: ResourceId = stored_id(
        "loan resource identity",
        &stored_column::<String>(row, first + 1, "loan resource identity")?,
    )?;
    let state: LoanState = stored_json(
        "loan state",
        &stored_column::<String>(row, first + 2, "loan state")?,
    )?;
    Ok(Loan {
        id,
        resource_id,
        state,
    })
}

pub(super) fn decode_loan(row: &Row<'_>) -> Result<Loan, ResourceStoreError> {
    decode_loan_at(row, 0)
}

pub(crate) fn select_request_by_id(
    conn: &Connection,
    request_id: RequestId,
) -> Result<Option<ResourceRequest>, ResourceStoreError> {
    let sql = format!("SELECT {REQUEST_COLUMNS} FROM resource_requests WHERE request_id = ?1");
    conn.query_row(&sql, [request_id.0.to_string()], |row| {
        Ok(decode_request(row))
    })
    .optional()?
    .transpose()
}

/// Require an existing resource whose fixed authority is `authority_machine`
pub(super) fn check_resource_authority(
    conn: &Connection,
    resource_id: ResourceId,
    authority_machine: MachineId,
) -> Result<(), ResourceStoreError> {
    check_authority(resource_authority(conn, resource_id)?, authority_machine)
}

fn resource_authority(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<MachineId, ResourceStoreError> {
    select_resource(conn, resource_id)?
        .map(|resource| resource.authority_machine())
        .ok_or(ResourceStoreError::ResourceNotFound)
}

pub(super) fn check_authority(
    expected: MachineId,
    found: MachineId,
) -> Result<(), ResourceStoreError> {
    if expected == found {
        return Ok(());
    }

    Err(ResourceStoreError::WrongAuthority { expected, found })
}

pub(super) fn decode_resource(row: &Row<'_>) -> Result<Resource, ResourceStoreError> {
    let text = |index, what| stored_column::<String>(row, index, what);
    let id: ResourceId = stored_id("resource identity", &text(0, "resource identity")?)?;
    let display_name = stored_column(row, 1, "resource display name")?;
    let authority_machine = MachineId::from_uuid(stored_uuid(
        "resource authority machine",
        &text(2, "resource authority machine")?,
    )?);
    let supervisor_machine = MachineId::from_uuid(stored_uuid(
        "resource supervisor machine",
        &text(3, "resource supervisor machine")?,
    )?);
    let supervisor_thread = ThreadId(stored_uuid(
        "resource supervisor thread",
        &text(4, "resource supervisor thread")?,
    )?);
    let assignment_revision = AssignmentRevision::new(stored_count(
        "resource assignment revision",
        stored_column(row, 5, "resource assignment revision")?,
    )?);
    let state_revision = ResourceRevision::new(stored_count(
        "resource state revision",
        stored_column(row, 6, "resource state revision")?,
    )?);
    let registered_background_task =
        stored_column::<Option<String>>(row, 7, "resource registered background task")?
            .map(|value| stored_uuid("resource registered background task", &value).map(TaskId))
            .transpose()?;

    Ok(Resource::new(
        id,
        display_name,
        authority_machine,
        SupervisorAddress {
            machine: supervisor_machine,
            thread: supervisor_thread,
        },
        assignment_revision,
        state_revision,
        registered_background_task,
    ))
}

pub(super) fn decode_request(row: &Row<'_>) -> Result<ResourceRequest, ResourceStoreError> {
    let text = |index, what| stored_column::<String>(row, index, what);
    let sequence = AcceptanceSequence::new(stored_count(
        "request acceptance sequence",
        stored_column(row, 0, "request acceptance sequence")?,
    )?);
    let request_id = RequestId(stored_uuid(
        "request identity",
        &text(1, "request identity")?,
    )?);
    let task_id = TaskId(stored_uuid(
        "request task identity",
        &text(2, "request task identity")?,
    )?);
    let resource_id: ResourceId = stored_id(
        "request resource identity",
        &text(3, "request resource identity")?,
    )?;
    let origin_machine = MachineId::from_uuid(stored_uuid(
        "request origin machine",
        &text(4, "request origin machine")?,
    )?);
    let normalized_spec: NormalizedSpec =
        stored_json("resource request spec", &text(5, "resource request spec")?)?;
    let state: ResourceRequestState = stored_json(
        "resource request state",
        &text(6, "resource request state")?,
    )?;

    let mut request = ResourceRequest::new(
        request_id,
        task_id,
        resource_id,
        sequence,
        origin_machine,
        normalized_spec,
    )
    .map_err(|error| ResourceStoreError::corrupt("resource request spec", error))?;
    request.state = state;
    Ok(request)
}
