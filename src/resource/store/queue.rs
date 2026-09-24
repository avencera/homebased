//! Authority queue acceptance, serving order, and reads of resource requests

use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};

use super::assigned_task::resource_task_ownership_risk;
use super::codec::{
    collect_decoded, encode_json, stored_column, stored_count, stored_id, stored_json, stored_uuid,
};
use super::error::{ConflictReason, ResourceStoreError};
use super::rows::{
    REQUEST_COLUMNS, check_resource_authority, decode_request, select_request_by_id,
};
use crate::domain::TaskId;
use crate::machine::MachineId;
use crate::resource::{
    AcceptanceSequence, CommandSpec, ResourceId, ResourceRequest, ResourceRequestState,
};
use crate::spec::NormalizedSpec;
use crate::submission::{ExecutorIdentity, RequestId};

const PREVENTION_COLUMNS: &str = "request_id, task_id, resource_id, origin_machine";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RequestIdentity {
    pub(super) request_id: RequestId,
    pub(super) task_id: TaskId,
    pub(super) resource_id: ResourceId,
    pub(super) origin_machine: MachineId,
}

/// Accept one command request on the resource's fixed authority
///
/// The origin route is stored by the caller before it sends this queue request
pub(crate) fn accept_request_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
    normalized_spec: NormalizedSpec,
) -> Result<ResourceRequest, ResourceStoreError> {
    let identity = RequestIdentity {
        request_id,
        task_id,
        resource_id,
        origin_machine,
    };
    {
        // an exact retry is answered from the saved request before the command
        // checks, so a file that changed after acceptance cannot reject it
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        if let Some(saved) = replay_request_on(&tx, authority_machine, identity, &normalized_spec)?
        {
            tx.commit()?;
            return Ok(saved);
        }
    }
    // the entry-point check reads the file system outside the IMMEDIATE write transaction
    check_queued_command_ownership(&normalized_spec)?;

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(saved) = replay_request_on(&tx, authority_machine, identity, &normalized_spec)? {
        tx.commit()?;
        return Ok(saved);
    }

    if task_id_exists(&tx, task_id)? {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::TaskIdentityInUse,
        ));
    }

    if prevention_exists(&tx, identity)? {
        return Err(ResourceStoreError::Prevented);
    }
    if executor_identity_exists(&tx, task_id)? {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::TaskIdentityInUse,
        ));
    }

    let command_spec = CommandSpec::try_from(normalized_spec)?;
    check_resource_authority(&tx, resource_id, authority_machine)?;
    let spec_json = encode_json(command_spec.as_normalized())?;
    let state_json = encode_json(&ResourceRequestState::Queued)?;

    tx.execute(
        "INSERT INTO resource_requests (
            request_id, task_id, resource_id, origin_machine, spec_json, state_json, queue_rank
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            request_id.0.to_string(),
            task_id.to_string(),
            resource_id.as_uuid().to_string(),
            origin_machine.as_uuid().to_string(),
            spec_json,
            state_json,
            next_queue_rank(&tx, resource_id)?,
        ],
    )?;
    let raw_sequence = tx.last_insert_rowid();
    let acceptance_sequence =
        AcceptanceSequence::new(stored_count("request acceptance sequence", raw_sequence)?);
    let saved = ResourceRequest::new(
        request_id,
        task_id,
        resource_id,
        acceptance_sequence,
        origin_machine,
        command_spec.as_normalized().clone(),
    )?;
    tx.commit()?;
    Ok(saved)
}

/// Return the saved request for an exact retry, or a conflict for changed content
fn replay_request_on(
    conn: &Connection,
    authority_machine: MachineId,
    identity: RequestIdentity,
    normalized_spec: &NormalizedSpec,
) -> Result<Option<ResourceRequest>, ResourceStoreError> {
    let Some(saved) = select_request_by_id(conn, identity.request_id)? else {
        return Ok(None);
    };
    if saved.task_id != identity.task_id
        || saved.resource_id != identity.resource_id
        || saved.origin_machine != identity.origin_machine
    {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestIdentityMismatch,
        ));
    }
    let command_spec = CommandSpec::try_from(normalized_spec.clone())?;
    if saved.spec().as_normalized() != command_spec.as_normalized() {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestSpecMismatch,
        ));
    }
    check_resource_authority(conn, identity.resource_id, authority_machine)?;

    Ok(Some(saved))
}

/// Refuse new queued work outside the ownership contract
///
/// A path-qualified entry point is inspected now. A bare program name resolves
/// from the executor `PATH`, so its task binding inspects it before any spawn. A
/// container's mount sources must exist on this authority
fn check_queued_command_ownership(spec: &NormalizedSpec) -> Result<(), ResourceStoreError> {
    let command_spec = CommandSpec::try_from(spec.clone())?;
    if let Some(risk) = resource_task_ownership_risk(&command_spec) {
        return Err(ResourceStoreError::UnsupportedCommandOwnership { risk });
    }
    crate::spec::check_workload_host(&spec.workload)
        .map_err(ResourceStoreError::TaskPreparation)?;
    crate::resource::foreground::inspect_path_qualified_entry_point(spec)
        .map_err(|risk| ResourceStoreError::UnsupportedCommandOwnership { risk })
}

/// Read all requests for one resource in serving order
pub(crate) fn requests_for_resource_for_authority(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
    check_resource_authority(conn, resource_id, authority_machine)?;
    let sql = format!(
        "SELECT {REQUEST_COLUMNS} FROM resource_requests \
         WHERE resource_id = ?1 ORDER BY queue_rank, acceptance_sequence"
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map([resource_id.as_uuid().to_string()], |row| {
        Ok(decode_request(row))
    })?;
    collect_decoded(rows)
}

/// Load assigned requests for one authority so task ownership can be checked durably
pub(crate) fn assigned_resource_requests_for_authority(
    conn: &Connection,
    authority_machine: MachineId,
) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
    let mut statement = conn.prepare(
        "SELECT rr.acceptance_sequence, rr.request_id, rr.task_id, rr.resource_id,
                rr.origin_machine, rr.spec_json, rr.state_json
         FROM resource_requests AS rr
         JOIN resources AS r ON r.id = rr.resource_id
         WHERE r.authority_machine = ?1
           AND json_extract(rr.state_json, '$.type') = 'assigned'
         ORDER BY rr.resource_id, rr.queue_rank, rr.acceptance_sequence",
    )?;
    let rows = statement.query_map([authority_machine.to_string()], |row| {
        Ok(decode_request(row))
    })?;
    collect_decoded(rows)
}

/// Read the next queued request for one resource in serving order
pub(crate) fn next_queued_request_for_authority(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
) -> Result<Option<ResourceRequest>, ResourceStoreError> {
    check_resource_authority(conn, resource_id, authority_machine)?;
    let sql = format!(
        "SELECT {REQUEST_COLUMNS} FROM resource_requests \
         WHERE resource_id = ?1 AND json_extract(state_json, '$.type') = 'queued' \
         ORDER BY queue_rank, acceptance_sequence LIMIT 1"
    );
    conn.query_row(&sql, [resource_id.as_uuid().to_string()], |row| {
        Ok(decode_request(row))
    })
    .optional()?
    .transpose()
}

/// Whether a queued or assigned request sits before `request_id` in serving order
pub(super) fn earlier_active_request_exists(
    conn: &Connection,
    resource_id: ResourceId,
    request_id: RequestId,
) -> Result<bool, ResourceStoreError> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM resource_requests AS earlier
            JOIN resource_requests AS selected
              ON selected.request_id = ?2
             AND selected.resource_id = earlier.resource_id
            WHERE earlier.resource_id = ?1
              AND (earlier.queue_rank, earlier.acceptance_sequence)
                  < (selected.queue_rank, selected.acceptance_sequence)
              AND json_extract(earlier.state_json, '$.type') IN ('queued', 'assigned')
        )",
        params![resource_id.as_uuid().to_string(), request_id.0.to_string()],
        |row| row.get(0),
    )?;
    Ok(exists)
}

/// Rewrite queued ranks as a permutation of their current values
///
/// The caller supplies a validated serving order from the same write transaction
pub(crate) fn rewrite_queued_request_ranks(
    conn: &Connection,
    resource_id: ResourceId,
    request_order: &[RequestId],
) -> Result<(), ResourceStoreError> {
    let mut statement = conn.prepare(
        "SELECT request_id, queue_rank FROM resource_requests
         WHERE resource_id = ?1 AND json_extract(state_json, '$.type') = 'queued'
         ORDER BY queue_rank, acceptance_sequence",
    )?;
    let rows = statement.query_map([resource_id.as_uuid().to_string()], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let current = rows.collect::<Result<Vec<_>, _>>()?;
    // rusqlite forbids another statement while this query statement is live
    drop(statement);

    let mut current_ids = current
        .iter()
        .map(|(request_id, _)| request_id.clone())
        .collect::<Vec<_>>();
    let mut requested_ids = request_order
        .iter()
        .map(|request_id| request_id.0.to_string())
        .collect::<Vec<_>>();
    current_ids.sort_unstable();
    requested_ids.sort_unstable();
    if current_ids != requested_ids {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestStateChanged,
        ));
    }

    let mut ranks = current
        .into_iter()
        .map(|(_, queue_rank)| queue_rank)
        .collect::<Vec<_>>();
    ranks.sort_unstable();
    for (request_id, queue_rank) in request_order.iter().zip(ranks) {
        let changed = conn.execute(
            "UPDATE resource_requests SET queue_rank = ?1
             WHERE request_id = ?2 AND resource_id = ?3
               AND json_extract(state_json, '$.type') = 'queued'",
            params![
                queue_rank,
                request_id.0.to_string(),
                resource_id.as_uuid().to_string(),
            ],
        )?;
        if changed != 1 {
            return Err(ResourceStoreError::Conflict(
                ConflictReason::RequestStateChanged,
            ));
        }
    }

    Ok(())
}

fn next_queue_rank(conn: &Connection, resource_id: ResourceId) -> Result<i64, ResourceStoreError> {
    let maximum: i64 = conn.query_row(
        "SELECT COALESCE(MAX(queue_rank), 0) FROM resource_requests WHERE resource_id = ?1",
        [resource_id.as_uuid().to_string()],
        |row| row.get(0),
    )?;
    maximum.checked_add(1).ok_or(ResourceStoreError::Conflict(
        ConflictReason::QueueRankExhausted,
    ))
}

pub(super) fn prevention_exists(
    conn: &Connection,
    identity: RequestIdentity,
) -> Result<bool, ResourceStoreError> {
    let sql = format!(
        "SELECT {PREVENTION_COLUMNS} FROM resource_request_preventions \
         WHERE request_id = ?1 OR task_id = ?2"
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(
        params![
            identity.request_id.0.to_string(),
            identity.task_id.to_string()
        ],
        |row| Ok(decode_prevention(row)),
    )?;
    let saved = collect_decoded(rows)?;
    if saved.iter().any(|saved| *saved != identity) {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::PreventionIdentityMismatch,
        ));
    }

    Ok(!saved.is_empty())
}

pub(super) fn request_matches_identity(
    request: &ResourceRequest,
    identity: RequestIdentity,
) -> bool {
    request.request_id == identity.request_id
        && request.task_id == identity.task_id
        && request.resource_id == identity.resource_id
        && request.origin_machine == identity.origin_machine
}

pub(super) fn task_id_exists(conn: &Connection, task_id: TaskId) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM resource_requests WHERE task_id = ?1)",
        [task_id.to_string()],
        |row| row.get(0),
    )
}

pub(super) fn executor_identity_exists(
    conn: &Connection,
    task_id: TaskId,
) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM executor_identities WHERE task_id = ?1)",
        [task_id.to_string()],
        |row| row.get(0),
    )
}

pub(super) fn select_executor_identity(
    conn: &Connection,
    task_id: TaskId,
) -> Result<Option<ExecutorIdentity>, ResourceStoreError> {
    let data: Option<String> = conn
        .query_row(
            "SELECT identity_json FROM executor_identities WHERE task_id = ?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let identity = data
        .as_deref()
        .map(|data| stored_json("executor identity", data))
        .transpose()?;
    Ok(identity)
}

pub(super) fn local_task_exists(
    conn: &Connection,
    task_id: TaskId,
) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE id = ?1)",
        [task_id.to_string()],
        |row| row.get(0),
    )
}

fn decode_prevention(row: &Row<'_>) -> Result<RequestIdentity, ResourceStoreError> {
    let text = |index, what| stored_column::<String>(row, index, what);
    Ok(RequestIdentity {
        request_id: RequestId(stored_uuid(
            "prevention request identity",
            &text(0, "prevention request identity")?,
        )?),
        task_id: TaskId(stored_uuid(
            "prevention task identity",
            &text(1, "prevention task identity")?,
        )?),
        resource_id: stored_id(
            "prevention resource identity",
            &text(2, "prevention resource identity")?,
        )?,
        origin_machine: MachineId::from_uuid(stored_uuid(
            "prevention origin machine",
            &text(3, "prevention origin machine")?,
        )?),
    })
}
