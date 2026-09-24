//! Test fixtures that drive the authority store without a daemon

use rusqlite::{Connection, TransactionBehavior, params};

use super::codec::encode_json;
use super::error::{ConflictReason, ResourceStoreError};
use super::notice::{SupervisorNoticeStoreError, retarget_supervisor_notice_in_transaction};
use super::release_completion::{ReleaseCompletionReceipt, ReleaseCompletionResult};
use super::release_loan::{
    OpenReleaseLoanError, OpenReleaseLoanResult, open_release_loan_in_transaction,
};
use super::rows::{select_request_by_id, select_resource};
use super::schema::RESOURCE_SCHEMA;
use super::{
    QueueCancellationResult, accept_request_for_authority, cancel_request_before_activation_on,
    oldest_queued_request_for_authority, register_resource_for_authority,
    requests_for_resource_for_authority,
};
use crate::domain::TaskId;
use crate::machine::MachineId;
use crate::resource::{
    ActionId, AssignmentRevision, Loan, LoanPhase, LoanState, NoticeId, Resource, ResourceId,
    ResourceRequest, ResourceRevision, ServingReleaseProvenance, SupervisorAddress,
    SupervisorNotice,
};
use crate::spec::NormalizedSpec;
use crate::submission::RequestId;

/// Install the resource schema on an explicitly supplied connection
pub(crate) fn install_schema(conn: &mut Connection) -> Result<(), ResourceStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(RESOURCE_SCHEMA)?;
    tx.commit()?;
    Ok(())
}

/// Authority saved for a resource
///
/// An unknown resource gets a fresh machine, because the store reports
/// `ResourceNotFound` before it compares authorities
fn saved_authority(conn: &Connection, resource_id: ResourceId) -> MachineId {
    select_resource(conn, resource_id)
        .ok()
        .flatten()
        .map_or_else(MachineId::new, |resource| resource.authority_machine())
}

/// Register a resource on its own declared authority
pub(crate) fn register_resource(
    conn: &mut Connection,
    resource: &Resource,
) -> Result<Resource, ResourceStoreError> {
    register_resource_for_authority(conn, resource.authority_machine(), resource)
}

/// Accept a request on the saved authority of its resource
pub(crate) fn accept_request(
    conn: &mut Connection,
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
    normalized_spec: NormalizedSpec,
) -> Result<ResourceRequest, ResourceStoreError> {
    let authority = saved_authority(conn, resource_id);
    accept_request_for_authority(
        conn,
        authority,
        request_id,
        task_id,
        resource_id,
        origin_machine,
        normalized_spec,
    )
}

/// Open one release action in its own transaction
pub(crate) fn open_release_loan_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    expected_state_revision: ResourceRevision,
) -> Result<OpenReleaseLoanResult, OpenReleaseLoanError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    open_release_loan_in_transaction(tx, authority_machine, resource_id, expected_state_revision)
}

/// Retarget an undelivered notice in its own transaction
pub(crate) fn retarget_supervisor_notice(
    conn: &mut Connection,
    notice_id: NoticeId,
    expected_assignment_revision: AssignmentRevision,
    destination: SupervisorAddress,
    new_assignment_revision: AssignmentRevision,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let notice = retarget_supervisor_notice_in_transaction(
        &tx,
        notice_id,
        expected_assignment_revision,
        destination,
        new_assignment_revision,
    )?;
    tx.commit()?;
    Ok(notice)
}

/// Cancel a queued or assigned request on its fixed authority in its own transaction
pub(crate) fn cancel_request_before_activation_for_authority(
    conn: &mut Connection,
    authority_machine: MachineId,
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
) -> Result<QueueCancellationResult, ResourceStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let result = cancel_request_before_activation_on(
        &tx,
        authority_machine,
        request_id,
        task_id,
        resource_id,
        origin_machine,
    )?;
    tx.commit()?;
    Ok(result)
}

/// Cancel a request on the saved authority of its resource
pub(crate) fn cancel_request_before_activation(
    conn: &mut Connection,
    request_id: RequestId,
    task_id: TaskId,
    resource_id: ResourceId,
    origin_machine: MachineId,
) -> Result<QueueCancellationResult, ResourceStoreError> {
    let authority = saved_authority(conn, resource_id);
    cancel_request_before_activation_for_authority(
        conn,
        authority,
        request_id,
        task_id,
        resource_id,
        origin_machine,
    )
}

/// Read all requests of a resource on its saved authority
pub(crate) fn requests_for_resource(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
    requests_for_resource_for_authority(conn, saved_authority(conn, resource_id), resource_id)
}

/// Read the oldest queued request of a resource on its saved authority
pub(crate) fn oldest_queued_request(
    conn: &Connection,
    resource_id: ResourceId,
) -> Result<Option<ResourceRequest>, ResourceStoreError> {
    oldest_queued_request_for_authority(conn, saved_authority(conn, resource_id), resource_id)
}

/// Save a verified completed-trainer release receipt for one Serving loan
pub(crate) fn seed_verified_serving_provenance_for_test(
    conn: &mut Connection,
    mut loan: Loan,
    authority_machine: MachineId,
    resource_id: ResourceId,
    request_id: RequestId,
    expected_state_revision: ResourceRevision,
    state_revision: ResourceRevision,
) -> Result<Loan, ResourceStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let resource =
        select_resource(&tx, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    let task_id = resource
        .registered_background_task
        .unwrap_or_else(TaskId::new);
    let action_id = ActionId::new();
    let publication_sha256 = crate::digest::Sha256Digest::from_bytes([0xab; 32]);
    let LoanState::Active {
        phase:
            LoanPhase::Serving {
                return_context,
                release_provenance,
                ..
            },
    } = &mut loan.state
    else {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    };
    *release_provenance = ServingReleaseProvenance::CompletedTrainerResult {
        action_id,
        task_id,
        publication_sha256,
    };
    let request = select_request_by_id(&tx, request_id)?
        .ok_or(ResourceStoreError::Conflict(ConflictReason::RequestMissing))?;
    let receipt = ReleaseCompletionReceipt {
        action_id,
        authority_machine,
        resource_id,
        expected_state_revision,
        return_context: return_context.clone(),
        release_provenance: release_provenance.clone(),
        result: ReleaseCompletionResult::Assigned {
            loan: loan.clone(),
            request,
            state_revision,
        },
    };
    let state_json = encode_json(&loan.state)?;
    let changed = tx.execute(
        "UPDATE loans SET state_json = ?1 WHERE id = ?2 AND resource_id = ?3",
        params![
            state_json,
            loan.id.as_uuid().to_string(),
            resource_id.as_uuid().to_string(),
        ],
    )?;
    if changed != 1 {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    }
    tx.execute(
        "INSERT INTO resource_release_completions (action_id, receipt_json)
         VALUES (?1, ?2)",
        params![action_id.as_uuid().to_string(), encode_json(&receipt)?,],
    )?;
    tx.commit()?;

    Ok(loan)
}
