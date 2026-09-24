//! Task-layer acceptance of the assigned resource request

use rusqlite::Connection;

use super::error::{ConflictReason, ResourceStoreError};
use super::provenance::serving_release_provenance_matches;
use super::queue::{
    RequestIdentity, earlier_active_request_exists, prevention_exists, request_matches_identity,
    select_executor_identity,
};
use super::rows::{
    check_resource_authority, select_non_closed_loan, select_request_by_id, select_resource,
};
use crate::domain::{ProcessStatus, TaskEnv, TaskId};
use crate::machine::MachineId;
use crate::resource::{
    AcceptanceSequence, CommandSpec, LoanId, LoanPhase, LoanState, ResourceId, ResourceRequest,
    ResourceRequestState, ResourceRevision,
};
use crate::submission::{ExecutorIdentity, PreAcceptanceRejection, RequestId};

/// Exact authority selection and executor context for accepting an assigned request
#[derive(Debug, Clone)]
pub(crate) struct ResourceTaskAcceptanceInput {
    /// Fixed authority machine recorded on the resource
    pub(crate) authority_machine: MachineId,
    /// Resource and serving loan that selected this request
    pub(crate) resource_id: ResourceId,
    /// Caller retry identity saved with the request
    pub(crate) request_id: RequestId,
    /// Preallocated task identity saved with the request
    pub(crate) task_id: TaskId,
    /// Immutable authority-assigned acceptance identity of the selected request
    pub(crate) acceptance_sequence: AcceptanceSequence,
    /// Serving loan that owns the selected request
    pub(crate) loan_id: LoanId,
    /// Resource revision observed when the request was selected
    pub(crate) expected_state_revision: ResourceRevision,
    /// Immutable command spec observed with the selected request
    pub(crate) command_spec: CommandSpec,
    /// Executor-machine environment used to create the task row
    pub(crate) executor_env: TaskEnv,
}

/// Result of the atomic task-layer acceptance for one assigned resource request
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResourceTaskAcceptance {
    /// The task row, accepted identity, and first queued event committed together
    Inserted {
        /// Preallocated task identity accepted by this authority
        task: TaskId,
    },
    /// An exact acceptance already exists and retains its current task state
    Existing {
        /// Preallocated task identity accepted by this authority
        task: TaskId,
        /// State retained by the task layer
        state: ProcessStatus,
    },
}

/// Accepted task identity retained by an authority-owned assigned request
#[derive(Debug, Clone)]
pub(crate) struct AcceptedResourceTask {
    /// Exact assigned resource request that owns this task identity and names its loan
    pub(crate) request: ResourceRequest,
    /// Durable executor state, cross-checked against the task row and first event
    pub(crate) state: ProcessStatus,
}

/// Validate the exact assigned request that is eligible for task-layer acceptance
pub(crate) fn assigned_resource_request_for_acceptance(
    conn: &Connection,
    input: &ResourceTaskAcceptanceInput,
) -> Result<ResourceRequest, ResourceStoreError> {
    let authority = input.authority_machine;
    check_resource_authority(conn, input.resource_id, authority)?;
    let saved = select_request_by_id(conn, input.request_id)?
        .ok_or(ResourceStoreError::Conflict(ConflictReason::RequestMissing))?;
    let identity = RequestIdentity {
        request_id: input.request_id,
        task_id: input.task_id,
        resource_id: input.resource_id,
        origin_machine: saved.origin_machine,
    };
    if !request_matches_identity(&saved, identity)
        || saved.acceptance_sequence != input.acceptance_sequence
        || saved.spec().as_normalized() != input.command_spec.as_normalized()
    {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestIdentityMismatch,
        ));
    }

    if prevention_exists(conn, identity)? {
        return Err(ResourceStoreError::Prevented);
    }
    if let Some(executor) = select_executor_identity(conn, input.task_id)? {
        match executor {
            ExecutorIdentity::Rejected(rejection) => {
                if rejection.origin_machine == saved.origin_machine
                    && rejection.execution_machine == authority
                    && rejection.reason == PreAcceptanceRejection::Cancelled.as_str()
                {
                    return Err(ResourceStoreError::Prevented);
                }
                return Err(ResourceStoreError::Conflict(
                    ConflictReason::ExecutorIdentityMismatch,
                ));
            }
            ExecutorIdentity::Accepted(record) => {
                let same_saved_spec = record.current_spec() == Some(saved.spec().as_normalized());
                if record.task != input.task_id
                    || record.origin_machine != saved.origin_machine
                    || record.execution_machine != authority
                    || !record.has_valid_spec_owners()
                    || !same_saved_spec
                {
                    return Err(ResourceStoreError::Conflict(
                        ConflictReason::ExecutorIdentityMismatch,
                    ));
                }
            }
        }
    }
    if matches!(&saved.state, ResourceRequestState::CancelledBeforeLaunch) {
        return Err(ResourceStoreError::Prevented);
    }

    let resource =
        select_resource(conn, input.resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    if resource.state_revision != input.expected_state_revision {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ResourceRevisionChanged,
        ));
    }

    let ResourceRequestState::Assigned { loan_id } = &saved.state else {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestStateChanged,
        ));
    };
    if *loan_id != input.loan_id {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    }
    let loan = select_non_closed_loan(conn, input.resource_id)?
        .ok_or(ResourceStoreError::Conflict(ConflictReason::LoanChanged))?;
    if loan.id != input.loan_id || loan.resource_id != input.resource_id {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    }
    let LoanState::Active {
        phase:
            LoanPhase::Serving {
                current_request_id,
                return_context,
                release_provenance,
            },
    } = &loan.state
    else {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    };
    if *current_request_id != input.request_id {
        return Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged));
    }
    if !serving_release_provenance_matches(
        conn,
        authority,
        &resource,
        &loan,
        return_context,
        release_provenance,
    )? {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::ServingReleaseUnverified,
        ));
    }

    if earlier_active_request_exists(conn, input.resource_id, input.request_id)? {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::EarlierRequestActive,
        ));
    }

    Ok(saved)
}
