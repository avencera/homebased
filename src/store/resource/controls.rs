//! Read models and idempotent operator controls for authority-owned resources
//!
//! Each control commits its operation identity, revision check, and any notice
//! transition in one IMMEDIATE transaction. The daemon performs the external
//! effect only after that commit, and an exact retry returns the saved effect
//! without a second revision check or a second notice attempt

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::machine::MachineId;
use crate::resource::api::BrowserResourceAction;
use crate::resource::store::{
    ResourceStoreError, SupervisorNoticeStoreError, decode_supervisor_notice_record,
    requests_for_resource_for_authority, resources_for_authority,
    retarget_supervisor_notice_in_transaction, select_non_closed_loan, select_request_by_id,
    select_resource, select_supervisor_notice_record, update_supervisor_notice_cas,
};
use crate::resource::{
    ActionId, AssignmentRevision, DeliveryAttemptId, Loan, LoanId, LoanPhase, LoanState, NoticeId,
    Resource, ResourceId, ResourceRequest, ResourceRequestState, ResourceRevision,
    SupervisorAddress, SupervisorNotice, SupervisorNoticeDelivery,
};
use crate::store::Store;

#[cfg(test)]
mod test_support;

/// Durable resource state from which every read view is derived
#[derive(Debug, Clone)]
pub(crate) struct ResourceReadModel {
    /// Resource owned by the reading authority
    pub(crate) resource: Resource,
    /// Non-closed loan, if one reserves the resource
    pub(crate) loan: Option<Loan>,
    /// Every request in acceptance order
    pub(crate) requests: Vec<ResourceRequest>,
    /// Notices that belong to the non-closed loan
    pub(crate) notices: Vec<SupervisorNotice>,
}

/// Immutable content bound to one control operation identity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResourceControlRequest {
    /// Resource that the control targets
    pub(crate) resource_id: ResourceId,
    /// Resource revision that the caller observed
    pub(crate) expected_revision: ResourceRevision,
    /// Requested control
    pub(crate) action: BrowserResourceAction,
}

/// External effect that the daemon performs after the operation commits
#[derive(Debug, Clone)]
pub(crate) enum ResourceControlEffect {
    /// Ask the request origin to cancel this queued request
    CancelQueued {
        /// Queued request read in the committing transaction
        request: ResourceRequest,
    },
    /// Ask the request origin to cancel this active command task
    StopActive {
        /// Assigned request whose task holds the resource
        request: ResourceRequest,
    },
    /// Deliver the one explicit attempt reserved by this operation
    Renotify {
        /// Notice whose automatic attempts failed
        notice_id: NoticeId,
        /// Attempt reserved for this operation
        attempt_id: DeliveryAttemptId,
    },
}

/// Committed control operation
#[derive(Debug, Clone)]
pub(crate) struct ResourceControlStart {
    /// Effect bound to the operation
    pub(crate) effect: ResourceControlEffect,
    /// Whether an earlier call already committed this operation
    pub(crate) replayed: bool,
}

/// Supervisor assignment after an explicit replacement
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SupervisorReplacement {
    /// Resource with its new supervisor and assignment revision
    pub(crate) resource: Resource,
    /// Undelivered notices moved to the new supervisor
    pub(crate) retargeted: Vec<SupervisorNotice>,
}

/// Why an authority refused or failed one resource control
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResourceControlError {
    /// The resource does not exist on this authority
    #[error("resource not found")]
    NotFound,
    /// The resource belongs to a different authority
    #[error("resource authority mismatch: expected {expected}, found {found}")]
    WrongAuthority {
        /// Fixed authority recorded for the resource
        expected: MachineId,
        /// Machine that received the control
        found: MachineId,
    },
    /// The caller observed an older resource revision
    #[error("resource revision is stale")]
    StaleRevision {
        /// Current resource revision
        current: ResourceRevision,
    },
    /// The operation identity already names different content
    #[error("operation identity already names different content")]
    Conflict,
    /// The action does not apply to the current resource phase
    #[error("{0}")]
    NotAllowed(String),
    /// Durable notice state refused the transition
    #[error(transparent)]
    Notice(#[from] SupervisorNoticeStoreError),
    /// Stored operation content cannot be decoded
    #[error("resource control operation encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    /// Other resource storage failure
    #[error(transparent)]
    Resource(ResourceStoreError),
    /// SQLite failed
    #[error("resource control storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

impl From<ResourceStoreError> for ResourceControlError {
    fn from(error: ResourceStoreError) -> Self {
        match error {
            ResourceStoreError::ResourceNotFound => Self::NotFound,
            ResourceStoreError::WrongAuthority { expected, found } => {
                Self::WrongAuthority { expected, found }
            }
            ResourceStoreError::Storage(error) => Self::Storage(error),
            other => Self::Resource(other),
        }
    }
}

impl Store {
    /// Read durable resource state owned by this authority, optionally for one resource
    pub(crate) fn resource_read_models(
        &self,
        authority_machine: MachineId,
        resource_id: Option<ResourceId>,
    ) -> Result<Vec<ResourceReadModel>, ResourceStoreError> {
        resource_read_models(&self.conn, authority_machine, resource_id)
    }

    /// Commit one idempotent control operation and return its external effect
    pub(crate) fn begin_resource_control(
        &mut self,
        authority_machine: MachineId,
        operation_id: Uuid,
        request: &ResourceControlRequest,
        attempt_id: DeliveryAttemptId,
    ) -> Result<ResourceControlStart, ResourceControlError> {
        begin_resource_control(
            &mut self.conn,
            authority_machine,
            operation_id,
            request,
            attempt_id,
        )
    }

    /// Replace the supervisor and retarget undelivered notices in one transaction
    pub(crate) fn replace_resource_supervisor(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        expected_revision: ResourceRevision,
        supervisor: SupervisorAddress,
    ) -> Result<SupervisorReplacement, ResourceControlError> {
        replace_resource_supervisor(
            &mut self.conn,
            authority_machine,
            resource_id,
            expected_revision,
            supervisor,
        )
    }
}

fn resource_read_models(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: Option<ResourceId>,
) -> Result<Vec<ResourceReadModel>, ResourceStoreError> {
    let snapshots = resources_for_authority(conn, authority_machine)?;
    snapshots
        .into_iter()
        .filter(|snapshot| resource_id.is_none_or(|id| snapshot.resource.id == id))
        .map(|snapshot| {
            let requests =
                requests_for_resource_for_authority(conn, authority_machine, snapshot.resource.id)?;
            let notices = match &snapshot.loan {
                Some(loan) => notices_for_loan(conn, loan.id)?,
                None => Vec::new(),
            };
            Ok(ResourceReadModel {
                resource: snapshot.resource,
                loan: snapshot.loan,
                requests,
                notices,
            })
        })
        .collect()
}

fn notices_for_loan(
    conn: &Connection,
    loan_id: LoanId,
) -> Result<Vec<SupervisorNotice>, rusqlite::Error> {
    let mut statement = conn.prepare(
        "SELECT id, loan_id, action_id, notice_json
         FROM resource_supervisor_notices
         WHERE loan_id = ?1
         ORDER BY id ASC",
    )?;
    statement
        .query_map(
            [loan_id.as_uuid().to_string()],
            decode_supervisor_notice_record,
        )?
        .map(|record| record.map(|(notice, _)| notice))
        .collect()
}

/// Action identity that the loan still waits on, if any
pub(crate) fn open_action_id(loan: &Loan) -> Option<ActionId> {
    match &loan.state {
        LoanState::Active { phase } => phase_action_id(phase),
        LoanState::NeedsAttention { action_id, .. } => Some(*action_id),
        LoanState::Closed { .. } => None,
    }
}

fn phase_action_id(phase: &LoanPhase) -> Option<ActionId> {
    match phase {
        LoanPhase::AwaitingRelease { action_id, .. }
        | LoanPhase::AwaitingReturn { action_id, .. }
        | LoanPhase::Restoring { action_id, .. } => Some(*action_id),
        LoanPhase::Serving { .. } => None,
    }
}

fn begin_resource_control(
    conn: &mut Connection,
    authority_machine: MachineId,
    operation_id: Uuid,
    request: &ResourceControlRequest,
    attempt_id: DeliveryAttemptId,
) -> Result<ResourceControlStart, ResourceControlError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let resource = authority_resource(&tx, authority_machine, request.resource_id)?;

    if let Some(saved) = saved_operation(&tx, operation_id)? {
        if saved.request != *request {
            return Err(ResourceControlError::Conflict);
        }
        let effect = replayed_effect(&tx, authority_machine, request, saved.attempt_id)?;
        tx.commit()?;
        return Ok(ResourceControlStart {
            effect,
            replayed: true,
        });
    }

    if resource.state_revision != request.expected_revision {
        return Err(ResourceControlError::StaleRevision {
            current: resource.state_revision,
        });
    }
    let loan = select_non_closed_loan(&tx, request.resource_id)?;
    let effect = match &request.action {
        BrowserResourceAction::CancelQueued { request_id } => {
            let queued = request_for_resource(&tx, request.resource_id, *request_id)?;
            if queued.state != ResourceRequestState::Queued {
                return Err(ResourceControlError::NotAllowed(format!(
                    "request is {}, not queued",
                    request_state_name(&queued.state)
                )));
            }
            ResourceControlEffect::CancelQueued { request: queued }
        }
        BrowserResourceAction::StopActive { task_id } => {
            let active = active_request(&tx, loan.as_ref())?;
            if active.task_id != *task_id {
                return Err(ResourceControlError::NotAllowed(
                    "task is not the active command on this resource".into(),
                ));
            }
            ResourceControlEffect::StopActive { request: active }
        }
        BrowserResourceAction::Renotify { notice_id } => {
            reserve_explicit_attempt(&tx, loan.as_ref(), *notice_id, attempt_id)?;
            ResourceControlEffect::Renotify {
                notice_id: *notice_id,
                attempt_id,
            }
        }
    };
    let saved_attempt = match &effect {
        ResourceControlEffect::Renotify { attempt_id, .. } => {
            Some(attempt_id.as_uuid().to_string())
        }
        ResourceControlEffect::CancelQueued { .. } | ResourceControlEffect::StopActive { .. } => {
            None
        }
    };
    tx.execute(
        "INSERT INTO resource_control_operations
            (operation_id, resource_id, request_json, attempt_id)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            operation_id.to_string(),
            request.resource_id.as_uuid().to_string(),
            serde_json::to_string(request)?,
            saved_attempt,
        ],
    )?;
    tx.commit()?;
    Ok(ResourceControlStart {
        effect,
        replayed: false,
    })
}

struct SavedOperation {
    request: ResourceControlRequest,
    attempt_id: Option<DeliveryAttemptId>,
}

fn saved_operation(
    tx: &Transaction<'_>,
    operation_id: Uuid,
) -> Result<Option<SavedOperation>, ResourceControlError> {
    let row = tx
        .query_row(
            "SELECT request_json, attempt_id FROM resource_control_operations
             WHERE operation_id = ?1",
            [operation_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    let Some((request_json, attempt_id)) = row else {
        return Ok(None);
    };
    let attempt_id = attempt_id
        .map(|value| {
            Uuid::parse_str(&value)
                .map(DeliveryAttemptId::from_uuid)
                .map_err(|_| {
                    ResourceControlError::NotAllowed(
                        "saved control operation has an invalid attempt identity".into(),
                    )
                })
        })
        .transpose()?;
    Ok(Some(SavedOperation {
        request: serde_json::from_str(&request_json)?,
        attempt_id,
    }))
}

// a replay repeats only effects that are idempotent at their owner; a renotify
// replay names its saved attempt and never reserves another one
fn replayed_effect(
    tx: &Transaction<'_>,
    authority_machine: MachineId,
    request: &ResourceControlRequest,
    attempt_id: Option<DeliveryAttemptId>,
) -> Result<ResourceControlEffect, ResourceControlError> {
    match &request.action {
        BrowserResourceAction::CancelQueued { request_id } => {
            Ok(ResourceControlEffect::CancelQueued {
                request: request_for_resource(tx, request.resource_id, *request_id)?,
            })
        }
        BrowserResourceAction::StopActive { task_id } => {
            let request =
                requests_for_resource_for_authority(tx, authority_machine, request.resource_id)?
                    .into_iter()
                    .find(|saved| saved.task_id == *task_id)
                    .ok_or_else(|| {
                        ResourceControlError::NotAllowed(
                            "saved stop operation no longer names a resource request".into(),
                        )
                    })?;
            Ok(ResourceControlEffect::StopActive { request })
        }
        BrowserResourceAction::Renotify { notice_id } => {
            let attempt_id = attempt_id.ok_or_else(|| {
                ResourceControlError::NotAllowed(
                    "saved renotify operation has no attempt identity".into(),
                )
            })?;
            Ok(ResourceControlEffect::Renotify {
                notice_id: *notice_id,
                attempt_id,
            })
        }
    }
}

fn authority_resource(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
) -> Result<Resource, ResourceControlError> {
    let resource = select_resource(conn, resource_id)?.ok_or(ResourceControlError::NotFound)?;
    if resource.authority_machine() != authority_machine {
        return Err(ResourceControlError::WrongAuthority {
            expected: resource.authority_machine(),
            found: authority_machine,
        });
    }
    Ok(resource)
}

fn request_for_resource(
    conn: &Connection,
    resource_id: ResourceId,
    request_id: crate::submission::RequestId,
) -> Result<ResourceRequest, ResourceControlError> {
    select_request_by_id(conn, request_id)?
        .filter(|request| request.resource_id == resource_id)
        .ok_or_else(|| {
            ResourceControlError::NotAllowed("request does not belong to this resource".into())
        })
}

fn active_request(
    conn: &Connection,
    loan: Option<&Loan>,
) -> Result<ResourceRequest, ResourceControlError> {
    let not_serving =
        || ResourceControlError::NotAllowed("no command task is active on this resource".into());
    let loan = loan.ok_or_else(not_serving)?;
    let LoanState::Active {
        phase: LoanPhase::Serving {
            current_request_id, ..
        },
    } = &loan.state
    else {
        return Err(not_serving());
    };
    let request = select_request_by_id(conn, *current_request_id)?.ok_or_else(not_serving)?;
    match request.state {
        ResourceRequestState::Assigned { loan_id } if loan_id == loan.id => Ok(request),
        _ => Err(not_serving()),
    }
}

fn reserve_explicit_attempt(
    tx: &Transaction<'_>,
    loan: Option<&Loan>,
    notice_id: NoticeId,
    attempt_id: DeliveryAttemptId,
) -> Result<(), ResourceControlError> {
    let (mut notice, old_json) = select_supervisor_notice_record(tx, notice_id)?
        .ok_or_else(|| ResourceControlError::NotAllowed("notice not found".into()))?;
    let pending = loan
        .filter(|loan| loan.id == notice.loan_id)
        .and_then(open_action_id);
    if pending != Some(notice.action_id) {
        return Err(ResourceControlError::NotAllowed(
            "the notice action is no longer pending".into(),
        ));
    }
    let attempts = match &notice.delivery {
        SupervisorNoticeDelivery::Failed { attempts, .. } => *attempts,
        SupervisorNoticeDelivery::Delivered { .. } => {
            return Err(ResourceControlError::NotAllowed(
                "the notice was already delivered".into(),
            ));
        }
        SupervisorNoticeDelivery::Pending { .. }
        | SupervisorNoticeDelivery::RetryPending { .. }
        | SupervisorNoticeDelivery::Sending { .. } => {
            return Err(ResourceControlError::NotAllowed(
                "automatic delivery has not failed for this notice".into(),
            ));
        }
    };
    // one explicit attempt past the exhausted budget; a failure settles back to failed
    notice.delivery = SupervisorNoticeDelivery::Sending {
        attempt_id,
        attempt: attempts.saturating_add(1),
    };
    update_supervisor_notice_cas(tx, &notice, &old_json)?;
    Ok(())
}

fn replace_resource_supervisor(
    conn: &mut Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    expected_revision: ResourceRevision,
    supervisor: SupervisorAddress,
) -> Result<SupervisorReplacement, ResourceControlError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut resource = authority_resource(&tx, authority_machine, resource_id)?;
    // an exact retry after a lost response finds the requested assignment already saved
    if resource.supervisor == supervisor {
        tx.commit()?;
        return Ok(SupervisorReplacement {
            resource,
            retargeted: Vec::new(),
        });
    }
    if resource.state_revision != expected_revision {
        return Err(ResourceControlError::StaleRevision {
            current: resource.state_revision,
        });
    }

    let previous = resource.assignment_revision;
    let next = AssignmentRevision::new(previous.get() + 1);
    let updated = tx.execute(
        "UPDATE resources
         SET supervisor_machine = ?1, supervisor_thread = ?2, assignment_revision = ?3
         WHERE id = ?4 AND assignment_revision = ?5 AND state_revision = ?6",
        params![
            supervisor.machine.as_uuid().to_string(),
            supervisor.thread.to_string(),
            sqlite_revision(next.get())?,
            resource_id.as_uuid().to_string(),
            sqlite_revision(previous.get())?,
            sqlite_revision(expected_revision.get())?,
        ],
    )?;
    if updated != 1 {
        return Err(ResourceControlError::StaleRevision {
            current: resource.state_revision,
        });
    }

    let mut retargeted = Vec::new();
    if let Some(loan) = select_non_closed_loan(&tx, resource_id)? {
        for notice in notices_for_loan(&tx, loan.id)? {
            if matches!(notice.delivery, SupervisorNoticeDelivery::Delivered { .. }) {
                continue;
            }
            retargeted.push(retarget_supervisor_notice_in_transaction(
                &tx,
                notice.id,
                notice.assignment_revision,
                supervisor,
                next,
            )?);
        }
    }
    tx.commit()?;

    resource.supervisor = supervisor;
    resource.assignment_revision = next;
    Ok(SupervisorReplacement {
        resource,
        retargeted,
    })
}

fn sqlite_revision(value: u64) -> Result<i64, ResourceControlError> {
    i64::try_from(value).map_err(|_| {
        ResourceControlError::NotAllowed("resource revision exceeds SQLite integer range".into())
    })
}

/// Stable snake-case name of a request state for messages
pub(crate) fn request_state_name(state: &ResourceRequestState) -> &'static str {
    match state {
        ResourceRequestState::Queued => "queued",
        ResourceRequestState::Assigned { .. } => "assigned",
        ResourceRequestState::Finished { .. } => "finished",
        ResourceRequestState::CancelledBeforeLaunch => "cancelled_before_launch",
        ResourceRequestState::Rejected { .. } => "rejected",
    }
}
