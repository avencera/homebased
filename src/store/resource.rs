//! StoreActor-facing operations for authority-local resource state

mod action_task;
mod assigned_task;
mod background;
mod cancellation;
mod controls;
mod initial_idle;
mod operator_release;
mod release_checkpoint;
mod release_completion;
mod release_proof;
mod release_watcher;
mod restore;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
mod trainer_association;
mod trainer_lock;

pub(crate) use action_task::{
    AcceptedActionTask, RemoteReleaseWatcherAcceptanceInput, ResourceActionError,
};
pub(crate) use background::{
    BackgroundLaunchAcceptance, BackgroundLaunchError, BackgroundLaunchInput,
    BackgroundLaunchPhase, BackgroundLaunchView, RemoteBackgroundLaunchInput,
    idle_boundary_decision_on, idle_opening_matches_on, open_idle_serving_loan_on,
    pending_background_launch_on, promote_started_background_launch_on,
};
pub(crate) use controls::{
    ResourceControlEffect, ResourceControlError, ResourceControlRequest, ResourceControlStart,
    ResourceReadModel, SupervisorReplacement, open_action_id,
};
pub(crate) use initial_idle::InitialIdleError;
pub(crate) use operator_release::{OperatorGpuFreeError, operator_serving_release_matches_on};
pub(crate) use release_proof::VerifiedReleaseProof;
pub(crate) use restore::{
    EndedRestoreResolution, PreparedReturnTask, RestoreReconcileOutcome, ReturnClosure,
    ReturnDecisionError, ReturnTaskAcceptance, ReturnTaskAcceptanceInput, ReturnTaskOrigin,
};

use rusqlite::{Connection, params};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Store;
use crate::domain::{TaskId, TaskRow};
use crate::machine::MachineId;
use crate::resource::store::{
    ResourceQueueReconcileError, ResourceSnapshot, ResourceStoreError, SupervisorNoticeStoreError,
    bind_release_watcher_for_authority as persist_release_watcher_binding,
    pending_supervisor_notices as load_pending_supervisor_notices,
    reconcile_resource_queue_for_authority as persist_resource_queue_reconciliation,
    recover_sending_supervisor_notices as recover_in_flight_supervisor_notices,
    register_resource_for_authority, requests_for_resource_for_authority,
    reserve_supervisor_notice_attempt as reserve_notice_attempt,
    resources_for_authority as load_resources_for_authority, select_resource,
    settle_supervisor_notice_attempt as settle_notice_attempt,
    supervisor_notice as load_supervisor_notice,
};
use crate::resource::{
    ActionId, DeliveryAttemptId, Loan, NoticeId, ReleaseWatcherIntent, Resource, ResourceId,
    ResourceQueueReconcileOutcome, ResourceRequest, SupervisorNotice,
};
use crate::spec::NormalizedSpec;
use crate::submission::RequestId;

impl Store {
    /// Register a resource only on the daemon that matches its fixed authority
    pub(crate) fn register_resource(
        &mut self,
        authority_machine: MachineId,
        resource: &Resource,
    ) -> Result<Resource, ResourceStoreError> {
        register_resource_for_authority(&mut self.conn, authority_machine, resource)
    }

    /// Load authority-owned resources and active loans on the store connection
    pub(crate) fn resource_snapshots_for_authority(
        &self,
        authority_machine: MachineId,
    ) -> Result<Vec<ResourceSnapshot>, ResourceStoreError> {
        load_resources_for_authority(&self.conn, authority_machine)
    }

    /// Read a resource's requests in authority-assigned FIFO order
    pub(crate) fn resource_requests(
        &self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
        requests_for_resource_for_authority(&self.conn, authority_machine, resource_id)
    }

    /// Reconcile queued work from authority-owned resource, loan, and task rows
    pub(crate) fn reconcile_resource_queue_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<ResourceQueueReconcileOutcome, ResourceQueueReconcileError> {
        persist_resource_queue_reconciliation(&mut self.conn, authority_machine, resource_id)
    }

    /// Bind one preallocated watcher launch identity to its saved release action
    pub(crate) fn bind_release_watcher_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        intent: ReleaseWatcherIntent,
    ) -> Result<ReleaseWatcherIntent, ResourceStoreError> {
        persist_release_watcher_binding(&mut self.conn, authority_machine, resource_id, intent)
    }

    /// Read one durable supervisor notice by its identity
    pub(crate) fn supervisor_notice(
        &self,
        notice_id: NoticeId,
    ) -> Result<Option<SupervisorNotice>, SupervisorNoticeStoreError> {
        load_supervisor_notice(&self.conn, notice_id)
    }

    /// List notices that can receive another delivery attempt
    pub(crate) fn pending_supervisor_notices(
        &self,
    ) -> Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError> {
        load_pending_supervisor_notices(&self.conn)
    }

    /// Reserve one bounded delivery attempt on this store connection
    pub(crate) fn reserve_supervisor_notice_attempt(
        &mut self,
        notice_id: NoticeId,
        attempt_id: DeliveryAttemptId,
    ) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
        reserve_notice_attempt(&mut self.conn, notice_id, attempt_id)
    }

    /// Settle only the exact in-flight delivery attempt
    pub(crate) fn settle_supervisor_notice_attempt(
        &mut self,
        notice_id: NoticeId,
        attempt_id: DeliveryAttemptId,
        result: Result<(), String>,
    ) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
        settle_notice_attempt(&mut self.conn, notice_id, attempt_id, result)
    }

    /// Recover in-flight notices after a daemon restart
    pub(crate) fn recover_sending_supervisor_notices(
        &mut self,
    ) -> Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError> {
        recover_in_flight_supervisor_notices(&mut self.conn)
    }
}

/// Read one resource and require that this daemon is its fixed authority
fn select_authority_resource(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
) -> Result<Resource, ResourceStoreError> {
    let resource =
        select_resource(conn, resource_id)?.ok_or(ResourceStoreError::ResourceNotFound)?;
    if resource.authority_machine() != authority_machine {
        return Err(ResourceStoreError::WrongAuthority {
            expected: resource.authority_machine(),
            found: authority_machine,
        });
    }

    Ok(resource)
}

/// Active loan phase that carries the release or return action being replaced
#[derive(Debug, Clone, Copy)]
enum LoanActionPhase {
    AwaitingRelease,
    AwaitingReturn,
    Restoring,
}

impl LoanActionPhase {
    /// Serialized `$.phase.type` tag of this phase
    const fn tag(self) -> &'static str {
        match self {
            Self::AwaitingRelease => "awaiting_release",
            Self::AwaitingReturn => "awaiting_return",
            Self::Restoring => "restoring",
        }
    }
}

/// Replace one active loan's state only while it is still in the exact action phase
///
/// Returns false when the loan left that phase, so each caller names its own refusal
fn replace_loan_in_action_phase_on(
    conn: &Connection,
    loan: &Loan,
    phase: LoanActionPhase,
    action_id: ActionId,
) -> Result<bool, ResourceStoreError> {
    let changed = conn.execute(
        "UPDATE loans SET state_json = ?1
         WHERE id = ?2 AND resource_id = ?3
           AND json_extract(state_json, '$.type') = 'active'
           AND json_extract(state_json, '$.phase.type') = ?4
           AND json_extract(state_json, '$.phase.action_id') = ?5",
        params![
            encode_resource_json(&loan.state)?,
            loan.id.as_uuid().to_string(),
            loan.resource_id.as_uuid().to_string(),
            phase.tag(),
            action_id.as_uuid().to_string(),
        ],
    )?;

    Ok(changed == 1)
}

/// Decode one saved JSON record; a failure is corrupt data, never a retryable storage error
fn decode_resource_json<T: DeserializeOwned>(
    what: &'static str,
    value: &str,
) -> Result<T, ResourceStoreError> {
    serde_json::from_str(value).map_err(|error| ResourceStoreError::corrupt(what, error))
}

fn encode_resource_json<T: Serialize>(value: &T) -> Result<String, ResourceStoreError> {
    serde_json::to_string(value).map_err(|error| {
        ResourceStoreError::Storage(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
    })
}

fn task_has_any_event(conn: &Connection, task: TaskId) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM executor_outbox WHERE task_id=?1
            UNION ALL SELECT 1 FROM executor_event_receipts WHERE task_id=?1
            UNION ALL SELECT 1 FROM executor_event_cursors WHERE task_id=?1
            UNION ALL SELECT 1 FROM executor_event_routes WHERE task_id=?1
        )",
        [task.to_string()],
        |row| row.get(0),
    )
}

/// Whether a task row runs exactly the command of this accepted spec
fn resource_task_row_matches(row: &TaskRow, task: TaskId, spec: &NormalizedSpec) -> bool {
    row.id == task
        && row.name.as_ref() == Some(&spec.name)
        && row.thread == spec.thread
        && row.workload == crate::invocation::persist_workload(&spec.workload)
        && row.cwd == spec.cwd
        && row.timeout == spec.timeout
        && row.binary.is_absolute()
}

/// Whether two task rows share every immutable launch field
fn same_task_binding(left: &TaskRow, right: &TaskRow) -> bool {
    left.id == right.id
        && left.name == right.name
        && left.thread == right.thread
        && left.workload == right.workload
        && left.cwd == right.cwd
        && left.timeout == right.timeout
        && left.env == right.env
        && left.binary == right.binary
}

/// Whether any request, task, route, identity, or event already claims a fresh identity pair
fn task_identity_is_used(
    conn: &Connection,
    request_id: RequestId,
    task_id: TaskId,
) -> Result<bool, ResourceStoreError> {
    Ok(resource_request_identity_exists(conn, request_id, task_id)?
        || super::release_watcher_task_id_is_reserved(conn, task_id)?
        || super::task_by_id_on(conn, task_id)
            .map_err(ResourceStoreError::TaskRow)?
            .is_some()
        || super::identity::origin_route_by_request_on(conn, request_id)?.is_some()
        || super::identity::origin_route_by_task_on(conn, task_id)?.is_some()
        || super::identity::executor_identity_on(conn, task_id)?.is_some()
        || task_has_any_event(conn, task_id)?)
}

/// Whether a resource request or prevention already claims this request or task identity
fn resource_request_identity_exists(
    conn: &Connection,
    request: RequestId,
    task: TaskId,
) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM resource_requests WHERE request_id=?1 OR task_id=?2
            UNION ALL
            SELECT 1 FROM resource_request_preventions WHERE request_id=?1 OR task_id=?2
        )",
        params![request.0.to_string(), task.to_string()],
        |row| row.get(0),
    )
}
