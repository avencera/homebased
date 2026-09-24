//! Acceptance of authority-assigned resource requests as executor tasks

use rusqlite::{Connection, TransactionBehavior};

use super::{encode_resource_json, resource_task_row_matches, task_has_any_event};
use crate::domain::{ProcessStatus, TaskId, TaskRow};
use crate::events::EventPayload;
use crate::machine::MachineId;
use crate::resource::store::{
    AcceptedResourceTask, AssignedResourceTaskReconcileInput, AssignedResourceTaskReconcileOutcome,
    ConflictReason, ResourceStoreError, ResourceTaskAcceptance, ResourceTaskAcceptanceInput,
    accept_request_for_authority, assigned_resource_request_for_acceptance,
    assigned_resource_requests_for_authority,
    reconcile_assigned_resource_task_for_authority as persist_assigned_task_reconciliation,
};
use crate::resource::{ResourceId, ResourceRequest, ResourceRequestState};
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::identity::{
    executor_identity_on, identity_origin_column_is_on, origin_route_by_request_on,
    origin_route_by_task_on,
};
use crate::store::{NewTask, Store};
use crate::submission::{
    ExecutionRecord, ExecutorIdentity, RequestId, ResourceRoutePhase, SubmissionState,
};

fn conflict(reason: ConflictReason) -> ResourceStoreError {
    ResourceStoreError::Conflict(reason)
}

impl Store {
    /// Read accepted resource task identities from their authority-owned assignments
    pub(crate) fn accepted_resource_tasks_for_authority(
        &self,
        authority_machine: MachineId,
    ) -> Result<Vec<AcceptedResourceTask>, ResourceStoreError> {
        let requests = assigned_resource_requests_for_authority(&self.conn, authority_machine)?;
        let mut accepted = Vec::new();

        for request in requests {
            let ResourceRequestState::Assigned { loan_id } = request.state else {
                continue;
            };
            let Some(executor) = executor_identity_on(&self.conn, request.task_id)? else {
                let task_row = crate::store::task_by_id_on(&self.conn, request.task_id)
                    .map_err(ResourceStoreError::TaskRow)?;
                if task_row.is_some() || task_has_any_event(&self.conn, request.task_id)? {
                    return Err(conflict(ConflictReason::TaskIdentityInUse));
                }
                continue;
            };
            let ExecutorIdentity::Accepted(record) = executor else {
                return Err(conflict(ConflictReason::ExecutorIdentityMismatch));
            };
            if !loan_is_reserved(&self.conn, loan_id, request.resource_id)? {
                return Err(conflict(ConflictReason::LoanChanged));
            }
            let task_row = crate::store::task_by_id_on(&self.conn, request.task_id)
                .map_err(ResourceStoreError::TaskRow)?;
            if !accepted_task_matches_request(
                &self.conn,
                &record,
                &request,
                authority_machine,
                task_row.as_ref(),
            )? {
                return Err(conflict(ConflictReason::ExecutorIdentityMismatch));
            }

            accepted.push(AcceptedResourceTask {
                request,
                state: record.state,
            });
        }

        Ok(accepted)
    }

    /// Accept a resource request and assign its authority-local FIFO sequence
    ///
    /// The origin route must be persisted by the caller before it sends this request
    pub(crate) fn accept_resource_request(
        &mut self,
        authority_machine: MachineId,
        request_id: RequestId,
        task_id: TaskId,
        resource_id: ResourceId,
        origin_machine: MachineId,
        normalized_spec: NormalizedSpec,
    ) -> Result<ResourceRequest, ResourceStoreError> {
        accept_request_for_authority(
            &mut self.conn,
            authority_machine,
            request_id,
            task_id,
            resource_id,
            origin_machine,
            normalized_spec,
        )
    }

    /// Accept one selected resource request as a task on this store connection
    pub(crate) fn accept_assigned_resource_task(
        &mut self,
        input: ResourceTaskAcceptanceInput,
    ) -> Result<ResourceTaskAcceptance, ResourceStoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let request = assigned_resource_request_for_acceptance(&tx, &input)?;
        let normalized_spec = request.spec().as_normalized();
        let executor = executor_identity_on(&tx, input.task_id)?;
        let task_row = crate::store::task_by_id_on(&tx, input.task_id)?;

        validate_resource_task_route(
            &tx,
            &request,
            input.authority_machine,
            normalized_spec,
            executor.is_some(),
        )?;

        if let Some(executor) = executor {
            let ExecutorIdentity::Accepted(record) = executor else {
                return Err(conflict(ConflictReason::ExecutorIdentityMismatch));
            };
            let same_env = task_row
                .as_ref()
                .is_some_and(|row| row.env == input.executor_env);
            if !same_env
                || !accepted_task_matches_request(
                    &tx,
                    &record,
                    &request,
                    input.authority_machine,
                    task_row.as_ref(),
                )?
            {
                return Err(conflict(ConflictReason::ExecutorIdentityMismatch));
            }

            tx.commit()?;
            return Ok(ResourceTaskAcceptance::Existing {
                task: input.task_id,
                state: record.state,
            });
        }

        let first_event_saved = crate::store::events::initial_queued_event_matches_on(
            &tx,
            input.task_id,
            request.origin_machine,
            input.authority_machine,
        )?;
        if task_row.is_some()
            || task_has_any_event(&tx, input.task_id)?
            || first_event_saved
            || crate::store::release_watcher_task_id_is_reserved(&tx, input.task_id)?
        {
            return Err(conflict(ConflictReason::TaskIdentityInUse));
        }

        let row = new_resource_task_row(&input, normalized_spec)?;
        let project_root = crate::store::find_project_root(&row.cwd);
        crate::store::insert_task_with_project_root_on(&tx, &row, project_root.as_deref())
            .map_err(ResourceStoreError::TaskPreparation)?;

        let identity = ExecutorIdentity::Accepted(ExecutionRecord {
            task: input.task_id,
            origin_machine: request.origin_machine,
            execution_machine: input.authority_machine,
            spec: normalized_spec.clone().into(),
            state: ProcessStatus::Queued,
        });
        tx.execute(
            "INSERT INTO executor_identities (task_id,origin_machine,identity_json)
             VALUES (?1,?2,?3)",
            rusqlite::params![
                input.task_id.to_string(),
                request.origin_machine.to_string(),
                encode_resource_json(&identity)?,
            ],
        )?;
        crate::store::events::append_produced_event_on(
            &tx,
            input.task_id,
            EventPayload::State {
                status: ProcessStatus::Queued,
            },
        )?;

        tx.commit()?;
        Ok(ResourceTaskAcceptance::Inserted {
            task: input.task_id,
        })
    }

    /// Reconcile one exact Serving task and commit completion only with release evidence
    pub(crate) fn reconcile_assigned_resource_task_for_authority(
        &mut self,
        input: AssignedResourceTaskReconcileInput,
    ) -> Result<AssignedResourceTaskReconcileOutcome, ResourceStoreError> {
        persist_assigned_task_reconciliation(&mut self.conn, input)
    }
}

/// Build the queued task row for a first acceptance on this executor
///
/// A bare program name resolves from the executor PATH only here, so the resolved
/// entry point must pass the foreground contract before any row exists. A
/// container runs through the resolved `docker` CLI, which Homebased drives
/// itself, so its mount sources are checked instead
fn new_resource_task_row(
    input: &ResourceTaskAcceptanceInput,
    normalized_spec: &NormalizedSpec,
) -> Result<TaskRow, ResourceStoreError> {
    crate::spec::check_cwd(&normalized_spec.cwd).map_err(ResourceStoreError::TaskPreparation)?;
    crate::spec::check_workload_host(&normalized_spec.workload)
        .map_err(ResourceStoreError::TaskPreparation)?;
    let workload = crate::invocation::persist_workload(&normalized_spec.workload);
    let binary = crate::invocation::resolve_workload_binary(
        &normalized_spec.workload,
        &input.executor_env.path,
        &normalized_spec.cwd,
    )
    .map_err(ResourceStoreError::TaskPreparation)?;
    if !matches!(normalized_spec.workload, NormalizedWorkload::Container(_)) {
        crate::resource::foreground::inspect_foreground_entry_point(&binary)
            .map_err(|risk| ResourceStoreError::UnsupportedCommandOwnership { risk })?;
    }
    let row = crate::store::new_queued_task(NewTask {
        id: input.task_id,
        name: Some(normalized_spec.name.clone()),
        thread: normalized_spec.thread,
        workload,
        cwd: normalized_spec.cwd.clone(),
        timeout: normalized_spec.timeout,
        env: input.executor_env.clone(),
        binary,
    });
    if !resource_task_row_matches(&row, input.task_id, normalized_spec) {
        return Err(conflict(ConflictReason::TaskRowMismatch));
    }

    Ok(row)
}

/// Check an accepted identity, its origin column, first event, and task row against one request
fn accepted_task_matches_request(
    conn: &Connection,
    record: &ExecutionRecord,
    request: &ResourceRequest,
    authority_machine: MachineId,
    task_row: Option<&TaskRow>,
) -> Result<bool, ResourceStoreError> {
    let task_id = request.task_id;
    let spec = request.spec().as_normalized();
    Ok(
        record.is_owned_by(task_id, request.origin_machine, authority_machine)
            && record.current_spec() == Some(spec)
            && identity_origin_column_is_on(conn, task_id, request.origin_machine)?
            && task_has_any_event(conn, task_id)?
            && crate::store::events::initial_queued_event_matches_on(
                conn,
                task_id,
                request.origin_machine,
                authority_machine,
            )?
            && task_row.is_some_and(|row| {
                resource_task_row_matches(row, task_id, spec) && row.status() == record.state
            }),
    )
}

fn loan_is_reserved(
    conn: &Connection,
    loan_id: crate::resource::LoanId,
    resource_id: ResourceId,
) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM loans
            WHERE id=?1 AND resource_id=?2
              AND json_extract(state_json, '$.type') != 'closed'
        )",
        rusqlite::params![
            loan_id.as_uuid().to_string(),
            resource_id.as_uuid().to_string()
        ],
        |row| row.get(0),
    )
}

/// Require the origin route that a local origin saved before sending its request
///
/// A remote origin keeps its route on its own machine, so none may exist here
fn validate_resource_task_route(
    conn: &Connection,
    request: &ResourceRequest,
    authority: MachineId,
    spec: &NormalizedSpec,
    already_accepted: bool,
) -> Result<(), ResourceStoreError> {
    let route_conflict = || conflict(ConflictReason::OriginRouteMismatch);
    let by_request = origin_route_by_request_on(conn, request.request_id)?;
    let by_task = origin_route_by_task_on(conn, request.task_id)?;

    if request.origin_machine != authority {
        return if by_request.is_none() && by_task.is_none() {
            Ok(())
        } else {
            Err(route_conflict())
        };
    }

    let (by_request, by_task) = match (by_request, by_task) {
        (Some(by_request), Some(by_task)) => (by_request, by_task),
        (None, None) => {
            return Err(ResourceStoreError::OriginRouteNotFound {
                task: request.task_id,
            });
        }
        _ => return Err(route_conflict()),
    };
    if by_request != by_task {
        return Err(route_conflict());
    }

    let SubmissionState::Resource { resource, phase } = &by_request.submission else {
        return Err(route_conflict());
    };
    if by_request.request != request.request_id
        || by_request.task != request.task_id
        || by_request.origin_machine != request.origin_machine
        || by_request.execution_machine != authority
        || by_request.thread != spec.thread
        || *resource != request.resource_id
        || by_request.current_spec() != Some(spec)
    {
        return Err(route_conflict());
    }

    match phase {
        ResourceRoutePhase::AcceptanceUnknown | ResourceRoutePhase::Waiting => Ok(()),
        ResourceRoutePhase::Activated if already_accepted => Ok(()),
        ResourceRoutePhase::CancelledBeforeLaunch if !already_accepted => {
            Err(ResourceStoreError::Prevented)
        }
        ResourceRoutePhase::Activated
        | ResourceRoutePhase::CancelledBeforeLaunch
        | ResourceRoutePhase::Rejected { .. } => Err(route_conflict()),
    }
}
