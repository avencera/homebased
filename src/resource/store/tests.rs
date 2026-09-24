//! Authority store behavior against an in-memory schema

use std::path::Path;

use rusqlite::{Connection, TransactionBehavior, params};
use serde_json::json;
use tempfile::tempdir;
use uuid::Uuid;

use super::assigned_task::{
    AssignedResourceTaskAttention, ResourceTaskReleaseProof, resource_task_release_proof,
};
use super::cancellation::QueueCancellationResult;
use super::error::{ConflictReason, ResourceStoreError};
use super::notice::{
    INTERRUPTED_DELIVERY_ERROR, RETARGETED_DELIVERY_ERROR, SupervisorNoticeStoreError,
    insert_supervisor_notice_in_transaction, pending_supervisor_notices,
    recover_sending_supervisor_notices, reserve_supervisor_notice_attempt,
    select_supervisor_notice_record_by_action, settle_supervisor_notice_attempt, supervisor_notice,
};
use super::queue::{
    accept_request_for_authority, executor_identity_exists, next_queued_request_for_authority,
    requests_for_resource_for_authority,
};
use super::release_loan::{
    OpenReleaseLoanError, OpenReleaseLoanResult, reconcile_resource_queue_for_authority,
};
use super::resources::{register_resource_for_authority, resources_for_authority};
use super::rows::{select_non_closed_loan, select_request_by_id, select_resource};
use super::test_support::{
    accept_request, cancel_request_before_activation, install_schema, next_queued_request,
    register_resource, requests_for_resource,
};
use super::test_support::{
    cancel_request_before_activation_for_authority, open_release_loan_for_authority,
    retarget_supervisor_notice,
};
use crate::domain::{ExitReason, TaskId, ThreadId, WorkExitEvidence};
use crate::machine::{MachineId, MachineName};
use crate::resource::{
    AcceptanceSequence, ActionId, AssignmentRevision, CommandSpecError, DeliveryAttemptId, Loan,
    LoanClosure, LoanId, LoanPhase, LoanState, NoticeId, Resource, ResourceId,
    ResourceQueueAttentionReason, ResourceQueueReconcileOutcome, ResourceRequest,
    ResourceRequestState, ResourceRevision, ResourceTaskOwnershipRisk, ReturnContext,
    SupervisorAddress, SupervisorNotice, SupervisorNoticeDelivery, SupervisorNoticePayload,
};
use crate::spec::NormalizedSpec;
use crate::submission::{ExecutionRecord, ExecutorIdentity, RejectionTombstone, RequestId};

fn connection() -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();
    install_schema(&mut conn).unwrap();
    conn.execute_batch(
        "CREATE TABLE tasks (
             id TEXT PRIMARY KEY,
             status TEXT NOT NULL DEFAULT 'queued'
         );
         CREATE TABLE executor_identities (
             task_id TEXT PRIMARY KEY,
             origin_machine TEXT NOT NULL,
             identity_json TEXT NOT NULL
         );
         CREATE TABLE origin_routes (
             request_id TEXT PRIMARY KEY,
             task_id TEXT NOT NULL UNIQUE,
             execution_machine TEXT NOT NULL,
             spec_json TEXT NOT NULL,
             route_json TEXT NOT NULL
         );",
    )
    .unwrap();
    conn
}

fn resource() -> Resource {
    resource_for_authority(MachineId::new())
}

fn resource_for_authority(authority: MachineId) -> Resource {
    Resource::new(
        ResourceId::new(),
        "gpu-0".into(),
        authority,
        SupervisorAddress {
            machine: authority,
            thread: ThreadId(Uuid::now_v7()),
        },
        AssignmentRevision::new(0),
        ResourceRevision::new(0),
        None,
    )
}

fn resource_with_background_task(task_id: TaskId) -> Resource {
    let mut resource = resource();
    resource.registered_background_task = Some(task_id);
    resource
}

fn queue_request(conn: &mut Connection, resource_id: ResourceId) -> ResourceRequest {
    accept_request(
        conn,
        RequestId::new(),
        TaskId::new(),
        resource_id,
        MachineId::new(),
        command_spec(&["echo", "queued"]),
    )
    .unwrap()
}

fn insert_local_task_status(conn: &Connection, task_id: TaskId, status: &str) {
    conn.execute(
        "INSERT INTO tasks (id, status) VALUES (?1, ?2)",
        params![task_id.to_string(), status],
    )
    .unwrap();
}

fn command_spec(command: &[&str]) -> NormalizedSpec {
    serde_json::from_value(json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "gpu command",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": { "type": "task", "command": command }
    }))
    .unwrap()
}

#[test]
fn no_child_evidence_proves_release_only_for_pre_spawn_outcomes() {
    let request = ResourceRequest::new(
        RequestId::new(),
        TaskId::new(),
        ResourceId::new(),
        AcceptanceSequence::new(1),
        MachineId::new(),
        command_spec(&["ssh", "gpu-host"]),
    )
    .unwrap();
    let spawn_failed = ExitReason::SpawnFailed {
        message: "fake spawn failure".into(),
    };
    // no child ran, so the command shape cannot have left work behind
    assert_eq!(
        resource_task_release_proof(&request, &spawn_failed, WorkExitEvidence::NoWorkStarted),
        Ok(ResourceTaskReleaseProof::NoChildSpawnedAfterSpawnFailure)
    );
    assert_eq!(
        resource_task_release_proof(
            &request,
            &ExitReason::Cancelled,
            WorkExitEvidence::NoWorkStarted
        ),
        Ok(ResourceTaskReleaseProof::NoChildSpawnedAfterQueuedCancel)
    );
    for outcome in [
        ExitReason::Exit { code: 0 },
        ExitReason::Signal { signal: 9 },
    ] {
        assert_eq!(
            resource_task_release_proof(&request, &outcome, WorkExitEvidence::NoWorkStarted),
            Err(AssignedResourceTaskAttention::InvalidNoChildSpawnEvidence)
        );
    }
}

#[test]
fn assigned_task_release_proof_rejects_commands_that_can_outlive_the_group() {
    for (command, risk) in [
        (
            &["ssh", "gpu-host"][..],
            ResourceTaskOwnershipRisk::RemoteShell,
        ),
        (
            &["docker", "run"][..],
            ResourceTaskOwnershipRisk::ContainerClient,
        ),
        (
            &["sh", "-c", "bench"][..],
            ResourceTaskOwnershipRisk::ShellWrapper,
        ),
        (
            &["setsid", "bench"][..],
            ResourceTaskOwnershipRisk::DetachedLauncher,
        ),
        // wrappers whose own exit hides a detached container or other launch
        (
            &["env", "docker", "run", "-d", "gpu"][..],
            ResourceTaskOwnershipRisk::ProgramLauncher,
        ),
        (
            &["sudo", "/opt/gpu/bench"][..],
            ResourceTaskOwnershipRisk::ProgramLauncher,
        ),
        (
            &["timeout", "1h", "bench"][..],
            ResourceTaskOwnershipRisk::ProgramLauncher,
        ),
        (
            &["/opt/tools/runner", "docker", "run", "-d"][..],
            ResourceTaskOwnershipRisk::ContainerClient,
        ),
    ] {
        // a request saved before the contract existed still cannot release
        let request = ResourceRequest::new(
            RequestId::new(),
            TaskId::new(),
            ResourceId::new(),
            AcceptanceSequence::new(1),
            MachineId::new(),
            command_spec(command),
        )
        .unwrap();
        assert_eq!(
            resource_task_release_proof(
                &request,
                &ExitReason::Exit { code: 0 },
                WorkExitEvidence::ProcessGroupExited,
            ),
            Err(AssignedResourceTaskAttention::OwnershipUncertain(risk))
        );
    }
}

fn agent_spec() -> NormalizedSpec {
    serde_json::from_value(json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "agent request",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": {
            "type": "agent",
            "agent": "codex",
            "prompt": "run this agent",
            "report_trailer": true
        }
    }))
    .unwrap()
}

fn request_id_at(timestamp_ms: u128) -> RequestId {
    RequestId(Uuid::from_u128(
        (timestamp_ms << 80) | (0x7 << 76) | (0x2 << 62),
    ))
}

fn insert_loan(
    conn: &Connection,
    loan_id: LoanId,
    resource_id: ResourceId,
    state: LoanState,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
        params![
            loan_id.as_uuid().to_string(),
            resource_id.as_uuid().to_string(),
            serde_json::to_string(&state).unwrap(),
        ],
    )?;
    Ok(())
}

fn assign_request_to_serving_loan(
    conn: &Connection,
    request: &ResourceRequest,
    return_context: ReturnContext,
) -> Loan {
    let loan_id = LoanId::new();
    let assigned_state = ResourceRequestState::Assigned { loan_id };
    conn.execute(
        "UPDATE resource_requests SET state_json = ?1 WHERE request_id = ?2",
        params![
            serde_json::to_string(&assigned_state).unwrap(),
            request.request_id.0.to_string(),
        ],
    )
    .unwrap();
    let loan = Loan {
        id: loan_id,
        resource_id: request.resource_id,
        state: LoanState::Active {
            phase: LoanPhase::Serving {
                return_context,
                current_request_id: request.request_id,
                release_provenance: crate::store::unreceipted_release_provenance(),
            },
        },
    };
    insert_loan(conn, loan.id, loan.resource_id, loan.state.clone()).unwrap();
    loan
}

fn insert_accepted_executor_identity(
    conn: &Connection,
    request: &ResourceRequest,
    authority: MachineId,
) {
    let identity = ExecutorIdentity::Accepted(ExecutionRecord {
        task: request.task_id,
        origin_machine: request.origin_machine,
        execution_machine: authority,
        spec: request.spec().as_normalized().clone().into(),
        state: crate::domain::ProcessStatus::Queued,
    });
    conn.execute(
        "INSERT INTO executor_identities (task_id, origin_machine, identity_json)
         VALUES (?1, ?2, ?3)",
        params![
            request.task_id.to_string(),
            request.origin_machine.as_uuid().to_string(),
            serde_json::to_string(&identity).unwrap(),
        ],
    )
    .unwrap();
}

fn connection_at(path: &Path) -> Connection {
    let mut conn = Connection::open(path).unwrap();
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();
    install_schema(&mut conn).unwrap();
    conn
}

fn persist_notice_for_test(
    conn: &mut Connection,
    notice: &SupervisorNotice,
) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let saved = insert_supervisor_notice_in_transaction(&tx, notice)?;
    tx.commit()?;
    Ok(saved)
}

fn insert_notice_fixture(conn: &mut Connection) -> SupervisorNotice {
    let resource = resource();
    register_resource(conn, &resource).unwrap();
    let loan_id = LoanId::new();
    let action_id = ActionId::new();
    let task_id = TaskId::new();
    let state = LoanState::Active {
        phase: LoanPhase::AwaitingRelease {
            action_id,
            observed_background_task: task_id,
            watcher_intent: None,
        },
    };
    insert_loan(conn, loan_id, resource.id, state).unwrap();

    SupervisorNotice {
        id: NoticeId::new(),
        loan_id,
        action_id,
        state_revision: resource.state_revision,
        destination: resource.supervisor,
        assignment_revision: resource.assignment_revision,
        payload: SupervisorNoticePayload::ReleaseRequired { task_id },
        delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
    }
}

#[test]
fn acceptance_sequence_is_not_request_uuid_time() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let origin = MachineId::new();
    let earlier_uuid = request_id_at(1);
    let later_uuid = request_id_at(2);

    let accepted_later_uuid = accept_request(
        &mut conn,
        later_uuid,
        TaskId::new(),
        resource.id,
        origin,
        command_spec(&["echo", "later uuid"]),
    )
    .unwrap();
    let accepted_earlier_uuid = accept_request(
        &mut conn,
        earlier_uuid,
        TaskId::new(),
        resource.id,
        origin,
        command_spec(&["echo", "earlier uuid"]),
    )
    .unwrap();

    assert!(earlier_uuid.0 < later_uuid.0);
    assert!(accepted_later_uuid.acceptance_sequence < accepted_earlier_uuid.acceptance_sequence);
    let serving_order = requests_for_resource(&conn, resource.id).unwrap();
    assert_eq!(serving_order[0].request_id, later_uuid);
    assert_eq!(serving_order[1].request_id, earlier_uuid);
    assert_eq!(
        next_queued_request(&conn, resource.id)
            .unwrap()
            .unwrap()
            .request_id,
        later_uuid
    );
}

#[test]
fn cancellation_before_acceptance_prevents_a_delayed_acceptance() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let request_id = RequestId::new();
    let task_id = TaskId::new();
    let origin = MachineId::new();

    assert!(matches!(
        cancel_request_before_activation(&mut conn, request_id, task_id, resource.id, origin,),
        Ok(QueueCancellationResult::PreventedBeforeAcceptance)
    ));
    assert!(matches!(
        cancel_request_before_activation(&mut conn, request_id, task_id, resource.id, origin,),
        Ok(QueueCancellationResult::PreventedBeforeAcceptance)
    ));
    assert!(matches!(
        accept_request(
            &mut conn,
            request_id,
            task_id,
            resource.id,
            origin,
            command_spec(&["echo", "delayed"]),
        ),
        Err(ResourceStoreError::Prevented)
    ));

    let request_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM resource_requests", [], |row| {
            row.get(0)
        })
        .unwrap();
    let prevention_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_request_preventions",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(request_count, 0);
    assert_eq!(prevention_count, 1);

    let first_accepted = accept_request(
        &mut conn,
        RequestId::new(),
        TaskId::new(),
        resource.id,
        origin,
        command_spec(&["echo", "next"]),
    )
    .unwrap();
    assert_eq!(
        first_accepted.acceptance_sequence,
        AcceptanceSequence::new(1)
    );
}

#[test]
fn cancellation_after_acceptance_retains_one_cancelled_row_and_its_sequence() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let request_id = RequestId::new();
    let task_id = TaskId::new();
    let origin = MachineId::new();
    let spec = command_spec(&["echo", "queued"]);
    let accepted = accept_request(
        &mut conn,
        request_id,
        task_id,
        resource.id,
        origin,
        spec.clone(),
    )
    .unwrap();

    let first_cancel =
        cancel_request_before_activation(&mut conn, request_id, task_id, resource.id, origin)
            .unwrap();
    let QueueCancellationResult::Request(cancelled) = first_cancel else {
        panic!("an accepted request must retain its queue row");
    };
    assert!(matches!(
        cancelled.state,
        ResourceRequestState::CancelledBeforeLaunch
    ));
    assert_eq!(cancelled.acceptance_sequence, accepted.acceptance_sequence);

    let retry =
        cancel_request_before_activation(&mut conn, request_id, task_id, resource.id, origin)
            .unwrap();
    let QueueCancellationResult::Request(retried) = retry else {
        panic!("a repeated cancellation must return the saved request");
    };
    assert_eq!(retried.acceptance_sequence, accepted.acceptance_sequence);
    assert!(matches!(
        retried.state,
        ResourceRequestState::CancelledBeforeLaunch
    ));

    let accepted_retry =
        accept_request(&mut conn, request_id, task_id, resource.id, origin, spec).unwrap();
    assert_eq!(
        accepted_retry.acceptance_sequence,
        accepted.acceptance_sequence
    );
    assert!(matches!(
        accepted_retry.state,
        ResourceRequestState::CancelledBeforeLaunch
    ));
    assert_eq!(requests_for_resource(&conn, resource.id).unwrap().len(), 1);
    assert!(next_queued_request(&conn, resource.id).unwrap().is_none());

    let prevention_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_request_preventions",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(prevention_count, 0);
}

#[test]
fn cancellation_rejects_reuse_of_either_prevented_identity() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let request_id = RequestId::new();
    let task_id = TaskId::new();
    let origin = MachineId::new();
    cancel_request_before_activation(&mut conn, request_id, task_id, resource.id, origin).unwrap();

    assert!(matches!(
        cancel_request_before_activation(&mut conn, request_id, TaskId::new(), resource.id, origin,),
        Err(ResourceStoreError::Conflict(
            ConflictReason::PreventionIdentityMismatch
        ))
    ));
    assert!(matches!(
        cancel_request_before_activation(&mut conn, RequestId::new(), task_id, resource.id, origin,),
        Err(ResourceStoreError::Conflict(
            ConflictReason::PreventionIdentityMismatch
        ))
    ));
    assert!(matches!(
        cancel_request_before_activation(&mut conn, request_id, task_id, ResourceId::new(), origin,),
        Err(ResourceStoreError::Conflict(
            ConflictReason::PreventionIdentityMismatch
        ))
    ));
    assert!(matches!(
        cancel_request_before_activation(
            &mut conn,
            request_id,
            task_id,
            resource.id,
            MachineId::new(),
        ),
        Err(ResourceStoreError::Conflict(
            ConflictReason::PreventionIdentityMismatch
        ))
    ));
    assert!(matches!(
        accept_request(
            &mut conn,
            request_id,
            TaskId::new(),
            resource.id,
            origin,
            command_spec(&["echo", "conflict"]),
        ),
        Err(ResourceStoreError::Conflict(
            ConflictReason::PreventionIdentityMismatch
        ))
    ));
    assert!(matches!(
        accept_request(
            &mut conn,
            RequestId::new(),
            task_id,
            resource.id,
            origin,
            command_spec(&["echo", "conflict"]),
        ),
        Err(ResourceStoreError::Conflict(
            ConflictReason::PreventionIdentityMismatch
        ))
    ));
    assert!(matches!(
        accept_request(
            &mut conn,
            request_id,
            task_id,
            ResourceId::new(),
            origin,
            command_spec(&["echo", "conflict"]),
        ),
        Err(ResourceStoreError::Conflict(
            ConflictReason::PreventionIdentityMismatch
        ))
    ));
    assert!(matches!(
        accept_request(
            &mut conn,
            request_id,
            task_id,
            resource.id,
            MachineId::new(),
            command_spec(&["echo", "conflict"]),
        ),
        Err(ResourceStoreError::Conflict(
            ConflictReason::PreventionIdentityMismatch
        ))
    ));

    let request_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM resource_requests", [], |row| {
            row.get(0)
        })
        .unwrap();
    let prevention_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_request_preventions",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(request_count, 0);
    assert_eq!(prevention_count, 1);
}

#[test]
fn queue_selection_skips_a_cancelled_request() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let origin = MachineId::new();
    let cancelled = accept_request(
        &mut conn,
        RequestId::new(),
        TaskId::new(),
        resource.id,
        origin,
        command_spec(&["echo", "cancelled"]),
    )
    .unwrap();
    let ready = accept_request(
        &mut conn,
        RequestId::new(),
        TaskId::new(),
        resource.id,
        origin,
        command_spec(&["echo", "ready"]),
    )
    .unwrap();

    cancel_request_before_activation(
        &mut conn,
        cancelled.request_id,
        cancelled.task_id,
        cancelled.resource_id,
        cancelled.origin_machine,
    )
    .unwrap();

    let selected = next_queued_request(&conn, resource.id).unwrap().unwrap();
    assert_eq!(selected.request_id, ready.request_id);
    assert_eq!(selected.acceptance_sequence, ready.acceptance_sequence);
    let saved = requests_for_resource(&conn, resource.id).unwrap();
    assert_eq!(saved.len(), 2);
    assert!(matches!(
        saved[0].state,
        ResourceRequestState::CancelledBeforeLaunch
    ));
    assert!(matches!(saved[1].state, ResourceRequestState::Queued));
}

#[test]
fn cancellation_preserves_assigned_state_when_executor_won_and_terminal_states() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let origin = MachineId::new();
    let assigned = accept_request(
        &mut conn,
        RequestId::new(),
        TaskId::new(),
        resource.id,
        origin,
        command_spec(&["echo", "assigned"]),
    )
    .unwrap();
    let loan = assign_request_to_serving_loan(
        &conn,
        &assigned,
        ReturnContext::AlreadyCompleted {
            task_id: TaskId::new(),
            result_ref: "saved-result".into(),
        },
    );
    insert_accepted_executor_identity(&conn, &assigned, resource.authority_machine());

    let result = cancel_request_before_activation(
        &mut conn,
        assigned.request_id,
        assigned.task_id,
        assigned.resource_id,
        assigned.origin_machine,
    );
    assert!(matches!(
        result,
        Err(ResourceStoreError::ExecutorAlreadyAccepted { task }) if task == assigned.task_id
    ));
    let saved_assigned = select_request_by_id(&conn, assigned.request_id)
        .unwrap()
        .unwrap();
    assert!(matches!(
        saved_assigned.state,
        ResourceRequestState::Assigned { loan_id: saved } if saved == loan.id
    ));
    assert_eq!(
        select_non_closed_loan(&conn, resource.id).unwrap(),
        Some(loan.clone())
    );
    assert_eq!(
        select_resource(&conn, resource.id)
            .unwrap()
            .unwrap()
            .state_revision,
        resource.state_revision
    );
    conn.execute(
        "DELETE FROM executor_identities WHERE task_id = ?1",
        [assigned.task_id.to_string()],
    )
    .unwrap();
    insert_local_task_status(&conn, assigned.task_id, "queued");
    assert!(matches!(
        cancel_request_before_activation(
            &mut conn,
            assigned.request_id,
            assigned.task_id,
            assigned.resource_id,
            assigned.origin_machine,
        ),
        Err(ResourceStoreError::Conflict(
            ConflictReason::TaskIdentityInUse
        ))
    ));
    assert!(matches!(
        select_request_by_id(&conn, assigned.request_id)
            .unwrap()
            .unwrap()
            .state,
        ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
    ));
    assert_eq!(
        select_non_closed_loan(&conn, resource.id).unwrap(),
        Some(loan.clone())
    );
    let notice_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(notice_count, 0);

    let finished = accept_request(
        &mut conn,
        RequestId::new(),
        TaskId::new(),
        resource.id,
        origin,
        command_spec(&["echo", "finished"]),
    )
    .unwrap();
    let finished_state = ResourceRequestState::Finished {
        outcome: ExitReason::Cancelled,
    };
    conn.execute(
        "UPDATE resource_requests SET state_json = ?1 WHERE request_id = ?2",
        params![
            serde_json::to_string(&finished_state).unwrap(),
            finished.request_id.0.to_string(),
        ],
    )
    .unwrap();

    let result = cancel_request_before_activation(
        &mut conn,
        finished.request_id,
        finished.task_id,
        finished.resource_id,
        finished.origin_machine,
    )
    .unwrap();
    let QueueCancellationResult::Request(saved_finished) = result else {
        panic!("a terminal request must remain in the queue store");
    };
    assert!(matches!(
        saved_finished.state,
        ResourceRequestState::Finished {
            outcome: ExitReason::Cancelled
        }
    ));
}

#[test]
fn assigned_cancellation_assigns_next_queued_request_to_the_same_loan() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let cancelled = queue_request(&mut conn, resource.id);
    let next = queue_request(&mut conn, resource.id);
    let later = queue_request(&mut conn, resource.id);
    let return_context = ReturnContext::Stopped {
        task_id: TaskId::new(),
        checkpoint_ref: "checkpoint-9".into(),
        recovery_ref: "recovery-9".into(),
    };
    let loan = assign_request_to_serving_loan(&conn, &cancelled, return_context.clone());
    let LoanState::Active {
        phase: LoanPhase::Serving {
            release_provenance, ..
        },
    } = &loan.state
    else {
        panic!("the fixture loan must be Serving");
    };
    let release_provenance = release_provenance.clone();

    let result = cancel_request_before_activation_for_authority(
        &mut conn,
        resource.authority_machine(),
        cancelled.request_id,
        cancelled.task_id,
        cancelled.resource_id,
        cancelled.origin_machine,
    )
    .unwrap();
    let QueueCancellationResult::Request(cancelled) = result else {
        panic!("an assigned request must retain its row");
    };
    assert!(matches!(
        cancelled.state,
        ResourceRequestState::CancelledBeforeLaunch
    ));

    let saved = requests_for_resource(&conn, resource.id).unwrap();
    assert!(matches!(
        saved[1].state,
        ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
    ));
    assert_eq!(saved[1].request_id, next.request_id);
    assert!(matches!(saved[2].state, ResourceRequestState::Queued));
    assert_eq!(saved[2].request_id, later.request_id);
    let saved_loan = select_non_closed_loan(&conn, resource.id).unwrap().unwrap();
    assert_eq!(
        saved_loan.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                return_context,
                current_request_id: next.request_id,
                release_provenance,
            },
        }
    );
    assert_eq!(saved_loan.id, loan.id);
    assert_eq!(
        select_resource(&conn, resource.id)
            .unwrap()
            .unwrap()
            .state_revision,
        ResourceRevision::new(1)
    );
    let identity_json: String = conn
        .query_row(
            "SELECT identity_json FROM executor_identities WHERE task_id = ?1",
            [cancelled.task_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(matches!(
        serde_json::from_str::<ExecutorIdentity>(&identity_json).unwrap(),
        ExecutorIdentity::Rejected(RejectionTombstone { task, .. }) if task == cancelled.task_id
    ));
}

#[test]
fn assigned_cancellation_with_empty_queue_persists_return_notice() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let assigned = queue_request(&mut conn, resource.id);
    let return_context = ReturnContext::AlreadyCompleted {
        task_id: TaskId::new(),
        result_ref: "final-result-51".into(),
    };
    let loan = assign_request_to_serving_loan(&conn, &assigned, return_context.clone());

    let result = cancel_request_before_activation_for_authority(
        &mut conn,
        resource.authority_machine(),
        assigned.request_id,
        assigned.task_id,
        assigned.resource_id,
        assigned.origin_machine,
    )
    .unwrap();
    assert!(matches!(
        result,
        QueueCancellationResult::Request(request)
            if matches!(request.state, ResourceRequestState::CancelledBeforeLaunch)
    ));

    let saved_loan = select_non_closed_loan(&conn, resource.id).unwrap().unwrap();
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingReturn {
                action_id,
                return_context: saved_context,
            },
    } = saved_loan.state
    else {
        panic!("an empty queue must reserve the return decision");
    };
    assert_eq!(saved_loan.id, loan.id);
    assert_eq!(saved_context, return_context);
    assert_eq!(
        select_resource(&conn, resource.id)
            .unwrap()
            .unwrap()
            .state_revision,
        ResourceRevision::new(1)
    );
    let notice = select_supervisor_notice_record_by_action(&conn, action_id)
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(notice.loan_id, loan.id);
    assert_eq!(notice.action_id, action_id);
    assert_eq!(notice.state_revision, ResourceRevision::new(1));
    assert_eq!(notice.destination, resource.supervisor);
    assert_eq!(notice.assignment_revision, resource.assignment_revision);
    assert_eq!(
        notice.payload,
        SupervisorNoticePayload::ReturnRequired { return_context }
    );
    assert_eq!(
        notice.delivery,
        SupervisorNoticeDelivery::Pending { attempts: 0 }
    );
}

#[test]
fn retrying_assigned_cancellation_does_not_advance_queue_or_add_a_notice() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let assigned = queue_request(&mut conn, resource.id);
    let return_context = ReturnContext::AlreadyCompleted {
        task_id: TaskId::new(),
        result_ref: "final-result-52".into(),
    };
    assign_request_to_serving_loan(&conn, &assigned, return_context);
    let first = cancel_request_before_activation(
        &mut conn,
        assigned.request_id,
        assigned.task_id,
        assigned.resource_id,
        assigned.origin_machine,
    )
    .unwrap();
    let QueueCancellationResult::Request(first_request) = first else {
        panic!("an assigned request must retain its row");
    };
    assert!(matches!(
        first_request.state,
        ResourceRequestState::CancelledBeforeLaunch
    ));
    let first_loan = select_non_closed_loan(&conn, resource.id).unwrap().unwrap();
    let first_notice_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(first_notice_count, 1);

    let later = queue_request(&mut conn, resource.id);
    let retry = cancel_request_before_activation(
        &mut conn,
        assigned.request_id,
        assigned.task_id,
        assigned.resource_id,
        assigned.origin_machine,
    )
    .unwrap();
    let QueueCancellationResult::Request(retried_request) = retry else {
        panic!("a repeated cancellation must retain the saved row");
    };
    assert!(matches!(
        retried_request.state,
        ResourceRequestState::CancelledBeforeLaunch
    ));
    assert_eq!(
        select_non_closed_loan(&conn, resource.id).unwrap().unwrap(),
        first_loan
    );
    assert!(matches!(
        select_request_by_id(&conn, later.request_id)
            .unwrap()
            .unwrap()
            .state,
        ResourceRequestState::Queued
    ));
    let notice_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(notice_count, 1);
    assert_eq!(
        select_resource(&conn, resource.id)
            .unwrap()
            .unwrap()
            .state_revision,
        ResourceRevision::new(1)
    );
}

#[test]
fn assigned_cancellation_refuses_a_loan_needing_attention() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let assigned = queue_request(&mut conn, resource.id);
    let return_context = ReturnContext::AlreadyCompleted {
        task_id: TaskId::new(),
        result_ref: "final-result-53".into(),
    };
    let loan = assign_request_to_serving_loan(&conn, &assigned, return_context.clone());
    let attention_state = LoanState::NeedsAttention {
        action_id: ActionId::new(),
        last_safe_phase: LoanPhase::Serving {
            return_context,
            current_request_id: assigned.request_id,
            release_provenance: crate::store::unreceipted_release_provenance(),
        },
        reason: "executor state is uncertain".into(),
    };
    conn.execute(
        "UPDATE loans SET state_json = ?1 WHERE id = ?2",
        params![
            serde_json::to_string(&attention_state).unwrap(),
            loan.id.as_uuid().to_string(),
        ],
    )
    .unwrap();

    assert!(matches!(
        cancel_request_before_activation(
            &mut conn,
            assigned.request_id,
            assigned.task_id,
            assigned.resource_id,
            assigned.origin_machine,
        ),
        Err(ResourceStoreError::Conflict(ConflictReason::LoanChanged))
    ));
    assert!(matches!(
        select_request_by_id(&conn, assigned.request_id)
            .unwrap()
            .unwrap()
            .state,
        ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
    ));
    assert_eq!(
        select_non_closed_loan(&conn, resource.id)
            .unwrap()
            .unwrap()
            .state,
        attention_state
    );
    assert!(!executor_identity_exists(&conn, assigned.task_id).unwrap());
    assert_eq!(
        select_resource(&conn, resource.id)
            .unwrap()
            .unwrap()
            .state_revision,
        resource.state_revision
    );
}

#[test]
fn identical_retry_returns_saved_request_after_its_state_changes() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let request_id = RequestId::new();
    let task_id = TaskId::new();
    let origin = MachineId::new();
    let spec = command_spec(&["echo", "hello"]);
    let first = accept_request(
        &mut conn,
        request_id,
        task_id,
        resource.id,
        origin,
        spec.clone(),
    )
    .unwrap();
    let changed_state = ResourceRequestState::Assigned {
        loan_id: LoanId::new(),
    };
    conn.execute(
        "UPDATE resource_requests SET state_json = ?1 WHERE request_id = ?2",
        params![
            serde_json::to_string(&changed_state).unwrap(),
            request_id.0.to_string(),
        ],
    )
    .unwrap();

    let retry = accept_request(&mut conn, request_id, task_id, resource.id, origin, spec).unwrap();
    assert_eq!(retry.acceptance_sequence, first.acceptance_sequence);
    assert_eq!(retry.state, changed_state);
    assert_eq!(requests_for_resource(&conn, resource.id).unwrap().len(), 1);
    assert!(next_queued_request(&conn, resource.id).unwrap().is_none());
}

#[test]
fn conflicting_retry_and_task_uuid_reuse_are_rejected() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let request_id = RequestId::new();
    let task_id = TaskId::new();
    let origin = MachineId::new();
    accept_request(
        &mut conn,
        request_id,
        task_id,
        resource.id,
        origin,
        command_spec(&["echo", "first"]),
    )
    .unwrap();

    assert!(matches!(
        accept_request(
            &mut conn,
            request_id,
            task_id,
            resource.id,
            origin,
            command_spec(&["echo", "changed"]),
        ),
        Err(ResourceStoreError::Conflict(
            ConflictReason::RequestSpecMismatch
        ))
    ));
    assert!(matches!(
        accept_request(
            &mut conn,
            RequestId::new(),
            task_id,
            resource.id,
            origin,
            command_spec(&["echo", "first"]),
        ),
        Err(ResourceStoreError::Conflict(
            ConflictReason::TaskIdentityInUse
        ))
    ));
    assert_eq!(requests_for_resource(&conn, resource.id).unwrap().len(), 1);
}

#[test]
fn agent_workloads_are_rejected_before_a_request_is_written() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();

    assert!(matches!(
        accept_request(
            &mut conn,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            agent_spec(),
        ),
        Err(ResourceStoreError::InvalidCommandSpec(
            CommandSpecError::AgentWorkload
        ))
    ));
    assert!(
        requests_for_resource(&conn, resource.id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn explicit_machine_is_rejected_before_a_request_is_written() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let mut spec = command_spec(&["echo", "gpu"]);
    spec.machine = Some(MachineName::parse("other").unwrap());

    assert!(matches!(
        accept_request(
            &mut conn,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            spec,
        ),
        Err(ResourceStoreError::InvalidCommandSpec(
            CommandSpecError::ExplicitMachine
        ))
    ));
    assert!(
        requests_for_resource(&conn, resource.id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn request_for_an_unknown_resource_returns_a_typed_error() {
    let mut conn = connection();

    assert!(matches!(
        accept_request(
            &mut conn,
            RequestId::new(),
            TaskId::new(),
            ResourceId::new(),
            MachineId::new(),
            command_spec(&["echo", "hello"]),
        ),
        Err(ResourceStoreError::ResourceNotFound)
    ));
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM resource_requests", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn resource_registration_is_idempotent_and_does_not_replace_content() {
    let mut conn = connection();
    let resource = resource();

    assert_eq!(register_resource(&mut conn, &resource).unwrap(), resource);
    assert_eq!(register_resource(&mut conn, &resource).unwrap(), resource);
    let mut conflicting = resource.clone();
    conflicting.display_name = "replacement".into();
    assert!(matches!(
        register_resource(&mut conn, &conflicting),
        Err(ResourceStoreError::RegistrationConflict { .. })
    ));
    let conflicting_authority = Resource::new(
        resource.id,
        resource.display_name.clone(),
        MachineId::new(),
        resource.supervisor,
        resource.assignment_revision,
        resource.state_revision,
        resource.registered_background_task,
    );
    assert!(matches!(
        register_resource(&mut conn, &conflicting_authority),
        Err(ResourceStoreError::RegistrationConflict { .. })
    ));
    assert_eq!(select_resource(&conn, resource.id).unwrap(), Some(resource));
}

#[test]
fn partial_index_allows_only_one_non_closed_loan_per_resource() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let closed = LoanState::Closed {
        result: LoanClosure::NoResume {
            return_context: ReturnContext::Idle,
            reason: "completed".into(),
        },
    };
    insert_loan(&conn, LoanId::new(), resource.id, closed).unwrap();

    let active = LoanState::Active {
        phase: LoanPhase::AwaitingRelease {
            action_id: ActionId::new(),
            observed_background_task: TaskId::new(),
            watcher_intent: None,
        },
    };
    insert_loan(&conn, LoanId::new(), resource.id, active.clone()).unwrap();
    assert!(insert_loan(&conn, LoanId::new(), resource.id, active).is_err());
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM loans WHERE resource_id = ?1",
            [resource.id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn authority_snapshot_restores_only_owned_non_closed_loans_in_stable_order() {
    let mut conn = connection();
    let authority = MachineId::new();
    let other_authority = MachineId::new();
    let active_resource = resource_for_authority(authority);
    let closed_resource = resource_for_authority(authority);
    let empty_resource = resource_for_authority(authority);
    let foreign_resource = resource_for_authority(other_authority);

    for resource in [
        &active_resource,
        &closed_resource,
        &empty_resource,
        &foreign_resource,
    ] {
        register_resource(&mut conn, resource).unwrap();
    }

    let active_loan_id = LoanId::new();
    let active_state = LoanState::Active {
        phase: LoanPhase::AwaitingRelease {
            action_id: ActionId::new(),
            observed_background_task: TaskId::new(),
            watcher_intent: None,
        },
    };
    let active_loan = Loan {
        id: active_loan_id,
        resource_id: active_resource.id,
        state: active_state.clone(),
    };
    insert_loan(
        &conn,
        active_loan_id,
        active_resource.id,
        active_loan.state.clone(),
    )
    .unwrap();
    insert_loan(
        &conn,
        LoanId::new(),
        closed_resource.id,
        LoanState::Closed {
            result: LoanClosure::NoResume {
                return_context: ReturnContext::Idle,
                reason: "completed".into(),
            },
        },
    )
    .unwrap();
    insert_loan(&conn, LoanId::new(), foreign_resource.id, active_state).unwrap();

    let snapshots = resources_for_authority(&conn, authority).unwrap();
    let mut expected_ids = vec![active_resource.id, closed_resource.id, empty_resource.id];
    expected_ids.sort_by_key(ResourceId::as_uuid);
    assert_eq!(
        snapshots
            .iter()
            .map(|snapshot| snapshot.resource.id)
            .collect::<Vec<_>>(),
        expected_ids
    );
    assert_eq!(
        snapshots
            .iter()
            .find(|snapshot| snapshot.resource.id == active_resource.id)
            .unwrap()
            .loan,
        Some(active_loan)
    );
    for resource_id in [closed_resource.id, empty_resource.id] {
        assert_eq!(
            snapshots
                .iter()
                .find(|snapshot| snapshot.resource.id == resource_id)
                .unwrap()
                .loan,
            None
        );
    }
    assert!(
        snapshots
            .iter()
            .all(|snapshot| snapshot.resource.id != foreign_resource.id)
    );

    let malformed_resource = resource_for_authority(authority);
    register_resource(&mut conn, &malformed_resource).unwrap();
    conn.execute(
        "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
        params![
            LoanId::new().as_uuid().to_string(),
            malformed_resource.id.as_uuid().to_string(),
            r#"{"type":"closed"}"#,
        ],
    )
    .unwrap();

    assert!(matches!(
        resources_for_authority(&conn, authority),
        Err(ResourceStoreError::CorruptRecord { .. })
    ));
}

#[test]
fn release_loan_requires_a_queued_request() {
    let mut conn = connection();
    let background_task = TaskId::new();
    let resource = resource_with_background_task(background_task);
    register_resource(&mut conn, &resource).unwrap();
    insert_local_task_status(&conn, background_task, "running");

    assert!(matches!(
        open_release_loan_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            resource.state_revision,
        ),
        Err(OpenReleaseLoanError::NoQueuedRequest)
    ));
    assert_eq!(select_resource(&conn, resource.id).unwrap(), Some(resource));
    let loan_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
        .unwrap();
    assert_eq!(loan_count, 0);
}

#[test]
fn queue_reconciliation_opens_one_release_action_and_keeps_requests_queued_in_order() {
    let mut conn = connection();
    let authority = MachineId::new();
    let background_task = TaskId::new();
    let mut resource = resource_for_authority(authority);
    resource.registered_background_task = Some(background_task);
    register_resource_for_authority(&mut conn, authority, &resource).unwrap();
    insert_local_task_status(&conn, background_task, "running");

    let first = accept_request_for_authority(
        &mut conn,
        authority,
        RequestId::new(),
        TaskId::new(),
        resource.id,
        MachineId::new(),
        command_spec(&["echo", "first"]),
    )
    .unwrap();
    let second = accept_request_for_authority(
        &mut conn,
        authority,
        RequestId::new(),
        TaskId::new(),
        resource.id,
        MachineId::new(),
        command_spec(&["echo", "second"]),
    )
    .unwrap();

    let outcome =
        reconcile_resource_queue_for_authority(&mut conn, authority, resource.id).unwrap();
    let ResourceQueueReconcileOutcome::ReleaseRequired { loan, notice } = outcome else {
        panic!("running background work must reserve one release action");
    };
    assert_eq!(notice.loan_id, loan.id);
    assert_eq!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                action_id: notice.action_id,
                observed_background_task: background_task,
                watcher_intent: None,
            },
        }
    );

    let repeated =
        reconcile_resource_queue_for_authority(&mut conn, authority, resource.id).unwrap();
    assert!(matches!(
        repeated,
        ResourceQueueReconcileOutcome::LoanAlreadyActive { loan: repeated_loan }
            if repeated_loan.id == loan.id && repeated_loan.state == loan.state
    ));
    assert_eq!(
        select_non_closed_loan(&conn, resource.id).unwrap(),
        Some(loan.clone())
    );
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        1
    );
    let requests = requests_for_resource_for_authority(&conn, authority, resource.id).unwrap();
    assert_eq!(requests[0].request_id, first.request_id);
    assert_eq!(requests[1].request_id, second.request_id);
    assert!(
        requests
            .iter()
            .all(|request| matches!(request.state, ResourceRequestState::Queued))
    );
}

#[test]
fn queue_reconciliation_reports_queued_work_and_does_not_infer_idle_from_missing_background() {
    let mut conn = connection();
    let authority = MachineId::new();
    let resource = resource_for_authority(authority);
    register_resource_for_authority(&mut conn, authority, &resource).unwrap();
    let first = accept_request_for_authority(
        &mut conn,
        authority,
        RequestId::new(),
        TaskId::new(),
        resource.id,
        MachineId::new(),
        command_spec(&["echo", "first"]),
    )
    .unwrap();
    let second = accept_request_for_authority(
        &mut conn,
        authority,
        RequestId::new(),
        TaskId::new(),
        resource.id,
        MachineId::new(),
        command_spec(&["echo", "second"]),
    )
    .unwrap();

    let outcome =
        reconcile_resource_queue_for_authority(&mut conn, authority, resource.id).unwrap();
    assert!(matches!(
        outcome,
        ResourceQueueReconcileOutcome::AttentionRequired {
            request,
            reason: ResourceQueueAttentionReason::IdleNotProven {
                gap: crate::resource::IdleProofGap::NoIdleEvidence,
            },
        } if request.request_id == first.request_id
    ));
    assert!(
        select_non_closed_loan(&conn, resource.id)
            .unwrap()
            .is_none()
    );
    let requests = requests_for_resource_for_authority(&conn, authority, resource.id).unwrap();
    assert_eq!(requests[0].request_id, first.request_id);
    assert_eq!(requests[1].request_id, second.request_id);
    assert!(
        requests
            .iter()
            .all(|request| matches!(request.state, ResourceRequestState::Queued))
    );
}

#[test]
fn queue_reconciliation_requires_attention_for_missing_or_uncertain_background_state() {
    for (task_status, expected_reason) in [
        (
            None,
            ResourceQueueAttentionReason::BackgroundTaskMissing {
                task_id: TaskId::new(),
            },
        ),
        (
            Some("lost"),
            ResourceQueueAttentionReason::BackgroundTaskNotRunning {
                task_id: TaskId::new(),
                state: "lost".into(),
            },
        ),
    ] {
        let mut conn = connection();
        let authority = MachineId::new();
        let background_task = match &expected_reason {
            ResourceQueueAttentionReason::BackgroundTaskMissing { task_id }
            | ResourceQueueAttentionReason::BackgroundTaskNotRunning { task_id, .. } => *task_id,
            ResourceQueueAttentionReason::IdleNotProven { .. }
            | ResourceQueueAttentionReason::BackgroundLaunchPending { .. }
            | ResourceQueueAttentionReason::AcceptedTaskLaunchUncertain { .. }
            | ResourceQueueAttentionReason::UnverifiedServingRelease
            | ResourceQueueAttentionReason::ReleaseProofUnavailable { .. }
            | ResourceQueueAttentionReason::AssignedTaskLaunchUncertain { .. }
            | ResourceQueueAttentionReason::AssignedTaskLost { .. }
            | ResourceQueueAttentionReason::AssignedTaskExitUnconfirmed { .. }
            | ResourceQueueAttentionReason::AssignedTaskIdentityMismatch { .. }
            | ResourceQueueAttentionReason::AssignedTaskNoChildSpawnProofInvalid { .. }
            | ResourceQueueAttentionReason::AssignedTaskOwnershipUncertain { .. }
            | ResourceQueueAttentionReason::AssignedTaskStaleRevision { .. }
            | ResourceQueueAttentionReason::AssignedTaskReconcileFailed { .. } => {
                unreachable!()
            }
        };
        let mut resource = resource_for_authority(authority);
        resource.registered_background_task = Some(background_task);
        register_resource_for_authority(&mut conn, authority, &resource).unwrap();
        if let Some(status) = task_status {
            insert_local_task_status(&conn, background_task, status);
        }
        let request = accept_request_for_authority(
            &mut conn,
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            command_spec(&["echo", "queued"]),
        )
        .unwrap();

        let outcome =
            reconcile_resource_queue_for_authority(&mut conn, authority, resource.id).unwrap();
        assert!(matches!(
            outcome,
            ResourceQueueReconcileOutcome::AttentionRequired {
                request: saved_request,
                reason,
            } if saved_request.request_id == request.request_id
                && reason == expected_reason
        ));
        assert!(
            select_non_closed_loan(&conn, resource.id)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            next_queued_request_for_authority(&conn, authority, resource.id)
                .unwrap()
                .unwrap()
                .state,
            ResourceRequestState::Queued
        ));
    }
}

#[test]
fn release_loan_rejects_the_wrong_authority() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();

    assert!(matches!(
        open_release_loan_for_authority(
            &mut conn,
            MachineId::new(),
            resource.id,
            resource.state_revision,
        ),
        Err(OpenReleaseLoanError::Resource(
            ResourceStoreError::WrongAuthority { expected, found }
        )) if expected == resource.authority_machine() && found != expected
    ));
}

#[test]
fn release_loan_rejects_a_stale_resource_revision() {
    let mut conn = connection();
    let background_task = TaskId::new();
    let resource = resource_with_background_task(background_task);
    register_resource(&mut conn, &resource).unwrap();
    queue_request(&mut conn, resource.id);
    insert_local_task_status(&conn, background_task, "running");

    assert!(matches!(
        open_release_loan_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            ResourceRevision::new(1),
        ),
        Err(OpenReleaseLoanError::StaleRevision {
            expected,
            actual,
        })
            if expected == ResourceRevision::new(1)
                && actual == ResourceRevision::new(0)
    ));
    assert!(
        select_non_closed_loan(&conn, resource.id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn release_loan_requires_a_registered_background_task() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    queue_request(&mut conn, resource.id);

    assert!(matches!(
        open_release_loan_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            resource.state_revision,
        ),
        Err(OpenReleaseLoanError::BackgroundTaskNotRegistered)
    ));
    assert!(
        select_non_closed_loan(&conn, resource.id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn release_loan_requires_the_registered_background_task_row() {
    let mut conn = connection();
    let background_task = TaskId::new();
    let resource = resource_with_background_task(background_task);
    register_resource(&mut conn, &resource).unwrap();
    queue_request(&mut conn, resource.id);

    assert!(matches!(
        open_release_loan_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            resource.state_revision,
        ),
        Err(OpenReleaseLoanError::BackgroundTaskMissing { task_id })
            if task_id == background_task
    ));
    assert!(
        select_non_closed_loan(&conn, resource.id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn release_loan_requires_the_registered_background_task_to_be_running() {
    let mut conn = connection();
    let background_task = TaskId::new();
    let resource = resource_with_background_task(background_task);
    register_resource(&mut conn, &resource).unwrap();
    queue_request(&mut conn, resource.id);
    insert_local_task_status(&conn, background_task, "queued");

    assert!(matches!(
        open_release_loan_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            resource.state_revision,
        ),
        Err(OpenReleaseLoanError::BackgroundTaskNotRunning { task_id, state })
            if task_id == background_task && state == "queued"
    ));
    assert!(
        select_non_closed_loan(&conn, resource.id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn release_loan_opens_with_one_matching_pending_notice_and_revision() {
    let mut conn = connection();
    let background_task = TaskId::new();
    let resource = resource_with_background_task(background_task);
    register_resource(&mut conn, &resource).unwrap();
    let request = queue_request(&mut conn, resource.id);
    insert_local_task_status(&conn, background_task, "running");

    let result = open_release_loan_for_authority(
        &mut conn,
        resource.authority_machine(),
        resource.id,
        resource.state_revision,
    )
    .unwrap();
    let OpenReleaseLoanResult::Opened { loan, notice } = result else {
        panic!("first call must open a new release loan");
    };
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task,
                ..
            },
    } = loan.state
    else {
        panic!("new release loan must await release");
    };

    assert_eq!(observed_background_task, background_task);
    assert_eq!(notice.loan_id, loan.id);
    assert_eq!(notice.action_id, action_id);
    assert_eq!(notice.state_revision, ResourceRevision::new(1));
    assert_eq!(notice.destination, resource.supervisor);
    assert_eq!(notice.assignment_revision, resource.assignment_revision);
    assert_eq!(
        notice.payload,
        SupervisorNoticePayload::ReleaseRequired {
            task_id: background_task,
        }
    );
    assert_eq!(
        notice.delivery,
        SupervisorNoticeDelivery::Pending { attempts: 0 }
    );

    let saved_resource = select_resource(&conn, resource.id).unwrap().unwrap();
    assert_eq!(saved_resource.state_revision, ResourceRevision::new(1));
    let saved_loan = select_non_closed_loan(&conn, resource.id).unwrap().unwrap();
    assert_eq!(saved_loan, loan);
    assert_eq!(
        next_queued_request(&conn, resource.id)
            .unwrap()
            .map(|queued| queued.request_id),
        Some(request.request_id)
    );
    let loan_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
        .unwrap();
    let notice_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(loan_count, 1);
    assert_eq!(notice_count, 1);
}

#[test]
fn release_loan_retry_returns_the_saved_action_after_queue_cancellation() {
    let mut conn = connection();
    let background_task = TaskId::new();
    let resource = resource_with_background_task(background_task);
    register_resource(&mut conn, &resource).unwrap();
    let request = queue_request(&mut conn, resource.id);
    insert_local_task_status(&conn, background_task, "running");

    let first = open_release_loan_for_authority(
        &mut conn,
        resource.authority_machine(),
        resource.id,
        resource.state_revision,
    )
    .unwrap();
    let OpenReleaseLoanResult::Opened {
        loan: first_loan,
        notice: first_notice,
    } = first
    else {
        panic!("first call must open a new release loan");
    };
    cancel_request_before_activation(
        &mut conn,
        request.request_id,
        request.task_id,
        request.resource_id,
        request.origin_machine,
    )
    .unwrap();
    conn.execute(
        "UPDATE tasks SET status = 'succeeded' WHERE id = ?1",
        [background_task.to_string()],
    )
    .unwrap();

    let retry = open_release_loan_for_authority(
        &mut conn,
        resource.authority_machine(),
        resource.id,
        resource.state_revision,
    )
    .unwrap();
    assert_eq!(
        retry,
        OpenReleaseLoanResult::AlreadyAwaitingRelease {
            loan: first_loan,
            notice: first_notice,
        }
    );
    assert!(next_queued_request(&conn, resource.id).unwrap().is_none());
    let loan_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
        .unwrap();
    let notice_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(loan_count, 1);
    assert_eq!(notice_count, 1);
}

#[test]
fn release_loan_does_not_replace_another_non_closed_loan() {
    let mut conn = connection();
    let background_task = TaskId::new();
    let resource = resource_with_background_task(background_task);
    register_resource(&mut conn, &resource).unwrap();
    queue_request(&mut conn, resource.id);
    insert_local_task_status(&conn, background_task, "running");
    let existing_loan = Loan {
        id: LoanId::new(),
        resource_id: resource.id,
        state: LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: ReturnContext::Idle,
                current_request_id: RequestId::new(),
                release_provenance: crate::store::unreceipted_release_provenance(),
            },
        },
    };
    insert_loan(
        &conn,
        existing_loan.id,
        resource.id,
        existing_loan.state.clone(),
    )
    .unwrap();

    assert!(matches!(
        open_release_loan_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            resource.state_revision,
        ),
        Err(OpenReleaseLoanError::ExistingLoan { loan }) if *loan == existing_loan
    ));
    assert_eq!(
        select_resource(&conn, resource.id)
            .unwrap()
            .unwrap()
            .state_revision,
        resource.state_revision
    );
    let loan_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
        .unwrap();
    assert_eq!(loan_count, 1);
}

#[test]
fn release_loan_notice_failure_rolls_back_loan_and_resource_revision() {
    let mut conn = connection();
    let background_task = TaskId::new();
    let resource = resource_with_background_task(background_task);
    register_resource(&mut conn, &resource).unwrap();
    let request = queue_request(&mut conn, resource.id);
    insert_local_task_status(&conn, background_task, "running");
    conn.execute_batch(
        "CREATE TRIGGER reject_release_notice
         BEFORE INSERT ON resource_supervisor_notices
         BEGIN
             SELECT RAISE(ABORT, 'injected notice insert failure');
         END;",
    )
    .unwrap();

    assert!(matches!(
        open_release_loan_for_authority(
            &mut conn,
            resource.authority_machine(),
            resource.id,
            resource.state_revision,
        ),
        Err(OpenReleaseLoanError::Notice(
            SupervisorNoticeStoreError::Storage(_)
        ))
    ));
    assert_eq!(
        select_resource(&conn, resource.id)
            .unwrap()
            .unwrap()
            .state_revision,
        resource.state_revision
    );
    assert!(
        select_non_closed_loan(&conn, resource.id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        next_queued_request(&conn, resource.id)
            .unwrap()
            .map(|queued| queued.request_id),
        Some(request.request_id)
    );
    let loan_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM loans", [], |row| row.get(0))
        .unwrap();
    let notice_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(loan_count, 0);
    assert_eq!(notice_count, 0);
}

#[test]
fn notice_identity_is_unique_per_action_and_retries_detect_conflicts() {
    let mut conn = connection();
    let notice = insert_notice_fixture(&mut conn);

    let saved = persist_notice_for_test(&mut conn, &notice).unwrap();
    assert_eq!(saved, notice);
    assert_eq!(persist_notice_for_test(&mut conn, &notice).unwrap(), notice);

    let attempt_id = DeliveryAttemptId::new();
    let reserved = reserve_supervisor_notice_attempt(&mut conn, notice.id, attempt_id).unwrap();
    assert!(matches!(
        reserved.delivery,
        SupervisorNoticeDelivery::Sending { .. }
    ));
    assert_eq!(
        persist_notice_for_test(&mut conn, &notice).unwrap(),
        reserved
    );

    let mut action_conflict = notice.clone();
    action_conflict.id = NoticeId::new();
    assert!(matches!(
        persist_notice_for_test(&mut conn, &action_conflict),
        Err(SupervisorNoticeStoreError::Conflict)
    ));

    let mut content_conflict = notice.clone();
    content_conflict.payload = SupervisorNoticePayload::AttentionRequired {
        reason: "different action content".into(),
    };
    assert!(matches!(
        persist_notice_for_test(&mut conn, &content_conflict),
        Err(SupervisorNoticeStoreError::Conflict)
    ));
}

#[test]
fn notice_insert_uses_the_callers_loan_transaction() {
    let mut conn = connection();
    let resource = resource();
    register_resource(&mut conn, &resource).unwrap();
    let loan_id = LoanId::new();
    let action_id = ActionId::new();
    let task_id = TaskId::new();
    let loan_state = LoanState::Active {
        phase: LoanPhase::AwaitingRelease {
            action_id,
            observed_background_task: task_id,
            watcher_intent: None,
        },
    };
    let notice = SupervisorNotice {
        id: NoticeId::new(),
        loan_id,
        action_id,
        state_revision: resource.state_revision,
        destination: resource.supervisor,
        assignment_revision: resource.assignment_revision,
        payload: SupervisorNoticePayload::ReleaseRequired { task_id },
        delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
    };

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    insert_loan(&tx, loan_id, resource.id, loan_state).unwrap();
    assert_eq!(
        insert_supervisor_notice_in_transaction(&tx, &notice).unwrap(),
        notice
    );
    tx.rollback().unwrap();

    let loan_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM loans WHERE id = ?1",
            [loan_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let notice_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices WHERE id = ?1",
            [notice.id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(loan_count, 0);
    assert_eq!(notice_count, 0);
}

#[test]
fn only_the_current_attempt_can_settle_and_delivery_does_not_finish_the_loan() {
    let mut conn = connection();
    let notice = insert_notice_fixture(&mut conn);
    persist_notice_for_test(&mut conn, &notice).unwrap();
    let loan_state_before: String = conn
        .query_row(
            "SELECT state_json FROM loans WHERE id = ?1",
            [notice.loan_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();

    let first_attempt = DeliveryAttemptId::new();
    reserve_supervisor_notice_attempt(&mut conn, notice.id, first_attempt).unwrap();
    let failed = settle_supervisor_notice_attempt(
        &mut conn,
        notice.id,
        first_attempt,
        Err("temporary transport failure".into()),
    )
    .unwrap();
    assert_eq!(
        failed.delivery,
        SupervisorNoticeDelivery::RetryPending {
            attempts: 1,
            last_error: "temporary transport failure".into(),
        }
    );

    let second_attempt = DeliveryAttemptId::new();
    reserve_supervisor_notice_attempt(&mut conn, notice.id, second_attempt).unwrap();
    assert!(matches!(
        settle_supervisor_notice_attempt(&mut conn, notice.id, first_attempt, Ok(())),
        Err(SupervisorNoticeStoreError::StaleAttempt)
    ));
    assert!(matches!(
        supervisor_notice(&conn, notice.id).unwrap().unwrap().delivery,
        SupervisorNoticeDelivery::Sending { attempt_id, attempt: 2 }
            if attempt_id == second_attempt
    ));

    let delivered =
        settle_supervisor_notice_attempt(&mut conn, notice.id, second_attempt, Ok(())).unwrap();
    assert_eq!(
        delivered.delivery,
        SupervisorNoticeDelivery::Delivered { attempts: 2 }
    );
    let loan_state_after: String = conn
        .query_row(
            "SELECT state_json FROM loans WHERE id = ?1",
            [notice.loan_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(loan_state_after, loan_state_before);
    assert!(matches!(
        reserve_supervisor_notice_attempt(&mut conn, notice.id, DeliveryAttemptId::new()),
        Err(SupervisorNoticeStoreError::AlreadyDelivered)
    ));
}

#[test]
fn retry_budget_and_last_error_survive_database_reopen() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    let notice = {
        let mut conn = connection_at(&path);
        let notice = insert_notice_fixture(&mut conn);
        persist_notice_for_test(&mut conn, &notice).unwrap();
        let attempt = DeliveryAttemptId::new();
        reserve_supervisor_notice_attempt(&mut conn, notice.id, attempt).unwrap();
        settle_supervisor_notice_attempt(
            &mut conn,
            notice.id,
            attempt,
            Err("first retry error".into()),
        )
        .unwrap();
        notice
    };

    let mut conn = connection_at(&path);
    assert_eq!(
        supervisor_notice(&conn, notice.id)
            .unwrap()
            .unwrap()
            .delivery,
        SupervisorNoticeDelivery::RetryPending {
            attempts: 1,
            last_error: "first retry error".into(),
        }
    );
    let second_attempt = DeliveryAttemptId::new();
    reserve_supervisor_notice_attempt(&mut conn, notice.id, second_attempt).unwrap();
    settle_supervisor_notice_attempt(
        &mut conn,
        notice.id,
        second_attempt,
        Err("second retry error".into()),
    )
    .unwrap();
    drop(conn);

    let mut conn = connection_at(&path);
    assert_eq!(
        supervisor_notice(&conn, notice.id)
            .unwrap()
            .unwrap()
            .delivery,
        SupervisorNoticeDelivery::RetryPending {
            attempts: 2,
            last_error: "second retry error".into(),
        }
    );
    let final_attempt = DeliveryAttemptId::new();
    reserve_supervisor_notice_attempt(&mut conn, notice.id, final_attempt).unwrap();
    let failed = settle_supervisor_notice_attempt(
        &mut conn,
        notice.id,
        final_attempt,
        Err("final retry error".into()),
    )
    .unwrap();
    assert_eq!(
        failed.delivery,
        SupervisorNoticeDelivery::Failed {
            attempts: 3,
            last_error: "final retry error".into(),
        }
    );
    drop(conn);

    let mut reopened = connection_at(&path);
    assert_eq!(
        supervisor_notice(&reopened, notice.id)
            .unwrap()
            .unwrap()
            .delivery,
        SupervisorNoticeDelivery::Failed {
            attempts: 3,
            last_error: "final retry error".into(),
        }
    );
    assert!(pending_supervisor_notices(&reopened).unwrap().is_empty());
    assert!(matches!(
        reserve_supervisor_notice_attempt(&mut reopened, notice.id, DeliveryAttemptId::new()),
        Err(SupervisorNoticeStoreError::AttemptBudgetExhausted)
    ));
}

#[test]
fn startup_recovery_retries_or_fails_sending_notices_without_new_identity() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    let (notice, in_flight_attempt) = {
        let mut conn = connection_at(&path);
        let notice = insert_notice_fixture(&mut conn);
        persist_notice_for_test(&mut conn, &notice).unwrap();
        for error in ["first error", "second error"] {
            let attempt = DeliveryAttemptId::new();
            reserve_supervisor_notice_attempt(&mut conn, notice.id, attempt).unwrap();
            settle_supervisor_notice_attempt(&mut conn, notice.id, attempt, Err(error.into()))
                .unwrap();
        }
        let in_flight_attempt = DeliveryAttemptId::new();
        reserve_supervisor_notice_attempt(&mut conn, notice.id, in_flight_attempt).unwrap();
        (notice, in_flight_attempt)
    };

    let mut reopened = connection_at(&path);
    assert!(matches!(
        supervisor_notice(&reopened, notice.id).unwrap().unwrap().delivery,
        SupervisorNoticeDelivery::Sending { attempt_id, attempt: 3 }
            if attempt_id == in_flight_attempt
    ));
    let recovered = recover_sending_supervisor_notices(&mut reopened).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].id, notice.id);
    assert_eq!(recovered[0].action_id, notice.action_id);
    assert_eq!(
        recovered[0].delivery,
        SupervisorNoticeDelivery::Failed {
            attempts: 3,
            last_error: INTERRUPTED_DELIVERY_ERROR.into(),
        }
    );
    assert!(matches!(
        settle_supervisor_notice_attempt(&mut reopened, notice.id, in_flight_attempt, Ok((),)),
        Err(SupervisorNoticeStoreError::StaleAttempt)
    ));
    assert!(
        recover_sending_supervisor_notices(&mut reopened)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn supervisor_retarget_uses_assignment_cas_and_invalidates_old_attempts() {
    let mut conn = connection();
    let notice = insert_notice_fixture(&mut conn);
    persist_notice_for_test(&mut conn, &notice).unwrap();
    let old_assignment = notice.assignment_revision;
    let next_assignment = AssignmentRevision::new(old_assignment.get() + 1);
    let next_destination = SupervisorAddress {
        machine: MachineId::new(),
        thread: ThreadId(Uuid::now_v7()),
    };
    assert!(matches!(
        retarget_supervisor_notice(
            &mut conn,
            notice.id,
            AssignmentRevision::new(old_assignment.get() + 1),
            next_destination,
            AssignmentRevision::new(old_assignment.get() + 2),
        ),
        Err(SupervisorNoticeStoreError::StaleAssignmentRevision)
    ));

    let old_attempt = DeliveryAttemptId::new();
    reserve_supervisor_notice_attempt(&mut conn, notice.id, old_attempt).unwrap();
    let retargeted = retarget_supervisor_notice(
        &mut conn,
        notice.id,
        old_assignment,
        next_destination,
        next_assignment,
    )
    .unwrap();
    assert_eq!(retargeted.id, notice.id);
    assert_eq!(retargeted.loan_id, notice.loan_id);
    assert_eq!(retargeted.action_id, notice.action_id);
    assert_eq!(retargeted.destination, next_destination);
    assert_eq!(retargeted.assignment_revision, next_assignment);
    assert_eq!(
        retargeted.delivery,
        SupervisorNoticeDelivery::RetryPending {
            attempts: 1,
            last_error: RETARGETED_DELIVERY_ERROR.into(),
        }
    );
    assert!(matches!(
        settle_supervisor_notice_attempt(&mut conn, notice.id, old_attempt, Ok(())),
        Err(SupervisorNoticeStoreError::StaleAttempt)
    ));
    assert!(matches!(
        retarget_supervisor_notice(
            &mut conn,
            notice.id,
            old_assignment,
            next_destination,
            AssignmentRevision::new(next_assignment.get() + 1),
        ),
        Err(SupervisorNoticeStoreError::StaleAssignmentRevision)
    ));

    let current_attempt = DeliveryAttemptId::new();
    reserve_supervisor_notice_attempt(&mut conn, notice.id, current_attempt).unwrap();
    settle_supervisor_notice_attempt(&mut conn, notice.id, current_attempt, Ok(())).unwrap();
    assert!(matches!(
        retarget_supervisor_notice(
            &mut conn,
            notice.id,
            next_assignment,
            next_destination,
            AssignmentRevision::new(next_assignment.get() + 1),
        ),
        Err(SupervisorNoticeStoreError::AlreadyDelivered)
    ));
    assert!(matches!(
        supervisor_notice(&conn, notice.id)
            .unwrap()
            .unwrap()
            .delivery,
        SupervisorNoticeDelivery::Delivered { attempts: 2 }
    ));
}

#[test]
fn pending_notices_are_ordered_by_stable_notice_id() {
    let mut conn = connection();
    let first = insert_notice_fixture(&mut conn);
    let mut second = first.clone();
    second.id = NoticeId::from_uuid(Uuid::from_u128(2)).unwrap();
    second.action_id = ActionId::new();
    let mut first = first;
    first.id = NoticeId::from_uuid(Uuid::from_u128(1)).unwrap();
    first.action_id = ActionId::new();
    persist_notice_for_test(&mut conn, &second).unwrap();
    persist_notice_for_test(&mut conn, &first).unwrap();

    let pending = pending_supervisor_notices(&conn).unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].id, first.id);
    assert_eq!(pending[1].id, second.id);
    assert_eq!(supervisor_notice(&conn, first.id).unwrap(), Some(first));
}

#[test]
fn corrupt_notice_record_is_not_a_retryable_storage_error() {
    let mut conn = connection();
    let notice = insert_notice_fixture(&mut conn);
    persist_notice_for_test(&mut conn, &notice).unwrap();
    conn.execute(
        "UPDATE resource_supervisor_notices
         SET notice_json = json_remove(notice_json, '$.payload.task_id') WHERE id = ?1",
        [notice.id.as_uuid().to_string()],
    )
    .unwrap();

    assert!(matches!(
        supervisor_notice(&conn, notice.id),
        Err(SupervisorNoticeStoreError::CorruptRecord { .. })
    ));
    assert!(matches!(
        pending_supervisor_notices(&conn),
        Err(SupervisorNoticeStoreError::CorruptRecord { .. })
    ));
}
