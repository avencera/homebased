//! StoreActor-facing operations for authority-local resource state.

use super::Store;
use crate::domain::TaskId;
use crate::machine::MachineId;
use crate::resource::store::{
    CompleteReleaseError, OpenReleaseLoanError, OpenReleaseLoanResult, QueueCancellationResult,
    ReleaseCompletionResult, ResourceSnapshot, ResourceStoreError, SupervisorNoticeStoreError,
    accept_request_for_authority, cancel_request_before_activation_for_authority,
    complete_release_for_authority as persist_release_completion_for_authority,
    oldest_queued_request_for_authority,
    open_release_loan_for_authority as persist_release_loan_for_authority,
    pending_supervisor_notices as load_pending_supervisor_notices,
    recover_sending_supervisor_notices as recover_in_flight_supervisor_notices,
    register_resource_for_authority, requests_for_resource_for_authority,
    reserve_supervisor_notice_attempt as reserve_notice_attempt,
    resources_for_authority as load_resources_for_authority,
    retarget_supervisor_notice as retarget_notice,
    settle_supervisor_notice_attempt as settle_notice_attempt,
    supervisor_notice as load_supervisor_notice,
};
use crate::resource::{
    ActionId, AssignmentRevision, DeliveryAttemptId, NoticeId, Resource, ResourceId,
    ResourceRequest, ResourceRevision, ReturnContext, SupervisorAddress, SupervisorNotice,
};
use crate::spec::NormalizedSpec;
use crate::submission::RequestId;

impl Store {
    /// Register a resource only on the daemon that matches its fixed authority.
    pub(crate) fn register_resource(
        &mut self,
        authority_machine: MachineId,
        resource: &Resource,
    ) -> Result<Resource, ResourceStoreError> {
        register_resource_for_authority(&mut self.conn, authority_machine, resource)
    }

    /// Load authority-owned resources and active loans on the store connection.
    pub(crate) fn resource_snapshots_for_authority(
        &self,
        authority_machine: MachineId,
    ) -> Result<Vec<ResourceSnapshot>, ResourceStoreError> {
        load_resources_for_authority(&self.conn, authority_machine)
    }

    /// Accept a resource request and assign its authority-local FIFO sequence.
    ///
    /// The origin route must be persisted by the caller before it sends this request.
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

    /// Read a resource's requests in authority-assigned FIFO order.
    pub(crate) fn resource_requests(
        &self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<Vec<ResourceRequest>, ResourceStoreError> {
        requests_for_resource_for_authority(&self.conn, authority_machine, resource_id)
    }

    /// Read the oldest queued request for one resource.
    pub(crate) fn oldest_queued_resource_request(
        &self,
        authority_machine: MachineId,
        resource_id: ResourceId,
    ) -> Result<Option<ResourceRequest>, ResourceStoreError> {
        oldest_queued_request_for_authority(&self.conn, authority_machine, resource_id)
    }

    /// Cancel a queued request or atomically retain prevention before acceptance.
    pub(crate) fn cancel_resource_request_before_activation(
        &mut self,
        authority_machine: MachineId,
        request_id: RequestId,
        task_id: TaskId,
        resource_id: ResourceId,
        origin_machine: MachineId,
    ) -> Result<QueueCancellationResult, ResourceStoreError> {
        cancel_request_before_activation_for_authority(
            &mut self.conn,
            authority_machine,
            request_id,
            task_id,
            resource_id,
            origin_machine,
        )
    }

    /// Open or reuse the authority's release loan on this store connection.
    pub(crate) fn open_release_loan_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        expected_state_revision: ResourceRevision,
    ) -> Result<OpenReleaseLoanResult, OpenReleaseLoanError> {
        persist_release_loan_for_authority(
            &mut self.conn,
            authority_machine,
            resource_id,
            expected_state_revision,
        )
    }

    /// Complete a saved release action on this store connection.
    pub(crate) fn complete_release_for_authority(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        action_id: ActionId,
        expected_state_revision: ResourceRevision,
        return_context: ReturnContext,
    ) -> Result<ReleaseCompletionResult, CompleteReleaseError> {
        persist_release_completion_for_authority(
            &mut self.conn,
            authority_machine,
            resource_id,
            action_id,
            expected_state_revision,
            return_context,
        )
    }

    /// Read one durable supervisor notice by its identity.
    pub(crate) fn supervisor_notice(
        &self,
        notice_id: NoticeId,
    ) -> Result<Option<SupervisorNotice>, SupervisorNoticeStoreError> {
        load_supervisor_notice(&self.conn, notice_id)
    }

    /// List notices that can receive another delivery attempt.
    pub(crate) fn pending_supervisor_notices(
        &self,
    ) -> Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError> {
        load_pending_supervisor_notices(&self.conn)
    }

    /// Reserve one bounded delivery attempt on this store connection.
    pub(crate) fn reserve_supervisor_notice_attempt(
        &mut self,
        notice_id: NoticeId,
        attempt_id: DeliveryAttemptId,
    ) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
        reserve_notice_attempt(&mut self.conn, notice_id, attempt_id)
    }

    /// Settle only the exact in-flight delivery attempt.
    pub(crate) fn settle_supervisor_notice_attempt(
        &mut self,
        notice_id: NoticeId,
        attempt_id: DeliveryAttemptId,
        result: Result<(), String>,
    ) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
        settle_notice_attempt(&mut self.conn, notice_id, attempt_id, result)
    }

    /// Recover in-flight notices after a daemon restart.
    pub(crate) fn recover_sending_supervisor_notices(
        &mut self,
    ) -> Result<Vec<SupervisorNotice>, SupervisorNoticeStoreError> {
        recover_in_flight_supervisor_notices(&mut self.conn)
    }

    /// Retarget an undelivered notice with an assignment-revision compare-and-set.
    pub(crate) fn retarget_supervisor_notice(
        &mut self,
        notice_id: NoticeId,
        expected_assignment_revision: AssignmentRevision,
        destination: SupervisorAddress,
        new_assignment_revision: AssignmentRevision,
    ) -> Result<SupervisorNotice, SupervisorNoticeStoreError> {
        retarget_notice(
            &mut self.conn,
            notice_id,
            expected_assignment_revision,
            destination,
            new_assignment_revision,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};

    use serde_json::json;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::domain::{ExitReason, ProcessStatus, TaskEnv, TaskWorkload, ThreadId, Workload};
    use crate::resource::store::{OpenReleaseLoanResult, ReleaseCompletionResult};
    use crate::resource::{
        AssignmentRevision, DeliveryAttemptId, ResourceRequestState, ResourceRevision,
        ReturnContext, SupervisorAddress, SupervisorNoticeDelivery,
    };
    use crate::spec::NormalizedWorkload;
    use crate::store::{ExecutorIdentity, IdentityError, NewTask, new_queued_task};
    use crate::submission::{ExecutionRecord, PreAcceptanceRejection, RejectionTombstone};

    fn resource(authority: MachineId) -> Resource {
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

    fn spec() -> NormalizedSpec {
        serde_json::from_value(json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "resource command",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["/bin/echo", "hello"] }
        }))
        .unwrap()
    }

    fn remote_task(task: TaskId, spec: &NormalizedSpec) -> crate::domain::TaskRow {
        let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
            panic!("resource spec must be a command");
        };
        new_queued_task(NewTask {
            id: task,
            name: Some(spec.name.clone()),
            thread: spec.thread,
            workload: Workload::Task(TaskWorkload {
                command: workload.command,
            }),
            cwd: spec.cwd.clone(),
            timeout: spec.timeout,
            env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
            binary: PathBuf::from("/bin/echo"),
        })
    }

    fn prevention_count(store: &Store) -> i64 {
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM resource_request_preventions",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn identity_count(store: &Store, task: TaskId) -> i64 {
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_identities WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn assert_cancelled_tombstone(
        identity: ExecutorIdentity,
        task: TaskId,
        origin: MachineId,
        authority: MachineId,
    ) {
        let ExecutorIdentity::Rejected(tombstone) = identity else {
            panic!("pre-activation cancellation must retain a rejection");
        };
        assert_eq!(tombstone.task, task);
        assert_eq!(tombstone.origin_machine, origin);
        assert_eq!(tombstone.execution_machine, authority);
        assert_eq!(tombstone.reason, PreAcceptanceRejection::Cancelled.as_str());
    }

    #[test]
    fn release_notice_facade_operations_share_the_store_connection() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("db");
        let mut store = Store::open(&database).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let mut resource = resource(authority);
        resource.registered_background_task = Some(background_task);

        store.register_resource(authority, &resource).unwrap();
        store
            .insert_task(&remote_task(background_task, &spec()))
            .unwrap();
        assert!(
            store
                .cas_status(
                    background_task,
                    ProcessStatus::Queued,
                    ProcessStatus::Running,
                )
                .unwrap()
                .is_some()
        );
        store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                MachineId::new(),
                spec(),
            )
            .unwrap();

        let OpenReleaseLoanResult::Opened { notice, .. } = store
            .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
            .unwrap()
        else {
            panic!("first facade call must open a release loan");
        };
        assert_eq!(
            store.supervisor_notice(notice.id).unwrap(),
            Some(notice.clone())
        );
        assert_eq!(
            store.pending_supervisor_notices().unwrap(),
            vec![notice.clone()]
        );

        let destination = SupervisorAddress {
            machine: MachineId::new(),
            thread: ThreadId(Uuid::now_v7()),
        };
        let retargeted = store
            .retarget_supervisor_notice(
                notice.id,
                notice.assignment_revision,
                destination,
                AssignmentRevision::new(1),
            )
            .unwrap();
        assert_eq!(retargeted.destination, destination);

        let first_attempt = DeliveryAttemptId::new();
        assert_eq!(
            store
                .reserve_supervisor_notice_attempt(notice.id, first_attempt)
                .unwrap()
                .id,
            notice.id
        );
        assert_eq!(
            store
                .recover_sending_supervisor_notices()
                .unwrap()
                .iter()
                .map(|notice| notice.id)
                .collect::<Vec<_>>(),
            vec![notice.id]
        );

        let second_attempt = DeliveryAttemptId::new();
        store
            .reserve_supervisor_notice_attempt(notice.id, second_attempt)
            .unwrap();
        let settled = store
            .settle_supervisor_notice_attempt(notice.id, second_attempt, Ok(()))
            .unwrap();
        assert_eq!(settled.id, notice.id);
        assert!(matches!(
            settled.delivery,
            SupervisorNoticeDelivery::Delivered { .. }
        ));
        assert_eq!(store.supervisor_notice(notice.id).unwrap(), Some(settled));
    }

    #[test]
    fn release_completion_facade_uses_the_store_connection() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let request_id = RequestId::new();
        let mut resource = resource(authority);
        resource.registered_background_task = Some(background_task);

        store.register_resource(authority, &resource).unwrap();
        store
            .insert_task(&remote_task(background_task, &spec()))
            .unwrap();
        assert!(
            store
                .cas_status(
                    background_task,
                    ProcessStatus::Queued,
                    ProcessStatus::Running,
                )
                .unwrap()
                .is_some()
        );
        store
            .accept_resource_request(
                authority,
                request_id,
                TaskId::new(),
                resource.id,
                MachineId::new(),
                spec(),
            )
            .unwrap();
        let OpenReleaseLoanResult::Opened { notice, loan } = store
            .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
            .unwrap()
        else {
            panic!("first facade call must open a release loan");
        };
        assert!(
            store
                .cas_exit(
                    background_task,
                    ProcessStatus::Running,
                    &ExitReason::Exit { code: 0 },
                )
                .unwrap()
                .is_some()
        );

        let result = store
            .complete_release_for_authority(
                authority,
                resource.id,
                notice.action_id,
                notice.state_revision,
                ReturnContext::Stopped {
                    task_id: background_task,
                    checkpoint_ref: "checkpoint-1".into(),
                    recovery_ref: "recovery-1".into(),
                },
            )
            .unwrap();
        let ReleaseCompletionResult::Assigned {
            loan: completed_loan,
            request,
            state_revision,
        } = result
        else {
            panic!("queued work must be assigned by release completion");
        };

        assert_eq!(completed_loan.id, loan.id);
        assert_eq!(request.request_id, request_id);
        assert_eq!(state_revision, ResourceRevision::new(2));
        let saved_requests = store.resource_requests(authority, resource.id).unwrap();
        assert_eq!(saved_requests.len(), 1);
        assert!(matches!(
            saved_requests[0].state,
            ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
        ));
    }

    #[test]
    fn queue_operations_require_the_registered_authority() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let other_machine = MachineId::new();
        let resource = resource(authority);

        assert!(matches!(
            store.register_resource(other_machine, &resource),
            Err(ResourceStoreError::WrongAuthority { expected, found })
                if expected == authority && found == other_machine
        ));
        store.register_resource(authority, &resource).unwrap();
        assert_eq!(
            store.register_resource(authority, &resource).unwrap(),
            resource
        );

        let request = RequestId::new();
        let task = TaskId::new();
        let origin = MachineId::new();
        assert!(matches!(
            store.accept_resource_request(
                other_machine,
                request,
                task,
                resource.id,
                origin,
                spec(),
            ),
            Err(ResourceStoreError::WrongAuthority { expected, found })
                if expected == authority && found == other_machine
        ));
        assert!(
            store
                .resource_requests(authority, resource.id)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .oldest_queued_resource_request(authority, resource.id)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            store.cancel_resource_request_before_activation(
                other_machine,
                request,
                task,
                resource.id,
                origin,
            ),
            Err(ResourceStoreError::WrongAuthority { expected, found })
                if expected == authority && found == other_machine
        ));
        assert_eq!(identity_count(&store, task), 0);
    }

    #[test]
    fn queue_acceptance_rejects_task_ids_owned_by_the_executor() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();

        let accepted_task = TaskId::new();
        store
            .accept_execution(&ExecutionRecord {
                task: accepted_task,
                origin_machine: origin,
                execution_machine: authority,
                spec: spec().into(),
                state: crate::domain::ProcessStatus::Queued,
            })
            .unwrap();
        let rejected_task = TaskId::new();
        store
            .reject_execution(&RejectionTombstone {
                task: rejected_task,
                origin_machine: origin,
                execution_machine: authority,
                reason: "prior rejection".into(),
            })
            .unwrap();

        for task in [accepted_task, rejected_task] {
            assert!(matches!(
                store.accept_resource_request(
                    authority,
                    RequestId::new(),
                    task,
                    resource.id,
                    origin,
                    spec(),
                ),
                Err(ResourceStoreError::Conflict)
            ));
        }
        assert!(
            store
                .resource_requests(authority, resource.id)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn cancellation_before_acceptance_fences_delayed_remote_task_acceptance() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let request = RequestId::new();
        let task = TaskId::new();

        assert!(matches!(
            store
                .cancel_resource_request_before_activation(
                    authority,
                    request,
                    task,
                    resource.id,
                    origin,
                )
                .unwrap(),
            QueueCancellationResult::PreventedBeforeAcceptance
        ));
        assert_eq!(prevention_count(&store), 1);
        assert_cancelled_tombstone(
            store.executor_identity(task).unwrap().unwrap(),
            task,
            origin,
            authority,
        );
        assert!(store.origin_route_by_task(task).unwrap().is_none());

        let execution = store
            .insert_remote_task(&remote_task(task, &spec()), &spec(), origin, authority)
            .unwrap();
        assert_cancelled_tombstone(execution, task, origin, authority);
        assert!(store.get_task(task).unwrap().is_none());
        assert_eq!(identity_count(&store, task), 1);

        assert!(matches!(
            store
                .cancel_resource_request_before_activation(
                    authority,
                    request,
                    task,
                    resource.id,
                    origin,
                )
                .unwrap(),
            QueueCancellationResult::PreventedBeforeAcceptance
        ));
        assert_eq!(prevention_count(&store), 1);
        assert_eq!(identity_count(&store, task), 1);
    }

    #[test]
    fn cancellation_after_queue_acceptance_is_atomic_and_idempotent() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let request = RequestId::new();
        let task = TaskId::new();
        let accepted = store
            .accept_resource_request(authority, request, task, resource.id, origin, spec())
            .unwrap();
        assert!(store.origin_route_by_task(task).unwrap().is_none());
        assert_eq!(
            store
                .oldest_queued_resource_request(authority, resource.id)
                .unwrap()
                .unwrap()
                .request_id,
            request
        );

        let cancelled = store
            .cancel_resource_request_before_activation(
                authority,
                request,
                task,
                resource.id,
                origin,
            )
            .unwrap();
        let QueueCancellationResult::Request(cancelled) = cancelled else {
            panic!("accepted cancellation must return its saved request");
        };
        assert_eq!(cancelled.acceptance_sequence, accepted.acceptance_sequence);
        assert!(matches!(
            cancelled.state,
            ResourceRequestState::CancelledBeforeLaunch
        ));
        assert!(
            store
                .oldest_queued_resource_request(authority, resource.id)
                .unwrap()
                .is_none()
        );
        assert_cancelled_tombstone(
            store.executor_identity(task).unwrap().unwrap(),
            task,
            origin,
            authority,
        );

        let retry = store
            .cancel_resource_request_before_activation(
                authority,
                request,
                task,
                resource.id,
                origin,
            )
            .unwrap();
        let QueueCancellationResult::Request(retry) = retry else {
            panic!("repeated cancellation must return the saved request");
        };
        assert_eq!(retry.acceptance_sequence, accepted.acceptance_sequence);
        assert!(matches!(
            retry.state,
            ResourceRequestState::CancelledBeforeLaunch
        ));

        let delayed_acceptance = store
            .insert_remote_task(&remote_task(task, &spec()), &spec(), origin, authority)
            .unwrap();
        assert_cancelled_tombstone(delayed_acceptance, task, origin, authority);
        assert!(store.get_task(task).unwrap().is_none());
        assert_eq!(identity_count(&store, task), 1);
    }

    #[test]
    fn concurrent_direct_acceptance_cannot_claim_a_resource_queue_id() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("db");
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        let request = RequestId::new();
        let task = TaskId::new();
        let barrier = Arc::new(Barrier::new(2));

        let mut setup = Store::open(&path).unwrap();
        setup.register_resource(authority, &resource).unwrap();
        setup
            .accept_resource_request(authority, request, task, resource.id, origin, spec())
            .unwrap();
        drop(setup);

        let accept_path = path.clone();
        let accept_barrier = barrier.clone();
        let spec_for_acceptance = spec();
        let acceptance = std::thread::spawn(move || {
            let mut store = Store::open(&accept_path).unwrap();
            accept_barrier.wait();
            store.accept_execution(&ExecutionRecord {
                task,
                origin_machine: origin,
                execution_machine: authority,
                spec: spec_for_acceptance.into(),
                state: crate::domain::ProcessStatus::Queued,
            })
        });

        let mut cancel_store = Store::open(&path).unwrap();
        barrier.wait();
        let cancellation = cancel_store.cancel_resource_request_before_activation(
            authority,
            request,
            task,
            resource.id,
            origin,
        );
        let execution_identity = acceptance.join().unwrap();

        let final_store = Store::open(&path).unwrap();
        let saved = final_store.executor_identity(task).unwrap().unwrap();
        let tombstone = match execution_identity {
            Err(IdentityError::Conflict) => match saved {
                ExecutorIdentity::Rejected(tombstone) => tombstone,
                identity => panic!("resource cancellation must retain a rejection: {identity:?}"),
            },
            Ok(ExecutorIdentity::Rejected(tombstone)) => tombstone,
            outcome => panic!("ordinary acceptance must not win this race: {outcome:?}"),
        };
        assert_eq!(tombstone.task, task);
        assert_eq!(tombstone.origin_machine, origin);
        assert_eq!(tombstone.execution_machine, authority);
        assert!(matches!(
            cancellation,
            Ok(QueueCancellationResult::Request(cancelled))
                if matches!(cancelled.state, ResourceRequestState::CancelledBeforeLaunch)
        ));
        assert_eq!(prevention_count(&final_store), 0);
    }

    #[test]
    fn tombstone_insert_failure_rolls_back_queued_request_cancellation() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let request = RequestId::new();
        let task = TaskId::new();
        store
            .accept_resource_request(authority, request, task, resource.id, origin, spec())
            .unwrap();
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER reject_executor_tombstone
                 BEFORE INSERT ON executor_identities
                 BEGIN SELECT RAISE(ABORT, 'forced tombstone failure'); END;",
            )
            .unwrap();

        assert!(
            store
                .cancel_resource_request_before_activation(
                    authority,
                    request,
                    task,
                    resource.id,
                    origin,
                )
                .is_err()
        );
        let requests = store.resource_requests(authority, resource.id).unwrap();
        assert_eq!(requests.len(), 1);
        assert!(matches!(requests[0].state, ResourceRequestState::Queued));
        assert_eq!(identity_count(&store, task), 0);
        assert!(
            store
                .oldest_queued_resource_request(authority, resource.id)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn assigned_or_terminal_cancellation_does_not_create_executor_tombstones() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let assigned = store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                origin,
                spec(),
            )
            .unwrap();
        let loan_id = crate::resource::LoanId::new();
        let assigned_json =
            serde_json::to_string(&ResourceRequestState::Assigned { loan_id }).unwrap();
        store
            .conn
            .execute(
                "UPDATE resource_requests SET state_json=?1 WHERE request_id=?2",
                rusqlite::params![assigned_json, assigned.request_id.0.to_string()],
            )
            .unwrap();

        let result = store
            .cancel_resource_request_before_activation(
                authority,
                assigned.request_id,
                assigned.task_id,
                resource.id,
                origin,
            )
            .unwrap();
        let QueueCancellationResult::Request(saved) = result else {
            panic!("assigned cancellation must return its saved state");
        };
        assert!(
            matches!(saved.state, ResourceRequestState::Assigned { loan_id: saved } if saved == loan_id)
        );
        assert_eq!(identity_count(&store, assigned.task_id), 0);

        let terminal = store
            .accept_resource_request(
                authority,
                RequestId::new(),
                TaskId::new(),
                resource.id,
                origin,
                spec(),
            )
            .unwrap();
        let terminal_json = serde_json::to_string(&ResourceRequestState::Finished {
            outcome: crate::domain::ExitReason::Cancelled,
        })
        .unwrap();
        store
            .conn
            .execute(
                "UPDATE resource_requests SET state_json=?1 WHERE request_id=?2",
                rusqlite::params![terminal_json, terminal.request_id.0.to_string()],
            )
            .unwrap();

        let result = store
            .cancel_resource_request_before_activation(
                authority,
                terminal.request_id,
                terminal.task_id,
                resource.id,
                origin,
            )
            .unwrap();
        let QueueCancellationResult::Request(saved) = result else {
            panic!("terminal cancellation must return its saved state");
        };
        assert!(matches!(
            saved.state,
            ResourceRequestState::Finished {
                outcome: crate::domain::ExitReason::Cancelled
            }
        ));
        assert_eq!(identity_count(&store, terminal.task_id), 0);
    }
}
