//! First background launch binding, confirmed-start registration, and the idle boundary

use super::fixtures::{acquire_test_lock, resource, saved_trainer_association_json, spec};
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId, ThreadId,
};
use crate::invocation::CommandLine;
use crate::machine::MachineId;
use crate::resource::background_launch::{
    BackgroundLaunchBinding, BackgroundSupervisorAssignment, RemoteBackgroundLaunchReceipt,
};
use crate::resource::command_shape::test_support::FakeTrainer;
use crate::resource::ownership_lock::test_support::start_fake_lock_process;
use crate::resource::ownership_lock::{
    OwnershipLockIdentity, OwnershipLockIdentityMismatchReason, OwnershipLockProbe,
    OwnershipLockProbeError, TrainerRequestDigest, VerifiedTrainerAttempt,
    build_trainer_attempt_registration_evidence, probe_segment_ownership_lock,
    test_support as trainer_attempt_test_support,
};
use crate::resource::store::{
    ResourceStoreError, ResourceTaskAcceptance, ResourceTaskAcceptanceInput,
    TrainerAttemptAssociationStoreError,
};
use crate::resource::{
    AssignmentRevision, IdleBoundaryProof, IdleProofGap, Loan, LoanPhase, LoanState, Resource,
    ResourceQueueAttentionReason, ResourceQueueReconcileOutcome, ResourceRevision, ReturnContext,
    ServingReleaseProvenance,
};
use crate::spec::{NormalizedSpec, NormalizedTaskWorkload, NormalizedWorkload};
use crate::store::resource::task_has_any_event;
use crate::store::resource::trainer_lock::TrainerLockReleaseGap;
use crate::store::{
    BackgroundLaunchAcceptance, BackgroundLaunchError, BackgroundLaunchInput,
    BackgroundLaunchPhase, ExecutorIdentity, RemoteBackgroundLaunchInput, Store,
};
use crate::submission::{CallbackExecutable, RequestId, normalized_spec_sha256};
use rusqlite::params;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use uuid::Uuid;

pub(super) struct LaunchFixture {
    _directory: tempfile::TempDir,
    pub(super) store: Store,
    pub(super) authority: MachineId,
    pub(super) resource: Resource,
    pub(super) trainer: FakeTrainer,
}

impl LaunchFixture {
    pub(super) fn new() -> Self {
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mut store = Store::open(&root.join("db")).unwrap();
        let authority = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        Self {
            _directory: directory,
            store,
            authority,
            resource,
            trainer: FakeTrainer::new(&root),
        }
    }

    pub(super) fn input(
        &self,
        request_id: RequestId,
        spec: NormalizedSpec,
    ) -> BackgroundLaunchInput {
        BackgroundLaunchInput {
            authority_machine: self.authority,
            resource_id: self.resource.id,
            request_id,
            task_id: TaskId::new(),
            spec,
            env: self.trainer.env.clone(),
            callback_codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
        }
    }

    pub(super) fn trainer_spec(&self) -> NormalizedSpec {
        self.trainer
            .spec(self.resource.supervisor.thread, "direct segment trainer")
    }

    pub(super) fn launch(&mut self, request_id: RequestId) -> TaskId {
        let input = self.input(request_id, self.trainer_spec());
        let BackgroundLaunchAcceptance::Inserted { task, .. } = self
            .store
            .accept_background_launch_for_authority(input)
            .unwrap()
        else {
            panic!("the first exact launch must insert its task");
        };
        task
    }

    pub(super) fn saved_resource(&self) -> Resource {
        self.store
            .resource_snapshots_for_authority(self.authority)
            .unwrap()
            .into_iter()
            .find(|snapshot| snapshot.resource.id == self.resource.id)
            .unwrap()
            .resource
    }

    pub(super) fn queue_request(&mut self) -> crate::resource::ResourceRequest {
        self.store
            .accept_resource_request(
                self.authority,
                RequestId::new(),
                TaskId::new(),
                self.resource.id,
                MachineId::new(),
                spec(),
            )
            .unwrap()
    }

    pub(super) fn reconcile(&mut self) -> ResourceQueueReconcileOutcome {
        self.store
            .reconcile_resource_queue_for_authority(self.authority, self.resource.id)
            .unwrap()
    }

    /// Path of the fixture database
    pub(super) fn db_path(&self) -> PathBuf {
        self._directory.path().canonicalize().unwrap().join("db")
    }

    /// Drop the open connection and read the same database as a restarted daemon
    pub(super) fn reopen(&mut self) {
        self.store = Store::open(&self.db_path()).unwrap();
    }

    pub(super) fn task_count(&self) -> i64 {
        self.store
            .conn
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
            .unwrap()
    }
}

#[test]
fn first_launch_binds_one_unregistered_task_and_exact_retry_reuses_it() {
    let mut fixture = LaunchFixture::new();
    let request_id = RequestId::new();
    let task = fixture.launch(request_id);

    // the queued row, accepted identity, callback route, and first event commit together
    let row = fixture.store.get_task(task).unwrap().unwrap();
    assert_eq!(row.status(), ProcessStatus::Queued);
    assert_eq!(row.thread, fixture.resource.supervisor.thread);
    let Some(ExecutorIdentity::Accepted(record)) = fixture.store.executor_identity(task).unwrap()
    else {
        panic!("the launch must save an accepted executor identity");
    };
    assert_eq!(record.origin_machine, fixture.authority);
    assert_eq!(record.execution_machine, fixture.authority);
    let route = fixture
        .store
        .origin_route_by_request(request_id)
        .unwrap()
        .unwrap();
    assert_eq!(route.task, task);
    assert!(task_has_any_event(&fixture.store.conn, task).unwrap());

    // an inserted row is not live work, so it is not registered
    let saved = fixture.saved_resource();
    assert_eq!(saved.registered_background_task, None);
    assert_eq!(
        saved.state_revision.get(),
        fixture.resource.state_revision.get() + 1
    );

    // a retry after a lost reply carries a new preallocated task but keeps the saved one
    let retry = fixture.input(request_id, fixture.trainer_spec());
    assert_eq!(
        fixture
            .store
            .accept_background_launch_for_authority(retry)
            .unwrap(),
        BackgroundLaunchAcceptance::Existing {
            task,
            state: ProcessStatus::Queued,
        }
    );
    assert_eq!(fixture.task_count(), 1);

    // different content for the same request identity is a conflict and writes nothing
    let changed = fixture.input(
        request_id,
        fixture
            .trainer
            .spec(fixture.resource.supervisor.thread, "another trainer"),
    );
    assert!(matches!(
        fixture
            .store
            .accept_background_launch_for_authority(changed),
        Err(BackgroundLaunchError::ConflictingRetry { .. })
    ));
    assert_eq!(fixture.task_count(), 1);
    assert_eq!(fixture.saved_resource(), saved);
}

#[test]
fn launch_refuses_unverifiable_ownership_and_other_threads_before_writing() {
    let mut fixture = LaunchFixture::new();
    let before = fixture.saved_resource();

    // a shell wrapper can start work outside its foreground process group
    let mut wrapper = fixture.trainer_spec();
    wrapper.workload = NormalizedWorkload::Task(NormalizedTaskWorkload {
        command: CommandLine::try_from_argv(vec![
            "/bin/sh".into(),
            "-c".into(),
            "python3 -m ops.run_segment run &".into(),
        ])
        .unwrap(),
    });
    // a script named like the trainer module path is not the maintained invocation
    let mut shebang = fixture.trainer_spec();
    shebang.workload = NormalizedWorkload::Task(NormalizedTaskWorkload {
        command: CommandLine::try_from_argv(vec!["python3".into(), "ops/run_segment.py".into()])
            .unwrap(),
    });
    for spec in [wrapper, shebang] {
        assert!(matches!(
            fixture
                .store
                .accept_background_launch_for_authority(fixture.input(RequestId::new(), spec)),
            Err(BackgroundLaunchError::UnsupportedCommand(_))
        ));
    }

    let other_thread = fixture.trainer.spec(ThreadId(Uuid::now_v7()), "trainer");
    assert!(matches!(
        fixture
            .store
            .accept_background_launch_for_authority(fixture.input(RequestId::new(), other_thread)),
        Err(BackgroundLaunchError::NotSupervisorThread { .. })
    ));
    assert_eq!(fixture.task_count(), 0);
    assert_eq!(fixture.saved_resource(), before);
}

#[test]
fn remote_supervisor_launch_is_unsupported_and_writes_nothing() {
    let mut fixture = LaunchFixture::new();
    let mut moved = fixture.resource.clone();
    moved.supervisor.machine = MachineId::new();
    fixture
        .store
        .conn
        .execute(
            "UPDATE resources SET supervisor_machine = ?1 WHERE id = ?2",
            params![
                moved.supervisor.machine.as_uuid().to_string(),
                moved.id.as_uuid().to_string()
            ],
        )
        .unwrap();
    let input = fixture.input(RequestId::new(), fixture.trainer_spec());
    assert_eq!(
        fixture
            .store
            .accept_background_launch_for_authority(input)
            .unwrap(),
        BackgroundLaunchAcceptance::UnsupportedRemoteSupervisor {
            authority_machine: fixture.authority,
            supervisor: moved.supervisor,
        }
    );
    assert_eq!(fixture.task_count(), 0);
    let count: i64 = fixture
        .store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_background_launches",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn launch_is_refused_while_queued_work_exists() {
    let mut fixture = LaunchFixture::new();
    let request = fixture.queue_request();
    let input = fixture.input(RequestId::new(), fixture.trainer_spec());
    assert!(matches!(
        fixture.store.accept_background_launch_for_authority(input),
        Err(BackgroundLaunchError::QueuedWorkAhead { request_id }) if request_id == request.request_id
    ));
    assert_eq!(fixture.task_count(), 0);
}

#[test]
fn queued_launch_reserves_the_resource_until_its_confirmed_start_registers_it() {
    let mut fixture = LaunchFixture::new();
    let task = fixture.launch(RequestId::new());
    let later = fixture.queue_request();

    // the queued launch is not proof of idle or of live work
    assert!(matches!(
        fixture.reconcile(),
        ResourceQueueReconcileOutcome::AttentionRequired {
            request,
            reason: ResourceQueueAttentionReason::BackgroundLaunchPending { task_id },
        } if request.request_id == later.request_id && task_id == task
    ));
    assert_eq!(fixture.saved_resource().registered_background_task, None);
    // a second launch cannot race the pending one
    let second = fixture.input(RequestId::new(), fixture.trainer_spec());
    assert!(matches!(
        fixture.store.accept_background_launch_for_authority(second),
        Err(BackgroundLaunchError::QueuedWorkAhead { .. })
    ));

    // the confirmed start registers the task, and the queued request asks for its release
    fixture
        .store
        .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture.reconcile(),
        ResourceQueueReconcileOutcome::ReleaseRequired { loan, .. }
            if matches!(
                loan.state,
                LoanState::Active {
                    phase: LoanPhase::AwaitingRelease { observed_background_task, .. }
                } if observed_background_task == task
            )
    ));
    assert_eq!(
        fixture.saved_resource().registered_background_task,
        Some(task)
    );
}

#[test]
fn launch_that_never_spawned_proves_idle_and_serves_the_queue() {
    let mut fixture = LaunchFixture::new();
    let request_id = RequestId::new();
    let task = fixture.launch(request_id);
    // the task layer fails the row before any worker child starts
    fixture
        .store
        .cas_exit_with_evidence(
            task,
            ProcessStatus::Queued,
            &ExitReason::SpawnFailed {
                message: "no worker".into(),
            },
            ProcessGroupExitEvidence::NoChildSpawned,
        )
        .unwrap()
        .unwrap();
    let request = fixture.queue_request();

    let ResourceQueueReconcileOutcome::IdleServing {
        loan,
        request: assigned,
        proof,
    } = fixture.reconcile()
    else {
        panic!("a launch with no child spawn must prove the idle boundary");
    };
    assert_eq!(
        proof,
        IdleBoundaryProof::BackgroundLaunchNeverSpawned {
            request_id,
            task_id: task
        }
    );
    assert_eq!(assigned.request_id, request.request_id);
    assert!(matches!(
        &loan.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: ReturnContext::Idle,
                release_provenance: ServingReleaseProvenance::IdleBoundary { .. },
                ..
            }
        }
    ));

    // the saved opening receipt is the provenance that lets the command start
    let saved = fixture.saved_resource();
    let input = ResourceTaskAcceptanceInput {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        request_id: request.request_id,
        task_id: request.task_id,
        acceptance_sequence: request.acceptance_sequence,
        loan_id: loan.id,
        expected_state_revision: saved.state_revision,
        command_spec: request.spec().clone(),
        executor_env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
    };
    fixture
        .store
        .conn
        .execute("DELETE FROM resource_idle_openings", [])
        .unwrap();
    assert!(matches!(
        fixture.store.accept_assigned_resource_task(input),
        Err(ResourceStoreError::Conflict(_))
    ));
}

#[test]
fn idle_serving_activates_only_with_its_opening_receipt() {
    let mut fixture = LaunchFixture::new();
    let task = fixture.launch(RequestId::new());
    fixture
        .store
        .cas_exit(task, ProcessStatus::Queued, &ExitReason::Cancelled)
        .unwrap()
        .unwrap();
    let request = fixture.queue_request();
    let ResourceQueueReconcileOutcome::IdleServing { loan, .. } = fixture.reconcile() else {
        panic!("a cancelled queued launch must prove the idle boundary");
    };
    let input = ResourceTaskAcceptanceInput {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        request_id: request.request_id,
        task_id: request.task_id,
        acceptance_sequence: request.acceptance_sequence,
        loan_id: loan.id,
        expected_state_revision: fixture.saved_resource().state_revision,
        command_spec: request.spec().clone(),
        executor_env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
    };
    assert_eq!(
        fixture.store.accept_assigned_resource_task(input).unwrap(),
        ResourceTaskAcceptance::Inserted {
            task: request.task_id
        }
    );
}

#[test]
fn launch_that_ran_before_registration_keeps_the_queue_reserved() {
    let mut fixture = LaunchFixture::new();
    let task = fixture.launch(RequestId::new());
    // the worker started and exited before the owner observed the running row
    fixture
        .store
        .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    fixture
        .store
        .cas_exit_with_evidence(
            task,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
    let request = fixture.queue_request();
    assert!(matches!(
        fixture.reconcile(),
        ResourceQueueReconcileOutcome::AttentionRequired {
            request: blocked,
            reason: ResourceQueueAttentionReason::IdleNotProven {
                gap: IdleProofGap::BackgroundLaunchReleaseUnproven { task_id },
            },
        } if blocked.request_id == request.request_id && task_id == task
    ));
    assert!(saved_loan_for(&fixture).is_none());
}

#[test]
fn trainer_association_waits_for_registration_and_the_exact_runtime_root() {
    let mut fixture = LaunchFixture::new();
    let task = fixture.launch(RequestId::new());
    let evidence = |root: &Path| {
        VerifiedTrainerAttempt::from_persisted(
            root.to_path_buf(),
            trainer_attempt_test_support::attempt_binding("attempt-1"),
            TrainerRequestDigest::from_hex(&"00".repeat(32)).unwrap(),
            OwnershipLockIdentity::new(1, 2),
        )
        .unwrap()
    };

    // a queued launch does not hold the trainer lock, so nothing may assert that it does
    assert!(matches!(
        fixture.store.bind_trainer_attempt_association(
            fixture.authority,
            fixture.resource.id,
            task,
            evidence(&fixture.trainer.runtime_root),
        ),
        Err(TrainerAttemptAssociationStoreError::TaskNotRegistered { .. })
    ));

    fixture
        .store
        .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture.reconcile(),
        ResourceQueueReconcileOutcome::NoQueuedRequest
    ));
    assert_eq!(
        fixture.saved_resource().registered_background_task,
        Some(task)
    );

    // evidence from another runtime root cannot bind to the accepted trainer command
    let other_root = fixture.trainer.cwd.join("other-runtime");
    fs::create_dir_all(&other_root).unwrap();
    assert!(matches!(
        fixture.store.bind_trainer_attempt_association(
            fixture.authority,
            fixture.resource.id,
            task,
            evidence(&other_root),
        ),
        Err(TrainerAttemptAssociationStoreError::DirectSegmentCommandShape(
            crate::resource::command_shape::DirectSegmentCommandShapeError::RuntimeRootMismatch { .. }
        ))
    ));

    let association = fixture
        .store
        .bind_trainer_attempt_association(
            fixture.authority,
            fixture.resource.id,
            task,
            evidence(&fixture.trainer.runtime_root),
        )
        .unwrap();
    assert_eq!(association.task_id(), task);
}

#[test]
fn an_ended_launch_frees_the_slot_for_the_next_launch() {
    let mut fixture = LaunchFixture::new();
    let first = fixture.launch(RequestId::new());
    fixture
        .store
        .cas_exit(first, ProcessStatus::Queued, &ExitReason::Cancelled)
        .unwrap()
        .unwrap();
    // an ended launch no longer occupies the slot, so the next launch binds
    let second = fixture.launch(RequestId::new());
    assert_ne!(first, second);
    let request = fixture.queue_request();
    assert!(matches!(
        fixture.reconcile(),
        ResourceQueueReconcileOutcome::AttentionRequired {
            request: blocked,
            reason: ResourceQueueAttentionReason::BackgroundLaunchPending { task_id },
        } if blocked.request_id == request.request_id && task_id == second
    ));
}

/// Launch a trainer and let the queue owner register its confirmed start
pub(super) fn registered_launch(fixture: &mut LaunchFixture, request_id: RequestId) -> TaskId {
    let task = fixture.launch(request_id);
    fixture
        .store
        .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    // with no queued work the reconciliation only registers the confirmed start
    assert!(matches!(
        fixture.reconcile(),
        ResourceQueueReconcileOutcome::NoQueuedRequest
    ));
    assert_eq!(
        fixture.saved_resource().registered_background_task,
        Some(task)
    );
    task
}

#[test]
fn predecessor_without_exit_proof_keeps_the_background_slot() {
    type End = fn(&mut LaunchFixture, TaskId);
    let lose: End = |fixture, task| {
        fixture
            .store
            .cas_status(task, ProcessStatus::Running, ProcessStatus::Lost)
            .unwrap()
            .unwrap();
    };
    let unconfirmed: End = |fixture, task| {
        fixture
            .store
            .cas_exit_with_evidence(
                task,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
                ProcessGroupExitEvidence::Unconfirmed,
            )
            .unwrap()
            .unwrap();
    };
    for (name, registered, end) in [
        ("lost wrapper before registration", false, lose),
        ("lost registered trainer", true, lose),
        ("unconfirmed exit before registration", false, unconfirmed),
        ("unconfirmed registered exit", true, unconfirmed),
    ] {
        let mut fixture = LaunchFixture::new();
        let request_id = RequestId::new();
        let task = if registered {
            registered_launch(&mut fixture, request_id)
        } else {
            let task = fixture.launch(request_id);
            fixture
                .store
                .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
                .unwrap()
                .unwrap();
            task
        };
        end(&mut fixture, task);
        let before = fixture.saved_resource();

        let next = fixture.input(RequestId::new(), fixture.trainer_spec());
        assert!(
            matches!(
                fixture.store.accept_background_launch_for_authority(next),
                Err(BackgroundLaunchError::PredecessorReleaseUnproven { task_id }) if task_id == task
            ),
            "{name}"
        );
        assert_eq!(fixture.task_count(), 1, "{name}");
        assert_eq!(fixture.saved_resource(), before, "{name}");
        // the accepted launch still answers its exact retry from the receipt
        let retry = fixture.input(request_id, fixture.trainer_spec());
        assert!(
            matches!(
                fixture.store.accept_background_launch_for_authority(retry),
                Ok(BackgroundLaunchAcceptance::Existing { task: existing, .. }) if existing == task
            ),
            "{name}"
        );
    }
}

/// Record exact evidence for a registered trainer while a holder keeps its new lock
///
/// Returns the lock path. The holder is dropped before return, as if the
/// supervisor bound the association while the worker was running
fn bind_real_trainer_lock(fixture: &mut LaunchFixture, task: TaskId) -> PathBuf {
    let runtime_root = fixture.trainer.runtime_root.clone();
    let binding = trainer_attempt_test_support::attempt_binding("attempt-1");
    crate::resource::trainer_publication::tests::write_request_for_test(&runtime_root, &binding);
    let lock_path = runtime_root.join(".segment.lock");
    let running_worker = acquire_test_lock(&lock_path, true);
    let evidence = build_trainer_attempt_registration_evidence(&runtime_root, &binding).unwrap();
    fixture
        .store
        .bind_trainer_attempt_association(fixture.authority, fixture.resource.id, task, evidence)
        .unwrap();
    drop(running_worker);
    lock_path
}

/// Let the wrapper of a running task exit with a confirmed process-group exit
fn end_wrapper(fixture: &mut LaunchFixture, task: TaskId) {
    fixture
        .store
        .cas_exit_with_evidence(
            task,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
}

#[test]
fn confirmed_wrapper_exit_frees_the_slot_only_after_the_exact_trainer_lock_is_free() {
    let mut fixture = LaunchFixture::new();
    let first_request = RequestId::new();
    let first = registered_launch(&mut fixture, first_request);
    let lock_path = bind_real_trainer_lock(&mut fixture, first);
    end_wrapper(&mut fixture, first);

    // the worker runs in its own session and outlives the wrapper that confirmed exit
    let mut worker = start_fake_lock_process(&lock_path);
    let before = fixture.saved_resource();
    let blocked = fixture.input(RequestId::new(), fixture.trainer_spec());
    let blocked_task = blocked.task_id;
    assert!(matches!(
        fixture.store.accept_background_launch_for_authority(blocked),
        Err(BackgroundLaunchError::PredecessorOwnershipUnproven {
            task_id,
            gap: TrainerLockReleaseGap::OwnershipHeld { task_id: witness },
        }) if task_id == first && witness == first
    ));
    assert_eq!(fixture.task_count(), 1);
    assert!(fixture.store.get_task(blocked_task).unwrap().is_none());
    assert_eq!(fixture.saved_resource(), before);

    worker.release();
    let second = fixture.launch(RequestId::new());
    assert_ne!(first, second);
    // the authority held the lock only through the launch transaction
    let identity = OwnershipLockIdentity::from_metadata(&fs::metadata(&lock_path).unwrap());
    assert!(matches!(
        probe_segment_ownership_lock(&fixture.trainer.runtime_root, identity),
        OwnershipLockProbe::ExactOwnershipReleased(_)
    ));
    // the unchanged first launch is answered from its receipt and spawns nothing
    let retry = fixture.input(first_request, fixture.trainer_spec());
    assert_eq!(
        fixture
            .store
            .accept_background_launch_for_authority(retry)
            .unwrap(),
        BackgroundLaunchAcceptance::Existing {
            task: first,
            state: ProcessStatus::Succeeded,
        }
    );
    assert_eq!(fixture.task_count(), 2);
}

#[test]
fn wrapper_exit_before_registration_has_no_lock_witness_and_keeps_the_slot() {
    let mut fixture = LaunchFixture::new();
    let task = fixture.launch(RequestId::new());
    fixture
        .store
        .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    end_wrapper(&mut fixture, task);

    let before = fixture.saved_resource();
    let next = fixture.input(RequestId::new(), fixture.trainer_spec());
    assert!(matches!(
        fixture.store.accept_background_launch_for_authority(next),
        Err(BackgroundLaunchError::PredecessorOwnershipUnproven {
            task_id,
            gap: TrainerLockReleaseGap::AssociationMissing { task_id: witness },
        }) if task_id == task && witness == task
    ));
    assert_eq!(fixture.task_count(), 1);
    assert_eq!(fixture.saved_resource(), before);
}

#[test]
fn changed_or_missing_trainer_lock_evidence_keeps_the_slot() {
    type Change = fn(&LaunchFixture, TaskId, &Path);
    let replace_lock: Change = |_, _, lock_path| {
        fs::rename(lock_path, lock_path.with_extension("lock.saved")).unwrap();
        fs::write(lock_path, b"").unwrap();
    };
    let change_saved_identity: Change = |fixture, task, _| {
        let saved = saved_trainer_association_json(&fixture.store, task);
        let mut association: serde_json::Value = serde_json::from_str(&saved).unwrap();
        let inode = association["ownership_lock_identity"]["inode"]
            .as_u64()
            .unwrap();
        association["ownership_lock_identity"]["inode"] = (inode + 1).into();
        fixture
            .store
            .conn
            .execute(
                "UPDATE trainer_attempt_associations SET association_json = ?1 WHERE task_id = ?2",
                params![association.to_string(), task.to_string()],
            )
            .unwrap();
    };
    let remove_association: Change = |fixture, task, _| {
        fixture
            .store
            .conn
            .execute(
                "DELETE FROM trainer_attempt_associations WHERE task_id = ?1",
                [task.to_string()],
            )
            .unwrap();
    };
    type Expected = fn(&TrainerLockReleaseGap, TaskId) -> bool;
    let different_file: Expected = |gap, _| {
        matches!(
            gap,
            TrainerLockReleaseGap::OwnershipLock(OwnershipLockProbeError::IdentityMismatch {
                reason: OwnershipLockIdentityMismatchReason::DifferentFile,
                ..
            })
        )
    };
    let missing: Expected = |gap, task| matches!(gap, TrainerLockReleaseGap::AssociationMissing { task_id } if *task_id == task);
    for (name, change, expected) in [
        ("replaced lock file", replace_lock, different_file),
        (
            "changed saved lock identity",
            change_saved_identity,
            different_file,
        ),
        ("missing association", remove_association, missing),
    ] {
        let mut fixture = LaunchFixture::new();
        let first = registered_launch(&mut fixture, RequestId::new());
        let lock_path = bind_real_trainer_lock(&mut fixture, first);
        end_wrapper(&mut fixture, first);
        change(&fixture, first, &lock_path);

        // no process holds any lock, yet a free file is not the saved lock
        let before = fixture.saved_resource();
        let next = fixture.input(RequestId::new(), fixture.trainer_spec());
        match fixture.store.accept_background_launch_for_authority(next) {
            Err(BackgroundLaunchError::PredecessorOwnershipUnproven { task_id, gap }) => {
                assert_eq!(task_id, first, "{name}");
                assert!(expected(&gap, first), "{name}: {gap:?}");
            }
            other => panic!("{name}: expected a fail-closed slot, got {other:?}"),
        }
        assert_eq!(fixture.task_count(), 1, "{name}");
        assert_eq!(fixture.saved_resource(), before, "{name}");
    }
}

pub(super) fn saved_loan_for(fixture: &LaunchFixture) -> Option<Loan> {
    fixture
        .store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == fixture.resource.id)
        .unwrap()
        .loan
}

/// Remote supervisor machine and its saved launch identities for one fixture resource
struct RemoteLaunch {
    receipt: RemoteBackgroundLaunchReceipt,
    spec: NormalizedSpec,
}

impl LaunchFixture {
    /// Fixture whose supervisor thread runs on another machine
    fn remote() -> Self {
        let mut fixture = Self::new();
        let supervisor = MachineId::new();
        fixture
            .store
            .conn
            .execute(
                "UPDATE resources SET supervisor_machine = ?1 WHERE id = ?2",
                params![
                    supervisor.as_uuid().to_string(),
                    fixture.resource.id.as_uuid().to_string()
                ],
            )
            .unwrap();
        fixture.resource = fixture.saved_resource();
        fixture
    }

    /// Launch that the supervisor machine saved for the current resource state
    fn remote_launch(&self, name: &str) -> RemoteLaunch {
        let resource = self.saved_resource();
        let spec = self.trainer.spec(resource.supervisor.thread, name);
        RemoteLaunch {
            receipt: RemoteBackgroundLaunchReceipt {
                binding: BackgroundLaunchBinding {
                    assignment: BackgroundSupervisorAssignment::of(&resource),
                    expected_state_revision: resource.state_revision,
                },
                request_id: RequestId::new(),
                task_id: TaskId::new(),
                normalized_spec_sha256: normalized_spec_sha256(&spec).unwrap(),
            },
            spec,
        }
    }

    fn accept_remote(
        &mut self,
        launch: &RemoteLaunch,
    ) -> Result<BackgroundLaunchAcceptance, BackgroundLaunchError> {
        self.store
            .accept_remote_background_launch_for_authority(RemoteBackgroundLaunchInput {
                receipt: launch.receipt,
                spec: launch.spec.clone(),
                env: self.trainer.env.clone(),
            })
    }
}

#[test]
fn remote_launch_keeps_the_callback_owner_remote_and_exact_retry_only_observes() {
    let mut fixture = LaunchFixture::remote();
    let launch = fixture.remote_launch("remote trainer");
    let task = launch.receipt.task_id;
    assert!(matches!(
        fixture.accept_remote(&launch).unwrap(),
        BackgroundLaunchAcceptance::Inserted { task: inserted, .. } if inserted == task
    ));

    // the authority keeps the execution identity and first event; the route stays remote
    let Some(ExecutorIdentity::Accepted(record)) = fixture.store.executor_identity(task).unwrap()
    else {
        panic!("the launch must save an accepted executor identity");
    };
    assert_eq!(record.origin_machine, fixture.resource.supervisor.machine);
    assert_eq!(record.execution_machine, fixture.authority);
    assert!(
        fixture
            .store
            .origin_route_by_request(launch.receipt.request_id)
            .unwrap()
            .is_none()
    );
    assert!(
        crate::store::initial_queued_event_matches_on(
            &fixture.store.conn,
            task,
            fixture.resource.supervisor.machine,
            fixture.authority,
        )
        .unwrap()
    );
    let launched = fixture
        .store
        .background_launch_for_authority(fixture.authority, fixture.resource.id)
        .unwrap()
        .unwrap();
    assert_eq!(launched.phase, BackgroundLaunchPhase::Queued);
    let saved = fixture.saved_resource();
    assert_eq!(saved.registered_background_task, None);

    // a lost reply is answered from the receipt, even after the revision moved
    assert_eq!(
        fixture.accept_remote(&launch).unwrap(),
        BackgroundLaunchAcceptance::Existing {
            task,
            state: ProcessStatus::Queued,
        }
    );

    // changed content, another task, or a co-located call cannot reuse the request
    let mut changed = fixture.remote_launch("changed trainer");
    changed.receipt.request_id = launch.receipt.request_id;
    let mut other_task = fixture.remote_launch("remote trainer");
    other_task.receipt.request_id = launch.receipt.request_id;
    for retry in [changed, other_task] {
        assert!(matches!(
            fixture.accept_remote(&retry),
            Err(BackgroundLaunchError::ConflictingRetry { .. })
        ));
    }
    let co_located = fixture.input(launch.receipt.request_id, launch.spec.clone());
    assert!(matches!(
        fixture
            .store
            .accept_background_launch_for_authority(co_located),
        Err(BackgroundLaunchError::ConflictingRetry { .. })
    ));
    assert_eq!(fixture.task_count(), 1);
    assert_eq!(fixture.saved_resource(), saved);
}

#[test]
fn remote_launch_refuses_a_stale_assignment_revision_or_queue_before_writing() {
    let mut fixture = LaunchFixture::remote();

    let mut old_assignment = fixture.remote_launch("trainer");
    old_assignment
        .receipt
        .binding
        .assignment
        .assignment_revision = AssignmentRevision::new(7);
    let mut other_thread = fixture.remote_launch("trainer");
    other_thread.receipt.binding.assignment.supervisor.thread = ThreadId(Uuid::now_v7());
    for launch in [old_assignment, other_thread] {
        assert!(matches!(
            fixture.accept_remote(&launch),
            Err(BackgroundLaunchError::NotCurrentSupervisor)
        ));
    }
    let mut stale = fixture.remote_launch("trainer");
    stale.receipt.binding.expected_state_revision = ResourceRevision::new(9);
    assert!(matches!(
        fixture.accept_remote(&stale),
        Err(BackgroundLaunchError::StaleRevision { .. })
    ));

    // queued work blocks a first launch
    let request = fixture.queue_request();
    let launch = fixture.remote_launch("trainer");
    assert!(matches!(
        fixture.accept_remote(&launch),
        Err(BackgroundLaunchError::QueuedWorkAhead { request_id }) if request_id == request.request_id
    ));
    assert_eq!(fixture.task_count(), 0);

    // a co-located resource cannot accept a launch that claims a remote callback owner
    let mut co_located = LaunchFixture::new();
    let mut claimed = co_located.remote_launch("trainer");
    claimed.receipt.binding.assignment.supervisor.machine = MachineId::new();
    assert!(matches!(
        co_located.accept_remote(&claimed),
        Err(BackgroundLaunchError::NotCurrentSupervisor)
    ));
    assert_eq!(co_located.task_count(), 0);
}

#[test]
fn remote_launch_registers_on_its_start_and_a_replaced_supervisor_cannot_launch_again() {
    let mut fixture = LaunchFixture::remote();
    let launch = fixture.remote_launch("remote trainer");
    let task = launch.receipt.task_id;
    fixture.accept_remote(&launch).unwrap();
    // a second launch cannot race the pending one
    assert!(matches!(
        fixture.accept_remote(&fixture.remote_launch("second")),
        Err(BackgroundLaunchError::LaunchPending { task_id }) if task_id == task
    ));

    fixture
        .store
        .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture.reconcile(),
        ResourceQueueReconcileOutcome::NoQueuedRequest
    ));
    assert_eq!(
        fixture.saved_resource().registered_background_task,
        Some(task)
    );

    // a replacement supervisor cannot start another trainer while this one runs
    let replaced = fixture.saved_resource();
    fixture
        .store
        .conn
        .execute(
            "UPDATE resources SET supervisor_thread = ?1, assignment_revision = ?2 WHERE id = ?3",
            params![
                ThreadId(Uuid::now_v7()).to_string(),
                i64::try_from(replaced.assignment_revision.get() + 1).unwrap(),
                replaced.id.as_uuid().to_string()
            ],
        )
        .unwrap();
    assert!(matches!(
        fixture.accept_remote(&fixture.remote_launch("replacement")),
        Err(BackgroundLaunchError::BackgroundTaskActive { task_id, .. }) if task_id == task
    ));
    // the old supervisor's retry only observes its running task
    assert_eq!(
        fixture.accept_remote(&launch).unwrap(),
        BackgroundLaunchAcceptance::Existing {
            task,
            state: ProcessStatus::Running,
        }
    );
    assert_eq!(fixture.task_count(), 1);
}
