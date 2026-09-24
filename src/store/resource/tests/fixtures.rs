//! Shared fixtures for authority-local resource store tests

use crate::domain::TaskRow;
use crate::submission::ResourceRouteProof;
use nix::fcntl::{Flock, FlockArg};

use crate::cancellation::ResourceCancellationRequestIdentity;
use crate::daemon::actors::resource::ResourceMsg;
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId, TaskWorkload, ThreadId,
    Workload,
};
use crate::home::Home;
use crate::machine::MachineId;
use crate::resource::ownership_lock::{
    OwnershipLockIdentity, TrainerRequestDigest, VerifiedTrainerAttempt,
    build_trainer_attempt_registration_evidence, test_support as trainer_attempt_test_support,
};
use crate::resource::release_watcher::ReleaseWatcherCommand;
use crate::resource::store::{
    AssignedResourceTaskReconcileInput, AssignedResourceTaskReconcileOutcome,
    OpenReleaseLoanResult, ReleaseCheckpointCancellationOutcome, ReleaseCompletionResult,
    ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError, ReleaseWatcherAcceptanceInput,
    ResourceTaskAcceptance, ResourceTaskAcceptanceInput, ResourceTaskCompletionResult,
    TrainerAttemptAssociationStoreError,
};
use crate::resource::trainer_publication::AttemptBinding;
use crate::resource::{
    ActionId, AssignmentRevision, Loan, LoanId, LoanPhase, LoanState, ReleaseCheckpointBaseline,
    ReleaseCheckpointCancellation, ReleaseCheckpointStopDecision, ReleaseCheckpointStopOutcome,
    ReleaseWatcherIntent, ReleaseWatcherTaskId, Resource, ResourceId, ResourceRequest,
    ResourceRevision, ReturnContext, ServingReleaseProvenance, SupervisorAddress, SupervisorNotice,
    TrainerAttemptAssociation,
};
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::resource::trainer_association::{
    trainer_association_by_resource_and_task, trainer_association_json,
};
use crate::store::{ExecutorIdentity, NewTask, Store, new_queued_task};
use crate::submission::{
    CallbackContext, CallbackExecutable, NewResourceRoute, OriginRoute, PreAcceptanceRejection,
    RequestId, ResourceQueueOutcome, ResourceQueueReceipt, ResourceRoutePhase,
    normalized_spec_sha256,
};
use rusqlite::params;
use serde_json::json;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use uuid::Uuid;

pub(super) fn resource(authority: MachineId) -> Resource {
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

pub(super) fn machine_other_than(machine: MachineId) -> MachineId {
    let first = MachineId::from_uuid(Uuid::from_u128(1));
    if first != machine {
        return first;
    }

    MachineId::from_uuid(Uuid::from_u128(2))
}

/// Hold an exclusive lock on `path` the way a running trainer worker does
///
/// No child inherits the test descriptor, so the lock may end when the guard drops
pub(super) fn acquire_test_lock(path: &Path, create: bool) -> Flock<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if create {
        options.create_new(true);
    }
    let file = options.open(path).unwrap();
    Flock::lock(file, FlockArg::LockExclusiveNonblock)
        .map_err(|(_, errno)| errno)
        .unwrap()
}

pub(super) fn spec() -> NormalizedSpec {
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

pub(super) fn resource_cancellation_identity(
    cancellation: uuid::Uuid,
    request: RequestId,
    task: TaskId,
    resource: ResourceId,
    origin: MachineId,
    authority: MachineId,
    target_phase: ResourceRoutePhase,
) -> ResourceCancellationRequestIdentity {
    ResourceCancellationRequestIdentity {
        requester_machine: origin,
        cancellation,
        request,
        task,
        origin_machine: origin,
        authority_machine: authority,
        resource,
        target_phase,
    }
}

pub(super) fn resource_cancellation_proof(
    identity: &ResourceCancellationRequestIdentity,
    normalized_spec: &NormalizedSpec,
    phase: ResourceRoutePhase,
) -> ResourceRouteProof {
    ResourceRouteProof {
        request: identity.request,
        task: identity.task,
        origin_machine: identity.origin_machine,
        authority_machine: identity.authority_machine,
        resource: identity.resource,
        thread: normalized_spec.thread,
        normalized_spec_sha256: crate::submission::normalized_spec_sha256(normalized_spec).unwrap(),
        phase,
    }
}

pub(super) fn remote_task(task: TaskId, spec: &NormalizedSpec) -> TaskRow {
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

pub(super) fn direct_segment_spec(
    trainer_root: &Path,
    task_file: &Path,
    input_root: &Path,
    runtime_root: &Path,
) -> NormalizedSpec {
    serde_json::from_value(json!({
        "api_version": 1,
        "thread": Uuid::now_v7(),
        "name": "direct segment trainer",
        "cwd": trainer_root,
        "timeout": "4h",
        "workload": {
            "type": "task",
            "command": [
                "python3",
                "-m",
                "ops.run_segment",
                "run",
                "--task",
                task_file,
                "--input-root",
                input_root,
                "--runtime-root",
                runtime_root,
                "--image-digest",
                "test-image-digest"
            ]
        }
    }))
    .unwrap()
}

pub(super) fn trainer_task(
    task: TaskId,
    spec: &NormalizedSpec,
    python: &Path,
    bin: &Path,
    home: &Path,
) -> TaskRow {
    let NormalizedWorkload::Task(workload) = spec.workload.clone() else {
        panic!("trainer spec must be a command");
    };
    let requested_program = workload.command.program();
    let binary = if requested_program == "python3" {
        python.to_path_buf()
    } else {
        PathBuf::from(requested_program)
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
            path: bin.to_string_lossy().into_owned(),
            home: home.to_string_lossy().into_owned(),
        },
        binary,
    })
}

pub(super) struct TrainerAssociationFixture {
    pub(super) _directory: tempfile::TempDir,
    pub(super) database: PathBuf,
    pub(super) store: Store,
    pub(super) authority: MachineId,
    pub(super) resource: Resource,
    pub(super) task_id: TaskId,
    pub(super) spec: NormalizedSpec,
    pub(super) trainer_root: PathBuf,
    pub(super) task_file: PathBuf,
    pub(super) input_root: PathBuf,
    pub(super) runtime_root: PathBuf,
    pub(super) bin: PathBuf,
    pub(super) python: PathBuf,
    pub(super) home: PathBuf,
}

impl TrainerAssociationFixture {
    pub(super) fn new() -> Self {
        let directory = tempdir().unwrap();
        let authority = MachineId::new();
        let database = directory.path().join("db");
        Self::new_with_database(directory, database, authority)
    }

    pub(super) fn new_for_home(
        directory: tempfile::TempDir,
        home: &Home,
        authority: MachineId,
    ) -> Self {
        Self::new_with_database(directory, home.db_path(), authority)
    }

    pub(super) fn new_with_database(
        directory: tempfile::TempDir,
        database: PathBuf,
        authority: MachineId,
    ) -> Self {
        let mut store = Store::open(&database).unwrap();
        let task_id = TaskId::new();
        let home = directory.path().canonicalize().unwrap();
        let trainer_root = home.join("trainer");
        let ops = trainer_root.join("ops");
        fs::create_dir_all(&ops).unwrap();
        fs::write(ops.join("run_segment.py"), b"# maintained trainer\n").unwrap();
        fs::write(
            ops.join("segment_artifacts.py"),
            b"# maintained artifacts\n",
        )
        .unwrap();

        let task_file = home.join("task.json");
        fs::write(&task_file, b"{}\n").unwrap();
        let input_root = home.join("inputs");
        fs::create_dir(&input_root).unwrap();
        let runtime_root = home.join("runtime");
        fs::create_dir(&runtime_root).unwrap();

        let bin = home.join("bin");
        fs::create_dir(&bin).unwrap();
        let python = bin.join("python3");
        fs::write(&python, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&python, fs::Permissions::from_mode(0o755)).unwrap();

        let spec = direct_segment_spec(&trainer_root, &task_file, &input_root, &runtime_root);
        let mut resource = resource(authority);
        resource.supervisor.thread = spec.thread;
        resource.registered_background_task = Some(task_id);
        store.register_resource(authority, &resource).unwrap();

        Self {
            _directory: directory,
            database,
            store,
            authority,
            resource,
            task_id,
            spec,
            trainer_root,
            task_file,
            input_root,
            runtime_root,
            bin,
            python,
            home,
        }
    }

    pub(super) fn insert_accepted_running_task(&mut self) {
        let row = self.trainer_task(self.task_id, &self.spec);
        self.store
            .insert_local_task(
                &row,
                &self.spec,
                self.authority,
                PathBuf::from("/bin/echo").into(),
            )
            .unwrap();
        self.store
            .cas_status(self.task_id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();
    }

    pub(super) fn evidence(&self) -> VerifiedTrainerAttempt {
        VerifiedTrainerAttempt::from_persisted(
            self.runtime_root.clone(),
            trainer_attempt_test_support::attempt_binding("attempt-1"),
            TrainerRequestDigest::from_hex(&"42".repeat(32)).unwrap(),
            OwnershipLockIdentity::new(7, 11),
        )
        .unwrap()
    }

    pub(super) fn register_release_attempt(&mut self) -> AttemptBinding {
        let binding = trainer_attempt_test_support::attempt_binding("attempt-1");
        crate::resource::trainer_publication::tests::write_request_for_test(
            &self.runtime_root,
            &binding,
        );
        let lock_path = self.runtime_root.join(".segment.lock");
        let lock_file = acquire_test_lock(&lock_path, true);
        let evidence =
            build_trainer_attempt_registration_evidence(&self.runtime_root, &binding).unwrap();
        self.bind(evidence).unwrap();
        drop(lock_file);
        binding
    }

    pub(super) fn hold_saved_lock(&self) -> Flock<File> {
        acquire_test_lock(&self.runtime_root.join(".segment.lock"), false)
    }

    pub(super) fn trainer_task(&self, task: TaskId, spec: &NormalizedSpec) -> TaskRow {
        trainer_task(task, spec, &self.python, &self.bin, &self.home)
    }

    pub(super) fn command(&self) -> Vec<String> {
        let NormalizedWorkload::Task(workload) = &self.spec.workload else {
            panic!("trainer spec must be a command");
        };
        workload.command.to_vec()
    }

    pub(super) fn set_command(&mut self, command: Vec<String>) {
        let NormalizedWorkload::Task(workload) = &mut self.spec.workload else {
            panic!("trainer spec must be a command");
        };
        workload.command = crate::invocation::CommandLine::try_from_argv(command).unwrap();
    }

    pub(super) fn remove_shape_files(&self) {
        fs::remove_file(&self.task_file).unwrap();
        fs::remove_dir(&self.input_root).unwrap();
        fs::remove_dir(&self.runtime_root).unwrap();
        fs::remove_dir_all(&self.trainer_root).unwrap();
        fs::remove_dir_all(&self.bin).unwrap();
    }

    pub(super) fn finish_registered_task(&mut self) {
        self.store
            .cas_exit(
                self.task_id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
            )
            .unwrap()
            .unwrap();
        self.store
            .update_execution_state(self.task_id, ProcessStatus::Succeeded)
            .unwrap();
    }

    pub(super) fn finish_registered_task_with_evidence(
        &mut self,
        evidence: ProcessGroupExitEvidence,
    ) {
        self.store
            .cas_exit_with_evidence(
                self.task_id,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
                evidence,
            )
            .unwrap()
            .unwrap();
        self.store
            .update_execution_state(self.task_id, ProcessStatus::Succeeded)
            .unwrap();
    }

    pub(super) fn finish_registered_task_cancelled_with_evidence(
        &mut self,
        evidence: ProcessGroupExitEvidence,
    ) {
        self.store
            .cas_exit_with_evidence(
                self.task_id,
                ProcessStatus::Running,
                &ExitReason::Cancelled,
                evidence,
            )
            .unwrap()
            .unwrap();
        self.store
            .update_execution_state(self.task_id, ProcessStatus::Cancelled)
            .unwrap();
    }

    pub(super) fn bind(
        &mut self,
        evidence: VerifiedTrainerAttempt,
    ) -> Result<TrainerAttemptAssociation, TrainerAttemptAssociationStoreError> {
        self.store.bind_trainer_attempt_association(
            self.authority,
            self.resource.id,
            self.task_id,
            evidence,
        )
    }

    pub(super) fn add_loan(&self, state: LoanState) {
        self.store
            .conn
            .execute(
                "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
                params![
                    LoanId::new().as_uuid().to_string(),
                    self.resource.id.as_uuid().to_string(),
                    serde_json::to_string(&state).unwrap(),
                ],
            )
            .unwrap();
    }
}

pub(super) fn saved_trainer_association_json(store: &Store, task_id: TaskId) -> String {
    store
        .conn
        .query_row(
            "SELECT association_json FROM trainer_attempt_associations WHERE task_id=?1",
            [task_id.to_string()],
            |row| row.get(0),
        )
        .unwrap()
}

pub(super) fn awaiting_release_state(task_id: TaskId) -> LoanState {
    LoanState::Active {
        phase: LoanPhase::AwaitingRelease {
            action_id: ActionId::new(),
            observed_background_task: task_id,
            watcher_intent: None,
        },
    }
}

pub(super) fn open_release_for_test(
    store: &mut Store,
    authority: MachineId,
    background_task: TaskId,
) -> (Resource, Loan, SupervisorNotice) {
    let mut resource = resource(authority);
    resource.supervisor.thread = spec().thread;
    resource.registered_background_task = Some(background_task);
    store.register_resource(authority, &resource).unwrap();
    let trainer_spec = spec();
    let trainer_row = remote_task(background_task, &trainer_spec);
    store
        .insert_local_task(
            &trainer_row,
            &trainer_spec,
            authority,
            PathBuf::from("/bin/echo").into(),
        )
        .unwrap();
    store
        .cas_status(
            background_task,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    store
        .update_execution_state(background_task, ProcessStatus::Running)
        .unwrap();
    test_release_association(store, authority, resource.id, background_task);
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

    let OpenReleaseLoanResult::Opened { loan, notice } = store
        .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
        .unwrap()
    else {
        panic!("fixture must create a new release action");
    };

    (resource, loan, notice)
}

pub(super) fn test_release_association(
    store: &mut Store,
    authority: MachineId,
    resource_id: ResourceId,
    task_id: TaskId,
) -> PathBuf {
    let runtime_root = store.tasks_dir.join(format!("release-proof-{task_id}"));
    fs::create_dir_all(&runtime_root).unwrap();
    let attempt_binding = AttemptBinding {
        campaign_id: "release-campaign".into(),
        campaign_revision_id: "release-revision".into(),
        task_id: "release-trainer-task".into(),
        attempt_id: format!("attempt-{}", task_id.0.simple()),
        attempt_number: 1,
        ownership_token: "release-owner".into(),
    };
    let evidence = VerifiedTrainerAttempt::from_persisted(
        runtime_root.clone(),
        attempt_binding,
        TrainerRequestDigest::from_hex(&"42".repeat(32)).unwrap(),
        OwnershipLockIdentity::new(7, 11),
    )
    .unwrap();
    let association = TrainerAttemptAssociation::from_components(
        resource_id,
        authority,
        task_id,
        evidence,
        normalized_spec_sha256(&spec()).unwrap(),
    )
    .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO trainer_attempt_associations
             (task_id, resource_id, authority_machine, association_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                task_id.to_string(),
                resource_id.as_uuid().to_string(),
                authority.as_uuid().to_string(),
                trainer_association_json(&association).unwrap(),
            ],
        )
        .unwrap();
    runtime_root
}

pub(super) struct ServingFixture {
    pub(super) directory: tempfile::TempDir,
    pub(super) store: Store,
    pub(super) authority: MachineId,
    pub(super) origin: MachineId,
    pub(super) resource: Resource,
    pub(super) request: ResourceRequest,
    pub(super) loan: Loan,
    pub(super) state_revision: ResourceRevision,
    pub(super) spec: NormalizedSpec,
}

pub(super) fn resource_origin_route(
    request: RequestId,
    task: TaskId,
    resource: ResourceId,
    origin: MachineId,
    authority: MachineId,
    spec: &NormalizedSpec,
) -> OriginRoute {
    OriginRoute::new_resource_waiting(NewResourceRoute {
        request,
        task,
        origin_machine: origin,
        authority_machine: authority,
        thread: spec.thread,
        callback: CallbackContext {
            env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
            cwd: PathBuf::from("/tmp"),
            codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
        },
        spec: spec.clone(),
        resource,
    })
    .unwrap()
}

pub(super) fn waiting_receipt(
    request: RequestId,
    task: TaskId,
    resource: ResourceId,
    origin: MachineId,
    authority: MachineId,
) -> ResourceQueueReceipt {
    ResourceQueueReceipt {
        request,
        task,
        origin_machine: origin,
        authority_machine: authority,
        resource,
        outcome: ResourceQueueOutcome::Waiting,
    }
}

pub(super) fn serving_fixture(local_origin: bool, save_local_route: bool) -> ServingFixture {
    serving_fixture_with_spec(local_origin, save_local_route, spec())
}

pub(super) fn serving_fixture_with_spec(
    local_origin: bool,
    save_local_route: bool,
    spec: NormalizedSpec,
) -> ServingFixture {
    let directory = tempdir().unwrap();
    let database = directory.path().join("db");
    serving_fixture_at(
        directory,
        &database,
        MachineId::new(),
        local_origin,
        save_local_route,
        spec,
    )
}

/// Serving fixture whose store and authority are the ones a daemon home uses
pub(super) fn serving_fixture_at(
    directory: tempfile::TempDir,
    database: &Path,
    authority: MachineId,
    local_origin: bool,
    save_local_route: bool,
    spec: NormalizedSpec,
) -> ServingFixture {
    let mut store = Store::open(database).unwrap();
    let origin = if local_origin {
        authority
    } else {
        MachineId::new()
    };
    let background_task = TaskId::new();
    let request_id = RequestId::new();
    let task_id = TaskId::new();
    let mut resource = resource(authority);
    resource.supervisor.thread = spec.thread;
    resource.registered_background_task = Some(background_task);

    store.register_resource(authority, &resource).unwrap();
    store
        .insert_task(&remote_task(background_task, &spec))
        .unwrap();
    store
        .cas_status(
            background_task,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    if local_origin && save_local_route {
        store
            .insert_origin_route(&resource_origin_route(
                request_id,
                task_id,
                resource.id,
                origin,
                authority,
                &spec,
            ))
            .unwrap();
    }
    let request = store
        .accept_resource_request(
            authority,
            request_id,
            task_id,
            resource.id,
            origin,
            spec.clone(),
        )
        .unwrap();
    if local_origin && save_local_route {
        store
            .resolve_resource_route(&waiting_receipt(
                request_id,
                task_id,
                resource.id,
                origin,
                authority,
            ))
            .unwrap();
    }

    let OpenReleaseLoanResult::Opened { .. } = store
        .open_release_loan_for_authority(authority, resource.id, resource.state_revision)
        .unwrap()
    else {
        panic!("fixture must open one release action");
    };
    store
        .cas_exit(
            background_task,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
        )
        .unwrap()
        .unwrap();
    let (loan, state_revision) = store
        .seed_verified_serving_loan_for_test(
            authority,
            resource.id,
            request.request_id,
            ReturnContext::AlreadyCompleted {
                task_id: background_task,
                result_ref: "test serving fixture".into(),
            },
        )
        .unwrap();

    ServingFixture {
        directory,
        store,
        authority,
        origin,
        resource,
        request,
        loan,
        state_revision,
        spec,
    }
}

pub(super) fn acceptance_input(fixture: &ServingFixture) -> ResourceTaskAcceptanceInput {
    ResourceTaskAcceptanceInput {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        request_id: fixture.request.request_id,
        task_id: fixture.request.task_id,
        acceptance_sequence: fixture.request.acceptance_sequence,
        loan_id: fixture.loan.id,
        expected_state_revision: fixture.state_revision,
        command_spec: crate::resource::CommandSpec::try_from(fixture.spec.clone()).unwrap(),
        executor_env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
    }
}

pub(super) fn completion(
    outcome: AssignedResourceTaskReconcileOutcome,
) -> Result<ResourceTaskCompletionResult, AssignedResourceTaskReconcileOutcome> {
    match outcome {
        AssignedResourceTaskReconcileOutcome::Completed(result) => Ok(*result),
        other => Err(other),
    }
}

pub(super) fn task_reconcile_input(fixture: &ServingFixture) -> AssignedResourceTaskReconcileInput {
    AssignedResourceTaskReconcileInput {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        loan_id: fixture.loan.id,
        request_id: fixture.request.request_id,
        task_id: fixture.request.task_id,
        expected_state_revision: fixture.state_revision,
    }
}

pub(super) fn accept_and_finish_resource_task(
    fixture: &mut ServingFixture,
    outcome: ExitReason,
    evidence: ProcessGroupExitEvidence,
) -> AssignedResourceTaskReconcileInput {
    let acceptance = acceptance_input(fixture);
    assert_eq!(
        fixture
            .store
            .accept_assigned_resource_task(acceptance)
            .unwrap(),
        ResourceTaskAcceptance::Inserted {
            task: fixture.request.task_id,
        }
    );
    fixture
        .store
        .cas_status(
            fixture.request.task_id,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    fixture
        .store
        .cas_exit_with_evidence(
            fixture.request.task_id,
            ProcessStatus::Running,
            &outcome,
            evidence,
        )
        .unwrap()
        .unwrap();
    task_reconcile_input(fixture)
}

pub(super) fn refresh_serving_fixture(
    fixture: &mut ServingFixture,
    loan: Loan,
    request: ResourceRequest,
) {
    fixture.loan = loan;
    fixture.request = request;
    fixture.state_revision = fixture
        .store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == fixture.resource.id)
        .unwrap()
        .resource
        .state_revision;
}

pub(super) fn accept_and_finish_next_resource_task(
    fixture: &mut ServingFixture,
    outcome: ExitReason,
    evidence: ProcessGroupExitEvidence,
) -> AssignedResourceTaskReconcileInput {
    let input = ResourceTaskAcceptanceInput {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        request_id: fixture.request.request_id,
        task_id: fixture.request.task_id,
        acceptance_sequence: fixture.request.acceptance_sequence,
        loan_id: fixture.loan.id,
        expected_state_revision: fixture.state_revision,
        command_spec: fixture.request.spec().clone(),
        executor_env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
    };
    assert_eq!(
        fixture.store.accept_assigned_resource_task(input).unwrap(),
        ResourceTaskAcceptance::Inserted {
            task: fixture.request.task_id,
        }
    );
    fixture
        .store
        .cas_status(
            fixture.request.task_id,
            ProcessStatus::Queued,
            ProcessStatus::Running,
        )
        .unwrap()
        .unwrap();
    fixture
        .store
        .cas_exit_with_evidence(
            fixture.request.task_id,
            ProcessStatus::Running,
            &outcome,
            evidence,
        )
        .unwrap()
        .unwrap();
    task_reconcile_input(fixture)
}

pub(super) fn acceptance_counts(store: &Store, request: RequestId, task: TaskId) -> [i64; 7] {
    [
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM tasks WHERE id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .unwrap(),
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_identities WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .unwrap(),
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .unwrap(),
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_event_cursors WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .unwrap(),
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM resource_requests WHERE request_id=?1",
                [request.0.to_string()],
                |row| row.get(0),
            )
            .unwrap(),
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM origin_routes WHERE request_id=?1",
                [request.0.to_string()],
                |row| row.get(0),
            )
            .unwrap(),
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_event_receipts WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .unwrap(),
    ]
}

pub(super) const TEST_WATCHER_EXECUTABLE: &str = "/bin/echo";

pub(super) fn release_watcher_intent(
    resource_id: ResourceId,
    notice: &SupervisorNotice,
    background_task: TaskId,
    watcher_task: TaskId,
) -> ReleaseWatcherIntent {
    let watcher_task_id = ReleaseWatcherTaskId::new(watcher_task);
    let command = ReleaseWatcherCommand {
        resource_id,
        action_id: notice.action_id,
        state_revision: notice.state_revision,
        trainer_task_id: background_task,
        watcher_task_id,
    };
    ReleaseWatcherIntent {
        action_id: notice.action_id,
        state_revision: notice.state_revision,
        observed_background_task: background_task,
        watcher_task_id,
        request_id: RequestId::new(),
        normalized_spec_sha256: command
            .normalized_spec_sha256(
                Path::new(TEST_WATCHER_EXECUTABLE),
                notice.destination.thread,
            )
            .unwrap(),
    }
}

pub(super) fn watcher_spec(resource: &Resource, intent: &ReleaseWatcherIntent) -> NormalizedSpec {
    ReleaseWatcherCommand::from_intent(resource.id, intent)
        .normalized_spec(
            Path::new(TEST_WATCHER_EXECUTABLE),
            resource.supervisor.thread,
        )
        .unwrap()
}

pub(super) fn prepare_release_checkpoint_baseline(
    store: &mut Store,
    authority: MachineId,
    resource: &Resource,
    intent: &ReleaseWatcherIntent,
) -> ReleaseCheckpointBaseline {
    store
        .bind_release_watcher_for_authority(authority, resource.id, intent.clone())
        .unwrap();
    store
        .capture_release_checkpoint_baseline_for_authority(
            authority,
            resource.id,
            intent.action_id,
            intent.state_revision,
        )
        .unwrap()
}

pub(super) fn saved_release_association(
    store: &Store,
    resource_id: ResourceId,
    task_id: TaskId,
) -> TrainerAttemptAssociation {
    trainer_association_by_resource_and_task(&store.conn, resource_id, task_id)
        .unwrap()
        .unwrap()
}

pub(super) fn watcher_task_and_callback(
    intent: &ReleaseWatcherIntent,
    spec: &NormalizedSpec,
) -> (TaskRow, CallbackContext) {
    let row = remote_task(intent.watcher_task_id.as_task_id(), spec);
    let callback = CallbackContext {
        env: row.env.clone(),
        cwd: row.cwd.clone(),
        codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
    };
    (row, callback)
}

pub(super) fn watcher_acceptance_input(
    resource: &Resource,
    intent: ReleaseWatcherIntent,
    row: &TaskRow,
    spec: &NormalizedSpec,
    callback: &CallbackContext,
) -> ReleaseWatcherAcceptanceInput {
    ReleaseWatcherAcceptanceInput {
        authority_machine: resource.authority_machine(),
        resource_id: resource.id,
        supervisor: resource.supervisor,
        intent,
        row: row.clone(),
        spec: spec.clone(),
        callback: callback.clone(),
    }
}

pub(super) fn accept_watcher(
    store: &mut Store,
    resource: &Resource,
    intent: ReleaseWatcherIntent,
    row: &TaskRow,
    spec: &NormalizedSpec,
    callback: &CallbackContext,
) -> Result<ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError> {
    store.accept_release_watcher_for_authority(watcher_acceptance_input(
        resource, intent, row, spec, callback,
    ))
}

pub(super) fn checkpoint_decision_for_cancellation(
    store: &mut Store,
    authority: MachineId,
    background_task: TaskId,
) -> (
    Resource,
    SupervisorNotice,
    ReleaseWatcherIntent,
    ReleaseCheckpointStopDecision,
) {
    let (resource, _, notice) = open_release_for_test(store, authority, background_task);
    let association = saved_release_association(store, resource.id, background_task);
    let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
    prepare_release_checkpoint_baseline(store, authority, &resource, &intent);
    crate::resource::trainer_publication::tests::write_generation_for_test(
        association.verified_attempt().canonical_runtime_root(),
        association.verified_attempt().binding(),
        "generation-cancellation",
        81,
    );
    let ReleaseCheckpointStopOutcome::Reserved(decision) = store
        .reserve_release_checkpoint_stop_for_authority(
            authority,
            resource.id,
            notice.action_id,
            notice.state_revision,
        )
        .unwrap()
    else {
        panic!("the exact checkpoint must reserve a stop decision");
    };

    (resource, notice, intent, decision)
}

pub(super) fn start_release_watcher_for_test(
    store: &mut Store,
    resource: &Resource,
    intent: &ReleaseWatcherIntent,
) {
    let watcher_spec = watcher_spec(resource, intent);
    let (row, callback) = watcher_task_and_callback(intent, &watcher_spec);
    accept_watcher(
        store,
        resource,
        intent.clone(),
        &row,
        &watcher_spec,
        &callback,
    )
    .unwrap();
    store
        .cas_status(row.id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap()
        .unwrap();
    store.set_pid(row.id, std::process::id() as i32).unwrap();
    store
        .update_execution_state(row.id, ProcessStatus::Running)
        .unwrap();
}

pub(super) fn watcher_acceptance_counts(
    store: &Store,
    request: RequestId,
    task: TaskId,
) -> [i64; 4] {
    [
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM tasks WHERE id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .unwrap(),
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM origin_routes WHERE request_id=?1",
                [request.0.to_string()],
                |row| row.get(0),
            )
            .unwrap(),
        identity_count(store, task),
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM executor_outbox WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .unwrap(),
    ]
}

pub(super) fn prevention_count(store: &Store) -> i64 {
    store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_request_preventions",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

pub(super) fn identity_count(store: &Store, task: TaskId) -> i64 {
    store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM executor_identities WHERE task_id=?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .unwrap()
}

pub(super) fn assert_cancelled_tombstone(
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

pub(super) fn release_completion_fixture() -> (
    TrainerAssociationFixture,
    AttemptBinding,
    RequestId,
    ActionId,
    ResourceRevision,
    LoanId,
) {
    release_completion_fixture_with(TrainerAssociationFixture::new(), spec(), MachineId::new())
}

pub(super) fn release_completion_fixture_with(
    mut fixture: TrainerAssociationFixture,
    request_spec: NormalizedSpec,
    request_origin: MachineId,
) -> (
    TrainerAssociationFixture,
    AttemptBinding,
    RequestId,
    ActionId,
    ResourceRevision,
    LoanId,
) {
    fixture.insert_accepted_running_task();
    let binding = fixture.register_release_attempt();
    let request_id = RequestId::new();
    fixture
        .store
        .accept_resource_request(
            fixture.authority,
            request_id,
            TaskId::new(),
            fixture.resource.id,
            request_origin,
            request_spec,
        )
        .unwrap();
    let OpenReleaseLoanResult::Opened { loan, notice } = fixture
        .store
        .open_release_loan_for_authority(
            fixture.authority,
            fixture.resource.id,
            fixture.resource.state_revision,
        )
        .unwrap()
    else {
        panic!("fixture must open one release action");
    };

    (
        fixture,
        binding,
        request_id,
        notice.action_id,
        notice.state_revision,
        loan.id,
    )
}

pub(super) fn publish_completed_result(
    fixture: &mut TrainerAssociationFixture,
    binding: &AttemptBinding,
    exit_evidence: ProcessGroupExitEvidence,
) {
    crate::resource::trainer_publication::tests::write_completed_result_for_test(
        &fixture.runtime_root,
        binding,
    );
    fixture.finish_registered_task_with_evidence(exit_evidence);
}

pub(super) fn commit_stopped_release_decision(
    fixture: &mut TrainerAssociationFixture,
    action_id: ActionId,
    revision: ResourceRevision,
) -> (ReleaseCheckpointStopDecision, ReleaseCheckpointCancellation) {
    let notice_json: String = fixture
        .store
        .conn
        .query_row(
            "SELECT notice_json FROM resource_supervisor_notices WHERE action_id = ?1",
            [action_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let notice: SupervisorNotice = serde_json::from_str(&notice_json).unwrap();
    let intent =
        release_watcher_intent(fixture.resource.id, &notice, fixture.task_id, TaskId::new());
    prepare_release_checkpoint_baseline(
        &mut fixture.store,
        fixture.authority,
        &fixture.resource,
        &intent,
    );
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &fixture.runtime_root,
        &trainer_attempt_test_support::attempt_binding("attempt-1"),
        "generation-stopped",
        81,
    );
    let ReleaseCheckpointStopOutcome::Reserved(decision) = fixture
        .store
        .reserve_release_checkpoint_stop_for_authority(
            fixture.authority,
            fixture.resource.id,
            action_id,
            revision,
        )
        .unwrap()
    else {
        panic!("the saved checkpoint must reserve a stopped release decision");
    };
    start_release_watcher_for_test(&mut fixture.store, &fixture.resource, &intent);
    let ReleaseCheckpointCancellationOutcome::Committed(cancellation) = fixture
        .store
        .commit_release_checkpoint_cancellation_for_authority(
            fixture.authority,
            fixture.resource.id,
            action_id,
            revision,
            &decision,
        )
        .unwrap()
    else {
        panic!("the running watcher must permit the saved stop decision");
    };

    (decision, cancellation.cancellation)
}

pub(super) fn fake_resource_task_spec(root: &Path, marker: &Path) -> NormalizedSpec {
    native_resource_command_spec(root, "fake resource command", &[marker])
}

// the command stays in its foreground process group until the test creates the
// gate, so a test can inspect the Serving loan while the task is running
pub(super) fn gated_resource_task_spec(root: &Path, marker: &Path, gate: &Path) -> NormalizedSpec {
    native_resource_command_spec(root, "gated resource command", &[marker, gate])
}

pub(super) fn native_resource_command_spec(
    root: &Path,
    name: &str,
    args: &[&Path],
) -> NormalizedSpec {
    let mut command = vec![crate::resource::foreground::test_support::native_fake_command()];
    command.extend_from_slice(args);

    serde_json::from_value(json!({
        "api_version": 1,
        "thread": Uuid::now_v7(),
        "name": name,
        "cwd": root,
        "timeout": "4h",
        "workload": { "type": "task", "command": command }
    }))
    .unwrap()
}

pub(super) async fn wait_for_running_task(
    store: &ractor::ActorRef<StoreMsg>,
    task_id: TaskId,
) -> TaskRow {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Some(row) = call(store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                && row.status() == ProcessStatus::Running
            {
                return row;
            }

            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("resource task must reach the running state")
}

/// Wait for the task-terminal event path, without a client wake, to reserve the return
pub(super) async fn wait_for_awaiting_return(
    supervisor: &ractor::ActorRef<SupervisorMsg>,
    resource_id: ResourceId,
) -> Loan {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let inspection = call(supervisor, |reply| SupervisorMsg::InspectResource {
                id: resource_id,
                reply,
            })
            .await
            .unwrap()
            .unwrap();
            if let Some(loan) = inspection.loan
                && matches!(
                    loan.state,
                    LoanState::Active {
                        phase: LoanPhase::AwaitingReturn { .. }
                    }
                )
            {
                return loan;
            }

            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("empty queue must reserve the return")
}

pub(super) fn return_notice_count(home: &Home, loan_id: LoanId) -> i64 {
    Store::open(&home.db_path())
        .unwrap()
        .conn
        .query_row(
            "SELECT COUNT(*) FROM resource_supervisor_notices
             WHERE loan_id = ?1
               AND json_extract(notice_json, '$.payload.type') = 'return_required'",
            [loan_id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap()
}

pub(super) async fn wait_for_terminal_task(
    store: &ractor::ActorRef<StoreMsg>,
    task_id: TaskId,
) -> TaskRow {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Some(row) = call(store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                && row.status().is_terminal()
            {
                return row;
            }

            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resource task must reach a terminal state")
}

pub(super) async fn stop_test_supervisor(
    supervisor: ractor::ActorRef<SupervisorMsg>,
    handle: ractor::concurrency::JoinHandle<()>,
) {
    supervisor.stop(None);
    let _ = handle.await;
}

pub(super) async fn stop_test_resource_actor(
    actor: ractor::ActorRef<ResourceMsg>,
    actor_handle: ractor::concurrency::JoinHandle<()>,
    store: ractor::ActorRef<StoreMsg>,
    store_handle: ractor::concurrency::JoinHandle<()>,
) {
    actor.stop(None);
    let _ = actor_handle.await;
    store.stop(None);
    let _ = store_handle.await;
}

/// Assert that a release served the queue only as a non-resumable ended run
pub(super) fn assert_ended_release(
    result: &ReleaseCompletionResult,
    task: TaskId,
    action: ActionId,
    outcome: &ExitReason,
) {
    let ReleaseCompletionResult::Assigned { loan, .. } = result else {
        panic!("the queued request must be assigned after an ended release: {result:?}");
    };
    assert!(
        matches!(
            &loan.state,
            LoanState::Active {
                phase: LoanPhase::Serving {
                    return_context: ReturnContext::EndedWithoutResult {
                        task_id,
                        outcome: saved_outcome,
                    },
                    release_provenance: ServingReleaseProvenance::EndedTrainerLockReleased {
                        action_id,
                        task_id: proven_task,
                        outcome: proven_outcome,
                        ..
                    },
                    ..
                }
            } if *task_id == task
                && saved_outcome == outcome
                && *action_id == action
                && *proven_task == task
                && proven_outcome == outcome
        ),
        "unexpected ended release: {loan:?}"
    );
}
