//! Bound release-watcher poll, launch, restart, and end-to-end release tests

use crate::domain::TaskRow;
use crate::resource::SupervisorActionAuthority;
use crate::resource::bound_action::{
    ActionTaskAcceptance, ActionTaskIdentity, ResourceActionOperation, ResourceActionOutcome,
    ResourceActionRejection,
};
use crate::store::{AcceptedActionTask, RemoteReleaseWatcherAcceptanceInput, ResourceActionError};
use std::time::Duration;

use tokio::net::UnixListener;

use super::fixtures::{
    TrainerAssociationFixture, accept_watcher, fake_resource_task_spec, gated_resource_task_spec,
    machine_other_than, open_release_for_test, prepare_release_checkpoint_baseline,
    release_completion_fixture_with, release_watcher_intent, remote_task, resource,
    return_notice_count, saved_release_association, spec, start_release_watcher_for_test,
    stop_test_supervisor, test_release_association, wait_for_awaiting_return,
    watcher_acceptance_counts, watcher_spec, watcher_task_and_callback,
};
use crate::daemon::AppState;
use crate::daemon::actors::resource::{ReleaseWatcherAttentionReason, ReleaseWatcherStatus};
use crate::daemon::actors::supervisor::SUPERVISOR_TEST_LOCK;
use crate::daemon::actors::{StoreMsg, SupervisorActor, SupervisorArgs, SupervisorMsg, call};
use crate::domain::{ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId};
use crate::files::StreamSlots;
use crate::fleet::FleetState;
use crate::fleet::directory::LocalMachine;
use crate::fleet::protocol::SUPPORTED_PROTOCOLS;
use crate::home::{Home, LockMode, flock_exclusive};
use crate::machine::{LocalIdentity, MachineId, MachineName, load_or_create_machine_id};
use crate::resource::release_watcher::{
    RELEASE_WATCHER_POLL_PATH, ReleaseWatcherCommand, ReleaseWatcherPollAttention,
    ReleaseWatcherPollOutcome, ReleaseWatcherPollRequest,
};
use crate::resource::store::{
    ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError, ReleaseWatcherAcceptanceInput,
};
use crate::resource::trainer_publication::AttemptBinding;
use crate::resource::{
    ActionId, LoanPhase, LoanState, ReleaseCheckpointStopOutcome, ReleaseWatcherIntent,
    ReleaseWatcherTaskId, Resource, ResourceId, ResourceRevision, ServingReleaseProvenance,
    SupervisorAddress, SupervisorNotice,
};
use crate::store::{ExecutorIdentity, NewTask, Store, new_queued_task};
use crate::submission::{CallbackContext, CallbackExecutable, RequestId, normalized_spec_sha256};
use ractor::Actor;
use rusqlite::params;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

const WATCHER_TASK_NAME: &str = "resource release watcher";

struct PollFixture {
    _directory: tempfile::TempDir,
    store: Store,
    authority: MachineId,
    resource: Resource,
    notice: SupervisorNotice,
    background_task: TaskId,
    intent: ReleaseWatcherIntent,
    runtime_root: PathBuf,
    binding: AttemptBinding,
}

impl PollFixture {
    /// Open one release action with a saved baseline and no accepted watcher
    fn new() -> Self {
        Self::with_pre_baseline_generation(false)
    }

    fn with_pre_baseline_generation(old_generation: bool) -> Self {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let background_task = TaskId::new();
        let (resource, _, notice) = open_release_for_test(&mut store, authority, background_task);
        let association = saved_release_association(&store, resource.id, background_task);
        let runtime_root = association
            .verified_attempt()
            .canonical_runtime_root()
            .to_path_buf();
        let binding = association.verified_attempt().binding().clone();
        if old_generation {
            crate::resource::trainer_publication::tests::write_generation_for_test(
                &runtime_root,
                &binding,
                "generation-before-baseline",
                10,
            );
        }
        let intent = release_watcher_intent(resource.id, &notice, background_task, TaskId::new());
        prepare_release_checkpoint_baseline(&mut store, authority, &resource, &intent);

        Self {
            _directory: directory,
            store,
            authority,
            resource,
            notice,
            background_task,
            intent,
            runtime_root,
            binding,
        }
    }

    fn command(&self) -> ReleaseWatcherCommand {
        ReleaseWatcherCommand::from_intent(self.resource.id, &self.intent)
    }

    fn poll(&mut self, command: ReleaseWatcherCommand) -> ReleaseWatcherPollOutcome {
        self.store
            .poll_release_watcher_for_authority(
                self.authority,
                ReleaseWatcherPollRequest::new(command),
            )
            .unwrap()
    }

    fn start_watcher(&mut self) {
        start_release_watcher_for_test(&mut self.store, &self.resource, &self.intent);
    }

    fn accept_watcher_without_start(&mut self) {
        let spec = watcher_spec(&self.resource, &self.intent);
        let (row, callback) = watcher_task_and_callback(&self.intent, &spec);
        accept_watcher(
            &mut self.store,
            &self.resource,
            self.intent.clone(),
            &row,
            &spec,
            &callback,
        )
        .unwrap();
    }

    /// Replace the supervisor machine through the operator control
    fn replace_supervisor(&mut self, machine: MachineId) {
        let resource = self
            .store
            .resource_snapshots_for_authority(self.authority)
            .unwrap()
            .remove(0)
            .resource;
        let replaced = self
            .store
            .replace_resource_supervisor(
                self.authority,
                resource.id,
                resource.state_revision,
                SupervisorAddress {
                    machine,
                    thread: resource.supervisor.thread,
                },
            )
            .unwrap();
        assert!(replaced.resource.assignment_revision.get() > resource.assignment_revision.get());
    }

    fn write_generation(&self, generation_id: &str, update_count: u64) {
        crate::resource::trainer_publication::tests::write_generation_for_test(
            &self.runtime_root,
            &self.binding,
            generation_id,
            update_count,
        );
    }

    fn trainer_cancel_marker(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.store
            .get_task(self.background_task)
            .unwrap()
            .unwrap()
            .cancel_requested_at
    }

    fn checkpoint_state_json(&self) -> String {
        self.store
            .conn
            .query_row(
                "SELECT state_json FROM resource_release_checkpoint_states WHERE action_id=?1",
                [self.notice.action_id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap()
    }
}

fn attention(reason: ReleaseWatcherPollAttention) -> ReleaseWatcherPollOutcome {
    ReleaseWatcherPollOutcome::Attention { reason }
}

#[test]
fn poll_waits_for_running_watcher_and_ignores_the_baseline_checkpoint() {
    let mut fixture = PollFixture::with_pre_baseline_generation(true);
    let command = fixture.command();

    assert_eq!(
        fixture.poll(command),
        ReleaseWatcherPollOutcome::WatcherNotRunning
    );
    fixture.accept_watcher_without_start();
    assert_eq!(
        fixture.poll(command),
        ReleaseWatcherPollOutcome::WatcherNotRunning
    );
    fixture.start_watcher();
    for _ in 0..2 {
        assert_eq!(
            fixture.poll(command),
            ReleaseWatcherPollOutcome::WaitingForCheckpoint
        );
    }
    assert!(fixture.trainer_cancel_marker().is_none());
}

#[test]
fn new_checkpoint_commits_one_marker_and_exact_retries_reuse_the_saved_decision() {
    let mut fixture = PollFixture::new();
    fixture.start_watcher();
    let command = fixture.command();
    fixture.write_generation("generation-after-baseline", 20);

    let ReleaseWatcherPollOutcome::StopCommitted {
        generation_id,
        cancel_requested_at,
    } = fixture.poll(command)
    else {
        panic!("a new complete checkpoint must commit the exact trainer stop");
    };
    assert_eq!(generation_id, "generation-after-baseline");
    assert_eq!(fixture.trainer_cancel_marker(), Some(cancel_requested_at));
    let committed = fixture.checkpoint_state_json();

    // a later publication cannot replace the saved checkpoint on retry
    fixture.write_generation("generation-later", 30);
    for _ in 0..2 {
        assert_eq!(
            fixture.poll(command),
            ReleaseWatcherPollOutcome::StopCommitted {
                generation_id: "generation-after-baseline".into(),
                cancel_requested_at,
            }
        );
    }
    assert_eq!(fixture.checkpoint_state_json(), committed);
    assert_eq!(fixture.trainer_cancel_marker(), Some(cancel_requested_at));
}

#[test]
fn final_result_before_stop_waits_for_trainer_exit_without_cancelling() {
    let mut fixture = PollFixture::new();
    fixture.start_watcher();
    let command = fixture.command();
    crate::resource::trainer_publication::tests::write_request_for_test(
        &fixture.runtime_root,
        &fixture.binding,
    );
    crate::resource::trainer_publication::tests::write_completed_result_for_test(
        &fixture.runtime_root,
        &fixture.binding,
    );
    fixture.write_generation("generation-with-result", 40);

    assert_eq!(
        fixture.poll(command),
        ReleaseWatcherPollOutcome::CompletedResultAwaitingTrainerExit
    );
    assert!(fixture.trainer_cancel_marker().is_none());

    fixture
        .store
        .cas_exit_with_evidence(
            fixture.background_task,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
            ProcessGroupExitEvidence::ConfirmedExited,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture.poll(command),
        ReleaseWatcherPollOutcome::TrainerCompleted
    );
    assert!(fixture.trainer_cancel_marker().is_none());
}

#[test]
fn wrong_watcher_action_trainer_revision_and_authority_are_typed_without_a_marker() {
    let mut fixture = PollFixture::new();
    fixture.start_watcher();
    fixture.write_generation("generation-after-baseline", 20);
    let command = fixture.command();

    let cases = [
        (
            ReleaseWatcherCommand {
                watcher_task_id: ReleaseWatcherTaskId::new(TaskId::new()),
                ..command
            },
            ReleaseWatcherPollAttention::WrongWatcher,
        ),
        (
            ReleaseWatcherCommand {
                watcher_task_id: ReleaseWatcherTaskId::new(fixture.background_task),
                ..command
            },
            ReleaseWatcherPollAttention::WrongWatcher,
        ),
        (
            ReleaseWatcherCommand {
                action_id: ActionId::new(),
                ..command
            },
            ReleaseWatcherPollAttention::ActionNotCurrent,
        ),
        (
            ReleaseWatcherCommand {
                trainer_task_id: TaskId::new(),
                ..command
            },
            ReleaseWatcherPollAttention::ActionNotCurrent,
        ),
        (
            ReleaseWatcherCommand {
                state_revision: ResourceRevision::new(command.state_revision.get() + 1),
                ..command
            },
            ReleaseWatcherPollAttention::ActionNotCurrent,
        ),
        (
            ReleaseWatcherCommand {
                resource_id: ResourceId::new(),
                ..command
            },
            ReleaseWatcherPollAttention::ActionNotCurrent,
        ),
    ];
    for (changed, reason) in cases {
        assert_eq!(fixture.poll(changed), attention(reason), "{changed:?}");
    }
    assert_eq!(
        fixture
            .store
            .poll_release_watcher_for_authority(
                MachineId::new(),
                ReleaseWatcherPollRequest::new(command),
            )
            .unwrap(),
        attention(ReleaseWatcherPollAttention::WrongAuthority)
    );
    assert!(fixture.trainer_cancel_marker().is_none());
    assert!(matches!(
        fixture.poll(command),
        ReleaseWatcherPollOutcome::StopCommitted { .. }
    ));
}

#[test]
fn changed_selected_checkpoint_and_lost_trainer_keep_the_loan_without_a_marker() {
    let mut changed = PollFixture::new();
    changed.write_generation("generation-selected", 20);
    assert!(matches!(
        changed
            .store
            .reserve_release_checkpoint_stop_for_authority(
                changed.authority,
                changed.resource.id,
                changed.notice.action_id,
                changed.notice.state_revision,
            )
            .unwrap(),
        ReleaseCheckpointStopOutcome::Reserved(_)
    ));
    fs::remove_dir_all(changed.runtime_root.join("published/generation-selected")).unwrap();
    changed.write_generation("generation-replacement", 21);
    changed.start_watcher();
    let command = changed.command();
    assert_eq!(
        changed.poll(command),
        attention(ReleaseWatcherPollAttention::PublicationChanged)
    );
    assert!(changed.trainer_cancel_marker().is_none());

    let mut lost = PollFixture::new();
    lost.start_watcher();
    lost.store
        .cas_status(
            lost.background_task,
            ProcessStatus::Running,
            ProcessStatus::Lost,
        )
        .unwrap()
        .unwrap();
    let command = lost.command();
    assert_eq!(
        lost.poll(command),
        attention(ReleaseWatcherPollAttention::TrainerLost)
    );
    let snapshot = lost
        .store
        .resource_snapshots_for_authority(lost.authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == lost.resource.id)
        .unwrap();
    assert!(matches!(
        snapshot.loan.unwrap().state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease { .. }
        }
    ));
}

#[test]
fn local_watcher_keeps_its_stop_after_the_supervisor_moves_to_another_machine() {
    let mut fixture = PollFixture::new();
    fixture.start_watcher();
    let remote = machine_other_than(fixture.authority);
    fixture.replace_supervisor(remote);
    fixture.write_generation("generation-after-baseline", 20);
    let command = fixture.command();

    // the saved local routes, not the new remote assignment, own this watcher
    let ReleaseWatcherPollOutcome::StopCommitted {
        cancel_requested_at,
        ..
    } = fixture.poll(command)
    else {
        panic!("the accepted local watcher must keep its stop authority");
    };
    assert_eq!(fixture.trainer_cancel_marker(), Some(cancel_requested_at));
}

#[test]
fn local_watcher_with_a_changed_route_is_refused_without_a_marker() {
    let mut fixture = PollFixture::new();
    fixture.start_watcher();
    fixture.write_generation("generation-after-baseline", 20);
    fixture
        .store
        .conn
        .execute(
            "UPDATE origin_routes SET route_json = json_set(route_json, '$.thread', ?1)
             WHERE request_id = ?2",
            params![
                uuid::Uuid::now_v7().to_string(),
                fixture.intent.request_id.0.to_string(),
            ],
        )
        .unwrap();
    let command = fixture.command();

    assert_eq!(
        fixture.poll(command),
        attention(ReleaseWatcherPollAttention::WatcherIdentityConflict)
    );
    assert!(fixture.trainer_cancel_marker().is_none());
}

#[test]
fn watcher_whose_initial_event_changed_is_not_running_and_leaves_no_marker() {
    let changed_outbox: fn(&Store, TaskId) = |store, watcher| {
        store
            .conn
            .execute(
                "UPDATE executor_outbox SET notification_required = 1
                 WHERE task_id = ?1 AND seq = 1",
                [watcher.to_string()],
            )
            .unwrap();
    };
    let receipt_with_wrong_digest: fn(&Store, TaskId) = |store, watcher| {
        store
            .conn
            .execute(
                "DELETE FROM executor_outbox WHERE task_id = ?1 AND seq = 1",
                [watcher.to_string()],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO executor_event_receipts
                 (task_id, seq, event_digest, result_json, terminal_callback)
                 VALUES (?1, 1, 'not-the-queued-event', '\"acknowledged\"', 0)",
                [watcher.to_string()],
            )
            .unwrap();
    };
    for tamper in [changed_outbox, receipt_with_wrong_digest] {
        let mut fixture = PollFixture::new();
        fixture.start_watcher();
        fixture.write_generation("generation-after-baseline", 20);
        tamper(&fixture.store, fixture.intent.watcher_task_id.as_task_id());
        let command = fixture.command();

        assert_eq!(
            fixture.poll(command),
            ReleaseWatcherPollOutcome::WatcherNotRunning
        );
        assert!(fixture.trainer_cancel_marker().is_none());
    }
}

#[test]
fn corrupt_checkpoint_state_is_attention_instead_of_a_retryable_error() {
    let mut fixture = PollFixture::new();
    fixture.start_watcher();
    fixture
        .store
        .conn
        .execute(
            "UPDATE resource_release_checkpoint_states
             SET state_json = json_set(state_json, '$.action.state_revision', 'corrupt')
             WHERE action_id = ?1",
            [fixture.notice.action_id.as_uuid().to_string()],
        )
        .unwrap();
    let command = fixture.command();

    assert_eq!(
        fixture.poll(command),
        attention(ReleaseWatcherPollAttention::CorruptRecord)
    );
    assert!(fixture.trainer_cancel_marker().is_none());
}

#[test]
fn corrupt_resource_or_loan_row_is_attention_instead_of_a_retryable_error() {
    let corrupt_loan_state =
        "UPDATE loans SET state_json = json_set(state_json, '$.phase', 'corrupt')
         WHERE resource_id = ?1";
    let malformed_thread = "UPDATE resources SET supervisor_thread = 'not-a-uuid' WHERE id = ?1";
    for tamper in [corrupt_loan_state, malformed_thread] {
        let mut fixture = PollFixture::new();
        fixture.start_watcher();
        fixture
            .store
            .conn
            .execute(tamper, [fixture.resource.id.as_uuid().to_string()])
            .unwrap();
        let command = fixture.command();

        assert_eq!(
            fixture.poll(command),
            attention(ReleaseWatcherPollAttention::CorruptRecord),
            "{tamper}"
        );
        assert!(fixture.trainer_cancel_marker().is_none());
    }
}

#[test]
fn caller_command_that_differs_from_the_canonical_watcher_is_rejected() {
    let mut fixture = PollFixture::new();
    let spec = watcher_spec(&fixture.resource, &fixture.intent);
    let (mut row, callback) = watcher_task_and_callback(&fixture.intent, &spec);
    row.binary = PathBuf::from("/bin/sh");

    assert!(matches!(
        accept_watcher(
            &mut fixture.store,
            &fixture.resource,
            fixture.intent.clone(),
            &row,
            &spec,
            &callback,
        ),
        Err(ReleaseWatcherAcceptanceError::Conflict)
    ));
    assert_eq!(
        watcher_acceptance_counts(&fixture.store, fixture.intent.request_id, row.id),
        [0, 0, 0, 0]
    );
}

fn watcher_executable() -> PathBuf {
    assert_cmd::cargo::cargo_bin("homebased")
}

/// Bind and baseline the release watcher as the authority would before any acceptance
fn bind_saved_watcher(
    fixture: &mut TrainerAssociationFixture,
    action_id: ActionId,
    revision: ResourceRevision,
) -> ReleaseWatcherIntent {
    let command = ReleaseWatcherCommand {
        resource_id: fixture.resource.id,
        action_id,
        state_revision: revision,
        trainer_task_id: fixture.task_id,
        watcher_task_id: ReleaseWatcherTaskId::new(TaskId::new()),
    };
    let intent = ReleaseWatcherIntent {
        action_id,
        state_revision: revision,
        observed_background_task: fixture.task_id,
        watcher_task_id: command.watcher_task_id,
        request_id: RequestId::new(),
        normalized_spec_sha256: command
            .normalized_spec_sha256(&watcher_executable(), fixture.resource.supervisor.thread)
            .unwrap(),
    };
    fixture
        .store
        .bind_release_watcher_for_authority(fixture.authority, fixture.resource.id, intent.clone())
        .unwrap();
    fixture
        .store
        .capture_release_checkpoint_baseline_for_authority(
            fixture.authority,
            fixture.resource.id,
            action_id,
            revision,
        )
        .unwrap();

    intent
}

fn watcher_task_count(database: &Path) -> i64 {
    Store::open(database)
        .unwrap()
        .conn
        .query_row(
            "SELECT COUNT(*) FROM tasks WHERE name = ?1",
            [WATCHER_TASK_NAME],
            |row| row.get(0),
        )
        .unwrap()
}

async fn wait_for_task(
    store: &ractor::ActorRef<StoreMsg>,
    task_id: TaskId,
    timeout: Duration,
    done: impl Fn(&TaskRow) -> bool,
) -> TaskRow {
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(row) = call(store, |reply| StoreMsg::GetTask { id: task_id, reply })
                .await
                .unwrap()
                && done(&row)
            {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("task {task_id} did not reach the expected state"))
}

async fn wait_for_watcher_status(
    supervisor: &ractor::ActorRef<SupervisorMsg>,
    resource_id: ResourceId,
    done: impl Fn(&ReleaseWatcherStatus) -> bool,
) -> ReleaseWatcherStatus {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let inspection = call(supervisor, |reply| SupervisorMsg::InspectResource {
                id: resource_id,
                reply,
            })
            .await
            .unwrap()
            .unwrap();
            if let Some(status) = inspection.release_watcher
                && done(&status)
            {
                return status;
            }
            call(supervisor, |reply| SupervisorMsg::ReconcileResource {
                id: resource_id,
                reply,
            })
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("release watcher status did not settle")
}

async fn cancel_watcher(supervisor: &ractor::ActorRef<SupervisorMsg>, task_id: TaskId) {
    let _ = call(supervisor, |reply| SupervisorMsg::Cancel {
        id: task_id,
        reply,
    })
    .await;
}

#[tokio::test]
async fn bound_watcher_stops_the_trainer_at_a_new_checkpoint_and_one_optimization_runs() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(watcher_executable());
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let mut fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
    // the fake trainer runs until its task-run cancels the child group
    fs::write(&fixture.python, b"#!/bin/sh\nexec /bin/sleep 600\n").unwrap();
    let marker = fixture.home.join("activation-count");
    let gate = fixture.home.join("release-commands");
    let request_spec = gated_resource_task_spec(&fixture.home, &marker, &gate);
    let resource_id = fixture.resource.id;
    let trainer_task_id = fixture.task_id;

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    home.prepare_task(trainer_task_id).unwrap();
    let trainer_row = fixture.trainer_task(trainer_task_id, &fixture.spec);
    call(&supervisor, |reply| SupervisorMsg::Launch {
        row: Box::new(trainer_row),
        spec: Box::new(fixture.spec.clone()),
        reply,
    })
    .await
    .unwrap();
    wait_for_task(&store, trainer_task_id, Duration::from_secs(10), |row| {
        row.status() == ProcessStatus::Running && row.pid().is_some()
    })
    .await;
    let binding = fixture.register_release_attempt();
    let request = fixture
        .store
        .accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            resource_id,
            machine_other_than(authority),
            request_spec,
        )
        .unwrap();

    call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource_id,
        reply,
    })
    .await
    .unwrap();
    let status = wait_for_watcher_status(&supervisor, resource_id, |status| {
        matches!(status, ReleaseWatcherStatus::Running { .. })
    })
    .await;
    let ReleaseWatcherStatus::Running {
        action_id,
        watcher_task_id,
    } = status
    else {
        unreachable!();
    };

    // the watcher starts while the daemon socket is absent and must retry
    tokio::time::sleep(Duration::from_millis(300)).await;
    let machine = LocalMachine {
        identity: LocalIdentity::start(&home).unwrap(),
        name: MachineName::fallback(),
        protocol: SUPPORTED_PROTOCOLS,
    };
    assert_eq!(machine.identity.machine, authority);
    let state = AppState {
        home: home.clone(),
        store: store.clone(),
        supervisor: supervisor.clone(),
        web: None,
        content: None,
        stream_slots: StreamSlots::new(),
        machine,
        fleet: FleetState::Disabled,
        message_receiver: crate::daemon::message_receiver::MessageReceiver::default(),
        locks: crate::daemon::DaemonLocks::default(),
        thread_titles: None,
    };
    let listener = UnixListener::bind(home.sock_path()).unwrap();
    let router = crate::daemon::api::socket_router(state.clone());
    let socket_server = tokio::spawn(async move { axum::serve(listener, router).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        call(&store, |reply| StoreMsg::GetTask {
            id: trainer_task_id,
            reply,
        })
        .await
        .unwrap()
        .unwrap()
        .cancel_requested_at
        .is_none()
    );
    crate::resource::trainer_publication::tests::write_generation_for_test(
        &fixture.runtime_root,
        &binding,
        "generation-after-baseline",
        90,
    );

    let trainer = wait_for_task(&store, trainer_task_id, Duration::from_secs(40), |row| {
        row.status().is_terminal()
    })
    .await;
    assert_eq!(trainer.status(), ProcessStatus::Cancelled);
    assert_eq!(
        trainer.process_group_exit_evidence(),
        ProcessGroupExitEvidence::ConfirmedExited
    );
    wait_for_task(&store, request.task_id, Duration::from_secs(30), |row| {
        row.status() == ProcessStatus::Running
    })
    .await;
    let inspection = call(&supervisor, |reply| SupervisorMsg::InspectResource {
        id: resource_id,
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        inspection.loan.as_ref().map(|loan| &loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::Serving {
                release_provenance: ServingReleaseProvenance::StoppedTrainerCheckpoint {
                    action_id: saved_action,
                    task_id: saved_task,
                    generation_id,
                    ..
                },
                ..
            }
        }) if *saved_action == action_id
            && *saved_task == trainer_task_id
            && generation_id == "generation-after-baseline"
    ));

    fs::write(&gate, b"").unwrap();
    let optimization = wait_for_task(&store, request.task_id, Duration::from_secs(30), |row| {
        row.status().is_terminal()
    })
    .await;
    assert_eq!(optimization.status(), ProcessStatus::Succeeded);
    let watcher = wait_for_task(&store, watcher_task_id, Duration::from_secs(30), |row| {
        row.status().is_terminal()
    })
    .await;
    assert_eq!(watcher.status(), ProcessStatus::Succeeded);

    for _ in 0..3 {
        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource_id,
            reply,
        })
        .await
        .unwrap();
    }
    assert_eq!(fs::read(&marker).unwrap(), b"x");
    assert_eq!(watcher_task_count(&fixture.database), 1);
    let loan = wait_for_awaiting_return(&supervisor, resource_id).await;
    assert_eq!(return_notice_count(&home, loan.id), 1);

    assert_route_is_socket_only(state).await;
    socket_server.abort();
    stop_test_supervisor(supervisor, handle).await;
}

async fn assert_route_is_socket_only(state: AppState) {
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::TokioIo;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind = listener.local_addr().unwrap();
    let router = crate::daemon::web::router(state, bind);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let stream = tokio::net::TcpStream::connect(bind).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(connection);
    let request = hyper::Request::builder()
        .method("POST")
        .uri(RELEASE_WATCHER_POLL_PATH)
        .header("host", bind.to_string())
        .header("content-type", "application/json")
        .body(Full::new(bytes::Bytes::from_static(b"{}")))
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    let status = response.status();
    let _ = response.into_body().collect().await;
    server.abort();

    assert!(
        status == hyper::StatusCode::NOT_FOUND || status == hyper::StatusCode::METHOD_NOT_ALLOWED,
        "dashboard listener served the watcher route with {status}"
    );
}

#[tokio::test]
async fn restart_before_acceptance_launches_the_saved_watcher_once_and_later_only_observes() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(watcher_executable());
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
    let marker = fixture.home.join("activation-count");
    let request_spec = fake_resource_task_spec(&fixture.home, &marker);
    let (mut fixture, _, _, action_id, revision, _) =
        release_completion_fixture_with(fixture, request_spec, machine_other_than(authority));
    // the trainer row is live, so hold its runner lock like a live worker
    home.prepare_task(fixture.task_id).unwrap();
    let trainer_lock = flock_exclusive(
        &home.task_paths(fixture.task_id).runner_lock,
        LockMode::NonBlocking,
    )
    .unwrap();
    let intent = bind_saved_watcher(&mut fixture, action_id, revision);
    let watcher_task_id = intent.watcher_task_id.as_task_id();
    let resource_id = fixture.resource.id;
    let database = fixture.database.clone();
    assert_eq!(watcher_task_count(&database), 0);

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let status = wait_for_watcher_status(&supervisor, resource_id, |status| {
        matches!(status, ReleaseWatcherStatus::Running { .. })
    })
    .await;
    assert_eq!(
        status,
        ReleaseWatcherStatus::Running {
            action_id,
            watcher_task_id,
        }
    );
    let running = wait_for_task(&store, watcher_task_id, Duration::from_secs(10), |row| {
        row.status() == ProcessStatus::Running && row.pid().is_some()
    })
    .await;
    assert_eq!(watcher_task_count(&database), 1);
    stop_test_supervisor(supervisor, handle).await;

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let status = wait_for_watcher_status(&supervisor, resource_id, |status| {
        matches!(status, ReleaseWatcherStatus::Running { .. })
    })
    .await;
    assert_eq!(
        status,
        ReleaseWatcherStatus::Running {
            action_id,
            watcher_task_id,
        }
    );
    let observed = call(&store, |reply| StoreMsg::GetTask {
        id: watcher_task_id,
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(observed.pid(), running.pid());
    assert_eq!(watcher_task_count(&database), 1);
    assert!(
        call(&store, |reply| StoreMsg::GetTask {
            id: fixture.task_id,
            reply,
        })
        .await
        .unwrap()
        .unwrap()
        .cancel_requested_at
        .is_none()
    );

    cancel_watcher(&supervisor, watcher_task_id).await;
    wait_for_task(&store, watcher_task_id, Duration::from_secs(20), |row| {
        row.status().is_terminal()
    })
    .await;
    stop_test_supervisor(supervisor, handle).await;
    drop(trainer_lock);
}

#[tokio::test]
async fn accepted_queued_watcher_after_restart_is_uncertain_and_never_spawned() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(watcher_executable());
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let fixture = TrainerAssociationFixture::new_for_home(directory, &home, authority);
    let marker = fixture.home.join("activation-count");
    let request_spec = fake_resource_task_spec(&fixture.home, &marker);
    let (mut fixture, _, _, action_id, revision, _) =
        release_completion_fixture_with(fixture, request_spec, machine_other_than(authority));
    home.prepare_task(fixture.task_id).unwrap();
    let trainer_lock = flock_exclusive(
        &home.task_paths(fixture.task_id).runner_lock,
        LockMode::NonBlocking,
    )
    .unwrap();
    let intent = bind_saved_watcher(&mut fixture, action_id, revision);
    let watcher_task_id = intent.watcher_task_id.as_task_id();
    // the previous daemon committed acceptance and stopped before its spawn
    let executable = watcher_executable();
    let spec = ReleaseWatcherCommand::from_intent(fixture.resource.id, &intent)
        .normalized_spec(&executable, fixture.resource.supervisor.thread)
        .unwrap();
    let env = TaskEnv::capture();
    let row = new_queued_task(NewTask {
        id: watcher_task_id,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: crate::invocation::persist_workload(&spec.workload),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: env.clone(),
        binary: executable,
    });
    let callback = CallbackContext {
        env,
        cwd: spec.cwd.clone(),
        codex: CallbackExecutable::available(PathBuf::from("/bin/echo")),
    };
    assert_eq!(
        fixture
            .store
            .accept_release_watcher_for_authority(ReleaseWatcherAcceptanceInput {
                authority_machine: authority,
                resource_id: fixture.resource.id,
                supervisor: fixture.resource.supervisor,
                intent,
                row,
                spec,
                callback,
            })
            .unwrap(),
        ReleaseWatcherAcceptance::Inserted {
            task: watcher_task_id
        }
    );
    home.prepare_task(watcher_task_id).unwrap();
    let resource_id = fixture.resource.id;
    let database = fixture.database.clone();

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    let status = wait_for_watcher_status(&supervisor, resource_id, |status| {
        matches!(status, ReleaseWatcherStatus::Attention { .. })
    })
    .await;
    assert!(
        matches!(
        status,
        ReleaseWatcherStatus::Attention {
            action_id: saved_action,
            reason: ReleaseWatcherAttentionReason::LaunchUncertain { watcher_task_id: task }
                | ReleaseWatcherAttentionReason::WatcherTaskEnded {
                    watcher_task_id: task,
                    state: ProcessStatus::Lost,
                },
        } if saved_action == action_id && task == watcher_task_id
        ),
        "{status:?}"
    );
    for _ in 0..3 {
        call(&supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: resource_id,
            reply,
        })
        .await
        .unwrap();
    }
    let watcher = call(&store, |reply| StoreMsg::GetTask {
        id: watcher_task_id,
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert!(watcher.pid().is_none());
    assert!(!matches!(watcher.status(), ProcessStatus::Running));
    assert_eq!(watcher_task_count(&database), 1);
    let inspection = call(&supervisor, |reply| SupervisorMsg::InspectResource {
        id: resource_id,
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        inspection.loan.map(|loan| loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::AwaitingRelease { .. }
        })
    ));

    stop_test_supervisor(supervisor, handle).await;
    drop(trainer_lock);
}

#[tokio::test]
async fn remote_supervisor_watcher_is_bound_then_launched_once_for_the_supervisor_machine() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(watcher_executable());
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let remote_supervisor = machine_other_than(authority);
    let background_task = TaskId::new();
    let resource_id = {
        let mut store = Store::open(&home.db_path()).unwrap();
        let trainer_spec = spec();
        let mut resource = resource(authority);
        resource.supervisor = SupervisorAddress {
            machine: remote_supervisor,
            thread: trainer_spec.thread,
        };
        resource.registered_background_task = Some(background_task);
        store.register_resource(authority, &resource).unwrap();
        store
            .insert_local_task(
                &remote_task(background_task, &trainer_spec),
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
        test_release_association(&mut store, authority, resource.id, background_task);
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
        resource.id
    };
    home.prepare_task(background_task).unwrap();
    let trainer_lock = flock_exclusive(
        &home.task_paths(background_task).runner_lock,
        LockMode::NonBlocking,
    )
    .unwrap();

    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let status = wait_for_watcher_status(&supervisor, resource_id, |_| true).await;
    let ReleaseWatcherStatus::AwaitingRemoteSupervisor {
        watcher_task_id,
        supervisor_machine,
        ..
    } = status
    else {
        panic!("remote supervisor watcher status was {status:?}");
    };
    assert_eq!(supervisor_machine, remote_supervisor);
    let saved = Store::open(&home.db_path())
        .unwrap()
        .resource_snapshots_for_authority(authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource_id)
        .unwrap();
    // the authority binds the identity and baseline, but writes no task until the
    // supervisor machine has saved its callback route and asks for the launch
    assert!(matches!(
        saved.loan.clone().map(|loan| loan.state),
        Some(LoanState::Active {
            phase: LoanPhase::AwaitingRelease {
                watcher_intent: Some(intent),
                ..
            }
        }) if intent.watcher_task_id.as_task_id() == watcher_task_id
    ));
    assert_eq!(watcher_task_count(&home.db_path()), 0);

    // the supervisor machine prepares, saves its route, and launches the same identity
    let SupervisorAddress { thread, .. } = saved.resource.supervisor;
    let loan = saved.loan.unwrap();
    let LoanState::Active {
        phase: LoanPhase::AwaitingRelease { action_id, .. },
    } = loan.state
    else {
        unreachable!();
    };
    let action = SupervisorActionAuthority {
        authority_machine: authority,
        resource_id,
        loan_id: loan.id,
        action_id,
        expected_state_revision: saved.resource.state_revision,
        supervisor: SupervisorAddress {
            machine: remote_supervisor,
            thread,
        },
        assignment_revision: saved.resource.assignment_revision,
    };
    let send = |operation| {
        let supervisor = supervisor.clone();
        async move {
            call(&supervisor, |reply| SupervisorMsg::ResourceAction {
                request: Box::new(crate::resource::bound_action::ResourceActionRequest::new(
                    1, action, operation,
                )),
                reply,
            })
            .await
            .unwrap()
        }
    };
    let ResourceActionOutcome::Prepared { task } =
        send(ResourceActionOperation::PrepareReleaseWatcher {
            observed_background_task: background_task,
        })
        .await
    else {
        panic!("the authority must prepare the bound watcher");
    };
    assert_eq!(task.task_id, watcher_task_id);
    assert!(task.digest_matches());
    assert_eq!(task.spec.thread, thread);
    let launch = ResourceActionOperation::LaunchReleaseWatcher {
        observed_background_task: background_task,
        task: ActionTaskIdentity {
            request_id: task.request_id,
            task_id: task.task_id,
            normalized_spec_sha256: task.normalized_spec_sha256,
        },
    };
    let ResourceActionOutcome::Accepted { acceptance, .. } = send(launch.clone()).await else {
        panic!("the first launch must be accepted");
    };
    assert_eq!(acceptance, ActionTaskAcceptance::Inserted);
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();
    wait_for_task(&store, watcher_task_id, Duration::from_secs(10), |row| {
        row.status() == ProcessStatus::Running
    })
    .await;
    let ResourceActionOutcome::Accepted { acceptance, .. } = send(launch).await else {
        panic!("an exact retry must observe the accepted watcher");
    };
    assert_eq!(
        acceptance,
        ActionTaskAcceptance::Existing {
            state: ProcessStatus::Running
        }
    );
    assert_eq!(watcher_task_count(&home.db_path()), 1);
    wait_for_watcher_status(&supervisor, resource_id, |status| {
        matches!(status, ReleaseWatcherStatus::Running { watcher_task_id: running, .. } if *running == watcher_task_id)
    })
    .await;

    cancel_watcher(&supervisor, watcher_task_id).await;
    stop_test_supervisor(supervisor, handle).await;
    drop(trainer_lock);
}

/// Release action whose supervisor thread runs on another machine
struct RemoteWatcherFixture {
    poll: PollFixture,
    authority: SupervisorActionAuthority,
}

impl RemoteWatcherFixture {
    fn new() -> Self {
        let poll = PollFixture::new();
        let remote = machine_other_than(poll.authority);
        poll.store
            .conn
            .execute(
                "UPDATE resources SET supervisor_machine = ?1 WHERE id = ?2",
                params![
                    remote.as_uuid().to_string(),
                    poll.resource.id.as_uuid().to_string()
                ],
            )
            .unwrap();
        let authority = SupervisorActionAuthority {
            authority_machine: poll.authority,
            resource_id: poll.resource.id,
            loan_id: poll.notice.loan_id,
            action_id: poll.notice.action_id,
            expected_state_revision: poll.notice.state_revision,
            supervisor: SupervisorAddress {
                machine: remote,
                thread: poll.resource.supervisor.thread,
            },
            assignment_revision: poll.resource.assignment_revision,
        };
        Self { poll, authority }
    }

    fn identity(&self) -> ActionTaskIdentity {
        ActionTaskIdentity {
            request_id: self.poll.intent.request_id,
            task_id: self.poll.intent.watcher_task_id.as_task_id(),
            normalized_spec_sha256: self.poll.intent.normalized_spec_sha256,
        }
    }

    fn input(&self) -> RemoteReleaseWatcherAcceptanceInput {
        let spec = watcher_spec(&self.poll.resource, &self.poll.intent);
        let row = remote_task(self.poll.intent.watcher_task_id.as_task_id(), &spec);
        RemoteReleaseWatcherAcceptanceInput {
            authority: self.authority,
            observed_background_task: self.poll.background_task,
            task: self.identity(),
            row,
            spec,
        }
    }

    fn accept(
        &mut self,
        input: RemoteReleaseWatcherAcceptanceInput,
    ) -> Result<AcceptedActionTask, ResourceActionError> {
        self.poll
            .store
            .accept_remote_release_watcher_for_authority(input)
    }

    fn start(&mut self) {
        let task = self.poll.intent.watcher_task_id.as_task_id();
        self.poll
            .store
            .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();
        self.poll
            .store
            .set_pid(task, std::process::id() as i32)
            .unwrap();
        self.poll
            .store
            .update_execution_state(task, ProcessStatus::Running)
            .unwrap();
    }

    /// Overwrite the saved receipt JSON as a corrupted or forged acceptance would
    fn rewrite_receipt(
        &self,
        change: impl FnOnce(&mut crate::resource::bound_action::ActionTaskReceipt),
    ) {
        let task = self.poll.intent.watcher_task_id.as_task_id();
        let mut receipt = crate::store::resource::action_task::action_task_receipt_by_task(
            &self.poll.store.conn,
            task,
        )
        .unwrap()
        .unwrap();
        change(&mut receipt);
        self.poll
            .store
            .conn
            .execute(
                "UPDATE resource_action_task_receipts SET receipt_json = ?1 WHERE task_id = ?2",
                params![serde_json::to_string(&receipt).unwrap(), task.to_string()],
            )
            .unwrap();
    }

    fn receipt_count(&self) -> i64 {
        self.poll
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM resource_action_task_receipts",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }
}

fn rejected(result: Result<AcceptedActionTask, ResourceActionError>) -> ResourceActionRejection {
    match result {
        Err(ResourceActionError::Rejected(reason)) => reason,
        other => panic!("expected a typed rejection, found {other:?}"),
    }
}

#[test]
fn remote_watcher_acceptance_saves_one_receipt_and_remote_identity_without_a_local_route() {
    use crate::resource::bound_action::ActionTaskAcceptance;

    let mut fixture = RemoteWatcherFixture::new();
    let task = fixture.poll.intent.watcher_task_id.as_task_id();
    let request = fixture.poll.intent.request_id;
    let accepted = fixture.accept(fixture.input()).unwrap();
    assert_eq!(accepted.acceptance, ActionTaskAcceptance::Inserted);
    assert_eq!(accepted.receipt.task_id, task);
    assert_eq!(
        accepted.receipt.origin_machine(),
        fixture.authority.supervisor.machine
    );
    // row, identity, and first event exist; the callback route lives on the supervisor machine
    assert_eq!(
        watcher_acceptance_counts(&fixture.poll.store, request, task),
        [1, 0, 1, 1]
    );
    assert_eq!(fixture.receipt_count(), 1);
    let Some(ExecutorIdentity::Accepted(record)) =
        fixture.poll.store.executor_identity(task).unwrap()
    else {
        panic!("the watcher must have an accepted identity");
    };
    assert_eq!(record.origin_machine, fixture.authority.supervisor.machine);
    assert_eq!(record.execution_machine, fixture.poll.authority);
    let event = fixture
        .poll
        .store
        .first_pending_outbound(task)
        .unwrap()
        .unwrap()
        .event;
    assert_eq!(event.origin_machine, fixture.authority.supervisor.machine);

    // an exact retry, even after reopening, only observes the saved acceptance
    let retried = fixture.accept(fixture.input()).unwrap();
    assert_eq!(
        retried.acceptance,
        ActionTaskAcceptance::Existing {
            state: ProcessStatus::Queued
        }
    );
    let path = fixture.poll._directory.path().join("db");
    fixture.poll.store = Store::open(&path).unwrap();
    assert_eq!(
        fixture.accept(fixture.input()).unwrap().acceptance,
        ActionTaskAcceptance::Existing {
            state: ProcessStatus::Queued
        }
    );
    assert_eq!(
        watcher_acceptance_counts(&fixture.poll.store, request, task),
        [1, 0, 1, 1]
    );

    // a changed digest for the saved identity is a conflicting retry
    let mut changed = fixture.input();
    changed.task.normalized_spec_sha256 = normalized_spec_sha256(&spec()).unwrap();
    assert_eq!(
        rejected(fixture.accept(changed)),
        ResourceActionRejection::ConflictingRetry
    );
    assert_eq!(fixture.receipt_count(), 1);
}

#[test]
fn remote_watcher_acceptance_rejects_changed_command_owner_and_revision_without_records() {
    use crate::resource::bound_action::ResourceActionRejection;

    let mut fixture = RemoteWatcherFixture::new();
    let task = fixture.poll.intent.watcher_task_id.as_task_id();
    let request = fixture.poll.intent.request_id;

    let mut caller_command = fixture.input();
    caller_command.row.binary = PathBuf::from("/bin/sh");
    assert_eq!(
        rejected(fixture.accept(caller_command)),
        ResourceActionRejection::SpecMismatch
    );
    let mut caller_spec = fixture.input();
    caller_spec.spec.timeout = std::time::Duration::from_secs(60);
    assert_eq!(
        rejected(fixture.accept(caller_spec)),
        ResourceActionRejection::SpecMismatch
    );
    let mut other_task = fixture.input();
    other_task.task.task_id = TaskId::new();
    assert_eq!(
        rejected(fixture.accept(other_task)),
        ResourceActionRejection::IdentityConflict
    );
    let mut other_thread = fixture.input();
    other_thread.authority.supervisor.thread = crate::domain::ThreadId(uuid::Uuid::now_v7());
    assert_eq!(
        rejected(fixture.accept(other_thread)),
        ResourceActionRejection::NotCurrentSupervisor
    );
    let mut old_assignment = fixture.input();
    old_assignment.authority.assignment_revision =
        crate::resource::AssignmentRevision::new(fixture.authority.assignment_revision.get() + 1);
    assert_eq!(
        rejected(fixture.accept(old_assignment)),
        ResourceActionRejection::NotCurrentSupervisor
    );
    let mut stale = fixture.input();
    stale.authority.expected_state_revision =
        ResourceRevision::new(fixture.authority.expected_state_revision.get() + 1);
    assert!(matches!(
        rejected(fixture.accept(stale)),
        ResourceActionRejection::StaleRevision { .. }
    ));
    let mut other_action = fixture.input();
    other_action.authority.action_id = ActionId::new();
    assert_eq!(
        rejected(fixture.accept(other_action)),
        ResourceActionRejection::ActionNotPending
    );
    let mut other_trainer = fixture.input();
    other_trainer.observed_background_task = TaskId::new();
    assert_eq!(
        rejected(fixture.accept(other_trainer)),
        ResourceActionRejection::ActionNotPending
    );
    assert_eq!(
        watcher_acceptance_counts(&fixture.poll.store, request, task),
        [0, 0, 0, 0]
    );
    assert_eq!(fixture.receipt_count(), 0);

    // the remote path never serves a supervisor that runs on the authority
    let mut co_located = RemoteWatcherFixture::new();
    co_located
        .poll
        .store
        .conn
        .execute(
            "UPDATE resources SET supervisor_machine = ?1 WHERE id = ?2",
            params![
                co_located.poll.authority.as_uuid().to_string(),
                co_located.poll.resource.id.as_uuid().to_string()
            ],
        )
        .unwrap();
    let mut local_input = co_located.input();
    local_input.authority.supervisor.machine = co_located.poll.authority;
    assert_eq!(
        rejected(co_located.accept(local_input)),
        ResourceActionRejection::NotCurrentSupervisor
    );
    assert_eq!(co_located.receipt_count(), 0);
}

#[test]
fn remote_watcher_uses_the_same_baseline_stop_marker_and_leaves_release_to_process_exit() {
    let mut fixture = RemoteWatcherFixture::new();
    let command = fixture.poll.command();
    fixture.accept(fixture.input()).unwrap();
    // a queued acceptance is not a running watcher and cannot stop the trainer
    assert_eq!(
        fixture.poll.poll(command),
        ReleaseWatcherPollOutcome::WatcherNotRunning
    );
    fixture.start();
    assert_eq!(
        fixture.poll.poll(command),
        ReleaseWatcherPollOutcome::WaitingForCheckpoint
    );
    assert!(fixture.poll.trainer_cancel_marker().is_none());

    fixture
        .poll
        .write_generation("generation-after-baseline", 30);
    let ReleaseWatcherPollOutcome::StopCommitted {
        generation_id,
        cancel_requested_at,
    } = fixture.poll.poll(command)
    else {
        panic!("a new checkpoint must commit the exact stop");
    };
    assert_eq!(generation_id, "generation-after-baseline");
    assert_eq!(
        fixture.poll.trainer_cancel_marker(),
        Some(cancel_requested_at)
    );
    // an exact retry reuses the saved decision and marker
    assert_eq!(
        fixture.poll.poll(command),
        ReleaseWatcherPollOutcome::StopCommitted {
            generation_id,
            cancel_requested_at,
        }
    );

    // the stop marker alone does not release the loan while the trainer runs
    assert!(
        fixture
            .poll
            .store
            .complete_release_for_authority(
                fixture.poll.authority,
                fixture.poll.resource.id,
                fixture.poll.notice.action_id,
                fixture.poll.notice.state_revision,
            )
            .is_err()
    );
    let snapshot = fixture
        .poll
        .store
        .resource_snapshots_for_authority(fixture.poll.authority)
        .unwrap()
        .remove(0);
    assert!(matches!(
        snapshot.loan.unwrap().state,
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease { .. }
        }
    ));
}

#[test]
fn accepted_remote_watcher_keeps_polling_after_its_supervisor_is_replaced() {
    for local_replacement in [false, true] {
        let mut fixture = RemoteWatcherFixture::new();
        let command = fixture.poll.command();
        fixture.accept(fixture.input()).unwrap();
        fixture.start();
        let replacement = if local_replacement {
            fixture.poll.authority
        } else {
            machine_other_than(fixture.authority.supervisor.machine)
        };
        fixture.poll.replace_supervisor(replacement);
        fixture
            .poll
            .write_generation("generation-after-baseline", 30);

        // the saved receipt, not the current assignment, owns the accepted watcher
        let ReleaseWatcherPollOutcome::StopCommitted {
            generation_id,
            cancel_requested_at,
        } = fixture.poll.poll(command)
        else {
            panic!("the accepted watcher must keep its stop authority ({local_replacement})");
        };
        assert_eq!(
            fixture.poll.trainer_cancel_marker(),
            Some(cancel_requested_at)
        );
        assert_eq!(
            fixture.poll.poll(command),
            ReleaseWatcherPollOutcome::StopCommitted {
                generation_id,
                cancel_requested_at,
            }
        );
        // an exact acceptance retry observes the saved watcher and inserts nothing
        assert!(matches!(
            fixture.accept(fixture.input()).unwrap().acceptance,
            ActionTaskAcceptance::Existing { .. }
        ));
        assert_eq!(fixture.receipt_count(), 1);
    }
}

#[test]
fn remote_watcher_acceptance_after_replacement_requires_the_current_assignment() {
    use crate::resource::bound_action::ResourceActionRejection;

    let mut fixture = RemoteWatcherFixture::new();
    fixture
        .poll
        .replace_supervisor(machine_other_than(fixture.authority.supervisor.machine));

    // an unaccepted watcher named by the replaced assignment is not a new owner
    assert_eq!(
        rejected(fixture.accept(fixture.input())),
        ResourceActionRejection::NotCurrentSupervisor
    );
    assert_eq!(fixture.receipt_count(), 0);
}

#[test]
fn remote_watcher_with_a_missing_or_changed_receipt_is_refused_without_a_marker() {
    type Tamper = fn(&RemoteWatcherFixture);
    let cases: [(&str, Tamper); 4] = [
        ("missing receipt", |fixture| {
            fixture
                .poll
                .store
                .conn
                .execute("DELETE FROM resource_action_task_receipts", [])
                .unwrap();
        }),
        ("receipt names the authority as supervisor", |fixture| {
            fixture.rewrite_receipt(|receipt| {
                receipt.authority.supervisor.machine = fixture.poll.authority;
            });
        }),
        ("receipt names another command", |fixture| {
            fixture.rewrite_receipt(|receipt| {
                receipt.normalized_spec_sha256 = normalized_spec_sha256(&spec()).unwrap();
            });
        }),
        ("receipt names another revision", |fixture| {
            fixture.rewrite_receipt(|receipt| {
                receipt.authority.expected_state_revision =
                    ResourceRevision::new(receipt.authority.expected_state_revision.get() + 1);
            });
        }),
    ];
    for (name, tamper) in cases {
        let mut fixture = RemoteWatcherFixture::new();
        let command = fixture.poll.command();
        fixture.accept(fixture.input()).unwrap();
        fixture.start();
        fixture
            .poll
            .write_generation("generation-after-baseline", 30);
        tamper(&fixture);

        assert_eq!(
            fixture.poll.poll(command),
            attention(ReleaseWatcherPollAttention::WatcherIdentityConflict),
            "{name}"
        );
        assert!(fixture.poll.trainer_cancel_marker().is_none(), "{name}");
    }
}
