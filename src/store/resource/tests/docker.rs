//! Container resource work through real daemon actors, workers, and Docker Engine
//!
//! These tests need a Docker Engine and pull a small public image. The engine
//! may have no GPU, so a `docker` wrapper first on `PATH` drops `--gpus` before
//! it calls the real CLI. Run them with `just test-docker`

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use ractor::Actor;
use serde_json::json;
use tempfile::tempdir;

use super::fixtures::{
    ServingFixture, machine_other_than, serving_fixture_at, spec, stop_test_supervisor,
    wait_for_awaiting_return, wait_for_terminal_task,
};
use crate::daemon::actors::supervisor::SUPERVISOR_TEST_LOCK;
use crate::daemon::actors::{StoreMsg, SupervisorActor, SupervisorArgs, SupervisorMsg, call};
use crate::domain::{ContainerExitEvidence, ExitReason, ProcessStatus, TaskId, TaskRow};
use crate::home::Home;
use crate::machine::load_or_create_machine_id;
use crate::resource::{
    CommandSpec, Loan, LoanClosure, LoanPhase, LoanState, ResourceId, ReturnDecision, ReturnLaunch,
    ReturnWork, SupervisorActionAuthority,
};
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::{EndedRestoreResolution, Store};
use crate::submission::RequestId;

const IMAGE: &str = "busybox:1.37";

/// Real Docker CLI, a pulled image, and a GPU-dropping wrapper on `PATH`
struct DockerEngine {
    _wrapper: tempfile::TempDir,
    real: PathBuf,
    image_id: String,
}

impl DockerEngine {
    fn prepare() -> Self {
        let real = which::which("docker").expect("these tests need the docker CLI on PATH");
        let run = |args: &[&str]| {
            let output = Command::new(&real).args(args).output().unwrap();
            assert!(
                output.status.success(),
                "docker {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        };
        run(&["info", "--format", "{{.ServerVersion}}"]);
        run(&["pull", "--quiet", IMAGE]);
        let image_id = run(&["image", "inspect", "--format", "{{.Id}}", IMAGE])
            .trim()
            .to_owned();

        let wrapper = tempdir().unwrap();
        let script = wrapper.path().join("docker");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n\
                 # this engine may have no GPU, so drop --gpus and its value\n\
                 skip=0\n\
                 for arg; do\n\
                 shift\n\
                 if [ \"$skip\" = 1 ]; then skip=0; continue; fi\n\
                 if [ \"$arg\" = --gpus ]; then skip=1; continue; fi\n\
                 set -- \"$@\" \"$arg\"\n\
                 done\n\
                 exec '{}' \"$@\"\n",
                real.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = format!(
            "{}:{}",
            wrapper.path().display(),
            std::env::var("PATH").unwrap_or_default()
        );
        // the daemon actors capture PATH as the executor environment; these
        // tests run one at a time under the supervisor test lock
        unsafe { std::env::set_var("PATH", path) };
        Self {
            _wrapper: wrapper,
            real,
            image_id,
        }
    }

    fn container_exists(&self, task: TaskId) -> bool {
        let name = crate::container::docker::container_name(task);
        let output = Command::new(&self.real)
            .args([
                "container",
                "ls",
                "--all",
                "--filter",
                &format!("name={name}"),
                "--format",
                "{{.Names}}",
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line == name)
    }

    fn spec(&self, fixture: &ServingFixture, script: &str) -> NormalizedSpec {
        let mut spec = fixture.spec.clone();
        spec.name = crate::domain::TaskName::parse("docker container test").unwrap();
        spec.workload = NormalizedWorkload::Container(Box::new(
            crate::container::ContainerWorkload::from_value(&json!({
                "image": self.image_id,
                "entrypoint": ["sh", "-c", script],
                "gpus": "all",
                "memory": "64m"
            }))
            .unwrap(),
        ));
        spec
    }
}

/// Daemon home with a Serving loan whose first request is a native command
struct DaemonFixture {
    home: Home,
    fixture: ServingFixture,
}

fn daemon_fixture() -> DaemonFixture {
    let directory = tempdir().unwrap();
    let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let authority = load_or_create_machine_id(&home).unwrap();
    let fixture = serving_fixture_at(directory, &home.db_path(), authority, true, true, spec());
    DaemonFixture { home, fixture }
}

async fn task_row(store: &ractor::ActorRef<StoreMsg>, id: TaskId) -> TaskRow {
    call(store, |reply| StoreMsg::GetTask { id, reply })
        .await
        .unwrap()
        .unwrap()
}

fn output(home: &Home, task: TaskId) -> String {
    std::fs::read_to_string(home.task_paths(task).output).unwrap_or_default()
}

fn assert_removed(row: &TaskRow, exit_code: i32) {
    assert!(
        matches!(
            &row.container_exit_evidence,
            ContainerExitEvidence::Confirmed { exit_code: code, .. } if *code == exit_code
        ),
        "{:?}",
        row.container_exit_evidence
    );
}

/// Authority of the exact pending return action on one AwaitingReturn loan
fn return_authority(
    fixture: &ServingFixture,
    loan: &Loan,
    database: &Path,
) -> SupervisorActionAuthority {
    let LoanState::Active {
        phase: LoanPhase::AwaitingReturn { action_id, .. },
    } = &loan.state
    else {
        panic!("the loan must await its return");
    };
    let store = Store::open(database).unwrap();
    let resource = store
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == fixture.resource.id)
        .unwrap()
        .resource;
    SupervisorActionAuthority {
        authority_machine: fixture.authority,
        resource_id: fixture.resource.id,
        loan_id: loan.id,
        action_id: *action_id,
        expected_state_revision: resource.state_revision,
        supervisor: resource.supervisor,
        assignment_revision: resource.assignment_revision,
    }
}

fn saved_loan_state(database: &Path, loan: &Loan) -> LoanState {
    let store = Store::open(database).unwrap();
    let json: String = store
        .conn
        .query_row(
            "SELECT state_json FROM loans WHERE id = ?1",
            [loan.id.as_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&json).unwrap()
}

async fn reconcile(supervisor: &ractor::ActorRef<SupervisorMsg>, resource: ResourceId) {
    call(supervisor, |reply| SupervisorMsg::ReconcileResource {
        id: resource,
        reply,
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Docker Engine; run with just test-docker"]
async fn queued_container_runs_in_docker_and_releases_its_turn_after_removal() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
    let docker = DockerEngine::prepare();
    let DaemonFixture { home, mut fixture } = daemon_fixture();
    let container = fixture
        .store
        .accept_resource_request(
            fixture.authority,
            RequestId::new(),
            TaskId::new(),
            fixture.resource.id,
            machine_other_than(fixture.authority),
            docker.spec(&fixture, "echo queued container output; exit 3"),
        )
        .unwrap();
    let resource_id = fixture.resource.id;
    drop(fixture.store);

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
    let row = wait_for_terminal_task(&store, container.task_id).await;
    assert_eq!(row.exit_reason(), Some(&ExitReason::Exit { code: 3 }));
    assert_removed(&row, 3);
    assert!(output(&home, container.task_id).contains("queued container output"));
    assert!(!docker.container_exists(container.task_id));
    wait_for_awaiting_return(&supervisor, resource_id).await;

    stop_test_supervisor(supervisor, handle).await;
}

/// Run the fixture's first request to the return decision, then launch one container return
async fn container_return(
    script: &str,
) -> (
    DockerEngine,
    Home,
    ServingFixture,
    ractor::ActorRef<SupervisorMsg>,
    ractor::concurrency::JoinHandle<()>,
    SupervisorActionAuthority,
    Loan,
    TaskId,
) {
    let docker = DockerEngine::prepare();
    let DaemonFixture { home, fixture } = daemon_fixture();
    let (supervisor, handle) = SupervisorActor::spawn(
        None,
        SupervisorActor,
        SupervisorArgs::new(home.clone(), None),
    )
    .await
    .unwrap();
    let loan = wait_for_awaiting_return(&supervisor, fixture.resource.id).await;
    let authority = return_authority(&fixture, &loan, &home.db_path());
    let launch = ReturnLaunch {
        request_id: RequestId::new(),
        task_id: TaskId::new(),
        work: ReturnWork::EvaluationOrNextEpoch {
            completed_task: fixture.resource.registered_background_task.unwrap(),
            spec: CommandSpec::try_from(docker.spec(&fixture, script)).unwrap(),
        },
    };
    let task_id = launch.task_id;
    call(&supervisor, |reply| SupervisorMsg::DecideReturn {
        authority,
        decision: Box::new(ReturnDecision::Launch(Box::new(launch))),
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    (
        docker, home, fixture, supervisor, handle, authority, loan, task_id,
    )
}

#[tokio::test]
#[ignore = "requires Docker Engine; run with just test-docker"]
async fn container_return_closes_its_loan_after_its_container_is_removed() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
    let (docker, home, fixture, supervisor, handle, _authority, loan, task_id) =
        container_return("sleep 1; echo evaluation done").await;
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();

    let row = wait_for_terminal_task(&store, task_id).await;
    assert_eq!(row.status(), ProcessStatus::Succeeded);
    assert_removed(&row, 0);
    assert!(output(&home, task_id).contains("evaluation done"));
    assert!(!docker.container_exists(task_id));
    reconcile(&supervisor, fixture.resource.id).await;
    assert!(matches!(
        saved_loan_state(&home.db_path(), &loan),
        LoanState::Closed {
            result: LoanClosure::ForegroundReturnEnded { task_id: ended, .. }
        } if ended == task_id
    ));
    assert_eq!(
        task_row(&store, task_id).await.status(),
        ProcessStatus::Succeeded
    );

    stop_test_supervisor(supervisor, handle).await;
}

#[tokio::test]
#[ignore = "requires Docker Engine; run with just test-docker"]
async fn failed_container_return_stays_reserved_until_the_supervisor_resolves_it() {
    let _guard = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
    let (docker, home, fixture, supervisor, handle, mut authority, loan, task_id) =
        container_return("echo evaluation failed; exit 4").await;
    let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
        .await
        .unwrap();

    let row = wait_for_terminal_task(&store, task_id).await;
    assert_eq!(row.exit_reason(), Some(&ExitReason::Exit { code: 4 }));
    assert_removed(&row, 4);
    assert!(!docker.container_exists(task_id));
    reconcile(&supervisor, fixture.resource.id).await;
    assert!(matches!(
        saved_loan_state(&home.db_path(), &loan),
        LoanState::Active {
            phase: LoanPhase::Restoring { resume_task_id, .. }
        } if resume_task_id == task_id
    ));

    let resource = Store::open(&home.db_path())
        .unwrap()
        .resource_snapshots_for_authority(fixture.authority)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == fixture.resource.id)
        .unwrap()
        .resource;
    authority.expected_state_revision = resource.state_revision;
    call(&supervisor, |reply| SupervisorMsg::ResolveEndedRestore {
        resolution: Box::new(EndedRestoreResolution {
            authority,
            task_id,
            reason: "evaluation failed; no background work".into(),
        }),
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        saved_loan_state(&home.db_path(), &loan),
        LoanState::Closed {
            result: LoanClosure::RestoreEnded {
                outcome: ExitReason::Exit { code: 4 },
                ..
            }
        }
    ));

    stop_test_supervisor(supervisor, handle).await;
}
