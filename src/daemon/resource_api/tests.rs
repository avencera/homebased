use crate::daemon::actors::supervisor::SUPERVISOR_TEST_LOCK;
use crate::store::Store;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use axum::http::StatusCode;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::{attention, decode_control_response, local_models, local_resource, remote_detail};
use crate::config::{Discovery, FleetSettings};
use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, SupervisorActor, SupervisorArgs, SupervisorMsg, call};
use crate::domain::{ProcessStatus, TaskId, ThreadId};
use crate::error::AppError;
use crate::files::StreamSlots;
use crate::fleet::FleetState;
use crate::fleet::address::MachineAddress;
use crate::fleet::directory::LocalMachine;
use crate::fleet::http::ClusterResponse;
use crate::fleet::protocol::SUPPORTED_PROTOCOLS;
use crate::fleet::runtime::{FleetRuntime, FleetStart, RuntimeTimings};
use crate::home::Home;
use crate::machine::{LocalIdentity, MachineId, MachineName};
use crate::resource::api::{
    AttentionCode, BrowserResourceAction, RESOURCE_PENDING_PATH, RESOURCE_REGISTER_PATH,
};
use crate::resource::command_shape::test_support::FakeTrainer;
use crate::resource::operator_release::{
    OperatorAttestationId, OperatorGpuFreeAttestation, OperatorGpuFreeConfirmation,
    OperatorObservation, OperatorStateBinding,
};
use crate::resource::{
    ActionId, AssignmentRevision, CommandSpec, DeliveryAttemptId, Loan, LoanId, LoanPhase,
    LoanState, NoticeId, ResourceId, ResourceRevision, ReturnContext, ReturnExecutionMode,
    ReturnLaunch, ReturnWork, SupervisorActionAuthority, SupervisorAddress, SupervisorNotice,
    SupervisorNoticeDelivery, SupervisorNoticePayload,
};
use crate::spec::{NormalizedTaskWorkload, NormalizedWorkload};
use crate::store::{
    BackgroundLaunchAcceptance, BackgroundLaunchInput, ResourceControlRequest,
    ReturnTaskAcceptance, ReturnTaskAcceptanceInput, ReturnTaskOrigin,
};
use crate::submission::{CallbackExecutable, RequestId};
use uuid::Uuid;

const THREAD: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e431";

/// Running daemon routes on one registered resource; callers hold the supervisor test lock
struct Fixture {
    _directory: TempDir,
    state: AppState,
    supervisor: ActorRef<SupervisorMsg>,
    supervisor_handle: JoinHandle<()>,
    servers: Vec<JoinHandle<()>>,
    dashboard: SocketAddr,
    socket: SocketAddr,
    resource: ResourceId,
    bin: PathBuf,
    callback_cwd: PathBuf,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().join("state"))).unwrap();
        home.ensure().unwrap();
        let bin = directory.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let codex = bin.join("codex");
        std::fs::write(&codex, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&codex, std::fs::Permissions::from_mode(0o755)).unwrap();
        let callback_cwd = directory.path().join("callback");
        std::fs::create_dir(&callback_cwd).unwrap();

        let (supervisor, supervisor_handle) = SupervisorActor::spawn(
            None,
            SupervisorActor,
            SupervisorArgs::new(home.clone(), None),
        )
        .await
        .unwrap();
        let store = call(&supervisor, |reply| SupervisorMsg::GetStore { reply })
            .await
            .unwrap();
        let state = AppState {
            home: home.clone(),
            store,
            supervisor: supervisor.clone(),
            web: None,
            content: None,
            stream_slots: StreamSlots::new(),
            machine: LocalMachine {
                identity: LocalIdentity::start(&home).unwrap(),
                name: MachineName::fallback(),
                protocol: SUPPORTED_PROTOCOLS,
            },
            fleet: FleetState::Disabled,
            message_receiver: crate::daemon::message_receiver::MessageReceiver::default(),
            locks: crate::daemon::DaemonLocks::default(),
        };

        let dashboard_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dashboard = dashboard_listener.local_addr().unwrap();
        let socket_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = socket_listener.local_addr().unwrap();
        let dashboard_router = crate::daemon::web::router(state.clone(), dashboard);
        let socket_router = crate::daemon::api::socket_router(state.clone());
        let servers = vec![
            tokio::spawn(async move {
                let _ = axum::serve(dashboard_listener, dashboard_router).await;
            }),
            tokio::spawn(async move {
                let _ = axum::serve(socket_listener, socket_router).await;
            }),
            // the origin-owned cancellation intent reaches the authority through this loop
            tokio::spawn(crate::daemon::cancel_delivery::run(state.clone())),
        ];

        let mut fixture = Self {
            _directory: directory,
            state,
            supervisor,
            supervisor_handle,
            servers,
            dashboard,
            socket,
            resource: ResourceId::new(),
            bin,
            callback_cwd,
        };
        let (status, body) = fixture
            .socket_post(
                RESOURCE_REGISTER_PATH,
                json!({
                    "api_version": 1,
                    "spec": {
                        "id": fixture.resource,
                        "display_name": "RTX 5090",
                        "supervisor": { "machine": fixture.local(), "thread": THREAD },
                    }
                }),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        fixture.resource = serde_json::from_value(body["resource"]["id"].clone()).unwrap();
        fixture
    }

    fn local(&self) -> MachineId {
        self.state.machine.identity.machine
    }

    fn origin(&self) -> String {
        format!("http://{}", self.dashboard)
    }

    async fn socket_post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        let host = self.socket.to_string();
        send(
            self.socket,
            "POST",
            path,
            &[("host", host), ("content-type", "application/json".into())],
            Some(body),
        )
        .await
    }

    async fn socket_get(&self, path: &str) -> (StatusCode, Value) {
        send(
            self.socket,
            "GET",
            path,
            &[("host", self.socket.to_string())],
            None,
        )
        .await
    }

    async fn browser_action(&self, body: Value) -> (StatusCode, Value) {
        let path = format!("/v1/resources/{}/actions", self.resource.as_uuid());
        send(
            self.dashboard,
            "POST",
            &path,
            &[
                ("host", self.dashboard.to_string()),
                ("origin", self.origin()),
                ("content-type", "application/json".into()),
            ],
            Some(body),
        )
        .await
    }

    async fn detail(&self) -> Value {
        let (status, body) = self
            .socket_get(&format!("/v1/resources/{}", self.resource.as_uuid()))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    async fn submit_request(&self) -> (RequestId, TaskId) {
        let request_id = RequestId::new();
        let (status, body) = self
            .socket_post(
                &format!("/v1/resources/{}/requests", self.resource.as_uuid()),
                json!({
                    "api_version": 1,
                    "request_id": request_id,
                    "spec": {
                        "api_version": 1,
                        "thread": THREAD,
                        "name": "attention benchmark",
                        "cwd": "/tmp",
                        "timeout": "4h",
                        "workload": { "type": "task", "command": ["echo", "hello"] }
                    },
                    "env": { "path": self.bin, "home": "/tmp" },
                    "callback_cwd": self.callback_cwd,
                }),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["outcome"]["type"], "waiting");
        let task_id = serde_json::from_value(body["task_id"].clone()).unwrap();
        (request_id, task_id)
    }

    async fn seed_registered_trainer(&self, end_task: bool) -> (TaskId, ResourceRevision) {
        let root = self.callback_cwd.canonicalize().unwrap();
        let trainer = FakeTrainer::new(&root);
        let request_id = RequestId::new();
        let task_id = TaskId::new();
        let acceptance = call(&self.state.store, |reply| {
            StoreMsg::AcceptBackgroundLaunchForAuthority {
                input: Box::new(BackgroundLaunchInput {
                    authority_machine: self.local(),
                    resource_id: self.resource,
                    request_id,
                    task_id,
                    spec: trainer.spec(THREAD.parse().unwrap(), "controlled trainer fixture"),
                    env: trainer.env.clone(),
                    callback_codex: CallbackExecutable::available(self.bin.join("codex")),
                }),
                reply,
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            acceptance,
            BackgroundLaunchAcceptance::Inserted { task, .. } if task == task_id
        ));
        call(&self.state.store, |reply| StoreMsg::CasStatus {
            id: task_id,
            from: ProcessStatus::Queued,
            to: ProcessStatus::Running,
            reply,
        })
        .await
        .unwrap()
        .expect("the synthetic launch row must enter running state");
        call(&self.supervisor, |reply| SupervisorMsg::ReconcileResource {
            id: self.resource,
            reply,
        })
        .await
        .unwrap();

        if end_task {
            call(&self.state.store, |reply| StoreMsg::CasStatus {
                id: task_id,
                from: ProcessStatus::Running,
                to: ProcessStatus::Lost,
                reply,
            })
            .await
            .unwrap()
            .expect("the synthetic trainer must end before attestation");
        }

        let resource = local_resource(&self.state, self.resource).await.unwrap();
        (task_id, resource.state_revision)
    }

    /// Bind a first launch whose fake task starts and is lost before its start registers
    async fn seed_early_ended_launch(&self) -> (RequestId, TaskId) {
        let root = self.callback_cwd.canonicalize().unwrap();
        let trainer = FakeTrainer::new(&root);
        let (request_id, task_id) = (RequestId::new(), TaskId::new());
        let acceptance = call(&self.state.store, |reply| {
            StoreMsg::AcceptBackgroundLaunchForAuthority {
                input: Box::new(BackgroundLaunchInput {
                    authority_machine: self.local(),
                    resource_id: self.resource,
                    request_id,
                    task_id,
                    spec: trainer.spec(THREAD.parse().unwrap(), "controlled trainer fixture"),
                    env: trainer.env.clone(),
                    callback_codex: CallbackExecutable::available(self.bin.join("codex")),
                }),
                reply,
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            acceptance,
            BackgroundLaunchAcceptance::Inserted { task, .. } if task == task_id
        ));
        for (from, to) in [
            (ProcessStatus::Queued, ProcessStatus::Running),
            (ProcessStatus::Running, ProcessStatus::Lost),
        ] {
            call(&self.state.store, |reply| StoreMsg::CasStatus {
                id: task_id,
                from,
                to,
                reply,
            })
            .await
            .unwrap()
            .expect("the synthetic launch row must change state");
        }
        (request_id, task_id)
    }

    async fn socket_with_supervisor(
        &self,
        supervisor: ActorRef<SupervisorMsg>,
    ) -> (SocketAddr, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut state = self.state.clone();
        state.supervisor = supervisor;
        let router = super::socket_routes().with_state(state);
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        (address, server)
    }

    /// Seed an AwaitingReturn loan whose return notice used every automatic attempt
    fn seed_failed_return_notice(&self, destination: SupervisorAddress) -> SupervisorNotice {
        let loan_id = LoanId::new();
        let action_id = ActionId::new();
        let state = LoanState::Active {
            phase: LoanPhase::AwaitingReturn {
                action_id,
                return_context: ReturnContext::Idle,
            },
        };
        let notice = SupervisorNotice {
            id: NoticeId::new(),
            loan_id,
            action_id,
            state_revision: ResourceRevision::new(0),
            destination,
            assignment_revision: AssignmentRevision::new(0),
            payload: SupervisorNoticePayload::ReturnRequired {
                return_context: ReturnContext::Idle,
            },
            delivery: SupervisorNoticeDelivery::Failed {
                attempts: 3,
                last_error: "receiver offline".into(),
            },
        };
        let loan = Loan {
            id: loan_id,
            resource_id: self.resource,
            state,
        };
        Store::seed_loan_notice_for_test(&self.state.home.db_path(), &loan, &notice);
        notice
    }

    /// Accept a queued return task without starting its worker
    async fn seed_restoring_return(&self, mode: ReturnExecutionMode) -> TaskId {
        let destination = SupervisorAddress {
            machine: self.local(),
            thread: THREAD.parse().unwrap(),
        };
        let notice = self.seed_failed_return_notice(destination);
        let trainer = FakeTrainer::new(&self.callback_cwd.canonicalize().unwrap());
        let mut spec = trainer.spec(destination.thread, "resource detail return fixture");
        if mode == ReturnExecutionMode::NativeForeground {
            spec.workload = NormalizedWorkload::Task(NormalizedTaskWorkload {
                command: crate::invocation::CommandLine::try_from_argv(vec![
                    "/bin/echo".into(),
                    "foreground return".into(),
                ])
                .unwrap(),
            });
        }
        let launch = ReturnLaunch {
            request_id: RequestId::new(),
            task_id: TaskId::new(),
            work: ReturnWork::NewBackgroundWork {
                spec: CommandSpec::try_from(spec).unwrap(),
            },
        };
        let authority = SupervisorActionAuthority {
            authority_machine: self.local(),
            resource_id: self.resource,
            loan_id: notice.loan_id,
            action_id: notice.action_id,
            expected_state_revision: notice.state_revision,
            supervisor: notice.destination,
            assignment_revision: notice.assignment_revision,
        };
        let task_id = launch.task_id;
        let acceptance = call(&self.state.store, |reply| {
            StoreMsg::AcceptReturnTaskForAuthority {
                input: Box::new(ReturnTaskAcceptanceInput {
                    authority,
                    launch,
                    executor_env: trainer.env.clone(),
                    origin: ReturnTaskOrigin::Local {
                        callback_codex: CallbackExecutable::available(self.bin.join("codex")),
                    },
                }),
                reply,
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            acceptance,
            ReturnTaskAcceptance::Inserted { task, .. } if task == task_id
        ));
        task_id
    }

    fn control_operation_count(&self) -> i64 {
        Store::resource_control_operation_count_for_test(&self.state.home.db_path())
    }

    async fn stop(self) {
        for server in &self.servers {
            server.abort();
        }
        self.supervisor.stop(None);
        // actor names are global, so the next test waits for this tree to unregister
        let _ = self.supervisor_handle.await;
    }
}

struct ReconcileProbe {
    fail: bool,
}

impl Actor for ReconcileProbe {
    type Msg = SupervisorMsg;
    type State = mpsc::UnboundedSender<ResourceId>;
    type Arguments = mpsc::UnboundedSender<ResourceId>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        requests: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(requests)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let SupervisorMsg::ReconcileResource { id, reply } = message else {
            return Ok(());
        };
        state
            .send(id)
            .expect("reconciliation probe receiver is live");
        let result = if self.fail {
            Err(AppError::Internal {
                message: "controlled reconciliation failure".into(),
            })
        } else {
            Ok(())
        };
        crate::daemon::actors::send_reply(reply, result);
        Ok(())
    }
}

async fn send(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: Option<Value>,
) -> (StatusCode, Value) {
    let stream = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut builder = http::Request::builder().method(method).uri(path);
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    let bytes = body.map_or_else(Vec::new, |body| serde_json::to_vec(&body).unwrap());
    let request = builder.body(Full::new(Bytes::from(bytes))).unwrap();
    let response = sender.send_request(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn error_code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or_default()
}

fn operator_attestation(
    resource_id: ResourceId,
    authority_machine: MachineId,
    task_id: TaskId,
    revision: ResourceRevision,
) -> OperatorGpuFreeAttestation {
    OperatorGpuFreeAttestation {
        operation_id: OperatorAttestationId::new(),
        resource_id,
        authority_machine,
        task_id,
        expected_state_revision: revision,
        state_binding: OperatorStateBinding::NoLoan,
        observation: OperatorObservation::try_from(
            "inspected the authority GPU; no trainer process remains".to_owned(),
        )
        .unwrap(),
        confirmation: OperatorGpuFreeConfirmation::OperatorConfirmedGpuFree,
    }
}

fn operator_release_body(attestation: &OperatorGpuFreeAttestation) -> Value {
    json!({
        "api_version": 1,
        "attestation": attestation,
    })
}

async fn spawn_reconcile_probe(
    fail: bool,
) -> (
    ActorRef<SupervisorMsg>,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<ResourceId>,
) {
    let (sender, requests) = mpsc::unbounded_channel();
    let (actor, handle) = Actor::spawn(None, ReconcileProbe { fail }, sender)
        .await
        .unwrap();
    (actor, handle, requests)
}

#[tokio::test]
async fn local_resource_detail_shows_the_saved_return_mode_only_while_restoring() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let idle = fixture.detail().await;
    assert!(idle.get("return_execution_mode").is_none(), "{idle}");

    fixture
        .seed_restoring_return(ReturnExecutionMode::DirectSegmentTrainer)
        .await;
    let detail = fixture.detail().await;
    assert_eq!(detail["return_execution_mode"], "direct_segment_trainer");
    fixture.stop().await;
}

#[tokio::test]
async fn remote_resource_detail_preserves_the_authority_return_mode() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let authority = Fixture::new().await;
    let task = authority
        .seed_restoring_return(ReturnExecutionMode::NativeForeground)
        .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let authority_runtime = FleetRuntime::start(FleetStart {
        local: authority.state.machine.clone(),
        settings: FleetSettings {
            discovery: Discovery {
                mdns: false,
                tailscale: None,
            },
            machines: Vec::new(),
        },
        listener: Some(address),
        peers_path: authority._directory.path().join("authority-peers.json"),
        timings: RuntimeTimings::default(),
    })
    .unwrap();
    let mut authority_state = authority.state.clone();
    authority_state.fleet = FleetState::Enabled(authority_runtime.handle());
    let router = crate::daemon::web::router(authority_state, address);
    let authority_server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    let caller_machine = LocalMachine {
        identity: LocalIdentity {
            machine: MachineId::new(),
            boot: crate::machine::BootId::new(),
        },
        name: MachineName::parse("resource-reader").unwrap(),
        protocol: SUPPORTED_PROTOCOLS,
    };
    let peer_address = MachineAddress::from_socket(address);
    let caller_runtime = FleetRuntime::start(FleetStart {
        local: caller_machine.clone(),
        settings: FleetSettings {
            discovery: Discovery {
                mdns: false,
                tailscale: None,
            },
            machines: vec![peer_address.clone()],
        },
        listener: None,
        peers_path: authority._directory.path().join("caller-peers.json"),
        timings: RuntimeTimings::default(),
    })
    .unwrap();
    let caller_handle = caller_runtime.handle();
    caller_handle.probe_address(&peer_address).await.unwrap();
    let mut caller_state = authority.state.clone();
    caller_state.machine = caller_machine;
    caller_state.fleet = FleetState::Enabled(caller_handle);

    let detail = remote_detail(&caller_state, authority.resource)
        .await
        .unwrap();
    assert_eq!(detail.current_task_id, Some(task));
    assert_eq!(
        detail.return_execution_mode,
        Some(ReturnExecutionMode::NativeForeground)
    );
    assert_eq!(
        serde_json::to_value(detail).unwrap()["return_execution_mode"],
        "native_foreground"
    );

    caller_runtime.shutdown().await;
    authority_server.abort();
    authority_runtime.shutdown().await;
    authority.stop().await;
}

#[tokio::test]
async fn operator_release_checks_path_authority_and_confirmation_before_store() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let (task_id, revision) = fixture.seed_registered_trainer(true).await;
    let attestation = operator_attestation(fixture.resource, fixture.local(), task_id, revision);
    let release_path = format!(
        "/v1/resources/{}/operator-release",
        fixture.resource.as_uuid()
    );

    let (status, response) = send(
        fixture.dashboard,
        "POST",
        &release_path,
        &[
            ("host", fixture.dashboard.to_string()),
            ("origin", fixture.origin()),
            ("content-type", "application/json".into()),
        ],
        Some(operator_release_body(&attestation)),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{response}");
    assert_eq!(error_code(&response), "not_found");

    let (status, response) = fixture
        .socket_post(
            &format!(
                "/v1/resources/{}/operator-release",
                ResourceId::new().as_uuid()
            ),
            operator_release_body(&attestation),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert_eq!(error_code(&response), "usage");

    let wrong_authority = OperatorGpuFreeAttestation {
        authority_machine: MachineId::new(),
        ..attestation.clone()
    };
    let (status, response) = fixture
        .socket_post(
            &format!(
                "/v1/resources/{}/operator-release",
                fixture.resource.as_uuid()
            ),
            operator_release_body(&wrong_authority),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_action_not_allowed");

    let mut missing_confirmation = operator_release_body(&attestation);
    missing_confirmation["attestation"]
        .as_object_mut()
        .unwrap()
        .remove("confirmation");
    let (status, response) = fixture
        .socket_post(
            &format!(
                "/v1/resources/{}/operator-release",
                fixture.resource.as_uuid()
            ),
            missing_confirmation,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert_eq!(error_code(&response), "usage");

    // every state binding is strict input, including the field-free no_loan tag
    let loan_id = LoanId::new();
    let action_id = ActionId::new();
    for binding in [
        json!({ "type": "no_loan" }),
        json!({ "type": "awaiting_release", "loan_id": loan_id, "action_id": action_id }),
        json!({ "type": "first_background_launch", "request_id": RequestId::new() }),
        json!({ "type": "restoring_return", "loan_id": loan_id, "action_id": action_id }),
        json!({
            "type": "restoring_foreground_return",
            "loan_id": loan_id,
            "action_id": action_id,
        }),
    ] {
        let mut extra_field = operator_release_body(&attestation);
        extra_field["attestation"]["state_binding"] = binding;
        extra_field["attestation"]["state_binding"]["unexpected"] = json!(true);
        let (status, response) = fixture
            .socket_post(
                &format!(
                    "/v1/resources/{}/operator-release",
                    fixture.resource.as_uuid()
                ),
                extra_field,
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
        assert_eq!(error_code(&response), "usage");
    }

    let mut malformed_confirmation = operator_release_body(&attestation);
    malformed_confirmation["attestation"]["confirmation"] = json!("automatic_proof");
    let (status, response) = fixture
        .socket_post(
            &format!(
                "/v1/resources/{}/operator-release",
                fixture.resource.as_uuid()
            ),
            malformed_confirmation,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert_eq!(error_code(&response), "usage");

    for attestation in [attestation, wrong_authority] {
        let receipt = Store::open(&fixture.state.home.db_path())
            .unwrap()
            .operator_attestation_receipt_for_authority(fixture.local(), attestation.operation_id)
            .unwrap();
        assert!(receipt.is_none());
    }

    fixture.stop().await;
}

#[tokio::test]
async fn operator_release_refuses_a_running_trainer() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let (task_id, revision) = fixture.seed_registered_trainer(false).await;
    let attestation = operator_attestation(fixture.resource, fixture.local(), task_id, revision);
    let (status, response) = fixture
        .socket_post(
            &format!(
                "/v1/resources/{}/operator-release",
                fixture.resource.as_uuid()
            ),
            operator_release_body(&attestation),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_action_not_allowed");
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("has not ended")
    );
    fixture.stop().await;
}

#[tokio::test]
async fn operator_release_commits_replays_and_requests_resource_reconciliation() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let (task_id, revision) = fixture.seed_registered_trainer(true).await;
    let attestation = operator_attestation(fixture.resource, fixture.local(), task_id, revision);
    let (probe, probe_handle, mut requests) = spawn_reconcile_probe(false).await;
    let (socket, server) = fixture.socket_with_supervisor(probe.clone()).await;
    let path = format!(
        "/v1/resources/{}/operator-release",
        fixture.resource.as_uuid()
    );
    let headers = [
        ("host", socket.to_string()),
        ("content-type", "application/json".into()),
    ];

    let (status, first) = send(
        socket,
        "POST",
        &path,
        &headers,
        Some(operator_release_body(&attestation)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["api_version"], 1);
    assert_eq!(first["replayed"], false);
    assert_eq!(
        first["receipt"]["attestation"]["operation_id"],
        json!(attestation.operation_id)
    );
    assert_eq!(first["receipt"]["outcome"]["type"], "idle_boundary");

    let (status, retry) = send(
        socket,
        "POST",
        &path,
        &headers,
        Some(operator_release_body(&attestation)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{retry}");
    assert_eq!(retry["replayed"], true);
    assert_eq!(retry["receipt"], first["receipt"]);

    let changed = OperatorGpuFreeAttestation {
        observation: OperatorObservation::try_from("different observation".to_owned()).unwrap(),
        ..attestation.clone()
    };
    let (status, response) = send(
        socket,
        "POST",
        &path,
        &headers,
        Some(operator_release_body(&changed)),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_operation_conflict");

    let stale = OperatorGpuFreeAttestation {
        operation_id: OperatorAttestationId::new(),
        ..attestation.clone()
    };
    let (status, response) = send(
        socket,
        "POST",
        &path,
        &headers,
        Some(operator_release_body(&stale)),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_stale_revision");

    assert_eq!(requests.try_recv().unwrap(), fixture.resource);
    assert_eq!(requests.try_recv().unwrap(), fixture.resource);
    assert!(requests.try_recv().is_err());
    server.abort();
    probe.stop(None);
    let _ = probe_handle.await;
    fixture.stop().await;
}

#[tokio::test]
async fn operator_release_keeps_its_saved_receipt_when_reconciliation_is_uncertain() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let (task_id, revision) = fixture.seed_registered_trainer(true).await;
    let attestation = operator_attestation(fixture.resource, fixture.local(), task_id, revision);
    let (probe, probe_handle, mut requests) = spawn_reconcile_probe(true).await;
    let (socket, server) = fixture.socket_with_supervisor(probe.clone()).await;
    let path = format!(
        "/v1/resources/{}/operator-release",
        fixture.resource.as_uuid()
    );
    let headers = [
        ("host", socket.to_string()),
        ("content-type", "application/json".into()),
    ];
    let (status, response) = send(
        socket,
        "POST",
        &path,
        &headers,
        Some(operator_release_body(&attestation)),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{response}");
    assert_eq!(error_code(&response), "resource_outcome_unknown");
    assert_eq!(
        response["error"]["input"]["operation_id"],
        json!(attestation.operation_id.as_uuid())
    );

    let receipt = Store::open(&fixture.state.home.db_path())
        .unwrap()
        .operator_attestation_receipt_for_authority(fixture.local(), attestation.operation_id)
        .unwrap()
        .expect("the transaction receipt must remain saved after the failed wake");
    assert_eq!(receipt.attestation, attestation);
    assert_eq!(requests.try_recv().unwrap(), fixture.resource);
    assert!(requests.try_recv().is_err());

    server.abort();
    probe.stop(None);
    let _ = probe_handle.await;
    fixture.stop().await;
}

#[tokio::test]
async fn early_ended_first_launch_is_reserved_in_the_read_model_until_attested() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let (request_id, task_id) = fixture.seed_early_ended_launch().await;
    let reserved = |body: &Value| {
        body["loan"].is_null()
            && body["resource"]["registered_background_task"].is_null()
            && body["attention"]["code"] == "background_launch_release_unproven"
            && body["attention"]["task_id"] == json!(task_id)
            && body["background_launch"]
                == json!({
                    "request_id": request_id,
                    "task_id": task_id,
                    "status": "release_unproven",
                })
    };

    // no request is queued, and the durable state alone names the reservation
    let detail = fixture.detail().await;
    assert!(reserved(&detail), "{detail}");
    assert_eq!(detail["requests"], json!([]));
    let (status, list) = fixture.socket_get("/v1/resources").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let overview = list["resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["resource"]["id"] == json!(fixture.resource))
        .unwrap();
    assert!(reserved(overview), "{overview}");
    assert_eq!(overview["queued_count"], 0);

    // the view needs no actor snapshot, which a restarted daemon may not have yet
    let model = local_models(&fixture.state, Some(fixture.resource))
        .await
        .unwrap()
        .remove(0);
    let durable = attention(&model, None).unwrap();
    assert_eq!(durable.code, AttentionCode::BackgroundLaunchReleaseUnproven);
    assert_eq!(durable.task_id, Some(task_id));

    let revision = serde_json::from_value(detail["resource"]["state_revision"].clone()).unwrap();
    let release_path = format!(
        "/v1/resources/{}/operator-release",
        fixture.resource.as_uuid()
    );
    // the registered-trainer binding cannot release an unregistered launch task
    let registered = operator_attestation(fixture.resource, fixture.local(), task_id, revision);
    let (status, response) = fixture
        .socket_post(&release_path, operator_release_body(&registered))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_action_not_allowed");
    assert!(reserved(&fixture.detail().await));

    let attestation = OperatorGpuFreeAttestation {
        state_binding: OperatorStateBinding::FirstBackgroundLaunch { request_id },
        ..operator_attestation(fixture.resource, fixture.local(), task_id, revision)
    };
    let (status, receipt) = fixture
        .socket_post(&release_path, operator_release_body(&attestation))
        .await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert_eq!(receipt["replayed"], false);
    assert_eq!(receipt["receipt"]["outcome"]["type"], "idle_boundary");
    assert_eq!(
        receipt["receipt"]["evidence"]["trainer_launch"],
        json!({ "type": "first_background_launch", "request_id": request_id })
    );
    let (status, replay) = fixture
        .socket_post(&release_path, operator_release_body(&attestation))
        .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["receipt"], receipt["receipt"]);

    let released = fixture.detail().await;
    assert!(released["attention"].is_null(), "{released}");
    assert!(released.get("background_launch").is_none(), "{released}");
    fixture.stop().await;
}

#[tokio::test]
async fn dashboard_action_requires_exact_origin_json_and_host() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let path = format!("/v1/resources/{}/actions", fixture.resource.as_uuid());
    let body = json!({
        "api_version": 1,
        "expected_revision": 0,
        "operation_id": Uuid::now_v7(),
        "action": { "type": "stop_active", "task_id": TaskId::new() },
    });
    let host = fixture.dashboard.to_string();
    let json_type = ("content-type", "application/json".to_owned());

    let cases = [
        vec![("host", host.clone()), json_type.clone()],
        vec![
            ("host", host.clone()),
            ("origin", "http://evil.example".into()),
            json_type.clone(),
        ],
        vec![
            ("host", host.clone()),
            ("origin", format!("https://{host}")),
            json_type.clone(),
        ],
        vec![
            ("host", host.clone()),
            ("origin", fixture.origin()),
            ("content-type", "text/plain".into()),
        ],
        vec![
            ("host", host.clone()),
            ("origin", fixture.origin()),
            ("sec-fetch-site", "cross-site".into()),
            json_type.clone(),
        ],
    ];
    for headers in cases {
        let (status, response) = send(
            fixture.dashboard,
            "POST",
            &path,
            &headers,
            Some(body.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{headers:?} {response}");
        assert_eq!(error_code(&response), "permission");
    }

    let (status, response) = send(
        fixture.dashboard,
        "POST",
        &path,
        &[
            ("host", "attacker.example".into()),
            ("origin", "http://attacker.example".into()),
            json_type.clone(),
        ],
        Some(body.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert_eq!(response["error"]["message"], "unexpected Host header");

    // the exact dashboard origin reaches the authority, which refuses a task that is not active
    let (status, response) = fixture.browser_action(body).await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_action_not_allowed");
    assert_eq!(fixture.control_operation_count(), 0);

    // general task submit and the CLI-only resource writes stay off the dashboard
    for write in [
        "/v1/tasks".to_owned(),
        RESOURCE_REGISTER_PATH.to_owned(),
        format!("/v1/resources/{}/requests", fixture.resource.as_uuid()),
        format!("/v1/resources/{}/supervisor", fixture.resource.as_uuid()),
    ] {
        let (status, _) = send(
            fixture.dashboard,
            "POST",
            &write,
            &[
                ("host", host.clone()),
                ("origin", fixture.origin()),
                json_type.clone(),
            ],
            Some(json!({})),
        )
        .await;
        assert!(
            matches!(
                status,
                StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_FOUND
            ),
            "{write} returned {status}"
        );
    }
    fixture.stop().await;
}

#[tokio::test]
async fn queued_cancel_uses_the_origin_intent_and_retries_by_operation() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let (request_id, task_id) = fixture.submit_request().await;
    let detail = fixture.detail().await;
    assert_eq!(detail["requests"][0]["state"]["type"], "queued");
    assert_eq!(detail["requests"][0]["display_name"], "attention benchmark");
    // no registered background task, so the queue stays reserved rather than idle
    assert_eq!(detail["attention"]["code"], "queue_blocked");
    let revision = detail["resource"]["state_revision"].as_u64().unwrap();

    let stale = json!({
        "api_version": 1,
        "expected_revision": revision + 1,
        "operation_id": Uuid::now_v7(),
        "action": { "type": "cancel_queued", "request_id": request_id },
    });
    let (status, response) = fixture.browser_action(stale).await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_stale_revision");
    assert_eq!(response["error"]["input"]["current_revision"], revision);

    // the queued task is not the active command, so stop is refused
    let stop = json!({
        "api_version": 1,
        "expected_revision": revision,
        "operation_id": Uuid::now_v7(),
        "action": { "type": "stop_active", "task_id": task_id },
    });
    let (status, response) = fixture.browser_action(stop).await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_action_not_allowed");

    let operation_id = Uuid::now_v7();
    let cancel = json!({
        "api_version": 1,
        "expected_revision": revision,
        "operation_id": operation_id,
        "action": { "type": "cancel_queued", "request_id": request_id },
    });
    let (status, response) = fixture.browser_action(cancel.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["api_version"], 1);
    assert_eq!(
        response["requests"][0]["state"]["type"],
        "cancelled_before_launch"
    );
    let intent = call(&fixture.state.store, |reply| {
        StoreMsg::GetCancellationRequest {
            task: task_id,
            reply,
        }
    })
    .await
    .unwrap();
    assert!(intent.is_some(), "the origin must own the cancellation");

    // an exact retry replays without a revision check; changed content conflicts
    let (status, response) = fixture.browser_action(cancel).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let changed = json!({
        "api_version": 1,
        "expected_revision": revision,
        "operation_id": operation_id,
        "action": { "type": "stop_active", "task_id": task_id },
    });
    let (status, response) = fixture.browser_action(changed).await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_operation_conflict");
    assert_eq!(fixture.control_operation_count(), 1);

    // the CLI cancel route uses the same operation identity rules
    let (status, response) = fixture
        .socket_post(
            &format!(
                "/v1/resources/{}/requests/{}/cancel",
                fixture.resource.as_uuid(),
                request_id.0
            ),
            json!({ "api_version": 1, "operation_id": operation_id, "expected_revision": revision }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    fixture.stop().await;
}

#[tokio::test]
async fn queued_move_action_route_reorders_and_replays_without_an_external_effect() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let (first_request, _) = fixture.submit_request().await;
    let (second_request, _) = fixture.submit_request().await;
    let detail = fixture.detail().await;
    let revision = detail["resource"]["state_revision"].as_u64().unwrap();
    let operation_id = Uuid::now_v7();
    let move_action = json!({
        "api_version": 1,
        "expected_revision": revision,
        "operation_id": operation_id,
        "action": {
            "type": "move_queued",
            "request_id": second_request,
            "placement": { "type": "front" },
        },
    });

    let (status, response) = fixture.browser_action(move_action.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(
        response["requests"][0]["request_id"],
        second_request.0.to_string()
    );
    assert_eq!(
        response["requests"][1]["request_id"],
        first_request.0.to_string()
    );
    assert_eq!(response["resource"]["state_revision"], revision + 1);

    let (status, replayed) = fixture.browser_action(move_action).await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(
        replayed["requests"][0]["request_id"],
        second_request.0.to_string()
    );
    assert_eq!(replayed["resource"]["state_revision"], revision + 1);

    let changed = json!({
        "api_version": 1,
        "expected_revision": revision,
        "operation_id": operation_id,
        "action": {
            "type": "move_queued",
            "request_id": second_request,
            "placement": { "type": "back" },
        },
    });
    let (status, response) = fixture.browser_action(changed).await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_operation_conflict");
    assert_eq!(fixture.control_operation_count(), 1);
    fixture.stop().await;
}

#[tokio::test]
async fn queued_cancel_race_reports_a_definite_activated_request_on_retry() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let (request_id, task_id) = fixture.submit_request().await;
    let revision = fixture.detail().await["resource"]["state_revision"]
        .as_u64()
        .unwrap();
    let operation_id = Uuid::now_v7();
    let control = ResourceControlRequest {
        resource_id: fixture.resource,
        expected_revision: ResourceRevision::new(revision),
        action: BrowserResourceAction::CancelQueued { request_id },
    };
    call(&fixture.state.store, |reply| {
        StoreMsg::BeginResourceControl {
            authority_machine: fixture.local(),
            operation_id,
            request: Box::new(control),
            attempt_id: DeliveryAttemptId::new(),
            reply,
        }
    })
    .await
    .unwrap();

    // activation can win after the control receipt commits but before its origin intent arrives
    crate::store::mark_request_assigned_for_race(&fixture.state.home.db_path(), request_id);

    let cancel = json!({
        "api_version": 1,
        "expected_revision": revision,
        "operation_id": operation_id,
        "action": { "type": "cancel_queued", "request_id": request_id },
    });
    for _ in 0..2 {
        let (status, response) = fixture.browser_action(cancel.clone()).await;
        assert_eq!(status, StatusCode::CONFLICT, "{response}");
        assert_eq!(error_code(&response), "resource_action_not_allowed");
    }
    let intent = call(&fixture.state.store, |reply| {
        StoreMsg::GetCancellationRequest {
            task: task_id,
            reply,
        }
    })
    .await
    .unwrap();
    assert!(intent.is_none());
    assert_eq!(fixture.control_operation_count(), 1);
    fixture.stop().await;
}

#[tokio::test]
async fn renotify_reserves_one_explicit_attempt_without_resetting_the_budget() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    // an unreachable destination fails the attempt without starting Codex
    let destination = SupervisorAddress {
        machine: MachineId::new(),
        thread: THREAD.parse().unwrap(),
    };
    let notice = fixture.seed_failed_return_notice(destination);

    let (status, pending) = fixture
        .socket_get(&format!(
            "{RESOURCE_PENDING_PATH}?machine={}&thread={THREAD}",
            fixture.local()
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{pending}");
    let action = &pending["actions"][0];
    assert_eq!(action["action_id"], json!(notice.action_id));
    assert_eq!(action["phase"]["type"], "return_required");
    assert_eq!(action["return_context"]["type"], "idle");
    assert_eq!(action["notice"]["delivery"]["type"], "failed");
    let (_, list) = fixture.socket_get("/v1/resources").await;
    assert_eq!(
        list["resources"][0]["attention"]["code"],
        "notice_delivery_failed"
    );
    assert_eq!(list["unavailable_authorities"], json!([]));

    let operation_id = Uuid::now_v7();
    let renotify = json!({
        "api_version": 1,
        "expected_revision": 0,
        "operation_id": operation_id,
        "action": { "type": "renotify", "notice_id": notice.id },
    });
    let (status, response) = fixture.browser_action(renotify.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let delivery = &response["notices"][0]["delivery"];
    assert_eq!(delivery["type"], "failed");
    assert_eq!(delivery["attempts"], 4);
    assert!(
        delivery["last_error"]
            .as_str()
            .unwrap()
            .contains("Fleet is disabled"),
        "{delivery}"
    );

    // a replay reports the settled attempt and never sends another one
    let (status, response) = fixture.browser_action(renotify).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["notices"][0]["delivery"]["attempts"], 4);

    // a new explicit operation is one more attempt, not a reset budget
    let again = json!({
        "api_version": 1,
        "expected_revision": 0,
        "operation_id": Uuid::now_v7(),
        "action": { "type": "renotify", "notice_id": notice.id },
    });
    let (status, response) = fixture.browser_action(again).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["notices"][0]["delivery"]["attempts"], 5);

    let unknown_notice = json!({
        "api_version": 1,
        "expected_revision": 0,
        "operation_id": Uuid::now_v7(),
        "action": { "type": "renotify", "notice_id": NoticeId::new() },
    });
    let (status, response) = fixture.browser_action(unknown_notice).await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_action_not_allowed");
    fixture.stop().await;
}

#[tokio::test]
async fn supervisor_replacement_retargets_undelivered_notices_and_checks_revision() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let notice = fixture.seed_failed_return_notice(SupervisorAddress {
        machine: fixture.local(),
        thread: THREAD.parse().unwrap(),
    });
    let replacement = SupervisorAddress {
        machine: fixture.local(),
        thread: ThreadId(Uuid::now_v7()),
    };
    let path = format!("/v1/resources/{}/supervisor", fixture.resource.as_uuid());
    let body = json!({ "api_version": 1, "expected_revision": 0, "supervisor": replacement });

    let (status, response) = fixture.socket_post(&path, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["resource"]["supervisor"], json!(replacement));
    assert_eq!(response["resource"]["assignment_revision"], 1);
    assert_eq!(response["notices"][0]["id"], json!(notice.id));
    assert_eq!(response["notices"][0]["destination"], json!(replacement));
    assert_eq!(response["notices"][0]["assignment_revision"], 1);

    // an exact retry after a lost response finds the saved assignment
    let (status, response) = fixture.socket_post(&path, body).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["resource"]["assignment_revision"], 1);

    let stale = json!({
        "api_version": 1,
        "expected_revision": 9,
        "supervisor": { "machine": fixture.local(), "thread": Uuid::now_v7() },
    });
    let (status, response) = fixture.socket_post(&path, stale).await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_stale_revision");
    fixture.stop().await;
}

#[tokio::test]
async fn registration_retry_uses_the_first_supervisor_after_reassignment() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let replacement = SupervisorAddress {
        machine: fixture.local(),
        thread: ThreadId(Uuid::now_v7()),
    };
    let path = format!("/v1/resources/{}/supervisor", fixture.resource.as_uuid());
    let (status, response) = fixture
        .socket_post(
            &path,
            json!({ "api_version": 1, "expected_revision": 0, "supervisor": replacement }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");

    let original = json!({
        "api_version": 1,
        "spec": {
            "id": fixture.resource,
            "display_name": "RTX 5090",
            "supervisor": { "machine": fixture.local(), "thread": THREAD },
        }
    });
    let (status, response) = fixture
        .socket_post(RESOURCE_REGISTER_PATH, original.clone())
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["resource"]["supervisor"], json!(replacement));

    let mut changed = original;
    changed["spec"]["supervisor"] = json!(replacement);
    let (status, response) = fixture.socket_post(RESOURCE_REGISTER_PATH, changed).await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_operation_conflict");
    fixture.stop().await;
}

#[tokio::test]
async fn reads_are_read_only_and_unowned_writes_fail_closed() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    fixture.submit_request().await;
    let before = fixture.detail().await;
    let dashboard_headers = [("host", fixture.dashboard.to_string())];
    for path in [
        "/v1/resources".to_owned(),
        format!("/v1/resources/{}", fixture.resource.as_uuid()),
        format!(
            "{RESOURCE_PENDING_PATH}?machine={}&thread={THREAD}",
            fixture.local()
        ),
    ] {
        let (status, body) = send(fixture.dashboard, "GET", &path, &dashboard_headers, None).await;
        assert_eq!(status, StatusCode::OK, "{path} {body}");
    }
    let (status, _) = send(
        fixture.dashboard,
        "GET",
        &format!("/v1/resources/{}/actions", fixture.resource.as_uuid()),
        &dashboard_headers,
        None,
    )
    .await;
    assert!(status.is_client_error(), "{status}");
    let after = fixture.detail().await;
    assert_eq!(before["resource"], after["resource"]);
    assert_eq!(before["requests"], after["requests"]);
    assert_eq!(fixture.control_operation_count(), 0);

    let (status, response) = fixture
        .socket_get(&format!("/v1/resources/{}", ResourceId::new().as_uuid()))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{response}");
    assert_eq!(error_code(&response), "resource_not_found");

    let (status, response) = fixture
        .socket_post(
            &format!("/v1/resources/{}/background", fixture.resource.as_uuid()),
            json!({
                "api_version": 1,
                "request_id": RequestId::new(),
                "spec": {
                    "api_version": 1,
                    "thread": THREAD,
                    "name": "trainer",
                    "cwd": "/tmp",
                    "timeout": "4h",
                    "workload": { "type": "task", "command": ["/bin/echo", "train"] }
                },
                "env": { "path": fixture.bin, "home": "/tmp" },
                "callback_cwd": fixture.callback_cwd,
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_operation_unavailable");
    let (_, tasks) = fixture.socket_get("/v1/tasks").await;
    assert_eq!(
        tasks["tasks"],
        json!([]),
        "a command without a verifiable ownership contract must not start a task"
    );

    let (status, response) = fixture
        .socket_post(
            RESOURCE_REGISTER_PATH,
            json!({
                "api_version": 1,
                "spec": {
                    "id": fixture.resource,
                    "display_name": "another GPU",
                    "supervisor": { "machine": fixture.local(), "thread": THREAD },
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_operation_conflict");
    fixture.stop().await;
}

fn response(status: StatusCode, body: &Value) -> ClusterResponse {
    ClusterResponse {
        status,
        body: Bytes::from(serde_json::to_vec(body).unwrap()),
    }
}

#[test]
fn forwarded_control_keeps_typed_refusals_and_reports_unknown_outcomes() {
    let resource = ResourceId::new();
    let authority = MachineId::new();
    let operation = Some(Uuid::now_v7());

    let stale = AppError::ResourceStaleRevision {
        resource,
        expected: 3,
        current: 4,
    };
    let decoded = decode_control_response(
        resource,
        authority,
        operation,
        &response(stale.http_status(), &stale.to_json()),
    );
    assert!(matches!(
        decoded,
        Err(AppError::ResourceStaleRevision {
            expected: 3,
            current: 4,
            ..
        })
    ));

    let conflict = AppError::ResourceOperationConflict {
        resource,
        operation,
        message: "changed".into(),
    };
    let decoded = decode_control_response(
        resource,
        authority,
        operation,
        &response(conflict.http_status(), &conflict.to_json()),
    );
    assert!(matches!(
        decoded,
        Err(AppError::ResourceOperationConflict { .. })
    ));

    // a server failure or an unreadable success may follow a committed mutation
    let internal = AppError::Internal {
        message: "store".into(),
    };
    for unknown in [
        response(internal.http_status(), &internal.to_json()),
        response(StatusCode::OK, &json!({ "api_version": 1 })),
        response(StatusCode::BAD_GATEWAY, &json!("proxy")),
    ] {
        let decoded = decode_control_response(resource, authority, operation, &unknown);
        assert!(
            matches!(decoded, Err(AppError::ResourceOutcomeUnknown { operation: found, .. }) if found == operation),
            "{decoded:?}"
        );
    }
}

#[tokio::test]
async fn background_route_launches_one_co_located_trainer_and_refuses_remote_supervisors() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    crate::runner::set_task_run_executable_for_tests(assert_cmd::cargo::cargo_bin("homebased"));
    let fixture = Fixture::new().await;
    let root = fixture.callback_cwd.canonicalize().unwrap();
    let trainer = FakeTrainer::new(&root);
    let body = |request_id: RequestId| {
        json!({
            "api_version": 1,
            "request_id": request_id,
            "spec": trainer.spec(THREAD.parse().unwrap(), "direct segment trainer"),
            "env": trainer.env,
            "callback_cwd": root,
        })
    };
    let path = format!("/v1/resources/{}/background", fixture.resource.as_uuid());
    let request_id = RequestId::new();

    let (status, first) = fixture.socket_post(&path, body(request_id)).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["outcome"]["type"], "inserted");
    assert_eq!(first["resource"]["id"], json!(fixture.resource));
    // the bound launch is not registered until its confirmed start
    assert_eq!(first["resource"]["registered_background_task"], Value::Null);
    let (status, retry) = fixture.socket_post(&path, body(request_id)).await;
    assert_eq!(status, StatusCode::OK, "{retry}");
    assert_eq!(retry["outcome"]["type"], "existing");
    assert_eq!(retry["task_id"], first["task_id"]);

    // the trainer holds no attempt lock here, so no association can be asserted for it
    let (status, response) = fixture
        .socket_post(
            &format!(
                "{path}/{}/trainer-attempt",
                first["task_id"].as_str().unwrap()
            ),
            json!({
                "api_version": 1,
                "attempt_binding": crate::resource::ownership_lock::test_support::attempt_binding(
                    "attempt-1"
                ),
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_action_not_allowed");

    // a remote supervisor thread would need a saved origin route on its own machine
    let remote = ResourceId::new();
    let (status, response) = fixture
        .socket_post(
            RESOURCE_REGISTER_PATH,
            json!({
                "api_version": 1,
                "spec": {
                    "id": remote,
                    "display_name": "remote-supervised GPU",
                    "supervisor": { "machine": MachineId::new(), "thread": THREAD },
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let (status, response) = fixture
        .socket_post(
            &format!("/v1/resources/{}/background", remote.as_uuid()),
            body(RequestId::new()),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_operation_unavailable");
    let (_, tasks) = fixture.socket_get("/v1/tasks").await;
    assert_eq!(tasks["tasks"].as_array().map(Vec::len), Some(1), "{tasks}");

    std::fs::write(&trainer.gate, b"").unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn initial_idle_saves_one_receipt_for_a_resource_with_no_history() {
    let _serial = SUPERVISOR_TEST_LOCK.lock().await;
    let fixture = Fixture::new().await;
    let (status, detail) = fixture
        .socket_get(&format!("/v1/resources/{}", fixture.resource.as_uuid()))
        .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let revision = detail["resource"]["state_revision"].clone();
    let attestation = json!({
        "operation_id": OperatorAttestationId::new(),
        "resource_id": fixture.resource,
        "authority_machine": fixture.local(),
        "expected_state_revision": revision,
        "observation": "nvidia-smi on the authority shows no compute processes",
        "confirmation": "operator_confirmed_gpu_free"
    });
    let body = json!({ "api_version": 1, "attestation": attestation });
    let path = format!("/v1/resources/{}/initial-idle", fixture.resource.as_uuid());

    // the browser dashboard cannot record a human attestation
    let (status, response) = send(
        fixture.dashboard,
        "POST",
        &path,
        &[
            ("host", fixture.dashboard.to_string()),
            ("origin", fixture.origin()),
            ("content-type", "application/json".into()),
        ],
        Some(body.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{response}");

    let (status, first) = fixture.socket_post(&path, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["replayed"], false);
    assert_eq!(first["receipt"]["attestation"], attestation);
    let (status, retry) = fixture.socket_post(&path, body).await;
    assert_eq!(status, StatusCode::OK, "{retry}");
    assert_eq!(retry["replayed"], true);
    assert_eq!(retry["receipt"], first["receipt"]);

    let mut second = attestation.clone();
    second["operation_id"] = json!(OperatorAttestationId::new());
    second["expected_state_revision"] = first["receipt"]["state_revision"].clone();
    let (status, response) = fixture
        .socket_post(&path, json!({ "api_version": 1, "attestation": second }))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    assert_eq!(error_code(&response), "resource_action_not_allowed");
    fixture.stop().await;
}
