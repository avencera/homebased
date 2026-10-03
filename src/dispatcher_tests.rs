//! Origin inbox dispatcher tests with a fake saved Codex executable

use std::io::Read;
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use ractor::Actor;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::daemon::actors::callback::{
    CallbackActor, CallbackArgs, CallbackMsg, WakeFailure, dispatch_inbox,
};
use crate::daemon::actors::{StoreActor, StoreMsg, call};
use crate::domain::{ProcessStatus, TaskEnv, TaskId};
use crate::events::{DeliveryState, EventPayload, TaskEvent};
use crate::home::Home;
use crate::machine::MachineId;
use crate::store::Store;
use crate::submission::{CallbackContext, OriginRoute, RequestId, SubmissionState};
use crate::t3::test_support::{FakeResponse, FakeT3Server, V2State, rpc_exit, write_runtime};

const T3_THREAD_ID: &str = "31c5fd73-3cc4-4ecb-a1cd-8f01c39fcb85";

fn t3_snapshot() -> serde_json::Value {
    serde_json::json!({
        "snapshotSequence": 9,
        "thread": {
            "id": T3_THREAD_ID,
            "title": "Fake thread",
            "runtimeMode": "full-access",
            "interactionMode": "default",
            "archivedAt": null,
            "deletedAt": null
        },
        "page": {}
    })
}

/// T3 user data with a runtime file and a fake `t3` CLI, but no state database
fn configure_t3(route: &mut OriginRoute, origin: &str, pid: i32) -> PathBuf {
    let callback_home = PathBuf::from(&route.callback.env.home);
    let userdata = callback_home.join(".t3/userdata");
    std::fs::create_dir_all(&userdata).unwrap();
    write_runtime(&userdata, origin, pid);

    let bin = callback_home.join("t3-bin");
    std::fs::create_dir_all(&bin).unwrap();
    let t3 = bin.join("t3");
    std::fs::write(
        &t3,
        "#!/bin/sh\nif [ \"$1 $2 $3\" = 'auth session issue' ]; then printf '%s\\n' '{\"sessionId\":\"fake-session\",\"token\":\"fake-token\",\"method\":\"bearer-access-token\",\"scopes\":[]}'; exit 0; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&t3, std::fs::Permissions::from_mode(0o700)).unwrap();
    route.callback.env.path = bin.to_string_lossy().into_owned();
    userdata
}

fn configure_t3_codex(route: &mut OriginRoute, origin: &str, pid: i32) -> PathBuf {
    let userdata = configure_t3(route, origin, pid);
    let db = rusqlite::Connection::open(userdata.join("state.sqlite")).unwrap();
    db.execute_batch(
        "CREATE TABLE provider_session_runtime (
            thread_id TEXT PRIMARY KEY,
            provider_name TEXT,
            last_seen_at TEXT,
            resume_cursor_json TEXT
         );
         CREATE TABLE projection_threads (
            thread_id TEXT PRIMARY KEY,
            deleted_at TEXT
         );",
    )
    .unwrap();
    db.execute(
        "INSERT INTO projection_threads (thread_id) VALUES (?1)",
        [T3_THREAD_ID],
    )
    .unwrap();
    db.execute(
        "INSERT INTO provider_session_runtime
         (thread_id, provider_name, last_seen_at, resume_cursor_json)
         VALUES (?1, 'codex', '2026-09-26T17:00:00Z', ?2)",
        rusqlite::params![
            T3_THREAD_ID,
            serde_json::json!({"threadId": route.thread.to_string()}).to_string()
        ],
    )
    .unwrap();
    PathBuf::from(&route.callback.env.home).join("commands")
}

/// Register a live Claude session for `route.thread` and return its socket
fn live_claude_session(route: &OriginRoute) -> UnixListener {
    let callback_home = PathBuf::from(&route.callback.env.home);
    let socket = callback_home.join("inbox.sock");
    register_live_claude_session(&callback_home, &route.thread.to_string(), &socket);
    let listener = UnixListener::bind(&socket).unwrap();
    listener.set_nonblocking(true).unwrap();
    listener
}

/// Messages a live session socket received, without blocking
fn socket_messages(listener: &UnixListener) -> Vec<String> {
    let mut messages = Vec::new();
    while let Ok((mut stream, _)) = listener.accept() {
        stream.set_nonblocking(false).unwrap();
        let mut body = String::new();
        stream.read_to_string(&mut body).unwrap();
        messages.push(body);
    }
    messages
}

fn fixture(script: &str) -> (TempDir, Home, OriginRoute) {
    let dir = tempfile::tempdir().unwrap();
    let home = Home::resolve(Some(dir.path().join("state"))).unwrap();
    home.ensure().unwrap();
    let callback_home = dir.path().join("saved-home");
    let callback_cwd = dir.path().join("saved-cwd");
    std::fs::create_dir(&callback_home).unwrap();
    std::fs::create_dir(&callback_cwd).unwrap();
    let codex = dir.path().join("fake-codex");
    std::fs::write(&codex, script).unwrap();
    std::fs::set_permissions(&codex, std::fs::Permissions::from_mode(0o700)).unwrap();
    let spec: crate::spec::NormalizedSpec = serde_json::from_value(serde_json::json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "origin inbox",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": { "type": "task", "command": ["echo", "remote"] }
    }))
    .unwrap();
    let route = OriginRoute {
        request: RequestId::new(),
        task: TaskId::new(),
        origin_machine: MachineId::new(),
        execution_machine: MachineId::new(),
        thread: spec.thread,
        callback: CallbackContext {
            env: TaskEnv {
                path: "saved-path".into(),
                home: callback_home.to_string_lossy().into_owned(),
            },
            cwd: callback_cwd,
            codex: codex.into(),
        },
        spec,
        submission: SubmissionState::AcceptanceUnknown,
        last_execution_state: None,
        last_updated_at: chrono::Utc::now(),
        last_accepted_seq: 0,
        last_settled_seq: 0,
    };
    (dir, home, route)
}

fn event(route: &OriginRoute, seq: u64, callback: bool) -> TaskEvent {
    let payload = if callback {
        EventPayload::Callback {
            event: Box::new(serde_json::from_value(serde_json::json!({
                "api_version": 1, "event": "TASK_REPORTED", "task": route.task,
                "name": "origin inbox", "workload": { "type": "task", "command": ["echo", "remote"] },
                "thread": route.thread, "cwd": "/tmp", "evidence": "/tmp/evidence",
                "reports": [], "process": null, "next_action": "read_report"
            })).unwrap()),
            state: None,
        }
    } else {
        EventPayload::State {
            status: ProcessStatus::Running,
        }
    };
    TaskEvent {
        task: route.task,
        seq: NonZeroU64::new(seq).unwrap(),
        origin_machine: route.origin_machine,
        execution_machine: route.execution_machine,
        payload,
    }
}

async fn dispatch(home: &Home, id: TaskId) {
    let (store, handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let (callback, callback_handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home: home.clone(),
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: None,
        },
    )
    .await
    .unwrap();
    dispatch_inbox(store.clone(), home.clone(), id, callback.clone())
        .await
        .unwrap();
    callback.stop(None);
    callback_handle.await.unwrap();
    store.stop(None);
    handle.await.unwrap();
}

fn lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

#[tokio::test]
async fn ordered_callbacks_use_saved_origin_and_skip_state_only() {
    let (_dir, home, route) = fixture(
        "#!/bin/sh\nprintf '%s|%s|%s|%s\\n' \"$(pwd -P)\" \"$PATH\" \"$HOME\" \"$*\" >> \"$HOME/commands\"\n",
    );
    let mut store = Store::open(&home.db_path()).unwrap();
    store.insert_origin_route(&route).unwrap();
    for (seq, callback) in [(1, true), (2, false), (3, true)] {
        store
            .accept_inbound_event(&event(&route, seq, callback))
            .unwrap();
    }
    drop(store);
    dispatch(&home, route.task).await;
    dispatch(&home, route.task).await;
    let commands = lines(&PathBuf::from(&route.callback.env.home).join("commands"));
    assert_eq!(commands.len(), 2);
    assert!(commands[0].contains("\"seq\":1"));
    assert!(commands[1].contains("\"seq\":3"));
    for line in commands {
        assert!(
            line.starts_with(&format!(
                "{}|saved-path|{}|queue --thread {} --message HOMEBASED_EVENT ",
                route.callback.cwd.canonicalize().unwrap().display(),
                route.callback.env.home,
                route.thread
            )),
            "{line}"
        );
        assert!(line.contains(&route.task.to_string()));
    }
    let store = Store::open(&home.db_path()).unwrap();
    assert_eq!(
        store
            .origin_route_by_task(route.task)
            .unwrap()
            .unwrap()
            .last_settled_seq,
        3
    );
    assert_eq!(
        store.inbound_events(route.task).unwrap()[1].delivery,
        DeliveryState::NotRequired
    );
}

#[tokio::test]
async fn t3_owned_codex_callback_uses_t3_without_codex_queue() {
    let (_dir, home, mut route) =
        fixture("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/commands\"\nexit 0\n");
    let server = FakeT3Server::start(
        Some(1),
        vec![
            FakeResponse::http(200, &t3_snapshot()),
            FakeResponse::http(200, &serde_json::json!({ "sequence": 10 })),
        ],
    );
    let commands = configure_t3_codex(
        &mut route,
        &server.origin,
        i32::try_from(std::process::id()).unwrap(),
    );
    let mut persisted = Store::open(&home.db_path()).unwrap();
    persisted.insert_origin_route(&route).unwrap();
    persisted
        .accept_inbound_event(&event(&route, 1, true))
        .unwrap();
    drop(persisted);

    let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let (callback, callback_handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home: home.clone(),
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: None,
        },
    )
    .await
    .unwrap();
    assert!(
        dispatch_inbox(store.clone(), home.clone(), route.task, callback.clone())
            .await
            .unwrap()
    );

    assert!(!commands.exists());
    assert_eq!(
        Store::open(&home.db_path())
            .unwrap()
            .inbound_events(route.task)
            .unwrap()[0]
            .delivery,
        DeliveryState::Delivered {
            attempts: 1,
            last_error: None
        }
    );
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].path.starts_with("/api/orchestration/threads/"));
    assert!(requests[1].body.contains("\"type\":\"thread.turn.start\""));
    assert!(requests[1].body.contains(&route.task.to_string()));
    assert!(
        !call(&callback, |reply| CallbackMsg::InspectAlert {
            thread: route.thread,
            reply
        })
        .await
        .unwrap()
    );

    callback.stop(None);
    callback_handle.await.unwrap();
    store.stop(None);
    store_handle.await.unwrap();
}

/// What one inbox run did with a callback for a T3 V2 Claude session
struct T3ClaudeDelivery {
    /// Messages the live session socket received
    socket: Vec<String>,
    delivery: DeliveryState,
    /// Whether the inbox drained instead of waiting
    completed: bool,
    /// Whether a T3 send that may have been taken is still recorded
    pending: bool,
}

/// Deliver one callback to a live Claude session that a T3 V2 thread owns
async fn deliver_to_t3_v2_claude(origin: &str, archived: bool) -> T3ClaudeDelivery {
    let (_dir, home, mut route) = fixture("#!/bin/sh\nexit 0\n");
    let userdata = configure_t3(
        &mut route,
        origin,
        i32::try_from(std::process::id()).unwrap(),
    );
    V2State::create(&userdata.join("statev2.sqlite"))
        .thread(T3_THREAD_ID, "V2 thread", archived)
        .native("claudeAgent", &route.thread.to_string(), T3_THREAD_ID);
    let socket = live_claude_session(&route);
    let mut persisted = Store::open(&home.db_path()).unwrap();
    persisted.insert_origin_route(&route).unwrap();
    persisted
        .accept_inbound_event(&event(&route, 1, true))
        .unwrap();
    drop(persisted);

    let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let (callback, callback_handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home: home.clone(),
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: None,
        },
    )
    .await
    .unwrap();
    let completed = dispatch_inbox(store.clone(), home.clone(), route.task, callback.clone())
        .await
        .unwrap();
    let delivery = Store::open(&home.db_path())
        .unwrap()
        .inbound_events(route.task)
        .unwrap()[0]
        .delivery
        .clone();
    let pending = home
        .task_paths(route.task)
        .dir
        .join("t3-pending-1")
        .exists();

    callback.stop(None);
    callback_handle.await.unwrap();
    store.stop(None);
    store_handle.await.unwrap();
    T3ClaudeDelivery {
        socket: socket_messages(&socket),
        delivery,
        completed,
        pending,
    }
}

#[tokio::test]
async fn live_claude_session_owned_by_t3_v2_gets_the_event_through_t3() {
    let server = FakeT3Server::start(
        Some(2),
        vec![
            FakeResponse::http(200, &serde_json::json!({ "ticket": "fake-ticket" })),
            FakeResponse::WebSocket(vec![rpc_exit(
                serde_json::json!({ "_tag": "Success", "value": { "sequence": 3 } }),
            )]),
        ],
    );

    let result = deliver_to_t3_v2_claude(&server.origin, false).await;

    assert!(result.socket.is_empty(), "{:?}", result.socket);
    assert!(matches!(
        result.delivery,
        DeliveryState::Delivered { attempts: 1, .. }
    ));
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    let rpc: serde_json::Value = serde_json::from_str(&requests[1].body).unwrap();
    assert_eq!(rpc["payload"]["type"], "message.dispatch");
    assert_eq!(rpc["payload"]["threadId"], T3_THREAD_ID);
    assert!(
        rpc["payload"]["text"]
            .as_str()
            .unwrap()
            .contains("HOMEBASED_EVENT")
    );
}

#[tokio::test]
async fn live_claude_session_falls_back_to_its_socket_when_t3_is_unreachable() {
    let result = deliver_to_t3_v2_claude("http://127.0.0.1:1", false).await;

    assert_eq!(result.socket.len(), 1);
    assert!(result.socket[0].contains("HOMEBASED_EVENT"));
    assert!(matches!(
        result.delivery,
        DeliveryState::Delivered { attempts: 1, .. }
    ));
}

#[tokio::test]
async fn lost_t3_reply_retries_through_t3_instead_of_the_socket() {
    let ticket = || FakeResponse::http(200, &serde_json::json!({ "ticket": "fake-ticket" }));
    let server = FakeT3Server::start(
        Some(2),
        vec![
            ticket(),
            FakeResponse::WebSocket(Vec::new()),
            ticket(),
            FakeResponse::WebSocket(vec![rpc_exit(
                serde_json::json!({ "_tag": "Success", "value": { "sequence": 3 } }),
            )]),
        ],
    );

    let result = deliver_to_t3_v2_claude(&server.origin, false).await;

    assert!(result.socket.is_empty(), "{:?}", result.socket);
    assert!(
        matches!(
            result.delivery,
            DeliveryState::Delivered { attempts: 2, .. }
        ),
        "{:?}",
        result.delivery
    );
    assert!(!result.pending);
    let dispatches = server
        .requests()
        .into_iter()
        .filter(|request| request.method == "WS")
        .map(|request| serde_json::from_str::<serde_json::Value>(&request.body).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(dispatches.len(), 2);
    assert_eq!(
        dispatches[0]["payload"]["commandId"],
        dispatches[1]["payload"]["commandId"]
    );
}

#[tokio::test]
async fn archived_t3_thread_is_unarchived_and_gets_the_event_through_t3() {
    let ticket = || FakeResponse::http(200, &serde_json::json!({ "ticket": "fake-ticket" }));
    let success = || {
        FakeResponse::WebSocket(vec![rpc_exit(
            serde_json::json!({ "_tag": "Success", "value": {} }),
        )])
    };
    let server = FakeT3Server::start(Some(2), vec![ticket(), success(), ticket(), success()]);

    let result = deliver_to_t3_v2_claude(&server.origin, true).await;

    assert!(result.socket.is_empty(), "{:?}", result.socket);
    assert!(matches!(result.delivery, DeliveryState::Delivered { .. }));
    let types = server
        .requests()
        .into_iter()
        .filter(|request| request.method == "WS")
        .map(|request| {
            serde_json::from_str::<serde_json::Value>(&request.body).unwrap()["payload"]["type"]
                .clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(types, ["thread.unarchive", "message.dispatch"]);
}

#[tokio::test]
async fn uncertain_t3_send_waits_for_t3_instead_of_using_the_socket() {
    // the first reply is lost, and then T3 stops answering tickets
    let server = FakeT3Server::start(
        Some(2),
        vec![
            FakeResponse::http(200, &serde_json::json!({ "ticket": "fake-ticket" })),
            FakeResponse::WebSocket(Vec::new()),
        ],
    );

    let result = deliver_to_t3_v2_claude(&server.origin, false).await;

    assert!(result.socket.is_empty(), "{:?}", result.socket);
    assert!(!result.completed);
    assert!(
        matches!(result.delivery, DeliveryState::AwaitingThread { .. }),
        "{:?}",
        result.delivery
    );
    assert!(result.pending);
}

#[tokio::test]
async fn unavailable_t3_codex_wake_falls_back_and_marks_delivery() {
    let (_dir, home, mut route) =
        fixture("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/commands\"\nexit 0\n");
    let commands = configure_t3_codex(&mut route, "http://127.0.0.1:1", -1);
    let mut persisted = Store::open(&home.db_path()).unwrap();
    persisted.insert_origin_route(&route).unwrap();
    persisted
        .accept_inbound_event(&event(&route, 1, true))
        .unwrap();
    drop(persisted);

    let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let (callback, callback_handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home: home.clone(),
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: None,
        },
    )
    .await
    .unwrap();
    assert!(
        dispatch_inbox(store.clone(), home.clone(), route.task, callback.clone())
            .await
            .unwrap()
    );

    assert_eq!(lines(&commands).len(), 1);
    assert!(matches!(
        Store::open(&home.db_path())
            .unwrap()
            .inbound_events(route.task)
            .unwrap()[0]
            .delivery,
        DeliveryState::Delivered { attempts: 1, .. }
    ));
    assert!(
        call(&callback, |reply| CallbackMsg::InspectAlert {
            thread: route.thread,
            reply
        })
        .await
        .unwrap()
    );

    callback.stop(None);
    callback_handle.await.unwrap();
    store.stop(None);
    store_handle.await.unwrap();
}

#[tokio::test]
async fn local_terminal_event_delivers_once_after_restart() {
    use crate::domain::{ExitReason, ReportOutcome, TaskWorkload, Workload};
    use crate::invocation::CommandLine;
    use crate::store::{NewTask, new_queued_task};

    let (_dir, home, mut route) =
        fixture("#!/bin/sh\nprintf 'callback\\n' >> \"$HOME/commands\"\n");
    let spec = &mut route.spec;
    spec.cwd = route.callback.cwd.clone();
    let machine = MachineId::new();
    let row = new_queued_task(NewTask {
        id: route.task,
        name: spec.name.clone(),
        thread: route.thread,
        workload: Workload::Task(TaskWorkload {
            command: CommandLine::try_from_argv(vec!["echo".into(), "local".into()]).unwrap(),
        }),
        cwd: route.callback.cwd.clone(),
        timeout: spec.timeout,
        env: route.callback.env.clone(),
        binary: PathBuf::from("/bin/echo"),
    });
    let store = Store::open(&home.db_path()).unwrap();
    store
        .insert_local_task(
            &row,
            spec,
            machine,
            crate::submission::RequestId::new(),
            route.callback.codex.clone(),
        )
        .unwrap();
    store
        .cas_status(row.id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    store
        .append_report_with_notification(row.id, ReportOutcome::Succeeded, "done", false)
        .unwrap();
    store
        .cas_exit(
            row.id,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 0 },
        )
        .unwrap();
    drop(store);

    let mut store = Store::open(&home.db_path()).unwrap();
    for outbox in store.pending_outbound_events(row.id).unwrap() {
        store.accept_inbound_event(&outbox.event).unwrap();
    }
    drop(store);
    dispatch(&home, row.id).await;
    dispatch(&home, row.id).await;
    let commands = lines(&PathBuf::from(&route.callback.env.home).join("commands"));
    assert_eq!(commands.len(), 1);
    let store = Store::open(&home.db_path()).unwrap();
    assert_eq!(
        store
            .origin_route_by_task(row.id)
            .unwrap()
            .unwrap()
            .last_settled_seq,
        4
    );
    assert_eq!(
        store
            .inbound_events(row.id)
            .unwrap()
            .iter()
            .filter(|event| event.event.payload.notification_required())
            .count(),
        1
    );
}

#[tokio::test]
async fn reserved_budget_survives_restart_and_failure_stays_visible() {
    let (_dir, home, route) =
        fixture("#!/bin/sh\nprintf 'attempt\\n' >> \"$HOME/commands\"\nexit 2\n");
    let mut store = Store::open(&home.db_path()).unwrap();
    store.insert_origin_route(&route).unwrap();
    store.accept_inbound_event(&event(&route, 1, true)).unwrap();
    store
        .reserve_inbox_attempt(route.task, NonZeroU64::new(1).unwrap())
        .unwrap();
    assert_eq!(store.pending_inbox_tasks().unwrap(), vec![route.task]);
    drop(store);
    dispatch(&home, route.task).await;
    dispatch(&home, route.task).await;
    assert_eq!(
        lines(&PathBuf::from(&route.callback.env.home).join("commands")).len(),
        2
    );
    let store = Store::open(&home.db_path()).unwrap();
    let failures = store.failed_inbox_events(route.task).unwrap();
    assert!(store.pending_inbox_tasks().unwrap().is_empty());
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].seq, 1);
    assert_eq!(failures[0].attempts, 3);
    assert_eq!(
        store
            .origin_route_by_task(route.task)
            .unwrap()
            .unwrap()
            .last_settled_seq,
        1
    );
    assert!(
        std::fs::read_to_string(home.fallback_log_path())
            .unwrap()
            .contains("\"seq\":1")
    );
}

#[tokio::test]
async fn later_success_retains_the_prior_attempt_error() {
    let (_dir, home, route) = fixture(
        "#!/bin/sh\nif [ ! -f \"$HOME/once\" ]; then : > \"$HOME/once\"; echo first-failure >&2; exit 2; fi\n",
    );
    let mut store = Store::open(&home.db_path()).unwrap();
    store.insert_origin_route(&route).unwrap();
    store.accept_inbound_event(&event(&route, 1, true)).unwrap();
    drop(store);
    dispatch(&home, route.task).await;
    let store = Store::open(&home.db_path()).unwrap();
    assert!(
        matches!(&store.inbound_events(route.task).unwrap()[0].delivery,
        DeliveryState::Delivered { attempts: 2, last_error: Some(error) }
            if error.contains("first-failure"))
    );
}

#[tokio::test]
async fn duplicate_wakes_share_one_in_flight_dispatcher() {
    let (_dir, home, route) =
        fixture("#!/bin/sh\n/bin/sleep 0.2\nprintf 'one\\n' >> \"$HOME/commands\"\n");
    let mut persisted = Store::open(&home.db_path()).unwrap();
    persisted.insert_origin_route(&route).unwrap();
    persisted
        .accept_inbound_event(&event(&route, 1, true))
        .unwrap();
    drop(persisted);
    let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let (callback, callback_handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home: home.clone(),
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: None,
        },
    )
    .await
    .unwrap();
    callback
        .cast(CallbackMsg::DispatchInbox { id: route.task })
        .unwrap();
    callback
        .cast(CallbackMsg::DispatchInbox { id: route.task })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let settled = call(&store, |reply| StoreMsg::EarliestInbox {
                id: route.task,
                reply,
            })
            .await
            .unwrap();
            if settled.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        lines(&PathBuf::from(&route.callback.env.home).join("commands")).len(),
        1
    );
    callback.stop(None);
    callback_handle.await.unwrap();
    store.stop(None);
    store_handle.await.unwrap();
}

#[tokio::test]
async fn push_only_after_failed_wake_until_delivery() {
    let (_dir, home, route) = fixture("#!/bin/sh\nexit 0\n");
    let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let (callback, callback_handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home,
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: None,
        },
    )
    .await
    .unwrap();
    let thread = route.thread;
    let alerted = || async {
        call(&callback, |reply| CallbackMsg::InspectAlert {
            thread,
            reply,
        })
        .await
        .unwrap()
    };
    assert!(!alerted().await);
    assert!(
        call(&callback, |reply| CallbackMsg::ClaimWake { thread, reply })
            .await
            .unwrap()
    );
    assert!(
        !call(&callback, |reply| CallbackMsg::ClaimWake { thread, reply })
            .await
            .unwrap()
    );
    assert!(!alerted().await);

    for _ in 0..2 {
        callback
            .cast(CallbackMsg::WakeFailed {
                thread,
                event: event(&route, 1, true),
                failure: WakeFailure::CodexQueued {
                    reason: "T3 refused wake".into(),
                },
            })
            .unwrap();
        assert!(alerted().await);
    }
    callback.cast(CallbackMsg::Delivered { thread }).unwrap();
    assert!(!alerted().await);
    callback
        .cast(CallbackMsg::WakeFailed {
            thread,
            event: event(&route, 2, true),
            failure: WakeFailure::CodexQueued {
                reason: "T3 refused wake".into(),
            },
        })
        .unwrap();
    assert!(alerted().await);

    callback.stop(None);
    callback_handle.await.unwrap();
    store.stop(None);
    store_handle.await.unwrap();
}

#[tokio::test]
async fn stopped_claude_session_waits_and_retry_delivers_to_live_socket() {
    let (_dir, home, route) = fixture("#!/bin/sh\nexit 0\n");
    let callback_home = PathBuf::from(&route.callback.env.home);
    let project = callback_home.join(".claude/projects/-work");
    std::fs::create_dir_all(&project).unwrap();
    let thread = route.thread;
    std::fs::write(project.join(format!("{thread}.jsonl")), "").unwrap();
    let mut persisted = Store::open(&home.db_path()).unwrap();
    persisted.insert_origin_route(&route).unwrap();
    persisted
        .accept_inbound_event(&event(&route, 1, true))
        .unwrap();
    persisted
        .accept_inbound_event(&event(&route, 2, true))
        .unwrap();
    drop(persisted);

    let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let (callback, callback_handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home: home.clone(),
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: None,
        },
    )
    .await
    .unwrap();
    callback
        .cast(CallbackMsg::DispatchInbox { id: route.task })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let entry = call(&store, |reply| StoreMsg::EarliestInbox {
                id: route.task,
                reply,
            })
            .await
            .unwrap()
            .unwrap();
            if matches!(entry.delivery, DeliveryState::AwaitingThread { .. }) {
                assert!(matches!(
                    entry.delivery,
                    DeliveryState::AwaitingThread { attempts: 0, .. }
                ));
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if call(&callback, |reply| CallbackMsg::InspectAlert {
                thread,
                reply,
            })
            .await
            .unwrap()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let persisted = Store::open(&home.db_path()).unwrap();
    assert_eq!(
        persisted
            .origin_route_by_task(route.task)
            .unwrap()
            .unwrap()
            .last_settled_seq,
        0
    );
    assert!(!home.fallback_log_path().exists());
    drop(persisted);

    let socket = callback_home.join("inbox.sock");
    register_live_claude_session(&callback_home, &route.thread.to_string(), &socket);
    let listener = UnixListener::bind(&socket).unwrap();
    let receiver = std::thread::spawn(move || {
        let mut messages = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let mut body = String::new();
            stream.read_to_string(&mut body).unwrap();
            messages.push(body);
        }
        messages
    });
    callback.cast(CallbackMsg::RetryInbox).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let entry = call(&store, |reply| StoreMsg::EarliestInbox {
                id: route.task,
                reply,
            })
            .await
            .unwrap();
            if entry.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let messages = receiver.join().unwrap();
    assert_eq!(messages.len(), 2);
    assert!(messages[0].contains("HOMEBASED_EVENT"));
    let persisted = Store::open(&home.db_path()).unwrap();
    let entries = persisted.inbound_events(route.task).unwrap();
    assert!(matches!(
        entries[0].delivery,
        DeliveryState::Delivered { attempts: 1, .. }
    ));
    assert!(matches!(
        entries[1].delivery,
        DeliveryState::Delivered { attempts: 1, .. }
    ));
    assert!(
        !call(&callback, |reply| CallbackMsg::InspectAlert {
            thread,
            reply
        })
        .await
        .unwrap()
    );
    callback.stop(None);
    callback_handle.await.unwrap();
    store.stop(None);
    store_handle.await.unwrap();
}

#[tokio::test]
async fn throttled_wake_check_waits_without_a_push() {
    let (_dir, home, route) = fixture("#!/bin/sh\nexit 0\n");
    let callback_home = PathBuf::from(&route.callback.env.home);
    let project = callback_home.join(".claude/projects/-work");
    std::fs::create_dir_all(&project).unwrap();
    let thread = route.thread;
    std::fs::write(project.join(format!("{thread}.jsonl")), "").unwrap();
    let mut persisted = Store::open(&home.db_path()).unwrap();
    persisted.insert_origin_route(&route).unwrap();
    persisted
        .accept_inbound_event(&event(&route, 1, true))
        .unwrap();
    drop(persisted);

    let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let (callback, callback_handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home: home.clone(),
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: None,
        },
    )
    .await
    .unwrap();
    // another task woke this thread moments ago
    assert!(
        call(&callback, |reply| CallbackMsg::ClaimWake { thread, reply })
            .await
            .unwrap()
    );

    let drained = dispatch_inbox(store.clone(), home.clone(), route.task, callback.clone())
        .await
        .unwrap();

    assert!(!drained);
    let entry = call(&store, |reply| StoreMsg::EarliestInbox {
        id: route.task,
        reply,
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        entry.delivery,
        DeliveryState::AwaitingThread { attempts: 0, ref reason, .. }
            if reason.contains("T3 wake retried later")
    ));
    assert!(
        !call(&callback, |reply| CallbackMsg::InspectAlert {
            thread,
            reply
        })
        .await
        .unwrap()
    );
    callback.stop(None);
    callback_handle.await.unwrap();
    store.stop(None);
    store_handle.await.unwrap();
}

#[tokio::test]
async fn permanent_context_error_settles_without_an_attempt() {
    let (_dir, home, mut route) = fixture("#!/bin/sh\nexit 0\n");
    route.callback.codex = PathBuf::from("/missing/saved-codex").into();
    let mut store = Store::open(&home.db_path()).unwrap();
    store.insert_origin_route(&route).unwrap();
    store.accept_inbound_event(&event(&route, 1, true)).unwrap();
    store
        .accept_inbound_event(&event(&route, 2, false))
        .unwrap();
    drop(store);
    dispatch(&home, route.task).await;
    let store = Store::open(&home.db_path()).unwrap();
    assert_eq!(
        store.failed_inbox_events(route.task).unwrap()[0].attempts,
        0
    );
    assert_eq!(
        store
            .origin_route_by_task(route.task)
            .unwrap()
            .unwrap()
            .last_settled_seq,
        2
    );
}

#[tokio::test]
async fn fallback_write_error_does_not_block_later_success() {
    let (_dir, home, route) = fixture(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/commands\"\ncase \"$*\" in *'\"seq\":1'*) exit 2;; esac\n",
    );
    std::fs::create_dir(home.fallback_log_path()).unwrap();
    let mut store = Store::open(&home.db_path()).unwrap();
    store.insert_origin_route(&route).unwrap();
    store.accept_inbound_event(&event(&route, 1, true)).unwrap();
    store.accept_inbound_event(&event(&route, 2, true)).unwrap();
    drop(store);
    dispatch(&home, route.task).await;
    let store = Store::open(&home.db_path()).unwrap();
    let entries = store.inbound_events(route.task).unwrap();
    assert!(matches!(
        entries[0].delivery,
        DeliveryState::DeliveryFailed { attempts: 3, .. }
    ));
    assert!(matches!(
        entries[1].delivery,
        DeliveryState::Delivered { attempts: 1, .. }
    ));
    assert_eq!(
        store
            .origin_route_by_task(route.task)
            .unwrap()
            .unwrap()
            .last_settled_seq,
        2
    );
    assert_eq!(store.failed_inbox_events(route.task).unwrap().len(), 1);
}

#[tokio::test]
async fn callback_actor_survives_an_unavailable_store() {
    let (_dir, home, route) = fixture("#!/bin/sh\nexit 0\n");
    let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let (callback, callback_handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home,
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: None,
        },
    )
    .await
    .unwrap();
    // every store call from the callback actor fails from here on
    store.stop(None);
    store_handle.await.unwrap();

    callback
        .cast(CallbackMsg::DispatchInbox { id: route.task })
        .unwrap();
    callback.cast(CallbackMsg::RetryInbox).unwrap();
    callback
        .cast(CallbackMsg::InboxFinished {
            id: route.task,
            completed: true,
        })
        .unwrap();

    let thread = route.thread;
    assert!(
        call(&callback, |reply| CallbackMsg::ClaimWake { thread, reply })
            .await
            .unwrap(),
        "the callback actor keeps running after failed store calls"
    );
    callback.stop(None);
    callback_handle.await.unwrap();
}

/// Write a live registry entry and peer key for `thread` under `callback_home`
fn register_live_claude_session(callback_home: &Path, thread: &str, socket: &Path) {
    let sessions = callback_home.join(".claude/sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let pid = std::process::id();
    std::fs::write(
        sessions.join(format!("{pid}.json")),
        serde_json::json!({
            "pid": pid,
            "sessionId": thread,
            "messagingSocketPath": socket,
            "peerProtocol": 1,
            "updatedAt": 1,
        })
        .to_string(),
    )
    .unwrap();
    let digest = Sha256::digest(socket.as_os_str().as_encoded_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    std::fs::write(
        sessions.join(format!("{pid}.{hex}.key")),
        serde_json::json!({ "peerToken": "test-token" }).to_string(),
    )
    .unwrap();
}

#[tokio::test]
async fn new_claude_process_receives_each_waiting_event_exactly_once() {
    let (_dir, home, route) = fixture("#!/bin/sh\nexit 0\n");
    let callback_home = PathBuf::from(&route.callback.env.home);
    let project = callback_home.join(".claude/projects/-work");
    std::fs::create_dir_all(&project).unwrap();
    let thread = route.thread;
    std::fs::write(project.join(format!("{thread}.jsonl")), "").unwrap();
    // a dead turn process of the same session, as T3 Code leaves behind
    let sessions = callback_home.join(".claude/sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    std::fs::write(
        sessions.join("0.json"),
        serde_json::json!({
            "pid": 0,
            "sessionId": thread.to_string(),
            "messagingSocketPath": callback_home.join("dead.sock"),
            "peerProtocol": 1,
            "updatedAt": 2,
        })
        .to_string(),
    )
    .unwrap();
    let mut persisted = Store::open(&home.db_path()).unwrap();
    persisted.insert_origin_route(&route).unwrap();
    for seq in [1, 2] {
        persisted
            .accept_inbound_event(&event(&route, seq, true))
            .unwrap();
    }
    drop(persisted);

    let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
        .await
        .unwrap();
    let (callback, callback_handle) = CallbackActor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store.clone(),
            home: home.clone(),
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: Some(sessions.clone()),
        },
    )
    .await
    .unwrap();
    callback
        .cast(CallbackMsg::DispatchInbox { id: route.task })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let entry = call(&store, |reply| StoreMsg::EarliestInbox {
                id: route.task,
                reply,
            })
            .await
            .unwrap()
            .unwrap();
            if matches!(entry.delivery, DeliveryState::AwaitingThread { .. }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let waiting = Store::open(&home.db_path())
        .unwrap()
        .waiting_inbox_events(route.task)
        .unwrap();
    assert_eq!(waiting.len(), 1, "only the head of the task waits");
    assert_eq!(waiting[0].seq, 1);
    assert_eq!(
        waiting[0].until - waiting[0].since,
        crate::events::THREAD_WAIT_LIMIT
    );

    let socket = callback_home.join("inbox.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    listener.set_nonblocking(true).unwrap();
    let receiver = std::thread::spawn(move || {
        let mut messages = Vec::new();
        // wait past a second registry change and retry for any duplicate
        let deadline = Instant::now() + Duration::from_secs(8);
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nonblocking(false).unwrap();
                    let mut body = String::new();
                    stream.read_to_string(&mut body).unwrap();
                    messages.push(body);
                }
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
        messages
    });
    // no RetryInbox cast: the registry watcher must notice the new process
    register_live_claude_session(&callback_home, &thread.to_string(), &socket);
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let entry = call(&store, |reply| StoreMsg::EarliestInbox {
                id: route.task,
                reply,
            })
            .await
            .unwrap();
            if entry.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // a later turn changes the registry again; settled events must not resend
    tokio::time::sleep(Duration::from_millis(1100)).await;
    register_live_claude_session(&callback_home, &thread.to_string(), &socket);
    callback.cast(CallbackMsg::RetryInbox).unwrap();

    let messages = tokio::task::spawn_blocking(move || receiver.join().unwrap())
        .await
        .unwrap();
    assert_eq!(messages.len(), 2, "{messages:?}");
    for (message, seq) in messages.iter().zip([1, 2]) {
        let frame: serde_json::Value =
            serde_json::from_str(message.lines().nth(1).unwrap()).unwrap();
        assert_eq!(frame["session_id"], thread.to_string());
        let content = frame["message"]["content"].as_str().unwrap();
        let event: serde_json::Value =
            serde_json::from_str(content.strip_prefix("HOMEBASED_EVENT ").unwrap()).unwrap();
        assert_eq!(event["seq"], seq, "events arrive in task order");
    }
    let entries = Store::open(&home.db_path())
        .unwrap()
        .inbound_events(route.task)
        .unwrap();
    for entry in entries {
        assert!(matches!(
            entry.delivery,
            DeliveryState::Delivered { attempts: 1, .. }
        ));
    }
    callback.stop(None);
    callback_handle.await.unwrap();
    store.stop(None);
    store_handle.await.unwrap();
}
