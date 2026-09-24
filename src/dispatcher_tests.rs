//! Origin inbox dispatcher tests with a fake saved Codex executable

use crate::daemon::actors::StoreActor;
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use ractor::Actor;
use tempfile::TempDir;

use crate::daemon::actors::callback::{CallbackActor, CallbackArgs, CallbackMsg, dispatch_inbox};
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{ProcessStatus, TaskEnv, TaskId};
use crate::events::{DeliveryState, EventPayload, TaskEvent};
use crate::home::Home;
use crate::machine::MachineId;
use crate::store::Store;
use crate::submission::{CallbackContext, OriginRoute, PersistedSpec, RequestId, SubmissionState};

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
        spec: spec.into(),
        submission: SubmissionState::AcceptanceUnknown,
        last_execution_state: None,
        last_updated_at: Some(chrono::Utc::now()),
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
                "display_name": "origin inbox", "workload": { "type": "task", "command": ["echo", "remote"] },
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
    dispatch_inbox(store.clone(), home.clone(), id)
        .await
        .unwrap();
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
async fn local_terminal_event_delivers_once_after_restart() {
    use crate::domain::{ExitReason, ReportOutcome, TaskWorkload, Workload};
    use crate::invocation::CommandLine;
    use crate::store::{NewTask, new_queued_task};

    let (_dir, home, mut route) =
        fixture("#!/bin/sh\nprintf 'callback\\n' >> \"$HOME/commands\"\n");
    let PersistedSpec::Current(spec) = &mut route.spec else {
        panic!("test fixture must have normalized spec");
    };
    spec.cwd = route.callback.cwd.clone();
    let machine = MachineId::new();
    let row = new_queued_task(NewTask {
        id: route.task,
        name: Some(spec.name.clone()),
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
        .insert_local_task(&row, spec, machine, route.callback.codex.clone())
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
            .filter(|event| event.notification_required)
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
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
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
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
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
