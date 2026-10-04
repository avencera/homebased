//! Remote submission: request identity, routes, acceptance, and abandon

use super::{
    Daemon, add_peer, cli_remote_spec, post_execution, register_thread, remote_spec, route_state,
    seed_origin_route, submit_command, submit_file, wait_for_probe, wait_until,
};
use homebased::domain::{ProcessStatus, TaskEnv, TaskId};
use homebased::fleet::http::ClusterClient;
use homebased::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
use homebased::store::{NewTask, Store, new_queued_task};
use homebased::submission::{RequestId, SubmissionState};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Duration;

#[test]
fn origin_socket_submission_retries_and_keeps_callbacks_local() {
    use std::os::unix::fs::PermissionsExt;

    let mut origin = Daemon::start("remote-origin", true);
    let executor = Daemon::start("remote-executor", true);
    let codex = origin.user_home.join("codex");
    fs::write(
        &codex,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/callbacks\"\n",
    )
    .unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    origin.codex_override = Some(codex.clone());
    origin.restart();
    add_peer(&origin, &executor);
    add_peer(&executor, &origin);

    let spec = cli_remote_spec(
        &executor,
        vec!["/bin/sh", "-c", "echo remote-run >> \"$HOME/remote-runs\""],
    );
    let request = RequestId::new();
    let first = submit_file(&origin, &spec, request);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let body: Value = serde_json::from_slice(&first.stdout).unwrap();
    let task: TaskId = body["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(body["task_id"], body["id"]);
    assert_eq!(body["request_id"], request.0.to_string());
    let second = submit_file(&origin, &spec, request);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let retry: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(retry["id"], body["id"]);
    let route = Store::open(&origin.home.join(homebased::home::DB_NAME))
        .unwrap()
        .origin_route_by_request(request)
        .unwrap()
        .unwrap();
    assert_eq!(route.task, task);
    assert_eq!(route.callback.cwd, origin.user_home.canonicalize().unwrap());
    assert_eq!(route.callback.codex.path(), Some(codex.as_path()));
    assert!(matches!(route.submission, SubmissionState::Accepted));
    assert!(
        Store::open(&origin.home.join(homebased::home::DB_NAME))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_none()
    );
    assert!(wait_until(Duration::from_secs(10), || origin
        .user_home
        .join("callbacks")
        .exists()));
    assert!(executor.user_home.join("remote-runs").exists());
    assert!(!executor.user_home.join("callbacks").exists());

    let mut changed = spec.clone();
    changed.cwd = executor.user_home.join("different-content");
    let conflict = submit_file(&origin, &changed, request);
    assert!(!conflict.status.success());
    let error: Value = serde_json::from_slice(&conflict.stderr).unwrap();
    assert_eq!(error["error"]["code"], "submission_conflict");
    assert_eq!(error["error"]["input"]["task_id"], task.to_string());
}

#[test]
fn concurrent_remote_submissions_with_one_request_uuid_share_the_first_task() {
    let origin = Daemon::start("remote-origin", true);
    let executor = Daemon::start("remote-executor", true);
    add_peer(&origin, &executor);
    add_peer(&executor, &origin);

    let spec = cli_remote_spec(&executor, vec!["/bin/echo", "one-task"]);
    let request = RequestId::new();
    let first_path = origin._dir.path().join("submit-concurrent-first.json");
    let second_path = origin._dir.path().join("submit-concurrent-second.json");
    let contents = serde_json::to_vec(&spec).unwrap();
    fs::write(&first_path, &contents).unwrap();
    fs::write(&second_path, contents).unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let first_barrier = barrier.clone();
    let mut first_command = submit_command(&origin, &first_path, request);
    let first = std::thread::spawn(move || {
        first_barrier.wait();
        first_command.output().unwrap()
    });
    let second_barrier = barrier.clone();
    let mut second_command = submit_command(&origin, &second_path, request);
    let second = std::thread::spawn(move || {
        second_barrier.wait();
        second_command.output().unwrap()
    });
    barrier.wait();

    let first = first.join().unwrap();
    let second = second.join().unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let first: Value = serde_json::from_slice(&first.stdout).unwrap();
    let second: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(first["id"], second["id"]);
    assert_eq!(first["request_id"], request.0.to_string());
    assert_eq!(second["request_id"], request.0.to_string());

    let task: TaskId = first["id"].as_str().unwrap().parse().unwrap();
    let origin_store = Store::open(&origin.home.join(homebased::home::DB_NAME)).unwrap();
    let route = origin_store
        .origin_route_by_request(request)
        .unwrap()
        .unwrap();
    assert_eq!(route.task, task);
    assert!(matches!(route.submission, SubmissionState::Accepted));
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .executor_identity(task)
            .unwrap()
            .is_some()
    );
}

#[test]
fn explicit_local_machine_name_is_rejected_as_a_remote_selector() {
    let daemon = Daemon::start("solo", false);
    let mut spec = remote_spec(&daemon, vec!["/bin/echo", "local"]);
    spec.machine = Some("solo".parse().unwrap());
    let request = RequestId::new();

    let output = submit_file(&daemon, &spec, request);
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "invalid_spec");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("omit machine")
    );
    assert!(
        Store::open(&daemon.home.join(homebased::home::DB_NAME))
            .unwrap()
            .list_tasks(&[], None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn unknown_origin_route_resolves_by_identity_or_abandon() {
    let mut origin = Daemon::start("remote-origin", true);
    let mut executor = Daemon::start("remote-executor", true);
    add_peer(&origin, &executor);
    let mut spec = cli_remote_spec(&executor, vec!["/bin/echo", "hello"]);
    let accepted_task = TaskId::new();
    let accepted_request = seed_origin_route(&origin, &executor, accepted_task, &spec);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (status, body) = post_execution(
            &executor,
            &origin,
            accepted_task,
            &serde_json::to_value(&spec).unwrap(),
        )
        .await;
        assert_eq!(status, 200, "{body}");
    });
    let retry = submit_file(&origin, &spec, accepted_request);
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&retry.stdout).unwrap()["id"],
        accepted_task.to_string()
    );

    let absent_task = TaskId::new();
    let absent_request = seed_origin_route(&origin, &executor, absent_task, &spec);
    executor.stop();
    origin.restart();
    let offline = submit_file(&origin, &spec, absent_request);
    assert!(!offline.status.success());
    let error: Value = serde_json::from_slice(&offline.stderr).unwrap();
    assert_eq!(error["error"]["code"], "submission_outcome_unknown");
    assert_eq!(error["error"]["input"]["task_id"], absent_task.to_string());
    assert!(matches!(
        Store::open(&origin.home.join(homebased::home::DB_NAME))
            .unwrap()
            .origin_route_by_task(absent_task)
            .unwrap()
            .unwrap()
            .submission,
        SubmissionState::AcceptanceUnknown
    ));
    executor.restart();
    let abandoned = submit_file(&origin, &spec, absent_request);
    assert!(!abandoned.status.success());
    let error: Value = serde_json::from_slice(&abandoned.stderr).unwrap();
    assert_eq!(error["error"]["code"], "submission_rejected");
    assert_eq!(
        error["error"]["input"]["reason"],
        "abandoned_before_acceptance"
    );
    runtime.block_on(async {
        let (status, body) = post_execution(
            &executor,
            &origin,
            absent_task,
            &serde_json::to_value(&spec).unwrap(),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["identity"]["reason"], "abandoned_before_acceptance");
    });

    spec.cwd = executor.user_home.join("not-present");
    let before = RequestId::new();
    executor.stop();
    let unavailable = submit_file(&origin, &spec, before);
    assert!(!unavailable.status.success());
    let error: Value = serde_json::from_slice(&unavailable.stderr).unwrap();
    assert_eq!(error["error"]["code"], "machine_unavailable");
    assert_eq!(error["error"]["retryable"], true);
    assert!(
        Store::open(&origin.home.join(homebased::home::DB_NAME))
            .unwrap()
            .origin_route_by_request(before)
            .unwrap()
            .is_none()
    );
}

#[test]
fn remote_dry_run_expands_executor_home_without_identity() {
    let origin = Daemon::start("remote-origin", true);
    let executor = Daemon::start("remote-executor", true);
    add_peer(&origin, &executor);
    let mut spec = cli_remote_spec(&executor, vec!["/bin/echo", "hello"]);
    spec.cwd = PathBuf::from("~/");
    let path = origin._dir.path().join("dry-run.json");
    fs::write(&path, serde_json::to_vec(&spec).unwrap()).unwrap();
    register_thread(&origin.user_home, &spec.thread.to_string());
    let output = origin
        .cmd()
        .current_dir(&origin.user_home)
        .args(["--json", "task", "submit", "--spec"])
        .arg(&path)
        .arg("--dry-run")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        body["execution_cwd"]
            .as_str()
            .unwrap()
            .trim_end_matches('/'),
        executor.user_home.to_string_lossy().as_ref()
    );
    assert_eq!(body["argv"], serde_json::json!(["/bin/echo", "hello"]));
    spec.cwd = PathBuf::from("~/not-present");
    fs::write(&path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let missing_cwd = origin
        .cmd()
        .current_dir(&origin.user_home)
        .args(["--json", "task", "submit", "--spec"])
        .arg(&path)
        .arg("--dry-run")
        .output()
        .unwrap();
    assert!(!missing_cwd.status.success());
    let error: Value = serde_json::from_slice(&missing_cwd.stderr).unwrap();
    assert_eq!(error["error"]["code"], "invalid_cwd");
    assert_eq!(error["error"]["input"]["problem"], "not_found");
    spec.cwd = PathBuf::from("~/");
    spec.workload = remote_spec(&executor, vec!["/missing-executor-command"]).workload;
    fs::write(&path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let missing_program = origin
        .cmd()
        .current_dir(&origin.user_home)
        .args(["--json", "task", "submit", "--spec"])
        .arg(&path)
        .arg("--dry-run")
        .output()
        .unwrap();
    assert!(!missing_program.status.success());
    let error: Value = serde_json::from_slice(&missing_program.stderr).unwrap();
    assert_eq!(error["error"]["code"], "executable_missing");
    assert!(
        Store::open(&origin.home.join(homebased::home::DB_NAME))
            .unwrap()
            .list_tasks(&[], None)
            .unwrap()
            .is_empty()
    );
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .list_tasks(&[], None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn dropped_local_socket_response_reuses_the_allocated_task() {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    let mut origin = Daemon::start("remote-origin", true);
    let executor = Daemon::start("remote-executor", true);
    origin.codex_override = Some(PathBuf::from("/usr/bin/true"));
    origin.restart();
    add_peer(&origin, &executor);
    add_peer(&executor, &origin);
    let spec = cli_remote_spec(&executor, vec!["/bin/echo", "one-run"]);
    let request = RequestId::new();
    let body = serde_json::to_vec(&serde_json::json!({
        "spec": spec,
        "env": TaskEnv::capture(),
        "callback_cwd": origin.user_home,
        "request_id": request,
    }))
    .unwrap();
    let mut socket = UnixStream::connect(origin.home.join("homebased.sock")).unwrap();
    write!(socket, "POST /v1/tasks HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", body.len()).unwrap();
    socket.write_all(&body).unwrap();
    let routed = wait_until(Duration::from_secs(5), || {
        Store::open(&origin.home.join(homebased::home::DB_NAME))
            .unwrap()
            .origin_route_by_request(request)
            .unwrap()
            .is_some()
    });
    if !routed {
        socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut response = String::new();
        let _ = socket.read_to_string(&mut response);
        panic!("socket request made no route: {response}");
    }
    let task = Store::open(&origin.home.join(homebased::home::DB_NAME))
        .unwrap()
        .origin_route_by_request(request)
        .unwrap()
        .unwrap()
        .task;
    assert!(wait_until(Duration::from_secs(5), || Store::open(
        &executor.home.join(homebased::home::DB_NAME)
    )
    .unwrap()
    .executor_identity(task)
    .unwrap()
    .is_some()));
    drop(socket);
    let retry = submit_file(&origin, &spec, request);
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let body: Value = serde_json::from_slice(&retry.stdout).unwrap();
    let returned: TaskId = body["id"].as_str().unwrap().parse().unwrap();
    let route = Store::open(&origin.home.join(homebased::home::DB_NAME))
        .unwrap()
        .origin_route_by_request(request)
        .unwrap()
        .unwrap();
    assert_eq!(route.task, returned);
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .executor_identity(task)
            .unwrap()
            .is_some()
    );
}

#[test]
fn restart_resolves_a_route_saved_before_send() {
    let mut origin = Daemon::start("remote-origin", true);
    let executor = Daemon::start("remote-executor", true);
    add_peer(&origin, &executor);
    let spec = cli_remote_spec(&executor, vec!["/bin/echo", "never-start"]);
    let task = TaskId::new();
    let request = seed_origin_route(&origin, &executor, task, &spec);
    origin.restart();
    assert!(wait_until(Duration::from_secs(10), || {
        matches!(
            Store::open(&origin.home.join(homebased::home::DB_NAME))
                .unwrap()
                .origin_route_by_request(request)
                .unwrap()
                .unwrap()
                .submission,
            SubmissionState::Rejected { .. }
        )
    }));
    let identity = Store::open(&executor.home.join(homebased::home::DB_NAME))
        .unwrap()
        .executor_identity(task)
        .unwrap()
        .unwrap();
    assert!(
        matches!(identity, homebased::submission::ExecutorIdentity::Rejected(tombstone)
        if tombstone.reason == "abandoned_before_acceptance")
    );
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_none()
    );
}

#[test]
fn cli_rejects_an_invalid_request_uuid() {
    let daemon = Daemon::start("remote-origin", true);
    let output = daemon
        .cmd()
        .args([
            "task",
            "submit",
            "--spec",
            "missing.json",
            "--request-id",
            "not-a-uuid",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid value"));
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_accept_retry_conflict_and_events_use_executor_only() {
    let origin = Daemon::start("remote-origin", true);
    let mut executor = Daemon::start("remote-executor", true);
    let fake_codex = executor.user_home.join("codex");
    fs::write(
        &fake_codex,
        "#!/bin/sh\necho called >> \"$HOME/local-callback\"\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    executor.codex_override = Some(fake_codex);
    executor.restart();
    wait_for_probe(&executor.address()).await;
    add_peer(&executor, &origin);
    let task = TaskId::new();
    let spec = remote_spec(
        &executor,
        vec![
            "/bin/sh",
            "-c",
            "echo run >> \"$HOME/count\"; echo remote-output",
        ],
    );
    seed_origin_route(&origin, &executor, task, &spec);
    let value = serde_json::to_value(&spec).unwrap();

    let (status, first) = post_execution(&executor, &origin, task, &value).await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(first["identity"]["type"], "accepted");
    let (status, duplicate) = post_execution(&executor, &origin, task, &value).await;
    assert_eq!(status, 200, "{duplicate}");
    assert_eq!(duplicate["identity"]["type"], "accepted");
    assert!(wait_until(Duration::from_secs(10), || {
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_some_and(|row| row.status() == ProcessStatus::Succeeded)
    }));
    assert!(wait_until(Duration::from_secs(12), || route_state(
        &executor, task
    )
    .acknowledged
        >= 3));
    assert_eq!(
        fs::read_to_string(executor.user_home.join("count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(
        fs::read_to_string(
            executor
                .home
                .join("tasks")
                .join(task.to_string())
                .join("output.log")
        )
        .unwrap()
        .contains("remote-output")
    );
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .origin_route_by_task(task)
            .unwrap()
            .is_none()
    );
    assert!(!executor.user_home.join("local-callback").exists());
    assert_eq!(
        Store::open(&origin.home.join(homebased::home::DB_NAME))
            .unwrap()
            .inbound_events(task)
            .unwrap()
            .len(),
        3
    );

    let mut changed = value.clone();
    changed["workload"]["command"][2] = serde_json::json!("echo changed");
    let (status, _) = post_execution(&executor, &origin, task, &changed).await;
    assert_eq!(status, 409);
    let (status, _) = post_execution(&executor, &executor, task, &value).await;
    assert_eq!(status, 409);
    assert_eq!(
        fs::read_to_string(executor.user_home.join("count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_destination_abandon_and_invalid_requests_do_not_launch() {
    let origin = Daemon::start("reject-origin", true);
    let executor = Daemon::start("reject-executor", true);
    wait_for_probe(&executor.address()).await;
    let probe = ClusterClient::default()
        .get(&executor.address(), "/v1/cluster/machine")
        .await
        .unwrap();
    assert_eq!(probe.status.as_u16(), 200);
    let advertisement: Value = serde_json::from_slice(&probe.body).unwrap();
    assert_eq!(advertisement["api_version"], 1);

    let task = TaskId::new();
    let spec = serde_json::to_value(remote_spec(&executor, vec!["/bin/echo", "never"])).unwrap();
    let wrong = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions",
            &serde_json::json!({
                "api_version": 1, "protocol_version": CLUSTER_PROTOCOL_VERSION, "destination_machine": origin.machine_id(),
                "origin_machine": origin.machine_id(), "task": task, "spec": spec
            }),
        )
        .await
        .unwrap();
    assert_eq!(wrong.status.as_u16(), 409);
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .executor_identity(task)
            .unwrap()
            .is_none()
    );

    let incompatible = TaskId::new();
    let response = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions",
            &serde_json::json!({
                "api_version": 1, "protocol_version": 99, "destination_machine": executor.machine_id(),
                "origin_machine": origin.machine_id(), "task": incompatible, "spec": spec
            }),
        )
        .await
        .unwrap();
    assert_ne!(response.status.as_u16(), 200);
    let error: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(error["api_version"], 1);
    assert_eq!(error["error"]["code"], "cluster_protocol_incompatible");
    assert_eq!(error["error"]["input"]["remote"]["min"], 99);
    assert_eq!(error["error"]["input"]["remote"]["max"], 99);
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .executor_identity(incompatible)
            .unwrap()
            .is_none()
    );

    for api_version in [Some(99), None] {
        let task = TaskId::new();
        let mut request = serde_json::json!({
            "protocol_version": CLUSTER_PROTOCOL_VERSION,
            "destination_machine": executor.machine_id(),
            "origin_machine": origin.machine_id(),
            "task": task,
            "spec": spec
        });
        if let Some(api_version) = api_version {
            request["api_version"] = serde_json::json!(api_version);
        }
        let response = ClusterClient::default()
            .post_json(&executor.address(), "/v1/cluster/executions", &request)
            .await
            .unwrap();
        assert_eq!(response.status.as_u16(), 400);
        let error: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error["api_version"], 1);
        assert_eq!(error["error"]["code"], "usage");
        assert!(
            Store::open(&executor.home.join(homebased::home::DB_NAME))
                .unwrap()
                .executor_identity(task)
                .unwrap()
                .is_none()
        );
    }

    let same_owner = TaskId::new();
    let (status, body) = post_execution(&executor, &executor, same_owner, &spec).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["identity"]["reason"], "origin_equals_executor");

    let abandon = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions/abandon",
            &serde_json::json!({
                "api_version": 1, "protocol_version": CLUSTER_PROTOCOL_VERSION, "destination_machine": executor.machine_id(),
                "origin_machine": origin.machine_id(), "task": task
            }),
        )
        .await
        .unwrap();
    assert_eq!(abandon.status.as_u16(), 200);
    let abandon_body: Value = serde_json::from_slice(&abandon.body).unwrap();
    assert_eq!(abandon_body["api_version"], 1);
    assert_eq!(abandon_body["protocol_version"], CLUSTER_PROTOCOL_VERSION.0);
    let (status, body) = post_execution(&executor, &origin, task, &spec).await;
    assert_eq!(status, 200);
    assert_eq!(body["identity"]["type"], "rejected");
    assert_eq!(body["identity"]["reason"], "abandoned_before_acceptance");
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_none()
    );

    let invalid = TaskId::new();
    let mut bad = spec;
    bad["workload"]["prompt_file"] = serde_json::json!("/origin/only");
    let (status, body) = post_execution(&executor, &origin, invalid, &bad).await;
    assert_eq!(status, 200);
    assert_eq!(body["identity"]["reason"], "invalid_spec");
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .get_task(invalid)
            .unwrap()
            .is_none()
    );

    let leaked_context = TaskId::new();
    let response = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions",
            &serde_json::json!({
                "api_version": 1, "protocol_version": CLUSTER_PROTOCOL_VERSION, "destination_machine": executor.machine_id(),
                "origin_machine": origin.machine_id(), "task": leaked_context,
                "spec": remote_spec(&executor, vec!["/bin/echo", "never"]),
                "callback_context": { "cwd": "/origin-only" }
            }),
        )
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["identity"]["reason"], "invalid_request_fields");

    let missing_binary = TaskId::new();
    let unavailable = serde_json::to_value(remote_spec(
        &executor,
        vec!["missing-homebased-test-binary"],
    ))
    .unwrap();
    let (status, body) = post_execution(&executor, &origin, missing_binary, &unavailable).await;
    assert_eq!(status, 503, "{body}");
    assert!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .executor_identity(missing_binary)
            .unwrap()
            .is_none()
    );

    let home_task = TaskId::new();
    let mut home_spec =
        serde_json::to_value(remote_spec(&executor, vec!["/bin/echo", "home"])).unwrap();
    home_spec["cwd"] = serde_json::json!("~/");
    let (status, accepted) = post_execution(&executor, &origin, home_task, &home_spec).await;
    assert_eq!(status, 200, "{accepted}");
    assert_eq!(accepted["identity"]["spec"]["cwd"], "~/", "{accepted}");
    assert_eq!(
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .require_task(home_task)
            .unwrap()
            .cwd,
        executor.user_home
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn accepted_queued_remote_task_launches_after_restart() {
    let origin = Daemon::start("restart-origin", true);
    let mut executor = Daemon::start("restart-executor", true);
    wait_for_probe(&executor.address()).await;
    let task = TaskId::new();
    let spec = remote_spec(
        &executor,
        vec!["/bin/sh", "-c", "echo once >> \"$HOME/restarted\""],
    );
    executor.stop();
    fs::create_dir_all(executor.home.join("tasks").join(task.to_string())).unwrap();
    let row = new_queued_task(NewTask {
        id: task,
        name: spec.name.clone(),
        thread: spec.thread,
        workload: homebased::invocation::persist_workload(&spec.workload),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: TaskEnv {
            path: "/bin:/usr/bin".into(),
            home: executor.user_home.to_string_lossy().into_owned(),
        },
        binary: PathBuf::from("/bin/sh"),
    });
    Store::open(&executor.home.join(homebased::home::DB_NAME))
        .unwrap()
        .insert_remote_task(&row, &spec, origin.machine_id(), executor.machine_id())
        .unwrap();
    executor.restart();
    assert!(wait_until(Duration::from_secs(10), || {
        Store::open(&executor.home.join(homebased::home::DB_NAME))
            .unwrap()
            .get_task(task)
            .unwrap()
            .is_some_and(|row| row.status() == ProcessStatus::Succeeded)
    }));
    let (status, body) = post_execution(
        &executor,
        &origin,
        task,
        &serde_json::to_value(spec).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        fs::read_to_string(executor.user_home.join("restarted"))
            .unwrap()
            .lines()
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_acceptance_and_abandon_have_one_winner() {
    let origin = Daemon::start("race-origin", true);
    let executor = Daemon::start("race-executor", true);
    wait_for_probe(&executor.address()).await;
    let task = TaskId::new();
    let spec = serde_json::to_value(remote_spec(
        &executor,
        vec!["/bin/sh", "-c", "echo run >> \"$HOME/race-count\""],
    ))
    .unwrap();
    let client = ClusterClient::default();
    let address = executor.address();
    let abandon_body = serde_json::json!({
        "api_version": 1, "protocol_version": CLUSTER_PROTOCOL_VERSION, "destination_machine": executor.machine_id(),
        "origin_machine": origin.machine_id(), "task": task
    });
    let abandon = client.post_json(&address, "/v1/cluster/executions/abandon", &abandon_body);
    let submit = post_execution(&executor, &origin, task, &spec);
    let (abandon, submit) = tokio::join!(abandon, submit);
    let abandoned: Value = serde_json::from_slice(&abandon.unwrap().body).unwrap();
    let (status, submitted) = submit;
    assert_eq!(status, 200, "{submitted}");
    assert_eq!(abandoned["identity"]["type"], submitted["identity"]["type"]);
    let saved = Store::open(&executor.home.join(homebased::home::DB_NAME)).unwrap();
    if submitted["identity"]["type"] == "rejected" {
        assert!(saved.get_task(task).unwrap().is_none());
        assert!(!executor.user_home.join("race-count").exists());
    } else {
        assert!(saved.get_task(task).unwrap().is_some());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lost_remote_response_retries_without_second_child() {
    let origin = Daemon::start("lost-origin", true);
    let executor = Daemon::start("lost-executor", true);
    wait_for_probe(&executor.address()).await;
    let task = TaskId::new();
    let spec = serde_json::to_value(remote_spec(
        &executor,
        vec!["/bin/sh", "-c", "echo run >> \"$HOME/lost-count\""],
    ))
    .unwrap();
    let response = ClusterClient::default()
        .post_json(
            &executor.address(),
            "/v1/cluster/executions",
            &serde_json::json!({
                "api_version": 1, "protocol_version": CLUSTER_PROTOCOL_VERSION, "destination_machine": executor.machine_id(),
                "origin_machine": origin.machine_id(), "task": task, "spec": spec
            }),
        )
        .await
        .unwrap();
    drop(response);
    let (status, retry) = post_execution(&executor, &origin, task, &spec).await;
    assert_eq!(status, 200, "{retry}");
    assert_eq!(retry["identity"]["type"], "accepted");
    assert!(wait_until(Duration::from_secs(10), || executor
        .user_home
        .join("lost-count")
        .exists()));
    assert_eq!(
        fs::read_to_string(executor.user_home.join("lost-count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
}

#[test]
fn remote_submit_with_a_missing_executor_cwd_is_a_typed_rejection() {
    let origin = Daemon::start("missing-cwd-origin", true);
    let executor = Daemon::start("remote-executor", true);
    add_peer(&origin, &executor);
    let mut spec = cli_remote_spec(&executor, vec!["/bin/echo", "hello"]);
    spec.cwd = PathBuf::from("~/not-present");
    let request = RequestId::new();

    // the executor refuses at acceptance, and a retry answers from the saved refusal
    for _ in 0..2 {
        let output = submit_file(&origin, &spec, request);
        assert!(!output.status.success());
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["code"], "invalid_cwd", "{error}");
        assert_eq!(error["error"]["input"]["pointer"], "/cwd");
        assert_eq!(error["error"]["input"]["value"], "~/not-present");
        assert_eq!(error["error"]["input"]["problem"], "not_found");
    }
    let rejected: String = rusqlite::Connection::open(executor.home.join(homebased::home::DB_NAME))
        .unwrap()
        .query_row("SELECT identity_json FROM executor_identities", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(rejected.contains("cwd_not_found"), "{rejected}");
}
