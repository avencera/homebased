fn run_unconditionally(
    cmd: &mut Command,
    timeout: Duration,
    delivery_lock: &Path,
) -> Result<std::process::Output, String> {
    run_command_deadline(
        cmd,
        timeout,
        SendGate {
            path: delivery_lock,
            check: None,
            hold: false,
        },
    )?
    .ok_or_else(|| "unconditional callback was suppressed".into())
}

use super::delivery::{find_saved_origin, run_command_deadline};
use super::send_check::SendGate;
use super::{
    EventKind, EventReason, ExitClass, ExitPolicy, NextAction, OriginSession, PendingT3Send,
    ProcessPayload, WorkloadView, check_due_event, classify_exit, exit_event, last_event_for_row,
    lost_event, notify_event, send_saved_queue_attempt,
};
use crate::domain::{
    Agent, AgentKind, AgentWorkload, ExitReason, ProcessGroupExitEvidence, ReportOutcome, TaskEnv,
    TaskId, TaskReport, TaskRow, TaskState, Workload,
};
use crate::submission::CallbackContext;
use crate::waiting::{Parking, WaitTargets, WaitingNotes, WaitingReport};
use chrono::Utc;
use nix::sys::signal::kill;
use nix::unistd::Pid;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

fn message_line(event: &super::HomebasedEvent) -> String {
    format!("HOMEBASED_EVENT {}", serde_json::to_string(event).unwrap())
}

fn row(state: TaskState) -> TaskRow {
    TaskRow {
        id: TaskId::new(),
        name: crate::domain::TaskName::parse("test task").unwrap(),
        thread: "01a0ab97-a7aa-7463-a5b0-8d500e40e431".parse().unwrap(),
        workload: Workload::Agent(AgentWorkload {
            agent: Agent::new(AgentKind::Claude, Some("fable".into())),
            extra_args: vec![],
            report_trailer: true,
            resume_thread: None,
        }),
        cwd: PathBuf::from("/work"),
        timeout: Duration::from_secs(4 * 3600),
        env: TaskEnv {
            path: "/bin".into(),
            home: "/home/u".into(),
        },
        binary: PathBuf::from("/bin/claude"),
        state,
        process_group_exit_evidence: ProcessGroupExitEvidence::Unconfirmed,
        container_exit_evidence: crate::domain::ContainerExitEvidence::Unconfirmed,
        child: None,
        check_due_at: None,
        cancel_requested_at: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[test]
fn direct_message_to_stopped_claude_session_needs_a_t3_owner() {
    let home = tempfile::tempdir().unwrap();
    let thread = row(TaskState::Queued).thread;
    let project = home.path().join(".claude/projects/-work");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join(format!("{thread}.jsonl")), "").unwrap();
    let context = CallbackContext {
        env: TaskEnv {
            path: String::new(),
            home: home.path().to_string_lossy().into_owned(),
        },
        cwd: home.path().to_path_buf(),
        // a Claude destination must not depend on the Codex executable
        codex: crate::submission::CallbackExecutable::Unavailable {
            reason: "codex is not installed".into(),
        },
    };
    assert!(matches!(
        find_saved_origin(&context, thread),
        Ok(OriginSession::Stopped)
    ));
    let log = home.path().join("callback.log");
    let lock = home.path().join("delivery.lock");
    let pending = PendingT3Send::new(home.path().join("t3-pending"));
    let error = send_saved_queue_attempt(
        &context,
        thread,
        "HOMEBASED_MESSAGE {}",
        &log,
        &lock,
        &pending,
    )
    .unwrap_err();
    assert_eq!(
        error,
        format!("Claude session {thread} is not running; no T3 thread owns it")
    );
    assert!(!log.exists());
}

fn report(seq: i64, outcome: ReportOutcome) -> TaskReport {
    TaskReport {
        seq,
        outcome,
        summary: format!("s{seq}"),
        reported_at: Utc::now(),
        notified_at: None,
    }
}

#[test]
fn workload_view_publishes_reasoning_level() {
    let workload = Workload::Agent(AgentWorkload {
        agent: Agent::new(AgentKind::Codex, Some("gpt-5.6-luna".into())),
        extra_args: vec!["--config".into(), "model_reasoning_effort=\"max\"".into()],
        report_trailer: true,
        resume_thread: None,
    });
    let json = serde_json::to_value(WorkloadView::from(&workload)).unwrap();
    assert_eq!(
        json,
        serde_json::json!({
            "type": "agent",
            "agent": "codex",
            "model": "gpt-5.6-luna",
            "reasoning": "max"
        })
    );
}

#[test]
fn workload_view_omits_unset_reasoning() {
    let json = serde_json::to_value(WorkloadView::from(&row(TaskState::Queued).workload)).unwrap();
    assert_eq!(
        json,
        serde_json::json!({"type": "agent", "agent": "claude", "model": "fable"})
    );
}

#[test]
fn cancelled_wins() {
    let event = exit_event(
        &row(TaskState::Finished {
            reason: ExitReason::Cancelled,
        }),
        &[report(1, ReportOutcome::Succeeded)],
        PathBuf::from("/e"),
        None,
    );
    assert_eq!(event.event, EventKind::TaskCancelled);
    assert_eq!(event.next_action, NextAction::None);
}

#[test]
fn lost_runner_event() {
    let event = lost_event(&row(TaskState::Lost), &[], PathBuf::from("/e"));
    assert_eq!(event.event, EventKind::TaskLost);
    assert!(matches!(event.process, Some(ProcessPayload::RunnerLost)));
}

#[test]
fn blocked_then_succeeded() {
    let event = exit_event(
        &row(TaskState::Finished {
            reason: ExitReason::Exit { code: 0 },
        }),
        &[
            report(1, ReportOutcome::Blocked),
            report(2, ReportOutcome::Succeeded),
        ],
        PathBuf::from("/e"),
        None,
    );
    assert_eq!(event.event, EventKind::TaskSucceeded);
    assert_eq!(event.reports.len(), 2);
}

fn codex_luna(extra_args: &[String]) -> Workload {
    Workload::Agent(AgentWorkload {
        agent: Agent::new(AgentKind::Codex, Some("gpt-5.6-luna".into())),
        extra_args: extra_args.to_vec(),
        report_trailer: true,
        resume_thread: None,
    })
}

#[test]
fn reasoning_comes_from_agent_argv() {
    let cases: &[(&[&str], Option<&str>)] = &[
        (&[], None),
        (&["--config", "model_reasoning_effort=\"max\""], Some("max")),
        (&["-c", "model_reasoning_effort=max"], Some("max")),
        (&["-c", "model_reasoning_effort=\"low\""], Some("low")),
        (&["--config=model_reasoning_effort='xhigh'"], Some("xhigh")),
        (&["--effort", "high"], Some("high")),
        (&["--reasoning-effort", "high"], Some("high")),
        (&["--effort=medium"], Some("medium")),
        (
            &[
                "-c",
                "model_reasoning_effort=\"low\"",
                "--config",
                "sandbox_mode=\"danger-full-access\"",
                "-c",
                "model_reasoning_effort=\"max\"",
            ],
            Some("max"),
        ),
        (&["--add-dir", "/tmp"], None),
        (&["-c", "model=\"gpt-5.6-luna\""], None),
        (&["--effort", "--add-dir"], None),
    ];
    for (args, expected) in cases {
        let owned: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        let view = WorkloadView::from(&codex_luna(&owned));
        let WorkloadView::Agent { reasoning, .. } = view else {
            panic!("agent view");
        };
        assert_eq!(reasoning.as_deref(), *expected, "{args:?}");
    }
}

#[test]
fn no_reports_exit_zero_fails_a_reporting_agent_with_a_derived_reason() {
    let event = exit_event(
        &row(TaskState::Finished {
            reason: ExitReason::Exit { code: 0 },
        }),
        &[],
        PathBuf::from("/e"),
        None,
    );
    assert_eq!(event.event, EventKind::TaskFailed);
    assert_eq!(event.next_action, NextAction::InspectLog);
    // the reason is derived by readers, never produced into the event itself
    assert_eq!(event.reason, None);
    assert_eq!(
        event.clone().with_derived_reason().reason,
        Some(EventReason::NoReport)
    );
    assert!(event.reports.is_empty());
    assert!(matches!(
        event.workload,
        WorkloadView::Agent {
            agent: AgentKind::Claude,
            ..
        }
    ));
}

fn waiting_report(seq: i64, on: TaskId) -> TaskReport {
    report(
        seq,
        ReportOutcome::Waiting(WaitingReport {
            on: WaitTargets::new(vec![on]).unwrap(),
            notes: WaitingNotes::new("next steps".into()).unwrap(),
        }),
    )
}

/// Every row of the exit classification table
#[test]
fn exit_classification_table() {
    use ExitClass::{Blocked, Cancelled, Failed, Lost, NoReport, Parked, Succeeded};
    use ExitPolicy::{NotAgent, ReportingAgent, SilentAgent};

    let target = TaskId::new();
    let exit = |code| Some(ProcessPayload::Exit { code });
    let signal = Some(ProcessPayload::Signal { signal: 9 });
    let waiting = [waiting_report(1, target)];
    let blocked = [report(1, ReportOutcome::Blocked)];
    let failed = [report(1, ReportOutcome::Failed)];
    let succeeded = [report(1, ReportOutcome::Succeeded)];
    let none: [TaskReport; 0] = [];
    let cases: &[(Option<ProcessPayload>, &[TaskReport], ExitPolicy, ExitClass)] = &[
        (
            Some(ProcessPayload::Cancelled),
            &waiting,
            ReportingAgent,
            Cancelled,
        ),
        (
            Some(ProcessPayload::RunnerLost),
            &succeeded,
            ReportingAgent,
            Lost,
        ),
        (exit(0), &waiting, ReportingAgent, Parked),
        (exit(0), &waiting, SilentAgent, Parked),
        (exit(1), &waiting, ReportingAgent, Failed),
        (signal.clone(), &waiting, ReportingAgent, Failed),
        (exit(0), &blocked, ReportingAgent, Blocked),
        (exit(3), &blocked, ReportingAgent, Blocked),
        (exit(1), &succeeded, ReportingAgent, Failed),
        (signal, &none, NotAgent, Failed),
        (exit(0), &failed, ReportingAgent, Failed),
        (exit(0), &succeeded, ReportingAgent, Succeeded),
        (exit(0), &none, ReportingAgent, NoReport),
        (exit(0), &none, SilentAgent, Succeeded),
        (exit(0), &none, NotAgent, Succeeded),
    ];
    for (process, reports, policy, expected) in cases {
        assert_eq!(
            classify_exit(process.as_ref(), reports, *policy, false),
            *expected,
            "{process:?} {reports:?} {policy:?}"
        );
    }

    // a requested cancel stops only parking; a finished report still wins
    let cancel_requested = [
        (&waiting[..], Cancelled),
        (&succeeded[..], Succeeded),
        (&blocked[..], Blocked),
    ];
    for (reports, expected) in cancel_requested {
        assert_eq!(
            classify_exit(exit(0).as_ref(), reports, ReportingAgent, true),
            expected,
            "cancel requested, {reports:?}"
        );
    }
}

#[test]
fn a_parked_exit_names_its_targets_and_continuation_and_asks_nothing() {
    let target = TaskId::new();
    let finished = row(TaskState::Finished {
        reason: ExitReason::Exit { code: 0 },
    });
    let parking = Parking {
        on: WaitTargets::new(vec![target]).unwrap(),
        continuation: TaskId::new(),
    };
    let reports = [waiting_report(1, target)];
    let event = exit_event(&finished, &reports, PathBuf::from("/e"), Some(&parking));
    assert_eq!(event.event, EventKind::TaskWaiting);
    assert_eq!(event.next_action, NextAction::None);
    assert_eq!(event.waiting_on, Some(parking.on.clone()));
    assert_eq!(event.continuation, Some(parking.continuation));
    let line = message_line(&event);
    assert!(line.contains("\"event\":\"TASK_WAITING\""));
    assert!(line.contains("\"outcome\":\"waiting\""));

    // without a saved handover nothing resumes the work, so it reads as failed
    let event = exit_event(&finished, &reports, PathBuf::from("/e"), None);
    assert_eq!(event.event, EventKind::TaskFailed);
    assert_eq!(event.waiting_on, None);
}

#[test]
fn check_due_has_null_process() {
    let r = row(TaskState::Running { pid: Some(1) });
    let event = check_due_event(&r, &[], PathBuf::from("/e"));
    assert_eq!(event.event, EventKind::TaskCheckDue);
    assert!(event.process.is_none());
    assert_eq!(event.next_action, NextAction::InspectTask);
    assert_eq!(event.timeout_secs, Some(4 * 3600));
    let line = message_line(&event);
    assert!(line.contains("\"process\":null"));
    assert!(line.contains("TASK_CHECK_DUE"));
}

#[test]
fn notify_has_null_process() {
    let r = row(TaskState::Running { pid: Some(1) });
    let report = report(1, ReportOutcome::Blocked);
    let event = notify_event(&r, &report, PathBuf::from("/e"));
    assert_eq!(event.event, EventKind::TaskReported);
    assert!(event.process.is_none());
    let line = message_line(&event);
    assert!(line.starts_with("HOMEBASED_EVENT {"));
    assert!(line.contains("\"process\":null"));
    assert!(line.contains("\"api_version\":1"));
}

#[test]
fn last_event_uses_the_latest_delivery_timestamp() {
    let now = Utc::now();
    let mut r = row(TaskState::Running { pid: Some(1) });
    r.check_due_at = Some(now);
    let mut later_report = report(1, ReportOutcome::Blocked);
    later_report.notified_at = Some(now + chrono::TimeDelta::seconds(1));
    let event = last_event_for_row(&r, &[later_report], PathBuf::from("/e"), None).unwrap();
    assert_eq!(event.event, EventKind::TaskReported);

    let mut earlier_report = report(2, ReportOutcome::Succeeded);
    earlier_report.notified_at = Some(now - chrono::TimeDelta::seconds(1));
    let event = last_event_for_row(&r, &[earlier_report], PathBuf::from("/e"), None).unwrap();
    assert_eq!(event.event, EventKind::TaskCheckDue);
}

#[test]
fn key_order_starts_with_api_version_event_task() {
    let event = exit_event(
        &row(TaskState::Finished {
            reason: ExitReason::Exit { code: 0 },
        }),
        &[report(1, ReportOutcome::Succeeded)],
        PathBuf::from("/e"),
        None,
    );
    let line = message_line(&event);
    let json = line.strip_prefix("HOMEBASED_EVENT ").unwrap();
    let start = &json[..80];
    assert!(start.starts_with("{\"api_version\":1,\"event\":\"TASK_SUCCEEDED\",\"task\":"));
}

#[test]
fn queue_attempt_deadline_stops_the_child_and_returns() {
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("slow-queue.sh");
    let pid_file = dir.path().join("pid");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\necho $$ > '{}'\n# flood stdout so an undrained pipe would block\n\
             dd if=/dev/zero bs=1024 count=256 2>/dev/null\n\
             sleep 1000\n",
            pid_file.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).unwrap();
    }

    let mut cmd = Command::new(&script);
    let started = Instant::now();
    let err = run_unconditionally(
        &mut cmd,
        Duration::from_secs(1),
        &dir.path().join("delivery.lock"),
    )
    .unwrap_err();
    let elapsed = started.elapsed();
    assert!(
        err.contains("timed out"),
        "expected timeout error, got {err}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "send returned too slowly: {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(900),
        "deadline returned too early: {elapsed:?}"
    );

    // child (and its sleep descendant) must be gone
    let pid_raw = std::fs::read_to_string(&pid_file).unwrap();
    let pid: i32 = pid_raw.trim().parse().unwrap();
    let still_alive = kill(Pid::from_raw(pid), None).is_ok();
    assert!(!still_alive, "timed-out child pid={pid} is still alive");
}

#[test]
fn delivery_lock_serializes_queue_processes() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("gate");
    let entered = dir.path().join("entered");
    let order = dir.path().join("order");
    let delivery_lock = dir.path().join("delivery.lock");
    std::fs::write(&gate, "closed").unwrap();

    let first_lock = delivery_lock.clone();
    let first_gate = gate.clone();
    let first_entered = entered.clone();
    let first_order = order.clone();
    let first = thread::spawn(move || {
        let mut cmd = Command::new("/bin/sh");
        cmd.args([
            "-c",
            "touch \"$ENTERED\"; while test -e \"$GATE\"; do sleep 0.02; done; printf 'first\\n' >> \"$ORDER\"",
        ])
        .env("ENTERED", first_entered)
        .env("GATE", first_gate)
        .env("ORDER", first_order);
        run_unconditionally(&mut cmd, Duration::from_secs(3), &first_lock).unwrap()
    });
    while !entered.exists() {
        thread::sleep(Duration::from_millis(10));
    }

    let second_lock = delivery_lock.clone();
    let second_order = order.clone();
    let second = thread::spawn(move || {
        let mut cmd = Command::new("/bin/sh");
        cmd.args(["-c", "printf 'second\\n' >> \"$ORDER\""])
            .env("ORDER", second_order);
        run_unconditionally(&mut cmd, Duration::from_secs(3), &second_lock).unwrap()
    });
    thread::sleep(Duration::from_millis(100));
    assert!(
        !order.exists(),
        "second sender passed the held delivery lock"
    );

    std::fs::remove_file(gate).unwrap();
    assert!(first.join().unwrap().status.success());
    assert!(second.join().unwrap().status.success());
    assert_eq!(std::fs::read_to_string(order).unwrap(), "first\nsecond\n");
}
