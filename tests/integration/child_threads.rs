//! Child threads end to end: `t3 launch --child`, the parent's view of its
//! children, `thread report`, and the settle that follows, against a fake T3

use super::fake_t3::{FakeT3, ask, install_t3, set_run};
use super::{Harness, wait_until};
use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::process::Output;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// The T3 thread every test launches children from, and its Codex thread
const PARENT: (&str, &str) = (
    "0199aaaa-0000-7000-8000-00000000000a",
    "0199aaaa-1111-7000-8000-00000000000a",
);
const CHILD_SESSION: &str = "0199aaaa-1111-7000-8000-00000000000b";
const SECOND_SESSION: &str = "0199aaaa-1111-7000-8000-00000000000c";
const GRANDCHILD_SESSION: &str = "0199aaaa-1111-7000-8000-00000000000d";
const GREAT_SESSION: &str = "0199aaaa-1111-7000-8000-00000000000e";

/// A harness whose daemon scans every second, a fake T3 that applies
/// launches and settles to its state, and the parent thread [`PARENT`]
fn child_harness() -> (Harness, FakeT3) {
    let mut h = Harness::new();
    let t3 = FakeT3::start(Some(h.user_home.join(".t3/userdata/statev2.sqlite")));
    install_t3(&h.user_home, &t3.origin);
    h.t3_thread(PARENT.0, "parent", Some(PARENT.1));
    h.codex_session(PARENT.1, &h.user_home, 1_000);
    h.restart_daemon_with_env(&[("HOMEBASED_TEST_SCAN_SECS", "1")]);
    (h, t3)
}

impl Harness {
    /// Run a command as the agent of T3 thread `t3`, in Codex thread `session`
    fn as_agent(&self, (t3, session): (&str, &str), args: &[&str]) -> Output {
        self.cmd()
            .env("T3CODE_THREAD_ID", t3)
            .env("CODEX_THREAD_ID", session)
            .arg("--json")
            .args(args)
            .output()
            .unwrap()
    }

    /// Launch a child thread of `parent`, and return the launch's output
    fn launch_child(&self, parent: (&str, &str), message: &str) -> Value {
        let request = uuid::Uuid::now_v7().to_string();
        succeeded(&self.as_agent(
            parent,
            &[
                "t3",
                "launch",
                "--project",
                "app",
                "--message",
                message,
                "--child",
                "--request-id",
                &request,
            ],
        ))
    }

    /// Give the launched thread `t3` a Codex thread to run in, idle for ten minutes
    fn child_agent(&self, t3: &str, session: &str) {
        self.t3_session(t3, session);
        self.codex_session(session, &self.user_home, 1_000);
    }

    fn child_run(&self, t3: &str, run: &str, status: &str) {
        set_run(&self.t3_state(), t3, run, status);
    }

    fn report(&self, child: (&str, &str), text: &str) -> Output {
        self.as_agent(child, &["thread", "report", "--summary", text])
    }

    /// The children `parent` lists with `thread context --children`
    fn listed_children(&self, parent: (&str, &str)) -> Vec<Value> {
        let listed = succeeded(&self.as_agent(parent, &["thread", "context", "--children"]));
        listed["children"].as_array().unwrap().clone()
    }

    /// The context of T3 thread `t3` on this machine
    fn t3_context(&self, t3: &str) -> Value {
        let reference = format!("thread_{t3}");
        let found = succeeded(&self.as_agent(PARENT, &["thread", "context", &reference]));
        found["threads"][0].clone()
    }

    /// How far the report of the child `t3` has got, on its own machine
    fn report_stage(&self, t3: &str) -> Value {
        self.t3_context(t3)["child_thread"]["report"].clone()
    }

    /// Stored payloads of `watch`'s events, in order
    fn child_watch_events(&self, watch: &str) -> Vec<Value> {
        let db = rusqlite::Connection::open(self.home.join(homebased::home::DB_NAME)).unwrap();
        let mut statement = db
            .prepare("SELECT event_json FROM thread_watch_events WHERE watch_id = ?1 ORDER BY seq")
            .unwrap();
        statement
            .query_map([watch], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|event| serde_json::from_str::<Value>(&event.unwrap()).unwrap()["payload"].clone())
            .collect()
    }

    /// Why `watch` ended, or `None` while it is live
    fn child_watch_end(&self, watch: &str) -> Option<String> {
        let all = succeeded(&self.as_agent(PARENT, &["thread", "watches", "--all"]));
        all["watches"]
            .as_array()
            .unwrap()
            .iter()
            .find(|listed| listed["watch"] == watch)
            .unwrap_or_else(|| panic!("{watch} is not listed: {all}"))["end_reason"]
            .as_str()
            .map(str::to_owned)
    }
}

/// Messages T3 was asked to put in `thread`
fn messages_to(t3: &FakeT3, thread: &str) -> Vec<Value> {
    t3.dispatched("message.dispatch")
        .into_iter()
        .filter(|payload| payload["threadId"] == thread)
        .collect()
}

fn settles_of(t3: &FakeT3, thread: &str) -> Vec<Value> {
    t3.dispatched("thread.settle")
        .into_iter()
        .filter(|payload| payload["threadId"] == thread)
        .collect()
}

fn launched_thread(launched: &Value) -> String {
    launched["thread"]["thread_id"].as_str().unwrap().to_owned()
}

fn succeeded(out: &Output) -> Value {
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

/// The error `out` failed with, after checking its exit code and error code
fn failed(out: &Output, exit: i32, code: &str) -> Value {
    let error: Value = serde_json::from_slice(&out.stderr).unwrap_or(Value::Null);
    assert_eq!(out.status.code(), Some(exit), "{error}");
    assert_eq!(error["error"]["code"], code, "{error}");
    error["error"].clone()
}

#[test]
fn a_child_keeps_its_parent_waiting_until_it_reports_and_settles_once_its_run_ends() {
    let (h, t3) = child_harness();
    let launched = h.launch_child(PARENT, "Fix the flaky upload test");
    let child = launched_thread(&launched);
    assert_eq!(launched["lineage"]["parent"]["t3_thread"], PARENT.0);
    assert_eq!(launched["lineage"]["child"]["t3_thread"], child.as_str());
    let text = t3.launches()[0]["payload"]["initialMessage"]["text"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(text.starts_with("Fix the flaky upload test\n"), "{text}");
    assert!(text.contains("homebased thread report"), "{text}");
    assert!(text.contains(&format!("thread_{}", PARENT.0)), "{text}");
    h.child_agent(&child, CHILD_SESSION);
    h.child_run(&child, "run-c-1", "running");

    let children = h.listed_children(PARENT);
    assert_eq!(children.len(), 1, "{children:?}");
    assert_eq!(children[0]["t3_thread"], child.as_str());
    assert_eq!(children[0]["child_thread"]["parent"]["t3_thread"], PARENT.0);
    assert_eq!(children[0]["child_thread"]["reported"], false);
    assert_eq!(h.t3_context(PARENT.0)["state"], "waiting");

    let body = "Fixed the race in the upload retry";
    let reported = succeeded(&h.report((&child, CHILD_SESSION), body));
    assert_eq!(reported["resumed"], false, "{reported}");
    assert_eq!(reported["report"]["stage"], "settle_pending", "{reported}");
    let delivered = messages_to(&t3, PARENT.0);
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    let delivered = delivered[0]["text"].as_str().unwrap();
    assert!(delivered.contains(body), "{delivered}");
    assert!(
        delivered.contains(&format!("Report from child thread thread_{child}")),
        "{delivered}"
    );
    assert_eq!(h.t3_context(PARENT.0)["state"], "idle");
    assert_eq!(
        h.listed_children(PARENT)[0]["child_thread"]["reported"],
        true
    );

    // a retry is the same report, and other text from the same run is refused
    let again = succeeded(&h.report((&child, CHILD_SESSION), body));
    assert_eq!(again["resumed"], true, "{again}");
    assert_eq!(
        again["report"]["message_id"],
        reported["report"]["message_id"]
    );
    let conflict = failed(
        &h.report((&child, CHILD_SESSION), "Something else"),
        5,
        "thread_report_conflict",
    );
    assert_eq!(conflict["input"]["reason"], "same_run", "{conflict}");
    assert_eq!(messages_to(&t3, PARENT.0).len(), 1);

    // the reporting run is still going, so the child is not settled under it
    std::thread::sleep(Duration::from_secs(3));
    assert!(settles_of(&t3, &child).is_empty());
    assert_eq!(h.report_stage(&child), "settle_pending");
    h.child_run(&child, "run-c-1", "completed");
    assert!(
        wait_until(Duration::from_secs(20), || h.report_stage(&child)
            == "settled"),
        "the child was not settled"
    );
    let settles = settles_of(&t3, &child);
    assert_eq!(settles.len(), 1, "{settles:?}");
    assert_eq!(settles[0]["expectedLatestRunId"], "run-c-1");

    // a user reopens the settled child, and its later run sends a follow-up
    h.t3_state()
        .execute(
            "UPDATE orchestration_v2_projection_threads
             SET payload_json = json_remove(payload_json, '$.settledOverride')
             WHERE thread_id = ?1",
            [child.as_str()],
        )
        .unwrap();
    h.child_run(&child, "run-c-2", "running");
    let more = "Also fixed the retry timeout";
    let follow_up = succeeded(&h.report((&child, CHILD_SESSION), more));
    assert_eq!(follow_up["resumed"], false, "{follow_up}");
    assert_eq!(follow_up["report"]["seq"], 1, "{follow_up}");
    assert_eq!(follow_up["report"]["stage"], "settle_pending");
    assert_ne!(
        follow_up["report"]["message_id"],
        reported["report"]["message_id"]
    );
    let delivered = messages_to(&t3, PARENT.0);
    assert_eq!(delivered.len(), 2, "{delivered:?}");
    let delivered = delivered[1]["text"].as_str().unwrap();
    assert!(delivered.contains(more), "{delivered}");
    assert!(
        delivered.contains(&format!(
            "Follow-up report from child thread thread_{child}"
        )),
        "{delivered}"
    );
    // the parent waited on the first report only
    assert_eq!(h.t3_context(PARENT.0)["state"], "idle");
    assert_eq!(h.report_stage(&child), "settled");

    // the follow-up retries like the first report, and settles after its own run
    let again = succeeded(&h.report((&child, CHILD_SESSION), more));
    assert_eq!(again["resumed"], true, "{again}");
    failed(
        &h.report((&child, CHILD_SESSION), "A third thing"),
        5,
        "thread_report_conflict",
    );
    assert_eq!(messages_to(&t3, PARENT.0).len(), 2);
    h.child_run(&child, "run-c-2", "completed");
    assert!(
        wait_until(Duration::from_secs(20), || settles_of(&t3, &child).len()
            == 2),
        "the child was not settled after its follow-up"
    );
    assert_eq!(settles_of(&t3, &child)[1]["expectedLatestRunId"], "run-c-2");

    // the same text from yet another run is a new follow-up, not a retry
    h.t3_state()
        .execute(
            "UPDATE orchestration_v2_projection_threads
             SET payload_json = json_remove(payload_json, '$.settledOverride')
             WHERE thread_id = ?1",
            [child.as_str()],
        )
        .unwrap();
    h.child_run(&child, "run-c-3", "running");
    let resent = succeeded(&h.report((&child, CHILD_SESSION), more));
    assert_eq!(resent["resumed"], false, "{resent}");
    assert_eq!(resent["report"]["seq"], 2, "{resent}");
    assert_eq!(messages_to(&t3, PARENT.0).len(), 3);

    // a message the user sends the child after its report keeps it unsettled
    let second = launched_thread(&h.launch_child(PARENT, "Check the upload metrics"));
    h.child_agent(&second, SECOND_SESSION);
    h.child_run(&second, "run-s-1", "running");
    succeeded(&h.report((&second, SECOND_SESSION), "The metrics look right"));
    let later =
        (Utc::now() + ChronoDuration::minutes(1)).to_rfc3339_opts(SecondsFormat::Millis, true);
    h.t3_state()
        .execute(
            "INSERT INTO orchestration_v2_projection_messages
             (message_id, thread_id, role, created_at, payload_json)
             VALUES ('message-after-report', ?1, 'user', ?2, '{}')",
            [second.as_str(), later.as_str()],
        )
        .unwrap();
    h.child_run(&second, "run-s-1", "completed");
    assert!(
        wait_until(Duration::from_secs(20), || h.report_stage(&second)
            == "sent"),
        "the report did not finish"
    );
    assert!(settles_of(&t3, &second).is_empty());
}

#[test]
fn a_child_with_open_work_cannot_report_until_its_own_child_reports_and_its_task_ends() {
    let (h, t3) = child_harness();
    let child = launched_thread(&h.launch_child(PARENT, "Port the uploader"));
    h.child_agent(&child, CHILD_SESSION);
    h.child_run(&child, "run-c-1", "running");
    let launched = h.launch_child((&child, CHILD_SESSION), "Port the retry module");
    let grandchild = launched_thread(&launched);
    assert_eq!(launched["lineage"]["parent"]["t3_thread"], child.as_str());
    h.child_agent(&grandchild, GRANDCHILD_SESSION);
    h.child_run(&grandchild, "run-g-1", "running");

    // each thread lists its own children only
    let of_child = h.listed_children((&child, CHILD_SESSION));
    assert_eq!(of_child.len(), 1, "{of_child:?}");
    assert_eq!(of_child[0]["t3_thread"], grandchild.as_str());
    let of_parent = h.listed_children(PARENT);
    assert_eq!(of_parent.len(), 1, "{of_parent:?}");
    assert_eq!(of_parent[0]["t3_thread"], child.as_str());

    let mut spec = h.gated_target("child-task", 0);
    spec["thread"] = json!(CHILD_SESSION);
    let task = h.submit(&spec);
    let slept = h.as_agent((&child, CHILD_SESSION), &["wait", "--long", &task]);
    assert!(
        slept.status.success(),
        "{}",
        String::from_utf8_lossy(&slept.stderr)
    );
    let refusal = failed(
        &h.report((&child, CHILD_SESSION), "Ported the uploader"),
        5,
        "thread_report_refused",
    );
    let open = refusal["input"]["open"].as_array().unwrap();
    assert!(
        open.iter().any(
            |work| work["kind"] == "child" && work["child"]["t3_thread"] == grandchild.as_str()
        ),
        "{refusal}"
    );
    assert!(
        open.iter()
            .any(|work| work["kind"] == "task" && work["id"] == task.as_str()),
        "{refusal}"
    );
    // the sleep's hold is open work of its own until the final event ends it
    assert!(
        open.iter()
            .any(|work| work["kind"] == "hold" && work["id"] == task.as_str()),
        "{refusal}"
    );
    assert!(messages_to(&t3, PARENT.0).is_empty());

    succeeded(&h.report((&grandchild, GRANDCHILD_SESSION), "Ported the retry module"));
    assert_eq!(
        messages_to(&t3, &child)
            .iter()
            .filter(|message| message["text"]
                .as_str()
                .is_some_and(|text| text.contains("Ported the retry module")))
            .count(),
        1
    );
    h.release_gate("child-task");
    // the task's final event reaches the child a moment after the task ends
    let mut last = None;
    let reported = wait_until(Duration::from_secs(30), || {
        let out = h.report((&child, CHILD_SESSION), "Ported the uploader");
        let done = out.status.success();
        last = Some(String::from_utf8_lossy(&out.stderr).into_owned());
        done
    });
    assert!(reported, "the child could not report: {last:?}");
    let delivered = messages_to(&t3, PARENT.0);
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    assert!(
        delivered[0]["text"]
            .as_str()
            .unwrap()
            .contains("Ported the uploader")
    );
}

#[test]
fn a_report_whose_delivery_is_unknown_goes_again_as_the_same_message() {
    let (mut h, t3) = child_harness();
    let child = launched_thread(&h.launch_child(PARENT, "Fix the flaky upload test"));
    h.child_agent(&child, CHILD_SESSION);
    h.child_run(&child, "run-c-1", "running");
    let body = "Fixed the race in the upload retry";

    t3.behavior
        .drop_message_replies
        .store(true, Ordering::Relaxed);
    failed(
        &h.report((&child, CHILD_SESSION), body),
        1,
        "message_delivery_failed",
    );
    failed(
        &h.report((&child, CHILD_SESSION), body),
        1,
        "message_delivery_failed",
    );
    assert_eq!(h.report_stage(&child), "sending");
    // the saved report is fixed, whatever its delivery
    let conflict = failed(
        &h.report((&child, CHILD_SESSION), "Something else"),
        5,
        "thread_report_conflict",
    );
    assert_eq!(conflict["input"]["reason"], "undelivered", "{conflict}");

    t3.behavior
        .drop_message_replies
        .store(false, Ordering::Relaxed);
    h.restart_daemon_with_env(&[("HOMEBASED_TEST_SCAN_SECS", "1")]);
    assert!(
        wait_until(Duration::from_secs(20), || h.report_stage(&child)
            == "settle_pending"),
        "the restarted daemon did not finish the send"
    );
    let resumed = succeeded(&h.report((&child, CHILD_SESSION), body));
    assert_eq!(resumed["resumed"], true, "{resumed}");

    // every attempt was the same T3 command, which T3 runs once
    let attempts = messages_to(&t3, PARENT.0);
    assert!(attempts.len() >= 3, "{attempts:?}");
    let commands: HashSet<&str> = attempts
        .iter()
        .map(|attempt| attempt["commandId"].as_str().unwrap())
        .collect();
    assert_eq!(commands.len(), 1, "{attempts:?}");
    assert_eq!(h.t3_context(PARENT.0)["state"], "idle");
}

#[test]
fn an_idle_unreported_child_stalls_and_its_watch_ends_once_it_reports_and_settles() {
    let (h, t3) = child_harness();
    let child = launched_thread(&h.launch_child(PARENT, "Fix the flaky upload test"));
    // its last activity is ten minutes old, and no run is going
    h.child_agent(&child, CHILD_SESSION);
    let watched = succeeded(&h.as_agent(
        PARENT,
        &["thread", "watch", "--children", "--stalled-after", "5m"],
    ));
    let watches = watched["watches"].as_array().unwrap();
    assert_eq!(watches.len(), 1, "{watched}");
    let watch = watches[0]["watch"].as_str().unwrap().to_owned();

    assert!(
        wait_until(Duration::from_secs(20), || !h
            .child_watch_events(&watch)
            .is_empty()),
        "the idle child did not stall"
    );
    let fired = &h.child_watch_events(&watch)[0];
    assert_eq!(fired["event"], "THREAD_STALLED", "{fired}");
    assert_eq!(fired["watch_ended"], false, "{fired}");

    h.child_run(&child, "run-c-1", "running");
    succeeded(&h.report(
        (&child, CHILD_SESSION),
        "Fixed the race in the upload retry",
    ));
    h.child_run(&child, "run-c-1", "completed");
    assert!(
        wait_until(Duration::from_secs(20), || h
            .child_watch_end(&watch)
            .as_deref()
            == Some("finished")),
        "the watch did not end: {:?}",
        h.child_watch_events(&watch)
    );
    assert_eq!(settles_of(&t3, &child).len(), 1);
    let stalls = h
        .child_watch_events(&watch)
        .iter()
        .filter(|event| event["event"] == "THREAD_STALLED")
        .count();
    assert_eq!(stalls, 1);
}

#[test]
fn a_parent_hears_once_when_its_child_waits_on_a_question_and_can_answer_it() {
    let (h, t3) = child_harness();
    let child = launched_thread(&h.launch_child(PARENT, "Pick a database"));
    h.child_agent(&child, CHILD_SESSION);
    h.child_run(&child, "run-c-1", "waiting");
    let questions = json!([{
        "id": "Which database?",
        "header": "Database",
        "question": "Which database?",
        "options": [{ "label": "Postgres" }, { "label": "SQLite" }]
    }]);
    ask(&h.t3_state(), &child, "request-1", &questions);

    let alerted = |t3: &FakeT3| -> Vec<Value> {
        messages_to(t3, PARENT.0)
            .into_iter()
            .filter(|message| {
                message["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("is waiting on a question"))
            })
            .collect()
    };
    assert!(
        wait_until(Duration::from_secs(30), || !alerted(&t3).is_empty()),
        "the parent heard nothing"
    );
    let text = alerted(&t3)[0]["text"].as_str().unwrap().to_owned();
    assert!(
        text.contains(&format!(
            "thread_{child} is waiting on a question (request request-1)"
        )),
        "{text}"
    );
    assert!(text.contains("Database: Which database?"), "{text}");
    assert!(text.contains("Options: Postgres | SQLite"), "{text}");
    assert!(
        text.contains(&format!(
            "homebased thread answer thread_{child} --request request-1"
        )),
        "{text}"
    );
    // later scans find the same request and send nothing more
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(alerted(&t3).len(), 1);

    let reference = format!("thread_{child}");
    let wrong = failed(
        &h.as_agent(
            PARENT,
            &[
                "thread",
                "answer",
                &reference,
                "--request",
                "request-2",
                "--answer",
                "x",
            ],
        ),
        5,
        "thread_control_refused",
    );
    assert!(
        wrong["message"]
            .as_str()
            .unwrap()
            .contains("it waits on request-1"),
        "{wrong}"
    );
    let answered = succeeded(&h.as_agent(
        PARENT,
        &[
            "thread",
            "answer",
            &reference,
            "--request",
            "request-1",
            "--answer",
            "sqlite",
        ],
    ));
    assert_eq!(answered["outcome"], "answered");
    let responses = t3.dispatched("runtime-request.respond");
    assert_eq!(responses.len(), 1, "{responses:?}");
    assert_eq!(responses[0]["threadId"], child.as_str());
    assert_eq!(responses[0]["requestId"], "request-1");
    assert_eq!(
        responses[0]["answers"],
        json!({ "Which database?": "SQLite" })
    );
}

#[test]
fn a_child_stops_its_unreported_children_by_interrupt_or_when_it_reports() {
    let (h, t3) = child_harness();
    let child = launched_thread(&h.launch_child(PARENT, "Port the uploader"));
    h.child_agent(&child, CHILD_SESSION);
    h.child_run(&child, "run-c-1", "running");
    let as_child = (child.as_str(), CHILD_SESSION);
    let first = launched_thread(&h.launch_child(as_child, "Port the retry module"));
    h.child_agent(&first, GRANDCHILD_SESSION);
    h.child_run(&first, "run-g-1", "running");
    let second = launched_thread(&h.launch_child(as_child, "Port the cache"));
    h.child_agent(&second, SECOND_SESSION);
    h.child_run(&second, "run-s-1", "running");
    let great = launched_thread(&h.launch_child((&second, SECOND_SESSION), "Port the cache keys"));
    h.child_agent(&great, GREAT_SESSION);
    h.child_run(&great, "run-k-1", "running");

    // an interrupt ends one run and leaves the child owing its report
    let first_ref = format!("thread_{first}");
    let interrupted = succeeded(&h.as_agent(as_child, &["thread", "interrupt", &first_ref]));
    assert_eq!(interrupted["outcome"], "interrupted");
    assert_eq!(interrupted["run"], "run-g-1");
    assert!(interrupted.get("closed_link").is_none(), "{interrupted}");
    let idle = succeeded(&h.as_agent(as_child, &["thread", "interrupt", &first_ref]));
    assert_eq!(idle["outcome"], "idle");
    let interrupts = t3.dispatched("run.interrupt");
    assert_eq!(interrupts.len(), 1, "{interrupts:?}");
    assert_eq!(interrupts[0]["runId"], "run-g-1");

    // a stop closes the link, so the stopped child no longer holds the report
    let stopped = succeeded(&h.as_agent(as_child, &["thread", "interrupt", "--stop", &first_ref]));
    assert_eq!(stopped["outcome"], "stopped");
    assert!(stopped["closed_link"].is_string(), "{stopped}");
    // a stopped child counts as done, so a live listing leaves it out
    assert_eq!(h.t3_context(&first)["child_thread"]["closed"], true);
    let live = succeeded(&h.as_agent(as_child, &["thread", "context", "--children", "--live"]));
    let live: Vec<&str> = live["children"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|listed| listed["t3_thread"].as_str())
        .collect();
    assert_eq!(live, vec![second.as_str()]);
    let refusal = failed(
        &h.report(as_child, "Ported the uploader"),
        5,
        "thread_report_refused",
    );
    let open = refusal["input"]["open"].as_array().unwrap();
    assert_eq!(open.len(), 1, "{refusal}");
    assert_eq!(open[0]["child"]["t3_thread"], second.as_str());

    let reported = succeeded(&h.as_agent(
        as_child,
        &[
            "thread",
            "report",
            "--summary",
            "Ported the uploader",
            "--stop-children",
        ],
    ));
    let stopped_children = reported["stopped"].as_array().unwrap();
    assert_eq!(stopped_children.len(), 1, "{reported}");
    assert_eq!(stopped_children[0]["t3_thread"], second.as_str());
    let stops: Vec<Value> = t3.dispatched("thread.stop");
    // stopping `second` stopped its own unreported child first
    assert_eq!(
        stops
            .iter()
            .map(|stop| stop["threadId"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        vec![first.clone(), great.clone(), second.clone()]
    );
    assert_eq!(h.t3_context(&great)["child_thread"]["closed"], true);
    assert!(stops.iter().all(|stop| stop["stopDelegatedTasks"] == true));
    let delivered = messages_to(&t3, PARENT.0);
    assert_eq!(delivered.len(), 1, "{delivered:?}");
}
