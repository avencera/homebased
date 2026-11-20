use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, params};
use tempfile::{TempDir, tempdir};
use uuid::Uuid;

use super::{
    Entry, EntryState, KeepReason, Limits, LiveReason, OutputKind, Owner, PassMode, PruneReason,
    RetentionConfig, Verdict, plan, run_pass,
};
use crate::domain::ThreadId;
use crate::home::Home;
use crate::store::Store;

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

fn days_ago(days: u32) -> DateTime<Utc> {
    Utc::now() - chrono::Duration::from_std(days * DAY).unwrap()
}

fn stamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// A state directory with a database, and helpers that write the few columns
/// retention reads straight into it
struct Fixture {
    _dir: TempDir,
    home: Home,
    conn: Connection,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempdir().unwrap();
        let home = Home::resolve(Some(dir.path().join("state"))).unwrap();
        home.ensure().unwrap();
        drop(Store::open(&home.db_path()).unwrap());
        let conn = Connection::open(home.db_path()).unwrap();
        Self {
            _dir: dir,
            home,
            conn,
        }
    }

    fn checkouts(&self) -> PathBuf {
        self.home.root().join("ci").join("checkouts")
    }

    /// A task row and its directory with a 4 KiB `output.log`, both dated `at`
    fn task(&self, thread: Uuid, status: &str, at: DateTime<Utc>, cwd: &Path) -> Uuid {
        let id = Uuid::now_v7();
        self.conn
            .execute(
                "INSERT INTO tasks (id, thread_id, name, workload_json, cwd, timeout_secs,
                     env_path, env_home, binary, status, created_at, updated_at,
                     process_group_exit_evidence)
                 VALUES (?1, ?2, 'job', '{\"type\":\"task\",\"command\":[\"true\"]}', ?3, '60',
                     '/bin', '/tmp', '/bin/true', ?4, ?5, ?5, 'unconfirmed')",
                params![
                    id.to_string(),
                    thread.to_string(),
                    cwd.to_string_lossy(),
                    status,
                    stamp(at)
                ],
            )
            .unwrap();
        let dir = self.home.tasks_dir().join(id.to_string());
        write_dated(&dir.join("output.log"), 4096, at);
        id
    }

    fn ended_task(&self, thread: Uuid, at: DateTime<Utc>) -> Uuid {
        self.task(thread, "succeeded", at, Path::new("/tmp"))
    }

    /// A succeeded queue job and its `state/` checkpoint
    fn job(&self, thread: Uuid, at: DateTime<Utc>) -> Uuid {
        let id = Uuid::now_v7();
        let machine = Uuid::now_v7().to_string();
        self.conn
            .execute(
                "INSERT INTO resource_jobs (id, machine, origin_machine, thread_id, spec_json,
                     spec_digest, env_path, env_home, priority, state, step, last_run_number,
                     event_seq, created_at, updated_at)
                 VALUES (?1, ?2, ?2, ?3, '{}', 'digest', '/bin', '/tmp', 1, 'succeeded', 0, 1, 0,
                     ?4, ?4)",
                params![id.to_string(), machine, thread.to_string(), stamp(at)],
            )
            .unwrap();
        let state = self.home.jobs_dir().join(id.to_string()).join("state");
        write_dated(&state.join("state.pt"), 64 * 1024, at);
        id
    }

    fn task_dir(&self, id: Uuid) -> PathBuf {
        self.home.tasks_dir().join(id.to_string())
    }

    fn pass(&self, config: &RetentionConfig, mode: PassMode) -> super::GcReport {
        run_pass(&self.home, &self.checkouts(), config, mode, &|| false).unwrap()
    }
}

/// Write `bytes` to `path` and date it and its parent directories at `at`
fn write_dated(path: &Path, bytes: usize, at: DateTime<Utc>) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, vec![7_u8; bytes]).unwrap();
    let time = SystemTime::from(at);
    File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(time)
        .unwrap();
    let mut dir = path.parent();
    while let Some(current) = dir {
        if current.file_name().is_some_and(|name| {
            ["tasks", "jobs", "checkouts"].contains(&name.to_string_lossy().as_ref())
        }) {
            break;
        }
        File::open(current).unwrap().set_modified(time).unwrap();
        dir = current.parent();
    }
}

#[test]
fn a_pass_prunes_only_ended_output_nothing_needs_and_tombstones_its_rows() {
    let fixture = Fixture::new();
    let config = RetentionConfig::default();
    let old = days_ago(20);

    // five old tasks of one thread: keep_last 3 keeps the newest three
    let thread = Uuid::now_v7();
    let mut history: Vec<Uuid> = (0..5)
        .map(|offset| fixture.ended_task(thread, old + chrono::Duration::hours(offset)))
        .collect();
    let kept_recent = history.split_off(2);
    let pruned_old = history;

    // old output that something still needs, each on a thread of its own
    let running = fixture.task(Uuid::now_v7(), "running", old, Path::new("/tmp"));
    let unsettled = fixture.ended_task(Uuid::now_v7(), old);
    fixture
        .conn
        .execute(
            "INSERT INTO origin_inbox (task_id, seq, event_json, delivery_json)
             VALUES (?1, 1, '{}', '{}')",
            [unsettled.to_string()],
        )
        .unwrap();
    let depended_on = fixture.ended_task(Uuid::now_v7(), old);
    fixture
        .conn
        .execute(
            "INSERT INTO origin_routes (request_id, task_id, route_json, after_json)
             VALUES (?1, ?2, '{}', ?3)",
            params![
                Uuid::now_v7().to_string(),
                Uuid::now_v7().to_string(),
                format!("[\"{depended_on}\"]")
            ],
        )
        .unwrap();
    let slept_on = fixture.ended_task(Uuid::now_v7(), old);
    fixture
        .conn
        .execute(
            "INSERT INTO sleep_holds (target_kind, target_id, thread_id, created_at)
             VALUES ('task', ?1, ?2, ?3)",
            params![
                slept_on.to_string(),
                Uuid::now_v7().to_string(),
                stamp(Utc::now())
            ],
        )
        .unwrap();
    let needed = [running, unsettled, depended_on, slept_on];

    // young tasks: one thread with four, so one is neither recent nor old
    let young_thread = Uuid::now_v7();
    let young: Vec<Uuid> = (0..4)
        .map(|offset| fixture.ended_task(young_thread, days_ago(5 - offset)))
        .collect();

    // a directory from an earlier database, and one whose worker still runs
    let legacy = fixture.task_dir(Uuid::now_v7());
    write_dated(&legacy.join("output.log"), 4096, old);
    let locked = fixture.task_dir(Uuid::now_v7());
    write_dated(&locked.join("runner.lock"), 0, old);
    let lock = nix::fcntl::Flock::lock(
        File::open(locked.join("runner.lock")).unwrap(),
        nix::fcntl::FlockArg::LockExclusiveNonblock,
    )
    .unwrap();

    // keep_last 1 keeps the thread's newer job
    let job_thread = Uuid::now_v7();
    let old_job = fixture.job(job_thread, old);
    let young_job = fixture.job(job_thread, days_ago(3));

    // an idle slot clone last used ten days ago, and a busy one
    let idle_clone = fixture
        .checkouts()
        .join("repo-0011223344556677")
        .join("rust-1");
    write_dated(
        &idle_clone.join("target").join("lib.rlib"),
        8192,
        days_ago(10),
    );
    fixture.task(Uuid::now_v7(), "succeeded", days_ago(10), &idle_clone);
    let busy_clone = fixture
        .checkouts()
        .join("repo-0011223344556677")
        .join("rust-2");
    write_dated(
        &busy_clone.join("target").join("lib.rlib"),
        8192,
        days_ago(10),
    );
    fixture.task(Uuid::now_v7(), "running", days_ago(10), &busy_clone);

    let dry = fixture.pass(&config, PassMode::DryRun);
    assert!(dry.dry_run);
    let planned: Vec<&Path> = dry
        .pruned
        .iter()
        .map(|entry| entry.path.as_path())
        .collect();
    assert_eq!(planned.len(), 5, "{planned:?}");
    assert!(
        fixture.task_dir(pruned_old[0]).exists(),
        "a dry run removes nothing"
    );

    let report = fixture.pass(&config, PassMode::Apply);
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(report.pruned.len(), 5);
    for id in &pruned_old {
        assert!(!fixture.task_dir(*id).exists());
    }
    assert!(!legacy.exists());
    assert!(!fixture.home.jobs_dir().join(old_job.to_string()).exists());
    assert!(!idle_clone.exists());

    for id in kept_recent.iter().chain(&needed).chain(&young) {
        assert!(fixture.task_dir(*id).exists(), "{id} must be kept");
    }
    assert!(locked.exists());
    assert!(fixture.home.jobs_dir().join(young_job.to_string()).exists());
    assert!(busy_clone.exists());

    let store = Store::open(&fixture.home.db_path()).unwrap();
    assert!(
        store
            .pruned_at(OutputKind::Task, pruned_old[0])
            .unwrap()
            .is_some()
    );
    assert!(store.pruned_at(OutputKind::Job, old_job).unwrap().is_some());
    assert!(
        store
            .pruned_at(OutputKind::Task, kept_recent[0])
            .unwrap()
            .is_none()
    );

    let task_kind = &report.kinds[0];
    assert_eq!(task_kind.kind, OutputKind::Task);
    // the idle clone's run is the newest task of its thread
    assert_eq!(task_kind.kept_recent, 3 + 3 + 1);
    // the locked directory and the busy clone's run are live too
    assert_eq!(task_kind.kept_live, needed.len() + 2);
    assert_eq!(task_kind.kept_young, 1);

    let again = fixture.pass(&config, PassMode::Apply);
    assert!(again.pruned.is_empty(), "{:?}", again.pruned);
    drop(lock);
}

#[test]
fn a_tombstoned_directory_left_by_an_interrupted_pass_is_removed_next_time() {
    let fixture = Fixture::new();
    let id = fixture.ended_task(Uuid::now_v7(), days_ago(1));
    fixture
        .conn
        .execute(
            "INSERT INTO pruned_outputs (kind, id, pruned_at, bytes, reason)
             VALUES ('task', ?1, ?2, 4096, 'age')",
            params![id.to_string(), stamp(Utc::now())],
        )
        .unwrap();

    let report = fixture.pass(&RetentionConfig::default(), PassMode::Apply);

    assert!(!fixture.task_dir(id).exists());
    assert_eq!(report.pruned[0].reason, PruneReason::Leftover);
}

fn ended(kind: OutputKind, thread: u128, days: u32, bytes: u64) -> Entry {
    Entry {
        kind,
        path: PathBuf::from(format!("/state/{thread}/{days}")),
        owner: Owner::Unreferenced,
        bytes,
        state: EntryState::Ended {
            at: days_ago(days),
            thread: Some(ThreadId(Uuid::from_u128(thread))),
        },
    }
}

#[test]
fn a_size_limit_prunes_the_oldest_young_output_but_never_inside_min_age_or_keep_last() {
    let mut config = RetentionConfig::default();
    config.jobs.limits = Limits {
        max_age: 14 * DAY,
        max_size: Some(250),
    };
    let entries = vec![
        // newest of its thread, so kept whatever the size
        ended(OutputKind::Job, 1, 2, 100),
        ended(OutputKind::Job, 1, 3, 100),
        ended(OutputKind::Job, 1, 5, 100),
        ended(OutputKind::Job, 2, 4, 100),
        // inside min_age
        Entry {
            state: EntryState::Ended {
                at: Utc::now() - chrono::Duration::hours(2),
                thread: None,
            },
            ..ended(OutputKind::Job, 3, 0, 100)
        },
        Entry {
            state: EntryState::Live {
                reason: LiveReason::Running,
            },
            ..ended(OutputKind::Job, 4, 30, 100)
        },
    ];

    let verdicts = plan(&config, Utc::now(), &entries);

    assert_eq!(
        verdicts,
        vec![
            Verdict::Keep(KeepReason::Recent),
            Verdict::Prune(PruneReason::Size),
            Verdict::Prune(PruneReason::Size),
            Verdict::Keep(KeepReason::Recent),
            Verdict::Keep(KeepReason::Young),
            Verdict::Keep(KeepReason::Live {
                reason: LiveReason::Running
            }),
        ]
    );
}
