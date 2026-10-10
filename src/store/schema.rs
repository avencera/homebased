//! SQLite schema and the version steps that move a database to it
//!
//! A fresh database gets [`SCHEMA`] and [`SCHEMA_VERSION`] in one transaction.
//! An older database runs each step in [`MIGRATIONS`] from its version up.
//! Version 1 is the baseline of `homebased_v1.sqlite`; databases written by
//! earlier releases live in a different file and are never opened

use rusqlite::Connection;

use crate::error::AppError;

/// Schema version this binary writes, stored in SQLite's `user_version`
pub const SCHEMA_VERSION: i64 = 4;

/// One step that moves a database from version `N` to `N + 1`
///
/// `MIGRATIONS[i]` moves version `i + 1` to `i + 2`, so the list always has
/// `SCHEMA_VERSION - 1` entries
type Migration = fn(&Connection) -> Result<(), rusqlite::Error>;

/// Steps from version 1 up to [`SCHEMA_VERSION`]; add the next one at the end
const MIGRATIONS: [Migration; 3] = [
    add_waiting_handoff,
    add_job_route_created_at,
    add_task_usage,
];

/// Version 2: waiting reports, the continuation release rule, and run chains
fn add_waiting_handoff(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(&format!(
        "ALTER TABLE reports ADD COLUMN {REPORT_WAITING_ON};
         ALTER TABLE reports ADD COLUMN {REPORT_NOTES};
         ALTER TABLE origin_routes ADD COLUMN {ROUTE_AFTER_RULE};
         {CHAIN_TABLES}"
    ))
}

/// Version 3: when each queue job route was saved
///
/// A route saved before this version is dated at the upgrade, so a job still
/// running then keeps the full [`JOB_ROUTE_CREATED_AT`] window
fn add_job_route_created_at(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(&format!(
        "ALTER TABLE resource_job_routes ADD COLUMN {JOB_ROUTE_CREATED_AT};
         UPDATE resource_job_routes SET created_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now');"
    ))
}

/// Version 4: token usage read from finished agent workers' output
///
/// Tasks that ended before this version get theirs from the daemon's backfill
fn add_task_usage(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(TASK_USAGE_TABLE)
}

/// Usage of a finished task, read from its output. `models_json` is a JSON
/// array of `ModelUsage`, empty when the output held no usage, so the
/// backfill never reads that task again
const TASK_USAGE_TABLE: &str = "
CREATE TABLE task_usage (
    task_id TEXT PRIMARY KEY REFERENCES tasks(id),
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    turns INTEGER NOT NULL CHECK (turns >= 0),
    models_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
";

/// When a queue job route was saved, as RFC 3339 UTC with milliseconds
///
/// Nothing closes a job route whose queue owner never reports an ending, so
/// readers that wait on unfinished jobs bound them by this age
const JOB_ROUTE_CREATED_AT: &str = "created_at TEXT";

/// Targets of a `waiting` report, as a JSON array; set exactly on waiting reports
const REPORT_WAITING_ON: &str =
    "waiting_on TEXT CHECK ((outcome = 'waiting') = (waiting_on IS NOT NULL))";

/// Notes of a `waiting` report; set exactly on waiting reports
const REPORT_NOTES: &str = "notes TEXT CHECK ((outcome = 'waiting') = (notes IS NOT NULL))";

/// Which endings of `after_json` release a held route: public `after` needs
/// success, and a continuation is released by any ending
const ROUTE_AFTER_RULE: &str = "after_rule TEXT NOT NULL DEFAULT 'succeeded'
    CHECK (after_rule IN ('succeeded', 'ended'))";

/// A chain owns the logical work of an agent that parked at least once. Its
/// id is the first run's task id, `state_json` is a `ChainState`, and each
/// run task, including a held continuation, has one numbered row
const CHAIN_TABLES: &str = "
CREATE TABLE task_chains (
    id TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE chain_runs (
    task_id TEXT PRIMARY KEY,
    chain_id TEXT NOT NULL REFERENCES task_chains(id),
    run_number INTEGER NOT NULL CHECK (run_number >= 1),
    UNIQUE (chain_id, run_number)
);
";

const _: () = assert!(
    MIGRATIONS.len() as i64 == SCHEMA_VERSION - 1,
    "every schema version above 1 needs exactly one migration step"
);

/// Bring the database inside `conn` to [`SCHEMA_VERSION`]
///
/// The caller holds an immediate transaction, so a failed step leaves the
/// database at its old version
pub(super) fn migrate(conn: &Connection) -> Result<(), AppError> {
    let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version == SCHEMA_VERSION {
        return Ok(());
    }
    if version == 0 {
        conn.execute_batch(&schema())?;
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        return Ok(());
    }
    if !(1..SCHEMA_VERSION).contains(&version) {
        return Err(AppError::SchemaTooNew {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }

    let first = usize::try_from(version - 1).map_err(|_| AppError::Internal {
        message: format!("invalid schema version {version}"),
    })?;
    for step in &MIGRATIONS[first..] {
        step(conn)?;
    }
    conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    Ok(())
}

/// Every table of the current schema version
fn schema() -> String {
    SCHEMA
        .replace("{REPORT_WAITING_ON}", REPORT_WAITING_ON)
        .replace("{REPORT_NOTES}", REPORT_NOTES)
        .replace("{ROUTE_AFTER_RULE}", ROUTE_AFTER_RULE)
        .replace("{CHAIN_TABLES}", CHAIN_TABLES)
        .replace("{JOB_ROUTE_CREATED_AT}", JOB_ROUTE_CREATED_AT)
        .replace("{TASK_USAGE_TABLE}", TASK_USAGE_TABLE)
}

/// Every table of the current schema, with the columns and tables shared with
/// [`add_waiting_handoff`], [`add_job_route_created_at`], and
/// [`add_task_usage`] as placeholders
///
/// `timeout_secs` is decimal TEXT, not INTEGER: the inactivity timer has no
/// product maximum, and a `Duration` above `i64::MAX` seconds cannot be stored
/// in SQLite's signed INTEGER without a lossy cast.
///
/// `child_pid` and `child_start_time` identify the worker's child, so cleanup
/// after a lost worker can tell its process group from a later process that
/// reused the PID. They are set together, once, at spawn.
///
/// A queue run is an ordinary task row with `resource_job_id`, `run_number`,
/// and `step_index`, set together, and `(resource_job_id, run_number)` is
/// unique. Only run reservation writes them, in the transaction that checks
/// the job, so `resource_job_id` needs no foreign key.
///
/// `resources` holds each resource's single active run in its `run_*`
/// columns, so one active run per resource is structural, and a unique index
/// on `run_job` keeps one active run per job. The CHECKs tie each run column
/// to the phases that use it. Jobs reference resources, and resources their
/// active job, through `(id, machine)` keys, so a run, its job, and a pinned
/// target always share one machine queue.
///
/// Every non-terminal job holds a slot `(priority, position)` that is unique
/// in its machine queue; terminal jobs hold none. The store keeps positions
/// dense by renumbering the affected levels in each transaction
pub(super) const SCHEMA: &str = r"
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    name TEXT NOT NULL,
    workload_json TEXT NOT NULL,
    cwd TEXT NOT NULL,
    timeout_secs TEXT NOT NULL,
    env_path TEXT NOT NULL,
    env_home TEXT NOT NULL,
    binary TEXT NOT NULL,
    status TEXT NOT NULL,
    exit_reason TEXT,
    check_due_at TEXT,
    pid INTEGER,
    cancel_requested_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    project_root TEXT,
    process_group_exit_evidence TEXT NOT NULL
        CHECK (process_group_exit_evidence IN ('unconfirmed', 'confirmed_exited', 'no_child_spawned')),
    container_exit_evidence TEXT,
    worker_thread TEXT,
    child_pid INTEGER CHECK (child_pid > 0),
    child_start_time INTEGER CHECK (
        (child_start_time IS NULL) = (child_pid IS NULL)
        AND (child_start_time IS NULL OR child_start_time >= 0)
    ),
    resource_job_id TEXT,
    run_number INTEGER CHECK (
        (run_number IS NULL) = (resource_job_id IS NULL)
        AND (run_number IS NULL OR run_number >= 1)
    ),
    step_index INTEGER CHECK (
        (step_index IS NULL) = (resource_job_id IS NULL)
        AND (step_index IS NULL OR step_index >= 0)
    )
);
CREATE INDEX tasks_status ON tasks(status);
CREATE INDEX tasks_thread ON tasks(thread_id);
CREATE UNIQUE INDEX tasks_resource_run
    ON tasks(resource_job_id, run_number) WHERE resource_job_id IS NOT NULL;

CREATE TABLE reports (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    summary TEXT NOT NULL,
    reported_at TEXT NOT NULL,
    notified_at TEXT,
    {REPORT_WAITING_ON},
    {REPORT_NOTES},
    PRIMARY KEY (task_id, seq),
    FOREIGN KEY (task_id) REFERENCES tasks(id)
);

CREATE TABLE task_containers (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES tasks(id),
    container_id TEXT CHECK (
        container_id IS NULL
        OR (length(container_id) = 64 AND container_id NOT GLOB '*[^0-9a-f]*')
    ),
    started_at TEXT,
    adoptions INTEGER NOT NULL DEFAULT 0 CHECK (adoptions >= 0)
);

CREATE TABLE origin_routes (
    request_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    route_json TEXT NOT NULL,
    after_json TEXT,
    outcome TEXT
        CHECK (outcome IN ('succeeded', 'failed', 'blocked', 'cancelled', 'lost', 'preempted')),
    {ROUTE_AFTER_RULE}
);

CREATE TABLE executor_identities (
    task_id TEXT PRIMARY KEY,
    origin_machine TEXT NOT NULL,
    identity_json TEXT NOT NULL
);

CREATE TABLE executor_outbox (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'acknowledged')),
    acknowledged_at TEXT,
    PRIMARY KEY (task_id, seq)
);
CREATE INDEX executor_outbox_pending ON executor_outbox(state, task_id, seq);
CREATE INDEX executor_outbox_retention ON executor_outbox(acknowledged_at, task_id, seq);

CREATE TABLE executor_event_cursors (
    task_id TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL CHECK (last_seq >= 0)
);

CREATE TABLE executor_event_routes (
    task_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state = 'orphaned'),
    reason TEXT NOT NULL
);

CREATE TABLE origin_inbox (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_json TEXT NOT NULL,
    delivery_json TEXT NOT NULL,
    settled_at TEXT,
    PRIMARY KEY (task_id, seq)
);
CREATE INDEX origin_inbox_order ON origin_inbox(task_id, seq);
CREATE INDEX origin_inbox_retention ON origin_inbox(settled_at, task_id, seq);

CREATE TABLE executor_event_receipts (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_digest TEXT NOT NULL,
    result_json TEXT NOT NULL,
    terminal_callback INTEGER NOT NULL CHECK (terminal_callback IN (0, 1)),
    PRIMARY KEY (task_id, seq)
);

CREATE TABLE origin_event_receipts (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_digest TEXT NOT NULL,
    delivery_json TEXT NOT NULL,
    terminal_callback INTEGER NOT NULL CHECK (terminal_callback IN (0, 1)),
    PRIMARY KEY (task_id, seq)
);

CREATE TABLE cancellation_requests (
    task_id TEXT PRIMARY KEY,
    request_json TEXT NOT NULL
);

CREATE TABLE executor_cancellations (
    cancellation_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    receipt_json TEXT NOT NULL
);
CREATE INDEX executor_cancellations_task ON executor_cancellations(task_id);

CREATE TABLE message_attempts (
    message_id TEXT PRIMARY KEY,
    attempt_json TEXT NOT NULL
);

CREATE TABLE message_receipts (
    message_id TEXT PRIMARY KEY REFERENCES message_attempts(message_id),
    receipt_json TEXT NOT NULL
);

CREATE TABLE outbound_message_bindings (
    message_id TEXT PRIMARY KEY,
    binding_json TEXT NOT NULL
);

CREATE TABLE resources (
    id TEXT PRIMARY KEY,
    machine TEXT NOT NULL,
    name TEXT NOT NULL,
    device INTEGER CHECK (device IS NULL OR device >= 0),
    origin TEXT NOT NULL
        CHECK (origin IN ('manual', 'detected_fallback', 'detected_device'))
        CHECK ((origin != 'detected_fallback' OR device IS NULL)
            AND (origin != 'detected_device' OR device IS NOT NULL)),
    created_at TEXT NOT NULL,
    run_job TEXT,
    run_task TEXT UNIQUE REFERENCES tasks(id),
    run_number INTEGER CHECK (run_number IS NULL OR run_number >= 1),
    run_step INTEGER CHECK (run_step IS NULL OR run_step >= 0),
    run_resume INTEGER CHECK (run_resume IN (0, 1)),
    run_phase TEXT
        CHECK (run_phase IN ('launching', 'executing', 'stopping', 'cleaning', 'attention')),
    run_reserved_at TEXT,
    run_started_at TEXT,
    run_stop_cause TEXT CHECK (run_stop_cause IN ('yield', 'restart', 'user_cancel')),
    run_stop_requested_at TEXT,
    run_cleanup_attempt INTEGER CHECK (run_cleanup_attempt IS NULL OR run_cleanup_attempt >= 1),
    run_attention_id TEXT UNIQUE,
    run_attention_failure TEXT,
    UNIQUE (machine, name),
    UNIQUE (id, machine),
    FOREIGN KEY (run_job, machine) REFERENCES resource_jobs(id, machine),
    CHECK ((run_phase IS NULL) = (run_job IS NULL)),
    CHECK ((run_phase IS NULL) = (run_task IS NULL)),
    CHECK ((run_phase IS NULL) = (run_number IS NULL)),
    CHECK ((run_phase IS NULL) = (run_step IS NULL)),
    CHECK ((run_phase IS NULL) = (run_resume IS NULL)),
    CHECK ((COALESCE(run_phase, '') = 'launching') = (run_reserved_at IS NOT NULL)),
    CHECK (CASE COALESCE(run_phase, '')
        WHEN 'executing' THEN run_started_at IS NOT NULL
        WHEN 'stopping' THEN 1
        ELSE run_started_at IS NULL
    END),
    CHECK ((COALESCE(run_phase, '') = 'stopping') = (run_stop_cause IS NOT NULL)),
    CHECK ((COALESCE(run_phase, '') = 'stopping') = (run_stop_requested_at IS NOT NULL)),
    CHECK ((COALESCE(run_phase, '') = 'cleaning') = (run_cleanup_attempt IS NOT NULL)),
    CHECK ((COALESCE(run_phase, '') = 'attention') = (run_attention_id IS NOT NULL)),
    CHECK ((COALESCE(run_phase, '') = 'attention') = (run_attention_failure IS NOT NULL))
);
CREATE UNIQUE INDEX resources_device ON resources(machine, device) WHERE device IS NOT NULL;
CREATE UNIQUE INDEX resources_run_job ON resources(run_job) WHERE run_job IS NOT NULL;

CREATE TABLE resource_jobs (
    id TEXT PRIMARY KEY,
    machine TEXT NOT NULL,
    origin_machine TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    spec_digest TEXT NOT NULL,
    env_path TEXT NOT NULL,
    env_home TEXT NOT NULL,
    target_resource TEXT,
    priority INTEGER NOT NULL CHECK (priority IN (0, 1, 2)),
    position INTEGER CHECK (position IS NULL OR position >= 1),
    state TEXT NOT NULL
        CHECK (state IN ('queued', 'active', 'succeeded', 'failed', 'cancelled')),
    active_resource TEXT,
    failed_run TEXT REFERENCES tasks(id),
    step INTEGER NOT NULL CHECK (step >= 0),
    resume INTEGER CHECK (resume IN (0, 1)),
    last_run_number INTEGER NOT NULL CHECK (last_run_number >= 0),
    event_seq INTEGER NOT NULL CHECK (event_seq >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (id, machine),
    FOREIGN KEY (target_resource, machine) REFERENCES resources(id, machine),
    FOREIGN KEY (active_resource, machine) REFERENCES resources(id, machine),
    CHECK ((state IN ('queued', 'active')) = (position IS NOT NULL)),
    CHECK ((state = 'active') = (active_resource IS NOT NULL)),
    CHECK ((state = 'failed') = (failed_run IS NOT NULL)),
    CHECK ((state = 'queued') = (resume IS NOT NULL)),
    CHECK (
        target_resource IS NULL OR active_resource IS NULL OR active_resource = target_resource
    )
);
CREATE UNIQUE INDEX resource_jobs_slot
    ON resource_jobs(machine, priority, position) WHERE position IS NOT NULL;

CREATE TABLE resource_job_events (
    job_id TEXT NOT NULL REFERENCES resource_jobs(id),
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    suppressed_at TEXT,
    PRIMARY KEY (job_id, seq)
);

CREATE TABLE resource_operations (
    id TEXT PRIMARY KEY,
    machine TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('move', 'cancel', 'release')),
    content_digest TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE resource_blocked_notices (
    id INTEGER PRIMARY KEY,
    machine TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES resource_jobs(id),
    blocked_since TEXT NOT NULL,
    notified_at TEXT,
    ended_at TEXT
);
CREATE UNIQUE INDEX resource_blocked_notices_open
    ON resource_blocked_notices(machine) WHERE ended_at IS NULL;

CREATE TABLE resource_run_history (
    task_id TEXT PRIMARY KEY REFERENCES tasks(id),
    resource_id TEXT NOT NULL REFERENCES resources(id),
    stop_cause TEXT,
    cleanup_json TEXT
);

CREATE TABLE resource_job_routes (
    job_id TEXT PRIMARY KEY,
    route_json TEXT NOT NULL,
    accepted_seq INTEGER NOT NULL DEFAULT 0 CHECK (accepted_seq >= 0),
    settled_seq INTEGER NOT NULL DEFAULT 0 CHECK (settled_seq >= 0 AND settled_seq <= accepted_seq),
    {JOB_ROUTE_CREATED_AT}
);

CREATE TABLE resource_job_inbox (
    job_id TEXT NOT NULL REFERENCES resource_job_routes(job_id),
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_json TEXT NOT NULL,
    suppressed_at TEXT,
    PRIMARY KEY (job_id, seq)
);

CREATE TABLE resource_job_delivery (
    job_id TEXT PRIMARY KEY REFERENCES resource_jobs(id),
    acknowledged_seq INTEGER NOT NULL CHECK (acknowledged_seq >= 0)
);
{CHAIN_TABLES}
{TASK_USAGE_TABLE}";

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::{SCHEMA_VERSION, migrate, schema};
    use crate::error::AppError;
    use crate::store::Store;

    fn user_version(conn: &Connection) -> i64 {
        conn.pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap()
    }

    fn table_names(conn: &Connection) -> Vec<String> {
        let mut statement = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[test]
    fn a_fresh_database_starts_at_the_current_version_and_reopens_unchanged() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(crate::home::DB_NAME);
        drop(Store::open(&path).unwrap());

        let conn = Connection::open(&path).unwrap();
        assert_eq!(user_version(&conn), SCHEMA_VERSION);
        let tables = table_names(&conn);
        for table in ["tasks", "origin_routes", "executor_outbox", "resource_jobs"] {
            assert!(
                tables.iter().any(|name| name == table),
                "{table}: {tables:?}"
            );
        }
        drop(conn);

        drop(Store::open(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        assert_eq!(user_version(&conn), SCHEMA_VERSION);
        assert_eq!(table_names(&conn), tables);
    }

    #[test]
    fn a_database_from_a_newer_build_is_refused_and_left_unchanged() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(crate::home::DB_NAME);
        let newer = SCHEMA_VERSION + 1;
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE future (id INTEGER PRIMARY KEY);")
            .unwrap();
        conn.pragma_update(None, "user_version", newer).unwrap();
        drop(conn);

        let Err(error) = Store::open(&path) else {
            panic!("a newer schema must be refused");
        };
        assert!(
            matches!(
                error,
                AppError::SchemaTooNew { found, supported }
                    if found == newer && supported == SCHEMA_VERSION
            ),
            "{error:?}"
        );
        assert_eq!(error.code(), "schema_too_new");

        let conn = Connection::open(&path).unwrap();
        assert_eq!(user_version(&conn), newer);
        assert_eq!(table_names(&conn), ["future"]);
    }

    fn columns(conn: &Connection, table: &str) -> Vec<String> {
        let mut statement = conn
            .prepare(&format!(
                "SELECT name FROM pragma_table_info('{table}') ORDER BY name"
            ))
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[test]
    fn a_version_1_database_migrates_to_the_fresh_schema_and_keeps_its_reports() {
        let fresh = Connection::open_in_memory().unwrap();
        migrate(&fresh).unwrap();

        // version 1 is the current schema without what version 2 added
        let v1 = schema()
            .replace(&format!(",\n    {}", super::REPORT_WAITING_ON), "")
            .replace(&format!(",\n    {}", super::REPORT_NOTES), "")
            .replace(&format!(",\n    {}", super::ROUTE_AFTER_RULE), "")
            .replace(super::CHAIN_TABLES, "")
            .replace(super::TASK_USAGE_TABLE, "")
            // the last column of its table, unlike the `created_at` of tasks
            .replace(&format!(",\n    {}\n)", super::JOB_ROUTE_CREATED_AT), "\n)");
        let old = Connection::open_in_memory().unwrap();
        old.execute_batch(&v1).unwrap();
        assert!(!columns(&old, "reports").contains(&"notes".to_string()));
        old.pragma_update(None, "user_version", 1).unwrap();
        old.execute_batch(
            "INSERT INTO tasks (id,thread_id,name,workload_json,cwd,timeout_secs,env_path,
                 env_home,binary,status,created_at,updated_at,process_group_exit_evidence)
             VALUES ('t','th','n','{}','/','1','/','/','/b','running','x','x','unconfirmed');
             INSERT INTO reports (task_id,seq,outcome,summary,reported_at)
             VALUES ('t',1,'blocked','old','x');
             INSERT INTO resource_job_routes (job_id,route_json) VALUES ('j','{}');",
        )
        .unwrap();

        migrate(&old).unwrap();
        assert_eq!(user_version(&old), SCHEMA_VERSION);
        assert_eq!(table_names(&old), table_names(&fresh));
        for table in [
            "reports",
            "origin_routes",
            "task_chains",
            "chain_runs",
            "resource_job_routes",
            "task_usage",
        ] {
            assert_eq!(columns(&old, table), columns(&fresh, table), "{table}");
        }
        let kept: String = old
            .query_row("SELECT summary FROM reports WHERE task_id='t'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(kept, "old");
        // a job running at the upgrade is dated then, not left undated
        let dated: Option<String> = old
            .query_row(
                "SELECT created_at FROM resource_job_routes WHERE job_id='j'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(dated.unwrap().ends_with('Z'));
        // a waiting report must carry its targets and notes
        assert!(
            old.execute(
                "INSERT INTO reports (task_id,seq,outcome,summary,reported_at)
                 VALUES ('t',2,'waiting','w','x')",
                [],
            )
            .is_err()
        );
    }

    #[test]
    fn migrate_is_a_no_op_at_the_current_version() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        let tables = table_names(&conn);
        migrate(&conn).unwrap();
        assert_eq!(table_names(&conn), tables);
        assert_eq!(user_version(&conn), SCHEMA_VERSION);
    }
}
