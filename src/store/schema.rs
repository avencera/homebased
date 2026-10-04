//! SQLite schema and the version steps that move a database to it
//!
//! A fresh database gets [`SCHEMA`] and [`SCHEMA_VERSION`] in one transaction.
//! An older database runs each step in [`MIGRATIONS`] from its version up.
//! Version 1 is the baseline of `homebased_v1.sqlite`; databases written by
//! earlier releases live in a different file and are never opened

use rusqlite::Connection;

use crate::error::AppError;

/// Schema version this binary writes, stored in SQLite's `user_version`
pub const SCHEMA_VERSION: i64 = 1;

/// One step that moves a database from version `N` to `N + 1`
///
/// `MIGRATIONS[i]` moves version `i + 1` to `i + 2`, so the list always has
/// `SCHEMA_VERSION - 1` entries
type Migration = fn(&Connection) -> Result<(), rusqlite::Error>;

/// Steps from version 1 up to [`SCHEMA_VERSION`]; add the next one at the end
const MIGRATIONS: [Migration; 0] = [];

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
        conn.execute_batch(SCHEMA)?;
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

/// Every table of schema version 1
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
        CHECK (outcome IN ('succeeded', 'failed', 'blocked', 'cancelled', 'lost', 'preempted'))
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
    settled_seq INTEGER NOT NULL DEFAULT 0 CHECK (settled_seq >= 0 AND settled_seq <= accepted_seq)
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
";

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::{SCHEMA_VERSION, migrate};
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
