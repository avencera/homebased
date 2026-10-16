//! Tasks submitted with `--hold`, and which of them still keep a T3 thread waiting

use rusqlite::params;

use super::Store;
use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;

/// A held task that still owes its thread something
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingHold {
    /// The held task
    pub task: TaskId,
    /// Its name, for the placeholder the thread shows
    pub name: String,
    /// Thread that receives its events
    pub thread: ThreadId,
}

// a held task is pending until its work ended and every event it owes the
// thread was delivered. A task that parked belongs to a chain, which ends only
// with its last run; its own route outcome stays open
const PENDING_SQL: &str = "
SELECT h.task_id, json_extract(r.route_json, '$.spec.name'), json_extract(r.route_json, '$.thread')
FROM t3_hold_tasks h
JOIN origin_routes r ON r.task_id = h.task_id
WHERE json_valid(r.route_json)
  AND (
    CASE
      WHEN EXISTS (SELECT 1 FROM chain_runs c WHERE c.task_id = h.task_id)
      THEN (SELECT json_extract(tc.state_json, '$.state')
            FROM chain_runs c JOIN task_chains tc ON tc.id = c.chain_id
            WHERE c.task_id = h.task_id) != 'ended'
      ELSE r.outcome IS NULL
    END
    OR EXISTS (
      SELECT 1 FROM origin_inbox i
      WHERE i.settled_at IS NULL
        AND (i.task_id = h.task_id
             OR i.task_id IN (
                 SELECT other.task_id
                 FROM chain_runs mine JOIN chain_runs other ON other.chain_id = mine.chain_id
                 WHERE mine.task_id = h.task_id))
    )
  )
ORDER BY h.task_id";

impl Store {
    /// Remember that `task` asked to keep its thread in T3's Working shelf
    ///
    /// Saving it again changes nothing, so a retried submit is safe
    pub fn mark_t3_hold(&self, task: TaskId) -> Result<(), AppError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO t3_hold_tasks (task_id) VALUES (?1)",
            params![task.to_string()],
        )?;
        Ok(())
    }

    /// Held tasks that still keep their thread waiting
    pub fn pending_t3_holds(&self) -> Result<Vec<PendingHold>, AppError> {
        let mut statement = self.conn.prepare(PENDING_SQL)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(task, name, thread)| {
                let invalid = |what: &str| AppError::Internal {
                    message: format!("invalid {what} in a held task route"),
                };
                Ok(PendingHold {
                    task: task.parse().map_err(|_| invalid("task id"))?,
                    // a route without a name must not stop every other hold
                    name: name.unwrap_or_else(|| task.clone()),
                    thread: thread.parse().map_err(|_| invalid("thread id"))?,
                })
            })
            .collect()
    }

    /// Threads of every held task, finished or not
    ///
    /// A restarted daemon reads these to end placeholder runs it left behind
    pub fn t3_hold_threads(&self) -> Result<Vec<ThreadId>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT DISTINCT json_extract(r.route_json, '$.thread')
             FROM t3_hold_tasks h JOIN origin_routes r ON r.task_id = h.task_id
             WHERE json_valid(r.route_json)",
        )?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|thread| {
                thread.parse().map_err(|_| AppError::Internal {
                    message: "invalid thread id in a held task route".into(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::Store;
    use crate::domain::{TaskId, ThreadId};

    fn task() -> TaskId {
        TaskId(Uuid::now_v7())
    }

    /// A route as the daemon saves it, reduced to the fields a hold reads
    fn route(store: &Store, task: TaskId, thread: ThreadId, outcome: Option<&str>) {
        let json = serde_json::json!({
            "spec": { "name": format!("job-{task}") },
            "thread": thread.to_string(),
        });
        store
            .conn
            .execute(
                "INSERT INTO origin_routes (request_id, task_id, route_json, outcome)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    Uuid::now_v7().to_string(),
                    task.to_string(),
                    json.to_string(),
                    outcome
                ],
            )
            .unwrap();
    }

    fn inbox(store: &Store, task: TaskId, seq: i64, settled: bool) {
        store
            .conn
            .execute(
                "INSERT INTO origin_inbox (task_id, seq, event_json, delivery_json, settled_at)
                 VALUES (?1, ?2, '{}', '{}', ?3)",
                rusqlite::params![
                    task.to_string(),
                    seq,
                    settled.then_some("2026-10-16T12:00:00Z")
                ],
            )
            .unwrap();
    }

    fn chain(store: &Store, runs: &[TaskId], state: &str) {
        let id = runs[0].to_string();
        store
            .conn
            .execute(
                "INSERT INTO task_chains (id, state_json, updated_at) VALUES (?1, ?2, '2026-10-16')",
                rusqlite::params![id, serde_json::json!({ "state": state }).to_string()],
            )
            .unwrap();
        for (number, run) in runs.iter().enumerate() {
            store
                .conn
                .execute(
                    "INSERT INTO chain_runs (task_id, chain_id, run_number) VALUES (?1, ?2, ?3)",
                    rusqlite::params![run.to_string(), id, i64::try_from(number + 1).unwrap()],
                )
                .unwrap();
        }
    }

    fn pending(store: &Store) -> Vec<TaskId> {
        store
            .pending_t3_holds()
            .unwrap()
            .into_iter()
            .map(|hold| hold.task)
            .collect()
    }

    #[test]
    fn a_held_task_is_pending_until_its_final_event_is_settled() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let thread = ThreadId(Uuid::now_v7());
        let (running, delivering, done, unheld) = (task(), task(), task(), task());
        for held in [running, delivering, done] {
            store.mark_t3_hold(held).unwrap();
        }
        route(&store, running, thread, None);
        route(&store, delivering, thread, Some("succeeded"));
        inbox(&store, delivering, 1, false);
        route(&store, done, thread, Some("succeeded"));
        inbox(&store, done, 1, true);
        route(&store, unheld, thread, None);

        let mut expected = vec![running, delivering];
        expected.sort_by_key(ToString::to_string);
        assert_eq!(pending(&store), expected);

        // marking a task again, as a retried submit does, changes nothing
        store.mark_t3_hold(running).unwrap();
        assert_eq!(pending(&store), expected);
    }

    #[test]
    fn a_route_without_a_name_does_not_stop_the_other_holds() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let thread = ThreadId(Uuid::now_v7());
        let (unnamed, named) = (task(), task());
        for held in [unnamed, named] {
            store.mark_t3_hold(held).unwrap();
        }
        store
            .conn
            .execute(
                "INSERT INTO origin_routes (request_id, task_id, route_json, outcome)
                 VALUES (?1, ?2, ?3, NULL)",
                rusqlite::params![
                    Uuid::now_v7().to_string(),
                    unnamed.to_string(),
                    serde_json::json!({ "thread": thread.to_string() }).to_string(),
                ],
            )
            .unwrap();
        route(&store, named, thread, None);

        let holds = store.pending_t3_holds().unwrap();

        assert_eq!(holds.len(), 2);
        let fallback = holds.iter().find(|hold| hold.task == unnamed).unwrap();
        assert_eq!(fallback.name, unnamed.to_string());
    }

    #[test]
    fn a_parked_task_stays_pending_until_its_whole_chain_ends() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let thread = ThreadId(Uuid::now_v7());
        let (first, second) = (task(), task());
        store.mark_t3_hold(first).unwrap();
        // the first run parked, so its own route outcome is already saved
        route(&store, first, thread, Some("succeeded"));
        route(&store, second, thread, None);
        inbox(&store, first, 1, true);
        chain(&store, &[first, second], "waiting");
        assert_eq!(pending(&store), vec![first]);

        store
            .conn
            .execute(
                "UPDATE task_chains SET state_json = '{\"state\":\"ended\"}'",
                [],
            )
            .unwrap();
        assert!(pending(&store).is_empty());

        // an event of a later run that was not delivered yet keeps the thread waiting
        inbox(&store, second, 1, false);
        assert_eq!(pending(&store), vec![first]);
    }

    #[test]
    fn a_database_from_before_holds_gains_the_table() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("db");
        Store::open(&path).unwrap();
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch("DROP TABLE t3_hold_tasks; PRAGMA user_version = 4;")
                .unwrap();
        }

        let store = Store::open(&path).unwrap();
        let held = task();
        store.mark_t3_hold(held).unwrap();
        assert!(pending(&store).is_empty());
    }
}
