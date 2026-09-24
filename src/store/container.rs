//! Saved container identity and adoption streak of container tasks

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, params};

use super::{Store, fmt_time, parse_time};
use crate::domain::{ContainerId, TaskId};
use crate::error::AppError;
use crate::resource::ResourceId;

/// Container lifecycle saved for one container task
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskContainerRecord {
    /// Container ID saved before the container started
    pub container_id: Option<ContainerId>,
    /// When a worker saw the container leave its created state
    pub started_at: Option<DateTime<Utc>>,
    /// Workers started since the last one that reached the container
    pub adoptions: u32,
}

impl Store {
    /// Read the saved container lifecycle of one task
    pub fn task_container(&self, task: TaskId) -> Result<Option<TaskContainerRecord>, AppError> {
        let row: Option<(Option<String>, Option<String>, i64)> = self
            .conn
            .query_row(
                "SELECT container_id, started_at, adoptions FROM task_containers WHERE task_id = ?1",
                [task.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((container_id, started_at, adoptions)) = row else {
            return Ok(None);
        };
        Ok(Some(TaskContainerRecord {
            container_id: container_id
                .as_deref()
                .map(ContainerId::parse)
                .transpose()?,
            started_at: started_at.as_deref().map(parse_time).transpose()?,
            adoptions: u32::try_from(adoptions).map_err(|_| AppError::Internal {
                message: format!("task {task} has an invalid container adoption count"),
            })?,
        }))
    }

    /// Save the container ID of one task before its container starts
    ///
    /// A saved ID is never replaced: a different ID for the same task means two
    /// containers claim it, which the caller must treat as unproven
    pub fn save_task_container_id(&self, task: TaskId, id: &ContainerId) -> Result<(), AppError> {
        self.immediate(|| {
            self.conn.execute(
                "INSERT INTO task_containers (task_id, container_id) VALUES (?1, ?2)
                 ON CONFLICT (task_id) DO UPDATE SET container_id = excluded.container_id
                 WHERE task_containers.container_id IS NULL",
                params![task.to_string(), id.as_str()],
            )?;
            let saved = self
                .task_container(task)?
                .and_then(|record| record.container_id);
            if saved.as_ref() == Some(id) {
                Ok(())
            } else {
                Err(AppError::Internal {
                    message: format!(
                        "task {task} already saved container {}, not {id}",
                        saved.map_or_else(|| "none".into(), |saved| saved.to_string())
                    ),
                })
            }
        })
    }

    /// Record the first time a worker saw the task's container leave its created state
    pub fn record_task_container_started(&self, task: TaskId) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE task_containers SET started_at = ?1
             WHERE task_id = ?2 AND started_at IS NULL AND container_id IS NOT NULL",
            params![fmt_time(Utc::now()), task.to_string()],
        )?;
        Ok(())
    }

    /// End the adoption streak after a worker reached the task's container
    pub fn reset_task_container_adoptions(&self, task: TaskId) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE task_containers SET adoptions = 0 WHERE task_id = ?1",
            [task.to_string()],
        )?;
        Ok(())
    }

    /// Resource whose request or return loan owns one task, if any
    pub(crate) fn resource_for_task(&self, task: TaskId) -> Result<Option<ResourceId>, AppError> {
        let resource: Option<String> = self
            .conn
            .query_row(
                "SELECT resource_id FROM resource_requests WHERE task_id = ?1
                 UNION ALL
                 SELECT resource_id FROM loans
                 WHERE json_extract(state_json, '$.phase.resume_task_id') = ?1
                    OR json_extract(state_json, '$.last_safe_phase.resume_task_id') = ?1
                 LIMIT 1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        resource
            .map(|raw| {
                raw.parse().map_err(|_| AppError::Internal {
                    message: format!("stored resource id {raw} is invalid"),
                })
            })
            .transpose()
    }

    /// Count one more adopting worker, unless `limit` workers in a row already
    /// stopped before they reached the container
    ///
    /// Returns whether the caller may start the worker
    pub fn claim_task_container_adoption(
        &self,
        task: TaskId,
        limit: u32,
    ) -> Result<bool, AppError> {
        self.immediate(|| {
            self.conn.execute(
                "INSERT INTO task_containers (task_id) VALUES (?1)
                 ON CONFLICT (task_id) DO NOTHING",
                [task.to_string()],
            )?;
            let changed = self.conn.execute(
                "UPDATE task_containers SET adoptions = adoptions + 1
                 WHERE task_id = ?1 AND adoptions < ?2",
                params![task.to_string(), limit],
            )?;
            Ok(changed == 1)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::container::ContainerWorkload;
    use crate::domain::{ContainerId, TaskEnv, TaskId, ThreadId, Workload};
    use crate::store::{NewTask, Store, new_queued_task};

    fn container_task(store: &Store) -> TaskId {
        let id = TaskId::new();
        let workload = ContainerWorkload::from_value(&serde_json::json!({
            "image": format!("sha256:{}", "0".repeat(64)),
            "memory": "1g"
        }))
        .unwrap();
        store
            .insert_task(&new_queued_task(NewTask {
                id,
                name: None,
                thread: ThreadId(uuid::Uuid::now_v7()),
                workload: Workload::Container(Box::new(workload)),
                cwd: "/tmp".into(),
                timeout: Duration::from_secs(1800),
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                binary: "/usr/bin/docker".into(),
            }))
            .unwrap();
        id
    }

    #[test]
    fn a_saved_container_id_is_never_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let task = container_task(&store);
        let first = ContainerId::parse(&"a".repeat(64)).unwrap();
        let second = ContainerId::parse(&"b".repeat(64)).unwrap();

        assert_eq!(store.task_container(task).unwrap(), None);
        store.save_task_container_id(task, &first).unwrap();
        store.save_task_container_id(task, &first).unwrap();
        assert!(store.save_task_container_id(task, &second).is_err());
        store.record_task_container_started(task).unwrap();
        let record = store.task_container(task).unwrap().unwrap();
        assert_eq!(record.container_id, Some(first));
        assert!(record.started_at.is_some());
    }

    #[test]
    fn adoption_streak_is_bounded_until_a_worker_reaches_the_container() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let task = container_task(&store);

        assert!(store.claim_task_container_adoption(task, 2).unwrap());
        assert!(store.claim_task_container_adoption(task, 2).unwrap());
        assert!(!store.claim_task_container_adoption(task, 2).unwrap());
        store.reset_task_container_adoptions(task).unwrap();
        assert!(store.claim_task_container_adoption(task, 2).unwrap());
        // an adoption claimed before any ID was saved still accepts the later ID
        let id = ContainerId::parse(&"c".repeat(64)).unwrap();
        store.save_task_container_id(task, &id).unwrap();
        assert_eq!(
            store.task_container(task).unwrap().unwrap().container_id,
            Some(id)
        );
    }
}
