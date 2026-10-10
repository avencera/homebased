//! Token usage of finished tasks

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, params};

use super::task::{TASK_SELECT, parse_task_row};
use super::{Store, fmt_time};
use crate::domain::{TaskId, TaskRow, ThreadId};
use crate::error::AppError;
use crate::usage::{ModelUsage, TaskUsage};

impl Store {
    /// Save a task's usage, replacing what an earlier read saved
    pub fn record_task_usage(&self, id: TaskId, usage: &TaskUsage) -> Result<(), AppError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO task_usage (task_id, complete, turns, models_json, recorded_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id.to_string(),
                usage.complete,
                // SQLite integers are signed; no worker takes 2^63 turns
                i64::try_from(usage.turns).unwrap_or(i64::MAX),
                serde_json::to_string(&usage.models)?,
                fmt_time(Utc::now()),
            ],
        )?;
        Ok(())
    }

    /// A task's usage, or `None` when none was recorded or its output held none
    pub fn task_usage(&self, id: TaskId) -> Result<Option<TaskUsage>, AppError> {
        let usage = self
            .conn
            .query_row(
                "SELECT complete, turns, models_json FROM task_usage WHERE task_id = ?1",
                [id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get::<_, String>(2)?)),
            )
            .optional()?;
        let Some((complete, turns, models)) = usage else {
            return Ok(None);
        };
        let usage = parse_usage(complete, turns, &models)?;
        Ok((!usage.is_empty()).then_some(usage))
    }

    /// Tasks submitted since `since` that recorded usage, optionally from one thread
    pub fn usage_since(
        &self,
        since: DateTime<Utc>,
        thread: Option<ThreadId>,
    ) -> Result<Vec<(TaskRow, TaskUsage)>, AppError> {
        let since = fmt_time(since);
        let thread = thread.map(|thread| thread.to_string());
        let mut statement = self.conn.prepare(
            "SELECT u.task_id, u.complete, u.turns, u.models_json
             FROM task_usage u JOIN tasks t ON t.id = u.task_id
             WHERE t.created_at >= ?1 AND u.models_json != '[]'
               AND (?2 IS NULL OR t.thread_id = ?2)",
        )?;
        let mut usage = HashMap::new();
        let mut rows = statement.query(params![since, thread])?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            usage.insert(
                id,
                parse_usage(row.get(1)?, row.get(2)?, &row.get::<_, String>(3)?)?,
            );
        }

        let mut statement = self.conn.prepare(&format!(
            "{TASK_SELECT}
             WHERE id IN (SELECT task_id FROM task_usage WHERE models_json != '[]')
               AND created_at >= ?1 AND (?2 IS NULL OR thread_id = ?2)"
        ))?;
        let tasks = statement
            .query_map(params![since, thread], parse_task_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(tasks
            .into_iter()
            .filter_map(|task| {
                let usage = usage.remove(&task.id.to_string())?;
                Some((task, usage))
            })
            .collect())
    }

    /// Finished Claude tasks with no recorded usage
    pub fn claude_tasks_without_usage(&self) -> Result<Vec<TaskId>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT id FROM tasks
             WHERE status NOT IN ('queued', 'running')
               AND json_extract(workload_json, '$.type') = 'agent'
               AND json_extract(workload_json, '$.agent') = 'claude'
               AND id NOT IN (SELECT task_id FROM task_usage)
             ORDER BY created_at",
        )?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.iter().map(|id| id.parse()).collect()
    }
}

fn parse_usage(complete: bool, turns: i64, models: &str) -> Result<TaskUsage, AppError> {
    let models: Vec<ModelUsage> = serde_json::from_str(models)?;
    // the schema CHECK keeps stored turns non-negative
    let turns = u64::try_from(turns).unwrap_or_default();
    Ok(TaskUsage::new(complete, turns, models))
}
