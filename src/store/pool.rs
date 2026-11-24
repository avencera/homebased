//! Pools a coordinator runs, their machines, and what a temporary machine
//! has on it

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, params};

use super::{fmt_time, parse_time};
use crate::domain::TaskId;
use crate::error::AppError;
use crate::pool::{Pool, PoolMachine, PoolName, PoolState, SlotId};
use crate::store::Store;

/// What runs or waits on a temporary machine, before output times are read
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredActivity {
    /// Queued and running tasks, queue runs included, with their creation time
    pub tasks: Vec<(TaskId, DateTime<Utc>)>,
    /// Jobs in this machine's queue that hold no run yet
    pub queued_jobs: usize,
    /// Task and job events not yet acknowledged by the machine that submitted them
    pub undelivered: usize,
}

/// Why a pool create was refused
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolInsert {
    /// Saved
    Created(Pool),
    /// A pool of that name is still open or releasing
    Exists(Pool),
}

impl Store {
    /// Save a new pool; a released pool of the same name is replaced with its
    /// machine rows, since every machine of it is confirmed gone
    pub fn insert_pool(&self, pool: &Pool) -> Result<PoolInsert, AppError> {
        self.immediate(|| {
            if let Some(existing) = self.pool(&pool.name)? {
                if !matches!(existing.state, PoolState::Released { .. }) {
                    return Ok(PoolInsert::Exists(existing));
                }
                self.conn.execute(
                    "DELETE FROM pool_machines WHERE pool = ?1",
                    [pool.name.to_string()],
                )?;
                self.conn
                    .execute("DELETE FROM pools WHERE name = ?1", [pool.name.to_string()])?;
            }
            self.conn.execute(
                "INSERT INTO pools (name, state, pool_json, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![
                    pool.name.to_string(),
                    pool.state.as_str(),
                    serde_json::to_string(pool)?,
                    fmt_time(pool.created_at),
                ],
            )?;
            Ok(PoolInsert::Created(pool.clone()))
        })
    }

    /// One pool
    pub fn pool(&self, name: &PoolName) -> Result<Option<Pool>, AppError> {
        let json: Option<String> = self
            .conn
            .query_row(
                "SELECT pool_json FROM pools WHERE name = ?1",
                [name.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        json.map(|json| Ok(serde_json::from_str(&json)?))
            .transpose()
    }

    /// Every pool, newest first
    pub fn pools(&self) -> Result<Vec<Pool>, AppError> {
        let mut statement = self
            .conn
            .prepare("SELECT pool_json FROM pools ORDER BY created_at DESC")?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.iter()
            .map(|json| Ok(serde_json::from_str(json)?))
            .collect()
    }

    /// Replace a pool's record
    pub fn update_pool(&self, pool: &Pool) -> Result<(), AppError> {
        let changed = self.conn.execute(
            "UPDATE pools SET state = ?2, pool_json = ?3 WHERE name = ?1",
            params![
                pool.name.to_string(),
                pool.state.as_str(),
                serde_json::to_string(pool)?
            ],
        )?;
        if changed == 0 {
            return Err(AppError::Internal {
                message: format!("pool {} vanished while it was updated", pool.name),
            });
        }
        Ok(())
    }

    /// Every machine of a pool, newest first
    pub fn pool_machines(&self, pool: &PoolName) -> Result<Vec<PoolMachine>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT machine_json FROM pool_machines WHERE pool = ?1 ORDER BY requested_at DESC",
        )?;
        let rows = statement
            .query_map([pool.to_string()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.iter()
            .map(|json| Ok(serde_json::from_str(json)?))
            .collect()
    }

    /// One machine of a pool
    pub fn pool_machine(&self, slot: SlotId) -> Result<Option<PoolMachine>, AppError> {
        let json: Option<String> = self
            .conn
            .query_row(
                "SELECT machine_json FROM pool_machines WHERE slot = ?1",
                [slot.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        json.map(|json| Ok(serde_json::from_str(&json)?))
            .transpose()
    }

    /// Save a machine before its create call goes out
    pub fn insert_pool_machine(&self, machine: &PoolMachine) -> Result<(), AppError> {
        self.conn.execute(
            "INSERT INTO pool_machines (slot, pool, state, machine_id, machine_json, requested_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                machine.slot.to_string(),
                machine.pool.to_string(),
                machine.state.as_str(),
                machine
                    .state
                    .joined()
                    .map(|joined| joined.machine.to_string()),
                serde_json::to_string(machine)?,
                fmt_time(machine.requested_at),
            ],
        )?;
        Ok(())
    }

    /// Replace a machine's record
    pub fn update_pool_machine(&self, machine: &PoolMachine) -> Result<(), AppError> {
        let changed = self.conn.execute(
            "UPDATE pool_machines SET state = ?2, machine_id = ?3, machine_json = ?4
             WHERE slot = ?1",
            params![
                machine.slot.to_string(),
                machine.state.as_str(),
                machine
                    .state
                    .joined()
                    .map(|joined| joined.machine.to_string()),
                serde_json::to_string(machine)?,
            ],
        )?;
        if changed == 0 {
            return Err(AppError::Internal {
                message: format!(
                    "pool machine {} vanished while it was updated",
                    machine.slot
                ),
            });
        }
        Ok(())
    }

    /// What runs or waits on this machine, for a temporary machine's renewal
    pub fn temporary_activity(&self) -> Result<StoredActivity, AppError> {
        let mut statement = self
            .conn
            .prepare("SELECT id, created_at FROM tasks WHERE status IN ('queued', 'running')")?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let tasks = rows
            .into_iter()
            .map(|(id, created)| Ok((id.parse()?, parse_time(&created)?)))
            .collect::<Result<Vec<_>, AppError>>()?;
        let queued_jobs: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM resource_jobs WHERE state = 'queued'",
            [],
            |row| row.get(0),
        )?;
        let pending_tasks: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT task_id) FROM executor_outbox WHERE state = 'pending'",
            [],
            |row| row.get(0),
        )?;
        let pending_jobs: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM resource_jobs j
             LEFT JOIN resource_job_delivery d ON d.job_id = j.id
             WHERE j.event_seq > COALESCE(d.acknowledged_seq, 0)",
            [],
            |row| row.get(0),
        )?;
        Ok(StoredActivity {
            tasks,
            queued_jobs: usize::try_from(queued_jobs).unwrap_or_default(),
            undelivered: usize::try_from(pending_tasks + pending_jobs).unwrap_or_default(),
        })
    }
}
