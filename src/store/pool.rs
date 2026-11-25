//! Pools a coordinator runs, their machines, and what a temporary machine
//! has on it

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, params};

use super::{fmt_time, parse_time};
use crate::domain::TaskId;
use crate::error::AppError;
use crate::pool::boot::BootPayload;
use crate::pool::{Pool, PoolEnd, PoolMachine, PoolName, PoolState, SlotId};
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

    /// Move an open or stopped pool to releasing for `end`, and return it as
    /// stored; `None` when there is no such pool
    pub fn release_pool(&self, name: &PoolName, end: PoolEnd) -> Result<Option<Pool>, AppError> {
        self.immediate(|| {
            let Some(mut pool) = self.pool(name)? else {
                return Ok(None);
            };
            if !matches!(pool.state, PoolState::Open | PoolState::Stopped { .. }) {
                return Ok(Some(pool));
            }
            pool.state = PoolState::Releasing { end };
            self.conn.execute(
                "UPDATE pools SET state = ?2, pool_json = ?3 WHERE name = ?1",
                params![
                    pool.name.to_string(),
                    pool.state.as_str(),
                    serde_json::to_string(&pool)?
                ],
            )?;
            Ok(Some(pool))
        })
    }

    /// Replace a pool's record, keeping its stored state when that is further
    /// along than the new one
    ///
    /// A reconcile pass reads the pool, waits on cloudlease, and saves; a
    /// `pool release` saved in between must not be undone by that save
    pub fn update_pool(&self, pool: &Pool) -> Result<(), AppError> {
        self.immediate(|| {
            let Some(stored) = self.pool(&pool.name)? else {
                return Err(AppError::Internal {
                    message: format!("pool {} vanished while it was updated", pool.name),
                });
            };

            let mut pool = pool.clone();
            if stored.state.stage() > pool.state.stage() {
                pool.state = stored.state;
            }
            self.conn.execute(
                "UPDATE pools SET state = ?2, pool_json = ?3 WHERE name = ?1",
                params![
                    pool.name.to_string(),
                    pool.state.as_str(),
                    serde_json::to_string(&pool)?
                ],
            )?;
            Ok(())
        })
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

    /// Save a machine about to be asked for, with the create body every
    /// attempt of it sends
    pub fn insert_pool_machine(
        &self,
        machine: &PoolMachine,
        boot: &BootPayload,
    ) -> Result<(), AppError> {
        self.conn.execute(
            "INSERT INTO pool_machines
                 (slot, pool, state, machine_id, machine_json, requested_at, pending_boot)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
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
                serde_json::to_string(boot)?,
            ],
        )?;
        Ok(())
    }

    /// The create body of a machine cloudlease has not answered for yet
    pub fn pending_boot(&self, slot: SlotId) -> Result<Option<BootPayload>, AppError> {
        let json: Option<Option<String>> = self
            .conn
            .query_row(
                "SELECT pending_boot FROM pool_machines WHERE slot = ?1",
                [slot.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        json.flatten()
            .map(|json| Ok(serde_json::from_str(&json)?))
            .transpose()
    }

    /// Replace a machine's record; its create body is dropped once it is no
    /// longer `requested`
    pub fn update_pool_machine(&self, machine: &PoolMachine) -> Result<(), AppError> {
        let changed = self.conn.execute(
            "UPDATE pool_machines SET state = ?2, machine_id = ?3, machine_json = ?4,
                 pending_boot = CASE WHEN ?2 = 'requested' THEN pending_boot END
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
            // an orphaned route's events are never sent, so they owe no one
            "SELECT COUNT(DISTINCT task_id) FROM executor_outbox
             WHERE state = 'pending'
               AND task_id NOT IN (SELECT task_id FROM executor_event_routes)",
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
