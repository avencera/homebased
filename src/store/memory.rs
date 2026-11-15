//! Started work that declared a memory peak, for the memory gate

use rusqlite::{OptionalExtension, params};

use super::Store;
use super::identity::stored_mem_peak;
use crate::domain::TaskId;
use crate::error::AppError;
use crate::machine::MachineId;
use crate::memory::Claim;
use crate::queue::RunPhase;

impl Store {
    /// Every task and GPU queue run on `machine` that started with a peak and has not ended
    ///
    /// A task is a claim from the moment its row exists, before its worker
    /// spawns, so no pass can start into the room it was admitted with. A run
    /// is a claim until its child exits; cleanup holds no memory worth counting
    pub fn memory_claims(&self, machine: MachineId) -> Result<Vec<Claim>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT routes.mem_peak_mib, tasks.child_pid FROM tasks
             JOIN origin_routes AS routes ON routes.task_id = tasks.id
             WHERE routes.mem_peak_mib IS NOT NULL
               AND tasks.resource_job_id IS NULL
               AND tasks.status IN ('queued', 'running')",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<u32>>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut claims = Vec::with_capacity(rows.len());
        for (peak, root) in rows {
            if let Some(peak) = stored_mem_peak(peak)? {
                claims.push(Claim { peak, root });
            }
        }

        for record in self.resources_on(machine)? {
            let Some(run) = record.run else {
                continue;
            };
            if !matches!(
                run.phase,
                RunPhase::Launching { .. } | RunPhase::Executing { .. } | RunPhase::Stopping { .. }
            ) {
                continue;
            }
            let Some(peak) = self.job(run.job)?.and_then(|job| job.spec.mem_peak) else {
                continue;
            };
            claims.push(Claim {
                peak,
                root: self.child_pid(run.task)?,
            });
        }
        Ok(claims)
    }

    fn child_pid(&self, task: TaskId) -> Result<Option<u32>, AppError> {
        Ok(self
            .conn
            .query_row(
                "SELECT child_pid FROM tasks WHERE id = ?1",
                params![task.to_string()],
                |row| row.get::<_, Option<u32>>(0),
            )
            .optional()?
            .flatten())
    }
}
