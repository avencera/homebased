//! Durable origin and executor task identities

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::Store;
use crate::dependency::TaskDependencies;
use crate::domain::TaskId;
use crate::error::AppError;
use crate::machine::MachineId;
use crate::submission::{
    ExecutionRecord, ExecutorIdentity, HeldPhase, OriginRoute, PreAcceptanceRejection,
    RejectionTombstone, RequestId, SubmissionState,
};

/// A durable identity operation failed without changing its existing owner
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// The UUID already identifies different content or a different owner
    #[error("identity conflict")]
    Conflict,
    /// The requested origin route does not exist
    #[error("origin route not found")]
    RouteNotFound,
    /// Storage or stored data failed
    #[error(transparent)]
    Storage(#[from] AppError),
}

impl IdentityError {
    /// Report an unusable saved owner of `task` as a cluster conflict
    pub(super) fn into_task_error(self, task: TaskId) -> AppError {
        match self {
            Self::Conflict | Self::RouteNotFound => AppError::ClusterTaskConflict { task },
            Self::Storage(error) => error,
        }
    }
}

fn encode<T: serde::Serialize>(value: &T) -> Result<String, IdentityError> {
    Ok(serde_json::to_string(value).map_err(AppError::from)?)
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, IdentityError> {
    Ok(serde_json::from_str(value).map_err(AppError::from)?)
}

fn decode_route(value: &str) -> Result<OriginRoute, IdentityError> {
    let route: OriginRoute = decode(value)?;
    route.validate().map_err(|error| {
        IdentityError::Storage(AppError::Internal {
            message: format!("invalid saved origin route: {error}"),
        })
    })?;
    Ok(route)
}

pub(super) fn executor_identity_on(
    conn: &Connection,
    task: TaskId,
) -> Result<Option<ExecutorIdentity>, IdentityError> {
    let data: Option<String> = conn
        .query_row(
            "SELECT identity_json FROM executor_identities WHERE task_id=?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    data.as_deref().map(decode).transpose()
}

fn validate_route(route: &OriginRoute) -> Result<(), IdentityError> {
    route.validate().map_err(|error| {
        IdentityError::Storage(AppError::Internal {
            message: format!("invalid origin route: {error}"),
        })
    })
}

/// Whether a retry repeats the saved route's request, owner, thread, and content
///
/// Direct retries keep the first route's generated task, destination, and
/// callback context
fn same_route_identity(left: &OriginRoute, right: &OriginRoute) -> bool {
    left.request == right.request
        && left.origin_machine == right.origin_machine
        && left.thread == right.thread
        && left.spec == right.spec
}

fn save_route(tx: &rusqlite::Transaction<'_>, route: &OriginRoute) -> Result<(), IdentityError> {
    tx.execute(
        "UPDATE origin_routes SET route_json=?1 WHERE task_id=?2",
        params![encode(route)?, route.task.to_string()],
    )
    .map_err(storage)?;
    Ok(())
}

/// Insert the first executor identity of `task`, owned by `origin`
pub(super) fn insert_identity_on(
    conn: &Connection,
    task: TaskId,
    origin: MachineId,
    identity: &ExecutorIdentity,
) -> Result<(), AppError> {
    conn.execute(
        "INSERT INTO executor_identities (task_id,origin_machine,identity_json) VALUES (?1,?2,?3)",
        params![
            task.to_string(),
            origin.to_string(),
            serde_json::to_string(identity)?
        ],
    )?;
    Ok(())
}

/// Whether a task row already uses `task`, which then cannot take a new identity
pub(super) fn task_row_exists(conn: &Connection, task: TaskId) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
        [task.to_string()],
        |row| row.get(0),
    )
}

fn storage(error: rusqlite::Error) -> IdentityError {
    IdentityError::Storage(AppError::from(error))
}

impl Store {
    /// Insert an origin route once; an identical request returns the saved route
    pub fn insert_origin_route(
        &mut self,
        route: &OriginRoute,
    ) -> Result<OriginRoute, IdentityError> {
        self.insert_origin_route_after(route, None)
    }

    /// Insert an origin route with the dependencies it was submitted with
    ///
    /// The dependencies take part in the retry identity: the same request with
    /// a different `after` list is a conflict. Only a route with dependencies
    /// may start held
    pub fn insert_origin_route_after(
        &mut self,
        route: &OriginRoute,
        after: Option<&TaskDependencies>,
    ) -> Result<OriginRoute, IdentityError> {
        validate_route(route)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let saved: Option<(String, Option<String>)> = tx
            .query_row(
                "SELECT route_json,after_json FROM origin_routes WHERE request_id=?1",
                [route.request.0.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(storage)?;
        if let Some((saved, saved_after)) = saved {
            let existing = decode_route(&saved)?;
            let saved_after: Option<TaskDependencies> =
                saved_after.as_deref().map(decode).transpose()?;
            if !same_route_identity(&existing, route) || saved_after.as_ref() != after {
                return Err(IdentityError::Conflict);
            }
            return Ok(existing);
        }
        if let SubmissionState::Held { phase } = &route.submission
            && (after.is_none() || !matches!(phase, HeldPhase::Waiting))
        {
            return Err(IdentityError::Conflict);
        }
        let occupied: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM origin_routes WHERE task_id=?1)",
                [route.task.to_string()],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if occupied {
            return Err(IdentityError::Conflict);
        }
        let after_json = after.map(encode).transpose()?;
        tx.execute(
            "INSERT INTO origin_routes (request_id,task_id,route_json,after_json)
             VALUES (?1,?2,?3,?4)",
            params![
                route.request.0.to_string(),
                route.task.to_string(),
                encode(route)?,
                after_json
            ],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(route.clone())
    }

    /// Read a saved origin route by caller request UUID
    pub fn origin_route_by_request(
        &self,
        request: RequestId,
    ) -> Result<Option<OriginRoute>, IdentityError> {
        let data: Option<String> = self
            .conn
            .query_row(
                "SELECT route_json FROM origin_routes WHERE request_id=?1",
                [request.0.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        let route = data.as_deref().map(decode_route).transpose()?;
        if route.as_ref().is_some_and(|route| route.request != request) {
            return Err(IdentityError::Conflict);
        }

        Ok(route)
    }

    /// Read a saved origin route by global task UUID
    pub fn origin_route_by_task(&self, task: TaskId) -> Result<Option<OriginRoute>, IdentityError> {
        let data: Option<String> = self
            .conn
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        let route = data.as_deref().map(decode_route).transpose()?;
        if route.as_ref().is_some_and(|route| route.task != task) {
            return Err(IdentityError::Conflict);
        }

        Ok(route)
    }

    /// Find unresolved origin routes for one startup recovery pass
    pub fn unknown_origin_routes(&self) -> Result<Vec<OriginRoute>, IdentityError> {
        let mut statement = self
            .conn
            .prepare("SELECT request_id,task_id,route_json FROM origin_routes ORDER BY request_id")
            .map_err(storage)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(storage)?;
        let mut routes = Vec::new();
        for row in rows {
            let (request, task, json) = row.map_err(storage)?;
            let route = decode_route(&json)?;
            if route.request.0.to_string() != request || route.task.to_string() != task {
                return Err(IdentityError::Conflict);
            }
            if matches!(route.submission, SubmissionState::AcceptanceUnknown) {
                routes.push(route);
            }
        }
        Ok(routes)
    }

    /// Set a definitive submission result only while acceptance is unknown
    ///
    /// A released held route resolves the same way. Nobody waits on its submit
    /// response, so a refusal also queues its terminal event
    pub fn resolve_origin_route(
        &mut self,
        task: TaskId,
        outcome: SubmissionState,
    ) -> Result<OriginRoute, IdentityError> {
        if matches!(
            outcome,
            SubmissionState::AcceptanceUnknown | SubmissionState::Held { .. }
        ) {
            return Err(IdentityError::Conflict);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let data: Option<String> = tx
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        let mut route = decode_route(data.as_deref().ok_or(IdentityError::RouteNotFound)?)?;
        match &route.submission {
            SubmissionState::AcceptanceUnknown => {
                route.submission = outcome;
                route.last_updated_at = chrono::Utc::now();
                save_route(&tx, &route)?;
                tx.commit().map_err(storage)?;
                Ok(route)
            }
            SubmissionState::Held {
                phase: HeldPhase::Launching,
            } => {
                let refused = match &outcome {
                    SubmissionState::Rejected { reason } => {
                        Some(super::dependency::refused_launch_ending(reason))
                    }
                    _ => None,
                };
                route.submission = outcome;
                route.last_updated_at = chrono::Utc::now();
                match refused {
                    Some(ending) => super::dependency::append_unlaunched_event_on(
                        &tx,
                        &self.tasks_dir,
                        &mut route,
                        ending,
                    )?,
                    None => save_route(&tx, &route)?,
                }
                tx.commit().map_err(storage)?;
                Ok(route)
            }
            old if encode(old)? == encode(&outcome)? => Ok(route),
            _ => Err(IdentityError::Conflict),
        }
    }

    /// Read accepted identity or rejection tombstone by task UUID
    pub fn executor_identity(
        &self,
        task: TaskId,
    ) -> Result<Option<ExecutorIdentity>, IdentityError> {
        executor_identity_on(&self.conn, task)
    }

    /// Atomically accept a task UUID, or return its existing identity
    pub fn accept_execution(
        &mut self,
        record: &ExecutionRecord,
    ) -> Result<ExecutorIdentity, IdentityError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        if let Some(existing) = executor_identity_on(&tx, record.task)? {
            if let ExecutorIdentity::Accepted(saved) = &existing
                && (saved.origin_machine != record.origin_machine
                    || saved.execution_machine != record.execution_machine
                    || saved.spec != record.spec)
            {
                return Err(IdentityError::Conflict);
            }
            if let ExecutorIdentity::Rejected(saved) = &existing
                && (saved.origin_machine != record.origin_machine
                    || saved.execution_machine != record.execution_machine)
            {
                return Err(IdentityError::Conflict);
            }
            return Ok(existing);
        }
        if task_row_exists(&tx, record.task).map_err(storage)? {
            return Err(IdentityError::Conflict);
        }
        let accepted = ExecutorIdentity::Accepted(record.clone());
        insert_identity_on(&tx, record.task, record.origin_machine, &accepted)?;
        tx.commit().map_err(storage)?;
        Ok(accepted)
    }

    /// Store a definitive rejection, or return the identity that won the race
    pub fn reject_execution(
        &mut self,
        tombstone: &RejectionTombstone,
    ) -> Result<ExecutorIdentity, IdentityError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        if let Some(existing) = executor_identity_on(&tx, tombstone.task)? {
            let (origin, execution) = match &existing {
                ExecutorIdentity::Accepted(row) => (row.origin_machine, row.execution_machine),
                ExecutorIdentity::Rejected(row) => (row.origin_machine, row.execution_machine),
            };
            if origin != tombstone.origin_machine || execution != tombstone.execution_machine {
                return Err(IdentityError::Conflict);
            }
            return Ok(existing);
        }
        if task_row_exists(&tx, tombstone.task).map_err(storage)? {
            return Err(IdentityError::Conflict);
        }
        let rejected = ExecutorIdentity::Rejected(tombstone.clone());
        insert_identity_on(&tx, tombstone.task, tombstone.origin_machine, &rejected)?;
        tx.commit().map_err(storage)?;
        Ok(rejected)
    }

    /// Abandon an unaccepted UUID without making an absent lookup a rejection
    pub fn abandon_before_acceptance(
        &mut self,
        task: TaskId,
        origin: MachineId,
        execution: MachineId,
    ) -> Result<ExecutorIdentity, IdentityError> {
        self.reject_execution(&RejectionTombstone {
            task,
            origin_machine: origin,
            execution_machine: execution,
            reason: PreAcceptanceRejection::Abandoned.as_str().into(),
        })
    }
}

#[cfg(test)]
mod tests;
