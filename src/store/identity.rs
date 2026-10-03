//! Durable origin and executor task identities

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::Store;
use crate::dependency::TaskDependencies;
use crate::domain::{ProcessStatus, TaskId};
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

fn decode_identity(value: &str) -> Result<ExecutorIdentity, IdentityError> {
    let identity: ExecutorIdentity = decode(value)?;
    if let ExecutorIdentity::Accepted(record) = &identity
        && !record.has_valid_spec_owners()
    {
        return Err(IdentityError::Conflict);
    }
    Ok(identity)
}

pub(super) fn executor_identity_on(
    conn: &rusqlite::Connection,
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
    data.as_deref().map(decode_identity).transpose()
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
            "INSERT INTO origin_routes (request_id,task_id,execution_machine,spec_json,route_json,after_json) VALUES (?1,?2,?3,?4,?5,?6)",
            params![route.request.0.to_string(), route.task.to_string(), route.execution_machine.as_uuid().to_string(), encode(&route.spec)?, encode(route)?, after_json],
        ).map_err(storage)?;
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
        data.as_deref()
            .map(decode_route)
            .transpose()
            .and_then(|route| {
                if route.as_ref().is_some_and(|route| route.request != request) {
                    return Err(IdentityError::Conflict);
                }
                Ok(route)
            })
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
        data.as_deref()
            .map(decode_route)
            .transpose()
            .and_then(|route| {
                if route.as_ref().is_some_and(|route| route.task != task) {
                    return Err(IdentityError::Conflict);
                }
                Ok(route)
            })
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
                route.last_updated_at = Some(chrono::Utc::now());
                tx.execute(
                    "UPDATE origin_routes SET route_json=?1 WHERE task_id=?2",
                    params![encode(&route)?, task.to_string()],
                )
                .map_err(storage)?;
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
                route.last_updated_at = Some(chrono::Utc::now());
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
        if record.current_spec().is_none() || !record.has_valid_spec_owners() {
            return Err(IdentityError::Conflict);
        }
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
        let task_exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
                [record.task.to_string()],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if task_exists {
            return Err(IdentityError::Conflict);
        }
        let accepted = ExecutorIdentity::Accepted(record.clone());
        tx.execute("INSERT INTO executor_identities (task_id,origin_machine,identity_json) VALUES (?1,?2,?3)",
            params![record.task.to_string(), record.origin_machine.as_uuid().to_string(), encode(&accepted)?]).map_err(storage)?;
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
        let identity = Self::reject_execution_in(&tx, tombstone)?;
        tx.commit().map_err(storage)?;
        Ok(identity)
    }

    /// Retain a rejection inside a caller-owned transaction
    pub(crate) fn reject_execution_in(
        tx: &rusqlite::Transaction<'_>,
        tombstone: &RejectionTombstone,
    ) -> Result<ExecutorIdentity, IdentityError> {
        if let Some(existing) = executor_identity_on(tx, tombstone.task)? {
            let (origin, execution) = match &existing {
                ExecutorIdentity::Accepted(row) => (row.origin_machine, row.execution_machine),
                ExecutorIdentity::Rejected(row) => (row.origin_machine, row.execution_machine),
            };
            if origin != tombstone.origin_machine || execution != tombstone.execution_machine {
                return Err(IdentityError::Conflict);
            }
            return Ok(existing);
        }
        let task_exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
                [tombstone.task.to_string()],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if task_exists {
            return Err(IdentityError::Conflict);
        }
        let rejected = ExecutorIdentity::Rejected(tombstone.clone());
        tx.execute("INSERT INTO executor_identities (task_id,origin_machine,identity_json) VALUES (?1,?2,?3)",
            params![tombstone.task.to_string(), tombstone.origin_machine.as_uuid().to_string(), encode(&rejected)?]).map_err(storage)?;
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

    /// Cancel an unaccepted UUID; a prior accepted identity is unchanged
    pub fn cancel_before_acceptance(
        &mut self,
        task: TaskId,
        origin: MachineId,
        execution: MachineId,
    ) -> Result<ExecutorIdentity, IdentityError> {
        self.reject_execution(&RejectionTombstone {
            task,
            origin_machine: origin,
            execution_machine: execution,
            reason: PreAcceptanceRejection::Cancelled.as_str().into(),
        })
    }

    /// Retain the latest process state for an accepted task after detail cleanup
    pub fn update_execution_state(
        &mut self,
        task: TaskId,
        state: ProcessStatus,
    ) -> Result<ExecutorIdentity, IdentityError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let mut identity = executor_identity_on(&tx, task)?.ok_or(IdentityError::RouteNotFound)?;
        match &mut identity {
            ExecutorIdentity::Accepted(record) => record.state = state,
            ExecutorIdentity::Rejected(_) => return Err(IdentityError::Conflict),
        }
        tx.execute(
            "UPDATE executor_identities SET identity_json=?1 WHERE task_id=?2",
            params![encode(&identity)?, task.to_string()],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::{Arc, Barrier};

    use tempfile::tempdir;

    use super::{IdentityError, encode};
    use crate::domain::{ProcessStatus, TaskEnv, TaskId};
    use crate::machine::MachineId;
    use crate::store::Store;
    use crate::submission::{
        CallbackContext, ExecutionRecord, ExecutorIdentity, OriginRoute, RequestId, SubmissionState,
    };
    use rusqlite::params;

    fn spec() -> crate::spec::NormalizedSpec {
        serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "test task",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": {"type": "task", "command": ["echo", "hello"]}
        }))
        .unwrap()
    }

    fn route() -> OriginRoute {
        let spec = spec();
        OriginRoute {
            request: RequestId::new(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            execution_machine: MachineId::new(),
            thread: spec.thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: Path::new("/tmp").to_path_buf(),
                codex: Path::new("/bin/echo").to_path_buf().into(),
            },
            spec: spec.into(),
            submission: SubmissionState::AcceptanceUnknown,
            last_execution_state: None,
            last_updated_at: Some(chrono::Utc::now()),
            last_accepted_seq: 0,
            last_settled_seq: 0,
        }
    }

    #[test]
    fn request_identity_and_origin_transition() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let mut first = route();
        first.spec.current_mut().unwrap().machine = Some("remote-executor".parse().unwrap());
        store.insert_origin_route(&first).unwrap();
        let saved = store.insert_origin_route(&first).unwrap();
        assert_eq!(saved.task, first.task);
        assert_eq!(saved.callback, first.callback);
        let mut changed_origin = first.clone();
        changed_origin.origin_machine = MachineId::new();
        assert!(matches!(
            store.insert_origin_route(&changed_origin),
            Err(IdentityError::Conflict)
        ));
        let mut retry = first.clone();
        retry.task = TaskId::new();
        retry.execution_machine = MachineId::new();
        retry.callback.cwd = Path::new("/other-origin-directory").to_path_buf();
        let saved = store.insert_origin_route(&retry).unwrap();
        assert_eq!(saved.task, first.task);
        assert_eq!(saved.execution_machine, first.execution_machine);
        assert_eq!(saved.callback, first.callback);
        let mut changed_callback = first.clone();
        changed_callback.callback.cwd = Path::new("/other-origin-directory").to_path_buf();
        let saved = store.insert_origin_route(&changed_callback).unwrap();
        assert_eq!(saved.task, first.task);
        assert_eq!(saved.callback, first.callback);
        let mut reused_task = first.clone();
        reused_task.request = RequestId::new();
        assert!(matches!(
            store.insert_origin_route(&reused_task),
            Err(IdentityError::Conflict)
        ));
        let mut changed_content = first.clone();
        changed_content.spec.current_mut().unwrap().cwd = Path::new("/different").to_path_buf();
        assert!(matches!(
            store.insert_origin_route(&changed_content),
            Err(IdentityError::Conflict)
        ));
        assert_eq!(
            store
                .origin_route_by_request(first.request)
                .unwrap()
                .unwrap()
                .task,
            first.task
        );
        assert_eq!(
            store
                .origin_route_by_task(first.task)
                .unwrap()
                .unwrap()
                .request,
            first.request
        );
        let resolved = store
            .resolve_origin_route(first.task, SubmissionState::Accepted)
            .unwrap();
        assert!(matches!(resolved.submission, SubmissionState::Accepted));
        assert!(matches!(
            store.resolve_origin_route(
                first.task,
                SubmissionState::Rejected {
                    reason: "late".into()
                }
            ),
            Err(IdentityError::Conflict)
        ));
    }

    #[test]
    fn saved_direct_route_json_is_readable_and_recoverable() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let route = route();
        let json = serde_json::to_value(&route).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO origin_routes (request_id,task_id,execution_machine,spec_json,route_json)
                 VALUES (?1,?2,?3,?4,?5)",
                params![
                    route.request.0.to_string(),
                    route.task.to_string(),
                    route.execution_machine.as_uuid().to_string(),
                    encode(route.spec.current().unwrap()).unwrap(),
                    serde_json::to_string(&json).unwrap(),
                ],
            )
            .unwrap();

        let saved = store
            .origin_route_by_request(route.request)
            .unwrap()
            .unwrap();
        assert!(matches!(
            saved.submission,
            SubmissionState::AcceptanceUnknown
        ));
        let unknown = store.unknown_origin_routes().unwrap();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown[0].request, saved.request);
    }

    #[test]
    fn accept_and_abandon_serialize_across_connections() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let task = TaskId::new();
        let origin = MachineId::new();
        let execution = MachineId::new();
        let barrier = Arc::new(Barrier::new(2));
        Store::open(&path).unwrap();
        let left_path = path.clone();
        let left_barrier = barrier.clone();
        let handle = std::thread::spawn(move || {
            let mut store = Store::open(&left_path).unwrap();
            left_barrier.wait();
            store
                .accept_execution(&ExecutionRecord {
                    task,
                    origin_machine: origin,
                    execution_machine: execution,
                    spec: spec().into(),
                    state: ProcessStatus::Queued,
                })
                .unwrap()
        });
        let mut store = Store::open(&path).unwrap();
        barrier.wait();
        let abandoned = store
            .abandon_before_acceptance(task, origin, execution)
            .unwrap();
        let accepted = handle.join().unwrap();
        assert_eq!(
            std::mem::discriminant(&abandoned),
            std::mem::discriminant(&accepted)
        );
        let saved = Store::open(&path)
            .unwrap()
            .executor_identity(task)
            .unwrap()
            .unwrap();
        assert_eq!(
            std::mem::discriminant(&saved),
            std::mem::discriminant(&accepted)
        );
    }

    #[test]
    fn concurrent_direct_origin_route_insertions_share_the_first_route() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let mut first = route();
        first.spec.current_mut().unwrap().machine = Some("remote-executor".parse().unwrap());
        let mut contender = first.clone();
        contender.task = TaskId::new();
        contender.execution_machine = MachineId::new();
        contender.callback.cwd = Path::new("/other-origin-directory").to_path_buf();
        Store::open(&path).unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let left_path = path.clone();
        let left_barrier = barrier.clone();
        let left_route = first.clone();
        let left = std::thread::spawn(move || {
            let mut store = Store::open(&left_path).unwrap();
            left_barrier.wait();
            store.insert_origin_route(&left_route).unwrap()
        });
        let mut store = Store::open(&path).unwrap();
        barrier.wait();
        let right = store.insert_origin_route(&contender).unwrap();
        let left = left.join().unwrap();

        assert_eq!(left.task, right.task);
        assert_eq!(left.origin_machine, right.origin_machine);
        assert_eq!(left.execution_machine, right.execution_machine);
        assert_eq!(left.callback, right.callback);
        let saved = Store::open(&path)
            .unwrap()
            .origin_route_by_request(first.request)
            .unwrap()
            .unwrap();
        assert_eq!(saved.task, left.task);
        assert_eq!(saved.execution_machine, left.execution_machine);
        assert_eq!(saved.callback, left.callback);
    }

    #[test]
    fn tombstone_survives_reopen_and_blocks_submission() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let task = TaskId::new();
        let origin = MachineId::new();
        let execution = MachineId::new();
        Store::open(&path)
            .unwrap()
            .cancel_before_acceptance(task, origin, execution)
            .unwrap();
        let mut reopened = Store::open(&path).unwrap();
        let found = reopened
            .accept_execution(&ExecutionRecord {
                task,
                origin_machine: origin,
                execution_machine: execution,
                spec: spec().into(),
                state: ProcessStatus::Queued,
            })
            .unwrap();
        assert!(
            matches!(found, ExecutorIdentity::Rejected(t) if t.reason == "cancelled_before_acceptance")
        );
    }

    #[test]
    fn accepted_identity_retains_state_and_rejects_changed_content() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let mut record = ExecutionRecord {
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            execution_machine: MachineId::new(),
            spec: spec().into(),
            state: ProcessStatus::Queued,
        };
        store.accept_execution(&record).unwrap();
        store
            .update_execution_state(record.task, ProcessStatus::Running)
            .unwrap();
        assert!(
            matches!(store.accept_execution(&record).unwrap(), ExecutorIdentity::Accepted(saved) if saved.state == ProcessStatus::Running)
        );
        record.spec.current_mut().unwrap().cwd = Path::new("/different").to_path_buf();
        assert!(matches!(
            store.accept_execution(&record),
            Err(IdentityError::Conflict)
        ));
    }
}
