//! Durable origin and executor task identities

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::{Store, resource_task_id_is_reserved};
use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::ActionId;
use crate::resource::background_launch::{
    RemoteBackgroundLaunchReceipt, ResourceBackgroundRejection,
};
use crate::resource::bound_action::ActionTaskReceipt;
use crate::submission::{
    ExecutionRecord, ExecutorIdentity, OriginRoute, PreAcceptanceRejection, RejectionTombstone,
    RequestId, ResourceActionRoutePhase, ResourceBackgroundRoutePhase, ResourceCancellationOutcome,
    ResourceCancellationReceipt, ResourceQueueOutcome, ResourceQueueReceipt, ResourceRoutePhase,
    SubmissionState,
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
            message: format!("invalid saved resource origin route: {error}"),
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

pub(super) fn origin_route_by_request_on(
    conn: &rusqlite::Connection,
    request: RequestId,
) -> Result<Option<OriginRoute>, IdentityError> {
    let data: Option<String> = conn
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

pub(super) fn origin_route_by_task_on(
    conn: &rusqlite::Connection,
    task: TaskId,
) -> Result<Option<OriginRoute>, IdentityError> {
    let data: Option<String> = conn
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

pub(super) fn executor_identity_on(
    conn: &rusqlite::Connection,
    task: TaskId,
) -> Result<Option<ExecutorIdentity>, IdentityError> {
    Ok(executor_identity_with_json_on(conn, task)?.map(|(identity, _)| identity))
}

/// Whether the saved origin column of one executor identity names this origin
///
/// The column indexes the origin outside the identity JSON, so resource
/// acceptance checks that both copies agree
pub(super) fn identity_origin_column_is_on(
    conn: &rusqlite::Connection,
    task: TaskId,
    origin_machine: MachineId,
) -> Result<bool, rusqlite::Error> {
    let saved: Option<String> = conn
        .query_row(
            "SELECT origin_machine FROM executor_identities WHERE task_id=?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(saved.is_some_and(|saved| saved == origin_machine.to_string()))
}

/// Read one executor identity with the exact saved JSON it was decoded from
///
/// A release proof keeps the JSON so its commit can detect any later rewrite
pub(super) fn executor_identity_with_json_on(
    conn: &rusqlite::Connection,
    task: TaskId,
) -> Result<Option<(ExecutorIdentity, String)>, IdentityError> {
    let data: Option<String> = conn
        .query_row(
            "SELECT identity_json FROM executor_identities WHERE task_id=?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    data.map(|json| Ok((decode_identity(&json)?, json)))
        .transpose()
}

/// Read acceptance-unknown routes of one resource submission type in request order
///
/// The SQL filter only selects candidates. Each decoded route must still name its
/// row identities and be unknown by `is_unknown`, or the whole scan is a conflict
fn acceptance_unknown_routes_on(
    conn: &rusqlite::Connection,
    submission_type: &str,
    is_unknown: fn(&SubmissionState) -> bool,
) -> Result<Vec<OriginRoute>, IdentityError> {
    let mut statement = conn
        .prepare(
            "SELECT request_id,task_id,route_json FROM origin_routes
             WHERE CASE WHEN json_valid(route_json) THEN
                json_extract(route_json, '$.submission.type') = ?1
                AND json_extract(route_json, '$.submission.phase.type') = 'acceptance_unknown'
             ELSE 0 END
             ORDER BY request_id",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map([submission_type], |row| {
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
        if route.request.0.to_string() != request
            || route.task.to_string() != task
            || !is_unknown(&route.submission)
        {
            return Err(IdentityError::Conflict);
        }
        routes.push(route);
    }
    Ok(routes)
}

fn validate_route(route: &OriginRoute) -> Result<(), IdentityError> {
    route.validate().map_err(|error| {
        IdentityError::Storage(AppError::Internal {
            message: format!("invalid resource origin route: {error}"),
        })
    })
}

fn same_route_identity(left: &OriginRoute, right: &OriginRoute) -> Result<bool, IdentityError> {
    if left.request != right.request || left.origin_machine != right.origin_machine {
        return Ok(false);
    }

    // direct retries retain the first route's generated task, destination, and callback context
    let same_spec = left.spec == right.spec;
    let same_thread = left.thread == right.thread;
    match (&left.submission, &right.submission) {
        (
            SubmissionState::Resource {
                resource: left_resource,
                ..
            },
            SubmissionState::Resource {
                resource: right_resource,
                ..
            },
        ) => Ok(left.task == right.task
            && left.execution_machine == right.execution_machine
            && same_thread
            && left.callback == right.callback
            && *left_resource == *right_resource
            && same_spec),
        (SubmissionState::Resource { .. }, _) | (_, SubmissionState::Resource { .. }) => Ok(false),
        // a retry must repeat the prepared identities, content, action, and callback
        (
            SubmissionState::ResourceAction {
                binding: left_binding,
                launch: left_launch,
                ..
            },
            SubmissionState::ResourceAction {
                binding: right_binding,
                launch: right_launch,
                ..
            },
        ) => Ok(left.task == right.task
            && left.execution_machine == right.execution_machine
            && same_thread
            && left.callback == right.callback
            && left_binding == right_binding
            && left_launch == right_launch
            && same_spec),
        (SubmissionState::ResourceAction { .. }, _)
        | (_, SubmissionState::ResourceAction { .. }) => Ok(false),
        // a retry must repeat the fixed identities, content, assignment, and callback
        (
            SubmissionState::ResourceBackground {
                binding: left_binding,
                ..
            },
            SubmissionState::ResourceBackground {
                binding: right_binding,
                ..
            },
        ) => Ok(left.task == right.task
            && left.execution_machine == right.execution_machine
            && same_thread
            && left.callback == right.callback
            && left_binding == right_binding
            && same_spec),
        (SubmissionState::ResourceBackground { .. }, _)
        | (_, SubmissionState::ResourceBackground { .. }) => Ok(false),
        _ => Ok(same_thread && same_spec),
    }
}

/// Definitive authority result that resolves one remote first background launch route
#[derive(Debug, Clone)]
pub enum ResourceBackgroundRouteResult {
    /// The authority saved this exact launch binding with the task
    Accepted(RemoteBackgroundLaunchReceipt),
    /// The authority refused the launch and wrote no task records
    Rejected(ResourceBackgroundRejection),
}

/// Definitive authority result that resolves one action-bound route
#[derive(Debug, Clone)]
pub enum ResourceActionRouteResult {
    /// The authority saved this exact action binding with the task
    Accepted(ActionTaskReceipt),
    /// The authority refused the launch and wrote no task records
    Rejected(crate::resource::bound_action::ResourceActionRejection),
}

pub(super) fn resource_action_route_by_action_on(
    conn: &rusqlite::Connection,
    action: ActionId,
) -> Result<Option<OriginRoute>, IdentityError> {
    let mut statement = conn
        .prepare(
            "SELECT route_json FROM origin_routes
             WHERE CASE WHEN json_valid(route_json) THEN
                json_extract(route_json, '$.submission.type') = 'resource_action'
                AND json_extract(route_json, '$.submission.binding.authority.action_id') = ?1
             ELSE 0 END",
        )
        .map_err(storage)?;
    let routes = statement
        .query_map([action.as_uuid().to_string()], |row| {
            row.get::<_, String>(0)
        })
        .map_err(storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(storage)?;
    let mut routes = routes
        .iter()
        .map(|json| decode_route(json))
        .collect::<Result<Vec<_>, _>>()?;
    if routes.len() > 1 {
        return Err(IdentityError::Conflict);
    }
    Ok(routes.pop())
}

fn check_resource_receipt(
    route: &OriginRoute,
    request: RequestId,
    task: TaskId,
    origin: MachineId,
    authority: MachineId,
    resource: crate::resource::ResourceId,
) -> Result<(), IdentityError> {
    let SubmissionState::Resource {
        resource: route_resource,
        ..
    } = &route.submission
    else {
        return Err(IdentityError::Conflict);
    };
    if route.request != request
        || route.task != task
        || route.origin_machine != origin
        || route.execution_machine != authority
        || *route_resource != resource
    {
        return Err(IdentityError::Conflict);
    }
    Ok(())
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
        validate_route(route)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let saved: Option<String> = tx
            .query_row(
                "SELECT route_json FROM origin_routes WHERE request_id=?1",
                [route.request.0.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        if let Some(saved) = saved {
            let existing = decode_route(&saved)?;
            if !same_route_identity(&existing, route)? {
                return Err(IdentityError::Conflict);
            }
            return Ok(existing);
        }
        if let SubmissionState::Resource { phase, .. } = &route.submission
            && !matches!(phase, ResourceRoutePhase::AcceptanceUnknown)
        {
            return Err(IdentityError::Conflict);
        }
        if let SubmissionState::ResourceBackground { phase, .. } = &route.submission
            && !matches!(phase, ResourceBackgroundRoutePhase::AcceptanceUnknown)
        {
            return Err(IdentityError::Conflict);
        }
        if let SubmissionState::ResourceAction { binding, phase, .. } = &route.submission {
            // one action binds at most one task, so another identity for it conflicts
            if !matches!(phase, ResourceActionRoutePhase::AcceptanceUnknown)
                || resource_action_route_by_action_on(&tx, binding.authority.action_id)?.is_some()
            {
                return Err(IdentityError::Conflict);
            }
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
        tx.execute(
            "INSERT INTO origin_routes (request_id,task_id,execution_machine,spec_json,route_json) VALUES (?1,?2,?3,?4,?5)",
            params![route.request.0.to_string(), route.task.to_string(), route.execution_machine.as_uuid().to_string(), encode(&route.spec)?, encode(route)?],
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

    /// Find unresolved resource origin routes for one startup recovery pass
    pub fn unknown_resource_origin_routes(&self) -> Result<Vec<OriginRoute>, IdentityError> {
        acceptance_unknown_routes_on(&self.conn, "resource", |submission| {
            matches!(
                submission,
                SubmissionState::Resource {
                    phase: ResourceRoutePhase::AcceptanceUnknown,
                    ..
                }
            )
        })
    }

    /// Read the one saved action-bound route for a resource action
    pub fn resource_action_route_by_action(
        &self,
        action: ActionId,
    ) -> Result<Option<OriginRoute>, IdentityError> {
        resource_action_route_by_action_on(&self.conn, action)
    }

    /// Find action-bound routes whose authority acceptance was unknown at startup
    pub fn unknown_resource_action_routes(&self) -> Result<Vec<OriginRoute>, IdentityError> {
        acceptance_unknown_routes_on(&self.conn, "resource_action", |submission| {
            matches!(
                submission,
                SubmissionState::ResourceAction {
                    phase: ResourceActionRoutePhase::AcceptanceUnknown,
                    ..
                }
            )
        })
    }

    /// Apply a definitive authority result to one action-bound route
    ///
    /// An acceptance must carry the exact saved binding and identities. A later
    /// phase is never replaced, and an identical retry leaves the route unchanged
    pub fn resolve_resource_action_route(
        &mut self,
        task: TaskId,
        result: &ResourceActionRouteResult,
    ) -> Result<OriginRoute, IdentityError> {
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
        let SubmissionState::ResourceAction {
            binding,
            launch,
            phase,
        } = &route.submission
        else {
            return Err(IdentityError::Conflict);
        };
        let target = match result {
            ResourceActionRouteResult::Accepted(receipt) => {
                let spec = route.current_spec().ok_or(IdentityError::Conflict)?;
                let digest = crate::submission::normalized_spec_sha256(spec)
                    .map_err(|error| IdentityError::Storage(error.into()))?;
                if receipt.kind != binding.kind
                    || receipt.authority != binding.authority
                    || receipt.request_id != route.request
                    || receipt.task_id != route.task
                    || receipt.normalized_spec_sha256 != digest
                {
                    return Err(IdentityError::Conflict);
                }
                ResourceActionRoutePhase::Accepted
            }
            ResourceActionRouteResult::Rejected(reason) => ResourceActionRoutePhase::Rejected {
                reason: reason.clone(),
            },
        };
        let next = match (phase, &target) {
            (ResourceActionRoutePhase::AcceptanceUnknown, _) => Some(target),
            (saved, target) if saved == target => None,
            _ => return Err(IdentityError::Conflict),
        };
        if let Some(phase) = next {
            route.submission = SubmissionState::ResourceAction {
                binding: *binding,
                launch: launch.clone(),
                phase,
            };
            route.last_updated_at = Some(chrono::Utc::now());
            validate_route(&route)?;
            save_route(&tx, &route)?;
        }
        tx.commit().map_err(storage)?;
        Ok(route)
    }

    /// Find remote first background launch routes whose acceptance was unknown at startup
    pub fn unknown_resource_background_routes(&self) -> Result<Vec<OriginRoute>, IdentityError> {
        acceptance_unknown_routes_on(&self.conn, "resource_background", |submission| {
            matches!(
                submission,
                SubmissionState::ResourceBackground {
                    phase: ResourceBackgroundRoutePhase::AcceptanceUnknown,
                    ..
                }
            )
        })
    }

    /// Apply a definitive authority result to one remote first background launch route
    ///
    /// An acceptance must carry the exact saved binding and identities. An accepted
    /// route is never replaced, and an identical retry leaves the route unchanged
    /// A refusal only replaces an unresolved phase, since a first queued event may
    /// already have accepted the route
    pub fn resolve_resource_background_route(
        &mut self,
        task: TaskId,
        result: &ResourceBackgroundRouteResult,
    ) -> Result<OriginRoute, IdentityError> {
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
        let SubmissionState::ResourceBackground { binding, phase } = &route.submission else {
            return Err(IdentityError::Conflict);
        };
        let target = match result {
            ResourceBackgroundRouteResult::Accepted(receipt) => {
                let spec = route.current_spec().ok_or(IdentityError::Conflict)?;
                let digest = crate::submission::normalized_spec_sha256(spec)
                    .map_err(|error| IdentityError::Storage(error.into()))?;
                if receipt.binding != *binding
                    || receipt.request_id != route.request
                    || receipt.task_id != route.task
                    || receipt.normalized_spec_sha256 != digest
                {
                    return Err(IdentityError::Conflict);
                }
                ResourceBackgroundRoutePhase::Accepted
            }
            ResourceBackgroundRouteResult::Rejected(reason) => {
                ResourceBackgroundRoutePhase::Rejected {
                    reason: reason.clone(),
                }
            }
        };
        let next = match (phase, &target) {
            (ResourceBackgroundRoutePhase::AcceptanceUnknown, _)
            | (
                ResourceBackgroundRoutePhase::Rejected { .. },
                ResourceBackgroundRoutePhase::Accepted,
            ) => Some(target),
            (saved, target) if saved == target => None,
            _ => return Err(IdentityError::Conflict),
        };
        if let Some(phase) = next {
            route.submission = SubmissionState::ResourceBackground {
                binding: *binding,
                phase,
            };
            route.last_updated_at = Some(chrono::Utc::now());
            validate_route(&route)?;
            save_route(&tx, &route)?;
        }
        tx.commit().map_err(storage)?;
        Ok(route)
    }

    /// Set a definitive submission result only while acceptance is unknown
    pub fn resolve_origin_route(
        &mut self,
        task: TaskId,
        outcome: SubmissionState,
    ) -> Result<OriginRoute, IdentityError> {
        if matches!(
            outcome,
            SubmissionState::AcceptanceUnknown
                | SubmissionState::Resource { .. }
                | SubmissionState::ResourceAction { .. }
                | SubmissionState::ResourceBackground { .. }
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
            old if encode(old)? == encode(&outcome)? => Ok(route),
            _ => Err(IdentityError::Conflict),
        }
    }

    /// Apply a definitive resource queue response without changing a later phase
    pub fn resolve_resource_route(
        &mut self,
        receipt: &ResourceQueueReceipt,
    ) -> Result<OriginRoute, IdentityError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let data: Option<String> = tx
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [receipt.task.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        let mut route = decode_route(data.as_deref().ok_or(IdentityError::RouteNotFound)?)?;
        check_resource_receipt(
            &route,
            receipt.request,
            receipt.task,
            receipt.origin_machine,
            receipt.authority_machine,
            receipt.resource,
        )?;
        let SubmissionState::Resource { resource, phase } = &route.submission else {
            return Err(IdentityError::Conflict);
        };
        let next = match (phase, &receipt.outcome) {
            (ResourceRoutePhase::AcceptanceUnknown, ResourceQueueOutcome::Waiting)
            | (ResourceRoutePhase::Waiting, ResourceQueueOutcome::Waiting) => {
                Some(ResourceRoutePhase::Waiting)
            }
            (ResourceRoutePhase::AcceptanceUnknown, ResourceQueueOutcome::Rejected { reason }) => {
                Some(ResourceRoutePhase::Rejected {
                    reason: reason.clone(),
                })
            }
            (
                ResourceRoutePhase::Rejected { reason: saved },
                ResourceQueueOutcome::Rejected { reason },
            ) if saved == reason => None,
            (ResourceRoutePhase::Activated, ResourceQueueOutcome::Waiting) => None,
            (ResourceRoutePhase::CancelledBeforeLaunch, ResourceQueueOutcome::Waiting) => None,
            (
                ResourceRoutePhase::CancelledBeforeLaunch,
                ResourceQueueOutcome::Rejected { reason },
            ) if reason == "cancelled_before_launch" => None,
            _ => return Err(IdentityError::Conflict),
        };
        if let Some(phase) = next {
            route.submission = SubmissionState::Resource {
                resource: *resource,
                phase,
            };
            route.last_updated_at = Some(chrono::Utc::now());
            validate_route(&route)?;
            save_route(&tx, &route)?;
        }
        tx.commit().map_err(storage)?;
        Ok(route)
    }

    /// Apply a definitive authority cancellation receipt before task activation
    pub fn cancel_resource_route_before_launch(
        &mut self,
        receipt: &ResourceCancellationReceipt,
    ) -> Result<OriginRoute, IdentityError> {
        if receipt.requester_machine != receipt.origin_machine
            || !matches!(
                &receipt.outcome,
                ResourceCancellationOutcome::PreventedBeforeAcceptance
                    | ResourceCancellationOutcome::CancelledBeforeLaunch
            )
        {
            return Err(IdentityError::Conflict);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let data: Option<String> = tx
            .query_row(
                "SELECT route_json FROM origin_routes WHERE task_id=?1",
                [receipt.task.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        let mut route = decode_route(data.as_deref().ok_or(IdentityError::RouteNotFound)?)?;
        check_resource_receipt(
            &route,
            receipt.request,
            receipt.task,
            receipt.origin_machine,
            receipt.authority_machine,
            receipt.resource,
        )?;
        let SubmissionState::Resource { resource, phase } = &route.submission else {
            return Err(IdentityError::Conflict);
        };
        let next = match (phase, &receipt.outcome) {
            (
                ResourceRoutePhase::AcceptanceUnknown,
                ResourceCancellationOutcome::PreventedBeforeAcceptance
                | ResourceCancellationOutcome::CancelledBeforeLaunch,
            )
            | (ResourceRoutePhase::Waiting, ResourceCancellationOutcome::CancelledBeforeLaunch) => {
                Some(ResourceRoutePhase::CancelledBeforeLaunch)
            }
            (ResourceRoutePhase::CancelledBeforeLaunch, _) => None,
            _ => return Err(IdentityError::Conflict),
        };
        if let Some(phase) = next {
            route.submission = SubmissionState::Resource {
                resource: *resource,
                phase,
            };
            route.last_updated_at = Some(chrono::Utc::now());
            validate_route(&route)?;
            save_route(&tx, &route)?;
        }
        tx.commit().map_err(storage)?;
        Ok(route)
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
            if matches!(&existing, ExecutorIdentity::Accepted(_))
                && resource_task_id_is_reserved(&tx, record.task).map_err(storage)?
            {
                return Err(IdentityError::Conflict);
            }
            return Ok(existing);
        }
        if resource_task_id_is_reserved(&tx, record.task).map_err(storage)? {
            return Err(IdentityError::Conflict);
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
    use crate::events::{EventPayload, TaskEvent};
    use crate::resource::bound_action::ResourceActionLaunch;
    use std::path::Path;
    use std::sync::{Arc, Barrier};

    use tempfile::tempdir;

    use super::{IdentityError, ResourceActionRouteResult, encode};
    use crate::domain::{ProcessStatus, TaskEnv, TaskId};
    use crate::machine::MachineId;
    use crate::resource::bound_action::ActionTaskReceipt;
    use crate::resource::{
        ActionId, AssignmentRevision, Resource, ResourceId, ResourceRevision, SupervisorAddress,
    };
    use crate::store::Store;
    use crate::submission::{
        CallbackContext, ExecutionRecord, ExecutorIdentity, OriginRoute, PreAcceptanceRejection,
        RequestId, ResourceActionRoutePhase, ResourceCancellationOutcome,
        ResourceCancellationReceipt, ResourceQueueOutcome, ResourceQueueReceipt,
        ResourceRoutePhase, SubmissionState,
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

    fn resource_route() -> OriginRoute {
        let spec = spec();
        OriginRoute::new_resource_waiting(crate::submission::NewResourceRoute {
            request: RequestId::new(),
            task: TaskId::new(),
            origin_machine: MachineId::new(),
            authority_machine: MachineId::new(),
            thread: spec.thread,
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: Path::new("/tmp").to_path_buf(),
                codex: Path::new("/bin/echo").to_path_buf().into(),
            },
            spec,
            resource: ResourceId::new(),
        })
        .unwrap()
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
        let mut store = Store::open(&dir.path().join("db")).unwrap();
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
        let resource = resource_route();
        store.insert_origin_route(&resource).unwrap();
        let unknown = store.unknown_origin_routes().unwrap();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown[0].request, saved.request);
        let unknown_resource = store.unknown_resource_origin_routes().unwrap();
        assert_eq!(unknown_resource.len(), 1);
        assert_eq!(unknown_resource[0].request, resource.request);
    }

    #[test]
    fn unknown_resource_scan_selects_only_resource_acceptance_unknown_routes() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let direct = route();
        store.insert_origin_route(&direct).unwrap();

        let unresolved = resource_route();
        store.insert_origin_route(&unresolved).unwrap();

        let waiting = resource_route();
        store.insert_origin_route(&waiting).unwrap();
        let SubmissionState::Resource { resource, .. } = &waiting.submission else {
            unreachable!();
        };
        store
            .resolve_resource_route(&ResourceQueueReceipt {
                request: waiting.request,
                task: waiting.task,
                origin_machine: waiting.origin_machine,
                authority_machine: waiting.execution_machine,
                resource: *resource,
                outcome: ResourceQueueOutcome::Waiting,
            })
            .unwrap();

        let activated = resource_route();
        store.insert_origin_route(&activated).unwrap();
        store
            .accept_inbound_event(&TaskEvent {
                task: activated.task,
                seq: std::num::NonZeroU64::new(1).unwrap(),
                origin_machine: activated.origin_machine,
                execution_machine: activated.execution_machine,
                payload: EventPayload::State {
                    status: ProcessStatus::Queued,
                },
            })
            .unwrap();

        let cancelled = resource_route();
        store.insert_origin_route(&cancelled).unwrap();
        let SubmissionState::Resource { resource, .. } = &cancelled.submission else {
            unreachable!();
        };
        store
            .cancel_resource_route_before_launch(&ResourceCancellationReceipt {
                cancellation: uuid::Uuid::now_v7(),
                requester_machine: cancelled.origin_machine,
                request: cancelled.request,
                task: cancelled.task,
                origin_machine: cancelled.origin_machine,
                authority_machine: cancelled.execution_machine,
                resource: *resource,
                target_phase: ResourceRoutePhase::AcceptanceUnknown,
                outcome: ResourceCancellationOutcome::CancelledBeforeLaunch,
            })
            .unwrap();

        let rejected = resource_route();
        store.insert_origin_route(&rejected).unwrap();
        let SubmissionState::Resource { resource, .. } = &rejected.submission else {
            unreachable!();
        };
        store
            .resolve_resource_route(&ResourceQueueReceipt {
                request: rejected.request,
                task: rejected.task,
                origin_machine: rejected.origin_machine,
                authority_machine: rejected.execution_machine,
                resource: *resource,
                outcome: ResourceQueueOutcome::Rejected {
                    reason: "resource unavailable".into(),
                },
            })
            .unwrap();

        let resource_unknown = store.unknown_resource_origin_routes().unwrap();
        assert_eq!(resource_unknown.len(), 1);
        assert_eq!(resource_unknown[0].request, unresolved.request);
        assert_eq!(resource_unknown[0].task, unresolved.task);

        let direct_unknown = store.unknown_origin_routes().unwrap();
        assert_eq!(direct_unknown.len(), 1);
        assert_eq!(direct_unknown[0].request, direct.request);

        store
            .conn
            .execute(
                "INSERT INTO origin_routes (request_id,task_id,execution_machine,spec_json,route_json)
                 VALUES (?1,?2,?3,?4,?5)",
                params![
                    RequestId::new().0.to_string(),
                    TaskId::new().to_string(),
                    MachineId::new().as_uuid().to_string(),
                    encode(&spec()).unwrap(),
                    "{malformed route JSON",
                ],
            )
            .unwrap();
        assert_eq!(store.unknown_resource_origin_routes().unwrap().len(), 1);
    }

    #[test]
    fn resource_route_validation_and_idempotent_identity_include_callback() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = resource_route();
        store.insert_origin_route(&route).unwrap();
        assert_eq!(store.insert_origin_route(&route).unwrap().task, route.task);

        let mut changed_callback = route.clone();
        changed_callback.callback.env.home = "/different-origin".into();
        assert!(matches!(
            store.insert_origin_route(&changed_callback),
            Err(IdentityError::Conflict)
        ));
        let mut changed_owner = route.clone();
        changed_owner.execution_machine = MachineId::new();
        assert!(matches!(
            store.insert_origin_route(&changed_owner),
            Err(IdentityError::Conflict)
        ));
        let mut changed_task = route.clone();
        changed_task.task = TaskId::new();
        assert!(matches!(
            store.insert_origin_route(&changed_task),
            Err(IdentityError::Conflict)
        ));
        let mut changed_resource = route.clone();
        if let SubmissionState::Resource { resource, phase } = &route.submission {
            let original_resource = *resource;
            let changed_id = ResourceId::new();
            changed_resource.submission = SubmissionState::Resource {
                resource: changed_id,
                phase: phase.clone(),
            };
            assert_ne!(original_resource, changed_id);
        }
        assert!(matches!(
            store.insert_origin_route(&changed_resource),
            Err(IdentityError::Conflict)
        ));

        let mut invalid = resource_route();
        invalid.thread = crate::domain::ThreadId(uuid::Uuid::now_v7());
        assert!(matches!(
            store.insert_origin_route(&invalid),
            Err(IdentityError::Storage(_))
        ));
        invalid = route.clone();
        invalid.spec.current_mut().unwrap().machine =
            Some(serde_json::from_value(serde_json::json!("gpu-authority")).unwrap());
        assert!(matches!(
            store.insert_origin_route(&invalid),
            Err(IdentityError::Storage(_))
        ));

        store
            .conn
            .execute(
                "UPDATE origin_routes SET route_json=?1 WHERE request_id=?2",
                params![
                    serde_json::to_string(&invalid).unwrap(),
                    route.request.0.to_string()
                ],
            )
            .unwrap();
        assert!(matches!(
            store.origin_route_by_request(route.request),
            Err(IdentityError::Storage(_))
        ));
    }

    #[test]
    fn resource_queue_receipt_cas_does_not_overwrite_activation() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = resource_route();
        store.insert_origin_route(&route).unwrap();
        let SubmissionState::Resource { resource, .. } = route.submission else {
            unreachable!();
        };
        let receipt = ResourceQueueReceipt {
            request: route.request,
            task: route.task,
            origin_machine: route.origin_machine,
            authority_machine: route.execution_machine,
            resource,
            outcome: ResourceQueueOutcome::Waiting,
        };

        let waiting = store.resolve_resource_route(&receipt).unwrap();
        assert!(matches!(
            waiting.submission,
            SubmissionState::Resource {
                phase: ResourceRoutePhase::Waiting,
                ..
            }
        ));
        store
            .accept_inbound_event(&TaskEvent {
                task: route.task,
                seq: std::num::NonZeroU64::new(1).unwrap(),
                origin_machine: route.origin_machine,
                execution_machine: route.execution_machine,
                payload: EventPayload::State {
                    status: ProcessStatus::Queued,
                },
            })
            .unwrap();
        let activated = store.origin_route_by_task(route.task).unwrap().unwrap();
        let late_receipt = store.resolve_resource_route(&receipt).unwrap();
        assert_eq!(late_receipt.submission, activated.submission);

        let mut late_rejection = receipt.clone();
        late_rejection.outcome = ResourceQueueOutcome::Rejected {
            reason: "stale rejection".into(),
        };
        assert!(matches!(
            store.resolve_resource_route(&late_rejection),
            Err(IdentityError::Conflict)
        ));
        let late_cancellation = ResourceCancellationReceipt {
            cancellation: uuid::Uuid::now_v7(),
            requester_machine: route.origin_machine,
            request: route.request,
            task: route.task,
            origin_machine: route.origin_machine,
            authority_machine: route.execution_machine,
            resource,
            target_phase: ResourceRoutePhase::AcceptanceUnknown,
            outcome: ResourceCancellationOutcome::CancelledBeforeLaunch,
        };
        assert!(matches!(
            store.cancel_resource_route_before_launch(&late_cancellation),
            Err(IdentityError::Conflict)
        ));
        let saved = store.origin_route_by_task(route.task).unwrap().unwrap();
        assert_eq!(saved.submission, activated.submission);
    }

    #[test]
    fn definitive_cancellation_ignores_late_queue_receipts_without_regressing() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = resource_route();
        store.insert_origin_route(&route).unwrap();
        let SubmissionState::Resource { resource, .. } = route.submission else {
            unreachable!();
        };
        let identity = ResourceCancellationReceipt {
            cancellation: uuid::Uuid::now_v7(),
            requester_machine: route.origin_machine,
            request: route.request,
            task: route.task,
            origin_machine: route.origin_machine,
            authority_machine: route.execution_machine,
            resource,
            target_phase: ResourceRoutePhase::AcceptanceUnknown,
            outcome: ResourceCancellationOutcome::CancelledBeforeLaunch,
        };
        let waiting = ResourceQueueReceipt {
            request: route.request,
            task: route.task,
            origin_machine: route.origin_machine,
            authority_machine: route.execution_machine,
            resource,
            outcome: ResourceQueueOutcome::Waiting,
        };
        store.resolve_resource_route(&waiting).unwrap();

        let cancelled = store
            .cancel_resource_route_before_launch(&identity)
            .unwrap();
        assert!(matches!(
            cancelled.submission,
            SubmissionState::Resource {
                phase: ResourceRoutePhase::CancelledBeforeLaunch,
                ..
            }
        ));

        let late_waiting = store.resolve_resource_route(&waiting).unwrap();
        assert_eq!(late_waiting.submission, cancelled.submission);
        let late_cancelled = ResourceQueueReceipt {
            outcome: ResourceQueueOutcome::Rejected {
                reason: "cancelled_before_launch".into(),
            },
            ..waiting.clone()
        };
        let late_cancelled = store.resolve_resource_route(&late_cancelled).unwrap();
        assert_eq!(late_cancelled.submission, cancelled.submission);

        let stale_rejection = ResourceQueueReceipt {
            outcome: ResourceQueueOutcome::Rejected {
                reason: "unrelated rejection".into(),
            },
            ..waiting.clone()
        };
        assert!(matches!(
            store.resolve_resource_route(&stale_rejection),
            Err(IdentityError::Conflict)
        ));

        let mismatched = ResourceQueueReceipt {
            request: RequestId::new(),
            ..waiting
        };
        assert!(matches!(
            store.resolve_resource_route(&mismatched),
            Err(IdentityError::Conflict)
        ));
        assert!(matches!(
            store.accept_inbound_event(&TaskEvent {
                task: route.task,
                seq: std::num::NonZeroU64::new(1).unwrap(),
                origin_machine: route.origin_machine,
                execution_machine: route.execution_machine,
                payload: EventPayload::State {
                    status: ProcessStatus::Queued,
                },
            }),
            Err(crate::events::EventError::Invalid { .. })
        ));
        let saved = store.origin_route_by_task(route.task).unwrap().unwrap();
        assert_eq!(saved.last_accepted_seq, 0);
        assert_eq!(saved.submission, cancelled.submission);
    }

    #[test]
    fn resource_rejection_is_idempotent_and_cannot_become_waiting() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = resource_route();
        store.insert_origin_route(&route).unwrap();
        let SubmissionState::Resource { resource, .. } = &route.submission else {
            unreachable!();
        };
        let rejected = ResourceQueueReceipt {
            request: route.request,
            task: route.task,
            origin_machine: route.origin_machine,
            authority_machine: route.execution_machine,
            resource: *resource,
            outcome: ResourceQueueOutcome::Rejected {
                reason: "resource unavailable".into(),
            },
        };
        let saved = store.resolve_resource_route(&rejected).unwrap();
        assert!(matches!(
            &saved.submission,
            SubmissionState::Resource {
                phase: ResourceRoutePhase::Rejected { .. },
                ..
            }
        ));
        assert_eq!(
            store.resolve_resource_route(&rejected).unwrap().submission,
            saved.submission
        );

        let mut stale_waiting = rejected;
        stale_waiting.outcome = ResourceQueueOutcome::Waiting;
        assert!(matches!(
            store.resolve_resource_route(&stale_waiting),
            Err(IdentityError::Conflict)
        ));
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
    fn direct_acceptance_respects_resource_ownership_and_reuses_cancellation_tombstone() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = Resource::new(
            ResourceId::new(),
            "gpu-0".into(),
            authority,
            SupervisorAddress {
                machine: authority,
                thread: spec().thread,
            },
            AssignmentRevision::new(0),
            ResourceRevision::new(0),
            None,
        );
        store.register_resource(authority, &resource).unwrap();

        let queued_task = TaskId::new();
        store
            .accept_resource_request(
                authority,
                RequestId::new(),
                queued_task,
                resource.id,
                origin,
                spec(),
            )
            .unwrap();
        let queued_execution = ExecutionRecord {
            task: queued_task,
            origin_machine: origin,
            execution_machine: authority,
            spec: spec().into(),
            state: ProcessStatus::Queued,
        };
        assert!(matches!(
            store.accept_execution(&queued_execution),
            Err(IdentityError::Conflict)
        ));
        assert!(store.executor_identity(queued_task).unwrap().is_none());

        let prevented_without_tombstone = TaskId::new();
        store
            .conn
            .execute(
                "INSERT INTO resource_request_preventions (
                    request_id, task_id, resource_id, origin_machine
                ) VALUES (?1, ?2, ?3, ?4)",
                params![
                    RequestId::new().0.to_string(),
                    prevented_without_tombstone.to_string(),
                    resource.id.as_uuid().to_string(),
                    origin.as_uuid().to_string(),
                ],
            )
            .unwrap();
        let prevented_execution = ExecutionRecord {
            task: prevented_without_tombstone,
            origin_machine: origin,
            execution_machine: authority,
            spec: spec().into(),
            state: ProcessStatus::Queued,
        };
        assert!(matches!(
            store.accept_execution(&prevented_execution),
            Err(IdentityError::Conflict)
        ));
        assert!(
            store
                .executor_identity(prevented_without_tombstone)
                .unwrap()
                .is_none()
        );

        let prevented_task = TaskId::new();
        let request = RequestId::new();
        assert!(matches!(
            store.cancel_resource_request_before_activation(
                authority,
                request,
                prevented_task,
                resource.id,
                origin,
            ),
            Ok(crate::resource::store::QueueCancellationResult::PreventedBeforeAcceptance)
        ));

        let cancelled_execution = ExecutionRecord {
            task: prevented_task,
            origin_machine: origin,
            execution_machine: authority,
            spec: spec().into(),
            state: ProcessStatus::Queued,
        };
        let delayed = store.accept_execution(&cancelled_execution).unwrap();
        let ExecutorIdentity::Rejected(delayed_tombstone) = delayed else {
            panic!("resource cancellation must retain a rejection");
        };
        assert_eq!(
            delayed_tombstone.reason,
            PreAcceptanceRejection::Cancelled.as_str()
        );

        let retried = store.accept_execution(&cancelled_execution).unwrap();
        let ExecutorIdentity::Rejected(retried_tombstone) = retried else {
            panic!("a delayed retry must reuse the rejection");
        };
        assert_eq!(retried_tombstone.task, delayed_tombstone.task);
        assert_eq!(
            retried_tombstone.origin_machine,
            delayed_tombstone.origin_machine
        );
        assert_eq!(
            retried_tombstone.execution_machine,
            delayed_tombstone.execution_machine
        );
        assert_eq!(retried_tombstone.reason, delayed_tombstone.reason);
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

    fn action_route(launch: ResourceActionLaunch) -> OriginRoute {
        let spec = spec();
        let binding = crate::submission::ResourceActionRouteBinding {
            kind: launch.kind(),
            authority: crate::resource::SupervisorActionAuthority {
                authority_machine: MachineId::new(),
                resource_id: ResourceId::new(),
                loan_id: crate::resource::LoanId::new(),
                action_id: ActionId::new(),
                expected_state_revision: ResourceRevision::new(4),
                supervisor: SupervisorAddress {
                    machine: MachineId::new(),
                    thread: spec.thread,
                },
                assignment_revision: AssignmentRevision::new(2),
            },
        };
        OriginRoute::new_resource_action(crate::submission::NewResourceActionRoute {
            request: RequestId::new(),
            task: TaskId::new(),
            callback: CallbackContext {
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                cwd: Path::new("/tmp").to_path_buf(),
                codex: Path::new("/bin/echo").to_path_buf().into(),
            },
            spec,
            binding,
            launch,
        })
        .unwrap()
    }

    fn watcher_route() -> OriginRoute {
        action_route(ResourceActionLaunch::ReleaseWatcher {
            observed_background_task: TaskId::new(),
        })
    }

    fn action_receipt(route: &OriginRoute) -> ActionTaskReceipt {
        let SubmissionState::ResourceAction { binding, .. } = &route.submission else {
            panic!("fixture must be an action route");
        };
        ActionTaskReceipt {
            kind: binding.kind,
            authority: binding.authority,
            request_id: route.request,
            task_id: route.task,
            normalized_spec_sha256: crate::submission::normalized_spec_sha256(
                route.current_spec().unwrap(),
            )
            .unwrap(),
        }
    }

    #[test]
    fn action_route_retry_needs_the_same_identity_content_and_one_route_per_action() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = watcher_route();
        let SubmissionState::ResourceAction { binding, .. } = &route.submission else {
            unreachable!();
        };
        let action = binding.authority.action_id;
        assert_eq!(store.insert_origin_route(&route).unwrap().task, route.task);
        // an exact retry returns the saved route
        assert_eq!(store.insert_origin_route(&route).unwrap().task, route.task);

        let mut changed = route.clone();
        changed.callback.cwd = Path::new("/var").to_path_buf();
        assert!(matches!(
            store.insert_origin_route(&changed),
            Err(IdentityError::Conflict)
        ));
        let mut other_task = route.clone();
        other_task.task = TaskId::new();
        assert!(matches!(
            store.insert_origin_route(&other_task),
            Err(IdentityError::Conflict)
        ));
        // a second identity for the same action cannot be saved
        let mut second = route.clone();
        second.request = RequestId::new();
        second.task = TaskId::new();
        assert!(matches!(
            store.insert_origin_route(&second),
            Err(IdentityError::Conflict)
        ));
        assert_eq!(
            store
                .resource_action_route_by_action(action)
                .unwrap()
                .unwrap()
                .task,
            route.task
        );

        // recovery retries it by identity; the generic abandon scan never sees it
        assert_eq!(store.unknown_resource_action_routes().unwrap().len(), 1);
        assert!(store.unknown_origin_routes().unwrap().is_empty());
        assert!(store.unknown_resource_origin_routes().unwrap().is_empty());

        // the saved route survives reopen with the same launch choice
        drop(store);
        let store = Store::open(&dir.path().join("db")).unwrap();
        let saved = store.origin_route_by_task(route.task).unwrap().unwrap();
        assert!(matches!(
            saved.submission,
            SubmissionState::ResourceAction {
                phase: ResourceActionRoutePhase::AcceptanceUnknown,
                ..
            }
        ));
        assert_eq!(saved.callback, route.callback);
    }

    #[test]
    fn action_route_resolution_requires_the_exact_receipt_and_never_moves_back() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("db")).unwrap();
        let route = watcher_route();
        store.insert_origin_route(&route).unwrap();
        let receipt = action_receipt(&route);

        let mut other_supervisor = receipt;
        other_supervisor.authority.assignment_revision = AssignmentRevision::new(3);
        assert!(matches!(
            store.resolve_resource_action_route(
                route.task,
                &ResourceActionRouteResult::Accepted(other_supervisor)
            ),
            Err(IdentityError::Conflict)
        ));
        let mut other_digest = receipt;
        other_digest.normalized_spec_sha256 = crate::submission::normalized_spec_sha256(&{
            let mut spec = spec();
            spec.timeout = std::time::Duration::from_secs(1);
            spec
        })
        .unwrap();
        assert!(matches!(
            store.resolve_resource_action_route(
                route.task,
                &ResourceActionRouteResult::Accepted(other_digest)
            ),
            Err(IdentityError::Conflict)
        ));

        let accepted = store
            .resolve_resource_action_route(
                route.task,
                &ResourceActionRouteResult::Accepted(receipt),
            )
            .unwrap();
        assert!(matches!(
            accepted.submission,
            SubmissionState::ResourceAction {
                phase: ResourceActionRoutePhase::Accepted,
                ..
            }
        ));
        // a duplicate acceptance is idempotent, and a rejection cannot replace it
        store
            .resolve_resource_action_route(
                route.task,
                &ResourceActionRouteResult::Accepted(receipt),
            )
            .unwrap();
        assert!(matches!(
            store.resolve_resource_action_route(
                route.task,
                &ResourceActionRouteResult::Rejected(
                    crate::resource::bound_action::ResourceActionRejection::ActionNotPending
                )
            ),
            Err(IdentityError::Conflict)
        ));
        assert!(store.unknown_resource_action_routes().unwrap().is_empty());
    }
}
