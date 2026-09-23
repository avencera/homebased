//! Requester delivery and executor receipt storage

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::{Store, resource_task_id_is_reserved};
use crate::cancellation::{
    CancellationDelivery, CancellationReceipt, CancellationRequest, CancellationRequestIdentity,
    ExecutorCancelState, ResourceCancellationRequestIdentity,
};
use crate::domain::TaskId;
use crate::error::AppError;
use crate::submission::{
    ExecutorIdentity, PreAcceptanceRejection, RejectionTombstone, ResourceCancellationReceipt,
};

fn encode<T: serde::Serialize>(value: &T) -> Result<String, AppError> {
    Ok(serde_json::to_string(value)?)
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, AppError> {
    Ok(serde_json::from_str(value)?)
}

fn conflict(task: TaskId) -> AppError {
    AppError::ClusterTaskConflict { task }
}

impl Store {
    /// Save one requester-owned record before any network send
    pub fn insert_cancellation_request(
        &mut self,
        request: CancellationRequest,
    ) -> Result<(CancellationRequest, bool), AppError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT request_json FROM cancellation_requests WHERE task_id=?1",
                [request.task.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            let saved: CancellationRequest = decode(&existing)?;
            if saved.requester_machine != request.requester_machine
                || saved.cancellation != request.cancellation
                || saved.execution_machine != request.execution_machine
                || saved.origin_machine != request.origin_machine
                || saved.target != request.target
            {
                return Err(conflict(request.task));
            }
            return Ok((saved, false));
        }
        tx.execute(
            "INSERT INTO cancellation_requests (task_id,request_json) VALUES (?1,?2)",
            params![request.task.to_string(), encode(&request)?],
        )?;
        tx.commit()?;
        Ok((request, true))
    }

    /// Read the retained caller-owned cancellation for one task
    pub fn cancellation_request(
        &self,
        task: TaskId,
    ) -> Result<Option<CancellationRequest>, AppError> {
        let request: Option<String> = self
            .conn
            .query_row(
                "SELECT request_json FROM cancellation_requests WHERE task_id=?1",
                [task.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        request.map(|request| decode(&request)).transpose()
    }

    /// List pending requests for requester restart recovery
    pub fn pending_cancellation_requests(&self) -> Result<Vec<CancellationRequest>, AppError> {
        let mut stmt = self
            .conn
            .prepare("SELECT request_json FROM cancellation_requests")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut pending = Vec::new();
        for row in rows {
            let request: CancellationRequest = decode(&row?)?;
            if matches!(request.delivery, CancellationDelivery::Pending) {
                pending.push(request);
            }
        }
        Ok(pending)
    }

    /// Complete requester delivery only with a matching executor receipt
    pub fn acknowledge_cancellation(
        &mut self,
        receipt: &CancellationReceipt,
    ) -> Result<CancellationRequest, AppError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let data: String = tx.query_row(
            "SELECT request_json FROM cancellation_requests WHERE task_id=?1",
            [receipt.request.task.to_string()],
            |row| row.get(0),
        )?;
        let mut request: CancellationRequest = decode(&data)?;
        if request.identity() != receipt.request {
            return Err(conflict(request.task));
        }
        match &request.delivery {
            CancellationDelivery::Pending => {
                request.delivery = CancellationDelivery::Delivered {
                    result: receipt.state.clone(),
                };
                tx.execute(
                    "UPDATE cancellation_requests SET request_json=?1 WHERE task_id=?2",
                    params![encode(&request)?, request.task.to_string()],
                )?;
                tx.commit()?;
            }
            CancellationDelivery::Delivered { result } if result == &receipt.state => {}
            CancellationDelivery::Delivered { .. }
            | CancellationDelivery::ResourceDelivered { .. } => {
                return Err(conflict(request.task));
            }
        }
        Ok(request)
    }

    /// Settle one resource intent only with its exact authority receipt.
    pub fn acknowledge_resource_cancellation(
        &mut self,
        receipt: &ResourceCancellationReceipt,
    ) -> Result<CancellationRequest, AppError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let data: String = tx.query_row(
            "SELECT request_json FROM cancellation_requests WHERE task_id=?1",
            [receipt.task.to_string()],
            |row| row.get(0),
        )?;
        let mut request: CancellationRequest = decode(&data)?;
        let Some(identity) = request.resource_identity() else {
            return Err(conflict(receipt.task));
        };
        if !resource_receipt_matches_identity(receipt, &identity)
            || request.identity()
                != (CancellationRequestIdentity {
                    requester_machine: receipt.requester_machine,
                    cancellation: receipt.cancellation,
                    task: receipt.task,
                    origin_machine: receipt.origin_machine,
                    execution_machine: receipt.authority_machine,
                })
        {
            return Err(conflict(receipt.task));
        }
        match &request.delivery {
            CancellationDelivery::Pending => {
                request.delivery = CancellationDelivery::ResourceDelivered {
                    result: receipt.clone(),
                };
                tx.execute(
                    "UPDATE cancellation_requests SET request_json=?1 WHERE task_id=?2",
                    params![encode(&request)?, request.task.to_string()],
                )?;
                tx.commit()?;
            }
            CancellationDelivery::ResourceDelivered { result } if result == receipt => {}
            CancellationDelivery::Delivered { .. }
            | CancellationDelivery::ResourceDelivered { .. } => {
                return Err(conflict(receipt.task));
            }
        }
        Ok(request)
    }

    /// Atomically retain executor receipt and pre-acceptance tombstone
    pub fn receive_cancellation(
        &mut self,
        request: CancellationRequestIdentity,
    ) -> Result<CancellationReceipt, AppError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if resource_task_id_is_reserved(&tx, request.task)? {
            return Err(AppError::ResourceCancellationUnavailable { task: request.task });
        }

        let previous: Option<String> = tx
            .query_row(
                "SELECT receipt_json FROM executor_cancellations WHERE cancellation_id=?1",
                [request.cancellation.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(previous) = previous {
            let receipt: CancellationReceipt = decode(&previous)?;
            if receipt.request != request {
                return Err(conflict(request.task));
            }
            return Ok(receipt);
        }
        let identity: Option<String> = tx
            .query_row(
                "SELECT identity_json FROM executor_identities WHERE task_id=?1",
                [request.task.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let state = if let Some(identity) = identity {
            match decode::<ExecutorIdentity>(&identity)? {
                ExecutorIdentity::Accepted(record) => {
                    if record.origin_machine != request.origin_machine
                        || record.execution_machine != request.execution_machine
                    {
                        ExecutorCancelState::Rejected {
                            reason: "cluster_task_conflict".into(),
                        }
                    } else if record.state.is_terminal() {
                        ExecutorCancelState::AlreadyTerminal {
                            status: record.state,
                        }
                    } else {
                        ExecutorCancelState::PendingApplication
                    }
                }
                ExecutorIdentity::Rejected(record) => {
                    if record.origin_machine != request.origin_machine
                        || record.execution_machine != request.execution_machine
                    {
                        ExecutorCancelState::Rejected {
                            reason: "cluster_task_conflict".into(),
                        }
                    } else if record.reason == PreAcceptanceRejection::Cancelled.as_str() {
                        ExecutorCancelState::PreventedBeforeStart {
                            reason: record.reason,
                        }
                    } else {
                        ExecutorCancelState::Rejected {
                            reason: record.reason,
                        }
                    }
                }
            }
        } else {
            let task_exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
                [request.task.to_string()],
                |row| row.get(0),
            )?;
            if task_exists {
                return Err(conflict(request.task));
            }
            let reason = PreAcceptanceRejection::Cancelled.as_str().to_string();
            let tombstone = ExecutorIdentity::Rejected(RejectionTombstone {
                task: request.task,
                origin_machine: request.origin_machine,
                execution_machine: request.execution_machine,
                reason: reason.clone(),
            });
            tx.execute(
                "INSERT INTO executor_identities (task_id,origin_machine,identity_json) VALUES (?1,?2,?3)",
                params![request.task.to_string(), request.origin_machine.to_string(), encode(&tombstone)?],
            )?;
            ExecutorCancelState::PreventedBeforeStart { reason }
        };
        let receipt = CancellationReceipt { request, state };
        tx.execute(
            "INSERT INTO executor_cancellations (cancellation_id,task_id,receipt_json) VALUES (?1,?2,?3)",
            params![request.cancellation.to_string(), request.task.to_string(), encode(&receipt)?],
        )?;
        tx.commit()?;
        Ok(receipt)
    }

    /// Find accepted cancellation receipts that still need process termination
    pub fn pending_executor_cancellations(&self) -> Result<Vec<CancellationReceipt>, AppError> {
        let mut stmt = self
            .conn
            .prepare("SELECT receipt_json FROM executor_cancellations")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut pending = Vec::new();
        for row in rows {
            let receipt: CancellationReceipt = decode(&row?)?;
            if matches!(receipt.state, ExecutorCancelState::PendingApplication) {
                pending.push(receipt);
            }
        }
        Ok(pending)
    }

    /// Set the retained executor result after the normal task cancellation path
    pub fn finish_executor_cancellation(
        &mut self,
        cancellation: uuid::Uuid,
        state: ExecutorCancelState,
    ) -> Result<CancellationReceipt, AppError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let data: String = tx.query_row(
            "SELECT receipt_json FROM executor_cancellations WHERE cancellation_id=?1",
            [cancellation.to_string()],
            |row| row.get(0),
        )?;
        let mut receipt: CancellationReceipt = decode(&data)?;
        if matches!(receipt.state, ExecutorCancelState::PendingApplication) {
            receipt.state = state;
            tx.execute(
                "UPDATE executor_cancellations SET receipt_json=?1 WHERE cancellation_id=?2",
                params![encode(&receipt)?, cancellation.to_string()],
            )?;
            tx.commit()?;
        }
        Ok(receipt)
    }
}

fn resource_receipt_matches_identity(
    receipt: &ResourceCancellationReceipt,
    identity: &ResourceCancellationRequestIdentity,
) -> bool {
    receipt.cancellation == identity.cancellation
        && receipt.requester_machine == identity.requester_machine
        && receipt.request == identity.request
        && receipt.task == identity.task
        && receipt.origin_machine == identity.origin_machine
        && receipt.authority_machine == identity.authority_machine
        && receipt.resource == identity.resource
        && receipt.target_phase == identity.target_phase
}

#[cfg(test)]
mod tests {
    use rusqlite::params;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::domain::ThreadId;
    use crate::machine::MachineId;
    use crate::resource::{
        AssignmentRevision, Resource, ResourceId, ResourceRevision, SupervisorAddress,
    };
    use crate::spec::NormalizedSpec;
    use crate::submission::{
        RequestId, ResourceCancellationIneligibleReason, ResourceCancellationOutcome,
        ResourceCancellationReceipt, ResourceRoutePhase,
    };

    fn resource(authority: MachineId) -> Resource {
        Resource::new(
            ResourceId::new(),
            "gpu-0".into(),
            authority,
            SupervisorAddress {
                machine: authority,
                thread: ThreadId(Uuid::now_v7()),
            },
            AssignmentRevision::new(0),
            ResourceRevision::new(0),
            None,
        )
    }

    fn spec() -> NormalizedSpec {
        serde_json::from_value(serde_json::json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "resource command",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["/bin/echo", "hello"] }
        }))
        .unwrap()
    }

    fn cancellation(
        task: TaskId,
        origin: MachineId,
        authority: MachineId,
    ) -> CancellationRequestIdentity {
        CancellationRequestIdentity {
            requester_machine: origin,
            cancellation: Uuid::now_v7(),
            task,
            origin_machine: origin,
            execution_machine: authority,
        }
    }

    fn resource_cancellation_request(
        origin: MachineId,
        authority: MachineId,
        resource: ResourceId,
        request: RequestId,
        task: TaskId,
        cancellation: Uuid,
    ) -> CancellationRequest {
        CancellationRequest {
            requester_machine: origin,
            cancellation,
            task,
            origin_machine: origin,
            execution_machine: authority,
            target: crate::cancellation::CancellationTarget::Resource(
                crate::cancellation::ResourceCancellationTarget {
                    request_id: request,
                    task_id: task,
                    resource_id: resource,
                    origin_machine: origin,
                    authority_machine: authority,
                    phase: ResourceRoutePhase::Waiting,
                },
            ),
            delivery: CancellationDelivery::Pending,
        }
    }

    #[test]
    fn resource_cancellation_intent_and_receipt_settle_by_full_identity() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let origin = MachineId::new();
        let authority = MachineId::new();
        let resource = ResourceId::new();
        let request_id = RequestId::new();
        let task = TaskId::new();
        let cancellation = Uuid::now_v7();
        let request = resource_cancellation_request(
            origin,
            authority,
            resource,
            request_id,
            task,
            cancellation,
        );
        assert_eq!(
            store.insert_cancellation_request(request.clone()).unwrap(),
            (request.clone(), true)
        );
        assert_eq!(
            store.insert_cancellation_request(request.clone()).unwrap(),
            (request.clone(), false)
        );

        let mut changed = request.clone();
        changed.cancellation = Uuid::now_v7();
        assert!(matches!(
            store.insert_cancellation_request(changed),
            Err(AppError::ClusterTaskConflict { task: found }) if found == task
        ));

        let receipt = ResourceCancellationReceipt {
            cancellation,
            requester_machine: origin,
            request: request_id,
            task,
            origin_machine: origin,
            authority_machine: authority,
            resource,
            target_phase: ResourceRoutePhase::Waiting,
            outcome: ResourceCancellationOutcome::CancelledBeforeLaunch,
        };
        let settled = store.acknowledge_resource_cancellation(&receipt).unwrap();
        assert_eq!(
            settled.delivery,
            CancellationDelivery::ResourceDelivered {
                result: receipt.clone(),
            }
        );
        assert_eq!(store.pending_cancellation_requests().unwrap().len(), 0);
        assert_eq!(
            store.acknowledge_resource_cancellation(&receipt).unwrap(),
            settled
        );

        let mut conflicting_receipt = receipt;
        conflicting_receipt.outcome = ResourceCancellationOutcome::NotEligible {
            reason: ResourceCancellationIneligibleReason::Terminal,
        };
        assert!(matches!(
            store.acknowledge_resource_cancellation(&conflicting_receipt),
            Err(AppError::ClusterTaskConflict { task: found }) if found == task
        ));
    }

    #[test]
    fn generic_cancellation_does_not_tombstone_a_queued_resource_request() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let resource = resource(authority);
        store.register_resource(authority, &resource).unwrap();
        let task = TaskId::new();
        store
            .accept_resource_request(
                authority,
                RequestId::new(),
                task,
                resource.id,
                origin,
                spec(),
            )
            .unwrap();

        assert!(matches!(
            store.receive_cancellation(cancellation(task, origin, authority)),
            Err(AppError::ResourceCancellationUnavailable { task: found }) if found == task
        ));
        assert!(store.executor_identity(task).unwrap().is_none());
        assert_eq!(
            store
                .resource_requests(authority, resource.id)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn generic_cancellation_does_not_tombstone_a_prevented_resource_request() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("db")).unwrap();
        let authority = MachineId::new();
        let origin = MachineId::new();
        let task = TaskId::new();
        store
            .conn
            .execute(
                "INSERT INTO resource_request_preventions (
                    request_id, task_id, resource_id, origin_machine
                ) VALUES (?1, ?2, ?3, ?4)",
                params![
                    RequestId::new().0.to_string(),
                    task.to_string(),
                    ResourceId::new().as_uuid().to_string(),
                    origin.as_uuid().to_string(),
                ],
            )
            .unwrap();

        assert!(matches!(
            store.receive_cancellation(cancellation(task, origin, authority)),
            Err(AppError::ResourceCancellationUnavailable { task: found }) if found == task
        ));
        assert!(store.executor_identity(task).unwrap().is_none());
    }
}
