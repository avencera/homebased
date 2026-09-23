//! Requester delivery and executor receipt storage

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::Store;
use crate::cancellation::{
    CancellationDelivery, CancellationReceipt, CancellationRequest, CancellationRequestIdentity,
    ExecutorCancelState,
};
use crate::domain::TaskId;
use crate::error::AppError;
use crate::submission::{ExecutorIdentity, PreAcceptanceRejection, RejectionTombstone};

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
                || saved.execution_machine != request.execution_machine
                || saved.origin_machine != request.origin_machine
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
        if matches!(request.delivery, CancellationDelivery::Pending) {
            request.delivery = CancellationDelivery::Delivered {
                result: receipt.state.clone(),
            };
            tx.execute(
                "UPDATE cancellation_requests SET request_json=?1 WHERE task_id=?2",
                params![encode(&request)?, request.task.to_string()],
            )?;
            tx.commit()?;
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
