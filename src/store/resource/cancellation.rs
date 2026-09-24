//! Durable resource cancellation receipts on the authority

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior};

use super::{decode_resource_json, encode_resource_json};
use crate::cancellation::ResourceCancellationRequestIdentity;
use crate::machine::MachineId;
use crate::resource::ResourceRequestState;
use crate::resource::store::{
    ConflictReason, QueueCancellationResult, ResourceStoreError,
    cancel_request_before_activation_on, requests_for_resource_for_authority,
};
use crate::store::Store;
use crate::submission::{
    ResourceCancellationIneligibleReason, ResourceCancellationOutcome, ResourceCancellationReceipt,
    ResourceRoutePhase, ResourceRouteProof, normalized_spec_sha256,
};

impl Store {
    /// Return the exact saved authority receipt for one cancellation identity
    pub(crate) fn resource_cancellation_receipt(
        &self,
        identity: &ResourceCancellationRequestIdentity,
    ) -> Result<Option<ResourceCancellationReceipt>, ResourceStoreError> {
        saved_cancellation_receipt_on(&self.conn, identity)
    }

    /// Apply one resource cancellation and retain its exact result in one transaction
    pub(crate) fn cancel_resource_request_with_receipt(
        &mut self,
        authority_machine: MachineId,
        identity: ResourceCancellationRequestIdentity,
        proof: ResourceRouteProof,
    ) -> Result<ResourceCancellationReceipt, ResourceStoreError> {
        if !proof_matches_identity(authority_machine, &identity, &proof) {
            return Err(receipt_mismatch());
        }

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(receipt) = saved_cancellation_receipt_on(&tx, &identity)? {
            tx.commit()?;
            return Ok(receipt);
        }
        check_retained_request(&tx, authority_machine, &identity, &proof)?;

        let outcome = match proof.phase {
            ResourceRoutePhase::Activated => {
                not_eligible(ResourceCancellationIneligibleReason::Activated)
            }
            ResourceRoutePhase::Rejected { reason } => {
                not_eligible(ResourceCancellationIneligibleReason::Rejected { reason })
            }
            ResourceRoutePhase::AcceptanceUnknown
            | ResourceRoutePhase::Waiting
            | ResourceRoutePhase::CancelledBeforeLaunch => {
                queue_cancellation_outcome(cancel_in_savepoint(&tx, authority_machine, &identity)?)?
            }
        };

        let receipt = ResourceCancellationReceipt {
            cancellation: identity.cancellation,
            requester_machine: identity.requester_machine,
            request: identity.request,
            task: identity.task,
            origin_machine: identity.origin_machine,
            authority_machine: identity.authority_machine,
            resource: identity.resource,
            target_phase: identity.target_phase.clone(),
            outcome,
        };
        tx.execute(
            "INSERT INTO resource_cancellation_receipts
             (cancellation_id, request_json, receipt_json) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                identity.cancellation.to_string(),
                encode_resource_json(&identity)?,
                encode_resource_json(&receipt)?,
            ],
        )?;
        tx.commit()?;
        Ok(receipt)
    }
}

fn receipt_mismatch() -> ResourceStoreError {
    ResourceStoreError::Conflict(ConflictReason::CancellationReceiptMismatch)
}

fn not_eligible(reason: ResourceCancellationIneligibleReason) -> ResourceCancellationOutcome {
    ResourceCancellationOutcome::NotEligible { reason }
}

/// Require that the origin's route proof names this cancellation and a forward phase
fn proof_matches_identity(
    authority_machine: MachineId,
    identity: &ResourceCancellationRequestIdentity,
    proof: &ResourceRouteProof,
) -> bool {
    identity.requester_machine == identity.origin_machine
        && identity.authority_machine == authority_machine
        && proof.request == identity.request
        && proof.task == identity.task
        && proof.resource == identity.resource
        && proof.origin_machine == identity.origin_machine
        && proof.authority_machine == authority_machine
        && phase_is_forward(&identity.target_phase, &proof.phase)
}

fn phase_is_forward(target: &ResourceRoutePhase, current: &ResourceRoutePhase) -> bool {
    match target {
        ResourceRoutePhase::AcceptanceUnknown => true,
        ResourceRoutePhase::Waiting => !matches!(current, ResourceRoutePhase::AcceptanceUnknown),
        ResourceRoutePhase::CancelledBeforeLaunch => {
            matches!(current, ResourceRoutePhase::CancelledBeforeLaunch)
        }
        ResourceRoutePhase::Activated | ResourceRoutePhase::Rejected { .. } => target == current,
    }
}

/// Require that any retained request is exactly the one the proof names
///
/// A waiting or activated route proves that the authority retained its request
fn check_retained_request(
    conn: &Connection,
    authority_machine: MachineId,
    identity: &ResourceCancellationRequestIdentity,
    proof: &ResourceRouteProof,
) -> Result<(), ResourceStoreError> {
    let requests =
        match requests_for_resource_for_authority(conn, authority_machine, identity.resource) {
            Ok(requests) => requests,
            Err(ResourceStoreError::ResourceNotFound)
                if matches!(&proof.phase, ResourceRoutePhase::Rejected { .. }) =>
            {
                Vec::new()
            }
            Err(error) => return Err(error),
        };
    let retained = requests
        .iter()
        .find(|request| request.request_id == identity.request || request.task_id == identity.task);
    let Some(retained) = retained else {
        return match &proof.phase {
            ResourceRoutePhase::Waiting | ResourceRoutePhase::Activated => {
                Err(ResourceStoreError::Conflict(ConflictReason::RequestMissing))
            }
            _ => Ok(()),
        };
    };
    let saved_digest = normalized_spec_sha256(retained.spec().as_normalized())
        .map_err(|error| ResourceStoreError::corrupt("resource request spec", error))?;
    if retained.request_id != identity.request
        || retained.task_id != identity.task
        || retained.resource_id != identity.resource
        || retained.origin_machine != identity.origin_machine
        || saved_digest != proof.normalized_spec_sha256
    {
        return Err(ResourceStoreError::Conflict(
            ConflictReason::RequestIdentityMismatch,
        ));
    }

    Ok(())
}

/// Cancel the request inside a savepoint of the receipt transaction
///
/// A refusal after partial writes, such as an executor acceptance that won the
/// race after a prevention row was written, rolls back only the cancellation;
/// the receipt of that refusal still commits with the outer transaction
fn cancel_in_savepoint(
    tx: &Transaction<'_>,
    authority_machine: MachineId,
    identity: &ResourceCancellationRequestIdentity,
) -> Result<Result<QueueCancellationResult, ResourceStoreError>, ResourceStoreError> {
    tx.execute_batch("SAVEPOINT resource_cancellation")?;
    let result = cancel_request_before_activation_on(
        tx,
        authority_machine,
        identity.request,
        identity.task,
        identity.resource,
        identity.origin_machine,
    );
    if result.is_err() {
        tx.execute_batch("ROLLBACK TO resource_cancellation")?;
    }
    tx.execute_batch("RELEASE resource_cancellation")?;
    Ok(result)
}

/// Map the queue cancellation result to the outcome its receipt records
fn queue_cancellation_outcome(
    result: Result<QueueCancellationResult, ResourceStoreError>,
) -> Result<ResourceCancellationOutcome, ResourceStoreError> {
    let request = match result {
        Ok(QueueCancellationResult::PreventedBeforeAcceptance) => {
            return Ok(ResourceCancellationOutcome::PreventedBeforeAcceptance);
        }
        Ok(QueueCancellationResult::Request(request)) => request,
        Err(ResourceStoreError::ExecutorAlreadyAccepted { .. }) => {
            return Ok(not_eligible(
                ResourceCancellationIneligibleReason::Activated,
            ));
        }
        Err(error) => return Err(error),
    };
    match request.state {
        ResourceRequestState::CancelledBeforeLaunch => {
            Ok(ResourceCancellationOutcome::CancelledBeforeLaunch)
        }
        ResourceRequestState::Finished { .. } => {
            Ok(not_eligible(ResourceCancellationIneligibleReason::Terminal))
        }
        ResourceRequestState::Rejected { reason } => Ok(not_eligible(
            ResourceCancellationIneligibleReason::Rejected { reason },
        )),
        ResourceRequestState::Queued | ResourceRequestState::Assigned { .. } => Err(
            ResourceStoreError::Conflict(ConflictReason::RequestStateChanged),
        ),
    }
}

/// Read the saved receipt of one cancellation and require its exact identity
fn saved_cancellation_receipt_on(
    conn: &Connection,
    identity: &ResourceCancellationRequestIdentity,
) -> Result<Option<ResourceCancellationReceipt>, ResourceStoreError> {
    let saved: Option<(String, String)> = conn
        .query_row(
            "SELECT request_json, receipt_json FROM resource_cancellation_receipts
             WHERE cancellation_id=?1",
            [identity.cancellation.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((request_json, receipt_json)) = saved else {
        return Ok(None);
    };
    let saved_identity: ResourceCancellationRequestIdentity =
        decode_resource_json("resource cancellation request", &request_json)?;
    let receipt: ResourceCancellationReceipt =
        decode_resource_json("resource cancellation receipt", &receipt_json)?;
    if saved_identity != *identity || !receipt_matches(&receipt, identity) {
        return Err(receipt_mismatch());
    }

    Ok(Some(receipt))
}

fn receipt_matches(
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
