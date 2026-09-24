//! Durable associations between a registered background task and its exact trainer attempt

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{resource_task_row_matches, same_task_binding, select_authority_resource};
use crate::domain::{ProcessStatus, TaskId, TaskRow};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::command_shape::DirectSegmentCommandShape;
use crate::resource::ownership_lock::VerifiedTrainerAttempt;
use crate::resource::store::{TrainerAttemptAssociationStoreError, select_non_closed_loan};
use crate::resource::{
    LoanPhase, LoanState, ResourceId, TrainerAttemptAssociation, TrainerAttemptAssociationProof,
};
use crate::spec::NormalizedSpec;
use crate::store::Store;
use crate::submission::{ExecutorIdentity, NormalizedSpecSha256, normalized_spec_sha256};

/// Registered trainer task and accepted identity read for one bind or stop
#[derive(Debug)]
pub(super) struct TrainerAssociationBindSnapshot {
    pub(super) task_row: TaskRow,
    pub(super) normalized_spec: NormalizedSpec,
    pub(super) normalized_spec_sha256: NormalizedSpecSha256,
    pub(super) identity_state: ProcessStatus,
}

impl TrainerAssociationBindSnapshot {
    /// Whether the task row still runs the command of the accepted spec
    pub(super) fn task_row_matches_spec(&self, task_id: TaskId) -> bool {
        resource_task_row_matches(&self.task_row, task_id, &self.normalized_spec)
    }
}

/// One saved association with the exact JSON it was decoded from
pub(super) struct SavedTrainerAssociation {
    pub(super) association: TrainerAttemptAssociation,
    pub(super) json: String,
}

impl Store {
    /// Bind exact trainer evidence and direct-segment command shape to a registered task
    ///
    /// The accepted identity and running task row are read before file-system validation
    /// A later IMMEDIATE transaction rechecks their immutable binding before insertion
    /// File paths may change between the shape check and that transaction
    /// Exact retries return the saved association without checking the current file-system layout
    /// This shape does not prove live lock use or permit release completion or `Serving`
    pub(crate) fn bind_trainer_attempt_association(
        &mut self,
        authority_machine: MachineId,
        resource_id: ResourceId,
        task_id: TaskId,
        verified_attempt: VerifiedTrainerAttempt,
    ) -> Result<TrainerAttemptAssociation, TrainerAttemptAssociationStoreError> {
        let (preflight, association) = {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Deferred)?;
            let snapshot =
                trainer_association_bind_snapshot(&tx, authority_machine, resource_id, task_id)?;
            let association = TrainerAttemptAssociation::from_components(
                resource_id,
                authority_machine,
                task_id,
                verified_attempt,
                snapshot.normalized_spec_sha256,
            )
            .map_err(|reason| {
                TrainerAttemptAssociationStoreError::InvalidStoredAssociation(reason.into())
            })?;

            if let Some(saved) = trainer_association_by_task(&tx, task_id)? {
                let binding_unchanged = snapshot.task_row_matches_spec(task_id);
                let saved = saved_association_retry(saved, &association, binding_unchanged)?;
                tx.commit()?;
                return Ok(saved);
            }
            if !snapshot.task_row_matches_spec(task_id) {
                return Err(
                    TrainerAttemptAssociationStoreError::NormalizedSpecMismatch { task_id },
                );
            }
            ensure_bindable(&tx, resource_id, task_id, &snapshot)?;

            tx.commit()?;
            (snapshot, association)
        };

        // keep canonical path checks outside the IMMEDIATE write transaction
        DirectSegmentCommandShape::validate_binding(
            &preflight.normalized_spec,
            &preflight.task_row,
            task_id,
            preflight.normalized_spec_sha256,
            association.verified_attempt(),
        )?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current =
            trainer_association_bind_snapshot(&tx, authority_machine, resource_id, task_id)?;
        let binding_unchanged = current.normalized_spec_sha256 == preflight.normalized_spec_sha256
            && same_task_binding(&current.task_row, &preflight.task_row)
            && current.task_row_matches_spec(task_id);
        if let Some(saved) = trainer_association_by_task(&tx, task_id)? {
            let saved = saved_association_retry(saved, &association, binding_unchanged)?;
            tx.commit()?;
            return Ok(saved);
        }
        if !binding_unchanged {
            return Err(TrainerAttemptAssociationStoreError::BindingChanged { task_id });
        }
        ensure_bindable(&tx, resource_id, task_id, &current)?;

        tx.execute(
            "INSERT INTO trainer_attempt_associations (
                resource_id, authority_machine, task_id, association_json
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                resource_id.as_uuid().to_string(),
                authority_machine.as_uuid().to_string(),
                task_id.to_string(),
                trainer_association_json(&association)?,
            ],
        )?;
        tx.commit()?;
        Ok(association)
    }

    /// Read one historical association by exact task without authorizing release or execution
    pub(crate) fn trainer_attempt_association_for_task_for_authority(
        &self,
        authority_machine: MachineId,
        task_id: TaskId,
    ) -> Result<Option<TrainerAttemptAssociation>, TrainerAttemptAssociationStoreError> {
        let Some(saved) = trainer_association_by_task(&self.conn, task_id)? else {
            return Ok(None);
        };
        select_authority_resource(&self.conn, authority_machine, saved.resource_id())?;
        if saved.authority_machine() != authority_machine {
            return Err(association_authority_mismatch());
        }

        Ok(Some(saved))
    }
}

fn association_authority_mismatch() -> TrainerAttemptAssociationStoreError {
    TrainerAttemptAssociationStoreError::InvalidStoredAssociation(
        "association authority does not match its resource".into(),
    )
}

/// Classify an association already saved for the task being bound
///
/// An association of another resource owns the task. The same resource must hold
/// exactly the expected association over an unchanged task binding, or the retry conflicts
fn saved_association_retry(
    saved: TrainerAttemptAssociation,
    expected: &TrainerAttemptAssociation,
    binding_unchanged: bool,
) -> Result<TrainerAttemptAssociation, TrainerAttemptAssociationStoreError> {
    if saved.resource_id() != expected.resource_id() {
        return Err(TrainerAttemptAssociationStoreError::TaskAlreadyAssociated {
            task_id: expected.task_id(),
        });
    }
    if saved != *expected || !binding_unchanged {
        return Err(TrainerAttemptAssociationStoreError::Conflict {
            resource_id: expected.resource_id(),
        });
    }

    Ok(saved)
}

/// Require a running registered task and a loan that is free or awaits this task's release
fn ensure_bindable(
    conn: &Connection,
    resource_id: ResourceId,
    task_id: TaskId,
    snapshot: &TrainerAssociationBindSnapshot,
) -> Result<(), TrainerAttemptAssociationStoreError> {
    let loan_awaits_other_work = select_non_closed_loan(conn, resource_id)?.is_some_and(|loan| {
        !matches!(
            loan.state,
            LoanState::Active {
                phase: LoanPhase::AwaitingRelease {
                    observed_background_task,
                    ..
                }
            } if observed_background_task == task_id
        )
    });
    if loan_awaits_other_work {
        return Err(TrainerAttemptAssociationStoreError::ActiveLoanConflict { resource_id });
    }
    if snapshot.task_row.status() != ProcessStatus::Running {
        return Err(TrainerAttemptAssociationStoreError::TaskNotRunning {
            task_id,
            state: snapshot.task_row.status().to_string(),
        });
    }
    if snapshot.identity_state != ProcessStatus::Running {
        return Err(TrainerAttemptAssociationStoreError::IdentityNotRunning { task_id });
    }

    Ok(())
}

/// Read the registered task row and its accepted identity on this authority
pub(super) fn trainer_association_bind_snapshot(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    task_id: TaskId,
) -> Result<TrainerAssociationBindSnapshot, TrainerAttemptAssociationStoreError> {
    let resource = select_authority_resource(conn, authority_machine, resource_id)?;
    if resource.registered_background_task != Some(task_id) {
        return Err(TrainerAttemptAssociationStoreError::TaskNotRegistered { task_id });
    }

    let task_row = crate::store::task_by_id_on(conn, task_id)?
        .ok_or(TrainerAttemptAssociationStoreError::TaskMissing { task_id })?;
    let identity = crate::store::identity::executor_identity_on(conn, task_id)?
        .ok_or(TrainerAttemptAssociationStoreError::IdentityMissing { task_id })?;
    let ExecutorIdentity::Accepted(record) = identity else {
        return Err(TrainerAttemptAssociationStoreError::IdentityNotAccepted { task_id });
    };
    // the trainer's callback origin may be any supervisor machine
    if !record.is_executed_by(task_id, authority_machine) {
        return Err(TrainerAttemptAssociationStoreError::IdentityMismatch { task_id });
    }

    let normalized_spec = record
        .current_spec()
        .ok_or(TrainerAttemptAssociationStoreError::NormalizedSpecMissing { task_id })?
        .clone();
    let normalized_spec_sha256 =
        normalized_spec_sha256(&normalized_spec).map_err(AppError::from)?;

    Ok(TrainerAssociationBindSnapshot {
        task_row,
        normalized_spec,
        normalized_spec_sha256,
        identity_state: record.state,
    })
}

/// Read the association of one resource and task with its saved JSON
pub(super) fn saved_trainer_association_on(
    conn: &Connection,
    resource_id: ResourceId,
    task_id: TaskId,
) -> Result<Option<SavedTrainerAssociation>, TrainerAttemptAssociationStoreError> {
    select_association(
        conn,
        "SELECT resource_id, authority_machine, task_id, association_json
         FROM trainer_attempt_associations WHERE resource_id=?1 AND task_id=?2",
        params![resource_id.as_uuid().to_string(), task_id.to_string()],
    )
}

pub(super) fn trainer_association_by_resource_and_task(
    conn: &Connection,
    resource_id: ResourceId,
    task_id: TaskId,
) -> Result<Option<TrainerAttemptAssociation>, TrainerAttemptAssociationStoreError> {
    Ok(saved_trainer_association_on(conn, resource_id, task_id)?.map(|saved| saved.association))
}

pub(super) fn trainer_association_by_task(
    conn: &Connection,
    task_id: TaskId,
) -> Result<Option<TrainerAttemptAssociation>, TrainerAttemptAssociationStoreError> {
    let saved = select_association(
        conn,
        "SELECT resource_id, authority_machine, task_id, association_json
         FROM trainer_attempt_associations WHERE task_id=?1",
        params![task_id.to_string()],
    )?;
    Ok(saved.map(|saved| saved.association))
}

fn select_association(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Option<SavedTrainerAssociation>, TrainerAttemptAssociationStoreError> {
    let row = conn
        .query_row(sql, params, |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .optional()?;
    let Some((resource_id, authority_machine, task_id, json)) = row else {
        return Ok(None);
    };
    let association =
        decode_trainer_association(&resource_id, &authority_machine, &task_id, &json)?;
    Ok(Some(SavedTrainerAssociation { association, json }))
}

pub(super) fn trainer_association_json(
    association: &TrainerAttemptAssociation,
) -> Result<String, TrainerAttemptAssociationStoreError> {
    serde_json::to_string(&TrainerAttemptAssociationProof::from(association))
        .map_err(AppError::from)
        .map_err(Into::into)
}

fn decode_trainer_association(
    resource_id: &str,
    authority_machine: &str,
    task_id: &str,
    association_json: &str,
) -> Result<TrainerAttemptAssociation, TrainerAttemptAssociationStoreError> {
    let invalid =
        |reason: String| TrainerAttemptAssociationStoreError::InvalidStoredAssociation(reason);
    let uuid =
        |value: &str| uuid::Uuid::parse_str(value).map_err(|error| invalid(error.to_string()));
    let resource_id =
        ResourceId::from_uuid(uuid(resource_id)?).map_err(|error| invalid(error.to_string()))?;
    let authority_machine = MachineId::from_uuid(uuid(authority_machine)?);
    let task_id = TaskId(uuid(task_id)?);
    let association: TrainerAttemptAssociationProof =
        serde_json::from_str(association_json).map_err(|error| invalid(error.to_string()))?;
    if association.resource_id != resource_id
        || association.authority_machine != authority_machine
        || association.task_id != task_id
    {
        return Err(invalid(
            "JSON identities do not match the association row".into(),
        ));
    }

    TrainerAttemptAssociation::try_from(association).map_err(|reason| invalid(reason.into()))
}
