//! Release-watcher acceptance, running checks, and poll handling on the authority

use rusqlite::{Connection, TransactionBehavior};

use super::action_task::{action_task_receipt_by_task, remote_release_watcher_is_running_on};
use super::release_checkpoint::{
    missing_release_checkpoint_state_error, release_checkpoint_binding_on,
    validate_release_checkpoint_baseline_on,
};
use super::{
    resource_request_identity_exists, resource_task_row_matches, same_task_binding,
    select_authority_resource,
};
use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::events::EventError;
use crate::machine::MachineId;
use crate::resource::bound_action::ActionTaskReceipt;
use crate::resource::release_watcher::{
    ReleaseWatcherCommand, ReleaseWatcherPollAttention, ReleaseWatcherPollOutcome,
    ReleaseWatcherPollRequest,
};
use crate::resource::store::{
    CompleteReleaseError, ReleaseCheckpointCancellationOutcome, ReleaseCheckpointError,
    ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError, ReleaseWatcherAcceptanceInput,
    ResourceStoreError, TrainerAttemptAssociationStoreError, bind_release_watcher_intent_on,
    release_checkpoint_state_for_action, release_completion_for_retry,
    validate_release_watcher_for_local_acceptance,
};
use crate::resource::trainer_publication::WatcherAttention;
use crate::resource::{
    ReleaseCheckpointAction, ReleaseCheckpointStopOutcome, ReleaseWatcherIntent, ResourceId,
};
use crate::spec::{NormalizedSpec, NormalizedWorkload};
use crate::store::Store;
use crate::store::identity::{
    executor_identity_on, origin_route_by_request_on, origin_route_by_task_on,
};
use crate::submission::{
    CallbackContext, ExecutorIdentity, OriginRoute, RequestId, SubmissionState,
    normalized_spec_sha256,
};

/// Saved acceptance that owns one bound watcher task
///
/// The supervisor assignment can change while an accepted watcher runs, so the
/// running check follows the records written at acceptance, not the current assignment
enum SavedWatcherAcceptance {
    /// No record accepts this watcher identity yet
    NotAccepted,
    /// The authority accepted the watcher with local origin routes
    Local(Box<LocalWatcherRoutes>),
    /// The authority accepted the watcher for a remote supervisor under this receipt
    Remote(ActionTaskReceipt),
}

/// Origin routes saved by one local watcher acceptance
struct LocalWatcherRoutes {
    by_request: OriginRoute,
    by_task: OriginRoute,
}

impl Store {
    /// Accept one fixed release-watcher task and all durable ownership records
    pub(crate) fn accept_release_watcher_for_authority(
        &mut self,
        input: ReleaseWatcherAcceptanceInput,
    ) -> Result<ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError> {
        let ReleaseWatcherAcceptanceInput {
            authority_machine,
            resource_id,
            supervisor,
            intent,
            row,
            spec,
            callback,
        } = input;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task_id = intent.watcher_task_id.as_task_id();
        let digest = normalized_spec_sha256(&spec).map_err(AppError::from)?;
        if intent.normalized_spec_sha256 != digest
            || task_id != row.id
            || row.thread != supervisor.thread
            || spec.thread != supervisor.thread
            || !row.binary.is_absolute()
            || row.workload != crate::invocation::persist_workload(&spec.workload)
            || callback.env != row.env
            || callback.cwd != row.cwd
            || !matches!(&spec.workload, NormalizedWorkload::Task(_))
        {
            return Err(ReleaseWatcherAcceptanceError::Conflict);
        }
        // the authority accepts only its own command for these exact release identities
        let canonical_digest = ReleaseWatcherCommand::from_intent(resource_id, &intent)
            .normalized_spec_sha256(&row.binary, supervisor.thread)
            .map_err(|_| ReleaseWatcherAcceptanceError::Conflict)?;
        if canonical_digest != digest {
            return Err(ReleaseWatcherAcceptanceError::Conflict);
        }
        crate::store::validate_local_task_acceptance(&row, &spec, &callback)
            .map_err(|_| ReleaseWatcherAcceptanceError::Conflict)?;

        let saved_supervisor = validate_release_watcher_for_local_acceptance(
            &tx,
            authority_machine,
            resource_id,
            supervisor,
            &intent,
        )?;
        if saved_supervisor.machine != authority_machine {
            return Ok(ReleaseWatcherAcceptance::UnsupportedRemoteSupervisor {
                authority_machine,
                supervisor: saved_supervisor,
            });
        }

        validate_release_checkpoint_baseline_on(
            &tx,
            authority_machine,
            resource_id,
            intent.action_id,
        )?;
        bind_release_watcher_intent_on(&tx, authority_machine, resource_id, intent.clone())?;

        if resource_request_identity_exists(&tx, intent.request_id, task_id)? {
            return Err(ReleaseWatcherAcceptanceError::Conflict);
        }
        let saved_task = crate::store::task_by_id_on(&tx, task_id)?;
        let route_by_request = origin_route_by_request_on(&tx, intent.request_id)?;
        let route_by_task = origin_route_by_task_on(&tx, task_id)?;
        let identity = executor_identity_on(&tx, task_id)?;
        let has_event = super::task_has_any_event(&tx, task_id)?;

        if saved_task.is_none()
            && route_by_request.is_none()
            && route_by_task.is_none()
            && identity.is_none()
            && !has_event
        {
            crate::store::insert_local_task_records_on(
                &tx,
                &row,
                &spec,
                authority_machine,
                intent.request_id,
                &callback,
                Some(task_id),
            )?;
            tx.commit()?;
            return Ok(ReleaseWatcherAcceptance::Inserted { task: task_id });
        }

        let (Some(saved_task), Some(route_by_request), Some(route_by_task), Some(identity)) =
            (saved_task, route_by_request, route_by_task, identity)
        else {
            return Err(ReleaseWatcherAcceptanceError::Conflict);
        };
        let initial_event_matches = crate::store::events::initial_queued_event_matches_on(
            &tx,
            task_id,
            authority_machine,
            authority_machine,
        )
        .map_err(ResourceStoreError::from)?;
        if !same_task_binding(&saved_task, &row)
            || !watcher_routes_match(
                &route_by_request,
                &route_by_task,
                (intent.request_id, task_id),
                authority_machine,
                supervisor.thread,
                &callback,
                &spec,
            )
            || !watcher_identity_matches(
                &identity,
                task_id,
                authority_machine,
                &spec,
                saved_task.status(),
            )
            || !initial_event_matches
        {
            return Err(ReleaseWatcherAcceptanceError::Conflict);
        }

        let state = saved_task.state.clone();
        tx.commit()?;
        Ok(ReleaseWatcherAcceptance::Existing {
            task: task_id,
            state,
        })
    }

    /// Serve one co-located release-watcher poll on this store connection
    ///
    /// The authority, action, revision, trainer, saved watcher intent, baseline, and
    /// accepted running watcher identity are validated before any publication is read
    /// A stop decision is reserved once and commits with the exact trainer cancel
    /// marker; retries reuse that saved decision and never select another checkpoint
    /// Only storage failures are errors; every domain refusal is typed attention
    pub(crate) fn poll_release_watcher_for_authority(
        &mut self,
        authority_machine: MachineId,
        request: ReleaseWatcherPollRequest,
    ) -> Result<ReleaseWatcherPollOutcome, ReleaseCheckpointError> {
        match self.poll_release_watcher(authority_machine, request.watcher) {
            Ok(outcome) => Ok(outcome),
            Err(error) => release_watcher_poll_attention(error)
                .map(|reason| ReleaseWatcherPollOutcome::Attention { reason }),
        }
    }

    fn poll_release_watcher(
        &mut self,
        authority_machine: MachineId,
        watcher: ReleaseWatcherCommand,
    ) -> Result<ReleaseWatcherPollOutcome, ReleaseCheckpointError> {
        let resource_id = watcher.resource_id;
        let action_id = watcher.action_id;
        let revision = watcher.state_revision;
        let attention = |reason| Ok(ReleaseWatcherPollOutcome::Attention { reason });

        match release_completion_for_retry(
            &self.conn,
            authority_machine,
            resource_id,
            action_id,
            revision,
        ) {
            Ok(Some(_)) => return Ok(ReleaseWatcherPollOutcome::ReleaseSettled),
            Ok(None) => {}
            Err(CompleteReleaseError::Storage(error)) => return Err(error.into()),
            Err(_) => return attention(ReleaseWatcherPollAttention::ActionNotCurrent),
        }

        match select_authority_resource(&self.conn, authority_machine, resource_id) {
            Ok(_) => {}
            Err(ResourceStoreError::ResourceNotFound) => {
                return attention(ReleaseWatcherPollAttention::ActionNotCurrent);
            }
            Err(error) => return Err(error.into()),
        }

        let Some((state, _)) =
            release_checkpoint_state_for_action(&self.conn, resource_id, action_id)?
        else {
            return Err(missing_release_checkpoint_state_error(
                &self.conn,
                resource_id,
                action_id,
            )?);
        };
        let expected_action = ReleaseCheckpointAction {
            resource_id,
            action_id,
            state_revision: revision,
            observed_background_task: watcher.trainer_task_id,
        };
        if state.action != expected_action {
            return attention(ReleaseWatcherPollAttention::ActionNotCurrent);
        }
        let binding = release_checkpoint_binding_on(
            &self.conn,
            authority_machine,
            resource_id,
            &state.action,
        )?;
        if binding.watcher_intent.watcher_task_id != watcher.watcher_task_id {
            return attention(ReleaseWatcherPollAttention::WrongWatcher);
        }
        validate_release_checkpoint_baseline_on(
            &self.conn,
            authority_machine,
            resource_id,
            action_id,
        )?;
        if !release_watcher_is_running_on(
            &self.conn,
            authority_machine,
            resource_id,
            &binding.watcher_intent,
        )? {
            return Ok(ReleaseWatcherPollOutcome::WatcherNotRunning);
        }

        let decision = match self.reserve_release_checkpoint_stop_for_authority(
            authority_machine,
            resource_id,
            action_id,
            revision,
        )? {
            ReleaseCheckpointStopOutcome::Reserved(decision)
            | ReleaseCheckpointStopOutcome::AlreadyReserved(decision) => decision,
            ReleaseCheckpointStopOutcome::WaitingForTaskStart => {
                return Ok(ReleaseWatcherPollOutcome::WaitingForTrainerStart);
            }
            ReleaseCheckpointStopOutcome::WaitingForCheckpoint => {
                return Ok(ReleaseWatcherPollOutcome::WaitingForCheckpoint);
            }
            ReleaseCheckpointStopOutcome::CompletedResultAwaitingTaskExit => {
                return Ok(ReleaseWatcherPollOutcome::CompletedResultAwaitingTrainerExit);
            }
            ReleaseCheckpointStopOutcome::AlreadyCompleted => {
                return Ok(ReleaseWatcherPollOutcome::TrainerCompleted);
            }
            ReleaseCheckpointStopOutcome::Attention(watcher_attention) => {
                return attention(watcher_poll_attention(&watcher_attention));
            }
        };

        match self.commit_release_checkpoint_cancellation_for_authority(
            authority_machine,
            resource_id,
            action_id,
            revision,
            &decision,
        )? {
            ReleaseCheckpointCancellationOutcome::Committed(result)
            | ReleaseCheckpointCancellationOutcome::AlreadyCommitted(result) => {
                Ok(ReleaseWatcherPollOutcome::StopCommitted {
                    generation_id: result.decision.selected_checkpoint.generation_id,
                    cancel_requested_at: result.cancellation.cancel_requested_at,
                })
            }
            ReleaseCheckpointCancellationOutcome::WatcherNotReady { .. } => {
                Ok(ReleaseWatcherPollOutcome::WatcherNotRunning)
            }
        }
    }
}

/// Decide whether the accepted watcher bound to this intent is running
pub(super) fn release_watcher_is_running_on(
    conn: &Connection,
    authority_machine: MachineId,
    resource_id: ResourceId,
    intent: &ReleaseWatcherIntent,
) -> Result<bool, ReleaseCheckpointError> {
    let task_id = intent.watcher_task_id.as_task_id();
    let conflict = || ReleaseCheckpointError::WatcherIdentityConflict { task_id };
    select_authority_resource(conn, authority_machine, resource_id)?;
    let routes = match saved_watcher_acceptance(conn, intent)? {
        SavedWatcherAcceptance::NotAccepted => return Ok(false),
        // a remote supervisor's watcher has no authority-local route; its receipt,
        // remote identity, and first event prove the same acceptance instead
        SavedWatcherAcceptance::Remote(receipt) => {
            return remote_release_watcher_is_running_on(
                conn,
                authority_machine,
                resource_id,
                intent,
                &receipt,
            );
        }
        SavedWatcherAcceptance::Local(routes) => routes,
    };
    let task = crate::store::task_by_id_on(conn, task_id)?;
    let identity = executor_identity_on(conn, task_id).map_err(ResourceStoreError::from)?;
    let (Some(task), Some(identity)) = (task, identity) else {
        return Ok(false);
    };
    let ExecutorIdentity::Accepted(record) = &identity else {
        return Ok(false);
    };
    let spec = record.current_spec().ok_or_else(conflict)?;
    if !record.is_owned_by(task_id, authority_machine, authority_machine)
        || spec.machine.is_some()
        || normalized_spec_sha256(spec)? != intent.normalized_spec_sha256
        || !matches!(&spec.workload, NormalizedWorkload::Task(_))
        || !resource_task_row_matches(&task, task_id, spec)
    {
        return Err(conflict());
    }

    if task.status() != ProcessStatus::Running
        || task.pid().is_none()
        || record.state != task.status()
    {
        return Ok(false);
    }
    // the intent digest binds the spec thread, so the saved routes are checked
    // against it rather than the current, possibly replaced, supervisor
    if !watcher_routes_match(
        &routes.by_request,
        &routes.by_task,
        (intent.request_id, task_id),
        authority_machine,
        spec.thread,
        &routes.by_request.callback,
        spec,
    ) || !watcher_identity_matches(&identity, task_id, authority_machine, spec, task.status())
    {
        return Err(conflict());
    }

    initial_watcher_event_matches(conn, task_id, authority_machine)
}

fn saved_watcher_acceptance(
    conn: &Connection,
    intent: &ReleaseWatcherIntent,
) -> Result<SavedWatcherAcceptance, ReleaseCheckpointError> {
    let task_id = intent.watcher_task_id.as_task_id();
    let receipt = action_task_receipt_by_task(conn, task_id)?;
    let by_request =
        origin_route_by_request_on(conn, intent.request_id).map_err(ResourceStoreError::from)?;
    let by_task = origin_route_by_task_on(conn, task_id).map_err(ResourceStoreError::from)?;
    match (receipt, by_request, by_task) {
        (Some(receipt), None, None) => Ok(SavedWatcherAcceptance::Remote(receipt)),
        (None, Some(by_request), Some(by_task)) => Ok(SavedWatcherAcceptance::Local(Box::new(
            LocalWatcherRoutes {
                by_request,
                by_task,
            },
        ))),
        // a task row without either acceptance record was accepted for another owner
        (None, None, None) if crate::store::task_by_id_on(conn, task_id)?.is_none() => {
            Ok(SavedWatcherAcceptance::NotAccepted)
        }
        _ => Err(ReleaseCheckpointError::WatcherIdentityConflict { task_id }),
    }
}

/// Check that the watcher's first saved event is the accepted queued state
///
/// Storage failures stay retryable; a different first event means the watcher
/// identity no longer matches its acceptance
fn initial_watcher_event_matches(
    conn: &Connection,
    task: TaskId,
    authority: MachineId,
) -> Result<bool, ReleaseCheckpointError> {
    crate::store::events::initial_queued_event_matches_on(conn, task, authority, authority).map_err(
        |error| match error {
            EventError::Storage(error) => ReleaseCheckpointError::Task(error),
            _ => ReleaseCheckpointError::WatcherIdentityConflict { task_id: task },
        },
    )
}

/// Classify a checkpoint failure as watcher attention, keeping storage failures retryable
fn release_watcher_poll_attention(
    error: ReleaseCheckpointError,
) -> Result<ReleaseWatcherPollAttention, ReleaseCheckpointError> {
    let reason = match error {
        ReleaseCheckpointError::Storage(_)
        | ReleaseCheckpointError::Serialization(_)
        | ReleaseCheckpointError::Task(_)
        | ReleaseCheckpointError::Resource(ResourceStoreError::Storage(_))
        | ReleaseCheckpointError::TrainerAssociation(
            TrainerAttemptAssociationStoreError::Storage(_),
        ) => return Err(error),
        ReleaseCheckpointError::Resource(ResourceStoreError::CorruptRecord { .. }) => {
            ReleaseWatcherPollAttention::CorruptRecord
        }
        ReleaseCheckpointError::Resource(ResourceStoreError::WrongAuthority { .. }) => {
            ReleaseWatcherPollAttention::WrongAuthority
        }
        ReleaseCheckpointError::WatcherIntentMissing { .. } => {
            ReleaseWatcherPollAttention::WatcherIntentMissing
        }
        ReleaseCheckpointError::WatcherIdentityConflict { .. } => {
            ReleaseWatcherPollAttention::WatcherIdentityConflict
        }
        ReleaseCheckpointError::TrainerAssociation(_)
        | ReleaseCheckpointError::TrainerAssociationMissing { .. }
        | ReleaseCheckpointError::TaskMissing { .. }
        | ReleaseCheckpointError::TrainerTaskNotRunning { .. }
        | ReleaseCheckpointError::TrainerCommandBindingChanged { .. }
        | ReleaseCheckpointError::TrainerCancellationConflict { .. } => {
            ReleaseWatcherPollAttention::TrainerChanged
        }
        ReleaseCheckpointError::Watcher(_)
        | ReleaseCheckpointError::SelectedCheckpointChanged { .. } => {
            ReleaseWatcherPollAttention::PublicationChanged
        }
        ReleaseCheckpointError::Resource(_)
        | ReleaseCheckpointError::NotAwaitingRelease { .. }
        | ReleaseCheckpointError::BaselineMissing { .. }
        | ReleaseCheckpointError::StopDecisionMissing { .. }
        | ReleaseCheckpointError::StopDecisionMismatch { .. }
        | ReleaseCheckpointError::Conflict
        | ReleaseCheckpointError::InvalidStoredEvidence(_) => {
            ReleaseWatcherPollAttention::ActionNotCurrent
        }
    };

    Ok(reason)
}

fn watcher_poll_attention(attention: &WatcherAttention) -> ReleaseWatcherPollAttention {
    match attention {
        WatcherAttention::LostTask { .. } => ReleaseWatcherPollAttention::TrainerLost,
        WatcherAttention::FailedTask { .. } => ReleaseWatcherPollAttention::TrainerFailed,
        WatcherAttention::PublicationBeforeTaskStart { .. } => {
            ReleaseWatcherPollAttention::PublicationBeforeTrainerStart
        }
        WatcherAttention::SuccessfulTaskWithoutFinalResult { .. } => {
            ReleaseWatcherPollAttention::TrainerCompletedWithoutResult
        }
    }
}

fn watcher_routes_match(
    by_request: &OriginRoute,
    by_task: &OriginRoute,
    route_identity: (RequestId, TaskId),
    authority: MachineId,
    thread: crate::domain::ThreadId,
    callback: &CallbackContext,
    spec: &NormalizedSpec,
) -> bool {
    let (request, task) = route_identity;
    let matches = |route: &OriginRoute| {
        route.request == request
            && route.task == task
            && route.origin_machine == authority
            && route.execution_machine == authority
            && route.thread == thread
            && route.callback == *callback
            && matches!(&route.submission, SubmissionState::Accepted)
            && route.spec.current() == Some(spec)
    };
    matches(by_request) && matches(by_task)
}

fn watcher_identity_matches(
    identity: &ExecutorIdentity,
    task: TaskId,
    authority: MachineId,
    spec: &NormalizedSpec,
    state: ProcessStatus,
) -> bool {
    let ExecutorIdentity::Accepted(record) = identity else {
        return false;
    };
    record.is_owned_by(task, authority, authority)
        && record.state == state
        && record.current_spec() == Some(spec)
}
