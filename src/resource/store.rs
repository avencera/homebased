//! SQLite operations for authority-local resource state

mod acceptance;
mod assigned_task;
mod cancellation;
mod checkpoint;
mod codec;
mod error;
mod notice;
mod provenance;
mod queue;
mod release_completion;
mod release_loan;
mod release_watcher;
mod resources;
mod return_window;
mod revision;
mod rows;
mod schema;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use acceptance::{
    AcceptedResourceTask, ResourceTaskAcceptance, ResourceTaskAcceptanceInput,
    assigned_resource_request_for_acceptance,
};
pub(crate) use assigned_task::{
    AssignedResourceTaskAttention, AssignedResourceTaskProgress,
    AssignedResourceTaskReconcileInput, AssignedResourceTaskReconcileOutcome,
    ResourceTaskCompletionResult, reconcile_assigned_resource_task_for_authority,
};
pub(crate) use cancellation::{QueueCancellationResult, cancel_request_before_activation_on};
pub(crate) use checkpoint::{
    ReleaseCheckpointCancellationOutcome, ReleaseCheckpointCancellationResult,
    ReleaseCheckpointError, release_checkpoint_state_for_action, update_release_checkpoint_state,
};
pub(crate) use error::{ConflictReason, ResourceStoreError, TrainerAttemptAssociationStoreError};
pub(crate) use notice::{
    SupervisorNoticeStoreError, decode_supervisor_notice_record,
    insert_supervisor_notice_in_transaction, pending_supervisor_notices,
    recover_sending_supervisor_notices, reserve_supervisor_notice_attempt,
    retarget_supervisor_notice_in_transaction, select_supervisor_notice_record,
    select_supervisor_notice_record_by_action, settle_supervisor_notice_attempt, supervisor_notice,
    update_supervisor_notice_cas,
};
pub(crate) use queue::{
    accept_request_for_authority, assigned_resource_requests_for_authority,
    next_queued_request_for_authority, requests_for_resource_for_authority,
    rewrite_queued_request_ranks,
};
pub(crate) use release_completion::{
    CompleteReleaseError, ReleaseCompletionResult, complete_release_for_authority,
    release_completion_for_loan, release_completion_for_retry,
};
pub(crate) use release_loan::{
    OpenReleaseLoanError, ResourceQueueReconcileError, reconcile_resource_queue_for_authority,
};
pub(crate) use return_window::{
    ReturnDeadlineOutcome, open_missing_return_windows_on, return_window_on,
    save_held_return_window_on, serve_after_return_deadline_for_authority,
};
// production opens release loans only inside queue reconciliation
#[cfg(test)]
pub(crate) use release_loan::OpenReleaseLoanResult;
pub(crate) use release_watcher::{
    ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError, ReleaseWatcherAcceptanceInput,
    bind_release_watcher_for_authority, bind_release_watcher_intent_on,
    validate_release_watcher_for_local_acceptance, validate_release_watcher_intent_on,
};
pub(crate) use resources::{
    ResourceSnapshot, register_resource_for_authority, resources_for_authority,
};
pub(crate) use revision::swap_resource_revision;
pub(crate) use rows::{select_non_closed_loan, select_request_by_id, select_resource};
pub(crate) use schema::RESOURCE_SCHEMA;
