//! Startup recovery of the non-terminal tasks that this machine owns

use ractor::ActorRef;

use super::{SupervisorMsg, SupervisorState, launch_accepted, spawn_task_actor};
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{ProcessStatus, TaskRow};
use crate::error::AppError;
use crate::submission::ExecutorIdentity;

/// Give every non-terminal task an owner after a daemon restart
///
/// Only a queued remote acceptance or released local task is launched, and
/// `launch_accepted` still refuses one whose runner lock is held; every other
/// row is observed
pub(super) async fn recover_tasks(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
) -> Result<(), AppError> {
    let released = call(&state.store, |reply| StoreMsg::UnstartedDependentTasks {
        reply,
    })
    .await?;
    let rows = call(&state.store, |reply| StoreMsg::NonTerminal { reply }).await?;
    for row in rows {
        let identity = if row.status() == ProcessStatus::Queued {
            call(&state.store, |reply| StoreMsg::ExecutorIdentity {
                id: row.id,
                reply,
            })
            .await?
        } else {
            None
        };
        match startup_recovery_action(&row, identity.as_ref(), released.contains(&row.id)) {
            StartupRecoveryAction::LaunchAccepted => {
                launch_accepted(supervisor, state, row.id).await?;
            }
            StartupRecoveryAction::Observe => {
                spawn_task_actor(supervisor, state, row.id).await?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartupRecoveryAction {
    LaunchAccepted,
    Observe,
}

/// Decide how startup recovery owns one non-terminal row
///
/// `released` marks a local task submitted with dependencies. Its origin, not
/// a caller, retries its launch, so a queued row is launched rather than
/// observed into `lost`
pub(super) fn startup_recovery_action(
    row: &TaskRow,
    identity: Option<&ExecutorIdentity>,
    released: bool,
) -> StartupRecoveryAction {
    if row.status() == ProcessStatus::Queued
        && (released
            || matches!(
                identity,
                Some(ExecutorIdentity::Accepted(record))
                    if record.origin_machine != record.execution_machine
            ))
    {
        return StartupRecoveryAction::LaunchAccepted;
    }

    StartupRecoveryAction::Observe
}
