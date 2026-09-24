//! Startup recovery of the non-terminal tasks that this machine owns

use std::collections::HashMap;

use ractor::ActorRef;

use super::{SupervisorMsg, SupervisorState, launch_accepted, spawn_task_actor};
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{ProcessStatus, TaskRow};
use crate::error::AppError;
use crate::resource::store::AcceptedResourceTask;
use crate::submission::ExecutorIdentity;

/// Give every non-terminal task an owner after a daemon restart
///
/// Only a queued remote acceptance is launched, and `launch_accepted` still
/// refuses one whose runner lock is held; every other row is observed or left
/// for its resource actor
pub(super) async fn recover_tasks(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
) -> Result<(), AppError> {
    let accepted_resource_tasks = call(&state.store, |reply| {
        StoreMsg::AcceptedResourceTasksForAuthority {
            authority_machine: state.machine,
            reply,
        }
    })
    .await?;
    let accepted_resource_tasks: HashMap<_, _> = accepted_resource_tasks
        .into_iter()
        .map(|task| (task.request.task_id, task))
        .collect();
    let restoring_tasks = call(&state.store, |reply| StoreMsg::RestoringTasksForAuthority {
        authority_machine: state.machine,
        reply,
    })
    .await?;
    let background_launches = call(&state.store, |reply| {
        StoreMsg::QueuedBackgroundLaunchTasksForAuthority {
            authority_machine: state.machine,
            reply,
        }
    })
    .await?;
    let watcher_tasks = call(&state.store, |reply| {
        StoreMsg::ReleaseWatcherTasksForAuthority {
            authority_machine: state.machine,
            reply,
        }
    })
    .await?;
    let rows = call(&state.store, |reply| StoreMsg::NonTerminal { reply }).await?;
    for row in rows {
        // a queued return or first background task may have lost its spawn, and a
        // free runner lock cannot prove otherwise, so it stays queued for resource attention
        if row.status() == ProcessStatus::Queued
            && (restoring_tasks.contains(&row.id) || background_launches.contains(&row.id))
        {
            continue;
        }
        let identity = if row.status() == ProcessStatus::Queued
            && !accepted_resource_tasks.contains_key(&row.id)
        {
            call(&state.store, |reply| StoreMsg::ExecutorIdentity {
                id: row.id,
                reply,
            })
            .await?
        } else {
            None
        };
        match startup_recovery_action(
            &row,
            accepted_resource_tasks.get(&row.id),
            watcher_tasks.contains(&row.id),
            identity.as_ref(),
        ) {
            StartupRecoveryAction::LaunchAccepted => {
                launch_accepted(supervisor, state, row.id).await?;
            }
            StartupRecoveryAction::Observe => {
                spawn_task_actor(supervisor, state, row.id).await?;
            }
            StartupRecoveryAction::DeferResource => {}
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartupRecoveryAction {
    LaunchAccepted,
    Observe,
    DeferResource,
}

pub(super) fn startup_recovery_action(
    row: &TaskRow,
    resource_task: Option<&AcceptedResourceTask>,
    release_watcher: bool,
    identity: Option<&ExecutorIdentity>,
) -> StartupRecoveryAction {
    if let Some(resource_task) = resource_task {
        return match (row.status(), resource_task.state) {
            (ProcessStatus::Queued, ProcessStatus::Queued) => StartupRecoveryAction::DeferResource,
            _ => StartupRecoveryAction::Observe,
        };
    }
    // a bound watcher, even one accepted for a remote supervisor, is never
    // relaunched from a queued row whose first spawn may already have happened
    if release_watcher {
        return StartupRecoveryAction::Observe;
    }

    if row.status() == ProcessStatus::Queued
        && matches!(
            identity,
            Some(ExecutorIdentity::Accepted(record))
                if record.origin_machine != record.execution_machine
        )
    {
        return StartupRecoveryAction::LaunchAccepted;
    }

    StartupRecoveryAction::Observe
}
