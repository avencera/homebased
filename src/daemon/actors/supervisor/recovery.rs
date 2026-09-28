//! Startup recovery of the non-terminal tasks that this machine owns

use std::collections::HashMap;

use ractor::ActorRef;

use super::{SupervisorMsg, SupervisorState, launch_accepted, spawn_task_actor};
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{ProcessStatus, TaskId, TaskRow};
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
    let owned = ResourceOwnedTasks::load(state).await?;
    let rows = call(&state.store, |reply| StoreMsg::NonTerminal { reply }).await?;
    for row in rows {
        // a queued return or first background task may have lost its spawn, and a
        // free runner lock cannot prove otherwise, so it stays queued for resource attention
        if row.status() == ProcessStatus::Queued
            && (owned.restoring.contains(&row.id) || owned.background.contains(&row.id))
        {
            continue;
        }
        let identity =
            if row.status() == ProcessStatus::Queued && !owned.accepted.contains_key(&row.id) {
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
            owned.accepted.get(&row.id),
            owned.watchers.contains(&row.id),
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

/// Tasks whose launch belongs to a resource flow on this authority
///
/// Startup recovery and a local submit retry leave these to their resource,
/// since a free runner lock cannot prove that the first spawn never happened
pub(super) struct ResourceOwnedTasks {
    accepted: HashMap<TaskId, AcceptedResourceTask>,
    restoring: Vec<TaskId>,
    background: Vec<TaskId>,
    watchers: Vec<TaskId>,
}

impl ResourceOwnedTasks {
    pub(super) async fn load(state: &SupervisorState) -> Result<Self, AppError> {
        let authority_machine = state.machine;
        let accepted = call(&state.store, |reply| {
            StoreMsg::AcceptedResourceTasksForAuthority {
                authority_machine,
                reply,
            }
        })
        .await?
        .into_iter()
        .map(|task| (task.request.task_id, task))
        .collect();
        let restoring = call(&state.store, |reply| StoreMsg::RestoringTasksForAuthority {
            authority_machine,
            reply,
        })
        .await?;
        let background = call(&state.store, |reply| {
            StoreMsg::QueuedBackgroundLaunchTasksForAuthority {
                authority_machine,
                reply,
            }
        })
        .await?;
        let watchers = call(&state.store, |reply| {
            StoreMsg::ReleaseWatcherTasksForAuthority {
                authority_machine,
                reply,
            }
        })
        .await?;
        Ok(Self {
            accepted,
            restoring,
            background,
            watchers,
        })
    }

    /// Whether a resource flow owns this task's launch
    pub(super) fn owns(&self, id: TaskId) -> bool {
        self.accepted.contains_key(&id)
            || self.restoring.contains(&id)
            || self.background.contains(&id)
            || self.watchers.contains(&id)
    }
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
