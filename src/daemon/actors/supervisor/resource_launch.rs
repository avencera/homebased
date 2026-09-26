//! Resource-owned launches: assigned tasks, release watchers, returns, and background tasks
//!
//! Every launch commits its task records before any spawn, and only the call
//! whose transaction inserted a row starts its worker

use ractor::{ActorRef, RpcReplyPort};

use super::{
    BackgroundLaunch, ReleaseWatcherLaunch, RemoteBackgroundLaunch, ReturnDecisionOutcome,
    SupervisorMsg, SupervisorState, ensure_resource_actor, reconcile_resource_actor,
    resolve_callback_codex, spawn_inserted, spawn_task_actor, wake_resource,
};
use crate::daemon::actors::resource::{
    BackgroundLaunchResult, ReleaseWatcherLaunchResult, ResourceMsg, RestoreLaunchResult,
};
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{ProcessStatus, TaskEnv, TaskId, TaskState};
use crate::error::AppError;
use crate::invocation::persist_workload;
use crate::resource::bound_action::{
    ActionTaskAcceptance, ActionTaskIdentity, ActionTaskReceipt, PreparedActionTask,
    ResourceActionKind, ResourceActionOperation, ResourceActionOutcome, ResourceActionRejection,
    ResourceActionRequest,
};
use crate::resource::release_watcher::ReleaseWatcherCommand;
use crate::resource::store::{
    ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError, ReleaseWatcherAcceptanceInput,
    ResourceStoreError, ResourceTaskAcceptance, ResourceTaskAcceptanceInput,
};
use crate::resource::{
    ReleaseWatcherIntent, ResourceId, ReturnDecision, ReturnLaunch, SupervisorActionAuthority,
};
use crate::store::{
    AcceptedActionTask, BackgroundLaunchAcceptance, BackgroundLaunchError, BackgroundLaunchInput,
    EndedRestoreResolution, NewTask, RemoteBackgroundLaunchInput,
    RemoteReleaseWatcherAcceptanceInput, ResourceActionError, ReturnClosure, ReturnDecisionError,
    ReturnTaskAcceptance, ReturnTaskAcceptanceInput, ReturnTaskOrigin, new_queued_task,
};
use crate::submission::{CallbackContext, RequestId};

/// Resource actor that hears when one launch starts and how it finished
///
/// The reports only speed up the actor's view; it rebuilds that view from
/// durable state, so a failed finish report is not an error
struct LaunchReporter {
    resource: Option<ActorRef<ResourceMsg>>,
}

impl LaunchReporter {
    /// Tell the resource actor, when one runs, that a launch has started
    fn start(
        state: &SupervisorState,
        resource_id: ResourceId,
        started: ResourceMsg,
    ) -> Result<Self, AppError> {
        let resource = state.resources.get(&resource_id).cloned();
        if let Some(resource) = &resource {
            resource.cast(started)?;
        }
        Ok(Self { resource })
    }

    /// Tell the resource actor, when one runs, how the launch finished
    fn finish(&self, finished: ResourceMsg) {
        if let Some(resource) = &self.resource
            && let Err(error) = resource.cast(finished)
        {
            tracing::debug!(resource = ?resource.get_name(), "launch result cast: {error}");
        }
    }
}

pub(super) async fn launch_assigned_resource_task(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    input: ResourceTaskAcceptanceInput,
) -> Result<Result<ResourceTaskAcceptance, ResourceStoreError>, AppError> {
    let task_id = input.task_id;
    let resource_id = input.resource_id;
    let acceptance = call(&state.store, |reply| StoreMsg::AcceptAssignedResourceTask {
        input: Box::new(input),
        reply,
    })
    .await?;
    let acceptance = match acceptance {
        Ok(acceptance) => acceptance,
        Err(error) => return Ok(Err(error)),
    };

    match &acceptance {
        ResourceTaskAcceptance::Inserted { task } if *task == task_id => {
            spawn_inserted(supervisor, state, task_id).await?;
        }
        ResourceTaskAcceptance::Existing {
            task,
            state: status,
        } if *task == task_id => match status {
            ProcessStatus::Queued => {
                reconcile_resource_actor(supervisor, state, resource_id).await?;
            }
            ProcessStatus::Running => spawn_task_actor(supervisor, state, task_id).await?,
            ProcessStatus::Succeeded
            | ProcessStatus::Failed
            | ProcessStatus::Cancelled
            | ProcessStatus::Lost => {}
        },
        _ => {
            return Err(AppError::Internal {
                message: format!(
                    "resource task acceptance returned an unexpected identity for {task_id}"
                ),
            });
        }
    }

    Ok(Ok(acceptance))
}

pub(super) async fn launch_bound_release_watcher(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    launch: ReleaseWatcherLaunch,
) -> Result<Result<ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError>, AppError> {
    let ReleaseWatcherLaunch {
        authority_machine,
        resource_id,
        supervisor: owner,
        intent,
        executable,
    } = launch;
    let task_id = intent.watcher_task_id.as_task_id();
    let spec = ReleaseWatcherCommand::from_intent(resource_id, &intent)
        .normalized_spec(&executable, owner.thread)?;
    let (env, callback) = watcher_launch_context(state, &intent, &spec.cwd).await?;
    let row = new_queued_task(NewTask {
        id: task_id,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: persist_workload(&spec.workload),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env,
        binary: executable,
    });
    let acceptance = call(&state.store, |reply| {
        StoreMsg::AcceptReleaseWatcherForAuthority {
            input: Box::new(ReleaseWatcherAcceptanceInput {
                authority_machine,
                resource_id,
                supervisor: owner,
                intent,
                row,
                spec,
                callback,
            }),
            reply,
        }
    })
    .await?;
    let acceptance = match acceptance {
        Ok(acceptance) => acceptance,
        Err(error) => return Ok(Err(error)),
    };

    match &acceptance {
        // only the transaction that inserted the row may spawn its worker
        ReleaseWatcherAcceptance::Inserted { task } if *task == task_id => {
            spawn_inserted(supervisor, state, task_id).await?;
        }
        // a queued row found again may already have a worker or a lost spawn, and a
        // free runner lock does not tell them apart, so it is only observed
        ReleaseWatcherAcceptance::Existing {
            task,
            state: task_state,
        } if *task == task_id => {
            if matches!(task_state, TaskState::Running { .. }) {
                spawn_task_actor(supervisor, state, task_id).await?;
            }
        }
        ReleaseWatcherAcceptance::UnsupportedRemoteSupervisor { .. } => {}
        ReleaseWatcherAcceptance::Inserted { .. } | ReleaseWatcherAcceptance::Existing { .. } => {
            return Err(AppError::Internal {
                message: format!(
                    "release watcher acceptance returned an unexpected identity for {task_id}"
                ),
            });
        }
    }

    Ok(Ok(acceptance))
}

/// Reuse the environment and callback saved by an earlier acceptance of this watcher
///
/// The canonical command and spec are rebuilt and compared by the store, but the
/// daemon environment can differ after a restart and must not turn an exact retry
/// into a conflict
async fn watcher_launch_context(
    state: &SupervisorState,
    intent: &ReleaseWatcherIntent,
    cwd: &std::path::Path,
) -> Result<(TaskEnv, CallbackContext), AppError> {
    let task_id = intent.watcher_task_id.as_task_id();
    let saved_row = call(&state.store, |reply| StoreMsg::GetTask {
        id: task_id,
        reply,
    })
    .await?;
    let saved_route = call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request: intent.request_id,
        reply,
    })
    .await?;
    if let (Some(row), Some(route)) = (saved_row, saved_route)
        && route.task == task_id
    {
        return Ok((row.env, route.callback));
    }

    let env = TaskEnv::capture();
    let callback = CallbackContext {
        codex: resolve_callback_codex(state, &env.path, cwd),
        env: env.clone(),
        cwd: cwd.to_path_buf(),
    };
    Ok((env, callback))
}

pub(super) async fn decide_return(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    authority: SupervisorActionAuthority,
    decision: ReturnDecision,
) -> Result<Result<ReturnDecisionOutcome, ReturnDecisionError>, AppError> {
    let launch = match decision {
        ReturnDecision::NoResume { reason } => {
            let result = call(&state.store, |reply| StoreMsg::RecordNoResumeForAuthority {
                authority,
                reason,
                reply,
            })
            .await?;
            if result.is_ok() {
                wake_resource(state, authority.resource_id);
            }
            return Ok(result.map(|closure| ReturnDecisionOutcome::Closed(Box::new(closure))));
        }
        ReturnDecision::Launch(launch) => *launch,
    };

    let executor_env = TaskEnv::capture();
    let callback_cwd = match launch.work.supervisor_spec() {
        Some(spec) => spec.as_normalized().cwd.clone(),
        None => state.home.root().to_path_buf(),
    };
    let callback_codex = resolve_callback_codex(state, &executor_env.path, &callback_cwd);
    launch_return_task(
        supervisor,
        state,
        authority,
        launch,
        executor_env,
        ReturnTaskOrigin::Local { callback_codex },
    )
    .await
    .map(|result| result.map(ReturnDecisionOutcome::Launch))
}

/// Bind one fixed return task, and spawn it only when this call inserted it
async fn launch_return_task(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    authority: SupervisorActionAuthority,
    launch: ReturnLaunch,
    executor_env: TaskEnv,
    origin: ReturnTaskOrigin,
) -> Result<Result<ReturnTaskAcceptance, ReturnDecisionError>, AppError> {
    let task_id = launch.task_id;
    let reporter = LaunchReporter::start(
        state,
        authority.resource_id,
        ResourceMsg::RestoreLaunchStarted {
            action_id: authority.action_id,
            task_id,
        },
    )?;
    let report = |result| {
        reporter.finish(ResourceMsg::RestoreLaunchFinished {
            action_id: authority.action_id,
            task_id,
            result,
        });
    };

    let acceptance = call(&state.store, |reply| {
        StoreMsg::AcceptReturnTaskForAuthority {
            input: Box::new(ReturnTaskAcceptanceInput {
                authority,
                launch,
                executor_env,
                origin,
            }),
            reply,
        }
    })
    .await;
    let acceptance = match acceptance {
        Ok(Ok(acceptance)) => acceptance,
        Ok(Err(error)) => {
            report(RestoreLaunchResult::NotInserted);
            return Ok(Err(error));
        }
        Err(error) => {
            report(RestoreLaunchResult::NotInserted);
            return Err(error);
        }
    };

    let result = match &acceptance {
        // only the transaction that inserted the row may spawn its worker
        ReturnTaskAcceptance::Inserted { task, .. } if *task == task_id => {
            if let Err(error) = spawn_inserted(supervisor, state, task_id).await {
                // the committed row may have no worker, so it must show as attention
                report(RestoreLaunchResult::NotInserted);
                return Err(error);
            }
            RestoreLaunchResult::Inserted
        }
        // an existing binding is observed from durable state and never respawned
        ReturnTaskAcceptance::Existing {
            task,
            state: status,
        } if *task == task_id => {
            if *status == ProcessStatus::Running {
                spawn_task_actor(supervisor, state, task_id).await?;
            }
            RestoreLaunchResult::Existing { state: *status }
        }
        ReturnTaskAcceptance::UnsupportedRemoteSupervisor { .. } => {
            RestoreLaunchResult::NotInserted
        }
        ReturnTaskAcceptance::Inserted { .. } | ReturnTaskAcceptance::Existing { .. } => {
            report(RestoreLaunchResult::NotInserted);
            return Err(AppError::Internal {
                message: format!(
                    "return task acceptance returned an unexpected identity for {task_id}"
                ),
            });
        }
    };
    report(result);

    Ok(Ok(acceptance))
}

/// Bind one co-located first background launch, and spawn it only when this call inserted it
pub(super) async fn launch_background(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    launch: BackgroundLaunch,
) -> Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError> {
    let BackgroundLaunch {
        resource_id,
        request_id,
        spec,
        env,
        callback_cwd,
    } = launch;
    let callback_codex = resolve_callback_codex(state, &env.path, &callback_cwd);
    let input = BackgroundLaunchInput {
        authority_machine: state.machine,
        resource_id,
        request_id,
        task_id: TaskId::new(),
        spec,
        env,
        callback_codex,
    };
    bind_background_launch(supervisor, state, resource_id, request_id, |reply| {
        StoreMsg::AcceptBackgroundLaunchForAuthority {
            input: Box::new(input),
            reply,
        }
    })
    .await
}

/// Bind one remote supervisor's first background launch, and spawn it only on insertion
///
/// The task runs with this authority's executor environment. The callback
/// context stays in the route that the supervisor machine saved
pub(super) async fn launch_remote_background(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    launch: RemoteBackgroundLaunch,
) -> Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError> {
    let RemoteBackgroundLaunch { receipt, spec } = launch;
    let resource_id = receipt.binding.assignment.resource_id;
    ensure_resource_actor(supervisor, state, resource_id).await?;
    let input = RemoteBackgroundLaunchInput {
        receipt,
        spec,
        env: TaskEnv::capture(),
    };
    bind_background_launch(
        supervisor,
        state,
        resource_id,
        receipt.request_id,
        |reply| StoreMsg::AcceptRemoteBackgroundLaunchForAuthority {
            input: Box::new(input),
            reply,
        },
    )
    .await
}

/// Apply one store launch binding, and spawn the task only when the store inserted it
///
/// An existing launch is observed from durable state. A queued row found again
/// may already have a worker or may have lost its spawn, and a free runner lock
/// cannot tell them apart, so it is never respawned here
async fn bind_background_launch(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    resource_id: ResourceId,
    request_id: RequestId,
    accept: impl FnOnce(
        RpcReplyPort<Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError>>,
    ) -> StoreMsg,
) -> Result<Result<BackgroundLaunchAcceptance, BackgroundLaunchError>, AppError> {
    let reporter = LaunchReporter::start(
        state,
        resource_id,
        ResourceMsg::BackgroundLaunchStarted { request_id },
    )?;
    let report = |result| {
        reporter.finish(ResourceMsg::BackgroundLaunchFinished { request_id, result });
    };

    let acceptance = match call(&state.store, accept).await {
        Ok(Ok(acceptance)) => acceptance,
        Ok(Err(error)) => {
            report(BackgroundLaunchResult::NotInserted);
            return Ok(Err(error));
        }
        Err(error) => {
            // the store may have committed; the resource owner treats a queued row as uncertain
            report(BackgroundLaunchResult::NotInserted);
            return Err(error);
        }
    };

    let result = match &acceptance {
        // only the transaction that inserted the row may spawn its worker
        BackgroundLaunchAcceptance::Inserted { task, .. } => {
            if let Err(error) = spawn_inserted(supervisor, state, *task).await {
                // the committed row may have no worker, so it must show as uncertain
                report(BackgroundLaunchResult::NotInserted);
                return Err(error);
            }
            BackgroundLaunchResult::Inserted
        }
        BackgroundLaunchAcceptance::Existing {
            task,
            state: status,
        } => {
            if *status == ProcessStatus::Running {
                spawn_task_actor(supervisor, state, *task).await?;
            }
            BackgroundLaunchResult::Existing
        }
        BackgroundLaunchAcceptance::UnsupportedRemoteSupervisor { .. } => {
            BackgroundLaunchResult::NotInserted
        }
    };
    report(result);

    Ok(Ok(acceptance))
}

/// Apply one validated remote-supervisor operation on this authority
///
/// Prepare operations only derive or bind identities. Launch operations commit
/// the task records and exact receipt before any spawn, and only an insertion
/// by this call starts a worker
pub(super) async fn resource_action(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    request: ResourceActionRequest,
) -> Result<ResourceActionOutcome, AppError> {
    let authority = request.authority;
    match request.operation {
        ResourceActionOperation::PrepareReleaseWatcher {
            observed_background_task,
        } => {
            ensure_resource_actor(supervisor, state, authority.resource_id).await?;
            let actor =
                state
                    .resources
                    .get(&authority.resource_id)
                    .ok_or_else(|| AppError::Internal {
                        message: format!(
                            "resource actor {} disappeared",
                            authority.resource_id.as_uuid()
                        ),
                    })?;
            let prepared = call(actor, |reply| ResourceMsg::PrepareRemoteWatcher {
                authority,
                observed_background_task,
                reply,
            })
            .await?;
            Ok(match prepared {
                Ok(task) => ResourceActionOutcome::Prepared { task },
                Err(reason) => ResourceActionOutcome::Rejected { reason },
            })
        }
        ResourceActionOperation::LaunchReleaseWatcher {
            observed_background_task,
            task,
        } => {
            launch_remote_release_watcher(
                supervisor,
                state,
                authority,
                observed_background_task,
                task,
            )
            .await
        }
        ResourceActionOperation::PrepareReturn { launch } => {
            let (request_id, task_id) = (launch.request_id, launch.task_id);
            let prepared = call(&state.store, |reply| {
                StoreMsg::PrepareReturnTaskForAuthority {
                    authority,
                    launch: Box::new(launch),
                    executor_env: TaskEnv::capture(),
                    reply,
                }
            })
            .await?;
            match prepared {
                Ok(prepared) => Ok(ResourceActionOutcome::Prepared {
                    task: PreparedActionTask {
                        request_id,
                        task_id,
                        spec: prepared.spec,
                        normalized_spec_sha256: prepared.normalized_spec_sha256,
                    },
                }),
                Err(error) => return_rejection(error),
            }
        }
        ResourceActionOperation::LaunchReturn {
            launch,
            normalized_spec_sha256,
        } => {
            let receipt = ActionTaskReceipt {
                kind: ResourceActionKind::Return,
                authority,
                request_id: launch.request_id,
                task_id: launch.task_id,
                normalized_spec_sha256,
            };
            let accepted = launch_return_task(
                supervisor,
                state,
                authority,
                launch,
                TaskEnv::capture(),
                ReturnTaskOrigin::Remote {
                    normalized_spec_sha256,
                },
            )
            .await?;
            let acceptance = match accepted {
                Ok(ReturnTaskAcceptance::Inserted { .. }) => ActionTaskAcceptance::Inserted,
                Ok(ReturnTaskAcceptance::Existing { state, .. }) => {
                    ActionTaskAcceptance::Existing { state }
                }
                Ok(ReturnTaskAcceptance::UnsupportedRemoteSupervisor { .. }) => {
                    return Ok(ResourceActionOutcome::Rejected {
                        reason: ResourceActionRejection::NotCurrentSupervisor,
                    });
                }
                Err(error) => return return_rejection(error),
            };
            Ok(ResourceActionOutcome::Accepted {
                receipt,
                acceptance,
            })
        }
        ResourceActionOperation::NoResume { reason } => {
            let closed = call(&state.store, |reply| StoreMsg::RecordNoResumeForAuthority {
                authority,
                reason,
                reply,
            })
            .await?;
            closed_outcome(state, authority.resource_id, closed)
        }
        ResourceActionOperation::HoldReturn { hold } => {
            let held = call(&state.store, |reply| StoreMsg::HoldReturnForAuthority {
                authority,
                hold,
                reply,
            })
            .await?;
            match held {
                Ok(window) => Ok(ResourceActionOutcome::ReturnHeld { window }),
                Err(error) => return_rejection(error),
            }
        }
        ResourceActionOperation::ResolveEndedRestore { task_id, reason } => {
            let closed = call(&state.store, |reply| {
                StoreMsg::ResolveEndedRestoreForAuthority {
                    resolution: Box::new(EndedRestoreResolution {
                        authority,
                        task_id,
                        reason,
                    }),
                    reply,
                }
            })
            .await?;
            closed_outcome(state, authority.resource_id, closed)
        }
    }
}

/// Accept a remote supervisor's bound watcher and spawn only a fresh insertion
async fn launch_remote_release_watcher(
    supervisor: &ActorRef<SupervisorMsg>,
    state: &mut SupervisorState,
    authority: SupervisorActionAuthority,
    observed_background_task: TaskId,
    task: ActionTaskIdentity,
) -> Result<ResourceActionOutcome, AppError> {
    let task_id = task.task_id;
    let executable = crate::resource::release_watcher::release_watcher_executable()?;
    // the authority rebuilds its own command; the store rejects any other identity
    let spec = ReleaseWatcherCommand {
        resource_id: authority.resource_id,
        action_id: authority.action_id,
        state_revision: authority.expected_state_revision,
        trainer_task_id: observed_background_task,
        watcher_task_id: crate::resource::ReleaseWatcherTaskId::new(task_id),
    }
    .normalized_spec(&executable, authority.supervisor.thread)?;
    let row = new_queued_task(NewTask {
        id: task_id,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: persist_workload(&spec.workload),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: TaskEnv::capture(),
        binary: executable,
    });
    let reporter = LaunchReporter::start(
        state,
        authority.resource_id,
        ResourceMsg::RemoteWatcherLaunchStarted {
            action_id: authority.action_id,
            watcher_task_id: task_id,
        },
    )?;
    let report = |result| {
        reporter.finish(ResourceMsg::WatcherLaunchFinished {
            action_id: authority.action_id,
            watcher_task_id: task_id,
            result,
        });
    };

    let accepted = call(&state.store, |reply| {
        StoreMsg::AcceptRemoteReleaseWatcherForAuthority {
            input: Box::new(RemoteReleaseWatcherAcceptanceInput {
                authority,
                observed_background_task,
                task,
                row,
                spec,
            }),
            reply,
        }
    })
    .await;
    let AcceptedActionTask {
        receipt,
        acceptance,
    } = match accepted {
        Ok(Ok(accepted)) => accepted,
        Ok(Err(ResourceActionError::Rejected(reason))) => {
            report(ReleaseWatcherLaunchResult::Rejected);
            return Ok(ResourceActionOutcome::Rejected { reason });
        }
        Ok(Err(ResourceActionError::Storage(error))) | Err(error) => {
            report(ReleaseWatcherLaunchResult::Uncertain);
            return Err(error);
        }
    };

    let result = match acceptance {
        // only the transaction that inserted the row may spawn its worker
        ActionTaskAcceptance::Inserted => {
            if let Err(error) = spawn_inserted(supervisor, state, task_id).await {
                // the committed row may have no worker, so it must show as attention
                report(ReleaseWatcherLaunchResult::Uncertain);
                return Err(error);
            }
            ReleaseWatcherLaunchResult::Inserted
        }
        // an existing acceptance is observed from durable state and never respawned
        ActionTaskAcceptance::Existing { state: status } => {
            if status == ProcessStatus::Running {
                spawn_task_actor(supervisor, state, task_id).await?;
            }
            ReleaseWatcherLaunchResult::Existing { state: status }
        }
    };
    report(result);

    Ok(ResourceActionOutcome::Accepted {
        receipt,
        acceptance,
    })
}

fn closed_outcome(
    state: &SupervisorState,
    resource_id: ResourceId,
    closed: Result<ReturnClosure, ReturnDecisionError>,
) -> Result<ResourceActionOutcome, AppError> {
    match closed {
        Ok(closure) => {
            wake_resource(state, resource_id);
            Ok(ResourceActionOutcome::Closed {
                loan: closure.loan,
                state_revision: closure.state_revision,
            })
        }
        Err(error) => return_rejection(error),
    }
}

/// Keep storage failures retryable and turn every domain refusal into a typed rejection
fn return_rejection(error: ReturnDecisionError) -> Result<ResourceActionOutcome, AppError> {
    return_decision_rejection(error).map(|reason| ResourceActionOutcome::Rejected { reason })
}

/// Split one return-decision error into a definitive rejection or an unknown outcome
///
/// Storage, encoding, and internal task-record failures leave the result unknown,
/// so they stay errors and are never reported as a refusal
pub(crate) fn return_decision_rejection(
    error: ReturnDecisionError,
) -> Result<ResourceActionRejection, AppError> {
    let reason = match error {
        ReturnDecisionError::Storage(error) => return Err(error.into()),
        ReturnDecisionError::Encoding(error) => return Err(error.into()),
        ReturnDecisionError::Identity(crate::store::IdentityError::Storage(error))
        | ReturnDecisionError::TaskRecords(error @ AppError::Internal { .. }) => return Err(error),
        ReturnDecisionError::Resource(ResourceStoreError::Storage(error)) => {
            return Err(error.into());
        }
        ReturnDecisionError::RevisionExhausted { revision } => {
            return Err(AppError::Internal {
                message: format!("resource revision {revision:?} cannot be incremented"),
            });
        }
        ReturnDecisionError::NotCurrentSupervisor | ReturnDecisionError::OriginMismatch => {
            ResourceActionRejection::NotCurrentSupervisor
        }
        ReturnDecisionError::ActionNotPending { .. }
        | ReturnDecisionError::InvalidReturnNotice { .. }
        | ReturnDecisionError::Resource(_) => ResourceActionRejection::ActionNotPending,
        ReturnDecisionError::StaleRevision { expected, actual } => {
            ResourceActionRejection::StaleRevision { expected, actual }
        }
        ReturnDecisionError::SpecMismatch => ResourceActionRejection::SpecMismatch,
        ReturnDecisionError::ConflictingRetry { .. } => ResourceActionRejection::ConflictingRetry,
        ReturnDecisionError::IdentityConflict { .. } | ReturnDecisionError::Identity(_) => {
            ResourceActionRejection::IdentityConflict
        }
        error @ (ReturnDecisionError::RestoreNotEnded { .. }
        | ReturnDecisionError::RestoreReleaseUnproven { .. }
        | ReturnDecisionError::RestoreOwnershipUnproven { .. }) => {
            ResourceActionRejection::RestoreNotResolvable {
                reason: error.to_string(),
            }
        }
        error @ (ReturnDecisionError::Rejected(_)
        | ReturnDecisionError::HoldRejected(_)
        | ReturnDecisionError::BackgroundTaskMismatch
        | ReturnDecisionError::TaskRecords(_)) => ResourceActionRejection::DecisionRejected {
            reason: error.to_string(),
        },
    };
    Ok(reason)
}
