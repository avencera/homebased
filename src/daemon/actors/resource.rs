//! Authority-owned queue reconciliation for one resource.

use ractor::{Actor, ActorId, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::daemon::actors::supervisor::ReleaseWatcherLaunch;
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call, send_reply};
use crate::domain::{ProcessStatus, TaskEnv, TaskId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::bound_action::{PreparedActionTask, ResourceActionRejection};
use crate::resource::release_watcher::{ReleaseWatcherCommand, release_watcher_executable};
use crate::resource::store::{
    AssignedResourceTaskAttention, AssignedResourceTaskProgress,
    AssignedResourceTaskReconcileInput, AssignedResourceTaskReconcileOutcome, CompleteReleaseError,
    ReleaseCompletionResult, ReleaseWatcherAcceptance, ReleaseWatcherAcceptanceError,
    ResourceSnapshot, ResourceTaskAcceptance, ResourceTaskAcceptanceInput,
    ResourceTaskCompletionResult,
};
use crate::resource::{
    ActionId, Loan, LoanPhase, LoanState, ReleaseProofAttentionReason, ReleaseWatcherIntent,
    ReleaseWatcherTaskId, Resource, ResourceId, ResourceQueueAttentionReason,
    ResourceQueueReconcileOutcome, ResourceRequest, ResourceRequestState, RestoreAttentionReason,
    SavedReleaseWatcherIntent, ServingReleaseProvenance, SupervisorActionAuthority,
};
use crate::store::{BackgroundLaunchPhase, BackgroundLaunchView, RestoreReconcileOutcome};
use crate::submission::{RequestId, normalized_spec_sha256};

/// Messages for one resource actor
pub enum ResourceMsg {
    /// Reconcile current authority-owned queue state and refresh this actor's snapshot
    Reconcile {
        /// Reply with the typed reconciliation outcome
        reply: RpcReplyPort<Result<ResourceQueueReconcileOutcome, AppError>>,
    },
    /// Wake only when one exact assigned task reached a task-layer terminal event
    TaskTerminal {
        /// Exact task whose durable terminal state is ready to reconcile
        task_id: TaskId,
    },
    /// Reconcile after this actor commits a queue transition
    Wake,
    /// Return the actor identity and the latest authority-owned snapshot
    Inspect {
        /// Reply with the actor's identity and current snapshot
        reply: RpcReplyPort<Result<ResourceActorInspection, AppError>>,
    },
    /// Result of one asynchronous, exact assigned-task launch request
    ActivationFinished {
        /// Exact request sent to the supervisor
        request_id: crate::submission::RequestId,
        /// Exact preallocated task sent to the supervisor
        task_id: crate::domain::TaskId,
        /// Result returned by the dedicated assigned-task launch path
        result: ResourceTaskActivationResult,
    },
    /// Wake when a durable event for one task was delivered; only the bound return task matters
    TaskProgress {
        /// Task whose durable state may have changed
        task_id: TaskId,
    },
    /// The supervisor is about to bind and launch one fixed return task
    RestoreLaunchStarted {
        /// Return action that owns the task
        action_id: ActionId,
        /// Fixed return task identity
        task_id: TaskId,
    },
    /// Result of the supervisor's one bind-and-launch request for a return task
    RestoreLaunchFinished {
        /// Return action that owns the task
        action_id: ActionId,
        /// Fixed return task identity
        task_id: TaskId,
        /// Whether that request inserted the task and started its worker
        result: RestoreLaunchResult,
    },
    /// The supervisor is about to bind and launch one first background task
    BackgroundLaunchStarted {
        /// Stable launch request identity
        request_id: RequestId,
    },
    /// Result of the supervisor's one bind-and-launch request for a first background task
    BackgroundLaunchFinished {
        /// Stable launch request identity
        request_id: RequestId,
        /// Whether that request inserted the task and started its worker
        result: BackgroundLaunchResult,
    },
    /// Bind and baseline the watcher for a remote supervisor, then return its canonical task
    PrepareRemoteWatcher {
        /// Exact action authority named by the supervisor machine
        authority: SupervisorActionAuthority,
        /// Background task named by the release action
        observed_background_task: TaskId,
        /// Canonical watcher task or a definitive refusal
        reply: RpcReplyPort<Result<Result<PreparedActionTask, ResourceActionRejection>, AppError>>,
    },
    /// The supervisor is about to accept a remote supervisor's bound watcher
    RemoteWatcherLaunchStarted {
        /// Release action that owns the watcher
        action_id: ActionId,
        /// Exact bound watcher task
        watcher_task_id: TaskId,
    },
    /// Result of one asynchronous, bound release-watcher launch request
    WatcherLaunchFinished {
        /// Release action that owns the watcher
        action_id: ActionId,
        /// Exact preallocated watcher task sent to the supervisor
        watcher_task_id: TaskId,
        /// Result returned by the dedicated watcher launch path
        result: ReleaseWatcherLaunchResult,
    },
}

/// Outcome of one supervisor request to bind and launch a first background task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundLaunchResult {
    /// The launch request has not replied yet
    Pending,
    /// The launch was inserted by this request, so it was the only spawn attempt
    Inserted,
    /// An exact earlier launch existed and was only observed
    Existing,
    /// Nothing was inserted, or a committed row may have no worker
    NotInserted,
}

/// Outcome of asking the supervisor to accept one bound release watcher
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseWatcherLaunchResult {
    /// The launch request has not replied yet
    Pending,
    /// The watcher row was inserted by this request, so it was the only spawn attempt
    Inserted,
    /// An exact watcher acceptance already existed with this process state
    Existing {
        /// State retained by the task layer
        state: ProcessStatus,
    },
    /// The resource supervisor is not on the authority machine
    UnsupportedRemoteSupervisor,
    /// The authority rejected the watcher identity or command
    Rejected,
    /// The request may have committed, but its result was not definitive
    Uncertain,
}

/// Outcome of the supervisor's one bind-and-launch request for a return task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreLaunchResult {
    /// The request has not replied yet
    Pending,
    /// The request inserted the task, so its worker launch was the only spawn attempt
    Inserted,
    /// An exact earlier binding existed, so the request only observed it
    Existing {
        /// State retained by the task layer
        state: ProcessStatus,
    },
    /// The request wrote no task records or its result was not definitive
    NotInserted,
}

/// Launch and observation state of the bound watcher for one release action
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseWatcherStatus {
    /// The watcher is bound, and the remote supervisor machine must save its route and launch it
    AwaitingRemoteSupervisor {
        /// Release action that owns the watcher
        action_id: ActionId,
        /// Exact bound watcher task
        watcher_task_id: TaskId,
        /// Machine that owns the supervisor thread and the watcher callback route
        supervisor_machine: MachineId,
    },
    /// The one launch request for this watcher is in flight or its task has not started
    Launching {
        /// Release action that owns the watcher
        action_id: ActionId,
        /// Exact preallocated watcher task
        watcher_task_id: TaskId,
    },
    /// The watcher task is running
    Running {
        /// Release action that owns the watcher
        action_id: ActionId,
        /// Exact watcher task
        watcher_task_id: TaskId,
    },
    /// The watcher finished its part; release proof remains with the resource owner
    Finished {
        /// Release action that owns the watcher
        action_id: ActionId,
        /// Exact watcher task
        watcher_task_id: TaskId,
    },
    /// The loan stays reserved and the watcher needs attention
    Attention {
        /// Release action that owns the watcher
        action_id: ActionId,
        /// Typed reason the watcher cannot proceed
        reason: ReleaseWatcherAttentionReason,
    },
}

/// Why a release action cannot use or trust its bound watcher
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseWatcherAttentionReason {
    /// The resource supervisor runs on another machine, so no watcher task was written
    RemoteSupervisorUnsupported {
        /// Machine that owns the supervisor thread
        supervisor_machine: MachineId,
    },
    /// The running trainer has no saved attempt association
    TrainerAssociationMissing {
        /// Exact registered trainer task
        task_id: TaskId,
    },
    /// The saved watcher identity is a legacy record that cannot prove release
    LegacyWatcherIntent,
    /// The daemon cannot name the executable for the canonical watcher command
    ExecutableUnavailable,
    /// The authority rejected binding the watcher identity to the release action
    BindingRejected,
    /// The authority could not capture the checkpoint baseline
    BaselineUnavailable,
    /// The authority rejected the watcher acceptance or its canonical command
    LaunchRejected {
        /// Exact watcher task
        watcher_task_id: TaskId,
    },
    /// The watcher task was accepted, but whether its worker started is unknown
    LaunchUncertain {
        /// Exact watcher task
        watcher_task_id: TaskId,
    },
    /// The watcher task ended without finishing its part
    WatcherTaskEnded {
        /// Exact watcher task
        watcher_task_id: TaskId,
        /// Terminal task state
        state: ProcessStatus,
    },
}

/// Outcome of asking the supervisor to accept one fixed resource task
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceTaskActivationResult {
    /// The task layer inserted this task for the first time
    Inserted,
    /// An exact task acceptance already existed with this process state
    Existing {
        /// State retained by the task layer
        state: ProcessStatus,
    },
    /// Cancellation prevented task acceptance
    Prevented,
    /// The request may have committed, but the supervisor result was not definitive
    Uncertain,
}

/// Read-only view of one resource actor
#[derive(Debug, Clone)]
pub struct ResourceActorInspection {
    /// Ractor identity of the actor that returned this snapshot
    pub actor_id: ActorId,
    /// Durable resource state refreshed after the most recent reconciliation
    pub resource: Resource,
    /// Durable active or attention loan restored by the actor, if present
    pub loan: Option<Loan>,
    /// Latest typed queue outcome, if a reconciliation has completed
    pub reconcile_outcome: Option<ResourceQueueReconcileOutcome>,
    /// Bound watcher state for the current release action, if one applies
    pub release_watcher: Option<ReleaseWatcherStatus>,
}

/// State for one resource actor
pub struct ResourceActorState {
    store: ActorRef<StoreMsg>,
    supervisor: Option<ActorRef<SupervisorMsg>>,
    authority_machine: MachineId,
    resource: Resource,
    loan: Option<Loan>,
    reconcile_outcome: Option<ResourceQueueReconcileOutcome>,
    activation_attempt: Option<ActivationAttempt>,
    watcher_launch: Option<WatcherLaunchAttempt>,
    release_watcher: Option<ReleaseWatcherStatus>,
    restore_launch: Option<RestoreLaunchAttempt>,
    background_launch: Option<BackgroundLaunchAttempt>,
    pending_background_task: Option<TaskId>,
}

// one supervisor launch request per first background request and actor lifetime;
// a queued row that this lifetime did not insert may have lost its spawn
#[derive(Debug, Clone, Copy)]
struct BackgroundLaunchAttempt {
    request_id: RequestId,
    result: BackgroundLaunchResult,
}

// one supervisor launch request per return task and actor lifetime; a queued row
// that this lifetime did not insert may have lost its spawn and is only observed
#[derive(Debug, Clone, Copy)]
struct RestoreLaunchAttempt {
    action_id: ActionId,
    task_id: TaskId,
    result: RestoreLaunchResult,
}

// one launch request per watcher identity and actor lifetime; a new actor after a
// restart asks again with the same saved identity and only observes what it finds
#[derive(Debug, Clone, Copy)]
struct WatcherLaunchAttempt {
    action_id: ActionId,
    watcher_task_id: TaskId,
    result: ReleaseWatcherLaunchResult,
}

#[derive(Debug, Clone, Copy)]
struct ActivationAttempt {
    request_id: crate::submission::RequestId,
    task_id: crate::domain::TaskId,
    result: ActivationAttemptResult,
}

#[derive(Debug, Clone, Copy)]
enum ActivationAttemptResult {
    Pending,
    Inserted,
    Existing(ProcessStatus),
    Prevented,
    Uncertain,
}

/// Actor that reconciles one resource from StoreActor-owned state
pub struct ResourceActor;

impl Actor for ResourceActor {
    type Msg = ResourceMsg;
    type State = ResourceActorState;
    type Arguments = (
        ActorRef<StoreMsg>,
        Option<ActorRef<SupervisorMsg>>,
        MachineId,
        Resource,
        Option<Loan>,
    );

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        (store, supervisor, authority_machine, resource, loan): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let mut state = ResourceActorState {
            store,
            supervisor,
            authority_machine,
            resource,
            loan,
            reconcile_outcome: None,
            activation_attempt: None,
            watcher_launch: None,
            release_watcher: None,
            restore_launch: None,
            background_launch: None,
            pending_background_task: None,
        };
        reconcile_and_refresh(&_myself, &mut state).await?;

        Ok(state)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ResourceMsg::Reconcile { reply } => {
                send_reply(reply, reconcile_and_refresh(&myself, state).await);
            }
            ResourceMsg::TaskTerminal { task_id } => {
                if restoring_task_id(state) == Some(task_id)
                    || state.pending_background_task == Some(task_id)
                    || serving_task_id(state).await? == Some(task_id)
                {
                    reconcile_and_refresh(&myself, state).await?;
                }
            }
            ResourceMsg::TaskProgress { task_id } => {
                if restoring_task_id(state) == Some(task_id)
                    || state.pending_background_task == Some(task_id)
                    || state
                        .watcher_launch
                        .is_some_and(|attempt| attempt.watcher_task_id == task_id)
                {
                    reconcile_and_refresh(&myself, state).await?;
                }
            }
            ResourceMsg::RestoreLaunchStarted { action_id, task_id } => {
                state.restore_launch = Some(RestoreLaunchAttempt {
                    action_id,
                    task_id,
                    result: RestoreLaunchResult::Pending,
                });
            }
            ResourceMsg::RestoreLaunchFinished {
                action_id,
                task_id,
                result,
            } => {
                if let Some(attempt) = state.restore_launch.as_mut()
                    && attempt.action_id == action_id
                    && attempt.task_id == task_id
                    && attempt.result == RestoreLaunchResult::Pending
                {
                    attempt.result = result;
                }
                reconcile_and_refresh(&myself, state).await?;
            }
            ResourceMsg::BackgroundLaunchStarted { request_id } => {
                state.background_launch = Some(BackgroundLaunchAttempt {
                    request_id,
                    result: BackgroundLaunchResult::Pending,
                });
            }
            ResourceMsg::BackgroundLaunchFinished { request_id, result } => {
                if let Some(attempt) = state.background_launch.as_mut()
                    && attempt.request_id == request_id
                    && attempt.result == BackgroundLaunchResult::Pending
                {
                    attempt.result = result;
                }
                reconcile_and_refresh(&myself, state).await?;
            }
            ResourceMsg::Wake => {
                reconcile_and_refresh(&myself, state).await?;
            }
            ResourceMsg::ActivationFinished {
                request_id,
                task_id,
                result,
            } => {
                handle_activation_finished(&myself, state, request_id, task_id, result).await?;
            }
            ResourceMsg::WatcherLaunchFinished {
                action_id,
                watcher_task_id,
                result,
            } => {
                handle_watcher_launch_finished(state, action_id, watcher_task_id, result).await?;
            }
            ResourceMsg::PrepareRemoteWatcher {
                authority,
                observed_background_task,
                reply,
            } => {
                let prepared =
                    prepare_remote_watcher(state, authority, observed_background_task).await;
                send_reply(reply, prepared);
                reconcile_and_refresh(&myself, state).await?;
            }
            ResourceMsg::RemoteWatcherLaunchStarted {
                action_id,
                watcher_task_id,
            } => {
                // only this lifetime's own accepted request may show a queued row as launching
                state.watcher_launch = Some(WatcherLaunchAttempt {
                    action_id,
                    watcher_task_id,
                    result: ReleaseWatcherLaunchResult::Pending,
                });
            }
            ResourceMsg::Inspect { reply } => send_reply(
                reply,
                Ok(ResourceActorInspection {
                    actor_id: myself.get_id(),
                    resource: state.resource.clone(),
                    loan: state.loan.clone(),
                    reconcile_outcome: state.reconcile_outcome.clone(),
                    release_watcher: state.release_watcher.clone(),
                }),
            ),
        }

        Ok(())
    }
}

async fn reconcile_and_refresh(
    myself: &ActorRef<ResourceMsg>,
    state: &mut ResourceActorState,
) -> Result<ResourceQueueReconcileOutcome, AppError> {
    let mut outcome = call(&state.store, |reply| StoreMsg::ReconcileResourceQueue {
        authority_machine: state.authority_machine,
        resource_id: state.resource.id,
        reply,
    })
    .await?;

    let mut snapshot = load_snapshot(state).await?;
    let release_failure = if let Some((loan, action_id, task_id)) = awaiting_release(&snapshot) {
        match call(&state.store, |reply| {
            StoreMsg::CompleteReleaseForAuthority {
                authority_machine: state.authority_machine,
                resource_id: snapshot.resource.id,
                action_id,
                expected_state_revision: snapshot.resource.state_revision,
                reply,
            }
        })
        .await?
        {
            Ok(ReleaseCompletionResult::Assigned { loan, .. })
            | Ok(ReleaseCompletionResult::ReturnRequired { loan, .. }) => {
                outcome = ResourceQueueReconcileOutcome::LoanAlreadyActive { loan };
                snapshot = load_snapshot(state).await?;
                None
            }
            Err(error) => Some((
                loan,
                action_id,
                task_id,
                release_proof_attention_reason(error),
            )),
        }
    } else {
        None
    };

    state.release_watcher = match &release_failure {
        Some((loan, action_id, task_id, _)) => {
            let resource = snapshot.resource.clone();
            progress_release_watcher(myself, state, &resource, loan, *action_id, *task_id).await?
        }
        None => {
            state.watcher_launch = None;
            None
        }
    };

    let requests = call(&state.store, |reply| StoreMsg::ResourceRequests {
        authority_machine: state.authority_machine,
        resource_id: snapshot.resource.id,
        reply,
    })
    .await?;
    let mut serving_request = serving_assignment(&snapshot, &requests);
    let current_activation = serving_request
        .as_ref()
        .map(|(_, request, _)| (request.request_id, request.task_id));
    if state
        .activation_attempt
        .is_some_and(|attempt| Some((attempt.request_id, attempt.task_id)) != current_activation)
    {
        state.activation_attempt = None;
    }

    let fresh_inserted = state.activation_attempt.is_some_and(|attempt| {
        Some((attempt.request_id, attempt.task_id)) == current_activation
            && matches!(attempt.result, ActivationAttemptResult::Inserted)
    });
    let mut accepted_progress = None;
    let mut task_attention = None;
    let mut completion_committed = false;
    if let Some((loan, request, _)) = &serving_request {
        let input = AssignedResourceTaskReconcileInput {
            authority_machine: state.authority_machine,
            resource_id: request.resource_id,
            loan_id: loan.id,
            request_id: request.request_id,
            task_id: request.task_id,
            expected_state_revision: snapshot.resource.state_revision,
        };
        match call(&state.store, |reply| {
            StoreMsg::AssignedResourceTaskReconcile { input, reply }
        })
        .await?
        {
            Err(error) => {
                let task_id = request.task_id;
                tracing::warn!(%task_id, "assigned resource task reconciliation failed: {error}");
                task_attention =
                    Some(ResourceQueueAttentionReason::AssignedTaskReconcileFailed { task_id });
            }
            Ok(AssignedResourceTaskReconcileOutcome::NotAccepted) => {}
            Ok(AssignedResourceTaskReconcileOutcome::Active(progress)) => {
                accepted_progress = Some(progress);
            }
            Ok(AssignedResourceTaskReconcileOutcome::Attention(reason)) => {
                task_attention = Some(task_completion_attention_reason(request.task_id, reason));
            }
            Ok(AssignedResourceTaskReconcileOutcome::Completed(result)) => {
                let next_assignment =
                    matches!(*result, ResourceTaskCompletionResult::Assigned { .. });
                snapshot = load_snapshot(state).await?;
                let Some(updated_loan) = snapshot.loan.clone() else {
                    return Err(AppError::Internal {
                        message: "resource task completion removed its active loan".into(),
                    });
                };
                outcome = ResourceQueueReconcileOutcome::LoanAlreadyActive { loan: updated_loan };
                completion_committed = true;
                if next_assignment {
                    myself.cast(ResourceMsg::Wake)?;
                }
            }
        }
    }

    if !completion_committed {
        if let Some((loan, action_id, task_id, reason)) = release_failure {
            outcome = ResourceQueueReconcileOutcome::ReleaseProofUnavailable {
                loan,
                action_id,
                task_id,
                reason,
            };
        } else if let Some((loan, request, provenance)) = serving_request.take() {
            if let Some(reason) = task_attention {
                outcome = ResourceQueueReconcileOutcome::AttentionRequired { request, reason };
            } else if matches!(provenance, ServingReleaseProvenance::Unverified) {
                outcome = ResourceQueueReconcileOutcome::AttentionRequired {
                    request,
                    reason: ResourceQueueAttentionReason::UnverifiedServingRelease,
                };
            } else if let Some(progress) = accepted_progress {
                // only this actor's own Inserted launch may still be starting; an
                // existing queued row from before is never respawned here
                if progress == AssignedResourceTaskProgress::Queued && !fresh_inserted {
                    let task_id = request.task_id;
                    outcome = ResourceQueueReconcileOutcome::AttentionRequired {
                        request,
                        reason: ResourceQueueAttentionReason::AcceptedTaskLaunchUncertain {
                            task_id,
                        },
                    };
                } else {
                    outcome = ResourceQueueReconcileOutcome::LoanAlreadyActive { loan };
                }
            } else if let Some(attempt) = state.activation_attempt {
                if attempt.request_id == request.request_id
                    && attempt.task_id == request.task_id
                    && matches!(
                        attempt.result,
                        ActivationAttemptResult::Pending
                            | ActivationAttemptResult::Prevented
                            | ActivationAttemptResult::Uncertain
                            | ActivationAttemptResult::Existing(ProcessStatus::Queued)
                    )
                {
                    outcome = ResourceQueueReconcileOutcome::AttentionRequired {
                        request: request.clone(),
                        reason: ResourceQueueAttentionReason::AssignedTaskLaunchUncertain {
                            task_id: request.task_id,
                        },
                    };
                }
            } else if let Some(supervisor) = state.supervisor.as_ref() {
                let input = ResourceTaskAcceptanceInput {
                    authority_machine: state.authority_machine,
                    resource_id: request.resource_id,
                    request_id: request.request_id,
                    task_id: request.task_id,
                    acceptance_sequence: request.acceptance_sequence,
                    loan_id: loan.id,
                    expected_state_revision: snapshot.resource.state_revision,
                    command_spec: request.spec().clone(),
                    executor_env: TaskEnv::capture(),
                };
                state.activation_attempt = Some(ActivationAttempt {
                    request_id: request.request_id,
                    task_id: request.task_id,
                    result: ActivationAttemptResult::Pending,
                });
                schedule_assigned_activation(myself.clone(), supervisor.clone(), input);
                outcome = ResourceQueueReconcileOutcome::LoanAlreadyActive { loan };
            }
        }
    }

    if let Some(restore_outcome) = reconcile_restore(myself, state, &mut snapshot).await? {
        outcome = restore_outcome;
    }

    if let Some(launch_outcome) = observe_background_launch(state, &snapshot).await? {
        outcome = launch_outcome;
    }

    state.resource = snapshot.resource;
    state.loan = snapshot.loan;
    state.reconcile_outcome = Some(outcome.clone());

    Ok(outcome)
}

/// Observe the bound return task and close the loan by its saved execution mode
///
/// A direct-segment task closes the loan on a confirmed start. A native
/// foreground task keeps it until a successful end with a confirmed
/// process-group exit. A queued row is a launch in progress only while this actor's own supervisor
/// request is pending or inserted it; otherwise its worker may never start. After
/// closure the queue is reconciled again, so requests accepted after the return
/// reservation open the next loan under the normal rules
async fn reconcile_restore(
    myself: &ActorRef<ResourceMsg>,
    state: &mut ResourceActorState,
    snapshot: &mut ResourceSnapshot,
) -> Result<Option<ResourceQueueReconcileOutcome>, AppError> {
    let pending_action = snapshot.loan.as_ref().and_then(|loan| match &loan.state {
        LoanState::Active {
            phase:
                LoanPhase::AwaitingReturn { action_id, .. } | LoanPhase::Restoring { action_id, .. },
        } => Some(*action_id),
        LoanState::Active { .. } | LoanState::NeedsAttention { .. } | LoanState::Closed { .. } => {
            None
        }
    });
    if state
        .restore_launch
        .is_some_and(|attempt| Some(attempt.action_id) != pending_action)
    {
        state.restore_launch = None;
    }
    let Some((loan, action_id, task_id)) = restoring(snapshot) else {
        return Ok(None);
    };

    let reply = call(&state.store, |reply| {
        StoreMsg::ReconcileRestoringLoanForAuthority {
            authority_machine: state.authority_machine,
            resource_id: snapshot.resource.id,
            reply,
        }
    })
    .await?;
    let attention = |loan, action_id, task_id, reason| {
        Ok(Some(
            ResourceQueueReconcileOutcome::RestoreAttentionRequired {
                loan,
                action_id,
                task_id,
                reason,
            },
        ))
    };
    match reply {
        Err(error) => {
            tracing::warn!(%task_id, "restoring resource task reconciliation failed: {error}");
            attention(
                loan,
                action_id,
                task_id,
                RestoreAttentionReason::ReconcileFailed,
            )
        }
        Ok(RestoreReconcileOutcome::NotRestoring) => Ok(None),
        Ok(RestoreReconcileOutcome::Queued {
            loan,
            action_id,
            task_id,
        }) => {
            let launching = state.restore_launch.is_some_and(|attempt| {
                attempt.action_id == action_id
                    && attempt.task_id == task_id
                    && matches!(
                        attempt.result,
                        RestoreLaunchResult::Pending | RestoreLaunchResult::Inserted
                    )
            });
            if launching {
                return Ok(Some(ResourceQueueReconcileOutcome::LoanAlreadyActive {
                    loan,
                }));
            }
            attention(
                loan,
                action_id,
                task_id,
                RestoreAttentionReason::LaunchUncertain,
            )
        }
        Ok(RestoreReconcileOutcome::Attention {
            loan,
            action_id,
            task_id,
            reason,
        }) => attention(loan, action_id, task_id, reason),
        // the running foreground task keeps the loan, so no queued request can start
        Ok(RestoreReconcileOutcome::ForegroundRunning { loan, .. }) => {
            Ok(Some(ResourceQueueReconcileOutcome::LoanAlreadyActive {
                loan,
            }))
        }
        Ok(
            RestoreReconcileOutcome::Closed { .. }
            | RestoreReconcileOutcome::ForegroundEnded { .. },
        ) => {
            state.restore_launch = None;
            let outcome = call(&state.store, |reply| StoreMsg::ReconcileResourceQueue {
                authority_machine: state.authority_machine,
                resource_id: snapshot.resource.id,
                reply,
            })
            .await?;
            *snapshot = load_snapshot(state).await?;
            // a newly opened loan still needs its watcher and assignment steps
            myself.cast(ResourceMsg::Wake)?;
            Ok(Some(outcome))
        }
    }
}

/// Track the pending first background launch and surface a queued row as uncertain
///
/// A queued row is a launch in progress only while this actor's own supervisor
/// request is pending or inserted it. The store registers the task on its
/// confirmed start, so a registered or ended launch needs no tracking here
async fn observe_background_launch(
    state: &mut ResourceActorState,
    snapshot: &ResourceSnapshot,
) -> Result<Option<ResourceQueueReconcileOutcome>, AppError> {
    let view = call(&state.store, |reply| {
        StoreMsg::BackgroundLaunchForAuthority {
            authority_machine: state.authority_machine,
            resource_id: snapshot.resource.id,
            reply,
        }
    })
    .await?;
    state.pending_background_task = view.as_ref().and_then(BackgroundLaunchView::pending_task);
    let Some(view) = view.filter(|view| view.phase == BackgroundLaunchPhase::Queued) else {
        return Ok(None);
    };
    let launching = state.background_launch.is_some_and(|attempt| {
        attempt.request_id == view.request_id
            && matches!(
                attempt.result,
                BackgroundLaunchResult::Pending | BackgroundLaunchResult::Inserted
            )
    });
    if launching {
        return Ok(None);
    }

    Ok(Some(
        ResourceQueueReconcileOutcome::BackgroundLaunchUncertain {
            task_id: view.task_id,
        },
    ))
}

fn restoring(snapshot: &ResourceSnapshot) -> Option<(Loan, ActionId, TaskId)> {
    let loan = snapshot.loan.as_ref()?;
    let LoanState::Active {
        phase:
            LoanPhase::Restoring {
                action_id,
                resume_task_id,
                ..
            },
    } = &loan.state
    else {
        return None;
    };

    Some((loan.clone(), *action_id, *resume_task_id))
}

fn restoring_task_id(state: &ResourceActorState) -> Option<TaskId> {
    match &state.loan.as_ref()?.state {
        LoanState::Active {
            phase: LoanPhase::Restoring { resume_task_id, .. },
        } => Some(*resume_task_id),
        LoanState::Active { .. } | LoanState::NeedsAttention { .. } | LoanState::Closed { .. } => {
            None
        }
    }
}

async fn load_snapshot(state: &ResourceActorState) -> Result<ResourceSnapshot, AppError> {
    let snapshots = call(&state.store, |reply| {
        StoreMsg::ResourceSnapshotsForAuthority {
            authority_machine: state.authority_machine,
            reply,
        }
    })
    .await?;
    snapshots
        .into_iter()
        .find(|snapshot| snapshot.resource.id == state.resource.id)
        .ok_or_else(|| AppError::Internal {
            message: format!(
                "resource {} disappeared from its authority store during reconciliation",
                state.resource.id.as_uuid()
            ),
        })
}

fn awaiting_release(
    snapshot: &ResourceSnapshot,
) -> Option<(Loan, crate::resource::ActionId, crate::domain::TaskId)> {
    let loan = snapshot.loan.as_ref()?;
    let LoanState::Active {
        phase:
            LoanPhase::AwaitingRelease {
                action_id,
                observed_background_task,
                ..
            },
    } = &loan.state
    else {
        return None;
    };

    Some((loan.clone(), *action_id, *observed_background_task))
}

fn serving_assignment(
    snapshot: &ResourceSnapshot,
    requests: &[ResourceRequest],
) -> Option<(Loan, ResourceRequest, ServingReleaseProvenance)> {
    let loan = snapshot.loan.as_ref()?;
    let LoanState::Active {
        phase:
            LoanPhase::Serving {
                current_request_id,
                release_provenance,
                ..
            },
    } = &loan.state
    else {
        return None;
    };
    let request = requests.iter().find(|request| {
        request.request_id == *current_request_id
            && matches!(
                request.state,
                ResourceRequestState::Assigned { loan_id } if loan_id == loan.id
            )
    })?;

    Some((loan.clone(), request.clone(), release_provenance.clone()))
}

fn schedule_assigned_activation(
    resource_actor: ActorRef<ResourceMsg>,
    supervisor: ActorRef<SupervisorMsg>,
    input: ResourceTaskAcceptanceInput,
) {
    let request_id = input.request_id;
    let task_id = input.task_id;
    tokio::spawn(async move {
        let result = call(&supervisor, |reply| {
            SupervisorMsg::LaunchAssignedResourceTask {
                input: Box::new(input),
                reply,
            }
        })
        .await;
        let result = match result {
            Ok(Ok(ResourceTaskAcceptance::Inserted { task })) if task == task_id => {
                ResourceTaskActivationResult::Inserted
            }
            Ok(Ok(ResourceTaskAcceptance::Existing { task, state })) if task == task_id => {
                ResourceTaskActivationResult::Existing { state }
            }
            Ok(Err(crate::resource::store::ResourceStoreError::Prevented)) => {
                ResourceTaskActivationResult::Prevented
            }
            _ => ResourceTaskActivationResult::Uncertain,
        };
        if let Err(error) = resource_actor.cast(ResourceMsg::ActivationFinished {
            request_id,
            task_id,
            result,
        }) {
            tracing::debug!(%task_id, "resource activation result cast: {error}");
        }
    });
}

async fn handle_activation_finished(
    myself: &ActorRef<ResourceMsg>,
    state: &mut ResourceActorState,
    request_id: crate::submission::RequestId,
    task_id: crate::domain::TaskId,
    result: ResourceTaskActivationResult,
) -> Result<(), ActorProcessingErr> {
    let Some(attempt) = state.activation_attempt.as_mut() else {
        return Ok(());
    };
    if attempt.request_id != request_id
        || attempt.task_id != task_id
        || !matches!(attempt.result, ActivationAttemptResult::Pending)
    {
        return Ok(());
    }
    attempt.result = match result {
        ResourceTaskActivationResult::Inserted => ActivationAttemptResult::Inserted,
        ResourceTaskActivationResult::Existing { state } => {
            ActivationAttemptResult::Existing(state)
        }
        ResourceTaskActivationResult::Prevented => ActivationAttemptResult::Prevented,
        ResourceTaskActivationResult::Uncertain => ActivationAttemptResult::Uncertain,
    };
    reconcile_and_refresh(myself, state).await?;

    Ok(())
}

/// Bind, baseline, and request the one watcher for a running trainer
///
/// Every step reuses saved identities first, so a retry or restart binds the same
/// watcher task and request instead of allocating replacements. A co-located
/// supervisor's watcher is launched here; a remote supervisor's machine saves the
/// callback route first and then asks the authority to accept the same identity.
/// The supervisor is called from a detached task because it may be waiting on
/// this actor
async fn progress_release_watcher(
    myself: &ActorRef<ResourceMsg>,
    state: &mut ResourceActorState,
    resource: &Resource,
    loan: &Loan,
    action_id: ActionId,
    trainer_task_id: TaskId,
) -> Result<Option<ReleaseWatcherStatus>, AppError> {
    let Some(supervisor) = state.supervisor.clone() else {
        return Ok(None);
    };
    if state
        .watcher_launch
        .is_some_and(|attempt| attempt.action_id != action_id)
    {
        state.watcher_launch = None;
    }
    if let Some(attempt) = state.watcher_launch {
        if !launch_never_committed(state, attempt).await? {
            return watcher_status(state, attempt).await.map(Some);
        }
        // no row exists, so the uncertain request did not accept this watcher and
        // asking again with the same saved identity cannot start a second worker
        state.watcher_launch = None;
    }

    let (intent, executable) =
        match bind_release_watcher(state, resource, loan, action_id, trainer_task_id).await? {
            WatcherBinding::NotNeeded => return Ok(None),
            WatcherBinding::Attention(reason) => {
                return Ok(Some(ReleaseWatcherStatus::Attention { action_id, reason }));
            }
            WatcherBinding::Bound { intent, executable } => (intent, executable),
        };

    let watcher_task_id = intent.watcher_task_id.as_task_id();
    if resource.supervisor.machine != state.authority_machine {
        return remote_watcher_status(state, resource, action_id, watcher_task_id)
            .await
            .map(Some);
    }
    let attempt = WatcherLaunchAttempt {
        action_id,
        watcher_task_id,
        result: ReleaseWatcherLaunchResult::Pending,
    };
    state.watcher_launch = Some(attempt);
    schedule_release_watcher_launch(
        myself.clone(),
        supervisor,
        ReleaseWatcherLaunch {
            authority_machine: state.authority_machine,
            resource_id: resource.id,
            supervisor: resource.supervisor,
            intent,
            executable,
        },
    );

    watcher_status(state, attempt).await.map(Some)
}

/// Result of binding and baselining the watcher for one release action
enum WatcherBinding {
    /// The trainer is not running, so no watcher is needed now
    NotNeeded,
    /// The watcher cannot be bound, and the loan stays reserved
    Attention(ReleaseWatcherAttentionReason),
    /// The saved watcher identity and a verified checkpoint baseline exist
    Bound {
        /// Saved watcher intent
        intent: ReleaseWatcherIntent,
        /// Executable placed in the canonical watcher command
        executable: std::path::PathBuf,
    },
}

/// Bind the watcher identity to the release action and capture its checkpoint baseline
///
/// The authority allocates the watcher and request identities once and binds
/// them before any launch, so co-located and remote supervisors use one path
async fn bind_release_watcher(
    state: &ResourceActorState,
    resource: &Resource,
    loan: &Loan,
    action_id: ActionId,
    trainer_task_id: TaskId,
) -> Result<WatcherBinding, AppError> {
    let trainer = call(&state.store, |reply| StoreMsg::GetTask {
        id: trainer_task_id,
        reply,
    })
    .await?;
    if trainer.is_none_or(|row| row.status() != ProcessStatus::Running) {
        return Ok(WatcherBinding::NotNeeded);
    }
    let association = call(&state.store, |reply| {
        StoreMsg::TrainerAttemptAssociationForTaskForAuthority {
            authority_machine: state.authority_machine,
            task_id: trainer_task_id,
            reply,
        }
    })
    .await?;
    if !matches!(
        association,
        Ok(Some(ref association)) if association.resource_id() == resource.id
    ) {
        return Ok(WatcherBinding::Attention(
            ReleaseWatcherAttentionReason::TrainerAssociationMissing {
                task_id: trainer_task_id,
            },
        ));
    }
    let Ok(executable) = release_watcher_executable() else {
        return Ok(WatcherBinding::Attention(
            ReleaseWatcherAttentionReason::ExecutableUnavailable,
        ));
    };

    let saved_intent = match &loan.state {
        LoanState::Active {
            phase: LoanPhase::AwaitingRelease { watcher_intent, .. },
        } => watcher_intent.clone(),
        LoanState::Active { .. } | LoanState::NeedsAttention { .. } | LoanState::Closed { .. } => {
            return Ok(WatcherBinding::NotNeeded);
        }
    };
    let intent = match saved_intent {
        Some(SavedReleaseWatcherIntent::Complete(intent)) => intent,
        Some(SavedReleaseWatcherIntent::LegacyUnproven(_)) => {
            return Ok(WatcherBinding::Attention(
                ReleaseWatcherAttentionReason::LegacyWatcherIntent,
            ));
        }
        None => {
            let command = ReleaseWatcherCommand {
                resource_id: resource.id,
                action_id,
                state_revision: resource.state_revision,
                trainer_task_id,
                watcher_task_id: ReleaseWatcherTaskId::new(TaskId::new()),
            };
            let Ok(normalized_spec_sha256) =
                command.normalized_spec_sha256(&executable, resource.supervisor.thread)
            else {
                return Ok(WatcherBinding::Attention(
                    ReleaseWatcherAttentionReason::ExecutableUnavailable,
                ));
            };
            let intent = ReleaseWatcherIntent {
                action_id,
                state_revision: resource.state_revision,
                observed_background_task: trainer_task_id,
                watcher_task_id: command.watcher_task_id,
                request_id: RequestId::new(),
                normalized_spec_sha256,
            };
            let bound = call(&state.store, |reply| {
                StoreMsg::BindReleaseWatcherForAuthority {
                    authority_machine: state.authority_machine,
                    resource_id: resource.id,
                    intent,
                    reply,
                }
            })
            .await?;
            match bound {
                Ok(intent) => intent,
                Err(error) => {
                    tracing::warn!(resource = %resource.id.as_uuid(), action = %action_id.as_uuid(), "bind release watcher: {error}");
                    return Ok(WatcherBinding::Attention(
                        ReleaseWatcherAttentionReason::BindingRejected,
                    ));
                }
            }
        }
    };

    let baseline = call(&state.store, |reply| {
        StoreMsg::CaptureReleaseCheckpointBaselineForAuthority {
            authority_machine: state.authority_machine,
            resource_id: resource.id,
            action_id,
            expected_state_revision: intent.state_revision,
            reply,
        }
    })
    .await?;
    if let Err(error) = baseline {
        tracing::warn!(resource = %resource.id.as_uuid(), action = %action_id.as_uuid(), "capture release checkpoint baseline: {error}");
        return Ok(WatcherBinding::Attention(
            ReleaseWatcherAttentionReason::BaselineUnavailable,
        ));
    }

    Ok(WatcherBinding::Bound { intent, executable })
}

/// Observe a remote supervisor's watcher without launching it from this actor
///
/// No row means the supervisor machine has not launched it. A row that this actor
/// lifetime did not see accepted is observed like any existing acceptance, so a
/// queued row whose spawn may have been lost needs attention
async fn remote_watcher_status(
    state: &ResourceActorState,
    resource: &Resource,
    action_id: ActionId,
    watcher_task_id: TaskId,
) -> Result<ReleaseWatcherStatus, AppError> {
    let row = call(&state.store, |reply| StoreMsg::GetTask {
        id: watcher_task_id,
        reply,
    })
    .await?;
    if row.is_none() {
        return Ok(ReleaseWatcherStatus::AwaitingRemoteSupervisor {
            action_id,
            watcher_task_id,
            supervisor_machine: resource.supervisor.machine,
        });
    }
    watcher_status(
        state,
        WatcherLaunchAttempt {
            action_id,
            watcher_task_id,
            result: ReleaseWatcherLaunchResult::Uncertain,
        },
    )
    .await
}

/// Validate a remote supervisor's action, then return the bound canonical watcher
async fn prepare_remote_watcher(
    state: &ResourceActorState,
    authority: SupervisorActionAuthority,
    observed_background_task: TaskId,
) -> Result<Result<PreparedActionTask, ResourceActionRejection>, AppError> {
    let snapshot = load_snapshot(state).await?;
    let resource = &snapshot.resource;
    if resource.supervisor != authority.supervisor
        || resource.assignment_revision != authority.assignment_revision
        || resource.supervisor.machine == state.authority_machine
    {
        return Ok(Err(ResourceActionRejection::NotCurrentSupervisor));
    }
    if resource.state_revision != authority.expected_state_revision {
        return Ok(Err(ResourceActionRejection::StaleRevision {
            expected: authority.expected_state_revision,
            actual: resource.state_revision,
        }));
    }
    let Some((loan, action_id, trainer_task_id)) = awaiting_release(&snapshot) else {
        return Ok(Err(ResourceActionRejection::ActionNotPending));
    };
    if loan.id != authority.loan_id
        || action_id != authority.action_id
        || trainer_task_id != observed_background_task
    {
        return Ok(Err(ResourceActionRejection::ActionNotPending));
    }

    let unavailable =
        |reason: String| Ok(Err(ResourceActionRejection::WatcherUnavailable { reason }));
    let (intent, executable) =
        match bind_release_watcher(state, resource, &loan, action_id, trainer_task_id).await? {
            WatcherBinding::NotNeeded => {
                return unavailable("the observed background task is not running".into());
            }
            WatcherBinding::Attention(reason) => return unavailable(format!("{reason:?}")),
            WatcherBinding::Bound { intent, executable } => (intent, executable),
        };
    let spec = ReleaseWatcherCommand::from_intent(resource.id, &intent)
        .normalized_spec(&executable, resource.supervisor.thread)?;
    let normalized_spec_sha256 = normalized_spec_sha256(&spec)?;
    // a changed daemon executable cannot silently replace the bound command
    if normalized_spec_sha256 != intent.normalized_spec_sha256 {
        return unavailable("the watcher executable differs from the bound command".into());
    }

    Ok(Ok(PreparedActionTask {
        request_id: intent.request_id,
        task_id: intent.watcher_task_id.as_task_id(),
        spec,
        normalized_spec_sha256,
    }))
}

fn schedule_release_watcher_launch(
    resource_actor: ActorRef<ResourceMsg>,
    supervisor: ActorRef<SupervisorMsg>,
    launch: ReleaseWatcherLaunch,
) {
    let action_id = launch.intent.action_id;
    let watcher_task_id = launch.intent.watcher_task_id.as_task_id();
    tokio::spawn(async move {
        let result = call(&supervisor, |reply| {
            SupervisorMsg::LaunchBoundReleaseWatcher {
                launch: Box::new(launch),
                reply,
            }
        })
        .await;
        let result = match result {
            Ok(Ok(ReleaseWatcherAcceptance::Inserted { task })) if task == watcher_task_id => {
                ReleaseWatcherLaunchResult::Inserted
            }
            Ok(Ok(ReleaseWatcherAcceptance::Existing { task, state }))
                if task == watcher_task_id =>
            {
                ReleaseWatcherLaunchResult::Existing {
                    state: state.status(),
                }
            }
            Ok(Ok(ReleaseWatcherAcceptance::UnsupportedRemoteSupervisor { .. })) => {
                ReleaseWatcherLaunchResult::UnsupportedRemoteSupervisor
            }
            Ok(Err(
                ReleaseWatcherAcceptanceError::Conflict
                | ReleaseWatcherAcceptanceError::Resource(_)
                | ReleaseWatcherAcceptanceError::Identity(_)
                | ReleaseWatcherAcceptanceError::Checkpoint(_),
            )) => ReleaseWatcherLaunchResult::Rejected,
            Ok(Ok(_) | Err(_)) | Err(_) => ReleaseWatcherLaunchResult::Uncertain,
        };
        if let Err(error) = resource_actor.cast(ResourceMsg::WatcherLaunchFinished {
            action_id,
            watcher_task_id,
            result,
        }) {
            tracing::debug!(%watcher_task_id, "release watcher launch result cast: {error}");
        }
    });
}

async fn handle_watcher_launch_finished(
    state: &mut ResourceActorState,
    action_id: ActionId,
    watcher_task_id: TaskId,
    result: ReleaseWatcherLaunchResult,
) -> Result<(), ActorProcessingErr> {
    let Some(attempt) = state.watcher_launch.as_mut() else {
        return Ok(());
    };
    if attempt.action_id != action_id
        || attempt.watcher_task_id != watcher_task_id
        || attempt.result != ReleaseWatcherLaunchResult::Pending
    {
        return Ok(());
    }
    attempt.result = result;
    let attempt = *attempt;
    state.release_watcher = Some(watcher_status(state, attempt).await?);

    Ok(())
}

async fn launch_never_committed(
    state: &ResourceActorState,
    attempt: WatcherLaunchAttempt,
) -> Result<bool, AppError> {
    if attempt.result != ReleaseWatcherLaunchResult::Uncertain {
        return Ok(false);
    }
    let row = call(&state.store, |reply| StoreMsg::GetTask {
        id: attempt.watcher_task_id,
        reply,
    })
    .await?;

    Ok(row.is_none())
}

/// Derive the watcher status from the launch reply and the durable task row
///
/// A queued row is only a launch in progress when this actor's own request inserted
/// it; otherwise nobody can tell whether its worker ever started
async fn watcher_status(
    state: &ResourceActorState,
    attempt: WatcherLaunchAttempt,
) -> Result<ReleaseWatcherStatus, AppError> {
    let WatcherLaunchAttempt {
        action_id,
        watcher_task_id,
        result,
    } = attempt;
    let attention = |reason| ReleaseWatcherStatus::Attention { action_id, reason };
    let inserted = match result {
        ReleaseWatcherLaunchResult::Pending => {
            return Ok(ReleaseWatcherStatus::Launching {
                action_id,
                watcher_task_id,
            });
        }
        ReleaseWatcherLaunchResult::UnsupportedRemoteSupervisor => {
            return Ok(attention(
                ReleaseWatcherAttentionReason::RemoteSupervisorUnsupported {
                    supervisor_machine: state.resource.supervisor.machine,
                },
            ));
        }
        ReleaseWatcherLaunchResult::Rejected => {
            return Ok(attention(ReleaseWatcherAttentionReason::LaunchRejected {
                watcher_task_id,
            }));
        }
        ReleaseWatcherLaunchResult::Inserted => true,
        ReleaseWatcherLaunchResult::Existing { .. } | ReleaseWatcherLaunchResult::Uncertain => {
            false
        }
    };

    let row = call(&state.store, |reply| StoreMsg::GetTask {
        id: watcher_task_id,
        reply,
    })
    .await?;
    let Some(row) = row else {
        return Ok(attention(ReleaseWatcherAttentionReason::LaunchUncertain {
            watcher_task_id,
        }));
    };
    Ok(match row.status() {
        ProcessStatus::Queued if inserted => ReleaseWatcherStatus::Launching {
            action_id,
            watcher_task_id,
        },
        ProcessStatus::Queued => {
            attention(ReleaseWatcherAttentionReason::LaunchUncertain { watcher_task_id })
        }
        ProcessStatus::Running => ReleaseWatcherStatus::Running {
            action_id,
            watcher_task_id,
        },
        ProcessStatus::Succeeded => ReleaseWatcherStatus::Finished {
            action_id,
            watcher_task_id,
        },
        status @ (ProcessStatus::Failed | ProcessStatus::Cancelled | ProcessStatus::Lost) => {
            attention(ReleaseWatcherAttentionReason::WatcherTaskEnded {
                watcher_task_id,
                state: status,
            })
        }
    })
}

fn release_proof_attention_reason(error: CompleteReleaseError) -> ReleaseProofAttentionReason {
    match error {
        CompleteReleaseError::BackgroundTaskNotTerminal { .. } => {
            ReleaseProofAttentionReason::TrainerNotCompleted
        }
        CompleteReleaseError::BackgroundTaskLost { .. } => ReleaseProofAttentionReason::TrainerLost,
        CompleteReleaseError::WorkerExitUnconfirmed { .. } => {
            ReleaseProofAttentionReason::WorkerExitUnconfirmed
        }
        CompleteReleaseError::CompletedResultMissing { .. }
        | CompleteReleaseError::CompletedResultChanged { .. }
        | CompleteReleaseError::CompletedResultRequestMismatch { .. }
        | CompleteReleaseError::Watcher(_) => {
            ReleaseProofAttentionReason::CompletedResultUnavailable
        }
        CompleteReleaseError::StoppedCheckpointChanged { .. } => {
            ReleaseProofAttentionReason::StoppedCheckpointUnavailable
        }
        CompleteReleaseError::OwnershipLockStillHeld { .. }
        | CompleteReleaseError::OwnershipLock(_) => {
            ReleaseProofAttentionReason::OwnershipLockUnverified
        }
        // no saved lock names the worker, so only an operator can resolve it
        CompleteReleaseError::TrainerAssociationMissing { .. } => {
            ReleaseProofAttentionReason::TrainerAssociationMissing
        }
        CompleteReleaseError::Resource(_)
        | CompleteReleaseError::Notice(_)
        | CompleteReleaseError::TrainerAssociation(_)
        | CompleteReleaseError::Identity(_)
        | CompleteReleaseError::TaskStorage(_)
        | CompleteReleaseError::CommandShape(_)
        | CompleteReleaseError::ActionNotFound { .. }
        | CompleteReleaseError::NotAwaitingRelease { .. }
        | CompleteReleaseError::InvalidReleaseNotice { .. }
        | CompleteReleaseError::StaleRevision { .. }
        | CompleteReleaseError::BackgroundTaskMissing { .. }
        | CompleteReleaseError::TrainerAssociationMismatch { .. }
        | CompleteReleaseError::TrainerIdentityMissing { .. }
        | CompleteReleaseError::TrainerIdentityChanged { .. }
        | CompleteReleaseError::StoppedProofUnavailable { .. }
        | CompleteReleaseError::TrainerCancellationMarkerChanged { .. }
        | CompleteReleaseError::Checkpoint(_)
        | CompleteReleaseError::TaskStateChanged { .. }
        | CompleteReleaseError::TaskCommandChanged { .. }
        | CompleteReleaseError::RevisionExhausted { .. }
        | CompleteReleaseError::ConflictingRetry { .. }
        | CompleteReleaseError::RequestChanged { .. }
        | CompleteReleaseError::LoanChanged { .. }
        | CompleteReleaseError::ResourceChanged
        | CompleteReleaseError::Storage(_) => ReleaseProofAttentionReason::SavedEvidenceMismatch,
    }
}

fn task_completion_attention_reason(
    task_id: TaskId,
    attention: AssignedResourceTaskAttention,
) -> ResourceQueueAttentionReason {
    match attention {
        AssignedResourceTaskAttention::RequestChanged
        | AssignedResourceTaskAttention::LoanChanged
        | AssignedResourceTaskAttention::TaskIdentityMismatch => {
            ResourceQueueAttentionReason::AssignedTaskIdentityMismatch { task_id }
        }
        AssignedResourceTaskAttention::StaleRevision => {
            ResourceQueueAttentionReason::AssignedTaskStaleRevision { task_id }
        }
        AssignedResourceTaskAttention::ServingReleaseUnverified => {
            ResourceQueueAttentionReason::UnverifiedServingRelease
        }
        AssignedResourceTaskAttention::TaskLost => {
            ResourceQueueAttentionReason::AssignedTaskLost { task_id }
        }
        AssignedResourceTaskAttention::ProcessGroupExitUnconfirmed => {
            ResourceQueueAttentionReason::AssignedTaskExitUnconfirmed { task_id }
        }
        AssignedResourceTaskAttention::InvalidNoChildSpawnEvidence => {
            ResourceQueueAttentionReason::AssignedTaskNoChildSpawnProofInvalid { task_id }
        }
        AssignedResourceTaskAttention::OwnershipUncertain(risk) => {
            ResourceQueueAttentionReason::AssignedTaskOwnershipUncertain { task_id, risk }
        }
    }
}

async fn serving_task_id(state: &ResourceActorState) -> Result<Option<TaskId>, AppError> {
    let Some(loan) = state.loan.as_ref() else {
        return Ok(None);
    };
    let (request_id, loan_id) = match &loan.state {
        LoanState::Active {
            phase: LoanPhase::Serving {
                current_request_id, ..
            },
        } => (*current_request_id, loan.id),
        LoanState::Active { .. } | LoanState::NeedsAttention { .. } | LoanState::Closed { .. } => {
            return Ok(None);
        }
    };
    let requests = call(&state.store, |reply| StoreMsg::ResourceRequests {
        authority_machine: state.authority_machine,
        resource_id: state.resource.id,
        reply,
    })
    .await?;
    Ok(requests
        .into_iter()
        .find(|request| {
            request.request_id == request_id
                && matches!(
                    &request.state,
                    ResourceRequestState::Assigned { loan_id: assigned_loan }
                        if *assigned_loan == loan_id
                )
        })
        .map(|request| request.task_id))
}

/// Stable actor name for one resource identity
#[must_use]
pub(crate) fn resource_actor_name(id: ResourceId) -> String {
    format!("homebased.resource.{}", id.as_uuid())
}

/// Parse one resource actor name into its stable resource identity
#[must_use]
pub(crate) fn resource_id_from_actor_name(name: Option<String>) -> Option<ResourceId> {
    let name = name?;
    let uuid = name.strip_prefix("homebased.resource.")?.parse().ok()?;
    Some(ResourceId::from_uuid(uuid))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::daemon::actors::{StoreActor, call};
    use crate::domain::{
        ExitReason, ProcessStatus, TaskEnv, TaskId, TaskWorkload, ThreadId, Workload,
    };
    use crate::home::Home;
    use crate::machine::load_or_create_machine_id;
    use crate::resource::{
        AssignmentRevision, LoanPhase, LoanState, ResourceQueueAttentionReason,
        ResourceQueueReconcileOutcome, ResourceRequestState, ResourceRevision, ReturnContext,
        SupervisorAddress,
    };
    use crate::spec::NormalizedSpec;
    use crate::store::{NewTask, Store, new_queued_task};
    use crate::submission::RequestId;

    fn resource(authority: MachineId, background_task: Option<TaskId>) -> Resource {
        Resource::new(
            ResourceId::new(),
            "gpu-test".into(),
            authority,
            SupervisorAddress {
                machine: authority,
                thread: ThreadId(Uuid::now_v7()),
            },
            AssignmentRevision::new(0),
            ResourceRevision::new(0),
            background_task,
        )
    }

    fn command_spec() -> NormalizedSpec {
        serde_json::from_value(json!({
            "api_version": 1,
            "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
            "name": "resource reconcile test",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["/bin/echo", "hello"] }
        }))
        .unwrap()
    }

    fn seed_queue(
        home: &Home,
        task_status: Option<ProcessStatus>,
    ) -> (MachineId, Resource, RequestId) {
        let authority = load_or_create_machine_id(home).unwrap();
        let background_task = task_status.map(|_| TaskId::new());
        let resource = resource(authority, background_task);
        let mut store = Store::open(&home.db_path()).unwrap();
        store.register_resource(authority, &resource).unwrap();

        if let (Some(task_id), Some(status)) = (background_task, task_status) {
            let spec = command_spec();
            let crate::spec::NormalizedWorkload::Task(workload) = spec.workload.clone() else {
                panic!("resource test must use a command workload");
            };
            let row = new_queued_task(NewTask {
                id: task_id,
                name: Some(spec.name.clone()),
                thread: spec.thread,
                workload: Workload::Task(TaskWorkload {
                    command: workload.command,
                }),
                cwd: spec.cwd.clone(),
                timeout: spec.timeout,
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                binary: PathBuf::from("/bin/echo"),
            });
            store.insert_task(&row).unwrap();
            match status {
                ProcessStatus::Queued => {}
                ProcessStatus::Running => {
                    store
                        .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
                        .unwrap()
                        .unwrap();
                }
                ProcessStatus::Lost => {
                    store
                        .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Lost)
                        .unwrap()
                        .unwrap();
                }
                other => {
                    let reason = match other {
                        ProcessStatus::Succeeded | ProcessStatus::Failed => {
                            ExitReason::Exit { code: 0 }
                        }
                        ProcessStatus::Cancelled => ExitReason::Cancelled,
                        ProcessStatus::Queued | ProcessStatus::Running | ProcessStatus::Lost => {
                            unreachable!()
                        }
                    };
                    store
                        .cas_exit(task_id, ProcessStatus::Queued, &reason)
                        .unwrap()
                        .unwrap();
                }
            }
        }

        let request_id = RequestId::new();
        store
            .accept_resource_request(
                authority,
                request_id,
                TaskId::new(),
                resource.id,
                authority,
                command_spec(),
            )
            .unwrap();
        drop(store);
        (authority, resource, request_id)
    }

    fn seed_serving_task(home: &Home) -> (MachineId, Resource, RequestId, TaskId) {
        let authority = load_or_create_machine_id(home).unwrap();
        let spec = command_spec();
        let background_task = TaskId::new();
        let request_id = RequestId::new();
        let task_id = TaskId::new();
        let origin = MachineId::new();
        let mut resource = resource(authority, Some(background_task));
        resource.supervisor.thread = spec.thread;
        let mut store = Store::open(&home.db_path()).unwrap();
        store.register_resource(authority, &resource).unwrap();

        let crate::spec::NormalizedWorkload::Task(workload) = spec.workload.clone() else {
            panic!("resource task fixture must use a command workload");
        };
        store
            .insert_task(&new_queued_task(NewTask {
                id: background_task,
                name: Some(spec.name.clone()),
                thread: spec.thread,
                workload: Workload::Task(TaskWorkload {
                    command: workload.command,
                }),
                cwd: spec.cwd.clone(),
                timeout: spec.timeout,
                env: TaskEnv {
                    path: "/bin".into(),
                    home: "/tmp".into(),
                },
                binary: PathBuf::from("/bin/echo"),
            }))
            .unwrap();
        store
            .cas_status(
                background_task,
                ProcessStatus::Queued,
                ProcessStatus::Running,
            )
            .unwrap()
            .unwrap();
        let request = store
            .accept_resource_request(
                authority,
                request_id,
                task_id,
                resource.id,
                origin,
                spec.clone(),
            )
            .unwrap();
        assert!(matches!(
            store
                .open_release_loan_for_authority(authority, resource.id, resource.state_revision,)
                .unwrap(),
            crate::resource::store::OpenReleaseLoanResult::Opened { .. }
        ));
        store
            .cas_exit(
                background_task,
                ProcessStatus::Running,
                &ExitReason::Exit { code: 0 },
            )
            .unwrap()
            .unwrap();
        let (loan, state_revision) = store
            .seed_verified_serving_loan_for_test(
                authority,
                resource.id,
                request_id,
                ReturnContext::AlreadyCompleted {
                    task_id: background_task,
                    result_ref: "actor fixture".into(),
                },
            )
            .unwrap();
        assert_eq!(request.task_id, task_id);
        assert!(matches!(
            loan.state,
            LoanState::Active {
                phase: LoanPhase::Serving { current_request_id, .. }
            } if current_request_id == request_id
        ));
        assert!(matches!(
            store
                .accept_assigned_resource_task(ResourceTaskAcceptanceInput {
                    authority_machine: authority,
                    resource_id: resource.id,
                    request_id,
                    task_id,
                    acceptance_sequence: request.acceptance_sequence,
                    loan_id: loan.id,
                    expected_state_revision: state_revision,
                    command_spec: crate::resource::CommandSpec::try_from(spec).unwrap(),
                    executor_env: TaskEnv {
                        path: "/bin".into(),
                        home: "/tmp".into(),
                    },
                })
                .unwrap(),
            ResourceTaskAcceptance::Inserted { task } if task == task_id
        ));
        store
            .cas_status(task_id, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();

        (authority, resource, request_id, task_id)
    }

    async fn start_resource_actor(
        home: &Home,
        authority: MachineId,
        resource: Resource,
    ) -> (
        ActorRef<ResourceMsg>,
        ractor::concurrency::JoinHandle<()>,
        ActorRef<StoreMsg>,
        ractor::concurrency::JoinHandle<()>,
    ) {
        let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
            .await
            .unwrap();
        let snapshot = call(&store, |reply| StoreMsg::ResourceSnapshotsForAuthority {
            authority_machine: authority,
            reply,
        })
        .await
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.resource.id == resource.id)
        .unwrap();
        let (actor, actor_handle) = ResourceActor::spawn(
            None,
            ResourceActor,
            (
                store.clone(),
                None,
                authority,
                snapshot.resource,
                snapshot.loan,
            ),
        )
        .await
        .unwrap();
        (actor, actor_handle, store, store_handle)
    }

    async fn stop_actor(actor: ActorRef<ResourceMsg>, handle: ractor::concurrency::JoinHandle<()>) {
        actor.stop(None);
        let _ = handle.await;
    }

    async fn stop_store(store: ActorRef<StoreMsg>, handle: ractor::concurrency::JoinHandle<()>) {
        store.stop(None);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn exact_terminal_wake_and_actor_restart_finish_one_assigned_task() {
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let (authority, resource, request_id, task_id) = seed_serving_task(&home);

        let (actor, actor_handle, store, store_handle) =
            start_resource_actor(&home, authority, resource.clone()).await;
        let before_exit = call(&actor, |reply| ResourceMsg::Inspect { reply })
            .await
            .unwrap();
        assert!(matches!(
            before_exit.loan,
            Some(crate::resource::Loan {
                state: LoanState::Active {
                    phase: LoanPhase::Serving { current_request_id: current, .. }
                },
                ..
            }) if current == request_id
        ));

        let task_store = Store::open(&home.db_path()).unwrap();
        task_store
            .cas_exit_with_evidence(
                task_id,
                ProcessStatus::Running,
                &ExitReason::Cancelled,
                crate::domain::ProcessGroupExitEvidence::ConfirmedExited,
            )
            .unwrap()
            .unwrap();
        drop(task_store);

        actor
            .cast(ResourceMsg::TaskTerminal {
                task_id: TaskId::new(),
            })
            .unwrap();
        actor.cast(ResourceMsg::TaskTerminal { task_id }).unwrap();
        let after_exit = call(&actor, |reply| ResourceMsg::Inspect { reply })
            .await
            .unwrap();
        assert!(matches!(
            after_exit.loan,
            Some(crate::resource::Loan {
                state: LoanState::Active {
                    phase: LoanPhase::AwaitingReturn { .. }
                },
                ..
            })
        ));
        assert!(matches!(
            call(&store, |reply| StoreMsg::ResourceRequests {
                authority_machine: authority,
                resource_id: resource.id,
                reply,
            })
            .await
            .unwrap()[0]
                .state,
            ResourceRequestState::Finished {
                outcome: ExitReason::Cancelled
            }
        ));
        let notices = call(&store, |reply| StoreMsg::PendingSupervisorNotices { reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            notices
                .iter()
                .filter(|notice| matches!(
                    notice.payload,
                    crate::resource::SupervisorNoticePayload::ReturnRequired { .. }
                ))
                .count(),
            1
        );
        let committed_revision = after_exit.resource.state_revision;

        stop_actor(actor, actor_handle).await;
        stop_store(store, store_handle).await;

        let (restarted, restarted_handle, reopened_store, reopened_store_handle) =
            start_resource_actor(&home, authority, resource.clone()).await;
        let after_restart = call(&restarted, |reply| ResourceMsg::Inspect { reply })
            .await
            .unwrap();
        assert_eq!(after_restart.resource.state_revision, committed_revision);
        let notices = call(&reopened_store, |reply| {
            StoreMsg::PendingSupervisorNotices { reply }
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            notices
                .iter()
                .filter(|notice| matches!(
                    notice.payload,
                    crate::resource::SupervisorNoticePayload::ReturnRequired { .. }
                ))
                .count(),
            1
        );

        stop_actor(restarted, restarted_handle).await;
        stop_store(reopened_store, reopened_store_handle).await;
    }

    #[tokio::test]
    async fn actor_startup_recovers_a_queued_request_into_one_release_action() {
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let (authority, resource, request_id) = seed_queue(&home, Some(ProcessStatus::Running));

        let (actor, actor_handle, store, store_handle) =
            start_resource_actor(&home, authority, resource.clone()).await;
        let inspection = call(&actor, |reply| ResourceMsg::Inspect { reply })
            .await
            .unwrap();
        let ResourceQueueReconcileOutcome::ReleaseProofUnavailable { loan, .. } =
            inspection.reconcile_outcome.unwrap()
        else {
            panic!("startup must retain the running task behind the release proof gate");
        };
        assert!(matches!(
            &loan.state,
            crate::resource::LoanState::Active {
                phase: crate::resource::LoanPhase::AwaitingRelease {
                    observed_background_task,
                    ..
                }
            } if Some(*observed_background_task) == resource.registered_background_task
        ));
        assert_eq!(inspection.loan, Some(loan));
        assert_eq!(inspection.resource.state_revision, ResourceRevision::new(1));
        assert_eq!(
            call(&store, |reply| StoreMsg::ResourceRequests {
                authority_machine: authority,
                resource_id: resource.id,
                reply,
            })
            .await
            .unwrap()[0]
                .request_id,
            request_id
        );

        stop_actor(actor, actor_handle).await;
        stop_store(store, store_handle).await;
    }

    #[tokio::test]
    async fn duplicate_wakes_keep_one_loan_and_one_notice() {
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let (authority, resource, _) = seed_queue(&home, Some(ProcessStatus::Running));

        let (actor, actor_handle, store, store_handle) =
            start_resource_actor(&home, authority, resource.clone()).await;
        let first = call(&actor, |reply| ResourceMsg::Inspect { reply })
            .await
            .unwrap();
        let first_loan = first.loan.unwrap();
        let first_loan_id = first_loan.id;

        for _ in 0..2 {
            let result = call(&actor, |reply| ResourceMsg::Reconcile { reply })
                .await
                .unwrap();
            assert!(matches!(
                result,
                ResourceQueueReconcileOutcome::ReleaseProofUnavailable { loan, .. }
                    if loan.id == first_loan_id
            ));
        }

        let notices = call(&store, |reply| StoreMsg::PendingSupervisorNotices { reply })
            .await
            .unwrap();
        let notices = notices.unwrap();
        assert_eq!(notices.len(), 1);
        let snapshot = call(&actor, |reply| ResourceMsg::Inspect { reply })
            .await
            .unwrap();
        assert_eq!(snapshot.loan.unwrap().id, first_loan_id);

        stop_actor(actor, actor_handle).await;
        stop_store(store, store_handle).await;
    }

    #[tokio::test]
    async fn uncertain_background_state_stays_queued_and_is_inspectable() {
        let directory = tempdir().unwrap();
        let home = Home::resolve(Some(directory.path().to_path_buf())).unwrap();
        home.ensure().unwrap();
        let (authority, resource, request_id) = seed_queue(&home, Some(ProcessStatus::Lost));

        let (actor, actor_handle, store, store_handle) =
            start_resource_actor(&home, authority, resource.clone()).await;
        let inspection = call(&actor, |reply| ResourceMsg::Inspect { reply })
            .await
            .unwrap();
        assert!(inspection.loan.is_none());
        assert!(matches!(
            inspection.reconcile_outcome,
            Some(ResourceQueueReconcileOutcome::AttentionRequired {
                request,
                reason: ResourceQueueAttentionReason::BackgroundTaskNotRunning {
                    state,
                    ..
                },
            }) if request.request_id == request_id && state == "lost"
        ));
        let requests = call(&store, |reply| StoreMsg::ResourceRequests {
            authority_machine: authority,
            resource_id: resource.id,
            reply,
        })
        .await
        .unwrap();
        assert!(matches!(requests[0].state, ResourceRequestState::Queued));
        assert!(
            call(&store, |reply| StoreMsg::OldestQueuedResourceRequest {
                authority_machine: authority,
                resource_id: resource.id,
                reply,
            })
            .await
            .unwrap()
            .is_some()
        );

        stop_actor(actor, actor_handle).await;
        stop_store(store, store_handle).await;
    }

    #[test]
    fn resource_ids_round_trip_in_actor_names() {
        let id = ResourceId::new();
        assert_eq!(
            resource_id_from_actor_name(Some(resource_actor_name(id))),
            Some(id)
        );
        assert!(resource_id_from_actor_name(Some("homebased.resource.invalid".into())).is_none());
    }
}
