//! Authority-owned queue reconciliation for one resource

use chrono::{DateTime, Utc};
use ractor::{Actor, ActorId, ActorProcessingErr, ActorRef, RpcReplyPort};
use tokio::task::AbortHandle;

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
    ResourceTaskCompletionResult, ReturnDeadlineOutcome,
};
use crate::resource::{
    ActionId, Loan, LoanPhase, LoanState, ReleaseProofAttentionReason, ReleaseWatcherIntent,
    ReleaseWatcherTaskId, Resource, ResourceId, ResourceQueueAttentionReason,
    ResourceQueueReconcileOutcome, ResourceRequest, ResourceRequestState, RestoreAttentionReason,
    ReturnDecisionWindow, SupervisorActionAuthority,
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
        request_id: RequestId,
        /// Exact preallocated task sent to the supervisor
        task_id: TaskId,
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
    supervisor: ActorRef<SupervisorMsg>,
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
    return_deadline_wake: Option<ReturnDeadlineWake>,
}

/// Wake armed for the saved deadline of one pending return action
///
/// The store decides from its saved window whether the deadline passed, so a
/// wake that fires early, or before a hold moved the deadline, only arms again
struct ReturnDeadlineWake {
    action_id: ActionId,
    deadline_at: DateTime<Utc>,
    wake: AbortHandle,
}

impl Drop for ReturnDeadlineWake {
    fn drop(&mut self) {
        self.wake.abort();
    }
}

// each phase launches through a different supervisor path and can outlive the
// others, so each keeps its own slot; a slot holds only this actor lifetime's
// request, so a queued row that this lifetime did not insert may have lost its
// spawn and is only observed, never respawned

// one supervisor launch request per first background request and actor lifetime
type BackgroundLaunchAttempt = LaunchAttempt<RequestId, BackgroundLaunchResult>;

// one supervisor launch request per return task and actor lifetime
type RestoreLaunchAttempt = LaunchAttempt<RestoreLaunchKey, RestoreLaunchResult>;

// one launch request per watcher identity and actor lifetime; a new actor after a
// restart asks again with the same saved identity and only observes what it finds
type WatcherLaunchAttempt = LaunchAttempt<WatcherLaunchKey, ReleaseWatcherLaunchResult>;

// one supervisor launch request per assigned task and actor lifetime
type ActivationAttempt = LaunchAttempt<ActivationKey, ResourceTaskActivationResult>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestoreLaunchKey {
    action_id: ActionId,
    task_id: TaskId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WatcherLaunchKey {
    action_id: ActionId,
    watcher_task_id: TaskId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActivationKey {
    request_id: RequestId,
    task_id: TaskId,
}

/// One supervisor launch request made by this actor lifetime
#[derive(Debug, Clone, Copy)]
struct LaunchAttempt<K, R> {
    key: K,
    progress: LaunchProgress<R>,
}

/// Whether a launch request has replied, and with which result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaunchProgress<R> {
    Pending,
    Finished(R),
}

impl<K: Copy + PartialEq, R: Copy> LaunchAttempt<K, R> {
    fn pending(key: K) -> Self {
        Self {
            key,
            progress: LaunchProgress::Pending,
        }
    }

    /// Record the reply for this exact pending request
    ///
    /// A reply for another identity, or a second reply, is stale and changes nothing
    fn finish(&mut self, key: K, result: R) -> bool {
        if self.key != key || !matches!(self.progress, LaunchProgress::Pending) {
            return false;
        }
        self.progress = LaunchProgress::Finished(result);
        true
    }

    /// Whether this request is still in flight or inserted the launch itself
    fn launching(&self, key: K, inserted: impl FnOnce(R) -> bool) -> bool {
        self.key == key
            && match self.progress {
                LaunchProgress::Pending => true,
                LaunchProgress::Finished(result) => inserted(result),
            }
    }
}

/// Actor that reconciles one resource from StoreActor-owned state
pub(crate) struct ResourceActor;

impl Actor for ResourceActor {
    type Msg = ResourceMsg;
    type State = ResourceActorState;
    type Arguments = (
        ActorRef<StoreMsg>,
        ActorRef<SupervisorMsg>,
        MachineId,
        Resource,
        Option<Loan>,
    );

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
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
            return_deadline_wake: None,
        };
        reconcile_and_refresh(&myself, &mut state).await?;

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
                        .is_some_and(|attempt| attempt.key.watcher_task_id == task_id)
                {
                    reconcile_and_refresh(&myself, state).await?;
                }
            }
            ResourceMsg::RestoreLaunchStarted { action_id, task_id } => {
                state.restore_launch = Some(RestoreLaunchAttempt::pending(RestoreLaunchKey {
                    action_id,
                    task_id,
                }));
            }
            ResourceMsg::RestoreLaunchFinished {
                action_id,
                task_id,
                result,
            } => {
                if let Some(attempt) = state.restore_launch.as_mut() {
                    attempt.finish(RestoreLaunchKey { action_id, task_id }, result);
                }
                reconcile_and_refresh(&myself, state).await?;
            }
            ResourceMsg::BackgroundLaunchStarted { request_id } => {
                state.background_launch = Some(BackgroundLaunchAttempt::pending(request_id));
            }
            ResourceMsg::BackgroundLaunchFinished { request_id, result } => {
                if let Some(attempt) = state.background_launch.as_mut() {
                    attempt.finish(request_id, result);
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
                state.watcher_launch = Some(WatcherLaunchAttempt::pending(WatcherLaunchKey {
                    action_id,
                    watcher_task_id,
                }));
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
    let queue_outcome = call(&state.store, |reply| StoreMsg::ReconcileResourceQueue {
        authority_machine: state.authority_machine,
        resource_id: state.resource.id,
        reply,
    })
    .await?;

    let mut snapshot = load_snapshot(state).await?;
    let release = complete_release(state, &mut snapshot).await?;
    state.release_watcher = match &release {
        ReleaseProgress::ProofUnavailable(failure) => {
            let resource = snapshot.resource.clone();
            progress_release_watcher(myself, state, &resource, failure).await?
        }
        ReleaseProgress::NotAwaiting | ReleaseProgress::Completed { .. } => {
            state.watcher_launch = None;
            None
        }
    };

    let mut outcome = match release {
        // the loan is still awaiting release, so no request can be serving
        ReleaseProgress::ProofUnavailable(failure) => {
            state.activation_attempt = None;
            failure.into_outcome()
        }
        ReleaseProgress::NotAwaiting => {
            let serving = reconcile_serving_task(myself, state, &mut snapshot).await?;
            serving_outcome(serving).unwrap_or(queue_outcome)
        }
        ReleaseProgress::Completed { loan } => {
            let serving = reconcile_serving_task(myself, state, &mut snapshot).await?;
            serving_outcome(serving)
                .unwrap_or(ResourceQueueReconcileOutcome::LoanAlreadyActive { loan })
        }
    };

    if let Some(restore_outcome) = reconcile_restore(myself, state, &mut snapshot).await? {
        outcome = restore_outcome;
    }

    if let Some(launch_outcome) = observe_background_launch(state, &snapshot, &outcome).await? {
        outcome = launch_outcome;
    }

    if let Some(deadline_outcome) = reconcile_return_deadline(myself, state, &mut snapshot).await? {
        outcome = deadline_outcome;
    }

    state.resource = snapshot.resource;
    state.loan = snapshot.loan;
    state.reconcile_outcome = Some(outcome.clone());

    Ok(outcome)
}

/// Result of trying to prove the release of a loan that awaits one
enum ReleaseProgress {
    /// The loan does not await a release
    NotAwaiting,
    /// The store committed the release, and this loan replaced the awaiting one
    Completed { loan: Loan },
    /// The release proof is not available yet, so the loan stays reserved
    ProofUnavailable(ReleaseProofFailure),
}

/// Awaiting-release loan whose release proof the store refused
struct ReleaseProofFailure {
    loan: Loan,
    action_id: ActionId,
    trainer_task_id: TaskId,
    reason: ReleaseProofAttentionReason,
}

impl ReleaseProofFailure {
    fn into_outcome(self) -> ResourceQueueReconcileOutcome {
        ResourceQueueReconcileOutcome::ReleaseProofUnavailable {
            loan: self.loan,
            action_id: self.action_id,
            task_id: self.trainer_task_id,
            reason: self.reason,
        }
    }
}

/// Complete the pending release action and reload the snapshot when it commits
async fn complete_release(
    state: &ResourceActorState,
    snapshot: &mut ResourceSnapshot,
) -> Result<ReleaseProgress, AppError> {
    let Some((loan, action_id, trainer_task_id)) = awaiting_release(snapshot) else {
        return Ok(ReleaseProgress::NotAwaiting);
    };
    let completion = call(&state.store, |reply| {
        StoreMsg::CompleteReleaseForAuthority {
            authority_machine: state.authority_machine,
            resource_id: snapshot.resource.id,
            action_id,
            expected_state_revision: snapshot.resource.state_revision,
            reply,
        }
    })
    .await?;
    match completion {
        Ok(
            ReleaseCompletionResult::Assigned { loan, .. }
            | ReleaseCompletionResult::ReturnRequired { loan, .. },
        ) => {
            *snapshot = load_snapshot(state).await?;
            Ok(ReleaseProgress::Completed { loan })
        }
        Err(error) => Ok(ReleaseProgress::ProofUnavailable(ReleaseProofFailure {
            loan,
            action_id,
            trainer_task_id,
            reason: release_proof_attention_reason(error),
        })),
    }
}

/// Progress of the task assigned to the current serving loan
enum ServingProgress {
    /// No request is assigned to a serving loan
    Unassigned,
    /// The assigned task ended, and the store committed its completion into this loan
    Completed { loan: Loan },
    /// The task layer accepted the assigned task
    Accepted {
        loan: Loan,
        // boxed because this is the common variant and both records are large
        request: Box<ResourceRequest>,
        progress: AssignedResourceTaskProgress,
        // whether this actor lifetime's own launch request inserted the task
        inserted_by_this_actor: bool,
    },
    /// This actor just asked the supervisor to launch the assigned task
    LaunchRequested { loan: Loan },
    /// The task is not accepted, and this actor lifetime already asked to launch it
    LaunchAttempted {
        request: ResourceRequest,
        progress: LaunchProgress<ResourceTaskActivationResult>,
    },
    /// The assigned task needs attention
    Attention {
        request: ResourceRequest,
        reason: ResourceQueueAttentionReason,
    },
}

/// Reconcile the assigned task of the serving loan, and launch it once when needed
async fn reconcile_serving_task(
    myself: &ActorRef<ResourceMsg>,
    state: &mut ResourceActorState,
    snapshot: &mut ResourceSnapshot,
) -> Result<ServingProgress, AppError> {
    let requests = call(&state.store, |reply| StoreMsg::ResourceRequests {
        authority_machine: state.authority_machine,
        resource_id: snapshot.resource.id,
        reply,
    })
    .await?;
    let serving = serving_assignment(snapshot, &requests);
    let current_key = serving.as_ref().map(|(_, request)| ActivationKey {
        request_id: request.request_id,
        task_id: request.task_id,
    });
    state.activation_attempt = state
        .activation_attempt
        .filter(|attempt| Some(attempt.key) == current_key);
    let Some((loan, request)) = serving else {
        return Ok(ServingProgress::Unassigned);
    };

    let input = AssignedResourceTaskReconcileInput {
        authority_machine: state.authority_machine,
        resource_id: request.resource_id,
        loan_id: loan.id,
        request_id: request.request_id,
        task_id: request.task_id,
        expected_state_revision: snapshot.resource.state_revision,
    };
    let reconciled = call(&state.store, |reply| {
        StoreMsg::AssignedResourceTaskReconcile { input, reply }
    })
    .await?;
    let task_id = request.task_id;
    match reconciled {
        Err(error) => {
            tracing::warn!(%task_id, "assigned resource task reconciliation failed: {error}");
            Ok(ServingProgress::Attention {
                request,
                reason: ResourceQueueAttentionReason::AssignedTaskReconcileFailed { task_id },
            })
        }
        Ok(AssignedResourceTaskReconcileOutcome::Attention(reason)) => {
            Ok(ServingProgress::Attention {
                request,
                reason: task_completion_attention_reason(task_id, reason),
            })
        }
        Ok(AssignedResourceTaskReconcileOutcome::Active(progress)) => {
            let inserted_by_this_actor = state.activation_attempt.is_some_and(|attempt| {
                attempt.progress == LaunchProgress::Finished(ResourceTaskActivationResult::Inserted)
            });
            Ok(ServingProgress::Accepted {
                loan,
                request: Box::new(request),
                progress,
                inserted_by_this_actor,
            })
        }
        Ok(AssignedResourceTaskReconcileOutcome::Completed(result)) => {
            let next_assignment = matches!(*result, ResourceTaskCompletionResult::Assigned { .. });
            *snapshot = load_snapshot(state).await?;
            let Some(loan) = snapshot.loan.clone() else {
                return Err(AppError::Internal {
                    message: "resource task completion removed its active loan".into(),
                });
            };
            if next_assignment {
                myself.cast(ResourceMsg::Wake)?;
            }
            Ok(ServingProgress::Completed { loan })
        }
        Ok(AssignedResourceTaskReconcileOutcome::NotAccepted) => {
            Ok(launch_assigned_task(myself, state, snapshot, loan, request))
        }
    }
}

/// Ask the supervisor once per actor lifetime to launch the assigned task
///
/// The supervisor is called from a detached task because it may be waiting on
/// this actor
fn launch_assigned_task(
    myself: &ActorRef<ResourceMsg>,
    state: &mut ResourceActorState,
    snapshot: &ResourceSnapshot,
    loan: Loan,
    request: ResourceRequest,
) -> ServingProgress {
    if let Some(attempt) = state.activation_attempt {
        return ServingProgress::LaunchAttempted {
            request,
            progress: attempt.progress,
        };
    }
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
    state.activation_attempt = Some(ActivationAttempt::pending(ActivationKey {
        request_id: request.request_id,
        task_id: request.task_id,
    }));
    schedule_assigned_activation(myself.clone(), state.supervisor.clone(), input);

    ServingProgress::LaunchRequested { loan }
}

/// Queue outcome for serving progress, or `None` to keep the earlier outcome
fn serving_outcome(progress: ServingProgress) -> Option<ResourceQueueReconcileOutcome> {
    let launch_uncertain = |request: ResourceRequest, reason| {
        Some(ResourceQueueReconcileOutcome::AttentionRequired { request, reason })
    };
    match progress {
        ServingProgress::Unassigned => None,
        ServingProgress::Completed { loan } | ServingProgress::LaunchRequested { loan } => {
            Some(ResourceQueueReconcileOutcome::LoanAlreadyActive { loan })
        }
        ServingProgress::Attention { request, reason } => {
            Some(ResourceQueueReconcileOutcome::AttentionRequired { request, reason })
        }
        // only this actor's own inserted launch may still be starting; an existing
        // queued row from before is never respawned here
        ServingProgress::Accepted {
            request,
            progress: AssignedResourceTaskProgress::Queued,
            inserted_by_this_actor: false,
            ..
        } => {
            let task_id = request.task_id;
            launch_uncertain(
                *request,
                ResourceQueueAttentionReason::AcceptedTaskLaunchUncertain { task_id },
            )
        }
        ServingProgress::Accepted { loan, .. } => {
            Some(ResourceQueueReconcileOutcome::LoanAlreadyActive { loan })
        }
        ServingProgress::LaunchAttempted {
            request,
            progress:
                LaunchProgress::Pending
                | LaunchProgress::Finished(
                    ResourceTaskActivationResult::Prevented
                    | ResourceTaskActivationResult::Uncertain
                    | ResourceTaskActivationResult::Existing {
                        state: ProcessStatus::Queued,
                    },
                ),
        } => {
            let task_id = request.task_id;
            launch_uncertain(
                request,
                ResourceQueueAttentionReason::AssignedTaskLaunchUncertain { task_id },
            )
        }
        ServingProgress::LaunchAttempted { .. } => None,
    }
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
        .is_some_and(|attempt| Some(attempt.key.action_id) != pending_action)
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
            let key = RestoreLaunchKey { action_id, task_id };
            let launching = state.restore_launch.is_some_and(|attempt| {
                attempt.launching(key, |result| result == RestoreLaunchResult::Inserted)
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

/// Serve queued work from an undecided return once its window closes
///
/// While the window is open, one wake is armed for its saved deadline. The
/// store serves the next queued request from the same loan only after the
/// deadline, so the supervisor's decision and this transition cannot both win
async fn reconcile_return_deadline(
    myself: &ActorRef<ResourceMsg>,
    state: &mut ResourceActorState,
    snapshot: &mut ResourceSnapshot,
) -> Result<Option<ResourceQueueReconcileOutcome>, AppError> {
    if awaiting_return_action(snapshot).is_none() {
        state.return_deadline_wake = None;
        return Ok(None);
    }
    let served = call(&state.store, |reply| StoreMsg::ServeAfterReturnDeadline {
        authority_machine: state.authority_machine,
        resource_id: snapshot.resource.id,
        reply,
    })
    .await?;
    match served {
        Err(error) => {
            let resource = snapshot.resource.id.as_uuid();
            tracing::warn!(%resource, "return deadline check failed: {error}");
            Ok(None)
        }
        Ok(ReturnDeadlineOutcome::Open { window }) => {
            arm_return_deadline_wake(myself, state, &window);
            Ok(None)
        }
        Ok(ReturnDeadlineOutcome::NotAwaiting | ReturnDeadlineOutcome::NoQueuedRequest) => {
            // a request accepted later reconciles this actor again
            state.return_deadline_wake = None;
            Ok(None)
        }
        Ok(ReturnDeadlineOutcome::Served { loan, request }) => {
            state.return_deadline_wake = None;
            let resource = snapshot.resource.id.as_uuid();
            let request_id = request.request_id.0;
            tracing::info!(
                %resource,
                %request_id,
                "return decision window closed; serving the next queued request"
            );
            *snapshot = load_snapshot(state).await?;
            // the serving loan still needs its assignment launch
            myself.cast(ResourceMsg::Wake)?;
            Ok(Some(ResourceQueueReconcileOutcome::LoanAlreadyActive {
                loan: *loan,
            }))
        }
    }
}

/// Arm one wake for the deadline of `window`, replacing a wake for an older deadline
fn arm_return_deadline_wake(
    myself: &ActorRef<ResourceMsg>,
    state: &mut ResourceActorState,
    window: &ReturnDecisionWindow,
) {
    if state.return_deadline_wake.as_ref().is_some_and(|armed| {
        armed.action_id == window.action_id && armed.deadline_at == window.deadline_at
    }) {
        return;
    }
    let wait = window.remaining_at(Utc::now());
    let wake = myself.send_after(wait, || ResourceMsg::Wake).abort_handle();
    state.return_deadline_wake = Some(ReturnDeadlineWake {
        action_id: window.action_id,
        deadline_at: window.deadline_at,
        wake,
    });
}

fn awaiting_return_action(snapshot: &ResourceSnapshot) -> Option<ActionId> {
    match &snapshot.loan.as_ref()?.state {
        LoanState::Active {
            phase: LoanPhase::AwaitingReturn { action_id, .. },
        } => Some(*action_id),
        LoanState::Active { .. } | LoanState::NeedsAttention { .. } | LoanState::Closed { .. } => {
            None
        }
    }
}

/// Track the pending first background launch and surface its unproven states
///
/// A queued row is a launch in progress only while this actor's own supervisor
/// request is pending or inserted it. The store registers the task on its
/// confirmed start, so a registered launch needs no tracking here. A launch
/// that ended before registration with no release proof keeps the resource
/// reserved even with an empty queue, so it replaces a `NoQueuedRequest` outcome
async fn observe_background_launch(
    state: &mut ResourceActorState,
    snapshot: &ResourceSnapshot,
    outcome: &ResourceQueueReconcileOutcome,
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
    if let Some(view) = &view
        && view.awaits_operator_release()
        && matches!(outcome, ResourceQueueReconcileOutcome::NoQueuedRequest)
    {
        return Ok(Some(
            ResourceQueueReconcileOutcome::BackgroundLaunchReleaseUnproven {
                request_id: view.request_id,
                task_id: view.task_id,
            },
        ));
    }
    let Some(view) = view.filter(|view| view.phase == BackgroundLaunchPhase::Queued) else {
        return Ok(None);
    };
    let launching = state.background_launch.is_some_and(|attempt| {
        attempt.launching(view.request_id, |result| {
            result == BackgroundLaunchResult::Inserted
        })
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

fn awaiting_release(snapshot: &ResourceSnapshot) -> Option<(Loan, ActionId, TaskId)> {
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
) -> Option<(Loan, ResourceRequest)> {
    let loan = snapshot.loan.as_ref()?;
    let LoanState::Active {
        phase: LoanPhase::Serving {
            current_request_id, ..
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

    Some((loan.clone(), request.clone()))
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
    request_id: RequestId,
    task_id: TaskId,
    result: ResourceTaskActivationResult,
) -> Result<(), ActorProcessingErr> {
    let key = ActivationKey {
        request_id,
        task_id,
    };
    let recorded = state
        .activation_attempt
        .as_mut()
        .is_some_and(|attempt| attempt.finish(key, result));
    if !recorded {
        return Ok(());
    }
    reconcile_and_refresh(myself, state).await?;

    Ok(())
}

/// Bind, baseline, and request the one watcher for a running trainer
///
/// Every step reuses saved identities first, so a retry or restart binds the same
/// watcher task and request instead of allocating replacements. A co-located
/// supervisor's watcher is launched here; a remote supervisor's machine saves the
/// callback route first and then asks the authority to accept the same identity
/// The supervisor is called from a detached task because it may be waiting on
/// this actor
async fn progress_release_watcher(
    myself: &ActorRef<ResourceMsg>,
    state: &mut ResourceActorState,
    resource: &Resource,
    failure: &ReleaseProofFailure,
) -> Result<Option<ReleaseWatcherStatus>, AppError> {
    let ReleaseProofFailure {
        loan,
        action_id,
        trainer_task_id,
        ..
    } = failure;
    let (action_id, trainer_task_id) = (*action_id, *trainer_task_id);
    if state
        .watcher_launch
        .is_some_and(|attempt| attempt.key.action_id != action_id)
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
    let attempt = WatcherLaunchAttempt::pending(WatcherLaunchKey {
        action_id,
        watcher_task_id,
    });
    state.watcher_launch = Some(attempt);
    schedule_release_watcher_launch(
        myself.clone(),
        state.supervisor.clone(),
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
        Some(intent) => intent,
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
            key: WatcherLaunchKey {
                action_id,
                watcher_task_id,
            },
            progress: LaunchProgress::Finished(ReleaseWatcherLaunchResult::Uncertain),
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
    let key = WatcherLaunchKey {
        action_id,
        watcher_task_id,
    };
    let Some(attempt) = state.watcher_launch.as_mut() else {
        return Ok(());
    };
    if !attempt.finish(key, result) {
        return Ok(());
    }
    let attempt = *attempt;
    state.release_watcher = Some(watcher_status(state, attempt).await?);

    Ok(())
}

async fn launch_never_committed(
    state: &ResourceActorState,
    attempt: WatcherLaunchAttempt,
) -> Result<bool, AppError> {
    if attempt.progress != LaunchProgress::Finished(ReleaseWatcherLaunchResult::Uncertain) {
        return Ok(false);
    }
    let row = call(&state.store, |reply| StoreMsg::GetTask {
        id: attempt.key.watcher_task_id,
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
        key: WatcherLaunchKey {
            action_id,
            watcher_task_id,
        },
        progress,
    } = attempt;
    let attention = |reason| ReleaseWatcherStatus::Attention { action_id, reason };
    let LaunchProgress::Finished(result) = progress else {
        return Ok(ReleaseWatcherStatus::Launching {
            action_id,
            watcher_task_id,
        });
    };
    let inserted = match result {
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
        CompleteReleaseError::CompletedResultChanged { .. }
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
        AssignedResourceTaskAttention::ExitWitnessUnconfirmed => {
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
    ResourceId::from_uuid(uuid).ok()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_support;
