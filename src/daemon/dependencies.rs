//! Tasks held on their origin until their dependencies succeed
//!
//! A submit with `after` is admitted here. When every dependency already
//! succeeded it launches as usual; when one is still pending the origin saves
//! a held route with the task UUID it returns. One loop then owns held routes:
//! it launches a route through the ordinary local or remote path once every
//! dependency succeeded, and cancels it before launch as soon as one ended any
//! other way. The loop starts with the daemon, so endings that arrived while
//! the daemon was down apply on start

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::time::MissedTickBehavior;
use tracing::warn;

use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::api::SubmitBody;
use super::api::views::DependencyView;
use super::cancel_delivery::CancelResponse;
use super::event_sender::Retry;
use super::{local_submit, origin_submit};
use crate::dependency::{
    DependencyOutcome, DependencyState, HeldCancellation, HeldDecision, TaskDependencies, decide,
};
use crate::domain::{ProcessStatus, TaskId, TaskStatus};
use crate::error::AppError;
use crate::invocation::resolve_workload_binary;
use crate::spec::{self, NormalizedSpec};
use crate::store::HeldCancel;
use crate::submission::{
    CallbackContext, DependentRoute, HeldPhase, NewHeldRoute, OriginRoute, SubmissionState,
};

/// How often the loop re-reads held routes; launches and endings are rare
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Accept a submit, holding it on this daemon when a dependency is still pending
pub(super) async fn submit(
    state: &AppState,
    body: SubmitBody,
) -> Result<(TaskId, TaskStatus), AppError> {
    let Some(after) = body.after.clone() else {
        return launch(state, body).await;
    };
    // a retry resolves against its saved route, held or launched
    let request = body.request;
    if call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request,
        reply,
    })
    .await?
    .is_some()
    {
        return launch(state, body).await;
    }
    match admit(state, &after).await? {
        Admission::Ready => launch(state, body).await,
        Admission::Pending => hold(state, body, after).await,
    }
}

/// Check the dependencies of a dry run the way a submit would, saving nothing
pub(super) async fn preview(
    state: &AppState,
    after: &TaskDependencies,
) -> Result<Vec<DependencyView>, AppError> {
    admit(state, after).await?;
    views(state, after).await
}

/// Dependency states in `after` order, for inspection
pub(super) async fn views(
    state: &AppState,
    after: &TaskDependencies,
) -> Result<Vec<DependencyView>, AppError> {
    Ok(known_states(state, after)
        .await?
        .into_iter()
        .map(|(task, state)| DependencyView { task, state })
        .collect())
}

/// Cancel a held task before launch, or `None` when it is not held here
///
/// Tasks held on this one are cancelled by the release loop, with this task
/// named as the dependency that ended
pub(super) async fn cancel(
    state: &AppState,
    id: TaskId,
) -> Result<Option<CancelResponse>, AppError> {
    let Some(route) = call(&state.store, |reply| StoreMsg::OriginRoute { id, reply }).await? else {
        return Ok(None);
    };
    if !matches!(
        route.submission,
        SubmissionState::Held {
            phase: HeldPhase::Waiting | HeldPhase::Cancelled { .. }
        }
    ) {
        return Ok(None);
    }
    let _request_guard = state.locks.origin_submissions.lock(route.request).await;
    match cancel_held(state, id, HeldCancellation::Requested).await? {
        HeldCancel::Cancelled(_) => Ok(Some(CancelResponse::status(id, ProcessStatus::Cancelled))),
        HeldCancel::NotHeld(_) => Ok(None),
    }
}

/// Watch held routes for as long as the daemon runs
pub(super) async fn run(state: AppState) {
    let mut retries: HashMap<TaskId, Retry> = HashMap::new();
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        if let Err(error) = settle(&state, &mut retries).await {
            warn!("task dependency release: {error}");
        }
    }
}

/// Whether a new submission may launch now
enum Admission {
    /// Every dependency succeeded
    Ready,
    /// At least one dependency has not finished
    Pending,
}

/// Refuse unknown and failed dependencies, then report whether the task must wait
async fn admit(state: &AppState, after: &TaskDependencies) -> Result<Admission, AppError> {
    let tasks = after.tasks().to_vec();
    let states = call(&state.store, |reply| StoreMsg::DependencyStates {
        tasks,
        reply,
    })
    .await?;
    let mut known = Vec::with_capacity(states.len());
    for (task, state) in states {
        known.push((task, state.ok_or(AppError::UnknownDependency { task })?));
    }
    match decide(known) {
        HeldDecision::Release => Ok(Admission::Ready),
        HeldDecision::Wait => Ok(Admission::Pending),
        HeldDecision::Cancel(failure) => Err(AppError::DependencyFailed {
            task: failure.dependency,
            outcome: failure.outcome,
        }),
    }
}

/// Dependency states, with a dependency whose route is gone read as an unknown ending
///
/// Admission accepted each dependency by its route here, and routes are not
/// deleted. If one is gone anyway, it can never be shown to succeed
async fn known_states(
    state: &AppState,
    after: &TaskDependencies,
) -> Result<Vec<(TaskId, DependencyState)>, AppError> {
    let tasks = after.tasks().to_vec();
    let states = call(&state.store, |reply| StoreMsg::DependencyStates {
        tasks,
        reply,
    })
    .await?;
    Ok(states
        .into_iter()
        .map(|(task, state)| {
            (
                task,
                state.unwrap_or(DependencyState::Ended(DependencyOutcome::Unknown)),
            )
        })
        .collect())
}

async fn launch(state: &AppState, body: SubmitBody) -> Result<(TaskId, TaskStatus), AppError> {
    if body.spec.machine.is_some() {
        origin_submit::submit(state, body).await
    } else {
        local_submit::submit(state, body).await
    }
}

/// Save a held route after checking what a launch would check now
///
/// The execution machine may be offline while the task waits, so a remote
/// spec is checked by its executor only at launch
async fn hold(
    state: &AppState,
    body: SubmitBody,
    after: TaskDependencies,
) -> Result<(TaskId, TaskStatus), AppError> {
    let SubmitBody {
        spec,
        env,
        request,
        callback_cwd,
        after: _,
    } = body;
    let (execution_machine, callback) = match &spec.machine {
        Some(name) => (
            origin_submit::execution_machine(state, name).await?,
            origin_submit::callback_context(env, callback_cwd)?,
        ),
        None => (
            state.machine.identity.machine,
            local_callback(state, &spec, env).await?,
        ),
    };
    let task = TaskId::new();
    let route = OriginRoute::new_held(NewHeldRoute {
        request,
        task,
        origin_machine: state.machine.identity.machine,
        execution_machine,
        callback,
        spec,
    });
    let saved = origin_submit::insert_route(state, route, Some(after)).await?;
    let SubmissionState::Held { phase } = &saved.submission else {
        // an identical concurrent request saved a route that already launched
        return Err(origin_submit::conflict(
            &saved,
            "request UUID was launched by a concurrent submit; retry it",
        ));
    };
    Ok((saved.task, phase.status()))
}

/// Callback context of a held local task, as a local launch would save it
async fn local_callback(
    state: &AppState,
    spec: &NormalizedSpec,
    env: crate::domain::TaskEnv,
) -> Result<CallbackContext, AppError> {
    spec::check_spec_host(spec)?;
    resolve_workload_binary(&spec.workload, &env.path, &spec.cwd)?;
    let path = env.path.clone();
    let cwd = spec.cwd.clone();
    let codex = call(&state.supervisor, |reply| SupervisorMsg::CallbackCodex {
        path,
        cwd,
        reply,
    })
    .await?;
    Ok(CallbackContext {
        env,
        cwd: spec.cwd.clone(),
        codex,
    })
}

/// Apply every dependency ending to the held routes, then launch what is ready
///
/// A local release whose row committed but whose worker never started, after
/// a timed-out launch or a daemon stop, is resumed first. A cancelled held
/// task is itself an ending for tasks held on it, so the pass repeats until it
/// cancels nothing
async fn settle(state: &AppState, retries: &mut HashMap<TaskId, Retry>) -> Result<(), AppError> {
    let unstarted = call(&state.store, |reply| StoreMsg::UnstartedDependentTasks {
        reply,
    })
    .await?;
    for task in &unstarted {
        retry_resume(state, retries, *task).await;
    }
    loop {
        let routes = call(&state.store, |reply| StoreMsg::HeldRoutes { reply }).await?;
        retries.retain(|task, _| {
            unstarted.contains(task) || routes.iter().any(|held| held.route.task == *task)
        });
        let mut cancelled = false;
        for DependentRoute { route, after } in routes {
            match &route.submission {
                SubmissionState::Held {
                    phase: HeldPhase::Waiting,
                } => {}
                // the executor may hold the task, so only a resend can resolve it
                _ => {
                    retry_release(state, retries, route).await;
                    continue;
                }
            }
            match decide(known_states(state, &after).await?) {
                HeldDecision::Wait => {}
                HeldDecision::Release => retry_release(state, retries, route).await,
                HeldDecision::Cancel(failure) => {
                    let _request_guard = state.locks.origin_submissions.lock(route.request).await;
                    let result = cancel_held(state, route.task, failure.into()).await?;
                    cancelled |= matches!(result, HeldCancel::Cancelled(_));
                }
            }
        }
        if !cancelled {
            return Ok(());
        }
    }
}

/// Launch one ready route unless its last attempt failed too recently
async fn retry_release(state: &AppState, retries: &mut HashMap<TaskId, Retry>, route: OriginRoute) {
    let task = route.task;
    if backing_off(retries, task) {
        return;
    }
    let released = if route.origin_machine == route.execution_machine {
        local_submit::release(state, route).await
    } else {
        origin_submit::release(state, route).await
    };
    record_attempt(retries, task, released, "held task launch deferred");
}

/// Start the worker of a released local task unless its last attempt failed too recently
///
/// The supervisor leaves a task whose worker already started alone
async fn retry_resume(state: &AppState, retries: &mut HashMap<TaskId, Retry>, task: TaskId) {
    if backing_off(retries, task) {
        return;
    }
    let resumed = call(&state.supervisor, |reply| SupervisorMsg::ResumeLocal {
        id: task,
        reply,
    })
    .await;
    record_attempt(
        retries,
        task,
        resumed.map(|_| ()),
        "released task launch deferred",
    );
}

fn backing_off(retries: &HashMap<TaskId, Retry>, task: TaskId) -> bool {
    retries
        .get(&task)
        .is_some_and(|retry| !retry.ready(Instant::now()))
}

/// Forget a task's backoff after a successful attempt, or extend it after a failure
fn record_attempt(
    retries: &mut HashMap<TaskId, Retry>,
    task: TaskId,
    result: Result<(), AppError>,
    deferred: &str,
) {
    let Err(error) = result else {
        retries.remove(&task);
        return;
    };
    warn!(%task, "{deferred}: {error}");
    let retry = Retry::after_failure(retries.get(&task).copied(), Instant::now());
    retries.insert(task, retry);
}

/// Cancel a waiting held route and deliver its terminal event
///
/// The caller holds the route's request lock, so a launch cannot start meanwhile
async fn cancel_held(
    state: &AppState,
    id: TaskId,
    cause: HeldCancellation,
) -> Result<HeldCancel, AppError> {
    let result = call(&state.store, |reply| StoreMsg::CancelHeldRoute {
        id,
        cause,
        reply,
    })
    .await?;
    if matches!(result, HeldCancel::Cancelled(_)) {
        state.supervisor.cast(SupervisorMsg::DispatchInbox { id })?;
    }
    Ok(result)
}
