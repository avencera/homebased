//! Local submission keyed by its request UUID, so a caller can retry after a lost response
//!
//! The row and the request's `origin_routes` entry commit together. A retry with
//! the same request finds that route and returns its task instead of creating a
//! second one, and starts the worker when the earlier launch stopped after the commit

use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::api::SubmitBody;
use super::origin_submit::conflict;
use crate::dependency::TaskDependencies;
use crate::domain::{ProcessStatus, TaskEnv, TaskId, TaskStatus};
use crate::error::AppError;
use crate::invocation::{persist_workload, resolve_workload_binary};
use crate::spec::{self, NormalizedSpec};
use crate::store::{self, LocalAdmission, NewTask};
use crate::submission::{HeldPhase, OriginRoute, RequestId, SubmissionState};

/// Accept a local task once per request UUID
pub(super) async fn submit(
    state: &AppState,
    body: SubmitBody,
) -> Result<(TaskId, TaskStatus), AppError> {
    let spec = body.spec;
    if spec.machine.is_some() {
        return Err(AppError::Usage {
            message: "local task acceptance received a remote machine selector".into(),
        });
    }
    spec::check_spec_host(&spec)?;
    let request = body.request;
    let _request_guard = state.locks.origin_submissions.lock(request).await;
    if let Some(route) = saved_route(state, request).await? {
        return resume(state, route, &spec, body.after.as_ref()).await;
    }

    let id = TaskId::new();
    let admission = LocalAdmission::Submitted {
        request,
        after: body.after.clone(),
    };
    let launched = launch_row(state, id, &spec, body.env, admission).await;
    match launched {
        Ok(()) => Ok((id, ProcessStatus::Queued.into())),
        // the launch keeps running after this caller stops waiting, so the row may commit later
        Err(AppError::DaemonBusy) => Err(AppError::SubmissionOutcomeUnknown {
            request,
            task: id,
            message: "the daemon is busy; retry with the same request UUID".into(),
        }),
        Err(error) => {
            // an earlier attempt of this request may have committed while this one was queued
            match saved_route(state, request).await? {
                Some(route) if route.task != id => {
                    resume(state, route, &spec, body.after.as_ref()).await
                }
                _ => Err(error),
            }
        }
    }
}

/// Launch a held local task whose dependencies all succeeded
///
/// It runs with the spec and environment saved at submit and keeps its
/// pre-assigned task UUID. A launch that can never succeed, such as a missing
/// executable, closes the route and tells its thread; a busy daemon is retried
pub(super) async fn release(state: &AppState, route: OriginRoute) -> Result<(), AppError> {
    let _request_guard = state.locks.origin_submissions.lock(route.request).await;
    let Some(route) = call(&state.store, |reply| StoreMsg::OriginRoute {
        id: route.task,
        reply,
    })
    .await?
    else {
        return Err(AppError::RouteNotFound { task: route.task });
    };
    if !matches!(
        route.submission,
        SubmissionState::Held {
            phase: HeldPhase::Waiting
        }
    ) {
        return Ok(());
    }
    let spec = route.spec.clone();
    let admission = LocalAdmission::Released {
        request: route.request,
    };
    let checked = spec::check_spec_host(&spec).map_err(AppError::from);
    let launched = match checked {
        Ok(()) => {
            launch_row(
                state,
                route.task,
                &spec,
                route.callback.env.clone(),
                admission,
            )
            .await
        }
        Err(error) => Err(error),
    };
    match launched {
        Ok(()) => Ok(()),
        Err(error) if is_transient(&error) => Err(error),
        Err(error) => {
            call(&state.store, |reply| StoreMsg::RefuseHeldLaunch {
                id: route.task,
                reason: error.to_string(),
                reply,
            })
            .await?;
            state
                .supervisor
                .cast(SupervisorMsg::DispatchInbox { id: route.task })?;
            Ok(())
        }
    }
}

/// Whether a refused local launch may succeed when retried later
///
/// A busy store, another task resuming the same Codex thread, and a storage
/// failure pass; anything else would refuse the same launch again
fn is_transient(error: &AppError) -> bool {
    matches!(
        error,
        AppError::DaemonBusy | AppError::ResumeThreadBusy { .. } | AppError::Internal { .. }
    )
}

/// Hand the queued row to the supervisor, which writes its files and launches it
async fn launch_row(
    state: &AppState,
    id: TaskId,
    spec: &NormalizedSpec,
    env: TaskEnv,
    admission: LocalAdmission,
) -> Result<(), AppError> {
    let binary = resolve_workload_binary(&spec.workload, &env.path, &spec.cwd)?;
    let row = store::new_queued_task(NewTask {
        id,
        name: spec.name.clone(),
        thread: spec.thread,
        workload: persist_workload(&spec.workload),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env,
        binary,
    });
    call(&state.supervisor, |reply| SupervisorMsg::Launch {
        row: Box::new(row),
        spec: Box::new(spec.clone()),
        admission,
        reply,
    })
    .await
}

async fn saved_route(
    state: &AppState,
    request: RequestId,
) -> Result<Option<OriginRoute>, AppError> {
    call(&state.store, |reply| StoreMsg::OriginRouteByRequest {
        request,
        reply,
    })
    .await
}

/// Return the task saved for this request, launching it if its worker never started
async fn resume(
    state: &AppState,
    route: OriginRoute,
    spec: &NormalizedSpec,
    after: Option<&TaskDependencies>,
) -> Result<(TaskId, TaskStatus), AppError> {
    let local = state.machine.identity.machine;
    if route.origin_machine != local || route.execution_machine != local {
        return Err(conflict(
            &route,
            "request UUID belongs to a remote submission",
        ));
    }
    if route.spec != *spec {
        return Err(conflict(
            &route,
            "request UUID has different normalized content",
        ));
    }
    let saved_after = call(&state.store, |reply| StoreMsg::RouteDependencies {
        id: route.task,
        reply,
    })
    .await?;
    if saved_after.as_ref() != after {
        return Err(conflict(&route, "request UUID has different dependencies"));
    }
    match &route.submission {
        SubmissionState::Accepted => {}
        SubmissionState::Held { phase } => return Ok((route.task, phase.status())),
        SubmissionState::Rejected { reason } => {
            return Err(AppError::SubmissionRejected {
                request: route.request,
                task: route.task,
                reason: reason.clone(),
            });
        }
        // only a remote submission waits on an unknown acceptance
        SubmissionState::AcceptanceUnknown => {
            return Err(conflict(
                &route,
                "request UUID belongs to a remote submission",
            ));
        }
    }
    let id = route.task;
    let status = call(&state.supervisor, |reply| SupervisorMsg::ResumeLocal {
        id,
        reply,
    })
    .await?;
    Ok((id, status.into()))
}
