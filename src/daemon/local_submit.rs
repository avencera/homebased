//! Local submission keyed by its request UUID, so a caller can retry after a lost response
//!
//! The row and the request's `origin_routes` entry commit together. A retry with
//! the same request finds that route and returns its task instead of creating a
//! second one, and starts the worker when the earlier launch stopped after the commit

use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::api::SubmitBody;
use super::origin_submit::conflict;
use crate::domain::{ProcessStatus, TaskId};
use crate::error::AppError;
use crate::invocation::{persist_workload, resolve_workload_binary};
use crate::spec::{self, NormalizedSpec, NormalizedWorkload};
use crate::store::{self, NewTask};
use crate::submission::{OriginRoute, RequestId, SubmissionState};

/// Accept a local task once per request UUID
pub(super) async fn submit(
    state: &AppState,
    body: SubmitBody,
) -> Result<(TaskId, ProcessStatus), AppError> {
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
        return resume(state, route, &spec).await;
    }

    let binary = resolve_workload_binary(&spec.workload, &body.env.path, &spec.cwd)?;
    let id = TaskId::new();
    let paths = state.home.prepare_task(id)?;
    if let NormalizedWorkload::Agent(agent) = &spec.workload {
        crate::runner::write_task_files(&paths, &agent.prompt, agent.report_trailer)?;
    }
    let row = store::new_queued_task(NewTask {
        id,
        name: Some(spec.name.clone()),
        thread: spec.thread,
        workload: persist_workload(&spec.workload),
        cwd: spec.cwd.clone(),
        timeout: spec.timeout,
        env: body.env,
        binary,
    });
    let launched = call(&state.supervisor, |reply| SupervisorMsg::Launch {
        row: Box::new(row),
        spec: Box::new(spec.clone()),
        request,
        reply,
    })
    .await;
    match launched {
        Ok(()) => Ok((id, ProcessStatus::Queued)),
        // the launch keeps running after this caller stops waiting, so the row may commit later
        Err(AppError::DaemonBusy) => Err(AppError::SubmissionOutcomeUnknown {
            request,
            task: id,
            message: "the daemon is busy; retry with the same request UUID".into(),
        }),
        Err(error) => {
            // an earlier attempt of this request may have committed while this one was queued
            match saved_route(state, request).await? {
                Some(route) if route.task != id => resume(state, route, &spec).await,
                _ => Err(error),
            }
        }
    }
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
) -> Result<(TaskId, ProcessStatus), AppError> {
    let local = state.machine.identity.machine;
    if route.origin_machine != local || route.execution_machine != local {
        return Err(conflict(
            &route,
            "request UUID belongs to a remote submission",
        ));
    }
    if !matches!(route.submission, SubmissionState::Accepted) {
        return Err(conflict(&route, "request UUID belongs to a resource route"));
    }
    if route.spec.current() != Some(spec) {
        return Err(conflict(
            &route,
            "request UUID has different normalized content",
        ));
    }
    let id = route.task;
    match call(&state.supervisor, |reply| SupervisorMsg::ResumeLocal {
        id,
        reply,
    })
    .await?
    {
        Some(status) => Ok((id, status)),
        None => Err(conflict(
            &route,
            "request UUID belongs to a resource launch",
        )),
    }
}
