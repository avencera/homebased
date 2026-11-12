//! CI queue: machine offers, placement of held CI jobs, and the `ci` routes
//!
//! A CI job waits on its origin, held like a job with `after`, until a
//! machine that offers its class has a free slot. Each pass of the held-job
//! loop reads every machine's offer and sends ready jobs to the machines
//! [`place`] picks. See `docs/ci-queue-design.md`

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::AppState;
use super::actors::{StoreMsg, call};
use super::ci_gauge::{checkout_root, read_gauge};
use super::fleet_view::{FleetMachine, Scope, read_fleet};
use super::queue_api::{QueueJson, forward, resolve_machine, send_job};
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::ci::{
    Admission, Assignment, ClassStats, JobClass, MachineChoice, MachineOffer, PendingJob,
    SlotCapacity, class_stats, place,
};
use crate::queue::delivery::JobRoute;
use crate::queue::spec::{JobSpec, MachineSelector};
use crate::queue::{JobId, Priority, QueueError};
use crate::store::queue::interface::{CiStatus, CiTimingList, QueueRequest};

/// Read routes, safe on the TCP listener
pub fn read_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/ci/slots", get(slots))
        .route("/v1/ci/stats", get(stats))
}

/// Controls that change capacity, admission, or where a job runs
pub fn control_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/ci/capacity", post(capacity))
        .route("/v1/ci/drain", post(drain))
        .route("/v1/ci/jobs/{job}/move", post(move_job))
}

/// This machine's answer to `ci_status`
pub(super) async fn status(state: &AppState) -> Result<Value, AppError> {
    let mut value = serde_json::to_value(local_status(state).await?)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("api_version".into(), API_VERSION.into());
    }
    Ok(value)
}

async fn local_status(state: &AppState) -> Result<CiStatus, AppError> {
    let machine = state.machine.identity.machine;
    let list = call(&state.store, |reply| StoreMsg::CiSlotList {
        machine,
        reply,
    })
    .await?;
    let root = checkout_root(&state.home, &state.ci);
    let gauge = tokio::task::spawn_blocking(move || read_gauge(&root))
        .await
        .map_err(|error| AppError::Internal {
            message: format!("CI gauge thread: {error}"),
        })?;
    Ok(CiStatus {
        offer: MachineOffer {
            machine,
            admission: Admission::decide(list.drain.as_ref(), &gauge, &state.ci),
            gauge,
            classes: list.classes,
        },
        slots: list.slots,
    })
}

/// One machine's `ci_status`, checked to come from that machine
async fn machine_status(state: &AppState, machine: MachineId) -> Result<CiStatus, AppError> {
    let status: CiStatus =
        serde_json::from_value(forward(state, machine, QueueRequest::CiStatus).await?)?;
    if status.offer.machine != machine {
        return Err(AppError::MachineUnavailable {
            machine,
            message: "peer answered for another machine".into(),
        });
    }
    Ok(status)
}

/// `ci_status` of every machine `scope` covers that answers; a peer on a
/// release without CI jobs is left out
async fn fleet_status(
    state: &AppState,
    scope: Scope,
) -> Result<(Vec<FleetMachine>, Vec<CiStatus>), AppError> {
    let read = read_fleet(state, scope, local_status(state), |_, machine| {
        let state = state.clone();
        async move {
            machine_status(&state, machine)
                .await
                .map_err(|error| error.to_string())
        }
    })
    .await?;
    Ok((
        read.machines,
        read.answers.into_iter().map(|(_, status)| status).collect(),
    ))
}

/// Refuse a CI job no reachable machine it may run on offers its class
pub(super) async fn check_offered(
    state: &AppState,
    spec: &JobSpec,
    choice: MachineChoice,
) -> Result<(), AppError> {
    let Some(ci) = &spec.ci else {
        return Ok(());
    };
    let scope = match choice {
        MachineChoice::Any => Scope::All,
        MachineChoice::Pinned(machine) => Scope::One(machine),
    };
    let (_, statuses) = fleet_status(state, scope).await?;
    let offered = |status: &CiStatus| {
        status
            .offer
            .class(&ci.class)
            .is_some_and(|offer| offer.capacity.get() > 0)
    };
    if statuses.iter().any(offered) {
        return Ok(());
    }
    let mut classes: Vec<JobClass> = statuses
        .iter()
        .flat_map(|status| status.offer.classes.iter())
        .filter(|offer| offer.capacity.get() > 0)
        .map(|offer| offer.class.clone())
        .collect();
    classes.sort();
    classes.dedup();
    Err(QueueError::ClassNotOffered {
        class: ci.class.clone(),
        offered: classes,
    }
    .into())
}

/// Which machines a held CI route may go to: its pinned authority, or any
pub(super) fn choice(route: &JobRoute) -> MachineChoice {
    match route.spec.machine {
        Some(_) => MachineChoice::Pinned(route.authority),
        None => MachineChoice::Any,
    }
}

/// Place CI routes whose `after` entries all succeeded, and send each placed
/// one to its machine
///
/// Returns each job that was sent with the send's result, so the caller
/// backs off a machine that did not answer
pub(super) async fn place_ready(
    state: &AppState,
    ready: &[JobRoute],
) -> Result<Vec<(JobId, Result<(), AppError>)>, AppError> {
    if ready.is_empty() {
        return Ok(Vec::new());
    }
    let (_, statuses) = fleet_status(state, Scope::All).await?;
    let offers: Vec<MachineOffer> = statuses.into_iter().map(|status| status.offer).collect();
    let pending: Vec<PendingJob> = ready
        .iter()
        .filter_map(|route| {
            let ci = route.spec.ci.as_ref()?;
            Some(PendingJob {
                job: route.job,
                class: ci.class.clone(),
                priority: route.spec.priority,
                choice: choice(route),
                submitted_at: route.submitted_at.unwrap_or_else(Utc::now),
            })
        })
        .collect();
    let mut sent = Vec::new();
    for Assignment { job, machine } in place(&pending, &offers) {
        let _guard = state.locks.job_submissions.lock(job).await;
        // a cancel or move may have changed the route since it was read
        let current = call(&state.store, |reply| StoreMsg::JobRoute { job, reply }).await?;
        let expected = ready.iter().find(|route| route.job == job);
        if current.as_ref().map(|route| &route.digest) != expected.map(|route| &route.digest) {
            continue;
        }
        let Some(route) = call(&state.store, |reply| StoreMsg::PlaceHeldJob {
            job,
            authority: machine,
            reply,
        })
        .await?
        else {
            continue;
        };
        tracing::info!(%job, %machine, "placed CI job");
        sent.push((job, send_job(state, route).await.map(|_| ())));
    }
    Ok(sent)
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeQuery {
    #[serde(default)]
    machine: Option<MachineSelector>,
}

/// `GET /v1/ci/slots`
#[derive(Debug, Serialize)]
pub struct CiSlots {
    /// Public API version
    pub api_version: u32,
    /// Machines the read covered and how each read went
    pub machines: Vec<FleetMachine>,
    /// One status per machine that answered
    pub statuses: Vec<CiStatus>,
}

async fn slots(
    State(state): State<AppState>,
    Query(query): Query<ScopeQuery>,
) -> Result<Json<CiSlots>, AppError> {
    let scope = scope(&state, query.machine.as_ref()).await?;
    let (machines, statuses) = fleet_status(&state, scope).await?;
    Ok(Json(CiSlots {
        api_version: API_VERSION,
        machines,
        statuses,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatsQuery {
    since: DateTime<Utc>,
    #[serde(default)]
    machine: Option<MachineSelector>,
    #[serde(default)]
    class: Option<JobClass>,
}

/// `GET /v1/ci/stats`
#[derive(Debug, Serialize)]
pub struct CiStats {
    /// Public API version
    pub api_version: u32,
    /// Earliest job end included
    pub since: DateTime<Utc>,
    /// Machines the read covered and how each read went
    pub machines: Vec<FleetMachine>,
    /// Per machine and class, waits and run times
    pub stats: Vec<ClassStats>,
}

async fn stats(
    State(state): State<AppState>,
    Query(query): Query<StatsQuery>,
) -> Result<Json<CiStats>, AppError> {
    let scope = scope(&state, query.machine.as_ref()).await?;
    let since = query.since;
    let local = async {
        let value = forward(
            &state,
            state.machine.identity.machine,
            QueueRequest::CiTimings { since },
        )
        .await?;
        Ok(serde_json::from_value::<CiTimingList>(value)?)
    };
    let read = read_fleet(&state, scope, local, |_, machine| {
        let state = state.clone();
        async move {
            let value = forward(&state, machine, QueueRequest::CiTimings { since })
                .await
                .map_err(|error| error.to_string())?;
            serde_json::from_value::<CiTimingList>(value).map_err(|error| error.to_string())
        }
    })
    .await?;
    let timings: Vec<_> = read
        .answers
        .into_iter()
        .flat_map(|(_, list)| list.timings)
        .filter(|timing| {
            query
                .class
                .as_ref()
                .is_none_or(|class| &timing.class == class)
        })
        .collect();
    Ok(Json(CiStats {
        api_version: API_VERSION,
        since,
        machines: read.machines,
        stats: class_stats(&timings),
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapacityBody {
    class: JobClass,
    capacity: SlotCapacity,
}

async fn capacity(
    State(state): State<AppState>,
    Query(query): Query<ScopeQuery>,
    QueueJson(body): QueueJson<CapacityBody>,
) -> Result<Json<Value>, AppError> {
    let machine = resolve_machine(&state, query.machine.as_ref()).await?;
    forward(
        &state,
        machine,
        QueueRequest::CiCapacity {
            class: body.class,
            capacity: body.capacity,
        },
    )
    .await
    .map(Json)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DrainBody {
    /// Why the machine is drained; `None` reopens it
    reason: Option<String>,
}

async fn drain(
    State(state): State<AppState>,
    Query(query): Query<ScopeQuery>,
    QueueJson(body): QueueJson<DrainBody>,
) -> Result<Json<Value>, AppError> {
    if body
        .reason
        .as_ref()
        .is_some_and(|reason| reason.trim().is_empty() || reason.len() > 500)
    {
        return Err(AppError::Usage {
            message: "a drain reason must be 1 to 500 bytes".into(),
        });
    }
    let machine = resolve_machine(&state, query.machine.as_ref()).await?;
    forward(
        &state,
        machine,
        QueueRequest::CiDrain {
            reason: body.reason,
        },
    )
    .await
    .map(Json)
}

/// Where a moved CI job may run
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "machine", rename_all = "snake_case")]
enum MoveTarget {
    /// Any machine that offers its class
    Any,
    /// Only this machine
    Machine(MachineSelector),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MoveBody {
    /// New machine choice; `None` keeps it
    #[serde(default)]
    target: Option<MoveTarget>,
    /// New level; `None` keeps it
    #[serde(default)]
    priority: Option<Priority>,
}

/// Move a CI job that is still waiting on this origin for a slot
///
/// A job already sent to a machine's queue is refused; `resource job move`
/// reorders it there
async fn move_job(
    State(state): State<AppState>,
    Path(job): Path<JobId>,
    QueueJson(body): QueueJson<MoveBody>,
) -> Result<Json<Value>, AppError> {
    if body.target.is_none() && body.priority.is_none() {
        return Err(AppError::Usage {
            message: "name a machine, any machine, or a priority".into(),
        });
    }
    let _guard = state.locks.job_submissions.lock(job).await;
    let route = call(&state.store, |reply| StoreMsg::JobRoute { job, reply })
        .await?
        .ok_or(QueueError::JobNotFound { job })?;
    let (machine, authority) = match body.target {
        None => (route.spec.machine.clone(), route.authority),
        Some(MoveTarget::Any) => (None, state.machine.identity.machine),
        Some(MoveTarget::Machine(selector)) => {
            let authority = resolve_machine(&state, Some(&selector)).await?;
            (Some(selector), authority)
        }
    };
    let mut moved = route.spec.clone();
    moved.machine.clone_from(&machine);
    check_offered(
        &state,
        &moved,
        if machine.is_some() {
            MachineChoice::Pinned(authority)
        } else {
            MachineChoice::Any
        },
    )
    .await?;
    let route = call(&state.store, |reply| StoreMsg::MoveHeldCiJob {
        job,
        machine,
        authority,
        priority: body.priority,
        reply,
    })
    .await?;
    Ok(Json(serde_json::json!({
        "api_version": API_VERSION,
        "job_id": route.job,
        "choice": choice(&route),
        "priority": route.spec.priority,
    })))
}

async fn scope(state: &AppState, machine: Option<&MachineSelector>) -> Result<Scope, AppError> {
    Ok(match machine {
        None => Scope::All,
        Some(selector) => Scope::One(resolve_machine(state, Some(selector)).await?),
    })
}
