//! The GPU lease guard loop on the queue's machine
//!
//! Each tick reads the GPU runs from the store, probes NVML's compute
//! processes once with `nvidia-smi --query-compute-apps`, and matches them
//! against each run's marked processes. A run idle past the threshold
//! trips: the store records the trip with its stop, the queue actor enacts
//! the stop, the event that ends the attempt carries the trip to the job's
//! thread, and the queue owner gets an ntfy notice when one is configured.
//! Linux and NVIDIA only; see [`crate::queue::lease_guard`] for the rules

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use ractor::ActorRef;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use super::actors::{StoreMsg, call};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::notify::{Notice, NoticePriority, Notifier};
use crate::queue::gpu_idle::nvidia_smi;
use crate::queue::lease_guard::{
    IdleClocks, LeaseGuardConfig, LeaseIdleTrip, Observation, TripAction, yield_overdue,
};
use crate::queue::{ActiveRun, RunPhase, StopCause};

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) async fn run(
    store: ActorRef<StoreMsg>,
    machine: MachineId,
    config: LeaseGuardConfig,
    notifier: Option<Arc<Notifier>>,
) {
    if !config.enabled || !cfg!(target_os = "linux") {
        return;
    }
    let mut clocks = IdleClocks::default();
    let mut ticks = tokio::time::interval(config.interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut probe_failed = false;
    loop {
        ticks.tick().await;
        if let Err(error) = tick(
            &store,
            machine,
            &config,
            &notifier,
            &mut clocks,
            &mut probe_failed,
        )
        .await
        {
            warn!("GPU lease guard: {error}");
        }
    }
}

async fn tick(
    store: &ActorRef<StoreMsg>,
    machine: MachineId,
    config: &LeaseGuardConfig,
    notifier: &Option<Arc<Notifier>>,
    clocks: &mut IdleClocks,
    probe_failed: &mut bool,
) -> Result<(), AppError> {
    let targets = call(store, |reply| StoreMsg::LeaseGuardTargets {
        machine,
        reply,
    })
    .await?;
    clocks.retain(&targets.iter().map(|target| target.run.task).collect());
    if targets.is_empty() {
        return Ok(());
    }
    let now = Utc::now();

    // a yield the guard asked for and the run ignored becomes a cancel
    for target in &targets {
        let (
            Some(trip),
            RunPhase::Stopping {
                cause: StopCause::Yield,
                ..
            },
        ) = (&target.trip, &target.run.phase)
        else {
            continue;
        };
        if trip.action == TripAction::Yield
            && trip.escalated_at.is_none()
            && yield_overdue(trip.tripped_at, now, config.yield_grace)
        {
            let task = target.run.task;
            let escalated = call(store, |reply| StoreMsg::LeaseGuardEscalate {
                task,
                now,
                reply,
            })
            .await?;
            if let Some((run, trip)) = escalated {
                announce(notifier, &run, &trip);
            }
        }
    }

    let apps = match nvidia_smi::compute_apps(PROBE_TIMEOUT).await {
        Ok(apps) => {
            *probe_failed = false;
            Some(apps)
        }
        Err(error) => {
            if !*probe_failed {
                warn!("GPU lease guard: {error}; no run trips while probes fail");
            }
            *probe_failed = true;
            None
        }
    };
    for target in targets.into_iter().filter(|target| target.trip.is_none()) {
        let task = target.run.task;
        let pids = tokio::task::spawn_blocking(move || run_pids(task))
            .await
            .ok()
            .flatten();
        let seen = Observation::of(apps.as_ref(), &target.gpu_uuid, pids.as_ref());
        let Some(idle_since) = clocks.observe(task, seen, now, config.idle_after) else {
            continue;
        };
        clocks.forget(task);
        let gpu_uuid = target.gpu_uuid.clone();
        let idle_after = config.idle_after;
        let tripped = call(store, |reply| StoreMsg::LeaseGuardTrip {
            task,
            gpu_uuid,
            idle_since,
            idle_after,
            now,
            reply,
        })
        .await?;
        if let Some((run, trip)) = tripped {
            announce(notifier, &run, &trip);
        }
    }
    Ok(())
}

/// PIDs of the run's processes, or `None` when they could not be read
fn run_pids(task: crate::domain::TaskId) -> Option<HashSet<i32>> {
    let marked = crate::cleanup::marked_processes(task).ok()?;
    Some(
        marked
            .iter()
            .map(|identity| identity.pid.as_raw())
            .collect(),
    )
}

/// Log the trip and tell the queue owner through ntfy
fn announce(notifier: &Option<Arc<Notifier>>, run: &ActiveRun, trip: &LeaseIdleTrip) {
    let reason = trip.reason();
    info!(job = %run.job, task = %run.task, action = trip.action.as_str(), "{reason}");
    let Some(notifier) = notifier.clone() else {
        return;
    };
    let notice = Notice {
        title: format!("GPU lease released: job {}", run.job),
        message: reason,
        tags: vec!["warning".into()],
        priority: NoticePriority::High,
    };
    tokio::task::spawn_blocking(move || {
        if let Err(error) = notifier.send(&notice) {
            warn!("GPU lease guard notice: {error}");
        }
    });
}
