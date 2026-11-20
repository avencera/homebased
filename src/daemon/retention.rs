//! The daemon's retention timer and `POST /v1/gc`
//!
//! One task owns every pass: it runs the timed ones and serves `gc` requests
//! in turn, each with [`crate::retention::run_pass`] on a blocking thread, so
//! `homebased gc` during a timed pass waits for it and then plans from what
//! that pass left

use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{info, warn};

use super::AppState;
use super::actors::store::StoreMsg;
use super::ci_gauge::checkout_root;
use crate::API_VERSION;
use crate::error::AppError;
use crate::retention::{GcReport, PassMode, RetentionConfig, run_pass};

/// Delay before the first timed pass, so a restart does not walk the state
/// directory while the daemon reconciles its workers
const FIRST_PASS_DELAY: Duration = Duration::from_secs(10 * 60);

/// Longest a vacuum may hold the store actor before `gc` stops waiting; the
/// vacuum itself runs on
const VACUUM_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// A `gc` request waiting for the retention task
struct PassRequest {
    mode: PassMode,
    vacuum: bool,
    reply: oneshot::Sender<Result<GcReport, AppError>>,
}

/// Retention settings and the way to ask the retention task for a pass
#[derive(Clone)]
pub(crate) struct Retention {
    config: RetentionConfig,
    requests: mpsc::Sender<PassRequest>,
}

/// The retention task's end of [`Retention`]
pub(crate) struct PassRequests(mpsc::Receiver<PassRequest>);

impl Retention {
    pub(crate) fn new(config: RetentionConfig) -> (Self, PassRequests) {
        // a few waiting `gc` calls are plenty; more wait to be queued
        let (requests, receiver) = mpsc::channel(4);
        (Self { config, requests }, PassRequests(receiver))
    }

    async fn request(&self, mode: PassMode, vacuum: bool) -> Result<GcReport, AppError> {
        let (reply, answer) = oneshot::channel();
        let stopped = || AppError::Internal {
            message: "the retention task stopped".into(),
        };
        self.requests
            .send(PassRequest {
                mode,
                vacuum,
                reply,
            })
            .await
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }
}

/// Body of `POST /v1/gc`
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcRequest {
    /// Plan and report without removing anything
    #[serde(default)]
    pub dry_run: bool,
    /// After the pass, rebuild the database to return its free pages
    #[serde(default)]
    pub vacuum: bool,
}

/// Response of `POST /v1/gc`
#[derive(Debug, Clone, Serialize)]
pub struct GcResponse {
    /// Schema version
    pub api_version: u32,
    /// What the pass found and pruned
    #[serde(flatten)]
    pub report: GcReport,
    /// The settings it pruned by
    pub retention: RetentionConfig,
}

/// Socket-only routes: a pass removes files, so the dashboard never mounts it
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/v1/gc", post(gc))
}

async fn gc(
    State(state): State<AppState>,
    Json(request): Json<GcRequest>,
) -> Result<Json<GcResponse>, AppError> {
    let mode = if request.dry_run {
        PassMode::DryRun
    } else {
        PassMode::Apply
    };
    let vacuum = request.vacuum && !request.dry_run;
    let report = state.retention.request(mode, vacuum).await?;

    Ok(Json(GcResponse {
        api_version: API_VERSION,
        report,
        retention: state.retention.config,
    }))
}

async fn pass(state: &AppState, mode: PassMode) -> Result<GcReport, AppError> {
    let home = state.home.clone();
    let root = checkout_root(&state.home, &state.ci);
    let config = state.retention.config;
    let shutdown = state.shutdown.clone();
    tokio::task::spawn_blocking(move || {
        // the runtime waits for blocking work at exit, so a pass stops early
        let stop = || *shutdown.borrow();
        run_pass(&home, &root, &config, mode, &stop)
    })
    .await
    .map_err(|error| AppError::Internal {
        message: format!("retention pass: {error}"),
    })?
}

/// A requested pass, then its vacuum, both inside the retention task so no
/// other pass opens the database while the vacuum holds it
async fn requested(
    state: &AppState,
    mode: PassMode,
    vacuum_after: bool,
) -> Result<GcReport, AppError> {
    let mut report = pass(state, mode).await?;
    if vacuum_after {
        report.database.vacuumed_bytes = Some(vacuum(state).await?);
    }
    Ok(report)
}

async fn vacuum(state: &AppState) -> Result<u64, AppError> {
    match state
        .store
        .call(|reply| StoreMsg::Vacuum { reply }, Some(VACUUM_TIMEOUT))
        .await
    {
        Ok(ractor::rpc::CallResult::Success(result)) => result,
        Ok(ractor::rpc::CallResult::Timeout) => Err(AppError::DaemonBusy),
        Ok(ractor::rpc::CallResult::SenderError) => Err(AppError::Internal {
            message: "vacuum reply dropped".into(),
        }),
        Err(error) => Err(error.into()),
    }
}

/// Serve `gc` requests, and prune on the configured interval, until the
/// daemon stops
pub(crate) async fn run(state: AppState, requests: PassRequests) {
    let PassRequests(mut requests) = requests;
    let interval = state.retention.config.interval;
    let mut next = interval.map(|_| Instant::now() + FIRST_PASS_DELAY);
    loop {
        let due = async {
            match next {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            request = requests.recv() => {
                let Some(request) = request else { return };
                // the caller may have gone; its pass still counts
                let report = requested(&state, request.mode, request.vacuum).await;
                let _ = request.reply.send(report);
            }
            () = due => {
                match pass(&state, PassMode::Apply).await {
                    Ok(report) => log_pass(&report),
                    Err(error) => warn!(%error, "retention pass failed"),
                }
                next = interval.map(|interval| Instant::now() + interval);
            }
        }
    }
}

fn log_pass(report: &GcReport) {
    let pruned: usize = report.kinds.iter().map(|kind| kind.pruned).sum();
    if pruned > 0 {
        info!(
            pruned,
            freed_bytes = report.freed(),
            "retention pruned finished output"
        );
    }
    for failure in &report.failures {
        warn!(path = %failure.path.display(), error = %failure.error, "retention could not remove");
    }
}
