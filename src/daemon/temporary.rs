//! A temporary machine's side of its pool: join the coordinator, renew the
//! lease while the machine is useful, and release it once idle
//!
//! The daemon reads the lease cloudlease wrote at boot, joins the pool's
//! coordinator with the single-use token from its config, stores the
//! variables the coordinator hands over, and offers the pool's CI class.
//! Each round it decides from what runs here whether to renew, withhold, or
//! release; see [`crate::pool::renewal`]. Before an idle release it drains
//! itself and checks again, so a job placed in that instant keeps it alive

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use super::queue_api::forward;
use crate::config::TemporaryConfig;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::address::MachineAddress;
use crate::fleet::http::ClusterClient;
use crate::pool::cloudlease::{CloudleaseError, LeaseCredential, SelfRelease};
use crate::pool::renewal::{Activity, IdleClock, Limits, Renewal};
use crate::pool::{JoinAnswer, JoinRequest};
use crate::store::queue::interface::QueueRequest;

/// Wait between attempts to read the lease or join the coordinator
const RETRY: Duration = Duration::from_secs(5);

const DRAIN_REASON: &str = "temporary machine releasing: idle";

/// What other parts of the daemon read about this machine's lease
///
/// A watch channel rather than a lock, since the daemon holds no shared
/// lock; the value is when cloudlease deletes this machine
#[derive(Clone, Default)]
pub struct Member(Option<Arc<watch::Sender<Option<DateTime<Utc>>>>>);

impl Member {
    /// A handle for a daemon with `[temporary]`, or an empty one
    #[must_use]
    pub fn new(temporary: bool) -> Self {
        Self(temporary.then(|| Arc::new(watch::Sender::new(None))))
    }

    /// When cloudlease deletes this machine, once its lease is known
    #[must_use]
    pub fn ends_at(&self) -> Option<DateTime<Utc>> {
        *self.0.as_ref()?.borrow()
    }

    fn set_ends_at(&self, at: DateTime<Utc>) {
        if let Some(sender) = &self.0 {
            sender.send_replace(Some(at));
        }
    }
}

/// Join, then renew or release until the lease ends
pub(super) async fn run(state: AppState, config: TemporaryConfig) {
    let lease = read_lease(&config).await;
    state.temporary.set_ends_at(lease.hard_deadline_at);
    let answer = join_until_accepted(&state, &config, &lease).await;
    if let Err(error) = apply(&state, &config, &answer).await {
        warn!("temporary machine setup: {error}");
    }
    info!(pool = %answer.pool, "temporary machine joined its pool");

    let limits = Limits {
        idle_limit: answer.idle_limit,
        stall_after: answer.stall_after,
    };
    let mut clock = IdleClock::started(Utc::now());
    let mut withheld = false;
    let mut tick = tokio::time::interval(config.renew_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let activity = match activity(&state).await {
            Ok(activity) => activity,
            Err(error) => {
                warn!("temporary machine activity: {error}");
                continue;
            }
        };
        match clock.observe(&activity, Utc::now(), limits) {
            Renewal::Renew => {
                withheld = false;
                if renew(&lease).await == LeaseEnd::Ended {
                    break;
                }
            }
            Renewal::Withhold { stalled } => {
                if !withheld {
                    warn!(stalled, "every run stalled: not renewing the lease");
                }
                withheld = true;
            }
            Renewal::Release { idle_since } => {
                if release_idle(&state, &lease, idle_since).await == LeaseEnd::Ended {
                    break;
                }
            }
        }
    }
    info!("lease ended; cloudlease deletes this machine");
}

async fn read_lease(config: &TemporaryConfig) -> LeaseCredential {
    loop {
        match LeaseCredential::read(&config.lease_file) {
            Ok(lease) => return lease,
            Err(error) => warn!("temporary machine lease: {error}"),
        }
        tokio::time::sleep(RETRY).await;
    }
}

async fn join_until_accepted(
    state: &AppState,
    config: &TemporaryConfig,
    lease: &LeaseCredential,
) -> JoinAnswer {
    loop {
        match join(state, config, lease).await {
            Ok(answer) => return answer,
            Err(error) => warn!("join pool {}: {error}", config.pool),
        }
        tokio::time::sleep(RETRY).await;
    }
}

async fn join(
    state: &AppState,
    config: &TemporaryConfig,
    lease: &LeaseCredential,
) -> Result<JoinAnswer, AppError> {
    let address = state
        .web
        .map(MachineAddress::from_socket)
        .ok_or_else(|| AppError::Usage {
            message: "a temporary machine needs its web listener on its tailnet address".into(),
        })?;
    let request = JoinRequest {
        api_version: API_VERSION,
        pool: config.pool.clone(),
        slot: config.slot,
        machine: state.machine.identity.machine,
        address,
        lease: lease.lease_id.clone(),
        join_token: config.join_token.clone(),
    };
    let response = ClusterClient::default()
        .post_json(&config.coordinator, "/v1/cluster/pool/join", &request)
        .await
        .map_err(|error| AppError::MachineUnavailable {
            machine: state.machine.identity.machine,
            message: error.to_string(),
        })?;
    if !response.status.is_success() {
        return Err(crate::client::map_error(response.status, &response.body));
    }
    Ok(serde_json::from_slice(&response.body)?)
}

/// Know the coordinator as a peer, store the pool's variables for runs, and
/// offer the pool's class
async fn apply(
    state: &AppState,
    config: &TemporaryConfig,
    answer: &JoinAnswer,
) -> Result<(), AppError> {
    // job events go back to the coordinator through this machine's Fleet
    // directory, which discovery alone may not fill on a fresh machine
    if let Some(fleet) = state.fleet.handle() {
        fleet.add_explicit(config.coordinator.clone()).await;
    }
    let path = state.home.pool_env_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let text: String = answer
        .env
        .iter()
        .map(|(name, value)| format!("{name}={value}\n"))
        .collect();
    write_private(&path, &text)?;
    let machine = state.machine.identity.machine;
    forward(
        state,
        machine,
        QueueRequest::CiCapacity {
            class: answer.pool.class().clone(),
            capacity: answer.slots,
        },
    )
    .await?;
    Ok(())
}

fn write_private(path: &std::path::Path, text: &str) -> Result<(), AppError> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(text.as_bytes())?;
    Ok(())
}

/// What runs or waits here now, with each run's last output time
async fn activity(state: &AppState) -> Result<Activity, AppError> {
    let stored = call(&state.store, |reply| StoreMsg::TemporaryActivity { reply }).await?;
    let home = state.home.clone();
    let runs = tokio::task::spawn_blocking(move || {
        stored
            .tasks
            .iter()
            .map(|(task, created)| {
                let output = home.task_paths(*task).output;
                super::actors::task::last_output_at(&output)
                    .map_or(*created, |last| last.max(*created))
            })
            .collect()
    })
    .await
    .map_err(|error| AppError::Internal {
        message: format!("activity thread: {error}"),
    })?;
    Ok(Activity {
        runs,
        queued_jobs: stored.queued_jobs,
        undelivered: stored.undelivered,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseEnd {
    Live,
    Ended,
}

async fn renew(lease: &LeaseCredential) -> LeaseEnd {
    let call = lease.clone();
    let renewed = tokio::task::spawn_blocking(move || call.renew())
        .await
        .unwrap_or_else(|error| {
            Err(CloudleaseError::Unavailable {
                message: error.to_string(),
            })
        });
    match renewed {
        Ok(_) => LeaseEnd::Live,
        Err(error) if error.is_gone() => {
            warn!("lease renewal refused: {error}");
            LeaseEnd::Ended
        }
        Err(error) => {
            warn!("lease renewal failed: {error}");
            LeaseEnd::Live
        }
    }
}

/// Drain, make sure nothing arrived, then release the lease
async fn release_idle(
    state: &AppState,
    lease: &LeaseCredential,
    idle_since: DateTime<Utc>,
) -> LeaseEnd {
    let machine = state.machine.identity.machine;
    let drain = |reason: Option<&str>| {
        forward(
            state,
            machine,
            QueueRequest::CiDrain {
                reason: reason.map(str::to_owned),
            },
        )
    };
    if let Err(error) = drain(Some(DRAIN_REASON)).await {
        warn!("drain before release: {error}");
        return LeaseEnd::Live;
    }
    // a job placed just before the drain is in the queue by now
    let busy = activity(state)
        .await
        .map_or(true, |activity| activity != Activity::default());
    if busy {
        if let Err(error) = drain(None).await {
            warn!("reopen after a late job: {error}");
        }
        return LeaseEnd::Live;
    }

    let call = lease.clone();
    let released = tokio::task::spawn_blocking(move || call.release(SelfRelease::Idle))
        .await
        .unwrap_or_else(|error| {
            Err(CloudleaseError::Unavailable {
                message: error.to_string(),
            })
        });
    match released {
        Ok(()) => {
            info!(%idle_since, "idle: released the lease");
            LeaseEnd::Ended
        }
        Err(error) if error.is_gone() => LeaseEnd::Ended,
        Err(error) => {
            // stay drained and idle; the next round releases again
            warn!("lease release failed: {error}");
            LeaseEnd::Live
        }
    }
}
