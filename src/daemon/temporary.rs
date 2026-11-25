//! A temporary machine's side of its pool: join the coordinator, renew the
//! lease while the machine is useful, and release it once idle
//!
//! The daemon reads the lease cloudlease wrote at boot, joins the pool's
//! coordinator with the single-use token from its config, stores the
//! variables the coordinator hands over, and offers the pool's CI class.
//! Each round it decides from what runs here whether to renew, withhold, or
//! release; see [`crate::pool::renewal`]. Past its idle limit it keeps
//! renewing, and offering its class, while cloudlease's last renew answer
//! says the provider's paid time runs on. Before an idle release it stops
//! offering the class and checks again, so a job placed in that instant
//! keeps it alive and none can be queued after the check
//!
//! When `lease.json` says the provider gives notice before it ends the
//! machine, as AWS does for a Spot interruption, a watcher polls for it. On a
//! notice the machine stops offering the class for good, writes the notice
//! where its runs find it (`HOMEBASED_INTERRUPTION_FILE`), and tells the
//! coordinator, which queues its jobs again for other machines at once

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use super::fleet_link::Grant;
use super::keyed_locks::KeyedLocks;
use super::pools::{JOIN_PATH, NOTICE_PATH};
use super::queue_api::forward;
use crate::config::TemporaryConfig;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::address::MachineAddress;
use crate::fleet::http::{ClusterClient, DEFAULT_MAX_BODY};
use crate::pool::cloudlease::{CloudleaseError, LeaseCredential, Renewed, SelfRelease};
use crate::pool::notice::{AwsImds, NoticeSource, POLL_INTERVAL, ProviderNotice};
use crate::pool::renewal::{Activity, IdleClock, IdleHold, Limits, Renewal};
use crate::pool::{InterruptionNotice, JoinAnswer, JoinRequest, NoticeAnswer};
use crate::queue::ci::SlotCapacity;
use crate::store::queue::interface::QueueRequest;

/// Wait between attempts to read the lease or join the coordinator
const RETRY: Duration = Duration::from_secs(5);

/// Time limit for one join call
const JOIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Wait between attempts at a renew that failed, within one round
const RENEW_RETRY: Duration = Duration::from_secs(5);

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

    /// Whether this daemon is a temporary machine
    #[must_use]
    pub fn is_temporary(&self) -> bool {
        self.0.is_some()
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
    let coordinator = start_link(&state, &config).await;
    let answer = join_until_accepted(&state, &config, &coordinator, &lease).await;
    info!(pool = %answer.pool, "temporary machine joined its pool");
    let gate = OfferGate::default();
    let mut offered = setup(&state, &gate, &coordinator, &answer).await;
    if let Some(source) = lease.interruption_notice {
        let watcher = NoticeWatcher {
            state: state.clone(),
            config: config.clone(),
            coordinator: coordinator.clone(),
            lease: lease.clone(),
            answer: answer.clone(),
            gate: gate.clone(),
        };
        tokio::spawn(watcher.run(source));
    }

    let limits = Limits {
        idle_limit: answer.idle_limit,
        stall_after: answer.stall_after,
    };
    let mut clock = IdleClock::started(Utc::now());
    let mut withheld = false;
    // from the last renew that answered; an older answer names the same paid
    // hour or one already over, so keeping it after a failed renew is safe
    let mut hold = IdleHold::Unknown;
    let mut held = false;
    let round = lease.renew_interval(config.renew_interval);
    let mut tick = tokio::time::interval(round);
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
        let decision = clock.observe(&activity, Utc::now(), limits, hold);
        // a setup or a reopen that failed is tried again each round the
        // machine is not releasing; until then it takes no CI job
        if !offered && !matches!(decision, Renewal::Release { .. }) {
            offered = setup(&state, &gate, &coordinator, &answer).await;
        }
        match decision {
            Renewal::Renew | Renewal::Hold { .. } => {
                if let Renewal::Hold { until } = decision
                    && !held
                {
                    info!(%until, "idle past its limit: staying until its paid time ends");
                }
                held = matches!(decision, Renewal::Hold { .. });
                withheld = false;
                match renew(&lease, round).await {
                    RenewOutcome::Renewed(renewed) => {
                        hold = IdleHold::answered(renewed.idle_hold_until);
                    }
                    RenewOutcome::Failed => {}
                    RenewOutcome::Ended => {
                        close_class(&state, &gate, &answer).await;
                        break;
                    }
                }
            }
            Renewal::Withhold { stalled } => {
                if !withheld {
                    warn!(stalled, "every run stalled: not renewing the lease");
                }
                withheld = true;
            }
            Renewal::Release { idle_since } => {
                let end =
                    release_idle(&state, &gate, &lease, &answer, idle_since, &mut offered).await;
                if end == LeaseEnd::Ended {
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

/// Reach the coordinator over the Fleet link; it is at the returned
/// loopback address from then on
async fn start_link(state: &AppState, config: &TemporaryConfig) -> MachineAddress {
    loop {
        match dial_coordinator(state, config).await {
            Ok(address) => return address,
            Err(error) => warn!("fleet link to the coordinator: {error}"),
        }
        tokio::time::sleep(RETRY).await;
    }
}

/// Allow the coordinator on the link as a full Fleet peer, keep a
/// connection to it, and return where this machine's Fleet reaches it
async fn dial_coordinator(
    state: &AppState,
    config: &TemporaryConfig,
) -> Result<MachineAddress, AppError> {
    let link = state.fleet_link.as_ref().ok_or_else(|| AppError::Usage {
        message: "a temporary machine needs [fleet.iroh] to reach its coordinator".into(),
    })?;
    if state.web.is_none() {
        return Err(AppError::Usage {
            message: "a temporary machine needs its web listener for the coordinator's calls"
                .into(),
        });
    }
    let hub = config.hub.endpoint_addr();
    link.grant(hub.id, Grant::Peer);
    let address = link.forward_to(hub.id, config.link_listen).await?;
    // a machine reconnects promptly so it delivers what it owes while its
    // lease still runs
    link.dial(state, hub, super::fleet_link::RECONNECT);
    Ok(address)
}

async fn join_until_accepted(
    state: &AppState,
    config: &TemporaryConfig,
    coordinator: &MachineAddress,
    lease: &LeaseCredential,
) -> JoinAnswer {
    loop {
        match join(state, config, coordinator, lease).await {
            Ok(answer) => return answer,
            Err(error) => warn!("join pool {}: {error}", config.pool),
        }
        tokio::time::sleep(RETRY).await;
    }
}

async fn join(
    state: &AppState,
    config: &TemporaryConfig,
    coordinator: &MachineAddress,
    lease: &LeaseCredential,
) -> Result<JoinAnswer, AppError> {
    let request = JoinRequest {
        api_version: API_VERSION,
        pool: config.pool.clone(),
        slot: config.slot,
        machine: state.machine.identity.machine,
        lease: lease.lease_id.clone(),
        join_token: config.join_token.clone(),
    };
    // the first call may wait for the link to find a path through a relay
    let response = ClusterClient::new(JOIN_TIMEOUT, DEFAULT_MAX_BODY)
        .post_json(coordinator, JOIN_PATH, &request)
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

/// Apply the join answer; whether the machine now offers the pool's class
async fn setup(
    state: &AppState,
    gate: &OfferGate,
    coordinator: &MachineAddress,
    answer: &JoinAnswer,
) -> bool {
    match apply(state, gate, coordinator, answer).await {
        Ok(()) => true,
        Err(error) => {
            warn!("temporary machine setup: {error}");
            false
        }
    }
}

/// Know the coordinator as a peer, store the pool's variables for runs, and
/// offer the pool's class
async fn apply(
    state: &AppState,
    gate: &OfferGate,
    coordinator: &MachineAddress,
    answer: &JoinAnswer,
) -> Result<(), AppError> {
    // job events go back to the coordinator through this machine's Fleet
    // directory, which discovery alone may not fill on a fresh machine
    if let Some(fleet) = state.fleet.handle() {
        fleet.add_explicit(coordinator.clone()).await;
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
    gate.offer(state, answer, answer.slots).await
}

/// Changes to how many slots of the pool's class this machine offers, one
/// at a time, so none reopens the class after a notice closed it for good
#[derive(Clone, Default)]
struct OfferGate {
    // one key: the machine offers one class, and the daemon allows no other mutex
    changes: KeyedLocks<()>,
    closed: Arc<AtomicBool>,
}

impl OfferGate {
    /// Offer `capacity` slots, unless a notice closed the class for good;
    /// closing it always applies
    async fn offer(
        &self,
        state: &AppState,
        answer: &JoinAnswer,
        capacity: SlotCapacity,
    ) -> Result<(), AppError> {
        let _change = self.changes.lock(()).await;
        if self.closed.load(Ordering::Acquire) && capacity.get() > 0 {
            return Err(AppError::Usage {
                message: "the provider gave notice that it ends this machine".into(),
            });
        }
        offer(state, answer, capacity).await
    }

    /// Stop offering the class, and never offer it again
    async fn close_for_good(&self, state: &AppState, answer: &JoinAnswer) -> Result<(), AppError> {
        let _change = self.changes.lock(()).await;
        self.closed.store(true, Ordering::Release);
        offer(state, answer, SlotCapacity::default()).await
    }
}

/// Set how many slots of the pool's class this machine offers
async fn offer(
    state: &AppState,
    answer: &JoinAnswer,
    capacity: SlotCapacity,
) -> Result<(), AppError> {
    let machine = state.machine.identity.machine;
    forward(
        state,
        machine,
        QueueRequest::CiCapacity {
            class: answer.pool.class().clone(),
            capacity,
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

/// How a round's renew went
#[derive(Debug, Clone, PartialEq, Eq)]
enum RenewOutcome {
    Renewed(Renewed),
    /// Every attempt this round failed; the next round tries again
    Failed,
    Ended,
}

/// Renew the lease, trying again within this round when a call fails
///
/// cloudlease's heartbeat window is a few renew rounds long, so a brief
/// outage must not wait out whole rounds before the next attempt
async fn renew(lease: &LeaseCredential, round: Duration) -> RenewOutcome {
    let started = tokio::time::Instant::now();
    loop {
        let call = lease.clone();
        let renewed = tokio::task::spawn_blocking(move || call.renew())
            .await
            .unwrap_or_else(|error| {
                Err(CloudleaseError::Unavailable {
                    message: error.to_string(),
                })
            });
        let error = match renewed {
            Ok(renewed) => return RenewOutcome::Renewed(renewed),
            Err(error) if error.is_gone() => {
                warn!("lease renewal refused: {error}");
                return RenewOutcome::Ended;
            }
            Err(error) => error,
        };

        // the next round tries again
        if started.elapsed() + RENEW_RETRY >= round {
            warn!("lease renewal failed: {error}");
            return RenewOutcome::Failed;
        }
        warn!("lease renewal failed, retrying: {error}");
        tokio::time::sleep(RENEW_RETRY).await;
    }
}

/// Stop offering the pool's class once cloudlease ended the lease, so no
/// job is placed on a machine about to be deleted; running jobs go on
/// until it is
async fn close_class(state: &AppState, gate: &OfferGate, answer: &JoinAnswer) {
    // the queue is this daemon's own, so a failure is brief; the machine is
    // deleted soon whatever happens
    while let Err(error) = gate.offer(state, answer, SlotCapacity::default()).await {
        warn!("close the class after the lease ended: {error}");
        tokio::time::sleep(RETRY).await;
    }
}

/// Stop offering the class, make sure nothing arrived, then release the lease
async fn release_idle(
    state: &AppState,
    gate: &OfferGate,
    lease: &LeaseCredential,
    answer: &JoinAnswer,
    idle_since: DateTime<Utc>,
    offered: &mut bool,
) -> LeaseEnd {
    // with no slots of its class the queue refuses a job of it in the same
    // store write that would queue it, so none can land after the check below
    if let Err(error) = gate.offer(state, answer, SlotCapacity::default()).await {
        warn!("close the class before release: {error}");
        return LeaseEnd::Live;
    }
    *offered = false;
    // a job queued just before the class closed is in the queue by now
    let busy = activity(state)
        .await
        .map_or(true, |activity| activity != Activity::default());
    if busy {
        // a failed reopen is tried again next round
        *offered = gate
            .offer(state, answer, answer.slots)
            .await
            .inspect_err(|error| warn!("reopen after a late job: {error}"))
            .is_ok();
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

/// Watches for the provider's notice that it ends this machine, and acts on
/// the first one
struct NoticeWatcher {
    state: AppState,
    config: TemporaryConfig,
    coordinator: MachineAddress,
    lease: LeaseCredential,
    answer: JoinAnswer,
    gate: OfferGate,
}

impl NoticeWatcher {
    async fn run(self, source: NoticeSource) {
        let NoticeSource::AwsImds = source;
        let notice = self.wait_for_notice().await;
        warn!(action = %notice.action, ends_at = %notice.ends_at, "the provider gave notice that it ends this machine");

        // first the file, so a run can start saving its work at once
        if let Err(error) = write_notice(&self.state, &notice) {
            warn!("write the interruption notice for runs: {error}");
        }
        while let Err(error) = self.gate.close_for_good(&self.state, &self.answer).await {
            warn!("close the class after the interruption notice: {error}");
            tokio::time::sleep(RETRY).await;
        }
        self.report(&notice).await;
    }

    /// Poll the metadata service until it shows a notice
    async fn wait_for_notice(&self) -> ProviderNotice {
        let mut imds = AwsImds::new(&self.config.imds_endpoint);
        let mut failing = false;
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let polled = tokio::task::spawn_blocking(move || {
                let action = imds.instance_action();
                (imds, action)
            })
            .await;
            let action = match polled {
                Ok((back, action)) => {
                    imds = back;
                    action
                }
                Err(error) => {
                    imds = AwsImds::new(&self.config.imds_endpoint);
                    Err(format!("metadata poll thread: {error}"))
                }
            };
            match action {
                Ok(Some(notice)) => return notice,
                Ok(None) => failing = false,
                // warn once per outage, since the poll runs every few seconds
                Err(error) if !failing => {
                    warn!("read the Spot interruption notice: {error}");
                    failing = true;
                }
                Err(_) => {}
            }
        }
    }

    /// Tell the coordinator until it answers, it refuses, or the machine
    /// should be gone
    async fn report(&self, notice: &ProviderNotice) {
        let body = InterruptionNotice {
            api_version: API_VERSION,
            pool: self.answer.pool.clone(),
            slot: self.config.slot,
            lease: self.lease.lease_id.clone(),
            action: notice.action.clone(),
            ends_at: notice.ends_at,
        };
        let give_up = notice.ends_at + chrono::Duration::minutes(1);
        while Utc::now() < give_up {
            let response = ClusterClient::new(JOIN_TIMEOUT, DEFAULT_MAX_BODY)
                .post_json(&self.coordinator, NOTICE_PATH, &body)
                .await;
            let error = match response {
                Ok(response) if response.status.is_success() => {
                    let queued = serde_json::from_slice::<NoticeAnswer>(&response.body)
                        .map_or(0, |answer| answer.queued.len());
                    info!(queued, "the coordinator queued this machine's jobs again");
                    return;
                }
                Ok(response) if response.status.is_client_error() => {
                    let error = crate::client::map_error(response.status, &response.body);
                    warn!("the coordinator refused the interruption notice: {error}");
                    return;
                }
                Ok(response) => format!("status {}", response.status),
                Err(error) => error.to_string(),
            };
            warn!("report the interruption notice, retrying: {error}");
            tokio::time::sleep(RETRY).await;
        }
    }
}

/// Write the notice where runs find it, whole or not at all
fn write_notice(state: &AppState, notice: &ProviderNotice) -> Result<(), AppError> {
    let path = state.home.interruption_notice_path();
    let partial = path.with_extension("json.partial");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&partial, serde_json::to_vec(notice)?)?;
    std::fs::rename(&partial, &path)?;
    Ok(())
}
