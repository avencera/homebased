//! Pool coordinator: the `pool` routes, temporary machines' joins, and the
//! loop that asks cloudlease for machines and drops the ones it deleted
//!
//! Each pass reads every pool's cloudlease run, moves each machine through
//! requested, booting, joined, releasing, and released from what cloudlease
//! reports, asks for machines while ready jobs of the pool's class find no
//! free slot, and ends the pool at its deadline. Placement of the jobs is the
//! CI queue's; this loop only makes and removes the machines it places on.
//! See `docs/temporary-machines.md`

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use super::dependencies::held_ci_routes;
use super::fleet_link::{FleetLink, Grant, LinkPeer};
use crate::config::PoolsConfig;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::advertisement::MACHINE_PROBE_PATH;
use crate::pool::boot::BootPlan;
use crate::pool::cloudlease::{CloudleaseClient, CloudleaseError, Lease, LeaseState, Run};
use crate::pool::{
    DEFAULT_BOOT_TIMEOUT, DEFAULT_IDLE_LIMIT, DEFAULT_STALL_AFTER, JoinAnswer, JoinRequest,
    JoinToken, JoinedMachine, MAX_LIFETIME, MAX_MACHINES, MachineClass, MachineLease,
    MachineReleased, MachineState, Pool, PoolCode, PoolEnd, PoolError, PoolMachine, PoolName,
    PoolSettings, PoolState, PoolView, ReleaseReason, SlotId, parse_env_file,
};
use crate::queue::ci::{CiSpec, SlotCapacity};
use crate::queue::delivery::HeldJobEnding;
use crate::store::PoolInsert;

/// Socket routes that change pools: create and release
pub fn socket_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/pools", post(create))
        .route("/v1/pools/{name}/release", post(release))
}

/// Read routes, safe on the TCP listener
pub fn read_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/pools", get(list))
        .route("/v1/pools/{name}", get(show))
}

/// Route a temporary machine joins through, over the Fleet link
pub const JOIN_PATH: &str = "/v1/cluster/pool/join";

/// What an endpoint with no grant may call on a coordinator: the join only
pub(super) fn enrollment_routes() -> Router<AppState> {
    Router::new().route(JOIN_PATH, post(join))
}

/// What a joined temporary machine may call on its coordinator: join again,
/// the Fleet probe, and event delivery for its own work; nothing that starts
/// work, sends messages, or reads other machines' tasks
pub(super) fn member_routes(state: &AppState) -> Router<AppState> {
    let routes = enrollment_routes()
        .route(
            "/v1/cluster/job-events",
            post(super::queue_api::member_job_event),
        )
        .route("/v1/cluster/events", post(super::cluster::member_event));
    let Some(fleet) = state.fleet.handle() else {
        return routes;
    };
    let fleet = fleet.clone();
    routes.route(
        MACHINE_PROBE_PATH,
        get(move || super::cluster::machine(fleet.clone())),
    )
}

/// Allow a joined machine's endpoint on the link as a pool member, and
/// return where this daemon's Fleet reaches it
async fn admit_member(
    link: &FleetLink,
    endpoint: iroh::EndpointId,
    machine: crate::machine::MachineId,
) -> Result<crate::fleet::address::MachineAddress, AppError> {
    link.grant(endpoint, Grant::PoolMember { machine });
    link.forward_to(endpoint, SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
}

fn configured(state: &AppState) -> Result<&PoolsConfig, AppError> {
    state.pools.as_ref().ok_or_else(|| {
        PoolError::new(
            PoolCode::PoolsNotConfigured,
            "this daemon has no [pools] config, so it cannot reach cloudlease",
        )
        .into()
    })
}

/// Body of `POST /v1/pools`
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    api_version: u32,
    name: PoolName,
    machine_class: MachineClass,
    max_machines: u32,
    #[serde(with = "humantime_serde")]
    deadline: Duration,
    #[serde(default)]
    slots: Option<SlotCapacity>,
    #[serde(default, with = "humantime_serde")]
    idle_limit: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    stall_after: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    boot_timeout: Option<Duration>,
    #[serde(default)]
    env_file: Option<std::path::PathBuf>,
}

impl CreateBody {
    fn into_pool(self) -> Result<Pool, AppError> {
        let usage = |message: String| AppError::Usage { message };
        if self.api_version != API_VERSION {
            return Err(usage(format!("api_version must be {API_VERSION}")));
        }
        if !(1..=MAX_MACHINES).contains(&self.max_machines) {
            return Err(usage(format!("--max must be 1 through {MAX_MACHINES}")));
        }
        if self.deadline.is_zero() || self.deadline > MAX_LIFETIME {
            return Err(usage(
                "--deadline must be more than 0 and at most 24h".into(),
            ));
        }
        let slots = self.slots.unwrap_or_else(|| SlotCapacity::default().grow());
        if slots.get() == 0 {
            return Err(usage("--slots must be at least 1".into()));
        }
        if let Some(path) = &self.env_file {
            read_env(path).map_err(usage)?;
        }
        let now = Utc::now();
        let deadline = chrono::Duration::from_std(self.deadline)
            .map_err(|_| usage("--deadline does not fit the clock".into()))?;
        Ok(Pool {
            name: self.name,
            settings: PoolSettings {
                machine_class: self.machine_class,
                max_machines: self.max_machines,
                deadline_at: now + deadline,
                slots,
                idle_limit: self.idle_limit.unwrap_or(DEFAULT_IDLE_LIMIT),
                stall_after: self.stall_after.unwrap_or(DEFAULT_STALL_AFTER),
                boot_timeout: self.boot_timeout.unwrap_or(DEFAULT_BOOT_TIMEOUT),
                env_file: self.env_file,
            },
            run_request: uuid::Uuid::now_v7(),
            run: None,
            state: PoolState::Open,
            created_at: now,
            last_error: None,
        })
    }
}

fn read_env(path: &std::path::Path) -> Result<Vec<(String, String)>, String> {
    if !path.is_absolute() {
        return Err(format!(
            "--env-file must be absolute, got {}",
            path.display()
        ));
    }
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    parse_env_file(&text).map_err(|error| format!("{}: {error}", path.display()))
}

async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateBody>,
) -> Result<Json<Value>, AppError> {
    configured(&state)?;
    if state.fleet.handle().is_none() {
        return Err(AppError::Usage {
            message: "pools need fleet.enabled = true, so machines can join".into(),
        });
    }
    let pool = body.into_pool()?;
    let inserted = call(&state.store, |reply| StoreMsg::InsertPool {
        pool: Box::new(pool),
        reply,
    })
    .await?;
    let pool = match inserted {
        PoolInsert::Created(pool) => pool,
        PoolInsert::Exists(existing) => {
            return Err(PoolError::new(
                PoolCode::PoolExists,
                format!(
                    "pool {} is {}; release it or pick another name",
                    existing.name,
                    existing.state.as_str()
                ),
            )
            .into());
        }
    };
    info!(pool = %pool.name, "pool created");
    Ok(Json(view_json(PoolView {
        pool,
        machines: Vec::new(),
    })?))
}

async fn list(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    let pools = call(&state.store, |reply| StoreMsg::Pools { reply }).await?;
    let mut views = Vec::with_capacity(pools.len());
    for pool in pools {
        let name = pool.name.clone();
        let machines = call(&state.store, |reply| StoreMsg::PoolMachines {
            pool: name,
            reply,
        })
        .await?;
        views.push(PoolView { pool, machines });
    }
    Ok(Json(json!({ "api_version": API_VERSION, "pools": views })))
}

async fn show(
    State(state): State<AppState>,
    Path(name): Path<PoolName>,
) -> Result<Json<Value>, AppError> {
    let pool = read_pool(&state, &name).await?;
    let machines = call(&state.store, |reply| StoreMsg::PoolMachines {
        pool: name,
        reply,
    })
    .await?;
    Ok(Json(view_json(PoolView { pool, machines })?))
}

fn view_json(view: PoolView) -> Result<Value, AppError> {
    let mut value = serde_json::to_value(view)?;
    value["api_version"] = json!(API_VERSION);
    Ok(value)
}

async fn read_pool(state: &AppState, name: &PoolName) -> Result<Pool, AppError> {
    let name = name.clone();
    let missing = PoolError::new(PoolCode::PoolNotFound, format!("no pool named {name}"));
    call(&state.store, |reply| StoreMsg::Pool { name, reply })
        .await?
        .ok_or_else(|| missing.into())
}

/// Stop asking for machines and release every machine of the pool; the
/// reconciler does the cloudlease calls
async fn release(
    State(state): State<AppState>,
    Path(name): Path<PoolName>,
) -> Result<Json<Value>, AppError> {
    let mut pool = read_pool(&state, &name).await?;
    if matches!(pool.state, PoolState::Open) {
        pool.state = PoolState::Releasing {
            end: PoolEnd::Requested,
        };
        save_pool(&state, &pool).await?;
        info!(pool = %pool.name, "pool release requested");
    }
    let machines = call(&state.store, |reply| StoreMsg::PoolMachines {
        pool: name,
        reply,
    })
    .await?;
    Ok(Json(view_json(PoolView { pool, machines })?))
}

/// Whether a CI job may wait for a machine of the pool its class names
///
/// A job of an open pool's class is admitted even with no machine yet, as
/// long as its kill limit ends before the pool's deadline; a releasing pool
/// takes no new job. Without a pool of that class the usual offer check runs
pub(super) async fn admit_pool_job(state: &AppState, ci: &CiSpec) -> Result<bool, AppError> {
    let name = PoolName::from_class(ci.class.clone());
    let pool = call(&state.store, |reply| StoreMsg::Pool { name, reply }).await?;
    let Some(pool) = pool else {
        return Ok(false);
    };
    match pool.state {
        PoolState::Released { .. } => Ok(false),
        PoolState::Releasing { .. } => Err(PoolError::new(
            PoolCode::PoolEnded,
            format!("pool {} is releasing and takes no new jobs", pool.name),
        )
        .into()),
        PoolState::Open => {
            let kill = chrono::Duration::from_std(ci.kill_after.get()).unwrap_or_default();
            let ends = Utc::now() + kill;
            if ends > pool.settings.deadline_at {
                return Err(PoolError::new(
                    PoolCode::PoolDeadlineTooClose,
                    format!(
                        "the job's --kill-after runs to {ends}, past pool {}'s deadline {}; \
                         its machine would be deleted under it",
                        pool.name, pool.settings.deadline_at
                    ),
                )
                .into());
            }
            Ok(true)
        }
    }
}

/// `POST /v1/cluster/pool/join` over the Fleet link: a temporary machine
/// joins its coordinator
///
/// The machine proves it is the one this coordinator asked for with the
/// single-use token from its boot data, which binds the link's endpoint id to
/// its lease, and gets its settings and the pool's variables back. A retry
/// from the same endpoint gets the same answer; any other endpoint is refused
pub(super) async fn join(
    State(state): State<AppState>,
    Extension(peer): Extension<LinkPeer>,
    Json(request): Json<JoinRequest>,
) -> Result<Json<JoinAnswer>, AppError> {
    configured(&state)?;
    let Some(link) = state.fleet_link.clone() else {
        return Err(PoolError::new(
            PoolCode::PoolsNotConfigured,
            "this daemon has no [fleet.iroh] link",
        )
        .into());
    };
    let refused =
        |message: String| -> AppError { PoolError::new(PoolCode::JoinRefused, message).into() };
    if request.api_version != API_VERSION {
        return Err(refused(format!("api_version must be {API_VERSION}")));
    }
    let slot = request.slot;
    let _guard = state.locks.pool_joins.lock(slot).await;
    let Some(mut machine) =
        call(&state.store, |reply| StoreMsg::PoolMachine { slot, reply }).await?
    else {
        return Err(refused(format!("no pool machine {slot}")));
    };
    if machine.pool != request.pool || machine.join_token != request.join_token.hash() {
        return Err(refused(format!(
            "pool machine {slot} does not match this join"
        )));
    }
    let pool = read_pool(&state, &machine.pool).await?;

    match &machine.state {
        MachineState::Booting { lease } if lease.lease == request.lease => {
            if !claims_new_machine(&state, slot, request.machine).await? {
                return Err(refused(format!(
                    "machine {} is already known to this coordinator",
                    request.machine
                )));
            }
            let address = admit_member(&link, peer.endpoint, request.machine).await?;
            let joined = JoinedMachine {
                machine: request.machine,
                endpoint: peer.endpoint,
                address: address.clone(),
                joined_at: Utc::now(),
            };
            machine.state = MachineState::Joined {
                lease: lease.clone(),
                joined,
            };
            if let Err(error) = save_machine(&state, &machine).await {
                link.revoke(peer.endpoint);
                return Err(error);
            }
            if let Some(fleet) = state.fleet.handle() {
                fleet.add_explicit(address).await;
            }
            info!(pool = %pool.name, machine = %machine.name, "temporary machine joined");
        }
        MachineState::Joined { lease, joined }
            if lease.lease == request.lease
                && joined.machine == request.machine
                && joined.endpoint == peer.endpoint => {}
        MachineState::Requested => {
            return Err(refused(format!(
                "pool machine {slot} has no lease recorded yet; retry"
            )));
        }
        other => {
            return Err(refused(format!(
                "pool machine {slot} is {} and cannot join",
                other.as_str()
            )));
        }
    }

    let env = match &pool.settings.env_file {
        Some(path) => read_env(path).map_err(|error| {
            warn!(pool = %pool.name, "pool env file: {error}");
            refused(format!(
                "the coordinator could not read the pool's env file: {error}"
            ))
        })?,
        None => Vec::new(),
    };
    Ok(Json(JoinAnswer {
        api_version: API_VERSION,
        pool: pool.name,
        slots: pool.settings.slots,
        idle_limit: pool.settings.idle_limit,
        stall_after: pool.settings.stall_after,
        env,
    }))
}

/// Whether a joining machine's id is one this coordinator has never seen
///
/// The machine names its own id, and when its lease ends the coordinator
/// fails the jobs and forgets the Fleet entry of that id, so the id of this
/// daemon, a Fleet peer, or another pool machine must never be bound to a lease
async fn claims_new_machine(
    state: &AppState,
    slot: SlotId,
    machine: crate::machine::MachineId,
) -> Result<bool, AppError> {
    if machine == state.machine.identity.machine {
        return Ok(false);
    }
    if let Some(fleet) = state.fleet.handle()
        && fleet
            .peers()
            .await
            .iter()
            .any(|peer| peer.machine == machine)
    {
        return Ok(false);
    }

    let pools = call(&state.store, |reply| StoreMsg::Pools { reply }).await?;
    for pool in pools {
        let name = pool.name;
        let machines = call(&state.store, |reply| StoreMsg::PoolMachines {
            pool: name,
            reply,
        })
        .await?;
        let taken = machines.iter().any(|other| {
            other.slot != slot
                && other
                    .state
                    .joined()
                    .is_some_and(|joined| joined.machine == machine)
        });
        if taken {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn save_pool(state: &AppState, pool: &Pool) -> Result<(), AppError> {
    let pool = Box::new(pool.clone());
    call(&state.store, |reply| StoreMsg::UpdatePool { pool, reply }).await
}

async fn save_machine(state: &AppState, machine: &PoolMachine) -> Result<(), AppError> {
    let machine = Box::new(machine.clone());
    call(&state.store, |reply| StoreMsg::UpdatePoolMachine {
        machine,
        reply,
    })
    .await
}

/// Run blocking cloudlease calls off the async runtime
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, CloudleaseError> + Send + 'static,
) -> Result<T, CloudleaseError> {
    tokio::task::spawn_blocking(work)
        .await
        .unwrap_or_else(|error| {
            Err(CloudleaseError::Unavailable {
                message: format!("cloudlease call thread: {error}"),
            })
        })
}

/// Reconcile every pool that is not released, forever
pub(super) async fn run(state: AppState) {
    let Some(config) = state.pools.clone() else {
        return;
    };
    let client =
        CloudleaseClient::new(&config.cloudlease_url, config.cloudlease_token_file.clone());
    let mut tick = tokio::time::interval(config.reconcile_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let pools = match call(&state.store, |reply| StoreMsg::Pools { reply }).await {
            Ok(pools) => pools,
            Err(error) => {
                warn!("pool reconcile: {error}");
                continue;
            }
        };
        for pool in pools {
            if matches!(pool.state, PoolState::Released { .. }) {
                continue;
            }
            let name = pool.name.clone();
            let pass = Pass {
                state: &state,
                client: &client,
                config: &config,
            };
            if let Err(error) = pass.reconcile(pool).await {
                warn!(pool = %name, "pool reconcile: {error}");
            }
        }
    }
}

struct Pass<'a> {
    state: &'a AppState,
    client: &'a CloudleaseClient,
    config: &'a PoolsConfig,
}

impl Pass<'_> {
    async fn reconcile(&self, mut pool: Pool) -> Result<(), AppError> {
        if matches!(pool.state, PoolState::Open) && Utc::now() >= pool.settings.deadline_at {
            pool.state = PoolState::Releasing {
                end: PoolEnd::Deadline,
            };
        }

        let run = match self.read_run(&mut pool).await {
            Ok(run) => run,
            Err(error) => {
                pool.last_error = Some(error.to_string());
                save_pool(self.state, &pool).await?;
                return Ok(());
            }
        };

        let name = pool.name.clone();
        let machines = call(&self.state.store, |reply| StoreMsg::PoolMachines {
            pool: name,
            reply,
        })
        .await?;
        let leases: BTreeMap<uuid::Uuid, &Lease> = run
            .as_ref()
            .map(|run| {
                run.machines
                    .iter()
                    .map(|lease| (lease.request_id, lease))
                    .collect()
            })
            .unwrap_or_default();
        let mut live = Vec::new();
        for machine in machines {
            if !machine.state.is_live() {
                continue;
            }
            let lease = leases.get(&machine.slot.as_uuid()).copied();
            let machine = self
                .sync_machine(&pool, run.as_ref(), machine, lease)
                .await?;
            if machine.state.is_live() {
                live.push(machine);
            }
        }

        match &pool.state {
            PoolState::Open => self.grow(&mut pool, run.as_ref(), &live).await?,
            PoolState::Releasing { end } => {
                let end = end.clone();
                self.wind_down(&mut pool, run.as_ref(), &live, end).await?;
            }
            PoolState::Released { .. } => {}
        }
        save_pool(self.state, &pool).await
    }

    /// The pool's run, created first if needed; `None` once there is no run
    /// to read because the pool ended before it had one
    async fn read_run(&self, pool: &mut Pool) -> Result<Option<Run>, CloudleaseError> {
        if pool.run.is_none() {
            if !matches!(pool.state, PoolState::Open) {
                return Ok(None);
            }
            let client = self.client.clone();
            let (request, name, class, max, deadline) = (
                pool.run_request,
                pool.name.clone(),
                pool.settings.machine_class.clone(),
                pool.settings.max_machines,
                pool.settings.deadline_at,
            );
            let created =
                blocking(move || client.create_run(request, &name, &class, max, deadline)).await;
            match created {
                Ok(run) => {
                    info!(pool = %pool.name, run = %run.run_id, "cloudlease run created");
                    pool.run = Some(run.run_id.clone());
                    pool.last_error = None;
                    return Ok(Some(run));
                }
                Err(error @ CloudleaseError::Refused { .. }) => {
                    pool.state = PoolState::Releasing {
                        end: refused_end(&error),
                    };
                    pool.last_error = Some(error.to_string());
                    return Ok(None);
                }
                Err(error) => return Err(error),
            }
        }
        let Some(run) = pool.run.clone() else {
            return Ok(None);
        };
        let client = self.client.clone();
        match blocking(move || client.run(&run)).await {
            Ok(run) => {
                pool.last_error = None;
                Ok(Some(run))
            }
            // a run cloudlease no longer has holds no machine
            Err(error) if error.is_gone() => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Move one machine to what cloudlease reports for its lease
    async fn sync_machine(
        &self,
        pool: &Pool,
        run: Option<&Run>,
        mut machine: PoolMachine,
        lease: Option<&Lease>,
    ) -> Result<PoolMachine, AppError> {
        let before = machine.state.clone();
        match (lease, &machine.state) {
            (Some(lease), _) if lease.state == LeaseState::Released => {
                let reason = local_reason(&machine.state)
                    .or_else(|| lease.release_reason())
                    .unwrap_or(ReleaseReason::Other("unknown".into()));
                let gone_at = lease.gone_at.unwrap_or_else(Utc::now);
                return self.finish(pool, machine, reason, gone_at).await;
            }
            // keep the reason this coordinator gave, if it gave one
            (Some(_), MachineState::Releasing { .. }) => {}
            (Some(lease), state) if lease.state == LeaseState::Releasing => {
                let reason = lease
                    .release_reason()
                    .unwrap_or(ReleaseReason::Other("unknown".into()));
                machine.state = MachineState::Releasing {
                    lease: machine_lease(lease),
                    joined: state.joined().cloned(),
                    reason,
                };
            }
            (Some(lease), MachineState::Requested) => {
                machine.state = MachineState::Booting {
                    lease: machine_lease(lease),
                };
            }
            (Some(lease), MachineState::Booting { .. }) => {
                let boot_age = (Utc::now() - lease.created_at).to_std().unwrap_or_default();
                if boot_age >= pool.settings.boot_timeout {
                    self.release_lease(&mut machine, ReleaseReason::BootTimeout)
                        .await;
                }
            }
            (Some(_), MachineState::Joined { .. } | MachineState::Released { .. }) => {}
            (None, MachineState::Requested) => {
                if run.is_some() {
                    self.create_machine(pool, &mut machine).await?;
                }
            }
            (None, _) if run.is_some() => {
                // cloudlease lists every lease of a run, released ones too, so
                // a lease missing from it no longer exists
                return self
                    .finish(
                        pool,
                        machine,
                        ReleaseReason::Other("lease_not_found".into()),
                        Utc::now(),
                    )
                    .await;
            }
            (None, _) if pool.run.is_some() => {
                // cloudlease no longer has the run at all
                return self
                    .finish(
                        pool,
                        machine,
                        ReleaseReason::Other("run_not_found".into()),
                        Utc::now(),
                    )
                    .await;
            }
            (None, _) => {}
        }

        if !matches!(pool.state, PoolState::Open)
            && matches!(
                machine.state,
                MachineState::Booting { .. } | MachineState::Joined { .. }
            )
        {
            self.release_lease(&mut machine, ReleaseReason::Requested)
                .await;
        }
        self.sync_link(&mut machine).await?;
        if machine.state != before {
            save_machine(self.state, &machine).await?;
        }
        Ok(machine)
    }

    /// Serve a joined machine on the link under its bound endpoint id, again
    /// after a coordinator restart, and stop serving any other
    async fn sync_link(&self, machine: &mut PoolMachine) -> Result<(), AppError> {
        let Some(link) = &self.state.fleet_link else {
            return Ok(());
        };
        let MachineState::Joined { joined, .. } = &mut machine.state else {
            if let Some(joined) = machine.state.joined() {
                link.revoke(joined.endpoint);
            }
            return Ok(());
        };

        let address = admit_member(link, joined.endpoint, joined.machine).await?;
        if address == joined.address {
            return Ok(());
        }
        if let Some(fleet) = self.state.fleet.handle() {
            fleet.remove_explicit(&joined.address).await;
            fleet.add_explicit(address.clone()).await;
        }
        joined.address = address;
        Ok(())
    }

    /// Ask cloudlease to release a machine's lease; a failed call is retried
    /// next pass, since the machine stays booting or joined
    async fn release_lease(&self, machine: &mut PoolMachine, reason: ReleaseReason) {
        let Some(lease) = machine.state.lease().cloned() else {
            return;
        };
        let client = self.client.clone();
        let id = lease.lease.clone();
        match blocking(move || client.release_lease(&id)).await {
            Ok(()) => {}
            Err(error) if error.is_gone() => {}
            Err(error) => {
                warn!(machine = %machine.name, "release lease: {error}");
                return;
            }
        }
        info!(machine = %machine.name, %reason, "temporary machine releasing");
        machine.state = MachineState::Releasing {
            lease,
            joined: machine.state.joined().cloned(),
            reason,
        };
    }

    /// cloudlease confirmed the machine gone: drop it from the Fleet and end
    /// the jobs it still held
    async fn finish(
        &self,
        pool: &Pool,
        mut machine: PoolMachine,
        reason: ReleaseReason,
        gone_at: chrono::DateTime<Utc>,
    ) -> Result<PoolMachine, AppError> {
        let lease = machine.state.lease().cloned();
        let joined = machine.state.joined().cloned();
        if let Some(joined) = &joined {
            if let Some(link) = &self.state.fleet_link {
                link.revoke(joined.endpoint);
            }
            if let Some(fleet) = self.state.fleet.handle() {
                fleet.remove_explicit(&joined.address).await;
                fleet.forget_machine(joined.machine).await;
            }
            let released = MachineReleased {
                pool: pool.name.clone(),
                machine: machine.name.clone(),
                output_lost: reason.may_lose_output(),
                reason: reason.clone(),
                hard_deadline_at: lease
                    .as_ref()
                    .map_or(pool.settings.deadline_at, |lease| lease.hard_deadline_at),
                gone_at,
            };
            let ended = call(&self.state.store, |reply| {
                StoreMsg::EndJobsOfReleasedMachine {
                    machine: joined.machine,
                    released: Box::new(released),
                    reply,
                }
            })
            .await?;
            if !ended.is_empty() {
                warn!(machine = %machine.name, jobs = ended.len(), %reason, "jobs ended with their machine");
            }
        }
        info!(machine = %machine.name, %reason, "temporary machine gone");
        machine.state = MachineState::Released {
            lease,
            joined,
            reason,
            gone_at,
        };
        save_machine(self.state, &machine).await?;
        Ok(machine)
    }

    /// Ask for machines while ready jobs of the pool's class have no slot to go to
    async fn grow(
        &self,
        pool: &mut Pool,
        run: Option<&Run>,
        live: &[PoolMachine],
    ) -> Result<(), AppError> {
        if run.is_none() {
            return Ok(());
        }
        let wanted = self.wanted_machines(pool, live).await?;
        for _ in 0..wanted {
            // a `pool release` saved since this pass read the pool stops it asking
            if !matches!(
                read_pool(self.state, &pool.name).await?.state,
                PoolState::Open
            ) {
                break;
            }
            let token = JoinToken::generate()?;
            let slot = SlotId::new();
            let mut machine = PoolMachine {
                slot,
                pool: pool.name.clone(),
                name: pool.name.machine_name(slot),
                join_token: token.hash(),
                requested_at: Utc::now(),
                state: MachineState::Requested,
            };
            // saved before the call, so a lost answer is retried with the same slot
            let saved = Box::new(machine.clone());
            call(&self.state.store, |reply| StoreMsg::InsertPoolMachine {
                machine: saved,
                reply,
            })
            .await?;
            let outcome = self.create_with_token(pool, &mut machine, &token).await?;
            if let Some(end) = outcome {
                pool.state = PoolState::Releasing { end };
                break;
            }
            if matches!(
                machine.state,
                MachineState::Requested | MachineState::Released { .. }
            ) {
                break;
            }
        }
        Ok(())
    }

    /// How many machines to ask for now: ready jobs with no slot, less the
    /// slots of machines still booting and free slots on joined ones, within
    /// the pool's limit
    async fn wanted_machines(&self, pool: &Pool, live: &[PoolMachine]) -> Result<u32, AppError> {
        let held = held_ci_routes(self.state, pool.name.class()).await?;
        let ready = u32::try_from(held.ready.len()).unwrap_or(u32::MAX);
        if ready == 0 {
            return Ok(0);
        }
        let slots = pool.settings.slots.get().max(1);
        let mut coming = 0_u32;
        for machine in live {
            match &machine.state {
                MachineState::Requested | MachineState::Booting { .. } => coming += slots,
                MachineState::Joined { joined, .. } => {
                    coming += self.free_slots(pool, joined).await;
                }
                MachineState::Releasing { .. } | MachineState::Released { .. } => {}
            }
        }
        let missing = ready.saturating_sub(coming);
        let room = pool
            .settings
            .max_machines
            .saturating_sub(u32::try_from(live.len()).unwrap_or(u32::MAX));
        Ok(missing.div_ceil(slots).min(room))
    }

    /// Free slots of the pool's class a joined machine offers now; one that
    /// does not answer offers none
    async fn free_slots(&self, pool: &Pool, joined: &JoinedMachine) -> u32 {
        // a machine that just joined is the Fleet's only once its address was
        // probed, and offers the class only once it applied the join answer;
        // it was asked for this pool, so count its slots rather than ask for
        // another machine in that window
        let Ok(status) = super::ci_api::machine_status(self.state, joined.machine).await else {
            return pool.settings.slots.into();
        };
        if !status.offer.admission.is_open() {
            return 0;
        }
        status.offer.class(pool.name.class()).map_or(
            pool.settings.slots.into(),
            crate::queue::ci::ClassOffer::free,
        )
    }

    /// Retry the create of a machine whose earlier call got no answer and
    /// left no lease in the run, so cloudlease never saw that call's token
    async fn create_machine(&self, pool: &Pool, machine: &mut PoolMachine) -> Result<(), AppError> {
        // only the token's hash was saved, so the retry sends a new token,
        // saved before the call so a machine booted from it can join even if
        // this daemon stops before the answer is saved
        let token = JoinToken::generate()?;
        machine.join_token = token.hash();
        save_machine(self.state, machine).await?;
        let mut pool = pool.clone();
        if let Some(end) = self.create_with_token(&mut pool, machine, &token).await? {
            warn!(pool = %pool.name, "machine create refused: {}", end.job_message(&pool.name));
        }
        Ok(())
    }

    /// One create call; returns why the pool must end when cloudlease refused for good
    async fn create_with_token(
        &self,
        pool: &mut Pool,
        machine: &mut PoolMachine,
        token: &JoinToken,
    ) -> Result<Option<PoolEnd>, AppError> {
        let Some(run) = pool.run.clone() else {
            return Ok(None);
        };
        let Some(link) = &self.state.fleet_link else {
            return Ok(None);
        };
        let hub = link.addr().await;
        let relays = link.relays();
        let boot = BootPlan {
            pool: &pool.name,
            slot: machine.slot,
            name: &machine.name,
            join_token: token,
            hub: &hub,
            relays: &relays,
            version: &self.config.homebased_version,
        }
        .render();
        let client = self.client.clone();
        let (slot, name) = (machine.slot, machine.name.clone());
        let created = blocking(move || client.create_machine(&run, slot, &name, &boot)).await;
        let end = match created {
            Ok(lease) => {
                info!(pool = %pool.name, machine = %machine.name, lease = %lease.lease_id, "temporary machine requested");
                machine.state = MachineState::Booting {
                    lease: machine_lease(&lease),
                };
                None
            }
            Err(error) if error.is_capacity() => {
                pool.last_error = Some(error.to_string());
                machine.state = refused_state();
                None
            }
            Err(error @ CloudleaseError::Refused { .. }) => {
                pool.last_error = Some(error.to_string());
                machine.state = refused_state();
                Some(refused_end(&error))
            }
            Err(error) => {
                // no answer: the lease may exist; the next pass finds it in
                // the run or calls again with the same slot
                pool.last_error = Some(error.to_string());
                None
            }
        };
        save_machine(self.state, machine).await?;
        Ok(end)
    }

    /// The pool takes no more machines: release its run, end its waiting
    /// jobs, and mark it released once every machine is gone
    async fn wind_down(
        &self,
        pool: &mut Pool,
        run: Option<&Run>,
        live: &[PoolMachine],
        end: PoolEnd,
    ) -> Result<(), AppError> {
        if let Some(run) = run.filter(|run| run.state == crate::pool::cloudlease::RunState::Open) {
            let client = self.client.clone();
            let id = run.run_id.clone();
            if let Err(error) = blocking(move || client.release_run(&id)).await {
                pool.last_error = Some(error.to_string());
            }
        }

        let held = held_ci_routes(self.state, pool.name.class()).await?;
        for route in held.ready.into_iter().chain(held.waiting) {
            let job = route.job;
            let ending = HeldJobEnding::Refused {
                code: end.job_code().to_owned(),
                message: end.job_message(&pool.name),
            };
            let _guard = self.state.locks.job_submissions.lock(job).await;
            call(&self.state.store, |reply| StoreMsg::EndHeldJob {
                job,
                ending,
                reply,
            })
            .await?;
        }

        if live.is_empty() {
            info!(pool = %pool.name, "pool released");
            pool.state = PoolState::Released {
                end,
                at: Utc::now(),
            };
        }
        Ok(())
    }
}

fn machine_lease(lease: &Lease) -> MachineLease {
    MachineLease {
        lease: lease.lease_id.clone(),
        hard_deadline_at: lease.hard_deadline_at,
        created_at: lease.created_at,
    }
}

fn refused_state() -> MachineState {
    MachineState::Released {
        lease: None,
        joined: None,
        reason: ReleaseReason::Refused,
        gone_at: Utc::now(),
    }
}

fn refused_end(error: &CloudleaseError) -> PoolEnd {
    match error {
        CloudleaseError::Refused { code, message, .. } => PoolEnd::Refused {
            code: code.clone(),
            message: message.clone(),
        },
        CloudleaseError::Unavailable { message } => PoolEnd::Refused {
            code: "unavailable".into(),
            message: message.clone(),
        },
    }
}

/// The reason this coordinator gave when it started the release itself
fn local_reason(state: &MachineState) -> Option<ReleaseReason> {
    match state {
        MachineState::Releasing { reason, .. } => Some(reason.clone()),
        _ => None,
    }
}
