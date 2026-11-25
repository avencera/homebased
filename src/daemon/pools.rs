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
use super::fleet_view::Scope;
use crate::config::PoolsConfig;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::advertisement::MACHINE_PROBE_PATH;
use crate::pool::boot::{BootPayload, BootPlan};
use crate::pool::cloudlease::{
    CloudleaseClient, CloudleaseError, Lease, LeaseState, Run, RunState,
};
use crate::pool::{
    ComingSlots, DEFAULT_IDLE_LIMIT, DEFAULT_JOB_RETRIES, DEFAULT_MIN_FREE_DISK_GIB,
    DEFAULT_STALL_AFTER, JoinAnswer, JoinRequest, JoinToken, JoinedMachine, MAX_JOB_RETRIES,
    MAX_LIFETIME, MAX_MACHINES, MachineClass, MachineLease, MachineReleased, MachineState, Pool,
    PoolCode, PoolEnd, PoolError, PoolMachine, PoolName, PoolReach, PoolSettings, PoolState,
    PoolView, ReleaseReason, Rerun, RerunPolicy, RunEndCause, SlotId, parse_env_file,
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

/// How long after its join a machine's slots count as coming even while it
/// cannot yet say what it offers
const JOIN_SETTLE: Duration = Duration::from_secs(120);

/// Reconcile passes a pool waits after a machine the provider could not
/// create before it asks for another, doubled for each failure in a row
const CREATE_BACKOFF_PASSES: u32 = 4;

/// What an endpoint with no grant may call on a coordinator: the join only
pub(super) fn enrollment_routes() -> Router<AppState> {
    Router::new().route(JOIN_PATH, post(join))
}

/// What a joined temporary machine may call on its coordinator: join again,
/// the Fleet probe, event delivery for its own work, and the bundles of the
/// jobs placed on it; nothing that starts work, sends messages, or reads
/// other machines' tasks
pub(super) fn member_routes(state: &AppState) -> Router<AppState> {
    let routes = enrollment_routes()
        .route(
            "/v1/cluster/job-events",
            post(super::queue_api::member_job_event),
        )
        .route("/v1/cluster/events", post(super::cluster::member_event))
        .route(super::bundles::MEMBER_ROUTE, super::bundles::member_route());
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
    #[serde(default)]
    env_file: Option<std::path::PathBuf>,
    #[serde(default)]
    spend_cap_cents: Option<std::num::NonZeroU32>,
    #[serde(default)]
    min_free_disk_gib: Option<u64>,
    #[serde(default)]
    job_retries: Option<u32>,
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
        let job_retries = self.job_retries.unwrap_or(DEFAULT_JOB_RETRIES);
        if job_retries > MAX_JOB_RETRIES {
            return Err(usage(format!(
                "--job-retries must be at most {MAX_JOB_RETRIES}"
            )));
        }
        let min_free_disk_gib = self.min_free_disk_gib.unwrap_or(DEFAULT_MIN_FREE_DISK_GIB);
        // the machine's config takes GiB that fit in bytes, and would not start otherwise
        if min_free_disk_gib.checked_mul(1 << 30).is_none() {
            return Err(usage(format!(
                "--min-free-disk {min_free_disk_gib} GiB is too large"
            )));
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
                min_free_disk_gib,
                env_file: self.env_file,
                spend_cap_cents: self.spend_cap_cents,
                job_retries,
            },
            run_request: uuid::Uuid::now_v7(),
            run: None,
            state: PoolState::Open,
            created_at: now,
            last_error: None,
            create_failures: None,
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
    refuse_fleet_class(&state, &pool.name).await?;
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

/// Refuse a pool whose class a Fleet machine already offers
///
/// An open pool owns its class: it asks for machines for every waiting job
/// of the class and fails those still waiting when it ends, so it must not
/// share the class with a Fleet machine. A Fleet machine that starts
/// offering the class while the pool is open is not refused, since its
/// offer is its own config: the pool keeps the class until it is released
async fn refuse_fleet_class(state: &AppState, name: &PoolName) -> Result<(), AppError> {
    let (_, statuses) = super::ci_api::fleet_status(state, Scope::All).await?;
    let offered = statuses.iter().any(|status| {
        status
            .offer
            .class(name.class())
            .is_some_and(|offer| offer.capacity.get() > 0)
    });
    if !offered {
        return Ok(());
    }
    Err(PoolError::new(
        PoolCode::ClassOffered,
        format!(
            "a Fleet machine already offers CI class {name} (see `ci slots`); name the pool after a class no Fleet machine offers"
        ),
    )
    .into())
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
    // only the state changes, in the store, so a reconcile pass saving its
    // own copy at the same time loses neither the release nor the run id
    let missing = PoolError::new(PoolCode::PoolNotFound, format!("no pool named {name}"));
    let release = name.clone();
    let pool = call(&state.store, |reply| StoreMsg::ReleasePool {
        name: release,
        end: PoolEnd::Requested,
        reply,
    })
    .await?
    .ok_or(missing)?;
    info!(pool = %pool.name, state = pool.state.as_str(), "pool release requested");
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
/// long as some machine of the pool could run it to its kill limit: one it
/// has, or one it would ask for now (see [`PoolReach`]); a stopped or
/// releasing pool takes no new job. Without a pool of that class the usual
/// offer check runs
pub(super) async fn admit_pool_job(state: &AppState, ci: &CiSpec) -> Result<bool, AppError> {
    let name = PoolName::from_class(ci.class.clone());
    let pool = call(&state.store, |reply| StoreMsg::Pool { name, reply }).await?;
    let Some(pool) = pool else {
        return Ok(false);
    };
    match pool.state {
        PoolState::Released { .. } => Ok(false),
        PoolState::Stopped { end } => Err(PoolError::new(
            PoolCode::PoolEnded,
            format!(
                "pool {} is stopped and takes no new jobs: {}",
                pool.name,
                end.job_message(&pool.name)
            ),
        )
        .into()),
        PoolState::Releasing { .. } => Err(PoolError::new(
            PoolCode::PoolEnded,
            format!("pool {} is releasing and takes no new jobs", pool.name),
        )
        .into()),
        PoolState::Open => {
            let name = pool.name.clone();
            let machines = call(&state.store, |reply| StoreMsg::PoolMachines {
                pool: name,
                reply,
            })
            .await?;
            let reach = PoolReach::new(&pool, &machines, Utc::now());
            let kill = ci.kill_after.get();
            if !reach.can_run(kill) {
                return Err(out_of_reach(&pool, &reach, kill).into());
            }
            // a machine has no git credentials, so the coordinator fetches the
            // commit now; one it cannot fetch fails the job before any machine
            // is asked for
            super::bundles::prepare(state, ci).await?;
            Ok(true)
        }
    }
}

/// Make the bundle of a job pinned to `machine` when it is a machine of the
/// pool its class names, so it reaches that machine with its commit; a
/// regular Fleet machine fetches the commit itself
pub(super) async fn prepare_pinned_pool_job(
    state: &AppState,
    ci: &CiSpec,
    machine: crate::machine::MachineId,
) -> Result<(), AppError> {
    let pool = PoolName::from_class(ci.class.clone());
    let machines = call(&state.store, |reply| StoreMsg::PoolMachines { pool, reply }).await?;
    let member = machines.iter().any(|candidate| {
        !matches!(candidate.state, MachineState::Released { .. })
            && candidate
                .state
                .joined()
                .is_some_and(|joined| joined.machine == machine)
    });
    if member {
        super::bundles::prepare(state, ci).await?;
    }
    Ok(())
}

/// Why a job with kill limit `kill` can run on no machine of `pool`
fn out_of_reach(pool: &Pool, reach: &PoolReach, kill: Duration) -> PoolError {
    let kill = humantime::format_duration(kill);
    // whole seconds, so the message stays short
    let longest = Duration::from_secs(reach.longest_kill().as_secs());
    let longest = humantime::format_duration(longest);
    PoolError::new(
        PoolCode::PoolDeadlineTooClose,
        format!(
            "a job with --kill-after {kill} would outlast every machine of pool {}, which \
             cloudlease deletes by its deadline {}; counting a new machine's boot, the \
             longest --kill-after the pool can run now is {longest}",
            pool.name, pool.settings.deadline_at
        ),
    )
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
    // read before anything is granted or saved, so a join that cannot get the
    // pool's variables leaves the machine booting, under its boot timeout
    let env = match &pool.settings.env_file {
        Some(path) => read_env(path).map_err(|error| {
            warn!(pool = %pool.name, "pool env file: {error}");
            refused(format!(
                "the coordinator could not read the pool's env file: {error}"
            ))
        })?,
        None => Vec::new(),
    };

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
/// daemon, a Fleet peer, or another pool machine must never be bound to a
/// lease. This check gives the refusal; the store's unique machine id holds
/// when two joins race past it
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
            let name = pool.name.clone();
            let pass = Pass {
                state: &state,
                client: &client,
                config: &config,
            };
            // a released pool ended its held jobs as it wound down; a job of
            // its class submitted since waits for a Fleet machine that
            // offers the class, so it is not the pool's to end
            if let PoolState::Released { .. } = &pool.state {
                if let Err(error) = super::bundles::remove_if_released(&state, &name).await {
                    warn!(pool = %name, "removing a released pool's bundles: {error}");
                }
                continue;
            }
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
        if matches!(pool.state, PoolState::Open | PoolState::Stopped { .. })
            && Utc::now() >= pool.settings.deadline_at
        {
            pool.state = PoolState::Releasing {
                end: PoolEnd::Deadline,
            };
        }

        let run = match self.read_run(&mut pool).await {
            Ok(run) => run,
            Err(error) => {
                pool.last_error = Some(error.to_string());
                // ending waiting jobs needs only this daemon, so a cloudlease
                // outage does not hold them
                if let PoolState::Stopped { end } | PoolState::Releasing { end } = &pool.state {
                    self.end_held_jobs(&pool, end).await?;
                }
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
        // every machine, released ones too, since their leases show the
        // class's boot grace and lifetime
        let mut synced = Vec::with_capacity(machines.len());
        for machine in machines {
            if !machine.state.is_live() {
                synced.push(machine);
                continue;
            }
            let slot = machine.slot;
            // a join holds the same lock, so neither saves over the other's
            // record; read the machine again under it
            let _guard = self.state.locks.pool_joins.lock(slot).await;
            let Some(machine) = call(&self.state.store, |reply| StoreMsg::PoolMachine {
                slot,
                reply,
            })
            .await?
            else {
                continue;
            };
            let lease = leases.get(&slot.as_uuid()).copied();
            let machine = self
                .sync_machine(&mut pool, run.as_ref(), machine, lease)
                .await?;
            synced.push(machine);
        }
        let live: Vec<PoolMachine> = synced
            .iter()
            .filter(|machine| machine.state.is_live())
            .cloned()
            .collect();

        match &pool.state {
            PoolState::Open => {
                let reach = PoolReach::new(&pool, &synced, Utc::now());
                self.end_out_of_reach(&pool, &reach).await?;
                self.grow(&mut pool, run.as_ref(), &live, &reach).await?;
            }
            // once its machines finished their jobs and are gone, a stopped
            // pool winds down like a released one
            PoolState::Stopped { end } if live.is_empty() => {
                let end = end.clone();
                self.wind_down(&mut pool, run.as_ref(), &live, end).await?;
            }
            PoolState::Stopped { end } => {
                let end = end.clone();
                self.end_held_jobs(&pool, &end).await?;
            }
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
            let (request, name, settings) =
                (pool.run_request, pool.name.clone(), pool.settings.clone());
            let created = blocking(move || client.create_run(request, &name, &settings)).await;
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
                if matches!(pool.state, PoolState::Open) && run.state != RunState::Open {
                    let end = ended_outside(&run, pool.settings.deadline_at);
                    warn!(pool = %pool.name, code = end.job_code(), "cloudlease ended the pool's run");
                    pool.state = PoolState::Releasing { end };
                }
                Ok(Some(run))
            }
            // a run cloudlease no longer has holds no machine and takes no
            // more, so the pool ends
            Err(error) if error.is_gone() => {
                if matches!(pool.state, PoolState::Open) {
                    pool.state = PoolState::Releasing {
                        end: PoolEnd::Refused {
                            code: "run_not_found".into(),
                            message: "cloudlease no longer has the pool's run".into(),
                        },
                    };
                }
                pool.last_error = Some(error.to_string());
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Whether the pool still asks for machines, as stored now: a `pool
    /// release` may have been saved since this pass read it
    async fn still_open(&self, pool: &Pool) -> Result<bool, AppError> {
        if !matches!(pool.state, PoolState::Open) {
            return Ok(false);
        }
        let stored = read_pool(self.state, &pool.name).await?;
        Ok(matches!(stored.state, PoolState::Open))
    }

    /// Move one machine to what cloudlease reports for its lease
    async fn sync_machine(
        &self,
        pool: &mut Pool,
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
                count_create_failure(pool, &machine, &reason);
                let gone_at = lease.gone_at.unwrap_or_else(Utc::now);
                return self.finish(pool, machine, reason, gone_at).await;
            }
            // keep the reason this coordinator gave, if it gave one
            (Some(_), MachineState::Releasing { .. }) => {}
            (Some(lease), state) if lease.state == LeaseState::Releasing => {
                let reason = lease
                    .release_reason()
                    .unwrap_or(ReleaseReason::Other("unknown".into()));
                let joined = state.joined().cloned();
                // the stored lease keeps the boot deadline, which renewals
                // moved on cloudlease's side
                let kept = state.lease().cloned();
                count_create_failure(pool, &machine, &reason);
                machine.state = MachineState::Releasing {
                    lease: kept.unwrap_or_else(|| machine_lease(lease)),
                    joined,
                    reason,
                };
            }
            // a create whose answer was lost and that the provider is still
            // working on: it is neither a success nor a failure yet, so it must
            // not reset the count of failed creates in a row
            (Some(lease), MachineState::Requested) if lease.state == LeaseState::Provisioning => {}
            // a create whose answer was lost, which the provider took
            (Some(lease), MachineState::Requested) => {
                pool.create_succeeded();
                machine.state = MachineState::Booting {
                    lease: machine_lease(lease),
                };
            }
            // cloudlease deletes a machine that has not joined by the end of
            // its boot grace, which the arm above then reads as releasing
            (
                Some(_),
                MachineState::Booting { .. }
                | MachineState::Joined { .. }
                | MachineState::Released { .. },
            ) => {}
            (None, MachineState::Requested) => match run {
                // the create may not have reached cloudlease: send it again
                Some(_) if self.still_open(pool).await? => {
                    if let Some(end) = self.create_machine(pool, &mut machine).await? {
                        stop(pool, end);
                    }
                }
                // wind-down releases the run first; cloudlease then lists
                // the lease if the create reached it
                Some(run)
                    if run.state == RunState::Open
                        && matches!(pool.state, PoolState::Releasing { .. }) => {}
                // the run ended, or the pool stopped, with no lease for this
                // slot; a create that still lands later holds a lease no
                // record knows, which ends with the run
                _ => machine.state = never_created(),
            },
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

        if ends_with_pool(&pool.state, &machine.state) {
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
                attempt: 1,
                not_rerun: None,
            };
            let policy = rerun_policy(self.state, pool, machine.slot).await?;
            end_jobs_of_lost_machine(self.state, joined.machine, &released, &policy).await?;
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
        reach: &PoolReach,
    ) -> Result<(), AppError> {
        let backoff = self.config.reconcile_interval * CREATE_BACKOFF_PASSES;
        if run.is_none() || !pool.creates_allowed(Utc::now(), backoff) {
            return Ok(());
        }
        let wanted = self.wanted_machines(pool, live, reach).await?;
        for _ in 0..wanted {
            if !self.still_open(pool).await? {
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
            let Some(boot) = self.boot_data(pool, &machine, &token).await else {
                break;
            };
            // saved before the call, so a lost answer is retried with the same
            // slot and body
            let (saved, saved_boot) = (Box::new(machine.clone()), Box::new(boot.clone()));
            call(&self.state.store, |reply| StoreMsg::InsertPoolMachine {
                machine: saved,
                boot: saved_boot,
                reply,
            })
            .await?;
            let outcome = self.create(pool, &mut machine, &boot).await?;
            if let Some(end) = outcome {
                stop(pool, end);
                break;
            }
            // no answer, a refusal, or a machine the provider could not make
            if !matches!(machine.state, MachineState::Booting { .. }) {
                break;
            }
        }
        Ok(())
    }

    /// How many machines to ask for now: ready jobs with no slot that a new
    /// machine could run to their kill limit, less the slots of machines
    /// still booting and free slots on joined ones, within the pool's limit
    async fn wanted_machines(
        &self,
        pool: &Pool,
        live: &[PoolMachine],
        reach: &PoolReach,
    ) -> Result<u32, AppError> {
        let held = held_ci_routes(self.state, pool.name.class()).await?;
        // a job only a machine the pool has can run waits for its slot; a
        // new machine would idle out without it
        let kills: Vec<Duration> = held
            .ready
            .iter()
            .filter_map(|route| route.spec.ci.as_ref().map(|ci| ci.kill_after.get()))
            .filter(|kill| reach.new_machine_can_run(*kill))
            .collect();
        if kills.is_empty() {
            return Ok(0);
        }
        let slots = pool.settings.slots.get().max(1);
        let mut coming = Vec::with_capacity(live.len());
        for machine in live {
            let free = match &machine.state {
                MachineState::Requested | MachineState::Booting { .. } => slots,
                MachineState::Joined { joined, .. } => self.free_slots(pool, joined).await,
                MachineState::Releasing { .. } | MachineState::Released { .. } => continue,
            };
            let ends_at = machine.state.lease().map(|lease| lease.hard_deadline_at);
            coming.push(ComingSlots { ends_at, free });
        }
        // a free slot on a machine that ends before a job's kill limit does
        // not take the job, so it must not stand in for a new machine
        let missing = reach.jobs_without_slot(kills, coming);
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
        // another machine in that window. Past it, a machine that cannot say
        // what it offers offers nothing, so waiting jobs get another machine
        let settling = (Utc::now() - joined.joined_at)
            .to_std()
            .is_ok_and(|age| age < JOIN_SETTLE);
        let unknown = if settling {
            pool.settings.slots.into()
        } else {
            0
        };
        let Ok(status) = super::ci_api::machine_status(self.state, joined.machine).await else {
            return unknown;
        };
        if !status.offer.admission.is_open() {
            return 0;
        }
        status
            .offer
            .class(pool.name.class())
            .map_or(unknown, crate::queue::ci::ClassOffer::free)
    }

    /// Retry the create of a machine whose earlier call got no answer and
    /// left no lease in the run, with the body that call sent, so cloudlease
    /// takes it as the same request; returns why the pool must end when
    /// cloudlease refused for good
    async fn create_machine(
        &self,
        pool: &mut Pool,
        machine: &mut PoolMachine,
    ) -> Result<Option<PoolEnd>, AppError> {
        let slot = machine.slot;
        let boot = call(&self.state.store, |reply| StoreMsg::PendingBoot {
            slot,
            reply,
        })
        .await?
        .ok_or_else(|| AppError::Internal {
            message: format!("pool machine {slot} has no create body to send again"),
        })?;
        self.create(pool, machine, &boot).await
    }

    /// The boot data of a new machine; `None` without a Fleet link to join over
    async fn boot_data(
        &self,
        pool: &Pool,
        machine: &PoolMachine,
        token: &JoinToken,
    ) -> Option<BootPayload> {
        let link = self.state.fleet_link.as_ref()?;
        let hub = link.addr().await;
        let relays = link.relays();
        let boot = BootPlan {
            pool: &pool.name,
            slot: machine.slot,
            name: &machine.name,
            join_token: token,
            hub: &hub,
            relays: &relays,
            install: &self.config.machine_install,
            min_free_disk_gib: pool.settings.min_free_disk_gib,
        };
        Some(boot.render())
    }

    /// One create call; returns why the pool must end when cloudlease refused for good
    async fn create(
        &self,
        pool: &mut Pool,
        machine: &mut PoolMachine,
        boot: &BootPayload,
    ) -> Result<Option<PoolEnd>, AppError> {
        let Some(run) = pool.run.clone() else {
            return Ok(None);
        };
        let client = self.client.clone();
        let (slot, name, boot) = (machine.slot, machine.name.clone(), boot.clone());
        let created = blocking(move || client.create_machine(&run, slot, &name, &boot)).await;
        let end = match created {
            Ok(lease) => {
                info!(pool = %pool.name, machine = %machine.name, lease = %lease.lease_id, "temporary machine requested");
                // cloudlease answers a provider failure with the lease already ending
                match lease.release_reason() {
                    Some(reason) => {
                        count_create_failure(pool, machine, &reason);
                        machine.state = MachineState::Releasing {
                            lease: machine_lease(&lease),
                            joined: None,
                            reason,
                        };
                    }
                    None => {
                        pool.create_succeeded();
                        machine.state = MachineState::Booting {
                            lease: machine_lease(&lease),
                        };
                    }
                }
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
        // a run still open after a failed release is released next pass, so
        // the pool is not released before cloudlease took the run's release
        let mut run_open = false;
        if let Some(run) = run.filter(|run| run.state == RunState::Open) {
            let client = self.client.clone();
            let id = run.run_id.clone();
            match blocking(move || client.release_run(&id)).await {
                Ok(()) => {}
                Err(error) if error.is_gone() => {}
                Err(error) => {
                    pool.last_error = Some(error.to_string());
                    run_open = true;
                }
            }
        }

        self.end_held_jobs(pool, &end).await?;
        if live.is_empty() && !run_open {
            info!(pool = %pool.name, "pool released");
            // no job of the pool is left to need its bundles
            super::bundles::remove(self.state, &pool.name);
            pool.state = PoolState::Released {
                end,
                at: Utc::now(),
            };
        }
        Ok(())
    }

    /// End the pool's jobs that have not started and that no machine of the
    /// pool can run to their kill limit any more: time only takes reach away
    async fn end_out_of_reach(&self, pool: &Pool, reach: &PoolReach) -> Result<(), AppError> {
        let held = held_ci_routes(self.state, pool.name.class()).await?;
        for route in held.ready.into_iter().chain(held.waiting) {
            let Some(kill) = route.spec.ci.as_ref().map(|ci| ci.kill_after.get()) else {
                continue;
            };
            if reach.can_run(kill) {
                continue;
            }
            let job = route.job;
            let refusal = out_of_reach(pool, reach, kill);
            let ending = HeldJobEnding::Refused {
                code: refusal.code.as_str().to_owned(),
                message: refusal.message,
            };
            let _guard = self.state.locks.job_submissions.lock(job).await;
            let ended = call(&self.state.store, |reply| StoreMsg::EndPoolJob {
                job,
                ending,
                reply,
            })
            .await?;
            if ended {
                warn!(pool = %pool.name, %job, "job ended: no machine of the pool can run it to its kill limit");
            }
        }
        Ok(())
    }

    /// End the pool's jobs that have not started, with why the pool ended
    async fn end_held_jobs(&self, pool: &Pool, end: &PoolEnd) -> Result<(), AppError> {
        let held = held_ci_routes(self.state, pool.name.class()).await?;
        for route in held.ready.into_iter().chain(held.waiting) {
            let job = route.job;
            let ending = HeldJobEnding::Refused {
                code: end.job_code().to_owned(),
                message: end.job_message(&pool.name),
            };
            let _guard = self.state.locks.job_submissions.lock(job).await;
            call(&self.state.store, |reply| StoreMsg::EndPoolJob {
                job,
                ending,
                reply,
            })
            .await?;
        }
        Ok(())
    }
}

/// Whether jobs of `pool` that lost the machine in `lost` run again: its
/// other machines, as stored, give the pool's reach
async fn rerun_policy(
    state: &AppState,
    pool: &Pool,
    lost: SlotId,
) -> Result<RerunPolicy, AppError> {
    let name = pool.name.clone();
    let machines = call(&state.store, |reply| StoreMsg::PoolMachines {
        pool: name,
        reply,
    })
    .await?;
    Ok(RerunPolicy::new(pool, &machines, lost, Utc::now()))
}

/// Rerun or fail each job this origin sent to a machine cloudlease
/// confirmed gone, and fail each job held pinned to it
///
/// Each job's submission lock keeps a send still in flight from resolving
/// a route this moves back to held
async fn end_jobs_of_lost_machine(
    state: &AppState,
    machine: crate::machine::MachineId,
    released: &MachineReleased,
    policy: &RerunPolicy,
) -> Result<(), AppError> {
    let jobs = call(&state.store, |reply| StoreMsg::JobsOfReleasedMachine {
        machine,
        reply,
    })
    .await?;
    for job in jobs {
        let _guard = state.locks.job_submissions.lock(job).await;
        let released = Box::new(released.clone());
        let outcome = call(&state.store, |reply| StoreMsg::EndJobOfReleasedMachine {
            job,
            machine,
            released: released.clone(),
            policy: *policy,
            reply,
        })
        .await?;
        match outcome {
            Some(Rerun::Queue { attempt }) => {
                info!(machine = %released.machine, %job, attempt, reason = %released.reason, "job queued again: its machine was lost");
            }
            Some(Rerun::End(not_rerun)) => {
                warn!(machine = %released.machine, %job, reason = %released.reason, ?not_rerun, "job ended with its machine");
            }
            None => {}
        }
    }
    Ok(())
}

/// The lease of a machine that has not joined, so it never renewed and its
/// heartbeat deadline is still the end of its boot grace
fn machine_lease(lease: &Lease) -> MachineLease {
    MachineLease {
        lease: lease.lease_id.clone(),
        hard_deadline_at: lease.hard_deadline_at,
        created_at: lease.created_at,
        boot_deadline_at: lease.heartbeat_deadline_at,
    }
}

/// Why a pool ends whose run cloudlease ended without this coordinator asking
///
/// The run itself carries no reason, so its leases tell: a spend cap
/// releases them with `spend_cap`. A run past its deadline ends like the
/// pool's own deadline, whichever clock saw it first
fn ended_outside(run: &Run, deadline: chrono::DateTime<Utc>) -> PoolEnd {
    let spend_cap = run
        .machines
        .iter()
        .any(|lease| lease.release_reason() == Some(ReleaseReason::SpendCap));
    if spend_cap {
        return PoolEnd::RunEnded {
            cause: RunEndCause::SpendCap,
        };
    }
    if Utc::now() >= deadline || Utc::now() >= run.deadline_at {
        return PoolEnd::Deadline;
    }
    PoolEnd::RunEnded {
        cause: RunEndCause::Released,
    }
}

/// A machine whose create cloudlease never took, now that its run ended
fn never_created() -> MachineState {
    MachineState::Released {
        lease: None,
        joined: None,
        reason: ReleaseReason::Other("never_created".into()),
        gone_at: Utc::now(),
    }
}

/// Stop the pool for a refusal, so its machines finish their jobs
fn stop(pool: &mut Pool, end: PoolEnd) {
    warn!(pool = %pool.name, code = end.job_code(), "pool stopped: it asks for no more machines");
    pool.stop(end);
}

/// Count a machine that ended before it ever joined because the provider
/// could not create it
fn count_create_failure(pool: &mut Pool, machine: &PoolMachine, reason: &ReleaseReason) {
    let unjoined = matches!(
        machine.state,
        MachineState::Requested | MachineState::Booting { .. }
    );
    if *reason != ReleaseReason::CreateFailed || !unjoined {
        return;
    }
    pool.create_failed(Utc::now());
    let failures = pool.create_failures.map_or(0, |failures| failures.count);
    warn!(pool = %pool.name, machine = %machine.name, failures, "the provider could not create a machine");
}

/// Whether a machine is released because its pool no longer runs: a
/// releasing pool releases every machine, a stopped one only those that
/// have not joined, so joined ones finish their jobs
fn ends_with_pool(pool: &PoolState, machine: &MachineState) -> bool {
    match pool {
        PoolState::Open => false,
        PoolState::Stopped { .. } => matches!(machine, MachineState::Booting { .. }),
        PoolState::Releasing { .. } | PoolState::Released { .. } => matches!(
            machine,
            MachineState::Booting { .. } | MachineState::Joined { .. }
        ),
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
