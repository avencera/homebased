//! `homebased thread`: find a thread anywhere in the Fleet, read how full its
//! context is, compact it, and watch it
//!
//! A reference is looked up on this machine first, then on every reachable
//! peer, unless a T3 link's environment, a task route, or `--machine` names
//! the machine. Each daemon reads only its own T3, Claude, and Codex state, so
//! peers answer `POST /v1/cluster/threads/context` for their own threads.
//! Compaction runs on the machine whose T3 owns the thread, forwarded with the
//! same destination and protocol checks as other Fleet writes. Watches live on
//! the caller's machine, which checks them in the idle compaction scan

mod local;
pub(crate) mod watch_scan;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tracing::debug;

use self::local::{LocalIndex, LocalSources};
use super::AppState;
use super::actors::{StoreMsg, SupervisorMsg, call};
use super::cluster::{check_api_version, check_protocol};
use super::compaction_log::Trigger;
use super::context_threads::{compact_session, run_blocking};
use super::inspection::{message_origin_route, message_worker_route};
use super::message_sender::resolve_machine;
use super::peer_read::post_peer;
use crate::domain::{API_VERSION, ThreadId};
use crate::error::{AppError, ThreadMatch};
use crate::fleet::directory::Reachability;
use crate::fleet::http::{ClusterClient, DEFAULT_MAX_BODY};
use crate::machine::MachineId;
use crate::submission::CallbackContext;
use crate::t3::{T3Env, environment_id};
use crate::thread_ref::{
    Caller, Provider, ThreadContext, ThreadContextRequest, ThreadContextResponse, ThreadHost,
    ThreadRef, ThreadSelector,
};
use crate::thread_watch::{
    CompactOutcome, CompactRequest, CompactResponse, ThreadWatch, Threshold, WatchAction,
    WatchEndReason, WatchId, WatchRequest, WatchTarget, WatchesResponse,
};

const CLUSTER_CONTEXT_PATH: &str = "/v1/cluster/threads/context";
const CLUSTER_COMPACT_PATH: &str = "/v1/cluster/threads/compact";

// the peer asks its T3, which answers within seconds or not at all
const COMPACT_TIMEOUT: Duration = Duration::from_secs(30);

/// Most references one lookup may name
const MAX_REFERENCES: usize = 64;

/// Lookups that only read state, safe on the TCP listener
pub fn read_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/threads/context", post(context))
        .route("/v1/threads/watches", get(watches))
}

/// Compaction and watches, for the Unix socket
pub fn write_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/threads/compact", post(compact))
        .route("/v1/threads/watches", post(watch))
        .route("/v1/threads/watches/{id}/unwatch", post(unwatch))
}

/// Peer routes for an enabled fleet
///
/// They share the dashboard's TCP listener, so the compaction, which acts on
/// a thread, refuses browser writes from another origin like the queue's
/// cluster routes; the context lookup only reads
pub fn cluster_routes() -> Router<AppState> {
    Router::new()
        .route(CLUSTER_CONTEXT_PATH, post(cluster_context))
        .merge(
            Router::new()
                .route(CLUSTER_COMPACT_PATH, post(cluster_compact))
                .layer(axum::middleware::from_fn(super::web::browser_write_guard)),
        )
}

/// `POST /v1/cluster/threads/context` body
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterContext {
    api_version: u32,
    destination_machine: MachineId,
    /// References as [`ThreadRef`] prints them
    references: Vec<String>,
    prefixes: bool,
    /// T3 thread whose children to list too
    children_of: Option<String>,
    live: bool,
}

/// `POST /v1/cluster/threads/context` answer
#[derive(Debug, Serialize, Deserialize)]
struct ClusterContextBody {
    api_version: u32,
    machine: MachineId,
    /// Matches of each reference, in request order
    results: Vec<Vec<ThreadContext>>,
    children: Vec<ThreadContext>,
}

/// `POST /v1/cluster/threads/compact` body
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterCompact {
    api_version: u32,
    protocol_version: u32,
    destination_machine: MachineId,
    origin_machine: MachineId,
    session: ThreadId,
    /// Whether a watch asked, for the compaction log
    watch: bool,
}

/// `POST /v1/cluster/threads/compact` answer
#[derive(Debug, Serialize, Deserialize)]
struct ClusterCompactBody {
    api_version: u32,
    protocol_version: u32,
    machine: MachineId,
    outcome: CompactOutcome,
    detail: Option<String>,
}

/// What the daemon knows about this machine's threads before reading files
async fn local_sources(state: &AppState) -> Result<LocalSources, AppError> {
    let workers = call(&state.store, |reply| StoreMsg::WorkerThreads { reply }).await?;
    let waiting = call(&state.store, |reply| StoreMsg::WaitingThreads { reply })
        .await?
        .into_iter()
        .map(|(thread, _)| thread)
        .collect();
    Ok(LocalSources {
        env: T3Env::from_env(),
        machine: state.machine.identity.machine,
        machine_name: state.machine.name.to_string(),
        workers,
        waiting,
        titles: state.thread_titles.clone(),
    })
}

/// Matches of each reference on this machine, and the children of `children_of`
async fn find_here(
    state: &AppState,
    references: Vec<ThreadRef>,
    prefixes: bool,
    children_of: Option<ChildrenOf>,
) -> Result<(Vec<Vec<ThreadContext>>, Vec<ThreadContext>, Option<String>), AppError> {
    let sources = local_sources(state).await?;
    run_blocking(move || {
        let index =
            LocalIndex::load(sources, Utc::now()).map_err(|message| AppError::T3Unavailable {
                message: format!("T3's thread state cannot be read: {message}"),
            })?;
        let results = references
            .iter()
            .map(|reference| index.find(reference, prefixes))
            .collect();
        let parent = children_of.as_ref().and_then(|of| match of {
            ChildrenOf::Caller(caller, _) => index.caller_t3(caller),
            ChildrenOf::T3(thread, _) => Some(thread.clone()),
        });
        let live = children_of.as_ref().is_some_and(ChildrenOf::live);
        let children = parent
            .as_deref()
            .map(|parent| index.children(parent, live))
            .unwrap_or_default();
        Ok((results, children, parent))
    })
    .await?
}

/// Whose T3 children a lookup lists
#[derive(Debug, Clone)]
enum ChildrenOf {
    /// The calling agent, with whether to leave out finished children
    Caller(Caller, bool),
    /// A T3 thread a peer named, with whether to leave out finished children
    T3(String, bool),
}

impl ChildrenOf {
    fn live(&self) -> bool {
        match self {
            Self::Caller(_, live) | Self::T3(_, live) => *live,
        }
    }
}

/// One selector on its way to a thread
struct Wanted {
    /// Text the caller passed, for errors and the answer
    text: String,
    reference: ThreadRef,
    /// Machine it must be on, when the reference, a task, or the request says
    machine: Option<MachineId>,
    prefixes: bool,
}

/// What [`resolve`] found
pub(crate) struct Resolved {
    /// One thread per selector, in order
    pub(crate) threads: Vec<ThreadContext>,
    /// The T3 children asked for
    pub(crate) children: Vec<ThreadContext>,
    /// Fleet machines that were asked and did not answer, each with why, so
    /// `children` can be missing some
    pub(crate) unanswered: Vec<String>,
}

/// Threads `selectors` name, in order, and the T3 children `children_of` asks for
///
/// Each selector must name exactly one thread. Machines the Fleet directory
/// marks offline are not asked. `carried_by` is the caller's T3 thread, whose
/// message a T3 chip came in
pub(crate) async fn resolve(
    state: &AppState,
    selectors: Vec<ThreadSelector>,
    machine: Option<&str>,
    prefixes: bool,
    children_of: Option<(Caller, bool)>,
    carried_by: Option<&str>,
) -> Result<Resolved, AppError> {
    if selectors.len() > MAX_REFERENCES {
        return Err(AppError::Usage {
            message: format!("at most {MAX_REFERENCES} threads per lookup"),
        });
    }
    let local = state.machine.identity.machine;
    let only = match machine {
        Some(machine) => Some(resolve_machine(state, machine).await?),
        None => None,
    };
    let peers = match state.fleet.handle() {
        Some(fleet) => fleet.peers().await,
        None => Vec::new(),
    };
    let here_environment = run_blocking(|| environment_id(&T3Env::from_env())).await?;

    let mut wanted = Vec::with_capacity(selectors.len());
    for selector in selectors {
        let text = selector.to_string();
        let (reference, machine, prefixes) = match selector {
            ThreadSelector::Reference { text } => {
                let reference =
                    chip_thread(state, ThreadRef::parse(&text)?, &text, carried_by).await?;
                let mut machine = only;
                if machine.is_none()
                    && let ThreadRef::T3 {
                        environment: Some(environment),
                        ..
                    } = &reference
                {
                    // a peer from before environments were advertised is searched like any other
                    machine = if here_environment.as_deref() == Some(environment.as_str()) {
                        Some(local)
                    } else {
                        peers
                            .iter()
                            .find(|peer| peer.t3_environment.as_deref() == Some(environment))
                            .map(|peer| peer.machine)
                    };
                }
                (reference, machine, prefixes)
            }
            ThreadSelector::Task { task } => {
                let (machine, thread) = message_origin_route(state, task).await?;
                (ThreadRef::Id(thread.to_string()), Some(machine), false)
            }
            ThreadSelector::Worker { task } => {
                let (machine, thread) = message_worker_route(state, task).await?;
                (ThreadRef::Id(thread.to_string()), Some(machine), false)
            }
        };
        // a task's thread is where its task says, so a `--machine` that names
        // another machine can only be a mistake; a reference takes `only` as is
        if let (Some(only), Some(found)) = (only, machine)
            && only != found
        {
            let on = if found == local {
                state.machine.name.to_string()
            } else {
                peer_name(&peers, found)
            };
            return Err(AppError::Usage {
                message: format!("{text} is on {on}, not the machine --machine names"),
            });
        }
        wanted.push(Wanted {
            text,
            reference,
            machine,
            prefixes,
        });
    }

    let here: Vec<usize> = (0..wanted.len())
        .filter(|index| {
            wanted[*index]
                .machine
                .is_none_or(|machine| machine == local)
        })
        .collect();
    let asks_children = children_of.is_some();
    // the caller's T3 thread is here whatever `--machine` names, so the parent
    // is always read here; `--machine` only picks whose children are listed
    let children_here = children_of
        .clone()
        .map(|(caller, live)| ChildrenOf::Caller(caller, live));
    let children_local = only.is_none_or(|machine| machine == local);
    let mut matches: Vec<Vec<ThreadContext>> = vec![Vec::new(); wanted.len()];
    let mut children = Vec::new();
    let mut parent = children_of
        .as_ref()
        .and_then(|(caller, _)| caller.t3_thread.clone());
    // lookups here never use prefixes and exact ids together, so one pass per mode
    for prefixes in [false, true] {
        let indexes: Vec<usize> = here
            .iter()
            .copied()
            .filter(|index| wanted[*index].prefixes == prefixes)
            .collect();
        let of = if prefixes {
            None
        } else {
            children_here.clone()
        };
        if indexes.is_empty() && of.is_none() {
            continue;
        }
        let references = indexes
            .iter()
            .map(|index| wanted[*index].reference.clone())
            .collect();
        let (results, found_children, found_parent) =
            find_here(state, references, prefixes, of).await?;
        for (index, result) in indexes.into_iter().zip(results) {
            matches[index] = result;
        }
        if children_local {
            children.extend(found_children);
        }
        parent = parent.or(found_parent);
    }

    // peers answer for what this machine did not find
    let reachable: Vec<MachineId> = peers
        .iter()
        .filter(|peer| !matches!(peer.reachability, Reachability::Offline { .. }))
        .map(|peer| peer.machine)
        .filter(|machine| only.is_none_or(|only| only == *machine))
        .collect();
    let mut asks: HashMap<MachineId, Vec<usize>> = HashMap::new();
    for (index, item) in wanted.iter().enumerate() {
        if !matches[index].is_empty() {
            continue;
        }
        match item.machine {
            Some(machine) if machine == local => {}
            Some(machine) => asks.entry(machine).or_default().push(index),
            None => {
                for machine in &reachable {
                    asks.entry(*machine).or_default().push(index);
                }
            }
        }
    }
    let children_parent = parent.filter(|_| asks_children);
    if children_parent.is_some() {
        for machine in &reachable {
            asks.entry(*machine).or_default();
        }
    }
    let live = children_of.as_ref().is_some_and(|(_, live)| *live);
    let mut asked: HashSet<MachineId> = HashSet::new();
    let mut missed: Vec<String> = Vec::new();
    if let Some(fleet) = state.fleet.handle() {
        let mut jobs = JoinSet::new();
        for (machine, indexes) in asks {
            asked.insert(machine);
            let body = ClusterContext {
                api_version: API_VERSION,
                destination_machine: machine,
                references: indexes
                    .iter()
                    .map(|index| wanted[*index].reference.to_string())
                    .collect(),
                // a reference that names a machine is never a prefix there
                prefixes: indexes.iter().any(|index| wanted[*index].prefixes),
                children_of: children_parent.clone(),
                live,
            };
            let fleet = fleet.clone();
            jobs.spawn(async move {
                let answer = post_peer::<_, ClusterContextBody>(
                    &fleet,
                    machine,
                    CLUSTER_CONTEXT_PATH,
                    &body,
                )
                .await;
                (machine, indexes, answer)
            });
        }
        while let Some(joined) = jobs.join_next().await {
            let Ok((machine, indexes, answer)) = joined else {
                continue;
            };
            let name = peer_name(&peers, machine);
            let body = match answer {
                Ok(body) if body.machine == machine && body.results.len() == indexes.len() => body,
                Ok(_) => {
                    missed.push(format!("{name} (answered for another lookup)"));
                    continue;
                }
                Err(message) => {
                    debug!("thread lookup on {machine} failed: {message}");
                    missed.push(format!("{name} ({message})"));
                    continue;
                }
            };
            for (index, result) in indexes.into_iter().zip(body.results) {
                matches[index].extend(result);
            }
            children.extend(body.children);
        }
    } else {
        for item in &wanted {
            if let Some(machine) = item.machine.filter(|machine| *machine != local) {
                missed.push(format!("{machine} (Fleet is off on this machine)"));
            }
        }
    }

    let mut searched = vec![state.machine.name.to_string()];
    searched.extend(asked.iter().map(|machine| peer_name(&peers, *machine)));
    let mut threads = Vec::with_capacity(wanted.len());
    for (item, found) in wanted.into_iter().zip(matches) {
        threads.push(one_match(item, found, &searched, &missed)?);
    }
    Ok(Resolved {
        threads,
        children,
        unanswered: missed,
    })
}

/// The thread a T3 chip names, from the context record T3 keeps for it in
/// this machine's T3, or `reference` unchanged when it is no chip
///
/// A chip copied out of another machine's T3 has its record there, so only a
/// `thread_<id>` ref, which holds the id itself, still reads back
async fn chip_thread(
    state: &AppState,
    reference: ThreadRef,
    text: &str,
    carrier: Option<&str>,
) -> Result<ThreadRef, AppError> {
    let ThreadRef::Chip { context } = &reference else {
        return Ok(reference);
    };
    let context = context.clone();
    let carrier = carrier.map(str::to_owned);
    let mut targets = run_blocking(move || {
        crate::t3::chip_targets(&T3Env::from_env(), &context, carrier.as_deref())
    })
    .await?
    .map_err(|message| AppError::T3Unavailable {
        message: format!("T3's thread state cannot be read: {message}"),
    })?;
    // the ref is unique only in its message, so another message's record is
    // taken only when no other target competes with it
    if targets.len() > 1 {
        return Err(AppError::ThreadRefAmbiguous {
            reference: text.to_owned(),
            matches: targets
                .into_iter()
                .map(|target| ThreadMatch {
                    machine: state.machine.identity.machine,
                    machine_name: state.machine.name.to_string(),
                    reference: format!("thread_{}", target.thread),
                    title: target.title,
                })
                .collect(),
        });
    }
    if let Some(target) = targets.pop() {
        return Ok(ThreadRef::T3 {
            environment: target.environment,
            thread: target.thread,
        });
    }
    reference
        .chip_fallback()
        .ok_or_else(|| AppError::ThreadNotFound {
            reference: text.to_owned(),
            detail: "; chip ref is not a thread id; pass the threadId from the t3_context block"
                .to_owned(),
        })
}

fn peer_name(peers: &[crate::fleet::directory::PeerView], machine: MachineId) -> String {
    peers
        .iter()
        .find(|peer| peer.machine == machine)
        .map_or_else(|| machine.to_string(), |peer| peer.name.to_string())
}

/// The one thread a reference names, or why it names none or several
fn one_match(
    wanted: Wanted,
    mut found: Vec<ThreadContext>,
    searched: &[String],
    missed: &[String],
) -> Result<ThreadContext, AppError> {
    match found.len() {
        1 => {
            let mut thread = found.remove(0);
            thread.reference = Some(wanted.text);
            Ok(thread)
        }
        0 => {
            let mut detail = format!("; looked on {}", searched.join(", "));
            if !missed.is_empty() {
                detail.push_str(&format!("; no answer from {}", missed.join(", ")));
            }
            Err(AppError::ThreadNotFound {
                reference: wanted.text,
                detail,
            })
        }
        _ => Err(AppError::ThreadRefAmbiguous {
            reference: wanted.text,
            matches: found
                .iter()
                .map(|thread| ThreadMatch {
                    machine: thread.machine,
                    machine_name: thread.machine_name.clone(),
                    reference: thread.exact_reference(),
                    title: thread.title.clone(),
                })
                .collect(),
        }),
    }
}

/// The Claude Code session or Codex thread that `reference` names on `machine`,
/// for a direct message
///
/// A UUID that no lookup finds, such as on a peer that predates thread
/// lookups, goes as given and the receiver checks it, as before references
pub(crate) async fn message_thread(
    state: &AppState,
    machine: MachineId,
    reference: &str,
    carried_by: Option<&str>,
) -> Result<ThreadId, AppError> {
    let exact = ThreadRef::parse(reference)?.uuid().map(ThreadId);
    let lookup = resolve(
        state,
        vec![ThreadSelector::Reference {
            text: reference.to_owned(),
        }],
        Some(&machine.to_string()),
        false,
        None,
        carried_by,
    )
    .await;
    match (lookup, exact) {
        (Ok(Resolved { mut threads, .. }), _) => {
            let thread = threads.pop().ok_or_else(|| AppError::Internal {
                message: "thread lookup returned no thread".into(),
            })?;
            thread.session.ok_or_else(|| AppError::Usage {
                message: format!(
                    "{reference} has not started a Claude Code session or Codex thread yet"
                ),
            })
        }
        (Err(error @ AppError::ThreadRefAmbiguous { .. }), _) => Err(error),
        (Err(_), Some(exact)) => Ok(exact),
        (Err(error), None) => Err(error),
    }
}

/// Exact matches of `references` on `machine`, for the watch scan
///
/// The error says why a peer did not answer
pub(crate) async fn find_on(
    state: &AppState,
    machine: MachineId,
    references: Vec<String>,
) -> Result<Vec<Vec<ThreadContext>>, String> {
    if machine == state.machine.identity.machine {
        let references = references
            .iter()
            .map(|reference| ThreadRef::parse(reference))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        return find_here(state, references, false, None)
            .await
            .map(|(results, _, _)| results)
            .map_err(|error| error.to_string());
    }
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| "Fleet is off on this machine".to_owned())?;
    // a peer takes at most `MAX_REFERENCES` per lookup
    let mut results = Vec::with_capacity(references.len());
    for batch in references.chunks(MAX_REFERENCES) {
        let body = ClusterContext {
            api_version: API_VERSION,
            destination_machine: machine,
            references: batch.to_vec(),
            prefixes: false,
            children_of: None,
            live: false,
        };
        let answer: ClusterContextBody =
            post_peer(fleet, machine, CLUSTER_CONTEXT_PATH, &body).await?;
        if answer.machine != machine || answer.results.len() != batch.len() {
            return Err("peer answered for another lookup".into());
        }
        results.extend(answer.results);
    }
    Ok(results)
}

async fn context(
    State(state): State<AppState>,
    Json(body): Json<ThreadContextRequest>,
) -> Result<Json<ThreadContextResponse>, AppError> {
    check_api_version(body.api_version)?;
    if body.threads.is_empty() && body.children_of.is_none() {
        return Err(AppError::Usage {
            message: "name a thread, or ask for the caller's children".into(),
        });
    }
    let asks_children = body.children_of.is_some();
    let children_of = body.children_of.map(|caller| (caller, body.live));
    let resolved = resolve(
        &state,
        body.threads,
        body.machine.as_deref(),
        body.prefixes,
        children_of,
        body.carried_by.as_deref(),
    )
    .await?;
    Ok(Json(ThreadContextResponse {
        api_version: API_VERSION,
        threads: resolved.threads,
        children: resolved.children,
        // a machine that did not answer may run children, so the list cannot be trusted whole
        children_complete: asks_children.then_some(resolved.unanswered.is_empty()),
        unanswered: if asks_children {
            resolved.unanswered
        } else {
            Vec::new()
        },
    }))
}

async fn cluster_context(
    State(state): State<AppState>,
    Json(body): Json<ClusterContext>,
) -> Result<Json<ClusterContextBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    if body.references.len() > MAX_REFERENCES {
        return Err(AppError::Usage {
            message: format!("at most {MAX_REFERENCES} threads per lookup"),
        });
    }
    let references = body
        .references
        .iter()
        .map(|reference| ThreadRef::parse(reference))
        .collect::<Result<Vec<_>, _>>()?;
    let children_of = body
        .children_of
        .map(|parent| ChildrenOf::T3(parent, body.live));
    let (results, children, _) = find_here(&state, references, body.prefixes, children_of).await?;
    Ok(Json(ClusterContextBody {
        api_version: API_VERSION,
        machine: state.machine.identity.machine,
        results,
        children,
    }))
}

async fn compact(
    State(state): State<AppState>,
    Json(body): Json<CompactRequest>,
) -> Result<Json<CompactResponse>, AppError> {
    check_api_version(body.api_version)?;
    let Resolved { mut threads, .. } = resolve(
        &state,
        vec![body.thread],
        body.machine.as_deref(),
        false,
        None,
        body.carried_by.as_deref(),
    )
    .await?;
    let thread = threads.pop().ok_or_else(|| AppError::Internal {
        message: "thread lookup returned no thread".into(),
    })?;
    let (outcome, detail) = compact_resolved(&state, &thread, Trigger::Manual).await?;
    // an agent reads the exit code: no session to compact is a missing thread,
    // while a refusal or an unreachable T3 is an answer about one that exists
    if outcome == CompactOutcome::NotFound {
        return Err(AppError::ThreadNotFound {
            reference: thread
                .reference
                .clone()
                .unwrap_or_else(|| thread.exact_reference()),
            detail: format!(
                "; {}",
                detail
                    .as_deref()
                    .unwrap_or("no open T3 thread owns its session")
            ),
        });
    }
    Ok(Json(CompactResponse {
        api_version: API_VERSION,
        outcome,
        detail,
        thread,
    }))
}

/// Compact a looked-up thread on the machine whose T3 owns it
///
/// # Errors
///
/// `compact_unsupported_host` for a thread outside T3, and the transport
/// errors of a forward
pub(crate) async fn compact_resolved(
    state: &AppState,
    thread: &ThreadContext,
    trigger: Trigger,
) -> Result<(CompactOutcome, Option<String>), AppError> {
    if thread.host != ThreadHost::T3 {
        return Err(AppError::CompactUnsupportedHost {
            reference: thread
                .reference
                .clone()
                .unwrap_or_else(|| thread.exact_reference()),
            host: thread.host,
        });
    }
    let Some(session) = thread.session else {
        return Ok((
            CompactOutcome::NotFound,
            Some("the T3 thread has not started a session yet".into()),
        ));
    };
    let local = state.machine.identity.machine;
    if thread.machine == local {
        let log = state.home.compaction_log_path();
        return run_blocking(move || compact_session(&T3Env::from_env(), session, &log, trigger))
            .await;
    }

    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| AppError::MachineNotFound {
            machine: thread.machine.to_string(),
        })?;
    let verified = fleet.connect(thread.machine).await?;
    let wire = ClusterCompact {
        api_version: API_VERSION,
        protocol_version: verified.protocol.0,
        destination_machine: thread.machine,
        origin_machine: local,
        session,
        watch: trigger == Trigger::Watch,
    };
    // T3 drops a repeated compaction of the same idle period, so a retry is safe
    let response = ClusterClient::new(COMPACT_TIMEOUT, DEFAULT_MAX_BODY)
        .post_json(&verified.address, CLUSTER_COMPACT_PATH, &wire)
        .await
        .map_err(|error| AppError::MachineUnavailable {
            machine: thread.machine,
            message: error.to_string(),
        })?;
    if !response.status.is_success() {
        // a peer without the route answers with the web listener's `not_found`
        let route_missing = response.status == StatusCode::NOT_FOUND
            && serde_json::from_slice::<serde_json::Value>(&response.body).is_ok_and(|body| {
                body.pointer("/error/code").and_then(|code| code.as_str()) == Some("not_found")
            });
        if route_missing {
            return Err(AppError::Usage {
                message: format!(
                    "{} runs a homebased without `thread compact`; run `homebased update` there",
                    thread.machine_name
                ),
            });
        }
        return Err(crate::client::map_error(response.status, &response.body));
    }
    let body: ClusterCompactBody = serde_json::from_slice(&response.body)?;
    if body.api_version != API_VERSION || body.protocol_version != wire.protocol_version {
        return Err(AppError::Internal {
            message: format!("{} answered with another version", thread.machine_name),
        });
    }
    if body.machine != thread.machine {
        return Err(AppError::MachineIdentityMismatch {
            expected: thread.machine,
            found: Some(body.machine),
        });
    }
    Ok((body.outcome, body.detail))
}

async fn cluster_compact(
    State(state): State<AppState>,
    Json(body): Json<ClusterCompact>,
) -> Result<Json<ClusterCompactBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.origin_machine)?;
    let log = state.home.compaction_log_path();
    let trigger = if body.watch {
        Trigger::Watch
    } else {
        Trigger::Manual
    };
    let session = body.session;
    let (outcome, detail) =
        run_blocking(move || compact_session(&T3Env::from_env(), session, &log, trigger)).await?;
    Ok(Json(ClusterCompactBody {
        api_version: API_VERSION,
        protocol_version: body.protocol_version,
        machine: state.machine.identity.machine,
        outcome,
        detail,
    }))
}

/// What a watch of `thread` follows
///
/// A Claude or Codex id names one session, which the watch follows until it
/// is gone; every other reference follows the T3 thread, whose session can change
fn watch_target(thread: &ThreadContext) -> Result<WatchTarget, AppError> {
    let by_session = thread
        .reference
        .as_deref()
        .and_then(|text| ThreadRef::parse(text).ok())
        .and_then(|reference| reference.uuid())
        .is_some_and(|uuid| {
            thread.session.map(|session| session.0) == Some(uuid)
                && thread.t3_thread.as_deref() != Some(uuid.to_string().as_str())
        });
    match (&thread.t3_thread, thread.session, thread.provider) {
        (Some(t3_thread), _, _) if !by_session => Ok(WatchTarget::T3 {
            machine: thread.machine,
            t3_thread: t3_thread.clone(),
        }),
        (_, Some(session), Some(provider)) => Ok(WatchTarget::Session {
            machine: thread.machine,
            provider,
            session,
        }),
        _ => Err(AppError::Internal {
            message: "a looked-up thread has neither a T3 id nor a session".into(),
        }),
    }
}

async fn watch(
    State(state): State<AppState>,
    Json(body): Json<WatchRequest>,
) -> Result<Json<WatchesResponse>, AppError> {
    check_api_version(body.api_version)?;
    if body.caller.is_detached() {
        return Err(AppError::Usage {
            message: "a watch reports to the calling thread; run it from an agent session or pass --thread".into(),
        });
    }
    if body.threads.is_empty() && !body.children {
        return Err(AppError::Usage {
            message: "name a thread to watch, or pass --children".into(),
        });
    }
    crate::spec::check_cwd(&body.callback_cwd)?;
    let children_of = body.children.then(|| (body.caller_ids.clone(), true));
    let Resolved {
        threads,
        children,
        unanswered,
    } = resolve(
        &state,
        body.threads,
        body.machine.as_deref(),
        false,
        children_of,
        body.caller_ids.t3_thread.as_deref(),
    )
    .await?;
    // saving only the children that answered would leave the rest unwatched without a word
    if body.children && !unanswered.is_empty() {
        return Err(AppError::ThreadLookupIncomplete { unanswered });
    }

    // every target is checked before any watch is saved, so a refusal saves none
    let mut targets = Vec::new();
    for thread in threads.iter().chain(&children) {
        if body.action == WatchAction::Compact && thread.host != ThreadHost::T3 {
            return Err(AppError::CompactUnsupportedHost {
                reference: thread
                    .reference
                    .clone()
                    .unwrap_or_else(|| thread.exact_reference()),
                host: thread.host,
            });
        }
        if matches!(body.threshold, Threshold::Percent(_))
            && thread.provider == Some(Provider::Claude)
        {
            return Err(AppError::Usage {
                message: format!(
                    "Claude Code records no context window, so a percent limit cannot apply to {}; pass a token count such as 150k",
                    thread.exact_reference()
                ),
            });
        }
        targets.push((watch_target(thread)?, thread));
    }
    if targets.is_empty() {
        return Ok(Json(WatchesResponse {
            api_version: API_VERSION,
            watches: Vec::new(),
        }));
    }

    let codex = call(&state.supervisor, |reply| SupervisorMsg::CallbackCodex {
        path: body.env.path.clone(),
        cwd: body.callback_cwd.clone(),
        reply,
    })
    .await?;
    let callback = CallbackContext {
        env: body.env,
        cwd: body.callback_cwd,
        codex,
    };
    let now = Utc::now();
    let mut watches = Vec::with_capacity(targets.len());
    for (target, thread) in targets {
        let watch = ThreadWatch {
            id: WatchId::new(),
            caller: body.caller,
            callback: callback.clone(),
            target,
            reference: thread
                .reference
                .clone()
                .unwrap_or_else(|| thread.exact_reference()),
            title: thread.title.clone(),
            threshold: body.threshold,
            action: body.action,
            once: body.once,
            created_at: now,
        };
        let (record, created) = call(&state.store, |reply| StoreMsg::UpsertWatch {
            watch: Box::new(watch),
            reply,
        })
        .await?;
        watches.push(record.view(created));
    }
    Ok(Json(WatchesResponse {
        api_version: API_VERSION,
        watches,
    }))
}

/// `GET /v1/threads/watches` query
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchesQuery {
    /// Only this caller's watches
    caller: Option<ThreadId>,
    /// Ended watches too
    #[serde(default)]
    all: bool,
}

async fn watches(
    State(state): State<AppState>,
    Query(query): Query<WatchesQuery>,
) -> Result<Json<WatchesResponse>, AppError> {
    let records = call(&state.store, |reply| StoreMsg::ListWatches {
        caller: query.caller,
        ended: query.all,
        reply,
    })
    .await?;
    Ok(Json(WatchesResponse {
        api_version: API_VERSION,
        watches: records.iter().map(|record| record.view(false)).collect(),
    }))
}

async fn unwatch(
    State(state): State<AppState>,
    Path(id): Path<WatchId>,
) -> Result<Json<WatchesResponse>, AppError> {
    let record = call(&state.store, |reply| StoreMsg::GetWatch { id, reply })
        .await?
        .ok_or(AppError::WatchNotFound { watch: id })?;
    // ending twice changes nothing, so a repeated unwatch just reports the watch
    if record.ended_at.is_none() {
        let update = watch_scan::ended(
            state.machine.identity.machine,
            &record,
            WatchEndReason::Unwatched,
            None,
        );
        call(&state.store, |reply| StoreMsg::UpdateWatch {
            id,
            update,
            reply,
        })
        .await?;
    }
    let record = call(&state.store, |reply| StoreMsg::GetWatch { id, reply })
        .await?
        .ok_or(AppError::WatchNotFound { watch: id })?;
    Ok(Json(WatchesResponse {
        api_version: API_VERSION,
        watches: vec![record.view(false)],
    }))
}
