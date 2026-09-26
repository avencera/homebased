//! Thread titles for the dashboard
//!
//! A title lives on the machine that runs the thread, so this daemon reads its
//! own agent stores and asks each peer for the threads it owns. A title is only
//! a label: a store or peer that cannot answer gives no title and never fails
//! the read

use std::collections::{HashMap, HashSet};

use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tracing::{debug, warn};

use super::AppState;
use super::cluster::check_api_version;
use super::peer_read::read_peer;
use crate::domain::{API_VERSION, ThreadId};
use crate::error::AppError;
use crate::fleet::directory::Reachability;
use crate::machine::MachineId;
use crate::thread_title::TitleSources;

/// Peer route that answers titles of threads on that machine
const CLUSTER_THREAD_TITLES_PATH: &str = "/v1/cluster/thread-titles";

/// Most threads one read may name, which also bounds the peer query string
const MAX_THREADS: usize = 200;

/// Read this machine's titles of `threads`
///
/// No cache: the dashboard reads each title at most every 30 seconds, and a
/// Claude Code transcript is read only when T3 Code has no title
async fn local_titles(
    sources: Option<TitleSources>,
    threads: Vec<ThreadId>,
) -> HashMap<ThreadId, Option<String>> {
    let Some(sources) = sources else {
        return threads.into_iter().map(|thread| (thread, None)).collect();
    };
    let read = {
        let threads = threads.clone();
        tokio::task::spawn_blocking(move || sources.titles(&threads)).await
    };
    let titles = read.unwrap_or_else(|error| {
        warn!("thread title read failed: {error}");
        vec![None; threads.len()]
    });
    threads.into_iter().zip(titles).collect()
}

/// One thread and the machine that runs it
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ThreadRef {
    machine: MachineId,
    thread: ThreadId,
}

/// One thread to name in `POST /v1/fleet/thread-titles`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadQuery {
    /// Machine that runs the thread; omitted for the machine that serves the read
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<MachineId>,
    /// Codex thread or Claude Code session
    pub thread: ThreadId,
}

/// `POST /v1/fleet/thread-titles` body
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadTitlesQuery {
    /// Threads to name, at most 200
    pub threads: Vec<ThreadQuery>,
}

/// `POST /v1/fleet/thread-titles` answer
#[derive(Debug, Serialize)]
pub struct FleetThreadTitles {
    /// Public API version
    pub api_version: u32,
    /// One entry per distinct requested thread
    pub titles: Vec<ThreadTitle>,
}

/// Title of one thread
#[derive(Debug, Serialize)]
pub struct ThreadTitle {
    /// The requested thread, with its machine as the request named it
    #[serde(flatten)]
    pub query: ThreadQuery,
    /// T3 Code title, else the agent's own title; `None` when no store or
    /// machine names the thread
    pub title: Option<String>,
}

/// `GET /v1/cluster/thread-titles` answer: titles of one machine's threads
#[derive(Debug, Serialize, Deserialize)]
struct ClusterThreadTitles {
    api_version: u32,
    /// Machine that answered, checked against the destination
    machine: MachineId,
    titles: Vec<ClusterThreadTitle>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterThreadTitle {
    thread: ThreadId,
    title: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterThreadTitlesQuery {
    api_version: u32,
    destination_machine: MachineId,
    /// Comma-separated thread UUIDs
    threads: String,
}

/// Public read route, safe on the TCP listener
pub fn read_routes() -> Router<AppState> {
    Router::new().route("/v1/fleet/thread-titles", post(fleet_thread_titles))
}

/// Peer route for an enabled fleet
pub fn cluster_routes() -> Router<AppState> {
    Router::new().route(CLUSTER_THREAD_TITLES_PATH, get(cluster_thread_titles))
}

async fn fleet_thread_titles(
    State(state): State<AppState>,
    Json(query): Json<ThreadTitlesQuery>,
) -> Result<Json<FleetThreadTitles>, AppError> {
    let requested = distinct(query.threads)?;
    let local = state.machine.identity.machine;
    let resolve = |query: &ThreadQuery| ThreadRef {
        machine: query.machine.unwrap_or(local),
        thread: query.thread,
    };
    let mut by_machine: HashMap<MachineId, Vec<ThreadId>> = HashMap::new();
    for thread in requested.iter().map(resolve).collect::<HashSet<_>>() {
        by_machine
            .entry(thread.machine)
            .or_default()
            .push(thread.thread);
    }

    let mut titles: HashMap<ThreadRef, String> = HashMap::new();
    if let Some(threads) = by_machine.remove(&local) {
        for (thread, title) in local_titles(state.thread_titles.clone(), threads).await {
            if let Some(title) = title {
                titles.insert(
                    ThreadRef {
                        machine: local,
                        thread,
                    },
                    title,
                );
            }
        }
    }
    titles.extend(read_peers(&state, by_machine).await);

    Ok(Json(FleetThreadTitles {
        api_version: API_VERSION,
        titles: requested
            .into_iter()
            .map(|query| ThreadTitle {
                title: titles.get(&resolve(&query)).cloned(),
                query,
            })
            .collect(),
    }))
}

async fn cluster_thread_titles(
    State(state): State<AppState>,
    Query(query): Query<ClusterThreadTitlesQuery>,
) -> Result<Json<ClusterThreadTitles>, AppError> {
    let machine = state.machine.identity.machine;
    state
        .machine
        .identity
        .check_destination(query.destination_machine)?;
    check_api_version(query.api_version)?;
    let threads = query
        .threads
        .split(',')
        .filter(|thread| !thread.is_empty())
        .map(|thread| thread.parse::<ThreadId>())
        .collect::<Result<Vec<_>, _>>()?;
    let threads = distinct(threads)?;
    let titles = local_titles(state.thread_titles.clone(), threads).await;
    Ok(Json(ClusterThreadTitles {
        api_version: API_VERSION,
        machine,
        titles: titles
            .into_iter()
            .map(|(thread, title)| ClusterThreadTitle { thread, title })
            .collect(),
    }))
}

/// Requested threads without repeats, in first-seen order
fn distinct<T: Copy + Eq + std::hash::Hash>(threads: Vec<T>) -> Result<Vec<T>, AppError> {
    let mut seen = HashSet::new();
    let threads: Vec<T> = threads
        .into_iter()
        .filter(|thread| seen.insert(*thread))
        .collect();
    if threads.len() > MAX_THREADS {
        return Err(AppError::Usage {
            message: format!("at most {MAX_THREADS} threads per title read"),
        });
    }
    Ok(threads)
}

/// Ask each peer in parallel for the titles of its own threads
///
/// A peer the directory marks offline is not dialled. A peer that fails, or an
/// older peer without the route, leaves its threads untitled
async fn read_peers(
    state: &AppState,
    by_machine: HashMap<MachineId, Vec<ThreadId>>,
) -> HashMap<ThreadRef, String> {
    let Some(fleet) = state.fleet.handle() else {
        return HashMap::new();
    };
    let offline: HashSet<MachineId> = fleet
        .peers()
        .await
        .into_iter()
        .filter(|peer| matches!(peer.reachability, Reachability::Offline { .. }))
        .map(|peer| peer.machine)
        .collect();

    let mut jobs = JoinSet::new();
    for (machine, threads) in by_machine {
        if offline.contains(&machine) {
            continue;
        }
        let fleet = fleet.clone();
        let threads = threads
            .iter()
            .map(ThreadId::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let path = format!(
            "{CLUSTER_THREAD_TITLES_PATH}?api_version={API_VERSION}&destination_machine={machine}&threads={threads}"
        );
        jobs.spawn(async move {
            let result = read_peer::<ClusterThreadTitles>(&fleet, machine, &path).await;
            (machine, result)
        });
    }

    let mut titles = HashMap::new();
    while let Some(joined) = jobs.join_next().await {
        let (machine, body) = match joined {
            Ok((machine, Ok(body))) => (machine, body),
            Ok((machine, Err(message))) => {
                debug!("thread title read from {machine} failed: {message}");
                continue;
            }
            Err(error) => {
                warn!("thread title read worker failed: {error}");
                continue;
            }
        };
        if body.machine != machine {
            debug!("thread title read from {machine} answered for another machine");
            continue;
        }
        for entry in body.titles {
            if let Some(title) = entry.title {
                titles.insert(
                    ThreadRef {
                        machine,
                        thread: entry.thread,
                    },
                    title,
                );
            }
        }
    }
    titles
}
