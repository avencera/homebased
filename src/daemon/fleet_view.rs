//! Shared fan-out for the dashboard's fleet-wide views
//!
//! The browser reads one origin only, so the serving daemon reads every known
//! peer in parallel and reports each machine's result. A peer that cannot
//! answer is listed with its reason and never hides the other machines
//!
//! A view's `machine` query narrows the read to one machine. The dashboard
//! reads the roster, then each machine on its own, so each one's rows land
//! when that machine answers

use std::future::Future;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::FutureExt;
use serde::Serialize;
use tokio::task::JoinSet;
use tracing::warn;

use super::AppState;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::address::{DashboardUrl, MachineAddress};
use crate::fleet::directory::{PeerView, Reachability};
use crate::fleet::runtime::FleetHandle;
use crate::machine::{MachineId, MachineName};

/// `GET /v1/fleet/roster`
#[derive(Debug, Serialize)]
pub struct Roster {
    /// Public API version
    pub api_version: u32,
    /// This machine first, then every known peer in directory order
    pub machines: Vec<ViewMachine>,
}

/// One machine a fleet-wide view lists, before any read
#[derive(Debug, Clone, Serialize)]
pub struct ViewMachine {
    /// Stable installation UUID
    pub machine: MachineId,
    /// Display name
    pub name: MachineName,
    /// Homebased version of the machine's daemon, from the last probe for a peer
    pub version: String,
    /// Where a browser opens this machine's dashboard
    pub location: MachineLocation,
}

/// One machine of a fleet-wide view
#[derive(Debug, Serialize)]
pub struct FleetMachine {
    /// The machine as the roster lists it
    #[serde(flatten)]
    pub view: ViewMachine,
    /// Result of reading this machine
    pub read: MachineRead,
}

/// Dashboard location of one machine
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MachineLocation {
    /// The daemon that serves this response, so the browser uses its own origin
    Local,
    /// A peer dashboard at its best-ranked address, when one is known
    Peer {
        /// Base URL of the peer daemon
        address: Option<MachineAddress>,
        /// HTTPS URL of the peer's dashboard, for links from an HTTPS page
        dashboard_url: Option<DashboardUrl>,
    },
}

/// Result of one machine's read
#[derive(Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MachineRead {
    /// The machine answered; its entries are in the view
    Online,
    /// The machine did not answer; its entries are missing from the view
    Unavailable {
        /// Why the read failed
        message: String,
    },
}

/// Which machines one fleet-wide read covers
///
/// The dashboard reads one machine per request, so a slow peer delays only its
/// own rows instead of the whole page
#[derive(Debug, Clone, Copy)]
pub(super) enum Scope {
    /// This machine and every known peer
    All,
    /// One machine, local or a known peer
    One(MachineId),
}

impl Scope {
    /// `machine` from a view query, where none means every machine
    pub(super) fn from_query(machine: Option<MachineId>) -> Self {
        machine.map_or(Self::All, Self::One)
    }

    fn covers(self, machine: MachineId) -> bool {
        match self {
            Self::All => true,
            Self::One(only) => only == machine,
        }
    }
}

/// Every machine of one fleet-wide read and the answers of those that responded
pub(super) struct FleetRead<T> {
    /// The machines `scope` covers, this machine first, then peers in directory order
    pub(super) machines: Vec<FleetMachine>,
    /// Answers in the same order, without the machines that failed
    pub(super) answers: Vec<(MachineId, T)>,
}

/// Public read route, safe on the TCP listener
pub fn read_routes() -> Router<AppState> {
    Router::new().route("/v1/fleet/roster", get(roster))
}

async fn roster(State(state): State<AppState>) -> Json<Roster> {
    let mut machines = vec![local_view(&state)];
    machines.extend(known_peers(&state).await.iter().map(peer_view));
    Json(Roster {
        api_version: API_VERSION,
        machines,
    })
}

fn local_view(state: &AppState) -> ViewMachine {
    ViewMachine {
        machine: state.machine.identity.machine,
        name: state.machine.name.clone(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        location: MachineLocation::Local,
    }
}

fn peer_view(peer: &PeerView) -> ViewMachine {
    ViewMachine {
        machine: peer.machine,
        name: peer.name.clone(),
        version: peer.version.clone(),
        location: MachineLocation::Peer {
            address: peer.addresses.first().map(|ranked| ranked.address.clone()),
            dashboard_url: peer.dashboard_url.clone(),
        },
    }
}

/// Every peer in the directory, without this machine
async fn known_peers(state: &AppState) -> Vec<PeerView> {
    let Some(fleet) = state.fleet.handle() else {
        return Vec::new();
    };
    let local = state.machine.identity.machine;
    fleet
        .peers()
        .await
        .into_iter()
        .filter(|peer| peer.machine != local)
        .collect()
}

/// Combine this machine's answer with a parallel read of the peers `scope` covers
///
/// `local` runs only when `scope` covers this machine, and `read` is called only
/// for peers, so it gets the fleet handle that found them. A machine `scope`
/// names that is neither this one nor a known peer is not found
///
/// A peer the directory already marks offline is not dialled, so one machine
/// that is down does not hold every refresh for the full read timeout
pub(super) async fn read_fleet<T, L, F, Fut>(
    state: &AppState,
    scope: Scope,
    local: L,
    read: F,
) -> Result<FleetRead<T>, AppError>
where
    T: Send + 'static,
    L: Future<Output = Result<T, AppError>>,
    F: Fn(&FleetHandle, MachineId) -> Fut,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    let local_machine = state.machine.identity.machine;
    let mut machines = Vec::new();
    let mut answers = Vec::new();
    if scope.covers(local_machine) {
        answers.push((local_machine, local.await?));
        machines.push(FleetMachine {
            view: local_view(state),
            read: MachineRead::Online,
        });
    }

    let peers = read_peers(state, scope, read).await;
    if let Scope::One(machine) = scope
        && machines.is_empty()
        && peers.is_empty()
    {
        return Err(AppError::MachineNotFound {
            machine: machine.to_string(),
        });
    }

    for (peer, result) in peers {
        let read = match result {
            Ok(answer) => {
                answers.push((peer.machine, answer));
                MachineRead::Online
            }
            Err(message) => MachineRead::Unavailable { message },
        };
        machines.push(FleetMachine {
            view: peer_view(&peer),
            read,
        });
    }

    Ok(FleetRead { machines, answers })
}

/// Read the known peers `scope` covers in parallel, in directory order
async fn read_peers<T, F, Fut>(
    state: &AppState,
    scope: Scope,
    read: F,
) -> Vec<(PeerView, Result<T, String>)>
where
    T: Send + 'static,
    F: Fn(&FleetHandle, MachineId) -> Fut,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    let Some(fleet) = state.fleet.handle() else {
        return Vec::new();
    };
    let peers: Vec<PeerView> = known_peers(state)
        .await
        .into_iter()
        .filter(|peer| scope.covers(peer.machine))
        .collect();

    let mut jobs = JoinSet::new();
    for (index, peer) in peers.iter().enumerate() {
        if matches!(peer.reachability, Reachability::Offline { .. }) {
            continue;
        }
        jobs.spawn(peer_worker(index, read(fleet, peer.machine)));
    }

    let mut results: Vec<Result<T, String>> = peers
        .iter()
        .map(|_| Err("machine is offline".to_owned()))
        .collect();
    while let Some(joined) = jobs.join_next().await {
        match joined {
            Ok((index, result)) => results[index] = result,
            Err(error) => warn!("fleet peer read worker failed: {error}"),
        }
    }

    peers.into_iter().zip(results).collect()
}

async fn peer_worker<T>(
    index: usize,
    read: impl Future<Output = Result<T, String>>,
) -> (usize, Result<T, String>) {
    let result = std::panic::AssertUnwindSafe(read)
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err("peer read worker panicked".into()));
    (index, result)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn bugfix_peer_panic_names_the_worker_failure() {
        let mut workers = tokio::task::JoinSet::new();
        workers.spawn(super::peer_worker::<()>(7, async {
            panic!("peer read panic")
        }));
        let (index, result) = workers.join_next().await.unwrap().unwrap();
        assert_eq!(index, 7);
        assert!(result.unwrap_err().contains("worker panicked"));
    }
}
