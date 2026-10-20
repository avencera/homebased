//! Shared fan-out for the dashboard's fleet-wide views
//!
//! The browser reads one origin only, so the serving daemon reads every known
//! peer in parallel and reports each machine's result. A peer that cannot
//! answer is listed with its reason and never hides the other machines

use std::future::Future;

use futures_util::FutureExt;
use serde::Serialize;
use tokio::task::JoinSet;
use tracing::warn;

use super::AppState;
use crate::fleet::address::MachineAddress;
use crate::fleet::directory::{PeerView, Reachability};
use crate::fleet::runtime::FleetHandle;
use crate::machine::{MachineId, MachineName};

/// One machine of a fleet-wide view
#[derive(Debug, Serialize)]
pub struct FleetMachine {
    /// Stable installation UUID
    pub machine: MachineId,
    /// Display name
    pub name: MachineName,
    /// Homebased version of the machine's daemon, from the last probe for a peer
    pub version: String,
    /// Where a browser opens this machine's dashboard
    pub location: MachineLocation,
    /// Result of reading this machine
    pub read: MachineRead,
}

/// Dashboard location of one machine
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MachineLocation {
    /// The daemon that serves this response, so the browser uses its own origin
    Local,
    /// A peer dashboard at its best-ranked address, when one is known
    Peer {
        /// Base URL of the peer daemon
        address: Option<MachineAddress>,
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

/// Every machine of one fleet-wide read and the answers of those that responded
pub(super) struct FleetRead<T> {
    /// This machine first, then every known peer in directory order
    pub(super) machines: Vec<FleetMachine>,
    /// Answers in the same order, without the machines that failed
    pub(super) answers: Vec<(MachineId, T)>,
}

/// Combine this machine's answer with a parallel read of every known peer
///
/// `read` is called only for peers, so it gets the fleet handle that found them
///
/// A peer the directory already marks offline is not dialled, so one machine
/// that is down does not hold every refresh for the full read timeout
pub(super) async fn read_fleet<T, F, Fut>(state: &AppState, local: T, read: F) -> FleetRead<T>
where
    T: Send + 'static,
    F: Fn(&FleetHandle, MachineId) -> Fut,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    let local_machine = state.machine.identity.machine;
    let mut machines = vec![FleetMachine {
        machine: local_machine,
        name: state.machine.name.clone(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        location: MachineLocation::Local,
        read: MachineRead::Online,
    }];
    let mut answers = vec![(local_machine, local)];

    for (peer, result) in read_peers(state, read).await {
        let read = match result {
            Ok(answer) => {
                answers.push((peer.machine, answer));
                MachineRead::Online
            }
            Err(message) => MachineRead::Unavailable { message },
        };
        machines.push(FleetMachine {
            machine: peer.machine,
            name: peer.name.clone(),
            version: peer.version.clone(),
            location: MachineLocation::Peer {
                address: peer.addresses.first().map(|ranked| ranked.address.clone()),
            },
            read,
        });
    }

    FleetRead { machines, answers }
}

/// Read every known peer in parallel, in directory order
async fn read_peers<T, F, Fut>(state: &AppState, read: F) -> Vec<(PeerView, Result<T, String>)>
where
    T: Send + 'static,
    F: Fn(&FleetHandle, MachineId) -> Fut,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    let Some(fleet) = state.fleet.handle() else {
        return Vec::new();
    };
    let local = state.machine.identity.machine;
    let peers: Vec<PeerView> = fleet
        .peers()
        .await
        .into_iter()
        .filter(|peer| peer.machine != local)
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
