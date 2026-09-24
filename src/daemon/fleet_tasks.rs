//! Fleet-wide task list for the dashboard
//!
//! The browser reads one origin only, so this daemon reads every known peer and
//! merges their public task summaries. A peer that cannot answer is reported
//! with its reason and never hides the tasks of the other machines

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tracing::warn;

use super::AppState;
use super::api::views::TaskSummary;
use super::api::{ListQuery, TaskFilter, local_task_summaries};
use super::cluster::check_api_version;
use super::peer_read::read_peer;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::address::MachineAddress;
use crate::fleet::directory::{PeerView, Reachability};
use crate::machine::{MachineId, MachineName};

/// Peer route that answers one daemon's own task summaries
const CLUSTER_TASK_LIST_PATH: &str = "/v1/cluster/task-list";

/// `GET /v1/fleet/tasks`
#[derive(Debug, Serialize)]
pub struct FleetTaskList {
    /// Public API version
    pub api_version: u32,
    /// This machine first, then every known peer and how its read went
    pub machines: Vec<FleetMachine>,
    /// One entry per task UUID, newest first
    pub tasks: Vec<FleetTask>,
}

/// One machine of the fleet task list
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
    /// Result of reading this machine's tasks
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

/// Result of one machine's task read
#[derive(Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MachineRead {
    /// The machine answered; its tasks are in the list
    Online,
    /// The machine did not answer; its tasks are missing from the list
    Unavailable {
        /// Why the read failed
        message: String,
    },
}

/// One logical task and the machine that runs it
#[derive(Debug, Serialize)]
pub struct FleetTask {
    /// Execution machine, or the machine that answered when the task has no owners
    pub machine: MachineId,
    /// Summary from the execution record when one answered
    pub task: TaskSummary,
}

/// `GET /v1/cluster/task-list` answer: one daemon's own tasks
#[derive(Debug, Serialize, Deserialize)]
struct ClusterTaskList {
    api_version: u32,
    /// Machine that answered, checked against the destination
    machine: MachineId,
    tasks: Vec<TaskSummary>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterTaskListQuery {
    api_version: u32,
    destination_machine: MachineId,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    thread: Option<String>,
}

/// Public read route, safe on the TCP listener
pub fn read_routes() -> Router<AppState> {
    Router::new().route("/v1/fleet/tasks", get(fleet_tasks))
}

/// Peer route for an enabled fleet
pub fn cluster_routes() -> Router<AppState> {
    Router::new().route(CLUSTER_TASK_LIST_PATH, get(cluster_task_list))
}

async fn fleet_tasks(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<FleetTaskList>, AppError> {
    let filter = TaskFilter::parse(query)?;
    let local = state.machine.identity.machine;
    let local_tasks = local_task_summaries(&state, filter.clone()).await?;

    let mut machines = vec![FleetMachine {
        machine: local,
        name: state.machine.name.clone(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        location: MachineLocation::Local,
        read: MachineRead::Online,
    }];
    let mut records = vec![(local, local_tasks)];
    for (peer, result) in read_peers(&state, &filter).await {
        let read = match result {
            Ok(tasks) => {
                records.push((peer.machine, tasks));
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

    Ok(Json(FleetTaskList {
        api_version: API_VERSION,
        machines,
        tasks: merge_records(records),
    }))
}

async fn cluster_task_list(
    State(state): State<AppState>,
    Query(query): Query<ClusterTaskListQuery>,
) -> Result<Json<ClusterTaskList>, AppError> {
    state
        .machine
        .identity
        .check_destination(query.destination_machine)?;
    check_api_version(query.api_version)?;
    let filter = TaskFilter::parse(ListQuery {
        status: query.status,
        thread: query.thread,
    })?;
    Ok(Json(ClusterTaskList {
        api_version: API_VERSION,
        machine: state.machine.identity.machine,
        tasks: local_task_summaries(&state, filter).await?,
    }))
}

/// Read every known peer in parallel, in directory order
///
/// A peer the directory already marks offline is not dialled, so one machine
/// that is down does not hold every refresh for the full read timeout
async fn read_peers(
    state: &AppState,
    filter: &TaskFilter,
) -> Vec<(PeerView, Result<Vec<TaskSummary>, String>)> {
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
        let fleet = fleet.clone();
        let machine = peer.machine;
        let path = format!(
            "{CLUSTER_TASK_LIST_PATH}?{}api_version={API_VERSION}&destination_machine={machine}",
            filter.query_prefix()
        );
        jobs.spawn(async move {
            let result = read_peer::<ClusterTaskList>(&fleet, machine, &path)
                .await
                .and_then(|body| {
                    if body.machine == machine {
                        Ok(body.tasks)
                    } else {
                        Err("peer answered for another machine".to_owned())
                    }
                });
            (index, result)
        });
    }

    let mut results: Vec<Result<Vec<TaskSummary>, String>> = peers
        .iter()
        .map(|_| Err("machine is offline".to_owned()))
        .collect();
    while let Some(joined) = jobs.join_next().await {
        match joined {
            Ok((index, result)) => results[index] = result,
            Err(error) => warn!("fleet task read worker failed: {error}"),
        }
    }
    peers.into_iter().zip(results).collect()
}

/// Merge per-machine records into one entry per task, newest first
///
/// A remote task has an origin record and an execution record. The execution
/// record wins because it owns the process state
fn merge_records(records: Vec<(MachineId, Vec<TaskSummary>)>) -> Vec<FleetTask> {
    let mut by_id: HashMap<_, (bool, FleetTask)> = HashMap::new();
    for (answered_by, tasks) in records {
        for task in tasks {
            let machine = task.execution_machine.unwrap_or(answered_by);
            let is_execution = machine == answered_by;
            let id = task.id;
            let replace = by_id
                .get(&id)
                .is_none_or(|(kept_is_execution, _)| is_execution && !kept_is_execution);
            if replace {
                by_id.insert(id, (is_execution, FleetTask { machine, task }));
            }
        }
    }
    let mut tasks: Vec<FleetTask> = by_id.into_values().map(|(_, task)| task).collect();
    tasks.sort_by(|left, right| {
        right
            .task
            .created_at
            .cmp(&left.task.created_at)
            .then_with(|| right.task.id.0.cmp(&left.task.id.0))
    });
    tasks
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const MAIN: &str = "00000000-0000-4000-8000-000000000001";
    const CODE: &str = "00000000-0000-4000-8000-000000000002";

    fn machine(id: &str) -> MachineId {
        id.parse().unwrap()
    }

    fn summary(
        id: &str,
        created_at: &str,
        status: &str,
        owners: Option<(&str, &str)>,
    ) -> TaskSummary {
        let mut value = json!({
            "id": id,
            "display_name": "train",
            "status": status,
            "workload": { "type": "task", "command": ["true"] },
            "thread": "01a0b19f-f048-7832-8e98-01618ccf44d7",
            "cwd": "/tmp",
            "pid": null,
            "callback": "pending",
            "timeout_secs": 60,
            "check_timeout": "pending",
            "exit_reason": null,
            "cancel_requested_at": null,
            "created_at": created_at,
            "updated_at": created_at,
        });
        if let Some((origin, execution)) = owners {
            value["origin_machine"] = json!(origin);
            value["execution_machine"] = json!(execution);
        }
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn execution_record_wins_over_origin_record() {
        let id = "01a0d1fd-90c9-771f-bc1c-2eb6f9d8faf8";
        let at = "2026-09-24T05:57:30Z";
        let origin_copy = summary(id, at, "queued", Some((MAIN, CODE)));
        let execution_copy = summary(id, at, "running", Some((MAIN, CODE)));

        let merged = merge_records(vec![
            (machine(MAIN), vec![origin_copy]),
            (machine(CODE), vec![execution_copy]),
        ]);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].machine, machine(CODE));
        assert_eq!(merged[0].task.status.as_str(), "running");
    }

    #[test]
    fn origin_record_stands_in_while_the_executor_is_unavailable() {
        let id = "01a0d1fd-90c9-771f-bc1c-2eb6f9d8faf8";
        let origin_copy = summary(id, "2026-09-24T05:57:30Z", "queued", Some((MAIN, CODE)));

        let merged = merge_records(vec![(machine(MAIN), vec![origin_copy])]);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].machine, machine(CODE));
    }

    #[test]
    fn tasks_are_newest_first_across_machines() {
        let older = summary(
            "01a0d100-0000-7000-8000-000000000001",
            "2026-09-24T01:00:00Z",
            "running",
            None,
        );
        let newer = summary(
            "01a0d100-0000-7000-8000-000000000002",
            "2026-09-24T02:00:00Z",
            "running",
            None,
        );

        let merged = merge_records(vec![
            (machine(MAIN), vec![older]),
            (machine(CODE), vec![newer]),
        ]);

        let order: Vec<MachineId> = merged.iter().map(|entry| entry.machine).collect();
        assert_eq!(order, vec![machine(CODE), machine(MAIN)]);
    }

    #[test]
    fn peer_task_list_round_trips_a_container_workload() {
        let body = json!({
            "api_version": API_VERSION,
            "machine": CODE,
            "tasks": [{
                "id": "01a0d100-0000-7000-8000-000000000003",
                "display_name": "train",
                "status": "running",
                "workload": { "type": "container", "image": "img@sha256:00", "args": [], "gpus": "all" },
                "thread": "01a0b19f-f048-7832-8e98-01618ccf44d7",
                "cwd": "/tmp",
                "pid": 7,
                "callback": "pending",
                "timeout_secs": 60,
                "check_timeout": "pending",
                "exit_reason": null,
                "cancel_requested_at": null,
                "created_at": "2026-09-24T01:00:00Z",
                "updated_at": "2026-09-24T01:00:00Z",
            }],
        });

        let list: ClusterTaskList = serde_json::from_value(body.clone()).unwrap();

        assert_eq!(serde_json::to_value(&list).unwrap(), body);
    }
}
