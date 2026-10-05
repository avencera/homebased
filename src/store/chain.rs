//! Chains of agent runs that park on other tasks and continue when they end
//!
//! A chain is created when a run first parks and is advanced in the
//! transaction that ends one of its runs, so its state and the run rows never
//! disagree after a crash. Parking commits the run's terminal state, the
//! chain, and the continuation's held route together; the dependency release
//! loop launches the continuation like any held task

use std::collections::HashSet;
use std::path::Path;

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use tracing::warn;

use super::Store;
use super::dependency::{append_unlaunched_event_on, dependent_route_on};
use super::fmt_time;
use super::lifecycle::reports_from;
use crate::callback::UnlaunchedEnding;
use crate::dependency::{HeldCancellation, ReleaseRule, TaskDependencies, TaskOutcome};
use crate::domain::{API_VERSION, AgentKind, ReportOutcome, TaskId, TaskRow, ThreadId, Workload};
use crate::error::AppError;
use crate::spec::{NormalizedAgentWorkload, NormalizedSpec, NormalizedWorkload};
use crate::submission::{HeldPhase, NewHeldRoute, OriginRoute, RequestId, SubmissionState};
use crate::waiting::{
    ChainState, ChainView, MAX_CHAIN_RUNS, Parking, WaitTargets, WaitingRejection, WaitingReport,
    continuation_block, continuation_name,
};

/// Most nodes the cycle check visits; every node is a local task or chain
const CYCLE_SEARCH_LIMIT: usize = 10_000;

/// One run's chain as saved
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ChainRecord {
    /// Chain id, the first run's task id
    pub(super) id: TaskId,
    /// Position of the looked-up run
    pub(super) run: u32,
    /// Runs the chain has
    pub(super) runs: u32,
    /// Saved state
    pub(super) state: ChainState,
}

impl ChainRecord {
    fn view(&self) -> ChainView {
        ChainView {
            id: self.id,
            run: self.run,
            runs: self.runs,
            state: self.state.clone(),
        }
    }
}

fn parse_task(value: &str) -> Result<TaskId, AppError> {
    value.parse().map_err(|_| AppError::Internal {
        message: format!("invalid task id {value} in task chains"),
    })
}

/// The chain `task` belongs to, or `None` for a task that never parked or continued
pub(super) fn chain_of_on(
    conn: &Connection,
    task: TaskId,
) -> Result<Option<ChainRecord>, AppError> {
    let saved: Option<(String, String, u32, u32)> = conn
        .query_row(
            "SELECT c.id, c.state_json, r.run_number,
                 (SELECT COUNT(*) FROM chain_runs WHERE chain_id = c.id)
             FROM chain_runs r JOIN task_chains c ON c.id = r.chain_id
             WHERE r.task_id = ?1",
            [task.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((id, state, run, runs)) = saved else {
        return Ok(None);
    };
    Ok(Some(ChainRecord {
        id: parse_task(&id)?,
        run,
        runs,
        state: serde_json::from_str(&state)?,
    }))
}

fn save_state_on(conn: &Connection, chain: TaskId, state: &ChainState) -> Result<(), AppError> {
    conn.execute(
        "UPDATE task_chains SET state_json = ?1, updated_at = ?2 WHERE id = ?3",
        params![
            serde_json::to_string(state)?,
            fmt_time(Utc::now()),
            chain.to_string()
        ],
    )?;
    Ok(())
}

fn add_run_on(conn: &Connection, chain: TaskId, task: TaskId, run: u32) -> Result<(), AppError> {
    conn.execute(
        "INSERT INTO chain_runs (task_id, chain_id, run_number) VALUES (?1, ?2, ?3)",
        params![task.to_string(), chain.to_string(), run],
    )?;
    Ok(())
}

/// The chain of a run that is about to park, created on its first park
fn chain_for_parking_on(conn: &Connection, run: TaskId) -> Result<ChainRecord, AppError> {
    if let Some(chain) = chain_of_on(conn, run)? {
        return Ok(chain);
    }
    let state = ChainState::Running { current: run };
    conn.execute(
        "INSERT INTO task_chains (id, state_json, updated_at) VALUES (?1, ?2, ?3)",
        params![
            run.to_string(),
            serde_json::to_string(&state)?,
            fmt_time(Utc::now())
        ],
    )?;
    add_run_on(conn, run, run, 1)?;
    Ok(ChainRecord {
        id: run,
        run: 1,
        runs: 1,
        state,
    })
}

/// End the chain whose current run is `task`, in the transaction that saves its outcome
///
/// The current run is the running run, or the held continuation of a parked
/// chain when it ends before launch. Any other run of the chain already
/// handed the work on, so its ending changes nothing
pub(super) fn end_chain_on(
    conn: &Connection,
    task: TaskId,
    outcome: TaskOutcome,
) -> Result<(), AppError> {
    let Some(chain) = chain_of_on(conn, task)? else {
        return Ok(());
    };
    let owns_work = match &chain.state {
        ChainState::Running { current } => *current == task,
        ChainState::Waiting { continuation, .. } => *continuation == task,
        ChainState::Ended { .. } => false,
    };
    if owns_work {
        save_state_on(conn, chain.id, &ChainState::Ended { outcome })?;
    }
    Ok(())
}

/// Hand the work to a released continuation, in the transaction that saves its row
pub(super) fn start_continuation_on(conn: &Connection, task: TaskId) -> Result<(), AppError> {
    let Some(chain) = chain_of_on(conn, task)? else {
        return Ok(());
    };
    if let ChainState::Waiting { continuation, .. } = chain.state
        && continuation == task
    {
        save_state_on(conn, chain.id, &ChainState::Running { current: task })?;
    }
    Ok(())
}

/// What a parked run handed over, or `None` for a run that did not park
///
/// The continuation is the next run of the chain, and the targets are the
/// parked run's last report, so nothing is saved twice
pub(super) fn parking_on(conn: &Connection, run: TaskId) -> Result<Option<Parking>, AppError> {
    let Some(chain) = chain_of_on(conn, run)? else {
        return Ok(None);
    };
    let next: Option<String> = conn
        .query_row(
            "SELECT task_id FROM chain_runs WHERE chain_id = ?1 AND run_number = ?2",
            params![chain.id.to_string(), chain.run + 1],
            |row| row.get(0),
        )
        .optional()?;
    let Some(next) = next else {
        return Ok(None);
    };
    let reports = reports_from(conn, run)?;
    let Some(ReportOutcome::Waiting(waiting)) = reports.last().map(|report| &report.outcome) else {
        return Ok(None);
    };
    Ok(Some(Parking {
        on: waiting.on.clone(),
        continuation: parse_task(&next)?,
    }))
}

/// Park a run that exited 0 after a final waiting report
///
/// Saves the chain as waiting and the continuation as a held route that any
/// ending of every target releases. A run whose cancel was already requested
/// parks with its continuation cancelled, so the chain ends cancelled. `None`
/// means the run cannot park, which report validation prevents; its exit then
/// reads as failed
pub(super) fn park_on(
    conn: &Connection,
    tasks_dir: &Path,
    row: &TaskRow,
    waiting: &WaitingReport,
) -> Result<Option<Parking>, AppError> {
    let Some((route, _)) = dependent_route_on(conn, row.id)? else {
        warn!(task = %row.id, "waiting worker has no origin route here; not parking");
        return Ok(None);
    };
    let NormalizedWorkload::Agent(agent) = &route.spec.workload else {
        return Ok(None);
    };
    if route.origin_machine != route.execution_machine {
        return Ok(None);
    }
    let chain = chain_for_parking_on(conn, row.id)?;
    if chain.runs >= MAX_CHAIN_RUNS {
        warn!(task = %row.id, runs = chain.runs, "chain is full; not parking");
        return Ok(None);
    }
    let first = if chain.id == row.id {
        route.clone()
    } else {
        dependent_route_on(conn, chain.id)?
            .ok_or(AppError::RouteNotFound { task: chain.id })?
            .0
    };
    let NormalizedWorkload::Agent(first_agent) = &first.spec.workload else {
        return Ok(None);
    };

    let worker_thread = worker_thread_on(conn, row.id)?;
    let block = continuation_block(row.id, waiting);
    let (prompt, resume_thread) = match (agent.agent, worker_thread) {
        // the same Codex conversation already holds the original prompt
        (AgentKind::Codex, Some(thread)) => (block, Some(thread)),
        _ => (
            format!("{}\n\n{block}", first_agent.prompt.trim_end()),
            first_agent.resume_thread,
        ),
    };
    let continuation = TaskId::new();
    let spec = NormalizedSpec {
        api_version: API_VERSION,
        thread: route.spec.thread,
        name: continuation_name(&first.spec.name),
        cwd: route.spec.cwd.clone(),
        machine: None,
        timeout: route.spec.timeout,
        workload: NormalizedWorkload::Agent(NormalizedAgentWorkload {
            agent: agent.agent,
            model: agent.model.clone(),
            prompt,
            extra_args: agent.extra_args.clone(),
            report_trailer: agent.report_trailer,
            resume_thread,
        }),
    };
    let mut held = OriginRoute::new_held(NewHeldRoute {
        request: RequestId::new(),
        task: continuation,
        origin_machine: route.origin_machine,
        execution_machine: route.execution_machine,
        callback: route.callback.clone(),
        spec,
    });
    let after =
        TaskDependencies::new(waiting.on.tasks().to_vec()).map_err(|error| AppError::Internal {
            message: format!("waiting targets are not a dependency list: {error}"),
        })?;
    conn.execute(
        "INSERT INTO origin_routes (request_id, task_id, route_json, after_json, after_rule)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            held.request.0.to_string(),
            continuation.to_string(),
            serde_json::to_string(&held)?,
            serde_json::to_string(&after)?,
            ReleaseRule::Ended.as_str(),
        ],
    )?;
    add_run_on(conn, chain.id, continuation, chain.runs + 1)?;
    save_state_on(
        conn,
        chain.id,
        &ChainState::Waiting {
            current: row.id,
            on: waiting.on.clone(),
            continuation,
        },
    )?;

    if row.cancel_requested_at.is_some() {
        let cause = HeldCancellation::Requested;
        held.submission = SubmissionState::Held {
            phase: HeldPhase::Cancelled { cause },
        };
        append_unlaunched_event_on(
            conn,
            tasks_dir,
            &mut held,
            UnlaunchedEnding::Cancelled(cause),
        )?;
    }
    Ok(Some(Parking {
        on: waiting.on.clone(),
        continuation,
    }))
}

fn worker_thread_on(conn: &Connection, task: TaskId) -> Result<Option<ThreadId>, AppError> {
    let thread: Option<Option<String>> = conn
        .query_row(
            "SELECT worker_thread FROM tasks WHERE id = ?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    thread.flatten().map(|thread| thread.parse()).transpose()
}

/// Held continuation that reserves a Codex thread, other than `except`
///
/// A parked chain resumes its worker's thread later, so no other task may
/// resume that thread meanwhile
pub(super) fn thread_reserved_by_on(
    conn: &Connection,
    thread: ThreadId,
    except: TaskId,
) -> Result<Option<TaskId>, AppError> {
    let task: Option<String> = conn
        .query_row(
            "SELECT r.task_id FROM task_chains c
             JOIN origin_routes r
               ON r.task_id = json_extract(c.state_json, '$.continuation')
             WHERE json_extract(c.state_json, '$.state') = 'waiting'
               AND json_extract(r.route_json, '$.submission.type') = 'held'
               AND json_extract(r.route_json, '$.submission.phase.type') IN ('waiting', 'launching')
               AND json_extract(r.route_json, '$.spec.workload.agent') = 'codex'
               AND json_extract(r.route_json, '$.spec.workload.resume_thread') = ?1
               AND r.task_id != ?2
             ORDER BY r.task_id LIMIT 1",
            params![thread.to_string(), except.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    task.as_deref().map(parse_task).transpose()
}

/// One node of the wait graph: a chain, or a task that never parked
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum WaitNode {
    Chain(TaskId),
    Task(TaskId),
}

fn node_on(conn: &Connection, task: TaskId) -> Result<WaitNode, AppError> {
    Ok(chain_of_on(conn, task)?.map_or(WaitNode::Task(task), |chain| WaitNode::Chain(chain.id)))
}

/// Targets of a task's final waiting report while it still runs
fn pending_wait_on(conn: &Connection, task: TaskId) -> Result<Vec<TaskId>, AppError> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM tasks WHERE id = ?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    if !matches!(status.as_deref(), Some("queued" | "running")) {
        return Ok(Vec::new());
    }
    let reports = reports_from(conn, task)?;
    Ok(match reports.last().map(|report| &report.outcome) {
        Some(ReportOutcome::Waiting(waiting)) => waiting.on.tasks().to_vec(),
        _ => Vec::new(),
    })
}

/// Tasks a held route still waits on
fn held_after_on(conn: &Connection, task: TaskId) -> Result<Vec<TaskId>, AppError> {
    let Some((route, Some(after))) = dependent_route_on(conn, task)? else {
        return Ok(Vec::new());
    };
    let held = matches!(
        route.submission,
        SubmissionState::Held {
            phase: HeldPhase::Waiting | HeldPhase::Launching
        }
    );
    Ok(if held {
        after.tasks().to_vec()
    } else {
        Vec::new()
    })
}

/// Tasks one node waits on: `after` lists of held routes, saved waiting
/// reports of running workers, and the targets of a parked chain
fn waits_of_on(conn: &Connection, node: WaitNode) -> Result<Vec<TaskId>, AppError> {
    match node {
        WaitNode::Task(task) => {
            let mut waits = held_after_on(conn, task)?;
            waits.extend(pending_wait_on(conn, task)?);
            Ok(waits)
        }
        WaitNode::Chain(chain) => {
            let state = chain_of_on(conn, chain)?.map(|chain| chain.state);
            match state {
                Some(ChainState::Waiting { on, .. }) => Ok(on.tasks().to_vec()),
                Some(ChainState::Running { current }) => {
                    let mut waits = held_after_on(conn, current)?;
                    waits.extend(pending_wait_on(conn, current)?);
                    Ok(waits)
                }
                Some(ChainState::Ended { .. }) | None => Ok(Vec::new()),
            }
        }
    }
}

/// Whether `target` leads back to `worker` through the wait graph
fn leads_back_on(conn: &Connection, worker: WaitNode, target: TaskId) -> Result<bool, AppError> {
    let mut seen = HashSet::new();
    let mut stack = vec![node_on(conn, target)?];
    while let Some(node) = stack.pop() {
        if node == worker {
            return Ok(true);
        }
        if !seen.insert(node) {
            continue;
        }
        if seen.len() > CYCLE_SEARCH_LIMIT {
            return Err(AppError::Internal {
                message: "the wait graph is too large to check for cycles".into(),
            });
        }
        for next in waits_of_on(conn, node)? {
            stack.push(node_on(conn, next)?);
        }
    }
    Ok(false)
}

/// Whether a task is local: its origin route is here and runs here
fn local_route_on(conn: &Connection, task: TaskId) -> Result<Option<OriginRoute>, AppError> {
    Ok(dependent_route_on(conn, task)?
        .map(|(route, _)| route)
        .filter(|route| route.origin_machine == route.execution_machine))
}

fn is_queue_run_on(conn: &Connection, task: TaskId) -> Result<bool, AppError> {
    let job: Option<Option<String>> = conn
        .query_row(
            "SELECT resource_job_id FROM tasks WHERE id = ?1",
            [task.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(job.flatten().is_some())
}

fn task_exists_on(conn: &Connection, task: TaskId) -> Result<bool, AppError> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE id = ?1)
             OR EXISTS(SELECT 1 FROM origin_routes WHERE task_id = ?1)",
        [task.to_string()],
        |row| row.get(0),
    )?)
}

/// Refuse a waiting report that could not park or resume, in the report's transaction
pub(super) fn check_waiting_on(
    conn: &Connection,
    row: &TaskRow,
    targets: &WaitTargets,
) -> Result<(), AppError> {
    let worker = row.id;
    if !matches!(row.workload, Workload::Agent(_)) || is_queue_run_on(conn, worker)? {
        return Err(WaitingRejection::UnsupportedWorkload.into());
    }
    if local_route_on(conn, worker)?.is_none() {
        return Err(WaitingRejection::Remote { task: worker }.into());
    }
    let chain = chain_of_on(conn, worker)?;
    let runs = chain.as_ref().map_or(1, |chain| chain.runs);
    if runs >= MAX_CHAIN_RUNS {
        return Err(WaitingRejection::TooManyContinuations { runs }.into());
    }

    for &task in targets.tasks() {
        if task == worker {
            return Err(WaitingRejection::Cycle { task }.into());
        }
        if !task_exists_on(conn, task)? {
            return Err(WaitingRejection::UnknownTarget { task }.into());
        }
        if is_queue_run_on(conn, task)? {
            return Err(WaitingRejection::TargetUnsupported { task }.into());
        }
        if local_route_on(conn, task)?.is_none() {
            return Err(WaitingRejection::Remote { task }.into());
        }
    }

    let node = node_on(conn, worker)?;
    for &task in targets.tasks() {
        if leads_back_on(conn, node, task)? {
            return Err(WaitingRejection::Cycle { task }.into());
        }
    }
    Ok(())
}

impl Store {
    /// Chain of each task that belongs to one, for task views
    pub fn chain_view(&self, task: TaskId) -> Result<Option<ChainView>, AppError> {
        Ok(chain_of_on(&self.conn, task)?.map(|chain| chain.view()))
    }

    /// What a parked run handed over, or `None` for any other task
    pub fn parking(&self, run: TaskId) -> Result<Option<Parking>, AppError> {
        parking_on(&self.conn, run)
    }

    /// The run that currently owns the work of `task`'s chain, or `None` for a
    /// task outside a chain or a chain that ended
    ///
    /// A parked chain is owned by its held continuation
    pub fn chain_current_run(&self, task: TaskId) -> Result<Option<TaskId>, AppError> {
        Ok(
            chain_of_on(&self.conn, task)?.and_then(|chain| match chain.state {
                ChainState::Running { current } => Some(current),
                ChainState::Waiting { continuation, .. } => Some(continuation),
                ChainState::Ended { .. } => None,
            }),
        )
    }
}
