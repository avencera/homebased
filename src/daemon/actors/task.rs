//! One `TaskActor` per non-terminal task: flock watch, apply, inactivity timer

use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::task::AbortHandle;

use crate::callback::ATTENTION_SETTLE;
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{
    AttentionState, ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskId, TaskRow, TaskState,
};
use crate::error::AppError;
use crate::home::{self, Home, LockMode};
use crate::store::{self, CancelResult};

/// Retry interval when an attention event cannot be committed
const ATTENTION_RETRY: Duration = Duration::from_secs(30);

/// Longest single sleep before the deadline is recomputed from the wall clock
/// and `output.log` mtime. The inactivity timer has no product maximum, so
/// bounded hops re-check for wall-clock changes and new output, and keep each
/// sleep within a practical timer limit
const ATTENTION_HOP_MAX: Duration = Duration::from_secs(3600);

/// Per-task messages
pub enum TaskMsg {
    /// `runner.lock` is free; apply `exit.json` or Lost
    LockReleased,
    /// Output-inactivity timer fired
    AttentionDue,
    /// A legacy attention sender can no longer be alive
    AttentionRecoveryDue,
}

/// Holds refs; `Arguments` is the `TaskId`
pub struct TaskActor {
    /// State dir
    pub home: Home,
    /// Store actor
    pub(crate) store: ActorRef<StoreMsg>,
}

/// In-memory attention phase for one watch actor. Armed and recovering cannot
/// overlap
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttentionPhase {
    /// Timer may still fire; no send is in flight
    Armed,
    /// A legacy sender may still own the old persisted claim until its bound passes
    Recovering,
    /// Delivered, or the task went terminal; no further reminder
    Done,
}

/// Runtime state of one watch actor
pub struct TaskWatch {
    id: TaskId,
    attention: AttentionPhase,
    attention_timer: Option<AbortHandle>,
}

impl TaskWatch {
    fn replace_attention_timer(&mut self, timer: AbortHandle) {
        self.cancel_attention_timer();
        self.attention_timer = Some(timer);
    }

    fn cancel_attention_timer(&mut self) {
        if let Some(timer) = self.attention_timer.take() {
            timer.abort();
        }
    }
}

impl Drop for TaskWatch {
    fn drop(&mut self) {
        self.cancel_attention_timer();
    }
}

impl Actor for TaskActor {
    type Msg = TaskMsg;
    type State = TaskWatch;
    type Arguments = TaskId;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        id: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let lock_path = self.home.task_paths(id).runner_lock;
        // a dedicated OS thread, not `spawn_blocking`: the `flock` is
        // uninterruptible and the tokio blocking pool waits for every thread on
        // runtime drop, which would hold `serve` open past SIGTERM until the
        // worker exits (REQ-23, DEC-21). This thread may outlive the runtime;
        // the cast simply fails once the actor is gone
        let watch = myself.clone();
        std::thread::Builder::new()
            .name(format!("hb-lock-{id}"))
            .spawn(move || {
                if let Err(err) = home::flock_exclusive(&lock_path, LockMode::Blocking) {
                    tracing::warn!(%id, "flock watch: {err}");
                }
                if let Err(err) = watch.cast(TaskMsg::LockReleased) {
                    tracing::debug!(%id, "lock watch cast: {err}");
                }
            })
            .map_err(|err| AppError::Internal {
                message: format!("spawn lock watch thread: {err}"),
            })?;

        let row = call(&self.store, |reply| StoreMsg::GetTask { id, reply })
            .await?
            .ok_or(AppError::TaskNotFound { id })?;
        let (attention, attention_timer) = match (&row.attention, row.state.is_terminal()) {
            (_, true) | (AttentionState::Delivered { .. }, false) => (AttentionPhase::Done, None),
            (AttentionState::Sending, false) => (
                AttentionPhase::Recovering,
                Some(schedule_attention_recovery(&myself)),
            ),
            (AttentionState::Pending, false) => (
                AttentionPhase::Armed,
                Some(arm_attention_timer(
                    myself.clone(),
                    &row,
                    self.home.task_paths(id).output,
                )),
            ),
        };
        Ok(TaskWatch {
            id,
            attention,
            attention_timer,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            TaskMsg::LockReleased => {
                if let Err(err) = apply_after_lock(self, state.id).await {
                    tracing::warn!(id = %state.id, "apply after lock: {err}");
                }
                myself.stop(None);
            }
            TaskMsg::AttentionDue => {
                state.cancel_attention_timer();
                if state.attention != AttentionPhase::Armed {
                    return Ok(());
                }
                match start_attention_reminder(self, myself.clone(), state.id).await {
                    Ok(AttentionStep::Deferred(timer)) => {
                        state.replace_attention_timer(timer);
                    }
                    Ok(AttentionStep::Done) => state.attention = AttentionPhase::Done,
                    Err(err) => {
                        tracing::warn!(id = %state.id, "attention reminder: {err}");
                        state.replace_attention_timer(schedule_attention_retry(&myself));
                    }
                }
            }
            TaskMsg::AttentionRecoveryDue => {
                state.cancel_attention_timer();
                if state.attention != AttentionPhase::Recovering {
                    return Ok(());
                }
                call(&self.store, |reply| StoreMsg::ReleaseAttention {
                    id: state.id,
                    reply,
                })
                .await?;
                state.attention = AttentionPhase::Armed;
                myself.cast(TaskMsg::AttentionDue)?;
            }
        }
        Ok(())
    }
}

/// Time left before an inactivity reminder is due. Saturating throughout:
/// neither a timeout near `u64::MAX` nor a clock that moved backwards can
/// overflow or collapse the wait to zero
fn inactivity_wait(
    created_at: DateTime<Utc>,
    last_output_at: Option<DateTime<Utc>>,
    timeout: Duration,
    now: DateTime<Utc>,
) -> Duration {
    let activity_at = last_output_at
        .filter(|at| *at > created_at)
        .unwrap_or(created_at);
    let elapsed = (now - activity_at).to_std().unwrap_or(Duration::ZERO);
    timeout.saturating_sub(elapsed)
}

/// Modification time of a non-empty `output.log`. An empty or missing file is
/// not activity: the runner creates the log at spawn
fn last_output_at(output: &Path) -> Option<DateTime<Utc>> {
    let metadata = match std::fs::metadata(output) {
        Ok(metadata) if metadata.len() != 0 => metadata,
        Ok(_) => return None,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => {
            tracing::warn!(path = %output.display(), "output metadata: {err}");
            return None;
        }
    };
    match metadata.modified() {
        Ok(modified) => Some(DateTime::<Utc>::from(modified)),
        Err(err) => {
            tracing::warn!(path = %output.display(), "output modified time: {err}");
            None
        }
    }
}

/// Hand-rolled rather than `send_after`: the wait is re-derived from the wall
/// clock and `output.log` mtime on every hop so a long timeout survives clock
/// changes, suspend, and new output
fn arm_attention_timer(
    myself: ActorRef<TaskMsg>,
    row: &TaskRow,
    output: impl AsRef<Path>,
) -> AbortHandle {
    let created_at = row.created_at;
    let timeout = row.timeout;
    let output = output.as_ref().to_path_buf();
    tokio::spawn(async move {
        loop {
            let wait = inactivity_wait(created_at, last_output_at(&output), timeout, Utc::now());
            if wait.is_zero() {
                break;
            }
            tokio::time::sleep(wait.min(ATTENTION_HOP_MAX)).await;
        }
        if let Err(err) = myself.cast(TaskMsg::AttentionDue) {
            tracing::debug!("attention timer cast: {err}");
        }
    })
    .abort_handle()
}

fn schedule_attention_retry(myself: &ActorRef<TaskMsg>) -> AbortHandle {
    myself
        .send_after(ATTENTION_RETRY, || TaskMsg::AttentionDue)
        .abort_handle()
}

fn schedule_attention_recovery(myself: &ActorRef<TaskMsg>) -> AbortHandle {
    myself
        .send_after(ATTENTION_SETTLE, || TaskMsg::AttentionRecoveryDue)
        .abort_handle()
}

/// Outcome of starting one attention attempt
enum AttentionStep {
    /// The task has not started or output resumed, so another check is armed
    Deferred(AbortHandle),
    /// Nothing to produce: already recorded, or the task is terminal
    Done,
}

/// Append one inactivity event before any later terminal event can be produced
async fn start_attention_reminder(
    actor: &TaskActor,
    myself: ActorRef<TaskMsg>,
    id: TaskId,
) -> Result<AttentionStep, AppError> {
    let Some(row) = call(&actor.store, |reply| StoreMsg::GetTask { id, reply }).await? else {
        return Ok(AttentionStep::Done);
    };

    match row.state {
        TaskState::Queued => {
            return Ok(AttentionStep::Deferred(schedule_attention_retry(&myself)));
        }
        TaskState::Running { .. } => {}
        TaskState::Finished { .. } | TaskState::Lost => return Ok(AttentionStep::Done),
    }

    let output = actor.home.task_paths(id).output;
    if !inactivity_wait(
        row.created_at,
        last_output_at(&output),
        row.timeout,
        Utc::now(),
    )
    .is_zero()
    {
        return Ok(AttentionStep::Deferred(arm_attention_timer(
            myself, &row, output,
        )));
    }

    if !call(&actor.store, |reply| StoreMsg::IsEventTask { id, reply }).await? {
        return Err(AppError::Internal {
            message: format!("task {id} has no durable event identity"),
        });
    }
    call(&actor.store, |reply| StoreMsg::ProduceAttentionEvent {
        id,
        reply,
    })
    .await?;
    Ok(AttentionStep::Done)
}

async fn apply_after_lock(actor: &TaskActor, id: TaskId) -> Result<(), AppError> {
    let Some(row) = call(&actor.store, |reply| StoreMsg::GetTask { id, reply }).await? else {
        return Ok(());
    };
    if row.state.is_terminal() {
        return Ok(());
    }
    let paths = actor.home.task_paths(id);
    match store::read_exit_json(&paths.exit_json)? {
        Some(exit) => {
            apply_exit(actor, row, &exit.reason, exit.process_group_exit_evidence).await?
        }
        None => apply_lost(actor, row).await?,
    };
    Ok(())
}

/// The worker released the lock without writing `exit.json`
async fn apply_lost(actor: &TaskActor, row: TaskRow) -> Result<TaskRow, AppError> {
    if row.state.is_terminal() {
        return Ok(row);
    }
    let id = row.id;
    let cas = call(&actor.store, |reply| StoreMsg::CasStatus {
        id,
        from: row.status(),
        to: ProcessStatus::Lost,
        reply,
    })
    .await?;
    match cas {
        Some(row) => {
            tracing::info!(%id, "runner lost");
            Ok(row)
        }
        // a cancel or exit landed first; report whatever it stored
        None => require_task(&actor.store, id).await,
    }
}

/// The worker wrote `exit.json`; adopt its reason if the row is still live
async fn apply_exit(
    actor: &TaskActor,
    row: TaskRow,
    reason: &ExitReason,
    process_group_exit_evidence: ProcessGroupExitEvidence,
) -> Result<TaskRow, AppError> {
    if row.state.is_terminal() {
        return Ok(row);
    }
    let id = row.id;
    let cas = call(&actor.store, |reply| StoreMsg::CasExit {
        id,
        from: row.status(),
        reason: reason.clone(),
        process_group_exit_evidence,
        reply,
    })
    .await?;
    match cas {
        Some(row) => Ok(row),
        None => require_task(&actor.store, id).await,
    }
}

/// Request cancel in the store, then signal a live worker when needed. Every state change is
/// a store CAS, so this needs no per-task actor and the supervisor runs it
/// directly
pub(crate) async fn cancel_task(
    store: &ActorRef<StoreMsg>,
    id: TaskId,
) -> Result<CancelResult, AppError> {
    let result = call(store, |reply| StoreMsg::RequestCancel { id, reply }).await?;
    match &result {
        CancelResult::AlreadyTerminal(_) => {}
        CancelResult::CancelledQueued(_) => {}
        // signal the worker pid, not `-pid`: `task-run` forwards to the child's own process group
        CancelResult::SignalWorker(row) => {
            if let Some(pid) = row.pid()
                && let Err(err) = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid),
                    nix::sys::signal::Signal::SIGTERM,
                )
            {
                tracing::warn!(%id, pid, "SIGTERM worker: {err}");
            }
        }
    }
    Ok(result)
}

async fn require_task(store: &ActorRef<StoreMsg>, id: TaskId) -> Result<TaskRow, AppError> {
    call(store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .ok_or(AppError::TaskNotFound { id })
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::str::FromStr;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;

    use chrono::{TimeDelta, Utc};
    use ractor::Actor;
    use tempfile::tempdir;

    use super::{
        ATTENTION_HOP_MAX, AttentionPhase, TaskWatch, cancel_task, inactivity_wait, last_output_at,
    };

    #[test]
    fn inactivity_wait_counts_down_from_creation_without_output() {
        let created = Utc::now();
        let timeout = Duration::from_secs(4 * 3600);
        assert_eq!(inactivity_wait(created, None, timeout, created), timeout);
        assert_eq!(
            inactivity_wait(created, None, timeout, created + TimeDelta::hours(1)),
            Duration::from_secs(3 * 3600)
        );
    }

    #[test]
    fn inactivity_wait_is_zero_once_overdue() {
        let created = Utc::now();
        let timeout = Duration::from_secs(2 * 3600);
        assert_eq!(
            inactivity_wait(created, None, timeout, created + TimeDelta::hours(3)),
            Duration::ZERO
        );
    }

    #[test]
    fn recent_output_restarts_the_inactivity_window() {
        let created = Utc::now();
        let output = created + TimeDelta::hours(2);
        let now = created + TimeDelta::hours(3);
        let timeout = Duration::from_secs(2 * 3600);
        assert_eq!(
            inactivity_wait(created, Some(output), timeout, now),
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn inactivity_wait_is_zero_when_output_is_stale() {
        let created = Utc::now();
        let output = created + TimeDelta::hours(1);
        let now = output + TimeDelta::hours(3);
        let timeout = Duration::from_secs(2 * 3600);
        assert_eq!(
            inactivity_wait(created, Some(output), timeout, now),
            Duration::ZERO
        );
    }

    #[test]
    fn inactivity_wait_ignores_output_older_than_creation() {
        let created = Utc::now();
        let output = created - TimeDelta::hours(1);
        let timeout = Duration::from_secs(2 * 3600);
        assert_eq!(
            inactivity_wait(
                created,
                Some(output),
                timeout,
                created + TimeDelta::hours(1)
            ),
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn last_output_at_ignores_missing_and_empty_files() {
        let dir = tempdir().unwrap();
        assert_eq!(last_output_at(&dir.path().join("missing.log")), None);

        let empty = dir.path().join("empty.log");
        std::fs::write(&empty, b"").unwrap();
        assert_eq!(last_output_at(&empty), None);
    }

    #[test]
    fn last_output_at_reads_mtime_of_non_empty_output() {
        use nix::sys::time::{TimeVal, TimeValLike};

        let dir = tempdir().unwrap();
        let path = dir.path().join("output.log");
        std::fs::write(&path, b"bytes").unwrap();
        let at = Utc::now() - TimeDelta::hours(2);
        let tv = TimeVal::seconds(at.timestamp());
        nix::sys::stat::utimes(&path, &tv, &tv).unwrap();
        let got = last_output_at(&path).unwrap();
        assert_eq!(got.timestamp(), at.timestamp());
    }

    #[test]
    fn inactivity_wait_survives_a_clock_that_moved_backwards() {
        let created = Utc::now();
        let timeout = Duration::from_secs(2 * 3600);
        // a backwards clock must not shorten the wait, and must not panic
        assert_eq!(
            inactivity_wait(created, None, timeout, created - TimeDelta::hours(5)),
            timeout
        );
    }

    #[test]
    fn inactivity_wait_handles_a_timeout_wider_than_any_instant() {
        let created = Utc::now();
        let huge = Duration::from_secs(u64::MAX);
        let wait = inactivity_wait(created, None, huge, created + TimeDelta::hours(1));
        assert_eq!(wait, huge - Duration::from_secs(3600));
        // hops keep each sleep within a practical timer limit
        assert_eq!(wait.min(ATTENTION_HOP_MAX), ATTENTION_HOP_MAX);
        let _ = tokio::time::Instant::now() + wait.min(ATTENTION_HOP_MAX);
    }

    #[tokio::test]
    async fn dropping_a_watch_cancels_its_attention_timer() {
        let fired = Arc::new(AtomicBool::new(false));
        let fired_by_timer = fired.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            fired_by_timer.store(true, Ordering::SeqCst);
        })
        .abort_handle();
        let watch = TaskWatch {
            id: TaskId::new(),
            attention: AttentionPhase::Armed,
            attention_timer: Some(timer),
        };
        drop(watch);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!fired.load(Ordering::SeqCst));
    }

    use crate::daemon::actors::{StoreActor, StoreMsg, call};
    use crate::domain::{
        Agent, AgentKind, AgentWorkload, ProcessStatus, TaskEnv, TaskId, ThreadId, Workload,
    };
    use crate::home::Home;
    use crate::store::{CancelResult, NewTask, new_queued_task};

    #[tokio::test]
    async fn queued_cancel_persists_a_terminal_event_without_direct_delivery() {
        let dir = tempdir().unwrap();
        let home = Home::resolve(Some(dir.path().to_path_buf())).unwrap();
        home.ensure().unwrap();

        let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
            .await
            .unwrap();

        let id = TaskId::new();
        let row = new_queued_task(NewTask {
            id,
            name: None,
            thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
            workload: Workload::Agent(AgentWorkload {
                agent: Agent::new(AgentKind::Claude, None),
                extra_args: vec![],
                report_trailer: false,
            }),
            cwd: dir.path().to_path_buf(),
            timeout: Duration::from_secs(4 * 3600),
            env: TaskEnv {
                // empty PATH so migration keeps an unavailable callback context
                path: dir.path().join("empty-bin").display().to_string(),
                home: dir.path().display().to_string(),
            },
            binary: Path::new("/bin/true").to_path_buf(),
        });
        crate::store::Store::open(&home.db_path())
            .unwrap()
            .insert_task(&row)
            .unwrap();

        call(&store, |reply| StoreMsg::MigrateLegacyLocal {
            machine: crate::machine::MachineId::new(),
            reply,
        })
        .await
        .unwrap();

        let result = cancel_task(&store, id).await.unwrap();
        assert!(matches!(result, CancelResult::CancelledQueued(_)));

        let final_row = call(&store, |reply| StoreMsg::GetTask { id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_row.status(), ProcessStatus::Cancelled);
        assert_eq!(
            final_row.callback_status,
            crate::domain::CallbackStatus::Pending
        );
        let event = call(&store, |reply| StoreMsg::FirstPendingOutbound { id, reply })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.event.seq.get(), 1);
        assert!(matches!(
            event.event.payload,
            crate::events::EventPayload::Callback {
                state: Some(ProcessStatus::Cancelled),
                ..
            }
        ));
        let pending_inbox = call(&store, |reply| StoreMsg::PendingInboxTasks { reply })
            .await
            .unwrap();
        assert!(pending_inbox.is_empty());

        store.stop(None);
        let _ = store_handle.await;
    }
}
