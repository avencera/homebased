//! One `TaskActor` per non-terminal task: flock watch, apply, attention timer.

use std::time::Duration;

use chrono::{DateTime, Utc};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::task::AbortHandle;

use crate::callback::{ATTENTION_SETTLE, HomebasedEvent, check_due_event, deliver_notify};
use crate::daemon::actors::callback::CallbackMsg;
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::{AttentionState, ExitReason, ProcessStatus, TaskId, TaskRow};
use crate::error::AppError;
use crate::home::{self, Home, LockMode};
use crate::store::{self, CancelResult};

/// Retry interval when an attention reminder fails to send.
const ATTENTION_RETRY: Duration = Duration::from_secs(30);

/// Longest single sleep before the deadline is recomputed from the wall clock.
/// The attention timer has no product maximum, so bounded hops re-check for
/// wall-clock changes and keep each sleep within a practical timer limit.
const ATTENTION_HOP_MAX: Duration = Duration::from_secs(3600);

/// Per-task messages.
pub enum TaskMsg {
    /// `runner.lock` is free; apply `exit.json` or Lost.
    LockReleased,
    /// Attention timer fired or a failed send should retry.
    AttentionDue,
    /// A prior daemon's bounded attention sender can no longer be alive.
    AttentionRecoveryDue,
    /// One attention send finished. Reported by the send task so the actor
    /// mailbox stays free while `codex queue` runs.
    AttentionSettled {
        /// Whether the queue send succeeded.
        delivered: bool,
    },
}

/// Holds refs; `Arguments` is the `TaskId`.
pub struct TaskActor {
    /// State dir.
    pub home: Home,
    /// Store actor.
    pub store: ActorRef<StoreMsg>,
    /// Callback actor.
    pub callback: ActorRef<CallbackMsg>,
}

/// In-memory attention phase for one watch actor. Done and sending cannot
/// overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttentionPhase {
    /// Timer may still fire; no send is in flight.
    Armed,
    /// A send task holds the claim and will report back.
    Sending,
    /// A prior daemon owns the persisted claim until its sender bound passes.
    Recovering,
    /// Delivered, or the task went terminal; no further reminder.
    Done,
}

/// Runtime state of one watch actor.
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
                Some(arm_attention_timer(myself.clone(), &row)),
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
                    Ok(AttentionStep::Sending) => state.attention = AttentionPhase::Sending,
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
            TaskMsg::AttentionSettled { delivered } => {
                if delivered {
                    state.attention = AttentionPhase::Done;
                } else {
                    state.attention = AttentionPhase::Armed;
                    state.replace_attention_timer(schedule_attention_retry(&myself));
                }
            }
        }
        Ok(())
    }
}

/// Time left before the attention reminder is due, measured from the persisted
/// creation time. Saturating throughout: neither a timeout near `u64::MAX` nor
/// a clock that moved backwards can overflow or collapse the wait to zero.
fn attention_wait(created_at: DateTime<Utc>, timeout: Duration, now: DateTime<Utc>) -> Duration {
    let elapsed = (now - created_at).to_std().unwrap_or(Duration::ZERO);
    timeout.saturating_sub(elapsed)
}

/// Hand-rolled rather than `send_after`: the wait is re-derived from the wall
/// clock on every hop so a long timeout survives clock changes and suspend.
fn arm_attention_timer(myself: ActorRef<TaskMsg>, row: &TaskRow) -> AbortHandle {
    let created_at = row.created_at;
    let timeout = row.timeout;
    tokio::spawn(async move {
        loop {
            let wait = attention_wait(created_at, timeout, Utc::now());
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

/// Outcome of starting one attention attempt.
enum AttentionStep {
    /// A send task now holds the claim and will report back.
    Sending,
    /// Nothing to send: already delivered, or the task is terminal.
    Done,
}

/// Claim the attention reminder, then send `TASK_CHECK_DUE` from a detached
/// task.
///
/// The claim is taken *before* the send. It only succeeds while the task is
/// non-terminal, and while it is held the terminal callback waits, so a
/// terminal transition can neither cancel a reminder already on the wire nor
/// let its own event overtake one. The delivered timestamp is written only
/// after the queue send succeeds, so a crash mid-send retries instead of
/// silently swallowing the reminder.
///
/// `codex queue` can take seconds; it runs off the actor so the mailbox stays
/// responsive mid-send.
async fn start_attention_reminder(
    actor: &TaskActor,
    myself: ActorRef<TaskMsg>,
    id: TaskId,
) -> Result<AttentionStep, AppError> {
    if !call(&actor.store, |reply| StoreMsg::ClaimAttention { id, reply }).await? {
        return Ok(AttentionStep::Done);
    }
    let Some(row) = call(&actor.store, |reply| StoreMsg::GetTask { id, reply }).await? else {
        return Ok(AttentionStep::Done);
    };
    let reports = call(&actor.store, |reply| StoreMsg::Reports { id, reply }).await?;
    let event = check_due_event(&row, &reports, actor.home.task_dir(id));
    let home = actor.home.clone();
    let store = actor.store.clone();
    tokio::spawn(async move {
        let delivered = finish_attention_send(&store, &home, row, event).await;
        if let Err(err) = myself.cast(TaskMsg::AttentionSettled { delivered }) {
            tracing::debug!(%id, "attention settled cast: {err}");
        }
    });
    Ok(AttentionStep::Sending)
}

/// Run the blocking queue send, then record or release the claim.
async fn finish_attention_send(
    store: &ActorRef<StoreMsg>,
    home: &Home,
    row: TaskRow,
    event: HomebasedEvent,
) -> bool {
    let id = row.id;
    let home_for_send = home.clone();
    let sent = tokio::task::spawn_blocking(move || deliver_notify(&home_for_send, &row, &event))
        .await
        .map_err(|err| AppError::Internal {
            message: format!("attention notify join: {err}"),
        })
        .and_then(|result| result);
    let delivered = sent.is_ok();
    let outcome = match sent {
        Ok(()) => {
            tracing::info!(%id, "attention reminder sent");
            call(store, |reply| StoreMsg::MarkAttentionDelivered {
                id,
                reply,
            })
            .await
        }
        Err(err) => {
            tracing::warn!(%id, "attention notify failed: {err}");
            call(store, |reply| StoreMsg::ReleaseAttention { id, reply }).await
        }
    };
    if let Err(err) = outcome {
        // the claim stays `sending`; the terminal callback releases it and the
        // next timer wake-up re-claims it
        tracing::warn!(%id, "recording attention outcome: {err}");
        return false;
    }
    delivered
}

async fn apply_after_lock(actor: &TaskActor, id: TaskId) -> Result<(), AppError> {
    let Some(row) = call(&actor.store, |reply| StoreMsg::GetTask { id, reply }).await? else {
        return Ok(());
    };
    if row.state.is_terminal() && !row.callback_outstanding() {
        return Ok(());
    }
    let paths = actor.home.task_paths(id);
    let row = match store::read_exit_json(&paths.exit_json)? {
        Some(exit) => apply_exit(actor, row, &exit.reason).await?,
        None => apply_lost(actor, row).await?,
    };
    deliver_terminal(actor, row)
}

/// The worker released the lock without writing `exit.json`.
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

/// The worker wrote `exit.json`; adopt its reason if the row is still live.
async fn apply_exit(
    actor: &TaskActor,
    row: TaskRow,
    reason: &ExitReason,
) -> Result<TaskRow, AppError> {
    if row.state.is_terminal() {
        return Ok(row);
    }
    let id = row.id;
    let cas = call(&actor.store, |reply| StoreMsg::CasExit {
        id,
        from: row.status(),
        reason: reason.clone(),
        reply,
    })
    .await?;
    match cas {
        Some(row) => Ok(row),
        None => require_task(&actor.store, id).await,
    }
}

/// Hand the row to the callback actor when its terminal event is still owed.
fn deliver_terminal(actor: &TaskActor, row: TaskRow) -> Result<(), AppError> {
    if !row.callback_outstanding() {
        return Ok(());
    }
    actor.callback.cast(CallbackMsg::Deliver { row })?;
    Ok(())
}

/// Request cancel in the store, then act on the outcome: deliver the exit
/// callback for a queued task, or SIGTERM a live worker. Every state change is
/// a store CAS, so this needs no per-task actor and the supervisor runs it
/// directly.
pub(crate) async fn cancel_task(
    store: &ActorRef<StoreMsg>,
    callback: &ActorRef<CallbackMsg>,
    id: TaskId,
) -> Result<CancelResult, AppError> {
    let result = call(store, |reply| StoreMsg::RequestCancel { id, reply }).await?;
    match &result {
        CancelResult::AlreadyTerminal(_) => {}
        CancelResult::CancelledQueued(row) => {
            callback.cast(CallbackMsg::Deliver { row: row.clone() })?;
        }
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

    use super::{ATTENTION_HOP_MAX, AttentionPhase, TaskWatch, attention_wait, cancel_task};

    #[test]
    fn attention_wait_counts_down_from_creation() {
        let created = Utc::now();
        let timeout = Duration::from_secs(4 * 3600);
        assert_eq!(attention_wait(created, timeout, created), timeout);
        assert_eq!(
            attention_wait(created, timeout, created + TimeDelta::hours(1)),
            Duration::from_secs(3 * 3600)
        );
    }

    #[test]
    fn attention_wait_is_zero_once_overdue() {
        let created = Utc::now();
        let timeout = Duration::from_secs(2 * 3600);
        assert_eq!(
            attention_wait(created, timeout, created + TimeDelta::hours(3)),
            Duration::ZERO
        );
    }

    #[test]
    fn attention_wait_survives_a_clock_that_moved_backwards() {
        let created = Utc::now();
        let timeout = Duration::from_secs(2 * 3600);
        // a backwards clock must not shorten the wait, and must not panic
        assert_eq!(
            attention_wait(created, timeout, created - TimeDelta::hours(5)),
            timeout
        );
    }

    #[test]
    fn attention_wait_handles_a_timeout_wider_than_any_instant() {
        let created = Utc::now();
        let huge = Duration::from_secs(u64::MAX);
        let wait = attention_wait(created, huge, created + TimeDelta::hours(1));
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

    use crate::daemon::actors::callback::{CallbackActor, CallbackArgs};
    use crate::daemon::actors::{StoreActor, StoreMsg, call};
    use crate::domain::{
        Agent, AgentKind, AgentWorkload, CallbackStatus, ProcessStatus, TaskEnv, TaskId, ThreadId,
        Workload,
    };
    use crate::home::Home;
    use crate::store::{CancelResult, NewTask, new_queued_task};

    #[tokio::test]
    async fn cancel_task_without_actor_cancels_queued_row_and_delivers_callback() {
        let dir = tempdir().unwrap();
        let home = Home::resolve(Some(dir.path().to_path_buf())).unwrap();
        home.ensure().unwrap();

        let (store, store_handle) = StoreActor::spawn(None, StoreActor, home.db_path())
            .await
            .unwrap();
        let (callback, callback_handle) = CallbackActor::spawn(
            None,
            CallbackActor,
            CallbackArgs {
                store: store.clone(),
                home: home.clone(),
            },
        )
        .await
        .unwrap();

        let id = TaskId::new();
        let row = new_queued_task(NewTask {
            id,
            thread: ThreadId::from_str("01a0ab97-a7aa-7463-a5b0-8d500e40e431").unwrap(),
            workload: Workload::Agent(AgentWorkload {
                agent: Agent::new(AgentKind::Claude, None),
                extra_args: vec![],
                report_trailer: false,
            }),
            cwd: dir.path().to_path_buf(),
            timeout: Duration::from_secs(4 * 3600),
            env: TaskEnv {
                // empty PATH so no `codex` resolves: the callback must still finish, as Failed
                path: dir.path().join("empty-bin").display().to_string(),
                home: dir.path().display().to_string(),
            },
            binary: Path::new("/bin/true").to_path_buf(),
        });
        call(&store, |reply| StoreMsg::InsertTask {
            row: Box::new(row),
            reply,
        })
        .await
        .unwrap();

        let result = cancel_task(&store, &callback, id).await.unwrap();
        assert!(matches!(result, CancelResult::CancelledQueued(_)));

        let final_row = wait_for_callback(&store, id).await;
        assert_eq!(final_row.status(), ProcessStatus::Cancelled);
        assert_ne!(final_row.callback_status, CallbackStatus::Pending);

        store.stop(None);
        callback.stop(None);
        let _ = store_handle.await;
        let _ = callback_handle.await;
    }

    /// Poll until the callback actor leaves `pending`, since `Deliver` is fire-and-forget.
    async fn wait_for_callback(
        store: &ractor::ActorRef<StoreMsg>,
        id: TaskId,
    ) -> crate::domain::TaskRow {
        for _ in 0..100 {
            let row = call(store, |reply| StoreMsg::GetTask { id, reply })
                .await
                .unwrap()
                .unwrap();
            if row.callback_status != CallbackStatus::Pending {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("callback stayed pending");
    }
}
