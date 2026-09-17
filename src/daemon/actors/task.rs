//! One `TaskActor` per non-terminal task: flock watch, apply, cancel, attention timer.

use std::time::Duration;

use chrono::{DateTime, Utc};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::callback::{HomebasedEvent, check_due_event, deliver_notify, exit_event, lost_event};
use crate::daemon::actors::callback::CallbackMsg;
use crate::daemon::actors::{StoreMsg, call_store, send_reply};
use crate::domain::{ExitReason, ProcessStatus, TaskId, TaskRow, TaskState};
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
    /// Cancel this task.
    Cancel {
        reply: RpcReplyPort<Result<CancelResult, AppError>>,
    },
    /// Attention timer fired or a failed send should retry.
    AttentionDue,
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
    /// Delivered, or the task went terminal; no further reminder.
    Done,
}

/// Runtime state of one watch actor.
pub struct TaskWatch {
    id: TaskId,
    attention: AttentionPhase,
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
        // the cast simply fails once the actor is gone.
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

        let row = call_store(&self.store, |reply| StoreMsg::GetTask { id, reply })
            .await?
            .ok_or(AppError::TaskNotFound { id })?;
        let attention = if row.attention.is_delivered() || row.state.is_terminal() {
            AttentionPhase::Done
        } else {
            arm_attention_timer(myself.clone(), &row);
            AttentionPhase::Armed
        };
        Ok(TaskWatch { id, attention })
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
            TaskMsg::Cancel { reply } => {
                send_reply(
                    reply,
                    cancel_task(&self.store, &self.callback, &self.home, state.id).await,
                );
            }
            TaskMsg::AttentionDue => {
                if state.attention != AttentionPhase::Armed {
                    return Ok(());
                }
                match start_attention_reminder(self, myself.clone(), state.id).await {
                    Ok(AttentionStep::Sending) => state.attention = AttentionPhase::Sending,
                    Ok(AttentionStep::Done) => state.attention = AttentionPhase::Done,
                    Err(err) => {
                        tracing::warn!(id = %state.id, "attention reminder: {err}");
                        schedule_attention_retry(myself.clone());
                    }
                }
            }
            TaskMsg::AttentionSettled { delivered } => {
                if delivered {
                    state.attention = AttentionPhase::Done;
                } else {
                    state.attention = AttentionPhase::Armed;
                    schedule_attention_retry(myself.clone());
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

fn arm_attention_timer(myself: ActorRef<TaskMsg>, row: &TaskRow) {
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
    });
}

fn schedule_attention_retry(myself: ActorRef<TaskMsg>) {
    tokio::spawn(async move {
        tokio::time::sleep(ATTENTION_RETRY).await;
        if let Err(err) = myself.cast(TaskMsg::AttentionDue) {
            tracing::debug!("attention retry cast: {err}");
        }
    });
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
/// `codex queue` can take seconds; it runs off the actor so a cancel arriving
/// mid-send is still answered.
async fn start_attention_reminder(
    actor: &TaskActor,
    myself: ActorRef<TaskMsg>,
    id: TaskId,
) -> Result<AttentionStep, AppError> {
    if !call_store(&actor.store, |reply| StoreMsg::ClaimAttention { id, reply }).await? {
        return Ok(AttentionStep::Done);
    }
    let Some(row) = call_store(&actor.store, |reply| StoreMsg::GetTask { id, reply }).await? else {
        return Ok(AttentionStep::Done);
    };
    let reports = call_store(&actor.store, |reply| StoreMsg::Reports { id, reply }).await?;
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
            call_store(store, |reply| StoreMsg::MarkAttentionDelivered {
                id,
                reply,
            })
            .await
        }
        Err(err) => {
            tracing::warn!(%id, "attention notify failed: {err}");
            call_store(store, |reply| StoreMsg::ReleaseAttention { id, reply }).await
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
    let Some(row) = call_store(&actor.store, |reply| StoreMsg::GetTask { id, reply }).await? else {
        return Ok(());
    };
    if row.state.is_terminal() && !row.callback_outstanding() {
        return Ok(());
    }
    let paths = actor.home.task_paths(id);
    match store::read_exit_json(&paths.exit_json)? {
        Some(exit) => apply_exit(actor, row, &exit.reason).await,
        None => apply_lost(actor, row).await,
    }
}

/// The worker released the lock without writing `exit.json`.
async fn apply_lost(actor: &TaskActor, row: TaskRow) -> Result<(), AppError> {
    let id = row.id;
    let row = match row.state {
        TaskState::Queued | TaskState::Running { .. } => {
            let from = row.status();
            match call_store(&actor.store, |reply| StoreMsg::CasStatus {
                id,
                from,
                to: ProcessStatus::Lost,
                reply,
            })
            .await?
            {
                None => require_task(&actor.store, id).await?,
                Some(row) => row,
            }
        }
        TaskState::Finished { .. } | TaskState::Lost => row,
    };
    if !row.callback_outstanding() {
        return Ok(());
    }
    let reports = call_store(&actor.store, |reply| StoreMsg::Reports { id, reply }).await?;
    let event = match &row.state {
        TaskState::Lost | TaskState::Queued | TaskState::Running { .. } => {
            tracing::info!(%id, "runner lost");
            lost_event(&row, &reports, actor.home.task_dir(id))
        }
        TaskState::Finished { .. } => exit_event(&row, &reports, actor.home.task_dir(id)),
    };
    actor.callback.cast(CallbackMsg::Deliver { row, event })?;
    Ok(())
}

/// The worker wrote `exit.json`; adopt its reason if the row is still live.
async fn apply_exit(actor: &TaskActor, row: TaskRow, reason: &ExitReason) -> Result<(), AppError> {
    let id = row.id;
    let row = if row.state.is_terminal() {
        row
    } else {
        let from = row.status();
        match call_store(&actor.store, |reply| StoreMsg::CasExit {
            id,
            from,
            reason: reason.clone(),
            reply,
        })
        .await?
        {
            None => require_task(&actor.store, id).await?,
            Some(row) => row,
        }
    };
    if !row.callback_outstanding() {
        return Ok(());
    }
    let reports = call_store(&actor.store, |reply| StoreMsg::Reports { id, reply }).await?;
    let event = exit_event(&row, &reports, actor.home.task_dir(id));
    actor.callback.cast(CallbackMsg::Deliver { row, event })?;
    Ok(())
}

/// Request cancel in the store, then act on the outcome: deliver the exit
/// callback for a queued task, or SIGTERM a live worker. Shared by the task
/// actor and the supervisor fallback when no task actor exists.
pub(crate) async fn cancel_task(
    store: &ActorRef<StoreMsg>,
    callback: &ActorRef<CallbackMsg>,
    home: &Home,
    id: TaskId,
) -> Result<CancelResult, AppError> {
    let result = call_store(store, |reply| StoreMsg::RequestCancel { id, reply }).await?;
    match &result {
        CancelResult::AlreadyTerminal(_) => {}
        CancelResult::CancelledQueued(row) => {
            let reports = call_store(store, |reply| StoreMsg::Reports { id, reply }).await?;
            let event = exit_event(row, &reports, home.task_dir(id));
            callback.cast(CallbackMsg::Deliver {
                row: row.clone(),
                event,
            })?;
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
    call_store(store, |reply| StoreMsg::GetTask { id, reply })
        .await?
        .ok_or(AppError::TaskNotFound { id })
}

impl From<ractor::MessagingErr<CallbackMsg>> for AppError {
    fn from(err: ractor::MessagingErr<CallbackMsg>) -> Self {
        Self::Internal {
            message: format!("callback actor: {err}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::str::FromStr;
    use std::time::Duration;

    use chrono::{TimeDelta, Utc};
    use ractor::Actor;
    use tempfile::tempdir;

    use super::{ATTENTION_HOP_MAX, attention_wait, cancel_task};

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

    use crate::daemon::actors::callback::{CallbackActor, CallbackArgs};
    use crate::daemon::actors::{StoreActor, StoreMsg, call_store};
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
        call_store(&store, |reply| StoreMsg::InsertTask {
            row: Box::new(row),
            reply,
        })
        .await
        .unwrap();

        let result = cancel_task(&store, &callback, &home, id).await.unwrap();
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
            let row = call_store(store, |reply| StoreMsg::GetTask { id, reply })
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
