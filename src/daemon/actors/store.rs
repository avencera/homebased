//! `StoreActor` owns the daemon's single SQLite connection.

use std::path::PathBuf;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::daemon::actors::send_reply;
use crate::domain::{
    AgentReport, CallbackStatus, ExitReason, ProcessStatus, TaskId, TaskRow, ThreadId,
};
use crate::error::AppError;
use crate::store::{CancelResult, Store};

/// Messages for daemon SQLite operations.
pub enum StoreMsg {
    /// Insert a queued task.
    InsertTask {
        row: Box<TaskRow>,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Fetch one task.
    GetTask {
        id: TaskId,
        reply: RpcReplyPort<Result<Option<TaskRow>, AppError>>,
    },
    /// List with optional filters.
    ListTasks {
        statuses: Vec<ProcessStatus>,
        thread: Option<ThreadId>,
        reply: RpcReplyPort<Result<Vec<TaskRow>, AppError>>,
    },
    /// Queued and running tasks.
    NonTerminal {
        reply: RpcReplyPort<Result<Vec<TaskRow>, AppError>>,
    },
    /// Terminal tasks whose exit callback is still `pending` or `sending`.
    PendingCallbacks {
        reply: RpcReplyPort<Result<Vec<TaskRow>, AppError>>,
    },
    /// Count of queued or running tasks.
    InFlightCount {
        reply: RpcReplyPort<Result<usize, AppError>>,
    },
    /// Compare-and-swap process status. Replies with the post-update row, or
    /// `None` when the CAS did not match.
    CasStatus {
        id: TaskId,
        from: ProcessStatus,
        to: ProcessStatus,
        reply: RpcReplyPort<Result<Option<TaskRow>, AppError>>,
    },
    /// Compare-and-swap to the status the reason implies, storing the reason.
    CasExit {
        id: TaskId,
        from: ProcessStatus,
        reason: ExitReason,
        reply: RpcReplyPort<Result<Option<TaskRow>, AppError>>,
    },
    /// Record the worker pid.
    SetPid {
        id: TaskId,
        pid: i32,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Request cancel.
    RequestCancel {
        id: TaskId,
        reply: RpcReplyPort<Result<CancelResult, AppError>>,
    },
    /// Claim the exit callback.
    ClaimCallback {
        id: TaskId,
        reply: RpcReplyPort<Result<bool, AppError>>,
    },
    /// Finish the callback as sent or failed.
    FinishCallback {
        id: TaskId,
        status: CallbackStatus,
        reply: RpcReplyPort<Result<(), AppError>>,
    },
    /// Reports in seq order.
    Reports {
        id: TaskId,
        reply: RpcReplyPort<Result<Vec<AgentReport>, AppError>>,
    },
}

/// Owns `rusqlite::Connection` via `Store`.
pub struct StoreActor;

impl Actor for StoreActor {
    type Msg = StoreMsg;
    type State = Store;
    type Arguments = PathBuf;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        db_path: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(Store::open(&db_path)?)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            StoreMsg::InsertTask { row, reply } => send_reply(reply, state.insert_task(&row)),
            StoreMsg::GetTask { id, reply } => send_reply(reply, state.get_task(id)),
            StoreMsg::ListTasks {
                statuses,
                thread,
                reply,
            } => send_reply(reply, state.list_tasks(&statuses, thread)),
            StoreMsg::NonTerminal { reply } => send_reply(reply, state.non_terminal()),
            StoreMsg::PendingCallbacks { reply } => send_reply(reply, state.pending_callbacks()),
            StoreMsg::InFlightCount { reply } => send_reply(reply, state.in_flight_count()),
            StoreMsg::CasStatus {
                id,
                from,
                to,
                reply,
            } => send_reply(reply, state.cas_status(id, from, to)),
            StoreMsg::CasExit {
                id,
                from,
                reason,
                reply,
            } => send_reply(reply, state.cas_exit(id, from, &reason)),
            StoreMsg::SetPid { id, pid, reply } => send_reply(reply, state.set_pid(id, pid)),
            StoreMsg::RequestCancel { id, reply } => send_reply(reply, state.request_cancel(id)),
            StoreMsg::ClaimCallback { id, reply } => send_reply(reply, state.claim_callback(id)),
            StoreMsg::FinishCallback { id, status, reply } => {
                send_reply(reply, state.finish_callback(id, status));
            }
            StoreMsg::Reports { id, reply } => send_reply(reply, state.reports(id)),
        }
        Ok(())
    }
}
