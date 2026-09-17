//! Daemon ractor topology: store, callback, per-task watch, supervisor.

pub mod callback;
pub mod store;
pub mod supervisor;
pub mod task;

use std::time::Duration;

use ractor::{ActorRef, RpcReplyPort};

use crate::error::AppError;

/// Timeout for every daemon `ActorRef::call`.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(5);

pub use callback::{CallbackActor, CallbackMsg};
pub use store::{StoreActor, StoreMsg};
pub use supervisor::{DaemonRefs, SupervisorActor, SupervisorMsg};
pub use task::{TaskActor, TaskMsg};

/// Map a ractor call result onto `AppError::Internal`.
pub fn flatten_call<T, M>(
    result: Result<ractor::rpc::CallResult<Result<T, AppError>>, ractor::MessagingErr<M>>,
) -> Result<T, AppError> {
    match result {
        Ok(ractor::rpc::CallResult::Success(inner)) => inner,
        Ok(ractor::rpc::CallResult::Timeout) => Err(AppError::Internal {
            message: "actor call timed out".into(),
        }),
        Ok(ractor::rpc::CallResult::SenderError) => Err(AppError::Internal {
            message: "actor call sender error".into(),
        }),
        Err(err) => Err(AppError::Internal {
            message: format!("actor messaging: {err}"),
        }),
    }
}

/// Call `StoreActor` with the 5s timeout.
pub async fn call_store<T: Send + 'static>(
    store: &ActorRef<StoreMsg>,
    build: impl FnOnce(RpcReplyPort<Result<T, AppError>>) -> StoreMsg,
) -> Result<T, AppError> {
    flatten_call(store.call(build, Some(CALL_TIMEOUT)).await)
}

/// Reply helper that ignores a dropped caller.
pub fn send_reply<T>(port: RpcReplyPort<T>, value: T) {
    let _ = port.send(value);
}
