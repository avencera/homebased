//! Daemon ractor topology: store, callback, per-task watch, per-resource owner, supervisor.

pub mod callback;
pub mod resource;
pub mod store;
pub mod supervisor;
pub mod task;

use std::time::Duration;

use ractor::{ActorRef, MessagingErr, RpcReplyPort};

use crate::error::AppError;

/// Timeout for every daemon `ActorRef::call`.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(5);

pub use callback::{CallbackActor, CallbackMsg};
pub use resource::{ResourceActor, ResourceActorInspection, ResourceMsg};
pub use store::{StoreActor, StoreMsg};
pub use supervisor::{SupervisorActor, SupervisorMsg};
pub use task::{TaskActor, TaskMsg};

/// Request-reply against any daemon actor with the shared timeout, flattening
/// transport failures into `AppError::Internal`.
pub async fn call<M, T>(
    actor: &ActorRef<M>,
    build: impl FnOnce(RpcReplyPort<Result<T, AppError>>) -> M,
) -> Result<T, AppError>
where
    M: ractor::Message,
    T: Send + 'static,
{
    match actor.call(build, Some(CALL_TIMEOUT)).await {
        Ok(ractor::rpc::CallResult::Success(inner)) => inner,
        Ok(ractor::rpc::CallResult::Timeout) => Err(AppError::Internal {
            message: "actor call timed out".into(),
        }),
        Ok(ractor::rpc::CallResult::SenderError) => Err(AppError::Internal {
            message: "actor call sender error".into(),
        }),
        Err(err) => Err(err.into()),
    }
}

/// Reply helper that ignores a dropped caller.
pub fn send_reply<T>(port: RpcReplyPort<T>, value: T) {
    let _ = port.send(value);
}

impl<M> From<MessagingErr<M>> for AppError {
    fn from(err: MessagingErr<M>) -> Self {
        Self::Internal {
            message: format!("actor messaging: {err}"),
        }
    }
}
