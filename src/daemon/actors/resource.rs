//! Passive owner for one authority-local resource snapshot.

use ractor::{Actor, ActorId, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::daemon::actors::send_reply;
use crate::error::AppError;
use crate::resource::{Loan, Resource, ResourceId};

/// Messages for one resource actor.
pub enum ResourceMsg {
    /// Return the actor identity and the exact snapshot held at startup.
    Inspect {
        /// Reply with the actor's identity and current snapshot.
        reply: RpcReplyPort<Result<ResourceActorInspection, AppError>>,
    },
}

/// Read-only view of one resource actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceActorInspection {
    /// Ractor identity of the actor that returned this snapshot.
    pub actor_id: ActorId,
    /// Durable resource state restored by the actor.
    pub resource: Resource,
    /// Durable active or attention loan restored by the actor, if present.
    pub loan: Option<Loan>,
}

/// State for one resource actor.
pub struct ResourceActorState {
    resource: Resource,
    loan: Option<Loan>,
}

/// Actor that passively holds one restored resource snapshot.
pub struct ResourceActor;

impl Actor for ResourceActor {
    type Msg = ResourceMsg;
    type State = ResourceActorState;
    type Arguments = (Resource, Option<Loan>);

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        (resource, loan): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(ResourceActorState { resource, loan })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ResourceMsg::Inspect { reply } => send_reply(
                reply,
                Ok(ResourceActorInspection {
                    actor_id: myself.get_id(),
                    resource: state.resource.clone(),
                    loan: state.loan.clone(),
                }),
            ),
        }

        Ok(())
    }
}

/// Stable actor name for one resource identity.
#[must_use]
pub(crate) fn resource_actor_name(id: ResourceId) -> String {
    format!("homebased.resource.{}", id.as_uuid())
}

/// Parse one resource actor name into its stable resource identity.
#[must_use]
pub(crate) fn resource_id_from_actor_name(name: Option<String>) -> Option<ResourceId> {
    let name = name?;
    let uuid = name.strip_prefix("homebased.resource.")?.parse().ok()?;
    Some(ResourceId::from_uuid(uuid))
}
