//! Test doubles for resource actor tests

use ractor::concurrency::JoinHandle;
use ractor::{Actor, ActorProcessingErr, ActorRef};

use crate::daemon::actors::SupervisorMsg;

/// Supervisor stand-in that drops every request unanswered
///
/// A dropped reply port reads as a failed call, so each launch the resource actor
/// asks for settles as uncertain and no task is ever started
pub(crate) struct StubSupervisor;

impl Actor for StubSupervisor {
    type Msg = SupervisorMsg;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        (): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        _message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}

/// Spawn an unnamed stub supervisor for one resource actor test
pub(crate) async fn spawn_stub_supervisor() -> (ActorRef<SupervisorMsg>, JoinHandle<()>) {
    StubSupervisor::spawn(None, StubSupervisor, ())
        .await
        .expect("spawn stub supervisor")
}
