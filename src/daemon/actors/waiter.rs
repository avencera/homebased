//! `WaiterActor` owns the connected `task wait` streams that take a task's
//! callback events in place of the origin thread's normal delivery

use std::collections::HashMap;
use std::time::Duration;

use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, RpcReplyPort, SpawnErr};
use tokio::sync::{mpsc, oneshot};

use crate::callback::EventKind;
use crate::daemon::actors::{call, send_reply};
use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::events::{EventPayload, TaskEvent};
use crate::task_wait::WaiterId;

// the client confirms an event only after its line reached stdout; without that
// confirmation the event falls through to normal delivery, so a client that died
// between receiving and printing cannot swallow it
const ACK_WINDOW: Duration = Duration::from_secs(5);

// handed-off events wait here while the stream writes the one before them
const WAITER_QUEUE: usize = 16;

/// Content of a callback event offered to a waiter
pub struct HandoffEvent {
    /// Task the event belongs to
    pub task: TaskId,
    /// Per-task event sequence
    pub seq: u64,
    /// What the event reports
    pub kind: EventKind,
    /// Continuation run, on `TASK_WAITING`
    pub continuation: Option<TaskId>,
    /// The `HOMEBASED_EVENT` line normal delivery would have sent
    pub line: String,
}

/// A waiter a hand-off can go to
pub struct Candidate {
    waiter: WaiterId,
    queue: mpsc::Sender<HandoffEvent>,
}

/// A newly opened waiter and the queue its events arrive on
pub struct Opened {
    waiter: WaiterId,
    events: mpsc::Receiver<HandoffEvent>,
}

/// Waiter messages
pub enum WaiterMsg {
    /// Register a waiter for `thread`
    Open {
        /// Thread of the waiting session
        thread: ThreadId,
        /// The waiter's id and event queue
        reply: RpcReplyPort<Result<Opened, AppError>>,
    },
    /// Start sending the callback events of `task` to `waiter`
    Watch {
        /// Waiter that takes the events
        waiter: WaiterId,
        /// Task to watch
        task: TaskId,
    },
    /// Stop sending the callback events of `task` to `waiter`
    Unwatch {
        /// Waiter that no longer takes the events
        waiter: WaiterId,
        /// Task to stop watching
        task: TaskId,
    },
    /// Remove a waiter and every task it watches
    Close {
        /// Waiter to remove
        waiter: WaiterId,
    },
    /// The most recently registered live waiter for `task` that belongs to `thread`
    Candidate {
        /// Origin thread of the task
        thread: ThreadId,
        /// Task whose event is handed off
        task: TaskId,
        /// Waiters that already failed to take the event
        exclude: Vec<WaiterId>,
        /// The waiter to offer the event to, if any
        reply: RpcReplyPort<Result<Option<Candidate>, AppError>>,
    },
    /// Register the confirmation a hand-off waits for, before the event is sent
    Expect {
        /// Waiter the event goes to
        waiter: WaiterId,
        /// Task of the event
        task: TaskId,
        /// Sequence of the event
        seq: u64,
        /// Completed when the client confirms the event
        confirmed: oneshot::Sender<()>,
    },
    /// Give up on a confirmation; the waiting hand-off then fails at once
    Forget {
        /// Waiter the event went to
        waiter: WaiterId,
        /// Task of the event
        task: TaskId,
        /// Sequence of the event
        seq: u64,
    },
    /// The client printed an event
    Confirm {
        /// Waiter the event went to
        waiter: WaiterId,
        /// Task of the event
        task: TaskId,
        /// Sequence of the event
        seq: u64,
    },
}

struct Waiter {
    thread: ThreadId,
    queue: mpsc::Sender<HandoffEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingKey {
    waiter: WaiterId,
    task: TaskId,
    seq: u64,
}

/// Waiter actor state
#[derive(Default)]
pub struct WaiterState {
    waiters: HashMap<WaiterId, Waiter>,
    // oldest registration first, so the last entry is the most recent waiter
    tasks: HashMap<TaskId, Vec<WaiterId>>,
    // kept after a waiter closes: the client's confirmation of its last event
    // can arrive just after the stream ended
    pending: HashMap<PendingKey, oneshot::Sender<()>>,
}

impl WaiterState {
    fn candidate(&self, thread: ThreadId, task: TaskId, exclude: &[WaiterId]) -> Option<Candidate> {
        self.tasks
            .get(&task)?
            .iter()
            .rev()
            .filter(|waiter| !exclude.contains(waiter))
            .find_map(|waiter| {
                let entry = self.waiters.get(waiter)?;
                (entry.thread == thread && !entry.queue.is_closed()).then(|| Candidate {
                    waiter: *waiter,
                    queue: entry.queue.clone(),
                })
            })
    }

    fn watch(&mut self, waiter: WaiterId, task: TaskId) {
        if !self.waiters.contains_key(&waiter) {
            return;
        }
        let watchers = self.tasks.entry(task).or_default();
        if !watchers.contains(&waiter) {
            watchers.push(waiter);
        }
    }

    fn unwatch(&mut self, waiter: WaiterId, task: TaskId) {
        if let Some(watchers) = self.tasks.get_mut(&task) {
            watchers.retain(|watcher| *watcher != waiter);
            if watchers.is_empty() {
                self.tasks.remove(&task);
            }
        }
    }

    fn close(&mut self, waiter: WaiterId) {
        self.waiters.remove(&waiter);
        self.tasks.retain(|_, watchers| {
            watchers.retain(|watcher| *watcher != waiter);
            !watchers.is_empty()
        });
    }
}

/// Owns which `task wait` stream takes which task's events
pub struct WaiterActor;

impl Actor for WaiterActor {
    type Msg = WaiterMsg;
    type State = WaiterState;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        (): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(WaiterState::default())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            WaiterMsg::Open { thread, reply } => {
                let waiter = WaiterId::default();
                let (queue, events) = mpsc::channel(WAITER_QUEUE);
                state.waiters.insert(waiter, Waiter { thread, queue });
                send_reply(reply, Ok(Opened { waiter, events }));
            }
            WaiterMsg::Watch { waiter, task } => state.watch(waiter, task),
            WaiterMsg::Unwatch { waiter, task } => state.unwatch(waiter, task),
            WaiterMsg::Close { waiter } => state.close(waiter),
            WaiterMsg::Candidate {
                thread,
                task,
                exclude,
                reply,
            } => send_reply(reply, Ok(state.candidate(thread, task, &exclude))),
            WaiterMsg::Expect {
                waiter,
                task,
                seq,
                confirmed,
            } => {
                state
                    .pending
                    .insert(PendingKey { waiter, task, seq }, confirmed);
            }
            WaiterMsg::Forget { waiter, task, seq } => {
                state.pending.remove(&PendingKey { waiter, task, seq });
            }
            WaiterMsg::Confirm { waiter, task, seq } => {
                if let Some(confirmed) = state.pending.remove(&PendingKey { waiter, task, seq }) {
                    let _ = confirmed.send(());
                }
            }
        }
        Ok(())
    }
}

/// Handle to the waiter actor, shared by the API handlers and the callback actor
#[derive(Clone)]
pub struct WaiterRegistry(ActorRef<WaiterMsg>);

impl WaiterRegistry {
    /// Start the waiter actor, linked to `supervisor` when given
    pub async fn spawn(supervisor: Option<ActorCell>) -> Result<Self, SpawnErr> {
        let (actor, _handle) = match supervisor {
            Some(supervisor) => {
                WaiterActor::spawn_linked(None, WaiterActor, (), supervisor).await?
            }
            None => WaiterActor::spawn(None, WaiterActor, ()).await?,
        };
        Ok(Self(actor))
    }

    /// Open a waiter for `thread` and the queue its events arrive on
    ///
    /// The waiter stops receiving events when the returned session drops
    pub async fn open(
        &self,
        thread: ThreadId,
    ) -> Result<(WaiterSession, mpsc::Receiver<HandoffEvent>), AppError> {
        let Opened { waiter, events } =
            call(&self.0, |reply| WaiterMsg::Open { thread, reply }).await?;
        let session = WaiterSession {
            actor: self.0.clone(),
            waiter,
        };
        Ok((session, events))
    }

    /// Record that the client of `waiter` printed event `seq` of `task`
    pub fn confirm(&self, waiter: WaiterId, task: TaskId, seq: u64) {
        // a dead actor means the hand-off already fell back to normal delivery
        let _ = self.0.cast(WaiterMsg::Confirm { waiter, task, seq });
    }

    /// Offer one event to the waiters of `event.task` that belong to `thread`
    ///
    /// Tries the most recently registered waiter first. Returns whether a client
    /// printed the event and confirmed it; the caller delivers it normally
    /// otherwise, which may repeat an event whose confirmation was lost
    pub async fn hand_off(&self, thread: ThreadId, event: &TaskEvent, line: &str) -> bool {
        let EventPayload::Callback {
            event: callback, ..
        } = &event.payload
        else {
            return false;
        };
        let mut tried = Vec::new();
        while let Ok(Some(candidate)) = call(&self.0, |reply| WaiterMsg::Candidate {
            thread,
            task: event.task,
            exclude: tried.clone(),
            reply,
        })
        .await
        {
            tried.push(candidate.waiter);
            let offer = HandoffEvent {
                task: event.task,
                seq: event.seq.get(),
                kind: callback.event,
                continuation: callback.continuation,
                line: line.to_owned(),
            };
            if self.offer(candidate, offer).await {
                return true;
            }
        }
        false
    }

    async fn offer(&self, candidate: Candidate, event: HandoffEvent) -> bool {
        let (waiter, task, seq) = (candidate.waiter, event.task, event.seq);
        let (confirmed, printed) = oneshot::channel();
        // registered before the event is sent so a quick confirmation cannot
        // arrive first
        if self
            .0
            .cast(WaiterMsg::Expect {
                waiter,
                task,
                seq,
                confirmed,
            })
            .is_err()
        {
            return false;
        }
        let exchange = async {
            candidate.queue.send(event).await.ok()?;
            printed.await.ok()
        };
        let delivered = matches!(
            tokio::time::timeout(ACK_WINDOW, exchange).await,
            Ok(Some(()))
        );
        if !delivered {
            let _ = self.0.cast(WaiterMsg::Forget { waiter, task, seq });
        }
        delivered
    }
}

/// One connected wait request; dropping it removes every task it watches
pub struct WaiterSession {
    actor: ActorRef<WaiterMsg>,
    waiter: WaiterId,
}

impl WaiterSession {
    /// Id the client names when it confirms an event
    #[must_use]
    pub fn id(&self) -> WaiterId {
        self.waiter
    }

    /// Start receiving the callback events of `task`
    pub fn watch(&self, task: TaskId) {
        let _ = self.actor.cast(WaiterMsg::Watch {
            waiter: self.waiter,
            task,
        });
    }

    /// Stop receiving the callback events of `task`
    pub fn unwatch(&self, task: TaskId) {
        let _ = self.actor.cast(WaiterMsg::Unwatch {
            waiter: self.waiter,
            task,
        });
    }

    /// The stream could not print an event it took, so its hand-off fails at once
    pub fn reject(&self, task: TaskId, seq: u64) {
        let _ = self.actor.cast(WaiterMsg::Forget {
            waiter: self.waiter,
            task,
            seq,
        });
    }
}

impl Drop for WaiterSession {
    fn drop(&mut self) {
        let _ = self.actor.cast(WaiterMsg::Close {
            waiter: self.waiter,
        });
    }
}
