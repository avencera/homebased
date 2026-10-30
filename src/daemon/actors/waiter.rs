//! `WaiterActor` owns the connected `homebased wait` streams that take the callback
//! events of a task or GPU queue job in place of the origin thread's normal
//! delivery

use std::collections::HashMap;
use std::time::Duration;

use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, RpcReplyPort, SpawnErr};
use tokio::sync::{mpsc, oneshot};

use crate::callback::EventKind;
use crate::daemon::actors::{call, send_reply};
use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::events::{EventPayload, TaskEvent};
use crate::queue::{JobEvent, JobEventKind, JobId};
use crate::task_wait::{WaitKey, WaiterId};

// the client confirms an event only after its line reached stdout; without that
// confirmation the event falls through to normal delivery, so a client that died
// between receiving and printing cannot swallow it
const ACK_WINDOW: Duration = Duration::from_secs(5);

// handed-off events wait here while the stream writes the one before them
const WAITER_QUEUE: usize = 16;

/// Content of a callback event offered to a waiter
#[derive(Clone)]
pub enum HandoffEvent {
    /// A task event
    Task {
        /// Task the event belongs to
        task: TaskId,
        /// Per-task event sequence
        seq: u64,
        /// What the event reports
        kind: EventKind,
        /// Continuation run, on `TASK_WAITING`
        continuation: Option<TaskId>,
        /// The `HOMEBASED_EVENT` line normal delivery would have sent
        line: String,
    },
    /// A GPU queue job event
    Job {
        /// Job the event belongs to
        job: JobId,
        /// Per-job event sequence
        seq: u64,
        /// What the event reports
        kind: JobEventKind,
        /// The `HOMEBASED_EVENT` line normal delivery would have sent
        line: String,
    },
}

impl HandoffEvent {
    /// Target the event belongs to
    #[must_use]
    pub fn key(&self) -> WaitKey {
        match self {
            Self::Task { task, .. } => WaitKey::Task(*task),
            Self::Job { job, .. } => WaitKey::Job(*job),
        }
    }

    /// Sequence of the event within its target
    #[must_use]
    pub fn seq(&self) -> u64 {
        match self {
            Self::Task { seq, .. } | Self::Job { seq, .. } => *seq,
        }
    }
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
    /// Start sending the callback events of `key` to `waiter`
    Watch {
        /// Waiter that takes the events
        waiter: WaiterId,
        /// Target to watch
        key: WaitKey,
    },
    /// Stop sending the callback events of `key` to `waiter`
    Unwatch {
        /// Waiter that no longer takes the events
        waiter: WaiterId,
        /// Target to stop watching
        key: WaitKey,
    },
    /// Remove a waiter and every target it watches
    Close {
        /// Waiter to remove
        waiter: WaiterId,
    },
    /// The most recently registered live waiter for `key` that belongs to `thread`
    Candidate {
        /// Origin thread of the target
        thread: ThreadId,
        /// Target whose event is handed off
        key: WaitKey,
        /// Waiters that already failed to take the event
        exclude: Vec<WaiterId>,
        /// The waiter to offer the event to, if any
        reply: RpcReplyPort<Result<Option<Candidate>, AppError>>,
    },
    /// Register the confirmation a hand-off waits for, before the event is sent
    Expect {
        /// Waiter the event goes to
        waiter: WaiterId,
        /// Target of the event
        key: WaitKey,
        /// Sequence of the event
        seq: u64,
        /// Completed when the client confirms the event
        confirmed: oneshot::Sender<()>,
    },
    /// Give up on a confirmation; the waiting hand-off then fails at once
    Forget {
        /// Waiter the event went to
        waiter: WaiterId,
        /// Target of the event
        key: WaitKey,
        /// Sequence of the event
        seq: u64,
    },
    /// The client printed an event
    Confirm {
        /// Waiter the event went to
        waiter: WaiterId,
        /// Target of the event
        key: WaitKey,
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
    key: WaitKey,
    seq: u64,
}

/// Waiter actor state
#[derive(Default)]
pub struct WaiterState {
    waiters: HashMap<WaiterId, Waiter>,
    // oldest registration first, so the last entry is the most recent waiter
    watched: HashMap<WaitKey, Vec<WaiterId>>,
    // kept after a waiter closes: the client's confirmation of its last event
    // can arrive just after the stream ended
    pending: HashMap<PendingKey, oneshot::Sender<()>>,
}

impl WaiterState {
    fn candidate(&self, thread: ThreadId, key: WaitKey, exclude: &[WaiterId]) -> Option<Candidate> {
        self.watched
            .get(&key)?
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

    fn watch(&mut self, waiter: WaiterId, key: WaitKey) {
        if !self.waiters.contains_key(&waiter) {
            return;
        }
        let watchers = self.watched.entry(key).or_default();
        if !watchers.contains(&waiter) {
            watchers.push(waiter);
        }
    }

    fn unwatch(&mut self, waiter: WaiterId, key: WaitKey) {
        if let Some(watchers) = self.watched.get_mut(&key) {
            watchers.retain(|watcher| *watcher != waiter);
            if watchers.is_empty() {
                self.watched.remove(&key);
            }
        }
    }

    fn close(&mut self, waiter: WaiterId) {
        self.waiters.remove(&waiter);
        self.watched.retain(|_, watchers| {
            watchers.retain(|watcher| *watcher != waiter);
            !watchers.is_empty()
        });
    }
}

/// Owns which `homebased wait` stream takes which target's events
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
            WaiterMsg::Watch { waiter, key } => state.watch(waiter, key),
            WaiterMsg::Unwatch { waiter, key } => state.unwatch(waiter, key),
            WaiterMsg::Close { waiter } => state.close(waiter),
            WaiterMsg::Candidate {
                thread,
                key,
                exclude,
                reply,
            } => send_reply(reply, Ok(state.candidate(thread, key, &exclude))),
            WaiterMsg::Expect {
                waiter,
                key,
                seq,
                confirmed,
            } => {
                state
                    .pending
                    .insert(PendingKey { waiter, key, seq }, confirmed);
            }
            WaiterMsg::Forget { waiter, key, seq } => {
                state.pending.remove(&PendingKey { waiter, key, seq });
            }
            WaiterMsg::Confirm { waiter, key, seq } => {
                if let Some(confirmed) = state.pending.remove(&PendingKey { waiter, key, seq }) {
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

    /// Record that the client of `waiter` printed event `seq` of `key`
    pub fn confirm(&self, waiter: WaiterId, key: WaitKey, seq: u64) {
        // a dead actor means the hand-off already fell back to normal delivery
        let _ = self.0.cast(WaiterMsg::Confirm { waiter, key, seq });
    }

    /// Offer one task event to the waiters of `event.task` that belong to `thread`
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
        let offer = HandoffEvent::Task {
            task: event.task,
            seq: event.seq.get(),
            kind: callback.event,
            continuation: callback.continuation,
            line: line.to_owned(),
        };
        matches!(
            self.hand_off_event(thread, offer, || std::future::ready(Some(true)))
                .await,
            JobHandoff::Printed
        )
    }

    /// Offer one job event to the waiters of `event.job` that belong to `thread`
    ///
    /// Same contract as [`Self::hand_off`]. `current` runs before each offer:
    /// `Some(false)` stops with [`JobHandoff::Obsolete`], and `None`, when
    /// currency cannot be told, stops with [`JobHandoff::Declined`] so normal
    /// delivery decides
    pub async fn hand_off_job<F>(
        &self,
        thread: ThreadId,
        event: &JobEvent,
        line: &str,
        current: impl Fn() -> F,
    ) -> JobHandoff
    where
        F: Future<Output = Option<bool>>,
    {
        let offer = HandoffEvent::Job {
            job: event.job,
            seq: event.seq,
            kind: event.event,
            line: line.to_owned(),
        };
        self.hand_off_event(thread, offer, current).await
    }

    async fn hand_off_event<F>(
        &self,
        thread: ThreadId,
        offer: HandoffEvent,
        current: impl Fn() -> F,
    ) -> JobHandoff
    where
        F: Future<Output = Option<bool>>,
    {
        let key = offer.key();
        let mut tried = Vec::new();
        while let Ok(Some(candidate)) = call(&self.0, |reply| WaiterMsg::Candidate {
            thread,
            key,
            exclude: tried.clone(),
            reply,
        })
        .await
        {
            // a waiter that let the ack window pass can take seconds, and a
            // blocked notice may stop being current meanwhile
            match current().await {
                Some(true) => {}
                Some(false) => return JobHandoff::Obsolete,
                None => return JobHandoff::Declined,
            }
            tried.push(candidate.waiter);
            if self.offer(candidate, offer.clone()).await {
                return JobHandoff::Printed;
            }
        }
        JobHandoff::Declined
    }

    async fn offer(&self, candidate: Candidate, event: HandoffEvent) -> bool {
        let (waiter, key, seq) = (candidate.waiter, event.key(), event.seq());
        let (confirmed, printed) = oneshot::channel();
        // registered before the event is sent so a quick confirmation cannot
        // arrive first
        if self
            .0
            .cast(WaiterMsg::Expect {
                waiter,
                key,
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
            let _ = self.0.cast(WaiterMsg::Forget { waiter, key, seq });
        }
        delivered
    }
}

/// How a hand-off ended
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobHandoff {
    /// A waiter printed the event and confirmed it
    Printed,
    /// No waiter took it; deliver it normally
    Declined,
    /// The event stopped being current before a waiter took it
    Obsolete,
}

/// One connected wait request; dropping it removes every target it watches
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

    /// Start receiving the callback events of `key`
    pub fn watch(&self, key: WaitKey) {
        let _ = self.actor.cast(WaiterMsg::Watch {
            waiter: self.waiter,
            key,
        });
    }

    /// Stop receiving the callback events of `key`
    pub fn unwatch(&self, key: WaitKey) {
        let _ = self.actor.cast(WaiterMsg::Unwatch {
            waiter: self.waiter,
            key,
        });
    }

    /// The stream could not print an event it took, so its hand-off fails at once
    pub fn reject(&self, key: WaitKey, seq: u64) {
        let _ = self.actor.cast(WaiterMsg::Forget {
            waiter: self.waiter,
            key,
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
