//! `POST /v1/tasks/wait`: stream callback events to a `task wait` client
//!
//! The origin inbox stays the single source of truth. A connected waiter only
//! changes where the callback actor sends a task's events: the actor offers each
//! event to the waiter and settles it `Delivered` once the client confirms on
//! `POST /v1/tasks/wait/ack` that it printed the line, otherwise it falls back to
//! the origin thread's normal delivery. Because only a printed event settles,
//! the handler can replay the inbox: a settled event advances its chains without
//! being printed again, and an unsettled one is still owed to someone

use std::collections::HashMap;
use std::convert::Infallible;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::Response;
use axum::routing::post;
use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tracing::warn;

use crate::callback::EventKind;
use crate::daemon::AppState;
use crate::daemon::actors::waiter::{HandoffEvent, WaiterSession};
use crate::daemon::actors::{StoreMsg, SupervisorMsg, call};
use crate::domain::{TaskId, ThreadId};
use crate::error::{AppError, WaitBlocker};
use crate::events::EventPayload;
use crate::task_wait::{WAIT_ACK_ROUTE, WAIT_ROUTE, WaitAck, WaitFrame, WaitRequest, WaitSet};

// keeps the stream observable: a dead client fails the write, and the client
// reconnects when a stream goes quiet
const PING_INTERVAL: Duration = Duration::from_secs(5);

// a hand-off can race with a normal delivery that was already under way, so
// the inbox is re-read now and then to catch events that settled elsewhere
const RECONCILE_EVERY_PINGS: u32 = 3;

// a client that stops reading must not hold its stream, and with it graceful
// shutdown, open
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Routes that stream events to waiters
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(WAIT_ROUTE, post(wait))
        .route(WAIT_ACK_ROUTE, post(ack))
}

/// The client printed one event; the callback actor may settle it
async fn ack(State(state): State<AppState>, Json(ack): Json<WaitAck>) -> StatusCode {
    state.waiters.confirm(ack.waiter, ack.task, ack.seq);
    StatusCode::NO_CONTENT
}

async fn wait(
    State(state): State<AppState>,
    Json(request): Json<WaitRequest>,
) -> Result<Response, AppError> {
    for task in request.tasks.tasks() {
        check_target(&state, request.thread, *task).await?;
    }

    let (session, events) = state.waiters.open(request.thread).await?;
    let (frames, body) = mpsc::channel(1);
    let pump = Pump {
        set: WaitSet::new(&request.tasks),
        state,
        session,
        events,
        frames,
        floor: HashMap::new(),
        fresh: Vec::new(),
    };
    tokio::spawn(async move {
        if let Err(error) = pump.serve().await {
            // dropping the stream makes the client reconnect
            warn!("task wait stream: {error}");
        }
    });

    let stream = futures_util::stream::unfold(body, |mut body| async {
        let frame: Bytes = body.recv().await?;
        Some((Ok::<_, Infallible>(frame), body))
    });
    Response::builder()
        .header(CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from_stream(stream))
        .map_err(|error| AppError::Internal {
            message: format!("build wait response: {error}"),
        })
}

/// Refuse a task whose events cannot go to a waiter of `thread`
async fn check_target(state: &AppState, thread: ThreadId, task: TaskId) -> Result<(), AppError> {
    let Some(route) = call(&state.store, |reply| StoreMsg::OriginRoute {
        id: task,
        reply,
    })
    .await?
    else {
        let known = call(&state.store, |reply| StoreMsg::GetTask { id: task, reply })
            .await?
            .is_some();
        return Err(if known {
            AppError::WaitUnavailable {
                task,
                reason: WaitBlocker::NoCallbackRoute,
            }
        } else {
            AppError::TaskNotFound { id: task }
        });
    };
    let reason = if route.origin_machine != state.machine.identity.machine {
        WaitBlocker::Remote
    } else if route.thread != thread {
        WaitBlocker::NotOwner
    } else {
        return Ok(());
    };
    Err(AppError::WaitUnavailable { task, reason })
}

async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    // a closed channel means the daemon is gone, which ends the stream the same way
    let _ = shutdown.wait_for(|requested| *requested).await;
}

/// What the stream does after handling one input
enum Flow {
    Continue,
    /// The client is gone or the wait is over
    Stop,
    /// The daemon is shutting down; tell the client to reconnect
    Restart,
}

/// How one frame write ended
enum Sent {
    Written,
    /// The client is gone or stopped reading
    Gone,
    Shutdown,
}

struct Pump {
    state: AppState,
    session: WaiterSession,
    events: mpsc::Receiver<HandoffEvent>,
    frames: mpsc::Sender<Bytes>,
    set: WaitSet,
    /// Highest sequence each task's wait has accounted for
    floor: HashMap<TaskId, u64>,
    /// Tasks that joined the wait and still need their inbox read
    fresh: Vec<TaskId>,
}

impl Pump {
    async fn serve(mut self) -> Result<(), AppError> {
        let mut shutdown = self.state.shutdown.clone();
        let tasks = self.set.pending().to_vec();
        for task in &tasks {
            self.session.watch(*task);
        }
        self.fresh = tasks;
        let open = WaitFrame::Open {
            waiter: self.session.id(),
        };
        if !self.push(&open).await {
            return Ok(());
        }
        let mut interval = tokio::time::interval(PING_INTERVAL);
        let mut pings = 0u32;
        loop {
            match self.turn(&mut shutdown, &mut interval, &mut pings).await? {
                Flow::Continue => {}
                Flow::Stop => return Ok(()),
                Flow::Restart => {
                    self.push(&WaitFrame::Restart).await;
                    return Ok(());
                }
            }
        }
    }

    /// Wait for the next input and handle it
    async fn turn(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
        interval: &mut tokio::time::Interval,
        pings: &mut u32,
    ) -> Result<Flow, AppError> {
        let flow = self.reconcile().await?;
        if !matches!(flow, Flow::Continue) {
            return Ok(flow);
        }
        if self.set.is_done() {
            self.push(&WaitFrame::Done).await;
            return Ok(Flow::Stop);
        }
        Ok(tokio::select! {
            () = shutdown_requested(shutdown) => Flow::Restart,
            () = self.frames.closed() => Flow::Stop,
            Some(event) = self.events.recv() => self.live(event).await,
            _ = interval.tick() => {
                *pings += 1;
                match self.send(&WaitFrame::Ping).await {
                    Sent::Written => {
                        if pings.is_multiple_of(RECONCILE_EVERY_PINGS) {
                            self.fresh = self.set.pending().to_vec();
                        }
                        Flow::Continue
                    }
                    Sent::Gone => Flow::Stop,
                    Sent::Shutdown => Flow::Restart,
                }
            }
        })
    }

    /// Write one frame, giving up after [`SEND_TIMEOUT`]
    async fn push(&self, frame: &WaitFrame) -> bool {
        let Ok(mut bytes) = serde_json::to_vec(frame) else {
            return false;
        };
        bytes.push(b'\n');
        matches!(
            tokio::time::timeout(SEND_TIMEOUT, self.frames.send(Bytes::from(bytes))).await,
            Ok(Ok(()))
        )
    }

    /// Write one frame unless the daemon starts shutting down first
    async fn send(&self, frame: &WaitFrame) -> Sent {
        let mut shutdown = self.state.shutdown.clone();
        tokio::select! {
            written = self.push(frame) => if written { Sent::Written } else { Sent::Gone },
            () = shutdown_requested(&mut shutdown) => Sent::Shutdown,
        }
    }

    /// Print one handed-off event
    ///
    /// The callback actor settles it only when the client confirms the print, so
    /// the wait moves on here without deciding whether the event counts as delivered
    async fn live(&mut self, event: HandoffEvent) -> Flow {
        let HandoffEvent {
            task,
            seq,
            kind,
            continuation,
            line,
        } = event;
        let frame = WaitFrame::Event {
            task,
            seq,
            kind,
            continuation,
            line,
        };
        let flow = match self.send(&frame).await {
            Sent::Written => {
                self.account(task, seq, kind, continuation);
                return Flow::Continue;
            }
            Sent::Gone => Flow::Stop,
            Sent::Shutdown => Flow::Restart,
        };
        // the event never reached the client, so normal delivery takes it now
        // instead of after the confirmation window
        self.session.reject(task, seq);
        flow
    }

    /// Update the wait for one event, printed or not
    fn account(&mut self, task: TaskId, seq: u64, kind: EventKind, continuation: Option<TaskId>) {
        let floor = self.floor.entry(task).or_default();
        *floor = (*floor).max(seq);
        let next = self.set.apply(task, kind, continuation);
        if !self.set.contains(task) {
            self.session.unwatch(task);
        }
        if let Some(next) = next {
            self.session.watch(next);
            self.fresh.push(next);
        }
    }

    /// Read the inbox of every task that joined the wait
    ///
    /// A settled event was printed or delivered normally, so it advances the wait
    /// without being printed again. An event whose hand-off is still unconfirmed
    /// is unsettled and ends the replay: it is owed to a waiter, this one included.
    /// Hand-offs already queued for this stream are printed before the replay
    async fn reconcile(&mut self) -> Result<Flow, AppError> {
        while !self.fresh.is_empty() {
            for task in std::mem::take(&mut self.fresh) {
                if !self.set.contains(task) {
                    continue;
                }
                let rows = call(&self.state.store, |reply| StoreMsg::InboundEvents {
                    id: task,
                    reply,
                })
                .await?;
                while let Ok(event) = self.events.try_recv() {
                    let flow = self.live(event).await;
                    if !matches!(flow, Flow::Continue) {
                        return Ok(flow);
                    }
                }
                for row in rows {
                    if row.delivery.is_unsettled() {
                        break;
                    }
                    let EventPayload::Callback { event, .. } = &row.event.payload else {
                        continue;
                    };
                    let seq = row.event.seq.get();
                    if self.floor.get(&task).is_some_and(|floor| seq <= *floor) {
                        continue;
                    }
                    self.account(task, seq, event.event, event.continuation);
                }
                // events that are still pending go out now, to this waiter
                self.state
                    .supervisor
                    .cast(SupervisorMsg::DispatchInbox { id: task })
                    .map_err(|error| AppError::Internal {
                        message: format!("wake inbox worker: {error}"),
                    })?;
            }
        }
        Ok(Flow::Continue)
    }
}
