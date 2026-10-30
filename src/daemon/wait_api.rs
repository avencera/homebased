//! `POST /v1/tasks/wait`: stream callback events to a `task wait` client
//!
//! The origin inbox stays the single source of truth. A connected waiter only
//! changes where a target's events go: the callback actor for task events, and
//! the job event sender for GPU queue job events, offer each event to the waiter
//! and settle it as delivered once the client confirms on
//! `POST /v1/tasks/wait/ack` that it printed the line, otherwise they fall back
//! to the origin thread's normal delivery. Because only a printed event settles,
//! the handler can replay the inbox: a settled event advances the wait without
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
use crate::error::{AppError, JobWaitBlocker, WaitBlocker};
use crate::events::EventPayload;
use crate::queue::delivery::JobSubmission;
use crate::queue::{JobEventKind, JobId, QueueError};
use crate::task_wait::{
    WAIT_ACK_ROUTE, WAIT_ROUTE, WaitAck, WaitFrame, WaitKey, WaitRequest, WaitSet,
};

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

/// The client printed one event; the sender that handed it off may settle it
async fn ack(
    State(state): State<AppState>,
    Json(ack): Json<WaitAck>,
) -> Result<StatusCode, AppError> {
    let key = ack.key().ok_or_else(|| AppError::Usage {
        message: "a wait ack must name exactly one of task and job".into(),
    })?;
    state.waiters.confirm(ack.waiter, key, ack.seq);
    Ok(StatusCode::NO_CONTENT)
}

async fn wait(
    State(state): State<AppState>,
    Json(request): Json<WaitRequest>,
) -> Result<Response, AppError> {
    let targets = request.targets().map_err(|error| AppError::Usage {
        message: error.to_string(),
    })?;
    for key in targets.keys() {
        match *key {
            WaitKey::Task(task) => check_target(&state, request.thread, task).await?,
            WaitKey::Job(job) => check_job(&state, request.thread, job).await?,
        }
    }

    let (session, events) = state.waiters.open(request.thread).await?;
    let (frames, body) = mpsc::channel(1);
    let pump = Pump {
        set: WaitSet::new(&targets),
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
        if known {
            return Err(AppError::WaitUnavailable {
                task,
                reason: WaitBlocker::NoCallbackRoute,
            });
        }
        // agents mix up the two ids, since a job submit prints an id that
        // looks like any task id
        let job = JobId::from_uuid(task.0);
        let is_job = call(&state.store, |reply| StoreMsg::JobRoute { job, reply })
            .await?
            .is_some();
        return Err(if is_job {
            AppError::WaitTargetIsJob { job }
        } else {
            AppError::TaskNotFound { id: task }
        });
    };
    let reason = if route.origin_machine != state.machine.identity.machine {
        WaitBlocker::Remote
    } else if route.thread.is_detached() {
        WaitBlocker::Detached
    } else if route.thread != thread {
        WaitBlocker::NotOwner
    } else {
        return Ok(());
    };
    Err(AppError::WaitUnavailable { task, reason })
}

/// Refuse a job whose events cannot go to a waiter of `thread`
///
/// Job routes live only on the origin machine, so a job submitted elsewhere is
/// not found here
async fn check_job(state: &AppState, thread: ThreadId, job: JobId) -> Result<(), AppError> {
    let route = call(&state.store, |reply| StoreMsg::JobRoute { job, reply })
        .await?
        .ok_or(QueueError::JobNotFound { job })?;
    let reason = if matches!(route.submission, JobSubmission::Rejected { .. }) {
        JobWaitBlocker::Rejected
    } else if matches!(route.submission, JobSubmission::Unknown) {
        JobWaitBlocker::Unconfirmed
    } else if route.thread.is_detached() {
        JobWaitBlocker::Detached
    } else if route.thread != thread {
        JobWaitBlocker::NotOwner
    } else {
        return Ok(());
    };
    Err(AppError::JobWaitUnavailable { job, reason })
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
    /// Highest sequence each target's wait has accounted for
    floor: HashMap<WaitKey, u64>,
    /// Targets that joined the wait and still need their inbox read
    fresh: Vec<WaitKey>,
}

impl Pump {
    async fn serve(mut self) -> Result<(), AppError> {
        let mut shutdown = self.state.shutdown.clone();
        let keys = self.set.pending().to_vec();
        for key in &keys {
            self.session.watch(*key);
        }
        self.fresh = keys;
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
    /// The sender settles it only when the client confirms the print, so the
    /// wait moves on here without deciding whether the event counts as delivered
    async fn live(&mut self, event: HandoffEvent) -> Flow {
        let (key, seq) = (event.key(), event.seq());
        let (frame, step) = match event {
            HandoffEvent::Task {
                task,
                seq,
                kind,
                continuation,
                line,
            } => (
                WaitFrame::Event {
                    task,
                    seq,
                    kind,
                    continuation,
                    line,
                },
                Step::Task {
                    task,
                    kind,
                    continuation,
                },
            ),
            HandoffEvent::Job {
                job,
                seq,
                kind,
                line,
            } => (
                WaitFrame::JobEvent {
                    job,
                    seq,
                    kind,
                    line,
                },
                Step::Job { job, kind },
            ),
        };
        let flow = match self.send(&frame).await {
            Sent::Written => {
                self.account(seq, step);
                return Flow::Continue;
            }
            Sent::Gone => Flow::Stop,
            Sent::Shutdown => Flow::Restart,
        };
        // the event never reached the client, so normal delivery takes it now
        // instead of after the confirmation window
        self.session.reject(key, seq);
        flow
    }

    /// Update the wait for one event, printed or not
    fn account(&mut self, seq: u64, step: Step) {
        let key = step.key();
        let floor = self.floor.entry(key).or_default();
        *floor = (*floor).max(seq);
        let next = match step {
            Step::Task {
                task,
                kind,
                continuation,
            } => self.set.apply(task, kind, continuation),
            Step::Job { job, kind } => {
                self.set.apply_job(job, kind);
                None
            }
        };
        if !self.set.contains(key) {
            self.session.unwatch(key);
        }
        if let Some(next) = next {
            let next = WaitKey::Task(next);
            self.session.watch(next);
            self.fresh.push(next);
        }
    }

    /// Whether the wait already accounted for event `seq` of `key`
    fn seen(&self, key: WaitKey, seq: u64) -> bool {
        self.floor.get(&key).is_some_and(|floor| seq <= *floor)
    }

    /// Read the inbox of every target that joined the wait
    ///
    /// A settled event was printed or delivered normally, so it advances the wait
    /// without being printed again. An event whose hand-off is still unconfirmed
    /// is unsettled and ends the replay: it is owed to a waiter, this one included.
    /// Hand-offs already queued for this stream are printed before the replay
    async fn reconcile(&mut self) -> Result<Flow, AppError> {
        while !self.fresh.is_empty() {
            for key in std::mem::take(&mut self.fresh) {
                if !self.set.contains(key) {
                    continue;
                }
                let flow = match key {
                    WaitKey::Task(task) => self.reconcile_task(task).await?,
                    WaitKey::Job(job) => self.reconcile_job(job).await?,
                };
                if !matches!(flow, Flow::Continue) {
                    return Ok(flow);
                }
            }
        }
        Ok(Flow::Continue)
    }

    /// Print the hand-offs already queued for this stream
    async fn drain_live(&mut self) -> Flow {
        while let Ok(event) = self.events.try_recv() {
            let flow = self.live(event).await;
            if !matches!(flow, Flow::Continue) {
                return flow;
            }
        }
        Flow::Continue
    }

    async fn reconcile_task(&mut self, task: TaskId) -> Result<Flow, AppError> {
        let rows = call(&self.state.store, |reply| StoreMsg::InboundEvents {
            id: task,
            reply,
        })
        .await?;
        let flow = self.drain_live().await;
        if !matches!(flow, Flow::Continue) {
            return Ok(flow);
        }
        let key = WaitKey::Task(task);
        for row in rows {
            if row.delivery.is_unsettled() {
                break;
            }
            let EventPayload::Callback { event, .. } = &row.event.payload else {
                continue;
            };
            let seq = row.event.seq.get();
            if self.seen(key, seq) {
                continue;
            }
            let step = Step::Task {
                task,
                kind: event.event,
                continuation: event.continuation,
            };
            self.account(seq, step);
        }
        // events that are still pending go out now, to this waiter
        self.state
            .supervisor
            .cast(SupervisorMsg::DispatchInbox { id: task })
            .map_err(|error| AppError::Internal {
                message: format!("wake inbox worker: {error}"),
            })?;
        Ok(Flow::Continue)
    }

    /// Catch a job up on its settled events
    ///
    /// The job event sender rescans every pending inbox several times a second,
    /// so the unsettled events reach this waiter without a wake
    async fn reconcile_job(&mut self, job: JobId) -> Result<Flow, AppError> {
        let events = call(&self.state.store, |reply| StoreMsg::SettledJobEvents {
            job,
            reply,
        })
        .await?;
        let flow = self.drain_live().await;
        if !matches!(flow, Flow::Continue) {
            return Ok(flow);
        }
        // no floor check: normal delivery can settle a final event after this
        // stream printed a later `JOB_ATTENTION`, and a job event applies the
        // same way twice, so every settled event is accounted again
        for event in events {
            let step = Step::Job {
                job,
                kind: event.event,
            };
            self.account(event.seq, step);
        }
        Ok(Flow::Continue)
    }
}

/// What one event reports, as far as the wait is concerned
#[derive(Clone, Copy)]
enum Step {
    Task {
        task: TaskId,
        kind: EventKind,
        continuation: Option<TaskId>,
    },
    Job {
        job: JobId,
        kind: JobEventKind,
    },
}

impl Step {
    fn key(self) -> WaitKey {
        match self {
            Self::Task { task, .. } => WaitKey::Task(task),
            Self::Job { job, .. } => WaitKey::Job(job),
        }
    }
}
