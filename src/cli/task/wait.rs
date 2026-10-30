//! `homebased task wait`

use std::io::Write;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use crate::cli::Ctx;
use crate::client::{Client, LineStream};
use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::queue::JobId;
use crate::task_wait::{
    WAIT_ACK_ROUTE, WAIT_ROUTE, WaitAck, WaitFrame, WaitKey, WaitKeys, WaitRequest, WaitSet,
    WaiterId,
};

use super::select_thread;

// a daemon update restarts the socket within seconds; a longer outage is a failure
const RECONNECT_BUDGET: Duration = Duration::from_secs(5 * 60);

const BACKOFF_START: Duration = Duration::from_millis(250);
const BACKOFF_CAP: Duration = Duration::from_secs(5);

// the daemon pings every few seconds, so a longer silence means a stuck stream
const IDLE_LIMIT: Duration = Duration::from_secs(30);

// an ack that takes longer is dropped; the daemon then delivers the event normally
const ACK_TIMEOUT: Duration = Duration::from_secs(3);

// a stream that stayed up this long was healthy, so a later outage starts a fresh budget
const HEALTHY_AFTER: Duration = Duration::from_secs(10);

/// How one connection to the daemon ended
enum StreamEnd {
    /// Every target reached its final outcome
    Done,
    /// The daemon is restarting
    Restart,
    /// The connection broke or went quiet
    Lost(String),
}

pub(super) async fn wait(
    ctx: &Ctx,
    tasks: Vec<TaskId>,
    jobs: Vec<JobId>,
    thread: Option<ThreadId>,
) -> Result<ExitCode, AppError> {
    let targets = WaitKeys::new(tasks, jobs).map_err(|error| AppError::Usage {
        message: error.to_string(),
    })?;
    let thread = select_thread(thread, crate::domain::session_thread()?)?;
    let client = Client::new(ctx.home.sock_path());
    let mut set = WaitSet::new(&targets);
    let mut outage: Option<Instant> = None;
    let mut backoff = BACKOFF_START;
    loop {
        let started = Instant::now();
        let mut printed = false;
        let end = match open(&client, thread, &set).await {
            Ok(mut stream) => follow(&client, &mut stream, &mut set, &mut printed).await?,
            Err(error) if is_outage(&error) => StreamEnd::Lost(error.to_string()),
            Err(error) => return Err(error),
        };
        let reason = match end {
            StreamEnd::Done => return Ok(ExitCode::SUCCESS),
            StreamEnd::Restart => "the daemon is restarting".to_owned(),
            StreamEnd::Lost(reason) => reason,
        };

        if printed || started.elapsed() >= HEALTHY_AFTER {
            outage = None;
            backoff = BACKOFF_START;
        }
        let since = *outage.get_or_insert_with(Instant::now);
        if since.elapsed() >= RECONNECT_BUDGET {
            return Err(AppError::DaemonUnavailable {
                message: format!(
                    "gave up waiting after {}s without the daemon: {reason}",
                    RECONNECT_BUDGET.as_secs()
                ),
            });
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_CAP);
    }
}

fn is_outage(error: &AppError) -> bool {
    matches!(
        error,
        AppError::DaemonUnavailable { .. } | AppError::DaemonBusy
    )
}

async fn open(client: &Client, thread: ThreadId, set: &WaitSet) -> Result<LineStream, AppError> {
    let body = serde_json::to_value(WaitRequest::new(thread, set))?;
    match client.post_lines(WAIT_ROUTE, &body).await {
        // a daemon that predates job waits refuses the request body before
        // any handler runs, which axum answers with a plain-text 422
        Err(AppError::Internal { message })
            if set.has_jobs() && message.starts_with("http 422 ") =>
        {
            Err(AppError::Usage {
                message: format!(
                    "the running daemon cannot wait on jobs; restart it with `homebased daemon restart` so it runs this binary ({message})"
                ),
            })
        }
        result => result,
    }
}

/// Print each event line as it arrives until the stream ends
///
/// Only event lines reach stdout, because every line wakes the agent that
/// armed the monitor. Each line is confirmed to the daemon once it is flushed,
/// which is what lets the daemon count the event as delivered
async fn follow(
    client: &Client,
    stream: &mut LineStream,
    set: &mut WaitSet,
    printed: &mut bool,
) -> Result<StreamEnd, AppError> {
    let mut waiter = None;
    loop {
        let line = match tokio::time::timeout(IDLE_LIMIT, stream.next_line()).await {
            Err(_) => return Ok(StreamEnd::Lost("the daemon went quiet".into())),
            Ok(Err(error)) if is_outage(&error) => return Ok(StreamEnd::Lost(error.to_string())),
            Ok(Err(error)) => return Err(error),
            Ok(Ok(None)) => return Ok(StreamEnd::Lost("the daemon closed the stream".into())),
            Ok(Ok(Some(line))) => line,
        };
        let frame: WaitFrame =
            serde_json::from_slice(&line).map_err(|error| AppError::Internal {
                message: format!("invalid wait frame: {error}"),
            })?;
        match frame {
            WaitFrame::Open { waiter: id } => waiter = Some(id),
            WaitFrame::Ping => {}
            WaitFrame::Restart => return Ok(StreamEnd::Restart),
            WaitFrame::Done => return Ok(StreamEnd::Done),
            WaitFrame::Event {
                task,
                seq,
                kind,
                continuation,
                line,
            } => {
                print_line(&line)?;
                *printed = true;
                if let Some(waiter) = waiter {
                    confirm(client, waiter, WaitKey::Task(task), seq).await;
                }
                set.apply(task, kind, continuation);
            }
            WaitFrame::JobEvent {
                job,
                seq,
                kind,
                line,
            } => {
                print_line(&line)?;
                *printed = true;
                if let Some(waiter) = waiter {
                    confirm(client, waiter, WaitKey::Job(job), seq).await;
                }
                set.apply_job(job, kind);
            }
        }
        if set.is_done() {
            return Ok(StreamEnd::Done);
        }
    }
}

fn print_line(line: &str) -> Result<(), AppError> {
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{line}")?;
    stdout.flush()?;
    Ok(())
}

/// Tell the daemon the event reached stdout
///
/// A failed ack is not an error: the daemon times out and delivers the event
/// normally, which agents already dedupe by `(task, seq)` or `(job, seq)`
async fn confirm(client: &Client, waiter: WaiterId, key: WaitKey, seq: u64) {
    let ack = WaitAck::new(waiter, key, seq);
    let Ok(body) = serde_json::to_value(&ack) else {
        return;
    };
    let _ = tokio::time::timeout(ACK_TIMEOUT, client.post(WAIT_ACK_ROUTE, &body)).await;
}
