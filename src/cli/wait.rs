//! `homebased wait`

use std::io::Write;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::Args;
use uuid::Uuid;

use crate::cli::Ctx;
use crate::client::{Client, LineStream};
use crate::domain::ThreadId;
use crate::error::AppError;
use crate::task_wait::{
    WAIT_ACK_ROUTE, WAIT_ROUTE, WaitAck, WaitFrame, WaitIds, WaitKey, WaitKeys, WaitRequest,
    WaitSet, WaiterId,
};

use super::task::select_thread;

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

// with `--next`, events the daemon already holds arrive back to back, so a
// short pause after the first one gathers them into one wake of the agent
const NEXT_GATHER: Duration = Duration::from_secs(1);

/// Arguments of `homebased wait`
#[derive(Debug, Args)]
pub struct WaitArgs {
    /// Full task or GPU queue job UUIDs, 1 to 16
    #[arg(value_name = "ID", required = true)]
    ids: Vec<Uuid>,
    /// Thread that submitted the tasks and jobs. Defaults to the session environment.
    #[arg(long)]
    thread: Option<ThreadId>,
    /// Exit 0 once the next events are printed, for a background command that
    /// reports only when it exits
    #[arg(long)]
    next: bool,
}

/// What the wait follows
enum Targets {
    /// Ids the daemon has not yet said are tasks or jobs
    Named(WaitIds),
    /// Typed targets the daemon resolved, followed from then on
    Resolved(WaitSet),
}

impl Targets {
    fn request(&self, thread: ThreadId) -> WaitRequest {
        match self {
            Self::Named(ids) => WaitRequest::resolve(thread, ids),
            Self::Resolved(set) => WaitRequest::new(thread, set),
        }
    }

    /// Adopt what the daemon resolved the named ids to
    ///
    /// Only the first `Open` frame after the ids were named carries the
    /// resolution; once resolved, reconnects name typed targets and later
    /// frames change nothing
    fn adopt(&mut self, resolved: Option<Vec<WaitKey>>) -> Result<(), AppError> {
        let Self::Named(ids) = self else {
            return Ok(());
        };
        let internal = |message: String| AppError::Internal { message };
        let keys = resolved.ok_or_else(|| {
            internal("the daemon did not say which wait ids are tasks and which are jobs".into())
        })?;
        if !keys
            .iter()
            .map(|key| key.uuid())
            .eq(ids.ids().iter().copied())
        {
            return Err(internal(
                "the daemon resolved other ids than the ones named".into(),
            ));
        }
        let keys = WaitKeys::new(keys).map_err(|error| internal(error.to_string()))?;
        *self = Self::Resolved(WaitSet::new(&keys));
        Ok(())
    }

    fn set(&mut self) -> Result<&mut WaitSet, AppError> {
        match self {
            Self::Resolved(set) => Ok(set),
            Self::Named(_) => Err(AppError::Internal {
                message: "the daemon sent an event before it resolved the wait ids".into(),
            }),
        }
    }
}

/// How one connection to the daemon ended
enum StreamEnd {
    /// Every target reached its final outcome
    Done,
    /// The daemon is restarting
    Restart,
    /// The connection broke or went quiet
    Lost(String),
}

/// Run `homebased wait`
pub(super) async fn wait(ctx: &Ctx, args: WaitArgs) -> Result<ExitCode, AppError> {
    let ids = WaitIds::new(args.ids).map_err(|error| AppError::Usage {
        message: error.to_string(),
    })?;
    let thread = select_thread(args.thread, crate::domain::session_thread()?)?;
    let client = Client::new(ctx.home.sock_path());
    let mut targets = Targets::Named(ids);
    let mut outage: Option<Instant> = None;
    let mut backoff = BACKOFF_START;
    loop {
        let started = Instant::now();
        let mut printed = false;
        let end = match open(&client, &targets.request(thread)).await {
            Ok(mut stream) => {
                follow(&client, &mut stream, &mut targets, &mut printed, args.next).await?
            }
            Err(error) if is_outage(&error) => StreamEnd::Lost(error.to_string()),
            Err(error) => return Err(error),
        };
        // the agent has events to read, and reconnecting would only delay them
        if args.next && printed {
            return Ok(ExitCode::SUCCESS);
        }
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

async fn open(client: &Client, request: &WaitRequest) -> Result<LineStream, AppError> {
    let body = serde_json::to_value(request)?;
    match client.post_lines(WAIT_ROUTE, &body).await {
        // a daemon that predates bare ids refuses the request body before any
        // handler runs, which axum answers with a plain-text 422, and one that
        // predates waits has no route; retrying either would loop until the
        // reconnect budget runs out
        Err(AppError::Internal { message })
            if message.starts_with("http 422 ") || message.starts_with("http 404 ") =>
        {
            Err(AppError::Usage {
                message: format!(
                    "the running daemon is older than this binary and cannot run `homebased wait`; restart it with `homebased daemon restart`, or run `homebased update` ({message})"
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
/// which is what lets the daemon count the event as delivered. With `next`,
/// the stream ends shortly after the first event
async fn follow(
    client: &Client,
    stream: &mut LineStream,
    targets: &mut Targets,
    printed: &mut bool,
    next: bool,
) -> Result<StreamEnd, AppError> {
    let mut waiter = None;
    let mut gather_until: Option<Instant> = None;
    loop {
        let limit = gather_until.map_or(IDLE_LIMIT, |until| {
            until.saturating_duration_since(Instant::now())
        });
        let line = match tokio::time::timeout(limit, stream.next_line()).await {
            Err(_) if gather_until.is_some() => return Ok(StreamEnd::Done),
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
            WaitFrame::Open {
                waiter: id,
                targets: resolved,
            } => {
                waiter = Some(id);
                targets.adopt(resolved)?;
            }
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
                let set = targets.set()?;
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
                let set = targets.set()?;
                print_line(&line)?;
                *printed = true;
                if let Some(waiter) = waiter {
                    confirm(client, waiter, WaitKey::Job(job), seq).await;
                }
                set.apply_job(job, kind);
            }
        }
        if matches!(targets, Targets::Resolved(set) if set.is_done()) {
            return Ok(StreamEnd::Done);
        }
        if next && *printed && gather_until.is_none() {
            gather_until = Some(Instant::now() + NEXT_GATHER);
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
