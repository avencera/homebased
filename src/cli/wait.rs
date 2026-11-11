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
    WaitSet, WaiterId, wakes_final_only,
};
use crate::thread_watch::{CompactOutcome, line_ends_watch};

use super::task::{is_t3_claude_caller, select_thread};
use super::thread::{compact_self, own_context_tokens};

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

/// How long a `--next` wait of a T3-owned Claude session runs with no event
///
/// T3 sessions cache their prompt for an hour. A wake-up inside that hour reads
/// a warm cache, while one after it rewrites the whole context, about $0.90 of
/// Opus input at 150K tokens, and starting the wait again costs about $0.13
pub(super) const CACHE_LIMIT: Duration = Duration::from_secs(55 * 60);

/// How long a `--long` wait runs with no event, under Claude Code's 2-hour
/// Bash timeout so the wait exits on its own first
///
/// Past about 4 hours of work, compacting once costs less than starting the
/// wait again every 55 minutes, and each later wake-up rebuilds only the
/// compacted context
pub(super) const LONG_LIMIT: Duration = Duration::from_secs(115 * 60);

// below this a cold wake-up costs little, and compacting a context that was
// already compacted mostly loses detail, so a `--long` wait started again
// after its limit does not compact the summary over and over
const LONG_COMPACT_ABOVE: u64 = 60_000;

/// Arguments of `homebased wait`
#[derive(Debug, Args)]
pub struct WaitArgs {
    /// Full task, GPU queue job, or thread watch UUIDs, 1 to 16
    #[arg(value_name = "ID", required = true)]
    ids: Vec<Uuid>,
    /// Thread that submitted the tasks and jobs or made the watches. Defaults
    /// to the session environment.
    #[arg(long)]
    thread: Option<ThreadId>,
    /// Exit 0 once the next events are printed, for a background command that
    /// reports only when it exits
    #[arg(long)]
    next: bool,
    /// With --next, exit only on an event that ends a target, or any watch
    /// event. Interim events are still printed and confirmed as they arrive
    #[arg(long, requires = "next")]
    final_only: bool,
    /// Exit 0 after this long with no event that wakes it, such as 30m. A
    /// Claude session that a T3 thread owns defaults to 55m, inside its
    /// hour-long prompt cache; elsewhere the wait has no limit
    #[arg(long, value_parser = humantime::parse_duration)]
    timeout: Option<Duration>,
    /// For work expected to take over 4 hours in a Claude session that a T3
    /// thread owns: compact this thread once the turn ends, then wait up to
    /// 115m instead of 55m
    #[arg(long, conflicts_with = "timeout")]
    long: bool,
}

/// When a `--next` wait exits
#[derive(Debug, Clone, Copy)]
enum Wake {
    /// After any event
    Any,
    /// After an event that ends a target, or any watch event
    FinalOnly,
}

impl Wake {
    fn by(self, frame: &WaitFrame) -> bool {
        match self {
            // connection frames carry no event for the agent to read
            Self::Any => matches!(
                frame,
                WaitFrame::Event { .. } | WaitFrame::JobEvent { .. } | WaitFrame::WatchEvent { .. }
            ),
            Self::FinalOnly => wakes_final_only(frame),
        }
    }
}

/// What the wait follows
enum Targets {
    /// Ids the daemon has not yet said are tasks, jobs, or watches
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
        let keys = resolved
            .ok_or_else(|| internal("the daemon did not say what each wait id names".into()))?;
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

impl Targets {
    /// Ids still to wait on, which is what a new wait names
    fn pending(&self) -> Vec<Uuid> {
        match self {
            Self::Named(ids) => ids.ids().to_vec(),
            Self::Resolved(set) => set.pending().iter().map(|key| key.uuid()).collect(),
        }
    }
}

/// How one connection to the daemon ended
enum StreamEnd {
    /// Every target reached its final outcome
    Done,
    /// The wait reached its limit with no event that wakes it
    Limit,
    /// The daemon is restarting
    Restart,
    /// The connection broke or went quiet
    Lost(String),
}

/// Run `homebased wait`
pub(super) async fn wait(ctx: &Ctx, args: WaitArgs) -> Result<ExitCode, AppError> {
    let ids = WaitIds::new(args.ids.clone()).map_err(|error| AppError::Usage {
        message: error.to_string(),
    })?;
    let thread = select_thread(args.thread, crate::domain::session_thread()?)?;
    let client = Client::new(ctx.home.sock_path());
    let limit = limit(&client, &args, thread).await?;
    let deadline = limit.map(|limit| Instant::now() + limit);
    let mut targets = Targets::Named(ids);
    let next = args.next.then_some(if args.final_only {
        Wake::FinalOnly
    } else {
        Wake::Any
    });
    let mut outage: Option<Instant> = None;
    let mut backoff = BACKOFF_START;
    loop {
        let started = Instant::now();
        let mut seen = Seen::default();
        let end = match open(&client, &targets.request(thread)).await {
            Ok(mut stream) => {
                follow(
                    &client,
                    &mut stream,
                    &mut targets,
                    &mut seen,
                    next,
                    deadline,
                )
                .await?
            }
            Err(error) if is_outage(&error) => StreamEnd::Lost(error.to_string()),
            Err(error) => return Err(error),
        };
        let printed = seen.printed;
        // the agent has events to read, and reconnecting would only delay them
        if next.is_some() && seen.woke {
            return Ok(ExitCode::SUCCESS);
        }
        let reason = match end {
            StreamEnd::Done => return Ok(ExitCode::SUCCESS),
            StreamEnd::Limit => return Ok(reached_limit(&args, &targets, limit)),
            StreamEnd::Restart => "the daemon is restarting".to_owned(),
            StreamEnd::Lost(reason) => reason,
        };
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(reached_limit(&args, &targets, limit));
        }

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

/// How long the wait may run with no event that wakes it, `None` for no limit
///
/// A `--long` wait queues the compaction first. When that fails, the wait
/// keeps the 55-minute limit, since a cold wake-up of the full context would
/// cost more than starting the wait again
async fn limit(
    client: &Client,
    args: &WaitArgs,
    thread: ThreadId,
) -> Result<Option<Duration>, AppError> {
    if let Some(timeout) = args.timeout {
        return Ok(Some(timeout));
    }
    if !is_t3_claude_caller(thread) {
        if args.long {
            return Err(AppError::Usage {
                message: "--long is for a Claude session that a T3 thread owns".into(),
            });
        }
        return Ok(None);
    }
    if !args.long {
        return Ok(Some(CACHE_LIMIT));
    }
    let compacting = compact_for_long_wait(client).await;
    Ok(Some(if compacting { LONG_LIMIT } else { CACHE_LIMIT }))
}

/// Compact the calling thread once its turn ends, and say on stderr what
/// happened; whether the long limit is safe to use
async fn compact_for_long_wait(client: &Client) -> bool {
    let tokens = own_context_tokens(client).await.ok().flatten();
    if let Some(tokens) = tokens.filter(|tokens| *tokens < LONG_COMPACT_ABOVE) {
        eprintln!("homebased wait: not compacting, the context is only {tokens} tokens");
        return true;
    }

    let failed = |detail: &str| {
        let minutes = CACHE_LIMIT.as_secs() / 60;
        eprintln!(
            "homebased wait: could not compact this thread ({detail}), so waiting at most {minutes} minutes"
        );
        false
    };
    match compact_self(client).await {
        Ok(response) => match response.outcome {
            CompactOutcome::Started | CompactOutcome::Queued => {
                eprintln!(
                    "homebased wait: compaction {}, this thread compacts once the turn ends",
                    response.outcome.as_str()
                );
                true
            }
            outcome => failed(response.detail.as_deref().unwrap_or(outcome.as_str())),
        },
        Err(error) => failed(&error.to_string()),
    }
}

/// End a wait that reached its limit, telling the agent on stderr how to wait again
///
/// Only event lines go to stdout, and this is not one
fn reached_limit(args: &WaitArgs, targets: &Targets, limit: Option<Duration>) -> ExitCode {
    let waited = limit.map_or_else(String::new, |limit| {
        format!(" after {}", humantime::format_duration(limit))
    });
    eprintln!(
        "homebased wait: stopped{waited} with no event that ends the wait; start it again with `{}`",
        again(args, &targets.pending())
    );
    ExitCode::SUCCESS
}

/// The command that starts the same wait again on `pending`
fn again(args: &WaitArgs, pending: &[Uuid]) -> String {
    let mut words = vec!["homebased wait".to_owned()];
    if args.next {
        words.push("--next".into());
    }
    if args.final_only {
        words.push("--final-only".into());
    }
    if args.long {
        words.push("--long".into());
    }
    if let Some(timeout) = args.timeout {
        // whole seconds stay one word, where `1h 30m` would split into two
        words.push(format!("--timeout {}s", timeout.as_secs()));
    }
    if let Some(thread) = args.thread {
        words.push(format!("--thread {thread}"));
    }
    words.extend(pending.iter().map(Uuid::to_string));
    words.join(" ")
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

/// What one connection printed
#[derive(Debug, Default)]
struct Seen {
    /// Any event line
    printed: bool,
    /// An event that ends a `--next` wait
    woke: bool,
}

/// Print each event line as it arrives until the stream ends
///
/// Only event lines reach stdout, because every line wakes the agent that
/// armed the monitor. Each line is confirmed to the daemon once it is flushed,
/// which is what lets the daemon count the event as delivered. With `next`,
/// the stream ends shortly after the first event that wakes it, and at
/// `deadline` when none has
async fn follow(
    client: &Client,
    stream: &mut LineStream,
    targets: &mut Targets,
    seen: &mut Seen,
    next: Option<Wake>,
    deadline: Option<Instant>,
) -> Result<StreamEnd, AppError> {
    let mut waiter = None;
    let mut gather_until: Option<Instant> = None;
    loop {
        let now = Instant::now();
        let idle = gather_until.map_or(IDLE_LIMIT, |until| until.saturating_duration_since(now));
        let limit = deadline.map_or(idle, |deadline| {
            idle.min(deadline.saturating_duration_since(now))
        });
        let line = match tokio::time::timeout(limit, stream.next_line()).await {
            Err(_) if gather_until.is_some() => return Ok(StreamEnd::Done),
            Err(_) if deadline.is_some_and(|deadline| Instant::now() >= deadline) => {
                return Ok(StreamEnd::Limit);
            }
            Err(_) => return Ok(StreamEnd::Lost("the daemon went quiet".into())),
            Ok(Err(error)) if is_outage(&error) => {
                return Ok(StreamEnd::Lost(error.to_string()));
            }
            Ok(Err(error)) => return Err(error),
            Ok(Ok(None)) => return Ok(StreamEnd::Lost("the daemon closed the stream".into())),
            Ok(Ok(Some(line))) => line,
        };
        let frame: WaitFrame =
            serde_json::from_slice(&line).map_err(|error| AppError::Internal {
                message: format!("invalid wait frame: {error}"),
            })?;
        let wakes = next.is_some_and(|wake| wake.by(&frame));
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
                seen.printed = true;
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
                seen.printed = true;
                if let Some(waiter) = waiter {
                    confirm(client, waiter, WaitKey::Job(job), seq).await;
                }
                set.apply_job(job, kind);
            }
            WaitFrame::WatchEvent {
                watch,
                seq,
                kind,
                line,
            } => {
                let set = targets.set()?;
                print_line(&line)?;
                seen.printed = true;
                if let Some(waiter) = waiter {
                    confirm(client, waiter, WaitKey::Watch(watch), seq).await;
                }
                set.apply_watch(watch, line_ends_watch(kind, &line));
            }
        }
        seen.woke |= wakes;
        if matches!(targets, Targets::Resolved(set) if set.is_done()) {
            return Ok(StreamEnd::Done);
        }
        if seen.woke && gather_until.is_none() {
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
/// normally, which agents already dedupe by `(task, seq)`, `(job, seq)`, or
/// `(watch, seq)`
async fn confirm(client: &Client, waiter: WaiterId, key: WaitKey, seq: u64) {
    let ack = WaitAck::new(waiter, key, seq);
    let Ok(body) = serde_json::to_value(&ack) else {
        return;
    };
    let _ = tokio::time::timeout(ACK_TIMEOUT, client.post(WAIT_ACK_ROUTE, &body)).await;
}
