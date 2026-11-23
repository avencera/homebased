//! `homebased thread`: context size, messages, compaction, watches,
//! interrupts, and answers of agent threads anywhere in the Fleet, and the
//! report a child thread sends its parent or its detach from it

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{ArgGroup, Args, Subcommand};

use crate::child_thread::{
    DetachNotice, ReportStageKind, ThreadDetachRequest, ThreadDetachResponse, ThreadReportRequest,
    ThreadReportResponse,
};
use crate::client::Client;
use crate::domain::{API_VERSION, TaskEnv, TaskId, ThreadId};
use crate::error::AppError;
use crate::thread_control::{
    ControlOutcome, ThreadControl, ThreadControlRequest, ThreadControlResponse,
};
use crate::thread_read::{
    CharLimit, MessageLimit, MessagePage, MessageRole, MessageWindow, ThreadMessage,
    ThreadReadRequest, ThreadReadResponse,
};
use crate::thread_ref::{
    Caller, ThreadContext, ThreadContextRequest, ThreadContextResponse, ThreadSelector,
    central_time,
};
use crate::thread_watch::{
    CompactRequest, CompactResponse, StallAfter, Threshold, WatchAction, WatchCondition, WatchId,
    WatchRequest, WatchesResponse,
};

use super::task::select_thread;
use super::{Ctx, OutputMode};

/// Help on what a thread reference can be, shared by every subcommand
const REFERENCE_HELP: &str = "\
A thread reference is any of:
  [Thread: <title>; ref=thread_<id>]   T3 chip text, copied whole
  thread_<id>                          a T3 thread id
  t3-thread://v2/<environment>/<id>    a T3 link, which also picks the machine
  <id>                                 a bare T3 id, Claude Code session, or Codex thread
With no reference, the command looks up the calling thread itself.";

/// Thread commands
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum ThreadCommand {
    /// Show how full each thread's context is, and what it is doing
    ///
    /// Looks on this machine first, then on every reachable Fleet machine. A
    /// UUID prefix of at least 4 characters is accepted here, and must name one
    /// thread. Times are US Central.
    #[command(after_help = REFERENCE_HELP)]
    Context {
        /// Thread references; the calling thread when none is given
        #[arg(value_name = "REF")]
        refs: Vec<String>,
        /// List the calling thread's T3 subagents, and the child threads it
        /// launched with `t3 launch --child` on any Fleet machine, finished
        /// ones included, to rebuild the list of children; forks are left out.
        /// A T3 subagent whose delegated task ended reads as `finished` while
        /// it is idle
        #[arg(long)]
        children: bool,
        /// With --children, leave out archived children, and settled ones or
        /// those whose delegated task ended, unless a turn runs
        #[arg(long, requires = "children")]
        live: bool,
        /// Look only on this machine name or UUID; with --children, list
        /// only its children
        #[arg(long)]
        machine: Option<String>,
        /// The origin thread of this task
        #[arg(long, conflicts_with_all = ["refs", "worker"])]
        task: Option<TaskId>,
        /// The worker session of this task
        #[arg(long, conflicts_with_all = ["refs", "task"])]
        worker: Option<TaskId>,
    },
    /// Read a T3 thread's messages on whichever machine runs it
    ///
    /// Prints the user and assistant messages T3 shows in the thread, oldest
    /// first, without tool calls; a message to the agent may be the user's,
    /// another agent's, or one homebased sent, and a fork starts with the
    /// history it inherited. Each message starts with its position, the same
    /// one `t3_thread_read` gives, which --before, --after, and --item take. A
    /// `running` message is still growing; read it again with --item. Looks on this
    /// machine first, then on every reachable Fleet machine; a UUID prefix of
    /// at least 4 characters is accepted, and must name one thread. Times are
    /// US Central.
    #[command(after_help = REFERENCE_HELP)]
    Read {
        /// Thread reference; the calling thread when omitted
        #[arg(value_name = "REF")]
        reference: Option<String>,
        #[command(flatten)]
        window: WindowArgs,
        /// Most messages to print, from 1 to 100
        #[arg(long, default_value_t)]
        limit: MessageLimit,
        /// Most characters of each message's text, from 1 to 20000; a longer
        /// text says how to read on with --item and --offset
        #[arg(long, default_value_t)]
        max_chars: CharLimit,
        /// Look only on this machine name or UUID
        #[arg(long)]
        machine: Option<String>,
        /// The origin thread of this task
        #[arg(long, conflicts_with_all = ["reference", "worker"])]
        task: Option<TaskId>,
        /// The worker session of this task
        #[arg(long, conflicts_with_all = ["reference", "task"])]
        worker: Option<TaskId>,
    },
    /// Get one event when a thread's context passes a limit while it is idle,
    /// or when it stalls
    ///
    /// With --context-above, sends `THREAD_CONTEXT_OVER` once per climb over
    /// the limit, and again only after the context drops below it. With
    /// --stalled-after, sends `THREAD_STALLED` once the thread has sat `idle`
    /// that long: no turn running, no homebased task or job owing it events,
    /// no T3 subagent due to report to it, and no child thread owing it a
    /// report. It fires again only after the thread is active and stalls
    /// again. A child thread that has sent its `thread report` never stalls,
    /// and its watch ends once it settles. A stall watch on another top-level
    /// thread cannot tell a stall from a thread that sent its last report, so
    /// unwatch it when that report arrives; a T3 subagent's watch ends on its
    /// own.
    /// The calling thread receives the events like task callbacks; `homebased
    /// wait --next <watch id>` takes them instead while it runs. The same watch
    /// again (thread, condition, and action) is the existing watch; another
    /// condition or action on the same thread is a second watch. A T3 thread
    /// that is settled, or finished with its delegated task, and runs no turn
    /// never fires, even while it shows `waiting`, and its watch ends after an
    /// hour without activity, as do the watches of a calling T3 thread that is
    /// archived, deleted, or done for as long. A started compaction of another
    /// thread, an unwatch, and a finished thread's end wake no caller; only a
    /// running `homebased wait` prints them.
    #[command(after_help = REFERENCE_HELP)]
    Watch {
        /// Thread references; the calling thread when none is given
        #[arg(value_name = "REF")]
        refs: Vec<String>,
        /// Limit as tokens (150000, 150k, 1.2m) or a percent of the context
        /// window (80%, Codex only)
        #[arg(
            long,
            value_name = "LIMIT",
            required_unless_present = "stalled_after",
            conflicts_with = "stalled_after"
        )]
        context_above: Option<Threshold>,
        /// How long the thread must sit idle with nothing due to wake it, as
        /// minutes (20m, 20) or hours (1h, 1h30m), from 5m to 24h
        #[arg(long, value_name = "GRACE")]
        stalled_after: Option<StallAfter>,
        /// What to do when the watch fires; a stall watch only notifies
        #[arg(long, value_enum, default_value_t = WatchAction::Notify)]
        action: WatchAction,
        /// End the watch after it fires once; that event says `watch_ended`,
        /// and no `THREAD_WATCH_ENDED` follows
        #[arg(long)]
        once: bool,
        /// Watch the calling thread's live T3 subagents and launched child
        /// threads too: those not settled, archived, or done with their
        /// delegated task, never forks
        #[arg(long)]
        children: bool,
        /// Look only on this machine name or UUID; with --children, list
        /// only its children
        #[arg(long)]
        machine: Option<String>,
        /// The origin thread of this task
        #[arg(long, conflicts_with_all = ["refs", "worker"])]
        task: Option<TaskId>,
        /// The worker session of this task
        #[arg(long, conflicts_with_all = ["refs", "task"])]
        worker: Option<TaskId>,
        /// Thread that receives the events. Defaults to the session environment.
        #[arg(long)]
        thread: Option<ThreadId>,
    },
    /// List the calling thread's live watches
    Watches {
        /// Every caller's watches, ended ones included
        #[arg(long)]
        all: bool,
        /// Thread whose watches to list. Defaults to the session environment.
        #[arg(long, conflicts_with = "all")]
        thread: Option<ThreadId>,
    },
    /// End a watch; its last event, `THREAD_WATCH_ENDED`, reaches only a
    /// running `homebased wait` and never wakes the caller
    Unwatch {
        /// Watch id that `thread watch` printed
        id: WatchId,
    },
    /// Report back from a child thread, then settle it
    ///
    /// Runs in a thread that `homebased t3 launch --child` started. Refuses
    /// while the thread still has open work: a homebased task or queue job
    /// without its final event, a T3 subagent due to report to it, or a
    /// child thread of its own that has neither reported nor been archived or
    /// settled. Sends the report to the parent thread as a
    /// `HOMEBASED_MESSAGE`, then settles this thread once the turn that ran
    /// the command has ended, unless a later run or message keeps it active.
    /// A retry, or a daemon restart, finishes the same report and never sends
    /// a second one. A report from a later turn, such as one a user message to
    /// the settled thread started, goes as a follow-up report once the one
    /// before it was delivered; other text from the same turn is refused.
    Report {
        /// The report: what was done, what was found, and what is left
        #[arg(long)]
        summary: String,
        /// Append this file's text to the report, or stdin with `-`
        #[arg(long)]
        file: Option<PathBuf>,
        /// Stop each child thread that has not reported, as `thread interrupt
        /// --stop` does, and stop waiting on it, when children are the only
        /// open work left; a child whose machine does not answer still refuses
        #[arg(long)]
        stop_children: bool,
    },
    /// Detach a child thread from the thread that launched it, so it no
    /// longer reports there
    ///
    /// For a child thread whose work outgrew its parent, such as one that
    /// became a long-running coordinator. Afterwards the parent no longer
    /// waits on its report or lists it under `thread context --children`, a
    /// stop of the parent no longer stops it, its questions no longer alert
    /// the parent, and `thread report` refuses in it. A report it already sent
    /// stays delivered, and its own child threads stay linked to it. Unless
    /// the parent runs the command itself, the parent gets a
    /// `HOMEBASED_MESSAGE` saying the child detached; a notice that fails is
    /// reported and leaves the detach in place. Both machines of the link
    /// must answer; after a failure, the same command again finishes the
    /// detach. The reference must name exactly one thread; prefixes are not
    /// accepted.
    #[command(after_help = REFERENCE_HELP)]
    Detach {
        /// Thread reference; the calling thread when omitted
        #[arg(value_name = "REF")]
        reference: Option<String>,
        /// Look only on this machine name or UUID
        #[arg(long)]
        machine: Option<String>,
    },
    /// Interrupt a T3 thread's running turn, or stop the thread, on whichever
    /// machine runs it
    ///
    /// Without --stop, ends the active run as the composer's stop button does,
    /// and answers `idle` when no run is active; queued messages still start.
    /// With --stop, does what the thread's Stop button does: ends the active
    /// run, holds queued messages, and stops the thread's T3 subagents. It
    /// stops the thread's own unreported child threads first, wherever they
    /// run. A stopped child thread that has not reported counts as done, so
    /// its parent stops waiting on it. Homebased tasks and jobs the thread
    /// submitted keep running, and their final events can start it again
    /// unless T3 still holds messages queued at the stop; cancel them with
    /// `homebased cancel`. The reference must name exactly one thread;
    /// prefixes are not accepted.
    #[command(after_help = REFERENCE_HELP)]
    Interrupt {
        /// Thread reference
        #[arg(value_name = "REF", required_unless_present = "task")]
        reference: Option<String>,
        /// Stop the thread instead of interrupting one run
        #[arg(long)]
        stop: bool,
        /// Look only on this machine name or UUID
        #[arg(long)]
        machine: Option<String>,
        /// The origin thread of this task; a worker session is not a T3
        /// thread, so stop a worker with `homebased cancel`
        #[arg(long, conflicts_with = "reference")]
        task: Option<TaskId>,
    },
    /// Answer a question a T3 thread's agent is waiting on, on whichever
    /// machine runs it
    ///
    /// The `HOMEBASED_MESSAGE` that tells a parent its child thread waits
    /// on a question names the request and its questions. Pass one --answer
    /// per question, in order; an answer that names an offered option picks
    /// it. Approvals of commands and file changes are answered in T3 Code.
    #[command(after_help = REFERENCE_HELP)]
    Answer {
        /// Thread reference
        #[arg(value_name = "REF")]
        reference: String,
        /// T3's id of the pending request
        #[arg(long)]
        request: String,
        /// One answer per question, in order
        #[arg(long = "answer", value_name = "TEXT", required = true)]
        answers: Vec<String>,
        /// Look only on this machine name or UUID
        #[arg(long)]
        machine: Option<String>,
    },
    /// Ask T3 to compact a T3 thread on whichever machine runs it
    ///
    /// A thread running a turn, the calling thread included, answers `queued`:
    /// T3 compacts it once that turn ends, and nothing wakes it afterwards.
    /// The reference must name exactly one thread; prefixes are not accepted.
    /// Threads outside T3 cannot be compacted by homebased.
    #[command(after_help = REFERENCE_HELP)]
    Compact {
        /// Thread reference; the calling thread when omitted
        #[arg(value_name = "REF")]
        reference: Option<String>,
        /// Look only on this machine name or UUID
        #[arg(long)]
        machine: Option<String>,
        /// The origin thread of this task
        #[arg(long, conflicts_with_all = ["reference", "worker"])]
        task: Option<TaskId>,
        /// The worker session of this task
        #[arg(long, conflicts_with_all = ["reference", "task"])]
        worker: Option<TaskId>,
    },
}

/// Which messages `thread read` prints; the newest when no flag is given
#[derive(Debug, Args)]
#[command(group(ArgGroup::new("window").args(["before", "after", "oldest", "item"])))]
pub struct WindowArgs {
    /// The newest messages before this position
    #[arg(long, value_name = "POSITION")]
    before: Option<i64>,
    /// The oldest messages after this position
    #[arg(long, value_name = "POSITION")]
    after: Option<i64>,
    /// The first messages of the thread
    #[arg(long)]
    oldest: bool,
    /// Only the message at this position
    #[arg(long, value_name = "POSITION")]
    item: Option<i64>,
    /// With --item, start its text at this character offset
    #[arg(long, requires = "item", default_value_t = 0)]
    offset: u32,
}

impl WindowArgs {
    fn window(&self) -> MessageWindow {
        match (self.before, self.after, self.oldest, self.item) {
            (Some(position), ..) => MessageWindow::Before { position },
            (None, Some(position), ..) => MessageWindow::After { position },
            (None, None, true, _) => MessageWindow::Oldest,
            (None, None, false, Some(position)) => MessageWindow::Item {
                position,
                offset: self.offset,
            },
            (None, None, false, None) => MessageWindow::Newest,
        }
    }
}

/// Run one thread command
pub async fn run(ctx: &Ctx, command: ThreadCommand) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    match command {
        ThreadCommand::Context {
            refs,
            children,
            live,
            machine,
            task,
            worker,
        } => {
            let caller = Caller::from_env()?;
            let threads = if refs.is_empty() && task.is_none() && worker.is_none() && children {
                Vec::new()
            } else {
                selectors(refs, task, worker, &caller)?
            };
            let carried_by = caller.t3_thread.clone();
            let request = ThreadContextRequest {
                api_version: API_VERSION,
                threads,
                children_of: children.then_some(caller),
                live,
                machine,
                prefixes: true,
                carried_by,
            };
            let response: ThreadContextResponse =
                client.post_json("/v1/threads/context", &request).await?;
            print_context(ctx, &response)
        }
        ThreadCommand::Read {
            reference,
            window,
            limit,
            max_chars,
            machine,
            task,
            worker,
        } => {
            let caller = Caller::from_env()?;
            let page = MessagePage {
                window: window.window(),
                limit,
                max_chars,
            };
            let mut threads = selectors(reference.into_iter().collect(), task, worker, &caller)?;
            let request = ThreadReadRequest {
                api_version: API_VERSION,
                thread: threads.remove(0),
                carried_by: caller.t3_thread,
                machine,
                page,
            };
            let response: ThreadReadResponse =
                client.post_json("/v1/threads/read", &request).await?;
            print_read(ctx, &response)
        }
        ThreadCommand::Watch {
            refs,
            context_above,
            stalled_after,
            action,
            once,
            children,
            machine,
            task,
            worker,
            thread,
        } => {
            let caller_ids = Caller::from_env()?;
            let caller = select_thread(thread, caller_ids.session)?;
            let threads = if refs.is_empty() && task.is_none() && worker.is_none() && children {
                Vec::new()
            } else {
                selectors(refs, task, worker, &caller_ids)?
            };
            let request = WatchRequest {
                api_version: API_VERSION,
                caller,
                caller_ids,
                env: TaskEnv::capture(),
                callback_cwd: std::env::current_dir()?,
                threads,
                children,
                machine,
                // clap requires exactly one of the two
                condition: match (context_above, stalled_after) {
                    (Some(threshold), _) => WatchCondition::ContextAbove(threshold),
                    (None, Some(after)) => WatchCondition::StalledAfter(after),
                    (None, None) => {
                        return Err(AppError::Usage {
                            message: "pass --context-above or --stalled-after".into(),
                        });
                    }
                },
                action,
                once,
            };
            let response: WatchesResponse =
                client.post_json("/v1/threads/watches", &request).await?;
            print_watches(ctx, &response, "no T3 children to watch")
        }
        ThreadCommand::Watches { all, thread } => {
            let path = if all {
                "/v1/threads/watches?all=true".to_owned()
            } else {
                let caller = select_thread(thread, crate::domain::session_thread()?)?;
                format!("/v1/threads/watches?caller={caller}")
            };
            let response: WatchesResponse = serde_json::from_value(client.get(&path).await?)?;
            print_watches(ctx, &response, "no watches")
        }
        ThreadCommand::Unwatch { id } => {
            let response: WatchesResponse = client
                .post_json(
                    &format!("/v1/threads/watches/{id}/unwatch"),
                    &serde_json::json!({}),
                )
                .await?;
            print_watches(ctx, &response, "no watch")
        }
        ThreadCommand::Report {
            summary,
            file,
            stop_children,
        } => {
            let body = match file {
                Some(path) => format!("{summary}\n\n{}", read_text(&path)?),
                None => summary,
            };
            let request = ThreadReportRequest {
                api_version: API_VERSION,
                caller: Caller::from_env()?,
                body,
                stop_children,
            };
            let response: ThreadReportResponse =
                client.post_json("/v1/threads/report", &request).await?;
            print_report(ctx, &response)
        }
        ThreadCommand::Detach { reference, machine } => {
            let caller = Caller::from_env()?;
            let mut threads = selectors(reference.into_iter().collect(), None, None, &caller)?;
            let request = ThreadDetachRequest {
                api_version: API_VERSION,
                thread: threads.remove(0),
                carried_by: caller.t3_thread.clone(),
                caller,
                machine,
            };
            let response: ThreadDetachResponse =
                client.post_json("/v1/threads/detach", &request).await?;
            print_detach(ctx, &response)
        }
        ThreadCommand::Interrupt {
            reference,
            stop,
            machine,
            task,
        } => {
            let caller = Caller::from_env()?;
            let mut threads = selectors(reference.into_iter().collect(), task, None, &caller)?;
            let request = ThreadControlRequest {
                api_version: API_VERSION,
                thread: threads.remove(0),
                machine,
                carried_by: caller.t3_thread,
                control: if stop {
                    ThreadControl::Stop
                } else {
                    ThreadControl::Interrupt
                },
            };
            control(ctx, &client, &request).await
        }
        ThreadCommand::Answer {
            reference,
            request,
            answers,
            machine,
        } => {
            let caller = Caller::from_env()?;
            let request = ThreadControlRequest {
                api_version: API_VERSION,
                thread: ThreadSelector::Reference { text: reference },
                machine,
                carried_by: caller.t3_thread,
                control: ThreadControl::Answer { request, answers },
            };
            control(ctx, &client, &request).await
        }
        ThreadCommand::Compact {
            reference,
            machine,
            task,
            worker,
        } => {
            let caller = Caller::from_env()?;
            let mut threads = selectors(reference.into_iter().collect(), task, worker, &caller)?;
            let request = CompactRequest {
                api_version: API_VERSION,
                thread: threads.remove(0),
                machine,
                carried_by: caller.t3_thread,
            };
            let response = compact(&client, &request).await?;
            match ctx.output {
                OutputMode::Json => ctx.print_json(serde_json::to_value(&response)?)?,
                OutputMode::Quiet => println!("{}", response.outcome.as_str()),
                OutputMode::Human => {
                    let detail = response
                        .detail
                        .as_deref()
                        .map(|detail| format!(": {detail}"))
                        .unwrap_or_default();
                    println!(
                        "{} {}{detail}",
                        response.outcome.as_str(),
                        response.thread.human_line()
                    );
                }
            }
            Ok(ExitCode::SUCCESS)
        }
    }
}

/// Send one control request and print what T3 did
async fn control(
    ctx: &Ctx,
    client: &Client,
    request: &ThreadControlRequest,
) -> Result<ExitCode, AppError> {
    let response: ThreadControlResponse = client.post_json("/v1/threads/control", request).await?;
    match ctx.output {
        OutputMode::Json => ctx.print_json(serde_json::to_value(&response)?)?,
        OutputMode::Quiet => println!("{}", response.outcome.as_str()),
        OutputMode::Human => {
            let detail = match &response.outcome {
                ControlOutcome::Interrupted { run } => format!(" (run {run})"),
                ControlOutcome::Answered { request } => format!(" (request {request})"),
                ControlOutcome::Idle | ControlOutcome::Stopped => String::new(),
            };
            let closed = if response.closed_link.is_some() {
                "; its parent no longer waits on its report"
            } else {
                ""
            };
            println!(
                "{}{detail} {}{closed}",
                response.outcome.as_str(),
                response.thread.human_line()
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn compact(client: &Client, request: &CompactRequest) -> Result<CompactResponse, AppError> {
    client.post_json("/v1/threads/compact", request).await
}

/// The threads a command names: `--task`, `--worker`, the references, or the caller
fn selectors(
    refs: Vec<String>,
    task: Option<TaskId>,
    worker: Option<TaskId>,
    caller: &Caller,
) -> Result<Vec<ThreadSelector>, AppError> {
    if let Some(task) = task {
        return Ok(vec![ThreadSelector::Task { task }]);
    }
    if let Some(task) = worker {
        return Ok(vec![ThreadSelector::Worker { task }]);
    }
    if refs.is_empty() {
        let own = caller.selector().ok_or_else(|| AppError::Usage {
            message: "name a thread, or run this from an agent session that sets T3CODE_THREAD_ID, CODEX_THREAD_ID, or CLAUDE_CODE_SESSION_ID".into(),
        })?;
        return Ok(vec![own]);
    }
    Ok(refs
        .into_iter()
        .map(|text| ThreadSelector::Reference { text })
        .collect())
}

fn print_context(ctx: &Ctx, response: &ThreadContextResponse) -> Result<ExitCode, AppError> {
    // JSON carries `children_complete` and `unanswered` instead
    if response.children_complete == Some(false) && !matches!(ctx.output, OutputMode::Json) {
        eprintln!(
            "warning: the children list may be incomplete; no answer from {}",
            response.unanswered.join(", ")
        );
    }
    match ctx.output {
        OutputMode::Json => ctx.print_json(serde_json::to_value(response)?)?,
        OutputMode::Quiet => {
            for thread in response.threads.iter().chain(&response.children) {
                println!("{}", thread.exact_reference());
            }
        }
        OutputMode::Human => {
            for thread in &response.threads {
                println!("{}", thread.human_line());
            }
            if !response.children.is_empty() {
                println!("children:");
                for child in &response.children {
                    println!("  {}", child_line(child));
                }
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn print_read(ctx: &Ctx, response: &ThreadReadResponse) -> Result<ExitCode, AppError> {
    let messages = &response.page.messages;
    match ctx.output {
        OutputMode::Json => ctx.print_json(serde_json::to_value(response)?)?,
        OutputMode::Quiet => {
            for message in messages {
                println!("{}", message.position);
            }
        }
        OutputMode::Human => {
            println!("{}", response.thread.human_line());
            if messages.is_empty() {
                println!("\nno messages");
            }
            for message in messages {
                println!("\n{}\n{}", message_header(message), message.text);
                if let Some(offset) = message.next_offset {
                    println!(
                        "[cut at {offset} of {} characters; read on with --item {} --offset {offset}]",
                        message.chars, message.position
                    );
                }
            }
            let reference = response.thread.exact_reference();
            if response.page.earlier.is_some() || response.page.later.is_some() {
                println!();
            }
            if let Some(position) = response.page.earlier {
                println!("older: homebased thread read {reference} --before {position}");
            }
            if let Some(position) = response.page.later {
                println!("newer: homebased thread read {reference} --after {position}");
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// `#5000030 assistant, 2026-11-23 11:17 CST`, naming who sent a message to
/// the agent and how
fn message_header(message: &ThreadMessage) -> String {
    let from = match (message.role, message.created_by.as_deref()) {
        (MessageRole::Assistant, _) => "assistant".to_owned(),
        (MessageRole::User, Some("agent")) => "agent".to_owned(),
        (MessageRole::User, _) => "user".to_owned(),
    };
    let inherited = message
        .inherited_from
        .as_deref()
        .map(|thread| format!(" (inherited from thread_{thread})"))
        .unwrap_or_default();
    let sender = message
        .sender_thread
        .as_deref()
        .map(|thread| format!(" from thread_{thread}"))
        .unwrap_or_default();
    let source = message
        .source
        .as_deref()
        .map(|source| format!(" via {source}"))
        .unwrap_or_default();
    let at = message
        .at
        .map(|at| format!(", {}", central_time(at)))
        .unwrap_or_default();
    let status = if message.status == "completed" {
        String::new()
    } else {
        format!(", {}", message.status)
    };
    format!(
        "#{} {from}{sender}{source}{at}{status}{inherited}",
        message.position
    )
}

fn child_line(child: &ThreadContext) -> String {
    let provider = child
        .provider
        .map(|provider| match provider {
            crate::thread_ref::Provider::Claude => "claude ",
            crate::thread_ref::Provider::Codex => "codex ",
        })
        .unwrap_or_default();
    format!("{provider}{}", child.human_line())
}

/// The text of `path`, or stdin for `-`
fn read_text(path: &Path) -> Result<String, AppError> {
    if path.as_os_str() == "-" {
        let mut text = String::new();
        std::io::stdin().read_to_string(&mut text)?;
        return Ok(text);
    }
    std::fs::read_to_string(path).map_err(|error| AppError::FileNotFound {
        message: format!("read report file {}: {error}", path.display()),
    })
}

fn print_report(ctx: &Ctx, response: &ThreadReportResponse) -> Result<ExitCode, AppError> {
    let report = &response.report;
    match ctx.output {
        OutputMode::Json => ctx.print_json(serde_json::to_value(response)?)?,
        OutputMode::Quiet => println!("{}", report.message_id),
        OutputMode::Human => {
            let verb = match (response.resumed, report.seq.is_follow_up()) {
                (true, false) => "already reported",
                (true, true) => "already sent the follow-up report",
                (false, false) => "reported",
                (false, true) => "sent a follow-up report",
            };
            let next = match report.stage {
                ReportStageKind::Sending => "the send is unfinished; homebased retries it",
                ReportStageKind::SettlePending => "this thread settles once this turn ends",
                ReportStageKind::Sent => "this thread stays active",
                ReportStageKind::Settled => "this thread is settled",
            };
            for child in &response.stopped {
                println!(
                    "stopped child thread thread_{} on machine {}",
                    child.t3_thread, child.machine
                );
            }
            println!(
                "{verb} to thread_{} on machine {} as message {}; {next}",
                report.parent.t3_thread, report.parent.machine, report.message_id
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn print_detach(ctx: &Ctx, response: &ThreadDetachResponse) -> Result<ExitCode, AppError> {
    let detached = &response.detached;
    match ctx.output {
        OutputMode::Json => ctx.print_json(serde_json::to_value(response)?)?,
        OutputMode::Quiet => println!("{}", detached.link),
        OutputMode::Human => {
            let parent = format!(
                "thread_{} on machine {}",
                detached.parent.t3_thread, detached.parent.machine
            );
            println!("detached thread_{} from {parent}", detached.child.t3_thread);
            match &detached.notice {
                DetachNotice::Sent { message_id } => {
                    println!("told the parent as message {message_id}");
                }
                DetachNotice::Skipped => println!("the parent detached it, so it was not told"),
                DetachNotice::Failed { message } => eprintln!(
                    "warning: the parent was not told ({message}); tell it with `homebased message send`"
                ),
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn print_watches(ctx: &Ctx, response: &WatchesResponse, empty: &str) -> Result<ExitCode, AppError> {
    match ctx.output {
        OutputMode::Json => ctx.print_json(serde_json::to_value(response)?)?,
        OutputMode::Quiet => {
            for watch in &response.watches {
                println!("{}", watch.watch);
            }
        }
        OutputMode::Human => {
            if response.watches.is_empty() {
                println!("{empty}");
            }
            for watch in &response.watches {
                println!("{}", watch.human_line());
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}
