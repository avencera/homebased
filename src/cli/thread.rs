//! `homebased thread`: context size, compaction, and watches of agent threads
//! anywhere in the Fleet

use std::process::ExitCode;

use clap::Subcommand;

use crate::client::Client;
use crate::domain::{API_VERSION, TaskEnv, TaskId, ThreadId};
use crate::error::AppError;
use crate::thread_ref::{
    Caller, ThreadContext, ThreadContextRequest, ThreadContextResponse, ThreadSelector,
};
use crate::thread_watch::{
    CompactRequest, CompactResponse, Threshold, WatchAction, WatchId, WatchRequest, WatchesResponse,
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
        /// List the calling thread's T3 children too, finished ones included,
        /// to rebuild a list of delegates
        #[arg(long)]
        children: bool,
        /// With --children, leave out settled and archived children
        #[arg(long, requires = "children")]
        live: bool,
        /// Look only on this machine name or UUID
        #[arg(long)]
        machine: Option<String>,
        /// The origin thread of this task
        #[arg(long, conflicts_with_all = ["refs", "worker"])]
        task: Option<TaskId>,
        /// The worker session of this task
        #[arg(long, conflicts_with_all = ["refs", "task"])]
        worker: Option<TaskId>,
    },
    /// Get one event when a thread's context passes a limit while it is idle
    ///
    /// Sends `THREAD_CONTEXT_OVER` once per climb over the limit, and again
    /// only after the context drops below it. The calling thread receives the
    /// events like task callbacks; `homebased wait --next <watch id>` takes
    /// them instead while it runs. The same watch again (thread, limit, and
    /// action) is the existing watch; another limit or action on the same
    /// thread is a second watch.
    #[command(after_help = REFERENCE_HELP)]
    Watch {
        /// Thread references; the calling thread when none is given
        #[arg(value_name = "REF")]
        refs: Vec<String>,
        /// Limit as tokens (150000, 150k, 1.2m) or a percent of the context
        /// window (80%, Codex only)
        #[arg(long, value_name = "LIMIT")]
        context_above: Threshold,
        /// What to do when the watch fires
        #[arg(long, value_enum, default_value_t = WatchAction::Notify)]
        action: WatchAction,
        /// End the watch after it fires once
        #[arg(long)]
        once: bool,
        /// Watch the calling thread's live T3 children too
        #[arg(long)]
        children: bool,
        /// Look only on this machine name or UUID
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
    /// End a watch; it sends `THREAD_WATCH_ENDED` as its last event
    Unwatch {
        /// Watch id that `thread watch` printed
        id: WatchId,
    },
    /// Ask T3 to compact a T3 thread on whichever machine runs it
    ///
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
            let request = ThreadContextRequest {
                api_version: API_VERSION,
                threads,
                children_of: children.then_some(caller),
                live,
                machine,
                prefixes: true,
            };
            let response: ThreadContextResponse =
                client.post_json("/v1/threads/context", &request).await?;
            print_context(ctx, &response)
        }
        ThreadCommand::Watch {
            refs,
            context_above,
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
                threshold: context_above,
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
            };
            let response: CompactResponse =
                client.post_json("/v1/threads/compact", &request).await?;
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
