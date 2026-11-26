//! Pointers from the commands agents guess, such as `status`, `ls`, and
//! `jobs`, to the commands that show what they were after
//!
//! Each kind of work (tasks, GPU queue jobs, CI, services, threads) has its
//! own `list` and `show`. `status` and `ls` summarize them and end with these
//! pointers for the full views, and other top-level words agents guess get
//! the pointers after clap's usage error

use std::ffi::OsString;
use std::io::{self, Write};

use serde::Serialize;

/// A real command to run, and what it shows
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Pointer {
    /// Full command line, with `<placeholders>` for ids
    pub command: &'static str,
    /// What the command shows or does
    pub shows: &'static str,
}

const fn to(command: &'static str, shows: &'static str) -> Pointer {
    Pointer { command, shows }
}

const QUEUE: Pointer = to(
    "homebased queue",
    "the GPU queue in serving order, as `resource jobs`",
);
const RESOURCES: Pointer = to("homebased resource list", "GPUs and the runs holding them");
const TASK_LIST: Pointer = to("homebased task list", "tasks on this machine");
const TASK_SHOW: Pointer = to("homebased task show <task>", "one task's state and exit");
const TASK_LOG: Pointer = to("homebased task log <task>", "one task's output");
const JOB_SHOW: Pointer = to(
    "homebased resource job show <job>",
    "one GPU queue or CI job, its progress, and its run tasks",
);
const CI_SLOTS: Pointer = to("homebased ci slots", "CI slots and load on each machine");
const SERVICE_LIST: Pointer = to("homebased service list", "this thread's services");
const SERVICE_LOG: Pointer = to("homebased service log <service>", "one service's output");
const THREAD_CONTEXT: Pointer = to(
    "homebased thread context",
    "how full each thread's context is, and what it is doing",
);
const THREAD_READ: Pointer = to("homebased thread read <thread>", "a T3 thread's messages");
const FLEET: Pointer = to(
    "homebased fleet machines",
    "Fleet machines and whether they answer",
);
const DAEMON: Pointer = to(
    "homebased daemon status",
    "whether the daemon is up, and how many tasks are in flight",
);

const STATUS: &[Pointer] = &[
    TASK_SHOW,
    JOB_SHOW,
    QUEUE,
    CI_SLOTS,
    THREAD_CONTEXT,
    FLEET,
    DAEMON,
];

const LIST: &[Pointer] = &[
    TASK_LIST,
    QUEUE,
    RESOURCES,
    CI_SLOTS,
    SERVICE_LIST,
    to("homebased thread watches", "this thread's live watches"),
    to("homebased pool list", "pools of temporary machines"),
    to("homebased t3 projects", "T3 Code projects"),
    FLEET,
];

/// Guessed top-level words that are not commands, and where each points
const GUESSES: &[(&str, &[Pointer])] = &[
    ("tasks", &[TASK_LIST, TASK_SHOW]),
    ("jobs", &[QUEUE, JOB_SHOW]),
    ("gpu", &[QUEUE, RESOURCES]),
    ("gpus", &[QUEUE, RESOURCES]),
    ("threads", &[THREAD_CONTEXT, THREAD_READ]),
    ("read", &[THREAD_READ, TASK_LOG]),
    ("services", &[SERVICE_LIST]),
    ("machines", &[FLEET]),
    ("log", &[TASK_LOG, SERVICE_LOG]),
    ("logs", &[TASK_LOG, SERVICE_LOG]),
    ("show", &[TASK_SHOW, JOB_SHOW]),
    ("info", &[TASK_SHOW, JOB_SHOW]),
    (
        "cancel",
        &[
            to("homebased task cancel <task>", "cancel a task"),
            to(
                "homebased resource job cancel <job>",
                "cancel a GPU queue or CI job",
            ),
            to("homebased service stop <service>", "stop a service"),
        ],
    ),
    (
        "run",
        &[
            to(
                "homebased task run -- <command...>",
                "run a command as a task",
            ),
            to(
                "homebased resource job submit --spec <file>",
                "queue a GPU job",
            ),
            to(
                "homebased ci run --class <class> -- <command...>",
                "queue a build or test on a CI slot",
            ),
        ],
    ),
];

/// A summary command, whose output ends with the full views behind it
#[derive(Debug, Clone, Copy)]
pub enum Guide {
    /// `status`: the state of each kind of work
    Status,
    /// `ls`: each active item
    List,
}

impl Guide {
    /// The commands that show this summary's parts in full
    pub(super) fn pointers(self) -> &'static [Pointer] {
        match self {
            Self::Status => STATUS,
            Self::List => LIST,
        }
    }
}

/// Write pointers as aligned `command  what it shows` lines
pub(super) fn print_pointers(out: &mut impl Write, pointers: &[Pointer]) -> io::Result<()> {
    let width = pointers.iter().map(|p| p.command.len()).max().unwrap_or(0);
    for Pointer { command, shows } in pointers {
        writeln!(out, "  {command:<width$}  {shows}")?;
    }

    Ok(())
}

/// After clap rejects a top-level word agents often guess, write the commands
/// to run instead to stderr
///
/// Nested words are left alone: `thread logs` is not a guess at `task log`
pub fn hint_unknown_command(err: &clap::Error, args: impl IntoIterator<Item = OsString>) {
    let Some(pointers) = unknown_command_hint(err, args) else {
        return;
    };

    let mut stderr = io::stderr();
    // stderr is already the error channel; there is nowhere left to report
    let _ = writeln!(stderr, "\nRun one of these instead:");
    let _ = print_pointers(&mut stderr, pointers);
}

fn unknown_command_hint(
    err: &clap::Error,
    args: impl IntoIterator<Item = OsString>,
) -> Option<&'static [Pointer]> {
    use clap::error::{ContextKind, ContextValue, ErrorKind};

    if err.kind() != ErrorKind::InvalidSubcommand {
        return None;
    }

    let Some(ContextValue::String(word)) = err.get(ContextKind::InvalidSubcommand) else {
        return None;
    };

    if first_command_word(args)?.to_str()? != word {
        return None;
    }

    GUESSES
        .iter()
        .find(|(guess, _)| guess == word)
        .map(|(_, pointers)| *pointers)
}

/// The first argument that is not a global flag or a global flag's value
fn first_command_word(args: impl IntoIterator<Item = OsString>) -> Option<OsString> {
    let mut args = args.into_iter().skip(1);
    while let Some(arg) = args.next() {
        match arg.to_str() {
            // the global flags that take a separate value
            Some("--home" | "--config") => {
                args.next();
            }
            Some(flag) if flag.starts_with('-') => {}
            _ => return Some(arg),
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{GUESSES, Guide, Pointer, unknown_command_hint};
    use crate::cli::Cli;
    use clap::{CommandFactory, Parser};
    use std::ffi::OsString;

    fn all_pointers() -> impl Iterator<Item = &'static Pointer> {
        [Guide::Status, Guide::List]
            .into_iter()
            .flat_map(Guide::pointers)
            .chain(GUESSES.iter().flat_map(|(_, pointers)| pointers.iter()))
    }

    /// Every pointer names a command path that exists, so renaming a command
    /// fails here instead of sending agents to a dead end
    #[test]
    fn every_pointer_names_a_real_command() {
        for pointer in all_pointers() {
            let mut words = pointer.command.split_whitespace();
            assert_eq!(words.next(), Some("homebased"), "{}", pointer.command);
            let mut command = Cli::command();
            for word in words.take_while(|w| !w.starts_with(['<', '-'])) {
                command = command
                    .find_subcommand(word)
                    .unwrap_or_else(|| panic!("`{}` has no `{word}`", pointer.command))
                    .clone();
            }

            assert!(
                !command.has_subcommands(),
                "`{}` stops at a command group",
                pointer.command
            );
        }
    }

    /// A guess that became a real command would never reach the hint
    #[test]
    fn guesses_are_not_commands() {
        let cli = Cli::command();
        for (guess, _) in GUESSES {
            assert!(
                cli.find_subcommand(guess).is_none(),
                "`{guess}` is a command"
            );
        }
    }

    fn hint(args: &[&str]) -> Option<Vec<&'static str>> {
        let argv: Vec<OsString> = args.iter().map(OsString::from).collect();
        let err = Cli::try_parse_from(&argv).expect_err("guess should not parse");
        unknown_command_hint(&err, argv).map(|p| p.iter().map(|p| p.command).collect())
    }

    #[test]
    fn top_level_guess_points_to_real_commands() {
        let commands = hint(&["homebased", "--home", "/tmp/hb", "jobs", "--json"]).unwrap();
        assert_eq!(commands[0], "homebased queue");
    }

    #[test]
    fn nested_and_unknown_words_get_no_hint() {
        assert_eq!(hint(&["homebased", "thread", "logs"]), None);
        assert_eq!(hint(&["homebased", "frobnicate"]), None);
    }
}
