//! Agent workers that park on other tasks and continue once those tasks end
//!
//! A worker that needs a long command submits it as its own task, reports
//! `waiting` on it with notes for later, and exits. Every run task stays
//! immutable once terminal; a chain is the logical owner of the work across
//! its runs. The daemon holds a continuation run until every waited task ends
//! with any outcome, then launches it with the worker's notes

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::dependency::TaskOutcome;
use crate::domain::{TASK_NAME_MAX_CHARS, TaskId, TaskName};

/// Most tasks one waiting report may name
pub const MAX_WAIT_TARGETS: usize = 16;

/// Largest notes text a waiting report may carry
pub const NOTES_MAX_BYTES: usize = 16 * 1024;

/// Most runs one chain may have; a worker on the last run must report `blocked`
pub const MAX_CHAIN_RUNS: u32 = 20;

/// Suffix of a continuation's name
const CONTINUED_SUFFIX: &str = " (continued)";

/// Tasks a waiting report names: 1 to [`MAX_WAIT_TARGETS`], without
/// duplicates, in the order the worker gave them
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<TaskId>", into = "Vec<TaskId>")]
pub struct WaitTargets(Vec<TaskId>);

/// Why a list of wait targets is invalid
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WaitTargetsError {
    /// No target was named
    #[error("a waiting report must name at least one task with --on")]
    Empty,
    /// The same task appears twice
    #[error("--on names task {task} more than once")]
    Duplicate {
        /// Repeated task
        task: TaskId,
    },
    /// More than [`MAX_WAIT_TARGETS`] tasks
    #[error("--on names {count} tasks; the limit is {MAX_WAIT_TARGETS}")]
    TooMany {
        /// Number of named tasks
        count: usize,
    },
}

impl WaitTargets {
    /// Validate the tasks a waiting report names
    ///
    /// # Errors
    ///
    /// Refuses an empty list, a repeated task, and more than [`MAX_WAIT_TARGETS`] tasks
    pub fn new(tasks: Vec<TaskId>) -> Result<Self, WaitTargetsError> {
        if tasks.is_empty() {
            return Err(WaitTargetsError::Empty);
        }
        if tasks.len() > MAX_WAIT_TARGETS {
            return Err(WaitTargetsError::TooMany { count: tasks.len() });
        }
        for (index, task) in tasks.iter().enumerate() {
            if tasks[..index].contains(task) {
                return Err(WaitTargetsError::Duplicate { task: *task });
            }
        }
        Ok(Self(tasks))
    }

    /// Targets in report order
    #[must_use]
    pub fn tasks(&self) -> &[TaskId] {
        &self.0
    }
}

impl TryFrom<Vec<TaskId>> for WaitTargets {
    type Error = WaitTargetsError;

    fn try_from(tasks: Vec<TaskId>) -> Result<Self, Self::Error> {
        Self::new(tasks)
    }
}

impl From<WaitTargets> for Vec<TaskId> {
    fn from(targets: WaitTargets) -> Self {
        targets.0
    }
}

/// Notes a waiting worker leaves for its continuation: what each outcome of
/// the waited tasks means and what to do next
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct WaitingNotes(String);

/// Why waiting notes are invalid
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WaitingNotesError {
    /// Empty or only whitespace
    #[error("waiting notes must not be blank")]
    Blank,
    /// Longer than [`NOTES_MAX_BYTES`]
    #[error("waiting notes are {len} bytes; the limit is {NOTES_MAX_BYTES}")]
    TooLong {
        /// Notes length in bytes
        len: usize,
    },
}

impl WaitingNotes {
    /// Validate notes text
    ///
    /// # Errors
    ///
    /// Refuses blank notes and notes over [`NOTES_MAX_BYTES`]
    pub fn new(text: String) -> Result<Self, WaitingNotesError> {
        if text.trim().is_empty() {
            return Err(WaitingNotesError::Blank);
        }
        if text.len() > NOTES_MAX_BYTES {
            return Err(WaitingNotesError::TooLong { len: text.len() });
        }
        Ok(Self(text))
    }

    /// Notes text
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for WaitingNotes {
    type Error = WaitingNotesError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl From<WaitingNotes> for String {
    fn from(notes: WaitingNotes) -> Self {
        notes.0
    }
}

/// Data of a `waiting` report: the tasks the worker waits on and its notes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitingReport {
    /// Tasks that must end before the continuation starts
    pub on: WaitTargets,
    /// Notes the continuation receives
    pub notes: WaitingNotes,
}

/// Where the logical work of a chain stands
///
/// A chain exists once its first run parks. Its runs are immutable task rows;
/// this state names the one that currently owns the work
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChainState {
    /// A launched continuation owns the work
    Running {
        /// Run that is queued or running
        current: TaskId,
    },
    /// The current run parked; its continuation is held until every target ends
    Waiting {
        /// Run that reported waiting and exited
        current: TaskId,
        /// Tasks the continuation waits on
        on: WaitTargets,
        /// Held run that continues the work
        continuation: TaskId,
    },
    /// The last run ended without parking; this is the outcome of every run
    Ended {
        /// Outcome of the last run
        outcome: TaskOutcome,
    },
}

/// One run's place in its chain, for `task show` and the dashboard
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainView {
    /// Chain identity: the task id of its first run
    pub id: TaskId,
    /// 1-based position of this task in the chain
    pub run: u32,
    /// Runs the chain has so far, including a held continuation
    pub runs: u32,
    /// State of the whole chain
    #[serde(flatten)]
    pub state: ChainState,
}

/// What a parked run handed over: the tasks it waits on and its continuation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parking {
    /// Tasks the continuation waits on
    pub on: WaitTargets,
    /// Held run that continues the work
    pub continuation: TaskId,
}

/// Why a waiting report was refused
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WaitingRejection {
    /// Only agent workers can be resumed, and queue runs report through their job
    #[error("only agent tasks outside the GPU queue may report waiting")]
    UnsupportedWorkload,
    /// A target is not a task this daemon knows
    #[error("waiting target {task} is not a task on this machine")]
    UnknownTarget {
        /// Unknown target
        task: TaskId,
    },
    /// A target is a GPU queue run, which can end while its job continues
    #[error("waiting target {task} is a GPU queue run; wait on a task instead")]
    TargetUnsupported {
        /// Queue run named as a target
        task: TaskId,
    },
    /// The worker or a target runs on, or reports to, another machine
    #[error(
        "task {task} runs on or reports to another machine; waiting needs the worker and every target on this machine"
    )]
    Remote {
        /// Task that is not local
        task: TaskId,
    },
    /// Waiting on these targets would make the work wait on itself
    #[error("waiting on {task} would wait on this task itself")]
    Cycle {
        /// Target that leads back to the worker
        task: TaskId,
    },
    /// The chain already has the most runs it may have
    #[error(
        "this work already has {runs} runs, the limit is {MAX_CHAIN_RUNS}; report blocked instead"
    )]
    TooManyContinuations {
        /// Runs the chain has
        runs: u32,
    },
}

impl WaitingRejection {
    /// Stable machine-readable code
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnsupportedWorkload => "waiting_unsupported_workload",
            Self::UnknownTarget { .. } => "unknown_dependency",
            Self::TargetUnsupported { .. } => "waiting_target_unsupported",
            Self::Remote { .. } => "waiting_remote_unsupported",
            Self::Cycle { .. } => "waiting_cycle",
            Self::TooManyContinuations { .. } => "too_many_continuations",
        }
    }

    /// Structured input for the error body
    #[must_use]
    pub fn input(&self) -> Value {
        match self {
            Self::UnsupportedWorkload => json!({}),
            Self::UnknownTarget { task }
            | Self::TargetUnsupported { task }
            | Self::Remote { task }
            | Self::Cycle { task } => json!({ "task": task }),
            Self::TooManyContinuations { runs } => json!({ "runs": runs, "max": MAX_CHAIN_RUNS }),
        }
    }
}

/// Block that follows a continuation's prompt
///
/// It is static text built once when the worker parks, so the stored prompt is
/// exactly what the continuation reads
#[must_use]
pub fn continuation_block(worker: TaskId, waiting: &WaitingReport) -> String {
    let targets = waiting
        .on
        .tasks()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let mut block = format!(
        "--- homebased continuation ---\n\
         You are continuing task {worker}. You reported waiting on {targets}.\n\
         Those tasks have now ended. Read each outcome with:\n"
    );
    for task in waiting.on.tasks() {
        block.push_str(&format!(
            "  homebased --json task show {task}\n  homebased task log {task} --tail 200\n"
        ));
    }
    block.push_str("Your notes from before you stopped:\n");
    block.push_str(waiting.notes.as_str());
    if !waiting.notes.as_str().ends_with('\n') {
        block.push('\n');
    }
    block.push_str("Continue the task from these notes. Do not redo completed work.\n");
    block
}

/// Name of a continuation: the first run's name with ` (continued)`
///
/// The base is shortened when the suffix would pass the name limit
#[must_use]
pub fn continuation_name(first: &TaskName) -> TaskName {
    let room = TASK_NAME_MAX_CHARS - CONTINUED_SUFFIX.chars().count();
    let base: String = first.as_str().chars().take(room).collect();
    TaskName::parse(&format!("{}{CONTINUED_SUFFIX}", base.trim_end()))
        .unwrap_or_else(|_| first.clone())
}

#[cfg(test)]
mod tests {
    use super::{
        ChainState, MAX_WAIT_TARGETS, NOTES_MAX_BYTES, WaitTargets, WaitTargetsError, WaitingNotes,
        WaitingNotesError, WaitingReport, continuation_block, continuation_name,
    };
    use crate::domain::{TaskId, TaskName};

    #[test]
    fn targets_and_notes_refuse_invalid_input() {
        let task = TaskId::new();
        assert_eq!(WaitTargets::new(Vec::new()), Err(WaitTargetsError::Empty));
        assert_eq!(
            WaitTargets::new(vec![task, task]),
            Err(WaitTargetsError::Duplicate { task })
        );
        let many = (0..=MAX_WAIT_TARGETS).map(|_| TaskId::new()).collect();
        assert!(matches!(
            WaitTargets::new(many),
            Err(WaitTargetsError::TooMany { .. })
        ));
        assert!(serde_json::from_value::<WaitTargets>(serde_json::json!([])).is_err());

        assert_eq!(
            WaitingNotes::new(" \n".into()),
            Err(WaitingNotesError::Blank)
        );
        assert!(matches!(
            WaitingNotes::new("x".repeat(NOTES_MAX_BYTES + 1)),
            Err(WaitingNotesError::TooLong { .. })
        ));
    }

    #[test]
    fn continuation_block_names_each_target_and_carries_the_notes() {
        let worker = TaskId::new();
        let (first, second) = (TaskId::new(), TaskId::new());
        let waiting = WaitingReport {
            on: WaitTargets::new(vec![first, second]).unwrap(),
            notes: WaitingNotes::new("if it fails, rerun with --small".into()).unwrap(),
        };
        let block = continuation_block(worker, &waiting);
        assert!(block.starts_with("--- homebased continuation ---\n"));
        assert!(block.contains(&format!("You are continuing task {worker}.")));
        assert!(block.contains(&format!("homebased --json task show {second}")));
        assert!(block.contains(&format!("homebased task log {first} --tail 200")));
        assert!(block.contains("if it fails, rerun with --small\n"));
        assert!(block.ends_with("Do not redo completed work.\n"));
    }

    #[test]
    fn continuation_name_fits_the_name_limit() {
        let short = TaskName::parse("build").unwrap();
        assert_eq!(continuation_name(&short).as_str(), "build (continued)");
        let long = TaskName::parse(&"a".repeat(120)).unwrap();
        let named = continuation_name(&long);
        assert_eq!(named.as_str().chars().count(), 120);
        assert!(named.as_str().ends_with(" (continued)"));
    }

    #[test]
    fn chain_state_is_tagged_by_state() {
        let continuation = TaskId::new();
        let state = ChainState::Waiting {
            current: TaskId::new(),
            on: WaitTargets::new(vec![TaskId::new()]).unwrap(),
            continuation,
        };
        let wire = serde_json::to_value(&state).unwrap();
        assert_eq!(wire["state"], "waiting");
        assert_eq!(wire["continuation"], continuation.to_string());
        assert_eq!(serde_json::from_value::<ChainState>(wire).unwrap(), state);
    }
}
