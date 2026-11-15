//! Agent workers that park on other tasks or GPU queue jobs and continue
//! once those end
//!
//! A worker that needs a long command submits it as its own task or GPU queue
//! job, reports `waiting` on it with notes for later, and exits. Every run
//! task stays immutable once terminal; a chain is the logical owner of the
//! work across its runs. The daemon holds a continuation run until every
//! waited task ended and every waited job sent its final event, with any
//! outcome, then launches it with the worker's notes

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::dependency::{Dependency, DependencyId, TaskOutcome};
use crate::domain::{TASK_NAME_MAX_CHARS, TaskId, TaskName};
use crate::queue::JobId;

/// Most tasks and jobs one waiting report may name
pub const MAX_WAIT_TARGETS: usize = 16;

/// Largest notes text a waiting report may carry
pub const NOTES_MAX_BYTES: usize = 16 * 1024;

/// Most runs one chain may have; a worker on the last run must report `blocked`
pub const MAX_CHAIN_RUNS: u32 = 20;

/// Suffix of a continuation's name
const CONTINUED_SUFFIX: &str = " (continued)";

/// Tasks and jobs a waiting report names: 1 to [`MAX_WAIT_TARGETS`], without
/// duplicates, in the order the worker gave them
///
/// Each target is a [`Dependency`], the same task-or-job model as `after`.
/// The wire form tags each one, as `{"task": id}` or `{"job": id}`. A bare
/// UUID also reads as a task, the form saved before jobs could be named
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<SavedTarget>", into = "Vec<Dependency>")]
pub struct WaitTargets(Vec<Dependency>);

/// One saved target: tagged, or a bare task UUID from before job targets
#[derive(Deserialize)]
#[serde(untagged)]
enum SavedTarget {
    Task(TaskId),
    Tagged(Dependency),
}

impl From<SavedTarget> for Dependency {
    fn from(saved: SavedTarget) -> Self {
        match saved {
            SavedTarget::Task(task) => Self::Task(task),
            SavedTarget::Tagged(target) => target,
        }
    }
}

/// Why a list of wait targets is invalid
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WaitTargetsError {
    /// No target was named
    #[error("a waiting report must name at least one task or job with --on")]
    Empty,
    /// The same id appears twice
    #[error("--on names {id} more than once")]
    Duplicate {
        /// Repeated id
        id: DependencyId,
    },
    /// More than [`MAX_WAIT_TARGETS`] targets
    #[error("--on names {count} tasks and jobs; the limit is {MAX_WAIT_TARGETS}")]
    TooMany {
        /// Number of named targets
        count: usize,
    },
}

impl WaitTargets {
    /// Validate the targets a waiting report names
    ///
    /// # Errors
    ///
    /// Refuses an empty list, a repeated id, and more than [`MAX_WAIT_TARGETS`] targets
    pub fn new(targets: Vec<Dependency>) -> Result<Self, WaitTargetsError> {
        let ids: Vec<DependencyId> = targets.iter().map(|target| target.id()).collect();
        Self::check_ids(&ids)?;
        Ok(Self(targets))
    }

    /// Check the shape of a list of ids before each is resolved to a task or job
    ///
    /// # Errors
    ///
    /// As [`Self::new`]
    pub fn check_ids(ids: &[DependencyId]) -> Result<(), WaitTargetsError> {
        if ids.is_empty() {
            return Err(WaitTargetsError::Empty);
        }
        if ids.len() > MAX_WAIT_TARGETS {
            return Err(WaitTargetsError::TooMany { count: ids.len() });
        }
        for (index, id) in ids.iter().enumerate() {
            if ids[..index].contains(id) {
                return Err(WaitTargetsError::Duplicate { id: *id });
            }
        }
        Ok(())
    }

    /// Targets in report order
    #[must_use]
    pub fn targets(&self) -> &[Dependency] {
        &self.0
    }

    /// The `after` entry of each target, in report order
    pub fn ids(&self) -> impl Iterator<Item = DependencyId> + '_ {
        self.0.iter().map(|target| target.id())
    }
}

impl TryFrom<Vec<SavedTarget>> for WaitTargets {
    type Error = WaitTargetsError;

    fn try_from(saved: Vec<SavedTarget>) -> Result<Self, Self::Error> {
        Self::new(saved.into_iter().map(Dependency::from).collect())
    }
}

impl From<WaitTargets> for Vec<Dependency> {
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

/// Data of a `waiting` report: the tasks and jobs the worker waits on and its notes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitingReport {
    /// Tasks and jobs that must end before the continuation starts
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
        /// Tasks and jobs the continuation waits on
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

/// What a parked run handed over: the tasks and jobs it waits on and its continuation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parking {
    /// Tasks and jobs the continuation waits on
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
    /// A target names no task or job this daemon knows
    #[error("waiting target {id} is not a task or a GPU queue job on this machine")]
    UnknownTarget {
        /// Unknown id
        id: DependencyId,
    },
    /// A target is a GPU queue run, which can end while its job continues
    #[error(
        "waiting target {task} is a run of GPU queue job {job}, which can run again; name the job with --on {job} instead"
    )]
    QueueRun {
        /// Queue run named as a target
        task: TaskId,
        /// Job the run belongs to
        job: JobId,
    },
    /// The worker or a target task runs on, or reports to, another machine
    #[error(
        "task {task} runs on or reports to another machine; waiting needs the worker and every target on this machine"
    )]
    Remote {
        /// Task that is not local
        task: TaskId,
    },
    /// A target job was submitted through another machine, so its events
    /// never reach this one
    #[error(
        "job {job} was submitted from another machine; waiting needs every target job submitted from this machine"
    )]
    RemoteJob {
        /// Job that is not local
        job: JobId,
    },
    /// Waiting on these targets would make the work wait on itself
    #[error("waiting on {target} would wait on this task itself")]
    Cycle {
        /// Target that leads back to the worker
        target: Dependency,
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
            Self::QueueRun { .. } => "waiting_target_unsupported",
            Self::Remote { .. } | Self::RemoteJob { .. } => "waiting_remote_unsupported",
            Self::Cycle { .. } => "waiting_cycle",
            Self::TooManyContinuations { .. } => "too_many_continuations",
        }
    }

    /// Structured input for the error body
    ///
    /// A task is under `task` and a job under `job`. An unknown id is under
    /// `task`, as `unknown_dependency` names it for `after`
    #[must_use]
    pub fn input(&self) -> Value {
        match self {
            Self::UnsupportedWorkload => json!({}),
            Self::UnknownTarget { id } => json!({ "task": id }),
            Self::QueueRun { task, job } => json!({ "task": task, "job": job }),
            Self::Remote { task }
            | Self::Cycle {
                target: Dependency::Task(task),
            } => {
                json!({ "task": task })
            }
            Self::RemoteJob { job }
            | Self::Cycle {
                target: Dependency::Job(job),
            } => {
                json!({ "job": job })
            }
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
        .targets()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let mut block = format!(
        "--- homebased continuation ---\n\
         You are continuing task {worker}. You reported waiting on {targets}.\n\
         Those have now ended. Read each outcome with:\n"
    );
    for target in waiting.on.targets() {
        match target {
            Dependency::Task(task) => block.push_str(&format!(
                "  homebased --json task show {task}\n  homebased task log {task} --tail 200\n"
            )),
            Dependency::Job(job) => block.push_str(&format!(
                "  homebased --json resource job show {job}\n  \
                 then homebased task log <run task> --tail 200 for its last run\n"
            )),
        }
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
    use crate::dependency::Dependency;
    use crate::domain::{TaskId, TaskName};
    use crate::queue::JobId;

    fn task() -> Dependency {
        Dependency::Task(TaskId::new())
    }

    #[test]
    fn targets_and_notes_refuse_invalid_input() {
        let task = task();
        assert_eq!(WaitTargets::new(Vec::new()), Err(WaitTargetsError::Empty));
        assert_eq!(
            WaitTargets::new(vec![task, Dependency::Job(JobId::new()), task]),
            Err(WaitTargetsError::Duplicate { id: task.id() })
        );
        let many = (0..=MAX_WAIT_TARGETS).map(|_| self::task()).collect();
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
    fn targets_are_tagged_and_a_bare_uuid_reads_as_a_saved_task() {
        let (task, job) = (TaskId::new(), JobId::new());
        let targets = WaitTargets::new(vec![Dependency::Task(task), Dependency::Job(job)]).unwrap();
        let wire = serde_json::to_value(&targets).unwrap();
        assert_eq!(wire, serde_json::json!([{ "task": task }, { "job": job }]));
        assert_eq!(
            serde_json::from_value::<WaitTargets>(wire).unwrap(),
            targets
        );

        // reports and chains saved before job targets hold bare task UUIDs
        let saved = serde_json::json!([task, { "job": job }]);
        assert_eq!(
            serde_json::from_value::<WaitTargets>(saved).unwrap(),
            targets
        );
    }

    #[test]
    fn continuation_block_names_each_target_and_carries_the_notes() {
        let worker = TaskId::new();
        let (first, second, job) = (TaskId::new(), TaskId::new(), JobId::new());
        let waiting = WaitingReport {
            on: WaitTargets::new(vec![
                Dependency::Task(first),
                Dependency::Job(job),
                Dependency::Task(second),
            ])
            .unwrap(),
            notes: WaitingNotes::new("if it fails, rerun with --small".into()).unwrap(),
        };
        let block = continuation_block(worker, &waiting);
        assert!(block.starts_with("--- homebased continuation ---\n"));
        assert!(block.contains(&format!("You are continuing task {worker}.")));
        assert!(block.contains(&format!(
            "You reported waiting on task {first}, job {job}, task {second}.\n"
        )));
        assert!(block.contains(&format!("homebased --json task show {second}\n")));
        assert!(block.contains(&format!("homebased task log {first} --tail 200\n")));
        assert!(block.contains(&format!("homebased --json resource job show {job}\n")));
        assert!(!block.contains(&format!("task show {job}")), "{block}");
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
            on: WaitTargets::new(vec![task(), Dependency::Job(JobId::new())]).unwrap(),
            continuation,
        };
        let wire = serde_json::to_value(&state).unwrap();
        assert_eq!(wire["state"], "waiting");
        assert_eq!(wire["continuation"], continuation.to_string());
        assert_eq!(serde_json::from_value::<ChainState>(wire).unwrap(), state);
    }
}
