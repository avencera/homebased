//! Token usage of agent workers, read from their output when they stop
//!
//! Claude workers run with `--no-session-persistence`, so their usage never
//! reaches `~/.claude/projects`. The task's `output.log` still holds Claude
//! Code's own accounting, and this module records it per task

mod claude;
mod report;

use std::cmp::Ordering;
use std::iter::Sum;
use std::ops::{Add, AddAssign};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::domain::{AgentKind, Workload};
use crate::error::AppError;
use crate::home::Home;
use crate::store::Store;

pub use report::{TaskUsageEntry, UsageGroup, UsageReport, UsageTotals};

/// Wait for the daemon's write lock; the backfill has no caller waiting on it
const BACKFILL_BUSY_TIMEOUT: Duration = Duration::from_secs(60);

/// Dollars, compared by bit pattern so the events that carry them stay `Eq`
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Usd(pub f64);

impl PartialEq for Usd {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for Usd {}

impl Add for Usd {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

/// Tokens and cost of one model, one task, or a group of tasks
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tokens {
    /// Input tokens that missed the prompt cache
    pub input_tokens: u64,
    /// Output tokens, thinking included
    pub output_tokens: u64,
    /// Input tokens read from the prompt cache
    pub cache_read_tokens: u64,
    /// Input tokens written to the prompt cache
    pub cache_write_tokens: u64,
    /// Cost at list prices, as Claude Code reports it
    pub cost_usd: Usd,
}

impl Tokens {
    /// Every input and output token
    #[must_use]
    pub fn total(&self) -> u64 {
        self.input_tokens + self.output_tokens + self.cache_read_tokens + self.cache_write_tokens
    }

    fn is_zero(&self) -> bool {
        self.total() == 0 && self.cost_usd.0 == 0.0
    }
}

impl AddAssign for Tokens {
    fn add_assign(&mut self, other: Self) {
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cache_read_tokens += other.cache_read_tokens;
        self.cache_write_tokens += other.cache_write_tokens;
        self.cost_usd = self.cost_usd + other.cost_usd;
    }
}

impl Sum for Tokens {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |mut sum, tokens| {
            sum += tokens;
            sum
        })
    }
}

/// Usage of one model within a task
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelUsage {
    /// Model id, such as `claude-opus-5-5`
    pub model: String,
    /// Tokens and cost of this model
    #[serde(flatten)]
    pub tokens: Tokens,
}

/// Usage of one task, summed over its runs
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskUsage {
    /// Whether every run ended with Claude Code's final accounting. A run
    /// stopped before it has exact input and cache tokens, but its output
    /// tokens are a lower bound and its cost is missing
    pub complete: bool,
    /// Assistant turns
    pub turns: u64,
    /// Sum over `models`
    #[serde(flatten)]
    pub total: Tokens,
    /// Per model, highest cost first
    pub models: Vec<ModelUsage>,
}

impl TaskUsage {
    /// Usage with its total and model order derived from `models`
    #[must_use]
    pub fn new(complete: bool, turns: u64, mut models: Vec<ModelUsage>) -> Self {
        models.retain(|model| !model.tokens.is_zero());
        models.sort_by(|a, b| costlier(&a.tokens, &b.tokens).then_with(|| a.model.cmp(&b.model)));
        Self {
            complete,
            turns,
            total: models.iter().map(|model| model.tokens).sum(),
            models,
        }
    }

    /// Whether the output held no usage at all
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }
}

/// Order by cost, then by tokens, since an incomplete run has tokens but no cost
fn costlier(a: &Tokens, b: &Tokens) -> Ordering {
    b.cost_usd
        .0
        .total_cmp(&a.cost_usd.0)
        .then_with(|| b.total().cmp(&a.total()))
}

/// Usage in a finished task's output, for workloads that report it
///
/// Only Claude workers report usage, so other workloads give `None`. An empty
/// usage means the output held none, which is still recorded so the backfill
/// does not read the task again
#[must_use]
pub fn read_task_usage(workload: &Workload, output: &Path) -> Option<TaskUsage> {
    match workload {
        Workload::Agent(agent) if agent.agent.kind == AgentKind::Claude => {
            Some(claude::read_output(output))
        }
        _ => None,
    }
}

/// Record usage for finished Claude tasks that have none, such as tasks that
/// ended before this build recorded usage. Returns how many it read
///
/// It opens its own connection, like a task worker, so reading large logs
/// never holds the daemon's store actor. It returns early once `stop` is set,
/// since the runtime waits for blocking work before the daemon exits; the
/// next start reads the tasks it skipped
pub fn backfill(home: &Home, stop: &AtomicBool) -> Result<usize, AppError> {
    let store = Store::open_with_busy_timeout(&home.db_path(), BACKFILL_BUSY_TIMEOUT)?;
    let mut read = 0;
    for id in store.claude_tasks_without_usage()? {
        if stop.load(AtomicOrdering::Relaxed) {
            break;
        }
        let usage = claude::read_output(&home.task_paths(id).output);
        store.record_task_usage(id, &usage)?;
        read += 1;
    }

    Ok(read)
}
