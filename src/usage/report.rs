//! Usage of the tasks started in a window, totalled and grouped for the CLI
//! and the dashboard

use std::collections::HashMap;
use std::path::PathBuf;

use chrono::{DateTime, Local, Utc};
use serde::Serialize;

use super::{TaskUsage, Tokens, costlier};
use crate::domain::{API_VERSION, ProcessStatus, TaskId, TaskName, TaskRow, ThreadId};
use crate::home::Home;

/// `GET /v1/usage`
#[derive(Debug, Serialize)]
pub struct UsageReport {
    /// Public API version
    pub api_version: u32,
    /// Start of the window; tasks count by when they were submitted
    pub since: DateTime<Utc>,
    /// Every task in the window
    pub totals: UsageTotals,
    /// Per model, highest cost first
    pub by_model: Vec<UsageGroup>,
    /// Per submission day in this machine's time zone, newest first
    pub by_day: Vec<UsageGroup>,
    /// Per submitting thread, highest cost first
    pub by_thread: Vec<UsageGroup>,
    /// Per task, highest cost first
    pub tasks: Vec<TaskUsageEntry>,
}

/// Totals of a set of tasks
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct UsageTotals {
    /// Tasks counted
    pub tasks: u64,
    /// Tasks whose usage is incomplete, so their output tokens and cost are
    /// lower bounds
    pub partial_tasks: u64,
    /// Summed tokens and cost
    #[serde(flatten)]
    pub tokens: Tokens,
}

impl UsageTotals {
    fn add(&mut self, complete: bool, tokens: Tokens) {
        self.tasks += 1;
        self.partial_tasks += u64::from(!complete);
        self.tokens += tokens;
    }
}

/// Totals of the tasks that share a model, day, or thread
#[derive(Debug, Clone, Serialize)]
pub struct UsageGroup {
    /// Model id, `YYYY-MM-DD` day, or thread id
    pub key: String,
    /// Totals of the group; a model group counts only that model's tokens
    #[serde(flatten)]
    pub totals: UsageTotals,
}

/// One task with its usage
#[derive(Debug, Clone, Serialize)]
pub struct TaskUsageEntry {
    /// Task id
    pub task: TaskId,
    /// Submitted name
    pub name: TaskName,
    /// Submitting thread
    pub thread: ThreadId,
    /// Task cwd
    pub cwd: PathBuf,
    /// Task directory, with the prompt and full output
    pub evidence: PathBuf,
    /// Process status
    pub status: ProcessStatus,
    /// Submission time
    pub created_at: DateTime<Utc>,
    /// Usage summed over the task's runs
    pub usage: TaskUsage,
}

impl UsageReport {
    /// Total and group `tasks`, which the store selected by `since`
    #[must_use]
    pub fn build(home: &Home, since: DateTime<Utc>, tasks: Vec<(TaskRow, TaskUsage)>) -> Self {
        let mut totals = UsageTotals::default();
        let mut by_model = HashMap::<String, UsageTotals>::new();
        let mut by_day = HashMap::<String, UsageTotals>::new();
        let mut by_thread = HashMap::<String, UsageTotals>::new();
        for (row, usage) in &tasks {
            totals.add(usage.complete, usage.total);
            for model in &usage.models {
                by_model
                    .entry(model.model.clone())
                    .or_default()
                    .add(usage.complete, model.tokens);
            }
            let day = row.created_at.with_timezone(&Local).date_naive();
            by_day
                .entry(day.to_string())
                .or_default()
                .add(usage.complete, usage.total);
            by_thread
                .entry(row.thread.to_string())
                .or_default()
                .add(usage.complete, usage.total);
        }

        let mut by_day = groups(by_day);
        by_day.sort_by(|a, b| b.key.cmp(&a.key));
        let mut tasks: Vec<_> = tasks
            .into_iter()
            .map(|(row, usage)| TaskUsageEntry {
                task: row.id,
                evidence: home.task_dir(row.id),
                status: row.status(),
                name: row.name,
                thread: row.thread,
                cwd: row.cwd,
                created_at: row.created_at,
                usage,
            })
            .collect();
        tasks.sort_by(|a, b| costlier(&a.usage.total, &b.usage.total));

        Self {
            api_version: API_VERSION,
            since,
            totals,
            by_model: groups(by_model),
            by_day,
            by_thread: groups(by_thread),
            tasks,
        }
    }
}

/// Groups, highest cost first
fn groups(totals: HashMap<String, UsageTotals>) -> Vec<UsageGroup> {
    let mut groups: Vec<_> = totals
        .into_iter()
        .map(|(key, totals)| UsageGroup { key, totals })
        .collect();
    groups.sort_by(|a, b| {
        costlier(&a.totals.tokens, &b.totals.tokens).then_with(|| a.key.cmp(&b.key))
    });
    groups
}
