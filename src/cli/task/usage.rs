//! `homebased task usage`

use std::process::ExitCode;
use std::time::Duration;

use chrono::{Local, SecondsFormat, TimeDelta, Utc};
use clap::ValueEnum;
use serde_json::Value;

use crate::cli::{Ctx, OutputMode};
use crate::client::Client;
use crate::domain::ThreadId;
use crate::error::AppError;

/// Rows of the human `task usage` table
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum UsageGrouping {
    /// One row per model
    Model,
    /// One row per submission day
    Day,
    /// One row per submitting thread
    Thread,
    /// One row per task
    Task,
}

impl UsageGrouping {
    fn field(self) -> &'static str {
        match self {
            Self::Model => "by_model",
            Self::Day => "by_day",
            Self::Thread => "by_thread",
            Self::Task => "tasks",
        }
    }
}

pub(super) async fn usage(
    ctx: &Ctx,
    since: Duration,
    thread: Option<ThreadId>,
    by: UsageGrouping,
) -> Result<ExitCode, AppError> {
    let too_long = || AppError::Usage {
        message: format!("--since {} is too long", humantime::format_duration(since)),
    };
    let window = TimeDelta::from_std(since).map_err(|_| too_long())?;
    let start = Utc::now().checked_sub_signed(window).ok_or_else(too_long)?;
    let mut path = format!(
        "/v1/usage?since={}",
        start.to_rfc3339_opts(SecondsFormat::Secs, true)
    );
    if let Some(thread) = thread {
        path.push_str(&format!("&thread={thread}"));
    }
    let value = Client::new(ctx.home.sock_path()).get(&path).await?;

    match ctx.output {
        OutputMode::Json => ctx.print_json(value)?,
        OutputMode::Quiet => {
            for task in rows(&value, "tasks") {
                println!("{}", text(task, "task"));
            }
        }
        OutputMode::Human => print_human(&value, start, by),
    }
    Ok(ExitCode::SUCCESS)
}

fn print_human(value: &Value, start: chrono::DateTime<Utc>, by: UsageGrouping) {
    let totals = &value["totals"];
    let partial = count(totals, "partial_tasks");
    println!(
        "Since {}: {} tasks, {}",
        start.with_timezone(&Local).format("%Y-%m-%d %H:%M"),
        count(totals, "tasks"),
        cost(totals, partial > 0),
    );
    if partial > 0 {
        println!("{partial} stopped early: their tokens and cost are lower bounds");
    }
    println!();

    let is_task = by == UsageGrouping::Task;
    println!(
        "{:>9} {:>7} {:>8} {:>8} {:>10} {:>11}  {}",
        "cost",
        if is_task { "turns" } else { "tasks" },
        "input",
        "output",
        "cache read",
        "cache write",
        if is_task { "task" } else { "key" },
    );
    for row in rows(value, by.field()) {
        // a task row nests its tokens under `usage`; a group row is flat
        let (tokens, counted, partial, key) = if is_task {
            let usage = &row["usage"];
            let key = format!("{}  {}", text(row, "task"), text(row, "name"));
            let partial = usage["complete"] == false;
            (usage, count(usage, "turns"), partial, key)
        } else {
            let partial = count(row, "partial_tasks") > 0;
            (
                row,
                count(row, "tasks"),
                partial,
                text(row, "key").to_string(),
            )
        };
        println!(
            "{:>9} {counted:>7} {:>8} {:>8} {:>10} {:>11}  {key}",
            cost(tokens, partial),
            amount(count(tokens, "input_tokens")),
            amount(count(tokens, "output_tokens")),
            amount(count(tokens, "cache_read_tokens")),
            amount(count(tokens, "cache_write_tokens")),
        );
    }
}

fn rows<'a>(value: &'a Value, field: &str) -> &'a [Value] {
    value
        .get(field)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value.get(field).and_then(Value::as_str).unwrap_or("-")
}

fn count(value: &Value, field: &str) -> u64 {
    value.get(field).and_then(Value::as_u64).unwrap_or_default()
}

/// Dollars, marked as a lower bound when a counted run stopped early
fn cost(value: &Value, partial: bool) -> String {
    let dollars = value
        .get("cost_usd")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let bound = if partial { "≥" } else { "" };
    format!("{bound}${dollars:.2}")
}

/// A token count in K, M, or B
fn amount(tokens: u64) -> String {
    let tokens = tokens as f64;
    match tokens {
        t if t >= 1e9 => format!("{:.1}B", t / 1e9),
        t if t >= 1e6 => format!("{:.1}M", t / 1e6),
        t if t >= 1e3 => format!("{:.1}K", t / 1e3),
        t => format!("{t}"),
    }
}
