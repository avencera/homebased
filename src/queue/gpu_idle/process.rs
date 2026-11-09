//! CPU use of a run's processes, for the alert text
//!
//! The run's processes are the ones carrying its marker, found with cleanup's
//! marker scan, so a container step or a re-parented child is seen as well
//! as the worker's direct child. CPU comes from two `/proc` readings keyed by
//! PID and start time, so a PID reused between them is not mixed in. One full
//! core is 100%

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::domain::TaskId;

/// The busiest process of a run
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSnapshot {
    /// Process ID
    pub pid: i32,
    /// Command name from `/proc/<pid>/stat`
    pub command: String,
    /// Kernel state letter, such as `R` running or `S` sleeping
    pub state: String,
    /// CPU over the sampling interval in whole percent; one full core is 100
    pub cpu_pct: u32,
    /// Processes that carry the run's marker
    pub marked: u32,
}

/// One `/proc/<pid>/stat` reading
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stat {
    command: String,
    state: char,
    cpu_ticks: u64,
    start: u64,
}

/// Command (field 2), state (3), user and system ticks (14, 15), start (22)
///
/// The command may hold spaces and parentheses, so fields after it are
/// counted from the last `)`
fn parse_stat(stat: &str) -> Option<Stat> {
    let open = stat.find('(')?;
    let close = stat.rfind(')')?;
    let command = stat.get(open + 1..close)?.to_owned();
    let fields: Vec<&str> = stat.get(close + 1..)?.split_whitespace().collect();
    let state = fields.first()?.chars().next()?;
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    let start = fields.get(19)?.parse().ok()?;
    Some(Stat {
        command,
        state,
        cpu_ticks: utime.saturating_add(stime),
        start,
    })
}

fn read_stat(pid: i32) -> Option<Stat> {
    parse_stat(&std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?)
}

fn ticks_per_second() -> f64 {
    // SAFETY: sysconf takes a name constant and only returns a value
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 { ticks as f64 } else { 100.0 }
}

/// The busiest marked process of `task` over `interval`, or `None` when no
/// marked process could be read
///
/// Blocks for `interval`; run it off the async runtime
#[must_use]
pub fn busiest(task: TaskId, interval: Duration) -> Option<ProcessSnapshot> {
    let marked = crate::cleanup::marked_processes(task).ok()?;
    let first: HashMap<i32, Stat> = marked
        .iter()
        .filter_map(|identity| {
            let pid = identity.pid.as_raw();
            let stat = read_stat(pid)?;
            (stat.start == identity.start.as_raw()).then_some((pid, stat))
        })
        .collect();
    if first.is_empty() {
        return None;
    }
    let began = Instant::now();
    std::thread::sleep(interval);
    let elapsed = began.elapsed().as_secs_f64().max(0.001);
    let ticks = ticks_per_second();
    first
        .into_iter()
        .filter_map(|(pid, before)| {
            let after = read_stat(pid).filter(|after| after.start == before.start)?;
            let used = after.cpu_ticks.saturating_sub(before.cpu_ticks) as f64;
            Some(ProcessSnapshot {
                pid,
                command: after.command,
                state: after.state.to_string(),
                cpu_pct: (used / ticks / elapsed * 100.0)
                    .round()
                    .clamp(0.0, f64::from(u32::MAX)) as u32,
                marked: u32::try_from(marked.len()).unwrap_or(u32::MAX),
            })
        })
        .max_by_key(|snapshot| snapshot.cpu_pct)
}

#[cfg(test)]
mod tests {
    use super::{Stat, parse_stat};

    #[test]
    fn stat_reads_command_state_ticks_and_start() {
        let stat = "4242 (python (train)) R 1 4240 4240 0 -1 4194560 120 0 0 0 1500 25 0 0 20 0 1 0 987654 1000 200";
        assert_eq!(
            parse_stat(stat),
            Some(Stat {
                command: "python (train)".into(),
                state: 'R',
                cpu_ticks: 1525,
                start: 987_654,
            })
        );
        assert_eq!(parse_stat("4242 (short) S 1"), None);
    }
}
