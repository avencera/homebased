//! Stops the task workers a test daemon left running
//!
//! Workers run in their own sessions and outlive a killed daemon. A test that
//! fails before it releases a gated workload would otherwise leave the worker
//! and its workload polling forever for a gate inside a deleted TempDir

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use homebased::store::{CancelResult, Store};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

/// Cancel every unfinished task under `home` and wait for its worker to exit
///
/// Mirrors `daemon stop --yes` with the socket down: record the cancel, then
/// SIGTERM the worker, which stops its workload's process group
pub fn stop_workers(home: &Path) {
    let Ok(store) = Store::open(&home.join(homebased::home::DB_NAME)) else {
        return;
    };
    let Ok(rows) = store.non_terminal() else {
        return;
    };

    let mut workers = Vec::new();
    for row in rows {
        if let Ok(CancelResult::SignalWorker(row)) = store.request_cancel(row.id)
            && let Some(pid) = row.pid()
        {
            let pid = Pid::from_raw(pid);
            let _ = kill(pid, Signal::SIGTERM);
            workers.push(pid);
        }
    }

    // the TempDir is removed right after this returns, so let workers finish
    // their last writes while it still exists
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && workers.iter().any(|pid| kill(*pid, None).is_ok()) {
        thread::sleep(Duration::from_millis(50));
    }
}
