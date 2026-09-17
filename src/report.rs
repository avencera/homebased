//! Worker reports: trailer text and direct SQLite writes.

use crate::domain::{AgentReport, ReportOutcome, TaskId};
use crate::error::AppError;
use crate::store::Store;

/// Fixed reporting trailer, versioned with `api_version`.
pub const REPORT_TRAILER: &str = r#"--- homebased ---
When your work is complete, run exactly one of:
  homebased task report --outcome succeeded --summary "<one paragraph>"
  homebased task report --outcome failed --summary "<what failed and why>"
  homebased task report --outcome blocked --summary "<the decision you need>"
Do not run `codex queue`. Do not report before the work is complete.
You may report more than once. Reports are appended in order, and the
last outcome is your final outcome. Add --notify only if the
orchestrator must see that report before you finish.
"#;

/// Append a report without contacting the daemon.
pub fn append_report(
    store: &Store,
    id: TaskId,
    outcome: ReportOutcome,
    summary: &str,
) -> Result<Vec<AgentReport>, AppError> {
    store.append_report(id, outcome, summary)
}

/// Trailer bytes stored in `prompt.trailer.txt` when enabled.
#[must_use]
pub fn trailer_text() -> &'static str {
    REPORT_TRAILER
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trailer_mentions_task_report_and_not_codex_queue() {
        assert!(REPORT_TRAILER.contains("homebased task report"));
        assert!(REPORT_TRAILER.contains("Do not run `codex queue`"));
        assert!(REPORT_TRAILER.contains("--notify"));
    }
}
