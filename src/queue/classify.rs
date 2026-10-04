//! How a run's end moves its job
//!
//! The store applies this table in the transaction that commits the run
//! task's terminal state, so the job transition holds even when only the
//! worker is alive, and a valid yield never becomes an ordinary failure

use crate::domain::{ExitReason, ProcessStatus};

use super::StopCause;

/// Exit code a `yield` job uses to stop at a checkpoint (`EX_TEMPFAIL`)
pub const YIELD_EXIT_CODE: i32 = 75;

/// How the run task ended, as far as the table distinguishes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunEnd {
    /// The child exited 0
    Exit0,
    /// The child exited 75, which counts as a yield only after a `Yield` request
    Exit75,
    /// The worker cancelled the run, or a queued run was cancelled before launch
    Cancelled,
    /// Any other exit, a signal, or a spawn failure
    OtherFailure,
    /// The worker was lost
    Lost,
}

impl RunEnd {
    /// Classify a terminal task status and its exit reason
    ///
    /// Returns `None` for a status that is not terminal
    #[must_use]
    pub fn from_terminal(status: ProcessStatus, reason: Option<&ExitReason>) -> Option<Self> {
        if status == ProcessStatus::Lost {
            return Some(Self::Lost);
        }
        if !status.is_terminal() {
            return None;
        }
        Some(match reason? {
            ExitReason::Exit { code: 0 } => Self::Exit0,
            ExitReason::Exit {
                code: YIELD_EXIT_CODE,
            } => Self::Exit75,
            ExitReason::Cancelled => Self::Cancelled,
            ExitReason::Exit { .. }
            | ExitReason::Signal { .. }
            | ExitReason::SpawnFailed { .. } => Self::OtherFailure,
        })
    }
}

/// What the run's task records as its outcome
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunOutcome {
    /// The step's work finished
    Succeeded,
    /// The run failed; the job fails
    Failed,
    /// A person cancelled the job
    Cancelled,
    /// The run stopped for higher-priority work; its job runs again later
    Preempted,
    /// The worker was lost
    Lost,
}

impl RunOutcome {
    /// Task status this outcome stores
    #[must_use]
    pub const fn status(self) -> ProcessStatus {
        match self {
            Self::Succeeded => ProcessStatus::Succeeded,
            Self::Failed => ProcessStatus::Failed,
            Self::Cancelled => ProcessStatus::Cancelled,
            Self::Preempted => ProcessStatus::Preempted,
            Self::Lost => ProcessStatus::Lost,
        }
    }
}

/// How the run's end moves its job
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobTransition {
    /// The last step succeeded
    Succeeded,
    /// Queued again for the step after the run's step
    NextStep,
    /// Queued again for the same step; `resume` says the run yielded at a checkpoint
    Requeue {
        /// Whether the next attempt resumes from a checkpoint
        resume: bool,
    },
    /// The job failed with this run
    Failed,
    /// A person cancelled the job
    Cancelled,
}

/// One row of the classification table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Classification {
    /// What the run task records
    pub outcome: RunOutcome,
    /// How the job moves
    pub job: JobTransition,
}

/// Apply the classification table
///
/// `last_step` says whether the run executed the job's last step, and `cause`
/// is the stop the authority committed for this exact run, if any
///
/// A real exit 0 is never turned into a preemption, so successful work is not
/// repeated. Exit 75 counts as a yield only with a committed `Yield`. A worker
/// cancel that no `Restart` or `UserCancel` explains came from a person who
/// cancelled the run task directly, so it cancels the job like a user cancel
#[must_use]
pub fn classify(end: RunEnd, cause: Option<StopCause>, last_step: bool) -> Classification {
    use JobTransition as Job;
    use RunOutcome as Run;

    let user_cancel = cause == Some(StopCause::UserCancel);
    let (outcome, job) = match end {
        RunEnd::Exit0 if last_step => (Run::Succeeded, Job::Succeeded),
        RunEnd::Exit0 if user_cancel => (Run::Succeeded, Job::Cancelled),
        RunEnd::Exit0 => (Run::Succeeded, Job::NextStep),
        RunEnd::Exit75 if cause == Some(StopCause::Yield) => {
            (Run::Preempted, Job::Requeue { resume: true })
        }
        RunEnd::Cancelled if cause == Some(StopCause::Restart) => {
            (Run::Preempted, Job::Requeue { resume: false })
        }
        RunEnd::Cancelled => (Run::Cancelled, Job::Cancelled),
        RunEnd::Exit75 | RunEnd::OtherFailure if user_cancel => (Run::Cancelled, Job::Cancelled),
        RunEnd::Exit75 | RunEnd::OtherFailure => (Run::Failed, Job::Failed),
        RunEnd::Lost if user_cancel => (Run::Lost, Job::Cancelled),
        RunEnd::Lost => (Run::Lost, Job::Failed),
    };
    Classification { outcome, job }
}

#[cfg(test)]
mod tests {
    use super::{Classification, JobTransition, RunEnd, RunOutcome, classify};
    use crate::domain::{ExitReason, ProcessStatus};
    use crate::queue::StopCause;

    const CAUSES: [Option<StopCause>; 4] = [
        None,
        Some(StopCause::Yield),
        Some(StopCause::Restart),
        Some(StopCause::UserCancel),
    ];

    fn row(outcome: RunOutcome, job: JobTransition) -> Classification {
        Classification { outcome, job }
    }

    /// Every row of the classification table, expanded over each cause it covers
    #[test]
    fn every_row_of_the_classification_table() {
        use JobTransition as Job;
        use RunOutcome as Run;
        use StopCause::{Restart, UserCancel, Yield};

        let mut cases = Vec::new();
        // exit 0, last step, any cause
        for cause in CAUSES {
            cases.push((
                RunEnd::Exit0,
                cause,
                true,
                row(Run::Succeeded, Job::Succeeded),
            ));
        }
        // exit 0, more steps
        for cause in [None, Some(Yield), Some(Restart)] {
            cases.push((
                RunEnd::Exit0,
                cause,
                false,
                row(Run::Succeeded, Job::NextStep),
            ));
        }
        cases.push((
            RunEnd::Exit0,
            Some(UserCancel),
            false,
            row(Run::Succeeded, Job::Cancelled),
        ));
        for last in [true, false] {
            cases.push((
                RunEnd::Exit75,
                Some(Yield),
                last,
                row(Run::Preempted, Job::Requeue { resume: true }),
            ));
            cases.push((
                RunEnd::Exit75,
                Some(UserCancel),
                last,
                row(Run::Cancelled, Job::Cancelled),
            ));
            for cause in [None, Some(Restart)] {
                cases.push((RunEnd::Exit75, cause, last, row(Run::Failed, Job::Failed)));
            }
            cases.push((
                RunEnd::Cancelled,
                Some(Restart),
                last,
                row(Run::Preempted, Job::Requeue { resume: false }),
            ));
            // a worker cancel that no queue cause explains came from a person
            for cause in [None, Some(Yield), Some(UserCancel)] {
                cases.push((
                    RunEnd::Cancelled,
                    cause,
                    last,
                    row(Run::Cancelled, Job::Cancelled),
                ));
            }
            for cause in [None, Some(Yield), Some(Restart)] {
                cases.push((
                    RunEnd::OtherFailure,
                    cause,
                    last,
                    row(Run::Failed, Job::Failed),
                ));
                cases.push((RunEnd::Lost, cause, last, row(Run::Lost, Job::Failed)));
            }
            cases.push((
                RunEnd::OtherFailure,
                Some(UserCancel),
                last,
                row(Run::Cancelled, Job::Cancelled),
            ));
            cases.push((
                RunEnd::Lost,
                Some(UserCancel),
                last,
                row(Run::Lost, Job::Cancelled),
            ));
        }

        for (end, cause, last, expected) in cases {
            assert_eq!(
                classify(end, cause, last),
                expected,
                "end={end:?} cause={cause:?} last_step={last}"
            );
        }
    }

    #[test]
    fn run_end_reads_the_terminal_status_and_reason() {
        let cases = [
            (
                ProcessStatus::Succeeded,
                Some(ExitReason::Exit { code: 0 }),
                Some(RunEnd::Exit0),
            ),
            (
                ProcessStatus::Failed,
                Some(ExitReason::Exit { code: 75 }),
                Some(RunEnd::Exit75),
            ),
            (
                ProcessStatus::Failed,
                Some(ExitReason::Exit { code: 1 }),
                Some(RunEnd::OtherFailure),
            ),
            (
                ProcessStatus::Failed,
                Some(ExitReason::Signal { signal: 9 }),
                Some(RunEnd::OtherFailure),
            ),
            (
                ProcessStatus::Failed,
                Some(ExitReason::SpawnFailed {
                    message: "no cwd".into(),
                }),
                Some(RunEnd::OtherFailure),
            ),
            (
                ProcessStatus::Cancelled,
                Some(ExitReason::Cancelled),
                Some(RunEnd::Cancelled),
            ),
            (ProcessStatus::Lost, None, Some(RunEnd::Lost)),
            (ProcessStatus::Running, None, None),
            (ProcessStatus::Queued, None, None),
        ];
        for (status, reason, expected) in cases {
            assert_eq!(
                RunEnd::from_terminal(status, reason.as_ref()),
                expected,
                "{status} {reason:?}"
            );
        }
    }
}
