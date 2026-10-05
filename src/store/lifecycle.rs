//! Task state changes and the events they produce: status CAS, cancel,
//! inactivity reminders, and reports

use chrono::Utc;
use rusqlite::{Connection, params};

use super::chain::{check_waiting_on, park_on, parking_on};
use super::{Store, fmt_time, parse_time};
use crate::callback::{
    ExitClass, ExitPolicy, ProcessPayload, ReportView, check_due_event, classify_exit,
    notify_event, terminal_event,
};
use crate::domain::{
    ContainerExitEvidence, ExitReason, ProcessGroupExitEvidence, ProcessStatus, REPORTS_MAX,
    ReportKind, ReportOutcome, SUMMARY_MAX_BYTES, TaskExitEvidence, TaskId, TaskReport, TaskRow,
    ThreadId, Workload, check_report_allowed, check_status_transition,
};
use crate::error::AppError;
use crate::events::EventPayload;
use crate::queue::StopCause;
use crate::submission::ExecutorIdentity;
use crate::waiting::{WaitTargets, WaitingNotes, WaitingReport};

/// Result of `request_cancel`
#[derive(Debug)]
pub enum CancelResult {
    /// Already terminal: no change
    AlreadyTerminal(TaskRow),
    /// Queued task flipped to Cancelled
    CancelledQueued(TaskRow),
    /// Running task: caller must SIGTERM the worker group
    SignalWorker(TaskRow),
}

impl Store {
    /// Compare-and-swap process status. `None` means the CAS did not match
    pub fn cas_status(
        &self,
        id: TaskId,
        from: ProcessStatus,
        to: ProcessStatus,
    ) -> Result<Option<TaskRow>, AppError> {
        self.cas_status_with_worker_thread(id, from, to, None)
    }

    /// CAS process status and save a worker thread in the same update
    pub(crate) fn cas_status_with_worker_thread(
        &self,
        id: TaskId,
        from: ProcessStatus,
        to: ProcessStatus,
        worker_thread: Option<ThreadId>,
    ) -> Result<Option<TaskRow>, AppError> {
        check_status_transition(from, to)?;
        self.immediate(|| {
            // a run of a queued job ends through its job's classification
            if to.is_terminal() && self.job_run_link(id)?.is_some() {
                return self.commit_lost(id, from, worker_thread);
            }
            let n = self.conn.execute(
                "UPDATE tasks SET status = ?1, updated_at = ?2,
                    worker_thread = COALESCE(?3, worker_thread)
                 WHERE id = ?4 AND status = ?5",
                params![
                    to.as_str(),
                    fmt_time(Utc::now()),
                    worker_thread.map(|thread| thread.to_string()),
                    id.to_string(),
                    from.as_str()
                ],
            )?;
            self.produce_state_event_after_cas(id, n)
        })
    }

    /// CAS status and store an exit reason, with the evidence `from` implies
    pub fn cas_exit(
        &self,
        id: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
    ) -> Result<Option<TaskRow>, AppError> {
        let evidence = if from == ProcessStatus::Queued {
            ProcessGroupExitEvidence::NoChildSpawned
        } else {
            ProcessGroupExitEvidence::Unconfirmed
        };
        self.cas_exit_with_evidence(id, from, reason, evidence, None)
    }

    /// Commit terminal state, evidence from the task-run worker or its
    /// exit-file recovery, and the Codex worker thread in one transaction
    pub(crate) fn cas_exit_with_evidence(
        &self,
        id: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
        evidence: impl Into<TaskExitEvidence>,
        worker_thread: Option<ThreadId>,
    ) -> Result<Option<TaskRow>, AppError> {
        let evidence = &evidence.into();
        check_exit_evidence(from, reason, evidence)?;
        let to = ProcessStatus::from(reason);
        check_status_transition(from, to)?;
        self.immediate(|| {
            if evidence.container != ContainerExitEvidence::Unconfirmed
                && !matches!(
                    self.get_task(id)?.map(|row| row.workload),
                    Some(Workload::Container(_))
                )
            {
                return Err(AppError::Internal {
                    message: "container evidence requires a container task".into(),
                });
            }
            self.cas_exit_inner(id, from, reason, evidence, worker_thread)
        })
    }

    fn cas_exit_inner(
        &self,
        id: TaskId,
        from: ProcessStatus,
        reason: &ExitReason,
        evidence: &TaskExitEvidence,
        worker_thread: Option<ThreadId>,
    ) -> Result<Option<TaskRow>, AppError> {
        // a run of a queued job ends through its job's classification, which
        // may store another outcome than the raw exit and produces no task event
        if self.job_run_link(id)?.is_some() {
            return self.commit_job_run_exit(id, from, reason, evidence, worker_thread);
        }
        let to = ProcessStatus::from(reason);
        let n = self.conn.execute(
            "UPDATE tasks SET status = ?1, exit_reason = ?2,
                process_group_exit_evidence = ?3, container_exit_evidence = ?4,
                updated_at = ?5, worker_thread = COALESCE(?6, worker_thread)
             WHERE id = ?7 AND status = ?8",
            params![
                to.as_str(),
                serde_json::to_string(reason)?,
                evidence.process_group.as_str(),
                evidence.container.to_storage()?,
                fmt_time(Utc::now()),
                worker_thread.map(|thread| thread.to_string()),
                id.to_string(),
                from.as_str()
            ],
        )?;
        if n == 1 && *reason == (ExitReason::Exit { code: 0 }) {
            self.park_if_waiting(id, reason)?;
        }
        self.produce_state_event_after_cas(id, n)
    }

    /// Park a run whose exit classifies as waiting, in its exit transaction
    ///
    /// The exit CAS matches once, so a repeated exit, such as the daemon
    /// applying `exit.json` after the worker's own commit, never parks twice
    fn park_if_waiting(&self, id: TaskId, reason: &ExitReason) -> Result<(), AppError> {
        let row = self.require_task(id)?;
        let reports = self.reports(id)?;
        let class = classify_exit(
            Some(&ProcessPayload::from(reason)),
            &reports,
            ExitPolicy::of(&row.workload),
        );
        let Some(ReportOutcome::Waiting(waiting)) = reports.last().map(|report| &report.outcome)
        else {
            return Ok(());
        };
        if class == ExitClass::Parked {
            park_on(&self.conn, &self.tasks_dir, &row, waiting)?;
        }
        Ok(())
    }

    /// Read the row a matched CAS changed and produce its state event
    fn produce_state_event_after_cas(
        &self,
        id: TaskId,
        updated: usize,
    ) -> Result<Option<TaskRow>, AppError> {
        if updated != 1 {
            return Ok(None);
        }
        let row = self.require_task(id)?;
        self.produce_state_event(&row)?;
        Ok(Some(row))
    }

    pub(super) fn produce_state_event(&self, row: &TaskRow) -> Result<(), AppError> {
        if self.job_run_link(row.id)?.is_some() {
            return Ok(());
        }
        if !self.is_event_task(row.id)? {
            return Ok(());
        }
        let identity: String = self.conn.query_row(
            "SELECT identity_json FROM executor_identities WHERE task_id=?1",
            [row.id.to_string()],
            |entry| entry.get(0),
        )?;
        let mut identity: ExecutorIdentity = serde_json::from_str(&identity)?;
        let ExecutorIdentity::Accepted(record) = &mut identity else {
            return Err(AppError::ClusterTaskConflict { task: row.id });
        };
        record.state = row.status();
        self.conn.execute(
            "UPDATE executor_identities SET identity_json=?1 WHERE task_id=?2",
            params![serde_json::to_string(&identity)?, row.id.to_string()],
        )?;
        let payload = if row.state.is_terminal() {
            let reports = self.reports(row.id)?;
            let evidence = self.tasks_dir.join(row.id.to_string());
            let parking = parking_on(&self.conn, row.id)?;
            let callback = terminal_event(row, &reports, evidence, parking.as_ref());
            EventPayload::Callback {
                event: Box::new(callback),
                state: Some(row.status()),
            }
        } else {
            EventPayload::State {
                status: row.status(),
            }
        };
        self.append_produced_event(row.id, payload)
    }

    /// Mark cancel requested. Terminal tasks are unchanged (idempotent)
    pub fn request_cancel(&self, id: TaskId) -> Result<CancelResult, AppError> {
        self.immediate(|| {
            let task = self.require_task(id)?;
            if !task.state.is_terminal()
                && let Some(job) = self.job_run_link(id)?
                && let Some(run) = self.run_of_job(job)?.filter(|run| run.task == id)
            {
                self.request_stop(run.resource, Some(id), StopCause::UserCancel, Utc::now())?;
            }
            self.request_cancel_inner(id)
        })
    }

    /// Record the cancel request and end a queued task; the caller holds the transaction
    pub(super) fn request_cancel_inner(&self, id: TaskId) -> Result<CancelResult, AppError> {
        self.conn.execute(
            "UPDATE tasks SET cancel_requested_at = ?1, updated_at = ?1
                 WHERE id = ?2 AND cancel_requested_at IS NULL
                   AND status NOT IN ('succeeded', 'failed', 'cancelled', 'lost', 'preempted')",
            params![fmt_time(Utc::now()), id.to_string()],
        )?;
        if let Some(row) = self.cas_exit_inner(
            id,
            ProcessStatus::Queued,
            &ExitReason::Cancelled,
            &ProcessGroupExitEvidence::NoChildSpawned.into(),
            None,
        )? {
            return Ok(CancelResult::CancelledQueued(row));
        }
        let row = self.require_task(id)?;
        if row.state.is_terminal() {
            Ok(CancelResult::AlreadyTerminal(row))
        } else {
            Ok(CancelResult::SignalWorker(row))
        }
    }

    /// Produce one inactivity callback while the task is running
    pub fn produce_attention_event(&self, id: TaskId) -> Result<bool, AppError> {
        if self.job_run_link(id)?.is_some() {
            return self.produce_job_check_due(id);
        }
        self.immediate(|| {
            if !self.is_event_task(id)? {
                return Ok(false);
            }
            let now = fmt_time(Utc::now());
            let changed = self.conn.execute(
                "UPDATE tasks SET check_due_at=?1, updated_at=?1
                 WHERE id=?2 AND check_due_at IS NULL AND status='running'",
                params![now, id.to_string()],
            )?;
            if changed == 0 {
                return Ok(false);
            }
            let row = self.require_task(id)?;
            let reports = self.reports(id)?;
            let event = check_due_event(&row, &reports, self.tasks_dir.join(id.to_string()));
            self.append_produced_event(
                id,
                EventPayload::Callback {
                    event: Box::new(event),
                    state: None,
                },
            )?;
            Ok(true)
        })
    }

    /// Commit a report and its silent or notifying event in one transaction
    ///
    /// Enforces the report cap, summary length, and terminal rejection. A
    /// waiting report is also checked against the tasks it names, in the same
    /// transaction that saves it
    pub fn append_report(
        &self,
        id: TaskId,
        outcome: &ReportOutcome,
        summary: &str,
        notify: bool,
    ) -> Result<Vec<TaskReport>, AppError> {
        if summary.len() > SUMMARY_MAX_BYTES {
            return Err(AppError::SummaryTooLong { len: summary.len() });
        }
        self.immediate(|| {
            let row = self.require_task(id)?;
            check_report_allowed(row.status()).map_err(|_| AppError::TaskTerminal {
                id,
                status: row.status(),
            })?;
            let existing = self.reports(id)?;
            if existing.len() >= REPORTS_MAX {
                return Err(AppError::TooManyReports {
                    count: existing.len(),
                });
            }
            if let ReportOutcome::Waiting(waiting) = outcome {
                check_waiting_on(&self.conn, &row, &waiting.on)?;
            }
            let (waiting_on, notes) = match outcome {
                ReportOutcome::Waiting(waiting) => (
                    Some(serde_json::to_string(&waiting.on)?),
                    Some(waiting.notes.as_str()),
                ),
                ReportOutcome::Succeeded | ReportOutcome::Failed | ReportOutcome::Blocked => {
                    (None, None)
                }
            };
            let seq = existing.len() as i64 + 1;
            self.conn.execute(
                "INSERT INTO reports (task_id, seq, outcome, summary, reported_at, waiting_on, notes)
                 VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    id.to_string(),
                    seq,
                    outcome.kind().as_str(),
                    summary,
                    fmt_time(Utc::now()),
                    waiting_on,
                    notes,
                ],
            )?;
            let reports = self.reports(id)?;
            if !self.is_event_task(id)? {
                return Ok(reports);
            }

            let report = reports.last().ok_or(AppError::Internal {
                message: "inserted report missing".into(),
            })?;
            let payload = if notify {
                let evidence = self.tasks_dir.join(id.to_string());
                EventPayload::Callback {
                    event: Box::new(notify_event(&row, report, evidence)),
                    state: None,
                }
            } else {
                EventPayload::Report {
                    report: ReportView::from(report),
                }
            };
            self.append_produced_event(id, payload)?;
            Ok(reports)
        })
    }

    /// Reports in seq order
    pub fn reports(&self, id: TaskId) -> Result<Vec<TaskReport>, AppError> {
        reports_from(&self.conn, id)
    }
}

/// One saved report row before its outcome is rebuilt
struct ReportRow {
    seq: i64,
    outcome: String,
    summary: String,
    reported_at: String,
    notified_at: Option<String>,
    waiting_on: Option<String>,
    notes: Option<String>,
}

pub(super) fn reports_from(conn: &Connection, id: TaskId) -> Result<Vec<TaskReport>, AppError> {
    let mut statement = conn.prepare(
        "SELECT seq, outcome, summary, reported_at, notified_at, waiting_on, notes
         FROM reports WHERE task_id = ?1 ORDER BY seq",
    )?;
    let rows = statement
        .query_map(params![id.to_string()], |row| {
            Ok(ReportRow {
                seq: row.get(0)?,
                outcome: row.get(1)?,
                summary: row.get(2)?,
                reported_at: row.get(3)?,
                notified_at: row.get(4)?,
                waiting_on: row.get(5)?,
                notes: row.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|row| {
            Ok(TaskReport {
                seq: row.seq,
                outcome: report_outcome(&row)?,
                summary: row.summary,
                reported_at: parse_time(&row.reported_at)?,
                notified_at: row.notified_at.as_deref().map(parse_time).transpose()?,
            })
        })
        .collect()
}

fn report_outcome(row: &ReportRow) -> Result<ReportOutcome, AppError> {
    let invalid = |message: String| AppError::Internal { message };
    Ok(match ReportKind::from_storage(&row.outcome)? {
        ReportKind::Succeeded => ReportOutcome::Succeeded,
        ReportKind::Failed => ReportOutcome::Failed,
        ReportKind::Blocked => ReportOutcome::Blocked,
        ReportKind::Waiting => {
            let (Some(on), Some(notes)) = (&row.waiting_on, &row.notes) else {
                return Err(invalid(format!(
                    "waiting report {} has no targets",
                    row.seq
                )));
            };
            let on: WaitTargets = serde_json::from_str(on)?;
            let notes = WaitingNotes::new(notes.clone())
                .map_err(|error| invalid(format!("waiting report {}: {error}", row.seq)))?;
            ReportOutcome::Waiting(WaitingReport { on, notes })
        }
    })
}

/// Refuse terminal evidence that no worker path records with this transition
fn check_exit_evidence(
    from: ProcessStatus,
    reason: &ExitReason,
    evidence: &TaskExitEvidence,
) -> Result<(), AppError> {
    let refuse = |message: &str| {
        Err(AppError::Internal {
            message: message.into(),
        })
    };
    if evidence.process_group == ProcessGroupExitEvidence::ConfirmedExited
        && from != ProcessStatus::Running
    {
        return refuse("confirmed process-group exit requires a running task worker");
    }
    // a worker that won Queued->Running may already have spawned, so only its
    // own pre-spawn failure can claim that no child exists
    if evidence.process_group == ProcessGroupExitEvidence::NoChildSpawned
        && from != ProcessStatus::Queued
        && !matches!(reason, ExitReason::SpawnFailed { .. })
    {
        return refuse("no-child evidence after start requires a spawn failure");
    }
    match &evidence.container {
        ContainerExitEvidence::Unconfirmed => Ok(()),
        _ if from != ProcessStatus::Running => {
            refuse("container evidence requires a running task worker")
        }
        ContainerExitEvidence::NeverStarted
            if !matches!(
                reason,
                ExitReason::SpawnFailed { .. } | ExitReason::Cancelled
            ) =>
        {
            refuse("never-started container evidence requires a spawn failure or a cancel")
        }
        ContainerExitEvidence::Confirmed { exit_code, .. }
            if !matches!(reason, ExitReason::Cancelled)
                && *reason != (ExitReason::Exit { code: *exit_code }) =>
        {
            refuse("confirmed container evidence must match the task exit code")
        }
        ContainerExitEvidence::NeverStarted | ContainerExitEvidence::Confirmed { .. } => Ok(()),
    }
}
