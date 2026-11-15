//! Child-thread links and the reports child threads send, see [`crate::child_thread`]
//!
//! A report moves only forward through its stages, inside one immediate
//! transaction per step, so the daemon's scan and a retried `thread report`
//! can both push the same report on without undoing each other

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Row, params};

use super::{Store, fmt_time, parse_time};
use crate::child_thread::{
    Lineage, LinkEnd, LinkId, LinkView, ReportStage, ReportStageKind, ThreadReport,
};
use crate::error::AppError;

const LINEAGE_COLUMNS: &str =
    "l.id, l.parent_machine, l.parent_thread, l.child_machine, l.child_thread, l.created_at";

const REPORT_SELECT: &str = "
    SELECT l.id, l.parent_machine, l.parent_thread, l.child_machine, l.child_thread, l.created_at,
           r.body, r.source_session, r.run_id, r.stage, r.created_at, r.sent_at, r.settled_at
    FROM thread_reports r JOIN thread_lineage l ON l.id = r.link_id";

impl Store {
    /// Save a link, or return the one a retried launch saved
    ///
    /// # Errors
    ///
    /// `t3_launch_refused` when the launch's request id already made a link
    /// between other threads, or the child thread already has another parent
    pub fn insert_lineage(&self, lineage: &Lineage) -> Result<Lineage, AppError> {
        self.immediate(|| {
            if let Some(saved) = self.lineage(lineage.id)? {
                if saved.parent != lineage.parent || saved.child != lineage.child {
                    return Err(AppError::T3LaunchRefused {
                        message: format!(
                            "request id {} already launched thread {} as a child of {}",
                            lineage.id, saved.child.t3_thread, saved.parent.t3_thread
                        ),
                    });
                }
                return Ok(saved);
            }
            if let Some(other) = self.lineage_of_child(&lineage.child)? {
                return Err(AppError::T3LaunchRefused {
                    message: format!(
                        "thread {} is already the child of {} on {}",
                        lineage.child.t3_thread, other.parent.t3_thread, other.parent.machine
                    ),
                });
            }
            self.conn.execute(
                "INSERT INTO thread_lineage
                 (id, parent_machine, parent_thread, child_machine, child_thread,
                  report_message_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    lineage.id.to_string(),
                    lineage.parent.machine.to_string(),
                    lineage.parent.t3_thread,
                    lineage.child.machine.to_string(),
                    lineage.child.t3_thread,
                    lineage.report_message().to_string(),
                    fmt_time(lineage.created_at),
                ],
            )?;
            Ok(lineage.clone())
        })
    }

    /// One saved link
    pub fn lineage(&self, id: LinkId) -> Result<Option<Lineage>, AppError> {
        self.conn
            .query_row(
                &format!("SELECT {LINEAGE_COLUMNS} FROM thread_lineage l WHERE l.id = ?1"),
                [id.to_string()],
                read_lineage,
            )
            .optional()?
            .transpose()
    }

    /// The link that names `child` as its child, if any
    pub fn lineage_of_child(&self, child: &LinkEnd) -> Result<Option<Lineage>, AppError> {
        self.conn
            .query_row(
                &format!(
                    "SELECT {LINEAGE_COLUMNS} FROM thread_lineage l
                     WHERE l.child_machine = ?1 AND l.child_thread = ?2"
                ),
                params![child.machine.to_string(), child.t3_thread],
                read_lineage,
            )
            .optional()?
            .transpose()
    }

    /// Every link this machine holds, oldest first, with what it knows of
    /// each child's report
    pub fn thread_links(&self) -> Result<Vec<LinkView>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "SELECT {LINEAGE_COLUMNS}, l.closed_at IS NOT NULL,
                    EXISTS (SELECT 1 FROM message_receipts m
                            WHERE m.message_id = l.report_message_id),
                    r.stage
             FROM thread_lineage l LEFT JOIN thread_reports r ON r.link_id = l.id
             ORDER BY l.created_at, l.id"
        ))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    read_lineage(row)?,
                    row.get::<_, bool>(6)?,
                    row.get::<_, bool>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(lineage, closed, report_received, stage)| {
                Ok(LinkView {
                    lineage: lineage?,
                    report_received,
                    closed,
                    report: stage.as_deref().map(str::parse).transpose()?,
                })
            })
            .collect()
    }

    /// Record that the parent's machine saw the child of `id` put away
    /// without a report; the first time counts
    pub fn close_link(&self, id: LinkId, now: DateTime<Utc>) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE thread_lineage SET closed_at = ?2 WHERE id = ?1 AND closed_at IS NULL",
            params![id.to_string(), fmt_time(now)],
        )?;
        Ok(())
    }

    /// Save a new report in [`ReportStage::Sending`], or return the one
    /// already saved for its link, with whether this call saved it
    pub fn begin_report(&self, report: &ThreadReport) -> Result<(ThreadReport, bool), AppError> {
        self.immediate(|| {
            if let Some(saved) = self.thread_report(report.lineage.id)? {
                return Ok((saved, false));
            }
            self.conn.execute(
                "INSERT INTO thread_reports
                 (link_id, body, source_session, run_id, stage, created_at)
                 VALUES (?1, ?2, ?3, ?4, 'sending', ?5)",
                params![
                    report.lineage.id.to_string(),
                    report.body,
                    report.source.to_string(),
                    report.run,
                    fmt_time(report.created_at),
                ],
            )?;
            let saved =
                self.thread_report(report.lineage.id)?
                    .ok_or_else(|| AppError::Internal {
                        message: "a saved report cannot be read back".into(),
                    })?;
            Ok((saved, true))
        })
    }

    /// The report of the child of `id`, if it has made one
    pub fn thread_report(&self, id: LinkId) -> Result<Option<ThreadReport>, AppError> {
        self.conn
            .query_row(
                &format!("{REPORT_SELECT} WHERE r.link_id = ?1"),
                [id.to_string()],
                read_report,
            )
            .optional()?
            .transpose()
    }

    /// Reports whose send or settle is unfinished, oldest first
    pub fn open_reports(&self) -> Result<Vec<ThreadReport>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "{REPORT_SELECT} WHERE r.stage IN ('sending', 'settle_pending')
             ORDER BY r.created_at, r.link_id"
        ))?;
        let rows = statement
            .query_map([], read_report)?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter().collect()
    }

    /// Move the report of `id` on to `next`, and return it as it is afterwards
    ///
    /// A step it has already taken, or one past it, leaves the report as it
    /// is, so two callers racing to the same stage agree. Moving back is
    /// refused as a bug
    pub fn advance_report(&self, id: LinkId, next: ReportStage) -> Result<ThreadReport, AppError> {
        self.immediate(|| {
            let saved = self.thread_report(id)?.ok_or_else(|| AppError::Internal {
                message: format!("no report for link {id}"),
            })?;
            let allowed = matches!(
                (saved.stage.kind(), next.kind()),
                (ReportStageKind::Sending, ReportStageKind::SettlePending)
                    | (
                        ReportStageKind::SettlePending,
                        ReportStageKind::Sent | ReportStageKind::Settled
                    )
            );
            if !allowed {
                let already = saved.stage.kind() == next.kind()
                    || matches!(
                        saved.stage.kind(),
                        ReportStageKind::Sent | ReportStageKind::Settled
                    );
                if already {
                    return Ok(saved);
                }
                return Err(AppError::Internal {
                    message: format!(
                        "report of link {id} cannot move from {} to {}",
                        saved.stage.kind().as_str(),
                        next.kind().as_str()
                    ),
                });
            }
            let settled_at = match next {
                ReportStage::Settled { settled_at, .. } => Some(fmt_time(settled_at)),
                _ => None,
            };
            self.conn.execute(
                "UPDATE thread_reports SET stage = ?2, sent_at = ?3, settled_at = ?4
                 WHERE link_id = ?1",
                params![
                    id.to_string(),
                    next.kind().as_str(),
                    next.sent_at().map(fmt_time),
                    settled_at,
                ],
            )?;
            Ok(ThreadReport {
                stage: next,
                ..saved
            })
        })
    }
}

fn read_lineage(row: &Row<'_>) -> rusqlite::Result<Result<Lineage, AppError>> {
    let id: String = row.get(0)?;
    let parent_machine: String = row.get(1)?;
    let parent_thread: String = row.get(2)?;
    let child_machine: String = row.get(3)?;
    let child_thread: String = row.get(4)?;
    let created_at: String = row.get(5)?;
    Ok((|| {
        Ok(Lineage {
            id: id.parse()?,
            parent: LinkEnd {
                machine: parent_machine.parse()?,
                t3_thread: parent_thread,
            },
            child: LinkEnd {
                machine: child_machine.parse()?,
                t3_thread: child_thread,
            },
            created_at: parse_time(&created_at)?,
        })
    })())
}

fn read_report(row: &Row<'_>) -> rusqlite::Result<Result<ThreadReport, AppError>> {
    let lineage = read_lineage(row)?;
    let body: String = row.get(6)?;
    let source: String = row.get(7)?;
    let run: String = row.get(8)?;
    let stage: String = row.get(9)?;
    let created_at: String = row.get(10)?;
    let sent_at: Option<String> = row.get(11)?;
    let settled_at: Option<String> = row.get(12)?;
    Ok((|| {
        let sent_at = sent_at.as_deref().map(parse_time).transpose()?;
        let settled_at = settled_at.as_deref().map(parse_time).transpose()?;
        let stage = match (stage.parse()?, sent_at, settled_at) {
            (ReportStageKind::Sending, None, None) => ReportStage::Sending,
            (ReportStageKind::SettlePending, Some(sent_at), None) => {
                ReportStage::SettlePending { sent_at }
            }
            (ReportStageKind::Sent, Some(sent_at), None) => ReportStage::Sent { sent_at },
            (ReportStageKind::Settled, Some(sent_at), Some(settled_at)) => ReportStage::Settled {
                sent_at,
                settled_at,
            },
            (kind, _, _) => {
                return Err(AppError::Internal {
                    message: format!("report stage {} has the wrong times", kind.as_str()),
                });
            }
        };
        Ok(ThreadReport {
            lineage: lineage?,
            body,
            source: source.parse()?,
            run,
            created_at: parse_time(&created_at)?,
            stage,
        })
    })())
}
