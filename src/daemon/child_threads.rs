//! `homebased thread report`, and the scan that finishes what it started
//!
//! The report route runs in the child thread's own machine. It refuses while
//! the thread has open work, numbers the report after the child's latest one
//! (see [`ReportPlan`]), saves it before it sends anything, sends
//! it to the parent through the message path, and leaves the settle to the
//! scan: T3 refuses `thread.settle` while a turn runs, and the turn that
//! reported is still running then. The scan runs with the idle compaction
//! loop. It resends a report whose send did not finish, which a lost reply
//! or a daemon restart leaves, settles a reported thread once its reporting
//! run has ended, and closes the parent's links to children on other
//! machines that were put away without reporting

use std::collections::HashMap;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use super::cluster::check_api_version;
use super::context_threads::run_blocking;
use super::message_sender;
use super::thread_api::local::LocalIndex;
use super::thread_api::watch_scan::GONE_SCANS;
use super::thread_api::{find_on, local_sources};
use crate::child_thread::{
    Lineage, LinkEnd, LinkId, OpenWork, ReportConflict, ReportPlan, ReportStage, ThreadReport,
    ThreadReportRequest, ThreadReportResponse,
};
use crate::domain::{API_VERSION, ThreadId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::message::MESSAGE_BODY_MAX_BYTES;
use crate::t3::{SettleDue, T3Env, WakeOutcome, active_run_of, settle_state, settle_thread};
use crate::thread_ref::Caller;

/// Local-only routes; a report sends a message and settles a thread
pub fn socket_routes() -> Router<AppState> {
    Router::new().route("/v1/threads/report", post(report))
}

/// What `thread report` reads of the reporting thread on this machine
struct Reporter {
    /// Its T3 thread
    t3_thread: String,
    /// The session T3 runs it in now, else the caller's
    session: Option<ThreadId>,
    /// The T3 run active in it now
    run: Option<String>,
    /// Open work this machine can see
    open: Vec<OpenWork>,
    /// Links to children on other machines that may still owe it a report
    remote_children: Vec<Lineage>,
}

async fn report(
    State(state): State<AppState>,
    Json(body): Json<ThreadReportRequest>,
) -> Result<Json<ThreadReportResponse>, AppError> {
    check_api_version(body.api_version)?;
    let local = state.machine.identity.machine;
    let reporter = read_reporter(&state, body.caller).await?;
    let child = LinkEnd {
        machine: local,
        t3_thread: reporter.t3_thread.clone(),
    };
    let lineage = call(&state.store, |reply| StoreMsg::LineageOfChild {
        child: child.clone(),
        reply,
    })
    .await?
    .ok_or_else(|| AppError::NotChildThread {
        thread: format!("thread_{}", child.t3_thread),
    })?;

    // the same text again finishes the saved report as it is, since its
    // checks passed when it was saved
    let id = lineage.id;
    let latest = call(&state.store, |reply| StoreMsg::LatestReport { id, reply }).await?;
    let seq = match ReportPlan::decide(latest, &body.body, reporter.run.as_deref())? {
        ReportPlan::Resume(saved) => return resume(&state, *saved, &body.body).await,
        ReportPlan::Save(seq) => seq,
    };

    check_body(&body.body)?;
    let mut open = reporter.open;
    for remote in reporter.remote_children {
        open.extend(remote_child_open(&state, remote).await);
    }
    if !open.is_empty() {
        return Err(AppError::ThreadReportRefused { open });
    }
    let run = reporter.run.ok_or_else(|| AppError::Usage {
        message: "no T3 run is active in this thread; run `homebased thread report` from the thread's own turn".into(),
    })?;
    let source = reporter.session.ok_or_else(|| AppError::Usage {
        message: "this T3 thread has no Claude session or Codex thread to send the report from"
            .into(),
    })?;

    let report = ThreadReport {
        lineage,
        seq,
        body: body.body,
        source,
        run,
        created_at: Utc::now(),
        stage: ReportStage::Sending,
    };
    // the message adds a header, so the size is checked on what is sent
    report.message().validate()?;
    let (saved, created) = call(&state.store, |reply| StoreMsg::BeginReport {
        report: Box::new(report.clone()),
        reply,
    })
    .await?;
    if !created {
        // another call saved one first
        return resume(&state, saved, &report.body).await;
    }
    info!(
        link = %saved.lineage.id,
        seq = saved.seq.get(),
        parent = %saved.lineage.parent.t3_thread,
        "Child thread report saved"
    );
    let sent = finish_send(&state, saved).await?;
    Ok(Json(response(&sent, false)))
}

/// Finish the report already saved, refusing another body
async fn resume(
    state: &AppState,
    saved: ThreadReport,
    body: &str,
) -> Result<Json<ThreadReportResponse>, AppError> {
    if saved.body != body {
        return Err(ReportConflict::with(&saved));
    }
    let report = finish_send(state, saved).await?;
    Ok(Json(response(&report, true)))
}

fn response(report: &ThreadReport, resumed: bool) -> ThreadReportResponse {
    ThreadReportResponse {
        api_version: API_VERSION,
        report: report.view(),
        resumed,
    }
}

fn check_body(body: &str) -> Result<(), AppError> {
    if body.trim().is_empty() {
        return Err(AppError::Usage {
            message: "the report must not be blank".into(),
        });
    }
    if body.len() > MESSAGE_BODY_MAX_BYTES {
        return Err(AppError::Usage {
            message: format!(
                "the report is {} bytes; at most {MESSAGE_BODY_MAX_BYTES} fit in one message",
                body.len()
            ),
        });
    }
    Ok(())
}

/// Read the reporting thread from this machine's index
async fn read_reporter(state: &AppState, caller: Caller) -> Result<Reporter, AppError> {
    let sources = local_sources(state).await?;
    // a sleep hold is open work as an owed task or job is, matched to the
    // thread's sessions the same way
    let mut owed = call(&state.store, |reply| StoreMsg::OwedTargets { reply }).await?;
    owed.extend(call(&state.store, |reply| StoreMsg::HoldTargets { reply }).await?);
    run_blocking(move || {
        let index =
            LocalIndex::load(sources, Utc::now()).map_err(|message| AppError::T3Unavailable {
                message: format!("T3's thread state cannot be read: {message}"),
            })?;
        let Some(t3) = index.caller_t3_here(&caller) else {
            let thread = caller.selector().map_or_else(
                || "this session".to_owned(),
                |selector| selector.to_string(),
            );
            return Err(AppError::NotChildThread { thread });
        };
        let (open, remote_children) = index.open_work(t3, &owed);
        let run =
            active_run_of(index.env(), &t3.id).map_err(|message| AppError::T3Unavailable {
                message: format!("T3's runs cannot be read: {message}"),
            })?;
        Ok(Reporter {
            t3_thread: t3.id.clone(),
            session: t3
                .session
                .map(crate::t3::ProviderThread::id)
                .or(caller.session),
            run,
            open,
            remote_children,
        })
    })
    .await?
}

/// Whether the child of `link`, on another machine, is still open, by the
/// rule of [`crate::child_thread::LinkView::owes_report`]
///
/// The reporting thread's own machine holds no receipt for it, so its
/// machine is asked. One it finds put away closes the link; one it does not
/// find, or a machine that does not answer, counts as open, so a report is
/// never made early. The scan closes a link whose child stays gone
async fn remote_child_open(state: &AppState, link: Lineage) -> Option<OpenWork> {
    let child = link.child;
    let reference = format!("thread_{}", child.t3_thread);
    let found = match find_on(state, child.machine, vec![reference]).await {
        Ok(mut results) => results.pop().unwrap_or_default(),
        Err(reason) => return Some(OpenWork::Unconfirmed { child, reason }),
    };
    let Some(thread) = found
        .iter()
        .find(|thread| thread.t3_thread.as_deref() == Some(child.t3_thread.as_str()))
    else {
        return Some(OpenWork::Unconfirmed {
            child,
            reason: "its machine does not find it".into(),
        });
    };
    if thread.put_away() {
        close_link(state, link.id, thread.state.as_str()).await;
        return None;
    }
    let reported = thread
        .child_thread
        .as_ref()
        .is_some_and(|info| info.reported);
    (!reported).then_some(OpenWork::Child { child })
}

async fn close_link(state: &AppState, id: LinkId, why: &str) {
    let now = Utc::now();
    match call(&state.store, |reply| StoreMsg::CloseLink { id, now, reply }).await {
        Ok(()) => info!(link = %id, "Child thread put away without a report ({why})"),
        Err(error) => warn!(link = %id, "child thread link close: {error}"),
    }
}

/// Send a report still in [`ReportStage::Sending`], with its saved message
/// id, and record it delivered
async fn finish_send(state: &AppState, report: ThreadReport) -> Result<ThreadReport, AppError> {
    if report.stage != ReportStage::Sending {
        return Ok(report);
    }
    message_sender::send(state, report.message()).await?;
    let (id, seq) = (report.lineage.id, report.seq);
    let next = ReportStage::SettlePending {
        sent_at: Utc::now(),
    };
    call(&state.store, |reply| StoreMsg::AdvanceReport {
        id,
        seq,
        next,
        reply,
    })
    .await
}

/// What the scan remembers between runs: how many scans in a row found each
/// remote child gone
#[derive(Debug, Default)]
pub(super) struct ChildScanMemory {
    gone: HashMap<LinkId, u8>,
}

/// Finish unfinished reports, and close links to remote children put away
/// without a report
pub(super) async fn scan(state: &AppState, memory: &mut ChildScanMemory) {
    let reports = match call(&state.store, |reply| StoreMsg::OpenReports { reply }).await {
        Ok(reports) => reports,
        Err(error) => {
            warn!("child thread report scan: {error}");
            return;
        }
    };
    for report in reports {
        let id = report.lineage.id;
        let result = match report.stage {
            ReportStage::Sending => finish_send(state, report).await.map(drop),
            ReportStage::SettlePending { sent_at } => settle(state, report, sent_at).await,
            ReportStage::Sent { .. } | ReportStage::Settled { .. } => Ok(()),
        };
        if let Err(error) = result {
            debug!(link = %id, "child thread report retry: {error}");
        }
    }
    close_gone_children(state, memory).await;
}

/// Settle the reporting thread of `report` once its run has ended, unless
/// something kept it active
async fn settle(
    state: &AppState,
    report: ThreadReport,
    sent_at: DateTime<Utc>,
) -> Result<(), AppError> {
    let thread = report.lineage.child.t3_thread.clone();
    let run = report.run.clone();
    let since = report.created_at;
    let checked = run_blocking(move || {
        let env = T3Env::from_env();
        let due = settle_state(&env, &thread, since)?.due(&run);
        let outcome = (due == SettleDue::Settle).then(|| settle_thread(&env, &thread, &run));
        Ok::<_, String>((due, outcome))
    })
    .await?;
    let (due, outcome) = checked.map_err(|message| AppError::T3Unavailable { message })?;

    let now = Utc::now();
    let next = match (due, outcome) {
        (SettleDue::Wait, _) => return Ok(()),
        (SettleDue::AlreadyDone, _) | (SettleDue::Settle, Some(WakeOutcome::Woken { .. })) => {
            ReportStage::Settled {
                sent_at,
                settled_at: now,
            }
        }
        // a later run or message, or T3's refusal, keeps the thread active
        (SettleDue::Skip, _) | (SettleDue::Settle, Some(WakeOutcome::Refused(_))) => {
            ReportStage::Sent { sent_at }
        }
        (SettleDue::Settle, outcome) => {
            return Err(AppError::T3Unavailable {
                message: format!("thread.settle did not go through: {outcome:?}"),
            });
        }
    };
    let (id, seq) = (report.lineage.id, report.seq);
    let saved = call(&state.store, |reply| StoreMsg::AdvanceReport {
        id,
        seq,
        next,
        reply,
    })
    .await?;
    info!(
        link = %id,
        seq = seq.get(),
        stage = saved.stage.kind().as_str(),
        "Child thread report finished"
    );
    Ok(())
}

/// Close each link this machine holds as the parent's whose child, on
/// another machine, has not reported and is put away, or gone for
/// [`GONE_SCANS`] scans in a row
///
/// Without it a parent whose remote child was archived unreported would
/// show `waiting` for good
async fn close_gone_children(state: &AppState, memory: &mut ChildScanMemory) {
    let local = state.machine.identity.machine;
    let links = match call(&state.store, |reply| StoreMsg::ThreadLinks { reply }).await {
        Ok(links) => links,
        Err(error) => {
            warn!("child thread links cannot be read: {error}");
            return;
        }
    };
    let open: Vec<Lineage> = links
        .into_iter()
        .filter(|link| {
            link.lineage.parent.machine == local
                && link.lineage.child.machine != local
                && link.owes_report(false)
        })
        .map(|link| link.lineage)
        .collect();
    memory
        .gone
        .retain(|id, _| open.iter().any(|lineage| lineage.id == *id));

    let mut by_machine: HashMap<MachineId, Vec<Lineage>> = HashMap::new();
    for lineage in open {
        by_machine
            .entry(lineage.child.machine)
            .or_default()
            .push(lineage);
    }
    for (machine, lineages) in by_machine {
        let references = lineages
            .iter()
            .map(|lineage| format!("thread_{}", lineage.child.t3_thread))
            .collect();
        let results = match find_on(state, machine, references).await {
            Ok(results) => results,
            Err(message) => {
                debug!(%machine, "child thread scan skipped a machine: {message}");
                continue;
            }
        };
        for (lineage, found) in lineages.iter().zip(results) {
            let thread = found.iter().find(|thread| {
                thread.t3_thread.as_deref() == Some(lineage.child.t3_thread.as_str())
            });
            match thread {
                Some(thread) if thread.put_away() => {
                    close_link(state, lineage.id, thread.state.as_str()).await;
                }
                Some(_) => {
                    memory.gone.remove(&lineage.id);
                }
                None => {
                    let gone = memory.gone.entry(lineage.id).or_default();
                    *gone = gone.saturating_add(1);
                    if *gone >= GONE_SCANS {
                        close_link(state, lineage.id, "deleted").await;
                    }
                }
            }
        }
    }
}
