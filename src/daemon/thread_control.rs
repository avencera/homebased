//! `homebased thread interrupt` and `homebased thread answer` on whichever
//! Fleet machine runs the thread
//!
//! The caller's daemon resolves the thread as `thread compact` does, then acts
//! on it here or forwards to `POST /v1/cluster/threads/control` on the
//! machine whose T3 owns it. A stop of a child thread whose parent is on the
//! caller's machine also closes that link there, so the parent stops waiting
//! on a report the stopped child may never send

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::{info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use super::cluster::{check_api_version, check_protocol};
use super::context_threads::run_blocking;
use super::thread_api::{Resolved, find_on, peer_error, resolve};
use crate::child_thread::LinkId;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::http::{ClusterClient, DEFAULT_MAX_BODY};
use crate::machine::MachineId;
use crate::t3::{InterruptOutcome, T3Env, answer_request, interrupt_thread, pending_requests};
use crate::thread_control::{
    ControlOutcome, ThreadControl, ThreadControlRequest, ThreadControlResponse,
};
use crate::thread_ref::{ThreadContext, ThreadHost};

const CLUSTER_CONTROL_PATH: &str = "/v1/cluster/threads/control";

// the peer asks its T3, which answers within seconds or not at all
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

/// Why homebased acted, which T3 records with an interrupt or stop
const REASON: &str = "homebased thread interrupt";

/// Local-only route; it acts on a thread
pub fn socket_routes() -> Router<AppState> {
    Router::new().route("/v1/threads/control", post(control))
}

/// Peer route for an enabled fleet; it acts on a thread, so it refuses
/// browser writes from another origin like the compaction route
pub fn cluster_routes() -> Router<AppState> {
    Router::new()
        .route(CLUSTER_CONTROL_PATH, post(cluster_control))
        .layer(axum::middleware::from_fn(super::web::browser_write_guard))
}

/// `POST /v1/cluster/threads/control` body
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterControl {
    api_version: u32,
    protocol_version: u32,
    destination_machine: MachineId,
    origin_machine: MachineId,
    t3_thread: String,
    /// The thread as the caller named it, for errors
    reference: String,
    control: ThreadControl,
}

/// `POST /v1/cluster/threads/control` answer
#[derive(Debug, Serialize, Deserialize)]
struct ClusterControlBody {
    api_version: u32,
    protocol_version: u32,
    machine: MachineId,
    outcome: ControlOutcome,
}

async fn control(
    State(state): State<AppState>,
    Json(body): Json<ThreadControlRequest>,
) -> Result<Json<ThreadControlResponse>, AppError> {
    check_api_version(body.api_version)?;
    let Resolved { mut threads, .. } = resolve(
        &state,
        vec![body.thread],
        body.machine.as_deref(),
        false,
        None,
        body.carried_by.as_deref(),
    )
    .await?;
    let thread = threads.pop().ok_or_else(|| AppError::Internal {
        message: "thread lookup returned no thread".into(),
    })?;
    let outcome = control_resolved(&state, &thread, body.control).await?;
    let closed_link = match outcome {
        ControlOutcome::Stopped => close_stopped_link(&state, &thread).await,
        _ => None,
    };
    Ok(Json(ThreadControlResponse {
        api_version: API_VERSION,
        outcome,
        thread,
        closed_link,
    }))
}

/// Act on a looked-up thread on the machine whose T3 owns it
///
/// # Errors
///
/// `read_unsupported_host` for a thread outside T3, `thread_control_refused`
/// when T3 or the request refuses, and the transport errors of a forward
pub(crate) async fn control_resolved(
    state: &AppState,
    thread: &ThreadContext,
    control: ThreadControl,
) -> Result<ControlOutcome, AppError> {
    let reference = thread
        .reference
        .clone()
        .unwrap_or_else(|| thread.exact_reference());
    let t3_thread = match (thread.host, &thread.t3_thread) {
        (ThreadHost::T3, Some(t3_thread)) => t3_thread.clone(),
        (host, _) => return Err(AppError::ReadUnsupportedHost { reference, host }),
    };
    let local = state.machine.identity.machine;
    if thread.machine == local {
        return control_here(state, t3_thread, reference, control).await;
    }

    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| AppError::MachineNotFound {
            machine: thread.machine.to_string(),
        })?;
    let verified = fleet.connect(thread.machine).await?;
    let wire = ClusterControl {
        api_version: API_VERSION,
        protocol_version: verified.protocol.0,
        destination_machine: thread.machine,
        origin_machine: local,
        t3_thread,
        reference,
        control,
    };
    // each T3 command id derives from what it acts on, so a retry is safe
    let response = ClusterClient::new(CONTROL_TIMEOUT, DEFAULT_MAX_BODY)
        .post_json(&verified.address, CLUSTER_CONTROL_PATH, &wire)
        .await
        .map_err(|error| AppError::MachineUnavailable {
            machine: thread.machine,
            message: error.to_string(),
        })?;
    if !response.status.is_success() {
        let command = match wire.control {
            ThreadControl::Answer { .. } => "thread answer",
            ThreadControl::Interrupt | ThreadControl::Stop => "thread interrupt",
        };
        return Err(peer_error(&response, &thread.machine_name, command));
    }
    let body: ClusterControlBody = serde_json::from_slice(&response.body)?;
    if body.api_version != API_VERSION || body.protocol_version != wire.protocol_version {
        return Err(AppError::Internal {
            message: format!("{} answered with another version", thread.machine_name),
        });
    }
    if body.machine != thread.machine {
        return Err(AppError::MachineIdentityMismatch {
            expected: thread.machine,
            found: Some(body.machine),
        });
    }
    Ok(body.outcome)
}

async fn cluster_control(
    State(state): State<AppState>,
    Json(body): Json<ClusterControl>,
) -> Result<Json<ClusterControlBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.origin_machine)?;
    let outcome = control_here(&state, body.t3_thread, body.reference, body.control).await?;
    Ok(Json(ClusterControlBody {
        api_version: API_VERSION,
        protocol_version: body.protocol_version,
        machine: state.machine.identity.machine,
        outcome,
    }))
}

/// Act on a T3 thread through this machine's T3
///
/// A stop first stops the thread's own unreported children, wherever they
/// run, so none of them wakes it again with a question or a report, then
/// closes this machine's copy of the thread's own link, so lookups of it
/// read it as done and its parent stops waiting on it from any machine
async fn control_here(
    state: &AppState,
    t3_thread: String,
    reference: String,
    control: ThreadControl,
) -> Result<ControlOutcome, AppError> {
    let refused = |message: String| AppError::ThreadControlRefused {
        reference: reference.clone(),
        message,
    };
    let unavailable = |message: String| AppError::T3Unavailable { message };
    let (request, answers) = match control {
        ThreadControl::Interrupt | ThreadControl::Stop => {
            let stop = control == ThreadControl::Stop;
            if stop {
                stop_children_of(state, &t3_thread).await;
            }
            let thread = t3_thread.clone();
            let outcome =
                run_blocking(move || interrupt_thread(&T3Env::from_env(), &thread, stop, REASON))
                    .await?
                    .map_err(unavailable)?;
            return match outcome {
                InterruptOutcome::Interrupted(run) => Ok(ControlOutcome::Interrupted { run }),
                InterruptOutcome::Idle => Ok(ControlOutcome::Idle),
                InterruptOutcome::Stopped => {
                    close_own_link(state, &t3_thread).await;
                    Ok(ControlOutcome::Stopped)
                }
                InterruptOutcome::Refused(detail) => Err(refused(detail)),
            };
        }
        ThreadControl::Answer { request, answers } => (request, answers),
    };

    let checked = run_blocking({
        let t3_thread = t3_thread.clone();
        let request = request.clone();
        move || {
            let env = T3Env::from_env();
            let pending = pending_requests(&env).map_err(Err)?;
            let answers = keyed_answers(&pending, &t3_thread, &request, answers).map_err(Ok)?;
            answer_request(&env, &t3_thread, &request, &answers).map_err(Err)
        }
    })
    .await?;
    // `Ok` carries a refusal of the answers, `Err` a T3 that cannot be reached
    match checked {
        Ok(()) => Ok(ControlOutcome::Answered { request }),
        Err(Ok(message)) => Err(refused(message)),
        Err(Err(message)) => Err(unavailable(message)),
    }
}

/// `answers` keyed by the ids of the questions `request` asks, in order
///
/// An answer that names an offered option in another case takes the
/// option's label, since T3 matches labels exactly
fn keyed_answers(
    pending: &[crate::t3::PendingRequest],
    t3_thread: &str,
    request: &str,
    answers: Vec<String>,
) -> Result<Value, String> {
    let Some(asked) = pending
        .iter()
        .find(|pending| pending.id == request && pending.thread == t3_thread)
    else {
        let waiting: Vec<&str> = pending
            .iter()
            .filter(|pending| pending.thread == t3_thread)
            .map(|pending| pending.id.as_str())
            .collect();
        return Err(if waiting.is_empty() {
            format!("no request {request} is pending, and the thread waits on none")
        } else {
            format!(
                "no request {request} is pending; it waits on {}",
                waiting.join(", ")
            )
        });
    };
    if !asked.is_question() {
        return Err(format!(
            "request {request} is a {} approval, which only T3 Code answers",
            asked.kind
        ));
    }
    if asked.questions.len() != answers.len() {
        let asks = match asked.questions.len() {
            1 => "1 question".to_owned(),
            count => format!("{count} questions"),
        };
        return Err(format!(
            "request {request} asks {asks}, and {} answers were given; pass one --answer per question, in order",
            answers.len()
        ));
    }
    let mut keyed = Map::new();
    for (question, answer) in asked.questions.iter().zip(answers) {
        let label = question
            .options
            .iter()
            .find(|option| option.eq_ignore_ascii_case(answer.trim()))
            .cloned()
            .unwrap_or(answer);
        keyed.insert(question.id.clone(), Value::String(label));
    }
    Ok(Value::Object(keyed))
}

/// Stop each unreported child of `t3_thread` on this machine, wherever the
/// child runs, and close its link here, which holds the parent's end
///
/// A child that cannot be stopped is logged and left, so the stop of its
/// parent still goes through
// boxed as `Send`, since a child's stop stops its own children in turn and
// the recursion would otherwise leave the future's type unbounded
fn stop_children_of<'a>(
    state: &'a AppState,
    t3_thread: &'a str,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let local = state.machine.identity.machine;
        let links = match call(&state.store, |reply| StoreMsg::ThreadLinks { reply }).await {
            Ok(links) => links,
            Err(error) => {
                warn!("child thread links cannot be read: {error}");
                return;
            }
        };
        let children = links.into_iter().filter(|link| {
            link.lineage.parent.machine == local
                && link.lineage.parent.t3_thread == t3_thread
                && link.owes_report(false)
        });
        for link in children {
            let child = link.lineage.child;
            let reference = format!("thread_{}", child.t3_thread);
            let found = match find_on(state, child.machine, vec![reference]).await {
                Ok(mut found) => found.pop().unwrap_or_default(),
                Err(reason) => {
                    warn!(link = %link.lineage.id, "child thread not stopped: {reason}");
                    continue;
                }
            };
            let Some(thread) = found
                .into_iter()
                .find(|thread| thread.t3_thread.as_deref() == Some(child.t3_thread.as_str()))
            else {
                // its machine no longer has it, so nothing is left to stop
                close_link(state, link.lineage.id).await;
                continue;
            };
            match control_resolved(state, &thread, ThreadControl::Stop).await {
                Ok(_) => {
                    close_link(state, link.lineage.id).await;
                }
                Err(error) => warn!(link = %link.lineage.id, "child thread not stopped: {error}"),
            }
        }
    })
}

/// Close this machine's copy of the link that made `t3_thread` a child,
/// when it has not reported
async fn close_own_link(state: &AppState, t3_thread: &str) {
    let local = state.machine.identity.machine;
    let links = match call(&state.store, |reply| StoreMsg::ThreadLinks { reply }).await {
        Ok(links) => links,
        Err(error) => {
            warn!("child thread links cannot be read: {error}");
            return;
        }
    };
    let own = links.into_iter().find(|link| {
        link.lineage.child.machine == local && link.lineage.child.t3_thread == t3_thread
    });
    if let Some(link) = own.filter(|link| link.owes_report(false)) {
        close_link(state, link.lineage.id).await;
    }
}

/// Close the link of a stopped child thread when this machine holds its
/// parent's end and the child has not reported
async fn close_stopped_link(state: &AppState, thread: &ThreadContext) -> Option<LinkId> {
    let info = thread.child_thread.as_ref()?;
    if info.reported || info.parent.machine != state.machine.identity.machine {
        return None;
    }
    close_link(state, info.link).await.then_some(info.link)
}

/// Record the link closed, so its parent no longer waits on a report
pub(super) async fn close_link(state: &AppState, id: LinkId) -> bool {
    let now = Utc::now();
    match call(&state.store, |reply| StoreMsg::CloseLink { id, now, reply }).await {
        Ok(()) => {
            info!(link = %id, "Child thread stopped without a report");
            true
        }
        Err(error) => {
            warn!(link = %id, "child thread link close: {error}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::keyed_answers;
    use crate::t3::{PendingQuestion, PendingRequest};

    fn question(id: &str, options: &[&str]) -> PendingQuestion {
        PendingQuestion {
            id: id.into(),
            header: None,
            question: id.into(),
            options: options.iter().map(|option| (*option).to_owned()).collect(),
        }
    }

    fn request(kind: &str, questions: Vec<PendingQuestion>) -> PendingRequest {
        PendingRequest {
            id: "request-1".into(),
            thread: "child".into(),
            kind: kind.into(),
            created_at: "2026-11-23T18:00:00Z".into(),
            questions,
            detail: None,
        }
    }

    #[test]
    fn answers_key_by_question_in_order_and_take_an_option_label() {
        let pending = vec![request(
            "user_input",
            vec![
                question("Which database?", &["Postgres", "SQLite"]),
                question("Anything else?", &[]),
            ],
        )];
        let answers = vec!["sqlite".to_owned(), "Keep the old schema".to_owned()];

        let keyed = keyed_answers(&pending, "child", "request-1", answers).unwrap();

        assert_eq!(
            keyed,
            json!({ "Which database?": "SQLite", "Anything else?": "Keep the old schema" })
        );
    }

    #[test]
    fn answers_are_refused_for_an_approval_a_wrong_count_or_another_request() {
        let approval = vec![request("command", Vec::new())];
        let asked = vec![request("user_input", vec![question("Which?", &[])])];

        let approved = keyed_answers(&approval, "child", "request-1", vec!["yes".into()]);
        let counted = keyed_answers(&asked, "child", "request-1", Vec::new());
        let missing = keyed_answers(&asked, "child", "request-2", vec!["x".into()]);

        assert!(approved.unwrap_err().contains("approval"));
        assert!(counted.unwrap_err().contains("asks 1 question,"));
        assert!(missing.unwrap_err().contains("it waits on request-1"));
    }
}
