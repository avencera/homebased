//! `homebased thread detach`: end a child thread's link to its parent, on
//! whichever Fleet machines hold the link
//!
//! The caller's daemon resolves the child, then has the child's machine
//! finish the detach, here or through `POST /v1/cluster/threads/detach`,
//! since only that machine can send a message from the child's session. The
//! child's machine detaches the parent's copy of the link first, on the
//! parent's machine when that is another one, so a failure never leaves a
//! parent waiting on a child that can no longer report; then it detaches its
//! own copy and tells the parent. Each copy's detach is idempotent and the
//! notice's message UUID derives from the link, so a retry after a failure
//! finishes the same detach

use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use super::cluster::{check_api_version, check_protocol};
use super::message_sender;
use super::thread_api::{Resolved, caller_t3_thread, peer_error, resolve};
use crate::child_thread::{
    DetachNotice, DetachedLink, LinkEnd, LinkId, ThreadDetachRequest, ThreadDetachResponse,
};
use crate::domain::{API_VERSION, ThreadId};
use crate::error::AppError;
use crate::fleet::http::{ClusterClient, DEFAULT_MAX_BODY};
use crate::machine::MachineId;
use crate::thread_ref::ThreadHost;

const CLUSTER_DETACH_PATH: &str = "/v1/cluster/threads/detach";

// the child's machine may wait on the parent's machine and the message path
// in turn, each of which answers within seconds or not at all
const DETACH_TIMEOUT: Duration = Duration::from_secs(60);

/// Local-only route; it ends a link and sends a message
pub fn socket_routes() -> Router<AppState> {
    Router::new().route("/v1/threads/detach", post(detach))
}

/// Peer route for an enabled fleet; it ends a link, so it refuses browser
/// writes from another origin like the control route
pub fn cluster_routes() -> Router<AppState> {
    Router::new()
        .route(CLUSTER_DETACH_PATH, post(cluster_detach))
        .layer(axum::middleware::from_fn(super::web::browser_write_guard))
}

/// What one machine does for a detach
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case", deny_unknown_fields)]
enum DetachStep {
    /// On the child's machine: the whole detach
    Child(ChildStep),
    /// On the parent's machine, when the child runs on another one: end its
    /// copy of the link
    Parent {
        /// The link
        link: LinkId,
    },
}

/// The detach the child's machine finishes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildStep {
    /// The child's T3 thread
    t3_thread: String,
    /// The child's session, which the notice is sent from
    session: Option<ThreadId>,
    /// Whether to tell the parent, which is false when it detached the child
    /// itself
    notify: bool,
}

/// `POST /v1/cluster/threads/detach` body
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterDetach {
    api_version: u32,
    protocol_version: u32,
    destination_machine: MachineId,
    origin_machine: MachineId,
    step: DetachStep,
}

/// `POST /v1/cluster/threads/detach` answer
#[derive(Debug, Serialize, Deserialize)]
struct ClusterDetachBody {
    api_version: u32,
    protocol_version: u32,
    machine: MachineId,
    /// What the child's machine finished; `None` for the parent's step
    detached: Option<DetachedLink>,
}

async fn detach(
    State(state): State<AppState>,
    Json(body): Json<ThreadDetachRequest>,
) -> Result<Json<ThreadDetachResponse>, AppError> {
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
    let not_child = || AppError::NotChildThread {
        thread: thread.exact_reference(),
    };
    let (ThreadHost::T3, Some(t3_thread), Some(info)) =
        (thread.host, &thread.t3_thread, &thread.child_thread)
    else {
        return Err(not_child());
    };

    let local = state.machine.identity.machine;
    let caller_t3 = caller_t3_thread(&state, body.caller).await?;
    let parent_detaches = info.parent.machine == local
        && caller_t3.as_deref() == Some(info.parent.t3_thread.as_str());
    let step = ChildStep {
        t3_thread: t3_thread.clone(),
        session: thread.session,
        notify: !parent_detaches,
    };
    let detached = if thread.machine == local {
        detach_child_here(&state, step).await?
    } else {
        let step = DetachStep::Child(step);
        post_step(&state, thread.machine, &thread.machine_name, step)
            .await?
            .ok_or_else(|| AppError::Internal {
                message: format!("{} answered a detach without its link", thread.machine_name),
            })?
    };
    Ok(Json(ThreadDetachResponse {
        api_version: API_VERSION,
        detached,
    }))
}

async fn cluster_detach(
    State(state): State<AppState>,
    Json(body): Json<ClusterDetach>,
) -> Result<Json<ClusterDetachBody>, AppError> {
    state
        .machine
        .identity
        .check_destination(body.destination_machine)?;
    check_api_version(body.api_version)?;
    check_protocol(body.protocol_version, body.origin_machine)?;
    let detached = match body.step {
        DetachStep::Child(step) => Some(detach_child_here(&state, step).await?),
        DetachStep::Parent { link } => {
            detach_copy(&state, link).await?;
            None
        }
    };
    Ok(Json(ClusterDetachBody {
        api_version: API_VERSION,
        protocol_version: body.protocol_version,
        machine: state.machine.identity.machine,
        detached,
    }))
}

/// Finish a detach of a child thread on this machine
///
/// # Errors
///
/// `not_child_thread` when no link here names the thread, and the
/// transport errors of the parent machine's step
async fn detach_child_here(state: &AppState, step: ChildStep) -> Result<DetachedLink, AppError> {
    let ChildStep {
        t3_thread,
        session,
        notify,
    } = step;
    let local = state.machine.identity.machine;
    let child = LinkEnd {
        machine: local,
        t3_thread,
    };
    let lineage = call(&state.store, |reply| StoreMsg::LineageOfChild {
        child: child.clone(),
        reply,
    })
    .await?
    .ok_or_else(|| AppError::NotChildThread {
        thread: format!("thread_{}", child.t3_thread),
    })?;

    let parent = lineage.parent.machine;
    if parent != local {
        let name = parent.to_string();
        post_step(
            state,
            parent,
            &name,
            DetachStep::Parent { link: lineage.id },
        )
        .await?;
    }
    detach_copy(state, lineage.id).await?;
    info!(
        link = %lineage.id,
        parent = %lineage.parent.t3_thread,
        child = %lineage.child.t3_thread,
        "Child thread detached"
    );

    let notice = match (notify, session) {
        (false, _) => DetachNotice::Skipped,
        (true, None) => DetachNotice::Failed {
            message: "the child has no session to send it from".into(),
        },
        (true, Some(source)) => {
            let message = lineage.detach_notice(source);
            let message_id = message.message_id;
            match message_sender::send(state, message).await {
                Ok(_) => DetachNotice::Sent { message_id },
                Err(error) => {
                    warn!(link = %lineage.id, "detach notice not sent: {error}");
                    DetachNotice::Failed {
                        message: error.to_string(),
                    }
                }
            }
        }
    };
    Ok(DetachedLink {
        link: lineage.id,
        parent: lineage.parent,
        child: lineage.child,
        notice,
    })
}

/// End this machine's copy of link `id`, if it holds one
async fn detach_copy(state: &AppState, id: LinkId) -> Result<(), AppError> {
    let now = Utc::now();
    call(&state.store, |reply| StoreMsg::DetachLink {
        id,
        now,
        reply,
    })
    .await
    .map(drop)
}

/// Run `step` on `machine`, named `machine_name` in errors
async fn post_step(
    state: &AppState,
    machine: MachineId,
    machine_name: &str,
    step: DetachStep,
) -> Result<Option<DetachedLink>, AppError> {
    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| AppError::MachineNotFound {
            machine: machine.to_string(),
        })?;
    let verified = fleet.connect(machine).await?;
    let wire = ClusterDetach {
        api_version: API_VERSION,
        protocol_version: verified.protocol.0,
        destination_machine: machine,
        origin_machine: state.machine.identity.machine,
        step,
    };
    let response = ClusterClient::new(DETACH_TIMEOUT, DEFAULT_MAX_BODY)
        .post_json(&verified.address, CLUSTER_DETACH_PATH, &wire)
        .await
        .map_err(|error| AppError::MachineUnavailable {
            machine,
            message: error.to_string(),
        })?;
    if !response.status.is_success() {
        return Err(peer_error(&response, machine_name, "thread detach"));
    }

    let body: ClusterDetachBody = serde_json::from_slice(&response.body)?;
    if body.api_version != API_VERSION || body.protocol_version != wire.protocol_version {
        return Err(AppError::Internal {
            message: format!("{machine_name} answered with another version"),
        });
    }
    if body.machine != machine {
        return Err(AppError::MachineIdentityMismatch {
            expected: machine,
            found: Some(body.machine),
        });
    }
    Ok(body.detached)
}
