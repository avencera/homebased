//! Tell a parent thread when its child thread waits on a person
//!
//! A child that asks a question or needs an approval stops until someone
//! answers in T3 Code, and nothing reaches the parent, which may be asleep on
//! the child. The child's machine checks its T3 for pending runtime requests
//! with the child-thread scan, and sends the parent one `HOMEBASED_MESSAGE`
//! per request, which wakes a sleeping parent as an alert does. Its message
//! UUID comes from the link and the request, so a restart that forgets what
//! was sent sends the same message, which the message path delivers once

use std::collections::HashSet;

use tracing::{debug, info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use super::context_threads::run_blocking;
use super::message_sender;
use super::thread_api::find_on;
use crate::child_thread::{Lineage, LinkId};
use crate::t3::{PendingRequest, T3Env, pending_requests};

/// Requests already sent, by link and T3 request id
#[derive(Debug, Default)]
pub(super) struct AlertMemory(HashSet<(LinkId, String)>);

/// Send each parent an alert for every new request its child on this
/// machine waits on
pub(super) async fn scan(state: &AppState, memory: &mut AlertMemory) {
    let local = state.machine.identity.machine;
    let links: Vec<Lineage> =
        match call(&state.store, |reply| StoreMsg::ThreadLinks { reply }).await {
            Ok(links) => links
                .into_iter()
                .filter(|link| link.lineage.child.machine == local && !link.closed)
                .map(|link| link.lineage)
                .collect(),
            Err(error) => {
                warn!("child thread links cannot be read: {error}");
                return;
            }
        };
    if links.is_empty() {
        memory.0.clear();
        return;
    }

    let pending = match run_blocking(|| pending_requests(&T3Env::from_env())).await {
        Ok(Ok(pending)) => pending,
        Ok(Err(message)) => {
            debug!("pending T3 requests cannot be read: {message}");
            return;
        }
        Err(error) => {
            debug!("pending T3 requests cannot be read: {error}");
            return;
        }
    };
    let waiting: Vec<(&Lineage, PendingRequest)> = pending
        .into_iter()
        .filter_map(|request| {
            let link = links
                .iter()
                .find(|link| link.child.t3_thread == request.thread)?;
            Some((link, request))
        })
        .collect();
    // an answered request leaves the memory, so it stays small
    memory.0.retain(|(link, request)| {
        waiting
            .iter()
            .any(|(waiting, pending)| waiting.id == *link && pending.id == *request)
    });
    let new: Vec<(&Lineage, PendingRequest)> = waiting
        .into_iter()
        .filter(|(link, request)| !memory.0.contains(&(link.id, request.id.clone())))
        .collect();
    if new.is_empty() {
        return;
    }

    let references: Vec<String> = new
        .iter()
        .map(|(link, _)| format!("thread_{}", link.child.t3_thread))
        .collect();
    let found = match find_on(state, local, references).await {
        Ok(found) => found,
        Err(message) => {
            debug!("child threads cannot be looked up: {message}");
            return;
        }
    };
    for ((link, request), matches) in new.into_iter().zip(found) {
        let Some(thread) = matches
            .iter()
            .find(|thread| thread.t3_thread.as_deref() == Some(link.child.t3_thread.as_str()))
        else {
            continue;
        };
        // a thread put away has nobody left to unblock
        if thread.put_away() {
            continue;
        }
        let Some(source) = thread.session else {
            debug!(link = %link.id, "child thread has no session to send an alert from");
            continue;
        };
        let message = link.request_alert(&request, source, thread.t3_link.as_deref());
        match message_sender::send(state, message).await {
            Ok(_) => {
                info!(
                    link = %link.id,
                    request = %request.id,
                    "Child thread waits on a person; parent told"
                );
                memory.0.insert((link.id, request.id));
            }
            Err(error) => debug!(link = %link.id, "child thread request alert: {error}"),
        }
    }
}
