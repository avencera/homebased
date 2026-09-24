//! Daemon-owned bounded delivery for durable supervisor notices

use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};

use ractor::ActorRef;
use tracing::{info, warn};

use super::AppState;
use super::actors::{StoreMsg, call};
use super::keyed_locks::KeyedLocks;
use super::resource_notice_sender::{
    ResourceNoticeDeliveryOutcome, ResourceNoticeSendError, deliver_one,
};
use crate::resource::{NoticeId, SupervisorNoticeDelivery};

const SCAN_INTERVAL: Duration = Duration::from_secs(15);
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(15);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5 * 60);

/// Recover durable in-flight attempts, then scan and deliver eligible notices
pub(super) async fn run(state: AppState) {
    let mut worker = DeliveryWorker::new(state.store.clone(), RetryPolicy::default());

    loop {
        let delivery_state = state.clone();
        worker
            .tick_with(|notice_id| {
                let delivery_state = delivery_state.clone();
                async move { deliver_one(&delivery_state, notice_id).await }
            })
            .await;
        tokio::time::sleep(SCAN_INTERVAL).await;
    }
}

struct DeliveryWorker {
    store: ActorRef<StoreMsg>,
    retry_policy: RetryPolicy,
    retries: HashMap<NoticeId, Retry>,
    in_flight: InFlight,
    recovered: bool,
}

impl DeliveryWorker {
    fn new(store: ActorRef<StoreMsg>, retry_policy: RetryPolicy) -> Self {
        Self {
            store,
            retry_policy,
            retries: HashMap::new(),
            in_flight: InFlight::default(),
            recovered: false,
        }
    }

    async fn tick_with<F, Fut>(&mut self, deliver: F)
    where
        F: FnMut(NoticeId) -> Fut,
        Fut: Future<Output = Result<ResourceNoticeDeliveryOutcome, ResourceNoticeSendError>>,
    {
        if !self.recovered && !self.recover().await {
            return;
        }

        self.scan_with(deliver).await;
    }

    async fn recover(&mut self) -> bool {
        match call(&self.store, |reply| {
            StoreMsg::RecoverSendingSupervisorNotices { reply }
        })
        .await
        {
            Ok(Ok(recovered)) => {
                if !recovered.is_empty() {
                    info!(
                        count = recovered.len(),
                        "recovered in-flight supervisor notices"
                    );
                }
                self.recovered = true;
                true
            }
            Ok(Err(error)) => {
                warn!("supervisor notice startup recovery failed: {error}");
                false
            }
            Err(error) => {
                warn!("supervisor notice startup recovery call failed: {error}");
                false
            }
        }
    }

    async fn scan_with<F, Fut>(&mut self, mut deliver: F)
    where
        F: FnMut(NoticeId) -> Fut,
        Fut: Future<Output = Result<ResourceNoticeDeliveryOutcome, ResourceNoticeSendError>>,
    {
        let pending = match call(&self.store, |reply| StoreMsg::PendingSupervisorNotices {
            reply,
        })
        .await
        {
            Ok(Ok(pending)) => pending,
            Ok(Err(error)) => {
                warn!("supervisor notice pending scan failed: {error}");
                return;
            }
            Err(error) => {
                warn!("supervisor notice pending scan call failed: {error}");
                return;
            }
        };

        for notice in pending {
            let ready = self
                .retries
                .entry(notice.id)
                .or_insert_with(|| Retry::new(self.retry_policy))
                .ready();
            if !ready {
                continue;
            }

            let notice_id = notice.id;
            let Some(result) = self
                .in_flight
                .deliver_if_idle(notice_id, || deliver(notice_id))
                .await
            else {
                continue;
            };

            match result {
                Ok(outcome) => self.record_outcome(notice_id, outcome),
                Err(error) => {
                    warn!(notice_id = %notice_id.as_uuid(), "supervisor notice delivery call failed: {error}");
                    self.retry_later(notice_id);
                }
            }
        }
    }

    fn record_outcome(&mut self, notice_id: NoticeId, outcome: ResourceNoticeDeliveryOutcome) {
        match outcome.notice.delivery {
            SupervisorNoticeDelivery::Delivered { attempts } => {
                info!(notice_id = %notice_id.as_uuid(), attempts, "supervisor notice delivered");
                self.retries.remove(&notice_id);
            }
            SupervisorNoticeDelivery::RetryPending {
                attempts,
                last_error,
            } => {
                warn!(
                    notice_id = %notice_id.as_uuid(),
                    attempts,
                    error = %last_error,
                    "supervisor notice delivery failed; retry scheduled"
                );
                self.retry_later(notice_id);
            }
            SupervisorNoticeDelivery::Failed {
                attempts,
                last_error,
            } => {
                warn!(
                    notice_id = %notice_id.as_uuid(),
                    attempts,
                    error = %last_error,
                    "supervisor notice delivery failed; automatic delivery stopped"
                );
                self.retries.remove(&notice_id);
            }
            SupervisorNoticeDelivery::Pending { .. } | SupervisorNoticeDelivery::Sending { .. } => {
                warn!(
                    notice_id = %notice_id.as_uuid(),
                    "supervisor notice delivery returned an unsettled state"
                );
                self.retry_later(notice_id);
            }
        }
    }

    fn retry_later(&mut self, notice_id: NoticeId) {
        self.retries
            .entry(notice_id)
            .or_insert_with(|| Retry::new(self.retry_policy))
            .failed(self.retry_policy);
    }
}

#[derive(Clone, Copy)]
struct RetryPolicy {
    initial: Duration,
    maximum: Duration,
}

impl RetryPolicy {
    const fn new(initial: Duration, maximum: Duration) -> Self {
        Self { initial, maximum }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(INITIAL_RETRY_DELAY, MAX_RETRY_DELAY)
    }
}

struct Retry {
    next: Instant,
    delay: Duration,
}

impl Retry {
    fn new(policy: RetryPolicy) -> Self {
        Self {
            next: Instant::now(),
            delay: policy.initial,
        }
    }

    fn ready(&self) -> bool {
        Instant::now() >= self.next
    }

    fn failed(&mut self, policy: RetryPolicy) {
        self.next = Instant::now() + self.delay;
        self.delay = (self.delay * 2).min(policy.maximum);
    }
}

/// Notices with a delivery in progress
#[derive(Default)]
struct InFlight(KeyedLocks<NoticeId>);

impl InFlight {
    /// Run `deliver` unless the same notice is already being delivered
    async fn deliver_if_idle<F, Fut, T>(&self, notice_id: NoticeId, deliver: F) -> Option<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let _permit = self.0.try_lock(notice_id)?;
        Some(deliver().await)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ractor::{Actor, ActorRef};
    use rusqlite::{Connection, params};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::{DeliveryWorker, InFlight, RetryPolicy};
    use crate::daemon::actors::{StoreActor, StoreMsg, call};
    use crate::daemon::resource_notice_sender::{
        ResourceNoticeDeliveryOutcome, ResourceNoticeSendError,
    };
    use crate::domain::ThreadId;
    use crate::machine::MachineId;
    use crate::resource::store::insert_supervisor_notice_in_transaction;
    use crate::resource::{
        ActionId, AssignmentRevision, DeliveryAttemptId, LoanId, NoticeId, ResourceId,
        ResourceRevision, SupervisorAddress, SupervisorNotice, SupervisorNoticeDelivery,
        SupervisorNoticePayload,
    };
    use crate::store::Store;
    use std::time::Duration;

    #[tokio::test]
    async fn startup_recovery_runs_before_dispatch() {
        let (store, notice, _directory) = seeded_store().await;
        reserve_attempt(&store, notice.id).await;
        let mut worker = DeliveryWorker::new(store.clone(), zero_retry_policy());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let dispatch_count = Arc::clone(&dispatches);
        let delivery_store = store.clone();

        worker
            .tick_with(move |notice_id| {
                let store = delivery_store.clone();
                let dispatch_count = Arc::clone(&dispatch_count);
                async move {
                    let recovered = read_notice(&store, notice_id).await;
                    assert!(matches!(
                        recovered.delivery,
                        SupervisorNoticeDelivery::RetryPending { attempts: 1, .. }
                    ));
                    dispatch_count.fetch_add(1, Ordering::SeqCst);
                    fail_attempt(&store, notice_id, "fake delivery failure").await
                }
            })
            .await;

        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        assert!(matches!(
            read_notice(&store, notice.id).await.delivery,
            SupervisorNoticeDelivery::RetryPending { attempts: 2, .. }
        ));
        store.stop(None);
    }

    #[tokio::test]
    async fn persisted_attempt_budget_stops_delivery_after_three_failures() {
        let (store, notice, _directory) = seeded_store().await;
        let first_worker = DeliveryWorker::new(store.clone(), zero_retry_policy());
        let calls = Arc::new(AtomicUsize::new(0));
        let first_calls = Arc::clone(&calls);
        let first_store = store.clone();
        let mut first_worker = first_worker;

        for _ in 0..2 {
            let store = first_store.clone();
            let calls = Arc::clone(&first_calls);
            first_worker
                .tick_with(move |notice_id| {
                    let store = store.clone();
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        fail_attempt(&store, notice_id, "temporary failure").await
                    }
                })
                .await;
        }
        assert!(matches!(
            read_notice(&store, notice.id).await.delivery,
            SupervisorNoticeDelivery::RetryPending { attempts: 2, .. }
        ));

        let mut restarted_worker = DeliveryWorker::new(store.clone(), zero_retry_policy());
        let store_for_retry = store.clone();
        let calls_for_retry = Arc::clone(&calls);
        restarted_worker
            .tick_with(move |notice_id| {
                let store = store_for_retry.clone();
                let calls = Arc::clone(&calls_for_retry);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    fail_attempt(&store, notice_id, "final failure").await
                }
            })
            .await;
        assert!(matches!(
            read_notice(&store, notice.id).await.delivery,
            SupervisorNoticeDelivery::Failed {
                attempts: 3,
                ref last_error,
            } if last_error == "final failure"
        ));

        let not_retried = Arc::new(AtomicUsize::new(0));
        let not_retried_counter = Arc::clone(&not_retried);
        restarted_worker
            .tick_with(move |_| {
                let not_retried_counter = Arc::clone(&not_retried_counter);
                async move {
                    not_retried_counter.fetch_add(1, Ordering::SeqCst);
                    Err(ResourceNoticeSendError::ReservationMismatch)
                }
            })
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(not_retried.load(Ordering::SeqCst), 0);
        assert!(pending_notices(&store).await.is_empty());
        store.stop(None);
    }

    #[tokio::test]
    async fn concurrent_dispatches_share_one_notice_guard() {
        let guard = InFlight::default();
        let notice_id = NoticeId::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let first_calls = Arc::clone(&calls);
        let second_calls = Arc::clone(&calls);

        let (first, second) = tokio::join!(
            guard.deliver_if_idle(notice_id, || async {
                first_calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }),
            guard.deliver_if_idle(notice_id, || async {
                second_calls.fetch_add(1, Ordering::SeqCst);
            })
        );

        assert!(first.is_some());
        assert!(second.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    fn zero_retry_policy() -> RetryPolicy {
        RetryPolicy::new(Duration::ZERO, Duration::ZERO)
    }

    async fn fail_attempt(
        store: &ActorRef<StoreMsg>,
        notice_id: NoticeId,
        error: &str,
    ) -> Result<ResourceNoticeDeliveryOutcome, ResourceNoticeSendError> {
        let attempt_id = DeliveryAttemptId::new();
        let reserved = call(store, |reply| StoreMsg::ReserveSupervisorNoticeAttempt {
            notice_id,
            attempt_id,
            reply,
        })
        .await??;
        if !matches!(
            reserved.delivery,
            SupervisorNoticeDelivery::Sending {
                attempt_id: reserved_attempt,
                ..
            } if reserved_attempt == attempt_id
        ) {
            return Err(ResourceNoticeSendError::ReservationMismatch);
        }

        let settled = call(store, |reply| StoreMsg::SettleSupervisorNoticeAttempt {
            notice_id,
            attempt_id,
            result: Err(error.to_owned()),
            reply,
        })
        .await??;
        Ok(ResourceNoticeDeliveryOutcome {
            notice: settled,
            receipt: None,
        })
    }

    async fn reserve_attempt(store: &ActorRef<StoreMsg>, notice_id: NoticeId) {
        let attempt_id = DeliveryAttemptId::new();
        call(store, |reply| StoreMsg::ReserveSupervisorNoticeAttempt {
            notice_id,
            attempt_id,
            reply,
        })
        .await
        .unwrap()
        .unwrap();
    }

    async fn read_notice(store: &ActorRef<StoreMsg>, notice_id: NoticeId) -> SupervisorNotice {
        call(store, |reply| StoreMsg::SupervisorNotice {
            notice_id,
            reply,
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    }

    async fn pending_notices(store: &ActorRef<StoreMsg>) -> Vec<SupervisorNotice> {
        call(store, |reply| StoreMsg::PendingSupervisorNotices { reply })
            .await
            .unwrap()
            .unwrap()
    }

    async fn seeded_store() -> (ActorRef<StoreMsg>, SupervisorNotice, TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("homebased.sqlite");
        drop(Store::open(&path).unwrap());

        let authority = MachineId::new();
        let resource_id = ResourceId::new();
        let notice = SupervisorNotice {
            id: NoticeId::new(),
            loan_id: LoanId::new(),
            action_id: ActionId::new(),
            state_revision: ResourceRevision::new(2),
            destination: SupervisorAddress {
                machine: authority,
                thread: ThreadId(Uuid::now_v7()),
            },
            assignment_revision: AssignmentRevision::new(1),
            payload: SupervisorNoticePayload::AttentionRequired {
                reason: "test notice".into(),
            },
            delivery: SupervisorNoticeDelivery::Pending { attempts: 0 },
        };
        let mut connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO resources (
                    id, display_name, authority_machine, supervisor_machine,
                    supervisor_thread, assignment_revision, state_revision,
                    registered_background_task
                ) VALUES (?1, 'test resource', ?2, ?2, ?3, 1, 2, NULL)",
                params![
                    resource_id.as_uuid().to_string(),
                    authority.as_uuid().to_string(),
                    notice.destination.thread.to_string(),
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, '{\"type\":\"active\"}')",
                params![
                    notice.loan_id.as_uuid().to_string(),
                    resource_id.as_uuid().to_string(),
                ],
            )
            .unwrap();
        let tx = connection.transaction().unwrap();
        insert_supervisor_notice_in_transaction(&tx, &notice).unwrap();
        tx.commit().unwrap();
        drop(connection);

        let (store, _handle) = Actor::spawn(None, StoreActor, path).await.unwrap();
        assert_eq!(read_notice(&store, notice.id).await, notice);

        (store, notice, directory)
    }
}
