//! One authority-side delivery attempt for a durable supervisor notice.

use axum::http::StatusCode;
use std::future::Future;

use crate::daemon::AppState;
use crate::daemon::actors::{StoreMsg, call};
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::http::ClusterClient;
use crate::fleet::protocol::CLUSTER_PROTOCOL_VERSION;
use crate::machine::MachineId;
use crate::resource::store::SupervisorNoticeStoreError;
use crate::resource::{
    DeliveryAttemptId, NoticeId, SupervisorNotice, SupervisorNoticeDelivery,
    SupervisorNoticeReceipt, SupervisorNoticeRequest, SupervisorNoticeResponse,
};

const RESOURCE_NOTICE_PATH: &str = "/v1/cluster/resource-notices";

/// Settled delivery state and the verified receiver receipt, when delivery succeeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResourceNoticeDeliveryOutcome {
    /// Notice after this attempt was settled by the StoreActor.
    pub notice: SupervisorNotice,
    /// Receipt returned by the exact local or remote destination.
    pub receipt: Option<SupervisorNoticeReceipt>,
}

/// The attempt could not be reserved or its settlement could not be persisted.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResourceNoticeSendError {
    /// Actor transport failed.
    #[error(transparent)]
    Actor(#[from] AppError),
    /// Durable notice state rejected the operation.
    #[error(transparent)]
    Store(#[from] SupervisorNoticeStoreError),
    /// The StoreActor did not return the attempt reserved by this call.
    #[error("reserved supervisor notice attempt does not match its identity")]
    ReservationMismatch,
}

/// Reserve and settle at most one supervisor-notice delivery attempt.
pub(crate) async fn deliver_one(
    state: &AppState,
    notice_id: NoticeId,
) -> Result<ResourceNoticeDeliveryOutcome, ResourceNoticeSendError> {
    reserve_deliver_settle(&state.store, notice_id, |notice, attempt_id| async move {
        deliver_reserved(state, &notice, attempt_id).await
    })
    .await
}

/// Deliver and settle one attempt that an explicit renotify already reserved
///
/// Returns `None` without sending when the notice no longer holds that exact
/// reservation, so a replayed operation never sends a second copy
pub(crate) async fn deliver_reserved_attempt(
    state: &AppState,
    notice_id: NoticeId,
    attempt_id: DeliveryAttemptId,
) -> Result<Option<ResourceNoticeDeliveryOutcome>, ResourceNoticeSendError> {
    let notice = call(&state.store, |reply| StoreMsg::SupervisorNotice {
        notice_id,
        reply,
    })
    .await??
    .ok_or(ResourceNoticeSendError::Store(
        SupervisorNoticeStoreError::NotFound,
    ))?;
    if !matches!(
        notice.delivery,
        SupervisorNoticeDelivery::Sending {
            attempt_id: reserved,
            ..
        } if reserved == attempt_id
    ) {
        return Ok(None);
    }
    let delivery = deliver_reserved(state, &notice, attempt_id).await;
    let (receipt, result) = match delivery {
        Ok(receipt) => (Some(receipt), Ok(())),
        Err(error) => (None, Err(error)),
    };
    let notice = call(&state.store, |reply| {
        StoreMsg::SettleSupervisorNoticeAttempt {
            notice_id,
            attempt_id,
            result,
            reply,
        }
    })
    .await??;
    Ok(Some(ResourceNoticeDeliveryOutcome { notice, receipt }))
}

async fn reserve_deliver_settle<F, Fut>(
    store: &ractor::ActorRef<StoreMsg>,
    notice_id: NoticeId,
    deliver: F,
) -> Result<ResourceNoticeDeliveryOutcome, ResourceNoticeSendError>
where
    F: FnOnce(SupervisorNotice, DeliveryAttemptId) -> Fut,
    Fut: Future<Output = Result<SupervisorNoticeReceipt, String>>,
{
    let attempt_id = DeliveryAttemptId::new();
    let notice = call(store, |reply| StoreMsg::ReserveSupervisorNoticeAttempt {
        notice_id,
        attempt_id,
        reply,
    })
    .await??;
    if !matches!(
        notice.delivery,
        SupervisorNoticeDelivery::Sending {
            attempt_id: reserved,
            ..
        } if reserved == attempt_id
    ) {
        return Err(ResourceNoticeSendError::ReservationMismatch);
    }

    let delivery = deliver(notice, attempt_id).await;
    let (receipt, result) = match delivery {
        Ok(receipt) => (Some(receipt), Ok(())),
        Err(error) => (None, Err(error)),
    };
    let notice = call(store, |reply| StoreMsg::SettleSupervisorNoticeAttempt {
        notice_id,
        attempt_id,
        result,
        reply,
    })
    .await??;

    Ok(ResourceNoticeDeliveryOutcome { notice, receipt })
}

async fn deliver_reserved(
    state: &AppState,
    notice: &SupervisorNotice,
    attempt_id: DeliveryAttemptId,
) -> Result<SupervisorNoticeReceipt, String> {
    let destination = notice.destination.machine;
    let local = state.machine.identity.machine;
    if destination == local {
        let request = notice_request(notice, attempt_id, local, CLUSTER_PROTOCOL_VERSION.0)
            .map_err(|error| error.to_string())?;
        let receipt = state
            .message_receiver
            .receive_supervisor_notice(state, request.clone())
            .await
            .map_err(|error| error.to_string())?;
        return verify_receipt(receipt, &request);
    }

    let fleet = state
        .fleet
        .handle()
        .ok_or_else(|| "Fleet is disabled for the remote supervisor destination".to_owned())?;
    let verified = fleet
        .connect(destination)
        .await
        .map_err(|error| error.to_string())?;
    let request = notice_request(notice, attempt_id, local, verified.protocol.0)
        .map_err(|error| error.to_string())?;
    post_remote_notice(&ClusterClient::default(), &verified.address, &request).await
}

fn notice_request(
    notice: &SupervisorNotice,
    attempt_id: DeliveryAttemptId,
    source_machine: MachineId,
    protocol_version: u32,
) -> Result<SupervisorNoticeRequest, AppError> {
    let request = SupervisorNoticeRequest {
        api_version: API_VERSION,
        protocol_version,
        source_machine,
        destination: notice.destination,
        notice_id: notice.id,
        loan_id: notice.loan_id,
        action_id: notice.action_id,
        state_revision: notice.state_revision,
        assignment_revision: notice.assignment_revision,
        attempt_id,
        payload: notice.payload.clone(),
    };
    request.validate()?;
    Ok(request)
}

fn decode_remote_receipt(
    status: StatusCode,
    body: &[u8],
    request: &SupervisorNoticeRequest,
) -> Result<SupervisorNoticeReceipt, String> {
    if status != StatusCode::OK {
        return Err(format!("resource notice receiver returned HTTP {status}"));
    }
    let response: SupervisorNoticeResponse = serde_json::from_slice(body).map_err(|error| {
        format!("resource notice receiver returned an invalid response: {error}")
    })?;
    if response.api_version != API_VERSION
        || response.protocol_version != request.protocol_version
        || response.destination_machine != request.destination.machine
    {
        return Err(
            "resource notice response does not match the requested destination or version".into(),
        );
    }
    verify_receipt(response.receipt, request)
}

async fn post_remote_notice(
    client: &ClusterClient,
    address: &crate::fleet::address::MachineAddress,
    request: &SupervisorNoticeRequest,
) -> Result<SupervisorNoticeReceipt, String> {
    let response = client
        .post_json(address, RESOURCE_NOTICE_PATH, request)
        .await
        .map_err(|error| error.to_string())?;
    decode_remote_receipt(response.status, &response.body, request)
}

fn verify_receipt(
    receipt: SupervisorNoticeReceipt,
    request: &SupervisorNoticeRequest,
) -> Result<SupervisorNoticeReceipt, String> {
    if receipt.api_version != API_VERSION
        || receipt.protocol_version != request.protocol_version
        || receipt.destination_machine != request.destination.machine
        || receipt.notice_id != request.notice_id
        || receipt.attempt_id != request.attempt_id
        || receipt.destination_thread != request.destination.thread
    {
        return Err("resource notice receipt does not match the reserved attempt".into());
    }
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::actors::StoreMsg;
    use crate::daemon::actors::{StoreActor, call};
    use crate::domain::ThreadId;
    use crate::fleet::address::MachineAddress;
    use crate::machine::MachineId;
    use crate::resource::store::insert_supervisor_notice_in_transaction;
    use crate::resource::{
        ActionId, AssignmentRevision, LoanId, NoticeId, ResourceId, ResourceRevision,
        SupervisorAddress, SupervisorNotice, SupervisorNoticeDelivery, SupervisorNoticePayload,
    };
    use crate::store::Store;
    use axum::Json;
    use axum::routing::post;
    use chrono::Utc;
    use ractor::Actor;
    use rusqlite::{Connection, params};
    use tempfile::TempDir;
    use uuid::Uuid;

    fn request() -> SupervisorNoticeRequest {
        SupervisorNoticeRequest {
            api_version: API_VERSION,
            protocol_version: CLUSTER_PROTOCOL_VERSION.0,
            source_machine: MachineId::new(),
            destination: SupervisorAddress {
                machine: MachineId::new(),
                thread: ThreadId(Uuid::now_v7()),
            },
            notice_id: NoticeId::new(),
            loan_id: LoanId::new(),
            action_id: ActionId::new(),
            state_revision: ResourceRevision::new(2),
            assignment_revision: AssignmentRevision::new(1),
            attempt_id: DeliveryAttemptId::new(),
            payload: SupervisorNoticePayload::AttentionRequired {
                reason: "test notice".into(),
            },
        }
    }

    fn receipt(request: &SupervisorNoticeRequest) -> SupervisorNoticeReceipt {
        SupervisorNoticeReceipt {
            api_version: API_VERSION,
            protocol_version: request.protocol_version,
            notice_id: request.notice_id,
            attempt_id: request.attempt_id,
            destination_machine: request.destination.machine,
            destination_thread: request.destination.thread,
            delivered_at: Utc::now(),
        }
    }

    fn response(request: &SupervisorNoticeRequest) -> SupervisorNoticeResponse {
        SupervisorNoticeResponse {
            api_version: API_VERSION,
            protocol_version: request.protocol_version,
            destination_machine: request.destination.machine,
            receipt: receipt(request),
        }
    }

    fn assert_same_receipt(actual: &SupervisorNoticeReceipt, expected: &SupervisorNoticeReceipt) {
        assert_eq!(actual.api_version, expected.api_version);
        assert_eq!(actual.protocol_version, expected.protocol_version);
        assert_eq!(actual.notice_id, expected.notice_id);
        assert_eq!(actual.attempt_id, expected.attempt_id);
        assert_eq!(actual.destination_machine, expected.destination_machine);
        assert_eq!(actual.destination_thread, expected.destination_thread);
    }

    #[test]
    fn accepts_a_matching_remote_receipt() {
        let request = request();
        let response = serde_json::to_vec(&response(&request)).unwrap();
        let decoded = decode_remote_receipt(StatusCode::OK, &response, &request).unwrap();
        assert_same_receipt(&decoded, &receipt(&request));
    }

    #[test]
    fn rejects_wrong_machine_thread_notice_and_attempt_receipts() {
        let request = request();
        let mut cases = Vec::new();

        let mut wrong_machine = response(&request);
        wrong_machine.receipt.destination_machine = MachineId::new();
        cases.push(wrong_machine);

        let mut wrong_thread = response(&request);
        wrong_thread.receipt.destination_thread = ThreadId(Uuid::now_v7());
        cases.push(wrong_thread);

        let mut wrong_notice = response(&request);
        wrong_notice.receipt.notice_id = NoticeId::new();
        cases.push(wrong_notice);

        let mut wrong_attempt = response(&request);
        wrong_attempt.receipt.attempt_id = DeliveryAttemptId::new();
        cases.push(wrong_attempt);

        for response in cases {
            let body = serde_json::to_vec(&response).unwrap();
            assert!(decode_remote_receipt(StatusCode::OK, &body, &request).is_err());
        }
    }

    #[test]
    fn rejects_invalid_remote_responses_as_uncertain_delivery() {
        let request = request();
        assert!(decode_remote_receipt(StatusCode::OK, b"{}", &request).is_err());
        assert!(decode_remote_receipt(StatusCode::SERVICE_UNAVAILABLE, b"{}", &request).is_err());
    }

    #[tokio::test]
    async fn posts_to_a_fake_remote_notice_endpoint_and_accepts_its_receipt() {
        async fn fake_receiver(
            Json(request): Json<SupervisorNoticeRequest>,
        ) -> Json<SupervisorNoticeResponse> {
            Json(response(&request))
        }

        let request = request();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address: MachineAddress = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().route(RESOURCE_NOTICE_PATH, post(fake_receiver)),
            )
            .await
            .unwrap();
        });

        let delivered = post_remote_notice(&ClusterClient::default(), &address, &request)
            .await
            .unwrap();
        assert_same_receipt(&delivered, &receipt(&request));
        server.abort();
    }

    #[tokio::test]
    async fn local_queue_receipt_settles_the_reserved_attempt_as_delivered() {
        let (store, notice, _directory) = seeded_store().await;
        let local = notice.destination.machine;
        let expected_notice = notice.clone();

        let outcome = reserve_deliver_settle(&store, notice.id, |saved, attempt_id| async move {
            assert_eq!(saved.id, expected_notice.id);
            assert_eq!(saved.loan_id, expected_notice.loan_id);
            assert_eq!(saved.action_id, expected_notice.action_id);
            assert_eq!(saved.state_revision, expected_notice.state_revision);
            assert_eq!(saved.destination, expected_notice.destination);
            assert_eq!(
                saved.assignment_revision,
                expected_notice.assignment_revision
            );
            assert_eq!(saved.payload, expected_notice.payload);
            assert!(matches!(
                saved.delivery,
                SupervisorNoticeDelivery::Sending { attempt_id: reserved, .. }
                    if reserved == attempt_id
            ));
            let request = notice_request(&saved, attempt_id, local, CLUSTER_PROTOCOL_VERSION.0)
                .map_err(|error| error.to_string())?;
            let fake_queue_receipt = receipt(&request);
            verify_receipt(fake_queue_receipt, &request)
        })
        .await
        .unwrap();

        let receipt = outcome.receipt.as_ref().unwrap();
        assert_eq!(receipt.notice_id, notice.id);
        assert_eq!(receipt.destination_machine, local);
        assert_eq!(receipt.destination_thread, notice.destination.thread);
        assert!(matches!(
            outcome.notice.delivery,
            SupervisorNoticeDelivery::Delivered { attempts: 1 }
        ));
        store.stop(None);
    }

    #[tokio::test]
    async fn delivery_failure_is_settled_without_a_receipt() {
        let (store, notice, _directory) = seeded_store().await;

        let outcome = reserve_deliver_settle(&store, notice.id, |saved, attempt_id| async move {
            assert!(matches!(
                saved.delivery,
                SupervisorNoticeDelivery::Sending { attempt_id: reserved, .. }
                    if reserved == attempt_id
            ));
            Err("fake queue endpoint unavailable".into())
        })
        .await
        .unwrap();

        assert!(outcome.receipt.is_none());
        assert!(matches!(
            outcome.notice.delivery,
            SupervisorNoticeDelivery::RetryPending {
                attempts: 1,
                ref last_error,
            } if last_error == "fake queue endpoint unavailable"
        ));
        store.stop(None);
    }

    async fn seeded_store() -> (ractor::ActorRef<StoreMsg>, SupervisorNotice, TempDir) {
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
        let saved = call(&store, |reply| StoreMsg::SupervisorNotice {
            notice_id: notice.id,
            reply,
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(saved, notice);

        (store, notice, directory)
    }
}
