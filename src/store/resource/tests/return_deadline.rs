//! Return decision windows: queued work after the deadline, holds, and upgrades

use std::time::Duration;

use chrono::{DateTime, Utc};

use super::fixtures::{
    ServingFixture, accept_and_finish_resource_task, completion, refresh_serving_fixture,
};
use super::restore::{
    accept_post_return_request, awaiting_return_fixture, saved_loan, saved_resource,
};
use crate::domain::{ExitReason, ProcessGroupExitEvidence};
use crate::resource::store::{
    ResourceTaskCompletionResult, ReturnDeadlineOutcome, open_missing_return_windows_on,
    return_window_on, serve_after_return_deadline_for_authority,
};
use crate::resource::{
    ActionId, LoanPhase, LoanState, RETURN_DECISION_GRACE, RETURN_DECISION_LIMIT,
    ResourceRequestState, ReturnContext, ReturnDecisionWindow, ServingReleaseProvenance,
    SupervisorActionAuthority,
};
use crate::store::ReturnDecisionError;

fn window(fixture: &ServingFixture, action_id: ActionId) -> ReturnDecisionWindow {
    return_window_on(&fixture.store.conn, action_id)
        .unwrap()
        .expect("every return action has a decision window")
}

fn serve_at(fixture: &mut ServingFixture, now: DateTime<Utc>) -> ReturnDeadlineOutcome {
    serve_after_return_deadline_for_authority(
        &mut fixture.store.conn,
        fixture.authority,
        fixture.resource.id,
        now,
    )
    .unwrap()
}

fn awaited_return(fixture: &ServingFixture) -> (ActionId, ReturnContext) {
    match saved_loan(&fixture.store, fixture.authority).map(|loan| loan.state) {
        Some(LoanState::Active {
            phase:
                LoanPhase::AwaitingReturn {
                    action_id,
                    return_context,
                },
        }) => (action_id, return_context),
        other => panic!("the loan must await a return decision, found {other:?}"),
    }
}

#[test]
fn a_return_action_opens_its_window_with_the_default_grace() {
    let (fixture, authority) = awaiting_return_fixture();

    let window = window(&fixture, authority.action_id);

    assert_eq!(window.loan_id(), authority.loan_id);
    assert_eq!(window.resource_id(), authority.resource_id);
    assert_eq!(
        window.deadline_at(),
        window.opened_at() + RETURN_DECISION_GRACE
    );
}

#[test]
fn queued_work_takes_the_resource_only_after_the_window_closes() {
    let (mut fixture, authority) = awaiting_return_fixture();
    let (_, return_context) = awaited_return(&fixture);
    let later = accept_post_return_request(&mut fixture);
    let window = window(&fixture, authority.action_id);

    let before = window.deadline_at() - chrono::Duration::seconds(1);
    assert!(matches!(
        serve_at(&mut fixture, before),
        ReturnDeadlineOutcome::Open { window: open } if open == window
    ));
    assert_eq!(awaited_return(&fixture).0, authority.action_id);

    let ReturnDeadlineOutcome::Served { loan, request } =
        serve_at(&mut fixture, window.deadline_at())
    else {
        panic!("an expired window with queued work must serve it");
    };
    assert_eq!(loan.id, authority.loan_id);
    assert_eq!(request.request_id, later.request_id);
    assert_eq!(
        request.state,
        ResourceRequestState::Assigned { loan_id: loan.id }
    );
    assert_eq!(
        loan.state,
        LoanState::Active {
            phase: LoanPhase::Serving {
                return_context: return_context.clone(),
                current_request_id: later.request_id,
                release_provenance: ServingReleaseProvenance::ReturnDeadlinePassed {
                    action_id: authority.action_id,
                },
            },
        }
    );
    let resource = saved_resource(&fixture.store, fixture.authority);
    assert_eq!(
        resource.state_revision,
        authority.expected_state_revision.next().unwrap()
    );

    // the supervisor's late decision names an action that no longer waits
    let late = fixture
        .store
        .record_no_resume_for_authority(authority, "too late".into())
        .unwrap_err();
    assert!(matches!(late, ReturnDecisionError::ActionNotPending { .. }));

    // the deadline receipt proves the release, so the served task starts and the
    // next drained queue asks for the same return again
    refresh_serving_fixture(&mut fixture, *loan, *request);
    let input = accept_and_finish_resource_task(
        &mut fixture,
        ExitReason::Exit { code: 0 },
        ProcessGroupExitEvidence::ConfirmedExited,
    );
    let result = fixture
        .store
        .reconcile_assigned_resource_task_for_authority(input)
        .unwrap();
    let Ok(ResourceTaskCompletionResult::ReturnRequired { loan, notice, .. }) = completion(result)
    else {
        panic!("the drained queue must ask for the return again");
    };
    assert_eq!(loan.id, authority.loan_id);
    assert_ne!(notice.action_id, authority.action_id);
    assert_eq!(awaited_return(&fixture), (notice.action_id, return_context));
    assert!(
        return_window_on(&fixture.store.conn, notice.action_id)
            .unwrap()
            .is_some()
    );
}

#[test]
fn an_expired_window_without_queued_work_keeps_the_return_pending() {
    let (mut fixture, authority) = awaiting_return_fixture();
    let window = window(&fixture, authority.action_id);

    assert!(matches!(
        serve_at(&mut fixture, window.limit_at()),
        ReturnDeadlineOutcome::NoQueuedRequest
    ));

    assert_eq!(awaited_return(&fixture).0, authority.action_id);
    fixture
        .store
        .record_no_resume_for_authority(authority, "not needed".into())
        .unwrap();
    assert!(matches!(
        serve_at(&mut fixture, window.limit_at()),
        ReturnDeadlineOutcome::NotAwaiting
    ));
}

#[test]
fn a_hold_keeps_queued_work_waiting_without_changing_the_pending_action() {
    let (mut fixture, authority) = awaiting_return_fixture();
    accept_post_return_request(&mut fixture);
    let opened = window(&fixture, authority.action_id);

    let held = fixture
        .store
        .hold_return_for_authority(authority, Duration::from_secs(5 * 60))
        .unwrap();

    assert!(held.deadline_at() > opened.deadline_at());
    assert!(held.deadline_at() <= opened.opened_at() + RETURN_DECISION_LIMIT);
    assert_eq!(window(&fixture, authority.action_id), held);
    assert_eq!(
        saved_resource(&fixture.store, fixture.authority).state_revision,
        authority.expected_state_revision
    );
    assert!(matches!(
        serve_at(&mut fixture, opened.deadline_at()),
        ReturnDeadlineOutcome::Open { .. }
    ));

    // the pending action is still decided with the same saved authority
    fixture
        .store
        .record_no_resume_for_authority(authority, "not needed".into())
        .unwrap();
}

#[test]
fn a_hold_needs_the_exact_pending_action() {
    let (mut fixture, authority) = awaiting_return_fixture();
    let other = SupervisorActionAuthority {
        action_id: ActionId::new(),
        ..authority
    };

    let error = fixture
        .store
        .hold_return_for_authority(other, Duration::from_secs(60))
        .unwrap_err();

    assert!(matches!(
        error,
        ReturnDecisionError::ActionNotPending { .. }
    ));
    let error = fixture
        .store
        .hold_return_for_authority(authority, Duration::ZERO)
        .unwrap_err();
    assert!(matches!(error, ReturnDecisionError::HoldRejected(_)));
}

#[test]
fn an_upgrade_opens_a_full_window_for_a_return_saved_without_one() {
    let (fixture, authority) = awaiting_return_fixture();
    fixture
        .store
        .conn
        .execute("DELETE FROM resource_return_windows", [])
        .unwrap();
    let upgraded_at = Utc::now();

    open_missing_return_windows_on(&fixture.store.conn, upgraded_at).unwrap();

    let window = window(&fixture, authority.action_id);
    assert_eq!(window.opened_at(), upgraded_at);
    assert_eq!(window.loan_id(), authority.loan_id);
    assert_eq!(window.deadline_at(), upgraded_at + RETURN_DECISION_GRACE);
}
