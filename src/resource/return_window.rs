//! Time the supervisor has to decide a return before queued work takes the resource
//!
//! A drained queue reserves the resource for the supervisor's return decision.
//! The reservation must not hold queued GPU work for a supervisor that does not
//! answer, so every return action opens a decision window. When the window
//! closes with a request queued, the authority serves that request from the
//! same loan and keeps the return obligation for the next drained queue. The
//! supervisor can hold the window open, but never past its hard limit

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ActionId, LoanId, ResourceId};

/// Decision time a return action gets before queued work can take the resource
pub const RETURN_DECISION_GRACE: Duration = Duration::from_secs(2 * 60);

/// Most decision time any hold can give a return action, counted from its opening
pub const RETURN_DECISION_LIMIT: Duration = Duration::from_secs(10 * 60);

/// Saved decision window of one return action
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReturnDecisionWindow {
    /// Return action that this window belongs to
    pub action_id: ActionId,
    /// Loan that awaits the return decision
    pub loan_id: LoanId,
    /// Resource reserved for the decision
    pub resource_id: ResourceId,
    /// When the queue drained and the return action opened
    pub opened_at: DateTime<Utc>,
    /// When queued work may take the resource if no decision exists
    pub deadline_at: DateTime<Utc>,
}

/// Why a hold cannot move the deadline of a return action
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReturnHoldRejection {
    /// A hold must ask for a positive duration
    #[error("a return hold must be longer than zero")]
    Empty,
    /// The window already reached its hard limit
    #[error("the return decision window already reached its {limit_minutes} minute limit")]
    LimitReached {
        /// Hard limit of every decision window, in minutes
        limit_minutes: u64,
    },
}

impl ReturnDecisionWindow {
    /// Open the window of a return action at `opened_at` with the default grace
    #[must_use]
    pub fn open(
        action_id: ActionId,
        loan_id: LoanId,
        resource_id: ResourceId,
        opened_at: DateTime<Utc>,
    ) -> Self {
        Self {
            action_id,
            loan_id,
            resource_id,
            opened_at,
            deadline_at: opened_at + RETURN_DECISION_GRACE,
        }
    }

    /// Latest deadline that any hold can set
    #[must_use]
    pub fn limit_at(&self) -> DateTime<Utc> {
        self.opened_at + RETURN_DECISION_LIMIT
    }

    /// Whether queued work may take the resource at `now`
    #[must_use]
    pub fn expired_at(&self, now: DateTime<Utc>) -> bool {
        now >= self.deadline_at
    }

    /// Time left before the deadline, or zero when it passed
    #[must_use]
    pub fn remaining_at(&self, now: DateTime<Utc>) -> Duration {
        (self.deadline_at - now).to_std().unwrap_or(Duration::ZERO)
    }

    /// Move the deadline to `now + hold`, capped at the hard limit
    ///
    /// A hold never shortens the window, so a short hold after a longer one
    /// keeps the longer deadline
    pub fn hold(self, now: DateTime<Utc>, hold: Duration) -> Result<Self, ReturnHoldRejection> {
        if hold.is_zero() {
            return Err(ReturnHoldRejection::Empty);
        }
        let limit_at = self.limit_at();
        if self.deadline_at >= limit_at || now >= limit_at {
            return Err(ReturnHoldRejection::LimitReached {
                limit_minutes: RETURN_DECISION_LIMIT.as_secs() / 60,
            });
        }
        // a hold longer than the chrono range is capped by the limit anyway
        let requested = chrono::Duration::from_std(hold)
            .ok()
            .and_then(|hold| now.checked_add_signed(hold))
            .unwrap_or(limit_at);
        Ok(Self {
            deadline_at: requested.min(limit_at).max(self.deadline_at),
            ..self
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(opened_at: DateTime<Utc>) -> ReturnDecisionWindow {
        ReturnDecisionWindow::open(ActionId::new(), LoanId::new(), ResourceId::new(), opened_at)
    }

    #[test]
    fn a_new_window_expires_after_the_default_grace() {
        let opened_at = Utc::now();
        let window = window(opened_at);

        assert!(!window.expired_at(opened_at + chrono::Duration::seconds(119)));
        assert!(window.expired_at(opened_at + chrono::Duration::seconds(120)));
    }

    #[test]
    fn a_hold_extends_from_now_but_never_past_the_limit() {
        let opened_at = Utc::now();
        let now = opened_at + chrono::Duration::minutes(1);

        let held = window(opened_at)
            .hold(now, Duration::from_secs(5 * 60))
            .unwrap();
        assert_eq!(held.deadline_at, now + chrono::Duration::minutes(5));

        let capped = held.hold(now, Duration::from_secs(60 * 60)).unwrap();
        assert_eq!(
            capped.deadline_at,
            opened_at + chrono::Duration::minutes(10)
        );
        assert_eq!(
            capped.hold(now, Duration::from_secs(60)),
            Err(ReturnHoldRejection::LimitReached { limit_minutes: 10 })
        );
    }

    #[test]
    fn a_short_hold_keeps_a_longer_deadline() {
        let opened_at = Utc::now();
        let held = window(opened_at)
            .hold(opened_at, Duration::from_secs(8 * 60))
            .unwrap();

        let shorter = held.hold(opened_at, Duration::from_secs(60)).unwrap();

        assert_eq!(shorter.deadline_at, held.deadline_at);
    }

    #[test]
    fn a_hold_after_the_limit_is_refused() {
        let opened_at = Utc::now();

        assert_eq!(
            window(opened_at).hold(
                opened_at + chrono::Duration::minutes(11),
                Duration::from_secs(60)
            ),
            Err(ReturnHoldRejection::LimitReached { limit_minutes: 10 })
        );
        assert_eq!(
            window(opened_at).hold(opened_at, Duration::ZERO),
            Err(ReturnHoldRejection::Empty)
        );
    }
}
