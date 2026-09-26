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
///
/// The fields are private so every window keeps its deadline between its
/// opening and its hard limit, including a window decoded from saved JSON
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SavedReturnDecisionWindow")]
pub struct ReturnDecisionWindow {
    action_id: ActionId,
    loan_id: LoanId,
    resource_id: ResourceId,
    opened_at: DateTime<Utc>,
    deadline_at: DateTime<Utc>,
}

/// Unchecked JSON shape of a saved window
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedReturnDecisionWindow {
    action_id: ActionId,
    loan_id: LoanId,
    resource_id: ResourceId,
    opened_at: DateTime<Utc>,
    deadline_at: DateTime<Utc>,
}

/// A saved window whose deadline is outside its opening and hard limit
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("return decision window deadline is outside its opening and {limit_minutes} minute limit")]
pub struct InvalidReturnDecisionWindow {
    limit_minutes: u64,
}

impl TryFrom<SavedReturnDecisionWindow> for ReturnDecisionWindow {
    type Error = InvalidReturnDecisionWindow;

    fn try_from(saved: SavedReturnDecisionWindow) -> Result<Self, Self::Error> {
        let window = Self {
            action_id: saved.action_id,
            loan_id: saved.loan_id,
            resource_id: saved.resource_id,
            opened_at: saved.opened_at,
            deadline_at: saved.deadline_at,
        };
        if window.deadline_at < window.opened_at || window.deadline_at > window.limit_at() {
            return Err(InvalidReturnDecisionWindow {
                limit_minutes: limit_minutes(),
            });
        }
        Ok(window)
    }
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

    /// Return action that this window belongs to
    #[must_use]
    pub const fn action_id(&self) -> ActionId {
        self.action_id
    }

    /// Loan that awaits the return decision
    #[must_use]
    pub const fn loan_id(&self) -> LoanId {
        self.loan_id
    }

    /// Resource reserved for the decision
    #[must_use]
    pub const fn resource_id(&self) -> ResourceId {
        self.resource_id
    }

    /// When the queue drained and the return action opened
    #[must_use]
    pub const fn opened_at(&self) -> DateTime<Utc> {
        self.opened_at
    }

    /// When queued work may take the resource if no decision exists
    #[must_use]
    pub const fn deadline_at(&self) -> DateTime<Utc> {
        self.deadline_at
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
                limit_minutes: limit_minutes(),
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

fn limit_minutes() -> u64 {
    RETURN_DECISION_LIMIT.as_secs() / 60
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
    fn a_saved_window_with_a_deadline_past_its_limit_is_refused() {
        let saved = serde_json::to_value(window(Utc::now())).unwrap();
        let opened_at = saved["opened_at"]
            .as_str()
            .unwrap()
            .parse::<DateTime<Utc>>()
            .unwrap();
        let mut past_limit = saved.clone();
        past_limit["deadline_at"] = serde_json::json!(opened_at + chrono::Duration::minutes(11));
        let mut before_opening = saved.clone();
        before_opening["deadline_at"] = serde_json::json!(opened_at - chrono::Duration::seconds(1));

        assert!(serde_json::from_value::<ReturnDecisionWindow>(saved).is_ok());
        assert!(serde_json::from_value::<ReturnDecisionWindow>(past_limit).is_err());
        assert!(serde_json::from_value::<ReturnDecisionWindow>(before_opening).is_err());
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
