//! Child threads: top-level T3 threads that another T3 thread launched with
//! `homebased t3 launch --child` to own a phase, an experiment, or a long run
//!
//! T3 records no lineage for a launched thread, so homebased keeps its own.
//! The launching daemon saves one [`Lineage`] per child, and a child on
//! another Fleet machine gets the same record there. The parent's machine
//! reads it to list the child, to keep the parent `waiting` while the child
//! owes its report, and to watch it; the child's machine reads it to know
//! where `homebased thread report` goes. A child can launch children of its
//! own, which makes grandchildren with nothing extra
//!
//! A child reports once. `thread report` saves a [`ThreadReport`] on the
//! child's machine before it sends anything, and the message UUID is fixed by
//! the lineage, so a retry or a daemon restart finishes the same report and
//! never sends a second one. The parent's machine counts the child reported
//! once it holds the receipt of that message. The child's machine then
//! settles the child after the run that reported has ended, unless a later
//! run or message kept the thread active

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::{Builder, Uuid};

use crate::domain::{API_VERSION, ThreadId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::message::{MessageId, MessageSendRequest, MessageSourceSelector, MessageTarget};
use crate::thread_ref::Caller;

/// One parent-child link: the request id of the launch that made it, so a
/// retried launch makes the same link
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LinkId(Uuid);

impl LinkId {
    /// The link of the launch with request id `request`
    #[must_use]
    pub const fn from_launch(request: Uuid) -> Self {
        Self(request)
    }

    /// Underlying UUID
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for LinkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for LinkId {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self).map_err(|error| AppError::Internal {
            message: format!("bad link id {value}: {error}"),
        })
    }
}

/// One end of a link: a T3 thread on a Fleet machine
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkEnd {
    /// Machine whose T3 runs the thread
    pub machine: MachineId,
    /// T3 thread id
    pub t3_thread: String,
}

/// A child thread's link to the T3 thread that launched it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lineage {
    /// The launch that made the link
    pub id: LinkId,
    /// The thread that launched the child
    pub parent: LinkEnd,
    /// The launched thread
    pub child: LinkEnd,
    /// When the launching daemon saved the link
    pub created_at: DateTime<Utc>,
}

impl Lineage {
    /// Message UUID of the child's one report
    ///
    /// Derived from the link, so the parent's machine knows it from the
    /// launch and a retried report reuses it with no saved state
    #[must_use]
    pub fn report_message(&self) -> MessageId {
        let mut hasher = Sha256::new();
        hasher.update(b"homebased-child-report\0");
        hasher.update(self.id.as_uuid().as_bytes());
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        MessageId::from_uuid(Builder::from_custom_bytes(bytes).into_uuid())
            // a SHA-256 prefix with version bits set is never nil
            .unwrap_or_default()
    }
}

/// What a link looks like from this machine, for thread lookups
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkView {
    /// The saved link
    pub lineage: Lineage,
    /// Whether this machine holds the receipt of the child's report, which
    /// only the parent's machine can
    pub report_received: bool,
    /// Whether this machine saw the child put away without a report, which
    /// the parent's machine records for a child on another machine
    pub closed: bool,
    /// Where the child's own report stands, which only the child's machine
    /// knows
    pub report: Option<ReportStageKind>,
}

/// How far a child's report has got, on the child's machine
///
/// The message UUID and body never change once saved; only the stage moves,
/// in the order the variants are listed, except that a settle can be skipped
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportStage {
    /// Saved; the message may or may not have reached the parent
    Sending,
    /// Delivered; the thread settles once its reporting run has ended and no
    /// later run or message exists
    SettlePending {
        /// When the receipt came back
        sent_at: DateTime<Utc>,
    },
    /// Delivered, and left active: a later run or message kept it, or T3
    /// refused the settle
    Sent {
        /// When the receipt came back
        sent_at: DateTime<Utc>,
    },
    /// Delivered and settled, or put away by someone else first
    Settled {
        /// When the receipt came back
        sent_at: DateTime<Utc>,
        /// When homebased saw the thread settled
        settled_at: DateTime<Utc>,
    },
}

impl ReportStage {
    /// The stage without its times
    #[must_use]
    pub fn kind(self) -> ReportStageKind {
        match self {
            Self::Sending => ReportStageKind::Sending,
            Self::SettlePending { .. } => ReportStageKind::SettlePending,
            Self::Sent { .. } => ReportStageKind::Sent,
            Self::Settled { .. } => ReportStageKind::Settled,
        }
    }

    /// When the receipt came back, once it did
    #[must_use]
    pub fn sent_at(self) -> Option<DateTime<Utc>> {
        match self {
            Self::Sending => None,
            Self::SettlePending { sent_at }
            | Self::Sent { sent_at }
            | Self::Settled { sent_at, .. } => Some(sent_at),
        }
    }
}

/// [`ReportStage`] without its times, as stored and on the wire
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportStageKind {
    /// See [`ReportStage::Sending`]
    Sending,
    /// See [`ReportStage::SettlePending`]
    SettlePending,
    /// See [`ReportStage::Sent`]
    Sent,
    /// See [`ReportStage::Settled`]
    Settled,
}

impl ReportStageKind {
    /// Lowercase name, as stored
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sending => "sending",
            Self::SettlePending => "settle_pending",
            Self::Sent => "sent",
            Self::Settled => "settled",
        }
    }

    /// Whether the parent's machine has the report
    #[must_use]
    pub fn delivered(self) -> bool {
        self != Self::Sending
    }
}

impl FromStr for ReportStageKind {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sending" => Ok(Self::Sending),
            "settle_pending" => Ok(Self::SettlePending),
            "sent" => Ok(Self::Sent),
            "settled" => Ok(Self::Settled),
            other => Err(AppError::Internal {
                message: format!("unknown report stage {other}"),
            }),
        }
    }
}

/// A child thread's one report, saved on the child's machine
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadReport {
    /// The child's link to its parent
    pub lineage: Lineage,
    /// Report text, as the parent receives it
    pub body: String,
    /// The child's session when it reported, which the message names as its source
    pub source: ThreadId,
    /// T3 run the report was made in, which must end before the thread settles
    pub run: String,
    /// When the report was saved
    pub created_at: DateTime<Utc>,
    /// How far it has got
    pub stage: ReportStage,
}

impl ThreadReport {
    /// The message that carries the report to the parent
    ///
    /// Every field comes from the saved report, so each attempt sends the
    /// same request, which the message path delivers at most once
    #[must_use]
    pub fn message(&self) -> MessageSendRequest {
        let message_id = self.lineage.report_message();
        MessageSendRequest {
            api_version: API_VERSION,
            message_id,
            // by T3 thread, so a parent that switched provider still gets it
            target: MessageTarget::Reference {
                machine: self.lineage.parent.machine.to_string(),
                reference: format!("thread_{}", self.lineage.parent.t3_thread),
                carried_by: None,
            },
            source: MessageSourceSelector::Thread {
                thread: self.source,
            },
            // the parent finds the child under `thread context --children` by its T3 id
            body: format!(
                "Report from child thread thread_{}:\n\n{}",
                self.lineage.child.t3_thread, self.body
            ),
            reply_to: None,
            conversation_id: message_id.as_uuid(),
        }
    }

    /// The answer `thread report` prints
    #[must_use]
    pub fn view(&self) -> ReportView {
        ReportView {
            link: self.lineage.id,
            parent: self.lineage.parent.clone(),
            child: self.lineage.child.clone(),
            message_id: self.lineage.report_message(),
            stage: self.stage.kind(),
            created_at: self.created_at,
            sent_at: self.stage.sent_at(),
        }
    }
}

impl LinkView {
    /// Whether the parent has the child's report: this machine holds its
    /// receipt, or saw it delivered
    #[must_use]
    pub fn reported(&self) -> bool {
        self.report_received || self.report.is_some_and(ReportStageKind::delivered)
    }

    /// Whether the child still owes its parent a report, given whether this
    /// machine sees the child archived, deleted, or settled
    ///
    /// The one rule behind a parent's `waiting` state, the wake it is owed,
    /// `thread report` refusals, and stall watches. A child that reported
    /// owes nothing more even before its machine settles it, since that
    /// settle waits for the reporting run to end, and a parent the report
    /// woke must be able to report in turn
    #[must_use]
    pub fn owes_report(&self, child_put_away: bool) -> bool {
        !self.reported() && !self.closed && !child_put_away
    }

    /// The link as a lookup of the child shows it
    #[must_use]
    pub fn info(&self) -> ChildThreadInfo {
        ChildThreadInfo {
            link: self.lineage.id,
            parent: self.lineage.parent.clone(),
            reported: self.reported(),
            report: self.report,
        }
    }
}

/// A child's link as a thread lookup shows it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildThreadInfo {
    /// The launch that made the link
    pub link: LinkId,
    /// The thread that launched this one
    pub parent: LinkEnd,
    /// Whether the parent has this thread's report
    pub reported: bool,
    /// How far this thread's report has got, on its own machine; `None`
    /// before it reports
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<ReportStageKind>,
}

/// `POST /v1/threads/report` body
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadReportRequest {
    /// Public API version
    pub api_version: u32,
    /// The child thread that reports
    pub caller: Caller,
    /// Report text
    pub body: String,
}

/// `POST /v1/threads/report` answer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadReportResponse {
    /// Public API version
    pub api_version: u32,
    /// The report as saved
    pub report: ReportView,
    /// Whether an earlier call already saved this report
    pub resumed: bool,
}

/// A saved report, for output
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportView {
    /// The child's link
    pub link: LinkId,
    /// Where the report goes
    pub parent: LinkEnd,
    /// The reporting thread
    pub child: LinkEnd,
    /// Message UUID, the same on every attempt
    pub message_id: MessageId,
    /// How far it has got
    pub stage: ReportStageKind,
    /// When it was saved
    pub created_at: DateTime<Utc>,
    /// When the parent's machine took it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<DateTime<Utc>>,
}

/// Open work that keeps a child thread from reporting
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OpenWork {
    /// A homebased task that has not sent its final event
    Task {
        /// Task id
        id: Uuid,
    },
    /// A GPU queue job that has not sent its final event
    Job {
        /// Job id
        id: Uuid,
    },
    /// Homebased work this lookup cannot name owes the thread events, such
    /// as work submitted from an earlier session of it
    OwedEvents,
    /// A sleep hold on a task or job
    Hold {
        /// Held task or job
        id: Uuid,
    },
    /// A T3 subagent T3 is due to report to the thread
    Subagent,
    /// A child thread that has neither reported nor been put away
    Child {
        /// The child
        child: LinkEnd,
    },
    /// A child thread on a machine that did not answer, so whether it is
    /// open cannot be told
    Unconfirmed {
        /// The child
        child: LinkEnd,
        /// Why the lookup failed
        reason: String,
    },
}

impl fmt::Display for OpenWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task { id } => write!(f, "task {id} has not sent its final event"),
            Self::Job { id } => write!(f, "job {id} has not sent its final event"),
            Self::OwedEvents => write!(f, "homebased work still owes this thread events"),
            Self::Hold { id } => write!(f, "{id} is held for this thread"),
            Self::Subagent => write!(f, "a T3 subagent has not reported yet"),
            Self::Child { child } => write!(
                f,
                "child thread {} on {} has not reported",
                child.t3_thread, child.machine
            ),
            Self::Unconfirmed { child, reason } => write!(
                f,
                "child thread {} on {} cannot be checked: {reason}",
                child.t3_thread, child.machine
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::{Lineage, LinkEnd, LinkId};
    use crate::machine::MachineId;

    fn lineage(request: Uuid) -> Lineage {
        Lineage {
            id: LinkId::from_launch(request),
            parent: LinkEnd {
                machine: MachineId::new(),
                t3_thread: "parent".into(),
            },
            child: LinkEnd {
                machine: MachineId::new(),
                t3_thread: "child".into(),
            },
            created_at: Utc::now(),
        }
    }

    #[test]
    fn the_report_message_is_fixed_by_the_launch_alone() {
        let request = Uuid::now_v7();
        let first = lineage(request);
        // another machine's copy of the same link, saved at another time
        let copy = lineage(request);
        assert_eq!(first.report_message(), copy.report_message());
        assert_ne!(
            first.report_message(),
            lineage(Uuid::now_v7()).report_message()
        );
        assert_ne!(first.report_message().as_uuid(), request);
    }
}
