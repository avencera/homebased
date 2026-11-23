//! Wire types of `homebased thread read`, which reads the messages of a T3
//! thread on whichever Fleet machine runs it
//!
//! A message's position is the one T3's own thread read (`t3_thread_read`)
//! gives it: the index of its item among every item T3 shows in the thread,
//! inherited ones first. Items are only appended, so a position is a stable
//! cursor between reads, and either tool's positions work in the other

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::thread_ref::{ThreadContext, ThreadSelector};

/// Most messages one read returns
pub const MAX_MESSAGES: u16 = 100;

/// Messages one read returns when the caller names no limit
pub const DEFAULT_MESSAGES: u16 = 20;

/// Most characters of one message's text that one read returns
pub const MAX_CHARS: u32 = 20_000;

/// Characters of each message's text when the caller names no limit
pub const DEFAULT_CHARS: u32 = 4_000;

/// How many messages a read returns, from 1 to [`MAX_MESSAGES`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u16", into = "u16")]
pub struct MessageLimit(u16);

impl MessageLimit {
    /// The limit as a count
    #[must_use]
    pub fn get(self) -> u16 {
        self.0
    }
}

impl Default for MessageLimit {
    fn default() -> Self {
        Self(DEFAULT_MESSAGES)
    }
}

impl TryFrom<u16> for MessageLimit {
    type Error = String;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        if (1..=MAX_MESSAGES).contains(&value) {
            return Ok(Self(value));
        }
        Err(format!("a read returns 1 to {MAX_MESSAGES} messages"))
    }
}

impl From<MessageLimit> for u16 {
    fn from(limit: MessageLimit) -> Self {
        limit.0
    }
}

impl fmt::Display for MessageLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for MessageLimit {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let value: u16 = text
            .parse()
            .map_err(|_| format!("a read returns 1 to {MAX_MESSAGES} messages"))?;
        Self::try_from(value)
    }
}

/// How many characters of each message's text a read returns, from 1 to
/// [`MAX_CHARS`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct CharLimit(u32);

impl CharLimit {
    /// The limit as a character count
    #[must_use]
    pub fn get(self) -> u32 {
        self.0
    }
}

impl Default for CharLimit {
    fn default() -> Self {
        Self(DEFAULT_CHARS)
    }
}

impl TryFrom<u32> for CharLimit {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if (1..=MAX_CHARS).contains(&value) {
            return Ok(Self(value));
        }
        Err(format!(
            "a message's text is read 1 to {MAX_CHARS} characters at a time"
        ))
    }
}

impl From<CharLimit> for u32 {
    fn from(limit: CharLimit) -> Self {
        limit.0
    }
}

impl fmt::Display for CharLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for CharLimit {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let value: u32 = text.parse().map_err(|_| {
            format!("a message's text is read 1 to {MAX_CHARS} characters at a time")
        })?;
        Self::try_from(value)
    }
}

/// Which of a thread's messages a read returns
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessageWindow {
    /// The newest messages
    #[default]
    Newest,
    /// The first messages of the thread
    Oldest,
    /// The newest messages before this position
    Before {
        /// Position the messages come before
        position: i64,
    },
    /// The oldest messages after this position
    After {
        /// Position the messages come after
        position: i64,
    },
    /// One message, its text from a character offset, to read past where an
    /// earlier read cut it
    Item {
        /// Position of the message
        position: i64,
        /// Character offset its text starts from
        offset: u32,
    },
}

/// What one read asks for
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessagePage {
    /// Which messages
    #[serde(default)]
    pub window: MessageWindow,
    /// How many at most
    #[serde(default)]
    pub limit: MessageLimit,
    /// How many characters of each message's text at most
    #[serde(default)]
    pub max_chars: CharLimit,
}

/// Who a message is from
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// A message to the agent: the user's, another agent's, or one homebased
    /// or T3 sent, such as a `HOMEBASED_EVENT`
    User,
    /// The agent's own text
    Assistant,
}

impl MessageRole {
    /// Lowercase name, as on the wire
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

impl fmt::Display for MessageRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One message of a thread
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadMessage {
    /// Position in the thread, the cursor for `before`, `after`, and `item`
    pub position: i64,
    /// Who it is from
    pub role: MessageRole,
    /// T3's `createdBy` of a message to the agent, `user` or `agent`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    /// T3's `creationSource` of a message to the agent, such as `web`,
    /// `mobile`, `server`, or `mcp`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// T3 thread that sent a message to the agent, for one an agent sent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_thread: Option<String>,
    /// Thread whose message this is, when the read thread was forked from it
    /// and shows it as inherited history
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<String>,
    /// T3's item status, such as `completed`, `running`, or `interrupted`
    pub status: String,
    /// When the message ended, or started while it is still running
    pub at: Option<DateTime<Utc>>,
    /// Its text from the offset asked for, at most the character limit
    pub text: String,
    /// Characters in the whole text
    pub chars: u32,
    /// Offset that the rest of the text starts from, when the limit cut it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<u32>,
}

/// Messages of one read, oldest first
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MessagesPage {
    /// The messages, oldest first
    pub messages: Vec<ThreadMessage>,
    /// Position to read older messages before, when there are any
    pub earlier: Option<i64>,
    /// Position to read newer messages after, when there are any
    pub later: Option<i64>,
}

/// `POST /v1/threads/read` body
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadReadRequest {
    /// Public API version
    pub api_version: u32,
    /// Thread to read; it must resolve to exactly one
    pub thread: ThreadSelector,
    /// The caller's T3 thread, whose message any T3 chip in `thread` came in
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carried_by: Option<String>,
    /// Look only on this machine, by name or UUID
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    /// Which messages
    #[serde(default)]
    pub page: MessagePage,
}

/// `POST /v1/threads/read` answer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadReadResponse {
    /// Public API version
    pub api_version: u32,
    /// The thread, as `thread context` describes it
    pub thread: ThreadContext,
    /// Its messages
    #[serde(flatten)]
    pub page: MessagesPage,
}
